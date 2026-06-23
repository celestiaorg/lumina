//! Transport-agnostic multi-endpoint failover engine.
//!
//! [`Failover`](crate::failover::Failover) holds a priority-ordered list of endpoints and runs logical
//! requests against the most-preferred *healthy* one, transparently falling
//! back to the next endpoint on a *network* error. Each endpoint carries a
//! health flag; the active endpoint is **derived** as the lowest-index healthy
//! one rather than stored, so an in-flight fallback request can never clobber a
//! concurrent recovery of a more-preferred endpoint. An optional background
//! health-check probes the currently-unhealthy endpoints and brings each one
//! back once it has recovered *and its head is fresh* (within a configurable max
//! age).
//!
//! The engine knows nothing about the concrete transport (`C`) or its error
//! type (`E`): callers inject
//!
//! - a per-endpoint *build* closure that lazily constructs the transport
//!   client (or pre-cache it for transports that are built eagerly),
//! - an `is_network_error` classifier deciding which errors trigger failover,
//! - a *call* closure performing a single request against one client, and
//! - (for recovery) a *probe* closure returning an endpoint's current head
//!   timestamp.
//!
//! Note: the recovery "healthy" criterion is purely the head's **timestamp
//! age**, not its block height. An endpoint can be a few blocks behind and still
//! count as healthy if its head time is within `max_head_age`. This favours a
//! simple, transport-agnostic signal over strict height parity.
//!
//! This lets both the JSON-RPC and the gRPC clients share one implementation.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tracing::{debug, warn};

use crate::executor::spawn;
use crate::time::{Interval, timeout};

/// A future that builds a client for a single endpoint.
///
/// It is `Send` on all targets (see [`box_build`]) so that the futures awaiting
/// it stay `Send` even on wasm, matching what transport client traits require.
/// Produce one with [`box_build`].
pub type BuildFut<C, E> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<C, E>> + Send>>;

type BuildFn<C, E> = Arc<dyn Fn() -> BuildFut<C, E> + Send + Sync>;

/// Box an endpoint-build future into a `Send` [`BuildFut`].
///
/// On wasm the underlying transport futures are `!Send`, so we wrap them in
/// `send_wrapper::SendWrapper`, which is valid on wasm's single thread.
#[cfg(not(target_arch = "wasm32"))]
pub fn box_build<C, E, F>(fut: F) -> BuildFut<C, E>
where
    C: 'static,
    E: 'static,
    F: std::future::Future<Output = Result<C, E>> + Send + 'static,
{
    Box::pin(fut)
}

/// Box an endpoint-build future into a `Send` [`BuildFut`] (wasm32 variant).
#[cfg(target_arch = "wasm32")]
pub fn box_build<C, E, F>(fut: F) -> BuildFut<C, E>
where
    C: 'static,
    E: 'static,
    F: std::future::Future<Output = Result<C, E>> + 'static,
{
    Box::pin(send_wrapper::SendWrapper::new(fut))
}

/// Configuration for a single endpoint passed to [`Failover::new`].
pub struct Endpoint<C, E> {
    /// Human-readable label used in logs (typically the url).
    pub label: String,
    /// How to lazily build the endpoint's client.
    ///
    /// `None` is only valid together with a pre-built `client`, for transports
    /// (e.g. gRPC channels) that are constructed eagerly and never rebuilt.
    pub build: Option<BuildFn<C, E>>,
    /// A pre-built client, cached up front so `build` is never invoked.
    pub client: Option<Arc<C>>,
}

impl<C, E> Endpoint<C, E> {
    /// An endpoint whose client is built lazily on first use.
    ///
    /// `build` is invoked (possibly repeatedly, after failures) to construct the
    /// client; have it return [`box_build`]`(async { ... })`.
    pub fn lazy<F>(label: impl Into<String>, build: F) -> Endpoint<C, E>
    where
        F: Fn() -> BuildFut<C, E> + Send + Sync + 'static,
    {
        Endpoint {
            label: label.into(),
            build: Some(Arc::new(build)),
            client: None,
        }
    }

    /// An endpoint whose client is already built.
    pub fn prebuilt(label: impl Into<String>, client: C) -> Endpoint<C, E> {
        Endpoint {
            label: label.into(),
            build: None,
            client: Some(Arc::new(client)),
        }
    }
}

/// State of a single endpoint: how to build its client, the cached client, and
/// whether it is currently believed to be healthy.
struct EndpointSlot<C, E> {
    label: String,
    build: Option<BuildFn<C, E>>,
    // Only ever locked briefly and never across an `.await`, so a std mutex is
    // fine on both native and wasm.
    client: Mutex<Option<Arc<C>>>,
    healthy: AtomicBool,
}

impl<C, E> EndpointSlot<C, E> {
    fn cached(&self) -> Option<Arc<C>> {
        self.client.lock().expect("mutex poisoned").clone()
    }

    /// Returns the endpoint's client, building it on first use (or after a
    /// previous build failure).
    async fn get_or_build(&self) -> Result<Arc<C>, E> {
        if let Some(client) = self.cached() {
            return Ok(client);
        }

        let build = self
            .build
            .as_ref()
            .expect("pre-built endpoints must always be cached");

        // Build without holding the lock.
        let built = Arc::new((build)().await?);

        let mut guard = self.client.lock().expect("mutex poisoned");
        if let Some(existing) = guard.clone() {
            // Another task built it concurrently; keep theirs.
            return Ok(existing);
        }
        *guard = Some(built.clone());
        Ok(built)
    }

    fn set_healthy(&self, healthy: bool) {
        self.healthy.store(healthy, Ordering::Release);
    }

    fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }
}

/// A transport-agnostic failover engine over a priority-ordered endpoint list.
///
/// See the [module documentation](self) for details.
pub struct Failover<C, E> {
    /// Priority-ordered endpoints; index `0` is the most preferred.
    ///
    /// There is deliberately no stored "active" index: the endpoint a request
    /// uses is *derived* as the lowest-index healthy endpoint (see
    /// [`attempt_order`](Failover::attempt_order)). Routing being a pure function
    /// of the per-endpoint `healthy` flags means an in-flight fallback request
    /// can never clobber a concurrent recovery of a more-preferred endpoint.
    endpoints: Vec<EndpointSlot<C, E>>,
    probe_timeout: Duration,
    /// Maximum age of an endpoint's head (by timestamp) for it to be considered
    /// healthy.
    ///
    /// Used by the background recovery probe: an endpoint whose head timestamp is
    /// older than this is treated as stale (e.g. reconnected but still syncing),
    /// so we keep it inactive. This is a timestamp check only — it does not
    /// compare block heights.
    max_head_age: Duration,
    is_network_error: fn(&E) -> bool,
}

impl<C, E> Failover<C, E> {
    /// Create a new failover engine over the given priority-ordered endpoints.
    ///
    /// The first endpoint is the most preferred. `is_network_error` decides
    /// which errors trigger failover; `probe_timeout` bounds a single
    /// health-check probe; `max_head_age` is how recent an endpoint's head must
    /// be for the recovery probe to consider it healthy again.
    ///
    /// Returns `None` if `endpoints` is empty.
    pub fn new(
        endpoints: Vec<Endpoint<C, E>>,
        is_network_error: fn(&E) -> bool,
        probe_timeout: Duration,
        max_head_age: Duration,
    ) -> Option<Failover<C, E>> {
        if endpoints.is_empty() {
            return None;
        }

        let endpoints = endpoints
            .into_iter()
            .map(|endpoint| EndpointSlot {
                label: endpoint.label,
                build: endpoint.build,
                client: Mutex::new(endpoint.client),
                healthy: AtomicBool::new(true),
            })
            .collect();

        Some(Failover {
            endpoints,
            probe_timeout,
            max_head_age,
            is_network_error,
        })
    }

    /// Number of configured endpoints.
    pub fn len(&self) -> usize {
        self.endpoints.len()
    }

    /// Whether there are no endpoints. Always `false` for a constructed engine.
    pub fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }

    /// Index of the endpoint requests currently prefer: the lowest-index healthy
    /// endpoint, or `0` if none are currently healthy.
    ///
    /// This is *derived* from the per-endpoint health flags, not stored, so it
    /// always reflects the latest health information without any writer that
    /// could race a concurrent recovery.
    pub fn active(&self) -> usize {
        (0..self.endpoints.len())
            .find(|&i| self.endpoints[i].is_healthy())
            .unwrap_or(0)
    }

    /// The cached client of the currently active endpoint, building it if needed.
    ///
    /// Used by callers that cannot replay a request across endpoints (e.g.
    /// JSON-RPC batches) and so must target a single endpoint without failover.
    pub async fn active_client(&self) -> Result<Arc<C>, E> {
        self.endpoints[self.active()].get_or_build().await
    }

    /// Order in which endpoints are attempted: currently-healthy endpoints in
    /// priority order, then currently-unhealthy ones (also in priority order) as
    /// a last resort.
    ///
    /// So requests prefer the most-preferred *healthy* endpoint, an unhealthy
    /// endpoint is skipped until the background probe (or a total outage) brings
    /// it back, and even when everything is marked unhealthy we still attempt the
    /// preferred endpoint first.
    fn attempt_order(&self) -> Vec<usize> {
        let n = self.endpoints.len();
        let (mut healthy, unhealthy): (Vec<usize>, Vec<usize>) =
            (0..n).partition(|&i| self.endpoints[i].is_healthy());
        healthy.extend(unhealthy);
        healthy
    }

    /// Whether a head produced at `head_unix_secs` is recent enough (within
    /// `max_head_age`) for the endpoint to count as healthy. This is a timestamp
    /// check only and does not consider block height.
    ///
    /// A head slightly in the future (clock skew) is treated as fresh.
    fn is_fresh(&self, head_unix_secs: i64) -> bool {
        let age = now_unix_secs().saturating_sub(head_unix_secs);
        age <= self.max_head_age.as_secs() as i64
    }

    /// Run a single logical request against the endpoints, failing over on
    /// network errors and returning the first success (or the last error).
    ///
    /// `call` is invoked once per attempted endpoint with that endpoint's
    /// client; it must be replayable. Non-network errors are surfaced
    /// immediately without trying another endpoint. If every endpoint fails to
    /// even build, the last build error is returned.
    pub async fn run<T, F, Fut>(&self, call: F) -> Result<T, E>
    where
        E: std::fmt::Display,
        F: Fn(Arc<C>) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        let mut last_err: Option<E> = None;

        for idx in self.attempt_order() {
            let slot = &self.endpoints[idx];

            let client = match slot.get_or_build().await {
                Ok(client) => client,
                Err(err) => {
                    warn!("failover: endpoint {} unavailable: {err}", slot.label);
                    slot.set_healthy(false);
                    last_err = Some(err);
                    continue;
                }
            };

            match call(client).await {
                Ok(value) => {
                    // Marking only this endpoint healthy is enough; routing is
                    // derived from the flags, so a fallback success cannot demote
                    // or override a more-preferred endpoint.
                    slot.set_healthy(true);
                    return Ok(value);
                }
                Err(err) if (self.is_network_error)(&err) => {
                    warn!("failover: endpoint {} network error: {err}", slot.label);
                    slot.set_healthy(false);
                    last_err = Some(err);
                    continue;
                }
                // Application-level error: surface it without failing over.
                Err(err) => return Err(err),
            }
        }

        Err(last_err.expect("attempt_order is never empty"))
    }

    /// Probe every currently-unhealthy endpoint and bring back the ones that have
    /// recovered, so routing can again prefer them.
    ///
    /// An endpoint is brought back only if it answers the probe **and** its head
    /// is fresh (within `max_head_age`). A reconnected-but-still-syncing endpoint
    /// reports an old head, so it stays inactive rather than serving stale data.
    /// Healthy endpoints are left untouched (they are presumably in use and would
    /// be marked unhealthy by [`run`](Failover::run) on a real failure).
    ///
    /// Because the active endpoint is derived as the lowest-index healthy one,
    /// reviving a more-preferred endpoint here automatically routes subsequent
    /// requests back to it — without any switch that an in-flight request could
    /// clobber.
    ///
    /// `probe` returns the endpoint's current head time (unix seconds) if it
    /// answers, or `None` if it is unreachable (the engine applies
    /// `probe_timeout`).
    pub async fn recover_unhealthy_with<F, Fut>(&self, probe: F)
    where
        F: Fn(Arc<C>) -> Fut,
        Fut: std::future::Future<Output = Option<i64>>,
    {
        for idx in 0..self.endpoints.len() {
            let slot = &self.endpoints[idx];
            if slot.is_healthy() {
                continue;
            }

            let Ok(client) = slot.get_or_build().await else {
                // Still cannot even build a client; remains inactive.
                continue;
            };

            let Some(head) = self.probe(&probe, client).await else {
                // Unreachable; remains inactive.
                continue;
            };

            if !self.is_fresh(head) {
                debug!(
                    "failover: endpoint {} reachable but its head is stale \
                     (older than {:?}); keeping it inactive",
                    slot.label, self.max_head_age
                );
                continue;
            }

            slot.set_healthy(true);
            debug!("failover: endpoint {} recovered and is active again", slot.label);
        }
    }

    async fn probe<F, Fut>(&self, probe: &F, client: Arc<C>) -> Option<i64>
    where
        F: Fn(Arc<C>) -> Fut,
        Fut: std::future::Future<Output = Option<i64>>,
    {
        (timeout(self.probe_timeout, probe(client)).await).unwrap_or(None)
    }
}

/// Current wall-clock time as unix seconds, on native and wasm.
fn now_unix_secs() -> i64 {
    #[cfg(not(target_arch = "wasm32"))]
    use std::time::{SystemTime, UNIX_EPOCH};
    #[cfg(target_arch = "wasm32")]
    use web_time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// The shared body of [`Failover::spawn_health_check`]. The public method is
/// `cfg`-split only to carry the right `Send`/`Sync` bounds per target.
#[cfg(not(target_arch = "wasm32"))]
impl<C, E> Failover<C, E>
where
    C: Send + Sync + 'static,
    E: Send + 'static,
{
    /// Spawn the background health-check task.
    ///
    /// Every `interval` it probes each currently-unhealthy endpoint via `probe`
    /// and brings back the ones that have recovered and whose head is recent
    /// enough (see
    /// [`recover_unhealthy_with`](Failover::recover_unhealthy_with)). The task
    /// holds a [`Weak`] reference, so it stops on its own once the engine is
    /// dropped.
    ///
    /// Does nothing when only a single endpoint is configured.
    pub fn spawn_health_check<F, Fut>(self: &Arc<Self>, interval: Duration, probe: F)
    where
        F: Fn(Arc<C>) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Option<i64>> + Send + 'static,
    {
        if self.endpoints.len() < 2 {
            return;
        }
        spawn(health_check_loop(Arc::downgrade(self), interval, probe));
    }
}

#[cfg(target_arch = "wasm32")]
impl<C, E> Failover<C, E>
where
    C: 'static,
    E: 'static,
{
    /// Spawn the background health-check task. See the non-wasm impl for docs.
    pub fn spawn_health_check<F, Fut>(self: &Arc<Self>, interval: Duration, probe: F)
    where
        F: Fn(Arc<C>) -> Fut + 'static,
        Fut: std::future::Future<Output = Option<i64>> + 'static,
    {
        if self.endpoints.len() < 2 {
            return;
        }
        spawn(health_check_loop(Arc::downgrade(self), interval, probe));
    }
}

async fn health_check_loop<C, E, F, Fut>(weak: Weak<Failover<C, E>>, interval: Duration, probe: F)
where
    F: Fn(Arc<C>) -> Fut,
    Fut: std::future::Future<Output = Option<i64>>,
{
    let mut ticker = Interval::new(interval);
    loop {
        ticker.tick().await;
        let Some(engine) = weak.upgrade() else {
            break;
        };
        engine.recover_unhealthy_with(&probe).await;
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

    use super::*;

    /// A head this many seconds old is considered stale by the tests.
    const STALE_AGE_SECS: i64 = 3600;

    /// A configurable fake transport client. `head` is the head's unix-seconds
    /// timestamp; the probe returns it so the engine can judge freshness.
    struct FakeClient {
        up: AtomicBool,
        calls: AtomicUsize,
        head: AtomicI64,
    }

    /// A simple error type with a "network error" variant.
    #[derive(Debug)]
    enum FakeError {
        Network,
        App,
    }

    impl std::fmt::Display for FakeError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                FakeError::Network => f.write_str("network"),
                FakeError::App => f.write_str("app"),
            }
        }
    }

    fn is_network(err: &FakeError) -> bool {
        matches!(err, FakeError::Network)
    }

    impl FakeClient {
        /// A client that is up and whose head is fresh (current time).
        fn new(up: bool) -> Self {
            FakeClient {
                up: AtomicBool::new(up),
                calls: AtomicUsize::new(0),
                head: AtomicI64::new(now_unix_secs()),
            }
        }

        fn set_up(&self, up: bool) {
            self.up.store(up, Ordering::SeqCst);
        }

        fn set_fresh_head(&self) {
            self.head.store(now_unix_secs(), Ordering::SeqCst);
        }

        fn set_stale_head(&self) {
            self.head
                .store(now_unix_secs() - STALE_AGE_SECS, Ordering::SeqCst);
        }

        fn head(&self) -> i64 {
            self.head.load(Ordering::SeqCst)
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        /// Returns `1` when up, a network error otherwise.
        fn request(&self) -> Result<u64, FakeError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.up.load(Ordering::SeqCst) {
                Ok(1)
            } else {
                Err(FakeError::Network)
            }
        }
    }

    /// Build a `Failover` whose endpoint clients are the provided fakes.
    fn engine(fakes: Vec<Arc<FakeClient>>) -> Failover<FakeClient, FakeError> {
        let endpoints = fakes
            .into_iter()
            .enumerate()
            .map(|(i, fake)| EndpointSlot {
                label: format!("fake://{i}"),
                build: None,
                client: Mutex::new(Some(fake)),
                healthy: AtomicBool::new(true),
            })
            .collect();

        Failover {
            endpoints,
            probe_timeout: Duration::from_secs(1),
            max_head_age: Duration::from_secs(30),
            is_network_error: is_network,
        }
    }

    async fn do_request(engine: &Failover<FakeClient, FakeError>) -> Result<u64, FakeError> {
        engine.run(|client| async move { client.request() }).await
    }

    /// Run one recovery pass, probing reachable endpoints for their head time.
    async fn do_health_check(engine: &Failover<FakeClient, FakeError>) {
        engine
            .recover_unhealthy_with(|client| async move {
                match client.request() {
                    Ok(_) => Some(client.head()),
                    Err(_) => None,
                }
            })
            .await
    }

    #[tokio::test]
    async fn uses_preferred_endpoint_when_healthy() {
        let preferred = Arc::new(FakeClient::new(true));
        let fallback = Arc::new(FakeClient::new(true));
        let engine = engine(vec![preferred.clone(), fallback.clone()]);

        do_request(&engine).await.unwrap();

        assert_eq!(preferred.calls(), 1);
        assert_eq!(fallback.calls(), 0);
        assert_eq!(engine.active(), 0);
    }

    #[tokio::test]
    async fn fails_over_to_next_endpoint() {
        let preferred = Arc::new(FakeClient::new(false));
        let fallback = Arc::new(FakeClient::new(true));
        let engine = engine(vec![preferred.clone(), fallback.clone()]);

        let value = do_request(&engine).await.unwrap();

        assert_eq!(value, 1);
        assert_eq!(preferred.calls(), 1, "preferred should be tried first");
        assert_eq!(fallback.calls(), 1, "fallback should serve the request");
        assert_eq!(engine.active(), 1);
    }

    #[tokio::test]
    async fn application_errors_do_not_fail_over() {
        let preferred = Arc::new(FakeClient::new(true));
        let fallback = Arc::new(FakeClient::new(true));
        let engine = engine(vec![preferred.clone(), fallback.clone()]);

        let err = engine
            .run(|_client| async move { Err::<u64, _>(FakeError::App) })
            .await
            .unwrap_err();

        assert!(matches!(err, FakeError::App));
        assert_eq!(fallback.calls(), 0, "must not fall back on app error");
    }

    #[tokio::test]
    async fn returns_last_error_when_all_endpoints_down() {
        let preferred = Arc::new(FakeClient::new(false));
        let fallback = Arc::new(FakeClient::new(false));
        let engine = engine(vec![preferred.clone(), fallback.clone()]);

        let err = do_request(&engine).await.unwrap_err();

        assert!(is_network(&err));
        assert_eq!(preferred.calls(), 1);
        assert_eq!(fallback.calls(), 1);
    }

    #[tokio::test]
    async fn switches_back_to_preferred_after_recovery() {
        let preferred = Arc::new(FakeClient::new(false));
        let fallback = Arc::new(FakeClient::new(true));
        let engine = engine(vec![preferred.clone(), fallback.clone()]);

        do_request(&engine).await.unwrap();
        assert_eq!(engine.active(), 1);

        preferred.set_up(true);
        do_health_check(&engine).await;

        assert_eq!(engine.active(), 0, "should switch back to preferred");

        let before = fallback.calls();
        do_request(&engine).await.unwrap();
        assert_eq!(fallback.calls(), before, "fallback no longer used");
    }

    #[tokio::test]
    async fn no_switch_back_while_preferred_still_down() {
        let preferred = Arc::new(FakeClient::new(false));
        let fallback = Arc::new(FakeClient::new(true));
        let engine = engine(vec![preferred.clone(), fallback.clone()]);

        do_request(&engine).await.unwrap();
        assert_eq!(engine.active(), 1);

        do_health_check(&engine).await;

        assert_eq!(engine.active(), 1, "must stay on fallback");
    }

    #[tokio::test]
    async fn no_switch_back_while_preferred_is_stale() {
        let preferred = Arc::new(FakeClient::new(false));
        let fallback = Arc::new(FakeClient::new(true));
        let engine = engine(vec![preferred.clone(), fallback.clone()]);

        do_request(&engine).await.unwrap();
        assert_eq!(engine.active(), 1);

        // Preferred answers again but its head is old: it has reconnected but is
        // still catching up.
        preferred.set_up(true);
        preferred.set_stale_head();
        do_health_check(&engine).await;
        assert_eq!(
            engine.active(),
            1,
            "must not switch back to a stale preferred"
        );

        // Once its head is fresh again, the next health-check switches back.
        preferred.set_fresh_head();
        do_health_check(&engine).await;
        assert_eq!(engine.active(), 0, "switches back once caught up");
    }

    #[tokio::test]
    async fn recovery_prefers_higher_priority_endpoint() {
        let preferred = Arc::new(FakeClient::new(false));
        let middle = Arc::new(FakeClient::new(false));
        let last = Arc::new(FakeClient::new(true));
        let engine = engine(vec![preferred.clone(), middle.clone(), last.clone()]);

        do_request(&engine).await.unwrap();
        assert_eq!(engine.active(), 2);

        // The intermediate endpoint recovers while the preferred is still down:
        // routing prefers it over the lower-priority `last` endpoint.
        middle.set_up(true);
        do_health_check(&engine).await;
        assert_eq!(
            engine.active(),
            1,
            "a recovered higher-priority endpoint is preferred over a lower one"
        );

        // Once the preferred recovers too, it takes precedence again.
        preferred.set_up(true);
        do_health_check(&engine).await;
        assert_eq!(engine.active(), 0, "preferred takes precedence once recovered");
    }

    #[tokio::test]
    async fn fallback_success_does_not_demote_preferred() {
        // The flapping bug, at the routing level: once the preferred endpoint is
        // healthy again, ongoing successful fallback usage must not pull routing
        // back to the fallback. With routing derived from per-endpoint health
        // flags there is no shared "active" pointer for a fallback success to
        // clobber.
        let preferred = Arc::new(FakeClient::new(true));
        let fallback = Arc::new(FakeClient::new(true));
        let engine = engine(vec![preferred.clone(), fallback.clone()]);

        // Force a failover to the fallback, then let the preferred recover.
        engine.endpoints[0].set_healthy(false);
        do_request(&engine).await.unwrap();
        assert_eq!(engine.active(), 1);

        do_health_check(&engine).await;
        assert_eq!(engine.active(), 0, "preferred is active again after recovery");

        // Further requests go to the preferred and never touch the fallback, no
        // matter how many succeed.
        let before = fallback.calls();
        for _ in 0..5 {
            do_request(&engine).await.unwrap();
        }
        assert_eq!(fallback.calls(), before, "fallback must not be re-pinned");
        assert_eq!(engine.active(), 0);
    }

    #[tokio::test]
    async fn empty_endpoints_returns_none() {
        let engine = Failover::<FakeClient, FakeError>::new(
            vec![],
            is_network,
            Duration::from_secs(1),
            Duration::from_secs(30),
        );
        assert!(engine.is_none());
    }
}
