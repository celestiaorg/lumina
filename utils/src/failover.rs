//! Transport-agnostic multi-endpoint failover engine.
//!
//! [`Failover`](crate::failover::Failover) holds a priority-ordered list of endpoints and runs logical
//! requests against the most-preferred reachable one, transparently falling
//! back to the next endpoint on a *network* error. An optional background
//! health-check probes the preferred endpoint and switches back to it once it
//! has recovered *and its head is fresh* (within a configurable max age).
//!
//! The engine knows nothing about the concrete transport (`C`) or its error
//! type (`E`): callers inject
//!
//! - a per-endpoint *build* closure that lazily constructs the transport
//!   client (or pre-cache it for transports that are built eagerly),
//! - an `is_network_error` classifier deciding which errors trigger failover,
//! - a *call* closure performing a single request against one client, and
//! - (for switch-back) a *probe* closure returning an endpoint's current head
//!   timestamp.
//!
//! Note: the switch-back "healthy" criterion is purely the head's **timestamp
//! age**, not its block height. An endpoint can be a few blocks behind and still
//! count as healthy if its head time is within `max_head_age`. This favours a
//! simple, transport-agnostic signal over strict height parity.
//!
//! This lets both the JSON-RPC and the gRPC clients share one implementation.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
}

/// A transport-agnostic failover engine over a priority-ordered endpoint list.
///
/// See the [module documentation](self) for details.
pub struct Failover<C, E> {
    endpoints: Vec<EndpointSlot<C, E>>,
    /// Index of the endpoint requests currently start from.
    active: AtomicUsize,
    probe_timeout: Duration,
    /// Maximum age of an endpoint's head (by timestamp) for it to be considered
    /// healthy.
    ///
    /// Used by the switch-back check: a preferred endpoint whose head timestamp
    /// is older than this is treated as stale (e.g. reconnected but still
    /// syncing), so we don't switch back to it. This is a timestamp check only —
    /// it does not compare block heights.
    max_head_age: Duration,
    is_network_error: fn(&E) -> bool,
}

impl<C, E> Failover<C, E> {
    /// Create a new failover engine over the given priority-ordered endpoints.
    ///
    /// The first endpoint is the most preferred. `is_network_error` decides
    /// which errors trigger failover; `probe_timeout` bounds a single
    /// health-check probe; `max_head_age` is how recent a preferred endpoint's
    /// head must be for the switch-back to consider it healthy.
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
            active: AtomicUsize::new(0),
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

    /// Index of the endpoint requests currently start from.
    pub fn active(&self) -> usize {
        self.active
            .load(Ordering::Acquire)
            .min(self.endpoints.len() - 1)
    }

    /// The cached client of the currently active endpoint, building it if needed.
    ///
    /// Used by callers that cannot replay a request across endpoints (e.g.
    /// JSON-RPC batches) and so must target a single endpoint without failover.
    pub async fn active_client(&self) -> Result<Arc<C>, E> {
        self.endpoints[self.active()].get_or_build().await
    }

    /// Order in which endpoints are attempted: the active endpoint first, then
    /// all others in priority order (so we always re-try the preferred ones).
    fn attempt_order(&self) -> Vec<usize> {
        let n = self.endpoints.len();
        let active = self.active();
        let mut order = Vec::with_capacity(n);
        order.push(active);
        order.extend((0..n).filter(|&i| i != active));
        order
    }

    fn select(&self, idx: usize) {
        if self.active.load(Ordering::Acquire) != idx {
            self.active.store(idx, Ordering::Release);
        }
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
                    slot.set_healthy(true);
                    self.select(idx);
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

    /// If the preferred endpoint (index `0`) is not the active one and has
    /// recovered, switch the active endpoint back to it.
    ///
    /// The preferred endpoint counts as healthy only if it answers the probe
    /// **and** its head is fresh (within `max_head_age`). A reconnected-but-still
    /// -syncing endpoint reports an old head, so we keep using the fallback
    /// rather than switch back to stale data.
    ///
    /// `probe` returns the endpoint's current head time (unix seconds) if it
    /// answers, or `None` if it is unreachable (the engine applies
    /// `probe_timeout`).
    pub async fn switch_back_with<F, Fut>(&self, probe: F)
    where
        F: Fn(Arc<C>) -> Fut,
        Fut: std::future::Future<Output = Option<i64>>,
    {
        if self.active() == 0 {
            return;
        }

        let slot = &self.endpoints[0];
        let client = match slot.get_or_build().await {
            Ok(client) => client,
            Err(_) => {
                slot.set_healthy(false);
                return;
            }
        };

        let Some(head) = self.probe(&probe, client).await else {
            slot.set_healthy(false);
            return;
        };

        if !self.is_fresh(head) {
            slot.set_healthy(false);
            debug!(
                "failover: preferred endpoint {} recovered but its head is stale \
                 (older than {:?}); staying on the active endpoint",
                slot.label, self.max_head_age
            );
            return;
        }

        slot.set_healthy(true);
        self.select(0);
        debug!(
            "failover: switched back to preferred endpoint {}",
            slot.label
        );
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
    /// Every `interval` it probes the preferred endpoint via `probe` and, once
    /// it has recovered and its head is recent enough, switches the active
    /// endpoint back to it (see
    /// [`switch_back_with`](Failover::switch_back_with)). The task holds a
    /// [`Weak`] reference, so it stops on its own once the engine is dropped.
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
        engine.switch_back_with(&probe).await;
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
            active: AtomicUsize::new(0),
            probe_timeout: Duration::from_secs(1),
            max_head_age: Duration::from_secs(30),
            is_network_error: is_network,
        }
    }

    async fn do_request(engine: &Failover<FakeClient, FakeError>) -> Result<u64, FakeError> {
        engine.run(|client| async move { client.request() }).await
    }

    /// A switch-back probe reporting a reachable endpoint's head height.
    async fn do_switch_back(engine: &Failover<FakeClient, FakeError>) {
        engine
            .switch_back_with(|client| async move {
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
        do_switch_back(&engine).await;

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

        do_switch_back(&engine).await;

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
        do_switch_back(&engine).await;
        assert_eq!(
            engine.active(),
            1,
            "must not switch back to a stale preferred"
        );

        // Once its head is fresh again, the next health-check switches back.
        preferred.set_fresh_head();
        do_switch_back(&engine).await;
        assert_eq!(engine.active(), 0, "switches back once caught up");
    }

    #[tokio::test]
    async fn only_switches_back_to_preferred_not_intermediate() {
        let preferred = Arc::new(FakeClient::new(false));
        let middle = Arc::new(FakeClient::new(false));
        let last = Arc::new(FakeClient::new(true));
        let engine = engine(vec![preferred.clone(), middle.clone(), last.clone()]);

        do_request(&engine).await.unwrap();
        assert_eq!(engine.active(), 2);

        // The intermediate endpoint recovers but the preferred is still down.
        middle.set_up(true);
        do_switch_back(&engine).await;
        assert_eq!(
            engine.active(),
            2,
            "intermediate recovery must not switch back"
        );

        // Once the preferred recovers, the next probe switches straight back.
        preferred.set_up(true);
        do_switch_back(&engine).await;
        assert_eq!(engine.active(), 0, "should switch back to preferred");
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
