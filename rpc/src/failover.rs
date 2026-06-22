//! Multi-endpoint Json-RPC client with automatic failover and switch-back.
//!
//! [`FailoverClient`] wraps a priority-ordered list of RPC endpoints. Requests
//! are sent to the most-preferred endpoint that is currently reachable; if a
//! network error occurs, the client transparently falls back to the next
//! endpoint. A background health-check task periodically probes the preferred
//! endpoint and, as soon as it recovers *and its head is recent enough* (within
//! a configurable max age), switches back to it.
//!
//! The actual failover state machine lives in the transport-agnostic
//! [`lumina_utils::failover::Failover`] engine; this module is the JSON-RPC
//! adapter around it.
//!
//! The first endpoint in the list (index `0`) is the most preferred one.
//!
//! # Notes
//!
//! - Only *network* errors trigger failover. Server-returned JSON-RPC errors
//!   (e.g. a method returning an error) are returned to the caller as-is and do
//!   not cause a retry on another endpoint.
//! - Batch requests are routed to the currently selected endpoint without
//!   failover, because [`jsonrpsee`]'s batch builder cannot be replayed.
//! - For subscriptions, only the initial subscribe call is subject to failover.
//!   If the underlying connection drops mid-stream the subscription ends, and
//!   the caller is expected to re-subscribe (which will pick a healthy endpoint).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use jsonrpsee::core::ClientError;
use jsonrpsee::core::JsonRawValue as RawValue;
use jsonrpsee::core::client::{BatchResponse, ClientT, Subscription, SubscriptionClientT};
use jsonrpsee::core::params::BatchRequestBuilder;
use jsonrpsee::core::traits::ToRpcParams;
use lumina_utils::failover::{Endpoint as FailoverEndpoint, Failover, box_build};
use serde::de::DeserializeOwned;

use crate::HeaderClient;
use crate::client::Client;
use crate::error::Error;

/// Default interval between background health-checks of the preferred endpoint.
pub const DEFAULT_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// Default timeout for a single health-check probe.
pub const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Default maximum head age for an endpoint to be considered healthy enough to
/// switch back to.
pub const DEFAULT_MAX_HEAD_AGE: Duration = Duration::from_secs(30);

/// A single RPC endpoint with its optional authentication token.
///
/// Different providers (e.g. your own node and a fallback such as QuickNode)
/// commonly require different auth tokens, hence the per-endpoint token.
#[derive(Debug, Clone)]
pub struct RpcEndpoint {
    /// The endpoint url, e.g. `ws://localhost:26658` or `https://rpc.example.com`.
    pub url: String,
    /// Optional bearer authentication token for this endpoint.
    pub auth_token: Option<String>,
}

impl RpcEndpoint {
    /// Create a new endpoint from a url, without an auth token.
    pub fn new(url: impl Into<String>) -> RpcEndpoint {
        RpcEndpoint {
            url: url.into(),
            auth_token: None,
        }
    }

    /// Set the authentication token for this endpoint.
    pub fn auth_token(mut self, token: impl Into<String>) -> RpcEndpoint {
        self.auth_token = Some(token.into());
        self
    }
}

impl<S> From<S> for RpcEndpoint
where
    S: Into<String>,
{
    fn from(url: S) -> RpcEndpoint {
        RpcEndpoint::new(url)
    }
}

/// Pre-serialized RPC params, so that a request can be replayed across endpoints.
///
/// [`ToRpcParams::to_rpc_params`] consumes its receiver, so to retry the same
/// request on a different endpoint we serialize the params once up front and
/// hand out clones.
struct RawParams(Option<Box<RawValue>>);

impl ToRpcParams for RawParams {
    fn to_rpc_params(self) -> Result<Option<Box<RawValue>>, serde_json::Error> {
        Ok(self.0)
    }
}

/// A Json-RPC client over multiple endpoints with automatic failover and
/// switch-back to the preferred endpoint once it recovers.
///
/// Requests go to the most-preferred reachable endpoint; on a network error the
/// client transparently fails over to the next one. When a health-check interval
/// is configured, a background task switches back to the preferred endpoint once
/// it recovers and its head is recent enough.
pub struct FailoverClient {
    inner: Arc<Failover<Client, ClientError>>,
}

impl FailoverClient {
    /// Create a new failover client over the given priority-ordered endpoints.
    ///
    /// The first endpoint is the most preferred. When more than one endpoint is
    /// provided and `health_check_interval` is `Some`, a background task probes
    /// the preferred endpoint on that interval and switches back to it once it
    /// recovers.
    ///
    /// Endpoint clients are built lazily on first use, so a fallback endpoint
    /// being unreachable at construction time does not prevent the client from
    /// being created.
    pub fn new(
        endpoints: Vec<RpcEndpoint>,
        connect_timeout: Option<Duration>,
        request_timeout: Option<Duration>,
        health_check_interval: Option<Duration>,
        max_head_age: Duration,
    ) -> Result<FailoverClient, Error> {
        let probe_timeout = request_timeout.unwrap_or(DEFAULT_PROBE_TIMEOUT);

        let slots = endpoints
            .into_iter()
            .map(|endpoint| {
                let url = endpoint.url.clone();
                FailoverEndpoint::lazy(url, move || {
                    let url = endpoint.url.clone();
                    let auth_token = endpoint.auth_token.clone();
                    box_build(async move {
                        Client::new(
                            &url,
                            auth_token.as_deref(),
                            connect_timeout,
                            request_timeout,
                        )
                        .await
                        .map_err(build_error_to_client_error)
                    })
                })
            })
            .collect::<Vec<_>>();

        let inner = Failover::new(slots, is_network_error, probe_timeout, max_head_age)
            .ok_or(Error::NoEndpoints)?;
        let inner = Arc::new(inner);

        if let Some(interval) = health_check_interval {
            inner.spawn_health_check(interval, |client: Arc<Client>| async move {
                HeaderClient::header_network_head(client.as_ref())
                    .await
                    .ok()
                    .map(|header| header.time().unix_timestamp())
            });
        }

        Ok(FailoverClient { inner })
    }
}

/// Returns whether an error should trigger failover to another endpoint.
///
/// Only connectivity/transport-level failures count; server-returned JSON-RPC
/// errors and response parsing errors are treated as real results.
pub(crate) fn is_network_error(err: &ClientError) -> bool {
    matches!(
        err,
        ClientError::Transport(_)
            | ClientError::RestartNeeded(_)
            | ClientError::RequestTimeout
            | ClientError::ServiceDisconnect
            | ClientError::Custom(_)
    )
}

/// Convert an endpoint-build error into a [`ClientError`], preserving the
/// underlying jsonrpsee error (e.g. a `Transport` connection failure) where
/// possible so callers see the same error variant as with a single endpoint.
fn build_error_to_client_error(err: Error) -> ClientError {
    match err {
        Error::JsonRpc(client_err) => client_err,
        other => ClientError::Custom(other.to_string()),
    }
}

impl ClientT for FailoverClient {
    async fn notification<Params>(&self, method: &str, params: Params) -> Result<(), ClientError>
    where
        Params: ToRpcParams + Send,
    {
        let raw = params.to_rpc_params()?;
        self.inner
            .run(|client| {
                let raw = raw.clone();
                async move { client.notification(method, RawParams(raw)).await }
            })
            .await
    }

    async fn request<R, Params>(&self, method: &str, params: Params) -> Result<R, ClientError>
    where
        R: DeserializeOwned,
        Params: ToRpcParams + Send,
    {
        let raw = params.to_rpc_params()?;
        self.inner
            .run(|client| {
                let raw = raw.clone();
                async move { client.request::<R, _>(method, RawParams(raw)).await }
            })
            .await
    }

    async fn batch_request<'a, R>(
        &self,
        batch: BatchRequestBuilder<'a>,
    ) -> Result<BatchResponse<'a, R>, ClientError>
    where
        R: DeserializeOwned + fmt::Debug + 'a,
    {
        // The batch builder cannot be replayed, so route to the active endpoint
        // without failover.
        let client = self.inner.active_client().await?;
        client.batch_request(batch).await
    }
}

impl SubscriptionClientT for FailoverClient {
    async fn subscribe<'a, N, Params>(
        &self,
        subscribe_method: &'a str,
        params: Params,
        unsubscribe_method: &'a str,
    ) -> Result<Subscription<N>, ClientError>
    where
        Params: ToRpcParams + Send,
        N: DeserializeOwned,
    {
        let raw = params.to_rpc_params()?;
        self.inner
            .run(|client| {
                let raw = raw.clone();
                async move {
                    client
                        .subscribe(subscribe_method, RawParams(raw), unsubscribe_method)
                        .await
                }
            })
            .await
    }

    async fn subscribe_to_method<N>(&self, method: &str) -> Result<Subscription<N>, ClientError>
    where
        N: DeserializeOwned,
    {
        self.inner
            .run(|client| async move { client.subscribe_to_method(method).await })
            .await
    }
}

impl fmt::Debug for FailoverClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FailoverClient { .. }")
    }
}

// Integration tests against a live node, mirroring the gRPC failover tests in
// `celestia-grpc`. The deterministic state-machine coverage lives in the shared
// engine's unit tests (`lumina_utils::failover`); these check the JSON-RPC
// adapter end-to-end: requests fail over from an unreachable endpoint to a real
// one, and exhausting all endpoints surfaces an error.
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::HeaderClient;

    // An endpoint with nothing listening, so connecting fails fast.
    const BAD_URL: &str = "ws://localhost:19999";
    const BAD_URL_2: &str = "ws://localhost:19998";

    /// The live node RPC url, overridable via `CELESTIA_RPC_URL` (node-2, the
    /// light node, allows unauthenticated header reads).
    fn celestia_rpc_url() -> String {
        std::env::var("CELESTIA_RPC_URL").unwrap_or_else(|_| "ws://localhost:46658".to_string())
    }

    fn failover_client(endpoints: Vec<RpcEndpoint>) -> FailoverClient {
        FailoverClient::new(endpoints, None, None, None, DEFAULT_MAX_HEAD_AGE).unwrap()
    }

    #[tokio::test]
    async fn failover_to_second_endpoint() {
        // First endpoint is unreachable, should fail over to the second, real one.
        let client = failover_client(vec![
            RpcEndpoint::new(BAD_URL),
            RpcEndpoint::new(celestia_rpc_url()),
        ]);

        let head = client.header_network_head().await.unwrap();
        assert!(head.height() > 0);
    }

    #[tokio::test]
    async fn endpoint_multiple_requests() {
        let client = failover_client(vec![
            RpcEndpoint::new(BAD_URL),
            RpcEndpoint::new(celestia_rpc_url()),
        ]);

        for _ in 0..5 {
            let head = client.header_network_head().await.unwrap();
            assert!(head.height() > 0);
        }
    }

    #[tokio::test]
    async fn all_endpoints_fail_returns_error() {
        let client = failover_client(vec![RpcEndpoint::new(BAD_URL), RpcEndpoint::new(BAD_URL_2)]);

        client.header_network_head().await.unwrap_err();
    }
}
