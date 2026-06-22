use std::error::Error as StdError;
use std::fmt::{self, Debug};
use std::sync::Arc;
use std::time::Duration;

use blockstore::cond_send::CondSend;
pub use celestia_grpc::Endpoint;
use celestia_grpc::{GrpcClient, GrpcClientBuilder};
pub use celestia_rpc::RpcEndpoint;
use celestia_rpc::{FailoverClient, HeaderClient};
use http::Request;
use tonic::body::Body as TonicBody;
use tonic::codegen::{Bytes, Service};

use crate::blob::BlobApi;
use crate::blobstream::BlobstreamApi;
use crate::fraud::FraudApi;
use crate::header::HeaderApi;
use crate::share::ShareApi;
use crate::state::StateApi;
use crate::tx::{DocSigner, Keypair, VerifyingKey};
use crate::types::ExtendedHeader;
use crate::types::state::AccAddress;
use crate::{Error, Result};

/// A high-level client for interacting with a Celestia node.
///
/// There are two modes: read-only mode and submit mode. Read-only mode requires
/// RPC and optionally gRPC endpoint, while submit mode requires both, plus a signer.
///
/// # Examples
///
/// Read-only mode:
///
/// ```no_run
/// # use celestia_client::{Client, Result};
/// # async fn docs() -> Result<()> {
/// let client = Client::builder()
///     .rpc_url("ws://localhost:26658")
///     .grpc_url("http://localhost:9090") // optional in read-only mode
///     .build()
///     .await?;
///
/// client.header().head().await?;
/// # Ok(())
/// # }
/// ```
///
/// Submit mode:
///
/// ```no_run
/// # use celestia_client::{Client, Endpoint, Result};
/// # use celestia_client::tx::TxConfig;
/// # use std::time::Duration;
/// # async fn docs() -> Result<()> {
/// let endpoint = Endpoint::from("http://localhost:9090")
///     .metadata("x-token", "auth-token")
///     .timeout(Duration::from_secs(30));
///
/// let client = Client::builder()
///     .rpc_url("ws://localhost:26658")
///     .grpc_endpoint(endpoint)
///     .private_key_hex("393fdb5def075819de55756b45c9e2c8531a8c78dd6eede483d3440e9457d839")
///     .build()
///     .await?;
///
/// let to_address = "celestia169s50psyj2f4la9a2235329xz7rk6c53zhw9mm".parse().unwrap();
/// client.state().transfer(&to_address, 12345, TxConfig::default()).await?;
/// # Ok(())
/// # }
/// ```
///
/// [`celestia-rpc`]: celestia_rpc
/// [`celestia-grpc`]: celestia_grpc
pub struct Client {
    inner: Arc<ClientInner>,
    state: StateApi,
    blob: BlobApi,
    header: HeaderApi,
    share: ShareApi,
    fraud: FraudApi,
    blobstream: BlobstreamApi,
    fibre: Option<crate::fibre::FibreApi>,
}

pub(crate) struct ClientInner {
    pub(crate) rpc: FailoverClient,
    grpc: Option<GrpcClient>,
    pubkey: Option<VerifyingKey>,
    chain_id: tendermint::chain::Id,
}

/// A builder for [`Client`].
#[derive(Default)]
pub struct ClientBuilder {
    rpc_url: Option<String>,
    rpc_auth_token: Option<String>,
    rpc_endpoints: Vec<RpcEndpoint>,
    rpc_health_check_interval: Option<Duration>,
    rpc_max_head_age: Option<Duration>,
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    grpc_builder: Option<GrpcClientBuilder>,
    fibre_client: Option<celestia_fibre::FibreClient>,
}

impl fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("rpc_url", &self.rpc_url)
            .field(
                "rpc_auth_token",
                &self.rpc_auth_token.as_ref().map(|_| "***"),
            )
            .field(
                "rpc_endpoints",
                &self
                    .rpc_endpoints
                    .iter()
                    .map(|e| &e.url)
                    .collect::<Vec<_>>(),
            )
            .field("rpc_health_check_interval", &self.rpc_health_check_interval)
            .field("rpc_max_head_age", &self.rpc_max_head_age)
            .field("timeout", &self.timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("grpc_builder", &self.grpc_builder)
            .field(
                "fibre_client",
                &self.fibre_client.as_ref().map(|_| "FibreClient { .. }"),
            )
            .finish()
    }
}

impl ClientInner {
    pub(crate) fn grpc(&self) -> Result<&GrpcClient> {
        self.grpc.as_ref().ok_or(Error::GrpcEndpointNotSet)
    }

    pub(crate) fn pubkey(&self) -> Result<&VerifyingKey> {
        self.pubkey.as_ref().ok_or(Error::NoAssociatedAddress)
    }

    pub(crate) fn address(&self) -> Result<AccAddress> {
        let pubkey = self.pubkey()?.to_owned();
        Ok(AccAddress::new(pubkey.into()))
    }

    pub(crate) async fn get_header_validated(&self, height: u64) -> Result<ExtendedHeader> {
        let header = self.rpc.header_get_by_height(height).await?;
        header.validate()?;
        Ok(header)
    }
}

impl Client {
    /// Returns `ClientBuilder`.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Returns chain id of the network.
    pub fn chain_id(&self) -> &tendermint::chain::Id {
        &self.inner.chain_id
    }

    /// Returns the public key of the signer.
    pub fn pubkey(&self) -> Result<VerifyingKey> {
        self.inner.pubkey().cloned()
    }

    /// Returns the address of signer.
    pub fn address(&self) -> Result<AccAddress> {
        self.inner.address()
    }

    /// Returns state API accessor.
    pub fn state(&self) -> &StateApi {
        &self.state
    }

    /// Returns blob API accessor.
    pub fn blob(&self) -> &BlobApi {
        &self.blob
    }

    /// Returns blobstream API accessor.
    pub fn blobstream(&self) -> &BlobstreamApi {
        &self.blobstream
    }

    /// Returns header API accessor.
    pub fn header(&self) -> &HeaderApi {
        &self.header
    }

    /// Returns share API accessor.
    pub fn share(&self) -> &ShareApi {
        &self.share
    }

    /// Returns fraud API accessor.
    pub fn fraud(&self) -> &FraudApi {
        &self.fraud
    }

    /// Returns fibre API accessor.
    ///
    /// Returns `None` if the client was not configured with a fibre client.
    /// Use [`ClientBuilder::fibre_client()`] to provide one.
    pub fn fibre(&self) -> Option<&crate::fibre::FibreApi> {
        self.fibre.as_ref()
    }
}

impl ClientBuilder {
    /// Returns a new builder.
    pub fn new() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Set signer and its public key.
    pub fn signer<S>(mut self, pubkey: VerifyingKey, signer: S) -> ClientBuilder
    where
        S: DocSigner + Sync + Send + 'static,
    {
        let grpc_builder = self.grpc_builder.unwrap_or_default();
        self.grpc_builder = Some(grpc_builder.pubkey_and_signer(pubkey, signer));
        self
    }

    /// Set signer from a keypair.
    pub fn keypair<S>(mut self, keypair: S) -> ClientBuilder
    where
        S: DocSigner + Keypair<VerifyingKey = VerifyingKey> + Sync + Send + 'static,
    {
        let grpc_builder = self.grpc_builder.unwrap_or_default();
        self.grpc_builder = Some(grpc_builder.signer_keypair(keypair));
        self
    }

    /// Set signer from a raw private key.
    pub fn private_key(mut self, bytes: &[u8]) -> ClientBuilder {
        let grpc_builder = self.grpc_builder.unwrap_or_default();
        self.grpc_builder = Some(grpc_builder.private_key(bytes));
        self
    }

    /// Set signer from a hex formatted private key.
    pub fn private_key_hex(mut self, s: &str) -> ClientBuilder {
        let grpc_builder = self.grpc_builder.unwrap_or_default();
        self.grpc_builder = Some(grpc_builder.private_key_hex(s));
        self
    }

    /// Set the RPC endpoint.
    pub fn rpc_url(mut self, url: &str) -> ClientBuilder {
        self.rpc_url = Some(url.to_owned());
        self
    }

    /// Set the authentication token of RPC endpoint.
    pub fn rpc_auth_token(mut self, auth_token: &str) -> ClientBuilder {
        self.rpc_auth_token = Some(auth_token.to_owned());
        self
    }

    /// Add an RPC endpoint for failover.
    ///
    /// Accepts [`RpcEndpoint`], `&str`, or `String`. Use [`RpcEndpoint`] when an
    /// endpoint needs its own authentication token (e.g. a fallback provider
    /// such as QuickNode that uses a different token than your own node).
    ///
    /// Endpoints are tried in priority order: the first endpoint added (or the
    /// one set via [`rpc_url`](ClientBuilder::rpc_url)) is the most preferred.
    /// When the preferred endpoint fails, the client automatically falls back to
    /// the next one, and a background health-check switches back to the preferred
    /// endpoint once it recovers.
    pub fn rpc_endpoint(mut self, endpoint: impl Into<RpcEndpoint>) -> ClientBuilder {
        self.rpc_endpoints.push(endpoint.into());
        self
    }

    /// Add multiple RPC endpoints at once for failover.
    ///
    /// Accepts [`RpcEndpoint`], `&str`, or `String` items. See
    /// [`rpc_endpoint`](ClientBuilder::rpc_endpoint) for the failover semantics.
    pub fn rpc_endpoints<I, E>(mut self, endpoints: I) -> ClientBuilder
    where
        I: IntoIterator<Item = E>,
        E: Into<RpcEndpoint>,
    {
        self.rpc_endpoints
            .extend(endpoints.into_iter().map(Into::into));
        self
    }

    /// Set the interval at which the preferred RPC endpoint is probed for
    /// recovery while a fallback is in use.
    ///
    /// Only has an effect when more than one RPC endpoint is configured.
    /// Defaults to [`celestia_rpc::DEFAULT_HEALTH_CHECK_INTERVAL`].
    pub fn rpc_health_check_interval(mut self, interval: Duration) -> ClientBuilder {
        self.rpc_health_check_interval = Some(interval);
        self
    }

    /// Set the maximum age of the preferred RPC endpoint's head for it to be
    /// considered healthy enough to switch back to.
    ///
    /// While a fallback is in use, the background health-check only switches back
    /// to the preferred endpoint once its head is no older than this (i.e. it has
    /// caught up to the chain tip). Defaults to
    /// [`celestia_rpc::DEFAULT_MAX_HEAD_AGE`].
    pub fn rpc_max_head_age(mut self, max_head_age: Duration) -> ClientBuilder {
        self.rpc_max_head_age = Some(max_head_age);
        self
    }

    /// Set the request timeout for both RPC and gRPC endpoints.
    ///
    /// This bounds the duration of individual requests only. To bound how long
    /// connection establishment may take, use [`ClientBuilder::connect_timeout`].
    ///
    /// # Note
    ///
    /// Unlike previous versions, this no longer doubles as the WebSocket RPC
    /// connection timeout; that is now controlled solely by
    /// [`ClientBuilder::connect_timeout`].
    pub fn timeout(mut self, timeout: Duration) -> ClientBuilder {
        self.timeout = Some(timeout);
        self
    }

    /// Set the connection establishment timeout for RPC (WebSocket) and gRPC endpoints.
    ///
    /// This is independent of [`ClientBuilder::timeout`] (the request timeout): a low
    /// connect timeout lets a down/unreachable host be detected faster than a merely
    /// slow one.
    ///
    /// # Note
    ///
    /// Not supported for HTTP(S) RPC, WASM RPC, or WASM (gRPC-Web) gRPC transports,
    /// where it is ignored.
    pub fn connect_timeout(mut self, connect_timeout: Duration) -> ClientBuilder {
        self.connect_timeout = Some(connect_timeout);
        self
    }

    /// Set the gRPC endpoint.
    ///
    /// Alias of [`ClientBuilder::grpc_endpoint`].
    ///
    /// Accepts `Endpoint`, `&str`, or `String`.
    ///
    /// # Note
    ///
    /// In WASM the endpoint needs to support gRPC-Web.
    pub fn grpc_url(self, url: impl Into<Endpoint>) -> ClientBuilder {
        self.grpc_endpoint(url)
    }

    /// Set the gRPC endpoint.
    pub fn grpc_endpoint(mut self, endpoint: impl Into<Endpoint>) -> ClientBuilder {
        let grpc_builder = self.grpc_builder.unwrap_or_default();
        self.grpc_builder = Some(grpc_builder.endpoint(endpoint));
        self
    }

    /// Add multiple gRPC endpoints at once for fallback support.
    ///
    /// Accepts `Endpoint`, `&str`, or `String` items.
    ///
    /// When multiple endpoints are configured, the client will automatically
    /// fall back to the next endpoint if a network-related error occurs.
    ///
    /// # Note
    ///
    /// In WASM the endpoints need to support gRPC-Web.
    pub fn grpc_endpoints<I, E>(mut self, endpoints: I) -> ClientBuilder
    where
        I: IntoIterator<Item = E>,
        E: Into<Endpoint>,
    {
        let grpc_builder = self.grpc_builder.unwrap_or_default();
        self.grpc_builder = Some(grpc_builder.endpoints(endpoints));
        self
    }

    /// Add multiple gRPC endpoints. Alias of [`ClientBuilder::grpc_endpoints`].
    pub fn grpc_urls<I, E>(self, urls: I) -> ClientBuilder
    where
        I: IntoIterator<Item = E>,
        E: Into<Endpoint>,
    {
        self.grpc_endpoints(urls)
    }

    /// Set manually configured gRPC transport
    pub fn grpc_transport<B, T>(mut self, transport: T) -> Self
    where
        B: http_body::Body<Data = Bytes> + Send + Unpin + 'static,
        <B as http_body::Body>::Error: StdError + Send + Sync,
        T: Service<Request<TonicBody>, Response = http::Response<B>>
            + Send
            + Sync
            + Clone
            + 'static,
        <T as Service<Request<TonicBody>>>::Error: StdError + Send + Sync + 'static,
        <T as Service<Request<TonicBody>>>::Future: CondSend + 'static,
    {
        let grpc_builder = self.grpc_builder.unwrap_or_default();
        self.grpc_builder = Some(grpc_builder.transport(transport));
        self
    }

    /// Set a pre-built [`FibreClient`](celestia_fibre::FibreClient) for the Fibre
    /// data availability protocol.
    ///
    /// When set, the client exposes a [`FibreApi`](crate::fibre::FibreApi) via
    /// [`Client::fibre()`].
    ///
    /// Use [`FibreClient::from_endpoint()`](celestia_fibre::FibreClient::from_endpoint) for
    /// convenient construction from a gRPC endpoint URL.
    pub fn fibre_client(mut self, fibre_client: celestia_fibre::FibreClient) -> Self {
        self.fibre_client = Some(fibre_client);
        self
    }

    /// Build [`Client`].
    pub async fn build(self) -> Result<Client> {
        // The endpoint set via `rpc_url` (with its optional `rpc_auth_token`) is
        // the most preferred one; any endpoints added via `rpc_endpoint(s)`
        // follow it as fallbacks.
        let mut rpc_endpoints = Vec::with_capacity(self.rpc_endpoints.len() + 1);
        if let Some(url) = self.rpc_url {
            let mut endpoint = RpcEndpoint::new(url);
            if let Some(token) = self.rpc_auth_token {
                endpoint = endpoint.auth_token(token);
            }
            rpc_endpoints.push(endpoint);
        }
        rpc_endpoints.extend(self.rpc_endpoints);

        if rpc_endpoints.is_empty() {
            return Err(Error::RpcEndpointNotSet);
        }

        let (grpc, pubkey) = if let Some(mut grpc_builder) = self.grpc_builder {
            if let Some(timeout) = self.timeout {
                grpc_builder = grpc_builder.timeout(timeout)
            };
            if let Some(connect_timeout) = self.connect_timeout {
                grpc_builder = grpc_builder.connect_timeout(connect_timeout)
            };
            let client = grpc_builder.build()?;
            let pubkey = client.get_account_pubkey();
            (Some(client), pubkey)
        } else {
            (None, None)
        };

        // Only run the background health-check when there is a preferred endpoint
        // to switch back to.
        let health_check_interval = (rpc_endpoints.len() > 1).then(|| {
            self.rpc_health_check_interval
                .unwrap_or(celestia_rpc::DEFAULT_HEALTH_CHECK_INTERVAL)
        });

        let max_head_age = self
            .rpc_max_head_age
            .unwrap_or(celestia_rpc::DEFAULT_MAX_HEAD_AGE);

        let rpc = FailoverClient::new(
            rpc_endpoints,
            self.connect_timeout,
            self.timeout,
            health_check_interval,
            max_head_age,
        )?;

        let head = rpc.header_network_head().await?;
        head.validate()?;

        if let Some(grpc) = &grpc
            && &grpc.chain_id().await? != head.chain_id()
        {
            return Err(Error::ChainIdMissmatch);
        }

        let inner = Arc::new(ClientInner {
            rpc,
            grpc,
            pubkey,
            chain_id: head.chain_id().to_owned(),
        });

        let fibre = self
            .fibre_client
            .map(|fc| crate::fibre::FibreApi::new(inner.clone(), fc));

        Ok(Client {
            inner: inner.clone(),
            blob: BlobApi::new(inner.clone()),
            header: HeaderApi::new(inner.clone()),
            share: ShareApi::new(inner.clone()),
            fraud: FraudApi::new(inner.clone()),
            blobstream: BlobstreamApi::new(inner.clone()),
            state: StateApi::new(inner.clone()),
            fibre,
        })
    }
}

impl Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Client { .. }")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use lumina_utils::test_utils::async_test;

    use crate::test_utils::{TEST_PRIV_KEY, TEST_RPC_URL};

    #[async_test]
    async fn builder() {
        let e = Client::builder()
            .rpc_url(TEST_RPC_URL)
            .private_key_hex(TEST_PRIV_KEY)
            .build()
            .await
            .unwrap_err();
        assert!(matches!(e, Error::GrpcEndpointNotSet))
    }

    #[test]
    fn connect_timeout_is_independent_of_timeout() {
        // opt-in: defaults to None
        assert!(ClientBuilder::default().connect_timeout.is_none());

        // request timeout does not set the connect timeout
        let builder = ClientBuilder::default().timeout(Duration::from_secs(30));
        assert_eq!(builder.timeout, Some(Duration::from_secs(30)));
        assert!(builder.connect_timeout.is_none());

        // connect timeout is set independently
        let builder = ClientBuilder::default().connect_timeout(Duration::from_secs(2));
        assert_eq!(builder.connect_timeout, Some(Duration::from_secs(2)));
        assert!(builder.timeout.is_none());
    }
}
