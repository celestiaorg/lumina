use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use k256::ecdsa::{SigningKey, VerifyingKey};
use lumina_utils::failover::{Endpoint as FailoverEndpoint, Failover};
use signature::Keypair;
use tonic::body::Body as TonicBody;
use tonic::codegen::Service;
use tonic::metadata::MetadataMap;
use zeroize::Zeroizing;

use crate::boxed::{BoxedTransport, TransportMetadata, boxed};
use crate::client::{AccountState, probe_head};
use crate::grpc::Context;
use crate::signer::{AccountSigner, BoxedDocSigner};
use crate::utils::CondSend;
use crate::{DocSigner, Error, GrpcClient, GrpcClientBuilderError};

/// Default interval between background health-checks of unhealthy endpoints.
const DEFAULT_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(3);

/// Default timeout for a single gRPC health-check probe.
const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Default maximum head age for an endpoint to be considered healthy enough to
/// switch back to.
const DEFAULT_MAX_HEAD_AGE: Duration = Duration::from_secs(30);

/// A URL endpoint with per-endpoint configuration.
///
/// This includes HTTP/2 headers (metadata) and timeout that will be applied
/// to all requests made to this endpoint.
///
/// # Example
///
/// ```no_run
/// use std::time::Duration;
/// use celestia_grpc::{Endpoint, GrpcClient};
///
/// let client = GrpcClient::builder()
///     .url(Endpoint::from("http://localhost:9090")
///         .metadata("authorization", "Bearer token")
///         .timeout(Duration::from_secs(30)))
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct Endpoint {
    /// The endpoint URL.
    pub url: String,
    /// ASCII metadata (HTTP/2 headers) as key-value pairs.
    ascii_metadata: Vec<(String, String)>,
    /// Binary metadata as key-value pairs (keys must have `-bin` suffix).
    binary_metadata: Vec<(String, Vec<u8>)>,
    /// Pre-built metadata map.
    metadata_map: Option<MetadataMap>,
    /// Request timeout for this endpoint.
    timeout: Option<Duration>,
    /// Connection establishment timeout for this endpoint.
    connect_timeout: Option<Duration>,
}

impl Endpoint {
    /// Create a new endpoint with a default configuration.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ascii_metadata: Vec::new(),
            binary_metadata: Vec::new(),
            metadata_map: None,
            timeout: None,
            connect_timeout: None,
        }
    }

    /// Appends ASCII metadata (HTTP/2 header) to requests made to this endpoint.
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.ascii_metadata.push((key.into(), value.into()));
        self
    }

    /// Appends binary metadata to requests made to this endpoint.
    ///
    /// Keys must have `-bin` suffix. Values are base64-encoded on the wire.
    pub fn metadata_bin(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.binary_metadata.push((key.into(), value.into()));
        self
    }

    /// Sets a metadata map for this endpoint.
    pub fn metadata_map(mut self, metadata: MetadataMap) -> Self {
        self.metadata_map = Some(metadata);
        self
    }

    /// Sets the request timeout for this endpoint.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the connection establishment (TCP/TLS) timeout for this endpoint.
    ///
    /// This is independent of the request timeout: a low value lets a
    /// down/unreachable host fail fast, distinct from a slow-but-reachable one.
    /// Not supported on WASM (gRPC-Web) transports, where it is ignored.
    pub fn connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = Some(connect_timeout);
        self
    }

    pub(crate) fn into_parts(self) -> Result<(String, Context), GrpcClientBuilderError> {
        let mut context = Context {
            timeout: self.timeout,
            connect_timeout: self.connect_timeout,
            ..Default::default()
        };
        for (key, value) in self.ascii_metadata {
            context.append_metadata(&key, &value)?;
        }
        for (key, value) in self.binary_metadata {
            context.append_metadata_bin(&key, &value)?;
        }
        if let Some(metadata) = self.metadata_map {
            context.append_metadata_map(&metadata);
        }
        Ok((self.url, context))
    }
}

impl<S> From<S> for Endpoint
where
    S: Into<String>,
{
    fn from(url: S) -> Self {
        Endpoint::new(url)
    }
}

enum TransportEntry {
    Endpoint(Endpoint),
    BoxedTransport(BoxedTransport),
}

/// Builder for [`GrpcClient`]
///
/// Note that TLS configuration is governed using `tls-*-roots` feature flags.
#[derive(Default)]
pub struct GrpcClientBuilder {
    transports: Vec<TransportEntry>,
    timeout: Option<Duration>,
    connect_timeout: Option<Duration>,
    health_check_interval: Option<Duration>,
    max_head_age: Option<Duration>,
    signer_kind: Option<SignerKind>,
}

enum SignerKind {
    Signer((VerifyingKey, BoxedDocSigner)),
    PrivKeyBytes(Zeroizing<Vec<u8>>),
    PrivKeyHex(Zeroizing<String>),
}

impl GrpcClientBuilder {
    /// Create a new, empty builder.
    pub fn new() -> Self {
        GrpcClientBuilder::default()
    }

    /// Add a URL endpoint. Multiple calls add multiple fallback endpoints.
    ///
    /// This is an alias of [`GrpcClientBuilder::endpoint`].
    ///
    /// When multiple endpoints are configured, the client will automatically
    /// fall back to the next endpoint if a network-related error occurs.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celestia_grpc::GrpcClient;
    ///
    /// let client = GrpcClient::builder()
    ///     .url("http://primary:9090")
    ///     .url("http://fallback:9090")
    ///     .build();
    /// ```
    pub fn url(self, endpoint: impl Into<Endpoint>) -> Self {
        self.endpoint(endpoint)
    }

    /// Add a URL endpoint. This is the primary entry point for endpoints.
    pub fn endpoint(mut self, endpoint: impl Into<Endpoint>) -> Self {
        self.transports
            .push(TransportEntry::Endpoint(endpoint.into()));
        self
    }

    /// Add multiple URL endpoints.
    ///
    /// When multiple endpoints are configured, the client will automatically
    /// fall back to the next endpoint if a network-related error occurs.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celestia_grpc::GrpcClient;
    ///
    /// let client = GrpcClient::builder()
    ///     .urls(["http://primary:9090", "http://fallback:9090"])
    ///     .build();
    /// ```
    pub fn endpoints<I, E>(mut self, endpoints: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: Into<Endpoint>,
    {
        for endpoint in endpoints {
            self = self.endpoint(endpoint);
        }
        self
    }

    /// Add multiple URL endpoints. This is an alias of [`GrpcClientBuilder::endpoints`].
    pub fn urls<I, E>(self, urls: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: Into<Endpoint>,
    {
        self.endpoints(urls)
    }

    /// Add a custom transport endpoint. Multiple calls add multiple fallback endpoints.
    ///
    /// When multiple endpoints are configured, the client will automatically
    /// fall back to the next endpoint if a network-related error occurs.
    ///
    /// Note: any configured connect timeout does not apply to custom transports,
    /// since the caller owns the channel.
    pub fn transport<B, T>(mut self, transport: T) -> Self
    where
        B: http_body::Body<Data = Bytes> + Send + Unpin + 'static,
        <B as http_body::Body>::Error: StdError + Send + Sync,
        T: Service<http::Request<TonicBody>, Response = http::Response<B>>
            + Send
            + Sync
            + Clone
            + 'static,
        <T as Service<http::Request<TonicBody>>>::Error: StdError + Send + Sync + 'static,
        <T as Service<http::Request<TonicBody>>>::Future: CondSend + 'static,
    {
        self.transports.push(TransportEntry::BoxedTransport(boxed(
            transport,
            TransportMetadata::default(),
        )));
        self
    }

    /// Add signer and a public key
    pub fn pubkey_and_signer<S>(
        mut self,
        account_pubkey: VerifyingKey,
        signer: S,
    ) -> GrpcClientBuilder
    where
        S: DocSigner + 'static,
    {
        let signer = BoxedDocSigner::new(signer);
        self.signer_kind = Some(SignerKind::Signer((account_pubkey, signer)));
        self
    }

    /// Add signer and associated public key
    pub fn signer_keypair<S>(self, signer: S) -> GrpcClientBuilder
    where
        S: DocSigner + Keypair<VerifyingKey = VerifyingKey> + 'static,
    {
        let pubkey = signer.verifying_key();
        self.pubkey_and_signer(pubkey, signer)
    }

    /// Add signer from an existing [`AccountSigner`].
    pub fn account_signer(mut self, signer: AccountSigner) -> GrpcClientBuilder {
        self.signer_kind = Some(SignerKind::Signer((signer.pubkey, signer.signer)));
        self
    }

    /// Set signer from a raw private key.
    pub fn private_key(mut self, bytes: &[u8]) -> GrpcClientBuilder {
        self.signer_kind = Some(SignerKind::PrivKeyBytes(Zeroizing::new(bytes.to_vec())));
        self
    }

    /// Set signer from a hex formatted private key.
    pub fn private_key_hex(mut self, s: &str) -> GrpcClientBuilder {
        self.signer_kind = Some(SignerKind::PrivKeyHex(Zeroizing::new(s.to_string())));
        self
    }

    /// Sets the request timeout, overriding default one from the transport.
    pub fn timeout(mut self, timeout: Duration) -> GrpcClientBuilder {
        self.timeout = Some(timeout);
        self
    }

    /// Sets the connection establishment (TCP/TLS) timeout for all endpoints.
    ///
    /// This is independent of the request timeout: a low value lets a
    /// down/unreachable host fail fast, distinct from a slow-but-reachable one.
    /// An individual [`Endpoint`] may override this via [`Endpoint::connect_timeout`].
    /// Ignored for custom transports and on WASM (gRPC-Web).
    pub fn connect_timeout(mut self, connect_timeout: Duration) -> GrpcClientBuilder {
        self.connect_timeout = Some(connect_timeout);
        self
    }

    /// Override the interval of the background health-check that switches back to
    /// the preferred endpoint once it recovers.
    ///
    /// Endpoints are tried in priority order (the first added is the most
    /// preferred). When a network error makes the client fail over to a
    /// fallback, this task probes the preferred endpoint on the given interval
    /// and switches back to it once it has recovered and its head is recent
    /// enough (within [`max_head_age`](GrpcClientBuilder::max_head_age)).
    ///
    /// The health-check is enabled by default (every 10s) whenever more than one
    /// endpoint is configured; use this only to change the interval.
    pub fn health_check_interval(mut self, interval: Duration) -> GrpcClientBuilder {
        self.health_check_interval = Some(interval);
        self
    }

    /// Set the maximum age of the preferred endpoint's head (by timestamp) for
    /// it to be considered healthy enough to switch back to.
    ///
    /// While a fallback is in use, the background health-check only switches back
    /// to the preferred endpoint once its head is no older than this. Defaults to
    /// 30 seconds. Only relevant when
    /// [`health_check_interval`](GrpcClientBuilder::health_check_interval) is set.
    pub fn max_head_age(mut self, max_head_age: Duration) -> GrpcClientBuilder {
        self.max_head_age = Some(max_head_age);
        self
    }

    /// Build [`GrpcClient`]
    ///
    /// Returns error if no transports were configured.
    pub fn build(self) -> Result<GrpcClient, GrpcClientBuilderError> {
        if self.transports.is_empty() {
            return Err(GrpcClientBuilderError::TransportNotSet);
        }

        let base_context = Context {
            timeout: self.timeout,
            connect_timeout: self.connect_timeout,
            ..Default::default()
        };

        let transports: Vec<BoxedTransport> = self
            .transports
            .into_iter()
            .map(|entry| match entry {
                TransportEntry::Endpoint(endpoint) => {
                    let (url, endpoint_context) = endpoint.into_parts()?;
                    let mut context = base_context.clone();
                    context.extend(&endpoint_context);
                    imp::build_transport(url, context)
                }
                TransportEntry::BoxedTransport(mut transport) => {
                    let mut context = base_context.clone();
                    context.extend(&transport.metadata.context);
                    transport.metadata = Arc::new(TransportMetadata {
                        url: transport.metadata.url.clone(),
                        context,
                    });
                    Ok(transport)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        // gRPC transports are built eagerly (lazily-connecting channels), so each
        // endpoint slot is pre-cached and never rebuilt.
        let endpoints = transports
            .into_iter()
            .map(|transport| {
                let label = transport.metadata.url.clone().unwrap_or_default();
                FailoverEndpoint::prebuilt(label, transport)
            })
            .collect();

        let probe_timeout = self.timeout.unwrap_or(DEFAULT_PROBE_TIMEOUT);
        let max_head_age = self.max_head_age.unwrap_or(DEFAULT_MAX_HEAD_AGE);
        let failover = Failover::new(
            endpoints,
            Error::is_network_error,
            probe_timeout,
            max_head_age,
        )
        .expect("transports were checked to be non-empty");
        let failover = Arc::new(failover);

        // Enabled by default; `spawn_health_check` is a no-op for a single
        // endpoint, so this only starts a task when there's a fallback.
        let health_check_interval = self
            .health_check_interval
            .unwrap_or(DEFAULT_HEALTH_CHECK_INTERVAL);
        failover.spawn_health_check(
            health_check_interval,
            |transport: Arc<BoxedTransport>| async move { probe_head(transport).await },
        );

        let signer_config = self.signer_kind.map(TryInto::try_into).transpose()?;

        Ok(GrpcClient::new(failover, signer_config))
    }
}

impl TryFrom<SignerKind> for AccountState {
    type Error = GrpcClientBuilderError;

    fn try_from(value: SignerKind) -> Result<Self, Self::Error> {
        match value {
            SignerKind::Signer((pubkey, signer)) => Ok(AccountState::new(pubkey, signer)),
            SignerKind::PrivKeyBytes(bytes) => priv_key_signer(&bytes),
            SignerKind::PrivKeyHex(string) => {
                let bytes = Zeroizing::new(
                    hex::decode(string.trim())
                        .map_err(|_| GrpcClientBuilderError::InvalidPrivateKey)?,
                );
                priv_key_signer(&bytes)
            }
        }
    }
}

fn priv_key_signer(bytes: &[u8]) -> Result<AccountState, GrpcClientBuilderError> {
    let signing_key =
        SigningKey::from_slice(bytes).map_err(|_| GrpcClientBuilderError::InvalidPrivateKey)?;
    let pubkey = signing_key.verifying_key().to_owned();
    let signer = BoxedDocSigner::new(signing_key);
    Ok(AccountState::new(pubkey, signer))
}

impl fmt::Debug for SignerKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SignerKind::Signer(..) => "SignerKind::Signer(..)",
            SignerKind::PrivKeyBytes(..) => "SignerKind::PrivKeyBytes(..)",
            SignerKind::PrivKeyHex(..) => "SignerKind::PrivKeyHex(..)",
        };
        f.write_str(s)
    }
}

impl fmt::Debug for GrpcClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GrpcClientBuilder { .. }")
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(any(feature = "tls-native-roots", feature = "tls-webpki-roots"))]
mod imp {
    use super::*;

    use tonic::transport::{ClientTlsConfig, Endpoint as TonicEndpoint};

    pub(super) fn build_transport(
        url: String,
        context: Context,
    ) -> Result<BoxedTransport, GrpcClientBuilderError> {
        let tls_config = ClientTlsConfig::new().with_enabled_roots();

        let mut endpoint = TonicEndpoint::from_shared(url.clone())?
            .user_agent("celestia-grpc")?
            .tls_config(tls_config)?;
        if let Some(connect_timeout) = context.connect_timeout {
            endpoint = endpoint.connect_timeout(connect_timeout);
        }
        let channel = endpoint.connect_lazy();

        Ok(boxed(
            channel,
            TransportMetadata::with_url_and_context(url, context),
        ))
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(any(feature = "tls-native-roots", feature = "tls-webpki-roots")))]
mod imp {
    use super::*;

    use tonic::transport::Endpoint as TonicEndpoint;

    pub(super) fn build_transport(
        url: String,
        context: Context,
    ) -> Result<BoxedTransport, GrpcClientBuilderError> {
        if url
            .split_once(':')
            .is_some_and(|(scheme, _)| scheme == "https")
        {
            return Err(GrpcClientBuilderError::TlsNotSupported);
        }

        let mut endpoint = TonicEndpoint::from_shared(url.clone())?.user_agent("celestia-grpc")?;
        if let Some(connect_timeout) = context.connect_timeout {
            endpoint = endpoint.connect_timeout(connect_timeout);
        }
        let channel = endpoint.connect_lazy();

        Ok(boxed(
            channel,
            TransportMetadata::with_url_and_context(url, context),
        ))
    }
}

#[cfg(target_arch = "wasm32")]
mod imp {
    use super::*;
    pub(super) fn build_transport(
        url: String,
        context: Context,
    ) -> Result<BoxedTransport, GrpcClientBuilderError> {
        // `context.connect_timeout` is unsupported on gRPC-Web (browser fetch has
        // no separate connection-establishment phase to bound) and is ignored here.
        let client = tonic_web_wasm_client::Client::new(url.clone());
        Ok(boxed(
            client,
            TransportMetadata::with_url_and_context(url, context),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lumina_utils::test_utils::async_test;

    #[test]
    fn empty_builder_returns_transport_not_set() {
        let result = GrpcClientBuilder::new().build();

        assert!(matches!(
            result,
            Err(GrpcClientBuilderError::TransportNotSet)
        ));
    }

    #[async_test]
    async fn single_url_builds_successfully() {
        let result = GrpcClientBuilder::new()
            .url("http://localhost:9090")
            .build();

        assert!(result.is_ok());
    }

    #[async_test]
    async fn multiple_urls_build_successfully() {
        let result = GrpcClientBuilder::new()
            .url("http://localhost:9090")
            .url("http://localhost:9091")
            .url("http://localhost:9092")
            .build();

        assert!(result.is_ok());
    }

    #[async_test]
    async fn endpoint_with_config_builds_successfully() {
        let result = GrpcClientBuilder::new()
            .endpoint(
                Endpoint::from("http://localhost:9090")
                    .metadata("authorization", "Bearer token")
                    .timeout(Duration::from_secs(30)),
            )
            .build();

        assert!(result.is_ok());
    }

    #[async_test]
    async fn urls_helper_builds_successfully() {
        let result = GrpcClientBuilder::new()
            .urls(["http://localhost:9090", "http://localhost:9091"])
            .build();

        assert!(result.is_ok());
    }

    #[async_test]
    async fn endpoints_helper_builds_successfully() {
        let result = GrpcClientBuilder::new()
            .endpoints([
                Endpoint::from("http://localhost:9090"),
                Endpoint::from("http://localhost:9091"),
            ])
            .build();

        assert!(result.is_ok());
    }

    #[test]
    fn endpoint_from_str_uses_default_config() {
        let endpoint: Endpoint = "http://localhost:9090".into();

        assert_eq!(endpoint.url, "http://localhost:9090");
        assert!(endpoint.ascii_metadata.is_empty());
        assert!(endpoint.binary_metadata.is_empty());
        assert!(endpoint.metadata_map.is_none());
        assert!(endpoint.timeout.is_none());
        assert!(endpoint.connect_timeout.is_none());
    }

    #[test]
    fn endpoint_from_string_uses_default_config() {
        let endpoint: Endpoint = String::from("http://localhost:9090").into();

        assert_eq!(endpoint.url, "http://localhost:9090");
        assert!(endpoint.ascii_metadata.is_empty());
        assert!(endpoint.binary_metadata.is_empty());
        assert!(endpoint.metadata_map.is_none());
        assert!(endpoint.timeout.is_none());
        assert!(endpoint.connect_timeout.is_none());
    }

    #[test]
    fn endpoint_connect_timeout_flows_into_context() {
        let connect_timeout = Duration::from_millis(250);
        let endpoint = Endpoint::from("http://localhost:9090").connect_timeout(connect_timeout);

        let (url, context) = endpoint.into_parts().unwrap();

        assert_eq!(url, "http://localhost:9090");
        assert_eq!(context.connect_timeout, Some(connect_timeout));
    }

    #[test]
    fn endpoint_connect_timeout_overrides_builder_wide() {
        let builder_wide = Duration::from_secs(5);
        let per_endpoint = Duration::from_millis(250);

        // builder-wide value with no per-endpoint override is used as-is
        let mut base = Context {
            connect_timeout: Some(builder_wide),
            ..Default::default()
        };
        let (_, plain) = Endpoint::from("http://localhost:9090")
            .into_parts()
            .unwrap();
        base.extend(&plain);
        assert_eq!(base.connect_timeout, Some(builder_wide));

        // per-endpoint value wins over the builder-wide one
        let mut base = Context {
            connect_timeout: Some(builder_wide),
            ..Default::default()
        };
        let (_, overridden) = Endpoint::from("http://localhost:9090")
            .connect_timeout(per_endpoint)
            .into_parts()
            .unwrap();
        base.extend(&overridden);
        assert_eq!(base.connect_timeout, Some(per_endpoint));
    }

    #[async_test]
    async fn builder_connect_timeout_builds_successfully() {
        let result = GrpcClientBuilder::new()
            .url("http://localhost:9090")
            .connect_timeout(Duration::from_millis(250))
            .build();

        assert!(result.is_ok());
    }
}
