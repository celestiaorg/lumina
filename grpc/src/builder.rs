use arc_swap::ArcSwap;
use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use k256::ecdsa::{SigningKey, VerifyingKey};
use signature::Keypair;
use tonic::body::Body as TonicBody;
use tonic::codegen::Service;
use tonic::metadata::MetadataMap;
use zeroize::Zeroizing;

use crate::boxed::{BoxedTransport, TransportMetadata, boxed};
use crate::client::AccountState;
use crate::grpc::Context;
use crate::signer::BoxedDocSigner;
use crate::utils::CondSend;
use crate::{DocSigner, GrpcClient, GrpcClientBuilderError};

use imp::build_transport;

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

    pub(crate) fn into_parts(self) -> Result<(String, Context), GrpcClientBuilderError> {
        let mut context = Context {
            timeout: self.timeout,
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

    /// Build [`GrpcClient`]
    ///
    /// Returns error if no transports were configured.
    pub fn build(self) -> Result<GrpcClient, GrpcClientBuilderError> {
        if self.transports.is_empty() {
            return Err(GrpcClientBuilderError::TransportNotSet);
        }

        let base_context = Context {
            timeout: self.timeout,
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
                    build_transport(url, context)
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

        let transports = Arc::new(ArcSwap::from_pointee(transports));

        let signer_config = self.signer_kind.map(TryInto::try_into).transpose()?;

        Ok(GrpcClient::new(transports, signer_config))
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

        let channel = TonicEndpoint::from_shared(url.clone())?
            .user_agent("celestia-grpc")?
            .tls_config(tls_config)?
            .connect_lazy();

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

        let channel = TonicEndpoint::from_shared(url.clone())?
            .user_agent("celestia-grpc")?
            .connect_lazy();

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
    }

    #[test]
    fn endpoint_from_string_uses_default_config() {
        let endpoint: Endpoint = String::from("http://localhost:9090").into();

        assert_eq!(endpoint.url, "http://localhost:9090");
        assert!(endpoint.ascii_metadata.is_empty());
        assert!(endpoint.binary_metadata.is_empty());
        assert!(endpoint.metadata_map.is_none());
        assert!(endpoint.timeout.is_none());
    }
}
