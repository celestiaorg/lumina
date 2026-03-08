//! FibreClient -- the main entry point for the Fibre DA protocol.
//!
//! The [`FibreClient`] struct provides `upload()` for distributing blobs to
//! validators and collecting signatures, and (in Stage 4) `download()` for
//! retrieving and reconstructing blobs.
//!
//! Use [`FibreClientBuilder`] (via [`FibreClient::builder()`]) to construct
//! an instance.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use k256::ecdsa::SigningKey;

use celestia_grpc::GrpcClient;

use crate::config::FibreClientConfig;
use crate::error::FibreError;
use crate::validator::SetGetter;
use crate::validator_client::ValidatorConnector;

/// The Fibre DA client.
///
/// Provides `upload()` for distributing blobs to validators and collecting
/// signatures, and `download()` for retrieving and reconstructing blobs.
///
/// Constructed via [`FibreClientBuilder`].
pub struct FibreClient {
    pub(crate) cfg: FibreClientConfig,
    pub(crate) signing_key: SigningKey,
    pub(crate) set_getter: Arc<dyn SetGetter>,
    pub(crate) connector: Arc<dyn ValidatorConnector>,
    pub(crate) upload_semaphore: Arc<tokio::sync::Semaphore>,
    pub(crate) download_semaphore: Arc<tokio::sync::Semaphore>,
    pub(crate) closed: AtomicBool,
    pub(crate) grpc_client: Option<GrpcClient>,
}

impl FibreClient {
    /// Returns a new [`FibreClientBuilder`].
    pub fn builder() -> FibreClientBuilder {
        FibreClientBuilder::new()
    }

    /// Returns a reference to the client's configuration.
    pub fn config(&self) -> &FibreClientConfig {
        &self.cfg
    }

    /// Returns `true` if the client has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Mark the client as closed so that subsequent operations return
    /// [`FibreError::ClientClosed`].
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }

    /// Convenience constructor that builds a fully-wired [`FibreClient`] from a
    /// single gRPC endpoint URL.
    ///
    /// Creates a gRPC channel for chain queries (validator sets via CometBFT
    /// `BlockAPI` and host resolution via `x/valaddr`), wires up the production
    /// [`GrpcSetGetter`], [`GrpcHostRegistry`], and [`GrpcValidatorConnector`],
    /// and optionally takes a [`GrpcClient`] for broadcasting `MsgPayForFibre`
    /// transactions on-chain.
    ///
    /// # Arguments
    ///
    /// * `grpc_url` - The consensus node gRPC endpoint (e.g. `"http://localhost:9090"`).
    /// * `signing_key` - secp256k1 key for signing payment promises.
    /// * `grpc_client` - Optional [`GrpcClient`] for on-chain broadcast. Only
    ///   needed if you intend to call [`FibreClient::put()`].
    /// * `config` - Client configuration. Use [`FibreClientConfig::default()`] for defaults.
    pub fn from_url(
        grpc_url: &str,
        signing_key: SigningKey,
        grpc_client: Option<GrpcClient>,
        config: FibreClientConfig,
    ) -> Result<Self, FibreError> {
        let channel = celestia_grpc::connect_lazy(grpc_url)
            .map_err(|e| FibreError::Other(format!("invalid gRPC endpoint '{grpc_url}': {e}")))?;

        let host_registry = Arc::new(crate::host_registry::GrpcHostRegistry::new(channel.clone()));
        let connector = crate::grpc_validator_client::GrpcValidatorConnector::new(
            host_registry,
            config.max_message_size,
        );

        let mut builder = Self::builder()
            .config(config)
            .signing_key(signing_key)
            .set_getter(crate::validator::GrpcSetGetter::new(channel))
            .connector(connector);

        if let Some(client) = grpc_client {
            builder = builder.grpc_client(client);
        }

        builder.build()
    }
}

/// Builder for [`FibreClient`].
///
/// Required fields: `signing_key`, `set_getter`, `connector`.
/// Optional: `config` (defaults to [`FibreClientConfig::default()`]).
pub struct FibreClientBuilder {
    config: Option<FibreClientConfig>,
    signing_key: Option<SigningKey>,
    set_getter: Option<Arc<dyn SetGetter>>,
    connector: Option<Arc<dyn ValidatorConnector>>,
    grpc_client: Option<GrpcClient>,
}

impl FibreClientBuilder {
    /// Creates a new builder with all fields unset.
    pub fn new() -> Self {
        Self {
            config: None,
            signing_key: None,
            set_getter: None,
            connector: None,
            grpc_client: None,
        }
    }

    /// Sets the client configuration.
    ///
    /// If not called, [`FibreClientConfig::default()`] is used.
    pub fn config(mut self, config: FibreClientConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the secp256k1 signing key used to sign payment promises.
    pub fn signing_key(mut self, key: SigningKey) -> Self {
        self.signing_key = Some(key);
        self
    }

    /// Sets the validator set provider.
    pub fn set_getter(mut self, getter: impl SetGetter + 'static) -> Self {
        self.set_getter = Some(Arc::new(getter));
        self
    }

    /// Sets the validator connection factory.
    pub fn connector(mut self, connector: impl ValidatorConnector + 'static) -> Self {
        self.connector = Some(Arc::new(connector));
        self
    }

    /// Sets the gRPC client used for broadcasting transactions.
    ///
    /// This is optional. Only the [`FibreClient::put()`] method requires it
    /// for broadcasting `MsgPayForFibre` on-chain. Upload and download work
    /// without a gRPC client.
    pub fn grpc_client(mut self, client: GrpcClient) -> Self {
        self.grpc_client = Some(client);
        self
    }

    /// Builds the [`FibreClient`].
    ///
    /// Returns an error if any required field (`signing_key`, `set_getter`,
    /// `connector`) has not been set.
    pub fn build(self) -> Result<FibreClient, FibreError> {
        let cfg = self.config.unwrap_or_default();
        let signing_key = self
            .signing_key
            .ok_or_else(|| FibreError::Other("signing_key is required".into()))?;
        let set_getter = self
            .set_getter
            .ok_or_else(|| FibreError::Other("set_getter is required".into()))?;
        let connector = self
            .connector
            .ok_or_else(|| FibreError::Other("connector is required".into()))?;

        Ok(FibreClient {
            upload_semaphore: Arc::new(tokio::sync::Semaphore::new(cfg.upload_concurrency)),
            download_semaphore: Arc::new(tokio::sync::Semaphore::new(cfg.download_concurrency)),
            cfg,
            signing_key,
            set_getter,
            connector,
            closed: AtomicBool::new(false),
            grpc_client: self.grpc_client,
        })
    }
}

impl Default for FibreClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k256::ecdsa::SigningKey;
    use rand::rngs::OsRng;

    struct DummySetGetter;

    #[async_trait::async_trait]
    impl crate::validator::SetGetter for DummySetGetter {
        async fn head(&self) -> Result<crate::validator::ValidatorSet, FibreError> {
            unimplemented!()
        }
    }

    struct DummyConnector;

    #[async_trait::async_trait]
    impl crate::validator_client::ValidatorConnector for DummyConnector {
        async fn connect(
            &self,
            _validator: &crate::validator::ValidatorInfo,
        ) -> Result<std::sync::Arc<dyn crate::validator_client::ValidatorConnection>, FibreError>
        {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn from_url_with_valid_url() {
        let sk = SigningKey::random(&mut OsRng);
        let result = FibreClient::from_url(
            "http://localhost:9090",
            sk,
            None,
            FibreClientConfig::default(),
        );
        assert!(
            result.is_ok(),
            "from_url with valid URL should succeed but got err: {}",
            result.err().map(|e| e.to_string()).unwrap_or_default()
        );
    }

    #[tokio::test]
    async fn from_url_with_invalid_url() {
        let sk = SigningKey::random(&mut OsRng);
        let result = FibreClient::from_url(
            "not a valid url \x00",
            sk,
            None,
            FibreClientConfig::default(),
        );
        assert!(result.is_err(), "from_url with invalid URL should fail");
    }

    #[tokio::test]
    async fn from_url_sets_config_correctly() {
        let sk = SigningKey::random(&mut OsRng);
        let mut config = FibreClientConfig::default();
        config.chain_id = "test-123".to_string();

        let client = FibreClient::from_url("http://localhost:9090", sk, None, config)
            .expect("from_url should succeed");

        assert_eq!(client.config().chain_id, "test-123");
    }

    #[tokio::test]
    async fn from_url_without_grpc_client_builds_successfully() {
        let sk = SigningKey::random(&mut OsRng);
        let client = FibreClient::from_url(
            "http://localhost:9090",
            sk,
            None,
            FibreClientConfig::default(),
        )
        .expect("from_url without grpc_client should succeed");

        // The client should be constructed, but grpc_client is None so put() would fail.
        assert!(client.grpc_client.is_none());
    }

    #[test]
    fn builder_missing_signing_key_returns_error() {
        let result = FibreClient::builder()
            .set_getter(DummySetGetter)
            .connector(DummyConnector)
            .build();

        match result {
            Err(FibreError::Other(msg)) => {
                assert!(
                    msg.contains("signing_key"),
                    "error should mention signing_key, got: {msg}"
                );
            }
            Err(other) => panic!("expected FibreError::Other mentioning signing_key, got: {other}"),
            Ok(_) => panic!("expected an error but build() succeeded"),
        }
    }

    #[test]
    fn builder_missing_set_getter_returns_error() {
        let sk = SigningKey::random(&mut OsRng);
        let result = FibreClient::builder()
            .signing_key(sk)
            .connector(DummyConnector)
            .build();

        match result {
            Err(FibreError::Other(msg)) => {
                assert!(
                    msg.contains("set_getter"),
                    "error should mention set_getter, got: {msg}"
                );
            }
            Err(other) => panic!("expected FibreError::Other mentioning set_getter, got: {other}"),
            Ok(_) => panic!("expected an error but build() succeeded"),
        }
    }

    #[test]
    fn builder_missing_connector_returns_error() {
        let sk = SigningKey::random(&mut OsRng);
        let result = FibreClient::builder()
            .signing_key(sk)
            .set_getter(DummySetGetter)
            .build();

        match result {
            Err(FibreError::Other(msg)) => {
                assert!(
                    msg.contains("connector"),
                    "error should mention connector, got: {msg}"
                );
            }
            Err(other) => panic!("expected FibreError::Other mentioning connector, got: {other}"),
            Ok(_) => panic!("expected an error but build() succeeded"),
        }
    }

    #[test]
    fn close_and_is_closed() {
        let sk = SigningKey::random(&mut OsRng);
        let client = FibreClient::builder()
            .signing_key(sk)
            .set_getter(DummySetGetter)
            .connector(DummyConnector)
            .build()
            .expect("builder should succeed");

        assert!(!client.is_closed(), "client should not be closed initially");
        client.close();
        assert!(client.is_closed(), "client should be closed after close()");
    }
}
