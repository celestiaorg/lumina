//! FibreClient -- the main entry point for the Fibre DA protocol.
//!
//! The [`FibreClient`] struct provides `upload()` for distributing blobs to
//! validators and collecting signatures, and (in Stage 4) `download()` for
//! retrieving and reconstructing blobs.
//!
//! Use [`FibreClientBuilder`] (via [`FibreClient::builder()`]) to construct
//! an instance.

pub(crate) mod download;
pub(crate) mod task;
pub(crate) mod upload;

use std::sync::Arc;

use k256::ecdsa::SigningKey;
use tokio_util::sync::CancellationToken;

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
    pub(crate) cancel_token: CancellationToken,
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
        self.cancel_token.is_cancelled()
    }

    /// Mark the client as closed so that subsequent and in-flight operations
    /// are cancelled with [`FibreError::ClientClosed`].
    pub fn close(&self) {
        self.cancel_token.cancel();
    }

    /// Returns the client's cancellation token.
    ///
    /// Callers can use this to listen for cancellation or to create child
    /// tokens for individual operations.
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancel_token
    }

    /// Convenience constructor that builds a fully-wired [`FibreClient`] from a
    /// single gRPC endpoint.
    ///
    /// Accepts anything that converts into a [`celestia_grpc::Endpoint`], such
    /// as a URL string (`"http://localhost:9090"`) or a rich [`Endpoint`] with
    /// metadata and timeout.
    ///
    /// Creates a [`GrpcClient`] for chain queries (validator sets via CometBFT
    /// `BlockAPI` and host resolution via `x/valaddr`), wires up the production
    /// [`GrpcSetGetter`], [`GrpcHostRegistry`], and [`GrpcValidatorConnector`].
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The consensus node gRPC endpoint.
    /// * `signing_key` - secp256k1 key for signing payment promises.
    /// * `config` - Client configuration. Use [`FibreClientConfig::default()`] for defaults.
    pub fn from_endpoint(
        endpoint: impl Into<celestia_grpc::Endpoint>,
        signing_key: SigningKey,
        config: FibreClientConfig,
    ) -> Result<Self, FibreError> {
        let grpc_client = celestia_grpc::GrpcClient::builder()
            .endpoint(endpoint)
            .build()
            .map_err(|e| FibreError::Other(format!("failed to build GrpcClient: {e}")))?;

        Self::from_grpc_client(grpc_client, signing_key, config)
    }

    /// Construct a [`FibreClient`] from an existing [`GrpcClient`].
    ///
    /// Use this when the caller already has a configured [`GrpcClient`]
    /// (e.g. with multiple endpoints, auth metadata, or a custom signer).
    pub fn from_grpc_client(
        grpc_client: celestia_grpc::GrpcClient,
        signing_key: SigningKey,
        config: FibreClientConfig,
    ) -> Result<Self, FibreError> {
        let host_registry = Arc::new(crate::host_registry::GrpcHostRegistry::new(
            grpc_client.clone(),
        ));
        let connector = crate::grpc_validator_client::GrpcValidatorConnector::new(host_registry);

        Self::builder()
            .config(config)
            .signing_key(signing_key)
            .set_getter(crate::validator::GrpcSetGetter::new(grpc_client))
            .connector(connector)
            .build()
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
}

impl FibreClientBuilder {
    /// Creates a new builder with all fields unset.
    pub fn new() -> Self {
        Self {
            config: None,
            signing_key: None,
            set_getter: None,
            connector: None,
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
            cancel_token: CancellationToken::new(),
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
    async fn from_endpoint_with_valid_url() {
        let sk = SigningKey::random(&mut OsRng);
        let result =
            FibreClient::from_endpoint("http://localhost:9090", sk, FibreClientConfig::default());
        assert!(
            result.is_ok(),
            "from_endpoint with valid URL should succeed but got err: {}",
            result.err().map(|e| e.to_string()).unwrap_or_default()
        );
    }

    #[tokio::test]
    async fn from_endpoint_with_invalid_url() {
        let sk = SigningKey::random(&mut OsRng);
        let result =
            FibreClient::from_endpoint("not a valid url \x00", sk, FibreClientConfig::default());
        assert!(
            result.is_err(),
            "from_endpoint with invalid URL should fail"
        );
    }

    #[tokio::test]
    async fn from_endpoint_sets_config_correctly() {
        let sk = SigningKey::random(&mut OsRng);
        let config = FibreClientConfig {
            chain_id: "test-123".to_string(),
            ..Default::default()
        };

        let client = FibreClient::from_endpoint("http://localhost:9090", sk, config)
            .expect("from_endpoint should succeed");

        assert_eq!(client.config().chain_id, "test-123");
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
