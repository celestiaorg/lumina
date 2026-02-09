//! Compatibility layer for exporting gRPC functionality via uniffi

use std::sync::{Arc, Mutex};
use std::time::Duration;

use k256::ecdsa::VerifyingKey;
use uniffi::Object;

mod grpc_client;

use crate::Endpoint as RustEndpoint;
use crate::GrpcClientBuilder as RustBuilder;
use crate::signer::{UniffiSigner, UniffiSignerBox};

pub use grpc_client::GrpcClient;

/// Errors returned when building Grpc Client
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum GrpcClientBuilderError {
    /// Error creating transport
    #[error("error creating transport: {msg}")]
    TonicTransportError {
        /// error message
        msg: String,
    },

    /// Invalid account public key
    #[error("invalid account public key")]
    InvalidAccountPublicKey,

    /// Invalid account private key
    #[error("invalid account private key")]
    InvalidAccountPrivateKey,

    /// Invalid metadata
    #[error("invalid metadata")]
    Metadata(String),

    /// Tls support is not enabled but requested
    #[error(
        "Tls support is not enabled but requested via url, please enable it using proper feature flags"
    )]
    TlsNotSupported,
}

/// A URL endpoint paired with its configuration.
#[derive(Object)]
pub struct Endpoint(Mutex<Option<RustEndpoint>>);

#[uniffi::export]
impl Endpoint {
    /// Create a new endpoint with a URL.
    #[uniffi::constructor]
    pub fn new(url: String) -> Self {
        let endpoint = RustEndpoint::from(url);
        Endpoint(Mutex::new(Some(endpoint)))
    }

    /// Appends ASCII metadata (HTTP/2 header) to requests made to this endpoint.
    #[uniffi::method(name = "withMetadata")]
    pub fn metadata(self: Arc<Self>, key: &str, value: &str) -> Arc<Self> {
        {
            let mut lock = self.0.lock().expect("lock poisoned");
            let endpoint = lock.take().expect("endpoint must be set");
            *lock = Some(endpoint.metadata(key, value));
        }
        self
    }

    /// Appends binary metadata to requests made to this endpoint.
    ///
    /// Keys must have `-bin` suffix.
    #[uniffi::method(name = "withMetadataBin")]
    pub fn metadata_bin(self: Arc<Self>, key: &str, value: &[u8]) -> Arc<Self> {
        {
            let mut lock = self.0.lock().expect("lock poisoned");
            let endpoint = lock.take().expect("endpoint must be set");
            *lock = Some(endpoint.metadata_bin(key, value.to_vec()));
        }
        self
    }

    /// Sets the request timeout in milliseconds for this endpoint.
    #[uniffi::method(name = "withTimeout")]
    pub fn timeout(self: Arc<Self>, timeout_ms: u64) -> Arc<Self> {
        {
            let mut lock = self.0.lock().expect("lock poisoned");
            let endpoint = lock.take().expect("endpoint must be set");
            *lock = Some(endpoint.timeout(Duration::from_millis(timeout_ms)));
        }
        self
    }
}

impl Endpoint {
    fn take(&self) -> RustEndpoint {
        self.0
            .lock()
            .expect("lock poisoned")
            .take()
            .expect("endpoint must be set")
    }
}

/// Builder for [`GrpcClient`]
#[derive(Object)]
pub struct GrpcClientBuilder(Mutex<Option<RustBuilder>>);

impl GrpcClientBuilder {
    /// Apply given transformation to the inner builder
    fn map_builder<F>(&self, map: F)
    where
        F: FnOnce(RustBuilder) -> RustBuilder,
    {
        let mut builder_lock = self.0.lock().expect("lock poisoned");
        let builder = builder_lock.take().expect("builder must be set");
        *builder_lock = Some(map(builder));
    }
}

// note: we cannot use the GrpcClient::builder() returns GrpcClientBuilder
// pattern as in rust or js, because uniffi does not support static methods
// except for constructors: https://github.com/mozilla/uniffi-rs/issues/1074
#[uniffi::export(async_runtime = "tokio")]
impl GrpcClientBuilder {
    /// Create a new builder for the provided url.
    ///
    /// Example:
    /// ```swift
    /// let client = try await GrpcClientBuilder.withUrl(url: "http://localhost:9090").build()
    /// ```
    #[uniffi::constructor(name = "withUrl")]
    pub fn with_url(url: String) -> Self {
        let builder = RustBuilder::new().url(url);
        GrpcClientBuilder(Mutex::new(Some(builder)))
    }

    /// Create a new builder for the provided endpoint.
    ///
    /// Example:
    /// ```swift
    /// let endpoint = Endpoint(url: "http://localhost:9090")
    /// let client = try await GrpcClientBuilder.withEndpoint(endpoint: endpoint).build()
    /// ```
    #[uniffi::constructor(name = "withEndpoint")]
    pub fn with_endpoint(endpoint: Arc<Endpoint>) -> Self {
        let builder = RustBuilder::new().endpoint(endpoint.take());
        GrpcClientBuilder(Mutex::new(Some(builder)))
    }

    /// Create a new builder with multiple fallback URLs.
    ///
    /// When multiple endpoints are configured, the client will automatically
    /// fall back to the next endpoint if a network-related error occurs.
    ///
    /// Example:
    /// ```swift
    /// let client = try await GrpcClientBuilder.withUrls(urls: ["http://primary:9090", "http://fallback:9090"]).build()
    /// ```
    #[uniffi::constructor(name = "withUrls")]
    pub fn with_urls(urls: Vec<String>) -> Self {
        let builder = RustBuilder::new().urls(urls);
        GrpcClientBuilder(Mutex::new(Some(builder)))
    }

    /// Create a new builder with multiple fallback endpoints.
    ///
    /// When multiple endpoints are configured, the client will automatically
    /// fall back to the next endpoint if a network-related error occurs.
    ///
    /// Example:
    /// ```swift
    /// let endpoints = [Endpoint(url: "http://primary:9090")]
    /// let client = try await GrpcClientBuilder.withEndpoints(endpoints: endpoints).build()
    /// ```
    #[uniffi::constructor(name = "withEndpoints")]
    pub fn with_endpoints(endpoints: Vec<Arc<Endpoint>>) -> Self {
        let endpoints = endpoints.into_iter().map(|e| e.take()).collect::<Vec<_>>();
        let builder = RustBuilder::new().endpoints(endpoints);
        GrpcClientBuilder(Mutex::new(Some(builder)))
    }

    /// Add public key and signer to the client being built
    #[uniffi::method(name = "withPubkeyAndSigner")]
    pub fn pubkey_and_signer(
        self: Arc<Self>,
        account_pubkey: Vec<u8>,
        signer: Arc<dyn UniffiSigner>,
    ) -> Result<Arc<Self>, GrpcClientBuilderError> {
        let vk = VerifyingKey::from_sec1_bytes(&account_pubkey)
            .map_err(|_| GrpcClientBuilderError::InvalidAccountPublicKey)?;
        let signer = UniffiSignerBox(signer);

        self.map_builder(move |builder| builder.pubkey_and_signer(vk, signer));

        Ok(self)
    }

    // this function _must_ be async despite not awaiting, so that it executes in tokio runtime
    // context
    /// Build the gRPC client.
    #[uniffi::method(name = "build")]
    pub async fn build(self: Arc<Self>) -> Result<GrpcClient, GrpcClientBuilderError> {
        let builder = self
            .0
            .lock()
            .expect("lock poisoned")
            .take()
            .expect("builder must be set");

        Ok(builder.build()?.into())
    }
}

impl From<crate::GrpcClientBuilderError> for GrpcClientBuilderError {
    fn from(error: crate::GrpcClientBuilderError) -> Self {
        match error {
            crate::GrpcClientBuilderError::TonicTransportError(error) => {
                GrpcClientBuilderError::TonicTransportError {
                    msg: error.to_string(),
                }
            }
            crate::GrpcClientBuilderError::InvalidPrivateKey => {
                GrpcClientBuilderError::InvalidAccountPrivateKey
            }
            crate::GrpcClientBuilderError::InvalidPublicKey => {
                GrpcClientBuilderError::InvalidAccountPublicKey
            }
            crate::GrpcClientBuilderError::TransportNotSet => {
                // API above should not allow creating a builder without any transport
                unimplemented!("transport not set for builder, should not happen")
            }
            crate::GrpcClientBuilderError::Metadata(err) => {
                GrpcClientBuilderError::Metadata(err.to_string())
            }
            crate::GrpcClientBuilderError::TlsNotSupported => {
                GrpcClientBuilderError::TlsNotSupported
            }
        }
    }
}
