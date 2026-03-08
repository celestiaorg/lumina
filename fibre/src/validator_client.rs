//! Abstract transport traits for communicating with validators.
//!
//! The transport is split into two traits:
//! - [`ValidatorConnection`] — a connection to a single validator (upload/download shards)
//! - [`ValidatorConnector`] — factory that creates/caches connections by validator address
//!
//! Production uses gRPC ([`crate::grpc_validator_client`]); tests use in-memory mocks.

use std::sync::Arc;

use crate::blob::BlobID;
use crate::error::FibreError;
use crate::payment_promise::PaymentPromise;
use crate::validator::ValidatorInfo;

/// Response from a validator after accepting a shard upload.
#[derive(Debug, Clone)]
pub struct UploadResponse {
    /// Ed25519 signature over the payment promise sign bytes.
    pub validator_signature: Vec<u8>,
}

/// Response from a validator when downloading a shard.
#[derive(Debug, Clone)]
pub struct DownloadResponse {
    /// The blob shard containing rows with inclusion proofs.
    pub rows: Vec<DownloadedRow>,
}

/// A single row from a download response.
#[derive(Debug, Clone)]
pub struct DownloadedRow {
    /// The row inclusion proof from rsema1d.
    pub proof: rsema1d::RowInclusionProof,
}

/// A connection to a single validator's fibre service.
///
/// In production this is backed by a tonic gRPC channel.
/// In tests this is an in-memory store with signing capability.
#[async_trait::async_trait]
pub trait ValidatorConnection: Send + Sync {
    /// Upload a shard (payment promise + row proofs + RLC coefficients) and receive a validator signature.
    async fn upload_shard(
        &self,
        promise: &PaymentPromise,
        rows: &[rsema1d::RowInclusionProof],
        rlc_coeffs: &[rsema1d::GF128],
    ) -> Result<UploadResponse, FibreError>;

    /// Download rows for a blob by its ID.
    async fn download_shard(&self, blob_id: &BlobID) -> Result<DownloadResponse, FibreError>;
}

/// Factory for creating/caching connections to individual validators.
///
/// In production this resolves hosts via [`crate::host_registry::HostRegistry`]
/// and manages tonic channels. In tests this returns mock connections that
/// can be inspected after the test.
#[async_trait::async_trait]
pub trait ValidatorConnector: Send + Sync {
    /// Get or create a connection to the given validator.
    async fn connect(
        &self,
        validator: &ValidatorInfo,
    ) -> Result<Arc<dyn ValidatorConnection>, FibreError>;
}
