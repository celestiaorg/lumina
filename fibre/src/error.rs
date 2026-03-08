//! Error types for the Fibre client library.

use thiserror::Error;

/// All errors that can occur in the Fibre client library.
#[derive(Debug, Error)]
pub enum FibreError {
    // -- Blob errors --
    /// The blob data provided was empty.
    #[error("blob data is empty")]
    EmptyBlobData,

    /// The blob data exceeds the maximum allowed size.
    #[error("blob data size {size} exceeds maximum {max}")]
    BlobTooLarge {
        /// Actual size of the blob data in bytes.
        size: usize,
        /// Maximum allowed size in bytes.
        max: usize,
    },

    /// The computed commitment does not match the expected commitment.
    #[error("commitment mismatch: expected {expected}, got {actual}")]
    CommitmentMismatch {
        /// The expected commitment (hex-encoded).
        expected: String,
        /// The actual commitment (hex-encoded).
        actual: String,
    },

    /// The blob version is not supported by this client.
    #[error("unsupported blob version: {0}")]
    UnsupportedBlobVersion(u8),

    /// The blob ID is invalid.
    #[error("invalid blob ID: {0}")]
    InvalidBlobId(String),

    /// Invalid or malformed data encountered during processing.
    #[error("invalid data: {0}")]
    InvalidData(String),

    // -- Download errors --
    /// No shards were found for the requested blob.
    #[error("blob not found: no shards retrieved")]
    NotFound,

    /// Not enough shards were collected to reconstruct the blob.
    #[error("not enough shards to reconstruct blob: got {got}, need {need}")]
    NotEnoughShards {
        /// Number of shards actually collected.
        got: usize,
        /// Number of shards needed for reconstruction.
        need: usize,
    },

    /// A validator returned an empty shard response.
    #[error("empty shard response from validator")]
    EmptyShardResponse,

    // -- Upload errors --
    /// Not enough validator signatures were collected to meet the voting power threshold.
    #[error("not enough voting power: collected {collected}, required {required}")]
    NotEnoughSignatures {
        /// Total voting power of collected signatures.
        collected: i64,
        /// Required voting power threshold.
        required: i64,
    },

    /// A validator returned an invalid signature.
    #[error("invalid validator signature from {validator}: {reason}")]
    InvalidValidatorSignature {
        /// Hex-encoded validator address.
        validator: String,
        /// Reason the signature is invalid.
        reason: String,
    },

    /// Signature verification failed (e.g., validator or payment promise signature).
    #[error("signature verification failed")]
    SignatureVerificationFailed,

    // -- PaymentPromise errors --
    /// The payment promise failed validation.
    #[error("payment promise validation failed: {0}")]
    InvalidPaymentPromise(String),

    /// Signing the payment promise failed.
    #[error("signing payment promise failed: {0}")]
    SigningFailed(String),

    // -- Connection errors --
    /// No host address was found for a validator.
    #[error("host not found for validator {0}")]
    HostNotFound(String),

    /// The client has been closed and can no longer process requests.
    #[error("client is closed")]
    ClientClosed,

    // -- Wrapped errors --
    /// A gRPC status error from tonic.
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    /// An error from the celestia-grpc client.
    #[error("grpc client error: {0}")]
    GrpcClient(#[from] celestia_grpc::Error),

    /// An error from the rsema1d erasure coding library.
    #[error("encoding error: {0}")]
    Encoding(#[from] rsema1d::Error),

    /// A tonic transport error (connection failure, TLS, etc.).
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// A secp256k1 ECDSA error (key or signature operations).
    ///
    /// Note: `k256::ecdsa::Error` and `ed25519_dalek::SignatureError` are both
    /// re-exports of `signature::Error`, so we use a single variant for both.
    /// The `#[from]` trait is not used to avoid conflicting blanket implementations;
    /// use `FibreError::CryptoSignature(err)` explicitly instead.
    #[error("cryptographic signature error: {0}")]
    CryptoSignature(k256::ecdsa::Error),

    /// A protobuf decoding error.
    #[error("proto decode error: {0}")]
    ProtoDecode(#[from] prost::DecodeError),

    /// A catch-all error for cases not covered by other variants.
    #[error("{0}")]
    Other(String),
}

impl From<k256::ecdsa::Error> for FibreError {
    fn from(err: k256::ecdsa::Error) -> Self {
        FibreError::CryptoSignature(err)
    }
}

/// Convenience alias for `std::result::Result<T, FibreError>`.
pub type Result<T> = std::result::Result<T, FibreError>;
