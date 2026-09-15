//! Error types for the Fibre client library.

use thiserror::Error;

pub(crate) const PAYMENT_PROMISE_ALREADY_PROCESSED: &str = "payment promise verification failed: stateful validation: rpc error: code = Internal desc = payment promise has already been processed";

use super::blob::BLOB_ID_SIZE;
use super::blob_header;
use super::config::MAX_CHAIN_ID_SIZE;
use super::payment_promise::SIGNATURE_SIZE;

#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum BlobIdError {
    #[error("blob ID must be {expected} bytes, got {0}", expected = BLOB_ID_SIZE)]
    Length(usize),
    #[error("blob ID is not valid hex: {0}")]
    Hex(#[from] hex::FromHexError),
}

#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum BlobHeaderError {
    #[error("no rows to decode")]
    NoRows,
    #[error(
        "first row too small: need at least {expected} bytes for header, got {0}",
        expected = blob_header::SIZE
    )]
    FirstRowTooSmall(usize),
    #[error("blob size in header must be greater than 0")]
    ZeroDataSize,
    #[error("blob size in header ({size} bytes) exceeds maximum allowed size ({max} bytes)")]
    DataSizeExceedsMax { size: u32, max: usize },
    #[error("data size mismatch: copied {copied} bytes, expected {expected}")]
    DataSizeMismatch { copied: usize, expected: usize },
}

#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum ShardError {
    #[error("empty shard response from validator")]
    Empty,
    #[error("download response is missing shard")]
    MissingShard,
    #[error("proof hash has invalid length {0}, expected 32")]
    ProofHashLength(usize),
    #[error("rlc vector has invalid length {0}, expected a non-zero multiple of 16")]
    RlcVectorLength(usize),
    #[error("row index {index} out of bounds (total rows: {total_rows})")]
    RowIndexOutOfBounds { index: usize, total_rows: usize },
    #[error("row size {row_size} out of bounds (max {max_row_size})")]
    RowSizeOutOfBounds {
        row_size: usize,
        max_row_size: usize,
    },
    #[error("rlc vector does not match the verified one")]
    RlcVectorMismatch,
}

#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum PaymentPromiseError {
    #[error("upload size must be positive")]
    ZeroUploadSize,
    #[error("creation timestamp must not be zero")]
    ZeroTimestamp,
    #[error("creation timestamp is before Unix epoch")]
    TimestampBeforeEpoch(#[source] std::time::SystemTimeError),
    #[error("signature must be present")]
    MissingSignature,
    #[error(
        "signature must be {expected} bytes, got {0}",
        expected = SIGNATURE_SIZE
    )]
    SignatureLength(usize),
    #[error("invalid signature format: {0}")]
    SignatureFormat(#[source] k256::ecdsa::Error),
    #[error("signature verification failed: {0}")]
    SignatureVerification(#[source] k256::ecdsa::Error),
}

#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum ValidatorSetError {
    #[error("validator set response is missing validator_set")]
    Missing,
    #[error("validator set has invalid height {0}")]
    InvalidHeight(i64),
    #[error("validator set height must be greater than 0")]
    ZeroHeight,
    #[error("validator set height {0} exceeds i64::MAX")]
    HeightTooLarge(u64),
    #[error("validator set is empty")]
    Empty,
    #[error("validator has zero voting power")]
    ZeroVotingPower,
    #[error("validator has negative voting power: {0}")]
    NegativeVotingPower(i64),
    #[error("validator voting power {0} exceeds maximum {max}", max = i64::MAX / 8)]
    VotingPowerTooLarge(u64),
    #[error("total voting power {0} exceeds maximum {max}", max = i64::MAX / 8)]
    TotalVotingPowerTooLarge(u64),
    #[error("validator set contains duplicate address {}", hex::encode_upper(.0))]
    DuplicateValidator([u8; 20]),
    #[error("validator set is missing proposer")]
    MissingProposer,
    #[error("validator set proposer is not in validator set")]
    ProposerNotInSet,
    #[error("validator is missing a public key")]
    MissingPublicKey,
    #[error("expected ed25519 public key for validator")]
    UnsupportedPublicKeyType,
    #[error("ed25519 key has invalid length {0}, expected 32")]
    PublicKeyLength(usize),
    #[error("invalid ed25519 key: {0}")]
    InvalidPublicKey(#[source] ed25519_dalek::SignatureError),
    #[error("validator address does not match public key")]
    AddressMismatch,
}

#[allow(missing_docs)]
#[derive(Debug, Error)]
pub enum FibreClientBuilderError {
    #[error("config is required")]
    MissingConfig,
    #[error("set_getter is required")]
    MissingSetGetter,
    #[error("connector is required")]
    MissingConnector,
}

/// All errors that can occur in the Fibre client library.
#[derive(Debug, Error)]
pub enum FibreError {
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

    /// The blob version is not supported by this client.
    #[error("unsupported blob version: {0}")]
    UnsupportedBlobVersion(u8),

    /// The blob ID is invalid.
    #[error("invalid blob ID: {0}")]
    InvalidBlobId(#[from] BlobIdError),

    /// The blob header is invalid.
    #[error("invalid blob header: {0}")]
    InvalidBlobHeader(#[from] BlobHeaderError),

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

    /// A validator returned an invalid shard.
    #[error("invalid shard: {0}")]
    InvalidShard(#[from] ShardError),

    /// Not enough validator signatures were collected to meet the voting power threshold.
    #[error("not enough voting power: collected {collected}, required {required}")]
    NotEnoughSignatures {
        /// Total voting power of collected signatures.
        collected: u64,
        /// Required voting power threshold.
        required: u64,
    },

    /// A validator returned an invalid signature.
    #[error(
        "invalid validator signature from {}: {source}",
        hex::encode_upper(.validator)
    )]
    InvalidValidatorSignature {
        /// Validator consensus address.
        validator: [u8; 20],
        /// Signature parsing or verification failure.
        #[source]
        source: ed25519_dalek::SignatureError,
    },

    /// The payment promise failed validation.
    #[error("payment promise validation failed: {0}")]
    InvalidPaymentPromise(#[from] PaymentPromiseError),

    /// The chain ID length is outside the supported range.
    #[error(
        "invalid chain ID length {len}; expected 1..={max}",
        max = MAX_CHAIN_ID_SIZE
    )]
    InvalidChainId {
        /// Actual chain ID length in bytes.
        len: usize,
    },

    /// The Fibre client builder is missing a required value.
    #[error("client builder error: {0}")]
    Builder(#[from] FibreClientBuilderError),

    /// WASM transports require an explicit byte-stream connector.
    #[error("a Fibre I/O connector is required on WASM")]
    IoConnectorRequired,

    /// The validator set is invalid.
    #[error("invalid validator set: {0}")]
    InvalidValidatorSet(#[from] ValidatorSetError),

    /// The validator consensus address is invalid.
    #[error("invalid validator consensus address: {0}")]
    InvalidValidatorAddress(#[source] celestia_types::Error),

    /// No host address was found for a validator.
    #[error("host not found for validator {}", hex::encode_upper(.0))]
    HostNotFound([u8; 20]),

    /// A Fibre endpoint could not be parsed.
    #[error("invalid Fibre endpoint '{endpoint}': {source}")]
    InvalidEndpoint {
        /// Endpoint string that failed to parse.
        endpoint: String,
        /// URI parsing error.
        #[source]
        source: http::uri::InvalidUri,
    },

    /// A Fibre endpoint has no host component.
    #[error("Fibre endpoint '{0}' has no host")]
    EndpointMissingHost(http::Uri),

    /// The client has been closed and can no longer process requests.
    #[error("client is closed")]
    ClientClosed,

    /// The operation was cancelled via the cancellation token.
    #[error("operation cancelled")]
    Cancelled,

    /// An error from the GrpcClient (consensus node queries).
    #[error("grpc client error: {0}")]
    GrpcClient(#[from] celestia_grpc::Error),

    /// An error while building a gRPC client.
    #[error("failed to build gRPC client: {0}")]
    GrpcClientBuilder(#[from] celestia_grpc::GrpcClientBuilderError),

    /// An error from the rsema1d erasure coding library.
    #[error("encoding error: {0}")]
    Encoding(#[from] rsema1d::Error),

    /// A tonic transport error (connection failure, TLS, etc.).
    #[cfg(not(target_arch = "wasm32"))]
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

impl FibreError {
    pub(crate) fn is_grpc_not_found(&self) -> bool {
        matches!(
            self,
            FibreError::GrpcClient(celestia_grpc::Error::TonicError(status))
                if status.code() == tonic::Code::NotFound
        )
    }

    pub(crate) fn is_payment_promise_already_processed(&self) -> bool {
        matches!(
            self,
            FibreError::GrpcClient(celestia_grpc::Error::TonicError(status))
                if status.code() == tonic::Code::InvalidArgument
                    && status.message() == PAYMENT_PROMISE_ALREADY_PROCESSED
        )
    }
}

impl From<tonic::Status> for FibreError {
    fn from(err: tonic::Status) -> Self {
        FibreError::Grpc(Box::new(err))
    }
}

impl From<k256::ecdsa::Error> for FibreError {
    fn from(err: k256::ecdsa::Error) -> Self {
        FibreError::CryptoSignature(err)
    }
}

/// Convenience alias for `std::result::Result<T, FibreError>`.
pub type Result<T> = std::result::Result<T, FibreError>;

#[cfg(test)]
mod tests {
    use super::*;

    fn grpc_error(code: tonic::Code, message: &'static str) -> FibreError {
        FibreError::GrpcClient(celestia_grpc::Error::TonicError(Box::new(
            tonic::Status::new(code, message),
        )))
    }

    #[test]
    fn identifies_payment_promise_already_processed() {
        assert!(
            grpc_error(
                tonic::Code::InvalidArgument,
                PAYMENT_PROMISE_ALREADY_PROCESSED
            )
            .is_payment_promise_already_processed()
        );
        assert!(
            !grpc_error(tonic::Code::Internal, PAYMENT_PROMISE_ALREADY_PROCESSED)
                .is_payment_promise_already_processed()
        );
        assert!(
            !grpc_error(
                tonic::Code::InvalidArgument,
                "payment promise has already been processed"
            )
            .is_payment_promise_already_processed()
        );
    }

    #[test]
    fn identifies_grpc_not_found() {
        assert!(grpc_error(tonic::Code::NotFound, "missing").is_grpc_not_found());
        assert!(!grpc_error(tonic::Code::Internal, "missing").is_grpc_not_found());
        assert!(!FibreError::NotFound.is_grpc_not_found());
    }
}
