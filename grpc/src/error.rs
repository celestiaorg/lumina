use celestia_types::hash::Hash;
use celestia_types::state::ErrorCode;
use k256::ecdsa::signature::Error as SignatureError;
use tonic::Status;

use crate::abci_proofs::ProofError;

/// Alias for a `Result` with the error type [`celestia_grpc::Error`].
///
/// [`celestia_grpc::Error`]: crate::Error
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Representation of all the errors that can occur when interacting with [`GrpcClient`].
///
/// [`GrpcClient`]: crate::GrpcClient
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Tonic error
    #[error(transparent)]
    TonicError(Box<Status>),

    /// Transport error
    #[cfg(not(target_arch = "wasm32"))]
    #[error("Transport: {0}")]
    TransportError(#[from] tonic::transport::Error),

    /// Tendermint Error
    #[error(transparent)]
    TendermintError(#[from] tendermint::Error),

    /// Celestia types error
    #[error(transparent)]
    CelestiaTypesError(#[from] celestia_types::Error),

    /// Tendermint Proto Error
    #[error(transparent)]
    TendermintProtoError(#[from] tendermint_proto::Error),

    /// Failed to parse gRPC response
    #[error("Failed to parse response")]
    FailedToParseResponse,

    /// Unexpected reponse type
    #[error("Unexpected response type")]
    UnexpectedResponseType(String),
    /// Transaction worker stopped after completing queued work.
    #[error("Transaction worker stopped")]
    TxWorkerStopped,
    /// Transaction worker is still running.
    #[error("Transaction worker is running")]
    TxWorkerRunning,

    /// Empty blob submission list
    #[error("Attempted to submit blob transaction with empty blob list")]
    TxEmptyBlobList,

    /// Broadcasting transaction failed
    #[error("Broadcasting transaction {0} failed; code: {1}, error: {2}")]
    TxBroadcastFailed(Hash, ErrorCode, String),

    /// Executing transaction failed
    #[error("Transaction {0} execution failed; code: {1}, error: {2}")]
    TxExecutionFailed(Hash, ErrorCode, String),

    /// Transaction was rejected
    #[error("Transaction {0} was rejected; code: {1}, error: {2}")]
    TxRejected(Hash, ErrorCode, String),

    /// Transaction was evicted from the mempool
    #[error("Transaction {0} was evicted from the mempool")]
    TxEvicted(Hash),

    /// Transaction wasn't found, it was likely rejected
    #[error("Transaction {0} wasn't found, it was likely rejected")]
    TxNotFound(Hash),

    /// Provided public key differs from one associated with account
    #[error("Provided public key differs from one associated with account")]
    PublicKeyMismatch,

    /// ABCI proof verification has failed
    #[error("ABCI proof verification has failed: {0}")]
    AbciProof(#[from] ProofError),

    /// ABCI query returned an error
    #[error("ABCI query returned an error (code {0}): {1}")]
    AbciQuery(ErrorCode, String),

    /// Signing error
    #[error(transparent)]
    SigningError(#[from] SignatureError),

    /// Client was constructed with without a signer
    #[error("Client was constructed with without a signer")]
    MissingSigner,

    /// Error related to the metadata
    #[error(transparent)]
    Metadata(#[from] MetadataError),

    /// Couldn't parse expected sequence from the error message
    #[error("Couldn't parse expected sequence from: '{0}'")]
    SequenceParsingFailed(String),

    /// Invalid parameter provided
    #[error("Invalid BroadcastedTx parameter provided: {0}")]
    InvalidBroadcastedTx(String),
}

/// Representation of all the errors that can occur when building [`GrpcClient`] using
/// [`GrpcClientBuilder`]
///
/// [`GrpcClient`]: crate::GrpcClient
/// [`GrpcClientBuilder`]: crate::GrpcClientBuilder
#[derive(Debug, thiserror::Error)]
pub enum GrpcClientBuilderError {
    /// Error from tonic transport
    #[error(transparent)]
    #[cfg(not(target_arch = "wasm32"))]
    TonicTransportError(#[from] tonic::transport::Error),

    /// Transport has not been set for builder
    #[error("Transport not set")]
    TransportNotSet,

    /// Invalid private key.
    #[error("Invalid private key")]
    InvalidPrivateKey,

    /// Invalid public key.
    #[error("Invalid public key")]
    InvalidPublicKey,

    /// Error related to the metadata
    #[error(transparent)]
    Metadata(#[from] MetadataError),

    /// Tls support is not enabled but requested
    #[error(
        "Tls support is not enabled but requested via url, please enable it using proper feature flags"
    )]
    TlsNotSupported,
}

#[derive(thiserror::Error, Debug)]
pub enum MetadataError {
    /// Invalid metadata key
    #[error("Invalid metadata key ({0})")]
    Key(String),

    /// Invalid binary metadata key
    #[error("Invalid binary metadata key ({0:?})")]
    KeyBin(Vec<u8>),

    /// Invalid metadata value for given key
    #[error("Invalid metadata value (key: {0:?})")]
    Value(String),
}

impl Error {
    /// Returns true if this error is network-related
    pub fn is_network_error(&self) -> bool {
        match self {
            Error::TonicError(status) => {
                // Network-related gRPC status codes
                matches!(
                    status.code(),
                    tonic::Code::Unavailable
                        | tonic::Code::Unknown
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Aborted
                )
            }
            #[cfg(not(target_arch = "wasm32"))]
            Error::TransportError(_) => true,
            _ => false,
        }
    }
}

impl From<Status> for Error {
    fn from(value: Status) -> Self {
        Error::TonicError(Box::new(value))
    }
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
impl From<Error> for wasm_bindgen::JsValue {
    fn from(error: Error) -> wasm_bindgen::JsValue {
        error.to_string().into()
    }
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
impl From<GrpcClientBuilderError> for wasm_bindgen::JsValue {
    fn from(error: GrpcClientBuilderError) -> wasm_bindgen::JsValue {
        error.to_string().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::{Code, Status};

    #[test]
    fn network_errors_return_true() {
        let network_codes = [
            Code::Unavailable,
            Code::Unknown,
            Code::DeadlineExceeded,
            Code::Aborted,
        ];

        for code in network_codes {
            let error: Error = Status::new(code, "test").into();
            assert!(
                error.is_network_error(),
                "Expected {:?} to be a network error",
                code
            );
        }
    }

    #[test]
    fn non_network_tonic_errors_return_false() {
        let non_network_codes = [
            Code::Ok,
            Code::Cancelled,
            Code::InvalidArgument,
            Code::NotFound,
            Code::AlreadyExists,
            Code::PermissionDenied,
            Code::ResourceExhausted,
            Code::FailedPrecondition,
            Code::OutOfRange,
            Code::Unimplemented,
            Code::Internal,
            Code::DataLoss,
            Code::Unauthenticated,
        ];

        for code in non_network_codes {
            let error: Error = Status::new(code, "test").into();
            assert!(
                !error.is_network_error(),
                "Expected {:?} to NOT be a network error",
                code
            );
        }
    }

    #[test]
    fn other_error_variants_return_false() {
        assert!(!Error::FailedToParseResponse.is_network_error());
        assert!(!Error::TxEmptyBlobList.is_network_error());
        assert!(!Error::MissingSigner.is_network_error());
        assert!(!Error::PublicKeyMismatch.is_network_error());
        assert!(!Error::UnexpectedResponseType("test".into()).is_network_error());
        assert!(!Error::SequenceParsingFailed("test".into()).is_network_error());
    }

    // we can't use async_test because this test should only be run for non wasm-32 archs
    // because it uses TransportError which is not available there
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn transport_error_is_network_error() {
        use tonic::transport::Endpoint;

        // Try to connect to an invalid endpoint to get a transport error
        let endpoint = Endpoint::from_static("http://[::1]:1");
        let result = endpoint.connect().await;

        let transport_error = result.expect_err("should fail to connect");
        let error = Error::TransportError(transport_error);

        assert!(error.is_network_error());
    }
}
