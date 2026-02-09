#![doc = include_str!("../README.md")]

#[cfg(all(target_arch = "wasm32", test))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

mod blob;
mod blobstream;
mod client;
mod fraud;
mod header;
mod share;
mod state;
#[cfg(test)]
mod test_utils;
mod utils;

/// API related types.
pub mod api {
    pub use crate::blob::BlobApi;
    pub use crate::blobstream::BlobstreamApi;
    pub use crate::fraud::FraudApi;
    pub use crate::header::HeaderApi;
    pub use crate::share::ShareApi;
    pub use crate::state::StateApi;

    /// Blob API related types.
    pub mod blob {
        #[doc(inline)]
        pub use celestia_rpc::blob::BlobsAtHeight;
    }

    /// Share API related types.
    pub mod share {
        #[doc(inline)]
        pub use celestia_rpc::share::{
            GetRangeResponse, GetRowResponse, RowSide, SampleCoordinates,
        };
    }

    /// Fraud API related types.
    pub mod fraud {
        #[doc(inline)]
        pub use celestia_rpc::fraud::{Proof, ProofType};
    }

    /// State API related types.
    pub mod state {
        #[doc(inline)]
        pub use crate::state::AsyncGrpcCall;
    }
}

/// TX related types.
pub mod tx {
    #[doc(inline)]
    pub use celestia_grpc::grpc::{GasEstimate, TxPriority};
    #[doc(inline)]
    pub use celestia_grpc::{DocSigner, IntoProtobufAny, SignDoc, TxConfig, TxInfo};
    #[doc(inline)]
    pub use k256::ecdsa::signature::{Error as SignatureError, Keypair};
    #[doc(inline)]
    pub use k256::ecdsa::{Signature, SigningKey, VerifyingKey};
}

use celestia_grpc::GrpcClientBuilderError;

/// Re-export of [`celestia-proto`].
///
/// [`celestia-proto`]: celestia_proto
#[doc(inline)]
pub use celestia_proto as proto;

/// Re-export of [`celestia-types`].
///
/// [`celestia-types`]: celestia_types
#[doc(inline)]
pub use celestia_types as types;

pub use crate::client::{Client, ClientBuilder, Endpoint};

/// Alias for a `Result` with the error type [`celestia_client::Error`].
///
/// [`celestia_client::Error`]: crate::Error
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Representation of all the errors that can occur when interacting with [`celestia_client`].
///
/// [`celestia_client`]: crate
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Celestia RPC error.
    #[error("RPC error: {0}")]
    Rpc(#[from] celestia_rpc::Error),

    /// Celestia gRPC error.
    #[error("gRPC error: {0}")]
    Grpc(celestia_grpc::Error),

    /// gRPC client builder error.
    #[error("gRPC client builder error: {0}")]
    GrpcBuilder(GrpcClientBuilderError),

    /// Celestia types error.
    #[error("Celestia types error: {0}")]
    Types(#[from] celestia_types::Error),

    /// Client is in read-only mode.
    #[error("Client is constructed for read-only mode, operation not supported")]
    ReadOnlyMode,

    /// RPC chain-id and gRPC chain-id missmatch.
    #[error("Chain id of RPC endpoint missmatch with chain id of gRPC endpoint")]
    ChainIdMissmatch,

    /// Invalid height.
    #[error("Invalid height: {0}")]
    InvalidHeight(u64),

    /// Invalid private key.
    #[error("Invalid private key")]
    InvalidPrivateKey,

    /// RPC endpoint is not set.
    #[error("RPC endpoint not set")]
    RpcEndpointNotSet,

    /// Client does not have associated account address
    #[error("Client has no associated account address")]
    NoAssociatedAddress,

    /// gRPC endpoint is not set.
    #[error("Signer is set but gRPC endpoint is not")]
    GrpcEndpointNotSet,
}

impl From<jsonrpsee_core::ClientError> for Error {
    fn from(value: jsonrpsee_core::ClientError) -> Self {
        Error::Rpc(celestia_rpc::Error::JsonRpc(value))
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        jsonrpsee_core::ClientError::ParseError(value).into()
    }
}

impl From<GrpcClientBuilderError> for Error {
    fn from(value: GrpcClientBuilderError) -> Self {
        match value {
            GrpcClientBuilderError::TransportNotSet => Error::GrpcEndpointNotSet,
            GrpcClientBuilderError::InvalidPrivateKey => Error::InvalidPrivateKey,
            e => Error::GrpcBuilder(e),
        }
    }
}

impl From<celestia_grpc::Error> for Error {
    fn from(value: celestia_grpc::Error) -> Self {
        match value {
            celestia_grpc::Error::MissingSigner => Error::ReadOnlyMode,
            e => Error::Grpc(e),
        }
    }
}

impl Error {
    /// Helper that returns the logical error of a gRPC call.
    pub fn as_grpc_status(&self) -> Option<&tonic::Status> {
        match self {
            Error::Grpc(celestia_grpc::Error::TonicError(status)) => Some(&**status),
            _ => None,
        }
    }

    /// Helper that returns the logical error of a RPC call.
    pub fn as_rpc_call_error(&self) -> Option<&jsonrpsee_types::error::ErrorObjectOwned> {
        match self {
            Error::Rpc(celestia_rpc::Error::JsonRpc(jsonrpsee_core::ClientError::Call(e))) => {
                Some(e)
            }
            _ => None,
        }
    }
}
