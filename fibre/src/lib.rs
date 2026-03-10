//! Client library for the Celestia Fibre data availability protocol.
//!
//! Fibre is an off-chain data availability protocol for Celestia. Instead of putting
//! blob data on-chain, Fibre distributes it directly to validators via gRPC. Only a
//! small payment receipt (`MsgPayForFibre`) goes on-chain.

mod blob;
mod blob_header;
mod client;
pub mod config;
mod download;
pub mod error;
mod grpc_validator_client;
mod host_registry;
mod payment_promise;
mod proto_conv;
mod shard_assignment;
mod shard_selection;
mod signature_set;
mod task;
mod upload;
pub mod validator;
mod validator_client;

#[cfg(test)]
mod roundtrip_test;

pub use blob::{Blob, BlobID, Commitment};
pub use celestia_grpc::Endpoint;
pub use client::{FibreClient, FibreClientBuilder};
pub use config::{
    BlobConfig, DEFAULT_PROTOCOL_PARAMS, FibreClientConfig, Fraction, ProtocolParams,
};
pub use error::{FibreError, Result};
pub use grpc_validator_client::GrpcValidatorConnector;
pub use host_registry::{GrpcHostRegistry, Host, HostRegistry};
pub use payment_promise::{PaymentPromise, SignedPaymentPromise};
pub use proto_conv::payment_promise_to_proto;
pub use upload::PreparedPut;
pub use validator::{GrpcSetGetter, SetGetter, ValidatorInfo, ValidatorSet};
pub use validator_client::{ValidatorConnection, ValidatorConnector};
