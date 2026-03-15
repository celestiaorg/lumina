//! Client library for the Celestia Fibre data availability protocol.
//!
//! Fibre is an off-chain data availability protocol for Celestia. Instead of putting
//! blob data on-chain, Fibre distributes it directly to validators via gRPC. Only a
//! small payment receipt (`MsgPayForFibre`) goes on-chain.

pub mod client;
pub mod domain;
pub mod transport;
pub mod validator;

pub use domain::config;
pub use domain::error;

// Compatibility aliases preserve historical crate-local paths so tests and
// internals don't need broad path rewrites during this refactor.
pub(crate) use domain::{blob, blob_header, payment_promise};
pub(crate) use transport::{grpc_validator_client, host_registry, proto_conv, validator_client};
pub(crate) use validator::signature_set;

#[cfg(test)]
mod roundtrip_test;

pub use celestia_grpc::Endpoint;
pub use client::upload::PreparedPut;
pub use client::{FibreClient, FibreClientBuilder};
pub use config::{
    BlobConfig, DEFAULT_PROTOCOL_PARAMS, FibreClientConfig, Fraction, ProtocolParams,
};
pub use domain::blob::{Blob, BlobID, Commitment};
pub use domain::payment_promise::{PaymentPromise, SignedPaymentPromise};
pub use error::{FibreError, Result};
pub use transport::grpc_validator_client::GrpcValidatorConnector;
pub use transport::host_registry::{GrpcHostRegistry, Host, HostRegistry};
pub use transport::validator_client::{ValidatorConnection, ValidatorConnector};
pub use validator::{GrpcSetGetter, SetGetter, ValidatorInfo, ValidatorSet};
