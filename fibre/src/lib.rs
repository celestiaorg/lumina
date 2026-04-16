//! Client library for the Celestia Fibre data availability protocol.
//!
//! Fibre is an off-chain data availability protocol for Celestia. Instead of putting
//! blob data on-chain, Fibre distributes it directly to validators via gRPC. Only a
//! small payment receipt (`MsgPayForFibre`) goes on-chain.

pub mod client;
pub mod domain;

pub use domain::config;
pub use domain::error;

// Compatibility aliases preserve historical crate-local paths so tests and
// internals don't need broad path rewrites during this refactor.
pub(crate) use domain::{blob, blob_header, payment_promise};

pub use client::{FibreClient, FibreClientBuilder};
pub use config::{
    BlobConfig, DEFAULT_PROTOCOL_PARAMS, FibreClientConfig, Fraction, ProtocolParams,
};
pub use domain::blob::{Blob, BlobID, Commitment};
pub use domain::payment_promise::{PaymentPromise, SignedPaymentPromise};
pub use error::{FibreError, Result};
