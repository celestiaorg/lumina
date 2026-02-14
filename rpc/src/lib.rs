#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

use std::fmt;

use jsonrpsee::core::client::Error as JsonrpseeError;

pub mod blob;
pub mod blobstream;
pub mod client;
pub mod das;
mod error;
pub mod fraud;
mod header;
#[cfg(feature = "p2p")]
/// Types and client for the p2p JSON-RPC API.
pub mod p2p;
pub mod share;
mod state;
mod tx_config;

pub use crate::blob::BlobClient;
pub use crate::blob::BlobServer;
pub use crate::blobstream::BlobstreamClient;
pub use crate::blobstream::BlobstreamServer;
#[cfg(any(
    not(target_arch = "wasm32"),
    all(target_arch = "wasm32", feature = "wasm-bindgen")
))]
#[cfg_attr(
    docsrs,
    doc(cfg(any(
        not(target_arch = "wasm32"),
        all(target_arch = "wasm32", feature = "wasm-bindgen")
    )))
)]
pub use crate::client::Client;
pub use crate::das::DasClient;
pub use crate::error::{Error, Result};
pub use crate::fraud::FraudClient;
pub use crate::fraud::FraudServer;
pub use crate::header::HeaderClient;
pub use crate::header::HeaderServer;
#[cfg(feature = "p2p")]
#[cfg_attr(docsrs, doc(cfg(feature = "p2p")))]
pub use crate::p2p::P2PClient;
pub use crate::share::ShareClient;
pub use crate::share::ShareServer;
pub use crate::state::StateClient;
pub use crate::state::StateServer;
pub use crate::tx_config::{TxConfig, TxPriority};

/// Re-exports of all the RPC traits.
pub mod prelude {
    pub use crate::BlobClient;
    pub use crate::BlobServer;
    pub use crate::BlobstreamClient;
    pub use crate::BlobstreamServer;
    pub use crate::DasClient;
    pub use crate::FraudClient;
    pub use crate::FraudServer;
    pub use crate::HeaderClient;
    pub use crate::HeaderServer;
    #[cfg(feature = "p2p")]
    pub use crate::P2PClient;
    pub use crate::ShareClient;
    pub use crate::ShareServer;
    pub use crate::StateClient;
    pub use crate::StateServer;
}

// helper to map errors to jsonrpsee using Display
fn custom_client_error<E: fmt::Display>(error: E) -> JsonrpseeError {
    JsonrpseeError::Custom(error.to_string())
}
