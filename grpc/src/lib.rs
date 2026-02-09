#![doc = include_str!("../README.md")]

mod abci_proofs;
pub(crate) mod boxed;
mod builder;
mod client;
mod error;
pub mod grpc;
#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
mod js_client;
pub mod signer;
#[cfg(test)]
mod test_utils;
mod tx;
mod tx_client_impl;
pub mod tx_client_v2;
#[cfg(all(not(target_arch = "wasm32"), feature = "uniffi"))]
pub mod uniffi_client;
mod utils;

pub use crate::builder::GrpcClientBuilder;
pub use crate::client::GrpcClient;
pub use crate::error::{Error, GrpcClientBuilderError, Result};
pub use crate::signer::DocSigner;
pub use crate::tx::{SignDoc, TxConfig, TxInfo};
pub use crate::tx_client_impl::{ConfirmHandle, TransactionService, TxServiceConfig, TxUserError};
pub use crate::tx_client_v2::{NodeId, TxHandle, TxRequest};
pub use celestia_types::any::IntoProtobufAny;

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();

#[cfg(all(test, target_arch = "wasm32"))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);
