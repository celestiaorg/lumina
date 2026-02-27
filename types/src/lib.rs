#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]

pub mod any;
pub mod blob;
pub mod block;
mod byzantine;
pub mod consts;
mod data_availability_header;
pub mod eds;
mod error;
#[cfg(any(
    feature = "uniffi",
    all(target_arch = "wasm32", feature = "wasm-bindgen")
))]
pub mod evidence;
mod extended_header;
pub mod fraud_proof;
pub mod hash;
mod height;
mod merkle_proof;
pub mod namespace_data;
pub mod nmt;
pub mod row;
pub mod row_namespace_data;
pub mod sample;
pub mod serializers;
mod share;
#[cfg(any(
    feature = "uniffi",
    all(target_arch = "wasm32", feature = "wasm-bindgen")
))]
pub mod signature;
pub mod state;
mod sync;
#[cfg(any(test, feature = "test-utils"))]
#[cfg_attr(docsrs, doc(cfg(feature = "test-utils")))]
pub mod test_utils;
pub mod trust_level;
#[cfg(feature = "uniffi")]
pub mod uniffi_types;
mod validate;
mod validator_set;

pub use crate::blob::{Blob, Commitment};
pub use crate::block::Height;
pub use crate::consts::appconsts::AppVersion;
pub use crate::data_availability_header::*;
pub use crate::eds::{AxisType, ExtendedDataSquare};
pub use crate::error::*;
pub use crate::extended_header::*;
pub use crate::fraud_proof::FraudProof;
pub use crate::height::TryIntoHeight;
pub use crate::merkle_proof::MerkleProof;
pub use crate::share::*;
pub use crate::sync::*;
pub use crate::validate::*;

// `uniffi::use_remote_type` macro seems a bit limited in that it works correctly only
// for types that are exported in the root of the crate
#[cfg(feature = "uniffi")]
pub use crate::hash::Hash;

#[cfg(all(test, target_arch = "wasm32"))]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();
