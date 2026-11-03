use celestia_proto::cosmos::base::tendermint::v1beta1::{
    AbciQueryResponse as RawAbciQueryResponse, ProofOps,
};
use serde::{Deserialize, Serialize};
#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
use wasm_bindgen::prelude::*;

use crate::state::ErrorCode;
use crate::{Error, Height, validation_error};

/// Response to a tx query
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "wasm-bindgen"),
    wasm_bindgen(getter_with_clone)
)]
pub struct AbciQueryResponse {
    /// The block height from which data was derived.
    ///
    /// Note that this is the height of the block containing the application's Merkle root hash,
    /// which represents the state as it was after committing the block at height - 1.
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "wasm-bindgen"),
        wasm_bindgen(skip)
    )]
    pub height: Height,

    /// Response code.
    pub code: ErrorCode,

    /// Namespace for the Code.
    pub codespace: String,

    /// The index of the key in the tree.
    pub index: u64,

    /// The key of the matching data.
    pub key: Vec<u8>,

    /// The value of the matching data.
    pub value: Vec<u8>,

    /// Serialized proof for the value data, if requested,
    /// to be verified against the [`AppHash`] for the given [`Height`].
    ///
    /// [`AppHash`]: tendermint::hash::AppHash
    pub proof_ops: Option<ProofOps>,

    /// The output of the application's logger (raw string). May be
    /// non-deterministic.
    pub log: String,

    /// Additional information. May be non-deterministic.
    pub info: String,
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
#[wasm_bindgen]
impl AbciQueryResponse {
    /// The block height from which data was derived.
    ///
    /// Note that this is the height of the block containing the application's Merkle root hash,
    /// which represents the state as it was after committing the block at height - 1.
    #[wasm_bindgen(getter)]
    pub fn height(&self) -> u64 {
        self.height.value()
    }
}

impl TryFrom<RawAbciQueryResponse> for AbciQueryResponse {
    type Error = Error;

    fn try_from(response: RawAbciQueryResponse) -> Result<AbciQueryResponse, Self::Error> {
        Ok(AbciQueryResponse {
            height: response.height.try_into()?,
            code: ErrorCode::from_abci(&response.codespace, response.code),
            codespace: response.codespace,
            index: response
                .index
                .try_into()
                .map_err(|_| validation_error!("Negative index"))?,
            key: response.key,
            value: response.value,
            proof_ops: response.proof_ops,
            log: response.log,
            info: response.info,
        })
    }
}
