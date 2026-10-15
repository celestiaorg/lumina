use celestia_proto::cosmos::base::node::v1beta1::{
    ConfigRequest, ConfigResponse as RawConfigResponse, StatusRequest,
    StatusResponse as RawStatusResponse,
};
use celestia_types::state::BOND_DENOM;
use serde::{Deserialize, Serialize};
#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
use wasm_bindgen::prelude::*;

use crate::grpc::{FromGrpcResponse, make_empty_params};
use crate::{Error, Result};

/// Response holding consensus node configuration.
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[cfg_attr(all(target_arch = "wasm32", feature = "wasm-bindgen"), wasm_bindgen)]
pub struct ConfigResponse {
    /// Minimum gas price for the node to accept tx. Value is in `utia` denom.
    pub minimum_gas_price: Option<f64>,

    /// How many recent blocks are stored by the node.
    pub pruning_keep_recent: u64,

    /// Amount of blocks used as an interval to trigger prunning.
    pub pruning_interval: u64,

    /// A height at which the node should stop advancing state.
    pub halt_height: u64,
}

impl FromGrpcResponse<ConfigResponse> for RawConfigResponse {
    fn try_from_response(self) -> Result<ConfigResponse> {
        let minimum_gas_price = if self.minimum_gas_price.is_empty() {
            None
        } else {
            Some(
                self.minimum_gas_price
                    .strip_suffix(BOND_DENOM)
                    .ok_or(Error::FailedToParseResponse)?
                    .parse()
                    .map_err(|_| Error::FailedToParseResponse)?,
            )
        };

        Ok(ConfigResponse {
            minimum_gas_price,
            pruning_keep_recent: self
                .pruning_keep_recent
                .parse()
                .map_err(|_| Error::FailedToParseResponse)?,
            pruning_interval: self
                .pruning_interval
                .parse()
                .map_err(|_| Error::FailedToParseResponse)?,
            halt_height: self.halt_height,
        })
    }
}

impl FromGrpcResponse<u64> for RawStatusResponse {
    fn try_from_response(self) -> Result<u64> {
        Ok(self.height)
    }
}

make_empty_params!(ConfigRequest);
make_empty_params!(StatusRequest);
