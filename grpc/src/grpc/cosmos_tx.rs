use celestia_proto::cosmos::base::abci::v1beta1::GasInfo;
use celestia_proto::cosmos::tx::v1beta1::{
    BroadcastTxRequest, BroadcastTxResponse, GetTxRequest as RawGetTxRequest,
    GetTxResponse as RawGetTxResponse, GetTxsEventRequest, GetTxsEventResponse, SimulateRequest,
    SimulateResponse,
};
use celestia_types::hash::Hash;
use celestia_types::state::{Tx, TxResponse};
#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
use wasm_bindgen::prelude::*;

use crate::grpc::{FromGrpcResponse, IntoGrpcParam};
use crate::{Error, Result};

pub use celestia_proto::cosmos::tx::v1beta1::BroadcastMode;

/// Response to GetTx
#[derive(Debug)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "wasm-bindgen"),
    wasm_bindgen(getter_with_clone)
)]
pub struct GetTxResponse {
    /// Response Transaction
    pub tx: Tx,

    /// TxResponse to a Query
    pub tx_response: TxResponse,
}

impl IntoGrpcParam<BroadcastTxRequest> for (Vec<u8>, BroadcastMode) {
    fn into_parameter(self) -> BroadcastTxRequest {
        let (tx_bytes, mode) = self;

        BroadcastTxRequest {
            tx_bytes,
            mode: mode.into(),
        }
    }
}

impl FromGrpcResponse<TxResponse> for BroadcastTxResponse {
    fn try_from_response(self) -> Result<TxResponse> {
        Ok(self
            .tx_response
            .ok_or(Error::FailedToParseResponse)?
            .try_into()?)
    }
}

impl IntoGrpcParam<RawGetTxRequest> for Hash {
    fn into_parameter(self) -> RawGetTxRequest {
        RawGetTxRequest {
            hash: self.to_string(),
        }
    }
}

impl FromGrpcResponse<GetTxResponse> for RawGetTxResponse {
    fn try_from_response(self) -> Result<GetTxResponse> {
        let tx_response = self
            .tx_response
            .ok_or(Error::FailedToParseResponse)?
            .try_into()?;

        let tx = self.tx.ok_or(Error::FailedToParseResponse)?;

        let cosmos_tx = Tx {
            body: tx.body.ok_or(Error::FailedToParseResponse)?.try_into()?,
            auth_info: tx
                .auth_info
                .ok_or(Error::FailedToParseResponse)?
                .try_into()?,
            signatures: tx.signatures,
        };

        Ok(GetTxResponse {
            tx: cosmos_tx,
            tx_response,
        })
    }
}

impl IntoGrpcParam<GetTxsEventRequest> for GetTxsEventRequest {
    fn into_parameter(self) -> GetTxsEventRequest {
        self
    }
}

impl FromGrpcResponse<GetTxsEventResponse> for GetTxsEventResponse {
    fn try_from_response(self) -> Result<GetTxsEventResponse> {
        Ok(self)
    }
}

impl IntoGrpcParam<SimulateRequest> for Vec<u8> {
    fn into_parameter(self) -> SimulateRequest {
        SimulateRequest {
            tx_bytes: self,
            ..SimulateRequest::default()
        }
    }
}

impl FromGrpcResponse<GasInfo> for SimulateResponse {
    fn try_from_response(self) -> Result<GasInfo> {
        self.gas_info.ok_or(Error::FailedToParseResponse)
    }
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
pub use wbg::*;

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
mod wbg {
    use super::BroadcastMode;
    use wasm_bindgen::prelude::*;

    /// BroadcastMode specifies the broadcast mode for the TxService.Broadcast RPC method.
    #[derive(Debug, Clone, Copy)]
    #[wasm_bindgen(js_name = BroadcastMode)]
    pub struct JsBroadcastMode(BroadcastMode);

    #[wasm_bindgen(js_class = BroadcastMode)]
    impl JsBroadcastMode {
        /// zero-value for mode ordering
        #[wasm_bindgen(js_name = Unspecified, getter)]
        pub fn unspecified() -> JsBroadcastMode {
            BroadcastMode::Unspecified.into()
        }

        /// DEPRECATED: use `BroadcastMode` `Sync` instead,
        /// `BroadcastMode` `Block` is not supported by the SDK from v0.47.x onwards.
        #[wasm_bindgen(js_name = Block, getter)]
        pub fn block() -> JsBroadcastMode {
            BroadcastMode::Block.into()
        }

        /// `BroadcastMode` `Sync` defines a tx broadcasting mode where the client waits for
        /// a CheckTx execution response only.
        #[wasm_bindgen(js_name = Sync, getter)]
        pub fn sync() -> JsBroadcastMode {
            BroadcastMode::Sync.into()
        }

        /// `BroadcastMode` `Async` defines a tx broadcasting mode where the client returns
        /// immediately.
        #[wasm_bindgen(js_name = Async, getter)]
        pub fn _async() -> JsBroadcastMode {
            BroadcastMode::Async.into()
        }
    }

    impl From<BroadcastMode> for JsBroadcastMode {
        fn from(value: BroadcastMode) -> Self {
            JsBroadcastMode(value)
        }
    }

    impl From<JsBroadcastMode> for BroadcastMode {
        fn from(value: JsBroadcastMode) -> Self {
            value.0
        }
    }
}
