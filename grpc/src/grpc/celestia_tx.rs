use std::fmt;
use std::str::FromStr;

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
use wasm_bindgen::prelude::*;

use celestia_proto::celestia::core::v1::tx::{
    TxStatusBatchRequest as RawTxStatusBatchRequest,
    TxStatusBatchResponse as RawTxStatusBatchResponse, TxStatusRequest as RawTxStatusRequest,
    TxStatusResponse as RawTxStatusResponse, TxStatusResult as RawTxStatusResult,
};
use celestia_types::Height;
use celestia_types::hash::Hash;
use celestia_types::state::ErrorCode;

use crate::grpc::{FromGrpcResponse, IntoGrpcParam};
use crate::{Error, Result};

#[cfg(feature = "uniffi")]
uniffi::use_remote_type!(celestia_types::Height);

/// Response to a tx status query
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "wasm-bindgen"),
    wasm_bindgen(getter_with_clone)
)]
pub struct TxStatusResponse {
    /// Height of the block in which the transaction was committed.
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "wasm-bindgen"),
        wasm_bindgen(skip)
    )]
    pub height: Height,
    /// Index of the transaction in block.
    pub index: u32,
    /// Execution_code is returned when the transaction has been committed
    /// and returns whether it was successful or errored. A non zero
    /// execution code indicates an error.
    pub execution_code: ErrorCode,
    /// Error log, if transaction failed.
    pub error: String,
    /// Status of the transaction.
    pub status: TxStatus,
}

/// Represents state of the transaction in the mempool
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
#[cfg_attr(all(target_arch = "wasm32", feature = "wasm-bindgen"), wasm_bindgen)]
pub enum TxStatus {
    /// The transaction is not known to the node, it could be never sent.
    Unknown,
    /// The transaction is still pending.
    Pending,
    /// The transaction was evicted from the mempool.
    Evicted,
    /// The transaction was rejected
    Rejected,
    /// The transaction was committed into the block.
    Committed,
}

/// Result entry returned in a batch tx status query.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct TxStatusResult {
    /// Transaction hash.
    pub hash: Hash,
    /// Status response for the transaction.
    pub status: TxStatusResponse,
}

/// Response to a batch tx status query.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct TxStatusBatchResponse {
    /// Status results for each requested transaction.
    pub statuses: Vec<TxStatusResult>,
}

impl fmt::Display for TxStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TxStatus::Unknown => "UNKNOWN",
            TxStatus::Pending => "PENDING",
            TxStatus::Evicted => "EVICTED",
            TxStatus::Rejected => "REJECTED",
            TxStatus::Committed => "COMMITTED",
        };
        write!(f, "{s}")
    }
}

impl FromStr for TxStatus {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "UNKNOWN" => Ok(TxStatus::Unknown),
            "PENDING" => Ok(TxStatus::Pending),
            "EVICTED" => Ok(TxStatus::Evicted),
            "REJECTED" => Ok(TxStatus::Rejected),
            "COMMITTED" => Ok(TxStatus::Committed),
            _ => Err(Error::FailedToParseResponse),
        }
    }
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
#[wasm_bindgen]
impl TxStatusResponse {
    /// Height of the block in which the transaction was committed.
    #[wasm_bindgen(getter)]
    pub fn height(&self) -> u64 {
        self.height.value()
    }
}

impl TryFrom<RawTxStatusResponse> for TxStatusResponse {
    type Error = Error;

    fn try_from(value: RawTxStatusResponse) -> Result<TxStatusResponse, Self::Error> {
        Ok(TxStatusResponse {
            height: value.height.try_into()?,
            index: value.index,
            execution_code: value.execution_code.try_into()?,
            error: value.error,
            status: value.status.parse()?,
        })
    }
}

impl IntoGrpcParam<RawTxStatusRequest> for Hash {
    fn into_parameter(self) -> RawTxStatusRequest {
        RawTxStatusRequest {
            tx_id: self.to_string(),
        }
    }
}

impl<I> IntoGrpcParam<RawTxStatusBatchRequest> for I
where
    I: IntoIterator<Item = Hash>,
{
    fn into_parameter(self) -> RawTxStatusBatchRequest {
        RawTxStatusBatchRequest {
            tx_ids: self.into_iter().map(|hash| hash.to_string()).collect(),
        }
    }
}

impl FromGrpcResponse<TxStatusResponse> for RawTxStatusResponse {
    fn try_from_response(self) -> Result<TxStatusResponse> {
        self.try_into()
    }
}

impl FromGrpcResponse<TxStatusBatchResponse> for RawTxStatusBatchResponse {
    fn try_from_response(self) -> Result<TxStatusBatchResponse> {
        let mut statuses = Vec::with_capacity(self.statuses.len());
        for RawTxStatusResult { tx_hash, status } in self.statuses {
            let hash = tx_hash.parse().map_err(|_| Error::FailedToParseResponse)?;
            let status = status.ok_or(Error::FailedToParseResponse)?.try_into()?;
            statuses.push(TxStatusResult { hash, status });
        }
        Ok(TxStatusBatchResponse { statuses })
    }
}
