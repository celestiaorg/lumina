use serde::{Deserialize, Serialize};

use celestia_types::hash::Hash;

use crate::Error;
use crate::grpc::{AsyncGrpcCall, TxPriority};

pub use celestia_proto::cosmos::tx::v1beta1::SignDoc;

/// A result of correctly submitted transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct TxInfo {
    /// Hash of the transaction.
    pub hash: Hash,
    /// Height at which transaction was submitted.
    pub height: u64,
}

/// Broadcasted, but still not confirmed transaction.
#[derive(Debug)]
pub struct SubmittedTx {
    broadcasted_tx: BroadcastedTx,
    confirm_tx: AsyncGrpcCall<TxInfo>,
}

impl SubmittedTx {
    pub(crate) fn new(tx: BroadcastedTx, confirm_tx: AsyncGrpcCall<TxInfo>) -> SubmittedTx {
        SubmittedTx {
            broadcasted_tx: tx,
            confirm_tx,
        }
    }

    /// Get reference to [`BroadcastedTx`]
    pub fn tx_ref(&self) -> &BroadcastedTx {
        &self.broadcasted_tx
    }

    /// Confirm the transaction once its application state is queryable and return [`TxInfo`].
    pub async fn confirm(self) -> Result<TxInfo, Error> {
        self.confirm_tx.await
    }
}

/// A transaction that was broadcasted
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct BroadcastedTx {
    /// Broadcasted bytes
    pub tx: Vec<u8>,
    /// Transaction hash
    pub hash: Hash,
    /// Transaction sequence
    pub sequence: u64,
}

const DEFAULT_CONFIRMATION_INTERVAL_MS: u64 = 500;

fn default_confirmation_interval_ms() -> u64 {
    DEFAULT_CONFIRMATION_INTERVAL_MS
}

/// Configuration for the transaction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
pub struct TxConfig {
    /// Custom gas limit for the transaction (in `utia`). By default, client will
    /// query gas estimation service to get estimate gas limit.
    pub gas_limit: Option<u64>,
    /// Custom gas price for fee calculation. By default, client will query gas
    /// estimation service to get gas price estimate.
    pub gas_price: Option<f64>,
    /// Memo for the transaction
    pub memo: Option<String>,
    /// Priority of the transaction, used with gas estimation service
    pub priority: TxPriority,
    /// Interval between confirmation polling attempts, in milliseconds.
    /// Defaults to 500ms.
    #[serde(default = "default_confirmation_interval_ms")]
    pub confirmation_interval_ms: u64,
}

impl TxConfig {
    /// Attach gas limit to this config.
    pub fn with_gas_limit(mut self, gas_limit: u64) -> Self {
        self.gas_limit = Some(gas_limit);
        self
    }

    /// Attach gas price to this config.
    pub fn with_gas_price(mut self, gas_price: f64) -> Self {
        self.gas_price = Some(gas_price);
        self
    }

    /// Attach memo to this config.
    pub fn with_memo(mut self, memo: impl Into<String>) -> Self {
        self.memo = Some(memo.into());
        self
    }

    /// Specify transaction priority to be used when using gas estimation service
    pub fn with_priority(mut self, priority: TxPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Specify the confirmation polling interval in milliseconds.
    pub fn with_confirmation_interval_ms(mut self, confirmation_interval_ms: u64) -> Self {
        self.confirmation_interval_ms = confirmation_interval_ms;
        self
    }
}

impl Default for TxConfig {
    fn default() -> Self {
        TxConfig {
            gas_limit: None,
            gas_price: None,
            memo: None,
            priority: TxPriority::default(),
            confirmation_interval_ms: DEFAULT_CONFIRMATION_INTERVAL_MS,
        }
    }
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
pub use wbg::*;

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
mod wbg {
    use super::{BroadcastedTx, TxConfig, TxInfo, TxPriority};
    use js_sys::{BigInt, Uint8Array};
    use lumina_utils::make_object;
    use wasm_bindgen::{JsCast, prelude::*};

    #[wasm_bindgen(typescript_custom_section)]
    const _: &str = "
    /**
     * Transaction info
     */
    export interface TxInfo {
      /**
       * Hash of the transaction.
       */
      hash: string;
      /**
       * Height at which transaction was submitted.
       */
      height: bigint;
    }

    /**
     * A transaction that was broadcasted
     */
    export interface BroadcastedTx {
      /**
       * Broadcasted bytes
       */
      tx: Uint8Array;
      /**
       * Transaction hash
       */
      hash: string;
      /**
       * Transaction sequence
       */
      sequence: bigint;
    }

    /**
     * Transaction config.
     */
    export interface TxConfig {
      /**
       * Custom gas limit for the transaction (in `utia`). By default, client will
       * query gas estimation service to get estimate gas limit.
       */
      gasLimit?: bigint; // utia
      /**
       * Custom gas price for fee calculation. By default, client will query gas
       * estimation service to get gas price estimate.
       */
      gasPrice?: number;
      /**
       * Memo for the transaction
       */
      memo?: string;
      /**
       * Priority of the transaction, used with gas estimation service
       */
      priority?: TxPriority;
      /**
       * Interval between confirmation polling attempts, in milliseconds.
       */
      confirmationIntervalMs?: bigint;
    }
    ";

    #[wasm_bindgen]
    extern "C" {
        /// TxInfo exposed to javascript
        #[wasm_bindgen(typescript_type = "TxInfo")]
        pub type JsTxInfo;

        /// BroadcastedTx exposed to javascript
        #[wasm_bindgen(typescript_type = "BroadcastedTx")]
        pub type JsBroadcastedTx;

        #[wasm_bindgen(method, getter, js_name = tx)]
        pub fn tx(this: &JsBroadcastedTx) -> Vec<u8>;

        #[wasm_bindgen(method, getter, js_name = hash)]
        pub fn hash(this: &JsBroadcastedTx) -> String;

        #[wasm_bindgen(method, getter, js_name = sequence)]
        pub fn sequence(this: &JsBroadcastedTx) -> BigInt;

        /// TxConfig exposed to javascript
        #[wasm_bindgen(typescript_type = "TxConfig")]
        pub type JsTxConfig;

        #[wasm_bindgen(method, getter, js_name = gasLimit)]
        pub fn gas_limit(this: &JsTxConfig) -> Option<u64>;

        #[wasm_bindgen(method, getter, js_name = gasPrice)]
        pub fn gas_price(this: &JsTxConfig) -> Option<f64>;

        #[wasm_bindgen(method, getter, js_name = memo)]
        pub fn memo(this: &JsTxConfig) -> Option<String>;

        #[wasm_bindgen(method, getter, js_name = priority)]
        pub fn priority(this: &JsTxConfig) -> Option<TxPriority>;

        #[wasm_bindgen(method, getter, js_name = confirmationIntervalMs)]
        pub fn confirmation_interval_ms(this: &JsTxConfig) -> Option<u64>;
    }

    impl From<TxInfo> for JsTxInfo {
        fn from(value: TxInfo) -> JsTxInfo {
            let obj = make_object!(
                "hash" => value.hash.to_string().into(),
                "height" => BigInt::from(value.height)
            );

            obj.unchecked_into()
        }
    }

    impl From<BroadcastedTx> for JsBroadcastedTx {
        fn from(value: BroadcastedTx) -> JsBroadcastedTx {
            let tx_bytes = Uint8Array::from(value.tx.as_slice());
            let obj = make_object!(
                "tx" => tx_bytes.into(),
                "hash" => value.hash.to_string().into(),
                "sequence" => BigInt::from(value.sequence)
            );

            obj.unchecked_into()
        }
    }

    impl TryFrom<JsBroadcastedTx> for BroadcastedTx {
        type Error = crate::Error;

        fn try_from(value: JsBroadcastedTx) -> Result<BroadcastedTx, Self::Error> {
            Ok(BroadcastedTx {
                tx: value.tx(),
                hash: value.hash().parse()?,
                sequence: value.sequence().try_into().map_err(|i| {
                    crate::Error::InvalidBroadcastedTx(format!("invalid sequence: {i}"))
                })?,
            })
        }
    }

    impl From<JsTxConfig> for TxConfig {
        fn from(value: JsTxConfig) -> TxConfig {
            TxConfig {
                gas_limit: value.gas_limit(),
                gas_price: value.gas_price(),
                memo: value.memo(),
                priority: value.priority().unwrap_or_default(),
                confirmation_interval_ms: value
                    .confirmation_interval_ms()
                    .unwrap_or(TxConfig::default().confirmation_interval_ms),
            }
        }
    }
}

impl From<SubmittedTx> for BroadcastedTx {
    fn from(value: SubmittedTx) -> Self {
        value.broadcasted_tx
    }
}

impl From<SubmittedTx> for AsyncGrpcCall<TxInfo> {
    fn from(value: SubmittedTx) -> Self {
        value.confirm_tx
    }
}

impl From<SubmittedTx> for (BroadcastedTx, AsyncGrpcCall<TxInfo>) {
    fn from(
        SubmittedTx {
            broadcasted_tx,
            confirm_tx,
        }: SubmittedTx,
    ) -> Self {
        (broadcasted_tx, confirm_tx)
    }
}
