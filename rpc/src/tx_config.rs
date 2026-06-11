use celestia_types::state::AccAddress;
use serde::{
    Deserialize, Serialize,
    ser::{SerializeStruct, Serializer},
};
use serde_repr::{Deserialize_repr, Serialize_repr};

/// Transaction priority for gas price estimation.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Debug,
    Default,
    Serialize_repr,
    Deserialize_repr,
)]
#[repr(u8)]
pub enum TxPriority {
    /// Estimated gas price is the value at the end of the lowest 10% of gas prices from the last 5 blocks.
    Low = 1,
    /// Estimated gas price is the mean of all gas prices from the last 5 blocks.
    #[default]
    Medium = 2,
    /// Estimated gas price is the price at the start of the top 10% of transactions’ gas prices from the last 5 blocks.
    High = 3,
}

/// [`TxConfig`] specifies additional options that are be applied to the Tx.
///
/// If no options are provided, then the default ones will be used.
/// Read more about the mechanisms of fees and gas usage in [`submitting data blobs`].
///
/// [`submitting data blobs`]: https://docs.celestia.org/developers/submit-data#fees-and-gas-limits
#[derive(Debug, Default, Deserialize)]
pub struct TxConfig {
    /// Specifies the address from the keystore that will sign transactions.
    ///
    /// # NOTE
    ///
    /// Only `signer_address` or `key_name` should be passed. `signer_address` is a primary cfg.
    /// This means If both the address and the key are specified, the address field will take priority.
    pub signer_address: Option<AccAddress>,
    /// Specifies the key from the keystore associated with an account that will be used to sign transactions.
    ///
    /// # NOTE
    ///
    /// This account must be available in the Keystore.
    pub key_name: Option<String>,
    /// Represents the amount to be paid per gas unit.
    ///
    /// Negative or missing `gas_price` means user want us to use the minGasPrice defined in the node.
    pub gas_price: Option<f64>,
    /// Represents the maximal amount to be paid per gas unit.
    pub max_gas_price: Option<f64>,
    /// Calculated amount of gas to be used by transaction.
    ///
    /// `0` or missing `gas` means that the node should calculate it itself.
    pub gas: Option<u64>,
    /// Transaction priority level used when estimating the gas price.
    pub priority: Option<TxPriority>,
    /// Specifies the account that will pay for the transaction.
    pub fee_granter_address: Option<AccAddress>,
}

impl TxConfig {
    /// Sets the [`gas_price`] of the transaction.
    ///
    /// [`gas_price`]: TxConfig::gas_price
    pub fn with_gas_price(mut self, gas_price: f64) -> Self {
        self.gas_price = Some(gas_price);
        self
    }

    /// Sets the [`max_gas_price`] of the transaction.
    ///
    /// [`max_gas_price`]: TxConfig::max_gas_price
    pub fn with_max_gas_price(mut self, max_gas_price: f64) -> Self {
        self.max_gas_price = Some(max_gas_price);
        self
    }

    /// Sets the [`gas`] of the transaction.
    ///
    /// [`gas`]: TxConfig::gas
    pub fn with_gas(mut self, gas: u64) -> Self {
        self.gas = Some(gas);
        self
    }

    /// Sets the [`priority`] of the transaction.
    ///
    /// [`priority`]: TxConfig::priority
    pub fn with_priority(mut self, priority: TxPriority) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Sets the [`fee_granter_address`] of the transaction.
    ///
    /// [`fee_granter_address`]: TxConfig::fee_granter_address
    pub fn with_fee_granter_address(mut self, fee_granter_address: AccAddress) -> Self {
        self.fee_granter_address = Some(fee_granter_address);
        self
    }

    /// Sets the [`signer_address`] of the transaction.
    ///
    /// [`signer_address`]: TxConfig::signer_address
    pub fn with_signer_address(mut self, signer_address: AccAddress) -> Self {
        self.signer_address = Some(signer_address);
        self
    }

    /// Sets the [`key_name`] of the transaction.
    ///
    /// [`key_name`]: TxConfig::key_name
    pub fn with_key_name(mut self, key_name: impl Into<String>) -> Self {
        self.key_name = Some(key_name.into());
        self
    }
}

impl Serialize for TxConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TxConfig", 6)?;

        if let Some(signer_address) = &self.signer_address {
            state.serialize_field("signer_address", signer_address)?;
        }
        if let Some(key_name) = &self.key_name {
            state.serialize_field("key_name", key_name)?;
        }

        if let Some(gas_price) = &self.gas_price {
            state.serialize_field("gas_price", gas_price)?;
            state.serialize_field("is_gas_price_set", &true)?;
        }

        if let Some(max_gas_price) = &self.max_gas_price {
            state.serialize_field("max_gas_price", max_gas_price)?;
        }

        if let Some(gas) = &self.gas {
            state.serialize_field("gas", gas)?;
        }

        if let Some(priority) = &self.priority {
            state.serialize_field("priority", priority)?;
        }

        if let Some(fee_granter_address) = &self.fee_granter_address {
            state.serialize_field("fee_granter_address", fee_granter_address)?;
        }

        state.end()
    }
}
