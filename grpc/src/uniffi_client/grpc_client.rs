//! GRPC client wrapper for uniffi

use std::sync::Arc;

use prost::Message;
use tendermint_proto::google::protobuf::Any;
use uniffi::{Object, Record};

use celestia_types::Blob;
use celestia_types::blob::BlobParams;
use celestia_types::block::Block;
use celestia_types::hash::uniffi_types::UniffiHash;
use celestia_types::state::auth::{Account, AuthParams};
use celestia_types::state::{AbciQueryResponse, Coin, TxResponse};
use celestia_types::{ExtendedHeader, UniffiConversionError};

use crate::grpc::{
    BroadcastMode, ConfigResponse, GasEstimate, GasInfo, GetTxResponse, TxPriority,
    TxStatusResponse,
};
use crate::tx::{BroadcastedTx, TxInfo};
use crate::{IntoProtobufAny, SignDoc, TxConfig};

/// Alias for a `Result` with the error type [`GrpcClientError`]
pub type Result<T, E = GrpcClientError> = std::result::Result<T, E>;

/// Errors returned from [`GrpcClient`]
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum GrpcClientError {
    /// Error from grpc client
    #[error("grpc error: {msg}")]
    GrpcError {
        /// error message
        msg: String,
    },

    /// Error during uniffi types conversion
    #[error("uniffi conversion error: {msg}")]
    UniffiConversionError {
        /// error message
        msg: String,
    },

    /// Invalid account public key
    #[error("invalid account public key")]
    InvalidAccountPublicKey {
        /// error message
        msg: String,
    },

    /// Invalid account id
    #[error("invalid account id: {msg}")]
    InvalidAccountId {
        ///error message
        msg: String,
    },

    /// Error occured during signing
    #[error("error while signing: {msg}")]
    SigningError {
        /// error message
        msg: String,
    },
}

/// Celestia GRPC client
#[derive(Object)]
pub struct GrpcClient {
    client: crate::GrpcClient,
}

/// Any contains an arbitrary serialized protocol buffer message along with a URL that
/// describes the type of the serialized message.
#[derive(Record)]
pub struct AnyMsg {
    /// A URL/resource name that uniquely identifies the type of the serialized protocol
    /// buffer message. This string must contain at least one “/” character. The last
    /// segment of the URL’s path must represent the fully qualified name of the type
    /// (as in path/google.protobuf.Duration). The name should be in a canonical form
    /// (e.g., leading “.” is not accepted).
    pub r#type: String,
    /// Must be a valid serialized protocol buffer of the above specified type.
    pub value: Vec<u8>,
}

#[uniffi::export(async_runtime = "tokio")]
impl GrpcClient {
    /// Get auth params
    #[uniffi::method(name = "getAuthParams")]
    pub async fn get_auth_params(&self) -> Result<AuthParams> {
        Ok(self.client.get_auth_params().await?)
    }

    /// Get account
    #[uniffi::method(name = "getAccount")]
    pub async fn get_account(&self, account: &str) -> Result<Account> {
        Ok(self.client.get_account(&account.parse()?).await?)
    }

    /// Get accounts
    #[uniffi::method(name = "getAccounts")]
    pub async fn get_accounts(&self) -> Result<Vec<Account>> {
        Ok(self.client.get_accounts().await?)
    }

    /// Retrieves the verified Celestia coin balance for the address.
    ///
    /// # Notes
    ///
    /// This returns the verified balance which is the one that was reported by
    /// the previous network block. In other words, if you transfer some coins,
    /// you need to wait 1 more block in order to see the new balance. If you want
    /// something more immediate then use [`GrpcClient::get_balance`].
    ///
    /// `header` argument is a json encoded [`ExtendedHeader`]
    ///
    /// [`ExtendedHeader`]: https://docs.rs/celestia-types/latest/celestia_types/struct.ExtendedHeader.html
    #[uniffi::method(name = "getVerifiedBalance")]
    pub async fn get_verified_balance(&self, address: &str, header: &str) -> Result<Coin> {
        let header: ExtendedHeader = serde_json::from_str(header)
            .map_err(|e| GrpcClientError::UniffiConversionError { msg: e.to_string() })?;
        Ok(self
            .client
            .get_verified_balance(&address.parse()?, &header)
            .await?)
    }

    /// Retrieves the Celestia coin balance for the given address.
    #[uniffi::method(name = "getBalance")]
    pub async fn get_balance(&self, address: &str, denom: String) -> Result<Coin> {
        Ok(self.client.get_balance(&address.parse()?, denom).await?)
    }

    /// Get balance of all coins
    #[uniffi::method(name = "getAllBalances")]
    pub async fn get_all_balances(&self, address: &str) -> Result<Vec<Coin>> {
        Ok(self.client.get_all_balances(&address.parse()?).await?)
    }

    /// Get balance of all spendable coins
    #[uniffi::method(name = "getSpendableBalances")]
    pub async fn get_spendable_balances(&self, address: &str) -> Result<Vec<Coin>> {
        Ok(self
            .client
            .get_spendable_balances(&address.parse()?)
            .await?)
    }

    /// Get total supply
    #[uniffi::method(name = "getTotalSupply")]
    pub async fn get_total_supply(&self) -> Result<Vec<Coin>> {
        Ok(self.client.get_total_supply().await?)
    }

    /// Get node configuration
    #[uniffi::method(name = "getNodeConfig")]
    pub async fn get_node_config(&self) -> Result<ConfigResponse> {
        Ok(self.client.get_node_config().await?)
    }

    /// Get latest block
    #[uniffi::method(name = "getLatestBlock")]
    pub async fn get_latest_block(&self) -> Result<Block> {
        Ok(self.client.get_latest_block().await?)
    }

    /// Get block by height
    #[uniffi::method(name = "getBlockByHeight")]
    pub async fn get_block_by_height(&self, height: i64) -> Result<Block> {
        Ok(self.client.get_block_by_height(height).await?)
    }

    /// Issue a direct ABCI query to the application
    #[uniffi::method(name = "abciQuery")]
    async fn abci_query(
        &self,
        data: &[u8],
        path: &str,
        height: u64,
        prove: bool,
    ) -> Result<AbciQueryResponse> {
        Ok(self.client.abci_query(data, path, height, prove).await?)
    }

    /// Broadcast prepared and serialised transaction
    #[uniffi::method(name = "broadcastTx")]
    pub async fn broadcast_tx(&self, tx_bytes: Vec<u8>, mode: BroadcastMode) -> Result<TxResponse> {
        Ok(self.client.broadcast_tx(tx_bytes, mode).await?)
    }

    /// Get Tx
    #[uniffi::method(name = "getTx")]
    pub async fn get_tx(&self, hash: UniffiHash) -> Result<GetTxResponse> {
        Ok(self.client.get_tx(hash.try_into()?).await?)
    }

    /// Broadcast prepared and serialised transaction
    pub async fn simulate(&self, tx_bytes: Vec<u8>) -> Result<GasInfo> {
        Ok(self.client.simulate(tx_bytes).await?)
    }

    /// Get blob params
    #[uniffi::method(name = "getBlobParams")]
    pub async fn get_blob_params(&self) -> Result<BlobParams> {
        Ok(self.client.get_blob_params().await?)
    }

    /// Get status of the transaction
    #[uniffi::method(name = "txStatus")]
    pub async fn tx_status(&self, hash: UniffiHash) -> Result<TxStatusResponse> {
        Ok(self.client.tx_status(hash.try_into()?).await?)
    }

    /// Estimate gas price for given transaction priority based
    /// on the gas prices of the transactions in the last five blocks.
    ///
    /// If no transaction is found in the last five blocks, return the network
    /// min gas price.
    #[uniffi::method(name = "estimateGasPrice")]
    pub async fn estimate_gas_price(&self, priority: TxPriority) -> Result<f64> {
        Ok(self.client.estimate_gas_price(priority).await?)
    }

    /// Estimate gas price for transaction with given priority and estimate gas usage
    /// for provided serialised transaction.
    ///
    /// The gas price estimation is based on the gas prices of the transactions
    /// in the last five blocks. If no transaction is found in the last five blocks,
    /// it returns the network min gas price.
    ///
    /// The gas used is estimated using the state machine simulation.
    #[uniffi::method(name = "estimateGasPriceAndUsage")]
    pub async fn estimate_gas_price_and_usage(
        &self,
        priority: TxPriority,
        tx_bytes: Vec<u8>,
    ) -> Result<GasEstimate> {
        Ok(self
            .client
            .estimate_gas_price_and_usage(priority, tx_bytes)
            .await?)
    }

    /// Submit blobs to the celestia network.
    #[uniffi::method(name = "submitBlobs")]
    pub async fn submit_blobs(
        &self,
        blobs: Vec<Arc<Blob>>,
        config: Option<TxConfig>,
    ) -> Result<TxInfo> {
        let blobs = Vec::from_iter(blobs.into_iter().map(Arc::<Blob>::unwrap_or_clone));
        let config = config.unwrap_or_default();
        Ok(self.client.submit_blobs(&blobs, config).await?)
    }

    /// Submit message to the celestia network.
    #[uniffi::method(name = "submitMessage")]
    pub async fn submit_message(
        &self,
        message: AnyMsg,
        config: Option<TxConfig>,
    ) -> Result<TxInfo> {
        let config = config.unwrap_or_default();
        Ok(self.client.submit_message(message, config).await?)
    }

    /// Broadcast message to the celestia network, and return without confirming.
    #[uniffi::method(name = "broadcastMessage")]
    pub async fn broadcast_message(
        &self,
        message: AnyMsg,
        config: Option<TxConfig>,
    ) -> Result<BroadcastedTx> {
        let config = config.unwrap_or_default();
        let submitted_tx = self.client.broadcast_message(message, config).await?;
        Ok(submitted_tx.into())
    }

    /// Broadcast blobs to the celestia network, and return without confirming.
    #[uniffi::method(name = "broadcastBlobs")]
    pub async fn broadcast_blobs(
        &self,
        blobs: Vec<Arc<Blob>>,
        config: Option<TxConfig>,
    ) -> Result<BroadcastedTx> {
        let blobs = Vec::from_iter(blobs.into_iter().map(Arc::<Blob>::unwrap_or_clone));
        let config = config.unwrap_or_default();
        let submitted_tx = self.client.broadcast_blobs(&blobs, config).await?;
        Ok(submitted_tx.into())
    }

    /// Confirm transaction broadcasted with broadcast_blobs or broadcast_message.
    #[uniffi::method(name = "confirmTx")]
    pub async fn confirm_tx(
        &self,
        broadcasted_tx: BroadcastedTx,
        config: Option<TxConfig>,
    ) -> Result<TxInfo> {
        let config = config.unwrap_or_default();
        Ok(self
            .client
            .confirm_broadcasted_tx(broadcasted_tx, config)
            .await?)
    }
}

impl IntoProtobufAny for AnyMsg {
    fn into_any(self) -> Any {
        Any {
            type_url: self.r#type,
            value: self.value,
        }
    }
}

impl From<crate::GrpcClient> for GrpcClient {
    fn from(client: crate::GrpcClient) -> GrpcClient {
        GrpcClient { client }
    }
}

impl From<crate::Error> for GrpcClientError {
    fn from(value: crate::Error) -> Self {
        GrpcClientError::GrpcError {
            msg: value.to_string(),
        }
    }
}

impl From<UniffiConversionError> for GrpcClientError {
    fn from(value: UniffiConversionError) -> Self {
        GrpcClientError::GrpcError {
            msg: value.to_string(),
        }
    }
}

#[uniffi::export]
pub fn proto_encode_sign_doc(sign_doc: SignDoc) -> Vec<u8> {
    sign_doc.encode_to_vec()
}

impl From<celestia_types::Error> for GrpcClientError {
    fn from(value: celestia_types::Error) -> Self {
        crate::Error::from(value).into()
    }
}
