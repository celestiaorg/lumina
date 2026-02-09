use send_wrapper::SendWrapper;
use tendermint_proto::google::protobuf::Any;
use wasm_bindgen::prelude::*;

use celestia_types::any::{IntoProtobufAny, JsAny};
use celestia_types::blob::BlobParams;
use celestia_types::block::Block;
use celestia_types::consts::appconsts::JsAppVersion;
use celestia_types::state::auth::{JsAuthParams, JsBaseAccount};
use celestia_types::state::{AbciQueryResponse, JsCoin, TxResponse};
use celestia_types::{Blob, ExtendedHeader};

use crate::Result;
use crate::grpc::{
    ConfigResponse, GasInfo, GetTxResponse, JsBroadcastMode, TxPriority, TxStatusResponse,
};
use crate::js_client::{GrpcClientBuilder, endpoints_from_js};
use crate::tx::{BroadcastedTx, JsBroadcastedTx, JsTxConfig, JsTxInfo};

/// Celestia gRPC client, for builder see [`GrpcClientBuilder`]
#[wasm_bindgen]
pub struct GrpcClient {
    client: crate::GrpcClient,
}

#[wasm_bindgen]
impl GrpcClient {
    /// Create a builder for [`GrpcClient`] connected to `url`.
    ///
    /// Accepts a string, an `Endpoint`, or an array of strings/endpoints.
    ///
    /// # Example
    ///
    /// ```js
    /// const client = await GrpcClient.withUrl("http://localhost:18080").build();
    /// ```
    #[wasm_bindgen(js_name = withUrl)]
    pub fn with_url(
        #[wasm_bindgen(unchecked_param_type = "UrlsOrEndpoints")] url: JsValue,
    ) -> Result<GrpcClientBuilder, JsValue> {
        let endpoints = endpoints_from_js(url)?;
        Ok(crate::GrpcClientBuilder::new().endpoints(endpoints).into())
    }

    /// Create a builder for [`GrpcClient`] with multiple URL endpoints for fallback support.
    ///
    /// Accepts a string, an `Endpoint`, or an array of strings/endpoints.
    ///
    /// When multiple endpoints are configured, the client will automatically
    /// fall back to the next endpoint if a network-related error occurs.
    ///
    /// # Example
    ///
    /// ```js
    /// const client = await GrpcClient.withUrls(["http://primary:9090", "http://fallback:9090"]).build();
    /// ```
    #[wasm_bindgen(js_name = withUrls)]
    pub fn with_urls(
        #[wasm_bindgen(unchecked_param_type = "UrlsOrEndpoints")] urls: JsValue,
    ) -> Result<GrpcClientBuilder, JsValue> {
        let endpoints = endpoints_from_js(urls)?;
        Ok(crate::GrpcClientBuilder::new().endpoints(endpoints).into())
    }

    /// Get auth params
    #[wasm_bindgen(js_name = getAuthParams)]
    pub async fn get_auth_params(&self) -> Result<JsAuthParams> {
        Ok(self.client.get_auth_params().await?.into())
    }

    /// Get account
    #[wasm_bindgen(js_name = getAccount)]
    pub async fn get_account(&self, account: &str) -> Result<JsBaseAccount> {
        Ok(self.client.get_account(&account.parse()?).await?.into())
    }

    // cosmos.bank

    /// Get accounts
    #[wasm_bindgen(js_name = getAccounts)]
    pub async fn get_accounts(&self) -> Result<Vec<JsBaseAccount>> {
        Ok(self
            .client
            .get_accounts()
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// Retrieves the verified Celestia coin balance for the address.
    ///
    /// # Notes
    ///
    /// This returns the verified balance which is the one that was reported by
    /// the previous network block. In other words, if you transfer some coins,
    /// you need to wait 1 more block in order to see the new balance. If you want
    /// something more immediate then use [`GrpcClient::get_balance`].
    #[wasm_bindgen(js_name = getVerifiedBalance)]
    pub async fn get_verified_balance(
        &self,
        address: &str,
        header: &ExtendedHeader,
    ) -> Result<JsCoin> {
        Ok(self
            .client
            .get_verified_balance(&address.parse()?, header)
            .await?
            .into())
    }

    /// Retrieves the Celestia coin balance for the given address.
    #[wasm_bindgen(js_name = getBalance)]
    pub async fn get_balance(&self, address: &str, denom: &str) -> Result<JsCoin> {
        Ok(self
            .client
            .get_balance(&address.parse()?, denom)
            .await?
            .into())
    }

    /// Get balance of all coins
    #[wasm_bindgen(js_name = getAllBalances)]
    pub async fn get_all_balances(&self, address: &str) -> Result<Vec<JsCoin>> {
        Ok(self
            .client
            .get_all_balances(&address.parse()?)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// Get balance of all spendable coins
    #[wasm_bindgen(js_name = getSpendableBalances)]
    pub async fn get_spendable_balances(&self, address: &str) -> Result<Vec<JsCoin>> {
        Ok(self
            .client
            .get_spendable_balances(&address.parse()?)
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// Get total supply
    #[wasm_bindgen(js_name = getTotalSupply)]
    pub async fn get_total_supply(&self) -> Result<Vec<JsCoin>> {
        Ok(self
            .client
            .get_total_supply()
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// Get node configuration
    pub async fn get_node_config(&self) -> Result<ConfigResponse> {
        self.client.get_node_config().await
    }

    /// Get latest block
    #[wasm_bindgen(js_name = getLatestBlock)]
    pub async fn get_latest_block(&self) -> Result<Block> {
        self.client.get_latest_block().await
    }

    /// Get block by height
    #[wasm_bindgen(js_name = getBlockByHeight)]
    pub async fn get_block_by_height(&self, height: i64) -> Result<Block> {
        self.client.get_block_by_height(height).await
    }

    /// Issue a direct ABCI query to the application
    #[wasm_bindgen(js_name = abciQuery)]
    pub async fn abci_query(
        &self,
        data: Vec<u8>,
        path: &str,
        height: u64,
        prove: bool,
    ) -> Result<AbciQueryResponse> {
        self.client.abci_query(data, path, height, prove).await
    }

    /// Broadcast prepared and serialised transaction
    #[wasm_bindgen(js_name = broadcastTx)]
    pub async fn broadcast_tx(
        &self,
        tx_bytes: Vec<u8>,
        mode: &JsBroadcastMode,
    ) -> Result<TxResponse> {
        self.client.broadcast_tx(tx_bytes, (*mode).into()).await
    }

    /// Get Tx
    #[wasm_bindgen(js_name = getTx)]
    pub async fn get_tx(&self, hash: &str) -> Result<GetTxResponse> {
        self.client.get_tx(hash.parse()?).await
    }

    /// Simulate prepared and serialised transaction, returning simulated gas usage
    pub async fn simulate(&self, tx_bytes: Vec<u8>) -> Result<GasInfo> {
        self.client.simulate(tx_bytes).await
    }

    /// Get blob params
    #[wasm_bindgen(js_name = getBlobParams)]
    pub async fn get_blob_params(&self) -> Result<BlobParams> {
        self.client.get_blob_params().await
    }

    /// Get status of the transaction
    #[wasm_bindgen(js_name = txStatus)]
    pub async fn tx_status(&self, hash: &str) -> Result<TxStatusResponse> {
        self.client.tx_status(hash.parse()?).await
    }

    /// Estimate gas price for given transaction priority based
    /// on the gas prices of the transactions in the last five blocks.
    ///
    /// If no transaction is found in the last five blocks, return the network
    /// min gas price.
    #[wasm_bindgen(js_name = estimateGasPrice)]
    pub async fn estimate_gas_price(&self, priority: TxPriority) -> Result<f64> {
        self.client.estimate_gas_price(priority).await
    }

    /// Chain id of the client
    #[wasm_bindgen(js_name = chainId, getter)]
    pub async fn chain_id(&self) -> Result<String> {
        Ok(self.client.chain_id().await?.to_string())
    }

    /// AppVersion of the client
    #[wasm_bindgen(js_name = appVersion, getter)]
    pub async fn app_version(&self) -> Result<JsAppVersion> {
        Ok(self.client.app_version().await?.into())
    }

    /// Submit blobs to the celestia network.
    ///
    /// # Example
    /// ```js
    /// const ns = Namespace.newV0(new Uint8Array([97, 98, 99]));
    /// const data = new Uint8Array([100, 97, 116, 97]);
    /// const blob = new Blob(ns, data, AppVersion.latest());
    ///
    /// const txInfo = await txClient.submitBlobs([blob]);
    /// await txClient.submitBlobs([blob], { gasLimit: 100000n, gasPrice: 0.02, memo: "foo" });
    /// ```
    ///
    /// # Note
    ///
    /// Provided blobs will be consumed by this method, meaning
    /// they will no longer be accessible. If this behavior is not desired,
    /// consider using `Blob.clone()`.
    ///
    /// ```js
    /// const blobs = [blob1, blob2, blob3];
    /// await txClient.submitBlobs(blobs.map(b => b.clone()));
    /// ```
    #[wasm_bindgen(js_name = submitBlobs)]
    pub async fn submit_blobs(
        &self,
        blobs: Vec<Blob>,
        tx_config: Option<JsTxConfig>,
    ) -> Result<JsTxInfo> {
        let tx_config = tx_config.map(Into::into).unwrap_or_default();
        let tx = self.client.submit_blobs(&blobs, tx_config).await?;
        Ok(tx.into())
    }

    /// Submit message to the celestia network.
    ///
    /// # Example
    /// ```js
    /// import { Registry } from "@cosmjs/proto-signing";
    ///
    /// const registry = new Registry();
    /// const sendMsg = {
    ///   typeUrl: "/cosmos.bank.v1beta1.MsgSend",
    ///   value: {
    ///     fromAddress: "celestia169s50psyj2f4la9a2235329xz7rk6c53zhw9mm",
    ///     toAddress: "celestia1t52q7uqgnjfzdh3wx5m5phvma3umrq8k6tq2p9",
    ///     amount: [{ denom: "utia", amount: "10000" }],
    ///   },
    /// };
    /// const sendMsgAny = registry.encodeAsAny(sendMsg);
    ///
    /// const txInfo = await txClient.submitMessage(sendMsgAny);
    /// ```
    #[wasm_bindgen(js_name = submitMessage)]
    pub async fn submit_message(
        &self,
        message: JsAny,
        tx_config: Option<JsTxConfig>,
    ) -> Result<JsTxInfo> {
        let tx_config = tx_config.map(Into::into).unwrap_or_default();
        let tx = self
            .client
            .submit_message(SendJsAny::from(message), tx_config)
            .await?;
        Ok(tx.into())
    }

    /// Broadcast message to the celestia network, and return without confirming.
    ///
    /// # Example
    /// ```js
    /// import { Registry } from "@cosmjs/proto-signing";
    ///
    /// const registry = new Registry();
    /// const sendMsg = {
    ///   typeUrl: "/cosmos.bank.v1beta1.MsgSend",
    ///   value: {
    ///     fromAddress: "celestia169s50psyj2f4la9a2235329xz7rk6c53zhw9mm",
    ///     toAddress: "celestia1t52q7uqgnjfzdh3wx5m5phvma3umrq8k6tq2p9",
    ///     amount: [{ denom: "utia", amount: "10000" }],
    ///   },
    /// };
    /// const sendMsgAny = registry.encodeAsAny(sendMsg);
    ///
    /// const broadcastedTx = await txClient.broadcastMessage(sendMsgAny);
    /// console.log("Tx hash:", broadcastedTx.hash);
    /// const txInfo = await txClient.confirmTx(broadcastedTx);
    /// ```
    #[wasm_bindgen(js_name = broadcastMessage)]
    pub async fn broadcast_message(
        &self,
        message: JsAny,
        tx_config: Option<JsTxConfig>,
    ) -> Result<JsBroadcastedTx> {
        let tx_config = tx_config.map(Into::into).unwrap_or_default();
        let submitted_tx = self
            .client
            .broadcast_message(SendJsAny::from(message), tx_config)
            .await?;
        let broadcasted_tx: BroadcastedTx = submitted_tx.into();
        Ok(broadcasted_tx.into())
    }

    /// Broadcast blobs to the celestia network, and return without confirming.
    ///
    /// # Example
    /// ```js
    /// const ns = Namespace.newV0(new Uint8Array([97, 98, 99]));
    /// const data = new Uint8Array([100, 97, 116, 97]);
    /// const blob = new Blob(ns, data, AppVersion.latest());
    ///
    /// const broadcastedTx = await txClient.broadcastBlobs([blob]);
    /// console.log("Tx hash:", broadcastedTx.hash);
    /// const txInfo = await txClient.confirmTx(broadcastedTx);
    /// ```
    ///
    /// # Note
    ///
    /// Provided blobs will be consumed by this method, meaning
    /// they will no longer be accessible. If this behavior is not desired,
    /// consider using `Blob.clone()`.
    ///
    /// ```js
    /// const blobs = [blob1, blob2, blob3];
    /// await txClient.broadcastBlobs(blobs.map(b => b.clone()));
    /// ```
    #[wasm_bindgen(js_name = broadcastBlobs)]
    pub async fn broadcast_blobs(
        &self,
        blobs: Vec<Blob>,
        tx_config: Option<JsTxConfig>,
    ) -> Result<JsBroadcastedTx> {
        let tx_config = tx_config.map(Into::into).unwrap_or_default();
        let submitted_tx = self.client.broadcast_blobs(&blobs, tx_config).await?;
        let broadcasted_tx: crate::tx::BroadcastedTx = submitted_tx.into();
        Ok(broadcasted_tx.into())
    }

    /// Confirm transaction broadcasted with [`broadcast_blobs`] or [`broadcast_message`].
    ///
    /// # Example
    /// ```js
    /// const ns = Namespace.newV0(new Uint8Array([97, 98, 99]));
    /// const data = new Uint8Array([100, 97, 116, 97]);
    /// const blob = new Blob(ns, data, AppVersion.latest());
    ///
    /// const broadcastedTx = await txClient.broadcastBlobs([blob]);
    /// console.log("Tx hash:", broadcastedTx.hash);
    /// const txInfo = await txClient.confirmTx(broadcastedTx);
    /// console.log("Confirmed at height:", txInfo.height);
    /// ```
    #[wasm_bindgen(js_name = confirmTx)]
    pub async fn confirm_tx(
        &self,
        broadcasted_tx: JsBroadcastedTx,
        tx_config: Option<JsTxConfig>,
    ) -> Result<JsTxInfo> {
        let tx_config = tx_config.map(Into::into).unwrap_or_default();

        let tx_info = self
            .client
            .confirm_broadcasted_tx(broadcasted_tx.try_into()?, tx_config)
            .await?;
        Ok(tx_info.into())
    }
}

impl From<crate::GrpcClient> for GrpcClient {
    fn from(client: crate::GrpcClient) -> Self {
        GrpcClient { client }
    }
}

struct SendJsAny(SendWrapper<JsAny>);

impl From<JsAny> for SendJsAny {
    fn from(value: JsAny) -> Self {
        SendJsAny(SendWrapper::new(value))
    }
}

impl IntoProtobufAny for SendJsAny {
    fn into_any(self) -> Any {
        self.0.take().into_any()
    }
}
