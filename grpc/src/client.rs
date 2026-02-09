use std::fmt;
use std::sync::Arc;

use ::tendermint::chain::Id;
use arc_swap::ArcSwap;
use celestia_types::any::IntoProtobufAny;
use k256::ecdsa::VerifyingKey;
use lumina_utils::time::Interval;
use prost::Message;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard, OnceCell};

use celestia_grpc_macros::grpc_method;
use celestia_proto::celestia::blob::v1::query_client::QueryClient as BlobQueryClient;
use celestia_proto::celestia::core::v1::gas_estimation::gas_estimator_client::GasEstimatorClient;
use celestia_proto::celestia::core::v1::tx::tx_client::TxClient as TxStatusClient;
use celestia_proto::cosmos::auth::v1beta1::query_client::QueryClient as AuthQueryClient;
use celestia_proto::cosmos::bank::v1beta1::query_client::QueryClient as BankQueryClient;
use celestia_proto::cosmos::base::node::v1beta1::service_client::ServiceClient as ConfigServiceClient;
use celestia_proto::cosmos::base::tendermint::v1beta1::service_client::ServiceClient as TendermintServiceClient;
use celestia_proto::cosmos::staking::v1beta1::query_client::QueryClient as StakingQueryClient;
use celestia_proto::cosmos::tx::v1beta1::service_client::ServiceClient as TxServiceClient;
use celestia_types::blob::{BlobParams, MsgPayForBlobs, RawBlobTx, RawMsgPayForBlobs};
use celestia_types::block::Block;
use celestia_types::consts::appconsts;
use celestia_types::hash::Hash;
use celestia_types::state::auth::{Account, AuthParams, BaseAccount};
use celestia_types::state::{
    AbciQueryResponse, PageRequest, QueryDelegationResponse, QueryRedelegationsResponse,
    QueryUnbondingDelegationResponse, RawTxBody, ValAddress,
};
use celestia_types::state::{
    AccAddress, Address, AddressTrait, BOND_DENOM, Coin, ErrorCode, TxResponse,
};
use celestia_types::{AppVersion, Blob, ExtendedHeader};

use crate::abci_proofs::ProofChain;
use crate::boxed::BoxedTransport;
use crate::builder::GrpcClientBuilder;
use crate::grpc::{
    AsyncGrpcCall, BroadcastMode, ConfigResponse, Context, GasEstimate, GasInfo, GetTxResponse,
    TxPriority, TxStatus, TxStatusBatchResponse, TxStatusResponse,
};
use crate::signer::{BoxedDocSigner, sign_tx};
use crate::tx::{BroadcastedTx, SubmittedTx, TxInfo};
use crate::{Error, Result, TxConfig};

// source https://github.com/celestiaorg/celestia-core/blob/v1.43.0-tm-v0.34.35/pkg/consts/consts.go#L19
const BLOB_TX_TYPE_ID: &str = "BLOB";
/// Message returned on errors related to sequence
const SEQUENCE_ERROR_PAT: &str = "account sequence mismatch, expected ";

#[derive(Debug, Clone)]
struct ChainState {
    app_version: AppVersion,
    chain_id: Id,
}

#[derive(Debug)]
pub(crate) struct AccountState {
    base: OnceCell<Mutex<BaseAccount>>,
    pubkey: VerifyingKey,
    signer: BoxedDocSigner,
}

#[derive(Debug)]
struct AccountGuard<'a> {
    base: MutexGuard<'a, BaseAccount>,
    pubkey: &'a VerifyingKey,
    signer: &'a BoxedDocSigner,
}

impl AccountState {
    pub fn new(pubkey: VerifyingKey, signer: BoxedDocSigner) -> Self {
        Self {
            base: OnceCell::new(),
            pubkey,
            signer,
        }
    }
}

/// gRPC client for the Celestia network
///
/// Under the hood, this struct wraps tonic and does type conversion
#[derive(Clone)]
pub struct GrpcClient {
    inner: Arc<GrpcClientInner>,
}

struct GrpcClientInner {
    transports: Arc<ArcSwap<Vec<BoxedTransport>>>,
    account: Option<AccountState>,
    chain_state: OnceCell<ChainState>,
}

impl GrpcClient {
    /// Create a new client wrapping given transports
    pub(crate) fn new(
        transports: Arc<ArcSwap<Vec<BoxedTransport>>>,
        account: Option<AccountState>,
    ) -> Self {
        Self {
            inner: Arc::new(GrpcClientInner {
                transports,
                account,
                chain_state: OnceCell::new(),
            }),
        }
    }

    /// Create a builder for [`GrpcClient`] connected to `url`
    pub fn builder() -> GrpcClientBuilder {
        GrpcClientBuilder::new()
    }

    // cosmos.auth

    /// Get auth params
    #[grpc_method(AuthQueryClient::params)]
    fn get_auth_params(&self) -> AsyncGrpcCall<AuthParams>;

    /// Get account
    #[grpc_method(AuthQueryClient::account)]
    fn get_account(&self, account: &AccAddress) -> AsyncGrpcCall<Account>;

    /// Get accounts
    #[grpc_method(AuthQueryClient::accounts)]
    fn get_accounts(&self) -> AsyncGrpcCall<Vec<Account>>;

    // cosmos.bank

    /// Retrieves the verified Celestia coin balance for the address.
    ///
    /// # Notes
    ///
    /// This returns the verified balance which is the one that was reported by
    /// the previous network block. In other words, if you transfer some coins,
    /// you need to wait 1 more block in order to see the new balance. If you want
    /// something more immediate then use [`GrpcClient::get_balance`].
    pub fn get_verified_balance(
        &self,
        address: &Address,
        header: &ExtendedHeader,
    ) -> AsyncGrpcCall<Coin> {
        let this = self.clone();
        let address = *address;
        let header = header.clone();

        AsyncGrpcCall::new(move |context| async move {
            this.get_verified_balance_impl(&address, &header, &context)
                .await
        })
    }

    /// Retrieves the Celestia coin balance for the given address.
    #[grpc_method(BankQueryClient::balance)]
    fn get_balance(&self, address: &Address, denom: impl Into<String>) -> AsyncGrpcCall<Coin>;

    /// Get balance of all coins
    #[grpc_method(BankQueryClient::all_balances)]
    fn get_all_balances(&self, address: &Address) -> AsyncGrpcCall<Vec<Coin>>;

    /// Get balance of all spendable coins
    #[grpc_method(BankQueryClient::spendable_balances)]
    fn get_spendable_balances(&self, address: &Address) -> AsyncGrpcCall<Vec<Coin>>;

    /// Get total supply
    #[grpc_method(BankQueryClient::total_supply)]
    fn get_total_supply(&self) -> AsyncGrpcCall<Vec<Coin>>;

    // cosmos.base.node

    /// Get node configuration
    #[grpc_method(ConfigServiceClient::config)]
    fn get_node_config(&self) -> AsyncGrpcCall<ConfigResponse>;

    // cosmos.base.tendermint

    /// Get latest block
    #[grpc_method(TendermintServiceClient::get_latest_block)]
    fn get_latest_block(&self) -> AsyncGrpcCall<Block>;

    /// Get block by height
    #[grpc_method(TendermintServiceClient::get_block_by_height)]
    fn get_block_by_height(&self, height: i64) -> AsyncGrpcCall<Block>;

    /// Issue a direct ABCI query to the application
    #[grpc_method(TendermintServiceClient::abci_query)]
    fn abci_query(
        &self,
        data: impl AsRef<[u8]>,
        path: impl Into<String>,
        height: u64,
        prove: bool,
    ) -> AsyncGrpcCall<AbciQueryResponse>;

    // cosmos.tx

    /// Broadcast prepared and serialised transaction
    #[grpc_method(TxServiceClient::broadcast_tx)]
    fn broadcast_tx(&self, tx_bytes: Vec<u8>, mode: BroadcastMode) -> AsyncGrpcCall<TxResponse>;

    /// Get Tx
    #[grpc_method(TxServiceClient::get_tx)]
    fn get_tx(&self, hash: Hash) -> AsyncGrpcCall<GetTxResponse>;

    /// Broadcast prepared and serialised transaction
    #[grpc_method(TxServiceClient::simulate)]
    fn simulate(&self, tx_bytes: Vec<u8>) -> AsyncGrpcCall<GasInfo>;

    // cosmos.staking

    /// Retrieves the delegation information between a delegator and a validator
    // TODO: Expose this to JS and  UniFFI
    #[grpc_method(StakingQueryClient::delegation)]
    fn query_delegation(
        &self,
        delegator_address: &AccAddress,
        validator_address: &ValAddress,
    ) -> AsyncGrpcCall<QueryDelegationResponse>;

    /// Retrieves the unbonding status between a delegator and a validator
    // TODO: Expose this to JS and  UniFFI
    #[grpc_method(StakingQueryClient::unbonding_delegation)]
    fn query_unbonding(
        &self,
        delegator_address: &AccAddress,
        validator_address: &ValAddress,
    ) -> AsyncGrpcCall<QueryUnbondingDelegationResponse>;

    /// Retrieves the status of the redelegations between a delegator and a validator
    // TODO: Expose this to JS and  UniFFI
    #[grpc_method(StakingQueryClient::redelegations)]
    fn query_redelegations(
        &self,
        delegator_address: &AccAddress,
        src_validator_address: &ValAddress,
        dest_validator_address: &ValAddress,
        pagination: Option<PageRequest>,
    ) -> AsyncGrpcCall<QueryRedelegationsResponse>;

    // celestia.blob

    /// Get blob params
    #[grpc_method(BlobQueryClient::params)]
    fn get_blob_params(&self) -> AsyncGrpcCall<BlobParams>;

    // celestia.core.tx

    /// Get status of the transaction
    #[grpc_method(TxStatusClient::tx_status)]
    fn tx_status(&self, hash: Hash) -> AsyncGrpcCall<TxStatusResponse>;

    /// Get status of transactions in batch.
    #[grpc_method(TxStatusClient::tx_status_batch)]
    fn tx_status_batch(&self, hashes: Vec<Hash>) -> AsyncGrpcCall<TxStatusBatchResponse>;

    // celestia.core.gas_estimation

    /// Estimate gas price for given transaction priority based
    /// on the gas prices of the transactions in the last five blocks.
    ///
    /// If no transaction is found in the last five blocks, return the network
    /// min gas price.
    #[grpc_method(GasEstimatorClient::estimate_gas_price)]
    fn estimate_gas_price(&self, priority: TxPriority) -> AsyncGrpcCall<f64>;

    /// Estimate gas price for transaction with given priority and estimate gas usage
    /// for the provided serialised transaction.
    ///
    /// The gas price estimation is based on the gas prices of the transactions
    /// in the last five blocks.
    /// If no transaction is found in the last five blocks, return the network
    /// min gas price.
    ///
    /// The gas used is estimated using the state machine simulation.
    #[grpc_method(GasEstimatorClient::estimate_gas_price_and_usage)]
    fn estimate_gas_price_and_usage(
        &self,
        priority: TxPriority,
        tx_bytes: Vec<u8>,
    ) -> AsyncGrpcCall<GasEstimate>;

    /// Submit given message to celestia network.
    ///
    /// # Example
    /// ```no_run
    /// # async fn docs() {
    /// use celestia_grpc::{GrpcClient, TxConfig};
    /// use celestia_proto::cosmos::bank::v1beta1::MsgSend;
    /// use celestia_types::state::{Address, Coin};
    /// use tendermint::crypto::default::ecdsa_secp256k1::SigningKey;
    ///
    /// let signing_key = SigningKey::random(&mut rand::rngs::OsRng);
    /// let address = Address::from_account_verifying_key(*signing_key.verifying_key());
    /// let grpc_url = "public-celestia-mocha4-consensus.numia.xyz:9090";
    ///
    /// let tx_client = GrpcClient::builder()
    ///     .url(grpc_url)
    ///     .signer_keypair(signing_key)
    ///     .build()
    ///     .unwrap();
    ///
    /// let msg = MsgSend {
    ///     from_address: address.to_string(),
    ///     to_address: "celestia169s50psyj2f4la9a2235329xz7rk6c53zhw9mm".to_string(),
    ///     amount: vec![Coin::utia(12345).into()],
    /// };
    ///
    /// tx_client
    ///     .submit_message(msg.clone(), TxConfig::default())
    ///     .await
    ///     .unwrap();
    /// # }
    /// ```
    pub fn submit_message<M>(&self, message: M, cfg: TxConfig) -> AsyncGrpcCall<TxInfo>
    where
        M: IntoProtobufAny + Send + 'static,
    {
        let this = self.clone();

        AsyncGrpcCall::new(move |context| async move {
            let tx = this
                .submit_message_impl(message, cfg.clone(), &context)
                .await?;
            this.confirm_tx(tx, cfg, &context).await
        })
    }

    /// Submit given message to celestia network, and return without confirming.
    ///
    /// # Example
    /// ```no_run
    /// # async fn docs() {
    /// use celestia_grpc::{GrpcClient, TxConfig};
    /// use celestia_proto::cosmos::bank::v1beta1::MsgSend;
    /// use celestia_types::state::{Address, Coin};
    /// use tendermint::crypto::default::ecdsa_secp256k1::SigningKey;
    ///
    /// let signing_key = SigningKey::random(&mut rand::rngs::OsRng);
    /// let address = Address::from_account_verifying_key(*signing_key.verifying_key());
    /// let grpc_url = "public-celestia-mocha4-consensus.numia.xyz:9090";
    ///
    /// let tx_client = GrpcClient::builder()
    ///     .url(grpc_url)
    ///     .signer_keypair(signing_key)
    ///     .build()
    ///     .unwrap();
    ///
    /// let msg = MsgSend {
    ///     from_address: address.to_string(),
    ///     to_address: "celestia169s50psyj2f4la9a2235329xz7rk6c53zhw9mm".to_string(),
    ///     amount: vec![Coin::utia(12345).into()],
    /// };
    ///
    /// let broadcasted_tx = tx_client
    ///     .broadcast_message(msg.clone(), TxConfig::default()).await.unwrap();
    /// println!("Tx: {:?}", broadcasted_tx.tx_ref());
    /// broadcasted_tx.confirm().await.unwrap();
    /// # }
    /// ```
    pub fn broadcast_message<M>(&self, message: M, cfg: TxConfig) -> AsyncGrpcCall<SubmittedTx>
    where
        M: IntoProtobufAny + Send + 'static,
    {
        let this = self.clone();

        AsyncGrpcCall::new(move |context| async move {
            let broadcasted_tx = this
                .submit_message_impl(message, cfg.clone(), &context)
                .await?;

            Ok(SubmittedTx::new(
                broadcasted_tx.clone(),
                AsyncGrpcCall::new(move |context| async move {
                    this.confirm_tx(broadcasted_tx, cfg, &context).await
                })
                .context(&context),
            ))
        })
    }

    /// Submit given blobs to celestia network.
    ///
    /// # Example
    /// ```no_run
    /// # async fn docs() {
    /// use celestia_grpc::{GrpcClient, TxConfig};
    /// use celestia_types::state::{Address, Coin};
    /// use celestia_types::{AppVersion, Blob};
    /// use celestia_types::nmt::Namespace;
    /// use tendermint::crypto::default::ecdsa_secp256k1::SigningKey;
    ///
    /// let signing_key = SigningKey::random(&mut rand::rngs::OsRng);
    /// let address = Address::from_account_verifying_key(*signing_key.verifying_key());
    /// let grpc_url = "public-celestia-mocha4-consensus.numia.xyz:9090";
    ///
    /// let tx_client = GrpcClient::builder()
    ///     .url(grpc_url)
    ///     .signer_keypair(signing_key)
    ///     .build()
    ///     .unwrap();
    ///
    /// let ns = Namespace::new_v0(b"abcd").unwrap();
    /// let blob = Blob::new(ns, "some data".into(), None, AppVersion::latest()).unwrap();
    ///
    /// tx_client
    ///     .submit_blobs(&[blob], TxConfig::default())
    ///     .await
    ///     .unwrap();
    /// # }
    /// ```
    pub fn submit_blobs(&self, blobs: &[Blob], cfg: TxConfig) -> AsyncGrpcCall<TxInfo> {
        let this = self.clone();
        let blobs = blobs.to_vec();

        AsyncGrpcCall::new(move |context| async move {
            let tx = this
                .submit_blobs_impl(&blobs, cfg.clone(), &context)
                .await?;
            this.confirm_tx(tx, cfg, &context).await
        })
    }

    /// Submit given blobs to celestia network, and return without confirming.
    ///
    /// # Example
    /// ```no_run
    /// # async fn docs() {
    /// use celestia_grpc::{GrpcClient, TxConfig};
    /// use celestia_types::state::{Address, Coin};
    /// use celestia_types::{AppVersion, Blob};
    /// use celestia_types::nmt::Namespace;
    /// use tendermint::crypto::default::ecdsa_secp256k1::SigningKey;
    ///
    /// let signing_key = SigningKey::random(&mut rand::rngs::OsRng);
    /// let address = Address::from_account_verifying_key(*signing_key.verifying_key());
    /// let grpc_url = "public-celestia-mocha4-consensus.numia.xyz:9090";
    ///
    /// let tx_client = GrpcClient::builder()
    ///     .url(grpc_url)
    ///     .signer_keypair(signing_key)
    ///     .build()
    ///     .unwrap();
    ///
    /// let ns = Namespace::new_v0(b"abcd").unwrap();
    /// let blob = Blob::new(ns, "some data".into(), None, AppVersion::V3).unwrap();
    ///
    /// let broadcasted_tx = tx_client
    ///     .broadcast_blobs(&[blob], TxConfig::default()).await.unwrap();
    /// println!("Tx: {:?}", broadcasted_tx.tx_ref());
    /// broadcasted_tx.confirm().await.unwrap();
    /// # }
    /// ```
    pub fn broadcast_blobs(&self, blobs: &[Blob], cfg: TxConfig) -> AsyncGrpcCall<SubmittedTx> {
        let this = self.clone();
        let blobs = blobs.to_vec();

        AsyncGrpcCall::new(move |context| async move {
            let broadcasted_tx = this
                .clone()
                .submit_blobs_impl(&blobs, cfg.clone(), &context)
                .await?;

            Ok(SubmittedTx::new(
                broadcasted_tx.clone(),
                AsyncGrpcCall::new(move |context| async move {
                    this.confirm_tx(broadcasted_tx, cfg, &context).await
                })
                .context(&context),
            ))
        })
    }

    /// Get client's app version
    pub fn app_version(&self) -> AsyncGrpcCall<AppVersion> {
        let this = self.clone();

        AsyncGrpcCall::new(move |context| async move {
            let ChainState { app_version, .. } = this.load_chain_state(&context).await?;
            Ok(*app_version)
        })
    }

    /// Get client's chain id
    pub fn chain_id(&self) -> AsyncGrpcCall<Id> {
        let this = self.clone();

        AsyncGrpcCall::new(move |context| async move {
            let ChainState { chain_id, .. } = this.load_chain_state(&context).await?;
            Ok(chain_id.clone())
        })
    }

    /// Manually confirm transaction broadcasted with [`GrpcClient::broadcast_blobs`] or [`GrpcClient::broadcast_message`].
    // this method is unused for native builds and exists for uniffi/wasm32 api only. Some targets
    // we build in CI actually compile out that api, so we also allow those cases.
    #[cfg_attr(
        any(
            all(not(target_arch = "wasm32"), not(feature = "uniffi")), // native build
            all(not(feature = "wasm-bindgen"), target_arch = "wasm32"),
            all(feature = "uniffi", target_arch = "wasm32")
        ),
        allow(dead_code)
    )]
    pub(crate) fn confirm_broadcasted_tx(
        &self,
        tx: BroadcastedTx,
        cfg: TxConfig,
    ) -> AsyncGrpcCall<TxInfo> {
        let this = self.clone();

        AsyncGrpcCall::new(move |context| async move { this.confirm_tx(tx, cfg, &context).await })
    }

    /// Get client's account public key if the signer is set
    pub fn get_account_pubkey(&self) -> Option<VerifyingKey> {
        self.account().map(|account| account.pubkey).ok()
    }

    /// Get client's account address if the signer is set
    pub fn get_account_address(&self) -> Option<AccAddress> {
        self.get_account_pubkey().map(Into::into)
    }
}

impl GrpcClient {
    async fn get_verified_balance_impl(
        &self,
        address: &Address,
        header: &ExtendedHeader,
        context: &Context,
    ) -> Result<Coin> {
        // construct the key for querying account balance from bank state
        let mut prefixed_account_key = Vec::with_capacity(1 + 1 + appconsts::SIGNER_SIZE + 4);
        prefixed_account_key.push(0x02); // balances prefix
        prefixed_account_key.push(address.as_bytes().len() as u8); // address length
        prefixed_account_key.extend_from_slice(address.as_bytes()); // address
        prefixed_account_key.extend_from_slice(BOND_DENOM.as_bytes()); // denom

        // abci queries are possible only from 2nd block, but we can rely on the node to
        // return an error if that is the case. We only want to prevent height == 0 here,
        // because that will make node interpret that as a network head. It would also fail,
        // on proof verification, but it would be harder to debug as the message would just
        // say that computed root is different than expected.
        let height = 1.max(header.height().saturating_sub(1));

        let response = self
            .abci_query(&prefixed_account_key, "store/bank/key", height, true)
            .context(context)
            .await?;
        if response.code != ErrorCode::Success {
            return Err(Error::AbciQuery(response.code, response.log));
        }

        // If account doesn't exist yet, just return 0
        if response.value.is_empty() {
            return Ok(Coin::utia(0));
        }

        // NOTE: don't put `ProofChain` directly in the AbciQueryResponse, because
        // it supports only small subset of proofs that are required for the balance
        // queries
        let proof: ProofChain = response.proof_ops.unwrap_or_default().try_into()?;
        proof.verify_membership(
            &header.header.app_hash,
            [prefixed_account_key.as_slice(), b"bank"],
            &response.value,
        )?;

        let amount = std::str::from_utf8(&response.value)
            .map_err(|_| Error::FailedToParseResponse)?
            .parse()
            .map_err(|_| Error::FailedToParseResponse)?;

        Ok(Coin::utia(amount))
    }

    async fn submit_message_impl<M>(
        &self,
        message: M,
        cfg: TxConfig,
        context: &Context,
    ) -> Result<BroadcastedTx>
    where
        M: IntoProtobufAny,
    {
        let tx_body = RawTxBody {
            messages: vec![message.into_any()],
            memo: cfg.memo.clone().unwrap_or_default(),
            ..RawTxBody::default()
        };

        self.sign_and_broadcast_tx(tx_body, cfg.clone(), context)
            .await
    }

    async fn submit_blobs_impl(
        &self,
        blobs: &[Blob],
        cfg: TxConfig,
        context: &Context,
    ) -> Result<BroadcastedTx> {
        if blobs.is_empty() {
            return Err(Error::TxEmptyBlobList);
        }
        let app_version = self.app_version().await?;
        for blob in blobs {
            blob.validate(app_version)?;
        }

        self.sign_and_broadcast_blobs(blobs.to_vec(), cfg.clone(), context)
            .await
    }

    async fn load_chain_state(&self, context: &Context) -> Result<&ChainState> {
        self.inner
            .chain_state
            .get_or_try_init(|| async {
                let block = self.get_latest_block().context(context).await?;
                let app_version = block.header.version.app;
                let app_version = AppVersion::from_u64(app_version)
                    .ok_or(celestia_types::Error::UnsupportedAppVersion(app_version))?;
                let chain_id = block.header.chain_id;

                Ok::<_, Error>(ChainState {
                    app_version,
                    chain_id,
                })
            })
            .await
    }

    fn account(&self) -> Result<&AccountState> {
        self.inner.account.as_ref().ok_or(Error::MissingSigner)
    }

    pub(crate) fn signer(&self) -> Result<(VerifyingKey, BoxedDocSigner)> {
        let account = self.account()?;
        Ok((account.pubkey, account.signer.clone()))
    }

    async fn lock_account(&self, context: &Context) -> Result<AccountGuard<'_>> {
        let account = self.account()?;

        let base = account
            .base
            .get_or_try_init(|| async {
                let address = AccAddress::from(account.pubkey);
                let account = self.get_account(&address).context(context).await?;
                Ok::<_, Error>(Mutex::new(account.into()))
            })
            .await?
            .lock()
            .await;

        Ok(AccountGuard {
            base,
            pubkey: &account.pubkey,
            signer: &account.signer,
        })
    }

    /// compute gas limit and gas price according to provided TxConfig for serialised
    /// transaction, potentially calling gas estimation service
    async fn calculate_transaction_gas_params(
        &self,
        tx_body: &RawTxBody,
        cfg: &TxConfig,
        chain_id: Id,
        account: &mut AccountGuard<'_>,
        context: &Context,
    ) -> Result<(u64, f64)> {
        if let Some(gas_limit) = cfg.gas_limit {
            let gas_price = if let Some(gas_price) = cfg.gas_price {
                gas_price
            } else {
                self.estimate_gas_price(cfg.priority)
                    .context(context)
                    .await?
            };

            return Ok((gas_limit, gas_price));
        }

        let tx = sign_tx(
            tx_body.clone(),
            chain_id.clone(),
            &account.base,
            account.pubkey,
            account.signer,
            0,
            1,
        )
        .await?;

        let GasEstimate { price, usage } = self
            .estimate_gas_price_and_usage(cfg.priority, tx.encode_to_vec())
            .context(context)
            .await?;

        Ok((usage, cfg.gas_price.unwrap_or(price)))
    }

    async fn sign_and_broadcast_blobs(
        &self,
        blobs: Vec<Blob>,
        cfg: TxConfig,
        context: &Context,
    ) -> Result<BroadcastedTx> {
        let ChainState { chain_id, .. } = self.load_chain_state(context).await?;
        // lock the account; tx signing and broadcast must be atomic
        // because node requires all transactions to be sequenced by account.sequence
        let mut account = self.lock_account(context).await?;

        let pfb = MsgPayForBlobs::new(&blobs, account.base.address)?;
        let pfb = RawTxBody {
            messages: vec![RawMsgPayForBlobs::from(pfb).into_any()],
            memo: cfg.memo.clone().unwrap_or_default(),
            ..RawTxBody::default()
        };
        let blobs: Vec<_> = blobs.into_iter().map(Into::into).collect();

        // in case parallel submission failed because of the sequence mismatch,
        // we update the account, resign and broadcast transaction
        loop {
            let res = self
                .calculate_transaction_gas_params(
                    &pfb,
                    &cfg,
                    chain_id.clone(),
                    &mut account,
                    context,
                )
                .await;

            // if gas limit is not provided, we simulate the transaction
            // which can also signal that sequence is wrong
            if let Some(new_sequence) = res.as_ref().err().and_then(extract_sequence_on_mismatch) {
                account.base.sequence = new_sequence?;
                continue;
            }

            let (gas_limit, gas_price) = res?;
            let fee = (gas_limit as f64 * gas_price).ceil() as u64;

            let tx = sign_tx(
                pfb.clone(),
                chain_id.clone(),
                &account.base,
                account.pubkey,
                account.signer,
                gas_limit,
                fee,
            )
            .await?;

            let blob_tx = RawBlobTx {
                tx: tx.encode_to_vec(),
                blobs: blobs.clone(),
                type_id: BLOB_TX_TYPE_ID.to_string(),
            }
            .encode_to_vec();

            let res = self
                .broadcast_tx_with_account(blob_tx, &cfg, &mut account, context)
                .await;

            if let Some(new_sequence) = res.as_ref().err().and_then(extract_sequence_on_mismatch) {
                account.base.sequence = new_sequence?;
                continue;
            }

            break res;
        }
    }

    async fn sign_and_broadcast_tx(
        &self,
        tx: RawTxBody,
        cfg: TxConfig,
        context: &Context,
    ) -> Result<BroadcastedTx> {
        let ChainState { chain_id, .. } = self.load_chain_state(context).await?;

        // lock the account; tx signing and broadcast must be atomic
        // because node requires all transactions to be sequenced by account.sequence
        let mut account = self.lock_account(context).await?;

        // in case parallel submission failed because of the sequence mismatch,
        // we update the account, resign and broadcast transaction
        loop {
            let res = self
                .calculate_transaction_gas_params(
                    &tx,
                    &cfg,
                    chain_id.clone(),
                    &mut account,
                    context,
                )
                .await;

            // if gas limit is not provided, we simulate the transaction
            // which can also signal that sequence is wrong
            if let Some(new_sequence) = res.as_ref().err().and_then(extract_sequence_on_mismatch) {
                account.base.sequence = new_sequence?;
                continue;
            }

            let (gas_limit, gas_price) = res?;
            let fee = (gas_limit as f64 * gas_price).ceil() as u64;

            let tx = sign_tx(
                tx.clone(),
                chain_id.clone(),
                &account.base,
                account.pubkey,
                account.signer,
                gas_limit,
                fee,
            )
            .await?;

            let res = self
                .broadcast_tx_with_account(tx.encode_to_vec(), &cfg, &mut account, context)
                .await;

            if let Some(new_sequence) = res.as_ref().err().and_then(extract_sequence_on_mismatch) {
                account.base.sequence = new_sequence?;
                continue;
            }

            break res;
        }
    }

    async fn broadcast_tx_with_account(
        &self,
        tx: Vec<u8>,
        cfg: &TxConfig,
        account: &mut AccountGuard<'_>,
        context: &Context,
    ) -> Result<BroadcastedTx> {
        let sequence = account.base.sequence;

        let res = self.broadcast_tx_with_cfg(tx.clone(), cfg, context).await;

        let hash = match res {
            Ok(resp) => {
                account.base.sequence += 1;
                resp.txhash
            }
            // If tx is already in mempool, we can get a confirmation for it.
            // We presume this did happen because one of the endpoints failed with timeout.
            // But the tx was still added.
            Err(Error::TxBroadcastFailed(hash, ErrorCode::TxInMempoolCache, _)) => {
                account.base.sequence += 1;
                hash
            }
            Err(e) => return Err(e),
        };

        Ok(BroadcastedTx { tx, hash, sequence })
    }

    async fn broadcast_tx_with_cfg(
        &self,
        tx: Vec<u8>,
        cfg: &TxConfig,
        context: &Context,
    ) -> Result<TxResponse> {
        let resp = self
            .broadcast_tx(tx, BroadcastMode::Sync)
            .context(context)
            .await?;

        if resp.code != ErrorCode::Success {
            // if transaction failed due to insufficient fee, include info
            // whether gas price was estimated or explicitely set
            let message = if resp.code == ErrorCode::InsufficientFee {
                if cfg.gas_price.is_some() {
                    format!("Gas price was set via config. {}", resp.raw_log)
                } else {
                    format!("Gas price was estimated. {}", resp.raw_log)
                }
            } else {
                resp.raw_log
            };

            Err(Error::TxBroadcastFailed(resp.txhash, resp.code, message))
        } else {
            Ok(resp)
        }
    }

    async fn confirm_tx(
        &self,
        tx: BroadcastedTx,
        cfg: TxConfig,
        context: &Context,
    ) -> Result<TxInfo> {
        let BroadcastedTx { tx, hash, sequence } = tx;
        let mut interval = Interval::new(Duration::from_millis(cfg.confirmation_interval_ms));

        loop {
            let tx_status = self.tx_status(hash).context(context).await?;
            match tx_status.status {
                TxStatus::Pending => interval.tick().await,
                TxStatus::Committed => {
                    if tx_status.execution_code == ErrorCode::Success {
                        return Ok(TxInfo {
                            hash,
                            height: tx_status.height.value(),
                        });
                    } else {
                        return Err(Error::TxExecutionFailed(
                            hash,
                            tx_status.execution_code,
                            tx_status.error,
                        ));
                    }
                }
                // If some transaction was rejected when creating a block, then it means that its
                // sequence wasn't used. This will cause all the following transactions in the
                // same block to also be rejected due to the sequence mismatch.
                // To recover, we roll back the sequence to the one of rejected transaction, but
                // only if rejection didn't happen due to the sequence mismatch. That way we should
                // always revert the sequence to the one of the first rejected transaction.
                TxStatus::Rejected => {
                    if !is_wrong_sequence(tx_status.execution_code) {
                        let mut acc = self.lock_account(context).await?;
                        acc.base.sequence = sequence;
                    }

                    return Err(Error::TxRejected(
                        hash,
                        tx_status.execution_code,
                        tx_status.error,
                    ));
                }
                // Evicted transaction should be retransmitted without ever trying to re-sign it
                // to prevent double spending, because some nodes may have it in a mempool and some not.
                // If we would re-sign it and both old and new one are confirmed, it'd double spend.
                // https://github.com/celestiaorg/celestia-app/pull/5783#discussion_r2360546232
                TxStatus::Evicted => {
                    if self
                        .broadcast_tx_with_cfg(tx.clone(), &cfg, context)
                        .await
                        .is_err()
                    {
                        // Go version of TxClient starts a one minute timeout before it returns an eviction
                        // error, to avoid user re-submitting the failed transaction right away.
                        //
                        // The reason for it is that Go client supports multi-endpoint, sending tx
                        // to multiple consensus nodes at once, each node having its own mempool.
                        // One of them could evict the tx from the mempool, but the tx may still be
                        // proposed and confirmed by other nodes which didn't evict it.
                        //
                        // In that case, if client would return eviction error right away, the user could
                        // see the error, eagerly resubmit the transaction, and have both old and new
                        // one succeed, resulting in double spending.
                        //
                        // Since we don't support multi-endpoint in our client yet, we can just return
                        // an error right away, as no other consensus node will try to include the transaction.
                        return Err(Error::TxEvicted(hash));
                    }
                }
                // this case should never happen for node that accepted a broadcast
                // however we handle it the same as evicted for extra safety
                TxStatus::Unknown => {
                    if self
                        .broadcast_tx_with_cfg(tx.clone(), &cfg, context)
                        .await
                        .is_err()
                    {
                        return Err(Error::TxNotFound(hash));
                    }
                }
            }
        }
    }
}

impl fmt::Debug for GrpcClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GrpcClient { .. }")
    }
}

fn is_wrong_sequence(code: ErrorCode) -> bool {
    code == ErrorCode::InvalidSequence || code == ErrorCode::WrongSequence
}

/// Returns Some if error is related to the wrong sequence.
/// Inner result is the outcome of the parsing operation.
fn extract_sequence_on_mismatch(err: &Error) -> Option<Result<u64>> {
    let msg = match err {
        Error::TonicError(status) if status.message().contains(SEQUENCE_ERROR_PAT) => {
            status.message()
        }
        Error::TxBroadcastFailed(_, code, message) if is_wrong_sequence(*code) => message.as_str(),
        _ => return None,
    };

    Some(extract_sequence(msg))
}

fn extract_sequence(msg: &str) -> Result<u64> {
    // get the `{expected_sequence}, {rest of the message}` part
    let (_, msg_with_sequence) = msg
        .split_once(SEQUENCE_ERROR_PAT)
        .ok_or_else(|| Error::SequenceParsingFailed(msg.into()))?;

    // drop the comma and rest of the message
    let (sequence, _) = msg_with_sequence
        .split_once(',')
        .ok_or_else(|| Error::SequenceParsingFailed(msg.into()))?;

    sequence
        .parse()
        .map_err(|_| Error::SequenceParsingFailed(msg.into()))
}

#[cfg(test)]
mod tests {
    use std::future::IntoFuture;
    use std::ops::RangeInclusive;
    use std::sync::Arc;
    use std::time::Duration;

    use celestia_proto::cosmos::bank::v1beta1::MsgSend;
    use celestia_rpc::HeaderClient;
    use celestia_types::nmt::Namespace;
    use celestia_types::state::{Coin, ErrorCode};
    use celestia_types::{AppVersion, Blob};
    use futures::FutureExt;
    use lumina_utils::test_utils::async_test;
    use lumina_utils::time::sleep;
    use rand::{Rng, RngCore};
    use tonic::Code;

    use super::GrpcClient;
    use crate::grpc::Context;
    use crate::test_utils::{
        CELESTIA_GRPC_URL, TestAccount, load_account, new_grpc_client, new_rpc_client,
        new_tx_client, spawn,
    };
    use crate::{Error, TxConfig};

    #[async_test]
    async fn per_call_context_works() {
        use crate::Endpoint;

        let client = GrpcClient::builder()
            .endpoint(Endpoint::from("http://foo").metadata("x-token", "secret-token"))
            .build()
            .unwrap();

        // Per-call metadata (.block_height) should be in call.context
        let call = client.app_version().block_height(1234);
        assert!(call.context.metadata.contains_key("x-cosmos-block-height"));
        // The token metadata is not in the call.context, because it is merged at runtime whenever we decide which transport we use
    }

    #[async_test]
    async fn get_auth_params() {
        let client = new_grpc_client();
        let params = client.get_auth_params().await.unwrap();
        assert!(params.max_memo_characters > 0);
        assert!(params.tx_sig_limit > 0);
        assert!(params.tx_size_cost_per_byte > 0);
        assert!(params.sig_verify_cost_ed25519 > 0);
        assert!(params.sig_verify_cost_secp256k1 > 0);
    }

    #[async_test]
    async fn get_account() {
        let client = new_grpc_client();

        let accounts = client.get_accounts().await.unwrap();

        let first_account = accounts.first().expect("account to exist");
        let account = client.get_account(&first_account.address).await.unwrap();

        assert_eq!(&account, first_account);
    }

    #[async_test]
    async fn get_balance() {
        let account = load_account();
        let client = new_grpc_client();

        let coin = client.get_balance(&account.address, "utia").await.unwrap();
        assert_eq!("utia", coin.denom());
        assert!(coin.amount() > 0);

        let all_coins = client.get_all_balances(&account.address).await.unwrap();
        assert!(!all_coins.is_empty());
        assert!(all_coins.iter().map(|c| c.amount()).sum::<u64>() > 0);

        let spendable_coins = client
            .get_spendable_balances(&account.address)
            .await
            .unwrap();
        assert!(!spendable_coins.is_empty());
        assert!(spendable_coins.iter().map(|c| c.amount()).sum::<u64>() > 0);

        let total_supply = client.get_total_supply().await.unwrap();
        assert!(!total_supply.is_empty());
        assert!(total_supply.iter().map(|c| c.amount()).sum::<u64>() > 0);
    }

    #[async_test]
    async fn get_verified_balance() {
        let client = new_grpc_client();
        let account = load_account();

        let jrpc_client = new_rpc_client().await;

        let (head, expected_balance) = tokio::join!(
            jrpc_client.header_network_head().map(Result::unwrap),
            client
                .get_balance(&account.address, "utia")
                .into_future()
                .map(Result::unwrap)
        );

        // trustless balance queries represent state at header.height - 1, so
        // we need to wait for a new head to compare it with the expected balance
        let head = jrpc_client
            .header_wait_for_height(head.height() + 1)
            .await
            .unwrap();

        let verified_balance = client
            .get_verified_balance(&account.address, &head)
            .await
            .unwrap();

        assert_eq!(expected_balance, verified_balance);
    }

    #[async_test]
    async fn get_verified_balance_not_funded_account() {
        let client = new_grpc_client();
        let account = TestAccount::random();

        let jrpc_client = new_rpc_client().await;
        let head = jrpc_client.header_network_head().await.unwrap();

        let verified_balance = client
            .get_verified_balance(&account.address, &head)
            .await
            .unwrap();

        assert_eq!(Coin::utia(0), verified_balance);
    }

    #[async_test]
    async fn get_node_config() {
        let client = new_grpc_client();
        let config = client.get_node_config().await.unwrap();

        // we don't set any explicit value for it in the config
        // so it should be empty since v6
        assert!(config.minimum_gas_price.is_none());
    }

    #[async_test]
    async fn get_block() {
        let client = new_grpc_client();

        let latest_block = client.get_latest_block().await.unwrap();
        let height = latest_block.header.height.value() as i64;

        let block = client.get_block_by_height(height).await.unwrap();
        assert_eq!(block.header, latest_block.header);
    }

    #[async_test]
    async fn get_blob_params() {
        let client = new_grpc_client();
        let params = client.get_blob_params().await.unwrap();
        assert!(params.gas_per_blob_byte > 0);
        assert!(params.gov_max_square_size > 0);
    }

    #[async_test]
    async fn query_state_at_block_height_with_metadata() {
        let (_lock, tx_client) = new_tx_client().await;

        let tx = tx_client
            .submit_blobs(&[random_blob(10..=1000)], TxConfig::default())
            .await
            .unwrap();

        let addr = tx_client.get_account_address().unwrap().into();
        let new_balance = tx_client.get_balance(&addr, "utia").await.unwrap();
        let old_balance = tx_client
            .get_balance(&addr, "utia")
            .block_height(tx.height - 1)
            .await
            .unwrap();

        assert!(new_balance.amount() < old_balance.amount());
    }

    #[async_test]
    async fn submit_and_get_tx() {
        let (_lock, tx_client) = new_tx_client().await;

        let tx = tx_client
            .submit_blobs(
                &[random_blob(10..=1000)],
                TxConfig::default().with_memo("foo"),
            )
            .await
            .unwrap();
        let tx2 = tx_client.get_tx(tx.hash).await.unwrap();

        sleep(Duration::from_millis(100)).await;

        assert_eq!(tx.hash, tx2.tx_response.txhash);
        assert_eq!(tx2.tx.body.memo, "foo");
    }

    #[async_test]
    async fn parallel_submission() {
        let (_lock, tx_client) = new_tx_client().await;
        let tx_client = Arc::new(tx_client);

        let futs = (0..50)
            .map(|_| {
                let tx_client = tx_client.clone();
                spawn(async move {
                    if rand::random() {
                        tx_client
                            .submit_blobs(&[random_blob(10..=10000)], TxConfig::default())
                            .await
                            .unwrap()
                    } else {
                        tx_client
                            .submit_message(random_transfer(&tx_client), TxConfig::default())
                            .await
                            .unwrap()
                    };
                })
            })
            .collect::<Vec<_>>();

        for fut in futs {
            fut.await.unwrap();
        }
    }

    #[async_test]
    async fn updating_sequence_and_resigning() {
        let (_lock, tx_client) = new_tx_client().await;

        // submit blob without providing gas limit, this will fail on gas estimation
        // with a dummy tx with invalid sequence
        invalidate_sequence(&tx_client).await;
        tx_client
            .submit_blobs(&[random_blob(10..=1000)], TxConfig::default())
            .await
            .unwrap();

        // submit blob with provided gas limit, this will fail on broadcasting the tx
        invalidate_sequence(&tx_client).await;
        tx_client
            .submit_blobs(
                &[random_blob(10..=1000)],
                TxConfig::default().with_gas_limit(100000),
            )
            .await
            .unwrap();

        // submit message without providing gas limit, this will fail on gas estimation
        // with a dummy tx with invalid sequence
        invalidate_sequence(&tx_client).await;
        tx_client
            .submit_message(random_transfer(&tx_client), TxConfig::default())
            .await
            .unwrap();

        // submit message with provided gas limit, this will fail on broadcasting the tx
        invalidate_sequence(&tx_client).await;
        tx_client
            .submit_message(
                random_transfer(&tx_client),
                TxConfig::default().with_gas_limit(100000),
            )
            .await
            .unwrap();
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn retransmit_evicted() {
        use tokio::task::JoinSet;

        use crate::grpc::TxStatus;

        const EVICTION_TESTING_VAL_URL: &str = "http://localhost:29090";

        let account = load_account();
        let client = GrpcClient::builder()
            .url(EVICTION_TESTING_VAL_URL)
            .signer_keypair(account.signing_key)
            .build()
            .unwrap();

        let txs = (0..10)
            .map(|_| {
                let client = client.clone();
                async move {
                    let blobs = (0..2).map(|_| random_blob(500000..=500000)).collect();
                    client
                        .sign_and_broadcast_blobs(
                            blobs,
                            // skip simulation part for faster submission
                            TxConfig::default()
                                .with_gas_limit(10000000)
                                .with_gas_price(1e6),
                            &Context::default(),
                        )
                        .await
                        .unwrap()
                }
            })
            .collect::<JoinSet<_>>()
            .join_all()
            .await;

        let successfully_retransmitted = txs
            .into_iter()
            .map(|tx| {
                let client = client.clone();
                async move {
                    // eviction happens if tx is in a mempool for an amount of blocks
                    // higher than the mempool.ttl-num-blocks, so we need to wait a bit
                    // to see correct status.
                    let mut status = TxStatus::Pending;
                    while status == TxStatus::Pending {
                        status = client.tx_status(tx.hash).await.unwrap().status;
                    }

                    let was_evicted = status == TxStatus::Evicted;

                    match client
                        .confirm_tx(tx, TxConfig::default(), &Context::default())
                        .await
                    {
                        // some of the retransmitted tx's will still be evicted because
                        // of sequence mismatch
                        Err(Error::TxEvicted(_)) => false,
                        res => {
                            res.unwrap();
                            was_evicted
                        }
                    }
                }
            })
            .collect::<JoinSet<_>>()
            .join_all()
            .await;

        assert!(
            successfully_retransmitted
                .into_iter()
                .any(std::convert::identity)
        );
    }

    #[async_test]
    async fn submit_blobs_insufficient_gas_price_and_limit() {
        let (_lock, tx_client) = new_tx_client().await;

        let blobs = vec![random_blob(10..=1000)];

        let err = tx_client
            .submit_blobs(&blobs, TxConfig::default().with_gas_limit(10000))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::TxBroadcastFailed(_, ErrorCode::OutOfGas, _)
        ));

        let err = tx_client
            .submit_blobs(&blobs, TxConfig::default().with_gas_price(0.0005))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::TxBroadcastFailed(_, ErrorCode::InsufficientFee, _)
        ));
    }

    #[async_test]
    async fn submit_message() {
        let account = load_account();
        let other_account = TestAccount::random();
        let amount = Coin::utia(12345);
        let (_lock, tx_client) = new_tx_client().await;

        let msg = MsgSend {
            from_address: account.address.to_string(),
            to_address: other_account.address.to_string(),
            amount: vec![amount.clone().into()],
        };

        tx_client
            .submit_message(msg, TxConfig::default())
            .await
            .unwrap();

        let coins = tx_client
            .get_all_balances(&other_account.address)
            .await
            .unwrap();

        assert_eq!(coins.len(), 1);
        assert_eq!(amount, coins[0]);
    }

    #[async_test]
    async fn broadcast_message() {
        let account = load_account();
        let other_account = TestAccount::random();
        let amount = Coin::utia(12345);
        let (_lock, tx_client) = new_tx_client().await;

        let msg = MsgSend {
            from_address: account.address.to_string(),
            to_address: other_account.address.to_string(),
            amount: vec![amount.clone().into()],
        };

        let submitted_tx = tx_client
            .broadcast_message(msg, TxConfig::default())
            .await
            .unwrap();

        submitted_tx.confirm().await.unwrap();

        let coins = tx_client
            .get_all_balances(&other_account.address)
            .await
            .unwrap();

        assert_eq!(coins.len(), 1);
        assert_eq!(amount, coins[0]);
    }

    #[async_test]
    async fn broadcast_blobs() {
        let (_lock, tx_client) = new_tx_client().await;

        let submitted_tx = tx_client
            .broadcast_blobs(
                &[random_blob(10..=1000)],
                TxConfig::default().with_memo("broadcast test"),
            )
            .await
            .unwrap();

        let tx_info = submitted_tx.confirm().await.unwrap();
        let tx = tx_client.get_tx(tx_info.hash).await.unwrap();

        assert_eq!(tx_info.hash, tx.tx_response.txhash);
        assert_eq!(tx.tx.body.memo, "broadcast test");
    }

    #[async_test]
    async fn submit_message_insufficient_gas_price_and_limit() {
        let account = load_account();
        let other_account = TestAccount::random();
        let amount = Coin::utia(12345);
        let (_lock, tx_client) = new_tx_client().await;

        let msg = MsgSend {
            from_address: account.address.to_string(),
            to_address: other_account.address.to_string(),
            amount: vec![amount.clone().into()],
        };

        let err = tx_client
            .submit_message(msg.clone(), TxConfig::default().with_gas_limit(10000))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::TxBroadcastFailed(_, ErrorCode::OutOfGas, _)
        ));

        let err = tx_client
            .submit_message(msg, TxConfig::default().with_gas_price(0.0005))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            Error::TxBroadcastFailed(_, ErrorCode::InsufficientFee, _)
        ));
    }

    #[async_test]
    async fn tx_client_is_send_and_sync() {
        fn is_send_and_sync<T: Send + Sync>(_: &T) {}
        fn is_send<T: Send>(_: &T) {}

        let (_lock, tx_client) = new_tx_client().await;
        is_send_and_sync(&tx_client);

        is_send(&tx_client.submit_blobs(&[], TxConfig::default()));
        is_send(
            &tx_client
                .submit_blobs(&[], TxConfig::default())
                .into_future(),
        );
        is_send(
            &tx_client
                .submit_message(
                    MsgSend {
                        from_address: "".into(),
                        to_address: "".into(),
                        amount: vec![],
                    },
                    TxConfig::default(),
                )
                .into_future(),
        );
    }

    #[async_test]
    async fn timeouts() {
        let account = load_account();
        let client = new_grpc_client();

        // defaults - no timeout
        let _balance = client.get_all_balances(&account.address).await.unwrap();

        // per-request too short timeout
        let Error::TonicError(timeout) = client
            .get_all_balances(&account.address)
            .timeout(Duration::from_nanos(1))
            .await
            .unwrap_err()
        else {
            panic!("invalid error type, expected TonicError");
        };
        assert!(timeout.code() == Code::DeadlineExceeded || timeout.code() == Code::Cancelled);

        // too short timeout set in endpoint config
        let client = GrpcClient::builder()
            .endpoint(crate::Endpoint::from(CELESTIA_GRPC_URL).timeout(Duration::from_nanos(1)))
            .build()
            .unwrap();

        // client-wide too-short timeout should fail
        let Error::TonicError(timeout) =
            client.get_all_balances(&account.address).await.unwrap_err()
        else {
            panic!("invalid error type, expected TonicError");
        };
        assert!(timeout.code() == Code::DeadlineExceeded || timeout.code() == Code::Cancelled);

        // per-request override
        let _balance = client
            .get_all_balances(&account.address)
            .timeout(Duration::from_secs(1))
            .await
            .unwrap();
    }

    fn random_blob(size: RangeInclusive<usize>) -> Blob {
        let rng = &mut rand::thread_rng();

        let mut ns_bytes = vec![0u8; 10];
        rng.fill_bytes(&mut ns_bytes);
        let namespace = Namespace::new_v0(&ns_bytes).unwrap();

        let len = rng.gen_range(size);
        let mut blob = vec![0; len];
        rng.fill_bytes(&mut blob);
        blob.resize(len, 1);

        Blob::new(namespace, blob, None, AppVersion::latest()).unwrap()
    }

    fn random_transfer(client: &GrpcClient) -> MsgSend {
        let address = client.get_account_address().unwrap();
        let other_account = TestAccount::random();
        let amount = rand::thread_rng().gen_range(10..1000);

        MsgSend {
            from_address: address.to_string(),
            to_address: other_account.address.to_string(),
            amount: vec![Coin::utia(amount).into()],
        }
    }

    async fn invalidate_sequence(client: &GrpcClient) {
        client
            .lock_account(&Context::default())
            .await
            .unwrap()
            .base
            .sequence += rand::thread_rng().gen_range(2..200);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn failover_to_second_endpoint() {
        use crate::test_utils::CELESTIA_GRPC_URL;

        // First endpoint is invalid, should failover to second valid one
        let client = GrpcClient::builder()
            .urls(["http://localhost:19999", CELESTIA_GRPC_URL])
            .build()
            .unwrap();

        let params = client.get_auth_params().await.unwrap();
        assert!(params.max_memo_characters > 0);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn endpoint_multiple_requests() {
        use crate::test_utils::CELESTIA_GRPC_URL;

        let client = GrpcClient::builder()
            .urls(["http://localhost:19999", CELESTIA_GRPC_URL])
            .build()
            .unwrap();

        client.get_auth_params().await.unwrap();

        let block = client.get_latest_block().await.unwrap();
        assert!(block.header.height.value() > 0);

        for _ in 0..5 {
            client.get_blob_params().await.unwrap();
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn all_endpoints_fail_returns_error() {
        let client = GrpcClient::builder()
            .urls(["http://localhost:19999", "http://localhost:19998"])
            .build()
            .unwrap();

        let result = client.get_auth_params().await;
        assert!(result.is_err());
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn auth_proxy_with_valid_token() {
        use crate::Endpoint;
        use crate::test_utils::{CELESTIA_GRPC_URL, TEST_AUTH_TOKEN, spawn_grpc_auth_proxy};

        // Spawn the auth proxy
        let (addr, _handle) = spawn_grpc_auth_proxy(CELESTIA_GRPC_URL, TEST_AUTH_TOKEN).await;
        let proxy_url = format!("http://{}", addr);

        // Connect with correct authorization token - should succeed
        let client = GrpcClient::builder()
            .endpoint(
                Endpoint::from(proxy_url.as_str())
                    .metadata("authorization", format!("Bearer {}", TEST_AUTH_TOKEN)),
            )
            .build()
            .unwrap();

        let params = client.get_auth_params().await.unwrap();
        assert!(params.max_memo_characters > 0);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn auth_proxy_without_token() {
        use crate::test_utils::{CELESTIA_GRPC_URL, TEST_AUTH_TOKEN, spawn_grpc_auth_proxy};

        // Spawn the auth proxy
        let (addr, _handle) = spawn_grpc_auth_proxy(CELESTIA_GRPC_URL, TEST_AUTH_TOKEN).await;
        let proxy_url = format!("http://{}", addr);

        // Connect without authorization token - should fail with Unauthenticated
        let client = GrpcClient::builder()
            .url(proxy_url.as_str())
            .build()
            .unwrap();

        let result = client.get_auth_params().await;
        let Error::TonicError(status) = result.unwrap_err() else {
            panic!("expected TonicError");
        };
        // Proxy returns HTTP 401 which maps to Unauthenticated or Unknown depending on tonic version
        assert!(
            status.code() == Code::Unauthenticated || status.code() == Code::Unknown,
            "expected Unauthenticated or Unknown, got {:?}",
            status.code()
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn auth_proxy_with_wrong_token() {
        use crate::Endpoint;
        use crate::test_utils::{CELESTIA_GRPC_URL, TEST_AUTH_TOKEN, spawn_grpc_auth_proxy};

        // Spawn the auth proxy
        let (addr, _handle) = spawn_grpc_auth_proxy(CELESTIA_GRPC_URL, TEST_AUTH_TOKEN).await;
        let proxy_url = format!("http://{}", addr);

        // Connect with wrong authorization token - should fail with Unauthenticated
        let client = GrpcClient::builder()
            .endpoint(
                Endpoint::from(proxy_url.as_str()).metadata("authorization", "Bearer wrong-token"),
            )
            .build()
            .unwrap();

        let result = client.get_auth_params().await;
        let Error::TonicError(status) = result.unwrap_err() else {
            panic!("expected TonicError");
        };
        // Proxy returns HTTP 401 which maps to Unauthenticated or Unknown depending on tonic version
        assert!(
            status.code() == Code::Unauthenticated || status.code() == Code::Unknown,
            "expected Unauthenticated or Unknown, got {:?}",
            status.code()
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn auth_proxy_failover_unreachable_then_authenticated() {
        use crate::Endpoint;
        use crate::test_utils::{CELESTIA_GRPC_URL, TEST_AUTH_TOKEN, spawn_grpc_auth_proxy};

        // Spawn the auth proxy
        let (addr, _handle) = spawn_grpc_auth_proxy(CELESTIA_GRPC_URL, TEST_AUTH_TOKEN).await;
        let proxy_url = format!("http://{}", addr);

        // Two endpoints with different configurations:
        // 1. First endpoint is unreachable (network error triggers failover)
        // 2. Second endpoint is auth proxy with correct token
        // This tests that per-endpoint metadata is correctly applied during failover
        let client = GrpcClient::builder()
            .endpoints([
                Endpoint::from("http://localhost:19999"),
                Endpoint::from(proxy_url.as_str())
                    .metadata("authorization", format!("Bearer {}", TEST_AUTH_TOKEN)),
            ])
            .build()
            .unwrap();

        // Should succeed - first endpoint fails (network error), falls back to second with auth
        let params = client.get_auth_params().await.unwrap();
        assert!(params.max_memo_characters > 0);
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn multiple_endpoints_different_configs() {
        use crate::Endpoint;
        use crate::test_utils::{CELESTIA_GRPC_URL, TEST_AUTH_TOKEN, spawn_grpc_auth_proxy};
        use std::time::Duration;

        // Spawn the auth proxy
        let (addr, _handle) = spawn_grpc_auth_proxy(CELESTIA_GRPC_URL, TEST_AUTH_TOKEN).await;
        let proxy_url = format!("http://{}", addr);

        // Test that different endpoints can have completely different configurations:
        // - First: direct gRPC (no auth needed) with short timeout
        // - Second: auth proxy (needs auth token) with longer timeout
        // Both should be valid configurations
        let client = GrpcClient::builder()
            .endpoints([
                Endpoint::from(CELESTIA_GRPC_URL).timeout(Duration::from_secs(5)),
                Endpoint::from(proxy_url.as_str())
                    .metadata("authorization", format!("Bearer {}", TEST_AUTH_TOKEN))
                    .timeout(Duration::from_secs(10)),
            ])
            .build()
            .unwrap();

        // First endpoint (direct, no auth) should work
        let params = client.get_auth_params().await.unwrap();
        assert!(params.max_memo_characters > 0);
    }
}
