use std::future::Future;

use celestia_types::blob::RawBlob;
use celestia_types::consts::appconsts;
use celestia_types::state::{
    AccAddress, Address, Coin, QueryDelegationResponse, QueryRedelegationsResponse,
    QueryUnbondingDelegationResponse, RawTxResponse, ValAddress,
};
use jsonrpsee::core::RpcResult;
use jsonrpsee::core::client::{ClientT, Error};
use jsonrpsee::proc_macros::rpc;

use crate::{TxConfig, custom_client_error};

mod rpc {
    use super::*;

    /// State RPC methods.
    #[rpc(client, server, namespace = "state", namespace_separator = ".")]
    pub trait State {
        /// See [`crate::StateClient::state_account_address`].
        #[method(name = "AccountAddress")]
        async fn state_account_address(&self) -> RpcResult<Address>;

        /// See [`crate::StateClient::state_balance`].
        #[method(name = "Balance")]
        async fn state_balance(&self) -> RpcResult<Coin>;

        /// See [`crate::StateClient::state_balance_for_address`].
        #[method(name = "BalanceForAddress")]
        async fn state_balance_for_address(&self, addr: Address) -> RpcResult<Coin>;

        /// See [`crate::StateClient::state_begin_redelegate`].
        #[method(name = "BeginRedelegate")]
        async fn state_begin_redelegate(
            &self,
            src: ValAddress,
            dest: ValAddress,
            amount: u64,
            config: TxConfig,
        ) -> RpcResult<RawTxResponse>;

        /// See [`crate::StateClient::state_cancel_unbonding_delegation`].
        #[method(name = "CancelUnbondingDelegation")]
        async fn state_cancel_unbonding_delegation(
            &self,
            addr: ValAddress,
            amount: u64,
            height: u64,
            config: TxConfig,
        ) -> RpcResult<RawTxResponse>;

        /// See [`crate::StateClient::state_delegate`].
        #[method(name = "Delegate")]
        async fn state_delegate(
            &self,
            addr: ValAddress,
            amount: u64,
            config: TxConfig,
        ) -> RpcResult<RawTxResponse>;

        /// See [`crate::StateClient::state_is_stopped`].
        #[method(name = "IsStopped")]
        async fn state_is_stopped(&self) -> RpcResult<bool>;

        /// See [`crate::StateClient::state_query_delegation`].
        #[method(name = "QueryDelegation")]
        async fn state_query_delegation(
            &self,
            addr: ValAddress,
        ) -> RpcResult<QueryDelegationResponse>;

        /// See [`crate::StateClient::state_query_redelegations`].
        #[method(name = "QueryRedelegations")]
        async fn state_query_redelegations(
            &self,
            src: ValAddress,
            dest: ValAddress,
        ) -> RpcResult<QueryRedelegationsResponse>;

        /// See [`crate::StateClient::state_query_unbonding`].
        #[method(name = "QueryUnbonding")]
        async fn state_query_unbonding(
            &self,
            addr: ValAddress,
        ) -> RpcResult<QueryUnbondingDelegationResponse>;

        /// See [`crate::StateClient::state_submit_pay_for_blob`].
        #[method(name = "SubmitPayForBlob")]
        async fn state_submit_pay_for_blob(
            &self,
            blobs: Vec<RawBlob>,
            config: TxConfig,
        ) -> RpcResult<RawTxResponse>;

        /// See [`crate::StateClient::state_transfer`].
        #[method(name = "Transfer")]
        async fn state_transfer(
            &self,
            to: AccAddress,
            amount: u64,
            config: TxConfig,
        ) -> RpcResult<RawTxResponse>;

        /// See [`crate::StateClient::state_undelegate`].
        #[method(name = "Undelegate")]
        async fn state_undelegate(
            &self,
            addr: ValAddress,
            amount: u64,
            config: TxConfig,
        ) -> RpcResult<RawTxResponse>;
    }
}

pub use rpc::StateServer;

/// Client implementation for the State RPC API.
pub trait StateClient: ClientT + Sized {
    /// Returns the default account address for the node.
    fn state_account_address(&self) -> impl Future<Output = Result<Address, Error>> + Send {
        rpc::StateClient::state_account_address(self)
    }

    /// Returns the balance for the node's default account.
    fn state_balance(&self) -> impl Future<Output = Result<Coin, Error>> + Send {
        rpc::StateClient::state_balance(self)
    }

    /// Retrieves the Celestia coin balance for a specific address.
    /// Verifies the returned balance against the corresponding block's AppHash.
    fn state_balance_for_address(
        &self,
        addr: Address,
    ) -> impl Future<Output = Result<Coin, Error>> + Send {
        rpc::StateClient::state_balance_for_address(self, addr)
    }

    /// Begins a redelegation from one validator to another.
    fn state_begin_redelegate(
        &self,
        src: ValAddress,
        dest: ValAddress,
        amount: u64,
        config: TxConfig,
    ) -> impl Future<Output = Result<RawTxResponse, Error>> + Send {
        rpc::StateClient::state_begin_redelegate(self, src, dest, amount, config)
    }

    /// Cancels an unbonding delegation at a specific height.
    fn state_cancel_unbonding_delegation(
        &self,
        addr: ValAddress,
        amount: u64,
        height: u64,
        config: TxConfig,
    ) -> impl Future<Output = Result<RawTxResponse, Error>> + Send {
        rpc::StateClient::state_cancel_unbonding_delegation(self, addr, amount, height, config)
    }

    /// Delegates tokens to a validator.
    fn state_delegate(
        &self,
        addr: ValAddress,
        amount: u64,
        config: TxConfig,
    ) -> impl Future<Output = Result<RawTxResponse, Error>> + Send {
        rpc::StateClient::state_delegate(self, addr, amount, config)
    }

    /// Checks whether the state service is stopped.
    fn state_is_stopped(&self) -> impl Future<Output = Result<bool, Error>> + Send {
        rpc::StateClient::state_is_stopped(self)
    }

    /// Queries delegation details for the given validator address.
    fn state_query_delegation(
        &self,
        addr: ValAddress,
    ) -> impl Future<Output = Result<QueryDelegationResponse, Error>> + Send {
        rpc::StateClient::state_query_delegation(self, addr)
    }

    /// Queries redelegations between the given validators.
    fn state_query_redelegations(
        &self,
        src: ValAddress,
        dest: ValAddress,
    ) -> impl Future<Output = Result<QueryRedelegationsResponse, Error>> + Send {
        rpc::StateClient::state_query_redelegations(self, src, dest)
    }

    /// Queries unbonding delegations for the given validator address.
    fn state_query_unbonding(
        &self,
        addr: ValAddress,
    ) -> impl Future<Output = Result<QueryUnbondingDelegationResponse, Error>> + Send {
        rpc::StateClient::state_query_unbonding(self, addr)
    }

    /// Submits a pay-for-blob transaction for the provided blobs.
    /// Share version 2 is reserved for Fibre system blobs and is rejected locally.
    fn state_submit_pay_for_blob(
        &self,
        blobs: Vec<RawBlob>,
        config: TxConfig,
    ) -> impl Future<Output = Result<RawTxResponse, Error>> + Send {
        let request = if blobs
            .iter()
            .any(|blob| blob.share_version == u32::from(appconsts::SHARE_VERSION_TWO))
        {
            Err(custom_client_error(
                celestia_types::Error::FibreBlobSubmission,
            ))
        } else {
            Ok(rpc::StateClient::state_submit_pay_for_blob(
                self, blobs, config,
            ))
        };
        async move { request?.await }
    }

    /// Transfers tokens to a destination account.
    fn state_transfer(
        &self,
        to: AccAddress,
        amount: u64,
        config: TxConfig,
    ) -> impl Future<Output = Result<RawTxResponse, Error>> + Send {
        rpc::StateClient::state_transfer(self, to, amount, config)
    }

    /// Undelegates tokens from a validator.
    fn state_undelegate(
        &self,
        addr: ValAddress,
        amount: u64,
        config: TxConfig,
    ) -> impl Future<Output = Result<RawTxResponse, Error>> + Send {
        rpc::StateClient::state_undelegate(self, addr, amount, config)
    }
}

impl<T: ClientT> StateClient for T {}
