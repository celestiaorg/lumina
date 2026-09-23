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
use jsonrpsee::rpc_params;

use crate::{TxConfig, custom_client_error};

/// State RPC methods.
#[rpc(server, namespace = "state", namespace_separator = ".")]
pub trait State {
    /// Returns the default account address for the node.
    #[method(name = "AccountAddress")]
    async fn state_account_address(&self) -> RpcResult<Address>;

    /// Returns the balance for the node's default account.
    #[method(name = "Balance")]
    async fn state_balance(&self) -> RpcResult<Coin>;

    /// Retrieves the Celestia coin balance for a specific address.
    /// Verifies the returned balance against the corresponding block's AppHash.
    #[method(name = "BalanceForAddress")]
    async fn state_balance_for_address(&self, addr: Address) -> RpcResult<Coin>;

    /// Begins a redelegation from one validator to another.
    #[method(name = "BeginRedelegate")]
    async fn state_begin_redelegate(
        &self,
        src: ValAddress,
        dest: ValAddress,
        amount: u64,
        config: TxConfig,
    ) -> RpcResult<RawTxResponse>;

    /// Cancels an unbonding delegation at a specific height.
    #[method(name = "CancelUnbondingDelegation")]
    async fn state_cancel_unbonding_delegation(
        &self,
        addr: ValAddress,
        amount: u64,
        height: u64,
        config: TxConfig,
    ) -> RpcResult<RawTxResponse>;

    /// Delegates tokens to a validator.
    #[method(name = "Delegate")]
    async fn state_delegate(
        &self,
        addr: ValAddress,
        amount: u64,
        config: TxConfig,
    ) -> RpcResult<RawTxResponse>;

    /// Checks whether the state service is stopped.
    #[method(name = "IsStopped")]
    async fn state_is_stopped(&self) -> RpcResult<bool>;

    /// Queries delegation details for the given validator address.
    #[method(name = "QueryDelegation")]
    async fn state_query_delegation(&self, addr: ValAddress) -> RpcResult<QueryDelegationResponse>;
    //

    /// Queries redelegations between the given validators.
    #[method(name = "QueryRedelegations")]
    async fn state_query_redelegations(
        &self,
        src: ValAddress,
        dest: ValAddress,
    ) -> RpcResult<QueryRedelegationsResponse>;

    /// Queries unbonding delegations for the given validator address.
    #[method(name = "QueryUnbonding")]
    async fn state_query_unbonding(
        &self,
        addr: ValAddress,
    ) -> RpcResult<QueryUnbondingDelegationResponse>;

    /// Submits a pay-for-blob transaction for the provided blobs.
    #[method(name = "SubmitPayForBlob")]
    async fn state_submit_pay_for_blob(
        &self,
        blobs: Vec<RawBlob>,
        config: TxConfig,
    ) -> RpcResult<RawTxResponse>;

    /// Transfers tokens to a destination account.
    #[method(name = "Transfer")]
    async fn state_transfer(
        &self,
        to: AccAddress,
        amount: u64,
        config: TxConfig,
    ) -> RpcResult<RawTxResponse>;

    /// Undelegates tokens from a validator.
    #[method(name = "Undelegate")]
    async fn state_undelegate(
        &self,
        addr: ValAddress,
        amount: u64,
        config: TxConfig,
    ) -> RpcResult<RawTxResponse>;
}

/// Client implementation for the State RPC API.
pub trait StateClient: ClientT {
    /// Returns the default account address for the node.
    fn state_account_address(&self) -> impl Future<Output = Result<Address, Error>> + Send {
        self.request("state.AccountAddress", rpc_params![])
    }

    /// Returns the balance for the node's default account.
    fn state_balance(&self) -> impl Future<Output = Result<Coin, Error>> + Send {
        self.request("state.Balance", rpc_params![])
    }

    /// Retrieves the Celestia coin balance for a specific address.
    /// Verifies the returned balance against the corresponding block's AppHash.
    fn state_balance_for_address(
        &self,
        addr: Address,
    ) -> impl Future<Output = Result<Coin, Error>> + Send {
        self.request("state.BalanceForAddress", rpc_params![addr])
    }

    /// Begins a redelegation from one validator to another.
    fn state_begin_redelegate(
        &self,
        src: ValAddress,
        dest: ValAddress,
        amount: u64,
        config: TxConfig,
    ) -> impl Future<Output = Result<RawTxResponse, Error>> + Send {
        self.request(
            "state.BeginRedelegate",
            rpc_params![src, dest, amount, config],
        )
    }

    /// Cancels an unbonding delegation at a specific height.
    fn state_cancel_unbonding_delegation(
        &self,
        addr: ValAddress,
        amount: u64,
        height: u64,
        config: TxConfig,
    ) -> impl Future<Output = Result<RawTxResponse, Error>> + Send {
        self.request(
            "state.CancelUnbondingDelegation",
            rpc_params![addr, amount, height, config],
        )
    }

    /// Delegates tokens to a validator.
    fn state_delegate(
        &self,
        addr: ValAddress,
        amount: u64,
        config: TxConfig,
    ) -> impl Future<Output = Result<RawTxResponse, Error>> + Send {
        self.request("state.Delegate", rpc_params![addr, amount, config])
    }

    /// Checks whether the state service is stopped.
    fn state_is_stopped(&self) -> impl Future<Output = Result<bool, Error>> + Send {
        self.request("state.IsStopped", rpc_params![])
    }

    /// Queries delegation details for the given validator address.
    fn state_query_delegation(
        &self,
        addr: ValAddress,
    ) -> impl Future<Output = Result<QueryDelegationResponse, Error>> + Send {
        self.request("state.QueryDelegation", rpc_params![addr])
    }

    /// Queries redelegations between the given validators.
    fn state_query_redelegations(
        &self,
        src: ValAddress,
        dest: ValAddress,
    ) -> impl Future<Output = Result<QueryRedelegationsResponse, Error>> + Send {
        self.request("state.QueryRedelegations", rpc_params![src, dest])
    }

    /// Queries unbonding delegations for the given validator address.
    fn state_query_unbonding(
        &self,
        addr: ValAddress,
    ) -> impl Future<Output = Result<QueryUnbondingDelegationResponse, Error>> + Send {
        self.request("state.QueryUnbonding", rpc_params![addr])
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
            Ok(self.request("state.SubmitPayForBlob", rpc_params![blobs, config]))
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
        self.request("state.Transfer", rpc_params![to, amount, config])
    }

    /// Undelegates tokens from a validator.
    fn state_undelegate(
        &self,
        addr: ValAddress,
        amount: u64,
        config: TxConfig,
    ) -> impl Future<Output = Result<RawTxResponse, Error>> + Send {
        self.request("state.Undelegate", rpc_params![addr, amount, config])
    }
}

impl<T: ClientT> StateClient for T {}
