use celestia_types::blob::RawBlob;
use celestia_types::state::{
    AccAddress, Address, Coin, QueryDelegationResponse, QueryRedelegationsResponse,
    QueryUnbondingDelegationResponse, RawTxResponse, ValAddress,
};
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;

use crate::TxConfig;

/// State RPC methods.
#[rpc(client, server, namespace = "state", namespace_separator = ".")]
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
