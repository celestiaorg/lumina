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
    /// See [`crate::StateClient::state_account_address`].
    #[method(name = "AccountAddress")]
    async fn state_account_address(&self) -> RpcResult<Address>;

    /// See [`crate::StateClient::state_balance`].
    #[method(name = "Balance")]
    async fn state_balance(&self) -> RpcResult<Coin>;

    /// See [`crate::StateClient::state_balance_for_address`].
    #[method(name = "BalanceForAddress")]
    async fn state_balance_for_address(&self, addr: Address) -> RpcResult<Coin>;
    //

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
    async fn state_query_delegation(&self, addr: ValAddress) -> RpcResult<QueryDelegationResponse>;
    //

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
