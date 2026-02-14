use celestia_types::blob::RawBlob;
use celestia_types::state::{
    AccAddress, Address, Coin, QueryDelegationResponse, QueryRedelegationsResponse,
    QueryUnbondingDelegationResponse, RawTxResponse, ValAddress,
};
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;

use crate::TxConfig;

#[rpc(client, server, namespace = "state", namespace_separator = ".")]
pub trait State {
    #[method(name = "AccountAddress")]
    async fn state_account_address(&self) -> RpcResult<Address>;

    #[method(name = "Balance")]
    async fn state_balance(&self) -> RpcResult<Coin>;

    #[method(name = "BalanceForAddress")]
    async fn state_balance_for_address(&self, addr: Address) -> RpcResult<Coin>;
    //

    #[method(name = "BeginRedelegate")]
    async fn state_begin_redelegate(
        &self,
        src: ValAddress,
        dest: ValAddress,
        amount: u64,
        config: TxConfig,
    ) -> RpcResult<RawTxResponse>;

    #[method(name = "CancelUnbondingDelegation")]
    async fn state_cancel_unbonding_delegation(
        &self,
        addr: ValAddress,
        amount: u64,
        height: u64,
        config: TxConfig,
    ) -> RpcResult<RawTxResponse>;

    #[method(name = "Delegate")]
    async fn state_delegate(
        &self,
        addr: ValAddress,
        amount: u64,
        config: TxConfig,
    ) -> RpcResult<RawTxResponse>;

    #[method(name = "IsStopped")]
    async fn state_is_stopped(&self) -> RpcResult<bool>;

    #[method(name = "QueryDelegation")]
    async fn state_query_delegation(&self, addr: ValAddress) -> RpcResult<QueryDelegationResponse>;
    //

    #[method(name = "QueryRedelegations")]
    async fn state_query_redelegations(
        &self,
        src: ValAddress,
        dest: ValAddress,
    ) -> RpcResult<QueryRedelegationsResponse>;

    #[method(name = "QueryUnbonding")]
    async fn state_query_unbonding(
        &self,
        addr: ValAddress,
    ) -> RpcResult<QueryUnbondingDelegationResponse>;

    #[method(name = "SubmitPayForBlob")]
    async fn state_submit_pay_for_blob(
        &self,
        blobs: Vec<RawBlob>,
        config: TxConfig,
    ) -> RpcResult<RawTxResponse>;

    #[method(name = "Transfer")]
    async fn state_transfer(
        &self,
        to: AccAddress,
        amount: u64,
        config: TxConfig,
    ) -> RpcResult<RawTxResponse>;

    #[method(name = "Undelegate")]
    async fn state_undelegate(
        &self,
        addr: ValAddress,
        amount: u64,
        config: TxConfig,
    ) -> RpcResult<RawTxResponse>;
}
