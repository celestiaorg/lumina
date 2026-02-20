use std::sync::Arc;

use celestia_rpc::{HeaderClient, StateClient};

use crate::Error;
use crate::client::ClientInner;
use crate::proto::cosmos::bank::v1beta1::MsgSend;
use crate::proto::cosmos::staking::v1beta1::{
    MsgBeginRedelegate, MsgCancelUnbondingDelegation, MsgDelegate, MsgUndelegate,
};
use crate::tx::{GasEstimate, IntoProtobufAny, TxConfig, TxInfo, TxPriority};
use crate::types::Blob;
use crate::types::state::{
    AccAddress, Address, Coin, PageRequest, QueryDelegationResponse, QueryRedelegationsResponse,
    QueryUnbondingDelegationResponse, ValAddress,
};
use crate::utils::height_i64;

/// An async grpc call with [`crate::Error`]
pub type AsyncGrpcCall<Response> = celestia_grpc::grpc::AsyncGrpcCall<Response, crate::Error>;

/// State API for quering and submiting TXs to a consensus node.
pub struct StateApi {
    inner: Arc<ClientInner>,
}

impl StateApi {
    pub(crate) fn new(inner: Arc<ClientInner>) -> StateApi {
        StateApi { inner }
    }

    /// Retrieves the Celestia coin balance for the signer. To query balance without
    /// adding signer to the client, see [`StateApi::balance_for_address`].
    ///
    /// # Notes
    ///
    /// This returns the verified balance which is the one that was reported by
    /// the previous network block. In other words, if you transfer some coins,
    /// you need to wait 1 more block in order to see the new balance. If you want
    /// something more immediate then use [`StateApi::balance_unverified`].
    pub fn balance(&self) -> AsyncGrpcCall<u64> {
        let this = StateApi::new(self.inner.clone());

        AsyncGrpcCall::new(move |context| async move {
            let address = this.inner.address()?;
            this.balance_for_address(&address).context(&context).await
        })
    }

    /// Retrieves the Celestia coin balance for the signer. To query balance without
    /// adding signer to the client, see [`StateApi::balance_for_address_unverified`].
    pub fn balance_unverified(&self) -> AsyncGrpcCall<u64> {
        let this = StateApi::new(self.inner.clone());

        AsyncGrpcCall::new(move |context| async move {
            let address = this.inner.address()?;
            this.balance_for_address_unverified(&address)
                .context(&context)
                .await
        })
    }

    /// Retrieves the Celestia coin balance for the given address.
    ///
    /// # Notes
    ///
    /// This returns the verified balance which is the one that was reported by
    /// the previous network block. In other words, if you transfer some coins,
    /// you need to wait 1 more block in order to see the new balance. If you want
    /// something more immediate then use [`StateApi::balance_for_address_unverified`].
    ///
    /// This is the only method of [`StateApi`] that fallbacks to RPC endpoint
    /// when gRPC endpoint wasn't set.
    pub fn balance_for_address(&self, address: &AccAddress) -> AsyncGrpcCall<u64> {
        let inner = self.inner.clone();
        let address = Address::AccAddress(address.to_owned());

        AsyncGrpcCall::new(move |context| async move {
            let grpc = match inner.grpc() {
                Ok(grpc) => grpc,
                Err(_) => {
                    return Ok(inner.rpc.state_balance_for_address(address).await?.amount());
                }
            };

            let head = inner.rpc.header_network_head().await?;
            head.validate()?;

            Ok(grpc
                .get_verified_balance(&address, &head)
                .context(&context)
                .await?
                .amount())
        })
    }

    /// Retrieves the Celestia coin balance for the given address.
    pub fn balance_for_address_unverified(&self, address: &AccAddress) -> AsyncGrpcCall<u64> {
        let inner = self.inner.clone();
        let address = address.to_owned().into();

        AsyncGrpcCall::new(move |context| async move {
            Ok(inner
                .grpc()?
                .get_balance(&address, "utia")
                .context(&context)
                .await
                .map(|res| res.amount())?)
        })
    }

    /// Estimate gas price for given transaction priority based
    /// on the gas prices of the transactions in the last five blocks.
    ///
    /// If no transaction is found in the last five blocks, it returns the
    /// network min gas price.
    pub fn estimate_gas_price(&self, priority: TxPriority) -> AsyncGrpcCall<f64> {
        let inner = self.inner.clone();

        AsyncGrpcCall::new(move |context| async move {
            Ok(inner
                .grpc()?
                .estimate_gas_price(priority)
                .context(&context)
                .await?)
        })
    }

    /// Estimate gas price for transaction with given priority and estimate gas usage
    /// for provided serialised transaction.
    ///
    /// The gas price estimation is based on the gas prices of the transactions
    /// in the last five blocks. If no transaction is found in the last five blocks,
    /// it returns the network min gas price.
    ///
    /// The gas used is estimated using the state machine simulation.
    pub fn estimate_gas_price_and_usage(
        &self,
        priority: TxPriority,
        tx_bytes: Vec<u8>,
    ) -> AsyncGrpcCall<GasEstimate> {
        let inner = self.inner.clone();

        AsyncGrpcCall::new(move |context| async move {
            Ok(inner
                .grpc()?
                .estimate_gas_price_and_usage(priority, tx_bytes)
                .context(&context)
                .await?)
        })
    }

    /// Submit given message to celestia network.
    ///
    /// # Example
    /// ```no_run
    /// # use celestia_client::{Client, Result};
    /// # use celestia_client::tx::TxConfig;
    /// # async fn docs() -> Result<()> {
    /// use celestia_proto::cosmos::bank::v1beta1::MsgSend;
    /// use celestia_types::state::{Address, Coin};
    ///
    /// let client = Client::builder()
    ///     .rpc_url("ws://localhost:26658")
    ///     .grpc_url("http://localhost:9090")
    ///     .private_key_hex("393fdb5def075819de55756b45c9e2c8531a8c78dd6eede483d3440e9457d839")
    ///     .build()
    ///     .await?;
    ///
    /// let msg = MsgSend {
    ///     from_address: client.address()?.to_string(),
    ///     to_address: "celestia169s50psyj2f4la9a2235329xz7rk6c53zhw9mm".to_string(),
    ///     amount: vec![Coin::utia(12345).into()],
    /// };
    ///
    /// client
    ///     .state()
    ///     .submit_message(msg, TxConfig::default())
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn submit_message<M>(&self, message: M, cfg: TxConfig) -> AsyncGrpcCall<TxInfo>
    where
        M: IntoProtobufAny + Send + 'static,
    {
        let inner = self.inner.clone();

        AsyncGrpcCall::new(move |context| async move {
            Ok(inner
                .grpc()?
                .submit_message(message, cfg)
                .context(&context)
                .await?)
        })
    }

    /// Sends the given amount of coins from signer's wallet to the given account address.
    pub fn transfer(
        &self,
        to_address: &AccAddress,
        amount: u64,
        cfg: TxConfig,
    ) -> AsyncGrpcCall<TxInfo> {
        let this = StateApi::new(self.inner.clone());
        let to_address = to_address.to_string();

        AsyncGrpcCall::new(move |context| async move {
            // remap error to one more appropriate in this context
            let from_address = this.inner.address().map_err(|_| Error::ReadOnlyMode)?;

            let msg = MsgSend {
                from_address: from_address.to_string(),
                to_address,
                amount: vec![Coin::utia(amount).into()],
            };

            this.submit_message(msg, cfg).context(&context).await
        })
    }

    /// Builds, signs and submits a PayForBlob transaction.
    ///
    /// # Note
    ///
    /// This is the same as [`BlobApi::submit`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use celestia_client::{Client, Result};
    /// # use celestia_client::tx::TxConfig;
    /// # async fn docs() -> Result<()> {
    /// use celestia_types::nmt::Namespace;
    /// use celestia_types::state::{Address, Coin};
    /// use celestia_types::{AppVersion, Blob};
    ///
    /// let client = Client::builder()
    ///     .rpc_url("ws://localhost:26658")
    ///     .grpc_url("http://localhost:9090")
    ///     .private_key_hex("393fdb5def075819de55756b45c9e2c8531a8c78dd6eede483d3440e9457d839")
    ///     .build()
    ///     .await?;
    ///
    /// let ns = Namespace::new_v0(b"abcd").unwrap();
    /// let blob = Blob::new(ns, "some data".into(), None, AppVersion::latest()).unwrap();
    ///
    /// client
    ///     .state()
    ///     .submit_pay_for_blob(&[blob], TxConfig::default())
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`BlobApi::submit`]: crate::api::BlobApi::submit
    pub fn submit_pay_for_blob(&self, blobs: &[Blob], cfg: TxConfig) -> AsyncGrpcCall<TxInfo> {
        let inner = self.inner.clone();
        let blobs = blobs.to_vec();

        AsyncGrpcCall::new(move |context| async move {
            Ok(inner
                .grpc()?
                .submit_blobs(&blobs, cfg)
                .context(&context)
                .await?)
        })
    }

    /// Cancels signer's pending undelegation from a validator.
    pub fn cancel_unbonding_delegation(
        &self,
        validator_address: &ValAddress,
        amount: u64,
        creation_height: u64,
        cfg: TxConfig,
    ) -> AsyncGrpcCall<TxInfo> {
        let this = StateApi::new(self.inner.clone());
        let validator_address = validator_address.to_string();

        AsyncGrpcCall::new(move |context| async move {
            let delegator_address = this.inner.address()?;

            let msg = MsgCancelUnbondingDelegation {
                delegator_address: delegator_address.to_string(),
                validator_address,
                amount: Some(Coin::utia(amount).into()),
                creation_height: height_i64(creation_height)?,
            };

            this.submit_message(msg, cfg).context(&context).await
        })
    }

    /// Sends signer's delegated tokens to a new validator for redelegation.
    pub fn begin_redelegate(
        &self,
        src_validator_address: &ValAddress,
        dest_validator_address: &ValAddress,
        amount: u64,
        cfg: TxConfig,
    ) -> AsyncGrpcCall<TxInfo> {
        let this = StateApi::new(self.inner.clone());
        let validator_src_address = src_validator_address.to_string();
        let validator_dst_address = dest_validator_address.to_string();

        AsyncGrpcCall::new(move |context| async move {
            let delegator_address = this.inner.address()?;

            let msg = MsgBeginRedelegate {
                delegator_address: delegator_address.to_string(),
                validator_src_address,
                validator_dst_address,
                amount: Some(Coin::utia(amount).into()),
            };

            this.submit_message(msg, cfg).context(&context).await
        })
    }

    /// Undelegates signer's delegated tokens, unbonding them from the current validator.
    pub fn undelegate(
        &self,
        validator_address: &ValAddress,
        amount: u64,
        cfg: TxConfig,
    ) -> AsyncGrpcCall<TxInfo> {
        let this = StateApi::new(self.inner.clone());
        let validator_address = validator_address.to_string();

        AsyncGrpcCall::new(move |context| async move {
            let delegator_address = this.inner.address()?;

            let msg = MsgUndelegate {
                delegator_address: delegator_address.to_string(),
                validator_address,
                amount: Some(Coin::utia(amount).into()),
            };

            this.submit_message(msg, cfg).context(&context).await
        })
    }

    /// Sends signer's liquid tokens to a validator for delegation.
    pub fn delegate(
        &self,
        validator_address: &ValAddress,
        amount: u64,
        cfg: TxConfig,
    ) -> AsyncGrpcCall<TxInfo> {
        let this = StateApi::new(self.inner.clone());
        let validator_address = validator_address.to_string();

        AsyncGrpcCall::new(move |context| async move {
            let delegator_address = this.inner.address()?;

            let msg = MsgDelegate {
                delegator_address: delegator_address.to_string(),
                validator_address,
                amount: Some(Coin::utia(amount).into()),
            };

            this.submit_message(msg, cfg).context(&context).await
        })
    }

    /// Retrieves the delegation information between signer and a validator.
    pub fn query_delegation(
        &self,
        validator_address: &ValAddress,
    ) -> AsyncGrpcCall<QueryDelegationResponse> {
        let this = StateApi::new(self.inner.clone());
        let validator_address = *validator_address;

        AsyncGrpcCall::new(move |context| async move {
            let delegator_address = this.inner.address()?;

            Ok(this
                .inner
                .grpc()?
                .query_delegation(&delegator_address, &validator_address)
                .context(&context)
                .await?)
        })
    }

    /// Retrieves the unbonding status between signer and a validator.
    pub fn query_unbonding(
        &self,
        validator_address: &ValAddress,
    ) -> AsyncGrpcCall<QueryUnbondingDelegationResponse> {
        let this = StateApi::new(self.inner.clone());
        let validator_address = *validator_address;

        AsyncGrpcCall::new(move |context| async move {
            let delegator_address = this.inner.address()?;

            Ok(this
                .inner
                .grpc()?
                .query_unbonding(&delegator_address, &validator_address)
                .context(&context)
                .await?)
        })
    }

    /// Retrieves the status of the redelegations between signer and a validator.
    pub fn query_redelegations(
        &self,
        src_validator_address: &ValAddress,
        dest_validator_address: &ValAddress,
    ) -> AsyncGrpcCall<QueryRedelegationsResponse> {
        let this = StateApi::new(self.inner.clone());
        let src_validator_address = *src_validator_address;
        let dest_validator_address = *dest_validator_address;

        AsyncGrpcCall::new(move |context| async move {
            let delegator_address = this.inner.address()?;

            let mut full_resp = QueryRedelegationsResponse {
                responses: Vec::new(),
                pagination: None,
            };

            let mut next_key = Vec::new();

            loop {
                let mut resp = this
                    .inner
                    .grpc()?
                    .query_redelegations(
                        &delegator_address,
                        &src_validator_address,
                        &dest_validator_address,
                        Some(PageRequest {
                            key: next_key,
                            ..Default::default()
                        }),
                    )
                    .context(&context)
                    .await?;

                full_resp.responses.append(&mut resp.responses);

                match resp.pagination {
                    Some(pagination) => next_key = pagination.next_key,
                    None => break,
                }
            }

            Ok(full_resp)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use celestia_grpc::Error as GrpcError;
    use celestia_rpc::Error as RpcError;
    use jsonrpsee::core::ClientError as JrpcError;
    use lumina_utils::test_utils::async_test;
    use tonic::Code;

    use celestia_grpc::TxConfig;
    use k256::ecdsa::SigningKey;

    use crate::test_utils::{
        TEST_GRPC_URL, TEST_RPC_URL, ensure_serializable_deserializable, new_client,
        new_read_only_client, new_rpc_only_client, node0_address, validator_address,
    };
    use crate::{Client, Error};

    #[async_test]
    async fn transfer() {
        let client = new_client().await;

        let random_key = SigningKey::random(&mut rand::rngs::OsRng);
        let random_acc = random_key.verifying_key().into();

        client
            .state()
            .transfer(&random_acc, 123, TxConfig::default())
            .await
            .unwrap();

        assert_eq!(
            client
                .state()
                .balance_for_address_unverified(&random_acc)
                .await
                .unwrap(),
            123
        );

        let client_ro = new_read_only_client().await;
        let e = client_ro
            .state()
            .transfer(&random_acc, 123, TxConfig::default())
            .await
            .unwrap_err();

        assert!(matches!(e, Error::ReadOnlyMode));
    }

    #[async_test]
    async fn delegation() {
        let client = new_client().await;
        let validator_addr = validator_address();
        let client_addr = client.address().unwrap();

        // Test delegation
        client
            .state()
            .delegate(&validator_addr, 100, TxConfig::default())
            .await
            .unwrap();

        let del = client
            .state()
            .query_delegation(&validator_addr)
            .await
            .unwrap();

        assert_eq!(del.response.balance, 100);
        assert_eq!(del.response.delegation.delegator_address, client_addr);
        assert_eq!(del.response.delegation.validator_address, validator_addr);
        assert_eq!(del.response.delegation.shares, 100.into());

        // Test unbonding
        let unbond_tx_height = client
            .state()
            .undelegate(&validator_addr, 10, TxConfig::default())
            .await
            .unwrap()
            .height;

        let unbond = client
            .state()
            .query_unbonding(&validator_addr)
            .await
            .unwrap();

        assert_eq!(unbond.unbond.delegator_address, client_addr);
        assert_eq!(unbond.unbond.validator_address, validator_addr);
        assert_eq!(unbond.unbond.entries.len(), 1);
        assert_eq!(
            unbond.unbond.entries[0].creation_height.value(),
            unbond_tx_height
        );
        assert_eq!(unbond.unbond.entries[0].initial_balance, 10);
        assert_eq!(unbond.unbond.entries[0].balance, 10);

        let del = client
            .state()
            .query_delegation(&validator_addr)
            .await
            .unwrap();

        assert_eq!(del.response.balance, 90);
        assert_eq!(del.response.delegation.delegator_address, client_addr);
        assert_eq!(del.response.delegation.validator_address, validator_addr);
        assert_eq!(del.response.delegation.shares, 90.into());

        // Test partial cancel unbonding
        client
            .state()
            .cancel_unbonding_delegation(&validator_addr, 3, unbond_tx_height, TxConfig::default())
            .await
            .unwrap();

        let unbond = client
            .state()
            .query_unbonding(&validator_addr)
            .await
            .unwrap();

        assert_eq!(unbond.unbond.delegator_address, client_addr);
        assert_eq!(unbond.unbond.validator_address, validator_addr);
        assert_eq!(unbond.unbond.entries.len(), 1);
        assert_eq!(
            unbond.unbond.entries[0].creation_height.value(),
            unbond_tx_height
        );
        assert_eq!(unbond.unbond.entries[0].initial_balance, 7);
        assert_eq!(unbond.unbond.entries[0].balance, 7);

        let del = client
            .state()
            .query_delegation(&validator_addr)
            .await
            .unwrap();

        assert_eq!(del.response.balance, 93);
        assert_eq!(del.response.delegation.delegator_address, client_addr);
        assert_eq!(del.response.delegation.validator_address, validator_addr);
        assert_eq!(del.response.delegation.shares, 93.into());

        // Test fully cancel unbonding
        client
            .state()
            .cancel_unbonding_delegation(&validator_addr, 7, unbond_tx_height, TxConfig::default())
            .await
            .unwrap();

        let err = client
            .state()
            .query_unbonding(&validator_addr)
            .await
            .unwrap_err();

        assert_eq!(err.as_grpc_status().unwrap().code(), tonic::Code::NotFound);

        let del = client
            .state()
            .query_delegation(&validator_addr)
            .await
            .unwrap();

        assert_eq!(del.response.balance, 100);
        assert_eq!(del.response.delegation.delegator_address, client_addr);
        assert_eq!(del.response.delegation.validator_address, validator_addr);
        assert_eq!(del.response.delegation.shares, 100.into());
    }

    #[async_test]
    async fn balance_for_address() {
        let client_ro = new_read_only_client().await;

        // Read only mode allows calling `balance_for_address`
        let addr = node0_address();
        let balance = client_ro.state().balance_for_address(&addr).await.unwrap();
        assert!(balance > 0);

        // Read only mode allows calling `balance_for_address_unverified`.
        let balance = client_ro
            .state()
            .balance_for_address_unverified(&addr)
            .await
            .unwrap();
        assert!(balance > 0);

        // Read only mode does not allow calling `balance`
        let e = client_ro.state().balance().await.unwrap_err();
        assert!(matches!(e, Error::NoAssociatedAddress));

        // Read only mode does not allow calling `balance_unverified`
        let e = client_ro.state().balance().await.unwrap_err();
        assert!(matches!(e, Error::NoAssociatedAddress));

        let client_rpc = new_rpc_only_client().await;

        // RPC only mode allows calling `balance_for_address`
        let balance = client_rpc.state().balance_for_address(&addr).await.unwrap();
        assert!(balance > 0);

        // RPC only mode does not allow calling `balance_for_address_unverified`.
        let e = client_rpc
            .state()
            .balance_for_address_unverified(&addr)
            .await
            .unwrap_err();
        assert!(matches!(e, Error::GrpcEndpointNotSet));
    }

    #[allow(dead_code)]
    #[allow(unused_variables)]
    #[allow(unreachable_code)]
    #[allow(clippy::diverging_sub_expression)]
    async fn enforce_serde_bounds() {
        // intentionally no-run, compile only test
        let api = StateApi::new(unimplemented!());

        let cfg = ensure_serializable_deserializable(TxConfig::default());
        let val_addr: ValAddress = ensure_serializable_deserializable(unimplemented!());
        let acc_addr: AccAddress = ensure_serializable_deserializable(unimplemented!());

        ensure_serializable_deserializable(api.balance().await.unwrap());

        ensure_serializable_deserializable(api.balance_unverified().await.unwrap());

        ensure_serializable_deserializable(api.balance_for_address(&acc_addr).await.unwrap());

        ensure_serializable_deserializable(
            api.balance_for_address_unverified(&acc_addr).await.unwrap(),
        );

        ensure_serializable_deserializable(api.estimate_gas_price(TxPriority::Low).await.unwrap());

        ensure_serializable_deserializable(
            api.estimate_gas_price_and_usage(TxPriority::Low, Vec::new())
                .await
                .unwrap(),
        );

        ensure_serializable_deserializable(api.submit_message((), cfg).await.unwrap());

        ensure_serializable_deserializable(api.transfer(&acc_addr, 0, cfg).await.unwrap());

        let blobs: Vec<_> = ensure_serializable_deserializable(unimplemented!());
        ensure_serializable_deserializable(api.submit_pay_for_blob(&blobs, cfg).await.unwrap());

        ensure_serializable_deserializable(
            api.cancel_unbonding_delegation(&val_addr, 0, 0, cfg)
                .await
                .unwrap(),
        );

        ensure_serializable_deserializable(
            api.begin_redelegate(&val_addr, &val_addr, 0, cfg)
                .await
                .unwrap(),
        );

        ensure_serializable_deserializable(api.undelegate(&val_addr, 0, cfg).await.unwrap());

        ensure_serializable_deserializable(api.delegate(&val_addr, 0, cfg).await.unwrap());

        ensure_serializable_deserializable(api.query_delegation(&val_addr).await.unwrap());

        ensure_serializable_deserializable(api.query_unbonding(&val_addr).await.unwrap());

        ensure_serializable_deserializable(
            api.query_redelegations(&val_addr, &val_addr).await.unwrap(),
        );
    }

    #[async_test]
    async fn rpc_timeout() {
        let client_build_error = Client::builder()
            .rpc_url(TEST_RPC_URL)
            .grpc_url(TEST_GRPC_URL)
            .timeout(Duration::from_nanos(1))
            .build()
            .await
            .unwrap_err();

        assert!(matches!(
            client_build_error,
            Error::Rpc(RpcError::JsonRpc(JrpcError::RequestTimeout))
        ));
    }

    #[async_test]
    async fn grpc_timeout() {
        let client = new_client().await;

        let balance_timeout = client
            .state()
            .balance()
            .timeout(Duration::from_nanos(100))
            .await
            .unwrap_err();

        let _balance_ok = client.state().balance().await.unwrap();

        let Error::Grpc(GrpcError::TonicError(status)) = balance_timeout else {
            panic!("Invalid error type");
        };

        assert!(status.code() == Code::DeadlineExceeded || status.code() == Code::Cancelled);
    }
}
