//! celestia-node rpc types and methods related to fraud proofs

use std::future::Future;
use std::marker::{Send, Sync};
use std::pin::Pin;

use async_stream::try_stream;
use futures_util::{Stream, StreamExt};
use jsonrpsee::core::client::{ClientT, Error, SubscriptionClientT};
use jsonrpsee::proc_macros::rpc;

use crate::{HeaderClient, custom_client_error};

pub use celestia_types::fraud_proof::{Proof, ProofType};

mod rpc {
    use jsonrpsee::core::{RpcResult, SubscriptionResult};

    use super::*;

    #[rpc(client, server, namespace = "fraud", namespace_separator = ".")]
    pub trait Fraud {
        #[method(name = "Get")]
        async fn fraud_get(&self, proof_type: ProofType) -> RpcResult<Vec<Proof>>;
    }

    #[rpc(client, server, namespace = "fraud", namespace_separator = ".")]
    pub trait FraudSubscription {
        #[subscription(name = "Subscribe", unsubscribe = "Unsubscribe", item = Proof)]
        async fn fraud_subscribe(&self, proof_type: ProofType) -> SubscriptionResult;
    }
}

/// Client implementation for the `Fraud` RPC API.
pub trait FraudClient: ClientT {
    /// Fetches fraud proofs by their type.
    fn fraud_get<'a, 'fut>(
        &'a self,
        proof_type: ProofType,
    ) -> impl Future<Output = Result<Vec<Proof>, Error>> + Send + 'fut
    where
        'a: 'fut,
        Self: Sized + Sync + 'fut,
    {
        rpc::FraudClient::fraud_get(self, proof_type)
    }

    /// Subscribe to fraud proof by its type.
    ///
    /// # Notes
    ///
    /// If client returns [`Error::HttpNotImplemented`], the subscription will fallback to
    /// using combination of [`HeaderClient::header_wait_for_height`] and
    /// [`FraudClient::fraud_get`] for streaming the proofs. The fallback stream will end
    /// after the first batch of proofs is returned.
    ///
    /// Unsubscribe is not implemented by Celestia nodes.
    fn fraud_subscribe<'a>(
        &'a self,
        proof_type: ProofType,
    ) -> Pin<Box<dyn Stream<Item = Result<Proof, Error>> + Send + 'a>>
    where
        Self: SubscriptionClientT + Sized + Sync,
    {
        try_stream! {
            match rpc::FraudSubscriptionClient::fraud_subscribe(self, proof_type).await {
                Ok(mut fraud_sub) => loop {
                    yield fraud_sub
                        .next()
                        .await
                        .ok_or_else(|| custom_client_error("unexpected end of stream"))??;
                },
                Err(Error::HttpNotImplemented) => {
                    let mut header_sub = HeaderClient::header_subscribe(self);
                    loop {
                        // tick; we don't care about the header
                        header_sub
                            .next()
                            .await
                            .ok_or_else(|| custom_client_error("unexpected end of stream"))??;

                        let proofs = rpc::FraudClient::fraud_get(self, proof_type).await?;
                        if !proofs.is_empty() {
                            for proof in proofs {
                                yield proof;
                            }

                            // after we get any proofs from the node, it will
                            // keep giving us the same proofs on each request,
                            // so we just end the stream here
                            break;
                        }
                    }
                }
                err => {
                    err?;
                }
            };
        }
        .boxed()
    }
}

impl<T> FraudClient for T where T: ClientT {}

pub trait FraudServer: rpc::FraudServer + rpc::FraudSubscriptionServer {}

impl<T> FraudServer for T where T: rpc::FraudServer + rpc::FraudSubscriptionServer {}

pub use rpc::FraudServer as FraudRpcServer;
pub use rpc::FraudSubscriptionServer as FraudSubscriptionRpcServer;
