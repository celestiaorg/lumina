//! celestia-node rpc types and methods related to blobs

use std::future::Future;
use std::marker::{Send, Sync};
use std::pin::Pin;

use async_stream::try_stream;
use celestia_types::nmt::{Namespace, NamespaceProof};
use celestia_types::{Blob, Commitment};
use futures_util::{Stream, StreamExt};
use jsonrpsee::core::client::{ClientT, Error, SubscriptionClientT};
use jsonrpsee::core::{RpcResult, SubscriptionResult};
use jsonrpsee::proc_macros::rpc;
use serde::{Deserialize, Serialize};

use crate::{HeaderClient, TxConfig, custom_client_error};

/// Response type for [`BlobClient::blob_subscribe`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BlobsAtHeight {
    /// Blobs submitted at given height.
    pub blobs: Option<Vec<Blob>>,
    /// A height for which the blobs were returned.
    pub height: u64,
}

mod rpc {
    use super::*;

    #[rpc(client, server, namespace = "blob", namespace_separator = ".")]
    pub trait Blob {
        #[method(name = "Get")]
        async fn blob_get(
            &self,
            height: u64,
            namespace: Namespace,
            commitment: Commitment,
        ) -> RpcResult<Blob>;

        #[method(name = "GetAll")]
        async fn blob_get_all(
            &self,
            height: u64,
            namespaces: Vec<Namespace>,
        ) -> RpcResult<Option<Vec<Blob>>>;

        #[method(name = "GetProof")]
        async fn blob_get_proof(
            &self,
            height: u64,
            namespace: Namespace,
            commitment: Commitment,
        ) -> RpcResult<Vec<NamespaceProof>>;

        #[method(name = "Included")]
        async fn blob_included(
            &self,
            height: u64,
            namespace: Namespace,
            proof: NamespaceProof,
            commitment: Commitment,
        ) -> RpcResult<bool>;

        #[method(name = "Submit")]
        async fn blob_submit(&self, blobs: Vec<Blob>, opts: TxConfig) -> RpcResult<u64>;
    }

    #[rpc(client, server, namespace = "blob", namespace_separator = ".")]
    pub trait BlobSubscription {
        #[subscription(name = "Subscribe", unsubscribe = "Unsubscribe", item = BlobsAtHeight)]
        async fn blob_subscribe(&self, namespace: Namespace) -> SubscriptionResult;
    }
}

/// Client implementation for the `Blob` RPC API.
pub trait BlobClient: ClientT {
    /// Get retrieves the blob by commitment under the given namespace and height.
    fn blob_get<'a, 'fut>(
        &'a self,
        height: u64,
        namespace: Namespace,
        commitment: Commitment,
    ) -> impl Future<Output = Result<Blob, Error>> + Send + 'fut
    where
        'a: 'fut,
        Self: Sized + Sync + 'fut,
    {
        rpc::BlobClient::blob_get(self, height, namespace, commitment)
    }

    /// GetAll returns all blobs under the given namespaces and height.
    fn blob_get_all<'a, 'b, 'fut>(
        &'a self,
        height: u64,
        namespaces: &'b [Namespace],
    ) -> impl Future<Output = Result<Option<Vec<Blob>>, Error>> + Send + 'fut
    where
        'a: 'fut,
        'b: 'fut,
        Self: Sized + Sync + 'fut,
    {
        rpc::BlobClient::blob_get_all(self, height, namespaces.to_vec())
    }

    /// GetProof retrieves proofs in the given namespaces at the given height by commitment.
    fn blob_get_proof<'a, 'fut>(
        &'a self,
        height: u64,
        namespace: Namespace,
        commitment: Commitment,
    ) -> impl Future<Output = Result<Vec<NamespaceProof>, Error>> + Send + 'fut
    where
        'a: 'fut,
        Self: Sized + Sync + 'fut,
    {
        rpc::BlobClient::blob_get_proof(self, height, namespace, commitment)
    }

    /// Included checks whether a blob's given commitment(Merkle subtree root) is included at given height and under the namespace.
    fn blob_included<'a, 'b, 'fut>(
        &'a self,
        height: u64,
        namespace: Namespace,
        proof: &'b NamespaceProof,
        commitment: Commitment,
    ) -> impl Future<Output = Result<bool, Error>> + Send + 'fut
    where
        'a: 'fut,
        'b: 'fut,
        Self: Sized + Sync + 'fut,
    {
        rpc::BlobClient::blob_included(self, height, namespace, proof.clone(), commitment)
    }

    /// Submit sends Blobs and reports the height in which they were included. Allows sending multiple Blobs atomically synchronously. Uses default wallet registered on the Node.
    fn blob_submit<'a, 'b, 'fut>(
        &'a self,
        blobs: &'b [Blob],
        opts: TxConfig,
    ) -> impl Future<Output = Result<u64, Error>> + Send + 'fut
    where
        'a: 'fut,
        'b: 'fut,
        Self: Sized + Sync + 'fut,
    {
        rpc::BlobClient::blob_submit(self, blobs.to_vec(), opts)
    }

    /// Subscribe to published blobs from the given namespace as they are included.
    ///
    /// # Notes
    ///
    /// If client returns [`Error::HttpNotImplemented`], the subscription will fallback to
    /// using combination of [`HeaderClient::header_wait_for_height`] and
    /// [`BlobClient::blob_get_all`] for streaming the blobs.
    ///
    /// Unsubscribe is not implemented by Celestia nodes.
    fn blob_subscribe<'a>(
        &'a self,
        namespace: Namespace,
    ) -> Pin<Box<dyn Stream<Item = Result<BlobsAtHeight, Error>> + Send + 'a>>
    where
        Self: SubscriptionClientT + Sized + Sync,
    {
        try_stream! {
            let subscription_res = rpc::BlobSubscriptionClient::blob_subscribe(self, namespace).await;
            let has_real_sub = !matches!(&subscription_res, Err(Error::HttpNotImplemented));

            let (mut blob_sub, mut header_sub) = if has_real_sub {
                (Some(subscription_res?), None)
            } else {
                (None, Some(HeaderClient::header_subscribe(self)))
            };

            // TODO: should we validate blobs? we could do it in fallback,
            // but we don't know the app version for real sub
            loop {
                yield if has_real_sub {
                    blob_sub
                        .as_mut()
                        .expect("must be some")
                        .next()
                        .await
                        .ok_or_else(|| custom_client_error("unexpected end of stream"))??
                } else {
                    let header = header_sub
                        .as_mut()
                        .expect("must be some")
                        .next()
                        .await
                        .ok_or_else(|| custom_client_error("unexpected end of stream"))??;
                    let height = header.height();
                    let blobs = rpc::BlobClient::blob_get_all(self, height, vec![namespace]).await?;

                    BlobsAtHeight {
                        blobs,
                        height,
                    }
                };
            }
        }
        .boxed()
    }
}

pub trait BlobServer: rpc::BlobServer + rpc::BlobSubscriptionServer {}

impl<T> BlobServer for T where T: rpc::BlobServer + rpc::BlobSubscriptionServer {}

impl<T> BlobClient for T where T: ClientT {}

pub use rpc::BlobServer as BlobRpcServer;
pub use rpc::BlobSubscriptionServer as BlobSubscriptionRpcServer;
