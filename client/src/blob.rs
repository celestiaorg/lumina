use std::pin::Pin;
use std::sync::Arc;

use async_stream::try_stream;
use celestia_rpc::BlobClient;
use futures_util::{Stream, StreamExt};

use crate::Result;
use crate::api::blob::BlobsAtHeight;
use crate::client::ClientInner;
use crate::state::AsyncGrpcCall;
use crate::tx::{TxConfig, TxInfo};
use crate::types::nmt::{Namespace, NamespaceProof};
use crate::types::{Blob, Commitment};

/// Blob API for quering bridge nodes.
pub struct BlobApi {
    inner: Arc<ClientInner>,
}

impl BlobApi {
    pub(crate) fn new(inner: Arc<ClientInner>) -> BlobApi {
        BlobApi { inner }
    }

    /// Submit given blobs to celestia network.
    ///
    /// # Note
    ///
    /// This is the same as [`StateApi::submit_pay_for_blob`].
    ///
    /// # Example
    /// ```no_run
    /// # use celestia_client::{Client, Result};
    /// # use celestia_client::tx::TxConfig;
    /// # async fn docs() -> Result<()> {
    /// use celestia_types::nmt::Namespace;
    /// use celestia_types::state::{Address, Coin};
    /// use celestia_types::Blob;
    ///
    /// let client = Client::builder()
    ///     .rpc_url("ws://localhost:26658")
    ///     .grpc_url("http://localhost:9090")
    ///     .private_key_hex("393fdb5def075819de55756b45c9e2c8531a8c78dd6eede483d3440e9457d839")
    ///     .build()
    ///     .await?;
    ///
    /// let ns = Namespace::new_v0(b"abcd").unwrap();
    /// let blob = Blob::new(ns, "some data".into(), None).unwrap();
    ///
    /// client.blob().submit(&[blob], TxConfig::default()).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`StateApi::submit_pay_for_blob`]: crate::api::StateApi::submit_pay_for_blob
    pub fn submit(&self, blobs: &[Blob], cfg: TxConfig) -> AsyncGrpcCall<TxInfo> {
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

    /// Retrieves the blob by commitment under the given namespace and height.
    pub async fn get(
        &self,
        height: u64,
        namespace: Namespace,
        commitment: Commitment,
    ) -> Result<Blob> {
        let blob = self
            .inner
            .rpc
            .blob_get(height, namespace, commitment)
            .await?;

        blob.validate_with_commitment(&commitment)?;

        Ok(blob)
    }

    /// Retrieves all blobs from the given namespaces and height.
    pub async fn get_all(
        &self,
        height: u64,
        namespaces: &[Namespace],
    ) -> Result<Option<Vec<Blob>>> {
        let Some(blobs) = self.inner.rpc.blob_get_all(height, namespaces).await? else {
            return Ok(None);
        };

        for blob in &blobs {
            blob.validate()?;
        }

        Ok(Some(blobs))
    }

    /// Retrieves proofs in the given namespaces at the given height by commitment.
    pub async fn get_proof(
        &self,
        height: u64,
        namespace: Namespace,
        commitment: Commitment,
    ) -> Result<Vec<NamespaceProof>> {
        Ok(self
            .inner
            .rpc
            .blob_get_proof(height, namespace, commitment)
            .await?)
    }

    /// Checks whether a blob's given commitment is included in the namespace at the given height.
    pub async fn included(
        &self,
        height: u64,
        namespace: Namespace,
        proof: &NamespaceProof,
        commitment: Commitment,
    ) -> Result<bool> {
        Ok(self
            .inner
            .rpc
            .blob_included(height, namespace, proof, commitment)
            .await?)
    }

    /// Subscribe to blobs from the given namespace, returning
    /// them as they are being published.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use futures_util::StreamExt;
    /// # use celestia_client::{Client, Result};
    /// # async fn docs() -> Result<()> {
    /// use celestia_types::nmt::Namespace;
    ///
    /// let client = Client::builder()
    ///     .rpc_url("ws://localhost:26658")
    ///     .build()
    ///     .await?;
    ///
    /// let ns = Namespace::new_v0(b"mydata").unwrap();
    /// let mut blobs_rx = client.blob().subscribe(ns);
    ///
    /// while let Some(blobs) = blobs_rx.next().await {
    ///     dbg!(blobs);
    /// }
    /// # Ok(())
    /// # }
    pub fn subscribe(
        &self,
        namespace: Namespace,
    ) -> Pin<Box<dyn Stream<Item = Result<BlobsAtHeight>> + Send + 'static>> {
        let inner = self.inner.clone();

        // we need to re-stream it to map error and satisfy 'static
        try_stream! {
            let mut subscription = inner.rpc.blob_subscribe(namespace);
            while let Some(item) = subscription.next().await {
                yield item?;
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{ensure_serializable_deserializable, new_client};
    use lumina_utils::test_utils::async_test;

    #[async_test]
    async fn blob_submit_and_retrieve() {
        let client = new_client().await;

        let ns = Namespace::new_v0(b"mydata").unwrap();

        let blob = Blob::new(
            ns,
            b"some data to store".to_vec(),
            Some(client.address().unwrap()),
        )
        .unwrap();

        let submitted_commitment = blob.commitment;
        let tx_info = client
            .blob()
            .submit(&[blob], TxConfig::default())
            .await
            .unwrap();

        let received_blob = client
            .blob()
            .get(tx_info.height, ns, submitted_commitment)
            .await
            .unwrap();

        received_blob
            .validate_with_commitment(&submitted_commitment)
            .unwrap();
    }

    #[async_test]
    async fn blob_retrieve_unknown() {
        let client = new_client().await;

        let head = client.header().head().await.unwrap();

        let ns = Namespace::new_v0(b"mydata").unwrap();
        let commitment = Commitment::new(rand::random());

        client
            .blob()
            .get(head.height(), ns, commitment)
            .await
            .unwrap_err();
    }

    #[allow(dead_code)]
    #[allow(unused_variables)]
    #[allow(unreachable_code)]
    #[allow(clippy::diverging_sub_expression)]
    async fn enforce_serde_bounds() {
        // intentionally no-run, compile only test
        let api = BlobApi::new(unimplemented!());

        let blobs: Vec<_> = ensure_serializable_deserializable(unimplemented!());
        let cfg = ensure_serializable_deserializable(unimplemented!());
        ensure_serializable_deserializable(api.submit(&blobs, cfg).await.unwrap());

        let namespace = ensure_serializable_deserializable(unimplemented!());
        let commitment = ensure_serializable_deserializable(unimplemented!());
        ensure_serializable_deserializable(api.get(0, namespace, commitment).await.unwrap());

        let namespaces: Vec<_> = ensure_serializable_deserializable(unimplemented!());
        ensure_serializable_deserializable(api.get_all(0, &namespaces).await.unwrap());

        let namespace = ensure_serializable_deserializable(unimplemented!());
        let commitment = ensure_serializable_deserializable(unimplemented!());
        ensure_serializable_deserializable(api.get_proof(0, namespace, commitment).await.unwrap());

        let namespace = ensure_serializable_deserializable(unimplemented!());
        let proof = ensure_serializable_deserializable(unimplemented!());
        let commitment = ensure_serializable_deserializable(unimplemented!());
        ensure_serializable_deserializable(
            api.included(0, namespace, &proof, commitment)
                .await
                .unwrap(),
        );

        let namespace = ensure_serializable_deserializable(unimplemented!());
        ensure_serializable_deserializable(api.subscribe(namespace).next().await.unwrap().unwrap());
    }
}
