use std::pin::Pin;
use std::sync::Arc;

use async_stream::try_stream;
use celestia_rpc::HeaderClient;
use futures_util::{Stream, StreamExt};

use crate::Result;
use crate::client::ClientInner;
use crate::types::hash::Hash;
use crate::types::{ExtendedHeader, SyncState};

/// Header API for quering bridge nodes.
pub struct HeaderApi {
    inner: Arc<ClientInner>,
}

impl HeaderApi {
    pub(crate) fn new(inner: Arc<ClientInner>) -> HeaderApi {
        HeaderApi { inner }
    }

    /// Returns the latest header synchronized by the node.
    pub async fn head(&self) -> Result<ExtendedHeader> {
        let header = self.inner.rpc.header_local_head().await?;
        header.validate()?;
        Ok(header)
    }

    /// Returns the latest header announced in the network.
    pub async fn network_head(&self) -> Result<ExtendedHeader> {
        let header = self.inner.rpc.header_network_head().await?;
        header.validate()?;
        Ok(header)
    }

    /// Returns the header of the given hash from the node's header store.
    pub async fn get_by_hash(&self, hash: Hash) -> Result<ExtendedHeader> {
        let header = self.inner.rpc.header_get_by_hash(hash).await?;
        header.validate()?;
        Ok(header)
    }

    /// Returns the header at the given height, if it is
    /// currently available.
    pub async fn get_by_height(&self, height: u64) -> Result<ExtendedHeader> {
        let header = self.inner.rpc.header_get_by_height(height).await?;
        header.validate()?;
        Ok(header)
    }

    /// Returns the given range headers from the node and verifies that they
    /// form a confirmed and continuous chain starting from the `from` header.
    ///
    /// The range is exclusive from both sides.
    pub async fn get_range_by_height(
        &self,
        from: &ExtendedHeader,
        to: u64,
    ) -> Result<Vec<ExtendedHeader>> {
        from.validate()?;

        let headers = self
            .inner
            .rpc
            .header_get_range_by_height(from.clone(), to)
            .await?;

        for header in &headers {
            header.validate()?;
        }

        from.verify_adjacent_range(&headers)?;

        Ok(headers)
    }

    /// Blocks until the header at the given height has been synced by the node.
    pub async fn wait_for_height(&self, height: u64) -> Result<ExtendedHeader> {
        let header = self.inner.rpc.header_wait_for_height(height).await?;
        header.validate()?;
        Ok(header)
    }

    /// Returns the current state of the node's Syncer.
    pub async fn sync_state(&self) -> Result<SyncState> {
        Ok(self.inner.rpc.header_sync_state().await?)
    }

    /// Blocks until the node's Syncer is synced to network head.
    pub async fn sync_wait(&self) -> Result<()> {
        Ok(self.inner.rpc.header_sync_wait().await?)
    }

    /// Subscribe to recent headers from the network.
    ///
    /// Headers will be validated and verified with the one that was
    /// previously received.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use futures_util::StreamExt;
    /// # use celestia_client::{Client, Result};
    /// # async fn docs() -> Result<()> {
    /// let client = Client::builder()
    ///     .rpc_url("ws://localhost:26658")
    ///     .build()
    ///     .await?;
    ///
    /// let mut headers_rx = client.header().subscribe();
    ///
    /// while let Some(header) = headers_rx.next().await {
    ///     dbg!(header);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn subscribe(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<ExtendedHeader>> + Send + 'static>> {
        let inner = self.inner.clone();

        // we need to re-stream it to map error and satisfy 'static
        try_stream! {
            let mut subscription = inner.rpc.header_subscribe();
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

    use crate::test_utils::ensure_serializable_deserializable;

    #[allow(dead_code)]
    #[allow(unused_variables)]
    #[allow(unreachable_code)]
    #[allow(clippy::diverging_sub_expression)]
    async fn enforce_serde_bounds() {
        // intentionally no-run, compile only test
        let api = HeaderApi::new(unimplemented!());

        let _: () = api.sync_wait().await.unwrap();

        ensure_serializable_deserializable(api.head().await.unwrap());

        ensure_serializable_deserializable(api.network_head().await.unwrap());

        let hash = ensure_serializable_deserializable(unimplemented!());
        ensure_serializable_deserializable(api.get_by_hash(hash).await.unwrap());

        ensure_serializable_deserializable(api.get_by_height(0).await.unwrap());

        let header = ensure_serializable_deserializable(unimplemented!());
        ensure_serializable_deserializable(api.get_range_by_height(&header, 0).await.unwrap());

        ensure_serializable_deserializable(api.wait_for_height(0).await.unwrap());

        ensure_serializable_deserializable(api.sync_state().await.unwrap());

        ensure_serializable_deserializable(api.subscribe().next().await.unwrap().unwrap());
    }
}
