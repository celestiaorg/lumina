//! Types and utilities related to header/blob/share subscriptions

use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use celestia_types::blob::BlobsAtHeight;
use celestia_types::namespace_data::NamespaceData;
use celestia_types::nmt::Namespace;
use celestia_types::{Blob, ExtendedHeader, SharesAtHeight};
use lumina_utils::executor::yield_now;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use crate::NodeError;
use crate::p2p::{P2p, P2pError};
use crate::store::{Store, StoreError};

const SHWAP_FETCH_TIMEOUT: Duration = Duration::from_secs(5);
const HEADER_BROADCAST_CHANNEL_CAPACITY: usize = 16;

/// Error thrown while processing the subscription
#[derive(Debug, thiserror::Error)]
pub enum SubscriptionError {
    /// Error retrieving subscription item
    #[error("Unable to receive subscription item at {height}: {source}")]
    Node {
        /// Height of the subscription item
        height: u64,
        /// Error that occurred
        #[source]
        source: NodeError,
    },
    /// Receiver lagged too far behind and the subscription will restart from the current head
    #[error("Subscription item height already pruned from the store, skipping {0} items")]
    Lagged(u64),
}

fn reconstruct_blobs(
    namespace_data: NamespaceData,
    header: &ExtendedHeader,
) -> Result<BlobsAtHeight, P2pError> {
    let shares = namespace_data
        .rows()
        .iter()
        .flat_map(|row| row.shares.iter());
    let blobs = Blob::reconstruct_all(shares)?;
    Ok(BlobsAtHeight {
        height: header.height(),
        blobs,
    })
}
/// As it gets notified about new header ranges being inserted, it generates a contiguous
/// stream of Headers as they are synchronised by the node.
#[derive(Debug)]
pub(crate) struct BroadcastingStore<S> {
    inner: Arc<S>,
    sender: broadcast::Sender<ExtendedHeader>,
    last_sent_height: Option<u64>,
    pending: Vec<Vec<ExtendedHeader>>,
}

impl<S> BroadcastingStore<S>
where
    S: Store,
{
    pub fn new(store: Arc<S>) -> Self {
        let (sender, _) = broadcast::channel(HEADER_BROADCAST_CHANNEL_CAPACITY);
        BroadcastingStore {
            inner: store,
            sender,
            last_sent_height: None,
            pending: Default::default(),
        }
    }

    pub fn clone_inner_store(&self) -> Arc<S> {
        self.inner.clone()
    }

    /// Prepare BroadcastingStore for forwarding headers it received, by setting
    /// the header height it should start at
    pub(crate) fn init_broadcast(&mut self, head: ExtendedHeader) {
        if self.last_sent_height.is_none() {
            // First initialisation means Syncer has acquired a network head for the first time,
            // start from there
            self.last_sent_height = Some(head.height());
            let _ = self.sender.send(head);
        } else {
            // Subsequent initialisations happen when syncer re-connects to the network
            // this could have caused a gap in sent heights. This will get sorted out on
            // next [`insert`].
            self.pending.push(vec![head]);
        }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<ExtendedHeader> {
        self.sender.subscribe()
    }

    pub(crate) async fn announce_insert(
        &mut self,
        range: Vec<ExtendedHeader>,
    ) -> Result<(), StoreError> {
        let last_sent_height = self
            .last_sent_height
            .expect("syncer should have initialised the height by now");

        let Some(lowest_range_height) = range.first().map(|h| h.height()) else {
            // Ignore empty range
            return Ok(());
        };

        debug_assert!(
            range.last().map(|h| h.height()).unwrap() < last_sent_height
                || lowest_range_height > last_sent_height
        );
        if lowest_range_height < last_sent_height {
            // We know range cannot cross last_sent_height, so either both ends are before or after.
            // Ignore node syncing historical header ranges.
            return self.inner.insert(range).await;
        }

        self.inner.insert(range.clone()).await?;

        if last_sent_height + 1 == lowest_range_height {
            self.send_range(range).await;
        } else {
            self.pending.push(range);
        }

        let mut i = 0;
        while i < self.pending.len() {
            let last_sent_height = self
                .last_sent_height
                .expect("last_sent_height should be initialised here");
            let first_pending_height = self.pending[i]
                .first()
                .expect("header range shouldn't be empty")
                .height();

            if last_sent_height + 1 == first_pending_height {
                let range = self.pending.swap_remove(i);
                self.send_range(range).await;
                i = 0;
            } else {
                i += 1;
            }
        }

        Ok(())
    }

    async fn send_range(&mut self, headers: Vec<ExtendedHeader>) {
        self.last_sent_height = Some(
            headers
                .last()
                .expect("range shouldn't be empty here")
                .height(),
        );
        for header in headers {
            if self.sender.send(header).is_err() {
                return; // no receivers - skip sending
            }
            // yield to allow receivers to go before the channel fills
            yield_now().await;
        }
    }
}

impl<S> Deref for BroadcastingStore<S>
where
    S: Store,
{
    type Target = S;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

pub(crate) async fn forward_new_blobs(
    namespace: Namespace,
    tx: mpsc::Sender<Result<BlobsAtHeight, SubscriptionError>>,
    mut header_receiver: broadcast::Receiver<ExtendedHeader>,
    p2p: Arc<P2p>,
) {
    loop {
        let header = match header_receiver.recv().await {
            Ok(header) => header,
            Err(RecvError::Lagged(skipped)) => {
                let _ = tx.send(Err(SubscriptionError::Lagged(skipped))).await;
                continue;
            }
            Err(RecvError::Closed) => {
                return;
            }
        };

        let blobs_or_error = p2p
            .get_namespace_data(namespace, header.height(), Some(SHWAP_FETCH_TIMEOUT))
            .await
            .and_then(|namespace_data| reconstruct_blobs(namespace_data, &header))
            .map_err(|e| SubscriptionError::Node {
                height: header.height(),
                source: e.into(),
            });

        if tx.send(blobs_or_error).await.is_err() {
            return; // receiver dropped
        }
    }
}

pub(crate) async fn forward_new_shares(
    namespace: Namespace,
    tx: mpsc::Sender<Result<SharesAtHeight, SubscriptionError>>,
    mut header_receiver: broadcast::Receiver<ExtendedHeader>,
    p2p: Arc<P2p>,
) {
    loop {
        let header = match header_receiver.recv().await {
            Ok(header) => header,
            Err(RecvError::Lagged(skipped)) => {
                let _ = tx.send(Err(SubscriptionError::Lagged(skipped))).await;
                continue;
            }
            Err(RecvError::Closed) => {
                return;
            }
        };

        let shares_or_error = match p2p
            .get_namespace_data(namespace, header.height(), Some(SHWAP_FETCH_TIMEOUT))
            .await
        {
            Ok(namespace_data) => Ok(SharesAtHeight {
                height: header.height(),
                shares: namespace_data
                    .into_inner()
                    .into_iter()
                    .flat_map(|row| row.shares.into_iter())
                    .collect(),
            }),
            Err(e) => Err(SubscriptionError::Node {
                height: header.height(),
                source: e.into(),
            }),
        };

        if tx.send(shares_or_error).await.is_err() {
            return; // receiver dropped
        }
    }
}

impl From<BroadcastStreamRecvError> for SubscriptionError {
    fn from(BroadcastStreamRecvError::Lagged(skipped): BroadcastStreamRecvError) -> Self {
        SubscriptionError::Lagged(skipped)
    }
}
