//! Node that connects to Celestia's P2P network.
//!
//! Upon creation, `Node` will try to connect to Celestia's P2P network
//! and then proceed with synchronization and data sampling of the blocks.

use std::ops::RangeBounds;
use std::sync::Arc;
use std::time::Duration;

use libp2p::identity::Keypair;
use libp2p::swarm::NetworkInfo;
use libp2p::{Multiaddr, PeerId};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use blockstore::Blockstore;
use celestia_types::blob::BlobsAtHeight;
use celestia_types::hash::Hash;
use celestia_types::namespace_data::NamespaceData;
use celestia_types::nmt::Namespace;
use celestia_types::row::Row;
use celestia_types::sample::Sample;
use celestia_types::{Blob, ExtendedDataSquare, ExtendedHeader, SharesAtHeight};
use lumina_utils::executor::{JoinHandle, spawn, spawn_cancellable};

use crate::blockstore::InMemoryBlockstore;
use crate::daser::{
    DEFAULT_ADDITIONAL_HEADER_SUB_CONCURENCY, DEFAULT_CONCURENCY_LIMIT, Daser, DaserArgs,
};
use crate::events::{EventChannel, EventSubscriber, NodeEvent};
use crate::node::subscriptions::{SubscriptionError, forward_new_blobs, forward_new_shares};
use crate::p2p::{P2p, P2pArgs};
use crate::pruner::{Pruner, PrunerArgs};
use crate::store::{InMemoryStore, SamplingMetadata, Store, StoreError};
use crate::syncer::{Syncer, SyncerArgs};

mod builder;
pub mod subscriptions;

pub use self::builder::{
    DEFAULT_PRUNING_WINDOW, DEFAULT_PRUNING_WINDOW_IN_MEMORY, NodeBuilder, NodeBuilderError,
    SAMPLING_WINDOW,
};
pub use crate::daser::DaserError;
pub use crate::p2p::{HeaderExError, P2pError};
pub use crate::peer_tracker::PeerTrackerInfo;
pub use crate::syncer::{SyncerError, SyncingInfo};

const DEFAULT_BLOCK_TIME: Duration = Duration::from_secs(6);

/// Alias of [`Result`] with [`NodeError`] error type
///
/// [`Result`]: std::result::Result
pub type Result<T, E = NodeError> = std::result::Result<T, E>;

/// Representation of all the errors that can occur when interacting with the [`Node`].
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    /// An error propagated from the [`NodeBuilder`] component.
    #[error("NodeBuilder: {0}")]
    NodeBuilder(#[from] NodeBuilderError),

    /// An error propagated from the `P2p` component.
    #[error("P2p: {0}")]
    P2p(#[from] P2pError),

    /// An error propagated from the `Syncer` component.
    #[error("Syncer: {0}")]
    Syncer(#[from] SyncerError),

    /// An error propagated from the [`Store`] component.
    #[error("Store: {0}")]
    Store(#[from] StoreError),

    /// An error propagated from the `Daser` component.
    #[error("Daser: {0}")]
    Daser(#[from] DaserError),
}

struct NodeConfig<B, S>
where
    B: Blockstore,
    S: Store,
{
    pub(crate) blockstore: B,
    pub(crate) store: S,
    pub(crate) network_id: String,
    pub(crate) p2p_local_keypair: Keypair,
    pub(crate) p2p_bootnodes: Vec<Multiaddr>,
    pub(crate) p2p_listen_on: Vec<Multiaddr>,
    pub(crate) sync_batch_size: u64,
    pub(crate) sampling_window: Duration,
    pub(crate) pruning_window: Duration,
}

/// Celestia node.
pub struct Node<B, S>
where
    B: Blockstore + 'static,
    S: Store + 'static,
{
    event_channel: EventChannel,
    p2p: Option<Arc<P2p>>,
    blockstore: Option<Arc<B>>,
    store: Option<Arc<S>>,
    syncer: Option<Arc<Syncer<S>>>,
    daser: Option<Arc<Daser>>,
    pruner: Option<Arc<Pruner>>,
    tasks_cancellation_token: CancellationToken,
    network_compromised_task: JoinHandle,
}

impl Node<InMemoryBlockstore, InMemoryStore> {
    /// Creates a new [`NodeBuilder`] that is using in-memory stores.
    ///
    /// After the creation you can use [`NodeBuilder::blockstore`]
    /// and [`NodeBuilder::store`] to set other stores.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use lumina_node::network::Network;
    /// # use lumina_node::Node;
    /// #
    /// # async fn example() {
    /// let node = Node::builder()
    ///     .network(Network::Mainnet)
    ///     .start()
    ///     .await
    ///     .unwrap();
    /// # }
    /// ```
    pub fn builder() -> NodeBuilder<InMemoryBlockstore, InMemoryStore> {
        NodeBuilder::new()
    }
}

impl<B, S> Node<B, S>
where
    B: Blockstore,
    S: Store,
{
    /// Creates and starts a new celestia node with a given config.
    async fn start(config: NodeConfig<B, S>) -> Result<(Self, EventSubscriber)> {
        let event_channel = EventChannel::new();
        let event_sub = event_channel.subscribe();
        let store = Arc::new(config.store);
        let blockstore = Arc::new(config.blockstore);

        let p2p = Arc::new(
            P2p::start(P2pArgs {
                network_id: config.network_id,
                local_keypair: config.p2p_local_keypair,
                bootnodes: config.p2p_bootnodes,
                listen_on: config.p2p_listen_on,
                store: store.clone(),
                event_pub: event_channel.publisher(),
            })
            .await?,
        );

        let syncer = Arc::new(Syncer::start(SyncerArgs {
            store: store.clone(),
            p2p: p2p.clone(),
            event_pub: event_channel.publisher(),
            batch_size: config.sync_batch_size,
            sampling_window: config.sampling_window,
            pruning_window: config.pruning_window,
        })?);

        let daser = Arc::new(Daser::start(DaserArgs {
            p2p: p2p.clone(),
            store: store.clone(),
            blockstore: blockstore.clone(),
            event_pub: event_channel.publisher(),
            sampling_window: config.sampling_window,
            concurrency_limit: DEFAULT_CONCURENCY_LIMIT,
            additional_headersub_concurrency: DEFAULT_ADDITIONAL_HEADER_SUB_CONCURENCY,
        })?);

        let pruner = Arc::new(Pruner::start(PrunerArgs {
            daser: daser.clone(),
            store: store.clone(),
            blockstore: blockstore.clone(),
            event_pub: event_channel.publisher(),
            block_time: DEFAULT_BLOCK_TIME,
            sampling_window: config.sampling_window,
            pruning_window: config.pruning_window,
        }));

        let tasks_cancellation_token = CancellationToken::new();

        // spawn the task that will stop the services when the fraud is detected
        let network_compromised_task = spawn_cancellable(tasks_cancellation_token.child_token(), {
            let network_compromised_token = p2p.get_network_compromised_token().await?;
            let syncer = syncer.clone();
            let daser = daser.clone();
            let pruner = pruner.clone();
            let event_pub = event_channel.publisher();

            async move {
                network_compromised_token.triggered().await;

                // Network compromised! Stop workers.
                syncer.stop();
                daser.stop();
                pruner.stop();

                event_pub.send(NodeEvent::NetworkCompromised);
                // This is a very important message and we want to log it even
                // if user consumes our events.
                warn!("{}", NodeEvent::NetworkCompromised);
            }
        });

        let node = Node {
            event_channel,
            p2p: Some(p2p),
            blockstore: Some(blockstore),
            store: Some(store),
            syncer: Some(syncer),
            daser: Some(daser),
            pruner: Some(pruner),
            tasks_cancellation_token,
            network_compromised_task,
        };

        Ok((node, event_sub))
    }

    /// Stop the node.
    pub async fn stop(mut self) {
        {
            let daser = self.daser.take().expect("Daser not initialized");
            let syncer = self.syncer.take().expect("Syncer not initialized");
            let pruner = self.pruner.take().expect("Pruner not initialized");
            let p2p = self.p2p.take().expect("P2p not initialized");

            // Cancel Node's tasks
            self.tasks_cancellation_token.cancel();
            self.network_compromised_task.join().await;

            // Stop all components that use P2p.
            daser.stop();
            syncer.stop();
            pruner.stop();

            daser.join().await;
            syncer.join().await;
            pruner.join().await;

            // Now stop P2p component.
            p2p.stop();
            p2p.join().await;
        }

        // Everything that was holding Blockstore is now dropped, so we can close it.
        let blockstore = self.blockstore.take().expect("Blockstore not initialized");
        let blockstore = Arc::into_inner(blockstore).expect("Not all Arc<Blockstore> were dropped");
        if let Err(e) = blockstore.close().await {
            warn!("Blockstore failed to close: {e}");
        }

        // Everything that was holding Store is now dropped, so we can close it.
        let store = self.store.take().expect("Store not initialized");
        let store = Arc::into_inner(store).expect("Not all Arc<Store> were dropped");
        if let Err(e) = store.close().await {
            warn!("Store failed to close: {e}");
        }

        self.event_channel.publisher().send(NodeEvent::NodeStopped);
    }

    fn syncer(&self) -> &Syncer<S> {
        self.syncer.as_ref().expect("Syncer not initialized")
    }

    fn p2p(&self) -> &P2p {
        self.p2p.as_ref().expect("P2p not initialized")
    }

    fn store(&self) -> &S {
        self.store.as_ref().expect("Store not initialized")
    }

    /// Returns a new `EventSubscriber`.
    pub fn event_subscriber(&self) -> EventSubscriber {
        self.event_channel.subscribe()
    }

    /// Get node's local peer ID.
    pub fn local_peer_id(&self) -> &PeerId {
        self.p2p().local_peer_id()
    }

    /// Get current [`PeerTrackerInfo`].
    pub fn peer_tracker_info(&self) -> PeerTrackerInfo {
        self.p2p().peer_tracker_info().clone()
    }

    /// Get [`PeerTrackerInfo`] watcher.
    pub fn peer_tracker_info_watcher(&self) -> watch::Receiver<PeerTrackerInfo> {
        self.p2p().peer_tracker_info_watcher()
    }

    /// Wait until the node is connected to at least 1 peer.
    pub async fn wait_connected(&self) -> Result<()> {
        Ok(self.p2p().wait_connected().await?)
    }

    /// Wait until the node is connected to at least 1 trusted peer.
    pub async fn wait_connected_trusted(&self) -> Result<()> {
        Ok(self.p2p().wait_connected_trusted().await?)
    }

    /// Get current network info.
    pub async fn network_info(&self) -> Result<NetworkInfo> {
        Ok(self.p2p().network_info().await?)
    }

    /// Get all the multiaddresses on which the node listens.
    pub async fn listeners(&self) -> Result<Vec<Multiaddr>> {
        Ok(self.p2p().listeners().await?)
    }

    /// Get all the peers that node is connected to.
    pub async fn connected_peers(&self) -> Result<Vec<PeerId>> {
        Ok(self.p2p().connected_peers().await?)
    }

    /// Trust or untrust the peer with a given ID.
    pub async fn set_peer_trust(&self, peer_id: PeerId, is_trusted: bool) -> Result<()> {
        Ok(self.p2p().set_peer_trust(peer_id, is_trusted).await?)
    }

    /// Manually mark the peer as archival.
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn mark_as_archival(&self, peer_id: PeerId) -> Result<()> {
        Ok(self.p2p().mark_as_archival(peer_id).await?)
    }

    /// Request the head header from the network.
    pub async fn request_head_header(&self) -> Result<ExtendedHeader> {
        Ok(self.p2p().get_head_header().await?)
    }

    /// Request a header for the block with a given hash from the network.
    pub async fn request_header_by_hash(&self, hash: &Hash) -> Result<ExtendedHeader> {
        Ok(self.p2p().get_header(*hash).await?)
    }

    /// Request a header for the block with a given height from the network.
    pub async fn request_header_by_height(&self, height: u64) -> Result<ExtendedHeader> {
        Ok(self.p2p().get_header_by_height(height).await?)
    }

    /// Request headers in range (from, from + amount] from the network.
    ///
    /// The headers will be verified with the `from` header.
    pub async fn request_verified_headers(
        &self,
        from: &ExtendedHeader,
        amount: u64,
    ) -> Result<Vec<ExtendedHeader>> {
        Ok(self.p2p().get_verified_headers_range(from, amount).await?)
    }

    /// Request a verified [`Row`] from the network.
    ///
    /// # Errors
    ///
    /// On failure to receive a verified [`Row`] within a certain time, the
    /// `NodeError::P2p(P2pError::RequestTimedOut)` error will be returned.
    pub async fn request_row(
        &self,
        row_index: u16,
        block_height: u64,
        timeout: Option<Duration>,
    ) -> Result<Row> {
        Ok(self.p2p().get_row(row_index, block_height, timeout).await?)
    }

    /// Request a verified [`Sample`] from the network.
    ///
    /// # Errors
    ///
    /// On failure to receive a verified [`Sample`] within a certain time, the
    /// `NodeError::P2p(P2pError::RequestTimedOut)` error will be returned.
    pub async fn request_sample(
        &self,
        row_index: u16,
        column_index: u16,
        block_height: u64,
        timeout: Option<Duration>,
    ) -> Result<Sample> {
        let sample = self
            .p2p()
            .get_sample(row_index, column_index, block_height, timeout)
            .await?;

        Ok(sample)
    }

    /// Request a verified [`ExtendedDataSquare`] from the network.
    ///
    /// # Errors
    ///
    /// On failure to receive a verified [`ExtendedDataSquare`] within a certain time, the
    /// `NodeError::P2p(P2pError::RequestTimedOut)` error will be returned.
    pub async fn request_extended_data_square(
        &self,
        block_height: u64,
        timeout: Option<Duration>,
    ) -> Result<ExtendedDataSquare> {
        Ok(self.p2p().get_eds(block_height, timeout).await?)
    }

    /// Request a verified [`NamespaceData`] from the network.
    ///
    /// # Errors
    ///
    /// On failure to receive a verified [`NamespaceData`] within a certain time, the
    /// `NodeError::P2p(P2pError::RequestTimedOut)` error will be returned.
    pub async fn request_namespace_data(
        &self,
        namespace: Namespace,
        block_height: u64,
        timeout: Option<Duration>,
    ) -> Result<NamespaceData> {
        Ok(self
            .p2p()
            .get_namespace_data(namespace, block_height, timeout)
            .await?)
    }

    /// Request all blobs with provided namespace in the block corresponding to this header
    /// using bitswap protocol.
    pub async fn request_all_blobs(
        &self,
        namespace: Namespace,
        block_height: u64,
        timeout: Option<Duration>,
    ) -> Result<Vec<Blob>> {
        Ok(self
            .p2p()
            .get_all_blobs(namespace, block_height, timeout, self.store())
            .await?)
    }

    /// Get current header syncing info.
    pub async fn syncer_info(&self) -> Result<SyncingInfo> {
        Ok(self.syncer().info().await?)
    }

    /// Get the latest header announced in the network.
    pub async fn get_network_head_header(&self) -> Result<Option<ExtendedHeader>> {
        Ok(self.p2p().get_network_head().await?)
    }

    /// Get the latest locally synced header.
    pub async fn get_local_head_header(&self) -> Result<ExtendedHeader> {
        Ok(self.store().get_head().await?)
    }

    /// Get a synced header for the block with a given hash.
    pub async fn get_header_by_hash(&self, hash: &Hash) -> Result<ExtendedHeader> {
        Ok(self.store().get_by_hash(hash).await?)
    }

    /// Get a synced header for the block with a given height.
    pub async fn get_header_by_height(&self, height: u64) -> Result<ExtendedHeader> {
        Ok(self.store().get_by_height(height).await?)
    }

    /// Get synced headers from the given heights range.
    ///
    /// If start of the range is unbounded, the first returned header will be of height 1.
    /// If end of the range is unbounded, the last returned header will be the last header in the
    /// store.
    ///
    /// # Errors
    ///
    /// If range contains a height of a header that is not found in the store or [`RangeBounds`]
    /// cannot be converted to a valid range.
    pub async fn get_headers<R>(&self, range: R) -> Result<Vec<ExtendedHeader>>
    where
        R: RangeBounds<u64> + Send,
    {
        Ok(self.store().get_range(range).await?)
    }

    /// Get data sampling metadata of an already sampled height.
    ///
    /// Returns `Ok(None)` if metadata for the given height does not exists.
    pub async fn get_sampling_metadata(&self, height: u64) -> Result<Option<SamplingMetadata>> {
        match self.store().get_sampling_metadata(height).await {
            Ok(val) => Ok(val),
            Err(StoreError::NotFound) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Subscribe to new headers received by the node from the network.
    ///
    /// Return a stream which will yield all the headers, as they are being received by the
    /// node, starting from the first header received after the call.
    pub async fn header_subscribe(&self) -> Result<broadcast::Receiver<ExtendedHeader>> {
        Ok(self.syncer().subscribe_headers().await?)
    }

    /// Subscribe to the shares from the namespace, as new headers are received by the node
    ///
    /// Return a stream which will yield all the blobs from the namespace, as the new headers
    /// are being received by the node, starting from the first header received after the call.
    pub async fn blob_subscribe(
        &self,
        namespace: Namespace,
    ) -> Result<ReceiverStream<Result<BlobsAtHeight, SubscriptionError>>> {
        let header_receiver = self.header_subscribe().await?;
        let p2p = self.p2p.as_ref().cloned().expect("p2p should be present");

        // We're keeping a small buffer of blobs, so that new headers are cached eagerly
        // as they come in, giving consumer more time before they are no longer available.
        // This is mostly relevant for cases where pruning window is set to zero.
        let (tx, rx) = mpsc::channel(16);

        spawn(async move { forward_new_blobs(namespace, tx, header_receiver, p2p).await });

        Ok(ReceiverStream::new(rx))
    }

    /// Subscribe to the blobs from the namespace, as new headers are received by the node
    ///
    /// Return a stream which will yield all the shares from the namespace, as the new headers
    /// are being received by the node, starting from the first header received after the call.
    pub async fn namespace_subscribe(
        &self,
        namespace: Namespace,
    ) -> Result<ReceiverStream<Result<SharesAtHeight, SubscriptionError>>> {
        let header_receiver = self.header_subscribe().await?;
        let p2p = self.p2p.as_ref().cloned().expect("p2p should be present");

        // We're keeping a small buffer of shares, so that new headers are cached eagerly
        // as they come in, giving consumer more time before they are no longer available.
        // This is mostly relevant for cases where pruning window is set to zero.
        let (tx, rx) = mpsc::channel(16);

        spawn(async move { forward_new_shares(namespace, tx, header_receiver, p2p).await });

        Ok(ReceiverStream::new(rx))
    }
}

impl<B, S> Drop for Node<B, S>
where
    B: Blockstore,
    S: Store,
{
    fn drop(&mut self) {
        // Stop everything, but don't join them.
        self.tasks_cancellation_token.cancel();

        if let Some(daser) = self.daser.take() {
            daser.stop();
        }

        if let Some(syncer) = self.syncer.take() {
            syncer.stop();
        }

        if let Some(pruner) = self.pruner.take() {
            pruner.stop();
        }

        if let Some(p2p) = self.p2p.take() {
            p2p.stop();
        }
    }
}
