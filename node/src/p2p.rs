//! Component responsible for the messaging and interacting with other nodes on the peer to peer layer.
//!
//! It is a high level integration of various p2p protocols used by Celestia nodes.
//! Currently supporting:
//! - libp2p-identify
//! - libp2p-kad
//! - libp2p-autonat
//! - libp2p-ping
//! - header-sub topic on libp2p-gossipsub
//! - fraud-sub topic on libp2p-gossipsub
//! - header-ex client
//! - header-ex server
//! - bitswap 1.2.0
//! - shwap - celestia's data availability protocol on top of bitswap

use std::sync::Arc;
use std::time::Duration;

use blockstore::block::CidError;
use celestia_proto::p2p::pb::{HeaderRequest, header_request};
use celestia_types::fraud_proof::BadEncodingFraudProof;
use celestia_types::hash::Hash;
use celestia_types::namespace_data::NamespaceData;
use celestia_types::nmt::Namespace;
use celestia_types::row::Row;
use celestia_types::sample::Sample;
use celestia_types::{Blob, ExtendedDataSquare, ExtendedHeader, FraudProof};
use libp2p::gossipsub::TopicHash;
use libp2p::identity::Keypair;
use libp2p::swarm::{NetworkBehaviour, NetworkInfo};
use libp2p::{Multiaddr, PeerId, gossipsub};
use lumina_utils::executor::{JoinHandle, spawn};
use lumina_utils::time::{self, Interval};
use lumina_utils::token::Token;
use tendermint_proto::Protobuf;
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, trace, warn};

mod connection_control;
mod header_ex;
pub(crate) mod header_session;
mod shrex;
mod swarm;
mod swarm_manager;
mod utils;

use crate::block_ranges::BlockRange;
use crate::events::EventPublisher;
use crate::p2p::header_session::HeaderSession;
use crate::p2p::swarm_manager::SwarmManager;
use crate::peer_tracker::PeerTracker;
use crate::peer_tracker::PeerTrackerInfo;
use crate::store::{Store, StoreError};
use crate::utils::{
    MultiaddrExt, OneshotResultSender, OneshotSenderExt, fraudsub_ident_topic,
    gossipsub_ident_topic,
};

pub use crate::p2p::header_ex::HeaderExError;
pub use crate::p2p::shrex::ShrExError;

// Maximum size of a [`Multihash`].
pub(crate) const MAX_MH_SIZE: usize = 64;

// all fraud proofs for height bigger than head height by this threshold
// will be ignored
const FRAUD_PROOF_HEAD_HEIGHT_THRESHOLD: u64 = 20;

pub(crate) type Result<T, E = P2pError> = std::result::Result<T, E>;

/// Representation of all the errors that can occur in `P2p` component.
#[derive(Debug, thiserror::Error)]
pub enum P2pError {
    /// Failed to initialize gossipsub behaviour.
    #[error("Failed to initialize gossipsub behaviour: {0}")]
    GossipsubInit(String),

    /// Failed to initialize TLS.
    #[error("Failed to initialize TLS: {0}")]
    TlsInit(String),

    /// Failed to initialize noise protocol.
    #[error("Failed to initialize noise: {0}")]
    NoiseInit(String),

    /// The worker has died.
    #[error("Worker died")]
    WorkerDied,

    /// Channel closed unexpectedly.
    #[error("Channel closed unexpectedly")]
    ChannelClosedUnexpectedly,

    /// An error propagated from the `header-ex`.
    #[error("HeaderEx: {0}")]
    HeaderEx(#[from] HeaderExError),

    /// An error propagated from the `shr-ex`.
    #[error("ShrEx: {0}")]
    ShrEx(#[from] ShrExError),

    /// Bootnode address is missing its peer ID.
    #[error("Bootnode multiaddrs without peer ID: {0:?}")]
    BootnodeAddrsWithoutPeerId(Vec<Multiaddr>),

    /// Protobuf message failed to be decoded.
    #[error("ProtoBuf decoding error: {0}")]
    ProtoDecodeFailed(#[from] tendermint_proto::Error),

    /// An error propagated from [`celestia_types`] that is related to [`Cid`].
    #[error("CID error: {0}")]
    Cid(CidError),

    /// Request timed out.
    #[error("Request timed out")]
    RequestTimedOut,

    /// Shwap protocol error.
    #[error("Shwap: {0}")]
    Shwap(String),

    /// An error propagated from [`celestia_types`].
    #[error(transparent)]
    CelestiaTypes(#[from] celestia_types::Error),

    /// An error propagated from [`Store`].
    #[error("Store error: {0}")]
    Store(#[from] StoreError),

    /// Header was pruned.
    #[error("Header of {0} block was pruned because it is outside of retention period")]
    HeaderPruned(u64),

    /// Header not synced yet.
    #[error("Header of {0} block is not synced yet")]
    HeaderNotSynced(u64),
}

impl P2pError {
    /// Returns `true` if an error is fatal in all possible scenarios.
    ///
    /// If unsure mark it as non-fatal error.
    pub(crate) fn is_fatal(&self) -> bool {
        match self {
            P2pError::GossipsubInit(_)
            | P2pError::NoiseInit(_)
            | P2pError::TlsInit(_)
            | P2pError::WorkerDied
            | P2pError::ChannelClosedUnexpectedly
            | P2pError::BootnodeAddrsWithoutPeerId(_) => true,
            P2pError::HeaderEx(_)
            | P2pError::ShrEx(_)
            | P2pError::ProtoDecodeFailed(_)
            | P2pError::Cid(_)
            | P2pError::RequestTimedOut
            | P2pError::Shwap(_)
            | P2pError::CelestiaTypes(_)
            | P2pError::HeaderPruned(_)
            | P2pError::HeaderNotSynced(_) => false,
            P2pError::Store(e) => e.is_fatal(),
        }
    }
}

impl From<oneshot::error::RecvError> for P2pError {
    fn from(_value: oneshot::error::RecvError) -> Self {
        P2pError::ChannelClosedUnexpectedly
    }
}

impl From<prost::DecodeError> for P2pError {
    fn from(value: prost::DecodeError) -> Self {
        P2pError::ProtoDecodeFailed(tendermint_proto::Error::decode_message(value))
    }
}

impl From<cid::Error> for P2pError {
    fn from(value: cid::Error) -> Self {
        P2pError::Cid(CidError::InvalidCid(value.to_string()))
    }
}

/// Component responsible for the peer to peer networking handling.
#[derive(Debug)]
pub(crate) struct P2p {
    cancellation_token: CancellationToken,
    cmd_tx: mpsc::Sender<P2pCmd>,
    join_handle: JoinHandle,
    peer_tracker_info_watcher: watch::Receiver<PeerTrackerInfo>,
    local_peer_id: PeerId,
}

/// Arguments used to configure the [`P2p`].
pub struct P2pArgs<S>
where
    S: Store,
{
    /// An id of the network to connect to.
    pub network_id: String,
    /// The keypair to be used as the identity.
    pub local_keypair: Keypair,
    /// List of bootstrap nodes to connect to and trust.
    pub bootnodes: Vec<Multiaddr>,
    /// List of the addresses on which to listen for incoming connections.
    pub listen_on: Vec<Multiaddr>,
    /// The store for headers.
    pub store: Arc<S>,
    /// Event publisher.
    pub event_pub: EventPublisher,
}

#[derive(Debug)]
pub(crate) enum P2pCmd {
    NetworkInfo {
        respond_to: oneshot::Sender<NetworkInfo>,
    },
    HeaderExRequest {
        request: HeaderRequest,
        respond_to: OneshotResultSender<Vec<ExtendedHeader>, P2pError>,
    },
    Listeners {
        respond_to: oneshot::Sender<Vec<Multiaddr>>,
    },
    ConnectedPeers {
        respond_to: oneshot::Sender<Vec<PeerId>>,
    },
    InitHeaderSub {
        head: Box<ExtendedHeader>,
        /// Any valid headers received by header-sub will be send to this channel.
        channel: mpsc::Sender<ExtendedHeader>,
    },
    SetPeerTrust {
        peer_id: PeerId,
        is_trusted: bool,
    },
    #[cfg(any(test, feature = "test-utils"))]
    MarkAsArchival {
        peer_id: PeerId,
    },
    GetNetworkCompromisedToken {
        respond_to: oneshot::Sender<Token>,
    },
    GetNetworkHead {
        respond_to: oneshot::Sender<Option<ExtendedHeader>>,
    },
    // This is dead code because `get_row` still uses Bitswap.
    // We can use this when celestia-node#4288 is merged.
    GetRow {
        row_index: u16,
        block_height: u64,
        respond_to: OneshotResultSender<Row, P2pError>,
    },
    // This is dead code because `get_sample` still uses Bitswap.
    // We can use this when celestia-node#4288 is merged.
    GetSample {
        row_index: u16,
        column_index: u16,
        block_height: u64,
        respond_to: OneshotResultSender<Sample, P2pError>,
    },
    GetNamespaceData {
        namespace: Namespace,
        block_height: u64,
        respond_to: OneshotResultSender<NamespaceData, P2pError>,
    },
    GetEds {
        block_height: u64,
        respond_to: OneshotResultSender<ExtendedDataSquare, P2pError>,
    },
}

impl P2p {
    /// Creates and starts a new p2p handler.
    pub async fn start<S>(args: P2pArgs<S>) -> Result<Self>
    where
        S: Store + 'static,
    {
        validate_bootnode_addrs(&args.bootnodes)?;

        let local_peer_id = PeerId::from(args.local_keypair.public());

        let peer_tracker = PeerTracker::new(args.event_pub.clone());
        let peer_tracker_info_watcher = peer_tracker.info_watcher();

        let cancellation_token = CancellationToken::new();
        let (cmd_tx, cmd_rx) = mpsc::channel(16);

        let mut worker =
            Worker::new(args, cancellation_token.child_token(), cmd_rx, peer_tracker).await?;

        let join_handle = spawn(async move {
            worker.run().await;
        });

        Ok(P2p {
            cancellation_token,
            cmd_tx,
            join_handle,
            peer_tracker_info_watcher,
            local_peer_id,
        })
    }

    /// Creates and starts a new mocked p2p handler.
    #[cfg(test)]
    pub fn mocked() -> (Self, crate::test_utils::MockP2pHandle) {
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let (peer_tracker_tx, peer_tracker_rx) = watch::channel(PeerTrackerInfo::default());
        let cancellation_token = CancellationToken::new();

        // Just a fake join_handle
        let join_handle = spawn(async {});

        let p2p = P2p {
            cmd_tx: cmd_tx.clone(),
            cancellation_token,
            join_handle,
            peer_tracker_info_watcher: peer_tracker_rx,
            local_peer_id: PeerId::random(),
        };

        let handle = crate::test_utils::MockP2pHandle {
            cmd_tx,
            cmd_rx,
            header_sub_tx: None,
            peer_tracker_tx,
        };

        (p2p, handle)
    }

    /// Stop the worker.
    pub fn stop(&self) {
        // Signal the Worker to stop.
        self.cancellation_token.cancel();
    }

    /// Wait until worker is completely stopped.
    pub async fn join(&self) {
        self.join_handle.join().await;
    }

    /// Local peer ID on the p2p network.
    pub fn local_peer_id(&self) -> &PeerId {
        &self.local_peer_id
    }

    async fn send_command(&self, cmd: P2pCmd) -> Result<()> {
        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| P2pError::WorkerDied)
    }

    /// Watcher for the current [`PeerTrackerInfo`].
    pub fn peer_tracker_info_watcher(&self) -> watch::Receiver<PeerTrackerInfo> {
        self.peer_tracker_info_watcher.clone()
    }

    /// A reference to the current [`PeerTrackerInfo`].
    pub fn peer_tracker_info(&self) -> watch::Ref<'_, PeerTrackerInfo> {
        self.peer_tracker_info_watcher.borrow()
    }

    /// Initializes `header-sub` protocol with a given `subjective_head`.
    pub async fn init_header_sub(
        &self,
        head: ExtendedHeader,
        channel: mpsc::Sender<ExtendedHeader>,
    ) -> Result<()> {
        self.send_command(P2pCmd::InitHeaderSub {
            head: Box::new(head),
            channel,
        })
        .await
    }

    /// Wait until the node is connected to any peer.
    pub async fn wait_connected(&self) -> Result<()> {
        self.peer_tracker_info_watcher()
            .wait_for(|info| info.num_connected_peers > 0)
            .await
            .map(drop)
            .map_err(|_| P2pError::WorkerDied)
    }

    /// Wait until the node is connected to any trusted peer.
    pub async fn wait_connected_trusted(&self) -> Result<()> {
        self.peer_tracker_info_watcher()
            .wait_for(|info| info.num_connected_trusted_peers > 0)
            .await
            .map(drop)
            .map_err(|_| P2pError::WorkerDied)
    }

    /// Get current [`NetworkInfo`].
    pub async fn network_info(&self) -> Result<NetworkInfo> {
        let (tx, rx) = oneshot::channel();

        self.send_command(P2pCmd::NetworkInfo { respond_to: tx })
            .await?;

        Ok(rx.await?)
    }

    /// Send a request on the `header-ex` protocol.
    pub async fn header_ex_request(&self, request: HeaderRequest) -> Result<Vec<ExtendedHeader>> {
        let (tx, rx) = oneshot::channel();

        self.send_command(P2pCmd::HeaderExRequest {
            request,
            respond_to: tx,
        })
        .await?;

        rx.await?
    }

    /// Request the head header on the `header-ex` protocol.
    pub async fn get_head_header(&self) -> Result<ExtendedHeader> {
        self.get_header_by_height(0).await
    }

    /// Request the header by hash on the `header-ex` protocol.
    pub async fn get_header(&self, hash: Hash) -> Result<ExtendedHeader> {
        self.header_ex_request(HeaderRequest {
            data: Some(header_request::Data::Hash(hash.as_bytes().to_vec())),
            amount: 1,
        })
        .await?
        .into_iter()
        .next()
        .ok_or(HeaderExError::HeaderNotFound.into())
    }

    /// Request the header by height on the `header-ex` protocol.
    pub async fn get_header_by_height(&self, height: u64) -> Result<ExtendedHeader> {
        self.header_ex_request(HeaderRequest {
            data: Some(header_request::Data::Origin(height)),
            amount: 1,
        })
        .await?
        .into_iter()
        .next()
        .ok_or(HeaderExError::HeaderNotFound.into())
    }

    /// Request the headers following the one given with the `header-ex` protocol.
    ///
    /// First header from the requested range will be verified against the provided one,
    /// then each subsequent is verified against the previous one.
    pub async fn get_verified_headers_range(
        &self,
        from: &ExtendedHeader,
        amount: u64,
    ) -> Result<Vec<ExtendedHeader>> {
        // User can give us a bad header, so validate it.
        from.validate().map_err(|_| HeaderExError::InvalidRequest)?;

        let height = from.height() + 1;

        let range = height..=height + amount - 1;

        let mut session = HeaderSession::new(range, self.cmd_tx.clone());
        let headers = session.run().await?;

        // `.validate()` is called on each header separately by `HeaderExClientHandler`.
        //
        // The last step is to verify that all headers are from the same chain
        // and indeed connected with the next one.
        from.verify_adjacent_range(&headers)
            .map_err(|_| HeaderExError::InvalidResponse)?;

        Ok(headers)
    }

    /// Request a list of ranges with the `header-ex` protocol
    ///
    /// For each of the ranges, headers are verified against each other, but it's the caller
    /// responsibility to verify range edges against headers existing in the store.
    pub(crate) async fn get_unverified_header_range(
        &self,
        range: BlockRange,
    ) -> Result<Vec<ExtendedHeader>> {
        if range.is_empty() {
            return Err(HeaderExError::InvalidRequest.into());
        }

        let mut session = HeaderSession::new(range, self.cmd_tx.clone());
        let headers = session.run().await?;

        let Some(head) = headers.first() else {
            return Err(HeaderExError::InvalidResponse.into());
        };

        head.verify_adjacent_range(&headers[1..])
            .map_err(|_| HeaderExError::InvalidResponse)?;

        Ok(headers)
    }

    /// Request a [`Row`] on bitswap protocol.
    pub async fn get_row(
        &self,
        row_index: u16,
        block_height: u64,
        timeout: Option<Duration>,
    ) -> Result<Row> {
        let (tx, rx) = oneshot::channel();

        self.send_command(P2pCmd::GetRow {
            row_index,
            block_height,
            respond_to: tx,
        })
        .await?;

        match timeout {
            Some(dur) => time::timeout(dur, rx)
                .await
                .map_err(|_| P2pError::RequestTimedOut)??,
            None => rx.await?,
        }
    }

    /// Request a [`Sample`] on bitswap protocol.
    pub async fn get_sample(
        &self,
        row_index: u16,
        column_index: u16,
        block_height: u64,
        timeout: Option<Duration>,
    ) -> Result<Sample> {
        let (tx, rx) = oneshot::channel();

        self.send_command(P2pCmd::GetSample {
            row_index,
            column_index,
            block_height,
            respond_to: tx,
        })
        .await?;

        match timeout {
            Some(dur) => time::timeout(dur, rx)
                .await
                .map_err(|_| P2pError::RequestTimedOut)??,
            None => rx.await?,
        }
    }

    pub async fn get_eds(
        &self,
        block_height: u64,
        timeout: Option<Duration>,
    ) -> Result<ExtendedDataSquare> {
        let (tx, rx) = oneshot::channel();

        self.send_command(P2pCmd::GetEds {
            block_height,
            respond_to: tx,
        })
        .await?;

        match timeout {
            Some(dur) => time::timeout(dur, rx)
                .await
                .map_err(|_| P2pError::RequestTimedOut)??,
            None => rx.await?,
        }
    }

    pub async fn get_namespace_data(
        &self,
        namespace: Namespace,
        block_height: u64,
        timeout: Option<Duration>,
    ) -> Result<NamespaceData> {
        let (tx, rx) = oneshot::channel();

        self.send_command(P2pCmd::GetNamespaceData {
            namespace,
            block_height,
            respond_to: tx,
        })
        .await?;

        match timeout {
            Some(dur) => time::timeout(dur, rx)
                .await
                .map_err(|_| P2pError::RequestTimedOut)??,
            None => rx.await?,
        }
    }

    /// Request all blobs with provided namespace in the block corresponding to this header
    /// using bitswap protocol.
    pub async fn get_all_blobs<S>(
        &self,
        namespace: Namespace,
        block_height: u64,
        timeout: Option<Duration>,
        store: &S,
    ) -> Result<Vec<Blob>>
    where
        S: Store,
    {
        // TODO: Figure out app_version based on network id and height instead
        // of retrieving the header.
        let app_version = match store.get_by_height(block_height).await {
            Ok(header) => header.app_version(),
            Err(StoreError::NotFound) => {
                let pruned_ranges = store.get_pruned_ranges().await?;

                if pruned_ranges.contains(block_height) {
                    return Err(P2pError::HeaderPruned(block_height));
                } else {
                    return Err(P2pError::HeaderNotSynced(block_height));
                }
            }
            Err(e) => return Err(e.into()),
        };

        let namespace_data = self
            .get_namespace_data(namespace, block_height, timeout)
            .await?;

        let shares = namespace_data
            .rows()
            .iter()
            .flat_map(|row| row.shares.iter());

        Ok(Blob::reconstruct_all(shares, app_version)?)
    }

    /// Get the addresses where [`P2p`] listens on for incoming connections.
    pub async fn listeners(&self) -> Result<Vec<Multiaddr>> {
        let (tx, rx) = oneshot::channel();

        self.send_command(P2pCmd::Listeners { respond_to: tx })
            .await?;

        Ok(rx.await?)
    }

    /// Get the list of connected peers.
    pub async fn connected_peers(&self) -> Result<Vec<PeerId>> {
        let (tx, rx) = oneshot::channel();

        self.send_command(P2pCmd::ConnectedPeers { respond_to: tx })
            .await?;

        Ok(rx.await?)
    }

    /// Alter the trust status for a given peer.
    pub async fn set_peer_trust(&self, peer_id: PeerId, is_trusted: bool) -> Result<()> {
        self.send_command(P2pCmd::SetPeerTrust {
            peer_id,
            is_trusted,
        })
        .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) async fn mark_as_archival(&self, peer_id: PeerId) -> Result<()> {
        self.send_command(P2pCmd::MarkAsArchival { peer_id }).await
    }

    /// Get the cancellation token which will be cancelled when the network gets compromised.
    ///
    /// After this token is cancelled, the network should be treated as insincere
    /// and should not be trusted.
    pub(crate) async fn get_network_compromised_token(&self) -> Result<Token> {
        let (tx, rx) = oneshot::channel();

        self.send_command(P2pCmd::GetNetworkCompromisedToken { respond_to: tx })
            .await?;

        Ok(rx.await?)
    }

    /// Get the latest header announced on the network.
    pub async fn get_network_head(&self) -> Result<Option<ExtendedHeader>> {
        let (tx, rx) = oneshot::channel();

        self.send_command(P2pCmd::GetNetworkHead { respond_to: tx })
            .await?;

        Ok(rx.await?)
    }
}

impl Drop for P2p {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(NetworkBehaviour)]
struct Behaviour<S>
where
    S: Store + 'static,
{
    header_ex: header_ex::Behaviour<S>,
    shr_ex: shrex::Behaviour<S>,
    gossipsub: gossipsub::Behaviour,
}

struct Worker<S>
where
    S: Store + 'static,
{
    cancellation_token: CancellationToken,
    swarm: SwarmManager<Behaviour<S>>,
    header_sub_topic_hash: TopicHash,
    bad_encoding_fraud_sub_topic: TopicHash,
    cmd_rx: mpsc::Receiver<P2pCmd>,
    header_sub_state: Option<HeaderSubState>,
    network_compromised_token: Token,
    store: Arc<S>,
}

struct HeaderSubState {
    known_head: ExtendedHeader,
    channel: mpsc::Sender<ExtendedHeader>,
}

impl<S> Worker<S>
where
    S: Store,
{
    async fn new(
        args: P2pArgs<S>,
        cancellation_token: CancellationToken,
        cmd_rx: mpsc::Receiver<P2pCmd>,
        peer_tracker: PeerTracker,
    ) -> Result<Self, P2pError> {
        let mut swarm = SwarmManager::new(
            &args.network_id,
            &args.local_keypair,
            &args.bootnodes,
            &args.listen_on,
            peer_tracker,
            args.event_pub.clone(),
        )
        .await?;

        let header_sub_topic = gossipsub_ident_topic(&args.network_id, "/header-sub/v0.0.1");
        let bad_encoding_fraud_sub_topic =
            fraudsub_ident_topic(BadEncodingFraudProof::TYPE, &args.network_id);
        let gossipsub = init_gossipsub(&args, [&header_sub_topic, &bad_encoding_fraud_sub_topic])?;

        let header_ex = header_ex::Behaviour::new(header_ex::Config {
            network_id: &args.network_id,
            header_store: args.store.clone(),
        });

        let shr_ex = shrex::Behaviour::new(shrex::Config {
            network_id: &args.network_id,
            local_keypair: &args.local_keypair,
            header_store: args.store.clone(),
            stream_ctrl: swarm.stream_control(),
        })?;

        swarm.attach_behaviour(Behaviour {
            gossipsub,
            header_ex,
            shr_ex,
        });

        Ok(Worker {
            cancellation_token,
            swarm,
            cmd_rx,
            bad_encoding_fraud_sub_topic: bad_encoding_fraud_sub_topic.hash(),
            header_sub_topic_hash: header_sub_topic.hash(),
            header_sub_state: None,
            network_compromised_token: Token::new(),
            store: args.store,
        })
    }

    async fn run(&mut self) {
        let mut report_interval = Interval::new(Duration::from_secs(60));

        loop {
            select! {
                _ = self.cancellation_token.cancelled() => break,
                _ = report_interval.tick() => {
                    self.report();
                }
                res = self.swarm.poll() => {
                    match res {
                        Ok(ev) => {
                            if let Err(e) = self.on_behaviour_event(ev).await {
                                warn!("Failure while handling behaviour event: {e}");
                            }
                        }
                        Err(e) => warn!("Failure while polling SwarmManager: {e}"),
                    }
                }
                Some(cmd) = self.cmd_rx.recv() => {
                    if let Err(e) = self.on_cmd(cmd).await {
                        warn!("Failure while handling command. (error: {e})");
                    }
                }
            }
        }

        self.swarm.context().behaviour.header_ex.stop();
        self.swarm.context().behaviour.shr_ex.stop();
        self.swarm.stop().await;
    }

    async fn on_behaviour_event(&mut self, ev: BehaviourEvent<S>) -> Result<()> {
        match ev {
            BehaviourEvent::Gossipsub(ev) => self.on_gossip_sub_event(ev).await,
            BehaviourEvent::HeaderEx(ev) => self.on_header_ex_event(ev).await,
            BehaviourEvent::ShrEx(ev) => self.on_shrex_event(ev).await,
        }

        Ok(())
    }

    async fn on_cmd(&mut self, cmd: P2pCmd) -> Result<()> {
        match cmd {
            P2pCmd::NetworkInfo { respond_to } => {
                respond_to.maybe_send(self.swarm.network_info());
            }
            P2pCmd::HeaderExRequest {
                request,
                respond_to,
            } => {
                self.swarm
                    .context()
                    .behaviour
                    .header_ex
                    .send_request(request, respond_to);
            }
            P2pCmd::Listeners { respond_to } => {
                respond_to.maybe_send(self.swarm.listeners());
            }
            P2pCmd::ConnectedPeers { respond_to } => {
                let peers = self
                    .swarm
                    .context()
                    .peer_tracker
                    .peers()
                    .filter_map(|peer| {
                        if peer.is_connected() {
                            Some(*peer.id())
                        } else {
                            None
                        }
                    })
                    .collect();
                respond_to.maybe_send(peers);
            }
            P2pCmd::InitHeaderSub { head, channel } => {
                self.on_init_header_sub(*head, channel);
            }
            P2pCmd::SetPeerTrust {
                peer_id,
                is_trusted,
            } => {
                self.swarm.set_peer_trust(&peer_id, is_trusted);
            }
            #[cfg(any(test, feature = "test-utils"))]
            P2pCmd::MarkAsArchival { peer_id } => {
                self.swarm.mark_as_archival(&peer_id);
            }
            P2pCmd::GetNetworkCompromisedToken { respond_to } => {
                respond_to.maybe_send(self.network_compromised_token.clone())
            }
            P2pCmd::GetNetworkHead { respond_to } => {
                let head = self
                    .header_sub_state
                    .as_ref()
                    .map(|state| state.known_head.clone());
                respond_to.maybe_send(head);
            }
            P2pCmd::GetRow {
                row_index,
                block_height,
                respond_to,
            } => {
                self.swarm
                    .context()
                    .behaviour
                    .shr_ex
                    .get_row(block_height, row_index, respond_to)
                    .await
            }
            P2pCmd::GetSample {
                row_index,
                column_index,
                block_height,
                respond_to,
            } => {
                self.swarm
                    .context()
                    .behaviour
                    .shr_ex
                    .get_sample(block_height, row_index, column_index, respond_to)
                    .await
            }
            P2pCmd::GetNamespaceData {
                namespace,
                block_height,
                respond_to,
            } => {
                self.swarm
                    .context()
                    .behaviour
                    .shr_ex
                    .get_namespace_data(block_height, namespace, respond_to)
                    .await
            }
            P2pCmd::GetEds {
                block_height,
                respond_to,
            } => {
                self.swarm
                    .context()
                    .behaviour
                    .shr_ex
                    .get_eds(block_height, respond_to)
                    .await
            }
        }

        Ok(())
    }

    #[instrument(skip_all)]
    fn report(&mut self) {
        let tracker_info = self.swarm.context().peer_tracker.info();

        info!(
            "peers: {}, trusted peers: {}",
            tracker_info.num_connected_peers, tracker_info.num_connected_trusted_peers,
        );
    }

    #[instrument(level = "trace", skip(self))]
    async fn on_gossip_sub_event(&mut self, ev: gossipsub::Event) {
        match ev {
            gossipsub::Event::Message {
                message,
                message_id,
                ..
            } => {
                let Some(peer) = message.source else {
                    // Validation mode is `strict` so this will never happen
                    return;
                };

                let acceptance = if message.topic == self.header_sub_topic_hash {
                    self.on_header_sub_message(&message.data[..])
                } else if message.topic == self.bad_encoding_fraud_sub_topic {
                    self.on_bad_encoding_fraud_sub_message(&message.data[..], &peer)
                        .await
                } else {
                    trace!("Unhandled gossipsub message");
                    gossipsub::MessageAcceptance::Ignore
                };

                if !matches!(acceptance, gossipsub::MessageAcceptance::Reject) {
                    // We may have discovered a new peer
                    self.swarm.peer_maybe_discovered(&peer);
                }

                let _ = self
                    .swarm
                    .context()
                    .behaviour
                    .gossipsub
                    .report_message_validation_result(&message_id, &peer, acceptance);
            }
            _ => trace!("Unhandled gossipsub event"),
        }
    }

    #[instrument(level = "trace", skip(self))]
    async fn on_header_ex_event(&mut self, ev: header_ex::Event) {
        match ev {
            header_ex::Event::SchedulePendingRequests => {
                let ctx = self.swarm.context();

                ctx.behaviour
                    .header_ex
                    .schedule_pending_requests(ctx.peer_tracker);
            }
            header_ex::Event::NeedTrustedPeers => {
                self.swarm.connect_to_bootnodes();
            }
            header_ex::Event::NeedArchivalPeers => {
                self.swarm.start_archival_node_kad_query();
            }
        }
    }

    #[instrument(level = "trace", skip(self))]
    async fn on_shrex_event(&mut self, ev: shrex::Event) {
        match ev {
            shrex::Event::SchedulePendingRequests => {
                let ctx = self.swarm.context();

                ctx.behaviour
                    .shr_ex
                    .schedule_pending_requests(ctx.peer_tracker);
            }

            shrex::Event::AddPeers(peers) => {
                let added = peers
                    .iter()
                    .filter(|peer| self.swarm.peer_maybe_discovered(peer))
                    .count();

                debug!("Added {added} peers discovered through shrex");
            }

            shrex::Event::BlockPeers(peers) => {
                let blocked = peers
                    .into_iter()
                    .filter(|peer| self.swarm.blacklist_peer(peer))
                    .count();

                debug!("Blocked {blocked} peers for shrex missbehaviour");
            }
        }
    }

    #[instrument(skip_all, fields(header = %head))]
    fn on_init_header_sub(&mut self, head: ExtendedHeader, channel: mpsc::Sender<ExtendedHeader>) {
        self.header_sub_state = Some(HeaderSubState {
            known_head: head,
            channel,
        });
        trace!("HeaderSub initialized");
    }

    #[instrument(skip_all)]
    fn on_header_sub_message(&mut self, data: &[u8]) -> gossipsub::MessageAcceptance {
        let Ok(header) = ExtendedHeader::decode_and_validate(data) else {
            trace!("Malformed or invalid header from header-sub");
            return gossipsub::MessageAcceptance::Reject;
        };

        trace!("Received header from header-sub ({header})");

        let Some(ref mut state) = self.header_sub_state else {
            debug!("header-sub not initialized yet");
            return gossipsub::MessageAcceptance::Ignore;
        };

        if state.known_head.verify(&header).is_err() {
            trace!("Failed to verify HeaderSub header. Ignoring {header}");
            return gossipsub::MessageAcceptance::Ignore;
        }

        trace!("New header from header-sub ({header})");

        state.known_head = header.clone();
        // We intentionally do not `send().await` to avoid blocking `P2p`
        // in case `Syncer` enters some weird state.
        let _ = state.channel.try_send(header);

        gossipsub::MessageAcceptance::Accept
    }

    #[instrument(skip_all)]
    async fn on_bad_encoding_fraud_sub_message(
        &mut self,
        data: &[u8],
        peer: &PeerId,
    ) -> gossipsub::MessageAcceptance {
        let Ok(befp) = BadEncodingFraudProof::decode(data) else {
            trace!("Malformed bad encoding fraud proof from {peer}");
            self.swarm
                .context()
                .behaviour
                .gossipsub
                .blacklist_peer(peer);
            return gossipsub::MessageAcceptance::Reject;
        };

        let height = befp.height();

        let current_height = if let Some(ref header_sub_state) = self.header_sub_state {
            header_sub_state.known_head.height()
        } else if let Ok(local_head) = self.store.get_head().await {
            local_head.height()
        } else {
            // we aren't tracking the network and have uninitialized store
            return gossipsub::MessageAcceptance::Ignore;
        };

        if height > current_height + FRAUD_PROOF_HEAD_HEIGHT_THRESHOLD {
            // does this threshold make any sense if we're gonna ignore it anyway
            // since we won't have the header
            return gossipsub::MessageAcceptance::Ignore;
        }

        let hash = befp.header_hash();
        let Ok(header) = self.store.get_by_hash(&hash).await else {
            // we can't verify the proof without a header
            // TODO: should we then store it and wait for the height? celestia doesn't
            return gossipsub::MessageAcceptance::Ignore;
        };

        if let Err(e) = befp.validate(&header) {
            trace!("Received invalid bad encoding fraud proof from {peer}: {e}");
            self.swarm
                .context()
                .behaviour
                .gossipsub
                .blacklist_peer(peer);
            return gossipsub::MessageAcceptance::Reject;
        }

        warn!("Received a valid bad encoding fraud proof");
        // trigger cancellation for all services
        self.network_compromised_token.trigger();

        gossipsub::MessageAcceptance::Accept
    }
}

fn validate_bootnode_addrs(addrs: &[Multiaddr]) -> Result<(), P2pError> {
    let mut invalid_addrs = Vec::new();

    for addr in addrs {
        if addr.peer_id().is_none() {
            invalid_addrs.push(addr.to_owned());
        }
    }

    if invalid_addrs.is_empty() {
        Ok(())
    } else {
        Err(P2pError::BootnodeAddrsWithoutPeerId(invalid_addrs))
    }
}

fn init_gossipsub<'a, S>(
    args: &'a P2pArgs<S>,
    topics: impl IntoIterator<Item = &'a gossipsub::IdentTopic>,
) -> Result<gossipsub::Behaviour>
where
    S: Store,
{
    // Set the message authenticity - How we expect to publish messages
    // Here we expect the publisher to sign the message with their key.
    let message_authenticity = gossipsub::MessageAuthenticity::Signed(args.local_keypair.clone());

    let config = gossipsub::ConfigBuilder::default()
        .validation_mode(gossipsub::ValidationMode::Strict)
        .validate_messages()
        .build()
        .map_err(|e| P2pError::GossipsubInit(e.to_string()))?;

    // build a gossipsub network behaviour
    let mut gossipsub: gossipsub::Behaviour =
        gossipsub::Behaviour::new(message_authenticity, config)
            .map_err(|e| P2pError::GossipsubInit(e.to_string()))?;

    for topic in topics {
        gossipsub
            .subscribe(topic)
            .map_err(|e| P2pError::GossipsubInit(e.to_string()))?;
    }

    Ok(gossipsub)
}
