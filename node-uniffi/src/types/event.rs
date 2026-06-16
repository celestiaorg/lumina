use libp2p::PeerId as Libp2pPeerId;
use lumina_node::events::NodeEvent as LuminaNodeEvent;
use std::str::FromStr;
use uniffi::Record;

#[derive(Record, Clone, Debug)]
pub struct PeerId {
    /// The peer ID stored as base58 string.
    pub peer_id: String,
}

impl PeerId {
    pub fn to_libp2p(&self) -> std::result::Result<Libp2pPeerId, String> {
        Libp2pPeerId::from_str(&self.peer_id).map_err(|e| format!("Invalid peer ID format: {e}"))
    }

    pub fn from_libp2p(peer_id: &Libp2pPeerId) -> Self {
        Self {
            peer_id: peer_id.to_string(),
        }
    }
}

impl From<Libp2pPeerId> for PeerId {
    fn from(peer_id: Libp2pPeerId) -> Self {
        Self {
            peer_id: peer_id.to_string(),
        }
    }
}

#[derive(Record)]
pub struct ShareCoordinate {
    row: u16,
    column: u16,
}

/// Events emitted by the node.
#[derive(uniffi::Enum)]
pub enum NodeEvent {
    /// Node is connecting to bootnodes
    ConnectingToBootnodes,
    /// Peer just connected
    PeerConnected {
        /// The ID of the peer.
        id: PeerId,
        /// Whether peer was in the trusted list or not.
        trusted: bool,
    },
    PeerDisconnected {
        /// The ID of the peer.
        id: PeerId,
        /// Whether peer was in the trusted list or not.
        trusted: bool,
    },
    /// Sampling just started.
    SamplingStarted {
        /// The block height that will be sampled.
        height: u64,
        /// The square width of the block.
        square_width: u16,
        /// The coordinates of the shares that will be sampled.
        shares: Vec<ShareCoordinate>,
    },
    /// Share sampling result.
    ShareSamplingResult {
        /// The block height of the share.
        height: u64,
        /// The square width of the block.
        square_width: u16,
        /// The row of the share.
        row: u16,
        /// The column of the share.
        column: u16,
        /// Share sampling timed out.
        failed: bool,
    },
    /// Sampling result.
    SamplingResult {
        /// The block height that was sampled.
        height: u64,
        /// Sampling timed out.
        failed: bool,
        /// How much time sampling took in milliseconds.
        took_ms: u64,
    },
    /// Data sampling fatal error.
    FatalDaserError {
        /// A human readable error.
        error: String,
    },
    /// A new header was added from HeaderSub.
    AddedHeaderFromHeaderSub {
        /// The height of the header.
        height: u64,
    },
    /// Fetching header of network head just started.
    FetchingHeadHeaderStarted,
    /// Fetching header of network head just finished.
    FetchingHeadHeaderFinished {
        /// The height of the network head.
        height: u64,
        /// How much time fetching took in milliseconds.
        took_ms: u64,
    },
    /// Fetching headers of a specific block range just started.
    FetchingHeadersStarted {
        /// Start of the range.
        from_height: u64,
        /// End of the range (inclusive).
        to_height: u64,
    },
    /// Fetching headers of a specific block range just finished.
    FetchingHeadersFinished {
        /// Start of the range.
        from_height: u64,
        /// End of the range (inclusive).
        to_height: u64,
        /// How much time fetching took in milliseconds.
        took_ms: u64,
    },
    /// Fetching headers of a specific block range just failed.
    FetchingHeadersFailed {
        /// Start of the range.
        from_height: u64,
        /// End of the range (inclusive).
        to_height: u64,
        /// A human readable error.
        error: String,
        /// How much time fetching took in milliseconds.
        took_ms: u64,
    },
    /// Header syncing fatal error.
    FatalSyncerError {
        /// A human readable error.
        error: String,
    },
    /// Range of headers that were pruned.
    PrunedHeaders {
        /// Start of the range.
        from_height: u64,
        /// End of the range (inclusive).
        to_height: u64,
    },
    /// Pruning fatal error.
    FatalPrunerError {
        /// A human readable error.
        error: String,
    },
    /// Network was compromised.
    ///
    /// This happens when a valid bad encoding fraud proof is received.
    /// Ideally it would never happen, but protection needs to exist.
    /// In case of compromised network, syncing and data sampling will
    /// stop immediately.
    NetworkCompromised,
    /// Node stopped.
    NodeStopped,
}

impl From<LuminaNodeEvent> for NodeEvent {
    fn from(event: LuminaNodeEvent) -> Self {
        match event {
            LuminaNodeEvent::ConnectingToBootnodes => NodeEvent::ConnectingToBootnodes,
            LuminaNodeEvent::PeerConnected { id, trusted } => NodeEvent::PeerConnected {
                id: PeerId::from_libp2p(&id),
                trusted,
            },
            LuminaNodeEvent::PeerDisconnected { id, trusted } => NodeEvent::PeerDisconnected {
                id: PeerId::from_libp2p(&id),
                trusted,
            },
            LuminaNodeEvent::SamplingStarted {
                height,
                square_width,
                shares,
            } => NodeEvent::SamplingStarted {
                height,
                square_width,
                shares: shares
                    .into_iter()
                    .map(|(row, col)| ShareCoordinate { row, column: col })
                    .collect(),
            },
            LuminaNodeEvent::ShareSamplingResult {
                height,
                square_width,
                row,
                column,
                failed,
            } => NodeEvent::ShareSamplingResult {
                height,
                square_width,
                row,
                column,
                failed,
            },
            LuminaNodeEvent::SamplingResult {
                height,
                failed,
                took,
            } => NodeEvent::SamplingResult {
                height,
                failed,
                took_ms: took.as_millis() as u64,
            },
            LuminaNodeEvent::FatalDaserError { error } => NodeEvent::FatalDaserError { error },
            LuminaNodeEvent::AddedHeaderFromHeaderSub { height } => {
                NodeEvent::AddedHeaderFromHeaderSub { height }
            }
            LuminaNodeEvent::FetchingHeadHeaderStarted => NodeEvent::FetchingHeadHeaderStarted,
            LuminaNodeEvent::FetchingHeadHeaderFinished { height, took } => {
                NodeEvent::FetchingHeadHeaderFinished {
                    height,
                    took_ms: took.as_millis() as u64,
                }
            }
            LuminaNodeEvent::FetchingHeadersStarted {
                from_height,
                to_height,
            } => NodeEvent::FetchingHeadersStarted {
                from_height,
                to_height,
            },
            LuminaNodeEvent::FetchingHeadersFinished {
                from_height,
                to_height,
                took,
            } => NodeEvent::FetchingHeadersFinished {
                from_height,
                to_height,
                took_ms: took.as_millis() as u64,
            },
            LuminaNodeEvent::FetchingHeadersFailed {
                from_height,
                to_height,
                error,
                took,
            } => NodeEvent::FetchingHeadersFailed {
                from_height,
                to_height,
                error,
                took_ms: took.as_millis() as u64,
            },
            LuminaNodeEvent::FatalSyncerError { error } => NodeEvent::FatalSyncerError { error },
            LuminaNodeEvent::PrunedHeaders {
                from_height,
                to_height,
            } => NodeEvent::PrunedHeaders {
                from_height,
                to_height,
            },
            LuminaNodeEvent::FatalPrunerError { error } => NodeEvent::FatalPrunerError { error },
            LuminaNodeEvent::NetworkCompromised => NodeEvent::NetworkCompromised,
            LuminaNodeEvent::NodeStopped => NodeEvent::NodeStopped,
            _ => panic!("Unknown event: {event:?}"),
        }
    }
}
