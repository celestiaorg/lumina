use std::any::TypeId;
use std::time::Duration;

use blockstore::Blockstore;
use libp2p::Multiaddr;
use libp2p::identity::Keypair;
use tracing::{info, warn};

use crate::blockstore::InMemoryBlockstore;
use crate::events::EventSubscriber;
use crate::network::Network;
use crate::node::{Node, NodeConfig, Result};
use crate::store::{InMemoryStore, Store, StoreError};
#[cfg(target_arch = "wasm32")]
use crate::utils::resolve_bootnode_addresses;

const HOUR: u64 = 60 * 60;
const DAY: u64 = 24 * HOUR;

/// Default maximum age of blocks [`Node`] will synchronise, sample, and store.
pub const SAMPLING_WINDOW: Duration = Duration::from_secs(7 * DAY);

/// Default maximum age of blocks before they get pruned.
pub const DEFAULT_PRUNING_WINDOW: Duration = Duration::from_secs(7 * DAY + HOUR);
/// Default pruninig window for in-memory stores.
pub const DEFAULT_PRUNING_WINDOW_IN_MEMORY: Duration = Duration::from_secs(0);

/// [`Node`] builder.
pub struct NodeBuilder<B, S>
where
    B: Blockstore + 'static,
    S: Store + 'static,
{
    blockstore: B,
    store: S,
    keypair: Option<Keypair>,
    network: Option<Network>,
    bootnodes: Vec<Multiaddr>,
    listen: Vec<Multiaddr>,
    sync_batch_size: Option<u64>,
    pruning_window: Option<Duration>,
}

/// Representation of all the errors that can occur when interacting with the [`NodeBuilder`].
#[derive(Debug, thiserror::Error)]
pub enum NodeBuilderError {
    /// Network is not specified
    #[error("Network is not specified")]
    NetworkNotSpecified,

    /// Builder failed to resolve dnsaddr multiaddresses for bootnodes
    #[error("Could not resolve any of the bootnode addresses")]
    FailedResolvingBootnodes,

    /// Error decoding libp2p identity from the store
    #[error(transparent)]
    IdentityDecodingError(#[from] libp2p::identity::DecodingError),

    /// Error propagated from the store
    #[error(transparent)]
    StoreError(#[from] StoreError),
}

impl NodeBuilder<InMemoryBlockstore, InMemoryStore> {
    /// Creates a new [`NodeBuilder`] which uses in-memory stores.
    ///
    /// After the creation you can call [`NodeBuilder::blockstore`]
    /// and [`NodeBuilder::store`] to use other stores.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use lumina_node::network::Network;
    /// # use lumina_node::NodeBuilder;
    /// #
    /// # async fn example() {
    /// let node = NodeBuilder::new()
    ///     .network(Network::Mainnet)
    ///     .start()
    ///     .await
    ///     .unwrap();
    /// # }
    /// ```
    pub fn new() -> Self {
        NodeBuilder {
            blockstore: InMemoryBlockstore::new(),
            store: InMemoryStore::new(),
            keypair: None,
            network: None,
            bootnodes: Vec::new(),
            listen: Vec::new(),
            sync_batch_size: None,
            pruning_window: None,
        }
    }
}

impl Default for NodeBuilder<InMemoryBlockstore, InMemoryStore> {
    fn default() -> Self {
        NodeBuilder::new()
    }
}

impl<B, S> NodeBuilder<B, S>
where
    B: Blockstore + 'static,
    S: Store + 'static,
{
    /// Creates and starts a new Celestia [`Node`].
    pub async fn start(self) -> Result<Node<B, S>> {
        let (node, _) = self.start_subscribed().await?;
        Ok(node)
    }

    /// Creates and starts a new Celestia [`Node`].
    ///
    /// Returns [`Node`] along with [`EventSubscriber`]. Use this to avoid missing
    /// any events that will be generated on the construction of the node.
    pub async fn start_subscribed(self) -> Result<(Node<B, S>, EventSubscriber)> {
        let config = self.build_config().await?;
        Node::start(config).await
    }

    /// Set the [`Blockstore`] used to store sampled data.
    ///
    /// **Default:** [`InMemoryBlockstore`]
    pub fn blockstore<B2>(self, blockstore: B2) -> NodeBuilder<B2, S>
    where
        B2: Blockstore + 'static,
    {
        NodeBuilder {
            blockstore,
            store: self.store,
            keypair: self.keypair,
            network: self.network,
            bootnodes: self.bootnodes,
            listen: self.listen,
            sync_batch_size: self.sync_batch_size,
            pruning_window: self.pruning_window,
        }
    }

    /// Set the [`Store`] for headers.
    ///
    /// **Default:** [`InMemoryStore`]
    pub fn store<S2>(self, store: S2) -> NodeBuilder<B, S2>
    where
        S2: Store + 'static,
    {
        NodeBuilder {
            blockstore: self.blockstore,
            store,
            keypair: self.keypair,
            network: self.network,
            bootnodes: self.bootnodes,
            listen: self.listen,
            sync_batch_size: self.sync_batch_size,
            pruning_window: self.pruning_window,
        }
    }

    /// The [`Network`] to connect to.
    pub fn network(self, network: Network) -> Self {
        NodeBuilder {
            network: Some(network),
            ..self
        }
    }

    /// Set the keypair to be used as [`Node`]s identity.
    ///
    /// This enforces given libp2p identity to be used by the node.
    ///
    /// By default, random identity is created by the store and persisted.
    /// Running more than one node with the same identiity is undefined behaviour,
    /// because all nodes with the same `peer_id` will be treated as a singular
    /// instance. Network can randomly choose a connection used to send a message
    /// to the node, so with multiple instances, each message from the network could
    /// be received by a random node with the same keypair. This can potentially
    /// lead to broken state and rejecting valid messages. Please make sure that
    /// when you use it, only one node has this keypair.
    ///
    /// Special care needs to be taken in wasm, as there if your app has hardcoded key,
    /// user opening multiple tabs with it may lead to undefined behaviour.
    pub fn keypair(self, keypair: Keypair) -> Self {
        NodeBuilder {
            keypair: Some(keypair),
            ..self
        }
    }

    /// Set the bootstrap nodes to connect and trust.
    ///
    /// **Default:** [`Network::canonical_bootnodes`]
    pub fn bootnodes<I>(self, addrs: I) -> Self
    where
        I: IntoIterator<Item = Multiaddr>,
    {
        NodeBuilder {
            bootnodes: addrs.into_iter().collect(),
            ..self
        }
    }

    /// Set the addresses where [`Node`] will listen for incoming connections.
    pub fn listen<I>(self, addrs: I) -> Self
    where
        I: IntoIterator<Item = Multiaddr>,
    {
        NodeBuilder {
            listen: addrs.into_iter().collect(),
            ..self
        }
    }

    /// Maximum number of headers in batch while syncing.
    ///
    /// **Default:** 512
    pub fn sync_batch_size(self, batch_size: u64) -> Self {
        NodeBuilder {
            sync_batch_size: Some(batch_size),
            ..self
        }
    }

    /// Set pruning window.
    ///
    /// Pruning window defines maximum age of a block for it to be retained in store.
    ///
    /// If pruning window is smaller than sampling window, then blocks will be pruned
    /// right after they get sampled. This is useful when you want to keep low
    /// memory footprint but still validate the blockchain.
    ///
    /// **Default if [`InMemoryStore`]/[`InMemoryBlockstore`] are used:** 0 seconds.\
    /// **Default:** 7 days plus 1 hour.\
    pub fn pruning_window(self, dur: Duration) -> Self {
        NodeBuilder {
            pruning_window: Some(dur),
            ..self
        }
    }

    async fn build_config(self) -> Result<NodeConfig<B, S>, NodeBuilderError> {
        let network = self.network.ok_or(NodeBuilderError::NetworkNotSpecified)?;

        let bootnodes = if self.bootnodes.is_empty() {
            network.canonical_bootnodes().collect()
        } else {
            self.bootnodes
        };

        if bootnodes.is_empty() && self.listen.is_empty() {
            // It is a valid scenario for user to create a node without any bootnodes
            // and listening addresses. However it may not be what they wanted. Because
            // of that we display a warning.
            warn!(
                "Node has empty bootnodes and listening addresses. It will never connect to another peer."
            );
        }

        #[cfg(target_arch = "wasm32")]
        let bootnodes = {
            let bootnodes_was_empty = bootnodes.is_empty();
            let bootnodes = resolve_bootnode_addresses(bootnodes).await;

            // If we had some bootnodes but resolving them failed for all of them,
            // then we fail with an error.
            if bootnodes.is_empty() && !bootnodes_was_empty {
                return Err(NodeBuilderError::FailedResolvingBootnodes);
            }

            bootnodes
        };

        // `Node` is memory hungry when in-memory stores are used and the user may not
        // expect they should set a smaller sampling window to reduce that. For user-friendliness
        // sake, use smaller default sampling window, if we're running in memory.
        let in_memory_stores_used = TypeId::of::<S>() == TypeId::of::<InMemoryStore>()
            || TypeId::of::<B>() == TypeId::of::<InMemoryBlockstore>();

        let pruning_window = if let Some(dur) = self.pruning_window {
            dur
        } else if in_memory_stores_used {
            DEFAULT_PRUNING_WINDOW_IN_MEMORY
        } else {
            DEFAULT_PRUNING_WINDOW
        };

        info!("Sampling window: {SAMPLING_WINDOW:?}, Pruning window: {pruning_window:?}");

        let p2p_local_keypair = if let Some(keypair) = self.keypair {
            keypair
        } else {
            self.store.get_identity().await?
        };

        Ok(NodeConfig {
            blockstore: self.blockstore,
            store: self.store,
            network_id: network.id().to_owned(),
            p2p_local_keypair,
            p2p_bootnodes: bootnodes,
            p2p_listen_on: self.listen,
            sync_batch_size: self.sync_batch_size.unwrap_or(512),
            sampling_window: SAMPLING_WINDOW,
            pruning_window,
        })
    }
}
