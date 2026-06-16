//! A browser compatible wrappers for the [`lumina-node`].

use std::time::Duration;

use blockstore::EitherBlockstore;
use celestia_types::blob::BlobsAtHeight;
use celestia_types::nmt::Namespace;
use celestia_types::{Blob, ExtendedHeader, SharesAtHeight};
use js_sys::{Array, AsyncIterator};
use libp2p::Multiaddr;
use libp2p::identity::Keypair;
use lumina_node::blockstore::{InMemoryBlockstore, IndexedDbBlockstore};
use lumina_node::network;
use lumina_node::node::{DEFAULT_PRUNING_WINDOW_IN_MEMORY, NodeBuilder};
use lumina_node::store::{EitherStore, InMemoryStore, IndexedDbStore, SamplingMetadata};
use serde::{Deserialize, Serialize};
use tracing::{debug, error};
use wasm_bindgen::prelude::*;
use web_sys::BroadcastChannel;

use crate::commands::{
    NodeCommand, SingleHeaderQuery, SubscriptionCommand, WorkerCommand, WorkerError, WorkerResponse,
};
use crate::error::{Context, Result};
use crate::subscriptions::into_async_iterator;
use crate::utils::{
    Network, is_safari, js_value_from_display, request_storage_persistence, timeout,
};
use crate::worker::{WasmBlockstore, WasmStore};
use crate::worker_client::WorkerClient;
use crate::wrapper::libp2p::NetworkInfoSnapshot;
use crate::wrapper::node::{PeerTrackerInfoSnapshot, SyncingInfoSnapshot};

/// Config for the lumina wasm node.
#[wasm_bindgen(inspectable, js_name = NodeConfig)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmNodeConfig {
    /// A network to connect to.
    pub network: Network,

    /// A list of bootstrap peers to connect to.
    #[wasm_bindgen(getter_with_clone)]
    pub bootnodes: Vec<String>,

    /// Optionally start with a provided private key used as libp2p identity. Expects 32 bytes
    /// containing ed25519 secret key.
    #[wasm_bindgen(getter_with_clone)]
    pub identity_key: Option<Vec<u8>>,

    /// Whether to store data in persistent memory or not.
    ///
    /// **Default value:** true
    #[wasm_bindgen(js_name = usePersistentMemory)]
    pub use_persistent_memory: bool,

    /// Pruning window defines maximum age of a block for it to be retained in store.
    ///
    /// If pruning window is smaller than sampling window, then blocks will be pruned
    /// right after they are sampled. This is useful when you want to keep low
    /// memory footprint but still validate the blockchain.
    ///
    /// If this is not set, then default value will apply:
    ///
    /// * If `use_persistent_memory == true`, default value is 7 days plus 1 hour.
    /// * If `use_persistent_memory == false`, default value is 0 seconds.
    #[wasm_bindgen(js_name = customPruningWindowSecs)]
    pub custom_pruning_window_secs: Option<u32>,
}

/// `NodeClient` is responsible for steering [`NodeWorker`] by sending it commands and receiving
/// responses over the provided port.
///
/// [`NodeWorker`]: crate::worker::NodeWorker
#[wasm_bindgen]
pub struct NodeClient {
    worker: WorkerClient,
}

#[wasm_bindgen]
impl NodeClient {
    /// Create a new connection to a Lumina node running in [`NodeWorker`]. Provided `port` is
    /// expected to have `MessagePort`-like interface for sending and receiving messages.
    #[wasm_bindgen(constructor)]
    #[allow(deprecated)] // TODO: https://github.com/eigerco/lumina/issues/754
    pub async fn new(port: JsValue) -> Result<NodeClient> {
        // Safari doesn't have the `navigator.storage()` api
        if !is_safari()?
            && let Err(e) = request_storage_persistence().await
        {
            error!("Error requesting storage persistence: {e}");
        }

        let worker = WorkerClient::new(port)?;

        // keep pinging worker until it responds.
        loop {
            if timeout(100, worker.worker_exec(WorkerCommand::InternalPing))
                .await
                .is_ok()
            {
                break;
            }
        }

        debug!("Connected to worker");

        Ok(Self { worker })
    }

    /// Establish a new connection to the existing worker over provided port
    #[wasm_bindgen(js_name = addConnectionToWorker)]
    pub async fn add_connection_to_worker(&self, port: JsValue) -> Result<()> {
        self.worker
            .worker_exec(WorkerCommand::ConnectPort(Some(port.into())))
            .await?;
        Ok(())
    }

    /// Check whether Lumina is currently running
    #[wasm_bindgen(js_name = isRunning)]
    pub async fn is_running(&self) -> Result<bool> {
        let command = WorkerCommand::IsRunning;
        let response = self.worker.worker_exec(command).await?;
        Ok(response
            .into_is_running()
            .map_err(|_| WorkerError::InvalidResponseType)?)
    }

    /// Start the node with the provided config, if it's not running
    pub async fn start(&self, config: &WasmNodeConfig) -> Result<()> {
        let command = WorkerCommand::StartNode(config.clone());
        let response = self.worker.worker_exec(command).await?;
        debug_assert!(matches!(response, WorkerResponse::Ok));

        Ok(())
    }

    /// Stop the node.
    pub async fn stop(&self) -> Result<()> {
        let command = WorkerCommand::StopNode;
        let response = self.worker.worker_exec(command).await?;
        debug_assert!(matches!(response, WorkerResponse::Ok));

        Ok(())
    }

    /// Get node's local peer ID.
    #[wasm_bindgen(js_name = localPeerId)]
    pub async fn local_peer_id(&self) -> Result<String> {
        let command = NodeCommand::GetLocalPeerId;
        let response = self.worker.node_exec(command).await?;
        let peer_id = response
            .into_local_peer_id()
            .map_err(|_| WorkerError::InvalidResponseType)?;

        Ok(peer_id)
    }

    /// Get current [`PeerTracker`] info.
    #[wasm_bindgen(js_name = peerTrackerInfo)]
    pub async fn peer_tracker_info(&self) -> Result<PeerTrackerInfoSnapshot> {
        let command = NodeCommand::GetPeerTrackerInfo;
        let response = self.worker.node_exec(command).await?;
        let peer_info = response
            .into_peer_tracker_info()
            .map_err(|_| WorkerError::InvalidResponseType)?;

        Ok(peer_info.into())
    }

    /// Wait until the node is connected to at least 1 peer.
    #[wasm_bindgen(js_name = waitConnected)]
    pub async fn wait_connected(&self) -> Result<()> {
        let command = NodeCommand::WaitConnected { trusted: false };
        let response = self.worker.node_exec(command).await?;
        debug_assert!(matches!(response, WorkerResponse::Ok));

        Ok(())
    }

    /// Wait until the node is connected to at least 1 trusted peer.
    #[wasm_bindgen(js_name = waitConnectedTrusted)]
    pub async fn wait_connected_trusted(&self) -> Result<()> {
        let command = NodeCommand::WaitConnected { trusted: true };
        let response = self.worker.node_exec(command).await?;
        debug_assert!(matches!(response, WorkerResponse::Ok));

        Ok(())
    }

    /// Get current network info.
    #[wasm_bindgen(js_name = networkInfo)]
    pub async fn network_info(&self) -> Result<NetworkInfoSnapshot> {
        let command = NodeCommand::GetNetworkInfo;
        let response = self.worker.node_exec(command).await?;

        Ok(response
            .into_network_info()
            .map_err(|_| WorkerError::InvalidResponseType)?)
    }

    /// Get all the multiaddresses on which the node listens.
    pub async fn listeners(&self) -> Result<Array> {
        let command = NodeCommand::GetListeners;
        let response = self.worker.node_exec(command).await?;
        let listeners = response
            .into_listeners()
            .map_err(|_| WorkerError::InvalidResponseType)?;
        let result = listeners.iter().map(js_value_from_display).collect();

        Ok(result)
    }

    /// Get all the peers that node is connected to.
    #[wasm_bindgen(js_name = connectedPeers)]
    pub async fn connected_peers(&self) -> Result<Array> {
        let command = NodeCommand::GetConnectedPeers;
        let response = self.worker.node_exec(command).await?;
        let peers = response
            .into_connected_peers()
            .map_err(|_| WorkerError::InvalidResponseType)?;
        let result = peers.iter().map(js_value_from_display).collect();

        Ok(result)
    }

    /// Trust or untrust the peer with a given ID.
    #[wasm_bindgen(js_name = setPeerTrust)]
    pub async fn set_peer_trust(&self, peer_id: &str, is_trusted: bool) -> Result<()> {
        let command = NodeCommand::SetPeerTrust {
            peer_id: peer_id.parse()?,
            is_trusted,
        };
        let response = self.worker.node_exec(command).await?;
        debug_assert!(matches!(response, WorkerResponse::Ok));

        Ok(())
    }

    /// Request the head header from the network.
    #[wasm_bindgen(js_name = requestHeadHeader)]
    pub async fn request_head_header(&self) -> Result<ExtendedHeader> {
        let command = NodeCommand::RequestHeader(SingleHeaderQuery::Head);
        let response = self.worker.node_exec(command).await?;
        Ok(response
            .into_header()
            .map_err(|_| WorkerError::InvalidResponseType)?)
    }

    /// Request a header for the block with a given hash from the network.
    #[wasm_bindgen(js_name = requestHeaderByHash)]
    pub async fn request_header_by_hash(&self, hash: &str) -> Result<ExtendedHeader> {
        let command = NodeCommand::RequestHeader(SingleHeaderQuery::ByHash(hash.parse()?));
        let response = self.worker.node_exec(command).await?;
        Ok(response
            .into_header()
            .map_err(|_| WorkerError::InvalidResponseType)?)
    }

    /// Request a header for the block with a given height from the network.
    #[wasm_bindgen(js_name = requestHeaderByHeight)]
    pub async fn request_header_by_height(&self, height: u64) -> Result<ExtendedHeader> {
        let command = NodeCommand::RequestHeader(SingleHeaderQuery::ByHeight(height));
        let response = self.worker.node_exec(command).await?;
        Ok(response
            .into_header()
            .map_err(|_| WorkerError::InvalidResponseType)?)
    }

    /// Request headers in range (from, from + amount] from the network.
    ///
    /// The headers will be verified with the `from` header.
    #[wasm_bindgen(js_name = requestVerifiedHeaders)]
    pub async fn request_verified_headers(
        &self,
        from: &ExtendedHeader,
        amount: u64,
    ) -> Result<Vec<ExtendedHeader>> {
        let command = NodeCommand::GetVerifiedHeaders {
            from: from.clone(),
            amount,
        };
        let response = self.worker.node_exec(command).await?;
        Ok(response
            .into_headers()
            .map_err(|_| WorkerError::InvalidResponseType)?)
    }

    /// Request all blobs with provided namespace in the block corresponding to this header
    /// using the shrex protocol.
    #[wasm_bindgen(js_name = requestAllBlobs)]
    pub async fn request_all_blobs(
        &self,
        namespace: &Namespace,
        block_height: u64,
        timeout_secs: Option<f64>,
    ) -> Result<Vec<Blob>> {
        let command = NodeCommand::RequestAllBlobs {
            namespace: *namespace,
            block_height,
            timeout_secs,
        };
        let response = self.worker.node_exec(command).await?;
        Ok(response
            .into_blobs()
            .map_err(|_| WorkerError::InvalidResponseType)?)
    }

    /// Get current header syncing info.
    #[wasm_bindgen(js_name = syncerInfo)]
    pub async fn syncer_info(&self) -> Result<SyncingInfoSnapshot> {
        let command = NodeCommand::GetSyncerInfo;
        let response = self.worker.node_exec(command).await?;
        let syncer_info = response
            .into_syncer_info()
            .map_err(|_| WorkerError::InvalidResponseType)?;

        Ok(syncer_info.into())
    }

    /// Get the latest header announced in the network.
    #[wasm_bindgen(js_name = getNetworkHeadHeader)]
    pub async fn get_network_head_header(&self) -> Result<Option<ExtendedHeader>> {
        let command = NodeCommand::LastSeenNetworkHead;
        let response = self.worker.node_exec(command).await?;
        Ok(response
            .into_last_seen_network_head()
            .map_err(|_| WorkerError::InvalidResponseType)?)
    }

    /// Get the latest locally synced header.
    #[wasm_bindgen(js_name = getLocalHeadHeader)]
    pub async fn get_local_head_header(&self) -> Result<ExtendedHeader> {
        let command = NodeCommand::GetHeader(SingleHeaderQuery::Head);
        let response = self.worker.node_exec(command).await?;
        Ok(response
            .into_header()
            .map_err(|_| WorkerError::InvalidResponseType)?)
    }

    /// Get a synced header for the block with a given hash.
    #[wasm_bindgen(js_name = getHeaderByHash)]
    pub async fn get_header_by_hash(&self, hash: &str) -> Result<ExtendedHeader> {
        let command = NodeCommand::GetHeader(SingleHeaderQuery::ByHash(hash.parse()?));
        let response = self.worker.node_exec(command).await?;
        Ok(response
            .into_header()
            .map_err(|_| WorkerError::InvalidResponseType)?)
    }

    /// Get a synced header for the block with a given height.
    #[wasm_bindgen(js_name = getHeaderByHeight)]
    pub async fn get_header_by_height(&self, height: u64) -> Result<ExtendedHeader> {
        let command = NodeCommand::GetHeader(SingleHeaderQuery::ByHeight(height));
        let response = self.worker.node_exec(command).await?;
        Ok(response
            .into_header()
            .map_err(|_| WorkerError::InvalidResponseType)?)
    }

    /// Get synced headers from the given heights range.
    ///
    /// If start of the range is undefined (None), the first returned header will be of height 1.
    /// If end of the range is undefined (None), the last returned header will be the last header in the
    /// store.
    ///
    /// # Errors
    ///
    /// If range contains a height of a header that is not found in the store.
    #[wasm_bindgen(js_name = getHeaders)]
    pub async fn get_headers(
        &self,
        start_height: Option<u64>,
        end_height: Option<u64>,
    ) -> Result<Vec<ExtendedHeader>> {
        let command = NodeCommand::GetHeadersRange {
            start_height,
            end_height,
        };
        let response = self.worker.node_exec(command).await?;
        Ok(response
            .into_headers()
            .map_err(|_| WorkerError::InvalidResponseType)?)
    }

    /// Get data sampling metadata of an already sampled height.
    #[wasm_bindgen(js_name = getSamplingMetadata)]
    pub async fn get_sampling_metadata(&self, height: u64) -> Result<Option<SamplingMetadata>> {
        let command = NodeCommand::GetSamplingMetadata { height };
        let response = self.worker.node_exec(command).await?;
        Ok(response
            .into_sampling_metadata()
            .map_err(|_| WorkerError::InvalidResponseType)?)
    }

    /// Returns a [`BroadcastChannel`] for events generated by [`Node`].
    #[wasm_bindgen(js_name = eventsChannel)]
    pub async fn events_channel(&self) -> Result<BroadcastChannel> {
        let command = WorkerCommand::GetEventsChannelName;
        let response = self.worker.worker_exec(command).await?;
        let name = response
            .into_events_channel_name()
            .map_err(|_| WorkerError::InvalidResponseType)?;

        Ok(BroadcastChannel::new(&name).unwrap())
    }

    /// Subscribe to new headers received by the node from the network.
    ///
    /// Return an async iterator which will yield all the headers, as they are being received by the
    /// node, starting from the first header received after the call.
    #[wasm_bindgen(js_name = headerSubscribe, unchecked_return_type = "AsyncIterable<ExtendedHeader | SubscriptionError>")]
    pub async fn header_subscribe(&self) -> Result<AsyncIterator> {
        let command = SubscriptionCommand::Headers;
        let port = self.worker.subscribe(command).await?;

        into_async_iterator::<ExtendedHeader>(port)
    }

    /// Subscribe to the shares from the namespace, as new headers are received by the node
    ///
    /// Return an async iterator which will yield all the blobs from the namespace, as the new headers
    /// are being received by the node, starting from the first header received after the call.
    #[wasm_bindgen(js_name = blobSubscribe, unchecked_return_type = "AsyncIterable<BlobsAtHeight | SubscriptionError>")]
    pub async fn blob_subscribe(&self, namespace: Namespace) -> Result<AsyncIterator> {
        let command = SubscriptionCommand::Blobs(namespace);
        let port = self.worker.subscribe(command).await?;

        into_async_iterator::<BlobsAtHeight>(port)
    }

    /// Subscribe to the blobs from the namespace, as new headers are received by the node
    ///
    /// Return an async iterator which will yield all the shares from the namespace, as the new headers
    /// are being received by the node, starting from the first header received after the call.
    #[wasm_bindgen(js_name = namespaceSubscribe, unchecked_return_type = "AsyncIterable<SharesAtHeight | SubscriptionError>")]
    pub async fn namespace_subscribe(&self, namespace: Namespace) -> Result<AsyncIterator> {
        let command = SubscriptionCommand::Shares(namespace);
        let port = self.worker.subscribe(command).await?;

        into_async_iterator::<SharesAtHeight>(port)
    }
}

#[wasm_bindgen(js_class = NodeConfig)]
impl WasmNodeConfig {
    /// Get the configuration with default bootnodes for provided network
    pub fn default(network: Network) -> WasmNodeConfig {
        let bootnodes = network::Network::from(network)
            .canonical_bootnodes()
            .map(|addr| addr.to_string())
            .collect::<Vec<_>>();

        WasmNodeConfig {
            network,
            bootnodes,
            identity_key: None,
            use_persistent_memory: true,
            custom_pruning_window_secs: None,
        }
    }

    pub(crate) async fn into_node_builder(self) -> Result<NodeBuilder<WasmBlockstore, WasmStore>> {
        let network = network::Network::from(self.network);
        let network_id = network.id();

        let mut builder = if self.use_persistent_memory {
            let store_name = format!("lumina-{network_id}");
            let blockstore_name = format!("lumina-{network_id}-blockstore");

            let store = IndexedDbStore::new(&store_name)
                .await
                .context("Failed to open the store")?;

            let blockstore = IndexedDbBlockstore::new(&blockstore_name)
                .await
                .context("Failed to open the blockstore")?;

            NodeBuilder::new()
                .store(EitherStore::Right(store))
                .blockstore(EitherBlockstore::Right(blockstore))
        } else {
            NodeBuilder::new()
                .store(EitherStore::Left(InMemoryStore::new()))
                .blockstore(EitherBlockstore::Left(InMemoryBlockstore::new()))
                // In-memory stores are memory hungry, so we prune blocks as soon as possible.
                .pruning_window(DEFAULT_PRUNING_WINDOW_IN_MEMORY)
        };

        if let Some(key_bytes) = self.identity_key {
            let keypair = Keypair::ed25519_from_bytes(key_bytes).context("could not decode key")?;
            builder = builder.keypair(keypair);
        }

        let bootnodes = self
            .bootnodes
            .into_iter()
            .map(|addr| {
                addr.parse()
                    .with_context(|| format!("invalid multiaddr: {addr}"))
            })
            .collect::<Result<Vec<Multiaddr>, _>>()?;

        builder = builder
            .network(network)
            .sync_batch_size(128)
            .bootnodes(bootnodes);

        if let Some(secs) = self.custom_pruning_window_secs {
            let dur = Duration::from_secs(secs.into());
            builder = builder.pruning_window(dur);
        }

        Ok(builder)
    }
}
