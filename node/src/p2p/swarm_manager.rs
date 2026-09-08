use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::LazyLock;
use std::time::Duration;

use futures::StreamExt;
use libp2p::allow_block_list::{self, BlockedPeers};
use libp2p::core::transport::ListenerId;
use libp2p::identity::Keypair;
use libp2p::kad::{QueryInfo, RecordKey};
use libp2p::multiaddr::{Multiaddr, Protocol};
use libp2p::swarm::behaviour::toggle::Toggle;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::{ConnectionId, DialError, NetworkBehaviour, NetworkInfo, Swarm, SwarmEvent};
use libp2p::{PeerId, autonat, identify, kad, ping};
use lumina_utils::time::Interval;
use multihash_codetable::{Code, MultihashDigest};
use tokio::select;
use tokio::sync::watch;
use tracing::{debug, error, instrument, trace, warn};

use crate::events::{EventPublisher, NodeEvent};
use crate::p2p::Result;
use crate::p2p::connection_control;
use crate::p2p::swarm::new_swarm;
use crate::peer_tracker::{GC_INTERVAL, Peer, PeerTracker, PeerTrackerInfo};
use crate::utils::{MultiaddrExt, celestia_protocol_id};

const FULL_NODES_PROTECT_LIMIT: usize = 5;
const ARCHIVAL_NODES_PROTECT_LIMIT: usize = 5;

static FULL_NODE_TOPIC: LazyLock<RecordKey> = LazyLock::new(|| dht_topic("/full/v0.1.0"));
static ARCHIVAL_NODE_TOPIC: LazyLock<RecordKey> = LazyLock::new(|| dht_topic("/archival/v0.1.0"));

const BOOTNODE_PROTECT_TAG: u32 = 0;
const FULL_PROTECT_TAG: u32 = 1;
const ARCHIVAL_PROTECT_TAG: u32 = 2;

const PEER_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(60);
const AGGRESSIVE_PEER_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(10);

#[derive(NetworkBehaviour)]
struct SwarmBehaviour<B>
where
    B: NetworkBehaviour + 'static,
    B::ToSwarm: Debug,
{
    stream: libp2p_stream::Behaviour,
    connection_control: connection_control::Behaviour,
    blacklist: allow_block_list::Behaviour<BlockedPeers>,
    autonat: autonat::Behaviour,
    ping: ping::Behaviour,
    identify: identify::Behaviour,
    kademlia: kad::Behaviour<kad::store::MemoryStore>,
    // We use `Toggle` here because we want a way to initialize `SwarmManager`
    // without the need to pass user's Behaviour in the constructor.
    //
    // The main reason we want this, is because `SwarmBehaviour` has its own
    // `libp2p_stream::Behaviour`, and user's Behaviour may need an allocated
    // `libp2p_stream::Control` from it, which creates chicken and the egg situation.
    // With `Toggle` we allow user to attach its Behaviour, after `SwarmManager` construction.
    behaviour: Toggle<B>,
}

pub(crate) struct SwarmManager<B>
where
    B: NetworkBehaviour + 'static,
    B::ToSwarm: Debug,
{
    swarm: Swarm<SwarmBehaviour<B>>,
    peer_tracker: PeerTracker,
    peer_tracker_info_watcher: watch::Receiver<PeerTrackerInfo>,
    event_pub: EventPublisher,
    bootnodes: HashMap<PeerId, Vec<Multiaddr>>,
    listeners: Vec<ListenerId>,
    peer_health_check_interval: Interval,
    gc_interval: Interval,
    first_connection_established: bool,
}

pub(crate) struct SwarmContext<'a, B>
where
    B: NetworkBehaviour,
{
    pub(crate) behaviour: &'a mut B,
    pub(crate) peer_tracker: &'a PeerTracker,
}

impl<B> SwarmManager<B>
where
    B: NetworkBehaviour,
    B::ToSwarm: Debug,
{
    pub(crate) async fn new(
        network_id: &str,
        keypair: &Keypair,
        bootnodes: &[Multiaddr],
        listen_on: &[Multiaddr],
        mut peer_tracker: PeerTracker,
        event_pub: EventPublisher,
    ) -> Result<SwarmManager<B>> {
        let local_peer_id = PeerId::from(keypair.public());

        let connection_control = connection_control::Behaviour::new();
        let autonat = autonat::Behaviour::new(local_peer_id, autonat::Config::default());
        let ping = ping::Behaviour::new(ping::Config::default());
        let kademlia = init_kademlia(network_id, keypair, listen_on)?;

        let agent_version = format!("lumina/{}/{}", network_id, env!("CARGO_PKG_VERSION"));
        let identify_config = identify::Config::new(String::new(), keypair.public())
            .with_agent_version(agent_version);
        let identify = identify::Behaviour::new(identify_config);

        let behaviour = SwarmBehaviour {
            stream: libp2p_stream::Behaviour::new(),
            connection_control,
            blacklist: Default::default(),
            autonat,
            ping,
            identify,
            kademlia,
            behaviour: None.into(),
        };

        let mut swarm = new_swarm(keypair.to_owned(), behaviour).await?;
        let mut listeners = Vec::new();

        for addr in listen_on {
            match swarm.listen_on(addr.clone()) {
                Ok(id) => listeners.push(id),
                Err(e) => error!("Failed to listen on {addr}: {e}"),
            }
        }

        let mut bootnodes_map = HashMap::<_, Vec<_>>::new();

        for addr in bootnodes {
            let peer_id = addr.peer_id().expect("multiaddr already validated");
            bootnodes_map
                .entry(peer_id)
                .or_default()
                .push(addr.to_owned());
        }

        for (peer_id, addrs) in bootnodes_map.iter_mut() {
            addrs.sort();
            addrs.dedup();
            addrs.shrink_to_fit();

            // Bootnodes are always trusted and protected
            peer_tracker.set_trusted(peer_id, true);
            peer_tracker.protect(peer_id, BOOTNODE_PROTECT_TAG);
        }

        let peer_tracker_info_watcher = peer_tracker.info_watcher();
        let peer_health_check_interval = Interval::new(AGGRESSIVE_PEER_HEALTH_CHECK_INTERVAL);
        let gc_interval = Interval::new(GC_INTERVAL);

        let mut manager = SwarmManager {
            swarm,
            peer_tracker,
            peer_tracker_info_watcher,
            event_pub,
            bootnodes: bootnodes_map,
            listeners,
            peer_health_check_interval,
            gc_interval,
            first_connection_established: false,
        };

        manager.bootstrap();
        manager.start_full_node_kad_query();
        manager.start_archival_node_kad_query();

        Ok(manager)
    }

    pub(crate) fn attach_behaviour(&mut self, behaviour: B) {
        if self.swarm.behaviour_mut().behaviour.is_enabled() {
            panic!("Behaviour can be attached on SwarmManager only once!");
        }

        self.swarm.behaviour_mut().behaviour = Some(behaviour).into();
    }

    pub(crate) fn stream_control(&self) -> libp2p_stream::Control {
        self.swarm.behaviour().stream.new_control()
    }

    pub(crate) async fn poll(&mut self) -> Result<B::ToSwarm> {
        if !self.swarm.behaviour_mut().behaviour.is_enabled() {
            panic!("Behaviour not attached on SwarmManager");
        }

        loop {
            select! {
                _ = self.peer_tracker_info_watcher.changed() => {
                    self.peer_health_check().await;
                }
                _ = self.peer_health_check_interval.tick() => {
                    self.peer_health_check().await;
                }
                _ = self.gc_interval.tick() => {
                    self.peer_tracker.gc();
                }
                ev = self.swarm.select_next_some() => {
                    if let Some(ev) = self.on_swarm_event(ev) {
                        return Ok(ev);
                    }
                }
            }
        }
    }

    pub(crate) fn context<'a>(&'a mut self) -> SwarmContext<'a, B> {
        SwarmContext {
            behaviour: self
                .swarm
                .behaviour_mut()
                .behaviour
                .as_mut()
                .expect("Behaviour not attached on SwarmManager"),
            peer_tracker: &self.peer_tracker,
        }
    }

    fn connect(&mut self, peer_id: &PeerId, addresses: impl Into<Option<Vec<Multiaddr>>>) {
        if self.peer_tracker.is_connected(peer_id) {
            return;
        }

        let addresses = addresses.into().unwrap_or_default();

        let dial_opts = DialOpts::peer_id(*peer_id)
            // Tell Swarm not to dial if peer is already connected or there
            // is an ongoing dialing.
            .condition(PeerCondition::DisconnectedAndNotDialing);

        let dial_opts = if addresses.is_empty() {
            dial_opts.build()
        } else {
            dial_opts.addresses(addresses.clone()).build()
        };

        if let Err(e) = self.swarm.dial(dial_opts)
            && !matches!(e, DialError::DialPeerConditionFalse(_))
        {
            warn!("Failed to dial on {addresses:?}: {e}");
        }
    }

    /// Tries to find the node via Kademlia and connect to it
    fn find_node_and_connect(&mut self, peer_id: &PeerId) {
        // Check if Kademlia already knows peer_id and their addresses
        let kad_entry_exists = self
            .swarm
            .behaviour_mut()
            .kademlia
            .kbucket(*peer_id)
            .map(|bucket| {
                bucket
                    .iter()
                    .any(|entry| entry.node.key.preimage() == peer_id)
            })
            .unwrap_or(false);

        if kad_entry_exists {
            // When `connect` is called without a specified address, then
            // swarm asks Kademlia about it.
            self.connect(peer_id, None);
            return;
        }

        // When Kademlia doesn't know peer_id, we need to find it and
        // its addresses using `get_closest_peers` query.

        // First, make sure there isn't already a query in progress for this peer.
        let peer_id_bytes = peer_id.to_bytes();
        let kad_query_exists = self
            .swarm
            .behaviour_mut()
            .kademlia
            .iter_queries()
            .any(|query| match query.info() {
                QueryInfo::GetClosestPeers { key, .. } => *key == peer_id_bytes,
                _ => false,
            });

        if !kad_query_exists {
            // When kademlia finds the addresses via `get_closest_peers`, it will
            // automatically connect to them.
            self.swarm
                .behaviour_mut()
                .kademlia
                .get_closest_peers(*peer_id);
        }
    }

    pub(crate) fn connect_to_bootnodes(&mut self) {
        // Collect all the bootnodes that are not currently connected.
        let bootnodes = self
            .bootnodes
            .iter()
            .filter_map(|(peer_id, addrs)| {
                if self.peer_tracker.is_connected(peer_id) {
                    None
                } else {
                    Some((peer_id.to_owned(), addrs.to_owned()))
                }
            })
            .collect::<Vec<_>>();

        if bootnodes.is_empty() {
            return;
        }

        // We produce this event only if we are going to connect to at least
        // one bootnode.
        self.event_pub.send(NodeEvent::ConnectingToBootnodes);

        for (peer_id, addrs) in bootnodes {
            for addr in &addrs {
                self.swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer_id, addr.to_owned());
            }

            self.connect(&peer_id, addrs);
        }
    }

    fn bootstrap(&mut self) {
        self.connect_to_bootnodes();

        // Check if there is bootstrap in progress
        let bootstrap_query_exists = self
            .swarm
            .behaviour_mut()
            .kademlia
            .iter_queries()
            .any(|query| matches!(query.info(), QueryInfo::Bootstrap { .. }));

        if bootstrap_query_exists {
            return;
        }

        if let Err(e) = self.swarm.behaviour_mut().kademlia.bootstrap() {
            warn!("Can't run kademlia bootstrap: {e}");
        }
    }

    fn start_get_providers_kad_query(&mut self, topic: &RecordKey) {
        // Avoid running multiple `GetProviders` queries for the same `topic`.
        let kad_query_exists = self.swarm.behaviour_mut().kademlia.iter_queries().any(
            |query| matches!(query.info(), QueryInfo::GetProviders { key, .. } if key == topic),
        );

        if kad_query_exists {
            return;
        }

        // `get_providers` reports already known providers of the
        // `topic` and tries to discover new ones.
        self.swarm
            .behaviour_mut()
            .kademlia
            .get_providers(topic.to_owned());
    }

    pub(crate) fn start_full_node_kad_query(&mut self) {
        self.start_get_providers_kad_query(&FULL_NODE_TOPIC);
    }

    pub(crate) fn start_archival_node_kad_query(&mut self) {
        self.start_get_providers_kad_query(&ARCHIVAL_NODE_TOPIC);
    }

    pub(crate) fn network_info(&self) -> NetworkInfo {
        self.swarm.network_info()
    }

    pub(crate) fn local_peer_id(&self) -> PeerId {
        self.swarm.local_peer_id().to_owned()
    }

    pub(crate) fn listeners(&self) -> Vec<Multiaddr> {
        let local_peer_id = self.local_peer_id();

        self.swarm
            .listeners()
            .cloned()
            .map(|mut ma| {
                if !ma.protocol_stack().any(|protocol| protocol == "p2p") {
                    ma.push(Protocol::P2p(local_peer_id))
                }
                ma
            })
            .collect()
    }

    pub(crate) fn set_peer_trust(&mut self, peer_id: &PeerId, is_trusted: bool) {
        if self.swarm.local_peer_id() != peer_id {
            self.peer_tracker.set_trusted(peer_id, is_trusted);
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn mark_as_archival(&mut self, peer_id: &PeerId) {
        if self.swarm.local_peer_id() != peer_id {
            self.peer_tracker.mark_as_archival(peer_id);
        }
    }

    fn protect(&mut self, peer_id: &PeerId, tag: u32) {
        if self.peer_tracker.protect(peer_id, tag) {
            // Change keep alive of ongoing connections to `true`
            for conn_id in self.peer_tracker.connections(peer_id) {
                self.swarm
                    .behaviour_mut()
                    .connection_control
                    .set_keep_alive(peer_id, conn_id, true);
            }
        }
    }

    fn unprotect(&mut self, peer_id: &PeerId, tag: u32) {
        if self.peer_tracker.unprotect(peer_id, tag) {
            // Change keep alive of ongoing connections to `false`
            for conn_id in self.peer_tracker.connections(peer_id) {
                self.swarm
                    .behaviour_mut()
                    .connection_control
                    .set_keep_alive(peer_id, conn_id, false);
            }
        }
    }

    async fn peer_health_check(&mut self) {
        let info = self.peer_tracker.info();
        let protected_full_nodes = self.peer_tracker.protected_len(FULL_PROTECT_TAG);
        let protected_archival_nodes = self.peer_tracker.protected_len(ARCHIVAL_PROTECT_TAG);

        if info.num_connected_peers == 0 {
            warn!("All peers disconnected");
            self.bootstrap();
        }

        // Based on the spec, when a protected node gets disconnected, the we unprotect
        // it (check `on_peer_disconnected`), and another node must be choosen to be
        // protected.
        if protected_full_nodes < FULL_NODES_PROTECT_LIMIT {
            // Protect the best full nodes to fill up the gap.
            self.protect_best_peers(
                FULL_NODES_PROTECT_LIMIT - protected_full_nodes,
                FULL_PROTECT_TAG,
                Peer::is_full,
            );

            // If we still have less than limit, we initiate full node discovery.
            if self.peer_tracker.protected_len(FULL_PROTECT_TAG) < FULL_NODES_PROTECT_LIMIT {
                self.start_full_node_kad_query();
            }
        }

        if protected_archival_nodes < ARCHIVAL_NODES_PROTECT_LIMIT {
            // Protect the best archival nodes to fill up the gap.
            self.protect_best_peers(
                ARCHIVAL_NODES_PROTECT_LIMIT - protected_archival_nodes,
                ARCHIVAL_PROTECT_TAG,
                Peer::is_archival,
            );

            // If we still have less than limit, we initiate archival node discovery.
            if self.peer_tracker.protected_len(ARCHIVAL_PROTECT_TAG) < ARCHIVAL_NODES_PROTECT_LIMIT
            {
                self.start_archival_node_kad_query();
            }
        }
    }

    /// Protect the best N peers based on their ping latency.
    fn protect_best_peers<F>(&mut self, count: usize, tag: u32, condition: F)
    where
        F: Fn(&Peer) -> bool,
    {
        // Select peers that:
        //
        // * Satisfy `condition`
        // * Are connected
        // * Not already protected for `tag`
        // * Have known ping latency
        let mut canditates = self
            .peer_tracker
            .peers()
            .filter_map(|peer| {
                if condition(peer) && peer.is_connected() && !peer.is_protected_with_tag(tag) {
                    Some((peer.id(), peer.best_ping()?))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // Sort them by ping
        canditates.sort_by_key(|(_, ping)| *ping);

        // Select the first N peers
        let to_be_protected = canditates
            .into_iter()
            .take(count)
            .map(|(peer_id, _)| *peer_id)
            .collect::<Vec<_>>();

        // Protect them
        for peer_id in to_be_protected {
            self.protect(&peer_id, tag);
        }
    }

    fn on_swarm_event(&mut self, ev: SwarmEvent<SwarmBehaviourEvent<B>>) -> Option<B::ToSwarm> {
        match ev {
            SwarmEvent::Behaviour(ev) => match ev {
                SwarmBehaviourEvent::Identify(ev) => self.on_identify_event(ev),
                SwarmBehaviourEvent::Kademlia(ev) => self.on_kademlia_event(ev),
                SwarmBehaviourEvent::Ping(ev) => self.on_ping_event(ev),
                SwarmBehaviourEvent::Stream(_)
                | SwarmBehaviourEvent::ConnectionControl(_)
                | SwarmBehaviourEvent::Autonat(_) => {}
                SwarmBehaviourEvent::Behaviour(ev) => return Some(ev),
            },
            SwarmEvent::ConnectionEstablished {
                peer_id,
                connection_id,
                ..
            } => {
                self.on_peer_connected(&peer_id, connection_id);
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                connection_id,
                ..
            } => {
                self.on_peer_disconnected(&peer_id, connection_id);
            }
            _ => {}
        }

        None
    }

    pub(crate) fn peer_maybe_discovered(&mut self, peer_id: &PeerId) -> bool {
        if !self.peer_tracker.add_peer_id(peer_id) {
            return false;
        }

        debug!("Peer discovered: {peer_id}");
        true
    }

    pub(crate) fn blacklist_peer(&mut self, peer_id: &PeerId) -> bool {
        if !self.swarm.behaviour_mut().blacklist.block_peer(*peer_id) {
            return false;
        }
        debug!("Peer blacklisted: {peer_id}");
        true
    }

    fn on_peer_connected(&mut self, peer_id: &PeerId, connection_id: ConnectionId) {
        debug!("Peer connected: {peer_id}");
        self.peer_tracker.add_connection(peer_id, connection_id);

        if self.peer_tracker.is_protected(peer_id) {
            // Connection should be protected from closing down on idle
            self.swarm
                .behaviour_mut()
                .connection_control
                .set_keep_alive(peer_id, connection_id, true);
        }

        if !self.first_connection_established {
            self.first_connection_established = true;
            self.peer_health_check_interval = Interval::new(PEER_HEALTH_CHECK_INTERVAL);
        }
    }

    fn on_peer_disconnected(&mut self, peer_id: &PeerId, connection_id: ConnectionId) {
        self.peer_tracker.remove_connection(peer_id, connection_id);

        if !self.peer_tracker.is_connected(peer_id) {
            debug!("Peer disconnected: {peer_id}");
            self.unprotect(peer_id, FULL_PROTECT_TAG);
            self.unprotect(peer_id, ARCHIVAL_PROTECT_TAG);
        }
    }

    #[instrument(level = "trace", skip(self))]
    fn on_identify_event(&mut self, ev: identify::Event) {
        match ev {
            identify::Event::Received { peer_id, info, .. } => {
                self.peer_tracker
                    .on_agent_version(&peer_id, &info.agent_version);

                // Inform Kademlia about the listening addresses
                // TODO: Remove this when rust-libp2p#5313 is implemented
                for addr in info.listen_addrs {
                    self.swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&peer_id, addr);
                }
            }
            _ => trace!("Unhandled identify event"),
        }
    }

    #[instrument(level = "trace", skip(self))]
    fn on_kademlia_event(&mut self, ev: kad::Event) {
        match ev {
            kad::Event::OutboundQueryProgressed {
                result:
                    kad::QueryResult::GetProviders(Ok(kad::GetProvidersOk::FoundProviders {
                        key,
                        providers,
                    })),
                ..
            } => {
                for p in &providers {
                    if key == *FULL_NODE_TOPIC {
                        if self.peer_tracker.protected_len(FULL_PROTECT_TAG)
                            < FULL_NODES_PROTECT_LIMIT
                        {
                            self.find_node_and_connect(p);
                        }
                    } else if key == *ARCHIVAL_NODE_TOPIC {
                        self.peer_tracker.mark_as_archival(p);

                        if self.peer_tracker.protected_len(ARCHIVAL_PROTECT_TAG)
                            < ARCHIVAL_NODES_PROTECT_LIMIT
                        {
                            self.find_node_and_connect(p);
                        }
                    }
                }
            }
            _ => trace!("Unhandled Kademlia event"),
        }
    }

    #[instrument(level = "debug", skip_all)]
    fn on_ping_event(&mut self, ev: ping::Event) {
        self.peer_tracker.on_ping_event(&ev);

        match ev.result {
            Ok(dur) => debug!(
                "Ping success: peer: {}, connection_id: {}, time: {:?}",
                ev.peer, ev.connection, dur
            ),
            Err(e) => {
                debug!(
                    "Ping failure: peer: {}, connection_id: {}, error: {}",
                    &ev.peer, &ev.connection, e
                );
                self.swarm.close_connection(ev.connection);
            }
        }
    }

    pub(crate) async fn stop(&mut self) {
        self.swarm
            .behaviour_mut()
            .connection_control
            .set_stopping(true);

        for listener in self.listeners.drain(..) {
            self.swarm.remove_listener(listener);
        }

        for (_, connection_id) in self.peer_tracker.all_connections() {
            self.swarm.close_connection(connection_id);
        }

        // Waiting until all established connections are closed.
        while self
            .swarm
            .network_info()
            .connection_counters()
            .num_established()
            > 0
        {
            match self.swarm.select_next_some().await {
                // We may receive this if connection was established just before we trigger stop.
                SwarmEvent::ConnectionEstablished { connection_id, .. } => {
                    // We immediately close the connection in this case.
                    self.swarm.close_connection(connection_id);
                }
                SwarmEvent::ConnectionClosed {
                    peer_id,
                    connection_id,
                    ..
                } => {
                    // This will generate the PeerDisconnected events.
                    self.on_peer_disconnected(&peer_id, connection_id);
                }
                _ => {}
            }
        }
    }
}

fn init_kademlia(
    network_id: &str,
    keypair: &Keypair,
    listen_on: &[Multiaddr],
) -> Result<kad::Behaviour<kad::store::MemoryStore>> {
    let local_peer_id = PeerId::from(keypair.public());
    let store = kad::store::MemoryStore::new(local_peer_id);

    let protocol_id = celestia_protocol_id(network_id, "/kad/1.0.0");
    let config = kad::Config::new(protocol_id);

    let mut kademlia = kad::Behaviour::with_config(local_peer_id, store, config);

    if !listen_on.is_empty() {
        kademlia.set_mode(Some(kad::Mode::Server));
    }

    Ok(kademlia)
}

/// Converts a topic to `RecordKey`.
///
/// This is the equivalent of `nsToCid` that is used in [`RoutingDiscovery.FindPeers`][1]
/// of go-libp2p which later on is unwraped to internal hash in [`IpfsDHT.FindProvidersAsync`][2].
///
/// [1]: https://github.com/libp2p/go-libp2p/blob/f6c14a215b2012f3839f1b7157dfec70a772143a/p2p/discovery/routing/routing.go#L75
/// [2]: https://github.com/libp2p/go-libp2p-kad-dht/blob/944883ea5a55102c8950478645d89183901859b4/routing.go#L504
pub(crate) fn dht_topic(topic: &str) -> RecordKey {
    Code::Sha2_256.digest(topic.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cid::Cid;

    #[test]
    fn dht_key() {
        let key = dht_topic("/full/v0.1.0");
        let key_vec = dht_topic("/full/v0.1.0").to_vec();
        let expected = "bafkreidjoruznlfsmvecpvipnfpoe4jehgjjd753qob53bo77se6whba34"
            .parse::<Cid>()
            .unwrap();

        assert_eq!(key.as_ref(), &expected.hash().to_bytes());
        assert_eq!(key_vec.as_slice(), &expected.hash().to_bytes());
    }
}
