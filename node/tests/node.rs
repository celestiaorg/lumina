use celestia_types::consts::HASH_SIZE;
use celestia_types::hash::Hash;
use lumina_node::test_utils::{gen_filled_store, test_node_builder};
use lumina_utils::test_utils::async_test;
use rand::Rng;

use crate::utils::new_connected_node;

mod utils;

#[async_test]
async fn connects_to_the_go_bridge_node() {
    let (node, _) = new_connected_node().await;

    let info = node.network_info().await.unwrap();
    assert!(info.num_peers() >= 1);
}

#[async_test]
async fn header_store_access() {
    let (store, _) = gen_filled_store(100).await;
    let node = test_node_builder().store(store).start().await.unwrap();

    // check local head
    let head = node.get_local_head_header().await.unwrap();
    let expected_head = node.get_header_by_height(100).await.unwrap();
    assert_eq!(head, expected_head);

    // check getting existing headers
    for height in 1..100 {
        let header_by_height = node.get_header_by_height(height).await.unwrap();
        let header_by_hash = node
            .get_header_by_hash(&header_by_height.hash())
            .await
            .unwrap();

        assert_eq!(header_by_height, header_by_hash);

        // check range requests
        let start = height + 1;
        let amount = rand::thread_rng().gen_range(1..50);
        let res = node.get_headers(start..start + amount).await;

        if height + amount > 100 {
            // errors out if exceeded store
            res.unwrap_err();
        } else {
            // returns continuous range of headers
            assert!(
                res.unwrap()
                    .into_iter()
                    .zip(start..start + amount)
                    .all(|(header, height)| header.height() == height)
            );
        }
    }

    // check getting non existing headers
    for _ in 0..100 {
        // by height
        let height = rand::thread_rng().gen_range(100..u64::MAX);
        node.get_header_by_height(height).await.unwrap_err();

        // by hash
        let mut hash = [0u8; HASH_SIZE];
        rand::thread_rng().fill(&mut hash);
        node.get_header_by_hash(&Hash::Sha256(hash))
            .await
            .unwrap_err();
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn peer_discovery() {
    use std::time::Duration;

    use lumina_node::test_utils::listening_test_node_builder;
    use tokio::time::sleep;

    use crate::utils::fetch_bridge_info;

    // Bridge node cannot connect to other nodes because it is behind Docker's NAT.
    // However Node2 and Node3 can discover its address via Node1.
    let (bridge_peer_id, bridge_addrs) = fetch_bridge_info().await;

    // Node1
    //
    // This node connects to Bridge node.
    let node1 = listening_test_node_builder()
        .bootnodes(bridge_addrs)
        .start()
        .await
        .unwrap();

    node1.wait_connected().await.unwrap();

    let node1_addrs = node1.listeners().await.unwrap();

    // Node2
    //
    // This node connects to Node1 and will discover Bridge node.
    let node2 = listening_test_node_builder()
        .bootnodes(node1_addrs.clone())
        .start()
        .await
        .unwrap();

    node2.wait_connected().await.unwrap();

    // Node3
    //
    // This node connects to Node1 and will discover Node2 and Bridge node.
    let node3 = listening_test_node_builder()
        .bootnodes(node1_addrs)
        .start()
        .await
        .unwrap();

    node3.wait_connected().await.unwrap();

    // Small wait until all nodes are discovered and connected
    sleep(Duration::from_millis(2000)).await;

    let node1_peer_id = node1.local_peer_id();
    let node2_peer_id = node2.local_peer_id();
    let node3_peer_id = node3.local_peer_id();

    // Check Node1 connected peers
    let connected_peers = node1.connected_peers().await.unwrap();
    let tracker_info = node1.peer_tracker_info();
    assert!(connected_peers.contains(&bridge_peer_id));
    assert!(connected_peers.contains(node2_peer_id));
    assert!(connected_peers.contains(node3_peer_id));
    assert!(tracker_info.num_connected_peers >= 3);
    assert_eq!(tracker_info.num_connected_trusted_peers, 1);

    // Check Node2 connected peers
    let connected_peers = node2.connected_peers().await.unwrap();
    let tracker_info = node2.peer_tracker_info();
    assert!(connected_peers.contains(&bridge_peer_id));
    assert!(connected_peers.contains(node1_peer_id));
    assert!(connected_peers.contains(node3_peer_id));
    assert!(tracker_info.num_connected_peers >= 3);
    assert_eq!(tracker_info.num_connected_trusted_peers, 1);

    // Check Node3 connected peers
    let connected_peers = node3.connected_peers().await.unwrap();
    let tracker_info = node2.peer_tracker_info();
    assert!(connected_peers.contains(&bridge_peer_id));
    assert!(connected_peers.contains(node1_peer_id));
    assert!(connected_peers.contains(node2_peer_id));
    assert!(tracker_info.num_connected_peers >= 3);
    assert_eq!(tracker_info.num_connected_trusted_peers, 1);
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn stops_services_when_network_is_compromised() {
    use std::time::Duration;

    use tokio::time::sleep;

    use celestia_types::AppVersion;
    use celestia_types::test_utils::{ExtendedHeaderGenerator, corrupt_eds, generate_dummy_eds};
    use lumina_node::store::{InMemoryStore, Store};
    use lumina_node::test_utils::{ExtendedHeaderGeneratorExt, listening_test_node_builder};

    let mut generator = ExtendedHeaderGenerator::new();
    let store = InMemoryStore::new();

    // add some initial headers
    store
        .insert(generator.next_many_verified(64))
        .await
        .unwrap();

    // create a corrupted block and insert it
    let mut eds = generate_dummy_eds(8, AppVersion::V2);
    let (header, befp) = corrupt_eds(&mut generator, &mut eds);

    store.insert(header).await.unwrap();

    // spawn node
    let node = listening_test_node_builder()
        .store(store)
        .start()
        .await
        .unwrap();

    // get the address to dial
    sleep(Duration::from_millis(300)).await;
    let listener_addr = node.listeners().await.unwrap()[0].clone();

    // spawn a proof broadcaster
    let befp_announce_tx = spawn_befp_announcer(listener_addr);
    sleep(Duration::from_millis(300)).await;

    // node services are running
    // TODO: also check the daser and blob submit
    assert!(node.syncer_info().await.is_ok());

    // announce befp
    befp_announce_tx.send(befp).await.unwrap();
    sleep(Duration::from_millis(300)).await;

    // node services are stopped
    // TODO: also check the daser and blob submit
    assert!(node.syncer_info().await.is_err());
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_befp_announcer(
    connect_to: libp2p::Multiaddr,
) -> tokio::sync::mpsc::Sender<celestia_types::fraud_proof::BadEncodingFraudProof> {
    use celestia_types::fraud_proof::BadEncodingFraudProof;
    use futures::StreamExt;
    use libp2p::swarm::NetworkBehaviour;
    use libp2p::{SwarmBuilder, gossipsub, noise, ping, tcp, yamux};
    use tendermint_proto::Protobuf;
    use tokio::sync::mpsc;
    use tokio::{select, spawn};

    #[derive(NetworkBehaviour)]
    struct Behaviour {
        ping: ping::Behaviour,
        gossipsub: gossipsub::Behaviour,
    }

    // create a new libp2p node with gossipsub
    let mut announcer = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .unwrap()
        .with_behaviour(|key| {
            let ping = ping::Behaviour::new(ping::Config::default());

            let config = gossipsub::ConfigBuilder::default().build().unwrap();
            let message_authenticity = gossipsub::MessageAuthenticity::Signed(key.clone());
            let gossipsub: gossipsub::Behaviour =
                gossipsub::Behaviour::new(message_authenticity, config).unwrap();

            Ok(Behaviour { ping, gossipsub })
        })
        .unwrap()
        .build();

    announcer.dial(connect_to).unwrap();

    // subscribe to the fraud-sub topic
    let topic = gossipsub::IdentTopic::new("/badencoding/fraud-sub/private/v0.0.1");
    announcer
        .behaviour_mut()
        .gossipsub
        .subscribe(&topic)
        .unwrap();

    // a channel for proof announcment
    let (tx, mut rx) = mpsc::channel::<BadEncodingFraudProof>(8);

    spawn(async move {
        loop {
            select! {
                _ = announcer.select_next_some() => (),
                Some(proof) = rx.recv() => {
                    let proof = proof.encode_vec();
                    announcer.behaviour_mut().gossipsub.publish(topic.hash(), proof).unwrap();
                }
            }
        }
    });

    tx
}
