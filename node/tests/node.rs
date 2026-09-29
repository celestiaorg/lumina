#![cfg(not(target_arch = "wasm32"))]

use std::time::Duration;

use celestia_types::consts::HASH_SIZE;
use celestia_types::hash::Hash;
use lumina_node::test_utils::{
    gen_filled_store, listening_test_node_builder, test_node_builder, wait_for_listeners,
};
use rand::Rng;
use tokio::time::{sleep, timeout};

use crate::utils::{fetch_bridge_info, new_connected_node};

mod utils;

#[tokio::test]
async fn connects_to_the_go_bridge_node() {
    let (node, _) = new_connected_node().await;

    let info = node.network_info().await.unwrap();
    assert!(info.num_peers() >= 1);
}

#[tokio::test]
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

#[tokio::test]
async fn peer_discovery() {
    // Bridge node cannot connect to other nodes because it is behind Docker's NAT.
    // However Node2 and Node3 can discover its address via Node1.
    let (bridge_peer_id, bridge_ma) = fetch_bridge_info().await;

    // Node1
    //
    // This node connects to Bridge node.
    let node1 = listening_test_node_builder()
        .bootnodes([bridge_ma])
        .start()
        .await
        .unwrap();

    timeout(Duration::from_secs(30), node1.wait_connected())
        .await
        .expect("node1 did not connect within 30s — is the devnet reachable? (docker compose -f ci/docker-compose.yml up)")
        .unwrap();

    let node1_addrs = wait_for_listeners(&node1).await;

    // Node2
    //
    // This node connects to Node1 and will discover Bridge node.
    let node2 = listening_test_node_builder()
        .bootnodes(node1_addrs.clone())
        .start()
        .await
        .unwrap();

    timeout(Duration::from_secs(30), node2.wait_connected())
        .await
        .expect("node2 did not connect to node1 within 30s")
        .unwrap();

    // Node3
    //
    // This node connects to Node1 and will discover Node2 and Bridge node.
    let node3 = listening_test_node_builder()
        .bootnodes(node1_addrs)
        .start()
        .await
        .unwrap();

    timeout(Duration::from_secs(30), node3.wait_connected())
        .await
        .expect("node3 did not connect to node1 within 30s")
        .unwrap();

    let node1_peer_id = *node1.local_peer_id();
    let node2_peer_id = *node2.local_peer_id();
    let node3_peer_id = *node3.local_peer_id();

    // Wait until all nodes have discovered and connected to each other.
    //
    // Discovery is transitive (Node2/Node3 learn the bridge's address via
    // Node1) and its timing depends on Kademlia and identify round-trips,
    // which vary with machine load. Poll for the observed end-state instead
    // of sleeping a fixed amount, otherwise the test is flaky on slow CI.
    let mesh_formed = timeout(Duration::from_secs(30), async {
        loop {
            let n1 = node1.connected_peers().await.unwrap();
            let n2 = node2.connected_peers().await.unwrap();
            let n3 = node3.connected_peers().await.unwrap();

            let n1_ok = n1.contains(&bridge_peer_id)
                && n1.contains(&node2_peer_id)
                && n1.contains(&node3_peer_id);
            let n2_ok = n2.contains(&bridge_peer_id)
                && n2.contains(&node1_peer_id)
                && n2.contains(&node3_peer_id);
            let n3_ok = n3.contains(&bridge_peer_id)
                && n3.contains(&node1_peer_id)
                && n3.contains(&node2_peer_id);

            if n1_ok && n2_ok && n3_ok {
                break;
            }

            sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    assert!(
        mesh_formed.is_ok(),
        "Timed out waiting for all nodes to discover and connect to each other"
    );

    // Check Node1 connected peers
    let connected_peers = node1.connected_peers().await.unwrap();
    let tracker_info = node1.peer_tracker_info();
    assert!(connected_peers.contains(&bridge_peer_id));
    assert!(connected_peers.contains(&node2_peer_id));
    assert!(connected_peers.contains(&node3_peer_id));
    assert!(tracker_info.num_connected_peers >= 3);
    assert_eq!(tracker_info.num_connected_trusted_peers, 1);

    // Check Node2 connected peers
    let connected_peers = node2.connected_peers().await.unwrap();
    let tracker_info = node2.peer_tracker_info();
    assert!(connected_peers.contains(&bridge_peer_id));
    assert!(connected_peers.contains(&node1_peer_id));
    assert!(connected_peers.contains(&node3_peer_id));
    assert!(tracker_info.num_connected_peers >= 3);
    assert_eq!(tracker_info.num_connected_trusted_peers, 1);

    // Check Node3 connected peers
    let connected_peers = node3.connected_peers().await.unwrap();
    let tracker_info = node3.peer_tracker_info();
    assert!(connected_peers.contains(&bridge_peer_id));
    assert!(connected_peers.contains(&node1_peer_id));
    assert!(connected_peers.contains(&node2_peer_id));
    assert!(tracker_info.num_connected_peers >= 3);
    assert_eq!(tracker_info.num_connected_trusted_peers, 1);
}
