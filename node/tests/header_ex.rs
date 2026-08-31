#![cfg(not(target_arch = "wasm32"))]

use std::time::{Duration, Instant};

use celestia_types::ExtendedHeader;
use celestia_types::test_utils::{invalidate, unverify};
use libp2p::PeerId;
use lumina_node::{
    blockstore::InMemoryBlockstore,
    node::{HeaderExError, Node, NodeError, P2pError},
    store::{InMemoryStore, Store, VerifiedExtendedHeaders},
    test_utils::{gen_filled_store, listening_test_node_builder, test_node_builder},
};
use tokio::time::{sleep, timeout};

use crate::utils::new_connected_node;

mod utils;

#[tokio::test]
async fn request_single_header() {
    let (node, _) = new_connected_node().await;

    let header = node.request_header_by_height(1).await.unwrap();
    let header_by_hash = node.request_header_by_hash(&header.hash()).await.unwrap();

    assert_eq!(header, header_by_hash);
}

#[tokio::test]
async fn request_verified_headers() {
    let (node, _) = new_connected_node().await;

    let from = node.request_header_by_height(1).await.unwrap();
    let verified_headers = node.request_verified_headers(&from, 2).await.unwrap();
    assert_eq!(verified_headers.len(), 2);

    let height2 = node.request_header_by_height(2).await.unwrap();
    assert_eq!(verified_headers[0], height2);

    let height3 = node.request_header_by_height(3).await.unwrap();
    assert_eq!(verified_headers[1], height3);
}

#[tokio::test]
async fn request_head() {
    let (node, _) = new_connected_node().await;

    let genesis = node.request_header_by_height(1).await.unwrap();

    let head1 = node.request_head_header().await.unwrap();
    genesis.verify(&head1).unwrap();

    let head2 = node.request_header_by_height(0).await.unwrap();
    assert!(head1 == head2 || head1.verify(&head2).is_ok());
}

#[tokio::test]
async fn client_server() {
    // Server Node
    let (server_store, mut header_generator) = gen_filled_store(0).await;
    let server_headers = header_generator.next_many(20);
    server_store.insert(&server_headers[..]).await.unwrap();

    let server = listening_test_node_builder()
        .store(server_store)
        .start()
        .await
        .unwrap();

    // give server a sec to breathe, otherwise occiasionally client has problems with connecting
    sleep(Duration::from_millis(100)).await;
    let server_addrs = server.listeners().await.unwrap();

    // Client node
    let client = test_node_builder()
        .bootnodes(server_addrs)
        .start()
        .await
        .unwrap();

    client
        .mark_as_archival(server.local_peer_id().to_owned())
        .await
        .unwrap();

    client.wait_connected().await.unwrap();

    // request head (with one peer)
    let received_head = client.request_head_header().await.unwrap();
    assert_eq!(server_headers.last().unwrap(), &received_head);

    // request by height
    let received_header_by_height = client.request_header_by_height(10).await.unwrap();
    assert_eq!(server_headers[9], received_header_by_height);

    // request by hash
    let expected_header = &server_headers[15];
    let received_header_by_hash = client
        .request_header_by_hash(&expected_header.hash())
        .await
        .unwrap();
    assert_eq!(expected_header, &received_header_by_hash);

    // request genesis by height
    let received_genesis = client.request_header_by_height(1).await.unwrap();
    assert_eq!(server_headers.first().unwrap(), &received_genesis);

    // request entire store range
    let received_all_headers = client
        .request_verified_headers(&received_genesis, 19)
        .await
        .unwrap();
    assert_eq!(server_headers[1..], received_all_headers);

    // reqest more headers than available in store
    timeout(
        Duration::from_millis(200),
        client.request_verified_headers(&received_genesis, 20),
    )
    .await
    .expect_err("sessions keep retrying until all headers are received");

    // request unknown hash
    let unstored_header = header_generator.next_of(&server_headers[0]);
    let unexpected_hash = client
        .request_header_by_hash(&unstored_header.hash())
        .await
        .unwrap_err();
    assert!(matches!(
        unexpected_hash,
        NodeError::P2p(P2pError::HeaderEx(HeaderExError::HeaderNotFound))
    ));

    // request unknown height
    let unexpected_height = client.request_header_by_height(21).await.unwrap_err();
    assert!(matches!(
        unexpected_height,
        NodeError::P2p(P2pError::HeaderEx(HeaderExError::HeaderNotFound))
    ));
}

#[tokio::test]
async fn head_selection_with_multiple_peers() {
    let (server_store, mut header_generator) = gen_filled_store(0).await;
    let common_server_headers = header_generator.next_many(20);
    server_store
        .insert(&common_server_headers[..])
        .await
        .unwrap();

    // Server group A, nodes with synced stores
    let mut servers = vec![
        listening_test_node_builder()
            .store(server_store.async_clone().await)
            .start()
            .await
            .unwrap(),
        listening_test_node_builder()
            .store(server_store.async_clone().await)
            .start()
            .await
            .unwrap(),
        listening_test_node_builder()
            .store(server_store.async_clone().await)
            .start()
            .await
            .unwrap(),
    ];

    // Server group B, single node with additional headers
    let additional_server_headers = header_generator.next_many(5);
    server_store
        .insert(&additional_server_headers[..])
        .await
        .unwrap();

    servers.push(
        listening_test_node_builder()
            .store(server_store.async_clone().await)
            .start()
            .await
            .unwrap(),
    );

    // give server a sec to breathe, otherwise occiasionally client has problems with connecting
    sleep(Duration::from_millis(100)).await;

    let mut server_addrs = vec![];
    for s in &servers {
        server_addrs.extend_from_slice(&s.listeners().await.unwrap()[..]);
    }

    // Client Node
    let client = listening_test_node_builder()
        .bootnodes(server_addrs)
        .start()
        .await
        .unwrap();

    client.wait_connected().await.unwrap();

    // head selection needs the client's own view of the peers, so wait until
    // it registers all the servers
    let server_ids = servers
        .iter()
        .map(|s| s.local_peer_id().to_owned())
        .collect::<Vec<_>>();
    wait_peers_connected(&client, &server_ids).await;

    // give client node a sec to breathe, otherwise occiasionally rogue node has problems with connecting
    sleep(Duration::from_millis(100)).await;
    let client_addr = client.listeners().await.unwrap();

    // Rogue node, connects to client so isn't trusted
    let rogue_node = listening_test_node_builder()
        .store(gen_filled_store(26).await.0)
        .bootnodes(client_addr.clone())
        .start()
        .await
        .unwrap();

    rogue_node.wait_connected().await.unwrap();
    // wait for client to include rogue_node in head selection process
    wait_peers_connected(&client, &[rogue_node.local_peer_id().to_owned()]).await;

    // client should prefer heighest head received from 2+ peers
    let expected_head = common_server_headers.last().unwrap();
    let network_head = request_head_with_retries(&client, expected_head).await;
    assert_eq!(expected_head, &network_head);

    // new node from group B joins, head should go up
    let new_b_node = test_node_builder()
        .store(server_store.async_clone().await)
        .bootnodes(client_addr)
        .start()
        .await
        .unwrap();

    // Head requests are send only to trusted peers, so we add
    // `new_b_node` as trusted.
    let new_b_peer_id = new_b_node.local_peer_id().to_owned();
    client.set_peer_trust(new_b_peer_id, true).await.unwrap();

    new_b_node.wait_connected().await.unwrap();
    // wait for client to include new_b_node in head selection process
    wait_peers_connected(&client, &[new_b_peer_id]).await;

    // now 2 nodes agree on head with height 25
    let expected_head = additional_server_headers.last().unwrap();
    let network_head = request_head_with_retries(&client, expected_head).await;
    assert_eq!(expected_head, &network_head);
}

/// Request the network head, retrying until `expected` is returned or the
/// deadline passes. The last received head is returned either way, so the
/// caller can `assert_eq!` on it.
///
/// A single request is not deterministic even when all the peers are connected
/// and trusted: it may join a HEAD round that is still in flight and was
/// scheduled with an older set of peers (e.g. syncer's initialization round),
/// and individual responses within a round can fail without being retried.
async fn request_head_with_retries(
    node: &Node<InMemoryBlockstore, InMemoryStore>,
    expected: &ExtendedHeader,
) -> ExtendedHeader {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let head = node.request_head_header().await.unwrap();
        if &head == expected || Instant::now() >= deadline {
            return head;
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until `node` sees all `peers` as connected.
///
/// `wait_connected()` on the other end of a connection is not enough: it only
/// says that the *peer's* swarm registered the connection, while `node`
/// processes its own `ConnectionEstablished` event asynchronously and can lag
/// behind under load. Peer selection (e.g. for HEAD requests) uses `node`'s
/// own view, so tests must wait for it.
async fn wait_peers_connected(node: &Node<InMemoryBlockstore, InMemoryStore>, peers: &[PeerId]) {
    timeout(Duration::from_secs(10), async {
        loop {
            let connected = node.connected_peers().await.unwrap();
            if peers.iter().all(|peer| connected.contains(peer)) {
                return;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("peers didn't connect in time");
}

#[tokio::test]
async fn replaced_header_server_store() {
    // Server node, header at height 11 shouldn't pass verification as it's been tampered with
    let (server_store, mut header_generator) = gen_filled_store(0).await;
    let mut server_headers = header_generator.next_many(20);
    // replaced header still pases verification and validation against itself
    let replaced_header = header_generator.another_of(&server_headers[10]);
    server_headers[10] = replaced_header.clone();

    server_store
        .insert(unsafe { VerifiedExtendedHeaders::new_unchecked(server_headers.clone()) })
        .await
        .unwrap();

    let server = listening_test_node_builder()
        .store(server_store)
        .start()
        .await
        .unwrap();

    // give server a sec to breathe, otherwise occiasionally client has problems with connecting
    sleep(Duration::from_millis(100)).await;
    let server_addrs = server.listeners().await.unwrap();

    let client = listening_test_node_builder()
        .bootnodes(server_addrs)
        .start()
        .await
        .unwrap();

    client.wait_connected().await.unwrap();

    let tampered_header_in_range = client
        .request_verified_headers(&server_headers[9], 5)
        .await
        .unwrap_err();
    assert!(matches!(
        tampered_header_in_range,
        NodeError::P2p(P2pError::HeaderEx(HeaderExError::InvalidResponse))
    ));

    let requested_from_tampered_header = client
        .request_verified_headers(&replaced_header, 1)
        .await
        .unwrap_err();
    assert!(matches!(
        requested_from_tampered_header,
        NodeError::P2p(P2pError::HeaderEx(HeaderExError::InvalidResponse))
    ));

    let requested_tampered_header = client
        .request_header_by_hash(&replaced_header.hash())
        .await
        .unwrap();
    assert_eq!(requested_tampered_header, replaced_header);

    let network_head = client.request_head_header().await.unwrap();
    assert_eq!(server_headers.last().unwrap(), &network_head);
}

#[tokio::test]
async fn invalidated_header_server_store() {
    // Server node, header at height 11 shouldn't pass verification as it's been tampered with
    let (server_store, mut header_generator) = gen_filled_store(0).await;
    let mut server_headers = header_generator.next_many(20);
    invalidate(&mut server_headers[10]);

    server_store.insert(&server_headers[..]).await.unwrap();

    let server = listening_test_node_builder()
        .store(server_store)
        .start()
        .await
        .unwrap();

    // give server a sec to breathe, otherwise occiasionally client has problems with connecting
    sleep(Duration::from_millis(100)).await;
    let server_addrs = server.listeners().await.unwrap();

    let client = listening_test_node_builder()
        .bootnodes(server_addrs)
        .start()
        .await
        .unwrap();

    client
        .mark_as_archival(server.local_peer_id().to_owned())
        .await
        .unwrap();

    client.wait_connected().await.unwrap();

    timeout(
        Duration::from_millis(200),
        client.request_verified_headers(&server_headers[9], 5),
    )
    .await
    .expect_err("session never stops retrying on invalid header");

    let requested_from_invalidated_header = client
        .request_verified_headers(&server_headers[10], 3)
        .await
        .unwrap_err();
    assert!(matches!(
        requested_from_invalidated_header,
        NodeError::P2p(P2pError::HeaderEx(HeaderExError::InvalidRequest))
    ));

    let requested_tampered_header = client
        .request_header_by_hash(&server_headers[10].hash())
        .await
        .unwrap_err();
    assert!(matches!(
        requested_tampered_header,
        NodeError::P2p(P2pError::HeaderEx(HeaderExError::InvalidResponse))
    ));

    // requests for non-invalidated headers should still pass
    let valid_header = client.request_header_by_height(10).await.unwrap();
    assert_eq!(server_headers[9], valid_header);
}

#[tokio::test]
async fn unverified_header_server_store() {
    // Server node, header at height 11 shouldn't pass verification as it's been tampered with
    let (server_store, mut header_generator) = gen_filled_store(0).await;
    let mut server_headers = header_generator.next_many(20);
    unverify(&mut server_headers[10]);

    server_store
        .insert(unsafe { VerifiedExtendedHeaders::new_unchecked(server_headers.clone()) })
        .await
        .unwrap();

    let server = listening_test_node_builder()
        .store(server_store)
        .start()
        .await
        .unwrap();

    // give server a sec to breathe, otherwise occiasionally client has problems with connecting
    sleep(Duration::from_millis(100)).await;
    let server_addrs = server.listeners().await.unwrap();

    let client = listening_test_node_builder()
        .bootnodes(server_addrs)
        .start()
        .await
        .unwrap();

    client.wait_connected().await.unwrap();

    let tampered_header_in_range = client
        .request_verified_headers(&server_headers[9], 5)
        .await
        .unwrap_err();
    assert!(matches!(
        tampered_header_in_range,
        NodeError::P2p(P2pError::HeaderEx(HeaderExError::InvalidResponse))
    ));

    let requested_from_tampered_header = client
        .request_verified_headers(&server_headers[10], 3)
        .await
        .unwrap_err();
    assert!(matches!(
        requested_from_tampered_header,
        NodeError::P2p(P2pError::HeaderEx(HeaderExError::InvalidResponse))
    ));

    let requested_tampered_header = client
        .request_header_by_hash(&server_headers[10].hash())
        .await
        .unwrap();
    assert_eq!(requested_tampered_header, server_headers[10]);

    let network_head = client.request_head_header().await.unwrap();
    assert_eq!(server_headers.last().unwrap(), &network_head);
}
