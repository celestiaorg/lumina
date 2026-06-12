#![cfg(not(target_arch = "wasm32"))]

use libp2p::identity::Keypair;
use lumina_node::{NodeBuilder, store::RedbStore};
use tempfile::TempDir;

// Pulls in the shared test setup, including the `#[ctor]` that installs the tracing
// subscriber for this test binary.
mod utils;

#[tokio::test]
async fn persists_identity() {
    let db_dir = TempDir::with_prefix("lumina.persistance.test")
        .unwrap()
        .keep()
        .join("db");

    let store = RedbStore::open(&db_dir).await.unwrap();
    let node = NodeBuilder::new()
        .network(lumina_node::network::Network::Mocha)
        .store(store)
        .start()
        .await
        .unwrap();
    let peer_id_0 = *node.local_peer_id();
    node.stop().await;

    let store = RedbStore::open(&db_dir).await.unwrap();
    let node = NodeBuilder::new()
        .network(lumina_node::network::Network::Mocha)
        .store(store)
        .start()
        .await
        .unwrap();
    let peer_id_1 = *node.local_peer_id();
    node.stop().await;

    assert_eq!(peer_id_0, peer_id_1);
}

#[tokio::test]
async fn identity_override() {
    let db_dir = TempDir::with_prefix("lumina.persistance.test")
        .unwrap()
        .keep()
        .join("db");

    let store = RedbStore::open(&db_dir).await.unwrap();
    let node = NodeBuilder::new()
        .network(lumina_node::network::Network::Mocha)
        .store(store)
        .start()
        .await
        .unwrap();
    let peer_id_0 = *node.local_peer_id();
    node.stop().await;

    let keypair = Keypair::generate_ed25519();
    let expected_peer_id = keypair.public().to_peer_id();

    let store = RedbStore::open(&db_dir).await.unwrap();
    let node = NodeBuilder::new()
        .network(lumina_node::network::Network::Mocha)
        .keypair(keypair)
        .store(store)
        .start()
        .await
        .unwrap();
    let peer_id_1 = *node.local_peer_id();
    node.stop().await;

    assert_eq!(expected_peer_id, peer_id_1);
    assert_ne!(peer_id_0, peer_id_1);
}
