#![allow(dead_code)]

use std::sync::OnceLock;
use std::time::Duration;

use blockstore::Blockstore;
use celestia_rpc::{Client, TxConfig, prelude::*};
use celestia_types::Blob;
use libp2p::{Multiaddr, PeerId, multiaddr::Protocol};
use lumina_node::NodeBuilder;
use lumina_node::blockstore::InMemoryBlockstore;
use lumina_node::events::EventSubscriber;
use lumina_node::node::Node;
use lumina_node::store::{InMemoryStore, Store};
use lumina_node::test_utils::test_node_builder;
use tokio::sync::Mutex;
use tokio::time::sleep;

const WS_URL: &str = "ws://localhost:26658";

pub async fn bridge_client() -> Client {
    Client::new(WS_URL, None, None, None).await.unwrap()
}

pub async fn fetch_bridge_info() -> (PeerId, Multiaddr) {
    let client = bridge_client().await;
    let bridge_info = client.p2p_info().await.unwrap();

    let mut ma = bridge_info
        .addrs
        .into_iter()
        .find(|ma| ma.protocol_stack().any(|protocol| protocol == "tcp"))
        .expect("Bridge doesn't listen on tcp");

    if !ma.protocol_stack().any(|protocol| protocol == "p2p") {
        ma.push(Protocol::P2p(bridge_info.id.into()))
    }

    (bridge_info.id.into(), ma)
}

pub async fn new_connected_node_with_builder<B, S>(
    builder: NodeBuilder<B, S>,
) -> (Node<B, S>, EventSubscriber)
where
    B: Blockstore + 'static,
    S: Store + 'static,
{
    let (_, bridge_ma) = fetch_bridge_info().await;

    let (node, events) = builder
        .bootnodes([bridge_ma])
        .start_subscribed()
        .await
        .unwrap();

    node.wait_connected_trusted().await.unwrap();

    // Wait until node reaches height 3
    loop {
        if node
            .get_network_head_header()
            .await
            .unwrap()
            .is_some_and(|head| head.height() >= 3)
        {
            break;
        }

        sleep(Duration::from_secs(1)).await;
    }

    (node, events)
}

pub async fn new_connected_node() -> (Node<InMemoryBlockstore, InMemoryStore>, EventSubscriber) {
    new_connected_node_with_builder(test_node_builder().pruning_window(Duration::from_secs(60 * 60))).await
}

pub async fn blob_submit(client: &Client, blobs: &[Blob]) -> u64 {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = LOCK.get_or_init(|| Mutex::new(())).lock().await;
    client
        .blob_submit(blobs, TxConfig::default())
        .await
        .unwrap()
}
