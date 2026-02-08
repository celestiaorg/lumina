#![allow(dead_code)]

use std::sync::OnceLock;
use std::time::Duration;

use blockstore::Blockstore;
use celestia_rpc::{Client, TxConfig, prelude::*};
use celestia_types::Blob;
use libp2p::identity::Keypair;
use libp2p::{Multiaddr, PeerId, multiaddr::Protocol};
use lumina_node::NodeBuilder;
use lumina_node::blockstore::InMemoryBlockstore;
use lumina_node::events::EventSubscriber;
use lumina_node::node::Node;
use lumina_node::store::{InMemoryStore, Store};
use lumina_node::test_utils::test_node_builder;
use lumina_utils::test_utils::env_var;
use lumina_utils::time::sleep;
use tokio::sync::Mutex;

#[cfg(not(target_arch = "wasm32"))]
const RPC_URL: &str = "ws://localhost:36658";
#[cfg(target_arch = "wasm32")]
const RPC_URL: &str = "http://localhost:36658";

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

pub async fn bridge_client() -> Client {
    let token = env_var("CELESTIA_NODE_AUTH_TOKEN_ADMIN")
        .expect("Token not found, run ./tools/gen_auth_tokens.sh");
    Client::new(RPC_URL, Some(&token), None, None)
        .await
        .unwrap()
}

pub async fn fetch_bridge_info() -> (PeerId, Vec<Multiaddr>) {
    let client = bridge_client().await;
    let bridge_info = client.p2p_info().await.unwrap();

    let addrs = bridge_info
        .addrs
        .into_iter()
        .map(|mut ma| {
            if !ma.protocol_stack().any(|protocol| protocol == "p2p") {
                ma.push(Protocol::P2p(bridge_info.id.into()))
            }
            ma
        })
        .collect();

    (bridge_info.id.into(), addrs)
}

pub async fn new_connected_node_with_builder<B, S>(
    builder: NodeBuilder<B, S>,
) -> (Node<B, S>, EventSubscriber)
where
    B: Blockstore + 'static,
    S: Store + 'static,
{
    let (_, bridge_addrs) = fetch_bridge_info().await;

    // TODO: tried protecting peer id on the go side but doesn't help either
    let keypair = Keypair::generate_ed25519();
    let peer_id: PeerId = keypair.public().into();
    let client = bridge_client().await;
    client.p2p_protect(&peer_id.into(), "test").await.unwrap();

    let (node, events) = builder
        .keypair(keypair)
        .bootnodes(bridge_addrs)
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
    new_connected_node_with_builder(test_node_builder()).await
}

pub async fn blob_submit(client: &Client, blobs: &[Blob]) -> u64 {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = LOCK.get_or_init(|| Mutex::new(())).lock().await;
    client
        .blob_submit(blobs, TxConfig::default())
        .await
        .unwrap()
}
