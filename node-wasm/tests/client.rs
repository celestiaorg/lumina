#![cfg(target_arch = "wasm32")]

use std::cmp::Ordering;
use std::time::Duration;

use celestia_rpc::TxConfig;
use celestia_rpc::p2p::PeerId;
use celestia_rpc::prelude::*;
use celestia_types::nmt::Namespace;
use celestia_types::{AppVersion, Blob};
use futures::FutureExt;
use gloo_timers::future::sleep;
use lumina_node_wasm::utils::setup_logging;
use wasm_bindgen_test::wasm_bindgen_test;

mod utils;

use crate::utils::{
    fetch_bridge_webtransport_multiaddr, new_rpc_client, remove_database, spawn_connected_node,
};

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn request_network_head_header() {
    setup_logging();
    remove_database().await.expect("failed to clear db");
    let rpc_client = new_rpc_client().await;
    let bridge_ma = fetch_bridge_webtransport_multiaddr(&rpc_client).await;

    let client = spawn_connected_node(vec![bridge_ma.to_string()]).await;

    let info = client.network_info().await.unwrap();
    assert_eq!(info.num_peers, 1);

    let (bridge_head_header, head_header) = futures::join!(
        rpc_client.header_network_head().map(Result::unwrap),
        client.request_head_header().map(Result::unwrap),
    );

    // due to head selection algorithm, we may get the network head or a previous header
    // if not enough nodes got a new head already
    match head_header.height().cmp(&bridge_head_header.height()) {
        Ordering::Equal => assert_eq!(head_header, bridge_head_header),
        Ordering::Less => assert!(head_header.verify_adjacent(&bridge_head_header).is_ok()),
        Ordering::Greater => assert!(bridge_head_header.verify_adjacent(&head_header).is_ok()),
    }

    rpc_client
        .p2p_close_peer(&PeerId(
            client.local_peer_id().await.unwrap().parse().unwrap(),
        ))
        .await
        .unwrap();
}

#[wasm_bindgen_test]
async fn get_blob() {
    setup_logging();
    remove_database().await.expect("failed to clear db");
    let rpc_client = new_rpc_client().await;
    let namespace = Namespace::new_v0(&[0xCD, 0xDC, 0xCD, 0xDC, 0xCD, 0xDC]).unwrap();
    let data = b"Hello, World";
    let blobs = vec![Blob::new(namespace, data.to_vec(), None, AppVersion::V3).unwrap()];

    let submitted_height = rpc_client
        .blob_submit(&blobs, TxConfig::default())
        .await
        .expect("successful submission");

    let bridge_ma = fetch_bridge_webtransport_multiaddr(&rpc_client).await;
    let client = spawn_connected_node(vec![bridge_ma.to_string()]).await;

    // Wait for the `client` node to sync until the `submitted_height`.
    sleep(Duration::from_secs(2)).await;

    let mut blobs = client
        .request_all_blobs(&namespace, submitted_height, None)
        .await
        .expect("to fetch blob");

    assert_eq!(blobs.len(), 1);
    let blob = blobs.pop().unwrap();
    assert_eq!(blob.data, data);
    assert_eq!(blob.namespace, namespace);
}
