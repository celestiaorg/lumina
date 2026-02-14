use celestia_rpc::prelude::*;
use futures_util::StreamExt;
use lumina_utils::test_utils::async_test;

pub mod utils;

use crate::utils::client::{AuthLevel, new_test_client};

#[async_test]
async fn local_head() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();

    let local_head = client.header_local_head().await.unwrap();

    let head_height = local_head.height();
    let genesis_header = client.header_get_by_height(1).await.unwrap();
    let adjacent_header = client.header_get_by_height(head_height - 1).await.unwrap();

    local_head.validate().unwrap();
    genesis_header.verify(&local_head).unwrap();
    adjacent_header.verify(&local_head).unwrap();
}

#[async_test]
async fn get_by_height() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();

    let genesis_header = client.header_get_by_height(1).await.unwrap();
    let second_header = client.header_get_by_height(2).await.unwrap();

    genesis_header.validate().unwrap();
    second_header.validate().unwrap();
}

#[async_test]
async fn get_by_height_non_existent() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();

    client.header_get_by_height(999_999_999).await.unwrap_err();
}

#[async_test]
async fn get_by_hash() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();

    let genesis_header = client.header_get_by_height(1).await.unwrap();
    let genesis_header2 = client
        .header_get_by_hash(genesis_header.hash())
        .await
        .unwrap();

    assert_eq!(genesis_header, genesis_header2);
}

#[async_test]
async fn get_range_by_height() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();

    let genesis_header = client.header_get_by_height(1).await.unwrap();
    let second_header = client.header_get_by_height(2).await.unwrap();

    let headers = client
        .header_get_range_by_height(genesis_header, 3)
        .await
        .unwrap();

    assert_eq!(headers.len(), 1);
    assert_eq!(second_header, headers[0]);
}

#[async_test]
async fn network_head() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();

    let network_head = client.header_network_head().await.unwrap();

    let genesis_header = client.header_get_by_height(1).await.unwrap();
    let adjacent_header = client
        .header_get_by_height(network_head.height() - 1)
        .await
        .unwrap();

    network_head.validate().unwrap();
    genesis_header.verify(&network_head).unwrap();
    adjacent_header.verify(&network_head).unwrap();
}

#[async_test]
async fn subscribe() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();

    let genesis_header = client.header_get_by_height(1).await.unwrap();

    let mut incoming_headers = client.header_subscribe();
    let header1 = incoming_headers.next().await.unwrap().unwrap();
    let header2 = incoming_headers.next().await.unwrap().unwrap();

    genesis_header.verify(&header1).unwrap();
    header1.verify(&header2).unwrap();
}

#[async_test]
async fn sync_state() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();

    let state1 = client.header_sync_state().await.unwrap();

    client
        .header_wait_for_height(state1.height + 1)
        .await
        .unwrap();

    let state2 = client.header_sync_state().await.unwrap();
    assert!(state2.height > state1.height);
}
