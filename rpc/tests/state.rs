use celestia_rpc::TxConfig;
use celestia_rpc::prelude::*;
use celestia_types::{AppVersion, Blob};
use lumina_utils::test_utils::async_test;

pub mod utils;

use crate::utils::client::{AuthLevel, new_test_client};
use crate::utils::{random_bytes, random_ns};

#[async_test]
async fn account_address() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();
    let _addr = client.state_account_address().await.unwrap();
}

#[async_test]
async fn balance() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();
    let balance = client.state_balance().await.unwrap();
    assert_eq!(balance.denom(), "utia");
}

#[async_test]
async fn balance_for_address() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();

    let my_addr = client.state_account_address().await.unwrap();
    let my_balance = client.state_balance().await.unwrap();

    let balance = client.state_balance_for_address(my_addr).await.unwrap();
    assert_eq!(my_balance, balance);
}

#[async_test]
async fn submit_pay_for_blob() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();
    let namespace = random_ns();
    let data = random_bytes(5);
    let blob = Blob::new(namespace, data, None, AppVersion::V2).unwrap();

    let tx_response = client
        .state_submit_pay_for_blob(vec![blob.clone().into()], TxConfig::default())
        .await
        .unwrap();

    let received_blob = client
        .blob_get(tx_response.height as u64, namespace, blob.commitment)
        .await
        .unwrap();

    received_blob.validate(AppVersion::V2).unwrap();
    assert_eq!(received_blob.data, blob.data);
}
