use std::slice;

use celestia_types::Blob;
use lumina_utils::test_utils::async_test;

pub mod utils;

use crate::utils::client::{AuthLevel, blob_submit, new_test_client_with_url};
use crate::utils::{random_bytes, random_ns};

// Use node-1 (bridge node) as the RPC URL
#[cfg(not(target_arch = "wasm32"))]
const CELESTIA_BRIDGE_RPC_URL: &str = "ws://localhost:36658";
#[cfg(target_arch = "wasm32")]
const CELESTIA_BRIDGE_RPC_URL: &str = "http://localhost:36658";

#[async_test]
async fn blob_submit_using_bridge_node() {
    let namespace = random_ns();
    let data = random_bytes(5);
    let blob = Blob::new(namespace, data, None).unwrap();

    for auth_level in [
        AuthLevel::Skip,
        AuthLevel::Read,
        AuthLevel::Write,
        AuthLevel::Admin,
    ] {
        let client = new_test_client_with_url(auth_level, CELESTIA_BRIDGE_RPC_URL)
            .await
            .unwrap();

        match auth_level {
            AuthLevel::Skip | AuthLevel::Read => {
                blob_submit(&client, slice::from_ref(&blob))
                    .await
                    .unwrap_err();
            }
            _ => {
                blob_submit(&client, slice::from_ref(&blob)).await.unwrap();
            }
        }
    }
}
