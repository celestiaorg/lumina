use std::sync::Arc;

use celestia_grpc::{GrpcClient, TxConfig};
use celestia_types::nmt::Namespace;
use celestia_types::state::AccAddress;
use k256::ecdsa::SigningKey;

use crate::{
    BlobID, BoxedFibreIo, DownloadOptions, FibreClient, FibreClientConfig, FibreIoConnector,
};

const APP_GRPC_URL: &str = "http://localhost:19090";
const CHAIN_ID: &str = "private";
const TEST_PRIVATE_KEY: &str = include_str!("../../ci/credentials/node-0.plaintext-key");

struct DockerFibreConnector;

#[async_trait::async_trait]
impl FibreIoConnector for DockerFibreConnector {
    async fn connect(&self, host: String, port: u16) -> std::io::Result<BoxedFibreIo> {
        let host = if host == "fibre" { "127.0.0.1" } else { &host };
        let stream = tokio::net::TcpStream::connect((host, port)).await?;
        Ok(Box::pin(stream))
    }
}

#[tokio::test]
async fn put_then_download() {
    let private_key = hex::decode(TEST_PRIVATE_KEY.trim()).unwrap();
    let signing_key = SigningKey::from_slice(&private_key).unwrap();
    let signer_address = AccAddress::new((*signing_key.verifying_key()).into());

    let grpc_client = GrpcClient::builder()
        .url(APP_GRPC_URL)
        .signer_keypair(signing_key.clone())
        .build()
        .unwrap();
    let fibre_client = FibreClient::from_grpc_client_with_io_connector(
        grpc_client.clone(),
        FibreClientConfig::new(CHAIN_ID).unwrap(),
        Arc::new(DockerFibreConnector),
    )
    .unwrap();

    let namespace = Namespace::new_v0(b"fibre-e2e").unwrap();
    let data = b"fibre docker devnet roundtrip";
    let msg = fibre_client
        .upload_and_prepare(&signing_key, namespace, data, &signer_address)
        .await
        .unwrap();

    let promise = msg.payment_promise.as_ref().unwrap();
    let blob_version = u8::try_from(promise.blob_version).unwrap();
    let commitment: [u8; 32] = promise.commitment.as_slice().try_into().unwrap();
    let blob_id = BlobID::new(blob_version, commitment);
    let promise_height = u64::try_from(promise.height).unwrap();

    grpc_client
        .broadcast_message(msg, TxConfig::default())
        .await
        .unwrap()
        .confirm()
        .await
        .unwrap();

    let downloaded = fibre_client
        .download(
            &blob_id,
            DownloadOptions {
                height: Some(promise_height),
            },
        )
        .await
        .unwrap();

    assert_eq!(downloaded.id(), &blob_id);
    assert_eq!(downloaded.data(), data);
}
