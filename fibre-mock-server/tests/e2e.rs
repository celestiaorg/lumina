//! End-to-end smoke test: a real FibreClient against the mock network.
#![cfg(not(target_arch = "wasm32"))]

use celestia_fibre::{Blob, BlobConfig, DownloadOptions, FibreClient, FibreClientConfig};
use celestia_types::nmt::Namespace;
use fibre_mock_server::{MockNetworkConfig, spawn_mock_network};

fn test_config() -> MockNetworkConfig {
    MockNetworkConfig {
        num_validators: 4,
        base_port: 0,
        ..Default::default()
    }
}

fn client(control_addr: std::net::SocketAddr) -> FibreClient {
    FibreClient::from_endpoint(
        format!("http://{control_addr}"),
        FibreClientConfig {
            // chain_id feeds the promise sign bytes; the default is empty.
            chain_id: "mock-1".to_string(),
            ..Default::default()
        },
    )
    .expect("client builds")
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_download_roundtrip() {
    let handle = spawn_mock_network(test_config()).await.unwrap();
    let fibre = client(handle.control_addr);

    let signing_key = k256::ecdsa::SigningKey::from_slice(&[7u8; 32]).unwrap();
    let namespace = Namespace::new_v0(b"mock").unwrap();
    let data = b"hello from the fibre mock server ".repeat(64);
    let blob = Blob::new(&data, BlobConfig::for_version(0).unwrap()).unwrap();
    let blob_id = blob.id().clone();

    let signed = fibre.upload(&signing_key, namespace, blob).await.unwrap();
    // Signature verification inside the client proves the mock's sign_bytes
    // reconstruction is byte-exact. 2/3 of 4 equal validators = 3 signatures.
    let sig_count = signed.validator_signatures.iter().flatten().count();
    assert!(
        sig_count >= 3,
        "expected >= 3 validator signatures, got {sig_count}"
    );

    let downloaded = fibre
        .download(&blob_id, DownloadOptions::default())
        .await
        .unwrap();
    assert_eq!(downloaded.data(), Some(data.as_slice()));

    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn no_store_uploads_succeed_but_downloads_fail() {
    let handle = spawn_mock_network(MockNetworkConfig {
        store_shards: false,
        ..test_config()
    })
    .await
    .unwrap();
    let fibre = client(handle.control_addr);

    let signing_key = k256::ecdsa::SigningKey::from_slice(&[7u8; 32]).unwrap();
    let namespace = Namespace::new_v0(b"mock").unwrap();
    let data = b"discarded after signing ".repeat(64);
    let blob = Blob::new(&data, BlobConfig::for_version(0).unwrap()).unwrap();
    let blob_id = blob.id().clone();

    // Upload still collects signatures (2/3 of 4 validators).
    let signed = fibre.upload(&signing_key, namespace, blob).await.unwrap();
    assert!(signed.validator_signatures.iter().flatten().count() >= 3);

    // But the shard was discarded, so download cannot be served.
    let result = fibre.download(&blob_id, DownloadOptions::default()).await;
    assert!(
        result.is_err(),
        "download must fail when storage is disabled"
    );

    handle.shutdown();
}

#[tokio::test(flavor = "multi_thread")]
async fn download_unknown_blob_fails() {
    let handle = spawn_mock_network(test_config()).await.unwrap();
    let fibre = client(handle.control_addr);

    // A valid blob id that was never uploaded; goes through the real client,
    // so this also exercises the TLS handshake with every validator.
    let blob = Blob::new(b"never uploaded", BlobConfig::for_version(0).unwrap()).unwrap();
    let result = fibre.download(blob.id(), DownloadOptions::default()).await;
    assert!(result.is_err(), "download of unknown blob must fail");

    handle.shutdown();
}

/// Mirrors fibre-demo: upload, broadcast MsgPayForFibre through the mock
/// app-node services, confirm, then download and compare bytes.
#[tokio::test(flavor = "multi_thread")]
async fn payment_roundtrip() {
    use celestia_fibre::{GrpcHostRegistry, GrpcSetGetter, GrpcValidatorConnector};
    use celestia_grpc::{GrpcClient, TxConfig};
    use celestia_proto::celestia::fibre::v1::MsgPayForFibre;

    let handle = spawn_mock_network(test_config()).await.unwrap();
    let control_url = format!("http://{}", handle.control_addr);

    let private_key = [7u8; 32];
    let signing_key = k256::ecdsa::SigningKey::from_slice(&private_key).unwrap();
    let app_grpc = GrpcClient::builder()
        .url(&control_url)
        .private_key(&private_key)
        .build()
        .unwrap();
    let core_grpc = GrpcClient::builder().url(&control_url).build().unwrap();

    let host_registry = std::sync::Arc::new(GrpcHostRegistry::new(app_grpc.clone()));
    let fibre = FibreClient::builder()
        .config(FibreClientConfig {
            chain_id: "mock-1".to_string(),
            ..Default::default()
        })
        .set_getter(GrpcSetGetter::new(core_grpc))
        .connector(GrpcValidatorConnector::new(host_registry, "mock-1"))
        .build()
        .unwrap();

    let namespace = Namespace::new_v0(b"mock").unwrap();
    let data = b"paid blob through the mock app node ".repeat(64);
    let blob = Blob::new(&data, BlobConfig::for_version(0).unwrap()).unwrap();
    let blob_id = blob.id().clone();

    let signed = fibre.upload(&signing_key, namespace, blob).await.unwrap();
    let signer = app_grpc.get_account_address().unwrap();
    let msg = MsgPayForFibre {
        signer: signer.to_string(),
        payment_promise: Some((&signed.promise).into()),
        validator_signatures: signed
            .validator_signatures
            .iter()
            .map(|s| s.clone().unwrap_or_default())
            .collect(),
    };

    let tx = app_grpc
        .broadcast_message(msg, TxConfig::default())
        .await
        .unwrap()
        .confirm()
        .await
        .unwrap();
    assert!(tx.height >= 1, "tx confirmed at height {}", tx.height);

    let downloaded = fibre
        .download(&blob_id, DownloadOptions::default())
        .await
        .unwrap();
    assert_eq!(downloaded.data(), Some(data.as_slice()));

    handle.shutdown();
}

#[tokio::test]
async fn invalid_network_configs_are_rejected() {
    for cfg in [
        MockNetworkConfig {
            num_validators: 0,
            base_port: 0,
            ..Default::default()
        },
        MockNetworkConfig {
            voting_power: 0,
            base_port: 0,
            ..Default::default()
        },
        MockNetworkConfig {
            height: 0,
            base_port: 0,
            ..Default::default()
        },
    ] {
        assert!(spawn_mock_network(cfg).await.is_err());
    }
}

#[tokio::test]
async fn wildcard_listener_uses_advertised_ip() {
    let handle = spawn_mock_network(MockNetworkConfig {
        listen_ip: "0.0.0.0".parse().unwrap(),
        advertise_ip: Some("127.0.0.1".parse().unwrap()),
        base_port: 0,
        ..Default::default()
    })
    .await
    .unwrap();

    assert_eq!(
        handle.control_addr.ip(),
        std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
    );
    assert!(
        handle
            .validators
            .iter()
            .all(|validator| validator.host.starts_with("http://127.0.0.1:"))
    );

    handle.shutdown();
}
