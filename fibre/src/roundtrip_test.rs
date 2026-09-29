//! End-to-end roundtrip tests for FibreClient.
//!
//! These tests exercise the full client flow: `upload()` → `download()` → verify
//! data matches. Mock validators store rows on upload and return them on download,
//! with valid ed25519 signatures so the upload signature collection succeeds.

use std::sync::Arc;

use crate::test_utils::{
    MockValidatorConnection, build_test_client, make_connector, test_blob, validator_set,
};

#[tokio::test]
async fn upload_then_download_roundtrip() {
    let (blob, original_data) = test_blob(200);
    let cfg = blob.config().clone();
    let blob_id = blob.id().clone();
    let (validators, val_set) = validator_set(&[100, 100, 100], 42);

    let connector = make_connector(&validators);
    let client = build_test_client(val_set, connector, "roundtrip-test");

    // Upload.
    let signed = client
        .upload(
            &k256::ecdsa::SigningKey::random(&mut rand::rngs::OsRng),
            celestia_types::nmt::Namespace::from_raw(&[0u8; 29]).unwrap(),
            blob,
        )
        .await;
    assert!(signed.is_ok(), "upload should succeed: {:?}", signed.err());

    let signed = signed.unwrap();
    let sig_count = signed
        .validator_signatures
        .iter()
        .filter(|s| s.is_some())
        .count();
    assert!(
        sig_count >= 2,
        "expected at least 2 validator signatures, got {sig_count}"
    );

    // Download and verify data matches.
    let downloaded = client.download_with_config(&blob_id, cfg).await;
    assert!(
        downloaded.is_ok(),
        "download should succeed: {:?}",
        downloaded.err()
    );

    let downloaded = downloaded.unwrap();
    assert_eq!(
        downloaded.data(),
        &original_data,
        "downloaded data should match original"
    );
    assert_eq!(downloaded.id(), &blob_id);
}

#[tokio::test]
async fn roundtrip_with_partial_validator_failure() {
    let (blob, original_data) = test_blob(200);
    let cfg = blob.config().clone();
    let blob_id = blob.id().clone();
    let (validators, val_set) = validator_set(&[200, 200, 200, 100, 100], 42);
    let mut connector = make_connector(&validators[..3]);
    for (key, validator) in &validators[3..] {
        connector.add(
            validator.address,
            Arc::new(MockValidatorConnection::new_failing(key.clone())),
        );
    }

    let client = build_test_client(val_set, connector, "roundtrip-test");

    // Total voting power = 800. 2/3 threshold = 533.
    // 3 good validators have 600 voting power > 533 → upload should succeed.
    let signed = client
        .upload(
            &k256::ecdsa::SigningKey::random(&mut rand::rngs::OsRng),
            celestia_types::nmt::Namespace::from_raw(&[0u8; 29]).unwrap(),
            blob,
        )
        .await;
    assert!(
        signed.is_ok(),
        "upload should succeed with 3/5 validators: {:?}",
        signed.err()
    );

    // Download should reconstruct from the 3 good validators.
    let downloaded = client.download_with_config(&blob_id, cfg).await;
    assert!(
        downloaded.is_ok(),
        "download should succeed: {:?}",
        downloaded.err()
    );
    assert_eq!(downloaded.unwrap().data(), &original_data);
}
