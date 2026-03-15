//! End-to-end roundtrip tests for FibreClient.
//!
//! These tests exercise the full client flow: `upload()` → `download()` → verify
//! data matches. Mock validators store rows on upload and return them on download,
//! with valid ed25519 signatures so the upload signature collection succeeds.

use std::sync::Arc;

use crate::blob::Blob;
use crate::test_utils::{
    build_test_client, make_connector, make_validator, test_blob_config, MockConnector,
    MockValidatorConnection,
};

#[tokio::test]
async fn upload_then_download_roundtrip() {
    let cfg = test_blob_config();
    let original_data: Vec<u8> = (0u8..200).collect();

    let blob = Blob::new(&original_data, cfg.clone()).unwrap();
    let blob_id = blob.id().clone();

    let validators = vec![
        make_validator(100, 1),
        make_validator(100, 2),
        make_validator(100, 3),
    ];

    let connector = make_connector(&validators);
    let val_infos = validators.iter().map(|(_, v)| v.clone()).collect();
    let val_set = crate::validator::ValidatorSet {
        validators: val_infos,
        height: 42,
    };

    let client = build_test_client(val_set, connector, "roundtrip-test");

    // Upload.
    let signed = client
        .upload(
            &k256::ecdsa::SigningKey::random(&mut rand::rngs::OsRng),
            &[0u8; 29],
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
        downloaded.data().unwrap(),
        &original_data,
        "downloaded data should match original"
    );
    assert_eq!(downloaded.id(), &blob_id);
}

#[tokio::test]
async fn roundtrip_with_partial_validator_failure() {
    let cfg = test_blob_config();
    let original_data: Vec<u8> = (0u8..200).collect();

    let blob = Blob::new(&original_data, cfg.clone()).unwrap();
    let blob_id = blob.id().clone();

    // 5 validators: first 3 succeed, last 2 fail.
    let v1 = make_validator(200, 1);
    let v2 = make_validator(200, 2);
    let v3 = make_validator(200, 3);
    let v4 = make_validator(100, 4);
    let v5 = make_validator(100, 5);

    let all_validators = [v1.clone(), v2.clone(), v3.clone(), v4.clone(), v5.clone()];

    let mut connector = MockConnector::new();
    for (ed_key, info) in &[v1, v2, v3] {
        connector.add(
            info.address,
            Arc::new(MockValidatorConnection::new(ed_key.clone())),
        );
    }
    for (ed_key, info) in &[v4, v5] {
        connector.add(
            info.address,
            Arc::new(MockValidatorConnection::new_failing(ed_key.clone())),
        );
    }

    let val_infos = all_validators.iter().map(|(_, v)| v.clone()).collect();
    let val_set = crate::validator::ValidatorSet {
        validators: val_infos,
        height: 42,
    };

    let client = build_test_client(val_set, connector, "roundtrip-test");

    // Total voting power = 800. 2/3 threshold = 533.
    // 3 good validators have 600 voting power > 533 → upload should succeed.
    let signed = client
        .upload(
            &k256::ecdsa::SigningKey::random(&mut rand::rngs::OsRng),
            &[0u8; 29],
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
    assert_eq!(downloaded.unwrap().data().unwrap(), &original_data);
}
