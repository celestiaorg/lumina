//! End-to-end roundtrip tests for FibreClient.
//!
//! These tests exercise the full client flow: `upload()` → `download()` → verify
//! data matches. Mock validators store rows on upload and return them on download,
//! with valid ed25519 signatures so the upload signature collection succeeds.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ed25519_dalek::SigningKey as Ed25519SigningKey;

use crate::blob::{Blob, BlobID};
use crate::client::FibreClient;
use crate::config::{BlobConfig, FibreClientConfig, Fraction};
use crate::error::FibreError;
use crate::payment_promise::PaymentPromise;
use crate::validator::{SetGetter, ValidatorInfo, ValidatorSet};
use crate::validator_client::{
    DownloadResponse, DownloadedRow, UploadResponse, ValidatorConnection, ValidatorConnector,
};

struct MockSetGetter {
    val_set: ValidatorSet,
}

#[async_trait::async_trait]
impl SetGetter for MockSetGetter {
    async fn head(&self) -> Result<ValidatorSet, FibreError> {
        Ok(self.val_set.clone())
    }
}

/// A mock validator connection that stores rows on upload and returns them on
/// download. Produces valid ed25519 signatures so the upload flow's
/// `SignatureSet` accepts them.
struct MockValidatorConnection {
    ed_signing_key: Ed25519SigningKey,
    /// Rows stored by commitment hash.
    stored: Mutex<HashMap<[u8; 32], Vec<rsema1d::RowInclusionProof>>>,
    /// If true, all operations return an error.
    fail: bool,
}

impl MockValidatorConnection {
    fn new(ed_signing_key: Ed25519SigningKey) -> Self {
        Self {
            ed_signing_key,
            stored: Mutex::new(HashMap::new()),
            fail: false,
        }
    }

    fn new_failing(ed_signing_key: Ed25519SigningKey) -> Self {
        Self {
            ed_signing_key,
            stored: Mutex::new(HashMap::new()),
            fail: true,
        }
    }
}

#[async_trait::async_trait]
impl ValidatorConnection for MockValidatorConnection {
    async fn upload_shard(
        &self,
        promise: &PaymentPromise,
        rows: &[rsema1d::RowInclusionProof],
        _rlc_coeffs: &[rsema1d::GF128],
    ) -> Result<UploadResponse, FibreError> {
        if self.fail {
            return Err(FibreError::Other("mock connection failure".into()));
        }

        // Store uploaded rows keyed by commitment.
        self.stored
            .lock()
            .unwrap()
            .entry(promise.commitment)
            .or_default()
            .extend(rows.to_vec());

        // Sign the promise's sign bytes (CometBFT-wrapped, same for client and validators).
        use ed25519_dalek::Signer;
        let sign_bytes = promise.sign_bytes()?;
        let signature = self.ed_signing_key.sign(&sign_bytes);

        Ok(UploadResponse {
            validator_signature: signature.to_bytes().to_vec(),
        })
    }

    async fn download_shard(&self, blob_id: &BlobID) -> Result<DownloadResponse, FibreError> {
        if self.fail {
            return Err(FibreError::Other("mock connection failure".into()));
        }

        let stored = self.stored.lock().unwrap();
        let proofs = stored
            .get(&blob_id.commitment())
            .ok_or(FibreError::NotFound)?;

        Ok(DownloadResponse {
            rows: proofs
                .iter()
                .map(|p| DownloadedRow { proof: p.clone() })
                .collect(),
        })
    }
}

struct MockConnector {
    connections: HashMap<[u8; 20], Arc<MockValidatorConnection>>,
}

#[async_trait::async_trait]
impl ValidatorConnector for MockConnector {
    async fn connect(
        &self,
        validator: &ValidatorInfo,
    ) -> Result<Arc<dyn ValidatorConnection>, FibreError> {
        self.connections
            .get(&validator.address)
            .cloned()
            .map(|c| c as Arc<dyn ValidatorConnection>)
            .ok_or_else(|| FibreError::HostNotFound(hex::encode(validator.address)))
    }
}

fn test_blob_config() -> BlobConfig {
    BlobConfig::new_test(0, 4, 4, 4096, 4, 64)
}

fn test_client_config() -> FibreClientConfig {
    FibreClientConfig {
        chain_id: "roundtrip-test".to_string(),
        safety_threshold: Fraction {
            numerator: 2,
            denominator: 3,
        },
        liveness_threshold: Fraction {
            numerator: 1,
            denominator: 3,
        },
        min_rows_per_validator: 1,
        max_message_size: 1 << 20,
        upload_concurrency: 10,
        download_concurrency: 10,
    }
}

fn make_validator(power: i64, seed: u8) -> (Ed25519SigningKey, ValidatorInfo) {
    let mut key_bytes = [0u8; 32];
    key_bytes[0] = seed;
    let ed_key = Ed25519SigningKey::from_bytes(&key_bytes);
    (
        ed_key.clone(),
        ValidatorInfo {
            address: [seed; 20],
            pubkey: ed_key.verifying_key(),
            voting_power: power,
        },
    )
}

fn build_client(
    validators: &[(Ed25519SigningKey, ValidatorInfo)],
    connector: MockConnector,
) -> FibreClient {
    let val_infos: Vec<ValidatorInfo> = validators.iter().map(|(_, v)| v.clone()).collect();
    let val_set = ValidatorSet {
        validators: val_infos,
        height: 42,
    };

    FibreClient::builder()
        .config(test_client_config())
        .set_getter(MockSetGetter { val_set })
        .connector(connector)
        .build()
        .unwrap()
}

fn make_connector(validators: &[(Ed25519SigningKey, ValidatorInfo)]) -> MockConnector {
    let mut connections = HashMap::new();
    for (ed_key, info) in validators {
        connections.insert(
            info.address,
            Arc::new(MockValidatorConnection::new(ed_key.clone())),
        );
    }
    MockConnector { connections }
}

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
    let client = build_client(&validators, connector);

    // Upload.
    let signed = client.upload(&k256::ecdsa::SigningKey::random(&mut rand::rngs::OsRng), &[0u8; 29], &blob).await;
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

    let all_validators = vec![v1.clone(), v2.clone(), v3.clone(), v4.clone(), v5.clone()];

    let mut connections: HashMap<[u8; 20], Arc<MockValidatorConnection>> = HashMap::new();
    for (ed_key, info) in &[v1, v2, v3] {
        connections.insert(
            info.address,
            Arc::new(MockValidatorConnection::new(ed_key.clone())),
        );
    }
    for (ed_key, info) in &[v4, v5] {
        connections.insert(
            info.address,
            Arc::new(MockValidatorConnection::new_failing(ed_key.clone())),
        );
    }
    let connector = MockConnector { connections };
    let client = build_client(&all_validators, connector);

    // Total voting power = 800. 2/3 threshold = 533.
    // 3 good validators have 600 voting power > 533 → upload should succeed.
    let signed = client.upload(&k256::ecdsa::SigningKey::random(&mut rand::rngs::OsRng), &[0u8; 29], &blob).await;
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
