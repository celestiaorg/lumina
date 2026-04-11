//! Shared test utilities for FibreClient tests.
//!
//! Contains unified mock implementations used across upload, download, and
//! roundtrip tests.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ed25519_dalek::SigningKey as Ed25519SigningKey;

use crate::blob::BlobID;
use crate::client::FibreClient;
use crate::config::{BlobConfig, FibreClientConfig, Fraction};
use crate::error::FibreError;
use crate::payment_promise::PaymentPromise;
use crate::validator::{SetGetter, ValidatorInfo, ValidatorSet};
use crate::validator_client::{
    DownloadResponse, DownloadedRow, UploadResponse, ValidatorConnection, ValidatorConnector,
};

/// Mock [`SetGetter`] that returns a fixed validator set.
pub(crate) struct MockSetGetter {
    pub val_set: ValidatorSet,
}

#[async_trait::async_trait]
impl SetGetter for MockSetGetter {
    async fn head(&self) -> Result<ValidatorSet, FibreError> {
        Ok(self.val_set.clone())
    }

    async fn get_by_height(&self, _height: u64) -> Result<ValidatorSet, FibreError> {
        Ok(self.val_set.clone())
    }
}

/// A mock validator connection that signs upload responses and stores/returns
/// rows for download. Supports an optional `fail` flag to simulate failures.
pub(crate) struct MockValidatorConnection {
    /// Ed25519 signing key for this mock validator.
    ed_signing_key: Ed25519SigningKey,
    /// Rows stored by commitment hash.
    stored: Mutex<HashMap<[u8; 32], Vec<rsema1d::RowInclusionProof>>>,
    /// Record of what was uploaded (each call appends a list of row proofs).
    uploaded: Mutex<Vec<Vec<rsema1d::RowInclusionProof>>>,
    /// Watch channel incremented on each upload, for awaiting uploads in tests.
    upload_tx: tokio::sync::watch::Sender<usize>,
    /// If true, all operations return an error.
    fail: bool,
}

impl MockValidatorConnection {
    pub fn new(ed_signing_key: Ed25519SigningKey) -> Self {
        let (tx, _) = tokio::sync::watch::channel(0usize);
        Self {
            ed_signing_key,
            stored: Mutex::new(HashMap::new()),
            uploaded: Mutex::new(Vec::new()),
            upload_tx: tx,
            fail: false,
        }
    }

    pub fn new_failing(ed_signing_key: Ed25519SigningKey) -> Self {
        let (tx, _) = tokio::sync::watch::channel(0usize);
        Self {
            ed_signing_key,
            stored: Mutex::new(HashMap::new()),
            uploaded: Mutex::new(Vec::new()),
            upload_tx: tx,
            fail: true,
        }
    }

    /// Store row proofs externally (for download-only tests where upload is skipped).
    pub fn store_proofs(&self, commitment: [u8; 32], proofs: Vec<rsema1d::RowInclusionProof>) {
        self.stored
            .lock()
            .unwrap()
            .entry(commitment)
            .or_default()
            .extend(proofs);
    }

    /// Returns the list of uploaded row proof batches.
    #[allow(dead_code)]
    pub fn uploaded(&self) -> Vec<Vec<rsema1d::RowInclusionProof>> {
        self.uploaded.lock().unwrap().clone()
    }

    /// Wait until at least one upload has been received.
    pub async fn wait_for_upload(&self) {
        let mut rx = self.upload_tx.subscribe();
        // If already uploaded, return immediately.
        if *rx.borrow() > 0 {
            return;
        }
        // Otherwise wait for the next change.
        let _ = rx.changed().await;
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

        // Record what was uploaded and notify waiters.
        self.uploaded.lock().unwrap().push(rows.to_vec());
        self.upload_tx.send_modify(|n| *n += 1);

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

/// A connector that returns pre-configured connections by validator address.
pub(crate) struct MockConnector {
    connections: HashMap<[u8; 20], Arc<MockValidatorConnection>>,
}

impl MockConnector {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
        }
    }

    pub fn add(&mut self, address: [u8; 20], conn: Arc<MockValidatorConnection>) {
        self.connections.insert(address, conn);
    }

    /// Returns the connection for the given address, if any.
    #[allow(dead_code)]
    pub fn get(&self, address: &[u8; 20]) -> Option<&Arc<MockValidatorConnection>> {
        self.connections.get(address)
    }
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

/// A connector that fails for specific validator addresses, delegating
/// everything else to an inner connector.
pub(crate) struct FailingConnector {
    pub inner: MockConnector,
    pub fail_addresses: Vec<[u8; 20]>,
}

#[async_trait::async_trait]
impl ValidatorConnector for FailingConnector {
    async fn connect(
        &self,
        validator: &ValidatorInfo,
    ) -> Result<Arc<dyn ValidatorConnection>, FibreError> {
        if self.fail_addresses.contains(&validator.address) {
            return Err(FibreError::HostNotFound(hex::encode(validator.address)));
        }
        self.inner.connect(validator).await
    }
}

/// Create a validator with a deterministic ed25519 keypair.
pub(crate) fn make_validator(power: i64, seed: u8) -> (Ed25519SigningKey, ValidatorInfo) {
    let mut key_bytes = [0u8; 32];
    key_bytes[0] = seed;
    let ed_key = Ed25519SigningKey::from_bytes(&key_bytes);
    let pubkey = ed_key.verifying_key();
    (
        ed_key,
        ValidatorInfo {
            address: [seed; 20],
            pubkey,
            voting_power: power,
        },
    )
}

/// Standard test blob configuration: K=4, N=4, min_row_size=64.
pub(crate) fn test_blob_config() -> BlobConfig {
    BlobConfig::new_test(0, 4, 4, 4096, 4, 64)
}

/// Standard test client configuration.
pub(crate) fn test_client_config(chain_id: &str) -> FibreClientConfig {
    FibreClientConfig {
        chain_id: chain_id.to_string(),
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

/// Build a [`FibreClient`] from a validator set and connector with test config.
pub(crate) fn build_test_client(
    val_set: ValidatorSet,
    connector: impl ValidatorConnector + 'static,
    chain_id: &str,
) -> FibreClient {
    FibreClient::builder()
        .config(test_client_config(chain_id))
        .set_getter(MockSetGetter { val_set })
        .connector(connector)
        .build()
        .unwrap()
}

/// Create a [`MockConnector`] with connections for each validator.
pub(crate) fn make_connector(validators: &[(Ed25519SigningKey, ValidatorInfo)]) -> MockConnector {
    let mut connector = MockConnector::new();
    for (ed_key, info) in validators {
        connector.add(
            info.address,
            Arc::new(MockValidatorConnection::new(ed_key.clone())),
        );
    }
    connector
}
