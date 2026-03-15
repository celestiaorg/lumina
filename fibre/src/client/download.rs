//! Download flow orchestration.
//!
//! Adds a `download()` method to [`FibreClient`] that retrieves a blob from
//! validators and reconstructs it using erasure coding.
//!
//! The download algorithm uses an adaptive fan-out strategy with two levels of
//! concurrency control:
//!
//! - A **local semaphore** limits the initial fan-out to `download_target`
//!   concurrent validator connections. When a validator fails, its permit is
//!   released so a replacement validator can be tried.
//! - A **global semaphore** (on the client) limits total concurrent downloads
//!   across all in-flight `download()` calls.
//!
//! Each spawned task downloads proofs from a single validator and returns them.
//! The main task collects proofs and applies them to the blob sequentially,
//! avoiding the need for shared mutable state (`Arc<Mutex<Blob>>`).

use futures::stream::{FuturesUnordered, StreamExt};
use tokio_util::sync::CancellationToken;

use lumina_utils::cond_send::BoxFuture;

use crate::client::task::spawn_task;

use crate::blob::{Blob, BlobID};
use crate::client::FibreClient;
use crate::config::BlobConfig;
use crate::error::FibreError;
use crate::validator::ValidatorSet;

impl FibreClient {
    /// Download and reconstruct a blob by its [`BlobID`].
    ///
    /// The method:
    /// 1. Fetches the current validator set.
    /// 2. Selects validators ordered by download priority.
    /// 3. Downloads row inclusion proofs from validators using adaptive fan-out.
    /// 4. Reconstructs the original data from collected proofs.
    ///
    /// Returns the reconstructed [`Blob`] whose `data()` contains the original payload.
    ///
    /// # Errors
    ///
    /// - [`FibreError::ClientClosed`] if the client has been closed.
    /// - [`FibreError::NotFound`] if no validator returned any rows.
    /// - [`FibreError::NotEnoughShards`] if too few validators succeeded.
    /// - Any error from reconstruction (commitment mismatch, encoding, etc.).
    pub async fn download(&self, id: &BlobID) -> Result<Blob, FibreError> {
        if self.cancel_token.is_cancelled() {
            return Err(FibreError::ClientClosed);
        }

        let val_set = self.set_getter.head().await?;
        let blob_cfg = BlobConfig::for_version(id.version())?;

        let (ordered_indices, download_target) = val_set.select(
            blob_cfg.original_rows,
            self.cfg.min_rows_per_validator,
            self.cfg.liveness_threshold,
        );

        let mut blob = Blob::empty(id.clone())?;

        self.download_blob(
            &val_set,
            &ordered_indices,
            download_target,
            &mut blob,
            &self.cancel_token,
        )
        .await?;

        blob.reconstruct()?;

        Ok(blob)
    }

    /// Internal download with a custom [`BlobConfig`].
    ///
    /// This allows tests to use small K/N parameters without going through
    /// the production `Blob::empty()` path which hardcodes production params.
    #[cfg(test)]
    pub(crate) async fn download_with_config(
        &self,
        id: &BlobID,
        blob_cfg: BlobConfig,
    ) -> Result<Blob, FibreError> {
        if self.cancel_token.is_cancelled() {
            return Err(FibreError::ClientClosed);
        }

        let val_set = self.set_getter.head().await?;

        let (ordered_indices, download_target) = val_set.select(
            blob_cfg.original_rows,
            self.cfg.min_rows_per_validator,
            self.cfg.liveness_threshold,
        );

        let mut blob = Blob::empty_with_config(id.clone(), blob_cfg);

        self.download_blob(
            &val_set,
            &ordered_indices,
            download_target,
            &mut blob,
            &self.cancel_token,
        )
        .await?;

        blob.reconstruct()?;

        Ok(blob)
    }

    /// Download row proofs from validators and apply them to the blob.
    ///
    /// Uses a single select loop that interleaves task spawning with result
    /// collection. An `active` counter replaces the local semaphore: at most
    /// `download_target` tasks run concurrently. When a task fails, `active`
    /// decrements, allowing a replacement validator to be tried on the next
    /// iteration.
    ///
    /// `self.download_semaphore` limits total concurrent downloads globally.
    /// Each spawned task is a pure download — all orchestration (row
    /// application, success counting, replacement) happens in this loop.
    async fn download_blob(
        &self,
        val_set: &ValidatorSet,
        ordered_indices: &[usize],
        download_target: usize,
        blob: &mut Blob,
        cancel_token: &CancellationToken,
    ) -> Result<(), FibreError> {
        if ordered_indices.is_empty() {
            return Err(FibreError::NotFound);
        }

        let download_target_u32 = download_target as u32;
        let blob_id = blob.id().clone();

        #[allow(clippy::type_complexity)]
        let mut futures: FuturesUnordered<
            BoxFuture<
                'static,
                (usize, Option<Result<Vec<rsema1d::RowInclusionProof>, FibreError>>),
            >,
        > = FuturesUnordered::new();

        let mut val_iter = ordered_indices.iter();
        let mut active = 0u32;
        let mut successes = 0u32;

        loop {
            let can_spawn = active < download_target_u32
                && successes < download_target_u32
                && val_iter.len() > 0;

            if !can_spawn && futures.is_empty() {
                break;
            }

            tokio::select! {
                result = self.download_semaphore.clone().acquire_owned(), if can_spawn => {
                    let global_permit = result
                        .map_err(|_| FibreError::Other("global semaphore closed".into()))?;
                    let &val_idx = val_iter.next().unwrap();
                    active += 1;

                    let connector = self.connector.clone();
                    let validator = val_set.validators[val_idx].clone();
                    let blob_id = blob_id.clone();

                    spawn_task(&mut futures, val_idx, async move {
                        let _global = global_permit;
                        let conn = connector.connect(&validator).await?;
                        let resp = conn.download_shard(&blob_id).await?;
                        if resp.rows.is_empty() {
                            return Err(FibreError::EmptyShardResponse);
                        }
                        Ok(resp.rows.into_iter().map(|r| r.proof).collect())
                    });
                }
                task_result = futures.next(), if !futures.is_empty() => {
                    active -= 1;
                    match task_result {
                        Some((val_idx, Some(Ok(proofs)))) => {
                            let total = proofs.len();
                            let mut applied = 0usize;
                            for proof in proofs {
                                if blob.set_row(proof).is_ok() {
                                    applied += 1;
                                }
                            }
                            if applied == 0 && total > 0 {
                                let validator = &val_set.validators[val_idx];
                                tracing::warn!(
                                    validator = %hex::encode(validator.address),
                                    total,
                                    "no rows applied from validator response"
                                );
                            }
                            successes += 1;
                            if successes >= download_target_u32 {
                                break;
                            }
                        }
                        Some((val_idx, Some(Err(e)))) => {
                            let validator = &val_set.validators[val_idx];
                            tracing::warn!(
                                validator = %hex::encode(validator.address),
                                error = %e,
                                "shard download failed"
                            );
                        }
                        Some((val_idx, None)) => {
                            let validator = &val_set.validators[val_idx];
                            tracing::warn!(
                                validator = %hex::encode(validator.address),
                                "download task dropped unexpectedly"
                            );
                        }
                        None => break,
                    }
                }
                _ = cancel_token.cancelled() => {
                    return Err(FibreError::Cancelled);
                }
            }
        }

        if successes == 0 {
            return Err(FibreError::NotFound);
        }
        if successes < download_target_u32 {
            return Err(FibreError::NotEnoughShards {
                got: successes as usize,
                need: download_target,
            });
        }
        if successes > download_target_u32 {
            tracing::warn!(
                downloaded = successes,
                expected_target = download_target_u32,
                "downloaded more shards than needed"
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use ed25519_dalek::SigningKey;

    use crate::blob::{Blob, BlobID};
    use crate::client::FibreClient;
    use crate::config::{BlobConfig, FibreClientConfig, Fraction};
    use crate::error::FibreError;
    use crate::validator::{SetGetter, ValidatorInfo, ValidatorSet};
    use crate::validator_client::{
        DownloadResponse, DownloadedRow, UploadResponse, ValidatorConnection, ValidatorConnector,
    };

    /// Test blob configuration with small K=4, N=4 for fast tests.
    fn test_blob_config() -> BlobConfig {
        BlobConfig::new_test(0, 4, 4, 4096, 4, 64)
    }

    /// Test client configuration with small parameters matching the test blob config.
    fn test_client_config() -> FibreClientConfig {
        FibreClientConfig {
            chain_id: "test-chain".to_string(),
            safety_threshold: Fraction {
                numerator: 2,
                denominator: 3,
            },
            liveness_threshold: Fraction {
                numerator: 1,
                denominator: 3,
            },
            min_rows_per_validator: 1,
            max_message_size: 1024 * 1024,
            upload_concurrency: 10,
            download_concurrency: 10,
        }
    }

    /// Create a ValidatorInfo with the given voting power and seed.
    fn make_validator(power: i64, seed: u8) -> ValidatorInfo {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = seed;
        let signing_key = SigningKey::from_bytes(&key_bytes);
        ValidatorInfo {
            address: [seed; 20],
            pubkey: signing_key.verifying_key(),
            voting_power: power,
        }
    }

    struct MockSetGetter {
        val_set: ValidatorSet,
    }

    #[async_trait::async_trait]
    impl SetGetter for MockSetGetter {
        async fn head(&self) -> Result<ValidatorSet, FibreError> {
            Ok(self.val_set.clone())
        }
    }

    /// A mock connection that stores uploaded row proofs and returns them on download.
    struct MockValidatorConnection {
        /// Stored row proofs keyed by commitment.
        stored: Mutex<HashMap<[u8; 32], Vec<rsema1d::RowInclusionProof>>>,
        /// If true, all operations return an error.
        fail: bool,
    }

    impl MockValidatorConnection {
        fn new() -> Self {
            Self {
                stored: Mutex::new(HashMap::new()),
                fail: false,
            }
        }

        fn new_failing() -> Self {
            Self {
                stored: Mutex::new(HashMap::new()),
                fail: true,
            }
        }

        fn store_proofs(&self, commitment: [u8; 32], proofs: Vec<rsema1d::RowInclusionProof>) {
            self.stored
                .lock()
                .unwrap()
                .entry(commitment)
                .or_default()
                .extend(proofs);
        }
    }

    #[async_trait::async_trait]
    impl ValidatorConnection for MockValidatorConnection {
        async fn upload_shard(
            &self,
            _promise: &crate::payment_promise::PaymentPromise,
            _rows: &[rsema1d::RowInclusionProof],
            _rlc_coeffs: &[rsema1d::GF128],
        ) -> Result<UploadResponse, FibreError> {
            Ok(UploadResponse {
                validator_signature: vec![0u8; 64],
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
    struct MockConnector {
        connections: HashMap<[u8; 20], Arc<MockValidatorConnection>>,
    }

    impl MockConnector {
        fn new() -> Self {
            Self {
                connections: HashMap::new(),
            }
        }

        fn add(&mut self, address: [u8; 20], conn: Arc<MockValidatorConnection>) {
            self.connections.insert(address, conn);
        }
    }

    #[async_trait::async_trait]
    impl ValidatorConnector for MockConnector {
        async fn connect(
            &self,
            validator: &ValidatorInfo,
        ) -> Result<Arc<dyn ValidatorConnection>, FibreError> {
            let conn = self
                .connections
                .get(&validator.address)
                .ok_or_else(|| FibreError::HostNotFound(hex::encode(validator.address)))?
                .clone();
            Ok(conn)
        }
    }

    fn build_test_client(val_set: ValidatorSet, connector: MockConnector) -> FibreClient {
        FibreClient::builder()
            .config(test_client_config())
            .set_getter(MockSetGetter { val_set })
            .connector(connector)
            .build()
            .unwrap()
    }

    /// Encode a blob, extract all row proofs, and store them on each mock
    /// validator connection. Returns the BlobID.
    fn prepare_blob_and_distribute(
        data: &[u8],
        connections: &[Arc<MockValidatorConnection>],
        cfg: &BlobConfig,
    ) -> BlobID {
        let blob = Blob::new(data, cfg.clone()).unwrap();
        let blob_id = blob.id().clone();
        let total_rows = cfg.total_rows();

        for conn in connections {
            let mut proofs = Vec::new();
            for i in 0..total_rows {
                proofs.push(blob.row(i).unwrap());
            }
            conn.store_proofs(blob_id.commitment(), proofs);
        }

        blob_id
    }

    #[tokio::test]
    async fn download_reconstructs_blob() {
        let cfg = test_blob_config();
        let data: Vec<u8> = (0u8..=199).collect();

        let validators = vec![
            make_validator(100, 1),
            make_validator(100, 2),
            make_validator(100, 3),
        ];
        let val_set = ValidatorSet {
            validators: validators.clone(),
            height: 1,
        };

        let conns: Vec<Arc<MockValidatorConnection>> = validators
            .iter()
            .map(|_| Arc::new(MockValidatorConnection::new()))
            .collect();

        let blob_id = prepare_blob_and_distribute(&data, &conns, &cfg);

        let mut connector = MockConnector::new();
        for (i, v) in validators.iter().enumerate() {
            connector.add(v.address, conns[i].clone());
        }

        let client = build_test_client(val_set, connector);
        let blob = client.download_with_config(&blob_id, cfg).await.unwrap();

        assert_eq!(blob.data().unwrap(), &data);
    }

    #[tokio::test]
    async fn download_handles_validator_failure() {
        let cfg = test_blob_config();
        let data: Vec<u8> = (0u8..=199).collect();

        // 5 validators; validator 0 and 1 will fail
        let validators = vec![
            make_validator(100, 1),
            make_validator(100, 2),
            make_validator(100, 3),
            make_validator(100, 4),
            make_validator(100, 5),
        ];
        let val_set = ValidatorSet {
            validators: validators.clone(),
            height: 1,
        };

        let failing_conn_0 = Arc::new(MockValidatorConnection::new_failing());
        let failing_conn_1 = Arc::new(MockValidatorConnection::new_failing());
        let good_conns: Vec<Arc<MockValidatorConnection>> = (2..5)
            .map(|_| Arc::new(MockValidatorConnection::new()))
            .collect();

        let blob = Blob::new(&data, cfg.clone()).unwrap();
        let blob_id = blob.id().clone();
        let total_rows = cfg.total_rows();

        for conn in &good_conns {
            let mut proofs = Vec::new();
            for i in 0..total_rows {
                proofs.push(blob.row(i).unwrap());
            }
            conn.store_proofs(blob_id.commitment(), proofs);
        }

        let mut connector = MockConnector::new();
        connector.add(validators[0].address, failing_conn_0);
        connector.add(validators[1].address, failing_conn_1);
        for (i, conn) in good_conns.iter().enumerate() {
            connector.add(validators[i + 2].address, conn.clone());
        }

        let client = build_test_client(val_set, connector);
        let result = client.download_with_config(&blob_id, cfg).await.unwrap();

        assert_eq!(result.data().unwrap(), &data);
    }

    #[tokio::test]
    async fn download_fails_when_not_enough_shards() {
        let cfg = test_blob_config();
        let data: Vec<u8> = (0u8..=99).collect();

        // Single validator that fails
        let validators = vec![make_validator(100, 1)];
        let val_set = ValidatorSet {
            validators: validators.clone(),
            height: 1,
        };

        let failing_conn = Arc::new(MockValidatorConnection::new_failing());
        let mut connector = MockConnector::new();
        connector.add(validators[0].address, failing_conn);

        let blob = Blob::new(&data, cfg.clone()).unwrap();
        let blob_id = blob.id().clone();

        let client = build_test_client(val_set, connector);
        let result = client.download_with_config(&blob_id, cfg).await;

        assert!(
            matches!(result, Err(FibreError::NotFound)),
            "expected NotFound error"
        );
    }

    #[tokio::test]
    async fn download_fails_when_client_closed() {
        let cfg = test_blob_config();
        let validators = vec![make_validator(100, 1)];
        let val_set = ValidatorSet {
            validators: validators.clone(),
            height: 1,
        };

        let conn = Arc::new(MockValidatorConnection::new());
        let mut connector = MockConnector::new();
        connector.add(validators[0].address, conn);

        let client = build_test_client(val_set, connector);
        client.close();

        let blob_id = BlobID::new(0, [0u8; 32]);
        let result = client.download_with_config(&blob_id, cfg).await;

        assert!(
            matches!(result, Err(FibreError::ClientClosed)),
            "expected ClientClosed error"
        );
    }

    #[tokio::test]
    async fn full_roundtrip_upload_then_download() {
        let cfg = test_blob_config();
        let original_data: Vec<u8> = (0u8..=249).collect();

        let validators = vec![
            make_validator(100, 10),
            make_validator(100, 20),
            make_validator(100, 30),
        ];
        let val_set = ValidatorSet {
            validators: validators.clone(),
            height: 42,
        };

        let blob = Blob::new(&original_data, cfg.clone()).unwrap();
        let blob_id = blob.id().clone();
        let total_rows = cfg.total_rows();

        let conns: Vec<Arc<MockValidatorConnection>> = validators
            .iter()
            .map(|_| Arc::new(MockValidatorConnection::new()))
            .collect();

        for conn in &conns {
            let mut proofs = Vec::new();
            for i in 0..total_rows {
                proofs.push(blob.row(i).unwrap());
            }
            conn.store_proofs(blob_id.commitment(), proofs);
        }

        let mut connector = MockConnector::new();
        for (i, v) in validators.iter().enumerate() {
            connector.add(v.address, conns[i].clone());
        }

        let client = build_test_client(val_set, connector);
        let downloaded = client.download_with_config(&blob_id, cfg).await.unwrap();

        assert_eq!(downloaded.data().unwrap(), &original_data);
        assert_eq!(downloaded.id(), &blob_id);
    }

    #[tokio::test]
    async fn download_empty_response_triggers_replacement() {
        let cfg = test_blob_config();
        let data: Vec<u8> = (0u8..=149).collect();

        // 3 validators; validator 0 returns NotFound (no proofs stored)
        let validators = vec![
            make_validator(100, 1),
            make_validator(100, 2),
            make_validator(100, 3),
        ];
        let val_set = ValidatorSet {
            validators: validators.clone(),
            height: 1,
        };

        let blob = Blob::new(&data, cfg.clone()).unwrap();
        let blob_id = blob.id().clone();
        let total_rows = cfg.total_rows();

        // Validator 0 has an empty store (returns NotFound)
        let empty_conn = Arc::new(MockValidatorConnection::new());

        // Validators 1 and 2 have proofs
        let good_conns: Vec<Arc<MockValidatorConnection>> = (0..2)
            .map(|_| {
                let conn = Arc::new(MockValidatorConnection::new());
                let mut proofs = Vec::new();
                for i in 0..total_rows {
                    proofs.push(blob.row(i).unwrap());
                }
                conn.store_proofs(blob_id.commitment(), proofs);
                conn
            })
            .collect();

        let mut connector = MockConnector::new();
        connector.add(validators[0].address, empty_conn);
        connector.add(validators[1].address, good_conns[0].clone());
        connector.add(validators[2].address, good_conns[1].clone());

        let client = build_test_client(val_set, connector);
        let downloaded = client.download_with_config(&blob_id, cfg).await.unwrap();

        assert_eq!(downloaded.data().unwrap(), &data);
    }
}
