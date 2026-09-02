//! Download flow orchestration.
//!
//! Retrieves a blob from validators and reconstructs it using erasure coding
//! with an adaptive fan-out strategy.

use std::sync::Arc;

use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::client::task::{TaskSet, spawn_task};

use crate::ValidatorInfo;
use crate::blob::{Blob, BlobID, ShardVerifier, VerifiedRows};
use crate::client::FibreClient;
#[cfg(test)]
use crate::config::BlobConfig;
use crate::error::FibreError;
use crate::validator::ValidatorSet;

/// Options for configuring blob download behavior.
#[derive(Default)]
pub struct DownloadOptions {
    /// When set, use the validator set at this height instead of head.
    pub height: Option<u64>,
}

impl FibreClient {
    /// Download and reconstruct a blob by its [`BlobID`].
    ///
    /// The method:
    /// 1. Fetches the current validator set (or at a specific height via `opts`).
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
    /// - [`FibreError::NotEnoughShards`] if too few unique rows were collected.
    /// - Any error from reconstruction (commitment mismatch, encoding, etc.).
    pub async fn download(&self, id: &BlobID, opts: DownloadOptions) -> Result<Blob, FibreError> {
        if self.cancel_token.is_cancelled() {
            return Err(FibreError::ClientClosed);
        }

        let val_set = match opts.height {
            Some(h) => self.set_getter.get_by_height(h).await?,
            None => self.set_getter.head().await?,
        };
        let mut blob = Blob::empty(id.clone())?;
        self.select_and_download(&val_set, &mut blob).await?;
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
        let mut blob = Blob::empty_with_config(id.clone(), blob_cfg);
        self.select_and_download(&val_set, &mut blob).await?;
        blob.reconstruct()?;
        Ok(blob)
    }

    async fn select_and_download(
        &self,
        val_set: &ValidatorSet,
        blob: &mut Blob,
    ) -> Result<(), FibreError> {
        let selected = val_set.select(
            blob.config().original_rows,
            self.cfg.min_rows_per_validator,
            self.cfg.liveness_threshold,
        );

        self.download_blob(
            &selected,
            blob.config().original_rows,
            blob,
            &self.cancel_token,
        )
        .await
    }

    /// Download row proofs from validators and apply them to the blob.
    ///
    /// Maintains a dynamic concurrency invariant:
    ///   `unique_rows + inflight_rows >= original_rows`
    /// When a validator fails, the shortfall immediately triggers spawning
    /// more validators to compensate.
    async fn download_blob(
        &self,
        selected: &[(usize, &ValidatorInfo)],
        original_rows: usize,
        blob: &mut Blob,
        cancel_token: &CancellationToken,
    ) -> Result<(), FibreError> {
        if selected.is_empty() {
            return Err(FibreError::NotFound);
        }

        let blob_id = blob.id().clone();
        let verifier = Arc::new(ShardVerifier::new(blob));
        let task_cancel = cancel_token.child_token();
        let _task_cancel_guard = task_cancel.clone().drop_guard();
        let mut futures: TaskSet<usize, Result<VerifiedRows, FibreError>> = TaskSet::new();

        let mut cur_idx = 0;
        let mut unique_rows: usize = 0;
        let mut inflight_rows: usize = 0;

        loop {
            // Spawn more validators while we need more rows covered.
            let need_more =
                (unique_rows + inflight_rows) < original_rows && cur_idx < selected.len();

            if !need_more && futures.is_empty() {
                break;
            }

            tokio::select! {
                result = self.download_semaphore.clone().acquire_owned(), if need_more => {
                    let global_permit = result
                        .map_err(|_| FibreError::Other("global semaphore closed".into()))?;
                    let val_idx = cur_idx;
                    cur_idx += 1;
                    let (rows, info) = selected[val_idx];
                    inflight_rows += rows;

                    let connector = self.connector.clone();
                    let validator = info.clone();
                    let blob_id = blob_id.clone();
                    let verifier = Arc::clone(&verifier);
                    let already_stored = blob.stored_rows_bitmap();
                    let task_cancel = task_cancel.child_token();

                    spawn_task(&mut futures, val_idx, async move {
                        let _global = global_permit;
                        tokio::select! {
                            biased;
                            _ = task_cancel.cancelled() => Err(FibreError::Cancelled),
                            result = async {
                                let conn = connector.connect(&validator).await?;
                                let shard = conn.download_shard(&blob_id).await?;
                                if shard.rows.is_empty() {
                                    return Err(FibreError::EmptyShardResponse);
                                }
                                // Verify here so the heavy crypto runs off the
                                // select! loop and per-task instead of serially.
                                verifier
                                    .verify(shard.rows, &shard.rlcs, &already_stored)
                                    .await
                            } => result,
                        }
                    });
                }
                task_result = futures.next(), if !futures.is_empty() => {
                    match task_result {
                        Some((val_idx, Some(Ok(verified)))) => {
                            let (rows, info) = selected[val_idx];
                            inflight_rows = inflight_rows.saturating_sub(rows);
                            let applied = blob.store_rows(verified)?;
                            // Shards are never empty here, so zero
                            // applied means all rows were duplicates.
                            if applied == 0 {
                                tracing::warn!(
                                    validator = %info.address_hex(),
                                    "no rows applied from validator response"
                                );
                            }
                            unique_rows += applied;
                            if unique_rows >= original_rows {
                                task_cancel.cancel();
                                while futures.next().await.is_some() {}
                                break;
                            }
                        }
                        Some((val_idx, Some(Err(error)))) => {
                            let (rows, info) = selected[val_idx];
                            inflight_rows = inflight_rows.saturating_sub(rows);
                            tracing::warn!(
                                validator = %info.address_hex(),
                                %error,
                                "shard download or verification failed"
                            );
                            // Invariant violated — loop will spawn more validators.
                        }
                        Some((val_idx, None)) => {
                            let (rows, info) = selected[val_idx];
                            inflight_rows = inflight_rows.saturating_sub(rows);
                            tracing::warn!(
                                validator = %info.address_hex(),
                                "download task dropped unexpectedly"
                            );
                        }
                        None => break,
                    }
                }
                _ = cancel_token.cancelled() => {
                    task_cancel.cancel();
                    while futures.next().await.is_some() {}
                    return Err(FibreError::Cancelled);
                }
            }
        }

        if unique_rows == 0 {
            return Err(FibreError::NotFound);
        }
        if unique_rows < original_rows {
            return Err(FibreError::NotEnoughShards {
                got: unique_rows,
                need: original_rows,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::Arc;

    use tokio::sync::Barrier;
    use tokio_util::sync::CancellationToken;

    use crate::blob::{Blob, BlobID};
    use crate::config::BlobConfig;
    use crate::error::FibreError;
    use crate::payment_promise::PaymentPromise;
    use crate::test_utils::{
        MockConnector, MockValidatorConnection, build_test_client, make_validator, test_blob_config,
    };
    use crate::validator::{ValidatorInfo, ValidatorSet};
    use crate::validator_client::{
        DownloadResponse, UploadResponse, ValidatorConnection, ValidatorConnector,
    };

    struct CoordinatedConnector {
        fast_address: [u8; 20],
        blocked_address: [u8; 20],
        response: DownloadResponse,
        barrier: Arc<Barrier>,
    }

    struct CoordinatedConnection {
        response: Option<DownloadResponse>,
        barrier: Arc<Barrier>,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl ValidatorConnector for CoordinatedConnector {
        async fn connect(
            &self,
            validator: &ValidatorInfo,
        ) -> Result<Arc<dyn ValidatorConnection>, FibreError> {
            let response = if validator.address == self.fast_address {
                Some(self.response.clone())
            } else if validator.address == self.blocked_address {
                None
            } else {
                return Err(FibreError::HostNotFound(validator.address_hex()));
            };

            Ok(Arc::new(CoordinatedConnection {
                response,
                barrier: Arc::clone(&self.barrier),
            }))
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl ValidatorConnection for CoordinatedConnection {
        async fn upload_shard(
            &self,
            _promise: &PaymentPromise,
            _rows: &[rsema1d::RowInclusionProof],
            _rlc_coeffs: &[rsema1d::GF128],
        ) -> Result<UploadResponse, FibreError> {
            Err(FibreError::Other("upload not supported".into()))
        }

        async fn download_shard(&self, _blob_id: &BlobID) -> Result<DownloadResponse, FibreError> {
            self.barrier.wait().await;
            match &self.response {
                Some(response) => Ok(response.clone()),
                None => pending().await,
            }
        }
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
            conn.store_proofs(
                blob_id.commitment(),
                proofs,
                blob.rlc_coeffs().unwrap().to_vec(),
            );
        }

        blob_id
    }

    #[tokio::test]
    async fn download_reconstructs_blob() {
        let cfg = test_blob_config();
        let data: Vec<u8> = (0u8..=199).collect();

        let validators = [
            make_validator(100, 1),
            make_validator(100, 2),
            make_validator(100, 3),
        ];
        let val_infos: Vec<_> = validators.iter().map(|(_, v)| v.clone()).collect();
        let val_set = ValidatorSet {
            validators: val_infos.clone(),
            height: 1,
        };

        let conns: Vec<Arc<MockValidatorConnection>> = validators
            .iter()
            .map(|(k, _)| Arc::new(MockValidatorConnection::new(k.clone())))
            .collect();

        let blob_id = prepare_blob_and_distribute(&data, &conns, &cfg);

        let mut connector = MockConnector::new();
        for (i, (_, v)) in validators.iter().enumerate() {
            connector.add(v.address, conns[i].clone());
        }

        let client = build_test_client(val_set, connector, "test-chain");
        let blob = client.download_with_config(&blob_id, cfg).await.unwrap();

        assert_eq!(blob.data().unwrap(), &data);
    }

    #[tokio::test]
    async fn download_handles_validator_failure() {
        let cfg = test_blob_config();
        let data: Vec<u8> = (0u8..=199).collect();

        // 5 validators; validator 0 and 1 will fail
        let validators = [
            make_validator(100, 1),
            make_validator(100, 2),
            make_validator(100, 3),
            make_validator(100, 4),
            make_validator(100, 5),
        ];
        let val_infos: Vec<_> = validators.iter().map(|(_, v)| v.clone()).collect();
        let val_set = ValidatorSet {
            validators: val_infos.clone(),
            height: 1,
        };

        let failing_conn_0 = Arc::new(MockValidatorConnection::new_failing(
            validators[0].0.clone(),
        ));
        let failing_conn_1 = Arc::new(MockValidatorConnection::new_failing(
            validators[1].0.clone(),
        ));
        let good_conns: Vec<Arc<MockValidatorConnection>> = validators[2..]
            .iter()
            .map(|(k, _)| Arc::new(MockValidatorConnection::new(k.clone())))
            .collect();

        let blob = Blob::new(&data, cfg.clone()).unwrap();
        let blob_id = blob.id().clone();
        let total_rows = cfg.total_rows();

        for conn in &good_conns {
            let mut proofs = Vec::new();
            for i in 0..total_rows {
                proofs.push(blob.row(i).unwrap());
            }
            conn.store_proofs(
                blob_id.commitment(),
                proofs,
                blob.rlc_coeffs().unwrap().to_vec(),
            );
        }

        let mut connector = MockConnector::new();
        connector.add(val_infos[0].address, failing_conn_0);
        connector.add(val_infos[1].address, failing_conn_1);
        for (i, conn) in good_conns.iter().enumerate() {
            connector.add(val_infos[i + 2].address, conn.clone());
        }

        let client = build_test_client(val_set, connector, "test-chain");
        let result = client.download_with_config(&blob_id, cfg).await.unwrap();

        assert_eq!(result.data().unwrap(), &data);
    }

    #[tokio::test]
    async fn download_handles_tampered_shard() {
        let cfg = test_blob_config();
        let data: Vec<u8> = (0u8..=199).collect();

        let validators = [make_validator(100, 1), make_validator(100, 2)];
        let val_infos: Vec<_> = validators.iter().map(|(_, v)| v.clone()).collect();
        let val_set = ValidatorSet {
            validators: val_infos.clone(),
            height: 1,
        };

        let conns: Vec<Arc<MockValidatorConnection>> = validators
            .iter()
            .map(|(k, _)| Arc::new(MockValidatorConnection::new(k.clone())))
            .collect();

        let blob = Blob::new(&data, cfg.clone()).unwrap();
        let blob_id = blob.id().clone();
        let total_rows = cfg.total_rows();

        for (i, conn) in conns.iter().enumerate() {
            let mut proofs = Vec::new();
            for row in 0..total_rows {
                let mut proof = blob.row(row).unwrap();
                if i == 0 {
                    let mut corrupted = proof.row.to_vec();
                    corrupted[0] ^= 1;
                    proof.row = corrupted.into();
                }
                proofs.push(proof);
            }
            conn.store_proofs(
                blob_id.commitment(),
                proofs,
                blob.rlc_coeffs().unwrap().to_vec(),
            );
        }

        let mut connector = MockConnector::new();
        for (i, (_, v)) in validators.iter().enumerate() {
            connector.add(v.address, conns[i].clone());
        }

        let client = build_test_client(val_set, connector, "test-chain");
        let mut result = Blob::empty_with_config(blob_id, cfg.clone());
        let selected = [
            (cfg.original_rows, &val_infos[0]),
            (cfg.original_rows, &val_infos[1]),
        ];

        client
            .download_blob(
                &selected,
                cfg.original_rows,
                &mut result,
                &CancellationToken::new(),
            )
            .await
            .unwrap();
        result.reconstruct().unwrap();

        assert_eq!(result.data().unwrap(), &data);
    }

    #[tokio::test]
    async fn download_cancels_inflight_tasks_after_reaching_threshold() {
        let cfg = test_blob_config();
        let data: Vec<u8> = (0u8..=199).collect();
        let blob = Blob::new(&data, cfg.clone()).unwrap();
        let blob_id = blob.id().clone();
        let validators = [make_validator(100, 1), make_validator(100, 2)];
        let val_infos: Vec<_> = validators.iter().map(|(_, info)| info.clone()).collect();
        let val_set = ValidatorSet {
            validators: val_infos.clone(),
            height: 1,
        };
        let response = DownloadResponse {
            rows: (0..cfg.total_rows())
                .map(|index| {
                    let proof = blob.row(index).unwrap();
                    rsema1d::RowProof {
                        index: proof.index,
                        row: std::borrow::Cow::Owned(proof.row.to_vec()),
                        row_proof: proof.row_proof,
                    }
                })
                .collect(),
            rlcs: blob.rlc_coeffs().unwrap().to_vec(),
        };
        let connector = CoordinatedConnector {
            fast_address: val_infos[0].address,
            blocked_address: val_infos[1].address,
            response,
            barrier: Arc::new(Barrier::new(2)),
        };
        let client = build_test_client(val_set, connector, "test-chain");
        let mut result = Blob::empty_with_config(blob_id, cfg.clone());
        let selected = [(2, &val_infos[0]), (2, &val_infos[1])];

        client
            .download_blob(
                &selected,
                cfg.original_rows,
                &mut result,
                &CancellationToken::new(),
            )
            .await
            .unwrap();

        assert_eq!(
            client.download_semaphore.available_permits(),
            client.config().download_concurrency
        );
    }

    #[tokio::test]
    async fn download_fails_when_not_enough_shards() {
        let cfg = test_blob_config();
        let data: Vec<u8> = (0u8..=99).collect();

        // Single validator that fails
        let validators = [make_validator(100, 1)];
        let val_infos: Vec<_> = validators.iter().map(|(_, v)| v.clone()).collect();
        let val_set = ValidatorSet {
            validators: val_infos.clone(),
            height: 1,
        };

        let failing_conn = Arc::new(MockValidatorConnection::new_failing(
            validators[0].0.clone(),
        ));
        let mut connector = MockConnector::new();
        connector.add(val_infos[0].address, failing_conn);

        let blob = Blob::new(&data, cfg.clone()).unwrap();
        let blob_id = blob.id().clone();

        let client = build_test_client(val_set, connector, "test-chain");
        let result = client.download_with_config(&blob_id, cfg).await;

        assert!(
            matches!(result, Err(FibreError::NotFound)),
            "expected NotFound error"
        );
    }

    #[tokio::test]
    async fn download_fails_when_client_closed() {
        let cfg = test_blob_config();
        let (key, val) = make_validator(100, 1);
        let val_set = ValidatorSet {
            validators: vec![val.clone()],
            height: 1,
        };

        let conn = Arc::new(MockValidatorConnection::new(key));
        let mut connector = MockConnector::new();
        connector.add(val.address, conn);

        let client = build_test_client(val_set, connector, "test-chain");
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

        let validators = [
            make_validator(100, 10),
            make_validator(100, 20),
            make_validator(100, 30),
        ];
        let val_infos: Vec<_> = validators.iter().map(|(_, v)| v.clone()).collect();
        let val_set = ValidatorSet {
            validators: val_infos.clone(),
            height: 42,
        };

        let blob = Blob::new(&original_data, cfg.clone()).unwrap();
        let blob_id = blob.id().clone();
        let total_rows = cfg.total_rows();

        let conns: Vec<Arc<MockValidatorConnection>> = validators
            .iter()
            .map(|(k, _)| Arc::new(MockValidatorConnection::new(k.clone())))
            .collect();

        for conn in &conns {
            let mut proofs = Vec::new();
            for i in 0..total_rows {
                proofs.push(blob.row(i).unwrap());
            }
            conn.store_proofs(
                blob_id.commitment(),
                proofs,
                blob.rlc_coeffs().unwrap().to_vec(),
            );
        }

        let mut connector = MockConnector::new();
        for (i, (_, v)) in validators.iter().enumerate() {
            connector.add(v.address, conns[i].clone());
        }

        let client = build_test_client(val_set, connector, "test-chain");
        let downloaded = client.download_with_config(&blob_id, cfg).await.unwrap();

        assert_eq!(downloaded.data().unwrap(), &original_data);
        assert_eq!(downloaded.id(), &blob_id);
    }

    #[tokio::test]
    async fn download_empty_response_triggers_replacement() {
        let cfg = test_blob_config();
        let data: Vec<u8> = (0u8..=149).collect();

        // 3 validators; validator 0 returns NotFound (no proofs stored)
        let validators = [
            make_validator(100, 1),
            make_validator(100, 2),
            make_validator(100, 3),
        ];
        let val_infos: Vec<_> = validators.iter().map(|(_, v)| v.clone()).collect();
        let val_set = ValidatorSet {
            validators: val_infos.clone(),
            height: 1,
        };

        let blob = Blob::new(&data, cfg.clone()).unwrap();
        let blob_id = blob.id().clone();
        let total_rows = cfg.total_rows();

        // Validator 0 has an empty store (returns NotFound)
        let empty_conn = Arc::new(MockValidatorConnection::new(validators[0].0.clone()));

        // Validators 1 and 2 have proofs
        let good_conns: Vec<Arc<MockValidatorConnection>> = validators[1..]
            .iter()
            .map(|(k, _)| {
                let conn = Arc::new(MockValidatorConnection::new(k.clone()));
                let mut proofs = Vec::new();
                for i in 0..total_rows {
                    proofs.push(blob.row(i).unwrap());
                }
                conn.store_proofs(
                    blob_id.commitment(),
                    proofs,
                    blob.rlc_coeffs().unwrap().to_vec(),
                );
                conn
            })
            .collect();

        let mut connector = MockConnector::new();
        connector.add(val_infos[0].address, empty_conn);
        connector.add(val_infos[1].address, good_conns[0].clone());
        connector.add(val_infos[2].address, good_conns[1].clone());

        let client = build_test_client(val_set, connector, "test-chain");
        let downloaded = client.download_with_config(&blob_id, cfg).await.unwrap();

        assert_eq!(downloaded.data().unwrap(), &data);
    }
}
