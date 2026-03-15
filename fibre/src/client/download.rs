//! Download flow orchestration.
//!
//! Retrieves a blob from validators and reconstructs it using erasure coding
//! with an adaptive fan-out strategy.

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
    /// - [`FibreError::NotEnoughShards`] if too few unique rows were collected.
    /// - Any error from reconstruction (commitment mismatch, encoding, etc.).
    pub async fn download(&self, id: &BlobID) -> Result<Blob, FibreError> {
        if self.cancel_token.is_cancelled() {
            return Err(FibreError::ClientClosed);
        }

        let val_set = self.set_getter.head().await?;
        let blob_cfg = BlobConfig::for_version(id.version())?;

        let ordered_indices = val_set.select(
            blob_cfg.original_rows,
            self.cfg.min_rows_per_validator,
            self.cfg.liveness_threshold,
        );

        let shard_map = val_set.assign(
            id.commitment(),
            blob_cfg.total_rows(),
            blob_cfg.original_rows,
            self.cfg.min_rows_per_validator,
            self.cfg.liveness_threshold,
        );

        let mut blob = Blob::empty(id.clone())?;

        self.download_blob(
            &val_set,
            &ordered_indices,
            blob_cfg.original_rows,
            &shard_map,
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

        let ordered_indices = val_set.select(
            blob_cfg.original_rows,
            self.cfg.min_rows_per_validator,
            self.cfg.liveness_threshold,
        );

        let shard_map = val_set.assign(
            id.commitment(),
            blob_cfg.total_rows(),
            blob_cfg.original_rows,
            self.cfg.min_rows_per_validator,
            self.cfg.liveness_threshold,
        );

        let mut blob = Blob::empty_with_config(id.clone(), blob_cfg);

        self.download_blob(
            &val_set,
            &ordered_indices,
            blob.config().original_rows,
            &shard_map,
            &mut blob,
            &self.cancel_token,
        )
        .await?;

        blob.reconstruct()?;

        Ok(blob)
    }

    /// Download row proofs from validators and apply them to the blob.
    ///
    /// Maintains a dynamic concurrency invariant:
    ///   `unique_rows + inflight_rows >= original_rows`
    /// When a validator fails, the shortfall immediately triggers spawning
    /// more validators to compensate.
    async fn download_blob(
        &self,
        val_set: &ValidatorSet,
        ordered_indices: &[usize],
        original_rows: usize,
        shard_map: &crate::validator::ShardMap,
        blob: &mut Blob,
        cancel_token: &CancellationToken,
    ) -> Result<(), FibreError> {
        if ordered_indices.is_empty() {
            return Err(FibreError::NotFound);
        }

        let blob_id = blob.id().clone();

        // Build expected-rows-per-validator lookup from the shard map.
        let expected_rows: Vec<usize> = (0..val_set.validators.len())
            .map(|i| shard_map.get(i).map_or(0, |v| v.len()))
            .collect();

        #[allow(clippy::type_complexity)]
        let mut futures: FuturesUnordered<
            BoxFuture<
                'static,
                (
                    usize,
                    Option<Result<Vec<rsema1d::RowInclusionProof>, FibreError>>,
                ),
            >,
        > = FuturesUnordered::new();

        let mut val_iter = ordered_indices.iter();
        let mut unique_rows: usize = 0;
        let mut inflight_rows: usize = 0;

        loop {
            // Spawn more validators while we need more rows covered.
            let need_more = (unique_rows + inflight_rows) < original_rows && val_iter.len() > 0;

            if !need_more && futures.is_empty() {
                break;
            }

            tokio::select! {
                result = self.download_semaphore.clone().acquire_owned(), if need_more => {
                    let global_permit = result
                        .map_err(|_| FibreError::Other("global semaphore closed".into()))?;
                    let &val_idx = val_iter.next().unwrap();
                    inflight_rows += expected_rows[val_idx];

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
                    match task_result {
                        Some((val_idx, Some(Ok(proofs)))) => {
                            inflight_rows = inflight_rows.saturating_sub(expected_rows[val_idx]);
                            let total = proofs.len();
                            let mut applied = 0usize;
                            for proof in proofs {
                                if blob.set_row(proof).is_ok() {
                                    applied += 1;
                                }
                            }
                            unique_rows += applied;
                            if applied == 0 && total > 0 {
                                let validator = &val_set.validators[val_idx];
                                tracing::warn!(
                                    validator = %hex::encode(validator.address),
                                    total,
                                    "no rows applied from validator response"
                                );
                            }
                            if unique_rows >= original_rows {
                                break;
                            }
                        }
                        Some((val_idx, Some(Err(e)))) => {
                            inflight_rows = inflight_rows.saturating_sub(expected_rows[val_idx]);
                            let validator = &val_set.validators[val_idx];
                            tracing::warn!(
                                validator = %hex::encode(validator.address),
                                error = %e,
                                "shard download failed"
                            );
                            // Invariant violated — loop will spawn more validators.
                        }
                        Some((val_idx, None)) => {
                            inflight_rows = inflight_rows.saturating_sub(expected_rows[val_idx]);
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
    use std::sync::Arc;

    use crate::blob::{Blob, BlobID};
    use crate::config::BlobConfig;
    use crate::error::FibreError;
    use crate::test_utils::{
        build_test_client, make_validator, test_blob_config, MockConnector,
        MockValidatorConnection,
    };
    use crate::validator::ValidatorSet;

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
            conn.store_proofs(blob_id.commitment(), proofs);
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
            conn.store_proofs(blob_id.commitment(), proofs);
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
                conn.store_proofs(blob_id.commitment(), proofs);
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
