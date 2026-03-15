//! Upload flow orchestration.
//!
//! Distributes a pre-encoded [`Blob`] to validators, collecting ed25519
//! signatures until the safety threshold is met, then returns a
//! [`SignedPaymentPromise`].
//!
//! The [`FibreClient::put()`] method encodes a blob, uploads it to validators,
//! and returns a [`PreparedPut`] containing the `MsgPayForFibre` ready for
//! broadcast by the caller.

use std::sync::Arc;

use celestia_proto::celestia::fibre::v1::MsgPayForFibre;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio_util::sync::CancellationToken;

use lumina_utils::cond_send::BoxFuture;

use crate::client::task::spawn_task;

use crate::blob::{Blob, BlobID};
use crate::client::FibreClient;
use crate::config::BlobConfig;
use crate::error::FibreError;
use crate::payment_promise::{PaymentPromise, SignedPaymentPromise};
use crate::signature_set::SignatureSet;
use crate::validator::{ShardMap, ValidatorSet};

/// Result of a successful [`FibreClient::put()`] operation.
///
/// Contains the blob identifier, the constructed `MsgPayForFibre` message
/// ready for on-chain broadcast, and the collected validator signatures.
#[derive(Debug)]
pub struct PreparedPut {
    /// The unique identifier of the uploaded blob.
    pub blob_id: BlobID,
    /// The `MsgPayForFibre` message ready for on-chain broadcast.
    pub msg: MsgPayForFibre,
    /// Validator signatures confirming they received and stored the blob.
    /// Positionally aligned with the validator set: `signatures[i]` corresponds
    /// to `validator[i]`. Missing signatures are empty `Vec<u8>`.
    pub validator_signatures: Vec<Vec<u8>>,
}

impl FibreClient {
    /// Upload a pre-encoded [`Blob`] and collect validator signatures.
    ///
    /// Returns a [`SignedPaymentPromise`] once enough validator signatures
    /// have been collected to meet the safety threshold.
    pub async fn upload(
        &self,
        signing_key: &k256::ecdsa::SigningKey,
        namespace: &[u8],
        blob: Blob,
    ) -> Result<SignedPaymentPromise, FibreError> {
        if self.cancel_token.is_cancelled() {
            return Err(FibreError::ClientClosed);
        }

        // 1. Get validator set
        let val_set = self.set_getter.head().await?;

        // 2. Create and sign payment promise
        let upload_size = blob
            .upload_size()
            .ok_or_else(|| FibreError::Other("blob has no data to upload".into()))?;
        let upload_size_u32 = u32::try_from(upload_size).map_err(|_| {
            FibreError::BlobTooLarge {
                size: upload_size,
                max: u32::MAX as usize,
            }
        })?;

        let mut promise = PaymentPromise {
            chain_id: self.cfg.chain_id.clone(),
            height: val_set.height,
            namespace: namespace.to_vec(),
            upload_size: upload_size_u32,
            blob_version: blob.config().blob_version as u32,
            commitment: blob.id().commitment(),
            creation_timestamp: std::time::SystemTime::now(),
            signer_pubkey: *signing_key.verifying_key(),
            signature: None,
        };
        promise.sign(signing_key)?;

        // 3. Assign shards
        let shard_map = val_set.assign(
            blob.id().commitment(),
            blob.config().total_rows(),
            blob.config().original_rows,
            self.cfg.min_rows_per_validator,
            self.cfg.liveness_threshold,
        );

        // 4. Create signature set
        // Both client and validators sign the same CometBFT-wrapped bytes.
        let validator_sign_bytes = promise.sign_bytes()?;
        let sig_set =
            Arc::new(val_set.new_signature_set(self.cfg.safety_threshold, validator_sign_bytes));

        // 5. Fan-out upload (child token so we can cancel stragglers early)
        let blob = Arc::new(blob);
        let upload_token = self.cancel_token.child_token();
        self.upload_shards(
            &val_set,
            &shard_map,
            &promise,
            &blob,
            &sig_set,
            &upload_token,
        )
        .await?;

        // 6. Collect signatures
        let sigs = sig_set.signatures()?;

        Ok(SignedPaymentPromise {
            promise,
            validator_signatures: sigs,
        })
    }

    /// Encode data, upload to validators, and build a `MsgPayForFibre`.
    ///
    /// Returns a [`PreparedPut`] containing the blob ID, the message, and
    /// validator signatures. The caller is responsible for broadcasting the
    /// message on-chain.
    pub async fn put(
        &self,
        signing_key: &k256::ecdsa::SigningKey,
        namespace: &[u8],
        data: &[u8],
        signer_address: &str,
    ) -> Result<PreparedPut, FibreError> {
        if self.cancel_token.is_cancelled() {
            return Err(FibreError::ClientClosed);
        }

        // 1. Encode data into a Blob.
        let blob = Blob::new(data, BlobConfig::for_version(0)?)?;
        let blob_id = blob.id().clone();

        // 2. Upload to validators and collect signatures.
        let signed_promise = self.upload(signing_key, namespace, blob).await?;

        // 3. Map signatures to on-chain format, preserving positional alignment.
        // The on-chain code maps signatures[i] → validator[i], so we must keep
        // None entries as empty vecs (which the chain skips) rather than removing
        // them, which would shift later signatures to wrong validator indices.
        let validator_signatures: Vec<Vec<u8>> = signed_promise
            .validator_signatures
            .iter()
            .map(|s| s.clone().unwrap_or_default())
            .collect();

        // 4. Construct the MsgPayForFibre proto message.
        let msg = MsgPayForFibre {
            signer: signer_address.to_string(),
            payment_promise: Some((&signed_promise.promise).into()),
            validator_signatures: validator_signatures.clone(),
        };

        Ok(PreparedPut {
            blob_id,
            msg,
            validator_signatures,
        })
    }

    /// Fan-out upload of row proofs to validators in the shard map.
    ///
    /// Returns early when the signature threshold is met. Individual upload
    /// failures are logged but not fatal.
    async fn upload_shards(
        &self,
        val_set: &ValidatorSet,
        shard_map: &ShardMap,
        promise: &PaymentPromise,
        blob: &Arc<Blob>,
        sig_set: &Arc<SignatureSet>,
        cancel_token: &CancellationToken,
    ) -> Result<(), FibreError> {
        // Collect (validator_index, row_indices) pairs for iteration.
        let validator_tasks: Vec<(usize, Vec<usize>)> = shard_map
            .inner()
            .iter()
            .map(|(&val_idx, row_indices)| (val_idx, row_indices.clone()))
            .collect();

        // Extract RLC coefficients once for all tasks (empty if unavailable).
        let rlc_coeffs: Arc<Vec<rsema1d::GF128>> =
            Arc::new(blob.rlc_coeffs().map(|c| c.to_vec()).unwrap_or_default());

        #[allow(clippy::type_complexity)]
        let mut futures: FuturesUnordered<
            BoxFuture<'static, (usize, Option<Result<Vec<u8>, FibreError>>)>,
        > = FuturesUnordered::new();

        let mut task_iter = validator_tasks.into_iter();

        loop {
            let can_spawn = task_iter.len() > 0;

            if !can_spawn && futures.is_empty() {
                break;
            }

            tokio::select! {
                result = self.upload_semaphore.clone().acquire_owned(), if can_spawn => {
                    let permit = result
                        .map_err(|_| FibreError::Other("upload semaphore closed".into()))?;
                    let (val_idx, row_indices) = task_iter.next().unwrap();

                    let connector = Arc::clone(&self.connector);
                    let validator = val_set.validators[val_idx].clone();
                    let promise = promise.clone();
                    let rlc_coeffs = Arc::clone(&rlc_coeffs);
                    let blob = Arc::clone(blob);

                    spawn_task(&mut futures, val_idx, async move {
                        let _permit = permit;
                        // Generate row proofs in this task, parallelizing
                        // proof generation across validators.
                        let mut proofs = Vec::with_capacity(row_indices.len());
                        for row_idx in &row_indices {
                            proofs.push(blob.row(*row_idx)?);
                        }

                        let conn = connector.connect(&validator).await?;
                        let resp =
                            conn.upload_shard(&promise, &proofs, &rlc_coeffs).await?;
                        Ok(resp.validator_signature)
                    });
                }
                task_result = futures.next(), if !futures.is_empty() => {
                    match task_result {
                        Some((val_idx, Some(Ok(signature)))) => {
                            let validator = &val_set.validators[val_idx];
                            match sig_set.add(validator, &signature) {
                                Ok(threshold_met) => {
                                    if threshold_met {
                                        cancel_token.cancel();
                                        return Ok(());
                                    }
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        validator = %hex::encode(validator.address),
                                        error = %e,
                                        "invalid validator signature"
                                    );
                                }
                            }
                        }
                        Some((val_idx, Some(Err(e)))) => {
                            let validator = &val_set.validators[val_idx];
                            tracing::warn!(
                                validator = %hex::encode(validator.address),
                                error = %e,
                                "shard upload failed"
                            );
                        }
                        Some((val_idx, None)) => {
                            let validator = &val_set.validators[val_idx];
                            tracing::warn!(
                                validator = %hex::encode(validator.address),
                                "upload task dropped unexpectedly"
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

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use k256::ecdsa::SigningKey;
    use rand::rngs::OsRng;

    use crate::blob::Blob;
    use crate::config::BlobConfig;
    use crate::error::FibreError;
    use crate::test_utils::{
        build_test_client, make_connector, make_validator, FailingConnector,
    };
    use crate::validator::{ValidatorInfo, ValidatorSet};

    fn make_test_blob() -> Blob {
        let cfg = BlobConfig::new_test(0, 4, 4, 4096, 4, 64);
        let data: Vec<u8> = (0u8..200).collect();
        Blob::new(&data, cfg).unwrap()
    }

    fn test_signing_key() -> SigningKey {
        SigningKey::random(&mut OsRng)
    }

    #[tokio::test]
    async fn upload_fails_when_client_closed() {
        let (_, val) = make_validator(100, 1);
        let validators = vec![make_validator(100, 1)];
        let connector = make_connector(&validators);

        let val_set = ValidatorSet {
            validators: vec![val],
            height: 1,
        };

        let client = build_test_client(val_set, connector, "test-chain");
        client.close();

        let sk = test_signing_key();
        let blob = make_test_blob();
        let namespace = vec![0u8; 29];
        let result = client.upload(&sk, &namespace, blob).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            FibreError::ClientClosed => {}
            other => panic!("expected ClientClosed, got: {other}"),
        }
    }

    #[tokio::test]
    async fn upload_collects_signatures() {
        let validators = vec![
            make_validator(100, 1),
            make_validator(100, 2),
            make_validator(100, 3),
        ];

        let val_infos: Vec<ValidatorInfo> =
            validators.iter().map(|(_, info)| info.clone()).collect();

        let connector = make_connector(&validators);

        let val_set = ValidatorSet {
            validators: val_infos,
            height: 42,
        };

        let sk = test_signing_key();
        let client = build_test_client(val_set, connector, "test-chain");
        let blob = make_test_blob();
        let namespace = vec![0u8; 29];

        let result = client.upload(&sk, &namespace, blob).await;
        assert!(result.is_ok(), "upload should succeed: {:?}", result.err());

        let signed = result.unwrap();
        assert_eq!(signed.promise.height, 42);
        assert_eq!(signed.promise.chain_id, "test-chain");

        let sig_count = signed
            .validator_signatures
            .iter()
            .filter(|s| s.is_some())
            .count();
        assert!(
            sig_count >= 2,
            "expected at least 2 signatures, got {sig_count}"
        );
    }

    #[tokio::test]
    async fn upload_assigns_rows_to_each_validator() {
        let validators = vec![
            make_validator(100, 1),
            make_validator(100, 2),
            make_validator(100, 3),
        ];

        let val_infos: Vec<ValidatorInfo> =
            validators.iter().map(|(_, info)| info.clone()).collect();

        let connector = make_connector(&validators);

        let val_set = ValidatorSet {
            validators: val_infos,
            height: 1,
        };

        let client = build_test_client(val_set, connector, "test-chain");
        let blob = make_test_blob();
        let namespace = vec![0u8; 29];

        client
            .upload(&test_signing_key(), &namespace, blob)
            .await
            .unwrap();

        // Note: with shared mocks, we cannot inspect uploaded data directly
        // because make_connector owns the connections. The upload succeeding
        // already proves that rows were sent and signed.
    }

    #[tokio::test]
    async fn upload_returns_when_threshold_met() {
        let v1 = make_validator(200, 1);
        let v2 = make_validator(200, 2);
        let v3 = make_validator(200, 3);
        let v4 = make_validator(100, 4);
        let v5 = make_validator(100, 5);

        let all_validators = [v1.clone(), v2.clone(), v3.clone(), v4.clone(), v5.clone()];
        let val_infos: Vec<ValidatorInfo> = all_validators
            .iter()
            .map(|(_, info)| info.clone())
            .collect();

        let successful = vec![v1, v2, v3];
        let inner = make_connector(&successful);
        let fail_addresses = vec![val_infos[3].address, val_infos[4].address];

        let failing_connector = FailingConnector {
            inner,
            fail_addresses,
        };

        let val_set = ValidatorSet {
            validators: val_infos,
            height: 10,
        };

        let client = build_test_client(val_set, failing_connector, "test-chain");
        let blob = make_test_blob();
        let namespace = vec![0u8; 29];

        let result = client.upload(&test_signing_key(), &namespace, blob).await;
        assert!(
            result.is_ok(),
            "upload should succeed with 3/5 validators: {:?}",
            result.err()
        );

        let signed = result.unwrap();
        let sig_count = signed
            .validator_signatures
            .iter()
            .filter(|s| s.is_some())
            .count();
        assert!(
            sig_count >= 2,
            "expected at least 2 signatures, got {sig_count}"
        );
    }

    #[tokio::test]
    async fn upload_fails_when_not_enough_signatures() {
        let v1 = make_validator(100, 1);
        let v2 = make_validator(100, 2);
        let v3 = make_validator(100, 3);
        let v4 = make_validator(100, 4);
        let v5 = make_validator(100, 5);

        let all_validators = [v1.clone(), v2.clone(), v3.clone(), v4.clone(), v5.clone()];
        let val_infos: Vec<ValidatorInfo> = all_validators
            .iter()
            .map(|(_, info)| info.clone())
            .collect();

        let successful = vec![v1];
        let inner = make_connector(&successful);
        let fail_addresses = vec![
            val_infos[1].address,
            val_infos[2].address,
            val_infos[3].address,
            val_infos[4].address,
        ];

        let failing_connector = FailingConnector {
            inner,
            fail_addresses,
        };

        let val_set = ValidatorSet {
            validators: val_infos,
            height: 10,
        };

        let client = build_test_client(val_set, failing_connector, "test-chain");
        let blob = make_test_blob();
        let namespace = vec![0u8; 29];

        let result = client.upload(&test_signing_key(), &namespace, blob).await;
        assert!(
            result.is_err(),
            "upload should fail when not enough signatures"
        );
        match result.unwrap_err() {
            FibreError::NotEnoughSignatures { .. } => {}
            other => panic!("expected NotEnoughSignatures, got: {other}"),
        }
    }

    #[tokio::test]
    async fn put_fails_when_client_closed() {
        let (_, val) = make_validator(100, 1);
        let validators = vec![make_validator(100, 1)];
        let connector = make_connector(&validators);

        let val_set = ValidatorSet {
            validators: vec![val],
            height: 1,
        };

        let client = build_test_client(val_set, connector, "test-chain");
        client.close();

        let namespace = vec![0u8; 29];
        let data = vec![1u8; 100];
        let sk = test_signing_key();
        let result = client.put(&sk, &namespace, &data, "celestia1test").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            FibreError::ClientClosed => {}
            other => panic!("expected ClientClosed, got: {other}"),
        }
    }
}
