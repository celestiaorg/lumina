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

use lumina_utils::cond_send::{BoxFuture, into_boxed};

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
    /// The upload algorithm:
    /// 1. Check if client is closed.
    /// 2. Retrieve the current validator set via [`SetGetter::head()`].
    /// 3. Create and sign a [`PaymentPromise`].
    /// 4. Compute the shard assignment (which rows go to which validator).
    /// 5. Create a [`SignatureSet`] for collecting validator signatures.
    /// 6. Fan-out upload to all validators concurrently (semaphore-bounded).
    /// 7. Return when the safety threshold of signatures is collected, or
    ///    all validators have responded.
    /// 8. Return the [`SignedPaymentPromise`].
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
        let mut promise = PaymentPromise {
            chain_id: self.cfg.chain_id.clone(),
            height: val_set.height,
            namespace: namespace.to_vec(),
            upload_size: blob.upload_size() as u32,
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

        // 5. Fan-out upload
        let blob = Arc::new(blob);
        self.upload_shards(
            &val_set,
            &shard_map,
            &promise,
            &blob,
            &sig_set,
            &self.cancel_token,
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
    /// This is the high-level "put" operation that combines:
    /// 1. Encoding the data into a [`Blob`].
    /// 2. Uploading the blob to validators via [`FibreClient::upload()`].
    /// 3. Constructing a `MsgPayForFibre` message ready for on-chain broadcast.
    ///
    /// Returns a [`PreparedPut`] containing the blob ID, the message, and
    /// validator signatures. The caller is responsible for broadcasting the
    /// message on-chain.
    ///
    /// # Arguments
    ///
    /// * `namespace` - The namespace for the blob.
    /// * `data` - The raw data to encode and upload.
    /// * `signer_address` - The on-chain signer address for `MsgPayForFibre`.
    ///
    /// # Errors
    ///
    /// - [`FibreError::ClientClosed`] if the client has been closed.
    /// - Any error from blob encoding or upload.
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
    /// Each spawned task generates its own row proofs and uploads them,
    /// parallelizing both CPU (proof generation) and I/O (gRPC upload).
    ///
    /// Uses a single select loop that interleaves task spawning (bounded by
    /// `self.upload_semaphore`) with result collection.
    ///
    /// Returns early when the signature threshold is met. Already-spawned tasks
    /// continue in the background via `lumina_utils::executor::spawn`.
    /// Individual upload failures are logged but not fatal — the method succeeds
    /// as long as enough signatures are ultimately collected.
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

                    let (tx, rx) = tokio::sync::oneshot::channel();
                    lumina_utils::executor::spawn(async move {
                        let _permit = permit;
                        let result = async {
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
                        }
                        .await;
                        let _ = tx.send(result);
                    });
                    futures.push(into_boxed(async move {
                        (val_idx, rx.await.ok())
                    }));
                }
                task_result = futures.next(), if !futures.is_empty() => {
                    match task_result {
                        Some((val_idx, Some(Ok(signature)))) => {
                            let validator = &val_set.validators[val_idx];
                            match sig_set.add(validator, &signature) {
                                Ok(threshold_met) => {
                                    if threshold_met {
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
    use std::collections::HashMap;
    use std::sync::Mutex;

    use ed25519_dalek::SigningKey as Ed25519SigningKey;
    use k256::ecdsa::SigningKey;
    use rand::rngs::OsRng;

    use crate::blob::{Blob, BlobID};
    use crate::config::{BlobConfig, FibreClientConfig, Fraction};
    use crate::error::FibreError;
    use crate::payment_promise::PaymentPromise;
    use crate::validator::{ValidatorInfo, ValidatorSet};
    use crate::validator_client::{
        DownloadResponse, UploadResponse, ValidatorConnection, ValidatorConnector,
    };

    use super::*;

    struct MockSetGetter {
        val_set: ValidatorSet,
    }

    #[async_trait::async_trait]
    impl crate::validator::SetGetter for MockSetGetter {
        async fn head(&self) -> Result<ValidatorSet, FibreError> {
            Ok(self.val_set.clone())
        }
    }

    struct MockValidatorConnection {
        /// Ed25519 signing key for this mock validator.
        ed_signing_key: Ed25519SigningKey,
        /// Record of what was uploaded (each call appends a list of row proofs).
        uploaded: Mutex<Vec<Vec<rsema1d::RowInclusionProof>>>,
    }

    #[async_trait::async_trait]
    impl ValidatorConnection for MockValidatorConnection {
        async fn upload_shard(
            &self,
            promise: &PaymentPromise,
            rows: &[rsema1d::RowInclusionProof],
            _rlc_coeffs: &[rsema1d::GF128],
        ) -> Result<UploadResponse, FibreError> {
            // Record what was uploaded.
            self.uploaded.lock().unwrap().push(rows.to_vec());

            // Sign the promise's sign bytes (CometBFT-wrapped, same for client and validators).
            use ed25519_dalek::Signer;
            let sign_bytes = promise.sign_bytes()?;
            let signature = self.ed_signing_key.sign(&sign_bytes);

            Ok(UploadResponse {
                validator_signature: signature.to_bytes().to_vec(),
            })
        }

        async fn download_shard(&self, _blob_id: &BlobID) -> Result<DownloadResponse, FibreError> {
            unimplemented!("download not needed for upload tests")
        }
    }

    struct MockValidatorConnector {
        connections: HashMap<[u8; 20], Arc<MockValidatorConnection>>,
    }

    #[async_trait::async_trait]
    impl ValidatorConnector for MockValidatorConnector {
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

    /// A connector that fails for specific validator addresses.
    struct FailingConnector {
        inner: MockValidatorConnector,
        /// Addresses that should fail to connect.
        fail_addresses: Vec<[u8; 20]>,
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

    /// Create a validator with a fresh ed25519 keypair and fixed address.
    fn make_validator(power: i64, seed: u8) -> (Ed25519SigningKey, ValidatorInfo) {
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

    /// Build a small test blob with K=4, N=4, min_row_size=64.
    fn make_test_blob() -> Blob {
        let cfg = BlobConfig::new_test(0, 4, 4, 4096, 4, 64);
        let data: Vec<u8> = (0u8..200).collect();
        Blob::new(&data, cfg).unwrap()
    }

    /// Build a FibreClientConfig suitable for tests with small parameters.
    fn test_config() -> FibreClientConfig {
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
            // With K=4, N=4 test blobs we need min_rows_per_validator to be
            // small enough that the shard assignment actually works.
            min_rows_per_validator: 1,
            max_message_size: 1 << 20,
            upload_concurrency: 10,
            download_concurrency: 10,
        }
    }

    /// Build a client with the given validator set and connector.
    fn build_client(
        val_set: ValidatorSet,
        connector: impl ValidatorConnector + 'static,
    ) -> FibreClient {
        FibreClient::builder()
            .config(test_config())
            .set_getter(MockSetGetter { val_set })
            .connector(connector)
            .build()
            .unwrap()
    }

    fn test_signing_key() -> SigningKey {
        SigningKey::random(&mut OsRng)
    }

    /// Create a MockValidatorConnector with connections for all validators.
    fn make_connector(
        validators: &[(Ed25519SigningKey, ValidatorInfo)],
    ) -> (
        MockValidatorConnector,
        HashMap<[u8; 20], Arc<MockValidatorConnection>>,
    ) {
        let mut connections: HashMap<[u8; 20], Arc<MockValidatorConnection>> = HashMap::new();
        for (ed_key, info) in validators {
            connections.insert(
                info.address,
                Arc::new(MockValidatorConnection {
                    ed_signing_key: ed_key.clone(),
                    uploaded: Mutex::new(Vec::new()),
                }),
            );
        }
        let connector = MockValidatorConnector {
            connections: connections.clone(),
        };
        (connector, connections)
    }

    #[tokio::test]
    async fn upload_fails_when_client_closed() {
        let (ed_key, val) = make_validator(100, 1);
        let validators = vec![(ed_key, val.clone())];
        let (connector, _conns) = make_connector(&validators);

        let val_set = ValidatorSet {
            validators: vec![val],
            height: 1,
        };

        let client = build_client(val_set, connector);
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
        // 3 validators with equal stake, all should succeed.
        let v1 = make_validator(100, 1);
        let v2 = make_validator(100, 2);
        let v3 = make_validator(100, 3);
        let validators = vec![v1, v2, v3];

        let val_infos: Vec<ValidatorInfo> =
            validators.iter().map(|(_, info)| info.clone()).collect();

        let (connector, _conns) = make_connector(&validators);

        let val_set = ValidatorSet {
            validators: val_infos,
            height: 42,
        };

        let sk = test_signing_key();
        let client = build_client(val_set, connector);
        let blob = make_test_blob();
        let namespace = vec![0u8; 29];

        let result = client.upload(&sk, &namespace, blob).await;
        assert!(result.is_ok(), "upload should succeed: {:?}", result.err());

        let signed = result.unwrap();
        // Promise should be properly constructed.
        assert_eq!(signed.promise.height, 42);
        assert_eq!(signed.promise.chain_id, "test-chain");

        // We should have collected signatures from validators.
        // With 3 equal-stake validators and 2/3 threshold, we need at least 2.
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
        // 3 validators with equal stake.
        let v1 = make_validator(100, 1);
        let v2 = make_validator(100, 2);
        let v3 = make_validator(100, 3);
        let validators = vec![v1, v2, v3];

        let val_infos: Vec<ValidatorInfo> =
            validators.iter().map(|(_, info)| info.clone()).collect();

        let (connector, conns) = make_connector(&validators);

        let val_set = ValidatorSet {
            validators: val_infos,
            height: 1,
        };

        let client = build_client(val_set, connector);
        let blob = make_test_blob();
        let namespace = vec![0u8; 29];

        client.upload(&test_signing_key(), &namespace, blob).await.unwrap();

        // Verify each mock validator received some rows.
        for (_, info) in &validators {
            let conn = conns.get(&info.address).unwrap();
            let uploads = conn.uploaded.lock().unwrap();
            assert!(
                !uploads.is_empty(),
                "validator {} should have received uploads",
                hex::encode(info.address)
            );
            // Each upload call should have at least 1 row proof.
            for upload_batch in uploads.iter() {
                assert!(!upload_batch.is_empty(), "upload batch should not be empty");
            }
        }
    }

    #[tokio::test]
    async fn upload_returns_when_threshold_met() {
        // 5 validators: 3 succeed, 2 fail to connect.
        // With 2/3 threshold, we need 2/3 of 500 = 333 voting power.
        // 3 validators * 100 = 300 + we need at least 334 stake, so with
        // equal stake of 100 each, 4 out of 5 needed for 2/3. But 2 fail.
        //
        // Let's adjust: give the 3 successful ones more stake so 2/3 is met.
        let v1 = make_validator(200, 1); // succeeds
        let v2 = make_validator(200, 2); // succeeds
        let v3 = make_validator(200, 3); // succeeds
        let v4 = make_validator(100, 4); // fails
        let v5 = make_validator(100, 5); // fails

        let all_validators = [v1.clone(), v2.clone(), v3.clone(), v4.clone(), v5.clone()];
        let val_infos: Vec<ValidatorInfo> = all_validators
            .iter()
            .map(|(_, info)| info.clone())
            .collect();

        // Build connector that only has connections for v1, v2, v3
        let successful = vec![v1, v2, v3];
        let (inner_connector, _conns) = make_connector(&successful);
        let fail_addresses = vec![val_infos[3].address, val_infos[4].address];

        let failing_connector = FailingConnector {
            inner: inner_connector,
            fail_addresses,
        };

        let val_set = ValidatorSet {
            validators: val_infos,
            height: 10,
        };

        let client = build_client(val_set, failing_connector);
        let blob = make_test_blob();
        let namespace = vec![0u8; 29];

        // Total voting power = 800. 2/3 threshold = 533.
        // 3 successful validators have 600 voting power > 533.
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
        // 5 validators with equal stake. Only 1 succeeds, 4 fail.
        // 2/3 threshold of 500 = 333. One validator has 100 < 333.
        let v1 = make_validator(100, 1); // succeeds
        let v2 = make_validator(100, 2); // fails
        let v3 = make_validator(100, 3); // fails
        let v4 = make_validator(100, 4); // fails
        let v5 = make_validator(100, 5); // fails

        let all_validators = [v1.clone(), v2.clone(), v3.clone(), v4.clone(), v5.clone()];
        let val_infos: Vec<ValidatorInfo> = all_validators
            .iter()
            .map(|(_, info)| info.clone())
            .collect();

        let successful = vec![v1];
        let (inner_connector, _conns) = make_connector(&successful);
        let fail_addresses = vec![
            val_infos[1].address,
            val_infos[2].address,
            val_infos[3].address,
            val_infos[4].address,
        ];

        let failing_connector = FailingConnector {
            inner: inner_connector,
            fail_addresses,
        };

        let val_set = ValidatorSet {
            validators: val_infos,
            height: 10,
        };

        let client = build_client(val_set, failing_connector);
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
        let (ed_key, val) = make_validator(100, 1);
        let validators = vec![(ed_key, val.clone())];
        let (connector, _conns) = make_connector(&validators);

        let val_set = ValidatorSet {
            validators: vec![val],
            height: 1,
        };

        let client = build_client(val_set, connector);
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
