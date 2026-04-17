//! Upload flow orchestration.
//!
//! Distributes a pre-encoded [`Blob`] to validators, collecting ed25519
//! signatures until the safety threshold is met, then returns a
//! [`SignedPaymentPromise`].
//!
//! The [`FibreClient::upload_and_prepare()`] method encodes a blob, uploads it to validators,
//! and returns a `MsgPayForFibre` ready for broadcast by the caller.

use std::sync::Arc;

use celestia_proto::celestia::fibre::v1::MsgPayForFibre;
use futures::stream::{FuturesUnordered, StreamExt};
use tokio_util::sync::CancellationToken;

use lumina_utils::cond_send::BoxFuture;

use crate::client::task::spawn_task;

use crate::blob::Blob;
use crate::client::FibreClient;
use crate::config::BlobConfig;
use crate::error::FibreError;
use crate::payment_promise::{PaymentPromise, SignedPaymentPromise};
use crate::validator::signature_set::SignatureSet;
use crate::validator::{ShardMap, ValidatorSet};

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
        let upload_size_u32 = u32::try_from(upload_size).map_err(|_| FibreError::BlobTooLarge {
            size: upload_size,
            max: u32::MAX as usize,
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

        // 5. Fan-out upload to all validators. Returns when the signature
        //    threshold is met, but spawned tasks continue uploading in the
        //    background so that every validator eventually receives its shard.
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
    /// Returns a [`MsgPayForFibre`] ready for on-chain broadcast.
    /// The caller is responsible for broadcasting the message on-chain.
    pub async fn upload_and_prepare(
        &self,
        signing_key: &k256::ecdsa::SigningKey,
        namespace: &[u8],
        data: &[u8],
        signer_address: &str,
    ) -> Result<MsgPayForFibre, FibreError> {
        if self.cancel_token.is_cancelled() {
            return Err(FibreError::ClientClosed);
        }

        // 1. Encode data into a Blob.
        let blob = Blob::new(data, BlobConfig::for_version(0)?)?;

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
        Ok(MsgPayForFibre {
            signer: signer_address.to_string(),
            payment_promise: Some((&signed_promise.promise).into()),
            validator_signatures,
        })
    }

    /// Fan-out upload of row proofs to validators in the shard map.
    ///
    /// Returns early when the signature threshold is met, but spawned upload
    /// tasks continue running in the background so that every validator
    /// eventually receives its shard. Individual upload failures are logged
    /// but not fatal.
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

        // Phase 1: Spawn all upload tasks up-front so that every validator is
        // contacted regardless of how quickly the signature threshold is met.
        for (val_idx, row_indices) in validator_tasks {
            let permit = self
                .upload_semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| FibreError::Other("upload semaphore closed".into()))?;

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
                let resp = conn.upload_shard(&promise, &proofs, &rlc_coeffs).await?;
                Ok(resp.validator_signature)
            });
        }

        // Phase 2: Collect results until the signature threshold is met.
        // Already-spawned tasks continue uploading in the background after
        // this function returns.
        loop {
            if futures.is_empty() {
                break;
            }

            tokio::select! {
                task_result = futures.next() => {
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
