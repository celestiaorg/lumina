//! Conversions between domain types and protobuf types.
//!
//! These functions convert between the crate's domain types and the generated
//! protobuf types from `celestia-proto`. They are used by the gRPC transport
//! layer ([`crate::grpc_validator_client`]) and the put flow ([`crate::upload`]).

#[cfg(test)]
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use celestia_proto::celestia::fibre::v1 as proto;
use celestia_proto::cosmos::crypto::secp256k1::PubKey as ProtoPubKey;
use tendermint_proto::google::protobuf::Timestamp;
#[cfg(test)]
use tendermint_proto::v0_38::crypto::public_key::Sum as CryptoKeySum;

use crate::error::FibreError;
use crate::payment_promise::PaymentPromise;
#[cfg(test)]
use crate::validator::ValidatorInfo;

impl From<&PaymentPromise> for proto::PaymentPromise {
    fn from(pp: &PaymentPromise) -> Self {
        let creation_timestamp = system_time_to_timestamp(pp.creation_timestamp);

        let signer_public_key = Some(ProtoPubKey {
            key: pp.signer_pubkey.to_encoded_point(true).as_bytes().to_vec(),
        });

        proto::PaymentPromise {
            chain_id: pp.chain_id.clone(),
            height: pp.height as i64,
            namespace: pp.namespace.clone(),
            blob_size: pp.upload_size,
            blob_version: pp.blob_version,
            commitment: pp.commitment.to_vec(),
            creation_timestamp: Some(creation_timestamp),
            signer_public_key,
            signature: pp.signature.clone().unwrap_or_default(),
        }
    }
}

/// Convert a [`rsema1d::RowInclusionProof`] to a proto [`proto::BlobRow`].
pub(crate) fn row_proof_to_blob_row(proof: &rsema1d::RowInclusionProof) -> proto::BlobRow {
    proto::BlobRow {
        index: proof.index as u32,
        data: proof.row.clone(),
        proof: proof.row_proof.iter().map(|h| h.to_vec()).collect(),
    }
}

/// Convert a proto [`proto::BlobRow`] and an RLC root into a
/// [`rsema1d::RowInclusionProof`].
pub(crate) fn blob_row_to_row_proof(
    row: proto::BlobRow,
    rlc_root: [u8; 32],
) -> Result<rsema1d::RowInclusionProof, FibreError> {
    let row_proof = row
        .proof
        .into_iter()
        .map(|h| {
            let len = h.len();
            h.try_into().map_err(|_| {
                FibreError::InvalidData(
                    format!("proof hash has invalid length {len}, expected 32",),
                )
            })
        })
        .collect::<Result<Vec<[u8; 32]>, FibreError>>()?;

    Ok(rsema1d::RowInclusionProof {
        index: row.index as usize,
        row: row.data,
        row_proof,
        rlc_root,
    })
}

/// Build a proto [`proto::BlobShard`] for an upload request.
///
/// Upload shards carry RLC coefficients (not a root) so the validator can
/// verify each row without having enough rows to reconstruct.
pub(crate) fn build_upload_shard(
    proofs: &[rsema1d::RowInclusionProof],
    rlc_coeffs: &[rsema1d::GF128],
) -> proto::BlobShard {
    let rows = proofs.iter().map(row_proof_to_blob_row).collect();

    // Flatten RLC coefficients: 16 bytes per original row (GF128 → bytes)
    let mut coefficients = Vec::with_capacity(rlc_coeffs.len() * 16);
    for coeff in rlc_coeffs {
        coefficients.extend_from_slice(&coeff.to_bytes());
    }

    proto::BlobShard {
        rows,
        rlc: Some(proto::blob_shard::Rlc::Coefficients(coefficients)),
    }
}

/// Parse a proto [`proto::DownloadShardResponse`] into a list of
/// [`rsema1d::RowInclusionProof`]s.
///
/// Download shards carry an RLC root (not coefficients) — the client has
/// enough rows to reconstruct and can verify the root after reconstruction.
pub(crate) fn parse_download_response(
    resp: proto::DownloadShardResponse,
) -> Result<Vec<rsema1d::RowInclusionProof>, FibreError> {
    let shard = resp
        .shard
        .ok_or_else(|| FibreError::InvalidData("download response missing shard".into()))?;

    let rlc_root = match shard.rlc {
        Some(proto::blob_shard::Rlc::Root(ref root)) => {
            let arr: [u8; 32] = root.as_slice().try_into().map_err(|_| {
                FibreError::InvalidData(format!(
                    "rlc root has invalid length {}, expected 32",
                    root.len()
                ))
            })?;
            arr
        }
        _ => {
            return Err(FibreError::InvalidData(
                "download response shard missing rlc root".into(),
            ));
        }
    };

    shard
        .rows
        .into_iter()
        .map(|row| blob_row_to_row_proof(row, rlc_root))
        .collect()
}

fn system_time_to_timestamp(t: SystemTime) -> Timestamp {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => Timestamp {
            seconds: d.as_secs() as i64,
            nanos: d.subsec_nanos() as i32,
        },
        Err(e) => {
            // Before epoch — use protobuf convention where nanos is
            // always non-negative: seconds = -(secs+1), nanos = 1e9 - subsec.
            let d = e.duration();
            let subsec = d.subsec_nanos();
            if subsec == 0 {
                Timestamp {
                    seconds: -(d.as_secs() as i64),
                    nanos: 0,
                }
            } else {
                Timestamp {
                    seconds: -(d.as_secs() as i64) - 1,
                    nanos: (1_000_000_000 - subsec) as i32,
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn timestamp_to_system_time(t: &Timestamp) -> Result<SystemTime, FibreError> {
    if t.seconds >= 0 {
        let d = Duration::new(t.seconds as u64, t.nanos as u32);
        UNIX_EPOCH
            .checked_add(d)
            .ok_or_else(|| FibreError::Other("timestamp overflow".into()))
    } else {
        // Reverse the protobuf convention: if nanos > 0 the actual
        // duration is (|seconds| - 1) seconds + (1e9 - nanos) subsec nanos.
        let (secs, nanos) = if t.nanos > 0 {
            ((-t.seconds - 1) as u64, (1_000_000_000 - t.nanos) as u32)
        } else {
            ((-t.seconds) as u64, 0u32)
        };
        let d = Duration::new(secs, nanos);
        UNIX_EPOCH
            .checked_sub(d)
            .ok_or_else(|| FibreError::Other("timestamp underflow".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k256::ecdsa::SigningKey;
    use rand::rngs::OsRng;

    #[test]
    fn payment_promise_to_proto_roundtrip_fields() {
        let sk = SigningKey::random(&mut OsRng);
        let pp = PaymentPromise {
            chain_id: "test-chain".into(),
            height: 42,
            namespace: vec![0u8; 29],
            upload_size: 1024,
            blob_version: 0,
            commitment: [7u8; 32],
            creation_timestamp: SystemTime::now(),
            signer_pubkey: *sk.verifying_key(),
            signature: Some(vec![1u8; 64]),
        };

        let proto_pp = proto::PaymentPromise::from(&pp);
        assert_eq!(proto_pp.chain_id, "test-chain");
        assert_eq!(proto_pp.height, 42);
        assert_eq!(proto_pp.namespace, vec![0u8; 29]);
        assert_eq!(proto_pp.blob_size, 1024);
        assert_eq!(proto_pp.blob_version, 0);
        assert_eq!(proto_pp.commitment, vec![7u8; 32]);
        assert_eq!(proto_pp.signature, vec![1u8; 64]);

        let ts = proto_pp.creation_timestamp.unwrap();
        assert!(ts.seconds > 0);

        let pk = proto_pp.signer_public_key.unwrap();
        assert_eq!(pk.key.len(), 33); // compressed secp256k1
    }

    #[test]
    fn row_proof_blob_row_roundtrip() {
        let proof = rsema1d::RowInclusionProof {
            index: 5,
            row: vec![42u8; 64],
            row_proof: vec![[1u8; 32], [2u8; 32]],
            rlc_root: [3u8; 32],
        };

        let blob_row = row_proof_to_blob_row(&proof);
        assert_eq!(blob_row.index, 5);
        assert_eq!(blob_row.data, vec![42u8; 64]);
        assert_eq!(blob_row.proof.len(), 2);

        let back = blob_row_to_row_proof(blob_row, [3u8; 32]).unwrap();
        assert_eq!(back.index, 5);
        assert_eq!(back.row, vec![42u8; 64]);
        assert_eq!(back.row_proof, vec![[1u8; 32], [2u8; 32]]);
        assert_eq!(back.rlc_root, [3u8; 32]);
    }

    #[test]
    fn blob_row_to_row_proof_invalid_hash_length() {
        let row = proto::BlobRow {
            index: 0,
            data: vec![0u8; 64],
            proof: vec![vec![0u8; 31]], // wrong length
        };
        let result = blob_row_to_row_proof(row, [0u8; 32]);
        assert!(result.is_err());
    }

    #[test]
    fn build_upload_shard_includes_coefficients() {
        let proofs = vec![rsema1d::RowInclusionProof {
            index: 0,
            row: vec![0u8; 64],
            row_proof: vec![[0u8; 32]],
            rlc_root: [0u8; 32],
        }];
        let coeffs = vec![rsema1d::GF128::zero(); 2];

        let shard = build_upload_shard(&proofs, &coeffs);
        assert_eq!(shard.rows.len(), 1);
        match shard.rlc {
            Some(proto::blob_shard::Rlc::Coefficients(c)) => {
                assert_eq!(c.len(), 32); // 2 coefficients × 16 bytes
            }
            _ => panic!("expected Coefficients variant"),
        }
    }

    #[test]
    fn parse_download_response_success() {
        let rlc_root = [9u8; 32];
        let resp = proto::DownloadShardResponse {
            shard: Some(proto::BlobShard {
                rows: vec![proto::BlobRow {
                    index: 3,
                    data: vec![1u8; 64],
                    proof: vec![vec![2u8; 32]],
                }],
                rlc: Some(proto::blob_shard::Rlc::Root(rlc_root.to_vec())),
            }),
        };

        let proofs = parse_download_response(resp).unwrap();
        assert_eq!(proofs.len(), 1);
        assert_eq!(proofs[0].index, 3);
        assert_eq!(proofs[0].rlc_root, rlc_root);
    }

    #[test]
    fn parse_download_response_missing_shard() {
        let resp = proto::DownloadShardResponse { shard: None };
        assert!(parse_download_response(resp).is_err());
    }

    #[test]
    fn parse_download_response_missing_rlc_root() {
        let resp = proto::DownloadShardResponse {
            shard: Some(proto::BlobShard {
                rows: vec![],
                rlc: None,
            }),
        };
        assert!(parse_download_response(resp).is_err());
    }

    #[test]
    fn timestamp_roundtrip() {
        let now = SystemTime::now();
        let ts = system_time_to_timestamp(now);
        let back = timestamp_to_system_time(&ts).unwrap();

        // Compare with nanosecond tolerance
        let diff = now
            .duration_since(back)
            .or_else(|_| back.duration_since(now))
            .unwrap();
        assert!(diff < Duration::from_micros(1));
    }

    #[test]
    fn pre_epoch_timestamp_roundtrip() {
        // 1969-06-15 00:00:00.500 UTC → 0.5s before some whole-second boundary
        let t = UNIX_EPOCH - Duration::new(10, 500_000_000);
        let ts = system_time_to_timestamp(t);

        // Protobuf convention: nanos must be non-negative
        assert!(ts.nanos >= 0, "nanos must be non-negative: {}", ts.nanos);
        assert_eq!(ts.seconds, -11);
        assert_eq!(ts.nanos, 500_000_000);

        let back = timestamp_to_system_time(&ts).unwrap();
        let diff = t
            .duration_since(back)
            .or_else(|_| back.duration_since(t))
            .unwrap();
        assert_eq!(diff.as_nanos(), 0, "pre-epoch roundtrip lost precision");
    }

    #[test]
    fn pre_epoch_timestamp_exact_second() {
        let t = UNIX_EPOCH - Duration::new(5, 0);
        let ts = system_time_to_timestamp(t);
        assert_eq!(ts.seconds, -5);
        assert_eq!(ts.nanos, 0);

        let back = timestamp_to_system_time(&ts).unwrap();
        let diff = t
            .duration_since(back)
            .or_else(|_| back.duration_since(t))
            .unwrap();
        assert_eq!(diff.as_nanos(), 0);
    }

    #[test]
    fn validator_from_proto_valid() {
        // Generate a random ed25519 key using raw bytes
        let mut secret = [0u8; 32];
        rand::RngCore::fill_bytes(&mut OsRng, &mut secret);
        let sk = ed25519_dalek::SigningKey::from_bytes(&secret);
        let pk = sk.verifying_key();

        let proto_val = tendermint_proto::v0_38::types::Validator {
            address: vec![0u8; 20], // not used in conversion (derived from pubkey)
            pub_key: Some(tendermint_proto::v0_38::crypto::PublicKey {
                sum: Some(CryptoKeySum::Ed25519(pk.as_bytes().to_vec())),
            }),
            voting_power: 100,
            proposer_priority: 0,
        };

        let info = ValidatorInfo::try_from(&proto_val).unwrap();
        assert_eq!(info.pubkey, pk);
        assert_eq!(info.voting_power, 100);
        // Verify address is derived from pubkey
        use sha2::{Digest, Sha256};
        let expected_addr: [u8; 20] = Sha256::digest(pk.as_bytes())[..20].try_into().unwrap();
        assert_eq!(info.address, expected_addr);
    }

    #[test]
    fn validator_from_proto_missing_key() {
        let proto_val = tendermint_proto::v0_38::types::Validator {
            address: vec![],
            pub_key: None,
            voting_power: 100,
            proposer_priority: 0,
        };
        assert!(ValidatorInfo::try_from(&proto_val).is_err());
    }
}
