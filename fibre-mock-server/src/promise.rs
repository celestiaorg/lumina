//! Rebuild a domain [`celestia_fibre::PaymentPromise`] from its proto form so
//! the mock can compute the exact `sign_bytes()` the uploading client signed.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use celestia_fibre::PaymentPromise;
use celestia_proto::celestia::fibre::v1 as proto;
use tendermint_proto::google::protobuf::Timestamp;
use tonic::Status;

/// Rebuild the domain promise; every field must round-trip exactly or the
/// signature the mock returns will not verify against the client's sign bytes.
pub fn promise_from_proto(pp: proto::PaymentPromise) -> Result<PaymentPromise, Status> {
    let namespace = celestia_types::nmt::Namespace::from_raw(&pp.namespace)
        .map_err(|e| Status::invalid_argument(format!("invalid namespace: {e}")))?;

    let commitment: [u8; 32] = pp
        .commitment
        .as_slice()
        .try_into()
        .map_err(|_| Status::invalid_argument("commitment must be 32 bytes"))?;

    let ts = pp
        .creation_timestamp
        .ok_or_else(|| Status::invalid_argument("missing creation_timestamp"))?;
    let creation_timestamp = timestamp_to_system_time(&ts)?;

    let pk = pp
        .signer_public_key
        .ok_or_else(|| Status::invalid_argument("missing signer_public_key"))?;
    let signer_pubkey = k256::ecdsa::VerifyingKey::from_sec1_bytes(&pk.key)
        .map_err(|e| Status::invalid_argument(format!("invalid signer public key: {e}")))?;

    Ok(PaymentPromise {
        chain_id: pp.chain_id,
        height: pp.height as u64,
        namespace,
        upload_size: pp.blob_size,
        blob_version: pp.blob_version,
        commitment,
        creation_timestamp,
        signer_pubkey,
        signature: (!pp.signature.is_empty()).then_some(pp.signature),
    })
}

/// Lossless inverse of the client's `system_time_to_timestamp`
/// (`fibre/src/transport/proto_conv.rs`). Must be exact: the Go
/// `time.MarshalBinary` of this value is part of the signed bytes.
fn timestamp_to_system_time(t: &Timestamp) -> Result<SystemTime, Status> {
    if t.seconds >= 0 {
        let d = Duration::new(t.seconds as u64, t.nanos as u32);
        UNIX_EPOCH
            .checked_add(d)
            .ok_or_else(|| Status::invalid_argument("timestamp overflow"))
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
            .ok_or_else(|| Status::invalid_argument("timestamp underflow"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_bytes_roundtrip_through_proto() {
        let sk = k256::ecdsa::SigningKey::from_slice(&[7u8; 32]).unwrap();
        let pp = PaymentPromise {
            chain_id: "mock-1".into(),
            height: 42,
            namespace: celestia_types::nmt::Namespace::new_v0(b"mock-test").unwrap(),
            upload_size: 1024,
            blob_version: 0,
            commitment: [9u8; 32],
            creation_timestamp: SystemTime::now(),
            signer_pubkey: *sk.verifying_key(),
            signature: Some(vec![1u8; 64]),
        };

        let rebuilt = promise_from_proto(proto::PaymentPromise::from(&pp)).unwrap();
        assert_eq!(rebuilt.sign_bytes().unwrap(), pp.sign_bytes().unwrap());
    }
}
