//! PaymentPromise and SignedPaymentPromise for fibre blob payment commitments.
//!
//! A `PaymentPromise` is a signed commitment to pay for storing a fibre blob.
//! The signer creates the promise, signs it with their secp256k1 key, and
//! validators counter-sign to confirm they received and stored the data.

use std::time::SystemTime;

use k256::ecdsa::signature::Signer;
use k256::ecdsa::{Signature, SigningKey, VerifyingKey};
use sha2::{Digest, Sha256};

use crate::blob::Commitment;
use crate::error::FibreError;

/// Domain separation prefix prepended to sign bytes.
/// Ensures payment promise signatures cannot be confused with consensus messages.
pub const SIGN_BYTES_PREFIX: &[u8] = b"fibre/pp:v0";

/// Size of the secp256k1 compressed public key (33 bytes).
const PUBKEY_SIZE: usize = 33;

/// Size of the Celestia namespace (29 bytes).
const NAMESPACE_SIZE: usize = 29;

/// Size of a secp256k1 signature in compact format (32 bytes r + 32 bytes s).
const SIGNATURE_SIZE: usize = 64;

/// Maximum allowed chain ID length.
const MAX_CHAIN_ID_SIZE: usize = 20;

/// Size of the Go time.Time MarshalBinary output for UTC times.
const TIMESTAMP_BINARY_SIZE: usize = 15;

/// Fixed-size portion of sign bytes (excluding prefix and variable-length chain_id).
const SIGN_BYTES_FIXED_SIZE: usize =
    PUBKEY_SIZE + NAMESPACE_SIZE + 4 + 32 + 4 + 8 + TIMESTAMP_BINARY_SIZE;

/// A promise to pay for fibre blob storage.
///
/// Contains all the information needed to identify the blob, the signer,
/// and the terms of payment. The `signature` field is set by calling `sign()`.
#[derive(Debug, Clone)]
pub struct PaymentPromise {
    /// Chain identifier for domain separation.
    pub chain_id: String,
    /// Height used to determine the validator set.
    pub height: u64,
    /// The 29-byte namespace the blob is associated with.
    pub namespace: Vec<u8>,
    /// Upload size of the blob (with padding, without parity), matching `Blob::upload_size()`.
    pub upload_size: u32,
    /// Version of the blob format.
    pub blob_version: u32,
    /// The blob commitment hash.
    pub commitment: Commitment,
    /// Timestamp when this promise was created.
    pub creation_timestamp: SystemTime,
    /// The secp256k1 public key of the signer (escrow account owner).
    pub signer_pubkey: VerifyingKey,
    /// The signer's signature over the sign bytes (64-byte compact secp256k1 signature).
    /// `None` if not yet signed.
    pub signature: Option<Vec<u8>>,
}

impl PaymentPromise {
    /// Compute the stripped sign bytes (payload only, no prefix or chain ID).
    ///
    /// Concatenates all promise fields in canonical order. The result is
    /// suitable for wrapping with CometBFT domain separation via
    /// [`raw_bytes_message_sign_bytes`].
    fn stripped_sign_bytes(&self) -> Result<Vec<u8>, FibreError> {
        let timestamp_bytes = marshal_binary_time(self.creation_timestamp)?;

        let mut buf = Vec::with_capacity(SIGN_BYTES_FIXED_SIZE);

        // Append signer_bytes (33 bytes - compressed public key)
        let pubkey_bytes = self.signer_pubkey.to_sec1_bytes();
        debug_assert_eq!(pubkey_bytes.len(), PUBKEY_SIZE);
        buf.extend_from_slice(&pubkey_bytes);

        // Append namespace (29 bytes)
        buf.extend_from_slice(&self.namespace);

        // Append blob_size (4 bytes, big-endian)
        buf.extend_from_slice(&self.upload_size.to_be_bytes());

        // Append commitment (32 bytes)
        buf.extend_from_slice(&self.commitment);

        // Append blob_version (4 bytes, big-endian)
        buf.extend_from_slice(&self.blob_version.to_be_bytes());

        // Append height (8 bytes, big-endian)
        buf.extend_from_slice(&self.height.to_be_bytes());

        // Append timestamp bytes (15 bytes)
        buf.extend_from_slice(&timestamp_bytes);

        Ok(buf)
    }

    /// Compute the canonical sign bytes for this payment promise.
    ///
    /// Wraps [`stripped_sign_bytes()`](Self::stripped_sign_bytes) with CometBFT's
    /// `RawBytesMessageSignBytes` domain separation.
    ///
    /// Both the client signer and validators sign these same bytes.
    pub fn sign_bytes(&self) -> Result<Vec<u8>, FibreError> {
        let stripped = self.stripped_sign_bytes()?;
        Ok(raw_bytes_message_sign_bytes(
            &self.chain_id,
            SIGN_BYTES_PREFIX,
            &stripped,
        ))
    }

    /// Sign this promise with the given signing key.
    ///
    /// Computes the sign bytes and signs them using secp256k1 ECDSA.
    /// Sets `self.signature` to the 64-byte compact signature.
    pub fn sign(&mut self, signing_key: &SigningKey) -> Result<(), FibreError> {
        let sign_bytes = self.sign_bytes()?;

        tracing::debug!(
            sign_bytes_hex = %hex::encode(&sign_bytes),
            sign_bytes_len = sign_bytes.len(),
            chain_id = %self.chain_id,
            height = self.height,
            upload_size = self.upload_size,
            blob_version = self.blob_version,
            namespace_hex = %hex::encode(&self.namespace),
            commitment_hex = %hex::encode(self.commitment),
            pubkey_hex = %hex::encode(self.signer_pubkey.to_sec1_bytes().as_ref()),
            "signing payment promise"
        );

        let signature: Signature = signing_key.sign(&sign_bytes);
        self.signature = Some(signature.to_bytes().to_vec());

        tracing::debug!(
            signature_hex = %hex::encode(self.signature.as_ref().unwrap()),
            "payment promise signed"
        );

        Ok(())
    }

    /// Validate all fields and verify the signature.
    ///
    /// Performs stateless validation of all field constraints and verifies
    /// the signature using the signer's public key.
    pub fn validate(&self) -> Result<(), FibreError> {
        // Chain ID must not be empty
        if self.chain_id.is_empty() {
            return Err(FibreError::InvalidPaymentPromise(
                "chain id must not be empty".into(),
            ));
        }

        // Chain ID length limit
        if self.chain_id.len() > MAX_CHAIN_ID_SIZE {
            return Err(FibreError::InvalidPaymentPromise(format!(
                "chain id length {} exceeds maximum {}",
                self.chain_id.len(),
                MAX_CHAIN_ID_SIZE
            )));
        }

        // Upload size must be positive
        if self.upload_size == 0 {
            return Err(FibreError::InvalidPaymentPromise(
                "upload size must be positive".into(),
            ));
        }

        // Creation timestamp must not be Unix epoch (Go's zero value check)
        if self.creation_timestamp == SystemTime::UNIX_EPOCH {
            return Err(FibreError::InvalidPaymentPromise(
                "creation timestamp must not be zero".into(),
            ));
        }

        // Height must be positive
        if self.height == 0 {
            return Err(FibreError::InvalidPaymentPromise(format!(
                "height must be positive, got {}",
                self.height
            )));
        }

        // Signature must be present and correct size
        let signature_bytes = self
            .signature
            .as_ref()
            .ok_or_else(|| FibreError::InvalidPaymentPromise("signature must be present".into()))?;

        if signature_bytes.len() != SIGNATURE_SIZE {
            return Err(FibreError::InvalidPaymentPromise(format!(
                "signature must be {} bytes, got {}",
                SIGNATURE_SIZE,
                signature_bytes.len()
            )));
        }

        // Verify signature
        let sign_bytes = self.sign_bytes()?;

        let signature = Signature::from_slice(signature_bytes).map_err(|e| {
            FibreError::InvalidPaymentPromise(format!("invalid signature format: {}", e))
        })?;

        use k256::ecdsa::signature::Verifier;
        self.signer_pubkey
            .verify(&sign_bytes, &signature)
            .map_err(|_| {
                FibreError::InvalidPaymentPromise("signature verification failed".into())
            })?;

        Ok(())
    }

    /// Compute the SHA-256 hash of `sign_bytes || signature`.
    ///
    /// The signature must be set before calling this method.
    pub fn hash(&self) -> Result<[u8; 32], FibreError> {
        let signature = self.signature.as_ref().ok_or_else(|| {
            FibreError::InvalidPaymentPromise("signature must be set before computing hash".into())
        })?;

        let sign_bytes = self.sign_bytes()?;

        let mut hasher = Sha256::new();
        hasher.update(&sign_bytes);
        hasher.update(signature);
        Ok(hasher.finalize().into())
    }
}

/// A `PaymentPromise` together with validator signatures confirming the promise.
#[derive(Debug, Clone)]
pub struct SignedPaymentPromise {
    /// The payment commitment promise that was signed by validators.
    pub promise: PaymentPromise,
    /// Signatures from validators confirming they received and stored the blob.
    /// Ordered by validator set position; `None` entries for non-signers.
    pub validator_signatures: Vec<Option<Vec<u8>>>,
}

/// CometBFT's domain separation prefix for raw-bytes signing.
const COMET_RAW_BYTES_PREFIX: &[u8] = b"COMET::RAW_BYTES::SIGN";

/// Construct the CometBFT domain-separated sign bytes for raw byte messages.
fn raw_bytes_message_sign_bytes(chain_id: &str, unique_id: &[u8], raw_bytes: &[u8]) -> Vec<u8> {
    use celestia_proto::tendermint_celestia_mods::privval::SignRawBytesRequest;
    use prost::Message;

    let request = SignRawBytesRequest {
        chain_id: chain_id.to_string(),
        raw_bytes: raw_bytes.to_vec(),
        unique_id: String::from_utf8_lossy(unique_id).into_owned(),
    };

    let mut result = Vec::with_capacity(COMET_RAW_BYTES_PREFIX.len() + request.encoded_len() + 5);
    result.extend_from_slice(COMET_RAW_BYTES_PREFIX);
    request
        .encode_length_delimited(&mut result)
        .expect("encoding SignRawBytesRequest into Vec cannot fail");

    result
}

/// Seconds between Go's internal epoch (January 1, year 1) and Unix epoch (January 1, 1970).
const UNIX_TO_INTERNAL: i64 = (1969 * 365 + 1969 / 4 - 1969 / 100 + 1969 / 400) * 86400;

/// Marshal a `SystemTime` into Go's `time.Time.MarshalBinary()` format.
fn marshal_binary_time(t: SystemTime) -> Result<Vec<u8>, FibreError> {
    let duration = t
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| FibreError::InvalidPaymentPromise("timestamp before Unix epoch".into()))?;

    // Convert from Unix epoch to Go's internal epoch (year 1)
    let secs = duration.as_secs() as i64 + UNIX_TO_INTERNAL;
    let nsecs = duration.subsec_nanos() as i32;

    let mut buf = Vec::with_capacity(TIMESTAMP_BINARY_SIZE);

    // Version byte
    buf.push(1);

    // Seconds since year 1 as big-endian int64
    buf.extend_from_slice(&secs.to_be_bytes());

    // Nanoseconds as big-endian int32
    buf.extend_from_slice(&nsecs.to_be_bytes());

    // Timezone offset: -1 for UTC (Go's sentinel value)
    // In Go, UTC times get offsetMin = -1, which is encoded as big-endian int16
    buf.extend_from_slice(&(-1i16).to_be_bytes());

    debug_assert_eq!(buf.len(), TIMESTAMP_BINARY_SIZE);
    Ok(buf)
}

/// Unmarshal a timestamp from Go's `time.Time.MarshalBinary()` format back to `SystemTime`.
#[cfg(test)]
fn unmarshal_binary_time(data: &[u8]) -> Result<SystemTime, FibreError> {
    if data.len() != TIMESTAMP_BINARY_SIZE {
        return Err(FibreError::Other(format!(
            "timestamp binary must be {} bytes, got {}",
            TIMESTAMP_BINARY_SIZE,
            data.len()
        )));
    }

    if data[0] != 1 {
        return Err(FibreError::Other(format!(
            "unsupported timestamp version: {}",
            data[0]
        )));
    }

    // Seconds since year 1 (Go's internal epoch)
    let internal_secs = i64::from_be_bytes(data[1..9].try_into().unwrap());
    let nsecs = i32::from_be_bytes(data[9..13].try_into().unwrap());

    // Convert from Go's internal epoch to Unix epoch
    let unix_secs = internal_secs - UNIX_TO_INTERNAL;

    if unix_secs < 0 {
        return Err(FibreError::Other(
            "timestamp before Unix epoch not supported".into(),
        ));
    }
    if nsecs < 0 {
        return Err(FibreError::Other(
            "negative timestamp nanoseconds not supported".into(),
        ));
    }

    let duration = std::time::Duration::new(unix_secs as u64, nsecs as u32);
    Ok(SystemTime::UNIX_EPOCH + duration)
}

#[cfg(test)]
mod tests {
    use super::*;
    use k256::ecdsa::SigningKey;
    use rand::rngs::OsRng;

    fn make_test_promise() -> (SigningKey, PaymentPromise) {
        let signing_key = SigningKey::random(&mut OsRng);
        let verifying_key = *signing_key.verifying_key();

        let promise = PaymentPromise {
            chain_id: "test-chain-1".to_string(),
            height: 12345,
            namespace: vec![0u8; NAMESPACE_SIZE],
            upload_size: 1024,
            blob_version: 0,
            commitment: [1u8; 32],
            creation_timestamp: SystemTime::now(),
            signer_pubkey: verifying_key,
            signature: None,
        };

        (signing_key, promise)
    }

    #[test]
    fn stripped_sign_bytes_format() {
        let (_, promise) = make_test_promise();
        let stripped = promise.stripped_sign_bytes().unwrap();

        // stripped_sign_bytes has no prefix or chain_id
        assert_eq!(stripped.len(), SIGN_BYTES_FIXED_SIZE);
    }

    #[test]
    fn sign_bytes_uses_comet_wrapper() {
        let (_, promise) = make_test_promise();
        let sign_bytes = promise.sign_bytes().unwrap();

        // sign_bytes starts with "COMET::RAW_BYTES::SIGN"
        assert_eq!(
            &sign_bytes[..COMET_RAW_BYTES_PREFIX.len()],
            COMET_RAW_BYTES_PREFIX
        );
    }

    #[test]
    fn stripped_sign_bytes_field_layout() {
        let (_, promise) = make_test_promise();
        let stripped = promise.stripped_sign_bytes().unwrap();

        let mut offset = 0;

        // Signer pubkey (33 bytes)
        let expected_pubkey = promise.signer_pubkey.to_sec1_bytes();
        assert_eq!(
            &stripped[offset..offset + PUBKEY_SIZE],
            expected_pubkey.as_ref()
        );
        offset += PUBKEY_SIZE;

        // Namespace (29 bytes)
        assert_eq!(
            &stripped[offset..offset + NAMESPACE_SIZE],
            &promise.namespace
        );
        offset += NAMESPACE_SIZE;

        // Upload size (4 bytes BE)
        assert_eq!(
            &stripped[offset..offset + 4],
            &promise.upload_size.to_be_bytes()
        );
        offset += 4;

        // Commitment (32 bytes)
        assert_eq!(&stripped[offset..offset + 32], &promise.commitment);
        offset += 32;

        // Blob version (4 bytes BE)
        assert_eq!(
            &stripped[offset..offset + 4],
            &promise.blob_version.to_be_bytes()
        );
        offset += 4;

        // Height (8 bytes BE)
        assert_eq!(
            &stripped[offset..offset + 8],
            &promise.height.to_be_bytes()
        );
        offset += 8;

        // Timestamp (15 bytes)
        assert_eq!(stripped.len() - offset, TIMESTAMP_BINARY_SIZE);
    }

    #[test]
    fn sign_and_validate() {
        let (signing_key, mut promise) = make_test_promise();

        // Sign
        promise.sign(&signing_key).unwrap();
        assert!(promise.signature.is_some());
        assert_eq!(promise.signature.as_ref().unwrap().len(), SIGNATURE_SIZE);

        // Validate
        promise.validate().unwrap();
    }

    #[test]
    fn validate_fails_without_signature() {
        let (_, promise) = make_test_promise();
        assert!(promise.validate().is_err());
    }

    #[test]
    fn validate_fails_with_wrong_key() {
        let (signing_key, mut promise) = make_test_promise();
        promise.sign(&signing_key).unwrap();

        // Change the public key to a different one
        let wrong_key = SigningKey::random(&mut OsRng);
        promise.signer_pubkey = *wrong_key.verifying_key();

        assert!(promise.validate().is_err());
    }

    #[test]
    fn validate_fails_with_empty_chain_id() {
        let (signing_key, mut promise) = make_test_promise();
        promise.chain_id = String::new();
        promise.sign(&signing_key).unwrap();
        assert!(promise.validate().is_err());
    }

    #[test]
    fn validate_fails_with_zero_upload_size() {
        let (signing_key, mut promise) = make_test_promise();
        promise.upload_size = 0;
        promise.sign(&signing_key).unwrap();
        assert!(promise.validate().is_err());
    }

    #[test]
    fn validate_fails_with_zero_height() {
        let (signing_key, mut promise) = make_test_promise();
        promise.height = 0;
        promise.sign(&signing_key).unwrap();
        assert!(promise.validate().is_err());
    }

    #[test]
    fn hash_requires_signature() {
        let (_, promise) = make_test_promise();
        assert!(promise.hash().is_err());
    }

    #[test]
    fn hash_is_deterministic() {
        let (signing_key, mut promise) = make_test_promise();
        promise.sign(&signing_key).unwrap();

        let h1 = promise.hash().unwrap();
        let h2 = promise.hash().unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_changes_with_different_data() {
        let (signing_key, mut promise1) = make_test_promise();
        promise1.sign(&signing_key).unwrap();
        let h1 = promise1.hash().unwrap();

        let mut promise2 = promise1.clone();
        promise2.upload_size = 2048;
        // Signature is still from promise1, so hash will differ because sign_bytes differ
        let h2 = promise2.hash().unwrap();
        assert_ne!(h1, h2);
    }

    #[test]
    fn marshal_unmarshal_time_roundtrip() {
        let now = SystemTime::now();
        let bytes = marshal_binary_time(now).unwrap();
        assert_eq!(bytes.len(), TIMESTAMP_BINARY_SIZE);
        assert_eq!(bytes[0], 1); // version

        let recovered = unmarshal_binary_time(&bytes).unwrap();

        // Check precision is maintained (nanosecond-level)
        let diff = now
            .duration_since(recovered)
            .or_else(|_| recovered.duration_since(now))
            .unwrap();
        assert_eq!(diff.as_nanos(), 0, "timestamp roundtrip lost precision");
    }

    #[test]
    fn timestamp_utc_offset() {
        let t = SystemTime::UNIX_EPOCH + std::time::Duration::new(1700000000, 500_000_000);
        let bytes = marshal_binary_time(t).unwrap();

        // Check the UTC offset sentinel (-1)
        let offset = i16::from_be_bytes(bytes[13..15].try_into().unwrap());
        assert_eq!(offset, -1);

        // Check seconds (since year 1, Go's internal epoch)
        let secs = i64::from_be_bytes(bytes[1..9].try_into().unwrap());
        // Unix 1700000000 + UNIX_TO_INTERNAL (62135596800) = 63835596800
        assert_eq!(secs, 1700000000 + UNIX_TO_INTERNAL);

        // Check nanoseconds
        let nsecs = i32::from_be_bytes(bytes[9..13].try_into().unwrap());
        assert_eq!(nsecs, 500_000_000);
    }

    #[test]
    fn sign_bytes_consistency() {
        let (_, promise) = make_test_promise();

        // sign_bytes should be deterministic
        let sb1 = promise.sign_bytes().unwrap();
        let sb2 = promise.sign_bytes().unwrap();
        assert_eq!(sb1, sb2);
    }

    #[test]
    fn signed_payment_promise_construction() {
        let (signing_key, mut promise) = make_test_promise();
        promise.sign(&signing_key).unwrap();

        let signed = SignedPaymentPromise {
            promise: promise.clone(),
            validator_signatures: vec![None, Some(vec![0u8; 64]), None],
        };

        assert_eq!(signed.validator_signatures.len(), 3);
        assert!(signed.validator_signatures[0].is_none());
        assert!(signed.validator_signatures[1].is_some());
        assert!(signed.validator_signatures[2].is_none());
    }

    /// Cross-language compatibility test.
    ///
    /// Uses a deterministic private key and fixed field values so the output
    /// can be reproduced in Go to verify sign_bytes and signature compatibility.
    #[test]
    fn cross_language_sign_bytes_deterministic() {
        // Use a deterministic private key (32 bytes, all 0x01).
        let key_bytes = [0x01u8; 32];
        let signing_key = SigningKey::from_bytes((&key_bytes).into()).unwrap();
        let verifying_key = *signing_key.verifying_key();

        // Fixed timestamp: 2023-11-14 22:13:20 UTC (Unix 1700000000, 0 nanos)
        let timestamp = SystemTime::UNIX_EPOCH + std::time::Duration::new(1700000000, 0);

        let mut promise = PaymentPromise {
            chain_id: "mocha-4".to_string(),
            height: 100,
            namespace: vec![0u8; NAMESPACE_SIZE],
            upload_size: 1024,
            blob_version: 0,
            commitment: [0xABu8; 32],
            creation_timestamp: timestamp,
            signer_pubkey: verifying_key,
            signature: None,
        };

        // Verify stripped_sign_bytes layout (fields only, no prefix/chainID)
        let stripped = promise.stripped_sign_bytes().unwrap();
        assert_eq!(stripped.len(), SIGN_BYTES_FIXED_SIZE);

        let mut offset = 0;

        // Public key (33 bytes)
        assert_eq!(
            &stripped[offset..offset + 33],
            verifying_key.to_sec1_bytes().as_ref()
        );
        offset += 33;

        // Namespace (29 bytes, all zeros)
        assert_eq!(&stripped[offset..offset + 29], &[0u8; 29]);
        offset += 29;

        // Upload size: 1024 = 0x00000400 (4 bytes BE)
        assert_eq!(&stripped[offset..offset + 4], &[0x00, 0x00, 0x04, 0x00]);
        offset += 4;

        // Commitment (32 bytes, all 0xAB)
        assert_eq!(&stripped[offset..offset + 32], &[0xABu8; 32]);
        offset += 32;

        // Blob version: 0 (4 bytes BE)
        assert_eq!(&stripped[offset..offset + 4], &[0x00, 0x00, 0x00, 0x00]);
        offset += 4;

        // Height: 100 = 0x0000000000000064 (8 bytes BE)
        assert_eq!(
            &stripped[offset..offset + 8],
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64]
        );
        offset += 8;

        // Timestamp: marshal_binary_time(Unix 1700000000, 0 nanos)
        let expected_ts: [u8; 15] = [
            0x01, // version
            0x00, 0x00, 0x00, 0x0E, 0xDC, 0xE5, 0xE8, 0x00, // seconds since year 1 BE
            0x00, 0x00, 0x00, 0x00, // nanos BE
            0xFF, 0xFF, // UTC offset (-1)
        ];
        assert_eq!(&stripped[offset..offset + 15], &expected_ts);
        offset += 15;
        assert_eq!(offset, stripped.len());

        // sign_bytes() wraps stripped with CometBFT domain separation
        let sign_bytes = promise.sign_bytes().unwrap();
        assert_eq!(
            &sign_bytes[..COMET_RAW_BYTES_PREFIX.len()],
            COMET_RAW_BYTES_PREFIX
        );

        // Print hex values for cross-language comparison
        let sign_bytes_hex = hex::encode(&sign_bytes);
        eprintln!("=== Cross-language compatibility test ===");
        eprintln!("Private key hex: {}", hex::encode(key_bytes));
        eprintln!(
            "Public key hex (33 bytes compressed): {}",
            hex::encode(verifying_key.to_sec1_bytes().as_ref())
        );
        eprintln!(
            "Stripped sign bytes hex ({} bytes): {}",
            stripped.len(),
            hex::encode(&stripped)
        );
        eprintln!(
            "Sign bytes hex ({} bytes): {sign_bytes_hex}",
            sign_bytes.len()
        );

        // Sign and verify
        promise.sign(&signing_key).unwrap();
        let signature_hex = hex::encode(promise.signature.as_ref().unwrap());
        eprintln!("Signature hex (64 bytes): {signature_hex}");
        promise.validate().unwrap();

        use sha2::{Digest as _, Sha256};
        let hash = Sha256::digest(&sign_bytes);
        eprintln!("SHA-256(sign_bytes) hex: {}", hex::encode(hash));
        eprintln!("=== End cross-language compatibility test ===");
    }

    /// Proto round-trip test: verify sign_bytes survive serialization.
    ///
    /// Converts a PaymentPromise to proto, encodes to bytes with prost,
    /// decodes back, reconstructs a PaymentPromise, and checks sign_bytes match.
    #[test]
    fn proto_roundtrip_preserves_sign_bytes() {
        use prost::Message;

        let key_bytes = [0x01u8; 32];
        let signing_key = SigningKey::from_bytes((&key_bytes).into()).unwrap();

        let timestamp = SystemTime::UNIX_EPOCH + std::time::Duration::new(1700000000, 500_000_000);

        let mut promise = PaymentPromise {
            chain_id: "mocha-4".to_string(),
            height: 100,
            namespace: vec![0u8; NAMESPACE_SIZE],
            upload_size: 1024,
            blob_version: 0,
            commitment: [0xABu8; 32],
            creation_timestamp: timestamp,
            signer_pubkey: *signing_key.verifying_key(),
            signature: None,
        };
        promise.sign(&signing_key).unwrap();

        let original_sign_bytes = promise.sign_bytes().unwrap();

        // Convert to proto and encode
        let proto_pp = celestia_proto::celestia::fibre::v1::PaymentPromise::from(&promise);
        let proto_bytes = proto_pp.encode_to_vec();

        // Print the raw proto bytes for cross-language decoding
        eprintln!("=== Proto round-trip test ===");
        eprintln!(
            "Proto bytes hex ({} bytes): {}",
            proto_bytes.len(),
            hex::encode(&proto_bytes)
        );
        eprintln!("=== End proto round-trip test ===");

        // Decode back from proto bytes
        use celestia_proto::celestia::fibre::v1::PaymentPromise as ProtoPP;
        let decoded_proto = ProtoPP::decode(proto_bytes.as_slice()).unwrap();

        // Reconstruct the domain PaymentPromise from decoded proto
        let ts = decoded_proto.creation_timestamp.unwrap();
        let reconstructed_timestamp = if ts.seconds >= 0 {
            SystemTime::UNIX_EPOCH + std::time::Duration::new(ts.seconds as u64, ts.nanos as u32)
        } else {
            panic!("negative timestamp in test");
        };

        let pk_bytes = decoded_proto.signer_public_key.unwrap().key;
        let reconstructed_pubkey = VerifyingKey::from_sec1_bytes(&pk_bytes).unwrap();

        let reconstructed = PaymentPromise {
            chain_id: decoded_proto.chain_id,
            height: decoded_proto.height as u64,
            namespace: decoded_proto.namespace,
            upload_size: decoded_proto.blob_size,
            blob_version: decoded_proto.blob_version,
            commitment: decoded_proto.commitment.try_into().unwrap(),
            creation_timestamp: reconstructed_timestamp,
            signer_pubkey: reconstructed_pubkey,
            signature: Some(decoded_proto.signature),
        };

        let reconstructed_sign_bytes = reconstructed.sign_bytes().unwrap();

        assert_eq!(
            hex::encode(&original_sign_bytes),
            hex::encode(&reconstructed_sign_bytes),
            "sign_bytes must survive proto round-trip"
        );

        // Also verify the signature still validates
        reconstructed.validate().unwrap();
    }

    /// Cross-language test: verifies that `raw_bytes_message_sign_bytes` produces
    /// the exact same output as Go's `core.RawBytesMessageSignBytes`.
    /// Go test: celestia-app-fibre/fibre/validator/cross_test.go
    #[test]
    fn cross_language_raw_bytes_message_sign_bytes() {
        let chain_id = "test-chain-1";
        let unique_id = b"fibre/pp:v0";
        let raw_bytes = b"hello world - test raw bytes";

        let result = raw_bytes_message_sign_bytes(chain_id, unique_id, raw_bytes);

        // Expected from Go's RawBytesMessageSignBytes:
        let expected = "434f4d45543a3a5241575f42595445533a3a5349474e390a0c746573742d636861696e2d31121c68656c6c6f20776f726c64202d2074657374207261772062797465731a0b66696272652f70703a7630";

        assert_eq!(
            hex::encode(&result),
            expected,
            "must match Go's RawBytesMessageSignBytes output"
        );
    }
}
