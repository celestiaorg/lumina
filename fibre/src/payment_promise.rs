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

/// Size of the Go time.Time MarshalBinary output for UTC times (15 bytes).
/// Format: 1 byte version + 8 bytes seconds + 4 bytes nanoseconds + 2 bytes timezone offset.
const TIMESTAMP_BINARY_SIZE: usize = 15;

/// Fixed-size portion of sign bytes (excluding prefix and variable-length chain_id).
/// signerPubKey(33) + namespace(29) + blobSize(4) + commitment(32) + blobVersion(4) + height(8) + timestamp(15)
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
    /// Compute the canonical sign bytes for this payment promise.
    ///
    /// Format:
    /// ```text
    /// prefix || chain_id || signer_bytes(33) || namespace(29)
    /// || blob_size(4 BE) || commitment(32) || blob_version(4 BE)
    /// || height(8 BE) || timestamp(15 binary)
    /// ```
    ///
    /// The timestamp uses Go's `time.Time.MarshalBinary()` format:
    /// - 1 byte: version (1)
    /// - 8 bytes: seconds as big-endian int64
    /// - 4 bytes: nanoseconds as big-endian int32
    /// - 2 bytes: timezone offset as big-endian int16 (-1 for UTC)
    pub fn sign_bytes(&self) -> Result<Vec<u8>, FibreError> {
        let timestamp_bytes = marshal_binary_time(self.creation_timestamp)?;

        let total_size = SIGN_BYTES_PREFIX.len() + self.chain_id.len() + SIGN_BYTES_FIXED_SIZE;
        let mut buf = Vec::with_capacity(total_size);

        // Prepend domain separation prefix
        buf.extend_from_slice(SIGN_BYTES_PREFIX);

        // Append chainID
        buf.extend_from_slice(self.chain_id.as_bytes());

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

    /// Sign this promise with the given signing key.
    ///
    /// Computes the sign bytes and signs them using secp256k1 ECDSA.
    /// Sets `self.signature` to the 64-byte compact signature (r || s).
    ///
    /// The Go implementation uses `privKey.Sign(signBytes)` which internally
    /// hashes the message with SHA-256 before ECDSA signing.
    pub fn sign(&mut self, signing_key: &SigningKey) -> Result<(), FibreError> {
        let sign_bytes = self.sign_bytes()?;

        // Hash the sign bytes with SHA-256 before signing.
        // This matches the Go cosmos-sdk secp256k1.Sign() behavior which
        // calls crypto/sha256 on the message before passing to btcec.
        let hash = Sha256::digest(&sign_bytes);

        let signature: Signature = signing_key.sign(&hash);
        self.signature = Some(signature.to_bytes().to_vec());

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
        let hash = Sha256::digest(&sign_bytes);

        let signature = Signature::from_slice(signature_bytes).map_err(|e| {
            FibreError::InvalidPaymentPromise(format!("invalid signature format: {}", e))
        })?;

        use k256::ecdsa::signature::Verifier;
        self.signer_pubkey.verify(&hash, &signature).map_err(|_| {
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

    /// Compute sign bytes for validator signing (CometBFT domain separation).
    ///
    /// This wraps the `sign_bytes()` with CometBFT's `RawBytesMessageSignBytes`
    /// domain separation format. In Go this calls:
    /// `core.RawBytesMessageSignBytes(p.ChainID, signBytesPrefix, signBytes)`
    ///
    /// TODO: Implement once CometBFT's exact encoding format is available
    /// in the Rust ecosystem.
    pub fn sign_bytes_validator(&self) -> Result<Vec<u8>, FibreError> {
        // TODO: Implement CometBFT domain separation wrapping.
        // This requires the exact protobuf encoding used by CometBFT's
        // RawBytesMessageSignBytes function.
        Err(FibreError::Other(
            "sign_bytes_validator not yet implemented: requires CometBFT domain separation format"
                .into(),
        ))
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

/// Marshal a `SystemTime` into Go's `time.Time.MarshalBinary()` format.
///
/// Go's binary time format (version 1, UTC):
/// - Byte 0: version = 1
/// - Bytes 1-8: seconds since Unix epoch as big-endian int64
/// - Bytes 9-12: nanoseconds as big-endian int32
/// - Bytes 13-14: timezone offset as big-endian int16 (-1 for UTC sentinel)
///
/// In Go, when the time is in UTC (as enforced by calling `.UTC().MarshalBinary()`),
/// the timezone offset is set to -1 as a special sentinel value indicating UTC.
fn marshal_binary_time(t: SystemTime) -> Result<Vec<u8>, FibreError> {
    let duration = t
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(|_| FibreError::InvalidPaymentPromise("timestamp before Unix epoch".into()))?;

    let secs = duration.as_secs() as i64;
    let nsecs = duration.subsec_nanos() as i32;

    let mut buf = Vec::with_capacity(TIMESTAMP_BINARY_SIZE);

    // Version byte
    buf.push(1);

    // Seconds as big-endian int64
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
///
/// This is the inverse of `marshal_binary_time`.
#[allow(dead_code)]
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

    let secs = i64::from_be_bytes(data[1..9].try_into().unwrap());
    let nsecs = i32::from_be_bytes(data[9..13].try_into().unwrap());

    if secs < 0 {
        return Err(FibreError::Other(
            "negative timestamp seconds not supported".into(),
        ));
    }
    if nsecs < 0 {
        return Err(FibreError::Other(
            "negative timestamp nanoseconds not supported".into(),
        ));
    }

    let duration = std::time::Duration::new(secs as u64, nsecs as u32);
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
    fn sign_bytes_format() {
        let (_, promise) = make_test_promise();
        let sign_bytes = promise.sign_bytes().unwrap();

        // Expected total size
        let expected_size =
            SIGN_BYTES_PREFIX.len() + promise.chain_id.len() + SIGN_BYTES_FIXED_SIZE;
        assert_eq!(sign_bytes.len(), expected_size);

        // Check prefix
        assert_eq!(&sign_bytes[..SIGN_BYTES_PREFIX.len()], SIGN_BYTES_PREFIX);
    }

    #[test]
    fn sign_bytes_field_layout() {
        let (_, promise) = make_test_promise();
        let sign_bytes = promise.sign_bytes().unwrap();

        let mut offset = 0;

        // Prefix
        assert_eq!(
            &sign_bytes[offset..offset + SIGN_BYTES_PREFIX.len()],
            SIGN_BYTES_PREFIX
        );
        offset += SIGN_BYTES_PREFIX.len();

        // Chain ID
        assert_eq!(
            &sign_bytes[offset..offset + promise.chain_id.len()],
            promise.chain_id.as_bytes()
        );
        offset += promise.chain_id.len();

        // Signer pubkey (33 bytes)
        let expected_pubkey = promise.signer_pubkey.to_sec1_bytes();
        assert_eq!(
            &sign_bytes[offset..offset + PUBKEY_SIZE],
            expected_pubkey.as_ref()
        );
        offset += PUBKEY_SIZE;

        // Namespace (29 bytes)
        assert_eq!(
            &sign_bytes[offset..offset + NAMESPACE_SIZE],
            &promise.namespace
        );
        offset += NAMESPACE_SIZE;

        // Upload size (4 bytes BE)
        assert_eq!(
            &sign_bytes[offset..offset + 4],
            &promise.upload_size.to_be_bytes()
        );
        offset += 4;

        // Commitment (32 bytes)
        assert_eq!(&sign_bytes[offset..offset + 32], &promise.commitment);
        offset += 32;

        // Blob version (4 bytes BE)
        assert_eq!(
            &sign_bytes[offset..offset + 4],
            &promise.blob_version.to_be_bytes()
        );
        offset += 4;

        // Height (8 bytes BE)
        assert_eq!(
            &sign_bytes[offset..offset + 8],
            &promise.height.to_be_bytes()
        );
        offset += 8;

        // Timestamp (15 bytes)
        assert_eq!(sign_bytes.len() - offset, TIMESTAMP_BINARY_SIZE);
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

        // Check seconds
        let secs = i64::from_be_bytes(bytes[1..9].try_into().unwrap());
        assert_eq!(secs, 1700000000);

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
}
