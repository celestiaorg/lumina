//! Signature collection and threshold detection for blob uploads.
//!
//! [`SignatureSet`] collects ed25519 validator signatures during blob upload,
//! tracks accumulated voting power, and detects when the required threshold
//! is met. It is safe for concurrent use from multiple tasks.

use std::collections::HashMap;
use std::sync::Mutex;

use ed25519_dalek::{Signature as Ed25519Signature, Verifier};

use crate::config::Fraction;
use crate::error::FibreError;
use crate::validator::{ValidatorInfo, ValidatorSet};

/// Collects and validates ed25519 signatures from validators.
///
/// Thread-safe. Signatures are returned in validator-set order by
/// [`SignatureSet::signatures`], with `None` entries for validators that did not sign.
pub struct SignatureSet {
    /// The bytes that each validator must have signed.
    required_bytes_signed: Vec<u8>,
    /// Minimum accumulated voting power to consider the threshold met.
    min_required_voting_power: i64,
    /// Ordered validator list (determines output ordering).
    validators: Vec<ValidatorInfo>,
    /// Mutable state protected by a mutex.
    inner: Mutex<SignatureSetInner>,
}

/// Mutable interior of [`SignatureSet`], protected by a [`Mutex`].
struct SignatureSetInner {
    /// Accumulated voting power from accepted signatures.
    voting_power: i64,
    /// Map from validator address to their signature bytes.
    signatures: HashMap<[u8; 20], Vec<u8>>,
}

impl SignatureSet {
    /// Creates a new `SignatureSet`.
    ///
    /// # Arguments
    ///
    /// * `validators` - The full ordered validator list.
    /// * `target_voting_power` - Fraction of total voting power required (e.g. 2/3).
    /// * `required_bytes_signed` - The exact bytes each signature must cover.
    pub fn new(
        validators: Vec<ValidatorInfo>,
        target_voting_power: Fraction,
        required_bytes_signed: Vec<u8>,
    ) -> Self {
        let total_voting_power: i64 = validators.iter().map(|v| v.voting_power).sum();
        let min_required_voting_power = total_voting_power * target_voting_power.numerator as i64
            / target_voting_power.denominator as i64;

        let capacity = validators.len();
        Self {
            required_bytes_signed,
            min_required_voting_power,
            validators,
            inner: Mutex::new(SignatureSetInner {
                voting_power: 0,
                signatures: HashMap::with_capacity(capacity),
            }),
        }
    }

    /// Validates and records a signature from the given validator.
    ///
    /// Returns `Ok(true)` if the voting power threshold has been met,
    /// `Ok(false)` if more signatures are still needed.
    pub fn add(&self, validator: &ValidatorInfo, signature: &[u8]) -> Result<bool, FibreError> {
        // Parse the raw signature bytes into an ed25519 Signature.
        let ed_sig = Ed25519Signature::from_slice(signature).map_err(|e| {
            FibreError::InvalidValidatorSignature {
                validator: validator.address_hex(),
                reason: e.to_string(),
            }
        })?;

        validator
            .pubkey
            .verify(&self.required_bytes_signed, &ed_sig)
            .map_err(|e| FibreError::InvalidValidatorSignature {
                validator: validator.address_hex(),
                reason: e.to_string(),
            })?;

        // Lock, record, and check threshold.
        let mut inner = self.inner.lock().expect("SignatureSet mutex poisoned");
        // Only count voting power once per validator (idempotent on duplicates).
        if !inner.signatures.contains_key(&validator.address) {
            inner.voting_power += validator.voting_power;
        }
        inner
            .signatures
            .insert(validator.address, signature.to_vec());

        Ok(inner.voting_power >= self.min_required_voting_power)
    }

    /// Returns the collected signatures ordered by validator-set position.
    ///
    /// Each entry is `Some(signature)` if the validator signed, or `None` if
    /// not. Returns `Err(FibreError::NotEnoughSignatures)` if the accumulated
    /// voting power is below the required threshold.
    pub fn signatures(&self) -> Result<Vec<Option<Vec<u8>>>, FibreError> {
        let inner = self.inner.lock().expect("SignatureSet mutex poisoned");

        let ordered: Vec<Option<Vec<u8>>> = self
            .validators
            .iter()
            .map(|v| inner.signatures.get(&v.address).cloned())
            .collect();

        if inner.voting_power < self.min_required_voting_power {
            return Err(FibreError::NotEnoughSignatures {
                collected: inner.voting_power,
                required: self.min_required_voting_power,
            });
        }

        Ok(ordered)
    }
}

impl ValidatorSet {
    /// Creates a new [`SignatureSet`] from this validator set.
    ///
    /// This is a convenience method equivalent to calling
    /// `SignatureSet::new(self.validators.clone(), target, bytes)`.
    pub fn new_signature_set(
        &self,
        target_voting_power: Fraction,
        required_bytes_signed: Vec<u8>,
    ) -> SignatureSet {
        SignatureSet::new(
            self.validators.clone(),
            target_voting_power,
            required_bytes_signed,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;
    use rand::RngCore;

    /// Helper: generate a ValidatorInfo with a fresh ed25519 keypair.
    fn make_validator(voting_power: i64) -> (SigningKey, ValidatorInfo) {
        let mut secret = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut secret);
        let signing_key = SigningKey::from_bytes(&secret);
        let pubkey = signing_key.verifying_key();
        // Derive a deterministic 20-byte address from the public key bytes.
        let pk_bytes = pubkey.to_bytes();
        let mut address = [0u8; 20];
        address.copy_from_slice(&pk_bytes[..20]);
        (
            signing_key,
            ValidatorInfo {
                address,
                pubkey,
                voting_power,
            },
        )
    }

    /// Helper: sign data with a signing key.
    fn sign(key: &SigningKey, data: &[u8]) -> Vec<u8> {
        use ed25519_dalek::Signer;
        let sig: Ed25519Signature = key.sign(data);
        sig.to_bytes().to_vec()
    }

    #[test]
    fn add_valid_signature() {
        let (sk, val) = make_validator(10);
        let data = b"hello world";
        let sig = sign(&sk, data);

        let ss = SignatureSet::new(
            vec![val.clone()],
            Fraction {
                numerator: 1,
                denominator: 2,
            },
            data.to_vec(),
        );

        let result = ss.add(&val, &sig);
        assert!(result.is_ok(), "valid signature should be accepted");
    }

    #[test]
    fn add_invalid_signature() {
        let (_sk, val) = make_validator(10);
        let data = b"hello world";
        // Sign with a different key to produce an invalid signature.
        let (wrong_sk, _) = make_validator(10);
        let bad_sig = sign(&wrong_sk, data);

        let ss = SignatureSet::new(
            vec![val.clone()],
            Fraction {
                numerator: 1,
                denominator: 2,
            },
            data.to_vec(),
        );

        let result = ss.add(&val, &bad_sig);
        assert!(result.is_err(), "invalid signature should be rejected");
        match result.unwrap_err() {
            FibreError::InvalidValidatorSignature { validator, .. } => {
                assert_eq!(validator, val.address_hex());
            }
            other => panic!("expected InvalidValidatorSignature, got: {other}"),
        }
    }

    #[test]
    fn threshold_detection() {
        // Three validators with powers 10, 20, 30. Total = 60.
        // Threshold = 2/3 => min_required = 60 * 2 / 3 = 40.
        let (sk1, v1) = make_validator(10);
        let (sk2, v2) = make_validator(20);
        let (sk3, v3) = make_validator(30);

        let data = b"threshold test";

        let ss = SignatureSet::new(
            vec![v1.clone(), v2.clone(), v3.clone()],
            Fraction {
                numerator: 2,
                denominator: 3,
            },
            data.to_vec(),
        );

        // v1 (10): total = 10 < 40 => false
        let met = ss.add(&v1, &sign(&sk1, data)).unwrap();
        assert!(!met, "10 < 40, threshold not met");

        // v2 (20): total = 30 < 40 => false
        let met = ss.add(&v2, &sign(&sk2, data)).unwrap();
        assert!(!met, "30 < 40, threshold not met");

        // v3 (30): total = 60 >= 40 => true
        let met = ss.add(&v3, &sign(&sk3, data)).unwrap();
        assert!(met, "60 >= 40, threshold should be met");
    }

    #[test]
    fn signatures_returns_ordered() {
        let (sk1, v1) = make_validator(10);
        let (sk2, v2) = make_validator(20);
        let (_sk3, v3) = make_validator(30);

        let data = b"order test";

        let ss = SignatureSet::new(
            vec![v1.clone(), v2.clone(), v3.clone()],
            Fraction {
                numerator: 1,
                denominator: 3,
            },
            data.to_vec(),
        );

        let sig1 = sign(&sk1, data);
        let sig2 = sign(&sk2, data);

        // Add v2 first, then v1 (out of validator-set order).
        ss.add(&v2, &sig2).unwrap();
        ss.add(&v1, &sig1).unwrap();

        let ordered = ss.signatures().unwrap();
        assert_eq!(ordered.len(), 3);
        // v1 is index 0
        assert_eq!(ordered[0].as_deref(), Some(sig1.as_slice()));
        // v2 is index 1
        assert_eq!(ordered[1].as_deref(), Some(sig2.as_slice()));
        // v3 is index 2 — did not sign
        assert!(ordered[2].is_none());
    }

    #[test]
    fn signatures_fails_below_threshold() {
        let (sk1, v1) = make_validator(10);
        let (_sk2, v2) = make_validator(20);

        let data = b"fail test";

        let ss = SignatureSet::new(
            vec![v1.clone(), v2.clone()],
            Fraction {
                numerator: 2,
                denominator: 3,
            },
            data.to_vec(),
        );

        // Only v1 signs. Total power = 10, required = 30 * 2 / 3 = 20.
        ss.add(&v1, &sign(&sk1, data)).unwrap();

        let result = ss.signatures();
        assert!(result.is_err());
        match result.unwrap_err() {
            FibreError::NotEnoughSignatures {
                collected,
                required,
            } => {
                assert_eq!(collected, 10);
                assert_eq!(required, 20);
            }
            other => panic!("expected NotEnoughSignatures, got: {other}"),
        }
    }

    #[test]
    fn duplicate_add_is_idempotent() {
        // Adding the same validator twice does NOT double-count voting power.
        // The signature map entry is overwritten but voting_power stays the same.
        let (sk, v) = make_validator(25);
        let data = b"dup test";

        let ss = SignatureSet::new(
            vec![v.clone()],
            Fraction {
                numerator: 2,
                denominator: 3,
            },
            data.to_vec(),
        );

        // min_required = 25 * 2 / 3 = 16 (integer division).
        // First add: power = 25 >= 16 => true
        let met = ss.add(&v, &sign(&sk, data)).unwrap();
        assert!(met, "first add: 25 >= 16, threshold met");

        // Second add: power still 25 (not accumulated) >= 16 => true
        let met = ss.add(&v, &sign(&sk, data)).unwrap();
        assert!(met, "second add: 25 >= 16, threshold still met");

        // Verify the signature map has exactly one entry (overwritten).
        let sigs = ss.signatures().unwrap();
        assert_eq!(sigs.len(), 1);
        assert!(sigs[0].is_some());
    }
}
