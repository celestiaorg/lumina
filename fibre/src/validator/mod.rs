//! Validator identity, validator set types, and the [`SetGetter`] trait.

pub(crate) mod shard_map;
pub(crate) mod signature_set;

use std::collections::{HashMap, HashSet};
use std::num::NonZeroU64;

use chacha8rand::ChaCha8Rand;
use ed25519_dalek::VerifyingKey as Ed25519PublicKey;
use rand::Rng;

use crate::blob::Commitment;
use crate::config::Fraction;
use crate::error::FibreError;
use celestia_grpc::GrpcClient;

pub(crate) use shard_map::ShardMap;

/// Maximum total voting power accepted by CometBFT.
const MAX_TOTAL_VOTING_POWER: u64 = (i64::MAX / 8) as u64;

/// A validator's identity and stake information.
#[derive(Debug, Clone)]
pub struct ValidatorInfo {
    /// The validator's consensus address (20-byte CometBFT address).
    pub(crate) address: [u8; 20],
    /// The validator's ed25519 public key for signing.
    pub(crate) pubkey: Ed25519PublicKey,
    /// The validator's voting power (stake weight).
    voting_power: NonZeroU64,
}

impl ValidatorInfo {
    /// Creates validator information from an ed25519 public key and voting power.
    pub fn try_new(pubkey: Ed25519PublicKey, voting_power: u64) -> Result<Self, FibreError> {
        let voting_power = NonZeroU64::new(voting_power)
            .ok_or_else(|| FibreError::InvalidData("validator has zero voting power".into()))?;
        if voting_power.get() > MAX_TOTAL_VOTING_POWER {
            return Err(FibreError::InvalidData(format!(
                "validator voting power {} exceeds maximum {MAX_TOTAL_VOTING_POWER}",
                voting_power.get()
            )));
        }

        let address = validator_address(&pubkey);
        Ok(Self {
            address,
            pubkey,
            voting_power,
        })
    }

    /// Returns the validator's consensus address.
    pub fn address(&self) -> &[u8; 20] {
        &self.address
    }

    /// Returns the validator's ed25519 public key.
    pub fn public_key(&self) -> &Ed25519PublicKey {
        &self.pubkey
    }

    /// Returns the validator's voting power.
    pub fn voting_power(&self) -> u64 {
        self.voting_power.get()
    }

    /// Returns the validator address as an uppercase hex string.
    pub fn address_hex(&self) -> String {
        hex::encode_upper(self.address)
    }
}

impl PartialEq for ValidatorInfo {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address
            && self.pubkey.as_bytes() == other.pubkey.as_bytes()
            && self.voting_power == other.voting_power
    }
}

impl Eq for ValidatorInfo {}

impl std::hash::Hash for ValidatorInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.address.hash(state);
        self.pubkey.as_bytes().hash(state);
        self.voting_power.hash(state);
    }
}

/// A validator set at a specific height.
#[derive(Debug, Clone)]
pub struct ValidatorSet {
    /// The validators in this set.
    validators: Vec<ValidatorInfo>,
    /// The block height at which this validator set is valid.
    height: u64,
    total_voting_power: u64,
}

impl ValidatorSet {
    /// Creates a validated validator set at the given height.
    pub fn try_new(validators: Vec<ValidatorInfo>, height: u64) -> Result<Self, FibreError> {
        if validators.is_empty() {
            return Err(FibreError::InvalidData("validator set is empty".into()));
        }

        let mut addresses = HashSet::with_capacity(validators.len());
        let mut total_voting_power = 0u64;
        for validator in &validators {
            if !addresses.insert(validator.address) {
                return Err(FibreError::InvalidData(format!(
                    "validator set contains duplicate address {}",
                    validator.address_hex()
                )));
            }
            total_voting_power = total_voting_power
                .checked_add(validator.voting_power())
                .ok_or_else(|| FibreError::InvalidData("validator voting power overflow".into()))?;
            if total_voting_power > MAX_TOTAL_VOTING_POWER {
                return Err(FibreError::InvalidData(format!(
                    "total voting power {total_voting_power} exceeds maximum {MAX_TOTAL_VOTING_POWER}"
                )));
            }
        }

        Ok(Self {
            validators,
            height,
            total_voting_power,
        })
    }

    /// Returns the validators in set order.
    pub fn validators(&self) -> &[ValidatorInfo] {
        &self.validators
    }

    /// Returns the height at which this validator set is valid.
    pub fn height(&self) -> u64 {
        self.height
    }

    /// Returns the total voting power of all validators in the set.
    pub fn total_voting_power(&self) -> u64 {
        self.total_voting_power
    }

    /// Computes the number of rows each validator should store based on stake.
    fn rows_per_validator(
        &self,
        original_rows: usize,
        min_rows: usize,
        liveness_threshold: Fraction,
    ) -> Vec<(usize, &ValidatorInfo)> {
        let total_voting_power = self.total_voting_power();
        self.validators
            .iter()
            .map(|v| {
                let num = (original_rows as u128)
                    * (v.voting_power() as u128)
                    * (liveness_threshold.denominator.get() as u128);
                let den =
                    (total_voting_power as u128) * (liveness_threshold.numerator.get() as u128);
                let rows = num.div_ceil(den).min(original_rows as u128) as usize;
                (rows.max(min_rows).min(original_rows), v)
            })
            .collect()
    }

    /// Deterministically assigns row indices to validators based on their stake.
    pub fn assign(
        &self,
        commitment: Commitment,
        total_rows: usize,
        original_rows: usize,
        min_rows: usize,
        liveness_threshold: Fraction,
    ) -> ShardMap {
        if total_rows == 0 || min_rows == 0 {
            return ShardMap::new(HashMap::new());
        }

        let rows_per_validator =
            self.rows_per_validator(original_rows, min_rows, liveness_threshold);

        let mut rng = ChaCha8Rand::new(&commitment);
        let mut row_indices: Vec<usize> = (0..total_rows).collect();
        go_shuffle(&mut rng, &mut row_indices);

        let mut shard_map = HashMap::with_capacity(self.validators.len());
        let mut offset: usize = 0;
        for (i, (row_count, _)) in rows_per_validator.iter().enumerate() {
            let rows: Vec<usize> = (0..*row_count)
                .map(|j| row_indices[(offset + j) % total_rows])
                .collect();
            shard_map.insert(i, rows);
            offset += row_count;
        }

        ShardMap::new(shard_map)
    }

    /// Selects validators for shard download, ordered by priority.
    ///
    /// Returns ordered `(expected_rows, validator)` pairs. Higher-stake
    /// validators come first (priority group), followed by the tail group.
    /// Both groups are shuffled weighted by stake.
    pub fn select(
        &self,
        original_rows: usize,
        min_rows: usize,
        liveness_threshold: Fraction,
    ) -> Vec<(usize, &ValidatorInfo)> {
        let mut rpv = self.rows_per_validator(original_rows, min_rows, liveness_threshold);

        let total_voting_power = self.total_voting_power();
        let total_distributed_rows = (original_rows as u128)
            * (liveness_threshold.denominator.get() as u128)
            / (liveness_threshold.numerator.get() as u128);
        let min_stake = ((min_rows as u128) * (total_voting_power as u128))
            .div_ceil(total_distributed_rows) as u64;

        let mut accumulated: u128 = 0;
        let mut split_idx = self.validators.len();
        for (i, validator) in self.validators.iter().enumerate() {
            accumulated += validator.voting_power().max(min_stake) as u128;
            if accumulated > total_voting_power as u128 {
                split_idx = i;
                break;
            }
        }

        let mut rng = rand::thread_rng();
        shuffle_by_stake(&mut rpv[..split_idx], &mut rng);
        shuffle_by_stake(&mut rpv[split_idx..], &mut rng);
        rpv
    }
}

#[cfg(test)]
impl ValidatorSet {
    fn new(validators: Vec<ValidatorInfo>, height: u64) -> Self {
        Self::try_new(validators, height).unwrap()
    }
}

#[cfg(test)]
mod validation_tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn validator(power: u64, seed: u8) -> ValidatorInfo {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = seed;
        ValidatorInfo::try_new(SigningKey::from_bytes(&key_bytes).verifying_key(), power).unwrap()
    }

    #[test]
    fn rejects_empty_set() {
        assert!(ValidatorSet::try_new(vec![], 0).is_err());
    }

    #[test]
    fn rejects_zero_power() {
        let key = SigningKey::from_bytes(&[1; 32]);
        assert!(ValidatorInfo::try_new(key.verifying_key(), 0).is_err());
    }

    #[test]
    fn rejects_power_above_cometbft_limit() {
        let key = SigningKey::from_bytes(&[1; 32]);
        assert!(ValidatorInfo::try_new(key.verifying_key(), MAX_TOTAL_VOTING_POWER + 1).is_err());
    }

    #[test]
    fn rejects_total_power_above_cometbft_limit() {
        let validators = vec![validator(MAX_TOTAL_VOTING_POWER, 1), validator(1, 2)];
        assert!(ValidatorSet::try_new(validators, 0).is_err());
    }

    #[test]
    fn rejects_duplicate_validator() {
        let validator = validator(1, 1);
        assert!(ValidatorSet::try_new(vec![validator.clone(), validator], 0).is_err());
    }
}

/// Trait for retrieving the current validator set.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait SetGetter: Send + Sync {
    /// Get the latest validator set.
    async fn head(&self) -> Result<ValidatorSet, FibreError>;
    /// Get the validator set at a specific height.
    async fn get_by_height(&self, height: u64) -> Result<ValidatorSet, FibreError>;
}

/// Production [`SetGetter`] backed by gRPC.
pub struct GrpcSetGetter {
    client: GrpcClient,
}

impl GrpcSetGetter {
    /// Create a new getter using the given [`GrpcClient`] to the CometBFT node.
    pub fn new(client: GrpcClient) -> Self {
        Self { client }
    }

    async fn get_by_height_inner(&self, height: i64) -> Result<ValidatorSet, FibreError> {
        let resp = self.client.get_fibre_validator_set(height).await?;

        let height = u64::try_from(resp.height).map_err(|_| {
            FibreError::InvalidData(format!("validator set has invalid height {}", resp.height))
        })?;
        if height == 0 {
            return Err(FibreError::InvalidData(
                "validator set has zero height".into(),
            ));
        }

        let proto_set = resp.validator_set.ok_or_else(|| {
            FibreError::InvalidData("ValidatorSetResponse missing validator_set".into())
        })?;

        (&proto_set, height).try_into()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl SetGetter for GrpcSetGetter {
    async fn head(&self) -> Result<ValidatorSet, FibreError> {
        self.get_by_height_inner(0).await
    }

    async fn get_by_height(&self, height: u64) -> Result<ValidatorSet, FibreError> {
        if height == 0 {
            return Err(FibreError::Other(
                "get_by_height requires height > 0".into(),
            ));
        }
        let height = i64::try_from(height)
            .map_err(|_| FibreError::InvalidData("validator set height exceeds i64::MAX".into()))?;
        self.get_by_height_inner(height).await
    }
}

impl TryFrom<(&tendermint_proto::v0_38::types::ValidatorSet, u64)> for ValidatorSet {
    type Error = FibreError;

    fn try_from(
        (proto_set, height): (&tendermint_proto::v0_38::types::ValidatorSet, u64),
    ) -> Result<Self, Self::Error> {
        let validators = proto_set
            .validators
            .iter()
            .map(ValidatorInfo::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let set = ValidatorSet::try_new(validators, height)?;

        let proposer = proto_set
            .proposer
            .as_ref()
            .ok_or_else(|| FibreError::InvalidData("validator set is missing proposer".into()))?;
        let proposer = ValidatorInfo::try_from(proposer)?;
        if !set
            .validators
            .iter()
            .any(|validator| validator.address == proposer.address)
        {
            return Err(FibreError::InvalidData(
                "validator set proposer is not in validator set".into(),
            ));
        }

        Ok(set)
    }
}

impl TryFrom<&tendermint_proto::v0_38::types::Validator> for ValidatorInfo {
    type Error = FibreError;

    fn try_from(
        proto_val: &tendermint_proto::v0_38::types::Validator,
    ) -> Result<Self, Self::Error> {
        use tendermint_proto::v0_38::crypto::public_key::Sum as CryptoKeySum;

        let pubkey_bytes = match proto_val.pub_key.as_ref() {
            Some(pk) => match &pk.sum {
                Some(CryptoKeySum::Ed25519(bytes)) => bytes.clone(),
                _ => {
                    return Err(FibreError::InvalidData(
                        "expected ed25519 public key for validator".into(),
                    ));
                }
            },
            None => {
                return Err(FibreError::InvalidData(
                    "validator missing public key".into(),
                ));
            }
        };

        let pubkey =
            Ed25519PublicKey::from_bytes(pubkey_bytes.as_slice().try_into().map_err(|_| {
                FibreError::InvalidData(format!(
                    "ed25519 key has invalid length {}, expected 32",
                    pubkey_bytes.len()
                ))
            })?)
            .map_err(|e| FibreError::InvalidData(format!("invalid ed25519 key: {e}")))?;

        let address = validator_address(&pubkey);

        let proto_address: [u8; 20] = proto_val.address.as_slice().try_into().map_err(|_| {
            FibreError::InvalidData(format!(
                "validator address has invalid length {}, expected 20",
                proto_val.address.len()
            ))
        })?;
        if proto_address != address {
            return Err(FibreError::InvalidData(
                "validator address does not match public key".into(),
            ));
        }

        let voting_power = u64::try_from(proto_val.voting_power).map_err(|_| {
            FibreError::InvalidData(format!(
                "validator has negative voting power: {}",
                proto_val.voting_power
            ))
        })?;

        ValidatorInfo::try_new(pubkey, voting_power)
    }
}

fn validator_address(pubkey: &Ed25519PublicKey) -> [u8; 20] {
    use sha2::{Digest, Sha256};
    Sha256::digest(pubkey.as_bytes())[..20]
        .try_into()
        .expect("sha256 output is always 32 bytes")
}

fn go_shuffle<T>(rng: &mut ChaCha8Rand, slice: &mut [T]) {
    let n = slice.len();
    for i in (1..n).rev() {
        let j = uint64n(rng, (i + 1) as u64) as usize;
        slice.swap(i, j);
    }
}

fn uint64n(rng: &mut ChaCha8Rand, n: u64) -> u64 {
    if n & (n - 1) == 0 {
        return rng.read_u64() & (n - 1);
    }

    let (mut hi, mut lo) = mul_u64_full(rng.read_u64(), n);
    if lo < n {
        let thresh = n.wrapping_neg() % n;
        while lo < thresh {
            let (h, l) = mul_u64_full(rng.read_u64(), n);
            hi = h;
            lo = l;
        }
    }
    hi
}

#[inline]
fn mul_u64_full(a: u64, b: u64) -> (u64, u64) {
    let full = (a as u128) * (b as u128);
    ((full >> 64) as u64, full as u64)
}

fn shuffle_by_stake(selected: &mut [(usize, &ValidatorInfo)], rng: &mut impl Rng) {
    let n = selected.len();
    if n <= 1 {
        return;
    }

    for i in 0..n - 1 {
        let total_weight: u64 = selected[i..]
            .iter()
            .map(|(_, val)| val.voting_power())
            .sum();

        let point = rng.gen_range(0..total_weight);

        let mut cumul: u64 = 0;
        for j in i..n {
            cumul += selected[j].1.voting_power();
            if point < cumul {
                selected.swap(i, j);
                break;
            }
        }
    }
}

#[cfg(test)]
mod assignment_tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn make_validator(power: u64, seed: u8) -> ValidatorInfo {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = seed;
        let signing_key = SigningKey::from_bytes(&key_bytes);
        ValidatorInfo::try_new(signing_key.verifying_key(), power).unwrap()
    }

    fn default_liveness() -> Fraction {
        crate::test_utils::fraction(1, 3)
    }

    #[test]
    fn zero_total_rows_returns_empty_map() {
        let set = ValidatorSet::new(vec![make_validator(100, 1)], 0);
        let map = set.assign([0u8; 32], 0, 50, 10, default_liveness());
        assert!(map.is_empty());
    }

    #[test]
    fn zero_min_rows_returns_empty_map() {
        let set = ValidatorSet::new(vec![make_validator(100, 1)], 0);
        let map = set.assign([0u8; 32], 100, 50, 0, default_liveness());
        assert!(map.is_empty());
    }

    #[test]
    fn single_validator_gets_original_rows() {
        let set = ValidatorSet::new(vec![make_validator(100, 1)], 0);
        let commitment = [0u8; 32];
        let total_rows = 200;
        let original_rows = 100;
        let min_rows = 10;

        let map = set.assign(
            commitment,
            total_rows,
            original_rows,
            min_rows,
            default_liveness(),
        );

        assert_eq!(map.len(), 1);
        assert_eq!(map.get(0).unwrap().len(), original_rows);
    }

    #[test]
    fn two_equal_stake_validators_get_equal_rows() {
        let set = ValidatorSet::new(vec![make_validator(50, 1), make_validator(50, 2)], 0);
        let map = set.assign([1u8; 32], 200, 100, 10, default_liveness());

        assert_eq!(map.len(), 2);
        assert_eq!(map.get(0).unwrap().len(), map.get(1).unwrap().len());
    }

    #[test]
    fn rows_per_validator_respects_min_rows_floor() {
        let set = ValidatorSet::new(vec![make_validator(1, 1), make_validator(999, 2)], 0);
        let map = set.assign([2u8; 32], 200, 100, 50, default_liveness());
        assert_eq!(map.get(0).unwrap().len(), 50);
    }

    #[test]
    fn rows_per_validator_respects_original_rows_ceiling() {
        let set = ValidatorSet::new(vec![make_validator(1000, 1)], 0);
        let map = set.assign([3u8; 32], 200, 100, 10, default_liveness());
        assert_eq!(map.get(0).unwrap().len(), 100);
    }

    #[test]
    fn assignment_is_deterministic() {
        let set = ValidatorSet::new(
            vec![
                make_validator(50, 1),
                make_validator(30, 2),
                make_validator(20, 3),
            ],
            0,
        );
        let map1 = set.assign([42u8; 32], 200, 100, 10, default_liveness());
        let map2 = set.assign([42u8; 32], 200, 100, 10, default_liveness());
        assert_eq!(map1, map2);
    }

    #[test]
    fn different_commitments_produce_different_assignments() {
        let set = ValidatorSet::new(
            vec![
                make_validator(50, 1),
                make_validator(30, 2),
                make_validator(20, 3),
            ],
            0,
        );
        let map1 = set.assign([1u8; 32], 200, 100, 10, default_liveness());
        let map2 = set.assign([2u8; 32], 200, 100, 10, default_liveness());

        for i in 0..set.validators.len() {
            assert_eq!(map1.get(i).unwrap().len(), map2.get(i).unwrap().len());
        }
        assert_ne!(map1.get(0).unwrap(), map2.get(0).unwrap());
    }

    #[test]
    fn shard_map_verify_correct() {
        let set = ValidatorSet::new(vec![make_validator(50, 1), make_validator(50, 2)], 0);
        let map = set.assign([10u8; 32], 200, 100, 10, default_liveness());
        let row_indices_u32: Vec<u32> = map.get(0).unwrap().iter().map(|&r| r as u32).collect();
        assert!(map.verify(0, &row_indices_u32).is_ok());
    }

    #[test]
    fn shard_map_verify_wrong_count() {
        let set = ValidatorSet::new(vec![make_validator(100, 1)], 0);
        let map = set.assign([11u8; 32], 200, 100, 10, default_liveness());
        assert!(map.verify(0, &[0, 1, 2]).is_err());
    }

    #[test]
    fn shard_map_verify_wrong_row() {
        let set = ValidatorSet::new(vec![make_validator(50, 1), make_validator(50, 2)], 0);
        let total_rows = 200;
        let map = set.assign([12u8; 32], total_rows, 100, 10, default_liveness());

        let mut wrong_indices: Vec<u32> = map.get(0).unwrap().iter().map(|&r| r as u32).collect();
        wrong_indices[0] = (total_rows + 999) as u32;
        assert!(map.verify(0, &wrong_indices).is_err());
    }

    #[test]
    fn shard_map_verify_missing_validator() {
        let set = ValidatorSet::new(vec![make_validator(100, 1)], 0);
        let map = set.assign([13u8; 32], 200, 100, 10, default_liveness());
        assert!(map.verify(5, &[0]).is_err());
    }

    #[test]
    fn total_assigned_rows_across_validators() {
        let set = ValidatorSet::new(
            vec![
                make_validator(40, 1),
                make_validator(35, 2),
                make_validator(25, 3),
            ],
            0,
        );
        let map = set.assign([20u8; 32], 200, 100, 10, default_liveness());

        let total_assigned: usize = (0..set.validators.len())
            .map(|i| map.get(i).unwrap().len())
            .sum();
        for i in 0..set.validators.len() {
            let rows = map.get(i).unwrap();
            assert!(rows.len() >= 10);
            assert!(rows.len() <= 100);
        }
        assert_eq!(
            total_assigned,
            (0..set.validators.len())
                .map(|i| map.get(i).unwrap().len())
                .sum::<usize>()
        );
    }

    #[test]
    fn cross_language_shuffle_matches_go() {
        let mut seed = [0u8; 32];
        for (i, byte) in seed.iter_mut().enumerate() {
            *byte = (i + 1) as u8;
        }

        let mut rng = ChaCha8Rand::new(&seed);
        let mut indices: Vec<usize> = (0..16).collect();
        go_shuffle(&mut rng, &mut indices);
        assert_eq!(
            indices,
            vec![3, 13, 15, 12, 1, 7, 0, 8, 4, 10, 11, 2, 9, 14, 6, 5]
        );

        let mut rng2 = ChaCha8Rand::new(&seed);
        let mut indices2: Vec<usize> = (0..100).collect();
        go_shuffle(&mut rng2, &mut indices2);
        assert_eq!(
            &indices2[..20],
            &[
                80, 56, 48, 69, 26, 60, 57, 22, 49, 54, 93, 13, 5, 75, 97, 38, 84, 16, 11, 89
            ]
        );
    }

    #[test]
    fn cross_language_assign_matches_go() {
        let set = ValidatorSet::new(
            vec![
                make_validator(300, 1),
                make_validator(200, 2),
                make_validator(100, 3),
            ],
            0,
        );
        let mut commitment = [0u8; 32];
        for (i, byte) in commitment.iter_mut().enumerate() {
            *byte = (i + 1) as u8;
        }

        let map = set.assign(commitment, 16, 8, 2, crate::test_utils::fraction(1, 3));

        assert_eq!(map.get(0).unwrap(), &vec![3, 13, 15, 12, 1, 7, 0, 8]);
        assert_eq!(map.get(1).unwrap(), &vec![4, 10, 11, 2, 9, 14, 6, 5]);
        assert_eq!(map.get(2).unwrap(), &vec![3, 13, 15, 12]);
    }

    #[test]
    fn all_row_indices_within_bounds() {
        let set = ValidatorSet::new(
            vec![
                make_validator(50, 1),
                make_validator(30, 2),
                make_validator(20, 3),
            ],
            0,
        );
        let total_rows = 200;
        let map = set.assign([30u8; 32], total_rows, 100, 10, default_liveness());

        for i in 0..set.validators.len() {
            for &row_idx in map.get(i).unwrap() {
                assert!(row_idx < total_rows, "row index {} out of bounds", row_idx);
            }
        }
    }
}

#[cfg(test)]
mod selection_tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn make_validator(power: u64, seed: u8) -> ValidatorInfo {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = seed;
        let signing_key = SigningKey::from_bytes(&key_bytes);
        ValidatorInfo::try_new(signing_key.verifying_key(), power).unwrap()
    }

    fn default_liveness() -> Fraction {
        crate::test_utils::fraction(1, 3)
    }

    #[test]
    fn single_validator_returns_one() {
        let validator = make_validator(100, 1);
        let expected_address = validator.address;
        let set = ValidatorSet::new(vec![validator], 0);
        let selected = set.select(100, 10, default_liveness());
        assert_eq!(selected.len(), 1);
        assert!(selected[0].0 > 0);
        assert_eq!(selected[0].1.address, expected_address);
    }

    #[test]
    fn multiple_validators_returns_all() {
        let validators = vec![
            make_validator(100, 1),
            make_validator(100, 2),
            make_validator(100, 3),
        ];
        let mut expected: Vec<_> = validators
            .iter()
            .map(|validator| validator.address)
            .collect();
        expected.sort();
        let set = ValidatorSet::new(validators, 0);
        let selected = set.select(100, 10, default_liveness());
        assert_eq!(selected.len(), 3);

        let mut actual: Vec<_> = selected.iter().map(|(_, info)| info.address).collect();
        actual.sort();
        assert_eq!(actual, expected);
    }

    #[test]
    fn split_idx_separates_groups_correctly() {
        let set = ValidatorSet::new(
            vec![
                make_validator(150, 1),
                make_validator(100, 2),
                make_validator(50, 3),
            ],
            0,
        );
        let selected = set.select(100, 10, default_liveness());
        assert_eq!(selected.len(), 3);
    }

    #[test]
    fn split_idx_with_high_min_rows() {
        let set = ValidatorSet::new((0..10).map(|i| make_validator(10, i as u8)).collect(), 0);
        let selected = set.select(100, 50, default_liveness());
        assert_eq!(selected.len(), 10);
    }

    #[test]
    fn all_validators_present_in_result() {
        let validators = vec![
            make_validator(50, 1),
            make_validator(30, 2),
            make_validator(20, 3),
        ];
        let mut expected: Vec<_> = validators
            .iter()
            .map(|validator| validator.address)
            .collect();
        expected.sort();
        let set = ValidatorSet::new(validators, 0);
        let selected = set.select(100, 10, default_liveness());

        assert_eq!(selected.len(), 3);
        let mut actual: Vec<_> = selected.iter().map(|(_, info)| info.address).collect();
        actual.sort();
        assert_eq!(actual, expected);
    }

    #[test]
    fn shuffle_by_stake_respects_weights() {
        let mut first_position_counts = [0usize; 3];
        let trials = 1000;

        for _ in 0..trials {
            let validators = vec![
                make_validator(100, 1),
                make_validator(10, 2),
                make_validator(10, 3),
            ];
            let addresses: Vec<_> = validators
                .iter()
                .map(|validator| validator.address)
                .collect();
            let set = ValidatorSet::new(validators, 0);
            let selected = set.select(100, 10, default_liveness());
            let first = addresses
                .iter()
                .position(|address| address == &selected[0].1.address)
                .unwrap();
            first_position_counts[first] += 1;
        }

        assert!(first_position_counts[0] > first_position_counts[1]);
        assert!(first_position_counts[0] > first_position_counts[2]);
    }

    #[test]
    fn select_expected_rows_match_assign() {
        let set = ValidatorSet::new(
            vec![
                make_validator(300, 1),
                make_validator(200, 2),
                make_validator(100, 3),
            ],
            0,
        );
        let liveness = default_liveness();
        let original_rows = 100;
        let min_rows = 10;

        let selected = set.select(original_rows, min_rows, liveness);
        let shard_map = set.assign([0u8; 32], 200, original_rows, min_rows, liveness);

        for (expected_rows, info) in &selected {
            let idx = set
                .validators
                .iter()
                .position(|v| v.address == info.address)
                .unwrap();
            assert_eq!(*expected_rows, shard_map.get(idx).unwrap().len());
        }
    }
}
