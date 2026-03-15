//! Validator identity, validator set types, and the [`SetGetter`] trait.

pub(crate) mod shard_map;
pub(crate) mod signature_set;

use std::collections::HashMap;

use chacha8rand::ChaCha8Rand;
use ed25519_dalek::VerifyingKey as Ed25519PublicKey;
use rand::Rng;

use crate::blob::Commitment;
use crate::config::Fraction;
use crate::error::FibreError;
use celestia_grpc::GrpcClient;

pub(crate) use shard_map::ShardMap;

/// A validator's identity and stake information.
#[derive(Debug, Clone)]
pub struct ValidatorInfo {
    /// The validator's consensus address (20-byte CometBFT address).
    pub address: [u8; 20],
    /// The validator's ed25519 public key for signing.
    pub pubkey: Ed25519PublicKey,
    /// The validator's voting power (stake weight).
    pub voting_power: i64,
}

impl ValidatorInfo {
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
    pub validators: Vec<ValidatorInfo>,
    /// The block height at which this validator set is valid.
    pub height: u64,
}

impl ValidatorSet {
    /// Creates a validator set at the given height.
    pub fn new(validators: Vec<ValidatorInfo>, height: u64) -> Self {
        Self { validators, height }
    }

    /// Returns the total voting power of all validators in the set.
    pub fn total_voting_power(&self) -> i64 {
        self.validators.iter().map(|v| v.voting_power).sum()
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
        if self.validators.is_empty() || total_rows == 0 || min_rows == 0 {
            return ShardMap::new(HashMap::new());
        }

        let total_voting_power = self.total_voting_power();
        let rows_per_validator: Vec<usize> = self
            .validators
            .iter()
            .map(|v| {
                let num = (original_rows as i64)
                    * v.voting_power
                    * (liveness_threshold.denominator as i64);
                let den = total_voting_power * (liveness_threshold.numerator as i64);
                let rows = ((num + den - 1) / den) as usize;
                rows.max(min_rows).min(original_rows)
            })
            .collect();

        let mut rng = ChaCha8Rand::new(&commitment);
        let mut row_indices: Vec<usize> = (0..total_rows).collect();
        go_shuffle(&mut rng, &mut row_indices);

        let mut shard_map = HashMap::with_capacity(self.validators.len());
        let mut offset: usize = 0;
        for (i, &row_count) in rows_per_validator.iter().enumerate() {
            let rows: Vec<usize> = (0..row_count)
                .map(|j| row_indices[(offset + j) % total_rows])
                .collect();
            shard_map.insert(i, rows);
            offset += row_count;
        }

        ShardMap::new(shard_map)
    }

    /// Selects validators for shard download, ordered by priority.
    ///
    /// Returns (ordered validator indices, minimum required count).
    pub fn select(
        &self,
        original_rows: usize,
        min_rows: usize,
        liveness_threshold: Fraction,
    ) -> (Vec<usize>, usize) {
        if self.validators.is_empty() {
            return (Vec::new(), 0);
        }

        let total_voting_power = self.total_voting_power();
        let mut indexed: Vec<(usize, i64)> = self
            .validators
            .iter()
            .enumerate()
            .map(|(i, v)| (i, v.voting_power))
            .collect();

        let total_distributed_rows = (original_rows as i64)
            * (liveness_threshold.denominator as i64)
            / (liveness_threshold.numerator as i64);
        let min_stake = ((min_rows as i64) * total_voting_power + total_distributed_rows - 1)
            / total_distributed_rows;

        let mut accumulated: i64 = 0;
        let mut split_idx = indexed.len();
        for (i, &(_, vp)) in indexed.iter().enumerate() {
            accumulated += vp.max(min_stake);
            if accumulated > total_voting_power {
                split_idx = i;
                break;
            }
        }

        let mut rng = rand::thread_rng();
        shuffle_by_stake(&mut indexed[..split_idx], &mut rng);
        shuffle_by_stake(&mut indexed[split_idx..], &mut rng);

        let liveness_num = total_voting_power * (liveness_threshold.numerator as i64);
        let liveness_den = liveness_threshold.denominator as i64;
        let liveness_stake = (liveness_num + liveness_den - 1) / liveness_den;

        let mut min_required: usize = 0;
        let mut covered_stake: i64 = 0;
        for &(_, vp) in &indexed[..split_idx] {
            min_required += 1;
            covered_stake += vp;
            if covered_stake >= liveness_stake {
                break;
            }
        }

        let ordered_indices: Vec<usize> = indexed.iter().map(|&(idx, _)| idx).collect();
        (ordered_indices, min_required)
    }
}

/// Trait for retrieving the current validator set.
///
/// The client only needs the latest set; height-specific lookups are server-side only.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait SetGetter: Send + Sync {
    /// Get the latest validator set.
    async fn head(&self) -> Result<ValidatorSet, FibreError>;
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
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl SetGetter for GrpcSetGetter {
    async fn head(&self) -> Result<ValidatorSet, FibreError> {
        let resp = self.client.get_fibre_validator_set(0).await?;

        let height = resp.height as u64;

        let proto_set = resp.validator_set.ok_or_else(|| {
            FibreError::Other("ValidatorSetResponse missing validator_set".into())
        })?;

        (&proto_set, height).try_into()
    }
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

fn shuffle_by_stake(validators: &mut [(usize, i64)], rng: &mut impl Rng) {
    let n = validators.len();
    if n <= 1 {
        return;
    }

    for i in 0..n - 1 {
        let total_weight: i64 = validators[i..].iter().map(|(_, vp)| vp).sum();
        if total_weight <= 0 {
            break;
        }

        let point = rng.gen_range(0..total_weight);

        let mut cumul: i64 = 0;
        for j in i..n {
            cumul += validators[j].1;
            if point < cumul {
                validators.swap(i, j);
                break;
            }
        }
    }
}

#[cfg(test)]
mod assignment_tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    fn make_validator(power: i64, seed: u8) -> ValidatorInfo {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = seed;
        let signing_key = SigningKey::from_bytes(&key_bytes);
        ValidatorInfo {
            address: [seed; 20],
            pubkey: signing_key.verifying_key(),
            voting_power: power,
        }
    }

    fn default_liveness() -> Fraction {
        Fraction {
            numerator: 1,
            denominator: 3,
        }
    }

    #[test]
    fn empty_validators_returns_empty_map() {
        let set = ValidatorSet::new(vec![], 0);
        let map = set.assign([0u8; 32], 100, 50, 10, default_liveness());
        assert!(map.is_empty());
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

        let map = set.assign(
            commitment,
            16,
            8,
            2,
            Fraction {
                numerator: 1,
                denominator: 3,
            },
        );

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

    fn make_validator(power: i64, seed: u8) -> ValidatorInfo {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = seed;
        let signing_key = SigningKey::from_bytes(&key_bytes);
        ValidatorInfo {
            address: [seed; 20],
            pubkey: signing_key.verifying_key(),
            voting_power: power,
        }
    }

    fn default_liveness() -> Fraction {
        Fraction {
            numerator: 1,
            denominator: 3,
        }
    }

    #[test]
    fn empty_validators_returns_empty() {
        let set = ValidatorSet::new(vec![], 0);
        let (indices, min_req) = set.select(100, 10, default_liveness());
        assert!(indices.is_empty());
        assert_eq!(min_req, 0);
    }

    #[test]
    fn single_validator_min_required_is_one() {
        let set = ValidatorSet::new(vec![make_validator(100, 1)], 0);
        let (indices, min_req) = set.select(100, 10, default_liveness());
        assert_eq!(indices, vec![0]);
        assert_eq!(min_req, 1);
    }

    #[test]
    fn multiple_validators_min_required_covers_liveness() {
        let set = ValidatorSet::new(
            vec![
                make_validator(100, 1),
                make_validator(100, 2),
                make_validator(100, 3),
            ],
            0,
        );
        let (indices, min_req) = set.select(100, 10, default_liveness());
        assert_eq!(indices.len(), 3);
        assert_eq!(min_req, 1);

        let mut present = indices;
        present.sort();
        assert_eq!(present, vec![0, 1, 2]);
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
        let (indices, min_req) = set.select(100, 10, default_liveness());
        assert_eq!(indices.len(), 3);
        assert!(min_req >= 1);
    }

    #[test]
    fn split_idx_with_high_min_rows() {
        let set = ValidatorSet::new((0..10).map(|i| make_validator(10, i as u8)).collect(), 0);
        let (_indices, min_req) = set.select(100, 50, default_liveness());
        assert!(min_req >= 1);
        assert!(min_req <= 5);
    }

    #[test]
    fn all_indices_present_in_result() {
        let set = ValidatorSet::new(
            vec![
                make_validator(50, 1),
                make_validator(30, 2),
                make_validator(20, 3),
            ],
            0,
        );
        let (indices, _) = set.select(100, 10, default_liveness());

        assert_eq!(indices.len(), 3);
        let mut sorted = indices;
        sorted.sort();
        assert_eq!(sorted, vec![0, 1, 2]);
    }

    #[test]
    fn shuffle_by_stake_respects_weights() {
        let mut first_position_counts = [0usize; 3];
        let trials = 1000;

        for _ in 0..trials {
            let set = ValidatorSet::new(
                vec![
                    make_validator(100, 1),
                    make_validator(10, 2),
                    make_validator(10, 3),
                ],
                0,
            );
            let (indices, _) = set.select(100, 10, default_liveness());
            first_position_counts[indices[0]] += 1;
        }

        assert!(first_position_counts[0] > first_position_counts[1]);
        assert!(first_position_counts[0] > first_position_counts[2]);
    }
}
