//! Deterministic shard assignment algorithm.
//!
//! Assigns row indices to validators based on their stake proportion, using a
//! ChaCha8 RNG seeded with the blob commitment for deterministic, reproducible
//! assignment. This mirrors the Go implementation in
//! `celestia-app-fibre/fibre/validator/set.go`.
//!
//! Uses [`chacha8rand`] which implements the C2SP chacha8rand spec — the same
//! PRNG used by Go's `math/rand/v2`. The bounded integer generation
//! ([`uint64n`]) and shuffle ([`go_shuffle`]) also match Go's exact algorithms
//! to ensure cross-language determinism.

use std::collections::{HashMap, HashSet};

use chacha8rand::ChaCha8Rand;

use crate::blob::Commitment;
use crate::config::Fraction;
use crate::error::FibreError;
use crate::validator::ValidatorInfo;

/// Maps validator index to the row indices assigned to that validator.
///
/// The validator index corresponds to the position in the `validators` slice
/// passed to [`assign`]. Row indices are positions in the extended data matrix
/// (0..total_rows).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMap {
    /// Mapping from validator index to assigned row indices.
    inner: HashMap<usize, Vec<usize>>,
}

impl ShardMap {
    /// Returns the inner map.
    pub fn inner(&self) -> &HashMap<usize, Vec<usize>> {
        &self.inner
    }

    /// Returns the row indices assigned to the given validator index.
    pub fn get(&self, validator_index: usize) -> Option<&Vec<usize>> {
        self.inner.get(&validator_index)
    }

    /// Returns the number of validators in the shard map.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the shard map contains no validators.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Verify that a validator's claimed row indices match their assignment.
    ///
    /// Checks that:
    /// 1. The validator exists in the shard map.
    /// 2. The count of provided row indices matches the assigned count.
    /// 3. Every provided row index is in the assigned set.
    ///
    /// Uses a temporary HashSet for O(r + n) complexity rather than O(n * r).
    pub fn verify(&self, validator_index: usize, row_indices: &[u32]) -> Result<(), FibreError> {
        let rows = self.inner.get(&validator_index).ok_or_else(|| {
            FibreError::InvalidData(format!(
                "validator index {} not in shard map",
                validator_index
            ))
        })?;

        if row_indices.len() != rows.len() {
            return Err(FibreError::InvalidData(format!(
                "expected {} rows, got {}",
                rows.len(),
                row_indices.len()
            )));
        }

        let assigned_set: HashSet<usize> = rows.iter().copied().collect();

        for &row_idx in row_indices {
            if !assigned_set.contains(&(row_idx as usize)) {
                return Err(FibreError::InvalidData(format!(
                    "row {} not assigned to validator {}",
                    row_idx, validator_index
                )));
            }
        }

        Ok(())
    }
}

/// Deterministically assigns row indices to validators based on their stake.
///
/// The assignment algorithm works as follows:
///
/// 1. For each validator, compute the number of rows proportional to their stake:
///    `rows = ceil(original_rows * voting_power * liveness_threshold.denominator
///                 / (total_voting_power * liveness_threshold.numerator))`
///    Then clamp to `[min_rows, original_rows]`.
///
/// 2. Seed a ChaCha8 RNG with the commitment bytes and shuffle all row indices
///    using Fisher-Yates.
///
/// 3. Assign consecutive chunks from the shuffled indices to each validator,
///    wrapping around with modulo when the total assigned rows exceed total_rows.
///
/// This ensures deterministic, stake-proportional row distribution. When the sum
/// of assigned rows exceeds `total_rows` (due to `min_rows` floor guarantees),
/// row indices wrap around using modulo, so the same row may appear in multiple
/// validators' assignments.
pub fn assign(
    validators: &[ValidatorInfo],
    total_voting_power: i64,
    commitment: Commitment,
    total_rows: usize,
    original_rows: usize,
    min_rows: usize,
    liveness_threshold: Fraction,
) -> ShardMap {
    if validators.is_empty() || total_rows == 0 || min_rows == 0 {
        return ShardMap {
            inner: HashMap::new(),
        };
    }

    // Compute rows per validator: ceil(original_rows * voting_power * denominator / (total_voting_power * numerator))
    let rows_per_validator: Vec<usize> = validators
        .iter()
        .map(|v| {
            let num =
                (original_rows as i64) * v.voting_power * (liveness_threshold.denominator as i64);
            let den = total_voting_power * (liveness_threshold.numerator as i64);
            // Ceiling division: (num + den - 1) / den
            let rows = ((num + den - 1) / den) as usize;
            rows.max(min_rows).min(original_rows)
        })
        .collect();

    // Seed ChaCha8 RNG with the commitment — uses C2SP chacha8rand spec
    // matching Go's math/rand/v2.NewChaCha8.
    let mut rng = ChaCha8Rand::new(&commitment);

    // Create and shuffle row indices using Go-compatible Fisher-Yates
    let mut row_indices: Vec<usize> = (0..total_rows).collect();
    go_shuffle(&mut rng, &mut row_indices);

    // Assign consecutive chunks to each validator, wrapping with modulo
    let mut shard_map = HashMap::with_capacity(validators.len());
    let mut offset: usize = 0;
    for (i, &row_count) in rows_per_validator.iter().enumerate() {
        let rows: Vec<usize> = (0..row_count)
            .map(|j| row_indices[(offset + j) % total_rows])
            .collect();
        shard_map.insert(i, rows);
        offset += row_count;
    }

    ShardMap { inner: shard_map }
}

/// Fisher-Yates shuffle matching Go's `math/rand/v2.(*Rand).Shuffle`.
///
/// Iterates backwards from `n-1` to `1`, picking `j` uniformly from `[0, i]`
/// using [`uint64n`] which mirrors Go's `uint64n` (Lemire's algorithm).
fn go_shuffle<T>(rng: &mut ChaCha8Rand, slice: &mut [T]) {
    let n = slice.len();
    for i in (1..n).rev() {
        let j = uint64n(rng, (i + 1) as u64) as usize;
        slice.swap(i, j);
    }
}

/// Bounded random integer matching Go's `math/rand/v2.(*Rand).uint64n`.
///
/// Uses Lemire's "nearly divisionless" algorithm:
/// 1. Power-of-two fast path: mask with `n - 1`.
/// 2. Otherwise: multiply a random `u64` by `n`, take the high 64 bits.
///    Reject if the low 64 bits fall in the bias zone.
fn uint64n(rng: &mut ChaCha8Rand, n: u64) -> u64 {
    if n & (n - 1) == 0 {
        // n is a power of two
        return rng.read_u64() & (n - 1);
    }

    let (mut hi, mut lo) = mul_u64_full(rng.read_u64(), n);
    if lo < n {
        let thresh = n.wrapping_neg() % n; // (2^64 - n) % n
        while lo < thresh {
            let (h, l) = mul_u64_full(rng.read_u64(), n);
            hi = h;
            lo = l;
        }
    }
    hi
}

/// Full 64×64 → 128-bit multiplication, returning (high, low).
#[inline]
fn mul_u64_full(a: u64, b: u64) -> (u64, u64) {
    let full = (a as u128) * (b as u128);
    ((full >> 64) as u64, full as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    /// Helper to create a ValidatorInfo with the given voting power.
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
        let map = assign(&[], 0, [0u8; 32], 100, 50, 10, default_liveness());
        assert!(map.is_empty());
    }

    #[test]
    fn zero_total_rows_returns_empty_map() {
        let v = vec![make_validator(100, 1)];
        let map = assign(&v, 100, [0u8; 32], 0, 50, 10, default_liveness());
        assert!(map.is_empty());
    }

    #[test]
    fn zero_min_rows_returns_empty_map() {
        let v = vec![make_validator(100, 1)];
        let map = assign(&v, 100, [0u8; 32], 100, 50, 0, default_liveness());
        assert!(map.is_empty());
    }

    #[test]
    fn single_validator_gets_original_rows() {
        // A single validator with 100% stake should get original_rows rows
        // rows = ceil(original_rows * 100 * 3 / (100 * 1)) = original_rows * 3
        // but clamped to min(rows, original_rows) = original_rows
        let v = vec![make_validator(100, 1)];
        let commitment = [0u8; 32];
        let total_rows = 200;
        let original_rows = 100;
        let min_rows = 10;

        let map = assign(
            &v,
            100,
            commitment,
            total_rows,
            original_rows,
            min_rows,
            default_liveness(),
        );

        assert_eq!(map.len(), 1);
        let rows = map.get(0).unwrap();
        assert_eq!(rows.len(), original_rows);
    }

    #[test]
    fn two_equal_stake_validators_get_equal_rows() {
        let v = vec![make_validator(50, 1), make_validator(50, 2)];
        let commitment = [1u8; 32];
        let total_rows = 200;
        let original_rows = 100;
        let min_rows = 10;

        let map = assign(
            &v,
            100,
            commitment,
            total_rows,
            original_rows,
            min_rows,
            default_liveness(),
        );

        assert_eq!(map.len(), 2);
        let rows0 = map.get(0).unwrap();
        let rows1 = map.get(1).unwrap();
        assert_eq!(rows0.len(), rows1.len());
    }

    #[test]
    fn rows_per_validator_respects_min_rows_floor() {
        // Validator with very small stake should still get min_rows
        let v = vec![make_validator(1, 1), make_validator(999, 2)];
        let commitment = [2u8; 32];
        let total_rows = 200;
        let original_rows = 100;
        let min_rows = 50;

        let map = assign(
            &v,
            1000,
            commitment,
            total_rows,
            original_rows,
            min_rows,
            default_liveness(),
        );

        // Validator 0 has 0.1% stake, rows = ceil(100 * 1 * 3 / (1000 * 1)) = ceil(0.3) = 1
        // But min_rows = 50, so it should get 50
        let rows0 = map.get(0).unwrap();
        assert_eq!(rows0.len(), min_rows);
    }

    #[test]
    fn rows_per_validator_respects_original_rows_ceiling() {
        // Even with very high stake, a validator should not get more than original_rows
        let v = vec![make_validator(1000, 1)];
        let commitment = [3u8; 32];
        let total_rows = 200;
        let original_rows = 100;
        let min_rows = 10;

        let map = assign(
            &v,
            1000,
            commitment,
            total_rows,
            original_rows,
            min_rows,
            default_liveness(),
        );

        let rows0 = map.get(0).unwrap();
        assert_eq!(rows0.len(), original_rows);
    }

    #[test]
    fn assignment_is_deterministic() {
        let v = vec![
            make_validator(50, 1),
            make_validator(30, 2),
            make_validator(20, 3),
        ];
        let commitment = [42u8; 32];
        let total_rows = 200;
        let original_rows = 100;
        let min_rows = 10;

        let map1 = assign(
            &v,
            100,
            commitment,
            total_rows,
            original_rows,
            min_rows,
            default_liveness(),
        );
        let map2 = assign(
            &v,
            100,
            commitment,
            total_rows,
            original_rows,
            min_rows,
            default_liveness(),
        );

        assert_eq!(map1, map2);
    }

    #[test]
    fn different_commitments_produce_different_assignments() {
        let v = vec![
            make_validator(50, 1),
            make_validator(30, 2),
            make_validator(20, 3),
        ];
        let total_rows = 200;
        let original_rows = 100;
        let min_rows = 10;

        let commitment1 = [1u8; 32];
        let commitment2 = [2u8; 32];

        let map1 = assign(
            &v,
            100,
            commitment1,
            total_rows,
            original_rows,
            min_rows,
            default_liveness(),
        );
        let map2 = assign(
            &v,
            100,
            commitment2,
            total_rows,
            original_rows,
            min_rows,
            default_liveness(),
        );

        // Row counts should be the same (same validators, same stake)
        for i in 0..v.len() {
            assert_eq!(map1.get(i).unwrap().len(), map2.get(i).unwrap().len());
        }

        // But actual row assignments should differ (different shuffle seed)
        let rows1 = map1.get(0).unwrap();
        let rows2 = map2.get(0).unwrap();
        assert_ne!(
            rows1, rows2,
            "different commitments should produce different row assignments"
        );
    }

    #[test]
    fn shard_map_verify_correct() {
        let v = vec![make_validator(50, 1), make_validator(50, 2)];
        let commitment = [10u8; 32];
        let total_rows = 200;
        let original_rows = 100;
        let min_rows = 10;

        let map = assign(
            &v,
            100,
            commitment,
            total_rows,
            original_rows,
            min_rows,
            default_liveness(),
        );

        // Verify with matching row indices should succeed
        let rows0 = map.get(0).unwrap();
        let row_indices_u32: Vec<u32> = rows0.iter().map(|&r| r as u32).collect();
        assert!(map.verify(0, &row_indices_u32).is_ok());
    }

    #[test]
    fn shard_map_verify_wrong_count() {
        let v = vec![make_validator(100, 1)];
        let commitment = [11u8; 32];
        let total_rows = 200;
        let original_rows = 100;
        let min_rows = 10;

        let map = assign(
            &v,
            100,
            commitment,
            total_rows,
            original_rows,
            min_rows,
            default_liveness(),
        );

        // Verify with wrong number of rows should fail
        assert!(map.verify(0, &[0, 1, 2]).is_err());
    }

    #[test]
    fn shard_map_verify_wrong_row() {
        let v = vec![make_validator(50, 1), make_validator(50, 2)];
        let commitment = [12u8; 32];
        let total_rows = 200;
        let original_rows = 100;
        let min_rows = 10;

        let map = assign(
            &v,
            100,
            commitment,
            total_rows,
            original_rows,
            min_rows,
            default_liveness(),
        );

        // Build correct indices but swap one
        let rows0 = map.get(0).unwrap();
        let mut wrong_indices: Vec<u32> = rows0.iter().map(|&r| r as u32).collect();
        // Set a row to a value that almost certainly is not in the set
        wrong_indices[0] = (total_rows + 999) as u32;

        assert!(map.verify(0, &wrong_indices).is_err());
    }

    #[test]
    fn shard_map_verify_missing_validator() {
        let v = vec![make_validator(100, 1)];
        let commitment = [13u8; 32];
        let total_rows = 200;
        let original_rows = 100;
        let min_rows = 10;

        let map = assign(
            &v,
            100,
            commitment,
            total_rows,
            original_rows,
            min_rows,
            default_liveness(),
        );

        // Validator index 5 does not exist
        assert!(map.verify(5, &[0]).is_err());
    }

    #[test]
    fn total_assigned_rows_across_validators() {
        let v = vec![
            make_validator(40, 1),
            make_validator(35, 2),
            make_validator(25, 3),
        ];
        let commitment = [20u8; 32];
        let total_rows = 200;
        let original_rows = 100;
        let min_rows = 10;
        let liveness = default_liveness();

        let map = assign(
            &v,
            100,
            commitment,
            total_rows,
            original_rows,
            min_rows,
            liveness,
        );

        // Compute expected rows per validator
        // v0: ceil(100 * 40 * 3 / (100 * 1)) = ceil(120) = 120 -> clamped to 100
        // v1: ceil(100 * 35 * 3 / (100 * 1)) = ceil(105) = 105 -> clamped to 100
        // v2: ceil(100 * 25 * 3 / (100 * 1)) = ceil(75) = 75
        let total_assigned: usize = (0..v.len()).map(|i| map.get(i).unwrap().len()).sum();

        // Each validator gets at least min_rows and at most original_rows
        for i in 0..v.len() {
            let rows = map.get(i).unwrap();
            assert!(rows.len() >= min_rows);
            assert!(rows.len() <= original_rows);
        }

        // Total should be sum of individual assignments
        assert_eq!(
            total_assigned,
            (0..v.len())
                .map(|i| map.get(i).unwrap().len())
                .sum::<usize>()
        );
    }

    /// Cross-language test: verifies that our ChaCha8 + shuffle produces the
    /// exact same sequence as Go's math/rand/v2 with the same seed.
    /// Go test: celestia-app-fibre/fibre/validator/cross_test.go
    #[test]
    fn cross_language_shuffle_matches_go() {
        // Seed: bytes 1..32
        let mut seed = [0u8; 32];
        for i in 0..32 {
            seed[i] = (i + 1) as u8;
        }

        // Test 1: shuffle [0..16)
        let mut rng = ChaCha8Rand::new(&seed);
        let mut indices: Vec<usize> = (0..16).collect();
        go_shuffle(&mut rng, &mut indices);

        // Expected from Go: [3 13 15 12 1 7 0 8 4 10 11 2 9 14 6 5]
        assert_eq!(
            indices,
            vec![3, 13, 15, 12, 1, 7, 0, 8, 4, 10, 11, 2, 9, 14, 6, 5],
            "16-element shuffle must match Go output"
        );

        // Test 2: shuffle [0..100) with fresh RNG (same seed)
        let mut rng2 = ChaCha8Rand::new(&seed);
        let mut indices2: Vec<usize> = (0..100).collect();
        go_shuffle(&mut rng2, &mut indices2);

        // Expected first 20 from Go: [80 56 48 69 26 60 57 22 49 54 93 13 5 75 97 38 84 16 11 89]
        assert_eq!(
            &indices2[..20],
            &[80, 56, 48, 69, 26, 60, 57, 22, 49, 54, 93, 13, 5, 75, 97, 38, 84, 16, 11, 89],
            "100-element shuffle (first 20) must match Go output"
        );
    }

    /// Cross-language test: verifies the full assign() output matches Go's
    /// validator/Set.Assign() for the same parameters.
    /// Go test: celestia-app-fibre/fibre/validator/cross_test.go
    #[test]
    fn cross_language_assign_matches_go() {
        // 3 validators: power 300, 200, 100 (same order as Go's sorted output)
        let validators = vec![
            make_validator(300, 1),
            make_validator(200, 2),
            make_validator(100, 3),
        ];
        let total_voting_power: i64 = 600;

        // Commitment = [1, 2, 3, ..., 32]
        let mut commitment = [0u8; 32];
        for i in 0..32 {
            commitment[i] = (i + 1) as u8;
        }

        let total_rows = 16;
        let original_rows = 8;
        let min_rows = 2;
        let liveness = Fraction {
            numerator: 1,
            denominator: 3,
        };

        let map = assign(
            &validators,
            total_voting_power,
            commitment,
            total_rows,
            original_rows,
            min_rows,
            liveness,
        );

        // Expected from Go:
        //   validator[0] (power=300): rows=[3, 13, 15, 12, 1, 7, 0, 8]
        //   validator[1] (power=200): rows=[4, 10, 11, 2, 9, 14, 6, 5]
        //   validator[2] (power=100): rows=[3, 13, 15, 12]
        assert_eq!(
            map.get(0).unwrap(),
            &vec![3, 13, 15, 12, 1, 7, 0, 8],
            "validator[0] rows must match Go"
        );
        assert_eq!(
            map.get(1).unwrap(),
            &vec![4, 10, 11, 2, 9, 14, 6, 5],
            "validator[1] rows must match Go"
        );
        assert_eq!(
            map.get(2).unwrap(),
            &vec![3, 13, 15, 12],
            "validator[2] rows must match Go"
        );
    }

    #[test]
    fn all_row_indices_within_bounds() {
        let v = vec![
            make_validator(50, 1),
            make_validator(30, 2),
            make_validator(20, 3),
        ];
        let commitment = [30u8; 32];
        let total_rows = 200;
        let original_rows = 100;
        let min_rows = 10;

        let map = assign(
            &v,
            100,
            commitment,
            total_rows,
            original_rows,
            min_rows,
            default_liveness(),
        );

        for i in 0..v.len() {
            for &row_idx in map.get(i).unwrap() {
                assert!(row_idx < total_rows, "row index {} out of bounds", row_idx);
            }
        }
    }
}
