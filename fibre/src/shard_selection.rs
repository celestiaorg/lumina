//! Shard download selection algorithm.
//!
//! Determines the order and minimum count of validators to download from,
//! using stake-weighted shuffling for load balancing. This mirrors the Go
//! implementation's `Set.Select` in `celestia-app-fibre/fibre/validator/set.go`.

use rand::Rng;

use crate::config::Fraction;
use crate::validator::ValidatorInfo;

/// Select validators for shard download, ordered by priority.
///
/// Returns a tuple of:
/// - `Vec<usize>`: validator indices (into the `validators` slice), ordered by
///   download priority. Validators before the split point (non-overlapping group)
///   come first, followed by the overlapping group, each group shuffled by stake
///   weight.
/// - `usize`: the minimum number of validators (from the front of the returned
///   list) whose combined stake covers the liveness threshold, guaranteeing enough
///   rows for reconstruction.
///
/// The algorithm:
/// 1. Computes `min_stake` — the minimum stake a validator effectively contributes
///    (derived from `min_rows`).
/// 2. Finds the split point where cumulative `max(voting_power, min_stake)` exceeds
///    `total_stake`. Validators before this point have non-overlapping row
///    assignments; those after may overlap.
/// 3. Shuffles each group independently by stake weight (higher stake = more likely
///    to appear first), providing load balancing.
/// 4. Counts the minimum validators from the non-overlapping group needed to cover
///    the liveness threshold stake.
pub fn select(
    validators: &[ValidatorInfo],
    total_voting_power: i64,
    original_rows: usize,
    min_rows: usize,
    liveness_threshold: Fraction,
) -> (Vec<usize>, usize) {
    if validators.is_empty() {
        return (Vec::new(), 0);
    }

    // Build (original_index, voting_power) pairs
    let mut indexed: Vec<(usize, i64)> = validators
        .iter()
        .enumerate()
        .map(|(i, v)| (i, v.voting_power))
        .collect();

    let total_stake = total_voting_power;

    // total_distributed_rows = original_rows * denominator / numerator
    let total_distributed_rows = (original_rows as i64) * (liveness_threshold.denominator as i64)
        / (liveness_threshold.numerator as i64);

    // min_stake = ceil(min_rows * total_stake / total_distributed_rows)
    let min_stake =
        ((min_rows as i64) * total_stake + total_distributed_rows - 1) / total_distributed_rows;

    // Find split point where accumulated max(voting_power, min_stake) exceeds total_stake
    let mut accumulated: i64 = 0;
    let mut split_idx = indexed.len();
    for (i, &(_, vp)) in indexed.iter().enumerate() {
        accumulated += vp.max(min_stake);
        if accumulated > total_stake {
            split_idx = i;
            break;
        }
    }

    // Shuffle each group by stake weight using non-cryptographic RNG
    let mut rng = rand::thread_rng();
    shuffle_by_stake(&mut indexed[..split_idx], &mut rng);
    shuffle_by_stake(&mut indexed[split_idx..], &mut rng);

    // Count minimum validators from the non-overlapping group to cover liveness threshold
    let liveness_num = total_stake * (liveness_threshold.numerator as i64);
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

/// Shuffles validators in-place using stake-weighted random selection.
///
/// Higher voting power validators are more likely to appear earlier.
/// This is a weighted variant of Fisher-Yates: for each position i,
/// we pick a random point in the cumulative weight of remaining elements,
/// then swap the selected element to position i.
fn shuffle_by_stake(validators: &mut [(usize, i64)], rng: &mut impl Rng) {
    let n = validators.len();
    if n <= 1 {
        return;
    }

    for i in 0..n - 1 {
        // Calculate total weight of remaining validators
        let total_weight: i64 = validators[i..].iter().map(|(_, vp)| vp).sum();
        if total_weight <= 0 {
            break;
        }

        // Pick a random point in the weight space [0, total_weight)
        let point = rng.gen_range(0..total_weight);

        // Find and swap the selected validator
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
    fn empty_validators_returns_empty() {
        let (indices, min_req) = select(&[], 0, 100, 10, default_liveness());
        assert!(indices.is_empty());
        assert_eq!(min_req, 0);
    }

    #[test]
    fn single_validator_min_required_is_one() {
        let v = vec![make_validator(100, 1)];
        let (indices, min_req) = select(&v, 100, 100, 10, default_liveness());

        assert_eq!(indices.len(), 1);
        assert_eq!(indices[0], 0);
        assert_eq!(min_req, 1);
    }

    #[test]
    fn multiple_validators_min_required_covers_liveness() {
        // Three validators with equal stake
        let v = vec![
            make_validator(100, 1),
            make_validator(100, 2),
            make_validator(100, 3),
        ];
        let total_vp = 300;
        let (indices, min_req) = select(&v, total_vp, 100, 10, default_liveness());

        assert_eq!(indices.len(), 3);

        // liveness_stake = ceil(300 * 1 / 3) = 100
        // Each validator has 100 stake, so 1 validator should suffice
        assert_eq!(min_req, 1);

        // All validator indices should be present
        let mut present = indices.clone();
        present.sort();
        assert_eq!(present, vec![0, 1, 2]);
    }

    #[test]
    fn split_idx_separates_groups_correctly() {
        // Create validators where the split occurs at a known position.
        // total_stake = 300
        // total_distributed_rows = 100 * 3 / 1 = 300
        // min_stake = ceil(10 * 300 / 300) = 10
        //
        // Validator voting powers: [150, 100, 50]
        // accumulated with max(vp, 10):
        //   i=0: 150 (not > 300)
        //   i=1: 150 + 100 = 250 (not > 300)
        //   i=2: 250 + 50 = 300 (not > 300)
        //   split_idx = 3 (all in non-overlapping group)
        let v = vec![
            make_validator(150, 1),
            make_validator(100, 2),
            make_validator(50, 3),
        ];
        let total_vp = 300;
        let (indices, min_req) = select(&v, total_vp, 100, 10, default_liveness());

        assert_eq!(indices.len(), 3);
        // liveness_stake = ceil(300 * 1 / 3) = 100
        // After stake-weighted shuffle, the first validator in the non-overlapping
        // group should have enough stake. With 3 validators totaling 300 and
        // liveness = 100, min_required should be 1 (any single validator with >= 100).
        assert!(min_req >= 1);
    }

    #[test]
    fn split_idx_with_high_min_rows() {
        // When min_rows is high, min_stake is high, and we reach split sooner.
        // total_stake = 100
        // total_distributed_rows = 100 * 3 / 1 = 300
        // min_stake = ceil(50 * 100 / 300) = ceil(16.66) = 17
        //
        // Validator voting powers: [10, 10, 10, 10, 10, 10, 10, 10, 10, 10] (10 validators)
        // accumulated with max(10, 17) = 17 each:
        //   i=0: 17 (not > 100)
        //   i=1: 34 (not > 100)
        //   i=2: 51 (not > 100)
        //   i=3: 68 (not > 100)
        //   i=4: 85 (not > 100)
        //   i=5: 102 > 100 => split_idx = 5
        let v: Vec<ValidatorInfo> = (0..10).map(|i| make_validator(10, i as u8)).collect();
        let total_vp = 100;
        let (_indices, min_req) = select(&v, total_vp, 100, 50, default_liveness());

        // liveness_stake = ceil(100 / 3) = 34
        // We need at least ceil(34 / 10) = 4 validators from the non-overlapping group
        // (each has voting power 10)
        assert!(min_req >= 1);
        assert!(min_req <= 5); // At most split_idx validators can be selected
    }

    #[test]
    fn all_indices_present_in_result() {
        let v = vec![
            make_validator(50, 1),
            make_validator(30, 2),
            make_validator(20, 3),
        ];
        let total_vp = 100;
        let (indices, _) = select(&v, total_vp, 100, 10, default_liveness());

        assert_eq!(indices.len(), 3);
        let mut sorted = indices.clone();
        sorted.sort();
        assert_eq!(sorted, vec![0, 1, 2]);
    }

    #[test]
    fn shuffle_by_stake_respects_weights() {
        // Run many iterations to verify that higher-stake validators appear first
        // more often. This is a statistical test.
        let mut first_position_counts = [0usize; 3];
        let trials = 1000;

        for _ in 0..trials {
            let v = vec![
                make_validator(100, 1), // high stake
                make_validator(10, 2),  // low stake
                make_validator(10, 3),  // low stake
            ];
            let total_vp = 120;
            let (indices, _) = select(&v, total_vp, 100, 10, default_liveness());
            first_position_counts[indices[0]] += 1;
        }

        // Validator 0 (highest stake) should appear first most often
        assert!(
            first_position_counts[0] > first_position_counts[1],
            "higher stake validator should appear first more often: {:?}",
            first_position_counts
        );
        assert!(
            first_position_counts[0] > first_position_counts[2],
            "higher stake validator should appear first more often: {:?}",
            first_position_counts
        );
    }
}
