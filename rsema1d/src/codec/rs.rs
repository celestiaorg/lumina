use super::default_work_budget;
use crate::codec::rows::{OriginalRowsView, RowMatrix};
use crate::error::{Error, Result};
use crate::field::GF128;
use crate::params::Parameters;
use rayon::prelude::*;
use reed_solomon_simd::engine::DefaultEngine;
use reed_solomon_simd::rate::{HighRateEncoder, RateEncoder};
use std::num::NonZeroUsize;

/// Leopard operates on 64-byte blocks, so stripes stay block-aligned.
const MIN_STRIPE: usize = 64;

#[derive(Clone, Copy)]
struct StripePlan {
    stripe_size: NonZeroUsize,
    parallelism: NonZeroUsize,
}

impl StripePlan {
    const fn new(stripe_size: NonZeroUsize, parallelism: NonZeroUsize) -> Self {
        Self {
            stripe_size,
            parallelism,
        }
    }

    const fn stripe_size(self) -> usize {
        self.stripe_size.get()
    }

    const fn parallelism(self) -> usize {
        self.parallelism.get()
    }
}

#[derive(Clone, Copy)]
struct Stripe {
    offset: usize,
    len: usize,
}

/// Number of shards in the Leopard high-rate work buffer for `(k, n)`.
fn work_shards(k: usize, n: usize) -> usize {
    let chunk = n.next_power_of_two();
    k.div_ceil(chunk) * chunk
}

/// Choose a block-aligned stripe width and bound the number encoded at once.
fn stripe_plan(
    k: usize,
    n: usize,
    row_size: usize,
    threads: usize,
    work_budget: NonZeroUsize,
) -> Result<StripePlan> {
    let work_shards = work_shards(k, n);
    let work_budget = work_budget.get();
    let max_parallelism = (work_budget / (work_shards * MIN_STRIPE)).max(1);
    let parallelism = threads.max(1).min(max_parallelism);
    let per_encoder = work_budget / parallelism / work_shards;
    let stripe = (per_encoder / MIN_STRIPE) * MIN_STRIPE;
    let stripe_size = NonZeroUsize::new(stripe.max(MIN_STRIPE).min(row_size)).ok_or_else(|| {
        Error::InvalidParameters("stripe planner produced a zero stripe size".into())
    })?;
    let parallelism = NonZeroUsize::new(parallelism).ok_or_else(|| {
        Error::InvalidParameters("stripe planner produced zero parallelism".into())
    })?;

    Ok(StripePlan::new(stripe_size, parallelism))
}

struct StripeEncoder {
    encoder: HighRateEncoder<DefaultEngine>,
    recovery: Vec<u8>,
}

impl StripeEncoder {
    fn new(k: usize, n: usize, stripe_size: usize) -> Result<Self> {
        let encoder = RateEncoder::new(k, n, stripe_size, DefaultEngine::new(), None)
            .map_err(|e| Error::ReedSolomon(e.to_string()))?;

        Ok(Self {
            encoder,
            recovery: Vec::with_capacity(n * stripe_size),
        })
    }

    fn encode_stripe(
        &mut self,
        original_rows: &[u8],
        k: usize,
        n: usize,
        row_size: usize,
        stripe: Stripe,
    ) -> Result<()> {
        self.encoder
            .reset(k, n, stripe.len)
            .map_err(|e| Error::ReedSolomon(e.to_string()))?;

        for row in original_rows.chunks_exact(row_size) {
            self.encoder
                .add_original_shard(&row[stripe.offset..stripe.offset + stripe.len])
                .map_err(|e| Error::ReedSolomon(e.to_string()))?;
        }

        let result = self
            .encoder
            .encode()
            .map_err(|e| Error::ReedSolomon(e.to_string()))?;

        self.recovery.clear();
        self.recovery.reserve(n * stripe.len);
        for recovery in result.recovery_iter() {
            self.recovery.extend_from_slice(recovery);
        }
        debug_assert_eq!(self.recovery.len(), n * stripe.len);

        Ok(())
    }
}

fn fill_parity(
    original_rows: &[u8],
    parity_rows: &mut [u8],
    k: usize,
    n: usize,
    row_size: usize,
) -> Result<()> {
    fill_parity_with_work_budget(
        original_rows,
        parity_rows,
        k,
        n,
        row_size,
        default_work_budget(),
    )
}

fn fill_parity_with_work_budget(
    original_rows: &[u8],
    parity_rows: &mut [u8],
    k: usize,
    n: usize,
    row_size: usize,
    work_budget: NonZeroUsize,
) -> Result<()> {
    if original_rows.len() != k * row_size {
        return Err(Error::InvalidParameters(format!(
            "expected {} bytes of original rows, got {}",
            k * row_size,
            original_rows.len()
        )));
    }
    if parity_rows.len() != n * row_size {
        return Err(Error::InvalidParameters(format!(
            "expected {} bytes of parity rows, got {}",
            n * row_size,
            parity_rows.len()
        )));
    }

    if k == 0 || n == 0 || row_size == 0 {
        return Err(Error::InvalidParameters(format!(
            "invalid shard extension parameters: k={}, n={}, row_size={}",
            k, n, row_size
        )));
    }
    if !row_size.is_multiple_of(MIN_STRIPE) {
        return Err(Error::InvalidParameters(format!(
            "row_size {} is not a multiple of {}",
            row_size, MIN_STRIPE
        )));
    }

    HighRateEncoder::<DefaultEngine>::validate(k, n, MIN_STRIPE)
        .map_err(|e| Error::ReedSolomon(e.to_string()))?;

    let plan = stripe_plan(k, n, row_size, rayon::current_num_threads(), work_budget)?;
    fill_parity_with_plan(original_rows, parity_rows, k, n, row_size, plan)
}

fn fill_parity_with_plan(
    original_rows: &[u8],
    parity_rows: &mut [u8],
    k: usize,
    n: usize,
    row_size: usize,
    plan: StripePlan,
) -> Result<()> {
    let stripe_size = plan.stripe_size();
    let parallelism = plan.parallelism();
    assert!(stripe_size.is_multiple_of(MIN_STRIPE));

    // A single stripe (always the case for the 64-byte RLC rows in
    // `extend_rlcs`) gains nothing from the rayon fan-out and staging copy
    // below; encode whole rows on the calling thread instead.
    if stripe_size >= row_size {
        let mut encoder: HighRateEncoder<DefaultEngine> =
            RateEncoder::new(k, n, row_size, DefaultEngine::new(), None)
                .map_err(|e| Error::ReedSolomon(e.to_string()))?;
        for row in original_rows.chunks_exact(row_size) {
            encoder
                .add_original_shard(row)
                .map_err(|e| Error::ReedSolomon(e.to_string()))?;
        }
        let result = encoder
            .encode()
            .map_err(|e| Error::ReedSolomon(e.to_string()))?;
        for (dst_row, src_row) in parity_rows
            .chunks_exact_mut(row_size)
            .zip(result.recovery_iter())
        {
            dst_row.copy_from_slice(src_row);
        }
        return Ok(());
    }

    let stripes: Vec<_> = (0..row_size)
        .step_by(stripe_size)
        .map(|offset| Stripe {
            offset,
            len: stripe_size.min(row_size - offset),
        })
        .collect();
    let slot_count = parallelism.min(stripes.len());
    let mut encoders = (0..slot_count)
        .into_par_iter()
        .map(|_| StripeEncoder::new(k, n, stripe_size))
        .collect::<Result<Vec<_>>>()?;

    // Leopard transforms each 64-byte block position independently. Encode a
    // cache-sized batch of column stripes, then scatter it by parity row so all
    // writes use ordinary disjoint mutable slices.
    for batch in stripes.chunks(parallelism) {
        encoders[..batch.len()]
            .par_iter_mut()
            .zip(batch.par_iter())
            .try_for_each(|(encoder, &stripe)| {
                encoder.encode_stripe(original_rows, k, n, row_size, stripe)
            })?;

        let recovery: Vec<_> = encoders[..batch.len()]
            .iter()
            .map(|encoder| encoder.recovery.as_slice())
            .collect();
        parity_rows
            .par_chunks_mut(row_size)
            .enumerate()
            .for_each(|(parity_index, row)| {
                for (&stripe, recovery) in batch.iter().zip(&recovery) {
                    let recovery_start = parity_index * stripe.len;
                    row[stripe.offset..stripe.offset + stripe.len]
                        .copy_from_slice(&recovery[recovery_start..recovery_start + stripe.len]);
                }
            });
    }

    Ok(())
}

/// Extend data using Reed-Solomon encoding.
pub fn extend_data(original_rows: OriginalRowsView<'_>, params: &Parameters) -> Result<RowMatrix> {
    extend_data_with_work_budget(original_rows, params, default_work_budget())
}

/// Extend data with an explicit combined Leopard work-buffer budget.
pub fn extend_data_with_work_budget(
    original_rows: OriginalRowsView<'_>,
    params: &Parameters,
    work_budget: NonZeroUsize,
) -> Result<RowMatrix> {
    let mut all_rows = RowMatrix::zeroed(params.total_rows(), params.row_size)?;
    let split_at = params.k * params.row_size;
    let (orig, parity) = all_rows.as_row_major_mut().split_at_mut(split_at);
    orig.copy_from_slice(original_rows.as_row_major());
    fill_parity_with_work_budget(
        orig,
        parity,
        params.k,
        params.n,
        params.row_size,
        work_budget,
    )?;
    Ok(all_rows)
}

/// Encode parity rows in place into an already allocated extended matrix.
///
/// The first K rows must already contain original data.
pub fn encode_parity_in_place(extended_rows: &mut RowMatrix, params: &Parameters) -> Result<()> {
    encode_parity_in_place_with_work_budget(extended_rows, params, default_work_budget())
}

/// Encode parity rows in place with an explicit Leopard work-buffer budget.
pub fn encode_parity_in_place_with_work_budget(
    extended_rows: &mut RowMatrix,
    params: &Parameters,
    work_budget: NonZeroUsize,
) -> Result<()> {
    let mut view = extended_rows.extended_view_mut(params)?;
    let (orig, parity) = view.split_original_parity();
    fill_parity_with_work_budget(
        orig,
        parity,
        params.k,
        params.n,
        params.row_size,
        work_budget,
    )
}

/// Pack GF128 value into a 64-byte Leopard shard.
pub fn pack_gf128_to_shard(gf128: &GF128) -> Vec<u8> {
    let mut shard = vec![0u8; 64];
    for i in 0..8 {
        let bytes = gf128.limbs[i].to_le_bytes();
        shard[i] = bytes[0];
        shard[32 + i] = bytes[1];
    }
    shard
}

/// Unpack GF128 value from a 64-byte Leopard shard.
pub fn unpack_shard_to_gf128(shard: &[u8]) -> GF128 {
    let mut limbs = [0u16; 8];
    for i in 0..8 {
        limbs[i] = u16::from_le_bytes([shard[i], shard[32 + i]]);
    }
    GF128 { limbs }
}

/// Extend K RLC values to K+N using the same RS encoder path as row extension.
pub fn extend_rlcs(rlc_orig: &[GF128], k: usize, n: usize) -> Result<Vec<GF128>> {
    if rlc_orig.len() != k {
        return Err(Error::InvalidParameters(format!(
            "expected {} RLC values, got {}",
            k,
            rlc_orig.len()
        )));
    }

    let mut shards = vec![0u8; k * 64];
    let (dst_shards, _) = shards.as_chunks_mut::<64>();
    for (dst_shard, rlc) in dst_shards.iter_mut().zip(rlc_orig.iter()) {
        let packed = pack_gf128_to_shard(rlc);
        dst_shard.copy_from_slice(&packed);
    }

    let mut extended_shards = vec![0u8; (k + n) * 64];
    let split_at = k * 64;
    let (orig, parity) = extended_shards.split_at_mut(split_at);
    orig.copy_from_slice(&shards);
    fill_parity(orig, parity, k, n, 64)?;
    let (extended, _) = extended_shards.as_chunks::<64>();
    Ok(extended
        .iter()
        .map(|shard| unpack_shard_to_gf128(shard))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_stripe_plan(stripe_size: usize, parallelism: usize) -> StripePlan {
        StripePlan::new(
            NonZeroUsize::new(stripe_size).expect("test stripe size must be non-zero"),
            NonZeroUsize::new(parallelism).expect("test parallelism must be non-zero"),
        )
    }

    #[test]
    fn test_extend_data() {
        let k = 4;
        let n = 4;
        let row_size = 64;
        let params = Parameters::new(k, n, row_size).unwrap();

        let mut original = vec![0u8; k * row_size];
        for i in 0..k {
            original[i * row_size] = i as u8;
        }
        let original = RowMatrix::with_shape(original, k, row_size).unwrap();
        let original_view = original.original_view(&params).unwrap();

        let extended = extend_data(original_view, &params).unwrap();

        assert_eq!(extended.as_row_major().len(), (k + n) * row_size);

        // Original rows should be unchanged
        for i in 0..k {
            assert_eq!(extended.row(i).unwrap(), original.row(i).unwrap());
        }

        // Parity rows should be different
        assert_ne!(extended.row(k).unwrap(), original.row(0).unwrap());
    }

    #[test]
    fn test_encode_parity_in_place_matches_extend_data() {
        let k = 4;
        let n = 4;
        let row_size = 64;
        let params = Parameters::new(k, n, row_size).unwrap();

        let mut original = vec![0u8; k * row_size];
        for i in 0..k {
            original[i * row_size] = (i as u8).wrapping_mul(17).wrapping_add(3);
        }
        let original = RowMatrix::with_shape(original, k, row_size).unwrap();
        let extended_via_extend_data =
            extend_data(original.original_view(&params).unwrap(), &params).unwrap();

        let mut in_place = vec![0u8; (k + n) * row_size];
        in_place[..k * row_size].copy_from_slice(original.as_row_major());
        let mut extended_in_place = RowMatrix::with_shape(in_place, k + n, row_size).unwrap();
        encode_parity_in_place(&mut extended_in_place, &params).unwrap();

        assert_eq!(
            extended_in_place.as_row_major(),
            extended_via_extend_data.as_row_major()
        );
    }

    #[test]
    fn test_gf128_packing() {
        let gf = GF128 {
            limbs: [1, 2, 3, 4, 5, 6, 7, 8],
        };
        let shard = pack_gf128_to_shard(&gf);
        let unpacked = unpack_shard_to_gf128(&shard);
        assert_eq!(gf, unpacked);
    }

    #[test]
    fn test_extend_rlcs() {
        let k = 4;
        let n = 4;

        let rlc_orig: Vec<GF128> = (0..k)
            .map(|i| {
                let mut limbs = [0u16; 8];
                limbs[0] = ((i + 1) * 100) as u16;
                limbs[1] = ((i + 1) * 200) as u16;
                GF128 { limbs }
            })
            .collect();

        let extended = extend_rlcs(&rlc_orig, k, n).unwrap();

        assert_eq!(extended.len(), k + n);
        for i in 0..k {
            assert_eq!(extended[i], rlc_orig[i]);
        }
        assert!(extended[k..(k + n)].iter().any(|rlc| *rlc != GF128::zero()));
    }

    #[test]
    fn stripe_plan_respects_work_budget() {
        for (k, n, row_size, threads, budget_mib, expected_stripe, expected_parallelism) in [
            (4096, 12288, 32768, 32, 32, 64, 32),
            (4096, 12288, 32768, 1, 1, 64, 1),
            (4096, 12288, 32768, 4, 4, 64, 4),
            (4096, 12288, 32768, 1, 32, 2048, 1),
            (32768, 32768, 32768, 32, 32, 64, 16),
        ] {
            let work_budget = NonZeroUsize::new(budget_mib << 20).unwrap();
            let plan = stripe_plan(k, n, row_size, threads, work_budget).unwrap();

            assert_eq!(plan.stripe_size(), expected_stripe);
            assert_eq!(plan.parallelism(), expected_parallelism);
            assert!(
                plan.parallelism() * work_shards(k, n) * plan.stripe_size() <= work_budget.get()
            );
        }

        assert!(stripe_plan(4, 4, 0, 1, NonZeroUsize::MIN).is_err());
    }

    /// Striped, parallel parity must be byte-identical to a single Leopard
    /// encoder over whole rows (what validators recompute on the Go side).
    #[test]
    fn striped_parity_matches_single_encoder() {
        use rand::{RngCore, SeedableRng};
        use rand_chacha::ChaCha8Rng;

        // Plans are explicit so coverage does not depend on the CI machine's
        // rayon pool. The second case also reuses a slot for a shorter stripe;
        // the last two take the single-stripe path.
        for (k, n, row_size, seed, plan) in [
            (
                4096usize,
                12288usize,
                512usize,
                1u64,
                test_stripe_plan(128, 2),
            ),
            (4096, 12288, 64 * 21, 2, test_stripe_plan(512, 2)),
            (16, 48, 4096, 3, test_stripe_plan(1024, 4)),
            (16, 48, 256, 5, test_stripe_plan(256, 4)),
            (4, 4, 64, 4, test_stripe_plan(64, 1)),
        ] {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let mut original = vec![0u8; k * row_size];
            rng.fill_bytes(&mut original);

            let mut expected = vec![0u8; n * row_size];
            let mut encoder: HighRateEncoder<DefaultEngine> =
                RateEncoder::new(k, n, row_size, DefaultEngine::new(), None).unwrap();
            for row in original.chunks_exact(row_size) {
                encoder.add_original_shard(row).unwrap();
            }
            let result = encoder.encode().unwrap();
            for (dst, src) in expected
                .chunks_exact_mut(row_size)
                .zip(result.recovery_iter())
            {
                dst.copy_from_slice(src);
            }

            let mut parity = vec![0u8; n * row_size];
            fill_parity_with_plan(&original, &mut parity, k, n, row_size, plan).unwrap();
            assert!(
                parity == expected,
                "striped parity differs for k={k} n={n} row_size={row_size}"
            );
        }
    }
}
