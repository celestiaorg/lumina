use crate::codec::rows::{OriginalRowsView, RowMatrix};
use crate::error::{Error, Result};
use crate::field::GF128;
use crate::params::Parameters;
use rayon::prelude::*;
use reed_solomon_simd::engine::DefaultEngine;
use reed_solomon_simd::rate::{HighRateEncoder, RateEncoder};
use std::cell::RefCell;

/// Upper bound on the combined Leopard work set of all stripe encoders that
/// run concurrently, in bytes.
///
/// Leopard's high-rate transform keeps [`work_shards`] shards in a work buffer
/// and sweeps it several times, so throughput is set by whether that buffer
/// sits in cache or in DRAM. Encoding every row as a single shard (32 KiB rows
/// at K=4096/N=12288) makes the buffer 512 MiB and the whole encode DRAM-bound
/// on one core. Splitting rows into column stripes gives independent encoders
/// whose buffers are `work_shards * stripe` bytes each; keeping all
/// concurrently running buffers within this budget keeps the transforms
/// cache-resident.
///
/// This knob only affects speed, never output: every stripe width that is a
/// multiple of [`MIN_STRIPE`] yields byte-identical parity. 32 MiB measured
/// best on a 32-thread Ryzen 9 3950X. The budget is exceeded when `MIN_STRIPE`
/// clamps the width (large K on many threads).
const STRIPE_WORK_BUDGET: usize = 32 << 20;

/// Smallest stripe worth encoding separately: one 64-byte Leopard block per
/// shard. Stripe widths and row sizes must be multiples of this so that every
/// stripe consists of whole Leopard blocks.
const MIN_STRIPE: usize = 64;

thread_local! {
    /// One encoder per rayon worker, reused across calls so the Leopard work
    /// buffer is allocated and page-faulted once per thread, not once per blob.
    /// Each worker keeps its buffer (`work_shards * stripe` bytes, ~1-2 MiB at
    /// the production shape) for the lifetime of the thread.
    static ENCODER: RefCell<Option<HighRateEncoder<DefaultEngine>>> = const { RefCell::new(None) };
}

/// Raw pointer to the parity region, shared across rayon tasks.
///
/// Each stripe task writes only the byte ranges
/// `[row * row_size + offset, row * row_size + offset + len)` of its own
/// stripe. Stripes are disjoint within every row, so concurrent writes never
/// alias, and the `&mut [u8]` the pointer was taken from is not used again
/// until the parallel section has joined all tasks.
#[derive(Clone, Copy)]
struct ParityOut(*mut u8);

// SAFETY: see the type documentation.
unsafe impl Send for ParityOut {}
unsafe impl Sync for ParityOut {}

impl ParityOut {
    /// Copy `src` to bytes `[at, at + src.len())` of the parity region.
    ///
    /// # Safety
    ///
    /// The range must lie inside the region and no other task may write it
    /// concurrently.
    unsafe fn write(self, at: usize, src: &[u8]) {
        // SAFETY: guaranteed by the caller.
        unsafe { std::ptr::copy_nonoverlapping(src.as_ptr(), self.0.add(at), src.len()) }
    }
}

/// Page size assumed when faulting in freshly allocated parity memory.
const TOUCH_PAGE: usize = 4096;
/// Contiguous span each rayon worker faults in at a time.
const TOUCH_CHUNK: usize = 2 << 20;

/// Fault in `buf` one page at a time, contiguous chunks per worker.
///
/// The striped scatter has every worker writing small pieces all over the
/// parity region at once. Concurrent first-touch of interleaved pages of one
/// fresh mapping serialises in the kernel, so the pages are faulted in
/// sequentially first; on already-resident memory this costs one cached store
/// per page.
fn touch_pages(buf: &mut [u8]) {
    buf.par_chunks_mut(TOUCH_CHUNK).for_each(|chunk| {
        for page in chunk.chunks_mut(TOUCH_PAGE) {
            // SAFETY: `page` is a non-empty, valid, writable slice.
            unsafe { std::ptr::write_volatile(page.as_mut_ptr(), page[0]) };
        }
    });
}

/// Number of shards in the Leopard high-rate work buffer for `(k, n)`.
///
/// Mirrors `HighRateEncoder::work_count` in `reed-solomon-simd`. If the two
/// drift apart only the chosen stripe width changes, not the output.
fn work_shards(k: usize, n: usize) -> usize {
    let chunk = n.next_power_of_two();
    k.div_ceil(chunk) * chunk
}

/// Stripe width in bytes for encoding `row_size`-byte rows with `(k, n)`.
///
/// Returns the largest multiple of [`MIN_STRIPE`] such that all rayon workers'
/// work buffers fit in [`STRIPE_WORK_BUDGET`], clamped to
/// `[MIN_STRIPE, row_size]`.
fn stripe_size(k: usize, n: usize, row_size: usize) -> usize {
    let threads = rayon::current_num_threads();
    let per_encoder = STRIPE_WORK_BUDGET / (threads * work_shards(k, n));
    let stripe = (per_encoder / MIN_STRIPE) * MIN_STRIPE;
    stripe.clamp(MIN_STRIPE, row_size)
}

fn fill_parity(
    original_rows: &[u8],
    parity_rows: &mut [u8],
    k: usize,
    n: usize,
    row_size: usize,
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

    let stripe = stripe_size(k, n, row_size);
    fill_parity_striped(original_rows, parity_rows, k, n, row_size, stripe)
}

/// Encode the parity rows in parallel, one column stripe of `stripe` bytes per
/// rayon task.
///
/// Leopard applies the same transform independently to every 64-byte block
/// position of a shard, so encoding column stripes of the rows separately
/// yields exactly the parity that one encoder over whole rows would, byte for
/// byte. Every stripe but the last is `stripe` bytes wide; the last one may be
/// shorter.
///
/// The caller has validated `k`, `n`, `row_size` and the buffer lengths.
/// `stripe` and `row_size` must be multiples of [`MIN_STRIPE`].
fn fill_parity_striped(
    original_rows: &[u8],
    parity_rows: &mut [u8],
    k: usize,
    n: usize,
    row_size: usize,
    stripe: usize,
) -> Result<()> {
    assert!(stripe > 0 && stripe.is_multiple_of(MIN_STRIPE));
    assert!(row_size.is_multiple_of(MIN_STRIPE));
    assert_eq!(original_rows.len(), k * row_size);
    assert_eq!(parity_rows.len(), n * row_size);

    touch_pages(parity_rows);
    let parity = ParityOut(parity_rows.as_mut_ptr());

    // Encode `original_rows[.., offset..offset + len]` into the same columns of
    // the parity rows, using this thread's cached encoder.
    let encode_stripe = |offset: usize, len: usize| -> Result<()> {
        ENCODER.with(|cell| {
            // No rayon call may happen while this borrow is held: rayon can run
            // another task on this worker at any rayon call point, and that task
            // would hit a re-entrant `borrow_mut`.
            let mut slot = cell.borrow_mut();
            let encoder = match slot.as_mut() {
                Some(encoder) => {
                    encoder
                        .reset(k, n, len)
                        .map_err(|e| Error::ReedSolomon(e.to_string()))?;
                    encoder
                }
                None => slot.insert(
                    RateEncoder::new(k, n, len, DefaultEngine::new(), None)
                        .map_err(|e| Error::ReedSolomon(e.to_string()))?,
                ),
            };

            for row in original_rows.chunks_exact(row_size) {
                encoder
                    .add_original_shard(&row[offset..offset + len])
                    .map_err(|e| Error::ReedSolomon(e.to_string()))?;
            }

            let result = encoder
                .encode()
                .map_err(|e| Error::ReedSolomon(e.to_string()))?;

            let mut recovery = result.recovery_iter();
            for i in 0..n {
                let shard = recovery
                    .next()
                    .expect("Leopard returned fewer recovery shards than requested");
                assert_eq!(shard.len(), len, "Leopard recovery shard has wrong length");
                // SAFETY: `parity` covers `n * row_size` bytes; the destination
                // `[i * row_size + offset, .. + len)` is in bounds because `i < n`
                // and `offset + len <= row_size`, and no other task writes it
                // (see `ParityOut`).
                unsafe { parity.write(i * row_size + offset, shard) };
            }
            assert!(
                recovery.next().is_none(),
                "Leopard returned more recovery shards than requested"
            );
            Ok(())
        })
    };

    (0..row_size.div_ceil(stripe))
        .into_par_iter()
        .try_for_each(|s| {
            let offset = s * stripe;
            encode_stripe(offset, stripe.min(row_size - offset))
        })
}

/// Extend data using Reed-Solomon encoding.
pub fn extend_data(original_rows: OriginalRowsView<'_>, params: &Parameters) -> Result<RowMatrix> {
    let mut all_rows = vec![0u8; (params.k + params.n) * params.row_size];
    let split_at = params.k * params.row_size;
    let (orig, parity) = all_rows.split_at_mut(split_at);
    orig.copy_from_slice(original_rows.as_row_major());
    fill_parity(orig, parity, params.k, params.n, params.row_size)?;
    RowMatrix::with_shape(all_rows, params.total_rows(), params.row_size)
}

/// Encode parity rows in place into an already allocated extended matrix.
///
/// The first K rows must already contain original data.
pub fn encode_parity_in_place(extended_rows: &mut RowMatrix, params: &Parameters) -> Result<()> {
    let mut view = extended_rows.extended_view_mut(params)?;
    let (orig, parity) = view.split_original_parity();
    fill_parity(orig, parity, params.k, params.n, params.row_size)
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

    fn random_rows(k: usize, row_size: usize, seed: u64) -> Vec<u8> {
        use rand::{RngCore, SeedableRng};
        let mut rows = vec![0u8; k * row_size];
        rand_chacha::ChaCha8Rng::seed_from_u64(seed).fill_bytes(&mut rows);
        rows
    }

    /// Oracle: one Leopard encoder over whole rows, which is what the Go
    /// verifier recomputes.
    fn whole_row_parity(original: &[u8], k: usize, n: usize, row_size: usize) -> Vec<u8> {
        let mut encoder: HighRateEncoder<DefaultEngine> =
            RateEncoder::new(k, n, row_size, DefaultEngine::new(), None).unwrap();
        for row in original.chunks_exact(row_size) {
            encoder.add_original_shard(row).unwrap();
        }
        let result = encoder.encode().unwrap();
        let mut expected = vec![0u8; n * row_size];
        for (dst, src) in expected
            .chunks_exact_mut(row_size)
            .zip(result.recovery_iter())
        {
            dst.copy_from_slice(src);
        }
        expected
    }

    fn assert_striped_matches(
        original: &[u8],
        expected: &[u8],
        k: usize,
        n: usize,
        row_size: usize,
        stripe: usize,
    ) {
        let mut parity = vec![0u8; n * row_size];
        fill_parity_striped(original, &mut parity, k, n, row_size, stripe).unwrap();
        assert!(
            parity == expected,
            "striped parity differs for k={k} n={n} row_size={row_size} stripe={stripe}"
        );
    }

    /// Striped, parallel parity must be byte-identical to a single Leopard
    /// encoder over whole rows, for every stripe width. Stripe widths are
    /// explicit so coverage does not depend on the host's thread count.
    #[test]
    #[cfg_attr(
        miri,
        ignore = "K=4096 is too slow under Miri; see striped_parity_small_shapes"
    )]
    fn striped_parity_matches_single_encoder() {
        // Production shape. 1344 = 64 * 21: stripe 64 gives 21 full stripes,
        // 128/256/512 leave a shorter last stripe, 1344 is a single stripe.
        let (k, n, row_size) = (4096, 12288, 64 * 21);
        let original = random_rows(k, row_size, 1);
        let expected = whole_row_parity(&original, k, n, row_size);
        for stripe in [64, 128, 256, 512, row_size] {
            assert_striped_matches(&original, &expected, k, n, row_size, stripe);
        }

        // Production entry point, with whatever width `stripe_size` picks here.
        let mut parity = vec![0u8; n * row_size];
        fill_parity(&original, &mut parity, k, n, row_size).unwrap();
        assert!(
            parity == expected,
            "fill_parity differs from single encoder"
        );
    }

    /// Small shapes, including a short last stripe and an odd `k`, that also
    /// run under Miri to check the unsafe scatter for aliasing and data races:
    ///
    /// ```text
    /// MIRIFLAGS="-Zmiri-disable-isolation -Zmiri-tree-borrows -Zmiri-ignore-leaks" \
    ///     RAYON_NUM_THREADS=4 cargo +nightly miri test -p rsema1d --lib -- striped
    /// ```
    ///
    /// Tree Borrows is required because `crossbeam-epoch` (rayon's deque) trips
    /// Stacked Borrows; `ignore-leaks` because rayon's global pool threads are
    /// never joined. Takes about ten minutes.
    #[test]
    fn striped_parity_small_shapes() {
        for (k, n, row_size, seed) in [
            (4usize, 4usize, 256usize, 2u64),
            (3, 5, 320, 3),
            (1, 1, 64, 4),
        ] {
            let original = random_rows(k, row_size, seed);
            let expected = whole_row_parity(&original, k, n, row_size);
            for stripe in [64, 128, 192, row_size] {
                assert_striped_matches(&original, &expected, k, n, row_size, stripe);
            }
            let mut parity = vec![0u8; n * row_size];
            fill_parity(&original, &mut parity, k, n, row_size).unwrap();
            assert!(
                parity == expected,
                "fill_parity differs for k={k} n={n} row_size={row_size}"
            );
        }
    }
}
