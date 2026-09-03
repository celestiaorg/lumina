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
/// Leopard's high-rate transform keeps `k.next_multiple_of(n.next_power_of_two())`
/// shards in a work buffer and sweeps it several times, so throughput is set by
/// whether that buffer sits in cache or in DRAM. Encoding every row as a single
/// shard (32 KiB rows at K=4096/N=12288) makes the buffer 512 MiB and the whole
/// encode DRAM-bound on one core. Splitting rows into column stripes gives
/// `row_size / stripe` independent encoders whose buffers are
/// `work_shards * stripe` bytes each; keeping all concurrently running buffers
/// within this budget keeps the transforms cache-resident. 32 MiB is half the
/// L3 of a 16-core Zen 2 part and was the best value measured there (16 threads
/// x 2 MiB or 32 threads x 1 MiB gave the same ~58 ms per 128 MiB blob).
const STRIPE_WORK_BUDGET: usize = 32 << 20;

/// Smallest stripe worth encoding separately. Below one 64-byte Leopard block
/// per shard the per-shard overhead dominates.
const MIN_STRIPE: usize = 64;

thread_local! {
    /// One encoder per rayon worker, reused across calls so the Leopard work
    /// buffer is allocated and page-faulted once per thread, not once per blob.
    static ENCODER: RefCell<Option<HighRateEncoder<DefaultEngine>>> = const { RefCell::new(None) };
}

/// Raw pointer to the parity region that can be shared across rayon tasks.
///
/// Each stripe task writes only the byte ranges
/// `[row * row_size + offset, row * row_size + offset + len)` of its own stripe,
/// which are disjoint between tasks, so concurrent writes never alias.
#[derive(Clone, Copy)]
struct ParityOut(*mut u8);

// SAFETY: see the type documentation; the pointer is only dereferenced for
// disjoint byte ranges, and the underlying `&mut [u8]` outlives the parallel
// section that uses it.
unsafe impl Send for ParityOut {}
unsafe impl Sync for ParityOut {}

/// Page size assumed when faulting in freshly allocated parity memory.
const TOUCH_PAGE: usize = 4096;
/// Contiguous span each rayon worker faults in at a time.
const TOUCH_CHUNK: usize = 2 << 20;

/// Write one byte per page of `buf`, contiguous chunks per worker.
///
/// The striped scatter below has every worker writing 64-byte pieces all
/// over the parity region at once. When that region is freshly allocated,
/// the first touch of each page is a page fault, and many threads faulting
/// interleaved pages of one mapping serialize in the kernel: measured 105 ms
/// versus 6 ms for a 24 MiB parity region with 32 threads. Faulting the pages
/// in sequentially first takes the fast path; on already-resident memory this
/// pass costs one cached store per page.
fn touch_pages(buf: &mut [u8]) {
    buf.par_chunks_mut(TOUCH_CHUNK).for_each(|chunk| {
        for page in chunk.chunks_mut(TOUCH_PAGE) {
            // SAFETY: `page` is a non-empty, valid, writable slice.
            unsafe { std::ptr::write_volatile(page.as_mut_ptr(), page[0]) };
        }
    });
}

/// Number of shards in the Leopard high-rate work buffer for `(k, n)`.
fn work_shards(k: usize, n: usize) -> usize {
    let chunk = n.next_power_of_two();
    k.div_ceil(chunk) * chunk
}

/// Stripe width in bytes for encoding `row_size`-byte rows with `(k, n)`.
///
/// Returns the largest multiple of 64 such that all rayon workers' work
/// buffers fit in [`STRIPE_WORK_BUDGET`], clamped to `[MIN_STRIPE, row_size]`.
fn stripe_size(k: usize, n: usize, row_size: usize) -> usize {
    let threads = 4;
    let per_encoder = STRIPE_WORK_BUDGET / (threads * work_shards(k, n));
    let stripe = (per_encoder / MIN_STRIPE) * MIN_STRIPE;
    stripe.clamp(MIN_STRIPE, row_size)
}

/// Encode one column stripe of `original_rows` into the matching stripe of
/// the parity rows, using this thread's cached encoder.
fn encode_stripe(
    original_rows: &[u8],
    parity: ParityOut,
    k: usize,
    n: usize,
    row_size: usize,
    offset: usize,
    len: usize,
) -> Result<()> {
    ENCODER.with(|cell| {
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

        for (i, recovery) in result.recovery_iter().enumerate() {
            debug_assert_eq!(recovery.len(), len);
            // SAFETY: `parity` points to `n * row_size` writable bytes that no
            // other task touches in this stripe's byte ranges (see `ParityOut`),
            // and `i < n`, `offset + len <= row_size`.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    recovery.as_ptr(),
                    parity.0.add(i * row_size + offset),
                    len,
                );
            }
        }
        Ok(())
    })
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

    // Leopard applies the same transform independently to every 64-byte
    // block position of a shard, so encoding column stripes of the rows
    // separately yields exactly the parity that one encoder over whole rows
    // would, byte for byte. The last stripe may be shorter.
    let stripe = stripe_size(k, n, row_size);
    touch_pages(parity_rows);
    let parity = ParityOut(parity_rows.as_mut_ptr());

    (0..row_size.div_ceil(stripe))
        .into_par_iter()
        .try_for_each(|s| {
            let offset = s * stripe;
            let len = stripe.min(row_size - offset);
            encode_stripe(original_rows, parity, k, n, row_size, offset, len)
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

    /// Striped, parallel parity must be byte-identical to a single Leopard
    /// encoder over whole rows (what validators recompute on the Go side).
    #[test]
    fn striped_parity_matches_single_encoder() {
        use rand::{RngCore, SeedableRng};
        use rand_chacha::ChaCha8Rng;

        // Production shape (K=4096, N=12288) with a row size that splits into
        // several stripes under any thread count, plus a row size that is not
        // a multiple of the stripe (last stripe shorter) and a tiny case.
        for (k, n, row_size, seed) in [
            (4096usize, 12288usize, 512usize, 1u64),
            (4096, 12288, 64 * 21, 2),
            (16, 48, 4096, 3),
            (4, 4, 64, 4),
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
            fill_parity(&original, &mut parity, k, n, row_size).unwrap();
            assert!(
                parity == expected,
                "striped parity differs for k={k} n={n} row_size={row_size}"
            );
        }
    }
}
