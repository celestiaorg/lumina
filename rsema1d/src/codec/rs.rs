use crate::codec::rows::{OriginalRowsView, RowMatrix};
use crate::error::{Error, Result};
use crate::field::GF128;
use crate::params::Parameters;
use reed_solomon_simd::engine::DefaultEngine;
use reed_solomon_simd::rate::{HighRateEncoder, RateEncoder};

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

    let engine = DefaultEngine::new();
    let mut encoder: HighRateEncoder<DefaultEngine> =
        RateEncoder::new(k, n, row_size, engine, None)
            .map_err(|e| Error::ReedSolomon(e.to_string()))?;

    // Add all original rows
    for row in original_rows.chunks_exact(row_size) {
        encoder
            .add_original_shard(row)
            .map_err(|e| Error::ReedSolomon(e.to_string()))?;
    }

    // Generate parity rows
    let result = encoder
        .encode()
        .map_err(|e| Error::ReedSolomon(e.to_string()))?;

    for (dst_row, src_row) in parity_rows
        .chunks_exact_mut(row_size)
        .zip(result.recovery_iter())
    {
        dst_row.copy_from_slice(src_row);
    }

    Ok(())
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
    for (dst_shard, rlc) in shards.chunks_exact_mut(64).zip(rlc_orig.iter()) {
        let packed = pack_gf128_to_shard(rlc);
        dst_shard.copy_from_slice(&packed);
    }

    let mut extended_shards = vec![0u8; (k + n) * 64];
    let split_at = k * 64;
    let (orig, parity) = extended_shards.split_at_mut(split_at);
    orig.copy_from_slice(&shards);
    fill_parity(orig, parity, k, n, 64)?;
    Ok(extended_shards
        .chunks_exact(64)
        .map(unpack_shard_to_gf128)
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
}
