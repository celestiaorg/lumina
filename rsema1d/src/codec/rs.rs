use crate::error::{Error, Result};
use crate::field::GF128;
use reed_solomon_simd::engine::DefaultEngine;
use reed_solomon_simd::rate::{HighRateEncoder, RateEncoder};

fn extend_shards(
    original_rows: &[Vec<u8>],
    k: usize,
    n: usize,
    row_size: usize,
) -> Result<Vec<Vec<u8>>> {
    // Validate all rows have correct size
    for row in original_rows.iter() {
        if row.len() != row_size {
            return Err(Error::RowLengthMismatch {
                expected: row_size,
                actual: row.len(),
            });
        }
    }

    if original_rows.len() != k {
        return Err(Error::InvalidParameters(format!(
            "expected {} original rows, got {}",
            k,
            original_rows.len()
        )));
    }

    let engine = DefaultEngine::new();
    let mut encoder: HighRateEncoder<DefaultEngine> =
        RateEncoder::new(k, n, row_size, engine, None)
            .map_err(|e| Error::ReedSolomon(e.to_string()))?;

    // Add all original rows
    for row in original_rows {
        encoder
            .add_original_shard(row)
            .map_err(|e| Error::ReedSolomon(e.to_string()))?;
    }

    // Generate parity rows
    let result = encoder
        .encode()
        .map_err(|e| Error::ReedSolomon(e.to_string()))?;

    let mut all_rows = Vec::with_capacity(k + n);
    original_rows.iter().for_each(|el| all_rows.push(el.clone()));
    result.recovery_iter().map(|slice| slice.to_vec()).for_each(|el| all_rows.push(el));
    
    Ok(all_rows)
}

/// Extend data using Reed-Solomon encoding.
pub fn extend_data(
    original_rows: &[Vec<u8>],
    k: usize,
    n: usize,
    row_size: usize,
) -> Result<Vec<Vec<u8>>> {
    extend_shards(original_rows, k, n, row_size)
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

    let shards: Vec<Vec<u8>> = rlc_orig.iter().map(pack_gf128_to_shard).collect();
    let extended_shards = extend_shards(&shards, k, n, 64)?;

    Ok(extended_shards
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

        let original: Vec<Vec<u8>> = (0..k)
            .map(|i| {
                let mut row = vec![0u8; row_size];
                row[0] = i as u8;
                row
            })
            .collect();

        let extended = extend_data(&original, k, n, row_size).unwrap();

        assert_eq!(extended.len(), k + n);

        // Original rows should be unchanged
        for i in 0..k {
            assert_eq!(extended[i], original[i]);
        }

        // Parity rows should be different
        assert_ne!(extended[k], original[0]);
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
