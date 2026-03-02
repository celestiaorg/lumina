use crate::codec::rows::{ExtendedRows, OriginalRows};
use crate::error::{Error, Result};
use crate::field::GF128;
use crate::params::Parameters;
use reed_solomon_simd::engine::DefaultEngine;
use reed_solomon_simd::rate::{HighRateEncoder, RateEncoder};

fn extend_shards(original_rows: &[u8], k: usize, n: usize, row_size: usize) -> Result<Vec<u8>> {
    if original_rows.len() != k * row_size {
        return Err(Error::InvalidParameters(format!(
            "expected {} bytes of original rows, got {}",
            k * row_size,
            original_rows.len()
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
    for i in 0..k {
        let start = i * row_size;
        let end = start + row_size;
        let row = &original_rows[start..end];
        encoder
            .add_original_shard(row)
            .map_err(|e| Error::ReedSolomon(e.to_string()))?;
    }

    // Generate parity rows
    let result = encoder
        .encode()
        .map_err(|e| Error::ReedSolomon(e.to_string()))?;

    let mut all_rows = vec![0u8; (k + n) * row_size];
    all_rows[..(k * row_size)].copy_from_slice(original_rows);
    for (i, slice) in result.recovery_iter().enumerate() {
        let start = (k + i) * row_size;
        let end = start + row_size;
        all_rows[start..end].copy_from_slice(slice);
    }

    Ok(all_rows)
}

/// Extend data using Reed-Solomon encoding.
pub fn extend_data(original_rows: &OriginalRows, params: &Parameters) -> Result<ExtendedRows> {
    if original_rows.rows() != params.k || original_rows.row_size() != params.row_size {
        return Err(Error::InvalidParameters(format!(
            "original rows shape mismatch: expected {}x{}, got {}x{}",
            params.k,
            params.row_size,
            original_rows.rows(),
            original_rows.row_size()
        )));
    }

    let data = extend_shards(
        original_rows.as_bytes(),
        params.k,
        params.n,
        params.row_size,
    )?;
    ExtendedRows::new(data, params)
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
    for (i, rlc) in rlc_orig.iter().enumerate() {
        let packed = pack_gf128_to_shard(rlc);
        let start = i * 64;
        let end = start + 64;
        shards[start..end].copy_from_slice(&packed);
    }

    let extended_shards = extend_shards(&shards, k, n, 64)?;
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
        let original = OriginalRows::new(original, &params).unwrap();

        let extended = extend_data(&original, &params).unwrap();

        assert_eq!(extended.as_bytes().len(), (k + n) * row_size);

        // Original rows should be unchanged
        for i in 0..k {
            let start = i * row_size;
            let end = start + row_size;
            assert_eq!(
                &extended.as_bytes()[start..end],
                &original.as_bytes()[start..end]
            );
        }

        // Parity rows should be different
        assert_ne!(
            &extended.as_bytes()[(k * row_size)..((k + 1) * row_size)],
            &original.as_bytes()[0..row_size]
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
