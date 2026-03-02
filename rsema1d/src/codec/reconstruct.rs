use crate::codec::rows::{OriginalRows, SampledRows};
use crate::error::{Error, Result};
use crate::params::Parameters;
use reed_solomon_simd::engine::DefaultEngine;
use reed_solomon_simd::rate::{HighRateDecoder, RateDecoder};

/// Reconstruct original data from any K rows
pub fn reconstruct_data(
    rows: &SampledRows,
    indices: &[usize],
    params: &Parameters,
) -> Result<OriginalRows> {
    if rows.row_size() != params.row_size {
        return Err(Error::InvalidParameters(format!(
            "row size mismatch: expected {}, got {}",
            params.row_size,
            rows.row_size()
        )));
    }

    if rows.rows() != indices.len() {
        return Err(Error::InvalidParameters(format!(
            "rows count mismatch: expected {}, got {}",
            indices.len(),
            rows.rows(),
        )));
    }

    if indices.len() < params.k {
        return Err(Error::InvalidParameters(format!(
            "need at least {} rows, got {}",
            params.k,
            indices.len()
        )));
    }

    if params.k == 0 {
        return Err(Error::InvalidK(params.k));
    }

    if params.n == 0 {
        return Err(Error::InvalidN(params.n));
    }

    let row_size = params.row_size;

    let engine = DefaultEngine::new();
    let mut decoder: HighRateDecoder<DefaultEngine> =
        RateDecoder::new(params.k, params.n, row_size, engine, None)
            .map_err(|e| Error::ReedSolomon(format!("Failed to create decoder: {:?}", e)))?;

    let row_bytes = rows.as_bytes();
    for (i, &index) in indices.iter().enumerate() {
        if index >= params.k + params.n {
            return Err(Error::InvalidIndex(index, params.k + params.n));
        }

        if index < params.k {
            let start = i * row_size;
            let end = start + row_size;
            decoder
                .add_original_shard(index, &row_bytes[start..end])
                .map_err(|e| {
                    Error::ReedSolomon(format!("Failed to add original shard: {:?}", e))
                })?;
        } else {
            let start = i * row_size;
            let end = start + row_size;
            decoder
                .add_recovery_shard(index - params.k, &row_bytes[start..end])
                .map_err(|e| {
                    Error::ReedSolomon(format!("Failed to add recovery shard: {:?}", e))
                })?;
        }
    }

    let result = decoder
        .decode()
        .map_err(|e| Error::ReedSolomon(format!("Failed to decode: {:?}", e)))?;

    let mut all_original = vec![0u8; params.k * row_size];

    for (i, &index) in indices.iter().enumerate() {
        if index < params.k {
            let src_start = i * row_size;
            let src_end = src_start + row_size;
            let dst_start = index * row_size;
            let dst_end = dst_start + row_size;
            all_original[dst_start..dst_end].copy_from_slice(&row_bytes[src_start..src_end]);
        }
    }

    for (index, shard) in result.restored_original_iter() {
        let dst_start = index * row_size;
        let dst_end = dst_start + row_size;
        all_original[dst_start..dst_end].copy_from_slice(shard);
    }

    OriginalRows::new(all_original, params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::ExtendedData;
    use crate::Parameters;

    #[test]
    fn test_reconstruct_from_original_rows() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let mut original = vec![0u8; params.k * params.row_size];
        for i in 0..params.k {
            original[i * params.row_size] = i as u8;
        }

        let original_rows = OriginalRows::new(original.clone(), &params).unwrap();
        let commitment = ExtendedData::generate(&original_rows, &params).unwrap();
        let indices = vec![0usize, 1, 2, 3];
        let rows = commitment.rows().sample(&indices).unwrap();
        let reconstructed = reconstruct_data(&rows, &indices, &params).unwrap();

        assert_eq!(reconstructed.as_bytes(), original.as_slice());
    }
}
