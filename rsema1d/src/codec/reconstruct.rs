use crate::codec::rows::RowMatrix;
use crate::error::{Error, Result};
use crate::params::Parameters;
use reed_solomon_simd::engine::DefaultEngine;
use reed_solomon_simd::rate::{HighRateDecoder, RateDecoder};

/// Reconstruct original data from any K rows
pub fn reconstruct_data(
    rows: &RowMatrix,
    indices: &[usize],
    params: &Parameters,
) -> Result<RowMatrix> {
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

    for (i, &index) in indices.iter().enumerate() {
        if index >= params.k + params.n {
            return Err(Error::InvalidIndex(index, params.k + params.n));
        }

        let row = rows.row(i)?;
        if index < params.k {
            decoder.add_original_shard(index, row).map_err(|e| {
                Error::ReedSolomon(format!("Failed to add original shard: {:?}", e))
            })?;
        } else {
            decoder
                .add_recovery_shard(index - params.k, row)
                .map_err(|e| {
                    Error::ReedSolomon(format!("Failed to add recovery shard: {:?}", e))
                })?;
        }
    }

    let result = decoder
        .decode()
        .map_err(|e| Error::ReedSolomon(format!("Failed to decode: {:?}", e)))?;

    let mut all_original =
        RowMatrix::with_shape(vec![0u8; params.k * row_size], params.k, params.row_size)?;

    for (i, &index) in indices.iter().enumerate() {
        if index < params.k {
            let src = rows.row(i)?;
            let dst = all_original.row_mut(index)?;
            dst.copy_from_slice(src);
        }
    }

    for (index, shard) in result.restored_original_iter() {
        let dst = all_original.row_mut(index)?;
        dst.copy_from_slice(shard);
    }

    Ok(all_original)
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

        let original_rows =
            RowMatrix::with_shape(original.clone(), params.k, params.row_size).unwrap();
        let commitment = ExtendedData::generate(&original_rows, &params).unwrap();
        let indices = vec![0usize, 1, 2, 3];
        let rows = commitment.rows().sample(&indices).unwrap();
        let reconstructed = reconstruct_data(&rows, &indices, &params).unwrap();

        assert_eq!(reconstructed.as_row_major(), original.as_slice());
    }
}
