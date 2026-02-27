use crate::error::{Error, Result};
use reed_solomon_simd::engine::DefaultEngine;
use reed_solomon_simd::rate::{HighRateDecoder, RateDecoder};

/// Reconstruct original data from any K rows
pub fn reconstruct_data(
    rows: &[Vec<u8>],
    indices: &[usize],
    k: usize,
    n: usize,
) -> Result<Vec<Vec<u8>>> {
    if rows.len() != indices.len() {
        return Err(Error::InvalidParameters(format!(
            "rows and indices must have same length: {} != {}",
            rows.len(),
            indices.len()
        )));
    }

    if rows.len() < k {
        return Err(Error::InvalidParameters(format!(
            "need at least {} rows, got {}",
            k,
            rows.len()
        )));
    }

    if k == 0 {
        return Err(Error::InvalidK(k));
    }

    if n == 0 {
        return Err(Error::InvalidN(n));
    }

    let row_size = rows[0].len();

    let engine = DefaultEngine::new();
    let mut decoder: HighRateDecoder<DefaultEngine> =
        RateDecoder::new(k, n, row_size, engine, None)
            .map_err(|e| Error::ReedSolomon(format!("Failed to create decoder: {:?}", e)))?;

    for (i, &index) in indices.iter().enumerate() {
        if index >= k + n {
            return Err(Error::InvalidIndex(index, k + n));
        }

        if index < k {
            decoder.add_original_shard(index, &rows[i]).map_err(|e| {
                Error::ReedSolomon(format!("Failed to add original shard: {:?}", e))
            })?;
        } else {
            decoder
                .add_recovery_shard(index - k, &rows[i])
                .map_err(|e| {
                    Error::ReedSolomon(format!("Failed to add recovery shard: {:?}", e))
                })?;
        }
    }

    let result = decoder
        .decode()
        .map_err(|e| Error::ReedSolomon(format!("Failed to decode: {:?}", e)))?;

    let mut all_original = vec![vec![0u8; row_size]; k];

    for (i, &index) in indices.iter().enumerate() {
        if index < k {
            all_original[index] = rows[i].clone();
        }
    }

    for (index, shard) in result.restored_original_iter() {
        all_original[index] = shard.to_vec();
    }

    Ok(all_original)
}

#[cfg(test)]
mod colocated_comprehensive_reconstruction_tests {
    #![allow(clippy::uninlined_format_args)]

    use crate::codec::reconstruct_data;
    use crate::{ExtendedData, Parameters};

    fn make_test_data(k: usize, row_size: usize) -> Vec<Vec<u8>> {
        (0..k)
            .map(|i| (0..row_size).map(|j| ((i + j) % 256) as u8).collect())
            .collect()
    }

    fn make_range(start: usize, end: usize) -> Vec<usize> {
        (start..end).collect()
    }

    fn make_mixed_indices(k: usize, n: usize) -> Vec<usize> {
        let mut indices = Vec::new();
        let step = (k + n) / k;

        for i in 0..k {
            indices.push((i * step) % (k + n));
        }

        // Ensure uniqueness
        let mut seen = std::collections::HashSet::new();
        for i in 0..indices.len() {
            let mut idx = indices[i];
            while seen.contains(&idx) {
                idx = (idx + 1) % (k + n);
            }
            indices[i] = idx;
            seen.insert(idx);
        }

        indices
    }

    #[test]
    fn test_reconstruct_from_original_rows() {
        let test_cases = vec![
            (4, 4, 64),
            (8, 8, 256),
            (3, 5, 64),
            (5, 7, 128),
            (10, 15, 256),
        ];

        for (k, n, row_size) in test_cases {
            let params = Parameters::new(k, n, row_size).unwrap();
            let original_data = make_test_data(k, row_size);
            let commitment = ExtendedData::generate(&original_data, &params).unwrap();

            // Use only original rows
            let indices = make_range(0, k);
            let rows: Vec<Vec<u8>> = indices
                .iter()
                .map(|&i| commitment.all_rows[i].clone())
                .collect();

            let reconstructed = reconstruct_data(&rows, &indices, k, n).unwrap();

            assert_eq!(reconstructed.len(), k, "Wrong number of reconstructed rows");

            for i in 0..k {
                assert_eq!(
                    reconstructed[i], original_data[i],
                    "Reconstructed row {} doesn't match original for k={}, n={}",
                    i, k, n
                );
            }
        }
    }

    #[test]
    fn test_reconstruct_from_parity_rows() {
        let test_cases = vec![
            (4, 4, 64),
            (8, 8, 256),
            // k≠n cases - testing if fix works
            (3, 5, 64),
            (5, 7, 128),
        ];

        for (k, n, row_size) in test_cases {
            if n < k {
                continue; // Need at least k parity rows
            }

            let params = Parameters::new(k, n, row_size).unwrap();
            let original_data = make_test_data(k, row_size);
            let commitment = ExtendedData::generate(&original_data, &params).unwrap();

            // Use only parity rows (first k of them)
            let indices: Vec<usize> = (k..(k + k)).collect();
            let rows: Vec<Vec<u8>> = indices
                .iter()
                .map(|&i| commitment.all_rows[i].clone())
                .collect();

            let reconstructed = reconstruct_data(&rows, &indices, k, n).unwrap();

            assert_eq!(reconstructed.len(), k, "Wrong number of reconstructed rows");

            for i in 0..k {
                assert_eq!(
                    reconstructed[i], original_data[i],
                    "Reconstructed row {} doesn't match original for k={}, n={} (parity only)",
                    i, k, n
                );
            }
        }
    }

    #[test]
    fn test_reconstruct_from_mixed_rows() {
        let test_cases = vec![
            (4, 4, 64),
            (8, 8, 256),
            // k≠n cases - testing if fix works
            (3, 5, 64),
            (5, 7, 128),
            (10, 15, 256),
        ];

        for (k, n, row_size) in test_cases {
            let params = Parameters::new(k, n, row_size).unwrap();
            let original_data = make_test_data(k, row_size);
            let commitment = ExtendedData::generate(&original_data, &params).unwrap();

            // Use mixed indices
            let indices = make_mixed_indices(k, n);
            let rows: Vec<Vec<u8>> = indices
                .iter()
                .map(|&i| commitment.all_rows[i].clone())
                .collect();

            let reconstructed = reconstruct_data(&rows, &indices, k, n).unwrap();

            assert_eq!(reconstructed.len(), k, "Wrong number of reconstructed rows");

            for i in 0..k {
                assert_eq!(
                    reconstructed[i], original_data[i],
                    "Reconstructed row {} doesn't match original for k={}, n={} (mixed)",
                    i, k, n
                );
            }
        }
    }

    #[test]
    fn test_reconstruct_insufficient_rows() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let data = make_test_data(4, 64);
        let commitment = ExtendedData::generate(&data, &params).unwrap();

        // Use only 3 rows (insufficient)
        let indices = vec![0, 1, 2];
        let rows: Vec<Vec<u8>> = indices
            .iter()
            .map(|&i| commitment.all_rows[i].clone())
            .collect();

        let result = reconstruct_data(&rows, &indices, 4, 4);
        assert!(
            result.is_err(),
            "Reconstruction should fail with insufficient rows"
        );
    }

    #[test]
    fn test_reconstruct_corrupted_row() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let original_data = make_test_data(4, 64);
        let commitment = ExtendedData::generate(&original_data, &params).unwrap();

        let indices = make_range(0, 4);
        let mut rows: Vec<Vec<u8>> = indices
            .iter()
            .map(|&i| commitment.all_rows[i].clone())
            .collect();

        // Corrupt one row
        rows[0][0] ^= 0xFF;

        let reconstructed = reconstruct_data(&rows, &indices, 4, 4).unwrap();

        // Reconstruction will succeed but data will be wrong
        assert_ne!(
            reconstructed[0], original_data[0],
            "Corrupted data should produce wrong reconstruction"
        );
    }

    #[test]
    fn test_reconstruct_determinism() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let data = make_test_data(4, 64);
        let commitment = ExtendedData::generate(&data, &params).unwrap();

        let indices = make_range(0, 4);
        let rows: Vec<Vec<u8>> = indices
            .iter()
            .map(|&i| commitment.all_rows[i].clone())
            .collect();

        let reconstructed1 = reconstruct_data(&rows, &indices, 4, 4).unwrap();
        let reconstructed2 = reconstruct_data(&rows, &indices, 4, 4).unwrap();

        assert_eq!(
            reconstructed1, reconstructed2,
            "Reconstruction not deterministic"
        );
    }
}
