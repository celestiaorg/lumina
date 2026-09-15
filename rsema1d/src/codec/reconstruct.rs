use super::default_work_budget;
use super::rs::{stripe_plan, Stripe, StripePlan, MIN_STRIPE};
use crate::codec::rows::RowMatrix;
use crate::error::{Error, Result};
use crate::params::Parameters;
use rayon::prelude::*;
use reed_solomon_simd::engine::DefaultEngine;
use reed_solomon_simd::rate::{HighRateDecoder, RateDecoder};
use std::num::NonZeroUsize;

// Reuse the decoder's work buffer across stripes.
fn decode_stripe<'a>(
    decoder: &'a mut HighRateDecoder<DefaultEngine>,
    rows: &[&[u8]],
    indices: &[usize],
    params: &Parameters,
    stripe: Stripe,
) -> Result<reed_solomon_simd::DecoderResult<'a>> {
    decoder
        .reset(params.k, params.n, stripe.len)
        .map_err(|e| Error::ReedSolomon(format!("Failed to create decoder: {e:?}")))?;
    for (row, &index) in rows.iter().zip(indices) {
        let row = &row[stripe.offset..stripe.offset + stripe.len];
        if index < params.k {
            decoder
                .add_original_shard(index, row)
                .map_err(|e| Error::ReedSolomon(format!("Failed to add original shard: {e:?}")))?;
        } else {
            decoder
                .add_recovery_shard(index - params.k, row)
                .map_err(|e| Error::ReedSolomon(format!("Failed to add recovery shard: {e:?}")))?;
        }
    }
    decoder
        .decode()
        .map_err(|e| Error::ReedSolomon(format!("Failed to decode: {e:?}")))
}

fn decoder_work_shards(k: usize, n: usize) -> usize {
    (n.next_power_of_two() + k).next_power_of_two()
}

/// Reconstruct original data from any K sampled rows.
///
/// `rows` are the raw row byte slices and `indices` are their corresponding
/// positions in the original+parity matrix.
pub fn reconstruct_data(
    rows: &[&[u8]],
    indices: &[usize],
    params: &Parameters,
) -> Result<RowMatrix> {
    reconstruct_data_with_work_budget(rows, indices, params, default_work_budget())
}

pub(super) fn reconstruct_data_with_work_budget(
    rows: &[&[u8]],
    indices: &[usize],
    params: &Parameters,
    work_budget: NonZeroUsize,
) -> Result<RowMatrix> {
    if rows.len() != indices.len() {
        return Err(Error::InvalidParameters(format!(
            "rows count mismatch: expected {}, got {}",
            indices.len(),
            rows.len(),
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

    for (i, row) in rows.iter().enumerate() {
        if row.len() != row_size {
            return Err(Error::InvalidParameters(format!(
                "row {} size mismatch: expected {}, got {}",
                i,
                row_size,
                row.len()
            )));
        }
    }

    HighRateDecoder::<DefaultEngine>::validate(params.k, params.n, row_size)
        .map_err(|e| Error::ReedSolomon(format!("Failed to create decoder: {:?}", e)))?;

    let mut seen = vec![false; params.k + params.n];
    for &index in indices {
        if index >= params.k + params.n {
            return Err(Error::InvalidIndex(index, params.k + params.n));
        }
        if seen[index] {
            let error = if index < params.k {
                let error = reed_solomon_simd::Error::DuplicateOriginalShardIndex { index };
                format!("Failed to add original shard: {error:?}")
            } else {
                let error = reed_solomon_simd::Error::DuplicateRecoveryShardIndex {
                    index: index - params.k,
                };
                format!("Failed to add recovery shard: {error:?}")
            };
            return Err(Error::ReedSolomon(error));
        }
        seen[index] = true;
    }

    let plan = stripe_plan(
        decoder_work_shards(params.k, params.n),
        row_size,
        rayon::current_num_threads(),
        work_budget,
    )?;
    reconstruct_data_with_plan(rows, indices, params, plan)
}

fn reconstruct_data_with_plan(
    rows: &[&[u8]],
    indices: &[usize],
    params: &Parameters,
    plan: StripePlan,
) -> Result<RowMatrix> {
    let row_size = params.row_size;
    let mut all_original = RowMatrix::zeroed(params.k, row_size)?;
    let mut missing = vec![true; params.k];

    for (row, &index) in rows.iter().zip(indices) {
        if index < params.k {
            all_original.row_mut(index)?.copy_from_slice(row);
            missing[index] = false;
        }
    }

    if missing.iter().all(|&missing| !missing) {
        return Ok(all_original);
    }

    if plan.stripe_size() >= row_size {
        let mut decoder =
            RateDecoder::new(params.k, params.n, row_size, DefaultEngine::new(), None)
                .map_err(|e| Error::ReedSolomon(format!("Failed to create decoder: {e:?}")))?;
        let result = decode_stripe(
            &mut decoder,
            rows,
            indices,
            params,
            Stripe {
                offset: 0,
                len: row_size,
            },
        )?;
        for (index, shard) in result.restored_original_iter() {
            all_original.row_mut(index)?.copy_from_slice(shard);
        }
        return Ok(all_original);
    }

    let stripe_size = plan.stripe_size();
    let parallelism = plan.parallelism();
    assert!(stripe_size.is_multiple_of(MIN_STRIPE));
    let stripes: Vec<_> = (0..row_size)
        .step_by(stripe_size)
        .map(|offset| Stripe {
            offset,
            len: stripe_size.min(row_size - offset),
        })
        .collect();
    let slot_count = parallelism.min(stripes.len());
    let mut decoders = (0..slot_count)
        .into_par_iter()
        .map(|_| {
            RateDecoder::new(params.k, params.n, stripe_size, DefaultEngine::new(), None)
                .map_err(|e| Error::ReedSolomon(format!("Failed to create decoder: {e:?}")))
        })
        .collect::<Result<Vec<_>>>()?;

    for batch in stripes.chunks(parallelism) {
        let results = decoders[..batch.len()]
            .par_iter_mut()
            .zip(batch.par_iter())
            .map(|(decoder, &stripe)| decode_stripe(decoder, rows, indices, params, stripe))
            .collect::<Result<Vec<_>>>()?;

        all_original
            .as_row_major_mut()
            .par_chunks_mut(row_size)
            .enumerate()
            .for_each(|(index, row)| {
                if !missing[index] {
                    return;
                }

                for (&stripe, result) in batch.iter().zip(&results) {
                    row[stripe.offset..stripe.offset + stripe.len]
                        .copy_from_slice(result.restored_original(index).unwrap());
                }
            });
    }

    Ok(all_original)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::ExtendedData;
    use crate::Parameters;

    fn test_stripe_plan(stripe_size: usize, parallelism: usize) -> StripePlan {
        StripePlan::new(
            NonZeroUsize::new(stripe_size).unwrap(),
            NonZeroUsize::new(parallelism).unwrap(),
        )
    }

    fn make_original(params: &Parameters) -> RowMatrix {
        use rand::{RngCore, SeedableRng};
        let mut data = vec![0; params.k * params.row_size];
        rand_chacha::ChaCha8Rng::seed_from_u64(42).fill_bytes(&mut data);
        RowMatrix::with_shape(data, params.k, params.row_size).unwrap()
    }

    #[test]
    fn decoder_plan_respects_work_budget() {
        assert_eq!(decoder_work_shards(4096, 12288), 32768);
        assert_eq!(decoder_work_shards(3073, 1024), 8192);
        for (k, n) in [(4096, 12288), (3073, 1024), (8, 24)] {
            let work_shards = decoder_work_shards(k, n);
            for threads in [1, 4, 16, 32] {
                for budget in [1, work_shards * MIN_STRIPE, 32 << 20] {
                    let plan = stripe_plan(
                        work_shards,
                        32768,
                        threads,
                        NonZeroUsize::new(budget).unwrap(),
                    )
                    .unwrap();
                    assert!(
                        plan.parallelism() * work_shards * plan.stripe_size()
                            <= budget.max(work_shards * MIN_STRIPE)
                    );
                }
            }
        }
    }

    #[test]
    fn reconstruction_handles_a_partial_final_stripe() {
        let params = Parameters::new(8, 24, 768).unwrap();
        let original = make_original(&params);
        let extended = ExtendedData::generate(&original, &params).unwrap();
        for indices in [
            vec![0, 3, 8, 11, 15, 20, 25, 31],
            (8..16).collect(),
            vec![25, 3, 11, 0, 31, 20, 8, 15, 27],
        ] {
            let rows: Vec<_> = indices.iter().map(|&i| extended.row(i).unwrap()).collect();
            for stripe_size in [512, 320] {
                let reconstructed = reconstruct_data_with_plan(
                    &rows,
                    &indices,
                    &params,
                    test_stripe_plan(stripe_size, 2),
                )
                .unwrap();
                assert_eq!(reconstructed.as_row_major(), original.as_row_major());
            }
            for budget in [1, 4096, 32768] {
                let reconstructed = reconstruct_data_with_work_budget(
                    &rows,
                    &indices,
                    &params,
                    NonZeroUsize::new(budget).unwrap(),
                )
                .unwrap();
                assert_eq!(reconstructed.as_row_major(), original.as_row_major());
            }
        }
    }

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
        let rows: Vec<&[u8]> = indices
            .iter()
            .map(|&i| commitment.rows().row(i).unwrap())
            .collect();
        let reconstructed = reconstruct_data(&rows, &indices, &params).unwrap();

        assert_eq!(reconstructed.as_row_major(), original.as_slice());
    }

    #[test]
    fn striped_reconstruction_matches_original() {
        let params = Parameters::new(8, 24, 512).unwrap();
        let original = make_original(&params);
        let extended = ExtendedData::generate(&original, &params).unwrap();

        for indices in [
            (0..params.k).collect::<Vec<_>>(),
            (params.k..params.k + params.k).collect(),
            vec![0, 3, 8, 11, 15, 20, 25, 31],
        ] {
            let rows: Vec<_> = indices
                .iter()
                .map(|&index| extended.row(index).unwrap())
                .collect();

            for plan in [
                test_stripe_plan(512, 4),
                test_stripe_plan(256, 2),
                test_stripe_plan(64, 4),
            ] {
                let reconstructed =
                    reconstruct_data_with_plan(&rows, &indices, &params, plan).unwrap();
                assert_eq!(reconstructed.as_row_major(), original.as_row_major());
            }
        }
    }

    #[test]
    fn invalid_inputs_are_rejected() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let rows = vec![[0u8; 64]; 4];
        let row_refs: Vec<_> = rows.iter().map(<[_; 64]>::as_slice).collect();

        assert!(matches!(
            reconstruct_data(&row_refs[..3], &[0, 1, 2, 3], &params),
            Err(Error::InvalidParameters(_))
        ));
        assert!(matches!(
            reconstruct_data(&row_refs[..3], &[0, 1, 2], &params),
            Err(Error::InvalidParameters(_))
        ));
        assert!(matches!(
            reconstruct_data(&row_refs, &[0, 1, 2, 8], &params),
            Err(Error::InvalidIndex(8, 8))
        ));

        let short = [0u8; 32];
        let mut short_rows = row_refs;
        short_rows[3] = &short;
        assert!(matches!(
            reconstruct_data(&short_rows, &[0, 1, 2, 3], &params),
            Err(Error::InvalidParameters(_))
        ));
    }

    #[test]
    fn duplicate_shard_errors_preserve_backend_messages() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let rows = [&[0u8; 64][..]; 4];
        for (indices, expected) in [
            (
                [0, 1, 2, 2],
                "Failed to add original shard: DuplicateOriginalShardIndex { index: 2 }",
            ),
            (
                [0, 1, 5, 5],
                "Failed to add recovery shard: DuplicateRecoveryShardIndex { index: 1 }",
            ),
        ] {
            let Error::ReedSolomon(message) =
                reconstruct_data(&rows, &indices, &params).unwrap_err()
            else {
                panic!("expected a Reed-Solomon error");
            };
            assert_eq!(message, expected);
        }
    }
}
