//! Reed-Solomon encoding/decoding with RLC integration.

mod commitment;
mod padding;
mod proof;
mod reconstruct;
mod rows;
mod rs;
mod symbols;
mod verification;

use crate::error::Result;
use crate::field::GF128;
use crate::params::Parameters;

pub use commitment::ExtendedData;
pub use padding::map_index_to_tree_position;
pub use proof::{RowInclusionProof, RowProof, StandaloneProof};
pub use reconstruct::reconstruct_data;
pub use rows::{ExtendedRowsView, OriginalRowsView, RowMatrix};
pub use rs::{
    encode_parity_in_place, extend_data, extend_rlcs, pack_gf128_to_shard, unpack_shard_to_gf128,
};
pub use symbols::{compute_rlc, extract_symbols};
pub use verification::{
    create_verification_context, verify_proof, verify_row_inclusion, verify_row_inclusion_proof,
    verify_row_with_context, verify_standalone, verify_standalone_proof, verify_with_context,
    VerificationContext,
};

/// A 32-byte SHA-256 commitment hash.
pub type Commitment = [u8; 32];

/// Encode original rows and return extended data, commitment, and original RLCs.
pub fn encode(
    data: &RowMatrix,
    params: &Parameters,
) -> Result<(ExtendedData, Commitment, Vec<GF128>)> {
    let ext_data = ExtendedData::generate(data, params)?;
    let commitment = ext_data.commitment();
    let rlc_orig = ext_data.rlc_original().to_vec();
    Ok((ext_data, commitment, rlc_orig))
}

/// Encode from a caller-provided extended row buffer.
///
/// This performs the same pipeline as [`encode`]:
/// 1. assumes the first K rows of `extended_rows` already contain original data
/// 2. computes parity rows in place
/// 3. builds commitment, trees, and RLCs from the extended rows
pub fn encode_in_place(
    mut extended_rows: RowMatrix,
    params: &Parameters,
) -> Result<(ExtendedData, Commitment, Vec<GF128>)> {
    extended_rows.extended_view(params)?;
    encode_parity_in_place(&mut extended_rows, params)?;

    let ext_data = ExtendedData::generate_from_extended_rows(extended_rows, params)?;
    let commitment = ext_data.commitment();
    let rlc_orig = ext_data.rlc_original().to_vec();
    Ok((ext_data, commitment, rlc_orig))
}

/// Compute commitment/proofs from already-extended rows.
pub fn encode_parity(
    extended_rows: RowMatrix,
    params: &Parameters,
) -> Result<(ExtendedData, Commitment, Vec<GF128>)> {
    let ext_data = ExtendedData::generate_from_extended_rows(extended_rows, params)?;
    let commitment = ext_data.commitment();
    let rlc_orig = ext_data.rlc_original().to_vec();
    Ok((ext_data, commitment, rlc_orig))
}

/// Reconstruct original rows from any K sampled rows.
pub fn reconstruct(rows: &RowMatrix, indices: &[usize], params: &Parameters) -> Result<RowMatrix> {
    reconstruct_data(rows, indices, params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crypto::{derive_coefficients, sha256};
    use crate::error::Error;
    use rand::{RngCore, SeedableRng};
    use rand_chacha::ChaCha8Rng;
    use std::borrow::Cow;

    #[derive(Clone, Copy)]
    struct Case {
        k: usize,
        n: usize,
        row_size: usize,
        seed: u64,
    }

    const CASES: &[Case] = &[
        Case {
            k: 1,
            n: 1,
            row_size: 64,
            seed: 1,
        },
        Case {
            k: 4,
            n: 4,
            row_size: 64,
            seed: 2,
        },
        Case {
            k: 8,
            n: 8,
            row_size: 64,
            seed: 3,
        },
        Case {
            k: 16,
            n: 16,
            row_size: 64,
            seed: 4,
        },
        Case {
            k: 4,
            n: 12,
            row_size: 64,
            seed: 5,
        },
        Case {
            k: 8,
            n: 24,
            row_size: 64,
            seed: 6,
        },
        Case {
            k: 16,
            n: 48,
            row_size: 64,
            seed: 7,
        },
        Case {
            k: 3,
            n: 5,
            row_size: 64,
            seed: 8,
        },
        Case {
            k: 5,
            n: 7,
            row_size: 64,
            seed: 9,
        },
        Case {
            k: 7,
            n: 9,
            row_size: 128,
            seed: 10,
        },
        Case {
            k: 10,
            n: 6,
            row_size: 64,
            seed: 11,
        },
        Case {
            k: 13,
            n: 3,
            row_size: 64,
            seed: 12,
        },
        Case {
            k: 33,
            n: 17,
            row_size: 64,
            seed: 13,
        },
        Case {
            k: 127,
            n: 129,
            row_size: 64,
            seed: 14,
        },
    ];

    fn make_original_rows(case: Case) -> (Parameters, RowMatrix) {
        let params = Parameters::new(case.k, case.n, case.row_size).unwrap();
        let mut bytes = vec![0u8; case.k * case.row_size];
        let mut rng = ChaCha8Rng::seed_from_u64(case.seed);
        rng.fill_bytes(&mut bytes);
        let rows = RowMatrix::with_shape(bytes, case.k, case.row_size).unwrap();
        (params, rows)
    }

    fn strategic_indices(params: &Parameters) -> Vec<usize> {
        let mut indices = vec![
            0,
            params.k - 1,
            params.k,
            params.total_rows() - 1,
            params.k / 2,
            params.k + (params.n / 2),
        ];
        indices.sort_unstable();
        indices.dedup();
        indices
    }

    fn mixed_indices(params: &Parameters) -> Vec<usize> {
        let mut out = Vec::with_capacity(params.k);
        let mut oi = 0usize;
        let mut pi = 0usize;

        while out.len() < params.k {
            if oi < params.k {
                out.push(oi);
                oi += 2;
                if oi >= params.k && params.k > 1 {
                    oi = 1;
                }
            }

            if out.len() >= params.k {
                break;
            }

            if pi < params.n {
                out.push(params.k + pi);
                pi += 2;
                if pi >= params.n && params.n > 1 {
                    pi = 1;
                }
            }

            if oi >= params.k && pi >= params.n {
                break;
            }
        }

        out.sort_unstable();
        out.dedup();
        if out.len() < params.k {
            for i in 0..params.total_rows() {
                if !out.contains(&i) {
                    out.push(i);
                    if out.len() == params.k {
                        break;
                    }
                }
            }
        }
        out
    }

    #[test]
    fn encode_and_verify_with_context_matrix() {
        for case in CASES {
            let (params, original) = make_original_rows(*case);
            let (ext_data, commitment, rlc_orig) = encode(&original, &params).unwrap();
            let (context, rlc_root) = create_verification_context(&rlc_orig, &params).unwrap();
            assert_eq!(rlc_root, ext_data.rlc_root());

            for idx in strategic_indices(&params) {
                let row_proof = ext_data.generate_row_proof(idx).unwrap();
                assert!(verify_with_context(&row_proof, &commitment, &context).unwrap());
                verify_row_with_context(&row_proof, &commitment, &context).unwrap();

                if idx < params.k {
                    let standalone = ext_data.generate_standalone_proof(idx).unwrap();
                    assert!(verify_standalone(&standalone, &commitment, &params).unwrap());
                    verify_standalone_proof(&standalone, &commitment, &params).unwrap();
                }
            }
        }
    }

    #[test]
    fn reconstruction_matrix() {
        for case in CASES {
            let (params, original) = make_original_rows(*case);
            let (ext_data, _, _) = encode(&original, &params).unwrap();

            let original_indices: Vec<usize> = (0..params.k).collect();
            let original_rows = ext_data.rows().sample(&original_indices).unwrap();
            let reconstructed = reconstruct(&original_rows, &original_indices, &params).unwrap();
            assert_eq!(reconstructed.as_row_major(), original.as_row_major());

            if params.n >= params.k {
                let parity_indices: Vec<usize> = (params.k..(params.k + params.k)).collect();
                let parity_rows = ext_data.rows().sample(&parity_indices).unwrap();
                let reconstructed = reconstruct(&parity_rows, &parity_indices, &params).unwrap();
                assert_eq!(reconstructed.as_row_major(), original.as_row_major());
            }

            let mixed = mixed_indices(&params);
            let mixed_rows = ext_data.rows().sample(&mixed).unwrap();
            let reconstructed = reconstruct(&mixed_rows, &mixed, &params).unwrap();
            assert_eq!(reconstructed.as_row_major(), original.as_row_major());
        }
    }

    #[test]
    fn rlc_commutation_property() {
        for case in CASES {
            let (params, original) = make_original_rows(*case);
            let (ext_data, _, _) = encode(&original, &params).unwrap();
            let coeffs = derive_coefficients(&ext_data.row_root(), params.symbols_per_row());

            let extended_from_orig =
                extend_rlcs(ext_data.rlc_original(), params.k, params.n).unwrap();
            assert_eq!(extended_from_orig, ext_data.rlc_extended());

            for i in 0..params.total_rows() {
                let row = ext_data.row(i).unwrap();
                let computed = compute_rlc(row, &coeffs);
                assert_eq!(computed, ext_data.rlc_extended()[i]);
            }
        }
    }

    #[test]
    fn invalid_proofs_rejected() {
        let params = Parameters::new(8, 8, 64).unwrap();
        let (_, original) = make_original_rows(Case {
            k: params.k,
            n: params.n,
            row_size: params.row_size,
            seed: 42,
        });

        let (ext_data, commitment, rlc_orig) = encode(&original, &params).unwrap();
        let context = VerificationContext::new(&rlc_orig, &params).unwrap();

        for idx in [0usize, params.k] {
            let proof = ext_data.generate_row_proof(idx).unwrap();
            assert!(verify_with_context(&proof, &commitment, &context).unwrap());

            let mut row = proof.row.to_vec();
            row[0] ^= 0x01;
            let corrupted_row = RowProof {
                index: proof.index,
                row: Cow::Owned(row),
                row_proof: proof.row_proof.clone(),
            };
            assert!(verify_with_context(&corrupted_row, &commitment, &context).is_err());

            let mut row_proof = proof.row_proof.clone();
            row_proof[0][0] ^= 0x01;
            let corrupted_path = RowProof {
                index: proof.index,
                row: Cow::Owned(proof.row.to_vec()),
                row_proof,
            };
            assert!(verify_with_context(&corrupted_path, &commitment, &context).is_err());

            let wrong_index = RowProof {
                index: (proof.index + 1) % params.total_rows(),
                row: Cow::Owned(proof.row.to_vec()),
                row_proof: proof.row_proof.clone(),
            };
            assert!(verify_with_context(&wrong_index, &commitment, &context).is_err());

            let nil_row = RowProof {
                index: proof.index,
                row: Cow::Owned(Vec::new()),
                row_proof: proof.row_proof.clone(),
            };
            assert!(verify_with_context(&nil_row, &commitment, &context).is_err());
        }

        let standalone = ext_data.generate_standalone_proof(0).unwrap();
        assert!(verify_standalone(&standalone, &commitment, &params).unwrap());

        let mut corrupted_row = standalone.clone();
        corrupted_row.row[0] ^= 0x01;
        assert!(verify_standalone(&corrupted_row, &commitment, &params).is_err());

        let mut corrupted_row_path = standalone.clone();
        corrupted_row_path.row_proof[0][0] ^= 0x01;
        assert!(verify_standalone(&corrupted_row_path, &commitment, &params).is_err());

        let mut corrupted_rlc_path = standalone.clone();
        corrupted_rlc_path.rlc_proof[0][0] ^= 0x01;
        assert!(verify_standalone(&corrupted_rlc_path, &commitment, &params).is_err());

        let mut wrong_index = standalone.clone();
        wrong_index.index = 1;
        assert!(verify_standalone(&wrong_index, &commitment, &params).is_err());

        let mut empty_rlc_path = standalone.clone();
        empty_rlc_path.rlc_proof.clear();
        assert!(verify_standalone(&empty_rlc_path, &commitment, &params).is_err());

        let mut empty_row = standalone;
        empty_row.row.clear();
        assert!(verify_standalone(&empty_row, &commitment, &params).is_err());
    }

    #[test]
    fn corrupted_context_fails_verification() {
        let (params, original) = make_original_rows(Case {
            k: 8,
            n: 8,
            row_size: 64,
            seed: 99,
        });
        let (ext_data, commitment, rlc_orig) = encode(&original, &params).unwrap();

        let proof = ext_data.generate_row_proof(0).unwrap();
        assert!(verify_with_context(
            &proof,
            &commitment,
            &VerificationContext::new(&rlc_orig, &params).unwrap()
        )
        .unwrap());

        let mut corrupted = rlc_orig.clone();
        corrupted[0].limbs[0] ^= 0x01;
        let corrupted_context = VerificationContext::new(&corrupted, &params).unwrap();
        assert!(verify_with_context(&proof, &commitment, &corrupted_context).is_err());
    }

    #[test]
    fn row_inclusion_proof_roundtrip_and_rejection() {
        let (params, original) = make_original_rows(Case {
            k: 8,
            n: 8,
            row_size: 64,
            seed: 77,
        });
        let (ext_data, commitment, _) = encode(&original, &params).unwrap();

        for i in 0..params.total_rows() {
            let proof = ext_data.generate_row_inclusion_proof(i).unwrap();
            assert!(verify_row_inclusion(&proof, &commitment, &params).unwrap());
            verify_row_inclusion_proof(&proof, &commitment, &params).unwrap();

            let row_proof = ext_data.generate_row_proof(i).unwrap();
            let manual = RowInclusionProof {
                index: row_proof.index,
                row: row_proof.row.into_owned(),
                row_proof: row_proof.row_proof,
                rlc_root: ext_data.rlc_root(),
            };
            assert!(verify_row_inclusion(&manual, &commitment, &params).unwrap());
            verify_row_inclusion_proof(&manual, &commitment, &params).unwrap();
        }

        let mut bad_commitment = commitment;
        bad_commitment[0] ^= 0x01;
        let proof = ext_data.generate_row_inclusion_proof(0).unwrap();
        assert!(verify_row_inclusion(&proof, &bad_commitment, &params).is_err());

        let mut bad_row = proof.clone();
        bad_row.row[0] ^= 0x01;
        assert!(verify_row_inclusion(&bad_row, &commitment, &params).is_err());

        assert!(ext_data
            .generate_row_inclusion_proof(params.total_rows())
            .is_err());
        assert!(ext_data.generate_row_proof(params.total_rows()).is_err());
    }

    #[test]
    fn encode_parity_matches_encode() {
        for case in CASES {
            let (params, original) = make_original_rows(*case);
            let (ext_a, commitment_a, rlc_a) = encode(&original, &params).unwrap();
            let (ext_b, commitment_b, rlc_b) =
                encode_parity(ext_a.rows().clone(), &params).unwrap();

            assert_eq!(commitment_a, commitment_b);
            assert_eq!(rlc_a, rlc_b);
            assert_eq!(ext_a.rows().as_row_major(), ext_b.rows().as_row_major());

            let context = VerificationContext::new(&rlc_b, &params).unwrap();
            for idx in strategic_indices(&params) {
                let proof = ext_b.generate_row_proof(idx).unwrap();
                assert!(verify_with_context(&proof, &commitment_b, &context).unwrap());
            }
        }
    }

    #[test]
    fn tampered_extended_rows_fail_on_tampered_indices() {
        let (params, original) = make_original_rows(Case {
            k: 8,
            n: 8,
            row_size: 64,
            seed: 123,
        });
        let (honest, _, _) = encode(&original, &params).unwrap();

        let mut tampered_rows = honest.rows().clone();
        tampered_rows.row_mut(params.k).unwrap()[0] ^= 0x01;
        let (tampered, commitment, rlc_orig) = encode_parity(tampered_rows, &params).unwrap();
        let context = VerificationContext::new(&rlc_orig, &params).unwrap();

        let parity_proof = tampered.generate_row_proof(params.k).unwrap();
        assert!(verify_with_context(&parity_proof, &commitment, &context).is_err());

        let original_proof = tampered.generate_row_proof(0).unwrap();
        assert!(verify_with_context(&original_proof, &commitment, &context).unwrap());
    }

    #[test]
    fn tampered_original_rlc_breaks_original_and_parity_checks() {
        let (params, original) = make_original_rows(Case {
            k: 8,
            n: 8,
            row_size: 64,
            seed: 321,
        });
        let (ext_data, commitment, rlc_orig) = encode(&original, &params).unwrap();

        let honest_context = VerificationContext::new(&rlc_orig, &params).unwrap();
        let proof0 = ext_data.generate_row_proof(0).unwrap();
        assert!(verify_with_context(&proof0, &commitment, &honest_context).unwrap());

        let mut tampered_rlc = rlc_orig.clone();
        tampered_rlc[0].limbs[0] ^= 0x01;
        let tampered_context = VerificationContext::new(&tampered_rlc, &params).unwrap();

        let mut combined = Vec::with_capacity(64);
        combined.extend_from_slice(&ext_data.row_root());
        combined.extend_from_slice(&tampered_context.rlc_root());
        let fake_commitment = sha256(&combined);

        assert!(verify_with_context(&proof0, &fake_commitment, &tampered_context).is_err());

        let parity_proof = ext_data.generate_row_proof(params.k).unwrap();
        assert!(verify_with_context(&parity_proof, &fake_commitment, &tampered_context).is_err());
    }

    #[test]
    fn multiple_tampered_rows_are_detected() {
        let (params, original) = make_original_rows(Case {
            k: 8,
            n: 8,
            row_size: 64,
            seed: 555,
        });
        let (honest, _, _) = encode(&original, &params).unwrap();
        let mut tampered_rows = honest.rows().clone();

        let tampered_indices: Vec<usize> =
            (0..usize::min(3, params.n)).map(|i| params.k + i).collect();
        for &idx in &tampered_indices {
            let row = tampered_rows.row_mut(idx).unwrap();
            row[0] ^= 0x55;
            row[7] ^= 0xA5;
        }

        let (tampered, commitment, rlc_orig) = encode_parity(tampered_rows, &params).unwrap();
        let context = VerificationContext::new(&rlc_orig, &params).unwrap();

        for &idx in &tampered_indices {
            let proof = tampered.generate_row_proof(idx).unwrap();
            assert!(verify_with_context(&proof, &commitment, &context).is_err());
        }

        let untampered = tampered.generate_row_proof(0).unwrap();
        assert!(verify_with_context(&untampered, &commitment, &context).unwrap());
    }

    #[test]
    fn invalid_row_proof_depth_rejected() {
        let (params, original) = make_original_rows(Case {
            k: 8,
            n: 8,
            row_size: 64,
            seed: 777,
        });
        let (ext_data, commitment, rlc_orig) = encode(&original, &params).unwrap();
        let context = VerificationContext::new(&rlc_orig, &params).unwrap();

        let proof = ext_data.generate_row_proof(0).unwrap();
        let mut short = proof.row_proof.clone();
        short.pop();
        let malicious = RowProof {
            index: proof.index,
            row: Cow::Owned(proof.row.to_vec()),
            row_proof: short,
        };

        match verify_with_context(&malicious, &commitment, &context) {
            Err(Error::InvalidParameters(msg)) => assert!(msg.contains("depth mismatch")),
            other => panic!("expected depth mismatch error, got {:?}", other),
        }
    }

    #[test]
    fn truncated_row_rejected_even_with_valid_merkle_path() {
        let (params, original) = make_original_rows(Case {
            k: 8,
            n: 8,
            row_size: 64,
            seed: 888,
        });
        let (ext_data, commitment, rlc_orig) = encode(&original, &params).unwrap();
        let context = VerificationContext::new(&rlc_orig, &params).unwrap();

        let proof = ext_data.generate_row_proof(3).unwrap();
        let mut truncated = proof.row.to_vec();
        truncated.truncate(params.row_size / 2);
        let malicious = RowProof {
            index: proof.index,
            row: Cow::Owned(truncated),
            row_proof: proof.row_proof,
        };

        match verify_with_context(&malicious, &commitment, &context) {
            Err(Error::RowLengthMismatch { .. }) => {}
            other => panic!("expected row length mismatch, got {:?}", other),
        }
    }

    #[test]
    fn extend_and_reconstruct_error_paths() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let (_, original) = make_original_rows(Case {
            k: params.k,
            n: params.n,
            row_size: params.row_size,
            seed: 999,
        });
        let extended = extend_data(original.original_view(&params).unwrap(), &params).unwrap();
        assert_eq!(extended.rows(), params.total_rows());
        for i in 0..params.k {
            assert_eq!(extended.row(i).unwrap(), original.row(i).unwrap());
        }

        let bad_indices = vec![0usize, 1];
        let sampled = extended.sample(&bad_indices).unwrap();
        assert!(reconstruct(&sampled, &bad_indices, &params).is_err());

        let mut indices: Vec<usize> = (0..params.k).collect();
        indices[0] = params.total_rows();
        let sampled = extended.sample(&[0, 1, 2, 3]).unwrap();
        assert!(reconstruct(&sampled, &indices, &params).is_err());
    }

    #[test]
    fn extend_rlc_padding_consistency() {
        let k = 8usize;
        let n = 8usize;
        let mut rng = ChaCha8Rng::seed_from_u64(123456);
        let mut rlc_orig = Vec::with_capacity(k);
        for _ in 0..k {
            let mut limbs = [0u16; 8];
            for limb in &mut limbs {
                *limb = (rng.next_u32() & 0xFFFF) as u16;
            }
            rlc_orig.push(GF128 { limbs });
        }

        for rlc in &rlc_orig {
            let packed = pack_gf128_to_shard(rlc);
            assert!(packed[8..32].iter().all(|b| *b == 0));
            assert!(packed[40..64].iter().all(|b| *b == 0));
        }

        let extended = extend_rlcs(&rlc_orig, k, n).unwrap();
        assert_eq!(extended.len(), k + n);
        assert_eq!(&extended[..k], &rlc_orig[..]);

        for rlc in &extended {
            let packed = pack_gf128_to_shard(rlc);
            assert!(packed[8..32].iter().all(|b| *b == 0));
            assert!(packed[40..64].iter().all(|b| *b == 0));
        }
    }

    #[test]
    fn commitment_determinism_and_coefficients_consistency() {
        let (params, original) = make_original_rows(Case {
            k: 16,
            n: 16,
            row_size: 128,
            seed: 404,
        });
        let (ext_a, commitment_a, _) = encode(&original, &params).unwrap();
        let (ext_b, commitment_b, _) = encode(&original, &params).unwrap();
        assert_eq!(commitment_a, commitment_b);
        assert_eq!(ext_a.row_root(), ext_b.row_root());

        let mut mutated = original.clone();
        mutated.row_mut(0).unwrap()[0] ^= 0x01;
        let (_, commitment_c, _) = encode(&mutated, &params).unwrap();
        assert_ne!(commitment_a, commitment_c);

        let root = ext_a.row_root();
        let coeffs_1 = derive_coefficients(&root, params.symbols_per_row());

        let p2 = Parameters::new(8, 24, 128).unwrap();
        let p3 = Parameters::new(32, 8, 128).unwrap();
        let coeffs_2 = derive_coefficients(&root, p2.symbols_per_row());
        let coeffs_3 = derive_coefficients(&root, p3.symbols_per_row());
        assert_eq!(coeffs_1, coeffs_2);
        assert_eq!(coeffs_1, coeffs_3);
    }

    #[test]
    fn extract_symbols_and_rlc_linearity() {
        let mut chunk = [0u8; 64];
        chunk[0] = 0x01;
        chunk[32] = 0x10;
        chunk[1] = 0x02;
        chunk[33] = 0x20;
        let symbols = extract_symbols(&chunk);
        assert_eq!(symbols[0], 0x1001);
        assert_eq!(symbols[1], 0x2002);

        let (params, original) = make_original_rows(Case {
            k: 8,
            n: 8,
            row_size: 64,
            seed: 8080,
        });
        let (ext_data, _, _) = encode(&original, &params).unwrap();
        let coeffs = derive_coefficients(&ext_data.row_root(), params.symbols_per_row());

        let a = ext_data.row(0).unwrap().to_vec();
        let b = ext_data.row(1).unwrap().to_vec();
        let xor: Vec<u8> = a.iter().zip(&b).map(|(x, y)| x ^ y).collect();

        let rlc_a = compute_rlc(&a, &coeffs);
        let rlc_b = compute_rlc(&b, &coeffs);
        let rlc_xor = compute_rlc(&xor, &coeffs);
        assert_eq!(rlc_xor, rlc_a + rlc_b);
    }

    #[test]
    fn encode_in_place_and_parity_in_place_match_encode() {
        let (params, original) = make_original_rows(Case {
            k: 8,
            n: 8,
            row_size: 64,
            seed: 5151,
        });
        let (expected, commitment, rlc_orig) = encode(&original, &params).unwrap();
        let context = VerificationContext::new(&rlc_orig, &params).unwrap();

        let mut prefilled = RowMatrix::with_shape(
            vec![0u8; params.total_rows() * params.row_size],
            params.total_rows(),
            params.row_size,
        )
        .unwrap();
        let split_at = params.k * params.row_size;
        prefilled.as_row_major_mut()[..split_at].copy_from_slice(original.as_row_major());

        let (actual, commitment_b, rlc_b) = encode_in_place(prefilled.clone(), &params).unwrap();
        assert_eq!(commitment, commitment_b);
        assert_eq!(rlc_orig, rlc_b);
        assert_eq!(expected.rows().as_row_major(), actual.rows().as_row_major());

        encode_parity_in_place(&mut prefilled, &params).unwrap();
        let (from_parity, commitment_c, rlc_c) = encode_parity(prefilled, &params).unwrap();
        assert_eq!(commitment, commitment_c);
        assert_eq!(rlc_orig, rlc_c);
        assert_eq!(
            expected.rows().as_row_major(),
            from_parity.rows().as_row_major()
        );

        for idx in strategic_indices(&params) {
            let proof = from_parity.generate_row_proof(idx).unwrap();
            assert!(verify_with_context(&proof, &commitment_c, &context).unwrap());
        }
    }
}
