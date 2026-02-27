//! Reed-Solomon encoding/decoding with RLC integration

mod commitment;
mod padding;
mod proof;
mod reconstruct;
mod rs;
mod symbols;
mod verification;

use crate::error::Result;
use crate::field::GF128;
use crate::params::Parameters;

pub use commitment::ExtendedData;
pub use padding::{build_padded_rlc_array, build_padded_row_array, map_index_to_tree_position};
pub use proof::{RowInclusionProof, RowProof, StandaloneProof};
pub use reconstruct::reconstruct_data;
pub use rs::{extend_data, extend_rlcs, pack_gf128_to_shard, unpack_shard_to_gf128};
pub use symbols::{compute_rlc, extract_symbols};
pub use verification::{
    create_verification_context, verify_proof, verify_row_inclusion, verify_row_inclusion_proof,
    verify_row_with_context, verify_standalone, verify_standalone_proof, verify_with_context,
    VerificationContext,
};

pub type Commitment = [u8; 32];

/// Encode data and return extended data, commitment, and original RLCs
///
/// # Example
/// ```ignore
/// use rsema1d::{encode, Parameters};
///
/// let params = Parameters::new(4, 4, 64)?;
/// let data = vec![vec![0u8; 64]; 4];
/// let (ext_data, commitment, rlc_orig) = encode(&data, &params)?;
/// ```
pub fn encode(
    data: &[Vec<u8>],
    params: &Parameters,
) -> Result<(ExtendedData, Commitment, Vec<GF128>)> {
    let ext_data = ExtendedData::generate(data, params)?;
    let commitment = ext_data.commitment();
    let rlc_orig = ext_data.rlc_original();
    Ok((ext_data, commitment, rlc_orig))
}

/// Compute commitment/proofs from already-extended rows (K+N rows).
pub fn encode_parity(
    extended_rows: &[Vec<u8>],
    params: &Parameters,
) -> Result<(ExtendedData, Commitment, Vec<GF128>)> {
    let ext_data = ExtendedData::generate_from_extended_rows(extended_rows, params)?;
    let commitment = ext_data.commitment();
    let rlc_orig = ext_data.rlc_original();
    Ok((ext_data, commitment, rlc_orig))
}

/// Reconstruct original data from any K rows
///
/// # Example
/// ```ignore
/// use rsema1d::{reconstruct, Parameters};
///
/// let params = Parameters::new(4, 4, 64)?;
/// let rows = vec![/* any K rows */];
/// let indices = vec![0, 2, 4, 6];  // Which indices these rows are from
/// let original = reconstruct(&rows, &indices, &params)?;
/// ```
pub fn reconstruct(
    rows: &[Vec<u8>],
    indices: &[usize],
    params: &Parameters,
) -> Result<Vec<Vec<u8>>> {
    reconstruct_data(rows, indices, params.k, params.n)
}

#[cfg(test)]
mod colocated_comprehensive_codec_tests {
    #![allow(clippy::uninlined_format_args)]

    use crate::{encode_parity, verify_row_inclusion, ExtendedData, Parameters};

    // Test cases matching Go implementation
    struct TestCase {
        name: &'static str,
        k: usize,
        n: usize,
        row_size: usize,
    }

    const TEST_CASES: &[TestCase] = &[
        // Power of 2 cases
        TestCase {
            name: "1:1 small k=4 n=4",
            k: 4,
            n: 4,
            row_size: 64,
        },
        TestCase {
            name: "1:3 small k=4 n=12",
            k: 4,
            n: 12,
            row_size: 64,
        },
        TestCase {
            name: "1:1 medium k=8 n=8",
            k: 8,
            n: 8,
            row_size: 256,
        },
        TestCase {
            name: "1:3 medium k=8 n=24",
            k: 8,
            n: 24,
            row_size: 256,
        },
        TestCase {
            name: "1:1 large k=16 n=16",
            k: 16,
            n: 16,
            row_size: 512,
        },
        TestCase {
            name: "1:3 large k=16 n=48",
            k: 16,
            n: 48,
            row_size: 512,
        },
        // Arbitrary K and N cases
        TestCase {
            name: "arbitrary k=3 n=5",
            k: 3,
            n: 5,
            row_size: 64,
        },
        TestCase {
            name: "arbitrary k=5 n=7",
            k: 5,
            n: 7,
            row_size: 128,
        },
        TestCase {
            name: "arbitrary k=7 n=9",
            k: 7,
            n: 9,
            row_size: 128,
        },
        TestCase {
            name: "arbitrary k=10 n=15",
            k: 10,
            n: 15,
            row_size: 256,
        },
        TestCase {
            name: "arbitrary k=13 n=19",
            k: 13,
            n: 19,
            row_size: 256,
        },
        TestCase {
            name: "arbitrary k=17 n=31",
            k: 17,
            n: 31,
            row_size: 512,
        },
        TestCase {
            name: "arbitrary k=100 n=150",
            k: 100,
            n: 150,
            row_size: 512,
        },
        TestCase {
            name: "arbitrary k=127 n=129",
            k: 127,
            n: 129,
            row_size: 512,
        },
    ];

    fn make_test_data(k: usize, row_size: usize) -> Vec<Vec<u8>> {
        (0..k)
            .map(|i| (0..row_size).map(|j| ((i + j) % 256) as u8).collect())
            .collect()
    }

    fn get_test_row_indices(k: usize, n: usize) -> Vec<usize> {
        vec![
            0,         // First original row
            k - 1,     // Last original row
            k,         // First extended row
            k + n - 1, // Last extended row
            k / 2,     // Middle original row
            k + n / 2, // Middle extended row
        ]
    }

    #[test]
    fn test_encode_and_generate_proofs() {
        for tc in TEST_CASES {
            let params = Parameters::new(tc.k, tc.n, tc.row_size).unwrap();
            let data = make_test_data(tc.k, tc.row_size);

            let commitment = ExtendedData::generate(&data, &params).unwrap();

            // Verify commitment is 32 bytes
            assert_eq!(commitment.commitment().len(), 32);

            // Test proof generation for various indices
            for &index in &get_test_row_indices(tc.k, tc.n) {
                let proof = commitment.generate_row_proof(index);
                assert!(
                    proof.is_ok(),
                    "Failed to generate proof for index {} in {}",
                    index,
                    tc.name
                );
            }
        }
    }

    #[test]
    fn test_commitment_determinism() {
        for tc in TEST_CASES {
            let params = Parameters::new(tc.k, tc.n, tc.row_size).unwrap();
            let data = make_test_data(tc.k, tc.row_size);

            let commitment1 = ExtendedData::generate(&data, &params).unwrap();
            let commitment2 = ExtendedData::generate(&data, &params).unwrap();

            assert_eq!(
                commitment1.commitment(),
                commitment2.commitment(),
                "Commitment not deterministic for {}",
                tc.name
            );
        }
    }

    #[test]
    fn test_different_data_different_commitment() {
        for tc in TEST_CASES.iter().take(5) {
            // Test first 5 cases for speed
            let params = Parameters::new(tc.k, tc.n, tc.row_size).unwrap();

            let data1 = make_test_data(tc.k, tc.row_size);
            let mut data2 = data1.clone();
            data2[0][0] ^= 1; // Change one byte

            let commitment1 = ExtendedData::generate(&data1, &params).unwrap();
            let commitment2 = ExtendedData::generate(&data2, &params).unwrap();

            assert_ne!(
                commitment1.commitment(),
                commitment2.commitment(),
                "Different data produced same commitment for {}",
                tc.name
            );
        }
    }

    #[test]
    fn test_row_count() {
        for tc in TEST_CASES {
            let params = Parameters::new(tc.k, tc.n, tc.row_size).unwrap();
            let data = make_test_data(tc.k, tc.row_size);

            let commitment = ExtendedData::generate(&data, &params).unwrap();

            assert_eq!(
                commitment.all_rows.len(),
                tc.k + tc.n,
                "Wrong number of rows for {}",
                tc.name
            );

            assert_eq!(
                commitment.rlc_extended.len(),
                tc.k + tc.n,
                "Wrong number of RLC values for {}",
                tc.name
            );

            assert_eq!(
                commitment.rlc_orig.len(),
                tc.k,
                "Wrong number of original RLC values for {}",
                tc.name
            );
        }
    }

    #[test]
    fn test_original_rows_preserved() {
        for tc in TEST_CASES.iter().take(5) {
            let params = Parameters::new(tc.k, tc.n, tc.row_size).unwrap();
            let data = make_test_data(tc.k, tc.row_size);

            let commitment = ExtendedData::generate(&data, &params).unwrap();

            // Check that original rows are preserved
            for i in 0..tc.k {
                assert_eq!(
                    commitment.all_rows[i], data[i],
                    "Original row {} not preserved in {}",
                    i, tc.name
                );
            }
        }
    }

    #[test]
    fn test_encode_parity_matches_encode() {
        for tc in TEST_CASES.iter().take(8) {
            let params = Parameters::new(tc.k, tc.n, tc.row_size).unwrap();
            let data = make_test_data(tc.k, tc.row_size);

            let ext1 = ExtendedData::generate(&data, &params).unwrap();
            let (ext2, commitment2, rlc_orig2) = encode_parity(&ext1.all_rows, &params).unwrap();

            assert_eq!(
                ext1.commitment(),
                commitment2,
                "commitment mismatch for {}",
                tc.name
            );
            assert_eq!(
                ext1.rlc_orig, rlc_orig2,
                "rlc_orig mismatch for {}",
                tc.name
            );
            assert_eq!(ext1.all_rows, ext2.all_rows, "row mismatch for {}", tc.name);
        }
    }

    #[test]
    fn test_row_inclusion_proof() {
        for tc in TEST_CASES.iter().take(8) {
            let params = Parameters::new(tc.k, tc.n, tc.row_size).unwrap();
            let data = make_test_data(tc.k, tc.row_size);
            let commitment = ExtendedData::generate(&data, &params).unwrap();

            for index in get_test_row_indices(tc.k, tc.n) {
                let proof = commitment.generate_row_inclusion_proof(index).unwrap();
                assert!(
                    verify_row_inclusion(&proof, &commitment.commitment(), &params).unwrap(),
                    "row inclusion proof failed for index {} in {}",
                    index,
                    tc.name
                );
            }
        }
    }
}

#[cfg(test)]
mod colocated_spec_compliance_tests {
    #![allow(clippy::uninlined_format_args)]

    /// Test compliance with SPEC.md test vector 1
    use crate::{
        codec::{build_padded_rlc_array, build_padded_row_array, compute_rlc, extend_data},
        crypto::{derive_coefficients, sha256, MerkleTree},
        ExtendedData, Parameters,
    };

    #[test]
    fn spec_test_vector_1_step_by_step() {
        // Input data from spec: K=4, N=4, rowSize=64
        let params = Parameters::new(4, 4, 64).unwrap();
        let original: Vec<Vec<u8>> = (0..4)
            .map(|i| {
                let mut row = vec![0u8; 64];
                row[63] = (i + 1) as u8;
                row
            })
            .collect();

        // Data Extension (Spec 3.2)
        let extended = extend_data(&original, params.k, params.n, params.row_size).unwrap();

        // Build Row Tree (Spec 3.3.1)
        let padded_rows = build_padded_row_array(&extended, params.k, params.n);
        let row_tree = MerkleTree::new(&padded_rows);
        let row_root = row_tree.root();

        // Derive RLC Coefficients (Spec 3.3.2)
        let coeffs = derive_coefficients(&row_root, params.symbols_per_row());

        // Compute Original RLCs (Spec 3.3.3)
        let mut rlc_orig = Vec::with_capacity(params.k);
        for row in original.iter().take(params.k) {
            let rlc = compute_rlc(row, &coeffs);
            rlc_orig.push(rlc);
        }

        // Build RLC Tree (Spec 3.3.5)
        let padded_rlcs = build_padded_rlc_array(&rlc_orig, params.k);
        let rlc_tree = MerkleTree::new(&padded_rlcs);
        let rlc_root = rlc_tree.root();

        // Final Commitment (Spec 3.3.6)
        let mut data = Vec::with_capacity(64);
        data.extend_from_slice(&row_root);
        data.extend_from_slice(&rlc_root);
        let commitment = sha256(&data);

        let expected = ExtendedData::generate(&original, &params)
            .unwrap()
            .commitment();

        assert_eq!(commitment, expected, "Commitment mismatch");
    }
}
