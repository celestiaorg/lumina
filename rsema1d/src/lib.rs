pub mod codec;
pub mod crypto;
pub mod error;
pub mod field;
pub mod params;

pub use error::{Error, Result};
pub use field::GF128;
pub use params::Parameters;

pub use codec::{ExtendedData, RowInclusionProof, RowProof, StandaloneProof, VerificationContext};

pub use codec::Commitment;

pub use codec::{
    create_verification_context, encode, encode_parity, reconstruct, verify_row_inclusion,
    verify_row_inclusion_proof, verify_row_with_context, verify_standalone,
    verify_standalone_proof, verify_with_context,
};

pub use codec::verify_proof;

#[cfg(test)]
mod tests {
    use super::*;

    /// Example 1: Basic encoding and commitment generation
    #[test]
    fn example_basic_encoding() {
        let params = Parameters::new(4, 4, 64).unwrap();

        let data: Vec<Vec<u8>> = (0..4)
            .map(|i| {
                let mut row = vec![0u8; 64];
                row[0] = i as u8;
                row
            })
            .collect();

        let (ext_data, commitment, rlc_orig) = encode(&data, &params).unwrap();

        assert_eq!(commitment.len(), 32);
        println!("Commitment: {:02x?}", &commitment[..8]);

        assert_eq!(rlc_orig.len(), 4);
        assert_eq!(ext_data.rows().len(), 8);
    }

    /// Example 2: Generate and verify standalone proof (original rows only)
    #[test]
    fn example_standalone_proof() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let data = vec![vec![0u8; 64]; 4];

        let (ext_data, commitment, _) = encode(&data, &params).unwrap();

        let proof = ext_data.generate_standalone_proof(0).unwrap();
        assert_eq!(proof.index, 0);
        assert_eq!(proof.row.len(), 64);

        verify_standalone(&proof, &commitment, &params).unwrap();

        println!("✓ Standalone proof verified for row 0");
    }

    /// Example 3: Generate and verify row proofs with context (works for all rows)
    #[test]
    fn example_row_proofs() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let data = vec![vec![1u8; 64]; 4];

        let (ext_data, commitment, rlc_orig) = encode(&data, &params).unwrap();
        let context = VerificationContext::new(&rlc_orig, &params).unwrap();

        let proof_orig = ext_data.generate_row_proof(0).unwrap();
        assert_eq!(proof_orig.index, 0);
        assert_eq!(proof_orig.row.len(), 64);
        println!("✓ Generated row proof for row 0");

        let proof_ext = ext_data.generate_row_proof(5).unwrap();
        assert_eq!(proof_ext.index, 5);
        assert_eq!(proof_ext.row.len(), 64);
        println!("✓ Generated row proof for row 5");

        assert!(verify_proof(&proof_orig, &commitment, &context).unwrap());
        assert!(verify_proof(&proof_ext, &commitment, &context).unwrap());
    }

    /// Example 4: Context-based verification (efficient for batch verification)
    #[test]
    fn example_context_based_verification() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let data = vec![vec![2u8; 64]; 4];

        let (ext_data, commitment, rlc_orig) = encode(&data, &params).unwrap();

        let context = VerificationContext::new(&rlc_orig, &params).unwrap();

        for i in 0..8 {
            let proof = ext_data.generate_row_proof(i).unwrap();
            assert!(verify_with_context(&proof, &commitment, &context).unwrap());
        }

        println!("✓ Verified all 8 rows with context");
    }

    /// Example 5: Data reconstruction from any K rows
    #[test]
    fn example_reconstruction() {
        let params = Parameters::new(4, 4, 64).unwrap();

        let original_data: Vec<Vec<u8>> = (0..4)
            .map(|i| {
                let mut row = vec![0u8; 64];
                row[0] = (i + 10) as u8;
                row
            })
            .collect();

        let (ext_data, _, _) = encode(&original_data, &params).unwrap();
        let all_rows = ext_data.rows();

        // Simulate data loss: only have rows at indices 0, 2, 5, 7
        let available_indices = vec![0, 2, 5, 7];
        let available_rows: Vec<Vec<u8>> = available_indices
            .iter()
            .map(|&i| all_rows[i].clone())
            .collect();

        let reconstructed = reconstruct(&available_rows, &available_indices, &params).unwrap();

        assert_eq!(reconstructed.len(), 4);
        assert_eq!(reconstructed, original_data);
        assert_eq!(reconstructed[0][0], 10);
        assert_eq!(reconstructed[3][0], 13);

        println!(
            "✓ Successfully reconstructed original data from indices {:?}",
            available_indices
        );
    }

    /// Example 6: Different K/N ratios
    #[test]
    fn example_different_ratios() {
        let params_high_redundancy = Parameters::new(4, 12, 64).unwrap();
        let data = vec![vec![0u8; 64]; 4];
        let (ext_data, _, _) = encode(&data, &params_high_redundancy).unwrap();
        assert_eq!(ext_data.rows().len(), 16);

        let params_low_redundancy = Parameters::new(12, 4, 64).unwrap();
        let data = vec![vec![0u8; 64]; 12];
        let (ext_data, _, _) = encode(&data, &params_low_redundancy).unwrap();
        assert_eq!(ext_data.rows().len(), 16);

        let params_equal = Parameters::new(8, 8, 128).unwrap();
        let data = vec![vec![0u8; 128]; 8];
        let (ext_data, _, _) = encode(&data, &params_equal).unwrap();
        assert_eq!(ext_data.rows().len(), 16);

        println!("✓ All K/N ratio configurations work");
    }

    /// Example 7: Complete workflow from encoding to verification
    #[test]
    fn example_complete_workflow() {
        let params = Parameters::new(3, 5, 128).unwrap();
        let original_data: Vec<Vec<u8>> = (0..3).map(|i| vec![i as u8; 128]).collect();

        let (ext_data, commitment, rlc_orig) = encode(&original_data, &params).unwrap();

        let context = VerificationContext::new(&rlc_orig, &params).unwrap();

        for i in 0..8 {
            let proof = ext_data.generate_row_proof(i).unwrap();
            assert!(verify_with_context(&proof, &commitment, &context).unwrap());
        }

        // Data availability sampling
        let sampled_indices = vec![1, 4, 6];
        for &idx in &sampled_indices {
            let proof = ext_data.generate_row_proof(idx).unwrap();
            assert!(verify_with_context(&proof, &commitment, &context).unwrap());
        }

        let indices = vec![0, 2, 4];
        let rows: Vec<Vec<u8>> = indices
            .iter()
            .map(|&i| ext_data.rows()[i].clone())
            .collect();
        let reconstructed = reconstruct(&rows, &indices, &params).unwrap();
        assert_eq!(reconstructed, original_data);

        println!("✓ Complete workflow: encode → verify → reconstruct");
    }
}

#[cfg(test)]
mod colocated_component_tests {
    #![allow(clippy::uninlined_format_args)]

    /// Test each component individually to find the issue
    use crate::crypto::{hash_internal, hash_leaf, sha256};

    #[test]
    fn test_hash_leaf_basic() {
        // Test with a simple zero-filled row
        let row = vec![0u8; 64];
        let hash = hash_leaf(&row);

        // Expected: SHA256(0x00 || [0; 64])
        let mut data = vec![0x00]; // Leaf prefix
        data.extend_from_slice(&row);
        let expected = sha256(&data);

        assert_eq!(hash, expected, "hash_leaf should use 0x00 prefix");
    }

    #[test]
    fn test_hash_internal_basic() {
        let left = [0u8; 32];
        let right = [1u8; 32];
        let hash = hash_internal(&left, &right);

        // Expected: SHA256(0x01 || left || right)
        let mut data = vec![0x01]; // Internal prefix
        data.extend_from_slice(&left);
        data.extend_from_slice(&right);
        let expected = sha256(&data);

        assert_eq!(hash, expected, "hash_internal should use 0x01 prefix");
    }

    #[test]
    fn test_gf16_mul_basic() {
        use crate::field::GF128;

        // Test GF16 multiplication through scalar_mul
        let gf = GF128 {
            limbs: [1, 0, 0, 0, 0, 0, 0, 0],
        };
        let result = gf.scalar_mul(5);

        assert_eq!(result.limbs[0], 5);
        assert_eq!(result.limbs[1], 0);
    }

    #[test]
    fn test_symbol_extraction() {
        use crate::codec::extract_symbols;

        // Create a test chunk with known pattern
        let mut chunk = [0u8; 64];
        chunk[0] = 0x01; // Low byte of symbol 0
        chunk[32] = 0x10; // High byte of symbol 0
        chunk[1] = 0x02; // Low byte of symbol 1
        chunk[33] = 0x20; // High byte of symbol 1

        let symbols = extract_symbols(&chunk);

        assert_eq!(symbols[0], 0x1001);
        assert_eq!(symbols[1], 0x2002);
    }
}

#[cfg(test)]
mod colocated_integration_tests {
    #![allow(clippy::uninlined_format_args)]

    use crate::codec::{verify_standalone, verify_with_context, VerificationContext};
    use crate::{ExtendedData, Parameters, RowProof};
    use std::borrow::Cow;

    #[test]
    fn test_full_flow() {
        let params = Parameters::new(8, 8, 128).unwrap();

        let original: Vec<Vec<u8>> = (0..params.k)
            .map(|i| {
                let mut row = vec![0xAAu8; params.row_size];
                row[0] = i as u8;
                row[params.row_size - 1] = (i * 2) as u8;
                row
            })
            .collect();

        let commitment = ExtendedData::generate(&original, &params).unwrap();
        let context = VerificationContext::new(&commitment.rlc_original(), &params).unwrap();

        for i in 0..(params.k + params.n) {
            let proof = commitment.generate_row_proof(i).unwrap();
            assert!(verify_with_context(&proof, &commitment.commitment(), &context).unwrap());
        }
    }

    #[test]
    fn test_tampered_row_fails_with_context() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let original: Vec<Vec<u8>> = (0..params.k)
            .map(|i| vec![i as u8; params.row_size])
            .collect();

        let commitment = ExtendedData::generate(&original, &params).unwrap();
        let context = VerificationContext::new(&commitment.rlc_original(), &params).unwrap();
        let proof = commitment.generate_row_proof(0).unwrap();

        let mut tampered_row = proof.row.to_vec();
        tampered_row[0] ^= 0xFF;
        let tampered = RowProof {
            index: proof.index,
            row: Cow::Owned(tampered_row),
            row_proof: proof.row_proof.clone(),
        };

        assert!(verify_with_context(&tampered, &commitment.commitment(), &context).is_err());
    }

    #[test]
    fn test_tampered_standalone_fails() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let original: Vec<Vec<u8>> = (0..params.k)
            .map(|i| vec![i as u8; params.row_size])
            .collect();

        let commitment = ExtendedData::generate(&original, &params).unwrap();
        let mut proof = commitment.generate_standalone_proof(0).unwrap();
        proof.row[0] ^= 0xFF;

        assert!(verify_standalone(&proof, &commitment.commitment(), &params).is_err());
    }
}

#[cfg(test)]
mod colocated_various_kn_tests {
    #![allow(clippy::uninlined_format_args)]

    use crate::{ExtendedData, Parameters, VerificationContext};
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    fn generate_test_data(k: usize, row_size: usize, seed: u64) -> Vec<Vec<u8>> {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        (0..k)
            .map(|_| (0..row_size).map(|_| rng.gen::<u8>()).collect())
            .collect()
    }

    #[test]
    fn test_various_k_n_combinations() {
        let test_configs = vec![
            // (name, k, n, row_size)
            ("k=n small", 4, 4, 64),
            ("k=n medium", 16, 16, 128),
            ("k=n large", 64, 64, 256),
            // k < n (more recovery chunks than original)
            ("k<n 1:2", 4, 8, 64),
            ("k<n 1:3", 4, 12, 64),
            ("k<n 1:4", 4, 16, 64),
            ("k<n 2:3", 8, 12, 64),
            ("k<n medium", 32, 96, 128),
            // k > n (more original chunks than recovery)
            ("k>n 2:1", 8, 4, 64),
            ("k>n 3:1", 12, 4, 64),
            ("k>n 4:1", 16, 4, 64),
            ("k>n 3:2", 12, 8, 64),
            ("k>n medium", 96, 32, 128),
            // Edge cases
            ("k=1 n=1", 1, 1, 64),
            ("k=1 n=3", 1, 3, 64),
            ("k=3 n=1", 3, 1, 64),
            ("k=2 n=2", 2, 2, 64),
        ];

        for (name, k, n, row_size) in &test_configs {
            // Create parameters
            let params = Parameters::new(*k, *n, *row_size)
                .expect(&format!("Failed to create parameters for {}", name));

            // Generate test data
            let data = generate_test_data(*k, *row_size, 12345);

            // Generate commitment
            let commitment = ExtendedData::generate(&data, &params)
                .expect(&format!("Failed to generate commitment for {}", name));
            let context = VerificationContext::new(&commitment.rlc_original(), &params).expect(
                &format!("Failed to create verification context for {}", name),
            );

            // Verify we can generate proofs for all indices
            for i in 0..(*k + *n) {
                let proof = commitment.generate_row_proof(i).expect(&format!(
                    "Failed to generate proof at index {} for {}",
                    i, name
                ));

                // Verify the proof
                assert!(
                    crate::verify_proof(&proof, &commitment.commitment(), &context).expect(
                        &format!("Failed to verify proof at index {} for {}", i, name)
                    ),
                    "Proof verification returned false at index {} for {}",
                    i,
                    name
                );
            }
        }
    }

    #[test]
    fn test_reconstruction_various_k_n() {
        let test_configs = vec![
            // (name, k, n, row_size)
            ("k=n", 8, 8, 64),
            ("k<n", 4, 12, 64),
            ("k>n", 12, 4, 64),
        ];

        for (name, k, n, row_size) in &test_configs {
            let params = Parameters::new(*k, *n, *row_size)
                .expect(&format!("Failed to create parameters for {}", name));

            let original_data = generate_test_data(*k, *row_size, 54321);

            // Encode
            let commitment = ExtendedData::generate(&original_data, &params)
                .expect(&format!("Failed to encode for {}", name));

            // Get all extended rows (original + recovery)
            let extended_rows = commitment.all_rows.clone();

            // Test reconstruction with different subsets of k rows
            // Test 1: Use first k rows (all original)
            let indices: Vec<usize> = (0..*k).collect();
            let rows: Vec<Vec<u8>> = indices.iter().map(|&i| extended_rows[i].clone()).collect();

            let reconstructed = crate::codec::reconstruct_data(&rows, &indices, *k, *n).expect(
                &format!("Failed to reconstruct with first {} rows for {}", k, name),
            );

            assert_eq!(
                reconstructed, original_data,
                "Reconstruction failed for {} using first k rows",
                name
            );

            // Test 2: Use mix of original and recovery rows
            if *n > 0 {
                let num_from_original = *k / 2;
                let num_from_recovery = *k - num_from_original;

                // Make sure we don't go beyond available recovery rows
                let num_recovery_available = (*n).min(num_from_recovery);

                let mut mixed_indices: Vec<usize> = (0..num_from_original).collect(); // From original
                if num_recovery_available > 0 {
                    mixed_indices.extend((*k..*k + num_recovery_available).collect::<Vec<_>>());
                    // From recovery
                }

                // If we don't have enough recovery rows, take more from original
                while mixed_indices.len() < *k {
                    let next_orig = mixed_indices.len() - num_recovery_available;
                    if next_orig < *k {
                        mixed_indices.push(next_orig);
                    } else {
                        break;
                    }
                }

                let mixed_rows: Vec<Vec<u8>> = mixed_indices
                    .iter()
                    .map(|&i| extended_rows[i].clone())
                    .collect();

                let reconstructed =
                    crate::codec::reconstruct_data(&mixed_rows, &mixed_indices, *k, *n).expect(
                        &format!("Failed to reconstruct with mixed rows for {}", name),
                    );

                assert_eq!(
                    reconstructed, original_data,
                    "Reconstruction failed for {} using mixed rows",
                    name
                );
            }
        }
    }
}

#[cfg(test)]
mod colocated_test_vectors_tests {
    #![allow(clippy::uninlined_format_args)]

    use crate::{ExtendedData, Parameters};

    #[test]
    fn test_vector_1_k4_n4_rowsize64() {
        // Test Vector 1 from spec: K=4, N=4, rowSize=64
        let params = Parameters::new(4, 4, 64).unwrap();

        // Input data: 4 rows × 64 bytes each, all zeros except last byte
        let original: Vec<Vec<u8>> = (0..4)
            .map(|i| {
                let mut row = vec![0u8; 64];
                row[63] = (i + 1) as u8; // Last byte is 1, 2, 3, 4
                row
            })
            .collect();

        let commitment = ExtendedData::generate(&original, &params).unwrap();

        // Expected commitment from spec
        let expected: [u8; 32] = [
            0x9f, 0x63, 0x75, 0x74, 0xec, 0xb6, 0x78, 0x28, 0xc5, 0xce, 0x75, 0x89, 0xa0, 0xa6,
            0xce, 0x13, 0x9c, 0xca, 0xd3, 0xbe, 0xa8, 0xe9, 0x2d, 0x22, 0xd9, 0xe2, 0x8f, 0xde,
            0x83, 0xa9, 0x05, 0xe7,
        ];

        assert_eq!(
            commitment.commitment(),
            expected,
            "Test Vector 1 failed: commitment mismatch"
        );
    }

    #[test]
    fn test_vector_2_k3_n9_rowsize256() {
        // Test Vector 2 from spec: K=3, N=9, rowSize=256
        let params = Parameters::new(3, 9, 256).unwrap();

        // Input data: 3 rows × 256 bytes each, all zeros except last byte
        let original: Vec<Vec<u8>> = (0..3)
            .map(|i| {
                let mut row = vec![0u8; 256];
                row[255] = (i + 1) as u8; // Last byte is 1, 2, 3
                row
            })
            .collect();

        let commitment = ExtendedData::generate(&original, &params).unwrap();

        // Expected commitment from spec
        let expected: [u8; 32] = [
            0x2d, 0x67, 0xc1, 0x3a, 0xa6, 0xa5, 0xc0, 0xbe, 0x41, 0xb7, 0xe8, 0x4f, 0x36, 0x18,
            0x85, 0x62, 0xc6, 0x4f, 0xf3, 0x54, 0x7c, 0xe7, 0x4f, 0xfd, 0x41, 0x0a, 0xb1, 0xaf,
            0xa7, 0x89, 0x7f, 0x22,
        ];

        assert_eq!(
            commitment.commitment(),
            expected,
            "Test Vector 2 failed: commitment mismatch"
        );
    }
}

#[cfg(test)]
mod colocated_verify_test_vectors_tests {
    #![allow(clippy::uninlined_format_args)]

    /// Verify test vector hex encoding
    #[test]
    fn verify_test_vector_hex() {
        // Test Vector 1 expected hex (from comment in test_vectors.rs)
        let hex_str = "9f637574ecb67828c5ce7589a0a6ce139ccad3bea8e92d22d9e28fde83a905e7";

        // Convert to bytes
        let mut bytes = [0u8; 32];
        for i in 0..32 {
            bytes[i] = u8::from_str_radix(&hex_str[i * 2..i * 2 + 2], 16).unwrap();
        }

        // Expected bytes from test_vectors.rs
        let expected: [u8; 32] = [
            0x9f, 0x63, 0x75, 0x74, 0xec, 0xb6, 0x78, 0x28, 0xc5, 0xce, 0x75, 0x89, 0xa0, 0xa6,
            0xce, 0x13, 0x9c, 0xca, 0xd3, 0xbe, 0xa8, 0xe9, 0x2d, 0x22, 0xd9, 0xe2, 0x8f, 0xde,
            0x83, 0xa9, 0x05, 0xe7,
        ];

        assert_eq!(bytes, expected, "Hex string doesn't match byte array!");
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
mod wasm_encoding_tests {
    use super::*;
    use wasm_bindgen_test::wasm_bindgen_test;

    #[wasm_bindgen_test]
    fn wasm_encode_basic_shape() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let data: Vec<Vec<u8>> = (0..params.k)
            .map(|i| {
                let mut row = vec![0u8; params.row_size];
                row[0] = i as u8;
                row
            })
            .collect();

        let (ext_data, commitment, rlc_orig) = encode(&data, &params).unwrap();

        assert_eq!(ext_data.rows().len(), params.k + params.n);
        assert_eq!(rlc_orig.len(), params.k);
        assert_eq!(commitment.len(), 32);
    }

    #[wasm_bindgen_test]
    fn wasm_encode_parity_matches_encode() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let data: Vec<Vec<u8>> = (0..params.k)
            .map(|i| {
                let mut row = vec![0u8; params.row_size];
                row[params.row_size - 1] = (i as u8) + 1;
                row
            })
            .collect();

        let (ext1, c1, r1) = encode(&data, &params).unwrap();
        let (ext2, c2, r2) = encode_parity(ext1.rows(), &params).unwrap();

        assert_eq!(c1, c2);
        assert_eq!(r1, r2);
        assert_eq!(ext1.rows(), ext2.rows());
    }
}
