//! Reed-Solomon erasure coding with Merkle commitments and Random Linear Combinations.

#![forbid(unsafe_code)]

pub mod codec;
/// Cryptographic primitives: hashing, Merkle trees, and RLC coefficient derivation.
pub mod crypto;
/// Error types for the rsema1d crate.
pub mod error;
pub mod field;
/// Codec parameters (K, N, row_size) with validation.
pub mod params;

pub use error::{Error, Result};
pub use field::GF128;
pub use params::Parameters;

pub use codec::{
    ExtendedData, ExtendedRowsView, OriginalRowsView, RowInclusionProof, RowMatrix, RowProof,
    StandaloneProof, VerificationContext,
};

pub use codec::Commitment;

pub use codec::{
    create_verification_context, default_work_budget, encode, encode_in_place,
    encode_in_place_with_work_budget, encode_parity, encode_with_work_budget, reconstruct,
    verify_row_inclusion, verify_row_inclusion_proof, verify_row_with_context, verify_standalone,
    verify_standalone_proof, verify_with_context,
};

pub use codec::verify_proof;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_encode_verify_reconstruct() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let mut original = vec![0u8; params.k * params.row_size];
        for i in 0..params.k {
            original[i * params.row_size] = (i + 1) as u8;
        }
        let original = RowMatrix::with_shape(original, params.k, params.row_size).unwrap();

        let (ext_data, commitment, rlc_orig) = encode(&original, &params).unwrap();
        let context = VerificationContext::new(&rlc_orig, &params).unwrap();

        for i in 0..params.total_rows() {
            let proof = ext_data.generate_row_proof(i).unwrap();
            assert!(verify_with_context(&proof, &commitment, &context).unwrap());
        }

        let indices = vec![0usize, 2, 5, 7];
        let rows: Vec<&[u8]> = indices
            .iter()
            .map(|&i| ext_data.rows().row(i).unwrap())
            .collect();
        let reconstructed = reconstruct(&rows, &indices, &params).unwrap();
        assert_eq!(reconstructed.as_row_major(), original.as_row_major());
    }

    #[test]
    fn test_vector_1_k4_n4_rowsize64() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let mut original = vec![0u8; params.k * params.row_size];
        for i in 0..4 {
            original[(i + 1) * 64 - 1] = (i + 1) as u8;
        }
        let original = RowMatrix::with_shape(original, params.k, params.row_size).unwrap();

        let commitment = ExtendedData::generate(&original, &params).unwrap();

        let expected: [u8; 32] = [
            0xf5, 0x7f, 0xdf, 0xf8, 0x7d, 0x54, 0xf7, 0x1b, 0xc0, 0xc8, 0x60, 0x80, 0x8b, 0x04,
            0x63, 0x56, 0xc8, 0xd4, 0x85, 0x0e, 0x67, 0xb9, 0x23, 0xe0, 0x84, 0x11, 0x20, 0x8d,
            0xf0, 0x8c, 0xb5, 0xab,
        ];

        assert_eq!(
            commitment.commitment(),
            expected,
            "Test Vector 1 failed: commitment mismatch"
        );
    }

    #[test]
    fn test_vector_2_k4_n12_rowsize256() {
        let params = Parameters::new(4, 12, 256).unwrap();
        let mut original = vec![0u8; params.k * params.row_size];
        for i in 0..4 {
            original[(i + 1) * 256 - 1] = (i + 1) as u8;
        }
        let original = RowMatrix::with_shape(original, params.k, params.row_size).unwrap();

        let commitment = ExtendedData::generate(&original, &params).unwrap();

        let expected: [u8; 32] = [
            0x8a, 0xc4, 0x64, 0x40, 0x86, 0x2f, 0x28, 0x03, 0x46, 0x63, 0x5e, 0xee, 0x50, 0x75,
            0xf8, 0x1f, 0xf0, 0x4b, 0x65, 0x9f, 0xb7, 0xa8, 0x6c, 0x1e, 0x25, 0xa2, 0x8f, 0x5f,
            0x71, 0xc3, 0xf9, 0x7e,
        ];

        assert_eq!(
            commitment.commitment(),
            expected,
            "Test Vector 2 failed: commitment mismatch"
        );
    }

    #[test]
    fn encode_in_place_matches_encode() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let mut original = vec![0u8; params.k * params.row_size];
        for i in 0..params.k {
            original[(i + 1) * params.row_size - 1] = (i + 1) as u8;
        }
        let original = RowMatrix::with_shape(original, params.k, params.row_size).unwrap();

        let (ext_a, commitment_a, rlc_a) = encode(&original, &params).unwrap();

        let mut extended = RowMatrix::with_shape(
            vec![0u8; params.total_rows() * params.row_size],
            params.total_rows(),
            params.row_size,
        )
        .unwrap();
        let split_at = params.k * params.row_size;
        extended.as_row_major_mut()[..split_at].copy_from_slice(original.as_row_major());
        let (ext_b, commitment_b, rlc_b) = encode_in_place(extended, &params).unwrap();

        assert_eq!(commitment_a, commitment_b);
        assert_eq!(rlc_a, rlc_b);
        assert_eq!(ext_a.all_rows.as_row_major(), ext_b.all_rows.as_row_major());
    }
}
