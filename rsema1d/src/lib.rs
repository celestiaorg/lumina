pub mod codec;
pub mod crypto;
pub mod error;
pub mod field;
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
    create_verification_context, encode, encode_in_place, encode_parity, reconstruct,
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
        let sampled = ext_data.rows().sample(&indices).unwrap();
        let reconstructed = reconstruct(&sampled, &indices, &params).unwrap();
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
        let params = Parameters::new(3, 9, 256).unwrap();
        let mut original = vec![0u8; params.k * params.row_size];
        for i in 0..3 {
            original[(i + 1) * 256 - 1] = (i + 1) as u8;
        }
        let original = RowMatrix::with_shape(original, params.k, params.row_size).unwrap();

        let commitment = ExtendedData::generate(&original, &params).unwrap();

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

    #[test]
    fn encode_in_place_matches_encode() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let mut original = vec![0u8; params.k * params.row_size];
        for i in 0..params.k {
            original[(i + 1) * params.row_size - 1] = (i + 1) as u8;
        }
        let original = RowMatrix::with_shape(original, params.k, params.row_size).unwrap();

        let (ext_a, commitment_a, rlc_a) = encode(&original, &params).unwrap();

        let extended = RowMatrix::with_shape(
            vec![0u8; params.total_rows() * params.row_size],
            params.total_rows(),
            params.row_size,
        )
        .unwrap();
        let (ext_b, commitment_b, rlc_b) = encode_in_place(&original, extended, &params).unwrap();

        assert_eq!(commitment_a, commitment_b);
        assert_eq!(rlc_a, rlc_b);
        assert_eq!(ext_a.all_rows.as_row_major(), ext_b.all_rows.as_row_major());
    }
}
