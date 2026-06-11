use std::borrow::Cow;

/// Lightweight row proof (works for both original and extended rows).
#[derive(Debug, Clone)]
pub struct RowProof<'a> {
    /// Row index within the extended data (0..K+N).
    pub index: usize,
    /// The row bytes.
    pub row: Cow<'a, [u8]>,
    /// Merkle proof siblings from leaf to root.
    pub row_proof: Vec<[u8; 32]>,
}

/// Standalone proof (self-contained, for original rows only)
#[derive(Debug, Clone)]
pub struct StandaloneProof {
    /// Row index within the original data (0..K).
    pub index: usize,
    /// The row bytes.
    pub row: Vec<u8>,
    /// Merkle proof siblings for the row tree.
    pub row_proof: Vec<[u8; 32]>,
    /// Merkle proof siblings for the RLC tree.
    pub rlc_proof: Vec<[u8; 32]>,
}

/// Row inclusion proof (committed row membership only, no RLC equality check).
#[derive(Debug, Clone)]
pub struct RowInclusionProof {
    /// Row index within the extended data (0..K+N).
    pub index: usize,
    /// The row bytes.
    pub row: Vec<u8>,
    /// Merkle proof siblings for the row tree.
    pub row_proof: Vec<[u8; 32]>,
    /// The RLC Merkle root used in the commitment.
    pub rlc_root: [u8; 32],
}

#[cfg(test)]
mod tests {
    use crate::codec::ExtendedData;
    use crate::codec::RowMatrix;
    use crate::params::Parameters;

    #[test]
    fn test_proof_generation() {
        let params = Parameters::new(4, 4, 64).unwrap();

        let mut original = vec![0u8; params.k * params.row_size];
        for i in 0..params.k {
            original[i * params.row_size] = i as u8;
        }

        let original = RowMatrix::with_shape(original, params.k, params.row_size).unwrap();
        let ext_data = ExtendedData::generate(&original, &params).unwrap();

        let proof = ext_data.generate_row_proof(0).unwrap();
        assert_eq!(proof.index, 0);

        let proof = ext_data.generate_row_proof(5).unwrap();
        assert_eq!(proof.index, 5);
    }
}
