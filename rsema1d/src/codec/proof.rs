use std::borrow::Cow;

/// Lightweight row proof (works for both original and extended rows).
#[derive(Debug, Clone)]
pub struct RowProof<'a> {
    pub index: usize,
    pub row: Cow<'a, [u8]>,
    pub row_proof: Vec<[u8; 32]>,
}

/// Standalone proof (self-contained, for original rows only)
#[derive(Debug, Clone)]
pub struct StandaloneProof {
    pub index: usize,
    pub row: Vec<u8>,
    pub row_proof: Vec<[u8; 32]>,
    pub rlc_proof: Vec<[u8; 32]>,
}

/// Row inclusion proof (committed row membership only, no RLC equality check).
#[derive(Debug, Clone)]
pub struct RowInclusionProof {
    pub index: usize,
    pub row: Vec<u8>,
    pub row_proof: Vec<[u8; 32]>,
    pub rlc_root: [u8; 32],
}

#[cfg(test)]
mod tests {
    use crate::codec::ExtendedData;
    use crate::params::Parameters;

    #[test]
    fn test_proof_generation() {
        let params = Parameters::new(4, 4, 64).unwrap();

        let original: Vec<Vec<u8>> = (0..params.k)
            .map(|i| {
                let mut row = vec![0u8; params.row_size];
                row[0] = i as u8;
                row
            })
            .collect();

        let ext_data = ExtendedData::generate(&original, &params).unwrap();

        let proof = ext_data.generate_row_proof(0).unwrap();
        assert_eq!(proof.index, 0);

        let proof = ext_data.generate_row_proof(5).unwrap();
        assert_eq!(proof.index, 5);
    }
}
