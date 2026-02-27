use crate::codec::padding::{
    build_padded_rlc_array, build_padded_row_array, map_index_to_tree_position,
};
use crate::codec::proof::{RowInclusionProof, RowProof, StandaloneProof};
use crate::codec::{compute_rlc, extend_data, extend_rlcs};
use crate::crypto::{derive_coefficients, sha256, MerkleTree};
use crate::error::{Error, Result};
use crate::field::GF128;
use crate::params::Parameters;
use rayon::prelude::*;
use std::borrow::Cow;

/// Extended data with commitment and cached trees.
#[derive(Debug, Clone)]
pub struct ExtendedData {
    pub commitment_hash: [u8; 32],
    pub row_root: [u8; 32],
    pub rlc_root: [u8; 32],
    pub all_rows: Vec<Vec<u8>>,
    pub rlc_orig: Vec<GF128>,
    pub rlc_extended: Vec<GF128>,
    params: Parameters,
    row_tree: MerkleTree,
    rlc_tree: MerkleTree,
}

impl ExtendedData {
    /// Generate commitment from original data.
    pub fn generate(original_rows: &[Vec<u8>], params: &Parameters) -> Result<Self> {
        let all_rows = extend_data(original_rows, params.k, params.n, params.row_size)?;
        Self::generate_from_extended_rows(&all_rows, params)
    }

    /// Generate commitment from already-extended rows (K+N rows).
    pub fn generate_from_extended_rows(
        extended_rows: &[Vec<u8>],
        params: &Parameters,
    ) -> Result<Self> {
        if extended_rows.len() != params.total_rows() {
            return Err(Error::InvalidParameters(format!(
                "expected {} rows (K+N), got {}",
                params.total_rows(),
                extended_rows.len()
            )));
        }
        for (i, row) in extended_rows.iter().enumerate() {
            if row.len() != params.row_size {
                return Err(Error::InvalidParameters(format!(
                    "row {} has size {}, expected {}",
                    i,
                    row.len(),
                    params.row_size
                )));
            }
        }

        let all_rows = extended_rows.to_vec();

        let padded_rows = build_padded_row_array(&all_rows, params.k, params.n);
        let row_tree = MerkleTree::new(&padded_rows);
        let row_root = row_tree.root();

        let coeffs = derive_coefficients(&row_root, params.symbols_per_row());
        let rlc_orig: Vec<GF128> = all_rows[0..params.k]
            .par_iter()
            .map(|row| compute_rlc(row, &coeffs))
            .collect();
        let rlc_extended = extend_rlcs(&rlc_orig, params.k, params.n)?;

        let padded_rlcs = build_padded_rlc_array(&rlc_orig, params.k);
        let rlc_tree = MerkleTree::new(&padded_rlcs);
        let rlc_root = rlc_tree.root();

        let mut combined = Vec::with_capacity(64);
        combined.extend_from_slice(&row_root);
        combined.extend_from_slice(&rlc_root);
        let commitment_hash = sha256(&combined);

        Ok(Self {
            commitment_hash,
            row_root,
            rlc_root,
            all_rows,
            rlc_orig,
            rlc_extended,
            params: *params,
            row_tree,
            rlc_tree,
        })
    }

    pub fn commitment(&self) -> [u8; 32] {
        self.commitment_hash
    }

    pub fn row_root(&self) -> [u8; 32] {
        self.row_root
    }

    pub fn rlc_root(&self) -> [u8; 32] {
        self.rlc_root
    }

    pub fn rows(&self) -> &[Vec<u8>] {
        &self.all_rows
    }

    pub fn rlc_original(&self) -> Vec<GF128> {
        self.rlc_orig.clone()
    }

    pub fn rlc_extended(&self) -> &[GF128] {
        &self.rlc_extended
    }

    pub fn params(&self) -> &Parameters {
        &self.params
    }

    /// Generate lightweight row proof (works for both original and extended rows).
    pub fn generate_row_proof(&self, index: usize) -> Result<RowProof<'_>> {
        if index >= self.params.total_rows() {
            return Err(Error::InvalidIndex(index, self.params.total_rows()));
        }

        let tree_pos = map_index_to_tree_position(index, self.params.k);
        let row_proof = self.row_tree.generate_proof(tree_pos);
        Ok(RowProof {
            index,
            row: Cow::Borrowed(self.all_rows[index].as_slice()),
            row_proof,
        })
    }

    /// Generate standalone proof (self-contained, original rows only).
    pub fn generate_standalone_proof(&self, index: usize) -> Result<StandaloneProof> {
        if index >= self.params.k {
            return Err(Error::InvalidParameters(format!(
                "standalone proofs only available for original rows (index {} >= k {})",
                index, self.params.k
            )));
        }

        let tree_pos = map_index_to_tree_position(index, self.params.k);
        let row_proof = self.row_tree.generate_proof(tree_pos);
        let rlc_proof = self.rlc_tree.generate_proof(index);

        Ok(StandaloneProof {
            index,
            row: self.all_rows[index].clone(),
            row_proof,
            rlc_proof,
        })
    }

    /// Generate row inclusion proof for any row.
    pub fn generate_row_inclusion_proof(&self, index: usize) -> Result<RowInclusionProof> {
        let row_proof = self.generate_row_proof(index)?;
        Ok(RowInclusionProof {
            index: row_proof.index,
            row: row_proof.row.into_owned(),
            row_proof: row_proof.row_proof,
            rlc_root: self.rlc_root,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commitment_generation() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let original: Vec<Vec<u8>> = (0..params.k)
            .map(|i| {
                let mut row = vec![0u8; params.row_size];
                row[params.row_size - 1] = (i + 1) as u8;
                row
            })
            .collect();

        let ext_data = ExtendedData::generate(&original, &params).unwrap();

        assert_eq!(ext_data.commitment_hash.len(), 32);
        assert_eq!(ext_data.all_rows.len(), params.k + params.n);
        assert_eq!(ext_data.rlc_orig.len(), params.k);
        assert_eq!(ext_data.rlc_extended.len(), params.k + params.n);
    }
}
