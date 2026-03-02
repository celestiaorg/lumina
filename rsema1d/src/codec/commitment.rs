use crate::codec::padding::map_index_to_tree_position;
use crate::codec::proof::{RowInclusionProof, RowProof, StandaloneProof};
use crate::codec::rows::RowMatrix;
use crate::codec::{compute_rlc, extend_data, extend_rlcs};
use crate::crypto::{derive_coefficients, hash_leaf, sha256, MerkleTree};
use crate::error::{Error, Result};
use crate::field::GF128;
use crate::params::Parameters;
use rayon::prelude::*;
use std::borrow::Cow;

fn row_slice(rows: &RowMatrix, index: usize) -> &[u8] {
    rows.row_unchecked(index)
}

fn build_row_tree(rows: &RowMatrix, params: &Parameters) -> MerkleTree {
    let k_padded = params.k_padded();
    let total_padded = params.total_padded();
    let zero_row = vec![0u8; params.row_size];
    let zero_hash = hash_leaf(&zero_row);

    let leaf_hashes: Vec<[u8; 32]> = if total_padded >= 64 {
        (0..total_padded)
            .into_par_iter()
            .map(|pos| {
                if pos < params.k {
                    hash_leaf(row_slice(rows, pos))
                } else if pos < k_padded {
                    zero_hash
                } else if pos < k_padded + params.n {
                    let row_index = params.k + (pos - k_padded);
                    hash_leaf(row_slice(rows, row_index))
                } else {
                    zero_hash
                }
            })
            .collect()
    } else {
        (0..total_padded)
            .map(|pos| {
                if pos < params.k {
                    hash_leaf(row_slice(rows, pos))
                } else if pos < k_padded {
                    zero_hash
                } else if pos < k_padded + params.n {
                    let row_index = params.k + (pos - k_padded);
                    hash_leaf(row_slice(rows, row_index))
                } else {
                    zero_hash
                }
            })
            .collect()
    };

    MerkleTree::from_leaf_hashes(leaf_hashes)
}

fn build_rlc_tree(rlc_orig: &[GF128], params: &Parameters) -> MerkleTree {
    let k_padded = params.k_padded();
    let zero_rlc = [0u8; 16];
    let zero_hash = hash_leaf(&zero_rlc);

    let leaf_hashes: Vec<[u8; 32]> = (0..k_padded)
        .map(|i| {
            if i < params.k {
                hash_leaf(&rlc_orig[i].to_bytes())
            } else {
                zero_hash
            }
        })
        .collect();
    MerkleTree::from_leaf_hashes(leaf_hashes)
}

/// Extended data with commitment and cached trees.
#[derive(Debug, Clone)]
pub struct ExtendedData {
    pub commitment_hash: [u8; 32],
    pub row_root: [u8; 32],
    pub rlc_root: [u8; 32],
    pub all_rows: RowMatrix,
    pub rlc_orig: Vec<GF128>,
    pub rlc_extended: Vec<GF128>,
    params: Parameters,
    row_tree: MerkleTree,
    rlc_tree: MerkleTree,
}

impl ExtendedData {
    /// Generate commitment from contiguous original rows.
    pub fn generate(original_rows: &RowMatrix, params: &Parameters) -> Result<Self> {
        let original_view = original_rows.original_view(params)?;
        let all_rows = extend_data(original_view, params)?;
        Self::generate_from_extended_rows(all_rows, params)
    }

    /// Generate commitment from contiguous already-extended rows (K+N rows).
    pub fn generate_from_extended_rows(
        extended_rows: RowMatrix,
        params: &Parameters,
    ) -> Result<Self> {
        extended_rows.extended_view(params)?;

        let row_tree = build_row_tree(&extended_rows, params);
        let row_root = row_tree.root();

        let coeffs = derive_coefficients(&row_root, params.symbols_per_row());
        let rlc_orig: Vec<GF128> = (0..params.k)
            .into_par_iter()
            .map(|i| compute_rlc(row_slice(&extended_rows, i), &coeffs))
            .collect();
        let rlc_extended = extend_rlcs(&rlc_orig, params.k, params.n)?;

        let rlc_tree = build_rlc_tree(&rlc_orig, params);
        let rlc_root = rlc_tree.root();

        let mut combined = Vec::with_capacity(64);
        combined.extend_from_slice(&row_root);
        combined.extend_from_slice(&rlc_root);
        let commitment_hash = sha256(&combined);

        Ok(Self {
            commitment_hash,
            row_root,
            rlc_root,
            all_rows: extended_rows,
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

    pub fn rows(&self) -> &RowMatrix {
        &self.all_rows
    }

    pub fn row(&self, index: usize) -> Result<&[u8]> {
        if index >= self.params.total_rows() {
            return Err(Error::InvalidIndex(index, self.params.total_rows()));
        }
        self.all_rows.row(index)
    }

    pub fn rlc_original(&self) -> &[GF128] {
        &self.rlc_orig
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
            row: Cow::Borrowed(self.row(index)?),
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
            row: self.row(index)?.to_vec(),
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
        let mut original = vec![0u8; params.k * params.row_size];
        for i in 0..params.k {
            original[(i + 1) * params.row_size - 1] = (i + 1) as u8;
        }
        let original = RowMatrix::with_shape(original, params.k, params.row_size).unwrap();

        let ext_data = ExtendedData::generate(&original, &params).unwrap();

        assert_eq!(ext_data.commitment_hash.len(), 32);
        assert_eq!(
            ext_data.all_rows.as_row_major().len(),
            (params.k + params.n) * params.row_size
        );
        assert_eq!(ext_data.rlc_orig.len(), params.k);
        assert_eq!(ext_data.rlc_extended.len(), params.k + params.n);
    }
}
