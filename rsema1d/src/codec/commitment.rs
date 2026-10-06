use crate::codec::padding::map_index_to_tree_position;
use crate::codec::proof::{RowInclusionProof, RowProof, StandaloneProof};
use crate::codec::rows::RowMatrix;
use crate::codec::symbols::compute_rlcs;
use crate::crypto::{derive_coefficients, hash_leaf, hash_leaf_pair, sha256_pair, MerkleTree};
use crate::error::{Error, Result};
use crate::field::GF128;
use crate::params::Parameters;
use rayon::prelude::*;

fn row_slice(rows: &RowMatrix, index: usize) -> &[u8] {
    rows.row_unchecked(index)
}

pub(crate) fn build_row_tree(rows: &RowMatrix, params: &Parameters) -> MerkleTree {
    let k_padded = params.k_padded();
    let total_padded = params.total_padded();
    let zero_row = vec![0u8; params.row_size];
    let zero_hash = hash_leaf(&zero_row);

    let row_at = |pos| {
        if pos < params.k {
            Some(row_slice(rows, pos))
        } else if (k_padded..k_padded + params.n).contains(&pos) {
            Some(row_slice(rows, params.k + pos - k_padded))
        } else {
            None
        }
    };
    let hash_pair = |pair| match (row_at(2 * pair), row_at(2 * pair + 1)) {
        (Some(a), Some(b)) => hash_leaf_pair(a, b),
        (a, b) => [
            a.map_or(zero_hash, hash_leaf),
            b.map_or(zero_hash, hash_leaf),
        ],
    };
    let pairs: Vec<[[u8; 32]; 2]> = if total_padded >= 64 {
        (0..total_padded / 2)
            .into_par_iter()
            .map(hash_pair)
            .collect()
    } else {
        (0..total_padded / 2).map(hash_pair).collect()
    };

    MerkleTree::from_leaf_hashes(pairs.into_flattened())
}

pub(crate) fn build_rlc_tree(rlc_orig: &[GF128], params: &Parameters) -> MerkleTree {
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
    /// The combined commitment hash (SHA-256 of row_root || rlc_root).
    pub commitment_hash: [u8; 32],
    /// Merkle root of the row tree.
    pub row_root: [u8; 32],
    /// Merkle root of the RLC tree.
    pub rlc_root: [u8; 32],
    /// All K+N rows (original followed by parity).
    pub all_rows: RowMatrix,
    /// RLC values for the original K rows.
    pub rlc_orig: Vec<GF128>,
    params: Parameters,
    row_tree: MerkleTree,
    rlc_tree: MerkleTree,
}

impl ExtendedData {
    /// Generate commitment from contiguous original rows.
    pub fn generate(original_rows: &RowMatrix, params: &Parameters) -> Result<Self> {
        Self::generate_with_work_budget(original_rows, params, super::default_work_budget())
    }

    /// Generate commitment using an explicit combined Leopard work-buffer budget.
    pub fn generate_with_work_budget(
        original_rows: &RowMatrix,
        params: &Parameters,
        work_budget: std::num::NonZeroUsize,
    ) -> Result<Self> {
        let original_view = original_rows.original_view(params)?;
        let all_rows = super::rs::extend_data_with_work_budget(original_view, params, work_budget)?;
        Self::generate_from_extended_rows(all_rows, params)
    }

    /// Generate commitment from contiguous already-extended rows (K+N rows).
    pub fn generate_from_extended_rows(
        mut extended_rows: RowMatrix,
        params: &Parameters,
    ) -> Result<Self> {
        extended_rows.extended_view(params)?;
        // Share encoded rows with inclusion proofs.
        extended_rows.freeze();

        let row_tree = build_row_tree(&extended_rows, params);
        let row_root = row_tree.root();

        let coefficients = derive_coefficients(&row_root, params.k, params.n, params.row_size);
        let rlc_orig = compute_rlcs(
            &extended_rows.as_row_major()[..params.k * params.row_size],
            params.row_size,
            coefficients,
        );

        let rlc_tree = build_rlc_tree(&rlc_orig, params);
        let rlc_root = rlc_tree.root();

        let commitment_hash = sha256_pair(&row_root, &rlc_root);

        Ok(Self {
            commitment_hash,
            row_root,
            rlc_root,
            all_rows: extended_rows,
            rlc_orig,
            params: *params,
            row_tree,
            rlc_tree,
        })
    }

    /// Returns the 32-byte commitment hash.
    pub fn commitment(&self) -> [u8; 32] {
        self.commitment_hash
    }

    /// Returns the Merkle root of the row tree.
    pub fn row_root(&self) -> [u8; 32] {
        self.row_root
    }

    /// Returns the Merkle root of the RLC tree.
    pub fn rlc_root(&self) -> [u8; 32] {
        self.rlc_root
    }

    /// Returns a reference to the full row matrix.
    pub fn rows(&self) -> &RowMatrix {
        &self.all_rows
    }

    /// Returns the row at `index`, or an error if out of bounds.
    pub fn row(&self, index: usize) -> Result<&[u8]> {
        if index >= self.params.total_rows() {
            return Err(Error::InvalidIndex(index, self.params.total_rows()));
        }
        self.all_rows.row(index)
    }

    /// Returns the RLC values for the original K rows.
    pub fn rlc_original(&self) -> &[GF128] {
        &self.rlc_orig
    }

    /// Returns the parameters used to generate this data.
    pub fn params(&self) -> &Parameters {
        &self.params
    }

    /// Generate lightweight row proof (works for both original and extended rows).
    pub fn generate_row_proof(&self, index: usize) -> Result<RowProof> {
        if index >= self.params.total_rows() {
            return Err(Error::InvalidIndex(index, self.params.total_rows()));
        }

        let tree_pos = map_index_to_tree_position(index, self.params.k);
        let row_proof = self.row_tree.generate_proof(tree_pos);
        Ok(RowProof {
            index,
            row: self.all_rows.row_bytes(index)?,
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
        if index >= self.params.total_rows() {
            return Err(Error::InvalidIndex(index, self.params.total_rows()));
        }
        let tree_pos = map_index_to_tree_position(index, self.params.k);
        Ok(RowInclusionProof {
            index,
            row: self.all_rows.row_bytes(index)?,
            row_proof: self.row_tree.generate_proof(tree_pos),
            rlc_root: self.rlc_root,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paired_row_tree_matches_independent_leaves() {
        for workers in [1, 4] {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(workers)
                .build()
                .unwrap();
            pool.install(|| {
                for (k, n) in [(1, 1), (3, 5), (5, 7), (32, 32), (33, 31)] {
                    for row_size in [64, 128, 256] {
                        let params = Parameters::new(k, n, row_size).unwrap();
                        let data: Vec<_> = (0..(k + n) * row_size)
                            .map(|i| ((i * 17 + i / row_size) % 251) as u8)
                            .collect();
                        let rows = RowMatrix::with_shape(data, k + n, row_size).unwrap();
                        let zero_hash = hash_leaf(&vec![0; row_size]);
                        let mut leaves: Vec<_> =
                            (0..k).map(|i| hash_leaf(row_slice(&rows, i))).collect();
                        leaves.resize(params.k_padded(), zero_hash);
                        leaves.extend((k..k + n).map(|i| hash_leaf(row_slice(&rows, i))));
                        leaves.resize(params.total_padded(), zero_hash);
                        let expected = MerkleTree::from_leaf_hashes(leaves.clone());
                        let actual = build_row_tree(&rows, &params);
                        assert_eq!(
                            actual.root(),
                            expected.root(),
                            "K={k} N={n} workers={workers}"
                        );
                        for (i, leaf) in leaves.iter().enumerate() {
                            let proof = actual.generate_proof(i);
                            assert_eq!(proof, expected.generate_proof(i));
                            assert!(crate::crypto::verify_proof(leaf, i, &proof, &actual.root()));
                        }
                    }
                }
            });
        }
    }

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
    }

    #[test]
    fn row_inclusion_proof_shares_matrix_storage() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let rows = RowMatrix::with_shape(
            vec![0; params.total_rows() * params.row_size],
            params.total_rows(),
            params.row_size,
        )
        .unwrap();
        let ext_data = ExtendedData::generate_from_extended_rows(rows, &params).unwrap();

        let proof = ext_data.generate_row_inclusion_proof(3).unwrap();

        assert_eq!(proof.row.as_ptr(), ext_data.row(3).unwrap().as_ptr());
    }
}
