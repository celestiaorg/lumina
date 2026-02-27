use rayon::prelude::*;
use sha2::{Digest, Sha256};

/// Binary Merkle tree (RFC 6962 compatible)
#[derive(Debug, Clone)]
pub struct MerkleTree {
    /// All nodes: leaves followed by internal nodes
    nodes: Vec<[u8; 32]>,
    /// Number of leaves (must be power of 2)
    num_leaves: usize,
}

impl MerkleTree {
    /// Build tree from raw data (hashes leaves internally)
    pub fn new(leaves: &[Vec<u8>]) -> Self {
        let num_leaves = leaves.len();
        assert!(
            num_leaves.is_power_of_two(),
            "Number of leaves must be power of 2"
        );

        // Total nodes = 2 * num_leaves - 1
        let total_nodes = 2 * num_leaves - 1;
        let mut nodes = vec![[0u8; 32]; total_nodes];

        // Hash and copy leaves (parallel for large trees)
        if num_leaves >= 64 {
            nodes[0..num_leaves]
                .par_iter_mut()
                .enumerate()
                .for_each(|(i, node)| {
                    *node = hash_leaf(&leaves[i]);
                });
        } else {
            for i in 0..num_leaves {
                nodes[i] = hash_leaf(&leaves[i]);
            }
        }

        // Build internal nodes
        let mut level_start = 0;
        let mut level_size = num_leaves;

        while level_size > 1 {
            let next_level_start = level_start + level_size;
            let parent_count = level_size / 2;

            // Parallelize internal node hashing for large levels
            if parent_count >= 64 {
                // Compute hashes in parallel, then write back
                let parent_hashes: Vec<[u8; 32]> = (0..parent_count)
                    .into_par_iter()
                    .map(|i| {
                        let left = &nodes[level_start + i * 2];
                        let right = &nodes[level_start + i * 2 + 1];
                        hash_internal(left, right)
                    })
                    .collect();

                nodes[next_level_start..next_level_start + parent_count]
                    .copy_from_slice(&parent_hashes);
            } else {
                for i in 0..parent_count {
                    let left = &nodes[level_start + i * 2];
                    let right = &nodes[level_start + i * 2 + 1];
                    nodes[next_level_start + i] = hash_internal(left, right);
                }
            }

            level_start = next_level_start;
            level_size /= 2;
        }

        Self { nodes, num_leaves }
    }

    /// Get root hash
    pub fn root(&self) -> [u8; 32] {
        self.nodes[self.nodes.len() - 1]
    }

    /// Generate Merkle proof for leaf at index
    pub fn generate_proof(&self, index: usize) -> Vec<[u8; 32]> {
        assert!(index < self.num_leaves);

        // Pre-allocate: depth = log2(num_leaves)
        let depth = (self.num_leaves as f64).log2() as usize;
        let mut proof = Vec::with_capacity(depth);

        let mut pos = index;
        let mut level_start = 0;
        let mut level_size = self.num_leaves;

        while level_size > 1 {
            // Find sibling
            let sibling_pos = if pos % 2 == 0 {
                level_start + pos + 1
            } else {
                level_start + pos - 1
            };

            proof.push(self.nodes[sibling_pos]);

            pos /= 2;
            level_start += level_size;
            level_size /= 2;
        }

        proof
    }
}

/// Hash leaf node (RFC 6962 format)
pub fn hash_leaf(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(&[0x00]);
    hasher.update(data);
    hasher.finalize().into()
}

/// Hash internal node (RFC 6962 format)
pub fn hash_internal(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(&[0x01]);
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

/// Verify Merkle proof
pub fn verify_proof(leaf: &[u8; 32], index: usize, proof: &[[u8; 32]], root: &[u8; 32]) -> bool {
    let mut current = *leaf;
    let mut pos = index;

    for sibling in proof {
        current = if pos % 2 == 0 {
            hash_internal(&current, sibling)
        } else {
            hash_internal(sibling, &current)
        };
        pos /= 2;
    }

    current == *root
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merkle_tree_4_leaves() {
        let leaves: Vec<Vec<u8>> = (0..4).map(|i| vec![i]).collect();

        let tree = MerkleTree::new(&leaves);
        let root = tree.root();

        // Test proof for each leaf
        for i in 0..4 {
            let leaf_hash = hash_leaf(&leaves[i]);
            let proof = tree.generate_proof(i);
            assert!(verify_proof(&leaf_hash, i, &proof, &root));
        }
    }

    #[test]
    fn test_merkle_proof_verification() {
        let leaves: Vec<Vec<u8>> = (0..8).map(|i| vec![i]).collect();

        let tree = MerkleTree::new(&leaves);
        let root = tree.root();

        let leaf3_hash = hash_leaf(&leaves[3]);
        let leaf2_hash = hash_leaf(&leaves[2]);

        let proof = tree.generate_proof(3);
        assert!(verify_proof(&leaf3_hash, 3, &proof, &root));

        // Wrong leaf should fail
        assert!(!verify_proof(&leaf2_hash, 3, &proof, &root));

        // Wrong index should fail
        assert!(!verify_proof(&leaf3_hash, 2, &proof, &root));
    }
}
