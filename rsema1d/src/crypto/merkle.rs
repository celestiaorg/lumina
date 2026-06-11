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
        let leaf_hashes: Vec<[u8; 32]> = leaves.iter().map(|leaf| hash_leaf(leaf)).collect();
        Self::from_leaf_hashes(leaf_hashes)
    }

    /// Build tree from pre-hashed leaves.
    pub fn from_leaf_hashes(leaf_hashes: Vec<[u8; 32]>) -> Self {
        let num_leaves = leaf_hashes.len();
        assert!(
            num_leaves.is_power_of_two(),
            "Number of leaves must be power of 2"
        );

        // Total nodes = 2 * num_leaves - 1
        let total_nodes = 2 * num_leaves - 1;
        let mut nodes = vec![[0u8; 32]; total_nodes];

        // Copy pre-hashed leaves.
        nodes[0..num_leaves].copy_from_slice(&leaf_hashes);

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
            let sibling_pos = if pos.is_multiple_of(2) {
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
    hasher.update([0x00]);
    hasher.update(data);
    hasher.finalize().into()
}

/// Hash internal node (RFC 6962 format)
pub fn hash_internal(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update([0x01]);
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

/// Verify Merkle proof
pub fn verify_proof(leaf: &[u8; 32], index: usize, proof: &[[u8; 32]], root: &[u8; 32]) -> bool {
    let mut current = *leaf;
    let mut pos = index;

    for sibling in proof {
        current = if pos.is_multiple_of(2) {
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

    fn make_leaves(count: usize, leaf_size: usize) -> Vec<Vec<u8>> {
        (0..count)
            .map(|i| {
                let mut leaf = vec![0u8; leaf_size];
                if leaf_size > 0 {
                    leaf[0] = i as u8;
                }
                leaf
            })
            .collect()
    }

    #[test]
    fn new_tree_valid_and_invalid_counts() {
        for n in [1usize, 2, 4, 8, 64] {
            let leaves = make_leaves(n, 8);
            let tree = MerkleTree::new(&leaves);
            assert_ne!(tree.root(), [0u8; 32]);
        }

        for n in [0usize, 3, 5] {
            let leaves = make_leaves(n, 8);
            let panicked = std::panic::catch_unwind(|| {
                let _ = MerkleTree::new(&leaves);
            });
            assert!(panicked.is_err(), "expected panic for invalid n={}", n);
        }
    }

    #[test]
    fn root_is_deterministic_and_sensitive() {
        for n in [1usize, 2, 4, 8, 16, 32, 64, 128] {
            let leaves = make_leaves(n, 16);
            let tree_a = MerkleTree::new(&leaves);
            let tree_b = MerkleTree::new(&leaves);
            assert_eq!(tree_a.root(), tree_b.root());

            let mut modified = leaves.clone();
            modified[n - 1][0] ^= 0x01;
            let tree_c = MerkleTree::new(&modified);
            assert_ne!(tree_a.root(), tree_c.root());
        }
    }

    #[test]
    fn proof_generation_and_verification_for_all_leaves() {
        let leaves = make_leaves(16, 16);
        let tree = MerkleTree::new(&leaves);
        let root = tree.root();

        for (i, leaf) in leaves.iter().enumerate() {
            let proof = tree.generate_proof(i);
            assert_eq!(proof.len(), 4);
            let leaf_hash = hash_leaf(leaf);
            assert!(verify_proof(&leaf_hash, i, &proof, &root));
        }
    }

    #[test]
    fn generate_proof_panics_for_out_of_bounds_index() {
        let leaves = make_leaves(8, 8);
        let tree = MerkleTree::new(&leaves);

        for bad in [8usize, 100] {
            let panicked = std::panic::catch_unwind(|| {
                let _ = tree.generate_proof(bad);
            });
            assert!(panicked.is_err(), "expected panic for bad index={}", bad);
        }
    }

    #[test]
    fn wrong_index_and_wrong_leaf_fail_verification() {
        let leaves = make_leaves(16, 8);
        let tree = MerkleTree::new(&leaves);
        let root = tree.root();

        for (i, leaf) in leaves.iter().enumerate() {
            let proof = tree.generate_proof(i);
            let leaf_hash = hash_leaf(leaf);
            assert!(verify_proof(&leaf_hash, i, &proof, &root));

            let wrong_index = (i + 1) % leaves.len();
            assert!(!verify_proof(&leaf_hash, wrong_index, &proof, &root));

            let mut wrong_leaf = leaf.clone();
            wrong_leaf[0] ^= 0x01;
            let wrong_leaf_hash = hash_leaf(&wrong_leaf);
            assert!(!verify_proof(&wrong_leaf_hash, i, &proof, &root));
        }
    }

    #[test]
    fn hash_pair_is_deterministic_and_order_sensitive() {
        let left = [1u8; 32];
        let right = [2u8; 32];

        let h1 = hash_internal(&left, &right);
        let h2 = hash_internal(&left, &right);
        assert_eq!(h1, h2);
        assert_ne!(h1, hash_internal(&right, &left));
    }

    #[test]
    fn hash_leaf_has_domain_separation() {
        let data = b"hello-merkle";
        let h1 = hash_leaf(data);
        let h2 = hash_leaf(data);
        assert_eq!(h1, h2);

        let plain = Sha256::digest(data);
        let plain: [u8; 32] = plain.into();
        assert_ne!(h1, plain);
    }

    #[test]
    fn edge_cases_leaf_sizes_and_empty_leaves() {
        for leaf_size in [16usize, 32, 64] {
            for count in [4usize, 16] {
                let leaves = make_leaves(count, leaf_size);
                let tree_a = MerkleTree::new(&leaves);
                let tree_b = MerkleTree::new(&leaves);
                assert_eq!(tree_a.root(), tree_b.root());
            }
        }

        let leaves_all_empty = vec![Vec::new(), Vec::new(), Vec::new(), Vec::new()];
        let tree = MerkleTree::new(&leaves_all_empty);
        let root = tree.root();
        let proof = tree.generate_proof(0);
        assert!(verify_proof(
            &hash_leaf(&leaves_all_empty[0]),
            0,
            &proof,
            &root
        ));

        let leaves_mixed = vec![Vec::new(), vec![1], Vec::new(), vec![2, 3]];
        let tree = MerkleTree::new(&leaves_mixed);
        let root = tree.root();
        for (i, leaf) in leaves_mixed.iter().enumerate() {
            let proof = tree.generate_proof(i);
            assert!(verify_proof(&hash_leaf(leaf), i, &proof, &root));
        }
    }
}
