use celestia_proto::celestia::core::v1::proof::Proof as RawMerkleProof;
use serde::{Deserialize, Serialize};
use tendermint::crypto::default::Sha256;
use tendermint::merkle::{Hash, MerkleHash};
use tendermint_proto::Protobuf;

use crate::{
    Error, Result, bail_validation, bail_verification, validation_error, verification_error,
};

/// A proof of inclusion of some leaf in a merkle tree.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawMerkleProof", into = "RawMerkleProof")]
pub struct MerkleProof {
    /// Leaf index.
    pub index: usize,
    /// Total number of leaves in the Merkle tree.
    pub total: usize,
    /// Hash of the leaf.
    pub leaf_hash: Hash,
    /// Hashes of the sibling nodes at each level of the tree.
    pub aunts: Vec<Hash>,
}

impl MerkleProof {
    /// Create a merkle proof of inclusion of a given leaf in a list.
    ///
    /// Returns a proof together with the root hash of a merkle tree built from given leaves.
    ///
    /// # Errors
    ///
    /// This function will return an error if `leaf_to_prove` index is out of `leaves` bounds.
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::MerkleProof;
    ///
    /// let (proof, root) = MerkleProof::new(0, &[b"a", b"b", b"c"]).unwrap();
    ///
    /// assert!(proof.verify(b"a", root).is_ok());
    /// ```
    pub fn new(leaf_to_prove: usize, leaves: &[impl AsRef<[u8]>]) -> Result<(Self, Hash)> {
        if leaf_to_prove >= leaves.len() {
            return Err(Error::IndexOutOfRange(leaf_to_prove, leaves.len()));
        }

        let tree_height = leaves.len().next_power_of_two().ilog2() as usize;
        let mut aunts = Vec::with_capacity(tree_height);

        let root = hash_leaves_collecting_aunts(0, leaf_to_prove, leaves, &mut aunts);
        let proof = Self {
            index: leaf_to_prove,
            total: leaves.len(),
            leaf_hash: Sha256::default().leaf_hash(leaves[leaf_to_prove].as_ref()),
            aunts,
        };

        Ok((proof, root))
    }

    /// Verify that given leaf is included under the root hash.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    ///  - provided leaf is different than the one for which proof was created
    ///  - proof is malformed, meaning some inconsistency between leaf index, leaves count,
    ///    or amount of inner nodes
    ///  - the recomputed root hash differs from expected one
    pub fn verify(&self, leaf: impl AsRef<[u8]>, root: Hash) -> Result<()> {
        if self.total == 0 || self.index >= self.total {
            bail_verification!("leaf index is outside the proof's tree");
        }
        let mut hasher = Sha256::default();
        let leaf = hasher.leaf_hash(leaf.as_ref());

        if leaf != self.leaf_hash {
            return Err(verification_error!("proof created for a different leaf").into());
        }

        let computed_root = subtree_root_from_aunts(self.index, self.total, leaf, &self.aunts)?;

        if computed_root != root {
            return Err(Error::RootMismatch);
        }

        Ok(())
    }
}

// creates a merkle tree out of the given leaves and returns its root hash.
// inner nodes needed to prove leaf with given index are collected in `aunts`.
fn hash_leaves_collecting_aunts(
    // for recursion, index of the first leaf in current subtree wrt the whole tree
    first_leaf_index: usize,
    leaf_to_prove: usize,
    leaves: &[impl AsRef<[u8]>],
    aunts: &mut Vec<Hash>,
) -> Hash {
    let mut hasher = Sha256::default();
    let total = leaves.len();

    match total {
        // can only happen if there are 0 leaves without recursing
        0 => hasher.empty_hash(),
        // we reached a leaf
        1 => hasher.leaf_hash(leaves[0].as_ref()),
        // split leaves into subtrees and hash them recursively
        _ => {
            let subtrees_split = total.next_power_of_two() / 2;
            let left = hash_leaves_collecting_aunts(
                first_leaf_index,
                leaf_to_prove,
                &leaves[..subtrees_split],
                aunts,
            );
            let right = hash_leaves_collecting_aunts(
                first_leaf_index + subtrees_split,
                leaf_to_prove,
                &leaves[subtrees_split..],
                aunts,
            );

            // if current subtree has the leaf to prove
            if (first_leaf_index..first_leaf_index + total).contains(&leaf_to_prove) {
                if leaf_to_prove < first_leaf_index + subtrees_split {
                    // leaf was in left subtree, so its aunt is right hash
                    aunts.push(right)
                } else {
                    // leaf was in right subtree, so its aunt is left hash
                    aunts.push(left)
                }
            }

            hasher.inner_hash(left, right)
        }
    }
}

// aunts are effectively a merkle proof for the leaf, so inner hashes needed
// to recompute root hash of a merkle tree
fn subtree_root_from_aunts(index: usize, total: usize, leaf: Hash, aunts: &[Hash]) -> Result<Hash> {
    debug_assert_ne!(total, 0);

    let root = if total == 1 {
        // we reached the leaf
        if !aunts.is_empty() {
            bail_verification!("extra aunts in proof");
        }
        leaf
    } else {
        let mut hasher = Sha256::default();
        let subtrees_split = total.next_power_of_two() / 2;

        // take next subtree root's sibling
        let (sibling, aunts) = aunts
            .split_last()
            .ok_or_else(|| verification_error!("aunts missing in proof"))?;

        if index < subtrees_split {
            // and recurse into left subtree
            let left_hash = subtree_root_from_aunts(index, subtrees_split, leaf, aunts)?;
            hasher.inner_hash(left_hash, *sibling)
        } else {
            // and recurse into right subtree
            let right_hash = subtree_root_from_aunts(
                index - subtrees_split,
                total - subtrees_split,
                leaf,
                aunts,
            )?;
            hasher.inner_hash(*sibling, right_hash)
        }
    };

    Ok(root)
}

impl Protobuf<RawMerkleProof> for MerkleProof {}

impl TryFrom<RawMerkleProof> for MerkleProof {
    type Error = Error;

    fn try_from(value: RawMerkleProof) -> Result<Self, Self::Error> {
        let index = usize::try_from(value.index)
            .map_err(|_| validation_error!("proof index is out of range"))?;
        let total = usize::try_from(value.total)
            .map_err(|_| validation_error!("proof total is out of range"))?;
        if total == 0 || index >= total {
            bail_validation!("proof index is outside the tree");
        }
        Ok(Self {
            index,
            total,
            leaf_hash: value
                .leaf_hash
                .try_into()
                .map_err(|_| validation_error!("invalid hash size"))?,
            aunts: value
                .aunts
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map_err(|_| validation_error!("invalid hash size"))?,
        })
    }
}

impl From<MerkleProof> for RawMerkleProof {
    fn from(value: MerkleProof) -> Self {
        Self {
            index: value.index as i64,
            total: value.total as i64,
            leaf_hash: value.leaf_hash.into(),
            aunts: value.aunts.into_iter().map(Into::into).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use tendermint::crypto::default::Sha256;
    use tendermint::merkle::simple_hash_from_byte_vectors;

    use crate::test_utils::random_bytes;

    use super::MerkleProof;

    #[test]
    fn rejects_out_of_bounds_leaf_index() {
        let (mut proof, root) = MerkleProof::new(0, &[b"a"]).unwrap();
        proof.index = 123;
        assert!(proof.verify(b"a", root).is_err());

        let (mut proof, root) = MerkleProof::new(3, &[b"a", b"b", b"c", b"d"]).unwrap();
        proof.index = 999;
        assert!(proof.verify(b"d", root).is_err());
    }

    #[test]
    fn create_and_verify() {
        for _ in 0..100 {
            let leaf_size = (rand::random::<usize>() % 1024) + 1;
            let leaves_amount = 8;
            let data = random_bytes(leaves_amount * leaf_size);

            let leaves: Vec<_> = data.chunks(leaf_size).collect();
            let leaf_to_prove = rand::random::<usize>() % leaves_amount;

            let (proof, root) = MerkleProof::new(leaf_to_prove, &leaves).unwrap();

            proof.verify(leaves[leaf_to_prove], root).unwrap();
            proof.verify(random_bytes(leaf_size), root).unwrap_err();
            proof
                .verify(leaves[leaf_to_prove], rand::random())
                .unwrap_err();
        }
    }

    #[test]
    fn tendermint_compatibility() {
        for _ in 0..100 {
            let leaves_amount = (rand::random::<usize>() % 500) + 1;
            let leaves: Vec<_> = (0..leaves_amount)
                .map(|_| {
                    let leaf_size = (rand::random::<usize>() % 1024) + 1;
                    random_bytes(leaf_size)
                })
                .collect();

            let (_, root) = MerkleProof::new(0, &leaves).unwrap();
            let tendermint_root = simple_hash_from_byte_vectors::<Sha256>(&leaves);

            assert_eq!(root, tendermint_root);
        }
    }
}
