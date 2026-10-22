use std::ops::{Deref, DerefMut};

use celestia_proto::celestia::core::v1::proof::NmtProof as RawNmtProof;
use celestia_proto::proof::pb::Proof as RawProof;
use nmt_rs::NamespaceId;
use nmt_rs::simple_merkle::error::RangeProofError;
use nmt_rs::simple_merkle::proof::Proof as NmtProof;
use serde::{Deserialize, Serialize};
use tendermint_proto::Protobuf;

use crate::nmt::{NS_SIZE, NamespacedHash, NamespacedHashExt, NamespacedSha2Hasher};
use crate::{Error, Result};

type NmtNamespaceProof = nmt_rs::nmt_proof::NamespaceProof<NamespacedSha2Hasher, NS_SIZE>;

/// A helper constant to be used as leaves when verifying the [`NamespaceProof`] of absence.
pub const EMPTY_LEAVES: &[&[u8]] = &[];

/// Merkle proof of inclusion or absence of some data in the [`Nmt`].
///
/// # Example
///
/// ```
/// use nmt_rs::NamespaceMerkleHasher;
/// use celestia_types::nmt::{Namespace, Nmt, NamespacedSha2Hasher, EMPTY_LEAVES};
///
/// let ns1 = Namespace::new_v0(&[1]).unwrap();
/// let ns2 = Namespace::new_v0(&[2]).unwrap();
/// let ns3 = Namespace::new_v0(&[3]).unwrap();
/// let ns4 = Namespace::new_v0(&[4]).unwrap();
///
/// let leaves = [
///     (ns1, b"leaf0"),
///     (ns2, b"leaf1"),
///     (ns2, b"leaf2"),
///     (ns4, b"leaf3"),
/// ];
///
/// // create the nmt and feed it with data
/// let mut nmt = Nmt::with_hasher(NamespacedSha2Hasher::with_ignore_max_ns(true));
///
/// for (namespace, data) in leaves {
///     nmt.push_leaf(data, *namespace);
/// }
///
/// // create and verify the proof of inclusion of namespace 2 data
/// let root = nmt.root();
/// let proof = nmt.get_namespace_proof(*ns2);
/// assert!(proof.is_of_presence());
/// assert!(
///     proof.verify_complete_namespace(&root, &["leaf1", "leaf2"], *ns2).is_ok()
/// );
///
/// // create and verify the proof of absence of namespace 3 data
/// let proof = nmt.get_namespace_proof(*ns3);
/// assert!(proof.is_of_absence());
/// assert!(
///     proof.verify_complete_namespace(&root, EMPTY_LEAVES, *ns3).is_ok()
/// );
/// ```
///
/// [`Nmt`]: crate::nmt::Nmt
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawProof", into = "RawProof")]
pub struct NamespaceProof(NmtNamespaceProof);

impl NamespaceProof {
    /// Convert the proof to the underlying [`nmt_rs`] equivalent.
    pub fn into_inner(self) -> NmtNamespaceProof {
        self.0
    }

    /// Get the hash of the leaf following the [`Namespace`] which absence is being proven.
    ///
    /// If the tree had contained the proven namespace, it should be in the tree
    /// right before the leaf returned by this function.
    ///
    /// This function returns [`None`] if the proof isn't an [`AbsenceProof`] or the
    /// proven [`Namespace`] is not in the range of the tree root [`NamespacedHash`].
    ///
    /// [`Namespace`]: crate::nmt::Namespace
    /// [`AbsenceProof`]: NmtNamespaceProof::PresenceProof
    /// [`NamespacedHash`]: crate::nmt::NamespacedHash
    pub fn leaf(&self) -> Option<&NamespacedHash> {
        match &self.0 {
            NmtNamespaceProof::AbsenceProof { leaf, .. } => leaf.as_ref(),
            _ => None,
        }
    }

    /// Returns true if the proof ignores all the leaves inserted with
    /// [`Namespace::PARITY_SHARE`].
    ///
    /// [`Namespace::PARITY_SHARE`]: crate::nmt::Namespace::PARITY_SHARE
    pub fn max_ns_ignored(&self) -> bool {
        match &self.0 {
            NmtNamespaceProof::AbsenceProof { ignore_max_ns, .. }
            | NmtNamespaceProof::PresenceProof { ignore_max_ns, .. } => *ignore_max_ns,
        }
    }

    /// Returns total amount of leaves in a tree for which proof was constructed.
    ///
    /// This method only works if the proof is created for a single leaf and it
    /// assumes that the tree is perfect, i.e. it's amount of leaves is power of 2.
    /// Returns `None` also when the proof is deeper than any tree that fits in `usize`.
    pub(crate) fn total_leaves(&self) -> Option<usize> {
        // If the proof is for a single leaf, then it must contain a sibling
        // for each tree level. Based on that we can recompute the total amount
        // of leaves in a tree.
        if self.end_idx().saturating_sub(self.start_idx()) == 1 {
            let depth = u32::try_from(self.siblings().len()).ok()?;
            1usize.checked_shl(depth)
        } else {
            None
        }
    }

    /// Verify that the raw leaves are present in the tree with the given root, as a
    /// contiguous range of leaves of the given namespace.
    ///
    /// Unlike the `nmt_rs` method of the same name, this rejects proofs whose nodes
    /// are not ordered by namespace instead of panicking on them.
    pub fn verify_range(
        &self,
        root: &NamespacedHash,
        raw_leaves: &[impl AsRef<[u8]>],
        leaf_namespace: NamespaceId<NS_SIZE>,
    ) -> Result<(), RangeProofError> {
        let leaf = NamespacedHash::with_min_and_max_ns(leaf_namespace, leaf_namespace);
        self.ensure_nodes_ordered(&leaf)?;
        self.0.verify_range(root, raw_leaves, leaf_namespace)
    }

    /// Verify that the raw leaves are all the leaves of the given namespace in the
    /// tree with the given root. This may be a proof of presence or absence.
    ///
    /// Unlike the `nmt_rs` method of the same name, this rejects proofs whose nodes
    /// are not ordered by namespace instead of panicking on them.
    pub fn verify_complete_namespace(
        &self,
        root: &NamespacedHash,
        raw_leaves: &[impl AsRef<[u8]>],
        namespace: NamespaceId<NS_SIZE>,
    ) -> Result<(), RangeProofError> {
        let leaf = match &self.0 {
            NmtNamespaceProof::PresenceProof { .. } => {
                NamespacedHash::with_min_and_max_ns(namespace, namespace)
            }
            NmtNamespaceProof::AbsenceProof {
                leaf: Some(leaf), ..
            } => leaf.clone(),
            // nothing gets hashed for an absence proof without a leaf
            NmtNamespaceProof::AbsenceProof { leaf: None, .. } => {
                return self
                    .0
                    .verify_complete_namespace(root, raw_leaves, namespace);
            }
        };
        self.ensure_nodes_ordered(&leaf)?;
        self.0
            .verify_complete_namespace(root, raw_leaves, namespace)
    }

    /// Check that the proof has the siblings its range requires and that the nodes
    /// it spans, in leaf order, have non-decreasing namespace ranges, as the nodes
    /// of any valid tree do.
    ///
    /// `nmt_rs` panics when indexing missing siblings and when hashing two nodes
    /// which are not ordered like that, so this has to be checked before verifying
    /// the proof.
    fn ensure_nodes_ordered(&self, leaf: &NamespacedHash) -> Result<(), RangeProofError> {
        let siblings = self.siblings();
        // the first `popcount(start_idx)` siblings are to the left of the proven range
        let num_left = self.start_idx().count_ones() as usize;
        if num_left > siblings.len() {
            return Err(RangeProofError::MissingProofNode);
        }
        let (left, right) = siblings.split_at(num_left);

        let mut nodes = left.iter().chain(std::iter::once(leaf)).chain(right);
        let mut prev = nodes.next().expect("the leaf is always present");
        if prev.min_namespace() > prev.max_namespace() {
            return Err(RangeProofError::MalformedTree);
        }

        for node in nodes {
            if node.min_namespace() > node.max_namespace()
                || prev.max_namespace() > node.min_namespace()
            {
                return Err(RangeProofError::MalformedTree);
            }
            prev = node;
        }

        Ok(())
    }
}

impl Deref for NamespaceProof {
    type Target = NmtNamespaceProof;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for NamespaceProof {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<NamespaceProof> for NmtNamespaceProof {
    fn from(value: NamespaceProof) -> NmtNamespaceProof {
        value.0
    }
}

impl From<NmtNamespaceProof> for NamespaceProof {
    fn from(value: NmtNamespaceProof) -> NamespaceProof {
        NamespaceProof(value)
    }
}

impl Protobuf<RawProof> for NamespaceProof {}

impl TryFrom<RawProof> for NamespaceProof {
    type Error = Error;

    fn try_from(value: RawProof) -> Result<Self, Self::Error> {
        let siblings = value
            .nodes
            .iter()
            .map(|bytes| NamespacedHash::from_raw(bytes))
            .collect::<Result<Vec<_>>>()?;

        let mut proof = NmtNamespaceProof::PresenceProof {
            proof: NmtProof {
                siblings,
                range: value.start as u32..value.end as u32,
            },
            ignore_max_ns: value.is_max_namespace_ignored,
        };

        if !value.leaf_hash.is_empty() {
            proof.convert_to_absence_proof(NamespacedHash::from_raw(&value.leaf_hash)?);
        }

        Ok(NamespaceProof(proof))
    }
}

impl From<NamespaceProof> for RawProof {
    fn from(value: NamespaceProof) -> Self {
        RawProof {
            start: value.start_idx() as i64,
            end: value.end_idx() as i64,
            nodes: value.siblings().iter().map(|hash| hash.to_vec()).collect(),
            leaf_hash: value.leaf().map(|hash| hash.to_vec()).unwrap_or_default(),
            is_max_namespace_ignored: value.max_ns_ignored(),
        }
    }
}

impl TryFrom<RawNmtProof> for NamespaceProof {
    type Error = Error;

    fn try_from(value: RawNmtProof) -> Result<Self, Self::Error> {
        let raw_proof = RawProof {
            start: value.start as i64,
            end: value.end as i64,
            nodes: value.nodes,
            leaf_hash: value.leaf_hash,
            is_max_namespace_ignored: true,
        };

        raw_proof.try_into()
    }
}

impl From<NamespaceProof> for RawNmtProof {
    fn from(value: NamespaceProof) -> Self {
        let raw_proof = RawProof::from(value);
        RawNmtProof {
            start: raw_proof.start as i32,
            end: raw_proof.end as i32,
            nodes: raw_proof.nodes,
            leaf_hash: raw_proof.leaf_hash,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nmt::Namespace;

    #[test]
    fn test_serialize_namespace_proof_binary() {
        let nmt_proof = NmtNamespaceProof::PresenceProof {
            proof: NmtProof {
                siblings: vec![],
                range: 0..1,
            },
            ignore_max_ns: false,
        };
        let proof = NamespaceProof::from(nmt_proof);
        let serialized = postcard::to_allocvec(&proof).unwrap();
        let deserialized: NamespaceProof = postcard::from_bytes(&serialized).unwrap();
        assert_eq!(proof, deserialized);
    }

    /// A sibling whose namespace range lies below the namespaces of everything
    /// to its left. Such a node can never appear in a valid tree.
    fn unordered_sibling() -> NamespacedHash {
        let ns = Namespace::new_v0(&[1]).unwrap();
        NamespacedHash::with_min_and_max_ns(*ns, *ns)
    }

    #[test]
    fn verify_range_rejects_unordered_nodes() {
        let ns = Namespace::new_v0(&[7]).unwrap();
        let root = NamespacedHash::with_min_and_max_ns(*Namespace::new_v0(&[1]).unwrap(), *ns);
        let proof: NamespaceProof = NmtNamespaceProof::PresenceProof {
            proof: NmtProof {
                siblings: vec![unordered_sibling()],
                range: 0..1,
            },
            ignore_max_ns: true,
        }
        .into();

        proof.verify_range(&root, &[b"leaf"], *ns).unwrap_err();
    }

    #[test]
    fn verify_complete_namespace_rejects_unordered_nodes() {
        let ns = Namespace::new_v0(&[7]).unwrap();
        let root = NamespacedHash::with_min_and_max_ns(*Namespace::new_v0(&[1]).unwrap(), *ns);
        let proof: NamespaceProof = NmtNamespaceProof::PresenceProof {
            proof: NmtProof {
                siblings: vec![unordered_sibling()],
                range: 0..1,
            },
            ignore_max_ns: true,
        }
        .into();

        proof
            .verify_complete_namespace(&root, &[b"leaf"], *ns)
            .unwrap_err();
    }

    #[test]
    fn absence_proof_rejects_unordered_nodes() {
        let absent = Namespace::new_v0(&[7]).unwrap();
        let present = Namespace::new_v0(&[9]).unwrap();
        let root = NamespacedHash::with_min_and_max_ns(*Namespace::new_v0(&[1]).unwrap(), *present);
        let proof: NamespaceProof = NmtNamespaceProof::AbsenceProof {
            proof: NmtProof {
                siblings: vec![unordered_sibling()],
                range: 0..1,
            },
            ignore_max_ns: true,
            leaf: Some(NamespacedHash::with_min_and_max_ns(*present, *present)),
        }
        .into();

        proof
            .verify_complete_namespace(&root, EMPTY_LEAVES, *absent)
            .unwrap_err();
    }

    #[test]
    fn verify_complete_namespace_rejects_too_few_left_siblings() {
        let ns = Namespace::new_v0(&[7]).unwrap();
        let root = NamespacedHash::with_min_and_max_ns(*Namespace::new_v0(&[1]).unwrap(), *ns);
        // a range starting at index 1 needs a left sibling
        let proof: NamespaceProof = NmtNamespaceProof::PresenceProof {
            proof: NmtProof {
                siblings: vec![],
                range: 1..3,
            },
            ignore_max_ns: true,
        }
        .into();

        proof
            .verify_complete_namespace(&root, &[b"leaf", b"leaf"], *ns)
            .unwrap_err();
    }

    #[test]
    fn absence_proof_rejects_too_few_left_siblings() {
        let absent = Namespace::new_v0(&[3]).unwrap();
        let present = Namespace::new_v0(&[4]).unwrap();
        let root = NamespacedHash::with_min_and_max_ns(*Namespace::new_v0(&[1]).unwrap(), *present);
        let proof: NamespaceProof = NmtNamespaceProof::AbsenceProof {
            proof: NmtProof {
                siblings: vec![],
                range: 3..4,
            },
            ignore_max_ns: true,
            leaf: Some(NamespacedHash::with_min_and_max_ns(*present, *present)),
        }
        .into();

        proof
            .verify_complete_namespace(&root, EMPTY_LEAVES, *absent)
            .unwrap_err();
    }
}
