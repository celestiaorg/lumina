use celestia_proto::cosmos::base::tendermint::v1beta1::{ProofOp, ProofOps};
use ics23::commitment_proof::Proof;
use ics23::{CommitmentProof, ExistenceProof};
use prost::Message;
use tendermint::crypto::Sha256 as _;
use tendermint::crypto::default::Sha256;

/// Representation of all the errors that can occur when trying to read and verify ABCI proof
#[derive(Debug, thiserror::Error)]
pub enum ProofError {
    /// Computed root is different than expected
    // NOTE: we don't get the computed root from ics23, we would need to contribute to it
    #[error("Computed root is different than expected")]
    RootMismatch,

    /// ABCI proof is missing
    #[error("ABCI proof is missing")]
    AbciProofMissing,

    /// Unsupported proof spec
    #[error("Unsupported proof spec: {0}")]
    UnsupportedSpec(String),

    /// Decoding from proto failed
    #[error("Decoding from proto failed: {0}")]
    Decode(#[from] prost::DecodeError),

    /// Existance proof missing in operation
    #[error("Existance proof missing in operation")]
    ExistanceProofMissing,

    /// Uneven proofs and keys lengths
    #[error("Uneven proofs ({0}) and keys ({1}) lenghts")]
    UnevenProofsAndKeysLengths(usize, usize),

    /// Operation key is different than expected
    #[error("Operation key ({0:?}) is different than expected ({1:?})")]
    OperationKeyMismatch(Vec<u8>, Vec<u8>),
}

/// A commitment operation is an existance or non-existance proof of a leaf in a merkle tree.
/// The type of the merkle tree is defined by the spec (eg. simple merkle tree, or iavl tree).
/// A key is the "label" of the leaf we want to prove, eg. for a proof of an account balance in
/// bank's state tree, the leaf would be the balance of an account, and the key would be an
/// encoded account's address. This is used to check if the proof was provided for the expected
/// value, e.g. if the proof isn't for a different address which happens to have the same balance
/// (thus the same leaf value).
#[derive(Debug)]
struct CommitmentOp {
    key: Vec<u8>,
    spec: ics23::ProofSpec,
    proof: ics23::CommitmentProof,
}

impl TryFrom<ProofOp> for CommitmentOp {
    type Error = ProofError;

    fn try_from(value: ProofOp) -> Result<Self, Self::Error> {
        let spec = match value.r#type.as_str() {
            "ics23:iavl" => ics23::iavl_spec(),
            "ics23:simple" => ics23::tendermint_spec(),
            _ => return Err(ProofError::UnsupportedSpec(value.r#type.clone())),
        };
        Ok(Self {
            key: value.key,
            spec,
            proof: CommitmentProof::decode(value.data.as_slice())?,
        })
    }
}

impl CommitmentOp {
    /// A helper to extract the underlying existance proof (if any) for a given key.
    fn get_existence_proof(&self, key: impl AsRef<[u8]>) -> Option<&ExistenceProof> {
        match self.proof.proof.as_ref()? {
            Proof::Exist(proof) => Some(proof),
            Proof::Batch(batch) => batch.entries.iter().find_map(|entry| {
                if let ics23::batch_entry::Proof::Exist(proof) = entry.proof.as_ref()? {
                    if key.as_ref() == proof.key {
                        Some(proof)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }),
            _ => None,
        }
    }
}

/// A chain of proofs that is used to verify a leaf value in multiple nested merkle trees.
pub struct ProofChain(Vec<CommitmentOp>);

impl ProofChain {
    /// Verifies that a leaf value exists in nested merkle trees.
    ///
    /// Root is an expected root hash of the uppermost merkle tree.
    /// A leaf is a value we want to prove inclusion of, starting from the lowermost merkle tree.
    /// Each proof corresponds to one tree in a chain, and proves the inclusion of its leaf up to a leaf of the next proof,
    /// until the last proof when the value is proven up to the expected uppermost root. Provided keys must match the leaves'
    /// keys starting from the lowermost leaf.
    ///
    /// ```text
    ///                         root
    ///                       /      \
    ///                      /        \
    ///                     o          o
    ///                   /   \      /   \
    ///                  /     \    /     \
    ///             inner leaf  o  o       o
    ///           (with 2nd key)
    ///                 ==
    ///             inner root
    ///              /      \
    ///             /        \
    ///            o          o
    ///          /   \      /   \
    ///         /     \    /     \
    ///       leaf     o  o       o
    ///  (with 1st key)
    /// ```
    pub fn verify_membership(
        &self,
        root: impl AsRef<[u8]>,
        keys: impl IntoIterator<Item = impl AsRef<[u8]>>,
        leaf: impl AsRef<[u8]>,
    ) -> Result<(), ProofError> {
        let root = root.as_ref();
        let mut current_leaf = leaf.as_ref();
        let mut current_idx = 0;
        // fuse the iterator to make sure it doesn't do any fancy logic
        // to obey the checks
        let mut keys = keys.into_iter().fuse();

        while let Some(key) = keys.next() {
            let key = key.as_ref();
            let proof = self
                .0
                .get(current_idx)
                .ok_or(ProofError::ExistanceProofMissing)?;

            if key != proof.key {
                return Err(ProofError::OperationKeyMismatch(
                    proof.key.clone(),
                    key.to_vec(),
                ));
            }

            if proof.get_existence_proof(key).is_none() {
                return Err(ProofError::ExistanceProofMissing);
            }

            // current proof must prove current leaf to the root of the current tree,
            // which is at the same time the leaf for the next proof or the uppermost root
            // in case we are in proving the last tree
            let current_root = if let Some(proof) = self.0.get(current_idx + 1) {
                proof
                    .get_existence_proof(key)
                    .map(|proof| proof.value.as_slice())
                    .ok_or(ProofError::ExistanceProofMissing)?
            } else {
                // check that we used all keys already and we can prove to the uppermost root
                if keys.next().is_some() {
                    return Err(ProofError::UnevenProofsAndKeysLengths(
                        current_idx,
                        current_idx + 1,
                    ));
                }
                root
            };

            if !ics23::verify_membership::<Sha256Provider>(
                &proof.proof,
                &proof.spec,
                &current_root.to_vec(), // removing to_vec needs fix upstream
                &proof.key,
                current_leaf,
            ) {
                return Err(ProofError::RootMismatch);
            }

            current_leaf = current_root;
            current_idx += 1;
        }

        // check that we used all proofs during verification
        if self.0.get(current_idx).is_some() {
            return Err(ProofError::UnevenProofsAndKeysLengths(
                current_idx,
                current_idx - 1,
            ));
        }

        Ok(())
    }
}

impl TryFrom<ProofOps> for ProofChain {
    type Error = ProofError;

    fn try_from(value: ProofOps) -> Result<Self, Self::Error> {
        if value.ops.is_empty() {
            return Err(ProofError::AbciProofMissing);
        }
        let chain = value
            .ops
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, _>>()?;

        Ok(Self(chain))
    }
}

/// A hasher provider for the ics23 crate, which only supports tendermint's sha256 to avoid
/// unneccessary dependencies. We anyway only support proof specs which use sha256
struct Sha256Provider;

impl ics23::HostFunctionsProvider for Sha256Provider {
    fn sha2_256(message: &[u8]) -> [u8; 32] {
        Sha256::digest(message)
    }

    fn sha2_512(_: &[u8]) -> [u8; 64] {
        [0; 64]
    }

    fn sha2_512_truncated(_: &[u8]) -> [u8; 32] {
        [0; 32]
    }

    fn keccak_256(_: &[u8]) -> [u8; 32] {
        [0; 32]
    }

    fn ripemd160(_: &[u8]) -> [u8; 20] {
        [0; 20]
    }

    fn blake2b_512(_: &[u8]) -> [u8; 64] {
        [0; 64]
    }

    fn blake2s_256(_: &[u8]) -> [u8; 32] {
        [0; 32]
    }

    fn blake3(_: &[u8]) -> [u8; 32] {
        [0; 32]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifies_simple_membership_proof() {
        let key = b"key".to_vec();
        let value = b"value".to_vec();
        let spec = ics23::tendermint_spec();
        let existence_proof = ExistenceProof {
            key: key.clone(),
            value: value.clone(),
            leaf: spec.leaf_spec.clone(),
            path: Vec::new(),
        };
        let root = ics23::calculate_existence_root::<Sha256Provider>(&existence_proof).unwrap();
        let chain = ProofChain(vec![CommitmentOp {
            key: key.clone(),
            spec,
            proof: CommitmentProof {
                proof: Some(Proof::Exist(existence_proof)),
            },
        }]);

        chain
            .verify_membership(root, [key.as_slice()], value)
            .unwrap();
    }
}
