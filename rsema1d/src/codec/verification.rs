use crate::codec::padding::map_index_to_tree_position;
use crate::codec::proof::{RowInclusionProof, RowProof, StandaloneProof};
use crate::codec::{compute_rlc, extend_rlcs};
use crate::crypto::{derive_coefficients, hash_internal, hash_leaf, sha256, MerkleTree};
use crate::error::{Error, Result};
use crate::field::GF128;
use crate::params::Parameters;

fn build_rlc_root(rlc_orig: &[GF128], params: &Parameters) -> [u8; 32] {
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
    MerkleTree::from_leaf_hashes(leaf_hashes).root()
}

/// Pre-computed context for efficient batch verification.
#[derive(Debug, Clone)]
pub struct VerificationContext {
    params: Parameters,
    rlc_extended: Vec<GF128>,
    rlc_root: [u8; 32],
}

impl VerificationContext {
    /// Create verification context from original K RLC values.
    pub fn new(rlc_orig: &[GF128], params: &Parameters) -> Result<Self> {
        if rlc_orig.len() != params.k {
            return Err(Error::InvalidParameters(format!(
                "expected {} RLC values, got {}",
                params.k,
                rlc_orig.len()
            )));
        }

        let rlc_extended = extend_rlcs(rlc_orig, params.k, params.n)?;
        let rlc_root = build_rlc_root(rlc_orig, params);

        Ok(Self {
            params: *params,
            rlc_extended,
            rlc_root,
        })
    }

    pub fn rlc_root(&self) -> [u8; 32] {
        self.rlc_root
    }

    pub fn params(&self) -> &Parameters {
        &self.params
    }
}

/// Create verification context and return its RLC root.
pub fn create_verification_context(
    rlc_orig: &[GF128],
    params: &Parameters,
) -> Result<(VerificationContext, [u8; 32])> {
    let context = VerificationContext::new(rlc_orig, params)?;
    let rlc_root = context.rlc_root();
    Ok((context, rlc_root))
}

fn reconstruct_root_from_proof(leaf: [u8; 32], mut pos: usize, proof: &[[u8; 32]]) -> [u8; 32] {
    let mut current = leaf;
    for sibling in proof {
        current = if pos.is_multiple_of(2) {
            hash_internal(&current, sibling)
        } else {
            hash_internal(sibling, &current)
        };
        pos /= 2;
    }
    current
}

fn row_tree_depth(params: &Parameters) -> usize {
    params.total_padded().ilog2() as usize
}

fn rlc_tree_depth(params: &Parameters) -> usize {
    params.k_padded().ilog2() as usize
}

/// Verify standalone proof (original rows only).
pub fn verify_standalone(
    proof: &StandaloneProof,
    commitment: &[u8; 32],
    params: &Parameters,
) -> Result<bool> {
    if proof.index >= params.k {
        return Err(Error::InvalidParameters(format!(
            "standalone verification only supports original rows: index {} >= k {}",
            proof.index, params.k
        )));
    }
    if proof.row.len() != params.row_size {
        return Err(Error::RowLengthMismatch {
            expected: params.row_size,
            actual: proof.row.len(),
        });
    }
    if proof.row_proof.len() != row_tree_depth(params) {
        return Err(Error::InvalidParameters(format!(
            "row proof depth mismatch: expected {}, got {}",
            row_tree_depth(params),
            proof.row_proof.len()
        )));
    }
    if proof.rlc_proof.len() != rlc_tree_depth(params) {
        return Err(Error::InvalidParameters(format!(
            "rlc proof depth mismatch: expected {}, got {}",
            rlc_tree_depth(params),
            proof.rlc_proof.len()
        )));
    }

    let tree_pos = map_index_to_tree_position(proof.index, params.k);
    let row_root = reconstruct_root_from_proof(hash_leaf(&proof.row), tree_pos, &proof.row_proof);

    let coeffs = derive_coefficients(&row_root, params.symbols_per_row());
    let rlc = compute_rlc(&proof.row, &coeffs);
    let rlc_root =
        reconstruct_root_from_proof(hash_leaf(&rlc.to_bytes()), proof.index, &proof.rlc_proof);

    let mut combined = Vec::with_capacity(64);
    combined.extend_from_slice(&row_root);
    combined.extend_from_slice(&rlc_root);
    let computed_commitment = sha256(&combined);

    if computed_commitment != *commitment {
        return Err(Error::VerificationFailed(
            "commitment verification failed".to_string(),
        ));
    }

    Ok(true)
}

/// Verify row proof with pre-computed context.
pub fn verify_with_context(
    proof: &RowProof<'_>,
    commitment: &[u8; 32],
    context: &VerificationContext,
) -> Result<bool> {
    if proof.index >= context.params.total_rows() {
        return Err(Error::InvalidIndex(
            proof.index,
            context.params.total_rows(),
        ));
    }
    if proof.row.len() != context.params.row_size {
        return Err(Error::RowLengthMismatch {
            expected: context.params.row_size,
            actual: proof.row.len(),
        });
    }
    if proof.row_proof.len() != row_tree_depth(&context.params) {
        return Err(Error::InvalidParameters(format!(
            "row proof depth mismatch: expected {}, got {}",
            row_tree_depth(&context.params),
            proof.row_proof.len()
        )));
    }

    let tree_pos = map_index_to_tree_position(proof.index, context.params.k);
    let row_root =
        reconstruct_root_from_proof(hash_leaf(proof.row.as_ref()), tree_pos, &proof.row_proof);

    let coeffs = derive_coefficients(&row_root, context.params.symbols_per_row());
    let computed_rlc = compute_rlc(proof.row.as_ref(), &coeffs);
    if computed_rlc != context.rlc_extended[proof.index] {
        return Err(Error::VerificationFailed(
            "computed RLC does not match expected value".to_string(),
        ));
    }

    let mut combined = Vec::with_capacity(64);
    combined.extend_from_slice(&row_root);
    combined.extend_from_slice(&context.rlc_root);
    let computed_commitment = sha256(&combined);

    if computed_commitment != *commitment {
        return Err(Error::VerificationFailed(
            "commitment verification failed".to_string(),
        ));
    }

    Ok(true)
}

/// Alias for context-based verification.
pub fn verify_proof(
    proof: &RowProof<'_>,
    commitment: &[u8; 32],
    context: &VerificationContext,
) -> Result<bool> {
    verify_with_context(proof, commitment, context)
}

/// Go-style alias for context-based verification.
pub fn verify_row_with_context(
    proof: &RowProof<'_>,
    commitment: &[u8; 32],
    context: &VerificationContext,
) -> Result<()> {
    verify_with_context(proof, commitment, context).map(|_| ())
}

/// Verify row inclusion proof (does not check RLC commutation).
pub fn verify_row_inclusion(
    proof: &RowInclusionProof,
    commitment: &[u8; 32],
    params: &Parameters,
) -> Result<bool> {
    if proof.index >= params.total_rows() {
        return Err(Error::InvalidIndex(proof.index, params.total_rows()));
    }
    if proof.row.len() != params.row_size {
        return Err(Error::RowLengthMismatch {
            expected: params.row_size,
            actual: proof.row.len(),
        });
    }
    if proof.row_proof.len() != row_tree_depth(params) {
        return Err(Error::InvalidParameters(format!(
            "row proof depth mismatch: expected {}, got {}",
            row_tree_depth(params),
            proof.row_proof.len()
        )));
    }

    let tree_pos = map_index_to_tree_position(proof.index, params.k);
    let row_root = reconstruct_root_from_proof(hash_leaf(&proof.row), tree_pos, &proof.row_proof);

    let mut combined = Vec::with_capacity(64);
    combined.extend_from_slice(&row_root);
    combined.extend_from_slice(&proof.rlc_root);
    let computed_commitment = sha256(&combined);

    if computed_commitment != *commitment {
        return Err(Error::VerificationFailed(
            "commitment verification failed".to_string(),
        ));
    }

    Ok(true)
}

/// Go-style alias for standalone proof verification.
pub fn verify_standalone_proof(
    proof: &StandaloneProof,
    commitment: &[u8; 32],
    params: &Parameters,
) -> Result<()> {
    verify_standalone(proof, commitment, params).map(|_| ())
}

/// Go-style alias for row inclusion verification.
pub fn verify_row_inclusion_proof(
    proof: &RowInclusionProof,
    commitment: &[u8; 32],
    params: &Parameters,
) -> Result<()> {
    verify_row_inclusion(proof, commitment, params).map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::ExtendedData;
    use crate::codec::RowMatrix;

    #[test]
    fn test_verify_with_context() {
        let params = Parameters::new(4, 4, 64).unwrap();
        let mut original = vec![0u8; params.k * params.row_size];
        for i in 0..params.k {
            original[i * params.row_size] = i as u8;
        }

        let original = RowMatrix::with_shape(original, params.k, params.row_size).unwrap();
        let ext_data = ExtendedData::generate(&original, &params).unwrap();
        let context = VerificationContext::new(ext_data.rlc_original(), &params).unwrap();

        let proof = ext_data.generate_row_proof(0).unwrap();
        assert!(verify_with_context(&proof, &ext_data.commitment(), &context).unwrap());

        let proof = ext_data.generate_row_proof(4).unwrap();
        assert!(verify_with_context(&proof, &ext_data.commitment(), &context).unwrap());
    }
}
