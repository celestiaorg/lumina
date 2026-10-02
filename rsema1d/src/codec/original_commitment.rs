use crate::codec::symbols::RlcCoefficientLogs;
use crate::codec::Commitment;
use crate::crypto::{derive_coefficients, hash_internal, hash_leaf, sha256_pair, MerkleTree};
use crate::error::{Error, Result};
use crate::field::GF128;
use crate::params::Parameters;
use rayon::prelude::*;

/// Build the RLC tree over the original rows' RLCs, zero-padded to `k.next_power_of_two()`.
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

/// Compute the commitment from the K original rows, without encoding parity.
///
/// Parity rows enter the commitment only through the row root, so they are
/// replaced by `row_root_siblings`: the nodes on the path from the root of the
/// original rows' subtree (the first `k.next_power_of_two()` leaves) to the
/// row root, lowest first. These are the last
/// `log2(total_padded / k_padded)` nodes of any original row's proof.
///
/// Gives the same commitment as `ExtendedData::commitment` for the same rows.
/// The RLCs are computed from `rows`, so a wrong row or sibling gives a
/// different commitment.
pub fn commitment_from_original_rows(
    rows: &[&[u8]],
    params: &Parameters,
    row_root_siblings: &[[u8; 32]],
) -> Result<Commitment> {
    if rows.len() != params.k {
        return Err(Error::InvalidParameters(format!(
            "expected {} original rows, got {}",
            params.k,
            rows.len()
        )));
    }
    if let Some(row) = rows.iter().find(|row| row.len() != params.row_size) {
        return Err(Error::RowLengthMismatch {
            expected: params.row_size,
            actual: row.len(),
        });
    }
    let depth = (params.total_padded() / params.k_padded()).trailing_zeros() as usize;
    if row_root_siblings.len() != depth {
        return Err(Error::InvalidParameters(format!(
            "expected {} row root siblings, got {}",
            depth,
            row_root_siblings.len()
        )));
    }

    let zero_hash = hash_leaf(&vec![0u8; params.row_size]);
    let mut leaves: Vec<[u8; 32]> = rows.par_iter().map(|row| hash_leaf(row)).collect();
    leaves.resize(params.k_padded(), zero_hash);
    // The original rows' subtree is the left-most one, so every sibling is a right child.
    let row_root = row_root_siblings.iter().fold(
        MerkleTree::from_leaf_hashes(leaves).root(),
        |node, sibling| hash_internal(&node, sibling),
    );

    let coefficient_logs = RlcCoefficientLogs::new(derive_coefficients(
        &row_root,
        params.k,
        params.n,
        params.row_size,
    ));
    let rlc_orig: Vec<GF128> = rows
        .par_iter()
        .map(|row| coefficient_logs.compute_rlc(row))
        .collect();
    let rlc_root = build_rlc_tree(&rlc_orig, params).root();

    Ok(sha256_pair(&row_root, &rlc_root))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{ExtendedData, RowMatrix};
    use rand::{RngCore, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    fn encode(params: &Parameters, seed: u64) -> (Vec<u8>, ExtendedData) {
        let mut data = vec![0u8; params.k * params.row_size];
        ChaCha8Rng::seed_from_u64(seed).fill_bytes(&mut data);
        let original = RowMatrix::with_shape(data.clone(), params.k, params.row_size).unwrap();
        (data, ExtendedData::generate(&original, params).unwrap())
    }

    fn siblings(ext: &ExtendedData, params: &Parameters) -> Vec<[u8; 32]> {
        let proof = ext.generate_row_proof(0).unwrap().row_proof;
        let depth = (params.total_padded() / params.k_padded()).trailing_zeros() as usize;
        proof[proof.len() - depth..].to_vec()
    }

    #[test]
    fn matches_encoder() {
        for (i, (k, n, row_size)) in [
            (1, 1, 64),
            (4, 4, 64),
            (4, 12, 256),
            (5, 7, 128),
            (16, 48, 64),
            (64, 64, 320),
            (3, 100, 64),
        ]
        .into_iter()
        .enumerate()
        {
            let params = Parameters::new(k, n, row_size).unwrap();
            let (data, ext) = encode(&params, i as u64);
            let rows: Vec<&[u8]> = data.chunks_exact(row_size).collect();
            let commitment =
                commitment_from_original_rows(&rows, &params, &siblings(&ext, &params)).unwrap();
            assert_eq!(
                commitment,
                ext.commitment(),
                "k={k} n={n} row_size={row_size}"
            );
        }
    }

    #[test]
    fn tampering_changes_commitment() {
        let params = Parameters::new(4, 12, 64).unwrap();
        let (mut data, ext) = encode(&params, 1);
        let mut siblings = siblings(&ext, &params);

        data[100] ^= 1;
        let rows: Vec<&[u8]> = data.chunks_exact(params.row_size).collect();
        let tampered = commitment_from_original_rows(&rows, &params, &siblings).unwrap();
        assert_ne!(tampered, ext.commitment());

        data[100] ^= 1;
        siblings[0][0] ^= 1;
        let rows: Vec<&[u8]> = data.chunks_exact(params.row_size).collect();
        let tampered = commitment_from_original_rows(&rows, &params, &siblings).unwrap();
        assert_ne!(tampered, ext.commitment());
    }

    #[test]
    fn rejects_malformed_input() {
        let params = Parameters::new(4, 12, 64).unwrap();
        let row: &[u8] = &[0u8; 64];
        let short: &[u8] = &[0u8; 32];
        let siblings = [[0u8; 32]; 2];

        assert!(matches!(
            commitment_from_original_rows(&[row; 3], &params, &siblings),
            Err(Error::InvalidParameters(_))
        ));
        assert!(matches!(
            commitment_from_original_rows(&[row, row, short, row], &params, &siblings),
            Err(Error::RowLengthMismatch {
                expected: 64,
                actual: 32
            })
        ));
        assert!(matches!(
            commitment_from_original_rows(&[row; 4], &params, &siblings[..1]),
            Err(Error::InvalidParameters(_))
        ));
    }
}
