//! Reed-Solomon encoding/decoding with RLC integration.

mod commitment;
mod padding;
mod proof;
mod reconstruct;
mod rows;
mod rs;
mod symbols;
mod verification;

use crate::error::Result;
use crate::field::GF128;
use crate::params::Parameters;

pub use commitment::ExtendedData;
pub use padding::map_index_to_tree_position;
pub use proof::{RowInclusionProof, RowProof, StandaloneProof};
pub use reconstruct::reconstruct_data;
pub use rows::{ExtendedRows, OriginalRows, RowMatrix, SampledRows};
pub use rs::{extend_data, extend_rlcs, pack_gf128_to_shard, unpack_shard_to_gf128};
pub use symbols::{compute_rlc, extract_symbols};
pub use verification::{
    create_verification_context, verify_proof, verify_row_inclusion, verify_row_inclusion_proof,
    verify_row_with_context, verify_standalone, verify_standalone_proof, verify_with_context,
    VerificationContext,
};

pub type Commitment = [u8; 32];

/// Encode typed original rows and return extended data, commitment, and original RLCs.
pub fn encode(
    data: &OriginalRows,
    params: &Parameters,
) -> Result<(ExtendedData, Commitment, Vec<GF128>)> {
    let ext_data = ExtendedData::generate(data, params)?;
    let commitment = ext_data.commitment();
    let rlc_orig = ext_data.rlc_original().to_vec();
    Ok((ext_data, commitment, rlc_orig))
}

/// Compute commitment/proofs from typed already-extended rows.
pub fn encode_parity(
    extended_rows: ExtendedRows,
    params: &Parameters,
) -> Result<(ExtendedData, Commitment, Vec<GF128>)> {
    let ext_data = ExtendedData::generate_from_extended_rows(extended_rows, params)?;
    let commitment = ext_data.commitment();
    let rlc_orig = ext_data.rlc_original().to_vec();
    Ok((ext_data, commitment, rlc_orig))
}

/// Reconstruct typed original rows from any K sampled rows.
pub fn reconstruct(
    rows: &SampledRows,
    indices: &[usize],
    params: &Parameters,
) -> Result<OriginalRows> {
    reconstruct_data(rows, indices, params)
}
