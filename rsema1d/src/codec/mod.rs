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
pub use rows::{ExtendedRowsView, OriginalRowsView, RowMatrix};
pub use rs::{
    encode_parity_in_place, extend_data, extend_rlcs, pack_gf128_to_shard, unpack_shard_to_gf128,
};
pub use symbols::{compute_rlc, extract_symbols};
pub use verification::{
    create_verification_context, verify_proof, verify_row_inclusion, verify_row_inclusion_proof,
    verify_row_with_context, verify_standalone, verify_standalone_proof, verify_with_context,
    VerificationContext,
};

pub type Commitment = [u8; 32];

/// Encode original rows and return extended data, commitment, and original RLCs.
pub fn encode(
    data: &RowMatrix,
    params: &Parameters,
) -> Result<(ExtendedData, Commitment, Vec<GF128>)> {
    let ext_data = ExtendedData::generate(data, params)?;
    let commitment = ext_data.commitment();
    let rlc_orig = ext_data.rlc_original().to_vec();
    Ok((ext_data, commitment, rlc_orig))
}

/// Encode original rows into a caller-provided extended row buffer.
///
/// This performs the same pipeline as [`encode`]:
/// 1. copy original rows into the first K rows of `extended_rows`
/// 2. compute parity rows in place
/// 3. build commitment, trees, and RLCs from the extended rows
pub fn encode_in_place(
    data: &RowMatrix,
    mut extended_rows: RowMatrix,
    params: &Parameters,
) -> Result<(ExtendedData, Commitment, Vec<GF128>)> {
    let data_view = data.original_view(params)?;
    extended_rows.extended_view(params)?;

    let split_at = params.k * params.row_size;
    extended_rows.as_row_major_mut()[..split_at].copy_from_slice(data_view.as_row_major());
    encode_parity_in_place(&mut extended_rows, params)?;

    let ext_data = ExtendedData::generate_from_extended_rows(extended_rows, params)?;
    let commitment = ext_data.commitment();
    let rlc_orig = ext_data.rlc_original().to_vec();
    Ok((ext_data, commitment, rlc_orig))
}

/// Compute commitment/proofs from already-extended rows.
pub fn encode_parity(
    extended_rows: RowMatrix,
    params: &Parameters,
) -> Result<(ExtendedData, Commitment, Vec<GF128>)> {
    let ext_data = ExtendedData::generate_from_extended_rows(extended_rows, params)?;
    let commitment = ext_data.commitment();
    let rlc_orig = ext_data.rlc_original().to_vec();
    Ok((ext_data, commitment, rlc_orig))
}

/// Reconstruct original rows from any K sampled rows.
pub fn reconstruct(rows: &RowMatrix, indices: &[usize], params: &Parameters) -> Result<RowMatrix> {
    reconstruct_data(rows, indices, params)
}
