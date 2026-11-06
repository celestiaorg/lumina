//! The encoder's batched, multi-threaded RLCs must match the per-row reference, and the
//! log-table RLCs in `commitment_from_original_rows` must give the same commitment.

#![no_main]

use libfuzzer_sys::fuzz_target;
use rsema1d::codec::compute_rlc;
use rsema1d::commitment_from_original_rows;
use rsema1d::crypto::derive_coefficients;
use rsema1d_fuzz::Square;

fuzz_target!(|square: Square| {
    let ext = square.encode();
    let params = *ext.params();
    let coefficients = derive_coefficients(&ext.row_root(), params.k, params.n, params.row_size);
    for (i, rlc) in ext.rlc_original().iter().enumerate() {
        assert_eq!(
            *rlc,
            compute_rlc(ext.row(i).unwrap(), &coefficients),
            "row {i}"
        );
    }

    let rows: Vec<&[u8]> = (0..params.k).map(|i| ext.row(i).unwrap()).collect();
    let row_proof = ext.generate_row_proof(0).unwrap().row_proof;
    let depth = (params.total_padded() / params.k_padded()).trailing_zeros() as usize;
    let siblings = &row_proof[row_proof.len() - depth..];
    let commitment = square
        .pool()
        .install(|| commitment_from_original_rows(&rows, &params, siblings))
        .unwrap();
    assert_eq!(commitment, ext.commitment());
});
