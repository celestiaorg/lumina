//! Rows to reconstruct from come from peers. Reconstruction must never panic, and must
//! return the original data whenever it is given at least K distinct genuine rows.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use rsema1d::reconstruct_with_work_budget;
use rsema1d_fuzz::Square;
use std::collections::HashSet;

#[derive(Debug, Arbitrary)]
struct Shard {
    index: u8,
    flip: Option<(u16, u8)>,
    len: Option<u16>,
}

#[derive(Debug, Arbitrary)]
struct Input {
    square: Square,
    shards: Vec<Shard>,
}

fuzz_target!(|input: Input| {
    let ext = input.square.encode();
    let params = *ext.params();

    let indices: Vec<usize> = input.shards.iter().map(|s| usize::from(s.index)).collect();
    let rows: Vec<Vec<u8>> = input
        .shards
        .iter()
        .map(|shard| {
            let mut row = match ext.row(usize::from(shard.index)) {
                Ok(row) => row.to_vec(),
                Err(_) => vec![0; params.row_size],
            };
            if let Some((pos, bit)) = shard.flip {
                let pos = usize::from(pos) % row.len();
                row[pos] ^= 1 << (bit % 8);
            }
            if let Some(len) = shard.len {
                row.resize(usize::from(len), 0);
            }
            row
        })
        .collect();
    let refs: Vec<&[u8]> = rows.iter().map(Vec::as_slice).collect();

    let result = input.square.pool().install(|| {
        reconstruct_with_work_budget(&refs, &indices, &params, input.square.work_budget())
    });

    let genuine = rows
        .iter()
        .zip(&indices)
        .all(|(row, &i)| ext.row(i).is_ok_and(|r| r == row.as_slice()));
    let distinct = indices.iter().collect::<HashSet<_>>().len() == indices.len();
    if genuine && distinct && indices.len() >= params.k {
        let original = &ext.rows().as_row_major()[..params.k * params.row_size];
        assert_eq!(result.unwrap().as_row_major(), original);
    }
});
