//! Rows to reconstruct from come from peers. Reconstruction must never panic, and must
//! return the original data whenever it is given at least K distinct genuine rows.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use rsema1d::reconstruct_with_work_budget;
use rsema1d_fuzz::{Square, xorshift};
use std::collections::HashSet;

#[derive(Debug, Arbitrary)]
enum Mutation {
    Drop(u8),
    Duplicate(u8),
    Index { shard: u8, index: u8 },
    Flip { shard: u8, pos: u16, bit: u8 },
    Resize { shard: u8, len: u16 },
}

#[derive(Debug, Arbitrary)]
struct Input {
    square: Square,
    /// Picks which rows are sent; most inputs then start from K or more genuine ones.
    seed: u64,
    extra: u8,
    mutations: Vec<Mutation>,
}

fuzz_target!(|input: Input| {
    let ext = input.square.encode();
    let params = *ext.params();

    let mut order: Vec<usize> = (0..params.total_rows()).collect();
    let mut x = input.seed | 1;
    for i in (1..order.len()).rev() {
        order.swap(i, xorshift(&mut x) as usize % (i + 1));
    }
    order.truncate(params.k + usize::from(input.extra) % (params.n + 1));
    let mut shards: Vec<(usize, Vec<u8>)> = order
        .into_iter()
        .map(|i| (i, ext.row(i).unwrap().to_vec()))
        .collect();

    for mutation in input.mutations {
        if shards.is_empty() {
            break;
        }
        let len = shards.len();
        let pick = |shard: u8| usize::from(shard) % len;
        match mutation {
            Mutation::Drop(shard) => {
                shards.remove(pick(shard));
            }
            Mutation::Duplicate(shard) => shards.push(shards[pick(shard)].clone()),
            Mutation::Index { shard, index } => shards[pick(shard)].0 = usize::from(index),
            Mutation::Flip { shard, pos, bit } => {
                let row = &mut shards[pick(shard)].1;
                if !row.is_empty() {
                    let pos = usize::from(pos) % row.len();
                    row[pos] ^= 1 << (bit % 8);
                }
            }
            Mutation::Resize { shard, len } => shards[pick(shard)].1.resize(usize::from(len), 0),
        }
    }

    let indices: Vec<usize> = shards.iter().map(|(i, _)| *i).collect();
    let rows: Vec<&[u8]> = shards.iter().map(|(_, row)| row.as_slice()).collect();
    let result = input.square.pool().install(|| {
        reconstruct_with_work_budget(&rows, &indices, &params, input.square.work_budget())
    });

    let genuine = shards
        .iter()
        .all(|(i, row)| ext.row(*i).is_ok_and(|r| r == row.as_slice()));
    let distinct = indices.iter().collect::<HashSet<_>>().len() == indices.len();
    if genuine && distinct && indices.len() >= params.k {
        let original = &ext.rows().as_row_major()[..params.k * params.row_size];
        assert_eq!(result.unwrap().as_row_major(), original);
    }
});
