#![no_main]

use arbitrary::Arbitrary;
use celestia_proto::shwap::row::HalfSide;
use celestia_proto::shwap::{Row as RawRow, Share as RawShare};
use celestia_types::row::RowId;
use celestia_types_fuzz::mutate::{ListMutation, ShareMutation, index, pick};
use celestia_types_fuzz::{HEIGHT, square};
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
enum Mutation {
    Shares(ListMutation),
    Share { share: u8, mutation: ShareMutation },
    HalfSide(i32),
}

#[derive(Arbitrary, Debug)]
struct Input {
    square: u8,
    index: u16,
    /// Row the response is built from, if not the requested one.
    source: Option<u16>,
    right: bool,
    mutations: Vec<Mutation>,
}

// Mutated valid `Row` responses, sending either half, checked like in `row`.
fuzz_target!(|input: Input| {
    let square = square(input.square);
    let width = square.eds.square_width();
    let id = RowId::new(index(input.index, width), HEIGHT).unwrap();

    let shares = square
        .eds
        .row(input.source.unwrap_or(input.index) % width)
        .unwrap();
    let (half, side) = shares.split_at(shares.len() / 2);
    let (half, half_side) = if input.right {
        (side, HalfSide::Right)
    } else {
        (half, HalfSide::Left)
    };
    let mut raw = RawRow {
        shares_half: half
            .iter()
            .map(|share| RawShare {
                data: share.to_vec(),
            })
            .collect(),
        half_side: half_side.into(),
    };

    for mutation in &input.mutations {
        match mutation {
            Mutation::Shares(mutation) => mutation.apply(&mut raw.shares_half),
            Mutation::Share { share, mutation } => {
                if let Some(share) = pick(&mut raw.shares_half, *share) {
                    mutation.apply(&mut share.data, square);
                }
            }
            Mutation::HalfSide(half_side) => raw.half_side = *half_side,
        }
    }

    square.check_row(id, raw);
});
