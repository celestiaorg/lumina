#![no_main]

use arbitrary::Arbitrary;
use celestia_types::eds::RawExtendedDataSquare;
use celestia_types::{DataAvailabilityHeader, ExtendedDataSquare};
use celestia_types_fuzz::mutate::{ListMutation, ShareMutation, pick};
use celestia_types_fuzz::square;
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
enum Mutation {
    Shares(ListMutation),
    Share { share: u16, mutation: ShareMutation },
    Codec(String),
}

#[derive(Arbitrary, Debug)]
struct Input {
    square: u8,
    mutations: Vec<Mutation>,
}

// Mutated squares received from a bridge node by `share_get_eds` in `rpc/src/share.rs`.
// Panics if a square other than the original one has the original DAH.
fuzz_target!(|input: Input| {
    let square = square(input.square);
    let mut raw = RawExtendedDataSquare::from(square.eds.clone());

    for mutation in &input.mutations {
        match mutation {
            Mutation::Shares(mutation) => mutation.apply(&mut raw.data_square),
            Mutation::Share { share, mutation } => {
                if let Some(share) = pick(&mut raw.data_square, *share) {
                    mutation.apply(share, square);
                }
            }
            Mutation::Codec(codec) => raw.codec = codec.clone(),
        }
    }

    let Ok(eds) = ExtendedDataSquare::from_raw(raw) else {
        return;
    };
    if DataAvailabilityHeader::from_eds(&eds) == square.dah {
        assert_eq!(eds.data_square(), square.eds.data_square());
    }
});
