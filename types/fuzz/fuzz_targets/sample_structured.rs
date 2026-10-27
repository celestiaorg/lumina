#![no_main]

use arbitrary::Arbitrary;
use celestia_proto::shwap::Sample as RawSample;
use celestia_types::sample::{Sample, SampleId};
use celestia_types_fuzz::mutate::{Axis, ProofMutation, ShareMutation, index};
use celestia_types_fuzz::{HEIGHT, square};
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
enum Mutation {
    Share(ShareMutation),
    Proof(ProofMutation),
    ProofType(i32),
}

#[derive(Arbitrary, Debug)]
struct Input {
    square: u8,
    row: u16,
    col: u16,
    axis: Axis,
    /// Share the response is built from, if not the requested one.
    source: Option<(u16, u16)>,
    mutations: Vec<Mutation>,
}

// Mutated valid `Sample` responses, checked like in `sample`.
fuzz_target!(|input: Input| {
    let square = square(input.square);
    let width = square.eds.square_width();
    let id = SampleId::new(index(input.row, width), index(input.col, width), HEIGHT).unwrap();

    let (row, col) = input.source.unwrap_or((input.row, input.col));
    let sample = Sample::new(row % width, col % width, input.axis.into(), &square.eds).unwrap();
    let mut raw = RawSample::from(sample);

    for mutation in &input.mutations {
        match mutation {
            Mutation::Share(mutation) => {
                mutation.apply(&mut raw.share.as_mut().unwrap().data, square)
            }
            Mutation::Proof(mutation) => mutation.apply(raw.proof.as_mut().unwrap(), square),
            Mutation::ProofType(proof_type) => raw.proof_type = *proof_type,
        }
    }

    square.check_sample(id, raw);
});
