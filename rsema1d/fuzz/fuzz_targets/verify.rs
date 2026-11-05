//! Proofs and RLCs come from peers. Mutate valid ones and check that verification never
//! panics, accepts only the encoded row at the claimed index, and accepts unmutated proofs.

#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use rsema1d::{
    RowInclusionProof, RowProof, StandaloneProof, VerificationContext, verify_row_inclusion,
    verify_standalone, verify_with_context,
};
use rsema1d_fuzz::Square;

#[derive(Debug, Arbitrary)]
enum Kind {
    Row,
    Standalone,
    Inclusion,
}

#[derive(Debug, Arbitrary)]
enum Mutation {
    Index(u8),
    FlipRow {
        pos: u16,
        bit: u8,
    },
    ResizeRow(u16),
    FlipNode {
        rlc: bool,
        node: u8,
        pos: u8,
        bit: u8,
    },
    SwapNodes {
        rlc: bool,
        a: u8,
        b: u8,
    },
    PopNode {
        rlc: bool,
    },
    PushNode {
        rlc: bool,
        node: [u8; 32],
    },
    FlipRlcRoot {
        pos: u8,
        bit: u8,
    },
}

#[derive(Debug, Arbitrary)]
struct Case {
    kind: Kind,
    index: u8,
    mutations: Vec<Mutation>,
}

#[derive(Debug, Arbitrary)]
struct Input {
    square: Square,
    /// (row, limb, xor) applied to the original RLCs the context is built from.
    rlc_flips: Vec<(u8, u8, u16)>,
    /// Verified in order against one context, which caches coefficients between calls.
    cases: Vec<Case>,
}

fn nodes<'a>(
    rlc: bool,
    row_proof: &'a mut Vec<[u8; 32]>,
    rlc_proof: &'a mut Vec<[u8; 32]>,
) -> &'a mut Vec<[u8; 32]> {
    if rlc { rlc_proof } else { row_proof }
}

fuzz_target!(|input: Input| {
    let ext = input.square.encode();
    let params = *ext.params();
    let commitment = ext.commitment();

    let mut rlcs = ext.rlc_original().to_vec();
    for &(row, limb, xor) in &input.rlc_flips {
        rlcs[usize::from(row) % params.k].limbs[usize::from(limb) % 8] ^= xor;
    }
    let context = VerificationContext::new(&rlcs, &params).unwrap();

    for case in input.cases {
        let honest = case.mutations.is_empty()
            && match case.kind {
                Kind::Row => rlcs == ext.rlc_original(),
                Kind::Standalone => usize::from(case.index) % params.total_rows() < params.k,
                Kind::Inclusion => true,
            };
        let mut index = usize::from(case.index) % params.total_rows();
        let mut row = ext.row(index).unwrap().to_vec();
        let mut row_proof = ext.generate_row_proof(index).unwrap().row_proof;
        let mut rlc_proof = match ext.generate_standalone_proof(index) {
            Ok(proof) => proof.rlc_proof,
            Err(_) => Vec::new(),
        };
        let mut rlc_root = ext.rlc_root();

        for mutation in case.mutations {
            match mutation {
                Mutation::Index(i) => index = usize::from(i),
                Mutation::FlipRow { pos, bit } => {
                    if !row.is_empty() {
                        let pos = usize::from(pos) % row.len();
                        row[pos] ^= 1 << (bit % 8);
                    }
                }
                Mutation::ResizeRow(len) => row.resize(usize::from(len), 0),
                Mutation::FlipNode {
                    rlc,
                    node,
                    pos,
                    bit,
                } => {
                    let nodes = nodes(rlc, &mut row_proof, &mut rlc_proof);
                    if !nodes.is_empty() {
                        let node = usize::from(node) % nodes.len();
                        nodes[node][usize::from(pos) % 32] ^= 1 << (bit % 8);
                    }
                }
                Mutation::SwapNodes { rlc, a, b } => {
                    let nodes = nodes(rlc, &mut row_proof, &mut rlc_proof);
                    if !nodes.is_empty() {
                        let len = nodes.len();
                        nodes.swap(usize::from(a) % len, usize::from(b) % len);
                    }
                }
                Mutation::PopNode { rlc } => {
                    nodes(rlc, &mut row_proof, &mut rlc_proof).pop();
                }
                Mutation::PushNode { rlc, node } => {
                    nodes(rlc, &mut row_proof, &mut rlc_proof).push(node);
                }
                Mutation::FlipRlcRoot { pos, bit } => {
                    rlc_root[usize::from(pos) % 32] ^= 1 << (bit % 8);
                }
            }
        }

        let result = match case.kind {
            Kind::Row => {
                let proof = RowProof {
                    index,
                    row: row.clone().into(),
                    row_proof,
                };
                input
                    .square
                    .pool()
                    .install(|| verify_with_context(&proof, &commitment, &context))
            }
            Kind::Standalone => {
                let proof = StandaloneProof {
                    index,
                    row: row.clone(),
                    row_proof,
                    rlc_proof,
                };
                verify_standalone(&proof, &commitment, &params)
            }
            Kind::Inclusion => {
                let proof = RowInclusionProof {
                    index,
                    row: row.clone().into(),
                    row_proof,
                    rlc_root,
                };
                verify_row_inclusion(&proof, &commitment, &params)
            }
        };
        if honest {
            assert!(result.is_ok(), "rejected an honest proof: {result:?}");
        }
        let Ok(accepted) = result else { continue };
        assert!(accepted, "verification returned Ok(false)");
        assert_eq!(
            Some(row.as_slice()),
            ext.row(index).ok(),
            "accepted a wrong row at {index}"
        );
        match case.kind {
            Kind::Row => assert_eq!(rlcs, ext.rlc_original(), "accepted against wrong RLCs"),
            Kind::Inclusion => assert_eq!(rlc_root, ext.rlc_root(), "accepted a wrong RLC root"),
            Kind::Standalone => {}
        }
    }
});
