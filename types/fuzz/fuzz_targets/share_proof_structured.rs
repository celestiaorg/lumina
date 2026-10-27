#![no_main]

use arbitrary::Arbitrary;
use celestia_types_fuzz::mutate::{
    ListMutation, MerkleMutation, Node, Ns, ProofMutation, ShareMutation, pick,
};
use celestia_types_fuzz::square;
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
enum Mutation {
    Data(ListMutation),
    Share { share: u8, mutation: ShareMutation },
    NamespaceId(Vec<u8>),
    NamespaceVersion(u32),
    ShareProofs(ListMutation),
    ShareProof { proof: u8, mutation: ProofMutation },
    RowRoots(ListMutation),
    RowRoot { root: u8, node: Node },
    RowProofs(ListMutation),
    RowProof { proof: u8, mutation: MerkleMutation },
    StartRow(u32),
    EndRow(u32),
    RemoveRowProof,
}

#[derive(Arbitrary, Debug)]
struct Input {
    square: u8,
    namespace: Ns,
    /// Offset of the first proven share within the namespace.
    start: u16,
    len: u16,
    mutations: Vec<Mutation>,
}

// Mutated valid share proofs, checked like in `share_proof`.
fuzz_target!(|input: Input| {
    let square = square(input.square);
    let Some(namespace) = input.namespace.namespace() else {
        return;
    };
    let range = square.namespace_range(namespace);
    if range.is_empty() {
        return;
    }
    let start = range.start + usize::from(input.start) % range.len();
    let end = start + 1 + usize::from(input.len) % (range.end - start);
    let mut raw = square.share_proof(namespace, start..end);

    for mutation in &input.mutations {
        let row_proof = raw.row_proof.as_mut();
        match mutation {
            Mutation::Data(mutation) => mutation.apply(&mut raw.data),
            Mutation::Share { share, mutation } => {
                if let Some(share) = pick(&mut raw.data, *share) {
                    mutation.apply(share, square);
                }
            }
            Mutation::NamespaceId(id) => raw.namespace_id = id.clone(),
            Mutation::NamespaceVersion(version) => raw.namespace_version = *version,
            Mutation::ShareProofs(mutation) => mutation.apply(&mut raw.share_proofs),
            Mutation::ShareProof { proof, mutation } => {
                if let Some(proof) = pick(&mut raw.share_proofs, *proof) {
                    mutation.apply_nmt(proof, square);
                }
            }
            Mutation::RowRoots(mutation) => {
                if let Some(row_proof) = row_proof {
                    mutation.apply(&mut row_proof.row_roots);
                }
            }
            Mutation::RowRoot { root, node } => {
                if let Some(root) = row_proof.and_then(|p| pick(&mut p.row_roots, *root)) {
                    *root = node.bytes(square);
                }
            }
            Mutation::RowProofs(mutation) => {
                if let Some(row_proof) = row_proof {
                    mutation.apply(&mut row_proof.proofs);
                }
            }
            Mutation::RowProof { proof, mutation } => {
                if let Some(proof) = row_proof.and_then(|p| pick(&mut p.proofs, *proof)) {
                    mutation.apply(proof, square);
                }
            }
            Mutation::StartRow(row) => {
                if let Some(row_proof) = row_proof {
                    row_proof.start_row = *row;
                }
            }
            Mutation::EndRow(row) => {
                if let Some(row_proof) = row_proof {
                    row_proof.end_row = *row;
                }
            }
            Mutation::RemoveRowProof => raw.row_proof = None,
        }
    }

    square.check_share_proof(raw);
});
