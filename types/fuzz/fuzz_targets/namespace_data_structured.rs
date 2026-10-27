#![no_main]

use arbitrary::Arbitrary;
use celestia_proto::shwap::RowNamespaceData as RawRowNamespaceData;
use celestia_types::namespace_data::NamespaceDataId;
use celestia_types::nmt::Namespace;
use celestia_types_fuzz::mutate::{ListMutation, Ns, ProofMutation, ShareMutation, pick};
use celestia_types_fuzz::{HEIGHT, Square, square};
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
enum Mutation {
    Rows(ListMutation),
    /// Replaces a row, or appends one if `None`, with a row of another namespace.
    Row {
        row: Option<u8>,
        namespace: Ns,
        source: u8,
    },
    Shares {
        row: u8,
        mutation: ListMutation,
    },
    Share {
        row: u8,
        share: u8,
        mutation: ShareMutation,
    },
    Proof {
        row: u8,
        mutation: ProofMutation,
    },
}

#[derive(Arbitrary, Debug)]
struct Input {
    square: u8,
    namespace: Ns,
    mutations: Vec<Mutation>,
}

fn raw_rows(square: &Square, namespace: Namespace) -> Vec<RawRowNamespaceData> {
    square
        .eds
        .get_namespace_data(namespace, &square.dah, HEIGHT)
        .unwrap()
        .into_iter()
        .map(|(_, row)| row.into())
        .collect()
}

// Mutated valid `NamespaceData` responses, checked like in `namespace_data`.
fuzz_target!(|input: Input| {
    let square = square(input.square);
    let Some(namespace) = input.namespace.namespace() else {
        return;
    };
    let id = NamespaceDataId::new(namespace, HEIGHT).unwrap();
    let mut rows = raw_rows(square, namespace);

    for mutation in &input.mutations {
        match mutation {
            Mutation::Rows(mutation) => mutation.apply(&mut rows),
            Mutation::Row {
                row,
                namespace,
                source,
            } => {
                let Some(namespace) = namespace.namespace() else {
                    continue;
                };
                let mut other = raw_rows(square, namespace);
                let Some(other) = pick(&mut other, *source).cloned() else {
                    continue;
                };
                match row {
                    Some(row) => {
                        if let Some(row) = pick(&mut rows, *row) {
                            *row = other;
                        }
                    }
                    None => rows.push(other),
                }
            }
            Mutation::Shares { row, mutation } => {
                if let Some(row) = pick(&mut rows, *row) {
                    mutation.apply(&mut row.shares);
                }
            }
            Mutation::Share {
                row,
                share,
                mutation,
            } => {
                if let Some(share) =
                    pick(&mut rows, *row).and_then(|row| pick(&mut row.shares, *share))
                {
                    mutation.apply(&mut share.data, square);
                }
            }
            Mutation::Proof { row, mutation } => {
                if let Some(proof) = pick(&mut rows, *row).and_then(|row| row.proof.as_mut()) {
                    mutation.apply(proof, square);
                }
            }
        }
    }

    square.check_namespace_data(id, rows);
});
