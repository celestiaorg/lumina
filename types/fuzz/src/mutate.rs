//! Structured mutations of valid shwap responses.
//!
//! Structured targets build a valid response for one of [`SQUARES`](crate::SQUARES)
//! and apply arbitrary mutations to it, so that fuzzing spends its time in proof
//! verification rather than in the protobuf decoder.

use std::mem;

use arbitrary::Arbitrary;
use celestia_proto::celestia::core::v1::proof::{
    NmtProof as RawNmtProof, Proof as RawMerkleProof, RowProof as RawRowProof,
};
use celestia_proto::proof::pb::Proof as RawProof;
use celestia_types::AxisType;
use celestia_types::nmt::{NS_SIZE, Namespace, NamespacedHashExt};
use celestia_types::sample::Sample;

use crate::{Square, namespace};

/// Index within a square of the given width, or just past its edge.
pub fn index(raw: u16, width: u16) -> u16 {
    raw % (width + 1)
}

/// Item at `index`, wrapping around the length of `items`.
pub fn pick<T>(items: &mut [T], index: impl Into<usize>) -> Option<&mut T> {
    let len = items.len();
    items.get_mut(index.into().checked_rem(len)?)
}

#[derive(Arbitrary, Debug, Clone, Copy)]
pub enum Axis {
    Row,
    Col,
}

impl From<Axis> for AxisType {
    fn from(axis: Axis) -> AxisType {
        match axis {
            Axis::Row => AxisType::Row,
            Axis::Col => AxisType::Col,
        }
    }
}

/// A namespace, biased towards the ones around the data of the squares.
#[derive(Arbitrary, Debug)]
pub enum Ns {
    /// See [`namespace`].
    Id(u8),
    TailPadding,
    Parity,
    Reserved(u8),
    Raw([u8; NS_SIZE]),
}

impl Ns {
    pub fn bytes(&self) -> [u8; NS_SIZE] {
        let ns = match self {
            Ns::Id(id) => namespace((*id).into()),
            Ns::TailPadding => Namespace::TAIL_PADDING,
            Ns::Parity => Namespace::PARITY_SHARE,
            Ns::Reserved(id) => Namespace::const_v0([0, 0, 0, 0, 0, 0, 0, 0, 0, *id]),
            Ns::Raw(bytes) => return *bytes,
        };
        ns.as_bytes().try_into().unwrap()
    }

    /// The namespace, if its bytes are a valid one.
    pub fn namespace(&self) -> Option<Namespace> {
        Namespace::from_raw(&self.bytes()).ok()
    }
}

/// A proof node.
#[derive(Arbitrary, Debug)]
pub enum Node {
    /// A sibling from the proof of a share of the square.
    Sibling {
        row: u16,
        col: u16,
        axis: Axis,
        index: u8,
    },
    /// A node with the given namespace range.
    Namespaced {
        min: Ns,
        max: Ns,
        hash: [u8; 32],
    },
    Bytes(Vec<u8>),
}

impl Node {
    pub fn bytes(&self, square: &Square) -> Vec<u8> {
        match self {
            Node::Sibling {
                row,
                col,
                axis,
                index,
            } => {
                let width = square.eds.square_width();
                let sample =
                    Sample::new(row % width, col % width, (*axis).into(), &square.eds).unwrap();
                let siblings = sample.proof.siblings();
                siblings[usize::from(*index) % siblings.len()].to_vec()
            }
            Node::Namespaced { min, max, hash } => [&min.bytes()[..], &max.bytes(), hash].concat(),
            Node::Bytes(bytes) => bytes.clone(),
        }
    }
}

#[derive(Arbitrary, Debug)]
pub enum ProofMutation {
    Start(i64),
    End(i64),
    /// Moves the proven range.
    Shift(i8),
    SetNode {
        index: u8,
        node: Node,
    },
    InsertNode {
        index: u8,
        node: Node,
    },
    RemoveNode(u8),
    /// Sets the leaf hash, which turns the proof into an absence proof, or clears it.
    LeafHash(Option<Node>),
    IgnoreMaxNs(bool),
}

impl ProofMutation {
    pub fn apply(&self, proof: &mut RawProof, square: &Square) {
        match self {
            ProofMutation::Start(start) => proof.start = *start,
            ProofMutation::End(end) => proof.end = *end,
            ProofMutation::Shift(by) => {
                proof.start = proof.start.wrapping_add((*by).into());
                proof.end = proof.end.wrapping_add((*by).into());
            }
            ProofMutation::SetNode { index, node } => {
                if let Some(slot) = pick(&mut proof.nodes, *index) {
                    *slot = node.bytes(square);
                }
            }
            ProofMutation::InsertNode { index, node } => {
                let index = usize::from(*index) % (proof.nodes.len() + 1);
                proof.nodes.insert(index, node.bytes(square));
            }
            ProofMutation::RemoveNode(index) => {
                if !proof.nodes.is_empty() {
                    proof.nodes.remove(usize::from(*index) % proof.nodes.len());
                }
            }
            ProofMutation::LeafHash(node) => {
                proof.leaf_hash = node
                    .as_ref()
                    .map(|node| node.bytes(square))
                    .unwrap_or_default();
            }
            ProofMutation::IgnoreMaxNs(ignore) => proof.is_max_namespace_ignored = *ignore,
        }
    }
}

impl ProofMutation {
    /// Applies the mutation to the proof format used by share proofs, which always
    /// ignores the maximum namespace.
    pub fn apply_nmt(&self, proof: &mut RawNmtProof, square: &Square) {
        let mut raw = RawProof {
            start: proof.start.into(),
            end: proof.end.into(),
            nodes: mem::take(&mut proof.nodes),
            leaf_hash: mem::take(&mut proof.leaf_hash),
            is_max_namespace_ignored: true,
        };
        self.apply(&mut raw, square);
        proof.start = raw.start as i32;
        proof.end = raw.end as i32;
        proof.nodes = raw.nodes;
        proof.leaf_hash = raw.leaf_hash;
    }
}

/// A mutation of a proof of a row root in the data root tree.
#[derive(Arbitrary, Debug)]
pub enum MerkleMutation {
    Total(i64),
    Index(i64),
    LeafHash(Vec<u8>),
    Aunts(ListMutation),
    Aunt {
        aunt: u8,
        hash: Vec<u8>,
    },
    /// Replaces an aunt with one from the proof of another row.
    AuntOf {
        row: u16,
        aunt: u8,
    },
}

impl MerkleMutation {
    pub fn apply(&self, proof: &mut RawMerkleProof, square: &Square) {
        match self {
            MerkleMutation::Total(total) => proof.total = *total,
            MerkleMutation::Index(index) => proof.index = *index,
            MerkleMutation::LeafHash(hash) => proof.leaf_hash = hash.clone(),
            MerkleMutation::Aunts(mutation) => mutation.apply(&mut proof.aunts),
            MerkleMutation::Aunt { aunt, hash } => {
                if let Some(aunt) = pick(&mut proof.aunts, *aunt) {
                    *aunt = hash.clone();
                }
            }
            MerkleMutation::AuntOf { row, aunt } => {
                let rows = square.eds.square_width();
                let row = row % rows;
                let mut other = RawRowProof::from(square.dah.row_proof(row..=row).unwrap());
                let other = pick(&mut other.proofs[0].aunts, *aunt).cloned();
                if let (Some(slot), Some(other)) = (pick(&mut proof.aunts, *aunt), other) {
                    *slot = other;
                }
            }
        }
    }
}

#[derive(Arbitrary, Debug)]
pub enum ShareMutation {
    Byte {
        offset: u16,
        value: u8,
    },
    Namespace(Ns),
    /// Replaces the share with another one of the square.
    Share {
        row: u16,
        col: u16,
    },
    Resize(u16),
}

impl ShareMutation {
    pub fn apply(&self, share: &mut Vec<u8>, square: &Square) {
        match self {
            ShareMutation::Byte { offset, value } => {
                if let Some(byte) = pick(share, *offset) {
                    *byte = *value;
                }
            }
            ShareMutation::Namespace(ns) => {
                if share.len() >= NS_SIZE {
                    share[..NS_SIZE].copy_from_slice(&ns.bytes());
                }
            }
            ShareMutation::Share { row, col } => {
                let width = square.eds.square_width();
                *share = square.eds.share(row % width, col % width).unwrap().to_vec();
            }
            ShareMutation::Resize(len) => share.resize((*len).into(), 0),
        }
    }
}

#[derive(Arbitrary, Debug)]
pub enum ListMutation {
    Remove(u8),
    Duplicate(u8),
    Swap(u8, u8),
}

impl ListMutation {
    pub fn apply<T: Clone>(&self, items: &mut Vec<T>) {
        let len = items.len();
        if len == 0 {
            return;
        }
        match self {
            ListMutation::Remove(i) => {
                items.remove(usize::from(*i) % len);
            }
            ListMutation::Duplicate(i) => {
                let i = usize::from(*i) % len;
                items.insert(i, items[i].clone());
            }
            ListMutation::Swap(i, j) => items.swap(usize::from(*i) % len, usize::from(*j) % len),
        }
    }
}
