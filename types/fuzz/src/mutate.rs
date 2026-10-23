//! Structured mutations of valid shwap responses.
//!
//! Structured targets build a valid response for one of [`SQUARES`](crate::SQUARES)
//! and apply arbitrary mutations to it, so that fuzzing spends its time in proof
//! verification rather than in the protobuf decoder.

use arbitrary::Arbitrary;
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
