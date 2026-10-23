//! Shared fixtures for the `celestia-types` fuzz targets.

use std::iter;
use std::ops::Range;
use std::sync::LazyLock;

use celestia_proto::celestia::core::v1::proof::{
    NmtProof as RawNmtProof, ShareProof as RawShareProof,
};
use celestia_proto::shwap::{
    Row as RawRow, RowNamespaceData as RawRowNamespaceData, Sample as RawSample,
};
use celestia_types::consts::appconsts::SHARE_SIZE;
use celestia_types::namespace_data::{NamespaceData, NamespaceDataId};
use celestia_types::nmt::{NS_SIZE, Namespace, NamespacedHashExt};
use celestia_types::row::{Row, RowId};
use celestia_types::sample::{Sample, SampleId};
use celestia_types::{DataAvailabilityHeader, ExtendedDataSquare, Share, ShareProof};

pub mod header;
pub mod mutate;

/// Block height used in ids of seeds and generated inputs.
pub const HEIGHT: u64 = 1;

/// Widths of the original data squares in [`SQUARES`].
pub const ODS_WIDTHS: [usize; 5] = [1, 2, 4, 8, 16];

/// A square and the header that responses are verified against.
pub struct Square {
    pub eds: ExtendedDataSquare,
    pub dah: DataAvailabilityHeader,
}

/// Deterministic squares, one for each of [`ODS_WIDTHS`].
pub static SQUARES: LazyLock<Vec<Square>> = LazyLock::new(|| {
    ODS_WIDTHS
        .iter()
        .map(|&width| {
            let eds = ExtendedDataSquare::from_ods(ods(width)).unwrap();
            let dah = DataAvailabilityHeader::from_eds(&eds);
            Square { eds, dah }
        })
        .collect()
});

/// Picks one of [`SQUARES`].
pub fn square(selector: u8) -> &'static Square {
    &SQUARES[usize::from(selector) % SQUARES.len()]
}

/// Namespace with the given id.
///
/// Shares in [`SQUARES`] use odd ids, so even ids fall into gaps between them and
/// exercise absence proofs.
pub fn namespace(id: u16) -> Namespace {
    let [hi, lo] = id.to_be_bytes();
    Namespace::const_v0([1, 0, 0, 0, 0, 0, 0, 0, hi, lo])
}

/// Original data shares of a square of the given width.
///
/// Namespaces come in runs of different lengths, so that some span multiple rows and
/// some share a row with others. The last quarter of the square is tail padding.
fn ods(width: usize) -> Vec<Vec<u8>> {
    let run_lengths = [width + 1, 1, 2, 1, 3];
    let total = width * width;
    let data = total - total / 4;

    let mut namespaces = Vec::with_capacity(total);
    for (run, len) in run_lengths.iter().cycle().enumerate() {
        if namespaces.len() >= data {
            break;
        }
        let len = (*len).min(data - namespaces.len());
        namespaces.extend(iter::repeat_n(namespace(2 * run as u16 + 1), len));
    }
    namespaces.resize(total, Namespace::TAIL_PADDING);

    namespaces
        .iter()
        .enumerate()
        .map(|(i, ns)| {
            let mut share = vec![0; SHARE_SIZE];
            share[..NS_SIZE].copy_from_slice(ns.as_bytes());
            // keep the info byte zero and make every share unique
            share[NS_SIZE + 1..NS_SIZE + 5].copy_from_slice(&(i as u32).to_be_bytes());
            share
        })
        .collect()
}

impl Square {
    /// Decodes and verifies a row the way the node does.
    ///
    /// Panics if verification accepts anything but the row of this square.
    pub fn check_row(&self, id: RowId, raw: RawRow) {
        let Ok(row) = Row::from_raw(id, raw) else {
            return;
        };
        if row.verify(id, &self.dah).is_ok() {
            assert_eq!(row.shares, self.eds.row(id.index()).unwrap());
        }
    }

    /// Decodes and verifies a sample the way the node does.
    ///
    /// Panics if verification accepts anything but the share of this square.
    pub fn check_sample(&self, id: SampleId, raw: RawSample) {
        let Ok(sample) = Sample::from_raw(id, raw) else {
            return;
        };
        if sample.verify(id, &self.dah).is_ok() {
            let share = self.eds.share(id.row_index(), id.column_index()).unwrap();
            assert_eq!(&sample.share, share);
        }
    }

    /// Decodes and verifies namespace data the way the node does.
    ///
    /// Panics if verification accepts anything but the complete namespace data of
    /// this square.
    pub fn check_namespace_data(&self, id: NamespaceDataId, raw: Vec<RawRowNamespaceData>) {
        let Ok(data) = NamespaceData::from_raw(id, raw) else {
            return;
        };
        if data.verify(id, &self.dah).is_ok() {
            let expected = self
                .eds
                .get_namespace_data(id.namespace(), &self.dah, HEIGHT)
                .unwrap();
            let shares = data.rows().iter().map(|row| &row.shares);
            assert!(shares.eq(expected.iter().map(|(_, row)| &row.shares)));
        }
    }

    fn ods_width(&self) -> usize {
        usize::from(self.eds.square_width() / 2)
    }

    /// Original data share at `index`, in row-major order.
    pub fn ods_share(&self, index: usize) -> &Share {
        let width = self.ods_width();
        self.eds
            .share((index / width) as u16, (index % width) as u16)
            .unwrap()
    }

    /// Range of original data shares, in row-major order, that are in `namespace`.
    pub fn namespace_range(&self, namespace: Namespace) -> Range<usize> {
        let total = self.ods_width().pow(2);
        let in_namespace = |&i: &usize| self.ods_share(i).namespace() == namespace;
        let start = (0..total).find(in_namespace).unwrap_or(total);
        let end = (start..total).find(|i| !in_namespace(i)).unwrap_or(total);
        start..end
    }

    /// Share proof of the original data shares in `range`, all in `namespace`.
    pub fn share_proof(&self, namespace: Namespace, range: Range<usize>) -> RawShareProof {
        let width = self.ods_width();
        let rows = range.start / width..=(range.end - 1) / width;

        RawShareProof {
            data: range.clone().map(|i| self.ods_share(i).to_vec()).collect(),
            share_proofs: rows
                .clone()
                .map(|row| {
                    let start = range.start.max(row * width) - row * width;
                    let end = range.end.min((row + 1) * width) - row * width;
                    let proof = self
                        .eds
                        .row_nmt(row as u16)
                        .unwrap()
                        .build_range_proof(start..end);
                    RawNmtProof {
                        start: start as i32,
                        end: end as i32,
                        nodes: proof.siblings().iter().map(|node| node.to_vec()).collect(),
                        leaf_hash: Vec::new(),
                    }
                })
                .collect(),
            namespace_id: namespace.id().to_vec(),
            namespace_version: namespace.version().into(),
            row_proof: Some(
                self.dah
                    .row_proof(*rows.start() as u16..=*rows.end() as u16)
                    .unwrap()
                    .into(),
            ),
        }
    }

    /// Decodes and verifies a share proof against the data root, the way a client of
    /// a bridge node does.
    ///
    /// Panics if verification accepts anything but shares of this square in the
    /// proven namespace and range.
    pub fn check_share_proof(&self, raw: RawShareProof) {
        let Ok(proof) = ShareProof::try_from(raw) else {
            return;
        };
        let Ok(range) = proof.covered_range(self.dah.hash()) else {
            return;
        };
        let range = range.start as usize..range.end as usize;
        assert!(range.end <= self.ods_width().pow(2));
        for (share, i) in proof.data.iter().zip(range.clone()) {
            assert_eq!(share, self.ods_share(i).data());
            assert_eq!(self.ods_share(i).namespace(), proof.namespace_id);
        }
        assert_eq!(proof.data.len(), range.len());
    }
}
