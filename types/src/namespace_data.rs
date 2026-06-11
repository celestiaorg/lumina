//! Types related to namespace data
//!
//! Namespace data in Celestia is understood as all the [`Share`]s in a particular
//! namespace inside the [`ExtendedDataSquare`]. It may span over multiple rows.
//!
//! [`Share`]: crate::Share
//! [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare

use bytes::{BufMut, BytesMut};
use celestia_proto::shwap::RowNamespaceData as RawRowNamespaceData;
use serde::{Deserialize, Serialize};

use crate::eds::{EDS_ID_SIZE, EdsId};
use crate::nmt::{NS_SIZE, Namespace};
use crate::row_namespace_data::{RowNamespaceData, RowNamespaceDataId};
use crate::{DataAvailabilityHeader, Error, Result, bail_verification};

/// Number of bytes needed to represent [`RowNamespaceDataId`] in `multihash`.
pub const NAMESPACE_DATA_ID_SIZE: usize = EDS_ID_SIZE + NS_SIZE;

/// Identifies [`Share`]s within a [`Namespace`] located on block's [`ExtendedDataSquare`].
///
/// [`Share`]: crate::Share
/// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct NamespaceDataId {
    eds_id: EdsId,
    namespace: Namespace,
}

/// A collection of rows of [`Share`]s from a particular [`Namespace`].
///
/// [`Share`]: crate::Share
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NamespaceData {
    rows: Vec<RowNamespaceData>,
}

impl NamespaceData {
    /// Create namespace data from one or many rows.
    pub fn new(rows: Vec<RowNamespaceData>) -> Self {
        NamespaceData { rows }
    }

    /// All rows containing shares within some namespace.
    pub fn rows(&self) -> &[RowNamespaceData] {
        &self.rows[..]
    }

    /// Extract inner rows namespace data
    pub fn into_inner(self) -> Vec<RowNamespaceData> {
        self.rows
    }

    /// Verify the namespace data against roots from DAH
    pub fn verify(&self, id: NamespaceDataId, dah: &DataAvailabilityHeader) -> Result<()> {
        if self.rows.len() > u16::MAX as usize {
            return Err(Error::NamespaceDataTooLarge);
        }

        let row_idxs = (0..dah.square_width())
            .filter(|&row| dah.row_contains(row, id.namespace).unwrap_or(false))
            .collect::<Vec<_>>();

        if row_idxs.len() != self.rows.len() {
            bail_verification!(
                "expected {} rows, found {} rows",
                row_idxs.len(),
                self.rows.len()
            );
        }

        for (row, row_index) in self.rows.iter().zip(row_idxs) {
            let ns_row_id = RowNamespaceDataId::new(id.namespace, row_index, id.block_height())?;
            row.verify(ns_row_id, dah)?;
        }

        Ok(())
    }

    /// Recover namespace data from it's raw representation.
    pub fn from_raw(id: NamespaceDataId, namespace_data: Vec<RawRowNamespaceData>) -> Result<Self> {
        if namespace_data.len() > u16::MAX as usize {
            return Err(Error::NamespaceDataTooLarge);
        }

        let mut rows = Vec::with_capacity(namespace_data.len());

        for (row_index, raw_ns_data) in namespace_data.into_iter().enumerate() {
            let ns_row_id =
                RowNamespaceDataId::new(id.namespace, row_index as u16, id.block_height())?;
            let ns_data = RowNamespaceData::from_raw(ns_row_id, raw_ns_data)?;
            rows.push(ns_data);
        }

        Ok(NamespaceData::new(rows))
    }
}

impl NamespaceDataId {
    /// Create a new [`NamespaceDataId`] for given block and the [`Namespace`].
    ///
    /// # Errors
    ///
    /// This function will return an error if the block height is invalid.
    pub fn new(namespace: Namespace, block_height: u64) -> Result<Self> {
        Ok(Self {
            eds_id: EdsId::new(block_height)?,
            namespace,
        })
    }

    /// A height of the block which contains the shares.
    pub fn block_height(&self) -> u64 {
        self.eds_id.block_height()
    }

    /// A namespace of the [`Share`]s.
    ///
    /// [`Share`]: crate::Share
    pub fn namespace(&self) -> Namespace {
        self.namespace
    }

    /// Encode row namespace data id into the byte representation.
    pub fn encode(&self, bytes: &mut BytesMut) {
        bytes.reserve(NAMESPACE_DATA_ID_SIZE);
        self.eds_id.encode(bytes);
        bytes.put(self.namespace.as_bytes());
    }

    /// Decode namespace data id from the byte representation.
    pub fn decode(buffer: &[u8]) -> Result<Self> {
        if buffer.len() != NAMESPACE_DATA_ID_SIZE {
            return Err(Error::InvalidLength(buffer.len(), NAMESPACE_DATA_ID_SIZE));
        }

        let (eds_bytes, ns_bytes) = buffer.split_at(EDS_ID_SIZE);
        let eds_id = EdsId::decode(eds_bytes)?;
        let namespace = Namespace::from_raw(ns_bytes)?;

        Ok(Self { eds_id, namespace })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_namespaced_shares() {
        let get_shares_by_namespace_response = r#"[
          {
            "shares": [
              "AAAAAAAAAAAAAAAAAAAAAAAAAAAADCBNOWAP3dMBAAAAG/HyDKgAfpEKO/iy5h2g8mvKB+94cXpupUFl9QAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
            ],
            "proof": {
              "start": 1,
              "end": 2,
              "nodes": [
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABFmTiyJVvgoyHdw7JGii/wyMfMbSdN3Nbi6Uj0Lcprk+",
                "/////////////////////////////////////////////////////////////////////////////0WE8jz9lbFjpXWj9v7/QgdAxYEqy4ew9TMdqil/UFZm"
              ],
              "leaf_hash": null,
              "is_max_namespace_ignored": true
            }
          }
        ]"#;

        let ns_shares: NamespaceData =
            serde_json::from_str(get_shares_by_namespace_response).unwrap();

        assert_eq!(ns_shares.rows()[0].shares.len(), 1);
        assert!(!ns_shares.rows()[0].proof.is_of_absence());
    }
}
