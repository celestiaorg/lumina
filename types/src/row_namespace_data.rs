//! Types related to the namespaced data.
//!
//! Namespaced data in Celestia is understood as all the [`Share`]s within
//! the same [`Namespace`] in a single row of the [`ExtendedDataSquare`].
//!
//! [`Share`]: crate::Share
//! [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare

use blockstore::block::CidError;
use bytes::{BufMut, BytesMut};
use celestia_proto::shwap::{RowNamespaceData as RawRowNamespaceData, Share as RawShare};
use cid::CidGeneric;
use multihash::Multihash;
use prost::Message;
use serde::{Deserialize, Serialize};

use crate::nmt::{NS_SIZE, Namespace, NamespaceProof};
use crate::row::{ROW_ID_SIZE, RowId};
use crate::{DataAvailabilityHeader, Error, Result, Share, bail_validation};

/// Number of bytes needed to represent [`RowNamespaceDataId`] in `multihash`.
pub const ROW_NAMESPACE_DATA_ID_SIZE: usize = ROW_ID_SIZE + NS_SIZE;
/// The code of the [`RowNamespaceDataId`] hashing algorithm in `multihash`.
pub const ROW_NAMESPACE_DATA_ID_MULTIHASH_CODE: u64 = 0x7821;
/// The id of codec used for the [`RowNamespaceDataId`] in `Cid`s.
pub const ROW_NAMESPACE_DATA_CODEC: u64 = 0x7820;

/// Identifies [`Share`]s within a [`Namespace`] located on a particular row of the
/// block's [`ExtendedDataSquare`].
///
/// [`Share`]: crate::Share
/// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct RowNamespaceDataId {
    row_id: RowId,
    namespace: Namespace,
}

/// `RowNamespaceData` contains up to a row of shares belonging to a particular namespace and a proof of their inclusion.
///
/// It is constructed out of the ExtendedDataSquare. If, for particular EDS, shares from the namespace span multiple rows,
/// one needs multiple RowNamespaceData instances to cover the whole range.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(into = "RawRowNamespaceData", try_from = "RawRowNamespaceData")]
pub struct RowNamespaceData {
    /// Proof of data inclusion
    pub proof: NamespaceProof,
    /// Shares with data
    #[serde(deserialize_with = "celestia_proto::serializers::null_default::deserialize")]
    pub shares: Vec<Share>,
}

impl RowNamespaceData {
    /// Verifies the proof inside `RowNamespaceData` using a row root from [`DataAvailabilityHeader`]
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celestia_types::nmt::Namespace;
    /// # use celestia_types::{ExtendedDataSquare, ExtendedHeader};
    /// # fn get_extended_data_square(height: usize) -> ExtendedDataSquare {
    /// #    unimplemented!()
    /// # }
    /// # fn get_extended_header(height: usize) -> ExtendedHeader {
    /// #    unimplemented!()
    /// # }
    /// #
    /// let block_height = 100;
    /// let eds = get_extended_data_square(block_height);
    /// let header = get_extended_header(block_height);
    ///
    /// let namespace = Namespace::new_v0(&[1, 2, 3]).unwrap();
    ///
    /// let rows = eds.get_namespace_data(namespace, &header.dah, block_height as u64).unwrap();
    /// for (id, namespace_data) in rows {
    ///     namespace_data.verify(id, &header.dah).unwrap()
    /// }
    /// ```
    ///
    /// [`DataAvailabilityHeader`]: crate::DataAvailabilityHeader
    pub fn verify(&self, id: RowNamespaceDataId, dah: &DataAvailabilityHeader) -> Result<()> {
        if (self.shares.is_empty() && self.proof.is_of_presence())
            || (!self.shares.is_empty() && self.proof.is_of_absence())
        {
            return Err(Error::WrongProofType);
        }

        let namespace = id.namespace();
        let row = id.row_index();
        let root = dah.row_root(row).ok_or(Error::EdsIndexOutOfRange(row, 0))?;

        self.proof
            .verify_complete_namespace(&root, &self.shares, *namespace)
            .map_err(Error::RangeProofError)
    }

    /// Encode RowNamespaceData into the raw binary representation.
    pub fn encode(&self, bytes: &mut BytesMut) {
        let raw = RawRowNamespaceData::from(self.clone());

        bytes.reserve(raw.encoded_len());
        raw.encode(bytes).expect("capacity reserved");
    }

    /// Decode RowNamespaceData from the binary representation.
    ///
    /// # Errors
    ///
    /// This function will return an error if protobuf deserialization
    /// fails and propagate errors from [`RowNamespaceData::from_raw`].
    pub fn decode(id: RowNamespaceDataId, buffer: &[u8]) -> Result<Self> {
        let raw = RawRowNamespaceData::decode(buffer)?;
        Self::from_raw(id, raw)
    }

    /// Recover RowNamespaceData from it's raw representation.
    ///
    /// # Errors
    ///
    /// This function will return error if proof is missing or invalid, shares are not in
    /// the expected namespace, and will propagate errors from [`Share`] construction.
    pub fn from_raw(id: RowNamespaceDataId, namespace_data: RawRowNamespaceData) -> Result<Self> {
        let Some(proof) = namespace_data.proof else {
            return Err(Error::MissingProof);
        };

        // extract all shares according to the expected namespace
        let shares: Vec<_> = namespace_data
            .shares
            .into_iter()
            .map(|shr| {
                if id.namespace != Namespace::PARITY_SHARE {
                    Share::from_raw(&shr.data)
                } else {
                    Share::parity(&shr.data)
                }
            })
            .collect::<Result<_>>()?;

        // and double check they all have equal namespaces
        if !shares.iter().all(|shr| shr.namespace() == id.namespace) {
            bail_validation!("Namespace data must have equal namespaces");
        }

        Ok(RowNamespaceData {
            shares,
            proof: proof.try_into()?,
        })
    }
}

impl From<RowNamespaceData> for RawRowNamespaceData {
    fn from(namespaced_data: RowNamespaceData) -> RawRowNamespaceData {
        RawRowNamespaceData {
            shares: namespaced_data
                .shares
                .into_iter()
                .map(|shr| RawShare { data: shr.to_vec() })
                .collect(),
            proof: Some(namespaced_data.proof.into()),
        }
    }
}

impl TryFrom<RawRowNamespaceData> for RowNamespaceData {
    type Error = Error;

    fn try_from(value: RawRowNamespaceData) -> std::result::Result<Self, Self::Error> {
        let Some(proof) = value.proof else {
            return Err(Error::MissingProof);
        };
        let proof = proof.try_into()?;

        let mut shares = Vec::with_capacity(value.shares.len());
        for raw_share in value.shares {
            shares.push(Share::try_from(raw_share)?);
        }
        Ok(RowNamespaceData { proof, shares })
    }
}

impl RowNamespaceDataId {
    /// Create a new [`RowNamespaceDataId`] for given block, row and the [`Namespace`].
    ///
    /// # Errors
    ///
    /// This function will return an error if the block height
    /// or row index is invalid.
    pub fn new(namespace: Namespace, row_index: u16, block_height: u64) -> Result<Self> {
        Ok(Self {
            row_id: RowId::new(row_index, block_height)?,
            namespace,
        })
    }

    /// A height of the block which contains the shares.
    pub fn block_height(&self) -> u64 {
        self.row_id.block_height()
    }

    /// Row index of the [`ExtendedDataSquare`] that shares are located on.
    ///
    /// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
    pub fn row_index(&self) -> u16 {
        self.row_id.index()
    }

    /// A namespace of the [`Share`]s.
    ///
    /// [`Share`]: crate::Share
    pub fn namespace(&self) -> Namespace {
        self.namespace
    }

    /// Encode row namespace data id into the byte representation.
    pub fn encode(&self, bytes: &mut BytesMut) {
        bytes.reserve(ROW_NAMESPACE_DATA_ID_SIZE);
        self.row_id.encode(bytes);
        bytes.put(self.namespace.as_bytes());
    }

    /// Decode row namespace data id from the byte representation.
    pub fn decode(buffer: &[u8]) -> Result<Self> {
        if buffer.len() != ROW_NAMESPACE_DATA_ID_SIZE {
            return Err(Error::InvalidLength(
                buffer.len(),
                ROW_NAMESPACE_DATA_ID_SIZE,
            ));
        }

        let (row_bytes, ns_bytes) = buffer.split_at(ROW_ID_SIZE);
        let row_id = RowId::decode(row_bytes)?;
        let namespace = Namespace::from_raw(ns_bytes)?;

        Ok(Self { row_id, namespace })
    }
}

impl<const S: usize> TryFrom<CidGeneric<S>> for RowNamespaceDataId {
    type Error = CidError;

    fn try_from(cid: CidGeneric<S>) -> Result<Self, Self::Error> {
        let codec = cid.codec();
        if codec != ROW_NAMESPACE_DATA_CODEC {
            return Err(CidError::InvalidCidCodec(codec));
        }

        let hash = cid.hash();

        let size = hash.size() as usize;
        if size != ROW_NAMESPACE_DATA_ID_SIZE {
            return Err(CidError::InvalidMultihashLength(size));
        }

        let code = hash.code();
        if code != ROW_NAMESPACE_DATA_ID_MULTIHASH_CODE {
            return Err(CidError::InvalidMultihashCode(
                code,
                ROW_NAMESPACE_DATA_ID_MULTIHASH_CODE,
            ));
        }

        RowNamespaceDataId::decode(hash.digest()).map_err(|e| CidError::InvalidCid(e.to_string()))
    }
}

impl From<RowNamespaceDataId> for CidGeneric<ROW_NAMESPACE_DATA_ID_SIZE> {
    fn from(namespaced_data_id: RowNamespaceDataId) -> Self {
        let mut bytes = BytesMut::with_capacity(ROW_NAMESPACE_DATA_ID_SIZE);
        namespaced_data_id.encode(&mut bytes);
        // length is correct, so the unwrap is safe
        let mh = Multihash::wrap(ROW_NAMESPACE_DATA_ID_MULTIHASH_CODE, &bytes[..]).unwrap();

        CidGeneric::new_v1(ROW_NAMESPACE_DATA_CODEC, mh)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Blob;
    use crate::test_utils::{generate_dummy_eds, generate_eds};

    #[test]
    fn round_trip() {
        let ns = Namespace::new_v0(&[0, 1]).unwrap();
        let data_id = RowNamespaceDataId::new(ns, 5, 100).unwrap();
        let cid = CidGeneric::from(data_id);

        let multihash = cid.hash();
        assert_eq!(multihash.code(), ROW_NAMESPACE_DATA_ID_MULTIHASH_CODE);
        assert_eq!(multihash.size(), ROW_NAMESPACE_DATA_ID_SIZE as u8);

        let deserialized_data_id = RowNamespaceDataId::try_from(cid).unwrap();
        assert_eq!(data_id, deserialized_data_id);
    }

    #[test]
    fn from_buffer() {
        let bytes = [
            0x01, // CIDv1
            0xA0, 0xF0, 0x01, // CID codec = 7820
            0xA1, 0xF0, 0x01, // multihash code = 7821
            0x27, // len = ROW_NAMESPACE_DATA_ID_SIZE = 39
            0, 0, 0, 0, 0, 0, 0, 64, // block height = 64
            0, 7, // row = 7
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, // NS = 1
        ];

        let cid = CidGeneric::<ROW_NAMESPACE_DATA_ID_SIZE>::read_bytes(bytes.as_ref()).unwrap();
        assert_eq!(cid.codec(), ROW_NAMESPACE_DATA_CODEC);
        let mh = cid.hash();
        assert_eq!(mh.code(), ROW_NAMESPACE_DATA_ID_MULTIHASH_CODE);
        assert_eq!(mh.size(), ROW_NAMESPACE_DATA_ID_SIZE as u8);
        let data_id = RowNamespaceDataId::try_from(cid).unwrap();
        assert_eq!(data_id.namespace(), Namespace::new_v0(&[1]).unwrap());
        assert_eq!(data_id.block_height(), 64);
        assert_eq!(data_id.row_index(), 7);
    }

    #[test]
    fn namespaced_data_id_size() {
        // Size MUST be 39 by the spec.
        assert_eq!(ROW_NAMESPACE_DATA_ID_SIZE, 39);

        let data_id = RowNamespaceDataId::new(Namespace::new_v0(&[1]).unwrap(), 0, 1).unwrap();
        let mut bytes = BytesMut::new();
        data_id.encode(&mut bytes);
        assert_eq!(bytes.len(), ROW_NAMESPACE_DATA_ID_SIZE);
    }

    #[test]
    fn multihash_invalid_code() {
        let multihash =
            Multihash::<ROW_NAMESPACE_DATA_ID_SIZE>::wrap(888, &[0; ROW_NAMESPACE_DATA_ID_SIZE])
                .unwrap();
        let cid =
            CidGeneric::<ROW_NAMESPACE_DATA_ID_SIZE>::new_v1(ROW_NAMESPACE_DATA_CODEC, multihash);
        let axis_err = RowNamespaceDataId::try_from(cid).unwrap_err();
        assert_eq!(
            axis_err,
            CidError::InvalidMultihashCode(888, ROW_NAMESPACE_DATA_ID_MULTIHASH_CODE)
        );
    }

    #[test]
    fn cid_invalid_codec() {
        let multihash = Multihash::<ROW_NAMESPACE_DATA_ID_SIZE>::wrap(
            ROW_NAMESPACE_DATA_ID_MULTIHASH_CODE,
            &[0; ROW_NAMESPACE_DATA_ID_SIZE],
        )
        .unwrap();
        let cid = CidGeneric::<ROW_NAMESPACE_DATA_ID_SIZE>::new_v1(4321, multihash);
        let axis_err = RowNamespaceDataId::try_from(cid).unwrap_err();
        assert_eq!(axis_err, CidError::InvalidCidCodec(4321));
    }

    #[test]
    fn test_roundtrip_verify() {
        // random
        for _ in 0..5 {
            let eds = generate_dummy_eds(2 << (rand::random::<usize>() % 8));
            let dah = DataAvailabilityHeader::from_eds(&eds);

            let namespace = eds.share(1, 1).unwrap().namespace();

            for (id, row) in eds.get_namespace_data(namespace, &dah, 1).unwrap() {
                let mut buf = BytesMut::new();
                row.encode(&mut buf);
                let decoded = RowNamespaceData::decode(id, &buf).unwrap();

                decoded.verify(id, &dah).unwrap();
            }
        }

        // parity share
        let eds = generate_dummy_eds(2 << (rand::random::<usize>() % 8));
        let dah = DataAvailabilityHeader::from_eds(&eds);
        for (id, row) in eds
            .get_namespace_data(Namespace::PARITY_SHARE, &dah, 1)
            .unwrap()
        {
            let mut buf = BytesMut::new();
            row.encode(&mut buf);
            let decoded = RowNamespaceData::decode(id, &buf).unwrap();

            decoded.verify(id, &dah).unwrap();
        }
    }

    #[test]
    fn verify_absent_ns() {
        // parity share
        let eds = generate_dummy_eds(2 << (rand::random::<usize>() % 8));
        let dah = DataAvailabilityHeader::from_eds(&eds);

        // namespace bigger than pay for blob, smaller than primary reserved padding, that is not
        // used
        let ns = Namespace::const_v0([0, 0, 0, 0, 0, 0, 0, 0, 0, 5]);
        for (id, row) in eds.get_namespace_data(ns, &dah, 1).unwrap() {
            assert!(row.shares.is_empty());
            row.verify(id, &dah).unwrap();
        }
    }

    #[test]
    fn reconstruct_all() {
        for _ in 0..3 {
            let eds = generate_eds(8 << (rand::random::<usize>() % 6));
            let dah = DataAvailabilityHeader::from_eds(&eds);

            let mut namespaces: Vec<_> = eds
                .data_square()
                .iter()
                .map(|shr| shr.namespace())
                .filter(|ns| !ns.is_reserved())
                .collect();
            namespaces.dedup();

            // first namespace should have 2 blobs over 3 rows
            let namespace_data = eds.get_namespace_data(namespaces[0], &dah, 1).unwrap();
            assert_eq!(namespace_data.len(), 3);
            let shares = namespace_data.iter().flat_map(|(_, row)| row.shares.iter());

            let blobs = Blob::reconstruct_all(shares).unwrap();
            assert_eq!(blobs.len(), 2);

            // rest of namespaces should have 1 blob each
            for ns in &namespaces[1..] {
                let namespace_data = eds.get_namespace_data(*ns, &dah, 1).unwrap();
                assert_eq!(namespace_data.len(), 1);
                let shares = namespace_data.iter().flat_map(|(_, row)| row.shares.iter());

                let blobs = Blob::reconstruct_all(shares).unwrap();
                assert_eq!(blobs.len(), 1);
            }
        }
    }

    #[test]
    fn namespace_data_roundtrip() {
        let proof = nmt_rs::nmt_proof::NamespaceProof::<
            crate::nmt::NamespacedSha2Hasher,
            { crate::nmt::NS_SIZE },
        >::AbsenceProof {
            proof: crate::nmt::Proof {
                siblings: vec![
                    nmt_rs::NamespacedHash::new(
                        nmt_rs::NamespaceId([
                            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                            0, 0, 0, 0, 4,
                        ]),
                        nmt_rs::NamespaceId([
                            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                            0, 0, 0, 0, 4,
                        ]),
                        [
                            180, 43, 29, 197, 134, 127, 103, 202, 217, 240, 11, 18, 15, 47, 140,
                            136, 58, 134, 117, 174, 162, 95, 216, 114, 31, 71, 90, 238, 49, 228,
                            95, 89,
                        ],
                    ),
                    nmt_rs::NamespacedHash::new(
                        nmt_rs::NamespaceId::MAX_ID,
                        nmt_rs::NamespaceId::MAX_ID,
                        [
                            126, 112, 141, 49, 103, 177, 23, 186, 153, 245, 110, 62, 165, 4, 39,
                            125, 171, 55, 116, 176, 36, 153, 101, 171, 25, 253, 200, 61, 226, 43,
                            81, 52,
                        ],
                    ),
                ],
                range: 1..2,
            },
            ignore_max_ns: true,
            leaf: Some(nmt_rs::NamespacedHash::new(
                nmt_rs::NamespaceId([
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 115, 111, 118, 45,
                    116, 101, 115, 116, 45, 112,
                ]),
                nmt_rs::NamespaceId([
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 115, 111, 118, 45,
                    116, 101, 115, 116, 45, 112,
                ]),
                [
                    132, 118, 183, 139, 217, 27, 43, 49, 209, 15, 142, 136, 209, 205, 230, 67, 247,
                    102, 202, 206, 118, 16, 124, 41, 208, 225, 148, 103, 192, 184, 59, 155,
                ],
            )),
        };

        let row = RowNamespaceData {
            proof: proof.into(),
            shares: vec![],
        };

        // JSON works,
        let row_j = serde_json::to_value(&row).unwrap();
        let d_from_json: RowNamespaceData = serde_json::from_value(row_j).unwrap();
        assert_eq!(d_from_json, row);

        // binary
        let s_row = postcard::to_allocvec(&row).unwrap();
        let d_row: RowNamespaceData = postcard::from_bytes(&s_row).unwrap();
        assert_eq!(row, d_row);
    }
}
