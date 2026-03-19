//! Types related to samples.
//!
//! Sample in Celestia is understood as a single [`Share`] located at an
//! index in the particular [`row`] of the [`ExtendedDataSquare`].
//!
//! [`row`]: crate::row
//! [`Share`]: crate::Share
//! [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare

use blockstore::block::CidError;
use bytes::{Buf, BufMut, BytesMut};
use celestia_proto::shwap::Share as RawShare;
use cid::CidGeneric;
use multihash::Multihash;
use nmt_rs::nmt_proof::NamespaceProof as NmtNamespaceProof;
use prost::Message;
use serde::Serialize;

use crate::eds::{AxisType, ExtendedDataSquare};
use crate::nmt::NamespaceProof;
use crate::row::{ROW_ID_SIZE, RowId};
use crate::{DataAvailabilityHeader, Error, Result, Share, bail_validation};

pub use celestia_proto::shwap::Sample as RawSample;

/// Number of bytes needed to represent [`SampleId`] in `multihash`.
const SAMPLE_ID_SIZE: usize = 12;
/// The code of the [`SampleId`] hashing algorithm in `multihash`.
pub const SAMPLE_ID_MULTIHASH_CODE: u64 = 0x7811;
/// The id of codec used for the [`SampleId`] in `Cid`s.
pub const SAMPLE_ID_CODEC: u64 = 0x7810;

/// Identifies a particular [`Share`] located in the [`ExtendedDataSquare`].
///
/// [`Share`]: crate::Share
/// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct SampleId {
    row_id: RowId,
    column_index: u16,
}

/// Represents Sample, with proof of its inclusion
#[derive(Clone, Debug, Serialize)]
#[serde(into = "RawSample")]
pub struct Sample {
    /// Indication whether proving was done row or column-wise
    pub proof_type: AxisType,
    /// Share that is being sampled
    pub share: Share,
    /// Proof of the inclusion of the share
    pub proof: NamespaceProof,
}

impl Sample {
    /// Create a new [`Sample`] for the given index of the [`ExtendedDataSquare`] in a block.
    ///
    /// `row_index` and `column_index` specifies the [`Share`] position in EDS.
    /// `proof_type` determines whether proof of inclusion of the [`Share`] should be
    /// constructed for its row or column.
    ///
    /// # Errors
    ///
    /// This function will return an error, if:
    ///
    /// - `row_index`/`column_index` falls outside the provided [`ExtendedDataSquare`].
    /// - [`ExtendedDataSquare`] is incorrect (either data shares don't have their namespace
    ///   prefixed, or [`Share`]s aren't namespace ordered)
    /// - Block height is zero
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celestia_types::AxisType;
    /// use celestia_types::sample::{Sample, SampleId};
    /// # use celestia_types::{ExtendedDataSquare, ExtendedHeader};
    /// #
    /// # fn get_extended_data_square(height: u64) -> ExtendedDataSquare {
    /// #    unimplemented!()
    /// # }
    /// #
    /// # fn get_extended_header(height: u64) -> ExtendedHeader {
    /// #    unimplemented!()
    /// # }
    ///
    /// let block_height = 15;
    /// let eds = get_extended_data_square(block_height);
    /// let header = get_extended_header(block_height);
    ///
    /// let sample_id = SampleId::new(2, 3, block_height).unwrap();
    /// let sample = Sample::new(2, 3, AxisType::Row, &eds).unwrap();
    ///
    /// sample.verify(sample_id, &header.dah).unwrap();
    /// ```
    ///
    /// [`Share`]: crate::Share
    /// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
    pub fn new(
        row_index: u16,
        column_index: u16,
        proof_type: AxisType,
        eds: &ExtendedDataSquare,
    ) -> Result<Self> {
        let share = eds.share(row_index, column_index)?.clone();

        let range_proof = match proof_type {
            AxisType::Row => eds
                .row_nmt(row_index)?
                .build_range_proof(usize::from(column_index)..usize::from(column_index) + 1),
            AxisType::Col => eds
                .column_nmt(column_index)?
                .build_range_proof(usize::from(row_index)..usize::from(row_index) + 1),
        };

        let proof = NmtNamespaceProof::PresenceProof {
            proof: range_proof,
            ignore_max_ns: true,
        };

        Ok(Sample {
            share,
            proof: proof.into(),
            proof_type,
        })
    }

    /// verify sample with root hash from ExtendedHeader
    pub fn verify(&self, id: SampleId, dah: &DataAvailabilityHeader) -> Result<()> {
        let root = match self.proof_type {
            AxisType::Row => dah
                .row_root(id.row_index())
                .ok_or(Error::EdsIndexOutOfRange(id.row_index(), 0))?,
            AxisType::Col => dah
                .column_root(id.column_index())
                .ok_or(Error::EdsIndexOutOfRange(0, id.column_index()))?,
        };

        self.proof
            .verify_range(&root, &[&self.share], *self.share.namespace())
            .map_err(Error::RangeProofError)
    }

    /// Encode Sample into the raw binary representation.
    pub fn encode(&self, bytes: &mut BytesMut) {
        let raw = RawSample::from(self.clone());

        bytes.reserve(raw.encoded_len());
        raw.encode(bytes).expect("capacity reserved");
    }

    /// Decode Sample from the binary representation.
    ///
    /// # Errors
    ///
    /// This function will return an error if protobuf deserialization
    /// fails and propagate errors from [`Sample::from_raw`].
    pub fn decode(id: SampleId, buffer: &[u8]) -> Result<Self> {
        let raw = RawSample::decode(buffer)?;
        Self::from_raw(id, raw)
    }

    /// Recover Sample from it's raw representation.
    ///
    /// # Errors
    ///
    /// This function will return error if proof is missing or invalid shares are not in
    /// the expected namespace, and will propagate errors from [`Share`] construction.
    pub fn from_raw(id: SampleId, sample: RawSample) -> Result<Self> {
        let Some(proof) = sample.proof else {
            return Err(Error::MissingProof);
        };

        let proof: NamespaceProof = proof.try_into()?;
        let proof_type = AxisType::try_from(sample.proof_type)?;

        if proof.is_of_absence() {
            return Err(Error::WrongProofType);
        }

        let Some(share) = sample.share else {
            bail_validation!("missing share");
        };
        let Some(square_size) = proof.total_leaves() else {
            bail_validation!("proof must be for single leaf");
        };

        let row_index = id.row_index() as usize;
        let col_index = id.column_index() as usize;
        let share = if row_index < square_size / 2 && col_index < square_size / 2 {
            Share::from_raw(&share.data)?
        } else {
            Share::parity(&share.data)?
        };

        Ok(Sample {
            proof_type,
            share,
            proof,
        })
    }
}

impl From<Sample> for RawSample {
    fn from(sample: Sample) -> RawSample {
        RawSample {
            share: Some(RawShare {
                data: sample.share.to_vec(),
            }),
            proof: Some(sample.proof.into()),
            proof_type: sample.proof_type as i32,
        }
    }
}

impl SampleId {
    /// Create a new [`SampleId`] for the given `row_index` and `column_index` of the
    /// [`ExtendedDataSquare`] in a block.
    ///
    /// # Errors
    ///
    /// This function will return an error if the block height is zero.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celestia_types::sample::SampleId;
    ///
    /// // Consider a 64th share of EDS with block height of 15
    /// let header_height = 15;
    /// SampleId::new(2, 1, header_height).unwrap();
    /// ```
    ///
    /// [`Share`]: crate::Share
    /// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
    pub fn new(row_index: u16, column_index: u16, block_height: u64) -> Result<Self> {
        if block_height == 0 {
            return Err(Error::ZeroBlockHeight);
        }

        Ok(SampleId {
            row_id: RowId::new(row_index, block_height)?,
            column_index,
        })
    }

    /// A height of the block which contains the sample.
    pub fn block_height(&self) -> u64 {
        self.row_id.block_height()
    }

    /// Row index of the [`ExtendedDataSquare`] that sample is located on.
    ///
    /// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
    pub fn row_index(&self) -> u16 {
        self.row_id.index()
    }

    /// Column index of the [`ExtendedDataSquare`] that sample is located on.
    ///
    /// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
    pub fn column_index(&self) -> u16 {
        self.column_index
    }

    /// Encode sample id into the byte representation.
    pub fn encode(&self, bytes: &mut BytesMut) {
        bytes.reserve(SAMPLE_ID_SIZE);
        self.row_id.encode(bytes);
        bytes.put_u16(self.column_index);
    }

    /// Decode sample id from the byte representation.
    pub fn decode(buffer: &[u8]) -> Result<Self> {
        if buffer.len() != SAMPLE_ID_SIZE {
            return Err(Error::InvalidLength(buffer.len(), SAMPLE_ID_SIZE));
        }

        let (row_bytes, mut col_bytes) = buffer.split_at(ROW_ID_SIZE);
        let row_id = RowId::decode(row_bytes)?;
        let column_index = col_bytes.get_u16();

        Ok(SampleId {
            row_id,
            column_index,
        })
    }
}

impl<const S: usize> TryFrom<&CidGeneric<S>> for SampleId {
    type Error = CidError;

    fn try_from(cid: &CidGeneric<S>) -> Result<Self, Self::Error> {
        let codec = cid.codec();
        if codec != SAMPLE_ID_CODEC {
            return Err(CidError::InvalidCidCodec(codec));
        }

        let hash = cid.hash();

        let size = hash.size() as usize;
        if size != SAMPLE_ID_SIZE {
            return Err(CidError::InvalidMultihashLength(size));
        }

        let code = hash.code();
        if code != SAMPLE_ID_MULTIHASH_CODE {
            return Err(CidError::InvalidMultihashCode(
                code,
                SAMPLE_ID_MULTIHASH_CODE,
            ));
        }

        SampleId::decode(hash.digest()).map_err(|e| CidError::InvalidCid(e.to_string()))
    }
}

impl<const S: usize> TryFrom<&mut CidGeneric<S>> for SampleId {
    type Error = CidError;

    fn try_from(cid: &mut CidGeneric<S>) -> Result<Self, Self::Error> {
        Self::try_from(&*cid)
    }
}

impl<const S: usize> TryFrom<CidGeneric<S>> for SampleId {
    type Error = CidError;

    fn try_from(cid: CidGeneric<S>) -> Result<Self, Self::Error> {
        Self::try_from(&cid)
    }
}

impl From<SampleId> for CidGeneric<SAMPLE_ID_SIZE> {
    fn from(sample_id: SampleId) -> Self {
        let mut bytes = BytesMut::with_capacity(SAMPLE_ID_SIZE);
        // length is correct, so unwrap is safe
        sample_id.encode(&mut bytes);

        let mh = Multihash::wrap(SAMPLE_ID_MULTIHASH_CODE, &bytes[..]).unwrap();

        CidGeneric::new_v1(SAMPLE_ID_CODEC, mh)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::generate_dummy_eds;

    #[test]
    fn round_trip() {
        let sample_id = SampleId::new(5, 10, 100).unwrap();
        let cid = CidGeneric::from(sample_id);

        let multihash = cid.hash();
        assert_eq!(multihash.code(), SAMPLE_ID_MULTIHASH_CODE);
        assert_eq!(multihash.size(), SAMPLE_ID_SIZE as u8);

        let deserialized_sample_id = SampleId::try_from(cid).unwrap();
        assert_eq!(sample_id, deserialized_sample_id);
    }

    #[test]
    fn index_calculation() {
        let eds = generate_dummy_eds(8);

        Sample::new(0, 0, AxisType::Row, &eds).unwrap();
        Sample::new(7, 6, AxisType::Row, &eds).unwrap();
        Sample::new(7, 7, AxisType::Row, &eds).unwrap();

        let sample_err = Sample::new(7, 8, AxisType::Row, &eds).unwrap_err();
        assert!(matches!(sample_err, Error::EdsIndexOutOfRange(7, 8)));

        let sample_err = Sample::new(12, 3, AxisType::Row, &eds).unwrap_err();
        assert!(matches!(sample_err, Error::EdsIndexOutOfRange(12, 3)));
    }

    #[test]
    fn sample_id_size() {
        // Size MUST be 12 by the spec.
        assert_eq!(SAMPLE_ID_SIZE, 12);

        let sample_id = SampleId::new(0, 4, 1).unwrap();
        let mut bytes = BytesMut::new();
        sample_id.encode(&mut bytes);
        assert_eq!(bytes.len(), SAMPLE_ID_SIZE);
    }

    #[test]
    fn from_buffer() {
        let bytes = [
            0x01, // CIDv1
            0x90, 0xF0, 0x01, // CID codec = 7810
            0x91, 0xF0, 0x01, // multihash code = 7811
            0x0C, // len = SAMPLE_ID_SIZE = 12
            0, 0, 0, 0, 0, 0, 0, 64, // block height = 64
            0, 7, // row index = 7
            0, 5, // sample index = 5
        ];

        let cid = CidGeneric::<SAMPLE_ID_SIZE>::read_bytes(bytes.as_ref()).unwrap();
        assert_eq!(cid.codec(), SAMPLE_ID_CODEC);
        let mh = cid.hash();
        assert_eq!(mh.code(), SAMPLE_ID_MULTIHASH_CODE);
        assert_eq!(mh.size(), SAMPLE_ID_SIZE as u8);
        let sample_id = SampleId::try_from(cid).unwrap();
        assert_eq!(sample_id.block_height(), 64);
        assert_eq!(sample_id.row_index(), 7);
        assert_eq!(sample_id.column_index(), 5);
    }

    #[test]
    fn multihash_invalid_code() {
        let multihash = Multihash::<SAMPLE_ID_SIZE>::wrap(888, &[0; SAMPLE_ID_SIZE]).unwrap();
        let cid = CidGeneric::<SAMPLE_ID_SIZE>::new_v1(SAMPLE_ID_CODEC, multihash);
        let code_err = SampleId::try_from(cid).unwrap_err();
        assert_eq!(
            code_err,
            CidError::InvalidMultihashCode(888, SAMPLE_ID_MULTIHASH_CODE)
        );
    }

    #[test]
    fn cid_invalid_codec() {
        let multihash =
            Multihash::<SAMPLE_ID_SIZE>::wrap(SAMPLE_ID_MULTIHASH_CODE, &[0; SAMPLE_ID_SIZE])
                .unwrap();
        let cid = CidGeneric::<SAMPLE_ID_SIZE>::new_v1(4321, multihash);
        let codec_err = SampleId::try_from(cid).unwrap_err();
        assert!(matches!(codec_err, CidError::InvalidCidCodec(4321)));
    }

    #[test]
    fn test_roundtrip_verify() {
        for _ in 0..5 {
            let eds = generate_dummy_eds(2 << (rand::random::<usize>() % 8));
            let dah = DataAvailabilityHeader::from_eds(&eds);

            let row_index = rand::random::<u16>() % eds.square_width();
            let col_index = rand::random::<u16>() % eds.square_width();
            let proof_type = if rand::random() {
                AxisType::Row
            } else {
                AxisType::Col
            };

            let id = SampleId::new(row_index, col_index, 1).unwrap();
            let sample = Sample::new(row_index, col_index, proof_type, &eds).unwrap();

            let mut buf = BytesMut::new();
            sample.encode(&mut buf);
            let decoded = Sample::decode(id, &buf).unwrap();

            decoded.verify(id, &dah).unwrap();
        }
    }
}
