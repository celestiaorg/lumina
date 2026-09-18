use celestia_proto::celestia::core::v1::proof::ShareProof as RawShareProof;
use serde::{Deserialize, Serialize};
use tendermint_proto::Protobuf;

use crate::blob::shares_needed_for_blob;
use crate::hash::Hash;
use crate::{Error, Result, Share, ShareProof, bail_verification};

/// A proof of inclusion of a [`Blob`] in a [`DataAvailabilityHeader`].
///
/// This is a [`ShareProof`] of all the shares of a single blob. Unlike the
/// [`ShareProof`], which proves that some continuous range of shares was included
/// in a block, this proves that the range is exactly one blob, as it was published.
///
/// [`Blob`]: crate::Blob
/// [`DataAvailabilityHeader`]: crate::DataAvailabilityHeader
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BlobProof(pub ShareProof);

impl BlobProof {
    /// Verify the proof against the hash of [`DataAvailabilityHeader`], proving that
    /// the shares are exactly one blob.
    ///
    /// The blob is identified by its [`Namespace`] and [`Commitment`], which are not
    /// proven by this function. Reconstruct the blob from the proven shares with
    /// [`Blob::reconstruct`] and compare both with the expected ones.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    ///  - the shares are not a continuous range of shares included in the data root
    ///  - the shares are in a reserved namespace
    ///  - the first share is not a first share of a blob
    ///  - the amount of shares doesn't match the length of the blob
    ///
    /// [`Blob::reconstruct`]: crate::Blob::reconstruct
    /// [`Commitment`]: crate::Commitment
    /// [`DataAvailabilityHeader`]: crate::DataAvailabilityHeader
    /// [`Namespace`]: crate::nmt::Namespace
    pub fn verify(&self, root: Hash) -> Result<()> {
        self.0.verify(root)?;

        // reserved namespaces hold compact shares, which have a different layout
        if self.0.namespace_id.is_reserved() {
            bail_verification!(
                "namespace ({:?}) is reserved, so it doesn't hold blobs",
                self.0.namespace_id
            );
        }

        let first_share = self
            .0
            .shares()
            .first()
            .map(|share| Share::from_raw(share))
            .transpose()?
            .ok_or(Error::MissingShares)?;

        let blob_len = first_share
            .sequence_length()
            .ok_or(Error::ExpectedShareWithSequenceStart)?;

        // shares of a zero length sequence are padding, not a blob
        if blob_len == 0 {
            bail_verification!("blob has no data");
        }

        // blobs have no end marker, their length in shares comes from the first share
        let shares_needed =
            shares_needed_for_blob(blob_len as usize, first_share.signer().is_some());

        if self.0.shares().len() != shares_needed {
            bail_verification!(
                "proof has ({}) shares, blob of ({}) bytes occupies ({})",
                self.0.shares().len(),
                blob_len,
                shares_needed
            );
        }

        Ok(())
    }

    /// Get the index of the blob's first share in the original data square.
    ///
    /// The index is only proven if [`BlobProof::verify`] succeeded for the same proof.
    pub fn index(&self) -> Option<u64> {
        let merkle_proof = self.0.row_proof.proofs().first()?;
        let nmt_proof = self.0.share_proofs.first()?;
        let ods_size = merkle_proof.total as u64 / 4;

        (merkle_proof.index as u64)
            .checked_mul(ods_size)?
            .checked_add(u64::from(nmt_proof.start_idx()))
    }
}

impl From<ShareProof> for BlobProof {
    fn from(value: ShareProof) -> Self {
        BlobProof(value)
    }
}

impl From<BlobProof> for ShareProof {
    fn from(value: BlobProof) -> Self {
        value.0
    }
}

impl Protobuf<RawShareProof> for BlobProof {}

impl TryFrom<RawShareProof> for BlobProof {
    type Error = Error;

    fn try_from(value: RawShareProof) -> Result<Self> {
        Ok(BlobProof(value.try_into()?))
    }
}

impl From<BlobProof> for RawShareProof {
    fn from(value: BlobProof) -> Self {
        value.0.into()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;

    use crate::consts::appconsts::SHARE_SIZE;
    use crate::nmt::{NS_SIZE, Namespace};
    use crate::test_utils::{
        SquareEntry, generate_eds_with_blob_lengths, generate_eds_with_layout,
        share_proof_for_range, share_proof_for_rows,
    };
    use crate::{Blob, DataAvailabilityHeader, ExtendedDataSquare, Share, ShareProof};

    use super::BlobProof;

    /// Proof for a range of ODS share indexes, as `share.GetRange` would return.
    fn proof_for_range(eds: &ExtendedDataSquare, range: Range<usize>) -> BlobProof {
        share_proof_for_range(eds, range).into()
    }

    /// Proof built from genuine per row proofs, claiming the rows are consecutive.
    fn proof_for_rows(eds: &ExtendedDataSquare, rows: &[(u16, Range<usize>)]) -> BlobProof {
        share_proof_for_rows(eds, rows).into()
    }

    #[test]
    fn verify_blob() {
        // 3 shares, then a blob spanning rows 0 and 1, then 2 shares
        let (eds, blobs) = generate_eds_with_blob_lengths(8, &[3, 5, 2]);
        let root = DataAvailabilityHeader::from_eds(&eds).hash();

        for (idx, range) in [0..3, 3..8, 8..10].into_iter().enumerate() {
            let index = range.start as u64;
            let proof = proof_for_range(&eds, range);

            proof.verify(root).unwrap();

            let shares: Vec<_> = proof
                .0
                .shares()
                .iter()
                .map(|share| Share::from_raw(share).unwrap())
                .collect();
            assert_eq!(Blob::reconstruct(&shares).unwrap(), blobs[idx]);
            assert_eq!(proof.index(), Some(index));
        }
    }

    #[test]
    fn verify_rejects_blob_spliced_from_two_blobs() {
        // blob B occupies (row 0, col 3) and (row 1, col 0), blob C (row 1, col 1) and (row 1, col 2)
        let (eds, blobs) = generate_eds_with_blob_lengths(8, &[3, 2, 2]);
        let root = DataAvailabilityHeader::from_eds(&eds).hash();

        // first share of B and last share of C, which are not adjacent
        let proof = proof_for_rows(&eds, &[(0, 3..4), (1, 2..3)]);

        let shares: Vec<_> = proof
            .0
            .shares()
            .iter()
            .map(|share| Share::from_raw(share).unwrap())
            .collect();
        let spliced = Blob::reconstruct(&shares).unwrap();
        assert!(
            blobs
                .iter()
                .all(|blob| blob.commitment != spliced.commitment)
        );

        let err = proof.verify(root).unwrap_err().to_string();
        assert!(err.contains("not continuous"), "{err}");
    }

    #[test]
    fn verify_rejects_part_of_blob() {
        let (eds, _) = generate_eds_with_blob_lengths(8, &[3, 5, 2]);
        let root = DataAvailabilityHeader::from_eds(&eds).hash();

        // first 2 shares of a 3 share blob
        let err = proof_for_range(&eds, 0..2)
            .verify(root)
            .unwrap_err()
            .to_string();
        assert!(err.contains("occupies"), "{err}");

        // first row of a blob spanning 2 rows
        let err = proof_for_range(&eds, 3..4)
            .verify(root)
            .unwrap_err()
            .to_string();
        assert!(err.contains("occupies"), "{err}");
    }

    #[test]
    fn verify_rejects_blob_with_extra_shares() {
        let (eds, _) = generate_eds_with_blob_lengths(8, &[3, 5, 2]);
        let root = DataAvailabilityHeader::from_eds(&eds).hash();

        // a blob and a share of the next one
        let err = proof_for_range(&eds, 0..4)
            .verify(root)
            .unwrap_err()
            .to_string();
        assert!(err.contains("occupies"), "{err}");
    }

    #[test]
    fn verify_rejects_range_starting_mid_blob() {
        let (eds, _) = generate_eds_with_blob_lengths(8, &[3, 5, 2]);
        let root = DataAvailabilityHeader::from_eds(&eds).hash();

        let err = proof_for_range(&eds, 1..3)
            .verify(root)
            .unwrap_err()
            .to_string();
        assert!(err.contains("Expected first share of a blob"), "{err}");
    }

    #[test]
    fn verify_rejects_padding_shares() {
        let ns = Namespace::const_v0([7; 10]);
        // padding shares are sequence starts of zero length
        let mut padding = vec![0u8; SHARE_SIZE];
        padding[..NS_SIZE].copy_from_slice(ns.as_bytes());
        padding[NS_SIZE] = 0x01;

        let mut shares = vec![padding];
        shares.resize(16, [ns.as_bytes(), &[0; SHARE_SIZE - NS_SIZE][..]].concat());

        let eds = ExtendedDataSquare::from_ods(shares).unwrap();
        let root = DataAvailabilityHeader::from_eds(&eds).hash();

        let err = proof_for_range(&eds, 0..1)
            .verify(root)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no data"), "{err}");
    }

    #[test]
    fn verify_rejects_reserved_namespace() {
        let (eds, _) =
            generate_eds_with_layout(8, &[SquareEntry::Reserved(Namespace::PAY_FOR_BLOB, 5)]);
        let root = DataAvailabilityHeader::from_eds(&eds).hash();

        let err = proof_for_range(&eds, 0..4)
            .verify(root)
            .unwrap_err()
            .to_string();
        assert!(err.contains("reserved"), "{err}");
    }

    #[test]
    fn blob_proof_serde() {
        let (eds, _) = generate_eds_with_blob_lengths(8, &[3, 5, 2]);
        let proof = proof_for_range(&eds, 0..3);

        let serialized = serde_json::to_string(&proof).unwrap();
        let deserialized: BlobProof = serde_json::from_str(&serialized).unwrap();
        assert_eq!(proof, deserialized);

        // same wire format as the share proof it is made of
        let as_share_proof: ShareProof = serde_json::from_str(&serialized).unwrap();
        assert_eq!(as_share_proof, proof.0);
    }
}
