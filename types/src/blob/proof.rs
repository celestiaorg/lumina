use celestia_proto::celestia::core::v1::proof::ShareProof as RawShareProof;
use serde::{Deserialize, Serialize};
use tendermint_proto::Protobuf;

use crate::blob::shares_needed_for_blob;
use crate::hash::Hash;
use crate::nmt::Namespace;
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
    /// the shares are exactly one blob of a given [`Namespace`].
    ///
    /// The proven shares can be turned into a blob with [`Blob::reconstruct`] and
    /// its [`Commitment`] compared with the expected one.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    ///  - the shares are not a continuous range of shares included in the data root
    ///  - the shares are not in the given namespace
    ///  - the first share is not a first share of a blob
    ///  - the amount of shares doesn't match the length of the blob
    ///
    /// [`Blob::reconstruct`]: crate::Blob::reconstruct
    /// [`Commitment`]: crate::Commitment
    /// [`DataAvailabilityHeader`]: crate::DataAvailabilityHeader
    pub fn verify(&self, root: Hash, namespace: Namespace) -> Result<()> {
        self.0.verify(root)?;

        if self.0.namespace_id != namespace {
            bail_verification!(
                "proof is for namespace ({:?}), expected ({:?})",
                self.0.namespace_id,
                namespace
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

        Some(merkle_proof.index as u64 * ods_size + u64::from(nmt_proof.start_idx()))
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

    use celestia_proto::celestia::core::v1::proof::RowProof as RawRowProof;
    use nmt_rs::NamespaceProof as NmtNamespaceProof;

    use crate::consts::appconsts::{
        CONTINUATION_SPARSE_SHARE_CONTENT_SIZE, FIRST_SPARSE_SHARE_CONTENT_SIZE, SHARE_SIZE,
    };
    use crate::nmt::{NS_SIZE, Namespace, NamespaceProof};
    use crate::test_utils::random_bytes;
    use crate::{Blob, DataAvailabilityHeader, ExtendedDataSquare, Share, ShareProof};

    use super::BlobProof;

    const NS: Namespace = Namespace::const_v0([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    /// A square with blobs of given lengths in shares, laid out one after another.
    fn eds_with_blobs(
        square_width: usize,
        blob_shares: &[usize],
    ) -> (ExtendedDataSquare, Vec<Blob>) {
        let ods_size = square_width / 2;
        let mut blobs = Vec::new();
        let mut shares: Vec<Vec<u8>> = Vec::new();

        for len in blob_shares {
            let data = random_bytes(
                FIRST_SPARSE_SHARE_CONTENT_SIZE
                    + (len - 1) * CONTINUATION_SPARSE_SHARE_CONTENT_SIZE,
            );
            let blob = Blob::new(NS, data, None).unwrap();
            shares.extend(blob.to_shares().unwrap().iter().map(Share::to_vec));
            blobs.push(blob);
        }

        while shares.len() < ods_size * ods_size {
            shares.push(
                [
                    Namespace::TAIL_PADDING.as_bytes(),
                    &[0; SHARE_SIZE - NS_SIZE][..],
                ]
                .concat(),
            );
        }

        (ExtendedDataSquare::from_ods(shares).unwrap(), blobs)
    }

    /// Proof for a range of ODS share indexes, as `share.GetRange` would return.
    fn proof_for_range(eds: &ExtendedDataSquare, range: Range<usize>) -> BlobProof {
        let ods_size = usize::from(eds.square_width() / 2);
        let rows: Vec<_> = (range.start / ods_size..=(range.end - 1) / ods_size)
            .map(|row| {
                let row_start = row * ods_size;
                let start = range.start.max(row_start) - row_start;
                let end = range.end.min(row_start + ods_size) - row_start;
                (row as u16, start..end)
            })
            .collect();

        proof_for_rows(eds, &rows)
    }

    /// Proof built from genuine per row proofs, claiming the rows are consecutive.
    fn proof_for_rows(eds: &ExtendedDataSquare, rows: &[(u16, Range<usize>)]) -> BlobProof {
        let dah = DataAvailabilityHeader::from_eds(eds);
        let mut data = Vec::new();
        let mut share_proofs: Vec<NamespaceProof> = Vec::new();
        let mut row_proof = RawRowProof {
            start_row: rows[0].0.into(),
            end_row: u32::from(rows[0].0) + rows.len() as u32 - 1,
            ..Default::default()
        };

        for (row, columns) in rows {
            let (row, start, end) = (*row, columns.start, columns.end);

            for column in start..end {
                data.push(*eds.share(row, column as u16).unwrap().data());
            }
            let proof = eds.row_nmt(row).unwrap().build_range_proof(start..end);
            share_proofs.push(
                NmtNamespaceProof::PresenceProof {
                    proof,
                    ignore_max_ns: true,
                }
                .into(),
            );

            let single = RawRowProof::from(dah.row_proof(row..=row).unwrap());
            row_proof.row_roots.extend(single.row_roots);
            row_proof.proofs.extend(single.proofs);
        }

        BlobProof(ShareProof {
            data,
            namespace_id: NS,
            share_proofs,
            row_proof: row_proof.try_into().unwrap(),
        })
    }

    #[test]
    fn verify_blob() {
        // 3 shares, then a blob spanning rows 0 and 1, then 2 shares
        let (eds, blobs) = eds_with_blobs(8, &[3, 5, 2]);
        let root = DataAvailabilityHeader::from_eds(&eds).hash();

        for (idx, range) in [0..3, 3..8, 8..10].into_iter().enumerate() {
            let index = range.start as u64;
            let proof = proof_for_range(&eds, range);

            proof.verify(root, NS).unwrap();

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
        let (eds, blobs) = eds_with_blobs(8, &[3, 2, 2]);
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

        let err = proof.verify(root, NS).unwrap_err().to_string();
        assert!(err.contains("not continuous"), "{err}");
    }

    #[test]
    fn verify_rejects_another_namespace() {
        let (eds, _) = eds_with_blobs(8, &[3, 5, 2]);
        let root = DataAvailabilityHeader::from_eds(&eds).hash();
        let proof = proof_for_range(&eds, 0..3);

        let err = proof
            .verify(root, Namespace::const_v0([9; 10]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("namespace"), "{err}");
    }

    #[test]
    fn verify_rejects_part_of_blob() {
        let (eds, _) = eds_with_blobs(8, &[3, 5, 2]);
        let root = DataAvailabilityHeader::from_eds(&eds).hash();

        // first 2 shares of a 3 share blob
        let err = proof_for_range(&eds, 0..2)
            .verify(root, NS)
            .unwrap_err()
            .to_string();
        assert!(err.contains("occupies"), "{err}");

        // first row of a blob spanning 2 rows
        let err = proof_for_range(&eds, 3..4)
            .verify(root, NS)
            .unwrap_err()
            .to_string();
        assert!(err.contains("occupies"), "{err}");
    }

    #[test]
    fn verify_rejects_blob_with_extra_shares() {
        let (eds, _) = eds_with_blobs(8, &[3, 5, 2]);
        let root = DataAvailabilityHeader::from_eds(&eds).hash();

        // a blob and a share of the next one
        let err = proof_for_range(&eds, 0..4)
            .verify(root, NS)
            .unwrap_err()
            .to_string();
        assert!(err.contains("occupies"), "{err}");
    }

    #[test]
    fn verify_rejects_range_starting_mid_blob() {
        let (eds, _) = eds_with_blobs(8, &[3, 5, 2]);
        let root = DataAvailabilityHeader::from_eds(&eds).hash();

        let err = proof_for_range(&eds, 1..3)
            .verify(root, NS)
            .unwrap_err()
            .to_string();
        assert!(err.contains("Expected first share of a blob"), "{err}");
    }

    #[test]
    fn blob_proof_serde() {
        let (eds, _) = eds_with_blobs(8, &[3, 5, 2]);
        let proof = proof_for_range(&eds, 0..3);

        let serialized = serde_json::to_string(&proof).unwrap();
        let deserialized: BlobProof = serde_json::from_str(&serialized).unwrap();
        assert_eq!(proof, deserialized);

        // same wire format as the share proof it is made of
        let as_share_proof: ShareProof = serde_json::from_str(&serialized).unwrap();
        assert_eq!(as_share_proof, proof.0);
    }
}
