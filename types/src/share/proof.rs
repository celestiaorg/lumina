use celestia_proto::celestia::core::v1::proof::ShareProof as RawShareProof;
use serde::{Deserialize, Serialize};
use tendermint_proto::Protobuf;

use std::ops::Range;

use crate::consts::appconsts::SHARE_SIZE;
use crate::hash::Hash;
use crate::nmt::NamespaceProof;
use crate::{Error, Result, nmt::Namespace};
use crate::{RowProof, bail_verification, validation_error};

/// A proof of inclusion of a continouous range of shares of some namespace
/// in a [`DataAvailabilityHeader`].
///
/// The proof will proof the inclusion of shares in row roots they span
/// and the inclusion of those row roots in the dah.
///
/// [`DataAvailabilityHeader`]: crate::DataAvailabilityHeader
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawShareProof", into = "RawShareProof")]
pub struct ShareProof {
    /// The shares, as a Vec of byte arrays
    pub data: Vec<[u8; SHARE_SIZE]>,
    /// The namespace of the shares
    pub namespace_id: Namespace,
    /// Vec of the NMT multiproofs for the rows spanned by the range
    pub share_proofs: Vec<NamespaceProof>,
    /// Proofs for row roots into data root
    pub row_proof: RowProof,
}

impl ShareProof {
    /// Get the shares proven by this proof.
    pub fn shares(&self) -> &[[u8; SHARE_SIZE]] {
        &self.data
    }

    /// Verify the proof against the hash of [`DataAvailabilityHeader`], proving
    /// the inclusion of shares.
    ///
    /// The proof doesn't say anything about the height of the block, it is bound to
    /// the one of the given data root. Use [`ShareProof::covered_range`] to get the
    /// position of the shares in the square, or [`ShareProof::verify_range`] to check
    /// that they are at the expected one.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    ///  - the proof is malformed. Number of shares, nmt proofs and row proofs needs to match.
    ///  - the shares are not a continuous range of the original data square
    ///  - the verification of any inner row proof fails
    ///
    /// [`DataAvailabilityHeader`]: crate::DataAvailabilityHeader
    pub fn verify(&self, root: Hash) -> Result<()> {
        self.covered_range(root)?;
        Ok(())
    }

    /// Verify the proof and check that the shares are at the given range of indexes
    /// in the original data square.
    ///
    /// # Errors
    ///
    /// This function will return an error if the verification fails or if the shares
    /// are at a different range than the expected one.
    pub fn verify_range(&self, root: Hash, range: Range<u64>) -> Result<()> {
        let covered_range = self.covered_range(root)?;

        if covered_range != range {
            bail_verification!(
                "shares are at range ({:?}), expected ({:?})",
                covered_range,
                range
            );
        }

        Ok(())
    }

    /// Verify the proof and return the range of indexes the shares occupy in the
    /// original data square.
    ///
    /// # Errors
    ///
    /// This function will return an error if the verification fails. See
    /// [`ShareProof::verify`].
    pub fn covered_range(&self, root: Hash) -> Result<Range<u64>> {
        let row_roots = self.row_proof.row_roots();

        if self.share_proofs.is_empty() {
            bail_verification!("proof without shares");
        }

        if self.share_proofs.len() != row_roots.len() {
            bail_verification!(
                "share proofs length ({}) != row roots length ({})",
                self.share_proofs.len(),
                row_roots.len()
            );
        }

        self.row_proof.verify(root)?;

        // Leaves of the data root tree are row roots followed by column roots of the EDS.
        // `RowProof::verify` proved that they are the consecutive ones starting at `start_row`.
        let ods_size = self.row_proof.proofs()[0].total / 4;
        let start_row = usize::from(self.row_proof.start_row());
        let last = self.share_proofs.len() - 1;

        let mut data = self.data.as_slice();

        for (i, (proof, row_root)) in self.share_proofs.iter().zip(row_roots).enumerate() {
            if proof.is_of_absence() {
                bail_verification!("only presence proofs allowed");
            }
            if proof.start_idx() >= proof.end_idx() {
                bail_verification!("proof without data");
            }

            let row = start_row + i;
            let start_idx = proof.start_idx() as usize;
            let end_idx = proof.end_idx() as usize;

            if row >= ods_size {
                bail_verification!("row ({}) is outside of the original data square", row);
            }
            if end_idx > ods_size {
                bail_verification!("shares in row ({}) extend into parity data", row);
            }
            // A range spanning multiple rows must continue from the last column
            // of a row to the first column of the next one.
            if i > 0 && start_idx != 0 {
                bail_verification!(
                    "shares are not continuous: row ({}) doesn't start at column 0",
                    row
                );
            }
            if i < last && end_idx != ods_size {
                bail_verification!(
                    "shares are not continuous: row ({}) doesn't end at column ({})",
                    row,
                    ods_size - 1
                );
            }

            let shares_in_row = end_idx - start_idx;
            if data.len() < shares_in_row {
                bail_verification!(
                    "shares needed ({}) > proof's data length ({})",
                    shares_in_row,
                    data.len()
                );
            }

            let (leaves, rest) = data.split_at(shares_in_row);
            proof
                .verify_range(row_root, leaves, *self.namespace_id)
                .map_err(Error::RangeProofError)?;
            data = rest;
        }

        if !data.is_empty() {
            bail_verification!("proof has ({}) shares which are not proven", data.len());
        }

        let ods_size = ods_size as u64;
        let first_proof = &self.share_proofs[0];
        let last_proof = &self.share_proofs[last];
        let start = start_row as u64 * ods_size + u64::from(first_proof.start_idx());
        let end = (start_row + last) as u64 * ods_size + u64::from(last_proof.end_idx());

        Ok(start..end)
    }
}

impl Protobuf<RawShareProof> for ShareProof {}

impl TryFrom<RawShareProof> for ShareProof {
    type Error = Error;

    fn try_from(value: RawShareProof) -> Result<Self> {
        Ok(Self {
            data: value
                .data
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()
                .map_err(|_| validation_error!("invalid share size"))?,
            namespace_id: Namespace::new(
                value
                    .namespace_version
                    .try_into()
                    .map_err(|_| validation_error!("namespace version must be single byte"))?,
                &value.namespace_id,
            )?,
            share_proofs: value
                .share_proofs
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_>>()?,
            row_proof: value
                .row_proof
                .ok_or_else(|| validation_error!("row proof missing"))?
                .try_into()?,
        })
    }
}

impl From<ShareProof> for RawShareProof {
    fn from(value: ShareProof) -> Self {
        Self {
            data: value.data.into_iter().map(Into::into).collect(),
            namespace_id: value.namespace_id.id().to_vec(),
            namespace_version: value.namespace_id.version() as u32,
            share_proofs: value.share_proofs.into_iter().map(Into::into).collect(),
            row_proof: Some(value.row_proof.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use celestia_proto::celestia::core::v1::proof::RowProof as RawRowProof;

    use crate::test_utils::{generate_dummy_eds, share_proof_for_range, share_proof_for_rows};
    use crate::{DataAvailabilityHeader, ExtendedDataSquare};

    use super::ShareProof;

    fn verify_err(proof: &ShareProof, eds: &ExtendedDataSquare) -> String {
        let root = DataAvailabilityHeader::from_eds(eds).hash();
        proof.verify(root).unwrap_err().to_string()
    }

    #[test]
    fn continuous_ranges_verify() {
        for square_width in [2usize, 4, 8] {
            let ods_shares = (square_width / 2).pow(2);
            let eds = generate_dummy_eds(square_width);
            let root = DataAvailabilityHeader::from_eds(&eds).hash();

            for start in 0..ods_shares {
                for end in start + 1..=ods_shares {
                    let proof = share_proof_for_range(&eds, start..end);
                    let range = start as u64..end as u64;

                    proof.verify(root).unwrap();
                    assert_eq!(proof.covered_range(root).unwrap(), range);
                    proof.verify_range(root, range).unwrap();
                }
            }
        }
    }

    #[test]
    fn verify_range_rejects_another_range() {
        let eds = generate_dummy_eds(8);
        let root = DataAvailabilityHeader::from_eds(&eds).hash();
        let proof = share_proof_for_range(&eds, 3..6);

        let err = proof.verify_range(root, 4..7).unwrap_err().to_string();
        assert!(err.contains("shares are at range"), "{err}");
    }

    #[test]
    fn shares_from_non_adjacent_positions_in_consecutive_rows() {
        let eds = generate_dummy_eds(8);

        // the last share in row 0 is skipped
        let proof = share_proof_for_rows(&eds, &[(0, 2..3), (1, 0..2)]);
        assert!(verify_err(&proof, &eds).contains("not continuous"));

        // the first share in row 1 is skipped
        let proof = share_proof_for_rows(&eds, &[(0, 2..4), (1, 1..2)]);
        assert!(verify_err(&proof, &eds).contains("not continuous"));

        // gaps on both sides
        let proof = share_proof_for_rows(&eds, &[(0, 2..3), (1, 1..2)]);
        assert!(verify_err(&proof, &eds).contains("not continuous"));

        // middle row is not full
        let proof = share_proof_for_rows(&eds, &[(0, 3..4), (1, 0..3), (2, 0..1)]);
        assert!(verify_err(&proof, &eds).contains("not continuous"));
    }

    #[test]
    fn shares_from_non_adjacent_rows() {
        let eds = generate_dummy_eds(8);

        // row 1 is skipped
        let mut proof = share_proof_for_range(&eds, 3..5);
        let row_2 = share_proof_for_rows(&eds, &[(2, 0..1)]);
        proof.data[1] = row_2.data[0];
        proof.share_proofs[1] = row_2.share_proofs[0].clone();
        let mut raw = RawRowProof::from(proof.row_proof);
        let raw_row_2 = RawRowProof::from(row_2.row_proof);
        raw.row_roots[1] = raw_row_2.row_roots[0].clone();
        raw.proofs[1] = raw_row_2.proofs[0].clone();
        proof.row_proof = raw.try_into().unwrap();

        assert!(verify_err(&proof, &eds).contains("row proof index"));
    }

    #[test]
    fn shares_from_the_same_row_twice() {
        let eds = generate_dummy_eds(8);

        let proof = share_proof_for_rows(&eds, &[(0, 0..1), (0, 3..4)]);
        assert!(verify_err(&proof, &eds).contains("row proof index"));
    }

    #[test]
    fn shares_outside_of_ods() {
        let eds = generate_dummy_eds(8);

        // parity shares in the ODS row
        let proof = share_proof_for_rows(&eds, &[(0, 3..5)]);
        assert!(verify_err(&proof, &eds).contains("parity"));

        // parity row
        let proof = share_proof_for_rows(&eds, &[(4, 0..1)]);
        assert!(verify_err(&proof, &eds).contains("outside of the original data square"));
    }

    #[test]
    fn share_proof_serde() {
        let raw_share_proof = r#"{
          "data": [
            "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"
          ],
          "namespace_id": "AAAAAAAAAAAAAAAAAAAAAAAA2Mu1M6JCYcTAow==",
          "namespace_version": 0,
          "share_proofs": [
            {
              "end": 2,
              "nodes": [
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABCU0aUrR/wpx09HFWeoyuV1vuw5Ew3rhtCaf/Zd4chb9",
                "/////////////////////////////////////////////////////////////////////////////ypPU4ZqDz1t8YcunXI8ETuBth1gXLvPWIMd0JPoeJF3"
              ],
              "start": 1
            },
            {
              "end": 2,
              "nodes": [
                "/////////////////////////////////////////////////////////////////////////////wdXw/2tc8hhuGLcsfU9pWo5BDIKSsNJFCytj++xtFgq"
              ]
            }
          ],
          "row_proof": {
             "end_row": 1,
             "proofs": [
               {
                 "aunts": [
                   "Ch+9PsBdsN5YUt8nvAmjdOAIcVdfmPAEUNmCA8KBe5A=",
                   "ojjC9H5JG/7OOrt5BzBXs/3w+n1LUI/0YR0d+RSfleU=",
                   "d6bMQbLTBfZGvqXOW9MPqRM+fTB2/wLJx6CkLc8glCI="
                 ],
                 "index": 0,
                 "leaf_hash": "nOpM3A3d0JYOmNaaI5BFAeKPGwQ90TqmM/kx+sHr79s=",
                 "total": 8
               },
               {
                 "aunts": [
                   "nOpM3A3d0JYOmNaaI5BFAeKPGwQ90TqmM/kx+sHr79s=",
                   "ojjC9H5JG/7OOrt5BzBXs/3w+n1LUI/0YR0d+RSfleU=",
                   "d6bMQbLTBfZGvqXOW9MPqRM+fTB2/wLJx6CkLc8glCI="
                 ],
                 "index": 1,
                 "leaf_hash": "Ch+9PsBdsN5YUt8nvAmjdOAIcVdfmPAEUNmCA8KBe5A=",
                 "total": 8
               }
             ],
             "row_roots": [
               "000000000000000000000000000000000000000000000000000000000400000000000000000000000000000000000000D8CBB533A24261C4C0A3D37F1CBFB6F4C5EA031472EBA390D482637933874AA0A2B9735E67629993852D",
               "00000000000000000000000000000000000000D8CBB533A24261C4C0A300000000000000000000000000000000000000D8CBB533A24261C4C0A37E409334CCB1125C793EC040741137634C148F089ACB06BFFF4C1C4CA2CBBA8E"
             ],
             "start_row": 0
          }
        }"#;
        let raw_dah = r#"
          {
            "row_roots": [
              "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAA2Mu1M6JCYcTAo9N/HL+29MXqAxRy66OQ1IJjeTOHSqCiuXNeZ2KZk4Ut",
              "AAAAAAAAAAAAAAAAAAAAAAAAANjLtTOiQmHEwKMAAAAAAAAAAAAAAAAAAAAAAAAA2Mu1M6JCYcTAo35AkzTMsRJceT7AQHQRN2NMFI8ImssGv/9MHEyiy7qO",
              "/////////////////////////////////////////////////////////////////////////////7mTwL+NxdxcYBd89/wRzW2k9vRkQehZiXsuqZXHy89X",
              "/////////////////////////////////////////////////////////////////////////////2X/FT2ugeYdWmvnEisSgW+9Ih8paNvrji2NYPb8ujaK"
            ],
            "column_roots": [
              "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAA2Mu1M6JCYcTAo/xEv//wkWzNtkcAZiZmSGU1Te6ERwUxTtTfHzoS4bv+",
              "AAAAAAAAAAAAAAAAAAAAAAAAANjLtTOiQmHEwKMAAAAAAAAAAAAAAAAAAAAAAAAA2Mu1M6JCYcTAo9FOCNvCjA42xYCwHrlo48iPEXLaKt+d+JdErCIrQIi6",
              "/////////////////////////////////////////////////////////////////////////////y2UErq/83uv433HekCWokxqcY4g+nMQn3tZn2Tr6v74",
              "/////////////////////////////////////////////////////////////////////////////z6fKmbJTvfLYFlNuDWHn87vJb6V7n44MlCkxv1dyfT2"
            ]
          }
        "#;

        let proof: ShareProof = serde_json::from_str(raw_share_proof).unwrap();
        let dah: DataAvailabilityHeader = serde_json::from_str(raw_dah).unwrap();

        proof.verify(dah.hash()).unwrap()
    }
}
