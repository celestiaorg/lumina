use std::ops::RangeInclusive;

use celestia_proto::celestia::core::v1::da::DataAvailabilityHeader as RawDataAvailabilityHeader;
use celestia_proto::celestia::core::v1::proof::RowProof as RawRowProof;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tendermint::merkle::simple_hash_from_byte_vectors;
use tendermint_proto::Protobuf;
#[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
use wasm_bindgen::prelude::*;

use crate::consts::data_availability_header::MIN_EXTENDED_SQUARE_WIDTH;
use crate::eds::AxisType;
use crate::hash::Hash;
use crate::nmt::{Namespace, NamespacedHash, NamespacedHashExt, NamespacedSha2Hasher};
use crate::{
    Error, ExtendedDataSquare, MerkleProof, Result, ValidateBasic, ValidationError,
    bail_validation, bail_verification, validation_error,
};

/// Header with commitments of the data availability.
///
/// It consists of the root hashes of the merkle trees created from each
/// row and column of the [`ExtendedDataSquare`]. Those are used to prove
/// the inclusion of the data in a block.
///
/// The hash of this header is a hash of all rows and columns and thus a
/// data commitment of the block.
///
/// # Example
///
/// ```no_run
/// # use celestia_types::{ExtendedHeader, Height, Share};
/// # use celestia_types::nmt::{Namespace, NamespaceProof};
/// # fn extended_header() -> ExtendedHeader {
/// #     unimplemented!();
/// # }
/// # fn shares_with_proof(_: u64, _: &Namespace) -> (Vec<Share>, NamespaceProof) {
/// #     unimplemented!();
/// # }
/// // fetch the block header and data for your namespace
/// let namespace = Namespace::new_v0(&[1, 2, 3, 4]).unwrap();
/// let eh = extended_header();
/// let (shares, proof) = shares_with_proof(eh.height(), &namespace);
///
/// // get the data commitment for a given row
/// let dah = eh.dah;
/// let root = dah.row_root(0).unwrap();
///
/// // verify a proof of the inclusion of the shares
/// assert!(proof.verify_complete_namespace(&root, &shares, *namespace).is_ok());
/// ```
///
/// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "RawDataAvailabilityHeader",
    into = "RawDataAvailabilityHeader"
)]
#[cfg_attr(
    all(feature = "wasm-bindgen", target_arch = "wasm32"),
    wasm_bindgen(inspectable)
)]
pub struct DataAvailabilityHeader {
    /// Merkle roots of the [`ExtendedDataSquare`] rows.
    row_roots: Vec<NamespacedHash>,
    /// Merkle roots of the [`ExtendedDataSquare`] columns.
    column_roots: Vec<NamespacedHash>,
}

impl DataAvailabilityHeader {
    /// Create new [`DataAvailabilityHeader`].
    pub fn new(row_roots: Vec<NamespacedHash>, column_roots: Vec<NamespacedHash>) -> Result<Self> {
        let dah = DataAvailabilityHeader::new_unchecked(row_roots, column_roots);
        dah.validate_basic()?;
        Ok(dah)
    }

    /// Create new non-validated [`DataAvailabilityHeader`].
    ///
    /// [`DataAvailabilityHeader::validate_basic`] can be used to check valitidy later on.
    pub fn new_unchecked(
        row_roots: Vec<NamespacedHash>,
        column_roots: Vec<NamespacedHash>,
    ) -> Self {
        DataAvailabilityHeader {
            row_roots,
            column_roots,
        }
    }

    /// Create a DataAvailabilityHeader by computing roots of a given [`ExtendedDataSquare`].
    pub fn from_eds(eds: &ExtendedDataSquare) -> Self {
        let square_width = eds.square_width();

        let mut dah = DataAvailabilityHeader {
            row_roots: Vec::with_capacity(square_width.into()),
            column_roots: Vec::with_capacity(square_width.into()),
        };

        for i in 0..square_width {
            let row_root = eds
                .row_nmt(i)
                .expect("EDS validated on construction")
                .root();
            dah.row_roots.push(row_root);

            let column_root = eds
                .column_nmt(i)
                .expect("EDS validated on construction")
                .root();
            dah.column_roots.push(column_root);
        }

        dah
    }

    /// Get the root from an axis at the given index.
    pub fn root(&self, axis: AxisType, index: u16) -> Option<NamespacedHash> {
        match axis {
            AxisType::Col => self.column_root(index),
            AxisType::Row => self.row_root(index),
        }
    }

    /// Merkle roots of the [`ExtendedDataSquare`] rows.
    ///
    /// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
    pub fn row_roots(&self) -> &[NamespacedHash] {
        &self.row_roots
    }

    /// Merkle roots of the [`ExtendedDataSquare`] columns.
    ///
    /// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
    pub fn column_roots(&self) -> &[NamespacedHash] {
        &self.column_roots
    }

    /// Get a root of the row with the given index.
    pub fn row_root(&self, row: u16) -> Option<NamespacedHash> {
        let row = usize::from(row);
        self.row_roots.get(row).cloned()
    }

    /// Check if row with given index contains provided namespace.
    pub fn row_contains(&self, row: u16, namespace: Namespace) -> Result<bool> {
        let row_root = self
            .row_root(row)
            .ok_or(Error::IndexOutOfRange(row as usize, self.row_roots.len()))?;
        Ok(row_root.contains::<NamespacedSha2Hasher>(*namespace))
    }

    /// Get the a root of the column with the given index.
    pub fn column_root(&self, column: u16) -> Option<NamespacedHash> {
        let column = usize::from(column);
        self.column_roots.get(column).cloned()
    }

    /// Check if column with given index contains provided namespace.
    pub fn column_contains(&self, column: u16, namespace: Namespace) -> Result<bool> {
        let column_root = self.column_root(column).ok_or(Error::IndexOutOfRange(
            column as usize,
            self.column_roots.len(),
        ))?;
        Ok(column_root.contains::<NamespacedSha2Hasher>(*namespace))
    }

    /// Compute the combined hash of all rows and columns.
    ///
    /// This is the data commitment for the block.
    ///
    /// # Example
    ///
    /// ```
    /// # use celestia_types::ExtendedHeader;
    /// # fn get_extended_header() -> ExtendedHeader {
    /// #   let s = include_str!("../test_data/chain1/extended_header_block_1.json");
    /// #   serde_json::from_str(s).unwrap()
    /// # }
    /// let eh = get_extended_header();
    /// let dah = eh.dah;
    ///
    /// assert_eq!(dah.hash(), eh.header.data_hash.unwrap());
    /// ```
    pub fn hash(&self) -> Hash {
        let all_roots: Vec<_> = self
            .row_roots
            .iter()
            .chain(self.column_roots.iter())
            .map(|root| root.to_array())
            .collect();

        Hash::Sha256(simple_hash_from_byte_vectors::<Sha256>(&all_roots))
    }

    /// Get the size of the [`ExtendedDataSquare`] for which this header was built.
    ///
    /// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
    pub fn square_width(&self) -> u16 {
        // `validate_basic` checks that rows num = cols num
        self.row_roots
            .len()
            .try_into()
            // On validated DAH this never happens
            .expect("len is bigger than u16::MAX")
    }

    /// Get the [`RowProof`] for given rows.
    pub fn row_proof(&self, rows: RangeInclusive<u16>) -> Result<RowProof> {
        let all_roots: Vec<_> = self
            .row_roots
            .iter()
            .chain(self.column_roots.iter())
            .map(|root| root.to_array())
            .collect();

        let start_row = *rows.start();
        let end_row = *rows.end();
        let mut proofs = Vec::with_capacity(rows.len());
        let mut row_roots = Vec::with_capacity(rows.len());

        for idx in rows {
            proofs.push(MerkleProof::new(idx as usize, &all_roots)?.0);
            let row = self
                .row_root(idx)
                .ok_or(Error::IndexOutOfRange(idx as usize, self.row_roots.len()))?;
            row_roots.push(row);
        }

        Ok(RowProof {
            proofs,
            row_roots,
            start_row,
            end_row,
        })
    }
}

#[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
#[wasm_bindgen]
impl DataAvailabilityHeader {
    /// Merkle roots of the [`ExtendedDataSquare`] rows.
    #[wasm_bindgen(js_name = rowRoots)]
    pub fn js_row_roots(&self) -> Result<js_sys::Array, serde_wasm_bindgen::Error> {
        self.row_roots()
            .iter()
            .map(|h| serde_wasm_bindgen::to_value(&h))
            .collect()
    }

    /// Merkle roots of the [`ExtendedDataSquare`] columns.
    #[wasm_bindgen(js_name = columnRoots)]
    pub fn js_column_roots(&self) -> Result<js_sys::Array, serde_wasm_bindgen::Error> {
        self.column_roots()
            .iter()
            .map(|h| serde_wasm_bindgen::to_value(&h))
            .collect()
    }

    /// Get a root of the row with the given index.
    #[wasm_bindgen(js_name = rowRoot)]
    pub fn js_row_root(&self, row: u16) -> Result<JsValue, serde_wasm_bindgen::Error> {
        serde_wasm_bindgen::to_value(&self.row_root(row))
    }

    /// Get the a root of the column with the given index.
    #[wasm_bindgen(js_name = columnRoot)]
    pub fn js_column_root(&self, column: u16) -> Result<JsValue, serde_wasm_bindgen::Error> {
        serde_wasm_bindgen::to_value(&self.column_root(column))
    }

    /// Compute the combined hash of all rows and columns.
    ///
    /// This is the data commitment for the block.
    #[wasm_bindgen(js_name = hash)]
    pub fn js_hash(&self) -> Result<JsValue, serde_wasm_bindgen::Error> {
        serde_wasm_bindgen::to_value(&self.hash())
    }

    /// Get the size of the [`ExtendedDataSquare`] for which this header was built.
    #[wasm_bindgen(js_name = squareWidth)]
    pub fn js_square_width(&self) -> u16 {
        self.square_width()
    }
}

impl Protobuf<RawDataAvailabilityHeader> for DataAvailabilityHeader {}

impl TryFrom<RawDataAvailabilityHeader> for DataAvailabilityHeader {
    type Error = Error;

    fn try_from(value: RawDataAvailabilityHeader) -> Result<Self, Self::Error> {
        Ok(DataAvailabilityHeader {
            row_roots: value
                .row_roots
                .iter()
                .map(|bytes| NamespacedHash::from_raw(bytes))
                .collect::<Result<Vec<_>>>()?,
            column_roots: value
                .column_roots
                .iter()
                .map(|bytes| NamespacedHash::from_raw(bytes))
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl From<DataAvailabilityHeader> for RawDataAvailabilityHeader {
    fn from(value: DataAvailabilityHeader) -> RawDataAvailabilityHeader {
        RawDataAvailabilityHeader {
            row_roots: value.row_roots.iter().map(|hash| hash.to_vec()).collect(),
            column_roots: value
                .column_roots
                .iter()
                .map(|hash| hash.to_vec())
                .collect(),
        }
    }
}

impl ValidateBasic for DataAvailabilityHeader {
    fn validate_basic(&self) -> Result<(), ValidationError> {
        if self.column_roots.len() != self.row_roots.len() {
            bail_validation!(
                "column_roots len ({}) != row_roots len ({})",
                self.column_roots.len(),
                self.row_roots.len(),
            )
        }

        if self.row_roots.len() < MIN_EXTENDED_SQUARE_WIDTH {
            bail_validation!(
                "row_roots len ({}) < minimum ({})",
                self.row_roots.len(),
                MIN_EXTENDED_SQUARE_WIDTH,
            )
        }

        Ok(())
    }
}

/// A proof of inclusion of a range of row roots in a [`DataAvailabilityHeader`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawRowProof", into = "RawRowProof")]
pub struct RowProof {
    row_roots: Vec<NamespacedHash>,
    proofs: Vec<MerkleProof>,
    start_row: u16,
    end_row: u16,
}

impl RowProof {
    /// Get the list of row roots this proof proves.
    pub fn row_roots(&self) -> &[NamespacedHash] {
        &self.row_roots
    }

    /// Get the inclusion proofs of each row root in the data availability header.
    pub fn proofs(&self) -> &[MerkleProof] {
        &self.proofs
    }

    /// Verify the proof against the hash of [`DataAvailabilityHeader`], proving
    /// the inclusion of rows.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    ///  - the proof is malformed. Number of proofs, row roots and the span between starting and ending row need to match.
    ///  - the verification of any inner merkle proof fails
    ///
    /// # Example
    ///
    /// ```
    /// # use celestia_types::ExtendedHeader;
    /// # fn get_extended_header() -> ExtendedHeader {
    /// #   let s = include_str!("../test_data/chain1/extended_header_block_1.json");
    /// #   serde_json::from_str(s).unwrap()
    /// # }
    /// let eh = get_extended_header();
    /// let dah = eh.dah;
    ///
    /// let proof = dah.row_proof(0..=1).unwrap();
    ///
    /// assert!(proof.verify(dah.hash()).is_ok());
    /// ```
    pub fn verify(&self, root: Hash) -> Result<()> {
        if self.row_roots.len() != self.proofs.len() {
            bail_verification!("invalid row proof: row_roots.len() != proofs.len()");
        }

        if self.end_row < self.start_row {
            bail_verification!(
                "start_row ({}) > end_row ({})",
                self.start_row,
                self.end_row
            );
        }

        let length = self.end_row - self.start_row + 1;
        if length as usize != self.proofs.len() {
            bail_verification!(
                "length based on start_row and end_row ({}) != length of proofs ({})",
                length,
                self.proofs.len()
            );
        }

        let Hash::Sha256(root) = root else {
            bail_verification!("empty hash");
        };

        for (row_root, proof) in self.row_roots.iter().zip(self.proofs.iter()) {
            proof.verify(row_root.to_array(), root)?;
        }

        Ok(())
    }
}

impl Protobuf<RawRowProof> for RowProof {}

impl TryFrom<RawRowProof> for RowProof {
    type Error = Error;

    fn try_from(value: RawRowProof) -> Result<Self> {
        Ok(Self {
            row_roots: value
                .row_roots
                .into_iter()
                .map(|hash| NamespacedHash::from_raw(&hash))
                .collect::<Result<_>>()?,
            proofs: value
                .proofs
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_>>()?,
            start_row: value
                .start_row
                .try_into()
                .map_err(|_| validation_error!("start_row ({}) exceeds u16", value.start_row))?,
            end_row: value
                .end_row
                .try_into()
                .map_err(|_| validation_error!("end_row ({}) exceeds u16", value.end_row))?,
        })
    }
}

impl From<RowProof> for RawRowProof {
    fn from(value: RowProof) -> Self {
        Self {
            row_roots: value
                .row_roots
                .into_iter()
                .map(|hash| hash.to_vec())
                .collect(),
            proofs: value.proofs.into_iter().map(Into::into).collect(),
            start_row: value.start_row as u32,
            end_row: value.end_row as u32,
            root: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::nmt::Namespace;

    use super::*;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as test;

    fn sample_dah() -> DataAvailabilityHeader {
        serde_json::from_str(r#"{
          "row_roots": [
            "//////////////////////////////////////7//////////////////////////////////////huZWOTTDmD36N1F75A9BshxNlRasCnNpQiWqIhdVHcU",
            "/////////////////////////////////////////////////////////////////////////////5iieeroHBMfF+sER3JpvROIeEJZjbY+TRE0ntADQLL3"
          ],
          "column_roots": [
            "//////////////////////////////////////7//////////////////////////////////////huZWOTTDmD36N1F75A9BshxNlRasCnNpQiWqIhdVHcU",
            "/////////////////////////////////////////////////////////////////////////////5iieeroHBMfF+sER3JpvROIeEJZjbY+TRE0ntADQLL3"
          ]
        }"#).unwrap()
    }

    #[test]
    fn validate_correct() {
        let dah = sample_dah();

        dah.validate_basic().unwrap();
    }

    #[test]
    fn validate_rows_and_cols_len_mismatch() {
        let mut dah = sample_dah();
        dah.row_roots.pop();

        dah.validate_basic().unwrap_err();
    }

    #[test]
    fn validate_too_little_square() {
        let mut dah = sample_dah();
        dah.row_roots = dah
            .row_roots
            .into_iter()
            .cycle()
            .take(MIN_EXTENDED_SQUARE_WIDTH)
            .collect();
        dah.column_roots = dah
            .column_roots
            .into_iter()
            .cycle()
            .take(MIN_EXTENDED_SQUARE_WIDTH)
            .collect();

        dah.validate_basic().unwrap();

        dah.row_roots.pop();
        dah.column_roots.pop();

        dah.validate_basic().unwrap_err();
    }

    #[test]
    fn row_proof_serde() {
        let raw_row_proof = r#"
          {
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
        "#;
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

        let row_proof: RowProof = serde_json::from_str(raw_row_proof).unwrap();
        let dah: DataAvailabilityHeader = serde_json::from_str(raw_dah).unwrap();

        row_proof.verify(dah.hash()).unwrap();
    }

    #[test]
    fn row_proof_verify_correct() {
        for square_width in [2, 4, 8, 16] {
            let dah = random_dah(square_width);
            let dah_root = dah.hash();

            for start_row in 0..dah.square_width() - 1 {
                for end_row in start_row..dah.square_width() {
                    let proof = dah.row_proof(start_row..=end_row).unwrap();

                    proof.verify(dah_root).unwrap()
                }
            }
        }
    }

    #[test]
    fn row_proof_verify_malformed() {
        let dah = random_dah(16);
        let dah_root = dah.hash();

        let valid_proof = dah.row_proof(0..=1).unwrap();

        // start_row > end_row
        #[allow(clippy::reversed_empty_ranges)]
        let proof = dah.row_proof(1..=0).unwrap();
        proof.verify(dah_root).unwrap_err();

        // length incorrect based on start and end
        let mut proof = valid_proof.clone();
        proof.end_row = 2;
        proof.verify(dah_root).unwrap_err();

        // incorrect amount of proofs
        let mut proof = valid_proof.clone();
        proof.proofs.push(proof.proofs[0].clone());
        proof.verify(dah_root).unwrap_err();

        // incorrect amount of roots
        let mut proof = valid_proof.clone();
        proof.row_roots.pop();
        proof.verify(dah_root).unwrap_err();

        // wrong proof order
        let mut proof = valid_proof.clone();
        proof.row_roots = proof.row_roots.into_iter().rev().collect();
        proof.verify(dah_root).unwrap_err();
    }

    #[test]
    fn test_serialize_row_proof_binary() {
        let dah = random_dah(16);
        let proof = dah.row_proof(0..=1).unwrap();
        let serialized = postcard::to_allocvec(&proof).unwrap();
        let deserialized: RowProof = postcard::from_bytes(&serialized).unwrap();
        assert_eq!(proof, deserialized);
    }

    fn random_dah(square_width: u16) -> DataAvailabilityHeader {
        let namespaces: Vec<_> = (0..square_width)
            .map(|n| Namespace::new_v0(&[n as u8]).unwrap())
            .collect();
        let (row_roots, col_roots): (Vec<_>, Vec<_>) = namespaces
            .iter()
            .map(|&ns| {
                let row = NamespacedHash::new(*ns, *ns, rand::random());
                let col = NamespacedHash::new(
                    **namespaces.first().unwrap(),
                    **namespaces.last().unwrap(),
                    rand::random(),
                );
                (row, col)
            })
            .unzip();

        DataAvailabilityHeader::new(row_roots, col_roots).unwrap()
    }
}
