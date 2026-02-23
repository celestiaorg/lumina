//! Types related to EDS.

use std::cmp::Ordering;
use std::fmt::Display;

use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};

use crate::consts::appconsts::{AppVersion, SHARE_SIZE};
use crate::consts::data_availability_header::{
    MIN_EXTENDED_SQUARE_WIDTH, max_extended_square_width,
};
use crate::nmt::{NS_SIZE, Namespace, Nmt, NmtExt};
use crate::row_namespace_data::{RowNamespaceData, RowNamespaceDataId};
use crate::{DataAvailabilityHeader, Error, InfoByte, Result, Share, bail_validation};

/// Number of bytes needed to represent [`EdsId`] in `multihash`.
pub const EDS_ID_SIZE: usize = 8;

/// Represents an EDS of a specific Height
///
/// # Note
///
/// EdsId is excluded from shwap operating on top of bitswap due to possible
/// EDS sizes exceeding bitswap block limits.
#[derive(Debug, PartialEq, Clone, Copy)]
pub struct EdsId {
    height: u64,
}

/// Represents either column or row of the [`ExtendedDataSquare`].
///
/// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum AxisType {
    /// A row of the data square.
    Row = 0,
    /// A column of the data square.
    Col,
}

impl TryFrom<i32> for AxisType {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(AxisType::Row),
            1 => Ok(AxisType::Col),
            n => Err(Error::InvalidAxis(n)),
        }
    }
}

impl Display for AxisType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AxisType::Row => write!(f, "Row"),
            AxisType::Col => write!(f, "Column"),
        }
    }
}

/// The data matrix in Celestia blocks extended with parity data.
///
/// It is created by a fixed size chunks of data, called [`Share`]s.
///
/// Each share is a cell of the [`ExtendedDataSquare`].
///
/// # Structure
///
/// The [`ExtendedDataSquare`] consists of four quadrants. The first
/// quadrant (upper-left) is the original data submitted to the network,
/// referred to as `OriginalDataSquare`. The other three quadrants are
/// the parity data encoded row-wise or column-wise using Reed-Solomon
/// `codec` specified in `EDS`.
///
/// The below diagram shows how the `EDS` is constructed. First, the 2nd
/// and 3rd quadrants are created by computing Reed-Solomon parity data
/// of the original data square, row-wise for 2nd and column-wise for
/// 3rd quadrant. Then, the 4th quadrant is computed either row-wise
/// from 3rd or column-wise from 2nd quadrant.
///
/// ```text
///  ---------------------------
/// |             |             |
/// |           --|->           |
/// |      1    --|->    2      |
/// |           --|->           |
/// |    | | |    |             |
///  -------------+-------------
/// |    v v v    |             |
/// |           --|->           |
/// |      3    --|->    4      |
/// |           --|->           |
/// |             |             |
///  ---------------------------
/// ```
///
/// # Data availability
///
/// The [`DataAvailabilityHeader`] is created by computing [`Nmt`] merkle
/// roots of each row and column of [`ExtendedDataSquare`].
/// By putting those together there are some key
/// properties those have in terms of data availability.
///
/// Thanks to the parity data, to make original data unrecoverable, a malicious
/// actor would need to hide more than a half of the data from each row and column.
/// If we take `k` as the width of the `OriginalDataSquare`, then the attacker
/// needs to hide more than `(k + 1)^2` shares from the [`ExtendedDataSquare`].
/// For the `EDS` with a width of 4, the attacker needs to hide more than 50% of
/// all the shares and that value approaches 25% as the square grows.
///
/// This allows for really efficient data sampling, as the sampling node can reach
/// very high confidence that whole data is available by taking only a few samples.
///
/// # Example
///
/// This example shows rebuilding the merkle trees for each row of the EDS and compares
/// them with the root hashes stored in data availability header.
///
/// ```no_run
/// use celestia_types::Share;
/// # use celestia_types::{ExtendedDataSquare, ExtendedHeader};
/// # fn get_header(_: usize) -> ExtendedHeader {
/// #     unimplemented!()
/// # }
/// # fn get_eds(_: usize) -> ExtendedDataSquare {
/// #     unimplemented!()
/// # }
///
/// let block_height = 15;
/// let header = get_header(block_height);
/// let eds = get_eds(block_height);
/// let width = header.dah.square_width();
///
/// // for each row of the data square, build an nmt
/// for row in 0..eds.square_width() {
///     // check if the root corresponds to the one from the dah
///     let root = eds.row_nmt(row).unwrap().root();
///     assert_eq!(root, header.dah.row_root(row).unwrap());
/// }
/// ```
///
/// [`Nmt`]: crate::nmt::Nmt
/// [`Share`]: crate::share::Share
/// [`DataAvailabilityHeader`]: crate::DataAvailabilityHeader
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(into = "RawExtendedDataSquare")]
pub struct ExtendedDataSquare {
    /// The raw data of the EDS.
    data_square: Vec<Share>,
    /// The codec used to encode parity shares.
    codec: String,
    /// pre-calculated square length
    square_width: u16,
}

impl ExtendedDataSquare {
    /// Create a new EDS out of the provided shares.
    ///
    /// Shares should be provided in a row-major order, i.e. first shares of the first row,
    /// then of the second row and so on.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    ///  - shares are of sizes different than [`SHARE_SIZE`]
    ///  - amount of shares doesn't allow for forming a square
    ///  - width of the square is smaller than [`MIN_EXTENDED_SQUARE_WIDTH`]
    ///  - width of the square is bigger than [`max_extended_square_width`]
    ///  - width of the square isn't a power of 2
    ///  - namespaces of shares aren't in non-decreasing order row and column wise
    pub fn new(shares: Vec<Vec<u8>>, codec: String, app_version: AppVersion) -> Result<Self> {
        const MIN_SHARES: usize = MIN_EXTENDED_SQUARE_WIDTH * MIN_EXTENDED_SQUARE_WIDTH;

        let max_extended_square_width = max_extended_square_width(app_version);
        let max_shares = max_extended_square_width * max_extended_square_width;

        if shares.len() < MIN_SHARES {
            bail_validation!(
                "shares len ({}) < MIN_SHARES ({})",
                shares.len(),
                MIN_SHARES
            );
        }
        if shares.len() > max_shares {
            bail_validation!(
                "shares len ({}) > max shares ({})",
                shares.len(),
                max_shares
            );
        }

        let square_width = f64::sqrt(shares.len() as f64) as usize;

        if square_width * square_width != shares.len() {
            return Err(Error::EdsInvalidDimentions);
        }

        let square_width = u16::try_from(square_width).map_err(|_| Error::EdsInvalidDimentions)?;

        // must be a power of 2
        if square_width.count_ones() != 1 {
            return Err(Error::EdsInvalidDimentions);
        }

        let check_share = |row, col, prev_ns: Option<Namespace>, axis| -> Result<Share> {
            let idx = flatten_index(row, col, square_width);
            let share = if is_ods_square(row, col, square_width) {
                Share::from_raw(&shares[idx])?
            } else {
                Share::parity(&shares[idx])?
            };
            share.validate(app_version)?;

            if prev_ns.is_some_and(|prev_ns| share.namespace() < prev_ns) {
                let axis_idx = match axis {
                    AxisType::Row => row,
                    AxisType::Col => col,
                };
                bail_validation!("Shares of {axis} {axis_idx} are not sorted by their namespace");
            }

            Ok(share)
        };

        // Validate that namespaces of each column are sorted
        for col in 0..square_width {
            let mut prev_ns = None;

            for row in 0..square_width {
                let share = check_share(row, col, prev_ns, AxisType::Col)?;
                prev_ns = Some(share.namespace());
            }
        }
        // Validate that namespaces of each row are sorted collecting data square
        let mut data_square = Vec::with_capacity(shares.len());
        for row in 0..square_width {
            let mut prev_ns = None;

            for col in 0..square_width {
                let share = check_share(row, col, prev_ns, AxisType::Row)?;
                prev_ns = Some(share.namespace());
                data_square.push(share);
            }
        }

        let eds = ExtendedDataSquare {
            data_square,
            codec,
            square_width,
        };

        Ok(eds)
    }

    /// Creates an `ExtendedDataSquare` from [`RawExtendedDataSquare`] and an [`AppVersion`].
    pub fn from_raw(raw_eds: RawExtendedDataSquare, app_version: AppVersion) -> Result<Self> {
        ExtendedDataSquare::new(raw_eds.data_square, raw_eds.codec, app_version)
    }

    /// Crate a new EDS that represents an empty block
    pub fn empty() -> ExtendedDataSquare {
        // ODS in this case it is just one tail padded share.
        let ods = vec![
            [
                Namespace::TAIL_PADDING.as_bytes(),
                &[InfoByte::new(0, true).unwrap().as_u8()],
                &[0; SHARE_SIZE - NS_SIZE - 1],
            ]
            .concat(),
        ];

        // App version doesn't matter in this case because an empty ODS is constructed
        // with the minimum size allowed shares, which is the same in any version.
        ExtendedDataSquare::from_ods(ods, AppVersion::V1).expect("invalid EDS")
    }

    /// Create a new EDS out of the provided original data square shares.
    ///
    /// This method is similar to the [`ExtendedDataSquare::new`] but parity data
    /// will be encoded automatically using the [`leopard_codec`]
    ///
    /// Shares should be provided in a row-major order.
    ///
    /// # Errors
    ///
    /// The same errors as in [`ExtendedDataSquare::new`] applies. The constrain
    /// will be checked after the parity data is generated.
    ///
    /// Additionally, this function will propagate any error from encoding parity data.
    pub fn from_ods(
        mut ods_shares: Vec<Vec<u8>>,
        app_version: AppVersion,
    ) -> Result<ExtendedDataSquare> {
        let ods_width = f64::sqrt(ods_shares.len() as f64) as usize;
        // this couldn't be detected later in `new()`
        if ods_width * ods_width != ods_shares.len() {
            return Err(Error::EdsInvalidDimentions);
        }

        let eds_width = ods_width * 2;
        let mut eds_shares = Vec::with_capacity(eds_width * eds_width);
        // take rows of ods and interleave them with parity shares
        for _ in 0..ods_width {
            eds_shares.extend(ods_shares.drain(..ods_width));
            for _ in 0..ods_width {
                eds_shares.push(vec![0; SHARE_SIZE]);
            }
        }
        // fill bottom half of the square with parity data
        eds_shares.resize(eds_width * eds_width, vec![0; SHARE_SIZE]);

        // 2nd quadrant - encode parity of rows of 1st quadrant
        for row in eds_shares.chunks_mut(eds_width).take(ods_width) {
            leopard_codec::encode(row, ods_width)?;
        }
        // 3rd quadrant - encode parity of columns of 1st quadrant
        for col in 0..ods_width {
            let mut col: Vec<_> = eds_shares.iter_mut().skip(col).step_by(eds_width).collect();
            leopard_codec::encode(&mut col, ods_width)?;
        }
        // 4th quadrant - encode parity of rows of 3rd quadrant
        for row in eds_shares.chunks_mut(eds_width).skip(ods_width) {
            leopard_codec::encode(row, ods_width)?;
        }

        ExtendedDataSquare::new(eds_shares, "Leopard".to_string(), app_version)
    }

    /// The raw data of the EDS.
    pub fn data_square(&self) -> &[Share] {
        &self.data_square
    }

    /// The codec used to encode parity shares.
    pub fn codec(&self) -> &str {
        self.codec.as_str()
    }

    /// Returns the share of the provided coordinates.
    pub fn share(&self, row: u16, column: u16) -> Result<&Share> {
        let index = usize::from(row) * usize::from(self.square_width) + usize::from(column);

        self.data_square
            .get(index)
            .ok_or(Error::EdsIndexOutOfRange(row, column))
    }

    /// Returns the mutable share of the provided coordinates.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn share_mut(&mut self, row: u16, column: u16) -> Result<&mut Share> {
        let index = flatten_index(row, column, self.square_width);

        self.data_square
            .get_mut(index)
            .ok_or(Error::EdsIndexOutOfRange(row, column))
    }

    /// Returns the shares of a row.
    pub fn row(&self, index: u16) -> Result<Vec<Share>> {
        self.axis(AxisType::Row, index)
    }

    /// Returns the [`Nmt`] of a row.
    pub fn row_nmt(&self, index: u16) -> Result<Nmt> {
        self.axis_nmt(AxisType::Row, index)
    }

    /// Returns the shares of a column.
    pub fn column(&self, index: u16) -> Result<Vec<Share>> {
        self.axis(AxisType::Col, index)
    }

    /// Returns the [`Nmt`] of a column.
    pub fn column_nmt(&self, index: u16) -> Result<Nmt> {
        self.axis_nmt(AxisType::Col, index)
    }

    /// Returns the shares of column or row.
    pub fn axis(&self, axis: AxisType, index: u16) -> Result<Vec<Share>> {
        (0..self.square_width)
            .map(|i| {
                let (row, col) = match axis {
                    AxisType::Row => (index, i),
                    AxisType::Col => (i, index),
                };

                self.share(row, col).map(ToOwned::to_owned)
            })
            .collect()
    }

    /// Returns the [`Nmt`] of column or row.
    pub fn axis_nmt(&self, axis: AxisType, index: u16) -> Result<Nmt> {
        let mut tree = Nmt::default();

        for i in 0..self.square_width {
            let (row, col) = match axis {
                AxisType::Row => (index, i),
                AxisType::Col => (i, index),
            };

            let share = self.share(row, col)?;

            tree.push_leaf(share.as_ref(), *share.namespace())
                .map_err(Error::Nmt)?;
        }

        Ok(tree)
    }

    /// Get EDS square length.
    pub fn square_width(&self) -> u16 {
        self.square_width
    }

    /// Return all the shares that belong to the provided namespace in the EDS.
    /// Results are returned as a list of rows of shares with the inclusion proof.
    pub fn get_namespace_data(
        &self,
        namespace: Namespace,
        dah: &DataAvailabilityHeader,
        height: u64,
    ) -> Result<Vec<(RowNamespaceDataId, RowNamespaceData)>> {
        let mut rows = Vec::new();

        for row in 0..self.square_width {
            if !dah.row_contains(row, namespace)? {
                continue;
            }

            let mut shares = Vec::with_capacity(self.square_width.into());

            for col in 0..self.square_width {
                let share = self.share(row, col)?;

                // Shares in each row of EDS are sorted by namespace, so we
                // can stop search the row if we reach to a bigger namespace.
                match share.namespace().cmp(&namespace) {
                    Ordering::Less => {}
                    Ordering::Equal => shares.push(share.clone()),
                    Ordering::Greater => break,
                }
            }

            let proof = self.row_nmt(row)?.get_namespace_proof(*namespace);
            let id = RowNamespaceDataId::new(namespace, row, height)?;
            let data = RowNamespaceData {
                proof: proof.into(),
                shares,
            };

            rows.push((id, data))
        }

        Ok(rows)
    }
}

impl EdsId {
    /// Create a new eds id.
    pub fn new(height: u64) -> Result<Self> {
        if height == 0 {
            return Err(Error::ZeroBlockHeight);
        }

        Ok(EdsId { height })
    }

    /// A height of the block which contains the data.
    pub fn block_height(&self) -> u64 {
        self.height
    }

    /// Encode eds id into the byte representation.
    pub fn encode(&self, bytes: &mut BytesMut) {
        bytes.reserve(EDS_ID_SIZE);
        bytes.put_u64(self.height);
    }

    /// Decode eds id from the byte representation.
    pub fn decode(mut buffer: &[u8]) -> Result<Self> {
        if buffer.len() != EDS_ID_SIZE {
            return Err(Error::InvalidLength(buffer.len(), EDS_ID_SIZE));
        }

        let height = buffer.get_u64();

        EdsId::new(height)
    }
}

/// Raw representation of [`ExtendedDataSquare`].
#[derive(Serialize, Deserialize, Clone)]
pub struct RawExtendedDataSquare {
    /// The raw data of the EDS.
    #[serde(with = "tendermint_proto::serializers::bytes::vec_base64string")]
    pub data_square: Vec<Vec<u8>>,
    /// The codec used to encode parity shares.
    pub codec: String,
}

impl From<ExtendedDataSquare> for RawExtendedDataSquare {
    fn from(eds: ExtendedDataSquare) -> RawExtendedDataSquare {
        RawExtendedDataSquare {
            data_square: eds
                .data_square
                .into_iter()
                .map(|shr| shr.to_vec())
                .collect(),
            codec: eds.codec,
        }
    }
}

/// Returns true if and only if the provided coordinates belongs to Original Data Square
/// (i.e. first quadrant of Extended Data Square).
pub(crate) fn is_ods_square(row: u16, column: u16, square_width: u16) -> bool {
    let ods_width = square_width / 2;
    row < ods_width && column < ods_width
}

fn flatten_index(row: u16, col: u16, square_width: u16) -> usize {
    usize::from(row) * usize::from(square_width) + usize::from(col)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consts::appconsts;
    use crate::test_utils::generate_eds;
    use crate::{Blob, ExtendedHeader};

    #[test]
    fn axis_type_serialization() {
        assert_eq!(AxisType::Row as u8, 0);
        assert_eq!(AxisType::Col as u8, 1);
    }

    #[test]
    fn axis_type_deserialization() {
        assert_eq!(AxisType::try_from(0).unwrap(), AxisType::Row);
        assert_eq!(AxisType::try_from(1).unwrap(), AxisType::Col);

        let axis_type_err = AxisType::try_from(2).unwrap_err();
        assert!(matches!(axis_type_err, Error::InvalidAxis(2)));
        let axis_type_err = AxisType::try_from(99).unwrap_err();
        assert!(matches!(axis_type_err, Error::InvalidAxis(99)));
    }

    #[test]
    fn get_namespaced_data() {
        let eds_json = include_str!("../test_data/shwap_samples/eds.json");
        let raw_eds: RawExtendedDataSquare = serde_json::from_str(eds_json).unwrap();
        let eds = ExtendedDataSquare::from_raw(raw_eds, AppVersion::V2).unwrap();

        let dah_json = include_str!("../test_data/shwap_samples/dah.json");
        let dah: DataAvailabilityHeader = serde_json::from_str(dah_json).unwrap();

        let height = 45577;

        let rows = eds
            .get_namespace_data(Namespace::new_v0(&[1, 170]).unwrap(), &dah, height)
            .unwrap();
        assert_eq!(rows.len(), 1);
        let (id, row) = &rows[0];
        row.verify(*id, &dah).unwrap();
        assert_eq!(row.shares.len(), 2);

        let rows = eds
            .get_namespace_data(Namespace::new_v0(&[1, 187]).unwrap(), &dah, height)
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1.shares.len(), 1);
        assert_eq!(rows[1].1.shares.len(), 4);
        for (id, row) in rows {
            row.verify(id, &dah).unwrap();
        }
    }

    #[test]
    fn nmt_roots() {
        let eds_json = include_str!("../test_data/shwap_samples/eds.json");
        let raw_eds: RawExtendedDataSquare = serde_json::from_str(eds_json).unwrap();
        let eds = ExtendedDataSquare::from_raw(raw_eds, AppVersion::V2).unwrap();

        let dah_json = include_str!("../test_data/shwap_samples/dah.json");
        let dah: DataAvailabilityHeader = serde_json::from_str(dah_json).unwrap();

        assert_eq!(dah.row_roots().len(), usize::from(eds.square_width()));
        assert_eq!(dah.column_roots().len(), usize::from(eds.square_width()));

        for (i, root) in dah.row_roots().iter().enumerate() {
            let mut tree = eds.row_nmt(i as u16).unwrap();
            assert_eq!(*root, tree.root());

            let mut tree = eds.axis_nmt(AxisType::Row, i as u16).unwrap();
            assert_eq!(*root, tree.root());
        }

        for (i, root) in dah.column_roots().iter().enumerate() {
            let mut tree = eds.column_nmt(i as u16).unwrap();
            assert_eq!(*root, tree.root());

            let mut tree = eds.axis_nmt(AxisType::Col, i as u16).unwrap();
            assert_eq!(*root, tree.root());
        }
    }

    #[test]
    fn ods_square() {
        assert!(is_ods_square(0, 0, 4));
        assert!(is_ods_square(0, 1, 4));
        assert!(is_ods_square(1, 0, 4));
        assert!(is_ods_square(1, 1, 4));

        assert!(!is_ods_square(0, 2, 4));
        assert!(!is_ods_square(0, 3, 4));
        assert!(!is_ods_square(1, 2, 4));
        assert!(!is_ods_square(1, 3, 4));

        assert!(!is_ods_square(2, 0, 4));
        assert!(!is_ods_square(2, 1, 4));
        assert!(!is_ods_square(3, 0, 4));
        assert!(!is_ods_square(3, 1, 4));

        assert!(!is_ods_square(2, 2, 4));
        assert!(!is_ods_square(2, 3, 4));
        assert!(!is_ods_square(3, 2, 4));
        assert!(!is_ods_square(3, 3, 4));
    }

    #[test]
    fn get_row_and_col() {
        let raw_share = |x, y| {
            [
                Namespace::new_v0(&[x, y]).unwrap().as_bytes(),
                &[0u8; SHARE_SIZE - NS_SIZE][..],
            ]
            .concat()
        };
        let share = |x, y, parity: bool| {
            if !parity {
                Share::from_raw(&raw_share(x, y)).unwrap()
            } else {
                Share::parity(&raw_share(x, y)).unwrap()
            }
        };

        #[rustfmt::skip]
        let shares = vec![
            raw_share(0, 0), raw_share(0, 1), raw_share(0, 2), raw_share(0, 3),
            raw_share(1, 0), raw_share(1, 1), raw_share(1, 2), raw_share(1, 3),
            raw_share(2, 0), raw_share(2, 1), raw_share(2, 2), raw_share(2, 3),
            raw_share(3, 0), raw_share(3, 1), raw_share(3, 2), raw_share(3, 3),
        ];

        let eds = ExtendedDataSquare::new(shares, "fake".to_string(), AppVersion::V2).unwrap();

        assert_eq!(
            eds.row(0).unwrap(),
            vec![
                share(0, 0, false),
                share(0, 1, false),
                share(0, 2, true),
                share(0, 3, true)
            ],
        );
        assert_eq!(
            eds.row(1).unwrap(),
            vec![
                share(1, 0, false),
                share(1, 1, false),
                share(1, 2, true),
                share(1, 3, true)
            ],
        );
        assert_eq!(
            eds.row(2).unwrap(),
            vec![
                share(2, 0, true),
                share(2, 1, true),
                share(2, 2, true),
                share(2, 3, true)
            ],
        );
        assert_eq!(
            eds.row(3).unwrap(),
            vec![
                share(3, 0, true),
                share(3, 1, true),
                share(3, 2, true),
                share(3, 3, true)
            ],
        );

        assert_eq!(
            eds.axis(AxisType::Row, 0).unwrap(),
            vec![
                share(0, 0, false),
                share(0, 1, false),
                share(0, 2, true),
                share(0, 3, true)
            ],
        );
        assert_eq!(
            eds.axis(AxisType::Row, 1).unwrap(),
            vec![
                share(1, 0, false),
                share(1, 1, false),
                share(1, 2, true),
                share(1, 3, true)
            ],
        );
        assert_eq!(
            eds.axis(AxisType::Row, 2).unwrap(),
            vec![
                share(2, 0, true),
                share(2, 1, true),
                share(2, 2, true),
                share(2, 3, true)
            ],
        );
        assert_eq!(
            eds.axis(AxisType::Row, 3).unwrap(),
            vec![
                share(3, 0, true),
                share(3, 1, true),
                share(3, 2, true),
                share(3, 3, true)
            ],
        );

        assert_eq!(
            eds.column(0).unwrap(),
            vec![
                share(0, 0, false),
                share(1, 0, false),
                share(2, 0, true),
                share(3, 0, true)
            ],
        );
        assert_eq!(
            eds.column(1).unwrap(),
            vec![
                share(0, 1, false),
                share(1, 1, false),
                share(2, 1, true),
                share(3, 1, true)
            ],
        );
        assert_eq!(
            eds.column(2).unwrap(),
            vec![
                share(0, 2, true),
                share(1, 2, true),
                share(2, 2, true),
                share(3, 2, true)
            ],
        );
        assert_eq!(
            eds.column(3).unwrap(),
            vec![
                share(0, 3, true),
                share(1, 3, true),
                share(2, 3, true),
                share(3, 3, true)
            ],
        );

        assert_eq!(
            eds.axis(AxisType::Col, 0).unwrap(),
            vec![
                share(0, 0, false),
                share(1, 0, false),
                share(2, 0, true),
                share(3, 0, true)
            ],
        );
        assert_eq!(
            eds.axis(AxisType::Col, 1).unwrap(),
            vec![
                share(0, 1, false),
                share(1, 1, false),
                share(2, 1, true),
                share(3, 1, true)
            ],
        );
        assert_eq!(
            eds.axis(AxisType::Col, 2).unwrap(),
            vec![
                share(0, 2, true),
                share(1, 2, true),
                share(2, 2, true),
                share(3, 2, true)
            ],
        );
        assert_eq!(
            eds.axis(AxisType::Col, 3).unwrap(),
            vec![
                share(0, 3, true),
                share(1, 3, true),
                share(2, 3, true),
                share(3, 3, true)
            ],
        );
    }

    #[test]
    fn validation() {
        ExtendedDataSquare::new(vec![], "fake".to_string(), AppVersion::V2).unwrap_err();
        ExtendedDataSquare::new(vec![vec![]], "fake".to_string(), AppVersion::V2).unwrap_err();
        ExtendedDataSquare::new(vec![vec![]; 4], "fake".to_string(), AppVersion::V2).unwrap_err();

        ExtendedDataSquare::new(
            vec![vec![0u8; SHARE_SIZE]; 4],
            "fake".to_string(),
            AppVersion::V2,
        )
        .unwrap();
        ExtendedDataSquare::new(
            vec![vec![0u8; SHARE_SIZE]; 6],
            "fake".to_string(),
            AppVersion::V2,
        )
        .unwrap_err();
        ExtendedDataSquare::new(
            vec![vec![0u8; SHARE_SIZE]; 16],
            "fake".to_string(),
            AppVersion::V2,
        )
        .unwrap();

        let share = |n| {
            [
                Namespace::new_v0(&[n]).unwrap().as_bytes(),
                &[0u8; SHARE_SIZE - NS_SIZE][..],
            ]
            .concat()
        };

        ExtendedDataSquare::from_ods(
            vec![
                // row 0
                share(0), // ODS
            ],
            AppVersion::V2,
        )
        .unwrap();

        ExtendedDataSquare::from_ods(
            vec![
                // row 0
                share(1),
                share(2),
                // row 1
                share(1),
                share(3),
            ],
            AppVersion::V2,
        )
        .unwrap();

        ExtendedDataSquare::from_ods(
            vec![
                // row 0
                share(1),
                share(2),
                // row 1
                share(1),
                share(1), // error: smaller namespace in 2nd column
            ],
            AppVersion::V2,
        )
        .unwrap_err();

        ExtendedDataSquare::from_ods(
            vec![
                // row 0
                share(1),
                share(1),
                // row 1
                share(2),
                share(1), // error: smaller namespace in 2nd row
            ],
            AppVersion::V2,
        )
        .unwrap_err();

        // not a power of 2
        ExtendedDataSquare::new(vec![share(1); 6 * 6], "fake".to_string(), AppVersion::V2)
            .unwrap_err();

        // too big
        // we need to go to the next power of 2 or we just hit other checks
        let square_width = max_extended_square_width(AppVersion::V2) * 2;
        ExtendedDataSquare::new(
            vec![share(1); square_width.pow(2)],
            "fake".to_string(),
            AppVersion::V2,
        )
        .unwrap_err();

        // unsupported share version
        let mut shr = share(1);
        shr[NS_SIZE] = InfoByte::new(appconsts::SHARE_VERSION_ONE, false)
            .unwrap()
            .as_u8();
        let shares = vec![shr, share(1), share(1), share(1)];
        ExtendedDataSquare::new(shares.clone(), "fake".to_string(), AppVersion::V2).unwrap_err();
        ExtendedDataSquare::new(shares, "fake".to_string(), AppVersion::V3).unwrap();
    }

    #[test]
    fn empty_block_eds() {
        let s = include_str!("../test_data/chain1/extended_header_block_1.json");
        let genesis: ExtendedHeader = serde_json::from_str(s).unwrap();

        let eds = ExtendedDataSquare::empty();
        let dah = DataAvailabilityHeader::from_eds(&eds);
        assert_eq!(dah, genesis.dah);
    }

    #[test]
    fn reconstruct_all() {
        let eds = generate_eds(8 << (rand::random::<usize>() % 6), AppVersion::V2);

        let blobs = Blob::reconstruct_all(eds.data_square(), AppVersion::V2).unwrap();
        // first ods row has PFB's, one blob occupies 2 rows, and rest rows have 1 blob each
        let expected = eds.square_width() as usize / 2 - 2;
        assert_eq!(blobs.len(), expected);
    }
}
