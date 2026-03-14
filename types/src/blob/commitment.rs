use std::io::Cursor;
use std::num::NonZeroU64;

use base64::prelude::*;
use bytes::{Buf, BufMut, BytesMut};
use celestia_proto::serializers::cow_str::CowStr;
use nmt_rs::NamespaceMerkleHasher;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tendermint::crypto::sha256::HASH_SIZE;
use tendermint::{crypto, merkle};
#[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
use wasm_bindgen::prelude::*;

use crate::consts::appconsts;
use crate::nmt::{Namespace, NamespacedHashExt, NamespacedSha2Hasher, Nmt, RawNamespacedHash};
use crate::state::{AccAddress, AddressTrait};
use crate::{Error, Result};
use crate::{InfoByte, Share};

/// A merkle hash used to identify the [`Blob`]s data.
///
/// In Celestia network, the transaction which pays for the blob's inclusion
/// is separated from the data itself. The reason for that is to allow verifying
/// the blockchain's state without the need to pull the actual data which got stored.
/// To achieve that, the [`MsgPayForBlobs`] transaction only includes the [`Commitment`]s
/// of the blobs it is paying for, not the data itself.
///
/// The algorithm of computing the [`Commitment`] of the [`Blob`]'s [`Share`]s is
/// designed in a way to allow easy and cheap proving of the [`Share`]s inclusion in the
/// block. It is computed as a [`merkle hash`] of all the [`Nmt`] subtree roots created from
/// the blob shares included in the [`ExtendedDataSquare`] rows. Assuming the `s1` and `s2`
/// are the only shares of some blob posted to the celestia, they'll result in a single subtree
/// root as shown below:
///
/// ```text
/// NMT:           row root
///                /     \
///              o   subtree root
///             / \      / \
///           _________________
/// EDS row: | s | s | s1 | s2 |
/// ```
///
/// Using subtree roots as a base for [`Commitment`] computation allows for much smaller
/// inclusion proofs than when the [`Share`]s would be used directly, but it imposes some
/// constraints on how the [`Blob`]s can be placed in the [`ExtendedDataSquare`]. You can
/// read more about that in the [`share commitment rules`].
///
/// [`Blob`]: crate::Blob
/// [`Share`]: crate::share::Share
/// [`MsgPayForBlobs`]: celestia_proto::celestia::blob::v1::MsgPayForBlobs
/// [`merkle hash`]: tendermint::merkle::simple_hash_from_byte_vectors
/// [`Nmt`]: crate::nmt::Nmt
/// [`ExtendedDataSquare`]: crate::ExtendedDataSquare
/// [`share commitment rules`]: https://github.com/celestiaorg/celestia-app/blob/main/specs/src/specs/data_square_layout.md#blob-share-commitment-rules
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(
    all(feature = "wasm-bindgen", target_arch = "wasm32"),
    wasm_bindgen(inspectable)
)]
pub struct Commitment {
    /// Hash of the commitment
    hash: merkle::Hash,
}

impl Commitment {
    /// Create a new commitment with hash
    pub fn new(hash: merkle::Hash) -> Self {
        Commitment { hash }
    }

    /// Generate the share commitment from the given blob data.
    pub fn from_blob(
        namespace: Namespace,
        blob_data: &[u8],
        share_version: u8,
        signer: Option<&AccAddress>,
    ) -> Result<Commitment> {
        validate_blob(share_version, signer.is_some())?;
        let shares = split_blob_to_shares(namespace, share_version, blob_data, signer)?;
        Self::from_shares(namespace, &shares)
    }

    /// Generate the commitment from the given shares.
    pub fn from_shares(namespace: Namespace, mut shares: &[Share]) -> Result<Commitment> {
        // the commitment is the root of a merkle mountain range with max tree size
        // determined by the number of roots required to create a share commitment
        // over that blob. The size of the tree is only increased if the number of
        // subtree roots surpasses a constant threshold.
        let subtree_root_threshold = appconsts::SUBTREE_ROOT_THRESHOLD;
        let subtree_width = subtree_width(shares.len() as u64, subtree_root_threshold);
        let tree_sizes = merkle_mountain_range_sizes(shares.len() as u64, subtree_width);

        let mut leaf_sets: Vec<&[_]> = Vec::with_capacity(tree_sizes.len());

        for size in tree_sizes {
            let (leafs, rest) = shares.split_at(size as usize);
            leaf_sets.push(leafs);
            shares = rest;
        }

        // create the commitments by pushing each leaf set onto an nmt
        let mut subtree_roots: Vec<RawNamespacedHash> = Vec::with_capacity(leaf_sets.len());
        for leaf_set in leaf_sets {
            // create the nmt
            let mut tree = Nmt::with_hasher(NamespacedSha2Hasher::with_ignore_max_ns(true));
            for leaf_share in leaf_set {
                tree.push_leaf(leaf_share.as_ref(), namespace.into())
                    .map_err(Error::Nmt)?;
            }
            // add the root
            subtree_roots.push(tree.root().to_array());
        }

        let hash = merkle::simple_hash_from_byte_vectors::<crypto::default::Sha256>(&subtree_roots);

        Ok(Commitment { hash })
    }

    /// Hash of the commitment
    pub fn hash(&self) -> &merkle::Hash {
        &self.hash
    }
}

#[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
#[wasm_bindgen]
impl Commitment {
    /// Hash of the commitment
    #[wasm_bindgen(js_name = hash)]
    pub fn js_hash(&self) -> Vec<u8> {
        self.hash.to_vec()
    }
}

impl From<Commitment> for merkle::Hash {
    fn from(commitment: Commitment) -> Self {
        commitment.hash
    }
}

impl Serialize for Commitment {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = BASE64_STANDARD.encode(self.hash);
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for Commitment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // base64 needs more buffer size than the final output
        let mut buf = [0u8; HASH_SIZE * 2];

        let s = CowStr::deserialize(deserializer)?;

        let len = BASE64_STANDARD
            .decode_slice(s, &mut buf)
            .map_err(|e| serde::de::Error::custom(e.to_string()))?;

        let hash: merkle::Hash = buf[..len]
            .try_into()
            .map_err(|_| serde::de::Error::custom("commitment is not a size of a sha256"))?;

        Ok(Commitment { hash })
    }
}

/// Check if the combination of share_version and signer is valid, and return appropriate error
/// otherwise
pub(crate) fn validate_blob(share_version: u8, has_signer: bool) -> Result<()> {
    if ![appconsts::SHARE_VERSION_ZERO, appconsts::SHARE_VERSION_ONE].contains(&share_version) {
        return Err(Error::UnsupportedShareVersion(share_version));
    }
    if share_version == appconsts::SHARE_VERSION_ZERO && has_signer {
        return Err(Error::SignerNotSupported);
    }
    if share_version == appconsts::SHARE_VERSION_ONE && !has_signer {
        return Err(Error::MissingSigner);
    }
    Ok(())
}

/// Splits blob's data to the sequence of shares
pub(crate) fn split_blob_to_shares(
    namespace: Namespace,
    share_version: u8,
    blob_data: &[u8],
    signer: Option<&AccAddress>,
) -> Result<Vec<Share>> {
    let mut shares = Vec::new();
    let mut cursor = Cursor::new(blob_data);

    while cursor.has_remaining() {
        let share = build_sparse_share(namespace, share_version, signer, &mut cursor)?;
        shares.push(share);
    }
    Ok(shares)
}

/// Build a sparse share from a cursor over data
fn build_sparse_share(
    namespace: Namespace,
    share_version: u8,
    signer: Option<&AccAddress>,
    data: &mut Cursor<impl AsRef<[u8]>>,
) -> Result<Share> {
    let is_first_share = data.position() == 0;
    let data_len = cursor_inner_length(data);
    let mut bytes = BytesMut::with_capacity(appconsts::SHARE_SIZE);

    // Write the namespace
    bytes.put_slice(namespace.as_bytes());
    // Write the info byte
    let info_byte = InfoByte::new(share_version, is_first_share)?;
    bytes.put_u8(info_byte.as_u8());

    // If this share is first in the sequence, write the bytes len of the sequence
    if is_first_share {
        let data_len = data_len
            .try_into()
            .map_err(|_| Error::ShareSequenceLenExceeded(data_len))?;
        bytes.put_u32(data_len);
        // additionally, if share_version is 1, put the signer after sequence len
        if share_version == appconsts::SHARE_VERSION_ONE {
            let signer = signer.as_ref().ok_or(Error::MissingSigner)?;
            bytes.put_slice(signer.as_bytes());
        }
    }

    // Calculate amount of bytes to read
    let current_size = bytes.len();
    let available_space = appconsts::SHARE_SIZE - current_size;
    let read_amount = available_space.min(data.remaining());

    // Resize to share size with 0 padding
    bytes.resize(appconsts::SHARE_SIZE, 0);
    // Read the share data
    data.copy_to_slice(&mut bytes[current_size..current_size + read_amount]);

    Share::from_raw(&bytes)
}

fn cursor_inner_length(cursor: &Cursor<impl AsRef<[u8]>>) -> usize {
    cursor.get_ref().as_ref().len()
}

/// merkle_mountain_range_sizes returns the sizes (number of leaf nodes) of the
/// trees in a merkle mountain range constructed for a given total_size and
/// max_tree_size.
///
/// https://docs.grin.mw/wiki/chain-state/merkle-mountain-range/
/// https://github.com/opentimestamps/opentimestamps-server/blob/master/doc/merkle-mountain-range.md
fn merkle_mountain_range_sizes(mut total_size: u64, max_tree_size: u64) -> Vec<u64> {
    let mut tree_sizes = Vec::new();

    while total_size != 0 {
        if total_size >= max_tree_size {
            tree_sizes.push(max_tree_size);
            total_size -= max_tree_size;
        } else {
            let tree_size = round_down_to_power_of_2(
                // unwrap is safe as total_size can't be zero there
                total_size.try_into().unwrap(),
            )
            .expect("Failed to find next power of 2");
            tree_sizes.push(tree_size);
            total_size -= tree_size;
        }
    }

    tree_sizes
}

/// blob_min_square_size returns the minimum square size that can contain share_count
/// number of shares.
fn blob_min_square_size(share_count: u64) -> u64 {
    round_up_to_power_of_2((share_count as f64).sqrt().ceil() as u64)
        .expect("Failed to find minimum blob square size")
}

/// subtree_width determines the maximum number of leaves per subtree in the share
/// commitment over a given blob. The input should be the total number of shares
/// used by that blob. The reasoning behind this algorithm is discussed in depth
/// in ADR013
/// (celestia-app/docs/architecture/adr-013-non-interative-default-rules-for-zero-padding).
fn subtree_width(share_count: u64, subtree_root_threshold: u64) -> u64 {
    // per ADR013, we use a predetermined threshold to determine width of sub
    // trees used to create share commitments
    let mut s = share_count / subtree_root_threshold;

    // round up if the width is not an exact multiple of the threshold
    if !share_count.is_multiple_of(subtree_root_threshold) {
        s += 1;
    }

    // use a power of two equal to or larger than the multiple of the subtree
    // root threshold
    s = round_up_to_power_of_2(s).expect("Failed to find next power of 2");

    // use the minimum of the subtree width and the min square size, this
    // gurarantees that a valid value is returned
    // return min(s, BlobMinSquareSize(shareCount))
    s.min(blob_min_square_size(share_count))
}

/// round_up_to_power_of_2 returns the next power of two that is strictly greater than input.
fn round_up_to_power_of_2(x: u64) -> Option<u64> {
    let mut po2 = 1;

    loop {
        if po2 >= x {
            return Some(po2);
        }
        if let Some(next_po2) = po2.checked_shl(1) {
            po2 = next_po2;
        } else {
            return None;
        }
    }
}

/// round_down_to_power_of_2 returns the next power of two less than or equal to input.
fn round_down_to_power_of_2(x: NonZeroU64) -> Option<u64> {
    let x: u64 = x.into();

    match round_up_to_power_of_2(x) {
        Some(po2) if po2 == x => Some(x),
        Some(po2) => Some(po2 / 2),
        _ => None,
    }
}

#[cfg(feature = "uniffi")]
mod uniffi_types {
    use super::{Commitment as RustCommitment, HASH_SIZE};
    use uniffi::Record;

    use crate::error::UniffiConversionError;

    #[derive(Record)]
    pub struct Commitment {
        pub sha_hash: Vec<u8>,
    }

    impl From<RustCommitment> for Commitment {
        fn from(value: RustCommitment) -> Self {
            Commitment {
                sha_hash: value.hash.to_vec(),
            }
        }
    }

    impl TryFrom<Commitment> for RustCommitment {
        type Error = UniffiConversionError;

        fn try_from(value: Commitment) -> Result<Self, Self::Error> {
            let hash: [u8; HASH_SIZE] = value
                .sha_hash
                .try_into()
                .map_err(|_| UniffiConversionError::InvalidCommitmentLength)?;
            Ok(RustCommitment { hash })
        }
    }

    uniffi::custom_type!(RustCommitment, Commitment);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as test;

    #[test]
    fn test_single_sparse_share_v0() {
        let namespace = Namespace::new(0, &[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]).unwrap();
        let data = vec![1, 2, 3, 4, 5, 6, 7];
        let mut cursor = Cursor::new(&data);

        let share = build_sparse_share(namespace, 0, None, &mut cursor).unwrap();

        // check cursor
        assert!(!cursor.has_remaining());

        // check namespace
        let (share_ns, share_data) = share.as_ref().split_at(appconsts::NAMESPACE_SIZE);
        assert_eq!(share_ns, namespace.as_bytes());

        // check data
        let expected_share_start: &[u8] = &[
            1, // info byte
            0, 0, 0, 7, // sequence len
            1, 2, 3, 4, 5, 6, 7, // data
        ];
        let (share_data, share_padding) = share_data.split_at(expected_share_start.len());
        assert_eq!(share_data, expected_share_start);

        // check padding
        assert_eq!(
            share_padding,
            &vec![0; appconsts::FIRST_SPARSE_SHARE_CONTENT_SIZE - data.len()],
        );
    }

    #[test]
    fn test_single_sparse_share_v1() {
        let namespace = Namespace::new(0, &[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]).unwrap();
        let data = vec![1, 2, 3, 4, 5, 6, 7];
        let signer = AccAddress::from([9; appconsts::SIGNER_SIZE]);
        let mut cursor = Cursor::new(&data);

        let share = build_sparse_share(namespace, 1, Some(&signer), &mut cursor).unwrap();

        // check cursor
        assert!(!cursor.has_remaining());

        // check namespace
        let (share_ns, share_data) = share.as_ref().split_at(appconsts::NAMESPACE_SIZE);
        assert_eq!(share_ns, namespace.as_bytes());

        // check data
        let expected_share_start: &[u8] = &[
            0b00000011, // info byte
            0, 0, 0, 7, // sequence len
            9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, // signer
            1, 2, 3, 4, 5, 6, 7, // data
        ];
        let (share_data, share_padding) = share_data.split_at(expected_share_start.len());
        assert_eq!(share_data, expected_share_start);

        // check padding
        assert_eq!(
            share_padding,
            &vec![
                0;
                appconsts::FIRST_SPARSE_SHARE_CONTENT_SIZE - appconsts::SIGNER_SIZE - data.len()
            ],
        );
    }

    #[test]
    fn test_sparse_share_v0_with_continuation() {
        let namespace = Namespace::new(0, &[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]).unwrap();
        let continuation_len = 7;
        let data = vec![7; appconsts::FIRST_SPARSE_SHARE_CONTENT_SIZE + continuation_len];
        let mut cursor = Cursor::new(&data);

        let first_share = build_sparse_share(namespace, 0, None, &mut cursor).unwrap();

        // check cursor
        assert_eq!(
            cursor.position(),
            appconsts::FIRST_SPARSE_SHARE_CONTENT_SIZE as u64
        );

        // check namespace
        let (share_ns, share_data) = first_share.as_ref().split_at(appconsts::NAMESPACE_SIZE);
        assert_eq!(share_ns, namespace.as_bytes());

        // check info byte
        let (share_info_byte, share_data) = share_data.split_at(appconsts::SHARE_INFO_BYTES);
        assert_eq!(share_info_byte, &[1]);

        // check sequence len
        let (share_seq_len, share_data) = share_data.split_at(appconsts::SEQUENCE_LEN_BYTES);
        assert_eq!(share_seq_len, &(data.len() as u32).to_be_bytes());

        // check data
        assert_eq!(
            share_data,
            &vec![7; appconsts::FIRST_SPARSE_SHARE_CONTENT_SIZE]
        );

        // Continuation share
        let continuation_share = build_sparse_share(namespace, 0, None, &mut cursor).unwrap();

        // check cursor
        assert!(!cursor.has_remaining());

        // check namespace
        let (share_ns, share_data) = continuation_share
            .as_ref()
            .split_at(appconsts::NAMESPACE_SIZE);
        assert_eq!(share_ns, namespace.as_bytes());

        // check data
        let expected_continuation_share_start: &[u8] = &[
            0, // info byte
            7, 7, 7, 7, 7, 7, 7, // data
        ];
        let (share_data, share_padding) =
            share_data.split_at(expected_continuation_share_start.len());
        assert_eq!(share_data, expected_continuation_share_start);

        // check padding
        assert_eq!(
            share_padding,
            &vec![0; appconsts::CONTINUATION_SPARSE_SHARE_CONTENT_SIZE - continuation_len],
        );
    }

    #[test]
    fn test_sparse_share_v0_empty_data() {
        let namespace = Namespace::new(0, &[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]).unwrap();
        let data = vec![];
        let mut cursor = Cursor::new(&data);
        let expected_share_start: &[u8] = &[
            1, // info byte
            0, 0, 0, 0, // sequence len
        ];

        let share = build_sparse_share(namespace, 0, None, &mut cursor).unwrap();

        // check cursor
        assert!(!cursor.has_remaining());

        // check namespace
        let (share_ns, share_data) = share.as_ref().split_at(appconsts::NAMESPACE_SIZE);
        assert_eq!(share_ns, namespace.as_bytes());

        // check data
        let (share_start, share_data) = share_data.split_at(expected_share_start.len());
        assert_eq!(share_start, expected_share_start);

        // check padding
        assert_eq!(
            share_data,
            &vec![0; appconsts::FIRST_SPARSE_SHARE_CONTENT_SIZE],
        );
    }

    #[test]
    fn merkle_mountain_ranges() {
        struct TestCase {
            total_size: u64,
            square_size: u64,
            expected: Vec<u64>,
        }

        let test_cases = [
            TestCase {
                total_size: 11,
                square_size: 4,
                expected: vec![4, 4, 2, 1],
            },
            TestCase {
                total_size: 2,
                square_size: 64,
                expected: vec![2],
            },
            TestCase {
                total_size: 64,
                square_size: 8,
                expected: vec![8, 8, 8, 8, 8, 8, 8, 8],
            },
            // Height
            // 3              x                               x
            //              /    \                         /    \
            //             /      \                       /      \
            //            /        \                     /        \
            //           /          \                   /          \
            // 2        x            x                 x            x
            //        /   \        /   \             /   \        /   \
            // 1     x     x      x     x           x     x      x     x         x
            //      / \   / \    / \   / \         / \   / \    / \   / \      /   \
            // 0   0   1 2   3  4   5 6   7       8   9 10  11 12 13 14  15   16   17    18
            TestCase {
                total_size: 19,
                square_size: 8,
                expected: vec![8, 8, 2, 1],
            },
        ];
        for case in test_cases {
            assert_eq!(
                merkle_mountain_range_sizes(case.total_size, case.square_size),
                case.expected,
            );
        }
    }

    #[test]
    fn blob_validation() {
        let share_signer_required = appconsts::SHARE_VERSION_ONE;
        let share_signer_forbidden = appconsts::SHARE_VERSION_ZERO;
        let share_version_unsupported = appconsts::MAX_SHARE_VERSION;

        let with_signer = true;
        let no_signer = false;

        // all good - no signer
        validate_blob(share_signer_forbidden, no_signer).unwrap();

        // all good - with signer
        validate_blob(share_signer_required, with_signer).unwrap();

        // unsupported share version
        validate_blob(share_version_unsupported, no_signer).unwrap_err();

        // no signer when required
        validate_blob(share_signer_required, no_signer).unwrap_err();

        // with signer when forbidden
        validate_blob(share_signer_forbidden, with_signer).unwrap_err();
    }
}
