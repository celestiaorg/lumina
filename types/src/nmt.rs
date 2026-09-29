//! Namespaces and Namespaced Merkle Tree.
//!
//! [`Namespace`] is a key to your data in Celestia. Whenever you publish blobs
//! to the Celestia network, you have to assign them to a namespace. You can
//! later use that namespace to acquire the data back from the network.
//!
//! All the data in each block is ordered by its [`Namespace`]. Things like
//! transactions or trailing bytes have its own namespaces too, but those are reserved
//! for block producers and cannot be utilized when submitting data.
//!
//! The fact that data in Celestia blocks is ordered allows for various optimizations
//! when proving data inclusion or absence. There is a namespace-aware implementation
//! of merkle trees, called 'Namespaced Merkle Tree', shortened as [`Nmt`].
//!
//! The generic implementation of this merkle tree lives in [`nmt-rs`]. This module is
//! a wrapper around that with Celestia specific defaults and functionalities.
//!
//! [`nmt-rs`]: https://github.com/sovereign-labs/nmt-rs

#[cfg(feature = "uniffi")]
use crate::error::UniffiError;
use base64::prelude::*;
use blockstore::block::CidError;
use celestia_proto::serializers::cow_str::CowStr;
use cid::CidGeneric;
use multihash::Multihash;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tendermint::hash::SHA256_HASH_SIZE;
#[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
use wasm_bindgen::prelude::*;

mod namespace_proof;
mod namespaced_hash;
mod namespaced_merkle_tree;

pub use self::namespace_proof::{EMPTY_LEAVES, NamespaceProof};
pub use self::namespaced_hash::{
    HASH_SIZE, NAMESPACED_HASH_SIZE, NamespacedHashExt, RawNamespacedHash,
};
pub use self::namespaced_merkle_tree::{MerkleHash, NamespacedSha2Hasher, Nmt, NmtExt};

use crate::{Error, Result};

pub use nmt_rs::NamespaceMerkleHasher;

/// Namespace version size in bytes.
pub const NS_VER_SIZE: usize = 1;
/// Namespace id size in bytes.
pub const NS_ID_SIZE: usize = 28;
/// Namespace size in bytes.
pub const NS_SIZE: usize = NS_VER_SIZE + NS_ID_SIZE;
/// Size of the user-defined namespace suffix in bytes. For namespaces in version `0`.
pub const NS_ID_V0_SIZE: usize = 10;

/// The code of the [`Nmt`] hashing algorithm in `multihash`.
pub const NMT_MULTIHASH_CODE: u64 = 0x7700;
/// The id of codec used for the [`Nmt`] in `Cid`s.
pub const NMT_CODEC: u64 = 0x7701;
/// The size of the [`Nmt`] hash in `multihash`.
pub const NMT_ID_SIZE: usize = 2 * NS_SIZE + SHA256_HASH_SIZE;

/// Hash that carries info about minimum and maximum [`Namespace`] of the hashed data.
///
/// [`NamespacedHash`] can represent leaves or internal nodes of the [`Nmt`].
///
/// If it is a hash of a leaf, then it will be constructed like so:
///
/// `leaf_namespace | leaf_namespace | Sha256(0x00 | leaf_namespace | data)`
///
/// If it is a hash of its children nodes, then it will be constructed like so:
///
/// `min_namespace | max_namespace | Sha256(0x01 | left | right)`
pub type NamespacedHash = nmt_rs::NamespacedHash<NS_SIZE>;
/// Proof of some statement about namesapced merkle tree. It can either prove presence
/// of a set of shares or absence of a particular namespace.
pub type Proof = nmt_rs::simple_merkle::proof::Proof<NamespacedSha2Hasher>;

/// Namespace of the data published to the celestia network.
///
/// The [`Namespace`] is a single byte defining the version
/// followed by 28 bytes specifying concrete ID of the namespace.
///
/// Currently there are two versions of namespaces:
///
///  - version `0` - the one allowing for the custom namespace ids. It requires an id to start
///    with 18 `0x00` bytes followed by a user specified suffix (except reserved ones, see below).
///  - version `255` - for secondary reserved namespaces. It requires an id to start with 27
///    `0xff` bytes followed by a single byte indicating the id.
///
/// Some namespaces are reserved for the block creation purposes and cannot be used
/// when submitting the blobs to celestia. Those fall into one of the two categories:
///
///  - primary reserved namespaces - those use version `0` and have id lower or equal to `0xff`
///    so they are always placed in blocks before user-submitted data.
///  - secondary reserved namespaces - those use version `0xff` so they are always placed after
///    user-submitted data.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(
    all(feature = "wasm-bindgen", target_arch = "wasm32"),
    wasm_bindgen(inspectable)
)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
pub struct Namespace(nmt_rs::NamespaceId<NS_SIZE>);

impl Namespace {
    /// Primary reserved [`Namespace`] for the compact [`Share`]s with [`cosmos SDK`] transactions.
    ///
    /// [`Share`]: crate::share::Share
    /// [`cosmos SDK`]: https://docs.cosmos.network/v0.46/core/transactions.html
    pub const TRANSACTION: Namespace = Namespace::const_v0([0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);

    /// Primary reserved [`Namespace`] for the compact [`Share`]s with [`MsgPayForBlobs`] transactions.
    ///
    /// [`Share`]: crate::share::Share
    /// [`MsgPayForBlobs`]: celestia_proto::celestia::blob::v1::MsgPayForBlobs
    pub const PAY_FOR_BLOB: Namespace = Namespace::const_v0([0, 0, 0, 0, 0, 0, 0, 0, 0, 4]);

    /// Primary reserved [`Namespace`] for the compact [`Share`]s with [`MsgPayForFibre`] transactions.
    ///
    /// This value is already covered by the primary reserved range checked by [`Namespace::is_reserved`].
    ///
    /// [`Share`]: crate::share::Share
    /// [`MsgPayForFibre`]: celestia_proto::celestia::fibre::v1::MsgPayForFibre
    pub const PAY_FOR_FIBRE: Namespace = Namespace::const_v0([0, 0, 0, 0, 0, 0, 0, 0, 0, 5]);

    /// Primary reserved [`Namespace`] for the [`Share`]s used for padding.
    ///
    /// [`Share`]s with this namespace are inserted after other shares from primary reserved namespace
    /// so that user-defined namespaces are correctly aligned in [`ExtendedDataSquare`]
    ///
    /// [`Share`]: crate::share::Share
    /// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
    pub const PRIMARY_RESERVED_PADDING: Namespace = Namespace::MAX_PRIMARY_RESERVED;

    /// Maximal primary reserved [`Namespace`].
    ///
    /// Used to indicate the end of the primary reserved group.
    ///
    /// [`PRIMARY_RESERVED_PADDING`]: Namespace::PRIMARY_RESERVED_PADDING
    pub const MAX_PRIMARY_RESERVED: Namespace =
        Namespace::const_v0([0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff]);

    /// Minimal secondary reserved [`Namespace`].
    ///
    /// Used to indicate the beginning of the secondary reserved group.
    pub const MIN_SECONDARY_RESERVED: Namespace = Namespace::const_v255(0);

    /// Secondary reserved [`Namespace`] used for padding after the blobs.
    ///
    /// It is used to fill up the `original data square` after all user-submitted
    /// blobs before the parity data is generated for the [`ExtendedDataSquare`].
    ///
    /// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
    pub const TAIL_PADDING: Namespace = Namespace::const_v255(0xfe);

    /// The [`Namespace`] for `parity shares`.
    ///
    /// It is the namespace with which all the `parity shares` from
    /// [`ExtendedDataSquare`] are inserted to the [`Nmt`] when computing
    /// merkle roots.
    ///
    /// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
    pub const PARITY_SHARE: Namespace = Namespace::const_v255(0xff);

    /// Create a new [`Namespace`] from the raw bytes.
    ///
    /// # Errors
    ///
    /// This function will return an error if the slice length is different than
    /// [`NS_SIZE`] or if the namespace is invalid. If you are constructing the
    /// version `0` namespace, check [`new_v0`] for more details.
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::nmt::{Namespace, NS_SIZE};
    /// let raw = [0; NS_SIZE];
    /// let namespace = Namespace::from_raw(&raw).unwrap();
    /// ```
    ///
    /// [`new_v0`]: crate::nmt::Namespace::new_v0
    pub fn from_raw(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != NS_SIZE {
            return Err(Error::InvalidNamespaceSize);
        }

        Namespace::new(bytes[0], &bytes[1..])
    }

    /// Create a new [`Namespace`] from the version and id.
    ///
    /// # Errors
    ///
    /// This function will return an error if provided namespace version isn't supported
    /// or if the namespace is invalid. If you are constructing the
    /// version `0` namespace, check [`new_v0`] for more details.
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::nmt::Namespace;
    /// let namespace = Namespace::new(0, b"custom-ns").unwrap();
    /// ```
    ///
    /// [`new_v0`]: crate::nmt::Namespace::new_v0
    pub fn new(version: u8, id: &[u8]) -> Result<Self> {
        match version {
            0 => Self::new_v0(id),
            255 => Self::new_v255(id),
            n => Err(Error::UnsupportedNamespaceVersion(n)),
        }
    }

    /// Create a new [`Namespace`] version `0` with given id.
    ///
    /// The `id` must be either:
    ///  - a 28 byte slice specifying full id
    ///  - a 10 or less byte slice specifying user-defined suffix
    ///
    /// [`Namespace`]s in version 0 must have id's prefixed with 18 `0x00` bytes.
    ///
    /// # Errors
    ///
    /// This function will return an error if the provided id has incorrect length
    /// or if the `id` has 28 bytes and have doesn't have mandatory 18x`0x00` bytes prefix
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::nmt::Namespace;
    ///
    /// // construct using 28 byte slice
    /// let id = [0u8; 28];
    /// let namespace = Namespace::new_v0(&id).unwrap();
    ///
    /// // construct using <=10 byte suffix
    /// let namespace = Namespace::new_v0(b"any-suffix").unwrap();
    ///
    /// // invalid
    /// let mut id = [0u8; 28];
    /// id[4] = 1;
    /// let namespace = Namespace::new_v0(&id).unwrap_err();
    /// ```
    pub fn new_v0(id: &[u8]) -> Result<Self> {
        let id_pos = match id.len() {
            // Allow 28 bytes len
            NS_ID_SIZE => NS_ID_SIZE - NS_ID_V0_SIZE,
            // Allow 10 bytes len or less
            n if n <= NS_ID_V0_SIZE => 0,
            // Anything else is an error
            _ => return Err(Error::InvalidNamespaceSize),
        };

        let prefix = &id[..id_pos];
        let id = &id[id_pos..];

        // Validate that prefix is all zeros
        if prefix.iter().any(|&x| x != 0) {
            return Err(Error::InvalidNamespaceV0);
        }

        let mut bytes = [0u8; NS_SIZE];
        bytes[NS_SIZE - id.len()..].copy_from_slice(id);

        Ok(Namespace(nmt_rs::NamespaceId(bytes)))
    }

    pub(crate) const fn new_unchecked(bytes: [u8; NS_SIZE]) -> Self {
        Namespace(nmt_rs::NamespaceId(bytes))
    }

    /// Create a new [`Namespace`] version `0` with a given id.
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::nmt::Namespace;
    ///
    /// const NAMESPACE: Namespace = Namespace::const_v0([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    /// ```
    pub const fn const_v0(id: [u8; NS_ID_V0_SIZE]) -> Self {
        let mut bytes = [0u8; NS_SIZE];
        let start = NS_SIZE - NS_ID_V0_SIZE;

        bytes[start] = id[0];
        bytes[start + 1] = id[1];
        bytes[start + 2] = id[2];
        bytes[start + 3] = id[3];
        bytes[start + 4] = id[4];
        bytes[start + 5] = id[5];
        bytes[start + 6] = id[6];
        bytes[start + 7] = id[7];
        bytes[start + 8] = id[8];
        bytes[start + 9] = id[9];

        Namespace(nmt_rs::NamespaceId(bytes))
    }

    /// Create a new [`Namespace`] version `255` with a given id.
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::nmt::Namespace;
    ///
    /// const NAMESPACE: Namespace = Namespace::const_v255(0xff);
    ///
    /// assert_eq!(NAMESPACE, Namespace::PARITY_SHARE);
    /// ```
    pub const fn const_v255(id: u8) -> Self {
        let mut bytes = [255u8; NS_SIZE];
        bytes[NS_ID_SIZE] = id;
        Namespace(nmt_rs::NamespaceId(bytes))
    }

    /// Create a new [`Namespace`] version `255` with a given id.
    ///
    /// [`Namespace`]s with version `255` must have ids prefixed with 27 `0xff` bytes followed by a
    /// single byte with an actual id.
    ///
    /// # Errors
    ///
    /// This function will return an error, if the provided id has incorrect length
    /// or if the `id` prefix is incorrect.
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::nmt::Namespace;
    ///
    /// // construct using 28 byte slice
    /// let id = [255u8; 28];
    /// let namespace = Namespace::new_v255(&id).unwrap();
    /// ```
    pub fn new_v255(id: &[u8]) -> Result<Self> {
        if id.len() != NS_ID_SIZE {
            return Err(Error::InvalidNamespaceSize);
        }

        // safe after the length check
        let (id, prefix) = id.split_last().unwrap();

        if prefix.iter().all(|&x| x == 0xff) {
            Ok(Namespace::const_v255(*id))
        } else {
            Err(Error::InvalidNamespaceV255)
        }
    }

    /// Converts the [`Namespace`] to a byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0.0
    }

    /// Returns the first byte indicating the version of the [`Namespace`].
    pub fn version(&self) -> u8 {
        self.as_bytes()[0]
    }

    /// Returns the trailing 28 bytes indicating the id of the [`Namespace`].
    pub fn id(&self) -> &[u8] {
        &self.as_bytes()[1..]
    }

    /// Returns the 10 bytes user-defined suffix of the [`Namespace`] if it's a version 0.
    pub fn id_v0(&self) -> Option<&[u8]> {
        if self.version() == 0 {
            let start = NS_SIZE - NS_ID_V0_SIZE;
            Some(&self.as_bytes()[start..])
        } else {
            None
        }
    }

    /// Returns true if the namespace is reserved for special purposes.
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::nmt::Namespace;
    ///
    /// let ns = Namespace::new_v0(b"lumina").unwrap();
    /// assert!(!ns.is_reserved());
    ///
    /// assert!(Namespace::PAY_FOR_BLOB.is_reserved());
    /// assert!(Namespace::PARITY_SHARE.is_reserved());
    /// ```
    pub fn is_reserved(&self) -> bool {
        *self <= Namespace::MAX_PRIMARY_RESERVED || *self >= Namespace::MIN_SECONDARY_RESERVED
    }
}

#[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
#[wasm_bindgen]
impl Namespace {
    /// Namespace size in bytes.
    #[wasm_bindgen(js_name = NS_SIZE, getter)]
    pub fn js_ns_size() -> usize {
        NS_SIZE
    }

    /// Primary reserved [`Namespace`] for the compact `Share`s with `cosmos SDK` transactions.
    #[wasm_bindgen(js_name = TRANSACTION, getter)]
    pub fn js_transaction() -> Namespace {
        Namespace::TRANSACTION
    }

    /// Primary reserved [`Namespace`] for the compact Shares with MsgPayForBlobs transactions.
    #[wasm_bindgen(js_name = PAY_FOR_BLOB, getter)]
    pub fn js_pay_for_blob() -> Namespace {
        Namespace::PAY_FOR_BLOB
    }

    /// Primary reserved [`Namespace`] for the compact Shares with MsgPayForFibre transactions.
    #[wasm_bindgen(js_name = PAY_FOR_FIBRE, getter)]
    pub fn js_pay_for_fibre() -> Namespace {
        Namespace::PAY_FOR_FIBRE
    }

    /// Primary reserved [`Namespace`] for the `Share`s used for padding.
    ///
    /// `Share`s with this namespace are inserted after other shares from primary reserved namespace
    /// so that user-defined namespaces are correctly aligned in `ExtendedDataSquare`
    #[wasm_bindgen(js_name = PRIMARY_RESERVED_PADDING, getter)]
    pub fn js_primary_reserved_padding() -> Namespace {
        Namespace::PRIMARY_RESERVED_PADDING
    }

    /// Maximal primary reserved [`Namespace`].
    ///
    /// Used to indicate the end of the primary reserved group.
    #[wasm_bindgen(js_name = MAX_PRIMARY_RESERVED, getter)]
    pub fn js_max_primary_reserved() -> Namespace {
        Namespace::MAX_PRIMARY_RESERVED
    }

    /// Minimal secondary reserved [`Namespace`].
    ///
    /// Used to indicate the beginning of the secondary reserved group.
    #[wasm_bindgen(js_name = MIN_SECONDARY_RESERVED, getter)]
    pub fn js_min_secondary_reserved() -> Namespace {
        Namespace::MIN_SECONDARY_RESERVED
    }

    /// Secondary reserved [`Namespace`] used for padding after the blobs.
    ///
    /// It is used to fill up the `original data square` after all user-submitted
    /// blobs before the parity data is generated for the `ExtendedDataSquare`.
    #[wasm_bindgen(js_name = TAIL_PADDING, getter)]
    pub fn js_tail_padding() -> Namespace {
        Namespace::TAIL_PADDING
    }

    /// The [`Namespace`] for `parity shares`.
    ///
    /// It is the namespace with which all the `parity shares` from
    /// `ExtendedDataSquare` are inserted to the `Nmt` when computing
    /// merkle roots.
    #[wasm_bindgen(js_name = PARITY_SHARE, getter)]
    pub fn js_parity_share() -> Namespace {
        Namespace::PARITY_SHARE
    }

    /// Create a new [`Namespace`] version `0` with given id.
    ///
    /// Check [`Namespace::new_v0`] for more details.
    ///
    /// [`Namespace::new_v0`]: https://docs.rs/celestia-types/latest/celestia_types/nmt/struct.Namespace.html#method.new_v0
    #[wasm_bindgen(js_name = newV0)]
    pub fn js_new_v0(id: Vec<u8>) -> Result<Self> {
        Self::new_v0(&id.to_vec())
    }

    /// Create a new [`Namespace`] from the raw bytes.
    ///
    /// # Errors
    ///
    /// This function will return an error if the slice length is different than
    /// [`NS_SIZE`] or if the namespace is invalid. If you are constructing the
    /// version `0` namespace, check [`newV0`].
    #[wasm_bindgen(js_name = fromRaw)]
    pub fn js_from_raw(raw: Vec<u8>) -> Result<Self> {
        Self::from_raw(&raw.to_vec())
    }

    /// Converts the [`Namespace`] to a byte slice.
    #[wasm_bindgen(js_name = asBytes)]
    pub fn js_as_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    /// Returns the first byte indicating the version of the [`Namespace`].
    #[wasm_bindgen(js_name = version, getter)]
    pub fn js_version(&self) -> u8 {
        self.version()
    }

    /// Returns the trailing 28 bytes indicating the id of the [`Namespace`].
    #[wasm_bindgen(js_name = id, getter)]
    pub fn js_id(&self) -> Vec<u8> {
        self.id().to_vec()
    }
}

#[cfg(feature = "uniffi")]
#[uniffi::export]
impl Namespace {
    /// Create a new [`Namespace`] from the version and id.
    ///
    /// # Errors
    ///
    /// This function will return an error if provided namespace version isn't supported
    /// or if the namespace is invalid. If you are constructing the
    /// version `0` namespace, check [`new_namespace_v0`] for more details.
    ///
    /// [`new_namespace_v0`]: crate::nmt::Namespace::new_namespace_v0
    #[uniffi::constructor(name = "new")]
    fn uniffi_new(version: u8, id: Vec<u8>) -> Result<Self, UniffiError> {
        Ok(Namespace::new(version, &id)?)
    }

    /// Create a new [`Namespace`] version `0` with given id.
    ///
    /// The `id` must be either:
    ///  - a 28 byte slice specifying full id
    ///  - a 10 or less byte slice specifying user-defined suffix
    ///
    /// [`Namespace`]s in version 0 must have id's prefixed with 18 `0x00` bytes.
    ///
    /// # Errors
    ///
    /// This function will return an error if the provided id has incorrect length
    /// or if the `id` has 28 bytes and have doesn't have mandatory 18x`0x00` bytes prefix
    #[uniffi::constructor(name = "new_v0")]
    fn uniffi_new_v0(id: Vec<u8>) -> Result<Self, UniffiError> {
        Ok(Namespace::new_v0(&id)?)
    }

    /// Return the [`Namespace`] as a byte slice.
    #[uniffi::method]
    pub fn bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    /// Returns the first byte indicating the version of the [`Namespace`].
    #[uniffi::method(name = "version")]
    pub fn uniffi_version(&self) -> u8 {
        self.version()
    }

    /// Returns the trailing 28 bytes indicating the id of the [`Namespace`].
    #[uniffi::method(name = "id")]
    pub fn uniffi_id(&self) -> Vec<u8> {
        self.id().to_vec()
    }

    /// Returns the 10 bytes user-defined suffix of the [`Namespace`] if it's a version 0.
    #[uniffi::method(name = "id_v0")]
    pub fn uniffi_id_v0(&self) -> Option<Vec<u8>> {
        self.id_v0().map(ToOwned::to_owned)
    }

    /// Returns true if the namespace is reserved for special purposes.
    #[uniffi::method(name = "is_reserved")]
    pub fn uniffi_is_reserved(&self) -> bool {
        self.is_reserved()
    }
}

impl From<Namespace> for nmt_rs::NamespaceId<NS_SIZE> {
    fn from(value: Namespace) -> Self {
        value.0
    }
}

impl From<nmt_rs::NamespaceId<NS_SIZE>> for Namespace {
    fn from(value: nmt_rs::NamespaceId<NS_SIZE>) -> Self {
        Namespace(value)
    }
}

impl std::ops::Deref for Namespace {
    type Target = nmt_rs::NamespaceId<NS_SIZE>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for Namespace {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = BASE64_STANDARD.encode(self.0);
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for Namespace {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // base64 needs more buffer size than the final output
        let mut buf = [0u8; NS_SIZE * 2];

        let s = CowStr::deserialize(deserializer)?;

        let len = BASE64_STANDARD
            .decode_slice(s, &mut buf)
            .map_err(|e| serde::de::Error::custom(e.to_string()))?;

        Namespace::from_raw(&buf[..len]).map_err(|e| serde::de::Error::custom(e.to_string()))
    }
}

/// A pair of two nodes in the [`Nmt`], usually the siblings.
pub struct NodePair(NamespacedHash, NamespacedHash);

impl NodePair {
    fn validate_namespace_order(&self) -> Result<()> {
        let NodePair(left, right) = self;

        left.validate_namespace_order()?;
        right.validate_namespace_order()?;

        if left.max_namespace() > right.min_namespace() {
            return Err(Error::InvalidNmtNodeOrder);
        }

        Ok(())
    }
}

impl TryFrom<NodePair> for CidGeneric<NMT_ID_SIZE> {
    type Error = CidError;

    fn try_from(nodes: NodePair) -> Result<Self, Self::Error> {
        nodes
            .validate_namespace_order()
            .map_err(|e| CidError::InvalidDataFormat(e.to_string()))?;

        let hasher = NamespacedSha2Hasher::with_ignore_max_ns(true);
        let digest = hasher.hash_nodes(&nodes.0, &nodes.1).to_array();

        let mh = Multihash::wrap(NMT_MULTIHASH_CODE, &digest).unwrap();

        Ok(CidGeneric::new_v1(NMT_CODEC, mh))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as test;

    #[test]
    fn namespace_id_8_bytes() {
        let nid = Namespace::new_v0(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();

        let expected_nid = Namespace(nmt_rs::NamespaceId([
            0, // version
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // prefix
            0, 0, 1, 2, 3, 4, 5, 6, 7, 8, // id with left padding
        ]));

        assert_eq!(nid, expected_nid);
    }

    #[test]
    fn namespace_id_8_bytes_with_prefix() {
        let nid = Namespace::new_v0(&[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // prefix
            0, 0, 1, 2, 3, 4, 5, 6, 7, 8, // id
        ])
        .unwrap();

        let expected_nid = Namespace(nmt_rs::NamespaceId([
            0, // version
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // prefix
            0, 0, 1, 2, 3, 4, 5, 6, 7, 8, // id
        ]));

        assert_eq!(nid, expected_nid);
    }

    #[test]
    fn namespace_id_10_bytes() {
        let nid = Namespace::new_v0(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).unwrap();
        let expected_nid = Namespace(nmt_rs::NamespaceId([
            0, // version
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // prefix
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, // id
        ]));

        assert_eq!(nid, expected_nid);
    }

    #[test]
    fn namespace_id_max() {
        let nid = Namespace::new(0xff, &[0xff; 28]).unwrap();
        let expected_nid = Namespace::PARITY_SHARE;

        assert_eq!(nid, expected_nid);
    }

    #[test]
    fn namespace_id_const_v0() {
        let nid = Namespace::const_v0([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let expected_nid = Namespace(nmt_rs::NamespaceId([
            0, // version
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // prefix
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, // id
        ]));

        assert_eq!(nid, expected_nid);
    }

    #[test]
    fn namespace_id_const_v255() {
        let nid = Namespace::const_v255(0xab);
        let expected_nid = Namespace(nmt_rs::NamespaceId([
            0xff, // version
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, // prefix
            0xab, // id
        ]));

        assert_eq!(nid, expected_nid);
    }

    #[test]
    fn namespace_id_10_bytes_with_prefix() {
        let nid = Namespace::new_v0(&[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // prefix
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
        ])
        .unwrap();

        let expected_nid = Namespace(nmt_rs::NamespaceId([
            0, // version
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // prefix
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, // id
        ]));

        assert_eq!(nid, expected_nid);
    }

    #[test]
    fn namespace_id_with_invalid_prefix() {
        let e = Namespace::new_v0(&[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, // prefix
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
        ])
        .unwrap_err();

        assert!(matches!(e, Error::InvalidNamespaceV0));
    }

    #[test]
    fn namespace_id_11_bytes() {
        let e = Namespace::new_v0(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]).unwrap_err();

        assert!(matches!(e, Error::InvalidNamespaceSize));
    }

    #[test]
    fn namespace_id_v255_too_long() {
        let e = Namespace::new_v255(&[0xff; 29]).unwrap_err();

        assert!(matches!(e, Error::InvalidNamespaceSize));
    }

    #[test]
    fn namespace_id_v255_too_short() {
        let e = Namespace::new_v255(&[0xff; 27]).unwrap_err();

        assert!(matches!(e, Error::InvalidNamespaceSize));
    }

    #[test]
    fn namespace_id_max_invalid_prefix() {
        let namespace = &[
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe, 0xff,
        ];
        let e = Namespace::new_v255(namespace).unwrap_err();

        assert!(matches!(e, Error::InvalidNamespaceV255));
    }

    #[test]
    fn namespace_id_from_raw_bytes() {
        let nid = Namespace::from_raw(&[
            0, // version
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // prefix
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, // id
        ])
        .unwrap();

        let expected_nid = Namespace(nmt_rs::NamespaceId([
            0, // version
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // prefix
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, // id
        ]));

        assert_eq!(nid, expected_nid);
    }

    #[test]
    fn namespace_id_with_28_raw_bytes() {
        let e = Namespace::from_raw(&[
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // prefix
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, // id
        ])
        .unwrap_err();

        assert!(matches!(e, Error::InvalidNamespaceSize));
    }

    #[test]
    fn namespace_id_with_30_raw_bytes() {
        let e = Namespace::from_raw(&[
            0, // extra
            0, // version
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // prefix
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, // id
        ])
        .unwrap_err();

        assert!(matches!(e, Error::InvalidNamespaceSize));
    }

    #[test]
    fn max_namespace_id_from_raw_bytes() {
        let nid = Namespace::from_raw(&[
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff,
        ])
        .unwrap();

        let expected_nid = Namespace::PARITY_SHARE;

        assert_eq!(nid, expected_nid);
    }

    #[test]
    fn invalid_max_namespace_id_from_raw_bytes() {
        let e = Namespace::from_raw(&[
            0xff, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0,
        ])
        .unwrap_err();

        assert!(matches!(e, Error::InvalidNamespaceV255));

        let e = Namespace::from_raw(&[
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe,
            0xff,
        ])
        .unwrap_err();

        assert!(matches!(e, Error::InvalidNamespaceV255));
    }

    #[test]
    fn invalid_version() {
        let e = Namespace::from_raw(&[
            254, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ])
        .unwrap_err();

        assert!(matches!(e, Error::UnsupportedNamespaceVersion(254)));
    }

    #[test]
    fn test_generate_inner_multihash() {
        let ns0 = Namespace::new_v0(&[1]).unwrap();
        let ns1 = Namespace::new_v0(&[2]).unwrap();
        let ns2 = Namespace::new_v0(&[3]).unwrap();

        let nodes = NodePair(
            NamespacedHash::with_min_and_max_ns(*ns0, *ns1),
            NamespacedHash::with_min_and_max_ns(*ns1, *ns2),
        );

        let cid = CidGeneric::try_from(nodes).unwrap();
        assert_eq!(cid.codec(), NMT_CODEC);

        let hash = cid.hash();
        assert_eq!(hash.code(), NMT_MULTIHASH_CODE);
        assert_eq!(hash.size(), NAMESPACED_HASH_SIZE as u8);

        let hash = NamespacedHash::from_raw(hash.digest()).unwrap();
        assert_eq!(hash.min_namespace(), *ns0);
        assert_eq!(hash.max_namespace(), *ns2);
    }

    #[test]
    fn invalid_ns_order_result() {
        let ns0 = Namespace::new_v0(&[1]).unwrap();
        let ns1 = Namespace::new_v0(&[2]).unwrap();
        let ns2 = Namespace::new_v0(&[3]).unwrap();

        let nodes = NodePair(
            NamespacedHash::with_min_and_max_ns(*ns1, *ns2),
            NamespacedHash::with_min_and_max_ns(*ns0, *ns0),
        );
        let result = CidGeneric::try_from(nodes).unwrap_err();

        assert_eq!(
            result,
            CidError::InvalidDataFormat("Invalid nmt node order".to_string())
        );
    }

    #[test]
    fn test_read_multihash() {
        let multihash = [
            0x81, 0xEE, 0x01, // code = 7701
            0x5A, // len = NAMESPACED_HASH_SIZE = 90
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, // min ns
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            9, // max ns
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
            0xFF, 0xFF, 0xFF, 0xFF, // hash
        ];

        let mh = Multihash::<NAMESPACED_HASH_SIZE>::from_bytes(&multihash).unwrap();
        assert_eq!(mh.code(), NMT_CODEC);
        assert_eq!(mh.size(), NAMESPACED_HASH_SIZE as u8);
        let hash = NamespacedHash::from_raw(mh.digest()).unwrap();
        assert_eq!(hash.min_namespace(), *Namespace::new_v0(&[1]).unwrap());
        assert_eq!(hash.max_namespace(), *Namespace::new_v0(&[9]).unwrap());
        assert_eq!(hash.hash(), [0xFF; 32]);
    }

    #[test]
    fn test_serialize_namespace_binary() {
        let ns = Namespace::new_v0(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]).unwrap();
        let serialized = postcard::to_allocvec(&ns).unwrap();
        let deserialized: Namespace = postcard::from_bytes(&serialized).unwrap();
        assert_eq!(ns, deserialized);
    }
}
