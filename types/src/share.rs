#[cfg(feature = "uniffi")]
use std::sync::Arc;

use blockstore::block::{Block, CidError};
use cid::CidGeneric;
use multihash::Multihash;
use nmt_rs::NamespaceMerkleHasher;
use nmt_rs::simple_merkle::tree::MerkleHash;
use serde::{Deserialize, Serialize};
#[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
use wasm_bindgen::prelude::*;

use crate::consts::appconsts;
#[cfg(feature = "uniffi")]
use crate::error::UniffiResult;
use crate::nmt::{
    NMT_CODEC, NMT_ID_SIZE, NMT_MULTIHASH_CODE, NS_SIZE, Namespace, NamespacedSha2Hasher,
};
use crate::state::AccAddress;
use crate::{Error, Result};

mod info_byte;
mod proof;

pub use celestia_proto::shwap::Share as RawShare;
pub use info_byte::InfoByte;
pub use proof::ShareProof;

const SHARE_SEQUENCE_LENGTH_OFFSET: usize = NS_SIZE + appconsts::SHARE_INFO_BYTES;
const SHARE_SIGNER_OFFSET: usize = SHARE_SEQUENCE_LENGTH_OFFSET + appconsts::SEQUENCE_LEN_BYTES;

/// A single fixed-size chunk of data which is used to form an [`ExtendedDataSquare`].
///
/// All data in Celestia is split into [`Share`]s before being put into a
/// block's data square. See [`Blob::to_shares`].
///
/// All shares have the fixed size of 512 bytes and the following structure:
///
/// ```text
/// | Namespace | InfoByte | (optional) sequence length | data |
/// ```
///
/// `sequence length` is the length of the original data in bytes and is present only in the first of the shares the data was split into.
///
/// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
/// [`Blob::to_shares`]: crate::Blob::to_shares
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawShare", into = "RawShare")]
#[cfg_attr(
    all(feature = "wasm-bindgen", target_arch = "wasm32"),
    wasm_bindgen(inspectable)
)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
pub struct Share {
    /// A raw data of the share.
    data: [u8; appconsts::SHARE_SIZE],
    is_parity: bool,
}

/// A list of shares that were published at particular height.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(
    all(feature = "wasm-bindgen", target_arch = "wasm32"),
    wasm_bindgen(getter_with_clone, inspectable)
)]
pub struct SharesAtHeight {
    /// height the shares were published at
    pub height: u64,
    /// shares published
    pub shares: Vec<Share>,
}

impl Share {
    /// Create a new [`Share`] from raw bytes.
    ///
    /// # Errors
    ///
    /// This function will return an error if the slice length isn't
    /// [`SHARE_SIZE`] or if a namespace encoded in the share is invalid.
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::Share;
    ///
    /// let raw = [0; 512];
    /// let share = Share::from_raw(&raw).unwrap();
    /// ```
    ///
    /// [`SHARE_SIZE`]: crate::consts::appconsts::SHARE_SIZE
    pub fn from_raw(data: &[u8]) -> Result<Self> {
        if data.len() != appconsts::SHARE_SIZE {
            return Err(Error::InvalidShareSize(data.len()));
        }

        // validate namespace and info byte so that we can return it later without checks
        Namespace::from_raw(&data[..NS_SIZE])?;
        InfoByte::from_raw(data[NS_SIZE])?;

        Ok(Share {
            data: data.try_into().unwrap(),
            is_parity: false,
        })
    }

    /// Create a new [`Share`] within [`Namespace::PARITY_SHARE`] from raw bytes.
    ///
    /// # Errors
    ///
    /// This function will return an error if the slice length isn't [`SHARE_SIZE`].
    ///
    /// [`SHARE_SIZE`]: crate::consts::appconsts::SHARE_SIZE
    pub fn parity(data: &[u8]) -> Result<Share> {
        if data.len() != appconsts::SHARE_SIZE {
            return Err(Error::InvalidShareSize(data.len()));
        }

        Ok(Share {
            data: data.try_into().unwrap(),
            is_parity: true,
        })
    }

    /// Returns true if share contains parity data.
    pub fn is_parity(&self) -> bool {
        self.is_parity
    }

    /// Get the [`Namespace`] the [`Share`] belongs to.
    pub fn namespace(&self) -> Namespace {
        if !self.is_parity {
            Namespace::new_unchecked(self.data[..NS_SIZE].try_into().unwrap())
        } else {
            Namespace::PARITY_SHARE
        }
    }

    /// Return Share's `InfoByte`
    ///
    /// Returns `None` if share is within [`Namespace::PARITY_SHARE`].
    pub fn info_byte(&self) -> Option<InfoByte> {
        if !self.is_parity() {
            Some(InfoByte::from_raw_unchecked(self.data[NS_SIZE]))
        } else {
            None
        }
    }

    /// For first share in a sequence, return sequence length, `None` for continuation shares
    pub fn sequence_length(&self) -> Option<u32> {
        if self.info_byte()?.is_sequence_start() {
            let sequence_length_bytes = &self.data[SHARE_SEQUENCE_LENGTH_OFFSET
                ..SHARE_SEQUENCE_LENGTH_OFFSET + appconsts::SEQUENCE_LEN_BYTES];
            Some(u32::from_be_bytes(
                sequence_length_bytes.try_into().unwrap(),
            ))
        } else {
            None
        }
    }

    /// Get the `signer` part of share if it's first in the sequence of sparse shares and `share_version` supprots
    /// it. Otherwise returns `None`
    pub fn signer(&self) -> Option<AccAddress> {
        let info_byte = self.info_byte()?;
        if info_byte.is_sequence_start() && info_byte.version() == appconsts::SHARE_VERSION_ONE {
            let signer_bytes =
                &self.data[SHARE_SIGNER_OFFSET..SHARE_SIGNER_OFFSET + appconsts::SIGNER_SIZE];
            Some(AccAddress::try_from(signer_bytes).expect("must have correct size"))
        } else {
            None
        }
    }

    /// Get the payload of the share.
    ///
    /// Payload is the data that shares contain after all its metadata,
    /// e.g. blob data in sparse shares.
    ///
    /// Returns `None` if share is within [`Namespace::PARITY_SHARE`].
    pub fn payload(&self) -> Option<&[u8]> {
        let info_byte = self.info_byte()?;

        let start = if info_byte.is_sequence_start() {
            if info_byte.version() == appconsts::SHARE_VERSION_ONE {
                // in sparse share v1, last metadata in first share is signer
                SHARE_SIGNER_OFFSET + appconsts::SIGNER_SIZE
            } else {
                // otherwise last metadata in first share is sequence len
                // (we ignore reserved bytes of compact shares, they are treated as payload)
                SHARE_SEQUENCE_LENGTH_OFFSET + appconsts::SEQUENCE_LEN_BYTES
            }
        } else {
            // or info byte in continuation shares
            SHARE_SEQUENCE_LENGTH_OFFSET
        };
        Some(&self.data[start..])
    }

    /// Get the underlying share data.
    pub fn data(&self) -> &[u8; appconsts::SHARE_SIZE] {
        &self.data
    }

    /// Converts this [`Share`] into the raw bytes vector.
    ///
    /// This will include also the [`InfoByte`] and the `sequence length`.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_ref().to_vec()
    }
}

#[cfg(feature = "uniffi")]
#[uniffi::export]
impl Share {
    /// Create a new [`Share`] from raw bytes.
    ///
    /// # Errors
    ///
    /// This function will return an error if data provided isn't share sized or
    /// if the namespace encoded in the share is invalid.
    #[uniffi::constructor(name = "from_raw")]
    pub fn uniffi_from_raw(data: &[u8]) -> UniffiResult<Self> {
        Ok(Share::from_raw(data)?)
    }

    /// Create a new [`Share`] within [`Namespace::PARITY_SHARE`] from raw bytes.
    ///
    /// # Errors
    ///
    /// This function will return an error if provided data isn't share sized
    #[uniffi::constructor(name = "parity")]
    pub fn uniffi_parity(data: &[u8]) -> UniffiResult<Share> {
        Ok(Share::parity(data)?)
    }

    /// Returns true if share contains parity data.
    #[uniffi::method(name = "is_parity")]
    pub fn uniffi_is_parity(&self) -> bool {
        self.is_parity()
    }

    /// Get the [`Namespace`] the [`Share`] belongs to.
    #[uniffi::method(name = "namespace")]
    pub fn uniffi_namespace(&self) -> Namespace {
        self.namespace()
    }

    /// Return Share's `InfoByte`
    ///
    /// Returns `None` if share is parity share
    #[uniffi::method(name = "info_byte")]
    pub fn uniffi_info_byte(&self) -> Option<Arc<InfoByte>> {
        self.info_byte().map(Arc::new)
    }

    /// For first share in a sequence, return sequence length, `None` for continuation shares
    #[uniffi::method(name = "sequence_length")]
    pub fn uniffi_sequence_length(&self) -> Option<u32> {
        self.sequence_length()
    }

    /// Get the `signer` part of share if it's first in the sequence of sparse shares and `share_version` supprots
    /// it. Otherwise returns `None`
    #[uniffi::method(name = "signer")]
    pub fn uniffi_signer(&self) -> Option<AccAddress> {
        self.signer()
    }

    /// Get the payload of the share.
    ///
    /// Payload is the data that shares contain after all its metadata,
    /// e.g. blob data in sparse shares.
    ///
    /// Returns `None` if share is parity share
    #[uniffi::method(name = "payload")]
    pub fn uniffi_payload(&self) -> Option<Vec<u8>> {
        self.payload().map(|p| p.to_vec())
    }

    /// Get the underlying share data.
    #[uniffi::method(name = "data")]
    pub fn uniffi_data(&self) -> Vec<u8> {
        self.data.to_vec()
    }
}

impl AsRef<[u8]> for Share {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl AsMut<[u8]> for Share {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl Block<NMT_ID_SIZE> for Share {
    fn cid(&self) -> Result<CidGeneric<NMT_ID_SIZE>, CidError> {
        let hasher = NamespacedSha2Hasher::with_ignore_max_ns(true);
        let digest = hasher.hash_leaf(self.as_ref()).iter().collect::<Vec<_>>();

        // size is correct, so unwrap is safe
        let mh = Multihash::wrap(NMT_MULTIHASH_CODE, &digest).unwrap();

        Ok(CidGeneric::new_v1(NMT_CODEC, mh))
    }

    fn data(&self) -> &[u8] {
        &self.data
    }
}

impl TryFrom<RawShare> for Share {
    type Error = Error;

    fn try_from(value: RawShare) -> Result<Self, Self::Error> {
        Share::from_raw(&value.data)
    }
}

impl From<Share> for RawShare {
    fn from(value: Share) -> Self {
        RawShare {
            data: value.to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Blob;
    use crate::nmt::{NAMESPACED_HASH_SIZE, NamespaceProof, NamespacedHash};
    use base64::prelude::*;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as test;

    #[test]
    fn share_v0_structure() {
        let ns = Namespace::new_v0(b"foo").unwrap();
        let blob = Blob::new(ns, vec![7; 512], None).unwrap();

        let shares = blob.to_shares().unwrap();

        assert_eq!(shares.len(), 2);

        assert_eq!(shares[0].namespace(), ns);
        assert_eq!(shares[1].namespace(), ns);

        assert_eq!(shares[0].info_byte().unwrap().version(), 0);
        assert_eq!(shares[1].info_byte().unwrap().version(), 0);

        assert!(shares[0].info_byte().unwrap().is_sequence_start());
        assert!(!shares[1].info_byte().unwrap().is_sequence_start());

        // share v0 doesn't have signer
        assert!(shares[0].signer().is_none());
        assert!(shares[1].signer().is_none());

        const BYTES_IN_SECOND: usize = 512 - appconsts::FIRST_SPARSE_SHARE_CONTENT_SIZE;
        assert_eq!(
            shares[0].payload().unwrap(),
            &[7; appconsts::FIRST_SPARSE_SHARE_CONTENT_SIZE]
        );
        assert_eq!(
            shares[1].payload().unwrap(),
            &[
                // rest of the blob
                &[7; BYTES_IN_SECOND][..],
                // padding
                &[0; appconsts::CONTINUATION_SPARSE_SHARE_CONTENT_SIZE - BYTES_IN_SECOND][..]
            ]
            .concat()
        );
    }

    #[test]
    fn share_v1_structure() {
        let ns = Namespace::new_v0(b"foo").unwrap();
        let blob = Blob::new(ns, vec![7; 512], Some([5; 20].into())).unwrap();

        let shares = blob.to_shares().unwrap();

        assert_eq!(shares.len(), 2);

        assert_eq!(shares[0].namespace(), ns);
        assert_eq!(shares[1].namespace(), ns);

        assert_eq!(shares[0].info_byte().unwrap().version(), 1);
        assert_eq!(shares[1].info_byte().unwrap().version(), 1);

        assert!(shares[0].info_byte().unwrap().is_sequence_start());
        assert!(!shares[1].info_byte().unwrap().is_sequence_start());

        // share v1 has signer only if it's the first share of the sequence
        assert_eq!(shares[0].signer().unwrap(), [5; 20].into());
        assert!(shares[1].signer().is_none());

        const BYTES_IN_SECOND: usize =
            512 - appconsts::FIRST_SPARSE_SHARE_CONTENT_SIZE + appconsts::SIGNER_SIZE;
        assert_eq!(
            shares[0].payload().unwrap(),
            &[7; appconsts::FIRST_SPARSE_SHARE_CONTENT_SIZE - appconsts::SIGNER_SIZE]
        );
        assert_eq!(
            shares[1].payload().unwrap(),
            &[
                // rest of the blob
                &[7; BYTES_IN_SECOND][..],
                // padding
                &[0; appconsts::CONTINUATION_SPARSE_SHARE_CONTENT_SIZE - BYTES_IN_SECOND][..]
            ]
            .concat()
        );
    }

    #[test]
    fn share_should_have_correct_len() {
        Share::from_raw(&[0; 0]).unwrap_err();
        Share::from_raw(&[0; 100]).unwrap_err();
        Share::from_raw(&[0; appconsts::SHARE_SIZE - 1]).unwrap_err();
        Share::from_raw(&[0; appconsts::SHARE_SIZE + 1]).unwrap_err();
        Share::from_raw(&[0; 2 * appconsts::SHARE_SIZE]).unwrap_err();

        Share::from_raw(&vec![0; appconsts::SHARE_SIZE]).unwrap();
    }

    #[test]
    fn decode_presence_proof() {
        let blob_get_proof_response = r#"{
            "start": 1,
            "end": 2,
            "nodes": [
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABA+poCQOx7UzVkteV9DgcA6g29ZXXOp0hYZb67hoNkFP",
                "/////////////////////////////////////////////////////////////////////////////8PbbPgQcFSaW2J/BWiJqrCoj6K4g/UUd0Y9dadwqrz+"
            ]
        }"#;

        let proof: NamespaceProof =
            serde_json::from_str(blob_get_proof_response).expect("can not parse proof");

        assert!(!proof.is_of_absence());

        let sibling = &proof.siblings()[0];
        let min_ns_bytes = &sibling.min_namespace().0[..];
        let max_ns_bytes = &sibling.max_namespace().0[..];
        let hash_bytes = &sibling.hash()[..];
        assert_eq!(
            min_ns_bytes,
            b64_decode("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQ=")
        );
        assert_eq!(
            max_ns_bytes,
            b64_decode("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQ=")
        );
        assert_eq!(
            hash_bytes,
            b64_decode("D6mgJA7HtTNWS15X0OBwDqDb1ldc6nSFhlvruGg2QU8=")
        );

        let sibling = &proof.siblings()[1];
        let min_ns_bytes = &sibling.min_namespace().0[..];
        let max_ns_bytes = &sibling.max_namespace().0[..];
        let hash_bytes = &sibling.hash()[..];
        assert_eq!(
            min_ns_bytes,
            b64_decode("//////////////////////////////////////8=")
        );
        assert_eq!(
            max_ns_bytes,
            b64_decode("//////////////////////////////////////8=")
        );
        assert_eq!(
            hash_bytes,
            b64_decode("w9ts+BBwVJpbYn8FaImqsKiPoriD9RR3Rj11p3CqvP4=")
        );
    }

    #[test]
    fn decode_absence_proof() {
        let blob_get_proof_response = r#"{
            "start": 1,
            "end": 2,
            "nodes": [
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABD+sL4GAQk9mj+ejzHmHjUJEyemkpExb+S5aEDtmuHEq",
                "/////////////////////////////////////////////////////////////////////////////zgUEBW/wWmCfnwXfalgqMfK9sMy168y3XRzdwY1jpZY"
            ],
            "leaf_hash": "AAAAAAAAAAAAAAAAAAAAAAAAAJLVUf6krS8362EAAAAAAAAAAAAAAAAAAAAAAAAAktVR/qStLzfrYeEAWUHOa+lE38pJyHstgGaqi9RXPhZtzUscK7iTUbQS",
            "is_max_namespace_ignored": true
        }"#;

        let proof: NamespaceProof =
            serde_json::from_str(blob_get_proof_response).expect("can not parse proof");

        assert!(proof.is_of_absence());

        let sibling = &proof.siblings()[0];
        let min_ns_bytes = &sibling.min_namespace().0[..];
        let max_ns_bytes = &sibling.max_namespace().0[..];
        let hash_bytes = &sibling.hash()[..];
        assert_eq!(
            min_ns_bytes,
            b64_decode("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQ=")
        );
        assert_eq!(
            max_ns_bytes,
            b64_decode("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQ=")
        );
        assert_eq!(
            hash_bytes,
            b64_decode("P6wvgYBCT2aP56PMeYeNQkTJ6aSkTFv5LloQO2a4cSo=")
        );

        let sibling = &proof.siblings()[1];
        let min_ns_bytes = &sibling.min_namespace().0[..];
        let max_ns_bytes = &sibling.max_namespace().0[..];
        let hash_bytes = &sibling.hash()[..];
        assert_eq!(
            min_ns_bytes,
            b64_decode("//////////////////////////////////////8=")
        );
        assert_eq!(
            max_ns_bytes,
            b64_decode("//////////////////////////////////////8=")
        );
        assert_eq!(
            hash_bytes,
            b64_decode("OBQQFb/BaYJ+fBd9qWCox8r2wzLXrzLddHN3BjWOllg=")
        );

        let nmt_rs::NamespaceProof::AbsenceProof {
            leaf: Some(leaf), ..
        } = &*proof
        else {
            unreachable!();
        };

        let min_ns_bytes = &leaf.min_namespace().0[..];
        let max_ns_bytes = &leaf.max_namespace().0[..];
        let hash_bytes = &leaf.hash()[..];
        assert_eq!(
            min_ns_bytes,
            b64_decode("AAAAAAAAAAAAAAAAAAAAAAAAAJLVUf6krS8362E=")
        );
        assert_eq!(
            max_ns_bytes,
            b64_decode("AAAAAAAAAAAAAAAAAAAAAAAAAJLVUf6krS8362E=")
        );
        assert_eq!(
            hash_bytes,
            b64_decode("4QBZQc5r6UTfyknIey2AZqqL1Fc+Fm3NSxwruJNRtBI=")
        );
    }

    fn b64_decode(s: &str) -> Vec<u8> {
        BASE64_STANDARD.decode(s).expect("failed to decode base64")
    }

    #[test]
    fn test_generate_leaf_multihash() {
        let namespace = Namespace::new_v0(&[1, 2, 3]).unwrap();
        let mut data = [0xCDu8; appconsts::SHARE_SIZE];
        data[..NS_SIZE].copy_from_slice(namespace.as_bytes());
        let share = Share::from_raw(&data).unwrap();

        let cid = share.cid().unwrap();
        assert_eq!(cid.codec(), NMT_CODEC);
        let hash = cid.hash();
        assert_eq!(hash.code(), NMT_MULTIHASH_CODE);
        assert_eq!(hash.size(), NAMESPACED_HASH_SIZE as u8);
        let hash = NamespacedHash::try_from(hash.digest()).unwrap();
        assert_eq!(hash.min_namespace(), *namespace);
        assert_eq!(hash.max_namespace(), *namespace);
    }
}
