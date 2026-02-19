//! Types related to creation and submission of blobs.

use std::iter;
#[cfg(feature = "uniffi")]
use std::sync::Arc;

use serde::{Deserialize, Serialize};
#[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
use wasm_bindgen::prelude::*;

mod commitment;
mod msg_pay_for_blobs;

use crate::consts::appconsts;
use crate::consts::appconsts::AppVersion;
#[cfg(feature = "uniffi")]
use crate::error::UniffiResult;
use crate::nmt::Namespace;
use crate::state::{AccAddress, AddressTrait};
use crate::{Error, Result, Share, bail_validation};

pub use self::commitment::Commitment;
pub use self::msg_pay_for_blobs::MsgPayForBlobs;
pub use celestia_proto::celestia::blob::v1::MsgPayForBlobs as RawMsgPayForBlobs;
pub use celestia_proto::proto::blob::v2::BlobProto as RawBlob;
pub use celestia_proto::proto::blob::v2::BlobTx as RawBlobTx;

/// Arbitrary data that can be stored in the network within certain [`Namespace`].
// NOTE: We don't use the `serde(try_from)` pattern for this type
// becase JSON representation needs to have `commitment` field but
// Protobuf definition doesn't.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "custom_serde::SerdeBlob", into = "custom_serde::SerdeBlob")]
#[cfg_attr(
    all(feature = "wasm-bindgen", target_arch = "wasm32"),
    wasm_bindgen(getter_with_clone, inspectable)
)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Object))]
pub struct Blob {
    /// A [`Namespace`] the [`Blob`] belongs to.
    pub namespace: Namespace,
    /// Data stored within the [`Blob`].
    pub data: Vec<u8>,
    /// Version indicating the format in which [`Share`]s should be created from this [`Blob`].
    pub share_version: u8,
    /// A [`Commitment`] computed from the [`Blob`]s data.
    pub commitment: Commitment,
    /// Index of the blob's first share in the EDS. Only set for blobs retrieved from chain.
    pub index: Option<u64>,
    /// A signer of the blob, i.e. address of the account which submitted the blob.
    ///
    /// Must be present in `share_version 1` and absent otherwise.
    pub signer: Option<AccAddress>,
}

/// Params defines the parameters for the blob module.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[cfg_attr(
    all(feature = "wasm-bindgen", target_arch = "wasm32"),
    wasm_bindgen(getter_with_clone, inspectable)
)]
pub struct BlobParams {
    /// Gas cost per blob byte
    pub gas_per_blob_byte: u32,
    /// Max square size
    pub gov_max_square_size: u64,
}

/// List of blobs together with height they were published at
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(
    all(feature = "wasm-bindgen", target_arch = "wasm32"),
    wasm_bindgen(getter_with_clone, inspectable)
)]
pub struct BlobsAtHeight {
    /// Height the blobs were published at
    pub height: u64,
    /// Published blobs
    pub blobs: Vec<Blob>,
}

/// Test struct for testing
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct TestStruct {
    pub some: String,
}

impl Blob {
    /// Create a new blob with the given data within the [`Namespace`], with optional signer.
    ///
    /// # Notes
    ///
    /// If present onchain, `signer` was verified by consensus node on blob submission.
    ///
    /// # Errors
    ///
    /// This function propagates any error from the [`Commitment`] creation.
    /// To use `signer = Some(address)`, [`AppVersion`] must be at least [`AppVersion::V3`].
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::{AppVersion, Blob, nmt::Namespace, state::AccAddress};
    ///
    /// let my_namespace = Namespace::new_v0(&[1, 2, 3, 4, 5]).expect("Invalid namespace");
    /// let signer: AccAddress = "celestia1377k5an3f94v6wyaceu0cf4nq6gk2jtpc46g7h"
    ///     .parse()
    ///     .unwrap();
    /// let blob = Blob::new(
    ///     my_namespace,
    ///     b"some data to store on blockchain".to_vec(),
    ///     Some(signer),
    ///     AppVersion::latest(),
    /// )
    /// .expect("Failed to create a blob");
    ///
    /// assert_eq!(
    ///     serde_json::to_string_pretty(&blob).unwrap(),
    ///     indoc::indoc! {r#"{
    ///       "namespace": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQIDBAU=",
    ///       "data": "c29tZSBkYXRhIHRvIHN0b3JlIG9uIGJsb2NrY2hhaW4=",
    ///       "share_version": 1,
    ///       "commitment": "AUpLKHYnlrfK0cX6DcGyryFMld1nJia+cjkCwXFTFgA=",
    ///       "index": -1,
    ///       "signer": "j71qdnFJas04ncZ4/CazBpFlSWE="
    ///     }"#},
    /// );
    /// ```
    pub fn new(
        namespace: Namespace,
        data: Vec<u8>,
        signer: Option<AccAddress>,
        app_version: AppVersion,
    ) -> Result<Blob> {
        let share_version = if signer.is_none() {
            appconsts::SHARE_VERSION_ZERO
        } else {
            appconsts::SHARE_VERSION_ONE
        };

        let commitment = Commitment::from_blob(
            namespace,
            &data[..],
            share_version,
            signer.as_ref(),
            app_version,
        )?;

        Ok(Blob {
            namespace,
            data,
            share_version,
            commitment,
            index: None,
            signer,
        })
    }

    /// Creates a `Blob` from [`RawBlob`] and an [`AppVersion`].
    pub fn from_raw(raw: RawBlob, app_version: AppVersion) -> Result<Blob> {
        let namespace = Namespace::new(raw.namespace_version as u8, &raw.namespace_id)?;
        let share_version =
            u8::try_from(raw.share_version).map_err(|_| Error::UnsupportedShareVersion(u8::MAX))?;
        let signer = raw.signer.try_into().map(AccAddress::new).ok();
        let commitment = Commitment::from_blob(
            namespace,
            &raw.data[..],
            share_version,
            signer.as_ref(),
            app_version,
        )?;

        Ok(Blob {
            namespace,
            data: raw.data,
            share_version,
            commitment,
            index: None,
            signer,
        })
    }

    /// Validate [`Blob`]s data with the [`Commitment`] it has.
    ///
    /// # Errors
    ///
    /// If validation fails, this function will return an error with a reason of failure.
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::Blob;
    /// # use celestia_types::consts::appconsts::AppVersion;
    /// # use celestia_types::nmt::Namespace;
    /// #
    /// # let namespace = Namespace::new_v0(&[1, 2, 3, 4, 5]).expect("Invalid namespace");
    ///
    /// let mut blob = Blob::new(namespace, b"foo".to_vec(), None, AppVersion::latest()).unwrap();
    ///
    /// assert!(blob.validate(AppVersion::latest()).is_ok());
    ///
    /// let other_blob = Blob::new(namespace, b"bar".to_vec(), None, AppVersion::latest()).unwrap();
    /// blob.commitment = other_blob.commitment;
    ///
    /// assert!(blob.validate(AppVersion::V2).is_err());
    /// ```
    pub fn validate(&self, app_version: AppVersion) -> Result<()> {
        let computed_commitment = Commitment::from_blob(
            self.namespace,
            &self.data,
            self.share_version,
            self.signer.as_ref(),
            app_version,
        )?;

        if self.commitment != computed_commitment {
            bail_validation!("blob commitment != localy computed commitment")
        }

        Ok(())
    }

    /// Validate [`Blob`]s data with a [`Commitment`].
    ///
    /// # Errors
    ///
    /// If validation fails, this function will return an error with a reason of failure.
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::Blob;
    /// # use celestia_types::consts::appconsts::AppVersion;
    /// # use celestia_types::nmt::Namespace;
    /// #
    /// # let namespace = Namespace::new_v0(&[1, 2, 3, 4, 5]).expect("Invalid namespace");
    /// #
    /// # let commitment = Blob::new(namespace, b"foo".to_vec(), None, AppVersion::latest())
    /// #     .unwrap()
    /// #     .commitment;
    ///
    /// let blob = Blob::new(namespace, b"foo".to_vec(), None, AppVersion::latest()).unwrap();
    ///
    /// assert!(blob.validate_with_commitment(&commitment, AppVersion::latest()).is_ok());
    ///
    /// let other_commitment = Blob::new(namespace, b"bar".to_vec(), None, AppVersion::latest())
    ///     .unwrap()
    ///     .commitment;
    ///
    /// assert!(blob.validate_with_commitment(&other_commitment, AppVersion::V2).is_err());
    /// ```
    pub fn validate_with_commitment(
        &self,
        commitment: &Commitment,
        app_version: AppVersion,
    ) -> Result<()> {
        self.validate(app_version)?;

        if self.commitment != *commitment {
            bail_validation!("blob commitment != commitment");
        }

        Ok(())
    }

    /// Encode the blob into a sequence of shares.
    ///
    /// Check the [`Share`] documentation for more information about the share format.
    ///
    /// # Errors
    ///
    /// This function will return an error if [`InfoByte`] creation fails
    /// or the data length overflows [`u32`].
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::Blob;
    /// # use celestia_types::consts::appconsts::AppVersion;
    /// # use celestia_types::nmt::Namespace;
    /// # let namespace = Namespace::new_v0(&[1, 2, 3, 4, 5]).expect("Invalid namespace");
    ///
    /// let blob = Blob::new(namespace, b"foo".to_vec(), None, AppVersion::latest()).unwrap();
    /// let shares = blob.to_shares().unwrap();
    ///
    /// assert_eq!(shares.len(), 1);
    /// ```
    ///
    /// [`Share`]: crate::share::Share
    /// [`InfoByte`]: crate::share::InfoByte
    pub fn to_shares(&self) -> Result<Vec<Share>> {
        commitment::split_blob_to_shares(
            self.namespace,
            self.share_version,
            &self.data,
            self.signer.as_ref(),
        )
    }

    /// Reconstructs a blob from shares.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - there is not enough shares to reconstruct the blob
    /// - blob doesn't start with the first share
    /// - shares are from any reserved namespace
    /// - shares for the blob have different namespaces / share version
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::{AppVersion, Blob};
    /// # use celestia_types::nmt::Namespace;
    /// # let namespace = Namespace::new_v0(&[1, 2, 3, 4, 5]).expect("Invalid namespace");
    ///
    /// let blob = Blob::new(namespace, b"foo".to_vec(), None, AppVersion::V2).unwrap();
    /// let shares = blob.to_shares().unwrap();
    ///
    /// let reconstructed = Blob::reconstruct(&shares, AppVersion::latest()).unwrap();
    ///
    /// assert_eq!(blob, reconstructed);
    /// ```
    pub fn reconstruct<'a, I>(shares: I, app_version: AppVersion) -> Result<Self>
    where
        I: IntoIterator<Item = &'a Share>,
    {
        let mut shares = shares.into_iter();
        let first_share = shares.next().ok_or(Error::MissingShares)?;
        let blob_len = first_share
            .sequence_length()
            .ok_or(Error::ExpectedShareWithSequenceStart)?;
        let namespace = first_share.namespace();
        if namespace.is_reserved() {
            return Err(Error::UnexpectedReservedNamespace);
        }
        let share_version = first_share.info_byte().expect("non parity").version();
        let signer = first_share.signer();

        let shares_needed = shares_needed_for_blob(blob_len as usize, signer.is_some());
        let mut data =
            Vec::with_capacity(shares_needed * appconsts::CONTINUATION_SPARSE_SHARE_CONTENT_SIZE);
        data.extend_from_slice(first_share.payload().expect("non parity"));

        for _ in 1..shares_needed {
            let share = shares.next().ok_or(Error::MissingShares)?;
            if share.namespace() != namespace {
                return Err(Error::BlobSharesMetadataMismatch(format!(
                    "expected namespace ({:?}) got ({:?})",
                    namespace,
                    share.namespace()
                )));
            }
            let version = share.info_byte().expect("non parity").version();
            if version != share_version {
                return Err(Error::BlobSharesMetadataMismatch(format!(
                    "expected share version ({share_version}) got ({version})"
                )));
            }
            if share.sequence_length().is_some() {
                return Err(Error::UnexpectedSequenceStart);
            }
            data.extend_from_slice(share.payload().expect("non parity"));
        }

        // remove padding
        data.truncate(blob_len as usize);

        if share_version == appconsts::SHARE_VERSION_ZERO {
            Self::new(namespace, data, None, app_version)
        } else if share_version == appconsts::SHARE_VERSION_ONE {
            // shouldn't happen as we have user namespace, seq start, and share v1
            let signer = signer.ok_or(Error::MissingSigner)?;
            Self::new(namespace, data, Some(signer), app_version)
        } else {
            Err(Error::UnsupportedShareVersion(share_version))
        }
    }

    /// Reconstructs all the blobs from shares.
    ///
    /// This function will seek shares that indicate start of the next blob (with
    /// [`Share::sequence_length`]) and pass them to [`Blob::reconstruct`].
    /// It will automatically ignore all shares that are within reserved namespaces
    /// e.g. it is completely fine to pass whole [`ExtendedDataSquare`] to this
    /// function and get all blobs in the block.
    ///
    /// # Errors
    ///
    /// This function propagates any errors from [`Blob::reconstruct`].
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::{AppVersion, Blob};
    /// # use celestia_types::nmt::Namespace;
    /// # let namespace1 = Namespace::new_v0(&[1, 2, 3, 4, 5]).expect("Invalid namespace");
    /// # let namespace2 = Namespace::new_v0(&[2, 3, 4, 5, 6]).expect("Invalid namespace");
    ///
    /// let blobs = vec![
    ///     Blob::new(namespace1, b"foo".to_vec(), None, AppVersion::latest()).unwrap(),
    ///     Blob::new(namespace2, b"bar".to_vec(), None, AppVersion::latest()).unwrap(),
    /// ];
    /// let shares: Vec<_> = blobs.iter().flat_map(|blob| blob.to_shares().unwrap()).collect();
    ///
    /// let reconstructed = Blob::reconstruct_all(&shares, AppVersion::latest()).unwrap();
    ///
    /// assert_eq!(blobs, reconstructed);
    /// ```
    ///
    /// [`ExtendedDataSquare`]: crate::ExtendedDataSquare
    pub fn reconstruct_all<'a, I>(shares: I, app_version: AppVersion) -> Result<Vec<Self>>
    where
        I: IntoIterator<Item = &'a Share>,
    {
        let mut shares = shares
            .into_iter()
            .filter(|shr| !shr.namespace().is_reserved());
        let mut blobs = Vec::with_capacity(2);

        loop {
            let mut blob = {
                // find next share from blobs namespace that is sequence start
                let Some(start) = shares.find(|&shr| shr.sequence_length().is_some()) else {
                    break;
                };
                iter::once(start).chain(&mut shares)
            };
            blobs.push(Blob::reconstruct(&mut blob, app_version)?);
        }

        Ok(blobs)
    }

    /// Get the amount of shares needed to encode this blob.
    ///
    /// # Example
    ///
    /// ```
    /// use celestia_types::{AppVersion, Blob};
    /// # use celestia_types::nmt::Namespace;
    /// # let namespace = Namespace::new_v0(&[1, 2, 3, 4, 5]).expect("Invalid namespace");
    ///
    /// let blob = Blob::new(namespace, b"foo".to_vec(), None, AppVersion::latest()).unwrap();
    /// let shares_len = blob.shares_len();
    ///
    /// let blob_shares = blob.to_shares().unwrap();
    ///
    /// assert_eq!(shares_len, blob_shares.len());
    /// ```
    pub fn shares_len(&self) -> usize {
        let Some(without_first_share) = self
            .data
            .len()
            .checked_sub(appconsts::FIRST_SPARSE_SHARE_CONTENT_SIZE)
        else {
            return 1;
        };
        1 + without_first_share.div_ceil(appconsts::CONTINUATION_SPARSE_SHARE_CONTENT_SIZE)
    }
}

#[cfg(feature = "uniffi")]
#[uniffi::export]
impl Blob {
    /// Create a new blob with the given data within the [`Namespace`].
    ///
    /// # Errors
    ///
    /// This function propagates any error from the [`Commitment`] creation.
    // constructor cannot be named `new`, otherwise it doesn't show up in Kotlin ¯\_(ツ)_/¯
    #[uniffi::constructor(name = "create")]
    pub fn uniffi_new(
        namespace: Arc<Namespace>,
        data: Vec<u8>,
        app_version: AppVersion,
    ) -> UniffiResult<Self> {
        let namespace = Arc::unwrap_or_clone(namespace);
        Ok(Blob::new(namespace, data, None, app_version)?)
    }

    /// Create a new blob with the given data within the [`Namespace`] and with given signer.
    ///
    /// # Errors
    ///
    /// This function propagates any error from the [`Commitment`] creation. Also [`AppVersion`]
    /// must be at least [`AppVersion::V3`].
    #[uniffi::constructor(name = "create_with_signer")]
    pub fn uniffi_new_with_signer(
        namespace: Arc<Namespace>,
        data: Vec<u8>,
        signer: AccAddress,
        app_version: AppVersion,
    ) -> UniffiResult<Blob> {
        let namespace = Arc::unwrap_or_clone(namespace);
        Ok(Blob::new(namespace, data, Some(signer), app_version)?)
    }

    /// A [`Namespace`] the [`Blob`] belongs to.
    #[uniffi::method(name = "namespace")]
    pub fn get_namespace(&self) -> Namespace {
        self.namespace
    }

    /// Data stored within the [`Blob`].
    #[uniffi::method(name = "data")]
    pub fn get_data(&self) -> Vec<u8> {
        self.data.clone()
    }

    /// Version indicating the format in which [`Share`]s should be created from this [`Blob`].
    #[uniffi::method(name = "share_version")]
    pub fn get_share_version(&self) -> u8 {
        self.share_version
    }

    /// A [`Commitment`] computed from the [`Blob`]s data.
    #[uniffi::method(name = "commitment")]
    pub fn get_commitment(&self) -> Commitment {
        self.commitment
    }

    /// Index of the blob's first share in the EDS. Only set for blobs retrieved from chain.
    #[uniffi::method(name = "index")]
    pub fn get_index(&self) -> Option<u64> {
        self.index
    }

    /// A signer of the blob, i.e. address of the account which submitted the blob.
    ///
    /// Must be present in `share_version 1` and absent otherwise.
    #[uniffi::method(name = "signer")]
    pub fn get_signer(&self) -> Option<AccAddress> {
        self.signer
    }
}

#[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
#[wasm_bindgen]
impl Blob {
    /// Create a new blob with the given data within the [`Namespace`].
    #[wasm_bindgen(constructor)]
    pub fn js_new(
        namespace: &Namespace,
        data: Vec<u8>,
        app_version: &appconsts::JsAppVersion,
    ) -> Result<Blob> {
        Self::new(*namespace, data, None, (*app_version).into())
    }

    /// Clone a blob creating a new deep copy of it.
    #[wasm_bindgen(js_name = clone)]
    pub fn js_clone(&self) -> Blob {
        self.clone()
    }
}

impl From<Blob> for RawBlob {
    fn from(value: Blob) -> RawBlob {
        RawBlob {
            namespace_id: value.namespace.id().to_vec(),
            namespace_version: value.namespace.version() as u32,
            data: value.data,
            share_version: value.share_version as u32,
            signer: value
                .signer
                .map(|addr| addr.as_bytes().to_vec())
                .unwrap_or_default(),
        }
    }
}

fn shares_needed_for_blob(blob_len: usize, has_signer: bool) -> usize {
    let mut first_share_content = appconsts::FIRST_SPARSE_SHARE_CONTENT_SIZE;
    if has_signer {
        first_share_content -= appconsts::SIGNER_SIZE;
    }

    let Some(without_first_share) = blob_len.checked_sub(first_share_content) else {
        return 1;
    };
    1 + without_first_share.div_ceil(appconsts::CONTINUATION_SPARSE_SHARE_CONTENT_SIZE)
}

mod custom_serde {
    use serde::de::Error as _;
    use serde::ser::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use tendermint_proto::serializers::bytes::base64string;

    use crate::nmt::Namespace;
    use crate::state::{AccAddress, AddressTrait};
    use crate::{Error, Result};

    use super::{Blob, Commitment, commitment};

    mod index_serde {
        use super::*;
        /// Serialize [`Option<u64>`] as `i64` with `None` represented as `-1`.
        pub fn serialize<S>(value: &Option<u64>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let x = value
                .map(i64::try_from)
                .transpose()
                .map_err(S::Error::custom)?
                .unwrap_or(-1);
            serializer.serialize_i64(x)
        }

        /// Deserialize [`Option<u64>`] from `i64` with negative values as `None`.
        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
        where
            D: Deserializer<'de>,
        {
            i64::deserialize(deserializer).map(|val| if val >= 0 { Some(val as u64) } else { None })
        }
    }

    mod signer_serde {
        use super::*;

        /// Serialize signer as optional base64 string
        pub fn serialize<S>(value: &Option<AccAddress>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            if let Some(ref addr) = value.as_ref().map(|addr| addr.as_bytes()) {
                base64string::serialize(addr, serializer)
            } else {
                serializer.serialize_none()
            }
        }

        /// Deserialize signer from optional base64 string
        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<AccAddress>, D::Error>
        where
            D: Deserializer<'de>,
        {
            let bytes: Vec<u8> = base64string::deserialize(deserializer)?;
            if bytes.is_empty() {
                Ok(None)
            } else {
                let addr = AccAddress::new(bytes.try_into().map_err(D::Error::custom)?);
                Ok(Some(addr))
            }
        }
    }

    /// This is the copy of the `Blob` struct, to perform additional checks during deserialization
    #[derive(Serialize, Deserialize)]
    pub(super) struct SerdeBlob {
        namespace: Namespace,
        #[serde(with = "base64string")]
        data: Vec<u8>,
        share_version: u8,
        commitment: Commitment,
        // NOTE: celestia supports deserializing blobs without index, so we should too
        #[serde(default, with = "index_serde")]
        index: Option<u64>,
        #[serde(default, with = "signer_serde")]
        signer: Option<AccAddress>,
    }

    impl From<Blob> for SerdeBlob {
        fn from(value: Blob) -> Self {
            Self {
                namespace: value.namespace,
                data: value.data,
                share_version: value.share_version,
                commitment: value.commitment,
                index: value.index,
                signer: value.signer,
            }
        }
    }

    impl TryFrom<SerdeBlob> for Blob {
        type Error = Error;

        fn try_from(value: SerdeBlob) -> Result<Self> {
            // we don't need to require app version when deserializing because commitment is provided
            // user can still verify commitment and app version compatibility using `Blob::validate`
            commitment::validate_blob(value.share_version, value.signer.is_some(), None)?;

            Ok(Blob {
                namespace: value.namespace,
                data: value.data,
                share_version: value.share_version,
                commitment: value.commitment,
                index: value.index,
                signer: value.signer,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nmt::{NS_ID_SIZE, NS_SIZE};
    use crate::test_utils::random_bytes;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as test;

    fn sample_blob() -> Blob {
        serde_json::from_str(
            r#"{
              "namespace": "AAAAAAAAAAAAAAAAAAAAAAAAAAAADCBNOWAP3dM=",
              "data": "8fIMqAB+kQo7+LLmHaDya8oH73hxem6lQWX1",
              "share_version": 0,
              "commitment": "D6YGsPWdxR8ju2OcOspnkgPG2abD30pSHxsFdiPqnVk=",
              "index": -1
            }"#,
        )
        .unwrap()
    }

    fn sample_blob_with_signer() -> Blob {
        serde_json::from_str(
            r#"{
              "namespace": "AAAAAAAAAAAAAAAAAAAAAAAAALwwSWpxCuQb5+A=",
              "data": "lQnnMKE=",
              "share_version": 1,
              "commitment": "dujykaNN+Ey7ET3QNdPG0g2uveriBvZusA3fLSOdMKU=",
              "index": -1,
              "signer": "Yjc3XldhbdYke5i8aSlggYxCCLE="
            }"#,
        )
        .unwrap()
    }

    #[test]
    fn create_new_blob_unsigned() {
        use crate::consts::appconsts;

        let ns = Namespace::new_v0(&[1, 2, 3, 4, 5]).expect("Invalid namespace");
        let blob = Blob::new(
            ns,
            b"some data to store on blockchain".to_vec(),
            None,
            AppVersion::V2,
        )
        .expect("Failed to create blob");

        let shares = blob.to_shares().expect("to_shares failed");
        assert!(!shares.is_empty(), "expected at least one share");

        let first = &shares[0];

        // first share should be a sequence start
        assert!(
            first.sequence_length().is_some(),
            "first share must be a sequence start"
        );

        // check share version and signer
        let version = first.info_byte().expect("non parity share").version();
        assert_eq!(
            version,
            appconsts::SHARE_VERSION_ZERO,
            "unexpected share version for unsigned blob"
        );
        assert_eq!(
            first.signer(),
            None,
            "unsigned blob must not carry a signer"
        );
    }

    #[test]
    fn create_new_blob_signed() {
        use crate::consts::appconsts;

        let ns = Namespace::new_v0(&[1, 2, 3, 4, 5]).expect("Invalid namespace");
        let signer: AccAddress = "celestia1377k5an3f94v6wyaceu0cf4nq6gk2jtpc46g7h"
            .parse()
            .expect("invalid signer");

        // AppVersion must allow signer (>= V3)
        let blob = Blob::new(
            ns,
            b"some data to store on blockchain".to_vec(),
            Some(signer),
            AppVersion::V5,
        )
        .expect("Failed to create signed blob");

        let shares = blob.to_shares().expect("to_shares failed");
        assert!(!shares.is_empty(), "expected at least one share");

        let first = &shares[0];

        // first share should be a sequence start
        assert!(
            first.sequence_length().is_some(),
            "first share must be a sequence start"
        );

        // check share version and signer
        let version = first.info_byte().expect("non parity share").version();
        assert_eq!(
            version,
            appconsts::SHARE_VERSION_ONE,
            "unexpected share version for signed blob"
        );
        assert_eq!(
            first.signer(),
            Some(signer),
            "first share must carry the expected signer"
        );
    }

    #[test]
    fn create_from_raw() {
        let expected = sample_blob();
        let raw = RawBlob::from(expected.clone());
        let created = Blob::from_raw(raw, AppVersion::V2).unwrap();

        assert_eq!(created, expected);
    }

    #[test]
    fn create_from_raw_with_signer() {
        let expected = sample_blob_with_signer();

        let raw = RawBlob::from(expected.clone());

        Blob::from_raw(raw.clone(), AppVersion::V2).unwrap_err();
        let created = Blob::from_raw(raw, AppVersion::V3).unwrap();

        assert_eq!(created, expected);
    }

    #[test]
    fn validate_blob() {
        sample_blob().validate(AppVersion::V2).unwrap();
    }

    #[test]
    fn validate_blob_with_signer() {
        sample_blob_with_signer()
            .validate(AppVersion::V2)
            .unwrap_err();
        sample_blob_with_signer().validate(AppVersion::V3).unwrap();
    }

    #[test]
    fn validate_blob_commitment_mismatch() {
        let mut blob = sample_blob();
        blob.commitment = Commitment::new([7; 32]);

        blob.validate(AppVersion::V2).unwrap_err();
    }

    #[test]
    fn deserialize_blob_with_missing_index() {
        serde_json::from_str::<Blob>(
            r#"{
              "namespace": "AAAAAAAAAAAAAAAAAAAAAAAAAAAADCBNOWAP3dM=",
              "data": "8fIMqAB+kQo7+LLmHaDya8oH73hxem6lQWX1",
              "share_version": 0,
              "commitment": "D6YGsPWdxR8ju2OcOspnkgPG2abD30pSHxsFdiPqnVk="
            }"#,
        )
        .unwrap();
    }

    #[test]
    fn deserialize_blob_with_share_version_and_signer_mismatch() {
        // signer in v0
        serde_json::from_str::<Blob>(
            r#"{
              "namespace": "AAAAAAAAAAAAAAAAAAAAAAAAALwwSWpxCuQb5+A=",
              "data": "lQnnMKE=",
              "share_version": 0,
              "commitment": "dujykaNN+Ey7ET3QNdPG0g2uveriBvZusA3fLSOdMKU=",
              "signer": "Yjc3XldhbdYke5i8aSlggYxCCLE="
            }"#,
        )
        .unwrap_err();

        // no signer in v1
        serde_json::from_str::<Blob>(
            r#"{
              "namespace": "AAAAAAAAAAAAAAAAAAAAAAAAALwwSWpxCuQb5+A=",
              "data": "lQnnMKE=",
              "share_version": 1,
              "commitment": "dujykaNN+Ey7ET3QNdPG0g2uveriBvZusA3fLSOdMKU=",
            }"#,
        )
        .unwrap_err();
    }

    #[test]
    fn reconstruct() {
        for _ in 0..10 {
            let len = rand::random::<usize>() % (1024 * 1024) + 1;
            let data = random_bytes(len);
            let ns = Namespace::const_v0(rand::random());
            let blob = Blob::new(ns, data, None, AppVersion::V2).unwrap();

            let shares = blob.to_shares().unwrap();
            assert_eq!(blob, Blob::reconstruct(&shares, AppVersion::V2).unwrap());
        }
    }

    #[test]
    fn reconstruct_with_signer() {
        for _ in 0..10 {
            let len = rand::random::<usize>() % (1024 * 1024) + 1;
            let data = random_bytes(len);
            let ns = Namespace::const_v0(rand::random());
            let signer = rand::random::<[u8; 20]>().into();

            let blob = Blob::new(ns, data, Some(signer), AppVersion::V3).unwrap();
            let shares = blob.to_shares().unwrap();

            Blob::reconstruct(&shares, AppVersion::V2).unwrap_err();
            assert_eq!(blob, Blob::reconstruct(&shares, AppVersion::V3).unwrap());
        }
    }

    #[test]
    fn reconstruct_empty() {
        assert!(matches!(
            Blob::reconstruct(&Vec::<Share>::new(), AppVersion::V2),
            Err(Error::MissingShares)
        ));
    }

    #[test]
    fn reconstruct_not_sequence_start() {
        let len = rand::random::<usize>() % (1024 * 1024) + 1;
        let data = random_bytes(len);
        let ns = Namespace::const_v0(rand::random());
        let mut shares = Blob::new(ns, data, None, AppVersion::V2)
            .unwrap()
            .to_shares()
            .unwrap();

        // modify info byte to remove sequence start bit
        shares[0].as_mut()[NS_SIZE] &= 0b11111110;

        assert!(matches!(
            Blob::reconstruct(&shares, AppVersion::V2),
            Err(Error::ExpectedShareWithSequenceStart)
        ));
    }

    #[test]
    fn reconstruct_reserved_namespace() {
        for ns in (0..255).flat_map(|n| {
            let mut v0 = [0; NS_ID_SIZE];
            *v0.last_mut().unwrap() = n;
            let mut v255 = [0xff; NS_ID_SIZE];
            *v255.last_mut().unwrap() = n;

            [Namespace::new_v0(&v0), Namespace::new_v255(&v255)]
        }) {
            let len = (rand::random::<usize>() % 1023 + 1) * 2;
            let data = random_bytes(len);
            let shares = Blob::new(ns.unwrap(), data, None, AppVersion::V2)
                .unwrap()
                .to_shares()
                .unwrap();

            assert!(matches!(
                Blob::reconstruct(&shares, AppVersion::V2),
                Err(Error::UnexpectedReservedNamespace)
            ));
        }
    }

    #[test]
    fn reconstruct_not_enough_shares() {
        let len = rand::random::<usize>() % 1024 * 1024 + 2048;
        let data = random_bytes(len);
        let ns = Namespace::const_v0(rand::random());
        let shares = Blob::new(ns, data, None, AppVersion::V2)
            .unwrap()
            .to_shares()
            .unwrap();

        assert!(matches!(
            // minimum for len is 4 so 3 will break stuff
            Blob::reconstruct(&shares[..2], AppVersion::V2),
            Err(Error::MissingShares)
        ));
    }

    #[test]
    fn reconstruct_inconsistent_share_version() {
        let len = rand::random::<usize>() % (1024 * 1024) + 512;
        let data = random_bytes(len);
        let ns = Namespace::const_v0(rand::random());
        let mut shares = Blob::new(ns, data, None, AppVersion::V2)
            .unwrap()
            .to_shares()
            .unwrap();

        // change share version in second share
        shares[1].as_mut()[NS_SIZE] = 0b11111110;

        assert!(matches!(
            Blob::reconstruct(&shares, AppVersion::V2),
            Err(Error::BlobSharesMetadataMismatch(..))
        ));
    }

    #[test]
    fn reconstruct_inconsistent_namespace() {
        let len = rand::random::<usize>() % (1024 * 1024) + 512;
        let data = random_bytes(len);
        let ns = Namespace::const_v0(rand::random());
        let ns2 = Namespace::const_v0(rand::random());
        let mut shares = Blob::new(ns, data, None, AppVersion::V2)
            .unwrap()
            .to_shares()
            .unwrap();

        // change namespace in second share
        shares[1].as_mut()[..NS_SIZE].copy_from_slice(ns2.as_bytes());

        assert!(matches!(
            Blob::reconstruct(&shares, AppVersion::V2),
            Err(Error::BlobSharesMetadataMismatch(..))
        ));
    }

    #[test]
    fn reconstruct_unexpected_sequence_start() {
        let len = rand::random::<usize>() % (1024 * 1024) + 512;
        let data = random_bytes(len);
        let ns = Namespace::const_v0(rand::random());
        let mut shares = Blob::new(ns, data, None, AppVersion::V2)
            .unwrap()
            .to_shares()
            .unwrap();

        // modify info byte to add sequence start bit
        shares[1].as_mut()[NS_SIZE] |= 0b00000001;

        assert!(matches!(
            Blob::reconstruct(&shares, AppVersion::V2),
            Err(Error::UnexpectedSequenceStart)
        ));
    }

    #[test]
    fn reconstruct_all() {
        let blobs: Vec<_> = (0..rand::random::<usize>() % 16 + 3)
            .map(|_| {
                let len = rand::random::<usize>() % (1024 * 1024) + 512;
                let data = random_bytes(len);
                let ns = Namespace::const_v0(rand::random());
                Blob::new(ns, data, None, AppVersion::V2).unwrap()
            })
            .collect();

        let shares: Vec<_> = blobs
            .iter()
            .flat_map(|blob| blob.to_shares().unwrap())
            .collect();
        let reconstructed = Blob::reconstruct_all(&shares, AppVersion::V2).unwrap();

        assert_eq!(blobs, reconstructed);
    }
}
