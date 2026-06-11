use crate::consts::appconsts;

/// Alias for a `Result` with the error type [`celestia_types::Error`].
///
/// [`celestia_types::Error`]: crate::Error
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// `Result` counterpart for functions exposed to uniffi with [`UniffiError`]
///
/// [`UniffiError`]: crate::error::UniffiError
#[cfg(feature = "uniffi")]
pub type UniffiResult<T> = std::result::Result<T, UniffiError>;

/// Representation of all the errors that can occur when interacting with [`celestia_types`].
///
/// [`celestia_types`]: crate
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Unsupported namespace version.
    #[error("Unsupported namespace version: {0}")]
    UnsupportedNamespaceVersion(u8),

    /// Invalid namespace size.
    #[error("Invalid namespace size")]
    InvalidNamespaceSize,

    /// Error propagated from the [`tendermint`].
    #[error(transparent)]
    Tendermint(#[from] tendermint::Error),

    /// Data length different than expected.
    #[error("Length ({0}) different than expected ({1})")]
    InvalidLength(usize, usize),

    /// Error propagated from the [`tendermint_proto`].
    #[error(transparent)]
    Protobuf(#[from] tendermint_proto::Error),

    /// Error propagated from the [`leopard_codec`].
    #[error(transparent)]
    LeopardCodec(#[from] leopard_codec::LeopardError),

    /// Missing header.
    #[error("Missing header")]
    MissingHeader,

    /// Missing commit.
    #[error("Missing commit")]
    MissingCommit,

    /// Missing validator set.
    #[error("Missing validator set")]
    MissingValidatorSet,

    /// Missing data availability header.
    #[error("Missing data availability header")]
    MissingDataAvailabilityHeader,

    /// Missing proof.
    #[error("Missing proof")]
    MissingProof,

    /// Missing shares.
    #[error("Missing shares")]
    MissingShares,

    /// Missing fee field
    #[error("Missing fee field")]
    MissingFee,

    /// Missing sum field
    #[error("Missing sum field")]
    MissingSum,

    /// Missing mode info field
    #[error("Missing mode info field")]
    MissingModeInfo,

    /// Missing bitarray field
    #[error("Missing bitarray field")]
    MissingBitarray,

    /// Bit array too large
    #[error("Bit array to large")]
    BitarrayTooLarge,

    /// Malformed CompactBitArray
    #[error("CompactBitArray malformed")]
    MalformedCompactBitArray,

    /// Wrong proof type.
    #[error("Wrong proof type")]
    WrongProofType,

    /// Unsupported share version.
    #[error("Unsupported share version: {0}")]
    UnsupportedShareVersion(u8),

    /// Invalid share size.
    #[error("Invalid share size: {0}")]
    InvalidShareSize(usize),

    /// Invalid nmt leaf size.
    #[error("Invalid nmt leaf size: {0}")]
    InvalidNmtLeafSize(usize),

    /// Invalid nmt node order.
    #[error("Invalid nmt node order")]
    InvalidNmtNodeOrder,

    /// Share sequence length exceeded.
    #[error(
        "Sequence len must fit into {len} bytes, got value {0}",
        len = appconsts::SEQUENCE_LEN_BYTES
    )]
    ShareSequenceLenExceeded(usize),

    /// Invalid namespace in version 0.
    #[error("Invalid namespace v0")]
    InvalidNamespaceV0,

    /// Invalid namespace in version 255.
    #[error("Invalid namespace v255")]
    InvalidNamespaceV255,

    /// Invalid namespaced hash.
    #[error(transparent)]
    InvalidNamespacedHash(#[from] nmt_rs::InvalidNamespacedHash),

    /// Invalid index of signature in commit.
    #[error("Invalid index of signature in commit {0}, height {1}")]
    InvalidSignatureIndex(usize, u64),

    /// Invalid axis.
    #[error("Invalid axis type: {0}")]
    InvalidAxis(i32),

    /// Invalid Shwap proof type in Protobuf.
    #[error("Invalid proof type: {0}")]
    InvalidShwapProofType(i32),

    /// Could not deserialise Public Key
    #[error("Could not deserialize public key")]
    InvalidPublicKey,

    /// Range proof verification error.
    #[error("Range proof verification failed: {0:?}")]
    RangeProofError(nmt_rs::simple_merkle::error::RangeProofError),

    /// Row root computed from shares doesn't match one received in `DataAvailabilityHeaderz
    #[error("Computed root doesn't match received one")]
    RootMismatch,

    /// Unexpected signature in absent commit.
    #[error("Unexpected absent commit signature")]
    UnexpectedAbsentSignature,

    /// Error that happened during validation.
    #[error("Validation error: {0}")]
    Validation(#[from] ValidationError),

    /// Error that happened during verification.
    #[error("Verification error: {0}")]
    Verification(#[from] VerificationError),

    /// Max share version exceeded.
    #[error(
        "Share version has to be at most {ver}, got {0}",
        ver = appconsts::MAX_SHARE_VERSION
    )]
    MaxShareVersionExceeded(u8),

    /// An error related to the namespaced merkle tree.
    #[error("Nmt error: {0}")]
    Nmt(&'static str),

    /// Invalid address bech32 prefix.
    #[error("Invalid address prefix: {0}")]
    InvalidAddressPrefix(String),

    /// Invalid size of the address.
    #[error("Invalid address size: {0}")]
    InvalidAddressSize(usize),

    /// Invalid address.
    #[error("Invalid address: {0}")]
    InvalidAddress(String),

    /// Invalid coin amount.
    #[error("Invalid coin amount: {0}")]
    InvalidCoinAmount(String),

    /// Invalid coin denomination.
    #[error("Invalid coin denomination: {0}")]
    InvalidCoinDenomination(String),

    /// Invalid balance
    #[error("Invalid balance: {0:?}")]
    InvalidBalance(String),

    /// Invalid Public Key
    #[error("Invalid Public Key")]
    InvalidPublicKeyType(String),

    /// Unsupported fraud proof type.
    #[error("Unsupported fraud proof type: {0}")]
    UnsupportedFraudProofType(String),

    /// Data square index out of range.
    #[error("Index ({0}) out of range ({1})")]
    IndexOutOfRange(usize, usize),

    /// Data square index out of range.
    #[error("Data square index out of range. row: {0}, column: {1}")]
    EdsIndexOutOfRange(u16, u16),

    /// Could not create EDS, provided number of shares doesn't form a pefect square.
    #[error("Invalid dimensions of EDS")]
    EdsInvalidDimentions,

    /// Zero block height.
    #[error("Invalid zero block height")]
    ZeroBlockHeight,

    /// Expected first share of a blob
    #[error("Expected first share of a blob")]
    ExpectedShareWithSequenceStart,

    /// Unexpected share from reserved namespace.
    #[error("Unexpected share from reserved namespace")]
    UnexpectedReservedNamespace,

    /// Unexpected start of a new blob
    #[error("Unexpected start of a new blob")]
    UnexpectedSequenceStart,

    /// Metadata mismatch between shares in blob.
    #[error("Metadata mismatch between shares in blob: {0}")]
    BlobSharesMetadataMismatch(String),

    /// Blob too large, length must fit u32
    #[error("Blob too large")]
    BlobTooLarge,

    /// NamespaceData too large, length must fit u16
    #[error("NamespaceData too large")]
    NamespaceDataTooLarge,

    /// Invalid comittment length
    #[error("Invalid committment length")]
    InvalidComittmentLength,

    /// Missing signer field in blob with share version 1
    #[error("Missing signer field in blob")]
    MissingSigner,

    /// Signer is not supported in share version 0
    #[error("Signer is not supported in share version 0")]
    SignerNotSupported,

    /// Empty blob list provided when creating MsgPayForBlobs
    #[error("Empty blob list")]
    EmptyBlobList,

    /// Missing delegation response
    #[error("Missing belegation response")]
    MissingDelegationResponse,

    /// Missing delegation
    #[error("Missing delegation")]
    MissingDelegation,

    /// Missing balance
    #[error("Missing balance")]
    MissingBalance,

    /// Missing unbond
    #[error("Missing unbond")]
    MissingUnbond,

    /// Missing completion time
    #[error("Missing completion time")]
    MissingCompletionTime,

    /// Missing redelegation
    #[error("Missing redelegation")]
    MissingRedelegation,

    /// Missing redelegation entry
    #[error("Missing redelegation entry")]
    MissingRedelegationEntry,

    /// Invalid Cosmos decimal
    #[error("Invalid Cosmos decimal: {0}")]
    InvalidCosmosDecimal(String),
}

impl From<prost::DecodeError> for Error {
    fn from(value: prost::DecodeError) -> Self {
        Error::Protobuf(tendermint_proto::Error::decode_message(value))
    }
}

#[cfg(all(feature = "wasm-bindgen", target_arch = "wasm32"))]
impl From<Error> for wasm_bindgen::JsValue {
    fn from(value: Error) -> Self {
        js_sys::Error::new(&value.to_string()).into()
    }
}

/// Representation of the errors that can occur when validating data.
///
/// See [`ValidateBasic`]
///
/// [`ValidateBasic`]: crate::ValidateBasic
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    /// Not enough voting power for a commit.
    #[error("Not enought voting power (got {0}, needed {1})")]
    NotEnoughVotingPower(u64, u64),

    /// Other errors that can happen during validation.
    #[error("{0}")]
    Other(String),
}

/// Representation of the errors that can occur when verifying data.
///
/// See [`ExtendedHeader::verify`].
///
/// [`ExtendedHeader::verify`]: crate::ExtendedHeader::verify
#[derive(Debug, thiserror::Error)]
pub enum VerificationError {
    /// Not enough voting power for a commit.
    #[error("Not enought voting power (got {0}, needed {1})")]
    NotEnoughVotingPower(u64, u64),

    /// Other errors that can happen during verification.
    #[error("{0}")]
    Other(String),
}

macro_rules! validation_error {
    ($fmt:literal $(,)?) => {
        $crate::ValidationError::Other(std::format!($fmt))
    };
    ($fmt:literal, $($arg:tt)*) => {
        $crate::ValidationError::Other(std::format!($fmt, $($arg)*))
    };
}

macro_rules! bail_validation {
    ($($arg:tt)*) => {
        return Err($crate::validation_error!($($arg)*).into())
    };
}

macro_rules! verification_error {
    ($fmt:literal $(,)?) => {
        $crate::VerificationError::Other(std::format!($fmt))
    };
    ($fmt:literal, $($arg:tt)*) => {
        $crate::VerificationError::Other(std::format!($fmt, $($arg)*))
    };
}

macro_rules! bail_verification {
    ($($arg:tt)*) => {
        return Err($crate::verification_error!($($arg)*).into())
    };
}

// NOTE: This need to be always after the macro definitions
pub(crate) use bail_validation;
pub(crate) use bail_verification;
pub(crate) use validation_error;
pub(crate) use verification_error;

/// Errors raised when converting from uniffi helper types
#[cfg(feature = "uniffi")]
#[derive(Debug, uniffi::Error, thiserror::Error)]
pub enum UniffiConversionError {
    /// Invalid namespace length
    #[error("Invalid namespace length")]
    InvalidNamespaceLength,

    /// Invalid commitment length
    #[error("Invalid commitment hash length")]
    InvalidCommitmentLength,

    /// Invalid account id length
    #[error("Invalid account id length")]
    InvalidAccountIdLength,

    /// Invalid hash length
    #[error("Invalid hash length")]
    InvalidHashLength,

    /// Invalid chain ID lenght
    #[error("Invalid chain id length")]
    InvalidChainIdLength,

    /// Invalid public key
    #[error("Invalid public key")]
    InvalidPublicKey,

    /// Invalid parts header
    #[error("Invalid parts header {msg}")]
    InvalidPartsHeader {
        /// error message
        msg: String,
    },

    /// Timestamp out of range
    #[error("Timestamp out of range")]
    TimestampOutOfRange,

    /// Header height out of range
    #[error("Header heigth out of range")]
    HeaderHeightOutOfRange,

    /// Invalid signature length
    #[error("Invalid signature length")]
    InvalidSignatureLength,

    /// Invalid round index
    #[error("Invalid round index")]
    InvalidRoundIndex,

    /// Invalid validator index
    #[error("Invalid validator index")]
    InvalidValidatorIndex,

    /// Invalid voting power
    #[error("Voting power out of range")]
    InvalidVotingPower,

    /// Invalid signed header
    #[error("Invalid signed header")]
    InvalidSignedHeader,

    /// Could not generate commitment
    #[error("Could not generate commitment")]
    CouldNotGenerateCommitment {
        /// error message
        msg: String,
    },

    /// Invalid address
    #[error("Invalid address")]
    InvalidAddress {
        /// error message
        msg: String,
    },
}

/// Representation of all the errors that can occur when interacting with [`celestia_types`].
///
/// [`celestia_types`]: crate
#[cfg(feature = "uniffi")]
#[derive(Debug, uniffi::Error, thiserror::Error)]
pub enum UniffiError {
    /// Unsupported namespace version.
    #[error("Unsupported namespace version: {0}")]
    UnsupportedNamespaceVersion(u8),

    /// Invalid namespace size.
    #[error("Invalid namespace size")]
    InvalidNamespaceSize,

    /// Error propagated from the [`tendermint`].
    #[error("Tendermint error: {0}")]
    Tendermint(String),

    /// Error propagated from the [`tendermint_proto`].
    #[error("tendermint_proto error: {0}")]
    Protobuf(String),

    /// Data length different than expected.
    #[error("Length ({0}) different than expected ({1})")]
    InvalidLength(u64, u64),

    /// Error propagated from the [`leopard_codec`].
    #[error("Leopard codec error: {0}")]
    LeopardCodec(String),

    /// Missing header.
    #[error("Missing header")]
    MissingHeader,

    /// Missing commit.
    #[error("Missing commit")]
    MissingCommit,

    /// Missing validator set.
    #[error("Missing validator set")]
    MissingValidatorSet,

    /// Missing data availability header.
    #[error("Missing data availability header")]
    MissingDataAvailabilityHeader,

    /// Missing proof.
    #[error("Missing proof")]
    MissingProof,

    /// Missing shares.
    #[error("Missing shares")]
    MissingShares,

    /// Missing fee field
    #[error("Missing fee field")]
    MissingFee,

    /// Missing sum field
    #[error("Missing sum field")]
    MissingSum,

    /// Missing mode info field
    #[error("Missing mode info field")]
    MissingModeInfo,

    /// Missing bitarray field
    #[error("Missing bitarray field")]
    MissingBitarray,

    /// Bit array too large
    #[error("Bit array to large")]
    BitarrayTooLarge,

    /// Malformed CompactBitArray
    #[error("CompactBitArray malformed")]
    MalformedCompactBitArray,

    /// Wrong proof type.
    #[error("Wrong proof type")]
    WrongProofType,

    /// Unsupported share version.
    #[error("Unsupported share version: {0}")]
    UnsupportedShareVersion(u8),

    /// Invalid share size.
    #[error("Invalid share size: {0}")]
    InvalidShareSize(u64),

    /// Invalid nmt leaf size.
    #[error("Invalid nmt leaf size: {0}")]
    InvalidNmtLeafSize(u64),

    /// Invalid nmt node order.
    #[error("Invalid nmt node order")]
    InvalidNmtNodeOrder,

    /// Share sequence length exceeded.
    #[error(
        "Sequence len must fit into {len} bytes, got value {0}",
        len = appconsts::SEQUENCE_LEN_BYTES
    )]
    ShareSequenceLenExceeded(u64),

    /// Invalid namespace in version 0.
    #[error("Invalid namespace v0")]
    InvalidNamespaceV0,

    /// Invalid namespace in version 255.
    #[error("Invalid namespace v255")]
    InvalidNamespaceV255,

    /// Invalid namespaced hash.
    #[error("Invalid namespace hash: {0}")]
    InvalidNamespacedHash(String),

    /// Invalid index of signature in commit.
    #[error("Invalid index of signature in commit {0}, height {1}")]
    InvalidSignatureIndex(u64, u64),

    /// Invalid axis.
    #[error("Invalid axis type: {0}")]
    InvalidAxis(i32),

    /// Invalid Shwap proof type in Protobuf.
    #[error("Invalid proof type: {0}")]
    InvalidShwapProofType(i32),

    /// Could not deserialise Public Key
    #[error("Could not deserialize public key")]
    InvalidPublicKey,

    /// Range proof verification error.
    #[error("Range proof verification failed: {0:?}")]
    RangeProofError(String),

    /// Row root computed from shares doesn't match one received in `DataAvailabilityHeaderz
    #[error("Computed root doesn't match received one")]
    RootMismatch,

    /// Unexpected signature in absent commit.
    #[error("Unexpected absent commit signature")]
    UnexpectedAbsentSignature,

    /// Error that happened during validation.
    #[error("Validation error: {0}")]
    Validation(String),

    /// Error that happened during verification.
    #[error("Verification error: {0}")]
    Verification(String),

    /// Max share version exceeded.
    #[error(
        "Share version has to be at most {ver}, got {0}",
        ver = appconsts::MAX_SHARE_VERSION
    )]
    MaxShareVersionExceeded(u8),

    /// An error related to the namespaced merkle tree.
    #[error("Nmt error: {0}")]
    Nmt(String),

    /// Invalid address bech32 prefix.
    #[error("Invalid address prefix: {0}")]
    InvalidAddressPrefix(String),

    /// Invalid size of the address.
    #[error("Invalid address size: {0}")]
    InvalidAddressSize(u64),

    /// Invalid address.
    #[error("Invalid address: {0}")]
    InvalidAddress(String),

    /// Invalid coin amount.
    #[error("Invalid coin amount: {0}")]
    InvalidCoinAmount(String),

    /// Invalid coin denomination.
    #[error("Invalid coin denomination: {0}")]
    InvalidCoinDenomination(String),

    /// Invalid balance
    #[error("Invalid balance")]
    InvalidBalance(String),

    /// Invalid Public Key
    #[error("Invalid Public Key")]
    InvalidPublicKeyType(String),

    /// Unsupported fraud proof type.
    #[error("Unsupported fraud proof type: {0}")]
    UnsupportedFraudProofType(String),

    /// Data square index out of range.
    #[error("Index ({0}) out of range ({1})")]
    IndexOutOfRange(u64, u64),

    /// Data square index out of range.
    #[error("Data square index out of range. row: {0}, column: {1}")]
    EdsIndexOutOfRange(u16, u16),

    /// Could not create EDS, provided number of shares doesn't form a pefect square.
    #[error("Invalid dimensions of EDS")]
    EdsInvalidDimentions,

    /// Zero block height.
    #[error("Invalid zero block height")]
    ZeroBlockHeight,

    /// Expected first share of a blob
    #[error("Expected first share of a blob")]
    ExpectedShareWithSequenceStart,

    /// Unexpected share from reserved namespace.
    #[error("Unexpected share from reserved namespace")]
    UnexpectedReservedNamespace,

    /// Unexpected start of a new blob
    #[error("Unexpected start of a new blob")]
    UnexpectedSequenceStart,

    /// Metadata mismatch between shares in blob.
    #[error("Metadata mismatch between shares in blob: {0}")]
    BlobSharesMetadataMismatch(String),

    /// Blob too large, length must fit u32
    #[error("Blob too large")]
    BlobTooLarge,

    /// NamespaceData too large, length must fit u16
    #[error("NamespaceData too large")]
    NamespaceDataTooLarge,

    /// Invalid comittment length
    #[error("Invalid committment length")]
    InvalidComittmentLength,

    /// Missing signer field in blob with share version 1
    #[error("Missing signer field in blob")]
    MissingSigner,

    /// Signer is not supported in share version 0
    #[error("Signer is not supported in share version 0")]
    SignerNotSupported,

    /// Empty blob list provided when creating MsgPayForBlobs
    #[error("Empty blob list")]
    EmptyBlobList,

    /// Missing DelegationResponse
    #[error("Missing DelegationResponse")]
    MissingDelegationResponse,

    /// Missing Delegation
    #[error("Missing Delegation")]
    MissingDelegation,

    /// Missing Balance
    #[error("Missing Balance")]
    MissingBalance,

    /// Missing unbond
    #[error("Missing unbond")]
    MissingUnbond,

    /// Missing completion time
    #[error("Missing completion time")]
    MissingCompletionTime,

    /// Missing redelegation
    #[error("Missing redelegation")]
    MissingRedelegation,

    /// Missing redelegation entry
    #[error("Missing redelegation entry")]
    MissingRedelegationEntry,

    /// Invalid Cosmos decimal
    #[error("Invalid Cosmos decimal: {0}")]
    InvalidCosmosDecimal(String),
}

#[cfg(feature = "uniffi")]
impl From<Error> for UniffiError {
    fn from(value: Error) -> Self {
        match value {
            Error::UnsupportedNamespaceVersion(v) => UniffiError::UnsupportedNamespaceVersion(v),
            Error::InvalidNamespaceSize => UniffiError::InvalidNamespaceSize,
            Error::Tendermint(e) => UniffiError::Tendermint(e.to_string()),
            Error::Protobuf(e) => UniffiError::Protobuf(e.to_string()),
            Error::InvalidLength(recv, expected) => {
                UniffiError::InvalidLength(recv as u64, expected as u64)
            }
            Error::LeopardCodec(e) => UniffiError::LeopardCodec(e.to_string()),
            Error::MissingHeader => UniffiError::MissingHeader,
            Error::MissingCommit => UniffiError::MissingCommit,
            Error::MissingValidatorSet => UniffiError::MissingValidatorSet,
            Error::MissingDataAvailabilityHeader => UniffiError::MissingDataAvailabilityHeader,
            Error::MissingProof => UniffiError::MissingProof,
            Error::MissingShares => UniffiError::MissingShares,
            Error::MissingFee => UniffiError::MissingFee,
            Error::MissingSum => UniffiError::MissingSum,
            Error::MissingModeInfo => UniffiError::MissingModeInfo,
            Error::MissingBitarray => UniffiError::MissingBitarray,
            Error::BitarrayTooLarge => UniffiError::BitarrayTooLarge,
            Error::MalformedCompactBitArray => UniffiError::MalformedCompactBitArray,
            Error::WrongProofType => UniffiError::WrongProofType,
            Error::UnsupportedShareVersion(v) => UniffiError::UnsupportedShareVersion(v),
            Error::InvalidShareSize(s) => {
                UniffiError::InvalidShareSize(s.try_into().expect("size to fit"))
            }
            Error::InvalidNmtLeafSize(s) => {
                UniffiError::InvalidNmtLeafSize(s.try_into().expect("size to fit"))
            }
            Error::InvalidNmtNodeOrder => UniffiError::InvalidNmtNodeOrder,
            Error::ShareSequenceLenExceeded(l) => {
                UniffiError::ShareSequenceLenExceeded(l.try_into().expect("length to fit"))
            }
            Error::InvalidNamespaceV0 => UniffiError::InvalidNamespaceV0,
            Error::InvalidNamespaceV255 => UniffiError::InvalidNamespaceV255,
            Error::InvalidNamespacedHash(h) => UniffiError::InvalidNamespacedHash(h.to_string()),
            Error::InvalidSignatureIndex(i, h) => {
                UniffiError::InvalidSignatureIndex(i.try_into().expect("index to fit"), h)
            }
            Error::InvalidAxis(a) => UniffiError::InvalidAxis(a),
            Error::InvalidShwapProofType(t) => UniffiError::InvalidShwapProofType(t),
            Error::InvalidPublicKey => UniffiError::InvalidPublicKey,
            Error::RangeProofError(e) => UniffiError::RangeProofError(format!("{e:?}")),
            Error::RootMismatch => UniffiError::RootMismatch,
            Error::UnexpectedAbsentSignature => UniffiError::UnexpectedAbsentSignature,
            Error::Validation(e) => UniffiError::Validation(e.to_string()),
            Error::Verification(e) => UniffiError::Verification(e.to_string()),
            Error::MaxShareVersionExceeded(v) => UniffiError::MaxShareVersionExceeded(v),
            Error::Nmt(e) => UniffiError::Nmt(e.to_string()),
            Error::InvalidAddressPrefix(p) => UniffiError::InvalidAddressPrefix(p),
            Error::InvalidAddressSize(s) => {
                UniffiError::InvalidAddressSize(s.try_into().expect("size to fit"))
            }
            Error::InvalidAddress(a) => UniffiError::InvalidAddress(a),
            Error::InvalidCoinAmount(a) => UniffiError::InvalidCoinAmount(a),
            Error::InvalidCoinDenomination(d) => UniffiError::InvalidCoinDenomination(d),
            Error::InvalidBalance(b) => UniffiError::InvalidBalance(b),
            Error::InvalidPublicKeyType(t) => UniffiError::InvalidPublicKeyType(t),
            Error::UnsupportedFraudProofType(t) => UniffiError::UnsupportedFraudProofType(t),
            Error::IndexOutOfRange(i, r) => UniffiError::IndexOutOfRange(
                i.try_into().expect("index to fit"),
                r.try_into().expect("range to fit"),
            ),
            Error::EdsIndexOutOfRange(r, c) => UniffiError::EdsIndexOutOfRange(r, c),
            Error::EdsInvalidDimentions => UniffiError::EdsInvalidDimentions,
            Error::ZeroBlockHeight => UniffiError::ZeroBlockHeight,
            Error::ExpectedShareWithSequenceStart => UniffiError::ExpectedShareWithSequenceStart,
            Error::UnexpectedReservedNamespace => UniffiError::UnexpectedReservedNamespace,
            Error::UnexpectedSequenceStart => UniffiError::UnexpectedSequenceStart,
            Error::BlobSharesMetadataMismatch(s) => UniffiError::BlobSharesMetadataMismatch(s),
            Error::BlobTooLarge => UniffiError::BlobTooLarge,
            Error::NamespaceDataTooLarge => UniffiError::NamespaceDataTooLarge,
            Error::InvalidComittmentLength => UniffiError::InvalidComittmentLength,
            Error::MissingSigner => UniffiError::MissingSigner,
            Error::SignerNotSupported => UniffiError::SignerNotSupported,
            Error::EmptyBlobList => UniffiError::EmptyBlobList,
            Error::MissingDelegationResponse => UniffiError::MissingDelegationResponse,
            Error::MissingDelegation => UniffiError::MissingDelegation,
            Error::MissingBalance => UniffiError::MissingBalance,
            Error::MissingUnbond => UniffiError::MissingUnbond,
            Error::MissingCompletionTime => UniffiError::MissingCompletionTime,
            Error::MissingRedelegation => UniffiError::MissingRedelegation,
            Error::MissingRedelegationEntry => UniffiError::MissingRedelegationEntry,
            Error::InvalidCosmosDecimal(s) => UniffiError::InvalidCosmosDecimal(s),
        }
    }
}
