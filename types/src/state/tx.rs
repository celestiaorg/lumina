use std::fmt;

use celestia_proto::cosmos::base::abci::v1beta1::AbciMessageLog;
use serde::{Deserialize, Serialize};
use serde_repr::Deserialize_repr;
use serde_repr::Serialize_repr;
use tendermint_proto::Protobuf;
use tendermint_proto::google::protobuf::Any;
use tendermint_proto::v0_38::abci::Event;
#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
use wasm_bindgen::prelude::*;

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
use crate::any::JsAny;
use crate::bail_validation;
use crate::hash::Hash;
#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
use crate::signature::JsSignature;
use crate::state::bit_array::BitVector;
#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
use crate::state::bit_array::JsBitVector;
use crate::state::{Address, Coin};
use crate::{Error, Height};

pub use celestia_proto::cosmos::base::abci::v1beta1::TxResponse as RawTxResponse;
pub use celestia_proto::cosmos::tx::v1beta1::AuthInfo as RawAuthInfo;
pub use celestia_proto::cosmos::tx::v1beta1::Fee as RawFee;
pub use celestia_proto::cosmos::tx::v1beta1::ModeInfo as RawModeInfo;
pub use celestia_proto::cosmos::tx::v1beta1::SignerInfo as RawSignerInfo;
pub use celestia_proto::cosmos::tx::v1beta1::Tx as RawTx;
pub use celestia_proto::cosmos::tx::v1beta1::TxBody as RawTxBody;
pub use celestia_proto::cosmos::tx::v1beta1::mode_info::Sum as RawSum;
pub use celestia_proto::cosmos::tx::v1beta1::mode_info::{Multi, Single};

pub type Signature = Vec<u8>;

/// [`BOND_DENOM`] defines the native staking denomination
pub const BOND_DENOM: &str = "utia";

/// [`Tx`] is the standard type used for broadcasting transactions.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "wasm-bindgen"),
    wasm_bindgen(getter_with_clone)
)]
pub struct Tx {
    /// Processable content of the transaction
    pub body: TxBody,

    /// Authorization related content of the transaction, specifically signers, signer modes
    /// and [`Fee`].
    pub auth_info: AuthInfo,

    /// List of signatures that matches the length and order of [`AuthInfo`]’s `signer_info`s to
    /// allow connecting signature meta information like public key and signing mode by position.
    ///
    /// Signatures are provided as raw bytes so as to support current and future signature types.
    /// [`AuthInfo`] should be introspected to determine the signature algorithm used.
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "wasm-bindgen"),
        wasm_bindgen(skip)
    )]
    pub signatures: Vec<Signature>,
}

/// [`TxBody`] of a transaction that all signers sign over.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "wasm-bindgen"),
    wasm_bindgen(getter_with_clone)
)]
pub struct TxBody {
    /// `messages` is a list of messages to be executed. The required signers of
    /// those messages define the number and order of elements in `AuthInfo`'s
    /// signer_infos and Tx's signatures. Each required signer address is added to
    /// the list only the first time it occurs.
    ///
    /// By convention, the first required signer (usually from the first message)
    /// is referred to as the primary signer and pays the fee for the whole
    /// transaction.
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "wasm-bindgen"),
        wasm_bindgen(skip)
    )]
    pub messages: Vec<Any>,
    /// `memo` is any arbitrary memo to be added to the transaction.
    pub memo: String,
    /// `timeout` is the block height after which this transaction will not
    /// be processed by the chain
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "wasm-bindgen"),
        wasm_bindgen(skip)
    )]
    pub timeout_height: Height,
    /// `extension_options` are arbitrary options that can be added by chains
    /// when the default options are not sufficient. If any of these are present
    /// and can't be handled, the transaction will be rejected
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "wasm-bindgen"),
        wasm_bindgen(skip)
    )]
    pub extension_options: Vec<Any>,
    /// `extension_options` are arbitrary options that can be added by chains
    /// when the default options are not sufficient. If any of these are present
    /// and can't be handled, they will be ignored
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "wasm-bindgen"),
        wasm_bindgen(skip)
    )]
    pub non_critical_extension_options: Vec<Any>,
}

/// Response to a tx query
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "wasm-bindgen"),
    wasm_bindgen(getter_with_clone)
)]
pub struct TxResponse {
    /// The block height
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "wasm-bindgen"),
        wasm_bindgen(skip)
    )]
    pub height: Height,

    /// The transaction hash.
    #[serde(with = "crate::serializers::hash")]
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "wasm-bindgen"),
        wasm_bindgen(skip)
    )]
    pub txhash: Hash,

    /// Namespace for the Code
    pub codespace: String,

    /// Response code.
    pub code: ErrorCode,

    /// Result bytes, if any.
    pub data: String,

    /// The output of the application's logger (raw string). May be
    /// non-deterministic.
    pub raw_log: String,

    /// The output of the application's logger (typed). May be non-deterministic.
    pub logs: Vec<AbciMessageLog>,

    /// Additional information. May be non-deterministic.
    pub info: String,

    /// Amount of gas requested for transaction.
    pub gas_wanted: i64,

    /// Amount of gas consumed by transaction.
    pub gas_used: i64,

    /// The request transaction bytes.
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "wasm-bindgen"),
        wasm_bindgen(skip)
    )]
    pub tx: Option<Any>,

    /// Time of the previous block. For heights > 1, it's the weighted median of
    /// the timestamps of the valid votes in the block.LastCommit. For height == 1,
    /// it's genesis time.
    pub timestamp: String,

    /// Events defines all the events emitted by processing a transaction. Note,
    /// these events include those emitted by processing all the messages and those
    /// emitted from the ante. Whereas Logs contains the events, with
    /// additional metadata, emitted only by processing the messages.
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "wasm-bindgen"),
        wasm_bindgen(skip)
    )]
    pub events: Vec<Event>,
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
#[wasm_bindgen]
impl TxResponse {
    /// The block height
    #[wasm_bindgen(getter)]
    pub fn height(&self) -> u64 {
        self.height.value()
    }

    /// Events defines all the events emitted by processing a transaction. Note,
    /// these events include those emitted by processing all the messages and those
    /// emitted from the ante. Whereas Logs contains the events, with
    /// additional metadata, emitted only by processing the messages.
    #[wasm_bindgen(getter)]
    pub fn events(&self) -> Vec<JsEvent> {
        self.events.iter().cloned().map(Into::into).collect()
    }
}

/// [`AuthInfo`] describes the fee and signer modes that are used to sign a transaction.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "wasm-bindgen"),
    wasm_bindgen(getter_with_clone)
)]
pub struct AuthInfo {
    /// Defines the signing modes for the required signers.
    ///
    /// The number and order of elements must match the required signers from transaction
    /// [`TxBody`]’s messages. The first element is the primary signer and the one
    /// which pays the [`Fee`].
    pub signer_infos: Vec<SignerInfo>,
    /// [`Fee`] and gas limit for the transaction.
    ///
    /// The first signer is the primary signer and the one which pays the fee.
    /// The fee can be calculated based on the cost of evaluating the body and doing signature
    /// verification of the signers. This can be estimated via simulation.
    pub fee: Fee,
}

/// SignerInfo describes the public key and signing mode of a single top-level
/// signer.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "wasm-bindgen"),
    wasm_bindgen(getter_with_clone)
)]
pub struct SignerInfo {
    /// public_key is the public key of the signer. It is optional for accounts
    /// that already exist in state. If unset, the verifier can use the required \
    /// signer address for this position and lookup the public key.
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "wasm-bindgen"),
        wasm_bindgen(skip)
    )]
    pub public_key: Option<Any>,
    /// mode_info describes the signing mode of the signer and is a nested
    /// structure to support nested multisig pubkey's
    pub mode_info: ModeInfo,
    /// sequence is the sequence of the account, which describes the
    /// number of committed transactions signed by a given address. It is used to
    /// prevent replay attacks.
    pub sequence: u64,
}

/// ModeInfo describes the signing mode of a single or nested multisig signer.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[cfg_attr(all(target_arch = "wasm32", feature = "wasm-bindgen"), wasm_bindgen)]
pub struct ModeInfo {
    /// sum is the oneof that specifies whether this represents a single or nested
    /// multisig signer
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "wasm-bindgen"),
        wasm_bindgen(skip)
    )]
    pub sum: Sum,
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
#[wasm_bindgen]
impl ModeInfo {
    /// Return signature mode for the stored signature(s)
    pub fn signature_mode(&self) -> SignatureMode {
        match self.sum {
            Sum::Single { .. } => SignatureMode::Single,
            Sum::Multi { .. } => SignatureMode::Multi,
        }
    }

    /// Single is the mode info for a single signer. It is structured as a message
    /// to allow for additional fields such as locale for SIGN_MODE_TEXTUAL in the
    /// future
    #[wasm_bindgen(getter)]
    pub fn mode(&self) -> Option<i32> {
        match self.sum {
            Sum::Single { mode } => Some(mode),
            _ => None,
        }
    }

    /// Multi is the mode info for a multisig public key
    /// bitarray specifies which keys within the multisig are signing
    #[wasm_bindgen(getter)]
    pub fn bitarray(&self) -> Option<JsBitVector> {
        match &self.sum {
            Sum::Multi { bitarray, .. } => Some(bitarray.clone().into()),
            Sum::Single { .. } => None,
        }
    }

    /// Multi is the mode info for a multisig public key
    /// mode_infos is the corresponding modes of the signers of the multisig
    /// which could include nested multisig public keys
    #[wasm_bindgen(getter)]
    pub fn mode_infos(&self) -> Option<Vec<ModeInfo>> {
        match &self.sum {
            Sum::Multi { mode_infos, .. } => Some(mode_infos.clone()),
            Sum::Single { .. } => None,
        }
    }
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
#[wasm_bindgen]
pub enum SignatureMode {
    Single,
    Multi,
}

/// sum is the oneof that specifies whether this represents a single or nested
/// multisig signer
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
pub enum Sum {
    /// Single is the mode info for a single signer. It is structured as a message
    /// to allow for additional fields such as locale for SIGN_MODE_TEXTUAL in the
    /// future
    Single {
        /// mode is the signing mode of the single signer
        mode: i32,
    },
    /// Multi is the mode info for a multisig public key
    Multi {
        /// bitarray specifies which keys within the multisig are signing
        bitarray: BitVector,
        /// mode_infos is the corresponding modes of the signers of the multisig
        /// which could include nested multisig public keys
        mode_infos: Vec<ModeInfo>,
    },
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
#[wasm_bindgen]
impl Tx {
    /// List of signatures that matches the length and order of [`AuthInfo`]’s `signer_info`s to
    /// allow connecting signature meta information like public key and signing mode by position.
    #[wasm_bindgen(getter)]
    pub fn signatures(&self) -> Vec<JsSignature> {
        self.signatures
            .iter()
            .map(|s| JsSignature::from(&s[..]))
            .collect()
    }
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
#[wasm_bindgen]
impl TxBody {
    /// `messages` is a list of messages to be executed. The required signers of
    /// those messages define the number and order of elements in `AuthInfo`'s
    /// signer_infos and Tx's signatures. Each required signer address is added to
    /// the list only the first time it occurs.
    ///
    /// By convention, the first required signer (usually from the first message)
    /// is referred to as the primary signer and pays the fee for the whole
    /// transaction.
    pub fn messages(&self) -> Vec<JsAny> {
        self.messages.iter().cloned().map(Into::into).collect()
    }

    /// `timeout` is the block height after which this transaction will not
    /// be processed by the chain
    #[wasm_bindgen(getter)]
    pub fn timeout_height(&self) -> u64 {
        self.timeout_height.value()
    }

    /// `extension_options` are arbitrary options that can be added by chains
    /// when the default options are not sufficient. If any of these are present
    /// and can't be handled, the transaction will be rejected
    pub fn extension_options(&self) -> Vec<JsAny> {
        self.extension_options
            .iter()
            .cloned()
            .map(Into::into)
            .collect()
    }

    /// `extension_options` are arbitrary options that can be added by chains
    /// when the default options are not sufficient. If any of these are present
    /// and can't be handled, they will be ignored
    pub fn non_critical_extension_options(&self) -> Vec<JsAny> {
        self.non_critical_extension_options
            .iter()
            .cloned()
            .map(Into::into)
            .collect()
    }
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
#[wasm_bindgen]
impl SignerInfo {
    /// public_key is the public key of the signer. It is optional for accounts
    /// that already exist in state. If unset, the verifier can use the required \
    /// signer address for this position and lookup the public key.
    pub fn public_key(&self) -> Option<JsAny> {
        self.public_key.clone().map(Into::into)
    }
}

/// Fee includes the amount of coins paid in fees and the maximum
/// gas to be used by the transaction. The ratio yields an effective "gasprice",
/// which must be above some miminum to be accepted into the mempool.
#[derive(Debug, Clone, PartialEq, Default)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Record))]
#[cfg_attr(
    all(target_arch = "wasm32", feature = "wasm-bindgen"),
    wasm_bindgen(getter_with_clone)
)]
pub struct Fee {
    /// amount is the amount of coins to be paid as a fee
    pub amount: Vec<Coin>,
    /// gas_limit is the maximum gas that can be used in transaction processing
    /// before an out of gas error occurs
    pub gas_limit: u64,
    /// if unset, the first signer is responsible for paying the fees. If set, the specified account must pay the fees.
    /// the payer must be a tx signer (and thus have signed this field in AuthInfo).
    /// setting this field does *not* change the ordering of required signers for the transaction.
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "wasm-bindgen"),
        wasm_bindgen(skip)
    )]
    pub payer: Option<Address>,
    /// if set, the fee payer (either the first signer or the value of the payer field) requests that a fee grant be used
    /// to pay fees instead of the fee payer's own balance. If an appropriate fee grant does not exist or the chain does
    /// not support fee grants, this will fail
    #[cfg_attr(
        all(target_arch = "wasm32", feature = "wasm-bindgen"),
        wasm_bindgen(skip)
    )]
    pub granter: Option<Address>,
}

impl Fee {
    /// Create [`Fee`] struct with provided number of utia and gas limit,
    /// without setting custom [`Fee::payer`] or [`Fee::granter`] fields,
    /// which means first tx signer is responsible for paying.
    pub fn new(utia_fee: u64, gas_limit: u64) -> Self {
        Fee {
            amount: vec![Coin::utia(utia_fee)],
            gas_limit,
            ..Default::default()
        }
    }
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
#[wasm_bindgen]
impl Fee {
    /// if unset, the first signer is responsible for paying the fees. If set, the specified account must pay the fees.
    /// the payer must be a tx signer (and thus have signed this field in AuthInfo).
    /// setting this field does *not* change the ordering of required signers for the transaction.
    #[wasm_bindgen(getter)]
    pub fn payer(&self) -> Option<String> {
        self.payer.as_ref().map(|a| a.to_string())
    }
    /// if set, the fee payer (either the first signer or the value of the payer field) requests that a fee grant be used
    /// to pay fees instead of the fee payer's own balance. If an appropriate fee grant does not exist or the chain does
    /// not support fee grants, this will fail
    #[wasm_bindgen(getter)]
    pub fn granter(&self) -> Option<String> {
        self.granter.as_ref().map(|a| a.to_string())
    }
}

/// Error codes associated with transaction responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize_repr, Deserialize_repr)]
#[repr(u32)]
#[cfg_attr(feature = "uniffi", derive(uniffi::Enum))]
#[cfg_attr(all(target_arch = "wasm32", feature = "wasm-bindgen"), wasm_bindgen)]
pub enum ErrorCode {
    // source https://github.com/celestiaorg/cosmos-sdk/blob/v1.25.1-sdk-v0.46.16/types/errors/errors.go#L38
    /// No error
    Success = 0,
    /// Cannot parse a transaction
    TxDecode = 2,
    /// Sequence number (nonce) is incorrect for the signature
    InvalidSequence = 3,
    /// Request without sufficient authorization is handled
    Unauthorized = 4,
    /// Account cannot pay requested amount
    InsufficientFunds = 5,
    /// Request is unknown
    UnknownRequest = 6,
    /// Address is invalid
    InvalidAddress = 7,
    /// Pubkey is invalid
    InvalidPubKey = 8,
    /// Address is unknown
    UnknownAddress = 9,
    /// Coin is invalid
    InvalidCoins = 10,
    /// Gas exceeded
    OutOfGas = 11,
    /// Memo too large
    MemoTooLarge = 12,
    /// Fee is insufficient
    InsufficientFee = 13,
    /// Too many signatures
    TooManySignatures = 14,
    /// No signatures in transaction
    NoSignatures = 15,
    /// Error converting to json
    JSONMarshal = 16,
    /// Error converting from json
    JSONUnmarshal = 17,
    /// Request contains invalid data
    InvalidRequest = 18,
    /// Tx already exists in the mempool
    TxInMempoolCache = 19,
    /// Mempool is full
    MempoolIsFull = 20,
    /// Tx is too large
    TxTooLarge = 21,
    /// Key doesn't exist
    KeyNotFound = 22,
    /// Key password is invalid
    WrongPassword = 23,
    /// Tx intended signer does not match the given signer
    InvalidSigner = 24,
    /// Invalid gas adjustment
    InvalidGasAdjustment = 25,
    /// Invalid height
    InvalidHeight = 26,
    /// Invalid version
    InvalidVersion = 27,
    /// Chain-id is invalid
    InvalidChainID = 28,
    /// Invalid type
    InvalidType = 29,
    /// Tx rejected due to an explicitly set timeout height
    TxTimeoutHeight = 30,
    /// Unknown extension options.
    UnknownExtensionOptions = 31,
    /// Account sequence defined in the signer info doesn't match the account's actual sequence
    WrongSequence = 32,
    /// Packing a protobuf message to Any failed
    PackAny = 33,
    /// Unpacking a protobuf message from Any failed
    UnpackAny = 34,
    /// Internal logic error, e.g. an invariant or assertion that is violated
    Logic = 35,
    /// Conflict error, e.g. when two goroutines try to access the same resource and one of them fails
    Conflict = 36,
    /// Called a branch of a code which is currently not supported
    NotSupported = 37,
    /// Requested entity doesn't exist in the state
    NotFound = 38,
    /// Internal errors caused by external operation
    IO = 39,
    /// Min-gas-prices field in BaseConfig is empty
    AppConfig = 40,
    /// Invalid GasWanted value is supplied
    InvalidGasLimit = 41,
    /// Node recovered from panic
    Panic = 111222,

    // source https://github.com/celestiaorg/celestia-app/blob/v3.0.2/x/blob/types/errors.go
    /// cannot use reserved namespace IDs
    ReservedNamespace = 11110,
    /// invalid namespace length
    InvalidNamespaceLen = 11111,
    /// data must be multiple of shareSize
    InvalidDataSize = 11112,
    /// actual blob size differs from that specified in the MsgPayForBlob
    BlobSizeMismatch = 11113,
    /// committed to invalid square size: must be power of two
    CommittedSquareSizeNotPowOf2 = 11114,
    /// unexpected error calculating commitment for share
    CalculateCommitment = 11115,
    /// invalid commitment for share
    InvalidShareCommitment = 11116,
    /// cannot use parity shares namespace ID
    ParitySharesNamespace = 11117,
    /// cannot use tail padding namespace ID
    TailPaddingNamespace = 11118,
    /// cannot use transaction namespace ID
    TxNamespace = 11119,
    /// invalid share commitments: all relevant square sizes must be committed to
    InvalidShareCommitments = 11122,
    /// unsupported share version
    UnsupportedShareVersion = 11123,
    /// cannot use zero blob size
    ZeroBlobSize = 11124,
    /// mismatched number of blobs per MsgPayForBlob
    MismatchedNumberOfPFBorBlob = 11125,
    /// no MsgPayForBlobs found in blob transaction
    NoPFB = 11126,
    /// namespace of blob and its respective MsgPayForBlobs differ
    NamespaceMismatch = 11127,
    /// failure to parse a transaction from its protobuf representation
    ProtoParsing = 11128,
    /// not yet supported: multiple sdk.Msgs found in BlobTx
    MultipleMsgsInBlobTx = 11129,
    /// number of each component in a MsgPayForBlobs must be identical
    MismatchedNumberOfPFBComponent = 11130,
    /// no blobs provided
    NoBlobs = 11131,
    /// no namespaces provided
    NoNamespaces = 11132,
    /// no share versions provided
    NoShareVersions = 11133,
    /// no blob sizes provided
    NoBlobSizes = 11134,
    /// no share commitments provided
    NoShareCommitments = 11135,
    /// invalid namespace
    InvalidNamespace = 11136,
    /// invalid namespace version
    InvalidNamespaceVersion = 11137,
    /// total blob size too large
    ///
    /// TotalBlobSize is deprecated, use BlobsTooLarge instead.
    TotalBlobSizeTooLarge = 11138,
    /// blob(s) too large
    BlobsTooLarge = 11139,
    /// invalid blob signer
    InvalidBlobSigner = 11140,
    /// share version of blob and its respective MsgPayForBlobs differ
    ShareVersionMismatch = 11141,
    /// transaction size exceeds maximum allowed limit
    TxExceedsMaxSize = 11142,
    /// transaction exceeds maximum allowed SDK message count
    TxExceedsMaxSDKMessages = 11143,
    /// index wrapper txs must contain a MsgPayForBlobs
    NonPFBIndexWrapper = 11144,
    /// index wrapper txs must contain exactly one message
    MultiMsgIndexWrapper = 11145,
    /// invalid MsgPayForFibre transaction
    InvalidPayForFibreTx = 11146,
    /// blob tx is not canonically encoded
    NonCanonicalBlobTx = 11147,
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{self:?}")
    }
}

impl TryFrom<u32> for ErrorCode {
    type Error = Error;

    fn try_from(value: u32) -> Result<ErrorCode, Self::Error> {
        let error_code = match value {
            // cosmos-sdk
            0 => ErrorCode::Success,
            2 => ErrorCode::TxDecode,
            3 => ErrorCode::InvalidSequence,
            4 => ErrorCode::Unauthorized,
            5 => ErrorCode::InsufficientFunds,
            6 => ErrorCode::UnknownRequest,
            7 => ErrorCode::InvalidAddress,
            8 => ErrorCode::InvalidPubKey,
            9 => ErrorCode::UnknownAddress,
            10 => ErrorCode::InvalidCoins,
            11 => ErrorCode::OutOfGas,
            12 => ErrorCode::MemoTooLarge,
            13 => ErrorCode::InsufficientFee,
            14 => ErrorCode::TooManySignatures,
            15 => ErrorCode::NoSignatures,
            16 => ErrorCode::JSONMarshal,
            17 => ErrorCode::JSONUnmarshal,
            18 => ErrorCode::InvalidRequest,
            19 => ErrorCode::TxInMempoolCache,
            20 => ErrorCode::MempoolIsFull,
            21 => ErrorCode::TxTooLarge,
            22 => ErrorCode::KeyNotFound,
            23 => ErrorCode::WrongPassword,
            24 => ErrorCode::InvalidSigner,
            25 => ErrorCode::InvalidGasAdjustment,
            26 => ErrorCode::InvalidHeight,
            27 => ErrorCode::InvalidVersion,
            28 => ErrorCode::InvalidChainID,
            29 => ErrorCode::InvalidType,
            30 => ErrorCode::TxTimeoutHeight,
            31 => ErrorCode::UnknownExtensionOptions,
            32 => ErrorCode::WrongSequence,
            33 => ErrorCode::PackAny,
            34 => ErrorCode::UnpackAny,
            35 => ErrorCode::Logic,
            36 => ErrorCode::Conflict,
            37 => ErrorCode::NotSupported,
            38 => ErrorCode::NotFound,
            39 => ErrorCode::IO,
            40 => ErrorCode::AppConfig,
            41 => ErrorCode::InvalidGasLimit,
            111222 => ErrorCode::Panic,
            // celestia-app blob
            11110 => ErrorCode::ReservedNamespace,
            11111 => ErrorCode::InvalidNamespaceLen,
            11112 => ErrorCode::InvalidDataSize,
            11113 => ErrorCode::BlobSizeMismatch,
            11114 => ErrorCode::CommittedSquareSizeNotPowOf2,
            11115 => ErrorCode::CalculateCommitment,
            11116 => ErrorCode::InvalidShareCommitment,
            11117 => ErrorCode::ParitySharesNamespace,
            11118 => ErrorCode::TailPaddingNamespace,
            11119 => ErrorCode::TxNamespace,
            11122 => ErrorCode::InvalidShareCommitments,
            11123 => ErrorCode::UnsupportedShareVersion,
            11124 => ErrorCode::ZeroBlobSize,
            11125 => ErrorCode::MismatchedNumberOfPFBorBlob,
            11126 => ErrorCode::NoPFB,
            11127 => ErrorCode::NamespaceMismatch,
            11128 => ErrorCode::ProtoParsing,
            11129 => ErrorCode::MultipleMsgsInBlobTx,
            11130 => ErrorCode::MismatchedNumberOfPFBComponent,
            11131 => ErrorCode::NoBlobs,
            11132 => ErrorCode::NoNamespaces,
            11133 => ErrorCode::NoShareVersions,
            11134 => ErrorCode::NoBlobSizes,
            11135 => ErrorCode::NoShareCommitments,
            11136 => ErrorCode::InvalidNamespace,
            11137 => ErrorCode::InvalidNamespaceVersion,
            11138 => ErrorCode::TotalBlobSizeTooLarge,
            11139 => ErrorCode::BlobsTooLarge,
            11140 => ErrorCode::InvalidBlobSigner,
            11141 => ErrorCode::ShareVersionMismatch,
            // celestia-app
            11142 => ErrorCode::TxExceedsMaxSize,
            11143 => ErrorCode::TxExceedsMaxSDKMessages,
            11144 => ErrorCode::NonPFBIndexWrapper,
            11145 => ErrorCode::MultiMsgIndexWrapper,
            11146 => ErrorCode::InvalidPayForFibreTx,
            11147 => ErrorCode::NonCanonicalBlobTx,
            _ => bail_validation!("error code ({}) unknown", value),
        };
        Ok(error_code)
    }
}

impl TryFrom<RawTxBody> for TxBody {
    type Error = Error;

    fn try_from(value: RawTxBody) -> Result<Self, Self::Error> {
        Ok(TxBody {
            messages: value.messages,
            memo: value.memo,
            timeout_height: value.timeout_height.try_into()?,
            extension_options: value.extension_options,
            non_critical_extension_options: value.non_critical_extension_options,
        })
    }
}

impl From<TxBody> for RawTxBody {
    fn from(value: TxBody) -> Self {
        RawTxBody {
            messages: value.messages,
            memo: value.memo,
            timeout_height: value.timeout_height.into(),
            extension_options: value.extension_options,
            non_critical_extension_options: value.non_critical_extension_options,
        }
    }
}

impl TryFrom<RawAuthInfo> for AuthInfo {
    type Error = Error;

    fn try_from(value: RawAuthInfo) -> Result<Self, Self::Error> {
        let signer_infos = value
            .signer_infos
            .into_iter()
            .map(TryFrom::try_from)
            .collect::<Result<_, Error>>()?;
        Ok(AuthInfo {
            signer_infos,
            fee: value.fee.ok_or(Error::MissingFee)?.try_into()?,
        })
    }
}

impl From<AuthInfo> for RawAuthInfo {
    fn from(value: AuthInfo) -> Self {
        let signer_infos = value.signer_infos.into_iter().map(Into::into).collect();
        #[allow(deprecated)] // tip is deprecated
        RawAuthInfo {
            signer_infos,
            fee: Some(value.fee.into()),
            tip: None,
        }
    }
}

impl TryFrom<RawTxResponse> for TxResponse {
    type Error = Error;

    fn try_from(response: RawTxResponse) -> Result<TxResponse, Self::Error> {
        Ok(TxResponse {
            height: response.height.try_into()?,
            txhash: response.txhash.parse()?,
            codespace: response.codespace,
            code: response.code.try_into()?,
            data: response.data,
            raw_log: response.raw_log,
            logs: response.logs,
            info: response.info,
            gas_wanted: response.gas_wanted,
            gas_used: response.gas_used,
            tx: response.tx,
            timestamp: response.timestamp,
            events: response.events,
        })
    }
}

impl TryFrom<RawSignerInfo> for SignerInfo {
    type Error = Error;

    fn try_from(value: RawSignerInfo) -> Result<Self, Self::Error> {
        Ok(SignerInfo {
            public_key: value.public_key,
            mode_info: value.mode_info.ok_or(Error::MissingModeInfo)?.try_into()?,
            sequence: value.sequence,
        })
    }
}

impl From<SignerInfo> for RawSignerInfo {
    fn from(value: SignerInfo) -> Self {
        RawSignerInfo {
            public_key: value.public_key,
            mode_info: Some(value.mode_info.into()),
            sequence: value.sequence,
        }
    }
}

impl TryFrom<RawFee> for Fee {
    type Error = Error;

    fn try_from(value: RawFee) -> Result<Fee, Self::Error> {
        let amount = value
            .amount
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<_, Error>>()?;

        Ok(Fee {
            amount,
            gas_limit: value.gas_limit,
            payer: (!value.payer.is_empty())
                .then(|| value.payer.parse())
                .transpose()?,
            granter: (!value.granter.is_empty())
                .then(|| value.granter.parse())
                .transpose()?,
        })
    }
}

impl From<Fee> for RawFee {
    fn from(value: Fee) -> Self {
        let amount = value.amount.into_iter().map(Into::into).collect();
        RawFee {
            amount,
            gas_limit: value.gas_limit,
            payer: value.payer.map(|acc| acc.to_string()).unwrap_or_default(),
            granter: value.granter.map(|acc| acc.to_string()).unwrap_or_default(),
        }
    }
}

impl TryFrom<RawModeInfo> for ModeInfo {
    type Error = Error;

    fn try_from(value: RawModeInfo) -> Result<Self, Self::Error> {
        Ok(ModeInfo {
            sum: value.sum.ok_or(Error::MissingSum)?.try_into()?,
        })
    }
}

impl From<ModeInfo> for RawModeInfo {
    fn from(value: ModeInfo) -> Self {
        RawModeInfo {
            sum: Some(value.sum.into()),
        }
    }
}

impl TryFrom<RawSum> for Sum {
    type Error = Error;

    fn try_from(value: RawSum) -> Result<Self, Self::Error> {
        let sum = match value {
            RawSum::Single(Single { mode }) => Sum::Single { mode },
            RawSum::Multi(Multi {
                bitarray,
                mode_infos,
            }) => {
                let bitarray = bitarray.ok_or(Error::MissingBitarray)?.try_into()?;
                let mode_infos = mode_infos
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, Error>>()?;
                Sum::Multi {
                    bitarray,
                    mode_infos,
                }
            }
        };
        Ok(sum)
    }
}

impl From<Sum> for RawSum {
    fn from(value: Sum) -> Self {
        match value {
            Sum::Single { mode } => RawSum::Single(Single { mode }),
            Sum::Multi {
                bitarray,
                mode_infos,
            } => {
                let mode_infos = mode_infos.into_iter().map(Into::into).collect();
                RawSum::Multi(Multi {
                    bitarray: Some(bitarray.into()),
                    mode_infos,
                })
            }
        }
    }
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
pub use wbg::*;

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
mod wbg {
    use tendermint_proto::v0_38::abci::{Event, EventAttribute};
    use wasm_bindgen::prelude::*;

    /// Event allows application developers to attach additional information to
    /// ResponseBeginBlock, ResponseEndBlock, ResponseCheckTx and ResponseDeliverTx.
    /// Later, transactions may be queried using these events.
    #[derive(Clone)]
    #[wasm_bindgen(getter_with_clone)]
    pub struct JsEvent {
        pub r#type: String,
        pub attributes: Vec<JsEventAttribute>,
    }

    impl From<Event> for JsEvent {
        fn from(value: Event) -> Self {
            JsEvent {
                r#type: value.r#type,
                attributes: value.attributes.into_iter().map(Into::into).collect(),
            }
        }
    }

    #[derive(Clone)]
    #[wasm_bindgen(getter_with_clone)]
    pub struct JsEventAttribute {
        pub key: String,
        pub value: String,
        pub index: bool,
    }

    /// EventAttribute is a single key-value pair, associated with an event.
    impl From<EventAttribute> for JsEventAttribute {
        fn from(value: EventAttribute) -> Self {
            JsEventAttribute {
                key: value.key,
                value: value.value,
                index: value.index,
            }
        }
    }
}

impl Protobuf<RawTxBody> for TxBody {}
impl Protobuf<RawAuthInfo> for AuthInfo {}

#[cfg(feature = "uniffi")]
mod uniffi_types {
    use tendermint_proto::v0_38::abci::{Event, EventAttribute};

    #[uniffi::remote(Record)]
    pub struct Event {
        pub r#type: String,
        pub attributes: Vec<EventAttribute>,
    }

    #[uniffi::remote(Record)]
    pub struct EventAttribute {
        pub key: String,
        pub value: String,
        pub index: bool,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as test;

    #[test]
    fn error_code_try_from_matches_repr() {
        for code in 0..=ErrorCode::Panic as u32 {
            let from_repr: Option<ErrorCode> = serde_json::from_value(code.into()).ok();
            assert_eq!(ErrorCode::try_from(code).ok(), from_repr, "code {code}");
        }
    }
}
