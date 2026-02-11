//! Types related to signing transactions
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use celestia_proto::cosmos::tx::v1beta1::SignDoc;
use k256::ecdsa::VerifyingKey;
use k256::ecdsa::signature::Signer;
use prost::{Message, Name};
use tendermint::chain::Id;
use tendermint_proto::Protobuf;
use tendermint_proto::google::protobuf::Any;

use celestia_proto::cosmos::crypto::secp256k1;
use celestia_types::state::auth::BaseAccount;
use celestia_types::state::{AuthInfo, Fee, ModeInfo, RawTx, RawTxBody, SignerInfo, Sum};

use crate::Result;

/// ECDSA/secp256k1 signature used for signing transactions
pub type DocSignature = k256::ecdsa::Signature;
/// Signature error
pub type SignatureError = k256::ecdsa::signature::Error;

/// Helper struct used to pass type erased signer. To implement custom signer, see
/// [`DocSigner`] trait.
#[derive(Clone)]
pub(crate) struct BoxedDocSigner(Arc<dyn AbstractDocSigner>);

/// Signer capable of producing ecdsa signature using secp256k1 curve.
pub trait DocSigner: Send + Sync {
    /// Try to sign the provided sign doc.
    fn try_sign(
        &self,
        doc: SignDoc,
    ) -> impl Future<Output = Result<DocSignature, SignatureError>> + Send;
}

impl<T> DocSigner for T
where
    T: Signer<DocSignature> + Send + Sync + 'static,
{
    async fn try_sign(&self, doc: SignDoc) -> Result<DocSignature, SignatureError> {
        let bytes = doc.encode_to_vec();
        self.try_sign(&bytes)
    }
}

impl BoxedDocSigner {
    pub fn new<S>(signer: S) -> BoxedDocSigner
    where
        S: AbstractDocSigner,
    {
        BoxedDocSigner(Arc::new(signer))
    }
}

impl DocSigner for BoxedDocSigner {
    async fn try_sign(&self, doc: SignDoc) -> Result<DocSignature, SignatureError> {
        self.0.try_sign(doc).await
    }
}

pub(crate) trait AbstractDocSigner: Sync + Send + 'static {
    fn try_sign<'a>(
        &'a self,
        doc: SignDoc,
    ) -> Pin<Box<dyn Future<Output = Result<DocSignature, SignatureError>> + Send + 'a>>;
}

impl<T> AbstractDocSigner for T
where
    T: DocSigner + Sync + Send + 'static,
{
    fn try_sign<'a>(
        &'a self,
        doc: SignDoc,
    ) -> Pin<Box<dyn Future<Output = Result<DocSignature, SignatureError>> + Send + 'a>> {
        Box::pin(DocSigner::try_sign(self, doc))
    }
}

impl fmt::Debug for BoxedDocSigner {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("BoxedDocSigner { .. }")
    }
}

/// Sign `tx_body` and the transaction metadata as the `base_account` using `signer`
pub async fn sign_tx(
    tx_body: RawTxBody,
    chain_id: Id,
    base_account: &BaseAccount,
    verifying_key: &VerifyingKey,
    signer: &impl DocSigner,
    gas_limit: u64,
    fee: u64,
) -> Result<RawTx> {
    // From https://github.com/celestiaorg/cosmos-sdk/blob/v1.25.0-sdk-v0.46.16/proto/cosmos/tx/signing/v1beta1/signing.proto#L24
    const SIGNING_MODE_INFO: ModeInfo = ModeInfo {
        sum: Sum::Single { mode: 1 },
    };

    let public_key = secp256k1::PubKey {
        key: verifying_key.to_encoded_point(true).as_bytes().to_vec(),
    };

    let public_key_as_any = Any {
        type_url: secp256k1::PubKey::type_url(),
        value: public_key.encode_to_vec(),
    };

    let mut fee = Fee::new(fee, gas_limit);
    fee.payer = Some(base_account.address.into());

    let auth_info = AuthInfo {
        signer_infos: vec![SignerInfo {
            public_key: Some(public_key_as_any),
            mode_info: SIGNING_MODE_INFO,
            sequence: base_account.sequence,
        }],
        fee,
    };

    let doc = SignDoc {
        body_bytes: tx_body.encode_to_vec(),
        auth_info_bytes: auth_info.clone().encode_vec(),
        chain_id: chain_id.into(),
        account_number: base_account.account_number,
    };
    let signature = signer.try_sign(doc).await?;

    Ok(RawTx {
        auth_info: Some(auth_info.into()),
        body: Some(tx_body),
        signatures: vec![signature.to_bytes().to_vec()],
    })
}

#[cfg(feature = "uniffi")]
pub use uniffi_types::*;

#[cfg(feature = "uniffi")]
mod uniffi_types {
    use super::*;

    use async_trait::async_trait;
    use k256::ecdsa::signature::Error as K256Error;
    use std::sync::Arc;
    use tendermint::signature::Secp256k1Signature;
    use uniffi::Record;

    /// Errors returned from [`UniffiSigner`]
    #[derive(Debug, thiserror::Error, uniffi::Error)]
    pub enum SignerError {
        /// Error during uniffi types conversion
        #[error("uniffi conversion error: {msg}")]
        UniffiConversionError {
            /// error message
            msg: String,
        },

        /// Error occured during signing
        #[error("error while signing: {msg}")]
        SigningError {
            /// error message
            msg: String,
        },
    }

    /// Trait that implements signing the transaction.
    ///
    /// Example usage:
    /// ```swift
    /// // uses 21-DOT-DEV/swift-secp256k1
    /// final class StaticSigner : UniffiSigner {
    ///     let sk : P256K.Signing.PrivateKey
    ///
    ///     init(sk: P256K.Signing.PrivateKey) {
    ///         self.sk = sk
    ///     }
    ///
    ///     func sign(doc: SignDoc) async throws -> UniffiSignature {
    ///         let messageData = protoEncodeSignDoc(signDoc: doc);
    ///         let signature = try! sk.signature(for: messageData)
    ///         return try! UniffiSignature (bytes: signature.compactRepresentation)
    ///     }
    /// }
    /// ```
    #[uniffi::export(with_foreign)]
    #[async_trait]
    pub trait UniffiSigner: Sync + Send {
        /// sign provided `SignDoc` using secp256k1. Use helper proto_encode_sign_doc to
        /// get canonical protobuf byte encoding of the message.
        async fn sign(&self, doc: SignDoc) -> Result<UniffiSignature, SignerError>;
    }

    /// Non-rust signer coming from uniffi
    pub struct UniffiSignerBox(pub Arc<dyn UniffiSigner>);

    /// Message signature
    #[derive(Record)]
    pub struct UniffiSignature {
        /// signature bytes
        pub bytes: Vec<u8>,
    }

    impl DocSigner for UniffiSignerBox {
        async fn try_sign(&self, doc: SignDoc) -> Result<Secp256k1Signature, K256Error> {
            match self.0.sign(doc).await {
                Ok(s) => Secp256k1Signature::try_from(s).map_err(K256Error::from_source),
                Err(e) => Err(K256Error::from_source(e)),
            }
        }
    }

    impl From<DocSignature> for UniffiSignature {
        fn from(value: DocSignature) -> Self {
            UniffiSignature {
                bytes: value.to_vec(),
            }
        }
    }

    impl TryFrom<UniffiSignature> for DocSignature {
        type Error = SignerError;

        fn try_from(value: UniffiSignature) -> std::result::Result<Self, Self::Error> {
            DocSignature::from_slice(&value.bytes).map_err(|e| SignerError::SigningError {
                msg: format!("invalid signature {e}"),
            })
        }
    }
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
pub use wbg::*;

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
mod wbg {
    use super::*;

    use celestia_proto::cosmos::tx::v1beta1::SignDoc;

    use js_sys::{BigInt, Function, Promise, Uint8Array};
    use lumina_utils::make_object;
    use send_wrapper::SendWrapper;
    use wasm_bindgen::prelude::*;
    use wasm_bindgen_futures::JsFuture;

    /// A helper to encode the SignDoc with protobuf to get bytes to sign directly.
    #[wasm_bindgen(js_name = protoEncodeSignDoc)]
    pub fn proto_encode_sign_doc(sign_doc: JsSignDoc) -> Vec<u8> {
        SignDoc::from(sign_doc).encode_to_vec()
    }

    /// Signer that uses a javascript function for signing.
    pub struct JsSigner {
        signer_fn: SendWrapper<JsSignerFn>,
    }

    impl JsSigner {
        /// Create a new signer out of a function
        pub fn new(function: JsSignerFn) -> Self {
            Self {
                signer_fn: SendWrapper::new(function),
            }
        }
    }

    impl DocSigner for JsSigner {
        async fn try_sign(&self, doc: SignDoc) -> Result<DocSignature, SignatureError> {
            let promise = {
                let msg = JsSignDoc::from(doc);

                let sig_or_promise = self.signer_fn.call1(&JsValue::null(), &msg).map_err(|e| {
                    let err = format!("Error calling signer fn: {e:?}");
                    SignatureError::from_source(err)
                })?;

                // we got the sig already, so return it
                if !sig_or_promise.has_type::<Promise>() {
                    return try_into_signature(sig_or_promise);
                }

                sig_or_promise.unchecked_into::<Promise>()
            };

            let sig = SendWrapper::new(JsFuture::from(promise))
                .await
                .map_err(|e| {
                    let err = format!("Error awaiting signer promise: {e:?}");
                    SignatureError::from_source(err)
                })?;

            try_into_signature(sig)
        }
    }

    fn try_into_signature(val: JsValue) -> Result<DocSignature, SignatureError> {
        let sig = val.dyn_into::<Uint8Array>().map_err(|orig| {
            let err = format!(
                "Signature must be Uint8Array, found: {}",
                orig.js_typeof().as_string().expect("typeof returns string")
            );
            SignatureError::from_source(err)
        })?;

        DocSignature::from_slice(&sig.to_vec()).map_err(SignatureError::from_source)
    }

    #[wasm_bindgen(typescript_custom_section)]
    const _: &str = "
    /**
     * A payload to be signed
     */
    export interface SignDoc {
      bodyBytes: Uint8Array;
      authInfoBytes: Uint8Array;
      chainId: string;
      accountNumber: bigint;
    }

    /**
     * A function that produces a signature of a payload
     */
    export type SignerFn = ((arg: SignDoc) => Uint8Array) | ((arg: SignDoc) => Promise<Uint8Array>);
    ";

    #[wasm_bindgen]
    extern "C" {
        /// A function that produces a signature of a payload
        #[wasm_bindgen(extends = Function, typescript_type = "SignerFn")]
        pub type JsSignerFn;

        /// Payload that is being signed
        #[wasm_bindgen(typescript_type = "SignDoc")]
        pub type JsSignDoc;

        /// Protobuf serialisation of TxBody, that matches the
        /// representation in TxRaw
        #[wasm_bindgen(method, getter, js_name = bodyBytes)]
        pub fn body_bytes(this: &JsSignDoc) -> Vec<u8>;

        /// Protobuf serialisation of AuthInfo that matches the representation
        /// in TxRaw.
        #[wasm_bindgen(method, getter, js_name = authInfoBytes)]
        pub fn auth_info_bytes(this: &JsSignDoc) -> Vec<u8>;

        /// Identifier of the chain the signed transaction targets
        #[wasm_bindgen(method, getter, js_name = chainId)]
        pub fn chain_id(this: &JsSignDoc) -> String;

        /// Account number of the account in state
        #[wasm_bindgen(method, getter, js_name = accountNumber)]
        pub fn account_number(this: &JsSignDoc) -> u64;
    }

    impl From<JsSignDoc> for SignDoc {
        fn from(value: JsSignDoc) -> SignDoc {
            SignDoc {
                body_bytes: value.body_bytes(),
                auth_info_bytes: value.auth_info_bytes(),
                chain_id: value.chain_id(),
                account_number: value.account_number(),
            }
        }
    }

    impl From<SignDoc> for JsSignDoc {
        fn from(value: SignDoc) -> JsSignDoc {
            let obj = make_object!(
                "bodyBytes" => Uint8Array::from(value.body_bytes.as_ref()),
                "authInfoBytes" => Uint8Array::from(value.auth_info_bytes.as_ref()),
                "chainId" => value.chain_id.into(),
                "accountNumber" => BigInt::from(value.account_number)
            );

            obj.unchecked_into()
        }
    }
}
