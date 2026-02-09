use std::time::Duration;

use js_sys::{Array, Uint8Array};
use k256::ecdsa::VerifyingKey;
use wasm_bindgen::convert::TryFromJsValue;
use wasm_bindgen::prelude::*;

use crate::GrpcClientBuilderError;
use crate::signer::{JsSigner, JsSignerFn};

mod grpc_client;

use grpc_client::GrpcClient;

#[wasm_bindgen(typescript_custom_section)]
const TS_APPEND_CONTENT: &str = r#"
export type UrlOrEndpoint = string | Endpoint;
export type UrlsOrEndpoints = UrlOrEndpoint | UrlOrEndpoint[];
"#;

/// A URL endpoint paired with its configuration.
///
/// Use this with `withUrl`/`withUrls` to configure endpoints with different settings.
///
/// # Example
///
/// ```js
/// const primary = new Endpoint(
///   "http://primary:9090"
/// );
/// primary = primary.withMetadata("auth", "token1");
/// const fallback = new Endpoint(
///   "http://fallback:9090"
/// );
/// fallback = fallback.withTimeout(10000);
///
/// const client = await GrpcClient
///   .withUrls([primary, fallback])
///   .build();
/// ```
#[wasm_bindgen]
pub struct Endpoint {
    pub(crate) inner: crate::Endpoint,
}

#[wasm_bindgen]
impl Endpoint {
    /// Create a new endpoint with a URL.
    #[wasm_bindgen(constructor)]
    pub fn new(url: String) -> Self {
        let endpoint = crate::Endpoint::from(url);
        Self { inner: endpoint }
    }

    /// Appends ASCII metadata (HTTP/2 header) to requests made to this endpoint.
    ///
    /// Note that this method **consumes** the endpoint and returns an updated instance.
    #[wasm_bindgen(js_name = "withMetadata")]
    pub fn with_metadata(self, key: String, value: String) -> Self {
        Self {
            inner: self.inner.metadata(key, value),
        }
    }

    /// Appends binary metadata to requests made to this endpoint.
    ///
    /// Keys must have `-bin` suffix.
    ///
    /// Note that this method **consumes** the endpoint and returns an updated instance.
    #[wasm_bindgen(js_name = "withMetadataBin")]
    pub fn with_metadata_bin(self, key: String, value: Uint8Array) -> Self {
        Self {
            inner: self.inner.metadata_bin(key, value.to_vec()),
        }
    }

    /// Sets the request timeout in milliseconds for this endpoint.
    ///
    /// Note that this method **consumes** the endpoint and returns an updated instance.
    #[wasm_bindgen(js_name = "withTimeout")]
    pub fn with_timeout(self, timeout_ms: u64) -> Self {
        Self {
            inner: self.inner.timeout(Duration::from_millis(timeout_ms)),
        }
    }
}

/// Builder for [`GrpcClient`] and [`TxClient`].
///
/// Url must point to a [grpc-web proxy](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-WEB.md).
///
/// # Keyless client example
///
/// ```js
/// const client = await GrpcClient
///   .withUrl("http://127.0.0.1:18080")
///   .build()
///
/// // With config:
/// const endpoint = new Endpoint("http://127.0.0.1:18080").withTimeout(BigInt(5000));
/// const client = await GrpcClient
///   .withUrl(endpoint)
///   .build()
/// ```
///
/// # Transaction client examples
///
/// ## Example with noble/curves
/// ```js
/// import { secp256k1 } from "@noble/curves/secp256k1";
///
/// const privKey = "fdc8ac75dfa1c142dbcba77938a14dd03078052ce0b49a529dcf72a9885a3abb";
/// const pubKey = secp256k1.getPublicKey(privKey);
///
/// const signer = (signDoc) => {
///   const bytes = protoEncodeSignDoc(signDoc);
///   const sig = secp256k1.sign(bytes, privKey, { prehash: true });
///   return sig.toCompactRawBytes();
/// };
///
/// const client = await GrpcClient
///   .withUrl("http://127.0.0.1:18080")
///   .withPubkeyAndSigner(pubKey, signer)
///   .build();
/// ```
///
/// ## Example with leap wallet
/// ```js
/// await window.leap.enable("mocha-4")
/// const keys = await window.leap.getKey("mocha-4")
///
/// const signer = (signDoc) => {
///   return window.leap.signDirect("mocha-4", keys.bech32Address, signDoc, { preferNoSetFee: true })
///     .then(sig => Uint8Array.from(atob(sig.signature.signature), c => c.charCodeAt(0)))
/// }
///
/// const client = await GrpcClient
///   .withUrl("http://127.0.0.1:18080")
///   .withPubkeyAndSigner(keys.pubKey, signer)
///   .build()
/// ```
#[wasm_bindgen]
pub struct GrpcClientBuilder {
    inner: crate::GrpcClientBuilder,
}

#[wasm_bindgen]
impl GrpcClientBuilder {
    /// Set the `url` of the grpc-web server to connect to.
    ///
    /// Accepts a string, an `Endpoint`, or an array of strings/endpoints.
    ///
    /// Note that this method **consumes** builder and returns updated instance of it.
    /// Make sure to re-assign it if you keep builder in a variable.
    #[wasm_bindgen(js_name = "withUrl")]
    pub fn with_url(
        self,
        #[wasm_bindgen(unchecked_param_type = "UrlsOrEndpoints")] url: JsValue,
    ) -> Result<Self, JsValue> {
        let endpoints = endpoints_from_js(url)?;
        let ret = Self {
            inner: self.inner.endpoints(endpoints),
        };
        Ok(ret)
    }

    /// Add multiple URL endpoints at once for fallback support.
    ///
    /// Accepts a string, an `Endpoint`, or an array of strings/endpoints.
    ///
    /// When multiple endpoints are configured, the client will automatically
    /// fall back to the next endpoint if a network-related error occurs.
    ///
    /// Note that this method **consumes** builder and returns updated instance of it.
    /// Make sure to re-assign it if you keep builder in a variable.
    #[wasm_bindgen(js_name = "withUrls")]
    pub fn with_urls(
        self,
        #[wasm_bindgen(unchecked_param_type = "UrlsOrEndpoints")] urls: JsValue,
    ) -> Result<Self, JsValue> {
        let endpoints = endpoints_from_js(urls)?;
        let ret = Self {
            inner: self.inner.endpoints(endpoints),
        };
        Ok(ret)
    }

    /// Add public key and signer to the client being built
    ///
    /// Note that this method **consumes** builder and returns updated instance of it.
    /// Make sure to re-assign it if you keep builder in a variable.
    #[wasm_bindgen(js_name = withPubkeyAndSigner)]
    pub fn with_pubkey_and_signer(
        self,
        account_pubkey: Uint8Array,
        signer_fn: JsSignerFn,
    ) -> Result<Self, GrpcClientBuilderError> {
        let signer = JsSigner::new(signer_fn);
        let account_pubkey = VerifyingKey::try_from(account_pubkey.to_vec().as_slice())
            .map_err(|_| GrpcClientBuilderError::InvalidPublicKey)?;
        Ok(Self {
            inner: self.inner.pubkey_and_signer(account_pubkey, signer),
        })
    }

    /// build gRPC client
    pub fn build(self) -> Result<GrpcClient, GrpcClientBuilderError> {
        Ok(self.inner.build()?.into())
    }
}

pub(crate) fn endpoint_from_js(value: JsValue) -> Result<crate::Endpoint, JsValue> {
    if let Some(url) = value.as_string() {
        return Ok(crate::Endpoint::from(url));
    }

    if let Ok(endpoint) = Endpoint::try_from_js_value(value) {
        return Ok(endpoint.inner);
    }

    Err(JsValue::from_str(
        "Expected a string or Endpoint for endpoint values",
    ))
}

pub(crate) fn endpoints_from_js(value: JsValue) -> Result<Vec<crate::Endpoint>, JsValue> {
    if Array::is_array(&value) {
        let array = Array::from(&value);
        return array
            .iter()
            .map(endpoint_from_js)
            .collect::<Result<Vec<_>, _>>();
    }
    Ok(vec![endpoint_from_js(value)?])
}

impl From<crate::GrpcClientBuilder> for GrpcClientBuilder {
    fn from(inner: crate::GrpcClientBuilder) -> Self {
        Self { inner }
    }
}
