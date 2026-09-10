use std::error::Error;
use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use der::asn1::OctetStringRef;
use der::{Decode, Sequence};
use ed25519_dalek::{Signature, VerifyingKey};
use hyper_util::rt::TokioIo;
use tokio_rustls::rustls::client::Resumption;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use tonic::body::Body;
use tower::Service;
use x509_parser::prelude::parse_x509_certificate;

use crate::error::FibreError;
use crate::payment_promise::raw_bytes_message_sign_bytes;
use crate::transport::io_connector::FibreIoConnector;

const IDENTITY_EXTENSION_OID: &str = "1.3.6.1.4.1.66463.1.1";
const SIGN_UNIQUE_ID: &[u8] = b"celestia-fibre-tls-v1";
const SIGN_PREFIX: &[u8] = b"celestia-fibre-tls:";
const BINDING_VERSION: i64 = 1;
const MAX_IDENTITY_EXTENSION_SIZE: usize = 8192;
const MAX_PAYLOAD_DER_SIZE: usize = 4096;
const MAX_CERT_VALIDITY_SECONDS: i128 = 31_536_600;
const CLOCK_SKEW_SECONDS: i128 = 300;

type BoxError = Box<dyn Error + Send + Sync>;
type H2Sender = hyper::client::conn::http2::SendRequest<Body>;

#[derive(Debug)]
struct FibreTransportError(BoxError);

impl fmt::Display for FibreTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl Error for FibreTransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl From<BoxError> for FibreTransportError {
    fn from(error: BoxError) -> Self {
        Self(error)
    }
}

#[derive(Sequence)]
struct SignedIdentity<'a> {
    payload: OctetStringRef<'a>,
    signature: OctetStringRef<'a>,
}

#[derive(Sequence)]
struct BindingPayload<'a> {
    version: i64,
    not_before: i64,
    not_after: i64,
    tls_pub_key: OctetStringRef<'a>,
}

#[derive(Debug, thiserror::Error)]
enum FibreCertificateError {
    #[error("peer cert is missing the fibre identity extension")]
    MissingIdentityExtension,
    #[error("peer cert has duplicate fibre identity extensions")]
    DuplicateIdentityExtensions,
    #[error("trailing bytes in identity extension: {0}")]
    IdentityTrailingData(#[source] der::Error),
    #[error("unmarshal identity extension: {0}")]
    IdentityDer(#[source] der::Error),
    #[error("trailing bytes in binding payload: {0}")]
    BindingTrailingData(#[source] der::Error),
    #[error("unmarshal binding payload: {0}")]
    BindingDer(#[source] der::Error),
    #[error("parse peer cert: {0}")]
    CertificateParse(#[source] x509_parser::nom::Err<x509_parser::error::X509Error>),
    #[error("peer cert has {0} trailing bytes")]
    CertificateTrailingData(usize),
    #[error("identity extension size {0} exceeds maximum {MAX_IDENTITY_EXTENSION_SIZE}")]
    IdentityExtensionTooLarge(usize),
    #[error("empty identity payload")]
    EmptyIdentityPayload,
    #[error("identity payload size {0} exceeds maximum {MAX_PAYLOAD_DER_SIZE}")]
    IdentityPayloadTooLarge(usize),
    #[error("empty identity signature")]
    EmptyIdentitySignature,
    #[error("unsupported fibre identity version {0}")]
    UnsupportedIdentityVersion(i64),
    #[error("peer cert signature is invalid: {0}")]
    InvalidIdentitySignature(#[source] ed25519_dalek::SignatureError),
    #[error("peer cert public key does not match signed identity")]
    TlsPublicKeyMismatch,
    #[error("fibre identity validity window is empty: {not_before}..{not_after}")]
    EmptyValidityWindow { not_before: i128, not_after: i128 },
    #[error("fibre identity validity window is {0} seconds, exceeding the maximum")]
    ValidityWindowTooLong(i128),
    #[error(
        "peer fibre identity is not currently valid: now {now}, window {not_before}..{not_after}"
    )]
    OutsideValidityWindow {
        now: i128,
        not_before: i128,
        not_after: i128,
    },
    #[error(
        "certificate validity {certificate_not_before}..{certificate_not_after} does not match signed identity {signed_not_before}..{signed_not_after}"
    )]
    CertificateValidityMismatch {
        signed_not_before: i64,
        signed_not_after: i64,
        certificate_not_before: i64,
        certificate_not_after: i64,
    },
    #[error("parse peer cert extended key usage: {0}")]
    ExtendedKeyUsage(#[source] x509_parser::error::X509Error),
    #[error("peer cert missing serverAuth extended key usage")]
    MissingServerAuth,
}

impl From<FibreCertificateError> for tokio_rustls::rustls::Error {
    fn from(error: FibreCertificateError) -> Self {
        tokio_rustls::rustls::Error::InvalidCertificate(
            tokio_rustls::rustls::CertificateError::Other(tokio_rustls::rustls::OtherError(
                Arc::new(error),
            )),
        )
    }
}

#[derive(Debug)]
struct FibreServerCertVerifier {
    validator_key: VerifyingKey,
    chain_id: String,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for FibreServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, tokio_rustls::rustls::Error> {
        verify_certificate(
            end_entity.as_ref(),
            &self.validator_key,
            &self.chain_id,
            now,
        )?;

        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        tokio_rustls::rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, tokio_rustls::rustls::Error> {
        tokio_rustls::rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub(crate) fn grpc_client(
    url: String,
    validator_key: VerifyingKey,
    chain_id: String,
    io_connector: Arc<dyn FibreIoConnector>,
) -> Result<celestia_grpc::GrpcClient, FibreError> {
    grpc_client_with_time(url, validator_key, chain_id, io_connector, None)
}

fn grpc_client_with_time(
    url: String,
    validator_key: VerifyingKey,
    chain_id: String,
    io_connector: Arc<dyn FibreIoConnector>,
    time_provider: Option<Arc<dyn tokio_rustls::rustls::time_provider::TimeProvider>>,
) -> Result<celestia_grpc::GrpcClient, FibreError> {
    let uri = url
        .parse::<http::Uri>()
        .map_err(|source| FibreError::InvalidEndpoint {
            endpoint: url,
            source,
        })?;
    let host = uri
        .host()
        .ok_or_else(|| FibreError::EndpointMissingHost(uri.clone()))?
        .to_string();
    let port = uri.port_u16().unwrap_or(443);
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let mut tls_config = fibre_tls_config(validator_key, chain_id, provider, time_provider);
    tls_config.alpn_protocols = vec![b"h2".to_vec()];
    let tls_connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));
    let transport = FibreH2Transport {
        inner: Arc::new(FibreH2TransportInner {
            uri,
            host,
            port,
            io_connector,
            tls_connector,
            sender: tokio::sync::Mutex::new(None),
        }),
    };

    celestia_grpc::GrpcClient::builder()
        .transport(transport)
        .build()
        .map_err(FibreError::from)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(super) fn grpc_client_at(
    url: String,
    validator_key: VerifyingKey,
    chain_id: String,
    io_connector: Arc<dyn FibreIoConnector>,
    now: UnixTime,
) -> Result<celestia_grpc::GrpcClient, FibreError> {
    grpc_client_with_time(
        url,
        validator_key,
        chain_id,
        io_connector,
        Some(Arc::new(FixedTime(now))),
    )
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[derive(Debug)]
struct FixedTime(UnixTime);

#[cfg(all(test, not(target_arch = "wasm32")))]
impl tokio_rustls::rustls::time_provider::TimeProvider for FixedTime {
    fn current_time(&self) -> Option<UnixTime> {
        Some(self.0)
    }
}

fn fibre_tls_config(
    validator_key: VerifyingKey,
    chain_id: String,
    provider: Arc<CryptoProvider>,
    time_provider: Option<Arc<dyn tokio_rustls::rustls::time_provider::TimeProvider>>,
) -> ClientConfig {
    let verifier = FibreServerCertVerifier {
        validator_key,
        chain_id,
        provider: provider.clone(),
    };
    let builder = match time_provider {
        Some(time_provider) => ClientConfig::builder_with_details(provider, time_provider),
        None => ClientConfig::builder_with_provider(provider),
    };
    let mut tls_config = builder
        .with_protocol_versions(&[&tokio_rustls::rustls::version::TLS13])
        .expect("ring crypto provider supports TLS 1.3")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    tls_config.resumption = Resumption::disabled();
    tls_config
}

struct FibreH2TransportInner {
    uri: http::Uri,
    host: String,
    port: u16,
    io_connector: Arc<dyn FibreIoConnector>,
    tls_connector: tokio_rustls::TlsConnector,
    sender: tokio::sync::Mutex<Option<H2Sender>>,
}

#[derive(Clone)]
struct FibreH2Transport {
    inner: Arc<FibreH2TransportInner>,
}

impl FibreH2TransportInner {
    async fn connect(&self) -> Result<H2Sender, BoxError> {
        let server_name = ServerName::try_from(self.host.clone())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        let io = self
            .io_connector
            .connect(self.host.clone(), self.port)
            .await
            .map_err(BoxError::from)?;
        let tls = self
            .tls_connector
            .connect(server_name, io)
            .await
            .map_err(BoxError::from)?;
        let (sender, connection) = hyper::client::conn::http2::Builder::new(h2_executor())
            .handshake(TokioIo::new(tls))
            .await?;
        spawn_connection(connection);
        Ok(sender)
    }

    fn absolute_uri(&self, request_uri: &http::Uri) -> Result<http::Uri, BoxError> {
        let mut parts = self.uri.clone().into_parts();
        parts.path_and_query = request_uri.path_and_query().cloned();
        Ok(http::Uri::from_parts(parts)?)
    }
}

impl Service<http::Request<Body>> for FibreH2Transport {
    type Response = http::Response<hyper::body::Incoming>;
    type Error = FibreTransportError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut request: http::Request<Body>) -> Self::Future {
        let inner = self.inner.clone();
        Box::pin(async move {
            *request.uri_mut() = inner.absolute_uri(request.uri())?;
            let mut sender = {
                let mut shared_sender = inner.sender.lock().await;
                if shared_sender.as_ref().is_none_or(H2Sender::is_closed) {
                    *shared_sender = Some(inner.connect().await?);
                }
                shared_sender
                    .as_ref()
                    .expect("sender was initialized")
                    .clone()
            };
            sender.ready().await.map_err(BoxError::from)?;
            sender
                .send_request(request)
                .await
                .map_err(BoxError::from)
                .map_err(FibreTransportError::from)
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn h2_executor() -> hyper_util::rt::TokioExecutor {
    hyper_util::rt::TokioExecutor::new()
}

#[cfg(target_arch = "wasm32")]
fn h2_executor() -> WasmExecutor {
    WasmExecutor
}

#[cfg(not(target_arch = "wasm32"))]
fn spawn_connection<F>(connection: F)
where
    F: Future<Output = Result<(), hyper::Error>> + Send + 'static,
{
    tokio::spawn(async move {
        let _ = connection.await;
    });
}

#[cfg(target_arch = "wasm32")]
fn spawn_connection<F>(connection: F)
where
    F: Future<Output = Result<(), hyper::Error>> + Send + 'static,
{
    lumina_utils::executor::spawn(async move {
        let _ = connection.await;
    });
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone, Copy)]
struct WasmExecutor;

#[cfg(target_arch = "wasm32")]
impl<F> hyper::rt::Executor<F> for WasmExecutor
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, future: F) {
        lumina_utils::executor::spawn(future);
    }
}

fn identity_extension<'a>(
    extensions: &'a [x509_parser::extensions::X509Extension<'a>],
) -> Result<&'a [u8], FibreCertificateError> {
    let mut matching = extensions
        .iter()
        .filter(|extension| extension.oid.to_id_string() == IDENTITY_EXTENSION_OID);
    let extension = matching
        .next()
        .ok_or(FibreCertificateError::MissingIdentityExtension)?;
    if matching.next().is_some() {
        return Err(FibreCertificateError::DuplicateIdentityExtensions);
    }
    Ok(extension.value)
}

fn decode_identity(extension: &[u8]) -> Result<SignedIdentity<'_>, FibreCertificateError> {
    SignedIdentity::from_der(extension).map_err(|error| {
        if matches!(error.kind(), der::ErrorKind::TrailingData { .. }) {
            FibreCertificateError::IdentityTrailingData(error)
        } else {
            FibreCertificateError::IdentityDer(error)
        }
    })
}

fn decode_binding(payload: &[u8]) -> Result<BindingPayload<'_>, FibreCertificateError> {
    BindingPayload::from_der(payload).map_err(|error| {
        if matches!(error.kind(), der::ErrorKind::TrailingData { .. }) {
            FibreCertificateError::BindingTrailingData(error)
        } else {
            FibreCertificateError::BindingDer(error)
        }
    })
}

fn verify_certificate(
    cert_der: &[u8],
    validator_key: &VerifyingKey,
    chain_id: &str,
    now: UnixTime,
) -> Result<(), FibreCertificateError> {
    let (remaining, cert) =
        parse_x509_certificate(cert_der).map_err(FibreCertificateError::CertificateParse)?;
    if !remaining.is_empty() {
        return Err(FibreCertificateError::CertificateTrailingData(
            remaining.len(),
        ));
    }

    let extension = identity_extension(cert.extensions())?;
    if extension.len() > MAX_IDENTITY_EXTENSION_SIZE {
        return Err(FibreCertificateError::IdentityExtensionTooLarge(
            extension.len(),
        ));
    }

    let identity = decode_identity(extension)?;
    let payload = identity.payload.as_bytes();
    if payload.is_empty() {
        return Err(FibreCertificateError::EmptyIdentityPayload);
    }
    if payload.len() > MAX_PAYLOAD_DER_SIZE {
        return Err(FibreCertificateError::IdentityPayloadTooLarge(
            payload.len(),
        ));
    }
    if identity.signature.as_bytes().is_empty() {
        return Err(FibreCertificateError::EmptyIdentitySignature);
    }

    let binding = decode_binding(payload)?;
    if binding.version != BINDING_VERSION {
        return Err(FibreCertificateError::UnsupportedIdentityVersion(
            binding.version,
        ));
    }

    let mut sign_input = Vec::with_capacity(SIGN_PREFIX.len() + payload.len());
    sign_input.extend_from_slice(SIGN_PREFIX);
    sign_input.extend_from_slice(payload);
    let signed_bytes = raw_bytes_message_sign_bytes(chain_id, SIGN_UNIQUE_ID, &sign_input);
    let signature = Signature::from_slice(identity.signature.as_bytes())
        .map_err(FibreCertificateError::InvalidIdentitySignature)?;
    validator_key
        .verify_strict(&signed_bytes, &signature)
        .map_err(FibreCertificateError::InvalidIdentitySignature)?;

    if cert.tbs_certificate.subject_pki.raw != binding.tls_pub_key.as_bytes() {
        return Err(FibreCertificateError::TlsPublicKeyMismatch);
    }

    let not_before = i128::from(binding.not_before);
    let not_after = i128::from(binding.not_after);
    if not_after <= not_before {
        return Err(FibreCertificateError::EmptyValidityWindow {
            not_before,
            not_after,
        });
    }
    if not_after - not_before > MAX_CERT_VALIDITY_SECONDS {
        return Err(FibreCertificateError::ValidityWindowTooLong(
            not_after - not_before,
        ));
    }
    let now = i128::from(now.as_secs());
    if now < not_before - CLOCK_SKEW_SECONDS || now > not_after + CLOCK_SKEW_SECONDS {
        return Err(FibreCertificateError::OutsideValidityWindow {
            now,
            not_before,
            not_after,
        });
    }

    if cert.validity().not_before.timestamp() != binding.not_before
        || cert.validity().not_after.timestamp() != binding.not_after
    {
        return Err(FibreCertificateError::CertificateValidityMismatch {
            signed_not_before: binding.not_before,
            signed_not_after: binding.not_after,
            certificate_not_before: cert.validity().not_before.timestamp(),
            certificate_not_after: cert.validity().not_after.timestamp(),
        });
    }

    let has_server_auth = cert
        .extended_key_usage()
        .map_err(FibreCertificateError::ExtendedKeyUsage)?
        .is_some_and(|usage| usage.value.server_auth);
    if !has_server_auth {
        return Err(FibreCertificateError::MissingServerAuth);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde::Deserialize;
    #[cfg(not(target_arch = "wasm32"))]
    use tokio::net::TcpStream;

    use super::*;

    #[derive(Deserialize)]
    struct Vectors {
        cases: Vec<Vector>,
    }

    #[derive(Deserialize)]
    struct Vector {
        name: String,
        cert_der: String,
        verifier_chain_id: String,
        verifier_consensus_pub: String,
        verify_at: u64,
        signed_bytes: String,
        tls_priv_seed: String,
        expected: Expected,
    }

    #[derive(Deserialize)]
    struct Expected {
        valid: bool,
        error: Option<String>,
    }

    fn matches_error_code(error: &FibreCertificateError, code: &str) -> bool {
        match code {
            "extension_missing" => matches!(error, FibreCertificateError::MissingIdentityExtension),
            "extension_too_large" => {
                matches!(error, FibreCertificateError::IdentityExtensionTooLarge(_))
            }
            "extension_malformed" => {
                matches!(error, FibreCertificateError::IdentityDer(_))
            }
            "extension_trailing_data" => {
                matches!(error, FibreCertificateError::IdentityTrailingData(_))
            }
            "payload_empty" => matches!(error, FibreCertificateError::EmptyIdentityPayload),
            "payload_too_large" => {
                matches!(error, FibreCertificateError::IdentityPayloadTooLarge(_))
            }
            "signature_empty" => matches!(error, FibreCertificateError::EmptyIdentitySignature),
            "payload_malformed" => matches!(error, FibreCertificateError::BindingDer(_)),
            "binding_trailing_data" => {
                matches!(error, FibreCertificateError::BindingTrailingData(_))
            }
            "unsupported_version" => {
                matches!(error, FibreCertificateError::UnsupportedIdentityVersion(_))
            }
            "signature_invalid" => {
                matches!(error, FibreCertificateError::InvalidIdentitySignature(_))
            }
            "tls_key_mismatch" => matches!(error, FibreCertificateError::TlsPublicKeyMismatch),
            "window_empty" => matches!(error, FibreCertificateError::EmptyValidityWindow { .. }),
            "window_too_long" => matches!(error, FibreCertificateError::ValidityWindowTooLong(_)),
            "outside_validity_window" => {
                matches!(error, FibreCertificateError::OutsideValidityWindow { .. })
            }
            "cert_window_mismatch" => matches!(
                error,
                FibreCertificateError::CertificateValidityMismatch { .. }
            ),
            "eku_missing" => matches!(error, FibreCertificateError::MissingServerAuth),
            unknown => panic!("unknown upstream error code {unknown}"),
        }
    }

    fn vectors() -> Vectors {
        serde_json::from_str(include_str!("testdata/identity_vectors.json"))
            .expect("identity vectors should be valid JSON")
    }

    #[test]
    fn verifies_upstream_identity_vectors() {
        for vector in vectors().cases {
            let cert_der = hex::decode(&vector.cert_der).expect("certificate should be hex");
            let key_bytes: [u8; 32] = hex::decode(&vector.verifier_consensus_pub)
                .expect("validator key should be hex")
                .try_into()
                .expect("validator key should have 32 bytes");
            let key = VerifyingKey::from_bytes(&key_bytes).expect("validator key should be valid");
            let now = UnixTime::since_unix_epoch(Duration::from_secs(vector.verify_at));
            let result = verify_certificate(&cert_der, &key, &vector.verifier_chain_id, now);

            if vector.expected.valid {
                assert!(result.is_ok(), "vector {} returned {result:?}", vector.name);
                assert!(vector.expected.error.is_none());
            } else {
                let error = result.expect_err(&format!("vector {} should fail", vector.name));
                let expected = vector
                    .expected
                    .error
                    .as_deref()
                    .expect("invalid vector should name its expected error");
                assert!(
                    matches_error_code(&error, expected),
                    "vector {} returned {error:?}, expected {expected}",
                    vector.name
                );
            }
        }
    }

    #[test]
    fn rejects_duplicate_identity_extensions() {
        let vector = vectors()
            .cases
            .into_iter()
            .find(|vector| vector.name == "valid")
            .expect("valid vector should exist");
        let cert_der = hex::decode(vector.cert_der).expect("certificate should be hex");
        let (_, cert) = parse_x509_certificate(&cert_der).expect("certificate should parse");
        let extension = cert
            .extensions()
            .iter()
            .find(|extension| extension.oid.to_id_string() == IDENTITY_EXTENSION_OID)
            .expect("identity extension should exist")
            .clone();

        let error = identity_extension(&[extension.clone(), extension])
            .expect_err("duplicate identity extensions should fail");
        assert!(matches!(
            error,
            FibreCertificateError::DuplicateIdentityExtensions
        ));
    }

    #[test]
    fn rejects_malformed_certificate() {
        let key = VerifyingKey::from_bytes(&[1u8; 32]).expect("key should be valid");
        let now = UnixTime::since_unix_epoch(Duration::from_secs(0));
        assert!(matches!(
            verify_certificate(&[0], &key, "chain", now),
            Err(FibreCertificateError::CertificateParse(_))
        ));
    }

    #[test]
    fn rejects_trailing_certificate_data() {
        let vector = vectors()
            .cases
            .into_iter()
            .find(|vector| vector.name == "valid")
            .expect("valid vector should exist");
        let mut cert_der = hex::decode(&vector.cert_der).expect("certificate should be hex");
        cert_der.push(0);
        let key_bytes: [u8; 32] = hex::decode(&vector.verifier_consensus_pub)
            .expect("validator key should be hex")
            .try_into()
            .expect("validator key should have 32 bytes");
        let key = VerifyingKey::from_bytes(&key_bytes).expect("validator key should be valid");
        let now = UnixTime::since_unix_epoch(Duration::from_secs(vector.verify_at));
        assert!(matches!(
            verify_certificate(&cert_der, &key, &vector.verifier_chain_id, now),
            Err(FibreCertificateError::CertificateTrailingData(1))
        ));
    }

    #[test]
    fn rejects_malformed_extended_key_usage() {
        let vector = vectors()
            .cases
            .into_iter()
            .find(|vector| vector.name == "valid")
            .expect("valid vector should exist");
        let mut cert_der = hex::decode(&vector.cert_der).expect("certificate should be hex");
        let (_, cert) = parse_x509_certificate(&cert_der).expect("certificate should parse");
        let eku = cert
            .extensions()
            .iter()
            .find(|extension| extension.oid.to_id_string() == "2.5.29.37")
            .expect("certificate should contain extended key usage");
        let offset = cert_der
            .windows(eku.value.len())
            .position(|window| window == eku.value)
            .expect("extension value should occur in certificate");
        cert_der[offset] = 0xff;

        let key_bytes: [u8; 32] = hex::decode(&vector.verifier_consensus_pub)
            .expect("validator key should be hex")
            .try_into()
            .expect("validator key should have 32 bytes");
        let key = VerifyingKey::from_bytes(&key_bytes).expect("validator key should be valid");
        let now = UnixTime::since_unix_epoch(Duration::from_secs(vector.verify_at));
        assert!(matches!(
            verify_certificate(&cert_der, &key, &vector.verifier_chain_id, now),
            Err(FibreCertificateError::ExtendedKeyUsage(_))
        ));
    }

    #[test]
    fn certificate_error_maps_to_rustls_other() {
        let error: tokio_rustls::rustls::Error =
            FibreCertificateError::MissingIdentityExtension.into();
        let tokio_rustls::rustls::Error::InvalidCertificate(
            tokio_rustls::rustls::CertificateError::Other(other),
        ) = error
        else {
            panic!("expected InvalidCertificate(Other)")
        };
        assert!(
            other
                .0
                .downcast_ref::<FibreCertificateError>()
                .is_some_and(|error| matches!(
                    error,
                    FibreCertificateError::MissingIdentityExtension
                ))
        );
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn grpc_client_rejects_invalid_uri() {
        let url = "not a valid url \0".to_string();
        let key = VerifyingKey::from_bytes(&[1u8; 32]).expect("key should be valid");
        let error = grpc_client(
            url.clone(),
            key,
            "chain".to_string(),
            Arc::new(crate::transport::io_connector::NativeTcpConnector),
        )
        .expect_err("invalid URI should fail");
        assert!(matches!(
            error,
            FibreError::InvalidEndpoint { endpoint, .. } if endpoint == url
        ));
    }

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn grpc_client_rejects_uri_without_host() {
        let uri: http::Uri = "/relative".parse().expect("relative URI should parse");
        let key = VerifyingKey::from_bytes(&[1u8; 32]).expect("key should be valid");
        let error = grpc_client(
            uri.to_string(),
            key,
            "chain".to_string(),
            Arc::new(crate::transport::io_connector::NativeTcpConnector),
        )
        .expect_err("URI without host should fail");
        assert!(matches!(
            error,
            FibreError::EndpointMissingHost(value) if value == uri
        ));
    }

    #[test]
    fn comet_sign_bytes_match_upstream_vector() {
        let vector = vectors()
            .cases
            .into_iter()
            .find(|vector| vector.name == "valid")
            .expect("valid vector should exist");
        let cert_der = hex::decode(vector.cert_der).expect("certificate should be hex");
        let (_, cert) = parse_x509_certificate(&cert_der).expect("certificate should parse");
        let extension = cert
            .extensions()
            .iter()
            .find(|extension| extension.oid.to_id_string() == IDENTITY_EXTENSION_OID)
            .expect("identity extension should exist");
        let identity =
            SignedIdentity::from_der(extension.value).expect("identity extension should parse");
        let mut sign_input = SIGN_PREFIX.to_vec();
        sign_input.extend_from_slice(identity.payload.as_bytes());

        assert_eq!(
            raw_bytes_message_sign_bytes(&vector.verifier_chain_id, SIGN_UNIQUE_ID, &sign_input),
            hex::decode(vector.signed_bytes).expect("signed bytes should be hex")
        );
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn completes_full_tls_handshakes_with_endorsed_certificate() {
        use tokio_rustls::rustls::HandshakeKind;
        use tokio_rustls::rustls::ServerConfig;
        use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

        let vector = vectors()
            .cases
            .into_iter()
            .find(|vector| vector.name == "valid")
            .expect("valid vector should exist");
        let cert_der = hex::decode(&vector.cert_der).expect("certificate should be hex");
        let seed = hex::decode(&vector.tls_priv_seed).expect("TLS seed should be hex");
        let mut pkcs8 =
            hex::decode("302e020100300506032b657004220420").expect("PKCS#8 prefix should be hex");
        pkcs8.extend_from_slice(&seed);

        let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
        let mut server_config = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&tokio_rustls::rustls::version::TLS13])
            .expect("TLS 1.3 should be supported")
            .with_no_client_auth()
            .with_single_cert(
                vec![CertificateDer::from(cert_der)],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pkcs8)),
            )
            .expect("golden certificate and key should match");
        server_config.alpn_protocols = vec![b"h2".to_vec()];
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let address = listener.local_addr().expect("listener should have address");
        let server = tokio::spawn(async move {
            let mut handshake_kinds = Vec::new();
            for _ in 0..2 {
                let (tcp, _) = listener
                    .accept()
                    .await
                    .expect("TCP connection should arrive");
                let tls = acceptor
                    .accept(tcp)
                    .await
                    .expect("TLS handshake should succeed");
                handshake_kinds.push(tls.get_ref().1.handshake_kind());
            }
            handshake_kinds
        });

        let key_bytes: [u8; 32] = hex::decode(&vector.verifier_consensus_pub)
            .expect("validator key should be hex")
            .try_into()
            .expect("validator key should have 32 bytes");
        let fixed_time = UnixTime::since_unix_epoch(Duration::from_secs(vector.verify_at));
        let mut client_config = fibre_tls_config(
            VerifyingKey::from_bytes(&key_bytes).expect("validator key should be valid"),
            vector.verifier_chain_id,
            provider,
            Some(Arc::new(FixedTime(fixed_time))),
        );
        client_config.alpn_protocols = vec![b"h2".to_vec()];
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
        let server_name =
            ServerName::try_from("127.0.0.1".to_string()).expect("server name should be valid");

        for _ in 0..2 {
            let tcp = TcpStream::connect(address)
                .await
                .expect("TCP connection should succeed");
            let tls = connector
                .connect(server_name.clone(), tcp)
                .await
                .expect("endorsed TLS handshake should succeed");
            assert_eq!(tls.get_ref().1.handshake_kind(), Some(HandshakeKind::Full));
        }
        assert_eq!(
            server.await.expect("server task should finish"),
            vec![Some(HandshakeKind::Full), Some(HandshakeKind::Full)]
        );
    }
}
