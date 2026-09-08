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
        )
        .map_err(tokio_rustls::rustls::Error::General)?;

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
    url: &str,
    validator_key: VerifyingKey,
    chain_id: String,
    io_connector: Arc<dyn FibreIoConnector>,
) -> Result<celestia_grpc::GrpcClient, FibreError> {
    let uri = url
        .parse::<http::Uri>()
        .map_err(|error| FibreError::Other(format!("invalid Fibre endpoint '{url}': {error}")))?;
    let host = uri
        .host()
        .ok_or_else(|| FibreError::Other(format!("Fibre endpoint '{url}' has no host")))?
        .to_string();
    let port = uri.port_u16().unwrap_or(443);
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let mut tls_config = fibre_tls_config(validator_key, chain_id, provider, None)?;
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
        .map_err(|error| FibreError::Other(format!("failed to build Fibre gRPC client: {error}")))
}

fn fibre_tls_config(
    validator_key: VerifyingKey,
    chain_id: String,
    provider: Arc<CryptoProvider>,
    time_provider: Option<Arc<dyn tokio_rustls::rustls::time_provider::TimeProvider>>,
) -> Result<ClientConfig, FibreError> {
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
        .map_err(|error| FibreError::Other(format!("failed to configure Fibre TLS: {error}")))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    tls_config.resumption = Resumption::disabled();
    Ok(tls_config)
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
) -> Result<&'a [u8], String> {
    let mut matching = extensions
        .iter()
        .filter(|extension| extension.oid.to_id_string() == IDENTITY_EXTENSION_OID);
    let extension = matching
        .next()
        .ok_or_else(|| "peer cert is missing the fibre identity extension".to_string())?;
    if matching.next().is_some() {
        return Err("peer cert has duplicate fibre identity extensions".to_string());
    }
    Ok(extension.value)
}

fn decode_identity(extension: &[u8]) -> Result<SignedIdentity<'_>, String> {
    SignedIdentity::from_der(extension).map_err(|error| {
        if matches!(error.kind(), der::ErrorKind::TrailingData { .. }) {
            "trailing bytes in identity extension".to_string()
        } else {
            format!("unmarshal identity extension: {error}")
        }
    })
}

fn decode_binding(payload: &[u8]) -> Result<BindingPayload<'_>, String> {
    BindingPayload::from_der(payload).map_err(|error| {
        if matches!(error.kind(), der::ErrorKind::TrailingData { .. }) {
            "trailing bytes in binding payload".to_string()
        } else {
            format!("unmarshal binding payload: {error}")
        }
    })
}

fn verify_certificate(
    cert_der: &[u8],
    validator_key: &VerifyingKey,
    chain_id: &str,
    now: UnixTime,
) -> Result<(), String> {
    let (remaining, cert) =
        parse_x509_certificate(cert_der).map_err(|error| format!("parse peer cert: {error}"))?;
    if !remaining.is_empty() {
        return Err("trailing bytes in peer cert".to_string());
    }

    let extension = identity_extension(cert.extensions())?;
    if extension.len() > MAX_IDENTITY_EXTENSION_SIZE {
        return Err(format!(
            "identity extension size {} exceeds maximum {MAX_IDENTITY_EXTENSION_SIZE}",
            extension.len()
        ));
    }

    let identity = decode_identity(extension)?;
    let payload = identity.payload.as_bytes();
    if payload.is_empty() {
        return Err("empty identity payload".to_string());
    }
    if payload.len() > MAX_PAYLOAD_DER_SIZE {
        return Err(format!(
            "identity payload size {} exceeds maximum {MAX_PAYLOAD_DER_SIZE}",
            payload.len()
        ));
    }
    if identity.signature.as_bytes().is_empty() {
        return Err("empty identity signature".to_string());
    }

    let binding = decode_binding(payload)?;
    if binding.version != BINDING_VERSION {
        return Err(format!(
            "unsupported fibre identity version {}",
            binding.version
        ));
    }

    let mut sign_input = Vec::with_capacity(SIGN_PREFIX.len() + payload.len());
    sign_input.extend_from_slice(SIGN_PREFIX);
    sign_input.extend_from_slice(payload);
    let signed_bytes = raw_bytes_message_sign_bytes(chain_id, SIGN_UNIQUE_ID, &sign_input);
    let signature = Signature::from_slice(identity.signature.as_bytes())
        .map_err(|_| "peer cert signature is invalid".to_string())?;
    validator_key
        .verify_strict(&signed_bytes, &signature)
        .map_err(|_| "peer cert signature is invalid".to_string())?;

    if cert.tbs_certificate.subject_pki.raw != binding.tls_pub_key.as_bytes() {
        return Err("peer cert public key does not match signed identity".to_string());
    }

    let not_before = i128::from(binding.not_before);
    let not_after = i128::from(binding.not_after);
    if not_after <= not_before {
        return Err("fibre identity validity window is empty".to_string());
    }
    if not_after - not_before > MAX_CERT_VALIDITY_SECONDS {
        return Err("fibre identity validity window exceeds maximum".to_string());
    }
    let now = i128::from(now.as_secs());
    if now < not_before - CLOCK_SKEW_SECONDS || now > not_after + CLOCK_SKEW_SECONDS {
        return Err("peer fibre identity is not currently valid".to_string());
    }

    if cert.validity().not_before.timestamp() != binding.not_before
        || cert.validity().not_after.timestamp() != binding.not_after
    {
        return Err("certificate validity does not match signed identity".to_string());
    }

    let has_server_auth = cert
        .extended_key_usage()
        .map_err(|error| format!("parse peer cert extended key usage: {error}"))?
        .is_some_and(|usage| usage.value.server_auth);
    if !has_server_auth {
        return Err("peer cert missing serverAuth extended key usage".to_string());
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

    fn expected_error_fragment(error: &str) -> &str {
        match error {
            "extension_missing" => "missing the fibre identity extension",
            "extension_too_large" => "identity extension size",
            "extension_malformed" => "unmarshal identity extension",
            "extension_trailing_data" => "trailing bytes in identity extension",
            "payload_empty" => "empty identity payload",
            "payload_too_large" => "identity payload size",
            "signature_empty" => "empty identity signature",
            "payload_malformed" => "unmarshal binding payload",
            "binding_trailing_data" => "trailing bytes in binding payload",
            "unsupported_version" => "unsupported fibre identity version",
            "signature_invalid" => "signature is invalid",
            "tls_key_mismatch" => "public key does not match signed identity",
            "window_empty" => "validity window is empty",
            "window_too_long" => "validity window exceeds maximum",
            "outside_validity_window" => "not currently valid",
            "cert_window_mismatch" => "certificate validity does not match signed identity",
            "eku_missing" => "serverAuth",
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
                    error.contains(expected_error_fragment(expected)),
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
        assert!(error.contains("duplicate fibre identity extensions"));
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
    #[derive(Debug)]
    struct FixedTime(UnixTime);

    #[cfg(not(target_arch = "wasm32"))]
    impl tokio_rustls::rustls::time_provider::TimeProvider for FixedTime {
        fn current_time(&self) -> Option<UnixTime> {
            Some(self.0)
        }
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
        )
        .expect("TLS configuration should succeed");
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
