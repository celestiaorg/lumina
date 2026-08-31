use std::error::Error;
use std::io;
use std::sync::Arc;

use der::asn1::OctetStringRef;
use der::{Decode, Sequence};
use ed25519_dalek::{Signature, VerifyingKey};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::crypto::CryptoProvider;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use tonic::transport::{Endpoint, Uri};
use tower::service_fn;
use x509_parser::prelude::parse_x509_certificate;

use crate::error::FibreError;
use crate::payment_promise::raw_bytes_message_sign_bytes;

const IDENTITY_EXTENSION_OID: &str = "1.3.6.1.4.1.66463.1.1";
const SIGN_UNIQUE_ID: &[u8] = b"celestia-fibre-tls-v1";
const SIGN_PREFIX: &[u8] = b"celestia-fibre-tls:";
const BINDING_VERSION: i64 = 1;
const MAX_IDENTITY_EXTENSION_SIZE: usize = 8192;
const MAX_PAYLOAD_DER_SIZE: usize = 4096;
const MAX_CERT_VALIDITY_SECONDS: i128 = 31_536_600;
const CLOCK_SKEW_SECONDS: i128 = 300;

type BoxError = Box<dyn Error + Send + Sync>;

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
) -> Result<celestia_grpc::GrpcClient, FibreError> {
    let endpoint = Endpoint::from_shared(url.to_string())?;
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let verifier = FibreServerCertVerifier {
        validator_key,
        chain_id,
        provider: provider.clone(),
    };
    let mut tls_config = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&tokio_rustls::rustls::version::TLS13])
        .map_err(|error| FibreError::Other(format!("failed to configure Fibre TLS: {error}")))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    tls_config.alpn_protocols = vec![b"h2".to_vec()];
    let tls_connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));

    let connector = service_fn(move |uri: Uri| {
        let tls_connector = tls_connector.clone();
        async move {
            let authority = uri.authority().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Fibre endpoint has no authority",
                )
            })?;
            let host = uri.host().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "Fibre endpoint has no host")
            })?;
            let server_name = ServerName::try_from(host.to_string())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            let tcp = TcpStream::connect(authority.as_str()).await?;
            let tls = tls_connector
                .connect(server_name, tcp)
                .await
                .map_err(BoxError::from)?;

            Ok::<_, BoxError>(TokioIo::new(tls))
        }
    });
    let channel = endpoint.connect_with_connector_lazy(connector);

    celestia_grpc::GrpcClient::builder()
        .transport(channel)
        .build()
        .map_err(|error| FibreError::Other(format!("failed to build Fibre gRPC client: {error}")))
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

    let extension = cert
        .extensions()
        .iter()
        .find(|extension| extension.oid.to_id_string() == IDENTITY_EXTENSION_OID)
        .ok_or_else(|| "peer cert is missing the fibre identity extension".to_string())?;
    if extension.value.len() > MAX_IDENTITY_EXTENSION_SIZE {
        return Err(format!(
            "identity extension size {} exceeds maximum {MAX_IDENTITY_EXTENSION_SIZE}",
            extension.value.len()
        ));
    }

    let identity = SignedIdentity::from_der(extension.value)
        .map_err(|error| format!("unmarshal identity extension: {error}"))?;
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

    let binding = BindingPayload::from_der(payload)
        .map_err(|error| format!("unmarshal binding payload: {error}"))?;
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

            assert_eq!(
                result.is_ok(),
                vector.expected.valid,
                "vector {} returned {result:?}",
                vector.name
            );
        }
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

    #[derive(Debug)]
    struct FixedTime(UnixTime);

    impl tokio_rustls::rustls::time_provider::TimeProvider for FixedTime {
        fn current_time(&self) -> Option<UnixTime> {
            Some(self.0)
        }
    }

    #[tokio::test]
    async fn completes_tls_handshake_with_endorsed_certificate() {
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
            let (tcp, _) = listener
                .accept()
                .await
                .expect("TCP connection should arrive");
            acceptor
                .accept(tcp)
                .await
                .expect("TLS handshake should succeed")
        });

        let key_bytes: [u8; 32] = hex::decode(&vector.verifier_consensus_pub)
            .expect("validator key should be hex")
            .try_into()
            .expect("validator key should have 32 bytes");
        let verifier = FibreServerCertVerifier {
            validator_key: VerifyingKey::from_bytes(&key_bytes)
                .expect("validator key should be valid"),
            chain_id: vector.verifier_chain_id,
            provider: provider.clone(),
        };
        let fixed_time = UnixTime::since_unix_epoch(Duration::from_secs(vector.verify_at));
        let mut client_config =
            ClientConfig::builder_with_details(provider, Arc::new(FixedTime(fixed_time)))
                .with_protocol_versions(&[&tokio_rustls::rustls::version::TLS13])
                .expect("TLS 1.3 should be supported")
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth();
        client_config.alpn_protocols = vec![b"h2".to_vec()];
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
        let tcp = TcpStream::connect(address)
            .await
            .expect("TCP connection should succeed");
        let server_name =
            ServerName::try_from("127.0.0.1".to_string()).expect("server name should be valid");

        connector
            .connect(server_name, tcp)
            .await
            .expect("endorsed TLS handshake should succeed");
        server.await.expect("server task should finish");
    }
}
