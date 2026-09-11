//! Endorsed TLS identities for the mock data plane.
//!
//! The fibre client forces TLS 1.3 on every validator connection
//! (`fibre/src/transport/tls.rs`), so each mock validator serves a self-signed
//! ed25519 cert carrying the fibre identity extension: a binding payload
//! signed by the validator's consensus key over CometBFT raw-bytes sign bytes.
//! Constants and DER shapes replicate the client verifier exactly.

use std::time::{SystemTime, UNIX_EPOCH};

use der::Encode;
use der::asn1::OctetString;
use ed25519_dalek::Signer;

/// OID of the fibre identity extension (`fibre/src/transport/tls.rs`).
const IDENTITY_EXTENSION_OID: &[u64] = &[1, 3, 6, 1, 4, 1, 66463, 1, 1];
const SIGN_UNIQUE_ID: &[u8] = b"celestia-fibre-tls-v1";
const SIGN_PREFIX: &[u8] = b"celestia-fibre-tls:";
const BINDING_VERSION: i64 = 1;
/// One year; must stay under the verifier's 31_536_600s maximum.
const CERT_VALIDITY_SECONDS: i64 = 31_536_000;
/// Backdate not_before so clients with mild clock skew accept the cert.
const NOT_BEFORE_BACKDATE_SECONDS: i64 = 300;

/// CometBFT's domain separation prefix for raw-bytes signing. Replicates the
/// pub(crate) `raw_bytes_message_sign_bytes` in `fibre/src/domain/payment_promise.rs`.
const COMET_RAW_BYTES_PREFIX: &[u8] = b"COMET::RAW_BYTES::SIGN";

fn raw_bytes_message_sign_bytes(chain_id: &str, unique_id: &[u8], raw_bytes: &[u8]) -> Vec<u8> {
    use celestia_proto::tendermint_celestia_mods::privval::SignRawBytesRequest;
    use prost::Message;

    let request = SignRawBytesRequest {
        chain_id: chain_id.to_string(),
        raw_bytes: raw_bytes.to_vec(),
        unique_id: String::from_utf8_lossy(unique_id).into_owned(),
    };

    let mut result = Vec::with_capacity(COMET_RAW_BYTES_PREFIX.len() + request.encoded_len() + 5);
    result.extend_from_slice(COMET_RAW_BYTES_PREFIX);
    request
        .encode_length_delimited(&mut result)
        .expect("encoding SignRawBytesRequest into Vec cannot fail");

    result
}

/// Owned mirror of the client's `SignedIdentity` (`fibre/src/transport/tls.rs`).
#[derive(der::Sequence)]
struct SignedIdentity {
    payload: OctetString,
    signature: OctetString,
}

/// Owned mirror of the client's `BindingPayload`.
#[derive(der::Sequence)]
struct BindingPayload {
    version: i64,
    not_before: i64,
    not_after: i64,
    tls_pub_key: OctetString,
}

struct EndorsedCert {
    cert: rcgen::Certificate,
    key_pair: rcgen::KeyPair,
    /// The DER binding payload, kept for the unit test's cross-checks.
    #[cfg_attr(not(test), allow(dead_code))]
    binding_der: Vec<u8>,
}

fn generate(
    consensus_key: &ed25519_dalek::SigningKey,
    chain_id: &str,
) -> anyhow::Result<EndorsedCert> {
    let key_pair = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519)?;
    let spki = key_pair.public_key_der();

    let now: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .try_into()?;
    let not_before = now - NOT_BEFORE_BACKDATE_SECONDS;
    let not_after = not_before + CERT_VALIDITY_SECONDS;

    let binding_der = BindingPayload {
        version: BINDING_VERSION,
        not_before,
        not_after,
        tls_pub_key: OctetString::new(spki)?,
    }
    .to_der()?;

    let mut sign_input = Vec::with_capacity(SIGN_PREFIX.len() + binding_der.len());
    sign_input.extend_from_slice(SIGN_PREFIX);
    sign_input.extend_from_slice(&binding_der);
    let signed_bytes = raw_bytes_message_sign_bytes(chain_id, SIGN_UNIQUE_ID, &sign_input);
    let signature = consensus_key.sign(&signed_bytes);

    let identity_der = SignedIdentity {
        payload: OctetString::new(binding_der.clone())?,
        signature: OctetString::new(signature.to_vec())?,
    }
    .to_der()?;

    let mut params = rcgen::CertificateParams::default();
    // Cert validity must equal the binding to the second; the verifier
    // compares raw timestamps, and rcgen's UTCTime keeps second precision.
    params.not_before = time::OffsetDateTime::from_unix_timestamp(not_before)?;
    params.not_after = time::OffsetDateTime::from_unix_timestamp(not_after)?;
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    params.custom_extensions = vec![rcgen::CustomExtension::from_oid_content(
        IDENTITY_EXTENSION_OID,
        identity_der,
    )];
    let cert = params.self_signed(&key_pair)?;

    Ok(EndorsedCert {
        cert,
        key_pair,
        binding_der,
    })
}

/// Generate a fresh TLS keypair and a self-signed cert whose fibre identity
/// extension is endorsed by `consensus_key` for `chain_id`.
pub fn endorsed_identity(
    consensus_key: &ed25519_dalek::SigningKey,
    chain_id: &str,
) -> anyhow::Result<tonic::transport::Identity> {
    let endorsed = generate(consensus_key, chain_id)?;
    Ok(tonic::transport::Identity::from_pem(
        endorsed.cert.pem(),
        endorsed.key_pair.serialize_pem(),
    ))
}

#[cfg(test)]
mod tests {
    use der::Decode;
    use der::asn1::OctetStringRef;
    use ed25519_dalek::Verifier;
    use x509_parser::prelude::{FromDer, X509Certificate};

    use super::*;

    #[derive(der::Sequence)]
    struct SignedIdentityRef<'a> {
        payload: OctetStringRef<'a>,
        signature: OctetStringRef<'a>,
    }

    /// Replicates the checks of the client's `verify_certificate`
    /// (`fibre/src/transport/tls.rs`); the e2e handshake is the real proof.
    #[test]
    fn generated_cert_passes_verifier_checks() {
        let consensus_key = ed25519_dalek::SigningKey::from_bytes(&[3u8; 32]);
        let endorsed = generate(&consensus_key, "mock-1").unwrap();

        let cert_der = endorsed.cert.der().to_vec();
        let (rem, cert) = X509Certificate::from_der(&cert_der).unwrap();
        assert!(rem.is_empty());

        let ext = cert
            .extensions()
            .iter()
            .find(|e| e.oid.to_id_string() == "1.3.6.1.4.1.66463.1.1")
            .expect("identity extension present");

        let signed = SignedIdentityRef::from_der(ext.value).unwrap();
        let payload = signed.payload.as_bytes();
        assert_eq!(payload, endorsed.binding_der.as_slice());

        // Consensus signature verifies over the recomputed sign bytes.
        let mut sign_input = Vec::new();
        sign_input.extend_from_slice(SIGN_PREFIX);
        sign_input.extend_from_slice(payload);
        let signed_bytes = raw_bytes_message_sign_bytes("mock-1", SIGN_UNIQUE_ID, &sign_input);
        let sig = ed25519_dalek::Signature::from_slice(signed.signature.as_bytes()).unwrap();
        consensus_key
            .verifying_key()
            .verify(&signed_bytes, &sig)
            .unwrap();

        // SPKI matches the binding's tls_pub_key.
        let binding = BindingPayload::from_der(payload).unwrap();
        assert_eq!(
            cert.tbs_certificate.subject_pki.raw,
            binding.tls_pub_key.as_bytes()
        );
        assert_eq!(binding.version, BINDING_VERSION);

        // Cert validity equals the binding exactly, inside the max window.
        assert_eq!(cert.validity().not_before.timestamp(), binding.not_before);
        assert_eq!(cert.validity().not_after.timestamp(), binding.not_after);
        assert!(binding.not_after - binding.not_before <= 31_536_600);

        // serverAuth EKU present.
        assert!(
            cert.extended_key_usage()
                .unwrap()
                .expect("EKU present")
                .value
                .server_auth
        );
    }
}
