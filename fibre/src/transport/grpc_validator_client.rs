//! Production gRPC transport implementation.
//!
//! [`GrpcValidatorConnector`] resolves validator addresses via a
//! [`HostRegistry`] and caches [`GrpcClient`] connections.
//! [`GrpcValidatorConnection`] uses [`GrpcClient`] methods to upload
//! and download shards over the Fibre gRPC service.

use std::collections::HashMap;
use std::sync::Arc;

use celestia_grpc::GrpcClient;

use crate::blob::BlobID;
use crate::error::FibreError;
use crate::host_registry::HostRegistry;
use crate::payment_promise::PaymentPromise;
use crate::proto_conv;
use crate::transport::io_connector::FibreIoConnector;
use crate::validator::ValidatorInfo;
use crate::validator_client::{
    DownloadResponse, UploadResponse, ValidatorConnection, ValidatorConnector,
};

/// Factory that resolves validator hosts and caches gRPC connections.
pub struct GrpcValidatorConnector {
    host_registry: Arc<dyn HostRegistry>,
    chain_id: String,
    io_connector: Arc<dyn FibreIoConnector>,
    connections: tokio::sync::Mutex<HashMap<[u8; 20], Arc<GrpcValidatorConnection>>>,
}

impl GrpcValidatorConnector {
    /// Create a new connector backed by the given host registry.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new(host_registry: Arc<dyn HostRegistry>, chain_id: impl Into<String>) -> Self {
        Self::new_with_io_connector(
            host_registry,
            chain_id,
            Arc::new(crate::transport::io_connector::NativeTcpConnector),
        )
    }

    /// Create a new connector over a caller-provided byte-stream transport.
    pub fn new_with_io_connector(
        host_registry: Arc<dyn HostRegistry>,
        chain_id: impl Into<String>,
        io_connector: Arc<dyn FibreIoConnector>,
    ) -> Self {
        Self {
            host_registry,
            chain_id: chain_id.into(),
            io_connector,
            connections: tokio::sync::Mutex::new(HashMap::new()),
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl ValidatorConnector for GrpcValidatorConnector {
    async fn connect(
        &self,
        validator: &ValidatorInfo,
    ) -> Result<Arc<dyn ValidatorConnection>, FibreError> {
        // Fast path: check cache under lock.
        {
            let cache = self.connections.lock().await;
            if let Some(conn) = cache.get(&validator.address) {
                return Ok(conn.clone() as Arc<dyn ValidatorConnection>);
            }
        }

        // Cache miss: resolve host and create a new channel.
        let host = self.host_registry.get_host(validator).await?;

        // Validators may register hosts in gRPC name-resolution format
        // (e.g. "dns:///1.2.3.4:9091"). Tonic expects a standard http(s)
        // URI, so normalise the address first.
        let url = normalize_host(&host.0);

        let client = crate::transport::tls::grpc_client(
            url,
            validator.pubkey,
            self.chain_id.clone(),
            self.io_connector.clone(),
        )?;

        let conn = Arc::new(GrpcValidatorConnection { client });

        // Re-check cache under lock: another task may have inserted a
        // connection for this validator while we were resolving/building.
        let mut cache = self.connections.lock().await;
        let conn = cache.entry(validator.address).or_insert(conn).clone();

        Ok(conn as Arc<dyn ValidatorConnection>)
    }
}

/// Normalise a host string into a standard `https://` URI.
fn normalize_host(raw: &str) -> String {
    let authority = raw
        .strip_prefix("dns:///")
        .or_else(|| raw.strip_prefix("dns://"))
        .or_else(|| raw.strip_prefix("http://"))
        .or_else(|| raw.strip_prefix("https://"))
        .unwrap_or(raw);

    format!("https://{authority}")
}

/// A connection to a single validator's Fibre gRPC service.
///
/// Wraps a [`GrpcClient`] for issuing upload/download RPCs.
pub struct GrpcValidatorConnection {
    client: GrpcClient,
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl ValidatorConnection for GrpcValidatorConnection {
    async fn upload_shard(
        &self,
        promise: &PaymentPromise,
        rows: &[rsema1d::RowInclusionProof],
        rlc_coeffs: &[rsema1d::GF128],
    ) -> Result<UploadResponse, FibreError> {
        let proto_promise = promise.into();
        let proto_shard = proto_conv::build_upload_shard(rows, rlc_coeffs);

        let request = celestia_proto::celestia::fibre::v1::UploadShardRequest {
            promise: Some(proto_promise),
            shard: Some(proto_shard),
        };

        let response = self.client.upload_shard(request).await?;

        Ok(UploadResponse {
            validator_signature: response.validator_signature,
        })
    }

    async fn download_shard(&self, blob_id: &BlobID) -> Result<DownloadResponse, FibreError> {
        let response = self
            .client
            .download_shard(blob_id.as_bytes().to_vec())
            .await?;

        proto_conv::parse_download_response(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::host_registry::Host;
    use crate::host_registry::HostRegistry;
    use crate::test_utils::make_validator;

    #[cfg(not(target_arch = "wasm32"))]
    mod wire {
        use std::num::NonZeroU64;
        use std::sync::Mutex;
        use std::time::{Duration, UNIX_EPOCH};

        use celestia_proto::celestia::fibre::v1::fibre_server::{Fibre, FibreServer};
        use celestia_proto::celestia::fibre::v1::{
            BlobShard, DownloadShardRequest, DownloadShardResponse, UploadShardRequest,
            UploadShardResponse,
        };
        use hyper_util::rt::{TokioExecutor, TokioIo};
        use hyper_util::service::TowerToHyperService;
        use k256::ecdsa::SigningKey;
        use serde::Deserialize;
        use tokio::io::DuplexStream;
        use tokio_rustls::rustls::ServerConfig;
        use tokio_rustls::rustls::pki_types::{
            CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, UnixTime,
        };

        use super::*;
        use crate::blob::{BlobID, EncodedBlob};
        use crate::config::BlobConfig;
        use crate::error::ShardError;
        use crate::payment_promise::PaymentPromise;
        use crate::transport::io_connector::{BoxedFibreIo, FibreIoConnector};

        const MALFORMED_COMMITMENT: [u8; 32] = [2; 32];
        const TIMEOUT_COMMITMENT: [u8; 32] = [3; 32];

        struct DuplexConnector(Mutex<Option<DuplexStream>>);

        #[async_trait::async_trait]
        impl FibreIoConnector for DuplexConnector {
            async fn connect(
                &self,
                _host: String,
                _port: u16,
            ) -> Result<BoxedFibreIo, std::io::Error> {
                let stream = self
                    .0
                    .lock()
                    .unwrap()
                    .take()
                    .expect("test client should open one connection");
                Ok(Box::pin(stream))
            }
        }

        struct WireService {
            upload: Mutex<Option<UploadShardRequest>>,
            downloads: Mutex<Vec<Vec<u8>>>,
            valid_blob_id: Vec<u8>,
            shard: BlobShard,
        }

        #[async_trait::async_trait]
        impl Fibre for WireService {
            async fn upload_shard(
                self: Arc<Self>,
                request: tonic::Request<UploadShardRequest>,
            ) -> Result<tonic::Response<UploadShardResponse>, tonic::Status> {
                *self.upload.lock().unwrap() = Some(request.into_inner());
                Ok(tonic::Response::new(UploadShardResponse {
                    validator_signature: vec![7; 64],
                }))
            }

            async fn download_shard(
                self: Arc<Self>,
                request: tonic::Request<DownloadShardRequest>,
            ) -> Result<tonic::Response<DownloadShardResponse>, tonic::Status> {
                let blob_id = request.into_inner().blob_id;
                self.downloads.lock().unwrap().push(blob_id.clone());
                if blob_id == self.valid_blob_id {
                    return Ok(tonic::Response::new(DownloadShardResponse {
                        shard: Some(self.shard.clone()),
                    }));
                }
                let commitment = &blob_id[1..];
                if commitment == MALFORMED_COMMITMENT {
                    return Ok(tonic::Response::new(DownloadShardResponse { shard: None }));
                }
                if commitment == TIMEOUT_COMMITMENT {
                    std::future::pending().await
                } else {
                    Err(tonic::Status::not_found("unknown blob"))
                }
            }
        }

        #[derive(Deserialize)]
        struct IdentityVectors {
            cases: Vec<IdentityVector>,
        }

        #[derive(Deserialize)]
        struct IdentityVector {
            name: String,
            cert_der: String,
            tls_priv_seed: String,
            verifier_chain_id: String,
            verifier_consensus_pub: String,
            verify_at: u64,
        }

        fn identity() -> IdentityVector {
            serde_json::from_str::<IdentityVectors>(include_str!("testdata/identity_vectors.json"))
                .unwrap()
                .cases
                .into_iter()
                .find(|vector| vector.name == "valid")
                .unwrap()
        }

        fn tls_acceptor(vector: &IdentityVector) -> tokio_rustls::TlsAcceptor {
            let cert_der = hex::decode(&vector.cert_der).unwrap();
            let seed = hex::decode(&vector.tls_priv_seed).unwrap();
            let mut pkcs8 = hex::decode("302e020100300506032b657004220420").unwrap();
            pkcs8.extend_from_slice(&seed);
            let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
            let mut config = ServerConfig::builder_with_provider(provider)
                .with_protocol_versions(&[&tokio_rustls::rustls::version::TLS13])
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(
                    vec![CertificateDer::from(cert_der)],
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(pkcs8)),
                )
                .unwrap();
            config.alpn_protocols = vec![b"h2".to_vec()];
            tokio_rustls::TlsAcceptor::from(Arc::new(config))
        }

        #[tokio::test]
        async fn grpc_tls_roundtrip_rejects_malformed_response_and_times_out() {
            let cfg = BlobConfig::new_test(0, 4, 4, 4096, 4, 64);
            let blob = EncodedBlob::new(b"wire payload", cfg).unwrap();
            let valid_id = blob.id().clone();
            let rows = vec![blob.row(0).unwrap()];
            let shard = proto_conv::build_upload_shard(&rows, blob.rlc_coeffs());
            let signing_key = SigningKey::from_slice(&[9; 32]).unwrap();
            let mut promise = PaymentPromise {
                chain_id: "wire-chain".into(),
                height: NonZeroU64::new(42).unwrap(),
                namespace: celestia_types::nmt::Namespace::const_v0([4; 10]),
                upload_size: blob.upload_size() as u32,
                blob_version: 0,
                commitment: valid_id.commitment(),
                creation_timestamp: UNIX_EPOCH + Duration::from_secs(1_700_000_000),
                signer_pubkey: *signing_key.verifying_key(),
                signature: None,
            };
            promise.sign(&signing_key).unwrap();
            let expected_upload = UploadShardRequest {
                promise: Some((&promise).into()),
                shard: Some(shard.clone()),
            };

            let service = Arc::new(WireService {
                upload: Mutex::new(None),
                downloads: Mutex::new(Vec::new()),
                valid_blob_id: valid_id.as_bytes().to_vec(),
                shard,
            });
            let (client_io, server_io) = tokio::io::duplex(64 * 1024);
            let vector = identity();
            let acceptor = tls_acceptor(&vector);
            let server_service = FibreServer::from_arc(service.clone());
            let server = tokio::spawn(async move {
                let tls = acceptor.accept(server_io).await.unwrap();
                hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(tls), TowerToHyperService::new(server_service))
                    .await
            });

            let key_bytes: [u8; 32] = hex::decode(&vector.verifier_consensus_pub)
                .unwrap()
                .try_into()
                .unwrap();
            let client = crate::transport::tls::grpc_client_at(
                "https://wire.test:443".into(),
                ed25519_dalek::VerifyingKey::from_bytes(&key_bytes).unwrap(),
                vector.verifier_chain_id,
                Arc::new(DuplexConnector(Mutex::new(Some(client_io)))),
                UnixTime::since_unix_epoch(Duration::from_secs(vector.verify_at)),
            )
            .unwrap();
            let connection = GrpcValidatorConnection { client };

            let upload = connection
                .upload_shard(&promise, &rows, blob.rlc_coeffs())
                .await
                .unwrap();
            assert_eq!(upload.validator_signature, vec![7; 64]);
            assert_eq!(*service.upload.lock().unwrap(), Some(expected_upload));

            let download = connection.download_shard(&valid_id).await.unwrap();
            assert_eq!(download.rows[0].index, rows[0].index);
            assert_eq!(download.rows[0].row.as_ref(), rows[0].row);
            assert_eq!(download.rows[0].row_proof, rows[0].row_proof);
            assert_eq!(download.rlcs, blob.rlc_coeffs());

            let malformed_id = BlobID::new(0, MALFORMED_COMMITMENT);
            assert!(matches!(
                connection.download_shard(&malformed_id).await,
                Err(FibreError::InvalidShard(ShardError::MissingShard))
            ));

            let timeout_id = BlobID::new(0, TIMEOUT_COMMITMENT);
            let error = connection
                .client
                .download_shard(timeout_id.as_bytes().to_vec())
                .timeout(Duration::from_millis(100))
                .await
                .unwrap_err();
            assert!(matches!(
                error,
                celestia_grpc::Error::TonicError(status)
                    if matches!(status.code(), tonic::Code::DeadlineExceeded | tonic::Code::Cancelled)
            ));

            assert_eq!(
                *service.downloads.lock().unwrap(),
                vec![
                    valid_id.as_bytes().to_vec(),
                    malformed_id.as_bytes().to_vec(),
                    timeout_id.as_bytes().to_vec(),
                ]
            );
            server.abort();
        }
    }

    struct MockHostRegistry {
        hosts: std::collections::HashMap<[u8; 20], Host>,
        call_count: AtomicUsize,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl HostRegistry for MockHostRegistry {
        async fn get_host(&self, validator: &ValidatorInfo) -> Result<Host, FibreError> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            self.hosts
                .get(&validator.address)
                .cloned()
                .ok_or(FibreError::HostNotFound(validator.address))
        }
    }

    #[tokio::test]
    async fn connector_caches_connections() {
        let validator = make_validator(100, 1).1;

        let mut hosts = std::collections::HashMap::new();
        hosts.insert(validator.address, Host("http://127.0.0.1:9090".to_string()));

        let registry = Arc::new(MockHostRegistry {
            hosts,
            call_count: AtomicUsize::new(0),
        });

        let connector = GrpcValidatorConnector::new(registry.clone(), "test-chain");

        // First connect should call the registry.
        let conn1 = connector.connect(&validator).await;
        assert!(conn1.is_ok(), "first connect should succeed");

        // Second connect should hit the cache.
        let conn2 = connector.connect(&validator).await;
        assert!(conn2.is_ok(), "second connect should succeed");

        // The registry should only have been called once.
        assert_eq!(
            registry.call_count.load(Ordering::SeqCst),
            1,
            "registry should only be called once due to caching"
        );
    }

    #[tokio::test]
    async fn connector_creates_separate_connections_for_different_validators() {
        let validator_a = make_validator(100, 1).1;
        let validator_b = make_validator(100, 2).1;

        let mut hosts = std::collections::HashMap::new();
        hosts.insert(
            validator_a.address,
            Host("http://127.0.0.1:9090".to_string()),
        );
        hosts.insert(
            validator_b.address,
            Host("http://127.0.0.1:9091".to_string()),
        );

        let registry = Arc::new(MockHostRegistry {
            hosts,
            call_count: AtomicUsize::new(0),
        });

        let connector = GrpcValidatorConnector::new(registry.clone(), "test-chain");

        let conn_a = connector.connect(&validator_a).await;
        assert!(conn_a.is_ok(), "connect to validator A should succeed");

        let conn_b = connector.connect(&validator_b).await;
        assert!(conn_b.is_ok(), "connect to validator B should succeed");

        // Both validators should have triggered a registry lookup.
        assert_eq!(
            registry.call_count.load(Ordering::SeqCst),
            2,
            "registry should be called once per distinct validator"
        );
    }

    #[tokio::test]
    async fn connector_propagates_host_not_found_error() {
        let validator = make_validator(100, 42).1;

        // Empty hosts map: every lookup will fail with HostNotFound.
        let registry = Arc::new(MockHostRegistry {
            hosts: std::collections::HashMap::new(),
            call_count: AtomicUsize::new(0),
        });

        let connector = GrpcValidatorConnector::new(registry.clone(), "test-chain");

        let result = connector.connect(&validator).await;
        match result {
            Err(FibreError::HostNotFound(addr)) => {
                assert_eq!(addr, validator.address);
            }
            Err(other) => panic!("expected HostNotFound error, got: {other}"),
            Ok(_) => panic!("expected connect to fail for unknown validator"),
        }
    }

    #[test]
    fn normalize_host_strips_dns_prefix() {
        assert_eq!(
            normalize_host("dns:///138.68.236.99:9091"),
            "https://138.68.236.99:9091"
        );
        assert_eq!(
            normalize_host("dns://138.68.236.99:9091"),
            "https://138.68.236.99:9091"
        );
    }

    #[test]
    fn normalize_host_uses_https() {
        assert_eq!(
            normalize_host("http://127.0.0.1:9090"),
            "https://127.0.0.1:9090"
        );
        assert_eq!(
            normalize_host("https://validator.example.com:9090"),
            "https://validator.example.com:9090"
        );
    }

    #[test]
    fn normalize_host_adds_https_to_bare() {
        assert_eq!(
            normalize_host("138.68.236.99:9091"),
            "https://138.68.236.99:9091"
        );
    }
}
