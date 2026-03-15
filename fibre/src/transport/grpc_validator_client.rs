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
use crate::validator::ValidatorInfo;
use crate::validator_client::{
    DownloadResponse, DownloadedRow, UploadResponse, ValidatorConnection, ValidatorConnector,
};

/// Factory that resolves validator hosts and caches gRPC connections.
pub struct GrpcValidatorConnector {
    host_registry: Arc<dyn HostRegistry>,
    connections: tokio::sync::Mutex<HashMap<[u8; 20], Arc<GrpcValidatorConnection>>>,
}

impl GrpcValidatorConnector {
    /// Create a new connector backed by the given host registry.
    pub fn new(host_registry: Arc<dyn HostRegistry>) -> Self {
        Self {
            host_registry,
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

        let client = GrpcClient::builder()
            .url(&url)
            .build()
            .map_err(|e| FibreError::Other(format!("invalid endpoint '{}': {e}", url)))?;

        let conn = Arc::new(GrpcValidatorConnection { client });

        // Re-check cache under lock: another task may have inserted a
        // connection for this validator while we were resolving/building.
        let mut cache = self.connections.lock().await;
        let conn = cache
            .entry(validator.address)
            .or_insert(conn)
            .clone();

        Ok(conn as Arc<dyn ValidatorConnection>)
    }
}

/// Normalise a host string into a standard `http://` URI.
fn normalize_host(raw: &str) -> String {
    // Strip the "dns:///" (or "dns://") prefix if present.
    if let Some(rest) = raw
        .strip_prefix("dns:///")
        .or_else(|| raw.strip_prefix("dns://"))
    {
        return format!("http://{rest}");
    }
    // Already a normal URI (http:// or https://).
    if raw.starts_with("http://") || raw.starts_with("https://") {
        return raw.to_string();
    }
    // Bare host:port — assume http.
    format!("http://{raw}")
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

        let proofs = proto_conv::parse_download_response(response)?;

        Ok(DownloadResponse {
            rows: proofs
                .into_iter()
                .map(|proof| DownloadedRow { proof })
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::host_registry::Host;
    use crate::host_registry::HostRegistry;

    struct MockHostRegistry {
        hosts: std::collections::HashMap<[u8; 20], Host>,
        call_count: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl HostRegistry for MockHostRegistry {
        async fn get_host(&self, validator: &ValidatorInfo) -> Result<Host, FibreError> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            self.hosts
                .get(&validator.address)
                .cloned()
                .ok_or_else(|| FibreError::HostNotFound(hex::encode(validator.address)))
        }
    }

    fn make_test_validator(seed: u8) -> ValidatorInfo {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = seed;
        let sk = ed25519_dalek::SigningKey::from_bytes(&key_bytes);
        ValidatorInfo {
            address: [seed; 20],
            pubkey: sk.verifying_key(),
            voting_power: 100,
        }
    }

    #[tokio::test]
    async fn connector_caches_connections() {
        let validator = make_test_validator(1);

        let mut hosts = std::collections::HashMap::new();
        hosts.insert(validator.address, Host("http://127.0.0.1:9090".to_string()));

        let registry = Arc::new(MockHostRegistry {
            hosts,
            call_count: AtomicUsize::new(0),
        });

        let connector = GrpcValidatorConnector::new(registry.clone());

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
        let validator_a = make_test_validator(1);
        let validator_b = make_test_validator(2);

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

        let connector = GrpcValidatorConnector::new(registry.clone());

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
        let validator = make_test_validator(42);

        // Empty hosts map: every lookup will fail with HostNotFound.
        let registry = Arc::new(MockHostRegistry {
            hosts: std::collections::HashMap::new(),
            call_count: AtomicUsize::new(0),
        });

        let connector = GrpcValidatorConnector::new(registry.clone());

        let result = connector.connect(&validator).await;
        match result {
            Err(FibreError::HostNotFound(addr_hex)) => {
                assert_eq!(
                    addr_hex,
                    hex::encode(validator.address),
                    "error should contain the validator address"
                );
            }
            Err(other) => panic!("expected HostNotFound error, got: {other}"),
            Ok(_) => panic!("expected connect to fail for unknown validator"),
        }
    }

    #[test]
    fn normalize_host_strips_dns_prefix() {
        assert_eq!(
            normalize_host("dns:///138.68.236.99:9091"),
            "http://138.68.236.99:9091"
        );
        assert_eq!(
            normalize_host("dns://138.68.236.99:9091"),
            "http://138.68.236.99:9091"
        );
    }

    #[test]
    fn normalize_host_preserves_http() {
        assert_eq!(
            normalize_host("http://127.0.0.1:9090"),
            "http://127.0.0.1:9090"
        );
        assert_eq!(
            normalize_host("https://validator.example.com:9090"),
            "https://validator.example.com:9090"
        );
    }

    #[test]
    fn normalize_host_adds_http_to_bare() {
        assert_eq!(
            normalize_host("138.68.236.99:9091"),
            "http://138.68.236.99:9091"
        );
    }
}
