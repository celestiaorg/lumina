//! Production gRPC transport implementation.
//!
//! [`GrpcValidatorConnector`] resolves validator addresses via a
//! [`HostRegistry`] and caches tonic [`Channel`]s.
//! [`GrpcValidatorConnection`] uses generated tonic clients to upload
//! and download shards over the Fibre gRPC service.

use std::collections::HashMap;
use std::sync::Arc;

use tonic::transport::Channel;

use crate::blob::BlobID;
use crate::error::FibreError;
use crate::host_registry::HostRegistry;
use crate::payment_promise::PaymentPromise;
use crate::proto_conv;
use crate::validator::ValidatorInfo;
use crate::validator_client::{
    DownloadResponse, DownloadedRow, UploadResponse, ValidatorConnection, ValidatorConnector,
};

use celestia_proto::celestia::fibre::v1::DownloadShardRequest;
use celestia_proto::celestia::fibre::v1::fibre_client::FibreClient as ProtoFibreClient;

/// Factory that resolves validator hosts and caches gRPC connections.
///
/// Uses a [`HostRegistry`] to map validator addresses to network endpoints,
/// then creates tonic [`Channel`]s lazily and caches them by the 20-byte
/// validator consensus address.
pub struct GrpcValidatorConnector {
    host_registry: Arc<dyn HostRegistry>,
    max_message_size: usize,
    connections: tokio::sync::Mutex<HashMap<[u8; 20], Arc<GrpcValidatorConnection>>>,
}

impl GrpcValidatorConnector {
    /// Create a new connector backed by the given host registry.
    ///
    /// `max_message_size` controls the tonic encoding/decoding limits applied
    /// to every connection created through this connector.
    pub fn new(host_registry: Arc<dyn HostRegistry>, max_message_size: usize) -> Self {
        Self {
            host_registry,
            max_message_size,
            connections: tokio::sync::Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
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

        let channel = tonic::transport::Endpoint::from_shared(host.0.clone())
            .map_err(|e| FibreError::Other(format!("invalid endpoint '{}': {e}", host.0)))?
            .connect_lazy();

        let conn = Arc::new(GrpcValidatorConnection {
            channel,
            max_message_size: self.max_message_size,
        });

        // Insert into cache.
        let mut cache = self.connections.lock().await;
        cache.insert(validator.address, conn.clone());

        Ok(conn as Arc<dyn ValidatorConnection>)
    }
}

/// A connection to a single validator's Fibre gRPC service.
///
/// Wraps a tonic [`Channel`] and applies message size limits to every RPC.
pub struct GrpcValidatorConnection {
    channel: Channel,
    max_message_size: usize,
}

#[async_trait::async_trait]
impl ValidatorConnection for GrpcValidatorConnection {
    async fn upload_shard(
        &self,
        promise: &PaymentPromise,
        rows: &[rsema1d::RowInclusionProof],
        rlc_coeffs: &[rsema1d::GF128],
    ) -> Result<UploadResponse, FibreError> {
        let mut client = ProtoFibreClient::new(self.channel.clone())
            .max_encoding_message_size(self.max_message_size)
            .max_decoding_message_size(self.max_message_size);

        let proto_promise = proto_conv::payment_promise_to_proto(promise);
        let proto_shard = proto_conv::build_upload_shard(rows, rlc_coeffs);

        let request = celestia_proto::celestia::fibre::v1::UploadShardRequest {
            promise: Some(proto_promise),
            shard: Some(proto_shard),
        };

        let response = client.upload_shard(request).await?;
        let inner = response.into_inner();

        Ok(UploadResponse {
            validator_signature: inner.validator_signature,
        })
    }

    async fn download_shard(&self, blob_id: &BlobID) -> Result<DownloadResponse, FibreError> {
        let mut client = ProtoFibreClient::new(self.channel.clone())
            .max_decoding_message_size(self.max_message_size);

        let request = DownloadShardRequest {
            blob_id: blob_id.as_bytes().to_vec(),
        };

        let response = client.download_shard(request).await?;
        let inner = response.into_inner();

        let proofs = proto_conv::parse_download_response(inner)?;

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

        let connector = GrpcValidatorConnector::new(registry.clone(), 4 * 1024 * 1024);

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

        let connector = GrpcValidatorConnector::new(registry.clone(), 4 * 1024 * 1024);

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

        let connector = GrpcValidatorConnector::new(registry.clone(), 4 * 1024 * 1024);

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
}
