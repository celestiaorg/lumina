//! Host registry for resolving validator network addresses.
//!
//! The [`HostRegistry`] trait maps validators to their fibre gRPC endpoints.
//! [`GrpcHostRegistry`] is the production implementation that queries the
//! `x/valaddr` on-chain module via gRPC.

use std::collections::HashMap;

use crate::error::FibreError;
use crate::validator::ValidatorInfo;
use celestia_grpc::GrpcClient;
use celestia_types::state::{AddressTrait, ConsAddress};

/// A validator's network address (e.g., `"dns:///validator.example.com:9090"`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Host(pub String);

/// Maps validators to their fibre gRPC network addresses.
///
/// In production this is backed by the `x/valaddr` on-chain query service.
/// In tests this can be a simple `HashMap`.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait HostRegistry: Send + Sync {
    /// Resolve the fibre gRPC address for the given validator.
    async fn get_host(&self, validator: &ValidatorInfo) -> Result<Host, FibreError>;
}

/// Production host registry backed by the `x/valaddr` on-chain query service.
///
/// Maintains a local cache of `[u8; 20]` -> [`Host`] mappings. The cache can
/// be bulk-populated via [`pull_all()`](GrpcHostRegistry::pull_all) or
/// lazily filled per-validator via
/// [`pull_host()`](GrpcHostRegistry::pull_host).
pub struct GrpcHostRegistry {
    client: GrpcClient,
    pub(crate) cache: tokio::sync::RwLock<HashMap<[u8; 20], Host>>,
}

impl GrpcHostRegistry {
    /// Create a new registry using the given [`GrpcClient`] to the Cosmos app.
    pub fn new(client: GrpcClient) -> Self {
        Self {
            client,
            cache: tokio::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Fetch all fibre providers from the chain and populate the cache.
    ///
    /// Queries `AllFibreProviders` on the `x/valaddr` module, decodes each
    /// provider's bech32 consensus address to a 20-byte key, and stores the
    /// host mapping.
    pub async fn pull_all(&self) -> Result<(), FibreError> {
        let resp = self.client.get_all_fibre_providers().await?;

        let providers = resp.providers;
        let mut cache = self.cache.write().await;

        for provider in providers {
            let addr_bytes = consensus_address_bytes(&provider.validator_consensus_address)?;
            let host_str = provider.info.map(|info| info.host).unwrap_or_default();

            if !host_str.is_empty() {
                cache.insert(addr_bytes, Host(host_str));
            }
        }

        Ok(())
    }

    /// Fetch the fibre host for a single validator and update the cache.
    ///
    /// Uses the validator's 20-byte address encoded as a bech32 consensus
    /// address to query `FibreProviderInfo`.
    pub async fn pull_host(&self, validator: &ValidatorInfo) -> Result<Host, FibreError> {
        let bech32_addr = ConsAddress::from(validator.address).to_string();

        let resp = self.client.get_fibre_provider_info(bech32_addr).await?;

        if !resp.found {
            return Err(FibreError::HostNotFound(validator.address));
        }

        let host_str = resp.info.map(|info| info.host).unwrap_or_default();

        if host_str.is_empty() {
            return Err(FibreError::HostNotFound(validator.address));
        }

        let host = Host(host_str);

        // Update cache.
        self.cache
            .write()
            .await
            .insert(validator.address, host.clone());

        Ok(host)
    }
}

fn consensus_address_bytes(address: &str) -> Result<[u8; 20], FibreError> {
    let address: ConsAddress = address
        .parse()
        .map_err(FibreError::InvalidValidatorAddress)?;
    Ok(address
        .id_ref()
        .as_bytes()
        .try_into()
        .expect("consensus address id is 20 bytes"))
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl HostRegistry for GrpcHostRegistry {
    async fn get_host(&self, validator: &ValidatorInfo) -> Result<Host, FibreError> {
        // Check cache first.
        {
            let cache = self.cache.read().await;
            if let Some(host) = cache.get(&validator.address) {
                return Ok(host.clone());
            }
        }

        // Cache miss: query the chain for this specific validator.
        self.pull_host(validator).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::make_validator;
    use std::collections::HashMap;

    #[test]
    fn consensus_address_rejects_wrong_prefix() {
        let address = celestia_types::state::AccAddress::from([1u8; 20]).to_string();
        assert!(matches!(
            consensus_address_bytes(&address),
            Err(FibreError::InvalidValidatorAddress(_))
        ));
    }

    struct MapRegistry {
        hosts: HashMap<[u8; 20], Host>,
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
    impl HostRegistry for MapRegistry {
        async fn get_host(&self, validator: &ValidatorInfo) -> Result<Host, FibreError> {
            self.hosts
                .get(&validator.address)
                .cloned()
                .ok_or(FibreError::HostNotFound(validator.address))
        }
    }

    #[tokio::test]
    async fn map_registry_returns_host_when_present() {
        let expected_host = Host("dns:///example.com:9090".to_string());
        let validator = make_validator(1, 1).1;

        let mut hosts = HashMap::new();
        hosts.insert(validator.address, expected_host.clone());

        let registry = MapRegistry { hosts };

        let host = registry
            .get_host(&validator)
            .await
            .expect("should find host");
        assert_eq!(host, expected_host);
    }

    #[tokio::test]
    async fn map_registry_returns_host_not_found_when_absent() {
        let registry = MapRegistry {
            hosts: HashMap::new(),
        };
        let validator = make_validator(1, 1).1;

        let result = registry.get_host(&validator).await;
        assert!(result.is_err(), "should return an error for missing host");

        match result.unwrap_err() {
            FibreError::HostNotFound(addr) => {
                assert_eq!(addr, *validator.address());
            }
            other => panic!("expected HostNotFound, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn grpc_registry_returns_cached_host() {
        let client = GrpcClient::builder()
            .url("http://localhost:50051")
            .build()
            .expect("GrpcClient builder should succeed for test URL");

        let registry = GrpcHostRegistry::new(client);

        let expected_host = Host("dns:///cached-validator.example.com:9090".to_string());
        let validator = make_validator(1, 2).1;

        // Manually populate the cache.
        registry
            .cache
            .write()
            .await
            .insert(validator.address, expected_host.clone());

        // get_host should return the cached value without hitting the (unreachable) server.
        let host = registry
            .get_host(&validator)
            .await
            .expect("should return cached host");

        assert_eq!(host, expected_host);
    }
}
