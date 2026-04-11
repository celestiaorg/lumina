//! Host registry for resolving validator network addresses.
//!
//! The [`HostRegistry`] trait maps validators to their fibre gRPC endpoints.
//! [`GrpcHostRegistry`] is the production implementation that queries the
//! `x/valaddr` on-chain module via gRPC.

use std::collections::HashMap;

use crate::error::FibreError;
use crate::validator::ValidatorInfo;
use celestia_grpc::GrpcClient;

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
            let addr_bytes = decode_bech32_address(&provider.validator_consensus_address)?;
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
        let bech32_addr = encode_bech32_address("celestiavalcons", &validator.address)?;

        let resp = self.client.get_fibre_provider_info(bech32_addr).await?;

        if !resp.found {
            return Err(FibreError::HostNotFound(hex::encode_upper(
                validator.address,
            )));
        }

        let host_str = resp.info.map(|info| info.host).unwrap_or_default();

        if host_str.is_empty() {
            return Err(FibreError::HostNotFound(hex::encode_upper(
                validator.address,
            )));
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

/// Decode a bech32-encoded address (e.g., `"celestiavalcons1..."`) into a
/// 20-byte raw address.
fn decode_bech32_address(bech32_addr: &str) -> Result<[u8; 20], FibreError> {
    let (_hrp, data) = bech32::decode(bech32_addr).map_err(|e| {
        FibreError::InvalidData(format!("invalid bech32 address '{bech32_addr}': {e}"))
    })?;

    let addr: [u8; 20] = data.try_into().map_err(|v: Vec<u8>| {
        FibreError::InvalidData(format!(
            "bech32 address decoded to {} bytes, expected 20",
            v.len()
        ))
    })?;

    Ok(addr)
}

/// Encode a 20-byte address into a bech32 string with the given human-readable
/// prefix (e.g., `"celestiavalcons"`).
fn encode_bech32_address(hrp: &str, addr: &[u8; 20]) -> Result<String, FibreError> {
    let hrp = bech32::Hrp::parse(hrp)
        .map_err(|e| FibreError::Other(format!("invalid bech32 hrp '{hrp}': {e}")))?;
    let encoded = bech32::encode::<bech32::Bech32>(hrp, addr)
        .map_err(|e| FibreError::Other(format!("bech32 encoding failed: {e}")))?;
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_validator(seed: u8, address: [u8; 20]) -> ValidatorInfo {
        let mut key_bytes = [0u8; 32];
        key_bytes[0] = seed;
        let sk = ed25519_dalek::SigningKey::from_bytes(&key_bytes);
        let pubkey = sk.verifying_key();
        ValidatorInfo {
            address,
            pubkey,
            voting_power: 1,
        }
    }

    #[test]
    fn bech32_encode_decode_roundtrip() {
        let addr: [u8; 20] = [
            0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A,
            0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
        ];

        let encoded =
            encode_bech32_address("celestiavalcons", &addr).expect("encoding should succeed");
        let decoded = decode_bech32_address(&encoded).expect("decoding should succeed");

        assert_eq!(decoded, addr);
    }

    #[test]
    fn bech32_encode_known_vector() {
        let addr = [1u8; 20];
        let encoded =
            encode_bech32_address("celestiavalcons", &addr).expect("encoding should succeed");

        assert!(
            encoded.starts_with("celestiavalcons1"),
            "encoded address should start with 'celestiavalcons1', got: {encoded}"
        );

        // Round-trip check: decoding gives back the same bytes.
        let decoded = decode_bech32_address(&encoded).expect("decoding should succeed");
        assert_eq!(decoded, addr);
    }

    #[test]
    fn bech32_decode_invalid_string() {
        let result = decode_bech32_address("notvalid");
        assert!(
            result.is_err(),
            "decoding an invalid bech32 string should fail"
        );

        let err = result.unwrap_err();
        match &err {
            FibreError::InvalidData(msg) => {
                assert!(
                    msg.contains("invalid bech32 address"),
                    "error message should mention 'invalid bech32 address', got: {msg}"
                );
            }
            other => panic!("expected InvalidData error, got: {other:?}"),
        }
    }

    #[test]
    fn bech32_decode_wrong_data_length() {
        // Encode a 10-byte payload (not 20), which should fail on decode.
        let short_bech32 =
            bech32::encode::<bech32::Bech32>(bech32::Hrp::parse("test").unwrap(), &[0u8; 10])
                .expect("bech32 encoding of short payload should succeed");

        let result = decode_bech32_address(&short_bech32);
        assert!(
            result.is_err(),
            "decoding a non-20-byte payload should fail"
        );

        let err = result.unwrap_err();
        match &err {
            FibreError::InvalidData(msg) => {
                assert!(
                    msg.contains("expected 20"),
                    "error message should mention 'expected 20', got: {msg}"
                );
            }
            other => panic!("expected InvalidData error, got: {other:?}"),
        }
    }

    struct MapRegistry {
        hosts: HashMap<[u8; 20], Host>,
    }

    #[async_trait::async_trait]
    impl HostRegistry for MapRegistry {
        async fn get_host(&self, validator: &ValidatorInfo) -> Result<Host, FibreError> {
            self.hosts
                .get(&validator.address)
                .cloned()
                .ok_or_else(|| FibreError::HostNotFound(hex::encode_upper(validator.address)))
        }
    }

    #[tokio::test]
    async fn map_registry_returns_host_when_present() {
        let addr = [42u8; 20];
        let expected_host = Host("dns:///example.com:9090".to_string());

        let mut hosts = HashMap::new();
        hosts.insert(addr, expected_host.clone());

        let registry = MapRegistry { hosts };
        let validator = make_validator(1, addr);

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
        let validator = make_validator(1, [99u8; 20]);

        let result = registry.get_host(&validator).await;
        assert!(result.is_err(), "should return an error for missing host");

        match result.unwrap_err() {
            FibreError::HostNotFound(addr_hex) => {
                assert_eq!(
                    addr_hex,
                    hex::encode_upper([99u8; 20]),
                    "error should contain the hex-encoded address"
                );
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

        let addr = [7u8; 20];
        let expected_host = Host("dns:///cached-validator.example.com:9090".to_string());

        // Manually populate the cache.
        registry
            .cache
            .write()
            .await
            .insert(addr, expected_host.clone());

        let validator = make_validator(2, addr);

        // get_host should return the cached value without hitting the (unreachable) server.
        let host = registry
            .get_host(&validator)
            .await
            .expect("should return cached host");

        assert_eq!(host, expected_host);
    }
}
