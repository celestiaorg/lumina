//! A simulated validator: deterministic ed25519 identity plus the gRPC host
//! it serves the Fibre data plane on.

use sha2::{Digest, Sha256};

/// Human-readable prefix for validator consensus addresses, matching the
/// client's bech32 encoding in `fibre::transport::host_registry`.
const CONSENSUS_ADDR_HRP: &str = "celestiavalcons";

/// One simulated validator with a deterministic (index-derived) ed25519 identity.
#[derive(Clone)]
pub struct MockValidator {
    /// Key used to sign payment promises in UploadShard responses.
    pub signing_key: ed25519_dalek::SigningKey,
    /// CometBFT consensus address: first 20 bytes of SHA-256(pubkey). The
    /// fibre client derives it the same way from the ValidatorSet response.
    pub address: [u8; 20],
    /// Bech32 (`celestiavalcons...`) form of `address`, as used by valaddr queries.
    pub consensus_addr_bech32: String,
    /// Host string advertised via the valaddr registry, e.g. `http://127.0.0.1:19001`.
    pub host: String,
    /// Voting power reported in the ValidatorSet response.
    pub voting_power: i64,
}

impl MockValidator {
    /// Create validator number `index` with a deterministic ed25519 key.
    pub fn new(index: u64, host: String, voting_power: i64) -> Self {
        let mut key_bytes = [0u8; 32];
        key_bytes[..8].copy_from_slice(&index.to_le_bytes());
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&key_bytes);

        let pubkey = signing_key.verifying_key();
        let address: [u8; 20] = Sha256::digest(pubkey.as_bytes())[..20]
            .try_into()
            .expect("sha256 output is 32 bytes");

        let hrp = bech32::Hrp::parse(CONSENSUS_ADDR_HRP).expect("valid hrp");
        let consensus_addr_bech32 =
            bech32::encode::<bech32::Bech32>(hrp, &address).expect("bech32 encoding of 20 bytes");

        Self {
            signing_key,
            address,
            consensus_addr_bech32,
            host,
            voting_power,
        }
    }

    /// The validator's ed25519 public key bytes.
    pub fn pubkey_bytes(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
    }
}
