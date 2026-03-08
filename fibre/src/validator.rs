//! Validator identity, validator set types, and the [`SetGetter`] trait.

use ed25519_dalek::VerifyingKey as Ed25519PublicKey;

use crate::error::FibreError;

use crate::blob::Commitment;
use crate::config::Fraction;
use crate::shard_assignment::{self, ShardMap};
use crate::shard_selection;
use celestia_grpc::BoxedTransport;

/// A validator's identity and stake information.
#[derive(Debug, Clone)]
pub struct ValidatorInfo {
    /// The validator's consensus address (20-byte CometBFT address).
    pub address: [u8; 20],
    /// The validator's ed25519 public key for signing.
    pub pubkey: Ed25519PublicKey,
    /// The validator's voting power (stake weight).
    pub voting_power: i64,
}

impl ValidatorInfo {
    /// Returns the validator address as an uppercase hex string.
    pub fn address_hex(&self) -> String {
        hex::encode_upper(self.address)
    }
}

impl PartialEq for ValidatorInfo {
    fn eq(&self, other: &Self) -> bool {
        self.address == other.address
            && self.pubkey.as_bytes() == other.pubkey.as_bytes()
            && self.voting_power == other.voting_power
    }
}

impl Eq for ValidatorInfo {}

impl std::hash::Hash for ValidatorInfo {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.address.hash(state);
        self.pubkey.as_bytes().hash(state);
        self.voting_power.hash(state);
    }
}

/// A validator set at a specific height.
#[derive(Debug, Clone)]
pub struct ValidatorSet {
    /// The validators in this set.
    pub validators: Vec<ValidatorInfo>,
    /// The block height at which this validator set is valid.
    pub height: u64,
}

impl ValidatorSet {
    /// Returns the total voting power of all validators in the set.
    pub fn total_voting_power(&self) -> i64 {
        self.validators.iter().map(|v| v.voting_power).sum()
    }

    /// Deterministically assigns row indices to validators based on their stake.
    ///
    /// Delegates to [`shard_assignment::assign`] using this set's validators and
    /// total voting power.
    pub fn assign(
        &self,
        commitment: Commitment,
        total_rows: usize,
        original_rows: usize,
        min_rows: usize,
        liveness_threshold: Fraction,
    ) -> ShardMap {
        shard_assignment::assign(
            &self.validators,
            self.total_voting_power(),
            commitment,
            total_rows,
            original_rows,
            min_rows,
            liveness_threshold,
        )
    }

    /// Selects validators for shard download, ordered by priority.
    ///
    /// Delegates to [`shard_selection::select`] using this set's validators and
    /// total voting power. Returns (ordered validator indices, minimum required count).
    pub fn select(
        &self,
        original_rows: usize,
        min_rows: usize,
        liveness_threshold: Fraction,
    ) -> (Vec<usize>, usize) {
        shard_selection::select(
            &self.validators,
            self.total_voting_power(),
            original_rows,
            min_rows,
            liveness_threshold,
        )
    }
}

/// Trait for retrieving the current validator set.
///
/// The client only needs the latest set; height-specific lookups are server-side only.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait SetGetter: Send + Sync {
    /// Get the latest validator set.
    async fn head(&self) -> Result<ValidatorSet, FibreError>;
}

use celestia_proto::tendermint_celestia_mods::rpc::grpc::ValidatorSetRequest;
use celestia_proto::tendermint_celestia_mods::rpc::grpc::block_api_client::BlockApiClient;

/// Production [`SetGetter`] backed by the CometBFT `BlockAPI` gRPC service.
///
/// Queries `ValidatorSet` with `height = 0` to retrieve the latest validator
/// set, then converts the protobuf response into the domain [`ValidatorSet`].
pub struct GrpcSetGetter {
    channel: BoxedTransport,
}

impl GrpcSetGetter {
    /// Create a new getter using the given gRPC channel to the CometBFT node.
    pub fn new(channel: BoxedTransport) -> Self {
        Self { channel }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl SetGetter for GrpcSetGetter {
    async fn head(&self) -> Result<ValidatorSet, FibreError> {
        let mut client = BlockApiClient::new(self.channel.clone());

        let resp = client
            .validator_set(ValidatorSetRequest { height: 0 })
            .await?;

        let inner = resp.into_inner();
        let height = inner.height as u64;

        let proto_set = inner.validator_set.ok_or_else(|| {
            FibreError::Other("ValidatorSetResponse missing validator_set".into())
        })?;

        crate::proto_conv::validator_set_from_proto(&proto_set, height)
    }
}
