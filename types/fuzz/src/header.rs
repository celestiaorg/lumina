//! Deterministically signed headers for the `header_verify` target.

use std::iter;
use std::sync::LazyLock;

use arbitrary::Arbitrary;
use celestia_types::block::CommitExt;
use celestia_types::consts::version;
use celestia_types::hash::Hash;
use celestia_types::{DataAvailabilityHeader, ExtendedHeader, ValidatorSet};
use ed25519_dalek::{Signer, SigningKey};
use tendermint::block::header::{Header, Version};
use tendermint::block::{self, Commit, CommitSig, parts};
use tendermint::public_key::PublicKey;
use tendermint::{Signature, Time, account, chain, validator, vote};

/// Upper bound on the size of generated validator sets.
pub const MAX_VALIDATORS: usize = 8;

/// Keys that validators are made of and votes are signed with.
pub static KEYS: LazyLock<Vec<SigningKey>> =
    LazyLock::new(|| (1..=16).map(|i| SigningKey::from_bytes(&[i; 32])).collect());

/// Addresses of [`KEYS`].
pub static ADDRESSES: LazyLock<Vec<account::Id>> =
    LazyLock::new(|| (0..KEYS.len()).map(|key| public_key(key).into()).collect());

fn public_key(key: usize) -> PublicKey {
    PublicKey::from_raw_ed25519(&KEYS[key].verifying_key().to_bytes()).unwrap()
}

/// Index into [`KEYS`].
fn key(selector: u8) -> usize {
    usize::from(selector) % KEYS.len()
}

#[derive(Arbitrary, Debug)]
pub struct Validator {
    pub key: u8,
    pub power: u64,
}

#[derive(Arbitrary, Debug, Default, PartialEq)]
pub enum Flag {
    #[default]
    Commit,
    Nil,
    Absent,
}

#[derive(Arbitrary, Debug, Default, PartialEq)]
pub enum SignatureKind {
    #[default]
    Valid,
    /// Flips a bit of the signature at the given offset.
    Corrupted(u8),
    Missing,
}

/// A vote in the commit, by default a valid one of its validator.
#[derive(Arbitrary, Debug, Default)]
pub struct Vote {
    pub flag: Flag,
    /// Key whose address the vote claims instead of the validator's.
    pub address: Option<u8>,
    /// Key which signs the vote instead of the validator's.
    pub signer: Option<u8>,
    pub signature: SignatureKind,
}

/// A valid commit signature: the key that made it and the address it claims.
pub type Signed = Option<(usize, account::Id)>;

/// Builds a validator set, or `None` if decoding it would fail.
pub fn validator_set(validators: &[Validator], proposer: Option<u8>) -> Option<ValidatorSet> {
    let validators = validators
        .iter()
        .take(MAX_VALIDATORS)
        .map(|v| {
            let power = vote::Power::try_from(v.power).ok()?;
            Some(validator::Info::new(public_key(key(v.key)), power))
        })
        .collect::<Option<Vec<_>>>()?;

    let total = validators
        .iter()
        .fold(0u64, |acc, v| acc.saturating_add(v.power.value()));
    if total > ValidatorSet::MAX_TOTAL_VOTING_POWER {
        return None;
    }

    let proposer = proposer
        .and_then(|p| validators.get(usize::from(p) % validators.len().max(1)))
        .cloned();
    Some(ValidatorSet::new(validators, proposer))
}

/// Parameters of a header to build.
pub struct Params<'a> {
    pub chain_id: chain::Id,
    pub height: u64,
    pub time: Time,
    pub last_block_id: Option<block::Id>,
    pub validator_set: ValidatorSet,
    pub next_validators_hash: Hash,
    pub dah: DataAvailabilityHeader,
    /// Votes of the validators in the order of the set, valid ones for the missing.
    pub votes: &'a [Vote],
}

/// Builds and signs a header.
///
/// Returns the header and the valid signatures in its commit.
pub fn build(params: Params) -> Option<(ExtendedHeader, Vec<Signed>)> {
    let Params {
        chain_id,
        height,
        time,
        last_block_id,
        validator_set,
        next_validators_hash,
        dah,
        votes,
    } = params;
    let height = block::Height::try_from(height).ok()?;
    let zero_hash = Hash::Sha256([0; 32]);

    let header = Header {
        version: Version {
            block: version::BLOCK_PROTOCOL,
            app: 1,
        },
        chain_id: chain_id.clone(),
        height,
        time,
        last_block_id,
        last_commit_hash: Some(zero_hash),
        data_hash: Some(dah.hash()),
        validators_hash: validator_set.hash(),
        next_validators_hash,
        consensus_hash: zero_hash,
        app_hash: vec![0; 32].try_into().unwrap(),
        last_results_hash: Some(zero_hash),
        evidence_hash: Some(zero_hash),
        proposer_address: validator_set
            .proposer()
            .as_ref()
            .map_or(ADDRESSES[0], |p| p.address),
    };

    let votes: Vec<_> = votes
        .iter()
        .chain(iter::repeat_with(|| &DEFAULT_VOTE))
        .take(validator_set.validators().len())
        .collect();

    let mut commit = Commit {
        height,
        round: 0u16.into(),
        block_id: block::Id {
            hash: header.hash(),
            part_set_header: parts::Header::new(1, zero_hash).unwrap(),
        },
        signatures: validator_set
            .validators()
            .iter()
            .zip(&votes)
            .map(|(validator, vote)| {
                let validator_address = vote
                    .address
                    .map_or(validator.address, |k| ADDRESSES[key(k)]);
                match vote.flag {
                    Flag::Commit => CommitSig::BlockIdFlagCommit {
                        validator_address,
                        timestamp: time,
                        signature: None,
                    },
                    Flag::Nil => CommitSig::BlockIdFlagNil {
                        validator_address,
                        timestamp: time,
                        signature: None,
                    },
                    Flag::Absent => CommitSig::BlockIdFlagAbsent,
                }
            })
            .collect(),
    };

    let mut signed = Vec::with_capacity(votes.len());
    for (idx, (validator, vote)) in validator_set.validators().iter().zip(&votes).enumerate() {
        let Ok(sign_bytes) = commit.vote_sign_bytes(&chain_id, idx) else {
            signed.push(None);
            continue;
        };
        let signer = vote.signer.map_or_else(
            || {
                ADDRESSES
                    .iter()
                    .position(|a| *a == validator.address)
                    .unwrap()
            },
            key,
        );
        let mut signature = KEYS[signer].sign(&sign_bytes).to_bytes();

        let signature = match vote.signature {
            SignatureKind::Valid => Signature::new(signature).unwrap(),
            SignatureKind::Corrupted(offset) => {
                signature[usize::from(offset) % signature.len()] ^= 1;
                Signature::new(signature).unwrap()
            }
            SignatureKind::Missing => None,
        };

        signed.push(
            (vote.flag == Flag::Commit && vote.signature == SignatureKind::Valid).then(|| {
                let CommitSig::BlockIdFlagCommit {
                    validator_address, ..
                } = commit.signatures[idx]
                else {
                    unreachable!()
                };
                (signer, validator_address)
            }),
        );

        match &mut commit.signatures[idx] {
            CommitSig::BlockIdFlagCommit { signature: s, .. }
            | CommitSig::BlockIdFlagNil { signature: s, .. } => *s = signature,
            CommitSig::BlockIdFlagAbsent => {}
        }
    }

    let header = ExtendedHeader {
        header,
        commit,
        validator_set,
        dah,
    };
    Some((header, signed))
}

static DEFAULT_VOTE: Vote = Vote {
    flag: Flag::Commit,
    address: None,
    signer: None,
    signature: SignatureKind::Valid,
};

/// Voting power of the validators of a header that signed it, matching votes by index.
pub fn own_power(validator_set: &ValidatorSet, signed: &[Signed]) -> u128 {
    validator_set
        .validators()
        .iter()
        .zip(signed)
        .filter(|(v, signed)| signed.is_some_and(|(key, _)| ADDRESSES[key] == v.address))
        .map(|(v, _)| u128::from(v.power.value()))
        .sum()
}

/// Voting power of the validators of `validator_set` that signed a header of another
/// one, matching votes by the address they claim.
pub fn trusted_power(validator_set: &ValidatorSet, signed: &[Signed]) -> u128 {
    validator_set
        .validators()
        .iter()
        .filter(|v| {
            signed
                .iter()
                .flatten()
                .any(|(key, address)| *address == v.address && ADDRESSES[*key] == v.address)
        })
        .map(|v| u128::from(v.power.value()))
        .sum()
}

pub fn total_power(validator_set: &ValidatorSet) -> u128 {
    validator_set.total_voting_power().value().into()
}
