use std::collections::HashMap;

use tendermint::block::CommitSig;
use tendermint::crypto::default::signature::Verifier;
use tendermint::validator::{Info, Set};
use tendermint::{account, block, chain};

use crate::block::CommitExt;
use crate::trust_level::TrustLevelRatio;
use crate::{
    Result, ValidateBasic, ValidationError, VerificationError, bail_validation, bail_verification,
};

impl ValidateBasic for Set {
    fn validate_basic(&self) -> Result<(), ValidationError> {
        if self.validators().is_empty() {
            bail_validation!("validatiors is empty")
        }

        if self.proposer().is_none() {
            bail_validation!("proposer is none")
        }

        Ok(())
    }
}

/// An extension trait for the [`Set`] to allow additional actions.
pub trait ValidatorSetExt {
    /// Verify the commit signatures and the voting power of the commit.
    fn verify_commit_light(
        &self,
        chain_id: &chain::Id,
        height: &block::Height,
        commit: &block::Commit,
    ) -> Result<()>;

    /// Verify the commit signatures and the voting power of the commit optimistically.
    fn verify_commit_light_trusting(
        &self,
        chain_id: &chain::Id,
        commit: &block::Commit,
        trust_level: TrustLevelRatio,
    ) -> Result<()>;
}

impl ValidatorSetExt for Set {
    fn verify_commit_light(
        &self,
        chain_id: &chain::Id,
        height: &block::Height,
        commit: &block::Commit,
    ) -> Result<()> {
        if self.validators().len() != commit.signatures.len() {
            bail_verification!(
                "validators signature len ({}) != commit signatures len ({})",
                self.validators().len(),
                commit.signatures.len(),
            )
        }

        if height != &commit.height {
            bail_verification!("height ({}) != commit height ({})", height, commit.height,)
        }

        let mut tallied_voting_power = 0;
        let voting_power_needed =
            TrustLevelRatio::new(2, 3).voting_power_needed(self.total_voting_power())?;

        for (idx, (validator, commit_sig)) in self
            .validators()
            .iter()
            .zip(commit.signatures.iter())
            .enumerate()
        {
            let signature = match commit_sig {
                CommitSig::BlockIdFlagCommit {
                    signature: Some(sig),
                    ..
                } => sig,
                CommitSig::BlockIdFlagCommit { .. } => {
                    bail_verification!("No signature in CommitSig");
                }
                // not commiting for the block
                _ => continue,
            };
            let vote_sign = commit.vote_sign_bytes(chain_id, idx)?;
            validator.verify_signature(&Verifier, &vote_sign, signature)?;

            tallied_voting_power += validator.power();
            if tallied_voting_power > voting_power_needed {
                return Ok(());
            }
        }

        Err(VerificationError::NotEnoughVotingPower(
            tallied_voting_power,
            voting_power_needed,
        ))?
    }

    fn verify_commit_light_trusting(
        &self,
        chain_id: &chain::Id,
        commit: &block::Commit,
        trust_level: TrustLevelRatio,
    ) -> Result<()> {
        let mut seen_vals = HashMap::<usize, usize>::new();
        let mut tallied_voting_power = 0;

        let voting_power_needed = trust_level.voting_power_needed(self.total_voting_power())?;

        for (idx, commit_sig) in commit.signatures.iter().enumerate() {
            let (val_id, signature) = match commit_sig {
                CommitSig::BlockIdFlagCommit {
                    validator_address,
                    signature: Some(sig),
                    ..
                } => (validator_address, sig),
                CommitSig::BlockIdFlagCommit { .. } => {
                    bail_verification!("No signature in CommitSig");
                }
                // not commiting for the block
                _ => continue,
            };

            let Some((val_idx, validator)) = find_validator(self, val_id) else {
                continue;
            };

            if let Some(prev_idx) = seen_vals.get(&val_idx) {
                bail_verification!("Double vote from {val_id} ({prev_idx} and {idx}");
            }

            seen_vals.insert(val_idx, idx);

            let vote_sign = commit.vote_sign_bytes(chain_id, idx)?;
            validator.verify_signature(&Verifier, &vote_sign, signature)?;

            tallied_voting_power += validator.power();

            if tallied_voting_power > voting_power_needed {
                return Ok(());
            }
        }

        Err(VerificationError::NotEnoughVotingPower(
            tallied_voting_power,
            voting_power_needed,
        ))?
    }
}

fn find_validator<'a>(vals: &'a Set, val_id: &account::Id) -> Option<(usize, &'a Info)> {
    vals.validators()
        .iter()
        .enumerate()
        .find(|(_idx, val)| val.address == *val_id)
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
pub use wbg::*;

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
mod wbg {
    use tendermint::validator;
    use wasm_bindgen::prelude::*;

    use crate::state::auth::JsPublicKey;

    /// Validator set contains a vector of validators
    #[derive(Clone, Debug)]
    #[wasm_bindgen(getter_with_clone, js_name = "ValidatorSet")]
    pub struct JsValidatorSet {
        /// Validators in the set
        pub validators: Vec<JsValidatorInfo>,
        /// Proposer
        pub proposer: Option<JsValidatorInfo>,
        /// Total voting power
        pub total_voting_power: u64,
    }

    impl From<validator::Set> for JsValidatorSet {
        fn from(value: validator::Set) -> Self {
            JsValidatorSet {
                validators: value.validators.into_iter().map(Into::into).collect(),
                proposer: value.proposer.map(Into::into),
                total_voting_power: value.total_voting_power.value(),
            }
        }
    }

    /// Validator information
    #[derive(Clone, Debug)]
    #[wasm_bindgen(getter_with_clone)]
    pub struct JsValidatorInfo {
        /// Validator account address
        pub address: String,
        /// Validator public key
        pub pub_key: JsPublicKey,
        /// Validator voting power
        pub power: u64,
        /// Validator name
        pub name: Option<String>,
        /// Validator proposer priority
        pub proposer_priority: i64,
    }

    impl From<validator::Info> for JsValidatorInfo {
        fn from(value: validator::Info) -> Self {
            JsValidatorInfo {
                address: value.address.to_string(),
                pub_key: value.pub_key.into(),
                power: value.power.into(),
                name: value.name,
                proposer_priority: value.proposer_priority.into(),
            }
        }
    }
}

/// uniffi types
#[cfg(feature = "uniffi")]
pub mod uniffi_types {
    use tendermint::public_key::PublicKey;
    use tendermint::validator::Info as TendermintValidatorInfo;
    use tendermint::validator::Set as TendermintValidatorSet;
    use uniffi::Record;

    use crate::error::UniffiConversionError;
    use crate::state::UniffiAccountId;

    /// Validator set contains a vector of validators
    #[derive(Record)]
    pub struct ValidatorSet {
        /// Validators in the set
        pub validators: Vec<ValidatorInfo>,
        /// Proposer
        pub proposer: Option<ValidatorInfo>,
        /// Total voting power
        pub total_voting_power: u64,
    }

    impl From<TendermintValidatorSet> for ValidatorSet {
        fn from(value: TendermintValidatorSet) -> Self {
            ValidatorSet {
                validators: value.validators.into_iter().map(Into::into).collect(),
                proposer: value.proposer.map(Into::into),
                total_voting_power: value.total_voting_power.value(),
            }
        }
    }

    impl TryFrom<ValidatorSet> for TendermintValidatorSet {
        type Error = UniffiConversionError;

        fn try_from(value: ValidatorSet) -> Result<Self, Self::Error> {
            Ok(TendermintValidatorSet {
                validators: value
                    .validators
                    .into_iter()
                    .map(|v| v.try_into())
                    .collect::<Result<Vec<_>, _>>()?,
                proposer: value.proposer.map(TryInto::try_into).transpose()?,
                total_voting_power: value
                    .total_voting_power
                    .try_into()
                    .map_err(|_| UniffiConversionError::InvalidVotingPower)?,
            })
        }
    }

    /// Validator information
    #[derive(Record)]
    pub struct ValidatorInfo {
        /// Validator account address
        pub address: UniffiAccountId,
        /// Validator public key
        pub pub_key: PublicKey,
        /// Validator voting power
        pub power: u64,
        /// Validator name
        pub name: Option<String>,
        /// Validator proposer priority
        pub proposer_priority: i64,
    }

    impl From<TendermintValidatorInfo> for ValidatorInfo {
        fn from(value: TendermintValidatorInfo) -> Self {
            ValidatorInfo {
                address: value.address.into(),
                pub_key: value.pub_key,
                power: value.power.into(),
                name: value.name,
                proposer_priority: value.proposer_priority.into(),
            }
        }
    }

    impl TryFrom<ValidatorInfo> for TendermintValidatorInfo {
        type Error = UniffiConversionError;
        fn try_from(value: ValidatorInfo) -> Result<Self, Self::Error> {
            Ok(TendermintValidatorInfo {
                address: value.address.try_into()?,
                pub_key: value.pub_key,
                power: value
                    .power
                    .try_into()
                    .map_err(|_| UniffiConversionError::InvalidVotingPower)?,
                name: value.name,
                proposer_priority: value.proposer_priority.into(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;

    use tendermint_proto::v0_38::types::ValidatorSet as RawValidatorSet;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as test;

    fn sample_commit() -> block::Commit {
        serde_json::from_str(r#"{
          "height": "1",
          "round": 0,
          "block_id": {
            "hash": "17F7D5108753C39714DCA67E6A73CE855C6EA9B0071BBD4FFE5D2EF7F3973BFC",
            "parts": {
              "total": 1,
              "hash": "BEEBB79CDA7D0574B65864D3459FAC7F718B82496BD7FE8B6288BF0A98C8EA22"
            }
          },
          "signatures": [
            {
              "block_id_flag": 2,
              "validator_address": "F1F83230835AA69A1AD6EA68C6D894A4106B8E53",
              "timestamp": "2023-06-23T10:40:48.769228056Z",
              "signature": "HNn4c02eCt2+nGuBs55L8f3DAz9cgy9psLFuzhtg2XCWnlkt2V43TX2b54hQNi7C0fepBEteA3GC01aJM/JJCg=="
            }
          ]
        }"#).unwrap()
    }

    fn sample_validator_set() -> Set {
        serde_json::from_str::<RawValidatorSet>(
            r#"{
              "validators": [
                {
                  "address": "F1F83230835AA69A1AD6EA68C6D894A4106B8E53",
                  "pub_key": {
                    "type": "tendermint/PubKeyEd25519",
                    "value": "yvrJ+hVxB/nh6sKTG+rrrpzyJgr4bxZ5KXM6VEw3t8w="
                  },
                  "voting_power": "5000",
                  "proposer_priority": "0"
                }
              ],
              "proposer": {
                "address": "F1F83230835AA69A1AD6EA68C6D894A4106B8E53",
                "pub_key": {
                  "type": "tendermint/PubKeyEd25519",
                  "value": "yvrJ+hVxB/nh6sKTG+rrrpzyJgr4bxZ5KXM6VEw3t8w="
                },
                "voting_power": "5000",
                "proposer_priority": "0"
              }
            }"#,
        )
        .unwrap()
        .try_into()
        .unwrap()
    }

    fn sample_validator_set_no_validators() -> Set {
        serde_json::from_str::<RawValidatorSet>(
            r#"{
              "validators": [],
              "proposer": {
                "address": "F1F83230835AA69A1AD6EA68C6D894A4106B8E53",
                "pub_key": {
                  "type": "tendermint/PubKeyEd25519",
                  "value": "yvrJ+hVxB/nh6sKTG+rrrpzyJgr4bxZ5KXM6VEw3t8w="
                },
                "voting_power": "5000",
                "proposer_priority": "0"
              }
            }"#,
        )
        .unwrap()
        .try_into()
        .unwrap()
    }

    fn sample_validator_set_no_proposer() -> Set {
        serde_json::from_str::<RawValidatorSet>(
            r#"{
              "validators": [
                {
                  "address": "F1F83230835AA69A1AD6EA68C6D894A4106B8E53",
                  "pub_key": {
                    "type": "tendermint/PubKeyEd25519",
                    "value": "yvrJ+hVxB/nh6sKTG+rrrpzyJgr4bxZ5KXM6VEw3t8w="
                  },
                  "voting_power": "5000",
                  "proposer_priority": "0"
                }
              ]
            }"#,
        )
        .unwrap()
        .try_into()
        .unwrap()
    }

    #[test]
    fn validate_correct() {
        sample_validator_set().validate_basic().unwrap();
    }

    #[test]
    fn validate_validators_missing() {
        sample_validator_set_no_validators()
            .validate_basic()
            .unwrap_err();
    }

    #[test]
    fn validate_proposer_missing() {
        sample_validator_set_no_proposer()
            .validate_basic()
            .unwrap_err();
    }

    #[test]
    fn verify_commit_light_success() {
        let commit = sample_commit();
        let val_set = sample_validator_set();

        val_set
            .verify_commit_light(
                &"private".to_string().try_into().unwrap(),
                &1u32.into(),
                &commit,
            )
            .unwrap();
    }

    #[test]
    fn verify_commit_light_validators_and_signatures_mismatch() {
        let mut commit = sample_commit();
        let val_set = sample_validator_set();
        commit.signatures.push(commit.signatures[0].clone());

        val_set
            .verify_commit_light(
                &"private".to_string().try_into().unwrap(),
                &1u32.into(),
                &commit,
            )
            .unwrap_err();
    }

    #[test]
    fn verify_commit_light_commit_height_mismatch() {
        let mut commit = sample_commit();
        let val_set = sample_validator_set();
        commit.height = 2u32.into();

        val_set
            .verify_commit_light(
                &"private".to_string().try_into().unwrap(),
                &1u32.into(),
                &commit,
            )
            .unwrap_err();
    }

    #[test]
    fn verify_commit_light_not_enough_voting_power() {
        let mut commit = sample_commit();
        let val_set = sample_validator_set();
        commit.signatures[0] = CommitSig::BlockIdFlagAbsent;

        let err = val_set
            .verify_commit_light(
                &"private".to_string().try_into().unwrap(),
                &1u32.into(),
                &commit,
            )
            .unwrap_err();

        assert!(matches!(
            err,
            Error::Verification(VerificationError::NotEnoughVotingPower(0, 3333))
        ));
    }

    #[test]
    fn verify_commit_light_missing_signature() {
        let mut commit = sample_commit();
        let val_set = sample_validator_set();
        let CommitSig::BlockIdFlagCommit { signature, .. } = &mut commit.signatures[0] else {
            unreachable!()
        };
        *signature = None;

        let err = val_set
            .verify_commit_light(
                &"private".to_string().try_into().unwrap(),
                &1u32.into(),
                &commit,
            )
            .unwrap_err();

        assert!(err.to_string().contains("No signature in CommitSig"));
    }

    #[test]
    fn verify_commit_light_invalid_signature() {
        let commit = sample_commit();
        let val_set = sample_validator_set();

        // The commit was signed for the "private" chain.
        let err = val_set
            .verify_commit_light(
                &"other".to_string().try_into().unwrap(),
                &1u32.into(),
                &commit,
            )
            .unwrap_err();

        assert!(matches!(err, Error::Tendermint(_)));
    }

    #[test]
    fn verify_commit_light_trusting_missing_signature() {
        let mut commit = sample_commit();
        let val_set = sample_validator_set();
        let CommitSig::BlockIdFlagCommit { signature, .. } = &mut commit.signatures[0] else {
            unreachable!()
        };
        *signature = None;

        let err = val_set
            .verify_commit_light_trusting(
                &"private".to_string().try_into().unwrap(),
                &commit,
                TrustLevelRatio::new(1, 3),
            )
            .unwrap_err();

        assert!(err.to_string().contains("No signature in CommitSig"));
    }

    #[test]
    fn verify_commit_light_trusting_double_vote() {
        let mut commit = sample_commit();
        let val_set = sample_validator_set();
        commit.signatures.push(commit.signatures[0].clone());

        // Require all voting power, so the first vote alone doesn't finish verification.
        let err = val_set
            .verify_commit_light_trusting(
                &"private".to_string().try_into().unwrap(),
                &commit,
                TrustLevelRatio::new(1, 1),
            )
            .unwrap_err();

        assert!(err.to_string().contains("Double vote"));
    }

    #[test]
    fn verify_commit_light_trusting_needs_more_than_trust_level() {
        let commit = sample_commit();
        let val_set = sample_validator_set();

        let err = val_set
            .verify_commit_light_trusting(
                &"private".to_string().try_into().unwrap(),
                &commit,
                TrustLevelRatio::new(1, 1),
            )
            .unwrap_err();

        assert!(matches!(
            err,
            Error::Verification(VerificationError::NotEnoughVotingPower(5000, 5000))
        ));
    }
}
