//! Types relaying malfeasance evidence

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
pub use wbg::*;

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
mod wbg {
    use tendermint::evidence::{ConflictingBlock, Evidence};
    use tendermint::vote::{Type as VoteType, Vote};
    use wasm_bindgen::prelude::*;

    use crate::block::{JsBlockId, JsSignedHeader};
    use crate::signature::JsSignature;
    use crate::validator_set::{JsValidatorInfo, JsValidatorSet};

    /// Votes are signed messages from validators for a particular block which include
    /// information about the validator signing it.
    #[derive(Clone, Debug)]
    #[wasm_bindgen(getter_with_clone, js_name = "Vote")]
    pub struct JsVote {
        /// Type of vote (prevote or precommit)
        pub vote_type: JsVoteType,
        /// Block height
        pub height: u64,
        /// Round
        pub round: u32,
        /// Block ID
        pub block_id: Option<JsBlockId>,
        /// Timestamp
        pub timestamp: Option<String>,
        /// Validator address
        pub validator_address: String,
        /// Validator index
        pub validator_index: u32,
        /// Signature
        pub signature: Option<JsSignature>,
        /// Vote extension provided by the application. Only valid for precommit messages.
        pub extension: Vec<u8>,
        /// Vote extension signature by the validator Only valid for precommit messages.
        pub extension_signature: Option<JsSignature>,
    }

    impl From<Vote> for JsVote {
        fn from(value: Vote) -> Self {
            JsVote {
                vote_type: value.vote_type.into(),
                height: value.height.value(),
                round: value.round.into(),
                block_id: value.block_id.map(Into::into),
                timestamp: value.timestamp.map(|ts| ts.to_rfc3339()),
                validator_address: value.validator_address.to_string(),
                validator_index: value.validator_index.into(),
                signature: value.signature.map(Into::into),
                extension: value.extension,
                extension_signature: value.extension_signature.map(Into::into),
            }
        }
    }

    /// Types of votes
    #[derive(Clone, Copy, Debug)]
    #[wasm_bindgen(js_name = "VoteType")]
    pub enum JsVoteType {
        /// Prevote
        Prevote,
        /// Precommit
        Precommit,
    }

    impl From<VoteType> for JsVoteType {
        fn from(value: VoteType) -> Self {
            match value {
                VoteType::Prevote => JsVoteType::Prevote,
                VoteType::Precommit => JsVoteType::Precommit,
            }
        }
    }

    /// Duplicate vote evidence
    #[derive(Clone, Debug)]
    #[wasm_bindgen(getter_with_clone, js_name = "DuplicateVoteEvidence")]
    pub struct JsDuplicateVoteEvidence {
        /// Vote A
        pub vote_a: JsVote,
        /// Vote B
        pub vote_b: JsVote,
        /// Total voting power
        pub total_voting_power: u64,
        /// Validator power
        pub validator_power: u64,
        /// Timestamp
        pub timestamp: String,
    }
    /// Conflicting block detected in light client attack
    #[derive(Clone, Debug)]
    #[wasm_bindgen(getter_with_clone, js_name = "ConflictingBlock")]
    pub struct JsConflictingBlock {
        /// Signed header
        pub signed_header: JsSignedHeader,
        /// Validator set
        pub validator_set: JsValidatorSet,
    }

    impl From<ConflictingBlock> for JsConflictingBlock {
        fn from(value: ConflictingBlock) -> Self {
            JsConflictingBlock {
                signed_header: value.signed_header.into(),
                validator_set: value.validator_set.into(),
            }
        }
    }

    /// LightClient attack evidence
    #[derive(Clone, Debug)]
    #[wasm_bindgen(getter_with_clone, js_name = "LightClientAttackEvidence")]
    pub struct JsLightClientAttackEvidence {
        /// Conflicting block
        pub conflicting_block: JsConflictingBlock,
        /// Common height
        pub common_height: u64,
        /// Byzantine validators
        pub byzantine_validators: Vec<JsValidatorInfo>,
        /// Total voting power
        pub total_voting_power: u64,
        /// Timestamp
        pub timestamp: String,
    }

    /// Attack type for the associated evidence
    #[wasm_bindgen(js_name = "AttackType")]
    pub enum JsAttackType {
        /// Duplicate vote
        DuplicateVote,
        /// LightClient attack
        LightClient,
    }

    /// Evidence of malfeasance by validators (i.e. signing conflicting votes or light client attack).
    #[wasm_bindgen(js_name = Evidence)]
    pub struct JsEvidence(Evidence);

    #[wasm_bindgen(js_class = Evidence)]
    impl JsEvidence {
        /// type of the attack for the provided evidence
        pub fn attack_type(&self) -> JsAttackType {
            match &self.0 {
                Evidence::DuplicateVote(_) => JsAttackType::DuplicateVote,
                Evidence::LightClientAttack(_) => JsAttackType::LightClient,
            }
        }

        /// get duplicate vote evidence, if appropriate type
        #[wasm_bindgen(getter)]
        pub fn duplicate_vote_evidence(&self) -> Option<JsDuplicateVoteEvidence> {
            let Evidence::DuplicateVote(e) = &self.0 else {
                return None;
            };
            Some(JsDuplicateVoteEvidence {
                vote_a: e.vote_a.clone().into(),
                vote_b: e.vote_b.clone().into(),
                total_voting_power: e.total_voting_power.into(),
                validator_power: e.validator_power.into(),
                timestamp: e.timestamp.to_rfc3339(),
            })
        }

        /// get light client attack evidence, if appropriate type
        #[wasm_bindgen(getter)]
        pub fn light_client_attack_evidence(&self) -> Option<JsLightClientAttackEvidence> {
            let Evidence::LightClientAttack(e) = &self.0 else {
                return None;
            };
            Some(JsLightClientAttackEvidence {
                conflicting_block: e.conflicting_block.clone().into(),
                common_height: e.common_height.value(),
                byzantine_validators: e
                    .byzantine_validators
                    .iter()
                    .cloned()
                    .map(Into::into)
                    .collect(),
                total_voting_power: e.total_voting_power.into(),
                timestamp: e.timestamp.to_rfc3339(),
            })
        }
    }

    impl From<Evidence> for JsEvidence {
        fn from(value: Evidence) -> Self {
            JsEvidence(value)
        }
    }
}

/// uniffi conversion types
#[cfg(feature = "uniffi")]
pub mod uniffi_types {
    use tendermint::evidence::{
        ConflictingBlock as TendermintConfliclingBlock, DuplicateVoteEvidence,
        Evidence as TendermintEvidence, LightClientAttackEvidence, List as TendermintEvidenceList,
    };
    use tendermint::vote::{Type as VoteType, Vote as TendermintVote};
    use uniffi::{Enum, Record};

    use crate::block::header::uniffi_types::SignedHeader;
    use crate::block::uniffi_types::{BlockHeight, BlockId};
    use crate::error::UniffiConversionError;
    use crate::signature::uniffi_types::Signature;
    use crate::state::UniffiAccountId;
    use crate::uniffi_types::Time;
    use crate::validator_set::uniffi_types::{ValidatorInfo, ValidatorSet};

    /// Conflicting block detected in light client attack
    #[derive(Record)]
    pub struct ConflictingBlock {
        /// Signed header
        pub signed_header: SignedHeader,
        /// Validator set
        pub validator_set: ValidatorSet,
    }

    impl TryFrom<TendermintConfliclingBlock> for ConflictingBlock {
        type Error = UniffiConversionError;

        fn try_from(value: TendermintConfliclingBlock) -> Result<Self, Self::Error> {
            Ok(ConflictingBlock {
                signed_header: value.signed_header.try_into()?,
                validator_set: value.validator_set.into(),
            })
        }
    }

    impl TryFrom<ConflictingBlock> for TendermintConfliclingBlock {
        type Error = UniffiConversionError;

        fn try_from(value: ConflictingBlock) -> Result<Self, Self::Error> {
            Ok(TendermintConfliclingBlock {
                signed_header: value.signed_header.try_into()?,
                validator_set: value.validator_set.try_into()?,
            })
        }
    }

    uniffi::custom_type!(TendermintEvidenceList, Vec<Evidence>, {
        remote,
        try_lift: |value| {
            let evidence : Vec<_> = value.into_iter().map(|e| e.try_into()).collect::<Result<_, _>>()?;
            Ok(TendermintEvidenceList::new(evidence))
        },
        lower: |value| value.into_vec().into_iter().map(|e| e.try_into()).collect::<Result<_,_>>().expect("valid timestamps")
    });

    /// Evidence of malfeasance by validators (i.e. signing conflicting votes or light client attack).
    #[allow(clippy::large_enum_variant)]
    #[derive(Enum)]
    pub enum Evidence {
        /// Duplicate vote evidence
        DuplicateVote {
            /// Vote A
            vote_a: Vote,
            /// Vote B
            vote_b: Vote,
            /// Total voting power
            total_voting_power: u64,
            /// Validator power
            validator_power: u64,
            /// Timestamp
            timestamp: Time,
        },
        /// LightClient attack evidence
        LightClientAttack {
            /// Conflicting block
            conflicting_block: ConflictingBlock,
            /// Common height
            common_height: u64,
            /// Byzantine validators
            byzantine_validators: Vec<ValidatorInfo>,
            /// Total voting power
            total_voting_power: u64,
            /// Timestamp
            timestamp: Time,
        },
    }

    impl TryFrom<TendermintEvidence> for Evidence {
        type Error = UniffiConversionError;

        fn try_from(value: TendermintEvidence) -> Result<Self, Self::Error> {
            Ok(match value {
                TendermintEvidence::DuplicateVote(e) => Evidence::DuplicateVote {
                    vote_a: e.vote_a.try_into()?,
                    vote_b: e.vote_b.try_into()?,
                    total_voting_power: e.total_voting_power.value(),
                    validator_power: e.validator_power.value(),
                    timestamp: e.timestamp.try_into()?,
                },
                TendermintEvidence::LightClientAttack(e) => Evidence::LightClientAttack {
                    conflicting_block: e.conflicting_block.try_into()?,
                    common_height: e.common_height.into(),
                    byzantine_validators: e
                        .byzantine_validators
                        .into_iter()
                        .map(|v| v.into())
                        .collect(),
                    total_voting_power: e.total_voting_power.value(),
                    timestamp: e.timestamp.try_into()?,
                },
            })
        }
    }

    impl TryFrom<Evidence> for TendermintEvidence {
        type Error = UniffiConversionError;
        fn try_from(value: Evidence) -> Result<Self, Self::Error> {
            match value {
                Evidence::DuplicateVote {
                    vote_a,
                    vote_b,
                    total_voting_power,
                    validator_power,
                    timestamp,
                } => {
                    let evidence = DuplicateVoteEvidence {
                        vote_a: vote_a.try_into()?,
                        vote_b: vote_b.try_into()?,
                        total_voting_power: total_voting_power
                            .try_into()
                            .map_err(|_| UniffiConversionError::InvalidVotingPower)?,
                        validator_power: validator_power
                            .try_into()
                            .map_err(|_| UniffiConversionError::InvalidVotingPower)?,
                        timestamp: timestamp.try_into()?,
                    };
                    Ok(TendermintEvidence::DuplicateVote(Box::new(evidence)))
                }
                Evidence::LightClientAttack {
                    conflicting_block,
                    common_height,
                    byzantine_validators,
                    total_voting_power,
                    timestamp,
                } => {
                    let evidence = LightClientAttackEvidence {
                        conflicting_block: conflicting_block.try_into()?,
                        common_height: common_height
                            .try_into()
                            .map_err(|_| UniffiConversionError::HeaderHeightOutOfRange)?,
                        byzantine_validators: byzantine_validators
                            .into_iter()
                            .map(|v| v.try_into())
                            .collect::<Result<_, _>>()?,
                        total_voting_power: total_voting_power
                            .try_into()
                            .map_err(|_| UniffiConversionError::InvalidVotingPower)?,
                        timestamp: timestamp.try_into()?,
                    };

                    Ok(TendermintEvidence::LightClientAttack(Box::new(evidence)))
                }
            }
        }
    }

    /// Types of votes
    #[uniffi::remote(Enum)]
    #[repr(u8)]
    pub enum VoteType {
        /// Prevote
        Prevote = 1,
        /// Precommit
        Precommit = 2,
    }

    /// Votes are signed messages from validators for a particular block which include information
    /// about the validator signing it.
    #[derive(Record)]
    pub struct Vote {
        /// Type of vote (prevote or precommit)
        pub vote_type: VoteType,
        /// Block height
        pub height: BlockHeight,
        /// Round
        pub round: u32,
        /// Block ID
        pub block_id: Option<BlockId>,
        /// Timestamp
        pub timestamp: Option<Time>,
        /// Validator address
        pub validator_address: UniffiAccountId,
        /// Validator index
        pub validator_index: u32,
        /// Signature
        pub signature: Option<Signature>,
        /// Vote extension provided by the application. Only valid for precommit messages.
        pub extension: Vec<u8>,
        /// Vote extension signature by the validator Only valid for precommit messages.
        pub extension_signature: Option<Signature>,
    }

    impl TryFrom<Vote> for TendermintVote {
        type Error = UniffiConversionError;

        fn try_from(value: Vote) -> Result<Self, Self::Error> {
            Ok(TendermintVote {
                vote_type: value.vote_type,
                height: value.height.try_into()?,
                round: value
                    .round
                    .try_into()
                    .map_err(|_| UniffiConversionError::InvalidRoundIndex)?,
                block_id: value.block_id.map(TryInto::try_into).transpose()?,
                timestamp: value.timestamp.map(TryInto::try_into).transpose()?,
                validator_address: value.validator_address.try_into()?,
                validator_index: value
                    .validator_index
                    .try_into()
                    .map_err(|_| UniffiConversionError::InvalidValidatorIndex)?,
                signature: value.signature.map(TryInto::try_into).transpose()?,
                extension: value.extension,
                extension_signature: value
                    .extension_signature
                    .map(TryInto::try_into)
                    .transpose()?,
            })
        }
    }

    impl TryFrom<TendermintVote> for Vote {
        type Error = UniffiConversionError;

        fn try_from(value: TendermintVote) -> Result<Self, Self::Error> {
            Ok(Vote {
                vote_type: value.vote_type,
                height: value.height.into(),
                round: value.round.value(),
                block_id: value.block_id.map(Into::into),
                timestamp: value.timestamp.map(TryInto::try_into).transpose()?,
                validator_address: value.validator_address.into(),
                validator_index: value.validator_index.value(),
                signature: value.signature.map(Into::into),
                extension: value.extension,
                extension_signature: value.extension_signature.map(Into::into),
            })
        }
    }

    uniffi::custom_type!(TendermintVote, Vote, {
        remote,
        try_lift: |value| Ok(value.try_into()?),
        lower: |value| value.try_into().expect("valid tendermint time")
    });
}
