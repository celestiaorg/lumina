//! Fraud proof related types and traits.
//!
//! A fraud proof is a proof of the detected malicious action done to the network.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use tendermint_proto::Protobuf;

pub use crate::byzantine::BadEncodingFraudProof;
use crate::hash::Hash;
use crate::{Error, ExtendedHeader, Result};

/// A proof of the malicious actions done to the network.
pub trait FraudProof {
    /// Name of the proof type.
    const TYPE: &'static str;

    /// HeaderHash returns the block hash.
    fn header_hash(&self) -> Hash;

    /// Height returns the block height corresponding to the Proof.
    fn height(&self) -> u64;

    /// Checks the validity of the fraud proof.
    ///
    /// # Errors
    ///
    /// Returns an error if some conditions don't pass and thus fraud proof is not valid.
    fn validate(&self, header: &ExtendedHeader) -> Result<()>;
}

/// Raw representation of the generic fraud proof.
///
/// Consists of the name of the proof type and the protobuf serialized payload
/// holding the proof itself.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RawFraudProof {
    proof_type: String,
    #[serde(with = "tendermint_proto::serializers::bytes::base64string")]
    data: Vec<u8>,
}

/// Aggregation of all the supported fraud proofs.
#[derive(Clone, Debug, PartialEq, Deserialize)]
#[serde(try_from = "RawFraudProof", into = "RawFraudProof")]
#[non_exhaustive]
pub enum Proof {
    /// A proof that a block producer incorrectly encoded [`ExtendedDataSquare`].
    ///
    /// [`ExtendedDataSquare`]: crate::eds::ExtendedDataSquare
    BadEncoding(BadEncodingFraudProof),
}

impl Proof {
    /// Returns proof type.
    pub fn proof_type(&self) -> ProofType {
        match self {
            Proof::BadEncoding(_) => ProofType::BadEncoding,
        }
    }
}

/// Proof type
#[derive(Clone, Copy, Debug)]
pub enum ProofType {
    /// Bad encoding fraud proof.
    BadEncoding,
}

impl ProofType {
    /// Returns the string representation.
    pub fn to_str(&self) -> &'static str {
        match self {
            ProofType::BadEncoding => BadEncodingFraudProof::TYPE,
        }
    }
}

impl Serialize for ProofType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ProofType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ProofTypeVisitor;

        impl<'de> de::Visitor<'de> for ProofTypeVisitor {
            type Value = ProofType;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string representing proof type: 'badencoding'")
            }

            fn visit_str<E>(self, value: &str) -> Result<ProofType, E>
            where
                E: de::Error,
            {
                match value {
                    BadEncodingFraudProof::TYPE => Ok(ProofType::BadEncoding),
                    _ => Err(E::invalid_value(de::Unexpected::Str(value), &self)),
                }
            }
        }

        deserializer.deserialize_str(ProofTypeVisitor)
    }
}

impl TryFrom<RawFraudProof> for Proof {
    type Error = Error;

    fn try_from(value: RawFraudProof) -> Result<Self, Self::Error> {
        match value.proof_type.as_str() {
            BadEncodingFraudProof::TYPE => {
                let befp = BadEncodingFraudProof::decode_vec(&value.data)?;
                Ok(Proof::BadEncoding(befp))
            }
            _ => Err(Error::UnsupportedFraudProofType(value.proof_type)),
        }
    }
}

impl From<&Proof> for RawFraudProof {
    fn from(value: &Proof) -> Self {
        match value {
            Proof::BadEncoding(befp) => RawFraudProof {
                proof_type: BadEncodingFraudProof::TYPE.to_owned(),
                data: befp.clone().encode_vec(),
            },
        }
    }
}

impl Serialize for Proof {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let raw: RawFraudProof = self.into();
        raw.serialize(serializer)
    }
}

#[cfg(test)]
mod tests {
    use crate::test_utils::{ExtendedHeaderGenerator, corrupt_eds, generate_dummy_eds};

    use super::*;

    #[test]
    fn befp_serde() {
        let mut generator = ExtendedHeaderGenerator::new();
        let mut eds = generate_dummy_eds(8);
        let (_, proof) = corrupt_eds(&mut generator, &mut eds);

        let proof = Proof::BadEncoding(proof);

        let serialized = serde_json::to_string(&proof).unwrap();
        let deserialized: Proof = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized, proof);
    }
}
