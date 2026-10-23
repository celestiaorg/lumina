//! Custom [`serde`] serializer for [`tendermint::hash::Hash`].

use std::str::FromStr;

use celestia_proto::serializers::cow_str::CowStr;
use serde::{Deserialize, Deserializer, Serializer};
use tendermint::hash::Hash;

/// Deserialize [`tendermint::hash::Hash`].
pub fn deserialize<'de, D>(deserializer: D) -> Result<Hash, D::Error>
where
    D: Deserializer<'de>,
{
    let hex = Option::<CowStr>::deserialize(deserializer)?.unwrap_or_default();
    Hash::from_str(&hex).map_err(serde::de::Error::custom)
}

/// Serialize [`tendermint::hash::Hash`].
pub fn serialize<S>(value: &Hash, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_some(&value.to_string())
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use tendermint::hash::Hash;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as test;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Wrapper(#[serde(with = "super")] Hash);

    #[test]
    fn round_trip() {
        let hash = Wrapper(Hash::Sha256([0xab; 32]));
        let json = serde_json::to_string(&hash).unwrap();

        assert_eq!(json, format!("\"{}\"", "AB".repeat(32)));
        assert_eq!(serde_json::from_str::<Wrapper>(&json).unwrap(), hash);
    }

    #[test]
    fn empty_and_null_are_none() {
        assert_eq!(serde_json::to_string(&Wrapper(Hash::None)).unwrap(), "\"\"");
        assert_eq!(
            serde_json::from_str::<Wrapper>("\"\"").unwrap().0,
            Hash::None
        );
        assert_eq!(
            serde_json::from_str::<Wrapper>("null").unwrap().0,
            Hash::None
        );
    }
}
