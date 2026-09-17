use serde::Serialize;
use tendermint::block::Height;

pub trait TryIntoHeight: Serialize + Sized {
    // Convert self into a Height, returning an error if conversion fails
    fn try_into_height(self) -> Result<Height, HeightConversionError>;
}

// Error type for conversion failures
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HeightConversionError {
    #[error("Height cannot be zero")]
    ZeroHeight,
    #[error("Height cannot be negative")]
    NegativeHeight,
    #[error("Value too large for Height")]
    Overflow,
}

// Implementations for different types
impl TryIntoHeight for Height {
    fn try_into_height(self) -> Result<Height, HeightConversionError> {
        Ok(self)
    }
}

impl TryIntoHeight for u64 {
    fn try_into_height(self) -> Result<Height, HeightConversionError> {
        Height::try_from(self).map_err(|_| HeightConversionError::Overflow)
    }
}

impl TryIntoHeight for i64 {
    fn try_into_height(self) -> Result<Height, HeightConversionError> {
        if self < 0 {
            Err(HeightConversionError::NegativeHeight)
        } else if self == 0 {
            Err(HeightConversionError::ZeroHeight)
        } else {
            Ok(Height::try_from(self as u64).map_err(|_| HeightConversionError::Overflow)?)
        }
    }
}
