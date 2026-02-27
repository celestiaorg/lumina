use crate::error::{Error, Result};

/// Codec parameters
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Parameters {
    /// Number of original rows
    pub k: usize,
    /// Number of parity rows
    pub n: usize,
    /// Size of each row in bytes (must be multiple of 64)
    pub row_size: usize,
}

impl Parameters {
    /// Create new parameters with validation
    pub fn new(k: usize, n: usize, row_size: usize) -> Result<Self> {
        if k == 0 || k > 65536 {
            return Err(Error::InvalidK(k));
        }

        if n == 0 || n > 65536 {
            return Err(Error::InvalidN(n));
        }

        if k + n > 65536 {
            return Err(Error::InvalidKPlusN(k, n));
        }

        if row_size == 0 || row_size % 64 != 0 {
            return Err(Error::InvalidRowSize(row_size));
        }

        Ok(Self { k, n, row_size })
    }

    /// Total number of rows (K + N)
    pub fn total_rows(&self) -> usize {
        self.k + self.n
    }

    /// Number of symbols per row
    pub fn symbols_per_row(&self) -> usize {
        self.row_size / 2
    }

    /// Number of 64-byte chunks per row
    pub fn chunks_per_row(&self) -> usize {
        self.row_size / 64
    }

    /// Next power of 2 >= K (for tree padding)
    pub fn k_padded(&self) -> usize {
        self.k.next_power_of_two()
    }

    /// Total padded size for tree construction
    pub fn total_padded(&self) -> usize {
        (self.k_padded() + self.n).next_power_of_two()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_parameters() {
        let params = Parameters::new(4, 4, 64).unwrap();
        assert_eq!(params.k, 4);
        assert_eq!(params.n, 4);
        assert_eq!(params.row_size, 64);
    }

    #[test]
    fn test_invalid_row_size() {
        assert!(Parameters::new(4, 4, 63).is_err());
        assert!(Parameters::new(4, 4, 100).is_err());
    }

    #[test]
    fn test_padding_calculation() {
        let params = Parameters::new(5, 7, 64).unwrap();
        assert_eq!(params.k_padded(), 8); // Next power of 2 after 5
        assert_eq!(params.total_padded(), 16); // Next power of 2 after 8+7=15
    }
}
