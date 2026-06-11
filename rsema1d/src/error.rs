use thiserror::Error;

/// Errors produced by rsema1d operations.
#[derive(Debug, Error)]
pub enum Error {
    /// Generic invalid-parameter error with a descriptive message.
    #[error("Invalid parameters: {0}")]
    InvalidParameters(String),

    /// Row size is not a positive multiple of 64.
    #[error("Invalid row size: must be multiple of 64, got {0}")]
    InvalidRowSize(usize),

    /// K is out of the valid range `[1, 65536]`.
    #[error("Invalid K: must be between 1 and 65536, got {0}")]
    InvalidK(usize),

    /// N is out of the valid range `[1, 65536]`.
    #[error("Invalid N: must be between 1 and 65536, got {0}")]
    InvalidN(usize),

    /// K + N exceeds the maximum of 65536.
    #[error("K + N must be <= 65536, got K={0}, N={1}")]
    InvalidKPlusN(usize, usize),

    /// A row did not have the expected byte length.
    #[error("Row length mismatch: expected {expected}, got {actual}")]
    RowLengthMismatch {
        /// Expected row length in bytes.
        expected: usize,
        /// Actual row length in bytes.
        actual: usize,
    },

    /// A row index was out of bounds.
    #[error("Invalid index: {0} (must be < K+N={1})")]
    InvalidIndex(usize, usize),

    /// An error propagated from the Reed-Solomon backend.
    #[error("Reed-Solomon error: {0}")]
    ReedSolomon(String),

    /// Proof verification did not succeed.
    #[error("Proof verification failed: {0}")]
    VerificationFailed(String),
}

/// Convenience alias for `std::result::Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;
