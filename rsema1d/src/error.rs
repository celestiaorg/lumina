use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid parameters: {0}")]
    InvalidParameters(String),

    #[error("Invalid row size: must be multiple of 64, got {0}")]
    InvalidRowSize(usize),

    #[error("Invalid K: must be between 1 and 65536, got {0}")]
    InvalidK(usize),

    #[error("Invalid N: must be between 1 and 65536, got {0}")]
    InvalidN(usize),

    #[error("K + N must be <= 65536, got K={0}, N={1}")]
    InvalidKPlusN(usize, usize),

    #[error("Row length mismatch: expected {expected}, got {actual}")]
    RowLengthMismatch { expected: usize, actual: usize },

    #[error("Invalid index: {0} (must be < K+N={1})")]
    InvalidIndex(usize, usize),

    #[error("Reed-Solomon error: {0}")]
    ReedSolomon(String),

    #[error("Proof verification failed: {0}")]
    VerificationFailed(String),
}

pub type Result<T> = std::result::Result<T, Error>;
