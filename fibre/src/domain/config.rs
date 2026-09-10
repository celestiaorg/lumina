//! Protocol parameters and client configuration.
//!
//! Defines the fundamental protocol constants from which all other
//! configuration values are derived.

use std::num::NonZeroU64;

use crate::error::FibreError;

/// Maximum allowed chain ID length.
pub(crate) const MAX_CHAIN_ID_SIZE: usize = 20;

pub(crate) fn validate_chain_id(chain_id: &str) -> Result<(), FibreError> {
    let len = chain_id.len();
    if len == 0 || len > MAX_CHAIN_ID_SIZE {
        return Err(FibreError::InvalidChainId { len });
    }
    Ok(())
}

/// Fraction represented as numerator/denominator.
///
/// Both fields are non-zero: threshold math divides by each of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fraction {
    /// The numerator of the fraction.
    pub numerator: NonZeroU64,
    /// The denominator of the fraction.
    pub denominator: NonZeroU64,
}

impl Fraction {
    /// Creates a new `Fraction`.
    pub const fn new(numerator: NonZeroU64, denominator: NonZeroU64) -> Self {
        Self {
            numerator,
            denominator,
        }
    }
}

/// Root protocol constants from which all other values are derived.
///
/// The design separates "root constants" (what we chose) from "derived values"
/// (what follows from our choices). This enables clear documentation of protocol
/// design decisions, a single source of truth for protocol constants, and easy
/// versioning.
#[derive(Debug, Clone)]
pub struct ProtocolParams {
    /// Number of original data rows (K in rsema1d). Default: 4096.
    pub rows: usize,
    /// Fraction of total rows that are original (K / (K + N)). Default: 0.25.
    pub encoding_ratio: f64,
    /// Maximum expected validator count. Default: 100.
    pub max_validator_count: usize,
    /// Security parameter for unique decodability (lambda). Default: 100.
    pub unique_decoding_security_bits: usize,
    /// Fraction of stake for safety (typically 2/3).
    pub safety_threshold: Fraction,
    /// Fraction of stake for liveness (typically 1/3).
    pub liveness_threshold: Fraction,
    /// Maximum blob size in bytes (including header). Default: 128 MiB.
    pub max_blob_size: usize,
    /// Minimum row size in bytes (rows are rounded up to this). Default: 64.
    pub min_row_size: usize,
}

/// Compile-time default protocol parameters.
pub const DEFAULT_PROTOCOL_PARAMS: ProtocolParams = ProtocolParams {
    rows: 4096,           // 1 << 12
    encoding_ratio: 0.25, // 3x parity (12288 parity rows, 16384 total)
    max_validator_count: 100,
    unique_decoding_security_bits: 100,
    safety_threshold: Fraction::new(NonZeroU64::new(2).unwrap(), NonZeroU64::new(3).unwrap()),
    liveness_threshold: Fraction::new(NonZeroU64::new(1).unwrap(), NonZeroU64::new(3).unwrap()),
    max_blob_size: 1 << 27, // 128 MiB
    min_row_size: 64,       // 1 << 6
};

/// The blob header length in bytes.
const BLOB_HEADER_LEN: usize = 5;

impl ProtocolParams {
    /// Returns the total number of rows (K + N).
    pub fn total_rows(&self) -> usize {
        (self.rows as f64 / self.encoding_ratio) as usize
    }

    /// Returns the number of parity rows (N in rsema1d).
    pub fn parity_rows(&self) -> usize {
        self.total_rows() - self.rows
    }

    /// Returns the maximum number of rows a single validator could receive.
    pub fn max_rows_per_validator(&self) -> usize {
        // maxStake = 1 - safety_threshold
        let max_stake_num = self
            .safety_threshold
            .denominator
            .get()
            .checked_sub(self.safety_threshold.numerator.get())
            .expect("safety threshold numerator must not exceed denominator")
            as usize;
        let max_stake_den = self.safety_threshold.denominator.get() as usize;

        // rows = ceil(rows * max_stake / liveness_threshold)
        let num = self.rows * max_stake_num * self.liveness_threshold.denominator.get() as usize;
        let den = max_stake_den * self.liveness_threshold.numerator.get() as usize;
        num.div_ceil(den)
    }

    /// Returns the minimum number of rows each validator must receive for
    /// unique decodability security, regardless of their stake percentage.
    pub fn min_rows_per_validator(&self) -> usize {
        // Constraint 1: Unique decoding security
        //
        // The minimum number of samples s required for lambda bits of security:
        //
        //         s >= ceil(lambda / (1 - log2(1 + rho)))
        //
        // Where:
        //   lambda = unique_decoding_security_bits
        //   rho    = encoding_ratio = K/(K+N)
        let unique_decode_samples = {
            let lambda = self.unique_decoding_security_bits as f64;
            let rho = self.encoding_ratio;
            let denominator = 1.0 - (1.0 + rho).log2();
            (lambda / denominator).ceil() as usize
        };

        // Constraint 2: Reconstruction samples for fault tolerance
        // We need enough rows from liveness_threshold fraction of validators to reconstruct.
        let reconstruction_samples = {
            let validators_for_reconstruction = self.validators_for_reconstruction();
            self.rows.div_ceil(validators_for_reconstruction)
        };

        unique_decode_samples.max(reconstruction_samples)
    }

    /// Returns the minimum number of validators needed to reconstruct the original data.
    pub fn validators_for_reconstruction(&self) -> usize {
        let num = self.liveness_threshold.numerator.get() as usize;
        let den = self.liveness_threshold.denominator.get() as usize;
        1usize.max((self.max_validator_count * num).div_ceil(den))
    }

    /// Computes the row size for the given blob version and total length.
    ///
    /// Returns 0 if `total_len` is 0.
    ///
    /// # Panics
    ///
    /// Panics if `blob_version` is not 0.
    pub fn row_size(&self, blob_version: u8, total_len: usize) -> usize {
        assert_eq!(blob_version, 0, "unsupported blob version: {blob_version}");
        compute_row_size(total_len, self.rows, self.min_row_size)
    }

    /// Returns the maximum row size based on `max_blob_size`.
    pub fn max_row_size(&self, blob_version: u8) -> usize {
        self.row_size(blob_version, self.max_blob_size)
    }
}

/// Runtime configuration for `FibreClient`.
#[derive(Debug, Clone)]
pub struct FibreClientConfig {
    /// Chain ID for domain separation in PaymentPromise signatures.
    chain_id: String,
    /// Safety threshold (fraction of stake needed for safety, typically 2/3).
    pub safety_threshold: Fraction,
    /// Liveness threshold (fraction of stake for liveness, typically 1/3).
    pub liveness_threshold: Fraction,
    /// Minimum rows each validator must receive.
    pub min_rows_per_validator: usize,
    /// Maximum concurrent upload tasks.
    pub upload_concurrency: usize,
    /// Maximum concurrent download tasks.
    pub download_concurrency: usize,
}

impl FibreClientConfig {
    /// Creates a `FibreClientConfig` with the default protocol parameters.
    pub fn new(chain_id: impl Into<String>) -> Result<Self, FibreError> {
        Self::from_params(chain_id, &DEFAULT_PROTOCOL_PARAMS)
    }

    /// Creates a `FibreClientConfig` from protocol parameters.
    pub fn from_params(
        chain_id: impl Into<String>,
        params: &ProtocolParams,
    ) -> Result<Self, FibreError> {
        let chain_id = chain_id.into();
        validate_chain_id(&chain_id)?;

        Ok(Self {
            chain_id,
            safety_threshold: params.safety_threshold,
            liveness_threshold: params.liveness_threshold,
            min_rows_per_validator: params.min_rows_per_validator(),
            upload_concurrency: params.max_validator_count,
            download_concurrency: params.max_validator_count,
        })
    }

    /// Returns the chain ID.
    pub fn chain_id(&self) -> &str {
        &self.chain_id
    }
}

/// Per-version blob encoding parameters.
///
/// Provides the row/parity counts and size computations needed for blob
/// encoding, decoding, and upload size calculation.
#[derive(Debug, Clone)]
pub struct BlobConfig {
    /// The blob format version (currently only version 0 is supported).
    pub blob_version: u8,
    /// Number of original rows before erasure coding (K in rsema1d).
    pub original_rows: usize,
    /// Number of parity rows added by erasure coding (N in rsema1d).
    pub parity_rows: usize,
    /// Maximum data size that can be passed to blob creation (excluding header).
    pub max_data_size: usize,
    // Store protocol params needed for row_size computation
    rows: usize,
    min_row_size: usize,
}

impl BlobConfig {
    /// Creates a `BlobConfig` for blob version 0 with default protocol parameters.
    pub fn v0() -> Self {
        Self::from_params(0, &DEFAULT_PROTOCOL_PARAMS)
    }

    /// Creates a `BlobConfig` from the given blob version and protocol parameters.
    ///
    /// # Panics
    ///
    /// Panics if `blob_version` is not 0.
    pub fn from_params(blob_version: u8, params: &ProtocolParams) -> Self {
        assert_eq!(blob_version, 0, "unsupported blob version: {blob_version}");

        Self {
            blob_version,
            original_rows: params.rows,
            parity_rows: params.parity_rows(),
            max_data_size: params.max_blob_size - BLOB_HEADER_LEN,
            rows: params.rows,
            min_row_size: params.min_row_size,
        }
    }

    /// Returns the total number of rows (original + parity).
    pub fn total_rows(&self) -> usize {
        self.original_rows + self.parity_rows
    }

    /// Computes the row size for the given data length.
    ///
    /// The data length is the raw data size (without header). The header length
    /// is added internally before computing the row size.
    pub fn row_size(&self, data_len: usize) -> usize {
        compute_row_size(data_len + BLOB_HEADER_LEN, self.rows, self.min_row_size)
    }

    /// Calculates the upload size of blob data with padding and without parity.
    ///
    /// This is the size included in the PaymentPromise and the one actually paid for.
    pub fn upload_size(&self, data_len: usize) -> usize {
        self.row_size(data_len) * self.original_rows
    }

    /// Returns the `BlobConfig` for the given blob version.
    ///
    /// Returns an error if the version is not supported.
    pub fn for_version(version: u8) -> Result<Self, crate::error::FibreError> {
        match version {
            0 => Ok(Self::v0()),
            _ => Err(crate::error::FibreError::UnsupportedBlobVersion(version)),
        }
    }

    /// Creates a `BlobConfig` with custom parameters for testing.
    ///
    /// This allows tests to use small K/N values and specific row sizes
    /// without going through the full `ProtocolParams` construction.
    #[cfg(test)]
    pub(crate) fn new_test(
        blob_version: u8,
        original_rows: usize,
        parity_rows: usize,
        max_data_size: usize,
        rows: usize,
        min_row_size: usize,
    ) -> Self {
        Self {
            blob_version,
            original_rows,
            parity_rows,
            max_data_size,
            rows,
            min_row_size,
        }
    }
}

/// Computes the row size for a given total byte length, rounding up to
/// `min_row_size` boundaries. Returns 0 if `total_len` is 0.
fn compute_row_size(total_len: usize, rows: usize, min_row_size: usize) -> usize {
    if total_len == 0 {
        return 0;
    }

    total_len.div_ceil(rows).div_ceil(min_row_size) * min_row_size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_configuration_values() {
        let blob_config = BlobConfig::v0();
        let client_config = FibreClientConfig::new("test-chain").unwrap();
        let cases = [
            ("total rows", DEFAULT_PROTOCOL_PARAMS.total_rows(), 16384),
            ("parity rows", DEFAULT_PROTOCOL_PARAMS.parity_rows(), 12288),
            (
                "maximum rows per validator",
                DEFAULT_PROTOCOL_PARAMS.max_rows_per_validator(),
                4096,
            ),
            (
                "minimum rows per validator",
                DEFAULT_PROTOCOL_PARAMS.min_rows_per_validator(),
                148,
            ),
            (
                "validators for reconstruction",
                DEFAULT_PROTOCOL_PARAMS.validators_for_reconstruction(),
                34,
            ),
            ("blob version", blob_config.blob_version as usize, 0),
            ("original rows", blob_config.original_rows, 4096),
            ("blob parity rows", blob_config.parity_rows, 12288),
            ("blob total rows", blob_config.total_rows(), 16384),
            (
                "maximum data size",
                blob_config.max_data_size,
                (1 << 27) - BLOB_HEADER_LEN,
            ),
            (
                "client minimum rows per validator",
                client_config.min_rows_per_validator,
                148,
            ),
            ("upload concurrency", client_config.upload_concurrency, 100),
            (
                "download concurrency",
                client_config.download_concurrency,
                100,
            ),
        ];

        for (name, actual, expected) in cases {
            assert_eq!(actual, expected, "{name}");
        }
        assert_eq!(client_config.chain_id(), "test-chain");
        assert_eq!(
            client_config.safety_threshold,
            crate::test_utils::fraction(2, 3)
        );
        assert_eq!(
            client_config.liveness_threshold,
            crate::test_utils::fraction(1, 3)
        );
    }

    #[test]
    fn row_size_cases() {
        let cases = [
            ("exact fit", 8, 64, 64 * 8, 64),
            ("rounding", 8, 64, 100 + BLOB_HEADER_LEN, 64),
            ("small data", 8, 64, 1 + BLOB_HEADER_LEN, 64),
            ("large data", 8, 64, 10000 + BLOB_HEADER_LEN, 1280),
            ("different minimum", 4, 128, 1000 + BLOB_HEADER_LEN, 256),
            ("zero", 8, 64, 0, 0),
        ];

        for (name, rows, min_row_size, total_len, expected) in cases {
            let params = ProtocolParams {
                rows,
                min_row_size,
                ..DEFAULT_PROTOCOL_PARAMS
            };
            assert_eq!(params.row_size(0, total_len), expected, "{name}");
        }
    }

    #[test]
    fn blob_config_upload_size() {
        let cfg = BlobConfig::v0();
        // For any data_len, upload_size = row_size(data_len) * original_rows
        let data_len = 1000;
        let row_size = cfg.row_size(data_len);
        assert_eq!(cfg.upload_size(data_len), row_size * cfg.original_rows);
    }

    #[test]
    fn fibre_client_config_rejects_invalid_chain_id() {
        assert!(matches!(
            FibreClientConfig::new(""),
            Err(FibreError::InvalidChainId { len: 0 })
        ));

        let too_long = "x".repeat(MAX_CHAIN_ID_SIZE + 1);
        assert!(matches!(
            FibreClientConfig::new(too_long),
            Err(FibreError::InvalidChainId { len }) if len == MAX_CHAIN_ID_SIZE + 1
        ));
    }

    #[test]
    #[should_panic(expected = "safety threshold numerator must not exceed denominator")]
    fn safety_threshold_above_one_panics() {
        let params = ProtocolParams {
            safety_threshold: Fraction::new(
                NonZeroU64::new(3).unwrap(),
                NonZeroU64::new(2).unwrap(),
            ),
            ..DEFAULT_PROTOCOL_PARAMS
        };

        params.max_rows_per_validator();
    }

    #[test]
    fn liveness_threshold_must_exceed_encoding_ratio() {
        let p = &DEFAULT_PROTOCOL_PARAMS;
        let liveness_ratio = p.liveness_threshold.numerator.get() as f64
            / p.liveness_threshold.denominator.get() as f64;
        assert!(
            liveness_ratio >= p.encoding_ratio,
            "LivenessThreshold ({liveness_ratio}) must be >= EncodingRatio ({})",
            p.encoding_ratio
        );
    }
}
