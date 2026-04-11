//! Protocol parameters and client configuration.
//!
//! Defines the fundamental protocol constants from which all other
//! configuration values are derived.

/// Fraction represented as numerator/denominator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fraction {
    /// The numerator of the fraction.
    pub numerator: u64,
    /// The denominator of the fraction.
    pub denominator: u64,
}

impl Fraction {
    /// Creates a new `Fraction`.
    ///
    /// # Panics
    ///
    /// Panics if `denominator` is zero.
    pub const fn new(numerator: u64, denominator: u64) -> Self {
        assert!(denominator != 0, "Fraction denominator must not be zero");
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
    safety_threshold: Fraction::new(2, 3),
    liveness_threshold: Fraction::new(1, 3),
    max_blob_size: 1 << 27, // 128 MiB
    min_row_size: 64,       // 1 << 6
};

/// The blob header length in bytes.
const BLOB_HEADER_LEN: usize = 5;

/// Maximum size of a PaymentPromise in bytes (excluding protobuf encoding overhead).
const MAX_PAYMENT_PROMISE_SIZE: usize = 209;

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
        let max_stake_num =
            (self.safety_threshold.denominator - self.safety_threshold.numerator) as usize;
        let max_stake_den = self.safety_threshold.denominator as usize;

        // rows = ceil(rows * max_stake / liveness_threshold)
        let num = self.rows * max_stake_num * self.liveness_threshold.denominator as usize;
        let den = max_stake_den * self.liveness_threshold.numerator as usize;
        ceil_div(num, den)
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
            ceil_div(self.rows, validators_for_reconstruction)
        };

        unique_decode_samples.max(reconstruction_samples)
    }

    /// Returns the minimum number of validators needed to reconstruct the original data.
    pub fn validators_for_reconstruction(&self) -> usize {
        let num = self.liveness_threshold.numerator as usize;
        let den = self.liveness_threshold.denominator as usize;
        1usize.max(ceil_div(self.max_validator_count * num, den))
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

    /// Calculates the maximum size of a shard in bytes.
    pub fn max_shard_size(&self) -> usize {
        const ROW_INDEX_SIZE: usize = 4; // uint32 index per row
        const RLC_COEFF_SIZE: usize = 16; // uint128 coefficient per row

        let total_rows = self.total_rows();
        let max_row_size = self.max_row_size(0); // version 0 is the only supported version
        let rlc_coeffs_size = self.rows * RLC_COEFF_SIZE;

        // Merkle tree depth for inclusion proofs
        let tree_depth = usize::BITS as usize - (total_rows - 1).leading_zeros() as usize;
        let proof_size_per_row = tree_depth * 32; // sha256::Size = 32

        rlc_coeffs_size
            + (self.max_rows_per_validator() * (ROW_INDEX_SIZE + max_row_size + proof_size_per_row))
    }

    /// Returns the maximum gRPC message size for upload requests.
    pub fn max_message_size(&self) -> usize {
        let msg_size = self.max_shard_size() + MAX_PAYMENT_PROMISE_SIZE;
        msg_size + (msg_size / 50) // add 2% protobuf overhead
    }
}

/// Runtime configuration for `FibreClient`.
#[derive(Debug, Clone)]
pub struct FibreClientConfig {
    /// Chain ID for domain separation in PaymentPromise signatures.
    pub chain_id: String,
    /// Safety threshold (fraction of stake needed for safety, typically 2/3).
    pub safety_threshold: Fraction,
    /// Liveness threshold (fraction of stake for liveness, typically 1/3).
    pub liveness_threshold: Fraction,
    /// Minimum rows each validator must receive.
    pub min_rows_per_validator: usize,
    /// Maximum gRPC message size for upload requests.
    pub max_message_size: usize,
    /// Maximum concurrent upload tasks.
    pub upload_concurrency: usize,
    /// Maximum concurrent download tasks.
    pub download_concurrency: usize,
}

impl FibreClientConfig {
    /// Creates a `FibreClientConfig` from protocol parameters.
    pub fn from_params(params: &ProtocolParams) -> Self {
        Self {
            chain_id: String::new(),
            safety_threshold: params.safety_threshold,
            liveness_threshold: params.liveness_threshold,
            min_rows_per_validator: params.min_rows_per_validator(),
            max_message_size: params.max_message_size(),
            upload_concurrency: params.max_validator_count,
            download_concurrency: params.max_validator_count,
        }
    }
}

impl Default for FibreClientConfig {
    fn default() -> Self {
        Self::from_params(&DEFAULT_PROTOCOL_PARAMS)
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
        assert!(
            blob_version == 0,
            "unsupported blob version: {blob_version}"
        );

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

/// Returns `ceil(a / b)` using integer arithmetic.
fn ceil_div(a: usize, b: usize) -> usize {
    a.div_ceil(b)
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
    fn default_total_rows() {
        assert_eq!(DEFAULT_PROTOCOL_PARAMS.total_rows(), 16384);
    }

    #[test]
    fn default_parity_rows() {
        assert_eq!(DEFAULT_PROTOCOL_PARAMS.parity_rows(), 12288);
    }

    #[test]
    fn default_max_rows_per_validator() {
        assert_eq!(DEFAULT_PROTOCOL_PARAMS.max_rows_per_validator(), 4096);
    }

    #[test]
    fn default_min_rows_per_validator() {
        // unique_decode_samples = ceil(100 / (1 - log2(1.25))) = ceil(147.47...) = 148
        // reconstruction_samples = ceil(4096 / 34) = 121
        // max(148, 121) = 148
        assert_eq!(DEFAULT_PROTOCOL_PARAMS.min_rows_per_validator(), 148);
    }

    #[test]
    fn default_validators_for_reconstruction() {
        // ceil(100 * 1 / 3) = ceil(33.33) = 34
        assert_eq!(DEFAULT_PROTOCOL_PARAMS.validators_for_reconstruction(), 34);
    }

    #[test]
    fn row_size_exact_fit() {
        let p = ProtocolParams {
            rows: 8,
            min_row_size: 64,
            ..DEFAULT_PROTOCOL_PARAMS
        };
        // 64 * 8 = 512 bytes, exactly 8 rows of 64
        assert_eq!(p.row_size(0, 64 * 8), 64);
    }

    #[test]
    fn row_size_needs_rounding() {
        let p = ProtocolParams {
            rows: 8,
            min_row_size: 64,
            ..DEFAULT_PROTOCOL_PARAMS
        };
        // ceil(105/8) = 14, rounded up to 64
        assert_eq!(p.row_size(0, 100 + BLOB_HEADER_LEN), 64);
    }

    #[test]
    fn row_size_small_data() {
        let p = ProtocolParams {
            rows: 8,
            min_row_size: 64,
            ..DEFAULT_PROTOCOL_PARAMS
        };
        // ceil(6/8) = 1, rounded up to 64
        assert_eq!(p.row_size(0, 1 + BLOB_HEADER_LEN), 64);
    }

    #[test]
    fn row_size_large_data() {
        let p = ProtocolParams {
            rows: 8,
            min_row_size: 64,
            ..DEFAULT_PROTOCOL_PARAMS
        };
        // ceil(10005/8) = 1251, rounded up to 1280 (20*64)
        assert_eq!(p.row_size(0, 10000 + BLOB_HEADER_LEN), 1280);
    }

    #[test]
    fn row_size_different_min() {
        let p = ProtocolParams {
            rows: 4,
            min_row_size: 128,
            ..DEFAULT_PROTOCOL_PARAMS
        };
        // ceil(1005/4) = 252, rounded up to 256 (2*128)
        assert_eq!(p.row_size(0, 1000 + BLOB_HEADER_LEN), 256);
    }

    #[test]
    fn row_size_zero() {
        let p = ProtocolParams {
            rows: 8,
            min_row_size: 64,
            ..DEFAULT_PROTOCOL_PARAMS
        };
        assert_eq!(p.row_size(0, 0), 0);
    }

    #[test]
    fn blob_config_v0_defaults() {
        let cfg = BlobConfig::v0();
        assert_eq!(cfg.blob_version, 0);
        assert_eq!(cfg.original_rows, 4096);
        assert_eq!(cfg.parity_rows, 12288);
        assert_eq!(cfg.total_rows(), 16384);
        assert_eq!(cfg.max_data_size, (1 << 27) - BLOB_HEADER_LEN);
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
    fn fibre_client_config_default() {
        let cfg = FibreClientConfig::default();
        assert_eq!(
            cfg.safety_threshold,
            Fraction {
                numerator: 2,
                denominator: 3
            }
        );
        assert_eq!(
            cfg.liveness_threshold,
            Fraction {
                numerator: 1,
                denominator: 3
            }
        );
        assert_eq!(cfg.min_rows_per_validator, 148);
        assert_eq!(cfg.upload_concurrency, 100);
        assert_eq!(cfg.download_concurrency, 100);
    }

    #[test]
    fn fraction_new_valid() {
        let f = Fraction::new(1, 3);
        assert_eq!(f.numerator, 1);
        assert_eq!(f.denominator, 3);
    }

    #[test]
    #[should_panic(expected = "Fraction denominator must not be zero")]
    fn fraction_new_zero_denominator_panics() {
        Fraction::new(1, 0);
    }

    #[test]
    fn liveness_threshold_must_exceed_encoding_ratio() {
        let p = &DEFAULT_PROTOCOL_PARAMS;
        let liveness_ratio =
            p.liveness_threshold.numerator as f64 / p.liveness_threshold.denominator as f64;
        assert!(
            liveness_ratio >= p.encoding_ratio,
            "LivenessThreshold ({liveness_ratio}) must be >= EncodingRatio ({})",
            p.encoding_ratio
        );
    }
}
