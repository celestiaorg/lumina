//! Blob encoding/decoding, BlobID, and Commitment types.
//!
//! This module implements the core blob data structures for the Fibre protocol:
//! - `Commitment`: a 32-byte SHA-256 commitment hash
//! - `BlobID`: version byte + commitment that uniquely identifies a blob
//! - `Blob`: encoded data with Reed-Solomon erasure coding

use std::fmt;
use std::sync::Arc;

use crate::blob_header::BlobHeaderV0;
use crate::config::BlobConfig;
use crate::error::FibreError;

/// A 32-byte SHA-256 commitment hash. Re-exports rsema1d's Commitment type.
pub type Commitment = rsema1d::Commitment;

/// Size of a Commitment in bytes.
pub const COMMITMENT_SIZE: usize = 32;

/// Size of a BlobID in bytes: 1 (version) + 32 (commitment) = 33.
pub const BLOB_ID_SIZE: usize = 33;

/// Uniquely identifies a blob by combining version and commitment.
///
/// The first byte encodes the blob version, followed by 32 bytes of commitment.
/// This makes BlobIDs self-describing, allowing clients to know the blob format
/// before downloading.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BlobID([u8; BLOB_ID_SIZE]);

impl BlobID {
    /// Create a new BlobID from a version byte and a commitment.
    pub fn new(version: u8, commitment: Commitment) -> Self {
        let mut id = [0u8; BLOB_ID_SIZE];
        id[0] = version;
        id[1..].copy_from_slice(&commitment);
        Self(id)
    }

    /// Returns the blob version encoded in this BlobID.
    pub fn version(&self) -> u8 {
        self.0[0]
    }

    /// Returns the 32-byte commitment (without the version prefix).
    pub fn commitment(&self) -> Commitment {
        let mut c = [0u8; COMMITMENT_SIZE];
        c.copy_from_slice(&self.0[1..]);
        c
    }

    /// Validate that this BlobID is well-formed.
    ///
    /// Checks that the version is supported (currently only version 0).
    pub fn validate(&self) -> Result<(), FibreError> {
        if self.0[0] != 0 {
            return Err(FibreError::UnsupportedBlobVersion(self.0[0]));
        }
        Ok(())
    }

    /// Returns a reference to the raw bytes of this BlobID.
    pub fn as_bytes(&self) -> &[u8; BLOB_ID_SIZE] {
        &self.0
    }

    /// Construct a BlobID from a byte slice.
    ///
    /// Returns an error if the slice is not exactly `BLOB_ID_SIZE` bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FibreError> {
        if bytes.len() != BLOB_ID_SIZE {
            return Err(FibreError::InvalidBlobId(format!(
                "blob ID must be {} bytes, got {}",
                BLOB_ID_SIZE,
                bytes.len()
            )));
        }
        let mut id = [0u8; BLOB_ID_SIZE];
        id.copy_from_slice(bytes);
        Ok(Self(id))
    }

    /// Construct a BlobID from a hex-encoded string.
    pub fn from_hex(s: &str) -> Result<Self, FibreError> {
        let bytes = hex::decode(s)
            .map_err(|e| FibreError::InvalidBlobId(format!("decoding hex: {}", e)))?;
        Self::from_bytes(&bytes)
    }
}

impl fmt::Display for BlobID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl fmt::Debug for BlobID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobID({})", hex::encode(self.0))
    }
}

/// Encoded blob with Reed-Solomon erasure coding.
///
/// A `Blob` can be created in two ways:
/// - `Blob::new()` encodes data into a new blob (for upload)
/// - [`FibreClient::download`](crate::FibreClient::download) reconstructs blobs from downloaded shards
pub struct Blob {
    cfg: BlobConfig,
    extended_data: Option<rsema1d::ExtendedData>,
    id: BlobID,
    rlc_coeffs: Option<Vec<rsema1d::GF128>>,
    header: BlobHeaderV0,
    /// The decoded original data (without header). `None` until encoded or reconstructed.
    data: Option<Vec<u8>>,
    /// Rows buffer for reconstruction (indexed by row number).
    /// `None` entries indicate missing rows.
    rows: Option<Vec<Option<Vec<u8>>>>,
}

impl Blob {
    /// Encode data into a new Blob.
    ///
    /// The data is prefixed with a header containing the blob version and data size,
    /// then split into rows and erasure-coded using rsema1d.
    ///
    /// Returns `FibreError::EmptyBlobData` if data is empty.
    /// Returns `FibreError::BlobTooLarge` if the data exceeds `cfg.max_data_size`.
    pub fn new(data: &[u8], cfg: BlobConfig) -> Result<Self, FibreError> {
        if data.is_empty() {
            return Err(FibreError::EmptyBlobData);
        }
        if data.len() > cfg.max_data_size {
            return Err(FibreError::BlobTooLarge {
                size: data.len(),
                max: cfg.max_data_size,
            });
        }

        let header = BlobHeaderV0::new(data.len());
        let row_size = cfg.row_size(data.len());

        // Allocate the full extended matrix (original + parity rows) up front.
        // Write header + data directly into the first K rows; parity rows stay
        // zeroed and will be filled by encode_in_place.
        let total_rows = cfg.original_rows + cfg.parity_rows;
        let mut flat = vec![0u8; total_rows * row_size];
        header.encode_into_buffer(data, &mut flat);
        let extended = rsema1d::RowMatrix::with_shape(flat, total_rows, row_size)?;
        let params = rsema1d::Parameters::new(cfg.original_rows, cfg.parity_rows, row_size)?;
        let (ext_data, commitment, rlc_orig) = rsema1d::encode_in_place(extended, &params)?;

        let id = BlobID::new(cfg.blob_version, commitment);

        Ok(Self {
            cfg,
            extended_data: Some(ext_data),
            id,
            rlc_coeffs: Some(rlc_orig),
            header,
            data: Some(data.to_vec()),
            rows: None,
        })
    }

    /// Create an empty blob for internal reconstruction from downloaded shards.
    ///
    /// Returns an error if the BlobID is invalid or the blob version is not supported.
    pub(crate) fn empty(id: BlobID) -> Result<Self, FibreError> {
        id.validate()?;
        let cfg = BlobConfig::for_version(id.version())?;
        let total_rows = cfg.total_rows();

        Ok(Self {
            cfg,
            extended_data: None,
            id,
            rlc_coeffs: None,
            header: BlobHeaderV0::default(),
            data: None,
            rows: Some(vec![None; total_rows]),
        })
    }

    /// Create an empty blob with a custom [`BlobConfig`] for reconstruction.
    ///
    /// This is like [`Blob::empty()`] but uses a caller-supplied config instead of
    /// deriving it from the blob version. Useful for tests that use small K/N parameters.
    #[cfg(test)]
    pub(crate) fn empty_with_config(id: BlobID, cfg: BlobConfig) -> Self {
        let total_rows = cfg.total_rows();
        Self {
            cfg,
            extended_data: None,
            id,
            rlc_coeffs: None,
            header: BlobHeaderV0::default(),
            data: None,
            rows: Some(vec![None; total_rows]),
        }
    }

    /// Returns the BlobID of this blob.
    pub fn id(&self) -> &BlobID {
        &self.id
    }

    /// Returns the blob's configuration.
    pub fn config(&self) -> &BlobConfig {
        &self.cfg
    }

    /// Returns the RLC coefficients of the original data, if available.
    pub fn rlc_coeffs(&self) -> Option<&[rsema1d::GF128]> {
        self.rlc_coeffs.as_deref()
    }

    /// Returns the size of each row in bytes, or `None` if no data is available.
    pub fn row_size(&self) -> Option<usize> {
        self.data.as_ref().map(|d| self.cfg.row_size(d.len()))
    }

    /// Returns the size of the original data (without header), or `None` if
    /// no data is available.
    pub fn data_size(&self) -> Option<usize> {
        self.data.as_ref().map(|d| d.len())
    }

    /// Returns the upload size (data with padding, without parity), or `None`
    /// if no data is available.
    ///
    /// This is the size included in the `PaymentPromise` and the one actually paid for.
    pub fn upload_size(&self) -> Option<usize> {
        self.data_size().map(|s| self.cfg.upload_size(s))
    }

    /// Returns the original data (without header), if available.
    ///
    /// Returns `None` if the data has not been decoded yet
    /// (call `reconstruct()` first for received blobs).
    pub fn data(&self) -> Option<&[u8]> {
        self.data.as_deref()
    }

    /// Generate a `RowInclusionProof` for the given row index from the extended data.
    pub fn row(&self, index: usize) -> Result<rsema1d::RowInclusionProof, FibreError> {
        let ext = self
            .extended_data
            .as_ref()
            .ok_or_else(|| FibreError::Other("no extended data available".into()))?;
        ext.generate_row_inclusion_proof(index)
            .map_err(|e| e.into())
    }

    /// Store rows that passed [`ShardVerifier::verify`].
    ///
    /// Rows whose index is already filled are skipped. Returns the number of
    /// genuinely new rows stored.
    pub(crate) fn store_rows(&mut self, rows: VerifiedRows) -> Result<usize, FibreError> {
        let mut applied = 0;
        for proof in rows.0 {
            if self.store_row(proof.index, proof.row.into_owned())? {
                applied += 1;
            }
        }
        Ok(applied)
    }

    /// Returns which row indices are already stored, for pre-filtering
    /// downloaded shards before verification.
    pub(crate) fn stored_rows_bitmap(&self) -> Vec<bool> {
        match &self.rows {
            Some(rows) => rows.iter().map(Option::is_some).collect(),
            None => Vec::new(),
        }
    }

    fn store_row(&mut self, index: usize, row: Vec<u8>) -> Result<bool, FibreError> {
        if let Some(ref mut rows) = self.rows {
            if index < rows.len() {
                if rows[index].is_some() {
                    return Ok(false);
                }
                rows[index] = Some(row);
            } else {
                return Err(FibreError::Other(format!(
                    "row index {} out of bounds (total rows: {})",
                    index,
                    rows.len()
                )));
            }
        } else {
            return Err(FibreError::Other(
                "blob not initialized for reconstruction".into(),
            ));
        }

        Ok(true)
    }

    /// Reconstruct the original data from accumulated rows.
    ///
    /// Requires at least `original_rows` (K) rows to have been set via `store_rows()`.
    /// After reconstruction, `data()` will return the original data.
    ///
    /// This method is NOT safe for concurrent calls.
    pub fn reconstruct(&mut self) -> Result<(), FibreError> {
        let rows_buffer = self
            .rows
            .as_ref()
            .ok_or_else(|| FibreError::Other("no rows available for reconstruction".into()))?;

        // Collect available row indices and data
        let mut indices = Vec::new();
        let mut row_size = 0usize;

        for (i, row_opt) in rows_buffer.iter().enumerate() {
            if let Some(row) = row_opt {
                if row_size == 0 {
                    row_size = row.len();
                }
                indices.push(i);
            }
        }

        if indices.len() < self.cfg.original_rows {
            return Err(FibreError::NotEnoughShards {
                got: indices.len(),
                need: self.cfg.original_rows,
            });
        }

        if row_size == 0 {
            return Err(FibreError::Other("no rows with data available".into()));
        }

        let params =
            rsema1d::Parameters::new(self.cfg.original_rows, self.cfg.parity_rows, row_size)?;

        // Take exactly K rows for reconstruction
        let k = self.cfg.original_rows;
        let selected_indices: Vec<usize> = indices[..k].to_vec();
        let selected_rows: Vec<&[u8]> = selected_indices
            .iter()
            .map(|&i| rows_buffer[i].as_ref().unwrap().as_slice())
            .collect();

        // Reconstruct original rows
        let reconstructed = rsema1d::reconstruct(&selected_rows, &selected_indices, &params)?;

        // Verify commitment by re-encoding
        let (ext_data, reconstructed_commitment, rlc_coeffs) =
            rsema1d::encode(&reconstructed, &params)?;

        if self.id.commitment() != reconstructed_commitment {
            return Err(FibreError::CommitmentMismatch {
                expected: hex::encode(self.id.commitment()),
                actual: hex::encode(reconstructed_commitment),
            });
        }

        // Decode header and extract original data from the first K rows
        let original_rows: Vec<&[u8]> = (0..k).map(|i| reconstructed.row(i).unwrap()).collect();
        let (header, original_data) = BlobHeaderV0::decode_from_rows(&original_rows, &self.cfg)?;

        self.header = header;
        self.data = Some(original_data);
        self.extended_data = Some(ext_data);
        self.rlc_coeffs = Some(rlc_coeffs);

        Ok(())
    }
}

/// Rows that passed shard verification, accepted by [`Blob::store_rows`].
///
/// Only [`ShardVerifier::verify`] can construct this, so unverified rows
/// cannot reach the blob's row buffer.
#[derive(Debug)]
pub(crate) struct VerifiedRows(Vec<rsema1d::RowProof<'static>>);

const ROWS_PER_YIELD: usize = 16;

/// Verifies downloaded shards against a blob's commitment.
///
/// Shared across download tasks so the expensive
/// [`rsema1d::VerificationContext`] (RS extension of the RLC vector plus a
/// Merkle build) is computed once per blob instead of once per validator
/// response.
pub(crate) struct ShardVerifier {
    commitment: Commitment,
    cfg: BlobConfig,
    /// RLC vector and context authenticated by a verified row. Later shards
    /// must carry the identical vector: once one row verifies,
    /// `sha256(row_root || rlc_root)` binds the vector to the commitment, so
    /// any other vector is invalid.
    cache: tokio::sync::Mutex<Option<(Vec<rsema1d::GF128>, Arc<rsema1d::VerificationContext>)>>,
}

impl ShardVerifier {
    pub(crate) fn new(blob: &Blob) -> Self {
        Self {
            commitment: blob.id.commitment(),
            cfg: blob.cfg.clone(),
            cache: tokio::sync::Mutex::new(None),
        }
    }

    /// Verify a downloaded shard, all-or-nothing: one bad row rejects the
    /// whole response.
    ///
    /// Rows already stored (per `already_stored`) or repeated within the
    /// response are skipped before any cryptography runs, bounding the work
    /// to the number of still-missing rows. Yields periodically so verification
    /// does not monopolize the executor.
    pub(crate) async fn verify(
        &self,
        rows: Vec<rsema1d::RowProof<'static>>,
        rlcs: &[rsema1d::GF128],
        already_stored: &[bool],
    ) -> Result<VerifiedRows, FibreError> {
        if rows.is_empty() {
            return Ok(VerifiedRows(Vec::new()));
        }

        let total_rows = self.cfg.total_rows();
        let mut seen = vec![false; total_rows];
        let mut needed = Vec::with_capacity(rows.len().min(total_rows));
        for proof in rows {
            let index = proof.index;
            if index >= total_rows {
                return Err(FibreError::InvalidData(format!(
                    "row index {index} out of bounds (total rows: {total_rows})"
                )));
            }
            if seen[index] || already_stored.get(index).copied().unwrap_or(false) {
                continue;
            }
            seen[index] = true;
            needed.push(proof);
        }

        if needed.is_empty() {
            return Ok(VerifiedRows(Vec::new()));
        }

        let row_size = needed[0].row.len();
        let max_row_size = self.cfg.row_size(self.cfg.max_data_size);
        if row_size == 0 || row_size > max_row_size {
            return Err(FibreError::InvalidData(format!(
                "row size {row_size} out of bounds (max {max_row_size})"
            )));
        }

        let (context, verified) = {
            let mut cache = self.cache.lock().await;
            match cache.as_ref() {
                Some((cached_rlcs, context)) => {
                    if cached_rlcs != rlcs {
                        return Err(FibreError::InvalidData(
                            "rlc vector does not match the verified one".into(),
                        ));
                    }
                    (Arc::clone(context), 0)
                }
                None => {
                    let params = rsema1d::Parameters::new(
                        self.cfg.original_rows,
                        self.cfg.parity_rows,
                        row_size,
                    )?;
                    let context = Arc::new(rsema1d::VerificationContext::new(rlcs, &params)?);
                    rsema1d::verify_row_with_context(&needed[0], &self.commitment, &context)?;
                    *cache = Some((rlcs.to_vec(), Arc::clone(&context)));
                    (context, 1)
                }
            }
        };

        let remaining = &needed[verified..];
        for (index, proof) in remaining.iter().enumerate() {
            rsema1d::verify_row_with_context(proof, &self.commitment, &context)?;
            if (index + 1).is_multiple_of(ROWS_PER_YIELD) && index + 1 < remaining.len() {
                lumina_utils::executor::yield_now().await;
            }
        }

        Ok(VerifiedRows(needed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blob_id_new_and_accessors() {
        let commitment = [42u8; 32];
        let id = BlobID::new(0, commitment);

        assert_eq!(id.version(), 0);
        assert_eq!(id.commitment(), commitment);
        assert_eq!(id.as_bytes()[0], 0);
        assert_eq!(&id.as_bytes()[1..], &commitment);
    }

    #[test]
    fn blob_id_validate() {
        let id = BlobID::new(0, [0u8; 32]);
        assert!(id.validate().is_ok());

        let id_v1 = BlobID::new(1, [0u8; 32]);
        assert!(id_v1.validate().is_err());
    }

    #[test]
    fn blob_id_from_bytes() {
        let commitment = [7u8; 32];
        let id = BlobID::new(0, commitment);

        let recovered = BlobID::from_bytes(id.as_bytes()).unwrap();
        assert_eq!(id, recovered);
    }

    #[test]
    fn blob_id_from_bytes_wrong_length() {
        assert!(BlobID::from_bytes(&[0u8; 10]).is_err());
        assert!(BlobID::from_bytes(&[0u8; 34]).is_err());
    }

    #[test]
    fn blob_id_hex_roundtrip() {
        let commitment = [0xAB; 32];
        let id = BlobID::new(0, commitment);
        let hex_str = id.to_string();

        let recovered = BlobID::from_hex(&hex_str).unwrap();
        assert_eq!(id, recovered);
    }

    #[test]
    fn blob_id_display() {
        let id = BlobID::new(0, [0u8; 32]);
        let s = format!("{}", id);
        assert_eq!(s.len(), BLOB_ID_SIZE * 2); // hex encoding doubles the length
        assert!(s.starts_with("00")); // version 0
    }

    #[test]
    fn blob_new_and_accessors() {
        // Use test parameters with small K/N and row_size=64 for fast tests
        let cfg = BlobConfig::new_test(0, 4, 4, 1024, 4, 64);
        let data = vec![1u8; 200];
        let blob = Blob::new(&data, cfg).unwrap();

        assert_eq!(blob.data_size(), Some(200));
        assert!(blob.data().is_some());
        assert_eq!(blob.data().unwrap(), &data);
        assert!(blob.row_size().unwrap() > 0);
        assert!(blob.upload_size().unwrap() > 0);
        assert!(blob.rlc_coeffs().is_some());
        assert!(blob.id().validate().is_ok());
    }

    #[test]
    fn blob_new_empty_data() {
        let cfg = BlobConfig::new_test(0, 4, 4, 1024, 4, 64);
        assert!(Blob::new(&[], cfg).is_err());
    }

    #[tokio::test]
    async fn blob_encode_reconstruct_roundtrip() {
        // Encode a blob with small test parameters
        let cfg = BlobConfig::new_test(0, 4, 4, 4096, 4, 64);
        let data: Vec<u8> = (0u8..=249).collect();
        let blob = Blob::new(&data, cfg.clone()).unwrap();

        let mut empty = Blob::empty_with_config(blob.id().clone(), cfg);

        // Set enough rows (need at least K=4)
        set_shard(&mut empty, shard_of(&blob, &[0, 1, 2, 3]))
            .await
            .unwrap();

        // Reconstruct
        empty.reconstruct().unwrap();
        assert_eq!(empty.data().unwrap(), &data);
    }

    #[test]
    fn blob_too_large() {
        let cfg = BlobConfig::new_test(0, 4, 4, 100, 4, 64);
        let data = vec![0u8; 101];
        match Blob::new(&data, cfg) {
            Err(FibreError::BlobTooLarge { size, max }) => {
                assert_eq!(size, 101);
                assert_eq!(max, 100);
            }
            other => panic!("expected BlobTooLarge error, got {:?}", other.err()),
        }
    }

    struct TestShard {
        rows: Vec<rsema1d::RowProof<'static>>,
        rlcs: Vec<rsema1d::GF128>,
    }

    fn shard_of(blob: &Blob, indices: &[usize]) -> TestShard {
        TestShard {
            rows: indices
                .iter()
                .map(|&i| {
                    let proof = blob.row(i).unwrap();
                    rsema1d::RowProof {
                        index: proof.index,
                        row: std::borrow::Cow::Owned(proof.row),
                        row_proof: proof.row_proof,
                    }
                })
                .collect(),
            rlcs: blob.rlc_coeffs().unwrap().to_vec(),
        }
    }

    async fn set_shard(blob: &mut Blob, shard: TestShard) -> Result<usize, FibreError> {
        let verifier = ShardVerifier::new(blob);
        let verified = verifier
            .verify(shard.rows, &shard.rlcs, &blob.stored_rows_bitmap())
            .await?;
        blob.store_rows(verified)
    }

    fn test_blob_pair() -> (Blob, Blob) {
        let cfg = BlobConfig::new_test(0, 4, 4, 4096, 4, 64);
        let data: Vec<u8> = (0u8..=249).collect();
        let blob = Blob::new(&data, cfg.clone()).unwrap();
        let empty = Blob::empty_with_config(blob.id().clone(), cfg);
        (blob, empty)
    }

    #[tokio::test]
    async fn set_shard_counts_only_new_rows() {
        let (blob, mut empty) = test_blob_pair();

        let unique = set_shard(&mut empty, shard_of(&blob, &[0, 1, 2]))
            .await
            .unwrap()
            + set_shard(&mut empty, shard_of(&blob, &[2, 3]))
                .await
                .unwrap();

        assert_eq!(unique, 4);
        empty.reconstruct().unwrap();
        assert_eq!(empty.data().unwrap(), blob.data().unwrap());
    }

    #[tokio::test]
    async fn set_shard_failing_shard_stores_nothing() {
        let (blob, mut empty) = test_blob_pair();

        // Tamper with the second row; the whole shard must be rejected.
        let mut shard = shard_of(&blob, &[0, 1]);
        shard.rows[1].row.to_mut()[0] ^= 1;
        assert!(set_shard(&mut empty, shard).await.is_err());

        // Row 0 was valid in the failing shard but must not have been stored.
        assert_eq!(
            set_shard(&mut empty, shard_of(&blob, &[0])).await.unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn set_shard_rejects_wrong_rlc_count() {
        let (blob, mut empty) = test_blob_pair();
        let mut shard = shard_of(&blob, &[0]);
        shard.rlcs.pop();

        assert!(set_shard(&mut empty, shard).await.is_err());
    }

    #[tokio::test]
    async fn set_shard_rejects_wrong_rlc_value() {
        let (blob, mut empty) = test_blob_pair();
        let mut shard = shard_of(&blob, &[0]);
        shard.rlcs[0].limbs[0] ^= 1;

        assert!(set_shard(&mut empty, shard).await.is_err());
    }

    #[tokio::test]
    async fn verify_rejects_oversized_row() {
        let (blob, empty) = test_blob_pair();
        let mut shard = shard_of(&blob, &[0]);
        // Larger than row_size(max_data_size) for the test config.
        shard.rows[0].row = std::borrow::Cow::Owned(vec![0u8; 2048]);

        let verifier = ShardVerifier::new(&empty);
        let err = verifier
            .verify(shard.rows, &shard.rlcs, &empty.stored_rows_bitmap())
            .await
            .unwrap_err();
        assert!(matches!(err, FibreError::InvalidData(_)), "got {err}");
    }

    #[tokio::test]
    async fn verify_rejects_out_of_range_index() {
        let (blob, empty) = test_blob_pair();
        let mut shard = shard_of(&blob, &[0]);
        shard.rows[0].index = empty.config().total_rows();

        let verifier = ShardVerifier::new(&empty);
        let err = verifier
            .verify(shard.rows, &shard.rlcs, &empty.stored_rows_bitmap())
            .await
            .unwrap_err();
        assert!(matches!(err, FibreError::InvalidData(_)), "got {err}");
    }

    #[tokio::test]
    async fn verify_skips_duplicate_indices_within_response() {
        let (blob, mut empty) = test_blob_pair();

        let applied = set_shard(&mut empty, shard_of(&blob, &[0, 0, 1]))
            .await
            .unwrap();
        assert_eq!(applied, 2);
    }

    #[tokio::test]
    async fn verify_skips_already_stored_rows_before_crypto() {
        let (blob, mut empty) = test_blob_pair();
        set_shard(&mut empty, shard_of(&blob, &[0, 1]))
            .await
            .unwrap();

        // Tampered copy of an already-stored row: it must be skipped by the
        // bitmap pre-filter, so verification never sees (and never rejects) it.
        let mut shard = shard_of(&blob, &[0]);
        shard.rows[0].row.to_mut()[0] ^= 1;

        let verifier = ShardVerifier::new(&empty);
        let verified = verifier
            .verify(shard.rows, &shard.rlcs, &empty.stored_rows_bitmap())
            .await
            .unwrap();
        assert_eq!(empty.store_rows(verified).unwrap(), 0);
    }

    #[tokio::test]
    async fn verify_uses_first_needed_row_size() {
        let (blob, mut empty) = test_blob_pair();
        set_shard(&mut empty, shard_of(&blob, &[0])).await.unwrap();

        let mut shard = shard_of(&blob, &[0, 1]);
        shard.rows[0].row = std::borrow::Cow::Owned(vec![0; 128]);

        let verifier = ShardVerifier::new(&empty);
        let verified = verifier
            .verify(shard.rows, &shard.rlcs, &empty.stored_rows_bitmap())
            .await
            .unwrap();

        assert_eq!(empty.store_rows(verified).unwrap(), 1);
    }

    #[tokio::test]
    async fn verify_caches_context_and_rejects_mismatched_rlcs() {
        let (blob, mut empty) = test_blob_pair();
        let verifier = ShardVerifier::new(&empty);

        let mut shard = shard_of(&blob, &[0]);
        shard.rows[0].row.to_mut()[0] ^= 1;
        assert!(
            verifier
                .verify(shard.rows, &shard.rlcs, &empty.stored_rows_bitmap())
                .await
                .is_err()
        );

        // First shard authenticates the RLC vector and fills the cache.
        let shard = shard_of(&blob, &[0, 1]);
        let verified = verifier
            .verify(shard.rows, &shard.rlcs, &empty.stored_rows_bitmap())
            .await
            .unwrap();
        empty.store_rows(verified).unwrap();

        // Same vector: verifies against the cached context.
        let shard = shard_of(&blob, &[2, 3]);
        let verified = verifier
            .verify(shard.rows, &shard.rlcs, &empty.stored_rows_bitmap())
            .await
            .unwrap();
        assert_eq!(empty.store_rows(verified).unwrap(), 2);

        // Different vector: rejected by the cheap cache comparison.
        let mut shard = shard_of(&blob, &[2, 3]);
        shard.rlcs[0].limbs[0] ^= 1;
        let fresh = Blob::empty_with_config(
            blob.id().clone(),
            BlobConfig::new_test(0, 4, 4, 4096, 4, 64),
        );
        let err = verifier
            .verify(shard.rows, &shard.rlcs, &fresh.stored_rows_bitmap())
            .await
            .unwrap_err();
        assert!(matches!(err, FibreError::InvalidData(_)), "got {err}");
        assert_eq!(fresh.stored_rows_bitmap().iter().filter(|s| **s).count(), 0);
    }
}
