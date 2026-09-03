//! Blob encoding/decoding, BlobID, and Commitment types.
//!
//! This module implements the core blob data structures for the Fibre protocol:
//! - `Commitment`: a 32-byte SHA-256 commitment hash
//! - `BlobID`: version byte + commitment that uniquely identifies a blob
//! - `EncodedBlob`: encoded data ready for upload
//! - `Blob`: decoded data returned by download

use std::fmt;
use std::num::NonZeroUsize;
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

/// Decoded blob returned by [`FibreClient::download`](crate::FibreClient::download).
#[derive(Debug)]
pub struct Blob {
    id: BlobID,
    data: Vec<u8>,
}

impl Blob {
    /// Returns the BlobID of this blob.
    pub fn id(&self) -> &BlobID {
        &self.id
    }

    /// Returns the original data without the header.
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

/// Encoded blob ready for upload.
pub struct EncodedBlob {
    cfg: BlobConfig,
    extended_data: rsema1d::ExtendedData,
    id: BlobID,
    data_size: usize,
}

impl EncodedBlob {
    /// Encode data into a new upload-ready blob.
    ///
    /// The data is prefixed with a header containing the blob version and data size,
    /// then split into rows and erasure-coded using rsema1d.
    ///
    /// Returns `FibreError::EmptyBlobData` if data is empty.
    /// Returns `FibreError::BlobTooLarge` if the data exceeds `cfg.max_data_size`.
    pub fn new(data: &[u8], cfg: BlobConfig) -> Result<Self, FibreError> {
        Self::new_with_work_budget(data, cfg, rsema1d::default_work_budget())
    }

    /// Encode data with an explicit combined Reed-Solomon work-buffer budget.
    pub fn new_with_work_budget(
        data: &[u8],
        cfg: BlobConfig,
        work_budget: NonZeroUsize,
    ) -> Result<Self, FibreError> {
        Self::validate_data_size(data.len(), &cfg)?;
        Self::new_owned_unchecked(data.to_vec(), cfg, work_budget)
    }

    /// Encode owned data into a new upload-ready blob.
    ///
    /// This is equivalent to [`EncodedBlob::new`] but avoids copying the provided
    /// allocation before encoding it.
    ///
    /// Returns `FibreError::EmptyBlobData` if data is empty.
    /// Returns `FibreError::BlobTooLarge` if the data exceeds `cfg.max_data_size`.
    pub fn new_owned(data: Vec<u8>, cfg: BlobConfig) -> Result<Self, FibreError> {
        Self::validate_data_size(data.len(), &cfg)?;
        Self::new_owned_unchecked(data, cfg, rsema1d::default_work_budget())
    }

    fn validate_data_size(data_size: usize, cfg: &BlobConfig) -> Result<(), FibreError> {
        if data_size == 0 {
            return Err(FibreError::EmptyBlobData);
        }
        if data_size > cfg.max_data_size {
            return Err(FibreError::BlobTooLarge {
                size: data_size,
                max: cfg.max_data_size,
            });
        }

        Ok(())
    }

    fn new_owned_unchecked(
        data: Vec<u8>,
        cfg: BlobConfig,
        work_budget: NonZeroUsize,
    ) -> Result<Self, FibreError> {
        let header = BlobHeaderV0::new(data.len());
        let row_size = cfg.row_size(data.len());

        // Allocate the full extended matrix (original + parity rows) up front.
        // Write header + data directly into the first K rows; parity rows stay
        // zeroed and will be filled by encode_in_place.
        let total_rows = cfg.original_rows + cfg.parity_rows;
        let mut flat = vec![0u8; total_rows * row_size];
        header.encode_into_buffer(&data, &mut flat);
        let extended = rsema1d::RowMatrix::with_shape(flat, total_rows, row_size)?;
        let params = rsema1d::Parameters::new(cfg.original_rows, cfg.parity_rows, row_size)?;
        let (extended_data, commitment, _) =
            rsema1d::encode_in_place_with_work_budget(extended, &params, work_budget)?;

        let id = BlobID::new(cfg.blob_version, commitment);

        Ok(Self {
            cfg,
            extended_data,
            id,
            data_size: data.len(),
        })
    }

    /// Returns the BlobID of this blob.
    pub fn id(&self) -> &BlobID {
        &self.id
    }

    /// Returns the blob's configuration.
    pub fn config(&self) -> &BlobConfig {
        &self.cfg
    }

    /// Returns the RLC coefficients of the original data.
    pub fn rlc_coeffs(&self) -> &[rsema1d::GF128] {
        self.extended_data.rlc_original()
    }

    /// Returns the size of each row in bytes.
    pub fn row_size(&self) -> usize {
        self.extended_data.rows().row_size()
    }

    /// Returns the size of the original data without the header.
    pub fn data_size(&self) -> usize {
        self.data_size
    }

    /// Returns the upload size: data with padding and without parity.
    pub fn upload_size(&self) -> usize {
        self.row_size() * self.cfg.original_rows
    }

    /// Generate a `RowInclusionProof` for the given row index from the extended data.
    pub fn row(&self, index: usize) -> Result<rsema1d::RowInclusionProof, FibreError> {
        self.extended_data
            .generate_row_inclusion_proof(index)
            .map_err(|e| e.into())
    }
}

/// Incomplete blob used while collecting rows for reconstruction.
pub(crate) struct BlobReconstruction {
    cfg: BlobConfig,
    id: BlobID,
    rows: Vec<Option<Vec<u8>>>,
}

impl BlobReconstruction {
    /// Create a reconstruction buffer for the blob ID.
    pub(crate) fn new(id: BlobID) -> Result<Self, FibreError> {
        let cfg = BlobConfig::for_version(id.version())?;
        let rows = vec![None; cfg.total_rows()];

        Ok(Self { cfg, id, rows })
    }

    /// Create a reconstruction buffer with a custom configuration for tests.
    #[cfg(test)]
    pub(crate) fn with_config(id: BlobID, cfg: BlobConfig) -> Self {
        let rows = vec![None; cfg.total_rows()];
        Self { cfg, id, rows }
    }

    pub(crate) fn id(&self) -> &BlobID {
        &self.id
    }

    pub(crate) fn config(&self) -> &BlobConfig {
        &self.cfg
    }

    /// Store rows that passed [`ShardVerifier::verify`].
    ///
    /// Rows whose index is already filled are skipped. Returns the number of
    /// genuinely new rows stored.
    ///
    /// Row indexes are not re-checked here: [`ShardVerifier::new`] copies this
    /// reconstruction's config, so `verify` already bounds them by `total_rows`.
    pub(crate) fn store_rows(&mut self, rows: VerifiedRows) -> usize {
        let mut applied = 0;
        for proof in rows.0 {
            let row = &mut self.rows[proof.index];
            if row.is_none() {
                *row = Some(proof.row.into_owned());
                applied += 1;
            }
        }
        applied
    }

    /// Returns which row indices are already stored, for pre-filtering
    /// downloaded shards before verification.
    pub(crate) fn stored_rows_bitmap(&self) -> Vec<bool> {
        self.rows.iter().map(Option::is_some).collect()
    }

    /// Reconstruct the original data from accumulated rows.
    ///
    /// Requires at least `original_rows` (K) rows to have been set via `store_rows()`.
    pub(crate) fn reconstruct(self) -> Result<Blob, FibreError> {
        let mut indices = Vec::new();
        for (i, row_opt) in self.rows.iter().enumerate() {
            if row_opt.is_some() {
                indices.push(i);
            }
        }

        if indices.len() < self.cfg.original_rows {
            return Err(FibreError::NotEnoughShards {
                got: indices.len(),
                need: self.cfg.original_rows,
            });
        }

        let k = self.cfg.original_rows;
        let selected_indices: Vec<usize> = indices[..k].to_vec();
        let selected_rows: Vec<&[u8]> = selected_indices
            .iter()
            .map(|&i| self.rows[i].as_ref().unwrap().as_slice())
            .collect();
        let params = rsema1d::Parameters::new(
            self.cfg.original_rows,
            self.cfg.parity_rows,
            selected_rows[0].len(),
        )?;

        let reconstructed = rsema1d::reconstruct(&selected_rows, &selected_indices, &params)?;

        let original_rows: Vec<&[u8]> = (0..k).map(|i| reconstructed.row(i).unwrap()).collect();
        let (_, data) = BlobHeaderV0::decode_from_rows(&original_rows, &self.cfg)?;

        Ok(Blob { id: self.id, data })
    }
}

/// Rows that passed shard verification, accepted by [`BlobReconstruction::store_rows`].
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
/// Merkle build and row-derived coefficients) is computed once per blob
/// instead of once per validator response.
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
    pub(crate) fn new(blob: &BlobReconstruction) -> Self {
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
    fn encoded_blob_new_and_accessors() {
        // Use test parameters with small K/N and row_size=64 for fast tests
        let cfg = BlobConfig::new_test(0, 4, 4, 1024, 4, 64);
        let data = vec![1u8; 200];
        let blob = EncodedBlob::new(&data, cfg.clone()).unwrap();

        assert_eq!(blob.data_size(), 200);
        assert!(blob.row_size() > 0);
        assert!(blob.upload_size() > 0);
        assert_eq!(blob.rlc_coeffs().len(), cfg.original_rows);
        assert!(blob.id().validate().is_ok());
    }

    #[test]
    fn encoded_blob_new_owned_matches_borrowed_constructor() {
        let cfg = BlobConfig::new_test(0, 4, 4, 1024, 4, 64);
        let data = vec![1u8; 200];
        let borrowed = EncodedBlob::new(&data, cfg.clone()).unwrap();
        let owned = EncodedBlob::new_owned(data, cfg).unwrap();

        assert_eq!(owned.id(), borrowed.id());
        assert_eq!(owned.data_size(), borrowed.data_size());
        assert_eq!(owned.row_size(), borrowed.row_size());
        assert_eq!(owned.upload_size(), borrowed.upload_size());
        assert_eq!(owned.rlc_coeffs(), borrowed.rlc_coeffs());
    }

    #[test]
    fn encoded_blob_new_empty_data() {
        let cfg = BlobConfig::new_test(0, 4, 4, 1024, 4, 64);
        assert!(EncodedBlob::new(&[], cfg).is_err());
    }

    #[tokio::test]
    async fn blob_encode_reconstruct_roundtrip() {
        // Encode a blob with small test parameters
        let cfg = BlobConfig::new_test(0, 4, 4, 4096, 4, 64);
        let data: Vec<u8> = (0u8..=249).collect();
        let blob = EncodedBlob::new(&data, cfg.clone()).unwrap();

        let mut reconstruction = BlobReconstruction::with_config(blob.id().clone(), cfg);

        // Set enough rows (need at least K=4)
        set_shard(&mut reconstruction, shard_of(&blob, &[0, 1, 2, 3]))
            .await
            .unwrap();

        let reconstructed = reconstruction.reconstruct().unwrap();
        assert_eq!(reconstructed.data(), &data);
    }

    #[test]
    fn reconstruction_rejects_insufficient_rows() {
        let cfg = BlobConfig::new_test(0, 4, 4, 4096, 4, 64);
        let reconstruction = BlobReconstruction::with_config(BlobID::new(0, [0; 32]), cfg);

        let err = reconstruction.reconstruct().unwrap_err();
        assert!(matches!(
            err,
            FibreError::NotEnoughShards { got: 0, need: 4 }
        ));
    }

    #[test]
    fn blob_too_large() {
        let cfg = BlobConfig::new_test(0, 4, 4, 100, 4, 64);
        let data = vec![0u8; 101];
        match EncodedBlob::new(&data, cfg) {
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

    fn shard_of(blob: &EncodedBlob, indices: &[usize]) -> TestShard {
        TestShard {
            rows: indices
                .iter()
                .map(|&i| {
                    let proof = blob.row(i).unwrap();
                    rsema1d::RowProof {
                        index: proof.index,
                        row: std::borrow::Cow::Owned(proof.row.to_vec()),
                        row_proof: proof.row_proof,
                    }
                })
                .collect(),
            rlcs: blob.rlc_coeffs().to_vec(),
        }
    }

    async fn set_shard(
        blob: &mut BlobReconstruction,
        shard: TestShard,
    ) -> Result<usize, FibreError> {
        let verifier = ShardVerifier::new(blob);
        let verified = verifier
            .verify(shard.rows, &shard.rlcs, &blob.stored_rows_bitmap())
            .await?;
        Ok(blob.store_rows(verified))
    }

    fn test_data() -> Vec<u8> {
        (0u8..=249).collect()
    }

    fn test_blob_and_reconstruction() -> (EncodedBlob, BlobReconstruction) {
        let cfg = BlobConfig::new_test(0, 4, 4, 4096, 4, 64);
        let blob = EncodedBlob::new(&test_data(), cfg.clone()).unwrap();
        let reconstruction = BlobReconstruction::with_config(blob.id().clone(), cfg);
        (blob, reconstruction)
    }

    #[tokio::test]
    async fn set_shard_counts_only_new_rows() {
        let (blob, mut reconstruction) = test_blob_and_reconstruction();

        let unique = set_shard(&mut reconstruction, shard_of(&blob, &[0, 1, 2]))
            .await
            .unwrap()
            + set_shard(&mut reconstruction, shard_of(&blob, &[2, 3]))
                .await
                .unwrap();

        assert_eq!(unique, 4);
        let reconstructed = reconstruction.reconstruct().unwrap();
        assert_eq!(reconstructed.data(), &test_data());
    }

    #[tokio::test]
    async fn set_shard_failing_shard_stores_nothing() {
        let (blob, mut reconstruction) = test_blob_and_reconstruction();

        // Tamper with the second row; the whole shard must be rejected.
        let mut shard = shard_of(&blob, &[0, 1]);
        shard.rows[1].row.to_mut()[0] ^= 1;
        assert!(set_shard(&mut reconstruction, shard).await.is_err());

        // Row 0 was valid in the failing shard but must not have been stored.
        assert_eq!(
            set_shard(&mut reconstruction, shard_of(&blob, &[0]))
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn set_shard_rejects_wrong_rlc_count() {
        let (blob, mut reconstruction) = test_blob_and_reconstruction();
        let mut shard = shard_of(&blob, &[0]);
        shard.rlcs.pop();

        assert!(set_shard(&mut reconstruction, shard).await.is_err());
    }

    #[tokio::test]
    async fn set_shard_rejects_wrong_rlc_value() {
        let (blob, mut reconstruction) = test_blob_and_reconstruction();
        let mut shard = shard_of(&blob, &[0]);
        shard.rlcs[0].limbs[0] ^= 1;

        assert!(set_shard(&mut reconstruction, shard).await.is_err());
    }

    #[tokio::test]
    async fn verify_rejects_oversized_row() {
        let (blob, reconstruction) = test_blob_and_reconstruction();
        let mut shard = shard_of(&blob, &[0]);
        // Larger than row_size(max_data_size) for the test config.
        shard.rows[0].row = std::borrow::Cow::Owned(vec![0u8; 2048]);

        let verifier = ShardVerifier::new(&reconstruction);
        let err = verifier
            .verify(
                shard.rows,
                &shard.rlcs,
                &reconstruction.stored_rows_bitmap(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, FibreError::InvalidData(_)), "got {err}");
    }

    #[tokio::test]
    async fn verify_rejects_out_of_range_index() {
        let (blob, reconstruction) = test_blob_and_reconstruction();
        let mut shard = shard_of(&blob, &[0]);
        shard.rows[0].index = reconstruction.config().total_rows();

        let verifier = ShardVerifier::new(&reconstruction);
        let err = verifier
            .verify(
                shard.rows,
                &shard.rlcs,
                &reconstruction.stored_rows_bitmap(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, FibreError::InvalidData(_)), "got {err}");
    }

    #[tokio::test]
    async fn verify_skips_duplicate_indices_within_response() {
        let (blob, mut reconstruction) = test_blob_and_reconstruction();

        let applied = set_shard(&mut reconstruction, shard_of(&blob, &[0, 0, 1]))
            .await
            .unwrap();
        assert_eq!(applied, 2);
    }

    #[tokio::test]
    async fn verify_skips_already_stored_rows_before_crypto() {
        let (blob, mut reconstruction) = test_blob_and_reconstruction();
        set_shard(&mut reconstruction, shard_of(&blob, &[0, 1]))
            .await
            .unwrap();

        // Tampered copy of an already-stored row: it must be skipped by the
        // bitmap pre-filter, so verification never sees (and never rejects) it.
        let mut shard = shard_of(&blob, &[0]);
        shard.rows[0].row.to_mut()[0] ^= 1;

        let verifier = ShardVerifier::new(&reconstruction);
        let verified = verifier
            .verify(
                shard.rows,
                &shard.rlcs,
                &reconstruction.stored_rows_bitmap(),
            )
            .await
            .unwrap();
        assert_eq!(reconstruction.store_rows(verified), 0);
    }

    #[tokio::test]
    async fn verify_uses_first_needed_row_size() {
        let (blob, mut reconstruction) = test_blob_and_reconstruction();
        set_shard(&mut reconstruction, shard_of(&blob, &[0]))
            .await
            .unwrap();

        let mut shard = shard_of(&blob, &[0, 1]);
        shard.rows[0].row = std::borrow::Cow::Owned(vec![0; 128]);

        let verifier = ShardVerifier::new(&reconstruction);
        let verified = verifier
            .verify(
                shard.rows,
                &shard.rlcs,
                &reconstruction.stored_rows_bitmap(),
            )
            .await
            .unwrap();

        assert_eq!(reconstruction.store_rows(verified), 1);
    }

    #[tokio::test]
    async fn verify_caches_context_and_rejects_mismatched_rlcs() {
        let (blob, mut reconstruction) = test_blob_and_reconstruction();
        let verifier = ShardVerifier::new(&reconstruction);

        let mut shard = shard_of(&blob, &[0]);
        shard.rows[0].row.to_mut()[0] ^= 1;
        assert!(
            verifier
                .verify(
                    shard.rows,
                    &shard.rlcs,
                    &reconstruction.stored_rows_bitmap(),
                )
                .await
                .is_err()
        );

        // First shard authenticates the RLC vector and fills the cache.
        let shard = shard_of(&blob, &[0, 1]);
        let verified = verifier
            .verify(
                shard.rows,
                &shard.rlcs,
                &reconstruction.stored_rows_bitmap(),
            )
            .await
            .unwrap();
        reconstruction.store_rows(verified);

        // Same vector: verifies against the cached context.
        let shard = shard_of(&blob, &[2, 3]);
        let verified = verifier
            .verify(
                shard.rows,
                &shard.rlcs,
                &reconstruction.stored_rows_bitmap(),
            )
            .await
            .unwrap();
        assert_eq!(reconstruction.store_rows(verified), 2);

        // Different vector: rejected by the cheap cache comparison.
        let mut shard = shard_of(&blob, &[2, 3]);
        shard.rlcs[0].limbs[0] ^= 1;
        let fresh_reconstruction = BlobReconstruction::with_config(
            blob.id().clone(),
            BlobConfig::new_test(0, 4, 4, 4096, 4, 64),
        );
        let err = verifier
            .verify(
                shard.rows,
                &shard.rlcs,
                &fresh_reconstruction.stored_rows_bitmap(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, FibreError::InvalidData(_)), "got {err}");
        assert_eq!(
            fresh_reconstruction
                .stored_rows_bitmap()
                .iter()
                .filter(|s| **s)
                .count(),
            0
        );
    }
}
