//! Version 0 blob header encoding and decoding.
//!
//! The blob header is prepended to the original data before splitting into rows.
//! Format: 1 byte version (always 0) + 4 bytes data size (big-endian u32).

use crate::config::BlobConfig;
use crate::error::FibreError;

/// Total header size in bytes: 1 byte version + 4 bytes data size.
const BLOB_VERSION_LEN: usize = 1;
const BLOB_DATA_SIZE_LEN: usize = 4;

/// Version 0 blob header placed at the start of the first row.
///
/// Format: `[version: u8 = 0] [data_size: u32 big-endian]`
#[derive(Debug, Clone, Copy, Default)]
pub struct BlobHeaderV0 {
    /// Size of the original data (excluding header).
    pub data_size: u32,
}

impl BlobHeaderV0 {
    /// Total header size in bytes (1 byte version + 4 bytes data size).
    pub const HEADER_SIZE: usize = BLOB_VERSION_LEN + BLOB_DATA_SIZE_LEN;

    /// Create a new version 0 blob header with the given data size.
    pub fn new(data_size: usize) -> Self {
        Self {
            data_size: data_size as u32,
        }
    }

    /// Encode the header into the first bytes of the provided buffer.
    ///
    /// The buffer must be at least `HEADER_SIZE` bytes long.
    fn encode(&self, buf: &mut [u8]) {
        buf[0] = 0; // version 0
        buf[BLOB_VERSION_LEN..Self::HEADER_SIZE].copy_from_slice(&self.data_size.to_be_bytes());
    }

    /// Decode the header from the provided buffer.
    ///
    /// Returns an error if the version byte is not 0.
    fn decode(buf: &[u8]) -> Result<Self, FibreError> {
        if buf[0] != 0 {
            return Err(FibreError::UnsupportedBlobVersion(buf[0]));
        }
        let data_size =
            u32::from_be_bytes(buf[BLOB_VERSION_LEN..Self::HEADER_SIZE].try_into().unwrap());
        Ok(Self { data_size })
    }

    /// Encode the data into rows with the version 0 header prepended.
    ///
    /// Returns `num_rows` rows of `row_size` bytes each, padding the last row
    /// with zeros as needed. The first row begins with the 5-byte header followed
    /// by the start of the data.
    ///
    /// This matches the Go `blobHeaderV0.encodeToRows` method.
    #[cfg(test)]
    fn encode_to_rows(&self, data: &[u8], row_size: usize, num_rows: usize) -> Vec<Vec<u8>> {
        let mut rows: Vec<Vec<u8>> = Vec::with_capacity(num_rows);

        // First row: header + beginning of data
        let mut first_row = vec![0u8; row_size];
        self.encode(&mut first_row);

        let first_row_data_capacity = row_size - Self::HEADER_SIZE;
        let first_row_data_size = first_row_data_capacity.min(data.len());
        first_row[Self::HEADER_SIZE..Self::HEADER_SIZE + first_row_data_size]
            .copy_from_slice(&data[..first_row_data_size]);
        rows.push(first_row);

        // Remaining rows
        let mut data_offset = first_row_data_size;
        for _ in 1..num_rows {
            let start = data_offset;
            let end = start + row_size;
            data_offset += row_size;

            if end <= data.len() {
                // Full row available in data
                rows.push(data[start..end].to_vec());
            } else {
                // Partial or empty row: allocate zero-filled padded row
                let mut row = vec![0u8; row_size];
                if start < data.len() {
                    row[..data.len() - start].copy_from_slice(&data[start..]);
                }
                rows.push(row);
            }
        }

        rows
    }

    /// Encode the header and data directly into a flat row-major buffer.
    ///
    /// Writes the 5-byte header at the start of the buffer, followed by `data`,
    /// writing across row boundaries. The buffer must be at least
    /// `num_rows * row_size` bytes; any trailing bytes are left as-is (typically
    /// zero-initialised by the caller).
    ///
    /// This is the zero-copy counterpart of the test-only `encode_to_rows`.
    pub fn encode_into_buffer(&self, data: &[u8], buf: &mut [u8]) {
        self.encode(buf);
        let payload_start = Self::HEADER_SIZE;
        let to_copy = data.len().min(buf.len() - payload_start);
        buf[payload_start..payload_start + to_copy].copy_from_slice(&data[..to_copy]);
    }

    /// Decode the header and extract original data from rows.
    ///
    /// Reads the header from the first row, validates it, then concatenates data
    /// from all rows (skipping the header in the first row) up to `data_size` bytes.
    ///
    /// This matches the Go `blobHeaderV0.decodeFromRows` method.
    pub fn decode_from_rows(
        rows: &[Vec<u8>],
        cfg: &BlobConfig,
    ) -> Result<(Self, Vec<u8>), FibreError> {
        if rows.is_empty() {
            return Err(FibreError::Other("no rows to decode".into()));
        }

        if rows[0].len() < Self::HEADER_SIZE {
            return Err(FibreError::Other(format!(
                "first row too small: need at least {} bytes for header, got {}",
                Self::HEADER_SIZE,
                rows[0].len()
            )));
        }

        // Decode header from first row
        let header = Self::decode(&rows[0])?;

        // Validate blob size is within reasonable bounds
        if header.data_size == 0 {
            return Err(FibreError::Other(
                "invalid blob size in header: must be greater than 0".into(),
            ));
        }
        if header.data_size as usize > cfg.max_data_size {
            return Err(FibreError::Other(format!(
                "blob size in header ({} bytes) exceeds maximum allowed size ({} bytes)",
                header.data_size, cfg.max_data_size
            )));
        }

        let data_size = header.data_size as usize;

        // Pre-allocate data buffer (excluding header)
        let mut data = vec![0u8; data_size];
        let mut offset = 0;
        for (i, row) in rows.iter().enumerate() {
            if offset >= data_size {
                break;
            }
            // Skip header in first row
            let row_data = if i == 0 {
                &row[Self::HEADER_SIZE..]
            } else {
                row.as_slice()
            };

            let remaining = data_size - offset;
            let to_copy = remaining.min(row_data.len());
            data[offset..offset + to_copy].copy_from_slice(&row_data[..to_copy]);
            offset += to_copy;
        }

        if offset != data_size {
            return Err(FibreError::Other(format!(
                "data size mismatch: copied {} bytes, expected {}",
                offset, data_size
            )));
        }

        Ok((header, data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_blob_config() -> BlobConfig {
        BlobConfig::new_test(0, 4, 4, 1024, 4, 64)
    }

    #[test]
    fn header_size_is_five() {
        assert_eq!(BlobHeaderV0::HEADER_SIZE, 5);
    }

    #[test]
    fn encode_decode_header() {
        let header = BlobHeaderV0::new(256);
        let mut buf = [0u8; BlobHeaderV0::HEADER_SIZE];
        header.encode(&mut buf);

        assert_eq!(buf[0], 0); // version
        assert_eq!(u32::from_be_bytes(buf[1..5].try_into().unwrap()), 256);

        let decoded = BlobHeaderV0::decode(&buf).unwrap();
        assert_eq!(decoded.data_size, 256);
    }

    #[test]
    fn encode_decode_roundtrip_small_data() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let header = BlobHeaderV0::new(data.len());
        let cfg = test_blob_config();
        let row_size = 64;
        let num_rows = cfg.original_rows;

        let rows = header.encode_to_rows(&data, row_size, num_rows);

        assert_eq!(rows.len(), num_rows);
        for row in &rows {
            assert_eq!(row.len(), row_size);
        }

        // Verify header is at start of first row
        assert_eq!(rows[0][0], 0); // version
        assert_eq!(
            u32::from_be_bytes(rows[0][1..5].try_into().unwrap()),
            data.len() as u32
        );

        // Decode back
        let (decoded_header, decoded_data) =
            BlobHeaderV0::decode_from_rows(&rows[..num_rows], &cfg).unwrap();
        assert_eq!(decoded_header.data_size, data.len() as u32);
        assert_eq!(decoded_data, data);
    }

    #[test]
    fn encode_decode_roundtrip_large_data() {
        // Data that spans multiple rows
        let data: Vec<u8> = (0..200).map(|i| i as u8).collect();
        let header = BlobHeaderV0::new(data.len());
        let row_size = 64;
        let num_rows = 4;
        let cfg = test_blob_config();

        let rows = header.encode_to_rows(&data, row_size, num_rows);

        assert_eq!(rows.len(), num_rows);
        for row in &rows {
            assert_eq!(row.len(), row_size);
        }

        let (decoded_header, decoded_data) = BlobHeaderV0::decode_from_rows(&rows, &cfg).unwrap();
        assert_eq!(decoded_header.data_size, data.len() as u32);
        assert_eq!(decoded_data, data);
    }

    #[test]
    fn decode_invalid_version() {
        let mut buf = [0u8; BlobHeaderV0::HEADER_SIZE];
        buf[0] = 1; // invalid version
        assert!(BlobHeaderV0::decode(&buf).is_err());
    }

    #[test]
    fn decode_empty_rows() {
        let cfg = test_blob_config();
        let result = BlobHeaderV0::decode_from_rows(&[], &cfg);
        assert!(result.is_err());
    }

    #[test]
    fn decode_zero_data_size() {
        let cfg = test_blob_config();
        let mut row = vec![0u8; 64];
        // version 0, data_size 0
        row[0] = 0;
        row[1..5].copy_from_slice(&0u32.to_be_bytes());
        let result = BlobHeaderV0::decode_from_rows(&[row], &cfg);
        assert!(result.is_err());
    }
}
