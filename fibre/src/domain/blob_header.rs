//! Version 0 blob header encoding and decoding.
//!
//! The blob header is prepended to the original data before splitting into rows.

use crate::config::BlobConfig;
use crate::error::FibreError;

/// Length of the version field in bytes.
const BLOB_VERSION_LEN: usize = 1;
/// Length of the data size field in bytes.
const BLOB_DATA_SIZE_LEN: usize = 4;

/// Version 0 blob header placed at the start of the first row.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlobHeaderV0 {
    /// Size of the original data (excluding header).
    pub data_size: u32,
}

impl BlobHeaderV0 {
    /// Total header size in bytes.
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

    /// Encode the header and data into a flat buffer.
    ///
    /// The caller must ensure `buf` is at least `HEADER_SIZE + data.len()` bytes.
    pub fn encode_into_buffer(&self, data: &[u8], buf: &mut [u8]) {
        self.encode(buf);
        buf[Self::HEADER_SIZE..Self::HEADER_SIZE + data.len()].copy_from_slice(data);
    }

    /// Decode the header and extract original data from rows.
    pub fn decode_from_rows(
        rows: &[&[u8]],
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
        let header = Self::decode(rows[0])?;

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
                row
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
    fn encode_into_buffer_writes_header_and_data() {
        let data = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let header = BlobHeaderV0::new(data.len());
        let mut buf = vec![0u8; 64];
        header.encode_into_buffer(&data, &mut buf);

        // Header: version 0, data_size 10
        assert_eq!(buf[0], 0);
        assert_eq!(u32::from_be_bytes(buf[1..5].try_into().unwrap()), 10);
        // Data immediately after header
        assert_eq!(&buf[5..15], &data);
        // Rest is zero-padded
        assert!(buf[15..].iter().all(|&b| b == 0));
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
        let rows: &[&[u8]] = &[];
        assert!(BlobHeaderV0::decode_from_rows(rows, &cfg).is_err());
    }

    #[test]
    fn decode_zero_data_size() {
        let cfg = test_blob_config();
        let mut row = vec![0u8; 64];
        row[1..5].copy_from_slice(&0u32.to_be_bytes());
        assert!(BlobHeaderV0::decode_from_rows(&[row.as_slice()], &cfg).is_err());
    }
}
