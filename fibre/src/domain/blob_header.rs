//! Version 0 blob header encoding and decoding.
//!
//! The blob header is prepended to the original data before splitting into rows.

use crate::error::{BlobHeaderError, FibreError};
use bytes::Bytes;

/// Length of the version field in bytes.
const BLOB_VERSION_LEN: usize = 1;
/// Length of the data size field in bytes.
const BLOB_DATA_SIZE_LEN: usize = 4;

const VERSION: u8 = 0;

/// Total header size in bytes.
pub(crate) const SIZE: usize = BLOB_VERSION_LEN + BLOB_DATA_SIZE_LEN;

/// Encode the header and data into a flat buffer.
///
/// The caller must ensure `buf` is at least `SIZE + data.len()` bytes long.
pub(crate) fn encode(data: &[u8], buf: &mut [u8]) {
    buf[0] = VERSION;
    buf[BLOB_VERSION_LEN..SIZE].copy_from_slice(&(data.len() as u32).to_be_bytes());
    buf[SIZE..SIZE + data.len()].copy_from_slice(data);
}

/// Decode the header and extract the original data from reconstructed rows.
pub(crate) fn decode(rows: rsema1d::RowMatrix, max_data_size: usize) -> Result<Bytes, FibreError> {
    if rows.rows() == 0 {
        return Err(BlobHeaderError::NoRows.into());
    }

    if rows.row_size() < SIZE {
        return Err(BlobHeaderError::FirstRowTooSmall(rows.row_size()).into());
    }

    let backing_len = rows.as_row_major().len();
    let data_size = {
        let buf = rows.as_row_major();
        if buf[0] != VERSION {
            return Err(FibreError::UnsupportedBlobVersion(buf[0]));
        }

        let data_size = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
        if data_size == 0 {
            return Err(BlobHeaderError::ZeroDataSize.into());
        }
        if data_size as usize > max_data_size {
            return Err(BlobHeaderError::DataSizeExceedsMax {
                size: data_size,
                max: max_data_size,
            }
            .into());
        }

        let data_size = data_size as usize;
        let payload = &buf[SIZE..];
        if payload.get(..data_size).is_none() {
            return Err(BlobHeaderError::DataSizeMismatch {
                copied: payload.len(),
                expected: data_size,
            }
            .into());
        }
        data_size
    };

    if data_size.saturating_mul(2) < backing_len {
        return Ok(Bytes::copy_from_slice(
            &rows.as_row_major()[SIZE..SIZE + data_size],
        ));
    }

    Ok(rows.into_bytes().slice(SIZE..SIZE + data_size))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(data: Vec<u8>, row_count: usize, row_size: usize) -> rsema1d::RowMatrix {
        rsema1d::RowMatrix::with_shape(data, row_count, row_size).unwrap()
    }

    #[test]
    fn header_size_is_five() {
        assert_eq!(SIZE, 5);
    }

    #[test]
    fn encode_decode() {
        let data = vec![1u8; 100];
        let mut buf = vec![0u8; 128];
        encode(&data, &mut buf);

        assert_eq!(buf[0], 0);
        assert_eq!(u32::from_be_bytes(buf[1..5].try_into().unwrap()), 100);
        assert_eq!(&buf[5..105], data);
        assert!(buf[105..].iter().all(|&b| b == 0));

        assert_eq!(decode(rows(buf, 2, 64), 1024).unwrap(), data);
    }

    #[test]
    fn decode_shares_large_payload_backing() {
        let data = vec![1u8; 100];
        let mut buf = vec![0u8; 128];
        encode(&data, &mut buf);
        let payload_ptr = buf[SIZE..].as_ptr();

        let decoded = decode(rows(buf, 2, 64), 1024).unwrap();

        assert_eq!(decoded.as_ptr(), payload_ptr);
        assert_eq!(decoded.as_ref(), data);
    }

    #[test]
    fn decode_copies_small_payload_out_of_large_backing() {
        let data = vec![1u8; 10];
        let mut buf = vec![0u8; 128];
        encode(&data, &mut buf);
        let payload_ptr = buf[SIZE..].as_ptr();

        let decoded = decode(rows(buf, 2, 64), 1024).unwrap();

        assert_ne!(decoded.as_ptr(), payload_ptr);
        assert_eq!(decoded.as_ref(), data);
    }

    #[test]
    fn decode_invalid_version() {
        let mut row = vec![0u8; 64];
        row[0] = 1;
        assert!(matches!(
            decode(rows(row, 1, 64), 1024),
            Err(FibreError::UnsupportedBlobVersion(1))
        ));
    }

    #[test]
    fn decode_empty_rows() {
        assert!(matches!(
            decode(rows(Vec::new(), 0, 64), 1024),
            Err(FibreError::InvalidBlobHeader(BlobHeaderError::NoRows))
        ));
    }

    #[test]
    fn decode_first_row_too_small() {
        assert!(matches!(
            decode(rows(vec![0u8; SIZE - 1], 1, SIZE - 1), 1024),
            Err(FibreError::InvalidBlobHeader(
                BlobHeaderError::FirstRowTooSmall(4)
            ))
        ));
    }

    #[test]
    fn decode_zero_data_size() {
        assert!(matches!(
            decode(rows(vec![0u8; 64], 1, 64), 1024),
            Err(FibreError::InvalidBlobHeader(BlobHeaderError::ZeroDataSize))
        ));
    }

    #[test]
    fn decode_data_size_exceeds_max() {
        let mut row = vec![0u8; 64];
        row[1..5].copy_from_slice(&1025u32.to_be_bytes());
        assert!(matches!(
            decode(rows(row, 1, 64), 1024),
            Err(FibreError::InvalidBlobHeader(
                BlobHeaderError::DataSizeExceedsMax {
                    size: 1025,
                    max: 1024
                }
            ))
        ));
    }

    #[test]
    fn decode_data_size_mismatch() {
        let mut row = vec![0u8; SIZE];
        row[1..5].copy_from_slice(&1u32.to_be_bytes());
        assert!(matches!(
            decode(rows(row, 1, SIZE), 1024),
            Err(FibreError::InvalidBlobHeader(
                BlobHeaderError::DataSizeMismatch {
                    copied: 0,
                    expected: 1
                }
            ))
        ));
    }
}
