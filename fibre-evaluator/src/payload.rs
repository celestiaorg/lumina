use celestia_fibre::{BlobConfig, DEFAULT_PROTOCOL_PARAMS};
use rand::RngCore;

const PAYLOAD_MAGIC: &[u8; 4] = b"FBE0";
const PAYLOAD_HEADER_LEN: usize = 16;

pub(crate) fn make_payload(sequence: u64, size: usize) -> Vec<u8> {
    let mut payload = vec![0u8; size];
    payload[..4].copy_from_slice(PAYLOAD_MAGIC);
    payload[4..12].copy_from_slice(&sequence.to_le_bytes());
    rand::thread_rng().fill_bytes(&mut payload[PAYLOAD_HEADER_LEN..]);
    let checksum = crc32fast::hash(&payload[PAYLOAD_HEADER_LEN..]);
    payload[12..16].copy_from_slice(&checksum.to_le_bytes());
    payload
}

pub(crate) fn verify_payload(
    data: &[u8],
    expected_sequence: u64,
    expected_size: usize,
) -> Result<(), String> {
    if data.len() != expected_size {
        return Err(format!(
            "payload size mismatch: expected {expected_size}, got {}",
            data.len()
        ));
    }
    if data.len() < PAYLOAD_HEADER_LEN {
        return Err(format!(
            "payload is shorter than {PAYLOAD_HEADER_LEN}-byte header"
        ));
    }
    if &data[..4] != PAYLOAD_MAGIC {
        return Err("payload magic mismatch".to_string());
    }

    let sequence = u64::from_le_bytes(data[4..12].try_into().expect("fixed-size slice"));
    if sequence != expected_sequence {
        return Err(format!(
            "payload sequence mismatch: expected {expected_sequence}, got {sequence}"
        ));
    }

    let expected_checksum = u32::from_le_bytes(data[12..16].try_into().expect("fixed-size slice"));
    let actual_checksum = crc32fast::hash(&data[PAYLOAD_HEADER_LEN..]);
    if actual_checksum != expected_checksum {
        return Err(format!(
            "payload CRC32 mismatch: expected {expected_checksum:#010x}, got {actual_checksum:#010x}"
        ));
    }

    Ok(())
}

pub(crate) fn payload_size_for_paid_size(paid_size: usize) -> Result<usize, String> {
    let cfg = BlobConfig::v0();
    let header_size = DEFAULT_PROTOCOL_PARAMS.max_blob_size - cfg.max_data_size;
    let min_paid_size = cfg.upload_size(PAYLOAD_HEADER_LEN);
    let max_paid_size = cfg.upload_size(cfg.max_data_size);

    if !(min_paid_size..=max_paid_size).contains(&paid_size) {
        return Err(format!(
            "must be between {min_paid_size} and {max_paid_size} bytes"
        ));
    }

    let payload_size = paid_size - header_size;
    if cfg.upload_size(payload_size) != paid_size {
        return Err(format!(
            "must be a multiple of {min_paid_size} bytes to represent an exact paid Fibre size"
        ));
    }
    Ok(payload_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_exact_paid_sizes_to_payload_sizes() {
        assert_eq!(payload_size_for_paid_size(262_144).unwrap(), 262_139);
        assert_eq!(
            payload_size_for_paid_size(134_217_728).unwrap(),
            134_217_723
        );
        assert!(payload_size_for_paid_size(262_143).is_err());
        assert!(payload_size_for_paid_size(262_145).is_err());
        assert!(payload_size_for_paid_size(134_217_729).is_err());
    }

    #[test]
    fn payload_roundtrip_verifies() {
        let payload = make_payload(42, 1024);
        verify_payload(&payload, 42, 1024).unwrap();
    }

    #[test]
    fn payload_corruption_is_detected() {
        let payload = make_payload(42, 1024);

        let mut bad_magic = payload.clone();
        bad_magic[0] ^= 1;
        assert!(verify_payload(&bad_magic, 42, 1024).is_err());

        assert!(verify_payload(&payload, 43, 1024).is_err());
        assert!(verify_payload(&payload, 42, 1023).is_err());

        let mut bad_checksum = payload.clone();
        bad_checksum[12] ^= 1;
        assert!(verify_payload(&bad_checksum, 42, 1024).is_err());

        let mut bad_body = payload;
        bad_body[PAYLOAD_HEADER_LEN] ^= 1;
        assert!(verify_payload(&bad_body, 42, 1024).is_err());
    }
}
