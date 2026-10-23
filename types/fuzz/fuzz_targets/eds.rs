#![no_main]

use celestia_types::consts::appconsts::SHARE_SIZE;
use celestia_types::{DataAvailabilityHeader, ExtendedDataSquare};
use celestia_types_fuzz::DAH;
use libfuzzer_sys::fuzz_target;

// Mirrors `ExtendedDataSquare::decode_and_verify` in `node/src/p2p/shrex/codec.rs`.
// Input: concatenated ODS shares.
fuzz_target!(|data: &[u8]| {
    if data.is_empty() || !data.len().is_multiple_of(SHARE_SIZE) {
        return;
    }

    let ods_shares = data.chunks(SHARE_SIZE).map(<[u8]>::to_vec).collect();
    let Ok(eds) = ExtendedDataSquare::from_ods(ods_shares) else {
        return;
    };
    let _ = DataAvailabilityHeader::from_eds(&eds) == *DAH;
});
