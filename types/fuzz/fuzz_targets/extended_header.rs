#![no_main]

use celestia_types::ExtendedHeader;
use libfuzzer_sys::fuzz_target;

// Mirrors header-sub and header-ex in `node/src/p2p.rs` and `node/src/p2p/header_ex/utils.rs`.
fuzz_target!(|data: &[u8]| {
    let _ = ExtendedHeader::decode_and_validate(data);
});
