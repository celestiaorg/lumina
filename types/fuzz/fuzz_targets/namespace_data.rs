#![no_main]

use celestia_proto::shwap::RowNamespaceData as RawRowNamespaceData;
use celestia_types::namespace_data::{NAMESPACE_DATA_ID_SIZE, NamespaceData, NamespaceDataId};
use celestia_types_fuzz::DAH;
use libfuzzer_sys::fuzz_target;
use prost::Message;

// Mirrors `NamespaceData::decode_and_verify` in `node/src/p2p/shrex/codec.rs`.
// Input: encoded `NamespaceDataId` followed by length-delimited `RawRowNamespaceData`s.
fuzz_target!(|data: &[u8]| {
    let Some((id, mut data)) = data.split_at_checked(NAMESPACE_DATA_ID_SIZE) else {
        return;
    };
    let Ok(id) = NamespaceDataId::decode(id) else {
        return;
    };

    let mut raw_rows = Vec::new();
    while !data.is_empty() {
        let Ok(raw_row) = RawRowNamespaceData::decode_length_delimited(&mut data) else {
            return;
        };
        raw_rows.push(raw_row);
    }

    let Ok(ns_data) = NamespaceData::from_raw(id, raw_rows) else {
        return;
    };
    let _ = ns_data.verify(id, &DAH);
});
