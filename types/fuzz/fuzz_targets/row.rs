#![no_main]

use celestia_proto::shwap::Row as RawRow;
use celestia_types::row::{ROW_ID_SIZE, Row, RowId};
use celestia_types_fuzz::DAH;
use libfuzzer_sys::fuzz_target;
use prost::Message;

// Mirrors `Row::decode_and_verify` in `node/src/p2p/shrex/codec.rs`.
// Input: encoded `RowId` followed by a length-delimited `RawRow`.
fuzz_target!(|data: &[u8]| {
    let Some((id, data)) = data.split_at_checked(ROW_ID_SIZE) else {
        return;
    };
    let Ok(id) = RowId::decode(id) else {
        return;
    };
    let Ok(raw) = RawRow::decode_length_delimited(data) else {
        return;
    };
    let Ok(row) = Row::from_raw(id, raw) else {
        return;
    };
    let _ = row.verify(id, &DAH);
});
