#![no_main]

use celestia_proto::shwap::Row as RawRow;
use celestia_types::row::{ROW_ID_SIZE, RowId};
use celestia_types_fuzz::square;
use libfuzzer_sys::fuzz_target;
use prost::Message;

// Mirrors `Row::decode_and_verify` in `node/src/p2p/shrex/codec.rs`.
// Input: square selector, encoded `RowId`, length-delimited `RawRow`.
fuzz_target!(|data: &[u8]| {
    let Some((&selector, data)) = data.split_first() else {
        return;
    };
    let Some((id, data)) = data.split_at_checked(ROW_ID_SIZE) else {
        return;
    };
    let Ok(id) = RowId::decode(id) else {
        return;
    };
    let Ok(raw) = RawRow::decode_length_delimited(data) else {
        return;
    };
    square(selector).check_row(id, raw);
});
