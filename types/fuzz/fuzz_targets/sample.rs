#![no_main]

use celestia_proto::shwap::Sample as RawSample;
use celestia_types::row::ROW_ID_SIZE;
use celestia_types::sample::SampleId;
use celestia_types_fuzz::square;
use libfuzzer_sys::fuzz_target;
use prost::Message;

const SAMPLE_ID_SIZE: usize = ROW_ID_SIZE + 2;

// Mirrors `Sample::decode_and_verify` in `node/src/p2p/shrex/codec.rs`.
// Input: square selector, encoded `SampleId`, length-delimited `RawSample`.
fuzz_target!(|data: &[u8]| {
    let Some((&selector, data)) = data.split_first() else {
        return;
    };
    let Some((id, data)) = data.split_at_checked(SAMPLE_ID_SIZE) else {
        return;
    };
    let Ok(id) = SampleId::decode(id) else {
        return;
    };
    let Ok(raw) = RawSample::decode_length_delimited(data) else {
        return;
    };
    square(selector).check_sample(id, raw);
});
