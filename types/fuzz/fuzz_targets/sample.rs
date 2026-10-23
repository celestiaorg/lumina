#![no_main]

use celestia_proto::shwap::Sample as RawSample;
use celestia_types::row::ROW_ID_SIZE;
use celestia_types::sample::{Sample, SampleId};
use celestia_types_fuzz::DAH;
use libfuzzer_sys::fuzz_target;
use prost::Message;

const SAMPLE_ID_SIZE: usize = ROW_ID_SIZE + 2;

// Mirrors `Sample::decode_and_verify` in `node/src/p2p/shrex/codec.rs`.
// Input: encoded `SampleId` followed by a length-delimited `RawSample`.
fuzz_target!(|data: &[u8]| {
    let Some((id, data)) = data.split_at_checked(SAMPLE_ID_SIZE) else {
        return;
    };
    let Ok(id) = SampleId::decode(id) else {
        return;
    };
    let Ok(raw) = RawSample::decode_length_delimited(data) else {
        return;
    };
    let Ok(sample) = Sample::from_raw(id, raw) else {
        return;
    };
    let _ = sample.verify(id, &DAH);
});
