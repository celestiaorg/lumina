#![no_main]

use celestia_proto::celestia::core::v1::proof::ShareProof as RawShareProof;
use celestia_types_fuzz::square;
use libfuzzer_sys::fuzz_target;
use prost::Message;

// Mirrors `ShareProof` received from a bridge node, verified against the data root.
// Input: square selector, encoded `RawShareProof`.
fuzz_target!(|data: &[u8]| {
    let Some((&selector, data)) = data.split_first() else {
        return;
    };
    let Ok(raw) = RawShareProof::decode(data) else {
        return;
    };
    square(selector).check_share_proof(raw);
});
