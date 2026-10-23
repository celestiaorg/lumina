#![no_main]

use bytes::BytesMut;
use celestia_types::eds::EdsId;
use celestia_types::namespace_data::NamespaceDataId;
use celestia_types::row::RowId;
use celestia_types::row_namespace_data::RowNamespaceDataId;
use celestia_types::sample::SampleId;
use libfuzzer_sys::fuzz_target;

// Mirrors request decoding in `node/src/p2p/shrex/codec.rs`.
// Any id that decodes must encode back to the same bytes.
macro_rules! roundtrip {
    ($ty:ty, $data:expr) => {
        if let Ok(id) = <$ty>::decode($data) {
            let mut bytes = BytesMut::new();
            id.encode(&mut bytes);
            assert_eq!(&bytes[..], $data);
        }
    };
}

fuzz_target!(|data: &[u8]| {
    let Some((selector, data)) = data.split_first() else {
        return;
    };

    match selector % 5 {
        0 => roundtrip!(EdsId, data),
        1 => roundtrip!(RowId, data),
        2 => roundtrip!(SampleId, data),
        3 => roundtrip!(NamespaceDataId, data),
        _ => roundtrip!(RowNamespaceDataId, data),
    }
});
