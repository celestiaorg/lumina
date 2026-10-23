#![no_main]

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use celestia_types::nmt::Namespace;
use libfuzzer_sys::fuzz_target;

// Namespaces of any length, as found in share proofs (`Namespace::new`) and in JSON
// from a bridge node (base64 decoded into `Namespace::from_raw`).
fuzz_target!(|data: &[u8]| {
    if let Some((&version, id)) = data.split_first()
        && let Ok(ns) = Namespace::new(version, id)
    {
        assert_eq!(ns.version(), version);
        assert_eq!(Namespace::from_raw(ns.as_bytes()).unwrap(), ns);
    }

    let json = serde_json::to_string(&BASE64_STANDARD.encode(data)).unwrap();
    if let Ok(ns) = serde_json::from_str::<Namespace>(&json) {
        assert_eq!(ns.as_bytes(), data);
        assert_eq!(serde_json::to_string(&ns).unwrap(), json);
    }
});
