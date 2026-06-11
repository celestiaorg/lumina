# Celestia types

Core types, traits and constants you may encounter when working with the Celestia ecosystem.

Most of the types are built on top of the [`celestia-tendermint-rs`](https://github.com/eigerco/celestia-tendermint-rs)
and [`celestia-proto`](https://github.com/eigerco/lumina/proto) and support the serialization and deserialization using
protobuf and serde to the format understood by nodes in celestia network.

```rust
use celestia_types::{Blob, nmt::Namespace};

let my_namespace = Namespace::new_v0(&[1, 2, 3, 4, 5]).expect("Invalid namespace");
let blob = Blob::new(
    my_namespace,
    b"some data to store on blockchain".to_vec(),
    None,
)
    .expect("Failed to create a blob");

assert_eq!(
    &serde_json::to_string_pretty(&blob).unwrap(),
    indoc::indoc! {r#"{
      "namespace": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQIDBAU=",
      "data": "c29tZSBkYXRhIHRvIHN0b3JlIG9uIGJsb2NrY2hhaW4=",
      "share_version": 0,
      "commitment": "m0A4feU6Fqd5Zy9td3M7lntG8A3PKqe6YdugmAsWz28=",
      "index": -1,
      "signer": null
    }"#},
);
```
