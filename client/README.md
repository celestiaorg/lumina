A high-level client for interacting with a Celestia node.

It combines the functionality of [`celestia-rpc`] and [`celestia-grpc`] crates.

There are two modes: read-only mode and submit mode. Read-only mode requires
RPC and optionally gRPC endpoint, while submit mode requires both, plus a signer.

# Examples

Read-only mode:

```rust,no_run,ignore-wasm32
use celestia_client::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::builder()
        .rpc_url("ws://localhost:26658")
        .build()
        .await?;

    let header = client.header().head().await?;
    dbg!(header);

    Ok(())
}
```

Submit mode:

```rust,no_run,ignore-wasm32
use std::env;

use celestia_client::{Client, Result};
use celestia_client::tx::TxConfig;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::builder()
        .rpc_url("ws://localhost:26658")
        .grpc_url("http://localhost:9090")
        .private_key_hex("393fdb5def075819de55756b45c9e2c8531a8c78dd6eede483d3440e9457d839")
        .build()
        .await?;

    let to_address = "celestia169s50psyj2f4la9a2235329xz7rk6c53zhw9mm".parse().unwrap();
    let tx_info = client.state().transfer(&to_address, 12345, TxConfig::default()).await?;
    dbg!(tx_info);

    Ok(())
}
```

Submitting and retrieving a blob:

```rust,no_run,ignore-wasm32
use std::env;

use celestia_client::{Client, Result};
use celestia_client::tx::TxConfig;
use celestia_client::types::nmt::Namespace;
use celestia_client::types::Blob;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::builder()
        .rpc_url("ws://localhost:26658")
        .grpc_url("http://localhost:9090")
        .private_key_hex("393fdb5def075819de55756b45c9e2c8531a8c78dd6eede483d3440e9457d839")
        .build()
        .await?;

    // Create the blob
    let ns = Namespace::new_v0(b"mydata")?;
    let blob = Blob::new(
        ns,
        b"some data to store".to_vec(),
        Some(client.address()?),
    )?;

    // This is the hash of the blob which is needed later on for retrieving
    // it form chain.
    let commitment = blob.commitment.clone();

    // Submit the blob
    let tx_info = client.blob().submit(&[blob], TxConfig::default()).await?;

    // Retrieve the blob. Blob is validated within the `get` method, so
    // we don't need to do anything else.
    let received_blob = client
        .blob()
        .get(tx_info.height, ns, commitment)
        .await?;

    println!("Data: {:?}", str::from_utf8(&received_blob.data).unwrap());

    Ok(())
}
```

[`celestia-rpc`]: celestia_rpc
[`celestia-grpc`]: celestia_grpc

# TLS support

Client will be configured to use TLS if at least one of the trust roots provider is enabled using the crate features
`tls-native-roots` and `tls-webpki-roots`. The trust roots are additive, selecting both will result in both being in use.

Moreover, the crypto provider for `rustls` can be configured by using `tls-ring` and `tls-aws-lc` features.

Those features are re-exposed from the `tonic` crate, please refer to its documentation for more info.
