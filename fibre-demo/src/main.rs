//! Submit a sample blob to a Fibre test network over the Fibre protocol.
//!
//! Flow: encode blob -> upload shards to validators & collect signatures ->
//! broadcast MsgPayForFibre on-chain -> download the blob back and verify.

use std::sync::Arc;

use celestia_fibre::{
    BlobConfig, DownloadOptions, EncodedBlob, FibreClient, FibreClientConfig, GrpcHostRegistry,
    GrpcSetGetter, GrpcValidatorConnector,
};
use celestia_grpc::{GrpcClient, TxConfig};
use celestia_proto::celestia::fibre::v1::MsgPayForFibre;
use celestia_types::nmt::Namespace;
use clap::Parser;
use k256::ecdsa::SigningKey;

const GREETING: &[u8] = b"Hello from the celestia-fibre demo! ";

#[derive(Parser)]
#[command(name = "fibre-demo", version)]
#[command(about = "Round-trip a sample blob through the Fibre protocol")]
struct Cli {
    /// Chain ID included in Fibre payment promises.
    #[arg(long, value_parser = parse_non_empty)]
    chain_id: String,

    /// Celestia app gRPC endpoint used for host queries and payment transactions.
    #[arg(long, value_parser = parse_non_empty)]
    app_grpc_url: String,

    /// Celestia core gRPC endpoint used to fetch validator sets.
    #[arg(long, value_parser = parse_non_empty)]
    core_grpc_url: String,

    /// Hex-encoded secp256k1 private key for Fibre promises and payment transactions.
    #[arg(long, value_parser = parse_private_key)]
    private_key: String,

    /// Ten-byte ASCII suffix for a version-zero namespace.
    #[arg(long, value_parser = parse_namespace)]
    namespace: String,

    /// Size of the generated payload in bytes.
    #[arg(long, value_parser = parse_blob_size)]
    blob_size: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().init();

    run(Cli::parse()).await
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    let private_key = hex::decode(&cli.private_key)?;
    let signing_key = SigningKey::from_slice(&private_key)?;

    let app_grpc = GrpcClient::builder()
        .url(&cli.app_grpc_url)
        .private_key(&private_key)
        .build()?;
    let core_grpc = GrpcClient::builder().url(&cli.core_grpc_url).build()?;

    let host_registry = Arc::new(GrpcHostRegistry::new(app_grpc.clone()));

    let fibre = FibreClient::builder()
        .config(FibreClientConfig::new(cli.chain_id.clone())?)
        .set_getter(GrpcSetGetter::new(core_grpc))
        .connector(GrpcValidatorConnector::new(host_registry, cli.chain_id))
        .build()?;

    let signer = app_grpc
        .get_account_address()
        .expect("signer is configured");
    let namespace = Namespace::new_v0(cli.namespace.as_bytes())?;

    let data: Vec<u8> = GREETING
        .iter()
        .copied()
        .cycle()
        .take(cli.blob_size)
        .collect();
    let blob = EncodedBlob::new(&data, BlobConfig::for_version(0)?)?;
    let blob_id = blob.id().clone();
    println!(
        "uploading blob {blob_id} ({} data bytes, {} paid upload bytes)",
        data.len(),
        blob.upload_size(),
    );

    let signed = fibre.upload(&signing_key, namespace, blob).await?;
    let sig_count = signed.validator_signatures.iter().flatten().count();
    println!("upload done, {sig_count} validator signatures collected");

    // signatures[i] must stay aligned with validator[i]; missing ones stay as
    // empty vecs, which the chain skips.
    let msg = MsgPayForFibre {
        signer: signer.to_string(),
        payment_promise: Some((&signed.promise).into()),
        validator_signatures: signed
            .validator_signatures
            .iter()
            .map(|s| s.clone().unwrap_or_default())
            .collect(),
    };

    let tx = app_grpc
        .broadcast_message(msg, TxConfig::default())
        .await?
        .confirm()
        .await?;
    println!(
        "MsgPayForFibre confirmed: hash {}, height {}",
        tx.hash, tx.height
    );

    let downloaded = fibre.download(&blob_id, DownloadOptions::default()).await?;
    assert_eq!(downloaded.data(), data.as_slice());
    println!("download roundtrip OK, {} bytes match", data.len());

    Ok(())
}

fn parse_non_empty(value: &str) -> Result<String, String> {
    if value.is_empty() {
        Err("must not be empty".to_string())
    } else {
        Ok(value.to_string())
    }
}

fn parse_private_key(value: &str) -> Result<String, String> {
    let bytes = hex::decode(value).map_err(|error| format!("invalid hex: {error}"))?;
    SigningKey::from_slice(&bytes)
        .map_err(|_| "must be a valid 32-byte secp256k1 key".to_string())?;
    Ok(value.to_string())
}

fn parse_namespace(value: &str) -> Result<String, String> {
    if !value.is_ascii() {
        return Err("must be ASCII".to_string());
    }
    if value.len() != 10 {
        return Err(format!("must be exactly 10 bytes, got {}", value.len()));
    }
    Namespace::new_v0(value.as_bytes()).map_err(|error| error.to_string())?;
    Ok(value.to_string())
}

fn parse_blob_size(value: &str) -> Result<usize, String> {
    let size = value
        .parse::<usize>()
        .map_err(|error| format!("invalid size: {error}"))?;
    let max = BlobConfig::v0().max_data_size;
    if !(1..=max).contains(&size) {
        return Err(format!("must be between 1 and {max} bytes"));
    }
    Ok(size)
}
