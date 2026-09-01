//! Command-line entry point: parse args, spawn the network, print the
//! endpoints, run until ctrl-c.

use std::net::IpAddr;

use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::{MockNetworkConfig, spawn_mock_network};

/// In-memory mock validator network for fibre load testing.
///
/// Blobs are kept in RAM for the lifetime of the process and never evicted.
#[derive(Parser)]
struct Args {
    /// Number of simulated validators (one gRPC port each)
    #[arg(long, default_value_t = 3)]
    validators: usize,
    /// IP address to listen on
    #[arg(long, default_value = "127.0.0.1")]
    listen: IpAddr,
    /// IP address advertised to clients; required when listening on a wildcard address
    #[arg(long)]
    advertise: Option<IpAddr>,
    /// Control-plane port; validator i listens on base-port + 1 + i.
    /// 0 = ephemeral ports for everything.
    #[arg(long, default_value_t = 19000)]
    base_port: u16,
    /// Voting power of each validator
    #[arg(long, default_value_t = 100)]
    voting_power: i64,
    /// Height reported by ValidatorSet
    #[arg(long, default_value_t = 1)]
    height: i64,
    /// Chain id signed into TLS identities and served in the latest-block
    /// header; must match the clients' --chain-id
    #[arg(long, default_value = "mock-1")]
    chain_id: String,
}

/// Run the mock server until ctrl-c.
pub async fn run() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let args = Args::parse();
    let handle = spawn_mock_network(MockNetworkConfig {
        num_validators: args.validators,
        listen_ip: args.listen,
        advertise_ip: args.advertise,
        base_port: args.base_port,
        voting_power: args.voting_power,
        height: args.height,
        chain_id: args.chain_id,
    })
    .await?;

    println!(
        "control plane: http://{} (pass to FibreClient::from_endpoint, --app-grpc-url and --core-grpc-url)",
        handle.control_addr
    );
    println!(
        "{:<4} {:<56} {:<28} {:>6}",
        "idx", "consensus address", "host", "power"
    );
    for (i, v) in handle.validators.iter().enumerate() {
        println!(
            "{:<4} {:<56} {:<28} {:>6}",
            i, v.consensus_addr_bech32, v.host, v.voting_power
        );
    }

    tokio::signal::ctrl_c().await?;
    handle.shutdown();
    Ok(())
}
