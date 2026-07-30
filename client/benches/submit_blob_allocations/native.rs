//! Heap-allocation profiler for the blob submission path.
//!
//! Each invocation profiles exactly one operation in a fresh process. Examples:
//!
//! ```text
//! cargo bench -p celestia-client --bench submit_blob_allocations \
//!   --features allocation-profiling -- \
//!   --phase construct --size 7MiB
//!
//! cargo bench -p celestia-client --bench submit_blob_allocations \
//!   --features allocation-profiling -- \
//!   --phase full --size 7MiB
//!
//! cargo bench -p celestia-client --bench submit_blob_allocations \
//!   --features allocation-profiling -- \
//!   --phase grpc-submit --scenario stale-sequence --size 1MiB
//!
//! cargo bench -p celestia-client --bench submit_blob_allocations \
//!   --features allocation-profiling -- \
//!   --phase broadcast --scenario failover --size 7MiB
//! ```
//!
//! Networked phases expect Lumina's development network at its standard
//! endpoints. Override them with `--rpc-url`, `--grpc-url`, and `--private-key`
//! or the `LUMINA_ALLOC_RPC_URL`, `LUMINA_ALLOC_GRPC_URL`, and
//! `LUMINA_ALLOC_PRIVATE_KEY` environment variables.

use std::error::Error;
use std::fmt;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::{env, fs};

use celestia_client::Client;
use celestia_client::tx::TxConfig;
use celestia_grpc::GrpcClient;
use celestia_grpc::grpc::BroadcastMode;
use celestia_types::Blob;
use celestia_types::nmt::Namespace;
use celestia_types::state::{AccAddress, TxResponse};

#[global_allocator]
static ALLOCATOR: dhat::Alloc = dhat::Alloc;

const DEFAULT_RPC_URL: &str = "ws://localhost:26658";
const DEFAULT_GRPC_URL: &str = "http://localhost:19090";
const DEFAULT_FAILING_GRPC_URL: &str = "http://localhost:19999";
const DEFAULT_BLOB_SIZE: usize = 7 * 1024 * 1024;
const WARMUP_BLOB_SIZE: usize = 1024;
const STALE_SEQUENCE_GAS_LIMIT: u64 = 100_000_000;
const STALE_SEQUENCE_GAS_PRICE: f64 = 0.1;
const DEFAULT_PRIVATE_KEY: &str =
    "393fdb5def075819de55756b45c9e2c8531a8c78dd6eede483d3440e9457d839";

pub(super) type DynError = Box<dyn Error + Send + Sync>;
pub(super) type Result<T> = std::result::Result<T, DynError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Construct,
    Broadcast,
    GrpcSubmit,
    ClientSubmit,
    Full,
}

impl Phase {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "construct" => Ok(Self::Construct),
            "broadcast" => Ok(Self::Broadcast),
            "grpc-submit" => Ok(Self::GrpcSubmit),
            "client-submit" => Ok(Self::ClientSubmit),
            "full" => Ok(Self::Full),
            _ => Err(argument_error(format!("unknown phase: {value}"))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Construct => "construct",
            Self::Broadcast => "broadcast",
            Self::GrpcSubmit => "grpc-submit",
            Self::ClientSubmit => "client-submit",
            Self::Full => "full",
        }
    }

    fn is_networked(self) -> bool {
        !matches!(self, Self::Construct)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Happy,
    StaleSequence,
    Failover,
}

impl Scenario {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "happy" => Ok(Self::Happy),
            "stale-sequence" => Ok(Self::StaleSequence),
            "failover" => Ok(Self::Failover),
            _ => Err(argument_error(format!("unknown scenario: {value}"))),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Happy => "happy",
            Self::StaleSequence => "stale-sequence",
            Self::Failover => "failover",
        }
    }
}

#[derive(Debug)]
struct Config {
    phase: Phase,
    scenario: Scenario,
    size: usize,
    rpc_url: String,
    grpc_url: String,
    failing_grpc_url: String,
    private_key: String,
    profile_dir: PathBuf,
}

impl Config {
    fn parse() -> Result<Option<Self>> {
        let workspace_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("client crate is in the workspace root");
        let mut config = Self {
            phase: Phase::Construct,
            scenario: Scenario::Happy,
            size: DEFAULT_BLOB_SIZE,
            rpc_url: env::var("LUMINA_ALLOC_RPC_URL")
                .unwrap_or_else(|_| DEFAULT_RPC_URL.to_owned()),
            grpc_url: env::var("LUMINA_ALLOC_GRPC_URL")
                .unwrap_or_else(|_| DEFAULT_GRPC_URL.to_owned()),
            failing_grpc_url: DEFAULT_FAILING_GRPC_URL.to_owned(),
            private_key: env::var("LUMINA_ALLOC_PRIVATE_KEY")
                .unwrap_or_else(|_| DEFAULT_PRIVATE_KEY.trim().to_owned()),
            profile_dir: workspace_dir.join("target/alloc-profiles"),
        };

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                // Cargo appends this marker to harness-free bench binaries.
                "--bench" => {}
                "-h" | "--help" => {
                    print_help();
                    return Ok(None);
                }
                "--phase" => config.phase = Phase::parse(&next_arg(&mut args, "--phase")?)?,
                "--scenario" => {
                    config.scenario = Scenario::parse(&next_arg(&mut args, "--scenario")?)?
                }
                "--size" => config.size = parse_size(&next_arg(&mut args, "--size")?)?,
                "--rpc-url" => config.rpc_url = next_arg(&mut args, "--rpc-url")?,
                "--grpc-url" => config.grpc_url = next_arg(&mut args, "--grpc-url")?,
                "--failing-grpc-url" => {
                    config.failing_grpc_url = next_arg(&mut args, "--failing-grpc-url")?
                }
                "--private-key" => config.private_key = next_arg(&mut args, "--private-key")?,
                "--profile-dir" => {
                    config.profile_dir = next_arg(&mut args, "--profile-dir")?.into()
                }
                _ => return Err(argument_error(format!("unknown argument: {arg}"))),
            }
        }

        config.validate()?;
        Ok(Some(config))
    }

    fn validate(&self) -> Result<()> {
        if self.size == 0 {
            return Err(argument_error("blob size must be greater than zero"));
        }
        match (self.phase, self.scenario) {
            (Phase::Broadcast, Scenario::StaleSequence) => Err(argument_error(
                "stale-sequence is only valid for a submission phase",
            )),
            (Phase::Construct, Scenario::Happy) => Ok(()),
            (Phase::Construct, _) => {
                Err(argument_error("construct only supports the happy scenario"))
            }
            (_, Scenario::Failover) if self.phase != Phase::Broadcast => Err(argument_error(
                "failover is isolated in the broadcast phase so the large request is the first RPC",
            )),
            _ => Ok(()),
        }
    }

    fn profile_path(&self) -> PathBuf {
        self.profile_dir.join(format!(
            "{}-{}-{}.json",
            self.phase.as_str(),
            self.scenario.as_str(),
            self.size
        ))
    }
}

#[derive(Debug)]
struct ProfileSummary {
    total_blocks: u64,
    total_bytes: u64,
    max_blocks: usize,
    max_bytes: usize,
    curr_blocks: usize,
    curr_bytes: usize,
}

#[tokio::main(flavor = "current_thread")]
pub(super) async fn run() -> Result<()> {
    let Some(config) = Config::parse()? else {
        return Ok(());
    };
    fs::create_dir_all(&config.profile_dir)?;

    if config.phase.is_networked() {
        println!(
            "profiling {} ({}, {}) against {}",
            config.phase.as_str(),
            config.scenario.as_str(),
            HumanBytes(config.size as u64),
            config.grpc_url
        );
    } else {
        println!(
            "profiling {} ({}, {})",
            config.phase.as_str(),
            config.scenario.as_str(),
            HumanBytes(config.size as u64)
        );
    }

    let profile_path = config.profile_path();
    let summary = match config.phase {
        Phase::Construct => profile_construct(&config, &profile_path).await?,
        Phase::Broadcast => profile_broadcast(&config, &profile_path).await?,
        Phase::GrpcSubmit => profile_grpc_submit(&config, &profile_path).await?,
        Phase::ClientSubmit => profile_client_submit(&config, &profile_path).await?,
        Phase::Full => profile_full(&config, &profile_path).await?,
    };

    println!("profile: {}", profile_path.display());
    println!(
        "total: {} in {} allocations",
        HumanBytes(summary.total_bytes),
        summary.total_blocks
    );
    println!(
        "peak:  {} in {} live allocations",
        HumanBytes(summary.max_bytes as u64),
        summary.max_blocks
    );
    println!(
        "end:   {} in {} live allocations",
        HumanBytes(summary.curr_bytes as u64),
        summary.curr_blocks
    );

    Ok(())
}

async fn profile_construct(config: &Config, profile_path: &Path) -> Result<ProfileSummary> {
    let payload = vec![0xA5; config.size];
    let address = build_grpc(config, Scenario::Happy)?
        .get_account_address()
        .ok_or_else(|| argument_error("benchmark client has no signer"))?;
    let namespace = benchmark_namespace();

    let profiler = start_profiler(profile_path);
    let blob = Blob::new(namespace, payload.to_vec(), Some(address))?;
    black_box(&blob);
    drop(blob);
    Ok(finish_profiler(profiler))
}

async fn profile_broadcast(config: &Config, profile_path: &Path) -> Result<ProfileSummary> {
    let client = build_grpc(config, config.scenario)?;
    // The bytes deliberately aren't a valid Cosmos transaction. This phase
    // isolates request cloning and gRPC framing, so rejection after the server
    // has received the complete request is the expected outcome.
    let tx_bytes = vec![0xA5; config.size];

    let profiler = start_profiler(profile_path);
    let result = client.broadcast_tx(tx_bytes, BroadcastMode::Sync).await;
    black_box(&result);
    let summary = finish_profiler(profiler);

    match result {
        Ok(response) => {
            let response: TxResponse = response;
            black_box(response);
        }
        Err(error) => println!("server response after receiving transport payload: {error}"),
    }
    Ok(summary)
}

async fn profile_grpc_submit(config: &Config, profile_path: &Path) -> Result<ProfileSummary> {
    let client = build_grpc(config, Scenario::Happy)?;
    warm_grpc(&client).await?;
    if config.scenario == Scenario::StaleSequence {
        advance_sequence(config).await?;
    }

    let address = client
        .get_account_address()
        .ok_or_else(|| argument_error("benchmark client has no signer"))?;
    let blob = make_blob(config.size, address)?;
    let tx_config = submission_config(config.scenario);

    let profiler = start_profiler(profile_path);
    let result = client.submit_blobs(&[blob], tx_config).await;
    black_box(&result);
    let summary = finish_profiler(profiler);

    black_box(result?);
    Ok(summary)
}

async fn profile_client_submit(config: &Config, profile_path: &Path) -> Result<ProfileSummary> {
    let client = build_client(config).await?;
    warm_client(&client).await?;
    if config.scenario == Scenario::StaleSequence {
        advance_sequence(config).await?;
    }

    let blob = make_blob(config.size, client.address()?)?;
    let tx_config = submission_config(config.scenario);

    let profiler = start_profiler(profile_path);
    let result = client.state().submit_pay_for_blob(&[blob], tx_config).await;
    black_box(&result);
    let summary = finish_profiler(profiler);

    black_box(result?);
    Ok(summary)
}

async fn profile_full(config: &Config, profile_path: &Path) -> Result<ProfileSummary> {
    let client = build_client(config).await?;
    warm_client(&client).await?;
    if config.scenario == Scenario::StaleSequence {
        advance_sequence(config).await?;
    }

    let payload = vec![0xA5; config.size];
    let address = client.address()?;
    let namespace = benchmark_namespace();
    let tx_config = submission_config(config.scenario);

    let profiler = start_profiler(profile_path);
    let result = async {
        let blob = Blob::new(namespace, payload.to_vec(), Some(address))?;
        client
            .state()
            .submit_pay_for_blob(&[blob], tx_config)
            .await
            .map_err(DynError::from)
    }
    .await;
    black_box(&result);
    let summary = finish_profiler(profiler);

    black_box(result?);
    Ok(summary)
}

fn start_profiler(profile_path: &Path) -> dhat::Profiler {
    dhat::Profiler::builder()
        .file_name(profile_path)
        .trim_backtraces(Some(20))
        .build()
}

fn finish_profiler(profiler: dhat::Profiler) -> ProfileSummary {
    let stats = dhat::HeapStats::get();
    let summary = ProfileSummary {
        total_blocks: stats.total_blocks,
        total_bytes: stats.total_bytes,
        max_blocks: stats.max_blocks,
        max_bytes: stats.max_bytes,
        curr_blocks: stats.curr_blocks,
        curr_bytes: stats.curr_bytes,
    };
    drop(profiler);
    summary
}

fn build_grpc(config: &Config, scenario: Scenario) -> Result<GrpcClient> {
    let builder = GrpcClient::builder().private_key_hex(&config.private_key);
    let builder = match scenario {
        Scenario::Failover => {
            builder.urls([config.failing_grpc_url.as_str(), config.grpc_url.as_str()])
        }
        _ => builder.url(config.grpc_url.as_str()),
    };
    Ok(builder.build()?)
}

async fn build_client(config: &Config) -> Result<Client> {
    Ok(Client::builder()
        .rpc_url(&config.rpc_url)
        .grpc_url(config.grpc_url.as_str())
        .private_key_hex(&config.private_key)
        .build()
        .await?)
}

async fn warm_grpc(client: &GrpcClient) -> Result<()> {
    let address = client
        .get_account_address()
        .ok_or_else(|| argument_error("benchmark client has no signer"))?;
    let blob = make_blob(WARMUP_BLOB_SIZE, address)?;
    client.submit_blobs(&[blob], TxConfig::default()).await?;
    Ok(())
}

async fn warm_client(client: &Client) -> Result<()> {
    let blob = make_blob(WARMUP_BLOB_SIZE, client.address()?)?;
    client
        .state()
        .submit_pay_for_blob(&[blob], TxConfig::default())
        .await?;
    Ok(())
}

async fn advance_sequence(config: &Config) -> Result<()> {
    let interfering_client = build_grpc(config, Scenario::Happy)?;
    let address = interfering_client
        .get_account_address()
        .ok_or_else(|| argument_error("benchmark client has no signer"))?;
    let blob = make_blob(WARMUP_BLOB_SIZE, address)?;
    interfering_client
        .submit_blobs(&[blob], TxConfig::default())
        .await?;
    Ok(())
}

fn submission_config(scenario: Scenario) -> TxConfig {
    match scenario {
        Scenario::StaleSequence => TxConfig::default()
            .with_gas_limit(STALE_SEQUENCE_GAS_LIMIT)
            .with_gas_price(STALE_SEQUENCE_GAS_PRICE),
        _ => TxConfig::default(),
    }
}

fn make_blob(size: usize, address: AccAddress) -> celestia_types::Result<Blob> {
    Blob::new(benchmark_namespace(), vec![0xA5; size], Some(address))
}

fn benchmark_namespace() -> Namespace {
    Namespace::new_v0(b"allocbench").expect("static namespace is valid")
}

fn next_arg(args: &mut impl Iterator<Item = String>, option: &str) -> Result<String> {
    args.next()
        .ok_or_else(|| argument_error(format!("missing value for {option}")))
}

fn parse_size(input: &str) -> Result<usize> {
    let input = input.trim();
    let split = input
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(input.len());
    let (number, suffix) = input.split_at(split);
    let value: usize = number
        .parse()
        .map_err(|_| argument_error(format!("invalid size: {input}")))?;
    let multiplier = match suffix.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        _ => return Err(argument_error(format!("invalid size suffix: {suffix}"))),
    };
    value
        .checked_mul(multiplier)
        .ok_or_else(|| argument_error(format!("size overflows usize: {input}")))
}

fn argument_error(message: impl Into<String>) -> DynError {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into()).into()
}

fn print_help() {
    println!(
        "\
Profile allocations made by one blob submission operation.

USAGE:
  cargo bench -p celestia-client --bench submit_blob_allocations \
    --features allocation-profiling -- [OPTIONS]

OPTIONS:
  --phase <PHASE>              construct | broadcast | grpc-submit |
                               client-submit | full [default: construct]
  --scenario <SCENARIO>        happy | stale-sequence | failover [default: happy]
  --size <BYTES|KiB|MiB>       Payload size [default: 7MiB]
  --rpc-url <URL>              RPC endpoint [default: ws://localhost:26658]
  --grpc-url <URL>             gRPC endpoint [default: http://localhost:19090]
  --failing-grpc-url <URL>     First endpoint for the failover scenario
  --private-key <HEX>          Submission account key
  --profile-dir <PATH>         DHAT JSON output directory
  -h, --help                   Print help

The failover scenario is restricted to the broadcast phase so the payload-sized
request, rather than an account or chain-state query, triggers failover."
    );
}

struct HumanBytes(u64);

impl fmt::Display for HumanBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        const KIB: f64 = 1024.0;
        const MIB: f64 = KIB * 1024.0;
        let bytes = self.0 as f64;
        if bytes >= MIB {
            write!(formatter, "{:.2} MiB", bytes / MIB)
        } else if bytes >= KIB {
            write!(formatter, "{:.2} KiB", bytes / KIB)
        } else {
            write!(formatter, "{} B", self.0)
        }
    }
}
