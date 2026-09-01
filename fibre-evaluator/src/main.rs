use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use celestia_fibre::{
    Blob, BlobConfig, BlobID, DownloadOptions, FibreClient, FibreClientConfig, GrpcHostRegistry,
    GrpcSetGetter, GrpcValidatorConnector,
};
use celestia_grpc::{GrpcClient, TxConfig};
use celestia_proto::celestia::fibre::v1::MsgPayForFibre;
use celestia_types::nmt::Namespace;
use celestia_types::state::AccAddress;
use clap::Parser;
use k256::ecdsa::SigningKey;
use rand::RngCore;
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinSet;
use tokio::time::{self, Instant, MissedTickBehavior};
use tracing_subscriber::EnvFilter;

const PAYLOAD_MAGIC: &[u8; 4] = b"FBE0";
const PAYLOAD_HEADER_LEN: usize = 16;
const MIB: f64 = 1024.0 * 1024.0;
const GIB: f64 = MIB * 1024.0;

#[derive(Parser)]
#[command(name = "fibre-evaluator", version)]
#[command(about = "Evaluate Fibre upload and download throughput")]
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

    /// Target number of blob submission launches per second.
    #[arg(long, value_parser = parse_blob_rate)]
    blobs_per_second: f64,

    /// Size of each generated payload in bytes, including its integrity header.
    #[arg(long, value_parser = parse_blob_size)]
    blob_size: usize,

    /// Duration during which new submissions are launched.
    #[arg(long, value_parser = parse_positive_u64)]
    run_for_seconds: u64,

    /// Maximum concurrent upload and payment pipelines.
    #[arg(long, default_value_t = 16, value_parser = parse_positive_usize)]
    max_in_flight: usize,

    /// Timeout applied to each submission pipeline and each download.
    #[arg(long, default_value_t = 120, value_parser = parse_positive_u64)]
    operation_timeout_seconds: u64,

    /// Interval between periodic statistics reports.
    #[arg(long, default_value_t = 10, value_parser = parse_positive_u64)]
    stats_interval_seconds: u64,
}

struct SubmissionContext {
    fibre: Arc<FibreClient>,
    app_grpc: GrpcClient,
    signing_key: SigningKey,
    signer: AccAddress,
    namespace: Namespace,
    blob_size: usize,
    operation_timeout: Duration,
}

struct DownloadJob {
    id: BlobID,
    sequence: u64,
    expected_size: usize,
}

enum Event {
    SubmissionAttempt,
    SubmissionSuccess { bytes: u64 },
    SubmissionFailure { stage: &'static str, error: String },
    DownloadSuccess { bytes: u64 },
    DownloadFailure { error: String },
    DownloadCorrupt { error: String },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Stats {
    submission_attempts: u64,
    submission_successes: u64,
    submission_failures: u64,
    submission_bytes: u64,
    download_successes: u64,
    download_failures: u64,
    download_corrupt: u64,
    download_bytes: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    run(Cli::parse()).await
}

async fn run(cli: Cli) -> Result<()> {
    let private_key = hex::decode(&cli.private_key).context("decoding --private-key")?;
    let signing_key = SigningKey::from_slice(&private_key).context("parsing --private-key")?;

    let app_grpc = GrpcClient::builder()
        .url(&cli.app_grpc_url)
        .private_key(&private_key)
        .build()
        .context("building app gRPC client")?;
    let core_grpc = GrpcClient::builder()
        .url(&cli.core_grpc_url)
        .build()
        .context("building core gRPC client")?;

    let signer = app_grpc
        .get_account_address()
        .context("app gRPC client has no signer")?;
    let namespace = Namespace::new_v0(cli.namespace.as_bytes()).context("parsing namespace")?;

    let host_registry = Arc::new(GrpcHostRegistry::new(app_grpc.clone()));

    let fibre = Arc::new(
        FibreClient::builder()
            .config(FibreClientConfig {
                chain_id: cli.chain_id.clone(),
                ..FibreClientConfig::default()
            })
            .set_getter(GrpcSetGetter::new(core_grpc))
            .connector(GrpcValidatorConnector::new(host_registry, cli.chain_id))
            .build()
            .context("building Fibre client")?,
    );

    let operation_timeout = Duration::from_secs(cli.operation_timeout_seconds);
    let context = Arc::new(SubmissionContext {
        fibre: Arc::clone(&fibre),
        app_grpc,
        signing_key,
        signer,
        namespace,
        blob_size: cli.blob_size,
        operation_timeout,
    });

    tracing::info!(
        chain_id = %context.fibre.config().chain_id,
        namespace = %cli.namespace,
        blob_size = cli.blob_size,
        blobs_per_second = cli.blobs_per_second,
        run_for_seconds = cli.run_for_seconds,
        max_in_flight = cli.max_in_flight,
        "starting Fibre evaluation"
    );

    let started_at = Instant::now();
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (download_tx, download_rx) = mpsc::unbounded_channel();

    let stats_handle = tokio::spawn(run_stats_collector(
        event_rx,
        Duration::from_secs(cli.stats_interval_seconds),
        started_at,
    ));
    let download_handle = tokio::spawn(run_download_loop(
        fibre,
        download_rx,
        event_tx.clone(),
        operation_timeout,
    ));

    let submission_elapsed = run_submission_loop(
        context,
        download_tx,
        event_tx.clone(),
        cli.blobs_per_second,
        Duration::from_secs(cli.run_for_seconds),
        cli.max_in_flight,
        started_at,
    )
    .await?;

    let download_elapsed = download_handle.await.context("download task panicked")?;
    drop(event_tx);
    let stats = stats_handle.await.context("stats task panicked")?;

    print_final_report(&stats, submission_elapsed, download_elapsed);
    Ok(())
}

async fn run_submission_loop(
    context: Arc<SubmissionContext>,
    download_tx: mpsc::UnboundedSender<DownloadJob>,
    event_tx: mpsc::UnboundedSender<Event>,
    blobs_per_second: f64,
    run_for: Duration,
    max_in_flight: usize,
    started_at: Instant,
) -> Result<Duration> {
    let period = submission_period(blobs_per_second);
    let deadline = started_at + run_for;
    let mut ticker = time::interval_at(started_at, period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let deadline_sleep = time::sleep_until(deadline);
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(deadline_sleep);
    tokio::pin!(ctrl_c);

    let semaphore = Arc::new(Semaphore::new(max_in_flight));
    let mut tasks = JoinSet::new();
    let mut sequence = 0u64;

    'launch: loop {
        tokio::select! {
            biased;
            _ = &mut deadline_sleep => break,
            result = &mut ctrl_c => {
                result.context("listening for Ctrl+C")?;
                tracing::info!("received Ctrl+C; stopping new submissions");
                break;
            }
            result = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    tracing::error!(%error, "submission task panicked");
                }
            }
            _ = ticker.tick() => {
                let permit = tokio::select! {
                    biased;
                    _ = &mut deadline_sleep => break 'launch,
                    result = &mut ctrl_c => {
                        result.context("listening for Ctrl+C")?;
                        tracing::info!("received Ctrl+C; stopping new submissions");
                        break 'launch;
                    }
                    permit = Arc::clone(&semaphore).acquire_owned() => {
                        permit.context("submission semaphore closed")?
                    }
                };

                sequence = sequence.checked_add(1).context("submission sequence overflow")?;
                let task_sequence = sequence;
                let context = Arc::clone(&context);
                let download_tx = download_tx.clone();
                let event_tx = event_tx.clone();
                let _ = event_tx.send(Event::SubmissionAttempt);

                tasks.spawn(async move {
                    let _permit = permit;
                    let result = time::timeout(
                        context.operation_timeout,
                        submit_one(&context, task_sequence),
                    )
                    .await;

                    match result {
                        Ok(Ok(job)) => {
                            let bytes = job.expected_size as u64;
                            let _ = event_tx.send(Event::SubmissionSuccess { bytes });
                            if download_tx.send(job).is_err() {
                                tracing::error!("download task stopped before receiving confirmed blob");
                            }
                        }
                        Ok(Err((stage, error))) => {
                            let _ = event_tx.send(Event::SubmissionFailure {
                                stage,
                                error: format!("{error:#}"),
                            });
                        }
                        Err(_) => {
                            let _ = event_tx.send(Event::SubmissionFailure {
                                stage: "timeout",
                                error: format!(
                                    "operation exceeded {:?}",
                                    context.operation_timeout
                                ),
                            });
                        }
                    }
                });
            }
        }
    }

    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            tracing::error!(%error, "submission task panicked");
        }
    }
    drop(download_tx);

    Ok(started_at.elapsed())
}

async fn submit_one(
    context: &SubmissionContext,
    sequence: u64,
) -> std::result::Result<DownloadJob, (&'static str, anyhow::Error)> {
    let payload = make_payload(sequence, context.blob_size);
    let blob = Blob::new(&payload, BlobConfig::v0()).map_err(|error| ("encode", anyhow!(error)))?;
    let id = blob.id().clone();

    let signed = context
        .fibre
        .upload(&context.signing_key, context.namespace, blob)
        .await
        .map_err(|error| ("fibre_upload", anyhow!(error)))?;

    let message = MsgPayForFibre {
        signer: context.signer.to_string(),
        payment_promise: Some((&signed.promise).into()),
        validator_signatures: signed
            .validator_signatures
            .iter()
            .map(|signature| signature.clone().unwrap_or_default())
            .collect(),
    };

    context
        .app_grpc
        .broadcast_message(message, TxConfig::default())
        .await
        .map_err(|error| ("payment_broadcast", anyhow!(error)))?
        .confirm()
        .await
        .map_err(|error| ("payment_confirmation", anyhow!(error)))?;

    Ok(DownloadJob {
        id,
        sequence,
        expected_size: payload.len(),
    })
}

async fn run_download_loop(
    fibre: Arc<FibreClient>,
    mut jobs: mpsc::UnboundedReceiver<DownloadJob>,
    event_tx: mpsc::UnboundedSender<Event>,
    operation_timeout: Duration,
) -> Duration {
    let mut first_started = None;
    let mut last_finished = None;

    while let Some(job) = jobs.recv().await {
        first_started.get_or_insert_with(Instant::now);
        let result = time::timeout(
            operation_timeout,
            fibre.download(&job.id, DownloadOptions::default()),
        )
        .await;

        match result {
            Ok(Ok(blob)) => match blob.data() {
                Some(data) => match verify_payload(data, job.sequence, job.expected_size) {
                    Ok(()) => {
                        let _ = event_tx.send(Event::DownloadSuccess {
                            bytes: data.len() as u64,
                        });
                    }
                    Err(error) => {
                        let _ = event_tx.send(Event::DownloadCorrupt { error });
                    }
                },
                None => {
                    let _ = event_tx.send(Event::DownloadCorrupt {
                        error: "downloaded blob has no reconstructed data".to_string(),
                    });
                }
            },
            Ok(Err(error)) => {
                let _ = event_tx.send(Event::DownloadFailure {
                    error: error.to_string(),
                });
            }
            Err(_) => {
                let _ = event_tx.send(Event::DownloadFailure {
                    error: format!("operation exceeded {operation_timeout:?}"),
                });
            }
        }
        last_finished = Some(Instant::now());
    }

    match (first_started, last_finished) {
        (Some(start), Some(finish)) => finish.duration_since(start),
        _ => Duration::ZERO,
    }
}

async fn run_stats_collector(
    mut events: mpsc::UnboundedReceiver<Event>,
    stats_interval: Duration,
    started_at: Instant,
) -> Stats {
    let mut stats = Stats::default();
    let mut ticker = time::interval(stats_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        tokio::select! {
            event = events.recv() => match event {
                Some(event) => stats.apply(event),
                None => break,
            },
            _ = ticker.tick() => stats.log_periodic(started_at.elapsed()),
        }
    }

    stats
}

impl Stats {
    fn apply(&mut self, event: Event) {
        match event {
            Event::SubmissionAttempt => self.submission_attempts += 1,
            Event::SubmissionSuccess { bytes } => {
                self.submission_successes += 1;
                self.submission_bytes += bytes;
            }
            Event::SubmissionFailure { stage, error } => {
                self.submission_failures += 1;
                tracing::warn!(stage, %error, "submission failed");
            }
            Event::DownloadSuccess { bytes } => {
                self.download_successes += 1;
                self.download_bytes += bytes;
            }
            Event::DownloadFailure { error } => {
                self.download_failures += 1;
                tracing::warn!(%error, "download failed");
            }
            Event::DownloadCorrupt { error } => {
                self.download_corrupt += 1;
                tracing::warn!(%error, "downloaded payload is corrupt");
            }
        }
    }

    fn log_periodic(&self, elapsed: Duration) {
        let bps = per_second(self.submission_bytes, elapsed);
        tracing::info!(
            elapsed_seconds = %format_args!("{:.2}", elapsed.as_secs_f64()),
            attempts = self.submission_attempts,
            successes = self.submission_successes,
            failures = self.submission_failures,
            success_percent = %format_args!(
                "{:.2}",
                success_percent(
                    self.submission_successes,
                    self.submission_successes + self.submission_failures,
                )
            ),
            blobs_per_second = %format_args!("{:.1}", per_second(self.submission_successes, elapsed)),
            bytes_per_second = %format_args!("{:.2}", bps),
            gib_per_second = %format_args!("{:.6}", bps / GIB),
            download_successes = self.download_successes,
            download_failures = self.download_failures,
            download_corrupt = self.download_corrupt,
            "periodic stats"
        );
    }
}

fn print_final_report(stats: &Stats, submission_elapsed: Duration, download_elapsed: Duration) {
    let submission_total = stats.submission_successes + stats.submission_failures;
    let download_total =
        stats.download_successes + stats.download_failures + stats.download_corrupt;
    let submission_bps = per_second(stats.submission_bytes, submission_elapsed);
    let download_bps = per_second(stats.download_bytes, download_elapsed);

    tracing::info!("final Fibre evaluation stats");
    tracing::info!(
        attempts = stats.submission_attempts,
        confirmed = stats.submission_successes,
        failed = stats.submission_failures,
        success_percent = %format_args!("{:.2}", success_percent(stats.submission_successes, submission_total)),
        payload_bytes = stats.submission_bytes,
        elapsed_seconds = submission_elapsed.as_secs_f64(),
        blobs_per_second = per_second(stats.submission_successes, submission_elapsed),
        bytes_per_second = %format_args!("{:.2}", submission_bps),
        mib_per_second = %format_args!("{:.4}", submission_bps / MIB),
        gib_per_second = %format_args!("{:.6}", submission_bps / GIB),
        "submission stats"
    );
    tracing::info!(
        verified = stats.download_successes,
        failed = stats.download_failures,
        corrupt = stats.download_corrupt,
        success_percent = %format_args!("{:.2}", success_percent(stats.download_successes, download_total)),
        payload_bytes = stats.download_bytes,
        elapsed_seconds = download_elapsed.as_secs_f64(),
        blobs_per_second = per_second(stats.download_successes, download_elapsed),
        bytes_per_second = %format_args!("{:.2}", download_bps),
        mib_per_second = %format_args!("{:.4}", download_bps / MIB),
        gib_per_second = %format_args!("{:.6}", download_bps / GIB),
        "download stats"
    );
}

fn make_payload(sequence: u64, size: usize) -> Vec<u8> {
    let mut payload = vec![0u8; size];
    payload[..4].copy_from_slice(PAYLOAD_MAGIC);
    payload[4..12].copy_from_slice(&sequence.to_le_bytes());
    rand::thread_rng().fill_bytes(&mut payload[PAYLOAD_HEADER_LEN..]);
    let checksum = crc32fast::hash(&payload[PAYLOAD_HEADER_LEN..]);
    payload[12..16].copy_from_slice(&checksum.to_le_bytes());
    payload
}

fn verify_payload(data: &[u8], expected_sequence: u64, expected_size: usize) -> Result<(), String> {
    if data.len() != expected_size {
        return Err(format!(
            "payload size mismatch: expected {expected_size}, got {}",
            data.len()
        ));
    }
    if data.len() < PAYLOAD_HEADER_LEN {
        return Err(format!(
            "payload is shorter than {PAYLOAD_HEADER_LEN}-byte header"
        ));
    }
    if &data[..4] != PAYLOAD_MAGIC {
        return Err("payload magic mismatch".to_string());
    }

    let sequence = u64::from_le_bytes(data[4..12].try_into().expect("fixed-size slice"));
    if sequence != expected_sequence {
        return Err(format!(
            "payload sequence mismatch: expected {expected_sequence}, got {sequence}"
        ));
    }

    let expected_checksum = u32::from_le_bytes(data[12..16].try_into().expect("fixed-size slice"));
    let actual_checksum = crc32fast::hash(&data[PAYLOAD_HEADER_LEN..]);
    if actual_checksum != expected_checksum {
        return Err(format!(
            "payload CRC32 mismatch: expected {expected_checksum:#010x}, got {actual_checksum:#010x}"
        ));
    }

    Ok(())
}

fn submission_period(blobs_per_second: f64) -> Duration {
    Duration::from_secs_f64(1.0 / blobs_per_second)
}

fn success_percent(successes: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        successes as f64 * 100.0 / total as f64
    }
}

fn per_second(value: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        0.0
    } else {
        value as f64 / elapsed.as_secs_f64()
    }
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

fn parse_blob_rate(value: &str) -> Result<f64, String> {
    let rate = value
        .parse::<f64>()
        .map_err(|error| format!("invalid rate: {error}"))?;
    if !rate.is_finite() || rate <= 0.0 {
        return Err("must be finite and greater than zero".to_string());
    }
    if 1.0 / rate < 1e-9 {
        return Err("must not exceed 1,000,000,000 blobs per second".to_string());
    }
    Ok(rate)
}

fn parse_blob_size(value: &str) -> Result<usize, String> {
    let size = value
        .parse::<usize>()
        .map_err(|error| format!("invalid size: {error}"))?;
    let max = BlobConfig::v0().max_data_size;
    if !(PAYLOAD_HEADER_LEN..=max).contains(&size) {
        return Err(format!(
            "must be between {PAYLOAD_HEADER_LEN} and {max} bytes"
        ));
    }
    Ok(size)
}

fn parse_positive_u64(value: &str) -> Result<u64, String> {
    let value = value
        .parse::<u64>()
        .map_err(|error| format!("invalid integer: {error}"))?;
    if value == 0 {
        Err("must be greater than zero".to_string())
    } else {
        Ok(value)
    }
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|error| format!("invalid integer: {error}"))?;
    if value == 0 {
        Err("must be greater than zero".to_string())
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_KEY: &str = "0101010101010101010101010101010101010101010101010101010101010101";

    fn valid_args() -> Vec<&'static str> {
        vec![
            "fibre-evaluator",
            "--chain-id",
            "test-chain",
            "--app-grpc-url",
            "http://127.0.0.1:9091",
            "--core-grpc-url",
            "http://127.0.0.1:9090",
            "--private-key",
            VALID_KEY,
            "--namespace",
            "fibre-eval",
            "--blobs-per-second",
            "2.5",
            "--blob-size",
            "1024",
            "--run-for-seconds",
            "60",
        ]
    }

    #[test]
    fn parses_required_arguments_and_defaults() {
        let cli = Cli::try_parse_from(valid_args()).unwrap();
        assert_eq!(cli.chain_id, "test-chain");
        assert_eq!(cli.namespace, "fibre-eval");
        assert_eq!(cli.blobs_per_second, 2.5);
        assert_eq!(cli.blob_size, 1024);
        assert_eq!(cli.max_in_flight, 16);
        assert_eq!(cli.operation_timeout_seconds, 120);
        assert_eq!(cli.stats_interval_seconds, 10);
    }

    #[test]
    fn rejects_invalid_workload_arguments() {
        for (flag, value) in [
            ("--blobs-per-second", "0"),
            ("--blobs-per-second", "NaN"),
            ("--blob-size", "15"),
            ("--run-for-seconds", "0"),
            ("--max-in-flight", "0"),
            ("--operation-timeout-seconds", "0"),
            ("--stats-interval-seconds", "0"),
        ] {
            let mut args = valid_args();
            args.extend([flag, value]);
            assert!(Cli::try_parse_from(args).is_err(), "{flag}={value}");
        }
    }

    #[test]
    fn rejects_invalid_key_and_namespace() {
        let mut invalid_key = valid_args();
        let key_index = invalid_key
            .iter()
            .position(|arg| *arg == VALID_KEY)
            .unwrap();
        invalid_key[key_index] = "not-hex";
        assert!(Cli::try_parse_from(invalid_key).is_err());

        let mut invalid_namespace = valid_args();
        let namespace_index = invalid_namespace
            .iter()
            .position(|arg| *arg == "fibre-eval")
            .unwrap();
        invalid_namespace[namespace_index] = "short";
        assert!(Cli::try_parse_from(invalid_namespace).is_err());
    }

    #[test]
    fn payload_roundtrip_verifies() {
        let payload = make_payload(42, 1024);
        verify_payload(&payload, 42, 1024).unwrap();
    }

    #[test]
    fn payload_corruption_is_detected() {
        let payload = make_payload(42, 1024);

        let mut bad_magic = payload.clone();
        bad_magic[0] ^= 1;
        assert!(verify_payload(&bad_magic, 42, 1024).is_err());

        assert!(verify_payload(&payload, 43, 1024).is_err());
        assert!(verify_payload(&payload, 42, 1023).is_err());

        let mut bad_checksum = payload.clone();
        bad_checksum[12] ^= 1;
        assert!(verify_payload(&bad_checksum, 42, 1024).is_err());

        let mut bad_body = payload;
        bad_body[PAYLOAD_HEADER_LEN] ^= 1;
        assert!(verify_payload(&bad_body, 42, 1024).is_err());
    }

    #[test]
    fn stats_calculations_handle_empty_and_nonempty_inputs() {
        assert_eq!(success_percent(0, 0), 0.0);
        assert_eq!(success_percent(3, 4), 75.0);
        assert_eq!(per_second(100, Duration::ZERO), 0.0);
        assert_eq!(per_second(100, Duration::from_secs(4)), 25.0);
    }
}
