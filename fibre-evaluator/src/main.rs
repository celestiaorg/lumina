use std::collections::BTreeMap;
use std::future::IntoFuture;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use celestia_fibre::{
    Blob, BlobConfig, DEFAULT_PROTOCOL_PARAMS, DownloadOptions, FibreClient, FibreClientConfig,
    GrpcHostRegistry, GrpcSetGetter, GrpcValidatorConnector,
};
use celestia_grpc::{GrpcClient, TxConfig};
use celestia_proto::celestia::fibre::v1::MsgPayForFibre;
use celestia_types::nmt::Namespace;
use celestia_types::state::AccAddress;
use clap::Parser;
use k256::ecdsa::SigningKey;
use rand::RngCore;
use tokio::sync::mpsc::error::TrySendError;
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

    /// Target number of blob lifecycle launches per second.
    #[arg(long, value_parser = parse_blob_rate)]
    blobs_per_second: f64,

    /// Exact paid Fibre upload size of each blob in bytes.
    #[arg(long, value_parser = parse_blob_size)]
    blob_size: usize,

    /// Duration during which new jobs are scheduled.
    #[arg(long, value_parser = parse_positive_u64)]
    run_for_seconds: u64,

    /// Maximum concurrent end-to-end blob lifecycles.
    #[arg(long, default_value_t = 16, value_parser = parse_positive_usize)]
    max_in_flight: usize,

    /// Capacity of the admission queue.
    #[arg(long, default_value_t = 16, value_parser = parse_positive_usize)]
    queue_capacity: usize,

    /// Maximum concurrent payload encoding jobs.
    #[arg(long, default_value_t = 1, value_parser = parse_positive_usize)]
    encode_concurrency: usize,

    /// Maximum concurrent blob downloads.
    #[arg(long, default_value_t = 4, value_parser = parse_positive_usize)]
    download_concurrency: usize,

    /// Timeout applied separately to each network stage.
    #[arg(long, default_value_t = 120, value_parser = parse_positive_u64)]
    operation_timeout_seconds: u64,

    /// Fixed payment transaction gas limit. Dynamic estimation is used when omitted.
    #[arg(long, value_parser = parse_positive_u64)]
    gas_limit: Option<u64>,

    /// Fixed payment transaction gas price. Dynamic estimation is used when omitted.
    #[arg(long, value_parser = parse_positive_f64)]
    gas_price: Option<f64>,

    /// Interval between periodic statistics reports.
    #[arg(long, default_value_t = 10, value_parser = parse_positive_u64)]
    stats_interval_seconds: u64,
}

struct LifecycleContext {
    fibre: Arc<FibreClient>,
    app_grpc: GrpcClient,
    signing_key: SigningKey,
    signer: AccAddress,
    namespace: Namespace,
    payload_size: usize,
    paid_size: usize,
    operation_timeout: Duration,
    tx_config: TxConfig,
    encode_semaphore: Arc<Semaphore>,
    download_semaphore: Arc<Semaphore>,
}

struct WorkItem {
    sequence: u64,
    scheduled_at: Instant,
}

struct StageFailure {
    stage: &'static str,
    error: anyhow::Error,
}

enum Event {
    Scheduled {
        count: u64,
    },
    Admitted,
    Dropped {
        reason: &'static str,
        count: u64,
    },
    Started {
        queue_latency: Duration,
    },
    StageFinished {
        stage: &'static str,
        elapsed: Duration,
    },
    LifecycleSuccess {
        payload_bytes: u64,
        paid_bytes: u64,
        elapsed: Duration,
    },
    LifecycleFailure {
        stage: &'static str,
        error: String,
        elapsed: Duration,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Stats {
    scheduled: u64,
    admitted: u64,
    dropped_queue_full: u64,
    dropped_scheduler_late: u64,
    started: u64,
    successes: u64,
    failures: u64,
    payload_bytes: u64,
    paid_bytes: u64,
    failures_by_stage: BTreeMap<&'static str, u64>,
    latencies: BTreeMap<&'static str, Vec<Duration>>,
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
    let payload_size = payload_size_for_paid_size(cli.blob_size).expect("validated by clap");

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

    let mut tx_config = TxConfig::default();
    if let Some(gas_limit) = cli.gas_limit {
        tx_config = tx_config.with_gas_limit(gas_limit);
    }
    if let Some(gas_price) = cli.gas_price {
        tx_config = tx_config.with_gas_price(gas_price);
    }

    let context = Arc::new(LifecycleContext {
        fibre,
        app_grpc,
        signing_key,
        signer,
        namespace,
        payload_size,
        paid_size: cli.blob_size,
        operation_timeout: Duration::from_secs(cli.operation_timeout_seconds),
        tx_config,
        encode_semaphore: Arc::new(Semaphore::new(cli.encode_concurrency)),
        download_semaphore: Arc::new(Semaphore::new(cli.download_concurrency)),
    });

    tracing::info!(
        chain_id = %context.fibre.config().chain_id,
        namespace = %cli.namespace,
        payload_size = context.payload_size,
        paid_size = context.paid_size,
        blobs_per_second = cli.blobs_per_second,
        run_for_seconds = cli.run_for_seconds,
        max_in_flight = cli.max_in_flight,
        queue_capacity = cli.queue_capacity,
        encode_concurrency = cli.encode_concurrency,
        download_concurrency = cli.download_concurrency,
        gas_limit = ?cli.gas_limit,
        gas_price = ?cli.gas_price,
        "starting Fibre evaluation"
    );

    let started_at = Instant::now();
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let (work_tx, work_rx) = mpsc::channel(cli.queue_capacity);
    let stats_handle = tokio::spawn(run_stats_collector(
        event_rx,
        Duration::from_secs(cli.stats_interval_seconds),
        started_at,
    ));
    let dispatcher_handle = tokio::spawn(run_dispatcher(
        context,
        work_rx,
        event_tx.clone(),
        cli.max_in_flight,
    ));

    let launch_elapsed = run_scheduler(
        work_tx,
        event_tx.clone(),
        cli.blobs_per_second,
        Duration::from_secs(cli.run_for_seconds),
        started_at,
    )
    .await?;
    dispatcher_handle
        .await
        .context("dispatcher task panicked")??;
    let total_elapsed = started_at.elapsed();

    drop(event_tx);
    let stats = stats_handle.await.context("stats task panicked")?;
    print_final_report(&stats, launch_elapsed, total_elapsed);
    Ok(())
}

async fn run_scheduler(
    work_tx: mpsc::Sender<WorkItem>,
    event_tx: mpsc::UnboundedSender<Event>,
    blobs_per_second: f64,
    run_for: Duration,
    started_at: Instant,
) -> Result<Duration> {
    let period = submission_period(blobs_per_second);
    let deadline = started_at + run_for;
    let mut ticker = time::interval_at(started_at, period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Burst);
    let deadline_sleep = time::sleep_until(deadline);
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(deadline_sleep);
    tokio::pin!(ctrl_c);

    let mut sequence = 0u64;
    let stopped_at_deadline = loop {
        tokio::select! {
            biased;
            _ = &mut deadline_sleep => break true,
            result = &mut ctrl_c => {
                result.context("listening for Ctrl+C")?;
                tracing::info!("received Ctrl+C; stopping new submissions");
                break false;
            }
            scheduled_at = ticker.tick() => {
                sequence = sequence.checked_add(1).context("submission sequence overflow")?;
                let _ = event_tx.send(Event::Scheduled { count: 1 });

                if Instant::now().saturating_duration_since(scheduled_at) >= period {
                    let _ = event_tx.send(Event::Dropped {
                        reason: "scheduler_late",
                        count: 1,
                    });
                    continue;
                }

                admit_work(&work_tx, &event_tx, WorkItem { sequence, scheduled_at })?;
            }
        }
    };

    if stopped_at_deadline {
        let expected = expected_launches(blobs_per_second, run_for);
        let missed = expected.saturating_sub(sequence);
        if missed > 0 {
            let _ = event_tx.send(Event::Scheduled { count: missed });
            let _ = event_tx.send(Event::Dropped {
                reason: "scheduler_late",
                count: missed,
            });
        }
        Ok(run_for)
    } else {
        Ok(started_at.elapsed())
    }
}

fn admit_work(
    work_tx: &mpsc::Sender<WorkItem>,
    event_tx: &mpsc::UnboundedSender<Event>,
    item: WorkItem,
) -> Result<()> {
    match work_tx.try_send(item) {
        Ok(()) => {
            let _ = event_tx.send(Event::Admitted);
            Ok(())
        }
        Err(TrySendError::Full(_)) => {
            let _ = event_tx.send(Event::Dropped {
                reason: "queue_full",
                count: 1,
            });
            Ok(())
        }
        Err(TrySendError::Closed(_)) => Err(anyhow!("lifecycle dispatcher stopped")),
    }
}

async fn run_dispatcher(
    context: Arc<LifecycleContext>,
    mut work_rx: mpsc::Receiver<WorkItem>,
    event_tx: mpsc::UnboundedSender<Event>,
    max_in_flight: usize,
) -> Result<()> {
    let semaphore = Arc::new(Semaphore::new(max_in_flight));
    let mut tasks = JoinSet::new();

    loop {
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .context("lifecycle semaphore closed")?;
        let Some(item) = work_rx.recv().await else {
            drop(permit);
            break;
        };
        let context = Arc::clone(&context);
        let event_tx = event_tx.clone();
        tasks.spawn(async move {
            let _permit = permit;
            run_lifecycle_job(context, item, event_tx).await;
        });

        while let Some(result) = tasks.try_join_next() {
            if let Err(error) = result {
                tracing::error!(%error, "lifecycle task panicked");
            }
        }
    }

    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            tracing::error!(%error, "lifecycle task panicked");
        }
    }
    Ok(())
}

async fn run_lifecycle_job(
    context: Arc<LifecycleContext>,
    item: WorkItem,
    event_tx: mpsc::UnboundedSender<Event>,
) {
    let started_at = Instant::now();
    let _ = event_tx.send(Event::Started {
        queue_latency: started_at.saturating_duration_since(item.scheduled_at),
    });

    match run_lifecycle(&context, item.sequence, &event_tx).await {
        Ok(()) => {
            let _ = event_tx.send(Event::LifecycleSuccess {
                payload_bytes: context.payload_size as u64,
                paid_bytes: context.paid_size as u64,
                elapsed: started_at.elapsed(),
            });
        }
        Err(StageFailure { stage, error }) => {
            let _ = event_tx.send(Event::LifecycleFailure {
                stage,
                error: format!("{error:#}"),
                elapsed: started_at.elapsed(),
            });
        }
    }
}

async fn run_lifecycle(
    context: &LifecycleContext,
    sequence: u64,
    event_tx: &mpsc::UnboundedSender<Event>,
) -> std::result::Result<(), StageFailure> {
    let encode_started = Instant::now();
    let encode_permit = Arc::clone(&context.encode_semaphore)
        .acquire_owned()
        .await
        .map_err(|error| StageFailure {
            stage: "encode",
            error: anyhow!(error),
        })?;
    let payload_size = context.payload_size;
    let encoded = tokio::task::spawn_blocking(move || {
        let _permit = encode_permit;
        let payload = make_payload(sequence, payload_size);
        Blob::new_owned(payload, BlobConfig::v0())
    })
    .await;
    let _ = event_tx.send(Event::StageFinished {
        stage: "encode",
        elapsed: encode_started.elapsed(),
    });
    let blob = encoded
        .map_err(|error| StageFailure {
            stage: "encode",
            error: anyhow!(error),
        })?
        .map_err(|error| StageFailure {
            stage: "encode",
            error: anyhow!(error),
        })?;
    let id = blob.id().clone();

    let signed = run_timed_stage(
        "fibre_upload",
        context.operation_timeout,
        event_tx,
        context
            .fibre
            .upload(&context.signing_key, context.namespace, blob),
    )
    .await?;
    let message = MsgPayForFibre {
        signer: context.signer.to_string(),
        payment_promise: Some((&signed.promise).into()),
        validator_signatures: signed
            .validator_signatures
            .iter()
            .map(|signature| signature.clone().unwrap_or_default())
            .collect(),
    };

    let submitted = run_timed_stage(
        "payment_broadcast",
        context.operation_timeout,
        event_tx,
        context
            .app_grpc
            .broadcast_message(message, context.tx_config.clone()),
    )
    .await?;
    run_timed_stage(
        "payment_confirmation",
        context.operation_timeout,
        event_tx,
        submitted.confirm(),
    )
    .await?;

    let download_permit = Arc::clone(&context.download_semaphore)
        .acquire_owned()
        .await
        .map_err(|error| StageFailure {
            stage: "download",
            error: anyhow!(error),
        })?;
    let downloaded = run_timed_stage(
        "download",
        context.operation_timeout,
        event_tx,
        context.fibre.download(&id, DownloadOptions::default()),
    )
    .await;
    drop(download_permit);
    let blob = downloaded?;
    let data = blob.data().ok_or_else(|| StageFailure {
        stage: "download_verify",
        error: anyhow!("downloaded blob has no reconstructed data"),
    })?;
    verify_payload(data, sequence, context.payload_size).map_err(|error| StageFailure {
        stage: "download_verify",
        error: anyhow!(error),
    })?;
    Ok(())
}

async fn run_timed_stage<T, E, F>(
    stage: &'static str,
    timeout: Duration,
    event_tx: &mpsc::UnboundedSender<Event>,
    future: F,
) -> std::result::Result<T, StageFailure>
where
    E: std::error::Error + Send + Sync + 'static,
    F: IntoFuture<Output = std::result::Result<T, E>>,
{
    let started_at = Instant::now();
    let result = time::timeout(timeout, future).await;
    let _ = event_tx.send(Event::StageFinished {
        stage,
        elapsed: started_at.elapsed(),
    });

    match result {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(StageFailure {
            stage,
            error: anyhow!(error),
        }),
        Err(_) => Err(StageFailure {
            stage,
            error: anyhow!("operation exceeded {timeout:?}"),
        }),
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
            Event::Scheduled { count } => self.scheduled += count,
            Event::Admitted => self.admitted += 1,
            Event::Dropped { reason, count } => match reason {
                "queue_full" => self.dropped_queue_full += count,
                "scheduler_late" => self.dropped_scheduler_late += count,
                _ => unreachable!("unknown drop reason"),
            },
            Event::Started { queue_latency } => {
                self.started += 1;
                self.record_latency("queue", queue_latency);
            }
            Event::StageFinished { stage, elapsed } => self.record_latency(stage, elapsed),
            Event::LifecycleSuccess {
                payload_bytes,
                paid_bytes,
                elapsed,
            } => {
                self.successes += 1;
                self.payload_bytes += payload_bytes;
                self.paid_bytes += paid_bytes;
                self.record_latency("total", elapsed);
            }
            Event::LifecycleFailure {
                stage,
                error,
                elapsed,
            } => {
                self.failures += 1;
                *self.failures_by_stage.entry(stage).or_default() += 1;
                self.record_latency("total", elapsed);
                tracing::warn!(stage, %error, "blob lifecycle failed");
            }
        }
    }

    fn record_latency(&mut self, stage: &'static str, elapsed: Duration) {
        self.latencies.entry(stage).or_default().push(elapsed);
    }

    fn log_periodic(&self, elapsed: Duration) {
        let completed = self.successes + self.failures;
        tracing::info!(
            elapsed_seconds = %format_args!("{:.2}", elapsed.as_secs_f64()),
            scheduled = self.scheduled,
            admitted = self.admitted,
            dropped_queue_full = self.dropped_queue_full,
            dropped_scheduler_late = self.dropped_scheduler_late,
            queued = self.admitted.saturating_sub(self.started),
            in_flight = self.started.saturating_sub(completed),
            verified = self.successes,
            failed = self.failures,
            blobs_per_second = %format_args!("{:.2}", per_second(self.successes, elapsed)),
            paid_gib_per_second = %format_args!("{:.6}", per_second(self.paid_bytes, elapsed) / GIB),
            "periodic stats"
        );
    }
}

fn print_final_report(stats: &Stats, launch_elapsed: Duration, total_elapsed: Duration) {
    let dropped = stats.dropped_queue_full + stats.dropped_scheduler_late;
    let payload_bps = per_second(stats.payload_bytes, total_elapsed);
    let paid_bps = per_second(stats.paid_bytes, total_elapsed);

    tracing::info!("final Fibre evaluation stats");
    tracing::info!(
        scheduled = stats.scheduled,
        admitted = stats.admitted,
        dropped,
        dropped_queue_full = stats.dropped_queue_full,
        dropped_scheduler_late = stats.dropped_scheduler_late,
        launch_elapsed_seconds = launch_elapsed.as_secs_f64(),
        scheduled_per_second = per_second(stats.scheduled, launch_elapsed),
        admitted_per_second = per_second(stats.admitted, launch_elapsed),
        "admission stats"
    );
    tracing::info!(
        started = stats.started,
        verified = stats.successes,
        failed = stats.failures,
        success_percent = %format_args!(
            "{:.2}",
            success_percent(stats.successes, stats.successes + stats.failures)
        ),
        payload_bytes = stats.payload_bytes,
        paid_bytes = stats.paid_bytes,
        elapsed_seconds = total_elapsed.as_secs_f64(),
        blobs_per_second = per_second(stats.successes, total_elapsed),
        payload_mib_per_second = %format_args!("{:.4}", payload_bps / MIB),
        payload_gib_per_second = %format_args!("{:.6}", payload_bps / GIB),
        paid_mib_per_second = %format_args!("{:.4}", paid_bps / MIB),
        paid_gib_per_second = %format_args!("{:.6}", paid_bps / GIB),
        "end-to-end stats"
    );

    for (stage, failures) in &stats.failures_by_stage {
        tracing::info!(stage, failures, "stage failures");
    }
    for stage in [
        "queue",
        "encode",
        "fibre_upload",
        "payment_broadcast",
        "payment_confirmation",
        "download",
        "total",
    ] {
        let Some(samples) = stats.latencies.get(stage) else {
            continue;
        };
        tracing::info!(
            stage,
            samples = samples.len(),
            p50_ms = %format_args!("{:.3}", percentile_ms(samples, 0.50).unwrap()),
            p95_ms = %format_args!("{:.3}", percentile_ms(samples, 0.95).unwrap()),
            p99_ms = %format_args!("{:.3}", percentile_ms(samples, 0.99).unwrap()),
            "stage latency"
        );
    }
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

fn payload_size_for_paid_size(paid_size: usize) -> Result<usize, String> {
    let cfg = BlobConfig::v0();
    let header_size = DEFAULT_PROTOCOL_PARAMS.max_blob_size - cfg.max_data_size;
    let min_paid_size = cfg.upload_size(PAYLOAD_HEADER_LEN);
    let max_paid_size = cfg.upload_size(cfg.max_data_size);

    if !(min_paid_size..=max_paid_size).contains(&paid_size) {
        return Err(format!(
            "must be between {min_paid_size} and {max_paid_size} bytes"
        ));
    }

    let payload_size = paid_size - header_size;
    if cfg.upload_size(payload_size) != paid_size {
        return Err(format!(
            "must be a multiple of {min_paid_size} bytes to represent an exact paid Fibre size"
        ));
    }
    Ok(payload_size)
}

fn submission_period(blobs_per_second: f64) -> Duration {
    Duration::from_secs_f64(1.0 / blobs_per_second)
}

fn expected_launches(blobs_per_second: f64, run_for: Duration) -> u64 {
    (blobs_per_second * run_for.as_secs_f64()).ceil() as u64
}

fn percentile_ms(samples: &[Duration], percentile: f64) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = ((sorted.len() as f64 * percentile).ceil() as usize).clamp(1, sorted.len());
    Some(sorted[rank - 1].as_secs_f64() * 1000.0)
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
    payload_size_for_paid_size(size)?;
    Ok(size)
}

fn parse_positive_f64(value: &str) -> Result<f64, String> {
    let value = value
        .parse::<f64>()
        .map_err(|error| format!("invalid number: {error}"))?;
    if !value.is_finite() || value <= 0.0 {
        Err("must be finite and greater than zero".to_string())
    } else {
        Ok(value)
    }
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
            "134217728",
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
        assert_eq!(cli.blob_size, 134_217_728);
        assert_eq!(cli.max_in_flight, 16);
        assert_eq!(cli.queue_capacity, 16);
        assert_eq!(cli.encode_concurrency, 1);
        assert_eq!(cli.download_concurrency, 4);
        assert_eq!(cli.operation_timeout_seconds, 120);
        assert_eq!(cli.gas_limit, None);
        assert_eq!(cli.gas_price, None);
        assert_eq!(cli.stats_interval_seconds, 10);
    }

    #[test]
    fn maps_exact_paid_sizes_to_payload_sizes() {
        assert_eq!(payload_size_for_paid_size(262_144).unwrap(), 262_139);
        assert_eq!(
            payload_size_for_paid_size(134_217_728).unwrap(),
            134_217_723
        );
        assert!(payload_size_for_paid_size(262_143).is_err());
        assert!(payload_size_for_paid_size(262_145).is_err());
        assert!(payload_size_for_paid_size(134_217_729).is_err());
    }

    #[test]
    fn rejects_invalid_workload_arguments() {
        for (flag, value) in [
            ("--blobs-per-second", "0"),
            ("--blobs-per-second", "NaN"),
            ("--blob-size", "262145"),
            ("--run-for-seconds", "0"),
            ("--max-in-flight", "0"),
            ("--queue-capacity", "0"),
            ("--encode-concurrency", "0"),
            ("--download-concurrency", "0"),
            ("--operation-timeout-seconds", "0"),
            ("--gas-limit", "0"),
            ("--gas-price", "NaN"),
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
    fn stats_track_admission_failures_and_latencies() {
        let mut stats = Stats::default();
        stats.apply(Event::Scheduled { count: 4 });
        stats.apply(Event::Admitted);
        stats.apply(Event::Dropped {
            reason: "queue_full",
            count: 2,
        });
        stats.apply(Event::Dropped {
            reason: "scheduler_late",
            count: 1,
        });
        stats.apply(Event::Started {
            queue_latency: Duration::from_millis(2),
        });
        stats.apply(Event::LifecycleFailure {
            stage: "download",
            error: "failed".to_string(),
            elapsed: Duration::from_millis(10),
        });

        assert_eq!(stats.scheduled, 4);
        assert_eq!(stats.admitted, 1);
        assert_eq!(stats.dropped_queue_full, 2);
        assert_eq!(stats.dropped_scheduler_late, 1);
        assert_eq!(stats.failures_by_stage["download"], 1);
        assert_eq!(stats.latencies["queue"], [Duration::from_millis(2)]);
        assert_eq!(stats.latencies["total"], [Duration::from_millis(10)]);
    }

    #[test]
    fn admission_drops_instead_of_waiting_for_queue_capacity() {
        let (work_tx, mut work_rx) = mpsc::channel(1);
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let scheduled_at = Instant::now();

        admit_work(
            &work_tx,
            &event_tx,
            WorkItem {
                sequence: 1,
                scheduled_at,
            },
        )
        .unwrap();
        admit_work(
            &work_tx,
            &event_tx,
            WorkItem {
                sequence: 2,
                scheduled_at,
            },
        )
        .unwrap();

        assert_eq!(work_rx.try_recv().unwrap().sequence, 1);
        assert!(matches!(event_rx.try_recv(), Ok(Event::Admitted)));
        assert!(matches!(
            event_rx.try_recv(),
            Ok(Event::Dropped {
                reason: "queue_full",
                count: 1
            })
        ));
    }

    #[test]
    fn percentile_and_rate_calculations_handle_boundaries() {
        let samples = [
            Duration::from_millis(1),
            Duration::from_millis(2),
            Duration::from_millis(3),
            Duration::from_millis(4),
        ];
        assert_eq!(percentile_ms(&[], 0.5), None);
        assert_eq!(percentile_ms(&samples, 0.5), Some(2.0));
        assert_eq!(percentile_ms(&samples, 0.95), Some(4.0));
        assert_eq!(expected_launches(2.5, Duration::from_secs(60)), 150);
        assert_eq!(success_percent(0, 0), 0.0);
        assert_eq!(success_percent(3, 4), 75.0);
        assert_eq!(per_second(100, Duration::ZERO), 0.0);
        assert_eq!(per_second(100, Duration::from_secs(4)), 25.0);
    }
}
