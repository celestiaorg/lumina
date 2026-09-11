use std::collections::{BTreeMap, HashSet};
use std::future::IntoFuture;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use celestia_fibre::{
    BlobConfig, BlobID, DownloadOptions, EncodedBlob, FibreClient, FibreClientConfig,
    GrpcHostRegistry, GrpcSetGetter, GrpcValidatorConnector,
};
use celestia_grpc::{GrpcClient, TxConfig};
use celestia_proto::celestia::fibre::v1::MsgPayForFibre;
use celestia_proto::cosmos::tx::v1beta1::{GetTxsEventRequest, OrderBy, Tx as CosmosTx};
use celestia_types::nmt::Namespace;
use celestia_types::state::AccAddress;
use k256::ecdsa::SigningKey;
use prost::{Message, Name};
use tokio::net::TcpListener;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::{self, Instant, MissedTickBehavior};

use crate::cli::Cli;
use crate::metrics;
use crate::payload::{
    make_payload, payload_size_for_paid_size, verify_payload, verify_payload_integrity,
};
use crate::stats::{Event, SharedStats, Stats, print_final_report, run_stats_collector};

struct LifecycleContext {
    client: usize,
    fibre: Arc<FibreClient>,
    workload: Workload,
    operation_timeout: Duration,
    download_semaphore: Arc<Semaphore>,
}

enum Workload {
    Writer(Box<WriterContext>),
    Reader(ReaderContext),
}

struct WriterContext {
    app_grpc: GrpcClient,
    signing_key: SigningKey,
    signer: AccAddress,
    namespace: Namespace,
    payload_size: usize,
    paid_size: usize,
    tx_config: TxConfig,
    encode_pool: Arc<rayon::ThreadPool>,
    encode_semaphore: Arc<Semaphore>,
    skip_download: bool,
}

struct ReaderContext {
    app_grpc: GrpcClient,
    namespace: Namespace,
    verify_crc: bool,
}

struct CompletedBlob {
    payload_bytes: u64,
    paid_bytes: u64,
}

struct WorkItem {
    work: Work,
    scheduled_at: Instant,
}

enum Work {
    Write(u64),
    Read(BlobID),
}

struct StageFailure {
    stage: &'static str,
    error: anyhow::Error,
}

pub(crate) async fn run(cli: Cli) -> Result<()> {
    let cli = Arc::new(cli);
    let client_count = if cli.reader_only {
        1
    } else {
        cli.private_keys.len()
    };
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let mut contexts = Vec::with_capacity(client_count);
    let mut signers = BTreeMap::new();

    for client in 1..=client_count {
        let context = Arc::new(
            build_lifecycle_context(client, &cli)
                .with_context(|| format!("client {client} setup failed"))?,
        );
        let identity = match &context.workload {
            Workload::Writer(writer) => writer.signer.to_string(),
            Workload::Reader(_) => "reader".to_string(),
        };
        tracing::info!(client, %identity, "Fibre client ready");
        signers.insert(client, identity);
        contexts.push(context);
    }

    let payload_size = cli
        .blob_size
        .map(|size| payload_size_for_paid_size(size).expect("validated by clap"));
    let blobs_per_second = cli.blobs_per_second.unwrap_or(0.0);
    tracing::info!(
        clients = client_count,
        reader_only = cli.reader_only,
        verify_crc = cli.verify_crc,
        chain_id = %cli.chain_id,
        namespace = %cli.namespace,
        ?payload_size,
        paid_size = ?cli.blob_size,
        per_client_target_blobs_per_second = blobs_per_second,
        aggregate_target_blobs_per_second = blobs_per_second * client_count as f64,
        run_for_seconds = cli.run_for_seconds,
        max_in_flight = cli.max_in_flight,
        queue_capacity = cli.queue_capacity,
        encode_concurrency = cli.encode_concurrency,
        tokio_worker_threads = cli.tokio_worker_threads,
        rayon_threads_per_signer = cli.rayon_threads_per_signer,
        download_concurrency = cli.download_concurrency,
        download_enabled = cli.reader_only || !cli.skip_download,
        gas_limit = ?cli.gas_limit,
        gas_price = ?cli.gas_price,
        "starting Fibre evaluation"
    );

    let mut start_txs = Vec::with_capacity(client_count);
    let mut client_tasks = JoinSet::new();
    for context in contexts {
        let (start_tx, start_rx) = oneshot::channel();
        start_txs.push(start_tx);
        let client = context.client;
        let cli = Arc::clone(&cli);
        let event_tx = event_tx.clone();
        client_tasks.spawn(async move {
            let result = async {
                let started_at = start_rx
                    .await
                    .context("coordinator stopped before workload launch")?;
                run_client(context, &cli, event_tx, started_at).await
            }
            .await;
            (client, result)
        });
    }

    let metrics_listener = TcpListener::bind(cli.metrics_listen_addr)
        .await
        .with_context(|| format!("binding metrics endpoint to {}", cli.metrics_listen_addr))?;
    tracing::info!(address = %cli.metrics_listen_addr, path = "/metrics", "Prometheus metrics ready");

    let started_at = Instant::now();
    let shared_stats: SharedStats = Arc::new(RwLock::new(Stats::default()));
    let metrics_handle = tokio::spawn(metrics::serve(
        metrics_listener,
        Arc::clone(&shared_stats),
        started_at,
        client_count,
        blobs_per_second,
    ));
    let stats_handle = tokio::spawn(run_stats_collector(
        event_rx,
        Duration::from_secs(cli.stats_interval_seconds),
        started_at,
        client_count,
        blobs_per_second,
        shared_stats,
    ));

    let mut client_error = None;
    for (client, start_tx) in start_txs.into_iter().enumerate() {
        if start_tx.send(started_at).is_err() {
            let client = client + 1;
            client_error
                .get_or_insert_with(|| anyhow!("client {client} stopped before workload launch"));
        }
    }

    let mut client_results = Vec::with_capacity(client_count);
    while let Some(result) = client_tasks.join_next().await {
        client_results.push(result.context("client task panicked")?);
    }
    let total_elapsed = started_at.elapsed();
    drop(event_tx);
    let stats = stats_handle.await.context("stats task panicked")?;

    let mut launch_elapsed = Duration::ZERO;
    for (client, result) in client_results {
        match result {
            Ok(elapsed) => launch_elapsed = launch_elapsed.max(elapsed),
            Err(error) => {
                let signer = signers
                    .get(&client)
                    .map(String::as_str)
                    .unwrap_or("unknown");
                tracing::error!(client, signer, %error, "Fibre client failed");
                client_error.get_or_insert_with(|| anyhow!("client {client} failed: {error:#}"));
            }
        }
    }

    print_final_report(
        &stats,
        launch_elapsed,
        total_elapsed,
        client_count,
        blobs_per_second,
    );
    metrics_handle.abort();
    if let Some(error) = client_error {
        return Err(error);
    }
    Ok(())
}

fn build_lifecycle_context(client: usize, cli: &Cli) -> Result<LifecycleContext> {
    let private_key = cli
        .private_keys
        .get(client - 1)
        .map(|private_key| hex::decode(private_key).context("decoding --private-key"))
        .transpose()?;
    let mut app_grpc_builder = GrpcClient::builder().url(&cli.app_grpc_url);
    if let Some(private_key) = &private_key {
        app_grpc_builder = app_grpc_builder.private_key(private_key);
    }
    let app_grpc = app_grpc_builder
        .build()
        .context("building app gRPC client")?;
    let core_grpc = GrpcClient::builder()
        .url(&cli.core_grpc_url)
        .build()
        .context("building core gRPC client")?;

    let host_registry = Arc::new(GrpcHostRegistry::new(app_grpc.clone()));
    let fibre_config =
        FibreClientConfig::new(cli.chain_id.clone()).context("building Fibre client config")?;
    let fibre = Arc::new(
        FibreClient::builder()
            .config(fibre_config)
            .set_getter(GrpcSetGetter::new(core_grpc))
            .connector(GrpcValidatorConnector::new(
                host_registry,
                cli.chain_id.clone(),
            ))
            .build()
            .context("building Fibre client")?,
    );

    let namespace = Namespace::new_v0(cli.namespace.as_bytes()).context("parsing namespace")?;
    let workload = if cli.reader_only {
        Workload::Reader(ReaderContext {
            app_grpc,
            namespace,
            verify_crc: cli.verify_crc,
        })
    } else {
        let private_key = private_key.expect("required by clap in writer mode");
        let signing_key = SigningKey::from_slice(&private_key).context("parsing --private-key")?;
        let signer = app_grpc
            .get_account_address()
            .context("app gRPC client has no signer")?;
        let paid_size = cli.blob_size.expect("required by clap in writer mode");
        let payload_size = payload_size_for_paid_size(paid_size).expect("validated by clap");

        let mut tx_config = TxConfig::default();
        if let Some(gas_limit) = cli.gas_limit {
            tx_config = tx_config.with_gas_limit(gas_limit);
        }
        if let Some(gas_price) = cli.gas_price {
            tx_config = tx_config.with_gas_price(gas_price);
        }

        let encode_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(cli.rayon_threads_per_signer)
            .thread_name(move |worker| format!("fibre-encode-{client}-{worker}"))
            .build()
            .context("building signer Rayon pool")?;

        Workload::Writer(Box::new(WriterContext {
            app_grpc,
            signing_key,
            signer,
            namespace,
            payload_size,
            paid_size,
            tx_config,
            encode_pool: Arc::new(encode_pool),
            encode_semaphore: Arc::new(Semaphore::new(cli.encode_concurrency)),
            skip_download: cli.skip_download,
        }))
    };

    Ok(LifecycleContext {
        client,
        fibre,
        workload,
        operation_timeout: Duration::from_secs(cli.operation_timeout_seconds),
        download_semaphore: Arc::new(Semaphore::new(cli.download_concurrency)),
    })
}

async fn run_client(
    context: Arc<LifecycleContext>,
    cli: &Cli,
    event_tx: mpsc::UnboundedSender<Event>,
    started_at: Instant,
) -> Result<Duration> {
    let (work_tx, work_rx) = mpsc::channel(cli.queue_capacity);
    let dispatcher_handle = tokio::spawn(run_dispatcher(
        Arc::clone(&context),
        work_rx,
        event_tx.clone(),
        cli.max_in_flight,
    ));
    let run_for = Duration::from_secs(cli.run_for_seconds);
    let launch_elapsed = match &context.workload {
        Workload::Writer(_) => {
            run_scheduler(
                work_tx,
                event_tx,
                cli.blobs_per_second
                    .expect("required by clap in writer mode"),
                run_for,
                started_at,
            )
            .await?
        }
        Workload::Reader(reader) => {
            run_reader_source(work_tx, event_tx, reader, run_for, started_at).await?
        }
    };
    dispatcher_handle
        .await
        .context("dispatcher task panicked")??;
    Ok(launch_elapsed)
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

                admit_work(
                    &work_tx,
                    &event_tx,
                    WorkItem {
                        work: Work::Write(sequence),
                        scheduled_at,
                    },
                )?;
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

async fn run_reader_source(
    work_tx: mpsc::Sender<WorkItem>,
    event_tx: mpsc::UnboundedSender<Event>,
    reader: &ReaderContext,
    run_for: Duration,
    started_at: Instant,
) -> Result<Duration> {
    let deadline = started_at + run_for;
    let deadline_sleep = time::sleep_until(deadline);
    let ctrl_c = tokio::signal::ctrl_c();
    let mut poll = time::interval_at(started_at, Duration::from_secs(1));
    poll.set_missed_tick_behavior(MissedTickBehavior::Skip);
    tokio::pin!(deadline_sleep);
    tokio::pin!(ctrl_c);

    let mut page = 1;
    let mut seen = HashSet::new();
    let stopped_at_deadline = loop {
        tokio::select! {
            biased;
            _ = &mut deadline_sleep => break true,
            result = &mut ctrl_c => {
                result.context("listening for Ctrl+C")?;
                tracing::info!("received Ctrl+C; stopping namespace reads");
                break false;
            }
            _ = poll.tick() => {
                read_available_blobs(
                    &work_tx,
                    &event_tx,
                    reader,
                    &mut page,
                    &mut seen,
                ).await?;
            }
        }
    };

    Ok(if stopped_at_deadline {
        run_for
    } else {
        started_at.elapsed()
    })
}

async fn read_available_blobs(
    work_tx: &mpsc::Sender<WorkItem>,
    event_tx: &mpsc::UnboundedSender<Event>,
    reader: &ReaderContext,
    page: &mut u64,
    seen: &mut HashSet<BlobID>,
) -> Result<()> {
    const PAGE_LIMIT: u64 = 100;

    loop {
        let response = reader
            .app_grpc
            .get_txs_event(GetTxsEventRequest {
                order_by: OrderBy::Asc.into(),
                page: *page,
                limit: PAGE_LIMIT,
                query: format!("message.action='{}'", MsgPayForFibre::type_url()),
                ..Default::default()
            })
            .await
            .context("querying Fibre payments")?;

        for blob_id in blob_ids_in_namespace(response.txs, reader.namespace)? {
            if seen.contains(&blob_id) {
                continue;
            }
            let _ = event_tx.send(Event::Scheduled { count: 1 });
            work_tx
                .send(WorkItem {
                    work: Work::Read(blob_id.clone()),
                    scheduled_at: Instant::now(),
                })
                .await
                .context("lifecycle dispatcher stopped")?;
            let _ = event_tx.send(Event::Admitted);
            seen.insert(blob_id);
        }

        if page.saturating_mul(PAGE_LIMIT) >= response.total {
            return Ok(());
        }
        *page += 1;
    }
}

fn blob_ids_in_namespace(txs: Vec<CosmosTx>, namespace: Namespace) -> Result<Vec<BlobID>> {
    let mut blob_ids = Vec::new();
    for tx in txs {
        let Some(body) = tx.body else {
            continue;
        };
        for message in body.messages {
            if message.type_url != MsgPayForFibre::type_url() {
                continue;
            }
            let message = MsgPayForFibre::decode(message.value.as_slice())
                .context("decoding MsgPayForFibre")?;
            let promise = message
                .payment_promise
                .context("MsgPayForFibre has no payment promise")?;
            if promise.namespace != namespace.as_bytes() {
                continue;
            }
            let version = u8::try_from(promise.blob_version).context("invalid blob version")?;
            let commitment = promise
                .commitment
                .try_into()
                .map_err(|commitment: Vec<u8>| {
                    anyhow!("commitment must be 32 bytes, got {}", commitment.len())
                })?;
            let blob_id = BlobID::new(version, commitment);
            blob_id.validate().context("invalid blob ID")?;
            blob_ids.push(blob_id);
        }
    }
    Ok(blob_ids)
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

    match run_lifecycle(&context, item.work, &event_tx).await {
        Ok(completed) => {
            let _ = event_tx.send(Event::LifecycleSuccess {
                payload_bytes: completed.payload_bytes,
                paid_bytes: completed.paid_bytes,
                elapsed: started_at.elapsed(),
            });
        }
        Err(StageFailure { stage, error }) => {
            let identity = match &context.workload {
                Workload::Writer(writer) => writer.signer.to_string(),
                Workload::Reader(_) => "reader".to_string(),
            };
            let _ = event_tx.send(Event::LifecycleFailure {
                client: context.client,
                signer: identity,
                stage,
                error: format!("{error:#}"),
                elapsed: started_at.elapsed(),
            });
        }
    }
}

async fn run_lifecycle(
    context: &LifecycleContext,
    work: Work,
    event_tx: &mpsc::UnboundedSender<Event>,
) -> std::result::Result<CompletedBlob, StageFailure> {
    match (&context.workload, work) {
        (Workload::Writer(writer), Work::Write(sequence)) => {
            run_writer_lifecycle(context, writer, sequence, event_tx).await
        }
        (Workload::Reader(reader), Work::Read(blob_id)) => {
            run_reader_lifecycle(context, reader, &blob_id, event_tx).await
        }
        _ => unreachable!("work item must match evaluator mode"),
    }
}

async fn run_writer_lifecycle(
    context: &LifecycleContext,
    writer: &WriterContext,
    sequence: u64,
    event_tx: &mpsc::UnboundedSender<Event>,
) -> std::result::Result<CompletedBlob, StageFailure> {
    let encode_wait_started = Instant::now();
    let encode_permit = Arc::clone(&writer.encode_semaphore)
        .acquire_owned()
        .await
        .map_err(|error| StageFailure {
            stage: "encode_wait",
            error: anyhow!(error),
        })?;
    let _ = event_tx.send(Event::StageFinished {
        stage: "encode_wait",
        elapsed: encode_wait_started.elapsed(),
    });

    let encode_compute_started = Instant::now();
    let payload_size = writer.payload_size;
    let encode_pool = Arc::clone(&writer.encode_pool);
    let encoded = tokio::task::spawn_blocking(move || {
        let _permit = encode_permit;
        let payload = make_payload(sequence, payload_size);
        encode_pool.install(|| EncodedBlob::new(&payload, BlobConfig::v0()))
    })
    .await;
    let _ = event_tx.send(Event::StageFinished {
        stage: "encode_compute",
        elapsed: encode_compute_started.elapsed(),
    });
    let blob = encoded
        .map_err(|error| StageFailure {
            stage: "encode_compute",
            error: anyhow!(error),
        })?
        .map_err(|error| StageFailure {
            stage: "encode_compute",
            error: anyhow!(error),
        })?;
    let id = blob.id().clone();

    let full_fanout_started = Instant::now();
    let (signed, completion) = run_timed_stage(
        "fibre_upload",
        context.operation_timeout,
        event_tx,
        context
            .fibre
            .upload_with_completion(&writer.signing_key, writer.namespace, blob),
    )
    .await?;
    let lifecycle_result = async {
        let message = MsgPayForFibre {
            signer: writer.signer.to_string(),
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
            writer
                .app_grpc
                .broadcast_message(message, writer.tx_config.clone()),
        )
        .await?;
        run_timed_stage(
            "payment_confirmation",
            context.operation_timeout,
            event_tx,
            submitted.confirm(),
        )
        .await?;

        if writer.skip_download {
            return Ok(());
        }

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
        verify_payload(blob.data(), sequence, writer.payload_size).map_err(|error| {
            StageFailure {
                stage: "download_verify",
                error: anyhow!(error),
            }
        })?;
        Ok(())
    }
    .await;

    let fanout = completion.wait().await;
    let _ = event_tx.send(Event::StageFinished {
        stage: "full_fanout",
        elapsed: full_fanout_started.elapsed(),
    });
    if fanout.ignored_already_processed > 0 {
        let _ = event_tx.send(Event::IgnoredValidatorUploads {
            count: fanout.ignored_already_processed as u64,
        });
    }

    lifecycle_result?;
    if fanout.failed > 0 {
        return Err(StageFailure {
            stage: "full_fanout",
            error: anyhow!(
                "{} of {} validator uploads failed",
                fanout.failed,
                fanout.successful + fanout.failed + fanout.ignored_already_processed
            ),
        });
    }
    Ok(CompletedBlob {
        payload_bytes: writer.payload_size as u64,
        paid_bytes: writer.paid_size as u64,
    })
}

async fn run_reader_lifecycle(
    context: &LifecycleContext,
    reader: &ReaderContext,
    blob_id: &BlobID,
    event_tx: &mpsc::UnboundedSender<Event>,
) -> std::result::Result<CompletedBlob, StageFailure> {
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
        context.fibre.download(blob_id, DownloadOptions::default()),
    )
    .await;
    drop(download_permit);
    let blob = downloaded?;
    if reader.verify_crc {
        verify_payload_integrity(blob.data()).map_err(|error| StageFailure {
            stage: "download_verify",
            error: anyhow!(error),
        })?;
    }

    Ok(CompletedBlob {
        payload_bytes: blob.data().len() as u64,
        paid_bytes: BlobConfig::for_version(blob_id.version())
            .expect("validated during discovery")
            .upload_size(blob.data().len()) as u64,
    })
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

fn submission_period(blobs_per_second: f64) -> Duration {
    Duration::from_secs_f64(1.0 / blobs_per_second)
}

fn expected_launches(blobs_per_second: f64, run_for: Duration) -> u64 {
    (blobs_per_second * run_for.as_secs_f64()).ceil() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use celestia_grpc::IntoProtobufAny;
    use celestia_proto::celestia::fibre::v1::PaymentPromise;
    use celestia_proto::cosmos::tx::v1beta1::TxBody;

    #[test]
    fn expected_launch_count_rounds_up() {
        assert_eq!(expected_launches(2.5, Duration::from_secs(60)), 150);
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
                work: Work::Write(1),
                scheduled_at,
            },
        )
        .unwrap();
        admit_work(
            &work_tx,
            &event_tx,
            WorkItem {
                work: Work::Write(2),
                scheduled_at,
            },
        )
        .unwrap();

        assert!(matches!(work_rx.try_recv().unwrap().work, Work::Write(1)));
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
    fn extracts_only_fibre_blobs_in_namespace() {
        let namespace = Namespace::new_v0(b"fibre-eval").unwrap();
        let other_namespace = Namespace::new_v0(b"other-data").unwrap();
        let tx = |namespace: Namespace, commitment: u8| CosmosTx {
            body: Some(TxBody {
                messages: vec![
                    MsgPayForFibre {
                        payment_promise: Some(PaymentPromise {
                            namespace: namespace.as_bytes().to_vec(),
                            blob_version: 0,
                            commitment: vec![commitment; 32],
                            ..Default::default()
                        }),
                        ..Default::default()
                    }
                    .into_any(),
                ],
                ..Default::default()
            }),
            ..Default::default()
        };

        let ids = blob_ids_in_namespace(vec![tx(namespace, 1), tx(other_namespace, 2)], namespace)
            .unwrap();

        assert_eq!(ids, [BlobID::new(0, [1; 32])]);
    }
}
