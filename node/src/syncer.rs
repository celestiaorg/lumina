//! Component responsible for synchronizing block headers announced in the Celestia network.

use std::marker::PhantomData;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use backoff::ExponentialBackoffBuilder;
use backoff::backoff::Backoff;
use celestia_types::ExtendedHeader;
use lumina_utils::executor::{JoinHandle, spawn};
use lumina_utils::time::{Instant, Interval, sleep};
use serde::{Deserialize, Serialize};
use tendermint::Time;
use tokio::select;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::block_ranges::{BlockRange, BlockRangeExt, BlockRanges};
use crate::events::{EventPublisher, NodeEvent};
use crate::node::subscriptions::BroadcastingStore;
use crate::p2p::{P2p, P2pError};
use crate::store::{Store, StoreError};
use crate::utils::{FusedReusableFuture, OneshotSenderExt, TimeExt};

type Result<T, E = SyncerError> = std::result::Result<T, E>;

const TRY_INIT_BACKOFF_MAX_INTERVAL: Duration = Duration::from_secs(60);
const SLOW_SYNC_MIN_THRESHOLD: u64 = 50;

/// Representation of all the errors that can occur in `Syncer` component.
#[derive(Debug, thiserror::Error)]
pub enum SyncerError {
    /// An error propagated from the `P2p` component.
    #[error("P2p: {0}")]
    P2p(#[from] P2pError),

    /// An error propagated from the [`Store`] component.
    #[error("Store: {0}")]
    Store(#[from] StoreError),

    /// The worker has died.
    #[error("Worker died")]
    WorkerDied,

    /// Channel closed unexpectedly.
    #[error("Channel closed unexpectedly")]
    ChannelClosedUnexpectedly,
}

impl SyncerError {
    pub(crate) fn is_fatal(&self) -> bool {
        match self {
            SyncerError::P2p(e) => e.is_fatal(),
            SyncerError::Store(e) => e.is_fatal(),
            SyncerError::WorkerDied | SyncerError::ChannelClosedUnexpectedly => true,
        }
    }
}

impl From<oneshot::error::RecvError> for SyncerError {
    fn from(_value: oneshot::error::RecvError) -> Self {
        SyncerError::ChannelClosedUnexpectedly
    }
}

/// Component responsible for synchronizing block headers from the network.
#[derive(Debug)]
pub(crate) struct Syncer<S>
where
    S: Store + 'static,
{
    cmd_tx: mpsc::Sender<SyncerCmd>,
    cancellation_token: CancellationToken,
    join_handle: JoinHandle,
    _store: PhantomData<S>,
}

/// Arguments used to configure the [`Syncer`].
pub(crate) struct SyncerArgs<S>
where
    S: Store + 'static,
{
    /// Handler for the peer to peer messaging.
    pub(crate) p2p: Arc<P2p>,
    /// Headers storage.
    pub(crate) store: Arc<S>,
    /// Event publisher.
    pub(crate) event_pub: EventPublisher,
    /// Batch size.
    pub(crate) batch_size: u64,
    /// Sampling window
    pub(crate) sampling_window: Duration,
    /// Pruning window
    pub(crate) pruning_window: Duration,
}

#[derive(Debug)]
enum SyncerCmd {
    GetInfo {
        respond_to: oneshot::Sender<SyncingInfo>,
    },
    SubscribeHeights {
        respond_to: oneshot::Sender<broadcast::Receiver<ExtendedHeader>>,
    },
    #[cfg(test)]
    TriggerFetchNextBatch,
}

/// Status of the synchronization.
#[derive(Debug, Serialize, Deserialize)]
pub struct SyncingInfo {
    /// Ranges of headers that are already synchronised
    pub stored_headers: BlockRanges,
    /// Syncing target. The latest height seen in the network that was successfully verified.
    pub subjective_head: u64,
}

impl<S> Syncer<S>
where
    S: Store,
{
    /// Create and start the [`Syncer`].
    pub(crate) fn start(args: SyncerArgs<S>) -> Result<Self> {
        let cancellation_token = CancellationToken::new();
        let event_pub = args.event_pub.clone();
        let (cmd_tx, cmd_rx) = mpsc::channel(16);
        let mut worker = Worker::new(args, cancellation_token.child_token(), cmd_rx)?;

        let join_handle = spawn(async move {
            if let Err(e) = worker.run().await {
                error!("Syncer stopped because of a fatal error: {e}");

                event_pub.send(NodeEvent::FatalSyncerError {
                    error: e.to_string(),
                });
            }
        });

        Ok(Syncer {
            cancellation_token,
            cmd_tx,
            join_handle,
            _store: PhantomData,
        })
    }

    /// Stop the worker.
    pub(crate) fn stop(&self) {
        // Signal the Worker to stop.
        self.cancellation_token.cancel();
    }

    /// Wait until worker is completely stopped.
    pub(crate) async fn join(&self) {
        self.join_handle.join().await;
    }

    async fn send_command(&self, cmd: SyncerCmd) -> Result<()> {
        self.cmd_tx
            .send(cmd)
            .await
            .map_err(|_| SyncerError::WorkerDied)
    }

    /// Get the current synchronization status.
    ///
    /// # Errors
    ///
    /// This function will return an error if the [`Syncer`] has been stopped.
    pub(crate) async fn info(&self) -> Result<SyncingInfo> {
        let (tx, rx) = oneshot::channel();

        self.send_command(SyncerCmd::GetInfo { respond_to: tx })
            .await?;

        Ok(rx.await?)
    }

    /// Subscribe to a broadcast of new header heights as they are being synchronised
    ///
    /// # Errors
    ///
    /// This function will return an error if the [`Syncer`] has been stopped.
    pub(crate) async fn subscribe_headers(&self) -> Result<broadcast::Receiver<ExtendedHeader>> {
        let (tx, rx) = oneshot::channel();

        self.send_command(SyncerCmd::SubscribeHeights { respond_to: tx })
            .await?;

        Ok(rx.await?)
    }

    #[cfg(test)]
    async fn trigger_fetch_next_batch(&self) -> Result<()> {
        self.send_command(SyncerCmd::TriggerFetchNextBatch).await
    }
}

impl<S> Drop for Syncer<S>
where
    S: Store,
{
    fn drop(&mut self) {
        self.stop();
    }
}

struct Worker<S>
where
    S: Store + 'static,
{
    cancellation_token: CancellationToken,
    cmd_rx: mpsc::Receiver<SyncerCmd>,
    event_pub: EventPublisher,
    p2p: Arc<P2p>,
    store: BroadcastingStore<S>,
    header_sub_rx: Option<mpsc::Receiver<ExtendedHeader>>,
    subjective_head_height: Option<u64>,
    highest_slow_sync_height: Option<u64>,
    batch_size: u64,
    ongoing_batch: Ongoing,
    sampling_window: Duration,
    pruning_window: Duration,
}

struct Ongoing {
    range: Option<BlockRange>,
    task: FusedReusableFuture<(Result<Vec<ExtendedHeader>, P2pError>, Duration)>,
}

impl<S> Worker<S>
where
    S: Store,
{
    fn new(
        args: SyncerArgs<S>,
        cancellation_token: CancellationToken,
        cmd_rx: mpsc::Receiver<SyncerCmd>,
    ) -> Result<Self> {
        Ok(Worker {
            cancellation_token,
            cmd_rx,
            event_pub: args.event_pub,
            p2p: args.p2p,
            store: BroadcastingStore::new(args.store),
            header_sub_rx: None,
            subjective_head_height: None,
            highest_slow_sync_height: None,
            batch_size: args.batch_size,
            ongoing_batch: Ongoing {
                range: None,
                task: FusedReusableFuture::terminated(),
            },
            sampling_window: args.sampling_window,
            pruning_window: args.pruning_window,
        })
    }

    async fn run(&mut self) -> Result<()> {
        loop {
            if self.cancellation_token.is_cancelled() {
                break;
            }

            self.connecting_event_loop().await?;

            if self.cancellation_token.is_cancelled() {
                break;
            }

            self.connected_event_loop().await?;
        }

        debug!("Syncer stopped");
        Ok(())
    }

    /// The responsibility of this event loop is to await a trusted peer to
    /// connect and get the network head, while accepting commands.
    ///
    /// NOTE: Only fatal errors should be propagated!
    async fn connecting_event_loop(&mut self) -> Result<()> {
        debug!("Entering connecting_event_loop");

        let mut report_interval = Interval::new(Duration::from_secs(60));
        self.report().await?;

        let mut try_init_fut = pin!(try_init_task(
            self.p2p.clone(),
            self.store.clone_inner_store(),
            self.event_pub.clone()
        ));

        loop {
            select! {
                _ = self.cancellation_token.cancelled() => {
                    break;
                }
                _ = report_interval.tick() => {
                    self.report().await?;
                }
                res = &mut try_init_fut => {
                    // `try_init_task` propagates only fatal errors
                    let (network_head, took) = res?;
                    let network_head_height = network_head.height();

                    info!("Setting initial subjective head to {network_head_height}");
                    self.set_subjective_head_height(network_head_height);
                    self.store.init_broadcast(network_head.clone());

                    let (header_sub_tx, header_sub_rx) = mpsc::channel(16);
                    self.p2p.init_header_sub(network_head, header_sub_tx).await?;
                    self.header_sub_rx = Some(header_sub_rx);

                    self.event_pub.send(NodeEvent::FetchingHeadHeaderFinished {
                        height: network_head_height,
                        took,
                    });

                    break;
                }
                Some(cmd) = self.cmd_rx.recv() => {
                    self.on_cmd(cmd).await?;
                }
            }
        }

        Ok(())
    }

    /// The responsibility of this event loop is to start the syncing process,
    /// handles events from HeaderSub, and accept commands.
    ///
    /// NOTE: Only fatal errors should be propagated!
    async fn connected_event_loop(&mut self) -> Result<()> {
        debug!("Entering connected_event_loop");

        let mut report_interval = Interval::new(Duration::from_secs(60));
        let mut peer_tracker_info_watcher = self.p2p.peer_tracker_info_watcher();

        // Check if connection status changed before creating the watcher
        if peer_tracker_info_watcher.borrow().num_connected_peers == 0 {
            warn!("All peers disconnected");
            return Ok(());
        }

        self.fetch_next_batch().await?;
        self.report().await?;

        loop {
            select! {
                _ = self.cancellation_token.cancelled() => {
                    break;
                }
                _ = peer_tracker_info_watcher.changed() => {
                    if peer_tracker_info_watcher.borrow().num_connected_peers == 0 {
                        warn!("All peers disconnected");
                        break;
                    }
                }
                _ = report_interval.tick() => {
                    self.report().await?;
                }
                res = header_sub_recv(self.header_sub_rx.as_mut()) => {
                    let header = res?;
                    self.on_header_sub_message(header).await?;
                    self.fetch_next_batch().await?;
                }
                Some(cmd) = self.cmd_rx.recv() => {
                    self.on_cmd(cmd).await?;
                }
                (res, took) = &mut self.ongoing_batch.task => {
                    self.on_fetch_next_batch_result(res, took).await?;
                    self.fetch_next_batch().await?;
                }
            }
        }

        if let Some(ongoing) = self.ongoing_batch.range.take() {
            warn!("Cancelling fetching of {}", ongoing.display());
            self.ongoing_batch.task.terminate();
        }

        self.header_sub_rx.take();

        Ok(())
    }

    async fn syncing_info(&self) -> Result<SyncingInfo> {
        Ok(SyncingInfo {
            stored_headers: self.store.get_stored_header_ranges().await?,
            subjective_head: self.subjective_head_height.unwrap_or(0),
        })
    }

    #[instrument(skip_all)]
    async fn report(&mut self) -> Result<()> {
        let SyncingInfo {
            stored_headers,
            subjective_head,
        } = self.syncing_info().await?;

        let ongoing_batch = self
            .ongoing_batch
            .range
            .as_ref()
            .map(|range| format!("{}", range.display()))
            .unwrap_or_else(|| "None".to_string());

        info!(
            "syncing: head: {subjective_head}, stored headers: {stored_headers}, ongoing batches: {ongoing_batch}"
        );
        Ok(())
    }

    async fn on_cmd(&mut self, cmd: SyncerCmd) -> Result<()> {
        match cmd {
            SyncerCmd::GetInfo { respond_to } => {
                let info = self.syncing_info().await?;
                respond_to.maybe_send(info);
            }
            SyncerCmd::SubscribeHeights { respond_to } => {
                let receiver = self.store.subscribe();
                respond_to.maybe_send(receiver);
            }
            #[cfg(test)]
            SyncerCmd::TriggerFetchNextBatch => {
                self.fetch_next_batch().await?;
            }
        }

        Ok(())
    }

    #[instrument(skip_all)]
    async fn on_header_sub_message(&mut self, new_head: ExtendedHeader) -> Result<()> {
        let new_head_height = new_head.height();

        self.set_subjective_head_height(new_head_height);

        if let Ok(store_head_height) = self.store.head_height().await {
            // If our new header is adjacent to the HEAD of the store
            if store_head_height + 1 == new_head_height {
                // Header is already verified by HeaderSub and will be validated against previous
                // head on insert
                if self.store.announce_insert(vec![new_head]).await.is_ok() {
                    self.event_pub.send(NodeEvent::AddedHeaderFromHeaderSub {
                        height: new_head_height,
                    });
                }
            }
        }

        Ok(())
    }

    fn set_subjective_head_height(&mut self, height: u64) {
        if self
            .subjective_head_height
            .is_some_and(|old_height| height <= old_height)
        {
            return;
        }

        self.subjective_head_height = Some(height);
    }

    #[instrument(skip_all)]
    async fn fetch_next_batch(&mut self) -> Result<()> {
        debug_assert_eq!(
            self.ongoing_batch.range.is_none(),
            self.ongoing_batch.task.is_terminated()
        );

        if !self.ongoing_batch.task.is_terminated() {
            // Another batch is ongoing. We do not parallelize `Syncer`
            // by design. Any parallel requests are done in the
            // HeaderEx client through `Session`.
            //
            // Nothing to schedule
            return Ok(());
        }

        if self.p2p.peer_tracker_info().num_connected_peers == 0 {
            // No connected peers. We can't do the request.
            // We will recover from this in `run`.
            return Ok(());
        }

        let Some(subjective_head_height) = self.subjective_head_height else {
            // Nothing to schedule
            return Ok(());
        };

        let store_ranges = self.store.get_stored_header_ranges().await?;
        let pruned_ranges = self.store.get_pruned_ranges().await?;

        // Pruner removes already sampled headers and creates gaps in the ranges.
        // Syncer must ignore those gaps.
        let synced_ranges = pruned_ranges + &store_ranges;

        let next_batch = calculate_range_to_fetch(
            subjective_head_height,
            synced_ranges.as_ref(),
            self.batch_size,
        );

        if next_batch.is_empty() {
            // no headers to fetch
            return Ok(());
        }

        // If all heights of next batch is within the slow sync range
        if self
            .highest_slow_sync_height
            .is_some_and(|height| *next_batch.end() <= height)
        {
            // Threshold is the half of batch size but it should be at least 50.
            let threshold = (self.batch_size / 2).max(SLOW_SYNC_MIN_THRESHOLD);

            // Calculate how many headers are locally available for sampling.
            let sampled_ranges = self.store.get_sampled_ranges().await?;
            let available_for_sampling = (store_ranges - sampled_ranges).len();

            // Do not fetch next batch if we have more headers than the threshold.
            if available_for_sampling > threshold {
                // NOTE: Recheck will happen when we receive a header from header sub (~6 secs).
                return Ok(());
            }
        }

        // make sure we're inside the sampling window before we start
        match self.store.get_by_height(next_batch.end() + 1).await {
            Ok(known_header) => {
                if !self.in_sampling_window(&known_header) {
                    return Ok(());
                }
            }
            Err(StoreError::NotFound) => {}
            Err(e) => return Err(e.into()),
        }

        self.event_pub.send(NodeEvent::FetchingHeadersStarted {
            from_height: *next_batch.start(),
            to_height: *next_batch.end(),
        });

        let p2p = self.p2p.clone();

        self.ongoing_batch.range = Some(next_batch.clone());

        self.ongoing_batch.task.set(async move {
            let now = Instant::now();
            let res = p2p.get_unverified_header_range(next_batch).await;
            (res, now.elapsed())
        });

        Ok(())
    }

    /// Handle the result of the batch request and propagate fatal errors.
    #[instrument(skip_all)]
    async fn on_fetch_next_batch_result(
        &mut self,
        res: Result<Vec<ExtendedHeader>, P2pError>,
        took: Duration,
    ) -> Result<()> {
        let range = self
            .ongoing_batch
            .range
            .take()
            .expect("ongoing_batch not initialized correctly");

        let from_height = *range.start();
        let to_height = *range.end();

        let headers = match res {
            Ok(headers) => headers,
            Err(e) => {
                if e.is_fatal() {
                    return Err(e.into());
                }

                self.event_pub.send(NodeEvent::FetchingHeadersFailed {
                    from_height,
                    to_height,
                    error: e.to_string(),
                    took,
                });

                return Ok(());
            }
        };

        let pruning_cutoff = Time::now().saturating_sub(self.pruning_window);

        // Iterate headers from highest to lowest and check if there is
        // a new highest "slow sync" height.
        for header in headers.iter().rev() {
            if self
                .highest_slow_sync_height
                .is_some_and(|height| header.height() <= height)
            {
                // `highest_slow_sync_height` is already higher than `header`
                // so we don't need to check anything else.
                break;
            }

            // If `header` is after the pruning edge, we mark it as the
            // `highest_slow_sync_height` and we stop checking lower headers.
            if header.time() <= pruning_cutoff {
                self.highest_slow_sync_height = Some(header.height());
                break;
            }
        }

        if let Err(e) = self.store.announce_insert(headers).await {
            if e.is_fatal() {
                return Err(e.into());
            }

            self.event_pub.send(NodeEvent::FetchingHeadersFailed {
                from_height,
                to_height,
                error: format!("Failed to store headers: {e}"),
                took,
            });
        }

        self.event_pub.send(NodeEvent::FetchingHeadersFinished {
            from_height,
            to_height,
            took,
        });

        Ok(())
    }

    fn in_sampling_window(&self, header: &ExtendedHeader) -> bool {
        let sampling_window_end = Time::now().saturating_sub(self.sampling_window);
        header.time().after(sampling_window_end)
    }
}

/// based on the synced headers and current network head height, calculate range of headers that
/// should be fetched from the network, anchored on already synced header range
fn calculate_range_to_fetch(
    subjective_head_height: u64,
    synced_headers: &[BlockRange],
    limit: u64,
) -> BlockRange {
    let mut synced_headers_iter = synced_headers.iter().rev();

    let Some(synced_head_range) = synced_headers_iter.next() else {
        // empty synced ranges, we're missing everything
        let range = 1..=subjective_head_height;
        return range.tailn(limit);
    };

    if synced_head_range.end() < &subjective_head_height {
        // if we haven't caught up with the network head, start from there
        let range = synced_head_range.end() + 1..=subjective_head_height;
        return range.tailn(limit);
    }

    // there exists a range contiguous with network head. inspect previous range end
    let penultimate_range_end = synced_headers_iter.next().map(|r| *r.end()).unwrap_or(0);

    let range = penultimate_range_end + 1..=synced_head_range.start().saturating_sub(1);
    range.headn(limit)
}

#[instrument(skip_all)]
async fn try_init_task<S>(
    p2p: Arc<P2p>,
    store: Arc<S>,
    event_pub: EventPublisher,
) -> Result<(ExtendedHeader, Duration)>
where
    S: Store + 'static,
{
    let now = Instant::now();
    let mut event_reported = false;
    let mut backoff = ExponentialBackoffBuilder::default()
        .with_max_interval(TRY_INIT_BACKOFF_MAX_INTERVAL)
        .with_max_elapsed_time(None)
        .build();

    loop {
        match try_init(&p2p, &*store, &event_pub, &mut event_reported).await {
            Ok(network_head) => {
                return Ok((network_head, now.elapsed()));
            }
            Err(e) if e.is_fatal() => {
                return Err(e);
            }
            Err(e) => {
                let sleep_dur = backoff
                    .next_backoff()
                    .expect("backoff never stops retrying");

                warn!(
                    "Initialization of subjective head failed: {e}. Trying again in {sleep_dur:?}."
                );
                sleep(sleep_dur).await;
            }
        }
    }
}

async fn try_init<S>(
    p2p: &P2p,
    store: &S,
    event_pub: &EventPublisher,
    event_reported: &mut bool,
) -> Result<ExtendedHeader>
where
    S: Store,
{
    p2p.wait_connected_trusted().await?;

    // Wait a bit to increase the possibility of multiple
    // trusted nodes to be connected. This benefits the
    // "best head" algorithm of HeaderEx. In test cases
    // this is not needed.
    #[cfg(not(test))]
    sleep(Duration::from_secs(1)).await;

    if !*event_reported {
        event_pub.send(NodeEvent::FetchingHeadHeaderStarted);
        *event_reported = true;
    }

    let network_head = p2p.get_head_header().await?;

    // If the network head and the store head have the same height,
    // then `insert` will error because of insertion constraints.
    // However, if both headers are the exactly the same, we
    // can skip inserting, as the header is already there.
    //
    // This can happen in case of fast node restart.
    let try_insert = match store.get_head().await {
        // `ExtendedHeader.commit.signatures` can be different set on each fetch
        // so we compare only hashes.
        Ok(store_head) => store_head.hash() != network_head.hash(),
        Err(StoreError::NotFound) => true,
        Err(e) => return Err(e.into()),
    };

    if try_insert {
        // Insert HEAD to the store and initialize header-sub.
        // Normal insertion checks still apply here.
        store.insert(network_head.clone()).await?;
    }

    Ok(network_head)
}

async fn header_sub_recv(
    rx: Option<&mut mpsc::Receiver<ExtendedHeader>>,
) -> Result<ExtendedHeader> {
    rx.expect("header-sub not initialized")
        .recv()
        .await
        .ok_or(SyncerError::P2p(P2pError::WorkerDied))
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;

    use super::*;
    use crate::block_ranges::{BlockRange, BlockRangeExt};
    use crate::events::EventChannel;
    use crate::node::{DEFAULT_PRUNING_WINDOW, HeaderExError, SAMPLING_WINDOW};
    use crate::p2p::header_session;
    use crate::store::InMemoryStore;
    use crate::test_utils::{MockP2pHandle, gen_filled_store};
    use celestia_types::test_utils::ExtendedHeaderGenerator;
    use libp2p::request_response::OutboundFailure;
    use lumina_utils::test_utils::async_test;
    use tokio::sync::broadcast::error::RecvError;

    #[test]
    fn calculate_range_to_fetch_test_header_limit() {
        let head_height = 1024;
        let ranges = [256..=512];

        let fetch_range = calculate_range_to_fetch(head_height, &ranges, 16);
        assert_eq!(fetch_range, 513..=528);

        let fetch_range = calculate_range_to_fetch(head_height, &ranges, 511);
        assert_eq!(fetch_range, 513..=1023);
        let fetch_range = calculate_range_to_fetch(head_height, &ranges, 512);
        assert_eq!(fetch_range, 513..=1024);
        let fetch_range = calculate_range_to_fetch(head_height, &ranges, 513);
        assert_eq!(fetch_range, 513..=1024);

        let fetch_range = calculate_range_to_fetch(head_height, &ranges, 1024);
        assert_eq!(fetch_range, 513..=1024);
    }

    #[test]
    fn calculate_range_to_fetch_empty_store() {
        let fetch_range = calculate_range_to_fetch(1, &[], 100);
        assert_eq!(fetch_range, 1..=1);

        let fetch_range = calculate_range_to_fetch(100, &[], 10);
        assert_eq!(fetch_range, 1..=10);

        let fetch_range = calculate_range_to_fetch(100, &[], 50);
        assert_eq!(fetch_range, 1..=50);
    }

    #[test]
    fn calculate_range_to_fetch_fully_synced() {
        let fetch_range = calculate_range_to_fetch(1, &[1..=1], 100);
        assert!(fetch_range.is_empty());

        let fetch_range = calculate_range_to_fetch(100, &[1..=100], 10);
        assert!(fetch_range.is_empty());

        let fetch_range = calculate_range_to_fetch(100, &[1..=100], 10);
        assert!(fetch_range.is_empty());
    }

    #[test]
    fn calculate_range_to_fetch_caught_up() {
        let head_height = 4000;

        let fetch_range = calculate_range_to_fetch(head_height, &[3000..=4000], 500);
        assert_eq!(fetch_range, 2500..=2999);
        let fetch_range = calculate_range_to_fetch(head_height, &[500..=1000, 3000..=4000], 500);
        assert_eq!(fetch_range, 2500..=2999);
        let fetch_range = calculate_range_to_fetch(head_height, &[2500..=2800, 3000..=4000], 500);
        assert_eq!(fetch_range, 2801..=2999);
        let fetch_range = calculate_range_to_fetch(head_height, &[2500..=2800, 3000..=4000], 500);
        assert_eq!(fetch_range, 2801..=2999);
        let fetch_range = calculate_range_to_fetch(head_height, &[300..=4000], 500);
        assert_eq!(fetch_range, 1..=299);
    }

    #[test]
    fn calculate_range_to_fetch_catching_up() {
        let head_height = 4000;

        let fetch_range = calculate_range_to_fetch(head_height, &[2000..=3000], 500);
        assert_eq!(fetch_range, 3001..=3500);
        let fetch_range = calculate_range_to_fetch(head_height, &[2000..=3500], 500);
        assert_eq!(fetch_range, 3501..=4000);
        let fetch_range = calculate_range_to_fetch(head_height, &[1..=2998, 3000..=3800], 500);
        assert_eq!(fetch_range, 3801..=4000);
    }

    #[async_test]
    async fn init_without_genesis_hash() {
        let events = EventChannel::new();
        let (mock, mut handle) = P2p::mocked();
        let mut generator = ExtendedHeaderGenerator::new();
        let header = generator.next();

        let _syncer = Syncer::start(SyncerArgs {
            p2p: Arc::new(mock),
            store: Arc::new(InMemoryStore::new()),
            event_pub: events.publisher(),
            batch_size: 512,
            sampling_window: SAMPLING_WINDOW,
            pruning_window: DEFAULT_PRUNING_WINDOW,
        })
        .unwrap();

        // Syncer is waiting for a trusted peer to connect
        handle.expect_no_cmd().await;
        handle.announce_peer_connected();
        handle.expect_no_cmd().await;
        handle.announce_trusted_peer_connected();

        // We're syncing from the front, so ask for head first
        let (height, amount, respond_to) = handle.expect_header_request_for_height_cmd().await;
        assert_eq!(height, 0);
        assert_eq!(amount, 1);
        respond_to.send(Ok(vec![header.clone()])).unwrap();

        // Now Syncer initializes HeaderSub with the latest HEAD
        let head_from_syncer = handle.expect_init_header_sub().await;
        assert_eq!(head_from_syncer, header);

        // network head = local head, so nothing else is produced.
        handle.expect_no_cmd().await;
    }

    #[async_test]
    async fn init_with_genesis_hash() {
        let mut generator = ExtendedHeaderGenerator::new();
        let head = generator.next();

        let (_syncer, _store, mut p2p_mock) = initialized_syncer(head.clone()).await;

        // network head = local head, so nothing else is produced.
        p2p_mock.expect_no_cmd().await;
    }

    #[async_test]
    async fn syncing() {
        let mut generator = ExtendedHeaderGenerator::new();
        let headers = generator.next_many(1500);

        let (syncer, store, mut p2p_mock) = initialized_syncer(headers[1499].clone()).await;
        assert_syncing(&syncer, &store, &[1500..=1500], 1500).await;

        // Syncer is syncing backwards from the network head (batch 1)
        handle_session_batch(&mut p2p_mock, &headers, 988..=1499, true).await;
        assert_syncing(&syncer, &store, &[988..=1500], 1500).await;

        // Syncer is syncing backwards from the network head (batch 2)
        handle_session_batch(&mut p2p_mock, &headers, 476..=987, true).await;
        assert_syncing(&syncer, &store, &[476..=1500], 1500).await;

        // New HEAD was received by HeaderSub (height 1501)
        let header1501 = generator.next();
        p2p_mock.announce_new_head(header1501.clone());
        // Height 1501 is adjacent to the last header of Store, so it is appended
        // immediately
        assert_syncing(&syncer, &store, &[476..=1501], 1501).await;

        // Syncer is syncing backwards from the network head (batch 3, partial)
        handle_session_batch(&mut p2p_mock, &headers, 1..=475, true).await;
        assert_syncing(&syncer, &store, &[1..=1501], 1501).await;

        // Syncer is fulling synced and awaiting for events
        p2p_mock.expect_no_cmd().await;

        // New HEAD was received by HeaderSub (height 1502), it should be appended immediately
        let header1502 = generator.next();
        p2p_mock.announce_new_head(header1502.clone());
        assert_syncing(&syncer, &store, &[1..=1502], 1502).await;
        p2p_mock.expect_no_cmd().await;

        // New HEAD was received by HeaderSub (height 1505), it should NOT be appended
        let headers_1503_1505 = generator.next_many(3);
        p2p_mock.announce_new_head(headers_1503_1505[2].clone());
        assert_syncing(&syncer, &store, &[1..=1502], 1505).await;

        // New HEAD is not adjacent to store, so Syncer requests a range
        handle_session_batch(&mut p2p_mock, &headers_1503_1505, 1503..=1505, true).await;
        assert_syncing(&syncer, &store, &[1..=1505], 1505).await;

        // New HEAD was received by HeaderSub (height 3000), it should NOT be appended
        let mut headers = generator.next_many(1495);
        p2p_mock.announce_new_head(headers[1494].clone());
        assert_syncing(&syncer, &store, &[1..=1505], 3000).await;

        // Syncer is syncing forwards, anchored on a range already in store (batch 1)
        handle_session_batch(&mut p2p_mock, &headers, 1506..=2017, true).await;
        assert_syncing(&syncer, &store, &[1..=2017], 3000).await;

        // New head from header sub added, should NOT be appended
        headers.push(generator.next());
        p2p_mock.announce_new_head(headers.last().unwrap().clone());
        assert_syncing(&syncer, &store, &[1..=2017], 3001).await;

        // Syncer continues syncing forwards (batch 2)
        handle_session_batch(&mut p2p_mock, &headers, 2018..=2529, true).await;
        assert_syncing(&syncer, &store, &[1..=2529], 3001).await;

        // Syncer continues syncing forwards, should include the new head received via HeaderSub (batch 3)
        handle_session_batch(&mut p2p_mock, &headers, 2530..=3001, true).await;
        assert_syncing(&syncer, &store, &[1..=3001], 3001).await;

        // Syncer is fulling synced and awaiting for events
        p2p_mock.expect_no_cmd().await;
    }

    #[async_test]
    async fn slow_sync() {
        let pruning_window = Duration::from_secs(600);
        let sampling_window = SAMPLING_WINDOW;

        let mut generator = ExtendedHeaderGenerator::new();

        // Generate back in time 4 batches of 512 messages (= 2048 headers).
        let first_header_time = (Time::now() - Duration::from_secs(2048)).unwrap();
        generator.set_time(first_header_time, Duration::from_secs(1));
        let headers = generator.next_many(2048);

        let (syncer, store, mut p2p_mock) =
            initialized_syncer_with_windows(headers[2047].clone(), sampling_window, pruning_window)
                .await;
        assert_syncing(&syncer, &store, &[2048..=2048], 2048).await;

        // The whole 1st batch does not reach the pruning edge.
        handle_session_batch(&mut p2p_mock, &headers, 1536..=2047, true).await;
        assert_syncing(&syncer, &store, &[1536..=2048], 2048).await;

        // 2nd batch is partially within pruning window, so fast sync applies.
        handle_session_batch(&mut p2p_mock, &headers, 1024..=1535, true).await;
        assert_syncing(&syncer, &store, &[1024..=2048], 2048).await;

        // The whole 3rd batch is beyond pruning edge. So now slow sync applies.
        // In order for the Syncer to send requests, more than a half of the previous
        // batch needs to be sampled.
        syncer.trigger_fetch_next_batch().await.unwrap();
        p2p_mock.expect_no_cmd().await;

        // After marking the 1st and more than of the 2nd batch, Syncer
        // will finally request the 3rd batch.
        for height in 1250..=2048 {
            // Simulate Daser
            store.mark_as_sampled(height).await.unwrap();
        }
        for height in 1300..=1450 {
            // Simulate Pruner
            store.remove_height(height).await.unwrap();
        }
        syncer.trigger_fetch_next_batch().await.unwrap();
        // Syncer should not request the pruned range
        handle_session_batch(&mut p2p_mock, &headers, 512..=1023, true).await;
        assert_syncing(&syncer, &store, &[512..=1299, 1451..=2048], 2048).await;

        // Syncer should not request the 4th batch since there are plenty
        // of blocks to be sampled.
        syncer.trigger_fetch_next_batch().await.unwrap();
        p2p_mock.expect_no_cmd().await;
    }

    #[async_test]
    async fn window_edge() {
        let month_and_day_ago = Duration::from_secs(31 * 24 * 60 * 60);
        let mut generator = ExtendedHeaderGenerator::new();
        generator.set_time(
            (Time::now() - month_and_day_ago).expect("to not underflow"),
            Duration::from_secs(1),
        );
        let mut headers = generator.next_many(1200);
        generator.reset_time();
        headers.append(&mut generator.next_many(2049 - 1200));

        let (syncer, store, mut p2p_mock) = initialized_syncer(headers[2048].clone()).await;
        assert_syncing(&syncer, &store, &[2049..=2049], 2049).await;

        // Syncer requested the first batch
        handle_session_batch(&mut p2p_mock, &headers, 1537..=2048, true).await;
        assert_syncing(&syncer, &store, &[1537..=2049], 2049).await;

        // Syncer requested the second batch hitting the sampling window
        handle_session_batch(&mut p2p_mock, &headers, 1025..=1536, true).await;
        assert_syncing(&syncer, &store, &[1025..=2049], 2049).await;

        // Syncer is fully synced and awaiting for events
        p2p_mock.expect_no_cmd().await;
    }

    #[async_test]
    async fn start_with_filled_store() {
        let events = EventChannel::new();
        let (p2p, mut p2p_mock) = P2p::mocked();
        let (store, mut generator) = gen_filled_store(25).await;
        let store = Arc::new(store);

        let mut headers = generator.next_many(520);
        let network_head = generator.next(); // height 546

        let syncer = Syncer::start(SyncerArgs {
            p2p: Arc::new(p2p),
            store: store.clone(),
            event_pub: events.publisher(),
            batch_size: 512,
            sampling_window: SAMPLING_WINDOW,
            pruning_window: DEFAULT_PRUNING_WINDOW,
        })
        .unwrap();

        p2p_mock.announce_trusted_peer_connected();

        // Syncer asks for current HEAD
        let (height, amount, respond_to) = p2p_mock.expect_header_request_for_height_cmd().await;
        assert_eq!(height, 0);
        assert_eq!(amount, 1);
        respond_to.send(Ok(vec![network_head.clone()])).unwrap();

        // Now Syncer initializes HeaderSub with the latest HEAD
        let head_from_syncer = p2p_mock.expect_init_header_sub().await;
        assert_eq!(head_from_syncer, network_head);

        assert_syncing(&syncer, &store, &[1..=25, 546..=546], 546).await;

        // Syncer requested the first batch
        handle_session_batch(&mut p2p_mock, &headers, 34..=545, true).await;
        assert_syncing(&syncer, &store, &[1..=25, 34..=546], 546).await;

        // Syncer requested the remaining batch ([26, 33])
        let (height, amount, respond_to) = p2p_mock.expect_header_request_for_height_cmd().await;
        assert_eq!(height, 26);
        assert_eq!(amount, 8);
        respond_to
            .send(Ok(headers.drain(..8).collect()))
            .map_err(|_| "headers [538, 545]")
            .unwrap();
        assert_syncing(&syncer, &store, &[1..=546], 546).await;

        // Syncer is fulling synced and awaiting for events
        p2p_mock.expect_no_cmd().await;
    }

    #[async_test]
    async fn stop_syncer() {
        let mut generator = ExtendedHeaderGenerator::new();
        let head = generator.next();

        let (syncer, _store, mut p2p_mock) = initialized_syncer(head.clone()).await;

        // network head height == 1, so nothing else is produced.
        p2p_mock.expect_no_cmd().await;

        syncer.stop();
        // Wait for Worker to stop
        sleep(Duration::from_millis(1)).await;
        assert!(matches!(
            syncer.info().await.unwrap_err(),
            SyncerError::WorkerDied
        ));
    }

    #[async_test]
    async fn all_peers_disconnected() {
        let mut generator = ExtendedHeaderGenerator::new();

        let _gap = generator.next_many(24);
        let header25 = generator.next();
        let _gap = generator.next_many(4);
        let header30 = generator.next();
        let _gap = generator.next_many(4);
        let header35 = generator.next();

        // Start Syncer and report height 30 as HEAD
        let (syncer, store, mut p2p_mock) = initialized_syncer(header30).await;

        // Wait for the request but do not reply to it.
        handle_session_batch(&mut p2p_mock, &[], 1..=29, false).await;

        p2p_mock.announce_all_peers_disconnected();
        // Syncer is now back to `connecting_event_loop`.
        p2p_mock.expect_no_cmd().await;

        // Announce a non-trusted peer. Syncer in `connecting_event_loop` can progress only
        // if a trusted peer is connected.
        p2p_mock.announce_peer_connected();
        p2p_mock.expect_no_cmd().await;

        // Announce a trusted peer.
        p2p_mock.announce_trusted_peer_connected();

        // Now Syncer will send request for HEAD.
        let (height, amount, respond_to) = p2p_mock.expect_header_request_for_height_cmd().await;
        assert_eq!(height, 0);
        assert_eq!(amount, 1);

        // Report an older head. Syncer should not accept it.
        respond_to.send(Ok(vec![header25])).unwrap();
        assert_syncing(&syncer, &store, &[30..=30], 30).await;

        // Syncer will request HEAD again after some time.
        sleep(Duration::from_secs(1)).await;
        let (height, amount, respond_to) = p2p_mock.expect_header_request_for_height_cmd().await;
        assert_eq!(height, 0);
        assert_eq!(amount, 1);

        // Report newer HEAD than before.
        respond_to.send(Ok(vec![header35.clone()])).unwrap();
        assert_syncing(&syncer, &store, &[30..=30, 35..=35], 35).await;

        // Syncer initializes HeaderSub with the latest HEAD.
        let head_from_syncer = p2p_mock.expect_init_header_sub().await;
        assert_eq!(head_from_syncer, header35);

        // Syncer now is in `connected_event_loop` and will try to sync the gap
        // that is closer to HEAD.
        let (height, amount, _respond_to) = p2p_mock.expect_header_request_for_height_cmd().await;
        assert_eq!(height, 31);
        assert_eq!(amount, 4);

        p2p_mock.announce_all_peers_disconnected();
        p2p_mock.expect_no_cmd().await;
    }

    #[async_test]
    async fn all_peers_disconnected_and_no_network_head_progress() {
        let mut generator = ExtendedHeaderGenerator::new_from_height(30);

        let header30 = generator.next();

        // Start Syncer and report height 30 as HEAD
        let (syncer, store, mut p2p_mock) = initialized_syncer(header30.clone()).await;

        // Wait for the request but do not reply to it.
        handle_session_batch(&mut p2p_mock, &[], 1..=29, false).await;

        p2p_mock.announce_all_peers_disconnected();
        // Syncer is now back to `connecting_event_loop`.
        p2p_mock.expect_no_cmd().await;

        // Announce a non-trusted peer. Syncer in `connecting_event_loop` can progress only
        // if a trusted peer is connected.
        p2p_mock.announce_peer_connected();
        p2p_mock.expect_no_cmd().await;

        // Announce a trusted peer.
        p2p_mock.announce_trusted_peer_connected();

        // Now Syncer will send request for HEAD.
        let (height, amount, respond_to) = p2p_mock.expect_header_request_for_height_cmd().await;
        assert_eq!(height, 0);
        assert_eq!(amount, 1);

        // Report the same HEAD as before.
        respond_to.send(Ok(vec![header30.clone()])).unwrap();
        assert_syncing(&syncer, &store, &[30..=30], 30).await;

        // Syncer initializes HeaderSub with the latest HEAD.
        let head_from_syncer = p2p_mock.expect_init_header_sub().await;
        assert_eq!(head_from_syncer, header30);

        // Syncer now is in `connected_event_loop` and will try to sync the gap
        handle_session_batch(&mut p2p_mock, &[], 1..=29, false).await;

        p2p_mock.announce_all_peers_disconnected();
        p2p_mock.expect_no_cmd().await;
    }

    #[async_test]
    async fn non_contiguous_response() {
        let mut generator = ExtendedHeaderGenerator::new();
        let mut headers = generator.next_many(20);

        // Start Syncer and report last header as network head
        let (syncer, store, mut p2p_mock) = initialized_syncer(headers[19].clone()).await;

        let header10 = headers[10].clone();
        // make a gap in response, preserving the amount of headers returned
        headers[10] = headers[11].clone();

        // Syncer requests missing headers
        handle_session_batch(&mut p2p_mock, &headers, 1..=19, true).await;

        // Syncer should not apply headers from invalid response
        assert_syncing(&syncer, &store, &[20..=20], 20).await;

        // correct the response
        headers[10] = header10;

        // Syncer requests missing headers again
        handle_session_batch(&mut p2p_mock, &headers, 1..=19, true).await;

        // With a correct response, Syncer should update the store
        assert_syncing(&syncer, &store, &[1..=20], 20).await;
    }

    #[async_test]
    async fn another_chain_response() {
        let headers = ExtendedHeaderGenerator::new().next_many(20);
        let headers_prime = ExtendedHeaderGenerator::new().next_many(20);

        // Start Syncer and report last header as network head
        let (syncer, store, mut p2p_mock) = initialized_syncer(headers[19].clone()).await;

        // Syncer requests missing headers
        handle_session_batch(&mut p2p_mock, &headers_prime, 1..=19, true).await;

        // Syncer should not apply headers from invalid response
        assert_syncing(&syncer, &store, &[20..=20], 20).await;

        // Syncer requests missing headers again
        handle_session_batch(&mut p2p_mock, &headers, 1..=19, true).await;

        // With a correct response, Syncer should update the store
        assert_syncing(&syncer, &store, &[1..=20], 20).await;
    }

    #[async_test]
    async fn height_sequence_smoke() {
        let mut generator = ExtendedHeaderGenerator::new();
        let headers = generator.next_many(20);
        let (syncer, store, mut p2p_mock) = initialized_syncer(headers[9].clone()).await;

        let mut header_receiver = syncer.subscribe_headers().await.unwrap();
        let (tx, mut received_heights) = mpsc::unbounded_channel();
        spawn(async move {
            while let Ok(header) = header_receiver.recv().await {
                tx.send(header.height()).unwrap();
            }
        });

        // header sub receiving header at height 11 (adjacent, should be added immediately)
        p2p_mock.announce_new_head(headers[10].clone());
        assert_syncing(&syncer, &store, &[10..=11], 11).await;
        assert_eq!(received_heights.recv().await.unwrap(), 11);

        // header sub receiving header at height 15 (gap, should NOT be added immediately)
        p2p_mock.announce_new_head(headers[14].clone());
        assert_syncing(&syncer, &store, &[10..=11], 15).await;
        received_heights.try_recv().unwrap_err();

        // Syncer requests past header range, should be ignored
        handle_session_batch(&mut p2p_mock, &headers, 2..=9, true).await;
        handle_session_batch(&mut p2p_mock, &headers, 1..=1, true).await;
        received_heights.try_recv().unwrap_err();

        // Syncer should request the gap [12, 15]
        handle_session_batch(&mut p2p_mock, &headers, 12..=15, true).await;
        assert_syncing(&syncer, &store, &[1..=15], 15).await;

        for i in 12..=15 {
            let height = received_heights.recv().await.unwrap();
            assert_eq!(height, i);
        }

        // header sub receives header at height 19 (gap)
        p2p_mock.announce_new_head(headers[18].clone());
        assert_syncing(&syncer, &store, &[1..=15], 19).await;
        received_heights.try_recv().unwrap_err();

        // Syncer should request the gap [16, 18]
        handle_session_batch(&mut p2p_mock, &headers, 16..=19, true).await;
        assert_syncing(&syncer, &store, &[1..=19], 19).await;

        for i in 16..=19 {
            let height = received_heights.recv().await.unwrap();
            assert_eq!(height, i);
        }
    }

    #[async_test]
    async fn height_sequence_handles_disconnect() {
        let mut generator = ExtendedHeaderGenerator::new();
        let headers = generator.next_many(20);
        let (syncer, store, mut p2p_mock) = initialized_syncer(headers[0].clone()).await;
        assert_syncing(&syncer, &store, &[1..=1], 1).await;

        let mut header_receiver = syncer.subscribe_headers().await.unwrap();
        let (disconnect_tx, mut disconnect_signal) = oneshot::channel();
        let (tx, mut received_headers) = mpsc::unbounded_channel();
        spawn(async move {
            loop {
                // gather all the headers received, marking the moment disconnection
                // happened with a None.
                select! {
                    header = header_receiver.recv() => {
                        let header = match header {
                            Err(RecvError::Closed) => break,
                            header_or_error => header_or_error.unwrap()
                        };
                        tx.send(Some(header)).unwrap();
                    },
                    _ = &mut disconnect_signal, if !disconnect_signal.is_terminated() => {
                        tx.send(None).unwrap();
                    }
                };
            }
        });

        // header sub receiving header at height 2
        p2p_mock.announce_new_head(headers[1].clone());
        assert_syncing(&syncer, &store, &[1..=2], 2).await;

        // disconnect and reconnect
        p2p_mock.announce_all_peers_disconnected();
        p2p_mock.expect_no_cmd().await;
        p2p_mock.announce_trusted_peer_connected();

        disconnect_tx.send(()).unwrap();

        // Now syncer will send request for HEAD.
        let (height, amount, respond_to) = p2p_mock.expect_header_request_for_height_cmd().await;
        assert_eq!((height, amount), (0, 1));

        // Network progressed while we were disconnected
        respond_to.send(Ok(vec![headers[9].clone()])).unwrap();
        let _ = p2p_mock.expect_init_header_sub().await;
        assert_syncing(&syncer, &store, &[1..=2, 10..=10], 10).await;

        handle_session_batch(&mut p2p_mock, &headers, 3..=9, true).await;
        assert_syncing(&syncer, &store, &[1..=10], 10).await;

        // Header sub receives header at height 11
        p2p_mock.announce_new_head(headers[10].clone());
        assert_syncing(&syncer, &store, &[1..=11], 11).await;

        assert_eq!(
            received_headers.recv().await.unwrap().map(|h| h.height()),
            Some(2)
        );
        assert_eq!(received_headers.recv().await.unwrap(), None);
        for i in 3..=11 {
            let header = received_headers.recv().await.unwrap().unwrap();
            assert_eq!(header.height(), i);
        }
    }

    async fn assert_syncing(
        syncer: &Syncer<InMemoryStore>,
        store: &InMemoryStore,
        expected_synced_ranges: &[RangeInclusive<u64>],
        expected_subjective_head: u64,
    ) {
        // Syncer receives responds on the same loop that receive other events.
        // Wait a bit to be processed.
        sleep(Duration::from_millis(1)).await;

        let store_ranges = store.get_stored_header_ranges().await.unwrap();
        let syncing_info = syncer.info().await.unwrap();

        assert_eq!(store_ranges.as_ref(), expected_synced_ranges);
        assert_eq!(syncing_info.stored_headers.as_ref(), expected_synced_ranges);
        assert_eq!(syncing_info.subjective_head, expected_subjective_head);
    }

    async fn initialized_syncer(
        head: ExtendedHeader,
    ) -> (Syncer<InMemoryStore>, Arc<InMemoryStore>, MockP2pHandle) {
        initialized_syncer_with_windows(head, SAMPLING_WINDOW, DEFAULT_PRUNING_WINDOW).await
    }

    async fn initialized_syncer_with_windows(
        head: ExtendedHeader,
        sampling_window: Duration,
        pruning_window: Duration,
    ) -> (Syncer<InMemoryStore>, Arc<InMemoryStore>, MockP2pHandle) {
        let events = EventChannel::new();
        let (mock, mut handle) = P2p::mocked();
        let store = Arc::new(InMemoryStore::new());

        let syncer = Syncer::start(SyncerArgs {
            p2p: Arc::new(mock),
            store: store.clone(),
            event_pub: events.publisher(),
            batch_size: 512,
            sampling_window,
            pruning_window,
        })
        .unwrap();

        // Syncer is waiting for a trusted peer to connect
        handle.expect_no_cmd().await;
        handle.announce_peer_connected();
        handle.expect_no_cmd().await;
        handle.announce_trusted_peer_connected();

        // After connection is established Syncer asks current network HEAD
        let (height, amount, respond_to) = handle.expect_header_request_for_height_cmd().await;
        assert_eq!(height, 0);
        assert_eq!(amount, 1);
        respond_to.send(Ok(vec![head.clone()])).unwrap();

        // Now Syncer initializes HeaderSub with the latest HEAD
        let head_from_syncer = handle.expect_init_header_sub().await;
        assert_eq!(head_from_syncer, head);

        let head_height = head.height();
        assert_syncing(&syncer, &store, &[head_height..=head_height], head_height).await;

        (syncer, store, handle)
    }

    async fn handle_session_batch(
        p2p_mock: &mut MockP2pHandle,
        remaining_headers: &[ExtendedHeader],
        range: BlockRange,
        respond: bool,
    ) {
        range.validate().unwrap();

        let mut ranges_to_request = BlockRanges::new();
        ranges_to_request.insert_relaxed(&range).unwrap();

        let mut no_respond_chans = Vec::new();

        for _ in 0..requests_in_session(range.len()) {
            let (height, amount, respond_to) =
                p2p_mock.expect_header_request_for_height_cmd().await;

            let requested_range = height..=height + amount - 1;
            ranges_to_request.remove_strict(requested_range);

            if respond {
                let header_index = remaining_headers
                    .iter()
                    .position(|h| h.height() == height)
                    .expect("height not found in provided headers");

                let response_range =
                    remaining_headers[header_index..header_index + amount as usize].to_vec();
                respond_to
                    .send(Ok(response_range))
                    .map_err(|_| format!("headers [{}, {}]", height, height + amount - 1))
                    .unwrap();
            } else {
                no_respond_chans.push(respond_to);
            }
        }

        // Real libp2p implementation will create a timeout error if the peer does not
        // respond. We need to simulate that, otherwise we end up with some undesirable
        // behaviours in Syncer.
        if !respond {
            spawn(async move {
                sleep(Duration::from_secs(10)).await;

                for respond_chan in no_respond_chans {
                    let _ = respond_chan.send(Err(P2pError::HeaderEx(
                        HeaderExError::OutboundFailure(OutboundFailure::Timeout),
                    )));
                }
            });
        }

        assert!(
            ranges_to_request.is_empty(),
            "Some headers weren't requested. expected range: {}, not requested: {}",
            range.display(),
            ranges_to_request
        );
    }

    fn requests_in_session(headers: u64) -> usize {
        let max_requests = headers.div_ceil(header_session::MAX_AMOUNT_PER_REQ) as usize;
        let min_requests = headers.div_ceil(header_session::MIN_AMOUNT_PER_REQ) as usize;

        if max_requests > header_session::MAX_CONCURRENT_REQS {
            // if we have to do more requests than our concurrency limit anyway
            max_requests
        } else {
            // otherwise, we can handle batch fully concurrent
            header_session::MAX_CONCURRENT_REQS.min(min_requests)
        }
    }

    impl BlockRanges {
        fn remove_strict(&mut self, range: BlockRange) {
            for stored in self.as_ref() {
                if stored.contains(range.start()) && stored.contains(range.end()) {
                    self.remove_relaxed(range).unwrap();
                    return;
                }
            }

            panic!("block ranges ({self}) don't contain {}", range.display());
        }
    }
}
