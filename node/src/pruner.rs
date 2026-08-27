//! Component responsible for removing blocks that aren't needed anymore.

use std::collections::{HashMap, HashSet, hash_map};
use std::sync::Arc;
use std::time::Duration;

use blockstore::Blockstore;
use lumina_utils::executor::{JoinHandle, spawn};
use lumina_utils::time::{Instant, sleep};
use tendermint::Time;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::daser::{Daser, DaserError};
use crate::events::{EventPublisher, NodeEvent};
use crate::p2p::P2pError;
use crate::store::{BlockRanges, Store, StoreError};
use crate::utils::TimeExt;

const MAX_PRUNABLE_BATCH_SIZE: u64 = 512;

type Result<T, E = PrunerError> = std::result::Result<T, E>;

/// Representation of all the errors that can occur when interacting with the [`Pruner`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum PrunerError {
    /// An error propagated from the [`P2p`] module.
    #[error("P2p: {0}")]
    P2p(#[from] P2pError),

    /// An error propagated from the [`Store`] module.
    #[error("Syncer: {0}")]
    Store(#[from] StoreError),

    /// An error propagated from the [`Blockstore`] module.
    #[error("Blockstore: {0}")]
    Blockstore(#[from] blockstore::Error),

    /// An error propagated from the `Daser` component.
    #[error("Daser: {0}")]
    Daser(#[from] DaserError),
}

pub(crate) struct Pruner {
    cancellation_token: CancellationToken,
    join_handle: JoinHandle,
}

pub(crate) struct PrunerArgs<S, B>
where
    S: Store,
    B: Blockstore,
{
    pub daser: Arc<Daser>,
    /// Headers storage.
    pub store: Arc<S>,
    /// Block storage.
    pub blockstore: Arc<B>,
    /// Event publisher.
    pub event_pub: EventPublisher,
    /// Average block production interval.
    pub block_time: Duration,
    /// Size of pruning window
    pub pruning_window: Duration,
    /// Size of sampling window
    pub sampling_window: Duration,
}

impl Pruner {
    pub(crate) fn start<S, B>(args: PrunerArgs<S, B>) -> Self
    where
        S: Store + 'static,
        B: Blockstore + 'static,
    {
        let cancellation_token = CancellationToken::new();
        let event_pub = args.event_pub.clone();

        let mut worker = Worker::new(args, cancellation_token.child_token());

        let join_handle = spawn(async move {
            if let Err(e) = worker.run().await {
                error!("Pruner stopped because of a fatal error: {e}");

                event_pub.send(NodeEvent::FatalPrunerError {
                    error: e.to_string(),
                });
            }
        });

        Pruner {
            cancellation_token,
            join_handle,
        }
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
}

impl Drop for Pruner {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Worker<S, B>
where
    S: Store + 'static,
    B: Blockstore + 'static,
{
    daser: Arc<Daser>,
    cancellation_token: CancellationToken,
    event_pub: EventPublisher,
    store: Arc<S>,
    blockstore: Arc<B>,
    block_time: Duration,
    pruning_window: Duration,
    sampling_window: Duration,
    prev_num_of_prunable_blocks: u64,
    cache: Cache,
}

#[derive(Default)]
struct Cache {
    /// When Cache was updated.
    updated_at: Option<Instant>,

    /// The first known height that is after the pruning window.
    after_pruning_window: Option<u64>,
    /// The first known height that is after the sampling window.
    after_sampling_window: Option<u64>,

    /// Cached `BlockInfo`.
    block_info: HashMap<u64, BlockInfo>,
    /// Blocks that we need to keep their `BlockInfo`.
    keep_block_info: HashSet<u64>,
}

#[derive(Debug, Clone)]
struct BlockInfo {
    height: u64,
    time: Time,
}

impl Cache {
    fn garbage_collect(&mut self) {
        self.block_info.retain(|&height, _| {
            self.after_pruning_window == Some(height)
                || self.after_sampling_window == Some(height)
                || self.keep_block_info.contains(&height)
        });
    }

    async fn get_block_info<S>(&mut self, store: &S, height: u64) -> Result<BlockInfo>
    where
        S: Store,
    {
        match self.block_info.entry(height) {
            hash_map::Entry::Occupied(entry) => Ok(entry.get().to_owned()),
            hash_map::Entry::Vacant(entry) => {
                let header = store.get_by_height(height).await?;
                let info = BlockInfo {
                    height: header.height(),
                    time: header.time(),
                };
                entry.insert(info.clone());
                Ok(info)
            }
        }
    }
}

impl<S, B> Worker<S, B>
where
    S: Store,
    B: Blockstore,
{
    fn new(args: PrunerArgs<S, B>, cancellation_token: CancellationToken) -> Self {
        Worker {
            cancellation_token,
            event_pub: args.event_pub,
            store: args.store,
            blockstore: args.blockstore,
            block_time: args.block_time,
            pruning_window: args.pruning_window,
            sampling_window: args.sampling_window,
            daser: args.daser,
            prev_num_of_prunable_blocks: 0,
            cache: Cache {
                updated_at: None,
                after_pruning_window: None,
                after_sampling_window: None,
                block_info: HashMap::new(),
                keep_block_info: HashSet::new(),
            },
        }
    }

    async fn run(&mut self) -> Result<()> {
        loop {
            if self.cancellation_token.is_cancelled() {
                break;
            }

            let now = Time::now();
            let sampling_window_end = now.saturating_sub(self.sampling_window);
            let pruning_window_end = now.saturating_sub(self.pruning_window);

            let prunable_batch = self
                .get_next_prunable_batch(sampling_window_end, pruning_window_end)
                .await?;

            if prunable_batch.is_empty() {
                select! {
                    _ = self.cancellation_token.cancelled() => break,
                    _ = sleep(self.block_time) => continue,
                }
            }

            info!("Going to prune {} blocks", prunable_batch.len());

            for range in prunable_batch.into_inner() {
                let from_height = *range.start();
                // Because of `cancellation_token` we need to track up to which
                // height we removed.
                let mut to_height = None;

                for height in range {
                    if self.cancellation_token.is_cancelled() {
                        break;
                    }

                    let cids = self
                        .store
                        .get_sampling_metadata(height)
                        .await?
                        .map(|m| m.cids)
                        .unwrap_or_default();

                    for cid in cids {
                        self.blockstore.remove(&cid).await?;
                    }

                    self.store.remove_height(height).await?;
                    to_height = Some(height);
                }

                if let Some(to_height) = to_height {
                    self.event_pub.send(NodeEvent::PrunedHeaders {
                        from_height,
                        to_height,
                    });
                }
            }
        }

        debug!("Pruner stopped");
        Ok(())
    }

    async fn update_cached_data(
        &mut self,
        stored_blocks: &BlockRanges,
        sampling_cutoff: &Time,
        pruning_cutoff: &Time,
    ) -> Result<()> {
        let update_after = Duration::from_secs(1).min(self.block_time);

        if self
            .cache
            .updated_at
            .is_some_and(|updated_at| updated_at.elapsed() < update_after)
        {
            // No need to update cached values
            return Ok(());
        }

        let after_sampling_window = find_height_after_window(
            &*self.store,
            stored_blocks,
            sampling_cutoff,
            self.cache.after_sampling_window,
            &mut self.cache,
        )
        .await?;

        let after_pruning_window = find_height_after_window(
            &*self.store,
            stored_blocks,
            pruning_cutoff,
            self.cache.after_pruning_window,
            &mut self.cache,
        )
        .await?;

        if self.cache.after_sampling_window < after_sampling_window {
            self.cache.after_sampling_window = after_sampling_window;
        }

        if self.cache.after_pruning_window < after_pruning_window {
            self.cache.after_pruning_window = after_pruning_window;

            // Send an update to Daser
            if let Some(height) = after_pruning_window {
                self.daser.update_highest_prunable_block(height).await?;
            }
        }

        self.cache.keep_block_info.clear();

        if let Some(stored_tail) = stored_blocks.tail() {
            self.cache.keep_block_info.insert(stored_tail);
        }

        if let Some(sampling_window_tail) = self
            .cache
            .after_sampling_window
            .and_then(|height| stored_blocks.right_of(height))
        {
            self.cache.keep_block_info.insert(sampling_window_tail);
        }

        if let Some(pruning_window_tail) = self
            .cache
            .after_pruning_window
            .and_then(|height| stored_blocks.right_of(height))
        {
            self.cache.keep_block_info.insert(pruning_window_tail);
        }

        self.cache.garbage_collect();
        self.cache.updated_at = Some(Instant::now());

        Ok(())
    }

    /// Returns a batch of at most 512 blocks that are need to be pruned.
    async fn get_next_prunable_batch(
        &mut self,
        sampling_cutoff: Time,
        pruning_cutoff: Time,
    ) -> Result<BlockRanges> {
        // We need to be able to handle two cases:
        //
        // Case 1 - Pruning window is equal or bigger than sampling window
        //
        // +------------------------+---------------+-------------------+
        // |         1 - 140        |   141 - 200   |      201 - 260    |
        // +------------------------+---------------+-------------------+
        // <-----Prunable area----->|<-----------Pruning window--------->
        // <-----------Non sampling area----------->|<--Sampling window->
        //
        // This case is straightforward: we just remove whatever is after
        // the pruning window, or in other words, anything that is inside
        // the prunable area.
        //
        //
        // Case 2 - Pruning window is smaller than sampling window
        //
        // +------------------------+---------------+-------------------+
        // |         1 - 140        |   141 - 200   |      201 - 260    |
        // +------------------------+---------------+-------------------+
        // <----------Prunable area---------------->|<--Pruning window-->
        // <---Non sampling area--->|<----------Sampling window--------->
        //
        // This case is a bit more complicated: The real use case of having pruning
        // window smaller than sampling window is when you want to prune blocks
        // that are after the pruning window as soon as they are sampled. This
        // is mainly used when you want small storage footprint but still validate
        // the blockchain.
        //
        // In this case when something is after the pruning window but within the
        // sampling window can it can be removed if both of these constraints are met:
        //
        // 1. Block was sampled
        // 2. Block header will not be needed in the future for verifying insertion
        //    of currently missing block headers. We call these needed headers "edges".
        //
        // If the block is after the pruning window and after the sampling window
        // then it can be removed as long as there isn't an ongoing data sampling
        // for it in Daser. We check this by sending `WantToPrune` message to `Daser`
        // and receiving back `true`. This mechanism exists to avoid race conditions
        // between Pruner and Daser.
        //
        // The following algorithm covers both cases.
        let stored_ranges = self.store.get_stored_header_ranges().await?;
        let pruned_ranges = self.store.get_pruned_ranges().await?;
        let sampled_ranges = self.store.get_sampled_ranges().await?;

        self.update_cached_data(&stored_ranges, &sampling_cutoff, &pruning_cutoff)
            .await?;

        // All heights after the sampling window
        let non_sampling_area = self
            .cache
            .after_sampling_window
            .map(|height| BlockRanges::try_from(1..=height).expect("never fails"))
            .unwrap_or_default();

        // All heights after the pruning window
        let prunable_area = self
            .cache
            .after_pruning_window
            .map(|height| BlockRanges::try_from(1..=height).expect("never fails"))
            .unwrap_or_default();

        // All synced heights (stored + pruned)
        let synced_ranges = pruned_ranges + &stored_ranges;
        // Edges of the synced ranges
        let edges = synced_ranges.edges();
        // All heights that are stored and after pruning window
        let prune_candidates = stored_ranges & &prunable_area;

        let after_sampling_window = prune_candidates.clone() & &non_sampling_area;
        let prunable_and_sampled =
            (prune_candidates.clone() - &after_sampling_window - &edges) & &sampled_ranges;

        let num_of_prunable_blocks = after_sampling_window.len() + prunable_and_sampled.len();

        // Send and update to Daser
        if self.prev_num_of_prunable_blocks != num_of_prunable_blocks {
            self.daser
                .update_number_of_prunable_blocks(num_of_prunable_blocks)
                .await?;
            self.prev_num_of_prunable_blocks = num_of_prunable_blocks;
        }

        let mut prunable_batch = prunable_and_sampled.headn(MAX_PRUNABLE_BATCH_SIZE);

        // Daser needs to allow us to remove anything beyond sampling and pruning window.
        for height in after_sampling_window.rev() {
            if prunable_batch.len() == MAX_PRUNABLE_BATCH_SIZE {
                break;
            }

            if sampled_ranges.contains(height) || self.daser.want_to_prune(height).await? {
                prunable_batch
                    .insert_relaxed(height..=height)
                    .expect("never fails");
            }
        }

        Ok(prunable_batch)
    }
}

async fn find_height_after_window<S>(
    store: &S,
    stored_headers: &BlockRanges,
    cutoff: &Time,
    prev_after_window: Option<u64>,
    cache: &mut Cache,
) -> Result<Option<u64>>
where
    S: Store,
{
    if let Some(res) =
        find_height_after_window_fast(store, stored_headers, cutoff, prev_after_window, cache)
            .await?
    {
        return Ok(res);
    }

    // Binary search
    find_height_after_window_slow(store, stored_headers, cutoff, cache).await
}

/// Uses cached values to figure out the first height after the window cutoff.
///
/// Returns:
///
/// * `Ok(None)` if it couldn't figure it out and a binary search is needed.
/// * `Ok(Some(None))` if it there is no height after the window cutoff.
/// * `Ok(Some(Some(height))` if it figured out the height.
async fn find_height_after_window_fast<S>(
    store: &S,
    stored_headers: &BlockRanges,
    cutoff: &Time,
    prev_after_window: Option<u64>,
    cache: &mut Cache,
) -> Result<Option<Option<u64>>>
where
    S: Store,
{
    match prev_after_window {
        Some(prev_after_window) => match stored_headers.right_of(prev_after_window) {
            Some(right_of_after_window) => {
                let block_info = cache.get_block_info(store, right_of_after_window).await?;

                // If the block on the right is still within window.
                if *cutoff < block_info.time {
                    if stored_headers.contains(prev_after_window) {
                        return Ok(Some(Some(prev_after_window)));
                    } else {
                        return Ok(Some(stored_headers.left_of(prev_after_window)));
                    }
                }

                // This point is reached when block on the right is now out of window.
                // As an optimization we need to check the next on if it is in.
                //
                // This check mostly catches the blocks that are on the boarder of window.
                match stored_headers.right_of(right_of_after_window) {
                    Some(right_of_right) => {
                        let block_info = cache.get_block_info(store, right_of_right).await?;

                        // If the right of the right block is within window.
                        if *cutoff < block_info.time {
                            return Ok(Some(Some(right_of_after_window)));
                        }
                    }
                    // If there aren't blocks on the right.
                    None => return Ok(Some(Some(right_of_after_window))),
                }
            }
            // If there aren't blocks on the right.
            None => {
                if stored_headers.contains(prev_after_window) {
                    return Ok(Some(Some(prev_after_window)));
                } else {
                    return Ok(Some(stored_headers.left_of(prev_after_window)));
                }
            }
        },
        None => {
            let Some(tail) = stored_headers.tail() else {
                // No stored headers
                return Ok(Some(None));
            };

            let block_info = cache.get_block_info(store, tail).await?;

            if *cutoff < block_info.time {
                // All blocks are within the window
                return Ok(Some(None));
            }
        }
    }

    // Binary search is needed
    Ok(None)
}

/// Use binary search and find the first height after the window cutoff.
async fn find_height_after_window_slow<S>(
    store: &S,
    stored_headers: &BlockRanges,
    cutoff: &Time,
    cache: &mut Cache,
) -> Result<Option<u64>>
where
    S: Store,
{
    let mut ranges = stored_headers.to_owned();
    let mut highest: Option<BlockInfo> = None;

    while let Some((left, middle, right)) = ranges.partitions() {
        let middle = cache.get_block_info(store, middle).await?;

        if middle.time < *cutoff {
            if highest
                .as_ref()
                .is_none_or(|highest| highest.time < middle.time)
            {
                highest = Some(middle);
            }

            ranges = right;
        } else {
            ranges = left;
        }
    }

    Ok(highest.map(|block_info| block_info.height))
}

#[cfg(test)]
// One-element arrays here really are arrays of ranges, not a shorthand for a range of values.
#[allow(clippy::single_range_in_vec_init)]
mod test {
    use blockstore::block::{Block, CidError};
    use celestia_types::test_utils::ExtendedHeaderGenerator;
    use cid::CidGeneric;
    use cid::multihash::Multihash;

    use super::*;
    use crate::blockstore::InMemoryBlockstore;
    use crate::events::{EventChannel, EventSubscriber, TryRecvError};
    use crate::node::{DEFAULT_PRUNING_WINDOW, SAMPLING_WINDOW};
    use crate::store::InMemoryStore;
    use crate::test_utils::{ExtendedHeaderGeneratorExt, gen_filled_store, new_block_ranges};
    use lumina_utils::test_utils::async_test;
    use lumina_utils::time::timeout;

    const TEST_CODEC: u64 = 0x0D;
    const TEST_MH_CODE: u64 = 0x0D;

    #[async_test]
    async fn prunable_height() {
        let now = Time::now();
        let store = InMemoryStore::new();
        let mut cache = Cache::default();
        let pruning_window = Duration::from_secs(60);
        let mut generator = ExtendedHeaderGenerator::new();

        let first_header_time = (now - Duration::from_secs(120)).unwrap();
        generator.set_time(first_header_time, Duration::from_secs(1));

        store.insert(generator.next_many(120)).await.unwrap();

        let stored_headers = store.get_stored_header_ranges().await.unwrap();
        let pruning_cutoff = now.saturating_sub(pruning_window);

        assert_eq!(
            find_height_after_window_slow(&store, &stored_headers, &pruning_cutoff, &mut cache)
                .await
                .unwrap(),
            Some(59)
        );
    }

    #[async_test]
    async fn prunable_height_with_gaps() {
        let now = Time::now();
        let store = InMemoryStore::new();
        let mut cache = Cache::default();
        let pruning_window = Duration::from_secs(60);
        let mut generator = ExtendedHeaderGenerator::new();

        let first_header_time = (now - Duration::from_secs(120)).unwrap();
        generator.set_time(first_header_time, Duration::from_secs(1));

        let headers = generator.next_many(120);

        store.insert(&headers[0..10]).await.unwrap();
        store.insert(&headers[20..30]).await.unwrap();
        store.insert(&headers[65..70]).await.unwrap();
        store.insert(&headers[80..90]).await.unwrap();
        store.insert(&headers[100..120]).await.unwrap();

        let stored_headers = store.get_stored_header_ranges().await.unwrap();
        let pruning_cutoff = now.saturating_sub(pruning_window);

        assert_eq!(
            find_height_after_window_slow(&store, &stored_headers, &pruning_cutoff, &mut cache)
                .await
                .unwrap(),
            Some(30)
        );

        store.insert(&headers[58..=64]).await.unwrap();

        assert_eq!(
            find_height_after_window_slow(&store, &stored_headers, &pruning_cutoff, &mut cache)
                .await
                .unwrap(),
            Some(30)
        );
    }

    #[async_test]
    async fn prunable_height_beyond_genesis() {
        let now = Time::now();
        let store = InMemoryStore::new();
        let mut cache = Cache::default();
        let pruning_window = Duration::from_secs(121);
        let mut generator = ExtendedHeaderGenerator::new();

        let first_header_time = (now - Duration::from_secs(120)).unwrap();
        generator.set_time(first_header_time, Duration::from_secs(1));

        let headers = generator.next_many(120);
        store.insert(headers).await.unwrap();

        let stored_headers = store.get_stored_header_ranges().await.unwrap();
        let pruning_cutoff = now.saturating_sub(pruning_window);

        assert_eq!(
            find_height_after_window_slow(&store, &stored_headers, &pruning_cutoff, &mut cache)
                .await
                .unwrap(),
            None
        );
    }

    #[async_test]
    async fn prunable_height_cached_empty_store() {
        let now = Time::now();
        let store = InMemoryStore::new();
        let mut cache = Cache::default();
        let pruning_window = Duration::from_secs(60);

        let stored_headers = store.get_stored_header_ranges().await.unwrap();
        let pruning_cutoff = now.saturating_sub(pruning_window);

        assert_eq!(
            find_height_after_window_fast(
                &store,
                &stored_headers,
                &pruning_cutoff,
                None,
                &mut cache
            )
            .await
            .unwrap(),
            Some(None)
        );

        assert_eq!(
            find_height_after_window_fast(
                &store,
                &stored_headers,
                &pruning_cutoff,
                Some(40),
                &mut cache
            )
            .await
            .unwrap(),
            Some(None)
        );
    }

    #[async_test]
    async fn prunable_height_cached_all_blocks_in_window() {
        let now = Time::now();
        let store = InMemoryStore::new();
        let mut cache = Cache::default();
        let pruning_window = Duration::from_secs(60);
        let mut generator = ExtendedHeaderGenerator::new();

        let first_header_time = (now - Duration::from_secs(120)).unwrap();
        generator.set_time(first_header_time, Duration::from_secs(1));

        let headers = generator.next_many(120);
        store.insert(&headers[110..120]).await.unwrap();

        let stored_headers = store.get_stored_header_ranges().await.unwrap();
        let pruning_cutoff = now.saturating_sub(pruning_window);

        assert_eq!(
            find_height_after_window_fast(
                &store,
                &stored_headers,
                &pruning_cutoff,
                None,
                &mut cache
            )
            .await
            .unwrap(),
            Some(None)
        );

        assert_eq!(
            find_height_after_window_fast(
                &store,
                &stored_headers,
                &pruning_cutoff,
                Some(40),
                &mut cache
            )
            .await
            .unwrap(),
            Some(None)
        );
    }

    #[async_test]
    async fn prunable_height_cached_all_blocks_out_of_window() {
        let now = Time::now();
        let store = InMemoryStore::new();
        let mut cache = Cache::default();
        let pruning_window = Duration::from_secs(60);
        let mut generator = ExtendedHeaderGenerator::new();

        let first_header_time = (now - Duration::from_secs(120)).unwrap();
        generator.set_time(first_header_time, Duration::from_secs(1));

        let headers = generator.next_many(120);
        store.insert(&headers[30..40]).await.unwrap();

        let stored_headers = store.get_stored_header_ranges().await.unwrap();
        let pruning_cutoff = now.saturating_sub(pruning_window);

        // Binary search is needed because there is no cached info.
        assert_eq!(
            find_height_after_window_fast(
                &store,
                &stored_headers,
                &pruning_cutoff,
                None,
                &mut cache
            )
            .await
            .unwrap(),
            None
        );

        assert_eq!(
            find_height_after_window_fast(
                &store,
                &stored_headers,
                &pruning_cutoff,
                Some(50),
                &mut cache
            )
            .await
            .unwrap(),
            Some(Some(40))
        );

        assert_eq!(
            find_height_after_window_fast(
                &store,
                &stored_headers,
                &pruning_cutoff,
                Some(40),
                &mut cache
            )
            .await
            .unwrap(),
            Some(Some(40))
        );

        assert_eq!(
            find_height_after_window_fast(
                &store,
                &stored_headers,
                &pruning_cutoff,
                Some(39),
                &mut cache
            )
            .await
            .unwrap(),
            Some(Some(40))
        );

        // Binary search is needed because of too many blocks that are out of window
        // are after the `prev_after_window` parameter.
        assert_eq!(
            find_height_after_window_fast(
                &store,
                &stored_headers,
                &pruning_cutoff,
                Some(38),
                &mut cache
            )
            .await
            .unwrap(),
            None
        );
    }

    #[async_test]
    async fn prunable_height_cached_mixed() {
        let now = Time::now();
        let store = InMemoryStore::new();
        let mut cache = Cache::default();
        let pruning_window = Duration::from_secs(60);
        let mut generator = ExtendedHeaderGenerator::new();

        let first_header_time = (now - Duration::from_secs(120)).unwrap();
        generator.set_time(first_header_time, Duration::from_secs(1));

        let headers = generator.next_many(120);
        store.insert(&headers[30..40]).await.unwrap();
        store.insert(&headers[110..120]).await.unwrap();

        let stored_headers = store.get_stored_header_ranges().await.unwrap();
        let pruning_cutoff = now.saturating_sub(pruning_window);

        // Binary search is needed because there is no cached info.
        assert_eq!(
            find_height_after_window_fast(
                &store,
                &stored_headers,
                &pruning_cutoff,
                None,
                &mut cache
            )
            .await
            .unwrap(),
            None
        );

        assert_eq!(
            find_height_after_window_fast(
                &store,
                &stored_headers,
                &pruning_cutoff,
                Some(50),
                &mut cache
            )
            .await
            .unwrap(),
            Some(Some(40))
        );

        assert_eq!(
            find_height_after_window_fast(
                &store,
                &stored_headers,
                &pruning_cutoff,
                Some(40),
                &mut cache
            )
            .await
            .unwrap(),
            Some(Some(40))
        );

        assert_eq!(
            find_height_after_window_fast(
                &store,
                &stored_headers,
                &pruning_cutoff,
                Some(39),
                &mut cache
            )
            .await
            .unwrap(),
            Some(Some(40))
        );

        // Binary search is needed because of too many blocks that are out of window
        // are after the `prev_after_window` parameter.
        assert_eq!(
            find_height_after_window_fast(
                &store,
                &stored_headers,
                &pruning_cutoff,
                Some(38),
                &mut cache
            )
            .await
            .unwrap(),
            None
        );
    }

    #[async_test]
    async fn empty_store() {
        let events = EventChannel::new();
        let store = Arc::new(InMemoryStore::new());
        let blockstore = Arc::new(InMemoryBlockstore::new());
        let mut event_subscriber = events.subscribe();
        let (daser, mut daser_handle) = Daser::mocked();

        let pruner = Pruner::start(PrunerArgs {
            daser: Arc::new(daser),
            store,
            blockstore,
            event_pub: events.publisher(),
            block_time: Duration::from_secs(1),
            pruning_window: DEFAULT_PRUNING_WINDOW,
            sampling_window: SAMPLING_WINDOW,
        });

        sleep(Duration::from_secs(1)).await;

        daser_handle.expect_no_cmd().await;
        pruner.stop();
        pruner.join().await;

        assert!(matches!(
            event_subscriber.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));
    }

    #[async_test]
    async fn nothing_to_prune() {
        let events = EventChannel::new();
        let (store, _gen) = gen_filled_store(100).await;
        let store = Arc::new(store);
        let blockstore = Arc::new(InMemoryBlockstore::new());
        let mut event_subscriber = events.subscribe();
        let (daser, mut daser_handle) = Daser::mocked();

        let pruner = Pruner::start(PrunerArgs {
            daser: Arc::new(daser),
            store: store.clone(),
            blockstore,
            event_pub: events.publisher(),
            block_time: Duration::from_secs(1),
            pruning_window: DEFAULT_PRUNING_WINDOW,
            sampling_window: SAMPLING_WINDOW,
        });

        sleep(Duration::from_secs(1)).await;

        daser_handle.expect_no_cmd().await;
        pruner.stop();
        pruner.join().await;

        assert!(matches!(
            event_subscriber.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));
        assert_eq!(
            store.get_stored_header_ranges().await.unwrap(),
            new_block_ranges([1..=100])
        );
    }

    #[async_test]
    async fn prune_large_tail_with_cids() {
        let events = EventChannel::new();
        let store = Arc::new(InMemoryStore::new());
        let mut generator = ExtendedHeaderGenerator::new();

        let blockstore = Arc::new(InMemoryBlockstore::new());
        let mut event_subscriber = events.subscribe();
        let (daser, mut daser_handle) = Daser::mocked();

        let first_header_time = (Time::now()
            - (DEFAULT_PRUNING_WINDOW + Duration::from_secs(30 * 24 * 60 * 60)))
        .unwrap();
        generator.set_time(first_header_time, Duration::from_secs(1));

        let blocks_with_sampling = (1..=500)
            .chain(601..=1000)
            .map(|height| {
                let block = TestBlock::from(height);
                let sampled = height % 3 == 0;
                (height, block, block.cid().unwrap(), sampled)
            })
            .collect::<Vec<_>>();

        store
            .insert(generator.next_many_verified(500))
            .await
            .unwrap();
        generator.skip(100);
        store
            .insert(generator.next_many_verified(400))
            .await
            .unwrap();

        for (height, block, cid, sampled) in &blocks_with_sampling {
            blockstore.put_keyed(cid, block.data()).await.unwrap();

            if *sampled {
                store.mark_as_sampled(*height).await.unwrap();
            }

            store
                .update_sampling_metadata(*height, vec![*cid])
                .await
                .unwrap();
        }

        let stored_ranges = store.get_stored_header_ranges().await.unwrap();
        let sampled_ranges = store.get_sampled_ranges().await.unwrap();

        let pruner = Pruner::start(PrunerArgs {
            daser: Arc::new(daser),
            store: store.clone(),
            blockstore: blockstore.clone(),
            event_pub: events.publisher(),
            block_time: Duration::from_secs(1),
            pruning_window: DEFAULT_PRUNING_WINDOW,
            sampling_window: SAMPLING_WINDOW,
        });

        assert_eq!(
            daser_handle.expect_update_highest_prunable_block().await,
            1000
        );
        assert_eq!(
            daser_handle.expect_update_number_of_prunable_blocks().await,
            900
        );

        for height in (601..=1000).rev().chain((1..=500).rev()) {
            assert!(stored_ranges.contains(height));

            // At height 388 a second batch starts and Pruner sends an update to Daser
            if height == 388 {
                assert_eq!(
                    daser_handle.expect_update_number_of_prunable_blocks().await,
                    388
                );
            }

            // If a block is not sampled, Pruner asks Daser for permission to prune it.
            if !sampled_ranges.contains(height) {
                let (want_to_prune, respond_to) = daser_handle.expect_want_to_prune().await;
                assert_eq!(want_to_prune, height);
                respond_to.send(true).unwrap();
            }
        }

        assert_eq!(
            daser_handle.expect_update_number_of_prunable_blocks().await,
            0
        );

        daser_handle.expect_no_cmd().await;

        assert_pruned_headers_event(&mut event_subscriber, 389, 500).await;
        assert_pruned_headers_event(&mut event_subscriber, 601, 1000).await;
        assert_pruned_headers_event(&mut event_subscriber, 1, 388).await;

        assert!(store.get_stored_header_ranges().await.unwrap().is_empty());

        for (height, _, cid, _) in &blocks_with_sampling {
            assert!(matches!(
                store.get_sampling_metadata(*height).await.unwrap_err(),
                StoreError::NotFound
            ));
            assert!(!blockstore.has(cid).await.unwrap());
        }

        pruner.stop();
        pruner.join().await;

        assert!(matches!(
            event_subscriber.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));
    }

    #[async_test]
    async fn prune_tail() {
        let block_time = Duration::from_millis(1);
        let pruning_window = Duration::from_millis(4000);
        let sampling_window = Duration::from_millis(500);

        let events = EventChannel::new();
        let store = Arc::new(InMemoryStore::new());
        let mut generator = ExtendedHeaderGenerator::new();
        let blockstore = Arc::new(InMemoryBlockstore::new());
        let mut event_subscriber = events.subscribe();
        let (daser, mut daser_handle) = Daser::mocked();

        let first_header_time = (Time::now() - Duration::from_millis(5000)).unwrap();
        generator.set_time(first_header_time, block_time);

        // Tail
        store
            .insert(generator.next_many_verified(10))
            .await
            .unwrap();
        // Gap
        generator.skip(2480);
        // 10 headers within 1.5sec of pruning window edge
        store
            .insert(generator.next_many_verified(10))
            .await
            .unwrap();
        // Gap
        generator.skip(2490);
        // 10 headers at the current time
        store
            .insert(generator.next_many_verified(10))
            .await
            .unwrap();

        assert_eq!(
            store.get_stored_header_ranges().await.unwrap(),
            new_block_ranges([1..=10, 2491..=2500, 4991..=5000]),
        );

        let pruner = Pruner::start(PrunerArgs {
            daser: Arc::new(daser),
            store: store.clone(),
            blockstore,
            event_pub: events.publisher(),
            block_time,
            pruning_window,
            sampling_window,
        });

        assert_eq!(
            daser_handle.expect_update_highest_prunable_block().await,
            10
        );
        assert_eq!(
            daser_handle.expect_update_number_of_prunable_blocks().await,
            10
        );

        for expected_height in (1..=10).rev() {
            let (height, respond_to) = daser_handle.expect_want_to_prune().await;
            assert_eq!(height, expected_height);
            respond_to.send(true).unwrap();
        }

        assert_eq!(
            daser_handle.expect_update_number_of_prunable_blocks().await,
            0
        );

        assert_pruned_headers_event(&mut event_subscriber, 1, 10).await;
        assert_eq!(
            store.get_stored_header_ranges().await.unwrap(),
            new_block_ranges([2491..=2500, 4991..=5000]),
        );

        sleep(Duration::from_millis(1500)).await;

        // Because Pruner runs in parallel, it generates 2 batches. The first one will
        // be the blocks that reached pruning edge while `sleep` is still running.
        let batch1_high_height = daser_handle.expect_update_highest_prunable_block().await;
        assert!((2491..=2500).contains(&batch1_high_height));

        let batch1_num_of_prunable_blocks =
            daser_handle.expect_update_number_of_prunable_blocks().await;
        assert!((1..=10).contains(&batch1_num_of_prunable_blocks));

        // 1st batch
        for expected_height in (2491..=batch1_high_height).rev() {
            let (height, respond_to) = daser_handle.expect_want_to_prune().await;
            assert_eq!(height, expected_height);
            respond_to.send(true).unwrap();
        }

        assert_pruned_headers_event(&mut event_subscriber, 2491, batch1_high_height).await;

        // If 2nd batch will be produced
        if batch1_high_height < 2500 {
            assert_eq!(
                daser_handle.expect_update_highest_prunable_block().await,
                2500
            );

            assert_eq!(
                daser_handle.expect_update_number_of_prunable_blocks().await,
                10 - batch1_num_of_prunable_blocks
            );

            for expected_height in (batch1_high_height + 1..=2500).rev() {
                let (height, respond_to) = daser_handle.expect_want_to_prune().await;
                assert_eq!(height, expected_height);
                respond_to.send(true).unwrap();
            }

            assert_pruned_headers_event(&mut event_subscriber, batch1_high_height + 1, 2500).await;
        }

        assert_eq!(
            daser_handle.expect_update_number_of_prunable_blocks().await,
            0
        );

        assert_eq!(
            store.get_stored_header_ranges().await.unwrap(),
            new_block_ranges([4991..=5000]),
        );

        daser_handle.expect_no_cmd().await;
        pruner.stop();
        pruner.join().await;

        assert!(matches!(
            event_subscriber.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));
        assert_eq!(
            store.get_stored_header_ranges().await.unwrap(),
            new_block_ranges([4991..=5000])
        );
    }

    #[async_test]
    async fn sampling_window_bigger_than_pruning_window() {
        let pruning_window = Duration::from_secs(60);
        let sampling_window = Duration::from_secs(120);

        let mut generator = ExtendedHeaderGenerator::new();
        let store = Arc::new(InMemoryStore::new());
        let blockstore = Arc::new(InMemoryBlockstore::new());

        // Start header creation 260 seconds in the past, with block time of 1 second.
        // Since pruning window is 60s and sampling window is 120s, this means:
        //
        // +---------------------------------------------------------+
        // |       1 - 140       |   141 - 200   |      201 - 260    |
        // +---------------------+---------------+-------------------+
        // <----------Prunable area------------->|<--Pruning window-->
        //                       <-----------Sampling window--------->
        //
        // NOTE 1: Pruning window is actually "keep in store" window. Even if its name
        // can be confusing, we kept it because this is how it meantioned in CIP-4.
        //
        // NOTE 2: Since `Time::now` in Pruner is changing, the edges may vary.
        let first_header_time = (Time::now() - Duration::from_secs(260)).unwrap();
        generator.set_time(first_header_time, Duration::from_secs(1));

        // 1 - 120
        store
            .insert(generator.next_many_empty_verified(120))
            .await
            .unwrap();
        // 121 - 145
        generator.skip(25);
        // 146 - 155
        store
            .insert(generator.next_many_empty_verified(10))
            .await
            .unwrap();
        // 156 - 165
        generator.skip(10);
        // 166 - 175
        store
            .insert(generator.next_many_empty_verified(10))
            .await
            .unwrap();
        // 176 - 185
        generator.skip(10);
        // 186 - 199
        store
            .insert(generator.next_many_empty_verified(14))
            .await
            .unwrap();
        // 200 - 201, We skip these because they are the edge of pruning window.
        // Pruner is using `Time::now` and we want to make the tests more predictable.
        generator.skip(2);
        // 202 - 260
        store
            .insert(generator.next_many_empty_verified(59))
            .await
            .unwrap();

        // Create gaps by pruning. We do this to test that Pruner takes them
        // into account when it calculates the edges of synced block ranges.
        store.remove_height(188).await.unwrap();
        store.remove_height(189).await.unwrap();
        store.remove_height(192).await.unwrap();

        for height in (1..=100).chain(103..=195).chain(199..=210) {
            // We ignore the error because we want to get makred only
            // those that are in store.
            let _ = store.mark_as_sampled(height).await;
        }

        assert_eq!(
            store.get_stored_header_ranges().await.unwrap(),
            new_block_ranges([
                1..=120,
                146..=155,
                166..=175,
                186..=187,
                190..=191,
                193..=199,
                202..=260
            ])
        );
        assert_eq!(
            store.get_sampled_ranges().await.unwrap(),
            new_block_ranges([
                1..=100,
                103..=120,
                146..=155,
                166..=175,
                186..=187,
                190..=191,
                193..=195,
                199..=199,
                202..=210
            ])
        );
        assert_eq!(
            store.get_pruned_ranges().await.unwrap(),
            new_block_ranges([188..=189, 192..=192])
        );

        let events = EventChannel::new();
        let mut event_subscriber = events.subscribe();
        let (daser, mut daser_handle) = Daser::mocked();

        let pruner = Pruner::start(PrunerArgs {
            daser: Arc::new(daser),
            store: store.clone(),
            blockstore,
            event_pub: events.publisher(),
            block_time: Duration::from_secs(1),
            pruning_window,
            sampling_window,
        });

        assert_eq!(
            daser_handle.expect_update_highest_prunable_block().await,
            199
        );
        assert_eq!(
            daser_handle.expect_update_number_of_prunable_blocks().await,
            142
        );

        // Pruner asks from Daser if it can remove block 102.
        // We simulate that Daser allows it.
        let (want_to_prune, respond_to) = daser_handle.expect_want_to_prune().await;
        assert_eq!(want_to_prune, 102);
        respond_to.send(true).unwrap();

        // Pruner asks from Daser if it can remove block 101.
        // We simulate that Daser does not allow it.
        let (want_to_prune, respond_to) = daser_handle.expect_want_to_prune().await;
        assert_eq!(want_to_prune, 101);
        respond_to.send(false).unwrap();

        // Because Pruner runs in parallel with this test and removes in
        // batches, we expect the following:
        //
        // - Removes 102 because Daser allowed it.
        // - Keeps 101 because Daser didn't allow it.
        // - Consider blocks 188, 189, 192 as synced because they exists in pruned ranges.
        // - Remove 103-120, 147-154, 167-174, 187, 190-191, 193-195.
        // - Keep 146, 155, 166, 175, 186 blocks because they are edges
        //   (i.e. the blocks that are neightbors of non-synced ranges).
        // - Keep 196-199 because they are in sampling window and not sampled yet.
        // - Keep 202-260 because they are in pruning window.
        sleep(Duration::from_millis(100)).await;
        assert_pruned_headers_event(&mut event_subscriber, 1, 100).await;
        assert_pruned_headers_event(&mut event_subscriber, 102, 120).await;
        assert_pruned_headers_event(&mut event_subscriber, 147, 154).await;
        assert_pruned_headers_event(&mut event_subscriber, 167, 174).await;
        assert_pruned_headers_event(&mut event_subscriber, 187, 187).await;
        assert_pruned_headers_event(&mut event_subscriber, 190, 191).await;
        assert_pruned_headers_event(&mut event_subscriber, 193, 195).await;
        assert_eq!(
            store.get_stored_header_ranges().await.unwrap(),
            new_block_ranges([
                101..=101,
                146..=146,
                155..=155,
                166..=166,
                175..=175,
                186..=186,
                196..=199,
                202..=260
            ])
        );
        assert_eq!(
            store.get_pruned_ranges().await.unwrap(),
            new_block_ranges([1..=100, 102..=120, 147..=154, 167..=174, 187..=195])
        );
        assert_eq!(
            daser_handle.expect_update_number_of_prunable_blocks().await,
            1
        );

        // Now Pruner will ask again for 101, but this time we allow it.
        let (want_to_prune, respond_to) = daser_handle.expect_want_to_prune().await;
        assert_eq!(want_to_prune, 101);
        respond_to.send(true).unwrap();

        assert_eq!(
            daser_handle.expect_update_number_of_prunable_blocks().await,
            0
        );

        assert_pruned_headers_event(&mut event_subscriber, 101, 101).await;
        assert_eq!(
            store.get_stored_header_ranges().await.unwrap(),
            new_block_ranges([
                146..=146,
                155..=155,
                166..=166,
                175..=175,
                186..=186,
                196..=199,
                202..=260
            ])
        );
        assert_eq!(
            store.get_pruned_ranges().await.unwrap(),
            new_block_ranges([1..=120, 147..=154, 167..=174, 187..=195])
        );

        daser_handle.expect_no_cmd().await;
        pruner.stop();
        pruner.join().await;

        assert!(matches!(
            event_subscriber.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));
    }

    async fn assert_pruned_headers_event(
        subscriber: &mut EventSubscriber,
        from_height: u64,
        to_height: u64,
    ) {
        let event = timeout(Duration::from_secs(1), subscriber.recv())
            .await
            .unwrap_or_else(|_| {
                panic!("Expecting PrunedHeaders({from_height}-{to_height}) event but nothing is received")
            })
            .unwrap()
            .event;

        match event {
            NodeEvent::PrunedHeaders {
                from_height: from,
                to_height: to,
            } => {
                assert_eq!((from, to), (from_height, to_height));
            }
            ev => panic!(
                "Expecting PrunedHeaders({from_height}-{to_height}) event, but received: {ev:?}"
            ),
        }
    }

    #[derive(Debug, PartialEq, Clone, Copy)]
    struct TestBlock(pub [u8; 8]);

    impl From<u64> for TestBlock {
        fn from(value: u64) -> Self {
            TestBlock(value.to_le_bytes())
        }
    }

    impl Block<64> for TestBlock {
        fn cid(&self) -> Result<CidGeneric<64>, CidError> {
            let mh = Multihash::wrap(TEST_MH_CODE, &self.0).unwrap();
            Ok(CidGeneric::new_v1(TEST_CODEC, mh))
        }

        fn data(&self) -> &[u8] {
            &self.0
        }
    }
}
