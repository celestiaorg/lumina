//! In-memory shard storage for one mock validator, keyed by blob commitment.
//! Bounded by bytes: when the budget is exceeded the oldest shards are
//! evicted, so no blob size can OOM the host. A download of an evicted blob
//! gets NotFound, so keep the budget comfortably above
//! max-in-flight × shard size.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use celestia_proto::celestia::fibre::v1::BlobShard;
use prost::Message;

/// Commitment-keyed, FIFO-evicted, byte-bounded shard storage for one validator.
pub struct ShardStore {
    budget_bytes: u64,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// Shard plus its approximate size (proto-encoded length).
    shards: HashMap<[u8; 32], (Arc<BlobShard>, u64)>,
    /// Insertion order; front is the eviction candidate.
    order: VecDeque<[u8; 32]>,
    total_bytes: u64,
}

impl ShardStore {
    /// Store shards up to `budget_bytes` total (must be non-zero), evicting
    /// oldest-first. The newest shard is always kept, even over budget.
    pub fn new(budget_bytes: u64) -> Self {
        assert!(budget_bytes > 0, "budget_bytes must be non-zero");
        Self {
            budget_bytes,
            inner: Mutex::new(Inner::default()),
        }
    }

    /// Store a shard, replacing any previous shard for the same commitment
    /// (keeping its age), then evict oldest shards down to the byte budget.
    pub fn insert(&self, commitment: [u8; 32], shard: BlobShard) {
        let size = shard.encoded_len() as u64;
        let mut inner = self.inner.lock().expect("shard store lock poisoned");
        match inner.shards.insert(commitment, (Arc::new(shard), size)) {
            Some((_, old_size)) => inner.total_bytes -= old_size,
            None => inner.order.push_back(commitment),
        }
        inner.total_bytes += size;

        while inner.total_bytes > self.budget_bytes && inner.shards.len() > 1 {
            let oldest = inner
                .order
                .pop_front()
                .expect("order tracks every stored shard");
            let (_, evicted_size) = inner
                .shards
                .remove(&oldest)
                .expect("order entries are stored");
            inner.total_bytes -= evicted_size;
            tracing::debug!(commitment = %hex::encode(oldest), "evicted shard");
        }
    }

    /// Fetch the shard stored for a commitment, if any.
    pub fn get(&self, commitment: &[u8; 32]) -> Option<Arc<BlobShard>> {
        self.inner
            .lock()
            .expect("shard store lock poisoned")
            .shards
            .get(commitment)
            .map(|(shard, _)| shard.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(marker: u8, size: usize) -> BlobShard {
        BlobShard {
            rows: vec![],
            rlcs: vec![marker; size],
        }
    }

    #[test]
    fn evicts_oldest_beyond_byte_budget() {
        // Each shard encodes to a bit over 100 bytes; budget fits two, not three.
        let store = ShardStore::new(250);
        store.insert([1; 32], shard(1, 100));
        store.insert([2; 32], shard(2, 100));
        store.insert([3; 32], shard(3, 100));

        assert!(store.get(&[1; 32]).is_none(), "oldest must be evicted");
        assert!(store.get(&[2; 32]).is_some());
        assert!(store.get(&[3; 32]).is_some());
    }

    #[test]
    fn reinsert_replaces_without_double_counting() {
        let store = ShardStore::new(250);
        store.insert([1; 32], shard(1, 100));
        store.insert([2; 32], shard(2, 100));
        store.insert([1; 32], shard(9, 100));

        assert_eq!(store.get(&[1; 32]).unwrap().rlcs, vec![9; 100]);
        assert!(store.get(&[2; 32]).is_some(), "reinsert must not evict");
    }

    #[test]
    fn oversized_newest_shard_is_kept() {
        let store = ShardStore::new(50);
        store.insert([1; 32], shard(1, 100));
        assert!(
            store.get(&[1; 32]).is_some(),
            "a single over-budget shard must survive"
        );

        store.insert([2; 32], shard(2, 100));
        assert!(store.get(&[1; 32]).is_none());
        assert!(store.get(&[2; 32]).is_some());
    }
}
