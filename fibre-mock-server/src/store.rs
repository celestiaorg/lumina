//! In-memory shard storage for one mock validator, keyed by blob commitment.
//! Unbounded by design: this is a load-test backend, blobs live for the
//! lifetime of the process.

use std::collections::HashMap;
use std::sync::Arc;

use celestia_proto::celestia::fibre::v1::BlobShard;

/// Commitment-keyed shard storage for one validator.
#[derive(Default)]
pub struct ShardStore {
    shards: std::sync::RwLock<HashMap<[u8; 32], Arc<BlobShard>>>,
}

impl ShardStore {
    /// Store a shard, replacing any previous shard for the same commitment.
    pub fn insert(&self, commitment: [u8; 32], shard: BlobShard) {
        self.shards
            .write()
            .expect("shard store lock poisoned")
            .insert(commitment, Arc::new(shard));
    }

    /// Fetch the shard stored for a commitment, if any.
    pub fn get(&self, commitment: &[u8; 32]) -> Option<Arc<BlobShard>> {
        self.shards
            .read()
            .expect("shard store lock poisoned")
            .get(commitment)
            .cloned()
    }
}
