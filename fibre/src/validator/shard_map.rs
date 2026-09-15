//! Row assignment map for validator uploads.

use std::collections::HashMap;

/// Maps validator index to the row indices assigned to that validator.
///
/// The validator index corresponds to the position in the validator list.
/// Row indices are positions in the extended data matrix (`0..total_rows`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMap {
    inner: HashMap<usize, Vec<usize>>,
}

impl ShardMap {
    /// Convenience method which initializes ShardMap with a HashMap
    pub(crate) fn new(inner: HashMap<usize, Vec<usize>>) -> Self {
        Self { inner }
    }

    /// Returns the inner map.
    pub fn inner(&self) -> &HashMap<usize, Vec<usize>> {
        &self.inner
    }

    /// Returns the row indices assigned to the given validator index.
    pub fn get(&self, validator_index: usize) -> Option<&Vec<usize>> {
        self.inner.get(&validator_index)
    }

    /// Returns the number of validators in the shard map.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns true if the shard map contains no validators.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}
