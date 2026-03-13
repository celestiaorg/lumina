//! Row assignment map for validator uploads.

use std::collections::{HashMap, HashSet};

use crate::error::FibreError;

/// Maps validator index to the row indices assigned to that validator.
///
/// The validator index corresponds to the position in the validator list.
/// Row indices are positions in the extended data matrix (`0..total_rows`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardMap {
    inner: HashMap<usize, Vec<usize>>,
}

impl ShardMap {
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

    /// Verify that a validator's claimed row indices match their assignment.
    pub fn verify(&self, validator_index: usize, row_indices: &[u32]) -> Result<(), FibreError> {
        let rows = self.inner.get(&validator_index).ok_or_else(|| {
            FibreError::InvalidData(format!(
                "validator index {} not in shard map",
                validator_index
            ))
        })?;

        if row_indices.len() != rows.len() {
            return Err(FibreError::InvalidData(format!(
                "expected {} rows, got {}",
                rows.len(),
                row_indices.len()
            )));
        }

        let assigned_set: HashSet<usize> = rows.iter().copied().collect();

        for &row_idx in row_indices {
            if !assigned_set.contains(&(row_idx as usize)) {
                return Err(FibreError::InvalidData(format!(
                    "row {} not assigned to validator {}",
                    row_idx, validator_index
                )));
            }
        }

        Ok(())
    }
}
