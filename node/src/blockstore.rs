//! Blockstore types aliases with lumina specific constants.

use crate::p2p::MAX_MH_SIZE;

/// An [`InMemoryBlockstore`] with maximum multihash size used by lumina.
///
/// [`InMemoryBlockstore`]: blockstore::InMemoryBlockstore
pub type InMemoryBlockstore = blockstore::InMemoryBlockstore<MAX_MH_SIZE>;

#[cfg(not(target_arch = "wasm32"))]
/// A [`RedbBlockstore`].
///
/// [`RedbBlockstore`]: blockstore::RedbBlockstore
pub type RedbBlockstore = blockstore::RedbBlockstore;

#[cfg(target_arch = "wasm32")]
/// An [`IndexedDbBlockstore`].
///
/// [`IndexedDbBlockstore`]: blockstore::IndexedDbBlockstore
pub type IndexedDbBlockstore = blockstore::IndexedDbBlockstore;
