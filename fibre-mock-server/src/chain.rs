//! Shared fake-chain state for the app-node services: identity of the chain
//! plus the set of "committed" transactions.

use std::collections::HashMap;
use std::sync::Mutex;

use celestia_proto::cosmos::tx::v1beta1::Tx;

struct CommittedTxs {
    heights: HashMap<String, i64>,
    txs: Vec<Tx>,
}

/// Minimal chain state backing the app-node gRPC services.
pub struct MockChain {
    /// Chain id served in the latest-block header and used for tx signing.
    pub chain_id: String,
    /// Height every response reports; txs "commit" at this height.
    pub height: i64,
    /// Proposer address for block headers (any 20 bytes; validator 0's).
    pub proposer: [u8; 20],
    /// Uppercase-hex tx hash → committed height. Held only for synchronous
    /// lookups; never across an await.
    txs: Mutex<CommittedTxs>,
}

impl MockChain {
    pub fn new(chain_id: String, height: i64, proposer: [u8; 20]) -> Self {
        Self {
            chain_id,
            height,
            proposer,
            txs: Mutex::new(CommittedTxs {
                heights: HashMap::new(),
                txs: Vec::new(),
            }),
        }
    }

    /// Record a broadcast tx as instantly committed; returns its height.
    pub fn commit_tx(&self, hash_hex_upper: String, tx: Tx) -> i64 {
        let mut txs = self.txs.lock().expect("mock chain lock poisoned");
        txs.heights.insert(hash_hex_upper, self.height);
        txs.txs.push(tx);
        self.height
    }

    /// Committed height of a tx, if the hash was ever broadcast.
    pub fn tx_height(&self, hash_hex_upper: &str) -> Option<i64> {
        self.txs
            .lock()
            .expect("mock chain lock poisoned")
            .heights
            .get(hash_hex_upper)
            .copied()
    }

    pub fn committed_txs(&self) -> Vec<Tx> {
        self.txs
            .lock()
            .expect("mock chain lock poisoned")
            .txs
            .clone()
    }
}
