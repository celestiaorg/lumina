use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
};

use crate::tx_client_v2::{NodeId, Transaction, TxIdT};

struct SubmissionState {
    last_submitted: Option<u64>,
    pending: Option<u64>,
}

impl SubmissionState {
    fn new() -> Self {
        SubmissionState {
            last_submitted: None,
            pending: None,
        }
    }
}

pub struct TxInternal<TxId: TxIdT + Eq + Hash, ConfirmInfo> {
    confirmed: u64,
    transactions: VecDeque<Transaction<TxId, ConfirmInfo>>,
    submissions: HashMap<NodeId, SubmissionState>,
    id_to_seq: HashMap<TxId, u64>,
    confirm_slot: Option<ConfirmSlot<ConfirmInfo>>,
}

struct ConfirmSlot<ConfirmInfo> {
    seq: u64,
    info: ConfirmInfo,
}

#[derive(Debug)]
pub enum TxInternalError {
    TransactionNotFound,
    ConfirmWithGaps,
    #[allow(dead_code)]
    AdvancePastMaxSeq,
    InvalidSequence,
}

type TxInternalResult<T> = Result<T, TxInternalError>;

impl<TxId: TxIdT + Eq + Hash, ConfirmInfo> TxInternal<TxId, ConfirmInfo> {
    pub fn new(confirmed: u64, nodes: Vec<NodeId>) -> Self {
        let mut nodes_map = HashMap::new();
        for node in nodes {
            nodes_map.insert(node, SubmissionState::new());
        }
        TxInternal {
            confirmed,
            transactions: VecDeque::new(),
            submissions: nodes_map,
            id_to_seq: HashMap::new(),
            confirm_slot: None,
        }
    }

    pub fn max_seq(&self) -> u64 {
        self.confirmed + self.transactions.len() as u64
    }

    pub fn confirmed_seq(&self) -> u64 {
        self.confirmed
    }

    pub fn len(&self) -> usize {
        self.transactions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    pub fn add_transaction(&mut self, tx: Transaction<TxId, ConfirmInfo>) -> TxInternalResult<()> {
        if self.confirmed == 0 && self.transactions.is_empty() {
            self.confirmed = tx.sequence - 1;
        }
        if tx.sequence != self.max_seq() + 1 {
            return Err(TxInternalError::InvalidSequence);
        }
        self.transactions.push_back(tx);
        Ok(())
    }

    pub fn submit_success(&mut self, node: &NodeId, id: TxId, seq: u64) -> TxInternalResult<()> {
        let state = self.submissions.get_mut(node).unwrap();
        if let Some(pending) = state.pending {
            if seq != pending {
                return Err(TxInternalError::InvalidSequence);
            }
            self.id_to_seq.insert(id.clone(), pending);
            state.pending = None;
            state.last_submitted = match state.last_submitted {
                None => Some(seq),
                Some(last) => {
                    if last + 1 != seq {
                        Some(last)
                    } else {
                        Some(seq)
                    }
                }
            };
            let Some(tx) = self.get_mut(seq) else {
                return Ok(()); // tx already confirmed by another node
            };
            tx.id = Some(id);
            Ok(())
        } else {
            Err(TxInternalError::InvalidSequence)
        }
    }

    pub fn submit_failure(&mut self, node: &NodeId, seq: u64) -> TxInternalResult<()> {
        let state = self.submissions.get_mut(node).unwrap();
        if let Some(pending) = state.pending {
            if seq != pending {
                return Err(TxInternalError::InvalidSequence);
            }
            state.pending = None;
            Ok(())
        } else {
            Err(TxInternalError::TransactionNotFound)
        }
    }

    fn tx_idx(&self, sequence: u64) -> Option<usize> {
        let idx = sequence.checked_sub(self.confirmed + 1)? as usize;
        (idx < self.transactions.len()).then_some(idx)
    }

    pub fn submit_start(&mut self, node: &NodeId) -> Option<&Transaction<TxId, ConfirmInfo>> {
        let to_submit = {
            let state = self.submissions.get_mut(node)?;
            if state.pending.is_some() {
                return None;
            }
            if state.last_submitted.is_none() {
                self.transactions.front()
            } else {
                let last_submitted = state.last_submitted.unwrap();
                let next_seq = last_submitted + 1;
                let idx = self.tx_idx(next_seq)?;
                self.transactions.get(idx)
            }
        };
        if let Some(tx) = to_submit {
            self.submissions.get_mut(node)?.pending = Some(tx.sequence);
            Some(tx)
        } else {
            None
        }
    }

    pub fn peek_submit(&self, node: &NodeId) -> Option<&Transaction<TxId, ConfirmInfo>> {
        let state = self.submissions.get(node)?;
        if state.pending.is_some() {
            return None;
        }
        if let Some(last_submitted) = state.last_submitted {
            let next_seq = last_submitted + 1;
            let idx = self.tx_idx(next_seq)?;
            self.transactions.get(idx)
        } else {
            self.transactions.front()
        }
    }

    pub fn get_by_id(&self, id: &TxId) -> Option<&Transaction<TxId, ConfirmInfo>> {
        self.id_to_seq
            .get(id)
            .and_then(|seq| self.tx_idx(*seq))
            .and_then(|idx| self.transactions.get(idx))
    }

    pub fn get_seq(&self, id: &TxId) -> Option<u64> {
        self.id_to_seq.get(id).cloned()
    }

    #[allow(dead_code)]
    pub fn get_pending(&self, node: &NodeId) -> Option<u64> {
        self.submissions.get(node).and_then(|s| s.pending)
    }

    pub fn get(&self, seq: u64) -> Option<&Transaction<TxId, ConfirmInfo>> {
        self.tx_idx(seq).and_then(|idx| self.transactions.get(idx))
    }

    pub fn get_mut(&mut self, seq: u64) -> Option<&mut Transaction<TxId, ConfirmInfo>> {
        self.tx_idx(seq)
            .and_then(|idx| self.transactions.get_mut(idx))
    }

    pub fn confirm(&mut self, seq: u64) -> TxInternalResult<Transaction<TxId, ConfirmInfo>> {
        if seq != self.confirmed + 1 {
            return Err(TxInternalError::ConfirmWithGaps);
        }
        let Some(first) = self.transactions.front() else {
            return Err(TxInternalError::TransactionNotFound);
        };
        if first.sequence != seq {
            return Err(TxInternalError::ConfirmWithGaps);
        }
        let tx = self
            .transactions
            .pop_front()
            .ok_or(TxInternalError::TransactionNotFound)?;
        if let Some(id) = tx.id.clone() {
            self.id_to_seq.remove(&id);
        }
        self.confirmed += 1;
        self.force_update_all_nodes(seq);
        Ok(tx)
    }

    pub fn to_confirm(&mut self, node: &NodeId, limit: usize) -> Option<Vec<TxId>> {
        let last = self.submissions.get(node).unwrap().last_submitted?;
        self.tx_idx(last).map(|idx| {
            self.transactions
                .iter()
                .take(limit.min(idx + 1))
                .filter(|tx| tx.id.is_some())
                .map(|tx| tx.id.clone().unwrap())
                .collect::<Vec<_>>()
        })
    }

    pub fn truncate_right_from(&mut self, seq: u64) -> Option<Vec<Transaction<TxId, ConfirmInfo>>> {
        if let Some(idx) = self.tx_idx(seq) {
            let count = self.transactions.len() - idx;
            let mut tx_ret = Vec::new();
            for _ in 0..count {
                let tx = self.transactions.pop_back().unwrap();
                if let Some(id) = tx.id.as_ref() {
                    self.id_to_seq.remove(id);
                }
                tx_ret.push(tx);
            }
            Some(tx_ret)
        } else {
            None
        }
    }

    pub fn reset_submitted(&mut self, node: &NodeId, seq: u64) {
        let clamped = seq.max(self.confirmed).min(self.max_seq());
        let state = self.submissions.get_mut(node).unwrap();
        if let Some(last_submitted) = state.last_submitted.as_mut() {
            *last_submitted = clamped;
        } else {
            state.last_submitted = Some(clamped);
        }
    }

    pub fn try_fill_confirm_slot(&mut self, seq: u64, info: ConfirmInfo) -> bool {
        if seq != self.confirmed + 1 {
            return false;
        }
        if self.confirm_slot.is_some() {
            return false;
        }
        self.confirm_slot = Some(ConfirmSlot { seq, info });
        true
    }

    pub fn take_confirm_slot(&mut self) -> Option<(u64, ConfirmInfo)> {
        self.confirm_slot.take().map(|slot| (slot.seq, slot.info))
    }

    fn force_update_all_nodes(&mut self, seq: u64) {
        for (_, state) in self.submissions.iter_mut() {
            if let Some(last) = state.last_submitted.as_mut() {
                *last = (*last).max(seq);
            } else {
                state.last_submitted = Some(seq);
            }
        }
    }
}

/// # TxInternal Invariants (Updated)
///
/// ## Core Data Structures
///
/// `SubmissionState` tracks per-node submission progress:
/// - `last_submitted: Option<u64>` - sequence of last successfully submitted tx
/// - `pending: Option<u64>` - sequence of in-flight submission
///
/// ## Submission State Machine
///
/// ```text
/// [Idle: pending=None]
///     |
///     +--submit_start()--> [Pending: pending=Some(seq)]
///     |                         |
///     |                         +--submit_success()--> [Idle, last_submitted updated]
///     |                         |
///     |                         +--submit_failure()--> [Idle, last_submitted unchanged]
///     |
///     +--submit_start() when pending!=None --> returns None (blocked)
/// ```
///
/// ## Key Invariants
///
/// ### INV-1: add_transaction Auto-Adjusts confirmed
/// When confirmed==0, `add_transaction` sets `confirmed = tx.sequence - 1` before validation.
/// This allows starting from arbitrary sequence numbers.
/// NOTE: Only the FIRST transaction should trigger this; subsequent adds already have confirmed!=0.
///
/// ### INV-2: submit_start Blocks If Pending Exists
/// `submit_start` returns `None` if `pending` is already `Some`.
/// You must call `submit_success` or `submit_failure` before starting another submission.
///
/// ### INV-3: submit_success Updates last_submitted
/// - If last_submitted=None: sets to Some(seq) (first submission initializes it)
/// - If last_submitted=Some(n) and n+1==seq: sets to Some(seq) (contiguous)
/// - If last_submitted=Some(n) and n+1!=seq: keeps Some(n) (gap detected)
///
/// ### INV-4: reset() Sets last_submitted
/// `reset(node, seq)` always sets `last_submitted = Some(clamped_seq)`.
/// Can be used to initialize or adjust tracking position.
///
/// ### INV-5: to_confirm Requires last_submitted
/// Returns `None` if `last_submitted` is `None`.
///
/// ### INV-6: confirm() Force-Updates All Nodes
/// After confirm, all nodes have `last_submitted >= confirmed_seq`.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx_client_v2::TxCallbacks;
    use std::sync::Arc;

    fn make_tx(sequence: u64, id: Option<u64>) -> Transaction<u64, ()> {
        Transaction {
            sequence,
            bytes: Arc::new(vec![sequence as u8]),
            callbacks: TxCallbacks::default(),
            id,
        }
    }

    fn make_internal(confirmed: u64, node_names: &[&str]) -> TxInternal<u64, ()> {
        let nodes: Vec<NodeId> = node_names.iter().map(|n| Arc::from(*n)).collect();
        TxInternal::new(confirmed, nodes)
    }

    // =========================================================================
    // Basic Properties
    // =========================================================================

    mod creation_and_basic_properties {
        use super::*;

        #[test]
        fn new_creates_empty_internal() {
            let internal = make_internal(10, &["node1", "node2"]);
            assert_eq!(internal.confirmed_seq(), 10);
            assert_eq!(internal.max_seq(), 10);
            assert_eq!(internal.len(), 0);
            assert!(internal.is_empty());
        }

        #[test]
        fn max_seq_equals_confirmed_plus_len() {
            let mut internal = make_internal(5, &["node1"]);
            assert_eq!(internal.max_seq(), 5);

            internal.add_transaction(make_tx(6, None)).unwrap();
            assert_eq!(internal.max_seq(), 6);

            internal.add_transaction(make_tx(7, None)).unwrap();
            assert_eq!(internal.max_seq(), 7);
        }
    }

    // =========================================================================
    // INV-1: add_transaction Auto-Adjusts confirmed
    // =========================================================================

    mod add_transaction {
        use super::*;

        #[test]
        fn auto_adjusts_confirmed_when_zero() {
            let mut internal = make_internal(0, &["node1"]);
            // confirmed=0, adding tx with seq=5 should set confirmed=4
            internal.add_transaction(make_tx(5, None)).unwrap();
            assert_eq!(internal.confirmed_seq(), 4);
            assert_eq!(internal.max_seq(), 5);
        }

        #[test]
        fn requires_contiguous_sequence() {
            let mut internal = make_internal(5, &["node1"]);
            // max_seq=5, must add seq=6
            let result = internal.add_transaction(make_tx(8, None));
            assert!(matches!(result, Err(TxInternalError::InvalidSequence)));
        }

        #[test]
        fn add_multiple_in_sequence() {
            let mut internal = make_internal(5, &["node1"]);
            for seq in 6..=10 {
                internal
                    .add_transaction(make_tx(seq, Some(seq * 100)))
                    .unwrap();
            }
            assert_eq!(internal.len(), 5);
            assert_eq!(internal.max_seq(), 10);
        }

        #[test]
        fn rejects_duplicate_sequence() {
            let mut internal = make_internal(5, &["node1"]);
            internal.add_transaction(make_tx(6, None)).unwrap();
            let result = internal.add_transaction(make_tx(6, None));
            assert!(matches!(result, Err(TxInternalError::InvalidSequence)));
        }
    }

    // =========================================================================
    // INV-2: submit_start Blocks If Pending Exists
    // =========================================================================

    mod submit_start {
        use super::*;

        #[test]
        fn returns_none_on_empty_queue() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            assert!(internal.submit_start(&node).is_none());
        }

        #[test]
        fn returns_first_tx_when_nothing_submitted() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, Some(600))).unwrap();
            internal.add_transaction(make_tx(7, Some(700))).unwrap();

            let tx = internal.submit_start(&node).unwrap();
            assert_eq!(tx.sequence, 6);
        }

        #[test]
        fn sets_pending() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, None)).unwrap();

            internal.submit_start(&node);
            // Verify pending is set by checking submit_success works
            assert!(internal.submit_success(&node, 100, 6).is_ok());
        }

        #[test]
        fn blocks_when_pending_exists() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, None)).unwrap();

            internal.submit_start(&node); // Sets pending
            // Second submit_start should return None
            assert!(internal.submit_start(&node).is_none());
        }

        #[test]
        fn returns_none_for_unknown_node() {
            let mut internal = make_internal(5, &["node1"]);
            let unknown: NodeId = Arc::from("unknown");
            internal.add_transaction(make_tx(6, None)).unwrap();
            assert!(internal.submit_start(&unknown).is_none());
        }
    }

    // =========================================================================
    // INV-3: submit_success Updates last_submitted Conditionally
    // =========================================================================

    mod submit_success {
        use super::*;

        #[test]
        fn fails_without_pending() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, None)).unwrap();

            let result = internal.submit_success(&node, 100, 6);
            assert!(matches!(result, Err(TxInternalError::InvalidSequence)));
        }

        #[test]
        fn fails_with_wrong_sequence() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, None)).unwrap();

            internal.submit_start(&node);
            let result = internal.submit_success(&node, 100, 7); // wrong seq
            assert!(matches!(result, Err(TxInternalError::InvalidSequence)));
        }

        #[test]
        fn clears_pending_and_adds_id() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, None)).unwrap();

            assert!(internal.get_seq(&100).is_none());
            internal.submit_start(&node);
            internal.submit_success(&node, 100, 6).unwrap();

            assert_eq!(internal.get_seq(&100), Some(6));
            // Pending cleared, can submit_start again
            // Since last_submitted is now set to 6, and there's no seq 7, returns None
            assert!(internal.submit_start(&node).is_none());
        }

        #[test]
        fn first_success_sets_last_submitted() {
            // INV-3: When last_submitted=None, first submission sets it to Some(seq)
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, None)).unwrap();
            internal.add_transaction(make_tx(7, None)).unwrap();

            internal.submit_start(&node);
            internal.submit_success(&node, 100, 6).unwrap();

            // last_submitted is now Some(6), so submit_start returns seq 7
            let tx = internal.submit_start(&node).unwrap();
            assert_eq!(tx.sequence, 7);
        }

        #[test]
        fn contiguous_success_updates_last_submitted() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, None)).unwrap();
            internal.add_transaction(make_tx(7, None)).unwrap();
            internal.add_transaction(make_tx(8, None)).unwrap();

            // Initialize last_submitted via reset
            internal.reset_submitted(&node, 5);

            // Submit seq 6 (last_submitted=5, so 5+1=6 is contiguous)
            internal.submit_start(&node);
            internal.submit_success(&node, 100, 6).unwrap();

            // Now last_submitted=6, next submit_start returns seq 7
            let tx = internal.submit_start(&node).unwrap();
            assert_eq!(tx.sequence, 7);
        }
    }

    // =========================================================================
    // submit_failure
    // =========================================================================

    mod submit_failure {
        use super::*;

        #[test]
        fn fails_without_pending() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, None)).unwrap();

            let result = internal.submit_failure(&node, 6);
            assert!(matches!(result, Err(TxInternalError::TransactionNotFound)));
        }

        #[test]
        fn clears_pending_allows_retry() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, Some(600))).unwrap();

            internal.submit_start(&node);
            internal.submit_failure(&node, 6).unwrap();

            // Can retry
            let tx = internal.submit_start(&node).unwrap();
            assert_eq!(tx.sequence, 6);

            // This time succeed
            internal.submit_success(&node, 600, 6).unwrap();
            assert_eq!(internal.get_seq(&600), Some(6));
        }

        #[test]
        fn does_not_add_id_to_mapping() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, None)).unwrap();

            internal.submit_start(&node);
            internal.submit_failure(&node, 6).unwrap();

            assert!(internal.get_seq(&100).is_none());
        }
    }

    // =========================================================================
    // INV-5: to_confirm Requires last_submitted
    // =========================================================================

    mod to_confirm {
        use super::*;

        #[test]
        fn returns_none_when_last_submitted_is_none() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, Some(600))).unwrap();

            assert!(internal.to_confirm(&node, 10).is_none());
        }

        #[test]
        fn returns_ids_up_to_last_submitted() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, Some(600))).unwrap();
            internal.add_transaction(make_tx(7, Some(700))).unwrap();
            internal.add_transaction(make_tx(8, Some(800))).unwrap();

            // Set last_submitted to 7 via reset
            internal.reset_submitted(&node, 7);

            let ids = internal.to_confirm(&node, 10).unwrap();
            assert_eq!(ids, vec![600, 700]);
        }

        #[test]
        fn respects_limit() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            for seq in 6..=10 {
                internal
                    .add_transaction(make_tx(seq, Some(seq * 100)))
                    .unwrap();
            }
            internal.reset_submitted(&node, 10);

            let ids = internal.to_confirm(&node, 2).unwrap();
            assert_eq!(ids, vec![600, 700]);
        }

        #[test]
        fn filters_none_ids() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, Some(600))).unwrap();
            internal.add_transaction(make_tx(7, None)).unwrap();
            internal.add_transaction(make_tx(8, Some(800))).unwrap();

            internal.reset_submitted(&node, 8);

            let ids = internal.to_confirm(&node, 10).unwrap();
            assert_eq!(ids, vec![600, 800]);
        }
    }

    // =========================================================================
    // INV-4: reset() Sets last_submitted
    // =========================================================================

    mod reset {
        use super::*;

        #[test]
        fn sets_last_submitted() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, None)).unwrap();
            internal.add_transaction(make_tx(7, None)).unwrap();

            internal.reset_submitted(&node, 6);

            // submit_start should return seq 7 (last_submitted=6, so next is 7)
            let tx = internal.submit_start(&node).unwrap();
            assert_eq!(tx.sequence, 7);
        }

        #[test]
        fn clamps_to_confirmed() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, Some(600))).unwrap();

            // Try to reset below confirmed
            internal.reset_submitted(&node, 2);

            // last_submitted should be clamped to 5
            // to_confirm returns None because tx_idx(5) is None
            assert!(internal.to_confirm(&node, 10).is_none());
        }

        #[test]
        fn clamps_to_max_seq() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, Some(600))).unwrap();
            internal.add_transaction(make_tx(7, Some(700))).unwrap();

            // max_seq = 7, try to reset to 100
            internal.reset_submitted(&node, 100);

            let ids = internal.to_confirm(&node, 10).unwrap();
            assert_eq!(ids, vec![600, 700]);
        }
    }

    // =========================================================================
    // INV-6: confirm() Force-Updates All Nodes
    // =========================================================================

    mod confirm {
        use super::*;

        #[test]
        fn must_be_sequential() {
            let mut internal = make_internal(5, &["node1"]);
            internal.add_transaction(make_tx(6, None)).unwrap();
            internal.add_transaction(make_tx(7, None)).unwrap();

            let result = internal.confirm(7);
            assert!(matches!(result, Err(TxInternalError::ConfirmWithGaps)));
        }

        #[test]
        fn fails_on_empty() {
            let mut internal = make_internal(5, &["node1"]);
            let result = internal.confirm(6);
            assert!(matches!(result, Err(TxInternalError::TransactionNotFound)));
        }

        #[test]
        fn removes_tx_and_increments_confirmed() {
            let mut internal = make_internal(5, &["node1"]);
            internal.add_transaction(make_tx(6, Some(600))).unwrap();

            let tx = internal.confirm(6).unwrap();
            assert_eq!(tx.sequence, 6);
            assert_eq!(internal.confirmed_seq(), 6);
            assert!(internal.is_empty());
        }

        #[test]
        fn removes_id_from_mapping() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, Some(600))).unwrap();

            internal.reset_submitted(&node, 5);
            internal.submit_start(&node);
            internal.submit_success(&node, 600, 6).unwrap();

            assert!(internal.get_seq(&600).is_some());
            internal.confirm(6).unwrap();
            assert!(internal.get_seq(&600).is_none());
        }

        #[test]
        fn force_updates_all_nodes() {
            let mut internal = make_internal(5, &["node1", "node2"]);
            let node1: NodeId = Arc::from("node1");
            let node2: NodeId = Arc::from("node2");

            internal.add_transaction(make_tx(6, None)).unwrap();
            internal.add_transaction(make_tx(7, None)).unwrap();

            // node1 has submitted via reset, node2 hasn't
            internal.reset_submitted(&node1, 6);

            internal.confirm(6).unwrap();

            // Both nodes should now have last_submitted >= 6
            // node2 should now see seq 7 as next
            let tx = internal.submit_start(&node2).unwrap();
            assert_eq!(tx.sequence, 7);
        }
    }

    // =========================================================================
    // truncate_right_from
    // =========================================================================

    mod truncate_right_from {
        use super::*;

        #[test]
        fn removes_from_seq_onwards() {
            let mut internal = make_internal(5, &["node1"]);
            for seq in 6..=10 {
                internal
                    .add_transaction(make_tx(seq, Some(seq * 100)))
                    .unwrap();
            }

            let removed = internal.truncate_right_from(8).unwrap();
            assert_eq!(removed.len(), 3);
            // Returned in reverse order
            assert_eq!(removed[0].sequence, 10);
            assert_eq!(removed[1].sequence, 9);
            assert_eq!(removed[2].sequence, 8);

            assert_eq!(internal.max_seq(), 7);
        }

        #[test]
        fn removes_ids_from_mapping() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");

            for seq in 6..=8 {
                internal
                    .add_transaction(make_tx(seq, Some(seq * 100)))
                    .unwrap();
            }

            // Submit all
            internal.reset_submitted(&node, 5);
            for seq in 6..=8 {
                internal.submit_start(&node);
                internal.submit_success(&node, seq * 100, seq).unwrap();
            }

            assert!(internal.get_seq(&700).is_some());
            assert!(internal.get_seq(&800).is_some());

            internal.truncate_right_from(7).unwrap();

            assert!(internal.get_seq(&600).is_some());
            assert!(internal.get_seq(&700).is_none());
            assert!(internal.get_seq(&800).is_none());
        }

        #[test]
        fn returns_none_for_invalid_seq() {
            let mut internal = make_internal(5, &["node1"]);
            internal.add_transaction(make_tx(6, None)).unwrap();

            assert!(internal.truncate_right_from(5).is_none()); // confirmed
            assert!(internal.truncate_right_from(10).is_none()); // beyond max
        }
    }

    // =========================================================================
    // get methods
    // =========================================================================

    mod get_methods {
        use super::*;

        #[test]
        fn get_by_sequence() {
            let mut internal = make_internal(5, &["node1"]);
            internal.add_transaction(make_tx(6, Some(600))).unwrap();
            internal.add_transaction(make_tx(7, Some(700))).unwrap();

            assert_eq!(internal.get(6).unwrap().id, Some(600));
            assert_eq!(internal.get(7).unwrap().id, Some(700));
            assert!(internal.get(5).is_none());
            assert!(internal.get(8).is_none());
        }

        #[test]
        fn get_by_id_requires_submit_success() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");
            internal.add_transaction(make_tx(6, Some(600))).unwrap();

            assert!(internal.get_by_id(&600).is_none());

            internal.submit_start(&node);
            assert!(internal.get_by_id(&600).is_none());

            internal.submit_success(&node, 600, 6).unwrap();
            assert_eq!(internal.get_by_id(&600).unwrap().sequence, 6);
        }

        #[test]
        fn get_mut_modifies() {
            let mut internal = make_internal(5, &["node1"]);
            internal.add_transaction(make_tx(6, Some(600))).unwrap();

            internal.get_mut(6).unwrap().id = Some(999);
            assert_eq!(internal.get(6).unwrap().id, Some(999));
        }
    }

    // =========================================================================
    // Multi-Node Independence
    // =========================================================================

    mod multi_node {
        use super::*;

        #[test]
        fn nodes_have_independent_states() {
            let mut internal = make_internal(5, &["node1", "node2"]);
            let node1: NodeId = Arc::from("node1");
            let node2: NodeId = Arc::from("node2");

            internal.add_transaction(make_tx(6, None)).unwrap();

            // Both nodes can submit_start independently
            let tx1 = internal.submit_start(&node1).unwrap();
            assert_eq!(tx1.sequence, 6);

            let tx2 = internal.submit_start(&node2).unwrap();
            assert_eq!(tx2.sequence, 6);

            // Both can succeed
            internal.submit_success(&node1, 100, 6).unwrap();
            internal.submit_success(&node2, 101, 6).unwrap();
        }

        #[test]
        fn reset_affects_single_node() {
            let mut internal = make_internal(5, &["node1", "node2"]);
            let node1: NodeId = Arc::from("node1");
            let node2: NodeId = Arc::from("node2");

            internal.add_transaction(make_tx(6, None)).unwrap();
            internal.add_transaction(make_tx(7, None)).unwrap();

            internal.reset_submitted(&node1, 6);
            // node2 still has last_submitted=None

            let tx1 = internal.submit_start(&node1).unwrap();
            assert_eq!(tx1.sequence, 7); // After seq 6

            let tx2 = internal.submit_start(&node2).unwrap();
            assert_eq!(tx2.sequence, 6); // From beginning
        }
    }

    // =========================================================================
    // Integration
    // =========================================================================

    mod integration {
        use super::*;

        #[test]
        fn full_submit_confirm_cycle() {
            let mut internal = make_internal(5, &["node1"]);
            let node: NodeId = Arc::from("node1");

            for seq in 6..=8 {
                internal
                    .add_transaction(make_tx(seq, Some(seq * 100)))
                    .unwrap();
            }

            // Initialize last_submitted for contiguous tracking
            internal.reset_submitted(&node, 5);

            // Submit all
            for seq in 6..=8 {
                let tx = internal.submit_start(&node).unwrap();
                assert_eq!(tx.sequence, seq);
                internal.submit_success(&node, seq * 100, seq).unwrap();
            }

            // Confirm all
            for seq in 6..=8 {
                internal.confirm(seq).unwrap();
            }

            assert!(internal.is_empty());
            assert_eq!(internal.confirmed_seq(), 8);
        }

        #[test]
        fn truncate_then_readd() {
            let mut internal = make_internal(5, &["node1"]);

            for seq in 6..=8 {
                internal.add_transaction(make_tx(seq, None)).unwrap();
            }

            internal.truncate_right_from(7).unwrap();
            assert_eq!(internal.max_seq(), 6);

            // Can re-add from seq 7
            internal.add_transaction(make_tx(7, None)).unwrap();
            assert_eq!(internal.max_seq(), 7);
        }

        #[test]
        fn confirm_then_add_more() {
            let mut internal = make_internal(5, &["node1"]);

            internal.add_transaction(make_tx(6, None)).unwrap();
            internal.confirm(6).unwrap();

            internal.add_transaction(make_tx(7, None)).unwrap();
            assert_eq!(internal.max_seq(), 7);
            assert_eq!(internal.confirmed_seq(), 6);
        }
    }
}
