use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
};

use crate::tx_client_v2::{Transaction, TxIdT};

pub struct TxBuffer<TxId: TxIdT + Eq + Hash, ConfirmInfo> {
    confirmed: u64,
    transactions: VecDeque<Transaction<TxId, ConfirmInfo>>,
    id_to_seq: HashMap<TxId, u64>,
}

#[derive(Debug)]
pub enum TxBufferError {
    TransactionNotFound,
    ConfirmWithGaps,
    #[allow(dead_code)]
    AdvancePastMaxSeq,
    InvalidSequence,
}

type TxBufferResult<T> = Result<T, TxBufferError>;

impl<TxId: TxIdT + Eq + Hash, ConfirmInfo> TxBuffer<TxId, ConfirmInfo> {
    pub fn new(confirmed: u64) -> Self {
        TxBuffer {
            confirmed,
            transactions: VecDeque::new(),
            id_to_seq: HashMap::new(),
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

    pub fn add_transaction(&mut self, tx: Transaction<TxId, ConfirmInfo>) -> TxBufferResult<()> {
        if self.confirmed == 0 && self.transactions.is_empty() {
            self.confirmed = tx.sequence - 1;
        }
        if tx.sequence != self.max_seq() + 1 {
            return Err(TxBufferError::InvalidSequence);
        }
        self.transactions.push_back(tx);
        Ok(())
    }

    fn tx_idx(&self, sequence: u64) -> Option<usize> {
        let idx = sequence.checked_sub(self.confirmed + 1)? as usize;
        (idx < self.transactions.len()).then_some(idx)
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

    pub fn get(&self, seq: u64) -> Option<&Transaction<TxId, ConfirmInfo>> {
        self.tx_idx(seq).and_then(|idx| self.transactions.get(idx))
    }

    pub fn get_mut(&mut self, seq: u64) -> Option<&mut Transaction<TxId, ConfirmInfo>> {
        self.tx_idx(seq)
            .and_then(|idx| self.transactions.get_mut(idx))
    }

    pub fn set_submitted_id(&mut self, seq: u64, id: TxId) -> bool {
        let Some(tx) = self.get_mut(seq) else {
            return false;
        };
        tx.id = Some(id.clone());
        self.id_to_seq.insert(id, seq);
        true
    }

    pub fn ids_up_to(&self, seq: u64, limit: usize) -> Option<Vec<TxId>> {
        let idx = self.tx_idx(seq)?;
        let take = limit.min(idx + 1);
        Some(
            self.transactions
                .iter()
                .take(take)
                .filter_map(|tx| tx.id.clone())
                .collect(),
        )
    }

    pub fn confirm(&mut self, seq: u64) -> TxBufferResult<Transaction<TxId, ConfirmInfo>> {
        if seq != self.confirmed + 1 {
            return Err(TxBufferError::ConfirmWithGaps);
        }
        let Some(first) = self.transactions.front() else {
            return Err(TxBufferError::TransactionNotFound);
        };
        if first.sequence != seq {
            return Err(TxBufferError::ConfirmWithGaps);
        }
        let tx = self
            .transactions
            .pop_front()
            .ok_or(TxBufferError::TransactionNotFound)?;
        if let Some(id) = tx.id.clone() {
            self.id_to_seq.remove(&id);
        }
        self.confirmed += 1;
        Ok(tx)
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
}

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

    fn make_buffer(confirmed: u64) -> TxBuffer<u64, ()> {
        TxBuffer::new(confirmed)
    }

    mod creation_and_basic_properties {
        use super::*;

        #[test]
        fn new_creates_empty_buffer() {
            let buffer = make_buffer(10);
            assert_eq!(buffer.confirmed_seq(), 10);
            assert_eq!(buffer.max_seq(), 10);
            assert_eq!(buffer.len(), 0);
            assert!(buffer.is_empty());
        }

        #[test]
        fn max_seq_equals_confirmed_plus_len() {
            let mut buffer = make_buffer(5);
            assert_eq!(buffer.max_seq(), 5);

            buffer.add_transaction(make_tx(6, None)).unwrap();
            assert_eq!(buffer.max_seq(), 6);

            buffer.add_transaction(make_tx(7, None)).unwrap();
            assert_eq!(buffer.max_seq(), 7);
        }
    }

    mod add_transaction {
        use super::*;

        #[test]
        fn auto_adjusts_confirmed_when_zero() {
            let mut buffer = make_buffer(0);
            buffer.add_transaction(make_tx(5, None)).unwrap();
            assert_eq!(buffer.confirmed_seq(), 4);
            assert_eq!(buffer.max_seq(), 5);
        }

        #[test]
        fn requires_contiguous_sequence() {
            let mut buffer = make_buffer(5);
            let result = buffer.add_transaction(make_tx(8, None));
            assert!(matches!(result, Err(TxBufferError::InvalidSequence)));
        }

        #[test]
        fn add_multiple_in_sequence() {
            let mut buffer = make_buffer(5);
            for seq in 6..=10 {
                buffer
                    .add_transaction(make_tx(seq, Some(seq * 100)))
                    .unwrap();
            }
            assert_eq!(buffer.len(), 5);
            assert_eq!(buffer.max_seq(), 10);
        }

        #[test]
        fn rejects_duplicate_sequence() {
            let mut buffer = make_buffer(5);
            buffer.add_transaction(make_tx(6, None)).unwrap();
            let result = buffer.add_transaction(make_tx(6, None));
            assert!(matches!(result, Err(TxBufferError::InvalidSequence)));
        }
    }

    mod submitted_and_ids {
        use super::*;

        #[test]
        fn set_submitted_id_updates_mapping() {
            let mut buffer = make_buffer(5);
            buffer.add_transaction(make_tx(6, None)).unwrap();

            assert!(buffer.get_seq(&100).is_none());
            assert!(buffer.set_submitted_id(6, 100));
            assert_eq!(buffer.get_seq(&100), Some(6));
        }

        #[test]
        fn ids_up_to_filters_none_ids() {
            let mut buffer = make_buffer(5);
            buffer.add_transaction(make_tx(6, Some(600))).unwrap();
            buffer.add_transaction(make_tx(7, None)).unwrap();
            buffer.add_transaction(make_tx(8, Some(800))).unwrap();

            let ids = buffer.ids_up_to(8, 10).unwrap();
            assert_eq!(ids, vec![600, 800]);
        }

        #[test]
        fn ids_up_to_respects_limit() {
            let mut buffer = make_buffer(5);
            for seq in 6..=10 {
                buffer
                    .add_transaction(make_tx(seq, Some(seq * 100)))
                    .unwrap();
            }

            let ids = buffer.ids_up_to(10, 2).unwrap();
            assert_eq!(ids, vec![600, 700]);
        }
    }

    mod confirm {
        use super::*;

        #[test]
        fn must_be_sequential() {
            let mut buffer = make_buffer(5);
            buffer.add_transaction(make_tx(6, None)).unwrap();
            buffer.add_transaction(make_tx(7, None)).unwrap();

            let result = buffer.confirm(7);
            assert!(matches!(result, Err(TxBufferError::ConfirmWithGaps)));
        }

        #[test]
        fn removes_tx_and_increments_confirmed() {
            let mut buffer = make_buffer(5);
            buffer.add_transaction(make_tx(6, Some(600))).unwrap();

            let tx = buffer.confirm(6).unwrap();
            assert_eq!(tx.sequence, 6);
            assert_eq!(buffer.confirmed_seq(), 6);
            assert!(buffer.is_empty());
        }

        #[test]
        fn removes_id_from_mapping() {
            let mut buffer = make_buffer(5);
            buffer.add_transaction(make_tx(6, Some(600))).unwrap();

            assert_eq!(buffer.get_seq(&600), None);
            buffer.set_submitted_id(6, 600);
            assert!(buffer.get_seq(&600).is_some());

            buffer.confirm(6).unwrap();
            assert!(buffer.get_seq(&600).is_none());
        }
    }

    mod truncate_right_from {
        use super::*;

        #[test]
        fn removes_from_seq_onwards() {
            let mut buffer = make_buffer(5);
            for seq in 6..=10 {
                buffer
                    .add_transaction(make_tx(seq, Some(seq * 100)))
                    .unwrap();
            }

            let removed = buffer.truncate_right_from(8).unwrap();
            assert_eq!(removed.len(), 3);
            assert_eq!(removed[0].sequence, 10);
            assert_eq!(removed[1].sequence, 9);
            assert_eq!(removed[2].sequence, 8);

            assert_eq!(buffer.max_seq(), 7);
        }

        #[test]
        fn removes_ids_from_mapping() {
            let mut buffer = make_buffer(5);
            for seq in 6..=8 {
                buffer
                    .add_transaction(make_tx(seq, Some(seq * 100)))
                    .unwrap();
            }

            buffer.set_submitted_id(6, 600);
            buffer.set_submitted_id(7, 700);
            buffer.set_submitted_id(8, 800);

            assert!(buffer.get_seq(&700).is_some());
            assert!(buffer.get_seq(&800).is_some());

            buffer.truncate_right_from(7).unwrap();

            assert!(buffer.get_seq(&600).is_some());
            assert!(buffer.get_seq(&700).is_none());
            assert!(buffer.get_seq(&800).is_none());
        }

        #[test]
        fn returns_none_for_invalid_seq() {
            let mut buffer = make_buffer(5);
            buffer.add_transaction(make_tx(6, None)).unwrap();

            assert!(buffer.truncate_right_from(5).is_none());
            assert!(buffer.truncate_right_from(10).is_none());
        }
    }

    mod get_methods {
        use super::*;

        #[test]
        fn get_by_sequence() {
            let mut buffer = make_buffer(5);
            buffer.add_transaction(make_tx(6, Some(600))).unwrap();
            buffer.add_transaction(make_tx(7, Some(700))).unwrap();

            assert_eq!(buffer.get(6).unwrap().id, Some(600));
            assert_eq!(buffer.get(7).unwrap().id, Some(700));
            assert!(buffer.get(5).is_none());
            assert!(buffer.get(8).is_none());
        }

        #[test]
        fn get_by_id_requires_mapping() {
            let mut buffer = make_buffer(5);
            buffer.add_transaction(make_tx(6, Some(600))).unwrap();

            assert!(buffer.get_by_id(&600).is_none());
            buffer.set_submitted_id(6, 600);
            assert_eq!(buffer.get_by_id(&600).unwrap().sequence, 6);
        }

        #[test]
        fn get_mut_modifies() {
            let mut buffer = make_buffer(5);
            buffer.add_transaction(make_tx(6, Some(600))).unwrap();

            buffer.get_mut(6).unwrap().id = Some(999);
            assert_eq!(buffer.get(6).unwrap().id, Some(999));
        }
    }
}
