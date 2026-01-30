use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
};

use crate::{
    tx_client_impl::TxConfirmInfo,
    tx_client_v2::{NodeId, Transaction, TxIdT},
};

type NodeCursor = u64;

struct TxInternal<TxId: TxIdT + Eq + Hash, ConfirmInfo> {
    confirmed: u64,
    transactions: VecDeque<Transaction<TxId, ConfirmInfo>>,
    nodes: HashMap<NodeId, NodeCursor>,
    id_to_seq: HashMap<TxId, u64>,
}

enum TxInternalError {
    TransactionNotFound,
    ConfirmWithGaps,
    AdvancePastMaxSeq,
    InvalidSequence,
}

type TxInternalResult<T> = Result<T, TxInternalError>;

impl<TxId: TxIdT + Eq + Hash, ConfirmInfo> TxInternal<TxId, ConfirmInfo> {
    pub fn new(confirmed: u64, nodes: Vec<NodeId>) -> Self {
        let mut nodes_map = HashMap::new();
        for node in nodes {
            nodes_map.insert(node, 0);
        }
        TxInternal {
            confirmed,
            transactions: VecDeque::new(),
            nodes: nodes_map,
            id_to_seq: HashMap::new(),
        }
    }

    fn max_seq(&self) -> u64 {
        self.confirmed + self.transactions.len() as u64
    }

    pub fn add_transaction(&mut self, tx: Transaction<TxId, ConfirmInfo>) -> TxInternalResult<()> {
        if tx.sequence != self.max_seq() + 1 {
            return Err(TxInternalError::InvalidSequence);
        }
        self.transactions.push_back(tx);
        Ok(())
    }

    pub fn advance(&mut self, node: &NodeId, id: TxId) -> TxInternalResult<()> {
        let mx_seq = self.max_seq();
        let state = self.nodes.get_mut(node).unwrap();
        if *state + 1 > mx_seq {
            return Err(TxInternalError::AdvancePastMaxSeq);
        }
        *state += 1;
        self.id_to_seq.insert(id, *state);
        Ok(())
    }

    fn tx_idx(&self, sequence: u64) -> Option<usize> {
        let idx = sequence.checked_sub(self.confirmed + 1)? as usize;
        (idx < self.transactions.len()).then_some(idx)
    }

    pub fn next(&self, node: &NodeId) -> Option<&Transaction<TxId, ConfirmInfo>> {
        let state = *self.nodes.get(node)?;
        if state == 0 {
            return self.transactions.front();
        }
        let idx = self.tx_idx(state + 1)?;
        self.transactions.get(idx)
    }

    pub fn confirm(&mut self, seq: u64) -> TxInternalResult<Transaction<TxId, ConfirmInfo>> {
        if seq != self.confirmed + 1 {
            return Err(TxInternalError::ConfirmWithGaps);
        }
        let tx = self
            .transactions
            .pop_front()
            .ok_or(TxInternalError::TransactionNotFound)?;
        if tx.sequence != seq {
            return Err(TxInternalError::ConfirmWithGaps);
        }
        if let Some(id) = tx.id.clone() {
            self.id_to_seq.remove(&id);
        }
        self.confirmed += 1;
        self.force_update_all_nodes(seq);
        Ok(tx)
    }

    pub fn to_confirm(&mut self, node: &NodeId, limit: usize) -> Option<Vec<TxId>> {
        self.tx_idx(*self.nodes.get(node).unwrap()).map(|idx| {
            self.transactions
                .iter()
                .take(limit.min(idx + 1))
                .filter(|tx| tx.id.is_some())
                .map(|tx| tx.id.clone().unwrap())
                .collect::<Vec<_>>()
        })
    }

    pub fn truncate_right_from(&mut self, seq: u64) {
        if let Some(idx) = self.tx_idx(seq) {
            for _ in 0..(self.transactions.len() - idx) {
                self.transactions
                    .pop_back()
                    .unwrap()
                    .id
                    .and_then(|id| self.id_to_seq.remove(&id));
            }
        }
    }

    pub fn reset(&mut self, node: &NodeId, seq: u64) -> TxInternalResult<()> {
        let idx = self
            .tx_idx(seq)
            .ok_or(TxInternalError::TransactionNotFound)?;
        let state = self.nodes.get_mut(node).unwrap();
        *state = seq;
        Ok(())
    }

    fn force_update_all_nodes(&mut self, seq: u64) {
        for state in self.nodes.values_mut() {
            *state = (*state).max(seq);
        }
    }
}
