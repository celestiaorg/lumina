use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
};

use crate::{
    tx_client_impl::TxConfirmInfo,
    tx_client_v2::{NodeId, Transaction, TxIdT},
};

pub type NodeCursor = u64;

pub struct TxInternal<TxId: TxIdT + Eq + Hash, ConfirmInfo> {
    confirmed: u64,
    transactions: VecDeque<Transaction<TxId, ConfirmInfo>>,
    nodes: HashMap<NodeId, NodeCursor>,
    id_to_seq: HashMap<TxId, u64>,
}

#[derive(Debug)]
pub enum TxInternalError {
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

    pub fn truncate_right_from(&mut self, seq: u64) -> Option<Vec<Transaction<TxId, ConfirmInfo>>> {
        if let Some(idx) = self.tx_idx(seq) {
            let mut tx_ret = Vec::new();
            for _ in 0..(self.transactions.len() - idx) {
                let tx = self.transactions
                    .pop_back()
                    .unwrap();
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

    pub fn reset(&mut self, node: &NodeId, seq: u64) {
        let max_seq = self.max_seq();
        let state = self.nodes.get_mut(node).unwrap();
        *state = seq.max(self.confirmed).min(max_seq);
    }

    fn force_update_all_nodes(&mut self, seq: u64) {
        for state in self.nodes.values_mut() {
            *state = (*state).max(seq);
        }
    }
}
