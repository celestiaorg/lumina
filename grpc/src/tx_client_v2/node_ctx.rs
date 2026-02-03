use std::sync::Arc;
use std::time::Duration;

use crate::tx_client_v2::tx_internal::TxInternal;
use crate::tx_client_v2::{
    NodeId, RejectionReason, StopReason, SubmitFailure, TransactionEvent, TxServer, TxStatus,
};

#[derive(Debug, Default, Clone)]
pub(crate) struct NodeSubmissionState {
    pub(crate) submit_inflight: bool,
    pub(crate) confirm_inflight: bool,
    pub(crate) submit_delay: Option<Duration>,
    pub(crate) epoch: u64,
    pub(crate) recover_sequence: Option<u64>,
}

impl NodeSubmissionState {
    pub(crate) fn set_submitting(&mut self) {
        self.recover_sequence = None;
    }

    pub(crate) fn set_recovering(&mut self, recover_sequence: Option<u64>) {
        self.recover_sequence = recover_sequence;
    }

    pub(crate) fn recovering(&self) -> bool {
        self.recover_sequence.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeMode {
    Active,
    Stopped,
}

#[derive(Debug)]
pub(crate) enum NodePlan<TxId> {
    Submit {
        node_id: NodeId,
        bytes: Arc<Vec<u8>>,
        sequence: u64,
        delay: Duration,
    },
    ConfirmBatch {
        node_id: NodeId,
        ids: Vec<TxId>,
        epoch: u64,
    },
    Recover {
        node_id: NodeId,
        id: TxId,
        epoch: u64,
    },
}

#[derive(Debug)]
pub(crate) enum NodeEffect<ConfirmInfo> {
    NotifySubmit,
    FillConfirmSlot {
        seq: u64,
        info: ConfirmInfo,
    },
    RecordError {
        seq: u64,
        status: TxStatus<ConfirmInfo>,
        node_id: NodeId,
    },
    Stop(StopReason),
}

pub(crate) struct NodeCtx<S: TxServer> {
    pub(crate) id: NodeId,
    pub(crate) server: Arc<S>,
    pub(crate) state: NodeSubmissionState,
    pub(crate) last_confirmed: u64,
    pub(crate) submit_up_to: u64,
    pub(crate) mode: NodeMode,
}

impl<S: TxServer> NodeCtx<S> {
    pub(crate) fn new(id: NodeId, server: Arc<S>, start_confirmed: u64) -> Self {
        Self {
            id,
            server,
            state: NodeSubmissionState::default(),
            last_confirmed: start_confirmed,
            submit_up_to: u64::MAX,
            mode: NodeMode::Active,
        }
    }

    pub(crate) fn maybe_clear_recovering(
        &mut self,
        txs: &TxInternal<S::TxId, S::ConfirmInfo>,
    ) -> bool {
        let Some(target) = self.state.recover_sequence else {
            return false;
        };
        if txs.confirmed_seq() >= target {
            self.state.set_submitting();
            return true;
        }
        false
    }

    pub(crate) fn plan_submission(
        &mut self,
        txs: &mut TxInternal<S::TxId, S::ConfirmInfo>,
    ) -> Option<NodePlan<S::TxId>> {
        if self.mode == NodeMode::Stopped {
            return None;
        }
        if self.state.submit_inflight || self.state.recovering() {
            return None;
        }
        let Some(tx) = txs.peek_submit(&self.id) else {
            return None;
        };
        if tx.sequence > self.submit_up_to {
            return None;
        }
        let Some(tx) = txs.submit_start(&self.id) else {
            return None;
        };
        let delay = self
            .state
            .submit_delay
            .take()
            .unwrap_or_else(|| Duration::from_nanos(0));
        self.state.submit_inflight = true;
        Some(NodePlan::Submit {
            node_id: self.id.clone(),
            bytes: tx.bytes.clone(),
            sequence: tx.sequence,
            delay,
        })
    }

    pub(crate) fn plan_confirmation(
        &mut self,
        txs: &mut TxInternal<S::TxId, S::ConfirmInfo>,
        max_batch: usize,
    ) -> Option<NodePlan<S::TxId>> {
        if self.state.confirm_inflight {
            return None;
        }
        if self.state.recovering() {
            let target = self.state.recover_sequence?;
            let tx = txs.get(target)?;
            let id = tx.id.clone()?;
            let epoch = self.state.epoch;
            self.state.confirm_inflight = true;
            return Some(NodePlan::Recover {
                node_id: self.id.clone(),
                id,
                epoch,
            });
        }
        let ids = txs.to_confirm(&self.id, max_batch)?;
        if ids.is_empty() {
            return None;
        }
        let epoch = self.state.epoch;
        self.state.confirm_inflight = true;
        Some(NodePlan::ConfirmBatch {
            node_id: self.id.clone(),
            ids,
            epoch,
        })
    }

    pub(crate) fn handle_event(
        &mut self,
        event: TransactionEvent<S::TxId, S::ConfirmInfo>,
        txs: &mut TxInternal<S::TxId, S::ConfirmInfo>,
        confirm_interval: Duration,
    ) -> Vec<NodeEffect<S::ConfirmInfo>> {
        let mut effects = Vec::new();
        match event {
            TransactionEvent::SubmitFailed {
                sequence, failure, ..
            } => {
                let _ = txs.submit_failure(&self.id, sequence);
                self.state.submit_inflight = false;
                match failure {
                    SubmitFailure::SequenceMismatch { expected } => {
                        effects.extend(self.on_sequence_mismatch(sequence, expected, txs));
                    }
                    SubmitFailure::MempoolIsFull | SubmitFailure::NetworkError { .. } => {
                        self.state.submit_delay = Some(confirm_interval);
                    }
                    other => {
                        effects.push(NodeEffect::Stop(other.into()));
                    }
                }
                effects.push(NodeEffect::NotifySubmit);
            }
            TransactionEvent::Submitted { sequence, id, .. } => {
                if let Some(submitted) = txs.get_mut(sequence) {
                    submitted.notify_submitted(Ok(id.clone()));
                }
                let _ = txs.submit_success(&self.id, id, sequence);
                self.state.submit_inflight = false;
                effects.push(NodeEffect::NotifySubmit);
            }
            TransactionEvent::SubmitStatusBatch { statuses, .. }
            | TransactionEvent::StopStatusBatch { statuses, .. } => {
                self.state.confirm_inflight = false;
                effects.extend(self.process_status_batch(statuses, txs));
            }
            TransactionEvent::RecoverStatus { id, status, .. } => {
                self.state.confirm_inflight = false;
                effects.extend(self.process_recover_status(id, status, txs));
            }
        }
        effects
    }

    fn on_sequence_mismatch(
        &mut self,
        sequence: u64,
        expected: u64,
        txs: &mut TxInternal<S::TxId, S::ConfirmInfo>,
    ) -> Vec<NodeEffect<S::ConfirmInfo>> {
        let mut effects = Vec::new();
        if txs.confirmed_seq() >= expected {
            return effects;
        }
        if txs.max_seq() < expected.saturating_sub(1) {
            effects.push(NodeEffect::Stop(
                SubmitFailure::SequenceMismatch { expected }.into(),
            ));
            return effects;
        }
        if sequence < expected {
            self.state.set_recovering(Some(expected - 1));
        } else {
            txs.reset_submitted(&self.id, expected - 1);
        }
        effects
    }

    fn process_status_batch(
        &mut self,
        statuses: Vec<(S::TxId, TxStatus<S::ConfirmInfo>)>,
        txs: &mut TxInternal<S::TxId, S::ConfirmInfo>,
    ) -> Vec<NodeEffect<S::ConfirmInfo>> {
        let mut effects = Vec::new();
        let mut collected = statuses
            .into_iter()
            .filter_map(|(tx_id, status)| txs.get_by_id(&tx_id).map(|tx| (tx.sequence, status)))
            .collect::<Vec<_>>();
        collected.sort_by(|first, second| first.0.cmp(&second.0));
        if collected.is_empty() {
            return effects;
        }
        let offset = self.last_confirmed + 1;
        for (idx, (sequence, status)) in collected.into_iter().enumerate() {
            if idx as u64 + offset != sequence {
                break;
            }
            match status {
                TxStatus::Pending => {
                    continue;
                }
                TxStatus::Evicted => {
                    effects.push(NodeEffect::RecordError {
                        seq: sequence,
                        status: TxStatus::Evicted,
                        node_id: self.id.clone(),
                    });
                    if self.mode == NodeMode::Active {
                        txs.reset_submitted(&self.id, sequence - 1);
                        effects.push(NodeEffect::NotifySubmit);
                    }
                    break;
                }
                TxStatus::Rejected { reason } => match reason {
                    RejectionReason::SequenceMismatch { expected, .. } => {
                        effects.extend(self.on_sequence_mismatch(sequence, expected, txs));
                        break;
                    }
                    other => {
                        let status = TxStatus::Rejected { reason: other };
                        effects.extend(self.record_fatal(sequence, status));
                        break;
                    }
                },
                TxStatus::Confirmed { info } => {
                    self.last_confirmed = sequence;
                    effects.push(NodeEffect::FillConfirmSlot {
                        seq: sequence,
                        info,
                    });
                }
                TxStatus::Unknown => {
                    effects.extend(self.record_fatal(sequence, TxStatus::Unknown));
                    break;
                }
            }
        }
        effects
    }

    fn process_recover_status(
        &mut self,
        id: S::TxId,
        status: TxStatus<S::ConfirmInfo>,
        txs: &mut TxInternal<S::TxId, S::ConfirmInfo>,
    ) -> Vec<NodeEffect<S::ConfirmInfo>> {
        let mut effects = Vec::new();
        let Some(seq) = txs.get_seq(&id) else {
            return effects;
        };
        match status {
            TxStatus::Confirmed { info } => {
                self.last_confirmed = self.last_confirmed.max(seq);
                txs.reset_submitted(&self.id, seq);
                self.state.set_submitting();
                effects.push(NodeEffect::FillConfirmSlot { seq, info });
                effects.push(NodeEffect::NotifySubmit);
            }
            other => {
                effects.extend(self.record_fatal(seq, other));
            }
        }
        effects
    }

    fn record_fatal(
        &mut self,
        seq: u64,
        status: TxStatus<S::ConfirmInfo>,
    ) -> Vec<NodeEffect<S::ConfirmInfo>> {
        self.submit_up_to = self.submit_up_to.min(seq);
        self.mode = NodeMode::Stopped;
        self.state.set_submitting();
        vec![NodeEffect::RecordError {
            seq,
            status,
            node_id: self.id.clone(),
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx_client_v2::{Transaction, TxCallbacks};
    use async_trait::async_trait;

    #[derive(Debug)]
    struct DummyServer;

    #[async_trait]
    impl TxServer for DummyServer {
        type TxId = u64;
        type ConfirmInfo = u64;

        async fn submit(
            &self,
            _tx_bytes: Arc<Vec<u8>>,
            _sequence: u64,
        ) -> crate::tx_client_v2::TxSubmitResult<Self::TxId> {
            unimplemented!()
        }

        async fn status_batch(
            &self,
            _ids: Vec<Self::TxId>,
        ) -> crate::tx_client_v2::TxConfirmResult<Vec<(Self::TxId, TxStatus<Self::ConfirmInfo>)>>
        {
            unimplemented!()
        }

        async fn status(
            &self,
            _id: Self::TxId,
        ) -> crate::tx_client_v2::TxConfirmResult<TxStatus<Self::ConfirmInfo>> {
            unimplemented!()
        }

        async fn current_sequence(&self) -> crate::tx_client_v2::Result<u64> {
            unimplemented!()
        }
    }

    fn make_tx(sequence: u64) -> Transaction<u64, u64> {
        Transaction {
            sequence,
            bytes: Arc::new(vec![sequence as u8]),
            callbacks: TxCallbacks::default(),
            id: None,
        }
    }

    fn make_internal(node_id: &NodeId) -> TxInternal<u64, u64> {
        TxInternal::new(0, vec![node_id.clone()])
    }

    fn make_node(node_id: &NodeId) -> NodeCtx<DummyServer> {
        NodeCtx::new(node_id.clone(), Arc::new(DummyServer), 0)
    }

    #[test]
    fn plan_submission_skips_when_stopped() {
        let node_id: NodeId = Arc::from("node-1");
        let mut txs = make_internal(&node_id);
        txs.add_transaction(make_tx(1)).unwrap();
        let mut node = make_node(&node_id);
        node.mode = NodeMode::Stopped;
        let plan = node.plan_submission(&mut txs);
        assert!(plan.is_none());
    }

    #[test]
    fn confirmed_fills_confirm_slot_once() {
        let node_id: NodeId = Arc::from("node-1");
        let mut txs = make_internal(&node_id);
        txs.add_transaction(make_tx(1)).unwrap();
        let mut node = make_node(&node_id);

        let _ = txs.submit_start(&node_id).unwrap();
        txs.submit_success(&node_id, 10, 1).unwrap();

        let statuses = vec![(10, TxStatus::Confirmed { info: 7 })];
        let event = TransactionEvent::SubmitStatusBatch {
            node_id: node_id.clone(),
            statuses,
        };

        let effects = node.handle_event(event, &mut txs, Duration::from_secs(1));
        let mut filled = 0;
        for effect in effects {
            if let NodeEffect::FillConfirmSlot { seq, info } = effect {
                if txs.try_fill_confirm_slot(seq, info) {
                    filled += 1;
                }
            }
        }
        assert_eq!(filled, 1);

        let second = txs.try_fill_confirm_slot(1, 9);
        assert!(!second);
    }

    #[test]
    fn rejected_clamps_and_records_error() {
        let node_id: NodeId = Arc::from("node-1");
        let mut txs = make_internal(&node_id);
        txs.add_transaction(make_tx(1)).unwrap();
        let mut node = make_node(&node_id);

        let _ = txs.submit_start(&node_id).unwrap();
        txs.submit_success(&node_id, 10, 1).unwrap();

        let statuses = vec![(
            10,
            TxStatus::Rejected {
                reason: RejectionReason::OtherReason {
                    error_code: celestia_types::state::ErrorCode::UnknownRequest,
                    message: "bad".to_string(),
                    node_id: node_id.clone(),
                },
            },
        )];
        let event = TransactionEvent::SubmitStatusBatch {
            node_id: node_id.clone(),
            statuses,
        };

        let effects = node.handle_event(event, &mut txs, Duration::from_secs(1));
        assert_eq!(node.mode, NodeMode::Stopped);
        assert_eq!(node.submit_up_to, 1);
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, NodeEffect::RecordError { seq: 1, .. }))
        );
    }

    #[test]
    fn unknown_clamps_and_records_error() {
        let node_id: NodeId = Arc::from("node-1");
        let mut txs = make_internal(&node_id);
        txs.add_transaction(make_tx(1)).unwrap();
        let mut node = make_node(&node_id);

        let _ = txs.submit_start(&node_id).unwrap();
        txs.submit_success(&node_id, 10, 1).unwrap();

        let statuses = vec![(10, TxStatus::Unknown)];
        let event = TransactionEvent::SubmitStatusBatch {
            node_id: node_id.clone(),
            statuses,
        };

        let effects = node.handle_event(event, &mut txs, Duration::from_secs(1));
        assert_eq!(node.mode, NodeMode::Stopped);
        assert_eq!(node.submit_up_to, 1);
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, NodeEffect::RecordError { seq: 1, .. }))
        );
    }

    #[test]
    fn recovery_confirmed_clears_recovering() {
        let node_id: NodeId = Arc::from("node-1");
        let mut txs = make_internal(&node_id);
        txs.add_transaction(make_tx(1)).unwrap();
        let mut node = make_node(&node_id);

        let _ = txs.submit_start(&node_id).unwrap();
        txs.submit_success(&node_id, 10, 1).unwrap();
        node.state.set_recovering(Some(1));

        let event = TransactionEvent::RecoverStatus {
            node_id: node_id.clone(),
            id: 10,
            status: TxStatus::Confirmed { info: 42 },
        };

        let effects = node.handle_event(event, &mut txs, Duration::from_secs(1));
        assert!(!node.state.recovering());
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, NodeEffect::NotifySubmit))
        );
    }
}
