use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use crate::tx_client_v2::tx_buffer::TxBuffer;
use crate::tx_client_v2::{
    NodeEvent, NodeId, NodeResponse, RejectionReason, SubmitFailure, TxIdT, TxServer, TxStatus,
    WakeUpKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeMode {
    Active,
    Stopped,
}

#[derive(Debug)]
pub(crate) enum WorkerEvent<TxId: TxIdT, ConfirmInfo> {
    EnqueueTx {
        tx: crate::tx_client_v2::Transaction<TxId, ConfirmInfo>,
    },
    SpawnSubmit {
        node_id: NodeId,
        bytes: Arc<Vec<u8>>,
        sequence: u64,
        delay: Duration,
    },
    SpawnConfirmBatch {
        node_id: NodeId,
        ids: Vec<TxId>,
        epoch: u64,
    },
    SpawnRecover {
        node_id: NodeId,
        id: TxId,
        epoch: u64,
    },
    MarkSubmitted {
        sequence: u64,
        id: TxId,
    },
    Confirm {
        seq: u64,
        info: ConfirmInfo,
    },
    RecordError {
        seq: u64,
        status: TxStatus<ConfirmInfo>,
        node_id: NodeId,
    },
    WorkerStop,
}

pub(crate) struct NodeOutcome<TxId: TxIdT, ConfirmInfo> {
    pub(crate) worker_events: Vec<WorkerEvent<TxId, ConfirmInfo>>,
    pub(crate) wake_up: Option<WakeUpKind>,
}

#[derive(Debug, Clone)]
struct NodeState {
    submit_inflight: bool,
    confirm_inflight: bool,
    submit_delay: Option<Duration>,
    epoch: u64,
    recover_sequence: Option<u64>,
    submit_up_to: u64,
    mode: NodeMode,
    last_submitted: Option<u64>,
    pending: Option<u64>,
    last_confirmed: u64,
}

impl Default for NodeState {
    fn default() -> Self {
        Self {
            submit_inflight: false,
            confirm_inflight: false,
            submit_delay: None,
            epoch: 0,
            recover_sequence: None,
            submit_up_to: u64::MAX,
            mode: NodeMode::Active,
            last_submitted: None,
            pending: None,
            last_confirmed: 0,
        }
    }
}

impl NodeState {
    fn new(start_confirmed: u64) -> Self {
        Self {
            last_confirmed: start_confirmed,
            submit_up_to: u64::MAX,
            mode: NodeMode::Active,
            ..Self::default()
        }
    }

    fn recovering(&self) -> bool {
        self.recover_sequence.is_some()
    }

    fn bump_epoch(&mut self) {
        self.epoch = self.epoch.saturating_add(1);
    }

    fn current_submitted(&self) -> Option<u64> {
        match (self.last_submitted, self.pending) {
            (Some(last), Some(pending)) => Some(last.max(pending)),
            (Some(last), None) => Some(last),
            (None, Some(pending)) => Some(pending),
            (None, None) => None,
        }
    }

    fn bump_submitted_to_confirmed(&mut self, confirmed: u64) {
        match self.last_submitted {
            Some(last) if last < confirmed => self.last_submitted = Some(confirmed),
            None if confirmed > 0 => self.last_submitted = Some(confirmed),
            _ => {}
        }
    }

    fn clear_recovering_if_confirmed(&mut self, confirmed: u64) -> bool {
        let Some(target) = self.recover_sequence else {
            return false;
        };
        if confirmed >= target {
            self.recover_sequence = None;
            self.bump_epoch();
            true
        } else {
            false
        }
    }

    fn on_submit_success(&mut self, sequence: u64) {
        self.submit_inflight = false;
        self.pending = None;
        match self.last_submitted {
            None => self.last_submitted = Some(sequence),
            Some(last) => {
                if last + 1 == sequence {
                    self.last_submitted = Some(sequence);
                }
            }
        }
    }

    fn on_submit_failure(&mut self) {
        self.submit_inflight = false;
        self.pending = None;
        if self.mode == NodeMode::Stopped {
            if let Some(submitted) = self.current_submitted() {
                self.submit_up_to = self.submit_up_to.min(submitted);
            } else {
                self.submit_up_to = self.submit_up_to.min(self.last_confirmed);
            }
        }
    }

    fn stop_submissions(&mut self, confirmed: u64) {
        self.bump_submitted_to_confirmed(confirmed);
        self.submit_inflight = false;
        self.pending = None;
        if let Some(submitted) = self.last_submitted {
            self.submit_up_to = submitted;
        } else {
            self.submit_up_to = confirmed;
        }
        self.mode = NodeMode::Stopped;
    }

    fn record_fatal<TxId: TxIdT, ConfirmInfo>(
        &mut self,
        node_id: &NodeId,
        seq: u64,
        status: TxStatus<ConfirmInfo>,
        effects: &mut Vec<WorkerEvent<TxId, ConfirmInfo>>,
    ) {
        self.submit_up_to = self.submit_up_to.min(seq);
        self.mode = NodeMode::Stopped;
        self.submit_inflight = false;
        self.confirm_inflight = false;
        self.pending = None;
        self.recover_sequence = None;
        effects.push(WorkerEvent::RecordError {
            seq,
            status,
            node_id: node_id.clone(),
        });
    }
}

pub(crate) struct NodeManager<S: TxServer> {
    nodes: HashMap<NodeId, NodeState>,
    _marker: std::marker::PhantomData<S>,
}

impl<S: TxServer> NodeManager<S> {
    pub(crate) fn new(node_ids: impl IntoIterator<Item = NodeId>, start_confirmed: u64) -> Self {
        let nodes = node_ids
            .into_iter()
            .map(|id| (id, NodeState::new(start_confirmed)))
            .collect();
        Self {
            nodes,
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn should_stop(&self, confirmed: u64) -> bool {
        self.nodes
            .values()
            .all(|node| node.submit_up_to <= confirmed)
    }

    pub(crate) fn handle_event(
        &mut self,
        event: NodeEvent<S::TxId, S::ConfirmInfo>,
        txs: &TxBuffer<S::TxId, S::ConfirmInfo>,
        confirm_interval: Duration,
        max_status_batch: usize,
    ) -> NodeOutcome<S::TxId, S::ConfirmInfo> {
        let mut worker_events = Vec::new();
        let mut wake_up = None;

        match event {
            NodeEvent::WakeUp(kind) => {
                for (node_id, node) in self.nodes.iter_mut() {
                    node.clear_recovering_if_confirmed(txs.confirmed_seq());
                    match kind {
                        WakeUpKind::Submission => {
                            if let Some(event) = plan_submission(node_id, node, txs) {
                                worker_events.push(event);
                            }
                        }
                        WakeUpKind::Confirmation => {
                            if let Some(event) =
                                plan_confirmation(node_id, node, txs, max_status_batch)
                            {
                                worker_events.push(event);
                            }
                        }
                    }
                }
            }
            NodeEvent::NodeResponse { node_id, response } => {
                let Some(node) = self.nodes.get_mut(&node_id) else {
                    return NodeOutcome {
                        worker_events,
                        wake_up,
                    };
                };
                match response {
                    NodeResponse::Submission { sequence, result } => {
                        match result {
                            Ok(id) => {
                                node.on_submit_success(sequence);
                                worker_events.push(WorkerEvent::MarkSubmitted { sequence, id });
                            }
                            Err(failure) => {
                                node.on_submit_failure();
                                match failure {
                                    SubmitFailure::SequenceMismatch { expected } => {
                                        let recovering =
                                            on_sequence_mismatch(node, sequence, expected, txs);
                                        if recovering {
                                            wake_up = Some(WakeUpKind::Confirmation);
                                        }
                                    }
                                    SubmitFailure::MempoolIsFull
                                    | SubmitFailure::NetworkError { .. } => {
                                        node.submit_delay = Some(confirm_interval);
                                    }
                                    other => {
                                        node.submit_up_to = node.submit_up_to.min(sequence);
                                        node.mode = NodeMode::Stopped;
                                        let _ = other;
                                    }
                                }
                            }
                        }
                        if wake_up.is_none() {
                            wake_up = Some(WakeUpKind::Submission);
                        }
                    }
                    NodeResponse::Confirmation {
                        state_epoch,
                        response,
                    } => {
                        if state_epoch != node.epoch {
                            return NodeOutcome {
                                worker_events,
                                wake_up,
                            };
                        }
                        node.confirm_inflight = false;
                        match response {
                            Ok(confirmation) => match confirmation {
                                crate::tx_client_v2::ConfirmationResponse::Batch { statuses } => {
                                    let mut effects = process_status_batch(
                                        &node_id,
                                        node,
                                        statuses,
                                        txs,
                                        &mut wake_up,
                                    );
                                    worker_events.append(&mut effects);
                                }
                                crate::tx_client_v2::ConfirmationResponse::Single {
                                    id,
                                    status,
                                } => {
                                    let mut effects =
                                        process_recover_status(&node_id, node, id, status, txs);
                                    worker_events.append(&mut effects);
                                    if node.recover_sequence.is_none() {
                                        wake_up = Some(WakeUpKind::Submission);
                                    }
                                }
                            },
                            Err(_err) => {
                                // TODO: add error handling
                            }
                        }
                    }
                }
            }
            NodeEvent::NodeStop => {
                let confirmed = txs.confirmed_seq();
                for node in self.nodes.values_mut() {
                    node.stop_submissions(confirmed);
                }
            }
        }

        if self.should_stop(txs.confirmed_seq()) {
            worker_events.push(WorkerEvent::WorkerStop);
        }

        NodeOutcome {
            worker_events,
            wake_up,
        }
    }
}

fn plan_submission<TxId: TxIdT + Eq + Hash, ConfirmInfo>(
    node_id: &NodeId,
    node: &mut NodeState,
    txs: &TxBuffer<TxId, ConfirmInfo>,
) -> Option<WorkerEvent<TxId, ConfirmInfo>> {
    if node.mode == NodeMode::Stopped {
        return None;
    }
    if node.submit_inflight || node.recovering() || node.pending.is_some() {
        return None;
    }
    node.bump_submitted_to_confirmed(txs.confirmed_seq());
    let next_seq = match node.last_submitted {
        Some(last) => last + 1,
        None => txs.confirmed_seq() + 1,
    };
    if next_seq > node.submit_up_to {
        return None;
    }
    let tx = txs.get(next_seq)?;
    let delay = node
        .submit_delay
        .take()
        .unwrap_or_else(|| Duration::from_nanos(0));
    node.submit_inflight = true;
    node.pending = Some(next_seq);
    Some(WorkerEvent::SpawnSubmit {
        node_id: node_id.clone(),
        bytes: tx.bytes.clone(),
        sequence: tx.sequence,
        delay,
    })
}

fn plan_confirmation<TxId: TxIdT + Eq + Hash, ConfirmInfo>(
    node_id: &NodeId,
    node: &mut NodeState,
    txs: &TxBuffer<TxId, ConfirmInfo>,
    max_batch: usize,
) -> Option<WorkerEvent<TxId, ConfirmInfo>> {
    if node.confirm_inflight {
        return None;
    }
    node.bump_submitted_to_confirmed(txs.confirmed_seq());
    if node.recovering() {
        let target = node.recover_sequence?;
        let tx = txs.get(target)?;
        let id = tx.id.clone()?;
        let epoch = node.epoch;
        node.confirm_inflight = true;
        return Some(WorkerEvent::SpawnRecover {
            node_id: node_id.clone(),
            id,
            epoch,
        });
    }
    let last = node.last_submitted?;
    let ids = txs.ids_up_to(last, max_batch)?;
    if ids.is_empty() {
        return None;
    }
    let epoch = node.epoch;
    node.confirm_inflight = true;
    Some(WorkerEvent::SpawnConfirmBatch {
        node_id: node_id.clone(),
        ids,
        epoch,
    })
}

fn on_sequence_mismatch<TxId: TxIdT + Eq + Hash, ConfirmInfo>(
    node: &mut NodeState,
    sequence: u64,
    expected: u64,
    txs: &TxBuffer<TxId, ConfirmInfo>,
) -> bool {
    if txs.confirmed_seq() >= expected {
        return false;
    }
    if txs.max_seq() < expected.saturating_sub(1) {
        node.submit_up_to = node.submit_up_to.min(sequence);
        node.mode = NodeMode::Stopped;
        return false;
    }
    if sequence < expected {
        node.recover_sequence = Some(expected - 1);
        node.bump_epoch();
        return true;
    }
    let clamped = expected.saturating_sub(1).max(txs.confirmed_seq());
    node.last_submitted = Some(clamped);
    false
}

fn process_status_batch<TxId: TxIdT + Eq + Hash, ConfirmInfo>(
    node_id: &NodeId,
    node: &mut NodeState,
    statuses: Vec<(TxId, TxStatus<ConfirmInfo>)>,
    txs: &TxBuffer<TxId, ConfirmInfo>,
    wake_up: &mut Option<WakeUpKind>,
) -> Vec<WorkerEvent<TxId, ConfirmInfo>> {
    let mut effects: Vec<WorkerEvent<TxId, ConfirmInfo>> = Vec::new();
    let mut collected = statuses
        .into_iter()
        .filter_map(|(tx_id, status)| txs.get_by_id(&tx_id).map(|tx| (tx.sequence, status)))
        .collect::<Vec<_>>();
    collected.sort_by(|first, second| first.0.cmp(&second.0));
    if collected.is_empty() {
        return effects;
    }
    let offset = node.last_confirmed + 1;
    for (idx, (sequence, status)) in collected.into_iter().enumerate() {
        if idx as u64 + offset != sequence {
            break;
        }
        match status {
            TxStatus::Pending => {
                break;
            }
            TxStatus::Evicted => {
                effects.push(WorkerEvent::RecordError {
                    seq: sequence,
                    status: TxStatus::Evicted,
                    node_id: node_id.clone(),
                });
                if node.mode == NodeMode::Active {
                    let clamped = sequence.saturating_sub(1).max(txs.confirmed_seq());
                    node.last_submitted = Some(clamped);
                    if wake_up.is_none() {
                        *wake_up = Some(WakeUpKind::Submission);
                    }
                }
                break;
            }
            TxStatus::Rejected { reason } => match reason {
                RejectionReason::SequenceMismatch { expected, .. } => {
                    let recovering = on_sequence_mismatch(node, sequence, expected, txs);
                    if recovering && wake_up.is_none() {
                        *wake_up = Some(WakeUpKind::Confirmation);
                    }
                    break;
                }
                other => {
                    let status = TxStatus::Rejected { reason: other };
                    node.record_fatal(node_id, sequence, status, &mut effects);
                    break;
                }
            },
            TxStatus::Confirmed { info } => {
                node.last_confirmed = sequence;
                effects.push(WorkerEvent::Confirm {
                    seq: sequence,
                    info,
                });
            }
            TxStatus::Unknown => {
                node.record_fatal(node_id, sequence, TxStatus::Unknown, &mut effects);
                break;
            }
        }
    }
    effects
}

fn process_recover_status<TxId: TxIdT + Eq + Hash, ConfirmInfo>(
    node_id: &NodeId,
    node: &mut NodeState,
    id: TxId,
    status: TxStatus<ConfirmInfo>,
    txs: &TxBuffer<TxId, ConfirmInfo>,
) -> Vec<WorkerEvent<TxId, ConfirmInfo>> {
    let mut effects: Vec<WorkerEvent<TxId, ConfirmInfo>> = Vec::new();
    let Some(seq) = txs.get_seq(&id) else {
        return effects;
    };
    match status {
        TxStatus::Confirmed { info } => {
            node.last_confirmed = node.last_confirmed.max(seq);
            node.last_submitted = Some(node.last_submitted.unwrap_or(seq).max(seq));
            node.recover_sequence = None;
            node.bump_epoch();
            effects.push(WorkerEvent::Confirm { seq, info });
        }
        other => {
            node.record_fatal(node_id, seq, other, &mut effects);
        }
    }
    effects
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx_client_v2::{ConfirmationResponse, Transaction, TxCallbacks};
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

    fn make_tx(sequence: u64, id: Option<u64>) -> Transaction<u64, u64> {
        Transaction {
            sequence,
            bytes: Arc::new(vec![sequence as u8]),
            callbacks: TxCallbacks::default(),
            id,
        }
    }

    fn make_buffer_with_ids() -> TxBuffer<u64, u64> {
        let mut txs = TxBuffer::new(0);
        txs.add_transaction(make_tx(1, None)).unwrap();
        txs.add_transaction(make_tx(2, None)).unwrap();
        txs.set_submitted_id(1, 10);
        txs.set_submitted_id(2, 11);
        txs
    }

    #[test]
    fn wakeup_submission_spawns_submit() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], 0);
        let mut txs = TxBuffer::new(0);
        txs.add_transaction(make_tx(1, None)).unwrap();

        let outcome = manager.handle_event(
            NodeEvent::WakeUp(WakeUpKind::Submission),
            &txs,
            Duration::from_secs(1),
            16,
        );

        assert!(outcome.wake_up.is_none());
        assert!(outcome.worker_events.iter().any(|event| {
            matches!(
                event,
                WorkerEvent::SpawnSubmit { node_id: id, sequence, .. }
                    if id == &node_id && *sequence == 1
            )
        }));
    }

    #[test]
    fn submission_success_marks_and_wakes() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], 0);
        let txs = TxBuffer::new(0);

        let outcome = manager.handle_event(
            NodeEvent::NodeResponse {
                node_id: node_id.clone(),
                response: NodeResponse::Submission {
                    sequence: 1,
                    result: Ok(10),
                },
            },
            &txs,
            Duration::from_secs(1),
            16,
        );

        assert_eq!(outcome.wake_up, Some(WakeUpKind::Submission));
        assert!(outcome.worker_events.iter().any(|event| {
            matches!(
                event,
                WorkerEvent::MarkSubmitted { sequence, id } if *sequence == 1 && *id == 10
            )
        }));
    }

    #[test]
    fn confirmation_batch_skips_gap() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], 0);
        let txs = make_buffer_with_ids();

        let statuses = vec![
            (10, TxStatus::Pending),
            (11, TxStatus::Confirmed { info: 2 }),
        ];
        let outcome = manager.handle_event(
            NodeEvent::NodeResponse {
                node_id,
                response: NodeResponse::Confirmation {
                    state_epoch: 0,
                    response: Ok(ConfirmationResponse::Batch { statuses }),
                },
            },
            &txs,
            Duration::from_secs(1),
            16,
        );

        assert!(
            !outcome
                .worker_events
                .iter()
                .any(|event| matches!(event, WorkerEvent::Confirm { .. }))
        );
    }

    #[test]
    fn confirmation_batch_confirms_contiguous() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], 0);
        let txs = make_buffer_with_ids();

        let statuses = vec![
            (10, TxStatus::Confirmed { info: 1 }),
            (11, TxStatus::Confirmed { info: 2 }),
        ];
        let outcome = manager.handle_event(
            NodeEvent::NodeResponse {
                node_id,
                response: NodeResponse::Confirmation {
                    state_epoch: 0,
                    response: Ok(ConfirmationResponse::Batch { statuses }),
                },
            },
            &txs,
            Duration::from_secs(1),
            16,
        );

        let mut confirmed = Vec::new();
        for event in outcome.worker_events {
            if let WorkerEvent::Confirm { seq, info } = event {
                confirmed.push((seq, info));
            }
        }
        assert_eq!(confirmed, vec![(1, 1), (2, 2)]);
    }

    #[test]
    fn sequence_mismatch_triggers_recover_spawn() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], 0);
        let mut txs = TxBuffer::new(0);
        txs.add_transaction(make_tx(1, None)).unwrap();
        txs.set_submitted_id(1, 10);

        let outcome = manager.handle_event(
            NodeEvent::NodeResponse {
                node_id: node_id.clone(),
                response: NodeResponse::Submission {
                    sequence: 1,
                    result: Err(SubmitFailure::SequenceMismatch { expected: 2 }),
                },
            },
            &txs,
            Duration::from_secs(1),
            16,
        );

        assert_eq!(outcome.wake_up, Some(WakeUpKind::Confirmation));

        let wake = manager.handle_event(
            NodeEvent::WakeUp(WakeUpKind::Confirmation),
            &txs,
            Duration::from_secs(1),
            16,
        );

        assert!(wake.worker_events.iter().any(|event| {
            matches!(
                event,
                WorkerEvent::SpawnRecover { node_id: id, id: tx_id, .. }
                    if id == &node_id && *tx_id == 10
            )
        }));
    }

    #[test]
    fn node_stop_emits_worker_stop() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id], 0);
        let txs = TxBuffer::new(0);

        let outcome = manager.handle_event(NodeEvent::NodeStop, &txs, Duration::from_secs(1), 16);

        assert!(
            outcome
                .worker_events
                .iter()
                .any(|event| matches!(event, WorkerEvent::WorkerStop))
        );
    }
}
