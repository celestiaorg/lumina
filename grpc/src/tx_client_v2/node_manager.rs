use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use crate::tx_client_v2::tx_buffer::TxBuffer;
use crate::tx_client_v2::{
    NodeEvent, NodeId, NodeResponse, RejectionReason, SubmitFailure, TxIdT, TxServer, TxStatus,
    WakeUpKind,
};

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
struct NodeShared {
    submit_delay: Option<Duration>,
    epoch: u64,
    submit_up_to: u64,
    last_submitted: Option<u64>,
    last_confirmed: u64,
}

#[derive(Debug, Clone)]
struct ActiveState {
    shared: NodeShared,
    pending: Option<u64>,
    confirm_inflight: bool,
}

#[derive(Debug, Clone)]
struct RecoveringState {
    shared: NodeShared,
    confirm_inflight: bool,
    target: u64,
}

impl RecoveringState {
    fn reset_limits_for_recover(&mut self) {
        self.shared.submit_up_to = u64::MAX;
    }
}

#[derive(Debug, Clone)]
struct StoppedState {
    shared: NodeShared,
    confirm_inflight: bool,
}

#[derive(Debug, Clone)]
enum NodeState {
    Active(ActiveState),
    Recovering(RecoveringState),
    Stopped(StoppedState),
}

impl Default for NodeShared {
    fn default() -> Self {
        Self {
            submit_delay: None,
            epoch: 0,
            submit_up_to: u64::MAX,
            last_submitted: None,
            last_confirmed: 0,
        }
    }
}

impl NodeState {
    fn new(start_confirmed: u64) -> Self {
        let shared = NodeShared {
            last_confirmed: start_confirmed,
            submit_up_to: u64::MAX,
            ..NodeShared::default()
        };
        NodeState::Active(ActiveState {
            shared,
            pending: None,
            confirm_inflight: false,
        })
    }

    fn is_stopped(&self) -> bool {
        matches!(self, NodeState::Stopped(_))
    }

    fn epoch(&self) -> u64 {
        self.shared().epoch
    }

    fn confirm_inflight(&self) -> bool {
        match self {
            NodeState::Active(state) => state.confirm_inflight,
            NodeState::Recovering(state) => state.confirm_inflight,
            NodeState::Stopped(state) => state.confirm_inflight,
        }
    }

    fn set_confirm_inflight(&mut self, inflight: bool) {
        match self {
            NodeState::Active(state) => state.confirm_inflight = inflight,
            NodeState::Recovering(state) => state.confirm_inflight = inflight,
            NodeState::Stopped(state) => state.confirm_inflight = inflight,
        }
    }

    fn shared(&self) -> &NodeShared {
        match self {
            NodeState::Active(state) => &state.shared,
            NodeState::Recovering(state) => &state.shared,
            NodeState::Stopped(state) => &state.shared,
        }
    }

    fn shared_mut(&mut self) -> &mut NodeShared {
        match self {
            NodeState::Active(state) => &mut state.shared,
            NodeState::Recovering(state) => &mut state.shared,
            NodeState::Stopped(state) => &mut state.shared,
        }
    }

    fn current_submitted(&self) -> Option<u64> {
        match self {
            NodeState::Active(state) => match (state.shared.last_submitted, state.pending) {
                (Some(last), Some(pending)) => Some(last.max(pending)),
                (Some(last), None) => Some(last),
                (None, Some(pending)) => Some(pending),
                (None, None) => None,
            },
            NodeState::Recovering(state) => state.shared.last_submitted,
            NodeState::Stopped(state) => state.shared.last_submitted,
        }
    }

    fn bump_submitted_to_confirmed(&mut self, confirmed: u64) {
        let shared = self.shared_mut();
        match shared.last_submitted {
            Some(last) if last < confirmed => shared.last_submitted = Some(confirmed),
            None if confirmed > 0 => shared.last_submitted = Some(confirmed),
            _ => {}
        }
    }

    fn stop_submissions(&mut self, confirmed: u64) {
        let mut shared = self.shared().clone();
        let current = self.current_submitted();
        let confirm_inflight = self.confirm_inflight();
        match shared.last_submitted {
            Some(last) if last < confirmed => shared.last_submitted = Some(confirmed),
            None if confirmed > 0 => shared.last_submitted = Some(confirmed),
            _ => {}
        }
        shared.submit_up_to = current.unwrap_or(shared.last_confirmed);
        *self = NodeState::Stopped(StoppedState {
            shared,
            confirm_inflight,
        });
    }

    fn record_fatal<TxId: TxIdT, ConfirmInfo: Clone>(
        &mut self,
        node_id: &NodeId,
        seq: u64,
        status: TxStatus<ConfirmInfo>,
        effects: &mut Vec<WorkerEvent<TxId, ConfirmInfo>>,
    ) {
        let mut shared = self.shared().clone();
        let confirm_inflight = self.confirm_inflight();
        shared.submit_up_to = shared.submit_up_to.min(seq);
        effects.push(WorkerEvent::RecordError {
            seq,
            status,
            node_id: node_id.clone(),
        });
        *self = NodeState::Stopped(StoppedState {
            shared,
            confirm_inflight,
        });
    }

    fn transition_to_recovering(&mut self, target: u64) {
        let shared = self.shared().clone();
        let confirm_inflight = self.confirm_inflight();
        *self = NodeState::Recovering(RecoveringState {
            shared,
            confirm_inflight,
            target,
        });
    }

    fn transition_to_active(&mut self) {
        let shared = self.shared().clone();
        *self = NodeState::Active(ActiveState {
            shared,
            pending: None,
            confirm_inflight: false,
        });
    }

    fn transition_to_stopped(&mut self) {
        let shared = self.shared().clone();
        let confirm_inflight = self.confirm_inflight();
        *self = NodeState::Stopped(StoppedState {
            shared,
            confirm_inflight,
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

    pub(crate) fn should_stop(&self) -> bool {
        self.nodes
            .values()
            .all(|node| matches!(node, NodeState::Stopped(_)))
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
                let node = self.nodes.get_mut(&node_id).unwrap();
                match response {
                    NodeResponse::Submission { sequence, result } => {
                        if node.is_stopped() {
                            return NodeOutcome {
                                worker_events,
                                wake_up,
                            };
                        }
                        match result {
                            Ok(id) => {
                                if let NodeState::Active(active) = node {
                                    on_submit_success(active, sequence);
                                    worker_events.push(WorkerEvent::MarkSubmitted { sequence, id });
                                }
                            }
                            Err(failure) => {
                                if let NodeState::Active(active) = node {
                                    on_submit_failure(active);
                                }
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
                                        node.shared_mut().submit_delay = Some(confirm_interval);
                                    }
                                    other => {
                                        let status = submit_failure_status(&node_id, &other);
                                        node.record_fatal(
                                            &node_id,
                                            sequence,
                                            status,
                                            &mut worker_events,
                                        );
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
                        if node.is_stopped() {
                            node.set_confirm_inflight(false);
                            return NodeOutcome {
                                worker_events,
                                wake_up,
                            };
                        }
                        if state_epoch != node.epoch() {
                            return NodeOutcome {
                                worker_events,
                                wake_up,
                            };
                        }
                        node.bump_submitted_to_confirmed(txs.confirmed_seq());
                        match node {
                            NodeState::Active(active) => active.confirm_inflight = false,
                            NodeState::Recovering(recovering) => {
                                recovering.confirm_inflight = false;
                            }
                            NodeState::Stopped(_) => {
                                return NodeOutcome {
                                    worker_events,
                                    wake_up,
                                };
                            }
                        }
                        match response {
                            Ok(confirmation) => match confirmation {
                                crate::tx_client_v2::ConfirmationResponse::Batch { statuses } => {
                                    if matches!(node, NodeState::Active(_)) {
                                        let mut effects = process_status_batch(
                                            &node_id,
                                            node,
                                            statuses,
                                            txs,
                                            &mut wake_up,
                                        );
                                        worker_events.append(&mut effects);
                                    }
                                }
                                crate::tx_client_v2::ConfirmationResponse::Single {
                                    id,
                                    status,
                                } => {
                                    let was_recovering = matches!(node, NodeState::Recovering(_));
                                    let mut effects =
                                        process_recover_status(&node_id, node, id, status, txs);
                                    worker_events.append(&mut effects);
                                    if was_recovering && matches!(node, NodeState::Active(_)) {
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

        if self.should_stop() {
            worker_events.push(WorkerEvent::WorkerStop);
        }

        NodeOutcome {
            worker_events,
            wake_up,
        }
    }
}

fn submit_failure_status<ConfirmInfo>(
    node_id: &NodeId,
    failure: &SubmitFailure,
) -> TxStatus<ConfirmInfo> {
    let (error_code, message) = match failure {
        SubmitFailure::InvalidTx { error_code } => {
            (*error_code, "submit failed: invalid tx".to_string())
        }
        SubmitFailure::InsufficientFunds => (
            celestia_types::state::ErrorCode::UnknownRequest,
            "submit failed: insufficient funds".to_string(),
        ),
        SubmitFailure::InsufficientFee { expected_fee } => (
            celestia_types::state::ErrorCode::UnknownRequest,
            format!(
                "submit failed: insufficient fee (expected {})",
                expected_fee
            ),
        ),
        SubmitFailure::SequenceMismatch { expected } => (
            celestia_types::state::ErrorCode::UnknownRequest,
            format!("submit failed: sequence mismatch (expected {})", expected),
        ),
        SubmitFailure::NetworkError { .. } => (
            celestia_types::state::ErrorCode::UnknownRequest,
            "submit failed: network error".to_string(),
        ),
        SubmitFailure::MempoolIsFull => (
            celestia_types::state::ErrorCode::UnknownRequest,
            "submit failed: mempool is full".to_string(),
        ),
    };
    TxStatus::Rejected {
        reason: RejectionReason::OtherReason {
            error_code,
            message,
            node_id: node_id.clone(),
        },
    }
}

fn on_submit_success(state: &mut ActiveState, sequence: u64) {
    state.pending = None;
    match state.shared.last_submitted {
        None => state.shared.last_submitted = Some(sequence),
        Some(last) => {
            if last + 1 == sequence {
                state.shared.last_submitted = Some(sequence);
            }
        }
    }
}

fn on_submit_failure(state: &mut ActiveState) {
    state.pending = None;
}

fn plan_submission<TxId: TxIdT + Eq + Hash, ConfirmInfo>(
    node_id: &NodeId,
    node: &mut NodeState,
    txs: &TxBuffer<TxId, ConfirmInfo>,
) -> Option<WorkerEvent<TxId, ConfirmInfo>> {
    match node {
        NodeState::Active(state) => {
            if state.pending.is_some() {
                return None;
            }
            let next_seq = match state.shared.last_submitted {
                Some(last) => last + 1,
                None => txs.confirmed_seq() + 1,
            };
            if next_seq > state.shared.submit_up_to {
                return None;
            }
            let tx = txs.get(next_seq)?;
            let delay = state
                .shared
                .submit_delay
                .take()
                .unwrap_or_else(|| Duration::from_nanos(0));
            state.pending = Some(next_seq);
            Some(WorkerEvent::SpawnSubmit {
                node_id: node_id.clone(),
                bytes: tx.bytes.clone(),
                sequence: tx.sequence,
                delay,
            })
        }
        NodeState::Recovering(_) | NodeState::Stopped(_) => None,
    }
}

fn plan_confirmation<TxId: TxIdT + Eq + Hash, ConfirmInfo>(
    node_id: &NodeId,
    node: &mut NodeState,
    txs: &TxBuffer<TxId, ConfirmInfo>,
    max_batch: usize,
) -> Option<WorkerEvent<TxId, ConfirmInfo>> {
    match node {
        NodeState::Active(state) => {
            if state.confirm_inflight {
                return None;
            }
            let last = state.shared.last_submitted?;
            let confirm_upto = last.min(state.shared.submit_up_to);
            let ids = txs.ids_up_to(confirm_upto, max_batch)?;
            if ids.is_empty() {
                return None;
            }
            let epoch = state.shared.epoch;
            state.confirm_inflight = true;
            Some(WorkerEvent::SpawnConfirmBatch {
                node_id: node_id.clone(),
                ids,
                epoch,
            })
        }
        NodeState::Recovering(state) => {
            if state.confirm_inflight {
                return None;
            }
            if state.target > state.shared.submit_up_to {
                state.reset_limits_for_recover();
            }
            let tx = txs.get(state.target)?;
            let id = tx.id.clone()?;
            let epoch = state.shared.epoch;
            state.confirm_inflight = true;
            Some(WorkerEvent::SpawnRecover {
                node_id: node_id.clone(),
                id,
                epoch,
            })
        }
        NodeState::Stopped(_) => None,
    }
}

fn on_sequence_mismatch<TxId: TxIdT + Eq + Hash, ConfirmInfo>(
    node: &mut NodeState,
    sequence: u64,
    expected: u64,
    txs: &TxBuffer<TxId, ConfirmInfo>,
) -> bool {
    let mut recover_target = None;
    let mut stop = false;
    {
        let NodeState::Active(state) = node else {
            return false;
        };
        if txs.confirmed_seq() >= expected {
            return false;
        }
        if txs.max_seq() < expected.saturating_sub(1) {
            state.shared.submit_up_to = state.shared.submit_up_to.min(sequence);
            stop = true;
        } else if sequence < expected {
            state.shared.epoch = state.shared.epoch.saturating_add(1);
            recover_target = Some(expected - 1);
        } else {
            let clamped = expected.saturating_sub(1).max(txs.confirmed_seq());
            state.shared.last_submitted = Some(clamped);
            return false;
        }
    }
    if let Some(target) = recover_target {
        node.transition_to_recovering(target);
        return true;
    }
    if stop {
        node.transition_to_stopped();
    }
    false
}

fn process_status_batch<TxId: TxIdT + Eq + Hash, ConfirmInfo: Clone>(
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
    let mut fatal: Option<(u64, TxStatus<ConfirmInfo>)> = None;
    let mut seq_mismatch: Option<(u64, u64)> = None;
    {
        let NodeState::Active(state) = node else {
            return effects;
        };
        let offset = state.shared.last_confirmed + 1;
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
                    let clamped = sequence.saturating_sub(1).max(txs.confirmed_seq());
                    state.shared.last_submitted = Some(clamped);
                    if wake_up.is_none() {
                        *wake_up = Some(WakeUpKind::Submission);
                    }
                    break;
                }
                TxStatus::Rejected { reason } => match reason {
                    RejectionReason::SequenceMismatch { expected, .. } => {
                        seq_mismatch = Some((sequence, expected));
                        break;
                    }
                    other => {
                        fatal = Some((sequence, TxStatus::Rejected { reason: other }));
                        break;
                    }
                },
                TxStatus::Confirmed { info } => {
                    state.shared.last_confirmed = sequence;
                    effects.push(WorkerEvent::Confirm {
                        seq: sequence,
                        info,
                    });
                }
                TxStatus::Unknown => {
                    fatal = Some((sequence, TxStatus::Unknown));
                    break;
                }
            }
        }
    }
    if let Some((sequence, expected)) = seq_mismatch {
        let recovering = on_sequence_mismatch(node, sequence, expected, txs);
        if recovering && wake_up.is_none() {
            *wake_up = Some(WakeUpKind::Confirmation);
        }
    }
    if let Some((sequence, status)) = fatal {
        node.record_fatal(node_id, sequence, status, &mut effects);
    }
    effects
}

fn process_recover_status<TxId: TxIdT + Eq + Hash, ConfirmInfo: Clone>(
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
    let mut confirmed_info: Option<ConfirmInfo> = None;
    let mut fatal: Option<TxStatus<ConfirmInfo>> = None;
    {
        let NodeState::Recovering(state) = node else {
            return effects;
        };
        match status {
            TxStatus::Confirmed { info } => {
                state.shared.last_confirmed = state.shared.last_confirmed.max(seq);
                state.shared.last_submitted =
                    Some(state.shared.last_submitted.unwrap_or(seq).max(seq));
                state.shared.epoch = state.shared.epoch.saturating_add(1);
                confirmed_info = Some(info);
            }
            other => {
                fatal = Some(other);
            }
        }
    }
    if let Some(info) = confirmed_info {
        node.transition_to_active();
        effects.push(WorkerEvent::Confirm { seq, info });
    }
    if let Some(status) = fatal {
        node.record_fatal(node_id, seq, status, &mut effects);
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
