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
    WorkerStop,
}

pub(crate) struct NodeOutcome<TxId: TxIdT, ConfirmInfo> {
    pub(crate) worker_events: Vec<WorkerEvent<TxId, ConfirmInfo>>,
    pub(crate) wake_up: Option<WakeUpKind>,
}

#[derive(Debug, Clone)]
enum StopError<ConfirmInfo> {
    ConfirmError(TxStatus<ConfirmInfo>),
    SubmitError(TxStatus<ConfirmInfo>),
    Other,
}

impl<ConfirmInfo: Clone> StopError<ConfirmInfo> {
    fn status(&self) -> Option<TxStatus<ConfirmInfo>> {
        match self {
            StopError::ConfirmError(status) | StopError::SubmitError(status) => {
                Some(status.clone())
            }
            StopError::Other => None,
        }
    }
}

#[derive(Debug, Clone)]
struct NodeShared {
    submit_delay: Option<Duration>,
    epoch: u64,
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

#[derive(Debug, Clone)]
struct StoppedState<ConfirmInfo> {
    shared: NodeShared,
    confirm_inflight: bool,
    submit_up_to: u64,
    error: StopError<ConfirmInfo>,
}

#[derive(Debug, Clone)]
enum NodeState<ConfirmInfo> {
    Active(ActiveState),
    Recovering(RecoveringState),
    Stopped(StoppedState<ConfirmInfo>),
}

impl Default for NodeShared {
    fn default() -> Self {
        Self {
            submit_delay: None,
            epoch: 0,
            last_submitted: None,
            last_confirmed: 0,
        }
    }
}

impl<ConfirmInfo: Clone> NodeState<ConfirmInfo> {
    fn new(start_confirmed: u64) -> Self {
        let shared = NodeShared {
            last_confirmed: start_confirmed,
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
        let submit_up_to = current.unwrap_or(shared.last_confirmed);
        *self = NodeState::Stopped(StoppedState {
            shared,
            confirm_inflight,
            submit_up_to,
            error: StopError::Other,
        });
    }

    fn stop_with_error(&mut self, seq: u64, error: StopError<ConfirmInfo>) {
        let shared = self.shared().clone();
        let confirm_inflight = self.confirm_inflight();
        let submit_up_to = seq;
        *self = NodeState::Stopped(StoppedState {
            shared,
            confirm_inflight,
            submit_up_to,
            error,
        });
    }

    fn apply_stop_error(&mut self, seq: u64, error: StopError<ConfirmInfo>) {
        match self {
            NodeState::Stopped(state) => {
                let prev_submit_up_to = state.submit_up_to;
                if seq < state.submit_up_to {
                    state.submit_up_to = seq;
                }
                if matches!(state.error, StopError::Other) || seq < prev_submit_up_to {
                    state.error = error;
                }
            }
            _ => self.stop_with_error(seq, error),
        }
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
}

pub(crate) struct NodeManager<S: TxServer> {
    nodes: HashMap<NodeId, NodeState<S::ConfirmInfo>>,
}

impl<S: TxServer> NodeManager<S> {
    pub(crate) fn new(node_ids: impl IntoIterator<Item = NodeId>, start_confirmed: u64) -> Self {
        let nodes = node_ids
            .into_iter()
            .map(|id| (id, NodeState::new(start_confirmed)))
            .collect();
        Self { nodes }
    }

    pub(crate) fn should_stop(&self) -> bool {
        self.nodes.values().all(|node| match node {
            NodeState::Stopped(state) => state.shared.last_confirmed >= state.submit_up_to,
            _ => false,
        })
    }

    pub(crate) fn tail_error_status(&self) -> Option<TxStatus<S::ConfirmInfo>> {
        let mut best: Option<(u64, TxStatus<S::ConfirmInfo>)> = None;
        for node in self.nodes.values() {
            if let NodeState::Stopped(state) = node {
                let Some(status) = state.error.status() else {
                    continue;
                };
                match &best {
                    None => best = Some((state.submit_up_to, status)),
                    Some((seq, _)) if state.submit_up_to > *seq => {
                        best = Some((state.submit_up_to, status))
                    }
                    _ => {}
                }
            }
        }
        best.map(|(_, status)| status)
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
                                        node.stop_with_error(
                                            sequence,
                                            StopError::SubmitError(status),
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
                            NodeState::Stopped(stopped) => {
                                stopped.confirm_inflight = false;
                            }
                        }
                        match response {
                            Ok(confirmation) => match confirmation {
                                crate::tx_client_v2::ConfirmationResponse::Batch { statuses } => {
                                    let mut effects =
                                        process_status_batch(node, statuses, txs, &mut wake_up);
                                    worker_events.append(&mut effects);
                                }
                                crate::tx_client_v2::ConfirmationResponse::Single {
                                    id,
                                    status,
                                } => {
                                    let was_recovering = matches!(node, NodeState::Recovering(_));
                                    let mut effects = process_recover_status(node, id, status, txs);
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
                    if !node.is_stopped() {
                        node.stop_submissions(confirmed);
                    }
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
    node: &mut NodeState<ConfirmInfo>,
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
    node: &mut NodeState<ConfirmInfo>,
    txs: &TxBuffer<TxId, ConfirmInfo>,
    max_batch: usize,
) -> Option<WorkerEvent<TxId, ConfirmInfo>> {
    match node {
        NodeState::Active(state) => {
            if state.confirm_inflight {
                return None;
            }
            let last = state.shared.last_submitted?;
            let ids = txs.ids_up_to(last, max_batch)?;
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
        NodeState::Stopped(state) => {
            if state.confirm_inflight {
                return None;
            }
            let last = state.shared.last_submitted?;
            let confirm_upto = last.min(state.submit_up_to);
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
    }
}

fn on_sequence_mismatch<TxId: TxIdT + Eq + Hash, ConfirmInfo: Clone>(
    node: &mut NodeState<ConfirmInfo>,
    sequence: u64,
    expected: u64,
    txs: &TxBuffer<TxId, ConfirmInfo>,
) -> bool {
    let mut recover_target = None;
    let mut stop_submit_up_to = None;
    match node {
        NodeState::Active(state) => {
            if txs.confirmed_seq() >= expected {
                return false;
            }
            if txs.max_seq() < expected.saturating_sub(1) {
                stop_submit_up_to = Some(sequence);
            } else if sequence < expected {
                state.shared.epoch = state.shared.epoch.saturating_add(1);
                recover_target = Some(expected - 1);
            } else {
                let clamped = expected.saturating_sub(1).max(txs.confirmed_seq());
                state.shared.last_submitted = Some(clamped);
                return false;
            }
        }
        NodeState::Stopped(state) => {
            if txs.confirmed_seq() >= expected {
                return false;
            }
            if txs.max_seq() < expected.saturating_sub(1) {
                stop_submit_up_to = Some(sequence);
            } else if sequence < expected {
                state.shared.epoch = state.shared.epoch.saturating_add(1);
                recover_target = Some(expected - 1);
            } else {
                let clamped = expected.saturating_sub(1).max(txs.confirmed_seq());
                state.shared.last_submitted = Some(clamped);
                return false;
            }
        }
        NodeState::Recovering(_) => {
            return false;
        }
    }
    if let Some(target) = recover_target {
        node.transition_to_recovering(target);
        return true;
    }
    if let Some(submit_up_to) = stop_submit_up_to {
        node.apply_stop_error(submit_up_to, StopError::Other);
    }
    false
}

fn process_status_batch<TxId: TxIdT + Eq + Hash, ConfirmInfo: Clone>(
    node: &mut NodeState<ConfirmInfo>,
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
        let shared = match node {
            NodeState::Recovering(_) => {
                return effects;
            }
            _ => node.shared_mut(),
        };
        let offset = shared.last_confirmed + 1;
        for (idx, (sequence, status)) in collected.into_iter().enumerate() {
            if idx as u64 + offset != sequence {
                break;
            }
            match status {
                TxStatus::Pending => {
                    break;
                }
                TxStatus::Evicted => {
                    let clamped = sequence.saturating_sub(1).max(txs.confirmed_seq());
                    shared.last_submitted = Some(clamped);
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
                    shared.last_confirmed = sequence;
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
        node.apply_stop_error(sequence, StopError::ConfirmError(status));
    }
    effects
}

fn process_recover_status<TxId: TxIdT + Eq + Hash, ConfirmInfo: Clone>(
    node: &mut NodeState<ConfirmInfo>,
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
        node.apply_stop_error(seq, StopError::ConfirmError(status));
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
