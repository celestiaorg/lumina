use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use tracing::debug;

use crate::tx_client_v2::tx_buffer::TxBuffer;
use crate::tx_client_v2::{
    NodeEvent, NodeId, NodeResponse, PlanRequest, RejectionReason, RequestWithChannels,
    SigningFailure, SubmitFailure, TxIdT, TxServer, TxStatus,
};

#[derive(Debug)]
pub(crate) enum WorkerPlan<TxId: TxIdT> {
    SpawnSigning {
        node_id: NodeId,
        sequence: u64,
        delay: Duration,
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
}

impl<TxId: TxIdT> WorkerPlan<TxId> {
    pub(crate) fn summary(&self) -> String {
        match self {
            WorkerPlan::SpawnSigning {
                node_id, sequence, ..
            } => {
                format!("SpawnSigning node={} seq={}", node_id.as_ref(), sequence)
            }
            WorkerPlan::SpawnSubmit {
                node_id, sequence, ..
            } => format!("SpawnSubmit node={} seq={}", node_id.as_ref(), sequence),
            WorkerPlan::SpawnConfirmBatch { node_id, ids, .. } => {
                format!(
                    "SpawnConfirmBatch node={} count={}",
                    node_id.as_ref(),
                    ids.len()
                )
            }
            WorkerPlan::SpawnRecover { node_id, .. } => {
                format!("SpawnRecover node={}", node_id.as_ref())
            }
        }
    }
}

type PendingRequest<S> = RequestWithChannels<
    <S as TxServer>::TxId,
    <S as TxServer>::ConfirmInfo,
    <S as TxServer>::TxRequest,
>;
type NodeBuffer<S> =
    TxBuffer<<S as TxServer>::TxId, <S as TxServer>::ConfirmInfo, PendingRequest<S>>;
type WorkerPlanFor<S> = WorkerPlan<<S as TxServer>::TxId>;

#[derive(Debug)]
pub(crate) enum WorkerMutation<TxId: TxIdT, ConfirmInfo> {
    EnqueueSigned {
        tx: crate::tx_client_v2::Transaction<TxId, ConfirmInfo>,
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

impl<TxId: TxIdT, ConfirmInfo> WorkerMutation<TxId, ConfirmInfo> {
    pub(crate) fn summary(&self) -> String {
        match self {
            WorkerMutation::EnqueueSigned { tx } => {
                format!("EnqueueSigned seq={}", tx.sequence)
            }
            WorkerMutation::MarkSubmitted { sequence, .. } => {
                format!("MarkSubmitted seq={}", sequence)
            }
            WorkerMutation::Confirm { seq, .. } => format!("Confirm seq={}", seq),
            WorkerMutation::WorkerStop => "WorkerStop".to_string(),
        }
    }
}

pub(crate) struct NodeApplyOutcome<TxId: TxIdT, ConfirmInfo> {
    pub(crate) mutations: Vec<WorkerMutation<TxId, ConfirmInfo>>,
    pub(crate) plan_requests: Vec<PlanRequest>,
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

    fn label(&self) -> String {
        match self {
            StopError::ConfirmError(status) => {
                format!("ConfirmError({})", tx_status_label(status))
            }
            StopError::SubmitError(status) => format!("SubmitError({})", tx_status_label(status)),
            StopError::Other => "Other".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct NodeShared {
    submit_delay: Option<Duration>,
    epoch: u64,
    last_submitted: Option<u64>,
    last_confirmed: u64,
    confirm_inflight: bool,
    signing_inflight: bool,
}

#[derive(Debug, Clone)]
struct ActiveState {
    shared: NodeShared,
    pending: Option<u64>,
}

#[derive(Debug, Clone)]
struct RecoveringState {
    shared: NodeShared,
    target: u64,
}

#[derive(Debug, Clone)]
struct StoppedState<ConfirmInfo> {
    shared: NodeShared,
    stop_seq: u64,
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
            confirm_inflight: false,
            signing_inflight: false,
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
        })
    }

    fn is_stopped(&self) -> bool {
        matches!(self, NodeState::Stopped(_))
    }

    fn epoch(&self) -> u64 {
        self.shared().epoch
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

    fn log_state(&self) -> String {
        match self {
            NodeState::Active(state) => format!(
                "Active last_confirmed={} last_submitted={:?} pending={:?} confirm_inflight={} signing_inflight={} epoch={} delay={:?}",
                state.shared.last_confirmed,
                state.shared.last_submitted,
                state.pending,
                state.shared.confirm_inflight,
                state.shared.signing_inflight,
                state.shared.epoch,
                state.shared.submit_delay,
            ),
            NodeState::Recovering(state) => format!(
                "Recovering target={} last_confirmed={} last_submitted={:?} confirm_inflight={} signing_inflight={} epoch={} delay={:?}",
                state.target,
                state.shared.last_confirmed,
                state.shared.last_submitted,
                state.shared.confirm_inflight,
                state.shared.signing_inflight,
                state.shared.epoch,
                state.shared.submit_delay,
            ),
            NodeState::Stopped(state) => format!(
                "Stopped stop_seq={} error={} last_confirmed={} last_submitted={:?} confirm_inflight={} signing_inflight={} epoch={} delay={:?}",
                state.stop_seq,
                state.error.label(),
                state.shared.last_confirmed,
                state.shared.last_submitted,
                state.shared.confirm_inflight,
                state.shared.signing_inflight,
                state.shared.epoch,
                state.shared.submit_delay,
            ),
        }
    }

    fn stop_submissions(&mut self) {
        let shared = self.shared().clone();
        let last_submitted = shared.last_submitted;
        let stop_seq = last_submitted
            .unwrap_or(shared.last_confirmed)
            .max(shared.last_confirmed);
        *self = NodeState::Stopped(StoppedState {
            shared,
            stop_seq,
            error: StopError::Other,
        });
    }

    fn stop_with_error(&mut self, seq: u64, error: StopError<ConfirmInfo>) {
        let shared = self.shared().clone();
        let stop_seq = seq;
        *self = NodeState::Stopped(StoppedState {
            shared,
            stop_seq,
            error,
        });
    }

    fn apply_stop_error(&mut self, seq: u64, error: StopError<ConfirmInfo>) {
        match self {
            NodeState::Stopped(state) => {
                let prev_stop_seq = state.stop_seq;
                if seq < state.stop_seq {
                    state.stop_seq = seq;
                }
                if matches!(state.error, StopError::Other) || seq < prev_stop_seq {
                    state.error = error;
                }
            }
            _ => self.stop_with_error(seq, error),
        }
    }

    fn transition_to_recovering(&mut self, target: u64) {
        let shared = self.shared().clone();
        *self = NodeState::Recovering(RecoveringState { shared, target });
    }

    fn transition_to_active(&mut self) {
        let mut shared = self.shared().clone();
        shared.confirm_inflight = false;
        shared.signing_inflight = false;
        *self = NodeState::Active(ActiveState {
            shared,
            pending: None,
        });
    }
}

pub(crate) struct NodeManager<S: TxServer> {
    nodes: HashMap<NodeId, NodeState<S::ConfirmInfo>>,
    signing_delay: Option<Duration>,
}

impl<S: TxServer> NodeManager<S> {
    pub(crate) fn new(node_ids: impl IntoIterator<Item = NodeId>, start_confirmed: u64) -> Self {
        let nodes = node_ids
            .into_iter()
            .map(|id| (id, NodeState::new(start_confirmed)))
            .collect();
        Self {
            nodes,
            signing_delay: None,
        }
    }

    pub(crate) fn should_stop(&self, confirmed_seq: u64) -> bool {
        let mut any_active = false;
        for (node_id, node) in self.nodes.iter() {
            let node_should_stop = match node {
                NodeState::Stopped(state) => confirmed_seq >= state.stop_seq,
                _ => {
                    any_active = true;
                    false
                }
            };
            debug!(
                node_id = %node_id.as_ref(),
                state = %node.log_state(),
                confirmed_seq,
                node_should_stop,
                "should_stop check"
            );
        }
        if any_active {
            return false;
        }
        debug!("all nodes stopped; stopping worker");
        true
    }

    pub(crate) fn min_confirmed_non_stopped(&self) -> Option<u64> {
        let mut min_confirmed: Option<u64> = None;
        for node in self.nodes.values() {
            if node.is_stopped() {
                continue;
            }
            let confirmed = node.shared().last_confirmed;
            min_confirmed = Some(match min_confirmed {
                Some(current) => current.min(confirmed),
                None => confirmed,
            });
        }
        min_confirmed
    }

    pub(crate) fn tail_error_status(&self) -> Option<TxStatus<S::ConfirmInfo>> {
        let mut best: Option<(u64, TxStatus<S::ConfirmInfo>)> = None;
        for node in self.nodes.values() {
            if let NodeState::Stopped(state) = node {
                let Some(status) = state.error.status() else {
                    continue;
                };
                match &best {
                    None => best = Some((state.stop_seq, status)),
                    Some((seq, _)) if state.stop_seq > *seq => {
                        best = Some((state.stop_seq, status))
                    }
                    _ => {}
                }
            }
        }
        best.map(|(_, status)| status)
    }

    pub(crate) fn plan(
        &mut self,
        plan_requests: &[PlanRequest],
        txs: &NodeBuffer<S>,
        max_status_batch: usize,
    ) -> Vec<WorkerPlanFor<S>> {
        let mut plans = Vec::new();
        for request in plan_requests {
            match request {
                PlanRequest::Submission => {
                    for (node_id, node) in self.nodes.iter_mut() {
                        debug!(
                            node_id = %node_id.as_ref(),
                            state = %node.log_state(),
                            request = ?request,
                            "node state (plan)"
                        );
                        if let Some(plan) = plan_submission(node_id, node, txs) {
                            plans.push(plan);
                        }
                    }
                }
                PlanRequest::Confirmation => {
                    for (node_id, node) in self.nodes.iter_mut() {
                        debug!(
                            node_id = %node_id.as_ref(),
                            state = %node.log_state(),
                            request = ?request,
                            "node state (plan)"
                        );
                        if let Some(plan) = plan_confirmation(node_id, node, txs, max_status_batch)
                        {
                            plans.push(plan);
                        }
                    }
                }
                PlanRequest::Signing => {
                    if let Some(plan) = self.plan_signing(txs) {
                        plans.push(plan);
                    }
                }
            }
        }
        plans
    }

    pub(crate) fn apply_event(
        &mut self,
        event: NodeEvent<S::TxId, S::ConfirmInfo>,
        txs: &NodeBuffer<S>,
        confirm_interval: Duration,
    ) -> NodeApplyOutcome<S::TxId, S::ConfirmInfo> {
        let mut mutations = Vec::new();
        let mut plan_requests = Vec::new();

        debug!(event = %event.summary(), "node event");
        match event {
            NodeEvent::NodeResponse { node_id, response } => {
                let mut stop_all = false;
                {
                    let node = self.nodes.get_mut(&node_id).unwrap();
                    debug!(
                        node_id = %node_id.as_ref(),
                        state = %node.log_state(),
                        "node state (before response)"
                    );
                    match response {
                        NodeResponse::Submission { sequence, result } => {
                            if node.is_stopped() {
                                return NodeApplyOutcome {
                                    mutations,
                                    plan_requests,
                                };
                            }
                            match result {
                                Ok(id) => {
                                    if let NodeState::Active(active) = node {
                                        on_submit_success(active, sequence);
                                        mutations
                                            .push(WorkerMutation::MarkSubmitted { sequence, id });
                                        let last_submitted =
                                            active.shared.last_submitted.unwrap_or(0);
                                        let next_sequence = txs.next_sequence().saturating_sub(1);
                                        if last_submitted == next_sequence && txs.has_pending() {
                                            push_plan_request(
                                                &mut plan_requests,
                                                PlanRequest::Signing,
                                            );
                                        }
                                    }
                                }
                                Err(failure) => {
                                    if let NodeState::Active(active) = node {
                                        on_submit_failure(active);
                                    }
                                    match failure {
                                        SubmitFailure::SequenceMismatch { expected } => {
                                            let recovering = on_sequence_mismatch(
                                                node, &node_id, sequence, expected, txs,
                                            );
                                            if recovering {
                                                push_plan_request(
                                                    &mut plan_requests,
                                                    PlanRequest::Confirmation,
                                                );
                                            }
                                        }
                                        SubmitFailure::MempoolIsFull
                                        | SubmitFailure::NetworkError { .. } => {
                                            node.shared_mut().submit_delay = Some(confirm_interval);
                                        }
                                        SubmitFailure::Other {
                                            error_code,
                                            message,
                                        } => {
                                            let stop_at = node
                                                .shared()
                                                .last_submitted
                                                .unwrap_or(node.shared().last_confirmed);
                                            let status =
                                                submit_other_status(&node_id, error_code, message);
                                            node.stop_with_error(
                                                stop_at,
                                                StopError::SubmitError(status),
                                            );
                                        }
                                    }
                                }
                            }
                            push_plan_request(&mut plan_requests, PlanRequest::Submission);
                        }
                        NodeResponse::Confirmation {
                            state_epoch,
                            response,
                        } => {
                            if state_epoch != node.epoch() {
                                debug!(
                                    node_id = %node_id.as_ref(),
                                    expected_epoch = node.epoch(),
                                    response_epoch = state_epoch,
                                    "confirmation epoch mismatch"
                                );
                                node.shared_mut().confirm_inflight = false;
                                push_plan_request(&mut plan_requests, PlanRequest::Confirmation);
                                return NodeApplyOutcome {
                                    mutations,
                                    plan_requests,
                                };
                            }
                            node.shared_mut().confirm_inflight = false;
                            match response {
                                Ok(confirmation) => match confirmation {
                                    crate::tx_client_v2::ConfirmationResponse::Batch {
                                        statuses,
                                    } => {
                                        let mut effects = process_status_batch(
                                            &node_id,
                                            node,
                                            statuses,
                                            txs,
                                            &mut plan_requests,
                                        );
                                        mutations.append(&mut effects);
                                    }
                                    crate::tx_client_v2::ConfirmationResponse::Single {
                                        id,
                                        status,
                                    } => {
                                        let mut effects =
                                            process_recover_status(node, id, status, txs);
                                        mutations.append(&mut effects);
                                    }
                                },
                                Err(_err) => {
                                    // TODO: add error handling
                                }
                            }
                        }
                        NodeResponse::Signing { sequence, result } => {
                            node.shared_mut().signing_inflight = false;
                            match result {
                                Ok(tx) => {
                                    mutations.push(WorkerMutation::EnqueueSigned { tx });
                                    push_plan_request(&mut plan_requests, PlanRequest::Submission);
                                }
                                Err(SigningFailure::SequenceMismatch { expected }) => {
                                    if expected >= sequence {
                                        stop_all = true;
                                    } else {
                                        self.signing_delay = Some(confirm_interval);
                                        push_plan_request(&mut plan_requests, PlanRequest::Signing);
                                    }
                                }
                                Err(SigningFailure::NetworkError { .. }) => {
                                    self.signing_delay = Some(confirm_interval);
                                    push_plan_request(&mut plan_requests, PlanRequest::Signing);
                                }
                                Err(SigningFailure::Other { .. }) => {
                                    stop_all = true;
                                }
                            }
                        }
                    }
                    debug!(
                        node_id = %node_id.as_ref(),
                        state = %node.log_state(),
                        "node state (after response)"
                    );
                }
                if stop_all {
                    self.stop_all_nodes();
                }
            }
            NodeEvent::NodeStop => {
                for (node_id, node) in self.nodes.iter_mut() {
                    debug!(
                        node_id = %node_id.as_ref(),
                        state = %node.log_state(),
                        "node state (before stop)"
                    );
                    if !node.is_stopped() {
                        node.stop_submissions();
                    }
                    debug!(
                        node_id = %node_id.as_ref(),
                        state = %node.log_state(),
                        "node state (after stop)"
                    );
                }
            }
        }

        if self.should_stop(txs.confirmed_seq()) {
            mutations.push(WorkerMutation::WorkerStop);
        }

        NodeApplyOutcome {
            mutations,
            plan_requests,
        }
    }

    fn plan_signing(&mut self, txs: &NodeBuffer<S>) -> Option<WorkerPlanFor<S>> {
        if self
            .nodes
            .values()
            .any(|node| node.shared().signing_inflight)
        {
            return None;
        }
        if !txs.has_pending() {
            return None;
        }
        let (node_id, max_last_submitted) = self.max_active_last_submitted()?;
        let next_sequence = txs.next_sequence();
        if max_last_submitted != 0 && next_sequence != max_last_submitted + 1 {
            return None;
        }
        let delay = self
            .signing_delay
            .take()
            .unwrap_or_else(|| Duration::from_nanos(0));
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.shared_mut().signing_inflight = true;
        }
        Some(WorkerPlan::SpawnSigning {
            node_id,
            sequence: next_sequence,
            delay,
        })
    }

    fn max_active_last_submitted(&self) -> Option<(NodeId, u64)> {
        self.nodes
            .iter()
            .filter_map(|(node_id, node)| match node {
                NodeState::Active(state) => {
                    Some((node_id.clone(), state.shared.last_submitted.unwrap_or(0)))
                }
                _ => None,
            })
            .max_by_key(|(_, last_submitted)| *last_submitted)
    }

    fn stop_all_nodes(&mut self) {
        for node in self.nodes.values_mut() {
            if !node.is_stopped() {
                node.stop_submissions();
            }
        }
    }
}

fn push_plan_request(requests: &mut Vec<PlanRequest>, request: PlanRequest) {
    if !requests.contains(&request) {
        requests.push(request);
    }
}

fn tx_status_label<ConfirmInfo>(status: &TxStatus<ConfirmInfo>) -> &'static str {
    match status {
        TxStatus::Pending => "Pending",
        TxStatus::Confirmed { .. } => "Confirmed",
        TxStatus::Rejected { .. } => "Rejected",
        TxStatus::Evicted => "Evicted",
        TxStatus::Unknown => "Unknown",
    }
}

fn submit_other_status<ConfirmInfo>(
    node_id: &NodeId,
    error_code: celestia_types::state::ErrorCode,
    message: String,
) -> TxStatus<ConfirmInfo> {
    TxStatus::Rejected {
        reason: RejectionReason::OtherReason {
            error_code,
            message,
            node_id: node_id.clone(),
        },
    }
}

fn sequence_mismatch_status<ConfirmInfo>(node_id: &NodeId, expected: u64) -> TxStatus<ConfirmInfo> {
    TxStatus::Rejected {
        reason: RejectionReason::SequenceMismatch {
            expected,
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

fn plan_submission<TxId: TxIdT + Eq + Hash, ConfirmInfo, Pending>(
    node_id: &NodeId,
    node: &mut NodeState<ConfirmInfo>,
    txs: &TxBuffer<TxId, ConfirmInfo, Pending>,
) -> Option<WorkerPlan<TxId>> {
    match node {
        NodeState::Active(state) => {
            if state.pending.is_some() {
                return None;
            }
            let confirmed = txs.confirmed_seq();
            let next_seq = match state.shared.last_submitted {
                Some(last) => last.max(confirmed) + 1,
                None => confirmed + 1,
            };
            let tx = txs.get(next_seq)?;
            let delay = state
                .shared
                .submit_delay
                .take()
                .unwrap_or_else(|| Duration::from_nanos(0));
            state.pending = Some(next_seq);
            Some(WorkerPlan::SpawnSubmit {
                node_id: node_id.clone(),
                bytes: tx.bytes.clone(),
                sequence: tx.sequence,
                delay,
            })
        }
        NodeState::Recovering(_) | NodeState::Stopped(_) => None,
    }
}

fn plan_confirmation<TxId: TxIdT + Eq + Hash, ConfirmInfo, Pending>(
    node_id: &NodeId,
    node: &mut NodeState<ConfirmInfo>,
    txs: &TxBuffer<TxId, ConfirmInfo, Pending>,
    max_batch: usize,
) -> Option<WorkerPlan<TxId>> {
    match node {
        NodeState::Active(state) => {
            if state.shared.confirm_inflight {
                return None;
            }
            let last = state.shared.last_submitted?;
            let ids = txs.ids_up_to(last, max_batch)?;
            if ids.is_empty() {
                return None;
            }
            let epoch = state.shared.epoch;
            state.shared.confirm_inflight = true;
            Some(WorkerPlan::SpawnConfirmBatch {
                node_id: node_id.clone(),
                ids,
                epoch,
            })
        }
        NodeState::Recovering(state) => {
            if state.shared.confirm_inflight {
                return None;
            }
            let tx = txs.get(state.target)?;
            let id = tx.id.clone()?;
            let epoch = state.shared.epoch;
            state.shared.confirm_inflight = true;
            Some(WorkerPlan::SpawnRecover {
                node_id: node_id.clone(),
                id,
                epoch,
            })
        }
        NodeState::Stopped(state) => {
            if state.shared.confirm_inflight {
                return None;
            }
            let last = state.shared.last_submitted?;
            let confirm_upto = last.min(state.stop_seq);
            let ids = txs.ids_up_to(confirm_upto, max_batch)?;
            if ids.is_empty() {
                return None;
            }
            let epoch = state.shared.epoch;
            state.shared.confirm_inflight = true;
            Some(WorkerPlan::SpawnConfirmBatch {
                node_id: node_id.clone(),
                ids,
                epoch,
            })
        }
    }
}

fn on_sequence_mismatch<TxId: TxIdT + Eq + Hash, ConfirmInfo: Clone, Pending>(
    node: &mut NodeState<ConfirmInfo>,
    node_id: &NodeId,
    sequence: u64,
    expected: u64,
    txs: &TxBuffer<TxId, ConfirmInfo, Pending>,
) -> bool {
    let mut recover_target = None;
    let mut stop_seq = None;
    let shared = match node {
        NodeState::Active(state) => &mut state.shared,
        NodeState::Stopped(state) => &mut state.shared,
        NodeState::Recovering(_) => {
            return false;
        }
    };
    if txs.confirmed_seq() >= expected {
        return false;
    }
    if sequence < expected {
        let target = expected - 1;
        if txs.get(target).and_then(|tx| tx.id.clone()).is_none() {
            let status = sequence_mismatch_status(node_id, expected);
            let stop_at = shared.last_submitted.unwrap_or(shared.last_confirmed);
            node.apply_stop_error(stop_at, StopError::ConfirmError(status));
            return false;
        }
        shared.epoch = shared.epoch.saturating_add(1);
        recover_target = Some(target);
    } else if txs.max_seq() < expected.saturating_sub(1) {
        let has_id = txs.get(sequence).and_then(|tx| tx.id.clone()).is_some();
        let stop_at = if has_id {
            sequence
        } else {
            shared.last_submitted.unwrap_or(shared.last_confirmed)
        };
        stop_seq = Some(stop_at);
    } else {
        let clamped = expected.saturating_sub(1).max(txs.confirmed_seq());
        shared.last_submitted = Some(clamped);
        return false;
    }
    if let Some(target) = recover_target {
        node.transition_to_recovering(target);
        return true;
    }
    if let Some(stop_seq) = stop_seq {
        node.apply_stop_error(stop_seq, StopError::Other);
    }
    false
}

fn process_status_batch<TxId: TxIdT + Eq + Hash, ConfirmInfo: Clone, Pending>(
    node_id: &NodeId,
    node: &mut NodeState<ConfirmInfo>,
    statuses: Vec<(TxId, TxStatus<ConfirmInfo>)>,
    txs: &TxBuffer<TxId, ConfirmInfo, Pending>,
    plan_requests: &mut Vec<PlanRequest>,
) -> Vec<WorkerMutation<TxId, ConfirmInfo>> {
    let mut effects: Vec<WorkerMutation<TxId, ConfirmInfo>> = Vec::new();
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
                    push_plan_request(plan_requests, PlanRequest::Submission);
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
                    effects.push(WorkerMutation::Confirm {
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
        let recovering = on_sequence_mismatch(node, node_id, sequence, expected, txs);
        if recovering {
            push_plan_request(plan_requests, PlanRequest::Confirmation);
        }
    }
    if let Some((sequence, status)) = fatal {
        node.apply_stop_error(sequence, StopError::ConfirmError(status));
    }
    effects
}

fn process_recover_status<TxId: TxIdT + Eq + Hash, ConfirmInfo: Clone, Pending>(
    node: &mut NodeState<ConfirmInfo>,
    id: TxId,
    status: TxStatus<ConfirmInfo>,
    txs: &TxBuffer<TxId, ConfirmInfo, Pending>,
) -> Vec<WorkerMutation<TxId, ConfirmInfo>> {
    let mut effects: Vec<WorkerMutation<TxId, ConfirmInfo>> = Vec::new();
    let Some(seq) = txs.get_seq(&id) else {
        return effects;
    };
    let mut confirmed_info: Option<ConfirmInfo> = None;
    let mut fatal: Option<TxStatus<ConfirmInfo>> = None;
    let mut confirmed = false;
    {
        let NodeState::Recovering(state) = node else {
            return effects;
        };
        match status {
            TxStatus::Confirmed { info } => {
                state.shared.last_submitted =
                    Some(state.shared.last_submitted.unwrap_or(seq).max(seq));
                state.shared.epoch = state.shared.epoch.saturating_add(1);
                let expected = txs.confirmed_seq() + 1;
                if seq == expected {
                    state.shared.last_confirmed = seq;
                    confirmed_info = Some(info);
                }
                confirmed = true;
            }
            other => {
                fatal = Some(other);
            }
        }
    }
    if confirmed {
        node.transition_to_active();
        if let Some(info) = confirmed_info {
            effects.push(WorkerMutation::Confirm { seq, info });
        }
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
        type TxRequest = crate::tx_client_v2::TxRequest;

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

        async fn simulate_and_sign(
            &self,
            _req: Arc<Self::TxRequest>,
            _sequence: u64,
        ) -> crate::tx_client_v2::TxSigningResult<Self::TxId, Self::ConfirmInfo> {
            unimplemented!()
        }
    }

    type TestBuffer = TxBuffer<u64, u64, PendingRequest<DummyServer>>;

    fn make_tx(sequence: u64, id: Option<u64>) -> Transaction<u64, u64> {
        Transaction {
            sequence,
            bytes: Arc::new(vec![sequence as u8]),
            callbacks: TxCallbacks::default(),
            id,
        }
    }

    fn make_buffer_with_ids() -> TestBuffer {
        let mut txs = TestBuffer::new(0);
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
        let mut txs = TestBuffer::new(0);
        txs.add_transaction(make_tx(1, None)).unwrap();

        let plans = manager.plan(&[PlanRequest::Submission], &mut txs, 16);

        assert!(plans.iter().any(|event| {
            matches!(
                event,
                WorkerPlan::SpawnSubmit { node_id: id, sequence, .. }
                    if id == &node_id && *sequence == 1
            )
        }));
    }

    #[test]
    fn submission_success_marks_and_wakes() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], 0);
        let mut txs = TestBuffer::new(0);

        let outcome = manager.apply_event(
            NodeEvent::NodeResponse {
                node_id: node_id.clone(),
                response: NodeResponse::Submission {
                    sequence: 1,
                    result: Ok(10),
                },
            },
            &mut txs,
            Duration::from_secs(1),
        );

        assert!(outcome.plan_requests.contains(&PlanRequest::Submission));
        assert!(outcome.mutations.iter().any(|event| {
            matches!(
                event,
                WorkerMutation::MarkSubmitted { sequence, id } if *sequence == 1 && *id == 10
            )
        }));
    }

    #[test]
    fn confirmation_batch_skips_gap() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], 0);
        let mut txs = make_buffer_with_ids();

        let statuses = vec![
            (10, TxStatus::Pending),
            (11, TxStatus::Confirmed { info: 2 }),
        ];
        let outcome = manager.apply_event(
            NodeEvent::NodeResponse {
                node_id,
                response: NodeResponse::Confirmation {
                    state_epoch: 0,
                    response: Ok(ConfirmationResponse::Batch { statuses }),
                },
            },
            &mut txs,
            Duration::from_secs(1),
        );

        assert!(
            !outcome
                .mutations
                .iter()
                .any(|event| matches!(event, WorkerMutation::Confirm { .. }))
        );
    }

    #[test]
    fn confirmation_batch_confirms_contiguous() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], 0);
        let mut txs = make_buffer_with_ids();

        let statuses = vec![
            (10, TxStatus::Confirmed { info: 1 }),
            (11, TxStatus::Confirmed { info: 2 }),
        ];
        let outcome = manager.apply_event(
            NodeEvent::NodeResponse {
                node_id,
                response: NodeResponse::Confirmation {
                    state_epoch: 0,
                    response: Ok(ConfirmationResponse::Batch { statuses }),
                },
            },
            &mut txs,
            Duration::from_secs(1),
        );

        let mut confirmed = Vec::new();
        for event in outcome.mutations {
            if let WorkerMutation::Confirm { seq, info } = event {
                confirmed.push((seq, info));
            }
        }
        assert_eq!(confirmed, vec![(1, 1), (2, 2)]);
    }

    #[test]
    fn sequence_mismatch_triggers_recover_spawn() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], 0);
        let mut txs = TestBuffer::new(0);
        txs.add_transaction(make_tx(1, None)).unwrap();
        txs.set_submitted_id(1, 10);

        let outcome = manager.apply_event(
            NodeEvent::NodeResponse {
                node_id: node_id.clone(),
                response: NodeResponse::Submission {
                    sequence: 1,
                    result: Err(SubmitFailure::SequenceMismatch { expected: 2 }),
                },
            },
            &mut txs,
            Duration::from_secs(1),
        );

        assert!(outcome.plan_requests.contains(&PlanRequest::Confirmation));

        let wake = manager.plan(&outcome.plan_requests, &mut txs, 16);

        assert!(wake.iter().any(|event| {
            matches!(
                event,
                WorkerPlan::SpawnRecover { node_id: id, id: tx_id, .. }
                    if id == &node_id && *tx_id == 10
            )
        }));
    }

    #[test]
    fn sequence_mismatch_missing_id_stops_on_submit() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], 0);
        let mut txs = TestBuffer::new(0);
        txs.add_transaction(make_tx(1, None)).unwrap();

        let _ = manager.apply_event(
            NodeEvent::NodeResponse {
                node_id: node_id.clone(),
                response: NodeResponse::Submission {
                    sequence: 1,
                    result: Err(SubmitFailure::SequenceMismatch { expected: 2 }),
                },
            },
            &mut txs,
            Duration::from_secs(1),
        );

        assert!(matches!(
            manager.nodes.get(&node_id).unwrap(),
            NodeState::Stopped(_)
        ));
        let status = manager.tail_error_status().expect("missing error status");
        match status {
            TxStatus::Rejected {
                reason:
                    RejectionReason::SequenceMismatch {
                        expected,
                        node_id: id,
                    },
            } => {
                assert_eq!(expected, 2);
                assert_eq!(id, node_id);
            }
            other => panic!("unexpected status: {:?}", other),
        }
    }

    #[test]
    fn sequence_mismatch_missing_id_stops_on_confirm() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], 0);
        let mut txs = TestBuffer::new(0);
        txs.add_transaction(make_tx(1, None)).unwrap();
        txs.add_transaction(make_tx(2, None)).unwrap();
        txs.set_submitted_id(1, 10);

        let statuses = vec![(
            10,
            TxStatus::Rejected {
                reason: RejectionReason::SequenceMismatch {
                    expected: 3,
                    node_id: node_id.clone(),
                },
            },
        )];
        let _ = manager.apply_event(
            NodeEvent::NodeResponse {
                node_id: node_id.clone(),
                response: NodeResponse::Confirmation {
                    state_epoch: 0,
                    response: Ok(ConfirmationResponse::Batch { statuses }),
                },
            },
            &mut txs,
            Duration::from_secs(1),
        );

        assert!(matches!(
            manager.nodes.get(&node_id).unwrap(),
            NodeState::Stopped(_)
        ));
        let status = manager.tail_error_status().expect("missing error status");
        match status {
            TxStatus::Rejected {
                reason:
                    RejectionReason::SequenceMismatch {
                        expected,
                        node_id: id,
                    },
            } => {
                assert_eq!(expected, 3);
                assert_eq!(id, node_id);
            }
            other => panic!("unexpected status: {:?}", other),
        }
    }

    #[test]
    fn node_stop_emits_worker_stop() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id], 0);
        let mut txs = TestBuffer::new(0);

        let outcome = manager.apply_event(NodeEvent::NodeStop, &mut txs, Duration::from_secs(1));

        assert!(
            outcome
                .mutations
                .iter()
                .any(|event| matches!(event, WorkerMutation::WorkerStop))
        );
    }
}
