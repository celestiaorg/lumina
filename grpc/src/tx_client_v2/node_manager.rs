use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::debug;

use crate::tx_client_v2::tx_buffer::TxBuffer;
use crate::tx_client_v2::{
    ConfirmationResponse, NodeEvent, NodeId, NodeResponse, PlanRequest, RejectionReason,
    RequestWithChannels, SigningError, StopError, SubmitError, TxConfirmResult, TxIdT, TxServer,
    TxSigningResult, TxStatus, TxStatusKind, TxSubmitResult,
};

#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
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
    <S as TxServer>::ConfirmResponse,
    <S as TxServer>::SubmitError,
    <S as TxServer>::TxRequest,
>;
type StopErrorFor<S> = StopError<
    <S as TxServer>::SubmitError,
    <S as TxServer>::ConfirmInfo,
    <S as TxServer>::ConfirmResponse,
>;
type NodeBuffer<S> = TxBuffer<
    <S as TxServer>::TxId,
    <S as TxServer>::ConfirmInfo,
    <S as TxServer>::ConfirmResponse,
    <S as TxServer>::SubmitError,
    PendingRequest<S>,
>;
type WorkerPlanFor<S> = WorkerPlan<<S as TxServer>::TxId>;
type NodeStateFor<S> = NodeState<
    <S as TxServer>::ConfirmInfo,
    <S as TxServer>::SubmitError,
    <S as TxServer>::ConfirmResponse,
>;
type NodeOutcome<S> = NodeApplyOutcome<
    <S as TxServer>::TxId,
    <S as TxServer>::ConfirmInfo,
    <S as TxServer>::ConfirmResponse,
    <S as TxServer>::SubmitError,
>;
type StatusesFor<S> =
    Statuses<<S as TxServer>::TxId, <S as TxServer>::ConfirmInfo, <S as TxServer>::ConfirmResponse>;
type FatalStatus<S> = Option<(
    u64,
    TxStatus<<S as TxServer>::ConfirmInfo, <S as TxServer>::ConfirmResponse>,
)>;
type MutationsFor<S> = Mutations<
    <S as TxServer>::TxId,
    <S as TxServer>::ConfirmInfo,
    <S as TxServer>::ConfirmResponse,
    <S as TxServer>::SubmitError,
>;
type PlanRequestsRef<'a> = &'a mut Vec<PlanRequest>;
type Mutations<TxId, ConfirmInfo, ConfirmResponse, SubmitErr> =
    Vec<WorkerMutation<TxId, ConfirmInfo, ConfirmResponse, SubmitErr>>;
type Statuses<TxId, ConfirmInfo, ConfirmResponse> =
    Vec<(TxId, TxStatus<ConfirmInfo, ConfirmResponse>)>;

#[derive(Debug)]
pub(crate) enum WorkerMutation<TxId: TxIdT, ConfirmInfo, ConfirmResponse, SubmitErr> {
    EnqueueSigned {
        tx: crate::tx_client_v2::Transaction<TxId, ConfirmInfo, ConfirmResponse, SubmitErr>,
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

impl<TxId: TxIdT, ConfirmInfo, ConfirmResponse, SubmitErr>
    WorkerMutation<TxId, ConfirmInfo, ConfirmResponse, SubmitErr>
{
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

pub(crate) struct NodeApplyOutcome<TxId: TxIdT, ConfirmInfo, ConfirmResponse, SubmitErr> {
    pub(crate) mutations: Vec<WorkerMutation<TxId, ConfirmInfo, ConfirmResponse, SubmitErr>>,
    pub(crate) plan_requests: Vec<PlanRequest>,
}

impl<SubmitErr, ConfirmInfo, ConfirmResponse> StopError<SubmitErr, ConfirmInfo, ConfirmResponse> {
    fn label(&self) -> String {
        match self {
            StopError::ConfirmError(status) => {
                format!("ConfirmError({})", tx_status_label(status))
            }
            StopError::SubmitError(_) => "SubmitError".to_string(),
            StopError::SignError(_) => "SignError".to_string(),
            StopError::WorkerStopped => "WorkerStopped".to_string(),
        }
    }
}

#[derive(Debug, Clone, Default)]
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
struct StoppedState<ConfirmInfo, SubmitErr, ConfirmResponse> {
    shared: NodeShared,
    stop_seq: u64,
    error: StopError<SubmitErr, ConfirmInfo, ConfirmResponse>,
}

#[derive(Debug, Clone)]
enum NodeState<ConfirmInfo, SubmitErr, ConfirmResponse> {
    Active(ActiveState),
    Recovering(RecoveringState),
    Stopped(StoppedState<ConfirmInfo, SubmitErr, ConfirmResponse>),
}

impl<ConfirmInfo: Clone, SubmitErr, ConfirmResponse>
    NodeState<ConfirmInfo, SubmitErr, ConfirmResponse>
{
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

    fn stop_submissions(&mut self, error: StopError<SubmitErr, ConfirmInfo, ConfirmResponse>) {
        let shared = self.shared();
        let last_submitted = shared.last_submitted;
        let stop_seq = last_submitted
            .unwrap_or(shared.last_confirmed)
            .max(shared.last_confirmed);
        self.stop_with_error(stop_seq, error);
    }

    fn stop_with_error(
        &mut self,
        seq: u64,
        error: StopError<SubmitErr, ConfirmInfo, ConfirmResponse>,
    ) {
        let shared = self.shared().clone();
        let stop_seq = seq;
        *self = NodeState::Stopped(StoppedState {
            shared,
            stop_seq,
            error,
        });
    }

    fn apply_stop_error(
        &mut self,
        seq: u64,
        error: StopError<SubmitErr, ConfirmInfo, ConfirmResponse>,
    ) {
        match self {
            NodeState::Stopped(state) => {
                let prev_stop_seq = state.stop_seq;
                if seq < state.stop_seq {
                    state.stop_seq = seq;
                }
                if matches!(state.error, StopError::WorkerStopped) || seq < prev_stop_seq {
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
    nodes: HashMap<NodeId, NodeStateFor<S>>,
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

    pub(crate) fn should_stop(&self) -> bool {
        let mut all_stopped = true;
        for (node_id, node) in self.nodes.iter() {
            let submitted_seq = node.shared().last_submitted.unwrap_or(0);
            let confirmed_seq = node.shared().last_confirmed;
            let node_should_stop = match node {
                NodeState::Stopped(state) => confirmed_seq >= state.stop_seq.min(submitted_seq),
                _ => false,
            };
            all_stopped = all_stopped && node_should_stop;
            debug!(
                node_id = %node_id.as_ref(),
                state = %node.log_state(),
                confirmed_seq,
                node_should_stop,
                "should_stop check"
            );
        }
        all_stopped
    }

    pub(crate) fn min_confirmed_non_stopped(&self) -> Option<u64> {
        let mut min_confirmed: Option<u64> = None;
        for node in self.nodes.values() {
            if node.is_stopped() {
                continue;
            }
            let confirmed = node.shared().last_confirmed;
            min_confirmed = Some(
                min_confirmed
                    .map(|current| current.min(confirmed))
                    .unwrap_or(confirmed),
            );
        }
        min_confirmed
    }

    pub(crate) fn tail_stop_error(&self) -> Option<StopErrorFor<S>> {
        let mut best: Option<(u64, StopErrorFor<S>)> = None;
        for node in self.nodes.values() {
            if let NodeState::Stopped(state) = node {
                match &best {
                    None => best = Some((state.stop_seq, state.error.clone())),
                    Some((seq, _)) if state.stop_seq > *seq => {
                        best = Some((state.stop_seq, state.error.clone()))
                    }
                    _ => {}
                }
            }
        }
        best.map(|(_, error)| error)
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
                        if let Some(plan) = plan_submission::<S>(node_id, node, txs) {
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
                        if let Some(plan) =
                            plan_confirmation::<S>(node_id, node, txs, max_status_batch)
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
        event: NodeEvent<S::TxId, S::ConfirmInfo, S::ConfirmResponse, S::SubmitError>,
        txs: &NodeBuffer<S>,
        confirm_interval: Duration,
    ) -> NodeOutcome<S> {
        let mut mutations = Vec::new();
        let mut plan_requests = Vec::new();

        debug!(event = %event.summary(), "node event");
        match event {
            NodeEvent::NodeResponse { node_id, response } => {
                {
                    let node = self.nodes.get_mut(&node_id).unwrap();
                    debug!(
                        node_id = %node_id.as_ref(),
                        state = %node.log_state(),
                        "node state (before response)"
                    );
                }
                let outcome = match response {
                    NodeResponse::Submission { sequence, result } => {
                        self.handle_submission(&node_id, sequence, result, txs, confirm_interval)
                    }
                    NodeResponse::Confirmation {
                        state_epoch,
                        response,
                    } => self.handle_confirmation(
                        &node_id,
                        state_epoch,
                        response,
                        txs,
                        confirm_interval,
                    ),
                    NodeResponse::Signing { sequence, result } => {
                        self.handle_signing(&node_id, sequence, result, confirm_interval)
                    }
                };
                mutations.extend(outcome.mutations);
                plan_requests.extend(outcome.plan_requests);
                {
                    let node = self.nodes.get_mut(&node_id).unwrap();
                    debug!(
                        node_id = %node_id.as_ref(),
                        state = %node.log_state(),
                        "node state (after response)"
                    );
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
                        node.stop_submissions(StopError::WorkerStopped);
                    }
                    debug!(
                        node_id = %node_id.as_ref(),
                        state = %node.log_state(),
                        "node state (after stop)"
                    );
                }
            }
        }

        if self.should_stop() {
            mutations.push(WorkerMutation::WorkerStop);
        }

        NodeApplyOutcome {
            mutations,
            plan_requests,
        }
    }

    fn handle_submission(
        &mut self,
        node_id: &NodeId,
        sequence: u64,
        result: TxSubmitResult<S::TxId, S::SubmitError>,
        txs: &NodeBuffer<S>,
        confirm_interval: Duration,
    ) -> NodeOutcome<S> {
        let mut mutations = Vec::new();
        let mut plan_requests = Vec::new();

        let max_sub = self.max_active_last_submitted();
        let Some(node) = self.nodes.get_mut(node_id) else {
            return NodeApplyOutcome {
                mutations,
                plan_requests,
            };
        };
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
                    mutations.push(WorkerMutation::MarkSubmitted { sequence, id });
                    let last_submitted = active.shared.last_submitted.unwrap_or(0);
                    let next_sequence = txs.next_sequence().saturating_sub(1);
                    if last_submitted == next_sequence && txs.has_pending() {
                        push_plan_request(&mut plan_requests, PlanRequest::Signing);
                    }
                }
            }
            Err(failure) => {
                debug!(
                    node_id = %node_id.as_ref(),
                    sequence,
                    failure = %failure.label(),
                    "submission error"
                );
                if let NodeState::Active(active) = node {
                    on_submit_failure(active);
                }
                match failure.mapped_error {
                    SubmitError::SequenceMismatch { expected } => {
                        let stop_error: StopErrorFor<S> =
                            StopError::SubmitError(failure.original_error.clone());
                        let recovering =
                            on_sequence_mismatch::<S>(node, sequence, expected, stop_error, txs);
                        if recovering {
                            push_plan_request(&mut plan_requests, PlanRequest::Confirmation);
                        }
                    }
                    SubmitError::MempoolIsFull | SubmitError::NetworkError => {
                        node.shared_mut().submit_delay = Some(confirm_interval);
                    }
                    SubmitError::TxInMempoolCache => {
                        if max_sub
                            .map(|(_, max)| max > node.shared().last_submitted.unwrap_or(0))
                            .unwrap_or(false)
                        {
                            let last = node.shared_mut().last_submitted.get_or_insert(0);
                            *last = last.saturating_add(1);
                        }
                    }
                    SubmitError::InsufficientFee { .. } => {
                        node.stop_submissions(StopError::SubmitError(
                            failure.original_error.clone(),
                        ));
                    }
                    SubmitError::Other { .. } => {
                        node.stop_submissions(StopError::SubmitError(
                            failure.original_error.clone(),
                        ));
                    }
                }
            }
        }
        push_plan_request(&mut plan_requests, PlanRequest::Submission);
        NodeApplyOutcome {
            mutations,
            plan_requests,
        }
    }

    fn handle_confirmation(
        &mut self,
        node_id: &NodeId,
        state_epoch: u64,
        response: TxConfirmResult<
            ConfirmationResponse<S::TxId, S::ConfirmInfo, S::ConfirmResponse>,
        >,
        txs: &NodeBuffer<S>,
        _confirm_interval: Duration,
    ) -> NodeOutcome<S> {
        let mut mutations = Vec::new();
        let mut plan_requests = Vec::new();

        let Some(node) = self.nodes.get_mut(node_id) else {
            return NodeApplyOutcome {
                mutations,
                plan_requests,
            };
        };
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
                ConfirmationResponse::Batch { statuses } => {
                    let mut effects =
                        process_status_batch::<S>(node_id, node, statuses, txs, &mut plan_requests);
                    mutations.append(&mut effects);
                }
                ConfirmationResponse::Single { id, status } => {
                    let mut effects = process_recover_status::<S>(node, id, status, txs);
                    mutations.append(&mut effects);
                }
            },
            Err(err) => {
                debug!(
                    node_id = %node_id.as_ref(),
                    error = %err,
                    is_network = err.is_network_error(),
                    "confirmation error"
                );
                // TODO: add error handling
            }
        }
        NodeApplyOutcome {
            mutations,
            plan_requests,
        }
    }

    fn handle_signing(
        &mut self,
        node_id: &NodeId,
        sequence: u64,
        result: TxSigningResult<S::TxId, S::ConfirmInfo, S::ConfirmResponse, S::SubmitError>,
        confirm_interval: Duration,
    ) -> NodeOutcome<S> {
        let mut mutations = Vec::new();
        let mut plan_requests = Vec::new();
        let mut stop_error: Option<StopErrorFor<S>> = None;
        let Some(node) = self.nodes.get_mut(node_id) else {
            return NodeApplyOutcome {
                mutations,
                plan_requests,
            };
        };
        node.shared_mut().signing_inflight = false;
        match result {
            Ok(tx) => {
                mutations.push(WorkerMutation::EnqueueSigned { tx });
                push_plan_request(&mut plan_requests, PlanRequest::Submission);
            }
            Err(failure) => {
                debug!(
                    node_id = %node_id.as_ref(),
                    sequence,
                    failure = %failure.label(),
                    "signing error"
                );
                match failure.mapped_error {
                    SigningError::SequenceMismatch { expected } => {
                        if expected >= sequence {
                            stop_error = Some(StopError::SignError(failure.original_error.clone()));
                        } else {
                            self.signing_delay = Some(confirm_interval);
                            push_plan_request(&mut plan_requests, PlanRequest::Signing);
                        }
                    }
                    SigningError::NetworkError => {
                        self.signing_delay = Some(confirm_interval);
                        push_plan_request(&mut plan_requests, PlanRequest::Signing);
                    }
                    SigningError::Other { .. } => {
                        stop_error = Some(StopError::SignError(failure.original_error.clone()));
                    }
                }
            }
        }
        if let Some(stop_error) = stop_error {
            self.stop_all_nodes(stop_error);
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

    fn stop_all_nodes(&mut self, error: StopErrorFor<S>) {
        for node in self.nodes.values_mut() {
            if node.is_stopped() {
                continue;
            }
            node.stop_submissions(error.clone());
        }
    }
}

fn push_plan_request(requests: &mut Vec<PlanRequest>, request: PlanRequest) {
    if !requests.contains(&request) {
        requests.push(request);
    }
}

fn tx_status_label<ConfirmInfo, ConfirmResponse>(
    status: &TxStatus<ConfirmInfo, ConfirmResponse>,
) -> &'static str {
    match status.kind {
        TxStatusKind::Pending => "Pending",
        TxStatusKind::Confirmed { .. } => "Confirmed",
        TxStatusKind::Rejected { .. } => "Rejected",
        TxStatusKind::Evicted => "Evicted",
        TxStatusKind::Unknown => "Unknown",
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

fn plan_submission<S: TxServer>(
    node_id: &NodeId,
    node: &mut NodeStateFor<S>,
    txs: &NodeBuffer<S>,
) -> Option<WorkerPlanFor<S>> {
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

fn plan_confirmation<S: TxServer>(
    node_id: &NodeId,
    node: &mut NodeStateFor<S>,
    txs: &NodeBuffer<S>,
    max_batch: usize,
) -> Option<WorkerPlanFor<S>> {
    match node {
        NodeState::Active(state) => {
            if state.shared.confirm_inflight {
                debug!(
                    node_id = %node_id.as_ref(),
                    "plan_confirmation: skip active, confirm_inflight"
                );
                return None;
            }
            let Some(last) = state.shared.last_submitted else {
                debug!(
                    node_id = %node_id.as_ref(),
                    "plan_confirmation: skip active, no last_submitted"
                );
                return None;
            };
            let ids = match txs.ids_from_to(state.shared.last_confirmed + 1, last, max_batch) {
                Some(ids) => ids,
                None => {
                    debug!(
                        node_id = %node_id.as_ref(),
                        start = state.shared.last_confirmed + 1,
                        stop = last,
                        confirmed_seq = txs.confirmed_seq(),
                        max_seq = txs.max_seq(),
                        "plan_confirmation: skip active, no ids in range"
                    );
                    return None;
                }
            };
            if ids.is_empty() {
                debug!(
                    node_id = %node_id.as_ref(),
                    start = state.shared.last_confirmed + 1,
                    stop = last,
                    "plan_confirmation: skip active, empty ids"
                );
                return None;
            }
            let epoch = state.shared.epoch;
            state.shared.confirm_inflight = true;
            debug!(
                node_id = %node_id.as_ref(),
                start = state.shared.last_confirmed + 1,
                stop = last,
                ids_len = ids.len(),
                "plan_confirmation: spawn active confirm batch"
            );
            Some(WorkerPlan::SpawnConfirmBatch {
                node_id: node_id.clone(),
                ids,
                epoch,
            })
        }
        NodeState::Recovering(state) => {
            if state.shared.confirm_inflight {
                debug!(
                    node_id = %node_id.as_ref(),
                    "plan_confirmation: skip recovering, confirm_inflight"
                );
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
                debug!(
                    node_id = %node_id.as_ref(),
                    "plan_confirmation: skip stopped, confirm_inflight"
                );
                return None;
            }
            let Some(last) = state.shared.last_submitted else {
                debug!(
                    node_id = %node_id.as_ref(),
                    "plan_confirmation: skip stopped, no last_submitted"
                );
                return None;
            };
            let confirm_upto = last.min(state.stop_seq);
            let ids =
                match txs.ids_from_to(state.shared.last_confirmed + 1, confirm_upto, max_batch) {
                    Some(ids) => ids,
                    None => {
                        debug!(
                            node_id = %node_id.as_ref(),
                            start = state.shared.last_confirmed + 1,
                            stop = confirm_upto,
                            confirmed_seq = txs.confirmed_seq(),
                            max_seq = txs.max_seq(),
                            "plan_confirmation: skip stopped, no ids in range"
                        );
                        return None;
                    }
                };
            if ids.is_empty() {
                debug!(
                    node_id = %node_id.as_ref(),
                    start = state.shared.last_confirmed + 1,
                    stop = confirm_upto,
                    "plan_confirmation: skip stopped, empty ids"
                );
                return None;
            }
            let epoch = state.shared.epoch;
            state.shared.confirm_inflight = true;
            debug!(
                node_id = %node_id.as_ref(),
                start = state.shared.last_confirmed + 1,
                stop = confirm_upto,
                ids_len = ids.len(),
                "plan_confirmation: spawn stopped confirm batch"
            );
            Some(WorkerPlan::SpawnConfirmBatch {
                node_id: node_id.clone(),
                ids,
                epoch,
            })
        }
    }
}

fn on_sequence_mismatch<S: TxServer>(
    node: &mut NodeStateFor<S>,
    sequence: u64,
    expected: u64,
    stop_error: StopErrorFor<S>,
    txs: &NodeBuffer<S>,
) -> bool {
    let mut stop_error = Some(stop_error);
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
            let stop_at = shared.last_submitted.unwrap_or(shared.last_confirmed);
            node.apply_stop_error(stop_at, stop_error.take().unwrap());
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
        node.apply_stop_error(stop_seq, stop_error.take().unwrap());
    }
    false
}

fn process_status_batch<S: TxServer>(
    node_id: &NodeId,
    node: &mut NodeStateFor<S>,
    statuses: StatusesFor<S>,
    txs: &NodeBuffer<S>,
    plan_requests: PlanRequestsRef<'_>,
) -> MutationsFor<S> {
    let mut effects: MutationsFor<S> = Vec::new();
    let mut collected = statuses
        .into_iter()
        .filter_map(|(tx_id, status)| txs.get_by_id(&tx_id).map(|tx| (tx.sequence, status)))
        .collect::<Vec<_>>();
    collected.sort_by(|first, second| first.0.cmp(&second.0));
    if collected.is_empty() {
        return effects;
    }
    let mut fatal: FatalStatus<S> = None;
    let mut seq_mismatch: Option<(u64, u64, StopErrorFor<S>)> = None;
    {
        let shared = match node {
            NodeState::Recovering(_) => {
                return effects;
            }
            _ => node.shared_mut(),
        };
        let offset = shared.last_confirmed + 1;
        let last_confirmed = shared.last_confirmed;
        let last_submitted = shared.last_submitted;
        for (idx, (sequence, status)) in collected.into_iter().enumerate() {
            if idx as u64 + offset != sequence {
                debug!(
                    node_id = %node_id.as_ref(),
                    idx,
                    expected_seq = idx as u64 + offset,
                    got_seq = sequence,
                    "process_status_batch: gap encountered"
                );
                break;
            }
            let TxStatus {
                kind,
                original_response,
            } = status;
            match kind {
                TxStatusKind::Pending => {
                    debug!(
                        node_id = %node_id.as_ref(),
                        sequence,
                        last_confirmed,
                        last_submitted = ?last_submitted,
                        "process_status_batch: pending encountered"
                    );
                    break;
                }
                TxStatusKind::Evicted => {
                    let clamped = sequence.saturating_sub(1).max(txs.confirmed_seq());
                    shared.last_submitted = Some(clamped);
                    push_plan_request(plan_requests, PlanRequest::Submission);
                    break;
                }
                TxStatusKind::Rejected { reason } => match reason {
                    RejectionReason::SequenceMismatch { expected, node_id } => {
                        let status = TxStatus::new(
                            TxStatusKind::Rejected {
                                reason: RejectionReason::SequenceMismatch { expected, node_id },
                            },
                            original_response,
                        );
                        seq_mismatch = Some((sequence, expected, StopError::ConfirmError(status)));
                        break;
                    }
                    other => {
                        fatal = Some((
                            sequence,
                            TxStatus::new(
                                TxStatusKind::Rejected { reason: other },
                                original_response,
                            ),
                        ));
                        break;
                    }
                },
                TxStatusKind::Confirmed { info } => {
                    shared.last_confirmed = sequence;
                    effects.push(WorkerMutation::Confirm {
                        seq: sequence,
                        info,
                    });
                }
                TxStatusKind::Unknown => {
                    fatal = Some((
                        sequence,
                        TxStatus::new(TxStatusKind::Unknown, original_response),
                    ));
                    break;
                }
            }
        }
    }
    if let Some((sequence, expected, stop_error)) = seq_mismatch {
        let recovering = on_sequence_mismatch::<S>(node, sequence, expected, stop_error, txs);
        if recovering {
            push_plan_request(plan_requests, PlanRequest::Confirmation);
        }
    }
    if let Some((sequence, status)) = fatal {
        node.apply_stop_error(sequence, StopError::ConfirmError(status));
    }
    effects
}

fn process_recover_status<S: TxServer>(
    node: &mut NodeStateFor<S>,
    id: S::TxId,
    status: TxStatus<S::ConfirmInfo, S::ConfirmResponse>,
    txs: &NodeBuffer<S>,
) -> MutationsFor<S> {
    let effects: MutationsFor<S> = Vec::new();
    let Some(seq) = txs.get_seq(&id) else {
        return effects;
    };
    let mut fatal: Option<TxStatus<S::ConfirmInfo, S::ConfirmResponse>> = None;
    let mut confirmed = false;
    {
        let NodeState::Recovering(state) = node else {
            return effects;
        };
        let TxStatus {
            kind,
            original_response,
        } = status;
        match kind {
            TxStatusKind::Confirmed { .. } | TxStatusKind::Pending => {
                state.shared.last_submitted =
                    Some(state.shared.last_submitted.unwrap_or(seq).max(seq));
                state.shared.epoch = state.shared.epoch.saturating_add(1);
                confirmed = true;
            }
            other => {
                fatal = Some(TxStatus::new(other, original_response));
            }
        }
    }
    if confirmed {
        node.transition_to_active();
    }
    if let Some(status) = fatal {
        node.apply_stop_error(seq, StopError::ConfirmError(status));
    }
    effects
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::tx_client_v2::{
        ConfirmationResponse, SigningFailure, SubmitFailure, Transaction, TxCallbacks,
    };
    use async_trait::async_trait;

    type TestConfirmResponse = ();
    type TestSubmitError = ();
    type TestStatus = TxStatus<u64, TestConfirmResponse>;

    fn status(kind: TxStatusKind<u64>) -> TestStatus {
        TxStatus::new(kind, ())
    }

    fn submit_failure(mapped_error: SubmitError) -> SubmitFailure<TestSubmitError> {
        SubmitFailure {
            mapped_error,
            original_error: (),
        }
    }

    #[derive(Debug)]
    struct DummyServer;

    #[async_trait]
    impl TxServer for DummyServer {
        type TxId = u64;
        type ConfirmInfo = u64;
        type SubmitError = TestSubmitError;
        type ConfirmResponse = TestConfirmResponse;
        type TxRequest = crate::tx_client_v2::TxRequest;

        async fn submit(
            &self,
            _tx_bytes: Arc<Vec<u8>>,
            _sequence: u64,
        ) -> crate::tx_client_v2::TxSubmitResult<Self::TxId, Self::SubmitError> {
            unimplemented!()
        }

        async fn status_batch(
            &self,
            _ids: Vec<Self::TxId>,
        ) -> crate::tx_client_v2::TxConfirmResult<
            Vec<(
                Self::TxId,
                TxStatus<Self::ConfirmInfo, Self::ConfirmResponse>,
            )>,
        > {
            unimplemented!()
        }

        async fn status(
            &self,
            _id: Self::TxId,
        ) -> crate::tx_client_v2::TxConfirmResult<TxStatus<Self::ConfirmInfo, Self::ConfirmResponse>>
        {
            unimplemented!()
        }

        async fn current_sequence(&self) -> crate::tx_client_v2::Result<u64> {
            unimplemented!()
        }

        async fn simulate_and_sign(
            &self,
            _req: Arc<Self::TxRequest>,
            _sequence: u64,
        ) -> crate::tx_client_v2::TxSigningResult<
            Self::TxId,
            Self::ConfirmInfo,
            Self::ConfirmResponse,
            Self::SubmitError,
        > {
            unimplemented!()
        }
    }

    type TestBuffer =
        TxBuffer<u64, u64, TestConfirmResponse, TestSubmitError, PendingRequest<DummyServer>>;

    fn make_tx(
        sequence: u64,
        id: Option<u64>,
    ) -> Transaction<u64, u64, TestConfirmResponse, TestSubmitError> {
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

        let plans = manager.plan(&[PlanRequest::Submission], &txs, 16);

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
        let txs = TestBuffer::new(0);

        let outcome = manager.apply_event(
            NodeEvent::NodeResponse {
                node_id: node_id.clone(),
                response: NodeResponse::Submission {
                    sequence: 1,
                    result: Ok(10),
                },
            },
            &txs,
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
        let txs = make_buffer_with_ids();

        let statuses = vec![
            (10, status(TxStatusKind::Pending)),
            (11, status(TxStatusKind::Confirmed { info: 2 })),
        ];
        let outcome = manager.apply_event(
            NodeEvent::NodeResponse {
                node_id,
                response: NodeResponse::Confirmation {
                    state_epoch: 0,
                    response: Ok(ConfirmationResponse::Batch { statuses }),
                },
            },
            &txs,
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
        let txs = make_buffer_with_ids();

        let statuses = vec![
            (10, status(TxStatusKind::Confirmed { info: 1 })),
            (11, status(TxStatusKind::Confirmed { info: 2 })),
        ];
        let outcome = manager.apply_event(
            NodeEvent::NodeResponse {
                node_id,
                response: NodeResponse::Confirmation {
                    state_epoch: 0,
                    response: Ok(ConfirmationResponse::Batch { statuses }),
                },
            },
            &txs,
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
                    result: Err(submit_failure(SubmitError::SequenceMismatch {
                        expected: 2,
                    })),
                },
            },
            &txs,
            Duration::from_secs(1),
        );

        assert!(outcome.plan_requests.contains(&PlanRequest::Confirmation));

        let wake = manager.plan(&outcome.plan_requests, &txs, 16);

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
                    result: Err(submit_failure(SubmitError::SequenceMismatch {
                        expected: 2,
                    })),
                },
            },
            &txs,
            Duration::from_secs(1),
        );

        assert!(matches!(
            manager.nodes.get(&node_id).unwrap(),
            NodeState::Stopped(_)
        ));
        let error = manager.tail_stop_error().expect("missing stop error");
        assert!(matches!(error, StopError::SubmitError(_)));
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
            status(TxStatusKind::Rejected {
                reason: RejectionReason::SequenceMismatch {
                    expected: 3,
                    node_id: node_id.clone(),
                },
            }),
        )];
        let _ = manager.apply_event(
            NodeEvent::NodeResponse {
                node_id: node_id.clone(),
                response: NodeResponse::Confirmation {
                    state_epoch: 0,
                    response: Ok(ConfirmationResponse::Batch { statuses }),
                },
            },
            &txs,
            Duration::from_secs(1),
        );

        assert!(matches!(
            manager.nodes.get(&node_id).unwrap(),
            NodeState::Stopped(_)
        ));
        let error = manager.tail_stop_error().expect("missing stop error");
        match error {
            StopError::ConfirmError(status) => match status.kind {
                TxStatusKind::Rejected {
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
            },
            other => panic!("unexpected stop error: {:?}", other),
        }
    }

    #[test]
    fn node_stop_emits_worker_stop() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id], 0);
        let txs = TestBuffer::new(0);

        let outcome = manager.apply_event(NodeEvent::NodeStop, &txs, Duration::from_secs(1));

        assert!(
            outcome
                .mutations
                .iter()
                .any(|event| matches!(event, WorkerMutation::WorkerStop))
        );
    }

    #[test]
    fn signing_success_enqueues_and_requests_submission() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], 0);
        let txs = TestBuffer::new(0);

        if let NodeState::Active(state) = manager.nodes.get_mut(&node_id).unwrap() {
            state.shared.signing_inflight = true;
        }

        let outcome = manager.apply_event(
            NodeEvent::NodeResponse {
                node_id: node_id.clone(),
                response: NodeResponse::Signing {
                    sequence: 1,
                    result: Ok(make_tx(1, None)),
                },
            },
            &txs,
            Duration::from_secs(1),
        );

        assert!(outcome.plan_requests.contains(&PlanRequest::Submission));
        assert!(outcome.mutations.iter().any(
            |event| matches!(event, WorkerMutation::EnqueueSigned { tx } if tx.sequence == 1)
        ));
        assert!(
            !manager
                .nodes
                .get(&node_id)
                .unwrap()
                .shared()
                .signing_inflight
        );
    }

    #[test]
    fn signing_sequence_mismatch_reschedules_with_delay() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], 0);
        let txs = TestBuffer::new(0);

        if let NodeState::Active(state) = manager.nodes.get_mut(&node_id).unwrap() {
            state.shared.signing_inflight = true;
        }

        let outcome = manager.apply_event(
            NodeEvent::NodeResponse {
                node_id: node_id.clone(),
                response: NodeResponse::Signing {
                    sequence: 10,
                    result: Err(SigningFailure {
                        mapped_error: SigningError::SequenceMismatch { expected: 9 },
                        original_error: (),
                    }),
                },
            },
            &txs,
            Duration::from_secs(3),
        );

        assert!(outcome.plan_requests.contains(&PlanRequest::Signing));
        assert!(manager.signing_delay.is_some());
        assert!(matches!(
            manager.nodes.get(&node_id).unwrap(),
            NodeState::Active(_)
        ));
    }

    #[test]
    fn confirmation_epoch_mismatch_resets_inflight_and_requests_confirmation() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], 0);
        let txs = TestBuffer::new(0);

        if let NodeState::Active(state) = manager.nodes.get_mut(&node_id).unwrap() {
            state.shared.confirm_inflight = true;
        }

        let outcome = manager.apply_event(
            NodeEvent::NodeResponse {
                node_id: node_id.clone(),
                response: NodeResponse::Confirmation {
                    state_epoch: 1,
                    response: Ok(ConfirmationResponse::Batch {
                        statuses: Vec::new(),
                    }),
                },
            },
            &txs,
            Duration::from_secs(1),
        );

        assert!(outcome.plan_requests.contains(&PlanRequest::Confirmation));
        assert!(outcome.mutations.is_empty());
        assert!(
            !manager
                .nodes
                .get(&node_id)
                .unwrap()
                .shared()
                .confirm_inflight
        );
    }

    #[test]
    fn submission_mempool_cache_advances_last_submitted() {
        let node_a: NodeId = Arc::from("node-a");
        let node_b: NodeId = Arc::from("node-b");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_a.clone(), node_b.clone()], 0);
        let txs = TestBuffer::new(0);

        if let NodeState::Active(state) = manager.nodes.get_mut(&node_a).unwrap() {
            state.shared.last_submitted = Some(5);
        }
        if let NodeState::Active(state) = manager.nodes.get_mut(&node_b).unwrap() {
            state.shared.last_submitted = Some(7);
        }

        let outcome = manager.apply_event(
            NodeEvent::NodeResponse {
                node_id: node_a.clone(),
                response: NodeResponse::Submission {
                    sequence: 6,
                    result: Err(submit_failure(SubmitError::TxInMempoolCache)),
                },
            },
            &txs,
            Duration::from_secs(2),
        );

        assert!(outcome.plan_requests.contains(&PlanRequest::Submission));
        let NodeState::Active(state) = manager.nodes.get(&node_a).unwrap() else {
            panic!("node_a should be active");
        };
        assert_eq!(state.shared.last_submitted, Some(6));
        assert_eq!(state.shared.submit_delay, None);
    }

    #[test]
    fn confirmation_pending_keeps_last_confirmed() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], 0);
        let mut txs = TestBuffer::new(0);

        txs.add_transaction(make_tx(6, None)).unwrap();
        txs.add_transaction(make_tx(7, None)).unwrap();
        txs.set_submitted_id(6, 10);
        txs.set_submitted_id(7, 11);

        if let NodeState::Active(state) = manager.nodes.get_mut(&node_id).unwrap() {
            state.shared.last_confirmed = 5;
            state.shared.last_submitted = Some(7);
        }

        let statuses = vec![
            (10, status(TxStatusKind::Pending)),
            (11, status(TxStatusKind::Confirmed { info: 2 })),
        ];
        let _ = manager.apply_event(
            NodeEvent::NodeResponse {
                node_id: node_id.clone(),
                response: NodeResponse::Confirmation {
                    state_epoch: 0,
                    response: Ok(ConfirmationResponse::Batch { statuses }),
                },
            },
            &txs,
            Duration::from_secs(1),
        );

        let NodeState::Active(state) = manager.nodes.get(&node_id).unwrap() else {
            panic!("node should be active");
        };
        assert_eq!(state.shared.last_confirmed, 5);
    }
}
