use std::collections::HashMap;
use std::time::Duration;

use tracing::debug;

use crate::tx_client_v2::tx_buffer::TxBuffer;
use crate::tx_client_v2::{
    ConfirmationResponse, NodeEvent, NodeId, NodeResponse, RejectionReason, RequestWithChannels,
    SigningError, StopError, SubmitError, TxConfirmResult, TxServer, TxSigningResult, TxStatus,
    TxStatusKind, TxSubmitResult, WorkerMutation, WorkerPlan,
};

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
type StatusesFor<S> =
    Statuses<<S as TxServer>::TxId, <S as TxServer>::ConfirmInfo, <S as TxServer>::ConfirmResponse>;
type FatalStatus<S> = Option<(
    u64,
    TxStatus<<S as TxServer>::ConfirmInfo, <S as TxServer>::ConfirmResponse>,
)>;
type MutationsFor<S> = Vec<
    WorkerMutation<
        <S as TxServer>::TxId,
        <S as TxServer>::ConfirmInfo,
        <S as TxServer>::ConfirmResponse,
        <S as TxServer>::SubmitError,
    >,
>;
type Statuses<TxId, ConfirmInfo, ConfirmResponse> =
    Vec<(TxId, TxStatus<ConfirmInfo, ConfirmResponse>)>;

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

#[derive(Debug, Clone, Copy, Default)]
struct CommonState {
    submit_delay: Option<Duration>,
    signing_delay: Option<Duration>,
    epoch: u64,
    last_submitted: Option<u64>,
    last_confirmed: Option<u64>,
    confirm_inflight: bool,
    signing_inflight: bool,
}

#[derive(Debug, Clone)]
struct ActiveState {
    shared: CommonState,
    pending: Option<u64>,
}

#[derive(Debug, Clone)]
struct RecoveringState {
    shared: CommonState,
    target: u64,
}

#[derive(Debug, Clone)]
struct StoppedState<ConfirmInfo, SubmitErr, ConfirmResponse> {
    shared: CommonState,
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
    fn new(start_confirmed: Option<u64>) -> Self {
        let shared = CommonState {
            last_confirmed: start_confirmed,
            ..CommonState::default()
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

    fn shared(&self) -> CommonState {
        match self {
            NodeState::Active(state) => state.shared,
            NodeState::Recovering(state) => state.shared,
            NodeState::Stopped(state) => state.shared,
        }
    }

    fn shared_mut(&mut self) -> &mut CommonState {
        match self {
            NodeState::Active(state) => &mut state.shared,
            NodeState::Recovering(state) => &mut state.shared,
            NodeState::Stopped(state) => &mut state.shared,
        }
    }

    fn log_state(&self) -> String {
        match self {
            NodeState::Active(state) => format!(
                "Active last_confirmed={:?} last_submitted={:?} pending={:?} confirm_inflight={} signing_inflight={} epoch={} delay={:?}",
                state.shared.last_confirmed,
                state.shared.last_submitted,
                state.pending,
                state.shared.confirm_inflight,
                state.shared.signing_inflight,
                state.shared.epoch,
                state.shared.submit_delay,
            ),
            NodeState::Recovering(state) => format!(
                "Recovering target={} last_confirmed={:?} last_submitted={:?} confirm_inflight={} signing_inflight={} epoch={} delay={:?}",
                state.target,
                state.shared.last_confirmed,
                state.shared.last_submitted,
                state.shared.confirm_inflight,
                state.shared.signing_inflight,
                state.shared.epoch,
                state.shared.submit_delay,
            ),
            NodeState::Stopped(state) => format!(
                "Stopped stop_seq={} error={} last_confirmed={:?} last_submitted={:?} confirm_inflight={} signing_inflight={} epoch={} delay={:?}",
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
        let stop_seq = stop_seq_from_last_good(shared.last_submitted, shared.last_confirmed);
        self.stop_with_error(stop_seq, error);
    }

    fn stop_with_error(
        &mut self,
        seq: u64,
        error: StopError<SubmitErr, ConfirmInfo, ConfirmResponse>,
    ) {
        let shared = self.shared();
        *self = NodeState::Stopped(StoppedState {
            shared,
            stop_seq: seq,
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
                if seq < state.stop_seq {
                    state.stop_seq = seq;
                    state.error = error;
                    return;
                }
                if matches!(state.error, StopError::WorkerStopped) {
                    state.error = error;
                }
            }
            _ => self.stop_with_error(seq, error),
        }
    }

    fn transition_to_recovering(&mut self, target: u64) {
        let shared = self.shared();
        *self = NodeState::Recovering(RecoveringState { shared, target });
    }

    fn transition_to_active(&mut self) {
        let mut shared = self.shared();
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
    delay: Duration,
}

impl<S: TxServer> NodeManager<S> {
    pub(crate) fn new(
        node_ids: impl IntoIterator<Item = NodeId>,
        start_confirmed: Option<u64>,
    ) -> Self {
        let nodes = node_ids
            .into_iter()
            .map(|id| (id, NodeState::new(start_confirmed)))
            .collect();
        Self {
            nodes,
            delay: Duration::from_millis(500),
        }
    }

    pub(crate) fn should_stop(&self) -> bool {
        let mut all_stopped = true;
        for (node_id, node) in self.nodes.iter() {
            let submitted_next = node
                .shared()
                .last_submitted
                .map(|seq| seq.saturating_add(1))
                .unwrap_or(0);
            let confirmed_next = next_after_confirmed(node.shared().last_confirmed);
            let node_should_stop = match node {
                NodeState::Stopped(state) => {
                    let target_next = state.stop_seq.min(submitted_next);
                    confirmed_next >= target_next
                }
                _ => false,
            };
            all_stopped = all_stopped && node_should_stop;
            debug!(
                node_id = %node_id.as_ref(),
                state = %node.log_state(),
                confirmed_next,
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
            let confirmed = node.shared().last_confirmed?;
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
        txs: &NodeBuffer<S>,
        max_status_batch: usize,
        should_confirm: bool,
    ) -> Vec<WorkerPlanFor<S>> {
        let mut plans = Vec::new();

        if let Some(plan) = self.plan_signing(txs) {
            plans.push(plan);
        }

        for (node_id, node) in self.nodes.iter_mut() {
            debug!(
                node_id = %node_id.as_ref(),
                state = %node.log_state(),
                "node state (plan submission)"
            );
            if let Some(plan) = plan_submission::<S>(node_id, node, txs) {
                plans.push(plan);
            }
        }

        if should_confirm {
            for (node_id, node) in self.nodes.iter_mut() {
                debug!(
                    node_id = %node_id.as_ref(),
                    state = %node.log_state(),
                    "node state (plan confirmation)"
                );
                if let Some(plan) = plan_confirmation::<S>(node_id, node, txs, max_status_batch) {
                    plans.push(plan);
                }
            }
        }

        plans
    }

    pub(crate) fn apply_event(
        &mut self,
        event: NodeEvent<S::TxId, S::ConfirmInfo, S::ConfirmResponse, S::SubmitError>,
        txs: &NodeBuffer<S>,
    ) -> MutationsFor<S> {
        let mut mutations = Vec::new();

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
                let mut outcome = match response {
                    NodeResponse::Submission { sequence, result } => {
                        self.handle_submission(&node_id, sequence, result, txs)
                    }
                    NodeResponse::Confirmation {
                        state_epoch,
                        response,
                    } => self.handle_confirmation(&node_id, state_epoch, response, txs),
                    NodeResponse::Signing { sequence, result } => {
                        self.handle_signing(&node_id, sequence, result)
                    }
                };
                mutations.append(&mut outcome);
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

        mutations
    }

    fn handle_submission(
        &mut self,
        node_id: &NodeId,
        sequence: u64,
        result: TxSubmitResult<S::TxId, S::SubmitError>,
        txs: &NodeBuffer<S>,
    ) -> MutationsFor<S> {
        let mut mutations = Vec::new();
        let Some(node) = self.nodes.get_mut(node_id) else {
            return mutations;
        };
        if node.is_stopped() {
            return mutations;
        }
        match result {
            Ok(id) => {
                if let NodeState::Active(active) = node {
                    on_submit_success(active, sequence);
                    mutations.push(WorkerMutation::MarkSubmitted { sequence, id });
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
                    active.pending = None;
                }
                match failure.mapped_error {
                    SubmitError::SequenceMismatch { expected } => {
                        let stop_error: StopErrorFor<S> =
                            StopError::SubmitError(failure.original_error.clone());
                        on_sequence_mismatch::<S>(
                            node,
                            sequence,
                            expected,
                            stop_error,
                            txs,
                            Some(self.delay),
                        );
                    }
                    SubmitError::MempoolIsFull | SubmitError::NetworkError => {
                        node.shared_mut().submit_delay = Some(self.delay);
                    }
                    SubmitError::TxInMempoolCache => {
                        if let NodeState::Active(state) = node {
                            on_submit_success(state, sequence);
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
        mutations
    }

    fn handle_confirmation(
        &mut self,
        node_id: &NodeId,
        state_epoch: u64,
        response: TxConfirmResult<
            ConfirmationResponse<S::TxId, S::ConfirmInfo, S::ConfirmResponse>,
        >,
        txs: &NodeBuffer<S>,
    ) -> MutationsFor<S> {
        let mut mutations = Vec::new();

        let Some(node) = self.nodes.get_mut(node_id) else {
            return mutations;
        };
        if state_epoch != node.epoch() {
            debug!(
                node_id = %node_id.as_ref(),
                expected_epoch = node.epoch(),
                response_epoch = state_epoch,
                "confirmation epoch mismatch"
            );
            node.shared_mut().confirm_inflight = false;
            return mutations;
        }
        node.shared_mut().confirm_inflight = false;
        match response {
            Ok(confirmation) => match confirmation {
                ConfirmationResponse::Batch { statuses } => {
                    let mut effects = process_status_batch::<S>(node_id, node, statuses, txs);
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
        mutations
    }

    fn handle_signing(
        &mut self,
        node_id: &NodeId,
        sequence: u64,
        result: TxSigningResult<S::TxId, S::ConfirmInfo, S::ConfirmResponse, S::SubmitError>,
    ) -> MutationsFor<S> {
        let mut mutations = Vec::new();
        let mut stop_error: Option<StopErrorFor<S>> = None;
        let Some(node) = self.nodes.get_mut(node_id) else {
            return mutations;
        };
        node.shared_mut().signing_inflight = false;
        match result {
            Ok(tx) => {
                mutations.push(WorkerMutation::EnqueueSigned { tx });
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
                            node.shared_mut().signing_delay = Some(self.delay);
                            let expected_prev = expected.saturating_sub(1);
                            let shared = node.shared_mut();
                            shared.last_submitted = shared
                                .last_submitted
                                .map(|submitted| submitted.min(expected_prev));
                        }
                    }
                    SigningError::NetworkError => {
                        node.shared_mut().signing_delay = Some(self.delay);
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
        mutations
    }

    fn plan_signing(&mut self, txs: &NodeBuffer<S>) -> Option<WorkerPlanFor<S>> {
        if self
            .nodes
            .values()
            .any(|node| node.shared().signing_inflight)
        {
            debug!("plan_signing: skip, signing already inflight");
            return None;
        }
        if !txs.has_pending() {
            debug!("plan_signing: skip, no pending transactions");
            return None;
        }
        let (node_id, max_last_submitted) = match self.max_active_last_submitted() {
            Some(result) => result,
            None => {
                debug!("plan_signing: skip, no active nodes");
                return None;
            }
        };
        let next_sequence = txs.next_sequence();
        if max_last_submitted != 0 && next_sequence != max_last_submitted + 1 {
            debug!(
                node_id = %node_id.as_ref(),
                next_sequence,
                max_last_submitted,
                "plan_signing: skip, next sequence not contiguous"
            );
            return None;
        }
        let delay = if let Some(node) = self.nodes.get_mut(&node_id).map(|node| node.shared_mut()) {
            node.signing_inflight = true;
            node.signing_delay.take().unwrap_or_default()
        } else {
            Duration::default()
        };
        debug!(
            node_id = %node_id.as_ref(),
            sequence = next_sequence,
            delay = ?delay,
            "plan_signing: spawn signing"
        );
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

fn next_after_confirmed(confirmed: Option<u64>) -> u64 {
    confirmed.map(|seq| seq.saturating_add(1)).unwrap_or(0)
}

fn stop_seq_from_last_good(last_submitted: Option<u64>, last_confirmed: Option<u64>) -> u64 {
    let last_good = match (last_submitted, last_confirmed) {
        (Some(submitted), Some(confirmed)) => Some(submitted.max(confirmed)),
        (Some(submitted), None) => Some(submitted),
        (None, Some(confirmed)) => Some(confirmed),
        (None, None) => None,
    };
    last_good.map(|seq| seq.saturating_add(1)).unwrap_or(0)
}

fn on_submit_success(state: &mut ActiveState, sequence: u64) {
    state.pending = None;
    match state.shared.last_submitted {
        None => state.shared.last_submitted = Some(sequence),
        Some(last) => {
            debug_assert!(last + 1 == sequence, "incorrect sequence number");
            if last + 1 == sequence {
                state.shared.last_submitted = Some(sequence);
            }
        }
    }
}

fn plan_submission<S: TxServer>(
    node_id: &NodeId,
    node: &mut NodeStateFor<S>,
    txs: &NodeBuffer<S>,
) -> Option<WorkerPlanFor<S>> {
    match node {
        NodeState::Active(state) => {
            if state.pending.is_some() {
                debug!(
                    node_id = %node_id.as_ref(),
                    pending = ?state.pending,
                    "plan_submission: skip active, pending already set"
                );
                return None;
            }
            let next_seq = match (state.shared.last_submitted, txs.confirmed_seq()) {
                (Some(last), Some(confirmed)) => last.max(confirmed).saturating_add(1),
                (Some(last), None) => last.saturating_add(1),
                (None, Some(confirmed)) => confirmed.saturating_add(1),
                (None, None) => 0,
            };
            let tx = match txs.get(next_seq) {
                Some(tx) => tx,
                None => {
                    debug!(
                        node_id = %node_id.as_ref(),
                        next_seq,
                        confirmed_seq = ?txs.confirmed_seq(),
                        max_seq = ?txs.max_seq(),
                        "plan_submission: skip active, no tx at sequence"
                    );
                    return None;
                }
            };
            let delay = state.shared.submit_delay.take().unwrap_or_default();
            state.pending = Some(next_seq);
            debug!(
                node_id = %node_id.as_ref(),
                sequence = tx.sequence,
                delay = ?delay,
                last_submitted = ?state.shared.last_submitted,
                last_confirmed = ?state.shared.last_confirmed,
                "plan_submission: spawn submit"
            );
            Some(WorkerPlan::SpawnSubmit {
                node_id: node_id.clone(),
                bytes: tx.bytes.clone(),
                sequence: tx.sequence,
                delay,
            })
        }
        NodeState::Recovering(_) | NodeState::Stopped(_) => {
            debug!(
                node_id = %node_id.as_ref(),
                state = %node.log_state(),
                "plan_submission: skip, node not active"
            );
            None
        }
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
            let start = next_after_confirmed(state.shared.last_confirmed);
            let ids = match txs.ids_from_to(start, last, max_batch) {
                Some(ids) => ids,
                None => {
                    debug!(
                        node_id = %node_id.as_ref(),
                        start,
                        stop = last,
                        confirmed_seq = ?txs.confirmed_seq(),
                        max_seq = ?txs.max_seq(),
                        "plan_confirmation: skip active, no ids in range"
                    );
                    return None;
                }
            };
            if ids.is_empty() {
                debug!(
                    node_id = %node_id.as_ref(),
                    start,
                    stop = last,
                    "plan_confirmation: skip active, empty ids"
                );
                return None;
            }
            let epoch = state.shared.epoch;
            state.shared.confirm_inflight = true;
            debug!(
                node_id = %node_id.as_ref(),
                start,
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
            if state.stop_seq == 0 {
                debug!(
                    node_id = %node_id.as_ref(),
                    "plan_confirmation: skip stopped, stop_seq=0"
                );
                return None;
            }
            let confirm_upto = last.min(state.stop_seq.saturating_sub(1));
            let start = next_after_confirmed(state.shared.last_confirmed);
            let ids = match txs.ids_from_to(start, confirm_upto, max_batch) {
                Some(ids) => ids,
                None => {
                    debug!(
                        node_id = %node_id.as_ref(),
                        start,
                        stop = confirm_upto,
                        confirmed_seq = ?txs.confirmed_seq(),
                        max_seq = ?txs.max_seq(),
                        "plan_confirmation: skip stopped, no ids in range"
                    );
                    return None;
                }
            };
            if ids.is_empty() {
                debug!(
                    node_id = %node_id.as_ref(),
                    start,
                    stop = confirm_upto,
                    "plan_confirmation: skip stopped, empty ids"
                );
                return None;
            }
            let epoch = state.shared.epoch;
            state.shared.confirm_inflight = true;
            debug!(
                node_id = %node_id.as_ref(),
                start,
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
    delay: Option<Duration>,
) {
    let mut recover_target = None;
    let mut stop_seq = None;
    let shared = match node {
        NodeState::Active(state) => &mut state.shared,
        NodeState::Stopped(state) => &mut state.shared,
        NodeState::Recovering(_) => {
            return;
        }
    };
    if matches!(txs.confirmed_seq(), Some(confirmed) if confirmed >= expected) {
        return;
    }
    // if our sequence is less than expected, we need to confirm
    // that the transaction with higher sequence nubmer is transaction
    // made by our client and not from outside the system
    if sequence < expected {
        let target = expected.saturating_sub(1);
        let Some(tx) = txs.get(target) else {
            let stop_seq = stop_seq_from_last_good(shared.last_submitted, shared.last_confirmed);
            node.apply_stop_error(stop_seq, stop_error);
            return;
        };
        if tx.id.is_none() {
            if let Some(value) = delay {
                node.shared_mut().submit_delay = Some(value);
                return;
            }
            let stop_seq = stop_seq_from_last_good(shared.last_submitted, shared.last_confirmed);
            node.apply_stop_error(stop_seq, stop_error);
            return;
        }
        shared.epoch = shared.epoch.saturating_add(1);
        recover_target = Some(target);
    } else if txs
        .max_seq()
        .map(|max_seq| max_seq < expected.saturating_sub(1))
        .unwrap_or(true)
    {
        let has_id = txs.get(sequence).and_then(|tx| tx.id.clone()).is_some();
        let stop_at = if has_id {
            sequence
        } else {
            stop_seq_from_last_good(shared.last_submitted, shared.last_confirmed)
        };
        stop_seq = Some(stop_at);
    } else {
        let expected_prev = expected.saturating_sub(1);
        let clamped = match txs.confirmed_seq() {
            Some(confirmed) => expected_prev.max(confirmed),
            None => expected_prev,
        };
        shared.last_submitted = Some(clamped);
        return;
    }
    if let Some(target) = recover_target {
        node.transition_to_recovering(target);
        return;
    }
    if let Some(stop_seq) = stop_seq {
        node.apply_stop_error(stop_seq, stop_error);
    }
}

fn process_status_batch<S: TxServer>(
    node_id: &NodeId,
    node: &mut NodeStateFor<S>,
    statuses: StatusesFor<S>,
    txs: &NodeBuffer<S>,
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
        let offset = next_after_confirmed(shared.last_confirmed);
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
                    let seq_minus_one = sequence.checked_sub(1);
                    let clamped = match txs.confirmed_seq() {
                        Some(confirmed) => Some(seq_minus_one.unwrap_or(0).max(confirmed)),
                        None => seq_minus_one,
                    };
                    shared.last_submitted = clamped;
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
                    shared.last_confirmed = Some(sequence);
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
        on_sequence_mismatch::<S>(node, sequence, expected, stop_error, txs, None);
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
    let mut active = false;
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
                active = true;
            }
            TxStatusKind::Evicted => {
                state.shared.epoch = state.shared.epoch.saturating_add(1);
                active = true;
            }
            other => {
                fatal = Some(TxStatus::new(other, original_response));
            }
        }
    }
    if active {
        node.transition_to_active();
    }
    if let Some(status) = fatal {
        node.apply_stop_error(seq, StopError::ConfirmError(status));
    }
    effects
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::sync::Arc;

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
        let mut txs = TestBuffer::new(Some(0));
        txs.add_transaction(make_tx(1, None)).unwrap();
        txs.add_transaction(make_tx(2, None)).unwrap();
        txs.set_submitted_id(1, 10);
        txs.set_submitted_id(2, 11);
        txs
    }

    #[test]
    fn wakeup_submission_spawns_submit() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(5));
        let mut txs = TestBuffer::new(Some(0));
        txs.add_transaction(make_tx(1, None)).unwrap();

        let plans = manager.plan(&txs, 16, false);

        assert!(plans.iter().any(|event| {
            matches!(
                event,
                WorkerPlan::SpawnSubmit { node_id: id, sequence, .. }
                    if id == &node_id && *sequence == 1
            )
        }));
    }

    #[test]
    fn wakeup_submission_spawns_submit_from_zero() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], None);
        let mut txs = TestBuffer::new(None);
        txs.add_transaction(make_tx(0, None)).unwrap();

        let plans = manager.plan(&txs, 16, false);

        assert!(plans.iter().any(|event| {
            matches!(
                event,
                WorkerPlan::SpawnSubmit { node_id: id, sequence, .. }
                    if id == &node_id && *sequence == 0
            )
        }));
    }

    #[test]
    fn submission_success_marks_and_wakes() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(0));
        let txs = TestBuffer::new(Some(0));

        let _outcome = manager.apply_event(
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

        assert!(outcome.iter().any(|event| {
            matches!(
                event,
                WorkerMutation::MarkSubmitted { sequence, id } if *sequence == 1 && *id == 10
            )
        }));
    }

    #[test]
    fn confirmation_batch_skips_gap() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(0));
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
                .iter()
                .any(|event| matches!(event, WorkerMutation::Confirm { .. }))
        );
    }

    #[test]
    fn confirmation_batch_confirms_contiguous() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(0));
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
        for event in outcome {
            if let WorkerMutation::Confirm { seq, info } = event {
                confirmed.push((seq, info));
            }
        }
        assert_eq!(confirmed, vec![(1, 1), (2, 2)]);
    }

    #[test]
    fn sequence_mismatch_triggers_recover_spawn() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(0));
        let mut txs = TestBuffer::new(Some(0));
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

        let wake = manager.plan(&txs, 16, true);

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
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(0));
        let mut txs = TestBuffer::new(Some(0));
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
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(0));
        let mut txs = TestBuffer::new(Some(0));
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
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id], Some(0));
        let txs = TestBuffer::new(Some(0));

        let outcome = manager.apply_event(NodeEvent::NodeStop, &txs, Duration::from_secs(1));

        assert!(
            outcome
                .iter()
                .any(|event| matches!(event, WorkerMutation::WorkerStop))
        );
    }

    #[test]
    fn signing_success_enqueues_and_requests_submission() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(0));
        let txs = TestBuffer::new(Some(0));

        if let NodeState::Active(state) = manager.nodes.get_mut(&node_id).unwrap() {
            state.shared.signing_inflight = true;
        }

        let _outcome = manager.apply_event(
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

        assert!(outcome.iter().any(
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
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(0));
        let txs = TestBuffer::new(Some(0));

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

        assert!(manager.signing_delay.is_some());
        assert!(matches!(
            manager.nodes.get(&node_id).unwrap(),
            NodeState::Active(_)
        ));
    }

    #[test]
    fn confirmation_epoch_mismatch_resets_inflight_and_requests_confirmation() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(0));
        let txs = TestBuffer::new(Some(0));

        if let NodeState::Active(state) = manager.nodes.get_mut(&node_id).unwrap() {
            state.shared.confirm_inflight = true;
        }

        let _outcome = manager.apply_event(
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

        assert!(outcome.is_empty());
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
        let mut manager =
            NodeManager::<DummyServer>::new(vec![node_a.clone(), node_b.clone()], Some(0));
        let txs = TestBuffer::new(Some(0));

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

        let NodeState::Active(state) = manager.nodes.get(&node_a).unwrap() else {
            panic!("node_a should be active");
        };
        assert_eq!(state.shared.last_submitted, Some(6));
        assert_eq!(state.shared.submit_delay, None);
    }

    #[test]
    fn confirmation_pending_keeps_last_confirmed() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(0));
        let mut txs = TestBuffer::new(Some(5));

        txs.add_transaction(make_tx(6, None)).unwrap();
        txs.add_transaction(make_tx(7, None)).unwrap();
        txs.set_submitted_id(6, 10);
        txs.set_submitted_id(7, 11);

        if let NodeState::Active(state) = manager.nodes.get_mut(&node_id).unwrap() {
            state.shared.last_confirmed = Some(5);
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
        assert_eq!(state.shared.last_confirmed, Some(5));
    }

    #[test]
    fn stop_submissions_without_history_sets_zero_stop_seq() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], None);
        let txs = TestBuffer::new(None);

        let _ = manager.apply_event(NodeEvent::NodeStop, &txs, Duration::from_secs(1));

        let NodeState::Stopped(state) = manager.nodes.get(&node_id).unwrap() else {
            panic!("node should be stopped");
        };
        assert_eq!(state.stop_seq, 0);
    }

    #[test]
    fn stop_submissions_uses_confirmed_plus_one() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(5));
        let txs = TestBuffer::new(Some(5));

        let _ = manager.apply_event(NodeEvent::NodeStop, &txs, Duration::from_secs(1));

        let NodeState::Stopped(state) = manager.nodes.get(&node_id).unwrap() else {
            panic!("node should be stopped");
        };
        assert_eq!(state.stop_seq, 6);
    }

    #[test]
    fn stop_submissions_uses_max_last_good_plus_one() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(5));
        let txs = TestBuffer::new(Some(5));

        if let NodeState::Active(state) = manager.nodes.get_mut(&node_id).unwrap() {
            state.shared.last_submitted = Some(7);
        }

        let _ = manager.apply_event(NodeEvent::NodeStop, &txs, Duration::from_secs(1));

        let NodeState::Stopped(state) = manager.nodes.get(&node_id).unwrap() else {
            panic!("node should be stopped");
        };
        assert_eq!(state.stop_seq, 8);
    }

    #[test]
    fn apply_stop_error_updates_to_earlier_sequence() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(0));

        let node = manager.nodes.get_mut(&node_id).unwrap();
        node.stop_with_error(10, StopError::WorkerStopped);

        node.apply_stop_error(8, StopError::SubmitError(()));

        let NodeState::Stopped(state) = manager.nodes.get(&node_id).unwrap() else {
            panic!("node should be stopped");
        };
        assert_eq!(state.stop_seq, 8);
        assert!(matches!(state.error, StopError::SubmitError(_)));
    }

    #[test]
    fn apply_stop_error_replaces_worker_stopped_error() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(0));

        let node = manager.nodes.get_mut(&node_id).unwrap();
        node.stop_with_error(10, StopError::WorkerStopped);

        node.apply_stop_error(12, StopError::SubmitError(()));

        let NodeState::Stopped(state) = manager.nodes.get(&node_id).unwrap() else {
            panic!("node should be stopped");
        };
        assert_eq!(state.stop_seq, 10);
        assert!(matches!(state.error, StopError::SubmitError(_)));
    }

    #[test]
    fn plan_confirmation_stopped_respects_stop_seq_minus_one() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(3));
        let mut txs = TestBuffer::new(Some(3));

        for seq in 4..=10 {
            txs.add_transaction(make_tx(seq, Some(seq * 10))).unwrap();
            txs.set_submitted_id(seq, seq * 10);
        }

        if let NodeState::Active(state) = manager.nodes.get_mut(&node_id).unwrap() {
            state.shared.last_submitted = Some(10);
        }

        let node = manager.nodes.get_mut(&node_id).unwrap();
        node.stop_with_error(6, StopError::WorkerStopped);

        let plans = manager.plan(&txs, 16, true);

        let mut planned_ids = None;
        for plan in plans {
            if let WorkerPlan::SpawnConfirmBatch {
                node_id: id, ids, ..
            } = plan
                && id == node_id
            {
                planned_ids = Some(ids);
            }
        }
        let ids = planned_ids.expect("missing confirm batch");
        assert_eq!(ids, vec![40, 50]);
    }

    #[test]
    fn plan_confirmation_stopped_skips_when_stop_seq_zero() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], None);
        let mut txs = TestBuffer::new(None);

        txs.add_transaction(make_tx(0, Some(0))).unwrap();
        txs.set_submitted_id(0, 0);

        if let NodeState::Active(state) = manager.nodes.get_mut(&node_id).unwrap() {
            state.shared.last_submitted = Some(0);
        }

        let node = manager.nodes.get_mut(&node_id).unwrap();
        node.stop_with_error(0, StopError::WorkerStopped);

        let plans = manager.plan(&txs, 16, true);
        assert!(plans.is_empty());
    }

    #[test]
    fn should_stop_waits_for_confirmed_next() {
        let node_id: NodeId = Arc::from("node-1");
        let mut manager = NodeManager::<DummyServer>::new(vec![node_id.clone()], Some(4));

        let node = manager.nodes.get_mut(&node_id).unwrap();
        if let NodeState::Active(state) = node {
            state.shared.last_submitted = Some(10);
        }
        node.stop_with_error(6, StopError::WorkerStopped);

        assert!(!manager.should_stop());

        let node = manager.nodes.get_mut(&node_id).unwrap();
        if let NodeState::Stopped(state) = node {
            state.shared.last_confirmed = Some(5);
        }
        assert!(manager.should_stop());
    }
}
