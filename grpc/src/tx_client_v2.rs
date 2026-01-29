//! Transaction manager v2: queueing, submission, confirmation, and recovery logic.
//!
//! # Overview
//! - `TransactionManager` is a front-end that signs and enqueues transactions.
//! - `TransactionWorker` is a background event loop that submits and confirms.
//! - `TxServer` abstracts the submission/confirmation backend (one or more nodes).
//!
//! The worker maintains a contiguous in-order queue keyed by sequence and resolves
//! submit/confirm callbacks as transitions are observed.
//!
//! # Notes
//! - Sequence continuity is enforced at enqueue time; any gap is treated as fatal.
//! - Confirmations are batched per node and reduced to a small set of events.
//! - Recovery runs a narrow confirmation loop for a single node when sequence
//!   mismatch is detected.
//!
//! # Example
//! ```no_run
//! # use std::collections::HashMap;
//! # use std::sync::Arc;
//! # use std::time::Duration;
//! # use celestia_grpc::{Result, TxConfig};
//! # use celestia_grpc::tx_client_v2::{
//! #     TransactionWorker, TxRequest, TxServer, SignFn,
//! # };
//! # struct DummyServer;
//! # #[async_trait::async_trait]
//! # impl TxServer for DummyServer {
//! #     type TxId = u64;
//! #     type ConfirmInfo = u64;
//! #     async fn submit(&self, _b: Vec<u8>, _s: u64) -> Result<u64, _> { unimplemented!() }
//! #     async fn status_batch(
//! #         &self,
//! #         _ids: Vec<u64>,
//! #     ) -> Result<Vec<(u64, celestia_grpc::tx_client_v2::TxStatus<u64>)>> {
//! #         unimplemented!()
//! #     }
//! #     async fn current_sequence(&self) -> Result<u64> { unimplemented!() }
//! # }
//! # struct DummySigner;
//! # #[async_trait::async_trait]
//! # impl SignFn<u64, u64> for DummySigner {
//! #     async fn sign(
//! #         &self,
//! #         _sequence: u64,
//! #         _request: &TxRequest,
//! #         _cfg: &TxConfig,
//! #     ) -> Result<celestia_grpc::tx_client_v2::Transaction<u64, u64>> {
//! #         unimplemented!()
//! #     }
//! # }
//! # async fn docs() -> Result<()> {
//! let nodes = HashMap::from([(Arc::from("node-1"), Arc::new(DummyServer))]);
//! let signer = Arc::new(DummySigner);
//! let (manager, mut worker) = TransactionWorker::new(
//!     nodes,
//!     Duration::from_secs(1),
//!     16,
//!     signer,
//!     1,
//!     128,
//! );
//! # let _ = manager;
//! # let _ = worker;
//! # Ok(())
//! # }
//! ```
use std::collections::{HashMap, VecDeque};
use std::hash::Hash as StdHash;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use celestia_types::state::ErrorCode;
use futures::future::{BoxFuture, FutureExt};
use futures::stream::{FuturesUnordered, StreamExt};
use lumina_utils::executor::spawn_cancellable;
use lumina_utils::time::{self, Interval};
use tendermint_proto::google::protobuf::Any;
use tendermint_proto::types::CanonicalVoteExtension;
use tokio::select;
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{Error, Result, TxConfig};
/// Identifier for a submission/confirmation node.
pub type NodeId = Arc<str>;
/// Result for submission calls: either a server TxId or a submission failure.
pub type TxSubmitResult<T> = Result<T, SubmitFailure>;
/// Result for confirmation calls.
pub type TxConfirmResult<T> = Result<T>;

pub trait TxIdT: Clone + std::fmt::Debug {}
impl<T> TxIdT for T where T: Clone + std::fmt::Debug {}

#[derive(Debug, Clone)]
pub enum TxRequest {
    /// Pay-for-blobs transaction.
    Blobs(Vec<celestia_types::Blob>),
    /// Arbitrary protobuf message (already encoded as `Any`).
    Message(Any),
    /// Raw bytes payload (used by tests or external signers).
    RawPayload(Vec<u8>),
}

#[derive(Debug)]
pub struct Transaction<TxId: TxIdT, ConfirmInfo> {
    /// Transaction sequence for the signer.
    pub sequence: u64,
    /// Signed transaction bytes ready for broadcast.
    pub bytes: Vec<u8>,
    /// One-shot callbacks for submit/confirm acknowledgements.
    pub callbacks: TxCallbacks<TxId, ConfirmInfo>,
    /// Id of the transaction
    pub id: Option<TxId>,
}

#[derive(Debug)]
pub struct TxCallbacks<TxId: TxIdT, ConfirmInfo> {
    /// Resolves when submission succeeds or fails.
    pub submitted: Option<oneshot::Sender<Result<TxId>>>,
    /// Resolves when the transaction is confirmed or rejected.
    pub confirmed: Option<oneshot::Sender<Result<TxStatus<ConfirmInfo>>>>,
}

impl<TxId: TxIdT, ConfirmInfo> Default for TxCallbacks<TxId, ConfirmInfo> {
    fn default() -> Self {
        Self {
            submitted: None,
            confirmed: None,
        }
    }
}

#[derive(Debug)]
pub struct TxHandle<TxId: TxIdT, ConfirmInfo> {
    /// Sequence reserved for this transaction.
    pub sequence: u64,
    /// Receives submit result.
    pub submitted: oneshot::Receiver<Result<TxId>>,
    /// Receives confirm result.
    pub confirmed: oneshot::Receiver<Result<TxStatus<ConfirmInfo>>>,
}

#[async_trait]
pub trait SignFn<TxId: TxIdT, ConfirmInfo>: Send + Sync {
    /// Produce a signed `Transaction` for the provided request and sequence.
    ///
    /// # Notes
    /// - The returned `Transaction.sequence` must match the input `sequence`.
    /// - Returning a mismatched sequence causes the caller to fail the enqueue.
    async fn sign(
        &self,
        sequence: u64,
        request: &TxRequest,
        cfg: &TxConfig,
    ) -> Result<Transaction<TxId, ConfirmInfo>>;
}

#[derive(Clone)]
pub struct TransactionManager<TxId: TxIdT, ConfirmInfo> {
    add_tx: mpsc::Sender<Transaction<TxId, ConfirmInfo>>,
    next_sequence: Arc<Mutex<u64>>,
    max_sent: Arc<AtomicU64>,
    signer: Arc<dyn SignFn<TxId, ConfirmInfo>>,
}

impl<TxId: TxIdT, ConfirmInfo> TransactionManager<TxId, ConfirmInfo> {
    /// Sign and enqueue a transaction, returning a handle for submit/confirm.
    ///
    /// # Notes
    /// - Sequence reservation is serialized with a mutex.
    /// - If the queue is full or closed, the sequence is not incremented.
    ///
    /// # Example
    /// ```no_run
    /// # use celestia_grpc::{TxConfig, Result};
    /// # use celestia_grpc::tx_client_v2::{TransactionManager, TxRequest};
    /// # async fn docs(manager: TransactionManager<u64, u64>) -> Result<()> {
    /// let handle = manager
    ///     .add_tx(TxRequest::RawPayload(vec![1, 2, 3]), TxConfig::default())
    ///     .await?;
    /// handle.submitted.await?;
    /// handle.confirmed.await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn add_tx(
        &self,
        request: TxRequest,
        cfg: TxConfig,
    ) -> Result<TxHandle<TxId, ConfirmInfo>> {
        let mut sequence = self.next_sequence.lock().await;
        let current = *sequence;
        let mut tx = self.signer.sign(current, &request, &cfg).await?;
        if tx.sequence != current {
            return Err(Error::UnexpectedResponseType(format!(
                "tx sequence mismatch: expected {}, got {}",
                current, tx.sequence
            )));
        }
        let (submit_tx, submit_rx) = oneshot::channel();
        let (confirm_tx, confirm_rx) = oneshot::channel();
        tx.callbacks.submitted = Some(submit_tx);
        tx.callbacks.confirmed = Some(confirm_tx);
        match self.add_tx.try_send(tx) {
            Ok(()) => {
                *sequence = sequence.saturating_add(1);
                self.max_sent.fetch_add(1, Ordering::Relaxed);
                Ok(TxHandle {
                    sequence: current,
                    submitted: submit_rx,
                    confirmed: confirm_rx,
                })
            }
            Err(mpsc::error::TrySendError::Full(_)) => Err(Error::UnexpectedResponseType(
                "transaction queue full".to_string(),
            )),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(Error::UnexpectedResponseType(
                "transaction manager closed".to_string(),
            )),
        }
    }

    /// Return the maximum sequence successfully enqueued so far.
    #[allow(dead_code)]
    pub fn max_sent(&self) -> u64 {
        self.max_sent.load(Ordering::Relaxed)
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct TxIndexEntry<TxId: TxIdT> {
    node_id: NodeId,
    sequence: u64,
    id: TxId,
}

#[derive(Debug, Default)]
struct NodeSubmissionState {
    submitted_seq: u64,
    inflight: bool,
    confirm_inflight: bool,
    delay: Option<Duration>,
}

#[derive(Debug, Clone)]
pub enum TxStatus<ConfirmInfo> {
    /// Submitted, but not yet committed.
    Pending,
    /// Included in a block successfully with confirmation info.
    Confirmed { info: ConfirmInfo },
    /// Rejected by the node with a specific reason.
    Rejected { reason: RejectionReason },
    /// Removed from mempool; may need resubmission.
    Evicted,
    /// Status could not be determined.
    Unknown,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum SubmitFailure {
    /// Server expects a different sequence.
    SequenceMismatch { expected: u64 },
    /// Transaction failed validation.
    InvalidTx { error_code: ErrorCode },
    /// Account has insufficient funds.
    InsufficientFunds,
    /// Fee too low for the computed gas price.
    InsufficientFee { expected_fee: u64 },
    /// Transport or RPC error while submitting.
    NetworkError { err: Arc<Error> },
    /// Node mempool is full.
    MempoolIsFull,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ConfirmFailure {
    reason: RejectionReason,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum RejectionReason {
    SequenceMismatch {
        expected: u64,
        node_id: NodeId,
    },
    OtherReason {
        error_code: ErrorCode,
        message: String,
        node_id: NodeId,
    },
}

#[async_trait]
pub trait TxServer: Send + Sync {
    type TxId: TxIdT + Eq + StdHash + Send + Sync + 'static;
    type ConfirmInfo: Clone + Send + Sync + 'static;

    /// Submit signed bytes with the given sequence, returning a server TxId.
    async fn submit(&self, tx_bytes: Vec<u8>, sequence: u64) -> TxSubmitResult<Self::TxId>;
    /// Batch status lookup for submitted TxIds.
    async fn status_batch(
        &self,
        ids: Vec<Self::TxId>,
    ) -> TxConfirmResult<Vec<(Self::TxId, TxStatus<Self::ConfirmInfo>)>>;
    /// Status lookup for submitted TxId.
    async fn status(&self, id: Self::TxId) -> TxConfirmResult<TxStatus<Self::ConfirmInfo>>;
    /// Fetch current sequence for the account (used by some implementations).
    async fn current_sequence(&self) -> Result<u64>;
}

#[derive(Debug)]
enum TransactionEvent<TxId, ConfirmInfo> {
    Submitted {
        node_id: NodeId,
        sequence: u64,
        id: TxId,
    },
    SubmitFailed {
        node_id: NodeId,
        sequence: u64,
        failure: SubmitFailure,
    },
    SubmitStatusBatch {
        node_id: NodeId,
        statuses: Vec<(TxId, TxStatus<ConfirmInfo>)>,
    },
    StopStatusBatch {
        node_id: NodeId,
        statuses: Vec<(TxId, TxStatus<ConfirmInfo>)>,
    },
    RecoverStatus {
        node_id: NodeId,
        id: TxId,
        status: TxStatus<ConfirmInfo>,
    },
}

#[derive(Debug, Clone)]
enum ProcessorState {
    Recovering { node_id: NodeId, expected: u64 },
    Submitting,
    Stopping(StopReason),
    Stopped(StopReason),
}

impl ProcessorState {
    fn transition(self, other: ProcessorState) -> ProcessorState {
        match (self, other) {
            (stopped @ ProcessorState::Stopped(_), _) => stopped,
            (ProcessorState::Stopping(_), stopped @ ProcessorState::Stopped(_)) => stopped,
            (ProcessorState::Recovering { .. }, stopped @ ProcessorState::Stopped(_)) => stopped,
            (ProcessorState::Recovering { .. }, stopping @ ProcessorState::Stopping(_)) => stopping,
            (recovering @ ProcessorState::Recovering { .. }, _) => recovering,
            (_, other) => other,
        }
    }

    fn is_stopped(&self) -> bool {
        matches!(self, ProcessorState::Stopped(_))
    }

    fn equivalent(&self, other: &ProcessorState) -> bool {
        matches!(
            (self, other),
            (ProcessorState::Submitting, ProcessorState::Submitting)
                | (
                    ProcessorState::Recovering { .. },
                    ProcessorState::Recovering { .. }
                )
                | (ProcessorState::Stopping(_), ProcessorState::Stopping(_))
                | (ProcessorState::Stopped(_), ProcessorState::Stopped(_))
        )
    }
}

#[derive(Debug, Clone)]
struct ProcessorStateWithEpoch {
    state: ProcessorState,
    epoch: u64,
}

impl ProcessorStateWithEpoch {
    fn new(state: ProcessorState) -> Self {
        Self { state, epoch: 0 }
    }

    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn snapshot(&self) -> ProcessorState {
        self.state.clone()
    }

    fn update(&mut self, other: ProcessorState) {
        let current = std::mem::replace(&mut self.state, ProcessorState::Submitting);
        let next = current.transition(other);
        self.set_if_changed(next);
    }

    fn force_update(&mut self, other: ProcessorState) {
        self.set_if_changed(other);
    }

    fn set_if_changed(&mut self, next: ProcessorState) {
        if !self.state.equivalent(&next) {
            println!(
                "[STATE] Transitioning from {:?} to {:?} (epoch {} -> {})",
                self.state,
                next,
                self.epoch,
                self.epoch.saturating_add(1)
            );
            self.state = next;
            self.epoch = self.epoch.saturating_add(1);
        }
    }

    fn is_stopped(&self) -> bool {
        self.state.is_stopped()
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum StopReason {
    SubmitFailure(SubmitFailure),
    ConfirmFailure(ConfirmFailure),
    Shutdown,
}

impl From<SubmitFailure> for StopReason {
    fn from(value: SubmitFailure) -> Self {
        StopReason::SubmitFailure(value)
    }
}

impl From<ConfirmFailure> for StopReason {
    fn from(value: ConfirmFailure) -> Self {
        StopReason::ConfirmFailure(value)
    }
}

fn recovery_expected(reason: &RejectionReason) -> Option<u64> {
    match reason {
        RejectionReason::SequenceMismatch { expected, .. } => Some(*expected),
        _ => None,
    }
}

struct SubmissionResult<TxId> {
    node_id: NodeId,
    sequence: u64,
    result: TxSubmitResult<TxId>,
}

enum ConfirmationResponse<TxId, ConfirmInfo> {
    Recovering {
        id: TxId,
        status: TxStatus<ConfirmInfo>,
    },
    Submitting(Vec<(TxId, TxStatus<ConfirmInfo>)>),
}

struct ConfirmationResult<TxId, ConfirmInfo> {
    node_id: NodeId,
    state_epoch: u64,
    response: TxConfirmResult<ConfirmationResponse<TxId, ConfirmInfo>>,
}

pub struct TransactionWorker<S: TxServer> {
    nodes: HashMap<NodeId, Arc<S>>,
    txs: VecDeque<Transaction<S::TxId, S::ConfirmInfo>>,
    tx_index: HashMap<S::TxId, u64>,
    events: VecDeque<TransactionEvent<S::TxId, S::ConfirmInfo>>,
    confirmed_sequence: u64,
    next_enqueue_sequence: u64,
    node_state: HashMap<NodeId, NodeSubmissionState>,
    add_tx_rx: mpsc::Receiver<Transaction<S::TxId, S::ConfirmInfo>>,
    new_submit: Arc<Notify>,
    submissions: FuturesUnordered<BoxFuture<'static, Option<SubmissionResult<S::TxId>>>>,
    confirmations:
        FuturesUnordered<BoxFuture<'static, Option<ConfirmationResult<S::TxId, S::ConfirmInfo>>>>,
    confirm_ticker: Interval,
    confirm_interval: Duration,
    max_status_batch: usize,
    state: ProcessorStateWithEpoch,
}

impl<S: TxServer + 'static> TransactionWorker<S> {
    /// Create a manager/worker pair with initial sequence and queue capacity.
    ///
    /// # Notes
    /// - `start_sequence` should be the next sequence to submit for the signer.
    /// - `confirm_interval` drives periodic confirmation polling.
    pub fn new(
        nodes: HashMap<NodeId, Arc<S>>,
        confirm_interval: Duration,
        max_status_batch: usize,
        signer: Arc<dyn SignFn<S::TxId, S::ConfirmInfo>>,
        start_sequence: u64,
        add_tx_capacity: usize,
    ) -> (TransactionManager<S::TxId, S::ConfirmInfo>, Self) {
        let (add_tx_tx, add_tx_rx) = mpsc::channel(add_tx_capacity);
        let next_sequence = Arc::new(Mutex::new(start_sequence));
        let max_sent = Arc::new(AtomicU64::new(0));
        let manager = TransactionManager {
            add_tx: add_tx_tx,
            next_sequence,
            max_sent,
            signer,
        };
        let node_state = nodes
            .keys()
            .map(|node_id| (node_id.clone(), NodeSubmissionState::default()))
            .collect();
        (
            manager,
            TransactionWorker {
                add_tx_rx,
                nodes,
                txs: VecDeque::new(),
                tx_index: HashMap::new(),
                events: VecDeque::new(),
                confirmed_sequence: start_sequence.saturating_sub(1),
                next_enqueue_sequence: start_sequence,
                node_state,
                new_submit: Arc::new(Notify::new()),
                submissions: FuturesUnordered::new(),
                confirmations: FuturesUnordered::new(),
                confirm_ticker: Interval::new(confirm_interval),
                state: ProcessorStateWithEpoch::new(ProcessorState::Submitting),
                confirm_interval,
                max_status_batch,
            },
        )
    }

    fn enqueue_tx(&mut self, tx: Transaction<S::TxId, S::ConfirmInfo>) -> Result<()> {
        if tx.sequence != self.next_enqueue_sequence {
            return Err(crate::Error::UnexpectedResponseType(format!(
                "tx sequence gap: expected {}, got {}",
                self.next_enqueue_sequence, tx.sequence
            )));
        }
        println!(
            "[ENQUEUE] Transaction enqueued with sequence {} (queue size: {})",
            tx.sequence,
            self.txs.len() + 1
        );
        self.txs.push_back(tx);
        self.next_enqueue_sequence += 1;
        self.new_submit.notify_one();
        Ok(())
    }

    /// Run the worker loop until shutdown or a terminal error.
    ///
    /// # Notes
    /// - The worker is single-owner; it should be run in a dedicated task.
    /// - On terminal failures it returns `Ok(())` after notifying callbacks.
    pub async fn process(&mut self, shutdown: CancellationToken) -> Result<()> {
        let submit_shutdown = shutdown.clone();
        let confirm_shutdown = shutdown.clone();
        // pushing pending futures to avoid busy loop
        self.submissions.push(
            async move {
                submit_shutdown.cancelled().await;
                None
            }
            .boxed(),
        );
        self.confirmations.push(
            async move {
                confirm_shutdown.cancelled().await;
                None
            }
            .boxed(),
        );

        loop {
            while let Some(event) = self.events.pop_front() {
                self.process_event(event).await?;
            }
            select! {
                _ = shutdown.cancelled() => self.state.update(ProcessorState::Stopped(StopReason::Shutdown)),
                res = async {
                    match self.state.snapshot() {
                        ProcessorState::Recovering { node_id, expected } => {
                            self.run_recovering(shutdown.clone(), &node_id, expected).await?;
                        }
                        ProcessorState::Submitting => {
                            self.run_submitting(shutdown.clone()).await?;
                        }
                        ProcessorState::Stopping(_) => {
                            self.run_stopping(shutdown.clone()).await?;
                        }
                        ProcessorState::Stopped(_) => (),
                    }
                    Ok::<_, Error>(())
                } => res?,
            }
            if self.state.is_stopped() {
                break;
            }
        }

        shutdown.cancel();
        // Wait for all in-flight tasks to complete before returning
        self.drain_pending().await;

        Ok(())
    }

    /// Wait for all in-flight submissions and confirmations to complete.
    ///
    /// Call this before dropping the worker if you need to ensure all spawned
    /// tasks have finished. Note: this will block until all pending network
    /// requests complete, so ensure your servers are responsive.
    pub async fn drain_pending(&mut self) {
        while self.submissions.next().await.is_some() {}
        while self.confirmations.next().await.is_some() {}
    }

    async fn run_recovering(
        &mut self,
        token: CancellationToken,
        node_id: &NodeId,
        expected: u64,
    ) -> Result<()> {
        select! {
            _ = self.confirm_ticker.tick() => {
                let target = expected.saturating_sub(1);
                let result = self
                    .index_for_sequence(target)
                    .and_then(|idx| {
                        self.txs[idx].id.clone()
                    });
                if let Some(id) = result {
                    let node_id = node_id.clone();
                    let node = self.nodes.get(&node_id).unwrap().clone();
                    let state_epoch = self.state.epoch();
                    let tx = push_oneshot(&mut self.confirmations);
                    spawn_cancellable(token, async move {
                        let response = node.status(id.clone()).await.map(|status| {
                            ConfirmationResponse::Recovering {
                                id,
                                status,
                            }
                        });
                        let _ = tx.send(ConfirmationResult {
                            node_id,
                            response,
                            state_epoch,
                        });
                    });
                }
            }
            result = self.confirmations.next() => {
                let Some(Some(result)) = result else {
                    return Ok(());
                };
                if result.state_epoch != self.state.epoch() {
                    return Ok(());
                }
                match result.response {
                    Ok(ConfirmationResponse::Recovering { id, status }) => {
                        self.events.push_back(TransactionEvent::RecoverStatus { node_id: result.node_id, id, status });
                    }
                    Err(_err) => {
                        // TODO: add error handling
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    async fn run_submitting(&mut self, token: CancellationToken) -> Result<()> {
        select! {
            tx = self.add_tx_rx.recv() => {
                if let Some(tx) = tx {
                    self.enqueue_tx(tx)?;
                }
            }
            _ = self.new_submit.notified() => {
                self.spawn_submissions(token.clone());
            }
            _ = self.confirm_ticker.tick() => {
                self.spawn_confirmations(token.clone());
            }
            result = self.submissions.next() => {
                if let Some(Some(result)) = result {
                    self.events.push_back(match result.result {
                        Ok(id) => TransactionEvent::Submitted {
                            node_id: result.node_id,
                            sequence: result.sequence,
                            id,
                        },
                        Err(failure) => TransactionEvent::SubmitFailed {
                            node_id: result.node_id,
                            sequence: result.sequence,
                            failure,
                        },
                    });
                }
            }
            result = self.confirmations.next() => {
                let Some(Some(result)) = result else {
                    return Ok(());
                };
                if result.state_epoch != self.state.epoch() {
                    return Ok(());
                }
                match result.response {
                    Ok(ConfirmationResponse::Submitting(statuses)) => {
                        self.events.push_back(TransactionEvent::SubmitStatusBatch {
                            node_id: result.node_id,
                            statuses,
                        });
                    }
                    Err(_err) => {
                        // TODO: add error handling
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    async fn run_stopping(&mut self, token: CancellationToken) -> Result<()> {
        select! {
            _ = self.confirm_ticker.tick() => {
                self.spawn_confirmations(token.clone());
            }
            result = self.confirmations.next() => {
                let Some(Some(result)) = result else {
                    return Ok(());
                };
                if result.state_epoch != self.state.epoch() {
                    return Ok(());
                }
                match result.response {
                    Ok(ConfirmationResponse::Submitting(statuses)) => {
                        self.events.push_back(TransactionEvent::StopStatusBatch {
                            node_id: result.node_id,
                            statuses,
                        });
                    }
                    Err(_err) => {
                        // TODO: add error handling
                    }
                    _ => {}
                }
            }
        }
        let mut max_sub_seq = 0;
        for (node, state) in self.node_state.iter() {
            max_sub_seq = state.submitted_seq.max(state.submitted_seq);
        }
        if max_sub_seq == self.confirmed_sequence {
            let reason = match &self.state.state {
                ProcessorState::Stopping(reason) => reason.clone(),
                _ => unreachable!(),
            };
            self.state.update(ProcessorState::Stopped(reason));
        }
        Ok(())
    }

    // either confirmation or submission or
    async fn process_event(
        &mut self,
        event: TransactionEvent<S::TxId, S::ConfirmInfo>,
    ) -> Result<()> {
        match event {
            TransactionEvent::SubmitFailed {
                node_id,
                sequence,
                failure,
                ..
            } => {
                println!(
                    "[EVENT] SubmitFailed: node={}, sequence={}, failure={:?}",
                    node_id, sequence, failure
                );
                let Some(state) = self.node_state.get_mut(&node_id) else {
                    return Ok(());
                };
                state.inflight = false;
                match failure {
                    SubmitFailure::SequenceMismatch { expected } => {
                        if sequence < expected {
                            self.state
                                .update(ProcessorState::Recovering { node_id, expected });
                        } else {
                            if let Some(state) = self.node_state.get_mut(&node_id) {
                                state.submitted_seq = expected.saturating_sub(1);
                            }
                            self.state.update(ProcessorState::Submitting);
                        }
                    }
                    SubmitFailure::MempoolIsFull => {
                        state.delay = Some(self.confirm_interval);
                    }
                    SubmitFailure::NetworkError { err: _ } => {
                        state.delay = Some(self.confirm_interval);
                    }
                    _ => {
                        self.state.update(ProcessorState::Stopping(failure.into()));
                    }
                };
                self.new_submit.notify_one();
            }
            TransactionEvent::Submitted {
                node_id,
                sequence,
                id,
            } => {
                println!(
                    "[EVENT] Submitted: node={}, sequence={}, tx_id={:?}",
                    node_id, sequence, id
                );
                debug!(
                    %node_id,
                    sequence,
                    tx_id = ?id,
                    "Transaction submitted"
                );
                let state_node_id = node_id.clone();
                let submit_id = id.clone();
                if let Some(submitted) = self.take_submitted(sequence) {
                    let _ = submitted.send(Ok(submit_id));
                }
                if let Some(state) = self.node_state.get_mut(&state_node_id) {
                    state.inflight = false;
                    state.submitted_seq = sequence;
                }
                if let Some(idx) = self.index_for_sequence(sequence) {
                    self.txs[idx].id = Some(id.clone());
                    self.tx_index.insert(id, sequence);
                }
                self.new_submit.notify_one();
            }
            TransactionEvent::SubmitStatusBatch { node_id, statuses } => {
                println!(
                    "[EVENT] SubmitStatusBatch: node={}, statuses_count={}",
                    node_id,
                    statuses.len()
                );
                let state = self.node_state.get_mut(&node_id).unwrap();
                state.confirm_inflight = false;
                self.process_status_batch_submitting(node_id, statuses)?;
            }
            TransactionEvent::StopStatusBatch { node_id, statuses } => {
                println!(
                    "[EVENT] StopStatusBatch: node={}, statuses_count={}",
                    node_id,
                    statuses.len()
                );
                let state = self.node_state.get_mut(&node_id).unwrap();
                state.confirm_inflight = false;
                self.process_status_batch_stopping(node_id, statuses)?;
            }
            TransactionEvent::RecoverStatus {
                node_id,
                id,
                status,
            } => {
                let status_name = match &status {
                    TxStatus::Pending => "Pending",
                    TxStatus::Confirmed { .. } => "Confirmed",
                    TxStatus::Rejected { .. } => "Rejected",
                    TxStatus::Evicted => "Evicted",
                    TxStatus::Unknown => "Unknown",
                };
                println!(
                    "[EVENT] RecoverStatus: node={}, id={:?}, status={}",
                    node_id, id, status_name
                );
                let state = self.node_state.get_mut(&node_id).unwrap();
                state.confirm_inflight = false;
                self.process_status_recovering(node_id, id, status)?;
            }
        }
        Ok(())
    }

    fn process_status_batch_submitting(
        &mut self,
        node_id: NodeId,
        statuses: Vec<(S::TxId, TxStatus<S::ConfirmInfo>)>,
    ) -> Result<()> {
        let mut collected = statuses
            .into_iter()
            .filter(|(tx_id, _)| self.tx_index.get(&tx_id).is_some())
            .map(|(tx_id, status)| (self.tx_index.get(&tx_id).unwrap().clone(), status))
            .collect::<Vec<_>>();
        collected.sort_by(|first, second| first.0.cmp(&second.0));

        for (cur_seq, status) in collected.into_iter() {
            match status {
                TxStatus::Pending => {
                    continue;
                }
                TxStatus::Evicted => {
                    let node_state = self.node_state.get_mut(&node_id).unwrap();
                    node_state.submitted_seq =
                        node_state.submitted_seq.min(cur_seq.saturating_sub(1));
                    self.new_submit.notify_one();
                    break;
                }
                TxStatus::Rejected { reason } => match reason {
                    RejectionReason::SequenceMismatch {
                        expected,
                        node_id: _,
                    } => {
                        if cur_seq < expected {
                            self.state.update(ProcessorState::Recovering {
                                node_id: node_id.clone(),
                                expected,
                            });
                        } else {
                            let node_state = self.node_state.get_mut(&node_id).unwrap();
                            node_state.submitted_seq = node_state.submitted_seq.min(expected);
                        }
                        break;
                    }
                    other => {
                        self.state
                            .update(ProcessorState::Stopping(StopReason::ConfirmFailure(
                                ConfirmFailure { reason: other },
                            )));
                    }
                },
                TxStatus::Confirmed { info } => {
                    self.update_confirmed_sequence(cur_seq, info)?;
                }
                TxStatus::Unknown => {
                    self.state
                        .update(ProcessorState::Stopping(StopReason::ConfirmFailure(
                            ConfirmFailure {
                                reason: RejectionReason::OtherReason {
                                    error_code: ErrorCode::UnknownRequest,
                                    message: "transaction status unknown".to_string(),
                                    node_id: node_id.clone(),
                                },
                            },
                        )));
                    break;
                }
            }
        }
        Ok(())
    }

    fn process_status_batch_stopping(
        &mut self,
        node_id: NodeId,
        statuses: Vec<(S::TxId, TxStatus<S::ConfirmInfo>)>,
    ) -> Result<()> {
        let mut collected = statuses
            .into_iter()
            .filter(|(tx_id, _)| self.tx_index.get(&tx_id).is_some())
            .map(|(tx_id, status)| (self.tx_index.get(&tx_id).unwrap().clone(), status))
            .collect::<Vec<_>>();
        collected.sort_by(|first, second| first.0.cmp(&second.0));

        for (cur_seq, status) in collected.into_iter() {
            match status {
                TxStatus::Pending => {
                    continue;
                }
                TxStatus::Confirmed { info } => {
                    self.update_confirmed_sequence(cur_seq, info)?;
                }
                TxStatus::Evicted => {
                    self.fail_transactions_from(cur_seq, TxStatus::Evicted);
                    break;
                }
                TxStatus::Rejected { reason } => {
                    self.fail_transactions_from(cur_seq, TxStatus::Rejected { reason });
                    break;
                }
                TxStatus::Unknown => {
                    self.fail_transactions_from(cur_seq, TxStatus::Unknown);
                    break;
                }
            }
        }

        if self.txs.is_empty() {
            self.transition_to_stopped();
        }

        Ok(())
    }

    fn fail_transactions_from(&mut self, from_seq: u64, status: TxStatus<S::ConfirmInfo>) {
        let Some(start_idx) = self.index_for_sequence(from_seq) else {
            return;
        };

        let num_to_fail = self.txs.len() - start_idx;
        let status_name = match &status {
            TxStatus::Pending => "Pending",
            TxStatus::Confirmed { .. } => "Confirmed",
            TxStatus::Rejected { .. } => "Rejected",
            TxStatus::Evicted => "Evicted",
            TxStatus::Unknown => "Unknown",
        };
        println!(
            "[FAIL] Failing {} transactions from sequence {} with status {}",
            num_to_fail, from_seq, status_name
        );

        // Fail and remove transactions from start_idx to the end
        while self.txs.len() > start_idx {
            if let Some(mut tx) = self.txs.pop_back() {
                if let Some(id) = &tx.id {
                    self.tx_index.remove(id);
                }
                if let Some(on_confirm) = tx.callbacks.confirmed.take() {
                    let _ = on_confirm.send(Ok(status.clone()));
                }
            }
        }
    }

    fn transition_to_stopped(&mut self) {
        if let ProcessorState::Stopping(reason) = self.state.snapshot() {
            println!("[STOP] Transitioning to Stopped state: reason={:?}", reason);
            self.state.force_update(ProcessorState::Stopped(reason));
        }
    }

    fn process_status_recovering(
        &mut self,
        node_id: NodeId,
        id: S::TxId,
        status: TxStatus<S::ConfirmInfo>,
    ) -> Result<()> {
        let seq = self.tx_index.get(&id).unwrap();
        match status {
            TxStatus::Confirmed { info } => {
                self.node_state.get_mut(&node_id).unwrap().submitted_seq = *seq;
                self.state.force_update(ProcessorState::Submitting)
            }
            _ => {
                self.state
                    .update(ProcessorState::Stopping(StopReason::ConfirmFailure(
                        ConfirmFailure {
                            reason: RejectionReason::SequenceMismatch {
                                expected: *seq,
                                node_id,
                            },
                        },
                    )));
            }
        }
        Ok(())
    }

    fn update_confirmed_sequence(&mut self, sequence: u64, info: S::ConfirmInfo) -> Result<()> {
        if sequence <= self.confirmed_sequence {
            return Ok(());
        }
        println!(
            "[CONFIRM] Transaction confirmed: sequence={} (prev_confirmed={})",
            sequence, self.confirmed_sequence
        );
        debug!(sequence, "Transaction confirmed");
        if sequence != self.confirmed_sequence + 1 {
            panic!("updating confirmed sequence with gaps shouldn't happen");
        }
        let mut tx = self.txs.pop_front().unwrap();
        if let Some(on_confirm) = tx.callbacks.confirmed.take() {
            let _ = on_confirm.send(Ok(TxStatus::Confirmed { info }));
        }
        for (_, state) in self.node_state.iter_mut() {
            state.submitted_seq = state.submitted_seq.max(sequence);
        }
        self.confirmed_sequence = sequence;
        Ok(())
    }

    fn spawn_submissions(&mut self, token: CancellationToken) {
        if self.nodes.is_empty() {
            return;
        }
        let nodes = self
            .nodes
            .iter_mut()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();
        for (node_id, node) in nodes {
            let (target_sequence, delay) = {
                let state = self.node_state.entry(node_id.clone()).or_default();
                if state.inflight {
                    continue;
                }
                let delay = if let Some(delay) = state.delay {
                    state.delay = None;
                    delay
                } else {
                    Duration::from_nanos(0)
                };
                if state.submitted_seq < self.confirmed_sequence {
                    state.submitted_seq = self.confirmed_sequence;
                }
                (state.submitted_seq.saturating_add(1), delay)
            };
            let Some(bytes) = self.peek_submission(target_sequence) else {
                continue;
            };
            if let Some(state) = self.node_state.get_mut(&node_id) {
                state.inflight = true;
            }
            let tx = push_oneshot(&mut self.submissions);
            spawn_cancellable(token.clone(), async move {
                time::sleep(delay).await;
                let result = node.submit(bytes.clone(), target_sequence).await;
                let _ = tx.send(SubmissionResult {
                    node_id,
                    sequence: target_sequence,
                    result,
                });
            });
        }
    }

    fn spawn_confirmations(&mut self, token: CancellationToken) {
        if self.tx_index.is_empty() {
            return;
        }
        let state_epoch = self.state.epoch();
        // gathering all the sequences for each node
        for (node_id, state) in self.node_state.iter_mut() {
            if state.confirm_inflight {
                continue;
            }
            let sub_seq = state.submitted_seq;
            let node_id = node_id.clone();
            let total = sub_seq - self.confirmed_sequence;
            let cap = self.max_status_batch.min(total as usize);
            if cap == 0 {
                continue;
            }
            let mut to_send = Vec::with_capacity(cap);
            for i in 0..cap {
                to_send.push(self.txs[i].id.clone().unwrap());
            }
            let node = self.nodes.get(&node_id).unwrap().clone();
            let tx = push_oneshot(&mut self.confirmations);
            spawn_cancellable(token.clone(), async move {
                let response = node
                    .status_batch(to_send)
                    .await
                    .map(|status| ConfirmationResponse::Submitting(status));
                let _ = tx.send(ConfirmationResult {
                    state_epoch,
                    node_id,
                    response,
                });
            });
        }
    }

    fn index_for_sequence(&self, sequence: u64) -> Option<usize> {
        // checking the first not confirmed sequence
        let start = self.confirmed_sequence.saturating_add(1);
        if sequence < start {
            return None;
        }
        let offset = sequence - start;
        if offset >= self.txs.len() as u64 {
            return None;
        }
        Some(offset as usize)
    }

    fn peek_submission(&mut self, sequence: u64) -> Option<Vec<u8>> {
        let idx = self.index_for_sequence(sequence)?;
        let tx = self.txs.get_mut(idx)?;
        Some(tx.bytes.clone())
    }

    fn take_submitted(&mut self, sequence: u64) -> Option<oneshot::Sender<Result<S::TxId>>> {
        let idx = self.index_for_sequence(sequence)?;
        let tx = self.txs.get_mut(idx)?;
        tx.callbacks.submitted.take()
    }
}

fn push_oneshot<T: 'static + Send>(
    unordered: &mut FuturesUnordered<BoxFuture<'static, Option<T>>>,
) -> oneshot::Sender<T> {
    let (tx, rx) = oneshot::channel();
    unordered.push(async move { rx.await.ok() }.boxed());
    tx
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::error::TryRecvError;
    use tokio::task::JoinHandle;

    type TestTxId = u64;
    type TestConfirmInfo = ();

    #[derive(Debug)]
    struct RoutedCall {
        node_id: NodeId,
        call: ServerCall,
    }

    impl RoutedCall {
        fn new(node_id: NodeId, call: ServerCall) -> Self {
            Self { node_id, call }
        }
    }

    #[derive(Debug)]
    enum ServerCall {
        Submit {
            bytes: Vec<u8>,
            sequence: u64,
            reply: oneshot::Sender<TxSubmitResult<TestTxId>>,
        },
        StatusBatch {
            ids: Vec<TestTxId>,
            reply: oneshot::Sender<TxConfirmResult<Vec<(TestTxId, TxStatus<TestConfirmInfo>)>>>,
        },
        Status {
            id: TestTxId,
            reply: oneshot::Sender<TxConfirmResult<TxStatus<TestConfirmInfo>>>,
        },
        CurrentSequence {
            reply: oneshot::Sender<Result<u64>>,
        },
    }

    #[derive(Debug)]
    struct MockTxServer {
        node_id: NodeId,
        calls: mpsc::Sender<RoutedCall>,
    }

    impl MockTxServer {
        fn new(node_id: NodeId, calls: mpsc::Sender<RoutedCall>) -> Self {
            Self { node_id, calls }
        }
        async fn send_call(&self, call: ServerCall, msg: &str) {
            self.calls
                .send(RoutedCall::new(self.node_id.clone(), call))
                .await
                .expect(msg);
        }
    }

    fn make_many_servers(
        num_servers: usize,
    ) -> (mpsc::Receiver<RoutedCall>, Vec<(NodeId, Arc<MockTxServer>)>) {
        let mut servers = Vec::with_capacity(num_servers);
        let (calls_tx, calls_rx) = mpsc::channel(64);
        for i in 0..num_servers {
            let node_name: NodeId = Arc::from(format!("node-{}", i));
            let server = Arc::new(MockTxServer::new(node_name.clone(), calls_tx.clone()));
            servers.push((node_name, server));
        }
        (calls_rx, servers)
    }

    #[derive(Default)]
    struct TestSigner;

    #[async_trait]
    impl SignFn<TestTxId, TestConfirmInfo> for TestSigner {
        async fn sign(
            &self,
            sequence: u64,
            request: &TxRequest,
            _cfg: &TxConfig,
        ) -> Result<Transaction<TestTxId, TestConfirmInfo>> {
            let bytes = match request {
                TxRequest::RawPayload(bytes) => bytes.clone(),
                TxRequest::Message(_) | TxRequest::Blobs(_) => Vec::new(),
            };
            Ok(Transaction {
                sequence,
                bytes,
                callbacks: TxCallbacks::default(),
                id: None,
            })
        }
    }

    #[derive(Debug, Clone)]
    struct NodeState {
        mempool_sequence: u64,
        block_sequence: u64,
        accepted_ids: Vec<TestTxId>,
        is_evicted: bool,
    }

    impl NodeState {
        fn new(sequence: u64, accepted_ids: Vec<TestTxId>) -> Self {
            Self {
                mempool_sequence: sequence,
                block_sequence: sequence,
                accepted_ids,
                is_evicted: false,
            }
        }
    }

    #[derive(Debug)]
    enum ServerReturn {
        Submit(TxSubmitResult<TestTxId>),
        StatusBatch(TxConfirmResult<Vec<(TestTxId, TxStatus<TestConfirmInfo>)>>),
        Status(TxConfirmResult<TxStatus<TestConfirmInfo>>),
        CurrentSequence(Result<u64>),
    }

    impl ServerReturn {
        fn assert_submit(self) -> TxSubmitResult<TestTxId> {
            match self {
                ServerReturn::Submit(result) => result,
                _ => panic!("expected Submit"),
            }
        }

        fn assert_status_batch(
            self,
        ) -> TxConfirmResult<Vec<(TestTxId, TxStatus<TestConfirmInfo>)>> {
            match self {
                ServerReturn::StatusBatch(result) => result,
                _ => panic!("expected StatusBatch"),
            }
        }

        fn assert_status(self) -> TxConfirmResult<TxStatus<TestConfirmInfo>> {
            match self {
                ServerReturn::Status(result) => result,
                _ => panic!("expected Status"),
            }
        }

        fn assert_sequence(self) -> Result<u64> {
            match self {
                ServerReturn::CurrentSequence(result) => result,
                _ => panic!("expected CurrentSequence"),
            }
        }
    }

    type ActionFunc =
        dyn for<'a> Fn(&'a RoutedCall, &mut HashMap<NodeId, NodeState>) -> ActionResult + Send + Sync + 'static;
    type MatchFunc = dyn for<'a> Fn(&'a RoutedCall) -> bool + Send + Sync + 'static;

    struct ActionResult {
        ret: ServerReturn,
    }

    #[derive(Clone)]
    struct Match {
        name: &'static str,
        check_func: Arc<MatchFunc>,
    }

    impl Match {
        fn new<F>(name: &'static str, f: F) -> Self
        where
            F: for<'a> Fn(&'a RoutedCall) -> bool + Send + Sync + 'static,
        {
            Self {
                name,
                check_func: Arc::new(f),
            }
        }

        fn check(&self, call: &RoutedCall) -> bool {
            (self.check_func)(call)
        }
    }

    impl std::fmt::Debug for Match {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
            f.debug_struct("Match").field("name", &self.name).finish()
        }
    }

    #[derive(Clone)]
    struct Action {
        name: &'static str,
        action: Arc<ActionFunc>,
    }

    impl Action {
        fn new<F>(name: &'static str, f: F) -> Self
        where
            F: for<'a> Fn(&'a RoutedCall, &mut HashMap<NodeId, NodeState>) -> ActionResult + Send + Sync + 'static,
        {
            Self {
                name,
                action: Arc::new(f),
            }
        }
        fn call(&self, rc: &RoutedCall, state: &mut HashMap<NodeId, NodeState>) -> ActionResult {
            (self.action)(rc, state)
        }
    }

    impl std::fmt::Debug for Action {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
            f.debug_struct("Action").field("name", &self.name).finish()
        }
    }

    #[derive(Debug, Clone)]
    enum Cardinality {
        Once,
        Times(u32),
        Any,
    }

    impl Cardinality {
        fn consume(&mut self) {
            match self {
                Cardinality::Once => *self = Cardinality::Times(0),
                Cardinality::Times(n) => {
                    if *n > 0 {
                        *n -= 1;
                    }
                }
                Cardinality::Any => {}
            }
        }
        fn exhausted(&self) -> bool {
            matches!(self, Cardinality::Times(0))
        }
        fn requires_consumption(&self) -> bool {
            !matches!(self, Cardinality::Any)
        }
    }

    #[derive(Debug, Clone)]
    struct Rule {
        name: &'static str,
        matcher: Match,
        action: Action,
        card: Cardinality,
    }

    impl Rule {
        fn new(name: &'static str, matcher: Match, action: Action, card: Cardinality) -> Self {
            Self {
                name,
                matcher,
                action,
                card,
            }
        }

        fn matches(&self, call: &RoutedCall) -> bool {
            if self.card.exhausted() {
                return false;
            }
            self.matcher.check(&call)
        }

        fn consume(&mut self) {
            self.card.consume();
        }

        fn call(&self, rc: &RoutedCall, states: &mut HashMap<NodeId, NodeState>) -> ActionResult {
            self.action.call(rc, states)
        }
    }

    #[async_trait]
    impl TxServer for MockTxServer {
        type TxId = TestTxId;
        type ConfirmInfo = TestConfirmInfo;

        async fn submit(&self, bytes: Vec<u8>, sequence: u64) -> TxSubmitResult<TestTxId> {
            let (reply, rx) = oneshot::channel();
            let ret = ServerCall::Submit {
                bytes,
                sequence,
                reply,
            };
            self.send_call(ret, "submit call").await;
            rx.await.expect("submit reply")
        }

        async fn status_batch(
            &self,
            ids: Vec<TestTxId>,
        ) -> TxConfirmResult<Vec<(TestTxId, TxStatus<TestConfirmInfo>)>> {
            let (reply, rx) = oneshot::channel();
            let ret = ServerCall::StatusBatch { ids, reply };
            self.send_call(ret, "status batch call").await;
            rx.await.expect("status batch reply")
        }

        async fn status(&self, id: Self::TxId) -> TxConfirmResult<TxStatus<Self::ConfirmInfo>> {
            let (reply, rx) = oneshot::channel();
            let ret = ServerCall::Status { id, reply };
            self.send_call(ret, "status call").await;
            rx.await.expect("status reply")
        }

        async fn current_sequence(&self) -> Result<u64> {
            let (reply, rx) = oneshot::channel();
            let ret = ServerCall::CurrentSequence { reply };
            self.send_call(ret, "current sequence call").await;
            rx.await.expect("current sequence reply")
        }
    }

    #[derive(Debug, Clone)]
    struct CallLogEntry {
        node_id: NodeId,
        kind: &'static str,
        seq: Option<u64>,
        ids: Vec<TestTxId>,
    }

    impl CallLogEntry {
        fn new(node_id: NodeId, kind: &'static str, seq: Option<u64>, ids: Vec<TestTxId>) -> Self {
            Self {
                node_id,
                kind,
                seq,
                ids,
            }
        }

        fn println(&self) {
            println!(
                "logging call: {}, {:?}, {:?}",
                self.kind, self.seq, &self.ids
            );
        }
    }

    impl From<&RoutedCall> for CallLogEntry {
        fn from(rc: &RoutedCall) -> Self {
            let node_id = rc.node_id.clone();
            match &rc.call {
                ServerCall::CurrentSequence { .. } => {
                    CallLogEntry::new(node_id, "CurrentSequence", None, vec![])
                }
                ServerCall::Status { id, .. } => {
                    CallLogEntry::new(node_id, "Status", None, vec![id.clone()])
                }
                ServerCall::StatusBatch { ids, .. } => {
                    CallLogEntry::new(node_id, "StatusBatch", None, ids.clone())
                }
                ServerCall::Submit { sequence, .. } => {
                    CallLogEntry::new(node_id, "Submit", Some(sequence.clone()), vec![])
                }
            }
        }
    }

    #[derive(Debug)]
    struct Driver {
        inbox: mpsc::Receiver<RoutedCall>,
        rules: Vec<Rule>,
        states: HashMap<NodeId, NodeState>,
        log: Vec<CallLogEntry>,
    }

    impl Driver {
        fn new(
            inbox: mpsc::Receiver<RoutedCall>,
            rules: Vec<Rule>,
            states: HashMap<NodeId, NodeState>,
        ) -> Self {
            Self {
                inbox,
                rules,
                log: Vec::new(),
                states,
            }
        }

        fn log_call(&mut self, call: &RoutedCall) {
            self.log.push(call.into());
        }

        async fn process_inbox(&mut self, shutdown: CancellationToken) -> DriverResult {
            while let Some(rc) = tokio::select! {
                _ = shutdown.cancelled() => None,
                msg = self.inbox.recv() => msg,
            } {
                self.log_call(&rc);
                let res = self.rules.iter_mut().find(|rule| rule.matches(&rc));
                if let Some(rule) = res {
                    rule.consume();
                    let ret = rule.call(&rc, &mut self.states);
                    match rc.call {
                        ServerCall::Submit { reply, .. } => {
                            reply.send(ret.ret.assert_submit());
                        }
                        ServerCall::Status { reply, .. } => {
                            reply.send(ret.ret.assert_status());
                        }
                        ServerCall::StatusBatch { reply, .. } => {
                            reply.send(ret.ret.assert_status_batch());
                        }
                        ServerCall::CurrentSequence { reply, .. } => {
                            reply.send(ret.ret.assert_sequence());
                        }
                    }
                }
            }
            let mut unmet = vec![];
            for rule in self.rules.iter() {
                if rule.card.requires_consumption() && !rule.card.exhausted() {
                    unmet.push(rule.clone());
                }
            }
            DriverResult {
                unmet,
                log: self.log.clone(),
            }
        }
    }

    #[derive(Debug)]
    struct DriverResult {
        log: Vec<CallLogEntry>,
        unmet: Vec<Rule>,
    }

    struct Harness {
        shutdown: CancellationToken,
        manager_handle: Option<JoinHandle<Result<()>>>,
        driver_handle: JoinHandle<DriverResult>,
        confirm_interval: Duration,
        manager: TransactionManager<TestTxId, TestConfirmInfo>,
    }

    impl Harness {
        fn new(
            confirm_interval: Duration,
            max_status_batch: usize,
            add_tx_capacity: usize,
            num_servers: usize,
            rules: Vec<Rule>,
        ) -> (Self, TransactionWorker<MockTxServer>) {
            let (calls_rx, servers) = make_many_servers(num_servers);
            let mut node_map = HashMap::new();
            let mut node_states = HashMap::new();
            for (node_id, node_server) in servers {
                node_map.insert(node_id.clone(), node_server);
                node_states.insert(node_id.clone(), NodeState::new(0, Vec::new()));
            }
            let signer = Arc::new(TestSigner::default());
            let (manager, worker) = TransactionWorker::new(
                node_map,
                confirm_interval,
                max_status_batch,
                signer,
                1,
                add_tx_capacity,
            );
            let mut driver = Driver::new(calls_rx, rules, node_states);
            let shutdown = CancellationToken::new();
            let process_shutdown = shutdown.clone();
            let driver_handle =
                tokio::spawn(async move { driver.process_inbox(process_shutdown).await });
            (
                Self {
                    shutdown,
                    manager_handle: None,
                    driver_handle,
                    confirm_interval,
                    manager,
                },
                worker,
            )
        }

        fn start(&mut self, mut manager: TransactionWorker<MockTxServer>) {
            let shutdown = self.shutdown.clone();
            self.manager_handle =
                Some(tokio::spawn(async move { manager.process(shutdown).await }));
        }

        async fn pump(&self) {
            tokio::task::yield_now().await;
        }

        async fn stop(self) -> DriverResult {
            self.shutdown.cancel();
            let handle = self.manager_handle.unwrap();
            _ = handle.await;
            self.driver_handle.await.unwrap()
        }

        async fn add_tx(
            &self,
            bytes: Vec<u8>,
        ) -> (
            u64,
            oneshot::Receiver<Result<TestTxId>>,
            oneshot::Receiver<Result<TxStatus<TestConfirmInfo>>>,
        ) {
            let handle = self
                .manager
                .add_tx(TxRequest::RawPayload(bytes), TxConfig::default())
                .await
                .expect("add tx");
            (handle.sequence, handle.submitted, handle.confirmed)
        }
    }

    #[tokio::test]
    async fn test_it_works() {
        let submit_matcher = Match::new("match submit", |rc: &RoutedCall| match &rc.call {
            ServerCall::Submit { .. } => true,
            _ => false,
        });
        let status_matcher = Match::new("match status", |rc: &RoutedCall| match &rc.call {
            ServerCall::Status { .. } => true,
            _ => false,
        });
        let status_batch_matcher =
            Match::new("match status batch", |rc: &RoutedCall| match &rc.call {
                ServerCall::StatusBatch { .. } => true,
                _ => false,
            });
        let sequence_matcher = Match::new("match sequence", |rc: &RoutedCall| match &rc.call {
            ServerCall::CurrentSequence { .. } => true,
            _ => false,
        });
        let sequence_action = Action::new(
            "sequence_action",
            |rc: &RoutedCall, states: &mut HashMap<NodeId, NodeState>| match rc.call {
                ServerCall::CurrentSequence { .. } => {
                    let state = states.get_mut(&rc.node_id).unwrap();
                    let exp_seq = state.mempool_sequence.saturating_add(1);
                    ActionResult {
                        ret: ServerReturn::CurrentSequence(Ok(exp_seq)),
                    }
                }
                _ => panic!("unexpected call"),
            },
        );
        let status_action =
            Action::new(
                "status_action",
                |rc: &RoutedCall, states: &mut HashMap<NodeId, NodeState>| match rc.call {
                    ServerCall::Status { id, .. } => {
                        let state = states.get_mut(&rc.node_id).unwrap();
                        let status = if id <= state.mempool_sequence {
                            TxStatus::Confirmed { info: () }
                        } else {
                            TxStatus::Pending
                        };
                        ActionResult {
                            ret: ServerReturn::Status(TxConfirmResult::Ok(status)),
                        }
                    }
                    _ => panic!("unexpected call"),
                },
            );
        let status_batch_action = Action::new(
            "status_action",
            |rc: &RoutedCall, states: &mut HashMap<NodeId, NodeState>| match &rc.call {
                ServerCall::StatusBatch { ids, .. } => {
                    let state = states.get_mut(&rc.node_id).unwrap();
                    let results = ids
                        .iter()
                        .map(|it| {
                            let status = if *it <= state.mempool_sequence {
                                TxStatus::Confirmed { info: () }
                            } else {
                                TxStatus::Pending
                            };
                            (*it, status)
                        })
                        .collect::<Vec<_>>();
                    ActionResult {
                        ret: ServerReturn::StatusBatch(TxConfirmResult::Ok(results)),
                    }
                }
                _ => panic!("unexpected call"),
            },
        );
        let submit_action = Action::new(
            "status_action",
            |rc: &RoutedCall, mut states: &mut HashMap<NodeId, NodeState>| match rc.call {
                ServerCall::Submit { sequence, .. } => {
                    let state = states.get_mut(&rc.node_id).unwrap();
                    let result = if state.mempool_sequence == sequence - 1 {
                        state.mempool_sequence += 1;
                        Ok(sequence)
                    } else {
                        Err(SubmitFailure::InvalidTx {
                            error_code: ErrorCode::TxTooLarge,
                        })
                    };
                    ActionResult {
                        ret: ServerReturn::Submit(result),
                    }
                }
                _ => panic!("unexpected call"),
            },
        );
        let rules: Vec<Rule> = vec![
            Rule::new("submit", submit_matcher, submit_action, Cardinality::Any),
            Rule::new("status", status_matcher, status_action, Cardinality::Any),
            Rule::new(
                "status batch",
                status_batch_matcher,
                status_batch_action,
                Cardinality::Any,
            ),
            Rule::new(
                "sequence",
                sequence_matcher,
                sequence_action,
                Cardinality::Any,
            ),
        ];
        let (mut harness, manager_worker) =
            Harness::new(Duration::from_millis(20), 10, 1000, 2, rules);
        harness.start(manager_worker);
        let mut add_handles = VecDeque::new();
        for i in 0..100 {
            let handle = harness.add_tx(vec![0, 0]).await;
            add_handles.push_back(handle);
        }
        while let Some(handle) = add_handles.pop_front() {
            let (seq, submit, confirm) = handle;
            submit.await;
            if let Err(e) = confirm.await.unwrap() {
                println!("got error confirming at sequence {}, error: {}", seq, e);
            }
        }
        let results = harness.stop().await;
        for log in results.log {
            log.println();
        }
    }

    // #[tokio::test]
    // async fn test_eviction() {
    //     let evict_sequence = 15;
    //     let submit_matcher = Match::new("match submit", |rc: &RoutedCall| match &rc.call {
    //         ServerCall::Submit { .. } => true,
    //         _ => false,
    //     });
    //     let status_matcher = Match::new("match status", |rc: &RoutedCall| match &rc.call {
    //         ServerCall::Status { .. } => true,
    //         _ => false,
    //     });
    //     let status_batch_matcher =
    //         Match::new("match status batch", |rc: &RoutedCall| match &rc.call {
    //             ServerCall::StatusBatch { .. } => true,
    //             _ => false,
    //         });
    //     let sequence_matcher = Match::new("match sequence", |rc: &RoutedCall| match &rc.call {
    //         ServerCall::CurrentSequence { .. } => true,
    //         _ => false,
    //     });
    //     let sequence_action = Action::new(
    //         "sequence_action",
    //         |rc: &RoutedCall, state: NodeState| match rc.call {
    //             ServerCall::CurrentSequence { .. } => {
    //                 let exp_seq = state.mempool_sequence.saturating_add(1);
    //                 ActionResult {
    //                     new_state: state,
    //                     ret: ServerReturn::CurrentSequence(Ok(exp_seq)),
    //                 }
    //             }
    //             _ => panic!("unexpected call"),
    //         },
    //     );
    //     let status_action =
    //         Action::new(
    //             "status_action",
    //             |rc: &RoutedCall, state: NodeState| match rc.call {
    //                 ServerCall::Status { id, .. } => {
    //                     let status = if id <= state.mempool_sequence && state.is_evicted {
    //                         TxStatus::Confirmed { info: () }
    //                     } else {
    //                         TxStatus::Pending
    //                     };
    //                     ActionResult {
    //                         new_state: state,
    //                         ret: ServerReturn::Status(TxConfirmResult::Ok(status)),
    //                     }
    //                 }
    //                 _ => panic!("unexpected call"),
    //             },
    //         );
    //     let status_batch_action = Action::new(
    //         "status_action",
    //         |rc: &RoutedCall, state: NodeState| match &rc.call {
    //             ServerCall::StatusBatch { ids, .. } => {
    //                 let results = ids
    //                     .iter()
    //                     .map(|it| {
    //                         let status = if *it <= state.block_sequence {
    //                             TxStatus::Confirmed { info: () }
    //                         } else if *it <= state.mempool_sequence {
    //                             TxStatus::Pending
    //                         } else {
    //                             TxStatus::Evicted
    //                         };
    //                         (*it, status)
    //                     })
    //                     .collect::<Vec<_>>();
    //                 ActionResult {
    //                     new_state: state,
    //                     ret: ServerReturn::StatusBatch(TxConfirmResult::Ok(results)),
    //                 }
    //             }
    //             _ => panic!("unexpected call"),
    //         },
    //     );
    //     let submit_action = Action::new(
    //         "status_action",
    //         move |rc: &RoutedCall, mut state: NodeState| match rc.call {
    //             ServerCall::Submit { sequence, .. } => {
    //                 let result = if state.mempool_sequence == sequence - 1 {
    //                     if sequence == evict_sequence && !state.is_evicted {
    //                         state.is_evicted = true;
    //                         state.mempool_sequence = 1;
    //                         state.block_sequence = 1;
    //                         println!("evicting!");
    //                         Err(SubmitFailure::SequenceMismatch { expected: 2 })
    //                     } else if state.is_evicted {
    //                         state.block_sequence += 1;
    //                         state.mempool_sequence += 1;
    //                         Ok(sequence)
    //                     } else {
    //                         state.mempool_sequence += 1;
    //                         state.block_sequence = 1;
    //                         Ok(sequence)
    //                     }
    //                 } else {
    //                     Err(SubmitFailure::InvalidTx {
    //                         error_code: ErrorCode::TxTooLarge,
    //                     })
    //                 };
    //                 ActionResult {
    //                     new_state: state,
    //                     ret: ServerReturn::Submit(result),
    //                 }
    //             }
    //             _ => panic!("unexpected call"),
    //         },
    //     );
    //     let rules: Vec<Rule> = vec![
    //         Rule::new("submit", submit_matcher, submit_action, Cardinality::Any),
    //         Rule::new("status", status_matcher, status_action, Cardinality::Any),
    //         Rule::new(
    //             "status batch",
    //             status_batch_matcher,
    //             status_batch_action,
    //             Cardinality::Any,
    //         ),
    //         Rule::new(
    //             "sequence",
    //             sequence_matcher,
    //             sequence_action,
    //             Cardinality::Any,
    //         ),
    //     ];
    //     let (mut harness, manager_worker) =
    //         Harness::new(Duration::from_millis(1), 10, 1000, 2, rules);
    //     harness.start(manager_worker);
    //     let mut add_handles = VecDeque::new();
    //     for i in 0..100 {
    //         let handle = harness.add_tx(vec![0, 0]).await;
    //         add_handles.push_back(handle);
    //     }
    //     while let Some(handle) = add_handles.pop_front() {
    //         let (seq, submit, confirm) = handle;
    //         submit.await;
    //         match confirm.await {
    //             Err(e) => {
    //                 println!("confirm await error: {:?} with seq {}", e, seq);
    //             }
    //             Ok(res) => match res {
    //                 Ok(conf) => {
    //                     println!("confirm successful for {}", seq);
    //                 }
    //                 Err(e) => {
    //                     println!("confirm failed for {}: {}", seq, e);
    //                 }
    //             },
    //         }
    //     }
    //     let results = harness.stop().await;
    //     for log in results.log {
    //         log.println();
    //     }
    // }
    //
    // #[tokio::test]
    // async fn test_recovering() {
    //     let evict_sequence = 15;
    //     let submit_matcher = Match::new("match submit", |rc: &RoutedCall| match &rc.call {
    //         ServerCall::Submit { .. } => true,
    //         _ => false,
    //     });
    //     let status_matcher = Match::new("match status", |rc: &RoutedCall| match &rc.call {
    //         ServerCall::Status { .. } => true,
    //         _ => false,
    //     });
    //     let status_batch_matcher =
    //         Match::new("match status batch", |rc: &RoutedCall| match &rc.call {
    //             ServerCall::StatusBatch { .. } => true,
    //             _ => false,
    //         });
    //     let sequence_matcher = Match::new("match sequence", |rc: &RoutedCall| match &rc.call {
    //         ServerCall::CurrentSequence { .. } => true,
    //         _ => false,
    //     });
    //     let sequence_action = Action::new(
    //         "sequence_action",
    //         |rc: &RoutedCall, state: NodeState| match rc.call {
    //             ServerCall::CurrentSequence { .. } => {
    //                 let exp_seq = state.mempool_sequence.saturating_add(1);
    //                 ActionResult {
    //                     new_state: state,
    //                     ret: ServerReturn::CurrentSequence(Ok(exp_seq)),
    //                 }
    //             }
    //             _ => panic!("unexpected call"),
    //         },
    //     );
    //     let status_action = Action::new(
    //         "status_action",
    //         |rc: &RoutedCall, mut state: NodeState| match rc.call {
    //             ServerCall::Status { id, .. } => {
    //                 state.mempool_sequence = id;
    //                 state.block_sequence = id;
    //                 ActionResult {
    //                     new_state: state,
    //                     ret: ServerReturn::Status(TxConfirmResult::Ok(TxStatus::Confirmed {
    //                         info: (),
    //                     })),
    //                 }
    //             }
    //             _ => panic!("unexpected call"),
    //         },
    //     );
    //     let status_batch_action = Action::new(
    //         "status_action",
    //         |rc: &RoutedCall, state: NodeState| match &rc.call {
    //             ServerCall::StatusBatch { ids, .. } => {
    //                 let results = ids
    //                     .iter()
    //                     .map(|it| {
    //                         let status = if *it <= state.block_sequence {
    //                             TxStatus::Confirmed { info: () }
    //                         } else if *it <= state.mempool_sequence {
    //                             TxStatus::Pending
    //                         } else {
    //                             TxStatus::Evicted
    //                         };
    //                         (*it, status)
    //                     })
    //                     .collect::<Vec<_>>();
    //                 ActionResult {
    //                     new_state: state,
    //                     ret: ServerReturn::StatusBatch(TxConfirmResult::Ok(results)),
    //                 }
    //             }
    //             _ => panic!("unexpected call"),
    //         },
    //     );
    //     let submit_action = Action::new(
    //         "status_action",
    //         move |rc: &RoutedCall, mut state: NodeState| match rc.call {
    //             ServerCall::Submit { sequence, .. } => {
    //                 let result = if state.mempool_sequence == sequence - 1 {
    //                     if sequence == evict_sequence && !state.is_evicted {
    //                         state.is_evicted = true;
    //                         state.mempool_sequence = 1;
    //                         state.block_sequence = 1;
    //                         println!("evicting!");
    //                         Err(SubmitFailure::SequenceMismatch { expected: 2 })
    //                     } else if state.is_evicted {
    //                         if state.mempool_sequence == 2 {
    //                             Err(SubmitFailure::SequenceMismatch {
    //                                 expected: evict_sequence,
    //                             })
    //                         } else {
    //                             state.block_sequence += 1;
    //                             state.mempool_sequence += 1;
    //                             Ok(sequence)
    //                         }
    //                     } else {
    //                         state.mempool_sequence += 1;
    //                         state.block_sequence = 1;
    //                         Ok(sequence)
    //                     }
    //                 } else {
    //                     Err(SubmitFailure::InvalidTx {
    //                         error_code: ErrorCode::TxTooLarge,
    //                     })
    //                 };
    //                 ActionResult {
    //                     new_state: state,
    //                     ret: ServerReturn::Submit(result),
    //                 }
    //             }
    //             _ => panic!("unexpected call"),
    //         },
    //     );
    //     let rules: Vec<Rule> = vec![
    //         Rule::new("submit", submit_matcher, submit_action, Cardinality::Any),
    //         Rule::new("status", status_matcher, status_action, Cardinality::Any),
    //         Rule::new(
    //             "status batch",
    //             status_batch_matcher,
    //             status_batch_action,
    //             Cardinality::Any,
    //         ),
    //         Rule::new(
    //             "sequence",
    //             sequence_matcher,
    //             sequence_action,
    //             Cardinality::Any,
    //         ),
    //     ];
    //     let (mut harness, manager_worker) =
    //         Harness::new(Duration::from_millis(1), 10, 1000, 2, rules);
    //     harness.start(manager_worker);
    //     let mut add_handles = VecDeque::new();
    //     for i in 0..100 {
    //         let handle = harness.add_tx(vec![0, 0]).await;
    //         add_handles.push_back(handle);
    //     }
    //     while let Some(handle) = add_handles.pop_front() {
    //         let (seq, submit, confirm) = handle;
    //         submit.await;
    //         match confirm.await {
    //             Err(e) => {
    //                 println!("confirm await error: {:?} with seq {}", e, seq);
    //             }
    //             Ok(res) => match res {
    //                 Ok(conf) => {
    //                     println!("confirm successful for {}", seq);
    //                 }
    //                 Err(e) => {
    //                     println!("confirm failed for {}: {}", seq, e);
    //                 }
    //             },
    //         }
    //     }
    //     let results = harness.stop().await;
    //     for log in results.log {
    //         log.println();
    //     }
    // }
}
