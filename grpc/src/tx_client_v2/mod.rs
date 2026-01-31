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

mod tx_internal;

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
    pub bytes: Arc<Vec<u8>>,
    /// One-shot callbacks for submit/confirm acknowledgements.
    pub callbacks: TxCallbacks<TxId, ConfirmInfo>,
    /// Id of the transaction
    pub id: Option<TxId>,
}

impl<TxId: TxIdT, ConfirmInfo> Transaction<TxId, ConfirmInfo> {
    fn notify_submitted(&mut self, result: Result<TxId>) {
        if let Some(submitted) = self.callbacks.submitted.take() {
            let _ = submitted.send(result);
        }
    }

    fn notify_confirmed(&mut self, result: Result<TxStatus<ConfirmInfo>>) {
        if let Some(confirmed) = self.callbacks.confirmed.take() {
            let _ = confirmed.send(result);
        }
    }
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
    async fn submit(&self, tx_bytes: Arc<Vec<u8>>, sequence: u64) -> TxSubmitResult<Self::TxId>;
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
    transactions: tx_internal::TxInternal<S::TxId, S::ConfirmInfo>,
    events: VecDeque<TransactionEvent<S::TxId, S::ConfirmInfo>>,
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
        let other_nodes = nodes.keys().cloned().collect();
        let transactions = tx_internal::TxInternal::new(0, other_nodes);
        (
            manager,
            TransactionWorker {
                add_tx_rx,
                nodes,
                transactions,
                events: VecDeque::new(),
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
        let exp_seq = self.transactions.max_seq() + 1;
        let tx_sequence = tx.sequence;
        let tx_id = tx.id.clone();
        println!(
            "[ENQUEUE] Enqueueing tx: seq={}, id={:?}, expected_seq={}",
            tx_sequence, tx_id, exp_seq
        );
        self.transactions.add_transaction(tx).map_err(|_| {
            crate::Error::UnexpectedResponseType(format!(
                "tx sequence gap: expected {}, got {}",
                exp_seq, tx_sequence
            ))
        })?;
        println!(
            "[ENQUEUE] SUCCESS: sequence={}, queue_size={}, confirmed={}, max_seq={}",
            tx_sequence,
            self.transactions.len(),
            self.transactions.confirmed_seq(),
            self.transactions.max_seq()
        );
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
        println!(
            "[RECOVERING] run_recovering called: node={}, expected={}, confirmed={}, max_seq={}",
            node_id,
            expected,
            self.transactions.confirmed_seq(),
            self.transactions.max_seq()
        );
        select! {
            _ = self.confirm_ticker.tick() => {
                let target = expected.saturating_sub(1);
                println!(
                    "[RECOVERING] tick: looking for target seq={} (expected-1)",
                    target
                );
                let tx_result = self.transactions.get(target);
                let id_result = tx_result.and_then(|tx| tx.id.clone());
                println!(
                    "[RECOVERING] get({}): found_tx={}, found_id={:?}",
                    target,
                    tx_result.is_some(),
                    id_result
                );
                if let Some(id) = id_result {
                    let node_id = node_id.clone();
                    let node = self.nodes.get(&node_id).unwrap().clone();
                    let state_epoch = self.state.epoch();
                    let tx = push_oneshot(&mut self.confirmations);
                    let node_state = self.node_state.get_mut(&node_id).unwrap();
                    println!(
                        "[RECOVERING] node_state: confirm_inflight={}",
                        node_state.confirm_inflight
                    );
                    if node_state.confirm_inflight {
                        println!("[RECOVERING] skipping: confirm already inflight");
                        return Ok(());
                    }
                    node_state.confirm_inflight = true;
                    println!(
                        "[RECOVERING] spawning recover status for target={}, id={:?}",
                        target, id
                    );
                    spawn_cancellable(token, async move {
                        println!("[RECOVERING] spawned recover status {}", target);
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
                } else {
                    println!(
                        "[RECOVERING] no tx found at target={}, cannot recover",
                        target
                    );
                }
            }
            result = self.confirmations.next() => {
                println!("[RECOVERING] confirmations.next() returned");
                let Some(Some(result)) = result else {
                    println!("[RECOVERING] confirmations returned None");
                    return Ok(());
                };
                println!(
                    "[RECOVERING] confirmation result: node={}, state_epoch={}, current_epoch={}",
                    result.node_id, result.state_epoch, self.state.epoch()
                );
                if result.state_epoch != self.state.epoch() {
                    println!("[RECOVERING] stale epoch, ignoring");
                    return Ok(());
                }
                match result.response {
                    Ok(ConfirmationResponse::Recovering { id, status }) => {
                        println!("[RECOVERING] got RecoverStatus: id={:?}", id);
                        self.events.push_back(TransactionEvent::RecoverStatus { node_id: result.node_id, id, status });
                    }
                    Err(ref err) => {
                        println!("[RECOVERING] confirmation error: {:?}", err);
                        // TODO: add error handling
                    }
                    _ => {
                        println!("[RECOVERING] unexpected response type");
                    }
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
        if self.transactions.is_empty() {
            let reason = match &self.state.state {
                ProcessorState::Stopping(reason) => reason.clone(),
                _ => unreachable!(),
            };
            self.state.update(ProcessorState::Stopped(reason));
        }
        Ok(())
    }

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
                eprintln!(
                    "[EVENT] SubmitFailed: node={}, sequence={}, failure={:?}",
                    node_id, sequence, failure
                );
                println!(
                    "[EVENT] SubmitFailed context: confirmed={}, max_seq={}, len={}",
                    self.transactions.confirmed_seq(),
                    self.transactions.max_seq(),
                    self.transactions.len()
                );
                // todo: handle submit failure error
                self.transactions.submit_failure(&node_id, sequence);
                let Some(state) = self.node_state.get_mut(&node_id) else {
                    return Ok(());
                };
                state.inflight = false;
                match failure {
                    SubmitFailure::SequenceMismatch { expected } => {
                        println!(
                            "[EVENT] SequenceMismatch: sequence={}, expected={}, decision: {}",
                            sequence,
                            expected,
                            if sequence < expected { "RECOVERING" } else { "RESET" }
                        );
                        if sequence < expected {
                            self.state
                                .update(ProcessorState::Recovering { node_id, expected });
                        } else {
                            self.transactions.reset(&node_id, expected);
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
                if let Some(submitted) = self.transactions.get_mut(sequence) {
                    submitted.notify_submitted(Ok(submit_id));
                }
                // todo: handle error
                self.transactions
                    .submit_success(&state_node_id, id.clone(), sequence)
                    .unwrap();
                if let Some(state) = self.node_state.get_mut(&state_node_id) {
                    state.inflight = false;
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
            .filter_map(|(tx_id, status)| {
                self.transactions
                    .get_by_id(&tx_id)
                    .map(|tx| (tx.sequence, status))
            })
            .collect::<Vec<_>>();
        collected.sort_by(|first, second| first.0.cmp(&second.0));
        if collected.is_empty() {
            return Ok(());
        }
        let offset = self.transactions.confirmed_seq() + 1;
        for (idx, (sequence, status)) in collected.into_iter().enumerate() {
            if idx as u64 + offset != sequence {
                return Ok(());
            }
            match status {
                TxStatus::Pending => {
                    continue;
                }
                TxStatus::Evicted => {
                    self.transactions.reset(&node_id, sequence - 1);
                    self.new_submit.notify_one();
                    break;
                }
                TxStatus::Rejected { reason } => match reason {
                    RejectionReason::SequenceMismatch {
                        expected,
                        node_id: _,
                    } => {
                        if sequence < expected {
                            self.state.update(ProcessorState::Recovering {
                                node_id: node_id.clone(),
                                expected,
                            });
                        } else {
                            self.transactions.reset(&node_id, sequence - 1);
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
                    self.transactions.confirm(sequence).and_then(|mut tx| {
                        tx.notify_confirmed(Ok(TxStatus::Confirmed { info }));
                        Ok(())
                    });
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
            .filter_map(|(tx_id, status)| {
                self.transactions
                    .get_by_id(&tx_id)
                    .map(|tx| (tx.sequence, status))
            })
            .collect::<Vec<_>>();
        collected.sort_by(|first, second| first.0.cmp(&second.0));
        if collected.is_empty() {
            return Ok(());
        }
        for (sequence, status) in collected.into_iter() {
            match status {
                TxStatus::Pending => {
                    continue;
                }
                TxStatus::Confirmed { info } => {
                    self.transactions.confirm(sequence).and_then(|mut tx| {
                        tx.notify_confirmed(Ok(TxStatus::Confirmed { info }));
                        Ok(())
                    });
                }
                TxStatus::Evicted => {
                    self.fail_transactions_from(sequence, TxStatus::Evicted);
                    break;
                }
                TxStatus::Rejected { reason } => {
                    self.fail_transactions_from(sequence, TxStatus::Rejected { reason });
                    break;
                }
                TxStatus::Unknown => {
                    self.fail_transactions_from(sequence, TxStatus::Unknown);
                    break;
                }
            }
        }

        if self.transactions.is_empty() {
            self.transition_to_stopped();
        }
        Ok(())
    }

    fn process_status_recovering(
        &mut self,
        node_id: NodeId,
        id: S::TxId,
        status: TxStatus<S::ConfirmInfo>,
    ) -> Result<()> {
        println!(
            "[PROCESS_RECOVERING] node={}, id={:?}, status={:?}",
            node_id, id,
            match &status {
                TxStatus::Pending => "Pending",
                TxStatus::Confirmed { .. } => "Confirmed",
                TxStatus::Rejected { .. } => "Rejected",
                TxStatus::Evicted => "Evicted",
                TxStatus::Unknown => "Unknown",
            }
        );
        let Some(seq) = self.transactions.get_seq(&id) else {
            println!("[PROCESS_RECOVERING] id not found in id_to_seq mapping, returning early");
            return Ok(());
        };
        println!("[PROCESS_RECOVERING] found seq={} for id", seq);
        match status {
            TxStatus::Confirmed { info } => {
                println!(
                    "[PROCESS_RECOVERING] Confirmed! Resetting node {} to seq={}, transitioning to Submitting",
                    node_id, seq
                );
                self.transactions.reset(&node_id, seq);
                self.state.force_update(ProcessorState::Submitting)
            }
            _ => {
                println!(
                    "[PROCESS_RECOVERING] Not confirmed, transitioning to Stopping"
                );
                self.state
                    .update(ProcessorState::Stopping(StopReason::ConfirmFailure(
                        ConfirmFailure {
                            reason: RejectionReason::SequenceMismatch {
                                expected: seq,
                                node_id,
                            },
                        },
                    )));
            }
        }
        Ok(())
    }

    fn fail_transactions_from(&mut self, seq: u64, status: TxStatus<S::ConfirmInfo>) {
        self.transactions.truncate_right_from(seq).map(|txs| {
            txs.into_iter().for_each(|mut tx| {
                let Some(id) = tx.id.as_ref() else {
                    // TODO: add tx.notify_submitted(Err()) with error
                    return;
                };
                tx.notify_confirmed(Ok(status.clone()));
            });
        });
    }

    fn transition_to_stopped(&mut self) {
        if let ProcessorState::Stopping(reason) = self.state.snapshot() {
            println!("[STOP] Transitioning to Stopped state: reason={:?}", reason);
            self.state.force_update(ProcessorState::Stopped(reason));
        }
    }

    fn spawn_submissions(&mut self, token: CancellationToken) {
        println!(
            "[SPAWN_SUBMISSIONS] called: nodes={}, confirmed={}, max_seq={}, len={}",
            self.nodes.len(),
            self.transactions.confirmed_seq(),
            self.transactions.max_seq(),
            self.transactions.len()
        );
        if self.nodes.is_empty() {
            println!("[SPAWN_SUBMISSIONS] no nodes, returning");
            return;
        }
        let nodes = self
            .nodes
            .iter_mut()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>();
        for (node_id, node) in nodes {
            println!("[SPAWN_SUBMISSIONS] checking node={}", node_id);
            let delay = {
                let state = self.node_state.entry(node_id.clone()).or_default();
                if state.inflight {
                    println!("[SPAWN_SUBMISSIONS] node={} is inflight, skipping", node_id);
                    continue;
                }
                let delay = if let Some(delay) = state.delay {
                    state.delay = None;
                    delay
                } else {
                    Duration::from_nanos(0)
                };
                delay
            };
            println!("[SPAWN_SUBMISSIONS] calling submit_start for node={}", node_id);
            let Some((bytes, sequence)) = self
                .transactions
                .submit_start(&node_id)
                .and_then(|tx| Some((tx.bytes.clone(), tx.sequence)))
            else {
                println!("[SPAWN_SUBMISSIONS] submit_start returned None for node={}", node_id);
                continue;
            };
            println!(
                "[SPAWN_SUBMISSIONS] spawning submission: node={}, seq={}, delay={:?}",
                node_id, sequence, delay
            );
            if let Some(state) = self.node_state.get_mut(&node_id) {
                state.inflight = true;
            }
            let tx = push_oneshot(&mut self.submissions);
            spawn_cancellable(token.clone(), async move {
                time::sleep(delay).await;
                let result = node.submit(bytes.clone(), sequence).await;
                let _ = tx.send(SubmissionResult {
                    node_id,
                    sequence,
                    result,
                });
            });
        }
    }

    fn spawn_confirmations(&mut self, token: CancellationToken) {
        if self.transactions.is_empty() {
            return;
        }
        let state_epoch = self.state.epoch();
        for (node_id, state) in self.node_state.iter_mut() {
            let Some(txs) = self.transactions.to_confirm(node_id, self.max_status_batch) else {
                continue;
            };
            if state.confirm_inflight {
                continue;
            }
            let tx = push_oneshot(&mut self.confirmations);
            let node_id = node_id.clone();
            let node = self.nodes.get(&node_id).unwrap().clone();
            spawn_cancellable(token.clone(), async move {
                let response = node
                    .status_batch(txs)
                    .await
                    .map(|status| ConfirmationResponse::Submitting(status));
                let _ = tx.send(ConfirmationResult {
                    state_epoch,
                    node_id: node_id.clone(),
                    response,
                });
            });
        }
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
mod tests;
