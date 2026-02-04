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
use std::collections::HashMap;
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
use tokio::select;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

mod node_manager;
mod tx_buffer;

use crate::{Error, Result, TxConfig};
use node_manager::{NodeManager, WorkerEvent};
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
    TxNotSubmitted {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WakeUpKind {
    Submission,
    Confirmation,
}

#[derive(Debug)]
enum NodeEvent<TxId, ConfirmInfo> {
    WakeUp(WakeUpKind),
    NodeResponse {
        node_id: NodeId,
        response: NodeResponse<TxId, ConfirmInfo>,
    },
    NodeStop,
}

#[derive(Debug)]
enum NodeResponse<TxId, ConfirmInfo> {
    Submission {
        sequence: u64,
        result: TxSubmitResult<TxId>,
    },
    Confirmation {
        state_epoch: u64,
        response: TxConfirmResult<ConfirmationResponse<TxId, ConfirmInfo>>,
    },
}

#[derive(Debug)]
enum ConfirmationResponse<TxId, ConfirmInfo> {
    Batch {
        statuses: Vec<(TxId, TxStatus<ConfirmInfo>)>,
    },
    Single {
        id: TxId,
        status: TxStatus<ConfirmInfo>,
    },
}

struct SubmissionResult<TxId> {
    node_id: NodeId,
    sequence: u64,
    result: TxSubmitResult<TxId>,
}

struct ConfirmationResult<TxId, ConfirmInfo> {
    node_id: NodeId,
    state_epoch: u64,
    response: TxConfirmResult<ConfirmationResponse<TxId, ConfirmInfo>>,
}

pub struct TransactionWorker<S: TxServer> {
    nodes: NodeManager<S>,
    servers: HashMap<NodeId, Arc<S>>,
    transactions: tx_buffer::TxBuffer<S::TxId, S::ConfirmInfo>,
    add_tx_rx: mpsc::Receiver<Transaction<S::TxId, S::ConfirmInfo>>,
    submissions: FuturesUnordered<BoxFuture<'static, Option<SubmissionResult<S::TxId>>>>,
    confirmations:
        FuturesUnordered<BoxFuture<'static, Option<ConfirmationResult<S::TxId, S::ConfirmInfo>>>>,
    confirm_ticker: Interval,
    confirm_interval: Duration,
    max_status_batch: usize,
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
        let start_confirmed = start_sequence.saturating_sub(1);
        let node_ids = nodes.keys().cloned().collect::<Vec<_>>();
        let node_manager = NodeManager::new(node_ids, start_confirmed);
        let transactions = tx_buffer::TxBuffer::new(0);
        (
            manager,
            TransactionWorker {
                add_tx_rx,
                nodes: node_manager,
                servers: nodes,
                transactions,
                submissions: FuturesUnordered::new(),
                confirmations: FuturesUnordered::new(),
                confirm_ticker: Interval::new(confirm_interval),
                confirm_interval,
                max_status_batch,
            },
        )
    }

    fn enqueue_tx(&mut self, tx: Transaction<S::TxId, S::ConfirmInfo>) -> Result<()> {
        let exp_seq = self.transactions.max_seq() + 1;
        let tx_sequence = tx.sequence;
        self.transactions.add_transaction(tx).map_err(|_| {
            crate::Error::UnexpectedResponseType(format!(
                "tx sequence gap: expected {}, got {}",
                exp_seq, tx_sequence
            ))
        })?;
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

        let mut current_event: Option<NodeEvent<S::TxId, S::ConfirmInfo>> = None;

        loop {
            if let Some(event) = current_event.take() {
                let outcome = self.nodes.handle_event(
                    event,
                    &self.transactions,
                    self.confirm_interval,
                    self.max_status_batch,
                );
                let stop = self.apply_worker_events(outcome.worker_events, shutdown.clone())?;
                if stop {
                    break;
                }
                if let Some(kind) = outcome.wake_up {
                    current_event = Some(NodeEvent::WakeUp(kind));
                }
                continue;
            }

            select! {
                _ = shutdown.cancelled() => {
                    current_event = Some(NodeEvent::NodeStop);
                }
                tx = self.add_tx_rx.recv() => {
                    if let Some(tx) = tx {
                        let stop = self.apply_worker_events(
                            vec![WorkerEvent::EnqueueTx { tx }],
                            shutdown.clone(),
                        )?;
                        if stop {
                            break;
                        }
                        current_event = Some(NodeEvent::WakeUp(WakeUpKind::Submission));
                    }
                }
                _ = self.confirm_ticker.tick() => {
                    current_event = Some(NodeEvent::WakeUp(WakeUpKind::Confirmation));
                }
                result = self.submissions.next() => {
                    if let Some(Some(result)) = result {
                        current_event = Some(NodeEvent::NodeResponse {
                            node_id: result.node_id,
                            response: NodeResponse::Submission {
                                sequence: result.sequence,
                                result: result.result,
                            },
                        });
                    }
                }
                result = self.confirmations.next() => {
                    if let Some(Some(result)) = result {
                        current_event = Some(NodeEvent::NodeResponse {
                            node_id: result.node_id,
                            response: NodeResponse::Confirmation {
                                state_epoch: result.state_epoch,
                                response: result.response,
                            },
                        });
                    }
                }
            }
        }

        shutdown.cancel();
        Ok(())
    }

    fn apply_worker_events(
        &mut self,
        events: Vec<WorkerEvent<S::TxId, S::ConfirmInfo>>,
        token: CancellationToken,
    ) -> Result<bool> {
        for event in events {
            match event {
                WorkerEvent::EnqueueTx { tx } => {
                    self.enqueue_tx(tx)?;
                }
                WorkerEvent::SpawnSubmit {
                    node_id,
                    bytes,
                    sequence,
                    delay,
                } => {
                    let Some(node) = self.servers.get(&node_id).cloned() else {
                        continue;
                    };
                    let tx = push_oneshot(&mut self.submissions);
                    spawn_cancellable(token.clone(), async move {
                        time::sleep(delay).await;
                        let result = node.submit(bytes, sequence).await;
                        let _ = tx.send(SubmissionResult {
                            node_id,
                            sequence,
                            result,
                        });
                    });
                }
                WorkerEvent::SpawnConfirmBatch {
                    node_id,
                    ids,
                    epoch,
                } => {
                    let Some(node) = self.servers.get(&node_id).cloned() else {
                        continue;
                    };
                    let tx = push_oneshot(&mut self.confirmations);
                    spawn_cancellable(token.clone(), async move {
                        let response = node
                            .status_batch(ids)
                            .await
                            .map(|statuses| ConfirmationResponse::Batch { statuses });
                        let _ = tx.send(ConfirmationResult {
                            state_epoch: epoch,
                            node_id,
                            response,
                        });
                    });
                }
                WorkerEvent::SpawnRecover { node_id, id, epoch } => {
                    let Some(node) = self.servers.get(&node_id).cloned() else {
                        continue;
                    };
                    let tx = push_oneshot(&mut self.confirmations);
                    spawn_cancellable(token.clone(), async move {
                        let response = node
                            .status(id.clone())
                            .await
                            .map(|status| ConfirmationResponse::Single { id, status });
                        let _ = tx.send(ConfirmationResult {
                            node_id,
                            response,
                            state_epoch: epoch,
                        });
                    });
                }
                WorkerEvent::MarkSubmitted { sequence, id } => {
                    self.mark_submitted(sequence, id);
                }
                WorkerEvent::Confirm { seq, info } => {
                    self.confirm_tx(seq, info);
                }
                WorkerEvent::WorkerStop => {
                    let fatal = self.nodes.tail_error_status();
                    self.finalize_remaining(fatal);
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    fn mark_submitted(&mut self, sequence: u64, id: S::TxId) {
        if let Some(tx) = self.transactions.get_mut(sequence) {
            tx.notify_submitted(Ok(id.clone()));
        }
        let _ = self.transactions.set_submitted_id(sequence, id);
    }

    fn confirm_tx(&mut self, seq: u64, info: S::ConfirmInfo) {
        let expected = self.transactions.confirmed_seq() + 1;
        if seq != expected {
            return;
        }
        if let Ok(mut tx) = self.transactions.confirm(seq) {
            tx.notify_confirmed(Ok(TxStatus::Confirmed { info }));
        }
    }

    fn finalize_remaining(&mut self, fatal: Option<TxStatus<S::ConfirmInfo>>) {
        let submit_error_msg = "not submitted: limit_seq".to_string();
        loop {
            let next = self.transactions.confirmed_seq() + 1;
            let Ok(mut tx) = self.transactions.confirm(next) else {
                break;
            };
            let status = if let Some(fatal_status) = fatal.clone() {
                fatal_status
            } else {
                TxStatus::Unknown
            };
            if tx.id.is_none() {
                tx.notify_submitted(Err(Error::UnexpectedResponseType(submit_error_msg.clone())));
            }
            tx.notify_confirmed(Ok(status));
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
