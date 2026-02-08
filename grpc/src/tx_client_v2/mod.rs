//! Transaction manager v2: queueing, submission, confirmation, and recovery logic.
//!
//! # Overview
//! - `TxSubmitter` is a front-end that enqueues transactions.
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
//! #     TransactionWorker, TxRequest, TxServer,
//! # };
//! # struct DummyServer;
//! # #[async_trait::async_trait]
//! # impl TxServer for DummyServer {
//! #     type TxId = u64;
//! #     type ConfirmInfo = u64;
//! #     type TxRequest = TxRequest;
//! #     async fn submit(
//! #         &self,
//! #         _b: Arc<Vec<u8>>,
//! #         _s: u64,
//! #     ) -> celestia_grpc::tx_client_v2::TxSubmitResult<Self::TxId> {
//! #         unimplemented!()
//! #     }
//! #     async fn status_batch(
//! #         &self,
//! #         _ids: Vec<u64>,
//! #     ) -> celestia_grpc::tx_client_v2::TxConfirmResult<
//! #         Vec<(Self::TxId, celestia_grpc::tx_client_v2::TxStatus<Self::ConfirmInfo>)>,
//! #     > {
//! #         unimplemented!()
//! #     }
//! #     async fn status(
//! #         &self,
//! #         _id: Self::TxId,
//! #     ) -> celestia_grpc::tx_client_v2::TxConfirmResult<
//! #         celestia_grpc::tx_client_v2::TxStatus<Self::ConfirmInfo>,
//! #     > {
//! #         unimplemented!()
//! #     }
//! #     async fn current_sequence(&self) -> Result<u64> { unimplemented!() }
//! #     async fn simulate_and_sign(
//! #         &self,
//! #         _req: Arc<Self::TxRequest>,
//! #         _sequence: u64,
//! #     ) -> std::result::Result<
//! #         celestia_grpc::tx_client_v2::Transaction<Self::TxId, Self::ConfirmInfo>,
//! #         celestia_grpc::tx_client_v2::SigningFailure,
//! #     > {
//! #         unimplemented!()
//! #     }
//! # }
//! # async fn docs() -> Result<()> {
//! let nodes = HashMap::from([(Arc::from("node-1"), Arc::new(DummyServer))]);
//! let (manager, mut worker) = TransactionWorker::new(
//!     nodes,
//!     Duration::from_secs(1),
//!     16,
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
use std::time::Duration;

use async_trait::async_trait;
use celestia_types::state::ErrorCode;
use futures::future::{BoxFuture, FutureExt};
use futures::stream::{FuturesUnordered, StreamExt};
use lumina_utils::executor::spawn_cancellable;
use lumina_utils::time::{self, Interval};
use tokio::select;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

mod node_manager;
mod tx_buffer;

use crate::{Error, Result, TxConfig};
use node_manager::{NodeManager, WorkerMutation, WorkerPlan};
/// Identifier for a submission/confirmation node.
pub type NodeId = Arc<str>;
/// Result for submission calls: either a server TxId or a submission failure.
pub type TxSubmitResult<T> = Result<T, SubmitFailure>;
/// Result for confirmation calls.
pub type TxConfirmResult<T> = Result<T>;
/// Result for signing calls.
pub type TxSigningResult<TxId, ConfirmInfo> =
    Result<Transaction<TxId, ConfirmInfo>, SigningFailure>;

pub trait TxIdT: Clone + std::fmt::Debug {}
impl<T> TxIdT for T where T: Clone + std::fmt::Debug {}

#[derive(Debug, Clone)]
pub enum TxPayload {
    /// Pay-for-blobs transaction.
    Blobs(Vec<celestia_types::Blob>),
    /// Raw Cosmos transaction body with arbitrary messages.
    Tx(celestia_types::state::RawTxBody),
}

#[derive(Debug, Clone)]
pub struct TxRequest {
    pub tx: TxPayload,
    pub cfg: TxConfig,
}

impl TxRequest {
    pub fn tx(body: celestia_types::state::RawTxBody, cfg: TxConfig) -> Self {
        Self {
            tx: TxPayload::Tx(body),
            cfg,
        }
    }

    pub fn blobs(blobs: Vec<celestia_types::Blob>, cfg: TxConfig) -> Self {
        Self {
            tx: TxPayload::Blobs(blobs),
            cfg,
        }
    }
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

struct RequestWithChannels<TxId, ConfirmInfo, Request> {
    request: Arc<Request>,
    sign_tx: oneshot::Sender<Result<()>>,
    submit_tx: oneshot::Sender<Result<TxId>>,
    confirm_tx: oneshot::Sender<Result<TxStatus<ConfirmInfo>>>,
}

impl<TxId: TxIdT, ConfirmInfo, Request> RequestWithChannels<TxId, ConfirmInfo, Request> {
    fn new(
        request: Request,
        sign_tx: oneshot::Sender<Result<()>>,
        submit_tx: oneshot::Sender<Result<TxId>>,
        confirm_tx: oneshot::Sender<Result<TxStatus<ConfirmInfo>>>,
    ) -> Self {
        Self {
            request: Arc::new(request),
            sign_tx,
            submit_tx,
            confirm_tx,
        }
    }
}

struct SigningResult<TxId: TxIdT, ConfirmInfo> {
    node_id: NodeId,
    sequence: u64,
    response: TxSigningResult<TxId, ConfirmInfo>,
}

#[derive(Debug)]
pub struct TxHandle<TxId: TxIdT, ConfirmInfo> {
    /// Returns if a transaction is correctly signed.
    pub signed: oneshot::Receiver<Result<()>>,
    /// Receives submit result.
    pub submitted: oneshot::Receiver<Result<TxId>>,
    /// Receives confirm result.
    pub confirmed: oneshot::Receiver<Result<TxStatus<ConfirmInfo>>>,
}

#[derive(Clone)]
pub struct TxSubmitter<TxId: TxIdT, ConfirmInfo, Request> {
    add_tx: mpsc::Sender<RequestWithChannels<TxId, ConfirmInfo, Request>>,
}

impl<TxId: TxIdT, ConfirmInfo, Request> TxSubmitter<TxId, ConfirmInfo, Request> {
    /// Enqueue a transaction request for signing, returning a handle for sign/submit/confirm.
    ///
    /// # Example
    /// ```no_run
    /// # use celestia_grpc::Result;
    /// # use celestia_grpc::tx_client_v2::{TxSubmitter, TxRequest};
    /// # async fn docs(manager: TxSubmitter<u64, u64, TxRequest>) -> Result<()> {
    /// let handle = manager
    ///     .add_tx(TxRequest::tx(
    ///         celestia_types::state::RawTxBody::default(),
    ///         TxConfig::default(),
    ///     ))
    ///     .await?;
    /// handle.signed.await?;
    /// handle.submitted.await?;
    /// handle.confirmed.await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn add_tx(&self, request: Request) -> Result<TxHandle<TxId, ConfirmInfo>> {
        let (sign_tx, sign_rx) = oneshot::channel();
        let (submit_tx, submit_rx) = oneshot::channel();
        let (confirm_tx, confirm_rx) = oneshot::channel();
        let request_with_channels =
            RequestWithChannels::new(request, sign_tx, submit_tx, confirm_tx);
        match self.add_tx.try_send(request_with_channels) {
            Ok(()) => Ok(TxHandle {
                signed: sign_rx,
                submitted: submit_rx,
                confirmed: confirm_rx,
            }),
            Err(mpsc::error::TrySendError::Full(_)) => Err(Error::UnexpectedResponseType(
                "transaction queue full".to_string(),
            )),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(Error::TxWorkerStopped),
        }
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
    SequenceMismatch {
        expected: u64,
    },
    /// Submission failed with a specific error code and message.
    Other {
        error_code: ErrorCode,
        message: String,
    },
    /// Transport or RPC error while submitting.
    NetworkError {
        err: Arc<Error>,
    },
    /// Node mempool is full.
    MempoolIsFull,
    /// Transaction is already in the mempool cache.
    TxInMempoolCache,
}

impl SubmitFailure {
    fn label(&self) -> String {
        match self {
            SubmitFailure::SequenceMismatch { expected } => {
                format!("SequenceMismatch expected={}", expected)
            }
            SubmitFailure::Other {
                error_code,
                message,
            } => format!("Other code={:?} msg={}", error_code, message),
            SubmitFailure::NetworkError { err } => format!("NetworkError {}", err),
            SubmitFailure::MempoolIsFull => "MempoolIsFull".to_string(),
            SubmitFailure::TxInMempoolCache => "TxInMempoolCache".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum SigningFailure {
    /// Server expects a different sequence.
    SequenceMismatch { expected: u64 },
    /// Submission failed with a specific error code and message.
    Other { message: String },
    /// Transport or RPC error while submitting.
    NetworkError { err: Arc<Error> },
}

impl SigningFailure {
    fn label(&self) -> String {
        match self {
            SigningFailure::SequenceMismatch { expected } => {
                format!("SequenceMismatch expected={}", expected)
            }
            SigningFailure::Other { message } => format!("Other msg={}", message),
            SigningFailure::NetworkError { err } => format!("NetworkError {}", err),
        }
    }
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
    type TxRequest: Send + Sync + 'static;

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
    /// Simulate a transaction and sign it with the given sequence, returning a signed transaction.
    async fn simulate_and_sign(
        &self,
        req: Arc<Self::TxRequest>,
        sequence: u64,
    ) -> Result<Transaction<Self::TxId, Self::ConfirmInfo>, SigningFailure> {
        let _ = (req, sequence);
        Err(SigningFailure::NetworkError {
            err: Arc::new(Error::UnexpectedResponseType(
                "simulate_and_sign not implemented".to_string(),
            )),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlanRequest {
    Submission,
    Confirmation,
    Signing,
}

#[derive(Debug)]
enum NodeEvent<TxId: TxIdT, ConfirmInfo> {
    NodeResponse {
        node_id: NodeId,
        response: NodeResponse<TxId, ConfirmInfo>,
    },
    NodeStop,
}

impl<TxId: TxIdT, ConfirmInfo> NodeEvent<TxId, ConfirmInfo> {
    fn summary(&self) -> String {
        match self {
            NodeEvent::NodeResponse { response, .. } => match response {
                NodeResponse::Submission { sequence, result } => {
                    match result {
                        Ok(_) => format!("NodeResponse::Submission seq={} Ok", sequence),
                        Err(err) => format!(
                            "NodeResponse::Submission seq={} Err {}",
                            sequence,
                            err.label()
                        ),
                    }
                }
                NodeResponse::Confirmation { response, .. } => match response {
                    Ok(ConfirmationResponse::Batch { statuses }) => {
                        format!("NodeResponse::Confirmation Batch {}", statuses.len())
                    }
                    Ok(ConfirmationResponse::Single { .. }) => {
                        "NodeResponse::Confirmation Single".to_string()
                    }
                    Err(err) => format!("NodeResponse::Confirmation Err {}", err),
                },
                NodeResponse::Signing { sequence, result } => {
                    match result {
                        Ok(_) => format!("NodeResponse::Signing seq={} Ok", sequence),
                        Err(err) => {
                            format!("NodeResponse::Signing seq={} Err {}", sequence, err.label())
                        }
                    }
                }
            },
            NodeEvent::NodeStop => "NodeStop".to_string(),
        }
    }
}

fn push_plan_request(requests: &mut Vec<PlanRequest>, request: PlanRequest) {
    if !requests.contains(&request) {
        requests.push(request);
    }
}

#[derive(Debug)]
enum NodeResponse<TxId: TxIdT, ConfirmInfo> {
    Submission {
        sequence: u64,
        result: TxSubmitResult<TxId>,
    },
    Confirmation {
        state_epoch: u64,
        response: TxConfirmResult<ConfirmationResponse<TxId, ConfirmInfo>>,
    },
    Signing {
        sequence: u64,
        result: TxSigningResult<TxId, ConfirmInfo>,
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
    transactions: tx_buffer::TxBuffer<
        S::TxId,
        S::ConfirmInfo,
        RequestWithChannels<S::TxId, S::ConfirmInfo, S::TxRequest>,
    >,
    add_tx_rx: mpsc::Receiver<RequestWithChannels<S::TxId, S::ConfirmInfo, S::TxRequest>>,
    submissions: FuturesUnordered<BoxFuture<'static, Option<SubmissionResult<S::TxId>>>>,
    confirmations:
        FuturesUnordered<BoxFuture<'static, Option<ConfirmationResult<S::TxId, S::ConfirmInfo>>>>,
    signing: Option<BoxFuture<'static, Option<SigningResult<S::TxId, S::ConfirmInfo>>>>,
    confirm_ticker: Interval,
    confirm_interval: Duration,
    max_status_batch: usize,
    plan_requests: Vec<PlanRequest>,
}

impl<S: TxServer + 'static> TransactionWorker<S> {
    /// Create a submitter/worker pair with initial sequence and queue capacity.
    ///
    /// # Notes
    /// - `start_sequence` should be the next sequence to submit for the signer.
    /// - `confirm_interval` drives periodic confirmation polling.
    pub fn new(
        nodes: HashMap<NodeId, Arc<S>>,
        confirm_interval: Duration,
        max_status_batch: usize,
        start_sequence: u64,
        add_tx_capacity: usize,
    ) -> (TxSubmitter<S::TxId, S::ConfirmInfo, S::TxRequest>, Self) {
        let (add_tx_tx, add_tx_rx) = mpsc::channel(add_tx_capacity);
        let manager = TxSubmitter {
            add_tx: add_tx_tx.clone(),
        };
        let start_confirmed = start_sequence.saturating_sub(1);
        let node_ids = nodes.keys().cloned().collect::<Vec<_>>();
        let node_manager = NodeManager::new(node_ids, start_confirmed);
        let transactions = tx_buffer::TxBuffer::new(start_confirmed);
        (
            manager,
            TransactionWorker {
                add_tx_rx,
                nodes: node_manager,
                servers: nodes,
                transactions,
                submissions: FuturesUnordered::new(),
                confirmations: FuturesUnordered::new(),
                signing: None,
                confirm_ticker: Interval::new(confirm_interval),
                confirm_interval,
                max_status_batch,
                plan_requests: Vec::new(),
            },
        )
    }

    fn enqueue_pending(
        &mut self,
        request: RequestWithChannels<S::TxId, S::ConfirmInfo, S::TxRequest>,
    ) -> Result<()> {
        self.transactions
            .add_pending(request)
            .map_err(|_| crate::Error::UnexpectedResponseType("pending queue error".to_string()))?;
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
        let mut shutdown_seen = false;

        loop {
            if !self.plan_requests.is_empty() {
                let plan_requests = std::mem::take(&mut self.plan_requests);
                let plans =
                    self.nodes
                        .plan(&plan_requests, &self.transactions, self.max_status_batch);
                self.execute_plans(plans, shutdown.clone());
            }

            if let Some(event) = current_event.take() {
                let outcome =
                    self.nodes
                        .apply_event(event, &self.transactions, self.confirm_interval);
                let stop = self.apply_mutations(outcome.mutations)?;
                if stop {
                    break;
                }
                for request in outcome.plan_requests {
                    push_plan_request(&mut self.plan_requests, request);
                }
                continue;
            }

            select! {
                _ = poll_shutdown(&shutdown, &mut shutdown_seen) => {
                    current_event = Some(NodeEvent::NodeStop);
                }
                tx = self.add_tx_rx.recv() => {
                    if let Some(tx) = tx {
                        self.enqueue_pending(tx)?;
                        push_plan_request(&mut self.plan_requests, PlanRequest::Signing);
                    }
                }
                _ = self.confirm_ticker.tick() => {
                    push_plan_request(&mut self.plan_requests, PlanRequest::Confirmation);
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
                result = poll_opt(&mut self.signing), if self.signing.is_some() => {
                    self.signing = None;
                    if let Some(Some(result)) = result {
                        current_event = Some(NodeEvent::NodeResponse {
                            node_id: result.node_id,
                            response: NodeResponse::Signing {
                                sequence: result.sequence,
                                result: result.response,
                            },
                        });
                    }
                }
            }
        }
        info!("stopping nodes");
        shutdown.cancel();
        Ok(())
    }

    fn execute_plans(&mut self, plans: Vec<WorkerPlan<S::TxId>>, token: CancellationToken) {
        for plan in plans {
            debug!(plan = %plan.summary(), "worker plan");
            match plan {
                WorkerPlan::SpawnSigning {
                    node_id,
                    sequence,
                    delay,
                } => {
                    if self.signing.is_some() {
                        continue;
                    }
                    let Some(node) = self.servers.get(&node_id).cloned() else {
                        continue;
                    };
                    let Some(request) = self.transactions.peek_pending() else {
                        continue;
                    };
                    let request_ref = request.request.clone();
                    let tx = self.push_signing();
                    spawn_cancellable(token.clone(), async move {
                        time::sleep(delay).await;
                        let response = node.simulate_and_sign(request_ref, sequence).await;
                        let _ = tx.send(SigningResult {
                            node_id,
                            sequence,
                            response,
                        });
                    });
                }
                WorkerPlan::SpawnSubmit {
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
                WorkerPlan::SpawnConfirmBatch {
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
                WorkerPlan::SpawnRecover { node_id, id, epoch } => {
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
            }
        }
    }

    fn apply_mutations(
        &mut self,
        mutations: Vec<WorkerMutation<S::TxId, S::ConfirmInfo>>,
    ) -> Result<bool> {
        for mutation in mutations {
            debug!(mutation = %mutation.summary(), "worker mutation");
            match mutation {
                WorkerMutation::EnqueueSigned { mut tx } => {
                    let Some(request) = self.transactions.pop_pending() else {
                        return Err(Error::UnexpectedResponseType(
                            "missing pending request".to_string(),
                        ));
                    };
                    tx.callbacks.submitted = Some(request.submit_tx);
                    tx.callbacks.confirmed = Some(request.confirm_tx);
                    self.enqueue_signed(tx, request.sign_tx)?;
                }
                WorkerMutation::MarkSubmitted { sequence, id } => {
                    self.mark_submitted(sequence, id);
                }
                WorkerMutation::Confirm { seq, info } => {
                    self.confirm_tx(seq, info);
                }
                WorkerMutation::WorkerStop => {
                    let fatal = self.nodes.tail_error_status();
                    self.finalize_remaining(fatal);
                    return Ok(true);
                }
            }
        }
        if let Some(limit) = self.nodes.min_confirmed_non_stopped() {
            let before = self.transactions.confirmed_seq();
            debug!(
                before,
                limit,
                max_seq = self.transactions.max_seq(),
                "clear_confirmed_up_to candidate"
            );
            self.clear_confirmed_up_to(limit);
            let after = self.transactions.confirmed_seq();
            debug!(before, after, limit, "clear_confirmed_up_to result");
        }
        Ok(false)
    }

    fn push_signing(&mut self) -> oneshot::Sender<SigningResult<S::TxId, S::ConfirmInfo>> {
        let (tx, rx) = oneshot::channel();
        self.signing = Some(async move { rx.await.ok() }.boxed());
        tx
    }

    fn enqueue_signed(
        &mut self,
        tx: Transaction<S::TxId, S::ConfirmInfo>,
        signed: oneshot::Sender<Result<()>>,
    ) -> Result<()> {
        let exp_seq = self.transactions.max_seq() + 1;
        let tx_sequence = tx.sequence;
        if let Err(_) = self.transactions.add_transaction(tx) {
            let msg = format!("tx sequence gap: expected {}, got {}", exp_seq, tx_sequence);
            let _ = signed.send(Err(crate::Error::UnexpectedResponseType(msg.clone())));
            return Err(crate::Error::UnexpectedResponseType(msg));
        }
        let _ = signed.send(Ok(()));
        Ok(())
    }

    fn mark_submitted(&mut self, sequence: u64, id: S::TxId) {
        if let Some(tx) = self.transactions.get_mut(sequence) {
            tx.notify_submitted(Ok(id.clone()));
        }
        let _ = self.transactions.set_submitted_id(sequence, id);
    }

    fn confirm_tx(&mut self, seq: u64, info: S::ConfirmInfo) {
        let Some(tx) = self.transactions.get_mut(seq) else {
            return;
        };
        tx.notify_confirmed(Ok(TxStatus::Confirmed { info }));
    }

    fn finalize_remaining(&mut self, fatal: Option<TxStatus<S::ConfirmInfo>>) {
        for pending in self.transactions.drain_pending() {
            let _ = pending.sign_tx.send(Err(Error::TxWorkerStopped));
            let _ = pending.submit_tx.send(Err(Error::TxWorkerStopped));
            let _ = pending.confirm_tx.send(Err(Error::TxWorkerStopped));
        }
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
                tx.notify_submitted(Err(Error::TxWorkerStopped));
            }
            tx.notify_confirmed(Ok(status));
        }
    }

    fn clear_confirmed_up_to(&mut self, limit: u64) {
        loop {
            let next = self.transactions.confirmed_seq() + 1;
            if next > limit {
                break;
            }
            if self.transactions.confirm(next).is_err() {
                break;
            }
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

async fn poll_opt<F: std::future::Future + Unpin>(fut: &mut Option<F>) -> Option<F::Output> {
    match fut.as_mut() {
        Some(fut) => Some(fut.await),
        None => futures::future::pending().await,
    }
}

async fn poll_shutdown(shutdown: &CancellationToken, seen: &mut bool) {
    if *seen {
        futures::future::pending::<()>().await;
    } else {
        shutdown.cancelled().await;
        *seen = true;
    }
}

#[cfg(test)]
mod tests;
