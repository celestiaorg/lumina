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
//! #     type SubmitError = ();
//! #     type ConfirmResponse = ();
//! #     async fn submit(
//! #         &self,
//! #         _b: Arc<Vec<u8>>,
//! #         _s: u64,
//! #     ) -> celestia_grpc::tx_client_v2::TxSubmitResult<Self::TxId, Self::SubmitError> {
//! #         unimplemented!()
//! #     }
//! #     async fn status_batch(
//! #         &self,
//! #         _ids: Vec<u64>,
//! #     ) -> celestia_grpc::tx_client_v2::TxConfirmResult<
//! #         Vec<(
//! #             Self::TxId,
//! #             celestia_grpc::tx_client_v2::TxStatus<Self::ConfirmInfo, Self::ConfirmResponse>,
//! #         )>,
//! #     > {
//! #         unimplemented!()
//! #     }
//! #     async fn status(
//! #         &self,
//! #         _id: Self::TxId,
//! #     ) -> celestia_grpc::tx_client_v2::TxConfirmResult<
//! #         celestia_grpc::tx_client_v2::TxStatus<Self::ConfirmInfo, Self::ConfirmResponse>,
//! #     > {
//! #         unimplemented!()
//! #     }
//! #     async fn current_sequence(&self) -> Result<u64> { unimplemented!() }
//! #     async fn simulate_and_sign(
//! #         &self,
//! #         _req: Arc<Self::TxRequest>,
//! #         _sequence: u64,
//! #     ) -> std::result::Result<
//! #         celestia_grpc::tx_client_v2::Transaction<
//! #             Self::TxId,
//! #             Self::ConfirmInfo,
//! #             Self::ConfirmResponse,
//! #             Self::SubmitError,
//! #         >,
//! #         celestia_grpc::tx_client_v2::SigningFailure<Self::SubmitError>,
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
//!     Some(0),
//!     128,
//! );
//! # let _ = manager;
//! # let _ = worker;
//! # Ok(())
//! # }
//! ```
use std::collections::HashMap;
use std::fmt;
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
use node_manager::NodeManager;
/// Identifier for a submission/confirmation node.
pub type NodeId = Arc<str>;
/// Result for submission calls: either a server TxId or a submission failure.
pub type TxSubmitResult<T, E> = Result<T, SubmitFailure<E>>;
/// Result for confirmation calls.
pub type TxConfirmResult<T> = Result<T>;
/// Result for signing calls.
pub type TxSigningResult<TxId, ConfirmInfo, ConfirmResponse, SubmitErr> =
    Result<Transaction<TxId, ConfirmInfo, ConfirmResponse, SubmitErr>, SigningFailure<SubmitErr>>;

/// Trait bound for transaction identifiers.
pub trait TxIdT: Clone + std::fmt::Debug {}
impl<T> TxIdT for T where T: Clone + std::fmt::Debug {}

/// Payload for a transaction request.
#[derive(Debug, Clone)]
pub enum TxPayload {
    /// Pay-for-blobs transaction.
    Blobs(Vec<celestia_types::Blob>),
    /// Raw Cosmos transaction body with arbitrary messages.
    Tx(celestia_types::state::RawTxBody),
}

/// A transaction request combining payload and configuration.
#[derive(Debug, Clone)]
pub struct TxRequest {
    /// The transaction payload (blobs or raw tx body).
    pub tx: TxPayload,
    /// Configuration for gas, fees, and other settings.
    pub cfg: TxConfig,
}

/// Error indicating the worker stopped before completing an operation.
#[derive(Debug, Clone)]
pub enum StopError<SubmitErr, ConfirmInfo, ConfirmResponse> {
    /// Confirmation failed with a status indicating rejection or other issue.
    ConfirmError(TxStatus<ConfirmInfo, ConfirmResponse>),
    /// Submission failed with an error from the node.
    SubmitError(SubmitErr),
    /// Signing failed with an error.
    SignError(SubmitErr),
    /// The transaction worker stopped unexpectedly.
    WorkerStopped,
}

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

/// Result type for confirmation, either success info or a stop error.
pub type ConfirmResult<ConfirmInfo, SubmitErr, ConfirmResponse> =
    std::result::Result<ConfirmInfo, StopError<SubmitErr, ConfirmInfo, ConfirmResponse>>;

impl<SubmitErr, ConfirmInfo, ConfirmResponse> fmt::Display
    for StopError<SubmitErr, ConfirmInfo, ConfirmResponse>
where
    SubmitErr: fmt::Debug,
    ConfirmInfo: fmt::Debug,
    ConfirmResponse: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StopError::ConfirmError(status) => write!(f, "ConfirmError({:?})", status),
            StopError::SubmitError(err) => write!(f, "SubmitError({:?})", err),
            StopError::SignError(err) => write!(f, "SignError({:?})", err),
            StopError::WorkerStopped => write!(f, "WorkerStopped"),
        }
    }
}

impl TxRequest {
    /// Create a request for a raw transaction body.
    pub fn tx(body: celestia_types::state::RawTxBody, cfg: TxConfig) -> Self {
        Self {
            tx: TxPayload::Tx(body),
            cfg,
        }
    }

    /// Create a request for a pay-for-blobs transaction.
    pub fn blobs(blobs: Vec<celestia_types::Blob>, cfg: TxConfig) -> Self {
        Self {
            tx: TxPayload::Blobs(blobs),
            cfg,
        }
    }
}

/// A signed transaction ready for submission.
#[derive(Debug)]
pub struct Transaction<TxId: TxIdT, ConfirmInfo, ConfirmResponse, SubmitErr> {
    /// Transaction sequence for the signer.
    pub sequence: u64,
    /// Signed transaction bytes ready for broadcast.
    pub bytes: Arc<Vec<u8>>,
    /// One-shot callbacks for submit/confirm acknowledgements.
    pub callbacks: TxCallbacks<TxId, ConfirmInfo, ConfirmResponse, SubmitErr>,
    /// Id of the transaction (set after submission).
    pub id: Option<TxId>,
}

impl<TxId: TxIdT, ConfirmInfo, ConfirmResponse, SubmitErr>
    Transaction<TxId, ConfirmInfo, ConfirmResponse, SubmitErr>
{
    fn notify_submitted(&mut self, result: Result<TxId>) {
        if let Some(submitted) = self.callbacks.submitted.take() {
            let _ = submitted.send(result);
        }
    }

    fn notify_confirmed(&mut self, result: ConfirmResult<ConfirmInfo, SubmitErr, ConfirmResponse>) {
        if let Some(confirmed) = self.callbacks.confirmed.take() {
            let _ = confirmed.send(result);
        }
    }
}

/// Callbacks for transaction lifecycle events.
#[derive(Debug)]
pub struct TxCallbacks<TxId: TxIdT, ConfirmInfo, ConfirmResponse, SubmitErr> {
    /// Resolves when submission succeeds or fails.
    pub submitted: Option<oneshot::Sender<Result<TxId>>>,
    /// Resolves when the transaction is confirmed or rejected.
    pub confirmed: Option<oneshot::Sender<ConfirmResult<ConfirmInfo, SubmitErr, ConfirmResponse>>>,
}

impl<TxId: TxIdT, ConfirmInfo, ConfirmResponse, SubmitErr> Default
    for TxCallbacks<TxId, ConfirmInfo, ConfirmResponse, SubmitErr>
{
    fn default() -> Self {
        Self {
            submitted: None,
            confirmed: None,
        }
    }
}

struct RequestWithChannels<TxId, ConfirmInfo, ConfirmResponse, SubmitErr, Request> {
    request: Arc<Request>,
    sign_tx: oneshot::Sender<Result<()>>,
    submit_tx: oneshot::Sender<Result<TxId>>,
    confirm_tx: oneshot::Sender<ConfirmResult<ConfirmInfo, SubmitErr, ConfirmResponse>>,
}

impl<TxId: TxIdT, ConfirmInfo, ConfirmResponse, SubmitErr, Request>
    RequestWithChannels<TxId, ConfirmInfo, ConfirmResponse, SubmitErr, Request>
{
    fn new(
        request: Request,
        sign_tx: oneshot::Sender<Result<()>>,
        submit_tx: oneshot::Sender<Result<TxId>>,
        confirm_tx: oneshot::Sender<ConfirmResult<ConfirmInfo, SubmitErr, ConfirmResponse>>,
    ) -> Self {
        Self {
            request: Arc::new(request),
            sign_tx,
            submit_tx,
            confirm_tx,
        }
    }
}

struct SigningResult<TxId: TxIdT, ConfirmInfo, ConfirmResponse, SubmitErr> {
    node_id: NodeId,
    sequence: u64,
    response: TxSigningResult<TxId, ConfirmInfo, ConfirmResponse, SubmitErr>,
}

/// Handle for tracking a transaction through signing, submission, and confirmation.
#[derive(Debug)]
pub struct TxHandle<TxId: TxIdT, ConfirmInfo, ConfirmResponse, SubmitErr> {
    /// Resolves when signing completes successfully or fails.
    pub signed: oneshot::Receiver<Result<()>>,
    /// Resolves when submission completes with the transaction id or an error.
    pub submitted: oneshot::Receiver<Result<TxId>>,
    /// Resolves when confirmation completes with info or a stop error.
    pub confirmed: oneshot::Receiver<ConfirmResult<ConfirmInfo, SubmitErr, ConfirmResponse>>,
}

/// Front-end handle for enqueuing transactions into the worker.
#[derive(Clone)]
pub struct TxSubmitter<TxId: TxIdT, ConfirmInfo, ConfirmResponse, SubmitErr, Request> {
    add_tx:
        mpsc::Sender<RequestWithChannels<TxId, ConfirmInfo, ConfirmResponse, SubmitErr, Request>>,
}

impl<TxId: TxIdT, ConfirmInfo, ConfirmResponse, SubmitErr, Request>
    TxSubmitter<TxId, ConfirmInfo, ConfirmResponse, SubmitErr, Request>
{
    /// Enqueue a transaction request and return a handle for tracking its progress.
    pub async fn add_tx(
        &self,
        request: Request,
    ) -> Result<TxHandle<TxId, ConfirmInfo, ConfirmResponse, SubmitErr>> {
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

/// The kind of status for a transaction.
#[derive(Debug, Clone)]
pub enum TxStatusKind<ConfirmInfo> {
    /// Submitted, but not yet committed.
    Pending,
    /// Included in a block successfully with confirmation info.
    Confirmed {
        /// Confirmation details.
        info: ConfirmInfo,
    },
    /// Rejected by the node with a specific reason.
    Rejected {
        /// The reason for rejection.
        reason: RejectionReason,
    },
    /// Removed from mempool; may need resubmission.
    Evicted,
    /// Status could not be determined.
    Unknown,
}

/// Transaction status with the original response from the node.
#[derive(Debug, Clone)]
pub struct TxStatus<ConfirmInfo, OriginalResponse> {
    /// The mapped status kind.
    pub kind: TxStatusKind<ConfirmInfo>,
    /// The original response from the node for additional details.
    pub original_response: OriginalResponse,
}

impl<ConfirmInfo, OriginalResponse> TxStatus<ConfirmInfo, OriginalResponse> {
    pub(crate) fn new(
        kind: TxStatusKind<ConfirmInfo>,
        original_response: OriginalResponse,
    ) -> Self {
        Self {
            kind,
            original_response,
        }
    }
}

/// Errors that can occur during transaction submission.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum SubmitError {
    /// Server expects a different sequence.
    SequenceMismatch {
        /// The sequence number the server expected.
        expected: u64,
    },
    /// Transaction failed due to insufficient fee.
    InsufficientFee {
        /// The minimum fee the server requires.
        expected_fee: u64,
        /// Error message from the server.
        message: String,
    },
    /// Transport or RPC error while submitting.
    NetworkError,
    /// Node mempool is full.
    MempoolIsFull,
    /// Transaction is already in the mempool cache.
    TxInMempoolCache,
    /// Submission failed with a specific error code and message.
    Other {
        /// Error code returned by the server.
        error_code: ErrorCode,
        /// Error message from the server.
        message: String,
    },
}

impl SubmitError {
    fn label(&self) -> String {
        match self {
            SubmitError::SequenceMismatch { expected } => {
                format!("SequenceMismatch expected={}", expected)
            }
            SubmitError::InsufficientFee {
                expected_fee,
                message,
            } => format!(
                "InsufficientFee expected_fee={} msg={}",
                expected_fee, message
            ),
            SubmitError::Other {
                error_code,
                message,
            } => format!("Other code={:?} msg={}", error_code, message),
            SubmitError::NetworkError => "NetworkError".to_string(),
            SubmitError::MempoolIsFull => "MempoolIsFull".to_string(),
            SubmitError::TxInMempoolCache => "TxInMempoolCache".to_string(),
        }
    }
}

/// A submission failure containing both mapped and original errors.
#[derive(Debug, Clone)]
pub struct SubmitFailure<T> {
    /// The categorized error for internal handling.
    pub mapped_error: SubmitError,
    /// The original error from the underlying transport or node.
    pub original_error: T,
}

impl<T: fmt::Debug> SubmitFailure<T> {
    fn label(&self) -> String {
        format!(
            "{} original={:?}",
            self.mapped_error.label(),
            self.original_error
        )
    }
}

/// Errors that can occur during transaction signing.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum SigningError {
    /// Server expects a different sequence (detected during simulation).
    SequenceMismatch {
        /// The sequence number the server expected.
        expected: u64,
    },
    /// Signing failed with a specific error message.
    Other {
        /// Error message describing the failure.
        message: String,
    },
    /// Transport or RPC error while signing.
    NetworkError,
}

impl SigningError {
    fn label(&self) -> String {
        match self {
            SigningError::SequenceMismatch { expected } => {
                format!("SequenceMismatch expected={}", expected)
            }
            SigningError::Other { message } => format!("Other msg={}", message),
            SigningError::NetworkError => "NetworkError".to_string(),
        }
    }
}

/// A signing failure containing both mapped and original errors.
#[derive(Debug, Clone)]
pub struct SigningFailure<T> {
    /// The categorized error for internal handling.
    pub mapped_error: SigningError,
    /// The original error from the underlying transport or node.
    pub original_error: T,
}

impl<T: fmt::Debug> SigningFailure<T> {
    fn label(&self) -> String {
        format!(
            "{} original={:?}",
            self.mapped_error.label(),
            self.original_error
        )
    }
}

/// A confirmation failure wrapping the rejection reason.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ConfirmFailure {
    reason: RejectionReason,
}

/// Reason a transaction was rejected during confirmation.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum RejectionReason {
    /// The transaction had a sequence mismatch.
    SequenceMismatch {
        /// The sequence number the node expected.
        expected: u64,
        /// The node that reported the mismatch.
        node_id: NodeId,
    },
    /// The transaction was not found (not submitted or expired).
    TxNotSubmitted {
        /// The expected sequence at the time of check.
        expected: u64,
        /// The node that reported the issue.
        node_id: NodeId,
    },
    /// Rejected for another reason.
    OtherReason {
        /// Error code from the node.
        error_code: ErrorCode,
        /// Error message from the node.
        message: String,
        /// The node that rejected the transaction.
        node_id: NodeId,
    },
}

/// Backend trait for submitting and confirming transactions on a node.
///
/// Implementations handle the actual network communication with blockchain nodes.
#[async_trait]
pub trait TxServer: Send + Sync {
    /// The transaction identifier type returned by the node.
    type TxId: TxIdT + Eq + StdHash + Send + Sync + 'static;
    /// Information returned when a transaction is confirmed.
    type ConfirmInfo: Clone + Send + Sync + 'static;
    /// The request type for transactions.
    type TxRequest: Send + Sync + 'static;
    /// The error type for submission failures.
    type SubmitError: Clone + Send + Sync + fmt::Debug + 'static;
    /// The raw response type from status queries.
    type ConfirmResponse: Clone + Send + Sync + 'static;

    /// Submit signed bytes with the given sequence, returning a server TxId.
    async fn submit(
        &self,
        tx_bytes: Arc<Vec<u8>>,
        sequence: u64,
    ) -> TxSubmitResult<Self::TxId, Self::SubmitError>;
    /// Batch status lookup for submitted TxIds.
    async fn status_batch(
        &self,
        ids: Vec<Self::TxId>,
    ) -> TxConfirmResult<
        Vec<(
            Self::TxId,
            TxStatus<Self::ConfirmInfo, Self::ConfirmResponse>,
        )>,
    >;
    /// Status lookup for submitted TxId.
    async fn status(
        &self,
        id: Self::TxId,
    ) -> TxConfirmResult<TxStatus<Self::ConfirmInfo, Self::ConfirmResponse>>;
    /// Fetch current sequence for the account (used by some implementations).
    #[allow(dead_code)]
    async fn current_sequence(&self) -> Result<u64>;
    /// Simulate a transaction and sign it with the given sequence, returning a signed transaction.
    async fn simulate_and_sign(
        &self,
        req: Arc<Self::TxRequest>,
        sequence: u64,
    ) -> Result<
        Transaction<Self::TxId, Self::ConfirmInfo, Self::ConfirmResponse, Self::SubmitError>,
        SigningFailure<Self::SubmitError>,
    >;
}

#[derive(Debug)]
enum NodeEvent<TxId: TxIdT, ConfirmInfo, ConfirmResponse, SubmitErr> {
    NodeResponse {
        node_id: NodeId,
        response: NodeResponse<TxId, ConfirmInfo, ConfirmResponse, SubmitErr>,
    },
    NodeStop,
}

impl<TxId: TxIdT, ConfirmInfo, ConfirmResponse, SubmitErr: fmt::Debug>
    NodeEvent<TxId, ConfirmInfo, ConfirmResponse, SubmitErr>
{
    fn summary(&self) -> String {
        match self {
            NodeEvent::NodeResponse { response, .. } => match response {
                NodeResponse::Submission { sequence, result } => match result {
                    Ok(_) => format!("NodeResponse::Submission seq={} Ok", sequence),
                    Err(err) => format!(
                        "NodeResponse::Submission seq={} Err {}",
                        sequence,
                        err.label()
                    ),
                },
                NodeResponse::Confirmation { response, .. } => match response {
                    Ok(ConfirmationResponse::Batch { statuses }) => {
                        format!("NodeResponse::Confirmation Batch {}", statuses.len())
                    }
                    Ok(ConfirmationResponse::Single { .. }) => {
                        "NodeResponse::Confirmation Single".to_string()
                    }
                    Err(err) => format!("NodeResponse::Confirmation Err {}", err),
                },
                NodeResponse::Signing { sequence, result } => match result {
                    Ok(_) => format!("NodeResponse::Signing seq={} Ok", sequence),
                    Err(err) => {
                        format!("NodeResponse::Signing seq={} Err {}", sequence, err.label())
                    }
                },
            },
            NodeEvent::NodeStop => "NodeStop".to_string(),
        }
    }
}

#[derive(Debug)]
enum NodeResponse<TxId: TxIdT, ConfirmInfo, ConfirmResponse, SubmitErr> {
    Submission {
        sequence: u64,
        result: TxSubmitResult<TxId, SubmitErr>,
    },
    Confirmation {
        state_epoch: u64,
        response: TxConfirmResult<ConfirmationResponse<TxId, ConfirmInfo, ConfirmResponse>>,
    },
    Signing {
        sequence: u64,
        result: TxSigningResult<TxId, ConfirmInfo, ConfirmResponse, SubmitErr>,
    },
}

#[derive(Debug)]
enum ConfirmationResponse<TxId, ConfirmInfo, ConfirmResponse> {
    Batch {
        statuses: Vec<(TxId, TxStatus<ConfirmInfo, ConfirmResponse>)>,
    },
    Single {
        id: TxId,
        status: TxStatus<ConfirmInfo, ConfirmResponse>,
    },
}

struct SubmissionResult<TxId, SubmitErr> {
    node_id: NodeId,
    sequence: u64,
    result: TxSubmitResult<TxId, SubmitErr>,
}

struct ConfirmationResult<TxId, ConfirmInfo, ConfirmResponse> {
    node_id: NodeId,
    state_epoch: u64,
    response: TxConfirmResult<ConfirmationResponse<TxId, ConfirmInfo, ConfirmResponse>>,
}

type PendingRequest<S> = RequestWithChannels<
    <S as TxServer>::TxId,
    <S as TxServer>::ConfirmInfo,
    <S as TxServer>::ConfirmResponse,
    <S as TxServer>::SubmitError,
    <S as TxServer>::TxRequest,
>;
type NodeEventFor<S> = NodeEvent<
    <S as TxServer>::TxId,
    <S as TxServer>::ConfirmInfo,
    <S as TxServer>::ConfirmResponse,
    <S as TxServer>::SubmitError,
>;
type SigningResultFor<S> = SigningResult<
    <S as TxServer>::TxId,
    <S as TxServer>::ConfirmInfo,
    <S as TxServer>::ConfirmResponse,
    <S as TxServer>::SubmitError,
>;
type StopErrorFor<S> = StopError<
    <S as TxServer>::SubmitError,
    <S as TxServer>::ConfirmInfo,
    <S as TxServer>::ConfirmResponse,
>;
type TxBuf<S> = tx_buffer::TxBuffer<
    <S as TxServer>::TxId,
    <S as TxServer>::ConfirmInfo,
    <S as TxServer>::ConfirmResponse,
    <S as TxServer>::SubmitError,
    PendingRequest<S>,
>;
type WorkerMutations<S> = Vec<
    WorkerMutation<
        <S as TxServer>::TxId,
        <S as TxServer>::ConfirmInfo,
        <S as TxServer>::ConfirmResponse,
        <S as TxServer>::SubmitError,
    >,
>;
type SubmissionFuture<S> = BoxFuture<
    'static,
    Option<SubmissionResult<<S as TxServer>::TxId, <S as TxServer>::SubmitError>>,
>;
type ConfirmationFuture<S> = BoxFuture<
    'static,
    Option<
        ConfirmationResult<
            <S as TxServer>::TxId,
            <S as TxServer>::ConfirmInfo,
            <S as TxServer>::ConfirmResponse,
        >,
    >,
>;
type SigningFuture<S> = BoxFuture<
    'static,
    Option<
        SigningResult<
            <S as TxServer>::TxId,
            <S as TxServer>::ConfirmInfo,
            <S as TxServer>::ConfirmResponse,
            <S as TxServer>::SubmitError,
        >,
    >,
>;

/// Background worker that processes transaction signing, submission, and confirmation.
///
/// Maintains per-node state machines and coordinates submissions across multiple nodes.
/// Handles sequence mismatches by entering recovery mode to re-sync with the network.
pub struct TransactionWorker<S: TxServer> {
    nodes: NodeManager<S>,
    servers: HashMap<NodeId, Arc<S>>,
    transactions: TxBuf<S>,
    add_tx_rx: mpsc::Receiver<PendingRequest<S>>,
    submissions: FuturesUnordered<SubmissionFuture<S>>,
    confirmations: FuturesUnordered<ConfirmationFuture<S>>,
    signing: Option<SigningFuture<S>>,
    confirm_ticker: Interval,
    confirm_interval: Duration,
    max_status_batch: usize,
    should_confirm: bool,
}

/// Return type for [`TransactionWorker::new`].
pub type WorkerPair<S> = (
    TxSubmitter<
        <S as TxServer>::TxId,
        <S as TxServer>::ConfirmInfo,
        <S as TxServer>::ConfirmResponse,
        <S as TxServer>::SubmitError,
        <S as TxServer>::TxRequest,
    >,
    TransactionWorker<S>,
);

impl<S: TxServer + 'static> TransactionWorker<S> {
    /// Create a submitter/worker pair with initial confirmed sequence and queue capacity.
    ///
    /// # Notes
    /// - `confirmed_sequence` is the last confirmed sequence for the signer.
    /// - `start_sequence` is derived as `confirmed_sequence + 1` or `0` when unknown.
    /// - `confirm_interval` drives periodic confirmation polling.
    pub fn new(
        nodes: HashMap<NodeId, Arc<S>>,
        confirm_interval: Duration,
        max_status_batch: usize,
        confirmed_sequence: Option<u64>,
        add_tx_capacity: usize,
    ) -> WorkerPair<S> {
        let (add_tx_tx, add_tx_rx) = mpsc::channel(add_tx_capacity);
        let manager = TxSubmitter {
            add_tx: add_tx_tx.clone(),
        };
        let node_ids = nodes.keys().cloned().collect::<Vec<_>>();
        let node_manager = NodeManager::new(node_ids, confirmed_sequence);
        let transactions = tx_buffer::TxBuffer::new(confirmed_sequence);
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
                should_confirm: false,
            },
        )
    }

    fn enqueue_pending(&mut self, request: PendingRequest<S>) -> Result<()> {
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
        let mut current_event: Option<NodeEventFor<S>> = None;
        let mut shutdown_seen = false;

        loop {
            let plans = self.nodes.plan(
                &self.transactions,
                self.max_status_batch,
                self.should_confirm,
            );
            if self.should_confirm {
                self.should_confirm = false;
            }
            self.execute_plans(plans, shutdown.clone());

            if let Some(event) = current_event.take() {
                let mutations = self.nodes.apply_event(event, &self.transactions);
                let stop = self.apply_mutations(mutations)?;
                if stop {
                    break;
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
                    }
                }
                _ = self.confirm_ticker.tick() => {
                    self.should_confirm = true;
                }
                result = self.submissions.next(), if !self.submissions.is_empty() => {
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
                result = self.confirmations.next(), if !self.confirmations.is_empty() => {
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
                    spawn_task(&mut self.submissions, token.clone(), async move |tx| {
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
                    spawn_task(&mut self.confirmations, token.clone(), async move |tx| {
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
                    spawn_task(&mut self.confirmations, token.clone(), async move |tx| {
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

    fn apply_mutations(&mut self, mutations: WorkerMutations<S>) -> Result<bool> {
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
                    let fatal = self.nodes.tail_stop_error();
                    self.finalize_remaining(fatal);
                    return Ok(true);
                }
            }
        }
        if let Some(limit) = self.nodes.min_confirmed_non_stopped() {
            let before = self.transactions.confirmed_seq();
            debug!(
                before = ?before,
                limit,
                max_seq = ?self.transactions.max_seq(),
                "clear_confirmed_up_to candidate"
            );
            self.clear_confirmed_up_to(limit);
            let after = self.transactions.confirmed_seq();
            debug!(before = ?before, after = ?after, limit, "clear_confirmed_up_to result");
        }
        Ok(false)
    }

    fn push_signing(&mut self) -> oneshot::Sender<SigningResultFor<S>> {
        let (tx, rx) = oneshot::channel();
        self.signing = Some(async move { rx.await.ok() }.boxed());
        tx
    }

    fn enqueue_signed(
        &mut self,
        tx: Transaction<S::TxId, S::ConfirmInfo, S::ConfirmResponse, S::SubmitError>,
        signed: oneshot::Sender<Result<()>>,
    ) -> Result<()> {
        let exp_seq = self.transactions.next_sequence();
        let tx_sequence = tx.sequence;
        if self.transactions.add_transaction(tx).is_err() {
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
        tx.notify_confirmed(Ok(info));
    }

    fn finalize_remaining(&mut self, fatal: Option<StopErrorFor<S>>) {
        for pending in self.transactions.drain_pending() {
            let _ = pending.sign_tx.send(Err(Error::TxWorkerStopped));
            let _ = pending.submit_tx.send(Err(Error::TxWorkerStopped));
            let _ = pending.confirm_tx.send(Err(StopError::WorkerStopped));
        }
        loop {
            let next = self
                .transactions
                .confirmed_seq()
                .map(|seq| seq.saturating_add(1))
                .unwrap_or(0);
            let Ok(mut tx) = self.transactions.confirm(next) else {
                break;
            };
            let status = fatal.clone().unwrap_or(StopError::WorkerStopped);
            if tx.id.is_none() {
                tx.notify_submitted(Err(Error::TxWorkerStopped));
            }
            tx.notify_confirmed(Err(status));
        }
    }

    fn clear_confirmed_up_to(&mut self, limit: u64) {
        loop {
            let next = self
                .transactions
                .confirmed_seq()
                .map(|seq| seq.saturating_add(1))
                .unwrap_or(0);
            if next > limit {
                break;
            }
            if self.transactions.confirm(next).is_err() {
                break;
            }
        }
    }
}

fn spawn_task<T, F, Fut>(
    unordered: &mut FuturesUnordered<BoxFuture<'static, Option<T>>>,
    token: CancellationToken,
    fut_fn: F,
) where
    T: Send + 'static,
    F: FnOnce(oneshot::Sender<T>) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    spawn_cancellable(token, fut_fn(tx));
    unordered.push(async move { rx.await.ok() }.boxed());
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
