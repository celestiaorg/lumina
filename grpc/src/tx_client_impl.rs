use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use lumina_utils::executor::spawn;
use prost::Message;
use tokio::sync::{Mutex, RwLock, oneshot};
use tokio_util::sync::CancellationToken;

use celestia_types::any::IntoProtobufAny;
use celestia_types::blob::{MsgPayForBlobs, RawBlobTx, RawMsgPayForBlobs};
use celestia_types::hash::Hash;
use celestia_types::state::ErrorCode;
use celestia_types::state::RawTxBody;
use celestia_types::state::auth::BaseAccount;

use crate::grpc::{BroadcastMode, GasEstimate, TxStatus as GrpcTxStatus, TxStatusResponse};
use crate::signer::sign_tx;

use crate::tx_client_v2::{
    ConfirmResult, NodeId, RejectionReason, SigningError, SigningFailure, StopError, SubmitError,
    SubmitFailure, Transaction, TransactionWorker, TxCallbacks, TxConfirmResult, TxPayload,
    TxRequest, TxServer, TxStatus, TxStatusKind, TxSubmitResult, TxSubmitter,
};
use crate::{Error, GrpcClient, Result, TxConfig, TxInfo};

const BLOB_TX_TYPE_ID: &str = "BLOB";
const SEQUENCE_ERROR_PAT: &str = "account sequence mismatch, expected ";
const DEFAULT_MAX_STATUS_BATCH: usize = 16;
const DEFAULT_QUEUE_CAPACITY: usize = 128;

pub struct ConfirmHandle<ConfirmInfo, ConfirmResponse> {
    pub hash: Hash,
    pub confirmed: oneshot::Receiver<ConfirmResult<ConfirmInfo, Arc<Error>, ConfirmResponse>>,
}

#[derive(Debug, Clone)]
pub enum TxUserError {
    Rejected { reason: RejectionReason },
    NotFound,
}

impl fmt::Display for TxUserError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TxUserError::Rejected { reason } => write!(f, "Transaction rejected: {:?}", reason),
            TxUserError::NotFound => write!(f, "Transaction not found"),
        }
    }
}

impl ConfirmHandle<TxConfirmInfo, TxStatusResponse> {
    pub async fn confirm(
        self,
    ) -> std::result::Result<TxInfo, StopError<Arc<Error>, TxConfirmInfo, TxStatusResponse>> {
        // receiver errors or internal errors are invariant violations
        let info = self.confirmed.await.expect("confirm receiver dropped")?;
        Ok(info.info)
    }
}

pub struct TxServiceConfig {
    pub nodes: Vec<(NodeId, GrpcClient)>,
    pub confirm_interval: Duration,
    pub max_status_batch: usize,
    pub queue_capacity: usize,
}

impl TxServiceConfig {
    pub fn new(nodes: Vec<(NodeId, GrpcClient)>) -> Self {
        Self {
            nodes,
            confirm_interval: Duration::from_millis(TxConfig::default().confirmation_interval_ms),
            max_status_batch: DEFAULT_MAX_STATUS_BATCH,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }
}

pub struct TransactionService {
    inner: Arc<TransactionServiceInner>,
}

struct TransactionServiceInner {
    submitter: RwLock<TxSubmitter<Hash, TxConfirmInfo, TxStatusResponse, Arc<Error>, TxRequest>>,
    worker: Mutex<Option<WorkerHandle>>,
    clients: HashMap<NodeId, GrpcClient>,
    primary_client: GrpcClient,
    account: BaseAccount,
    confirm_interval: Duration,
    max_status_batch: usize,
    queue_capacity: usize,
}

struct WorkerHandle {
    done_rx: oneshot::Receiver<Result<()>>,
}

impl WorkerHandle {
    fn is_finished(&mut self) -> bool {
        match self.done_rx.try_recv() {
            Ok(_) => true,
            Err(oneshot::error::TryRecvError::Closed) => true,
            Err(oneshot::error::TryRecvError::Empty) => false,
        }
    }
}

impl TransactionService {
    pub async fn new(config: TxServiceConfig) -> Result<Self> {
        let Some((_, client)) = config.nodes.first() else {
            return Err(Error::UnexpectedResponseType(
                "no grpc clients provided".to_string(),
            ));
        };
        let client = client.clone();
        let address = client.get_account_address().ok_or(Error::MissingSigner)?;
        let account = client.get_account(&address).await?;
        let account = BaseAccount::from(account);

        let clients: HashMap<NodeId, GrpcClient> = config.nodes.into_iter().collect();
        let (submitter, worker_handle) = Self::spawn_worker(
            account.clone(),
            &clients,
            client.clone(),
            config.confirm_interval,
            config.max_status_batch,
            config.queue_capacity,
        )
        .await?;

        Ok(Self {
            inner: Arc::new(TransactionServiceInner {
                submitter: RwLock::new(submitter),
                worker: Mutex::new(Some(worker_handle)),
                clients,
                primary_client: client,
                account,
                confirm_interval: config.confirm_interval,
                max_status_batch: config.max_status_batch,
                queue_capacity: config.queue_capacity,
            }),
        })
    }

    pub async fn submit(
        &self,
        request: TxRequest,
    ) -> Result<ConfirmHandle<TxConfirmInfo, TxStatusResponse>> {
        let submitter = self.inner.submitter.read().await.clone();
        let handle = submitter.add_tx(request).await?;
        match handle.signed.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err),
            Err(_) => return Err(Error::TxWorkerStopped),
        }
        match handle.submitted.await {
            Ok(Ok(hash)) => Ok(ConfirmHandle {
                hash,
                confirmed: handle.confirmed,
            }),
            Ok(Err(err)) => Err(err),
            Err(_) => Err(Error::TxWorkerStopped),
        }
    }

    pub async fn submit_restart(
        &self,
        request: TxRequest,
    ) -> Result<ConfirmHandle<TxConfirmInfo, TxStatusResponse>> {
        let retry_request = request.clone();
        match self.submit(request).await {
            Ok(handle) => Ok(handle),
            Err(Error::TxWorkerStopped) => {
                self.recreate_worker().await?;
                self.submit(retry_request).await
            }
            Err(err) => Err(err),
        }
    }

    pub async fn recreate_worker(&self) -> Result<()> {
        {
            let mut worker_guard = self.inner.worker.lock().await;
            if let Some(worker) = worker_guard.as_mut()
                && !worker.is_finished()
            {
                return Err(Error::TxWorkerRunning);
            }
        }
        let (submitter, worker_handle) = Self::spawn_worker(
            self.inner.account.clone(),
            &self.inner.clients,
            self.inner.primary_client.clone(),
            self.inner.confirm_interval,
            self.inner.max_status_batch,
            self.inner.queue_capacity,
        )
        .await?;
        let mut submitter_guard = self.inner.submitter.write().await;
        let mut worker_guard = self.inner.worker.lock().await;
        *submitter_guard = submitter;
        *worker_guard = Some(worker_handle);

        Ok(())
    }

    async fn spawn_worker(
        account: BaseAccount,
        clients: &HashMap<NodeId, GrpcClient>,
        primary_client: GrpcClient,
        confirm_interval: Duration,
        max_status_batch: usize,
        queue_capacity: usize,
    ) -> Result<(
        TxSubmitter<Hash, TxConfirmInfo, TxStatusResponse, Arc<Error>, TxRequest>,
        WorkerHandle,
    )> {
        let start_sequence = current_sequence(&primary_client).await?;
        let nodes = clients
            .iter()
            .map(|(node_id, client)| {
                (
                    node_id.clone(),
                    Arc::new(NodeClient {
                        node_id: node_id.clone(),
                        client: client.clone(),
                        account: account.clone(),
                    }),
                )
            })
            .collect::<HashMap<_, _>>();
        let (submitter, mut worker) = TransactionWorker::new(
            nodes,
            confirm_interval,
            max_status_batch,
            start_sequence,
            queue_capacity,
        );

        let worker_shutdown = CancellationToken::new();
        let (done_tx, done_rx) = oneshot::channel();
        spawn(async move {
            let result = worker.process(worker_shutdown).await;
            let _ = done_tx.send(result);
        });

        Ok((submitter, WorkerHandle { done_rx }))
    }
}

#[derive(Clone)]
struct NodeClient {
    node_id: NodeId,
    client: GrpcClient,
    account: BaseAccount,
}

#[async_trait]
impl TxServer for NodeClient {
    type TxId = Hash;
    type ConfirmInfo = TxConfirmInfo;
    type TxRequest = TxRequest;
    type SubmitError = Arc<Error>;
    type ConfirmResponse = TxStatusResponse;

    async fn submit(
        &self,
        tx_bytes: Arc<Vec<u8>>,
        _sequence: u64,
    ) -> TxSubmitResult<Self::TxId, Self::SubmitError> {
        let resp = match self
            .client
            .broadcast_tx(tx_bytes.to_vec(), BroadcastMode::Sync)
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                return Err(SubmitFailure {
                    mapped_error: SubmitError::NetworkError,
                    original_error: Arc::new(err),
                });
            }
        };

        if resp.code == ErrorCode::Success {
            return Ok(resp.txhash);
        }

        let original_error = Arc::new(Error::TxBroadcastFailed(
            resp.txhash,
            resp.code,
            resp.raw_log.clone(),
        ));
        Err(SubmitFailure {
            mapped_error: map_submit_error(resp.code, &resp.raw_log),
            original_error,
        })
    }

    async fn status_batch(
        &self,
        ids: Vec<Self::TxId>,
    ) -> TxConfirmResult<
        Vec<(
            Self::TxId,
            TxStatus<Self::ConfirmInfo, Self::ConfirmResponse>,
        )>,
    > {
        let response = self.client.tx_status_batch(ids.clone()).await?;
        let mut response_map = HashMap::new();
        for result in response.statuses {
            response_map.insert(result.hash, result.status);
        }

        let mut statuses = Vec::with_capacity(ids.len());
        for hash in ids {
            match response_map.remove(&hash) {
                Some(status) => {
                    let mapped = map_status_response(hash, status, self.node_id.as_ref())?;
                    statuses.push((hash, mapped));
                }
                None => {
                    return Err(Error::UnexpectedResponseType(format!(
                        "missing status for tx {:?}",
                        hash
                    )));
                }
            }
        }

        Ok(statuses)
    }

    async fn status(
        &self,
        id: Self::TxId,
    ) -> TxConfirmResult<TxStatus<Self::ConfirmInfo, Self::ConfirmResponse>> {
        let response = self.client.tx_status(id).await?;
        map_status_response(id, response, self.node_id.as_ref())
    }

    async fn current_sequence(&self) -> Result<u64> {
        current_sequence(&self.client).await
    }

    async fn simulate_and_sign(
        &self,
        req: Arc<Self::TxRequest>,
        sequence: u64,
    ) -> std::result::Result<
        Transaction<Self::TxId, Self::ConfirmInfo, Self::ConfirmResponse, Self::SubmitError>,
        SigningFailure<Self::SubmitError>,
    > {
        sign_with_client(self.account.clone(), &self.client, req.as_ref(), sequence)
            .await
            .map_err(map_signing_failure)
    }
}

async fn sign_with_client(
    mut account: BaseAccount,
    client: &GrpcClient,
    request: &TxRequest,
    sequence: u64,
) -> Result<Transaction<Hash, TxConfirmInfo, TxStatusResponse, Arc<Error>>> {
    let (pubkey, signer) = client.signer()?;
    account.sequence = sequence;

    let chain_id = client.chain_id().await?;
    let cfg = &request.cfg;
    let (tx_body, blobs) = match &request.tx {
        TxPayload::Blobs(blobs) => {
            let pfb =
                MsgPayForBlobs::new(blobs, account.address).map_err(Error::CelestiaTypesError)?;
            let tx_body = RawTxBody {
                messages: vec![RawMsgPayForBlobs::from(pfb).into_any()],
                memo: cfg.memo.clone().unwrap_or_default(),
                ..RawTxBody::default()
            };
            (tx_body, Some(blobs))
        }
        TxPayload::Tx(body) => (body.clone(), None),
    };

    let (gas_limit, gas_price) = match cfg.gas_limit {
        Some(gas_limit) => {
            let gas_price = match cfg.gas_price {
                Some(price) => price,
                None => client.estimate_gas_price(cfg.priority).await?,
            };
            (gas_limit, gas_price)
        }
        None => {
            let probe_tx = sign_tx(
                tx_body.clone(),
                chain_id.clone(),
                &account,
                &pubkey,
                &signer,
                0,
                1,
            )
            .await?;
            let GasEstimate { price, usage } = client
                .estimate_gas_price_and_usage(cfg.priority, probe_tx.encode_to_vec())
                .await?;
            let gas_price = cfg.gas_price.unwrap_or(price);
            (usage, gas_price)
        }
    };
    let fee = (gas_limit as f64 * gas_price).ceil() as u64;

    let tx = sign_tx(
        tx_body, chain_id, &account, &pubkey, &signer, gas_limit, fee,
    )
    .await?;
    let bytes = match blobs {
        Some(blobs) => {
            let blob_tx = RawBlobTx {
                tx: tx.encode_to_vec(),
                blobs: blobs.iter().cloned().map(Into::into).collect(),
                type_id: BLOB_TX_TYPE_ID.to_string(),
            };
            blob_tx.encode_to_vec()
        }
        None => tx.encode_to_vec(),
    };
    Ok(Transaction {
        sequence,
        bytes: Arc::new(bytes),
        callbacks: TxCallbacks::default(),
        id: None,
    })
}

#[derive(Debug, Clone)]
pub struct TxConfirmInfo {
    pub info: TxInfo,
    pub execution_code: ErrorCode,
}

fn map_status_response(
    hash: Hash,
    response: TxStatusResponse,
    node_id: &str,
) -> Result<TxStatus<TxConfirmInfo, TxStatusResponse>> {
    let original_response = response.clone();
    match response.status {
        GrpcTxStatus::Committed => Ok(TxStatus::new(
            TxStatusKind::Confirmed {
                info: TxConfirmInfo {
                    info: TxInfo {
                        hash,
                        height: response.height.value(),
                    },
                    execution_code: response.execution_code,
                },
            },
            original_response,
        )),
        GrpcTxStatus::Rejected => {
            if is_wrong_sequence(response.execution_code) {
                let Some(expected) = extract_sequence_on_mismatch(&response.error) else {
                    return Ok(TxStatus::new(
                        TxStatusKind::Rejected {
                            reason: RejectionReason::OtherReason {
                                error_code: response.execution_code,
                                message: response.error.clone(),
                                node_id: Arc::from(node_id),
                            },
                        },
                        original_response,
                    ));
                };
                Ok(TxStatus::new(
                    TxStatusKind::Rejected {
                        reason: RejectionReason::SequenceMismatch {
                            expected,
                            node_id: Arc::from(node_id),
                        },
                    },
                    original_response,
                ))
            } else {
                Ok(TxStatus::new(
                    TxStatusKind::Rejected {
                        reason: RejectionReason::OtherReason {
                            error_code: response.execution_code,
                            message: response.error.clone(),
                            node_id: Arc::from(node_id),
                        },
                    },
                    original_response,
                ))
            }
        }
        GrpcTxStatus::Evicted => Ok(TxStatus::new(TxStatusKind::Evicted, original_response)),
        GrpcTxStatus::Pending => Ok(TxStatus::new(TxStatusKind::Pending, original_response)),
        GrpcTxStatus::Unknown => Ok(TxStatus::new(TxStatusKind::Unknown, original_response)),
    }
}

async fn current_sequence(client: &GrpcClient) -> Result<u64> {
    let address = client.get_account_address().ok_or(Error::MissingSigner)?;
    let account = client.get_account(&address).await?;
    Ok(account.sequence)
}

fn map_submit_error(code: ErrorCode, message: &str) -> SubmitError {
    if is_wrong_sequence(code)
        && let Some(expected) = extract_sequence_on_mismatch(message)
    {
        return SubmitError::SequenceMismatch { expected };
    }
    if code == ErrorCode::InsufficientFee
        && let Some(expected_fee) = extract_expected_fee(message)
    {
        return SubmitError::InsufficientFee {
            expected_fee,
            message: message.to_string(),
        };
    }

    match code {
        ErrorCode::MempoolIsFull => SubmitError::MempoolIsFull,
        ErrorCode::TxInMempoolCache => SubmitError::TxInMempoolCache,
        _ => SubmitError::Other {
            error_code: code,
            message: message.to_string(),
        },
    }
}

fn extract_expected_fee(message: &str) -> Option<u64> {
    let patterns = ["required fee:", "required:"];
    for pattern in patterns {
        if let Some(index) = message.find(pattern) {
            let rest = &message[index + pattern.len()..];
            return parse_leading_digits(rest);
        }
    }
    None
}

fn parse_leading_digits(input: &str) -> Option<u64> {
    let digits: String = input
        .trim_start()
        .chars()
        .take_while(|c| c.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn map_signing_failure(err: Error) -> SigningFailure<Arc<Error>> {
    if err.is_network_error() {
        return SigningFailure {
            mapped_error: SigningError::NetworkError,
            original_error: Arc::new(err),
        };
    }
    if let Some(expected) = extract_sequence_on_mismatch(&err.to_string()) {
        return SigningFailure {
            mapped_error: SigningError::SequenceMismatch { expected },
            original_error: Arc::new(err),
        };
    }
    SigningFailure {
        mapped_error: SigningError::Other {
            message: err.to_string(),
        },
        original_error: Arc::new(err),
    }
}

fn is_wrong_sequence(code: ErrorCode) -> bool {
    code == ErrorCode::InvalidSequence || code == ErrorCode::WrongSequence
}

fn extract_sequence_on_mismatch(msg: &str) -> Option<u64> {
    msg.contains(SEQUENCE_ERROR_PAT)
        .then(|| extract_sequence(msg))
        .and_then(|res| res.ok())
}

fn extract_sequence(msg: &str) -> Result<u64> {
    let (_, msg_with_sequence) = msg
        .split_once(SEQUENCE_ERROR_PAT)
        .ok_or_else(|| Error::SequenceParsingFailed(msg.into()))?;
    let (sequence, _) = msg_with_sequence
        .split_once(',')
        .ok_or_else(|| Error::SequenceParsingFailed(msg.into()))?;
    sequence
        .parse()
        .map_err(|_| Error::SequenceParsingFailed(msg.into()))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::ops::RangeInclusive;
    use std::sync::Arc;

    use super::*;
    use crate::GrpcClient;
    use crate::test_utils::{CELESTIA_GRPC_URL, load_account, new_tx_client};
    use celestia_types::nmt::Namespace;
    use celestia_types::{AppVersion, Blob};
    use lumina_utils::test_utils::async_test;
    use rand::{Rng, RngCore};

    #[async_test]
    async fn submit_with_worker_and_confirm() {
        let (_lock, _client) = new_tx_client().await;
        let account = load_account();
        let client = GrpcClient::builder()
            .url(CELESTIA_GRPC_URL)
            .signer_keypair(account.signing_key)
            .build()
            .unwrap();

        let service =
            TransactionService::new(TxServiceConfig::new(vec![(Arc::from("default"), client)]))
                .await
                .unwrap();
        let handle = service
            .submit(TxRequest::blobs(
                vec![random_blob(10..=1000)],
                TxConfig::default(),
            ))
            .await
            .unwrap();
        let info = handle.confirm().await.unwrap();
        assert!(info.height > 0);
    }

    fn random_blob(size: RangeInclusive<usize>) -> Blob {
        let rng = &mut rand::thread_rng();

        let mut ns_bytes = vec![0u8; 10];
        rng.fill_bytes(&mut ns_bytes);
        let namespace = Namespace::new_v0(&ns_bytes).unwrap();

        let len = rng.gen_range(size);
        let mut blob = vec![0; len];
        rng.fill_bytes(&mut blob);
        blob.resize(len, 1);

        Blob::new(namespace, blob, None, AppVersion::latest()).unwrap()
    }

    #[test]
    fn extract_expected_fee_parses_required_fee() {
        let message = "insufficient fee; got: 123utest required fee: 456utest";
        assert_eq!(super::extract_expected_fee(message), Some(456));
    }

    #[test]
    fn extract_expected_fee_parses_required_fallback() {
        let message = "insufficient fee; got: 123utest required: 789utest";
        assert_eq!(super::extract_expected_fee(message), Some(789));
    }

    #[test]
    fn extract_expected_fee_returns_none_when_missing() {
        let message = "insufficient fee; got: 123utest";
        assert_eq!(super::extract_expected_fee(message), None);
    }
}
