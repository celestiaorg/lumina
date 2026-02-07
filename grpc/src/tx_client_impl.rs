use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use k256::ecdsa::VerifyingKey;
use lumina_utils::executor::spawn;
use prost::Message;
use tendermint::chain::Id;
use tokio::sync::{Mutex, OnceCell, RwLock, oneshot};
use tokio_util::sync::CancellationToken;

use celestia_types::any::IntoProtobufAny;
use celestia_types::blob::{MsgPayForBlobs, RawBlobTx, RawMsgPayForBlobs};
use celestia_types::hash::Hash;
use celestia_types::state::ErrorCode;
use celestia_types::state::RawTxBody;
use celestia_types::state::auth::BaseAccount;

use crate::grpc::{
    BroadcastMode, GasEstimate, TxPriority, TxStatus as GrpcTxStatus, TxStatusResponse,
};
use crate::signer::{BoxedDocSigner, sign_tx};

use crate::tx_client_v2::{
    NodeId, RejectionReason, SigningFailure, SubmitFailure, Transaction, TransactionWorker,
    TxCallbacks, TxConfirmResult, TxPayload, TxRequest, TxServer, TxStatus, TxSubmitResult,
    TxSubmitter,
};
use crate::{Error, GrpcClient, Result, TxConfig, TxInfo};

const BLOB_TX_TYPE_ID: &str = "BLOB";
const SEQUENCE_ERROR_PAT: &str = "account sequence mismatch, expected ";
const DEFAULT_MAX_STATUS_BATCH: usize = 16;
const DEFAULT_QUEUE_CAPACITY: usize = 128;

#[async_trait]
pub(crate) trait SignContext: Send + Sync {
    async fn get_account(&self) -> Result<BaseAccount>;
    async fn chain_id(&self) -> Result<Id>;
    async fn estimate_gas_price(&self, priority: TxPriority) -> Result<f64>;
    async fn estimate_gas_price_and_usage(
        &self,
        priority: TxPriority,
        tx_bytes: Vec<u8>,
    ) -> Result<GasEstimate>;
}

pub(crate) struct SignFnBuilder {
    context: Arc<dyn SignContext>,
    pubkey: VerifyingKey,
    signer: Arc<BoxedDocSigner>,
}

pub(crate) struct GrpcSignContext {
    client: GrpcClient,
}

impl GrpcSignContext {
    pub(crate) fn new(client: GrpcClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl SignContext for GrpcSignContext {
    async fn get_account(&self) -> Result<BaseAccount> {
        let address = self
            .client
            .get_account_address()
            .ok_or(Error::MissingSigner)?;
        let account = self.client.get_account(&address).await?;
        Ok(account.into())
    }

    async fn chain_id(&self) -> Result<Id> {
        self.client.chain_id().await
    }

    async fn estimate_gas_price(&self, priority: TxPriority) -> Result<f64> {
        self.client.estimate_gas_price(priority).await
    }

    async fn estimate_gas_price_and_usage(
        &self,
        priority: TxPriority,
        tx_bytes: Vec<u8>,
    ) -> Result<GasEstimate> {
        self.client
            .estimate_gas_price_and_usage(priority, tx_bytes)
            .await
    }
}

impl SignFnBuilder {
    pub(crate) fn new(
        context: Arc<dyn SignContext>,
        pubkey: VerifyingKey,
        signer: Arc<BoxedDocSigner>,
    ) -> Self {
        Self {
            context,
            pubkey,
            signer,
        }
    }

    pub(crate) fn build(self) -> Arc<BuiltSignFn> {
        Arc::new(BuiltSignFn {
            context: self.context,
            pubkey: self.pubkey,
            signer: self.signer,
            cached_account: OnceCell::new(),
            cached_chain_id: OnceCell::new(),
        })
    }
}

pub struct ConfirmHandle<ConfirmInfo> {
    pub hash: Hash,
    pub confirmed: oneshot::Receiver<Result<TxStatus<ConfirmInfo>>>,
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

impl ConfirmHandle<TxConfirmInfo> {
    pub async fn confirm(self) -> std::result::Result<TxInfo, TxUserError> {
        // receiver errors or internal errors are invariant violations
        let status = self
            .confirmed
            .await
            .expect("confirm receiver dropped")
            .expect("confirm error");
        match status {
            // TODO: handle execution errors as erros
            TxStatus::Confirmed { info } => Ok(info.info),
            TxStatus::Rejected { reason } => Err(TxUserError::Rejected { reason }),
            TxStatus::Evicted => {
                panic!("should not get evicted statuses in confirm")
            }
            TxStatus::Unknown | TxStatus::Pending => Err(TxUserError::NotFound),
        }
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
    submitter: RwLock<TxSubmitter<Hash, TxConfirmInfo, TxRequest>>,
    worker: Mutex<Option<WorkerHandle>>,
    clients: HashMap<NodeId, GrpcClient>,
    primary_client: GrpcClient,
    signer: Arc<BuiltSignFn>,
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
        let Some((_primary_id, primary_client)) = config.nodes.first() else {
            return Err(Error::UnexpectedResponseType(
                "no grpc clients provided".to_string(),
            ));
        };
        let primary_client = primary_client.clone();
        let clients: HashMap<NodeId, GrpcClient> = config.nodes.into_iter().collect();
        let context = Arc::new(GrpcSignContext::new(primary_client.clone()));
        let (pubkey, signer) = primary_client.signer()?;
        let signer = Arc::new(signer);
        let signer = SignFnBuilder::new(context, pubkey, signer).build();
        let (submitter, worker_handle) = Self::spawn_worker(
            &clients,
            primary_client.clone(),
            signer.clone(),
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
                primary_client,
                signer,
                confirm_interval: config.confirm_interval,
                max_status_batch: config.max_status_batch,
                queue_capacity: config.queue_capacity,
            }),
        })
    }

    pub async fn submit(&self, request: TxRequest) -> Result<ConfirmHandle<TxConfirmInfo>> {
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

    pub async fn submit_restart(&self, request: TxRequest) -> Result<ConfirmHandle<TxConfirmInfo>> {
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
        let mut worker_guard = self.inner.worker.lock().await;
        if let Some(worker) = worker_guard.as_mut() {
            if !worker.is_finished() {
                return Err(Error::TxWorkerRunning);
            }
        }
        let (submitter, worker_handle) = Self::spawn_worker(
            &self.inner.clients,
            self.inner.primary_client.clone(),
            self.inner.signer.clone(),
            self.inner.confirm_interval,
            self.inner.max_status_batch,
            self.inner.queue_capacity,
        )
        .await?;
        *worker_guard = Some(worker_handle);

        let mut submitter_guard = self.inner.submitter.write().await;
        *submitter_guard = submitter;

        Ok(())
    }

    async fn spawn_worker(
        clients: &HashMap<NodeId, GrpcClient>,
        primary_client: GrpcClient,
        signer: Arc<BuiltSignFn>,
        confirm_interval: Duration,
        max_status_batch: usize,
        queue_capacity: usize,
    ) -> Result<(TxSubmitter<Hash, TxConfirmInfo, TxRequest>, WorkerHandle)> {
        let start_sequence = current_sequence(&primary_client).await?;
        let nodes = clients
            .iter()
            .map(|(node_id, client)| {
                (
                    node_id.clone(),
                    Arc::new(NodeClient {
                        node_id: node_id.clone(),
                        client: client.clone(),
                        signer: signer.clone(),
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

pub(crate) struct BuiltSignFn {
    context: Arc<dyn SignContext>,
    pubkey: VerifyingKey,
    signer: Arc<BoxedDocSigner>,
    cached_account: OnceCell<BaseAccount>,
    cached_chain_id: OnceCell<Id>,
}

#[derive(Clone)]
struct NodeClient {
    node_id: NodeId,
    client: GrpcClient,
    signer: Arc<BuiltSignFn>,
}

impl BuiltSignFn {
    async fn sign(
        &self,
        sequence: u64,
        request: &TxRequest,
    ) -> Result<Transaction<Hash, TxConfirmInfo>> {
        let chain_id = self
            .cached_chain_id
            .get_or_try_init(|| async { self.context.chain_id().await })
            .await?
            .clone();
        let base = self
            .cached_account
            .get_or_try_init(|| async { self.context.get_account().await })
            .await?
            .clone();

        let mut account = base.clone();
        account.sequence = sequence;
        let cfg = &request.cfg;
        let (tx_body, blobs) = match &request.tx {
            TxPayload::Blobs(blobs) => {
                let pfb = MsgPayForBlobs::new(blobs, account.address)
                    .map_err(Error::CelestiaTypesError)?;
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
                    None => self.context.estimate_gas_price(cfg.priority).await?,
                };
                (gas_limit, gas_price)
            }
            None => {
                let probe_tx = sign_tx(
                    tx_body.clone(),
                    chain_id.clone(),
                    &account,
                    &self.pubkey,
                    &*self.signer,
                    0,
                    1,
                )
                .await?;
                let GasEstimate { price, usage } = self
                    .context
                    .estimate_gas_price_and_usage(cfg.priority, probe_tx.encode_to_vec())
                    .await?;
                let gas_price = cfg.gas_price.unwrap_or(price);
                (usage, gas_price)
            }
        };
        let fee = (gas_limit as f64 * gas_price).ceil() as u64;

        let tx = sign_tx(
            tx_body,
            chain_id,
            &account,
            &self.pubkey,
            &*self.signer,
            gas_limit,
            fee,
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
        // id is set later after submission succeeds (with server's hash)
        Ok(Transaction {
            sequence,
            bytes: Arc::new(bytes),
            callbacks: TxCallbacks::default(),
            id: None,
        })
    }
}

#[async_trait]
impl TxServer for NodeClient {
    type TxId = Hash;
    type ConfirmInfo = TxConfirmInfo;
    type TxRequest = TxRequest;

    async fn submit(&self, tx_bytes: Arc<Vec<u8>>, _sequence: u64) -> TxSubmitResult<Self::TxId> {
        let resp = self
            .client
            .broadcast_tx(tx_bytes.to_vec(), BroadcastMode::Sync)
            .await
            .map_err(|err| SubmitFailure::NetworkError { err: Arc::new(err) })?;

        if resp.code == ErrorCode::Success {
            return Ok(resp.txhash);
        }

        Err(map_submit_failure(resp.code, &resp.raw_log))
    }

    async fn status_batch(
        &self,
        ids: Vec<Self::TxId>,
    ) -> TxConfirmResult<Vec<(Self::TxId, TxStatus<Self::ConfirmInfo>)>> {
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
                    statuses.push((hash, TxStatus::Unknown));
                }
            }
        }

        Ok(statuses)
    }

    async fn status(&self, id: Self::TxId) -> TxConfirmResult<TxStatus<Self::ConfirmInfo>> {
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
    ) -> std::result::Result<Transaction<Self::TxId, Self::ConfirmInfo>, SigningFailure> {
        self.signer
            .sign(sequence, req.as_ref())
            .await
            .map_err(map_signing_failure)
    }
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
) -> Result<TxStatus<TxConfirmInfo>> {
    match response.status {
        GrpcTxStatus::Committed => Ok(TxStatus::Confirmed {
            info: TxConfirmInfo {
                info: TxInfo {
                    hash,
                    height: response.height.value(),
                },
                execution_code: response.execution_code,
            },
        }),
        GrpcTxStatus::Rejected => {
            if is_wrong_sequence(response.execution_code) {
                let Some(expected) = extract_sequence_on_mismatch(&response.error) else {
                    return Ok(TxStatus::Rejected {
                        reason: RejectionReason::OtherReason {
                            error_code: response.execution_code,
                            message: response.error.clone(),
                            node_id: Arc::from(node_id),
                        },
                    });
                };
                Ok(TxStatus::Rejected {
                    reason: RejectionReason::SequenceMismatch {
                        expected,
                        node_id: Arc::from(node_id),
                    },
                })
            } else {
                Ok(TxStatus::Rejected {
                    reason: RejectionReason::OtherReason {
                        error_code: response.execution_code,
                        message: response.error.clone(),
                        node_id: Arc::from(node_id),
                    },
                })
            }
        }
        GrpcTxStatus::Evicted => Ok(TxStatus::Evicted),
        GrpcTxStatus::Pending => Ok(TxStatus::Pending),
        GrpcTxStatus::Unknown => Ok(TxStatus::Unknown),
    }
}

async fn current_sequence(client: &GrpcClient) -> Result<u64> {
    let address = client.get_account_address().ok_or(Error::MissingSigner)?;
    let account = client.get_account(&address).await?;
    Ok(account.sequence)
}

fn map_submit_failure(code: ErrorCode, message: &str) -> SubmitFailure {
    if is_wrong_sequence(code) {
        if let Some(expected) = extract_sequence_on_mismatch(message) {
            return SubmitFailure::SequenceMismatch { expected };
        }
    }

    match code {
        ErrorCode::MempoolIsFull => SubmitFailure::MempoolIsFull,
        ErrorCode::TxInMempoolCache => SubmitFailure::TxInMempoolCache,
        _ => SubmitFailure::Other {
            error_code: code,
            message: message.to_string(),
        },
    }
}

fn map_signing_failure(err: Error) -> SigningFailure {
    if err.is_network_error() {
        return SigningFailure::NetworkError { err: Arc::new(err) };
    }
    if let Some(expected) = extract_sequence_on_mismatch(&err.to_string()) {
        return SigningFailure::SequenceMismatch { expected };
    }
    SigningFailure::Other {
        message: err.to_string(),
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

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;
    use std::sync::Arc;

    use super::*;
    use crate::GrpcClient;
    use crate::test_utils::{CELESTIA_GRPC_URL, load_account, new_tx_client};
    use async_trait::async_trait;
    use celestia_types::nmt::Namespace;
    use celestia_types::state::{AccAddress, RawTxBody};
    use celestia_types::{AppVersion, Blob};
    use k256::ecdsa::SigningKey;
    use lumina_utils::test_utils::async_test;
    use rand::rngs::OsRng;
    use rand::{Rng, RngCore};
    use tendermint::chain::Id;

    struct MockContext {
        account: BaseAccount,
        chain_id: Id,
        gas_price: f64,
    }

    #[async_trait]
    impl SignContext for MockContext {
        async fn get_account(&self) -> Result<BaseAccount> {
            Ok(self.account.clone())
        }

        async fn chain_id(&self) -> Result<Id> {
            Ok(self.chain_id.clone())
        }

        async fn estimate_gas_price(&self, _priority: TxPriority) -> Result<f64> {
            Ok(self.gas_price)
        }

        async fn estimate_gas_price_and_usage(
            &self,
            _priority: TxPriority,
            _tx_bytes: Vec<u8>,
        ) -> Result<GasEstimate> {
            Ok(GasEstimate {
                price: self.gas_price,
                usage: 123,
            })
        }
    }

    #[tokio::test]
    async fn signfn_builder_signs_tx_body() {
        let signing_key = SigningKey::random(&mut OsRng);
        let pubkey = signing_key.verifying_key().clone();
        let signer = Arc::new(BoxedDocSigner::new(signing_key.clone()));
        let address = AccAddress::from(pubkey);
        let account = BaseAccount {
            address,
            pub_key: None,
            account_number: 1,
            sequence: 0,
        };
        let context = Arc::new(MockContext {
            account,
            chain_id: Id::try_from("test-chain").expect("chain id"),
            gas_price: 0.1,
        });

        let sign_fn = SignFnBuilder::new(context, pubkey, signer).build();
        let tx = sign_fn
            .sign(
                1,
                &TxRequest::tx(
                    RawTxBody::default(),
                    TxConfig::default().with_gas_limit(100),
                ),
            )
            .await
            .expect("sign tx");

        assert_eq!(tx.sequence, 1);
        assert!(!tx.bytes.is_empty());
    }

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
}
