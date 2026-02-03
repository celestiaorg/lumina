use std::collections::HashMap;
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
    RejectionReason, SignFn, SubmitFailure, Transaction, TransactionManager, TransactionWorker,
    TxCallbacks, TxConfirmResult, TxHandle, TxRequest, TxServer, TxStatus, TxSubmitResult,
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

    pub(crate) fn build(self) -> Arc<dyn SignFn<Hash, TxConfirmInfo>> {
        Arc::new(BuiltSignFn {
            context: self.context,
            pubkey: self.pubkey,
            signer: self.signer,
            cached_account: OnceCell::new(),
            cached_chain_id: OnceCell::new(),
        })
    }
}

pub struct TransactionService {
    inner: Arc<TransactionServiceInner>,
}

struct TransactionServiceInner {
    manager: RwLock<TransactionManager<Hash, TxConfirmInfo>>,
    worker: Mutex<Option<WorkerHandle>>,
    client: GrpcClient,
    sign_fn: Arc<dyn SignFn<Hash, TxConfirmInfo>>,
    confirm_interval: Duration,
    max_status_batch: usize,
    queue_capacity: usize,
}

struct WorkerHandle {
    shutdown: CancellationToken,
    done_rx: oneshot::Receiver<Result<()>>,
}

impl TransactionService {
    pub(crate) async fn new(client: GrpcClient) -> Result<Self> {
        let (pubkey, signer) = client.signer()?;
        let signer = Arc::new(signer);
        let context = Arc::new(GrpcSignContext::new(client.clone()));
        let sign_fn = SignFnBuilder::new(context, pubkey, signer).build();
        let confirm_interval = Duration::from_millis(TxConfig::default().confirmation_interval_ms);
        let max_status_batch = DEFAULT_MAX_STATUS_BATCH;
        let queue_capacity = DEFAULT_QUEUE_CAPACITY;
        let (manager, worker_handle) = Self::spawn_worker(
            &client,
            sign_fn.clone(),
            confirm_interval,
            max_status_batch,
            queue_capacity,
        )
        .await?;

        Ok(Self {
            inner: Arc::new(TransactionServiceInner {
                manager: RwLock::new(manager),
                worker: Mutex::new(Some(worker_handle)),
                client,
                sign_fn,
                confirm_interval,
                max_status_batch,
                queue_capacity,
            }),
        })
    }

    pub async fn submit(
        &self,
        request: TxRequest,
        cfg: TxConfig,
    ) -> Result<TxHandle<Hash, TxConfirmInfo>> {
        let manager = self.inner.manager.read().await.clone();
        manager.add_tx(request, cfg).await
    }

    pub async fn recreate_worker(&self) -> Result<()> {
        let (manager, worker_handle) = Self::spawn_worker(
            &self.inner.client,
            self.inner.sign_fn.clone(),
            self.inner.confirm_interval,
            self.inner.max_status_batch,
            self.inner.queue_capacity,
        )
        .await?;

        let mut worker_guard = self.inner.worker.lock().await;
        if let Some(old_worker) = worker_guard.replace(worker_handle) {
            old_worker.shutdown.cancel();
            // Wait for worker to finish
            let _ = old_worker.done_rx.await;
        }

        let mut manager_guard = self.inner.manager.write().await;
        *manager_guard = manager;

        Ok(())
    }

    async fn spawn_worker(
        client: &GrpcClient,
        sign_fn: Arc<dyn SignFn<Hash, TxConfirmInfo>>,
        confirm_interval: Duration,
        max_status_batch: usize,
        queue_capacity: usize,
    ) -> Result<(TransactionManager<Hash, TxConfirmInfo>, WorkerHandle)> {
        let start_sequence = client.current_sequence().await?;
        let nodes = HashMap::from([(Arc::from("default"), Arc::new(client.clone()))]);
        let (manager, mut worker) = TransactionWorker::new(
            nodes,
            confirm_interval,
            max_status_batch,
            sign_fn,
            start_sequence,
            queue_capacity,
        );

        let shutdown = CancellationToken::new();
        let worker_shutdown = shutdown.clone();
        let (done_tx, done_rx) = oneshot::channel();
        spawn(async move {
            let result = worker.process(worker_shutdown).await;
            let _ = done_tx.send(result);
        });

        Ok((manager, WorkerHandle { shutdown, done_rx }))
    }
}

impl GrpcClient {
    pub async fn transaction_service(&self) -> Result<TransactionService> {
        TransactionService::new(self.clone()).await
    }
}

struct BuiltSignFn {
    context: Arc<dyn SignContext>,
    pubkey: VerifyingKey,
    signer: Arc<BoxedDocSigner>,
    cached_account: OnceCell<BaseAccount>,
    cached_chain_id: OnceCell<Id>,
}

#[async_trait]
impl SignFn<Hash, TxConfirmInfo> for BuiltSignFn {
    async fn sign(
        &self,
        sequence: u64,
        request: &TxRequest,
        cfg: &TxConfig,
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
        let (tx_body, blobs) = match request {
            TxRequest::Blobs(blobs) => {
                let pfb = MsgPayForBlobs::new(blobs, account.address)
                    .map_err(Error::CelestiaTypesError)?;
                let tx_body = RawTxBody {
                    messages: vec![RawMsgPayForBlobs::from(pfb).into_any()],
                    memo: cfg.memo.clone().unwrap_or_default(),
                    ..RawTxBody::default()
                };
                (tx_body, Some(blobs))
            }
            TxRequest::Message(msg) => {
                let tx_body = RawTxBody {
                    messages: vec![msg.clone()],
                    memo: cfg.memo.clone().unwrap_or_default(),
                    ..RawTxBody::default()
                };
                (tx_body, None)
            }
            TxRequest::RawPayload(_) => {
                return Err(Error::UnexpectedResponseType(
                    "raw payload not supported".into(),
                ));
            }
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
impl TxServer for GrpcClient {
    type TxId = Hash;
    type ConfirmInfo = TxConfirmInfo;

    async fn submit(&self, tx_bytes: Arc<Vec<u8>>, _sequence: u64) -> TxSubmitResult<Self::TxId> {
        let resp = self
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
        let response = self.tx_status_batch(ids.clone()).await?;
        let mut response_map = HashMap::new();
        for result in response.statuses {
            response_map.insert(result.hash, result.status);
        }

        let mut statuses = Vec::with_capacity(ids.len());
        let mut expected_sequence: Option<u64> = None;

        for hash in ids {
            match response_map.remove(&hash) {
                Some(status) => {
                    let mapped =
                        map_status_response(hash, status, &mut expected_sequence, self, "").await?;
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
        let mut result = self.status_batch(vec![id]).await?;
        result
            .pop()
            .map(|(_, status)| status)
            .ok_or_else(|| Error::UnexpectedResponseType("empty status response".to_string()))
    }

    async fn current_sequence(&self) -> Result<u64> {
        let address = self.get_account_address().ok_or(Error::MissingSigner)?;
        let account = self.get_account(&address).await?;
        Ok(account.sequence)
    }
}

#[derive(Debug, Clone)]
pub struct TxConfirmInfo {
    pub info: TxInfo,
    pub execution_code: ErrorCode,
}

async fn map_status_response(
    hash: Hash,
    response: TxStatusResponse,
    expected_sequence: &mut Option<u64>,
    client: &GrpcClient,
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
                let expected = ensure_expected_sequence(expected_sequence, client).await?;
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

async fn ensure_expected_sequence(
    expected_sequence: &mut Option<u64>,
    client: &GrpcClient,
) -> Result<u64> {
    if let Some(expected) = expected_sequence {
        return Ok(*expected);
    }
    let expected = client.current_sequence().await?;
    *expected_sequence = Some(expected);
    Ok(expected)
}

fn map_submit_failure(code: ErrorCode, message: &str) -> SubmitFailure {
    if is_wrong_sequence(code) {
        if let Some(parsed) = extract_sequence_on_mismatch(message) {
            return match parsed {
                Ok(expected) => SubmitFailure::SequenceMismatch { expected },
                Err(err) => SubmitFailure::NetworkError { err: Arc::new(err) },
            };
        }
    }

    match code {
        ErrorCode::InsufficientFunds => SubmitFailure::InsufficientFunds,
        ErrorCode::InsufficientFee => SubmitFailure::InsufficientFee { expected_fee: 0 },
        ErrorCode::MempoolIsFull => SubmitFailure::MempoolIsFull,
        _ => SubmitFailure::InvalidTx { error_code: code },
    }
}

fn is_wrong_sequence(code: ErrorCode) -> bool {
    code == ErrorCode::InvalidSequence || code == ErrorCode::WrongSequence
}

fn extract_sequence_on_mismatch(msg: &str) -> Option<Result<u64>> {
    msg.contains(SEQUENCE_ERROR_PAT)
        .then(|| extract_sequence(msg))
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
    use tendermint_proto::google::protobuf::Any;

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
    async fn signfn_builder_signs_message() {
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
                &TxRequest::Message(Any {
                    type_url: "/test.Msg".into(),
                    value: RawTxBody::default().encode_to_vec(),
                }),
                &TxConfig::default().with_gas_limit(100),
            )
            .await
            .expect("sign tx");

        assert_eq!(tx.sequence, 1);
        assert!(!tx.bytes.is_empty());
    }

    #[tokio::test]
    async fn signfn_builder_rejects_raw_payload() {
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
        let err = sign_fn
            .sign(
                1,
                &TxRequest::RawPayload(vec![1, 2, 3]),
                &TxConfig::default().with_gas_limit(100),
            )
            .await
            .expect_err("raw payload");

        assert!(matches!(err, Error::UnexpectedResponseType(_)));
    }

    #[test]
    fn txserver_impl_compiles_with_grpc_client() {
        fn assert_txserver<T: TxServer<TxId = Hash, ConfirmInfo = TxConfirmInfo>>() {}
        assert_txserver::<GrpcClient>();
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

        let service = client.transaction_service().await.unwrap();
        let handle = service
            .submit(
                TxRequest::Blobs(vec![random_blob(10..=1000)]),
                TxConfig::default(),
            )
            .await
            .unwrap();
        let submit_hash = handle.submitted.await.unwrap().unwrap();
        let confirm_status = handle.confirmed.await.unwrap().unwrap();

        let TxStatus::Confirmed { info } = confirm_status else {
            panic!("expected confirmed status");
        };
        assert_eq!(submit_hash, info.info.hash);
        assert!(info.info.height > 0);
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
