use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use celestia_types::any::IntoProtobufAny;
use celestia_types::blob::Blob;
use celestia_types::state::AccAddress;
use tokio::sync::RwLock;

use crate::grpc::TxStatusResponse;
use crate::signer::AccountSigner;
use crate::tx_client_impl::{ConfirmHandle, TransactionService, TxConfirmInfo, TxServiceConfig};
use crate::tx_client_v2::{NodeId, TxRequest};
use crate::{Error, GrpcClient, Result, TxConfig};

const DEFAULT_MAX_STATUS_BATCH: usize = 16;
const DEFAULT_QUEUE_CAPACITY: usize = 128;

/// Configuration for [`MultiAccountTxService`].
pub struct MultiAccountTxServiceConfig {
    /// List of nodes (id, client) for transaction submission and confirmation.
    pub nodes: Vec<(NodeId, GrpcClient)>,
    /// Initial set of account signers; one worker is spawned per signer.
    pub signers: Vec<AccountSigner>,
    /// Interval between confirmation polling attempts.
    pub confirm_interval: Duration,
    /// Maximum number of transactions queried in a single status batch.
    pub max_status_batch: usize,
    /// Capacity of the per-account pending transaction queue.
    pub queue_capacity: usize,
}

impl MultiAccountTxServiceConfig {
    /// Create a config with defaults and the given nodes and signers.
    pub fn new(nodes: Vec<(NodeId, GrpcClient)>, signers: Vec<AccountSigner>) -> Self {
        Self {
            nodes,
            signers,
            confirm_interval: Duration::from_millis(TxConfig::default().confirmation_interval_ms),
            max_status_batch: DEFAULT_MAX_STATUS_BATCH,
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }
}

/// A multi-account transaction service that manages one [`TransactionService`]
/// worker per account, sharing the same gRPC connection pool.
#[derive(Clone)]
pub struct MultiAccountTxService {
    inner: Arc<MultiAccountTxServiceInner>,
}

struct MultiAccountTxServiceInner {
    services: RwLock<HashMap<AccAddress, TransactionService>>,
    nodes: Vec<(NodeId, GrpcClient)>,
    confirm_interval: Duration,
    max_status_batch: usize,
    queue_capacity: usize,
}

impl MultiAccountTxService {
    /// Create a new service from config, spawning a worker per initial signer.
    pub async fn new(config: MultiAccountTxServiceConfig) -> Result<Self> {
        let mut services = HashMap::with_capacity(config.signers.len());
        for signer in config.signers {
            let address = signer.address();
            let tx_service = TransactionService::new(TxServiceConfig {
                nodes: config.nodes.clone(),
                signer,
                confirm_interval: config.confirm_interval,
                max_status_batch: config.max_status_batch,
                queue_capacity: config.queue_capacity,
            })
            .await?;
            services.insert(address, tx_service);
        }

        Ok(Self {
            inner: Arc::new(MultiAccountTxServiceInner {
                services: RwLock::new(services),
                nodes: config.nodes,
                confirm_interval: config.confirm_interval,
                max_status_batch: config.max_status_batch,
                queue_capacity: config.queue_capacity,
            }),
        })
    }

    /// Add a new account dynamically (spawns its own worker).
    pub async fn add_account(&self, signer: AccountSigner) -> Result<()> {
        let address = signer.address();
        let tx_service = TransactionService::new(TxServiceConfig {
            nodes: self.inner.nodes.clone(),
            signer,
            confirm_interval: self.inner.confirm_interval,
            max_status_batch: self.inner.max_status_batch,
            queue_capacity: self.inner.queue_capacity,
        })
        .await?;

        self.inner
            .services
            .write()
            .await
            .insert(address, tx_service);
        Ok(())
    }

    /// Submit a [`TxRequest`] for a specific account.
    ///
    /// The worker is automatically restarted if it has stopped.
    pub async fn submit(
        &self,
        account: &AccAddress,
        request: TxRequest,
    ) -> Result<ConfirmHandle<TxConfirmInfo, TxStatusResponse>> {
        let service = self.get_service(account).await?;
        service.submit_restart(request).await
    }

    /// Submit blobs for a specific account.
    pub async fn submit_blobs(
        &self,
        account: &AccAddress,
        blobs: Vec<Blob>,
        cfg: TxConfig,
    ) -> Result<ConfirmHandle<TxConfirmInfo, TxStatusResponse>> {
        self.submit(account, TxRequest::blobs(blobs, cfg)).await
    }

    /// Submit a single cosmos message for a specific account.
    pub async fn submit_message(
        &self,
        account: &AccAddress,
        msg: impl IntoProtobufAny,
        cfg: TxConfig,
    ) -> Result<ConfirmHandle<TxConfirmInfo, TxStatusResponse>> {
        self.submit(account, TxRequest::message(msg, cfg)).await
    }

    async fn get_service(&self, account: &AccAddress) -> Result<TransactionService> {
        self.inner
            .services
            .read()
            .await
            .get(account)
            .cloned()
            .ok_or_else(|| Error::UnknownAccount(*account))
    }
}
