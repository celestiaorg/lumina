//! App-node gRPC services needed by `GrpcClient::broadcast_message(..).confirm()`:
//! latest block (chain id), auth account, gas estimation, tx broadcast, and
//! celestia tx status. Everything reports instant success; txs "commit" at the
//! mock height the moment they are broadcast.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use celestia_proto::celestia::core::v1::gas_estimation::gas_estimator_server::GasEstimator;
use celestia_proto::celestia::core::v1::gas_estimation::{
    EstimateGasPriceAndUsageRequest, EstimateGasPriceAndUsageResponse, EstimateGasPriceRequest,
    EstimateGasPriceResponse,
};
use celestia_proto::celestia::core::v1::tx::tx_server::Tx;
use celestia_proto::celestia::core::v1::tx::{
    TxStatusBatchRequest, TxStatusBatchResponse, TxStatusRequest, TxStatusResponse, TxStatusResult,
};
use celestia_proto::cosmos::auth::v1beta1::query_server::Query as AuthQuery;
use celestia_proto::cosmos::auth::v1beta1::{
    AddressBytesToStringRequest, AddressBytesToStringResponse, AddressStringToBytesRequest,
    AddressStringToBytesResponse, BaseAccount, Bech32PrefixRequest, Bech32PrefixResponse,
    QueryAccountAddressByIdRequest, QueryAccountAddressByIdResponse, QueryAccountInfoRequest,
    QueryAccountInfoResponse, QueryAccountRequest, QueryAccountResponse, QueryAccountsRequest,
    QueryAccountsResponse, QueryModuleAccountByNameRequest, QueryModuleAccountByNameResponse,
    QueryModuleAccountsRequest, QueryModuleAccountsResponse, QueryParamsRequest,
    QueryParamsResponse,
};
use celestia_proto::cosmos::base::abci::v1beta1::TxResponse;
use celestia_proto::cosmos::base::tendermint::v1beta1::service_server::Service as TendermintService;
use celestia_proto::cosmos::base::tendermint::v1beta1::{
    AbciQueryRequest, AbciQueryResponse, GetBlockByHeightRequest, GetBlockByHeightResponse,
    GetLatestBlockRequest, GetLatestBlockResponse, GetLatestValidatorSetRequest,
    GetLatestValidatorSetResponse, GetNodeInfoRequest, GetNodeInfoResponse, GetSyncingRequest,
    GetSyncingResponse, GetValidatorSetByHeightRequest, GetValidatorSetByHeightResponse,
};
use celestia_proto::cosmos::tx::v1beta1::service_server::Service as CosmosTxService;
use celestia_proto::cosmos::tx::v1beta1::{
    BroadcastTxRequest, BroadcastTxResponse, GetBlockWithTxsRequest, GetBlockWithTxsResponse,
    GetTxRequest, GetTxResponse, GetTxsEventRequest, GetTxsEventResponse, SimulateRequest,
    SimulateResponse, TxDecodeAminoRequest, TxDecodeAminoResponse, TxDecodeRequest,
    TxDecodeResponse, TxEncodeAminoRequest, TxEncodeAminoResponse, TxEncodeRequest,
    TxEncodeResponse,
};
use celestia_proto::tendermint_celestia_mods::types::{Block, Data};
use prost::{Message, Name};
use sha2::{Digest, Sha256};
use tendermint_proto::google::protobuf::{Any, Timestamp};
use tendermint_proto::v0_38::types::Header;
use tendermint_proto::v0_38::version::Consensus;
use tonic::{Request, Response, Status};

use crate::chain::MockChain;

/// Gas values served by the mock estimator; the tx is never executed, so any
/// positive numbers work.
const GAS_PRICE: f64 = 0.002;
const GAS_USED: u64 = 200_000;

fn unimplemented<T>() -> Result<Response<T>, Status> {
    Err(Status::unimplemented("not implemented in mock"))
}

/// `cosmos.base.tendermint.v1beta1.Service`: serves the latest block so the
/// client can learn the chain id.
pub struct MockTendermintService(pub Arc<MockChain>);

#[tonic::async_trait]
impl TendermintService for MockTendermintService {
    async fn get_latest_block(
        self: Arc<Self>,
        _request: Request<GetLatestBlockRequest>,
    ) -> Result<Response<GetLatestBlockResponse>, Status> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Status::internal("system clock before Unix epoch"))?;
        let header = Header {
            version: Some(Consensus { block: 11, app: 1 }),
            chain_id: self.0.chain_id.clone(),
            height: self.0.height,
            time: Some(Timestamp {
                seconds: now.as_secs() as i64,
                nanos: now.subsec_nanos() as i32,
            }),
            proposer_address: self.0.proposer.to_vec(),
            ..Default::default()
        };
        Ok(Response::new(GetLatestBlockResponse {
            block_id: None,
            block: Some(Block {
                header: Some(header),
                data: Some(Data::default()),
                evidence: None,
                last_commit: None,
            }),
            sdk_block: None,
        }))
    }

    async fn get_node_info(
        self: Arc<Self>,
        _request: Request<GetNodeInfoRequest>,
    ) -> Result<Response<GetNodeInfoResponse>, Status> {
        unimplemented()
    }

    async fn get_syncing(
        self: Arc<Self>,
        _request: Request<GetSyncingRequest>,
    ) -> Result<Response<GetSyncingResponse>, Status> {
        unimplemented()
    }

    async fn get_block_by_height(
        self: Arc<Self>,
        _request: Request<GetBlockByHeightRequest>,
    ) -> Result<Response<GetBlockByHeightResponse>, Status> {
        unimplemented()
    }

    async fn get_latest_validator_set(
        self: Arc<Self>,
        _request: Request<GetLatestValidatorSetRequest>,
    ) -> Result<Response<GetLatestValidatorSetResponse>, Status> {
        unimplemented()
    }

    async fn get_validator_set_by_height(
        self: Arc<Self>,
        _request: Request<GetValidatorSetByHeightRequest>,
    ) -> Result<Response<GetValidatorSetByHeightResponse>, Status> {
        unimplemented()
    }

    async fn abci_query(
        self: Arc<Self>,
        _request: Request<AbciQueryRequest>,
    ) -> Result<Response<AbciQueryResponse>, Status> {
        unimplemented()
    }
}

/// `cosmos.auth.v1beta1.Query`: fabricates a base account for any address.
/// The client queries once and tracks the sequence locally afterwards.
pub struct MockAuthQuery;

#[tonic::async_trait]
impl AuthQuery for MockAuthQuery {
    async fn account(
        self: Arc<Self>,
        request: Request<QueryAccountRequest>,
    ) -> Result<Response<QueryAccountResponse>, Status> {
        let account = BaseAccount {
            address: request.into_inner().address,
            pub_key: None,
            account_number: 1,
            sequence: 0,
        };
        Ok(Response::new(QueryAccountResponse {
            account: Some(Any {
                type_url: BaseAccount::type_url(),
                value: account.encode_to_vec(),
            }),
        }))
    }

    async fn accounts(
        self: Arc<Self>,
        _request: Request<QueryAccountsRequest>,
    ) -> Result<Response<QueryAccountsResponse>, Status> {
        unimplemented()
    }

    async fn account_address_by_id(
        self: Arc<Self>,
        _request: Request<QueryAccountAddressByIdRequest>,
    ) -> Result<Response<QueryAccountAddressByIdResponse>, Status> {
        unimplemented()
    }

    async fn params(
        self: Arc<Self>,
        _request: Request<QueryParamsRequest>,
    ) -> Result<Response<QueryParamsResponse>, Status> {
        unimplemented()
    }

    async fn module_accounts(
        self: Arc<Self>,
        _request: Request<QueryModuleAccountsRequest>,
    ) -> Result<Response<QueryModuleAccountsResponse>, Status> {
        unimplemented()
    }

    async fn module_account_by_name(
        self: Arc<Self>,
        _request: Request<QueryModuleAccountByNameRequest>,
    ) -> Result<Response<QueryModuleAccountByNameResponse>, Status> {
        unimplemented()
    }

    async fn bech32_prefix(
        self: Arc<Self>,
        _request: Request<Bech32PrefixRequest>,
    ) -> Result<Response<Bech32PrefixResponse>, Status> {
        unimplemented()
    }

    async fn address_bytes_to_string(
        self: Arc<Self>,
        _request: Request<AddressBytesToStringRequest>,
    ) -> Result<Response<AddressBytesToStringResponse>, Status> {
        unimplemented()
    }

    async fn address_string_to_bytes(
        self: Arc<Self>,
        _request: Request<AddressStringToBytesRequest>,
    ) -> Result<Response<AddressStringToBytesResponse>, Status> {
        unimplemented()
    }

    async fn account_info(
        self: Arc<Self>,
        _request: Request<QueryAccountInfoRequest>,
    ) -> Result<Response<QueryAccountInfoResponse>, Status> {
        unimplemented()
    }
}

/// `celestia.core.v1.gas_estimation.GasEstimator`: fixed price and usage.
pub struct MockGasEstimator;

#[tonic::async_trait]
impl GasEstimator for MockGasEstimator {
    async fn estimate_gas_price(
        self: Arc<Self>,
        _request: Request<EstimateGasPriceRequest>,
    ) -> Result<Response<EstimateGasPriceResponse>, Status> {
        Ok(Response::new(EstimateGasPriceResponse {
            estimated_gas_price: GAS_PRICE,
        }))
    }

    async fn estimate_gas_price_and_usage(
        self: Arc<Self>,
        _request: Request<EstimateGasPriceAndUsageRequest>,
    ) -> Result<Response<EstimateGasPriceAndUsageResponse>, Status> {
        Ok(Response::new(EstimateGasPriceAndUsageResponse {
            estimated_gas_price: GAS_PRICE,
            estimated_gas_used: GAS_USED,
        }))
    }
}

/// `cosmos.tx.v1beta1.Service`: accepts any broadcast as instantly committed.
pub struct MockCosmosTxService(pub Arc<MockChain>);

#[tonic::async_trait]
impl CosmosTxService for MockCosmosTxService {
    async fn broadcast_tx(
        self: Arc<Self>,
        request: Request<BroadcastTxRequest>,
    ) -> Result<Response<BroadcastTxResponse>, Status> {
        // The client takes this hash verbatim and parses it as uppercase hex.
        let txhash = hex::encode_upper(Sha256::digest(&request.into_inner().tx_bytes));
        let height = self.0.commit_tx(txhash.clone());
        Ok(Response::new(BroadcastTxResponse {
            tx_response: Some(TxResponse {
                height,
                txhash,
                code: 0,
                ..Default::default()
            }),
        }))
    }

    async fn simulate(
        self: Arc<Self>,
        _request: Request<SimulateRequest>,
    ) -> Result<Response<SimulateResponse>, Status> {
        unimplemented()
    }

    async fn get_tx(
        self: Arc<Self>,
        _request: Request<GetTxRequest>,
    ) -> Result<Response<GetTxResponse>, Status> {
        unimplemented()
    }

    async fn get_txs_event(
        self: Arc<Self>,
        _request: Request<GetTxsEventRequest>,
    ) -> Result<Response<GetTxsEventResponse>, Status> {
        unimplemented()
    }

    async fn get_block_with_txs(
        self: Arc<Self>,
        _request: Request<GetBlockWithTxsRequest>,
    ) -> Result<Response<GetBlockWithTxsResponse>, Status> {
        unimplemented()
    }

    async fn tx_decode(
        self: Arc<Self>,
        _request: Request<TxDecodeRequest>,
    ) -> Result<Response<TxDecodeResponse>, Status> {
        unimplemented()
    }

    async fn tx_encode(
        self: Arc<Self>,
        _request: Request<TxEncodeRequest>,
    ) -> Result<Response<TxEncodeResponse>, Status> {
        unimplemented()
    }

    async fn tx_encode_amino(
        self: Arc<Self>,
        _request: Request<TxEncodeAminoRequest>,
    ) -> Result<Response<TxEncodeAminoResponse>, Status> {
        unimplemented()
    }

    async fn tx_decode_amino(
        self: Arc<Self>,
        _request: Request<TxDecodeAminoRequest>,
    ) -> Result<Response<TxDecodeAminoResponse>, Status> {
        unimplemented()
    }
}

/// `celestia.core.v1.tx.Tx`: every broadcast tx is already committed.
pub struct MockCelestiaTxStatus(pub Arc<MockChain>);

fn status_of(chain: &MockChain, tx_id: &str) -> TxStatusResponse {
    match chain.tx_height(tx_id) {
        Some(height) => TxStatusResponse {
            height,
            status: "COMMITTED".to_string(),
            ..Default::default()
        },
        None => TxStatusResponse {
            status: "UNKNOWN".to_string(),
            ..Default::default()
        },
    }
}

#[tonic::async_trait]
impl Tx for MockCelestiaTxStatus {
    async fn tx_status(
        self: Arc<Self>,
        request: Request<TxStatusRequest>,
    ) -> Result<Response<TxStatusResponse>, Status> {
        Ok(Response::new(status_of(
            &self.0,
            &request.into_inner().tx_id,
        )))
    }

    async fn tx_status_batch(
        self: Arc<Self>,
        request: Request<TxStatusBatchRequest>,
    ) -> Result<Response<TxStatusBatchResponse>, Status> {
        let statuses = request
            .into_inner()
            .tx_ids
            .into_iter()
            .map(|id| TxStatusResult {
                status: Some(status_of(&self.0, &id)),
                tx_hash: id,
            })
            .collect();
        Ok(Response::new(TxStatusBatchResponse { statuses }))
    }
}
