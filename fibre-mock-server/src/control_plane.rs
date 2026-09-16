//! Control-plane services the fibre client uses for discovery:
//! `tendermint.rpc.grpc.BlockAPI/ValidatorSet` (the validator set) and
//! `celestia.valaddr.v1.Query` (validator address → fibre host).

use std::collections::HashMap;
use std::sync::Arc;

use celestia_proto::celestia::valaddr::v1::query_server::Query;
use celestia_proto::celestia::valaddr::v1::{
    FibreProvider, FibreProviderInfo, QueryAllFibreProvidersRequest,
    QueryAllFibreProvidersResponse, QueryFibreProviderInfoRequest, QueryFibreProviderInfoResponse,
};
use celestia_proto::tendermint_celestia_mods::rpc::grpc::block_api_server::BlockApi;
use celestia_proto::tendermint_celestia_mods::rpc::grpc::{
    BlockByHashRequest, BlockByHashResponse, BlockByHeightRequest, BlockByHeightResponse,
    CommitRequest, CommitResponse, StatusRequest, StatusResponse, SubscribeNewHeightsRequest,
    SubscribeNewHeightsResponse, ValidatorSetRequest, ValidatorSetResponse,
};
use tendermint_proto::v0_38::crypto::PublicKey;
use tendermint_proto::v0_38::crypto::public_key::Sum;
use tendermint_proto::v0_38::types::{Validator, ValidatorSet};
use tonic::{Request, Response, Status};

use crate::validator::MockValidator;

/// BlockAPI implementation that serves a static validator set.
pub struct MockBlockApi {
    validators: Arc<Vec<MockValidator>>,
    height: i64,
}

impl MockBlockApi {
    /// Serve `validators` as the validator set at `height`.
    pub fn new(validators: Arc<Vec<MockValidator>>, height: i64) -> Self {
        Self { validators, height }
    }
}

#[tonic::async_trait]
impl BlockApi for MockBlockApi {
    type BlockByHashStream = futures::stream::Empty<Result<BlockByHashResponse, Status>>;
    type BlockByHeightStream = futures::stream::Empty<Result<BlockByHeightResponse, Status>>;
    type SubscribeNewHeightsStream =
        futures::stream::Empty<Result<SubscribeNewHeightsResponse, Status>>;

    /// Returns the same static validator set for any height (0 = latest).
    async fn validator_set(
        self: Arc<Self>,
        _request: Request<ValidatorSetRequest>,
    ) -> Result<Response<ValidatorSetResponse>, Status> {
        let validators: Vec<Validator> = self
            .validators
            .iter()
            .map(|v| Validator {
                address: v.address.to_vec(),
                pub_key: Some(PublicKey {
                    sum: Some(Sum::Ed25519(v.pubkey_bytes().to_vec())),
                }),
                voting_power: v.voting_power,
                proposer_priority: 0,
            })
            .collect();
        let total_voting_power = validators.iter().map(|v| v.voting_power).sum();

        Ok(Response::new(ValidatorSetResponse {
            validator_set: Some(ValidatorSet {
                validators,
                proposer: None,
                total_voting_power,
            }),
            height: self.height,
        }))
    }

    async fn block_by_hash(
        self: Arc<Self>,
        _request: Request<BlockByHashRequest>,
    ) -> Result<Response<Self::BlockByHashStream>, Status> {
        Err(Status::unimplemented("not implemented in mock"))
    }

    async fn block_by_height(
        self: Arc<Self>,
        _request: Request<BlockByHeightRequest>,
    ) -> Result<Response<Self::BlockByHeightStream>, Status> {
        Err(Status::unimplemented("not implemented in mock"))
    }

    async fn commit(
        self: Arc<Self>,
        _request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        Err(Status::unimplemented("not implemented in mock"))
    }

    async fn subscribe_new_heights(
        self: Arc<Self>,
        _request: Request<SubscribeNewHeightsRequest>,
    ) -> Result<Response<Self::SubscribeNewHeightsStream>, Status> {
        Err(Status::unimplemented("not implemented in mock"))
    }

    async fn status(
        self: Arc<Self>,
        _request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        Err(Status::unimplemented("not implemented in mock"))
    }
}

/// valaddr Query implementation mapping consensus addresses to fibre hosts.
pub struct MockValaddrQuery {
    /// bech32 consensus address → fibre host.
    hosts: HashMap<String, String>,
    providers: Vec<FibreProvider>,
}

impl MockValaddrQuery {
    /// Precompute the registry for the given validators.
    pub fn new(validators: &[MockValidator]) -> Self {
        let hosts = validators
            .iter()
            .map(|v| (v.consensus_addr_bech32.clone(), v.host.clone()))
            .collect();
        let providers = validators
            .iter()
            .map(|v| FibreProvider {
                validator_consensus_address: v.consensus_addr_bech32.clone(),
                info: Some(FibreProviderInfo {
                    host: v.host.clone(),
                }),
            })
            .collect();
        Self { hosts, providers }
    }
}

#[tonic::async_trait]
impl Query for MockValaddrQuery {
    async fn fibre_provider_info(
        self: Arc<Self>,
        request: Request<QueryFibreProviderInfoRequest>,
    ) -> Result<Response<QueryFibreProviderInfoResponse>, Status> {
        let addr = request.into_inner().validator_consensus_address;
        let resp = match self.hosts.get(&addr) {
            Some(host) => QueryFibreProviderInfoResponse {
                info: Some(FibreProviderInfo { host: host.clone() }),
                found: true,
            },
            None => QueryFibreProviderInfoResponse {
                info: None,
                found: false,
            },
        };
        Ok(Response::new(resp))
    }

    async fn all_fibre_providers(
        self: Arc<Self>,
        _request: Request<QueryAllFibreProvidersRequest>,
    ) -> Result<Response<QueryAllFibreProvidersResponse>, Status> {
        Ok(Response::new(QueryAllFibreProvidersResponse {
            providers: self.providers.clone(),
        }))
    }
}
