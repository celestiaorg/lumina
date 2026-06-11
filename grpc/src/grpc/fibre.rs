use celestia_proto::celestia::fibre::v1::{
    DownloadShardRequest, DownloadShardResponse, UploadShardRequest, UploadShardResponse,
};
use celestia_proto::celestia::valaddr::v1::{
    QueryAllFibreProvidersRequest, QueryAllFibreProvidersResponse, QueryFibreProviderInfoRequest,
    QueryFibreProviderInfoResponse,
};
use celestia_proto::tendermint_celestia_mods::rpc::grpc::{
    ValidatorSetRequest, ValidatorSetResponse,
};

use crate::Result;
use crate::grpc::{FromGrpcResponse, IntoGrpcParam, make_empty_params};

// BlockAPI — validator set

impl IntoGrpcParam<ValidatorSetRequest> for i64 {
    fn into_parameter(self) -> ValidatorSetRequest {
        ValidatorSetRequest { height: self }
    }
}

impl FromGrpcResponse<ValidatorSetResponse> for ValidatorSetResponse {
    fn try_from_response(self) -> Result<ValidatorSetResponse> {
        Ok(self)
    }
}

// Valaddr — all fibre providers

make_empty_params!(QueryAllFibreProvidersRequest);

impl FromGrpcResponse<QueryAllFibreProvidersResponse> for QueryAllFibreProvidersResponse {
    fn try_from_response(self) -> Result<QueryAllFibreProvidersResponse> {
        Ok(self)
    }
}

// Valaddr — single fibre provider info

impl IntoGrpcParam<QueryFibreProviderInfoRequest> for String {
    fn into_parameter(self) -> QueryFibreProviderInfoRequest {
        QueryFibreProviderInfoRequest {
            validator_consensus_address: self,
        }
    }
}

impl FromGrpcResponse<QueryFibreProviderInfoResponse> for QueryFibreProviderInfoResponse {
    fn try_from_response(self) -> Result<QueryFibreProviderInfoResponse> {
        Ok(self)
    }
}

// Fibre service — upload/download shards

impl IntoGrpcParam<UploadShardRequest> for UploadShardRequest {
    fn into_parameter(self) -> UploadShardRequest {
        self
    }
}

impl FromGrpcResponse<UploadShardResponse> for UploadShardResponse {
    fn try_from_response(self) -> Result<UploadShardResponse> {
        Ok(self)
    }
}

impl IntoGrpcParam<DownloadShardRequest> for Vec<u8> {
    fn into_parameter(self) -> DownloadShardRequest {
        DownloadShardRequest { blob_id: self }
    }
}

impl FromGrpcResponse<DownloadShardResponse> for DownloadShardResponse {
    fn try_from_response(self) -> Result<DownloadShardResponse> {
        Ok(self)
    }
}
