//! celestia-node rpc types and methods related to blobstream

use std::future::Future;
use std::marker::{Send, Sync};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use celestia_types::MerkleProof;
use celestia_types::consts::HASH_SIZE;
use celestia_types::hash::Hash;
use jsonrpsee::core::RpcResult;
use jsonrpsee::core::client::{ClientT, Error};
use jsonrpsee::proc_macros::rpc;

use crate::custom_client_error;

mod rpc {
    use super::*;

    #[rpc(client, server, namespace = "blobstream", namespace_separator = ".")]
    pub trait Blobstream {
        /// Collects the data roots over a provided ordered range of blocks, and then
        /// creates a new Merkle root of those data roots.
        ///
        /// The range is end exclusive.
        #[method(name = "GetDataRootTupleRoot")]
        async fn blobstream_get_data_root_tuple_root(
            &self,
            start: u64,
            end: u64,
        ) -> RpcResult<String>;

        /// Creates an inclusion proof, for the data root tuple of block height `height`,
        /// in the set of blocks defined by `start` and `end`.
        ///
        /// The range is end exclusive.
        #[method(name = "GetDataRootTupleInclusionProof")]
        async fn blobstream_get_data_root_tuple_inclusion_proof(
            &self,
            height: u64,
            start: u64,
            end: u64,
        ) -> RpcResult<MerkleProof>;
    }
}

/// Client implementation for the Blobstream RPC API.
// TODO: get rid of this after a release or two
// https://github.com/eigerco/lumina/issues/683
pub trait BlobstreamClient: ClientT {
    /// Collects the data roots over a provided ordered range of blocks, and then
    /// creates a new Merkle root of those data roots.
    ///
    /// The range is end exclusive.
    fn blobstream_get_data_root_tuple_root<'a, 'fut>(
        &'a self,
        start: u64,
        end: u64,
    ) -> impl Future<Output = Result<Hash, Error>> + Send + 'fut
    where
        'a: 'fut,
        Self: Sized + Sync + 'fut,
    {
        async move {
            let root = rpc::BlobstreamClient::blobstream_get_data_root_tuple_root(self, start, end)
                .await?;

            if root.len() == 2 * HASH_SIZE {
                root.parse::<Hash>().map_err(custom_client_error)
            } else {
                let decoded = BASE64.decode(&root).map_err(custom_client_error)?;
                Hash::try_from(decoded).map_err(custom_client_error)
            }
        }
    }

    /// Creates an inclusion proof, for the data root tuple of block height `height`,
    /// in the set of blocks defined by `start` and `end`.
    ///
    /// The range is end exclusive.
    fn blobstream_get_data_root_tuple_inclusion_proof<'a, 'fut>(
        &'a self,
        height: u64,
        start: u64,
        end: u64,
    ) -> impl Future<Output = Result<MerkleProof, Error>> + Send + 'fut
    where
        'a: 'fut,
        Self: Sized + Sync + 'fut,
    {
        rpc::BlobstreamClient::blobstream_get_data_root_tuple_inclusion_proof(
            self, height, start, end,
        )
    }
}

pub trait BlobstreamServer: rpc::BlobstreamServer {}

impl<T> BlobstreamClient for T where T: ClientT {}

impl<T> BlobstreamServer for T where T: rpc::BlobstreamServer {}

pub use rpc::BlobstreamServer as BlobstreamRpcServer;
