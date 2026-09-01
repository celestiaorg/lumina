//! Per-validator implementation of `celestia.fibre.v1.Fibre`.
//!
//! UploadShard blind-stores the shard (no verification — the client verifies
//! everything on download) and returns a real ed25519 signature over the
//! promise sign bytes. DownloadShard echoes the stored shard back.

use std::sync::Arc;

use celestia_proto::celestia::fibre::v1::fibre_server::Fibre;
use celestia_proto::celestia::fibre::v1::{
    BlobShard, DownloadShardRequest, DownloadShardResponse, UploadShardRequest, UploadShardResponse,
};
use ed25519_dalek::Signer;
use tonic::{Request, Response, Status};

use crate::promise::promise_from_proto;
use crate::store::ShardStore;

/// One validator's Fibre data-plane service.
pub struct MockFibreService {
    signing_key: ed25519_dalek::SigningKey,
    store: ShardStore,
}

impl MockFibreService {
    /// Create a service with an empty store, signing with the given key.
    pub fn new(signing_key: ed25519_dalek::SigningKey) -> Self {
        Self {
            signing_key,
            store: ShardStore::default(),
        }
    }
}

#[tonic::async_trait]
impl Fibre for MockFibreService {
    async fn upload_shard(
        self: Arc<Self>,
        request: Request<UploadShardRequest>,
    ) -> Result<Response<UploadShardResponse>, Status> {
        let req = request.into_inner();
        let promise_proto = req
            .promise
            .ok_or_else(|| Status::invalid_argument("missing promise"))?;
        let shard = req
            .shard
            .ok_or_else(|| Status::invalid_argument("missing shard"))?;

        let commitment: [u8; 32] = promise_proto
            .commitment
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("commitment must be 32 bytes"))?;

        let promise = promise_from_proto(promise_proto)?;
        let sign_bytes = promise
            .sign_bytes()
            .map_err(|e| Status::invalid_argument(format!("promise sign bytes: {e}")))?;
        let signature = self.signing_key.sign(&sign_bytes);

        self.store.insert(commitment, shard);
        tracing::trace!(commitment = %hex::encode(commitment), "stored shard");

        Ok(Response::new(UploadShardResponse {
            validator_signature: signature.to_bytes().to_vec(),
        }))
    }

    async fn download_shard(
        self: Arc<Self>,
        request: Request<DownloadShardRequest>,
    ) -> Result<Response<DownloadShardResponse>, Status> {
        let blob_id = request.into_inner().blob_id;
        if blob_id.len() != 33 {
            return Err(Status::invalid_argument(
                "blob_id must be 33 bytes (version + commitment)",
            ));
        }
        let commitment: [u8; 32] = blob_id[1..33].try_into().expect("length checked above");

        let shard = self
            .store
            .get(&commitment)
            .ok_or_else(|| Status::not_found("blob not found"))?;

        Ok(Response::new(DownloadShardResponse {
            shard: Some(BlobShard::clone(&shard)),
        }))
    }
}
