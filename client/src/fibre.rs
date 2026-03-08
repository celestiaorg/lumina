//! Fibre API for off-chain data availability.
//!
//! The [`FibreApi`] wraps a [`celestia_fibre::FibreClient`] to provide
//! `put()` and `download()` operations through the celestia-client.

use std::sync::Arc;

use celestia_fibre::{Blob, BlobID, FibreClient, FibreError, PutResult};

use crate::client::ClientInner;

/// Fibre API for off-chain data availability.
///
/// Provides access to the Fibre DA protocol for uploading and downloading
/// blobs directly to/from validators, bypassing on-chain blob submission.
pub struct FibreApi {
    #[allow(dead_code)]
    inner: Arc<ClientInner>,
    fibre_client: Arc<FibreClient>,
}

impl FibreApi {
    pub(crate) fn new(inner: Arc<ClientInner>, fibre_client: FibreClient) -> Self {
        Self {
            inner,
            fibre_client: Arc::new(fibre_client),
        }
    }

    /// Upload data and broadcast `MsgPayForFibre` on-chain.
    ///
    /// Encodes the data into a blob, distributes it to validators via the
    /// Fibre protocol, collects signatures, and broadcasts a `MsgPayForFibre`
    /// transaction.
    ///
    /// Requires the [`FibreClient`] to have been constructed with a
    /// [`GrpcClient`](celestia_grpc::GrpcClient) for transaction broadcast.
    pub async fn put(&self, namespace: &[u8], data: &[u8]) -> Result<PutResult, FibreError> {
        self.fibre_client.put(namespace, data).await
    }

    /// Download and reconstruct a blob by its [`BlobID`].
    ///
    /// Fetches row proofs from validators and reconstructs the original data
    /// using erasure coding.
    pub async fn download(&self, id: &BlobID) -> Result<Blob, FibreError> {
        self.fibre_client.download(id).await
    }

    /// Returns `true` if the underlying fibre client has been closed.
    pub fn is_closed(&self) -> bool {
        self.fibre_client.is_closed()
    }

    /// Close the underlying fibre client.
    pub fn close(&self) {
        self.fibre_client.close();
    }
}
