//! Fibre API for off-chain data availability.
//!
//! The [`FibreApi`] wraps a [`celestia_fibre::FibreClient`] to provide
//! `put()` and `download()` operations through the celestia-client.

use std::sync::Arc;

use celestia_fibre::{
    Blob, BlobID, DownloadOptions, FibreClient, FibreError, SignedPaymentPromise,
};
use celestia_grpc::{SubmittedTx, TxConfig};
use k256::ecdsa::SigningKey;

use crate::Error;
use crate::client::ClientInner;

/// Fibre API for off-chain data availability.
///
/// Provides access to the Fibre DA protocol for uploading and downloading
/// blobs directly to/from validators, bypassing on-chain blob submission.
pub struct FibreApi {
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

    /// Upload a pre-encoded [`Blob`] and collect validator signatures.
    ///
    /// Returns a [`SignedPaymentPromise`] once enough validator signatures
    /// have been collected to meet the safety threshold.
    pub async fn upload(
        &self,
        signing_key: &SigningKey,
        namespace: &[u8],
        blob: Blob,
    ) -> Result<SignedPaymentPromise, FibreError> {
        self.fibre_client.upload(signing_key, namespace, blob).await
    }

    /// Upload data and broadcast `MsgPayForFibre` on-chain.
    ///
    /// Encodes the data into a blob, distributes it to validators via the
    /// Fibre protocol, collects signatures, and broadcasts a `MsgPayForFibre`
    /// transaction. Returns a [`SubmittedTx`] handle that can be used to
    /// confirm the transaction.
    ///
    /// Requires the client to have a gRPC endpoint and signer configured.
    pub async fn put(
        &self,
        signing_key: &SigningKey,
        namespace: &[u8],
        data: &[u8],
    ) -> Result<SubmittedTx, Error> {
        let grpc = self.inner.grpc()?;
        let signer_address = grpc
            .get_account_address()
            .ok_or(Error::NoAssociatedAddress)?;

        let msg = self
            .fibre_client
            .put(signing_key, namespace, data, &signer_address.to_string())
            .await
            .map_err(fibre_err)?;

        let submitted = grpc.broadcast_message(msg, TxConfig::default()).await?;

        Ok(submitted)
    }

    /// Download and reconstruct a blob by its [`BlobID`].
    ///
    /// Fetches row proofs from validators and reconstructs the original data
    /// using erasure coding.
    pub async fn download(&self, id: &BlobID, opts: DownloadOptions) -> Result<Blob, FibreError> {
        self.fibre_client.download(id, opts).await
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

/// Convert a [`FibreError`] into the client [`Error`] type.
fn fibre_err(e: FibreError) -> Error {
    match e {
        FibreError::Grpc(status) => Error::Grpc(celestia_grpc::Error::from(*status)),
        FibreError::GrpcClient(grpc_err) => Error::Grpc(grpc_err),
        #[cfg(not(target_arch = "wasm32"))]
        FibreError::Transport(t) => Error::Grpc(celestia_grpc::Error::from(t)),
        other => Error::Fibre(other),
    }
}
