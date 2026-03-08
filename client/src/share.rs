use std::sync::Arc;

use celestia_rpc::ShareClient;

use crate::Result;
use crate::api::share::{GetRangeResponse, GetRowResponse, SampleCoordinates};
use crate::client::ClientInner;
use crate::types::namespace_data::NamespaceData;
use crate::types::nmt::Namespace;
use crate::types::sample::Sample;
use crate::types::{ExtendedDataSquare, Share};

/// Share API for quering bridge nodes.
pub struct ShareApi {
    inner: Arc<ClientInner>,
}

impl ShareApi {
    pub(crate) fn new(inner: Arc<ClientInner>) -> ShareApi {
        ShareApi { inner }
    }

    /// Performs a subjective validation to check if the shares committed to the
    /// header at the specified height are available and retrievable from the network.
    ///
    /// Returns `Ok(())` if shares are available.
    pub async fn shares_available(&self, height: u64) -> Result<()> {
        Ok(self.inner.rpc.share_shares_available(height).await?)
    }

    /// Retrieves a specific share from the [`ExtendedDataSquare`] at the given
    /// height  using its row and column coordinates.
    pub async fn get(
        &self,
        height: u64,
        square_width: u16,
        row: u16,
        column: u16,
    ) -> Result<Share> {
        Ok(self
            .inner
            .rpc
            .share_get_share(height, square_width, row, column)
            .await?)
    }

    /// Retrieves a specific share from the [`ExtendedDataSquare`] at the given
    /// height  using its row and column coordinates.
    ///
    /// # NOTE
    ///
    /// This method will first fetch and validate the header at a given height and
    /// then use it to provide necessary data for validation and post-processing.
    ///
    /// If you already have access to the necessary data, it is recommended to use
    /// the equivalent method without the `_with_root` suffix.
    pub async fn get_with_root(&self, height: u64, row: u16, column: u16) -> Result<Share> {
        let header = self.inner.get_header_validated(height).await?;
        Ok(self
            .inner
            .rpc
            .share_get_share(header.height(), header.square_width(), row, column)
            .await?)
    }

    /// Retrieves multiple shares from the [`ExtendedDataSquare`] at the given
    /// sample coordinates.
    ///
    /// `coordinates` is a list of `(row, column)`.
    pub async fn get_samples<I, C>(&self, height: u64, coordinates: I) -> Result<Vec<Sample>>
    where
        I: IntoIterator<Item = C>,
        C: Into<SampleCoordinates>,
    {
        Ok(self
            .inner
            .rpc
            .share_get_samples(height, coordinates)
            .await?)
    }

    /// Retrieves multiple shares from the [`ExtendedDataSquare`] at the given
    /// sample coordinates.
    ///
    /// `coordinates` is a list of `(row, column)`.
    ///
    /// # NOTE
    ///
    /// This method will first fetch and validate the header at a given height and
    /// then use it to provide necessary data for validation and post-processing.
    ///
    /// If you already have access to the necessary data, it is recommended to use
    /// the equivalent method without the `_with_root` suffix.
    pub async fn get_samples_with_root<I, C>(
        &self,
        height: u64,
        coordinates: I,
    ) -> Result<Vec<Sample>>
    where
        I: IntoIterator<Item = C>,
        C: Into<SampleCoordinates>,
    {
        let header = self.inner.get_header_validated(height).await?;
        Ok(self
            .inner
            .rpc
            .share_get_samples(header.height(), coordinates)
            .await?)
    }

    /// Retrieves the complete [`ExtendedDataSquare`] for the specified height.
    pub async fn get_eds(&self, height: u64) -> Result<ExtendedDataSquare> {
        Ok(self.inner.rpc.share_get_eds(height).await?)
    }

    /// Retrieves the complete [`ExtendedDataSquare`] for the specified height.
    ///
    /// # NOTE
    ///
    /// This method will first fetch and validate the header at a given height and
    /// then use it to provide necessary data for validation and post-processing.
    ///
    /// If you already have access to the necessary data, it is recommended to use
    /// the equivalent method without the `_with_root` suffix.
    pub async fn get_eds_with_root(&self, height: u64) -> Result<ExtendedDataSquare> {
        let header = self.inner.get_header_validated(height).await?;
        Ok(self.inner.rpc.share_get_eds(header.height()).await?)
    }

    /// Retrieves all shares from a specific row of the [`ExtendedDataSquare`]
    /// at the given height.
    pub async fn get_row(
        &self,
        height: u64,
        square_width: u16,
        row: u16,
    ) -> Result<GetRowResponse> {
        Ok(self
            .inner
            .rpc
            .share_get_row(height, square_width, row)
            .await?)
    }

    /// Retrieves all shares from a specific row of the [`ExtendedDataSquare`]
    /// at the given height.
    ///
    /// # NOTE
    ///
    /// This method will first fetch and validate the header at a given height and
    /// then use it to provide necessary data for validation and post-processing.
    ///
    /// If you already have access to the necessary data, it is recommended to use
    /// the equivalent method without the `_with_root` suffix.
    pub async fn get_row_with_root(&self, height: u64, row: u16) -> Result<GetRowResponse> {
        let header = self.inner.get_header_validated(height).await?;
        Ok(self
            .inner
            .rpc
            .share_get_row(header.height(), header.square_width(), row)
            .await?)
    }

    /// Retrieves all shares that belong to the specified namespace within the
    /// [`ExtendedDataSquare`] at the given height.
    ///
    /// The shares are returned in a row-by-row order, maintaining the original
    /// layout if the namespace spans multiple rows.
    pub async fn get_namespace_data(
        &self,
        height: u64,
        namespace: Namespace,
    ) -> Result<NamespaceData> {
        Ok(self
            .inner
            .rpc
            .share_get_namespace_data(height, namespace)
            .await?)
    }

    /// Retrieves all shares that belong to the specified namespace within the
    /// [`ExtendedDataSquare`] at the given height.
    ///
    /// The shares are returned in a row-by-row order, maintaining the original
    /// layout if the namespace spans multiple rows.
    ///
    /// # NOTE
    ///
    /// This method will first fetch and validate the header at a given height and
    /// then use it to provide necessary data for validation and post-processing.
    ///
    /// If you already have access to the necessary data, it is recommended to use
    /// the equivalent method without the `_with_root` suffix.
    pub async fn get_namespace_data_with_root(
        &self,
        height: u64,
        namespace: Namespace,
    ) -> Result<NamespaceData> {
        let header = self.inner.get_header_validated(height).await?;

        Ok(self
            .inner
            .rpc
            .share_get_namespace_data(header.height(), namespace)
            .await?)
    }

    /// Retrieves a list of shares and their corresponding proof.
    ///
    /// The start and end index ignores parity shares and corresponds to ODS.
    pub async fn get_range(&self, height: u64, start: u64, end: u64) -> Result<GetRangeResponse> {
        Ok(self.inner.rpc.share_get_range(height, start, end).await?)
    }

    /// Retrieves a list of shares and their corresponding proof.
    ///
    /// The start and end index ignores parity shares and corresponds to ODS.
    ///
    /// # NOTE
    ///
    /// This method will first fetch and validate the header at a given height and
    /// then use it to provide necessary data for validation and post-processing.
    ///
    /// If you already have access to the necessary data, it is recommended to use
    /// the equivalent method without the `_with_root` suffix.
    pub async fn get_range_with_root(
        &self,
        height: u64,
        start: u64,
        end: u64,
    ) -> Result<GetRangeResponse> {
        let header = self.inner.get_header_validated(height).await?;
        Ok(self
            .inner
            .rpc
            .share_get_range(header.height(), start, end)
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils::{ensure_serializable, ensure_serializable_deserializable};

    #[allow(dead_code)]
    #[allow(unused_variables)]
    #[allow(unreachable_code)]
    #[allow(clippy::diverging_sub_expression)]
    async fn enforce_serde_bounds() {
        // intentionally no-run, compile only test
        let api = ShareApi::new(unimplemented!());

        let _: () = api.shares_available(0).await.unwrap();

        let coordinates: Vec<SampleCoordinates> =
            ensure_serializable_deserializable(unimplemented!());
        ensure_serializable(api.get_samples(0, coordinates).await.unwrap());

        ensure_serializable(api.get_eds(0).await.unwrap());

        ensure_serializable_deserializable(api.get(0, 0, 0, 0).await.unwrap());

        ensure_serializable_deserializable(api.get_row(0, 0, 0).await.unwrap());

        let namespace = ensure_serializable_deserializable(unimplemented!());
        ensure_serializable_deserializable(api.get_namespace_data(0, namespace).await.unwrap());

        ensure_serializable_deserializable(api.get_range(0, 0, 0).await.unwrap());
    }
}
