use std::sync::Arc;

use celestia_proto::shwap::row::HalfSide;
use celestia_proto::shwap::{Row as RawRow, Share as RawShare};
use celestia_rpc::ShareClient;

use crate::api::share::{GetRangeResponse, GetRowResponse, RowSide, SampleCoordinates};
use crate::client::ClientInner;
use crate::types::namespace_data::{NamespaceData, NamespaceDataId};
use crate::types::nmt::Namespace;
use crate::types::row::{Row, RowId};
use crate::types::sample::{Sample, SampleId};
use crate::types::{
    DataAvailabilityHeader, ExtendedDataSquare, ExtendedHeader, Share, VerificationError,
};
use crate::{Error, Result};

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
    /// then verify the samples against it.
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
        let coordinates: Vec<SampleCoordinates> = coordinates.into_iter().map(Into::into).collect();
        let samples = self
            .inner
            .rpc
            .share_get_samples(header.height(), coordinates.iter().copied())
            .await?;
        verify_samples(&header, &coordinates, &samples)?;
        Ok(samples)
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
    /// then verify the data square against it.
    ///
    /// If you already have access to the necessary data, it is recommended to use
    /// the equivalent method without the `_with_root` suffix.
    pub async fn get_eds_with_root(&self, height: u64) -> Result<ExtendedDataSquare> {
        let header = self.inner.get_header_validated(height).await?;
        let eds = self.inner.rpc.share_get_eds(header.height()).await?;
        verify_eds(&header, &eds)?;
        Ok(eds)
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
    /// then verify the row against it.
    ///
    /// If you already have access to the necessary data, it is recommended to use
    /// the equivalent method without the `_with_root` suffix.
    pub async fn get_row_with_root(&self, height: u64, row: u16) -> Result<GetRowResponse> {
        let header = self.inner.get_header_validated(height).await?;
        let response = self
            .inner
            .rpc
            .share_get_row(header.height(), header.square_width(), row)
            .await?;
        verify_row(&header, row, &response)?;
        Ok(response)
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
    /// then verify the namespace data against it, including that all the rows
    /// with the namespace are present.
    ///
    /// If you already have access to the necessary data, it is recommended to use
    /// the equivalent method without the `_with_root` suffix.
    pub async fn get_namespace_data_with_root(
        &self,
        height: u64,
        namespace: Namespace,
    ) -> Result<NamespaceData> {
        let header = self.inner.get_header_validated(height).await?;
        let data = self
            .inner
            .rpc
            .share_get_namespace_data(header.height(), namespace)
            .await?;
        verify_namespace_data(&header, namespace, &data)?;
        Ok(data)
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
    /// then verify that the proof covers exactly the requested range of the data
    /// square committed to by the header and that the shares are the proven ones.
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
        let response = self
            .inner
            .rpc
            .share_get_range(header.height(), start, end)
            .await?;
        verify_range_response(&header, start, end, &response)?;
        Ok(response)
    }
}

fn verification_error(message: String) -> Error {
    celestia_types::Error::Verification(VerificationError::Other(message)).into()
}

fn verify_range_response(
    header: &ExtendedHeader,
    start: u64,
    end: u64,
    response: &GetRangeResponse,
) -> Result<()> {
    response.proof.verify_range(header.dah.hash(), start..end)?;

    let proven = response.proof.shares();
    let same_shares = response.shares.len() == proven.len()
        && response
            .shares
            .iter()
            .zip(proven)
            .all(|(share, proven)| share.data() == proven);
    if !same_shares {
        return Err(verification_error(
            "shares differ from the ones proven by the proof".to_string(),
        ));
    }

    Ok(())
}

fn verify_samples(
    header: &ExtendedHeader,
    coordinates: &[SampleCoordinates],
    samples: &[Sample],
) -> Result<()> {
    if coordinates.len() != samples.len() {
        return Err(verification_error(format!(
            "requested ({}) samples, received ({})",
            coordinates.len(),
            samples.len()
        )));
    }

    for (coordinates, sample) in coordinates.iter().zip(samples) {
        let id = SampleId::new(coordinates.row, coordinates.column, header.height())?;
        sample.verify(id, &header.dah)?;
    }

    Ok(())
}

fn verify_eds(header: &ExtendedHeader, eds: &ExtendedDataSquare) -> Result<()> {
    if DataAvailabilityHeader::from_eds(eds) != header.dah {
        return Err(verification_error(
            "data square doesn't match the data availability header".to_string(),
        ));
    }

    Ok(())
}

fn verify_row(header: &ExtendedHeader, row: u16, response: &GetRowResponse) -> Result<()> {
    let id = RowId::new(row, header.height())?;

    let row = match response.side {
        RowSide::Both => Row {
            shares: response.shares.clone(),
        },
        RowSide::Left | RowSide::Right => {
            let half_side = match response.side {
                RowSide::Left => HalfSide::Left,
                _ => HalfSide::Right,
            };
            let raw = RawRow {
                shares_half: response
                    .shares
                    .iter()
                    .map(|share| RawShare {
                        data: share.to_vec(),
                    })
                    .collect(),
                half_side: half_side.into(),
            };
            Row::from_raw(id, raw)?
        }
    };

    Ok(row.verify(id, &header.dah)?)
}

fn verify_namespace_data(
    header: &ExtendedHeader,
    namespace: Namespace,
    data: &NamespaceData,
) -> Result<()> {
    let id = NamespaceDataId::new(namespace, header.height())?;
    Ok(data.verify(id, &header.dah)?)
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

    use std::ops::Range;

    use celestia_types::sample::Sample;
    use celestia_types::test_utils::{
        ExtendedHeaderGenerator, generate_dummy_eds, share_proof_for_range,
    };
    use celestia_types::{AxisType, DataAvailabilityHeader, ExtendedHeader};

    use crate::api::share::RowSide;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test as test;

    fn header_for(eds: &ExtendedDataSquare) -> ExtendedHeader {
        ExtendedHeaderGenerator::new().next_with_dah(DataAvailabilityHeader::from_eds(eds))
    }

    fn ods_shares(eds: &ExtendedDataSquare, range: Range<usize>) -> Vec<Share> {
        let ods_width = usize::from(eds.square_width() / 2);
        range
            .map(|idx| {
                eds.share((idx / ods_width) as u16, (idx % ods_width) as u16)
                    .unwrap()
                    .clone()
            })
            .collect()
    }

    #[test]
    fn range_response_is_verified() {
        let eds = generate_dummy_eds(8);
        let header = header_for(&eds);
        let response = GetRangeResponse {
            shares: ods_shares(&eds, 2..6),
            proof: share_proof_for_range(&eds, 2..6),
        };

        verify_range_response(&header, 2, 6, &response).unwrap();
    }

    #[test]
    fn range_response_at_other_range_is_rejected() {
        let eds = generate_dummy_eds(8);
        let header = header_for(&eds);
        let response = GetRangeResponse {
            shares: ods_shares(&eds, 2..6),
            proof: share_proof_for_range(&eds, 2..6),
        };

        verify_range_response(&header, 3, 7, &response).unwrap_err();
    }

    #[test]
    fn range_response_with_shares_not_matching_proof_is_rejected() {
        let eds = generate_dummy_eds(8);
        let header = header_for(&eds);
        let response = GetRangeResponse {
            shares: ods_shares(&eds, 3..7),
            proof: share_proof_for_range(&eds, 2..6),
        };

        verify_range_response(&header, 2, 6, &response).unwrap_err();
    }

    #[test]
    fn samples_are_verified() {
        let eds = generate_dummy_eds(8);
        let header = header_for(&eds);
        let coordinates = [SampleCoordinates::new(1, 2), SampleCoordinates::new(5, 3)];
        let samples = [
            Sample::new(1, 2, AxisType::Row, &eds).unwrap(),
            Sample::new(5, 3, AxisType::Col, &eds).unwrap(),
        ];

        verify_samples(&header, &coordinates, &samples).unwrap();
    }

    #[test]
    fn sample_of_other_coordinates_is_rejected() {
        let eds = generate_dummy_eds(8);
        let header = header_for(&eds);
        let coordinates = [SampleCoordinates::new(1, 2)];
        let samples = [Sample::new(1, 3, AxisType::Row, &eds).unwrap()];

        verify_samples(&header, &coordinates, &samples).unwrap_err();
    }

    #[test]
    fn eds_is_verified() {
        let eds = generate_dummy_eds(8);
        let header = header_for(&eds);

        verify_eds(&header, &eds).unwrap();
    }

    #[test]
    fn eds_of_other_block_is_rejected() {
        let eds = generate_dummy_eds(8);
        let header = header_for(&generate_dummy_eds(8));

        verify_eds(&header, &eds).unwrap_err();
    }

    #[test]
    fn row_response_is_verified() {
        let eds = generate_dummy_eds(8);
        let header = header_for(&eds);
        let row = eds.row(1).unwrap();

        for (side, shares) in [
            (RowSide::Left, row[..4].to_vec()),
            (RowSide::Right, row[4..].to_vec()),
            (RowSide::Both, row.clone()),
        ] {
            let response = GetRowResponse { shares, side };
            verify_row(&header, 1, &response).unwrap();
        }
    }

    #[test]
    fn row_response_with_share_of_other_row_is_rejected() {
        let eds = generate_dummy_eds(8);
        let header = header_for(&eds);
        let mut shares = eds.row(1).unwrap();
        shares[2] = eds.share(2, 2).unwrap().clone();
        let response = GetRowResponse {
            shares,
            side: RowSide::Both,
        };

        verify_row(&header, 1, &response).unwrap_err();
    }

    #[test]
    fn namespace_data_is_verified() {
        let eds = generate_dummy_eds(8);
        let header = header_for(&eds);
        let namespace = eds.share(0, 0).unwrap().namespace();
        let rows = eds
            .get_namespace_data(namespace, &header.dah, header.height())
            .unwrap()
            .into_iter()
            .map(|(_, row)| row)
            .collect();

        verify_namespace_data(&header, namespace, &NamespaceData::new(rows)).unwrap();
    }

    #[test]
    fn namespace_data_missing_a_row_is_rejected() {
        let eds = generate_dummy_eds(8);
        let header = header_for(&eds);
        let namespace = eds.share(0, 0).unwrap().namespace();
        let mut rows: Vec<_> = eds
            .get_namespace_data(namespace, &header.dah, header.height())
            .unwrap()
            .into_iter()
            .map(|(_, row)| row)
            .collect();
        rows.pop();

        verify_namespace_data(&header, namespace, &NamespaceData::new(rows)).unwrap_err();
    }
}
