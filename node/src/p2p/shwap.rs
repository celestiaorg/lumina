use std::sync::Arc;

use beetswap::multihasher::{Multihasher, MultihasherError};
use blockstore::block::CidError;
use celestia_proto::bitswap::Block;
use celestia_types::row::{ROW_ID_MULTIHASH_CODE, Row, RowId};
use celestia_types::row_namespace_data::{
    ROW_NAMESPACE_DATA_ID_MULTIHASH_CODE, RowNamespaceData, RowNamespaceDataId,
};
use celestia_types::sample::{SAMPLE_ID_MULTIHASH_CODE, Sample, SampleId};
use cid::{Cid, CidGeneric};
use libp2p::multihash::Multihash;
use prost::Message;

use tracing::info;

use crate::p2p::{MAX_MH_SIZE, P2pError, Result};
use crate::store::Store;

/// Multihasher for Shwap types.
pub(super) struct ShwapMultihasher<S>
where
    S: Store + 'static,
{
    header_store: Arc<S>,
}

impl<S> ShwapMultihasher<S>
where
    S: Store + 'static,
{
    pub(super) fn new(header_store: Arc<S>) -> Self {
        ShwapMultihasher { header_store }
    }
}

impl<S> Multihasher<MAX_MH_SIZE> for ShwapMultihasher<S>
where
    S: Store + 'static,
{
    async fn hash(
        &self,
        multihash_code: u64,
        input: &[u8],
    ) -> Result<Multihash<MAX_MH_SIZE>, MultihasherError> {
        macro_rules! hash_shwap_block {
            ($id_type:ty, $container_type:ty) => {{
                let block = Block::decode(input).map_err(MultihasherError::custom_fatal)?;
                let cid = CidGeneric::<MAX_MH_SIZE>::read_bytes(block.cid.as_slice())
                    .map_err(MultihasherError::custom_fatal)?;

                let id = <$id_type>::try_from(cid).map_err(MultihasherError::custom_fatal)?;
                let container = <$container_type>::decode(id, block.container.as_slice())
                    .map_err(MultihasherError::custom_fatal)?;

                let hash = convert_cid(&id.into())
                    .map_err(MultihasherError::custom_fatal)?
                    .hash()
                    .to_owned();

                let header = self
                    .header_store
                    .get_by_height(id.block_height())
                    .await
                    .map_err(MultihasherError::custom_fatal)?;

                container
                    .verify(id, &header.dah)
                    .map_err(MultihasherError::custom_fatal)?;

                info!(
                    "Verified {} block at height {}",
                    stringify!($container_type),
                    id.block_height(),
                );

                Ok(hash)
            }};
        }

        match multihash_code {
            ROW_ID_MULTIHASH_CODE => hash_shwap_block!(RowId, Row),
            ROW_NAMESPACE_DATA_ID_MULTIHASH_CODE => {
                hash_shwap_block!(RowNamespaceDataId, RowNamespaceData)
            }
            SAMPLE_ID_MULTIHASH_CODE => hash_shwap_block!(SampleId, Sample),
            _ => Err(MultihasherError::UnknownMultihashCode),
        }
    }
}

pub(crate) fn sample_cid(row_index: u16, column_index: u16, block_height: u64) -> Result<Cid> {
    let sample_id = SampleId::new(row_index, column_index, block_height)?;
    convert_cid(&sample_id.into())
}

pub(crate) fn convert_cid<const S: usize>(cid: &CidGeneric<S>) -> Result<Cid> {
    beetswap::utils::convert_cid(cid).ok_or(P2pError::Cid(CidError::InvalidMultihashLength(64)))
}

/// Extracts the `container` part from shwap's `Block` wrapper if its CID matches the expected one
pub(crate) fn get_block_container(expected_cid: &Cid, block: &[u8]) -> Result<Vec<u8>> {
    let block = Block::decode(block)?;
    let block_cid = Cid::read_bytes(block.cid.as_slice())?;
    if block_cid != *expected_cid {
        return Err(P2pError::Shwap(format!(
            "cid in block ({block_cid}) different than expected ({expected_cid})",
        )));
    }

    Ok(block.container)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::InMemoryStore;
    use bytes::BytesMut;
    use celestia_types::test_utils::{ExtendedHeaderGenerator, generate_dummy_eds};
    use celestia_types::{AxisType, DataAvailabilityHeader};
    use lumina_utils::test_utils::async_test;

    #[async_test]
    async fn hash() {
        let store = Arc::new(InMemoryStore::new());

        let eds = generate_dummy_eds(4);
        let dah = DataAvailabilityHeader::from_eds(&eds);

        let mut generator = ExtendedHeaderGenerator::new();
        let header = generator.next_with_dah(dah.clone());

        let sample = Sample::new(0, 0, AxisType::Row, &eds).unwrap();
        let mut sample_bytes = BytesMut::new();
        sample.encode(&mut sample_bytes);

        let cid = sample_cid(0, 0, 1).unwrap();
        let sample_id = SampleId::new(0, 0, 1).unwrap();

        sample.verify(sample_id, &dah).unwrap();
        store.insert(header).await.unwrap();

        let block = Block {
            cid: cid.to_bytes(),
            container: sample_bytes.to_vec(),
        };

        let hash = ShwapMultihasher::new(store)
            .hash(SAMPLE_ID_MULTIHASH_CODE, &block.encode_to_vec())
            .await
            .unwrap();

        assert_eq!(hash, *cid.hash());
    }
}
