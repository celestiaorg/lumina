use std::iter;

use celestia_rpc::prelude::*;
use celestia_types::Blob;
use celestia_types::nmt::{Namespace, NamespacedSha2Hasher};
use lumina_utils::test_utils::async_test;

pub mod utils;

use crate::utils::client::{AuthLevel, blob_submit, new_test_client};
use crate::utils::{random_bytes, random_ns, random_ns_range};

#[async_test]
async fn get_share() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();
    let header = client.header_network_head().await.unwrap();
    let square_width = header.square_width();

    for row in 0..square_width {
        for col in 0..square_width {
            let share = client
                .share_get_share(header.height(), square_width, row, col)
                .await
                .unwrap();
            let is_parity = row >= square_width / 2 || col >= square_width / 2;

            assert_eq!(share.is_parity(), is_parity);
        }
    }
}

#[async_test]
async fn get_shares_by_namespace() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();
    let namespace = random_ns();
    let blobs: Vec<_> = (0..4)
        .map(|_| {
            let data = random_bytes(1024);
            Blob::new(namespace, data.clone(), None).unwrap()
        })
        .collect();

    let submitted_height = blob_submit(&client, &blobs).await.unwrap();

    let header = client.header_get_by_height(submitted_height).await.unwrap();

    let ns_shares = client
        .share_get_namespace_data(header.height(), namespace)
        .await
        .unwrap();

    let reconstructed =
        Blob::reconstruct_all(ns_shares.rows().iter().flat_map(|row| row.shares.iter())).unwrap();

    assert_eq!(reconstructed, blobs);
}

#[async_test]
async fn get_shares_by_namespace_forbidden() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();
    let header = client.header_network_head().await.unwrap();

    // those namespaces are forbidden in celestia-node's implementation
    for ns in [Namespace::TAIL_PADDING, Namespace::PARITY_SHARE] {
        client
            .share_get_namespace_data(header.height(), ns)
            .await
            .unwrap_err();
    }
}

#[async_test]
async fn get_shares_range() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();
    let namespace = random_ns();
    let data = random_bytes(1024);
    let blob = Blob::new(namespace, data.clone(), None).unwrap();
    let commitment = blob.commitment;

    let submitted_height = blob_submit(&client, &[blob]).await.unwrap();

    let header = client.header_get_by_height(submitted_height).await.unwrap();
    let blob_on_chain = client
        .blob_get(submitted_height, namespace, commitment)
        .await
        .unwrap();
    let index = blob_on_chain.index.unwrap();
    let shares = blob_on_chain.to_shares().unwrap();

    let shares_range = client
        .share_get_range(header.height(), index, index + shares.len() as u64)
        .await
        .unwrap();

    shares_range.proof.verify(header.dah.hash()).unwrap();

    for ((share, received), proven) in shares
        .into_iter()
        .zip(shares_range.shares)
        .zip(shares_range.proof.shares().iter())
    {
        assert_eq!(share, received);
        assert_eq!(share.as_ref(), proven.as_ref());
    }
}

#[async_test]
async fn get_shares_range_not_existing() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();
    let header = client.header_network_head().await.unwrap();
    let shares_in_block = header.dah.square_width().pow(2);

    client
        .share_get_range(
            header.height(),
            shares_in_block as u64 - 2,
            shares_in_block as u64 + 2,
        )
        .await
        .unwrap_err();
}

#[async_test]
async fn get_shares_range_ignores_parity() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();

    let namespace = random_ns();
    let data = random_bytes(100);
    let blob = Blob::new(namespace, data.clone(), None).unwrap();
    let submitted_height = blob_submit(&client, &[blob]).await.unwrap();

    let header = client.header_get_by_height(submitted_height).await.unwrap();
    let square_width = header.dah.square_width();

    let first_parity_share_in_first_row = client
        .share_get_share(header.height(), header.square_width(), 0, square_width / 2)
        .await
        .unwrap();
    let first_ods_share_in_second_row = client
        .share_get_share(header.height(), header.square_width(), 1, 0)
        .await
        .unwrap();

    // Try to get the first parity share in first row
    let fetched_range = client
        .share_get_range(
            header.height(),
            (square_width / 2) as u64,
            (square_width / 2 + 1) as u64,
        )
        .await
        .unwrap()
        .shares;
    assert_eq!(fetched_range.len(), 1);

    // Since parity data is ignored in `get_range` api, we shouldn't receive the parity share
    assert!(first_parity_share_in_first_row.is_parity());
    assert_ne!(first_parity_share_in_first_row, fetched_range[0]);
    // but instead first share from 2nd row of ods
    assert!(!first_ods_share_in_second_row.is_parity());
    assert_eq!(first_ods_share_in_second_row, fetched_range[0]);
}

#[async_test]
async fn get_shares_by_namespace_wrong_ns() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();
    let namespace = random_ns();
    let data = random_bytes(1024);
    let blob = Blob::new(namespace, data.clone(), None).unwrap();

    let submitted_height = blob_submit(&client, &[blob]).await.unwrap();

    let header = client.header_get_by_height(submitted_height).await.unwrap();

    let root_hash = header.dah.row_root(0).unwrap();
    let min_ns = root_hash.min_namespace().into();
    let max_ns = root_hash.max_namespace().into();

    // When we try to get shares for the unknown namespace then
    // if there exists a row where row_root.min_namespace() < namespace < row_root.max_namespace()
    // then we will get an absence proof for that row
    // The rows where namespace falls outside of the ns range of the row_root
    // are not included in the response.
    // As the block has just a single blob, we can only care about the first row.

    // check the case where we receive absence proof
    let random_ns = random_ns_range(min_ns, max_ns);
    let ns_shares = client
        .share_get_namespace_data(header.height(), random_ns)
        .await
        .unwrap();
    assert_eq!(ns_shares.rows().len(), 1);
    assert!(ns_shares.rows()[0].shares.is_empty());

    let proof = &ns_shares.rows()[0].proof;
    assert!(proof.is_of_absence());

    let no_leaves: &[&[u8]] = &[];

    proof
        .verify_complete_namespace(&root_hash, no_leaves, random_ns.into())
        .unwrap();
}

#[async_test]
async fn get_shares_by_namespace_wrong_ns_out_of_range() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();
    let namespace = random_ns();
    let data = random_bytes(1024);
    let blob = Blob::new(namespace, data.clone(), None).unwrap();

    let submitted_height = blob_submit(&client, &[blob]).await.unwrap();

    let header = client.header_get_by_height(submitted_height).await.unwrap();

    let root_hash = header.dah.row_root(0).unwrap();
    let min_ns = root_hash.min_namespace().into();

    // check the case where namespace is outside of the root hash range
    let zero = Namespace::const_v0([0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    let random_ns = random_ns_range(zero, min_ns);
    let ns_shares = client
        .share_get_namespace_data(header.height(), random_ns)
        .await
        .unwrap();

    assert!(ns_shares.rows().is_empty());
    assert!(!root_hash.contains::<NamespacedSha2Hasher>(random_ns.into()));
}

#[async_test]
async fn get_shares_by_namespace_wrong_roots() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();
    let namespace = random_ns();
    let data = random_bytes(1024);
    let blob = Blob::new(namespace, data.clone(), None).unwrap();

    blob_submit(&client, &[blob]).await.unwrap();

    let genesis = client.header_get_by_height(1).await.unwrap();

    let ns_shares = client
        .share_get_namespace_data(genesis.height(), namespace)
        .await
        .unwrap();

    assert!(ns_shares.rows().is_empty());
}

#[async_test]
async fn get_eds() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();
    let namespace = random_ns();
    let data = vec![1, 2, 3, 4];
    let blob = Blob::new(namespace, data.clone(), None).unwrap();

    let submitted_height = blob_submit(&client, &[blob]).await.unwrap();

    let header = client.header_get_by_height(submitted_height).await.unwrap();
    let eds = client.share_get_eds(header.height()).await.unwrap();

    for i in 0..header.dah.square_width() {
        let row_root = eds.row_nmt(i).unwrap().root();
        assert_eq!(row_root, header.dah.row_root(i).unwrap());

        let column_root = eds.column_nmt(i).unwrap().root();
        assert_eq!(column_root, header.dah.column_root(i).unwrap());
    }
}

#[async_test]
async fn get_samples() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();
    let namespace = random_ns();
    let data = random_bytes(1024);
    let blob = Blob::new(namespace, data.clone(), None).unwrap();

    let submitted_height = blob_submit(&client, &[blob]).await.unwrap();
    let header = client.header_get_by_height(submitted_height).await.unwrap();

    let parity_idx = header.dah.square_width() / 2;

    let ods_shares = (0..parity_idx).flat_map(|x| iter::repeat(x).zip(0..parity_idx));
    let parity_shares =
        (parity_idx..2 * parity_idx).flat_map(|x| iter::repeat(x).zip(parity_idx..2 * parity_idx));

    for sample in client
        .share_get_samples(header.height(), ods_shares)
        .await
        .unwrap()
    {
        assert!(!sample.share.is_parity());
    }

    for sample in client
        .share_get_samples(header.height(), parity_shares)
        .await
        .unwrap()
    {
        assert!(sample.share.is_parity());
    }
}

#[async_test]
async fn get_samples_wrong_coords() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();
    let namespace = random_ns();
    let data = random_bytes(1024);
    let blob = Blob::new(namespace, data.clone(), None).unwrap();

    let submitted_height = blob_submit(&client, &[blob]).await.unwrap();
    let header = client.header_get_by_height(submitted_height).await.unwrap();

    let square_width = header.dah.square_width();

    for coords in [
        (square_width - 1, square_width),
        (square_width, square_width - 1),
        (square_width, square_width),
        (square_width * 2, square_width * 2),
    ] {
        client
            .share_get_samples(header.height(), [coords])
            .await
            .unwrap_err();
    }
}

#[async_test]
async fn get_shares_by_row() {
    let client = new_test_client(AuthLevel::Skip).await.unwrap();
    let namespace = random_ns();
    let data = random_bytes(1024);
    let blob = Blob::new(namespace, data.clone(), None).unwrap();
    let commitment = blob.commitment;

    let submitted_height = blob_submit(&client, &[blob]).await.unwrap();

    let header = client.header_get_by_height(submitted_height).await.unwrap();
    let blob_on_chain = client
        .blob_get(submitted_height, namespace, commitment)
        .await
        .unwrap();
    let blob_index = blob_on_chain.index.unwrap() as usize;
    let shares = blob_on_chain.to_shares().unwrap();

    let ods_width = (header.dah.square_width() / 2) as usize;
    let row_index = blob_index / ods_width;
    let index_in_row = (blob_index % ods_width) as usize;

    let n_rows_to_fetch = (index_in_row + shares.len()).div_ceil(ods_width);
    let mut rows = Vec::with_capacity(n_rows_to_fetch);
    for i in 0..n_rows_to_fetch {
        let row = client
            .share_get_row(
                header.height(),
                header.square_width(),
                (row_index + i) as u16,
            )
            .await
            .unwrap();
        rows.push(row);
    }

    for (i, share) in shares.iter().enumerate() {
        let row_index = (blob_index + i) / ods_width;
        let index_in_row = (blob_index + i) % ods_width;

        assert_eq!(share, &rows[row_index].shares[index_in_row]);
    }
}
