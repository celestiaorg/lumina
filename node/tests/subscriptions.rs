use std::{array::from_ref, time::Duration};

use celestia_types::{Blob, blob::BlobsAtHeight, nmt::Namespace};
use futures::stream::StreamExt;
use lumina_utils::time::sleep;

use crate::utils::{blob_submit, bridge_client, new_connected_node};

mod utils;

#[tokio::test]
async fn header_subscription() {
    let (node, _) = new_connected_node().await;

    let mut header_stream = node.header_subscribe().await.unwrap();
    let current_head = node.get_local_head_header().await.unwrap();

    // we'll get either current_head, or the next header
    let h0 = header_stream.recv().await.unwrap();
    assert!(h0.height() - current_head.height() <= 1);

    let h1 = header_stream.recv().await.unwrap();
    assert_eq!(h0.height() + 1, h1.height());

    let h2 = header_stream.recv().await.unwrap();
    assert_eq!(h1.height() + 1, h2.height());

    let h3 = header_stream.recv().await.unwrap();
    assert_eq!(h2.height() + 1, h3.height());

    let tail_header = node.get_header_by_height(h3.height()).await.unwrap();
    assert_eq!(h3, tail_header);
}

#[tokio::test]
async fn blob_subscription() {
    let (node, _) = new_connected_node().await;
    let client = bridge_client().await;

    let namespace = Namespace::new_v0(&[200, 199, 198, 197, 196, 195, 194, 193, 192, 191]).unwrap();
    let data = b"bleb0";
    let blob = Blob::new(namespace, data.to_vec(), None).unwrap();

    let mut blob_stream = node.blob_subscribe(namespace).await.unwrap();

    sleep(Duration::from_secs(2)).await;

    let submitted_at = blob_submit(&client, from_ref(&blob)).await;

    let blobs = loop {
        let BlobsAtHeight { height, blobs } = blob_stream.next().await.unwrap().unwrap();
        if height == submitted_at {
            break blobs;
        }
        assert!(height < submitted_at);
        assert!(blobs.is_empty());
    };

    assert_eq!(&blobs, from_ref(&blob));

    let BlobsAtHeight { blobs, .. } = blob_stream.next().await.unwrap().unwrap();
    assert_eq!(blobs, vec![]);
}
