use std::cmp::Ordering;
use std::collections::HashSet;
use std::time::Duration;

use celestia_rpc::ShareClient;
use celestia_types::nmt::Namespace;
use celestia_types::{AppVersion, Blob, ExtendedHeader};
use futures::stream::FuturesUnordered;
use futures::{FutureExt, StreamExt};
use lumina_node::blockstore::InMemoryBlockstore;
use lumina_node::events::NodeEvent;
use lumina_node::node::P2pError;
use lumina_node::store::InMemoryStore;
use lumina_node::test_utils::test_node_builder;
use lumina_node::{Node, NodeError};
use lumina_utils::test_utils::async_test;
use lumina_utils::time::timeout;
use rand::RngCore;

use crate::utils::{
    blob_submit, bridge_client, new_connected_node, new_connected_node_with_builder,
};

mod utils;

// TODO: running each of those tests in browser separately makes them pass in milliseconds,
// but running them all at once hangs after 2-3 tests. Probably an issue with opening connections
// to the same go node over and over. Couldn't dig deep enough to understand it fully tho. Would
// need to investigate the QLOG of chrome to see what happens on quic level, maybe it is
// https://issues.chromium.org/issues/400699540
// ^- above link is now private, which means they think it is security issue, it was public before
// more info: https://github.com/celestiaorg/lumina/issues/287, it's the old bug

#[async_test]
async fn shwap_sampling_forward() {
    #[cfg(target_arch = "wasm32")]
    use tracing_subscriber::prelude::*;

    #[cfg(target_arch = "wasm32")]
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
        .with_writer(tracing_web::MakeConsoleWriter) // write events to the console
        .with_filter(tracing_subscriber::filter::LevelFilter::DEBUG);

    #[cfg(target_arch = "wasm32")]
    let _ = tracing_subscriber::registry().with(fmt_layer).try_init();
    let (node, _) = new_connected_node().await;

    // create new events sub to ignore all previous events
    let mut events = node.event_subscriber();

    for _ in 0..5 {
        // wait for new block
        let get_new_head = async {
            loop {
                let ev = events.recv().await.unwrap();
                let NodeEvent::AddedHeaderFromHeaderSub { height, .. } = ev.event else {
                    continue;
                };
                break height;
            }
        };
        // Timeout is double of the block time on CI
        let new_head = timeout(Duration::from_secs(9), get_new_head).await.unwrap();

        // wait for height to be sampled
        let wait_height_sampled = async {
            loop {
                let ev = events.recv().await.unwrap();
                let NodeEvent::SamplingResult { height, failed, .. } = ev.event else {
                    continue;
                };

                if height == new_head {
                    assert!(!failed);
                    break;
                }
            }
        };
        timeout(Duration::from_secs(1), wait_height_sampled)
            .await
            .unwrap();
    }

    // shouldn't be needed but I'm trying anything
    node.stop().await;
}

#[async_test]
async fn shwap_sampling_backward() {
    #[cfg(target_arch = "wasm32")]
    use tracing_subscriber::prelude::*;

    #[cfg(target_arch = "wasm32")]
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
        .with_writer(tracing_web::MakeConsoleWriter) // write events to the console
        .with_filter(tracing_subscriber::filter::LevelFilter::DEBUG);

    #[cfg(target_arch = "wasm32")]
    let _ = tracing_subscriber::registry().with(fmt_layer).try_init();
    let (node, mut events) = new_connected_node().await;

    let current_head = node.get_local_head_header().await.unwrap().height();

    // wait for some past headers to be synchronized
    let new_batch_synced = async {
        loop {
            let ev = events.recv().await.unwrap();
            let NodeEvent::FetchingHeadersFinished {
                from_height,
                to_height,
                ..
            } = ev.event
            else {
                continue;
            };
            if to_height < current_head {
                break (from_height, to_height);
            }
        }
    };
    let (from_height, to_height) = timeout(Duration::from_secs(5), new_batch_synced)
        .await
        .unwrap();

    // take just first N headers because batch size can be big
    let mut headers_to_sample: HashSet<_> = (from_height..to_height).rev().take(10).collect();

    // wait for all heights to be sampled
    timeout(Duration::from_secs(10), async {
        loop {
            let ev = events.recv().await.unwrap();
            let NodeEvent::SamplingResult { height, failed, .. } = ev.event else {
                continue;
            };

            assert!(!failed);
            headers_to_sample.remove(&height);

            if headers_to_sample.is_empty() {
                break;
            }
        }
    })
    .await
    .unwrap();

    node.stop().await;
}

#[async_test]
async fn shwap_request_sample() {
    #[cfg(target_arch = "wasm32")]
    use tracing_subscriber::prelude::*;

    #[cfg(target_arch = "wasm32")]
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
        .with_writer(tracing_web::MakeConsoleWriter) // write events to the console
        .with_filter(tracing_subscriber::filter::LevelFilter::DEBUG);

    #[cfg(target_arch = "wasm32")]
    let _ = tracing_subscriber::registry().with(fmt_layer).try_init();
    let (node, _) = new_connected_node().await;
    let client = bridge_client().await;

    let ns = Namespace::const_v0(rand::random());
    let blob_len = rand::random::<usize>() % 4096 + 1;
    let blob = Blob::new(ns, random_bytes(blob_len), None, AppVersion::V2).unwrap();

    let height = blob_submit(&client, &[blob]).await;
    let header = wait_for_height(&node, height).await;
    let square_width = header.square_width();

    // check existing sample
    let expected = client
        .share_get_share(
            header.height(),
            header.app_version(),
            header.square_width(),
            0,
            0,
        )
        .await
        .unwrap();
    let sample = node
        .request_sample(0, 0, height, Some(Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(expected, sample.share);

    // check nonexisting sample
    let err = node
        .request_sample(
            square_width + 1,
            square_width + 1,
            height,
            Some(Duration::from_secs(10)),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, NodeError::P2p(P2pError::ShrEx(_))));

    node.stop().await;
}

#[async_test]
async fn shwap_request_row() {
    #[cfg(target_arch = "wasm32")]
    use tracing_subscriber::prelude::*;

    #[cfg(target_arch = "wasm32")]
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
        .with_writer(tracing_web::MakeConsoleWriter) // write events to the console
        .with_filter(tracing_subscriber::filter::LevelFilter::DEBUG);

    #[cfg(target_arch = "wasm32")]
    let _ = tracing_subscriber::registry().with(fmt_layer).try_init();
    let (node, _) = new_connected_node().await;
    let client = bridge_client().await;

    let ns = Namespace::const_v0(rand::random());
    let blob_len = rand::random::<usize>() % 4096 + 1;
    let blob = Blob::new(ns, random_bytes(blob_len), None, AppVersion::V2).unwrap();

    let height = blob_submit(&client, &[blob]).await;

    let header = wait_for_height(&node, height).await;
    let eds = client
        .share_get_eds(header.height(), header.app_version())
        .await
        .unwrap();
    let square_width = header.square_width();

    // check existing row
    let row = node
        .request_row(0, height, Some(Duration::from_secs(1)))
        .await
        .unwrap();
    assert_eq!(eds.row(0).unwrap(), row.shares);

    // check nonexisting row
    let err = node
        .request_row(square_width + 1, height, Some(Duration::from_secs(1)))
        .await
        .unwrap_err();
    assert!(matches!(err, NodeError::P2p(P2pError::ShrEx(_))));

    node.stop().await;
}

#[async_test]
async fn shwap_request_all_blobs() {
    #[cfg(target_arch = "wasm32")]
    use tracing_subscriber::prelude::*;

    #[cfg(target_arch = "wasm32")]
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
        .with_writer(tracing_web::MakeConsoleWriter) // write events to the console
        .with_filter(tracing_subscriber::filter::LevelFilter::DEBUG);

    #[cfg(target_arch = "wasm32")]
    let _ = tracing_subscriber::registry().with(fmt_layer).try_init();
    let (node, _) = new_connected_node().await;
    let client = bridge_client().await;

    let ns = Namespace::const_v0(rand::random());
    let blobs: Vec<_> = (0..5)
        .map(|_| {
            let blob_len = rand::random::<usize>() % 4096 + 1;
            Blob::new(ns, random_bytes(blob_len), None, AppVersion::V2).unwrap()
        })
        .collect();

    let height = blob_submit(&client, &blobs).await;
    wait_for_height(&node, height).await;

    // check existing namespace
    let received = node
        .request_all_blobs(ns, height, Some(Duration::from_secs(2)))
        .await
        .unwrap();

    assert_eq!(blobs, received);

    // check nonexisting namespace
    let ns = Namespace::const_v0(rand::random());
    let received = node
        .request_all_blobs(ns, height, Some(Duration::from_secs(2)))
        .await
        .unwrap();

    assert!(received.is_empty());

    node.stop().await;
}

#[async_test]
async fn shwap_request_concurrent() {
    #[cfg(target_arch = "wasm32")]
    use tracing_subscriber::prelude::*;

    #[cfg(target_arch = "wasm32")]
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
        .with_writer(tracing_web::MakeConsoleWriter) // write events to the console
        .with_filter(tracing_subscriber::filter::LevelFilter::DEBUG);

    #[cfg(target_arch = "wasm32")]
    let _ = tracing_subscriber::registry().with(fmt_layer).try_init();

    let builder = test_node_builder().pruning_window(Duration::from_secs(60));
    let (node, _) = new_connected_node_with_builder(builder).await;
    let client = bridge_client().await;

    let blob_ns = Namespace::const_v0(rand::random());

    let mut headers = Vec::new();

    for _ in 0..3 {
        let blobs: Vec<_> = (0..3)
            .map(|_| {
                let blob_len = rand::random::<usize>() % 512 + 1;
                Blob::new(blob_ns, random_bytes(blob_len), None, AppVersion::V2).unwrap()
            })
            .collect();

        let height = blob_submit(&client, &blobs).await;
        headers.push(wait_for_height(&node, height).await);
    }

    let mut futs = FuturesUnordered::new();

    for hdr in &headers {
        // samples for each header
        for _ in 0..4 {
            let row = rand::random::<u16>() % hdr.square_width();
            let col = rand::random::<u16>() % hdr.square_width();

            futs.push(
                node.request_sample(row, col, hdr.height(), Some(Duration::from_secs(1)))
                    .map(|res| res.map(|_| ()))
                    .boxed(),
            );
        }

        // rows
        for _ in 0..2 {
            let row = rand::random::<u16>() % hdr.square_width();

            futs.push(
                node.request_row(row, hdr.height(), Some(Duration::from_secs(1)))
                    .map(|res| res.map(|_| ()))
                    .boxed(),
            );
        }

        // namespace data
        for ns in [Namespace::PAY_FOR_BLOB, blob_ns] {
            futs.push(
                node.request_namespace_data(ns, hdr.height(), Some(Duration::from_secs(2)))
                    .map(|res| res.map(|_| ()))
                    .boxed(),
            );
        }

        // eds
        futs.push(
            node.request_extended_data_square(hdr.height(), Some(Duration::from_secs(2)))
                .map(|res| res.map(|_| ()))
                .boxed(),
        );
    }

    while let Some(res) = futs.next().await {
        res.expect("Request failed");
    }

    drop(futs);
    node.stop().await;
}

fn random_bytes(len: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; len];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes
}

async fn wait_for_height(
    node: &Node<InMemoryBlockstore, InMemoryStore>,
    height: u64,
) -> ExtendedHeader {
    if let Ok(hdr) = node.get_header_by_height(height).await {
        return hdr;
    }

    // we didn't find header, so let's wait for it on subscription
    let mut sub = node.header_subscribe().await.unwrap();
    loop {
        let hdr = sub.recv().await.unwrap();

        match hdr.height().cmp(&height) {
            Ordering::Less => continue,
            Ordering::Equal => return hdr,
            Ordering::Greater => break,
        }
    }

    // check last time with get by height, maybe it was inserted in a moment that
    // we didn't get it previously yet but also missed it on subscription
    node.get_header_by_height(height).await.unwrap()
}
