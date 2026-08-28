#![cfg(not(target_arch = "wasm32"))]

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use celestia_rpc::ShareClient;
use celestia_types::nmt::Namespace;
use celestia_types::{Blob, ExtendedHeader};
use lumina_node::blockstore::InMemoryBlockstore;
use lumina_node::events::NodeEvent;
use lumina_node::node::P2pError;
use lumina_node::store::InMemoryStore;
use lumina_node::{Node, NodeError};
use rand::RngCore;
use tokio::time::timeout;

use crate::utils::{blob_submit, bridge_client, new_connected_node};

mod utils;

#[tokio::test]
async fn shrex_sampling_forward() {
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
        // timeout is double of the block time on CI
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
}

#[tokio::test]
async fn shrex_sampling_backward() {
    let (node, mut events) = new_connected_node().await;

    let current_head = node.get_local_head_header().await.unwrap().height();
    let peer_id = *node.local_peer_id();
    tracing::info!(
        target: "lumina_node::tests",
        test = "shrex_sampling_backward",
        %peer_id,
        current_head,
        "node connected"
    );

    // Diagnostics for the CI flake: count every event variant the subscriber delivers
    // and log the header-fetching ones, so we can tell whether `FetchingHeadersFinished`
    // was never emitted or was emitted but lost (e.g. evicted from the event channel).
    let mut events_seen: BTreeMap<&'static str, usize> = BTreeMap::new();
    let events_seen_ref = &mut events_seen;

    // wait for some past headers to be synchronized
    let new_batch_synced = async {
        loop {
            let ev = events.recv().await.unwrap();
            *events_seen_ref
                .entry(event_variant_name(&ev.event))
                .or_default() += 1;
            match &ev.event {
                NodeEvent::FetchingHeadersStarted {
                    from_height,
                    to_height,
                } => tracing::info!(
                    target: "lumina_node::tests",
                    test = "shrex_sampling_backward",
                    %peer_id,
                    from_height,
                    to_height,
                    "FetchingHeadersStarted"
                ),
                NodeEvent::FetchingHeadersFailed {
                    from_height,
                    to_height,
                    error,
                    took,
                } => tracing::info!(
                    target: "lumina_node::tests",
                    test = "shrex_sampling_backward",
                    %peer_id,
                    from_height,
                    to_height,
                    ?took,
                    error,
                    "FetchingHeadersFailed"
                ),
                NodeEvent::FetchingHeadersFinished {
                    from_height,
                    to_height,
                    took,
                } => tracing::info!(
                    target: "lumina_node::tests",
                    test = "shrex_sampling_backward",
                    %peer_id,
                    from_height,
                    to_height,
                    ?took,
                    "FetchingHeadersFinished"
                ),
                _ => {}
            }
            let NodeEvent::FetchingHeadersFinished {
                from_height,
                to_height,
                ..
            } = ev.event
            else {
                continue;
            };
            if from_height < current_head {
                break (from_height, to_height);
            }
        }
    };
    let batch = timeout(Duration::from_secs(30), new_batch_synced).await;

    if batch.is_err() {
        // Dump everything we know before failing. `eprintln!` lands in the job log,
        // `info!` in `LUMINA_TEST_LOG`.
        let local_head = node.get_local_head_header().await.map(|h| h.height());
        let mut stored_below_head = 0u64;
        for height in 1..current_head {
            if node.get_header_by_height(height).await.is_ok() {
                stored_below_head += 1;
            }
        }
        let expected_below_head = current_head.saturating_sub(1);
        eprintln!(
            "shrex_sampling_backward: {peer_id} timed out waiting for a backward batch; \
             head at start {current_head}, local head now {local_head:?}, \
             headers stored below start head: {stored_below_head}/{expected_below_head}, \
             events received by variant: {events_seen:?}"
        );
        tracing::info!(
            target: "lumina_node::tests",
            test = "shrex_sampling_backward",
            %peer_id,
            current_head,
            ?local_head,
            stored_below_head,
            expected_below_head,
            ?events_seen,
            "timed out waiting for a backward batch"
        );
    }

    let (from_height, to_height) = batch.unwrap();

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
}

#[tokio::test]
async fn shrex_request_sample() {
    let (node, _) = new_connected_node().await;
    let client = bridge_client().await;

    let ns = Namespace::const_v0(rand::random());
    let blob_len = rand::random::<usize>() % 4096 + 1;
    let blob = Blob::new(ns, random_bytes(blob_len), None).unwrap();

    let height = blob_submit(&client, &[blob]).await;
    let header = wait_for_height(&node, height).await;
    let square_width = header.square_width();

    // check existing sample
    let expected = client
        .share_get_share(header.height(), header.square_width(), 0, 0)
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
            Some(Duration::from_secs(1)),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, NodeError::P2p(P2pError::ShrEx(_))));
}

#[tokio::test]
async fn shrex_request_row() {
    let (node, _) = new_connected_node().await;
    let client = bridge_client().await;

    let ns = Namespace::const_v0(rand::random());
    let blob_len = rand::random::<usize>() % 4096 + 1;
    let blob = Blob::new(ns, random_bytes(blob_len), None).unwrap();

    let height = blob_submit(&client, &[blob]).await;

    let header = wait_for_height(&node, height).await;
    let eds = client.share_get_eds(header.height()).await.unwrap();
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
}

#[tokio::test]
async fn shrex_request_all_blobs() {
    let (node, _) = new_connected_node().await;
    let client = bridge_client().await;

    let ns = Namespace::const_v0(rand::random());
    let blobs: Vec<_> = (0..5)
        .map(|_| {
            let blob_len = rand::random::<usize>() % 4096 + 1;
            Blob::new(ns, random_bytes(blob_len), None).unwrap()
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

/// Stable name of the [`NodeEvent`] variant, used for per-variant event counters in diagnostics.
fn event_variant_name(event: &NodeEvent) -> &'static str {
    match event {
        NodeEvent::ConnectingToBootnodes => "ConnectingToBootnodes",
        NodeEvent::PeerConnected { .. } => "PeerConnected",
        NodeEvent::PeerDisconnected { .. } => "PeerDisconnected",
        NodeEvent::SamplingStarted { .. } => "SamplingStarted",
        NodeEvent::ShareSamplingResult { .. } => "ShareSamplingResult",
        NodeEvent::SamplingResult { .. } => "SamplingResult",
        NodeEvent::FatalDaserError { .. } => "FatalDaserError",
        NodeEvent::AddedHeaderFromHeaderSub { .. } => "AddedHeaderFromHeaderSub",
        NodeEvent::FetchingHeadHeaderStarted => "FetchingHeadHeaderStarted",
        NodeEvent::FetchingHeadHeaderFinished { .. } => "FetchingHeadHeaderFinished",
        NodeEvent::FetchingHeadersStarted { .. } => "FetchingHeadersStarted",
        NodeEvent::FetchingHeadersFinished { .. } => "FetchingHeadersFinished",
        NodeEvent::FetchingHeadersFailed { .. } => "FetchingHeadersFailed",
        NodeEvent::FatalSyncerError { .. } => "FatalSyncerError",
        NodeEvent::PrunedHeaders { .. } => "PrunedHeaders",
        NodeEvent::FatalPrunerError { .. } => "FatalPrunerError",
        NodeEvent::NetworkCompromised => "NetworkCompromised",
        NodeEvent::NodeStopped => "NodeStopped",
        _ => "Other",
    }
}
