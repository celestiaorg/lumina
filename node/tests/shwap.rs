#![cfg(not(target_arch = "wasm32"))]

use std::collections::HashSet;
use std::sync::OnceLock;
use std::time::Duration;

use beetswap::utils::convert_cid;
use blockstore::Blockstore;
use celestia_rpc::{HeaderClient, ShareClient};
use celestia_types::nmt::{Namespace, NamespacedSha2Hasher};
use celestia_types::sample::SampleId;
use celestia_types::{Blob, ExtendedHeader};
use cid::{Cid, CidGeneric};
use lumina_node::NodeError;
use lumina_node::blockstore::InMemoryBlockstore;
use lumina_node::events::NodeEvent;
use lumina_node::node::P2pError;
use lumina_node::test_utils::test_node_builder;
use rand::RngCore;
use tokio::sync::{Mutex, mpsc};
use tokio::time::timeout;
use utils::new_connected_node_with_builder;

use crate::utils::{blob_submit, bridge_client, new_connected_node};

mod utils;

/// Serialize tests so only one node connects to the bridge at a time.
fn test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[tokio::test]
async fn shwap_sampling_forward() {
    let _guard = test_lock().lock().await;
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
                let NodeEvent::SamplingResult {
                    height, timed_out, ..
                } = ev.event
                else {
                    continue;
                };

                if height == new_head {
                    assert!(!timed_out);
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
async fn shwap_sampling_backward() {
    let _guard = test_lock().lock().await;
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
    let (from_height, to_height) = timeout(Duration::from_secs(4), new_batch_synced)
        .await
        .unwrap();

    // take just first N headers because batch size can be big
    let mut headers_to_sample: HashSet<_> = (from_height..to_height).rev().take(10).collect();

    // wait for all heights to be sampled
    timeout(Duration::from_secs(10), async {
        loop {
            let ev = events.recv().await.unwrap();
            let NodeEvent::SamplingResult {
                height, timed_out, ..
            } = ev.event
            else {
                continue;
            };

            assert!(!timed_out);
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
async fn shwap_request_sample() {
    let _guard = test_lock().lock().await;
    let (node, _) = new_connected_node().await;
    let client = bridge_client().await;

    let ns = Namespace::const_v0(rand::random());
    let blob_len = rand::random::<usize>() % 4096 + 1;
    let blob = Blob::new(ns, random_bytes(blob_len), None).unwrap();

    let height = blob_submit(&client, &[blob]).await;
    let header = node.get_header_by_height(height).await.unwrap();
    let square_width = header.square_width();

    // check existing sample
    let expected = client
        .share_get_share(header.height(), header.square_width(), 0, 0)
        .await
        .unwrap();
    let sample = node
        .request_sample(0, 0, height, Some(Duration::from_millis(500)))
        .await
        .unwrap();
    assert_eq!(expected, sample.share);

    // check nonexisting sample
    let err = node
        .request_sample(
            square_width + 1,
            square_width + 1,
            height,
            Some(Duration::from_millis(500)),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, NodeError::P2p(P2pError::RequestTimedOut)));
}

#[tokio::test]
async fn shwap_request_row() {
    let _guard = test_lock().lock().await;
    let (node, _) = new_connected_node().await;
    let client = bridge_client().await;

    let ns = Namespace::const_v0(rand::random());
    let blob_len = rand::random::<usize>() % 4096 + 1;
    let blob = Blob::new(ns, random_bytes(blob_len), None).unwrap();

    let height = blob_submit(&client, &[blob]).await;
    let header = node.get_header_by_height(height).await.unwrap();
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
    assert!(matches!(err, NodeError::P2p(P2pError::RequestTimedOut)));
}

#[tokio::test]
async fn shwap_request_row_namespace_data() {
    let _guard = test_lock().lock().await;
    let (node, _) = new_connected_node().await;
    let client = bridge_client().await;

    let ns = Namespace::const_v0(rand::random());
    let blob_len = rand::random::<usize>() % 4096 + 1;
    let blob = Blob::new(ns, random_bytes(blob_len), None).unwrap();

    let height = blob_submit(&client, &[blob]).await;
    let header = node.get_header_by_height(height).await.unwrap();
    let eds = client.share_get_eds(header.height()).await.unwrap();
    let square_width = header.square_width();

    // check existing row namespace data
    let rows_with_ns: Vec<_> = header
        .dah
        .row_roots()
        .iter()
        .enumerate()
        .filter_map(|(n, hash)| {
            hash.contains::<NamespacedSha2Hasher>(*ns)
                .then_some(n as u16)
        })
        .collect();
    let eds_ns_data = eds.get_namespace_data(ns, &header.dah, height).unwrap();

    for (n, &row) in rows_with_ns.iter().enumerate() {
        let row_ns_data = node
            .request_row_namespace_data(ns, row, height, Some(Duration::from_secs(1)))
            .await
            .unwrap();
        assert_eq!(eds_ns_data[n].1, row_ns_data);
    }

    // check nonexisting row row namespace data
    let err = node
        .request_row_namespace_data(ns, square_width + 1, height, Some(Duration::from_secs(1)))
        .await
        .unwrap_err();
    assert!(matches!(err, NodeError::P2p(P2pError::RequestTimedOut)));

    // check nonexisting namespace row namespace data
    // for namespace that row actually contains
    // PFB (0x04) < 0x05 < Primary ns padding (0x255)
    let unknown_ns = Namespace::const_v0([0, 0, 0, 0, 0, 0, 0, 0, 0, 5]);
    let row = node
        .request_row_namespace_data(unknown_ns, 0, height, Some(Duration::from_secs(1)))
        .await
        .unwrap();
    assert!(row.shares.is_empty());

    // check nonexisting namespace row namespace data
    // for namespace that row doesn't contain
    let unknown_ns = Namespace::TAIL_PADDING;
    let err = node
        .request_row_namespace_data(unknown_ns, 0, height, Some(Duration::from_secs(1)))
        .await
        .unwrap_err();
    assert!(matches!(err, NodeError::P2p(P2pError::RequestTimedOut)));
}

#[tokio::test]
async fn shwap_request_all_blobs() {
    let _guard = test_lock().lock().await;
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

#[tokio::test]
async fn shwap_request_sample_should_cleanup_unneeded_samples() {
    let _guard = test_lock().lock().await;
    // submit some blobs to celestia to get bigger square, so that daser
    // doesn't sample whole block
    let ns = Namespace::const_v0(rand::random());
    let blobs: Vec<_> = (0..5)
        .map(|_| {
            let blob_len = rand::random::<usize>() % 4096 + 1;
            Blob::new(ns, random_bytes(blob_len), None).unwrap()
        })
        .collect();

    // we submit before creating a node because it's quite slow and events can lag
    let client = bridge_client().await;
    let submitted_height = blob_submit(&client, &blobs).await;
    let header = client.header_get_by_height(submitted_height).await.unwrap();

    let (removed_sender, mut removed_receiver) = mpsc::unbounded_channel();
    let builder = test_node_builder()
        // explicitely set pruning window to something big so that
        // pruner doesn't kick in
        .pruning_window(Duration::from_secs(60 * 60 * 24))
        .blockstore(TestBlockstore::new(removed_sender));

    let (node, mut events) = new_connected_node_with_builder(builder).await;

    // wait for node to sample the height we just submitted
    timeout(Duration::from_secs(10), async {
        loop {
            let ev = events.recv().await.unwrap();
            let NodeEvent::SamplingResult { height, .. } = ev.event else {
                continue;
            };

            if height == submitted_height {
                break;
            }
        }
    })
    .await
    .unwrap();

    // get the cids selected by daser
    let cids = node
        .get_sampling_metadata(submitted_height)
        .await
        .unwrap()
        .unwrap()
        .cids;

    // try to request a sample that wasn't selected by daser
    let (row, col, cid) = loop {
        let (row, col, cid) = random_sample(&header);
        if !cids.contains(&cid) {
            break (row, col, cid);
        }
    };

    node.request_sample(row, col, submitted_height, None)
        .await
        .unwrap();

    // it should already be removed from the blockstore
    let removed_cid = removed_receiver.try_recv().unwrap();
    assert_eq!(removed_cid, cid);

    // now try to get a cid that was selected by daser
    let id = SampleId::try_from(cids[0]).unwrap();
    assert_eq!(id.block_height(), submitted_height);

    node.request_sample(id.row_index(), id.column_index(), submitted_height, None)
        .await
        .unwrap();

    // it shouldn't be removed from blockstore within pruning window
    removed_receiver.try_recv().unwrap_err();
}

struct TestBlockstore {
    blockstore: InMemoryBlockstore,
    removed_sender: mpsc::UnboundedSender<Cid>,
}

impl TestBlockstore {
    fn new(removed_sender: mpsc::UnboundedSender<Cid>) -> Self {
        Self {
            blockstore: InMemoryBlockstore::new(),
            removed_sender,
        }
    }
}

impl Blockstore for TestBlockstore {
    async fn get<const S: usize>(
        &self,
        cid: &CidGeneric<S>,
    ) -> blockstore::Result<Option<Vec<u8>>> {
        self.blockstore.get(cid).await
    }

    async fn put_keyed<const S: usize>(
        &self,
        cid: &CidGeneric<S>,
        data: &[u8],
    ) -> blockstore::Result<()> {
        self.blockstore.put_keyed(cid, data).await
    }

    async fn remove<const S: usize>(&self, cid: &CidGeneric<S>) -> blockstore::Result<()> {
        self.blockstore.remove(cid).await?;

        let cid = convert_cid(cid).unwrap();
        self.removed_sender.send(cid).unwrap();

        Ok(())
    }

    async fn close(self) -> blockstore::Result<()> {
        self.blockstore.close().await
    }
}

fn random_sample(header: &ExtendedHeader) -> (u16, u16, Cid) {
    let square = header.square_width();
    let id = SampleId::new(
        rand::random::<u16>() % square,
        rand::random::<u16>() % square,
        header.height(),
    )
    .unwrap();

    let cid = convert_cid(&id.into()).unwrap();

    (id.row_index(), id.column_index(), cid)
}

fn random_bytes(len: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; len];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes
}
