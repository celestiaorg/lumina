//! Discovery of Fibre blobs through on-chain payment transactions.

use std::collections::HashSet;
use std::fmt;
use std::num::NonZeroU64;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use async_stream::stream;
use celestia_grpc::GrpcClient;
use celestia_proto::celestia::fibre::v1::MsgPayForFibre;
use celestia_proto::cosmos::tx::v1beta1::{GetTxsEventRequest, GetTxsEventResponse, OrderBy, Tx};
use celestia_types::hash::Hash;
use celestia_types::nmt::Namespace;
use futures::Stream;
use prost::{Message, Name};
use tokio_util::sync::CancellationToken;

use crate::blob::BlobID;
use crate::client::FibreClient;
use crate::error::{DiscoveryError, FibreError};

const DEFAULT_PAGE_SIZE: u64 = 100;
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);
const MAX_BATCH_QUERY_ATTEMPTS: usize = 4;

/// A Fibre blob found through an on-chain `MsgPayForFibre` transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveredBlob {
    /// Identifier accepted by [`FibreClient::download`](crate::FibreClient::download).
    pub id: BlobID,
    /// Height of the validator set that signed the payment promise.
    pub validator_set_height: u64,
    /// Block height containing the payment transaction.
    pub tx_height: u64,
    /// Hash of the payment transaction.
    pub tx_hash: Hash,
}

/// Options controlling continuous Fibre discovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiscoveryOptions {
    /// Delay between completed transaction queries.
    pub poll_interval: Duration,
    /// Maximum number of transactions requested per page.
    pub page_size: NonZeroU64,
}

impl Default for DiscoveryOptions {
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_POLL_INTERVAL,
            page_size: NonZeroU64::new(DEFAULT_PAGE_SIZE).expect("default page size is non-zero"),
        }
    }
}

/// Stream of Fibre blobs discovered through on-chain payments.
#[must_use = "streams do nothing unless polled"]
pub struct FibreStream {
    inner: Pin<Box<dyn Stream<Item = Result<DiscoveredBlob, FibreError>> + Send + 'static>>,
}

impl fmt::Debug for FibreStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FibreStream").finish_non_exhaustive()
    }
}

impl Stream for FibreStream {
    type Item = Result<DiscoveredBlob, FibreError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}

#[async_trait::async_trait]
pub(crate) trait TransactionEventSource: Send + Sync {
    async fn get_txs_event(
        &self,
        request: GetTxsEventRequest,
    ) -> Result<GetTxsEventResponse, celestia_grpc::Error>;
}

#[async_trait::async_trait]
impl TransactionEventSource for GrpcClient {
    async fn get_txs_event(
        &self,
        request: GetTxsEventRequest,
    ) -> Result<GetTxsEventResponse, celestia_grpc::Error> {
        GrpcClient::get_txs_event(self, request).await
    }
}

struct DiscoveryBatch {
    blobs: Vec<DiscoveredBlob>,
    through_height: u64,
}

enum BatchScan {
    Complete(DiscoveryBatch),
    IndexChanged,
}

impl FibreClient {
    /// Query all currently indexed Fibre payments for `namespace` after `from_height`.
    ///
    /// `from_height` is an exclusive transaction-height cursor. Results are ordered by
    /// transaction height and message order. After processing the complete result, callers can
    /// persist the greatest [`DiscoveredBlob::tx_height`] as their next cursor.
    pub async fn query_discovered(
        &self,
        namespace: Namespace,
        from_height: u64,
    ) -> Result<Vec<DiscoveredBlob>, FibreError> {
        if self.cancel_token.is_cancelled() {
            return Err(FibreError::ClientClosed);
        }

        let source = self
            .discovery_source
            .as_deref()
            .ok_or(FibreError::DiscoveryUnavailable)?;
        let batch = query_batch(
            source,
            namespace,
            from_height,
            NonZeroU64::new(DEFAULT_PAGE_SIZE).expect("default page size is non-zero"),
            &self.cancel_token,
        )
        .await?;
        Ok(batch.blobs)
    }

    /// Continuously discover Fibre payments for `namespace` after `from_height`.
    ///
    /// The first query runs when the stream is first polled. Transient network errors are
    /// yielded and retried; malformed responses and non-transient errors end the stream. A
    /// discovered blob's validator-set height can be passed to [`DownloadOptions::height`](crate::DownloadOptions::height).
    pub fn discover(
        &self,
        namespace: Namespace,
        from_height: u64,
    ) -> Result<FibreStream, FibreError> {
        self.discover_with_options(namespace, from_height, DiscoveryOptions::default())
    }

    /// Continuously discover Fibre payments using explicit polling options.
    pub fn discover_with_options(
        &self,
        namespace: Namespace,
        from_height: u64,
        options: DiscoveryOptions,
    ) -> Result<FibreStream, FibreError> {
        if self.cancel_token.is_cancelled() {
            return Err(FibreError::ClientClosed);
        }
        if options.poll_interval.is_zero() {
            return Err(DiscoveryError::ZeroPollInterval.into());
        }
        validate_from_height(from_height)?;

        let source = Arc::clone(
            self.discovery_source
                .as_ref()
                .ok_or(FibreError::DiscoveryUnavailable)?,
        );
        let cancel_token = self.cancel_token.clone();

        let inner = stream! {
            let mut cursor = from_height;
            let mut seen_at_cursor = HashSet::new();

            loop {
                // Re-query the latest observed height so that a payment indexed slightly later
                // than another transaction in the same block cannot be skipped.
                let query_from_height = cursor.saturating_sub(1).max(from_height);
                let result = query_batch(
                    source.as_ref(),
                    namespace,
                    query_from_height,
                    options.page_size,
                    &cancel_token,
                )
                .await;

                match result {
                    Ok(batch) => {
                        let next_cursor = cursor.max(batch.through_height);
                        let mut next_seen = HashSet::new();

                        for blob in batch.blobs {
                            let key = (blob.tx_hash, blob.id.clone());
                            let already_seen = blob.tx_height == cursor
                                && seen_at_cursor.contains(&key);
                            if blob.tx_height == next_cursor {
                                next_seen.insert(key);
                            }
                            if !already_seen {
                                if cancel_token.is_cancelled() {
                                    yield Err(FibreError::Cancelled);
                                    return;
                                }
                                yield Ok(blob);
                            }
                        }

                        cursor = next_cursor;
                        seen_at_cursor = next_seen;
                    }
                    Err(FibreError::GrpcClient(error)) if error.is_network_error() => {
                        yield Err(FibreError::GrpcClient(error));
                    }
                    Err(FibreError::Cancelled) => {
                        yield Err(FibreError::Cancelled);
                        return;
                    }
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                }

                let cancelled = tokio::select! {
                    biased;
                    _ = cancel_token.cancelled() => true,
                    _ = lumina_utils::time::sleep(options.poll_interval) => false,
                };
                if cancelled {
                    yield Err(FibreError::Cancelled);
                    return;
                }
            }
        };

        Ok(FibreStream {
            inner: Box::pin(inner),
        })
    }
}

async fn query_batch(
    source: &dyn TransactionEventSource,
    namespace: Namespace,
    from_height: u64,
    page_size: NonZeroU64,
    cancel_token: &CancellationToken,
) -> Result<DiscoveryBatch, FibreError> {
    validate_from_height(from_height)?;

    for _ in 0..MAX_BATCH_QUERY_ATTEMPTS {
        match scan_batch(source, namespace, from_height, page_size, cancel_token).await? {
            BatchScan::Complete(batch) => return Ok(batch),
            BatchScan::IndexChanged => continue,
        }
    }

    Err(DiscoveryError::UnstablePagination.into())
}

async fn scan_batch(
    source: &dyn TransactionEventSource,
    namespace: Namespace,
    from_height: u64,
    page_size: NonZeroU64,
    cancel_token: &CancellationToken,
) -> Result<BatchScan, FibreError> {
    let message_type_url = MsgPayForFibre::type_url();
    let query = format!("message.action='{message_type_url}' AND tx.height > {from_height}");
    let mut blobs = Vec::new();
    let mut through_height = from_height;
    let mut fetched = 0u64;
    let mut page = 1u64;
    let mut previous_total = 0;
    let mut seen_transactions = HashSet::new();

    loop {
        let request = GetTxsEventRequest {
            order_by: OrderBy::Asc.into(),
            page,
            limit: page_size.get(),
            query: query.clone(),
            ..Default::default()
        };

        let response = tokio::select! {
            biased;
            _ = cancel_token.cancelled() => return Err(FibreError::Cancelled),
            result = source.get_txs_event(request) => result?,
        };

        if response.txs.len() != response.tx_responses.len() {
            return Err(DiscoveryError::ResponseLengthMismatch {
                transactions: response.txs.len(),
                responses: response.tx_responses.len(),
            }
            .into());
        }

        // Page-number queries are not snapshots, so a changing result set invalidates this scan.
        if response.total < previous_total {
            return Ok(BatchScan::IndexChanged);
        }

        let page_len = u64::try_from(response.txs.len()).unwrap_or(u64::MAX);
        if page_len == 0 && fetched < response.total {
            return Err(DiscoveryError::IncompletePage {
                page,
                total: response.total,
            }
            .into());
        }

        previous_total = response.total;

        for (tx, tx_response) in response.txs.into_iter().zip(response.tx_responses) {
            let tx_hash = parse_tx_hash(&tx_response.txhash)?;
            if !seen_transactions.insert(tx_hash) {
                return Ok(BatchScan::IndexChanged);
            }

            let tx_height = parse_tx_height(tx_response.height, from_height)?;
            through_height = through_height.max(tx_height);

            if tx_response.code != 0 {
                continue;
            }

            decode_transaction(
                tx,
                namespace,
                tx_height,
                tx_hash,
                &tx_response.txhash,
                &message_type_url,
                &mut blobs,
            )?;
        }

        fetched = fetched.saturating_add(page_len);
        if fetched >= previous_total {
            return Ok(BatchScan::Complete(DiscoveryBatch {
                blobs,
                through_height,
            }));
        }
        page = page.checked_add(1).ok_or(DiscoveryError::PageOverflow)?;
    }
}

fn validate_from_height(from_height: u64) -> Result<(), DiscoveryError> {
    if from_height > i64::MAX as u64 {
        return Err(DiscoveryError::HeightTooLarge(from_height));
    }
    Ok(())
}

fn parse_tx_height(height: i64, from_height: u64) -> Result<u64, DiscoveryError> {
    let height = u64::try_from(height)
        .ok()
        .filter(|height| *height > 0)
        .ok_or(DiscoveryError::InvalidTransactionHeight(height))?;
    if height <= from_height {
        return Err(DiscoveryError::UnexpectedTransactionHeight {
            height,
            from_height,
        });
    }
    Ok(height)
}

fn parse_tx_hash(hash: &str) -> Result<Hash, DiscoveryError> {
    let parsed = hash
        .parse()
        .map_err(|_| DiscoveryError::InvalidTransactionHash(hash.to_owned()))?;
    if parsed == Hash::None {
        return Err(DiscoveryError::InvalidTransactionHash(hash.to_owned()));
    }
    Ok(parsed)
}

fn decode_transaction(
    tx: Tx,
    namespace: Namespace,
    tx_height: u64,
    tx_hash: Hash,
    tx_hash_text: &str,
    message_type_url: &str,
    blobs: &mut Vec<DiscoveredBlob>,
) -> Result<(), FibreError> {
    let body = tx
        .body
        .ok_or_else(|| DiscoveryError::MissingTransactionBody(tx_hash_text.to_owned()))?;

    for message in body.messages {
        if message.type_url != message_type_url {
            continue;
        }

        let message = MsgPayForFibre::decode(message.value.as_slice())
            .map_err(DiscoveryError::DecodeMessage)?;
        let promise = message
            .payment_promise
            .ok_or(DiscoveryError::MissingPaymentPromise)?;
        if promise.namespace != namespace.as_bytes() {
            continue;
        }

        let version = u8::try_from(promise.blob_version)
            .map_err(|_| DiscoveryError::BlobVersionOutOfRange(promise.blob_version))?;
        let commitment_len = promise.commitment.len();
        let commitment = promise
            .commitment
            .try_into()
            .map_err(|_| DiscoveryError::CommitmentLength(commitment_len))?;
        let id = BlobID::new(version, commitment);
        id.validate()?;

        let validator_set_height = u64::try_from(promise.height)
            .ok()
            .filter(|height| *height > 0)
            .ok_or(DiscoveryError::InvalidValidatorSetHeight(promise.height))?;

        blobs.push(DiscoveredBlob {
            id,
            validator_set_height,
            tx_height,
            tx_hash,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use celestia_proto::celestia::fibre::v1::PaymentPromise;
    use celestia_proto::cosmos::base::abci::v1beta1::TxResponse;
    use celestia_proto::cosmos::tx::v1beta1::TxBody;
    use celestia_types::any::IntoProtobufAny;
    use futures::StreamExt;

    use super::*;
    use crate::config::FibreClientConfig;
    use crate::test_utils::{MockConnector, MockSetGetter, make_validator};
    use crate::validator::ValidatorSet;

    type SourceResult = Result<GetTxsEventResponse, celestia_grpc::Error>;

    #[derive(Clone, Default)]
    struct MockSource {
        responses: Arc<Mutex<VecDeque<SourceResult>>>,
        requests: Arc<Mutex<Vec<GetTxsEventRequest>>>,
    }

    impl MockSource {
        fn new(responses: impl IntoIterator<Item = SourceResult>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
                requests: Arc::default(),
            }
        }

        fn requests(&self) -> Vec<GetTxsEventRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl TransactionEventSource for MockSource {
        async fn get_txs_event(
            &self,
            request: GetTxsEventRequest,
        ) -> Result<GetTxsEventResponse, celestia_grpc::Error> {
            self.requests.lock().unwrap().push(request);
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Ok(GetTxsEventResponse::default()))
        }
    }

    fn test_client(source: MockSource) -> FibreClient {
        let (_, validator) = make_validator(1, 1);
        let val_set = ValidatorSet::try_new(vec![validator], 1).unwrap();

        FibreClient::builder()
            .config(FibreClientConfig::new("test-chain").unwrap())
            .set_getter(MockSetGetter { val_set })
            .connector(MockConnector::new())
            .discovery_source(source)
            .build()
            .unwrap()
    }

    fn payment_message(
        namespace: Namespace,
        commitment: Vec<u8>,
        blob_version: u32,
        validator_set_height: i64,
    ) -> tendermint_proto::google::protobuf::Any {
        MsgPayForFibre {
            payment_promise: Some(PaymentPromise {
                namespace: namespace.as_bytes().to_vec(),
                commitment,
                blob_version,
                height: validator_set_height,
                ..Default::default()
            }),
            ..Default::default()
        }
        .into_any()
    }

    fn transaction(
        messages: Vec<tendermint_proto::google::protobuf::Any>,
        height: i64,
        hash_byte: u8,
        code: u32,
    ) -> (Tx, TxResponse) {
        (
            Tx {
                body: Some(TxBody {
                    messages,
                    ..Default::default()
                }),
                ..Default::default()
            },
            TxResponse {
                height,
                txhash: hex::encode_upper([hash_byte; 32]),
                code,
                ..Default::default()
            },
        )
    }

    fn response(
        entries: impl IntoIterator<Item = (Tx, TxResponse)>,
        total: u64,
    ) -> GetTxsEventResponse {
        let (txs, tx_responses) = entries.into_iter().unzip();
        GetTxsEventResponse {
            txs,
            tx_responses,
            total,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn query_fetches_all_pages_and_filters_namespace_and_failed_txs() {
        let namespace = Namespace::new_v0(b"fibre-eval").unwrap();
        let other_namespace = Namespace::new_v0(b"other-data").unwrap();
        let first = transaction(
            vec![payment_message(namespace, vec![1; 32], 0, 21)],
            11,
            1,
            0,
        );
        let other = transaction(
            vec![payment_message(other_namespace, vec![2; 32], 0, 22)],
            12,
            2,
            0,
        );
        let failed = transaction(
            vec![payment_message(namespace, vec![3; 32], 0, 23)],
            13,
            3,
            1,
        );
        let source = MockSource::new([Ok(response([first], 3)), Ok(response([other, failed], 3))]);
        let client = test_client(source.clone());

        let blobs = client.query_discovered(namespace, 10).await.unwrap();

        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].id, BlobID::new(0, [1; 32]));
        assert_eq!(blobs[0].validator_set_height, 21);
        assert_eq!(blobs[0].tx_height, 11);
        assert_eq!(
            blobs[0].tx_hash,
            hex::encode_upper([1; 32]).parse().unwrap()
        );

        let requests = source.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].page, 1);
        assert_eq!(requests[1].page, 2);
        assert_eq!(requests[0].limit, DEFAULT_PAGE_SIZE);
        assert_eq!(requests[0].order_by, OrderBy::Asc as i32);
        assert_eq!(
            requests[0].query,
            format!(
                "message.action='{}' AND tx.height > 10",
                MsgPayForFibre::type_url()
            )
        );
    }

    #[tokio::test]
    async fn query_restarts_after_page_boundaries_shift() {
        let namespace = Namespace::new_v0(b"fibre-eval").unwrap();
        let first = transaction(
            vec![payment_message(namespace, vec![1; 32], 0, 11)],
            11,
            1,
            0,
        );
        let second = transaction(
            vec![payment_message(namespace, vec![2; 32], 0, 12)],
            12,
            2,
            0,
        );
        let late = transaction(
            vec![payment_message(namespace, vec![3; 32], 0, 10)],
            10,
            3,
            0,
        );
        let source = MockSource::new([
            Ok(response([first.clone()], 2)),
            Ok(response([first.clone()], 3)),
            Ok(response([late, first, second], 3)),
        ]);
        let client = test_client(source.clone());

        let blobs = client.query_discovered(namespace, 9).await.unwrap();

        let ids: Vec<_> = blobs.into_iter().map(|blob| blob.id).collect();
        assert_eq!(
            ids,
            [
                BlobID::new(0, [3; 32]),
                BlobID::new(0, [1; 32]),
                BlobID::new(0, [2; 32]),
            ]
        );
        let pages: Vec<_> = source
            .requests()
            .into_iter()
            .map(|request| request.page)
            .collect();
        assert_eq!(pages, [1, 2, 1]);
    }

    #[tokio::test]
    async fn query_rejects_an_index_that_never_stabilizes() {
        let namespace = Namespace::new_v0(b"fibre-eval").unwrap();
        let entry = transaction(
            vec![payment_message(namespace, vec![1; 32], 0, 11)],
            11,
            1,
            0,
        );
        let responses = (0..MAX_BATCH_QUERY_ATTEMPTS).flat_map(|_| {
            [
                Ok(response([entry.clone()], 2)),
                Ok(response([entry.clone()], 2)),
            ]
        });
        let client = test_client(MockSource::new(responses));

        assert!(matches!(
            client.query_discovered(namespace, 10).await,
            Err(FibreError::Discovery(DiscoveryError::UnstablePagination))
        ));
    }

    #[tokio::test]
    async fn query_preserves_message_order_and_transaction_metadata() {
        let namespace = Namespace::new_v0(b"fibre-eval").unwrap();
        let entry = transaction(
            vec![
                tendermint_proto::google::protobuf::Any {
                    type_url: "/example.Unrelated".into(),
                    value: vec![],
                },
                payment_message(namespace, vec![4; 32], 0, 31),
                payment_message(namespace, vec![5; 32], 0, 32),
            ],
            30,
            7,
            0,
        );
        let client = test_client(MockSource::new([Ok(response([entry], 1))]));

        let blobs = client.query_discovered(namespace, 29).await.unwrap();

        assert_eq!(blobs.len(), 2);
        assert_eq!(blobs[0].id, BlobID::new(0, [4; 32]));
        assert_eq!(blobs[1].id, BlobID::new(0, [5; 32]));
        assert_eq!(blobs[0].tx_hash, blobs[1].tx_hash);
        assert_eq!(blobs[0].tx_height, 30);
        assert_eq!(blobs[1].tx_height, 30);
    }

    #[tokio::test]
    async fn query_rejects_malformed_responses() {
        let namespace = Namespace::new_v0(b"fibre-eval").unwrap();
        let mismatch = GetTxsEventResponse {
            txs: vec![Tx::default()],
            total: 1,
            ..Default::default()
        };
        let client = test_client(MockSource::new([Ok(mismatch)]));

        assert!(matches!(
            client.query_discovered(namespace, 0).await,
            Err(FibreError::Discovery(
                DiscoveryError::ResponseLengthMismatch { .. }
            ))
        ));

        let invalid_commitment = transaction(
            vec![payment_message(namespace, vec![8; 31], 0, 40)],
            40,
            8,
            0,
        );
        let client = test_client(MockSource::new([Ok(response([invalid_commitment], 1))]));

        assert!(matches!(
            client.query_discovered(namespace, 39).await,
            Err(FibreError::Discovery(DiscoveryError::CommitmentLength(31)))
        ));
    }

    #[tokio::test]
    async fn query_rejects_invalid_fibre_records_and_cursors() {
        let namespace = Namespace::new_v0(b"fibre-eval").unwrap();
        let missing_promise = transaction(vec![MsgPayForFibre::default().into_any()], 20, 1, 0);
        let client = test_client(MockSource::new([Ok(response([missing_promise], 1))]));
        assert!(matches!(
            client.query_discovered(namespace, 19).await,
            Err(FibreError::Discovery(DiscoveryError::MissingPaymentPromise))
        ));

        let unsupported_version = transaction(
            vec![payment_message(namespace, vec![1; 32], 1, 20)],
            20,
            2,
            0,
        );
        let client = test_client(MockSource::new([Ok(response([unsupported_version], 1))]));
        assert!(matches!(
            client.query_discovered(namespace, 19).await,
            Err(FibreError::UnsupportedBlobVersion(1))
        ));

        let zero_validator_height = transaction(
            vec![payment_message(namespace, vec![1; 32], 0, 0)],
            20,
            3,
            0,
        );
        let client = test_client(MockSource::new([Ok(response([zero_validator_height], 1))]));
        assert!(matches!(
            client.query_discovered(namespace, 19).await,
            Err(FibreError::Discovery(
                DiscoveryError::InvalidValidatorSetHeight(0)
            ))
        ));

        let invalid_hash = (
            Tx {
                body: Some(TxBody {
                    messages: vec![payment_message(namespace, vec![1; 32], 0, 20)],
                    ..Default::default()
                }),
                ..Default::default()
            },
            TxResponse {
                height: 20,
                txhash: "not-a-hash".into(),
                ..Default::default()
            },
        );
        let client = test_client(MockSource::new([Ok(response([invalid_hash], 1))]));
        assert!(matches!(
            client.query_discovered(namespace, 19).await,
            Err(FibreError::Discovery(
                DiscoveryError::InvalidTransactionHash(_)
            ))
        ));

        let client = test_client(MockSource::default());
        assert!(matches!(
            client
                .query_discovered(namespace, i64::MAX as u64 + 1)
                .await,
            Err(FibreError::Discovery(DiscoveryError::HeightTooLarge(_)))
        ));
    }

    #[tokio::test]
    async fn stream_advances_cursor_without_repeating_transactions() {
        let namespace = Namespace::new_v0(b"fibre-eval").unwrap();
        let first = transaction(
            vec![payment_message(namespace, vec![1; 32], 0, 11)],
            11,
            1,
            0,
        );
        let first_again = transaction(
            vec![payment_message(namespace, vec![1; 32], 0, 11)],
            11,
            1,
            0,
        );
        let late_same_height = transaction(
            vec![payment_message(namespace, vec![2; 32], 0, 12)],
            11,
            2,
            0,
        );
        let source = MockSource::new([
            Ok(response([first], 1)),
            Ok(response([first_again, late_same_height], 2)),
        ]);
        let client = test_client(source.clone());
        let mut stream = client
            .discover_with_options(
                namespace,
                10,
                DiscoveryOptions {
                    poll_interval: Duration::from_millis(1),
                    ..Default::default()
                },
            )
            .unwrap();

        assert_eq!(stream.next().await.unwrap().unwrap().tx_height, 11);
        let late = stream.next().await.unwrap().unwrap();
        assert_eq!(late.id, BlobID::new(0, [2; 32]));
        assert_eq!(late.tx_height, 11);

        let requests = source.requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].query.ends_with("tx.height > 10"));
        assert!(requests[1].query.ends_with("tx.height > 10"));
    }

    #[tokio::test]
    async fn stream_retries_transient_network_errors() {
        let namespace = Namespace::new_v0(b"fibre-eval").unwrap();
        let entry = transaction(
            vec![payment_message(namespace, vec![9; 32], 0, 15)],
            15,
            9,
            0,
        );
        let source = MockSource::new([
            Err(tonic::Status::unavailable("temporarily unavailable").into()),
            Ok(response([entry], 1)),
        ]);
        let client = test_client(source);
        let mut stream = client
            .discover_with_options(
                namespace,
                10,
                DiscoveryOptions {
                    poll_interval: Duration::from_millis(1),
                    ..Default::default()
                },
            )
            .unwrap();

        assert!(matches!(
            stream.next().await,
            Some(Err(FibreError::GrpcClient(_)))
        ));
        assert_eq!(stream.next().await.unwrap().unwrap().tx_height, 15);
    }

    #[tokio::test]
    async fn stream_ends_after_structural_error() {
        let namespace = Namespace::new_v0(b"fibre-eval").unwrap();
        let source = MockSource::new([Ok(GetTxsEventResponse {
            txs: vec![Tx::default()],
            total: 1,
            ..Default::default()
        })]);
        let client = test_client(source);
        let mut stream = client.discover(namespace, 0).unwrap();

        assert!(matches!(
            stream.next().await,
            Some(Err(FibreError::Discovery(_)))
        ));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn discovery_requires_a_source_and_observes_client_close() {
        let (_, validator) = make_validator(1, 1);
        let val_set = ValidatorSet::try_new(vec![validator], 1).unwrap();
        let client = FibreClient::builder()
            .config(FibreClientConfig::new("test-chain").unwrap())
            .set_getter(MockSetGetter { val_set })
            .connector(MockConnector::new())
            .build()
            .unwrap();
        let namespace = Namespace::new_v0(b"fibre-eval").unwrap();

        assert!(matches!(
            client.discover(namespace, 0),
            Err(FibreError::DiscoveryUnavailable)
        ));

        let client = test_client(MockSource::default());
        let mut stream = client.discover(namespace, 0).unwrap();
        client.close();

        assert!(matches!(
            stream.next().await,
            Some(Err(FibreError::Cancelled))
        ));
        assert!(stream.next().await.is_none());
        assert!(matches!(
            client.query_discovered(namespace, 0).await,
            Err(FibreError::ClientClosed)
        ));
    }

    #[test]
    fn discovery_options_reject_zero_poll_interval() {
        let namespace = Namespace::new_v0(b"fibre-eval").unwrap();
        let client = test_client(MockSource::default());

        assert!(matches!(
            client.discover_with_options(
                namespace,
                0,
                DiscoveryOptions {
                    poll_interval: Duration::ZERO,
                    ..Default::default()
                }
            ),
            Err(FibreError::Discovery(DiscoveryError::ZeroPollInterval))
        ));
    }
}
