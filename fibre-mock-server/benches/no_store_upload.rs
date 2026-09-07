use std::convert::Infallible;
use std::time::{Duration, SystemTime};

use bytes::{Bytes, BytesMut};
use celestia_fibre::PaymentPromise;
use celestia_proto::celestia::fibre::v1::fibre_server::FibreServer;
use celestia_proto::celestia::fibre::v1::{
    BlobRow, BlobShard, UploadShardRequest, UploadShardResponse,
};
use celestia_types::nmt::Namespace;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use fibre_mock_server::BenchmarkNoStoreFibreServer;
use fibre_mock_server::fibre_service::MockFibreService;
use futures::future::join_all;
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use prost::Message;
use tonic::body::Body;
use tonic::codegen::{Service, http};

const ROW_BYTES: usize = 64 * 1024;
const ROW_COUNT: usize = 39 * 1024 * 1024 / ROW_BYTES;
const CHUNK_BYTES: usize = 64 * 1024;
const CONCURRENCY: usize = 4;
const MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

fn encoded_request() -> Bytes {
    let signer = k256::ecdsa::SigningKey::from_slice(&[7; 32]).unwrap();
    let mut promise = PaymentPromise {
        chain_id: "mock-1".into(),
        height: 1,
        namespace: Namespace::new_v0(b"benchmark").unwrap(),
        upload_size: (ROW_BYTES * ROW_COUNT) as u32,
        blob_version: 0,
        commitment: [9; 32],
        creation_timestamp: SystemTime::now(),
        signer_pubkey: *signer.verifying_key(),
        signature: None,
    };
    promise.sign(&signer).unwrap();

    let row = Bytes::from(vec![42; ROW_BYTES]);
    let rows = (0..ROW_COUNT)
        .map(|index| BlobRow {
            index: index as u32,
            data: row.clone(),
            proof: Vec::new(),
        })
        .collect();
    let request = UploadShardRequest {
        promise: Some((&promise).into()),
        shard: Some(BlobShard {
            rows,
            rlcs: Bytes::new(),
        }),
    };

    let message_length = request.encoded_len();
    let mut encoded = Vec::with_capacity(5 + message_length);
    encoded.push(0);
    encoded.extend_from_slice(&(message_length as u32).to_be_bytes());
    request.encode(&mut encoded).unwrap();
    Bytes::from(encoded)
}

fn chunked_body(encoded: Bytes) -> Body {
    let stream = futures::stream::unfold(encoded, |mut remaining| async move {
        if remaining.is_empty() {
            None
        } else {
            let length = CHUNK_BYTES.min(remaining.len());
            let chunk = remaining.split_to(length);
            Some((Ok::<_, Infallible>(Frame::data(chunk)), remaining))
        }
    });
    Body::new(StreamBody::new(stream))
}

fn request(encoded: Bytes) -> http::Request<Body> {
    http::Request::builder()
        .uri("/celestia.fibre.v1.Fibre/UploadShard")
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .body(chunked_body(encoded))
        .unwrap()
}

async fn call_and_verify<S>(service: &mut S, encoded: Bytes)
where
    S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>,
    S::Future: Send,
{
    let response = service.call(request(encoded)).await.unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    assert!(response.headers().get("grpc-status").is_none());

    let mut body = response.into_body();
    let mut message = BytesMut::new();
    let mut grpc_status = None;
    while let Some(frame) = body.frame().await {
        let frame = frame.unwrap();
        if frame.is_data() {
            message.extend_from_slice(&frame.into_data().unwrap());
        } else if frame.is_trailers() {
            grpc_status = frame.into_trailers().unwrap().get("grpc-status").cloned();
        }
    }

    assert_eq!(grpc_status.unwrap(), "0");
    assert_eq!(message[0], 0);
    let length = u32::from_be_bytes(message[1..5].try_into().unwrap()) as usize;
    assert_eq!(length, message.len() - 5);
    let response = UploadShardResponse::decode(&message[5..]).unwrap();
    assert_eq!(response.validator_signature.len(), 64);
}

fn benchmark(c: &mut Criterion) {
    let encoded = encoded_request();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let generated = FibreServer::new(MockFibreService::new(
        ed25519_dalek::SigningKey::from_bytes(&[3; 32]),
        None,
    ))
    .max_decoding_message_size(MAX_MESSAGE_SIZE)
    .max_encoding_message_size(MAX_MESSAGE_SIZE);
    let streaming = BenchmarkNoStoreFibreServer::new(MockFibreService::new(
        ed25519_dalek::SigningKey::from_bytes(&[3; 32]),
        None,
    ))
    .max_decoding_message_size(MAX_MESSAGE_SIZE)
    .max_encoding_message_size(MAX_MESSAGE_SIZE);

    let mut single = c.benchmark_group("no_store_upload_single");
    single.sample_size(10);
    single.warm_up_time(Duration::from_secs(1));
    single.measurement_time(Duration::from_secs(3));
    single.throughput(Throughput::Bytes(encoded.len() as u64));
    let mut service = generated.clone();
    single.bench_function("generated", |b| {
        b.iter(|| runtime.block_on(call_and_verify(&mut service, encoded.clone())));
    });
    let mut service = streaming.clone();
    single.bench_function("streaming", |b| {
        b.iter(|| runtime.block_on(call_and_verify(&mut service, encoded.clone())));
    });
    single.finish();

    let mut concurrent = c.benchmark_group("no_store_upload_concurrent_4");
    concurrent.sample_size(10);
    concurrent.warm_up_time(Duration::from_secs(1));
    concurrent.measurement_time(Duration::from_secs(3));
    concurrent.throughput(Throughput::Bytes((encoded.len() * CONCURRENCY) as u64));
    concurrent.bench_function("generated", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let requests = (0..CONCURRENCY).map(|_| {
                    let mut service = generated.clone();
                    let encoded = encoded.clone();
                    async move { call_and_verify(&mut service, encoded).await }
                });
                join_all(requests).await;
            })
        });
    });
    concurrent.bench_function("streaming", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let requests = (0..CONCURRENCY).map(|_| {
                    let mut service = streaming.clone();
                    let encoded = encoded.clone();
                    async move { call_and_verify(&mut service, encoded).await }
                });
                join_all(requests).await;
            })
        });
    });
    concurrent.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
