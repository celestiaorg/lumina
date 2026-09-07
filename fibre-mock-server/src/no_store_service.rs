use std::convert::Infallible;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use bytes::{Buf, Bytes};
use celestia_proto::celestia::fibre::v1::fibre_server::FibreServer;
use celestia_proto::celestia::fibre::v1::{PaymentPromise, UploadShardResponse};
use futures::future::BoxFuture;
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use prost::Message;
use tonic::Status;
use tonic::body::Body;
use tonic::codegen::{Service, http};
use tonic::server::NamedService;

use crate::fibre_service::MockFibreService;

const UPLOAD_SHARD_PATH: &str = "/celestia.fibre.v1.Fibre/UploadShard";
const DEFAULT_MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;
const MAX_PROMISE_SIZE: usize = 4 * 1024;

#[derive(Clone)]
pub struct NoStoreFibreServer {
    inner: Arc<MockFibreService>,
    fallback: FibreServer<MockFibreService>,
    max_decoding_message_size: usize,
}

impl NoStoreFibreServer {
    pub fn new(inner: MockFibreService) -> Self {
        let inner = Arc::new(inner);
        Self {
            fallback: FibreServer::from_arc(inner.clone()),
            inner,
            max_decoding_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }

    #[must_use]
    pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
        self.fallback = self.fallback.max_decoding_message_size(limit);
        self.max_decoding_message_size = limit;
        self
    }

    #[must_use]
    pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
        self.fallback = self.fallback.max_encoding_message_size(limit);
        self
    }
}

impl NamedService for NoStoreFibreServer {
    const NAME: &'static str = "celestia.fibre.v1.Fibre";
}

impl Service<http::Request<Body>> for NoStoreFibreServer {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        if request.uri().path() != UPLOAD_SHARD_PATH {
            return self.fallback.call(request);
        }

        let inner = self.inner.clone();
        let max_message_size = self.max_decoding_message_size;
        Box::pin(async move {
            let response = match upload(inner, request.into_body(), max_message_size).await {
                Ok(response) => success_response(response),
                Err(status) => status.into_http::<Body>(),
            };
            Ok(response)
        })
    }
}

async fn upload(
    service: Arc<MockFibreService>,
    body: Body,
    max_message_size: usize,
) -> Result<UploadShardResponse, Status> {
    let started = Instant::now();
    let mut reader = FrameReader::new(body);
    let compressed = reader.read_byte().await?;
    if compressed != 0 {
        return Err(Status::unimplemented(
            "compressed upload requests are not supported",
        ));
    }

    let mut length = [0; 4];
    for byte in &mut length {
        *byte = reader.read_byte().await?;
    }
    let declared_message_bytes = u32::from_be_bytes(length) as usize;
    if declared_message_bytes > max_message_size {
        return Err(Status::out_of_range(format!(
            "decoded message length {declared_message_bytes} exceeds limit {max_message_size}",
        )));
    }

    let promise_bytes = parse_request(&mut reader, declared_message_bytes).await?;
    reader.ensure_end().await?;
    let body_drain_time = started.elapsed();

    let sign_started = Instant::now();
    let promise = PaymentPromise::decode(promise_bytes.as_slice())
        .map_err(|error| Status::internal(error.to_string()))?;
    let response = service.sign_promise(promise)?;
    let promise_decode_sign_time = sign_started.elapsed();

    tracing::trace!(
        declared_message_bytes,
        body_drain_micros = body_drain_time.as_micros(),
        promise_decode_sign_micros = promise_decode_sign_time.as_micros(),
        total_request_micros = started.elapsed().as_micros(),
        "processed streaming no-store upload"
    );

    Ok(response)
}

async fn parse_request(reader: &mut FrameReader, mut remaining: usize) -> Result<Vec<u8>, Status> {
    let mut promise = None;
    let mut shard_present = false;

    while remaining > 0 {
        let key = read_varint(reader, &mut remaining).await?;
        if key > u64::from(u32::MAX) {
            return Err(Status::internal("invalid protobuf field key"));
        }
        let field = key >> 3;
        let wire_type = key & 7;
        if field == 0 {
            return Err(Status::internal("protobuf field number must be non-zero"));
        }

        match (field, wire_type) {
            (1, 2) => {
                if promise.is_some() {
                    return Err(Status::invalid_argument("duplicate promise"));
                }
                let length = read_length(reader, &mut remaining).await?;
                if length > MAX_PROMISE_SIZE {
                    return Err(Status::out_of_range(format!(
                        "promise length {length} exceeds limit {MAX_PROMISE_SIZE}",
                    )));
                }
                promise = Some(reader.copy_exact(length).await?);
                remaining -= length;
            }
            (2, 2) => {
                if shard_present {
                    return Err(Status::invalid_argument("duplicate shard"));
                }
                let length = read_length(reader, &mut remaining).await?;
                reader.skip_exact(length).await?;
                remaining -= length;
                shard_present = true;
            }
            (1 | 2, _) => {
                return Err(Status::internal(format!(
                    "protobuf field {field} has invalid wire type {wire_type}",
                )));
            }
            (_, 0) => {
                read_varint(reader, &mut remaining).await?;
            }
            (_, 1) => skip_field(reader, &mut remaining, 8).await?,
            (_, 2) => {
                let length = read_length(reader, &mut remaining).await?;
                reader.skip_exact(length).await?;
                remaining -= length;
            }
            (_, 5) => skip_field(reader, &mut remaining, 4).await?,
            (_, 3 | 4) => return Err(Status::internal("protobuf groups are not supported")),
            (_, _) => return Err(Status::internal("invalid protobuf wire type")),
        }
    }

    let promise = promise.ok_or_else(|| Status::invalid_argument("missing promise"))?;
    if !shard_present {
        return Err(Status::invalid_argument("missing shard"));
    }
    Ok(promise)
}

async fn read_varint(reader: &mut FrameReader, remaining: &mut usize) -> Result<u64, Status> {
    let mut value = 0u64;
    for index in 0..10 {
        if *remaining == 0 {
            return Err(Status::internal("truncated protobuf varint"));
        }
        let byte = reader.read_byte().await?;
        *remaining -= 1;
        if index == 9 && byte > 1 {
            return Err(Status::internal("invalid protobuf varint"));
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(Status::internal("invalid protobuf varint"))
}

async fn read_length(reader: &mut FrameReader, remaining: &mut usize) -> Result<usize, Status> {
    let length = read_varint(reader, remaining).await?;
    let length = usize::try_from(length)
        .map_err(|_| Status::internal("protobuf field length does not fit usize"))?;
    if length > *remaining {
        return Err(Status::internal("protobuf field exceeds declared message"));
    }
    Ok(length)
}

async fn skip_field(
    reader: &mut FrameReader,
    remaining: &mut usize,
    length: usize,
) -> Result<(), Status> {
    if length > *remaining {
        return Err(Status::internal("protobuf field exceeds declared message"));
    }
    reader.skip_exact(length).await?;
    *remaining -= length;
    Ok(())
}

fn success_response(response: UploadShardResponse) -> http::Response<Body> {
    let message_length = response.encoded_len();
    let mut encoded = Vec::with_capacity(5 + message_length);
    encoded.push(0);
    encoded.extend_from_slice(&(message_length as u32).to_be_bytes());
    response
        .encode(&mut encoded)
        .expect("encoding UploadShardResponse into Vec cannot fail");

    let mut trailers = http::HeaderMap::new();
    Status::ok("")
        .add_header(&mut trailers)
        .expect("an empty OK status has valid headers");
    let frames = futures::stream::iter([
        Ok::<_, Infallible>(Frame::data(Bytes::from(encoded))),
        Ok(Frame::trailers(trailers)),
    ]);

    http::Response::builder()
        .status(http::StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/grpc")
        .body(Body::new(StreamBody::new(frames)))
        .expect("static gRPC response is valid")
}

struct FrameReader {
    body: Body,
    current: Bytes,
}

impl FrameReader {
    fn new(body: Body) -> Self {
        Self {
            body,
            current: Bytes::new(),
        }
    }

    async fn read_byte(&mut self) -> Result<u8, Status> {
        self.fill().await?;
        if self.current.is_empty() {
            return Err(Status::internal("unexpected EOF in request body"));
        }
        let byte = self.current[0];
        self.current.advance(1);
        Ok(byte)
    }

    async fn copy_exact(&mut self, mut length: usize) -> Result<Vec<u8>, Status> {
        let mut output = Vec::with_capacity(length);
        while length > 0 {
            self.fill().await?;
            if self.current.is_empty() {
                return Err(Status::internal("unexpected EOF in request body"));
            }
            let take = length.min(self.current.len());
            output.extend_from_slice(&self.current[..take]);
            self.current.advance(take);
            length -= take;
        }
        Ok(output)
    }

    async fn skip_exact(&mut self, mut length: usize) -> Result<(), Status> {
        while length > 0 {
            self.fill().await?;
            if self.current.is_empty() {
                return Err(Status::internal("unexpected EOF in request body"));
            }
            let take = length.min(self.current.len());
            self.current.advance(take);
            length -= take;
        }
        Ok(())
    }

    async fn ensure_end(&mut self) -> Result<(), Status> {
        if !self.current.is_empty() {
            return Err(Status::internal("additional data after gRPC message"));
        }
        while let Some(frame) = self.body.frame().await {
            let frame = frame.map_err(|error| Status::internal(error.to_string()))?;
            if let Ok(data) = frame.into_data()
                && !data.is_empty()
            {
                return Err(Status::internal("additional data after gRPC message"));
            }
        }
        Ok(())
    }

    async fn fill(&mut self) -> Result<(), Status> {
        while self.current.is_empty() {
            let Some(frame) = self.body.frame().await else {
                return Ok(());
            };
            let frame = frame.map_err(|error| Status::internal(error.to_string()))?;
            if let Ok(data) = frame.into_data() {
                self.current = data;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use celestia_fibre::PaymentPromise as DomainPaymentPromise;
    use celestia_proto::celestia::fibre::v1::{BlobRow, BlobShard};
    use celestia_types::nmt::Namespace;
    use tonic::Code;

    use super::*;

    fn service() -> Arc<MockFibreService> {
        Arc::new(MockFibreService::new(
            ed25519_dalek::SigningKey::from_bytes(&[3; 32]),
            None,
        ))
    }

    fn promise() -> PaymentPromise {
        let signer = k256::ecdsa::SigningKey::from_slice(&[7; 32]).unwrap();
        let mut promise = DomainPaymentPromise {
            chain_id: "mock-1".into(),
            height: 1,
            namespace: Namespace::new_v0(b"test").unwrap(),
            upload_size: 1024,
            blob_version: 0,
            commitment: [9; 32],
            creation_timestamp: SystemTime::now(),
            signer_pubkey: *signer.verifying_key(),
            signature: None,
        };
        promise.sign(&signer).unwrap();
        (&promise).into()
    }

    fn encode_varint(mut value: u64, output: &mut Vec<u8>) {
        while value >= 0x80 {
            output.push((value as u8) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }

    fn message_field<M: Message>(field: u8, message: &M, output: &mut Vec<u8>) {
        output.push((field << 3) | 2);
        encode_varint(message.encoded_len() as u64, output);
        message.encode(output).unwrap();
    }

    fn valid_message(shard_first: bool) -> Vec<u8> {
        let promise = promise();
        let shard = BlobShard {
            rows: vec![BlobRow {
                index: 1,
                data: vec![5; 256].into(),
                proof: vec![],
            }],
            rlcs: Bytes::new(),
        };
        let mut message = Vec::new();
        if shard_first {
            message_field(2, &shard, &mut message);
            message_field(1, &promise, &mut message);
        } else {
            message_field(1, &promise, &mut message);
            message_field(2, &shard, &mut message);
        }
        message
    }

    fn envelope(message: &[u8]) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(5 + message.len());
        encoded.push(0);
        encoded.extend_from_slice(&(message.len() as u32).to_be_bytes());
        encoded.extend_from_slice(message);
        encoded
    }

    fn body(chunks: Vec<Bytes>) -> Body {
        let frames = chunks
            .into_iter()
            .map(|chunk| Ok::<_, Infallible>(Frame::data(chunk)));
        Body::new(StreamBody::new(futures::stream::iter(frames)))
    }

    async fn upload_chunks(chunks: Vec<Bytes>) -> Result<UploadShardResponse, Status> {
        upload(service(), body(chunks), 256 * 1024 * 1024).await
    }

    #[tokio::test]
    async fn valid_request_in_one_frame() {
        let encoded = envelope(&valid_message(false));
        let response = upload_chunks(vec![Bytes::from(encoded)]).await.unwrap();
        assert_eq!(response.validator_signature.len(), 64);
    }

    #[tokio::test]
    async fn valid_request_split_at_every_boundary() {
        let encoded = envelope(&valid_message(false));
        for split in 1..encoded.len() {
            let chunks = vec![
                Bytes::copy_from_slice(&encoded[..split]),
                Bytes::copy_from_slice(&encoded[split..]),
            ];
            upload_chunks(chunks)
                .await
                .unwrap_or_else(|error| panic!("split {split} failed: {error}"));
        }
    }

    #[tokio::test]
    async fn valid_request_in_one_byte_frames() {
        let encoded = envelope(&valid_message(false));
        let chunks = encoded
            .into_iter()
            .map(|byte| Bytes::from(vec![byte]))
            .collect();
        upload_chunks(chunks).await.unwrap();
    }

    #[tokio::test]
    async fn accepts_shard_before_promise() {
        let encoded = envelope(&valid_message(true));
        upload_chunks(vec![Bytes::from(encoded)]).await.unwrap();
    }

    #[tokio::test]
    async fn rejects_missing_promise() {
        let shard = BlobShard {
            rows: vec![],
            rlcs: Bytes::new(),
        };
        let mut message = Vec::new();
        message_field(2, &shard, &mut message);
        let error = upload_chunks(vec![Bytes::from(envelope(&message))])
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(error.message(), "missing promise");
    }

    #[tokio::test]
    async fn rejects_missing_shard() {
        let mut message = Vec::new();
        message_field(1, &promise(), &mut message);
        let error = upload_chunks(vec![Bytes::from(envelope(&message))])
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert_eq!(error.message(), "missing shard");
    }

    #[tokio::test]
    async fn rejects_truncated_shard() {
        let message = valid_message(false);
        let mut encoded = envelope(&message);
        encoded.pop();
        let error = upload_chunks(vec![Bytes::from(encoded)]).await.unwrap_err();
        assert_eq!(error.code(), Code::Internal);
    }

    #[tokio::test]
    async fn rejects_oversized_envelope() {
        let mut encoded = vec![0];
        encoded.extend_from_slice(&((256 * 1024 * 1024 + 1) as u32).to_be_bytes());
        let error = upload_chunks(vec![Bytes::from(encoded)]).await.unwrap_err();
        assert_eq!(error.code(), Code::OutOfRange);
    }

    #[tokio::test]
    async fn rejects_compression_flag() {
        let mut encoded = envelope(&valid_message(false));
        encoded[0] = 1;
        let error = upload_chunks(vec![Bytes::from(encoded)]).await.unwrap_err();
        assert_eq!(error.code(), Code::Unimplemented);
    }

    #[tokio::test]
    async fn preserves_invalid_promise_status() {
        let mut message = Vec::new();
        message_field(1, &PaymentPromise::default(), &mut message);
        message_field(
            2,
            &BlobShard {
                rows: vec![],
                rlcs: Bytes::new(),
            },
            &mut message,
        );
        let error = upload_chunks(vec![Bytes::from(envelope(&message))])
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert!(error.message().starts_with("invalid namespace:"));
    }

    #[tokio::test]
    async fn skips_supported_unknown_fields() {
        let mut message = Vec::new();
        encode_varint((10 << 3) as u64, &mut message);
        encode_varint(300, &mut message);
        encode_varint(((11 << 3) | 1) as u64, &mut message);
        message.extend_from_slice(&[0; 8]);
        encode_varint(((12 << 3) | 2) as u64, &mut message);
        encode_varint(3, &mut message);
        message.extend_from_slice(&[1, 2, 3]);
        encode_varint(((13 << 3) | 5) as u64, &mut message);
        message.extend_from_slice(&[0; 4]);
        message.extend_from_slice(&valid_message(false));
        upload_chunks(vec![Bytes::from(envelope(&message))])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn rejects_duplicate_fields() {
        for field in [1, 2] {
            let mut message = valid_message(false);
            if field == 1 {
                message_field(1, &promise(), &mut message);
            } else {
                message_field(
                    2,
                    &BlobShard {
                        rows: vec![],
                        rlcs: Bytes::new(),
                    },
                    &mut message,
                );
            }
            let error = upload_chunks(vec![Bytes::from(envelope(&message))])
                .await
                .unwrap_err();
            assert_eq!(error.code(), Code::InvalidArgument);
        }
    }

    #[tokio::test]
    async fn rejects_data_after_first_message() {
        let mut encoded = envelope(&valid_message(false));
        encoded.push(0);
        let error = upload_chunks(vec![Bytes::from(encoded)]).await.unwrap_err();
        assert_eq!(error.code(), Code::Internal);
    }
}
