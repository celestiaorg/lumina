use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll, ready};
use std::{error::Error as StdError, future::Future};

use bytes::Bytes;
use dyn_clone::{DynClone, clone_box};
use futures::FutureExt;
use http_body::Frame;
use tonic::body::Body as TonicBody;
use tonic::codegen::Service;

use crate::grpc::Context as GrpcContext;
use crate::utils::CondSend;

/// Metadata associated with a transport endpoint
#[derive(Debug, Clone, Default)]
pub(crate) struct TransportMetadata {
    /// URL or identifier for the transport endpoint
    pub url: Option<String>,
    /// Context (metadata, timeout) specific to this endpoint
    pub context: GrpcContext,
}

impl TransportMetadata {
    pub fn with_url_and_context(url: impl Into<String>, context: GrpcContext) -> Self {
        Self {
            url: Some(url.into()),
            context,
        }
    }
}

dyn_clone::clone_trait_object!(AbstractTransport);

type BoxedResponse = http::Response<BoxedBody>;
type BoxedError = Box<dyn StdError + Sync + Send + 'static>;
type BoxedResponseFuture =
    Pin<Box<dyn ConditionalSendFuture<Output = Result<BoxedResponse, BoxedError>> + 'static>>;

pub(crate) struct BoxedTransport {
    inner: Box<dyn AbstractTransport + Send + Sync>,
    pub(crate) metadata: Arc<TransportMetadata>,
}

pub(crate) struct BoxedBody {
    inner: Box<dyn AbstractBody + Unpin + Send + 'static>,
}

pub(crate) trait ConditionalSendFuture: Future + CondSend {}

trait AbstractBody {
    fn poll_frame_inner(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxedError>>>;
}

impl<T: Future + CondSend> ConditionalSendFuture for T {}

trait AbstractTransport: DynClone {
    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), BoxedError>>;

    fn call(&mut self, req: http::Request<TonicBody>) -> BoxedResponseFuture;
}

trait AbstractResponse {
    fn boxed(self) -> http::Response<BoxedBody>;
}

impl http_body::Body for BoxedBody {
    type Data = Bytes;

    type Error = BoxedError;

    fn poll_frame(
        mut self: Pin<&mut BoxedBody>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        AbstractBody::poll_frame_inner(Pin::new(self.inner.deref_mut()), cx)
    }
}

impl<T> AbstractBody for T
where
    T: http_body::Body<Data = Bytes>,
    <T as http_body::Body>::Error: StdError + Send + Sync + 'static,
{
    fn poll_frame_inner(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxedError>>> {
        let ready = ready!(self.poll_frame(cx));

        let Some(result) = ready else {
            return Poll::Ready(None);
        };

        Poll::Ready(Some(result.map_err(box_err)))
    }
}

impl Clone for BoxedTransport {
    fn clone(&self) -> Self {
        Self {
            inner: clone_box(&*self.inner),
            metadata: Arc::clone(&self.metadata),
        }
    }
}

impl<B> AbstractResponse for http::Response<B>
where
    B: AbstractBody + Unpin + Send + 'static,
{
    fn boxed(self) -> http::Response<BoxedBody> {
        self.map(boxed_body)
    }
}

impl<T> AbstractTransport for T
where
    T: Service<http::Request<TonicBody>> + Clone,
    <T as Service<http::Request<TonicBody>>>::Response: AbstractResponse,
    <T as Service<http::Request<TonicBody>>>::Error: StdError + Send + Sync + 'static,
    <T as Service<http::Request<TonicBody>>>::Future: CondSend + 'static,
{
    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), BoxedError>> {
        Service::poll_ready(self, cx).map_err(box_err)
    }

    fn call(&mut self, req: http::Request<TonicBody>) -> BoxedResponseFuture {
        Box::pin(Service::call(self, req).map(|response| match response {
            Ok(response) => Ok(response.boxed()),
            Err(e) => Err(box_err(e)),
        }))
    }
}

impl Service<http::Request<TonicBody>> for BoxedTransport {
    type Response = http::Response<BoxedBody>;
    type Error = BoxedError;
    type Future = BoxedResponseFuture;

    fn poll_ready(&mut self, cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<TonicBody>) -> Self::Future {
        self.inner.call(req)
    }
}

pub(crate) fn boxed<T, B>(transport: T, metadata: TransportMetadata) -> BoxedTransport
where
    B: http_body::Body<Data = Bytes> + Send + Unpin + 'static,
    <B as http_body::Body>::Error: StdError + Sync + Send,
    T: Service<http::Request<TonicBody>, Response = http::Response<B>>
        + Send
        + Sync
        + Clone
        + 'static,
    <T as Service<http::Request<TonicBody>>>::Error: StdError + Send + Sync + 'static,
    <T as Service<http::Request<TonicBody>>>::Future: CondSend + 'static,
{
    BoxedTransport {
        inner: Box::new(transport),
        metadata: Arc::new(metadata),
    }
}

fn boxed_body<B>(body: B) -> BoxedBody
where
    B: AbstractBody + Send + Unpin + 'static,
{
    BoxedBody {
        inner: Box::new(body),
    }
}

fn box_err<E: StdError + Send + Sync + 'static>(e: E) -> BoxedError {
    Box::new(e)
}

#[cfg(test)]
mod tests {
    use super::*;

    // compile-only test for type compliance
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::diverging_sub_expression)]
    #[allow(dead_code)]
    #[allow(unreachable_code)]
    #[allow(unused_variables)]
    fn can_box_tonic_channel() {
        let endpoint: tonic::transport::Endpoint = unimplemented!();
        let _boxed = boxed(endpoint.connect_lazy(), TransportMetadata::default());
    }

    // compile-only test for type compliance
    #[cfg(target_arch = "wasm32")]
    #[allow(clippy::diverging_sub_expression)]
    #[allow(dead_code)]
    #[allow(unreachable_code)]
    #[allow(unused_variables)]
    fn can_box_grpc_web_client() {
        let client: tonic_web_wasm_client::Client = unimplemented!();
        let _boxed = boxed(client, TransportMetadata::default());
    }
}
