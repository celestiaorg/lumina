//! Clients for the celestia Json-RPC.
//!
//! This module aims to provide a convenient way to create a Json-RPC clients. If
//! you need more configuration options and / or some custom client you can create
//! one using [`jsonrpsee`] crate directly.

#[cfg(not(target_arch = "wasm32"))]
pub use self::native::Client;

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
pub use self::wasm::Client;

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::fmt;
    use std::result::Result;
    use std::sync::Arc;
    use std::time::Duration;

    use http::{HeaderValue, header};
    use jsonrpsee::core::ClientError;
    use jsonrpsee::core::client::{BatchResponse, ClientT, Subscription, SubscriptionClientT};
    use jsonrpsee::core::params::BatchRequestBuilder;
    use jsonrpsee::core::traits::ToRpcParams;
    use jsonrpsee::http_client::{HeaderMap, HttpClient, HttpClientBuilder};
    use jsonrpsee::ws_client::{PingConfig, WsClient, WsClientBuilder};
    use serde::de::DeserializeOwned;
    use tokio::sync::RwLock;

    use crate::Error;

    const MAX_RESPONSE_SIZE: usize = 256 * 1024 * 1024;

    /// Json RPC client.
    pub enum Client {
        /// A client using 'http\[s\]' protocol.
        Http(HttpClient),
        /// A client using 'ws\[s\]' protocol.
        Ws(WsReconnectClient<WsClient>),
    }

    impl Client {
        /// Create a new Json RPC client.
        ///
        /// Only 'http\[s\]' and 'ws\[s\]' protocols are supported and they should
        /// be specified in the provided `url`. For more flexibility
        /// consider creating the client using [`jsonrpsee`] directly.
        ///
        /// Please note that currently the celestia-node supports only 'http' and 'ws'.
        /// For a secure connection you have to hide it behind a proxy.
        pub async fn new(
            url: &str,
            auth_token: Option<&str>,
            connect_timeout: Option<Duration>,
            request_timeout: Option<Duration>,
        ) -> Result<Self, Error> {
            let protocol = url.split_once(':').map(|(proto, _)| proto);
            let client = match protocol {
                Some("http") | Some("https") => {
                    let headers = build_headers(auth_token)?;
                    let mut builder = HttpClientBuilder::default()
                        .max_response_size(MAX_RESPONSE_SIZE as u32)
                        .set_headers(headers);
                    if let Some(timeout) = request_timeout {
                        builder = builder.request_timeout(timeout);
                    }
                    if let Some(timeout) = connect_timeout {
                        builder = builder.connect_timeout(timeout);
                    }
                    Client::Http(builder.build(url)?)
                }
                Some("ws") | Some("wss") => Client::Ws(
                    WsReconnectClient::new(url, auth_token, connect_timeout, request_timeout)
                        .await?,
                ),
                _ => return Err(Error::ProtocolNotSupported(url.into())),
            };

            Ok(client)
        }
    }

    pub struct WsReconnectClient<C> {
        state: RwLock<WsState<C>>,
        build: BuildFn<C>,
    }

    struct WsState<C> {
        inner: Arc<C>,
        poisoned: bool,
        epoch: u64,
    }

    type BuildFn<C> =
        Arc<dyn Fn() -> futures_util::future::BoxFuture<'static, Result<C, Error>> + Send + Sync>;

    impl WsReconnectClient<WsClient> {
        async fn new(
            url: &str,
            auth_token: Option<&str>,
            connect_timeout: Option<Duration>,
            request_timeout: Option<Duration>,
        ) -> Result<Self, Error> {
            let url = url.to_owned();
            let auth_token = auth_token.map(str::to_owned);
            let build: BuildFn<WsClient> = Arc::new(move || {
                let url = url.clone();
                let auth_token = auth_token.clone();
                Box::pin(async move {
                    build_ws_client(
                        &url,
                        auth_token.as_deref(),
                        connect_timeout,
                        request_timeout,
                    )
                    .await
                })
            });
            WsReconnectClient::new_with_factory(build).await
        }
    }

    impl<C> WsReconnectClient<C>
    where
        C: ClientT + SubscriptionClientT + Send + Sync + 'static,
    {
        async fn new_with_factory(build: BuildFn<C>) -> Result<Self, Error> {
            let inner = Arc::new((build)().await?);
            Ok(Self {
                state: RwLock::new(WsState {
                    inner,
                    poisoned: false,
                    epoch: 0,
                }),
                build,
            })
        }

        async fn get_inner(&self) -> Result<Arc<C>, ClientError> {
            let (epoch, poisoned, inner) = {
                let state = self.state.read().await;
                (state.epoch, state.poisoned, state.inner.clone())
            };
            if !poisoned {
                return Ok(inner);
            }

            let mut state = self.state.write().await;
            if state.poisoned && state.epoch == epoch {
                let new_inner = Arc::new(
                    (self.build)()
                        .await
                        .map_err(|err| ClientError::Custom(err.to_string()))?,
                );
                state.inner = new_inner;
                state.poisoned = false;
            }
            Ok(state.inner.clone())
        }

        async fn mark_poisoned(&self) {
            let mut state = self.state.write().await;
            if !state.poisoned {
                state.poisoned = true;
                state.epoch += 1;
            }
        }

        async fn notification<Params>(
            &self,
            method: &str,
            params: Params,
        ) -> Result<(), ClientError>
        where
            Params: ToRpcParams + Send,
        {
            let inner = self.get_inner().await?;
            let err = match inner.notification(method, params).await {
                Err(err @ ClientError::RestartNeeded(_)) => err,
                res => return res,
            };
            self.mark_poisoned().await;
            Err(err)
        }

        async fn request<R, Params>(&self, method: &str, params: Params) -> Result<R, ClientError>
        where
            R: DeserializeOwned,
            Params: ToRpcParams + Send,
        {
            let inner = self.get_inner().await?;
            let err = match inner.request(method, params).await {
                Err(err @ ClientError::RestartNeeded(_)) => err,
                res => return res,
            };
            self.mark_poisoned().await;
            Err(err)
        }

        async fn subscribe<'a, N, Params>(
            &self,
            subscribe_method: &'a str,
            params: Params,
            unsubscribe_method: &'a str,
        ) -> Result<Subscription<N>, ClientError>
        where
            Params: ToRpcParams + Send,
            N: DeserializeOwned,
        {
            let inner = self.get_inner().await?;
            let err = match inner
                .subscribe(subscribe_method, params, unsubscribe_method)
                .await
            {
                Err(err @ ClientError::RestartNeeded(_)) => err,
                res => return res,
            };
            self.mark_poisoned().await;
            Err(err)
        }

        async fn subscribe_to_method<N>(&self, method: &str) -> Result<Subscription<N>, ClientError>
        where
            N: DeserializeOwned,
        {
            let inner = self.get_inner().await?;
            let err = match inner.subscribe_to_method(method).await {
                Err(err @ ClientError::RestartNeeded(_)) => err,
                res => return res,
            };
            self.mark_poisoned().await;
            Err(err)
        }
    }

    fn build_headers(auth_token: Option<&str>) -> Result<HeaderMap, Error> {
        let mut headers = HeaderMap::new();

        if let Some(token) = auth_token {
            let val = HeaderValue::from_str(&format!("Bearer {token}"))?;
            headers.insert(header::AUTHORIZATION, val);
        }

        Ok(headers)
    }

    async fn build_ws_client(
        url: &str,
        auth_token: Option<&str>,
        connect_timeout: Option<Duration>,
        request_timeout: Option<Duration>,
    ) -> Result<WsClient, Error> {
        let headers = build_headers(auth_token)?;
        let mut builder = WsClientBuilder::default()
            .max_response_size(MAX_RESPONSE_SIZE as u32)
            .set_headers(headers)
            .enable_ws_ping(PingConfig::default());
        if let Some(timeout) = request_timeout {
            builder = builder.request_timeout(timeout);
        }
        if let Some(timeout) = connect_timeout {
            builder = builder.connection_timeout(timeout);
        }
        Ok(builder.build(url).await?)
    }

    impl ClientT for Client {
        async fn notification<Params>(
            &self,
            method: &str,
            params: Params,
        ) -> Result<(), ClientError>
        where
            Params: ToRpcParams + Send,
        {
            match self {
                Client::Http(client) => client.notification(method, params).await,
                Client::Ws(client) => client.notification(method, params).await,
            }
        }

        async fn request<R, Params>(&self, method: &str, params: Params) -> Result<R, ClientError>
        where
            R: DeserializeOwned,
            Params: ToRpcParams + Send,
        {
            match self {
                Client::Http(client) => client.request(method, params).await,
                Client::Ws(client) => client.request(method, params).await,
            }
        }

        async fn batch_request<'a, R>(
            &self,
            batch: BatchRequestBuilder<'a>,
        ) -> Result<BatchResponse<'a, R>, ClientError>
        where
            R: DeserializeOwned + fmt::Debug + 'a,
        {
            match self {
                Client::Http(client) => client.batch_request(batch).await,
                Client::Ws(client) => {
                    let inner = client.get_inner().await?;
                    let err = match inner.batch_request(batch).await {
                        Err(err @ ClientError::RestartNeeded(_)) => err,
                        res => return res,
                    };
                    client.mark_poisoned().await;
                    Err(err)
                }
            }
        }
    }

    impl SubscriptionClientT for Client {
        async fn subscribe<'a, N, Params>(
            &self,
            subscribe_method: &'a str,
            params: Params,
            unsubscribe_method: &'a str,
        ) -> Result<Subscription<N>, ClientError>
        where
            Params: ToRpcParams + Send,
            N: DeserializeOwned,
        {
            match self {
                Client::Http(client) => {
                    client
                        .subscribe(subscribe_method, params, unsubscribe_method)
                        .await
                }
                Client::Ws(client) => {
                    client
                        .subscribe(subscribe_method, params, unsubscribe_method)
                        .await
                }
            }
        }

        async fn subscribe_to_method<N>(&self, method: &str) -> Result<Subscription<N>, ClientError>
        where
            N: DeserializeOwned,
        {
            match self {
                Client::Http(client) => client.subscribe_to_method(method).await,
                Client::Ws(client) => client.subscribe_to_method(method).await,
            }
        }
    }

    impl fmt::Debug for Client {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("Client { .. }")
        }
    }

    #[cfg(test)]
    mod tests {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::Duration;

        use jsonrpsee::core::ClientError;
        use jsonrpsee::core::client::ClientT;
        use jsonrpsee::core::params::BatchRequestBuilder;
        use serde::de::DeserializeOwned;
        use serde_json::Value as JsonValue;
        use tokio::join;
        use tokio::sync::Barrier;

        use super::{BuildFn, Client, WsReconnectClient};

        struct FakeWsClient {
            remaining_failures: AtomicUsize,
            response: JsonValue,
            barrier: Option<Arc<tokio::sync::Barrier>>,
        }

        impl FakeWsClient {
            fn new(remaining_failures: usize, response: JsonValue) -> Self {
                Self {
                    remaining_failures: AtomicUsize::new(remaining_failures),
                    response,
                    barrier: None,
                }
            }

            fn new_with_barrier(
                remaining_failures: usize,
                response: JsonValue,
                barrier: Arc<tokio::sync::Barrier>,
            ) -> Self {
                Self {
                    remaining_failures: AtomicUsize::new(remaining_failures),
                    response,
                    barrier: Some(barrier),
                }
            }
        }

        impl jsonrpsee::core::client::ClientT for FakeWsClient {
            async fn notification<Params>(
                &self,
                _method: &str,
                _params: Params,
            ) -> Result<(), ClientError>
            where
                Params: jsonrpsee::core::traits::ToRpcParams + Send,
            {
                Ok(())
            }

            fn request<R, Params>(
                &self,
                _method: &str,
                _params: Params,
            ) -> impl std::future::Future<Output = Result<R, ClientError>> + Send
            where
                R: DeserializeOwned,
                Params: jsonrpsee::core::traits::ToRpcParams + Send,
            {
                let barrier = self.barrier.clone();
                let response = self.response.clone();
                async move {
                    if let Some(barrier) = barrier {
                        barrier.wait().await;
                    }
                    let should_fail = self
                        .remaining_failures
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                            value.checked_sub(1)
                        })
                        .is_ok();
                    if should_fail {
                        return Err(ClientError::RestartNeeded(Arc::new(ClientError::Custom(
                            "restart".into(),
                        ))));
                    }
                    serde_json::from_value(response).map_err(ClientError::ParseError)
                }
            }

            async fn batch_request<'a, R>(
                &self,
                _batch: BatchRequestBuilder<'a>,
            ) -> Result<jsonrpsee::core::client::BatchResponse<'a, R>, ClientError>
            where
                R: DeserializeOwned + std::fmt::Debug + 'a,
            {
                Err(ClientError::Custom(
                    "batch_request not implemented in FakeWsClient".into(),
                ))
            }
        }

        impl jsonrpsee::core::client::SubscriptionClientT for FakeWsClient {
            async fn subscribe<'a, N, Params>(
                &self,
                _subscribe_method: &'a str,
                _params: Params,
                _unsubscribe_method: &'a str,
            ) -> Result<jsonrpsee::core::client::Subscription<N>, ClientError>
            where
                Params: jsonrpsee::core::traits::ToRpcParams + Send,
                N: DeserializeOwned,
            {
                Err(ClientError::Custom(
                    "subscribe not implemented in FakeWsClient".into(),
                ))
            }

            async fn subscribe_to_method<N>(
                &self,
                _method: &str,
            ) -> Result<jsonrpsee::core::client::Subscription<N>, ClientError>
            where
                N: DeserializeOwned,
            {
                Err(ClientError::Custom(
                    "subscribe_to_method not implemented in FakeWsClient".into(),
                ))
            }
        }

        #[tokio::test]
        async fn request_marks_poisoned_on_restart_needed() {
            let build_count = Arc::new(AtomicUsize::new(0));
            let response = serde_json::json!(7u64);
            let build: BuildFn<FakeWsClient> = {
                let build_count = build_count.clone();
                Arc::new(move || {
                    let build_count = build_count.clone();
                    let response = response.clone();
                    Box::pin(async move {
                        let id = build_count.fetch_add(1, Ordering::SeqCst);
                        let failures = if id == 0 { 1 } else { 0 };
                        Ok(FakeWsClient::new(failures, response))
                    })
                })
            };

            let client = WsReconnectClient::new_with_factory(build).await.unwrap();
            let err = client
                .request::<u64, _>("test", Vec::<u8>::new())
                .await
                .unwrap_err();
            assert!(matches!(err, ClientError::RestartNeeded(_)));
            assert_eq!(build_count.load(Ordering::SeqCst), 1);

            let value: u64 = client.request("test", Vec::<u8>::new()).await.unwrap();
            assert_eq!(value, 7);
            assert_eq!(build_count.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn concurrent_requests_share_single_reconnect() {
            let build_count = Arc::new(AtomicUsize::new(0));
            let response = serde_json::json!(5u64);
            let build: BuildFn<FakeWsClient> = {
                let build_count = build_count.clone();
                Arc::new(move || {
                    let build_count = build_count.clone();
                    let response = response.clone();
                    Box::pin(async move {
                        let id = build_count.fetch_add(1, Ordering::SeqCst);
                        let failures = if id == 0 { 1 } else { 0 };
                        Ok(FakeWsClient::new(failures, response))
                    })
                })
            };

            let client = WsReconnectClient::new_with_factory(build).await.unwrap();
            let err = client
                .request::<u64, _>("test", Vec::<u8>::new())
                .await
                .unwrap_err();
            assert!(matches!(err, ClientError::RestartNeeded(_)));
            assert_eq!(build_count.load(Ordering::SeqCst), 1);

            let (a, b) = join!(
                client.request::<u64, _>("test", Vec::<u8>::new()),
                client.request::<u64, _>("test", Vec::<u8>::new())
            );
            assert_eq!(a.unwrap(), 5);
            assert_eq!(b.unwrap(), 5);
            assert_eq!(build_count.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn concurrent_restart_needed_dedupes_reconnect() {
            let build_count = Arc::new(AtomicUsize::new(0));
            let response = serde_json::json!(9u64);
            let barrier = Arc::new(Barrier::new(2));
            let build: BuildFn<FakeWsClient> = {
                let build_count = build_count.clone();
                let barrier = barrier.clone();
                Arc::new(move || {
                    let build_count = build_count.clone();
                    let response = response.clone();
                    let barrier = barrier.clone();
                    Box::pin(async move {
                        let id = build_count.fetch_add(1, Ordering::SeqCst);
                        let failures = if id == 0 { 2 } else { 0 };
                        if id == 0 {
                            Ok(FakeWsClient::new_with_barrier(failures, response, barrier))
                        } else {
                            Ok(FakeWsClient::new(failures, response))
                        }
                    })
                })
            };

            let client = WsReconnectClient::new_with_factory(build).await.unwrap();
            let (a, b) = join!(
                client.request::<u64, _>("test", Vec::<u8>::new()),
                client.request::<u64, _>("test", Vec::<u8>::new())
            );
            assert!(matches!(a.unwrap_err(), ClientError::RestartNeeded(_)));
            assert!(matches!(b.unwrap_err(), ClientError::RestartNeeded(_)));
            assert_eq!(build_count.load(Ordering::SeqCst), 1);

            let value: u64 = client.request("test", Vec::<u8>::new()).await.unwrap();
            assert_eq!(value, 9);
            assert_eq!(build_count.load(Ordering::SeqCst), 2);
        }

        #[tokio::test]
        async fn http_connect_timeout_fires_before_request_timeout() {
            // 192.0.2.1 is TEST-NET-1 (RFC 5737): not routable, so the TCP
            // handshake never completes.
            let client = Client::new(
                "http://192.0.2.1:9",
                None,
                Some(Duration::from_millis(100)),
                Some(Duration::from_secs(60)),
            )
            .await
            .unwrap();

            let res = tokio::time::timeout(
                Duration::from_secs(10),
                client.request::<u64, _>("test", Vec::<u8>::new()),
            )
            .await
            .expect("connect should fail well before the request timeout");

            match res {
                Ok(_) => panic!("expected a connect error"),
                Err(ClientError::RequestTimeout) => panic!("failed via request timeout"),
                Err(_) => {}
            }
        }
    }
}

#[cfg(all(target_arch = "wasm32", feature = "wasm-bindgen"))]
mod wasm {
    use std::fmt;
    use std::result::Result;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use gloo_net::http::{Request as JsRequest, Response as JsResponse};
    use gloo_timers::callback::Timeout;
    use jsonrpsee::core::client::{BatchResponse, ClientT, Subscription, SubscriptionClientT};
    use jsonrpsee::core::middleware::Batch;
    use jsonrpsee::core::params::BatchRequestBuilder;
    use jsonrpsee::core::traits::ToRpcParams;
    use jsonrpsee::core::{ClientError, JsonRawValue};
    use jsonrpsee::types::{
        Id, InvalidRequestId, Notification, Request, Response, ResponseSuccess,
    };
    use send_wrapper::SendWrapper;
    use serde::Serialize;
    use serde::de::DeserializeOwned;
    use tracing::warn;
    use web_sys::AbortController;

    use crate::Error;

    const ABORT_ERROR_NAME: &str = "AbortError";

    /// Json RPC client.
    pub struct Client {
        id: AtomicU64,
        url: String,
        auth_token: Option<String>,
        timeout_ms: Option<u32>,
    }

    impl Client {
        /// Create a new Json RPC client.
        ///
        /// Only the 'http\[s\]' protocols are supported because JavaScript
        /// doesn't allow setting headers with websocket. If you want to
        /// use the websocket client anyway, you can use the one from the
        /// `jsonrpsee` directly, but you need a node with `--rpc.skip-auth`.
        pub async fn new(
            url: &str,
            auth_token: Option<&str>,
            connect_timeout: Option<Duration>,
            request_timeout: Option<Duration>,
        ) -> Result<Self, Error> {
            let protocol = url.split_once(':').map(|(proto, _)| proto);
            match protocol {
                Some("http") | Some("https") => (),
                _ => return Err(Error::ProtocolNotSupported(url.into())),
            };
            let timeout_ms = request_timeout
                .map(|t| t.as_millis().try_into())
                .transpose()
                .map_err(|_| Error::TimeoutOutOfRange)?;
            if connect_timeout.is_some() {
                warn!("ignored connect_timeout: not supported in wasm");
            }

            Ok(Client {
                id: AtomicU64::new(0),
                url: url.into(),
                auth_token: auth_token.map(ToOwned::to_owned),
                timeout_ms,
            })
        }

        async fn send<T: Serialize>(&self, request: T) -> Result<JsResponse, ClientError> {
            let (fut, _timeout) = {
                let mut req = JsRequest::post(&self.url);
                let mut timeout_callback = None;

                if let Some(timeout) = self.timeout_ms {
                    let abort_controller =
                        AbortController::new().expect("AbortController should be available");
                    let abort_signal = abort_controller.signal();
                    let _ = timeout_callback.insert(Timeout::new(timeout, move || {
                        abort_controller.abort();
                    }));
                    req = req.abort_signal(Some(&abort_signal));
                }

                if let Some(token) = self.auth_token.as_ref() {
                    req = req.header("Authorization", &format!("Bearer {token}"));
                }

                (
                    req.json(&request).map_err(into_parse_error)?.send(),
                    SendWrapper::new(timeout_callback),
                )
            };

            SendWrapper::new(fut).await.map_err(|e| match e {
                gloo_net::Error::JsError(e) => {
                    if e.name == ABORT_ERROR_NAME {
                        ClientError::RequestTimeout
                    } else {
                        ClientError::Transport(e.into())
                    }
                }
                gloo_net::Error::SerdeError(e) => ClientError::ParseError(e),
                gloo_net::Error::GlooError(e) => ClientError::Transport(e.into()),
            })
        }

        async fn send_and_read_body<T: Serialize>(
            &self,
            request: T,
        ) -> Result<Vec<u8>, ClientError> {
            let response = SendWrapper::new(self.send(request).await?);

            if !response.ok() {
                let error = format!(
                    "Request failed: {} {}",
                    response.status(),
                    response.status_text(),
                );
                return Err(ClientError::Transport(error.into()));
            }

            SendWrapper::new(response.binary())
                .await
                .map_err(|err| ClientError::Transport(err.into()))
        }
    }

    impl ClientT for Client {
        async fn notification<Params>(
            &self,
            method: &str,
            params: Params,
        ) -> Result<(), ClientError>
        where
            Params: ToRpcParams + Send,
        {
            let params = params.to_rpc_params()?;
            let notification = Notification::new(method.into(), params);

            self.send(notification).await?;

            Ok(())
        }

        async fn request<R, Params>(&self, method: &str, params: Params) -> Result<R, ClientError>
        where
            R: DeserializeOwned,
            Params: ToRpcParams + Send,
        {
            let id = Id::Number(self.id.fetch_add(1, Ordering::Relaxed));
            let params = params.to_rpc_params()?;
            let request = Request::borrowed(method, params.as_deref(), id.clone());
            let body = self.send_and_read_body(request).await?;

            // deserialize whole jsonrpc response except the `result` field, which is
            // kept in raw form (if it is present at all)
            let response: Response<&JsonRawValue> = serde_json::from_slice(&body)?;
            // bail if the response is an error
            let success = ResponseSuccess::try_from(response)?;

            if success.id == id {
                // read the actual `result` field
                let result = serde_json::from_str(success.result.get())?;
                Ok(result)
            } else {
                Err(InvalidRequestId::NotPendingRequest(success.id.to_string()).into())
            }
        }

        async fn batch_request<'a, R>(
            &self,
            batch: BatchRequestBuilder<'a>,
        ) -> Result<BatchResponse<'a, R>, ClientError>
        where
            R: DeserializeOwned + fmt::Debug + 'a,
        {
            // this will throw an error if batch is empty
            let batch = batch.build()?;
            let batch_len = batch.len();

            // fill in batch request
            let mut ids = Vec::with_capacity(batch_len);
            let mut batch_request = Batch::with_capacity(batch_len);

            for (method, params) in batch.into_iter() {
                let id = self.id.fetch_add(1, Ordering::Relaxed);
                let request = Request::owned(method.into(), params, Id::Number(id));

                ids.push(id);
                batch_request.push(request);
            }

            // send it and grab response, using the same trick with not deserializing `result` field yet
            let body = self.send_and_read_body(batch_request).await?;
            let mut resps: Vec<Response<&JsonRawValue>> = serde_json::from_slice(&body)?;

            // no docs mention that responses should be returned in order
            // but jsonrpsee implementations always take care to do that
            resps.sort_by(|lhs, rhs| {
                // put those with non-numeric ids first, we'll error out on them
                // at the beginning of the next loop
                let lhs_id = lhs.id.try_parse_inner_as_number().unwrap_or(0);
                let rhs_id = rhs.id.try_parse_inner_as_number().unwrap_or(0);
                lhs_id.cmp(&rhs_id)
            });

            // prepare the batch result
            let mut successful = 0;
            let mut failed = 0;

            let mut batch_resp = Vec::with_capacity(batch_len);
            for resp in resps.into_iter() {
                let id = resp.id.try_parse_inner_as_number()?;
                if !ids.contains(&id) {
                    return Err(InvalidRequestId::NotPendingRequest(id.to_string()).into());
                }

                let res = match ResponseSuccess::try_from(resp) {
                    Ok(success) => {
                        successful += 1;
                        Ok(serde_json::from_str(success.result.get())?)
                    }
                    Err(error) => {
                        failed += 1;
                        Err(error)
                    }
                };

                batch_resp.push(res);
            }

            Ok(BatchResponse::new(successful, batch_resp, failed))
        }
    }

    impl SubscriptionClientT for Client {
        async fn subscribe<'a, N, Params>(
            &self,
            _subscribe_method: &'a str,
            _params: Params,
            _unsubscribe_method: &'a str,
        ) -> Result<Subscription<N>, ClientError>
        where
            Params: ToRpcParams + Send,
            N: DeserializeOwned,
        {
            Err(ClientError::HttpNotImplemented)
        }

        async fn subscribe_to_method<N>(
            &self,
            _method: &str,
        ) -> Result<Subscription<N>, ClientError>
        where
            N: DeserializeOwned,
        {
            Err(ClientError::HttpNotImplemented)
        }
    }

    impl fmt::Debug for Client {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("Client { .. }")
        }
    }

    /// Used to translate gloo errors originating from serde into client errors.
    /// Can panic if used in a place that has other gloo error types possible.
    fn into_parse_error(err: gloo_net::Error) -> ClientError {
        match err {
            gloo_net::Error::SerdeError(e) => ClientError::ParseError(e),
            _ => unreachable!("this can only fail on a call to serde_json"),
        }
    }
}
