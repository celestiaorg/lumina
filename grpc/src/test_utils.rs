use celestia_types::state::{AccAddress, Address};
use tendermint::crypto::default::ecdsa_secp256k1::SigningKey;
use tendermint::public_key::Secp256k1 as VerifyingKey;

pub use imp::*;

/// [`TestAccount`] stores celestia account credentials and information, for cases where we don't
/// mind just keeping the plaintext secret key in memory
#[derive(Debug, Clone)]
pub struct TestAccount {
    /// Bech32 `AccountId` of this account
    pub address: Address,
    /// public key
    #[allow(dead_code)]
    pub verifying_key: VerifyingKey,
    /// private key
    pub signing_key: SigningKey,
}

impl TestAccount {
    pub fn random() -> Self {
        let signing_key = SigningKey::random(&mut rand::rngs::OsRng);
        let verifying_key = *signing_key.verifying_key();

        Self {
            address: AccAddress::new(verifying_key.into()).into(),
            verifying_key,
            signing_key,
        }
    }

    pub fn from_pk(pk: &[u8]) -> Self {
        let signing_key = SigningKey::from_slice(pk).unwrap();
        let verifying_key = *signing_key.verifying_key();

        Self {
            address: AccAddress::new(verifying_key.into()).into(),
            verifying_key,
            signing_key,
        }
    }
}

pub fn load_account() -> TestAccount {
    let hex_key = include_str!("../../ci/credentials/node-0.plaintext-key").trim();

    TestAccount::from_pk(&hex::decode(hex_key).expect("valid hex representation"))
}

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    use std::{convert::Infallible, future::Future, net::SocketAddr, sync::Arc, sync::OnceLock};

    use bytes::Bytes;
    use celestia_rpc::Client;
    use http_body_util::{BodyExt, Empty};
    use hyper::{Request, Response, StatusCode, body::Incoming, header::HOST, service::service_fn};
    use hyper_util::{
        client::legacy::{Client as HyperClient, connect::HttpConnector},
        rt::{TokioExecutor, TokioIo},
        server::conn::auto::Builder as ServerBuilder,
    };
    use tokio::net::TcpListener;
    use tokio::sync::{Mutex, MutexGuard};

    use super::*;
    use crate::GrpcClient;

    pub const CELESTIA_GRPC_URL: &str = "http://localhost:19090";
    pub const TEST_AUTH_TOKEN: &str = "test-secret-token";
    pub const CELESTIA_RPC_URL: &str = "ws://localhost:26658";

    pub fn new_grpc_client() -> GrpcClient {
        GrpcClient::builder()
            .url(CELESTIA_GRPC_URL)
            .build()
            .unwrap()
    }

    pub async fn new_rpc_client() -> Client {
        Client::new(CELESTIA_RPC_URL, None, None, None)
            .await
            .unwrap()
    }

    // we have to sequence the tests which submits transactions.
    // multiple independent tx clients don't work well in parallel
    // as they break each other's account.sequence
    pub async fn new_tx_client() -> (MutexGuard<'static, ()>, GrpcClient) {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        let lock = LOCK.get_or_init(|| Mutex::new(())).lock().await;

        let creds = load_account();
        let client = GrpcClient::builder()
            .url(CELESTIA_GRPC_URL)
            .signer_keypair(creds.signing_key)
            .build()
            .unwrap();

        (lock, client)
    }

    pub fn spawn<F>(future: F) -> tokio::task::JoinHandle<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future)
    }

    /// Spawns a gRPC authentication proxy that validates the `authorization` header.
    ///
    /// Returns the socket address the proxy is listening on and a join handle for the server task.
    /// The proxy accepts both `Bearer {token}` and just `{token}` as valid authorization values.
    pub async fn spawn_grpc_auth_proxy(
        upstream_base: &str,
        expected_token: &str,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let mut http = HttpConnector::new();
        http.enforce_http(false);

        let client: HyperClient<_, Incoming> = HyperClient::builder(TokioExecutor::new())
            .http2_only(true)
            .build(http);

        let upstream_base = Arc::new(upstream_base.trim_end_matches('/').to_string());
        let expected_token = Arc::new(expected_token.to_string());
        let client = Arc::new(client);

        let handle = tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(stream) => stream,
                    Err(_) => break,
                };

                let upstream_base = upstream_base.clone();
                let expected_token = expected_token.clone();
                let client = client.clone();

                tokio::spawn(async move {
                    let service = service_fn(move |mut req: Request<Incoming>| {
                        let upstream_base = upstream_base.clone();
                        let expected_token = expected_token.clone();
                        let client = client.clone();

                        async move {
                            // --- 1) auth check ---
                            let auth = req
                                .headers()
                                .get("authorization")
                                .and_then(|v| v.to_str().ok());

                            let want = format!("Bearer {}", expected_token);
                            let ok = auth == Some(want.as_str())
                                || auth == Some(expected_token.as_str());

                            if !ok {
                                let resp = Response::builder()
                                    .status(StatusCode::UNAUTHORIZED)
                                    .header("content-type", "application/grpc")
                                    .body(Empty::<Bytes>::new().map_err(|err| match err {}).boxed())
                                    .unwrap();
                                return Ok::<_, Infallible>(resp);
                            }

                            // --- 2) rewrite URI to upstream (keep path/query intact) ---
                            let path_and_query = req
                                .uri()
                                .path_and_query()
                                .map(|pq| pq.as_str())
                                .unwrap_or("/");

                            let new_uri = format!("{}{}", upstream_base.as_str(), path_and_query);

                            *req.uri_mut() = new_uri.parse().unwrap();

                            if let Some(authority) =
                                req.uri().authority().map(|a| a.as_str().to_string())
                            {
                                req.headers_mut().insert(
                                    HOST,
                                    hyper::header::HeaderValue::from_str(&authority).unwrap(),
                                );
                            }

                            // --- 3) forward as-is (streaming body preserved) ---
                            match client.request(req).await {
                                Ok(resp) => Ok::<_, Infallible>(resp.map(|body| body.boxed())),
                                Err(_e) => {
                                    let resp = Response::builder()
                                        .status(StatusCode::BAD_GATEWAY)
                                        .header("content-type", "application/grpc")
                                        .body(
                                            Empty::<Bytes>::new()
                                                .map_err(|err| match err {})
                                                .boxed(),
                                        )
                                        .unwrap();
                                    Ok::<_, Infallible>(resp)
                                }
                            }
                        }
                    });

                    let stream = TokioIo::new(stream);
                    let _ = ServerBuilder::new(TokioExecutor::new())
                        .http2_only()
                        .serve_connection(stream, service)
                        .await;
                });
            }
        });

        (addr, handle)
    }
}

#[cfg(target_arch = "wasm32")]
mod imp {
    use std::future::Future;

    use celestia_rpc::Client as RpcClient;
    use tokio::sync::oneshot;
    use wasm_bindgen_futures::spawn_local;

    use super::*;
    use crate::GrpcClient;

    pub const CELESTIA_GRPCWEB_PROXY_URL: &str = "http://localhost:18080";
    pub const CELESTIA_GRPC_URL: &str = CELESTIA_GRPCWEB_PROXY_URL;
    pub const CELESTIA_RPC_URL: &str = "http://localhost:26658";

    pub fn new_grpc_client() -> GrpcClient {
        GrpcClient::builder()
            .url(CELESTIA_GRPCWEB_PROXY_URL)
            .build()
            .unwrap()
    }

    pub async fn new_rpc_client() -> RpcClient {
        RpcClient::new(CELESTIA_RPC_URL, None, None, None)
            .await
            .unwrap()
    }

    pub async fn new_tx_client() -> ((), GrpcClient) {
        let creds = load_account();
        let client = GrpcClient::builder()
            .url(CELESTIA_GRPCWEB_PROXY_URL)
            .signer_keypair(creds.signing_key)
            .build();

        ((), client.unwrap())
    }

    pub fn spawn<F>(future: F) -> oneshot::Receiver<()>
    where
        F: Future<Output = ()> + 'static,
    {
        let (tx, rx) = oneshot::channel();
        spawn_local(async move {
            future.await;
            let _ = tx.send(());
        });

        rx
    }
}
