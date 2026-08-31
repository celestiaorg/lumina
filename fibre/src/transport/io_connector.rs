use std::pin::Pin;

use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::FibreError;

/// A Fibre transport byte stream.
pub trait FibreIo: AsyncRead + AsyncWrite + Send + Unpin {}

impl<T> FibreIo for T where T: AsyncRead + AsyncWrite + Send + Unpin {}

/// A boxed Fibre transport byte stream.
pub type BoxedFibreIo = Pin<Box<dyn FibreIo + 'static>>;

/// Opens byte streams to Fibre validators.
#[async_trait::async_trait]
pub trait FibreIoConnector: Send + Sync {
    /// Connect to `host:port` without applying TLS.
    async fn connect(&self, host: String, port: u16) -> Result<BoxedFibreIo, FibreError>;
}

#[cfg(not(target_arch = "wasm32"))]
/// Opens Fibre connections with native TCP sockets.
#[derive(Debug, Clone, Copy, Default)]
pub struct NativeTcpConnector;

#[cfg(not(target_arch = "wasm32"))]
#[async_trait::async_trait]
impl FibreIoConnector for NativeTcpConnector {
    async fn connect(&self, host: String, port: u16) -> Result<BoxedFibreIo, FibreError> {
        let stream = tokio::net::TcpStream::connect((host, port))
            .await
            .map_err(|error| {
                FibreError::Other(format!("failed to connect Fibre socket: {error}"))
            })?;
        Ok(Box::pin(stream))
    }
}

#[cfg(target_arch = "wasm32")]
/// Opens Fibre connections through a WebSocket-to-TCP relay.
///
/// The relay must negotiate `celestia-fibre-tcp-v1`, accept one `host:port` text frame, reply with one `ok` text frame, and then exchange only binary frames containing the TCP byte stream.
#[derive(Debug, Clone)]
pub struct BrowserWebSocketConnector {
    relay_url: String,
}

#[cfg(target_arch = "wasm32")]
impl BrowserWebSocketConnector {
    /// Create a connector targeting `relay_url`.
    pub fn new(relay_url: impl Into<String>) -> Self {
        Self {
            relay_url: relay_url.into(),
        }
    }
}

#[cfg(target_arch = "wasm32")]
struct BrowserIo {
    socket: send_wrapper::SendWrapper<gloo_net::websocket::futures::WebSocket>,
    pending: Vec<u8>,
}

#[cfg(target_arch = "wasm32")]
impl AsyncRead for BrowserIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use futures::Stream;
        use gloo_net::websocket::{Message, WebSocketError};

        if buf.remaining() == 0 {
            return std::task::Poll::Ready(Ok(()));
        }
        let mut data = if self.pending.is_empty() {
            loop {
                match futures::ready!(Pin::new(&mut *self.socket).poll_next(cx)) {
                    Some(Ok(Message::Bytes(data))) if data.is_empty() => {}
                    Some(Ok(Message::Bytes(data))) => break data,
                    Some(Ok(Message::Text(_))) => {
                        return std::task::Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "Fibre relay sent a text frame after tunnel acknowledgement",
                        )));
                    }
                    Some(Err(WebSocketError::ConnectionClose(event))) if event.was_clean => {
                        return std::task::Poll::Ready(Ok(()));
                    }
                    Some(Err(error)) => {
                        return std::task::Poll::Ready(Err(std::io::Error::other(error)));
                    }
                    None => return std::task::Poll::Ready(Ok(())),
                }
            }
        } else {
            std::mem::take(&mut self.pending)
        };
        let count = data.len().min(buf.remaining());
        buf.put_slice(&data[..count]);
        if count < data.len() {
            self.pending = data.split_off(count);
        }
        std::task::Poll::Ready(Ok(()))
    }
}

#[cfg(target_arch = "wasm32")]
impl AsyncWrite for BrowserIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        use futures::Sink;
        use gloo_net::websocket::Message;

        futures::ready!(Pin::new(&mut *self.socket).poll_ready(cx))
            .map_err(std::io::Error::other)?;
        Pin::new(&mut *self.socket)
            .start_send(Message::Bytes(buf.to_vec()))
            .map_err(std::io::Error::other)?;
        std::task::Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use futures::Sink;

        Pin::new(&mut *self.socket)
            .poll_flush(cx)
            .map_err(std::io::Error::other)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use futures::Sink;

        Pin::new(&mut *self.socket)
            .poll_close(cx)
            .map_err(std::io::Error::other)
    }
}

#[cfg(target_arch = "wasm32")]
#[async_trait::async_trait]
impl FibreIoConnector for BrowserWebSocketConnector {
    async fn connect(&self, host: String, port: u16) -> Result<BoxedFibreIo, FibreError> {
        use futures::{SinkExt, StreamExt};
        use gloo_net::websocket::{Message, futures::WebSocket};

        const PROTOCOL: &str = "celestia-fibre-tcp-v1";

        let relay_url = self.relay_url.clone();
        let authority = if host.contains(':') {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };
        let connect = async move {
            let mut socket =
                WebSocket::open_with_protocol(&relay_url, PROTOCOL).map_err(|error| {
                    FibreError::Other(format!("failed to connect Fibre relay: {error}"))
                })?;
            socket
                .send(Message::Text(authority))
                .await
                .map_err(|error| {
                    FibreError::Other(format!("failed to open Fibre tunnel: {error}"))
                })?;
            match socket.next().await {
                Some(Ok(Message::Text(response))) if response == "ok" => {}
                Some(Ok(_)) => {
                    return Err(FibreError::Other(
                        "Fibre relay returned an invalid tunnel acknowledgement".into(),
                    ));
                }
                Some(Err(error)) => {
                    return Err(FibreError::Other(format!(
                        "failed to open Fibre tunnel: {error}"
                    )));
                }
                None => {
                    return Err(FibreError::Other(
                        "Fibre relay closed before acknowledging the tunnel".into(),
                    ));
                }
            }

            Ok(BrowserIo {
                socket: send_wrapper::SendWrapper::new(socket),
                pending: Vec::new(),
            })
        };

        Ok(Box::pin(send_wrapper::SendWrapper::new(connect).await?))
    }
}
