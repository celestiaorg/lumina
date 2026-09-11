use std::io;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;

use crate::stats::SharedStats;

pub(crate) async fn serve(
    listener: TcpListener,
    stats: SharedStats,
    started_at: Instant,
    client_count: usize,
    blobs_per_second: f64,
) -> io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let stats = stats.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_request(
                stream,
                stats,
                started_at.elapsed(),
                client_count,
                blobs_per_second,
            )
            .await
            {
                tracing::warn!(%error, "serving Prometheus metrics failed");
            }
        });
    }
}

async fn handle_request(
    mut stream: TcpStream,
    stats: SharedStats,
    elapsed: Duration,
    client_count: usize,
    blobs_per_second: f64,
) -> io::Result<()> {
    let mut request = [0; 1024];
    let bytes_read = stream.read(&mut request).await?;
    let (status, content_type, body) = if request[..bytes_read].starts_with(b"GET /metrics ") {
        let stats = stats.read().unwrap().clone();
        (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            stats.encode_prometheus(elapsed, client_count, blobs_per_second),
        )
    } else {
        (
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n".to_string(),
        )
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use super::*;
    use crate::stats::Stats;

    #[tokio::test]
    async fn serves_prometheus_metrics_at_metrics_path() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stats = Arc::new(RwLock::new(Stats::default()));
        let server = tokio::spawn(serve(listener, stats, Instant::now(), 2, 1.5));

        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();

        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("Content-Type: text/plain; version=0.0.4; charset=utf-8"));
        assert!(response.contains("fibre_evaluator_clients 2\n"));
        assert!(response.contains("fibre_evaluator_aggregate_target_blobs_per_second 3\n"));
        server.abort();
    }
}
