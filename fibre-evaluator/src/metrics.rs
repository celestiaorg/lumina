use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;

use crate::stats::SharedStats;

#[derive(Clone, Copy)]
pub(crate) struct Config {
    pub(crate) started_at: Instant,
    pub(crate) client_count: usize,
    pub(crate) blobs_per_second: f64,
    pub(crate) workload: &'static str,
    pub(crate) wait_for_full_fanout_before_payment: bool,
    pub(crate) download_concurrency: usize,
}

pub(crate) async fn serve(
    listener: TcpListener,
    stats: SharedStats,
    config: Config,
) -> io::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let stats = stats.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_request(stream, stats, config).await {
                tracing::warn!(%error, "serving Prometheus metrics failed");
            }
        });
    }
}

async fn handle_request(
    mut stream: TcpStream,
    stats: SharedStats,
    config: Config,
) -> io::Result<()> {
    let mut request = [0; 1024];
    let bytes_read = stream.read(&mut request).await?;
    let (status, content_type, body) = if request[..bytes_read].starts_with(b"GET /metrics ") {
        let stats = stats.read().unwrap().clone();
        (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            stats.encode_prometheus(
                config.started_at.elapsed(),
                config.client_count,
                config.blobs_per_second,
                config.workload,
                config.wait_for_full_fanout_before_payment,
                config.download_concurrency,
            ),
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
        let server = tokio::spawn(serve(
            listener,
            stats,
            Config {
                started_at: Instant::now(),
                client_count: 2,
                blobs_per_second: 1.5,
                workload: "writer",
                wait_for_full_fanout_before_payment: false,
                download_concurrency: 4,
            },
        ));

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
        assert!(response.contains(
            "fibre_evaluator_run_info{workload=\"writer\",full_fanout_before_payment=\"false\"} 1\n"
        ));
        assert!(response.contains("fibre_evaluator_download_concurrency_limit 4\n"));
        server.abort();
    }
}
