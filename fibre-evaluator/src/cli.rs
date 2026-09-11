use std::net::SocketAddr;

use celestia_types::nmt::Namespace;
use clap::Parser;
use k256::ecdsa::SigningKey;

use crate::payload::payload_size_for_paid_size;

#[derive(Parser)]
#[command(name = "fibre-evaluator", version)]
#[command(about = "Evaluate Fibre upload and download throughput")]
pub(crate) struct Cli {
    /// Chain ID included in Fibre payment promises.
    #[arg(long, value_parser = parse_non_empty)]
    pub(crate) chain_id: String,

    /// Celestia app gRPC endpoint used for host queries and payment transactions.
    #[arg(long, value_parser = parse_non_empty)]
    pub(crate) app_grpc_url: String,

    /// Celestia core gRPC endpoint used to fetch validator sets.
    #[arg(long, value_parser = parse_non_empty)]
    pub(crate) core_grpc_url: String,

    /// Hex-encoded secp256k1 private keys for Fibre promises and payment transactions.
    #[arg(
        long = "private-key",
        required_unless_present = "reader_only",
        value_parser = parse_private_key
    )]
    pub(crate) private_keys: Vec<String>,

    /// Ten-byte ASCII suffix for a version-zero namespace.
    #[arg(long, value_parser = parse_namespace)]
    pub(crate) namespace: String,

    /// Discover and download Fibre blobs paid after the startup height without uploading.
    #[arg(
        long,
        conflicts_with_all = [
            "private_keys",
            "blobs_per_second",
            "blob_size",
            "skip_download"
        ]
    )]
    pub(crate) reader_only: bool,

    /// Verify that downloaded blobs contain valid evaluator payloads and CRCs.
    #[arg(long, requires = "reader_only")]
    pub(crate) verify_crc: bool,

    /// Target number of blob lifecycle launches per second.
    #[arg(
        long,
        required_unless_present = "reader_only",
        value_parser = parse_blob_rate
    )]
    pub(crate) blobs_per_second: Option<f64>,

    /// Exact paid Fibre upload size of each blob in bytes.
    #[arg(
        long,
        required_unless_present = "reader_only",
        value_parser = parse_blob_size
    )]
    pub(crate) blob_size: Option<usize>,

    /// Duration during which new jobs are scheduled.
    #[arg(long, value_parser = parse_positive_u64)]
    pub(crate) run_for_seconds: u64,

    /// Maximum concurrent end-to-end blob lifecycles.
    #[arg(long, default_value_t = 16, value_parser = parse_positive_usize)]
    pub(crate) max_in_flight: usize,

    /// Capacity of the admission queue.
    #[arg(long, default_value_t = 16, value_parser = parse_positive_usize)]
    pub(crate) queue_capacity: usize,

    /// Maximum concurrent payload encoding jobs.
    #[arg(long, default_value_t = 1, value_parser = parse_positive_usize)]
    pub(crate) encode_concurrency: usize,

    /// Number of Tokio worker threads shared by all signers.
    #[arg(long, default_value_t = 32, value_parser = parse_positive_usize)]
    pub(crate) tokio_worker_threads: usize,

    /// Number of Rayon encoding threads dedicated to each signer.
    #[arg(long, default_value_t = 4, value_parser = parse_positive_usize)]
    pub(crate) rayon_threads_per_signer: usize,

    /// Maximum concurrent blob downloads.
    #[arg(long, default_value_t = 4, value_parser = parse_positive_usize)]
    pub(crate) download_concurrency: usize,

    /// Skip blob download and integrity verification after payment confirmation.
    #[arg(long)]
    pub(crate) skip_download: bool,

    /// Timeout applied separately to each network stage.
    #[arg(long, default_value_t = 120, value_parser = parse_positive_u64)]
    pub(crate) operation_timeout_seconds: u64,

    /// Fixed payment transaction gas limit. Dynamic estimation is used when omitted.
    #[arg(long, value_parser = parse_positive_u64)]
    pub(crate) gas_limit: Option<u64>,

    /// Fixed payment transaction gas price. Dynamic estimation is used when omitted.
    #[arg(long, value_parser = parse_positive_f64)]
    pub(crate) gas_price: Option<f64>,

    /// Interval between periodic statistics reports.
    #[arg(long, default_value_t = 10, value_parser = parse_positive_u64)]
    pub(crate) stats_interval_seconds: u64,

    /// Address serving Prometheus metrics at /metrics.
    #[arg(long, default_value = "0.0.0.0:9464")]
    pub(crate) metrics_listen_addr: SocketAddr,

    /// Export logs over OTLP/HTTP to the local collector (default http://localhost:4318,
    /// override with OTEL_EXPORTER_OTLP_ENDPOINT). Logs stay on stdout too.
    #[arg(long)]
    pub(crate) otel_logs: bool,
}

fn parse_non_empty(value: &str) -> Result<String, String> {
    if value.is_empty() {
        Err("must not be empty".to_string())
    } else {
        Ok(value.to_string())
    }
}

fn parse_private_key(value: &str) -> Result<String, String> {
    let bytes = hex::decode(value).map_err(|error| format!("invalid hex: {error}"))?;
    SigningKey::from_slice(&bytes)
        .map_err(|_| "must be a valid 32-byte secp256k1 key".to_string())?;
    Ok(value.to_string())
}

fn parse_namespace(value: &str) -> Result<String, String> {
    if !value.is_ascii() {
        return Err("must be ASCII".to_string());
    }
    if value.len() != 10 {
        return Err(format!("must be exactly 10 bytes, got {}", value.len()));
    }
    Namespace::new_v0(value.as_bytes()).map_err(|error| error.to_string())?;
    Ok(value.to_string())
}

fn parse_blob_rate(value: &str) -> Result<f64, String> {
    let rate = value
        .parse::<f64>()
        .map_err(|error| format!("invalid rate: {error}"))?;
    if !rate.is_finite() || rate <= 0.0 {
        return Err("must be finite and greater than zero".to_string());
    }
    if 1.0 / rate < 1e-9 {
        return Err("must not exceed 1,000,000,000 blobs per second".to_string());
    }
    Ok(rate)
}

fn parse_blob_size(value: &str) -> Result<usize, String> {
    let size = value
        .parse::<usize>()
        .map_err(|error| format!("invalid size: {error}"))?;
    payload_size_for_paid_size(size)?;
    Ok(size)
}

fn parse_positive_f64(value: &str) -> Result<f64, String> {
    let value = value
        .parse::<f64>()
        .map_err(|error| format!("invalid number: {error}"))?;
    if !value.is_finite() || value <= 0.0 {
        Err("must be finite and greater than zero".to_string())
    } else {
        Ok(value)
    }
}

fn parse_positive_u64(value: &str) -> Result<u64, String> {
    let value = value
        .parse::<u64>()
        .map_err(|error| format!("invalid integer: {error}"))?;
    if value == 0 {
        Err("must be greater than zero".to_string())
    } else {
        Ok(value)
    }
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let value = value
        .parse::<usize>()
        .map_err(|error| format!("invalid integer: {error}"))?;
    if value == 0 {
        Err("must be greater than zero".to_string())
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_KEY: &str = "0101010101010101010101010101010101010101010101010101010101010101";
    const SECOND_VALID_KEY: &str =
        "0202020202020202020202020202020202020202020202020202020202020202";

    fn valid_args() -> Vec<&'static str> {
        vec![
            "fibre-evaluator",
            "--chain-id",
            "test-chain",
            "--app-grpc-url",
            "http://127.0.0.1:9091",
            "--core-grpc-url",
            "http://127.0.0.1:9090",
            "--private-key",
            VALID_KEY,
            "--namespace",
            "fibre-eval",
            "--blobs-per-second",
            "2.5",
            "--blob-size",
            "134217728",
            "--run-for-seconds",
            "60",
        ]
    }

    #[test]
    fn parses_required_arguments_and_defaults() {
        let cli = Cli::try_parse_from(valid_args()).unwrap();
        assert_eq!(cli.chain_id, "test-chain");
        assert_eq!(cli.private_keys, [VALID_KEY]);
        assert_eq!(cli.namespace, "fibre-eval");
        assert!(!cli.reader_only);
        assert!(!cli.verify_crc);
        assert_eq!(cli.blobs_per_second, Some(2.5));
        assert_eq!(cli.blob_size, Some(134_217_728));
        assert_eq!(cli.max_in_flight, 16);
        assert_eq!(cli.queue_capacity, 16);
        assert_eq!(cli.encode_concurrency, 1);
        assert_eq!(cli.tokio_worker_threads, 32);
        assert_eq!(cli.rayon_threads_per_signer, 4);
        assert_eq!(cli.download_concurrency, 4);
        assert!(!cli.skip_download);
        assert_eq!(cli.operation_timeout_seconds, 120);
        assert_eq!(cli.gas_limit, None);
        assert_eq!(cli.gas_price, None);
        assert_eq!(cli.stats_interval_seconds, 10);
        assert_eq!(cli.metrics_listen_addr, "0.0.0.0:9464".parse().unwrap());
        assert!(!cli.otel_logs);
    }

    #[test]
    fn parses_repeated_private_keys() {
        let mut args = valid_args();
        args.extend(["--private-key", SECOND_VALID_KEY]);
        let cli = Cli::try_parse_from(args).unwrap();
        assert_eq!(cli.private_keys, [VALID_KEY, SECOND_VALID_KEY]);
    }

    #[test]
    fn requires_at_least_one_private_key() {
        let mut args = valid_args();
        let key_flag = args.iter().position(|arg| *arg == "--private-key").unwrap();
        args.drain(key_flag..=key_flag + 1);
        assert!(Cli::try_parse_from(args).is_err());
    }

    #[test]
    fn parses_reader_only_without_writer_arguments() {
        let mut args = vec![
            "fibre-evaluator",
            "--chain-id",
            "test-chain",
            "--app-grpc-url",
            "http://127.0.0.1:9091",
            "--core-grpc-url",
            "http://127.0.0.1:9090",
            "--namespace",
            "fibre-eval",
            "--reader-only",
            "--run-for-seconds",
            "60",
        ];
        let cli = Cli::try_parse_from(&args).unwrap();

        assert!(cli.private_keys.is_empty());
        assert_eq!(cli.namespace, "fibre-eval");
        assert!(cli.reader_only);
        assert!(!cli.verify_crc);
        assert!(cli.blobs_per_second.is_none());
        assert!(cli.blob_size.is_none());

        args.push("--verify-crc");
        assert!(Cli::try_parse_from(args).unwrap().verify_crc);
    }

    #[test]
    fn reader_only_rejects_writer_arguments() {
        let mut writer_args = valid_args();
        writer_args.push("--reader-only");
        assert!(Cli::try_parse_from(writer_args).is_err());
    }

    #[test]
    fn parses_skip_download() {
        let mut args = valid_args();
        args.push("--skip-download");
        assert!(Cli::try_parse_from(args).unwrap().skip_download);
    }

    #[test]
    fn parses_otel_logs() {
        let mut args = valid_args();
        args.push("--otel-logs");
        assert!(Cli::try_parse_from(args).unwrap().otel_logs);
    }

    #[test]
    fn rejects_invalid_workload_arguments() {
        for (flag, value) in [
            ("--blobs-per-second", "0"),
            ("--blobs-per-second", "NaN"),
            ("--blob-size", "262145"),
            ("--run-for-seconds", "0"),
            ("--max-in-flight", "0"),
            ("--queue-capacity", "0"),
            ("--encode-concurrency", "0"),
            ("--tokio-worker-threads", "0"),
            ("--rayon-threads-per-signer", "0"),
            ("--download-concurrency", "0"),
            ("--operation-timeout-seconds", "0"),
            ("--gas-limit", "0"),
            ("--gas-price", "NaN"),
            ("--stats-interval-seconds", "0"),
        ] {
            let mut args = valid_args();
            args.extend([flag, value]);
            assert!(Cli::try_parse_from(args).is_err(), "{flag}={value}");
        }
    }

    #[test]
    fn rejects_invalid_key_and_namespace() {
        let mut invalid_key = valid_args();
        invalid_key.extend(["--private-key", "not-hex"]);
        assert!(Cli::try_parse_from(invalid_key).is_err());

        let mut invalid_namespace = valid_args();
        let namespace_index = invalid_namespace
            .iter()
            .position(|arg| *arg == "fibre-eval")
            .unwrap();
        invalid_namespace[namespace_index] = "short";
        assert!(Cli::try_parse_from(invalid_namespace).is_err());
    }
}
