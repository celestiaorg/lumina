use std::collections::BTreeMap;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::{self, Instant, MissedTickBehavior};

const MIB: f64 = 1024.0 * 1024.0;
const GIB: f64 = MIB * 1024.0;

pub(crate) enum Event {
    Scheduled {
        count: u64,
    },
    Admitted,
    Dropped {
        reason: &'static str,
        count: u64,
    },
    Started {
        queue_latency: Duration,
    },
    StageFinished {
        stage: &'static str,
        elapsed: Duration,
    },
    LifecycleSuccess {
        payload_bytes: u64,
        paid_bytes: u64,
        elapsed: Duration,
    },
    LifecycleFailure {
        client: usize,
        signer: String,
        stage: &'static str,
        error: String,
        elapsed: Duration,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Stats {
    scheduled: u64,
    admitted: u64,
    dropped_queue_full: u64,
    dropped_scheduler_late: u64,
    started: u64,
    successes: u64,
    failures: u64,
    payload_bytes: u64,
    paid_bytes: u64,
    failures_by_stage: BTreeMap<&'static str, u64>,
    latencies: BTreeMap<&'static str, Vec<Duration>>,
}

pub(crate) async fn run_stats_collector(
    mut events: mpsc::UnboundedReceiver<Event>,
    stats_interval: Duration,
    started_at: Instant,
    client_count: usize,
    blobs_per_second: f64,
) -> Stats {
    let mut stats = Stats::default();
    let mut ticker = time::interval(stats_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        tokio::select! {
            event = events.recv() => match event {
                Some(event) => stats.apply(event),
                None => break,
            },
            _ = ticker.tick() => stats.log_periodic(
                started_at.elapsed(),
                client_count,
                blobs_per_second,
            ),
        }
    }

    stats
}

impl Stats {
    fn apply(&mut self, event: Event) {
        match event {
            Event::Scheduled { count } => self.scheduled += count,
            Event::Admitted => self.admitted += 1,
            Event::Dropped { reason, count } => match reason {
                "queue_full" => self.dropped_queue_full += count,
                "scheduler_late" => self.dropped_scheduler_late += count,
                _ => unreachable!("unknown drop reason"),
            },
            Event::Started { queue_latency } => {
                self.started += 1;
                self.record_latency("queue", queue_latency);
            }
            Event::StageFinished { stage, elapsed } => self.record_latency(stage, elapsed),
            Event::LifecycleSuccess {
                payload_bytes,
                paid_bytes,
                elapsed,
            } => {
                self.successes += 1;
                self.payload_bytes += payload_bytes;
                self.paid_bytes += paid_bytes;
                self.record_latency("total", elapsed);
            }
            Event::LifecycleFailure {
                client,
                signer,
                stage,
                error,
                elapsed,
            } => {
                self.failures += 1;
                *self.failures_by_stage.entry(stage).or_default() += 1;
                self.record_latency("total", elapsed);
                tracing::warn!(client, %signer, stage, %error, "blob lifecycle failed");
            }
        }
    }

    fn record_latency(&mut self, stage: &'static str, elapsed: Duration) {
        self.latencies.entry(stage).or_default().push(elapsed);
    }

    fn log_periodic(&self, elapsed: Duration, client_count: usize, blobs_per_second: f64) {
        let completed = self.successes + self.failures;
        tracing::info!(
            clients = client_count,
            per_client_target_blobs_per_second = blobs_per_second,
            aggregate_target_blobs_per_second = blobs_per_second * client_count as f64,
            elapsed_seconds = %format_args!("{:.2}", elapsed.as_secs_f64()),
            scheduled = self.scheduled,
            admitted = self.admitted,
            dropped_queue_full = self.dropped_queue_full,
            dropped_scheduler_late = self.dropped_scheduler_late,
            queued = self.admitted.saturating_sub(self.started),
            in_flight = self.started.saturating_sub(completed),
            verified = self.successes,
            failed = self.failures,
            blobs_per_second = %format_args!("{:.2}", per_second(self.successes, elapsed)),
            paid_gib_per_second = %format_args!("{:.6}", per_second(self.paid_bytes, elapsed) / GIB),
            "periodic stats"
        );
    }
}

pub(crate) fn print_final_report(
    stats: &Stats,
    launch_elapsed: Duration,
    total_elapsed: Duration,
    client_count: usize,
    blobs_per_second: f64,
) {
    let dropped = stats.dropped_queue_full + stats.dropped_scheduler_late;
    let payload_bps = per_second(stats.payload_bytes, total_elapsed);
    let paid_bps = per_second(stats.paid_bytes, total_elapsed);

    tracing::info!("final Fibre evaluation stats");
    tracing::info!(
        clients = client_count,
        per_client_target_blobs_per_second = blobs_per_second,
        aggregate_target_blobs_per_second = blobs_per_second * client_count as f64,
        scheduled = stats.scheduled,
        admitted = stats.admitted,
        dropped,
        dropped_queue_full = stats.dropped_queue_full,
        dropped_scheduler_late = stats.dropped_scheduler_late,
        launch_elapsed_seconds = launch_elapsed.as_secs_f64(),
        scheduled_per_second = per_second(stats.scheduled, launch_elapsed),
        admitted_per_second = per_second(stats.admitted, launch_elapsed),
        "admission stats"
    );
    tracing::info!(
        clients = client_count,
        started = stats.started,
        verified = stats.successes,
        failed = stats.failures,
        success_percent = %format_args!(
            "{:.2}",
            success_percent(stats.successes, stats.successes + stats.failures)
        ),
        payload_bytes = stats.payload_bytes,
        paid_bytes = stats.paid_bytes,
        elapsed_seconds = total_elapsed.as_secs(),
        elapsed_seconds_precise = %format_args!("{:.6}", total_elapsed.as_secs_f64()),
        blobs_per_second = %format_args!("{:.1}", per_second(stats.successes, total_elapsed)),
        payload_mib_per_second = %format_args!("{:.4}", payload_bps / MIB),
        payload_gib_per_second = %format_args!("{:.6}", payload_bps / GIB),
        paid_mib_per_second = %format_args!("{:.4}", paid_bps / MIB),
        paid_gib_per_second = %format_args!("{:.6}", paid_bps / GIB),
        "end-to-end stats"
    );

    for (stage, failures) in &stats.failures_by_stage {
        tracing::info!(stage, failures, "stage failures");
    }
    for stage in [
        "queue",
        "encode_wait",
        "encode_compute",
        "fibre_upload",
        "full_fanout",
        "payment_broadcast",
        "payment_confirmation",
        "download",
        "total",
    ] {
        let Some(samples) = stats.latencies.get(stage) else {
            continue;
        };
        tracing::info!(
            stage,
            samples = samples.len(),
            p50_ms = %format_args!("{:.3}", percentile_ms(samples, 0.50).unwrap()),
            p95_ms = %format_args!("{:.3}", percentile_ms(samples, 0.95).unwrap()),
            p99_ms = %format_args!("{:.3}", percentile_ms(samples, 0.99).unwrap()),
            "stage latency"
        );
    }
}

fn percentile_ms(samples: &[Duration], percentile: f64) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = ((sorted.len() as f64 * percentile).ceil() as usize).clamp(1, sorted.len());
    Some(sorted[rank - 1].as_secs_f64() * 1000.0)
}

fn success_percent(successes: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        successes as f64 * 100.0 / total as f64
    }
}

fn per_second(value: u64, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        0.0
    } else {
        value as f64 / elapsed.as_secs_f64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_aggregate_events_from_multiple_clients() {
        let mut stats = Stats::default();
        stats.apply(Event::Scheduled { count: 4 });
        stats.apply(Event::Scheduled { count: 3 });
        stats.apply(Event::Admitted);
        stats.apply(Event::Admitted);
        stats.apply(Event::Dropped {
            reason: "queue_full",
            count: 2,
        });
        stats.apply(Event::Dropped {
            reason: "scheduler_late",
            count: 1,
        });
        stats.apply(Event::Started {
            queue_latency: Duration::from_millis(2),
        });
        stats.apply(Event::LifecycleFailure {
            client: 1,
            signer: "signer-1".to_string(),
            stage: "download",
            error: "failed".to_string(),
            elapsed: Duration::from_millis(10),
        });
        stats.apply(Event::LifecycleSuccess {
            payload_bytes: 100,
            paid_bytes: 105,
            elapsed: Duration::from_millis(20),
        });

        assert_eq!(stats.scheduled, 7);
        assert_eq!(stats.admitted, 2);
        assert_eq!(stats.dropped_queue_full, 2);
        assert_eq!(stats.dropped_scheduler_late, 1);
        assert_eq!(stats.successes, 1);
        assert_eq!(stats.failures, 1);
        assert_eq!(stats.payload_bytes, 100);
        assert_eq!(stats.paid_bytes, 105);
        assert_eq!(stats.failures_by_stage["download"], 1);
        assert_eq!(stats.latencies["queue"], [Duration::from_millis(2)]);
        assert_eq!(
            stats.latencies["total"],
            [Duration::from_millis(10), Duration::from_millis(20)]
        );
    }

    #[test]
    fn percentile_and_rate_calculations_handle_boundaries() {
        let samples = [
            Duration::from_millis(1),
            Duration::from_millis(2),
            Duration::from_millis(3),
            Duration::from_millis(4),
        ];
        assert_eq!(percentile_ms(&[], 0.5), None);
        assert_eq!(percentile_ms(&samples, 0.5), Some(2.0));
        assert_eq!(percentile_ms(&samples, 0.95), Some(4.0));
        assert_eq!(success_percent(0, 0), 0.0);
        assert_eq!(success_percent(3, 4), 75.0);
        assert_eq!(per_second(100, Duration::ZERO), 0.0);
        assert_eq!(per_second(100, Duration::from_secs(4)), 25.0);
    }
}
