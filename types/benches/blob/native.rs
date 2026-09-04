//! Wall-time benchmarks for the CPU-bound part of blob submission.
//!
//! A rollup that submits data to Celestia builds a [`Blob`] from its payload
//! (`Blob::new`: split into shares + share commitment) and the gRPC client
//! validates it again before signing (`Blob::validate`). Both run on the
//! caller's thread, so their cost is paid on every submission. The two smaller
//! groups isolate the halves of that work: `blob_to_shares` (share splitting)
//! and `commitment_from_shares` (subtree roots + merkle mountain range).
//!
//! Payloads use share version 1 (with a signer), which is what
//! `Blob::new(_, _, Some(address))` produces for pay-for-blob transactions.
//! Throughput is reported in payload bytes per second.
//!
//! To compare two revisions, save a baseline before the change and compare
//! after:
//!
//! ```sh
//! cargo bench -p celestia-types --bench blob -- --save-baseline main
//! # ...apply the change...
//! cargo bench -p celestia-types --bench blob -- --baseline main
//! ```
//!
//! A fast sanity pass (each case runs once) is `-- --test`.

use std::hint::black_box;
use std::time::Duration;

use celestia_types::consts::appconsts;
use celestia_types::nmt::Namespace;
use celestia_types::state::{AccAddress, Id};
use celestia_types::{Blob, Commitment};
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group,
    measurement::WallTime,
};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};

const DATA_SEED: u64 = 0x862;
const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

/// Payload sizes: a small blob, a typical rollup batch, and the largest blob
/// that fits a Celestia block on mainnet.
const SIZES: &[(&str, usize)] = &[("64KiB", 64 * KIB), ("1MiB", MIB), ("7MiB", 7 * MIB)];

fn payload(size: usize) -> Vec<u8> {
    let mut rng = StdRng::seed_from_u64(DATA_SEED);
    let mut data = vec![0u8; size];
    rng.fill_bytes(&mut data);
    data
}

fn namespace() -> Namespace {
    Namespace::new_v0(b"blobbench").expect("static namespace is valid")
}

fn signer() -> AccAddress {
    AccAddress::new(Id::new([0x42; appconsts::SIGNER_SIZE]))
}

fn configure(group: &mut BenchmarkGroup<'_, WallTime>, size: usize) {
    group.throughput(Throughput::Bytes(size as u64));
    if size >= 4 * MIB {
        group
            .sampling_mode(SamplingMode::Flat)
            .sample_size(30)
            .warm_up_time(Duration::from_secs(3))
            .measurement_time(Duration::from_secs(20));
    } else {
        group
            .sampling_mode(SamplingMode::Linear)
            .sample_size(50)
            .warm_up_time(Duration::from_secs(3))
            .measurement_time(Duration::from_secs(10));
    }
}

fn bench_blob_new(c: &mut Criterion) {
    let mut group = c.benchmark_group("blob_new");
    let namespace = namespace();
    let signer = signer();

    for &(name, size) in SIZES {
        configure(&mut group, size);
        let data = payload(size);

        group.bench_with_input(BenchmarkId::from_parameter(name), &data, |b, data| {
            b.iter_batched(
                || data.clone(),
                |data| Blob::new(namespace, data, Some(signer)).unwrap(),
                criterion::BatchSize::LargeInput,
            )
        });
    }

    group.finish();
}

fn bench_blob_validate(c: &mut Criterion) {
    let mut group = c.benchmark_group("blob_validate");
    let namespace = namespace();
    let signer = signer();

    for &(name, size) in SIZES {
        configure(&mut group, size);
        let blob = Blob::new(namespace, payload(size), Some(signer)).unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(name), &blob, |b, blob| {
            b.iter(|| black_box(blob).validate().unwrap())
        });
    }

    group.finish();
}

fn bench_blob_to_shares(c: &mut Criterion) {
    let mut group = c.benchmark_group("blob_to_shares");
    let namespace = namespace();
    let signer = signer();

    for &(name, size) in SIZES {
        configure(&mut group, size);
        let blob = Blob::new(namespace, payload(size), Some(signer)).unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(name), &blob, |b, blob| {
            b.iter(|| black_box(blob).to_shares().unwrap())
        });
    }

    group.finish();
}

fn bench_commitment_from_shares(c: &mut Criterion) {
    let mut group = c.benchmark_group("commitment_from_shares");
    let namespace = namespace();
    let signer = signer();

    for &(name, size) in SIZES {
        configure(&mut group, size);
        let shares = Blob::new(namespace, payload(size), Some(signer))
            .unwrap()
            .to_shares()
            .unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(name), &shares, |b, shares| {
            b.iter(|| Commitment::from_shares(namespace, black_box(shares)).unwrap())
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_blob_new,
    bench_blob_validate,
    bench_blob_to_shares,
    bench_commitment_from_shares
);

pub(super) fn main() {
    benches();
    Criterion::default().configure_from_args().final_summary();
}
