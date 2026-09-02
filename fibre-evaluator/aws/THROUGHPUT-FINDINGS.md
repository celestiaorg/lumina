# Fibre throughput on 2× c6in.32xlarge — findings & handoff

Date: 2026-09-02. Goal: 2.2 GiB/s paid (= 75.6 Gbps encoded on the wire, since upload sends 4× paid).
Hosts: client 54.161.45.179 (priv 172.31.44.238), node/mock 100.24.16.122 (priv 172.31.45.200).
Both c6in.32xlarge (128 vCPU Intel 8375C Ice Lake, 256 GiB), placement group `fibre-eval`, same subnet.
Runs were orchestrated in tmux on each host. The mock advertises `172.31.45.200`.

## Current status: target achieved at 2.29–2.31 GiB/s paid with 16 evaluator binaries

The highest completed run delivered 996 verified 128 MiB blobs in 54 seconds, or 2.306 GiB/s paid. An exact repeat delivered 989 in 54 seconds, or 2.289 GiB/s. Both had zero lifecycle failures. This is the highest stable configuration tested, not a proof that no narrower parameter combination can go higher.

## Latest branch continuation — 2026-09-02

Both hosts ran commit `96055420ad6227747f7a137adf9d19878211093f` from `nikolai/fibre-throughput-eval-with-improvement`. Each host received the branch with `git pull --ff-only`. The evaluator and mock were built after `cargo clean` with `RUSTFLAGS="-C target-cpu=native"` and `cargo build --locked --release` against their standalone manifests.

All valid continuation runs used 32 mock validators, `--no-store`, a 134217728-byte paid blob, `--skip-download`, `--gas-limit 200000`, `--gas-price 0.002`, and unique deterministic mock-only keys. Aggregate paid throughput is `sum(verified) * 0.125 GiB / maximum process elapsed`, not the sum of independently rounded per-process rates. Active wire averages include `ens5` samples with at least 100000 KiB/s transmitted.

| Evaluators | Rayon workers each | Slots each / aggregate | Verified / shared elapsed | Paid GiB/s | Active wire avg / peak | Client CPU avg / peak | Client peak used memory |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | default | 32 / 32 | 155 / 62 s | 0.308 | 7.89 / 9.19 Gbps | 26.1 cores average | not retained |
| 8 | 8 | 16 / 128 | 786 / 53 s | 1.854 | 46.52 / 61.36 Gbps | 37.4 / 48.0 cores | 218.8 GiB |
| 16 | 4 | 8 / 128 | 996 / 54 s | 2.306 | 59.42 / 70.82 Gbps | 65.0 / 68.0 cores | 115.2 GiB |
| 16 repeat | 4 | 8 / 128 | 989 / 54 s | 2.289 | 59.06 / 68.39 Gbps | 63.7 / 70.2 cores | 116.6 GiB |
| 16, invalid | 6 | 8 / 128 | no final report | invalid | invalid | 79.8 / 86.0 cores before death | 243.8 GiB before OOM |

The stable 16-process command used `RAYON_NUM_THREADS=4`, `--blobs-per-second 3`, `--encode-concurrency 1`, `--max-in-flight 8`, `--queue-capacity 4`, and `--run-for-seconds 45` per binary. Each binary ran in its own tmux window with one unique private key. Raw client logs are `/home/ubuntu/bench-results/improvement-16-p*.log` and `/home/ubuntu/bench-results/improvement-16r4-repeat-p*.log`; matching `sar` logs use the same prefixes. Validator repeat samples are under `/home/ubuntu/bench-results/improvement-16r4-repeat-validator-*`.

The native mock command was:

```bash
RUST_LOG=info /home/ubuntu/workspace/lumina/fibre-mock-server/target/release/fibre-mock-server --validators 32 --listen 0.0.0.0 --advertise 172.31.45.200 --base-port 19000 --chain-id mock-1 --no-store
```

Each of the 16 evaluator tmux windows ran the following command with a different mock-only key:

```bash
RAYON_NUM_THREADS=4 RUST_LOG=info /home/ubuntu/workspace/lumina/fibre-evaluator/target/release/fibre-evaluator --chain-id mock-1 --app-grpc-url http://172.31.45.200:19000 --core-grpc-url http://172.31.45.200:19000 --private-key KEY --namespace fibre-eval --blob-size 134217728 --blobs-per-second 3 --encode-concurrency 1 --max-in-flight 8 --queue-capacity 4 --skip-download --gas-limit 200000 --gas-price 0.002 --run-for-seconds 45 --stats-interval-seconds 10
```

The six-Rayon-worker run is not a throughput result. All 16 evaluators stopped around 20 seconds without a final report, and the kernel recorded a global OOM kill of `fibre-evaluator`. One killed process had about 18.6 GiB anonymous RSS. Raising encode parallelism lets more large encoded matrices coexist and exhausts memory before CPU or the mock is saturated.

### Hypotheses resolved

- **Tokio is a bottleneck.** The evaluator creates one OS thread per key at [`src/main.rs:198`](../src/main.rs#L198), but each thread runs a Tokio current-thread runtime at [`src/main.rs:315`](../src/main.rs#L315). In the earlier exact one-client A/B, current-thread delivered 0.184 GiB/s while a temporary four-worker runtime delivered 0.456 GiB/s, 2.48× higher. The current-thread `fibre-client-1` thread averaged 99.36% CPU. Running independent binaries is a valid workaround and is what moved the stable result above 2.2 GiB/s.
- **The latest encode work is material, but `target-cpu=native` alone was not.** On the old code, a fully clean native build delivered 0.169 GiB/s versus 0.184 GiB/s baseline while encode p50 improved only from 1369 ms to 1185 ms. The latest branch plus a clean native build reduced the exact control's encode p50 to 271 ms and raised paid throughput to 0.308 GiB/s. This combined run does not isolate native from the branch changes. The branch parallelizes parity stripes through Rayon at [`rsema1d/src/codec/rs.rs:185`](../../rsema1d/src/codec/rs.rs#L185).
- **The mock is not the stable-run limit.** During the 2.289 GiB/s repeat, the mock averaged 8.8 cores and peaked at 10.7 on a 128-vCPU host; validator-host used memory peaked at 42.9 GiB. Client memory and encode scheduling are the tighter constraints.
- **The current useful operating point is 16 runtimes, four Rayon workers each, and 128 aggregate lifecycle slots.** Eight binaries at the same aggregate slot count left runtime parallelism unused and reached only 1.854 GiB/s. Six Rayon workers per binary increased live encoded data enough to OOM. Four workers repeated at 2.289–2.306 GiB/s without failures.

### Parameter sanity check

- `--max-in-flight` bounds the entire lifecycle through the dispatcher semaphore at [`src/main.rs:543`](../src/main.rs#L543) and [`src/main.rs:549`](../src/main.rs#L549).
- `--encode-concurrency` is a separate per-client semaphore at [`src/main.rs:412`](../src/main.rs#L412) and is acquired before `spawn_blocking` at [`src/main.rs:619`](../src/main.rs#L619). Payload allocation and `Blob::new_owned` happen only after that permit at [`src/main.rs:627`](../src/main.rs#L627), which explains why jobs waiting for encode do not all allocate a matrix immediately.
- The scheduler applies `--blobs-per-second` independently inside every client runtime at [`src/main.rs:418`](../src/main.rs#L418) and [`src/main.rs:431`](../src/main.rs#L431). With one key per binary, the configured offered load is per binary.
- Fibre's default upload semaphore is 100 tasks per client at [`fibre/src/domain/config.rs:192`](../../fibre/src/domain/config.rs#L192) and [`fibre/src/domain/config.rs:207`](../../fibre/src/domain/config.rs#L207). With 32 validators, that semaphore is not the per-client cap in these runs.
- The protocol defaults are 4096 original rows, a 0.25 original-to-total encoding ratio, and a 128 MiB maximum paid blob at [`fibre/src/domain/config.rs:56`](../../fibre/src/domain/config.rs#L56). The maximum blob size is therefore the right choice for minimizing per-lifecycle overhead while measuring paid-byte throughput.

### Important metric caveat

The evaluator reports success after Fibre reaches the validator signature threshold and the payment is confirmed. `FibreClient::upload` explicitly returns at the threshold while other validator uploads continue in background at [`fibre/src/client/upload.rs:83`](../../fibre/src/client/upload.rs#L83) and [`fibre/src/client/upload.rs:206`](../../fibre/src/client/upload.rs#L206). Those background uploads are detached Tokio tasks at [`fibre/src/client/task.rs:23`](../../fibre/src/client/task.rs#L23). Therefore 2.29–2.31 GiB/s is confirmed paid throughput, but it does not prove that all 4× encoded bytes reached every validator before the evaluator processes exited. The 59 Gbps measured wire average, below the 75.6 Gbps full-fanout equivalent of 2.2 GiB/s paid, demonstrates the distinction.

## Earlier measurements (historical; continuation above supersedes contradictions)

The remainder is the prior handoff preserved for context. It describes the older branch and is not evidence for the currently deployed branch where it conflicts with the continuation above.

### Earlier measured facts

- **iperf3: 104 Gbps** (8 flows) between the hosts. Wire is wide open.
- **Single h2 connection ceiling** (1-validator mock, 1 client): **~3.1 Gbps avg, 8.5 Gbps peak.**
- **App plateau: ~12 Gbps avg / ~18–20 Gbps peak** regardless of config.
- **Node (mock) CPU: 96–98% idle** during every run. The mock is NOT the bottleneck.
- **Client CPU: only ~4 of 128 cores busy** at the ~18 Gbps plateau. ← central mystery.
- **3 independent clients (3 keys) × 32 validators = 96 connections → SAME ~18 Gbps.** More connections did NOT break the wall. So the ceiling is NOT per-connection count alone.
- **Not compiled native.** `RUSTFLAGS` unset; `.cargo/config.toml` only has the wasm getrandom cfg. rsema1d GF128 encode runs on baseline SSE2 (no AVX-512/GFNI) on an Ice Lake box that has both.
- **Encode p50 ≈ 1.2–1.46 s / 128 MiB ≈ 88–105 MiB/s/core** (non-native).

### What was changed and already deployed on the older branch

- Mock `fibre-mock-server/src/lib.rs`: h2 windows 16/64 MiB + `http2_adaptive_window(true)`, byte-based `--store-budget-gib` (default 16). Both hosts have the rebuilt binary.
- Client `fibre/src/transport/tls.rs:225`: `adaptive_window(true)` + `max_send_buf_size(32 MiB)` on the h2 client builder. This cut upload p50 13.6s→9.3s (+30% throughput) — flow control WAS a real partial cause, now mitigated.
- Evaluator multi-key (`--private-key` repeats; one client/thread per key; `--blobs-per-second` is PER-CLIENT). Local `fibre-evaluator/src/main.rs`, synced+built on client.

### Earlier handoff question (resolved by the continuation above)

**Why does the client cap at ~18 Gbps using only ~4 cores, with the server idle and 104 Gbps available?**
Only ~4 busy cores means the client is BLOCKED, not compute-bound — so it is neither encode-CPU-bound (would show ~40 cores) nor wire-bound. Prime suspects, in order:

1. **Per-client tokio runtime / thread model.** Each `--private-key` client runs on its own OS thread (`run_client_thread`, main.rs:~206). Check how many worker threads each client's runtime gets and whether uploads actually spread across cores, or funnel through one runtime/one poller. This is the most likely cause of "only 4 cores busy."
2. **A global serialization point in the upload path** (allocator contention on 512 MiB encoded buffers, or a shared lock). Profile the client under load (`perf top`, `tokio-console`, or flamegraph).
3. **Encode feeding** — but encode shows only ~4 cores too, so encode is starved, not saturating. Still, fix native first (below) so it stops mattering.

### Earlier encode task (resolved by the latest branch run)

- **Rebuild fully native, it did not take.** `RUSTFLAGS="-C target-cpu=native" cargo build --release` only recompiled the final crate; **rsema1d (the GF128 hot path) stayed non-native** (verified: its `.rlib` was not rebuilt). Force it:
  `cd fibre-evaluator && RUSTFLAGS="-C target-cpu=native" cargo clean && RUSTFLAGS="-C target-cpu=native" cargo build --release`
  (or set `[build] rustflags=["-C","target-cpu=native"]` in `.cargo/config.toml`). Verify rsema1d rebuilt (its `.rlib` timestamp) and that the encode path emits `gf2p8*`/`vpclmul`/AVX-512.
- **Micro-bench target:** `rsema1d/benches/codec_bench.rs` (criterion, `Throughput::Bytes`, has a `128MB_k8192_n24576` config). Compare encode MiB/s native vs non-native on this box. Expectation: several-× from GFNI/AVX-512.
- Then re-measure evaluator `encode` p50; it should drop well below 1s and stop being a co-limit.

### Earlier mock storage A/B

Added `fibre-mock-server --no-store` (discard shards after signing; downloads then error). Same 3-key/32-validator/`--skip-download` config, back to back:
- **store:** 0.409 GiB/s paid, mock RSS **94.6 GiB**.
- **--no-store:** 0.371 GiB/s paid (within run noise, actually a hair lower), mock RSS **7.8 GiB**.

Throughput unchanged → the ~0.4 GiB/s ceiling is **entirely client-side**, not the mock. `--no-store`'s real value is memory: 94.6 → 7.8 GiB, zero OOM risk, `--store-budget-gib` irrelevant. Use `--no-store` for all pure upload-throughput runs.

### Earlier encode agent brief

### What "encode" is (per blob, in `Blob::new_owned` → rsema1d `encode()`)
Three sub-steps, all on ONE core per blob:
1. **RS parity** — `codec/rs.rs` `fill_parity`: `reed_solomon_simd::DefaultEngine::new()` + `HighRateEncoder`, run sequentially. For a 128 MiB v0 blob at ratio 0.25: K=4096, N=12288, total 16384 rows × row_size 32768 = 512 MiB produced.
2. **Merkle commitment** — `crypto/hash.rs` uses `sha2::Sha256`; hashes all extended rows (~512 MiB of SHA-256) + tree.
3. **RLC** — `field/mod.rs:61` GF(2^16) multiply via reed-solomon-simd log/exp **scalar tables** over the rows.

### CPU: 8375C (Ice Lake-SP), verified flags on the client host
`avx2 avx512f avx512bw gfni vaes vpclmulqdq sha_ni` = ALL YES. (`sha` name shows as `sha_ni`.)

### What to expect from `target-cpu=native` — TEMPER EXPECTATIONS
The two heaviest kernels ALREADY use SIMD via **runtime** dispatch, independent of target-cpu:
- RS parity: `reed-solomon-simd` 3.1 `DefaultEngine` picks **AVX2 at runtime** (the crate has **no AVX-512 engine**). Native will NOT upgrade this.
- SHA-256 Merkle: `sha2` crate uses **SHA-NI at runtime** (present here). Native will NOT change this.
- So native only helps the **scalar glue**: the RLC GF(2^16) table-mul (step 3), symbol packing, and copies. Best candidate for a real win is the **RLC** — it is scalar; autovectorization (native) or a **GFNI-based GF multiply** (the box has GFNI) could speed it materially.
- **Net: measure, don't assume.** Likely 5–20% overall from native, with the RLC step the part that can move. My earlier "several-×" framing was wrong — the RS+SHA kernels are already accelerated.

### Micro-bench (this is the deliverable)
`rsema1d/benches/codec_bench.rs` (criterion, `Throughput::Bytes`). Run:
```
cd rsema1d && cargo bench                                  # baseline
RUSTFLAGS="-C target-cpu=native" cargo bench               # native
```
Configs: `128MB_k4096_n12288` (= fibre's ratio 0.25, the relevant one) and `128MB_k8192_n24576`. Groups `encode` and `encode_in_place`. Also a Go comparison: `rsema1d/go/cmd/bench` + `rsema1d/scripts/run_benchmarks.sh` (env `RUN_RUST_BENCH`/`RUN_GO_BENCH`).
NOTE: `RUSTFLAGS=native cargo build` earlier did NOT rebuild rsema1d (dep stayed non-native). For benches this is fine (bench recompiles rsema1d), but for the evaluator do a clean/forced rebuild and verify rsema1d's `.rlib` timestamp updates.

### Core budget (the structural number)
Encode is **single-threaded per blob** at ~90–100 MiB/s/core (paid) non-native. To sustain 2.2 GiB/s paid you need **~23–25 cores continuously encoding** (of 128). Two ways to get there:
- Raise `--encode-concurrency` so ≥~24 blobs encode in parallel (simplest), OR
- **Parallelize encode WITHIN a blob** (chunk RS + Merkle across cores) — cuts per-blob latency and the core count; bigger change.

### Coordination caveat (important)
In the real EC2 runs only **~4 of 128 cores were busy** — encode is currently **starved by the upstream client bottleneck**, not saturating. So encode native/parallelism only changes end-to-end throughput AFTER the "why only 4 cores busy" client issue (see above) is fixed. The bench work can proceed independently in isolation; just don't expect the evaluator's end-to-end number to move from encode changes until the client block is cleared.

### Earlier repro commands

Mock (node tmux), 32 validators:
```
RUST_LOG=info ./target/release/fibre-mock-server --validators 32 --listen 0.0.0.0 \
  --advertise 172.31.45.200 --base-port 19000 --chain-id mock-1 --store-budget-gib 80
```
Evaluator (client tmux), 3 keys — RUST_LOG=info is REQUIRED (warn hides the final report):
```
RUST_LOG=info ./target/release/fibre-evaluator --chain-id mock-1 \
  --app-grpc-url http://172.31.45.200:19000 --core-grpc-url http://172.31.45.200:19000 \
  --private-key 01..01 --private-key 02..02 --private-key 03..03 \
  --namespace fibre-eval --blob-size 134217728 --blobs-per-second 10 \
  --encode-concurrency 16 --max-in-flight 32 --queue-capacity 24 \
  --skip-download --run-for-seconds 70
```
Wire sampling: `sar -n DEV 1 90`; TX Gbps = txkB/s column ($7) ×8/1e6.
Note: `--skip-download` grows client RES unbounded (detached upload backlog) — watch `free -g`.
```
