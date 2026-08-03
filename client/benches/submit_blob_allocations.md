# Blob submission allocation investigation

This document records the static and measured analysis for
[issue #862](https://github.com/celestiaorg/lumina/issues/862). The measurements
were taken at `d137b4d` with a local `ci` devnet and a 7 MiB
(7,340,032-byte) share-version-1 blob. Reported results are the component-wise
medians of three fresh-process runs.

## Result

The issue is not completely false, but its `~49 MiB allocated` headline is not
an accurate total-allocation estimate:

- several identified payload-sized clones are real;
- `Blob -> RawBlob` and `IntoGrpcParam<BroadcastTxRequest>` move their `Vec`
  payloads and do not allocate another payload-sized buffer;
- the estimate omits commitment construction/validation and tonic's gRPC
  encoding buffer;
- it conflates cumulative allocated bytes with simultaneously live bytes.

For the direct gRPC path, 49.01 MiB happens to be the measured peak of
allocations made inside the profiled operation. Its cumulative allocation is
108.45 MiB. Including the high-level state API raises those values to
56.01 MiB and 115.45 MiB. Constructing the `Blob` in the measured region raises
them to 63.01 MiB and 167.77 MiB.

The input buffers and warmed clients are deliberately created before each
profile starts, so the figures describe allocations performed by the operation
rather than total process RSS. DHAT also does not measure stack use.

## Static allocation ledger

| Site from the issue | Verdict | Reason |
| --- | --- | --- |
| caller `data.to_vec()` | real when the caller has a borrowed buffer | Creates the `Vec` required by `Blob::new`. |
| `StateApi::submit_pay_for_blob`: `blobs.to_vec()` | real | `Blob::Clone` deep-clones `data`. |
| `GrpcClient::submit_blobs`: `blobs.to_vec()` | real | Deep clone used to own data in `AsyncGrpcCall`. |
| `submit_blobs_impl`: `blobs.to_vec()` | real | Deep clone before the signing loop. |
| `Blob -> RawBlob` | not payload-sized | The consuming conversion moves `Blob::data`; only small namespace/signer vectors are new. |
| `blobs.clone()` in `RawBlobTx` | real | Deep-clones every `RawBlob::data` on each signing retry. |
| `RawBlobTx::encode_to_vec()` | real | Creates the serialized blob transaction. |
| `broadcast_tx_with_account`: `tx.clone()` | real | Keeps bytes for `BroadcastedTx` while broadcasting another copy. |
| `IntoGrpcParam<BroadcastTxRequest>` | not payload-sized | Moves `tx_bytes` into the request. |
| generated RPC/failover request clone | real | `BroadcastTxRequest::clone` deep-copies `tx_bytes` for every endpoint attempt. |

The static list omitted two dominant areas:

1. `Blob::new` computes a commitment, and `submit_blobs_impl` computes it again
   in `Blob::validate`. For 7 MiB, one commitment pass creates about 15,229
   shares and does tens of thousands of allocations.
2. Tonic starts its protobuf encoder with an 8 KiB `BytesMut`. Growing that
   buffer for the 7 MiB unary request allocated 21.01 MiB cumulatively in three
   blocks and left a 14.00 MiB block live at the measured peak.

Gas simulation serializes the small signed pay-for-blob transaction, not the
raw blob payload, so it is not another 7 MiB allocation.

## Benchmark

`submit_blob_allocations.rs` is a harness-free DHAT benchmark. Each invocation
profiles one operation in a fresh process and writes a call-site-attributed JSON
profile under `target/alloc-profiles`.

```sh
# No network: Blob::new only
cargo bench -p celestia-client --bench submit_blob_allocations \
  --features allocation-profiling -- \
  --phase construct --size 7MiB

# With `validator` and `node-0` from ci/docker-compose.yml running
cargo bench -p celestia-client --bench submit_blob_allocations \
  --features allocation-profiling -- \
  --phase grpc-submit --size 7MiB
cargo bench -p celestia-client --bench submit_blob_allocations \
  --features allocation-profiling -- \
  --phase client-submit --size 7MiB
cargo bench -p celestia-client --bench submit_blob_allocations \
  --features allocation-profiling -- \
  --phase full --size 7MiB

# Compare the additive owned-input APIs
cargo bench -p celestia-client --bench submit_blob_allocations \
  --features allocation-profiling -- \
  --phase grpc-submit-owned --size 7MiB
cargo bench -p celestia-client --bench submit_blob_allocations \
  --features allocation-profiling -- \
  --phase client-submit-owned --size 7MiB
cargo bench -p celestia-client --bench submit_blob_allocations \
  --features allocation-profiling -- \
  --phase full-owned --size 7MiB

# Exercise the shared-payload path
cargo bench -p celestia-client --bench submit_blob_allocations \
  --features allocation-profiling -- \
  --phase grpc-submit-bytes --size 7MiB
cargo bench -p celestia-client --bench submit_blob_allocations \
  --features allocation-profiling -- \
  --phase client-submit-bytes --size 7MiB

# Isolate transport request cloning and force exceptional paths
cargo bench -p celestia-client --bench submit_blob_allocations \
  --features allocation-profiling -- \
  --phase broadcast --scenario failover --size 7MiB
cargo bench -p celestia-client --bench submit_blob_allocations \
  --features allocation-profiling -- \
  --phase grpc-submit --scenario stale-sequence --size 7MiB
```

Run
`cargo bench -p celestia-client --bench submit_blob_allocations --features allocation-profiling -- --help`
for endpoint, key, output-directory, and size options.

### Baseline 7 MiB profiles

`Total` is cumulative allocator traffic during the profiled region. `Peak` is
the high-water mark of allocations from that region that were live together.
Counts are allocation blocks, not retained objects.

| Phase | Scenario | Total | Allocations | Peak | Live at peak |
| --- | --- | ---: | ---: | ---: | ---: |
| construct | happy | 52.31 MiB | 77,132 | 15.12 MiB | 138 |
| broadcast | happy | 28.08 MiB | 116 | 21.05 MiB | 53 |
| broadcast | one unavailable endpoint | 35.09 MiB | 152 | 21.05 MiB | 61 |
| direct gRPC submit | happy | 108.45 MiB | 77,476 | 49.01 MiB | 42 |
| state API submit | happy | 115.45 MiB | 77,480 | 56.01 MiB | 47 |
| construct + state API submit | happy | 167.77 MiB | 154,612 | 63.01 MiB | 48 |
| direct gRPC submit | stale sequence retry | 157.46 MiB | 77,493 | 49.01 MiB | 44 |

The failover attempt adds 7.02 MiB cumulatively without increasing the peak:
the first request is released before the second endpoint request is encoded.
A stale-sequence retry adds 49.01 MiB cumulatively and also leaves the peak
unchanged.

Construction scales with payload size:

| Payload | Total | Allocations | Peak |
| ---: | ---: | ---: | ---: |
| 1 KiB | 13.21 KiB | 23 | 6.10 KiB |
| 1 MiB | 9.17 MiB | 11,105 | 3.05 MiB |
| 7 MiB | 52.31 MiB | 77,132 | 15.12 MiB |

### Incremental optimization results

Each row is the median delta from the immediately preceding stage. Negative
values are improvements.

| Optimization | Affected measurement | Total bytes | Allocations | Peak bytes |
| --- | --- | ---: | ---: | ---: |
| exact share-vector preallocation | one commitment pass | -8,995,455 | -12 | -592,515 |
| exact share-vector preallocation | full construct + submit | -17,990,904 | -24 | +2 |
| stack-backed share construction | one commitment pass | -7,797,248 | -15,229 | 0 |
| stack-backed share construction | full construct + submit | -15,594,493 | -30,458 | +1 |
| retained raw blobs | direct gRPC submit | -7,340,112 | -4 | +440 |
| retained raw blobs | stale-sequence retry | -14,680,274 | -9 | +442 |
| private ownership flow | direct gRPC submit | -7,340,115 | -2 | -7,340,122 |
| private ownership flow | full construct + submit | -7,340,115 | -2 | -7,340,122 |
| exact tonic encode buffer | transport-only broadcast | -14,688,522 | -2 | -7,339,536 |
| exact tonic encode buffer | stale-sequence retry | -29,378,964 | -5 | -7,339,932 |
| root-only NMT accumulation | one commitment pass | -22,896,820 | -61,882 | -96,612 |
| root-only NMT accumulation | full construct + submit | -45,794,822 | -123,767 | -30 |
| owned delegation | borrowed full construct + submit | -7,340,160 | -2 | -7,340,160 |
| owned input | owned vs borrowed full path | -7,340,032 | -1 | -7,340,032 |
| shared transport payload | direct Bytes vs owned Blob | -14,681,028 | -1 | -14,680,948 |
| end-to-end Bytes input | client Bytes vs full owned Blob | -29,846,641 | -10 | -22,020,948 |
| shared retry payload | Bytes vs borrowed Blob stale retry | -36,701,922 | -2 | -22,020,984 |

### Final 7 MiB comparison

The borrowed methods retain their original signatures. The owned and Bytes
rows are additive APIs for callers that can transfer or share their input.

| Full high-level path | Total bytes | Allocations | Peak bytes |
| --- | ---: | ---: | ---: |
| baseline borrowed Blob | 175,917,618 | 154,612 | 66,073,361 |
| optimized borrowed Blob | 59,827,550 | 353 | 44,053,461 |
| optimized owned Blob | 52,487,518 | 352 | 36,713,429 |
| optimized Bytes | 22,640,877 | 342 | 14,692,481 |

The optimized borrowed path cuts cumulative allocation by 65.99%. The Bytes
path cuts it by 87.13%, cuts peak bytes by 77.76%, and reduces allocation count
by 99.779% relative to the baseline.

### Largest 7 MiB allocation sites

The direct gRPC submission profile attributes the dominant sites as follows.
The encoded blob transaction is 7,340,493 bytes, 461 bytes larger than the
payload in this run.

| Allocation site | Cumulative bytes | Blocks | Largest/live block |
| --- | ---: | ---: | ---: |
| tonic gRPC `BytesMut` growth | 21.01 MiB | 3 | 14.00 MiB |
| share vector growth during commitment validation | 16.03 MiB | 13 | 8.02 MiB |
| one 512-byte buffer per constructed share | 7.44 MiB | 15,229 | 512 B |
| NMT copy of every 512-byte leaf | 7.44 MiB | 15,229 | 512 B |
| each `Blob::data`/`RawBlob::data` deep clone | 7.00 MiB | 1 | 7.00 MiB |
| each encoded transaction/request deep copy | 7.00 MiB + 461 B | 1 | 7.00 MiB + 461 B |
| NMT leaf-vector growth | 3.43 MiB | 724 | 15.00 KiB |

## Candidate solutions

Recommended order:

1. **Remove avoidable ownership clones without changing public types.**
   Add owned variants such as
   `submit_pay_for_blob_owned(Vec<Blob>, TxConfig)` and
   `submit_blobs_owned(Vec<Blob>, TxConfig)`. Have the existing borrowed APIs
   clone once and delegate to an owned core. This removes two 7 MiB clones from
   the existing state path; callers able to transfer ownership can remove the
   remaining entry clone.
2. **Reuse the raw blob transaction across sequence retries.**
   Move converted raw blobs into a `RawBlobTx` before the retry loop and update
   only its small signed `tx` field. Prost encoding borrows the message, so
   `blobs.clone()` is unnecessary. This saves one payload copy per signing
   attempt, mainly cumulative traffic.
3. **Right-size tonic's encoding buffer.**
   The generated client uses tonic's default 8 KiB codec buffer, which grows to
   14 MiB. A specialized broadcast call with a codec initialized from
   `encoded_len + 5` should turn three allocations totaling 21 MiB into one
   roughly 7 MiB gRPC frame. The frame copy itself remains unless the transport
   is redesigned for scatter/gather output.
4. **Reduce commitment churn.**
   Preallocate the share vector with `shares_needed_for_blob`, construct each
   share in a stack `[u8; 512]`, and investigate a root-only NMT API that does
   not retain a heap copy of every leaf. Avoiding the second commitment pass
   needs a trusted/immutable blob representation because `Blob` fields are
   currently public and can be mutated after construction.
5. **Add a `Bytes` API only if bytes stay shared end to end.**
   An additive method accepting `bytes::Bytes` but immediately producing the
   current `Blob { data: Vec<u8> }` does not solve the problem. A useful version
   needs an internal Bytes-backed blob/raw-blob representation, a Bytes-backed
   broadcast request, and the owned submission core. Private wire-compatible
   prost messages can provide this without changing all generated public
   types. The final tonic protobuf frame is still one copy.
6. **Consider `Bytes` as the next breaking model.**
   Changing `Blob::data`, raw protobuf byte fields, `BroadcastedTx::tx`, and the
   v2 transaction worker from `Vec`/`Arc<Vec>` to `Bytes` makes clones cheap and
   gives the cleanest ownership model. It is Rust-source-breaking and requires
   explicit copy boundaries for serde, wasm-bindgen, and UniFFI consumers.

The experimental v2 transaction client does not currently remove these costs:
it deep-clones blobs while building `RawBlobTx` and converts
`Arc<Vec<u8>>` back to a `Vec` before calling the legacy broadcast path.
