# celestia-types fuzzing

Fuzz targets for the `celestia-types` code that handles data received from peers. Errors are expected, panics are bugs.

Responses are verified against deterministic squares with original data widths of 1 to 16 (`src/lib.rs`). Their namespaces form runs that span rows, share rows and leave gaps, so that both presence and absence proofs get exercised. Besides panics, targets check soundness: whenever verification succeeds, the response must match the square exactly.

## Byte-level targets

These decode raw bytes the way `lumina-node` does. Shwap response inputs start with a byte selecting the square.

| Target | Mirrors |
| --- | --- |
| `extended_header` | `ExtendedHeader::decode_and_validate` in header-sub and header-ex |
| `shwap_ids` | shwap request id decoding, plus encode round trip |
| `sample` | `Sample` response decoding and verification |
| `row` | `Row` response decoding and verification |
| `namespace_data` | `NamespaceData` response decoding and verification |
| `eds` | `ExtendedDataSquare` response decoding and DAH check |
| `share_proof` | `ShareProof` from a bridge node, verified against the data root |
| `namespace` | `Namespace` of any length, from share proofs and from JSON |

## Structured targets

These use `arbitrary` to build a valid input and mutate it, so that fuzzing spends its time in verification instead of the protobuf decoder (`src/mutate.rs`). An empty input is a valid one, so they need no seeds.

| Target | Input |
| --- | --- |
| `sample_structured` | `Sample` with share, proof node, proof range and axis mutations |
| `row_structured` | `Row` sending either half, with share and half side mutations |
| `namespace_data_structured` | `NamespaceData` of any namespace, with row, share and proof mutations |
| `share_proof_structured` | `ShareProof` of a range of a namespace, with share, NMT proof and row proof mutations |
| `eds_raw_structured` | `ExtendedDataSquare` from a bridge node, with share and codec mutations |
| `header_verify` | Trusted header and a chain of untrusted ones signed with chosen validators and votes, run through `decode_and_validate`, `verify`, `verify_range` and `verify_adjacent_range` |

`header_verify` signs with fixed keys (`src/header.rs`) and checks that accepted headers carry the required voting power. `verify` rejects headers too far in the future, so a few inputs depend on the current time.

## Running

Requires a nightly toolchain and [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) (`cargo install cargo-fuzz`). From the repository root:

```sh
# seed the corpus of byte-level targets with valid encodings (optional, speeds up coverage)
cargo run --manifest-path types/fuzz/Cargo.toml --example seed_corpus

cargo +nightly fuzz list --fuzz-dir types/fuzz
cargo +nightly fuzz run --fuzz-dir types/fuzz sample_structured
```

`eds` needs `-- -max_len=131072` to reach the widest square.

Crashing inputs land in `types/fuzz/artifacts/<target>/`; reproduce one with `cargo +nightly fuzz run --fuzz-dir types/fuzz <target> <path>`, or print a structured one with `cargo +nightly fuzz fmt --fuzz-dir types/fuzz <target> <path>`.

## Coverage

Requires `rustup component add --toolchain nightly llvm-tools-preview`. From the repository root, replay a target's corpus and render an HTML report into `types/fuzz/coverage/<target>/html/`:

```sh
target=sample_structured
cargo +nightly fuzz coverage --fuzz-dir types/fuzz $target

host=$(rustc -vV | sed -n 's/host: //p')
llvm_bin=$(rustc +nightly --print sysroot)/lib/rustlib/$host/bin
$llvm_bin/llvm-cov show target/$host/coverage/$host/release/$target \
    -instr-profile=types/fuzz/coverage/$target/coverage.profdata \
    -format=html -output-dir=types/fuzz/coverage/$target/html \
    -ignore-filename-regex='/.cargo/registry|/rustc/'
```

Use `llvm-cov report` with the same arguments, minus the output ones, for a per-file summary in the terminal.

By default targets are built with overflow checks and debug assertions. Pass `-O` to match a release build.
