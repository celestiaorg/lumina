# celestia-types fuzzing

Fuzz targets for the `celestia-types` decoders that handle bytes received from peers. Each target mirrors a decode path in `lumina-node`; errors are expected, panics are bugs.

| Target | Mirrors |
| --- | --- |
| `extended_header` | `ExtendedHeader::decode_and_validate` in header-sub and header-ex |
| `shwap_ids` | shwap request id decoding, plus encode round trip |
| `sample` | `Sample` response decoding and verification |
| `row` | `Row` response decoding and verification |
| `namespace_data` | `NamespaceData` response decoding and verification |
| `eds` | `ExtendedDataSquare` response decoding and DAH check |

Shwap responses are verified against a fixed `DataAvailabilityHeader` from `src/lib.rs`.

## Running

Requires a nightly toolchain and [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) (`cargo install cargo-fuzz`). From the repository root:

```sh
# seed the corpus with valid encodings (optional, speeds up coverage)
cargo run --manifest-path types/fuzz/Cargo.toml --example seed_corpus

cargo +nightly fuzz list --fuzz-dir types/fuzz
cargo +nightly fuzz run --fuzz-dir types/fuzz sample
```

Crashing inputs land in `types/fuzz/artifacts/<target>/`; reproduce one with `cargo +nightly fuzz run --fuzz-dir types/fuzz <target> <path>`.

By default targets are built with overflow checks and debug assertions. Pass `-O` to match a release build.
