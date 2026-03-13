# Fibre Package Fix Plan

## Overview
Fix all issues identified in the code review + add cancellation token support to upload/download operations.

## Changes

### 1. Add `Cancelled` error variant to `FibreError` (error.rs)
- Add `Cancelled` variant to `FibreError` enum for when operations are cancelled via token.

### 2. Replace `AtomicBool` with `CancellationToken` in `FibreClient` (client.rs)
- Replace `closed: AtomicBool` with `cancel_token: CancellationToken` (using `tokio_util::sync::CancellationToken`).
- `close()` calls `cancel_token.cancel()`.
- `is_closed()` calls `cancel_token.is_cancelled()`.
- Add `cancellation_token()` accessor so callers can obtain a child token or listen for cancellation externally.
- Update `FibreClientBuilder` accordingly.
- Add `tokio-util` dependency to `fibre/Cargo.toml`.

### 3. Thread cancellation token through `upload` (upload.rs)
- `upload()` and `put()`: replace the `closed.load(SeqCst)` check with `cancel_token.is_cancelled()` and add a `cancel_token.cancelled()` branch in the `tokio::select!` in `upload_shards`.
- Inside `upload_shards`, add a `cancelled()` branch to the `tokio::select!` that waits for either threshold-met, all-tasks-complete, or cancellation.
- On cancellation, return `FibreError::Cancelled`.

### 4. Thread cancellation token through `download` (download.rs)
- `download()` and `download_with_config()`: replace `closed.load(SeqCst)` checks with `cancel_token.is_cancelled()`.
- Inside `download_blob`, add a `cancelled()` branch to the `tokio::select!` loop.
- On cancellation, return `FibreError::Cancelled`.

### 5. Extract `spawn_task` to a shared module (lib.rs + new util)
- Create a private `task.rs` module with the shared `spawn_task` function.
- Remove the duplicated `spawn_task` from both `upload.rs` and `download.rs`.
- Import from the shared module in both files.

### 6. Fix duplicate voting power in `SignatureSet::add` (signature_set.rs)
- Before incrementing `voting_power`, check if the validator already has a signature in the map.
- If the validator already signed, skip the voting power increment (idempotent behavior).
- Update the `duplicate_add_accumulates` test to reflect the fixed behavior (now idempotent — no double-count).

### 7. Remove dead code `v0_with_test_params` (config.rs)
- Delete the unused `BlobConfig::v0_with_test_params` method that triggers a compiler warning.

### 8. Change `#[allow(dead_code)]` to `#[cfg(test)]` (payment_promise.rs, proto_conv.rs)
- `unmarshal_binary_time` in `payment_promise.rs`: change `#[allow(dead_code)]` to `#[cfg(test)]` since it's only used in tests.
- `timestamp_to_system_time` in `proto_conv.rs`: change `#[allow(dead_code)]` to `#[cfg(test)]` since it's only used in tests.

### 9. Deduplicate `row_size` logic (config.rs)
- Extract the row-size rounding logic into a standalone `fn compute_row_size(total_len, rows, min_row_size) -> usize`.
- Have both `ProtocolParams::row_size()` and `BlobConfig::row_size()` delegate to it.

### 10. Update all tests
- Update existing tests that reference `closed`, `AtomicBool`, or the old cancellation pattern.
- Add tests for cancellation token behavior (cancel during upload, cancel during download).
- Fix the `duplicate_add_accumulates` test in signature_set.rs.
