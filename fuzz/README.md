# SurrealKV SSTable Fuzz Testing

This directory contains fuzz tests for SurrealKV's SSTable components using `libfuzzer-sys` and `arbitrary` for structured input generation.

## Prerequisites

Install `cargo-fuzz`:

```bash
cargo install cargo-fuzz
```

Ensure you have a nightly Rust toolchain:

```bash
rustup toolchain install nightly
```

## Fuzz Targets

### 1. `block_roundtrip`
Tests block write/read roundtrip consistency. Verifies that all entries written to a block can be read back correctly.

**Focus**: Encoding/decoding correctness, iterator completeness

### 2. `block_iterator`
Exercises block iterator operations (seek, next, prev) in various patterns.

**Focus**: Iterator state machine, edge cases (first/last entries)

### 3. `block_decode`
Tests robustness against corrupted/malformed block data.

**Focus**: Security, graceful error handling, no panics on invalid input

### 4. `index_roundtrip`
Tests partitioned index write/read cycle.

**Focus**: Partition creation, separator key handling, partition lookup

### 5. `index_lookup`
Focused testing of `find_block_handle_by_key()` binary search logic.

**Focus**: Binary search correctness, edge cases (single partition, empty partitions)

### 6. `table_roundtrip`
End-to-end SSTable write/read cycle.

**Focus**: Full table integrity, filter correctness, metadata

### 7. `table_iterator`
Tests table iterator with range bounds and bidirectional traversal.

**Focus**: Range filtering, direction tracking (no interleaving), ordering

### 8. `bloom_filter`
Tests bloom filter creation and lookup consistency.

**Focus**: No false negatives, reasonable false positive rate

## Running Fuzzers

Run a specific target:

```bash
cd fuzz
cargo +nightly fuzz run block_roundtrip
```

Run with memory limit and timeout:

```bash
cargo +nightly fuzz run block_roundtrip -- -max_len=65536 -timeout=10
```

Run multiple targets in parallel:

```bash
cargo +nightly fuzz run block_roundtrip &
cargo +nightly fuzz run index_roundtrip &
cargo +nightly fuzz run table_roundtrip &
wait
```

## Reproducing Crashes

If a fuzzer finds a crash, it will save the input to `fuzz/artifacts/<target>/crash-<hash>`.

Reproduce the crash:

```bash
cargo +nightly fuzz run block_roundtrip fuzz/artifacts/block_roundtrip/crash-<hash>
```

## Important Notes

1. **Key Ordering**: All fuzz targets automatically sort and deduplicate entries to ensure ascending key order (required by `BlockWriter`).

2. **No Interleaving**: The double-ended iterator doesn't support interleaving `next()` and `next_back()`. Fuzz targets track direction to avoid this.

3. **Memory Limits**: Input sizes are limited to prevent excessive memory usage during fuzzing.

4. **Deterministic Ordering**: Entries are sorted using the same `InternalKeyComparator` used by the actual SSTable implementation.

## Priority Order

1. **block_roundtrip** - Core data structure, most likely to have encoding bugs
2. **block_decode** - Security-critical, prevents crashes on corrupt data
3. **table_roundtrip** - End-to-end correctness
4. **index_lookup** - Critical for correct partition selection
5. **bloom_filter** - Simple but important for no false negatives
6. **block_iterator** - Finds state machine bugs
7. **table_iterator** - Complex two-level iteration
8. **index_roundtrip** - Integration of index components

