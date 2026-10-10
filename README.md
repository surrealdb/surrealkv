<br>

<p align="center">
    <a href="https://surrealdb.com#gh-dark-mode-only" target="_blank">
        <img width="200" src="/img/white/logo.svg" alt="SurrealKV Logo">
    </a>
    <a href="https://surrealdb.com#gh-light-mode-only" target="_blank">
        <img width="200" src="/img/black/logo.svg" alt="SurrealKV Logo">
    </a>
</p>

<p align="center">An embedded key-value storage engine with an async commit pipeline.</p>

<br>

<p align="center">
	<a href="https://docs.rs/surrealkv/"><img src="https://img.shields.io/docsrs/surrealkv?style=flat-square"></a>
	&nbsp;
	<a href="https://crates.io/crates/surrealkv"><img src="https://img.shields.io/crates/v/surrealkv?style=flat-square"></a>
	&nbsp;
	<a href="https://github.com/surrealdb/surrealkv"><img src="https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square"></a>
</p>

SurrealKV is a high-performance, embedded key-value storage engine built on a modern Log-Structured Merge (LSM) tree architecture. Commits go through an asynchronous commit pipeline; point reads and iteration are synchronous, and `get_async` is available as an opt-in asynchronous point read.

It is designed as an independent, standalone key-value store suitable for high-throughput server workloads, embedded systems, and desktop applications. Storage primitives for browser WebAssembly over the Origin Private File System (OPFS) are included, but are not yet wired into `Tree`.

---

## Performance

> **The write figures are provisional.** The Create, Update and Delete numbers below were measured before a fix to WAL group commit and are being re-measured. The memory figures come from the same benchmark run and may change as well. Write throughput also depends on durability: the default is `Durability::Eventual`, where a commit does not wait for an fsync, while these numbers were produced with `--sync`.

Benchmarked on bare metal (**AMD Ryzen Threadripper 9970X 32-Core / 64-Thread Processor @ 5.48 GHz, 128 GB DDR5 RAM, 4TB PCIe 4.0 NVMe SSD**, 500,000 keys across 48 concurrent worker threads with 128 clients, synchronous durability enabled with `--sync` via [`crud-bench`](https://github.com/surrealdb/crud-bench)):

| Engine | Point&nbsp;Read&nbsp;(OPS) | Create&nbsp;(OPS) | Update&nbsp;(OPS) | Delete&nbsp;(OPS) | Scan&nbsp;(OPS) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **SurrealKV** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**6,748,647** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**219,545** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**199,730** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**253,717** | 438 |
| RocksDB | 864,757 | 35,183 | 36,764 | 37,291 | 145 |
| SlateDB | 88,566 | 8,276 | 8,145 | 8,217 | 81 |
| Fjall | 1,022,000 | 1,064 | 1,145 | 898 | 43 |
| ReDB | 333,229 | 1,289 | 1,387 | 1,380 | 243 |
| LMDB | 1,269,373 | 694 | 701 | 703 | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**1,007** |
| Libmdbx | 627,955 | 706 | 712 | 674 | 925 |

- **Creates (Writes)**: **219,545 OPS** (**6.24× faster** than RocksDB, **26.5× faster** than SlateDB, **170× faster** than ReDB, and **206× faster** than Fjall) in the `--sync` run (provisional, see above).
- **Updates**: **199,730 OPS** (**5.43× faster** than RocksDB, **24.5× faster** than SlateDB, **144× faster** than ReDB, and **174× faster** than Fjall) in the `--sync` run (provisional, see above).
- **Point Reads**: **6.75M OPS** sustained (**5.32× faster** than LMDB, **7.80× faster** than RocksDB, **20.3× faster** than ReDB, and **76.2× faster** than SlateDB).
- **Deletes**: **253,717 OPS** (**6.80× faster** than RocksDB, **30.9× faster** than SlateDB, **184× faster** than ReDB, and **283× faster** than Fjall), also provisional.
- **Range Scans**: **1.80× faster** than ReDB, **3.01× faster** than RocksDB, **5.42× faster** than SlateDB, and **10.3× faster** than Fjall via zero-copy iterator merges and block-level restart points. LMDB (**1,007 OPS**) and Libmdbx (**925 OPS**) are faster at scans than SurrealKV (**438 OPS**), by **2.30×** and **2.11×**.

### Memory Profile

Resting and peak memory are the lowest and highest process memory sampled across the whole benchmark run.

| Engine | Resting&nbsp;Memory | Peak&nbsp;Memory |
| :--- | ---: | ---: |
| **SurrealKV** | ~611 MB | 3.13 GB |
| RocksDB | ~741 MB | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**1.22 GB** |
| SlateDB | ~406 MB | 3.41 GB |
| Fjall | ~935 MB | 2.34 GB |
| ReDB | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**~367 MB** | 3.03 GB |
| LMDB | ~1.06 GB | 1.38 GB |
| Libmdbx | ~1.01 GB | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**1.22 GB** |

- **Resting Footprint**: **~611 MB** at rest, the third-lowest of the engines tested, after ReDB (**~367 MB**) and SlateDB (**~406 MB**).
- **Dynamic Scaling**: Peak memory expands dynamically under high concurrency to buffer active batches during parallel commits, then contracts back to resting baseline.

---

## Features

- **Async Commit Pipeline**: Concurrent commit pipeline built on lock-free ring buffers and optimistic concurrency control (OCC), awaited asynchronously. Point reads and iteration are synchronous; `get_async` is an opt-in asynchronous point read.
- **Snapshot Isolation**: Multi-version concurrency control with non-blocking concurrent reads and isolated read transactions.
- **Origin Private File System (OPFS) Primitives**: `LogStore` and `ObjectStore` implementations over `FileSystemSyncAccessHandle` for Web Workers (`surrealkv::storage::opfs`, `wasm32` only). They are not yet wired into `Tree`, so a `Tree` does not persist to OPFS yet.
- **Automated Zero-Effort Migration**: Automatically detects and migrates legacy databases on startup in pure Rust:
  - RocksDB BlockBasedTable formats (v2 through v7) via `surrealkv-compat-rocksdb`.
  - SurrealKV V1 stores via `surrealkv-compat-v1`.
  - Browser IndexedDB (`indxdb://`) data from an exported IndexedDB dump via `surrealkv-compat-indxdb`.
- **Value Log Separation (WiscKey)**: Opt-in via `with_enable_vlog(true)`. Values larger than `vlog_value_threshold` (1 KiB by default) are stored outside the LSM tree to minimize write amplification during leveled compaction.
- **Data Integrity**: CRC32 checksums on SSTable blocks and WAL records, verified when they are read from disk, plus the offline `skv scrub` sweep of SSTable data blocks. Continuous background scrubbing is not implemented yet.
- **Deterministic Simulation Tested (DST)**: Differentially tested against an in-memory reference model, with every divergence failing the run: 500 seeds of 5,000 steps each (2,500,000 operations) in every CI run.

---

## Workspace Crates

The SurrealKV workspace is organized into modular crates:

```text
surrealkv/
├── src/                                    (Core LSM Kernel: MemTable, WAL, Ring, SSTables, Compaction)
└── crates/
    ├── surrealkv-cli/                      (Operator and developer diagnostic tool: `skv`)
    ├── surrealkv-sim/                      (Deterministic Simulation Testing & Differential Testing Harness)
    ├── surrealkv-compat-rocksdb/           (Pure-Rust RocksDB BlockBasedTable v2-v7 Reader & Migrator)
    ├── surrealkv-compat-v1/                (Pure-Rust SurrealKV V1 Reader & Migrator)
    └── surrealkv-compat-indxdb/            (Pure-Rust & WASM IndexedDB Reader & Migrator)
```

---

## Quick Start

This README describes the engine on the `main` branch, which has not been released yet. The `0.21.x` versions of `surrealkv` on crates.io are the previous engine, so `surrealkv = "0.21"` does not give you what is described here. Until the next release, depend on this repository directly:

```toml
[dependencies]
surrealkv = { git = "https://github.com/surrealdb/surrealkv" }
tokio = { version = "1", features = ["full"] }
```

### Basic Usage

```rust,no_run
use surrealkv::TreeBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open or create a database
    let tree = TreeBuilder::new()
        .with_path("data/mydb".into())
        .build()?;

    // Write transaction
    {
        let mut tx = tree.begin()?;
        tx.set(b"user:001", b"Alice")?;
        tx.set(b"user:002", b"Bob")?;
        tx.commit().await?;
    }

    // Read transaction
    {
        let tx = tree.begin()?;
        if let Some(val) = tx.get(b"user:001")? {
            println!("Value: {}", String::from_utf8_lossy(&val));
        }
    }

    // Graceful close
    tree.close().await?;
    Ok(())
}
```

Commits use `Durability::Eventual` by default, so a commit does not wait for an fsync. Call `tx.set_durability(Durability::Immediate)` before `commit()` to make a commit durable before it returns.

---

## Range Scans & Iteration

SurrealKV provides a cursor-based iterator API supporting forward and backward traversal. The iterator methods come from the `LSMIterator` trait, which must be in scope:

```rust,no_run
use surrealkv::{LSMIterator, TreeBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tree = TreeBuilder::new()
        .with_path("data/mydb".into())
        .build()?;

    let tx = tree.begin()?;

    // Forward scan [start .. end)
    let mut iter = tx.range(b"user:000", b"user:999")?;
    iter.seek_first()?;
    while iter.valid() {
        let key = iter.key();
        let value = iter.value()?;
        println!("{:?} => {:?}", key.user_key(), value);
        iter.next()?;
    }

    // Backward scan
    let mut iter = tx.range(b"user:000", b"user:999")?;
    iter.seek_last()?;
    while iter.valid() {
        let key = iter.key();
        let value = iter.value()?;
        println!("{:?} => {:?}", key.user_key(), value);
        iter.prev()?;
    }

    Ok(())
}
```

---

## Transparent Data Encryption (not yet available)

The cipher primitives are implemented and unit-tested but are not connected to the SSTable, WAL or value-log write paths. `TreeBuilder::build()` returns an error if a key manager is configured, so data is never written unencrypted by accident. Use volume or filesystem encryption (LUKS, FileVault, BitLocker, encrypted cloud volumes) for encryption at rest today.

---

## Command-Line Tool (`skv`)

The `skv` utility (`crates/surrealkv-cli`) provides inspection, diagnostics, and data migration capabilities:

```bash
# Build the CLI
make build-cli

# Inspect database directory and storage breakdown
skv inspect path/to/db

# Dump LevelManifest hierarchy
skv manifest path/to/db

# Deep inspection of an SSTable file with checksum verification
skv sst path/to/db/sstables/00000000000000000001.sst --dump-keys --verify-checksum

# CRC32 integrity sweep across all SSTable data blocks
skv scrub path/to/db

# Point lookups and range scans
skv get path/to/db "user:001"
skv scan path/to/db --prefix "user:"

# Offline migration from RocksDB or SurrealKV V1
skv migrate path/to/rocksdb_dir path/to/new_surrealkv_db
```

---

## WebAssembly & OPFS Storage Primitives

SurrealKV builds for WebAssembly (`wasm32-unknown-unknown`). On that target, `surrealkv::storage::opfs` provides `LogStore` and `ObjectStore` implementations over the Origin Private File System (OPFS), using `FileSystemSyncAccessHandle` inside Web Workers.

These are storage primitives only. They are not yet wired into `Tree`, which opens its WAL, SSTable and value-log files through `std::fs`, so a `Tree` cannot persist to OPFS today. CI builds the crate for `wasm32-unknown-unknown` and tests the OPFS primitives in headless Chrome, but it does not run a `Tree` on that target.

To compile for WebAssembly:

```bash
RUSTFLAGS="--cfg getrandom_backend=\"wasm_js\"" cargo build --target wasm32-unknown-unknown
```

---

## Platform Support

| Operating System | Architecture | Tier |
| :--- | :--- | :--- |
| **Linux** | `x86_64` | Tier 1 |
| **Linux** | `aarch64` | Tier 2 |
| **macOS / Darwin** | `aarch64` (Apple Silicon) | Tier 1 |
| **macOS / Darwin** | `x86_64` | Tier 2 |
| **Windows** | `x86_64` | Tier 2 |
| **WebAssembly** | `wasm32-unknown-unknown` | Tier 2 |

- **Tier 1**: built and tested in CI.
- **Tier 2**: built in CI, but the test suite is not run on that target. Windows tests are skipped, and the Linux `aarch64` and macOS `x86_64` builds are cross-compiled while the tests run on the CI runner's own architecture. For WebAssembly only the OPFS storage primitives are tested (in headless Chrome); `Tree` is not run.

WAL appends and syncs, `get_async` block reads and parallel WAL replay go through `affinitypool`, which runs them on a worker pool when the host application installs one and otherwise inline on the calling thread. SurrealKV does not install a pool itself. Point reads and iteration are plain synchronous `pread` calls on the calling thread. `io_uring` is not used.

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
