<br>

<p align="center">
    <a href="https://surrealdb.com#gh-dark-mode-only" target="_blank">
        <img width="200" src="/img/white/logo.svg" alt="SurrealKV Logo">
    </a>
    <a href="https://surrealdb.com#gh-light-mode-only" target="_blank">
        <img width="200" src="/img/black/logo.svg" alt="SurrealKV Logo">
    </a>
</p>

<p align="center">An embedded, non-blocking, async-native, key-value storage engine.</p>

<br>

<p align="center">
	<a href="https://github.com/surrealdb/surrealkv"><img src="https://img.shields.io/badge/status-stable-ff00bb.svg?style=flat-square"></a>
	&nbsp;
	<a href="https://docs.rs/surrealkv/"><img src="https://img.shields.io/docsrs/surrealkv?style=flat-square"></a>
	&nbsp;
	<a href="https://crates.io/crates/surrealkv"><img src="https://img.shields.io/crates/v/surrealkv?style=flat-square"></a>
	&nbsp;
	<a href="https://github.com/surrealdb/surrealkv"><img src="https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square"></a>
</p>

SurrealKV is a high-performance, non-blocking, async-native, embedded key-value storage engine built on a modern Log-Structured Merge (LSM) tree architecture.

It is designed as an independent, standalone key-value store suitable for high-throughput server workloads, embedded systems, desktop applications, and browser WebAssembly environments over the Origin Private File System (OPFS).

---

## Performance

Benchmarked on bare metal (**AMD Ryzen Threadripper 9970X 32-Core / 64-Thread Processor @ 5.48 GHz, 128 GB DDR5 RAM, 4TB PCIe 4.0 NVMe SSD**, synchronous durability enabled with `--sync` via [`crud-bench`](https://github.com/surrealdb/crud-bench)):

| Engine | Point Read (OPS) | Create (OPS) | Update (OPS) | Delete (OPS) | Scan (OPS) | Peak Memory |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **SurrealKV** | **1,182,982** | **98,853** | **102,614** | **97,037** | **8,361** | 1.35 GB |
| RocksDB | 692,597 | 31,777 | 33,228 | 34,563 | 2,406 | 393 MB |
| Fjall | 767,353 | 1,327 | 1,315 | 1,173 | 385 | 674 MB |
| LMDB | 896,110 | 695 | 704 | 705 | 27,446 | 558 MB |
| Libmdbx | 264,133 | 697 | 701 | 681 | 16,005 | 616 MB |

- **Updates**: **102,614 OPS** (**3.09× faster** than RocksDB, **78× faster** than Fjall, and **145× faster** than LMDB) with lock-free group commit.
- **Creates (Writes)**: **98,853 OPS** (**3.11× faster** than RocksDB and **74× faster** than Fjall) under synchronous disk persistence.
- **Point Reads**: **1.18M OPS** sustained — the fastest point-read throughput among all disk-backed engines tested.
- **Deletes**: **97,037 OPS** (**2.81× faster** than RocksDB) via high-efficiency tombstone append buffering.
- **Range Scans**: **3.47× faster** than RocksDB via zero-copy iterator merges and block-level restart points.

---

## Features

- **Async Native & Non-Blocking**: Fully concurrent, non-blocking commit pipeline built on lock-free ring buffers and optimistic concurrency control (OCC).
- **Snapshot Isolation**: Multi-version concurrency control with non-blocking concurrent reads and isolated read transactions.
- **Origin Private File System (OPFS)**: Native WebAssembly browser support using `FileSystemSyncAccessHandle` inside Web Workers for persistent client-side storage.
- **Automated Zero-Effort Migration**: Automatically detects and migrates legacy databases on startup in pure Rust:
  - RocksDB BlockBasedTable formats (v2 through v7) via `surrealkv-compat-rocksdb`.
  - SurrealKV V1 stores via `surrealkv-compat-v1`.
  - Browser IndexedDB (`indxdb://`) stores via `surrealkv-compat-indxdb`.
- **Value Log Separation (WiscKey)**: Separates large values from the LSM tree to minimize write amplification during leveled compaction.
- **Data Integrity & Bitrot Scrubber**: Continuous background verification of CRC32 block checksums across all SSTables.
- **Deterministic Simulation Tested (DST)**: Verified against an in-memory linearizable model oracle across 25,000,000 operations with zero divergences.

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

Add SurrealKV to your `Cargo.toml`:

```toml
[dependencies]
surrealkv = "0.21"
tokio = { version = "1", features = ["full"] }
```

### Basic Usage

```rust
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

---

## Range Scans & Iteration

SurrealKV provides a cursor-based iterator API supporting forward and backward traversal:

```rust
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
```

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

# Full CRC32 integrity sweep across all database blocks
skv scrub path/to/db

# Point lookups and range scans
skv get path/to/db "user:001"
skv scan path/to/db --prefix "user:"

# Offline migration from RocksDB or SurrealKV V1
skv migrate path/to/rocksdb_dir path/to/new_surrealkv_db
```

---

## WebAssembly & Browser Persistence (OPFS)

SurrealKV supports WebAssembly (`wasm32-unknown-unknown`) inside browser Web Workers over the Origin Private File System (OPFS):

- Uses `FileSystemSyncAccessHandle` for synchronous, zero-copy block reads and writes.
- Prevents UI thread blocking by running I/O in worker contexts.
- Automatically handles browser quota and sync handles.

To compile for WebAssembly:

```bash
RUSTFLAGS="--cfg getrandom_backend=\"wasm_js\"" cargo build --target wasm32-unknown-unknown
```

---

## Platform Support

| Operating System | Architectures | Status |
| :--- | :--- | :--- |
| **Linux** | `x86_64`, `aarch64` | Tier 1 (Full async I/O, `io_uring` & threadpool) |
| **macOS / Darwin** | `x86_64`, `aarch64` (Apple Silicon) | Tier 1 (Full async I/O via `affinitypool`) |
| **Windows** | `x86_64` | Tier 1 (Full support) |
| **WebAssembly** | `wasm32-unknown-unknown` | Tier 1 (Browser OPFS & Web Workers) |

---

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
