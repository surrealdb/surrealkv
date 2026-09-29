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

Benchmarked on bare metal (**AMD Ryzen Threadripper 9970X 32-Core / 64-Thread Processor @ 5.48 GHz, 128 GB DDR5 RAM, 4TB PCIe 4.0 NVMe SSD**, 50,000 keys across 48 concurrent worker threads with 128 clients, synchronous durability enabled with `--sync` via [`crud-bench`](https://github.com/surrealdb/crud-bench)):

| Engine | Point&nbsp;Read&nbsp;(OPS) | Create&nbsp;(OPS) | Update&nbsp;(OPS) | Delete&nbsp;(OPS) | Scan&nbsp;(OPS) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **SurrealKV** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**1,440,502** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**156,638** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**177,110** | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**225,318** | 8,356 |
| RocksDB | 692,257 | 34,233 | 35,652 | 36,440 | 2,447 |
| Fjall | 848,857 | 1,351 | 1,219 | 1,180 | 379 |
| SlateDB | 279,526 | 9,039 | 8,792 | 8,847 | 1,109 |
| LMDB | 924,010 | 692 | 701 | 704 | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**26,946** |
| Libmdbx | 294,190 | 687 | 709 | 640 | 21,911 |

- **Creates (Writes)**: **156,638 OPS** (**4.58× faster** than RocksDB, **17.3× faster** than SlateDB, and **116× faster** than Fjall) under synchronous disk persistence.
- **Updates**: **177,110 OPS** (**4.97× faster** than RocksDB, **20.1× faster** than SlateDB, and **145× faster** than Fjall) with lock-free group commit.
- **Point Reads**: **1.44M OPS** sustained: the fastest point-read throughput among all engines tested (**2.08× faster** than RocksDB, **1.56× faster** than LMDB, and **5.15× faster** than SlateDB).
- **Deletes**: **225,318 OPS** (**6.18× faster** than RocksDB, **25.5× faster** than SlateDB, and **191× faster** than Fjall).
- **Range Scans**: **3.42× faster** than RocksDB and **7.53× faster** than SlateDB via zero-copy iterator merges and block-level restart points.

### Memory Profile

Resting and peak memory are the lowest and highest process memory sampled across the whole benchmark run.

| Engine | Resting&nbsp;Memory | Peak&nbsp;Memory |
| :--- | ---: | ---: |
| **SurrealKV** | ~353 MB | 1.50 GB |
| RocksDB | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**~294 MB** | 1.10 GB |
| Fjall | ~422 MB | 1.13 GB |
| SlateDB | ~380 MB | 1.84 GB |
| LMDB | ~382 MB | 877 MB |
| Libmdbx | ~438 MB | <img width="16" align="absmiddle" src="/img/rocket.png" alt="🚀">&nbsp;**857 MB** |

- **Resting Footprint**: **~353 MB** at rest, the second-lowest of the engines tested after RocksDB (**~294 MB**).
- **Dynamic Scaling**: Peak memory expands dynamically under high concurrency to buffer active batches during parallel commits, then contracts back to resting baseline.

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

## Transparent Data Encryption (TDE) & Key Rotation

SurrealKV supports authenticated encryption at rest (AEAD) with self-describing block envelopes and online key rotation without downtime.

### Supported Cipher Suites
- `CipherSuite::Aes256Gcm` (default): Hardware-accelerated standard AEAD via AES-NI / ARMv8 crypto instructions.
- `CipherSuite::XChaCha20Poly1305`: Constant-time software AEAD with an extended 192-bit nonce to eliminate nonce collision risk.
- `CipherSuite::ChaCha20Blake3`: Committing AEAD construction combining ChaCha20 stream encryption with keyed BLAKE3 MAC for maximum throughput.

### Configuration & Key Rotation Example

```rust
use std::sync::Arc;
use surrealkv::{CipherSuite, Options, SoftwareKeyManager, TreeBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize a KeyManager with a 256-bit (32-byte) master key
    let initial_key = [0x42u8; 32];
    let key_manager = Arc::new(SoftwareKeyManager::new(initial_key));

    // 2. Configure Options with Transparent Data Encryption (TDE)
    let opts = Options::new()
        .with_path("data/encrypted_db".into())
        .with_encryption(key_manager.clone(), CipherSuite::Aes256Gcm);

    let tree = TreeBuilder::with_options(opts).build()?;

    // Writes are transparently encrypted before hitting disk
    {
        let mut tx = tree.begin()?;
        tx.set(b"secret:key", b"super_sensitive_data")?;
        tx.commit().await?;
    }

    // 3. Online Key Rotation
    // Add a new key and set it as the active key ID for subsequent writes
    let rotated_key = [0x99u8; 32];
    key_manager.add_key(2, rotated_key);
    key_manager.set_active_key_id(2)?;

    // New writes immediately use key ID 2
    {
        let mut tx = tree.begin()?;
        tx.set(b"secret:new_key", b"data_under_rotated_key")?;
        tx.commit().await?;
    }

    // Existing blocks with older key IDs remain transparently readable
    // via self-describing authenticated envelope headers
    {
        let tx = tree.begin()?;
        let val1 = tx.get(b"secret:key")?;
        let val2 = tx.get(b"secret:new_key")?;
        assert!(val1.is_some() && val2.is_some());
    }

    tree.close().await?;
    Ok(())
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
