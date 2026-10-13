<br>

<p align="center">
    <a href="https://surrealdb.com" target="_blank">
        <img width="200" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/black/logo.svg" alt="SurrealKV Logo">
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

Benchmarked on bare metal (**AMD Ryzen Threadripper 9970X 32-Core / 64-Thread Processor @ 5.48 GHz, 128 GB DDR5 RAM, 4TB PCIe 4.0 NVMe SSD**, 500,000 keys across 48 concurrent worker threads with 128 clients, and 5,000,000 iterations of each range scan, via [`crud-bench`](https://github.com/surrealdb/crud-bench)). Each engine is measured twice: with synchronous durability (`--sync`), and without it.

### With synchronous durability

Every commit is fully durable and flushed to disk before it returns (each figure is a single run).

| Engine | Point&nbsp;Read&nbsp;(OPS) | Create&nbsp;(OPS) | Update&nbsp;(OPS) | Delete&nbsp;(OPS) | Scan&nbsp;(OPS) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **SurrealKV** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**6,804,864** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**229,376** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**221,341** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**264,089** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**905,132** |
| RocksDB | 855,851 | 8,568 | 36,263 | 32,043 | 852,661 |
| SlateDB | 92,303 | 8,257 | 8,122 | 8,242 | 17,985 |
| Fjall | \* | \* | \* | \* | \* |
| ReDB | \* | \* | \* | \* | \* |
| LMDB | \* | \* | \* | \* | \* |
| Libmdbx | \* | \* | \* | \* | \* |

\* Not benchmarked with synchronous durability: at this dataset size the run was too slow to complete in the time available.

- **Creates (Writes)**: **229,376 OPS**: **26.8× faster** than RocksDB and **27.8× faster** than SlateDB.
- **Updates**: **221,341 OPS**: **6.10× faster** than RocksDB and **27.3× faster** than SlateDB.
- **Point Reads**: **6.80M OPS** sustained: **7.95× faster** than RocksDB and **73.7× faster** than SlateDB.
- **Deletes**: **264,089 OPS**: **8.24× faster** than RocksDB and **32.0× faster** than SlateDB.
- **Range Scans**: **905,132 OPS** on `select(*) limit(100)` scans: **1.06× faster** than RocksDB and **50.3× faster** than SlateDB. See [Scan Profile](#scan-profile) for other kinds of range scan.

### Without synchronous durability

Commits are not flushed to disk before they return (each figure is a single run).

| Engine | Point&nbsp;Read&nbsp;(OPS) | Create&nbsp;(OPS) | Update&nbsp;(OPS) | Delete&nbsp;(OPS) | Scan&nbsp;(OPS) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **SurrealKV** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**6,992,767** | 321,715 | 274,172 | 307,034 | 896,366 |
| RocksDB | 846,293 | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**603,452** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**608,494** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**739,357** | 851,848 |
| SlateDB | 96,397 | 6,314 | 6,352 | 6,380 | 26,187 |
| Fjall | 992,874 | 62,608 | 64,059 | 2,676 | 132,698 |
| ReDB | 779,262 | 35,171 | 35,805 | 65,686 | 561,837 |
| LMDB | 1,461,261 | 127,045 | 163,099 | 157,965 | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**1,192,772** |
| Libmdbx | 613,904 | 24,378 | 5,026 | 11,521 | 384,128 |

- **Creates (Writes)**: **321,715 OPS**: **1.88× slower** than RocksDB, **51.0× faster** than SlateDB, **5.14× faster** than Fjall, **9.15× faster** than ReDB, **2.53× faster** than LMDB, and **13.2× faster** than Libmdbx.
- **Updates**: **274,172 OPS**: **2.22× slower** than RocksDB, **43.2× faster** than SlateDB, **4.28× faster** than Fjall, **7.66× faster** than ReDB, **1.68× faster** than LMDB, and **54.6× faster** than Libmdbx.
- **Point Reads**: **6.99M OPS** sustained: **8.26× faster** than RocksDB, **72.5× faster** than SlateDB, **7.04× faster** than Fjall, **8.97× faster** than ReDB, **4.79× faster** than LMDB, and **11.4× faster** than Libmdbx.
- **Deletes**: **307,034 OPS**: **2.41× slower** than RocksDB, **48.1× faster** than SlateDB, **115× faster** than Fjall, **4.67× faster** than ReDB, **1.94× faster** than LMDB, and **26.7× faster** than Libmdbx.
- **Range Scans**: **896,366 OPS** on `select(*) limit(100)` scans: **1.05× faster** than RocksDB, **34.2× faster** than SlateDB, **6.75× faster** than Fjall, **1.60× faster** than ReDB, **1.33× slower** than LMDB, and **2.33× faster** than Libmdbx. See [Scan Profile](#scan-profile) for other kinds of range scan.

### Scan Profile

Range scans over the same dataset, as scans per second, without synchronous durability: limit and skip scans, and counts over the full key range.

| Engine | &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Limit&nbsp;(OPS) | Keys,&nbsp;Limit&nbsp;(OPS) | Skip&nbsp;+&nbsp;Limit&nbsp;(OPS) | Keys,&nbsp;Skip&nbsp;+&nbsp;Limit&nbsp;(OPS) | Full&nbsp;Count&nbsp;(OPS) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **SurrealKV** | 896,366 | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**2,313,942** | 96,365 | 98,612 | 402 |
| RocksDB | 851,848 | 851,580 | 35,539 | 35,902 | 159 |
| SlateDB | 26,187 | 26,266 | 7,460 | 7,477 | 87 |
| Fjall | 132,698 | 151,861 | 4,335 | 4,383 | 42.4 |
| ReDB | 561,837 | 571,629 | 44,438 | 44,462 | 249 |
| LMDB | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**1,192,772** | 1,195,166 | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**549,220** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**552,090** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**902** |
| Libmdbx | 384,128 | 260,769 | 183,361 | 185,073 | 848 |

- **Limit**: `select(*) limit(100)` returns the first 100 key-value pairs of the range. **Keys, Limit** (`select(id) limit(100)`) returns only the keys.
- **Skip + Limit**: `select(*) start(5000) limit(100)` walks and discards 5,000 entries before returning 100. **Keys, Skip + Limit** returns only the keys.
- **Full Count**: `count()` visits every key in the range.

### Memory Profile

Resting and peak memory are the lowest and highest process memory sampled across the whole benchmark run, without synchronous durability.

| Engine | Resting&nbsp;Memory | Peak&nbsp;Memory |
| :--- | ---: | ---: |
| **SurrealKV** | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**221 MiB** | 1.79 GiB |
| RocksDB | 590 MiB | 1.02 GiB |
| SlateDB | 265 MiB | 2.73 GiB |
| Fjall | 627 MiB | 1.71 GiB |
| ReDB | 257 MiB | 2.17 GiB |
| LMDB | 776 MiB | <img width="16" align="absmiddle" src="https://raw.githubusercontent.com/surrealdb/surrealkv/main/img/rocket.png" alt="🚀">&nbsp;**826 MiB** |
| Libmdbx | 40.98 GiB | 117.71 GiB |

- **Resting Footprint**: **221 MiB** at rest, the lowest of the engines tested.
- **Dynamic Scaling**: Peak memory rises under high concurrency, when parallel commits buffer active batches.

---

## Features

- **Async Commit Pipeline**: Concurrent commit pipeline built on lock-free ring buffers and optimistic concurrency control (OCC), awaited asynchronously. Point reads and iteration are synchronous; `get_async` is an opt-in asynchronous point read.
- **Snapshot Isolation**: Multi-version concurrency control with non-blocking concurrent reads and isolated read transactions.
- **Versioned Queries**: Opt in with `with_versioning(true, retention_ns)`. Every write is stamped with its timestamp (`set_at`, or the time of the commit) and kept with its value. `get_at(key, ts)` reads a key as of a time, `history` returns every version of a key newest timestamp first, with its timestamp, and compaction keeps the versions for the retention period, or for ever with `0`. Versions can be written for a time in the past. It needs no value log and no index.
- **Origin Private File System (OPFS) Primitives**: `LogStore` and `ObjectStore` implementations over `FileSystemSyncAccessHandle` for Web Workers (`surrealkv::storage::opfs`, `wasm32` only, doc-hidden and not part of the stable API). They are not yet wired into `Tree`, so a `Tree` does not persist to OPFS yet.
- **Automated Zero-Effort Migration**: Opening an existing database converts it. The conversion is one way: the original files are moved to a backup directory inside it, but the backup is not a rollback point (the value log is shared, and its garbage collection follows the new tables), and a database that this version has opened can no longer be opened by an earlier release. Take a copy of the directory first if you may need to go back:
  - SurrealKV v0.20 and v0.21 databases, migrated when they are opened: the retained versions with their sequence numbers and timestamps, and the unflushed write-ahead log. The old files are kept in `_surrealkv_v0_backup/`.
  - RocksDB databases written by SurrealDB (RocksDB 8 to 11, read without linking RocksDB) via `surrealkv-compat-rocksdb`: a database that SurrealDB ran in versioned mode is migrated with the versions it stores and their timestamps (converted from SurrealDB's hybrid logical clock to nanoseconds); any other keeps the newest value of each key. Blob files and the write-ahead log are read. The database must not be open in RocksDB while it is migrated, and its files are kept in `_rocksdb_backup/`. A tree opened with `with_versioning(true, retention_ns)` keeps those versions as its retention says, and answers `get_at` and `history` for them.
  - Browser IndexedDB (`indxdb://`) data from an exported IndexedDB dump via `surrealkv-compat-indxdb`.
- **Encryption at Rest**: Transparent data encryption of the manifest, SSTables, value log and write-ahead log with AES-256-GCM or XChaCha20-Poly1305, under per-file keys derived from a root key that you provide through a `KeyManager`. See [Transparent Data Encryption](#transparent-data-encryption).
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
    ├── surrealkv-compat-rocksdb/           (Pure-Rust Reader of SurrealDB's RocksDB databases, for migration)
    ├── surrealkv-compat-v1/                (Standalone reader of LSMV1 databases (SurrealKV up to v0.21.4); the core converts them to LSMV2 when it opens them)
    └── surrealkv-compat-indxdb/            (Pure-Rust & WASM IndexedDB Reader & Migrator)
```

---

## Quick Start

Databases written by SurrealKV 0.20 and 0.21, and by SurrealDB's RocksDB backend, are migrated when they are opened.

```toml
[dependencies]
surrealkv = "1"
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

## Transparent Data Encryption

A database created with a key manager is encrypted at rest. Every file is sealed: the manifest, the SSTables, the value log and the write-ahead log, and so are checkpoints. Nothing else changes for the application: reads, writes, transactions and iteration work as before.

- **Ciphers**: AES-256-GCM, or XChaCha20-Poly1305 on a CPU without AES instructions. `CipherPreference::Auto` picks one when the database is created and the choice is stored in it. Pick one yourself with `CipherSuite::Aes256Gcm` or `CipherSuite::XChaCha20Poly1305`.
- **Keys**: a `KeyManager` owns the 32 byte root keys and the database keeps none. Roots never encrypt data: every file gets a key of its own, derived from the root, a fresh random salt and the identity of the file.
- **Authenticated**: every block is bound to its database, file, key and position, so a block that is moved, replayed or edited is refused and never returned as data.
- **Fixed at creation**: encryption cannot be added to a database that exists, and an encrypted database cannot be opened without its key. Neither is ever repaired or deleted because of a key.
- **Not hidden**: file names, file sizes and access timing are visible to anyone who can read the directory.

> **A key that is lost is data that is lost.** There is no copy to recover it from, and every file, checkpoint and backup written under a key is unreadable without it. The on-disk formats are not frozen yet, so do not rely on them for data that must survive an upgrade without a backup.

The `encryption` feature is on by default. Build with `default-features = false` to compile it out: a build without it refuses a key manager with `Error::InvalidArgument` and creates nothing, so data is never written unencrypted by accident.

### Create or open an encrypted database

The key is a file with 32 raw bytes or 64 hex digits. Create one that only you can read:

```bash
openssl rand -hex 32 > mydb.key && chmod 600 mydb.key
```

Then give it to the builder. The same code creates the database and opens it again, and an existing database keeps the cipher it was created with:

```rust,no_run
use std::sync::Arc;

use surrealkv::{CipherPreference, SoftwareKeyManager, TreeBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // On Unix, a key file that every user can read is refused.
    let keys = Arc::new(SoftwareKeyManager::from_key_file("mydb.key")?);

    let tree = TreeBuilder::new()
        .with_path("data/mydb".into())
        .with_encryption(keys, CipherPreference::Auto)
        .build()?;

    let mut tx = tree.begin()?;
    tx.set(b"user:001", b"Alice")?;
    tx.commit().await?;

    tree.close().await?;
    Ok(())
}
```

`with_encryption` is also available on `Options` for code that builds its options first.

### Choose the cipher

```rust,no_run
use std::sync::Arc;

use surrealkv::{CipherSuite, SoftwareKeyManager, TreeBuilder};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let keys = Arc::new(SoftwareKeyManager::from_hex(
        "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
    )?);

    // The cipher is used when the database is created, and kept in it after that.
    let _tree = TreeBuilder::new()
        .with_path("data/mydb".into())
        .with_encryption(keys, CipherSuite::XChaCha20Poly1305)
        .build()?;
    Ok(())
}
```

A key in source code is for tests. Read it from a file, or from your key management service, in a real deployment.

### Rotate the key

New files are written under the active key, and files written under an older key stay readable while its key is decrypt-only. Keep an old key for as long as any file or checkpoint written under it is wanted.

```rust,no_run
use std::sync::Arc;

use surrealkv::{CipherPreference, SoftwareKeyManager, TreeBuilder};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let keys = Arc::new(SoftwareKeyManager::from_key_file("mydb.key")?); // key id 1

    // Register a second key and make it the one that new files are written under.
    // Key id 1 becomes decrypt-only.
    keys.add_key(2, [0x42; 32])?; // use a random key from a secure source
    keys.set_active_key_id(2)?;

    // Every key id that a file names must be held by the manager when the database is opened.
    let _tree = TreeBuilder::new()
        .with_path("data/mydb".into())
        .with_encryption(keys, CipherPreference::Auto)
        .build()?;
    Ok(())
}
```

### Use your own key management service

`SoftwareKeyManager` keeps roots in memory, which suits tests and small deployments. To keep them in a KMS or an HSM, implement `KeyManager`. The trait is called on the commit path, so `active_key_id`, `key_state` and `root_key` must answer from memory. Do the slow work of fetching roots in `prepare`, which the engine calls when the database is opened.

```rust,no_run
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use surrealkv::tde::Zeroizing;
use surrealkv::{EncryptionError, KeyManager, KeyState, TreeBuilder};

struct KmsKeys {
    active: u32,
    cache: RwLock<HashMap<u32, Zeroizing<[u8; 32]>>>,
}

impl KmsKeys {
    /// Fetches a root from your provider. Slow, so it is only called from `prepare`.
    fn fetch(&self, _key_id: u32) -> Result<[u8; 32], EncryptionError> {
        unimplemented!("call your key management service")
    }
}

impl KeyManager for KmsKeys {
    fn active_key_id(&self) -> u32 {
        self.active
    }

    fn key_state(&self, key_id: u32) -> KeyState {
        match (key_id == self.active, self.cache.read().unwrap().contains_key(&key_id)) {
            (true, _) => KeyState::Active,
            (false, true) => KeyState::DecryptOnly,
            (false, false) => KeyState::Unknown,
        }
    }

    fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; 32]>, EncryptionError> {
        match self.cache.read().unwrap().get(&key_id) {
            Some(root) => Ok(Zeroizing::new(**root)),
            None => Err(EncryptionError::UnknownKeyId),
        }
    }

    fn prepare(&self, key_ids: &[u32]) -> Result<(), EncryptionError> {
        for &id in key_ids {
            let root = self.fetch(id)?;
            self.cache.write().unwrap().insert(id, Zeroizing::new(root));
        }
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let keys = Arc::new(KmsKeys { active: 1, cache: RwLock::new(HashMap::new()) });
    let _tree = TreeBuilder::new()
        .with_path("data/mydb".into())
        .with_encryption(keys, surrealkv::CipherPreference::Auto)
        .build()?;
    Ok(())
}
```

A wrong or missing key is refused by name, before anything is changed: `Error::Encryption` carries `WrongKey`, `UnknownKeyId`, `KeyRetired`, `NotConfigured` (an encrypted database opened without a key manager) or `NotEncrypted` (a key given to a database that was created without one).

### Encrypted databases and `skv`

`skv` reads encrypted databases when it is given the key file with `--key-file` or `SKV_KEY_FILE`. The variable holds a path, never a key. `skv enc-info` lists which cipher and keys a database uses, and needs no key.

```bash
skv enc-info path/to/db
skv inspect path/to/db --key-file mydb.key
SKV_KEY_FILE=mydb.key skv scan path/to/db --prefix "user:"
```

## Command-Line Tool (`skv`)

The `skv` utility (`crates/surrealkv-cli`) provides inspection, diagnostics, and data migration capabilities:

```bash
# Install the CLI
cargo install surrealkv-cli

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

# Offline migration of a RocksDB or SurrealKV (up to v0.21.4) database into a new directory (a SurrealKV database is also migrated in place when it is opened)
skv migrate path/to/rocksdb_dir path/to/new_surrealkv_db
```

---

## WebAssembly & OPFS Storage Primitives

SurrealKV builds for WebAssembly (`wasm32-unknown-unknown`). On that target, `surrealkv::storage::opfs` (doc-hidden, not part of the stable API) provides `LogStore` and `ObjectStore` implementations over the Origin Private File System (OPFS), using `FileSystemSyncAccessHandle` inside Web Workers.

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

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/surrealdb/surrealkv/blob/main/LICENSE) for details.
