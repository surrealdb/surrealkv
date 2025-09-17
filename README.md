# SurrealKV

[![License](https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square)](https://github.com/surrealdb/surrealkv)

> **⚠️ Development Status**: SurrealKV is currently under active development and is not feature complete. The API and implementation may change significantly between versions. Use with caution in production environments.

surrealkv is a versioned, low-level, persistent, embedded key-value database implemented in Rust using an LSM (Log-Structured Merge) tree architecture. It offers the following features:

## Features

- ✨ **ACID Compliance**: Full support for Atomicity, Consistency, Isolation, and Durability
- 🔄 **Rich Transaction Support**: Atomic operations for multiple inserts, updates, and deletes
- 🔒 **Isolation Level**: Supports Snapshot Isolation
- 💾 **Durability Guaranteed**: Persistent storage with protection against system failures
- 📦 **Embedded Database**: Easily integrate into your Rust applications
- 🔄 **MVCC Support**: Non-blocking concurrent reads and writes with snapshot isolation
- 📚 [TODO] **Built-in Versioning**: Track and access historical versions of your data

## Quick Start

```rust
use surrealkv::{Tree, TreeBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new LSM tree using TreeBuilder
    let tree = TreeBuilder::new()
        .with_path("path/to/db".into())
        .build()?;

    // Start a read-write transaction
    let mut txn = tree.begin()?;

    // Set some key-value pairs
    let key = b"hello";
    let value = b"world";
    txn.set(key, value)?;

    // Commit the transaction (async)
    txn.commit().await?;

    Ok(())
}
```

## Configuration

SurrealKV can be configured through various options when creating a new LSM tree:

```rust
use surrealkv::{Tree, TreeBuilder};

let tree = TreeBuilder::new()
    .with_path("path/to/db".into())                    // Database directory path
    .with_max_memtable_size(100 * 1024 * 1024)         // 100MB memtable size
    .with_block_size(4096)                             // 4KB block size
    .with_level_count(1)                               // Number of levels in LSM tree
    .with_vlog_max_file_size(128 * 1024 * 1024)        // 128MB VLog file size
    .with_enable_vlog(true)                            // Enable/disable VLog
    .build()?
```

### Storage Options

- `path`: Database directory path where SSTables and WAL files are stored
- `max_memtable_size`: Size threshold for memtable before flushing to SSTable
- `block_size`: Size of data blocks in SSTables (affects read performance)
- `level_count`: Number of levels in the LSM tree structure

### VLog Options

- `vlog_max_file_size`: Maximum size of VLog files before rotation
- `with_enable_vlog()`: Enable/disable Value Log for large value storage
- `vlog_gc_discard_ratio`: Threshold for triggering VLog garbage collection

### Performance Options

- `block_cache`: Cache for frequently accessed data blocks
- `vlog_cache`: Cache for VLog entries to reduce disk I/O

## Transaction Operations

### Basic Operations

```rust
use surrealkv::{Tree, TreeBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the LSM tree using TreeBuilder
    let tree = TreeBuilder::new()
        .with_path("path/to/db".into())
        .build()?;

    // Write Transaction
    {
        let mut txn = tree.begin()?;
        
        // Set multiple key-value pairs
        txn.set(b"foo1", b"bar")?;
        txn.set(b"foo2", b"bar")?;
        
        // Commit changes (async)
        txn.commit().await?;
    }

    // Read Transaction
    {
        let txn = tree.begin()?;
        
        if let Some(value) = txn.get(b"foo1")? {
            println!("Value: {:?}", value);
        }
    }

    Ok(())
}
```

### Range Operations

```rust
// Range scan between keys
let mut txn = tree.begin()?;
let range: Vec<_> = txn.range(b"key1", b"key5", None)?
    .map(|r| r.unwrap())
    .collect();

// Keys-only scan (more efficient for large values)
let keys: Vec<_> = txn.keys(b"key1", b"key5", None)?
    .map(|r| r.unwrap())
    .collect();

// Delete a key
txn.delete(b"key1")?;
txn.commit().await?;
```

### Transaction Control

```rust
// Read-only transaction
let txn = tree.begin_with_mode(Mode::ReadOnly)?;

// Write-only transaction
let mut txn = tree.begin_with_mode(Mode::WriteOnly)?;

// Rollback transaction
txn.rollback();

// Set durability level
txn.set_durability(Durability::Immediate);
```

## Features

### Durability Levels

The `Durability` enum provides two levels of durability for transactions:

- `Eventual`: Commits with this durability level are guaranteed to be persistent eventually. The data is written to the kernel buffer, but it is not fsynced before returning from `Transaction::commit`. This is the default durability level.
- `Immediate`: Commits with this durability level are guaranteed to be persistent as soon as `Transaction::commit` returns. Data is fsynced to disk before returning from `Transaction::commit`. This is the slowest durability level, but it is the safest.

```rust
// Set transaction durability to Eventual (default)
tx.set_durability(Durability::Eventual);

// Set transaction durability to Immediate
tx.set_durability(Durability::Immediate);
```

## Platform Compatibility

### ✅ Supported Platforms
- **Linux** (x86_64, aarch64): Full support including all features and tests
- **macOS** (x86_64, aarch64): Full support including all features and tests

### ❌ Not Supported
- **WebAssembly (WASM)**: Not supported due to fundamental incompatibilities:
  - Requires file system access not available in WASM environments
  - Write-Ahead Log (WAL) and Value Log (VLog) operations are not compatible
  - System-level I/O operations are not available

- **Windows** (x86_64): Basic functionality supported, but some features are limited:
  - File operations are not thread safe (TODO)
  - Some advanced file system operations may have reduced functionality
  - Performance may be lower compared to Unix-like systems

## History

SurrealKV has undergone a significant architectural evolution to address scalability challenges:

### Previous Design (VART-based)
The original implementation used a **versioned adaptive radix trie (VART)** architecture with the following components:

- **In-Memory Index**: Versioned adaptive radix trie using [vart](https://github.com/surrealdb/vart) for key-to-offset mappings
- **Sequential Log Storage**: Append-only storage divided into segments with binary record format
- **Memory Limitations**: The entire index had to reside in memory, limiting scalability for large datasets

**Why the Change?**
The VART-based design had fundamental scalability limitations:
- **Memory Constraint**: The entire index must fit in memory, making it unsuitable for datasets larger than available RAM
- **Recovery Overhead**: Startup required scanning all log segments to rebuild the in-memory index
- **Write Amplification**: Each update created new versions, leading to memory pressure

### Current Design (LSM Tree)
The new LSM (Log-Structured Merge) tree architecture provides:

- **Efficient Compaction**: Leveled compaction strategy for optimal space utilization
- **Better Scalability**: Supports datasets much larger than available memory

This architectural change enables SurrealKV to handle large-scale datasets while maintaining ACID properties and high performance.

## License

Licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.