# SurrealKV

[![License](https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square)](https://github.com/surrealdb/surrealkv)

SurrealKV is a versioned, embedded key-value store built on an LSM (Log-Structured Merge) tree architecture, with support for time-travel queries.

It is designed specifically for use within SurrealDB, with the goal of reducing dependency on external storage engine (RocksDB). This approach allows the storage layer to evolve in alignment with SurrealDB’s requirements and access patterns.

## Features

- **ACID Compliance**: Full support for Atomicity, Consistency, Isolation, and Durability
- **Snapshot Isolation**: MVCC support with non-blocking concurrent reads and writes
- **Durability Levels**: Immediate and Eventual durability modes
- **Time-Travel Queries**: Built-in versioning with point-in-time reads and historical queries
- **Checkpoint and Restore**: Create consistent snapshots for backup and recovery
- **Value Log (Wisckey)**: Ability to store large values separately, with garbage collection

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
    txn.set(b"hello", b"world")?;

    // Commit the transaction (async)
    txn.commit().await?;

    Ok(())
}
```

## Configuration

SurrealKV can be configured through various options when creating a new LSM tree:

### Basic Configuration

```rust
use surrealkv::TreeBuilder;

let tree = TreeBuilder::new()
    .with_path("path/to/db".into())           // Database directory path
    .with_max_memtable_size(100 * 1024 * 1024) // 100MB memtable size
    .with_block_size(4096)                    // 4KB block size
    .with_level_count(7)                      // Number of levels in LSM tree
    .build()?;
```

**Options:**
- `with_path()` - Database directory where SSTables and WAL files are stored
- `with_max_memtable_size()` - Size threshold for memtable before flushing to SSTable
- `with_block_size()` - Size of data blocks in SSTables (affects read performance)
- `with_level_count()` - Number of levels in the LSM tree structure

### Compression Configuration

SurrealKV supports per-level compression for SSTable data blocks, allowing different compression algorithms for different LSM levels. By default, no compression is used.

```rust
use surrealkv::{CompressionType, Options, TreeBuilder};

// Default: No compression (for maximum write performance)
let tree = TreeBuilder::new()
    .with_path("path/to/db".into())
    .build()?;

// Explicitly disable compression (same as default)
let opts = Options::new()
    .with_path("path/to/db".into())
    .without_compression();

let tree = TreeBuilder::with_options(opts).build()?;

// Per-level compression configuration
let opts = Options::new()
    .with_path("path/to/db".into())
    .with_compression_per_level(vec![
        CompressionType::None,        // L0: No compression for speed
        CompressionType::SnappyCompression, // L1+: Snappy compression
    ]);

let tree = TreeBuilder::with_options(opts).build()?;

// Convenience: No compression on L0, Snappy on other levels
let opts = Options::new()
    .with_path("path/to/db".into())
    .with_l0_no_compression();

let tree = TreeBuilder::with_options(opts).build()?;
```

**Options:**
- `without_compression()` - Disable compression for all levels (default behavior)
- `with_compression_per_level()` - Set compression type per level (vector index = level number)
- `with_l0_no_compression()` - Convenience method for no compression on L0, Snappy compression on other levels

**Compression Types:**
- `CompressionType::None` - No compression (fastest writes, largest files)
- `CompressionType::SnappyCompression` - Snappy compression (good balance of speed and compression ratio)

### Value Log Configuration

The Value Log (VLog) separates large values from the LSM tree for more efficient storage and compaction.

```rust
use surrealkv::{TreeBuilder, VLogChecksumLevel};

let tree = TreeBuilder::new()
    .with_path("path/to/db".into())
    .with_enable_vlog(true)                     // Enable VLog
    .with_vlog_value_threshold(1024)            // Values > 1KB go to VLog
    .with_vlog_max_file_size(256 * 1024 * 1024) // 256MB VLog file size
    .with_vlog_checksum_verification(VLogChecksumLevel::Full)
    .build()?;
```

**Options:**
- `with_enable_vlog()` - Enable/disable Value Log for large value storage
- `with_vlog_value_threshold()` - Size threshold in bytes; values larger than this are stored in VLog (default: 1KB)
- `with_vlog_max_file_size()` - Maximum size of VLog files before rotation (default: 256MB)
- `with_vlog_checksum_verification()` - Checksum verification level (`Disabled` or `Full`)


### Versioning Configuration

Enable time-travel queries to read historical versions of your data:

```rust
use surrealkv::{Options, TreeBuilder};

let opts = Options::new()
    .with_path("path/to/db".into())
    .with_versioning(true, 0);  // Enable versioning, retention_ns = 0 means no limit

let tree = TreeBuilder::with_options(opts).build()?;
```

**Note:** Versioning requires VLog to be enabled. When you call `with_versioning(true, retention_ns)`, VLog is automatically enabled and configured appropriately.

**Important:** When versioning is enabled without the B+tree index, timestamps inserted "back in time" (earlier than existing timestamps) will not be read correctly. This is because the LSM tree orders entries by user key ascending and sequence number descending, not by timestamp.

If you need to insert historical data with earlier timestamps, enable the B+tree versioned index with `with_versioned_index(true)`. The B+tree allows in-place updates and correctly handles out-of-order timestamp inserts.

## Transaction Operations

### Basic Operations

```rust
use surrealkv::TreeBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tree = TreeBuilder::new()
        .with_path("path/to/db".into())
        .build()?;

    // Write Transaction
    {
        let mut txn = tree.begin()?;
        
        // Set multiple key-value pairs
        txn.set(b"foo1", b"bar1")?;
        txn.set(b"foo2", b"bar2")?;
        
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

**Note:** The transaction API accepts flexible key and value types through the `IntoBytes` trait. You can use `&[u8]`, `&str`, `String`, `Vec<u8>`, or `Bytes` for both keys and values.

### Transaction Modes

SurrealKV supports three transaction modes for different use cases:

```rust
use surrealkv::Mode;

// Read-write transaction (default)
let mut txn = tree.begin()?;

// Read-only transaction - prevents any writes
let txn = tree.begin_with_mode(Mode::ReadOnly)?;

// Write-only transaction - optimized for writes, no reads allowed
let mut txn = tree.begin_with_mode(Mode::WriteOnly)?;
```

### Range Operations

Range operations use a cursor-based iterator API for efficient iteration over key ranges:

```rust
let txn = tree.begin()?;

// Range scan between keys (inclusive start, exclusive end)
let mut iter = txn.range(b"key1", b"key5")?;
iter.seek_first()?;
while iter.valid() {
    let key = iter.key();
    let value = iter.value()?;
    println!("{:?} = {:?}", key, value);
    iter.next()?;
}

// Backward iteration
let mut iter = txn.range(b"key1", b"key5")?;
iter.seek_last()?;
while iter.valid() {
    let key = iter.key();
    let value = iter.value()?;
    println!("{:?} = {:?}", key, value);
    iter.prev()?;
}

// Delete a key
let mut txn = tree.begin()?;
txn.delete(b"key1")?;
txn.commit().await?;
```

**Note:** Range iterators support both forward (`next()`) and backward (`prev()`) iteration using the cursor-based API.

### Durability Levels

Control the durability guarantees for your transactions:

```rust
use surrealkv::Durability;

let mut txn = tree.begin()?;

// Eventual durability (default) - faster, data written to OS buffer
txn.set_durability(Durability::Eventual);

// Immediate durability - slower, fsync before commit returns
txn.set_durability(Durability::Immediate);

txn.set(b"key", b"value")?;
txn.commit().await?;
```

**Durability Levels:**
- `Eventual`: Commits are guaranteed to be persistent eventually. Data is written to the kernel buffer but not fsynced before returning from `commit()`. This is the default and provides the best performance.
- `Immediate`: Commits are guaranteed to be persistent as soon as `commit()` returns. Data is fsynced to disk before returning. This is slower but provides the strongest durability guarantees.


## Time-Travel Queries

Time-travel queries allow you to read historical versions of your data at specific points in time.

### Enabling Versioning

```rust
use surrealkv::{Options, TreeBuilder};

let opts = Options::new()
    .with_path("path/to/db".into())
    .with_versioning(true, 0);  // retention_ns = 0 means no retention limit

let tree = TreeBuilder::with_options(opts).build()?;
```

### Writing Versioned Data

```rust
// Write data with explicit timestamps
let mut tx = tree.begin()?;
tx.set_at(b"key1", b"value_v1", 100)?;
tx.commit().await?;

// Update with a new version at a later timestamp
let mut tx = tree.begin()?;
tx.set_at(b"key1", b"value_v2", 200)?;
tx.commit().await?;
```

### Point-in-Time Reads

Query data as it existed at a specific timestamp:

```rust
let tx = tree.begin()?;

// Get value at specific timestamp
let value = tx.get_at(b"key1", 100)?;
assert_eq!(value.unwrap().as_ref(), b"value_v1");

// Get value at later timestamp
let value = tx.get_at(b"key1", 200)?;
assert_eq!(value.unwrap().as_ref(), b"value_v2");
```

### Retrieving All Versions

Use the unified `history()` API to iterate over all historical versions of keys in a range.
This API uses streaming iteration (no memory collection) and works with both LSM and B+tree backends:

```rust
let tx = tree.begin()?;
let mut iter = tx.history(b"key1", b"key2")?;

iter.seek_first()?;
while iter.valid() {
    let key = iter.key();
    let timestamp = iter.timestamp();

    if iter.is_tombstone() {
        println!("Key {:?} deleted at timestamp {}", key, timestamp);
    } else {
        let value = iter.value()?;
        println!("Key {:?} = {:?} at timestamp {}", key, value, timestamp);
    }
    iter.next()?;
}
```

For more control, use `history_with_options()`:

```rust
use surrealkv::HistoryOptions;

let tx = tree.begin()?;
let opts = HistoryOptions::new()
    .with_tombstones(true)  // Include deleted entries
    .with_limit(100);       // Limit to 100 unique keys

let mut iter = tx.history_with_options(b"key1", b"key2", &opts)?;
iter.seek_first()?;
while iter.valid() {
    let key = iter.key();
    let timestamp = iter.timestamp();
    if iter.is_tombstone() {
        println!("Key {:?} deleted at timestamp {}", key, timestamp);
    } else {
        let value = iter.value()?;
        println!("Key {:?} = {:?} at timestamp {}", key, value, timestamp);
    }
    iter.next()?;
}
```

## Advanced Read Options

Use `ReadOptions` for fine-grained control over read operations:

```rust
use surrealkv::ReadOptions;

let tx = tree.begin()?;

// Range query with bounds using setter methods
let mut options = ReadOptions::new();
options.set_iterate_lower_bound(Some(b"a".to_vec()));
options.set_iterate_upper_bound(Some(b"z".to_vec()));

// Use cursor-based iteration
let mut iter = tx.range_with_options(&options)?;
iter.seek_first()?;
while iter.valid() {
    let key = iter.key();
    let value = iter.value()?;
    println!("{:?} = {:?}", key, value);
    iter.next()?;
}

// Point-in-time read (requires versioning enabled)
let value = tx.get_at(b"key1", 12345)?;
```

## Checkpoint and Restore

Create consistent point-in-time snapshots of your database for backup and recovery.

### Creating Checkpoints

```rust
let tree = TreeBuilder::new()
    .with_path("path/to/db".into())
    .build()?;

// Insert some data
let mut txn = tree.begin()?;
txn.set(b"key1", b"value1")?;
txn.set(b"key2", b"value2")?;
txn.commit().await?;

// Create checkpoint
let checkpoint_dir = "path/to/checkpoint";
let metadata = tree.create_checkpoint(&checkpoint_dir)?;

println!("Checkpoint created at timestamp: {}", metadata.timestamp);
println!("Sequence number: {}", metadata.sequence_number);
println!("SSTable count: {}", metadata.sstable_count);
println!("Total size: {} bytes", metadata.total_size);
```

### Restoring from Checkpoint

```rust
// Restore database to checkpoint state
tree.restore_from_checkpoint(&checkpoint_dir)?;

// Data is now restored to the checkpoint state
// Any data written after checkpoint creation is discarded
```

**What's included in a checkpoint:**
- All SSTables from all levels
- Current WAL segments
- Level manifest
- VLog directories (if VLog is enabled)
- Checkpoint metadata

**Note:** Restoring from a checkpoint discards any pending writes in the active memtable and returns the database to the exact state when the checkpoint was created.

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

- **Better Scalability**: Supports datasets much larger than available memory
- **Leveled Compaction**: Score-based compaction strategy for efficient space utilization

This architectural change enables SurrealKV to handle larger than memory datasets.

For detailed architecture documentation, see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## References
SurrealKV draws inspiration from design ideas used in [RocksDB](https://github.com/facebook/rocksdb) and [Pebble](https://github.com/cockroachdb/pebble)


## License

Licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.
