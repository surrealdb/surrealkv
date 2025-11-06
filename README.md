# SurrealKV

[![License](https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square)](https://github.com/surrealdb/surrealkv)

> **⚠️ Development Status**: SurrealKV is currently under active development and is not feature complete. The API and implementation may change significantly between versions. Use with caution in production environments.

SurrealKV is a versioned, low-level, persistent, embedded key-value database implemented in Rust using an LSM (Log-Structured Merge) tree architecture with built-in support for time-travel queries.

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

### Value Log Configuration

The Value Log (VLog) separates large values from the LSM tree for more efficient storage and compaction.

```rust
let tree = TreeBuilder::new()
    .with_path("path/to/db".into())
    .with_enable_vlog(true)                    // Enable VLog
    .with_vlog_max_file_size(256 * 1024 * 1024) // 256MB VLog file size
    .with_vlog_gc_discard_ratio(0.5)           // Trigger GC at 50% garbage
    .with_vlog_checksum_verification(VLogChecksumLevel::Full)
    .build()?;
```

**Options:**
- `with_enable_vlog()` - Enable/disable Value Log for large value storage
- `with_vlog_max_file_size()` - Maximum size of VLog files before rotation
- `with_vlog_gc_discard_ratio()` - Threshold (0.0-1.0) for triggering VLog garbage collection
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

Range operations support efficient iteration over key ranges with optional limits:

```rust
// Range scan between keys (inclusive start, exclusive end)
let mut txn = tree.begin()?;
let range: Vec<_> = txn.range(b"key1", b"key5", None)?
    .map(|r| r.unwrap())
    .collect();

// Keys-only scan (faster, doesn't fetch values when vlog is enabled)
let keys: Vec<_> = txn.keys(b"key1", b"key5", None)?
    .map(|r| r.unwrap())
    .collect();

// Range with limit
let limited: Vec<_> = txn.range(b"key1", b"key9", Some(10))?
    .map(|r| r.unwrap())
    .collect();

// Delete a key
txn.delete(b"key1")?;
txn.commit().await?;
```

**Note:** Range iterators are double-ended, supporting both forward and backward iteration.

### Counting Keys

Efficiently count keys in a range without iterating through all values:

```rust
let mut txn = tree.begin()?;

// Count all keys between "key1" and "key9"
let count = txn.count(b"key1", b"key9", None)?;
println!("Found {} keys", count);

// Count with a limit (useful for "at least N" checks)
let count = txn.count(b"key1", b"key9", Some(100))?;
println!("Found at least {} keys (limited to 100)", count.min(100));

// Count with custom options (limit, bounds, etc.)
let options = ReadOptions::new()
    .with_limit(Some(100))
    .with_iterate_lower_bound(Some(b"a".to_vec()))
    .with_iterate_upper_bound(Some(b"z".to_vec()));
let count = txn.count_with_options(&options)?;
```

**Note:** The `count()` operation is optimized and more efficient than manually counting iterator results, as it doesn't need to fetch or resolve values from the value log. The optional `limit` parameter is useful for checking if at least N items exist without counting all items.

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
tx.set_at_version(b"key1", b"value_v1", 100)?;
tx.commit().await?;

// Update with a new version at a later timestamp
let mut tx = tree.begin()?;
tx.set_at_version(b"key1", b"value_v2", 200)?;
tx.commit().await?;
```

### Point-in-Time Reads

Query data as it existed at a specific timestamp:

```rust
let tx = tree.begin()?;

// Get value at specific timestamp
let value = tx.get_at_version(b"key1", 100)?;
assert_eq!(value.unwrap().as_ref(), b"value_v1");

// Get value at later timestamp
let value = tx.get_at_version(b"key1", 200)?;
assert_eq!(value.unwrap().as_ref(), b"value_v2");

// Range query at specific timestamp
let range: Vec<_> = tx.range_at_version(b"key1", b"key9", 150, None)?
    .map(|r| r.unwrap())
    .collect();

// Keys-only query at timestamp (faster, when vlog enabled)
let keys: Vec<_> = tx.keys_at_version(b"key1", b"key9", 150, None)?
    .map(|r| r.unwrap())
    .collect();

// Count keys at a specific timestamp
let count = tx.count_at_version(b"key1", b"key9", 150, None)?;
println!("Found {} keys at timestamp 150", count);

// Count with limit at timestamp
let count = tx.count_at_version(b"key1", b"key9", 150, Some(100))?;
println!("Found at least {} keys at timestamp 150", count.min(100));
```

### Retrieving All Versions

Get all historical versions of keys in a range:

```rust
let tx = tree.begin()?;
let versions = tx.scan_all_versions(b"key1", b"key2", None)?;

for (key, value, timestamp, is_tombstone) in versions {
    if is_tombstone {
        println!("Key {:?} deleted at timestamp {}", key, timestamp);
    } else {
        println!("Key {:?} = {:?} at timestamp {}", key, value, timestamp);
    }
}
```

## Advanced Read Options

Use `ReadOptions` for fine-grained control over read operations:

```rust
use surrealkv::ReadOptions;

let tx = tree.begin()?;

// Range query with limit and bounds
let options = ReadOptions::new()
    .with_limit(Some(10))
    .with_iterate_lower_bound(Some(b"a".to_vec()))
    .with_iterate_upper_bound(Some(b"z".to_vec()));

let results: Vec<_> = tx.range_with_options(&options)?
    .map(|r| r.unwrap())
    .collect();

// Keys-only iteration (faster, doesn't fetch values from disk when vlog is enabled)
let options = ReadOptions::new()
    .with_keys_only(true)
    .with_limit(Some(100))
    .with_iterate_lower_bound(Some(b"a".to_vec()))
    .with_iterate_upper_bound(Some(b"z".to_vec()));

let keys: Vec<_> = tx.keys_with_options(&options)?
    .map(|r| r.unwrap())
    .collect();

// Point-in-time read with options (requires versioning enabled)
let options = ReadOptions::new()
    .with_timestamp(Some(12345))
    .with_limit(Some(50))
    .with_iterate_lower_bound(Some(b"a".to_vec()))
    .with_iterate_upper_bound(Some(b"z".to_vec()));

let historical_data: Vec<_> = tx.range_with_options(&options)?
    .map(|r| r.unwrap())
    .collect();
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

- **Compaction**: Leveled compaction strategy for space utilization
- **Better Scalability**: Supports datasets much larger than available memory

This architectural change enables SurrealKV to handle larger then memory datasets.

## License

Licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.
