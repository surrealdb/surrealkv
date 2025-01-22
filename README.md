# SurrealKV

[![License](https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square)](https://github.com/surrealdb/surrealkv)

surrealkv is a versioned, low-level, persistent, embedded key-value database implemented in Rust. It offers the following features:

## Features

- ✨ **ACID Compliance**: Full support for Atomicity, Consistency, Isolation, and Durability
- 🔄 **Rich Transaction Support**: Atomic operations for multiple inserts, updates, and deletes
- 🔒 **Isolation Levels**: Choose between Snapshot Isolation and Serializable Snapshot Isolation
- 💾 **Durability Guaranteed**: Persistent storage with protection against system failures
- 📦 **Embedded Database**: Easily integrate into your Rust applications
- 🔄 **MVCC Support**: Non-blocking concurrent reads and writes using [versioned adaptive radix trie](https://github.com/surrealdb/vart)
- 📚 **Built-in Versioning**: Track and access historical versions of your data
- 🗜️ **Compaction**: Efficient storage management through compaction

## Quick Start

```rust
use surrealkv::{Store, Options};
use bytes::Bytes;

// Create a new store
let mut opts = Options::new();
opts.dir = "path/to/db".into();
let store = Store::new(opts).expect("failed to create store");

// Start a read-write transaction
let mut txn = store.begin().unwrap();

// Set some key-value pairs
let key = Bytes::from("hello");
let value = Bytes::from("world");
txn.set(&key, &value).unwrap();

// Commit the transaction
txn.commit().unwrap();
```

## Configuration

SurrealKV can be configured through various options when creating a new store:

```rust
let mut opts = Options::new();

// Required configuration
opts.dir = "path/to/db".into();                    // Database directory path

// Storage configuration
opts.disk_persistence = true;                       // false for in-memory only operation
opts.max_value_threshold = 4096;                    // Values smaller than this stored in memory
opts.max_segment_size = 268_435_456;               // 256MB segment size
opts.max_compaction_segment_size = 1_073_741_824;  // 1GB max compaction segment

// Transaction and versioning
opts.isolation_level = IsolationLevel::Snapshot;    // Controls transaction isolation
opts.enable_versions = true;                        // Enable/disable versioning

// Cache settings
opts.max_value_cache_size = 67_108_864;            // 64MB value cache size

let store = Store::new(opts).expect("failed to create store");
```

### Storage Options

- `disk_persistence`: Controls whether data is persisted to disk or kept only in memory
- `max_value_threshold`: Values within this size are stored and served directly from memory
- `max_segment_size`: Controls when new log segments are created, affects compaction frequency

### Transaction Options

- `isolation_level`: Choose between Snapshot Isolation and Serializable Snapshot Isolation
- `enable_versions`: Toggle version tracking functionality, disable for pure key-value usage

### Performance Options

- `max_value_cache_size`: Controls the size of value cache, affects read performance for frequently accessed values

## Transaction Operations

### Basic Operations

```rust
use surrealkv::{Store, Options};
use bytes::Bytes;

// Initialize the store
let mut opts = Options::new();
opts.dir = "path/to/db".into();
let store = Store::new(opts).expect("failed to create store");

// Write Transaction
{
    let mut txn = store.begin().unwrap();
    
    // Set multiple key-value pairs
    let key1 = Bytes::from("foo1");
    let key2 = Bytes::from("foo2");
    let value = Bytes::from("bar");
    
    txn.set(&key1, &value).unwrap();
    txn.set(&key2, &value).unwrap();
    
    // Commit changes
    txn.commit().unwrap();
}

// Read Transaction
{
    let mut txn = store.begin().unwrap();
    
    let key = Bytes::from("foo1");
    if let Some(value) = txn.get(&key).unwrap() {
        println!("Value: {:?}", value);
    }
}

// Close the store when done
store.close().unwrap();
```

### Versioned Operations

```rust
// Get value at specific timestamp
let value = tx.get_at_ts(b"key1", timestamp)?;

// Get complete history of a key
let history = tx.get_history(b"key1")?;

// Scan range at specific timestamp
let range = b"start"..b"end";
let results = tx.scan_at_ts(range, timestamp, Some(10))?;
```

### Transaction Control

```rust
// Set a savepoint
tx.set_savepoint()?;

// Make some changes
tx.set(b"key", b"value")?;

// Rollback to savepoint if needed
tx.rollback_to_savepoint()?;

// Or rollback entire transaction
tx.rollback();
```

### Range Operations

```rust
// Scan a range of keys
let range = b"start"..b"end";
let results = tx.scan(range, Some(10))?;

// Scan all versions in a range
let all_versions = tx.scan_all_versions(range, Some(10))?;
```

## Advanced Features

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

### Custom Queries

```rust
// Use custom query types for specific lookups
let result = tx.get_value_by_query(&key, QueryType::LatestByTs)?;
```

## Implementation Details

### Architecture

SurrealKV implements a two-component architecture:

1. **Index Component**
   - In-memory versioned adaptive radix trie using [vart](https://github.com/surrealdb/vart)
   - Stores key-to-offset mappings for each version of the key

2. **Log Component**
   - Sequential append-only storage divided into segments
   - Each segment is a separate file with a monotonically increasing ID
   - Active segment receives all new writes
   - Older segments are immutable and candidates for compaction
   - Records stored in the binary format described below
   - Sequential writes for optimal write performance
   - No in-place updates

### Data Operations

1. **Write Path**
   - Serialize record in binary format
   - Append to log file
   - Update index with new offset

2. **Read Path**
   - Query index for file offset
   - Seek to file position
   - Deserialize record

3. **Compaction Process**
   - Identify obsolete records
   - Copy valid records to new file
   - Update index references
   - Remove old log file

4. **Recovery Process**
   - Sequential scan of all log segments during startup
   - Reconstruction of in-memory index from log entries
   - Startup time directly proportional to:
     * Total size of all segments
     * Number of unique keys and versions


### Storage Format

SurrealKV stores records on disk in a strictly defined binary format:

```
Record Layout:
|----------|------------|------------|---------|-----------------|------------|------------|-----|--------------|-------|
| crc32(4) | version(2) | tx_id(8)   | ts(8)   | metadata_len(2) | metadata   | key_len(4) | key | value_len(4) | value |
|----------|------------|------------|---------|-----------------|------------|------------|-----|--------------|-------|
```

Each field serves a specific purpose:
- `crc32`: 4-byte checksum for data integrity verification
- `version`: 2-byte format version identifier
- `tx_id`: 8-byte transaction identifier
- `ts`: 8-byte timestamp
- `metadata_len`: 2-byte length of metadata section
- `metadata`: Variable-length metadata
- `key_len`: 4-byte length of key
- `key`: Variable-length key data
- `value_len`: 4-byte length of value
- `value`: Variable-length value data


### MVCC Implementation

The Multi-Version Concurrency Control system allows:
- Multiple concurrent readers without blocking
- Multiple concurrent writers without blocking
- Snapshot isolation for consistent reads


## Performance Characteristics and Trade-offs

### Strengths

1. **Latency Characteristics**
   - Constant-time retrieval operations due to direct offset lookups
   - Write latency bound by sequential I/O performance
   - Minimal disk seeks during normal operation

2. **Throughput Properties**
   - Sequential write patterns maximize I/O bandwidth utilization
   - Concurrent read operations scale with available CPU cores
   - Range queries benefit from trie's prefix-based organization

3. **Recovery Semantics**
   - Initial startup requires full segment scan to rebuild index
   - Recovery time proportional to total size of all segments
   - Repair time proportional to size of last active segment
   - CRC verification ensures data integrity during recovery
   - Partial write detection:
     * Uses CRC32 calculated from the record fields to detect truncated writes
     * Identifies and truncates incomplete records during recovery
     * Transaction logs are recovered to the last valid record boundary


4. **Operational Advantages**
   - Compaction process runs concurrently with normal operations
   - Append-only format simplifies replication procedures

### Limitations

1. **Memory Requirements**
   - Index must reside in memory
   - Memory usage scales with:
     * Number of unique keys
     * Key size distribution
     * Number of versions per key

2. **Write Amplification**
   - Each update creates new version
   - Requires periodic compaction
   - Space usage temporarily increases during compaction

3. **Range Query Performance**
   - Performance dependent on:
     * Key distribution
     * Version history depth
     * Range size
   - May require multiple disk reads for large ranges

4. **Operational Considerations**
   - Compaction necessary for space reclamation
   - Restart time increases with log size
   - Memory pressure in high-cardinality keyspaces

### Performance Implications

1. **Optimal Use Cases**
   - Write-intensive workloads
   - Point query dominated patterns
   - Prefix-based access patterns
   - Time-series data with version tracking

2. **Suboptimal Scenarios**
   - Memory-constrained environments
   - Very large key spaces
   - Scan-heavy workloads
   - Random updates to large datasets

## Benchmarks

### Key-Value Operations Performance

The following benchmarks measure single-operation latency across different key and value sizes.

#### Sequential Insert Performance (μs)

| Value Size | Key Size (bytes) ||||
|------------|-----------|-----------|-----------|-----------|
| (bytes)    | 8         | 32        | 128       | 256       |
|------------|-----------|-----------|-----------|-----------|
| 8          | 16.57     | 15.60     | 16.19     | 16.61     |
| 256        | 16.44     | 16.21     | 16.82     | 16.60     |
| 1024       | 17.71     | 18.12     | 18.01     | 18.17     |
| 4096       | 25.47     | 26.67     | 26.92     | 26.55     |

#### Random Insert Performance (μs)

| Value Size | Key Size (bytes) ||||
|------------|-----------|-----------|-----------|-----------|
| (bytes)    | 8         | 32        | 128       | 256       |
|------------|-----------|-----------|-----------|-----------|
| 8          | 19.55     | 19.01     | 20.84     | 19.99     |
| 256        | 20.07     | 19.33     | 21.58     | 21.16     |
| 1024       | 20.18     | 20.60     | 22.65     | 20.45     |
| 4096       | 24.08     | 22.73     | 24.39     | 23.24     |

#### Range Scan Performance

| Number of Keys | Key Size (bytes) | Value Size (bytes) | Latency (μs)
|---------------|------------------|-------------------|--------------|
| 100           | 4 (u32)          | 100               | 7.01         |
| 1,000         | 4 (u32)          | 100               | 71.92        |
| 10,000        | 4 (u32)          | 100               | 823.29       |

#### Concurrent Operations

##### Multi-threaded Insert Performance
Configuration:
- Key size: 16 bytes
- Value size: 32 bytes
- Dataset size: 5MB

| Thread Count | Latency (ms) | Throughput (K ops/sec) |
|--------------|-------------|----------------------|
| 1            | 1,055.6     | 94.7                |
| 2            | 739.3       | 135.3               |
| 4            | 589.8       | 169.6               |


All benchmarks were performed with:
- Durability: Eventual
- Disk persistence: Enabled


### Startup Performance

SurrealKV rebuilds its index from log segments during startup. The following benchmarks demonstrate how different factors affect startup performance.

#### Impact of Key-Value Sizes on Load Time

This benchmark shows how different key-value size combinations affect load time and storage size (1M entries each):

| Key Size | Value Size | Distribution | Load Time (s) | Store Size (GB) |
|----------|------------|--------------|---------------|-----------------|
| 32       | 64        | Sequential   | 0.61         | 0.12           |
| 32       | 64        | Random       | 0.70         | 0.12           |
| 32       | 256       | Sequential   | 0.74         | 0.30           |
| 32       | 256       | Random       | 0.83         | 0.30           |
| 32       | 1024      | Sequential   | 1.13         | 1.01           |
| 32       | 1024      | Random       | 1.43         | 1.01           |
| 32       | 4096      | Sequential   | 2.85         | 3.87           |
| 32       | 4096      | Random       | 2.82         | 3.87           |
| 32       | 16384     | Sequential   | 8.63         | 15.32          |
| 32       | 16384     | Random       | 8.99         | 15.32          |
| 32       | 65536     | Sequential   | 31.04        | 61.09          |
| 32       | 65536     | Random       | 31.79        | 61.09          |
| 128      | 64        | Sequential   | 0.63         | 0.21           |
| 128      | 64        | Random       | 0.64         | 0.21           |
| 128      | 256       | Sequential   | 0.68         | 0.39           |
| 128      | 256       | Random       | 0.81         | 0.39           |
| 128      | 1024      | Sequential   | 1.10         | 1.10           |
| 128      | 1024      | Random       | 1.31         | 1.10           |
| 128      | 4096      | Sequential   | 2.95         | 3.96           |
| 128      | 4096      | Random       | 3.01         | 3.96           |
| 128      | 16384     | Sequential   | 8.67         | 15.41          |
| 128      | 16384     | Random       | 8.91         | 15.41          |
| 128      | 65536     | Sequential   | 31.36        | 61.18          |
| 128      | 65536     | Random       | 31.47        | 61.18          |
| 256      | 64        | Sequential   | 0.73         | 0.33           |
| 256      | 64        | Random       | 0.71         | 0.33           |
| 256      | 256       | Sequential   | 0.77         | 0.51           |
| 256      | 256       | Random       | 0.91         | 0.51           |
| 256      | 1024      | Sequential   | 1.22         | 1.22           |
| 256      | 1024      | Random       | 1.29         | 1.22           |
| 256      | 4096      | Sequential   | 3.11         | 4.08           |
| 256      | 4096      | Random       | 3.03         | 4.08           |
| 256      | 16384     | Sequential   | 8.81         | 15.53          |
| 256      | 16384     | Random       | 9.12         | 15.53          |
| 256      | 65536     | Sequential   | 31.42        | 61.30          |
| 256      | 65536     | Random       | 32.66        | 61.30          |

Key observations:
- Load time scales roughly linearly with store size
- Key and value size impact load time because each record's checksum is calculated based on their bytes, so an increase in size leads to an increase in time to calculate the checksum. However, the insertion into the index only stores the value offset against the key, which does not significantly affect load time.


#### Impact of Version Count

This benchmark demonstrates how the number of versions affects load time while maintaining a constant total entry count:

| Versions | Keys    | Load Time (s) | Store Size (MB) |
|----------|---------|---------------|-----------------|
| 10       | 100,000 | 1.01         | 1,251.22       |
| 100      | 10,000  | 0.97         | 1,251.22       |
| 1,000    | 1,000   | 1.10         | 1,251.22       |

Key observations:
- Version count has minimal impact on load time when total data size remains constant


## License

Licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.