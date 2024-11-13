# SurrealKV

[![License](https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square)](https://github.com/surrealdb/surrealkv)

SurrealKV is a versioned, low-level, persistent, embedded key-value database implemented in Rust. It provides ACID compliance, rich transaction support, and efficient concurrent operations through Multi-Version Concurrency Control (MVCC).

## Features

- ✨ **ACID Compliance**: Full support for Atomicity, Consistency, Isolation, and Durability
- 🔄 **Rich Transaction Support**: Atomic operations for multiple inserts, updates, and deletes
- 🔒 **Isolation Levels**: Choose between Snapshot Isolation and Serializable Snapshot Isolation
- 💾 **Durability Guaranteed**: Persistent storage with protection against system failures
- 📦 **Embedded Database**: Easily integrate into your Rust applications
- 🔄 **MVCC Support**: Non-blocking concurrent reads and writes using [immutable versioned adaptive radix trie](https://github.com/surrealdb/vart)
- 📚 **Built-in Versioning**: Track and access historical versions of your data
- 🗜️ **Compaction**: Efficient storage management through compaction

## Quick Start

```rust
use surrealkv::{Store, Options, Bytes};

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
txn.commit().await.unwrap();
```

## Configuration

SurrealKV can be configured through various options when creating a new store:

```rust
let mut opts = Options::new();

// Required configuration
opts.dir = "path/to/db".into();                    // Database directory path

// Storage configuration
opts.disk_persistence = true;                       // false for in-memory only operation
opts.max_value_threshold = 4096;                    // Values larger than this stored in separate files
opts.max_segment_size = 268_435_456;               // 256MB default segment size
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
- `max_value_threshold`: Values exceeding this size are stored in separate files for better memory management
- `max_segment_size`: Controls when new log segments are created, affects compaction frequency
- `max_compaction_segment_size`: Maximum size of segments after compaction

### Transaction Options

- `isolation_level`: Choose between Snapshot Isolation and Serializable Snapshot Isolation
- `enable_versions`: Toggle version tracking functionality, disable for pure key-value usage

### Performance Options

- `max_value_cache_size`: Controls the size of value cache, affects read performance for frequently accessed values

## Transaction Operations

### Basic Operations

```rust
use surrealkv::{Store, Options, Bytes};

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
    txn.commit().await.unwrap();
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
store.close().await.unwrap();
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

- `Eventual`: Commits with this durability level are guaranteed to be persistent eventually. The data is written to the disk, but it is not fsynced before returning from `Transaction::commit`. This is the default durability level.
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
   - Time complexity for operations:
     * Insert: O(L) where L is key length in bytes
     * Search: O(L) where L is key length in bytes
     * Delete: O(L) where L is key length in bytes

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
   - Memory usage during recovery proportional to key count


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
     * Uses CRC32 and record fields to detect truncated writes
     * Records are self-validating through header checksum metadata
     * System identifies and truncates incomplete records during recovery
     * Transaction logs are recovered to the last valid record boundary


4. **Operational Advantages**
   - Backup operations can occur during live operation
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
   - Recovery time increases with log size
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
- Random vs Sequential key distribution has minimal impact on load times (~10% difference)
- Load performance is primarily bound by I/O throughput

Key observations:
- Load time scales roughly linearly with store size
- Key and value size impact load time because each record's checksum is calculated based on their bytes, so an increase in size leads to an increase in time. However, the insertion into the index only stores the value offset against the key, which does not significantly affect load time.
- Load performance is primarily bound by I/O throughput
- Random vs Sequential key distribution has minimal impact on load times (typically <15% difference)
- At larger store sizes (>15GB), load times remain predictable and proportional


#### Impact of Version Count

This benchmark demonstrates how the number of versions affects load time while maintaining a constant total entry count:

| Versions | Keys    | Load Time (s) | Store Size (MB) |
|----------|---------|---------------|-----------------|
| 10       | 100,000 | 1.01         | 1,251.22       |
| 100      | 10,000  | 0.97         | 1,251.22       |
| 1,000    | 1,000   | 1.10         | 1,251.22       |

Key observations:
- Version count has minimal impact on load time when total data size remains constant
- Memory usage scales with number of unique keys rather than total versions




## License

Licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.