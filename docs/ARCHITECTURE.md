# [WIP] SurrealKV Architecture

SurrealKV is a versioned, embedded key-value database built on an LSM (Log-Structured Merge) tree architecture. It is designed specifically for use within SurrealDB, with the goal of reducing dependency on external storage engine (RocksDB). This approach allows the storage layer to evolve in alignment with SurrealDB’s requirements and access patterns.

This document provides a comprehensive overview of the internal design, data flow, and key subsystems.

## Table of Contents

1. [Overall Architecture](#overall-architecture)
2. [Write Path](#write-path)
3. [Read Path](#read-path)
4. [Versioning and MVCC](#versioning-and-mvcc)
5. [Compaction](#compaction)
6. [Value Log (VLog)](#value-log-vlog)
7. [VLog Garbage Collection](#vlog-garbage-collection)
8. [Recovery](#recovery)
9. [Checkpoint and Restore](#checkpoint-and-restore)

---

## Overall Architecture

SurrealKV is organized into four main layers: the Client API, Core Components, Storage Layer, and Background Tasks.

### Data Flow

Data flows through the layers as follows:

1. **Writes**: Client -> Transaction (buffer) -> CommitPipeline -> WAL + MemTable -> Background Flush -> SSTables
2. **Reads**: Client -> Transaction -> Snapshot -> MemTable(s) -> SSTables -> (VLog if pointer)
3. **Compaction**: Background task merges SSTables, respecting snapshot visibility, updating VLog discard stats

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CLIENT LAYER                                   │
│  ┌─────────────┐      ┌─────────────────────────────────────────────────┐   │
│  │    Tree     │──────│                  Transaction                    │   │
│  │    API      │      │  ┌─────────┐  ┌─────────┐  ┌─────────────────┐  │   │
│  └─────────────┘      │  │write_set│  │ get()   │  │ commit()        │  │   │
│                       │  │ buffer  │  │ set()   │  │ rollback()      │  │   │
│                       │  └─────────┘  └─────────┘  └─────────────────┘  │   │
│                       └─────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                              CORE COMPONENTS                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │   Oracle     │  │   Commit     │  │   Active     │  │  Immutable   │    │
│  │  (conflict   │  │   Pipeline   │  │  MemTable    │  │  MemTables   │    │
│  │  detection)  │  │ (lock-free)  │  │  (skiplist)  │  │   (queue)    │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
│         │                 │                 │                 │            │
│         └────────────────-┼─────────────────┼─────────────────┘             │
│                          ▼                 ▼                               │
│                    ┌──────────────────────────────────────┐                │
│                    │           Snapshot                   │                │
│                    │   (point-in-time consistent view)    │                │
│                    └──────────────────────────────────────┘                │
└────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                              STORAGE LAYER                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
│  │     WAL      │  │   SSTables   │  │  Value Log   │  │   Manifest   │    │
│  │  (durability)│  │   (L0..Ln)   │  │  (WiscKey)   │  │   (levels)   │    │
│  │  32KB blocks │  │  sorted runs │  │ large values │  │  table meta  │    │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                            BACKGROUND TASKS                                │
│  ┌──────────────────┐  ┌───────────────────┐  ┌──────────────────┐         │
│  │  MemTable Flush  │  │ Level Compaction  │  │     VLog GC      │         │
│  │  (imm -> L0)     │  │ (merge SSTables)  │  │ (reclaim space)  │         │
│  └──────────────────┘  └───────────────────┘  └──────────────────┘         │
└────────────────────────────────────────────────────────────────────────────┘
```

### Layer Details

**Client Layer**: The `Tree` struct is the main entry point, providing methods to begin transactions, create checkpoints, and manage the database lifecycle. Each `Transaction` maintains its own write buffer (`write_set`) for staging changes before commit, enabling atomic multi-key updates and rollback support.

**Core Components**: These in-memory structures coordinate between the client API and persistent storage:
- The **Oracle** assigns monotonically increasing sequence numbers and detects write-write conflicts between concurrent transactions
- The **CommitPipeline** queues pending commits and ensures WAL writes are serialized while allowing concurrent memtable updates
- The **MemTable** is a lock-free skip list that holds recent writes before they are flushed to disk
- **Snapshots** capture the visible sequence number at transaction start, providing isolation from concurrent commits

**Storage Layer**: All durable state lives here:
- The **WAL** ensures committed data survives crashes by persisting entries before they become visible
- **SSTables** are immutable sorted files organized into levels, with Level 0 containing recently flushed data
- The **VLog** stores large values separately, reducing SSTable size and compaction cost
- The **Manifest** tracks which SSTables exist at each level and their key ranges

**Background Tasks**: These run asynchronously to maintain system health:
- **Flush** converts immutable memtables into Level 0 SSTables
- **Compaction** merges overlapping SSTables to reduce read amplification and reclaim space
- **VLog GC** removes stale entries from value log files when their discard ratio exceeds a threshold

### Component Overview

| Component | Purpose | Key Files |
|-----------|---------|-----------|
| **Tree** | Public API for database operations | `src/lsm.rs` |
| **Transaction** | ACID transaction with MVCC | `src/transaction.rs` |
| **Oracle** | Transaction timestamp and conflict detection | `src/oracle.rs` |
| **CommitPipeline** | Lock-free commit queue (inspired by Pebble) | `src/commit.rs` |
| **MemTable** | In-memory skip list for recent writes | `src/memtable/` |
| **Snapshot** | Point-in-time consistent view | `src/snapshot.rs` |
| **WAL** | Write-ahead log for durability | `src/wal/` |
| **SSTable** | Sorted string table on disk | `src/sstable/` |
| **VLog** | Value log for large value separation | `src/vlog.rs` |
| **Manifest** | Level metadata and SSTable tracking | `src/levels/` |

### Directory Structure

```
database_path/
├── wal/                    # Write-ahead log segments
│   ├── 00000000000000000001.wal
│   └── 00000000000000000002.wal
├── sstables/               # SSTable files
│   ├── 00000000000000000001.sst
│   └── 00000000000000000002.sst
├── manifest/               # Level manifest files
│   └── 00000000000000000001.manifest
├── vlog/                   # Value log files (if enabled)
│   ├── 00000000000000000001.vlog
│   └── 00000000000000000002.vlog
├── versioned_index/        # B+tree index (if enabled)
└── LOCK                    # Lock file
```

---

## Write Path

This section covers how data flows from a transaction's `set()` call through to durable storage.

### Overview

A write operation proceeds through three main phases:

1. **Transaction Phase**: The application buffers writes in the transaction's local `write_set`. No locks are held, no disk I/O occurs, and other transactions cannot see these changes.

2. **Commit Phase**: When the application calls `commit()`, the transaction coordinates with the Oracle for conflict detection, assigns sequence numbers, and enters the commit pipeline.

3. **Background Phase**: Asynchronously, full memtables are rotated and flushed to Level 0 SSTables.

The design minimizes contention during concurrent writes. The WAL write is kept as short as possible, while memtable insertion and visibility publishing can proceed in parallel.

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              TRANSACTION PHASE                             │
│                                                                            │
│  txn.set(key, value)  ──►  Buffer in write_set  ──►  txn.commit()          │
│                            (BTreeMap)                                      │
└────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                                 COMMIT PHASE                               │
│                                                                            │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐         │
│  │     Oracle      │───►│  Assign SeqNum  │───►│  Create Batch   │         │
│  │ (conflict check)│    │  (atomic inc)   │    │  (entries)      │         │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘         │
└────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                         COMMIT PIPELINE (Serialized)                       │
│                                                                            │
│  ┌─────────────────┐    ┌─────────────────┐                                │
│  │ Acquire Write   │───►│   Write to WAL  │                                │
│  │     Mutex       │    │  (durability)   │                                │
│  └─────────────────┘    └─────────────────┘                                │
│                                  │                                         │
│                                  ▼                                         │
│                    ┌─────────────────────────────┐                         │
│                    │  Value Size > Threshold?    │                         │
│                    └─────────────────────────────┘                         │
│                         │                  │                               │
│                    Yes  ▼                  ▼  No                           │
│            ┌─────────────────┐    ┌─────────────────┐                      │
│            │ Append to VLog  │    │  Store Inline   │                      │
│            │ Create Pointer  │    │  (in SSTable)   │                      │
│            └─────────────────┘    └─────────────────┘                      │
│                         │                  │                               │
│                         └────────┬─────────┘                               │
│                                  ▼                                         │
│                    ┌─────────────────────────────┐                         │
│                    │   Apply to Active MemTable  │                         │
│                    │       (skip list insert)    │                         │
│                    └─────────────────────────────┘                         │
│                                  │                                         │
│                                  ▼                                         │
│                    ┌─────────────────────────────┐                         │
│                    │  Publish Visibility         │                         │
│                    │  (update visible_seq_num)   │                         │
│                    └─────────────────────────────┘                         │
└────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                               BACKGROUND TASKS                             │
│                                                                            │
│  ┌─────────────────────────┐    ┌─────────────────────────────────────┐    │
│  │  MemTable Full?         │───►│  Rotate: Active → Immutable Queue   │    │
│  │  (size > threshold)     │Yes │  Create new empty Active MemTable   │    │
│  └─────────────────────────┘    └─────────────────────────────────────┘    │
│                                              │                             │
│                                              ▼                             │
│                                 ┌─────────────────────────────────────┐    │
│                                 │  Flush Immutable to L0 SSTable      │    │
│                                 │  (background task, async)           │    │
│                                 └─────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────────┘
```

### Key Implementation Details

**Transaction Buffering**

All writes within a transaction are buffered in a `BTreeMap<Key, Vec<Entry>>` called `write_set`. The BTreeMap maintains keys in sorted order, which enables efficient range scans during read-your-own-writes checks. Multiple writes to the same key within a transaction are stored as a vector, preserving the write order.

This buffering approach provides:
- **Read-your-own-writes (RYOW)**: Uncommitted changes are visible to subsequent reads within the same transaction
- **Savepoint support**: The write set can be partially rolled back to a previous savepoint
- **Atomic commit**: All buffered writes are applied together or not at all

**Commit Pipeline**

The commit pipeline is inspired by [Pebble's commit design](https://github.com/cockroachdb/pebble/blob/master/docs/rocksdb.md#commit-pipeline). The key insight is that while WAL writes must be serialized (to maintain a consistent log order), other commit steps can proceed concurrently.

The pipeline operates in three stages:
1. **Write**: Acquire the write mutex, append entries to WAL, release mutex
2. **Apply**: Insert entries into the active memtable (lock-free skip list)
3. **Publish**: Update `visible_seq_num` to make entries visible to readers

A semaphore limits concurrent commits (default: 8) to prevent memory exhaustion from too many in-flight transactions.

**Value Separation (WiscKey)**

When the Value Log is enabled, the commit path checks each value's size against `vlog_value_threshold` (default: 1KB):

- **Large values** (> threshold): Written to VLog file, a 25-byte `ValuePointer` is stored in the memtable/SSTable
- **Small values** (<= threshold): Stored inline in the memtable/SSTable

This trade-off reduces write amplification during compaction (fewer bytes to merge) at the cost of an extra read for large values. The threshold can be tuned based on workload characteristics.

**Durability Modes**

SurrealKV offers two durability levels:
- **Eventual** (default): Data is written to the OS buffer cache but not fsynced. This is fast but data may be lost if the system crashes before the OS flushes to disk.
- **Immediate**: Each commit calls fsync before returning. This guarantees durability but reduces throughput.

**Sequence Numbers**

Every entry receives a monotonically increasing 56-bit sequence number (the upper 56 bits of the 64-bit trailer). Sequence numbers serve multiple purposes:
- **MVCC visibility**: Readers with snapshot seq=N see only entries with seq <= N
- **Ordering**: During compaction, newer entries supersede older ones with the same key
- **Conflict detection**: The Oracle tracks which keys were written at which sequence numbers

**MemTable Rotation**

When the active memtable exceeds `max_memtable_size`, it is rotated:
1. The current memtable is marked immutable and added to a flush queue
2. A new empty memtable becomes the active one
3. A background task asynchronously flushes immutable memtables to Level 0 SSTables

This rotation is transparent to writers and ensures the active memtable never grows unbounded.

---

## Read Path

The read path describes how data is retrieved from a transaction's `get()` call, traversing all storage layers. Understanding the read path helps explain SurrealKV's performance characteristics and the trade-offs involved in LSM-tree designs.

### Overview

In an LSM tree, reads must check multiple locations because the same key may exist in several places simultaneously:

- The transaction's uncommitted write buffer
- The active memtable (most recent committed writes)
- One or more immutable memtables (pending flush)
- Level 0 SSTables (recently flushed, may overlap)
- Level 1+ SSTables (compacted, non-overlapping)

The search proceeds from newest to oldest, stopping at the first match. This ensures that the most recent version of a key is always returned.

### Read Amplification

**Read amplification** measures how many locations must be checked to find a key. In the worst case (key doesn't exist), a read may check all levels. LSM trees trade write performance for read performance: writes are fast (sequential appends) but reads may require multiple lookups.

SurrealKV minimizes read amplification through:
- **Bloom filters**: Quickly eliminate SSTables that definitely don't contain a key
- **Block cache**: Keep frequently accessed data blocks in memory
- **Non-overlapping levels**: L1+ SSTables don't overlap, enabling binary search

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           TRANSACTION READ                                  │
│                                                                             │
│  txn.get(key)  ──►  Check write_set (RYOW)  ──►  Found? Return immediately  │
│                            │                                                │
│                            ▼ Not in write_set                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SNAPSHOT READ                                     │
│                                                                             │
│  Create Snapshot (seq_num = visible_seq_num at txn start)                   │
│                            │                                                │
│                            ▼                                                │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │ Search Order (stop on first match with seq_num <= snapshot.seq_num): │   │
│  │                                                                      │   │
│  │   1. Active MemTable (newest writes, in-memory skiplist)             │   │
│  │              │                                                       │   │
│  │              ▼                                                       │   │
│  │   2. Immutable MemTables (newest first, pending flush)               │   │
│  │              │                                                       │   │
│  │              ▼                                                       │   │
│  │   3. Level 0 SSTables (ALL tables, may overlap)                      │   │
│  │              │                                                       │   │
│  │              ▼                                                       │   │
│  │   4. Level 1...N SSTables (binary search, non-overlapping)           │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────-───┐
│                         SSTABLE LOOKUP DETAIL                               |
│                                                                             │
│  For each SSTable candidate:                                                │
│                                                                             │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐          │
│  │  Bloom Filter   │───►│  Index Block    │───►│  Data Block     │          │
│  │  (may_contain?) │ Yes│  (binary search)│    │  (with cache)   │          │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘          │
│         │ No                                                                │
│         ▼                                                                   │
│    Skip to next SSTable                                                     │
└─────────────────────────────────────────────────────────────────────────-───┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         VALUE RESOLUTION                                    │
│                                                                             │
│                    ┌─────────────────────────────-┐                         │
│                    │  Is ValueLocation a Pointer? │                         │
│                    └─────────────────────────────-┘                         │
│                         │                  │                                │
│                    Yes  ▼                  ▼  No                            │
│            ┌────────────────-─┐    ┌─────────────────┐                      │
│            │  Read from VLog  │    │ Return Inline   │                      │
│            │  (file_id,offset)│    │    Value        │                      │
│            └─────────────────-┘    └─────────────────┘                      │
│                         │                  │                                │
│                         └────────┬─────────┘                                │
│                                  ▼                                          │
│                         Return Value to Caller                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Implementation Details

**RYOW (Read Your Own Writes)**

Before checking persistent storage, a read first consults the transaction's local `write_set` buffer. This ensures uncommitted changes are visible to the transaction that made them, which is needed for multi-statement transactions that read data they just wrote.

**Snapshot Isolation**

When a transaction begins, it captures the current `visible_seq_num` as its snapshot. All reads within that transaction see a consistent view of the database as of that moment:
- Entries with `seq_num <= snapshot.seq_num` are visible
- Entries with `seq_num > snapshot.seq_num` are invisible (committed after the transaction started)

This provides **repeatable reads**: the same query will return the same results throughout the transaction, regardless of concurrent commits.

**Level 0 vs Level 1+**

Level 0 is special because it contains recently flushed memtables whose key ranges may overlap. When searching L0, **all** SSTables must be checked because any of them might contain the target key.

Levels 1 and above maintain a **sorted, non-overlapping** invariant: each SSTable covers a distinct key range. This enables binary search to find the single SSTable that might contain a key, reducing read amplification for deeper levels.

**Bloom Filters**

Each SSTable includes a Bloom filter, a probabilistic data structure that can definitively say "key is NOT in this table" or "key MIGHT be in this table." With a 1% false positive rate (tunable), Bloom filters eliminate ~99% of unnecessary SSTable reads.

The filter is loaded into memory when the SSTable is opened, so checking it is extremely fast compared to reading from disk.

**Block Cache**

SSTable data is organized into blocks (default: 4KB). When a block is read from disk, it is cached in an LRU (Least Recently Used) cache. Subsequent reads for keys in the same block are served from memory.

The cache key is `(file_id, block_offset)`, ensuring that blocks from different SSTables don't collide.

**VLog Resolution**

When value separation is enabled, large values are not stored directly in SSTables. Instead, a `ValuePointer` (25 bytes) indicates where to find the actual value in the VLog.

The read path checks the `BIT_VALUE_POINTER` flag in the value's metadata byte:
- If set: Read the value from VLog at the specified `(file_id, offset)`
- If not set: Return the inline value directly

This extra indirection adds latency for large value reads but reduces SSTable size and compaction cost.

---

## Versioning and MVCC

SurrealKV implements Multi-Version Concurrency Control (MVCC) with snapshot isolation for concurrent reads and writes. MVCC stores multiple versions of each key, allowing readers and writers to operate concurrently without blocking each other.

### Why MVCC?

MVCC provides the following:

1. **Non-blocking reads**: Readers never block writers, and writers never block readers. Each transaction sees a consistent snapshot.

2. **Time-travel queries**: Applications can query historical data at specific timestamps, enabling auditing, debugging, and undo functionality.

3. **Optimistic concurrency**: Transactions proceed without locks and check for conflicts at commit time, which works well for read-heavy workloads.

4. **Isolation guarantees**: Each transaction sees a consistent view of the database, unaffected by concurrent modifications.

### InternalKey Structure

Every key stored in SurrealKV is an `InternalKey` that combines the user-provided key with version metadata. This encoding is central to how MVCC works:

```
┌──────────────────┬───────────────────┬──────────────────┐
│    user_key      │    trailer (8B)   │  timestamp (8B)  │
│   (variable)     │  seq_num | kind   │   nanoseconds    │
└──────────────────┴───────────────────┴──────────────────┘

trailer = (seq_num << 8) | kind

kind values:
  0 = Delete (hard delete, tombstone)
  1 = SoftDelete (versioned delete)
  2 = Set (normal write)
  6 = Replace (replaces all previous versions)
```

The **trailer** packs the sequence number and operation kind into 8 bytes. The sequence number occupies the upper 56 bits, and the kind occupies the lower 8 bits. This encoding allows efficient comparison: keys are sorted first by user key, then by sequence number (descending), so the newest version appears first.

The **timestamp** (optional, enabled with versioning) stores the application-provided timestamp in nanoseconds. This enables point-in-time queries using actual wall-clock times rather than internal sequence numbers.

**Operation Kinds Explained:**

- **Delete (0)**: A hard delete that removes the key permanently. During compaction at the bottom level, this tombstone can be dropped.
- **SoftDelete (1)**: A versioned delete that preserves history. The key appears deleted to current readers but historical queries can still see previous versions.
- **Set (2)**: A normal write that creates a new version of the key.
- **Replace (6)**: A special write that supersedes all previous versions, allowing aggressive garbage collection.

### Snapshot Isolation

Snapshot isolation ensures that each transaction sees a consistent view of the database. When a transaction starts, it captures the current `visible_seq_num`. All operations within that transaction see exactly the state as of that moment.

```
Timeline:
─────────────────────────────────────────────────────────────►
     │           │           │           │
  seq=1       seq=2       seq=3       seq=4
  Set(A,1)    Set(A,2)    Set(A,3)    Delete(A)
     │           │           │           │
     │           │           │           │
     ▼           ▼           ▼           ▼
┌─────────────────────────────────────────────────────────┐
│ Snapshot at seq=2 sees: A=2                             │
│ Snapshot at seq=3 sees: A=3                             │
│ Snapshot at seq=4 sees: A deleted (tombstone)           │
└─────────────────────────────────────────────────────────┘
```

**How visibility works:**

When reading key A with snapshot seq=2:
1. Find all versions of A: (seq=4, Delete), (seq=3, Set), (seq=2, Set), (seq=1, Set)
2. Filter to versions with seq <= 2: (seq=2, Set), (seq=1, Set)
3. Return the newest visible version: seq=2, value=2

### The Oracle

The Oracle is responsible for:
- **Sequence number allocation**: Atomically incrementing the global sequence counter for each commit
- **Conflict detection**: Tracking which keys were written at which sequence numbers to detect write-write conflicts

When two transactions both modify the same key, the Oracle ensures only one can commit. The first to call commit wins; the second receives a conflict error and must retry.

### Version Retention

By default, compaction drops old versions as soon as they are no longer visible to any active snapshot. When versioning is enabled (`with_versioning(true, retention_ns)`), the behavior changes:

- **All versions are preserved** during compaction, not just the latest
- **Retention period**: Versions older than `retention_ns` nanoseconds may be garbage collected
- **Unlimited retention**: Setting `retention_ns = 0` keeps all versions forever

This enables time-travel queries where applications can read the database state at any historical timestamp within the retention window.

### Versioned Storage Architecture

When versioning is enabled, SurrealKV automatically configures the storage layer to support efficient historical queries. The key insight is that all values are stored in the VLog, and indexes (both LSM and optional B+tree) store only value pointers. This architecture enables multiple index structures on top of a single value store.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     VERSIONED STORAGE ARCHITECTURE                          │
│                                                                             │
│  When versioning is enabled, all values are stored in VLog.                 │
│  Indexes store only value pointers, enabling multiple index structures.     │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                        INDEX LAYER                                  │    │
│  │                                                                     │    │
│  │   ┌───────────────────┐         ┌───────────────────┐               │    │
│  │   │  LSM Tree Index   │         │  B+Tree Index     │               │    │
│  │   │  (always present) │         │  (optional)       │               │    │
│  │   │                   │         │                   │               │    │
│  │   │  InternalKey ──►  │         │  InternalKey ──►  │               │    │
│  │   │  ValuePointer     │         │  ValuePointer     │               │    │
│  │   └─────────┬─────────┘         └─────────┬─────────┘               │    │
│  │             │                             │                         │    │
│  │             └──────────────┬──────────────┘                         │    │
│  │                            │                                        │    │
│  └────────────────────────────┼────────────────────────────────────────┘    │
│                               │                                             │
│                               ▼                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                        VALUE LOG (VLog)                             │    │
│  │                                                                     │    │
│  │   All versioned entries stored here (vlog_value_threshold = 0)      │    │
│  │                                                                     │    │
│  │   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐                   │    │
│  │   │ Entry 1 │ │ Entry 2 │ │ Entry 3 │ │ Entry 4 │ ...               │    │
│  │   │ v1 of A │ │ v2 of A │ │ v1 of B │ │ v3 of A │                   │    │
│  │   └─────────┘ └─────────┘ └─────────┘ └─────────┘                   │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Configuration:**

When `with_versioning(true, retention_ns)` is called:
- VLog is automatically enabled
- `vlog_value_threshold` is set to 0, so all values go to VLog
- The LSM tree stores `(InternalKey → ValuePointer)` instead of inline values

**Two Query Modes:**

| Mode | Configuration | Query Method |
|------|---------------|--------------|
| LSM-only | `with_versioning(true, retention_ns)` | K-way merge across all LSM components |
| B+tree indexed | + `with_versioned_index(true)` | Direct B+tree lookup |

**LSM-based Versioned Queries:**

When using LSM-only versioning, the `history()` API uses a `KMergeIterator` that gathers iterators from:
- Active memtable
- All immutable memtables (pending flush)
- All SSTable levels (L0, L1, L2, ...)

These are merged using a k-way merge that returns entries sorted by (user_key, seq_num descending). Finding all versions of a key requires scanning all levels.

**B+tree Versioned Queries:**

When `with_versioned_index(true)` is also enabled, versioned queries go directly to the B+tree which stores all `(InternalKey → ValuePointer)` entries. The B+tree keeps entries sorted, so all versions of a key are contiguous and can be retrieved with a single seek.

The B+tree implementation uses a disk-based page management approach, with the handling of large entries drawing inspiration from SQLite's overflow page design.

**Why Use B+tree Index:**

The motivation for the optional B+tree index:
- LSM stores keys across multiple SSTables; finding all versions requires checking every level
- B+tree consolidates all versions in a single sorted structure
- Single index lookup vs. merging N sources (where N = memtables + SSTables)

**B+tree Trade-offs:**

| Pros | Cons |
|------|------|
| Single index lookup instead of k-way merge | Slow insertion (in-place modified tree on disk) |
| Fast point and range queries for history | Insert performance slows during LSM writes |
| All versions of a key are contiguous | Every LSM write also updates B+tree |

**Read-after-Write Consistency:**

Currently, the B+tree index is updated synchronously during LSM writes, providing read-after-write consistency for versioned queries. A recently written version is immediately visible via the `history()` API.

**Future Work:**

Async indexing could improve write performance by decoupling B+tree updates from the LSM write path. However, this would require relaxing the read-after-write consistency guarantee (recently written versions may not be immediately visible via B+tree queries). This is a potential optimization for future versions.

---

## Compaction

Compaction is a background process that merges SSTables to reduce read amplification, reclaim space from obsolete versions, and maintain the leveled structure of the LSM tree.

### Why Compaction is Necessary

Without compaction, an LSM tree would degrade over time:
- **Read amplification grows**: More SSTables means more files to check for each read
- **Space amplification grows**: Obsolete versions and tombstones accumulate
- **Level 0 bloats**: Overlapping SSTables in L0 hurt read performance

Compaction addresses these issues by:
1. Merging overlapping SSTables into non-overlapping ones
2. Dropping obsolete versions that are no longer visible
3. Removing tombstones at the bottom level
4. Maintaining size ratios between levels

### Leveled Compaction Strategy

SurrealKV uses **leveled compaction**, similar to RocksDB's default strategy. Each level has a target size, with each level being approximately 10x larger than the previous:

- **Level 0**: ~64MB (4 files × 16MB each, overlapping allowed)
- **Level 1**: ~256MB (non-overlapping)
- **Level 2**: ~2.5GB (non-overlapping)
- **Level N**: 10x Level N-1

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         LSM TREE STRUCTURE                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  LEVEL 0 (Overlapping - recently flushed memtables)                         │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐                                        │
│  │ SST A   │ │ SST B   │ │ SST C   │  ← May have overlapping key ranges     │
│  │ a───z   │ │ d───m   │ │ b───k   │                                        │
│  └─────────┘ └─────────┘ └─────────┘                                        │
│       │           │           │                                             │
│       └───────────┼───────────┘                                             │
│                   ▼                                                         │
│  LEVEL 1 (Non-overlapping, sorted)                                          │
│  ┌─────────┬─────────┬─────────┐                                            │
│  │ SST 1   │ SST 2   │ SST 3   │  ← Non-overlapping, can binary search      │
│  │ a───f   │ g───m   │ n───z   │                                            │
│  └─────────┴─────────┴─────────┘                                            │
│                   │                                                         │
│                   ▼                                                         │
│  LEVEL 2 (Non-overlapping, 10x larger)                                      │
│  ┌──────┬──────┬──────┬──────┐                                              │
│  │SST 1 │SST 2 │SST 3 │SST 4 │                                              │
│  │ a─c  │ d─h  │ i─o  │ p─z  │                                              │
│  └──────┴──────┴──────┴──────┘                                              │
│                   │                                                         │
│                   ▼                                                         │
│  LEVEL N (Bottom level)                                                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         COMPACTION TRIGGER                                  │
│                                                                             │
│  Calculate scores for each level:                                           │
│    L0 score = max(file_count / 4, total_bytes / max_bytes)                  │
│    Ln score = level_bytes / target_bytes_for_level                          │
│                                                                             │
│  Pick level with highest score >= 1.0                                       │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          FILE SELECTION                                     │
│                                                                             │
│  1. Select file by priority:                                                │
│     - ByCompensatedSize: file_size * (1 + delete_ratio * 0.5)               │
│     - OldestSmallestSeqFirst: oldest sequence number                        │
│                                                                             │
│  2. Expand to include all files sharing boundary keys                       │
│                                                                             │
│  3. Select overlapping files from target level                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      COMPACTION ITERATOR                                    │
│                                                                             │
│  For each key in merged iterator:                                           │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                   SNAPSHOT-AWARE DECISION                             │  │
│  │                                                                       │  │
│  │  1. Find earliest snapshot that can see this version                  │  │
│  │                                                                       │  │
│  │  2. If same visibility boundary as newer version → DROP (superseded)  │  │
│  │                                                                       │  │
│  │  3. If visible to any active snapshot → KEEP                          │  │
│  │                                                                       │  │
│  │  4. If versioning enabled and within retention → KEEP                 │  │
│  │                                                                       │  │
│  │  5. Tombstone at bottom level with no snapshots → DROP                │  │
│  │                                                                       │  │
│  │  6. Otherwise → DROP                                                  │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  Track VLog discard stats for garbage collection                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           FINISH                                            │
│                                                                             │
│  1. Write new SSTable to disk                                               │
│  2. Update manifest atomically (add new, remove old)                        │
│  3. Delete old SSTable files                                                │
│  4. Flush VLog discard stats                                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Compaction Scoring

Each level receives a compaction score that determines priority:

**Level 0 Score:**
```
L0_score = max(file_count / L0_compaction_trigger, total_bytes / max_bytes)
```
L0 is special because overlapping files hurt read performance. The score considers both file count and total size.

**Level N Score (N > 0):**
```
Ln_score = level_bytes / target_bytes_for_level
```
Higher levels are compacted when they exceed their target size.

The level with the highest score >= 1.0 is selected for compaction.

### File Selection Algorithms

When a level is selected for compaction, individual files are chosen based on priority:

- **ByCompensatedSize**: Prioritizes files with high delete ratios: `score = file_size * (1 + delete_ratio * 0.5)`. This helps reclaim space from files with many tombstones.
- **OldestSmallestSeqFirst**: Prioritizes files with the oldest sequence numbers, which helps propagate old data to deeper levels.

After selecting the initial file, the compaction expands to include:
1. All files in the source level that share boundary keys (to avoid key range gaps)
2. All overlapping files in the target level

### Write Amplification

**Write amplification** measures how many times data is written to disk over its lifetime. In leveled compaction:
- Each entry is written once per level it passes through
- With a 10x size ratio and N levels, write amplification is approximately N
- Typical values: 10-30x for write-heavy workloads

SurrealKV's value separation (VLog) reduces write amplification by keeping large values out of the compaction process entirely.

---

## Value Log (VLog)

SurrealKV implements WiscKey-style value separation to reduce write amplification for large values. This design is based on the [WiscKey paper](https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf) from FAST '16.

### The Write Amplification Problem

In a traditional LSM tree, values are stored alongside keys in SSTables. During compaction, entire key-value pairs are read, merged, and rewritten. For large values, this leads to significant write amplification:

- A 1MB value written once may be rewritten 10-30 times as it moves through levels
- SSD lifespan is reduced by excessive writes
- Compaction becomes I/O bound on value size rather than key count

### WiscKey Solution

WiscKey separates keys from values:
- **Keys** (typically small) remain in the LSM tree and participate in compaction
- **Values** (potentially large) are stored separately in an append-only log

This separation provides:
- **Reduced write amplification**: Only keys are compacted, not values
- **Faster compaction**: Less data to read and write
- **Better SSD utilization**: Sequential writes to VLog, random reads amortized by SSD parallelism

**Trade-offs:**
- **Read overhead**: Large value reads require an extra I/O to the VLog
- **Garbage collection**: Stale VLog entries must be reclaimed separately
- **Complexity**: Additional component to manage

### When to Enable VLog

VLog is beneficial when:
- Values are large (> 1KB on average)
- Write volume is high
- SSD longevity is a concern

VLog may hurt performance when:
- Values are small (overhead of pointer > benefit)
- Workload is read-heavy (extra VLog I/O per read)
- Random access patterns (VLog reads are sequential within a file)

### Value Separation Decision

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      VALUE SEPARATION DECISION                              │
│                                                                             │
│  During commit, for each value:                                             │
│                                                                             │
│                    ┌─────────────────────────────┐                          │
│                    │  value.len() > threshold?   │                          │
│                    │  (default: 1KB)             │                          │
│                    └─────────────────────────────┘                          │
│                         │                  │                                │
│                    Yes  ▼                  ▼  No                            │
│            ┌─────────────────┐    ┌─────────────────┐                       │
│            │  Store in VLog  │    │  Store Inline   │                       │
│            │                 │    │                 │                       │
│            │ ┌─────────────┐ │    │ ┌─────────────┐ │                       │
│            │ │ValuePointer │ │    │ │ Raw Value   │ │                       │
│            │ │ (25 bytes)  │ │    │ │ (variable)  │ │                       │
│            │ │ in SSTable  │ │    │ │ in SSTable  │ │                       │
│            │ └─────────────┘ │    │ └─────────────┘ │                       │
│            └─────────────────┘    └─────────────────┘                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### ValuePointer Structure

```
┌─────────────────────────────────────────────────────────────────┐
│                    ValuePointer (25 bytes)                       │
├─────────┬──────────┬──────────┬────────────┬────────────┬───────┤
│ version │ file_id  │  offset  │  key_size  │ value_size │  crc  │
│  (1B)   │  (4B)    │  (8B)    │   (4B)     │   (4B)     │ (4B)  │
└─────────┴──────────┴──────────┴────────────┴────────────┴───────┘
```

### ValueLocation Encoding

```
┌─────────────────────────────────────────────────────────────────┐
│                      ValueLocation                              │
├─────────┬─────────┬─────────────────────────────────────────────┤
│  meta   │ version │               value                         │
│  (1B)   │  (1B)   │   (inline bytes OR ValuePointer)            │
└─────────┴─────────┴─────────────────────────────────────────────┘

meta bit 0 = BIT_VALUE_POINTER
  If set: value contains ValuePointer (25 bytes)
  If not set: value contains inline bytes
```

### VLog File Format

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          VLOG FILE FORMAT                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                      FILE HEADER (31 bytes)                        │     │
│  ├─────────┬─────────┬─────────┬────────────┬────────────┬────────────┤     │
│  │  magic  │ version │ file_id │ created_at │  max_size  │ compressed │     │
│  │  (4B)   │  (2B)   │  (4B)   │   (8B)     │    (8B)    │    (1B)    │     │
│  │ "VLOG"  │         │         │            │            │            │     │
│  └─────────┴─────────┴─────────┴────────────┴────────────┴────────────┘     │
│                                                                             │
│  ┌────────────────────────────────────────────────────────────────────┐     │
│  │                         ENTRY 1                                    │     │
│  ├───────────┬────────────┬──────────────┬──────────────┬─────────────┤     │
│  │  key_len  │ value_len  │     key      │    value     │    crc32    │     │
│  │   (4B)    │   (4B)     │  (variable)  │  (variable)  │    (4B)     │     │
│  └───────────┴────────────┴──────────────┴──────────────┴─────────────┘     │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                         ENTRY 2                                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                ...                                          │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                         ENTRY N                                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

Entry total size = 8 + key_len + value_len + 4 bytes
File rotation when size >= vlog_max_file_size (default: 256MB)
```

### VLog Configuration

Key configuration options:

| Option | Default | Description |
|--------|---------|-------------|
| `enable_vlog` | false | Enable value separation |
| `vlog_value_threshold` | 1KB | Values larger than this go to VLog |
| `vlog_max_file_size` | 256MB | Maximum size before rotating to new file |
| `vlog_gc_discard_ratio` | 0.5 | Trigger GC when 50% of file is garbage |

### File Management

VLog files are managed as follows:

1. **Active file**: New values are appended to the current active file
2. **Rotation**: When the active file reaches `vlog_max_file_size`, it is closed and a new file is created
3. **Reference counting**: Each VLog file maintains a reference count of active iterators to prevent deletion during reads
4. **Garbage collection**: Files with high discard ratios are compacted to reclaim space (see next section)

---

## VLog Garbage Collection

VLog files accumulate stale entries as values are overwritten or deleted. SurrealKV uses a **Global Minimum approach** inspired by RocksDB's BlobDB to determine when VLog files are safe to delete.

### The Garbage Problem

When a key is updated or deleted:
1. The new value is written to a new VLog file (values are append-only)
2. The LSM tree's pointer is updated to reference the new location
3. The old VLog entry becomes unreferenced

Over time, old VLog files may contain only stale entries that are no longer referenced by any SSTable. These files can be safely deleted to reclaim disk space.

### Global Minimum Approach

The key insight is that a VLog file is safe to delete when **no live SSTable references it**. SurrealKV tracks this using the `oldest_vlog_file_id` metadata in each SSTable:

**During SSTable Creation:**

When writing an SSTable (during flush or compaction), the `TableWriter` tracks the minimum VLog file ID referenced by any value pointer in that SSTable. This is stored in the SSTable's properties as `oldest_vlog_file_id`.

**Computing the Global Minimum:**

The `LevelManifest.min_oldest_vlog_file_id()` method computes the minimum `oldest_vlog_file_id` across all live SSTables:

```rust
// Pseudocode
min_oldest_vlog = manifest
    .iter()
    .filter(|sst| sst.oldest_vlog_file_id > 0)
    .map(|sst| sst.oldest_vlog_file_id)
    .min()
    .unwrap_or(0)
```

Any VLog file with `file_id < min_oldest_vlog` is safe to delete because no SSTable references it.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SST METADATA TRACKING                              │
│                                                                             │
│  During SSTable creation (flush or compaction):                             │
│                                                                             │
│  For each key-value pair:                                                   │
│    if value is a VLog pointer:                                              │
│      min_vlog_file_id = min(min_vlog_file_id, pointer.file_id)              │
│                                                                             │
│  Store min_vlog_file_id as oldest_vlog_file_id in SST properties            │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    AFTER FLUSH OR COMPACTION                                │
│                                                                             │
│  1. Compute global minimum:                                                 │
│     min_oldest_vlog = manifest.min_oldest_vlog_file_id()                    │
│                                                                             │
│  2. If min_oldest_vlog == 0: No SSTs reference VLog, skip cleanup           │
│                                                                             │
│  3. Otherwise: Delete VLog files with file_id < min_oldest_vlog             │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              ITERATOR SAFETY                                │
│                                                                             │
│  Before deleting a VLog file, check iterator_count:                         │
│    - If iterator_count > 0: Defer deletion (readers may access file)        │
│    - If iterator_count == 0: Safe to delete immediately                     │
│                                                                             │
│  This prevents deleting files that in-flight reads may access.              │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         VERSIONED INDEX CLEANUP                             │
│                                                                             │
│  If versioned_index (B+tree) is enabled:                                    │
│    - Scan B+tree entries for stale VLog pointers                            │
│    - Remove entries where pointer.file_id < min_oldest_vlog                 │
│    - Uses read-scan + batched-write-delete for concurrency safety           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### GC Components

| Component | Purpose | Location |
|-----------|---------|----------|
| **oldest_vlog_file_id** | SST metadata tracking oldest VLog file referenced | SST properties |
| **min_oldest_vlog_file_id()** | Computes global minimum across all SSTs | `LevelManifest` method |
| **iterator_count** | Safety counter for active readers | `VLog` struct |
| **cleanup_stale_versioned_index()** | Removes stale B+tree entries | `CoreInner` method |

### GC Trigger Points

VLog garbage collection runs automatically at two points:

1. **After Flush**: When a memtable is flushed to an L0 SSTable
2. **After Compaction**: When SSTables are merged during compaction

Both paths call `vlog.cleanup_obsolete_files(min_oldest_vlog)` after successfully updating the manifest.

### Algorithm Details

**Why Global Minimum Works:**

The global minimum approach is correct because:
- VLog file IDs are monotonically increasing
- Each SSTable records the oldest VLog file it references
- If `min_oldest_vlog = N`, then all live SSTables reference only VLog files >= N
- Therefore, VLog files < N have no live references and can be deleted

**Iterator Safety:**

VLog files cannot be deleted while readers might access them. The `iterator_count` provides a safety net:
- Incremented when a snapshot/iterator is created
- Decremented when the snapshot/iterator is dropped
- If `iterator_count > 0`, deletion is deferred to startup cleanup

This prevents the race condition where GC deletes a file that an in-flight read is about to access.

**Versioned Index Cleanup:**

When the optional B+tree versioned index is enabled, it stores `(InternalKey → ValuePointer)` entries. After VLog files are deleted, stale entries pointing to those files must be removed:

1. **Read Phase**: Scan the B+tree under a read lock, collecting keys with `pointer.file_id < min_oldest_vlog`
2. **Delete Phase**: Remove collected keys in batches under brief write locks

This two-phase approach avoids holding locks during the full scan, allowing concurrent writes.

**Comparison to RocksDB BlobDB:**

SurrealKV's approach is similar to RocksDB's BlobDB:
- Both track `oldest_blob_file_number` / `oldest_vlog_file_id` per SST
- Both compute a global minimum to determine safe-to-delete files
- SurrealKV adds `iterator_count` as an additional safety mechanism
- SurrealKV integrates cleanup with the optional versioned B+tree index

---

## Recovery

When SurrealKV starts, it recovers the database state from disk by loading the manifest and replaying the WAL. This process ensures that all committed data is restored, even after crashes or power failures.

### Crash Recovery Guarantees

SurrealKV provides the following guarantees:

- **Committed data is durable**: Any transaction that received a successful commit response will be recovered
- **Uncommitted data may be lost**: Transactions that crashed before commit are not recovered
- **Atomic recovery**: Either all of a transaction's writes are recovered, or none are

The strength of the durability guarantee depends on the `Durability` mode used at commit time:
- **Immediate**: Data was fsynced before commit returned; it will survive any crash
- **Eventual**: Data was written to OS buffers; it may be lost if the OS crashes before flushing

### Recovery Process Overview

Recovery proceeds in three phases:

1. **Initialization**: Load metadata and acquire exclusive access
2. **WAL Replay**: Reconstruct in-memory state from the write-ahead log
3. **Finalization**: Clean up orphaned files and start background tasks

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                INITIALIZATION                               │
│                                                                             │
│  1. Acquire LOCK file (prevents concurrent access)                          │
│                                                                             │
│  2. Load Level Manifest                                                     │
│     - SSTable locations per level                                           │
│     - min_wal_number (WALs older than this are flushed)                     │
│     - last_sequence number                                                  │
│                                                                             │
│  3. Open/create required directories                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                  WAL REPLAY                                 │
│                                                                             │
│  For each WAL segment with id >= min_wal_number:                            │
│                                                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  Read records sequentially:                                           │  │
│  │                                                                       │  │
│  │  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐              │  │
│  │  │ Read Header │ ──► │ Verify CRC  │ ──► │ Decompress  │              │  │
│  │  │ (7 bytes)   │     │             │     │ (if needed) │              │  │
│  │  └─────────────┘     └─────────────┘     └─────────────┘              │  │
│  │         │                   │                   │                     │  │
│  │         ▼                   ▼                   ▼                     │  │
│  │  On corruption:       CRC mismatch:       Apply to MemTable           │  │
│  │  - TolerateWithRepair: truncate & continue                            │  │
│  │  - AbsoluteConsistency: return error                                  │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  If MemTable fills during replay → Flush to L0 SSTable                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                       │
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                 FINALIZATION                                │
│                                                                             │
│  1. Set sequence numbers:                                                   │
│     visible_seq_num = max(manifest_last_seq, wal_max_seq)                   │
│     log_seq_num = visible_seq_num + 1                                       │
│                                                                             │
│  2. Cleanup orphaned files:                                                 │
│     - SST files not in manifest → delete                                    │
│     - WAL files < min_wal_number → delete                                   │
│                                                                             │
│  3. Start background tasks:                                                 │
│     - MemTable flush task                                                   │
│     - Level compaction task                                                 │
│     - VLog GC task (if VLog enabled)                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### WAL Record Format

The WAL uses a block-based format inspired by LevelDB/RocksDB. Each block is 32KB, and records that don't fit in a single block are fragmented across multiple blocks.

```
┌───────────────────────────────────────────────────────────────────────────┐
│                         WAL BLOCK (32KB)                                  │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌───────┬───────┬───────┬─────────────────────────────────────────────┐  │
│  │  CRC  │ Size  │ Type  │                 Payload                     │  │
│  │ (4B)  │ (2B)  │ (1B)  │              (variable)                     │  │
│  └───────┴───────┴───────┴─────────────────────────────────────────────┘  │
│                                                                           │
│  Record Types:                                                            │
│    0 = Empty (padding)                                                    │
│    1 = Full (complete record in one block)                                │
│    2 = First (start of fragmented record)                                 │
│    3 = Middle (continuation of fragmented record)                         │
│    4 = Last (end of fragmented record)                                    │
│                                                                           │
│  CRC covers: Type byte + Payload                                          │
└───────────────────────────────────────────────────────────────────────────┘
```

**Record Fragmentation:**

Large records that don't fit in the remaining space of a block are split:
- **First**: Contains the beginning of the record
- **Middle**: Contains continuation data (may be multiple)
- **Last**: Contains the final portion

During replay, fragments are reassembled before processing.

### Recovery Modes

SurrealKV supports two recovery modes that control behavior when WAL corruption is detected:

| Mode | Behavior |
|------|----------|
| **AbsoluteConsistency** | Return error on any corruption; do not start |
| **TolerateWithRepair** | Truncate corrupted tail and continue recovery |

`TolerateWithRepair` is useful for recovering from crashes that left partial writes. The corrupted data represents uncommitted transactions (they crashed before completing), so truncating them maintains consistency.

### Manifest's Role in Recovery

The manifest tracks the durable state of the LSM tree:
- Which SSTables exist at each level
- Their key ranges and file sizes
- The `min_wal_number`: WALs older than this have been fully flushed to SSTables

During recovery, SurrealKV only replays WALs with `id >= min_wal_number`. Older WALs contain data that is already in SSTables and can be safely deleted.

### Orphan File Cleanup

After recovery, the database may contain orphaned files:
- **SST files not in manifest**: Created but crashed before manifest update; safe to delete
- **WAL files < min_wal_number**: Already flushed to SSTables; safe to delete
- **Temporary files**: Incomplete compaction outputs; safe to delete

This cleanup ensures disk space is reclaimed after crashes.

---

## Checkpoint and Restore

Checkpoints create consistent point-in-time snapshots of the database for backup and recovery. Checkpoints capture the entire database state at a specific moment, enabling point-in-time recovery.

### Use Cases

**Backup and Disaster Recovery:**
- Create periodic checkpoints to external storage (S3, NFS, etc.)
- Restore from checkpoint after catastrophic failure
- Geographic distribution of backup copies

**Testing and Development:**
- Checkpoint production data for test environments
- Restore to known state between test runs
- Debug issues with production data snapshots

**Migration:**
- Checkpoint before major upgrades
- Transfer database between machines
- Clone database for read replicas

### Why Flush Before Checkpoint?

A checkpoint must capture a consistent state. The active memtable contains uncommitted and in-flight data that may be partially applied. By flushing to SSTables first:

1. All committed data is durably stored in files
2. The checkpoint captures exactly what was committed at that moment
3. Recovery from checkpoint is deterministic

### Checkpoint Process

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         CHECKPOINT CREATION                             │
│                                                                         │
│  1. PREPARATION                                                         │
│     - Flush active memtable to SSTable                                  │
│     - Wait for all immutable memtables to flush                         │
│     - Capture current sequence number                                   │
│                                                                         │
│  2. COPY FILES TO CHECKPOINT DIRECTORY                                  │
│     checkpoint_path/                                                    │
│     ├── sstables/         ← Copy all SSTable files                      │
│     ├── wal/              ← Copy WAL segments                           │
│     ├── manifest/         ← Copy manifest file                          │
│     ├── vlog/             ← Copy VLog files (if enabled)                │
│     ├── discard_stats/    ← Copy discard stats (if VLog)                │
│     ├── delete_list/      ← Copy delete list (if VLog)                  │
│     └── metadata.json     ← Checkpoint metadata                         │
│                                                                         │
│  3. METADATA                                                            │
│     {                                                                   │
│       "timestamp": 1234567890,                                          │
│       "sequence_number": 12345,                                         │
│       "sstable_count": 42,                                              │
│       "total_size": 1073741824                                          │
│     }                                                                   │
└─────────────────────────────────────────────────────────────────────────┘
```

### Restore Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                            RESTORE PROCESS                              │
│                                                                         │
│  1. VALIDATION                                                          │
│     - Verify checkpoint metadata exists                                 │
│     - Verify all required files present                                 │
│                                                                         │
│  2. REPLACE DATABASE FILES                                              │
│     - Clear current sstables/, wal/, manifest/, vlog/ directories       │
│     - Copy files from checkpoint                                        │
│                                                                         │
│  3. RELOAD STATE                                                        │
│     - Load manifest from checkpoint                                     │
│     - Clear in-memory memtables (discards uncommitted data)             │
│     - Replay WAL segments from checkpoint                               │
│     - Set sequence numbers from checkpoint                              │
│                                                                         │
│  4. RESUME OPERATIONS                                                   │
│     - Database returns to checkpoint state                              │
│     - Any data written after checkpoint is discarded                    │
└─────────────────────────────────────────────────────────────────────────┘
```

