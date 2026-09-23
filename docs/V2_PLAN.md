# SurrealKV V2 Transition Plan & Architecture

## Objective
Evolve SurrealKV from a traditional embedded, local-disk LSM tree into a highly concurrent, non-blocking, async-native, and object-storage-ready Key-Value store. The new architecture draws inspiration from modern storage engines like ShaleDB, SlateDB, and ScyllaDB, as well as the high-throughput Optimistic Concurrency Control (OCC) pipeline from SurrealMX.

The ultimate goal is to build an engine that handles massive concurrency without locking, scales seamlessly from local NVMe to cloud object storage (S3/MinIO), and is fiercely optimized out-of-the-box without requiring complex configuration tuning.

## Core Architectural Shifts
1. **Push Versioning Up:** Remove native MVCC (timestamps/seq_nums) from the core LSM tree, shifting version management to composite keys.
2. **True Non-Blocking Commits:** Replace the mutex-bound commit pipeline with an OCC lock-free ring buffer and background async I/O flushers.
3. **Async-Native I/O:** Replace standard synchronous filesystem calls with async traits, leveraging `io_uring` on Linux and `affinitypool` elsewhere.
4. **Cloud-Native Storage:** Treat WAL segments and SSTables as strictly immutable blobs ready for object storage.

---

## Phase 1: The Concurrency Revamp (Lock-Free Ring Buffer & OCC)
The primary performance bottleneck in the current SurrealKV implementation is the serialization of disk I/O and conflict checking behind a single `write_mutex` in `commit_pipeline.rs`. We will decouple CPU-bound conflict validation from I/O-bound disk writes.

- [x] Create the V2 Transition Plan documentation.
- [x] Delete the legacy `commit.rs` module and `CommitPipeline`.
- [x] Scaffold the Lock-Free MPSC `Ring` buffer.
- [x] Implement fast `BloomFilter` for OCC read/write sets.
- [x] Connect the `Transaction` to use the `Ring` buffer (claim, validate, write).
- [x] Implement the `Flusher` task for Group Commit to WAL & Memtable.
- [x] Implement Multiple Immutable Memtables (Burst Buffering).
- [x] Implement Proactive Write Pacing.
- [x] Refactor hot-path `Vec<u8>` usage for Zero-Allocation data paths.

### The Commit Ring Buffer
We will replace the `commit_sem` and `write_mutex` with a fixed-size, multi-producer single-consumer (MPSC) Ring Buffer inspired by the LMAX Disruptor and ShaleDB.
* Writers atomically `claim()` a slot (sequence number) in the ring.
* Writers encode their transaction directly into their pre-allocated slot's buffer, then mark the slot as `published`.
* This design guarantees zero-allocation on the critical write path and naturally and strictly orders concurrent transactions.

### Optimistic Concurrency Control (OCC)
Instead of taking locks to validate reads and writes, transactions will build local `readset` and `writeset` Bloom filters during execution. When claiming a slot in the ring, they intersect these Bloom filters against recent commits to detect conflicts entirely lock-free.

### Background Flusher (Group Commit)
A dedicated flusher task spins/waits on the Ring Buffer. When slots become `published`, the flusher grabs all contiguous ready slots, batches them, and issues a single large `write()` to the WAL and Memtable. The flusher then updates a `taken` watermark, which notifies awaiting writers and signals that their transaction is durable. This amortizes async/await overhead across thousands of transactions.

### Multiple Immutable Memtables (Burst Buffering)
Instead of a single memtable that halts writes during a disk flush (causing latency spikes), the engine will maintain one *Active Memtable* and a queue of *Immutable Memtables*.
* When the Active Memtable hits capacity, it is atomically sealed and pushed to the immutable queue. A new Active Memtable is instantly allocated, ensuring zero write stalling.
* Background Tokio tasks pop from the immutable queue and flush them to L0 SSTables.
* Queries seamlessly merge results across the Active Memtable, the Immutable Memtables, and the disk SSTables.

### Proactive Write Pacing
To prevent burst writes from filling RAM faster than the disk can flush, we will introduce smooth backpressure tied to the immutable memtable queue. As the queue grows (e.g., > 3 memtables), the `claim()` operation intentionally introduces microsecond delays, slowing producers gently. A hard limit (e.g., 8 memtables) acts as a strict OOM safeguard.

### Retain the Arena Skiplist
We will continue using the highly optimized, fixed-size lock-free `arenaskl` (from Pebble) for the underlying Memtable structures. This guarantees zero-allocation inserts and strictly bounded memory usage per memtable, perfectly complementing the Ring Buffer.

### Zero-Allocation Data Paths
Refactor code utilizing `Vec<u8>` or `Bytes` in the hot paths (like `Batch::encode` and `Batch::decode`) to reuse pooled buffers or operate strictly within the Ring Buffer and Arena allocations, eliminating allocator thrashing.

---

## Phase 2: The Purge (Simplifying the LSM Tree)
With the new commit pipeline in place, we strip out the deeply integrated versioning logic to make the tree a pure, ultra-fast byte-to-byte store.

- [x] Delete the legacy `bplustree` module completely.
- [x] Flat Entries: Remove `timestamp`, `seq_num`, and `kind` from core `Node` and `Entry`.
- [x] True Single-Version KV: Implement direct overwrite/tombstone semantics without retention windows.
- [x] Simplified Compaction: Purge timestamp comparator logic and rewrite compaction as pure byte-prefix merging.
- [x] Implement Range Tombstones (O(1) Mass Deletions).

### Flat Entries & True Single-Version KV
Remove `timestamp`, `seq_num`, and `kind` (Tombstone vs. Value) from the core `Node` and `Entry` structs. SurrealKV becomes a strict, single-version key-value store. Overwrites physically replace older values. Time-travel, MVCC, and historical versioning must be handled at the higher database layer (SurrealDB) by appending timestamps to the keys themselves.

### Delete the Legacy B+Tree
Since versioning is pushed up the stack, the dual-architecture approach (having both an LSM tree and a B+Tree for in-place versioned updates) is obsolete. We will completely delete the `bplustree` module, shedding thousands of lines of complex code and massively reducing binary size and maintenance burden.

### Simplified Compaction
Compaction becomes a pure byte-prefix merging operation. The tree no longer needs to understand the semantics of time-travel, sequence numbers, or retention windows.

### Range Tombstones (O(1) Mass Deletions)
Traditional LSM engines handle mass deletions (like dropping a table) by inserting millions of individual tombstone records into the memtable, which floods the WAL, crashes performance, and bloats SSTables. We will implement **Range Tombstones**. Deleting a prefix (e.g., `user:123:*`) inserts a single entry: `[START: user:123:00, END: user:123:FF, TYPE: RangeDelete]`. This makes mass deletions a true O(1) operation, saving massive amounts of I/O. During compaction or point reads, any key falling within this range is instantly recognized as deleted.

---

## Phase 3: Deterministic Simulation Testing (DST) & Differential Testing
Before swapping out the underlying storage abstraction for async components, we must ensure we can test the new concurrent architecture rigorously. Distributed, async, lock-free code is notoriously difficult to test for race conditions and crash consistency.

- [x] Implement the DST simulation harness (seeded RNG for mocked executor, simulated time, and I/O).
- [x] Differential Testing Oracle (ModelDb): Build a reference in-memory model (using `BTreeMap`) that records the ground truth of committed state, verifying that `engine.get()` and `engine.range()` never diverge from the model under chaotic interleavings.
- [x] Fault injection framework (simulating crash & recovery, rollback, and OCC concurrency).
- [x] Fuzzing the Lock-Free Ring Buffer & Commit Pipeline under chaotic interleavings.

### The Simulation Harness
Build a deterministic simulator (inspired by `shale-sim` and FoundationDB) where the async executor, network (for S3), and disk I/O are mocked and controlled by a single seeded RNG.

### The Differential Testing Oracle (ModelDb)
A crash-free simulation run does not prove data correctness—a storage engine could silently lose keys or return stale data without crashing. We will build a differential testing harness:
* Maintain a trivial, mathematically correct in-memory reference model (`ModelDb`) alongside SurrealKV.
* Execute identical randomized transactions (inserts, overwrites, deletes, range tombstones, and snapshot reads) against both SurrealKV and the reference model simultaneously.
* After every commit, range scan, or simulated power failure/recovery, verify that SurrealKV's state matches the reference model byte-for-byte.
* If a single divergence occurs, the test halts and outputs the exact random seed for 100% deterministic reproduction.

### Fault Injection
Simulate power losses, dropped network packets, out-of-order writes, and thread stalls instantly, finding trillion-to-one edge cases in seconds on a laptop. If a test fails, the exact seed can be replayed to perfectly reproduce the bug.

### Proving the Ring Buffer
The new lock-free OCC Ring Buffer (from Phase 1) will be the first component subjected to the DST to guarantee strict serialization and memory safety under extreme simulated concurrency before it ever touches real disk I/O.

---

## Phase 4: Async Native & `io_uring` / `affinitypool`
Swap the synchronous `vfs.rs` abstraction for a truly async storage layer. The current implementation relies on blocking operations that severely limit throughput and stall the async executor.

- [x] Add `affinitypool = "0.8"` dependency.
- [x] Define asynchronous storage traits (`LogStore` for append-only sequential log persistence and `ObjectStore` / `PageStore` for immutable block/SSTable random reads).
- [x] Implement `AffinityLogStore` and `AffinityObjectStore` backed by `affinitypool` (and local filesystem).
- [x] Implement in-memory async stores for testing and simulator mocks (`MemLogStore`, `MemObjectStore`).
- [x] Migrate `SSTable` block reading and point lookup to async (`read_block_async`, `Table::get_async`, `Transaction::get_async`).
- [x] Integrate asynchronous LogStore & ObjectStore tests into test suite.
- [ ] Upgrade WAL append & sync in `CommitPipeline` flusher to async `LogStore::append`.
- [ ] Run full DST and unit tests verifying async storage equivalence.

### Distinct Storage Traits
We will split the monolithic `File` trait into two distinct abstractions tailored to their access patterns:
* `LogStore`: Optimized for the WAL. Requires sequential, append-only writes, and fast `fdatasync`. 
* `ObjectStore` (or `Pages`): Optimized for SSTables. Requires immutable blob creation (write once, seal) and highly concurrent random reads (`read_at` or range queries).

### `tokio-uring` Backend (Linux)
Implement a native storage backend using `tokio-uring` to submit zero-copy, fully non-blocking I/O operations directly to the Linux kernel without thread-pool overhead. Because `io_uring` requires passing ownership of buffers to the kernel to prevent use-after-free, we will integrate a buffer pool (reusing `BytesMut` or `Vec<u8>`).

### AffinityPool Fallbacks
For macOS and Windows (or environments lacking `io_uring`), going completely lock-free async natively is not possible at the OS level for files. Instead of falling back to standard `tokio::fs` (which uses an unbounded, shared blocking pool that can starve the runtime), we will utilize `affinitypool`. It routes filesystem tasks to dedicated blocking worker threads isolated by a sharded MPMC queue, ensuring the core asynchronous executor remains unblocked.

### Parallel SSTable Reads
Upgrade the read path. Instead of blocking the executor during `SSTable::get`, reads will await a purely async `get_range` call (backed by `tokio-uring` or `affinitypool`), freeing executor threads to process other queries while the disk seeks.

---

## Phase 5: Object-Storage Native (The Cloud Tier)
Adapt the LSM tree to function efficiently over network boundaries, mirroring SlateDB and ShaleDB architectures.

### Immutable Object Paradigm
Guarantee that once a WAL segment or SSTable is closed, it is strictly immutable. This enables aggressive local caching and trivial replication.

### S3 Integration via `object_store`
Implement the `ObjectStore` trait using the `object_store` crate, allowing SurrealKV to run entirely backed by S3, MinIO, or Azure Blob Storage.

### Manifest Compare-and-Swap (CAS)
Store the LSM manifest (the list of active SSTables and levels) in a centralized location using atomic CAS operations (e.g., S3 conditional puts). This acts as the single source of truth for the database state.

### Zero-Cost Branching & Forking
Because SSTables are immutable, forking the database requires zero data copying. Branching is achieved simply by duplicating the Manifest file. Both branches initially share the same underlying SSTables, diverging seamlessly (Copy-On-Write) as they write new data and generate independent SSTables.

### Point-In-Time Recovery (PITR) & Checkpointing
By continuously archiving immutable WAL segments and periodically snapshotting the Manifest (creating lightweight "Checkpoints") to object storage, PITR becomes a trivial routing operation. Restoring to a specific millisecond involves loading the nearest historical Checkpoint Manifest and replaying the archived WAL up to the target sequence number.

### Cloud-Optimized Compaction (Cost & Network Mitigation)
Traditional leveled compaction has massive write amplification, leading to saturated networks and high S3 PUT costs. The new compaction strategy will keep L0/L1 on fast local NVMe (acting as a disk-backed buffer) and switch higher object-storage levels (L2+) to Size-Tiered or Lazy-Leveled compaction to drastically slash write amplification.

### Tiered Storage (Hot NVMe vs. Cold S3)
To prevent excessive S3 egress fees and high latency on repeated queries, the engine will natively support Tiered Storage. The local NVMe drive will act as a massive LRU cache for S3. Data lifecycle shifts dynamically: "Hot" SSTables live in RAM (Block Cache), "Warm" SSTables live on local NVMe, and "Cold" SSTables live strictly in S3. When a Cold SSTable is queried, it is fetched and pinned to local NVMe, while a background thread evicts the least-recently-used warm SSTables back to "S3-only" status when local disk space fills up.

---

## Phase 6: Advanced Engine Optimizations
To push read and write throughput to the absolute hardware and network limits, we will implement these cutting-edge storage engine techniques.

### Direct I/O (`O_DIRECT`)
Bypass the OS page cache entirely on local storage. Data moves via DMA directly from the database's pre-allocated memory pools to the NVMe controller, eliminating CPU memory copies and preventing double-caching (where data lives in both the OS cache and SurrealKV's block cache).

### High-Performance User-Space Cache
Because `O_DIRECT` bypasses the OS cache, we are fully responsible for memory management. Implement a highly concurrent block cache (using W-TinyLFU) tailored to our SSTables and Object Store chunks, preventing cache thrashing during large range scans.

### Zstd Dictionary Compression
Instead of compressing 4KB blocks in isolation (like standard Snappy/LZ4), train a Zstd Dictionary for the entire SSTable during flush. This massively compresses repetitive document schemas (JSON-like data), drastically multiplying I/O throughput.

### Remote / Stateless Compactions
Because Phase 5 moves SSTables to the `ObjectStore`, compaction can be fully offloaded. Stateless "Compactor" worker processes can read SSTables from S3, merge them, and write out new SSTables independently. This prevents background compactions from causing latency spikes (write stalls) on the primary serving node.

### VLog WAL Bypass (WiscKey)
Write large values directly to the Value Log (VLog) during the group commit phase, and only write a tiny VLog pointer to the WAL. This halves write amplification for large values and dramatically shrinks the size of the WAL, accelerating recovery.

### XOR / Ribbon Filters
Upgrade standard Bloom filters to XOR or Ribbon filters (like RocksDB). They consume ~20-30% less memory for the same false-positive rate and are designed to fit perfectly into a single CPU cache line, significantly accelerating point-read (`get()`) queries.

### Parallel WAL Replay (Instant Crash Recovery)
Because V2 uses a single-version KV model (Phase 2), writes to different keys are independent. During crash recovery, one thread streams the WAL and shards records by key-hash into multiple queues, while N worker threads concurrently insert them into the Memtable. This shifts recovery from I/O-bound to CPU-bound across all cores, shrinking failover times to milliseconds.

### Zero-Copy Deserialization (Aligned Block Formats)
When reading a block from an SSTable, parsing the keys and values typically requires copying bytes into new Rust structs or strings, which burns CPU cycles. We will design the SSTable block format to be strictly memory-aligned (using zero-copy techniques like `zerocopy` or `rkyv`). When a 4KB block is pulled from disk via `O_DIRECT`, we cast the raw memory pointer directly to a Rust struct, achieving literal zero-CPU-cost deserialization once the data is in RAM.

---

## Phase 7: Resilience, Security, & Telemetry
To ensure the engine behaves predictably in production, we must protect against hardware lies, secure data at rest, and provide zero-cost observability.

### Transparent Data Encryption (TDE) & KMS Integration
Pushing SSTables and WAL segments to S3/Object Storage mandates strict security. We will add block-level Encryption at Rest. Before a 4KB block or a WAL frame is written to disk/S3, it is encrypted (e.g., using AES-GCM or ChaCha20-Poly1305). The engine will integrate with a Key Management System (KMS) so that the master encryption key can be rotated without rewriting the data. Because encryption happens *after* Zstd compression, it has minimal impact on storage size, and hardware acceleration makes the CPU cost negligible.

### End-to-End Integrity & Background Scrubbing
Protect against silent bit-rot. Every SSTable block, WAL frame, and the Manifest itself must carry inline `xxHash3` checksums. A low-priority background "scrubber" task will continuously trickle through cold data on disk/S3, verifying checksums and proactively self-healing from replicas/backups before a user query hits a bad sector.

### Zero-Allocation Telemetry (The Flight Recorder)
Logging is too slow for a high-throughput engine. Measure everything (Ring Buffer wait times, `fsync` latency percentiles, cache hit rates) using `Relaxed` atomics, thread-local aggregators, and lock-free HDRHistograms. This allows SurrealDB to expose Prometheus metrics with p99/p99.99 latencies in real-time with zero performance penalty on the critical path.

---

## Phase 8: Operational Excellence
To ensure the engine is not just fast, but robust and easy to operate in production environments, we will implement these final operational pillars.

### Global Memory Accounting (OOM Prevention)
Tie all in-memory components (Active Memtable, Immutable Memtables, Ring Buffer, User-Space Block Cache) into a unified memory tracker. The engine dynamically balances a strict `max_memory` budget—for example, if a write burst balloons the immutable memtable queue, the block cache automatically shrinks to compensate, ensuring the database never crashes due to Out-Of-Memory errors.

### S3 Orphan Garbage Collection
When writing to object storage, network partitions or crashed compactors can leave "orphaned" SSTables that cost money but belong to no Manifest. An epoch-based background garbage collector will routinely reconcile the S3 bucket contents against active and Checkpoint Manifests, safely deleting unreferenced blobs after a safe grace period.

### Zero-Config Auto-Tuning
Avoid "configuration hell." On startup, SurrealKV should query the OS for total system RAM and CPU core count, automatically calculating the optimal thread pool sizes, memtable capacities, and block cache limits. It will be fiercely optimized out-of-the-box, with manual overrides available but rarely necessary.
