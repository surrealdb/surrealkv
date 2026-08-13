# SurrealKV Branch-Native LSM Rewrite Plan

Status: Proposed

Scope: Complete storage-format and public-API reset. Backward compatibility with the current
on-disk format, B+tree index, timestamp semantics, checkpoint format, and VLog layout is not a
requirement.

Execution companion: [BRANCHING_IMPLEMENTATION_PLAN.md](BRANCHING_IMPLEMENTATION_PLAN.md).
Adversarial source review: [BRANCHING_ADVERSARIAL_REVIEW.md](BRANCHING_ADVERSARIAL_REVIEW.md).

## 1. Decision

SurrealKV should be rewritten as a branch-native, LSM-only embedded key-value store.

The target design is based on the branch substrate proven by Strata:

- a branch identifier is part of every physical key;
- every transaction receives one globally ordered commit version;
- every branch owns mutable state and immutable tables;
- a fork references immutable parent tables at a fixed version rather than copying rows;
- inherited reads are capped at the fork version;
- child and inherited rows are merged into one MVCC view;
- branch generations fence stale transactions and stale WAL replay;
- table and blob deletion requires a database-wide reachability proof;
- timestamps select commit versions through a persisted commit timeline;
- storage remains unaware of SurrealDB record, index, graph, or schema semantics.

The B+tree is not needed for any of these properties. Before its removal, it compensated for
ambiguous out-of-order user timestamps, not for MVCC or branching. The rewrite removes the remaining
semantic ambiguity by making commit version the only visibility order and commit timestamp only a
selector that resolves to a commit version.

## 2. Adversarial verdict

### Feasibility

The rewrite is feasible without a B+tree. An LSM can provide point reads, range reads, history,
forks, historical forks, diffs, and merges when its key layout, immutable-table ownership, and
auxiliary change journal are designed for those operations.

### Conditions on that verdict

The design is viable only if all of the following are treated as foundational rather than later
cleanup:

1. Fork anchors participate in retention. Compaction must never prune a version or delete a table
   needed by a live descendant.
2. Shared tables are immutable and globally identified. A branch manifest refers to table objects;
   it does not own their filesystem lifetime.
3. Ordinary deletes are tombstones. Physical purge is a separate administrative operation that
   runs only after reachability is proven.
4. Branch lifecycle is durable in the WAL or metadata log. Catalog files and branch manifests are
   recoverable projections, not two unrelated sources of truth.
5. Historical timestamp selection resolves to a retained commit version before reading or
   forking. It never performs a separate best-effort timestamp decision for each key.
6. Nested forks have a depth/read-amplification policy. Unlimited inheritance chains are not
   acceptable.
7. The first format does not use the current mutable VLog design. Values are inline until an
   immutable, reachability-aware blob tier exists.
8. Diff and merge use a commit change journal. Full branch scans are a fallback and testing oracle,
   not the primary algorithm.
9. Recovery and reclamation fail closed when branch or table reachability cannot be proven.

If any of conditions 1-5 is deferred, the system can silently return incorrect historical data or
delete data still visible to another branch. That is a no-go.

### Current SurrealKV evidence

This plan targets the current `v2` branch. Commit `2c9e8f3` (`remove bplustree`) has already removed
the B+tree module, benchmark, options, migration test, compactor hooks, and alternate snapshot path.
That deletion is a completed baseline, not future work.

The remaining code still needs a format-level rewrite:

- `src/transaction.rs` accepts per-entry explicit timestamps through `set_at` and `WriteOptions`;
- `src/snapshot.rs` selects historical values by scanning versions and comparing those timestamps;
- `src/comparator.rs` still contains `TimestampComparator` and timestamp-seek assumptions even
  though the canonical internal ordering is by sequence number;
- `src/lsm.rs` has one global mutable/frozen/manifest state rather than branch-local LSM state;
- `src/checkpoint.rs` copies one manifest and VLog layout, so it is a backup primitive rather than
  branch lineage;
- `src/vlog.rs` uses mutable lifetime/GC rules that are not safe for SSTables shared by descendants.

Therefore removing the B+tree was necessary but not sufficient. The high-risk semantic mismatch is
now the coexistence of sequence-ordered rows with arbitrary user timestamps.

## 3. Explicitly rejected designs

### Keep the current B+tree as a timestamp index

Rejected. It creates two persistence paths, two iterator implementations, synchronous secondary
index maintenance, additional recovery state, and extra VLog references. Commit-time travel does
not require timestamp-ordered rows; it requires a timestamp-to-commit timeline.

### Prefix every key with a branch but retain one global LSM state

Rejected as the final architecture. It is a useful prototype, but descendants would force global
compaction to retain ancestor versions, inheritance reads would walk logical prefixes rather than
share table objects, and branch-local compaction/backpressure would be impossible.

### One checkpoint directory per branch

Rejected. Checkpoints can cheaply hard-link SSTables, but they create independent database
instances, duplicate or complicate WAL and blob state, do not encode lineage, and do not provide
diff, merge, or safe shared-object reclamation.

### Replace the LSM with Dolt-style Prolly trees

Rejected. Prolly trees are excellent for structural sharing and difference-proportional diffs, but
adopting them would replace the central write/compaction model rather than make SurrealKV a
branch-native LSM. SurrealKV can borrow Dolt's commit-DAG and three-way-merge concepts without
borrowing its physical tree.

### Carry the current VLog into the new format

Rejected for the first release. An inherited SSTable can contain pointers to a parent's VLog
segments. Rewriting or deleting those segments then becomes a cross-branch operation, and a local
minimum-file-ID calculation is not a sufficient liveness proof. Inline values make the initial
correctness argument much smaller.

### What adjacent engines establish

| System | Mechanism | What SurrealKV should take | Why it is not a drop-in answer |
| --- | --- | --- | --- |
| [Strata v1](/Users/kfarhan/workspace/projects/strata-core/docs/architecture/strata-v1-design-overview.md) | Branch-keyed LSM state plus capped inherited immutable tables | The direct branch substrate, generation fencing, fork anchors, and global reachability | Its contracts and implementation are a design source, not a reusable SurrealKV storage format |
| [SlateDB](/Users/kfarhan/workspace/projects/slatedb/README.md) | Object-native LSM with fenced immutable metadata, ranged SST reads, caches, persistent compaction state, and GC protocols | Lost-response reconciliation, remote table mechanics, request tagging, durable job states, and object-store failure cases | Its clones are separate database roots with checkpoints/external SST paths; they are not integral high-fan-out branches |
| [Dolt](https://www.dolthub.com/docs/architecture/storage-engine/) | Content-addressed Prolly Trees plus a commit graph | Commit DAGs, structural-sharing goals, three-way comparison, and difference-proportional UX | Reusing it means choosing a Prolly-tree engine, not completing an LSM rewrite |
| [Neon](https://neon.com/blog/get-page-at-lsn) | Non-overwriting, LSM-like image/delta layers; reads and forks at an LSN | Strong external evidence that immutable layers, a monotonic version, retained history, and parent fallback can support copy-on-write branching | It stores PostgreSQL pages/WAL in a distributed service and reconstructs pages; it is not an embedded ordered KV library |
| [RocksDB](https://github.com/facebook/rocksdb/wiki/Basic-Operations) and [Pebble](https://github.com/cockroachdb/pebble) | LSM snapshots, checkpoints, immutable SSTables, manifests, and compaction | Mature implementation patterns for iterators, snapshots, file metadata, filters, and compaction | Their documented snapshots are read views, not durable writable branches with lineage, merge, generations, or descendant-aware GC |
| [Badger](https://github.com/dgraph-io/badger) | Versioned LSM keys plus a value log | Evidence that versioned iteration and retention fit an LSM | It does not supply branch lineage, and its value-log lifetime problem is the same cross-branch hazard as SurrealKV's current VLog |
| [ForkBase](https://www.vldb.org/pvldb/vol11/p1137-wang.pdf) | Content-addressed POS-trees and version DAGs | Independent evidence for immutable sharing and explicit version lineage | It is another persistent-tree design, not an LSM implementation |
| [BranchBench](https://arxiv.org/abs/2604.17180) | Branch-heavy benchmark shapes for agentic workloads | Fan-out, deep search, simulations, failure reproduction, and branch lifecycle metrics | It validates workload coverage, not the storage algorithm |

The comparison supports a specific conclusion: ordinary LSM engines provide useful machinery, and
Neon demonstrates an LSM-like branch architecture, but none of RocksDB, Pebble, or Badger supplies
the complete embedded branch contract. The missing catalog, lineage, fork caps, reachability, diff,
and promotion layers must be built into SurrealKV.

## 4. Product contract

### Required branch lifecycle

- create an empty branch;
- create a branch from the current state of another branch;
- create a branch from an explicit retained commit version;
- create a branch from a timestamp resolved to a retained commit version;
- get, list, and test existence;
- select a branch as transaction context;
- delete a branch without affecting ancestors or descendants;
- recreate the same branch name without seeing stale data;
- report lineage, generation, head, retained range, inherited depth, and storage pressure.

### Required temporal behavior

- latest reads;
- reads at an exact commit version;
- reads at the latest commit at or before a timestamp;
- key history in commit-version order;
- branch timeline bounds;
- typed resolution outcomes for before-retained-history, after-latest-global-commit, and empty
  history, with explicit strict or clamp policy at API boundaries;
- historical range reads with the same tombstone and TTL semantics as point reads.

### Required change management

- compare two branch heads or retained commit points;
- preview conflicts against a derived merge base;
- promote one branch into another atomically;
- copy selected keys or selected commits;
- restore a commit range by writing a compensating commit;
- preserve source history and never rewrite existing commits;
- report exactly which keys were applied, deleted, skipped, or conflicted.

### Storage versus SurrealDB semantics

SurrealKV owns opaque-byte semantics:

- unchanged, added, removed, or modified key;
- strict byte conflict;
- target unchanged since base;
- source-wins as an explicit low-level policy;
- atomic application with expected-head and expected-generation checks.

SurrealDB owns semantic merging of records, schemas, indexes, graph structures, and derived state.
SurrealKV must expose enough base/source/target information and atomic apply primitives for that
layer to implement capability-aware merges.

## 5. Core durable model

### 5.1 Durable atoms

```rust
pub struct BranchId([u8; 16]);
pub struct BranchGeneration(u64);
pub struct CommitVersion(u64);
pub struct CommitTimestamp(u64); // microseconds
pub struct TableObjectId([u8; 32]);

pub enum ReadSelector {
    Latest,
    AtVersion(CommitVersion),
    AtTimestamp(CommitTimestamp),
}

struct TemporalContext {
    max_version: CommitVersion,
    evaluation_time: CommitTimestamp,
}
```

One transaction receives one `CommitVersion`; every row in the transaction has that version. The
version is globally monotonic across branches. A branch head is the latest commit version applied
to that branch. A read selector is resolved once into a `TemporalContext`; row visibility uses
`max_version`, while TTL uses `evaluation_time`.

### 5.2 Physical row

```text
PhysicalKey = branch_id | storage_space | user_key
InternalKey = PhysicalKey | !commit_version
StorageRow  = InternalKey | commit_timestamp | expires_at | flags | value
```

Requirements:

- physical keys sort ascending;
- the bitwise-inverted big-endian version sorts versions newest-first;
- all versions of one physical key form one contiguous run;
- branch and storage-space bounds are directly seekable;
- decoding cross-checks branch, version, lengths, and checksums;
- future format versions and trailing bytes are rejected.

`storage_space` is an uninterpreted byte or small integer. The storage crate may reserve spaces for
the commit timeline, branch control rows, and change journal; all other values belong to the engine.

### 5.3 Branch catalog

```text
BranchRecord {
    name,
    branch_id,
    generation,
    status,
    head_version,
    created_at_version,
    deleted_at_version,
    parent: optional {
        branch_id,
        generation,
        fork_version,
        fork_timestamp
    }
}
```

Branch names are user-facing aliases. Durable identity is `(BranchId, BranchGeneration)`.

### 5.4 Commit record and timelines

```text
CommitRecord {
    version,
    timestamp,
    branch_id,
    branch_generation,
    first_parent,
    optional_second_parent,
    mutation_count,
    change_digest,
    metadata
}
```

The database-wide commit timeline maps commit timestamp to the globally published commit version.
Timestamps must be non-decreasing in commit-version order. If the wall clock moves backward, clamp
to the previous timestamp or use a hybrid logical clock. Application-provided event time is payload
data and never controls MVCC visibility.

Each branch also keeps a sparse index of its own commits and its capped ancestry. That index powers
history bounds, lineage, merge-base discovery, and retention validation; it does not redefine the
global meaning of a version or timestamp. A derived branch does not copy its ancestor's complete
index. Its effective branch history is its own entries plus ancestor entries, with each ancestor
segment capped by the corresponding fork version. Timeline and history entries follow the same
retention and reachability rules as the rows they select.

### 5.5 Branch-local LSM state

```rust
struct BranchState {
    descriptor: BranchRecord,
    active: MutableTable,
    frozen: Vec<FrozenTable>,
    owned_levels: Vec<Vec<OwnedTable>>,
    inherited_layers: Vec<InheritedLayer>,
    timeline_index: RetainedCommitTimeline,
    active_snapshots: SnapshotTracker,
}

struct InheritedLayer {
    source_branch: BranchId,
    source_generation: BranchGeneration,
    fork_version: CommitVersion,
    tables_by_level: Vec<Vec<TableObjectId>>,
}
```

Table files live in an object namespace and are referenced by manifests. A branch manifest lists
owned tables and inherited table references. Files are not deleted merely because one manifest no
longer lists them.

## 6. Read semantics

### Own rows

- `Latest`: use the current globally published version as `max_version` and the current monotonic
  database clock as `evaluation_time`;
- `AtVersion(V)`: validate `V` against the global published and retained bounds, use `V` as
  `max_version`, and use its global commit timestamp as `evaluation_time`;
- `AtTimestamp(T)`: resolve the global timeline to the latest published `V` at or before `T`, use
  `V` as `max_version`, and preserve the requested `T` as `evaluation_time`.

A branch can have no local commit at `V`; global commit versions are deliberately sparse per
branch. Its visible state is still well-defined as the newest branch-visible row at or below `V`.
The branch history index is consulted to distinguish an empty result from unavailable retained
history.

Snapshots pin `(branch_generation, read_version)` in the branch-local tracker. A read on one branch
must not indefinitely prevent compaction on unrelated branches.

### Inherited rows

For a layer forked at `F`, use:

```text
effective_version = min(requested_version, F)
```

The read path rewrites the child physical key into the inherited source namespace, reads under the
effective cap, rewrites returned rows to the child namespace, and merges them with child-owned rows.
The newest visible child row wins; a child tombstone masks an inherited value. Capping an inherited
version does not cap or replace `evaluation_time`.

Point reads, forward and reverse ranges, prefix scans, history, TTL, and tombstones must use the same
visibility helper. Separate implementations are a correctness risk.

### TTL

TTL is evaluated against `TemporalContext.evaluation_time`. Historical version reads use the
global timestamp of the selected version; timestamp reads use the requested timestamp; latest
reads use the current monotonic database clock. This keeps historical reads deterministic while
still allowing a value to expire during an otherwise idle database. Expiry is a visibility result,
not a synthetic commit, unless a separate sweeper writes an explicit tombstone.

## 7. Fork algorithm

### Current fork

1. Resolve and generation-fence the source and destination.
2. Capture the database's published global version `F` at the fork linearization point.
3. Freeze the source mutable table.
4. Seal the in-fork mutable/frozen delta into an immutable table if it is not already durable.
5. Build inherited descriptors over source tables containing any row `<= F`.
6. Append a durable branch-fork control record to the WAL/metadata log.
7. Install and publish the child state.

The operation pins the source manifest revision and every referenced table until the branch control
record and child manifest are durable. Concurrent source compaction may complete, but reclamation
cannot invalidate the fork's pinned view. Failure releases the pins only after recovery can prove
that no child manifest was published.

### Historical fork

For requested version `F`:

- reject if `F` is below the source's effective retained history or above the database's published
  global frontier;
- share any source table whose minimum version is `<= F`;
- admit straddle tables containing versions both below and above `F`;
- hide above-`F` rows with the inherited read cap;
- seal only unpersisted rows `<= F` when necessary;
- record the exact resolved fork version and timestamp.

Fork cost is `O(branch metadata + unsealed delta)`, not unconditionally `O(1)`. The unsealed delta is
bounded by the configured mutable-table size. Claiming strict constant time would be misleading.

### Nested forks

Each fork flattens its parent's active inherited descriptors into the child descriptor set while
preserving each layer's cap. Configure:

- maximum inherited layers;
- maximum tables per inherited view;
- maximum point-read and scan source count;
- automatic background materialization when any limit is approached.

Materialization writes the child's visible state into child-owned tables and then releases inherited
references after an atomic manifest update. It is never performed inline with a latency-sensitive
read.

## 8. Commit and WAL protocol

The commit record contains branch identity and generation. The required ordering is:

1. validate branch status and generation;
2. validate optimistic conflicts at the transaction's read version;
3. allocate one commit version and monotonic timestamp;
4. encode rows, timeline entry, change-journal entries, and branch-head update as one commit payload;
5. append and sync according to durability policy;
6. apply the complete payload to branch memory state;
7. publish the visible global version and branch head.

Allocation and publication pass through an ordered global commit sequencer. Version `V + 1` cannot
become visible before `V` has a durable committed or aborted outcome. A reserved version that cannot
commit becomes a durable no-op/abort timeline record; it is never silently reused. Group commit may
amortize synchronization, but it must preserve this publication order.

If the WAL append is durable but apply or publication fails, latch the database against further
mutations until recovery. Return a typed `DurableButNotVisible` outcome. Continuing to accept writes
would make branch heads and change journals unreliable.

WAL replay rejects records whose branch generation does not match the catalog generation. This is
what prevents deleted branch data from reappearing after same-name recreation.

Deleting a branch tombstones its catalog generation and blocks new transactions; it does not break
descendants. Source identity and minimal lineage metadata remain until no descendant, checkpoint,
reader, or pending operation references them. Descendants address inherited table objects directly,
so they remain readable after ancestor deletion.

## 9. Compaction and retention

### Owned compaction

A branch compacts its owned tables. Inherited tables are immutable inputs to reads but are not
silently rewritten or deleted by the child's ordinary compactor.

Compaction must preserve:

- the latest branch-visible version;
- versions required by active snapshots;
- versions within configured history retention;
- versions needed by branch fork anchors;
- versions needed by unresolved merge/revert/cherry-pick operations;
- tombstones required to mask lower or inherited state;
- commit timeline and change-journal coverage required by retained commits.

### Reclamation

Before deleting a table object, produce a stable reachability snapshot over:

- every active and deleted-but-not-reclaimed branch manifest;
- every inherited descriptor;
- every checkpoint/snapshot pin;
- every live reader pin;
- pending branch and compaction operations;
- recovery/quarantine state.

Deletion is allowed only if the object is absent from the proven reachable set and the catalog
revision used for the proof is still current. Recovery degradation or an incomplete manifest blocks
reclamation.

### VLog/blob tier

Format V1 of the rewrite stores values inline.

A later blob tier must use immutable objects identified by content hash or another stable object ID.
Blob reclamation uses the same database-wide reachability pass as tables. No branch-local
minimum-file heuristic is sufficient. Blob GC must ship with crash tests proving that readers never
observe dangling references.

## 10. Diff, promotion, selective copy, and restore

### Change journal

Every commit writes LSM rows ordered by branch and commit version:

```text
change/<branch>/<version>/<physical-key> -> mutation kind + optional hashes
```

The journal is an LSM row family, not a B+tree. It duplicates keys and small metadata, not values.
It is committed atomically with user rows.

### Diff

1. Derive the merge base from commit lineage.
2. Union changed keys on both sides after the base using change-journal scans.
3. Resolve each key at base, source head, and target head.
4. Classify added, removed, modified, unchanged, or conflict.
5. Page output and enforce byte/key budgets.

A full three-view scan is retained as a slow oracle for tests and repair diagnostics.

### Promotion

- Fast path: target head equals the recorded fork/base head. Apply source changes atomically.
- Diverged path: perform three-way comparison.
- Default policy: strict; any conflict aborts the entire promotion.
- Optional low-level policy: source wins, with a complete overwrite report.
- Promotion writes a new target commit with source and previous target as parents.
- Source is not mutated.

### Selective copy

Support two distinct operations:

- copy the current value of explicitly selected keys;
- apply explicitly selected commit changes.

Do not overload one API with ambiguous tombstone behavior.

### Restore

Restore writes a compensating commit. For each key touched in the selected range, restore the value
before the range only when later changes do not require preservation under the selected policy.
History is never rewritten.

## 11. Agent-facing API

```rust
let base = db.branch("main")?;
let task = db.fork_branch(
    &base,
    "agent/run-42",
    ForkPoint::Version(base.head()),
    BranchOptions {
        owner: Some("agent-42"),
        lease: Some(Duration::from_hours(1)),
        ..Default::default()
    },
)?;

let mut tx = task.begin_write()?;
tx.set(key, value)?;
let commit = tx.commit().await?;

let preview = db.preview_promotion(&task, &base, DiffBudget::default())?;
db.promote(
    &task,
    &base,
    PromotePolicy::Strict,
    ExpectedHead(preview.target_head()),
).await?;
```

Required operational fields:

- agent/run/task owner;
- idempotency key for branch creation and promotion;
- lease expiry and optional archival policy;
- branch depth and inherited-source counts;
- logical and physical byte estimates;
- commit provenance metadata;
- expected-head preconditions;
- read-only evaluation handles;
- storage, mutation, duration, and branch-count quotas.

Expired leases mark branches reclaimable; they do not synchronously remove files.

## 12. Backend portability and object-storage architecture

### 12.1 Adversarial verdict

One branch engine can support memory, local disk, generic object storage, Cloudflare edge, browser
OPFS, and deterministic simulation. Branch semantics do not need backend-specific implementations.
They stay above the IO boundary: commit versions, fork caps, inherited views, tombstones, timelines,
diff, promotion, and reachability operate on immutable object IDs and durable roots.

This works only if the rewrite is object-first and async-capable from the beginning. A filesystem
trait added after an append-oriented WAL and path-oriented table runtime are complete will not be
enough.

Strata gets the following design choices right:

- canonical object names rather than paths;
- opaque backend fences rather than exposed rename/fsync sequences;
- explicit, open-time capability validation;
- immutable table objects plus manifest reachability;
- a conformance suite shared by memory and local backends;
- typed publication outcomes that distinguish not-visible, visibility-unknown, and
  visible-but-durability-unconfirmed failures;
- fault-injecting storage as a peer backend rather than test-only mocks.

Strata's current object-durable guardrail also explicitly says its production S3/R2 mode is not yet
proved. SurrealKV must not copy the interface and infer that the durability proof follows. SlateDB
fills in many concrete object mechanics, but its clone layout is not the desired branch model. The
remaining SurrealKV proof is the object-native commit log, conditional root publication, recovery
without trusting listing, multi-object failure handling, branch-wide reachability, and object-aware
reclamation economics.

### 12.2 One semantic engine, composed storage roles

Do not expose one giant filesystem-shaped backend. Compose three behavior roles plus explicit
capability/profile facts:

```rust
pub struct BackendBundle {
    pub objects: Arc<dyn ImmutableObjectStore>,
    pub authority: Arc<dyn CommitAuthority>,
    pub cache: Option<Arc<dyn BlockCacheStore>>,
    pub capabilities: BackendProfile,
}

#[async_trait(?Send)]
pub trait ImmutableObjectStore {
    async fn get(&self, id: &ObjectId) -> Result<Bytes>;
    async fn get_range(&self, id: &ObjectId, range: ByteRange) -> Result<Bytes>;
    async fn put_if_absent(&self, id: &ObjectId, body: ByteStream) -> Result<PutOutcome>;
    async fn metadata(&self, id: &ObjectId) -> Result<ObjectMetadata>;
    async fn delete(&self, id: &ObjectId) -> Result<DeleteOutcome>;
}

#[async_trait(?Send)]
pub trait CommitAuthority {
    async fn load_root(&self) -> Result<AuthorityRoot>;
    async fn commit(
        &self,
        expected: AuthorityFence,
        proposal: CommitProposal,
    ) -> Result<CommitOutcome>;
}
```

`CommitAuthority` has several mechanical implementations but one semantic contract:

- memory: mutex-protected state;
- local disk/OPFS: framed append log plus checksummed root slots;
- generic object storage: immutable commit chunk plus conditional root compare-and-swap;
- Cloudflare edge: one SQLite-backed Durable Object transaction;
- simulation: deterministic state machine with scheduled failures.

The authority assigns or accepts exactly the next globally publishable version, makes the commit
durable under the selected profile, and advances the root fence atomically. Branch code never sees
WAL offsets, SQLite row IDs, ETags, local paths, R2 bindings, or OPFS handles.

Target-specific runtime modules hide native threads, io_uring, JavaScript Promises, and
single-threaded WASM polling. Do not introduce an executor trait until two production executor
implementations need behavior selected at runtime. The core storage API is async. Native
memory/local builds may provide a blocking facade; Workers and browser WASM must use the async API
because blocking a Promise-backed R2 or OPFS call is not implementable safely in a single-threaded
isolate.

SurrealKV owns these narrow traits. OpenDAL may implement `ImmutableObjectStore` for native
S3-compatible deployments, but it is an adapter rather than the architectural contract. It does
not supply `CommitAuthority`, and it cannot by itself model the composed Durable Object SQLite + R2
edge profile. Direct R2-binding and OPFS adapters remain first-party implementations of the same
narrow SurrealKV traits.

### 12.3 Capability profile

Capabilities are semantic and quantitative, not only booleans:

```text
durability: Ephemeral | DeviceBestEffort | SingleHost | ReplicatedObject
commit_protocol: Memory | AppendAndSync | ConditionalRoot | TransactionalCoordinator
read_after_write: Strong | Session | Eventual
range_read: native | emulated(max_object_bytes)
conditional_publish: none | create_only | compare_and_swap
listing: authoritative | strong_but_advisory | eventual_advisory
execution: NativeAsync | DedicatedThread | WorkerPromise | DedicatedWebWorker
limits: max_object, max_buffer, max_parallel_io, quota, preferred_table_size
```

Open selects a proven protocol from the profile and fails before mutation when requirements are
missing. It must never accept a provider merely because an adapter compiles. A backend unable to
delete can still be correct in leak-only mode; one unable to durably fence the authoritative root
cannot accept durable writes.

### 12.4 Canonical object model

Use one object-shaped namespace on every backend:

```text
meta/identity
roots/<root-generation-or-hash>
commits/<commit-object-id>
tables/<table-object-id>
blobs/<blob-object-id>
snapshots/<snapshot-object-id>
operations/<operation-object-id>
quarantine/<object-id>
```

The mutable authority root is backend-private. On object storage it is a small conditional `HEAD`
object or coordinator row; on local disk it is a durable root slot; in a Durable Object it is a
SQLite row. It points to immutable, checksummed metadata and data objects.

Table and blob object names do not include an owning branch. Branch manifests reference global
object IDs, which permits ancestor/descendant sharing, cache reuse, relocation between hot and cold
tiers, and database-wide reachability without renaming objects. Each object carries format version,
length, checksum, and creation provenance. Content hashes are preferred IDs; if encryption or
streaming construction requires random IDs, a separate full-object digest remains mandatory.

Listing is never the recovery source of truth. Recovery starts at the fenced root and traverses
immutable references. Listing is used only for orphan discovery, diagnostics, and GC. This avoids
making correctness depend on the weakest S3-compatible listing behavior.

### 12.5 Object-native publication protocol

For generic object storage, candidate root and commit keys are content hashes or cryptographically
random never-reused IDs, never reusable sequence numbers:

1. Load the current authority root and opaque fence.
2. Build the proposed globally ordered commit group in memory or a bounded stream.
3. Upload the immutable commit object and any newly sealed table/metadata objects with
   put-if-absent semantics.
4. Verify object identity, length, and checksum.
5. Conditionally replace the small authority root against the old fence.
6. Only after step 5 succeeds publish the new version to readers.
7. Treat a failed precondition as a concurrency retry. Treat timeout-after-CAS as visibility
   unknown; re-read the authority root and walk immutable parent links. If the candidate is in the
   current lineage, return success; if the expected root is still current, retry; if a different
   child won, rebuild against the current root.
8. Leave failed-proposal objects as harmless orphans for later reachability GC.

`HEAD` is created once during bootstrap and is never garbage-collected. Its disappearance after
initialization is corruption and recovery fails closed. The first certified object profile holds
one renewable fenced writer session; CAS remains the publication fence and protects against stale
or overlapping sessions. The writer epoch is part of `HEAD`, and takeover CAS advances it even when
the root ID is unchanged; a separately checked lease object is not a sufficient fence. Durable
remote reader leases are required before unbounded multiprocess snapshots are claimed.

The root CAS is the single-writer fence and global version linearization point. It will serialize
publication across branches. Group commit amortizes its latency, but object-only mode must publish a
measured contention and cost budget; it cannot promise local-NVMe commit latency.

SSTables must be remote-read friendly: independently checksummed blocks, a footer that locates the
index and filters, native byte-range reads, coalesced/prefetched blocks, streaming construction, and
backend-tuned table size. Compaction uploads output before root publication and streams inputs so it
never requires one table-sized buffer.

### 12.6 First-party profiles

| Profile | Commit authority | Immutable objects / cache | Branch semantics | Honest qualification |
| --- | --- | --- | --- | --- |
| Memory | In-process mutex | RAM / none | Full | Ephemeral; reopen starts empty |
| SimStorage | Deterministic simulated authority | Simulated objects/cache | Full | Test peer; can inject stale list, CAS race, partial read, timeout-after-success, quota, eviction, and crash |
| Local disk | Append+sync journal and durable root slots | Local immutable tables; optional block cache | Full | Reference crash-durable single-host mode |
| Object only | Conditional root CAS plus fenced writer session | R2/S3-family immutable objects; memory cache | Full | First certification allows one active writer and bounded remote snapshots; strong reads, native ranges, proven CAS, and explicit request budgets are required |
| Cloudflare edge | One SQLite-backed Durable Object per database | DO SQLite hot log/catalog plus R2 immutable cold tables; isolate memory cache | Full | Recommended Cloudflare write mode; SQLite and R2 are not one transaction, so R2 promotion uses durable intents |
| Browser OPFS | Dedicated-worker append/root protocol | OPFS immutable objects plus memory block cache | Full | Device-local best-effort durability; quota, user clearing, and browser eviction must be surfaced |
| Server tiered | Local or external commit authority, chosen explicitly | NVMe hot cache/tier plus object-store cold tables | Full | Must declare which tier is authoritative; an asynchronous backup is not transparent failover |

Configuration should change the profile, not the branch API:

```rust
SurrealKv::open(Profile::Memory).await?;
SurrealKv::open(Profile::Local(path)).await?;
SurrealKv::open(Profile::Object(object_config)).await?;
SurrealKv::open(Profile::Cloudflare(bindings)).await?;
SurrealKv::open(Profile::Opfs(database_name)).await?;
```

### 12.7 Cloudflare edge protocol

Use one SQLite-backed Durable Object as the database's commit authority and branch catalog for the
first implementation. It provides the ordered writer, transaction boundary, hot commit rows,
generation fences, leases, pending-upload intents, and authoritative root. R2 stores immutable
SSTables, snapshots, and later blobs.

SQLite and R2 cannot participate in one atomic transaction. Use an intent protocol:

1. In a Durable Object transaction, record a flush/compaction intent and pin every input object.
2. Stream the immutable output to R2 and verify its metadata/checksum.
3. In a second Durable Object transaction, install the output in the branch/global reachability
   state and mark the intent installed.
4. Retain old inputs until a later reachability epoch proves them dead.
5. On restart, retry an unuploaded intent, validate and install an uploaded intent, or quarantine an
   ambiguous object. Never infer success from an R2 listing alone.

This fits Cloudflare because R2's Worker API provides ranged reads and conditional operations, and
R2 documents strong consistency. It still needs platform-shaped engineering:

- keep compaction and scans streaming under the Worker isolate memory limit;
- split maintenance into resumable bounded steps driven by alarms/queues;
- budget R2 subrequests and coalesce table block reads;
- keep each Durable Object below its SQLite storage ceiling by offloading sealed history;
- use the Durable Object, not R2 last-writer-wins behavior, as the multi-request sequencer.

One Durable Object per database is intentionally a correctness-first bottleneck. Before sharding
the authority, measure branch fan-out and commit throughput. Per-branch Durable Objects would make
fork, promotion, global versions, and database-wide GC distributed transactions; do not adopt that
shape without a separate protocol and proof.

### 12.8 Browser OPFS protocol

Run the engine inside a dedicated Web Worker and use synchronous OPFS access handles there. Keep
the public JavaScript boundary async. The sync handle gives byte-range read/write, truncate, flush,
and an exclusive file lock, but OPFS does not justify POSIX rename assumptions.

Use a framed append journal with checksums plus two alternating root slots. Recovery selects the
highest fully valid generation and ignores a partial journal tail. Request persistent browser
storage where available, report quota/usage, and return typed quota/eviction errors. OPFS remains
device durability, not replicated durability; site-data clearing can delete the entire database.

### 12.9 Server NVMe plus object cold tier

Support two explicit policies:

- object-authoritative: commits and immutable objects become durable remotely; NVMe is a disposable
  block/table cache;
- local-authoritative: the local journal/root is the durability boundary and cold upload is archive
  or capacity management, not failover protection until an upload watermark is durable.

For tier relocation, keep a global `ObjectId -> verified locations` registry in immutable metadata.
Upload and verify the cold copy, publish a dual-location state, then drop the local location only
after reader pins and reachability permit it. io_uring, mmap, direct IO, and filesystem preallocation
belong entirely inside the native local implementation and do not leak into table or branch logic.

### 12.10 What does not fit

The following promises are rejected:

- one synchronous API implementation for local files and Promise-backed Worker/browser storage;
- identical latency, throughput, durability, or availability across profiles;
- POSIX append/rename/fsync as the universal durability protocol;
- an SSTable containing mutable local VLog offsets that remains portable to object storage;
- atomicity across Durable Object SQLite and R2 without an intent/recovery protocol;
- using bucket listing to decide authoritative recovery state;
- per-branch Durable Objects while retaining atomic cross-branch promotion and one global commit
  order without distributed coordination;
- treating OPFS as replicated or non-evictable storage;
- calling an asynchronous cold-tier copy a durable failover copy before its watermark is committed.

### 12.11 Portability gates

Before branch implementation hardens the wrong seams:

1. Freeze the object ID, table footer/range-read, authority root, capability, and async execution
   contracts in M1.
2. Make Memory, Local, and SimStorage pass one semantic suite in M2.
3. Run every M3-M8 branch model test on those three profiles.
4. Add object-only conformance before claiming S3/R2 durability.
5. Add Cloudflare miniflare/workerd plus real R2/DO smoke tests before claiming edge support.
6. Add browser-engine OPFS reopen, quota, exclusive-open, and eviction tests before claiming browser
   persistence.
7. Keep semantic parity and durability certification separate: a backend can produce identical
   branch results while honestly carrying a weaker durability class.

### 12.12 Evidence reviewed

- Strata's [L1 backend IO](/Users/kfarhan/workspace/projects/strata-core/docs/architecture/storage/l1-backend-io.md),
  [object layout](/Users/kfarhan/workspace/projects/strata-core/docs/architecture/storage/l2-object-layout.md),
  [durable services](/Users/kfarhan/workspace/projects/strata-core/docs/architecture/storage/l4-log-manifest-snapshot-services.md),
  [portability plan](/Users/kfarhan/workspace/projects/strata-core/docs/storage/storage-portability-plan.md),
  and [object-durable guardrails](/Users/kfarhan/workspace/projects/strata-core/docs/architecture/storage/future-object-durable-guardrails.md).
- Strata's implemented backend capability contract, conformance suite, local backend, memory
  backend, publish outcome types, and explicit `object-durable-candidate` validation under
  `crates/storage/src/backend/` and `crates/storage/src/config/mode.rs`.
- SlateDB's immutable sequenced manifest implementation, fenced writer/compactor epochs,
  retrying object store with lost-response identity, range-oriented `TableStore`, separated WAL
  store, cached object wrapper, clone/checkpoint machinery, compaction-state persistence, and GC
  boundary/ULID failure analyses at local revision
  `4b45c2f01fda568eff47e50012a63f9ddf1517a8`.
- Cloudflare's [R2 Workers API](https://developers.cloudflare.com/r2/api/workers/workers-api-reference/),
  [R2 consistency contract](https://developers.cloudflare.com/r2/reference/consistency/),
  [SQLite-backed Durable Object API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/),
  [Durable Object limits](https://developers.cloudflare.com/durable-objects/platform/limits/), and
  [Workers limits](https://developers.cloudflare.com/workers/platform/limits/).
- MDN's [OPFS overview](https://developer.mozilla.org/en-US/docs/Web/API/File_System_API/Origin_private_file_system),
  [synchronous access handle contract](https://developer.mozilla.org/en-US/docs/Web/API/FileSystemSyncAccessHandle),
  and [browser quota/eviction rules](https://developer.mozilla.org/en-US/docs/Web/API/Storage_API/Storage_quotas_and_eviction_criteria).

## 13. Implementation roadmap

Each milestone ends in a shippable invariant boundary. Later work must not be merged when the current
gate is red.

These are architecture milestones. The PR-sized implementation/test slices, dependency DAG, and
release cut lines are authoritative in
[BRANCHING_IMPLEMENTATION_PLAN.md](BRANCHING_IMPLEMENTATION_PLAN.md).

### M0: Lock the B+tree removal and delete residual dual-time semantics

Work:

- retain commit `2c9e8f3` as the no-B+tree baseline and add a regression check that the module,
  benchmark, options, and dependencies do not return;
- remove `TimestampComparator`, `new_for_history`, and timestamp-based seek assumptions;
- remove public `set_at`/per-entry MVCC timestamps; preserve application event time only as ordinary
  value or commit metadata;
- reduce current history reads to the canonical sequence/version-ordered LSM path before replacing
  its format in M2;
- remove residual B+tree/versioned-index wording from docs and errors;
- do not automatically delete old `versioned_index/` directories; the rewrite does not open old
  stores at all.

Gate:

- the repository still contains no B+tree production code, dependency, benchmark, or public option;
- all surviving tests use one version-ordered LSM iterator path;
- no API can assign MVCC visibility time per entry;
- format/API break is documented.

### M1: Freeze the new contracts

Work:

- specify durable atoms, physical/internal keys, rows, WAL payloads, branch catalog, branch manifests,
  commit timeline, and checksums;
- freeze the async engine boundary, `BackendBundle`, commit-authority contract, capability profile,
  canonical object namespace, immutable table footer, and ranged-read contract;
- define `ReadSelector`, TTL, tombstone, retained-history, and error semantics;
- produce golden byte vectors and corruption tests before implementing higher layers.

Gate:

- every durable object is versioned, checksummed, bounds-checked, and has golden tests;
- commit version is the sole MVCC visibility order;
- there is no arbitrary user-controlled commit timestamp;
- no format or engine contract requires a path, rename, mmap, file descriptor, fsync, appendable
  remote object, ETag, SQLite row ID, JavaScript runtime, or provider name.

### M2: Build the single-branch LSM kernel

Work:

- implement the new row/table format with inline values;
- implement mutable/frozen/immutable tables, blooms, block cache, range cursors, leveled compaction,
  manifest, WAL, and recovery;
- implement Memory, Local, and SimStorage backend bundles against the same async storage protocol;
- implement one-version-per-transaction publication;
- implement latest, at-version, history, tombstones, TTL, and retention;
- implement the commit timeline and timestamp resolution.

Gate:

- single-branch crash consistency and MVCC model tests pass;
- Memory, Local, and SimStorage pass the same semantic and backend-conformance suites;
- timestamp reads are implemented only as timeline resolution plus at-version reads;
- compaction is observationally equivalent to an uncompacted reference model.

### M3: Add branch identity and lifecycle

Work:

- add branch-aware key encoding and branch-local state registry;
- persist branch catalog and generations;
- add empty create, get, list, exists, delete, and same-name recreation;
- add branch-aware transactions, conflict keys, WAL records, recovery, and metrics;
- protect default/system branches.

Gate:

- concurrent writes to different branches do not conflict;
- stale-generation transactions and WAL records are rejected;
- deletion never leaks data into same-name recreation;
- every lifecycle crash point has a recovery test.

### M4: Add COW current and historical forks

Work:

- introduce global table object IDs and inherited manifests;
- implement current, version, and timestamp fork;
- implement cross-branch key rewriting and merged MVCC reads;
- support straddle tables and bounded unsealed-delta sealing;
- add inherited-depth limits, materialization, and branch storage reports.

Gate:

- forked children are isolated from parent post-fork writes;
- child tombstones shadow ancestors in points, ranges, reverse scans, and history;
- historical forks exactly match source `AtVersion(F)` scans;
- fork cost is independent of total dataset size when no materialization is required;
- deep-chain tests stay within configured read-source bounds.

### M5: Prove reachability and reclamation

Work:

- implement table reader pins and catalog revisions;
- compute all-branch table reachability;
- make compaction publication and file deletion separate phases;
- block GC under incomplete/degraded recovery;
- implement branch retention floors and materialization-based release;
- add checkpoints and branch-aware snapshot install only after reachability is stable.

Gate:

- randomized branch/create/write/compact/delete/reopen tests never lose reachable data;
- injected crashes at every manifest and deletion boundary recover safely;
- no table is deleted while referenced by a branch, reader, checkpoint, or pending operation;
- orphaned objects are reported before they are reclaimable.

### M6: Add change journal and branch comparison

Work:

- write change-journal and commit-DAG records atomically with commits;
- implement merge-base, bounded diff, pagination, and a full-scan comparison oracle;
- add retention requirements for journal and base versions.

Gate:

- journal diff equals full-scan diff under randomized histories;
- diff cost tracks changed keys rather than total dataset size in the normal path;
- unavailable base history returns a typed error rather than an incomplete diff.

### M7: Add promotion, copy, and restore

Work:

- implement expected-head fast-forward promotion;
- implement strict three-way conflict detection and optional source-wins;
- implement selected-current-value copy and selected-change apply as separate APIs;
- implement compensating restore and merge commits with two parents;
- expose semantic merge hooks to SurrealDB without interpreting values in SurrealKV.

Gate:

- promotion is all-or-nothing;
- source is unchanged;
- conflict reports contain base/source/target facts;
- recovery cannot publish half a promotion;
- restore never mutates old commits.

### M8: Add the agent control plane

Work:

- leases, quotas, idempotency, provenance, archival, and expected-head workflows;
- bulk branch create/evaluate/prune APIs;
- branch lifecycle notifications and metrics;
- bounded branch-operation concurrency and backpressure.

Gate:

- wide fan-out, deep search, simulation, failure-reproduction, and data-curation workloads complete
  within explicit budgets;
- lease expiry cannot race a live transaction or promotion;
- repeated idempotent requests return the same outcome.

### M9: Optional immutable blob tier

Work:

- content-addressed immutable blob objects;
- table rows reference blob IDs plus length/checksum;
- all-branch blob reachability and reader pins;
- crash-safe blob rewrite/reclamation;
- inline threshold and migration within the new format only.

Gate:

- blob-enabled results are byte-identical to inline-value results;
- no dangling blob reference is possible across fork, compaction, delete, checkpoint, or recovery;
- GC failures leak space rather than lose data.

### M10: Add generic object-only durability

Work:

- implement immutable commit chunks and conditional authority-root publication;
- implement unique candidate-root IDs, immutable parent lineage, bootstrap-only `HEAD` creation,
  and a renewable fenced writer session;
- add native S3-family and Cloudflare R2-binding object adapters;
- implement range-read/coalescing, streaming table upload, retries, and timeout-after-success
  reconciliation;
- make root traversal authoritative and listing advisory;
- add orphan inventory, cost accounting, and object-aware compaction sizing.

Gate:

- capability probes reject missing strong-read, range-read, or compare-and-swap guarantees before
  database mutation;
- MinIO/S3-family conformance, real R2 smoke, crash/fault, and lost-response tests pass;
- overlapping/stale writer sessions are fenced, and a defensive CAS race produces one published
  order and no fabricated version;
- request count, bytes transferred, commit latency, and orphan growth stay within explicit budgets;
- full branch semantics match Memory and Local under the parity suite.

### M11: Add Cloudflare Durable Object plus R2 edge profile

Work:

- implement the SQLite-backed Durable Object commit authority and branch catalog;
- keep hot commit/recovery state in SQLite and immutable tables/snapshots in R2;
- implement durable upload/compaction intents and recovery reconciliation;
- make maintenance resumable through bounded alarms/queues;
- add streaming compaction and subrequest/memory budgeting.

Gate:

- workerd/miniflare tests and deployed R2/DO smoke tests pass;
- injected failure at every SQLite-intent/R2-upload/install boundary recovers safely;
- no transaction is acknowledged before its selected edge durability boundary;
- wide agent fan-out is benchmarked against the one-Durable-Object sequencer and has an explicit
  supported throughput envelope;
- the branch parity suite matches Local byte-for-byte at the API level.

### M12: Add browser OPFS profile

Work:

- run the async engine in a dedicated Web Worker;
- implement sync-access-handle journal, range reads, flush, exclusive open, and dual root slots;
- expose quota, persistence grant, and durability diagnostics;
- use wasm-compatible checksums/compression with no hidden native thread or mmap dependency.

Gate:

- Chromium, Firefox, and WebKit target tests cover write/close/reopen, partial tail, root-slot tear,
  quota exhaustion, exclusive open, storage clearing, and best-effort eviction classification;
- no IndexedDB fallback is silently selected when the profile requests OPFS;
- the branch parity suite matches Memory within the browser harness.

### M13: Add server NVMe plus object cold tier

Work:

- implement native io_uring/read-ahead/preallocation only inside the local adapter;
- implement the global verified-location registry and resumable hot-to-cold relocation;
- support object-authoritative-with-NVMe-cache and local-authoritative-with-cold-archive as distinct
  policies;
- add cache admission, eviction, corruption fallback, and remote rehydration.

Gate:

- every configuration reports one unambiguous durability authority and upload watermark;
- a crash at every relocation boundary leaves at least one verified reachable copy;
- deleting a local hot copy cannot break a descendant branch or pinned reader;
- warm-cache performance and cold-read/object-cost budgets are published.

## 14. Test strategy

### Model-based testing

Build a simple in-memory reference model containing:

- branch catalog and generations;
- global commit order;
- per-branch mutation history;
- fork anchors;
- tombstones and TTL;
- diff, promotion, copy, restore, and deletion.

Generate operations and compare every point/range/history result between the model and the LSM after
each step, after compaction, and after reopen.

### Backend parity and conformance

Run the same generated operation trace and compare canonical API results on Memory, SimStorage,
Local, Object, Cloudflare Edge, and OPFS. Normalize only backend diagnostics and durability class;
never normalize row visibility, branch lineage, commit order, conflicts, or diff results.

Each backend additionally runs a capability-specific suite for range boundaries, put-if-absent,
conditional publication, lost responses, metadata/checksum validation, quota, delete outcomes,
exclusive writer fencing, and open-time rejection. SimStorage must reproduce every failure class
without real sleeps or nondeterministic timing.

### Required randomized operations

- create, fork current/version/timestamp, delete, and recreate;
- point and range writes, tombstones, and multi-key transactions;
- forward/reverse scans and history at multiple selectors;
- mutable-table freeze, flush, and every compaction shape;
- nested fork, materialize, and ancestor deletion;
- strict/source-wins promotion, selected copy, and restore;
- checkpoint, reopen, WAL replay, and fault injection.

### Crash matrix

Inject failure before and after:

- WAL append and sync;
- memtable apply;
- visible-version publish;
- source delta seal;
- branch control-record append;
- child manifest install;
- catalog publication;
- compaction output sync;
- manifest replacement;
- immutable commit/table upload;
- conditional authority-root CAS and lost CAS response;
- Durable Object intent commit, R2 upload, and install transaction;
- OPFS journal frame, flush, and root-slot switch;
- hot-to-cold upload, location-registry publication, and hot-copy release;
- reachability proof;
- table unlink;
- promotion payload publication.

For each point, reopen must produce either the complete old state or complete new state, never a
mixture.

### Concurrency matrix

- parent writes concurrent with fork;
- child writes concurrent with parent compaction;
- branch delete concurrent with transaction begin/commit;
- materialization concurrent with reads;
- promotion concurrent with target writes;
- GC concurrent with branch create and checkpoint;
- same-name recreate concurrent with stale WAL replay.

## 15. Benchmarks and budgets

Measure both lifecycle agility and branch-local performance.

### Branch lifecycle

- fork latency versus total dataset size and active-delta size;
- create/list/switch/delete latency at 1, 10, 100, 1,000, and 10,000 branches;
- wide fan-out and deep-chain creation throughput;
- materialization latency and bytes rewritten.

### Branch-local work

- point reads, prefix/range scans, writes, and compaction by inheritance depth;
- bloom/filter effectiveness across inherited sources;
- read amplification and open file count by branch count;
- cache isolation and fairness between hot and cold branches.

### Change operations

- diff versus total rows and changed rows;
- merge-base lookup by DAG depth;
- promotion and conflict-preview latency;
- journal storage amplification.

### Safety/resource budgets

- maximum inherited layers consulted per read;
- maximum open table readers;
- maximum live branches and pending lifecycle operations;
- maximum unreclaimed bytes per deleted branch;
- recovery duration by branch/table/WAL count.

### Backend economics and limits

- object GET/HEAD/PUT/DELETE/list calls per point read, scan MiB, commit, flush, compaction, fork,
  promotion, and GC epoch;
- remote bytes transferred and cache hit/byte-hit rates;
- root-CAS retries and group-commit efficiency under cross-branch writers;
- Cloudflare isolate peak memory, CPU, subrequests, Durable Object SQLite bytes, and resumable-job
  duration;
- OPFS quota use, flush latency, reopen latency, and dedicated-worker message overhead;
- NVMe-to-cold-tier lag, dual-location bytes, rehydration latency, and archive/failover watermark.

Run BranchBench-inspired shapes: shallow fan-out, deep speculative chains, Monte Carlo simulation,
Monte Carlo tree search, failure reproduction, and independent data-cleaning branches.

## 16. Review gates

The project should pause for an adversarial design review at these boundaries:

1. After M1: byte format, visibility, and timestamp semantics.
2. After M3: branch generation and lifecycle recovery.
3. After M4: inherited-read correctness and depth policy.
4. After M5: deletion/reclamation proof.
5. After M7: conflict and atomic-promotion semantics.
6. Before M9: blob reachability and GC proof.
7. Before M10: async/backend contract, root CAS, and listing-independent recovery.
8. Before M11: SQLite/R2 intent atomicity and one-Durable-Object throughput envelope.
9. Before M12: OPFS durability classification, quota, and browser support matrix.
10. Before M13: tier authority, relocation safety, and failover claims.

Each review must include counterexamples, fault injection, model-test evidence, benchmark results,
and a written list of remaining assumptions. Passing unit tests alone is not sufficient.

## 17. Final go/no-go criteria

The branch-native engine is ready for integration only when:

- the B+tree and alternate history path are gone;
- all temporal reads reduce to commit-version visibility;
- current and historical forks share immutable tables safely;
- nested branches have bounded read amplification;
- branch deletion and compaction cannot reclaim reachable data;
- crash recovery preserves catalog, generation, manifest, timeline, and branch-head agreement;
- change-journal diffs match the full-scan oracle;
- promotion is atomic and expected-head guarded;
- agent branches have leases, quotas, provenance, and idempotency;
- all first-party profiles preserve the same branch and temporal semantics;
- each durable profile has a separately passed fencing, publication, recovery, and reclamation
  certification; unsupported capability combinations fail before mutation;
- object recovery traverses the authoritative root and does not trust listing;
- Cloudflare SQLite/R2 and server hot/cold transitions use durable intents and never expose an
  unverified object;
- OPFS is reported as device-local best-effort durability rather than replicated durability;
- model-based, randomized, crash, concurrency, and branch-shaped benchmark suites are green.

The key policy is simple: uncertainty may leak disk space or stop writes, but it must never fabricate
history, cross branch boundaries, or delete reachable state.
