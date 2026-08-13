# Branch-local runtime design over SurrealKV's shared WAL

Status: accepted architecture; implementation has not started

Updated: 2026-08-13

## Decision

Do **not** replace `CoreInner::active_memtable` with a map of active memtables in isolation.
Instead, extract the existing default-branch LSM state into a branch-local aggregate and only then
make that aggregate lazy and owner-indexed.

```text
Database runtime
├── one CommitPipeline / global CommitVersion clock
├── one WAL and WAL dependency tracker
├── one durable database root / branch catalog
├── one global table-id allocator and write-buffer budget
└── BranchRuntimeRegistry
    ├── main@0 -> BranchRuntime
    │   ├── active memtable (allocated lazily)
    │   ├── immutable memtables
    │   ├── owned L0..Ln
    │   ├── inherited immutable views
    │   └── branch snapshot/maintenance facts
    └── agent/work@0 -> BranchRuntime
        └── same component shape, with no separate WAL or task manager
```

Every memtable and SST remains branch/generation-pure. User keys, the internal-key comparator,
data blocks, indexes, and bloom filters remain free of branch prefixes.

The shared WAL follows the established column-family model: branches do not share memtables or
SSTs, but do share a WAL. A branch can rotate independently. An old WAL segment is reclaimable
only after every active/immutable branch component and every durable-but-not-yet-applied commit has
released its dependency on that segment.

## Why this is the correct unit

SurrealKV's current single active memtable is coupled to all of these:

- commit apply and arena-full retry;
- WAL rotation and cleanup;
- immutable ordering and replay floors;
- L0 flush and manifest publication;
- L1+ non-overlap and binary search;
- compaction selection and stalls;
- snapshot point reads and merged iterators;
- checkpoint, close, reopen, and repair recovery.

Changing only the active pointer would make the upper tier branch-aware while the lower tiers
still assume one keyspace. In particular, two branch-pure L1 tables may contain the same user-key
range. Putting both in today's global `Level` violates its non-overlap invariant even if reads later
filter by owner. The branch-local aggregate therefore has to own the whole component set.

## Evidence and limits of analogy

### Strata

Strata's `BranchLocalState` owns active, frozen, owned levels, and inherited layers together
(`crates/storage/src/branch/state.rs`). Its rotation is branch-local
(`crates/storage/src/branch/state/rotation.rs`), and a captured read view pins that complete source
set (`crates/storage/src/branch/state/read_hooks.rs`). This supports the aggregate boundary.

We are not copying Strata's physical-key prefix. Strata needs cross-branch key rewriting because
the branch is encoded into every physical key. SurrealKV can retain branch-pure components and use
the unchanged user/internal key inside each component.

We also cannot copy Strata's rotation code directly. Its durability and maintenance layers track
flush coverage differently. SurrealKV's current recovery authority is a numbered WAL replay floor,
so shared-WAL dependencies must be explicit here.

### RocksDB column families

RocksDB column families share one WAL but have separate memtables and table files. Flushing one
column family rolls the WAL; older WALs remain live until every column family with data in them has
flushed. RocksDB also uses a database-wide write-buffer budget and forces cold components to flush
when old WAL retention grows.

This is the closest durability precedent for SurrealKV's no-prefix design. It also exposes the
main operational risk: a cold dirty branch can pin old WAL segments indefinitely unless the engine
has an oldest-dependency flush policy.

References:

- <https://github.com/facebook/rocksdb/wiki/Column-Families>
- <https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-%28WAL%29>
- <https://github.com/facebook/rocksdb/wiki/MemTable>

### SlateDB

SlateDB is useful for immutable-object publication, manifest/checkpoint references, and shallow
database clones. It does not solve the in-process multi-branch mutable-state problem: a clone is a
new database/manifest rather than another branch-local component set sharing one commit pipeline.
Its lesson applies later to inherited table references and object-store publication, not to the
active-memtable container.

### Dolt

Dolt obtains branching, structural sharing, and change-proportional diffs from content-addressed
Prolly trees plus a commit graph. Replacing SurrealKV's LSM with that data structure would be a
different engine rewrite and would discard the retained memtable/SST/compaction path. The reusable
ideas are immutable roots, explicit reachability, and commit/change metadata—not its mutable write
buffer.

References:

- <https://www.dolthub.com/blog/2024-02-29-storage-engine/>
- <https://www.dolthub.com/blog/2020-05-13-dolt-commit-graph-and-structural-sharing/>

## WAL and rotation protocol

### Required provenance

The WAL segment that durably contains a batch must be captured under the WAL append lock and
carried into memtable apply. Reading `wal.active_segment()` later is incorrect because apply runs
outside the commit write mutex and another thread may rotate in between.

Each mutable or immutable memtable tracks its **earliest actual WAL segment**, not merely the WAL
that happened to be current when the memtable was constructed. A memtable may span multiple WAL
segments; the earliest segment is sufficient to keep replay conservative.

That fact is registered in one database-wide `WalDependencyTracker`. The tracker, rather than a
scan across unrelated branch locks, is the authority for the physical WAL replay floor. It has
typed dependencies for active/immutable components and for durable in-flight commits. Moving a
memtable from active to immutable transfers the component token without changing its dependency;
flushing removes the token only after the SST and root/manifest transition are durable.

### Durable-but-not-applied pin

There is a second dependency not represented by any memtable: a batch can be appended durably and
then wait before apply. Before releasing the WAL append critical section, the commit pipeline must
register an in-flight dependency on the actual segment. It releases that dependency only after
atomic memtable apply succeeds. Apply failure leaves the engine fenced and the dependency pinned
until recovery.

After successful apply, dependency ownership moves atomically inside the tracker: first register or
lower the destination memtable's component dependency, then release the commit's in-flight
dependency in the same tracker critical section. There is no observable state in which neither
dependency protects the segment.

The safe physical replay floor is:

```text
min(
  every active/immutable component dependency in WalDependencyTracker,
  every durable-but-not-applied dependency in WalDependencyTracker,
  current WAL segment
)
```

Manifest/root publication and WAL cleanup use the same versioned tracker snapshot. They must not
independently derive a floor. A branch may additionally persist a logical flush/version watermark
for fork coverage and maintenance, but there is only one database-wide physical WAL replay floor.

### Independent branch rotation

When owner `B` returns `ArenaFull`:

1. Re-resolve and generation-check `B`.
2. Acquire `B`'s active-slot write lock and retry against the current active memtable. Another
   writer may already have rotated it.
3. Roll the shared WAL while excluding concurrent append to the old segment.
4. Swap only `B`'s full active memtable into `B`'s immutable queue.
5. Create a new lazy/chunked active memtable for `B`.
6. Retry the already-durable batch using its captured actual WAL segment; if it was in the old WAL,
   the new memtable correctly lowers its earliest dependency to that old segment.
7. Wake the shared maintenance scheduler with `B` as the flush candidate.

Other branches are not frozen merely because `B` filled. They continue using their current
memtables, which may span the WAL boundary and keep the old segment live.

### WAL-pressure policy

A database-wide policy bounds retained WAL bytes/segments. When exceeded, it selects the dirty
branch with the oldest WAL dependency, seals it if necessary, and trickle-flushes that branch. It
does not launch a mass flush of every branch. Global mutable-memory pressure separately selects a
largest/oldest victim through one write-buffer manager.

## Concurrency and lock rules

The registry lookup returns an `Arc<BranchRuntime>` and releases the registry/catalog lock before
touching mutable components. No branch component lock is held across object/file I/O.

The intended order for short in-memory transitions is:

```text
catalog/generation validation
  -> branch active slot (rotation path only)
  -> WAL append/rotation state
  -> WAL dependency tracker
  -> durable root/manifest transition
  -> branch immutable queue
```

This is not a requirement to hold the whole chain at once. Normal commit append releases the WAL
lock before memtable apply; its tracker token bridges that interval. Prepare/build I/O and publish
transitions are split so component locks are not held across file/object I/O. Any path that holds
two locks in a different order needs a lock-order test or a written proof. Existing `std::sync`
guards must not be carried across async suspension.

Snapshot capture takes immutable `Arc` views of one branch's complete source set. It never scans a
global level and filters after the fact. Inherited views are already immutable sources and are not
inserted into the child's owned levels.

## Rejected alternatives

### Map only the active memtable

Rejected. It leaves global L1 non-overlap, compaction, manifest, snapshot, and checkpoint
assumptions broken and encourages owner filtering after incorrect selection.

### Freeze every branch on every WAL rotation

Correct in a small prototype but rejected as the steady-state design. Hundreds of dirty branches
would turn one hot branch's rotation into hundreds of tiny immutables/SSTs, amplify manifest work,
and violate the branch-count complexity budget.

### One shared memtable with owner-prefixed keys

Technically coherent and similar to Strata's physical namespace, but rejected by the binding
decision: it changes comparator/index/bloom semantics, requires inherited-key rewriting, and makes
every row pay for branch identity.

### One WAL or database instance per branch

Rejected. It multiplies fsync and background machinery, weakens atomic global version/root
publication, and makes cross-branch operations coordinate independent durability domains.

### Let active memtables span WALs without tracking provenance

Rejected as data-loss-prone. A construction-time WAL number cannot protect a delayed apply that
lands after rotation, and a flushed sibling cannot prove a shared WAL segment reclaimable.

## Implementation order and gates

No step changes more than one architectural dimension without its owner regression and a vertical
recovery/read test.

### BR0 — Characterize the current single-branch boundary

- Add concurrency tests for append/rotate/apply ordering, checkpoint/close rotation, and replay
  floor publication.
- Record lock ordering and all direct `active_memtable` consumers.
- Gate: retained suite stays green; no production structure changes.

### BR1 — Actual WAL provenance and unresolved dependency pins

- Extend the retained `PreparedWrite`/commit environment so apply receives the actual appended WAL
  segment.
- Add the database-wide typed dependency tracker for in-flight commits and LSM components.
- Make replay-floor calculation consume a single dependency snapshot.
- Tests: append-old/rotate/apply-new sabotage twin; apply failure; crash after manifest attempt;
  cleanup cannot remove the only durable copy.
- Gate: still one default branch and the existing active memtable field.

### BR2 — Extract `BranchRuntime` for the default branch

- Move—not duplicate—the existing active memtable, immutable queue, and owned `Levels` access
  behind one `BranchRuntime`.
- Keep `Core`, `CommitPipeline`, `MemTable`, `Table`, `Levels`, compactor, task manager, and public
  `Tree`/`Transaction` path.
- Compatibility accessors may temporarily delegate to the default runtime but must shrink in every
  subsequent slice.
- Tests: all old point/range/history/snapshot/flush/compaction/checkpoint/recovery tests run through
  the extracted default runtime.

### BR3 — Partition durable levels by owner

- Change the manifest model so L0..Ln and branch logical flush facts are selected by `BatchOwner`,
  while retaining one database-wide physical WAL replay floor.
- Keep table IDs globally unique.
- Prohibit a table from appearing in two owned component sets; sharing occurs only through typed
  inherited references.
- Tests: same key ranges in two owners are legal; mixed-owner compaction is impossible by
  construction; corrupt owner/reference mismatches fail closed on open.

### BR4 — Lazy runtime registry and routed writes

- Add a registry entry without allocating an arena.
- Allocate the active memtable on the first write using chunked/lazy backing; the current
  `vec![0; capacity]` arena must not be multiplied by branch count.
- Route apply by the already-validated `BatchOwner` and use independent branch rotation.
- Add one database-wide mutable-memory budget and branch-scoped stall counts.
- Tests: 1,000 idle branches consume catalog-scale memory; same keys in two branches remain
  isolated through concurrent writes and rotations.

### BR5 — Branch-selected snapshots and reads

- Add owner/generation to `Snapshot` and transaction snapshot creation.
- Capture only that runtime's active, immutables, owned levels, and explicit inherited views.
- Point/range/reverse/history tests cover identical keys and tombstones across owners.
- Gate: no read path performs a global-table scan followed by owner filtering.

### BR6 — Demultiplexed recovery

- Replay every retained WAL record through catalog generation fencing.
- Rebuild lazy branch runtimes and group recovered memtables by owner and actual WAL dependency.
- Accept byte-identical replay duplicates and reject divergent ownership/bytes.
- Tests: multiple owners in one segment, one owner across segments, delete/recreate fencing,
  partial branch flush, crash at each root/replay-floor boundary.

### BR7 — Maintenance pressure and shutdown/checkpoint parity

- Select branch-specific flush/compaction candidates while retaining one scheduler.
- Add oldest-WAL-dependency trickle flush and global-memory victim selection.
- Make close/checkpoint capture all dirty branch runtimes without freeze-all in ordinary rotation.
- Tests: hundreds of dirty/cold branches, bounded WAL retention, no flush storm, checkpoint/reopen
  equivalence, and no acknowledged commit dependent on a deleted segment.

Only after BR0-BR7 pass does fork/inherited-view publication build on this runtime. Public branch
handles remain unavailable until catalog persistence, generation fencing, branch reads, and
recovery all use the same integrated path.

## Required invariants

1. A memtable or SST has exactly one physical `BatchOwner`.
2. A user/internal key never contains branch bytes.
3. L1+ non-overlap is evaluated inside one owner only.
4. The actual append segment, not an observed later segment, pins WAL durability.
5. A durable-but-not-applied batch pins its WAL before append can become externally durable.
6. In-flight-to-component WAL dependency handoff is atomic in one tracker.
7. WAL cleanup uses the same tracker snapshot committed as the physical replay floor.
8. An idle branch allocates no arena, WAL, or scheduler.
9. Branch deletion/recreation cannot route stale-generation replay into a new runtime.
10. Snapshots pin a complete branch source view; compaction cannot make inherited history appear.
11. No parallel commit/read/table engine is introduced during extraction.
