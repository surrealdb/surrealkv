# Branch-Native Integration and Reuse Audit

Status: binding correction to `BRANCHING_CONSOLIDATED_PLAN.md`

## Why this audit exists

The first P3 cutover incorrectly treated permission to break durable and public compatibility as
permission to replace the existing SurrealKV engine wholesale. It removed 61,000 lines of tracked
production and test code after a 6,400-line prototype passed 50 tests. That was not sufficient
evidence of equivalence.

All removed tracked files have been restored. The crate now builds the existing runtime and the
branch-native modules together, and the complete retained suite is registered. No existing module
or test may now be removed merely because a replacement prototype exists.

## Decision rule

Every original component receives one of four dispositions:

1. **Reuse**: retain the implementation and tests.
2. **Adapt**: retain the algorithms/tests while changing ownership, format, or host boundaries.
3. **Supersede after proof**: keep reachable until an integrated replacement passes the original
   owner tests plus the new vertical parity/crash tests.
4. **Remove**: only for an explicitly rejected feature, after its callers and feature-specific
   tests are identified. VLog and B+tree are the current examples.

A clean API/format break permits changing formats. It does not waive reuse analysis or regression
coverage.

## Existing component ledger

| Existing component | Disposition | Integration work |
| --- | --- | --- |
| `batch` | Adapt | Keep owned inline values, allocation accounting, in-place sequence stamping, and tests. Add branch/generation commit identity only with the branch-aware WAL recovery slice. |
| `commit` | Adapt, preferred coordinator | Keep queue ordering, concurrency bounds, apply/publish sequencing, failure cleanup, and tests. Generalize the durability operation behind `CommitEnv`; do not maintain the prototype mutex commit path as a second final coordinator. |
| `oracle` | Reuse/adapt | Keep conflict-index and GC logic. Scope conflict facts by branch identity without prefixing stored user keys. |
| `stall` | Reuse/adapt | Keep controller semantics and tests; feed branch-local and database-wide budgets. |
| `clock` | Reuse/adapt | Keep injectable clock machinery; commit publication assigns the one global ordered version/timestamp pair. |
| `memtable` | Reuse/adapt | Keep arena/skiplist and atomic batch preflight. Instantiate lazily per dirty branch and retain the multiset/snapshot regressions. |
| `comparator` | Reuse/adapt | Keep comparator abstractions and bytewise behavior. Update only the internal suffix codec needed by the global commit version. |
| `iter` | Reuse/adapt | Keep merge/concat/version iterator algorithms and their large regression suite. Add capped inherited component sources rather than a second iterator stack. |
| `sstable/block`, `index_block`, `filter_block`, `bloom`, `table` | Reuse/adapt | Keep restart compression, filters, indexes, block validation, iteration, and tests. Add table owner/generation metadata and an immutable ranged-object reader. The prototype table format is evidence, not the final duplicate implementation. |
| `cache` | Reuse/adapt | Keep the weighted cache and statistics; change the cache identity to immutable table ID/source identity and preserve corruption eviction/refetch tests. |
| `compression` | Reuse | Keep selectors and codecs; add only format-versioned codec metadata required by object tables. |
| `levels`, `compaction` | Reuse/adapt | Keep selection and merge machinery. Operate on branch-pure component sets and fix snapshot-safe tombstone retention. |
| `snapshot`, `tracker` | Reuse/adapt | Keep pinning, range/history behavior, and tests. Add branch generation and inherited-view caps. |
| `wal` | Reuse for Local; adapt | Keep framing, fragmentation, checksums, rotation, repair policy, and crash tests. Add branch/generation to commit records atomically with branch-aware replay. Remote/object-only authority may use a different binding behind the same semantic commit contract. |
| `checkpoint` | Adapt | Reuse pin/snapshot and restore tests; express checkpoints as immutable root/catalog pins. |
| `vfs`, `lockfile` | Reuse for Local | Keep native positioned IO, fsync, locks, and tests inside the Local binding. They are not the universal object-store interface. |
| `task` | Reuse/adapt | Keep lifecycle coordination and cancellation tests; make branch maintenance lazy and backend-capability aware. |
| `lsm` | Adapt as integration host | Evolve the existing engine into database/branch ownership instead of bypassing it. Remove old public semantics only at the final API cutover. |
| `transaction` | Reuse internally, supersede public API later | Keep batching, savepoints, conflict facts, range/history machinery, and tests. Add branch binding and expected-head semantics; remove compatibility surface only after the new API uses this path. |
| VLog and value-pointer resolution | Remove (already absent) | Inline values are the sole representation. Keep absence tests. |
| B+tree | Remove/keep absent | It is not part of the branch-native design. Keep absence tests. |

## Prototype component ledger

| Prototype component | Disposition |
| --- | --- |
| `api`, branch identifiers/selectors, reference model | Integrate into the existing public/internal types. |
| `storage` contracts, Memory, Local object adapter, SimStorage, Local authority prototype | Keep as adapter/conformance work. Reuse existing Local VFS/WAL capabilities rather than duplicating them. |
| `format` row validation and fail-closed codecs | Port the validation rules into existing internal-key/SST/WAL codecs. |
| prototype `table` | Mine owner metadata, ranged-read, digest, and corruption tests; merge those concerns into existing SST blocks/index/filter/cache. Do not ship two table engines. |
| prototype `database` | Mine root/catalog, expected-head, timeline, fencing, and crash tests. Route final commits through the adapted existing commit pipeline; do not ship two commit coordinators. |

## Test retention rule

The restored suite is the default. A test can be removed only when it asserts an intentionally
removed public or durable-format contract, and the change records:

- the exact removed contract;
- why translating the test would be false or meaningless;
- the replacement owner regression and vertical test, when behavior still exists.

Tests of blocks, filters, iterators, compaction, recovery, concurrency, snapshots, batching, and
failure cleanup are behavioral assets and must be ported even if their fixtures or API calls change.

## Corrected integration order

1. Keep the existing runtime and all retained tests green.
2. Integrate branch identity into the existing batch/commit/recovery path as one vertical slice.
3. Add branch-local memtable/level ownership while preserving existing iterator and transaction
   behavior for the default branch.
4. Merge owner metadata and ranged-object IO into the existing SST implementation.
5. Add the durable branch catalog and exact fork views above those retained components.
6. Cut the public API only after both old regressions and new branch/backend parity suites pass on
   the same runtime.
7. Only then remove superseded compatibility code and only its contract-specific tests.

