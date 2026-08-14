# Removed surfaces

What this rewrite deleted, why, and what guards the absence. Tests here prove **absence**; they
do not preserve old behaviour. If you are about to reintroduce something on this list, the burden
is on you to say what changed.

---

## bplustree

Removed before this line of work began (commit `2c9e8f3`). The engine is LSM-only.

## VLog / key–value separation

`src/vlog.rs` plus pointer decoding, VLog SST metadata, its GC, checkpoint and restore paths,
builder options, and VLog-only tests.

**Why:** neither strata-core nor SlateDB has a value log; values are inline in SSTs in both.
Branching makes VLog actively harmful — a second cross-branch lifetime domain, old branches
pinning log files, non-hardlinkable checkpoints, and another refcount/GC family. Versioned
queries store values inline and no longer depend on VLog configuration.

**Accepted tradeoff:** higher compaction write amplification for large values. If measured
workloads later require separation, it returns as immutable manifest-referenced blob objects
through a separate reviewed design — not by restoring this code.

## Full-rewrite manifest engine

`write_manifest_to_disk`, `load_from_file`, `replace_file_content`, `SnapshotInfo`,
`MANIFEST_FORMAT_VERSION`, and the whole-file rename publish path.

**Why:** replaced in FK1 by numbered immutable authority lineages (catalog / branch state / root)
with conditional-create publication. POSIX `rename` silently overwrites, so it can never be a
publish primitive for a version file; `fs::hard_link` is no-clobber and therefore IS
conditional-create.

**Guard:** `src/test/architecture_guard_tests.rs` — no whole-file manifest rewrite path returns.

## Refcount registry, release journal, two-cycle scan GC (planned, never built)

The master plan's D5 specified an `SstRefRegistry`, a deferred-deletion journal, a deletion
delay, and abolishing `cleanup_orphaned_sst_files`.

**Why none of it exists:** FK3 made inherited views *logical*. A child holds no physical table
references, so every live table is referenced by exactly one owner's state manifest.
`LevelManifestIterator` walks every table of every owner, so `cleanup_orphaned_sst_files` already
computes exactly the reachability set. Cross-branch file sharing never happens, and unlinking a
file whose `Table` still holds its descriptor is safe on POSIX — which is already how
default-branch compaction behaves.

**The invariant is unchanged** ("no deletion without proof"); the proof is the manifest, not a
registry. PB1 adds *runtime* reclamation for tombstoned branches, which is the one real gap.

**Guard:** an absence test asserting no refcount-registry or release-journal module reappears.

## The prototype engine (deleted in PA2)

A parallel, BTreeMap-backed implementation that proved the three-role storage seam (P0/P0B) and
then shared no data path with `Tree`.

| Deleted | Lines | Tests |
|---|---|---|
| `src/database.rs` — `KernelDatabase` | 990 | 3 |
| `src/table.rs` — a second table format + ranged reader | 1,569 | 7 |
| `src/format.rs` — a second codec stack | 312 | 4 |
| `src/testkit.rs` — reference fixtures | 778 | 18 |
| `src/test/rewrite_public_tests.rs` | 72 | 2 |
| `src/branch_native.rs` — re-export shim "during integration" | 15 | 0 |
| `src/lifecycle.rs` — a two-line placeholder | 2 | 0 |
| `BranchModel`, `ReadSelector`, `WriteOperation`, and 3 model tests in `src/branch.rs` | ~320 | 3 |
| `BranchRecord.head` + `BranchCatalog::advance_head` + its test | — | 1 |

**Kept:** `src/storage/*` — the three roles (`ObjectStore`, `CommitStore`, `Platform`) and their
memory / local / sim / native implementations. That is the adapter seam and the whole point of
the prototype.

**Why deleted:** the tree held **two `table.rs` and two `format.rs`**, one pair live and one dead,
so "edit the format file" was a coin flip. `KernelDatabase` could never become the product — it
is a BTreeMap, not an LSM — and a second set of semantics for branches, time travel and
durability is a standing invitation for the two to disagree.

**Why `head` went with it:** `advance_head` had CAS semantics and three callers, all of them
tests of the deleted model. Production never advanced it, so it was permanently
`CommitVersion(0)`. Wiring it would have meant a catalog publish per commit; `BranchInfo.head_seq`
is derived at read time instead (from the owner's state `last_sequence` and its live memtables).

**Guard:** `src/test/architecture_guard_tests.rs` — no `crate::table` / `crate::format` module,
no `KernelDatabase` / `BranchModel` symbol, with a non-vacuity assertion on the detector.

### Coverage carried forward, not lost

Eleven of `testkit.rs`'s eighteen tests touched only the storage roles and moved unchanged to
`src/storage/conformance_tests.rs`. The remaining seven are accounted for here. **Four are
obligations of PF1** (the slice that ports `Tree`'s authority and WAL IO onto the roles) and must
be re-expressed there against `Tree`, not against a prototype:

1. **Database identity is published before `open` returns.** Reopening the same authority yields
   the same identity, and a fresh authority at version zero has an empty head.
   (was `empty_database_identity_is_published_before_open_returns`)
2. **An unknown commit outcome reconciles and reopens after a crash.** With a fault injected
   after the authority write, the commit reports `Unknown`, `reconcile` confirms it, and a reopen
   sees the committed data.
   (was `single_branch_sim_commit_unknown_reconciles_and_reopens_after_crash`)
3. **A failed root publish leaks an object but recovers the complete old state.** With a fault
   injected before the authority write, the flush fails typed `Unavailable`, the uploaded object
   is left unreferenced, and a reopen reads the complete pre-flush state.
   (was `sim_flush_root_failure_leaks_object_but_recovers_complete_old_state`)
4. **A local spine commits and reopens through the durable authority.** Same identity, same head,
   same data across a close/reopen on the local object + commit stores.
   (was `single_branch_local_spine_commits_and_reopens_through_durable_authority`)

**One is an obligation of PF2** (object-backed table reads):

5. **The same immutable table's bytes are readable by range, identically, from every backend.**
   Cannot be expressed today: `src/sstable` reads through `vfs::File`, not `ObjectStore`, and the
   table format the original test built is the one being deleted here. `assert_object_contract`
   already covers ranged reads at the object level; this is the table-level claim on top of it.
   (was `memory_sim_and_local_read_identical_immutable_table_bytes_by_range`)

**Two are deliberately not carried forward** — they compare the engine against the reference
model, so they die with the model:
`single_branch_memory_spine_matches_logical_model_after_each_operation`,
`reference_model_executes_branch_and_temporal_script`.

## The three-role storage seam (deleted in V1)

`src/storage/` — `ObjectStore` / `CommitStore` / `Platform` plus the memory, local, sim and native
implementations. 7 files, 2,591 lines, 21 tests.

**Why:** it had **zero engine callers**. Its only references were `mod storage;` in `src/lib.rs`
and two architecture-guard tests that read the files as *text* — guards that pinned dead code in
place rather than protecting anything. The module carried `#![allow(dead_code)]` at its root plus
three `#[allow(unused_imports)]` markers whose comments promised a "P3 runtime cutover" that never
happened.

**And it could not simply have been wired up.** `ObjectStore` was `async fn` throughout, while all
seventeen engine call sites that would use it hold a `std::sync` guard across the would-be `await`
(nine `catalog_publish.lock()`, eight `level_manifest.write()`). A `std` guard may not be held
across an await, so the seam was incompatible **by calling convention**, not merely unused. See the
PF1 gate record in `P3_INTEGRATION_PROGRESS.md`.

**Recovery:** commit `5d8d769`.

**What replaces it:** nothing, in `v2`. The branch that takes on async IO and object storage
designs its seam against the engine's real call sites. `ASYNC_OBJECT_STORE_HANDOVER.md` — written
in slice V0, deliberately *before* this deletion — records the decision, what in the deleted code
was worth keeping as an idea (`PutOutcome`'s idempotency semantics, `Bindings::validate`'s
refuse-don't-degrade posture, the `FaultPoint` before/after taxonomy), what was not, the measured
scope of the port, and the invariants it must not break.

**Guard:** `src/test/architecture_guard_tests.rs` —
`removed_storage_role_seam_remains_absent` asserts the directory and the `mod storage;` declaration
stay gone, with a non-vacuity check that it parsed the real crate root.

### Taken with it

`src/api.rs` lost `MonotonicTime`, `DurabilityClass`, `OperationId`, `SessionId` and
`AuthorityFence` — vocabulary that existed only for this seam. `AuthorityFence` was additionally
re-exported from `src/lib.rs` despite nothing in the crate using it.

## Orphans removed in the same slice (V1)

Each had zero callers. Listed because "it might be useful later" is how the 2,591 lines above
accumulated.

| Removed | Where | Note |
|---|---|---|
| `CoreInner::new` | `src/lsm.rs` | A pass-through wrapper over `new_impl`, which is what production and now tests call. |
| `WriteStall::{should_stall, provider_counts, memtable_limit}` | `src/stall.rs` | Built for the per-branch stall exemption that was deferred and never returned. |
| `PartitionedIndexIterator::key` | `src/sstable/index_block.rs` | |
| `Reporter::old_log_record` + its impl + `report_old_log_record` | `src/wal/{reader,recovery}.rs` | The whole path was unreachable: nothing ever called the reporter method. |
| `DefaultReporter`'s three counters | `src/wal/recovery.rs` | Nothing read them; recovery decides from its `Result`. The struct is now a logger. |
| `Writer.compressed_buffer` | `src/wal/writer.rs` | WAL compression is unfinished. **Finishing it is a feature, not a dead-code sweep**, so the unread buffer went and the half-written encode stayed — the compression-type byte on the wire comes from `compression_type` and the format is unchanged. |
| `SnapshotIterator.core` | `src/snapshot.rs` | |
| `collect_history_all` / `KeyVersionsMap` / `point_in_time_from_history` markers | `src/test/mod.rs` | **Kept, markers removed** — all three are heavily used; the attributes were stale and had been hiding that. |

**Guard:** `no_module_suppresses_dead_code_warnings` fails if any `allow(dead_code)` attribute
returns to `src/`, with a written allowlist (currently one entry: `src/wal/mod.rs`, whose two
fields are read under `#[cfg(unix)]` and genuinely unread on Windows). The guard asserts its own
non-vacuity twice — that the walk reached the tree, and that the detector matches its own target.

## Destructive restore without a catalog

Restore refuses a checkpoint that lacks the authority lineages. Local restore is a whole-database
swap including the catalog, so a sequence rewind stays internally consistent; object mode never
rewinds the global sequence.

## fs2 lockfile as the sole writer exclusion

The file lock remains an optional local belt. **Fencing is the writer exclusion** — writer epochs
in the catalog, validated on every publish — because the wasm lockfile path was a silent no-op,
which is a live corruption door on any backend without advisory locks.
