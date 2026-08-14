# P3 Existing-Engine Integration Progress

Updated: 2026-08-14

This file records the corrected P3 implementation slices. The branch-native design is being built
through SurrealKV's retained commit, WAL, memtable, SST, manifest, iterator, compaction, and
transaction machinery. Prototype modules remain test-only evidence until their semantics have been
ported into that path.

## Completed integration slices

### Inline batch and WAL owner

- The batch format carries `BranchId` and `BranchGeneration` once per batch (format version 1
  of the rewrite line; renumbered from the interim v4 on 2026-08-14 — see the hardening record).
- User keys remain unchanged and values remain inline.
- Existing in-place sequence stamping remains valid because the fixed sequence offset is unchanged.
- Batch decoding now rejects every truncated owner/record boundary without panicking.
- Existing WAL recovery separates batches with different physical owners into branch-pure
  memtables, even within one WAL segment.

### Existing commit coordinator and oracle

- `CommitPipeline` remains the one production coordinator.
- Conflict fingerprints are scoped by branch/generation plus user key.
- Same-key writes conflict within one owner and remain independent across owners.
- Failure rollback uses the same owner-scoped fingerprint.

### Existing memtable and SST

- Existing arena/skiplist memtables have one physical owner and reject mixed-owner batches before
  reservation or allocation.
- Existing SST blocks, restart compression, indexes, bloom filters, cache, and iterators are
  retained.
- The SST table format persists owner metadata; the stored internal/user key is unchanged. (SST
  identity is the table magic footer — this rewrite line carries no lineage version numbers.)
- Existing compaction propagates the common owner and rejects mixed-owner input before output.

### Catalog and transaction fencing

- The existing `CoreInner` owns the integration catalog for the default branch.
- Every transaction carries a branch/generation owner and builds the ordinary `Batch` with it.
- The catalog is revalidated immediately before commit. A deleted or stale generation returns
  `BranchFenced` before sequence allocation or WAL append.
- Branch lifecycle methods remain test-only until catalog persistence, read routing, flush,
  recovery, and compaction are all wired. No partial public branch API is exposed.

### Replay-floor correction

- Manifest replay floors are computed from all remaining active and immutable WAL dependencies.
- Flushing one of several memtables derived from one WAL cannot advance or delete that segment.
- Clean shutdown advances past the closed segment only when it actually flushed data.
- Empty reopen/close cycles do not drift the WAL number.

## Verification

- 964 library tests passed; 1 ignored.
- 4 documentation tests passed; 5 ignored.
- strict all-target/all-feature Clippy passed.
- changed-file whitespace audit passed.

New owner and vertical regressions cover batch truncation, unprefixed keys, memtable isolation,
owner-scoped conflicts, WAL owner splitting, SST owner persistence, mixed-owner compaction,
transaction deletion fencing, split-segment partial flush, clean shutdown, and empty shutdown.

## Branch-runtime slices (P4_BRANCH_RUNTIME_DESIGN.md)

### BR1 — WAL provenance and dependency pins: landed

- `src/wal/dependency.rs` holds the database-wide typed dependency tracker; apply receives the
  actual appended WAL segment through the extended `PreparedWrite` path, and replay-floor
  calculation consumes a single dependency snapshot (`src/lsm.rs`, `src/memtable/mod.rs`).

### BR2 — Extract `BranchRuntime` for the default branch: complete (2026-08-13)

As built:

- `src/branch_runtime.rs` owns the complete default-branch component set (active memtable,
  immutable queue, level manifest). The fields were moved, not duplicated: `CoreInner` holds
  `default_runtime: BranchRuntime` and temporarily dereferences to it, so every retained call
  site (snapshot, flush, compaction, checkpoint, recovery) runs through the extracted runtime.
- Owner purity is validated fail-closed at `CoreInner` construction
  (`BranchRuntime::validate_component_owners`).

Exit evidence (two-level test gate):

- Owner-module regressions with sabotage twins: a planted foreign-owner immutable memtable
  reddens `validate_component_owners` (with a pre-sabotage non-vacuity assertion), and the
  constructor rejects a foreign-owner active memtable
  (`src/branch_runtime.rs::tests`).
- Enforced-absence guard: `core_inner_owns_lsm_components_only_through_branch_runtime`
  (`src/test/architecture_guard_tests.rs`) fails if `CoreInner` regrows alias component fields or
  the extraction is reverted; it parses the real struct block and asserts its own non-vacuity.
- Vertical: the full retained suite (point/range/history/snapshot/flush/compaction/checkpoint/
  recovery) runs through the extracted runtime — 972 library tests passed, 1 ignored; strict
  all-target/all-feature Clippy clean.

### BR3 — Partition durable levels by owner: complete (2026-08-14)

As built:

- `LevelManifest` now holds per-owner level sets (`levels_by_owner`); reads select an owner via
  `levels_for(owner)` — the owner-blind `levels` field no longer exists, so a read path cannot
  scan globally and filter afterwards (the adopted gate amendment). `default_owner_levels()` is
  the explicit single-branch convenience; its mutable twin is test-only, and production mutation
  goes through `apply_changeset` exclusively.
- Manifest format V2 encodes `(BranchId, BranchGeneration)` per level set; V1 is rejected by
  identity. Table IDs remain one global counter; the id-keyed hidden set is unchanged.
- Fail-closed validation: `apply_changeset` rejects, before any mutation, a table whose persisted
  `meta.owner` differs from the changeset owner; open rejects an owner/reference mismatch, a table
  listed more than once across owned sets, and a missing default set. Rollback is owner-scoped.
- `ManifestChangeSet` carries its owner; flush stamps the flushed memtable's owner and compaction
  stamps `CompactionOptions.owner`. `CompactionStrategy::pick_levels(manifest, owner)` only ever
  sees one owner's level set, so a mixed-owner compaction is unrepresentable. The database-wide
  physical WAL replay floor (`log_number` + BR1 dependency tracker) is retained unchanged.
- Deferred to BR4 (owner of routed writes): per-owner logical flush facts beyond table routing —
  today only the default owner writes, and `last_sequence` stays a database-wide fact.

Exit evidence (two-level test gate):

- Owner-module regressions with sabotage + non-vacuity (`src/test/manifest_tests.rs::br3_*`):
  same key ranges under two owners load legally; a mixed-owner changeset fails closed with the
  manifest unmutated (after proving the same table applies under its own owner); an on-disk
  owner/reference mismatch and a double listing each fail closed on open; a V1 manifest is
  rejected by identity.
- Vertical: the full retained suite (flush/compaction/checkpoint/recovery/transactions) runs
  through the partitioned manifest — 977 library tests passed, 1 ignored; strict all-target
  all-feature Clippy clean; `cargo fmt --check` and changed-file whitespace audit clean.

### BR4 — Lazy runtime registry and routed writes: complete (2026-08-14)

As built:

- `BranchRuntimeRegistry` (`src/branch_runtime.rs`): `RwLock<HashMap<BatchOwner,
  Arc<BranchRuntime>>>` seeded with the default runtime. A lookup never allocates;
  `get_or_create` (double-checked locking) builds a runtime — and with it the first arena, sized
  `branch_memtable_size` (new option, default 2 MiB) — only on the owner's first routed write.
  `CoreInner` holds `default_runtime + runtimes`; the BR2 `Deref` remains for default-path code.
- `LsmCommitEnv::apply` routes by `batch.owner` (already catalog-validated pre-commit). The
  `ArenaFull` retry rotates the owning runtime with `min_capacity = batch.memtable_size_estimate()`.
- Right-sizing verifies against the reservation formula, not raw capacity: new
  `MemTable::arena_available()` (capacity − skiplist usage − outstanding reservations) exposes the
  real headroom; `rotate_runtime_memtable` rebuilds the replacement if the sentinel-node overhead
  (empty skiplist head/tail towers, ~400 B) would leave it short. The gate test caught this: an
  arena sized exactly to the batch estimate can never admit that batch, which would have made a
  WAL-durable batch permanently unappliable (the precise hazard the amendment targets).
- Rotation decoupling (amendment 2): `rotate_runtime_memtable` never touches the shared WAL; the
  retiree keeps its BR1 dependency-derived `wal_number`, an empty retiree (pure right-sizing) is
  dropped instead of queued, and the WAL rotates on its own size policy at append
  (`Wal::should_rotate_for_size`, `bytes_in_active_segment` vs `max_file_size`).
- Write-buffer budget (`write_buffer_budget: Option<u64>`, `None` = today's behavior):
  `enforce_write_buffer_budget` rotates the largest non-empty active memtable and reports whether
  it rotated; `apply` then wakes the memtable flush task. The wake is load-bearing, found by
  deadlock audit: the flush task is purely event-driven (`Notify`), so an unwoken budget rotation
  reclaims no memory and can park the victim's next write in the stall loop waiting for a flush
  that was never scheduled. Every production rotation site now either wakes the flusher (budget,
  ArenaFull) or flushes synchronously (checkpoint, test helper).
- Stall accounting is owner-scoped (`get_stall_counts(owner)`): a branch stalls on its own
  immutable backlog and its own L0 (via `levels_for`); a never-written owner reports zero. The
  provider drops the immutables guard before taking the manifest lock (no hold-and-wait; verified
  against the flush path's manifest→immutables order).
- Flush visits every runtime (default first, one memtable per call); `flush_immutable_to_sst`
  resolves the owning runtime by the memtable's owner and `apply_changeset` creates the owner's
  level set on first flush (`ensure_owner_levels`). `validate_component_owners` treats an absent
  level set (owner never flushed) as vacuously pure — the second gate-test catch.

Deadlock audit (question raised mid-slice): no lock-order cycle exists — rotate takes
`active.write → wal.read → manifest.read → immutables.write`; flush and compaction take
`manifest.write → immutables.write` (same relative order); registry guards and budget accounting
are transient (Arc-clone-and-drop) and `get_or_create` acquires nothing else while holding the
registry write lock. The one real hang was the missing flush wake above, now fixed and regression-
covered by the budget gate test.

Exit evidence (two-level test gate):

- Owner-module regressions (`src/test/branch_runtime_tests.rs`): 1,000 idle branches create no
  runtime and no arena beyond the default, with the non-vacuity arm proving the first routed write
  creates exactly one runtime of exactly `branch_memtable_size` bytes; same-key writes in two
  branches stay isolated through interleaved independent rotations (default reads resolve only
  default components; owner-purity validated); the oversized-batch sabotage twin (16 KiB batch
  into a 4 KiB arena) commits via right-sized rotation; a budget breach rotates the largest victim
  with flush-race-proof evidence (active emptied AND the bulk row survives in queue or owned L0 —
  data preservation, not just queue depth).
- WAL policy unit (`src/test/wal_tests.rs`): `should_rotate_for_size` false on a fresh segment,
  trips at the cap, resets on rotation, and the counter genuinely restarts.
- Vertical: 982 library tests passed, 1 ignored; strict all-target all-feature Clippy clean;
  `cargo fmt --check` clean. Ten pre-existing fixtures that asserted the old rotate-rotates-WAL
  contract were migrated to explicit WAL rotation; the BR1 sabotage test's delayed-apply window was
  recreated (WAL rotate placed before memtable rotation) to preserve its assertion strength.

### BR5 — Branch-selected snapshots and reads: complete (2026-08-14)

As built:

- `Snapshot` now carries `(owner, runtime: Option<Arc<BranchRuntime>>)` resolved once at creation
  (`Snapshot::new_owned`; the ownerless constructor is deleted). `Transaction::new_owned` passes
  its validated owner through, so read-mode transactions on a branch read that branch. Both
  capture points — `Snapshot::get` and `collect_iter_state` (which range/reverse/history/`get_at`/
  count all build on) — route memtable phases through the captured runtime and durable tables
  through `levels_for(owner)`.
- Idle-branch reads allocate nothing (amendment 1): capture uses `runtimes.get`, never
  `get_or_create`; an absent runtime is a memtable-less view and an absent owner level set is an
  empty `Levels` (missing DEFAULT set stays fail-closed corruption). `IterState.active` is now
  `Option<Arc<MemTable>>` (amendment 2; one consumer). The absent-runtime view is complete by the
  FIFO-publish argument recorded in the gate.
- **Pre-existing snapshot-protection bug fixed (plan finding C9, surfaced by this slice's
  compile pressure):** `SnapshotIterator::new_from` constructed a stand-in `Snapshot` literal that
  was never registered with the tracker, yet its drop unregistered the caller's sequence — every
  range scan silently stripped its own snapshot's compaction protection. And the tracker was a
  `SkipSet<u64>` set, so any two same-sequence snapshots (any two read transactions with no commit
  in between) collided the same way. Fixes: `new_from` now takes the real snapshot (no stand-in
  exists to mis-drop), and `SnapshotTracker` stores `(seq, unique_id)` entries handed out as RAII
  `SnapshotGuard`s — the proven `ActiveTxnTracker` pattern — making an unregister without a
  matching register unrepresentable. `get_all_snapshots` dedups; `first()` semantics unchanged.
- Per-branch snapshot trackers remain deferred to BR7 (global tracker conservatively pins all
  owners' compaction — correct, performance-only). Inherited views stay structurally empty until
  fork (amendment 3).

Exit evidence (two-level test gate):

- Owner-module regressions (`src/test/branch_runtime_tests.rs::br5_*`): point reads and
  tombstones stay owner-scoped (a foreign delete never shadows the default value, default-only
  keys never leak into foreign reads); forward and reverse scans return exactly the owner's
  keys/values including a shared key with per-branch values; versioned reads (`get_at`,
  `history`) resolve only the owner's version chain with interleaved cross-branch timestamps;
  an idle-branch read sees an empty view while creating no runtime and no arena, with the
  non-vacuity arm proving the same reads observe data after the first write; after draining every
  flush queue, a shared key resolves from each owner's own durable L0 (owner-routed table
  selection, not memtable residue).
- Tracker regressions (`src/snapshot.rs::tests`): two same-sequence registrations do not collide
  — dropping one keeps the survivor's protection (the C9 fix's sabotage-adjacent arm), plus the
  pre-existing ordering/clone tests migrated to guards.
- Vertical: 988 library tests passed, 1 ignored; strict all-target all-feature Clippy clean;
  `cargo fmt --check` clean.

### BR6 — Demultiplexed recovery: complete (2026-08-14)

As built:

- `replay_wal` now demultiplexes per (WAL segment, live owner): one branch-pure memtable per
  owner per segment (no more split-per-alternation), each stamped with its segment as the WAL
  dependency. It takes the fencing authority as a parameter (`is_live_owner`); the open path
  passes a closure over the catalog-at-open (`validate_owner`), so deleted, stale-generation, and
  unknown owners' batches are dropped during replay and never installed (invariant 9). Fenced
  batches still advance the returned `max_seq_num`, so the recovered clock never reseeds below an
  allocated (possibly acknowledged) sequence.
- Strict monotone-sequence enforcement replaces the "byte-identical duplicates" clause for the
  local WAL: append order equals sequence order (both under the commit write mutex), so a batch
  whose `starting_seq` does not exceed its predecessor's highest is corruption and the open fails
  closed (through both recovery modes — repair cannot mask it, since both records are
  reader-valid).
- The BR4 right-sizing rule is extracted into `MemTable::new_owned_admitting` (one shared
  implementation) and used by forced rotation AND replay: recovery memtables start at the owner's
  configured arena size (default → `max_memtable_size`, foreign → `branch_memtable_size`) and an
  oversized batch right-sizes a fresh memtable instead of hitting the old fatal
  "batch too large for arena" replay path.
- Installation is one shared method, `CoreInner::install_recovered_memtables`, used by open and
  by checkpoint-restore (whose stale callback-based path was replaced; restore fences against the
  running store's catalog): per owner, all-but-last memtables register their WAL dependency and
  flush into the owner's own level set during open (`ensure_owner_levels` creates it on first
  flush), the last becomes the owner's runtime active; foreign runtimes are created at open via
  the registry (they hold data — not idle). Every runtime's active then gets the current-WAL
  baseline (empty → `set_wal_number`; recovered → `record_wal_dependency`, which keeps the min).
- The BR6 placeholder guard ("branch-owned WAL requires the branch catalog recovery path") is
  deleted: a store holding branch writes now reopens.
- Test seam for the injected authority: `TreeBuilder::with_initial_branch_catalog` (test-only
  field) threads through `Tree::new_with_catalog → Core::new_impl → CoreInner::new_impl`;
  production constructors pass `None` and open with the default-only catalog until the fork
  lifecycle slice lands durable catalogs.

Exit evidence (two-level test gate):

- Owner-module regressions (`src/test/branch_runtime_tests.rs::br6_*`): two owners interleaved in
  one segment reopen into isolated runtimes with correct per-branch reads; one owner across two
  segments flushes the intermediate into the owner's own L0 at open and serves both rows; a
  deleted/recreated name fences the stale generation (no runtime, no leak into generation 1,
  which reads only its own row); reopening with a default-only catalog succeeds, drops the
  unknown owner's batches, preserves default data, and holds the sequence clock exactly at the
  pre-crash maximum (the fenced batch was the maximum — the non-vacuity arm) with post-fence
  commits allocating above it; a partially-flushed branch replays only the WAL tail past the
  advanced replay floor and serves both the SST row and the tail row; and the monotone-sequence
  sabotage twin (regressed sequences fail the open closed; the same fixture with increasing
  sequences opens and reads fine).
- Vertical: 994 library tests passed, 1 ignored; strict all-target all-feature Clippy clean;
  `cargo fmt --check` clean. All ~20 pre-existing `replay_wal` fixtures migrated to the explicit
  five-argument form (accept-all fence, uniform arenas) — no behavioral change to what they
  assert.

## FK3 gate record (adversarial gate passed 2026-08-14)

Verified against the tree: `process_accumulated_versions` (`src/iter.rs`) computes
`latest_is_delete_at_bottom` BEFORE the snapshot checks — exactly the C1 hazard; the parent-side
`is_bottom_level = false` override neutralizes it and the pin floor covers the remaining drop
paths (superseded, wall-clock retention, REPLACE). `BranchRecord` carries no parent link yet
(FK1's `to_entries` writes `parent: None`); BR5's snapshot capture is single-runtime and both
read capture points funnel through `collect_iter_state`/`KMergeIterator`, so the layer extension
is contained. `MemTable::get` and the table point-get already take per-call seq caps; range/
history iterators need a `SeqCappedIterator` wrapper applied per layer BEFORE the merge (the
global snapshot-seq filter cannot express per-layer caps).

Amendments adopted: (1) anchors and chains match parent branch AND generation; catalog load
fail-closes when an Active child's parent is not Active with the matching generation
(invariant 3 as a load check, ahead of FK5's delete guard); (2) `history_pin_floor` =
min Active-child anchor implements BOTH the range pin and the wall-clock clamp as one rule —
"a version with `seq <= floor` is never dropped, by any path, REPLACE included"; (3)
`Error::MaterializationRequired` and the depth cap (64) land now (chains exist now; FK5 adds
budgets/metrics); (4) FK3 tests create parented catalog entries through a `#[cfg(test)]` seam
(`create_fork_entry_for_test`) that FK4's real fork protocol replaces and deletes —
transitional by declaration, with the catalog-level `create_fork` being the production-shaped
half FK4 keeps.

### FK5 — Lifecycle, TTLs, metadata GC, scale: complete (2026-08-14)

As built:

- **Deletion guard.** `BranchCatalog::delete` refuses a branch with Active fork children
  (`ErrorCode::Conflict`). The catalog loader already rejected that shape, so the guard closes a path
  that could publish an unopenable store.
- **TTL.** `set_expiry` / `CoreInner::set_branch_expiry` (one catalog version), and `expire_due` /
  `sweep_branch_maintenance`, which tombstones every branch whose `expires_at` has passed
  `opts.clock.now()`. Expiry does NOT cascade (gate amendment 3): an expired parent with Active
  children is skipped and expires on the first sweep after the last child goes.
- **Metadata GC.** `versions_in` + `prune_versions` in the publish layer, `prune_metadata` on the
  authority store, `KEEP_METADATA_VERSIONS = 4` per lineage. Keep-last-K only — no min-age, no
  reader fence — because recovery reads only the newest version, `resolve_from_hint` probes forward,
  and a decoded reader already holds its bytes. Pruning runs after the sweep's own publish, so the
  version it just created can never be its own victim.
- **Maintenance placement.** `CompactionOperations::sweep_branch_maintenance` runs at the tail of the
  level-compaction wake, not on a timer (gate amendment 2, D10 seam 3). A failure there costs
  retention, not correctness, so it is logged and does not stall writes.
- **Reversal of gate amendment 1 on `catalog_version_floor`.** The gate called the field stale and
  removed it. Removing it broke a live fail-closed check at open —
  `latest catalog version is below the root's reclaim floor` — which had simply never been given an
  input. So the field came back with a real producer: `LevelManifest` carries an
  `Arc<AtomicU64>` mirror written after each successful catalog publish, and `persist_root` records
  it. The check now detects a catalog lineage truncated or rolled back underneath a root that
  already depended on it, and pruning provably cannot trip it, because pruning only removes versions
  older than the newest. Keeping a stale field would have been wrong; so would deleting a real
  invariant — wiring it satisfies both rules.

Verification: 1,053 lib tests green, clippy `--all-targets --all-features` clean, fmt clean. Seven
new tests: the deletion guard with its release arm, TTL expiry with a due/far-future/no-TTL trio
plus re-entrancy and write fencing, the no-cascade rule with the child's read still resolving
through its skipped parent, pruning bounded to the retained window followed by a reopen that finds
every branch, the scale gate (900 plain branches + 100 forks, pruned, reopened, 1,001 records, an
inherited view still resolving, one runtime), and the truncation sabotage with its control arm.

### PB2 — Detach (materialize): complete (2026-08-14)

The relief valve for the two costs a long-lived fork imposes: the ancestor chain every read walks,
and the retention pin its anchor places on the parent.

As built:

- **`Snapshot::inherited_only`** — the ancestor layers with their cumulative caps, minus the
  branch's own layer. Returns `None` for a branch that inherits nothing.
- **`CoreInner::materialize_inherited`** — copies that view, every version and every tombstone, in
  internal-key order, into one branch-owned table, then publishes the branch's state.
- **`CoreInner::detach_branch`** = materialize, then clear the parent link in the catalog.
  **`BranchCatalog::detach`** does the latter. Public entry point: `Tree::detach_branch(name)`,
  returning rows copied. A branch with no parent returns `Ok(0)` — detaching is idempotent.
- **Placement** (gate amendment 1): `deepest_occupied_level + 1`, refusing when the branch occupies
  the last level with a message telling the caller to compact first.

**Decomposed so the crash window is testable, not just argued.** Detach publishes state before
catalog (gate amendment 4). Splitting `materialize_inherited` out of `detach_branch` is not
cosmetic: it makes the exact crash state — materialized tables plus a still-live parent link —
constructible in a test, so `detach_interrupted_between_its_publishes_reads_correctly` asserts the
claim that duplication is harmless instead of leaving it as a comment. It also shows the
interrupted operation is simply retryable.

**A dishonest fixture of mine, caught by a probe.** The "branch occupies every level" test built
that state by adding a table to L1 while leaving it in L0 — a state the branch state manifest
rejects as corrupt ("table id listed more than once"). It passed only because the refusal fired
before anything was published; under the placement probe it surfaced as a corruption error rather
than the expected assertion failure. Rebuilt with two distinct tables and a `persist_owner_update`
in the fixture itself, so the setup proves the state is legal before the test relies on it.

Sensitivity probe: forcing placement to L0 reddens both placement tests — the materialized rows
land above the branch's own compacted row (`left: 0, right: 1`), and the every-level branch is
detached instead of refused. That is precisely plan C8's hazard, reproduced.

Verification: **1,041 → 1,047 lib tests** green, clippy `--all-targets --all-features` at zero
warnings, fmt clean.

## PB2 gate record (adversarial gate passed 2026-08-14, with four amendments)

Verified against the tree before implementing detach.

**Amendment 1: plan C8's placement hazard is real, and the precise rule follows from the read
path.** `get_in_layer` walks the owner's levels in order and returns the FIRST table containing the
key (`src/snapshot.rs:377-398`) — there is no cross-table sequence comparison. Within L0 that is
safe because `Level::insert` orders descending by largest seqno (`src/levels/level.rs:47`), so a
materialized table (every row at or below the fork anchor) would sort last within L0. But across
levels it is not: a child that compacted its own `k@200` down to L1, given materialized `k@50` in
L0, would read the stale inherited row and never reach L1. So the output must sit **below every
level the child occupies**. The rule: place at `deepest_occupied_level + 1`, and refuse when the
child already occupies the last level (it can compact first). A child that has never written
places at L0, where there is nothing to shadow.

**Amendment 2: detach reads an ancestors-only view, not the child's merged view.** Copying the
merged view would duplicate the child's own rows into the new tables at their original sequences.
Compaction's equal-sequence dedup would survive it, but it is wasteful and it muddies what the
materialized table means. A `Snapshot` variant that omits layer 0 gives exactly the inherited set,
with the same cumulative caps the read path already applies.

**Amendment 3: every version and every tombstone is copied, not just the newest.** The merge over
ancestor layers yields all versions in internal-key order, which is what makes the result
self-contained: a tombstone below the cap has to come across or the value it hides would
reappear. Internal-key order is also exactly what `TableWriter` requires, so no sorting step is
needed.

**Amendment 4: publish the state BEFORE the catalog.** Detach touches two authorities. If the
catalog's `parent` were cleared first and the state publish then failed, the child would lose its
inherited data outright. In the other order, a crash between them leaves a child that has both the
materialized tables and its parent link — every inherited row is simply present twice, at the same
sequences, with identical values. Harmless duplication is the correct failure mode, and it is
reclaimed by the child's next compaction.

### PB1 — Runtime reclamation of tombstoned branches: complete (2026-08-14)

Closes gap F2: a deleted branch's data was only freed at the next process start, so a
long-running store that churns sandbox branches leaked their tables for its whole lifetime.

As built:

- **`LevelManifest::reclaim_owner`** drops a tombstoned owner's level set, `state_versions` and
  `retained_floors`, and hands back its tables. **`BranchRuntimeRegistry::reclaim`** drops the
  runtime and hands back its memtables.
- **`CoreInner::reclaim_tombstoned_branches`**, run at the tail of `sweep_branch_maintenance`:
  drop the runtime first (so the flush loop can no longer pick the owner up), release every
  discarded memtable's WAL dependency, remove the level set and republish the root under the
  manifest write lock, then unlink the tables.
- **The blocker's fix**: a liveness check inside the flush's manifest critical section. It returns
  `Error::BranchFenced` and removes the just-written table. The pre-existing runtime lookup at the
  top of the flush is *not* sufficient — it runs before the lock — and `apply_changeset` would
  otherwise recreate the level set via `ensure_owner_levels` and publish a deleted branch's data.
- No refcount registry, no deletion journal, no grace period (gate amendment 2). The manifest is
  the proof, and `Arc<Table>` owns the descriptor so unlinking under an in-flight reader is safe —
  the same property compaction has always relied on.

**Two test defects found by probing, both fixed at the test rather than the assertion.** This is
the second slice running where the first version of a test passed against a deliberately broken
implementation:

1. `Tree::flush` is a test helper that rotates only the *default* runtime's active memtable (it
   flushes every runtime's immutables, but seals only main's). Two tests assumed it sealed branch
   memtables too, so they were asserting against branches that had never flushed anything. Fixed
   with a local `flush_branch` helper that rotates the branch's runtime explicitly — the same
   thing the trickle policy does in production, and the pattern the BR7 tests already use.
   Deliberately did *not* "fix" the shared helper: changing what `flush()` means would silently
   alter the meaning of every test that calls it.
2. The WAL-dependency test watched `replay_floor`, which in a single-segment fixture is 0 either
   way — so it passed with the release call removed. The discriminating observable is
   `WalDependencySnapshot::component_count`: a discarded memtable that keeps its dependency is a
   ghost no flush will ever release, because its runtime is gone. Rewritten to assert the count
   returns to its baseline.
3. The race test originally hit the runtime lookup, not the in-lock guard, and asserted the wrong
   error. Rebuilt around the real production interleaving — a branch tombstoned in the catalog
   while its runtime is still live, which is exactly the state between `delete_branch` and the
   next sweep — so it reaches the manifest lock and tests the guard that exists for it.

Sensitivity probes, all reddening on their intended assertion: disabling the liveness guard
publishes the deleted branch's table (`Ok(1024)`); dropping the dependency release orphans a
component (`left: 2, right: 1`); disabling the sweep leaves 2 tables instead of 1, and leaves 41
instead of 1 in the churn test — the leak PB1 exists to fix, measured.

Verification: **1,037 → 1,041 lib tests** green, clippy `--all-targets --all-features` at zero
warnings, fmt clean.

## PB1 gate record (adversarial gate passed 2026-08-14, with one blocker and three amendments)

Verified against the tree before implementing runtime reclamation.

**Confirmed the premise.** `LevelManifest::hydrate` iterates
`catalog.all_records().filter(|record| !record.deleted)` (`src/levels/mod.rs:185`), so a tombstoned
branch's level set is simply not loaded at open, its tables become unreferenced, and
`cleanup_orphaned_sst_files` reclaims them. Reclamation is therefore already correct — it just
only happens at process start, which is exactly gap F2.

**BLOCKER: `apply_changeset` silently resurrects a reclaimed owner.** It calls
`ensure_owner_levels(changeset.owner)` (`src/levels/mod.rs:529`), which *creates* the level set
when it is missing — the mechanism that lets a brand-new branch's first flush work. So a flush
that lands after the sweep removed a deleted branch would not fail; it would recreate that
branch's level set, add the flushed table, and publish it. The bug would be silent and durable.
`flush_immutable_to_sst` does require a live runtime (`src/lsm.rs:609`), but that check runs
*before* it takes the manifest lock at `:615`, leaving a real window. The guard therefore has to
live INSIDE the manifest critical section, where it is serialised against the sweep — the same
placement and the same shape as compaction's `CompactionPinRaced` re-validation and the fork's
retention-floor check. Reclamation without it is unsound and must not be written.

**Amendment 1: discarded memtables must release their WAL dependencies.** Dropping a deleted
branch's runtime discards memtables that hold `ComponentId` entries in `WalDependencyTracker`.
Without `release_component` for each, the reclaimed branch pins WAL segments forever — turning a
space fix into a strictly worse space leak. This is the kind of thing that would pass every test
and show up as unbounded disk growth in production.

**Amendment 2: no grace period.** The plan borrowed "deletion delay + second cycle" from D5,
where it guards against listing races on object storage. Here it protects nothing: an in-flight
reader holds an `Arc<Table>` that owns the file descriptor, so unlinking is safe on POSIX — which
is already how compaction deletes its inputs today. Adding a delay would only postpone reclamation
and add a timer the executor seam does not want. Skipped deliberately, recorded here so it is not
re-added as an oversight.

**Amendment 3: the sweep also drops the owner's `state_versions` and `retained_floors` entries
and republishes the root.** Otherwise the root keeps a `state_hint` pointing at a reclaimed
branch's state lineage. Nothing reads it (hydrate skips deleted records), so this is tidiness
rather than correctness — but leaving a dangling hint in a durable record is the kind of untruth
that later phases end up trusting.

### PA — Public branch API: complete (2026-08-14)

As built. Everything below is additive: `begin`, `view` and every existing method still operate on
`main`, which behaves exactly as it did before branching.

- **`Tree`**: `branch`, `create_branch`, `fork_branch`, `delete_branch`, `list_branches`,
  `set_branch_ttl`, `begin_on`. Each wraps existing `pub(crate)` machinery — no new engine logic.
- **`BranchHandle`** (`begin`, `begin_with_mode`, `info`, `name`, `id`, `generation`), pinned to
  the generation it was opened at. A hand-written `Debug` prints identity only: the handle holds
  the whole engine, which is neither printable nor useful in a diagnostic.
- **`BranchInfo`** + **`BranchLineage`** as public shapes (gate amendment 3 — `ParentLink` is
  `pub(crate)` and cannot be exported).
- **`ForkPoint`** made public; **`ForkReceipt` deliberately not** (gate amendment 2 — it names
  `BatchOwner`). `Tree::fork_branch` returns a `BranchHandle`, which is what plan D9 specified and
  is immediately usable.
- **TTL is a `Duration`**, converted to an absolute expiry against `Options.clock` inside the
  engine, so callers never handle the engine's clock unit.
- `BranchCatalog::list` lost its `#[cfg(test)]` and `MemTable::lsn` lost its `#[allow(unused)]`;
  both now have production callers.

**`last_write_seq`, and the probe that caught a weak test.** Gate amendment 1 replaced the
planned `head_seq` with "the newest sequence this branch wrote itself", because
`LevelManifest.last_sequence` is global — `persist_owner_update` copies the same value into every
owner's state. The first version of the public test passed *even with the derivation deliberately
polluted by that global field*: the fixture never flushed, so the global stayed at zero and the
two implementations were indistinguishable. Rather than accept a green probe, the coverage gap
was closed properly with `last_write_seq_comes_from_the_branch_s_own_components` in
`fork_view_tests.rs` (where forcing a flush is allowed), which asserts a never-written fork
reports `None` while the global sequence is non-zero. The probe now reddens on exactly that
assertion. The test lives outside the public-API file on purpose: that file's charter is to use
nothing but `Tree`, `BranchHandle` and `Transaction`, and there is no public flush.

Verification: **1,028 → 1,037 lib tests** (8 public-API tests including a compile-checked public
surface snapshot, 1 discriminating derivation test), 4 doc tests, clippy
`--all-targets --all-features` at zero warnings, fmt clean.

## PA gate record (adversarial gate passed 2026-08-14, with five amendments)

Verified against the tree before writing the public branch API.

**Amendment 1: `BranchInfo.head_seq` as planned is not derivable, and the honest field is a
different one.** The plan says to derive it from "the owner's state `last_sequence` plus its live
memtables". But `LevelManifest.last_sequence` is a single **global** field
(`src/levels/mod.rs:125`) that `persist_owner_update` writes identically into every owner's state
manifest — it is not per-owner and never was. Under one global commit clock a per-branch "head"
is not even a well-defined notion; what IS well defined, and what diff needs later, is *the
newest sequence this branch wrote itself*. So the field is `last_write_seq: Option<u64>` =
max(owner's tables' `largest_seq_num`, owner's active and immutable memtables' `lsn()`), with
`None` for a branch that has never written. `MemTable::lsn()` already exists and carries
`#[allow(unused)]` (`src/memtable/mod.rs:394`) — PA gives it its first real consumer and removes
the attribute.

**Amendment 2: `ForkReceipt` cannot be public.** It holds `BatchOwner`, which is `pub(crate)`, so
exporting it would put a private type in a public interface. Rather than reshaping it,
`Tree::fork_branch` returns a `BranchHandle` — which is what plan D9 specified in the first place
and is more useful, since the handle is immediately usable. `ForkReceipt` stays crate-internal
for `Core::fork_branch`'s idempotency path.

**Amendment 3: `BranchInfo` cannot carry `ParentLink`** (it lives in `authority::format`, which is
`pub(crate)`). It gets its own public `BranchLineage { branch, generation, fork_seq }` built from
`BranchId` and `BranchGeneration`, both already public.

**Amendment 4: the branch errors need no work.** `Error` is a public enum, so `BranchFenced`,
`BelowRetentionFloor`, `MaterializationRequired`, `ForkFenceTimeout` and `TimestampBelowHorizon`
are already reachable. The plan's "expose the branch errors" step is a no-op.

**Amendment 5: `TransactionOptions` is not public**, so the handle offers `begin()` and
`begin_with_mode(Mode)` (`Mode` is exported) rather than the `begin_with_opts` shape `Tree` uses
internally.

Also confirmed: `BranchCatalog::list()` has only test consumers today, so `list_branches` gives it
its first production caller; a handle needs no explicit generation check because
`Transaction::new_owned` already validates the owner and returns `BranchFenced`; and TTL is
expressed publicly as a `Duration` converted against `Options.clock` (wall-clock nanoseconds),
so callers never have to know the engine's clock unit.

### PA2 — Delete the parallel prototype engine: complete (2026-08-14)

The first slice of the remaining-work plan (public API → diff/merge → adapters). Its gate finding
is why it runs second instead of last: the tree held **two `table.rs` and two `format.rs`**, one
pair live and one dead, so "edit the format file" was a coin flip for every later slice.

Deleted (`git rm`), 4,058 lines and 37 test attributes:

- `src/database.rs` — `KernelDatabase`, a BTreeMap engine over the injected roles (3 tests)
- `src/table.rs` — a second table format and ranged reader (7 tests)
- `src/format.rs` — a second codec stack (4 tests)
- `src/testkit.rs` — reference fixtures (18 tests)
- `src/test/rewrite_public_tests.rs` (2 tests)
- `src/branch_native.rs` — a re-export shim "during integration"
- `src/lifecycle.rs` — a two-line placeholder comment
- `BranchModel` + `ReadSelector` + `WriteOperation` + 3 model/head tests in `src/branch.rs`
- `BranchRecord.head`, `BranchCatalog::advance_head`, and `api.rs`'s now-unused `CommitVersion`,
  `CommitTimestamp`, `DatabaseId`, `TableId`, and the `id32!` macro

Kept: `src/storage/*` — the three roles and their memory / local / sim / native implementations.
That seam is what the prototype existed to prove and is what PF1/PF2 port `Tree` onto.

**Two facts the gate turned up that the plan had wrong:**

1. **Eleven tests extract cleanly, not twelve.** The twelfth,
   `memory_sim_and_local_read_identical_immutable_table_bytes_by_range`, builds a table with the
   format being deleted and reads it through `ObjectStore`. `src/sstable` reads through
   `vfs::File`, so the claim cannot be expressed until PF2 makes table reads object-backed. It is
   recorded as a **PF2 obligation** in `docs/removed-surfaces.md` rather than rewritten into
   something weaker that would look like coverage without being it.
2. **The whole prototype was already `#[cfg(test)]`.** Every deleted module carried
   `#[cfg(test)]` on the line above its `mod` declaration, so none of it ever shipped in the
   library — it was pure test scaffolding. That also produced the one real hazard during the
   edit: removing a `mod X;` line orphaned its `#[cfg(test)]` onto the *next* module, silently
   making `error`, `iter`, `lockfile` and `task` test-only. The compiler caught it immediately
   ("found an item that was configured out"), but it is worth knowing that attribute-carrying
   `mod` lines cannot be deleted line-wise.

Coverage is accounted for individually, not in aggregate: 11 of `testkit.rs`'s 18 tests moved to
`src/storage/conformance_tests.rs` unchanged and pass there; 4 became written PF1 obligations
(identity published before open returns; unknown commit reconciles across a crash; failed root
publish leaks an object but recovers complete old state; local spine reopens through the durable
authority); 1 became the PF2 obligation above; 2 compare the engine against the reference model
and die with it by design. All of that is in `docs/removed-surfaces.md`, written **before** the
code was deleted.

Two architecture guards asserted the prototype was *wired in*
(`crate_root_keeps_existing_engine_during_branch_native_integration` required `mod database;` and
`mod table;`) and correctly went red. They were inverted rather than deleted:
`crate_root_exposes_one_engine_over_the_injected_roles` keeps the retention half, and a new
`removed_prototype_engine_remains_absent` guards the deletion — files, `lib.rs` declarations, the
model's types inside `branch.rs`, and the surviving role seam — with two non-vacuity assertions
(it must find `struct BranchCatalog` in the file it parses, and must not match a planted
placeholder).

Verification: **1,053 → 1,028 lib tests**, all green (37 removed, 11 extracted, 1 new guard added
— the arithmetic closes exactly), clippy `--all-targets --all-features` at zero warnings, fmt
clean. `src/` net −2,433 lines.

## FK5 gate record (adversarial gate passed 2026-08-14, with three amendments)

Verified against the tree. `BranchCatalog::delete` refuses only the default branch — a parent of
Active children can be tombstoned today, which the catalog loader then rejects at the next open
(invariant 3), so the store would publish itself into an unopenable state. `expires_at` exists on
`BranchRecord` and round-trips through the format, but nothing sets it and nothing enforces it.
`TaskManager` has two notify-driven loops and no timer.

**Amendment 1: metadata GC is keep-last-K with no min-age and no root fence.** The spec asks for
keep-last-K + never-pinned + min-age fenced by the root's `catalog_version_floor`. Two of those
three are unnecessary here and one is a stale field. Recovery reads only the NEWEST catalog version,
and `resolve_from_hint` probes FORWARD, so a state version below the newest is never needed — a
missing hint resolves to a newer version by design (FK1's stale-hint test). Min-age exists to
protect readers mid-open of a file being unlinked; a metadata reader already holds the bytes it
decoded, so there is nothing to protect, and dropping it removes a clock/mtime dependency from the
authority layer. `catalog_version_floor` is written as a literal 0 and never read: it is a stale
field by the project's own rule, so it leaves format v1 now. The checkpoint phase reintroduces a
pin — with an actual pinning source — when it has one.

**Amendment 2: TTL expiry runs at the tail of the existing maintenance wake, not on a timer.** A
resident timer loop would violate the task/executor seam (D10 seam 3) that the object and edge
adapters depend on. The sweep is a re-entrant step at the end of the level-compaction task, plus a
`pub(crate)` entry point so tests drive it deterministically instead of waiting on wall clock.

**Amendment 3: expiry does not cascade.** An expired parent with Active children is skipped, not
deleted along with its subtree: deleting it would either break the children's views or silently
destroy branches whose own TTL has not fired. Its tombstone lands on the first sweep after the last
child goes. Cascading deletion is a policy the public API can offer explicitly; the maintenance
sweep must not infer it. `BranchesUnrelated` stays out of this slice — it is a merge-phase error
with no producer until diff/merge lands.

### FK4 — Exact fork, three selectors: complete (2026-08-14)

As built:

- **`Core::fork_branch(parent, child, ForkPoint)`** — one durable step. Under `catalog_publish` (the
  branch-op mutex): resolve the parent and check idempotency; refuse a chain that would exceed
  `MAX_VIEW_DEPTH`; fence writes with `lock_writes()` and drain the commit pipeline; resolve the
  anchor per selector; release the fence; then take `level_manifest.read()` and
  `branch_catalog.write()` and publish ONE catalog version. That publish is the commit point.
- **No data is written.** Gate amendment 1 held up: the child inherits committed-but-unflushed parent
  rows through the parent's live memtables under its cap, so the bounded delta table, its orphan
  crash window, and `CatalogEntry.delta_table` are all gone. `ForkPoint::LastDurable` is unnecessary
  — every fork point is exact.
- **`ForkPoint::{Head, AtVersion, AtTimestamp}`** and `ForkReceipt { child, parent, fork_seq }`.
  `AtVersion` refuses above the drained head; `AtTimestamp` resolves through FK2's timeline
  exact-or-abstain; every selector is checked against the parent's retention floor.
- **Drain on the queue, not the counters** (gate amendment 3), with `FORK_DRAIN_TIMEOUT` and a typed
  `ForkFenceTimeout` so a commit future dropped between enqueue and apply cannot hold the write mutex
  forever.
- **`retained_floor_seq`** in `BranchStateManifest`, `LevelManifest::{retained_floor,
  raise_retained_floor}`, and `CompactionIterator::retained_floor_advance()`, raised past the
  SURVIVING version of any key that lost one and published atomically with that compaction's state
  version. `Error::BelowRetentionFloor` carries the boundary.
- **Idempotency.** A retried fork returns the original receipt; a name reused with a different
  parent, a different `AtVersion`, or an existing non-fork branch fails closed with a specific
  message.

One design claim was checked and corrected by a test: my first `retained_floor_seq` validation
required `floor <= last_sequence`, and `test_tombstone_propagation_through_levels` reddened on it.
The test was right — a bottom-level hard delete drops the key's newest version, so the floor must
clear a tombstone sequence that no longer survives in any table, and `last_sequence` only tracks
what survives. The validation was removed with that reasoning recorded in the format.

Verification: 1,046 lib tests green at the slice boundary, clippy and fmt clean. Nine new tests:
exact fork over unflushed rows (asserting the rows really are unflushed), every acknowledged commit
inherited, `AtVersion` history, `AtTimestamp` exact-or-abstain, above-head refusal, floor refusal
with a pre-compaction success arm, idempotent retry with three divergence arms, crash-after-publish
reopen, and the depth-budget refusal with the at-budget success arm. Two pipeline-level tests state
amendment 3 directly: `is_drained` is false while a batch applies, and a failed WAL append drains
the queue while `visible_seq_num` can never reach `log_seq_num - 1`.

## FK4 gate record (adversarial gate passed 2026-08-14, with four amendments)

Verified against the tree. The fork protocol as written in §4 does not survive contact with the
code; four things change, one of them large.

**Amendment 1 (large): the bounded delta table is deleted from the design.** §4/A4/C11 require a
child-owned SST covering committed rows in `(durable_table_seq, F]`, because in the pre-v4 model a
child held physical references to parent SSTs and therefore could not see anything still in the
parent's memory. v4 §3.2 replaced that with a logical view over the parent's LIVE runtime — and
`Snapshot::new_owned` (FK3) already resolves the ancestor's active and immutable memtables, capped
at `F`. So committed-but-unflushed rows at or below `F` are visible to the child with no delta at
all, and after a crash the parent's WAL replay restores exactly the same rows into the same
runtime, so recovery needs no coverage check either. C11 is vacuous under the shipped model, and
§3.3c self-containment with it. Consequences: no delta build, no orphan-delta crash window, no
second physical-reference mechanism, no `ForkPoint::LastDurable` distinction (every fork point is
exact), and fork is genuinely O(metadata) even on a hot parent. `CatalogEntry.delta_table` is
removed from format v1 — it is already a stale field today: `to_entries` always writes `None` and
`from_manifest` drops it, so nothing in the runtime can carry it. Golden vectors regenerate.

**Amendment 2: `retained_floor_seq` must land in FK4, not later.** `AtVersion`/`AtTimestamp` fork at
a historical `F` where the parent may already have collapsed that history; with no floor the child
is silently short of rows — the exact silent-empty failure the plan bans. It goes in
`BranchStateManifest`, advanced by the compaction that drops versions and published atomically with
that compaction's state version (`persist_owner_update`). The advance rule is
`floor = max(floor, newest seq of any key that lost a version)`, not `highest dropped seq + 1`:
dropping `k@30` in favour of `k@40` breaks every view capped in `[30, 39]`, so the floor must clear
the surviving version. Known conservative consequence, accepted and documented: a parent with a
child at anchor `A` may refuse a NEW fork at `V <= A` even though the pin kept that history intact,
because the floor does not record which pins were in force. The first-class workaround is to fork
the child instead of the parent — a write-free child has the identical view — so no capability is
lost. With versioning disabled (the default) the floor tracks close to the newest sequence and
historical forks are effectively unavailable; that is honest, since such a store keeps no history.

**Amendment 3: the drain condition is the commit QUEUE, not the sequence counters.** C2 specifies
draining to `visible_seq_num == log_seq_num - 1`. That condition is unreachable after any failed
commit: `write_prepared` (WAL) failure rolls back oracle entries and drains the queue slot but
cannot rewind `log_seq_num`, so the gap is permanent and the drain would spin forever. The shipped
drain holds `lock_writes()` (which blocks enqueue, since enqueue is inside the critical section)
and waits for `CommitQueue` head == tail; at that point every allocated batch has been published or
discarded, so `F := visible_seq_num` is exactly the readable head. Sequence gaps are harmless: they
are seqs no row will ever carry. The wait is bounded and fails closed with a typed error rather
than blocking all writes forever, because a caller whose commit future is dropped between enqueue
and apply would otherwise hang the fork with the write mutex held.

**Amendment 4: the commit point takes `level_manifest.read()` before `branch_catalog.write()`.**
FK3 established the lock order `level_manifest -> branch_catalog` and made compaction re-validate
the pin floor under the manifest write lock. The fork's floor check must be serialised against
compaction publication the same way, so it reads the floor under `level_manifest.read()` while
holding `branch_catalog.write()` through the publish. The two checks interlock exactly: whichever
of {fork, compaction} publishes second is the one that sees the other and fails closed. The commit
fence is released BEFORE this pair is taken — `F` is already captured, and rows committed
afterwards carry seqs above it.

Also verified as safe: `apply` (and its arena-full rotation path) never takes `write_mutex`,
`catalog_publish`, or `level_manifest`, so a fork holding the write mutex while draining cannot
deadlock against in-flight applies. `catalog_publish` is the natural branch-op mutex — no commit
path takes it — and holding it across the whole fork serialises branch operations by design.

### FK3 — Logical inherited views and the retention promise: complete (2026-08-14)

As built:

- **Catalog links and anchors.** `BranchRecord.parent: Option<ParentLink>`; `create_fork` validates
  the parent is live at the linked generation; `min_active_child_anchor(parent, generation)`,
  `record_has_parent`, and `parent_chain(branch, generation, max_depth)` derive everything from the
  catalog alone. `from_manifest` fail-closes when an Active child's parent is not Active at the
  linked generation (invariant 3 as a load check). `MAX_VIEW_DEPTH = 64`, exceeded →
  `Error::MaterializationRequired { depth }`.
- **Read layering.** `Snapshot` holds `layers: Vec<SnapshotLayer>` built in `new_owned` from the
  ancestor chain, each carrying `cap = min(snapshot seq, every fork anchor on the path)` —
  cumulative, so a grandchild is bounded by the *minimum* anchor. Point reads walk layers
  nearest-first via `get_in_layer`; a `LayerHit::Tombstone` stops the walk (a child's delete hides
  farther layers). `IterState` became `layers: Vec<IterLayer>`, and every layer's iterators are
  wrapped in `SeqCappedIterator` BEFORE the k-way merge, because the global snapshot-seq filter
  cannot express per-layer caps.
- **Retention pin.** `CompactionOptions::for_owner` samples `history_pin_floor` (min Active-child
  anchor) and `force_not_bottom` (has Active children OR is itself a child, C1(c)) from the
  catalog. The floor rides into `CompactionIterator` and applies as an OVERRIDE layer over the
  existing cascade (`should_output = output_ignoring_pin || pinned_by_child_view`), so a store with
  no children behaves byte-identically to one that never forked, and `pin_retained_versions` counts
  exactly the versions the pin saved.
- **Amendment to gate item (2), forced by the code.** The pin is not one rule but two shapes, both
  independent of live snapshots: a versioned parent pins the whole range at or below the anchor
  (children inherit full history), a non-versioned parent pins only the newest version at or below
  it (all a point-in-time child can read). A single range rule would have over-retained every
  version of every key in a non-versioned store for the life of the child; a single point rule
  would have made inherited history depend on which snapshots happened to be live during
  compaction, because `snapshot_allows_drop` is true for `BoundedBySnapshot` and collapses
  same-boundary history.
- **New amendment (5): publication re-validates the floor.** The floor is sampled before the merge,
  outside the manifest lock, so a fork published mid-merge could pin history the job already
  dropped. `update_manifest` re-reads the floor under the manifest write lock and returns
  `Error::CompactionPinRaced` if it regressed; the output file is removed, `HiddenTablesGuard`
  restores the inputs, and the scheduler treats this one error as "retry next cycle" rather than a
  failed cycle. This discharges the coordination FK4's fork protocol would otherwise need, and the
  orphan-removal it adds fixes a pre-existing leak on every `update_manifest` failure path.
- **Lock order** is now `level_manifest -> branch_catalog`; every other catalog acquisition in the
  tree is leaf-level, verified site by site. `CoreInner.branch_catalog` became
  `Arc<RwLock<BranchCatalog>>` so a compaction job can hold the handle.

Two pre-existing defects fixed at the root, both found by building the pin next to them:

- **C1(a)/(b), precisely.** The bottom-level "latest is a hard delete → drop the whole key"
  shortcut ran ahead of the per-version snapshot check, so a snapshot below the tombstone lost its
  version. The first predicate written for this ("is any version snapshot-bounded") was too coarse
  and reddened `test_snapshot_compaction_tombstone_at_bottom_with_snapshot` — correctly, because a
  snapshot ABOVE the tombstone sees the delete and drop-all stays legal. The shipped rule is
  `hard_delete_may_drop_all`: illegal exactly when a reader boundary (live snapshot seq, or the pin
  floor) lands in `[oldest_seq, delete_seq)`. When it is illegal the tombstone is retained too, or
  the surviving version would resurrect for readers at or above it.
- **`commit` released the oracle watermark but not the read snapshot.** `rollback` took the
  snapshot; `commit` did not, so a committed `Transaction` kept alive in a struct field (or merely
  shadowed in a Rust scope) pinned the retention boundary compaction honours for as long as the
  handle lived. Both paths now call `release_reader_state`. This surfaced as a *test* whose
  retention assertion was passing on a leaked snapshot pin rather than on the fork pin —
  `snapshots=[0, 1]` at compaction time in a test with no live readers.

Verification: 1,035 lib tests green, clippy `--all-targets --all-features` clean, fmt clean.
`src/test/fork_view_tests.rs` (7 tests) covers anchor-bounded reads, child shadowing and
tombstones, capped range merge, cumulative grandchild caps, the depth-cap error, the end-to-end
bottom-level retention twin, and the `force_not_bottom` derivation in all three shapes;
`iterator_tests.rs` adds 3 unit arms (pin vs bottom-level delete, the snapshot regression, and pin
shape) each with a sabotage twin; `compaction_tests.rs` adds the pin-race abort with its control
arm. Sensitivity probes run and recorded: forcing `pinned_by_child_view = false` reddens the
end-to-end retention test on its own assertion, and the retention test now asserts no snapshot is
live at compaction time so it can never again pass for the wrong reason.

### FK2 — Commit timestamps and the timeline: complete (2026-08-14)

As built:

- **Batch header** gains a fixed-width `commit_ts` at the named offsets
  (`BATCH_HEADER_TS_OFFSET/LEN`, bound by the same encode/patch/decode asserts as the seq —
  the P1 hardening paying off exactly as intended); `patch_encoded_seq` became
  `patch_encoded_header(seq, commit_ts)`. The constructor placeholder mirrors
  `starting_seq_num` (documented: the pipeline ALWAYS overwrites it under the write mutex, and
  hand-built batches with increasing seqs form valid monotone timelines).
- **`Timeline`** (`src/timeline.rs`): strictly monotone clamp `max(now, last+1)` stamped in
  `write_prepared` under the commit write mutex; `(commit_ts, highest_seq)` fenceposts with a
  named in-memory cap (4096) whose pruning moves an observable horizon floor;
  exact-or-abstain `resolve(ts) -> seq` with the typed `Error::TimestampBelowHorizon`;
  `horizon()` as the metered coverage quantity. Shared as one `Arc` between `CoreInner`
  (stamping) and `LevelManifest` (root publishes snapshot `last_commit_ts` + a 512-entry tail).
- **Recovery**: replay validates strict timestamp monotonicity exactly like sequences
  (regression = fail-closed corruption, repair cannot mask it) and returns a `ReplayOutcome`
  struct carrying per-batch fenceposts — fenced/deleted batches fencepost too (their instants
  were allocated; resolving onto them is exact and their data is simply unreachable). Open
  seeds the timeline root-tail-first then replay-past-the-tail (the overlap is filtered by
  timestamp); checkpoint restore resets the timeline wholesale from the restored root
  (whole-clock swap).
- Two fixtures that hand-craft WAL batches after real commits now stamp monotone timestamps
  explicitly (the mixed real/crafted timeline was the only fixture class the new validation
  caught — updated to be honest, not exempted).
- Exit evidence: unit tests for clamp/resolution/pruning/seeding; integration gate tests
  (`src/test/timeline_tests.rs`): backdated-clock stamping with exact per-commit resolution;
  crash-reopen resolution identity with the clamp resuming above the recovered maximum;
  pre-floor resolutions served from the durable root tail after the replay floor advances; and
  the timestamp-regression sabotage twin (fails closed; increasing-timestamp arm opens fine).
  1023 library tests passed, 1 ignored; strict Clippy clean; `cargo fmt --check` clean.

## FK2 gate record (adversarial gate passed 2026-08-14)

Verified against the tree: the batch header extension lands exactly on FK-hardening's named
offset constants (`encode`/`decode`/`patch` all derive from them); the commit timestamp is
taken pre-pipeline today (`clock.now()` in the transaction) and only feeds per-entry versioned
values — nothing stamps a commit-ordered timestamp; the root format already carries
`last_commit_ts` + `timeline_tail` (golden-pinned, strictly increasing on both axes);
`MockLogicalClock` exists for backdated-clock sabotage; the clock trait documents monotonicity
but neither the mock nor a wall clock guarantees it — the clamp is load-bearing.

Amendments adopted: (1) the clamp is STRICTLY increasing (`max(now, last + 1)`) — the root
format's strict-increase law makes equal-timestamp fenceposts unrepresentable; (2) the timeline
lives as one shared `Arc<Timeline>` reachable from both the commit path (stamping under the
write mutex) and `LevelManifest::persist_root` (tail snapshot) — the compactor persists roots
without `CoreInner` access, so the manifest must carry the handle; (3) fenced/deleted batches'
timestamps feed the recovered clock and timeline exactly like their sequences (BR6's rule
extended to the time axis); (4) per-entry timestamps are UNCHANGED (user `set_at` semantics);
the header timestamp exists for the timeline only; (5) the in-memory fencepost window is
bounded (named cap) with a typed `TimestampBelowHorizon` abstain and a horizon getter — the
durable tail (512) plus WAL replay define coverage after reopen.

### FK1 — Numbered-lineage authority: complete (2026-08-14)

As built (design: docs/FK_AUTHORITY_FORK_DESIGN.md FINAL v4):

- **Formats** (`src/authority/format.rs`): `SKBC` catalog / `SKBM` branch-state / `SKRT` root,
  all little-endian `magic + version(=1) + body + crc32`, caps checked before allocation,
  canonical ordering enforced, trailing bytes rejected, version identity checked before the
  checksum. Golden vectors byte-pinned bidirectionally with a flipped-byte sabotage arm
  (`golden_tests.rs`); the full fail-closed decode ladder is exercised per format.
- **Publication** (`src/authority/publish.rs`): conditional create via temp → fsync →
  `fs::hard_link` → dir-fsync (verbatim the proven `storage/local.rs` mechanics — rename is
  never used for version files); byte-compare `AlreadyExistsSame` gives idempotent retries;
  divergent same-version bytes fail closed and provably do not clobber. Latest = max id;
  `resolve_from_hint` probes forward past stale hints (SlateDB pattern).
- **AuthorityStore** (`src/authority/store.rs`): typed load/publish per lineage,
  decode-own-bytes-before-publish, header-version==filename and db_id identity validation.
- **Integration**: `LevelManifest` lost its file — `hydrate` rebuilds the runtime aggregate
  from catalog → states-by-hint → root (fail-closed: unknown branch, generation/filename
  mismatch, cross-state duplicate table, owner-metadata mismatch); `persist_owner_update`
  publishes the touched owner's state + a root version (a root failure after a state success
  intentionally keeps counters — the superset is healed by probing); the shutdown floor is a
  root-only publish. The catalog is DURABLE: `CoreInner::{create_branch, delete_branch}`
  mutate-then-publish with rollback-on-failure, generations allocate from the catalog's global
  monotone allocator (`BranchCatalog` lost `name_generations`; anchors
  `created_at_seq`/`deleted_at_seq` recorded and folded into the recovered clock alongside the
  root's visible floor — invariant 6, with the restore path applying the same rule). Writer
  epochs bump lazily on the session's first catalog write. Table ids allocate from the root's
  block-reserved watermark (block 1024); hydrate additionally resumes above the max referenced
  id, so a lost root can never cause reuse. `db_id` is minted from the injected clock + pid +
  counter. Checkpoints capture and restores swap the three lineage directories as one unit
  (restore reloads catalog + hydrates — a whole-database swap including the catalog).
- **Deletions** (no shims): `write_manifest_to_disk`, `load_from_file`, `replace_file_content`,
  `SnapshotInfo` + manifest snapshot list + changeset fields, `MANIFEST_FORMAT_VERSION`,
  owner encode/decode for the old file, `Levels::{encode,decode}`, `Options::{manifest_dir,
  manifest_file_path}`, the BR6 injected-catalog builder seam (`with_initial_branch_catalog`,
  `Tree::new_with_catalog`), and `CoreInner::lookup_branch` (written, unused, deleted — FK4
  re-adds it with its first caller). An enforced-absence guard
  (`removed_manifest_file_engine_remains_absent`) keeps the single-file engine from returning.
- **Test rewrites**: `register_branch` is the durable op; the BR6/BR7 reopen fixtures dropped
  catalog injection entirely (the durable catalog IS the fixture); the BR6 unknown-owner test
  became the honest deleted-branch-tombstone-fences-replay test; corrupt-log-number fixtures
  poison the root's `wal_reclaim_floor` by decode→mutate→re-encode; the crash-simulation
  fixture that dropped a Tree without close was itself a fidelity bug — `Tree::drop` SPAWNS an
  async close whose flush raced the reopen, masked before by whole-file rename overwrite and
  exposed by conditional create — fixed by closing with `flush_on_close = false`.
- **FK1 sabotage matrix**: corrupt latest catalog refuses open (with a pre-corruption
  non-vacuity arm); a deleted newest root is healed by forward probing with no data loss and
  no version collisions; future state version rejected by identity through the full open path;
  writer epoch bumps once per writing session, monotone, and not at all for read-only
  sessions; publish idempotency and temp-litter cleanliness; catalog cap enforced at the
  format layer before allocation.
- Exit evidence: 1015 library tests passed, 1 ignored; strict all-target all-feature Clippy
  clean; `cargo fmt --check` clean.

## FK1 gate record (adversarial gate passed 2026-08-14)

Design: docs/FK_AUTHORITY_FORK_DESIGN.md is FINAL (v4 + review amendments + strata-parity
resolutions: full-history range pin, catalog cap 4096 fail-closed, chain depth 64).

Verified against the tree for FK1:

- 34 `write_manifest_to_disk` references across `lsm.rs`, `compaction/compactor.rs`,
  `levels/mod.rs`, and the compaction/manifest test files — the rewrite scope the review
  budgeted. `SnapshotInfo`/`new_snapshots`/`deleted_snapshots` have no non-test producers and
  the tracker is never seeded from disk — deleted in this slice.
- Today's manifest file: BigEndian, no magic, no checksum; header carries
  `next_table_id`/`log_number`/`last_sequence`; per-owner level sets store table ids only
  (facts hydrate from SSTs) — the state format inherits exactly that shape per branch. New
  formats standardize on little-endian (matches the batch header and strata).
- The publish primitive must be synchronous (flush paths run under locks); the async
  `src/storage/local.rs:137` hard-link mechanics are re-implemented as a small sync helper in
  the authority module, byte-compare `AlreadyExistsSame` included.
- The BR6/BR7 injected-catalog tests are REWRITTEN, not migrated: with a durable catalog the
  scenarios become honest (create/delete/recreate as durable catalog versions in session one;
  reopen reads the real catalog). The BR6 "unknown owner" test becomes "deleted branch's WAL
  batches are fenced by the durable tombstone".
- Gate amendments: (1) v1 catalog format carries NO pin section — process-local pins suffice
  for local fork (v4 §4-FK4) and checkpoint pins belong to their own phase; speculative format
  fields are exactly the legacy the mandate forbids; a version bump adds them when a phase
  needs them. (2) `db_id` is introduced (16 bytes from the injected clock + pid + counter) for
  identity validation across lineages; global uniqueness is not claimed until the adapter
  phase mints real ULIDs. (3) `name_generations` in `BranchCatalog` is deleted — the global
  monotone `next_generation` allocator subsumes it.

## Next phase — fork/inherited-view publication (gate opened 2026-08-14; authority decision pending)

The BR0-BR7 ladder closed every runtime prerequisite. Remaining P4 scope (consolidated plan §5.3,
§817-850): flattened `InheritedView` + capped multi-source iterators; exact `Head` fork with
bounded fork-delta; historical version/timestamp fork; nested-view coalescing and budgets; typed
`MaterializationRequired` + fork receipts; and the durable publication these require.

Verified against the tree:

- The integrated engine has no durable authority beyond the full-rewrite `LevelManifest`: the
  catalog is memory-only (BR6's injected-authority seam), there is no root, no journal, no
  timeline, no inherited views. Snapshot reads stack memtables + owned levels only.
- The P0-era prototype modules (test-only evidence per this file's header) already implement the
  durable model the fork protocol needs: `DatabaseRoot` (encode/decode; identity, clock,
  version→timestamp timeline) in `src/database.rs`; `LocalCommitStore` (append-only framed root
  journal + two durable root slots + writer lock + recovery) in `src/storage/local_commit.rs`;
  remote-ready table/descriptor formats in `src/table.rs`/`src/format.rs`. Fork itself was never
  prototyped.
- The fork protocol (§5.3) requires child catalog entry + child branch state to publish in ONE
  atomic root transition; a catalog file separate from table state would reintroduce a two-file
  crash window.

Decision (user, 2026-08-14, after reference studies of strata-core, SlateDB, RocksDB-Cloud, and
the P0 prototype): catalog-commit-point authority with a RETENTION PROMISE making every fork
child re-materializable from its parent (strata's model, affordable because our rebuild is
metadata-only), combined with SlateDB's numbered-immutable-version lineages, in-record epoch
fencing, and TTL'd self-cleaning pins; complete new design — no legacy shims, built for agentic
workloads (branch TTLs and ephemeral operation pins first-class); all three fork selectors
(`Head`/`AtVersion`/`AtTimestamp`) ship together; `LastDurable` removed. v4 incorporates the
user's code-grounded review of v3 (12 findings + gaps, all resolved — ledger in the doc's §0);
the structural change is LOGICAL inherited views resolved through the parent's live state, which
dissolves rebuild into the read path, removes physical table refs from child metadata entirely,
and lets a fresh fork child publish exactly one catalog version. Full design + FK1-FK5 slice
ladder + explicit parent-plan reversals: docs/FK_AUTHORITY_FORK_DESIGN.md (DRAFT v4, awaiting
user review before any code).

### Literal/sentinel hardening and format renumbering: complete (2026-08-14)

User directive: no duplicated magic literals, no versioned-lineage relics — this is a rewrite, so
every format identifier is version 1 of this line, and cross-subsystem agreements are named
constants with checked invariants. As built:

- **Format renumbering.** `BATCH_VERSION` 4 → 1 and `MANIFEST_FORMAT_VERSION` (was `_V2` = 2) → 1;
  the retained `MANIFEST_FORMAT_VERSION_V1` rejection relic is deleted. Both decoders reject any
  other version by identity; pre-rewrite files that happen to share the number fail the
  fail-closed structural validation (batch count sanity, manifest owner validation) — this line
  makes no on-disk compatibility promise (old stores migrate externally). The rejection tests now
  derive their foreign versions from the constants (`0` and `current + 1`), so a future renumber
  cannot silently vacate them. `format.rs`, `database.rs`, and `checkpoint.rs` were already 1.
- **P1 — batch header layout named once**: `BATCH_HEADER_VERSION_OFFSET` / `BATCH_HEADER_SEQ_OFFSET`
  / `BATCH_HEADER_SEQ_LEN`; `encode`, `decode`, and `patch_encoded_seq` all derive from them, with
  debug asserts binding each to the layout (directly de-risks the planned C6 `commit_ts` header
  field).
- **P2 — trailer packing defined nowhere twice**: new `make_trailer(seq, kind)` beside
  `trailer_to_seq_num` in `lib.rs`; the four inline `(seq << 8) | kind` / `>> 8` sites
  (`transaction.rs` ×2, `memtable/mod.rs` ×2) and `InternalKey::new` itself now delegate.
- **P3 — WAL segment filenames built only by `segment_name()`**: the raw `format!("{:020}.wal")`
  construction in cleanup and corruption-repair paths now calls the named builder; error/log
  messages render the resolved path instead of hand-formatting the name.
- **P4 — the reference model uses `DEFAULT_BRANCH_NAME`** instead of `"main"` literals (including
  its delete guard) — the oracle can no longer drift from the engine's constant.
- **P5 — the point-get `timestamp: 0` sentinel is documented** at its construction site (callers
  use value + seq only; versioned reads take the iterator paths).
- **P6 — axis-named MAX constants**: new `INTERNAL_KEY_TRAILER_MAX` and
  `MemTable::NO_WAL_DEPENDENCY`; the raw `u64::MAX` seek sites in `snapshot.rs`/`transaction.rs`
  now use `INTERNAL_KEY_SEQ_NUM_MAX` / `INTERNAL_KEY_TIMESTAMP_MAX` / `INTERNAL_KEY_TRAILER_MAX`
  (bit-identical: `u64::MAX << 8 == SEQ_NUM_MAX << 8`).
- **P7 — dependency-id start-at-1 documented as non-sentinel** (0 carries no meaning anywhere;
  the offset only keeps 0 out of logs).
- Together with the earlier two fixes in this class: `BranchId::DEFAULT` (named reserved identity
  + fail-closed open check) and `MemTable.wal_number` initialized to `NO_WAL_DEPENDENCY`.
- Exit evidence: 1000 library tests passed, 1 ignored; strict all-target all-feature Clippy
  clean; `cargo fmt --check` clean.

### BR7 — Maintenance pressure and shutdown/checkpoint parity: complete (2026-08-14)

As built:

- Per-owner compaction under the one scheduler: `CoreInner::compact` iterates
  `LevelManifest::owners()` (new accessor) and runs the same strategy per owner set via
  `CompactionOptions::for_owner` (the default-only `from` constructor is gone). Without this, a
  foreign branch's L0 never compacted and — stall accounting being owner-scoped since BR4 — a
  branch reaching the L0 threshold would have parked its writes permanently.
- Shutdown and checkpoint capture every dirty runtime uniformly: both rotate each runtime's
  non-empty active (memory-only) and drain every queue via `flush_all_immutables_sync` (now
  returning the flushed count); the shutdown replay-floor changeset is unchanged (its dependency
  snapshot already spans all owners). The orphaned default-only
  `flush_memtable_and_update_manifest` was deleted with its last caller.
- Trickle WAL-span policy (plan amendment A5): new option `wal_pinned_segment_limit` (default 8,
  validated ≥ 1, TreeBuilder passthrough). `CoreInner::rotate_wal_pinned_runtime_impl` retires AT
  MOST ONE victim per call — the non-empty active with the oldest pinned segment at least the
  limit behind the active segment. The memtable task calls it each loop pass (rotate one → flush →
  ask again; terminates when no victim and no backlog), and a size-driven WAL rotation in
  `write_prepared` wakes the task. `CompactionOperations` gained the hook with a no-op default so
  mocks stay untouched; `has_pending_immutables` now spans all runtimes.
- **Found by the gate's trickle test (a real BR4-era bug, exposed as a parallel-order flake):**
  `MemTable`'s `wal_number` initialized to 0 while `record_wal_dependency` is `fetch_min`, so a
  lazily-created branch memtable could never raise its baseline — every young branch looked
  pinned at segment 0, and victim selection tie-broke on nondeterministic registry order. Fixed
  at the root: fresh memtables start at `u64::MAX` ("no dependency yet") so the first recorded
  append segment is adopted naturally; all `get_wal_number` readers were audited (non-empty
  memtables always hold a recorded real segment; empty actives are explicitly baselined by
  open/rotation).
- **Default-branch identity de-hacked (user review finding):** the reserved id is now the single
  named constant `BranchId::DEFAULT` (documented as reserved; user branches never receive it);
  `BatchOwner::DEFAULT` and the open path derive from it, and `CoreInner::new_impl` fail-closes
  on any catalog — built-in or injected — that cannot validate the reserved identity, converting
  a two-literal coincidence into a checked invariant.

Exit evidence (two-level test gate):

- Owner-module regressions (`src/test/branch_runtime_tests.rs::br7_*` + one `br6_` addition):
  five foreign L0 tables compact into the owner's own L1 with all rows surviving; a
  flush-on-close shutdown leaves nothing to replay (no foreign runtime on reopen) while the
  branch row is served from its own durable levels; a checkpoint (which starts a fresh WAL, so
  SSTs are the only capture) serves a dirty branch active's row after restore; the trickle
  rotates exactly one victim per call, oldest first, terminates, and loses no acknowledged
  commit across reopen; one hundred cold dirty branches drain one-at-a-time (exactly 100
  rotations), the replay floor advances, segment cleanup reclaims below it with bounded
  retention, and all 100 rows read back after reopen; an injected catalog with a foreign default
  identity is refused at open.
- Vertical: 1000 library tests passed, 1 ignored (full run post-fixes); strict all-target
  all-feature Clippy and `cargo fmt --check` clean as of the identity change (final re-run
  deferred at user request — the subsequent wal_number initializer change has no lint surface).

## BR7 gate record (adversarial gate passed 2026-08-14, with four amendments)

Verified against the tree:

- Compaction is default-owner-only: `CompactionOptions::from` stamps `default_runtime.owner()`
  and `CoreInner::compact` runs one pick. A foreign branch's L0 therefore never compacts, and —
  because BR4 made stall accounting owner-scoped — a branch reaching the L0 stall threshold
  parks its writes permanently (no compaction ever clears it). This is the severest finding.
- `flush_all_memtables_for_shutdown` reads `self.immutable_memtables`/`self.active_memtable`
  through the default `Deref` only: on a flush-on-close shutdown, foreign runtimes' data stays
  WAL-only. Checkpoint's `flush_all_memtables` rotates only the default active (its immutables
  drain already visits every runtime), so dirty branch actives are missing from checkpoints.
- Nothing bounds a cold dirty branch's WAL pinning: a non-empty active holds its dependency
  segment forever, and only ArenaFull/budget events rotate it.
- Both background loops are single-scheduler `Notify` loops that call `CoreInner` trait methods —
  per-owner behavior extends them without new schedulers (invariant: one scheduler).

Written amendments adopted:

1. **Per-owner compaction inside the one scheduler:** `CoreInner::compact` iterates every owner
   present in the manifest and runs the same strategy per owner set (mixed-owner picks remain
   unrepresentable per BR3). `CompactionOptions` gains an explicit owner constructor.
2. **Shutdown and checkpoint capture all dirty runtimes uniformly:** rotate every runtime's
   non-empty active into its queue (memory-only, no WAL rotation), then drain every queue via the
   existing all-runtimes flush; the shutdown replay-floor changeset stays as-is (its dependency
   snapshot already covers all owners).
3. **Trickle WAL-span flush (plan amendment A5):** new option `wal_pinned_segment_limit`
   (default 8). `rotate_wal_pinned_runtime` retires AT MOST ONE victim per call — the non-empty
   active with the oldest pinned segment at least `limit` behind the active segment — so reclaim
   is a trickle by construction, never a mass flush. The memtable task calls it each loop pass
   (rotate one → flush → repeat until no victim and no backlog); size-driven WAL rotation in
   `write_prepared` wakes the task. Per-branch stall exemption for policy flushes is deferred: a
   trickle rotation adds one immutable, and the same pass flushes it.
4. **WAL max-file-size stays non-configurable** from the public builder in this slice; tests
   drive rotation explicitly. Exposing it is a follow-up option change, not BR7 scope.

## BR6 gate record (adversarial gate passed 2026-08-14, with five amendments)

Verified against the tree:

- Recovery today fails closed on any foreign-owner WAL batch (`replay_wal_with_repair`'s explicit
  BR6 placeholder guard) — a store holding branch writes cannot reopen. Replay already produces
  branch-pure memtables but splits on every owner alternation, has no generation fencing, and
  treats a batch larger than the arena as fatal.
- Installation contract at open: intermediates are registered with the BR1 dependency tracker and
  synchronously flushed; the last memtable becomes the active. `flush_immutable_to_sst` resolves
  the owning runtime from the registry, so foreign runtimes must exist before their recovered
  memtables flush.
- **The branch catalog is memory-only** — `create`/`delete` are `#[cfg(test)]`; production knows
  only the default branch and nothing survives reopen. The design docs place durable catalog
  persistence with the fork/root slice ("public branch handles remain unavailable until catalog
  persistence … all use the same integrated path"), not BR6.
- Batches append under the same `write_mutex` that allocates sequences, so WAL append order equals
  sequence order — strict monotonicity across segments is a sound replay invariant.
- Crash fixture: `flush_on_close(false)` + `close()` leaves the WAL as the only copy of recent
  writes.

Written amendments adopted:

1. **The catalog-at-open is the fencing authority, injected — catalog persistence is NOT BR6.**
   Recovery validates each batch's `(branch, generation)` against whatever catalog exists at open:
   live and matching → route; deleted, stale-generation, or unknown → drop as fenced, never
   installed into any runtime (invariant 9). Until the fork slice lands durable catalogs, a
   production reopen fences all non-default WAL data — which is today unreachable anyway, since
   branch creation itself is test-only. Tests inject catalogs through a test-only builder seam.
2. **Fenced batches still advance the recovered sequence clock.** Their sequences were allocated
   and possibly acknowledged pre-crash; seeding `visible_seq` below them would let new commits
   reuse those sequences and break every uniqueness/idempotency argument. `max_seq` is computed
   before the fencing decision.
3. **"Accept byte-identical duplicates" is replaced, for the local WAL, by strict monotone-sequence
   enforcement** — a batch whose sequences do not strictly increase over its predecessor is
   corruption and fails closed. Byte-identity duplicate tolerance is deferred to the
   object-storage replay slice that actually produces such duplicates.
4. **Per-(segment, owner) grouping with per-owner arena sizing.** One memtable per owner per
   segment (not split-per-alternation); default uses `max_memtable_size`, foreign owners use
   `branch_memtable_size`; replay right-sizes a fresh memtable for an oversized batch (the BR4
   `arena_available` rule extended to recovery, fixing the pre-existing fatal-on-large-batch
   replay path). The BR4 right-sizing logic is extracted into one shared constructor rather than
   duplicated.
5. **Installation mirrors the default policy uniformly per owner:** for each owner, all-but-last
   recovered memtables are registered and flushed during open, the last becomes the owner's
   runtime active; foreign runtimes are created at open via `get_or_create` (they hold data — not
   idle, so invariant 8 is untouched).

Deferred with owner: at-open orphan-SST deletion stays (owner-partitioned manifest already
protects foreign tables; the reachability-registry redesign is the file-lifetime slice);
byte-identity duplicate replay → object-storage replay slice; durable catalog + two-phase
create/delete → fork lifecycle slice.

## BR5 gate record (adversarial gate passed 2026-08-14, with three amendments)

Verified against the tree:

- Every read funnels through exactly two capture points: `Snapshot::get` and
  `Snapshot::collect_iter_state` (range, reverse, history, `get_at`, and count all build on the
  latter via `KMergeIterator`, whose only active-memtable consumer is one constructor). Both
  currently hardcode the default runtime (Core `Deref` + `levels_for(default)`), so a read-mode
  `Transaction::new_owned` today silently reads the DEFAULT branch's data — the defect BR5 fixes.
  Nothing calls it in a read mode yet (BR4 tests used `write_only` deliberately).
- The BR5 gate requirement "no global-table scan followed by owner filtering" already holds by
  construction: BR3 removed owner-blind level access, and `LevelManifestIterator` (which does
  traverse all owners) has no read-path callers — it is lifecycle-only.
- The oracle is already owner-scoped (`owned_fp` seeds the key hash with branch+generation;
  commit uses `check_owned`/`publish_owned`), so cross-branch same-key transactions cannot
  falsely conflict. No BR5 oracle work.
- `new_owned` validates the owner's catalog generation at transaction start (`BranchFenced`),
  which covers read transactions unchanged.

Written amendments adopted:

1. **Idle-branch reads allocate nothing (invariant 8 extends to reads).** The snapshot captures
   `runtimes.get(owner)` — an `Option` — and never `get_or_create`. Absent runtime → memtable-less
   view; absent owner level set → empty `Levels` (a missing DEFAULT set remains fail-closed
   corruption, as manifest load guarantees it). This is correct, not merely cheap: `seq_num()`
   returns the drained visible sequence and apply creates a runtime before publication advances
   (FIFO publish), so a runtime absent at snapshot creation can only ever contain sequences beyond
   the snapshot horizon.
2. **`IterState.active` becomes `Option<Arc<MemTable>>`** (it has a single consumer). Synthesizing
   an empty memtable instead is rejected — it would allocate a full arena per idle-branch read.
3. **Inherited views are structurally empty until fork (P4).** BR5 capture is the owner's own
   components only; the slice text's "explicit inherited views" gain content with fork
   publication, not here.

Deferred with owner: per-branch snapshot trackers — the global tracker stays, so a branch
snapshot conservatively pins version visibility for every owner's compaction (correct,
performance-only) → BR7 maintenance pressure.

Scope: owner plumbing through `Snapshot` + capture routing + two-level tests (point/range/reverse/
history and tombstone isolation across owners on identical keys; idle-branch read non-allocation
with a non-vacuity arm; a flushed-branch read proving owned-levels routing, not just memtables).

## BR4 gate record (adversarial gate passed 2026-08-14, with three amendments)

Verified against the tree: transaction/batch owner plumbing already exists and is catalog-validated
pre-commit (`transaction.rs:269`, `Batch::for_owner`), so routed apply is wiring, not new protocol;
the BR1 dependency tracker already computes the replay floor from dependency snapshots, which is
what makes rotation decoupling sound; BR3's `levels_for(owner)` provides owner-scoped L0 counts.

Written amendments adopted:

1. **"Chunked" arena backing is replaced by right-sized lazy arenas.** The arena is one contiguous
   `Box<[u8]>` with `ptr -> offset` reverse mapping (`memtable/arena.rs:118`); chunked backing
   would rewrite unsafe skiplist internals for little gain, because the lazy unit is the *runtime*:
   a registry entry allocates nothing, and a `BranchRuntime` (with its arena) is created on first
   write. Non-default runtimes allocate `branch_memtable_size` (new small option); when a rotation
   is forced by `ArenaFull`, the replacement arena is sized `max(branch_memtable_size,
   batch_estimate)` so a batch larger than the branch arena cannot permanently fail post-WAL.
   Invariant 8 ("an idle branch allocates no arena") is satisfied without touching the allocator.
2. **Branch memtable rotation no longer rotates the shared WAL.** `rotate_memtable` currently
   rotates the WAL on every memtable swap (`lsm.rs`); with dependency-derived replay floors this
   coupling is unnecessary and would multiply WAL churn by branch count. The WAL rotates on its own
   size policy at append time; memtable rotation is per-runtime and records dependencies on the
   segment it actually appended to.
3. **Registry reads use a `RwLock<HashMap>` in this slice**, not a lock-free structure; the routed
   lookup is one read-lock + hash on a path already taking several locks. Lock-free (A9) remains a
   recorded optimization gated on probe evidence, strata-style.

Scope: registry + routed apply + lazy runtimes + right-sized rotation + database-wide write-buffer
budget (`Option<u64>`, `None` = today's behavior) + owner-scoped stall counts. Reads remain
default-branch (BR5); recovery demux already lands per-branch memtables (P3) and BR6 completes it.

## Superseded section (pre-BR3 gate record)

The BR3 gate findings recorded before implementation:

## Superseded section (pre-BR3 gate record)

The BR3 gate findings recorded before implementation:

Pre-implementation plan re-check performed against the tree; the BR3 spec holds:

- `LevelManifest.levels` is a single un-owned `Levels(Vec<Arc<Level>>)`; no owner validation exists
  anywhere on manifest load, so fail-closed owner/reference checking is new, needed work.
- `manifest_format_version` (V1) gives the clean format-evolution path; the owner-partitioned V2
  rejects V1 by identity (no on-disk compatibility on this line).
- Table-ID uniqueness is a single manifest counter and survives partitioning unchanged; the
  id-keyed `hidden_set` also works unchanged across owners.
- Single-level-set consumers are a small surface: `compactor.rs:242`, `leveled.rs:379`,
  `leveled.rs:430`, and the flush changeset (`lsm.rs:273`, currently owner-blind).
- L1+ non-overlap is enforced inside `Levels`, so per-owner level sets make "non-overlap inside one
  owner only" structural.

Written amendment adopted for the implementation: the partitioned `Levels` exposes
`levels_for(owner)` as the only access path — no global level-iteration API survives — so BR5's
"no global scan then owner filter" gate is guaranteed by construction; manifest changeset entries
carry an owner that is validated against the persisted SST `table.meta.owner`, and mismatches fail
closed at apply and at open. The BR2 `CoreInner` deref compatibility shrinks in this slice: level
access through the runtime resolves to the default owner's set.
