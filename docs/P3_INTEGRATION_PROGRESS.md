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

### FK6 — Multi-anchor retention pins: complete (2026-08-14)

As built:

- **`RetentionAnchors`** (`branch.rs`) — the sorted, deduplicated set of caps one owner must stay
  exactly readable at, with `AnchorWalker` serving them in a single pass over a key's versions.
  The walker is the whole fix in five lines: anchors descend, versions arrive newest-first, so one
  shared index decides "is this version the newest at or below an anchor nothing has answered yet".
- **`BranchCatalog::retention_anchors(owner, generation)`** — every live child's `fork_seq` plus
  every merge edge's `target_through_seq` whose source is still live at that generation. A deleted
  source releases both of its anchors, so branch churn still reclaims.
- **`CompactionIterator`** takes the set instead of a floor. Non-versioned: the newest version at
  or below *each* anchor. Versioned: the range below the *highest* anchor, which is what "forks
  inherit full history ≤ F" means once F is not unique.
- **`RetentionAnchors::view_is_complete_at(cap, floor)`** — one predicate, used by the fork check
  and the merge check, replacing a pin-blind `cap < floor` comparison in each.
- **`CompactionPinRaced { unsampled_anchor }`** — was `{ sampled_floor, current_floor }`. The
  question generalised from "did the floor drop" to "does the catalog hold an anchor this job never
  sampled"; an anchor that vanished still means the job merely over-retained.
- `min_active_child_anchor` survives, wired to its one real caller: the delete guard's "does
  anything still fork off this branch".

**Both gate defects are now permanent tests, and both were probed.**

Defect 1 (`two_children_at_different_anchors_each_read_their_own_view`) is backed by a unit
sabotage: `fk6_every_anchor_keeps_the_version_that_answers_for_it` runs the same versions through
the production iterator with each of the two single-anchor sets the design used to have, and
records that `min` (what shipped) loses the deep child's version and `max` loses the shallow
child's. Neither single pin is a fix for the other.

Defect 2's probe is worth recording in full, because it took two steps. Disabling the merge-edge
anchor alone did NOT expose the overwrite — it reddened on `BelowRetentionFloor` instead, because
the completeness guard caught the missing anchor first. Disabling **both** produced the real
failure: `preview_merge_into` reports **zero conflicts** where it must report one, meaning the
merge applies straight over the target's deliberate revert. Two layers, and the outer one is doing
real work; that is why the guard stays even though a live source always pins the cap its merge
reads at.

Plan-vs-reality deltas:

1. **The gate's "the anchor set is capped" is wrong and the code does not cap it.** A cap that
   truncates would silently drop a promise, which is the defect being fixed wearing a hat. The
   count needs no separate cap: it is bounded by the catalog's own 4096-entry limit at two anchors
   per entry.
2. **`retained_floor` is now often 0 where it used to advance.** The first version of defect 1's
   test asserted `retained_floor > 0` as its non-vacuity fact — copied from the probe, where the
   floor rose *because* the pin had failed. With the pin working the fixture drops nothing at all.
   The honest non-vacuity fact is that a compaction happened: L0 shrank and L1 grew.
3. **`fork_below_the_retention_floor_is_refused` was passing for a fixture reason.** Its control
   arm forked at the very point it later expected to be refused, and that live child now pins the
   point — so the second fork is legitimately servable. The control is deleted before the
   compaction, and a new arm pins the new behaviour: **a fork at a live anchor is servable however
   low the floor is**, which is the ordinary "fork two branches off the same commit" case that the
   old floor check refused.
4. **`a_base_below_the_target_s_retention_floor_is_refused` never observed its own refusal** — it
   asserted the floor moved and that an unrelated fresh fork still planned. Renamed to
   `detaching_a_source_releases_the_parent_s_pin`, which is what it proves, with the reachability
   of the guard written down rather than implied.
5. **No format change**, so no golden vectors were regenerated: anchors derive from the catalog,
   which is already durable, and `retained_floor_seq` keeps its meaning.

Carried to PD3: the merge guard is reachable through one narrow window — between a merge's data
commit and its edge record, a compaction can sample anchors without the new one and drop what it
will promise. The refusal is the right end for that race, and PD3's durable merge intent closes it
properly by pinning the base before the commit rather than after.

Verification: 1,092 lib tests green (1,078 → 1,092), 4 doc tests, clippy `--all-targets
--all-features` clean, fmt clean. Fourteen new tests: seven unit arms on the anchor set and its
walker, three on the compaction iterator (two-anchor retention with both single-anchor sabotage
arms, one-version-per-anchor, the bottom-level delete against an anchor set), two integration arms
for defect 1 (two children, and a 64-child parent where every child still reads its own anchor),
and two for defect 2 (a merge surviving the parent's compaction, and the reverted-key conflict).

### PD3a — The bounded merge: complete (2026-08-14)

As built:

- **`MergeSession`** (`merge.rs`) — one merge, holding everything it is judged against: one source
  diff snapshot, one pair of target snapshots, one planning sequence, and one `ActiveTxnGuard` at
  that sequence. Both passes read the fixed view, so a write arriving mid-merge cannot make pass two
  disagree with pass one about what conflicts.
- **`Transaction::new_owned_at(core, opts, owner, start_seq)`** — a transaction whose conflict
  window opens at a stated sequence instead of at "now". `new_owned` is now a one-line call into it.
  This is F1's fix, and it is the whole of it.
- **`preflight`** — pass one. Classifies everything, buffers nothing, counts writes / converged /
  conflicts, measures total and largest-entry bytes, and keeps conflicts up to
  `CONFLICT_REPORT_LIMIT` (1024) for the refusal. Under `Strict` it stops measuring once a conflict
  is seen, because nothing will be written.
- **`apply`** — pass two. Re-streams the same view, fills a chunk to `max_memtable_size` bytes,
  commits, repeats. Returns the target head and the chunk count.
- **`MergeOutcome.chunks`** — new public field. One means the merge landed atomically; more means
  each chunk is durable on its own. A caller cannot otherwise tell whether the operation it just ran
  was all-or-nothing, and after this slice that is no longer a constant.
- **`record_merge_edge`** holds the level manifest for reading across the catalog publish (F5), so
  no compaction can publish between the last chunk and the edge that becomes its retention anchor.

Verified by probe, both ways:

- Removing the shared `start_seq` (chunks built with `new_owned` instead of `new_owned_at`) turns
  `a_target_write_between_planning_and_writing_conflicts` from a conflict into a **success**:
  `apply` returns `(4, 1)` and the concurrent write is overwritten. That is F1's defect, reproduced.
- `outcome.chunks > 1` is the non-vacuity fact for the chunking test — without it, "40 keys landed"
  is equally true of a single batch, and the test would pass with chunking deleted.

Plan-vs-reality deltas:

1. **`MergeTooLarge` changed meaning rather than dying.** The plan wanted a size preflight raising
   it; chunking makes the merge-wide version unreachable. It now names a *single entry* larger than
   the whole budget, which is the one size chunking genuinely cannot fix, and it is raised in pass
   one before anything is written. `an_oversized_merge_is_refused_before_writing` became
   `a_merge_past_the_chunk_budget_completes_in_chunks` — same fixture, opposite expectation,
   because the behaviour it asserted is the behaviour this slice removes.
2. **`Transaction::write_set_size_estimate` is deleted.** Its only caller was `commit_merge`, which
   measured after materializing the whole write set — the thing F3 says is too late. Pass one
   measures from the diff entries instead, so the method had no consumer left.
3. **The durable cursor is NOT here** and the gate argues it is not the correctness mechanism the
   plan implies: a crashed chunked merge, re-run, classifies every already-applied key as Converged
   and reaches the same final state. PD3b builds the intent for the three things it *is* good for —
   naming the half-merged state, refusing a conflicting second merge, and skipping the re-scan.
4. **Two passes, uniformly, including for small merges.** A "buffer until it overflows, then fall
   back" shape would save a pass on the common case at the cost of two code paths through the most
   dangerous operation in the store. The extra pass over a small diff is microseconds; PD3c's scan
   probe makes it cheaper still.
5. **`merge_session` is `pub(crate)`, not a test hook.** `merge_into` goes through it. The window in
   F1 is only deterministic if a test can stand between planning and writing, and the honest way to
   allow that is for the two halves to be genuinely separate in production, which they now are.

Verification: 1,095 lib tests green (1,092 → 1,095), 4 doc tests, clippy `--all-targets
--all-features` clean, fmt clean. Four tests replace one: chunked completion with a re-merge proving
the edge covers the whole extent, one-chunk atomicity, the unchunkable single entry with nothing
written, and the planning-window conflict.

### DEFECT — a store whose L1 key order disagreed with its sequence order would not reopen (2026-08-15)

**Found by the property suite, generating a case rather than re-running a planted one.** This is the
fifth production defect the coverage slices have found, and the first the randomized testing found on
its own. `proptest` shrank it to
`[Churn { key: 2 }, Compact, Churn { key: 0 }, Compact, Reopen]`; the seed is persisted in
`proptest-regressions/test/branch_property_tests.txt`.

**The defect.** `LevelManifest::hydrate` validated every level below L0 with
`validate_table_sequence_numbers`, which required adjacent tables to satisfy
`next.smallest_seq_num > current.largest_seq_num` — ascending, disjoint *sequence* ranges.

Levels below L0 are ordered by **key**. `Level::find_first_overlapping_table` and
`find_last_overlapping_table` read them with `slice::partition_point`, a binary search that is only
correct on a key-sorted vector, and `hydrate` pushes tables in manifest order and never sorts them.
So the loader demanded one ordering of a vector every reader requires to be in another. Key order
and sequence order are independent, and the guard was `level_idx > 0` — the exact inverse of the
level where a sequence-disjointness argument could even be made.

**The consequence.** Write a high key, compact it to L1, write a low key, compact it to L1, close,
reopen: `LoadManifestFail("Level 1 tables have overlapping sequence numbers: Table 1031
(Some(6)-Some(6)) and Table 1027 (Some(3)-Some(3))")`. Note the two ranges do not overlap — they are
disjoint and *descending*, which is the tell that the check was reporting an ordering violation as
an overlap. **The store never opens again.** No branches are involved; this is the plain single-owner
write path.

**The fix.** `validate_level_tables` keeps the per-table sanity checks (both sequence numbers
present, `smallest <= largest`) and replaces the cross-table sequence check with the invariant the
readers actually depend on: adjacent tables must be in ascending key order. Nothing validated that
before, so a manifest that would have broken the binary search loaded silently — a wrong answer
returned quietly rather than an error.

**Verification.**

- `a_store_reopens_when_l1_key_order_and_sequence_order_disagree` (`src/test/lsm_tests.rs`) is the
  defect reduced to the plain public API, with no branching. It fails on the old check with the
  identical error, table ids and sequence numbers as the generated case.
- **A first reduction was too small and passed.** Two keys and two compactions merge into a *single*
  L1 table, and the check is skipped for `tables.len() <= 1`. The reproducer needs the property
  harness's configuration — `level_count: 2`, `level0_max_files: 2` — and three writes per key, so
  that each key becomes its own L1 table. Recorded because the near-miss is the interesting part: a
  reduction that passes is not proof the diagnosis is wrong.
- **Sabotage twin.** `reverse_l1_table_order` decodes the newest `BranchStateManifest`, reverses
  L1's table ids, and re-encodes — so only the level's key ordering is wrong and every other
  invariant is intact. The reopen must be refused with the new message. Without this arm the test
  would only prove the loader accepts things.
- **Sensitivity probe.** Neutering the new condition makes the sabotage arm fail with "a level whose
  tables are out of key order must be refused". (A first probe using `windows(0)` was invalid — it
  panicked inside the probe itself, so the test failed for the wrong reason. Recorded because a
  probe that reddens for the wrong reason is not a probe.)

**What this says about the property suite.** The 1,129-test hand-written suite never caught this
because its tests either put one table in a level or wrote keys in ascending order, so key order and
sequence order coincided. Nothing was wrong with those tests; the shape simply never occurred to
anyone writing them by hand. That is the argument for V6 in one defect.

### V8b-1 — The two test-layer defects: complete (2026-08-15)

**`PIN_EXERCISED` is gone; `run_history` returns a `HistoryOutcome`.** The static was incremented by
the seeded retention test *and* by all 48 generated cases, so the seeded test's
`assert!(PIN_EXERCISED > 0)` — added specifically to stop that test silently ceasing to test the
retention promise — could be satisfied by somebody else's history. It was disarmed from the day it
was written. The seeded test now asserts on its own return value, and the message says so: "THIS
history must make a compaction retain a version for a fork anchor".

**`branch_exists` is one function.** It was defined in `fault_injection_tests.rs` (via
`list_branches`) and in `fork_view_tests.rs` (via `core.inner.branch_catalog.get_by_name`).

Delta: **the plan said these "answer different questions"; they do not.** `BranchCatalog::list` and
`get_by_name` are both driven by the same `live_names` index, and `delete` removes from it, so both
forms answer "is there a live branch with this name" and every existing assertion means what it
appears to mean. The hazard was real but different: two implementations under one name, at two
different layers, that nothing held together — a test moved between the files would have changed
which layer it exercised without changing a line. The `list_branches` form was also the worse of the
two in the file that used it, since it builds a `BranchInfo` for *every* branch (walking the level
manifest per branch), so in a fault-injection test an unrelated failure turns a clean `false` into a
panic from `.unwrap()`.

The single definition lives in the new `src/test/support/` and goes through `Tree::branch(name)`:
public API, O(1), no panic path, and identical semantics to both — so no assertion changed meaning,
which the "stop if" for this slice required.

Verification: **1,137 lib tests**, clippy `--all-targets --all-features` clean, fmt clean. The
ambient-state guard failed once during this slice, correctly: deleting `PIN_EXERCISED` left its
allowlist entry stale, which is the both-directions check earning its place on its first outing.

### V8a — The injection seam: in progress (2026-08-15)

**V8a-1 — the fault policy becomes a dependency.** V3 shipped its failpoints as a `#[cfg(test)]`
`thread_local!` registry keyed by `&'static str` and reached through a `failpoint!` macro. It is now
`Options.fault_policy: Arc<dyn FaultPolicy>`, defaulted in `Default::default()`, mirroring
`clock: Arc<dyn LogicalClock>` exactly. `FaultPoint` is a closed enum the engine matches
exhaustively; `NoFaults` is the zero-sized production default; `ScriptedFaults` is `#[cfg(test)]`
and holds its script **in the instance**, which is why the global version needed a thread-local and
this one does not. `LevelManifest` stores the policy as a field — `fresh`/`hydrate` already take
`Arc<Options>`, so `persist_owner_update` and `persist_root` reach it with no signature changes.
`TreeBuilder::with_fault_policy` is the test seam. Production and tests now compile the same call
sites; only the injected value differs.

**V8a-2 — the block cache's `#[cfg(test)]` counters are deleted.** `src/cache.rs` carried six
`AtomicU64` fields, three `#[cfg(test)]` blocks in the lookup path, `get_stats`, `reset_stats` and
`CacheStats` — about twenty conditional sites on the hottest read path in the engine, serving four
assertions in one test. It was the sharpest violation of principle 4 in the crate: the cache under
test had a different struct size and six extra atomic read-modify-writes per lookup than the one
that ships. All of it is gone; `BlockCache` is one object in every build.

Plan-vs-reality deltas:

1. **The replacement is stronger than the assertions it replaces, and lives at a different level.**
   The plan said "rewrite the 4 assertions to check observable behaviour — a second read of a cached
   block does not re-read the file". That is not observable through `Tree`: table readers hold an
   open `Arc<dyn File>`, so nothing at the tree level can tell a cache hit from a file read. It *is*
   observable one level down, because `crate::vfs::File` is already a trait. `src/test/cache_tests.rs`
   introduces `SealableFile`, an ordinary `File` implementation that fails every `read_at` once
   sealed. The table under test therefore runs exactly the shipped code path and only the file
   beneath it differs — principle 4's allowed case, a test *double*, not a `#[cfg(test)]` branch
   inside the subject.
2. **Non-vacuity is asserted inside the test, not only by a probe.** After sealing, the same lookup
   still succeeds (the cache served it) **and** a lookup into a data block this table has never read
   fails with the sealed-file error. The second assertion is what stops the first from being vacuous
   if the seal ever stops biting. A separate control arm seals before any lookup and requires the
   read to fail, pinning the fact that uncached reads genuinely reach the file.
3. **The history block cache is covered too.** It had its own pair of counters
   (`data_history_hits`/`misses`) and is keyed by a distinct kind, so it gets its own test: a full
   scan under `TimestampComparator`, sealed, re-scanned, asserted identical.
4. **The tree-level test survives, minus the counters, and gains a second arm.** What it can still
   prove end-to-end is that a scan across many SSTables returns identical results warm and cold
   (`repeated_range_queries_over_many_ssts_return_identical_results`). Added alongside it:
   `a_cache_too_small_to_hold_the_working_set_returns_the_same_results`, which runs the same workload
   under a 4 KiB cache and a 10 MiB one and requires the results to match. **Nothing covered eviction
   before** — the deleted counters measured hits, never whether an evicted block was re-read
   correctly.
5. **It stayed in `snapshot_tests.rs` rather than moving.** Moving it would have needed an eighth
   copy of `create_temp_directory`; V8b consolidates those into `src/test/support/` and can move it
   then. The new table-level tests need no temp directory, so they live in the new file.

**Sensitivity probe.** Making all three `BlockCache` getters return `None` (cache disabled) reddens
`a_block_already_in_the_cache_is_served_without_reading_the_file` and
`the_history_block_cache_serves_a_repeat_scan_without_reading_the_file` on exactly the intended
assertion — the sealed-read error — and leaves the control arm
`a_block_not_in_the_cache_is_read_from_the_file` green, which is correct: that test asserts the
*miss* path. Probe reverted.

Verification after V8a-2: **1,133 lib tests** (1,129 → 1,133: three table-level cache tests, plus
the eviction arm, less the one test replaced), 4 doc tests, clippy `--all-targets --all-features`
clean, fmt clean.

**V8a-3 — the last ambient dependency, and a gate finding that struck half the slice.**

The env read is gone. `SURREALKV_MAX_CONCURRENT_COMMITS` (`src/commit.rs`) was the only
`std::env::var` in the crate, and it set the commit pipeline's semaphore permits — so every store in
a process silently inherited one ambient value that changes concurrency, and therefore
reproducibility. It is now `Options::max_concurrent_commits` with
`TreeBuilder::with_max_concurrent_commits`, defaulted to `DEFAULT_MAX_CONCURRENT_COMMITS` (7, the v2
baseline). `CommitPipeline::new` takes it as a parameter — thirteen call sites, twelve of them in
`commit.rs`'s own unit tests; a missed one cannot compile, which is why a parameter was preferred to
a second constructor.

`SSTableError::FailedToGetSystemTime` is deleted: never constructed, a vestige of the
`SystemTime::now()` call removed from the SST writer.

Deltas:

1. **The plan's "five `#[cfg(test)]` methods with zero call sites" is wrong, and the deletion is
   struck.** Measured against the current tree, *every one* has live callers:
   `BranchCatalog::default_branch` (`src/branch.rs:736`), `CommitPipeline::oracle` (five sites
   across `commit.rs` and `src/test/oracle_tests.rs`), `allocated_seq_high_water`
   (`src/commit.rs:1322`), and `BackgroundErrorHandler`'s `get_error` / `is_db_stopped` /
   `error_count` / `clear_error` (eleven sites in `error.rs`'s own test module — the plan also
   miscounted these as three items, not four). They are test-only accessors on production types
   with tests that legitimately need them, which is exactly principle 4's *allowed* case. Deleting
   them would have deleted the assertions with them. The standing method's "stop if any deleted item
   turns out to have a live consumer" applied, so they stay.
2. **The concern behind that entry is still real, and moves to `docs/KNOWN_GAPS.md`.** V1's guard
   forbids `allow(dead_code)`, so genuinely dead code can satisfy it by hiding behind `#[cfg(test)]`
   instead. That loophole exists; these five simply are not instances of it. A static check cannot
   reasonably prove a `cfg(test)` item is uncalled, so it is recorded as a known limit of the guard
   rather than papered over.
3. **The clamp moved to the pipeline, and it is load-bearing.** The env version clamped at the read
   site; the option is clamped inside `CommitPipeline::new`, so no construction path — builder,
   default, or a future caller — can hand the ring more in-flight commits than it has slots. The
   probe below shows this is not defensive decoration: without it, `Semaphore::new(usize::MAX)`
   panics inside tokio at store construction.
4. **A compile-time invariant that was a comment became a check.** `COMMIT_QUEUE_SIZE` carried
   "Must be a power of two" with nothing enforcing it, while the queue indexes slots with
   `head & (COMMIT_QUEUE_SIZE - 1)`. It is now `const _: () = assert!(…is_power_of_two())`. This
   replaced a runtime assertion in the new test that clippy correctly flagged as constant — and it
   *was* vacuous, since the ceiling is defined as `COMMIT_QUEUE_SIZE - 1`; the power-of-two fact is
   the one a reader cannot see from the literal.

**Sensitivity probes.** (a) Replacing the clamp with `.max(1)` reddens
`the_commit_limit_is_honoured_and_clamped_to_the_ring` — by panicking inside tokio's semaphore,
which is the failure the clamp exists to prevent. (b) Hard-coding the default at the `lsm.rs`
construction site instead of reading `opts.max_concurrent_commits` reddens
`each_store_gets_its_own_commit_concurrency_limit` with `left: 7, right: 3`. Both reverted.

Verification after V8a-3: **1,135 lib tests**, 4 doc tests, clippy `--all-targets --all-features`
clean, fmt clean.

**V8a-4 — the allowlist becomes machine-checked, and the gaps get a home.**

`no_ambient_state_outside_the_written_allowlist` (`src/test/architecture_guard_tests.rs`) walks
`src/` and fails on: a `static` item declaration not named in `AMBIENT_STATE_ALLOWLIST`, any
`thread_local!` / `lazy_static!` / `once_cell::`, any `env::var`, any `SystemTime::now()` outside
`src/clock.rs`, and any `rand::rng()` / `thread_rng()` outside `src/memtable/skiplist.rs`. The
allowlist is keyed `file:NAME` and each entry carries its reason inline, so moving a global to
another file forces a fresh decision rather than inheriting one.

Deltas:

1. **The allowlist is checked in both directions.** The plan asked for the shape of the dead-code
   guard, which only catches *unlisted* offenders. This one also fails when a listed global no
   longer exists, so a reason cannot outlive the code it describes. That turns the allowlist into a
   ratchet: V8b-1's deletion of `PIN_EXERCISED` will fail this test until its entry is removed too,
   which is the intended behaviour.
2. **The measurement moved.** The plan said "six statics with interior mutability, one env read, one
   ambient RNG". Measured now: **seven** statics (the plan's six plus
   `src/test/iterator_tests.rs:TEST_TABLE_ID_COUNTER`, which the earlier sweep missed), **zero** env
   reads (V8a-3 removed the only one), **zero** `thread_local!` (V8a-1 removed the only one), one
   ambient RNG, and one `SystemTime::now()` — in `src/clock.rs`, which is the clock adapter itself.
   The two other apparent `SystemTime::now()` hits are comments explaining why the call is *not*
   there, which is why the detector skips comment lines.
3. **`docs/KNOWN_GAPS.md` requires a trigger per entry.** Each gap states what it is, why it was
   left, what it would take, and **what would raise its priority** — because an entry with no
   trigger is not a decision. Six entries: `Tree::flush`, `ForkFenceTimeout`, the skiplist RNG,
   `BRANCH_ID_COUNTER`, `vfs::sync_tracker`, and (new, from V8a-3's gate) the `#[cfg(test)]`
   loophole in the dead-code guard — carrying the warning that the plan's "five dead methods" claim
   was wrong, so nobody deletes on that basis again. A "not gaps" section records the decisions that
   should not be re-litigated.
4. **The `Tree::flush` entry is written at length, on request.** It is the longest entry because it
   is the largest divergence: 206 test call sites reaching durability through a `#[cfg(test)]`
   synchronous path production never runs, so anything that only breaks *because a flush is
   concurrent with something else* is invisible to all of them — and V4's defect was found in
   exactly that territory, by a test that did not go through `Tree::flush`. It carries a four-step
   migration sketch and the warning that the barrier must be the production path plus a completion
   signal, or it becomes a second `Tree::flush` with extra steps.

**Sensitivity probes.** All four detector arms were planted and fired: an unlisted
`static PROBE_GLOBAL` in `src/stall.rs` (`unlisted global 'PROBE_GLOBAL'`), a fabricated allowlist
entry for a global that does not exist (`allowlist entries name globals that no longer exist`), an
`env::var` read, and a `SystemTime::now()` outside the clock adapter. All reverted;
`git diff src/stall.rs` is empty.

**V8a exit gate met.** No `static`/`thread_local!` state in `src/` outside the written allowlist, no
`SystemTime::now()` outside `src/clock.rs`, no env reads, the 7 fault-injection tests and the crash
lane green against the injected policy. **1,136 lib tests**, 4 doc tests, clippy `--all-targets
--all-features` clean, fmt clean.

### REGRESSION introduced and fixed in V7/V8a — every SST lost its final data block (2026-08-15)

**Committed in `8fa5387` ("p1"). Fixed in the working tree. Read this before trusting anything
written by that commit.**

While replacing `SystemTime::now()` with the injected clock in `TableWriter::finish`
(`src/sstable/table.rs`), a scripted string replacement computed its end offset by searching
forward for `"?;\n"` and then to the next line. That offset ran past the statement it was meant to
replace and swallowed **two more**:

```rust
self.meta.properties.seqnos =
    (self.meta.smallest_seq_num.unwrap_or(0), self.meta.largest_seq_num.unwrap_or(0));

// Flush last data block if it has entries
if self.data_block.as_ref().is_some_and(|db| db.entries() > 0) {
    let key_past_last = self.internal_cmp.successor(&self.data_block.as_ref().unwrap().last_key);
    self.write_data_block(&key_past_last)?;
}
```

The consequences, in order of severity:

1. **Every SST written lost its last data block.** For a small table that is the *only* data block,
   so the table's index has no entries at all and any lookup through it fails
   `EmptyCorruptPartitionedIndex`. This is silent data loss at write time, not a read bug.
2. **`properties.seqnos` was left at its default**, so per-SST sequence ranges were wrong — which is
   precisely the metadata PD3c's scan-mode probe and PC's seq-filtered scan use to skip tables.

**How it survived.** The edit compiled, and it left a stray `}` that I "fixed" by deleting the
brace — treating a symptom of the real damage as a formatting slip. I then made two further edits
and **never re-ran the suite** before the session moved on; the last green run (1,129) predated the
change. The commit was taken on top of that stale confidence.

**How it was caught.** V8a's first `cargo test` after unbreaking the failpoint refactor showed one
failure, `a_failed_publish_leaves_a_detached_branch_reading_through_its_parent`, whose *injected
fault never fired* — detach was failing earlier, for an unrelated reason. Chasing "why did the
fault not fire" rather than accepting the red test is what led to it. Confirmed by removing the
fault entirely: detach still failed, so the fault mechanism was never involved.

**What this changes about how edits are made.** Offset-computed replacements over source are not
acceptable for anything but an exact, whole-statement literal match. Where a replacement cannot be
expressed as one, edit the file directly and read the result back. And a build that compiles is not
a verification — the suite must run before the work is called done, and certainly before a commit.

### V6 — Property tests against a branch-semantics model: complete (2026-08-15)

`proptest` had been a declared dev-dependency with **zero uses** since it was added. This slice uses
it: generated histories of fork / write / delete / merge / flush / compact / churn / reopen, with
every live branch's view of every key checked against a model after **every** operation.

`VisibilityModel` is ~90 lines and answers exactly one question — "what should this branch see for
this key?" — by walking the ancestor chain with a running-minimum cap. It knows nothing about SSTs,
memtables, compaction, WAL or the authority. The module docs carry the test for whether it has
drifted back into the shape PA2 deleted: **if it ever needs to know about a flush, a level, a table,
or a sequence that is not a fork anchor or a commit cap, it has.** Merge is modelled by applying the
keys the engine *reported* writing, deliberately — restating the decision table in the model would
only prove it was written twice.

**The non-vacuity check failed three times before it passed, and each failure was informative.**

1. **First attempt: the re-planted FK6 defect was not caught.** Collapsing `retention_anchors` to
   its single lowest anchor — the pre-FK6 behaviour — left the whole suite green.
2. **A coverage counter said why: zero of 48 histories ever exercised the retention pin.** Random
   operations almost never build the shape a compaction needs in order to *drop* a version, so a
   suite that claimed to test a retention promise was not reaching that code at all. Fixed by
   configuring the store so compaction actually triggers (`level0_max_files: 2`, `level_count: 2`)
   and adding an `Op::Churn` that overwrites one key several times with flushes between. Biasing a
   generator toward the interesting region is the point of writing one.
3. **Still not caught, and a seeded history explained it.** The pin was now being exercised, but the
   seeded history had **one** child — and with one child the lowest anchor IS the only anchor, so a
   single point pin is accidentally correct. Two children at different anchors is the entire
   defect. With that, the re-planted defect fails immediately and legibly:
   `branch b2 key "a": engine says Some("1"), the specification says Some("2")` — the bug in one
   line.

That seeded history ships as `a_seeded_history_reaches_the_retention_pin_and_agrees_with_the_
specification`, and it asserts the pin was reached, so it cannot quietly stop testing what it is
for.

**Recorded honestly:** the random generator still does not reliably reach the retention region in 48
cases; the seeded history is what covers it. Both belong, and pretending the generator covers it
would be exactly the kind of claim this project's probes exist to prevent.

Four hand-checked model tests as well — a specification nobody has verified is just a second
opinion: an inherited view capped at the anchor, each anchor getting its own answer, a chain
narrowing monotonically, and absence treated as a value.

Verification: 1,126 lib tests green (1,119 → 1,126), 4 doc tests, clippy `--all-targets
--all-features` clean, fmt clean.

### V5 — The concurrency matrix: complete (2026-08-15)

Before this slice, **no branch operation appeared inside a spawned task or thread anywhere in the
repository**, and no branch test used a multi-threaded runtime. `Error::CompactionPinRaced` exists
solely to describe a race and was tested by hand-assembling the state that race would leave.

Six tests in `src/test/branch_concurrency_tests.rs`, all on
`#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`, each asserting an **invariant rather
than a schedule**: concurrent forks off one parent (unique generations, all resolving), fork ∥
compaction, delete ∥ write, merge ∥ merge into one target, merge ∥ target writes, and the
maintenance sweep ∥ branch churn. Run ten times consecutively without a flake.

**It found a deadlock — the most serious defect of this whole stretch.**

`Core::fork_branch` holds a `level_manifest.read()` guard across its catalog publish (FK6's floor
check), and `publish_catalog_locked` then took `level_manifest.read()` **again on the same thread**
to mirror the catalog version. `std::sync::RwLock` is not reentrant, and a queued writer blocks new
readers — so as soon as a compaction was waiting for the write lock, the fork blocked on itself.
Forever. `BranchHandle::record_merge_edge` has the same shape for the same reason (FK6's window
closure), so **a merge could deadlock the same way**.

Neither was reachable before this slice, because nothing had ever run a branch operation
concurrently with a compaction. The probe is unambiguous: restore the recursive read and
`forking_while_the_parent_compacts_never_costs_the_child_its_view` hangs indefinitely instead of
finishing in 1.5 seconds.

Fixed by giving `CoreInner` its own clone of the `Arc<AtomicU64>` the manifest already holds, so the
publish path records the catalog version without acquiring that lock at all. The two deliberate
read-guard holds stay exactly as FK6 designed them — they are what excludes a concurrent compaction
publish — and the second acquisition simply no longer exists.

Plan-vs-reality deltas:

1. **`merge_into`'s future is not `Send`**, so a merge cannot be `tokio::spawn`ed. It holds a
   `DiffIter` and a `&mut dyn TargetProbe` across await points, and making it `Send` would require
   `LSMIterator: Send` — a deep change well outside this slice. The tests drive merges through
   `spawn_blocking` + `Handle::block_on`, which is exactly what a caller has to do, and the
   constraint is now recorded here rather than discovered by the first user who tries. Worth
   revisiting when the async work lands, since it is the same layer.
2. A `Send + Sync` assertion for `Tree` and `BranchHandle` was added to the architecture guards
   first. Nothing had ever shared a `Tree` across threads, so the alternative was discovering the
   answer inside a race.
3. The fork ∥ compaction test accepts `CompactionPinRaced` as a legal outcome rather than a
   failure — that error means the mechanism worked. What it asserts instead is the child's read,
   which is the property; asserting which side won would be asserting the scheduler.

Verification: 1,119 lib tests green (1,112 → 1,119), 4 doc tests, clippy `--all-targets
--all-features` clean, fmt clean, concurrency lane green over 10 consecutive runs.

### V4 — The crash and reopen matrix: complete (2026-08-15)

Seven tests in `src/test/branch_crash_tests.rs`, closing the holes the exploration measured: a
deleted branch's tombstone reloaded (and its name re-created, against a fresh generation), a TTL
surviving a restart and firing on the first sweep after it, **retention floors and fork anchors
across a restart** with the pinned version still readable, `AtVersion`/`AtTimestamp` resolving
identically after a reopen, an interrupted reclamation finishing after a restart, a power-loss test
that loses an unsynced *branch-owned* SST, and metrics gauges re-derived while counters reset.

**It found a second production bug, worse than V3's.**

`close()` failed outright after the ordinary sequence *write on a branch → delete the branch →
close*: `"Failed to flush memtables during shutdown: Branch generation is stale or deleted"`. The
deleted branch's dirty memtable is still queued, shutdown tries to flush it, and PB1's liveness
guard correctly refuses — but the refusal propagated as a hard error.

The shutdown failure is the visible symptom; the real exposure is that **this is the one flush loop
every branch shares**. A single deleted branch holding a dirty memtable would fail that loop on
every cycle, so flushing stalls for the whole store, not just for the branch that was deleted.

Fixed in `flush_oldest_immutable_for_runtime`: `Err(Error::BranchFenced)` now discards the entry and
releases its WAL dependency instead of propagating. Discarding is correct rather than merely
convenient — the owner has no level set to publish into and the reclamation sweep is about to drop
everything it owns, so there is nowhere for those rows to go. `flush_oldest_immutable_to_sst` also
had to learn to drain a runtime rather than take one entry, because an entry that produces no table
(empty or fenced) previously stopped the sweep with work still queued behind it.

Probed: restoring the propagation reproduces the exact shutdown failure.

Plan-vs-reality delta: the plan listed "interrupted detach reloaded from disk" as its own arm. V3's
failpoint test already covers the in-process half, and the reload half is subsumed by
`an_interrupted_reclamation_finishes_after_a_restart`, which exercises the same shape — an
operation interrupted between two publishes, then reloaded. A separate detach reload would assert
the same property twice.

Verification: 1,112 lib tests green (1,105 → 1,112), clippy `--all-targets --all-features` clean,
fmt clean.

### V3 — Failpoints and the branch fault-injection lane: complete (2026-08-15)

This is the coverage PE2 was blocked on. PE2 needed `SimObjectStore`'s fault points, which the
engine never called; V3 injects at the engine's own durable steps instead, and needs no seam.

As built: `src/failpoints.rs` (`#[cfg(test)]`) plus a `failpoint!` macro that expands to **nothing**
in a published build — no registry, no lookup, no branch. Three points, at
`publish_catalog_locked`, `persist_owner_update` and `persist_root`. Thread-local and RAII-disarmed,
so a panicking test cannot leak a fault into the next one and a background flush cannot be surprised
by another test's injection. The cost, stated in the module docs: faults cannot be injected into
background work this way — irrelevant here, because every branch state machine publishes on the
caller's thread.

**It found a production bug on its first run.** `reclaim_tombstoned_branches` removed a deleted
branch's level set, state version and retention floor from the in-memory manifest and *then*
published a root without it. That order is forced — the root's contents derive from the
state-version map, so it cannot describe an owner's absence until the owner is absent — which means
a failed publish left memory ahead of disk. Unlike every other publish path in this engine, it did
not roll back. The consequence: the next sweep finds nothing to reclaim while the files sit on disk
referenced by a root that was never replaced, so **the tables leak until the next process start** —
exactly the leak PB1 existed to fix, returning on the failure path.

Fixed with `LevelManifest::{reclaim_owner -> Option<ReclaimedOwner>, restore_owner}`, matching the
rollback discipline of `revert_changeset` and the catalog's `*catalog = snapshot`. Probed: with the
`restore_owner` call removed, the test reddens on "the level set was removed in memory without being
published" (`left: 0, right: 1`).

Seven tests, each pairing an injected failure with a control arm that must succeed — without the
control, a test could pass because the operation was refused for an unrelated reason:
fork, delete, detach, TTL expiry, the merge promotion edge, reclamation, and the registry's own
behaviour (one-shot fires once, always-armed keeps firing, the guard disarms on drop). That last one
exists so the control arms cannot be silently vacuous.

Plan-vs-reality deltas:

1. **My assertion read the wrong number, and finding out why was worth the detour.**
   `sweep_branch_maintenance` returns `(expired, removed)` where `removed` is the **metadata-prune**
   count; the reclaimed-table count only feeds a `log::debug!`. The test now asserts on
   `Tree::metrics().tables_reclaimed` and on the owner's level-set size directly. A comment at the
   return site now says which number is which, because the tuple gives no clue.
2. **The detach failure case is the one with a genuine window** and it behaves correctly:
   detach materializes inherited rows into the branch's own tables *before* clearing the parent
   link, so a failure between them leaves rows copied but the link intact. Reads are identical
   either way, because the copies shadow what they were copied from — asserted rather than assumed.
3. Two of `docs/removed-surfaces.md`'s carried-forward obligations are now partially reachable
   (a failed publish that recovers the complete old state). The object-store-specific half — an
   outcome that is *unknown* rather than chosen — stays with PG1, as `ASYNC_OBJECT_STORE_HANDOVER.md`
   records.

Verification: 1,105 lib tests green (1,098 → 1,105), 4 doc tests, clippy `--all-targets
--all-features` clean, fmt clean.

### V2b — Selected apply and compensating restore: complete (2026-08-15)

Closes the last two gaps against strata's design contract (items 8 and 9 of its ten V1 workflows).
With these, SurrealKV implements all ten; strata itself implements six.

As built:

- **`BranchDiff::iter_range(lower, upper)`** — bounded at the iterator, not filtered after it, so a
  diff over a narrow range of a large branch reads a narrow range. `DiffIter::new` now takes the
  bounds that were hard-coded `Unbounded`.
- **`BranchHandle::merge_range(target, strategy, lower, upper)`** — the same two-pass session,
  scoped. `MergeSession::scoped_to` sets the range; `changes()` applies it to both passes, so the
  plan and the write see the same subset.
- **`BranchHandle::revert_range(lower, upper)`** — a compensating commit. For each key the branch
  changed in the range, it writes back what the branch inherited at its fork anchor, deleting keys
  it created. Keys already equal to their inherited value are skipped, which is what makes it
  idempotent.

**The load-bearing decision: a scoped merge records NO promotion edge.** An edge says "everything
this source wrote up to sequence N is in the target" — a claim about *sequences*, and there is no
way to express "all of it except the keys outside this range". Recording one after a partial apply
would make the next full merge skip the keys the scoped one did not carry.

Probed, and the failure is exactly as bad as predicted: with the `is_scoped()` guard removed so a
scoped merge records a full edge, `a_scoped_merge_does_not_claim_the_source_is_fully_merged` reports
`applied: 0` on the following full merge. Two keys that were never merged are **never offered
again**. That is silent data loss, and it is now guarded by a test whose assertion names the keys
that would vanish.

Plan-vs-reality delta:

1. **`diff()` is write-based, not value-based, and a revert test had to be rewritten to say so.**
   The first version asserted the diff shrinks to one entry after reverting three keys. It does not,
   and should not: a revert is a compensating *write*, so the branch still reports four changed
   keys — it has written values that happen to equal what it inherited. The corrected test asserts
   the count stays at four, that the reverted key's newest op is a `Set` of the inherited value, and
   that the invented key is now **tombstoned rather than absent**. That is the difference between
   compensating and rewriting history, and it is worth an explicit test rather than a comment.
2. The `merge_range` entry point and `merge_into` now share `run_merge`, so the refuse / apply /
   record-edge sequence exists once. The scoped/unscoped difference is a single `if` at the edge
   record, which is where it belongs.

Verification: 1,098 lib tests green (1,093 → 1,098), 4 doc tests, clippy `--all-targets
--all-features` clean, fmt clean. Five new tests: a scoped merge carrying only its range, the
no-edge guarantee with its data-loss assertion, revert restoring / un-deleting / removing-created
with an out-of-range control, revert's idempotence and its no-op case, and its refusal on a branch
with no lineage.

### V2a — The strategy set, and one place it becomes an action: complete (2026-08-15)

As built:

- **`MergeStrategy::decide(&Conflict) -> ConflictOutcome`** — the single exhaustive `match`. Every
  decision point routes through it, so adding a variant is now a compile error at exactly one site.
  It replaced **four `==` comparisons** spread across `merge.rs` and `lsm.rs`, under which a new
  `TargetWins` would have fallen through to "not Strict, therefore write the source's value" — the
  precise opposite of its name, compiling clean.
- **`MergeStrategy::TargetWins`** — keeps the target on a conflict; everything non-conflicting still
  applies.
- **`MergeStrategy::Resolve(Arc<dyn ConflictResolver>)`** with `ConflictChoice { Source, Target,
  Value(Option<Value>), Refuse }`. Blanket-implemented for `Fn(&Conflict) -> ConflictChoice`, so a
  closure is a resolver. It runs inside pass two, which is the point: a caller who previews,
  decides, and then writes through an ordinary transaction is writing **outside** the merge's
  `start_seq` window, where a concurrent target write is not caught. Resolution was always
  *possible*; this makes it safe.
- **`merge_into_expecting(target, strategy, expected_head)`** with `Error::UnexpectedHead`.
  `expected_head` is the target's `BranchInfo::last_write_seq`. Deliberately stronger than the
  oracle's protection: the oracle refuses when the target changed a key the merge writes, this
  refuses when the target changed *at all*.

Verified by probe: making `TargetWins` write the source's value — exactly what the old `==` chain
would have done — reddens `target_wins_keeps_the_target_and_still_applies_the_rest` on its first
assertion (`applied` 2 where 1 is correct). `source_wins_is_the_exact_mirror_of_target_wins` runs
the same fixture with the opposite expectation, so a strategy quietly doing the other one's job
cannot pass either.

Plan-vs-reality deltas:

1. **`MergeStrategy` lost `Copy`, `PartialEq` and `Eq`.** `Resolve` carries behaviour, and a
   strategy that carries behaviour cannot honestly be compared for equality. Checked first that
   nothing depended on it: all 28 test uses pass a strategy as an argument, none compares one.
   `Debug` is now hand-written — a resolver is a function and there is nothing useful to print.
   Strategies pass by reference through `preflight`/`apply`.
2. **`resolved` moved into `MergePreflight` and is no longer inferred at the call site.** It was
   `if strategy == SourceWins { conflicts } else { 0 }` — which would have reported `0` for
   `TargetWins` and for every resolver. `MergePreflight` also gained `refused`, so the refusal
   check is "did the strategy refuse", not "is the strategy `Strict`" — which is what makes a
   resolver's `Refuse` work without a second code path.
3. **`preflight` re-measures the entry when a resolver substitutes a value.** A resolver may return
   something larger than the source's value, and the chunk budget and `MergeTooLarge` preflight are
   both sized from that number.
4. **The resolver's determinism is a documented contract, not an assertion.** Both passes ask about
   the same keys; a resolver that answers differently the second time makes the plan a lie about
   the write. `apply`'s `Refuse` arm is therefore a real typed error rather than an
   `unreachable!()`, and it says so — with the honest consequence that chunks already committed
   stay committed.

Verification: 1,093 lib tests green (1,087 → 1,093), 4 doc tests, clippy `--all-targets
--all-features` clean, fmt clean. Six new tests: `TargetWins` and its `SourceWins` mirror, a
resolver inventing a value (asserting it was handed base/source/target for the conflicting key
only), a resolver deleting and a resolver refusing, per-key `Source`/`Target` choices, and
expected-head refusing a target that moved on an unrelated key — with a control arm proving the
refusal was the condition.

### V1 — Dead code deleted: complete (2026-08-15)

As built: `src/storage/` gone (7 files, 2,591 lines, 21 tests), plus eight orphans and every stale
`allow(dead_code)` marker in the tree. Full ledger in `docs/removed-surfaces.md`.

**The method that mattered here was stripping the markers and letting the compiler classify.**
Eighteen `#[cfg_attr(not(test), allow(dead_code))]` markers were removed wholesale; the compiler
then flagged exactly six items as unused. Those six are genuinely test-only and are now labelled
`#[cfg(test)]`, which is what they always meant. The other **twelve markers were stale** — their
items have had real production callers since PA wired the public branch API, and the attributes had
been silencing the very warning that would have found the dead code sitting next to them.

**Two claims in the plan were wrong, and the code was right both times.**

1. **`CoreInner::new` was not callerless.** The exploration said "zero callers anywhere, incl.
   tests"; `src/test/lsm_tests.rs:1762` used it. The `task.rs` hits that produced the false
   negative were `MockCoreInner::new` — a different type. Resolved by pointing the test at
   `new_impl`, the constructor production actually uses, and making it `pub(crate)`.
2. **Three `src/test/mod.rs` helpers were not dead.** `collect_history_all` alone has ~80 callers
   across `transaction_tests.rs` and `version_iterator_tests.rs`. Their `allow(dead_code)` markers
   were stale, and an in-flight deletion of them was caught — by the new guard, before it reached a
   commit. The markers went; the helpers stayed.

**The sanctioned test-count drop was undercounted in the plan.** It said "exactly the 11
conformance tests". The real figure is **21**: 11 in `conformance_tests.rs` plus 10 inline tests in
the backends the exploration's per-file table did not include (`local.rs` 2, `local_commit.rs` 4,
`memory.rs` 3, `native.rs` 1). Reconciled: −21 storage tests − 1 removed guard
(`simulated_fault_backend_is_test_only`, vacuous once the module is gone) + 2 new guards = **−20**,
which is exactly 1,107 → 1,087.

Other plan-vs-reality deltas:

3. **`WriteStallInfo` was wired, not deleted.** `check()` built it and `commit.rs` discarded the
   result, so its fields were write-only in production while `stall_tests.rs` asserted on them —
   `#[cfg(test)]` was therefore not available. A stall that cleared, with its reason, duration,
   value and threshold, is genuine operator signal, so the commit path now logs it. "Wire it or
   delete it": wired.
4. **`Writer.compressed_buffer` deleted rather than finished.** WAL compression is half-built;
   completing it is a feature and does not belong in a dead-code sweep. The unread buffer went, the
   compression-type byte on the wire is unaffected, and the decision is recorded rather than left
   implicit.
5. **Two guards were narrowed rather than deleted.**
   `branch_native_filesystem_io_is_confined_to_local_adapter` became
   `branch_decisions_stay_free_of_filesystem_io` over `src/api.rs` and `src/branch.rs` — still a
   real claim (the catalog decides, it does not do IO) — and gained a non-vacuity arm proving the
   detector finds `std::fs` where it genuinely is.
   `crate_root_exposes_one_engine_over_the_injected_roles` simply lost `"mod storage;"` and its
   now-inaccurate name.
6. **`SnapshotIterator.core` was deleted; `KMergeIterator.iter_state` was renamed `_iter_state`.**
   The second is a drop-order keepalive that must be held — the underscore says so without an
   attribute, which is the difference between documenting a constraint and suppressing a warning.

The new `no_module_suppresses_dead_code_warnings` guard found four false positives on its first
run — its own doc comment and its own comparison expression — and was tightened to match
attributes only. It self-tests twice: that the walk reached the tree (>30 files) and that the
detector matches its own target.

Verification: 1,087 lib tests green (1,107 → 1,087, reconciled above), 4 doc tests, clippy
`--all-targets --all-features` clean, fmt clean. **Zero `allow(dead_code)` attributes remain in
`src/`** outside one written allowlist entry.

### V0 — The async / object-store handover: complete (2026-08-15)

**Scope change:** `v2` no longer runs the seam port or the object-store adapter. Those move to a
separate experimental branch; `v2` finishes branching and closes the coverage gaps. The plan is at
`~/.claude/plans/ignore-bplustree-understand-how-harmonic-eclipse.md` (revision 2026-08-15),
stages V0–V8.

As built: `docs/ASYNC_OBJECT_STORE_HANDOVER.md`, docs only, no code changes. It carries forward the
PF1 gate's findings plus everything the deleted `src/storage/` module is worth remembering, so that
V1 can delete 2,591 lines without losing the reasoning. This is PA2's discipline repeated — write
the ledger first, then delete — and it is why V0 precedes V1.

Contents: the Option A / Option B decision with both cases stated (so the trade stays
re-examinable); why the deleted traits must not be restored verbatim; what in them *was* good
(`PutOutcome`'s idempotency semantics, `Bindings::validate`'s refuse-don't-degrade posture, the
`FaultPoint` before/after taxonomy) and what was not; the measured ~170 sites across 17 files; F3
and the atomic-swap manifest that resolves it; the nine-slice derived plan; six invariants the port
must not break, each named with the code that enforces it; the five carried-forward obligations
re-homed with their owners; PE2's split between V3's local failpoints and what only a real object
store can test; and nine open questions recorded rather than invented.

Gate note — a docs slice's gate is fact-checking, and it caught one error before publication. The
draft claimed the engine "uses tokio only in the commit pipeline and tests". It does not: `task.rs`
(11 uses), `commit.rs` (7), `lsm.rs` (2), `stall.rs` (1), `lockfile.rs` (1). Corrected, and the
open question reframed from "adopt a runtime" to "how much more of the engine becomes
runtime-bound, and does an embedded user without a runtime still have a usable API" — which is the
question that actually matters.

Also verified before writing, rather than recalled: the seventeen lock-holding publish sites
(9 × `catalog_publish.lock()` + 8 × `level_manifest.write()`); `PublishOutcome` and `PutOutcome`
being variant-for-variant identical; `BindingRequirements`' eight fields and its
`ErrorCode::CapabilityMismatch`; the seven `FaultPoint` variants; `snapshot.rs:355` taking the
manifest read guard from sync iterator construction; and that `src/vfs.rs` is a `File` trait for
`read_at`, not a swappable filesystem.

Verification: 1,107 lib tests green (unchanged — docs only), clippy clean, fmt clean.

## PF1 gate record (adversarial gate 2026-08-15 — STOPPED: the "cheap half" is not cheap, and one
## decision has to be made before any of it is written)

Two findings. The first is a scope measurement; the second is an architectural fork that determines
the shape of PF1, PF2 and PG1, and it is the reason this gate stops rather than amends.

**F1 — the seam port is ~170 IO sites across 17 files, not one module.** Counted against the tree:

| file | sites | | file | sites |
|---|---|---|---|---|
| `wal/recovery.rs` | 26 | | `lsm.rs` | 14 |
| `wal/manager.rs` | 25 | | `vfs.rs` | 7 |
| `checkpoint.rs` | 24 | | `compaction/compactor.rs` | 7 |
| `wal/reader.rs` | 21 | | `wal/writer.rs` | 6 |
| `authority/publish.rs` | 16 | | `lockfile.rs` | 6 |

(plus `wal/mod.rs`, `memtable/mod.rs`, `branch_runtime.rs`, `levels/mod.rs`, `sstable/*` at 1–4
each). The plan's PF1 covers authority + manifest + WAL + checkpoint in one slice at "≤ ~1,500 LOC
net". The WAL alone is four files and ~78 sites. **PF1 is at least three slices** — authority,
then WAL, then checkpoint + lockfile — with the source guard landing last, when the allowlist can
actually be minimal.

**F2 — `ObjectStore` is async; every caller that would use it is sync AND holds a lock across the
call.** This is the finding that stops the gate.

`ObjectStore` (`storage/mod.rs:168`) is `async fn` throughout. The authority publish path is called
from seventeen sites that hold either `catalog_publish` (a `std::sync::Mutex`) or
`level_manifest.write()` (a `std::sync::RwLock`) across the publish — fork, delete, detach, TTL
expiry, merge-edge recording, flush, compaction. A `std` guard may not be held across an `await`, so
these cannot become `async` without restructuring their locking, and that locking is load-bearing:
FK6's merge-edge publish holds the manifest read lock *precisely* to exclude a concurrent compaction
publish, and the compactor's `update_manifest` holds the write lock to make its anchor re-check
atomic with its own publish.

Plan finding F5 called PF1 "the cheap half" because FK1's publish primitive (temp-write → fsync →
`hard_link` → unlink → dir-fsync) maps exactly onto `put_unique` + `PutOutcome`. That mapping is
real and still correct — but it is a *semantic* match, and F5 did not check the calling convention.
It is not cheap.

`spawn_blocking` or a bare `block_on` inside the engine is not available as an escape: `merge_into`
is `async` and calls the sync `record_merge_edge`, so a `block_on` there would run on a runtime
worker. Plan finding F8 already refused this shape once, for fork, in the same words: "Do not paper
over this with `spawn_blocking` inside the engine — that hides a real property from the caller."

**The fork in the road.** Two coherent answers, and they are not compatible:

- **(A) The engine's IO seam is synchronous.** `ObjectStore` becomes sync, or gains a sync sibling
  that the engine uses; the async object-store adapter owns a runtime internally and blocks its
  caller. Keeps every lock structure exactly as built, keeps `LSMIterator` sync (which F4 already
  decided for reads), and confines async to the hydration path and the adapter's insides. The cost
  is honest and must be documented: an authority publish against S3 blocks the calling thread, and
  for `fork_branch` that thread is one every writer is waiting behind.
- **(B) The engine goes async on its write paths.** `std` locks become async locks, the publish
  paths become `async fn`, and fork's contract changes. This is what SlateDB does, and it is the
  shape that does not lie about the cost. It is also a rewrite of the locking in the commit, flush,
  compaction and branch-op paths — far beyond a seam port — and it contradicts plan finding F8's
  recorded decision that fork stays sync.

**Recommendation: (A)**, because F4 has already committed the read path to "sync engine, async
confined to hydration", and (B) would make the read and write paths disagree about their own
concurrency model while rewriting locking that three slices of this project were spent getting
right. But this is the decision that cannot be discovered by writing code and then revised: it
determines whether PG1's object-store adapter is a blocking bridge or a native async backend, and
picking wrong means porting all 170 sites twice.

**DECISION (user, 2026-08-15): (B) — the engine's write paths go async.** Recorded against my
recommendation of (A); the reasoning above stands as the case that was made and lost, and it is kept
because a later reader deserves to know the trade was made deliberately. **Plan finding F8 is
superseded**: `fork_branch` no longer has to stay sync.

**F3 — (B) as literally stated would drag the READ path async too, and that is not what was
chosen.** `Snapshot::collect_iter_state` (`snapshot.rs:355`) takes a read guard on
`level_manifest`, and it is called from sync `LSMIterator` construction. Turning that `RwLock` into
an async lock makes every scan and every point read `async`, which is exactly what plan finding F4
rejected on evidence, and nothing in the decision above asks for it.

The way to have async publishes and sync reads is to stop making readers take a lock the publisher
holds. Two mechanisms, and both are needed:

- **`level_manifest` becomes immutable versions behind an atomic swap.** Readers clone an `Arc` of
  the current version — no lock, no await, no blocking. A publisher builds the next version, awaits
  its IO, and swaps it in only on success. This removes the window that today is covered by holding
  the write lock across `apply_changeset` + `persist_owner_update`: with a swap there is no moment
  where an applied-but-not-durable state is observable, because the state readers see does not
  change until the IO has succeeded. (This is RocksDB's `VersionSet` shape, and it is a better fit
  for an LSM manifest than a mutex regardless of the async question.)
- **`catalog_publish` becomes an async mutex**, held across the await. It already serialises every
  branch operation; that is unchanged, it just becomes awaitable.

FK6's merge-edge ordering survives intact and gets *simpler*: it holds the manifest read lock today
to exclude a concurrent compaction publish, and with serialised publishers the exclusion is the
publish mutex itself.

**Derived slice plan (replaces the plan's single PF1):**

| slice | scope |
|---|---|
| **PF0a** | `LevelManifest` → immutable versions + atomic swap. Readers lock-free and sync. No IO or async changes; pure restructuring, fully testable on its own. |
| **PF0b** | Publish paths → `async fn`; `catalog_publish` → async mutex; fork / delete / detach / expiry / flush / compaction publishes and their transitive callers become async. `Tree`'s branch API becomes async. |
| **PF1a** | Authority publish through `ObjectStore::put_unique` / `read_range` / `list_page`, keeping `AlreadyExistsSame` idempotency. |
| **PF1b** | WAL IO through the roles (4 files, ~78 sites). |
| **PF1c** | Checkpoint + lockfile through the roles; `Platform` clock/randomness injection; the source guard, whose allowlist can only be minimal once everything above has landed. |

PF0a is next and carries its own gate.

### PE1 — Branch metrics: complete (2026-08-15)

As built: `src/metrics.rs`, `Tree::metrics() -> BranchMetricsSnapshot`, and one new test file.

Eight counters, incremented where the event happens: `forks`, `fork_drain_nanos`, `detaches`,
`merges`, `chunked_merges`, `branches_reclaimed`, `tables_reclaimed`, `pin_retained_versions`,
`compaction_pin_races`. Three gauges, computed when the snapshot is taken and never stored:
`live_branches`, `timeline_horizon`, `wal_pinned_segments`.

`Timeline::horizon` loses its `allow(dead_code)`: FK2 wrote it for a metric that never shipped, and
this is that metric.

**Two production defects, both found by writing the twin rather than the happy arm.**

1. **`branches_reclaimed` was counting SST files.** The wiring took `reclaim_tombstoned_branches`'s
   return value, which is `deleted` — incremented per `remove_file`, not per branch. A branch
   deleted before it flushed would have reported zero reclamations while having been fully
   reclaimed. Fixed by counting both, separately, because they answer different questions and
   routinely differ: `branches_reclaimed` says the sweep is running, `tables_reclaimed` says how
   much disk it gave back.
2. **The branch count climbed on every idle sweep.** A tombstoned catalog entry survives until
   metadata pruning drops it, so the sweep revisits owners it has already emptied. `released += 1`
   per tombstoned owner therefore counted *attempts*. Fixed to count only an owner that still had
   something to give up — a runtime with its memtables and WAL dependency, or tables. The twin that
   caught it is "a second sweep releases nothing".

Plan-vs-reality deltas beyond the gate's four findings:

3. **The metric's meaning was pinned down by the test, and then written into its doc.** A branch
   that never wrote holds no runtime (BR4's lazy runtimes), so releasing it releases nothing and it
   is not counted. That is defensible and now stated: the counter measures resources freed, not
   tombstones walked. Counting tombstones would need new durable state to know which had been seen.
4. **Two of my test expectations were wrong and the code was right**, recorded because they are the
   same class of error each time — asserting the behaviour I remembered rather than the one that
   shipped. A fork retry is *idempotent* (FK4), so it returns the original receipt rather than
   erroring, and it correctly counts nothing. And `Tree::flush` rotates only the default runtime, so
   a child branch's memtable has to be rotated explicitly to put a table on disk.

Verification: 1,107 lib tests green (1,101 → 1,107), clippy `--all-targets --all-features` clean,
fmt clean. Six tests, each with the twin its counter needs.

## PE gate record (adversarial gate 2026-08-14 — the slice splits; fault injection is blocked)

**F1 — PE's fault-injection half cannot be built yet.** The plan says to inject faults "using the
existing `SimObjectStore`/`SimCommitStore` fault scripts (`src/storage/sim.rs`)". Those intercept
nothing today: `src/authority/publish.rs:11` opens files through `std::fs`/`OpenOptions` directly,
and the level manifest does the same (`src/levels/mod.rs:2`). Routing that IO through the roles is
**PF1's** entire job. Injecting faults into the branch state machines before the seam exists would
mean building a second, throwaway injection mechanism.
→ **PE splits.** PE1 is the metrics, now. **PE2** is the fault-injection lane over fork / delete /
expiry / detach / merge, and it moves to immediately after PF1, where the sim stores actually sit
under the engine. Recorded as a dependency, not a deferral of convenience.

**F2 — `pinned_bytes_by_branch` is not attributable after FK6, and bytes are not measured.** The
plan names a per-branch byte gauge. Two problems: retention pins are now a *set* of anchors and one
retained version commonly serves several of them, so attributing its bytes to one branch is a
choice, not a measurement; and the compaction iterator counts versions, not bytes. What it already
computes — `pin_retained_versions`, currently thrown away in a `log::debug!` — is the honest form of
the same question ("what is branching costing me in retained history"), so that is what ships.

**F3 — half the plan's list is already computable, and storing it would create staleness.** The
timeline horizon (`timeline.rs:113`, carrying an `allow(dead_code)` since FK2 precisely because its
metric never shipped), the WAL pinned-segment count (`WalDependencySnapshot`), the live branch count
and the per-owner retention floor are all derivable on demand from state that is already the source
of truth. Copying them into atomics would add a second place for each to be wrong — the exact hazard
the no-stale-state rule names.
→ The snapshot mixes **stored counters** (events, which must be counted as they happen) with
**derived gauges** (computed at read time, structurally incapable of drifting). Only the counters
need plumbing.

**F4 — "fork latency" as the caller sees it is not the number worth having.** A caller times its own
`fork_branch` call. What no one can see is how long the write fence was held, which is the cost the
whole store pays for one branch operation and the number the "many cheap branches" premise stands
on. `fork_drain_nanos` measures the fence, not the call. Detach and merge get counts but no timing:
the caller awaits them and can time them itself, and neither blocks unrelated writers.

**Exit gate (PE1):** every counter has a test that moves it and a twin where it must not move; the
derived gauges are read through the same snapshot and asserted against the state they derive from.

### PD3c — The scan-mode probe and its equivalence: complete (2026-08-14)

Closes PD1's deferral. Plan A7 asked for "point `get_latest_meta` per key below a threshold,
seq-filtered scan above it", with the two modes proven to agree.

As built:

- **`TargetProbe` now asks the question it needs**, not two questions that imply it.
  `base_meta` + `target_meta` became `moved_since_base(key)`. The old pair forced every
  implementation to manufacture two `VersionMeta`s so the caller could compare their sequences,
  which the scan cannot do cheaply and does not need to: "has the target moved since the base" and
  "is this key in the target's changes above the base" are the same question. The conflict rules in
  `classify` are otherwise untouched, which is what PD1's gate required of this slice.
- **`ScanProbe`** — a forward-only walk of the target's OWN changes above `base.target_at`, which is
  a `BranchDiff` on the target: the same machinery PC built for the source side, pointed the other
  way. A key absent from the walk has not moved, answered with no reads at all. A key present hands
  over its value too, so the read the point probe would have made for `target_value` is already
  done.
- **The ordering contract is enforced, not assumed.** The probe refuses a key at or below the last
  one it answered. Answering would be wrong in the worst available direction — a moved key reported
  as untouched, which applies straight over it — so it fails closed.
- **`SCAN_PROBE_THRESHOLD` (256), chosen from an exact count rather than an estimate.** Pass one
  always points, because it is the pass that *discovers* how big the merge is; pass two picks by
  `preflight.examined()`. The alternative was estimating the source's size from SST `item_count` and
  memtable bytes before either pass, which needs an invented bytes-per-entry constant — the skiplist
  exposes no entry count — for a decision the first pass answers exactly a moment later.

Verified by probe, both directions:

- Advancing the cursor with `<=` instead of `<`, so it steps past the key it was asked about,
  reddens the equivalence test on "the two probes must not disagree about anything". The test binds.
- Removing only the cursor-value optimisation, so `target_value` always reads through the snapshot,
  leaves it green. The test is insensitive to changes that are not semantic, which is what an
  equivalence test should be.

Plan-vs-reality delta: the plan describes the crossover as a property of the *detection mode* chosen
per merge. As built it is a property of pass two only. Pass one cannot choose by size without
already knowing the size, and the honest resolutions are either a fabricated estimate or this. The
cost is that a very large merge still pays one point-probing pass — the same one PD2 always paid —
and pass two, the pass PD3a added, is the one that gets cheap.

Verification: 1,101 lib tests green (1,098 → 1,101), 4 doc tests, clippy `--all-targets
--all-features` clean, fmt clean. Three new tests: the decision-table equivalence fixture covering
every row (with assertions that the fixture reaches all of them, so agreement is not cheap), the
out-of-order refusal, and a merge above the threshold applied end to end through the public API.

### PD3b — The crash matrix (no durable intent): complete (2026-08-14)

As built: three tests and no production code. The gate below records why the slice's planned
mechanism was not built; this records what stands in its place and what it proves.

- **`interrupted_chunked_merge`** — the shared fixture. It drives a chunked merge that breaks off
  part-way, using no injection: a concurrent write to a key late in the key order makes that key's
  chunk fail on the oracle while the chunks before it are already durable, which is the same state a
  crash between chunks leaves. The fixture asserts its own non-vacuity — the first key present, the
  last absent — so a change that made the merge atomic again could not leave these tests green.
- **`an_interrupted_chunked_merge_completes_on_a_re_run`** — the re-run finishes it, with
  `converged > 0` and `resolved == 1`. Probed: removing the convergence rule from `classify`
  reddens it on exactly that assertion, so it is evidence that the re-run completes by comparing
  values rather than by replaying blindly.
- **`a_half_merged_target_reopens_and_the_re_run_finishes_it`** — the same across `close` and
  reopen. The half-merged data is durable, and so is the *absence* of an edge, so the re-run still
  has the whole merge to offer.
- **`a_second_source_finds_a_half_merged_target_and_conflicts`** — another branch merging into a
  half-merged target sees keys it did not put there and conflicts.

Plan-vs-reality delta beyond the gate: the second-source test first asserted a conflict that did not
happen, and the code was right. The second source had been forked *after* the interrupted merge, so
the half-merged writes were its own base and overwriting them is an ordinary non-conflicting write.
Moving the fork before the merge is what makes the scenario the one the test names.

Verification: 1,098 lib tests green (1,095 → 1,098), clippy `--all-targets --all-features` clean,
fmt clean.

## PD3b gate record (adversarial gate 2026-08-14 — the durable intent is NOT built; reasoning below)

The plan's PD3 asks for "a durable `MergeIntent` cursor so a crash resumes instead of stranding
partial state. The intent pins the base against retention." Re-verified against the code after
PD3a, each of the three things it is for is either already true, already free, or actively worse
than not having it.

**The pin is already held, twice over.** A merge reads the target at `base.target_at`, which is
either the source's fork anchor or the target side of the previous merge edge. FK6 made both of
those live retention anchors derived from the catalog, and PD3a's `record_merge_edge` holds the
level manifest across the publish so the anchor cannot be missed by an in-flight compaction. There
is nothing left for the intent to pin, and a second mechanism pinning the same thing is the kind of
duplicate expression of one invariant that PD2's C5 probe already showed makes a system harder to
reason about, not safer.

**"Stranding partial state" overstates the risk.** A crashed chunked merge, re-run, classifies every
already-applied key as **Converged** — the target holds exactly the source's value, which is the
third row of the decision table — and applies the rest. The final state is the same one an
uninterrupted merge would have reached. That is not a workaround; convergence is what the three-way
comparison is *for*. It is now proven by test rather than asserted.

**A cursor would make behaviour worse, not better.** Skipping keys at or below a recorded cursor
assumes they are still as the merge left them. If the target changed one of them after the crash, a
cursor skips it silently, where re-classification reports a conflict. The cursor trades a correct
conflict report for speed on a path that runs once per crash. That is the wrong direction for the
one operation in this store that can destroy another writer's data.

**And it introduces a stuck state.** An intent written before the first chunk and cleared after the
edge outlives a crash by construction. Something must then clear it, which means either automatic
resume on open — resuming a merge the operator never asked to resume — or a manual escape, or an
expiry. All three are new machinery guarding against a hazard that the absence of an edge already
handles correctly.

**Cost, for completeness:** a catalog format change with golden vectors regenerated, plus two extra
catalog publishes (~4 fsyncs) on *every* merge including the small atomic ones, to buy a typed
"merge in progress" in place of an accurate conflict.

**One benefit is real and is being declined knowingly.** After a crash mid-chunked-merge, if the
source then changes a key it had already merged, the missing edge makes the target's movement look
like independent divergence and the re-merge reports a **false conflict**. PD2 recorded that
degradation and accepted it ("false conflicts, never silent overwrite"); only a cursor would remove
it, and a cursor costs the silent skip above. Refusing is the safe end of that trade.

**What PD3b ships instead:** the crash matrix the exit gate actually cares about, with no new
durable state — an interrupted chunked merge completing on a re-run, the same across a close and
reopen, and a second source finding a half-merged target and conflicting rather than overwriting.
The interruption is real rather than injected: a concurrent write to a key in a later chunk makes
that chunk fail on the oracle, which is exactly the shape a crash leaves behind.

## PD3a gate record (adversarial gate passed 2026-08-14, with one defect and four amendments)

The retention half of PD3's gate is above (it became FK6). This is the merge half, re-verified
against the code after FK6 landed. PD3 splits into three slices; this is the first.

**F1 — the merge's oracle check does not cover the planning window. (a PD2 defect)**
`preview_merge_into` plans against `visible_seq_num = V`; `commit_merge` then builds a transaction
whose `start_seq_num` is `core.seq_num()` — read later, from the *same* `Arc<AtomicU64>`
(`lsm.rs:334` and `commit.rs:271` share it). The oracle refuses a commit only when a written key's
stamp is `> start_seq` (`commit.rs:378`), so a target write landing in `[V, start_seq]` is invisible
to the plan AND accepted by the oracle: silently overwritten, under `Strict`, with no conflict
reported. PD2's doc comment claims the oracle catches concurrent writers; it catches only those
arriving after the transaction was constructed.
→ The merge holds ONE `start_seq` — the sequence it planned at — and every chunk commits with it.
A target write to any merged key since planning then conflicts. If the merge runs long enough for
the oracle to GC past it, `check_owned` returns `TransactionRetry`: an honest refusal, not a missed
conflict. The session also holds one `ActiveTxnGuard` at that sequence for its whole life, so the
GC watermark cannot advance past it *between* chunks and manufacture that retry.

**F2 — chunking cannot preserve `Strict`'s "nothing is written", so planning becomes a separate
pass.** PD1's exit gate is that conflicts are reported before any mutation. If chunks commit as the
plan streams, a conflict in chunk 5 arrives after chunks 1–4 are durable. So pass 1 scans and
classifies without buffering (counts, bytes, and conflicts up to a reporting cap) and pass 2
re-streams and commits. Both passes read ONE fixed source diff and one fixed pair of target
snapshots, held by the session — otherwise a source write between the passes could introduce a
conflict pass 1 never saw.
→ Recorded contract change: **a merge that fits in one chunk is atomic; above that it is resumable,
not atomic.** Chunking and all-or-nothing are incompatible, and the plan asked for chunking.

**F3 — the size preflight is after the materialization it exists to prevent.** Today `MergeTooLarge`
is raised after a full `Vec<(key, value)>` and a full transaction write set are built — three copies
of the apply set in RAM before the refusal. Pass 1 measures while streaming, so the chunk-or-refuse
decision happens before anything is materialized.

**F4 — `MergeTooLarge` would become dead code, so it is re-pointed at the one thing chunking cannot
fix.** If everything above the ceiling chunks, nothing is left to refuse. A single entry larger than
the chunk budget is genuinely unchunkable — one key cannot be split across two batches — and that is
now what the error means, with both numbers being the offending entry and the budget. Reachable,
testable, and actionable ("raise `max_memtable_size`"). No new options type; `merge_into` keeps its
signature.

**F5 — FK6's carried window closes here, and it does not need the durable intent.** The hazard was a
compaction publishing between a merge's data commit and its edge record, having sampled anchors
without the new one. Taking the level-manifest READ lock across the edge publish blocks exactly that
(compaction publishes under the write lock), and any compaction already in flight hits
`appeared_since` on re-check and discards its output. Lock order stays `level_manifest ->
branch_catalog`, matching the fork path (`lsm.rs:1882`) and the compactor's publish.

**Split.** PD3a is F1–F5 with no format change. **PD3b** is the durable `MergeIntent` (a catalog
format change, so golden vectors), automatic resume, and the refusal of a second merge into a
half-merged target. **PD3c** is PD1's deferred scan-mode `TargetProbe` and its equivalence test.
The plan's "durable cursor so a crash resumes instead of stranding partial state" overstates what is
at risk: a crashed chunked merge re-run from the start classifies every already-applied key as
**Converged** and reaches the same final state without a cursor. The intent's real jobs are to name
the half-merged state, refuse a conflicting concurrent merge, and skip the re-scan — worth a slice,
not worth conflating with this one.

**Exit gate:** a merge larger than the chunk budget completes in chunks and is fully visible; a
target write in the planning window produces a conflict rather than an overwrite (with the probe
showing the overwrite when the shared `start_seq` is removed); an unchunkable single entry is
refused with both numbers and nothing is written; a one-chunk merge stays atomic and byte-identical
to PD2's behaviour.

## PD3 gate record (adversarial gate FAILED 2026-08-14 — two confirmed defects, plan amended)

PD3's gate re-verified the chunked-merge plan against the code and found that the ground it stands
on is not sound. Two defects, both confirmed by executable probes rather than by argument, both
rooted in the same mechanism. PD3 is postponed behind a new slice, **FK6**, which fixes the root
cause. Chunking a merge that is already refused-or-wrong would have been building on sand.

### The mechanism

§3.3a of the fork design says a parent's compaction pins "the oldest Active child anchor" — one
sequence number, `min_active_child_anchor` (`branch.rs:270`), fed to the compaction iterator as
`history_pin_floor` (`iter.rs:767`). With versioning off (the default) that is a *point* pin: for
each key it retains the newest version at or below the floor (`iter.rs:1178`). Separately,
`retained_floor_advance` (`iter.rs:782`) records, for any key that lost a version, that key's
NEWEST sequence — so `retained_floor` means "a view capped below this is incomplete", and it is
pin-blind: it rises even when the dropped version was irrelevant to every pin.

**One point pin cannot serve two distinct read anchors on one owner.** That is the root cause of
both defects below, and neither `min` nor `max` fixes it: `min` breaks the deeper child, `max`
breaks the shallower one, and range-pinning at `max` retains the parent's entire history for as
long as any child forked at head is alive, which is unacceptable for a store whose premise is many
cheap branches.

### Defect 1 — a second child at a different anchor reads a stale inherited value

Probe (temporary, run against the tree at this commit): parent writes `k=v1`, flush, fork
`shallow`; writes `k=v2`, flush, fork `deep`; writes `k=v3`, flush; four filler flushes; compact.

```
PROBE retained_floor after compaction = 3
PROBE shallow=Some("v1") deep=Some("v1")
assertion failed: deep child's inherited view
  left: Some("v1")   right: Some("v2")
```

The pin floor is `min` = `shallow`'s anchor, so the newest version at or below it (`v1`) is
retained and `v2` is dropped as superseded by `v3`. `deep`, capped at `v2`'s sequence, finds `v1`
and answers with it. No error, no diagnostic: **a fork child silently reads a value that was never
current at its anchor.** This is invariant 5 of the FK design failing, and it predates PC/PD1/PD2 —
it has been reachable since FK3. Under versioning the same hole exists in its wall-clock flavour:
the clamped retention floor is clamped to the *oldest* anchor, so versions between the anchors age
out while a deeper child still needs them.

### Defect 2 — the first parent compaction permanently disables merge

Probe: fork `work`, write, merge into `main` (clean, edge recorded), `main` reverts the key, five
filler flushes, compact, `work` writes again, re-merge.

```
PROBE merge 1 = MergeOutcome { applied: 1, ..., source_through_seq: 2, target_through_seq: 3 }
PROBE retained_floor = 4
PROBE merge 2 refused: Fork point 1 is below the retention floor 4; that history has been collapsed
```

`validate_lineage` (`merge.rs:294`) gates on `retained_floor > fork_seq`. Because the floor is
pin-blind, ANY key losing ANY version raises it above every child's anchor — which is what ordinary
compaction of an updated key does. So a merge works exactly until the parent first compacts, and is
refused for ever after. The refusal is *false*: `work`'s inherited view is intact, pinned at its
anchor. Merge is, as shipped in PD2, a one-shot feature that dies at the first compaction.

Worse, the refusal is also load-bearing in a way I did not design: relaxing it naively would expose
a silent overwrite. After the first merge, `effective_base.target_at` is the edge's
`target_through_seq` — a cap ABOVE the fork anchor, pinned by nothing. Compaction may drop the
version the merge left behind, so the base reads as the pre-merge value; if the target has since
reverted to that value, `base_value == target_value` classifies the key as **Apply** and the second
merge overwrites the target's deliberate revert with no conflict. The over-broad floor check is
currently the only thing standing between PD2 and that overwrite.

### FK6 — the fix (this slice)

Retention pins become a **set of anchors per owner**, derived from the catalog as today:

- every live child's `fork_seq` (defect 1), and
- every live merge edge's `target_through_seq` (defect 2) — the base a future merge from that
  source reads at, which is exactly the same kind of promise a fork anchor makes and was simply
  never expressed as one.

Compaction retains, per key, the newest version at or below *each* anchor (the range form under
versioning). Completeness becomes one predicate — a view capped at `C` is exact iff
`C >= retained_floor || C` is a live anchor — used by both the fork check (`lsm.rs:1884`) and the
merge check, replacing the pin-blind comparison in each. `CompactionPinRaced` generalises from
"the floor regressed" to "the current anchor set contains an anchor the job did not sample"; an
anchor that disappeared is harmless, because the job retained more than was needed.

No format change: anchors derive from the catalog, which is already durable, so `retained_floor_seq`
keeps its meaning and the golden vectors stand.

**Exit gate:** the two probes above become permanent tests and pass; a merge still succeeds after
the parent has compacted (defect 2's false refusal is gone); the silent-overwrite scenario behind
it produces a **conflict**; the anchor set is capped and a 1000-branch parent still compacts; each
arm ships its sabotage twin (collapse the anchor set back to `min` and the deep-child test reddens;
drop the merge-edge anchor and the overwrite test reddens).

### PD2 — The merge commit and promotion edges: complete (2026-08-14)

As built:

- **`BranchHandle::merge_into(target, strategy)`** — plans, then writes the applies as ONE ordinary
  transaction on the target (the gate's reversal: no SST ingest). It therefore inherits sequence
  allocation, WAL durability, fresh commit timestamps, oracle conflict detection and `visible_seq`
  publication from the existing write path rather than re-implementing them beside it.
- **`MergeStrategy::{Strict, SourceWins}`** — `Strict` is the default and refuses the entire merge
  on any conflict, writing nothing; `SourceWins` applies the source's value to conflicting keys and
  reports how many it overrode in `MergeOutcome::resolved`.
- **Promotion edges** — `MergeEdge { source, source_generation, source_through_seq,
  target_through_seq }` on the target's catalog entry, sorted by source id, capped, and validated
  (strictly ascending, no self-merge). Format change: the catalog golden vector was regenerated
  deliberately and its comment updated to describe the new field.
- **`effective_base`** — a later merge diffs the source from `source_through_seq` and reads the
  target's base side at `target_through_seq`. Both are needed; the first design carried only the
  source side and would have reported a clean re-merge as a conflict against the target's own copy
  of the source's change (gate amendment 1).
- **Refusals**: `MergeConflicts` under `Strict`, `MergeTooLarge` when the write set exceeds the
  target's memtable budget (checked *before* committing), `BranchesUnrelated`,
  `BelowRetentionFloor`, and `TransactionWriteConflict` when a concurrent writer touched the same
  keys.

**A probe that refused to redden, and what it taught.** The C5 test — one source's merge must not
shift another's base — passed with the per-source edge lookup replaced by "any edge on the target".
The reason is that the generation guard is a second expression of the same invariant: FK1 makes
generations globally unique, so "an edge whose `source_generation` matches this source" is already
"an edge from this exact source incarnation". The two checks are redundant by construction, and
either alone blocks the hazard. Removing **both** does redden the test, and the failure is
instructive: the second source's merge returns `applied: 0` — silently dropping its change in the
belief it was already merged. Recorded rather than left as a mysteriously-green probe.

Other probes, each reddening on its intended assertion: ignoring the recorded edge makes a repeat
merge re-offer an already-merged key (`left: 1, right: 0`).

Verification: **1,070 → 1,078 lib tests** green, clippy `--all-targets --all-features` at zero
warnings, fmt clean. Eight new merge tests covering the clean path, `Strict`'s all-or-nothing
refusal, `SourceWins`, iterative re-merge (including a target edit after a merge not resurrecting a
conflict), per-source edge scoping, the lost-edge crash window degrading to convergence, durability
across a reopen, and the oversized-merge refusal.

## PD2 gate record (adversarial gate passed 2026-08-14, with one reversal and four amendments)

Verified against the tree before implementing the merge commit — the first slice in this phase
that writes to a branch it does not own.

**REVERSAL: merge-as-SST-ingest is not built. The merge commits as an ordinary transaction.**
Plan amendment A2 mandated building the merged rows into an SST with `TableWriter` and adding it
to the target's L0 through a manifest changeset, bypassing the write path. Its stated
justification — that `MemTable::add` reserves the whole batch in a fixed arena, so a large merge
could never be applied — expired when the FK-era fix made `apply` rotate to a right-sized arena
(recorded as PD1 gate finding F6). What remained was "a multi-hundred-megabyte memtable is absurd
and a giant oracle publish freezes writers", and that argues for **bounding the batch**, not for
bypassing the write path. Bounding is what PD3's chunking does, and it is required anyway.

Weighed against each other, ingest loses on every correctness axis. Committing as a transaction
gets sequence allocation, WAL durability, fresh commit timestamps from the FK2 timeline, oracle
conflict detection against concurrent target writers, and `visible_seq` publication — all from a
path with a thousand tests behind it. Ingest would need each of those re-implemented beside the
pipeline, in the one operation where a mistake means silently overwriting someone's data. So:
merge is a transaction on the target, with a size preflight that refuses `MergeTooLarge` above the
target's memtable bound, and PD3 removes the bound by chunking rather than by bypassing.

**Amendment 1: a promotion edge needs BOTH sides' sequences, not just the source's.** The
first design recorded here carried only `source_through_seq` (diff from the last merge instead of
the fork). Working an example shows that is not enough: after S's `K=1` is merged, the three-way
base for a later merge must be evaluated at the target as it stood *after* that merge, not at the
fork. Read at the fork, `K`'s base is the pre-merge value while the target holds the merged one, so
a clean re-merge reports a conflict against the target's own copy of the source's change. The edge
is `{source, source_generation, source_through_seq, target_through_seq}` and the effective base is
`(diff from source_through, compare against the target at target_through)`. The plan's original
shape was right and the simplification was wrong.

**Amendment 2: edges are a usability requirement, not a correctness one — which is why they ship
now.** Without any edge the merge is still sound: an already-merged key has `source_value ==
target_value`, so PD1's converged rule makes it a no-op, and a key the target has since changed
becomes a false conflict rather than an overwrite. But that makes merge effectively single-use per
branch — the second merge conflicts on everything the first one applied and the target later
touched. For a workload built around agents iterating on a branch and merging repeatedly, that is
not a rough edge, it is the feature not working.

**Amendment 3: data before edge, verified rather than asserted.** The merge commits its rows
first and publishes the edge second. A crash in between leaves the edge at its previous value, so
the next merge re-diffs from the older base: already-merged keys reappear, converge if the target
still holds them, and conflict if the target has moved on. False conflicts, never silent
overwrite. The opposite order would advance the base past changes that were never applied — which
loses them permanently and silently.

**Amendment 4: concurrent target writes are the oracle's job, and the answer is a typed retry.**
The merge transaction's write set goes through `check_owned` like any other, so a target write that
lands between planning and commit produces `TransactionWriteConflict`. PD2 surfaces that rather
than looping: an automatic re-plan is a policy decision, and a caller that just had its merge
invalidated by a concurrent writer usually wants to know.

### PD1 — Merge planning and conflict detection: complete (2026-08-14)

Planning is a pure read that produces the complete verdict before anything is written, so a merge
that would conflict is refusable without having already applied half of itself.

As built (`src/merge.rs`):

- **The decision table**, in rule order: `base_seq == target_seq` → apply (metadata only, no value
  read); `base_value == target_value` → apply (the target moved and came back);
  `source_value == target_value` → converged, a no-op; otherwise conflict. Absence is a value
  throughout, which is what makes delete-vs-delete converge and delete-vs-modify conflict.
- **`MergeReport { applies, conflicts, converged }`** and
  **`Conflict { key, kind, base, source, target }`** carrying all three sides, so a caller decides
  without re-reading. `ConflictKind` distinguishes both-modified from either delete direction.
- **`Snapshot::get_latest_meta`** — metadata for the newest visible version, tombstones reported
  rather than hidden. Built by extending `LayerHit::Tombstone` to carry `(seq, kind)` so it uses
  the *same* layer walk as `get`, rather than a parallel one that could disagree about visibility.
- **`TargetProbe`** seam with the point-lookup implementation. The scan implementation and its
  equivalence test are deferred to PD3 (gate amendment 4), recorded rather than dropped.
- **`validate_lineage`** — merges are accepted only into the source's recorded parent
  (`BranchesUnrelated` otherwise), and only while the target's history at the anchor survives
  (`BelowRetentionFloor`).
- **`BranchHandle::preview_merge_into`** is the public entry point.

**A wrong justification of mine, corrected while writing the tests.** Gate amendment 1 originally
claimed the writes-only comparison is *unsound* because a target that is itself a fork child can
have keys change under it. That is false — inheritance is frozen at the anchor, so an ancestor's
later writes are permanently invisible. The comparison shipped is unchanged and still correct, but
the reason is now the honest one, and
`an_inherited_view_is_frozen_so_only_the_target_s_own_writes_move_it` pins the property the
argument rests on instead of leaving it assumed. The full correction is in the gate record below.

Sensitivity probes, all reddening on their intended assertion:

- removing the revert-to-base rule turns a content-unchanged target into a false conflict;
- removing the converged rule turns agreement between the two sides into a conflict;
- removing the lineage check lets a **sibling merge return an empty clean report** — a silent wrong
  answer rather than an error, which is precisely the failure class merge must not have.

Also removed: a speculative `VersionMeta::is_delete` with no caller.

Verification: **1,055 → 1,070 lib tests** green (7 decision-table unit tests against a fake probe,
8 integration tests against the real read stack), clippy `--all-targets --all-features` at zero
warnings, fmt clean.

## PD1 gate record (adversarial gate passed 2026-08-14, with five amendments)

Verified against the tree before implementing merge planning. Merge is where silent overwrite
lives, so these are stated as rules rather than notes.

**Amendment 1: "changed since the base" is a question about the target's VIEW, not its writes.**
The sound test is the newest version visible at cap `F` versus the newest version visible now, both
resolved through the target's full read stack — same machinery as an ordinary read, different cap.

> **Correction, made while writing the tests.** The justification first recorded here was wrong.
> It claimed the writes-only comparison is *unsound* because a target that is itself a fork child
> can have a key change under it when its own parent writes. That cannot happen: a fork child's
> inherited view is frozen at its anchor, so an ancestor's later writes are permanently invisible
> to it. Under the shipped model the two comparisons therefore agree, and the writes-only one is
> not wrong today. The view comparison is kept anyway, for a weaker but honest reason: it is
> correct *without* depending on inheritance staying frozen, so a future feature that let a branch
> re-anchor would not silently turn merge into an overwrite. `an_inherited_view_is_frozen_so_only_
> the_target_s_own_writes_move_it` pins the property the argument now rests on, rather than
> leaving it assumed.

**Amendment 2: the comparison is three-way by value, with a sequence fast path.** Comparing
sequences alone makes a target that changed a key and then changed it back look modified, which
produces a false conflict on a target that is, in content, untouched. The shipped rule is:

1. `base_seq == target_seq` → target never moved → **apply** (no value read at all — the common
   case stays metadata-only);
2. else `base_value == target_value` → target moved and came back → **apply**;
3. else `source_value == target_value` → both sides converged → **no-op**;
4. else → **conflict**.

Absence is a value here: a delete on either side compares equal to a delete on the other, which is
what makes delete-vs-delete resolve rather than conflict. Delete-vs-modify falls through to (4) and
always conflicts.

**Amendment 3: `LayerHit::Tombstone` discards the sequence, so it has to carry it.** The variant
currently collapses to `None` in `get`, losing the one fact `get_latest_meta` needs — a delete IS a
version, and its sequence is what decides whether the target moved. Extended to carry `(seq, kind)`.
This is deliberately done by extending the existing layer walk rather than writing a parallel one:
metadata and values must never disagree about what is visible, and one code path is how that is
guaranteed rather than tested.

**Amendment 4: scan-mode conflict detection is deferred to PD3, and the exit criterion goes with
it.** Plan A7 pairs per-key point lookups with a seq-filtered scan of the target, chosen by a size
crossover, and PD1's exit gate asks that both modes agree. Building the second mode now would add a
second implementation of the most dangerous logic in the system, for a performance crossover
nothing can measure yet — and the "both agree" test would be asserting an equivalence between two
things written on the same afternoon by the same author, which is worth very little. Point mode
ships here behind a `TargetProbe` seam so the scan implementation drops in without touching the
conflict rules, and the equivalence test lands in PD3 where it compares against code that has
soaked. Recorded rather than silently dropped.

**Amendment 5: merging needs the base to still exist.** Reading the target at cap `F` is only
sound if the target's history at `F` is intact. `retained_floor(target) > F` means the versions the
comparison depends on have been collapsed, so the merge is refused with `BelowRetentionFloor`
rather than planned against a base that is partly guesswork. Direction is also checked: a merge is
only accepted into the source's recorded parent, with `BranchesUnrelated` otherwise — no
sibling-to-sibling, no re-anchoring.

### PC — Branch diff: complete (2026-08-14)

A read-only, streaming view of what a branch changed since it was forked. Ships alone so it can
soak as an inspection tool before merge depends on it.

As built (`src/diff.rs`):

- **`Snapshot::own_only`** — the branch's own memtables and level set, no ancestors. The mirror of
  PB2's `inherited_only`.
- **`BranchDiff`** owns that snapshot, so the view is fixed for as long as it lives: iterate it
  twice and get the same answer, and writes landing afterwards do not appear. `iter()` yields a
  cursor, `collect()` materializes, `base_seq()` reports the anchor.
- **`DiffIter`** is a real `Iterator<Item = Result<DiffEntry>>` with an inherent `seek(key)` for
  resumption. One entry per key (the branch's newest write), tombstone-inclusive.
- **`DiffOp { Set | Delete | SoftDelete }`** rather than leaking `InternalKeyKind`, which carries
  encoding-only variants that are not part of any contract (gate amendment 5).
- **`BranchHandle::diff()`** refuses a branch with no lineage — `main`, a plain branch, or a
  detached one.

**The sequence filter is the whole slice.** Gate amendment 1 found that "everything in a branch's
own components was written by that branch" stopped being true when PB2 landed: detach materializes
inherited rows into the branch's own tables. So entries are filtered `seq > base` explicitly.
The probe is unambiguous — removing the filter makes a materialized branch report its entire
inheritance as changes (3 entries instead of 1).

**A real API bug the tests caught.** `collect` took the current entry, and `next` inferred
"exhausted" from `current.is_none()` — so every diff returned exactly one entry and stopped. Four
tests failed at once. Fixed by tracking exhaustion explicitly rather than inferring it from a field
a consumer can drain. Clippy then flagged the deeper problem: a cursor exposing `next()` without
implementing `Iterator` is a trap for exactly this kind of confusion. Reworked into a genuine
`Iterator`, which removed `collect`'s hand-rolled loop entirely and made the tests idiomatic.

Verification: **1,047 → 1,055 lib tests** green, clippy `--all-targets --all-features` at zero
warnings, fmt clean.

## PC gate record (adversarial gate passed 2026-08-14, with five amendments)

Verified against the tree before implementing diff.

**Amendment 1 (the important one): "everything a branch owns is above its fork anchor" is NOT an
invariant, and PB2 is what broke it.** The obvious implementation of diff — "iterate the branch's
own components, everything there is a change" — was true when the plan was written: a fork child's
writes all draw sequences above `fork_seq`. Detach violates it deliberately, by copying inherited
rows (all at or below the anchor) into tables the branch owns. So diff filters `seq > base`
EXPLICITLY rather than leaning on the invariant. Two consequences follow: a detached branch has no
parent and therefore no base, so `diff` refuses it rather than reporting its entire materialized
inheritance as "changes"; and the filter is written as a filter, not as an assumption with a
comment, because the next feature that puts a low-sequence row in a branch's own set would
otherwise silently corrupt every merge built on top.

**Amendment 2: diff reads the branch's OWN layer only** — the mirror of PB2's
`Snapshot::inherited_only`. A `Snapshot::own_only` gives exactly the rows the branch is
responsible for, with none of its ancestors'.

**Amendment 3: `SnapshotIterator` cannot be reused.** It is newest-version-per-key, which diff
wants, but it drops tombstones unconditionally (`src/snapshot.rs:1167`) — and a delete is the most
important thing a diff can report, since it is what tells a merge to remove a key. Adding an
`include_tombstones` flag would put a branch in the hot read path for a feature that path never
uses. Diff gets its own small iterator over `KMergeIterator` instead.

**Amendment 4: `get_latest_meta` moves to PD1.** The plan lists it here, but nothing in diff calls
it — it exists for merge's target-side "has this key changed since the base" check. Building it
now would add an API with no consumer and a shape guessed rather than driven by its caller, which
the project's own rules forbid. It lands in PD1 with its first real use.

**Amendment 5: diff needs its own public op type.** `InternalKeyKind` is `pub` but deliberately
never re-exported from `lib.rs` — it is an internal encoding detail (it carries `Separator`,
`LogData`, `Max`, `Invalid`). Leaking it through `DiffEntry` would make an internal enum part of
the public contract. Diff exposes `DiffOp { Set(value) | Delete | SoftDelete }` instead, which is
the complete set of things a branch can express about a key.

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
