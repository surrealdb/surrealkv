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
