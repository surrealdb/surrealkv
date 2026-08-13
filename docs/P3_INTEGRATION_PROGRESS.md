# P3 Existing-Engine Integration Progress

Updated: 2026-08-13

This file records the corrected P3 implementation slices. The branch-native design is being built
through SurrealKV's retained commit, WAL, memtable, SST, manifest, iterator, compaction, and
transaction machinery. Prototype modules remain test-only evidence until their semantics have been
ported into that path.

## Completed integration slices

### Inline batch and WAL owner

- Batch format v4 carries `BranchId` and `BranchGeneration` once per batch.
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
- SST format v3 persists owner metadata; the stored internal/user key is unchanged.
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

## Next slice — BR4 (gate required before starting)

BR4 (lazy runtime registry and routed writes) starts only after the standing adversarial plan
re-check against the tree at that time: verify the BR2 runtime extraction and BR3 partitioning
assumptions still hold, that chunked/lazy memtable allocation is designed against the real arena
(`vec![0; capacity]` must not be multiplied by branch count), and that write routing composes with
the BR1 dependency tracker. Amend `P4_BRANCH_RUNTIME_DESIGN.md` in writing first if any assumption
fails.

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
