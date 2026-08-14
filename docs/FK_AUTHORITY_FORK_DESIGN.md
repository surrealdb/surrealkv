# FK Phase — Durable Authority and Exact COW Fork Design

Status: IMPLEMENTED through FK5 (2026-08-14). FK1-FK5 have shipped; each slice's
as-built record, including the amendments that reversed parts of this document,
is in P3_INTEGRATION_PROGRESS.md. Where the two disagree, the as-built record is
what the code does. The largest reversal: the bounded fork-delta table (§0.10,
§1, §3.2, §3.3c, FK4) does not exist — a child inherits committed-but-unflushed
parent rows through the parent's live memtables, so C11 is vacuous under §3.2's
logical-view model.
v4 responds to the code-grounded review of v3: every blocking finding is resolved
below, with a ledger in §0 mapping finding → resolution. The structural change is
finding 2's own suggestion, adopted as primary: **inherited views are logical**
(resolved through the parent's live state), not physical table refs.
Parents: BRANCHING_CONSOLIDATED_PLAN.md, P4_BRANCH_RUNTIME_DESIGN.md (BR0-BR7
complete), P3_INTEGRATION_PROGRESS.md. Reference studies (2026-08-14): strata-core
`crates/storage`; SlateDB main/0.15.0 (v0.10.0 deltas checked); RocksDB-Cloud
notes; P0 prototype (`src/storage/local.rs`, `local_commit.rs`, `database.rs`).

## 0. Review-response ledger (v3 findings → v4 resolutions)

This ledger is the historical record of the v3 review. Items 10 and 11 were
partly superseded during implementation: the bounded delta in 10 was deleted
outright (FK4 as-built), and 11's min-age and root-catalog-version fence were
dropped from metadata GC while the catalog-version floor was repurposed as an
open-time truncation check (FK5 as-built).

1. rename-is-not-CAS → publish primitive is `fs::hard_link` with
   AlreadyExists → byte-compare → `AlreadyExistsSame`, verbatim the existing
   `src/storage/local.rs:137` mechanics (§3.4). Free idempotency for retries.
2. physical retention was false → **views are logical** (§3.2): no physical table
   refs in child metadata; `cleanup_old_tables`' immediate deletion stays legal;
   version retention (§3.3a) is the parent's only obligation. Rebuild dissolves
   into the read path; the "tolerated absence" special case is gone.
3. bottom-level hard-delete overrides pins → parent compaction runs with
   `is_bottom_level = false` while any Active child anchor exists (§3.3a), and
   FK3 gains a parent-hard-delete-after-fork sabotage twin.
4. WAL floor for state-less children → the persisted reclaim floor is ALWAYS the
   BR1 dependency-tracker snapshot (which includes every catalog-live branch's
   runtime components), never recomputed from state versions (§3.6, invariant 7).
5. seq gaps vs `F` → fork refuses while a sticky `DurableCommitApply` background
   error exists (plan C10 codified); post-restart replay closes the gap, so every
   resolvable `F` is gap-free (§3.5, invariant 6, sabotage twin in FK4).
6. missing parent generation → catalog entry carries `parent_generation` (§3.4).
7. `state_revision` vs invariant 9 → field dropped; latest = max version id.
8. `next_table_id` homeless → block-reserved watermark in root versions (§3.4);
   crash wastes at most one block; recovery resumes from the watermark, so lost
   flushes can never cause id reuse. (ULID table ids remain the adapter-phase
   option per the parent plan; not taken now — the u64 id is load-bearing across
   sstable/cache/levels and this phase does not need the churn.)
9. cap/tombstone/generation triangle → generations become globally unique from a
   monotone `next_generation` allocator in the catalog header (§3.4); equality
   fencing is unchanged; tombstones become reclaimable (GC phase) without any
   per-name state; the 4096 cap applies to live + not-yet-reclaimed entries.
10. fork-cost arithmetic → re-costed honestly in §4-FK4: one catalog version +
    at most one bounded delta (bound = parent's unsealed committed bytes ≤ its
    arena, 100 MB default for main — stated, configurable, metered); durable
    TTL pins are dropped from the local fork path — the parent plan already
    specifies process-local pins locally; parent commits are blocked only for
    the drain instant, the delta builds outside the fence.
11. append-only without GC / LIST-per-lineage opens → minimal metadata-version
    GC is IN this phase (FK5): keep-last-K + never-pinned + min-age, fenced by
    the root's catalog-version floor; open cost is O(lineages-touched) via
    state-version hints in root + SlateDB's probe-forward, no per-branch LISTs;
    the FK5 gate is raised to 1,000 branches × 10 versions each.
12. epoch bump per open → epochs bump lazily, folded into the session's first
    real catalog write; read-only opens cost nothing; flock stays the local belt.

Gaps: AtTimestamp horizon is a documented, metered quantity (§3.7). Naming split:
commit-clock values keep `_seq`; lineage file numbers are `catalog_version` /
`state_version` / `root_version`. Delta-owner identity is reserved at fork start
under the branch-op mutex and validated at commit (§4-FK4). History: forks
inherit FULL parent history ≤ F — §3.3a is a RANGE pin; the wall-clock version
retention floor is clamped to the oldest Active child anchor, metered (§3.3a).

Cheap wins folded into FK1: delete `SnapshotInfo` + `new_snapshots` /
`deleted_snapshots` (zero non-test writers; the tracker is never seeded from
disk); magic+CRC+caps on all metadata (today's manifest has neither). Scope note
budgeted: 27 `write_manifest_to_disk` call sites + `new_for_test` fixtures across
compaction/manifest tests are rewritten in FK1.

## 1. Goal, workload, non-goals

Goal: a durable branch authority and the exact COW fork protocol with all three
selectors (`Head`, `AtVersion`, `AtTimestamp`) shipping together, such that a fork
child is served — always, not just after crashes — by resolving its parent's
retained state at the fork anchor.

Designed-for workload (agentic systems): branches are numerous, cheap, mostly
ephemeral. Fork = one catalog version, no data written at all (as built).
Delete = one
tombstone version, reclaim deferred. Branches carry optional TTLs. Idle sandboxes
cost nothing in memory (BR4), metadata writes (per-branch state lineages), or
open time (hint-based resolution).

Not in this phase: merge/diff; object-store adapters (shapes are adapter-ready;
adapters are a later phase); reachability GC beyond fork pins; per-branch
snapshot trackers; public API. `LastDurable` is removed (exactly three selectors).

## 2. Reference findings (what we take)

Unchanged from v3's study table (strata-core / SlateDB / prototype), with the
v4-relevant takes: strata's catalog-commit-point + ordering; generation anchors
folded into the recovered clock; publish-failure taxonomy + decode-own-bytes +
sync-before-publish tripwire; whole-record CRC/caps/canonical-order + golden
vectors + fuzz-corpus byte-pinning. SlateDB's numbered immutable versions;
fence-check placement; probe-forward latest resolution; version-GC rules; write
batching. Prototype's `hard_link` + `AlreadyExistsSame` publish (adopted verbatim)
and its journal/dual-slots/op-ledger (retired — numbered lineages are the log).

## 3. The design

### 3.1 Authority model

The **branch catalog lineage** is the sole authority for branch existence,
generation, parent linkage, fork anchors, and TTLs. **Per-branch state lineages**
carry only that branch's OWNED durable facts (levels, flushed floor,
last_sequence) — they say nothing about inheritance and are validated against the
catalog. A **root lineage** carries global recovery facts. Fork's commit point is
one catalog version. Flush/compaction of branch B writes only B's state (+ root),
so the data plane never funnels through a global publication point.

### 3.2 Logical inherited views (primary model — v4 structural change)

A child's view of its parent is **computed, not stored**: from the catalog alone,
child C with `(parent P, parent_generation, fork anchor F)` reads as

```
C's read stack = C's memtables → C's owned levels
                 → resolve(P) capped at F → … ancestors, nearest-first, each
                 capped at min(all caps on the path)
```

where `resolve(P)` is P's LIVE runtime/state — the same structures P's own reads
use. There are no physical table references in child metadata, so parent
compaction may delete its inputs immediately (today's behavior stays legal);
soundness rests entirely on the version-retention promise (§3.3). Reads,
recovery, and "rebuild" are one code path. A fresh fork child has NO state
version at all — its state lineage appears on its first flush, symmetric with
BR4's lazy runtimes. Depth is capped (plan's 64) and metered; coalescing and
materialization (detach) are the maintenance-phase relief valves.

### 3.3 The retention promise

For every Active child C in the catalog with parent P and anchor `F`:

- **(a) Version retention (RANGE pin)**: P's compaction preserves every version
  at or below `F` that the child can read: forks inherit full history, so with
  versioning enabled the wall-clock retention floor (`iter.rs` version-drop) is
  clamped to the oldest Active child anchor, and the newest-≤F version per key
  plus plan-C1 tombstone rules are preserved unconditionally. Additionally, P's
  compaction runs with `is_bottom_level = false` while any Active child anchor
  exists — the bottom-level hard-delete drop-all path otherwise overrides
  retention (`iter.rs:1037`, `:1104-1112`). Pins derive from CATALOG anchors
  alone (never child states, never the snapshot tracker — C9). Pinned-bytes and
  clamped-retention metrics ship with the mechanism.
- **(b) Resolvability**: P's catalog entry and state lineage remain loadable
  while any Active child names P — branch deletion is REFUSED for parents of
  Active children (invariant 3). Physical file lifetime needs nothing further,
  because children hold no physical refs.
- **(c) Self-containment**: VACUOUS as built. With no delta table, C's inherited
  rows live in P's own components, and P's WAL retention already covers its own
  unflushed memtables — the same segments C reads through. C adds no retention
  obligation of its own.
- **(d) Release**: only by catalog transition — child deleted, expired (TTL),
  or detached (maintenance phase).

### 3.4 Durable objects — numbered immutable versions, format version 1
(magic + version + CRC32 + caps + canonical order; golden vectors + fuzz seeds
from FK1)

```
catalog/{version:020}.catalog   THE authority. Header: db id, catalog_version
                                (== filename), next_generation (monotone global
                                generation allocator — generations are unique
                                across all branches ever, so tombstone
                                reclamation can never enable stale-generation
                                acceptance), writer_epoch, maintenance_epoch.
                                Entries (cap 4096 live+unreclaimed): branch id,
                                name, generation, status (Active|Deleted),
                                parent, parent_generation, anchors
                                {created_at_seq, fork_seq, deleted_at_seq},
                                optional expires_at.
                                Written on branch ops, lazy epoch bumps, and
                                pin-class transitions only.
branch/{id}/{version:020}.state per-branch OWNED facts: owned levels (table
                                refs + facts), flushed replay floor,
                                last_sequence. Header: branch id, generation,
                                state_version (== filename). One version per
                                flush batch / compaction of that branch.
                                Absent until the branch first flushes.
root/{version:020}.root        global recovery facts: db id, visible_seq floor,
                                last_commit_ts + timeline tail (cap 512),
                                persisted WAL reclaim floor (from the BR1
                                tracker snapshot — §3.6), next_table_id
                                watermark (block-reserved, block 1024),
                                catalog_version floor, per-branch state_version
                                hints. Written on flush-class events, never per
                                commit.
wal/, sstables/                unchanged.
```

Resolution: latest = max version per lineage; header version must equal the
filename; corrupt latest refuses open (no N−1 fallback). Open reads root latest
→ follows state hints, probing forward (SlateDB pattern) for versions published
after the last root write; only catalog and root lineages are LISTed.

Publish primitive (local): temp write → fsync → `fs::hard_link(temp, final)` →
unlink temp → dir fsync; `AlreadyExists` → byte-compare → `AlreadyExistsSame`
(idempotent success) or fail — verbatim `src/storage/local.rs:137` mechanics.
This IS conditional-create; rename is not used and its silent-overwrite hazard
does not exist. Object adapters use `PutMode::Create`. Every publish classifies
into strata's three failure windows; `VisibilityUnknown` forces
reload-before-classify. Decode-own-bytes before publish. Sync-before-publish
tripwire in tests.

Fencing: local flock (existing) as the machine belt. `writer_epoch` /
`maintenance_epoch` live in the catalog header and bump LAZILY — folded into the
session's first real catalog write; a session that never writes the catalog
bumps nothing. Checks run before preparing any candidate, before publish, after
refresh. Generation anchors fence replay (BR6 mechanism over durable anchors).

### 3.5 Fork commit rules

`F` resolves only from a clean pipeline: the drain requires
`visible_seq == log_seq - 1` AND no sticky `DurableCommitApply` background error
(plan C10) — an apply-failed seq makes fork refuse with a typed error until
restart, where replay re-applies the WAL-durable batch and closes the gap. Thus
every anchor `F` ever committed is gap-free: capping at `F` yields the same rows
before and after any crash.

### 3.6 WAL floor rule

The persisted reclaim floor (root) is ALWAYS taken from the BR1 dependency
tracker's snapshot — which includes every catalog-live branch's runtime
components, including children that have never flushed (no state lineage) —
never recomputed as min-over-state-files. Recovery validates each state's
flushed floor ≥ nothing it shouldn't (states are per-branch facts); segment
reclamation uses the root floor alone.

### 3.7 AtTimestamp horizon

`AtTimestamp` is exact-or-abstain within the covered horizon
`[oldest retained fencepost, last_commit_ts]`. The horizon is a first-class
metered quantity (root carries the tail; sealed history extends it in the
maintenance phase); below-horizon requests fail typed, never approximate.

### 3.8 Deletions (complete-new-design mandate; no shims)

FK1 deletes: the single `LevelManifest` file + `write_manifest_to_disk` (all 27
call sites + test fixtures rewritten); `SnapshotInfo` and the manifest snapshot
list; the BR6 injected-catalog builder seam. Retired unported: prototype
journal/dual-slots/op-ledger; SlateDB's `initialized` flag; rename-replace
manifests; CURRENT pointers; `LastDurable`. The in-memory owner-partitioned
`LevelManifest` remains the runtime aggregate, hydrated from state versions.

## 4. The FK slice ladder

Standing rules: adversarial gate per slice; two-level test gate (sabotage twins +
full vertical suite); golden vectors when formats stabilize; deferrals name
owners.

- **FK1 — Numbered-lineage authority.** Three formats + hard-link publish
  primitive with failure taxonomy; global generation allocator; lazy epochs;
  block-reserved table-id watermark; state/root split replacing the manifest
  file; recovery = catalog → states-by-hint (validated: unknown branch, id/
  generation/filename mismatch, catalog-version floor regression → refuse) →
  root facts → BR6 replay fencing off durable anchors; clock folds catalog
  anchors. Durable create/delete/recreate (still `pub(crate)`). Deletions per
  §3.8. Sabotage: corrupt each latest (refuse); cap breach (typed); crash
  between state and root publish (hints probe forward); zombie writer (epoch +
  taken-id); duplicate publish (`AlreadyExistsSame` idempotency); generation
  allocator monotone across delete/recreate.
- **FK2 — Commit timestamps + timeline (C6).** Fixed-width `commit_ts` in the
  batch header (named header constants), stamped under the write mutex, monotone
  clamp; timeline tail in root; WAL-header rebuild on recovery; exact-or-abstain
  `ts → seq`; the §3.7 horizon metric.
- **FK3 — Logical views, capped reads, the retention promise.** Read-stack
  resolution through the catalog (§3.2) wired into BR5's snapshot capture;
  child compaction `is_bottom_level = false` while views exist (C1); parent
  compaction: catalog-anchor pins enforce §3.3a — range pin, retention-floor
  clamp, and parent `is_bottom_level = false` while Active child anchors exist.
  Sabotage twins: parent compaction without the pin violates a planted
  assertion; WITH the pin, newest-≤F versions and C1 tombstones survive;
  **parent hard-deletes a key after F, compacts to bottom — the child still
  reads the pre-F version** (the finding-3 twin); above-cap rows stay invisible;
  child hard-deletes never resurrect inherited rows; history reads below F
  return the parent's full version chain.
  As built (2026-08-14), two amendments: the pin has TWO shapes — range under
  versioning, newest-≤F otherwise — both independent of live snapshots (a single
  shape is either wasteful or non-deterministic; see the FK3 as-built record);
  and compaction RE-VALIDATES the floor under the manifest write lock at
  publication, refusing with `CompactionPinRaced` if a fork lowered it mid-merge.
  That re-validation is what makes FK4's fork safe against an in-flight
  compaction, so FK4 needs no additional coordination with the compactor beyond
  its own floor check.
- **FK4 — Exact fork, all three selectors.** Under the branch-op mutex: validate
  names/generations/destination absence; reserve child identity (id +
  generation from the allocator's current value, CAS-checked at commit);
  register the PROCESS-LOCAL fork pin (parent-plan posture: crash before the
  catalog commit aborts the fork, nothing durable leaks — compaction consults
  live fork-ops for §3.3a during the window); drain to `visible_seq` under the
  commit fence and resolve `F` per selector (`Head` = drained visible;
  `AtVersion` ≤ visible and ≥ floor; `AtTimestamp` via §3.7); release the
  fence; build + publish the bounded delta OUTSIDE the fence when committed
  rows in `(durable_table_seq, F]` exist (C11; bound = parent's unsealed
  committed bytes ≤ its arena — 100 MB default for main, configurable,
  metered); commit ONE catalog version (child entry + anchors +
  parent_generation + delta_ref) — the commit point; `ForkReceipt`. No child
  state version exists or is needed. Honest cost statement: parent commits are
  blocked for the drain instant only; a hot-parent fork pays the delta build.
  Idempotency: retry with same fork identity → catalog lookup returns the
  original receipt; divergent parameters fail closed; delta re-publish is
  `AlreadyExistsSame`. Sabotage twins: crash before commit (orphan delta only —
  at-open orphan deletion reclaims it; no durable branch); crash after commit
  (child reads correctly through §3.2 with zero further writes); concurrent
  parent commits during delta build (not blocked, invisible to child); fork
  during parent compaction (live pin holds); duplicate fork (idempotent);
  apply-failure gap (fork refused typed, §3.5); fork-of-fork chain reads
  (flattening replaced by bounded chain resolution — depth cap enforced).
  As built (2026-08-14), one reversal and three amendments: the bounded delta
  table and `CatalogEntry.delta_table` are DELETED — a child inherits
  committed-but-unflushed parent rows through the parent's live memtables under
  its cap, so C11 is vacuous under the v4 logical-view model and every fork
  point is exact (no `LastDurable`); the drain condition is the commit QUEUE,
  not `visible_seq_num == log_seq_num - 1`, which a failed WAL append makes
  permanently unreachable; `retained_floor_seq` lands here rather than later,
  because `AtVersion`/`AtTimestamp` are unsafe without it; and the commit point
  takes `level_manifest.read()` before `branch_catalog.write()`, interlocking
  with FK3's compaction-side re-validation so whichever publishes second fails
  closed.
- **FK5 — Lifecycle, TTLs, metadata GC, scale.** Ancestor deletion refused for
  parents of Active children (§3.3b); branch TTL expiry via maintenance
  tombstones; minimal metadata-version GC (keep-last-K + never-pinned +
  min-age, fenced by the root catalog-version floor); typed
  `BelowRetentionFloor` / `BranchesUnrelated` / `MaterializationRequired`;
  depth/coalescing budgets with metrics; C10 fences. Scale gates: 1,000
  branches × 10 metadata versions each open within budget; 100 forks;
  1,000-idle-branch memory unchanged (BR4).

  As built (2026-08-14): keep-last-K metadata GC only (`KEEP_METADATA_VERSIONS
  = 4`) — no min-age and no reader fence, because recovery reads only the newest
  version, hints probe forward, and a decoded reader holds its bytes; TTL expiry
  runs at the tail of the maintenance wake, never on a timer (D10 seam 3); and
  expiry does not cascade — an expired parent with Active children is skipped
  until the last child goes. `catalog_version_floor` was kept and WIRED rather
  than removed: it is the input to a real open-time check for a catalog lineage
  truncated under a root that already depended on it, and pruning cannot trip it
  because it only removes versions older than the newest. `BranchesUnrelated`
  stays with diff/merge, its only producer.

Deferred with owners: reachability GC + release worklists (file-lifetime phase —
now simpler: no child physical refs); materialization/detach as the §3.3d
release valve + chain-depth relief (maintenance phase); per-branch snapshot
trackers (maintenance); object adapters incl. epochs-over-real-CAS (adapter
phase; ULID table ids optional there); public API (after FK5).

## 5. Amendments to the consolidated plan (explicit reversals, per review)

- §4.4 single-atomic-root → catalog-commit-point + subordinate lineages (v2).
- §4.3 "forking flattens views … does not ask its parent later" → REVERSED:
  views are logical and resolve through the parent's live state; soundness from
  §3.3; flattening returns only as maintenance-phase materialization.
- §5.3 "recovers entirely from the child root without the parent" → REVERSED:
  a child is served through its parent by design; the parent is guaranteed
  resolvable by §3.3b.
- §5.3 "crash before publication leaves no durable pending branch" → KEPT
  (v4 drops durable fork pins locally; the pin is process-local again).
- §5.4 "children reference exact immutable parent tables; parent compaction
  need not retain logical versions" → REVERSED: retention is logical
  (versions), not physical (files); §3.3a is the binding rule.
- §3.2 "no independently authoritative per-branch HEAD objects" → AMENDED:
  state lineages resolve a branch's durable head, but they are subordinate
  (validated against, and meaningless without, the catalog); the catalog
  remains the single authority — no split-brain surface.
- §5.6 fail-closed GC posture → KEPT and simplified: any Active branch's
  metadata failing to load refuses the open outright (there is no
  tolerated-absence mode anymore; a never-flushed branch legitimately has no
  state lineage, which is not an absence of required metadata).

## 6. Invariants

1. The catalog lineage is the sole authority for existence, generation,
   parentage, anchors, TTLs; states are subordinate, validated against it.
2. Ordering: no metadata version publishes while depended-on WAL bytes are
   unsynced. (The delta-before-catalog half of this invariant retired with the
   delta table itself.)
3. A parent of an Active child cannot be deleted; its catalog entry and state
   lineage remain loadable (resolvability, §3.3b).
4. Parent compaction preserves the §3.3a pin: under versioning, every version
   at or below the oldest Active child anchor; otherwise the newest version at
   or below it. C1 tombstone rules included, with `is_bottom_level = false`
   while any Active child anchor exists. Whichever of {fork, compaction}
   publishes second re-checks the other and fails closed
   (`CompactionPinRaced` / `BelowRetentionFloor`).
5. Rows above a view's cap are unreadable through that view on every path.
6. Every committed fork anchor `F` is gap-free (§3.5); capping at `F` yields
   identical rows before and after any crash.
7. The persisted WAL reclaim floor derives from the dependency-tracker snapshot
   over all catalog-live branches, never from state files alone.
8. Generations are globally unique and monotone (allocator in the catalog
   header); equality fencing is therefore stable across tombstone reclamation.
9. No metadata file is rewritten in place; every lineage is numbered immutable
   versions; header version equals filename; corrupt latest refuses open.
10. The catalog changes only on branch ops, lazy epoch bumps, and pin-class
    transitions; a branch's state changes only on that branch's
    flush/compaction; root changes only on flush-class events.
11. Fork retries are idempotent (catalog lookup + `AlreadyExistsSame`);
    divergent parameters fail closed.
12. Table ids allocate from the root watermark's reserved block; recovery
    resumes at the watermark; ids are never reused.

## 7. Resolved questions (user decision 2026-08-14: functionality parity with strata)

1. **Forks inherit full parent history ≤ F** (range pin, §3.3a as written).
   Strata's children read the parent's version history through the fork cap and
   it offers no history-stripping fork option; neither do we. The clamped
   wall-clock retention floor ships with pinned-bytes + clamped-floor metrics.
2. **Catalog cap = 4096 live + unreclaimed entries, fail-closed typed error at
   4097** — strata's exact posture (no degradation, no silent eviction).
3. **Chain depth cap = 64 with `MaterializationRequired` beyond** (the parent
   plan's D6 number; strata's functional equivalent — deep chains work up to a
   hard cap, beyond it the system demands materialization rather than
   degrading). Depth is metered from FK3.

Status: design FINAL (v4, questions resolved). FK1 proceeds under the standing
per-slice adversarial gate.
