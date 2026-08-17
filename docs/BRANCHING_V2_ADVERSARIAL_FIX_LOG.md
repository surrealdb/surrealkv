# Branching v2 adversarial fix log

Status: implementation fixes verified; persistent catalog subobjects remain a format-design item

This log records the 2026-08-17 adversarial review of the shipped `v2` branching runtime. Every
correctness fix uses a red/green gate: a vertical regression is added and observed failing before
production code changes, then the same regression and its focused subsystem suite must pass. A
helper-only test is supplementary evidence, not the correctness gate.

## Work ledger

| Area | Defect | State |
| --- | --- | --- |
| Merge | Incremental edge treated target-only values as a future common base | Fixed and verified |
| Catalog | Deleted records permanently consume the 4,096-entry durable cap | Fixed and verified |
| Restore | Non-default runtimes survive a whole-database restore | Fixed and verified |
| Checkpoint | Files and authority are copied without one consistent publication cut | Fixed and verified |
| Fork | Historical child may predate its parent's creation | Fixed and verified |
| WAL | Sync/rotation failures leak the in-flight dependency pin | Fixed and verified |
| Authority | Restore hydrates levels with a different authority than later catalog publication | Fixed and verified |
| Locking | Merge-edge publication reverses checkpoint/restore's catalog/level lock order | Fixed and verified |
| Reads | Owner level lookup linearly scans all owners | Fixed and verified |
| Root publish | State-hint construction is quadratic | Fixed and verified |
| Merge performance | Preflight point-probes every changed key | Fixed and verified |
| Memory | Write-buffer “budget” is a pressure trigger, not a hard bound | Contract corrected and verified |
| Detach | Catalog publication lock spans full inherited-data materialization | Fixed and verified |
| Metrics/API | WAL pin, retained-version, and timestamp-retry contracts are inaccurate | Fixed and verified |
| Catalog publish cost | Every branch mutation re-encodes the entire live catalog | Open: persistent-subobject format phase |

## Adjudication of the disputed merge finding

The feedback correctly observed that `EffectiveBase` already had separate `source_through` and
`target_at` fields. That observation does **not** refute the defect. The bug was which branch supplied
the value side of the later three-way base: `MergeSession::new` read `target` at `target_at`. A merge
changes only the target, so target-only values present at that edge were never incorporated into the
source. Treating them as a common base makes a later source edit look uncontested and overwrites the
target.

The two vertical regressions are the deciding evidence. One uses a no-op first merge; the other
successfully merges an unrelated key first. Both failed on the old code by applying the later source
write without a conflict. Git's analogous DAG also chooses the previous source tip as the merge base:
the target-side merge commit has that tip as a parent, but the target's pre-merge tip is not an
ancestor of the continuing source branch. The earlier report's cited line number was stale, but its
semantic finding was real.

## M1 — incremental merge base correctness

### Broken invariant

Merging source into target does not mutate source. Therefore a prior merge edge may consume source
changes, but it cannot globally turn target-only values into source history. The source snapshot at
`source_through_seq` is the base for a later three-way comparison. The target head at
`target_through_seq` is separate durable edge history.

Both caps must remain exact across compaction. A target stores the edge, so catalog retention derives
the target-side pin from that record and the source-side pin from every live incoming edge.

### Red evidence

Before the production change:

```text
cargo test no_op_merge_does_not_reconcile_unseen_target_changes -- --nocapture
FAILED: expected MergeConflicts; got MergeOutcome { applied: 1, ... }

cargo test merge_of_an_unrelated_key_does_not_reconcile_target_changes -- --nocapture
FAILED: expected MergeConflicts; got MergeOutcome { applied: 1, ... }

cargo test anchors_come_from_live_children_and_live_merge_edges -- --nocapture
FAILED: source owner had RetentionAnchors { anchors: [] }, expected prior source cursor
```

The second vertical test proves the defect was not confined to the zero-write path: a successful
merge of an unrelated source key also advanced the target baseline past an untouched target-only
value.

### Fix

- `MergeSession` materializes the source snapshot at `source_through_seq` as its base.
- Point probes compare that source base with target-now.
- Scan probes cover target-owned changes from the original fork point, not merely from the last
  target edge, so older target-only changes cannot disappear from large-merge classification.
- Catalog retention pins `source_through_seq` on the live source owner as well as
  `target_through_seq` on the target owner.
- Incremental merge preconditions fail closed if the pinned source cap is no longer exact.

### Green evidence

```text
cargo test reconcile -- --nocapture
2 passed

cargo test anchors_come_from_live_children_and_live_merge_edges -- --nocapture
1 passed

cargo test source_compaction_keeps_the_previous_merge_base_exact -- --nocapture
1 passed

cargo test merge_tests -- --nocapture
43 passed
```

The compaction regression creates five source-owned L0 tables, compacts past the prior source
cursor, and verifies that a target revert still conflicts instead of being overwritten.

## C1 — catalog tombstone exhaustion

### Broken invariant

Deletion needs a durable tombstone until the deleted owner's runtimes, WAL dependencies, level
state, tables and authority lineage are reclaimed. It does not need that record forever. Keeping
every tombstone in every later catalog made the lifetime create count, rather than concurrent live
branch count, consume the 4,096-entry format cap.

### Red evidence

```text
cargo test the_sweep_retires_reclaimed_catalog_tombstones -- --nocapture
FAILED: after a complete sweep all_records() was 2, expected 1
```

The test writes and flushes a fork, deletes it, runs the real maintenance sweep, checks the
in-memory catalog and state-lineage removal, reopens the store, checks the durable catalog, and
reuses the retired name.

### Fix

- The sweep first reclaims runtimes/WAL dependencies and owner levels, then removes the deleted
  owner's now-unreachable state lineage and syncs its parent directory.
- Only after those steps does it retire deleted catalog records and stale merge edges in a new
  durable catalog publication.
- `next_generation` remains monotone, so a repeated BranchId cannot validate an old physical owner.
- Catalog-growing operations perform a synchronous maintenance pass when already at the format cap,
  preventing a burst from permanently outrunning background maintenance.
- Public deletion wakes the maintenance worker.

### Green evidence

```text
cargo test the_sweep_retires_reclaimed_catalog_tombstones -- --nocapture
1 passed

cargo test sweep -- --nocapture
8 passed
```

The existing 40-cycle churn regression now additionally requires `all_records() == 1`, rather than
checking only the live-branch iterator that hides accumulated tombstones.

The red test was subsequently strengthened with a branch-owned SST. It exposed a second lifecycle
leak: dropping the catalog record left its now-unreachable state-lineage directory forever. The
fixed order is runtime/WAL/level/root reclamation, state-lineage removal plus parent-directory sync,
then tombstone retirement. A failure before the last step leaves the tombstone to make maintenance
retry safely.

## R1 — whole-database restore state

### Red evidence

```text
cargo test checkpoint_restore_discards_non_default_runtime_writes_permanently -- --nocapture
FAILED: an unrelated post-restore commit made "discarded-future" visible again

cargo test cross_database_restore_keeps_one_authority_identity_after_new_publications -- --nocapture
FAILED on reopen: root manifest carries a foreign database identity
```

### Fix and green evidence

Restore now takes locks in the global catalog-then-commit order, resets the runtime registry to the
default runtime before WAL recovery rebuilds owners, swaps `CoreInner` to the restored authority,
and reconnects root and catalog publication to one shared restored catalog-version handle.

```text
cargo test checkpoint_restore_discards_non_default_runtime_writes_permanently -- --nocapture
1 passed

cargo test cross_database_restore_keeps_one_authority_identity_after_new_publications -- --nocapture
1 passed
```

## K1 — checkpoint consistent cut

### Red evidence

```text
cargo test checkpoint_holds_catalog_fence_across_the_consistent_cut -- --nocapture
FAILED: catalog publication remained possible while checkpoint files were being cut
```

The regression holds the level manifest to park checkpoint creation after destination creation,
then observes the catalog mutex. This makes the missing fence deterministic without depending on a
probabilistic corrupt-checkpoint race.

### Fix and green evidence

Checkpoint creation now holds the catalog fence, drains/holds the commit pipeline, flushes every
runtime, and holds the level manifest read guard across both the referenced SST copy and authority
lineage copy.

```text
cargo test checkpoint_holds_catalog_fence_across_the_consistent_cut -- --nocapture
1 passed
```

## F1 — nested historical fork birth bound

### Red and green evidence

```text
cargo test nested_fork_before_the_parent_s_creation_is_refused -- --nocapture
RED: created child at sequence 0 under a parent created at sequence 1
GREEN: 1 passed; no child was published
```

Every resolved `AtVersion` or `AtTimestamp` fork sequence is now checked against the selected
parent record's `created_at_seq` before retention validation and catalog publication.

## W1 — in-flight WAL dependency cleanup

### Red and green evidence

```text
cargo test a_failed_wal_sync_releases_its_in_flight_dependency -- --nocapture
RED: in_flight_count was 1 after the failed commit, expected 0
GREEN: 1 passed
```

The fault is injected after append and before immediate-durability sync. Append, sync and
size-driven rotation now form one fallible region; every error cancels the commit pin and clears
the prepared segment, while success leaves the pin for atomic memtable handoff.

## L1 — catalog/level lock inversion

Checkpoint and restore take `catalog_publish` before entering level-manifest work. Merge-edge
publication took the same pair in reverse, so the two paths could hold one lock each forever.

```text
cargo test merge_edge_publication_obeys_the_catalog_then_level_lock_order -- --nocapture
RED: record_merge_edge takes level_manifest before catalog_publish
GREEN: 1 passed
```

Merge-edge publication now takes the catalog serializer first, then holds the level read guard
across publication. It therefore preserves the retention-anchor exclusion without reversing the
whole-database cut's order.

## P1 — constant-time owner-level lookup

`LevelManifest::levels_for` used `Vec::iter().find`, putting total branch count into every owner
point read and every layer of an inherited read. `levels_by_owner` is now a
`HashMap<BatchOwner, Levels>`; lifecycle iteration sorts owner keys where deterministic order is
required.

```text
cargo test point_reads_use_constant_time_owner_level_lookup -- --nocapture
RED: LevelManifest stored owner levels in a linearly searched collection
GREEN: 1 passed
```

## P2 — linear root-hint construction

Root publication previously searched the accumulated hint vector for every state owner. It now
selects the newest generation in a hash map and sorts the final branch list once.

```text
cargo test root_state_hints_are_built_without_a_quadratic_scan -- --nocapture
RED: persist_root built hints with hints.iter_mut().find(...)
GREEN: 1 passed
```

## P3 — bounded merge probing

Large-merge preflight performed two target point reads for every source change even though apply
already had a sequential scan probe. Preflight now counts the fixed source diff sequentially and
uses the scan probe at `SCAN_PROBE_THRESHOLD`, retaining the point path for small merges.

```text
cargo test large_merge_preflight_selects_the_scan_probe -- --nocapture
RED: preflight unconditionally constructed PointProbe
GREEN: 1 passed
```

The semantic equivalence and end-to-end merge gates remain green in the 43-test merge suite.

## P4 — detach publication critical section

Detach held `catalog_publish` while copying the inherited dataset. A separate materialization mutex
now excludes delete, reclamation, checkpoint and restore during the copy; `catalog_publish` is taken
only for the final parent-link publication. Other metadata publications no longer wait for the
dataset copy.

```text
cargo test detach_does_not_hold_catalog_publication_lock_while_copying_data -- --nocapture
RED: detach acquired catalog_publish before materialize_inherited
GREEN: 1 passed
```

## P5 — write-buffer contract

The old `write_buffer_budget` name promised a bound the arena design does not provide. Rotation
does not release the immutable arena until asynchronous flush completes, and an oversized batch
may require an arena larger than the configured number. The public option and builder are now
`write_buffer_soft_limit`; documentation states that it triggers pressure rotation and is not a
hard resident-memory cap.

```text
cargo test write_buffer_pressure_configuration_is_not_advertised_as_a_hard_budget -- --nocapture
RED: public API still exposed write_buffer_budget
GREEN: 1 passed
```

A true cap requires admission/backpressure accounting over active, immutable and in-flight flush
memory; that is a separate allocator design, not a truthful one-line repair.

## O1 — metrics and retry contracts

- `wal_pinned_segments` now counts distinct referenced segment IDs, not dependent memtables.
- The cumulative compaction counter is named `pin_retained_versions_total`; it is not presented as
  a current on-disk gauge.
- An idempotent `AtTimestamp` fork retry re-resolves the timestamp and refuses it when the sequence
  differs from the existing fork, matching `AtVersion`.

```text
cargo test wal_pinned_segments_counts_distinct_segments_not_memtables -- --nocapture
RED: metric was 2 for two memtables on one segment; expected 1
GREEN: 1 passed

cargo test fork_retry_with_a_different_timestamp_is_refused -- --nocapture
RED: returned the existing fork receipt for a timestamp resolving to another sequence
GREEN: 1 passed
```

## Remaining format work — catalog publication cost

Tombstone retirement removes the correctness/capacity failure, but every catalog mutation still
clones and encodes all live branch records. That O(B) publication cost is not marked fixed. The
canonical rewrite already selects persistent immutable metadata subobjects under one authoritative
root; that phase needs a format migration, crash/lost-response tests and branch-count benchmarks.
It should not be smuggled into the current numbered-manifest format as an unverified local patch.

## Final verification

```text
cargo test architecture_guard_tests -- --nocapture
20 passed

cargo test merge_tests -- --nocapture
43 passed

cargo test fork_view_tests -- --nocapture
38 passed

cargo test branch_runtime_tests -- --nocapture
23 passed

cargo test lsm_tests -- --nocapture
67 passed

cargo test fault_injection_tests -- --nocapture
8 passed

cargo test metrics_tests -- --nocapture
7 passed

cargo test --lib
1173 passed; 0 failed

cargo clippy --all-targets --all-features -- -D warnings
clean
```
