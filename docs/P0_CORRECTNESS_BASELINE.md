# P0 correctness baseline

Status: implemented baseline for the branch-native rewrite.

## Ship decision

The current inline-only LSM remains deployable until the P3 single-branch spine replaces it.
Therefore the four known P0 failures are fixed in this runtime and retained as regressions; they are
not merely deferred to the future testkit.

## Invariant cards

1. **One value representation.** Values are raw inline bytes in WAL, memtables, and SSTs. There is
   no VLog, value pointer, prefix discriminator, or resolution path.
2. **LSM only.** No B+tree module, option, durable format, or fallback implementation may return.
3. **Branch-pure rows.** Branch ownership belongs to component metadata. Branch identity must not be
   prefixed to user or internal row keys.
4. **One commit order.** A database has one monotonically ordered commit-version timeline. Timestamp
   is a selector/commit fact, not an independent MVCC order.
5. **One authority transition.** Global version, timeline, branch catalog/state and durable pins are
   published through one logical root transition and fence.
6. **Exact fork or explicit refusal.** A fork resolves the requested head/version exactly. It must
   not silently fall back to a dataset copy or an approximate mutable-parent view.

## Correctness regressions

- Bottom compaction retains `DELETE@100` and `PUT@30` while snapshot `@50` is live. Retaining the
  value without its covering tombstone is also forbidden because it resurrects current state.
- WAL append records the actual segment identity. A delayed post-rotation apply lowers the receiving
  memtable's replay floor, so cleanup cannot remove the acknowledged commit's only durable copy.
- When one WAL segment produces multiple recovery memtables, flushing a partial chunk publishes the
  next unflushed chunk's WAL as the replay floor. It never publishes `segment + 1` prematurely.
- Snapshot registrations are a multiset keyed by `(version, unique registration)`. Dropping one of
  two snapshots at the same version cannot release the other's compaction pin.

### Test evidence map

| Invariant | Direct owner regression | Vertical boundary regression |
| --- | --- | --- |
| Bottom tombstone with snapshot | `test_bottom_tombstone_preserves_pre_delete_snapshot_visibility` | `live_pre_delete_snapshot_survives_real_bottom_level_compaction` |
| Delayed WAL apply | `append_returns_the_actual_segment_identity`, `delayed_apply_lowers_memtable_wal_replay_floor`, `pending_delayed_apply_blocks_manifest_replay_floor_advance` | `delayed_apply_survives_rotation_flush_cleanup_and_reopen` |
| Split-segment replay floor | `split_segment_does_not_advance_replay_floor_past_unflushed_chunk` | `crash_after_first_split_recovery_flush_replays_the_remainder` |
| Counted snapshot registration | `test_duplicate_snapshot_versions_are_counted_independently` | `duplicate_live_transactions_keep_independent_snapshot_pins` |
| Removed storage paths | `removed_storage_engines_remain_absent` | Full build/test proves the only reachable runtime is the inline LSM |

All future phases use the same direct-plus-vertical evidence rule from the canonical plan. A phase
cannot be called complete based only on helper tests or a green full suite.

## Recorded baseline

Baseline commit before these fixes: `4b07fce` (`remove vlog`). The full correctness suite is the
required behavioral baseline; intentionally retired tests are only the VLog-specific tests removed
with that commit.

Allocation benchmark command:

```text
cargo bench --bench alloc_bench -- --sample-count 10 --max-time 2
```

Observed on 2026-08-13 (Apple arm64, optimized build; ten samples):

| Operation | Median | Median throughput | Stable allocation fact |
| --- | ---: | ---: | ---: |
| `seq_get/10000` | 2.220 ms | 4.502 Mitem/s | 20,002 allocations / 800 KB |
| `seq_insert/10000` | 5.805 µs per sampled transaction | 1.722 Gitem/s reported | median 15.5 allocations |
| `seq_range/10000` | 473.9 µs | 21.09 Mitem/s | 10,021 allocations / 320.7 KB |

The insert fixture's throughput counter represents the configured item count per sampled
transaction and should only be compared using this exact command. Benchmark noise is not a release
oracle; later phases must report repeated distributions and allocation deltas.
