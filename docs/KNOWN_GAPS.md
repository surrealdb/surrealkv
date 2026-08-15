# Known gaps

The companion to `docs/removed-surfaces.md`. That file records what this rewrite **deleted**; this
one records what it deliberately **did not do**, so a decision taken with reasons does not decay
into an oversight nobody can date.

Every entry states four things: **what** the gap is, **why** it was left, **what it would take** to
close, and **what would raise its priority**. An entry with no trigger is not a decision, it is a
shrug — if you cannot name what would change the answer, the answer was probably never made.

Where an entry names a global, `src/test/architecture_guard_tests.rs`'s
`no_ambient_state_outside_the_written_allowlist` holds the machine-checked version, keyed
`file:NAME`. That guard fails both ways: an unlisted global fails it, and so does a listed global
that no longer exists. So these reasons cannot outlive the code they describe.

Written 2026-08-15 (v2, slice V8a-4); §6 added in V8b, this note in V9.

Related: `docs/PATTERNS.md` states the injection and testing patterns these gaps are measured
against, and `docs/BRANCHING.md` is the user-facing branch semantics.

---

## 1. ~~`Tree::flush` is `#[cfg(test)]`~~ — CLOSED, and the original claim was wrong (G1, 2026-08-15)

**What this entry used to say.** That the suite's durability path was "a synchronous path production
never runs", so "a defect that only appears in the asynchronous flush pipeline is invisible to all
206" call sites.

**That was overstated, and measuring it is what closed the gap.** The background worker's
`compact_memtable` is:

```rust
fn compact_memtable(&self) -> Result<()> {
    self.flush_oldest_immutable_to_sst().map(|_| ())
}
```

— literally the function the synchronous drain calls in a loop. **The work is identical.** There was
never a second code path. What the synchronous drain removes is *everything else happening at the
same time*: the caller is blocked inside the flush, so no read, no rotation, no branch operation and
no compaction can be in flight while it runs.

So the blind spot was **concurrency, not a divergent code path** — and converting 207 call sites
would not have covered it. Only tests that deliberately put something *alongside* a flush do.

**What was done instead.**

- **`Tree::flush` is now `Tree::drain_flushes_synchronously`.** Same behaviour; a name that says it
  bypasses the scheduler, plus a doc comment on when not to reach for it. All 207 sites renamed.
- **`Tree::flush_and_wait`** goes through the real worker: rotate, `wake_up_memtable` (production's
  own signal), then wait for the flush counter to advance and the backlog to empty. It is `async`
  because the worker is a spawned task and tests run on a current-thread runtime by default — a
  blocking wait would never let the worker run.
- **`BranchMetricsSnapshot::memtable_flushes`**, a real production counter (RocksDB exposes the
  same), incremented at the single point where a memtable becomes a table. Not `#[cfg(test)]`.
- **`support::{flush_branch_and_wait, rotate_branch_and_signal}`** for per-branch flushes, the
  latter deliberately not waiting so a test can race something against a flush in flight.
- **`src/test/flush_concurrency_tests.rs`** — seven tests: branch delete racing that branch's own
  flush, compaction racing an unpublished flush, WAL reclamation under concurrent flushes, writes
  under queued flushes, reads racing a rotation, and two on the barrier itself.

**What is honestly left.** The 207 renamed sites still use the synchronous drain, and that is the
recommendation, not a shortcut: they want a quiescent store, and the code they exercise is the same
either way. The residual risk is that a *new* test reaches for the synchronous drain when it wanted
the pipeline — which the rename and the doc comment are there to prevent.

**Two findings recorded from doing it:**

- A probe that made the barrier return without waiting left all five concurrency tests **green** —
  because `rotate` and `wake_up_memtable` fire before the wait, so the race still happens. Those
  tests check invariants under concurrency; `the_barrier_returns_only_once_the_flush_has_produced_a_table`
  is what checks the barrier. The probe refusing to redden is what made the distinction visible.
- The branch-delete test originally called `Tree::flush_and_wait`, which rotates only the **default**
  runtime — so it raced `main`'s flush while claiming to race the branch's.
  `rotate_branch_and_signal` exists because of that.

## 2. `Error::ForkFenceTimeout` is unreachable by construction — RE-SCOPED (G5, 2026-08-15)

**What this entry used to say.** That `ForkFenceTimeout` was the one branch-reachable error with no
test, and that reaching it needed a commit stalled between enqueue and apply plus an injectable
deadline.

**Half of that is now done, and it produced a better answer than a test.**

**Done: the deadline is configurable.** `FORK_DRAIN_TIMEOUT` was a hardcoded 5-second constant; it
is now `Options::fork_drain_timeout` with `TreeBuilder::with_fork_drain_timeout`. That removes the
last hardcoded timing constant in the branch path and gives the async port something to tune when it
revisits the fence (handover open question 1).

**Found: the error cannot currently be produced.** Its own doc comment says it guards against "a
commit whose future was dropped between enqueue and apply". In `CommitPipeline::commit` the awaits
are at the stall check, the semaphore acquire, and the completion receiver — and the last is *after*
`publish()`. Between `pending.enqueue()` and `publish()` there is **no suspension point at all**:
enqueue, apply and publish run synchronously under a `parking_lot` mutex. A future cannot be dropped
mid-span, so a batch cannot be stranded, so the fence always finds a drained pipeline.

Measured, not assumed: a test asserting a timeout must occur caught **zero in 200 fork attempts
against 217 concurrent commits**, with the timeout set to zero — the most hostile setting available.
Its non-vacuity assertion is what surfaced this rather than letting a vacuous test pass.

**Why the guard stays rather than being deleted.** It is not dead code — the fence does construct
it — it is an unreachable branch. And it becomes reachable the moment this path grows an `await`,
which is precisely what the async port does to the publish paths. Deleting it now and rediscovering
the need later is the wrong trade.

**What exists instead of a timeout test.**
`forking_under_load_with_a_zero_drain_timeout_stays_consistent` asserts the invariant that does
hold: with a zero timeout under concurrent commits, every fork either succeeds or fails with exactly
`ForkFenceTimeout` and nothing else, and a fork that reported failure leaves no branch behind.

**What would change the answer.** The async port introducing a suspension point between enqueue and
publish. At that moment the state becomes producible, and this becomes an ordinary test to write —
gate it on that.

## 3. The skiplist draws from an ambient RNG

**What.** `src/memtable/skiplist.rs:455` calls `rand::rng()` per insert to pick a node's tower
height.

**Why kept** (user decision, 2026-08-15). It affects in-memory structure only — never visible
semantics, never iteration order, never anything durable. A dispatched call per insert on the
hottest path in the engine is not worth determinism nothing currently consumes.

**What it would take.** An `Arc<dyn RngSource>` on `Options`, or a seeded generator per memtable.
The per-memtable form is cheaper and probably right.

**What would raise its priority.** Deterministic simulation becoming a goal. Tower height determines
memtable size accounting, which determines flush timing, which determines SST boundaries and
compaction scheduling — so its absence would make replays diverge, and it would have to be first,
not last.

## 4. Minted identities fold in a process-global counter and the pid

**What.** `BRANCH_ID_COUNTER` (`src/lsm.rs:106`) plus the pid go into minted branch and database
identities.

**Why kept.** `mint_branch_id` already loops against the catalog until it finds an unused id, so the
counter is a collision *reducer*, not a correctness dependency. Nothing reads an id for meaning.

**What it would take.** Deriving ids from durable catalog state alone.

**What would raise its priority.** The same trigger as the skiplist RNG: deterministic simulation.
Identities that differ per run make a replay's manifests differ per run.

## 5. `vfs::sync_tracker`'s global ledger — one hazard closed, one remains (G3, 2026-08-15)

**What.** `SYNCED` (`src/vfs.rs`), a process-global set of fsynced paths that the crash tests read to
decide what a power loss would have lost.

**Why kept as a global.** `#[cfg(test)]` at both the module and the call site, so it costs production
nothing, and keyed by canonicalised absolute path, so two tests with their own `TempDir` cannot
collide. Injecting it would thread a test-only observation through every write path in the engine —
the "stop if" it was kept under.

### Closed: the undocumented SST-only invariant is now checked

This entry used to say the readers' `extension == "sst"` filter was "the only thing keeping a durable
WAL segment out of the truncation blast radius". **Measured, that was wrong on two counts:** both
readers walk `opts.sstable_dir()`, not the store root, so a WAL segment is never even enumerated —
the directory scope is the primary guard and the extension filter is secondary.

The real invariant, which genuinely was undocumented: **the ledger contains SSTables and nothing
else**, because `fsync_file` is called from exactly three places and all three are SST paths (flush,
compaction output, detach's materialised table). So `was_synced` returns `false` for a WAL segment
*because it was never a candidate*, not because the file is vulnerable — and a caller that widened
its walk would act on that and truncate durable data.

`record` and `was_synced` now `debug_assert` that the path is an SSTable, and
`the_sync_ledger_refuses_to_answer_about_a_non_sstable` proves the guard fires. The invariant is
checked rather than commented, so widening the walk is loud instead of silently destructive.

### Remaining: the ledger is never cleared

Still true, and still unreachable: every test owns a `TempDir`, so no two tests produce the same
canonical path.

**What would make it reachable:** a test that **crashes twice on one path**. Entries from the first
store instance would still be present, so a file the second instance never synced would be spared
truncation — a weaker test, not a wrong one, but silently so.

**What it would take.** A `forget_all_under(path)` called by the crash helpers. Not added now
because nothing calls it, and an unused API is the thing this project deletes on sight. Add it with
the first test that crashes twice.

## 6. Tests reach `store.core.inner` at 256 sites

**What.** Test files read the store's internals directly — `level_manifest` (57 sites), `runtimes`
(34), `wal` (30), `opts` (17), `branch_catalog` (9), spread across fourteen files. Renaming any one
of those fields is a 150-site edit, and a test that reaches past the public API can assert something
no user could observe.

**Why not closed in V8b.** Doing it properly means designing accessors for what tests *legitimately*
need — per-owner table counts, the retained floor, a runtime's memtable state — and that is a slice
of its own. Attempting it inside V8b would have buried the two real defects that slice existed to
fix (`PIN_EXERCISED`, `test_is_stalled_flag`) under several hundred lines of mechanical churn.
V8b took the count from 259 to 256, which is honest rather than impressive.

**What it would take.** A `#[cfg(test)]` accessor per legitimate need, on the production type
(principle 4's allowed case), then migrating file by file — largest first, since `lsm_tests.rs`,
`fork_view_tests.rs` and `branch_runtime_tests.rs` are 200 of the 256. Each remaining reach should
end up with a comment saying why it cannot go through an accessor.

**What would raise its priority.** Renaming or restructuring any `CoreInner` field — the cost lands
all at once. The async port will do exactly that to `level_manifest`, which is the single most-read
field.

## 7. ~~The dead-code guard has a `#[cfg(test)]` loophole~~ — CLOSED (G4, 2026-08-15)

**What it was.** V1's `no_module_suppresses_dead_code_warnings` forbids `allow(dead_code)` anywhere
in `src/` outside a written allowlist. Genuinely dead code could still satisfy it by hiding behind
`#[cfg(test)]` instead, since an item compiled only in test builds raises no dead-code warning in a
release build.

**How it closed.** The item *is* dead in the test build, where the compiler does see it. So:

```yaml
- name: No dead test-only code
  run: RUSTFLAGS="-D warnings" cargo test --lib --no-run
```

One line in `.github/workflows/ci.yml`, next to the clippy step. The tree is clean under it today,
and it was verified non-vacuous by planting a dead `#[cfg(test)]` method on `BranchCatalog`:

```
error: method `probe_dead_cfg_test_item` is never used
```

**The caution in the original entry still stands**, and is why this is a compiler check rather than
a manual audit: V8a's plan listed five `#[cfg(test)]` methods as uncalled and **every one had live
callers**. Do not delete on suspicion; let the build say so.

---

## Not gaps

Recorded so they are not re-litigated:

- **`Instant::now()` in `commit.rs`, `task.rs`, `stall.rs`** — monotonic *durations* for latency and
  stall measurement. Routing them through a logical clock would be actively wrong. The fork fence
  (gap 2) is the exception, because it is the only one whose value changes control flow.
- **`NEXT_DEPENDENCY_ID`, `TEMP_COUNTER`, `TEST_TABLE_ID_COUNTER`** — monotonic uniqueness sources.
  Nothing reads them for meaning and no value is a sentinel.
- **`PROBABILITIES: OnceLock`** (`src/memtable/skiplist.rs:54`) — a memoized constant, a pure
  function of two compile-time constants. Not state.
- **The block cache has no hit/miss counters.** It had six under `#[cfg(test)]`; they are deleted
  (V8a-2). The claim they existed to check — a cached block is served without touching the file — is
  proven directly in `src/test/cache_tests.rs` by sealing the file, which needs no instrumentation
  in the cache and no divergence between builds.
