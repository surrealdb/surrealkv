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

Written 2026-08-15 (v2, slice V8a-4).

---

## 1. `Tree::flush` is `#[cfg(test)]`, and 206 test sites depend on it

**What.** `Tree::flush` (`src/lsm.rs:2788`) is gated `#[cfg(test)]`. It rotates the active memtable
and then calls `flush_all_immutables_sync`, returning only once every memtable is an SST. **The
suite's entire notion of "the data is now durable" is this method**: 206 `.flush()` call sites
across `src/test/`.

Production never executes it. A real store reaches durability through the *asynchronous* pipeline —
a rotation enqueues a flush task, a background worker picks it up, writes the SST, publishes the
owner state, and only then is the memtable released and the WAL segment reclaimable.

**Why this is the largest test/production divergence in the crate.** The two paths differ in
exactly the dimension the tests cannot see: *time*. Anything that only breaks when a flush is
concurrent with something else is invisible to all 206 sites — a read racing a rotation, a branch
operation racing its own branch's flush, a compaction scheduled off a flush that has not published
yet, a WAL segment reclaimed against a memtable still in flight, back-pressure that only engages
when flushes queue. The synchronous path serialises all of it away and hands the test a quiescent
store. Every one of those is a real bug class in an LSM engine, and the V4 defect (`close()` failing
after write-branch → delete-branch → close, stalling the shared flush loop store-wide) was found in
exactly this territory — by a test that did *not* go through `Tree::flush`.

It is also self-reinforcing: because `flush()` is synchronous, tests written against it acquire no
synchronisation of their own, so migrating them later means re-deriving what each one was actually
waiting for.

**Why it was not fixed here.** Changing how the suite reaches durability is a bigger change than the
whole of V8 combined, and mixing it in would have made the injection work unreviewable. It is not a
refactor: the moment tests wait on the real pipeline, timing dependencies the synchronous path hides
become visible, and some of what surfaces will be engine defects rather than test problems. That is
the *point*, and it needs a slice that expects it.

**What it would take.** One deliberately planned slice, in this order:

1. A durability barrier the tests can await that goes through the **real** path — e.g. a
   `flush_and_wait` that enqueues exactly what production enqueues and then waits for the
   publication, rather than bypassing the queue. This is the piece that must be designed, not
   improvised: it has to be the production path plus a completion signal, or it becomes a second
   `Tree::flush` with extra steps.
2. Migrate in batches, smallest files first, watching for tests that quietly *depended* on
   quiescence.
3. Keep a deliberate subset on a synchronous path — some unit tests genuinely want determinism, and
   that is legitimate as long as it is a choice rather than the only option.
4. Add coverage specifically for the operations where durability *timing* matters: branch delete
   while its own flush is in flight, compaction triggered by a flush that has not published, WAL
   reclamation against an in-flight memtable, back-pressure under queued flushes.

**What would raise its priority.** Any bug found in the background flush path — that is direct
evidence the blind spot is occupied. Also the async/object-store work landing, which rewrites this
layer anyway: doing it after that rewrite means migrating the suite twice.

## 2. `Error::ForkFenceTimeout` has no behavioural test

**What.** It is the one branch-reachable error with no test that produces it. The only occurrence
outside its definition is a constructor in a compile-time array.

**Why.** Reaching it needs a commit stalled between enqueue and apply while a fork waits on the
fence. The only harness that can gate a commit that way (`GatedEnv`) lives inside `commit.rs`'s unit
tests and does not reach the branch layer. The fence's deadline is `Instant::now()`
(`src/lsm.rs:1931-1940`) — **the one `Instant` in the crate that changes control flow rather than
just reporting** — so the timeout is neither reachable nor deterministic from a test.

**What it would take.** An injected deadline source on `Options`, in the same shape as `clock` and
`fault_policy`. One change closes both problems: the fence becomes testable, and the last
control-flow-affecting ambient time source goes away.

**What would raise its priority.** A report of a fork timing out in practice, or the async port,
which touches the fence's drain logic directly.

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

## 5. `vfs::sync_tracker`'s global ledger

**What.** `SYNCED` (`src/vfs.rs:271`), a process-global set of fsynced paths that the crash tests
read to decide what a power loss would have lost.

**Why kept.** It is `#[cfg(test)]` at both the module and the call site, so it costs production
nothing, and it is keyed by canonicalised absolute path, so two tests with their own `TempDir`
cannot collide. Injecting it would thread a test-only observation through every write path in the
engine — which is precisely the "stop if" this slice was written under.

**Two hazards worth knowing before touching it.**

- **It is never cleared.** It would go wrong the moment a test reuses a path across two store
  instances and expects the second store's fsync history to start empty.
- **It only records `fsync_file`.** The WAL, the authority publish and the manifest fsync through
  other paths, so they never enter the ledger. The readers' `extension == "sst"` filter is the only
  thing keeping a durable WAL segment out of the truncation blast radius, and **that invariant is
  undocumented at the call sites** — a reader who widened the filter would silently start truncating
  data the engine had promised was durable.

**What would raise its priority.** A crash test needing to observe a non-SST fsync, or a test
reusing a store path.

## 6. The dead-code guard has a `#[cfg(test)]` loophole

**What.** V1's `no_module_suppresses_dead_code_warnings` forbids `allow(dead_code)` anywhere in
`src/` outside a written allowlist. Genuinely dead code can still satisfy it by hiding behind
`#[cfg(test)]` instead, since an item only compiled in test builds raises no dead-code warning in a
release build.

**Why not closed.** A static text check cannot reasonably prove a `cfg(test)` item is uncalled;
that is a job for the compiler, and the compiler does warn — but only in a test build, where these
items *are* live.

**A caution, learned the hard way.** V8a's plan listed five `#[cfg(test)]` methods as having "zero
call sites" and scheduled them for deletion. Re-measuring at the gate found **every one has live
callers** — `BranchCatalog::default_branch`, `CommitPipeline::oracle`,
`allocated_seq_high_water`, and four `BackgroundErrorHandler` accessors with eleven call sites
between them. They are test-only accessors on production types, serving tests that legitimately need
internals, which is an *allowed* shape, not the loophole. Deleting them would have deleted the
assertions with them. **Measure before deleting anything on this basis**, including anything a
future version of this document claims.

**What it would take.** `cargo test --no-run` with warnings-as-errors would catch a genuinely
uncalled `#[cfg(test)]` item, since it is dead *in the test build too*. Worth trying; it may be a
one-line CI addition rather than a slice.

**What would raise its priority.** Finding real dead code hiding this way.

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
