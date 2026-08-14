# Handover: async write paths and the object-store adapter

**Written:** 2026-08-15, on branch `v2`, slice V0.
**Audience:** whoever plans and implements the experimental branch that takes this on.
**Status:** nothing here is implemented. Zero lines written. This is the brief.

This document exists because the code it describes is about to be deleted (slice V1). It is the
same discipline PA2 used: carry the knowledge forward in writing *first*, then delete. You should
be able to plan the whole port from this file alone.

---

## 1. The decision, and the case that was made against it

**Decision (user, 2026-08-15): the engine's write paths go async.**

It was taken against the recommendation of the alternative. Both cases are recorded here so the
trade can be re-examined rather than inherited — if you find the ground has moved, you are entitled
to reopen it, and you should say so explicitly rather than quietly switching.

### The problem

`ObjectStore` (in the now-deleted `src/storage/mod.rs`) was `async fn` throughout. Every engine
call site that would use it is synchronous **and holds a `std::sync` guard across the call** —
`catalog_publish` (a `Mutex`) or `level_manifest` (an `RwLock`). A `std` guard may not be held
across an `await`. There were **seventeen** such sites: nine taking `catalog_publish.lock()` and
eight taking `level_manifest.write()`, covering fork, delete, detach, TTL expiry, merge-edge
recording, flush and compaction.

`spawn_blocking` or a bare `block_on` inside the engine is not an escape: `merge_into` is `async`
and calls the sync `record_merge_edge`, so a `block_on` there runs on a runtime worker. Plan
finding F8 had already refused that shape once, for fork, in the same words: *"Do not paper over
this with `spawn_blocking` inside the engine — that hides a real property from the caller."*

### Option A — sync engine seam (recommended, not chosen)

`ObjectStore` becomes sync, or gains a sync sibling the engine uses; the async object-store adapter
owns a runtime internally and blocks its caller. Every lock structure stays exactly as built,
`LSMIterator` stays sync (which plan finding F4 had already decided for the read path), and async
is confined to the hydration path and the adapter's insides.

Its honest cost, which is why it was rejected: an authority publish against S3 blocks the calling
thread, and for `fork_branch` that thread is one every writer in the store is waiting behind.

### Option B — async engine write paths (chosen)

`std` locks become async locks, the publish paths become `async fn`, and fork's contract changes.
This is SlateDB's shape and it does not lie about the cost of a network round-trip. It is also a
rewrite of the locking in the commit, flush, compaction and branch-op paths — well beyond a seam
port.

**Consequence: plan finding F8 is superseded.** `fork_branch` no longer has to stay sync. Its
existing contract — "blocks briefly; blocks all writers for the drain instant", with
`ForkFenceTimeout` as the escape — has to be restated for an async world, and the drain fence
(`FORK_DRAIN_TIMEOUT`, 5s, spin-waiting on `commit_pipeline.is_drained()`) needs rethinking rather
than translating: a spin loop that made sense while holding a lock for microseconds is a different
proposition when the operation around it is a network call.

---

## 2. Why the deleted `src/storage/` traits must not simply be restored

Deleted in slice V1. **Recovery pointer: commit `5d8d769`**, paths
`src/storage/{mod,memory,local,local_commit,sim,native,conformance_tests}.rs` — 2,591 lines across
7 files, plus 11 conformance tests.

It was deleted because it was dead: zero engine callers, `#![allow(dead_code)]` on the whole
module, and three `#[allow(unused_imports)]` markers whose comments promised a "P3 runtime
cutover" that never happened. Two architecture-guard tests pinned it in place by string-matching
its contents, which made it worse than ordinary dead code.

**But the reason not to restore it verbatim is architectural, not hygienic.** The traits were
designed by the P0 prototype against a prototype engine that no longer exists. Their calling
convention does not fit this engine — that is finding F2 above, and it is not fixable by moving the
files back. **Design the seam against the seventeen real call sites**, with whatever async shape
Option B settles on.

### What was genuinely good, and worth carrying forward as ideas

- **`PutOutcome { Created, AlreadyExistsSame }`.** This is an exact semantic match for the
  authority's own `PublishOutcome { Created, AlreadyExistsSame }` (`src/authority/publish.rs:19`),
  whose doc comment reads *"The version already exists with exactly these bytes — an idempotent
  retry of our own publish, not a conflict."* FK1's publish primitive — temp-write → fsync →
  `fs::hard_link` → unlink → dir-fsync — **is** conditional-create, because POSIX `hard_link` is
  no-clobber where `rename` silently overwrites. That mapping onto `put_unique` is real and still
  correct; it was only ever the *calling convention* that was wrong. Preserve the idempotency
  semantics exactly: FK1's tests pin them.
- **`Bindings::validate(BindingRequirements) -> KernelResult<()>`**, returning
  `ErrorCode::CapabilityMismatch` **before any mutation**. `BindingRequirements` carried eight
  fields: `ranged_reads`, `unique_put`, `paginated_list`, `idempotent_delete`,
  `conditional_publish`, `writer_fencing`, `reconcile_unknown`, `minimum_durability:
  DurabilityClass`. This is the refuse-don't-degrade posture in executable form and it is the right
  shape — a backend that cannot do conditional create must be *refused* for multi-writer mode, not
  silently downgraded.
  *(Note: the `missing()` list described in the old PG1 plan text was never built. `validate`
  returned a single typed error, not an enumeration of what was absent. If you want the list, you
  are building it, not restoring it.)*
- **`SimHarness`'s fault-point taxonomy** — `FaultPoint { ObjectPutBefore, ObjectPutAfter,
  ObjectDeleteBefore, ObjectDeleteAfter, CommitBefore, CommitAfter, Maintenance }`, with
  `crash()` and `advance()`. The before/after pairing per operation is what makes
  "leaks an object but recovers" distinguishable from "did not happen at all", and that distinction
  is the whole point of the durability tests. Slice V3 builds a `#[cfg(test)]` failpoint registry
  for the *local* engine with the same before/after discipline — compare against it rather than
  duplicating it.
- **`ObjectCapabilities`** as a plain 5-field struct (`ranged_reads`, `unique_put`,
  `paginated_list`, `idempotent_delete`, `durability`).

### What was not worth keeping

- The async trait signatures themselves — see above.
- `ReconcileOutcome::Unknown` existed as a variant with no producer, justified by a comment about
  "remote authorities that cannot yet prove either outcome". The *concept* is essential (see
  obligation 2 below); the dead variant is not.
- The whole `memory` / `local` / `local_commit` backend triple was written against the prototype's
  needs. The engine's real needs are the seventeen call sites; derive from those.

---

## 3. Measured scope of the IO port

**~170 sites across 17 files**, counted 2026-08-15 by grepping for
`std::fs | File::create | OpenOptions | sync_all | fs::` outside `src/test/` and `src/storage/`:

| file | sites | | file | sites |
|---|---|---|---|---|
| `src/wal/recovery.rs` | 26 | | `src/vfs.rs` | 7 |
| `src/wal/manager.rs` | 25 | | `src/compaction/compactor.rs` | 7 |
| `src/checkpoint.rs` | 24 | | `src/wal/writer.rs` | 6 |
| `src/wal/reader.rs` | 21 | | `src/lockfile.rs` | 6 |
| `src/authority/publish.rs` | 16 | | `src/wal/mod.rs` | 4 |
| `src/lsm.rs` | 14 | | `src/memtable/mod.rs` | 4 |
| | | | `src/branch_runtime.rs` | 4 |
| | | | `src/levels/mod.rs` | 2 |
| | | | `src/sstable/{table,index_block}.rs` | 1 each |

The WAL alone is four files and ~78 sites. Do not attempt this as one slice; the original plan's
PF1 tried to and that is what the gate caught.

Note `src/vfs.rs` is **not** a filesystem abstraction you can swap — it is a `File` trait used
mostly for `read_at`, with files created by `SysFile::create` at each call site. It will not save
you.

---

## 4. Finding F3: async on the manifest would drag the READ path async too

This is the trap. **Read it before designing anything.**

`Snapshot::collect_iter_state` (`src/snapshot.rs:355`) takes a read guard on `level_manifest`, and
it is called from **synchronous `LSMIterator` construction**. Turn that `RwLock` into an async lock
and every scan and every point read becomes `async` — which is exactly what plan finding F4
rejected on evidence, and which nothing in the async decision asks for. The decision was about
*write* paths.

### The resolution: immutable manifest versions behind an atomic swap

Readers must stop taking a lock the publisher holds. Two mechanisms, both needed:

- **`level_manifest` becomes immutable versions behind an atomic swap.** A reader clones an `Arc`
  of the current version — no lock, no await, no blocking. A publisher builds the next version,
  awaits its IO, and swaps it in **only on success**.
- **`catalog_publish` becomes an async mutex**, held across the await. It already serialises every
  branch operation; it just becomes awaitable.

This is RocksDB's `VersionSet` shape, and it is a better fit for an LSM manifest than a mutex
regardless of the async question.

**It also removes a real window.** Today `update_manifest` applies the changeset in memory and
*then* persists, reverting on failure — an applied-but-not-durable state exists, and it is
unobservable only because the write lock is held across both. With a swap there is no such moment:
what readers see does not change until the IO has succeeded. Do not lose this property by splitting
the lock naively; that is the failure mode to design against.

**Suggested first slice: PF0a, manifest versioning alone.** No IO changes, no async, pure
restructuring, fully testable on its own. It de-risks everything after it.

---

## 5. Derived slice plan

| slice | scope |
|---|---|
| **PF0a** | `LevelManifest` → immutable versions + atomic swap. Readers lock-free and sync. No IO or async changes. |
| **PF0b** | Publish paths → `async fn`; `catalog_publish` → async mutex; fork / delete / detach / expiry / flush / compaction publishes and their transitive callers become async. `Tree`'s branch API becomes async. Restate `fork_branch`'s contract and rethink the drain fence. |
| **PF1a** | Authority publish through the object seam, keeping `AlreadyExistsSame` idempotency exactly as FK1's tests pin it. |
| **PF1b** | WAL IO through the seam — 4 files, ~78 sites. Almost certainly splits further. |
| **PF1c** | Checkpoint + lockfile through the seam; `Platform` clock/randomness injection (no ambient `SystemTime`/`rand` in core); the source guard, whose allowlist can only be minimal once everything above has landed. |
| **PF2** | Table reads through the object seam, **hydrate-then-iterate** (plan finding F4): blocks fetched asynchronously into `src/cache.rs` before a sync iteration pass. `LSMIterator` stays sync. `DirectRead` (async iteration) stays deferred behind its own gate. Bounded memory: hydration respects a budget and **refuses rather than degrades**. |
| **PF3** | The parity suite: one set of semantic assertions run against `Tree` on every backend, plus per-adapter conformance (conditional publish, range reads, streaming bounds, durability acknowledgement, crash windows). Negative peers: memory MUST refuse durable mode. |
| **PG1** | S3/R2-family `ObjectStore` + conditional-HEAD `CommitStore`. Capability validation at open with refuse-don't-degrade. **State the throughput ceiling in the docs**: R2 permits one write per second to the same key, so conditional-HEAD is a low-write authority. Do not market it otherwise. |
| **PG2** | Read modes, hydration tuning, prefetch, bounded-memory lane; epoch fencing against a real conditional store. No support claim before this lane is green. |

---

## 6. Invariants the port must not break

Each of these was established by a slice that found a real bug. Breaking one silently reintroduces
that bug. The code that enforces each is named so you can find it after refactoring.

1. **The merge-edge publish excludes concurrent compaction publication.**
   `BranchHandle::record_merge_edge` (`src/lsm.rs`) holds the level-manifest **read** lock across
   the catalog publish, because the edge's target-side sequence becomes a retention anchor the
   instant it lands. Compaction publishes under the write lock, so this closes the window where a
   job that sampled anchors without this one could install an output missing what the anchor
   promises. **Under an atomic-swap manifest this exclusion must be re-established by the publish
   serialiser**, not lost.
2. **Compaction's anchor re-check is atomic with its own publish.** `Compactor::update_manifest`
   holds the manifest write lock across `RetentionAnchors::appeared_since` + `apply_changeset` +
   `persist_owner_update`, and refuses with `CompactionPinRaced` if the catalog gained an anchor it
   never sampled. Same requirement: the atomicity has to survive the restructuring.
3. **`catalog_publish` serialises every branch operation.** Fork resolves its parent under it and
   relies on that parent not changing while the fence is taken and released.
4. **Retention anchors derive from the CATALOG alone** — never from child state files, never from
   the snapshot tracker (design §3.3a, plan amendment C9). FK6 fixed two silent-wrong-answer bugs
   here; the anchor set is every live child's `fork_seq` plus every live merge edge's
   `target_through_seq`.
5. **Lock order is `level_manifest` → `branch_catalog`**, consistently. Fork takes manifest-read
   then catalog-write; the compactor takes manifest-write then catalog-read. Whatever replaces
   these must keep a single global order.
6. **Data before metadata, always.** A merge's chunks are durable before its edge is recorded; a
   crash in between re-offers what was applied (which converges) rather than advancing past changes
   that were never written. The opposite order loses data silently.

---

## 7. The five carried-forward obligations

These came from `testkit.rs`, deleted in PA2, and were recorded in `docs/removed-surfaces.md` as
obligations of PF1 and PF2. They travel here with their owners intact. **Re-express them against
`Tree`, not against a prototype.**

1. **Database identity is published before `open` returns.** Reopening the same authority yields
   the same identity; a fresh authority at version zero has an empty head. *(PF1a)*
2. **An unknown commit outcome reconciles and reopens after a crash.** With a fault injected after
   the authority write, the commit reports `Unknown`, `reconcile` confirms it, and a reopen sees the
   committed data. *(PF1a)*
3. **A failed root publish leaks an object but recovers the complete old state.** With a fault
   injected before the authority write, the flush fails typed `Unavailable`, the uploaded object is
   left unreferenced, and a reopen reads the complete pre-flush state. *(PF1a)*
4. **A local spine commits and reopens through the durable authority.** Same identity, same head,
   same data across a close/reopen. *(PF1a)*
5. **The same immutable table's bytes are readable by range, identically, from every backend.**
   *(PF2 — it could not be expressed before, because `src/sstable` reads through `vfs::File`.)*

**Cross-check obligations 2 and 3 against slice V3 before rebuilding them.** V3 adds a
`#[cfg(test)]` failpoint registry to the local engine covering catalog publish, owner-state
publish, root publish, SST rename/fsync and WAL append. The *local* half of both obligations is
reachable from that lane. What only a real object-store fault injector can add on top is the
network-specific part: an unknown outcome that is genuinely unknown (rather than a chosen failure),
and an object that is durable while the commit that references it is not.

---

## 8. PE2's status

The original plan's PE2 — a fault-injection lane over the branch state machines using
`SimObjectStore`/`SimCommitStore` — was blocked because those stores intercept nothing: the engine
never called them.

**Slice V3 on `v2` delivers the local equivalent**: failpoints at the durable steps, with every
branch state machine (fork, delete, detach, TTL expiry, merge-edge record, reclamation sweep)
driven through each failure and asserted against its own contract. Check what V3 actually covered
before planning anything here.

What remains genuinely object-store-specific, and belongs to PG1/PG2:

- unknown commit outcomes that are unknown because the network said so, not because a test chose it;
- writer-epoch fencing exercised against a real conditional store;
- partial/interrupted multipart uploads;
- the read-after-write and listing consistency behaviour of the actual provider;
- the R2 one-write-per-second-per-key ceiling as a *tested* property, not a documented claim.

---

## 9. Open questions this handover does not answer

Recorded rather than invented, per V0's exit gate.

1. **What replaces the fork drain fence?** It currently spins on `commit_pipeline.is_drained()`
   under `lock_writes()` with a 5-second timeout, and `PE1` now measures the hold as
   `fork_drain_nanos`. Under async publishes, is the fence still taken around the same region, and
   does `ForkFenceTimeout` keep its meaning?
2. **Does `Tree`'s whole public API become async, or only the branch-mutating half?** Reads stay
   sync per F4. `create_branch` / `delete_branch` / `set_branch_ttl` / `detach_branch` all publish,
   so they must become async — which changes every caller. Decide deliberately whether `Tree` grows
   a sync facade for embedded users.
3. **Which async runtime, and is it a dependency the library imposes?** Tokio is already load-
   bearing in more places than the commit pipeline: `src/task.rs` (11 uses — the background
   maintenance runner), `src/commit.rs` (7), `src/lsm.rs` (2), `src/stall.rs` (1),
   `src/lockfile.rs` (1). So the question is not "adopt a runtime" but "how much more of the engine
   becomes runtime-bound, and does an embedded user without a runtime still have a usable API".
4. **Does the atomic-swap manifest change recovery?** Open builds the manifest from the root and
   per-owner state files; versioned manifests may want a different reconstruction path.
5. **What is the WAL's story on an object store at all?** The port list assumes it moves, but an
   append-only WAL on S3 is a different design (SlateDB writes WAL objects per batch). This may be
   a redesign rather than a port, and if so it should be planned as one.
