# Patterns: injection and testing

How this codebase takes a dependency it must not choose, and how it tests. Both were re-derived
per-slice until V8 wrote them down; this file is so you inherit them instead.

Companion files: `docs/KNOWN_GAPS.md` (what we deliberately did not do),
`docs/removed-surfaces.md` (what we deleted), `docs/P3_INTEGRATION_PROGRESS.md` (per-slice as-built
records, including the deltas from plan).

---

## Injection

### 1. One injection point

A dependency the engine *needs* but must not *choose* is carried on `Options` as `Arc<dyn Trait>`,
defaulted in `Default::default()`. There is no second mechanism: no globals, no macros, no `cfg`
forks in call sites.

Two worked examples, and they are deliberately identical in shape:

```rust
// src/lib.rs
pub struct Options {
    /// Logical clock for time-based operations
    pub(crate) clock: Arc<dyn LogicalClock>,
    /// Consulted at each durable step. `NoFaults` in every build a user sees.
    pub(crate) fault_policy: Arc<dyn FaultPolicy>,
    // ...
}
```

Fields are `pub(crate)` and `Options` derives `Clone` only, so external users configure through
`TreeBuilder` and adding a field is invisible outside the crate.

### 2. Interfaces name a capability, not an implementation

A new implementation requires zero changes in consumers. `LevelManifest::{fresh, hydrate}` already
took `Arc<Options>`, so `fault_policy` reached `persist_owner_update` and `persist_root` with **no
signature changes** — one stored field, like `timeline` and `next_table_id` beside it.

### 3. Closed sets are enums; open sets are traits

`FaultPoint` is an enum: the set of durable steps is small, closed, and worth matching exhaustively.
No stringly-typed names that typo silently.

`FaultPolicy` is a trait: scripted, always-fail, probabilistic, recording — all future policies are
additions, not edits.

```rust
// src/failpoints.rs
pub(crate) enum FaultPoint { CatalogPublish, OwnerStatePublish, RootPublish }

pub(crate) trait FaultPolicy: Debug + Send + Sync {
    fn check(&self, point: FaultPoint) -> Result<()>;
}

pub(crate) struct NoFaults;   // zero-sized, returns Ok, the production default
```

### 4. Test doubles implement the same interface as production

Both builds compile the **same call sites**; only the injected value differs. A `#[cfg(test)]`
*implementation* is fine. A `#[cfg(test)]` *call site* means the tests are not exercising the
shipped path.

`ScriptedFaults` is `#[cfg(test)]` and holds its script **in the instance**, which is the practical
reason the earlier global version needed a thread-local and this one does not: two stores in two
parallel tests cannot see each other's script.

What this rules out, with the real example: the block cache used to carry six `AtomicU64` counters
under `#[cfg(test)]`, so the cache under test had a different struct size and six extra atomic
read-modify-writes per lookup than the one that ships. Deleted in V8a-2. The claim they served — a
cached block is served without touching the file — is now proven by a `vfs::File` double that fails
every read once sealed (`src/test/cache_tests.rs`). That double *is* principle 4: an ordinary `File`
implementation, so the table under test runs exactly the shipped code.

### 5. A global needs a written allowlist entry

`no_ambient_state_outside_the_written_allowlist`
(`src/test/architecture_guard_tests.rs`) walks `src/` and fails on an unlisted `static`, any
`thread_local!` / `lazy_static!` / `once_cell::`, any `env::var`, any `SystemTime::now()` outside
`src/clock.rs`, and any ambient RNG outside the skiplist.

It fails **both ways**: a listed global that no longer exists also fails it, so a reason cannot
outlive the code it describes. Surviving because someone argued for it is a decision; surviving
because nobody looked is not.

---

## Testing

### 6. Test support is a layer, not a habit

`src/test/support/` holds store construction, branch assertions, the branch-flush helper and
`wait_until`. `src/test/mod.rs` holds the iterator collectors it predates. A helper redefined in
three files is three helpers that can disagree — `create_temp_directory` had six copies and
`create_store` nine, two of which answered a different question under the same name.

`test_helpers_are_not_redefined_outside_the_shared_layers` enforces it, reading the exported names
out of both files so the list cannot drift.

**One name, one meaning.** If two files want the same word for different questions, that is two
names. `create_store_with_faults`, `create_shared_store` and `create_store_with_two_levels` are all
former `create_store`s that were doing something else.

### 7. A test asserts an invariant, not a schedule

Concurrency tests assert "no silent overwrite / no lost commit / every outcome one of a named set",
never a particular interleaving. A flaky assertion is a finding, not a test to loosen.

**Never sleep-then-assert.** A sleep encodes a guess about how fast the machine is; use
`support::wait_until`, which encodes what you are waiting for and gets *slower* rather than *wrong*
under load.

**A negative needs a positive control.** `wait_until` cannot establish "this did not happen". A
bounded wait plus an assertion that nothing happened is equally consistent with "nothing was
running" — `test_no_spurious_small_flush` waits, asserts no flush occurred, and *then* proves the
same worker does flush an over-threshold memtable.

**Never assert inside a spawned task.** A panic there is captured in the `JoinHandle`; drop the
handle and the test passes whatever the assertion said. Return the observation and assert on it in
the main task (`test_is_stalled_flag`).

### 8. Sabotage twins and sensitivity probes

Every retention, recovery or correctness oracle ships with a planted violation that reddens it. A
green test that would pass with the mechanism deleted is not a test.

After a slice, disable its mechanism and confirm the intended test fails **on the intended
assertion**. Record the probe and its outcome — including when it refuses to redden, which is itself
a finding. Two cautions learned by doing it wrong:

- A probe that reddens for the *wrong reason* is not a probe. One attempt disabled a check with
  `windows(0)`, which panics; the test failed inside the probe itself and proved nothing.
- Non-vacuity checks must test the thing they claim. A flush-parity test checked that its ranges
  discriminated, but not that the flush had moved any data — and the synchronous drain rotates only
  the default runtime, so the branch's rows never left the memtable and the test compared it with
  itself. `support::flush_branch_to_table` asserts a table appeared, for exactly this reason.
- A probe can also refuse to redden because it is aimed at the wrong thing. Making
  `Tree::flush_and_wait` return without waiting left all five flush-concurrency tests green — the
  race is created by `rotate` + `wake_up_memtable`, which run *before* the wait. That is how those
  tests were distinguished from `the_barrier_returns_only_once_the_flush_has_produced_a_table`,
  which is the one that actually tests the barrier.
- **Scope a probe to the test whose claim it breaks.** Applied file-wide, that same probe made ~15
  barrier calls each burn the full 5s `WAIT_UNTIL_DEADLINE` — 75s of silence that looked like a
  deadlock. Scoped to one test: 5.05s, same answer.

### 9. A failing test is evidence about production logic first

Change a fixture only with an argument for why no production path shares the problem. Never weaken
a check to get green. Three of the defects on this branch were found by taking a red test literally:

- the property suite's `LoadManifestFail` was a **valid** L1 layout the loader rejected;
- a fault-injection test whose *injected fault never fired* led to silent data loss in the SST
  writer;
- `revert_range` returning 2 for a one-key range was a range bound the memtable could not express.

### 10. Guards are tested for non-vacuity

Every source-scanning guard asserts that its walk reached the tree *and* that its detector matches
its own target. A guard that silently stops matching is worse than no guard, because it reads as
coverage.

---

## Where the seams are

| Seam | Injected as | Production default | Test double |
|---|---|---|---|
| Logical clock | `Options::clock` | `DefaultLogicalClock` | controlled clocks in tests |
| Failure at durable steps | `Options::fault_policy` | `NoFaults` (zero-sized) | `ScriptedFaults` |
| Commit concurrency | `Options::max_concurrent_commits` | 7 | any value; clamped in `CommitPipeline::new` |
| File IO | `Arc<dyn vfs::File>` | `SysFile` | `SealableFile`, in-memory `Vec<u8>` |
| Compaction policy | `Arc<dyn CompactionStrategy>` | leveled `Strategy` | per-test strategies |
| Fork fence deadline | `Options::fork_drain_timeout` | 5s | `Duration::ZERO` to make the fence hostile |
| Durability barrier | — | background worker | `flush_and_wait` vs `drain_flushes_synchronously` |

Things that are deliberately *not* injected, with reasons, are in `docs/KNOWN_GAPS.md`.
