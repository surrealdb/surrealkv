# P3 Single-Branch Prototype Evidence

> Integration correction (2026-08-13): this document records prototype behavior and adversarial
> findings. It no longer proves a production cutover. The wholesale removal was reversed; final P3
> must reuse/adapt the existing engine as specified in `INTEGRATION_REUSE_AUDIT.md`.

Prototype completed: 2026-08-13

The prototype demonstrated a possible vertical semantic spine. It is currently additive: the
existing `Tree`, commit, WAL, memtable, SST, compaction, checkpoint, transaction, benchmark, and
test paths remain compiled until those semantics are integrated into them.

## Delivered vertical spine

- Public async `Database::memory` and `Database::open_local` constructors.
- Public typed database/branch/version/timestamp/fence identities, expected-head commits,
  `WriteOperation`, exact latest/version/timestamp selectors, receipts, and stable errors.
- One checksummed `DatabaseRoot` codec containing database identity, main-branch generation/head,
  global version/timestamp timeline, immutable table references, and a bounded unflushed delta.
- Creation publishes the empty root before returning, so database and main-branch identities survive
  an empty reopen.
- One serialized commit transition assigns one version/timestamp to the whole batch and conditionally
  publishes the complete candidate root. Stale expected heads conflict before mutation.
- Latest/version/timestamp point, range, and history reads share temporal resolution and newest-row
  visibility. Tombstones and expiry mask older values.
- A database-wide mutable-byte budget rejects writes before publication. Explicit flush converts the
  complete bounded delta into one P2 table and atomically replaces rows with its descriptor.
- Owned compaction merges immutable tables, verifies duplicate row equality, publishes one new table,
  and atomically replaces descriptors. Old objects intentionally leak until P5 reachability/GC.
- Local authority uses an advisory single-writer lock, append-only checksummed root frames, two
  checksummed root slots, file/directory durability, session fencing, exact retries, and operation
  reconciliation.
- Native platform supplies persistent-domain wall timestamps and operating-system randomness. P5
  owns automatic restartable maintenance; P3 exposes explicit flush/compaction.

The authority journal temporarily carries the bounded unflushed delta inside root candidates. This
removes the old append/ack/delayed-memtable-apply and split-WAL recovery states. It is intentionally
bounded and flushed into immutable tables; persistent catalog/timeline subobjects remain later
scalability work and may not change the public semantics.

## Paired test evidence

Direct owner regressions cover:

- root round-trip plus every truncation boundary;
- write-budget refusal before authority mutation;
- flush replacing mutable rows with one owner-correct table descriptor;
- owned compaction replacing two tables with one;
- Local authority reopen, exact reconciliation, stale-session fencing, uninstalled journal suffix,
  present corrupt slot, and installed torn frame;
- native monotonic time and entropy.

Highest-boundary tests cover:

- Memory results match the independent branch model after every commit/read, before and after flush,
  and after owned compaction;
- SimStorage timeout-after-commit reconciles and survives crash/reopen;
- a table upload followed by root-publication failure leaves one unreachable object while crash
  recovery exposes the complete old state;
- Local commits, flushes to the Local object adapter, and reopens through Local authority;
- public Memory and Local API tests exercise commit, conflict, read, flush, identity, and reopen;
- architecture guards prove only the branch-native runtime is reachable and raw filesystem IO is
  confined to Local adapters.

## P0 failure-class disposition after cutover

- Bottom-level tombstone/snapshot loss: historical tombstone/value visibility is compared to the
  model after real flush and compaction; compaction retains versions in P3.
- WAL rotation versus delayed apply: eliminated. A complete root candidate is authoritative before
  runtime state replacement; timeout reconciliation/reopen tests cover the publication boundary.
- Split-segment recovery floor: eliminated. Local recovery follows one installed root-frame chain;
  uninstalled suffixes are ignored and installed tears fail closed.
- duplicate-version snapshot-pin loss: eliminated from P3 reads, which are immutable exact selectors
  and do not destructively register by version. P5 introduces explicit counted durable pins only
  where reclamation needs them.

## Adversarial findings resolved

1. Empty database identity was initially process-local until the first user commit. Creation now
   publishes an empty authoritative root before returning.
2. A present corrupt root slot was initially treated as absent, silently permitting fallback to an
   older slot. Present corruption now fails closed; only a never-installed temporary/suffix state can
   leave the prior root authoritative.
3. Initial capability validation lacked some object/authority requirements; P1 now validates the
   complete set before database creation.
4. Flush upload success followed by authority failure initially lacked a vertical proof. SimStorage
   now proves leak-only behavior and complete-old-state recovery.
5. The old runtime remained physically present during the brief public overlap. It and its old tests
   are now removed, and the crate-root guard prevents renewed reachability.

## Exit commands

```text
cargo test --quiet
50 passed; 0 failed

cargo clippy --all-targets --all-features -- -D warnings
passed
```

P3 exit gate: satisfied. Memory and Local share one public spine and exact temporal semantics; Local
recovery fails closed; values remain inline; the old runtime, VLog, and B+tree are absent.
