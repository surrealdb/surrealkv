# Branching

SurrealKV is a branch-native versioned key–value store. A branch is a cheap, copy-on-write view of
another branch: creating one writes a catalog entry and copies no data. Branches are meant to be
made freely and thrown away — the design target is many short-lived ones, not a handful of
long-lived release lines.

This document is the semantics you need to use branching *safely*. The API is small; the parts
worth understanding before you rely on them are the fork anchor, what retention costs, and what a
merge's base is.

---

## The one idea everything rests on: a single commit clock

**There is one global sequence counter for the whole store.** Every commit on every branch draws
the next number from it. Branches do not have their own clocks, and there is no per-branch "head"
in the git sense.

This is why `BranchInfo::last_write_seq` is documented as "the newest sequence this branch wrote
*itself*" rather than as a head. Two branches' sequence numbers are directly comparable, and that
comparability is what makes forks and merges cheap: a branch's view is defined by a *ceiling* on
the one clock, not by a separate history.

## Fork anchors

When you fork branch `b` from branch `a`, the fork records `a`'s visible sequence at that instant.
That number is the **fork anchor**.

```
global commit sequence ────────────────────────────────────────▶
                  10        20        30        40
main   ───●───────●─────────●─────────●─────────●──────
                            │
                       fork_seq = 25
                            │
child                       └──●───────●──────────
                              32      41
```

The child reads:

- **its own writes**, all of them; plus
- **the parent's writes at or below 25** — the anchor.

The child **never** sees a parent write above its anchor, ever, no matter when it reads. Sequence
30 and 40 on `main` above are invisible to `child` permanently. This is not a snapshot that ages;
it is the branch's definition. A fork *is* a cap on the global clock.

Forking a child of a child composes the obvious way: each link contributes a cap and the effective
ceiling is the **lowest** anchor on the chain. A grandchild can never see more of its grandparent
than its parent could.

Three ways to choose the anchor:

```rust
use surrealkv::{ForkPoint, TreeBuilder};

let tree = TreeBuilder::new().with_path(path).build()?;

// The parent's head right now. Drains the commit pipeline under a write fence
// so the anchor is exact rather than approximate.
let head = tree.fork_branch("main", "feature", ForkPoint::Head)?;

// A specific sequence you observed earlier.
let pinned = tree.fork_branch("main", "audit", ForkPoint::AtVersion(25))?;

// The highest sequence committed at or before a timestamp.
let historical = tree.fork_branch("main", "yesterday", ForkPoint::AtTimestamp(ts))?;
```

`ForkPoint::Head` briefly fences writes **store-wide** while the pipeline drains. It is short, but
it is not free, and it is shared: `metrics().fork_drain_nanos / metrics().forks` is the average
pause every writer in the store took for somebody else's fork. If you fork in a hot loop, watch
that number.

`AtVersion` and `AtTimestamp` are refused rather than approximated if they cannot be served
exactly — see `BelowRetentionFloor` and `TimestampBelowHorizon` below.

## What retention costs

A child reading through its parent means the parent's *old versions* must stay readable. Compaction
would normally discard a superseded version; when a live fork anchor still needs it, compaction
keeps it.

That is the retention promise, and it has a price you can read directly:

```rust
let m = tree.metrics()?;
println!("versions kept alive for anchors: {}", m.pin_retained_versions);
```

`pin_retained_versions` is the number of versions compaction has retained *solely* because an
anchor needed them. It is the direct storage cost of your live branches. If it grows without bound,
you have branches nobody is using — delete them, or `detach` the ones that must outlive their
parent.

Two related counters:

- `compaction_pin_races` — compactions that discarded their output because a fork appeared while
  they were running. A few are normal; a lot means you are forking against heavy compaction.
- `timeline_horizon` — the timestamp range `ForkPoint::AtTimestamp` can still answer inside.

**Retention floors.** Once compaction *has* dropped versions below some sequence, that branch can
no longer serve a complete view below it. Forking below that point returns
`BelowRetentionFloor { requested, floor }` rather than silently handing you a view that is short of
rows. This is deliberate: a fork that quietly lost rows would be worse than a refused one.

## Detaching

`detach_branch` copies a branch's inherited view into its own tables and clears its parent link:

```rust
let copied = tree.detach_branch("long-lived")?; // returns how many keys were materialized
```

After this the branch is self-contained, its anchor no longer pins anything in the parent, and the
parent is free to compact. Use it for the branch that turned out to be permanent. It costs a copy
of everything the branch was inheriting, so it is the opposite trade from forking.

## Diff

A branch's diff is **its own writes** — overwrites of inherited keys, brand-new keys and deletes.
Keys it inherited and never touched are not changes and do not appear.

```rust
let feature = tree.branch("feature")?;
let diff = feature.diff()?;
for entry in diff.collect()? {
    // entry.key, entry.op (Set(value) or Delete), entry.seq, entry.timestamp
}
// Or stream it, optionally bounded to a key range:
let mut iter = diff.iter()?;
```

The diff streams and is resumable; it does not materialize the branch.

## Merging

A merge replays the source branch's diff onto a target.

### The base, and why it moves

A merge compares three things per key: the value the source has, the value the target has, and the
value at the **base** — the point the two branches last agreed. Initially the base is the source's
fork anchor. When a merge completes, it records a **promotion edge** on the target naming the
sequence it consumed, and that edge becomes the base for the next merge.

Without the edge, merging twice would re-apply the first merge's changes and report conflicts that
were already settled. With it, the second merge considers only what the source did *since*.

**A target records at most one edge per source branch**, and a new edge replaces the old one.

### Strategies

```rust
use surrealkv::{ConflictChoice, ConflictResolver, MergeStrategy};

let outcome = source.merge_into(&target, MergeStrategy::Strict).await?;
```

| Strategy | On a conflicting key |
|---|---|
| `Strict` (default) | refuses the whole merge with `MergeConflicts { count }`; nothing is written |
| `SourceWins` | takes the source's value |
| `TargetWins` | keeps the target's value; the source's change is dropped and counted in `resolved` |
| `Resolve(Arc<dyn ConflictResolver>)` | asks you, per conflict |

A resolver sees the whole conflict and can also refuse:

```rust
struct PreferLonger;
impl ConflictResolver for PreferLonger {
    fn resolve(&self, c: &surrealkv::Conflict) -> ConflictChoice {
        match (&c.source, &c.target) {
            (Some(s), Some(t)) if s.len() >= t.len() => ConflictChoice::Source,
            (Some(_), Some(_)) => ConflictChoice::Target,
            _ => ConflictChoice::Refuse,      // refuses the whole merge
        }
    }
}
let outcome = source
    .merge_into(&target, MergeStrategy::Resolve(Arc::new(PreferLonger)))
    .await?;
```

Resolving *through the merge* rather than by previewing and writing yourself is not just
convenience. Your own writes would land outside the merge's planning window, so a concurrent write
to the target between your preview and your write would not be caught. Inside the merge it is.

### Preview first

```rust
let report = source.preview_merge_into(&target)?;
println!("{} applies, {} conflicts", report.applies.len(), report.conflicts.len());
```

`preview_merge_into` computes exactly what `merge_into` would do and mutates nothing. It shares the
merge's classification code, so it cannot drift from the real thing.

### Atomic or resumable — check `chunks`

```rust
let outcome = source.merge_into(&target, MergeStrategy::SourceWins).await?;
assert!(outcome.chunks <= 1, "this merge was not atomic");
```

**`MergeOutcome::chunks` is how you tell**, and you should look at it:

- `chunks == 1` — the merge landed in **one transaction**. All or nothing.
- `chunks > 1` — the merge was too large for one batch and was split. **Each chunk is durable on
  its own.** A failure part-way leaves the earlier chunks applied. That is resumable, not atomic:
  re-running the merge continues from where it stopped, because the promotion edge advanced with
  the chunks that succeeded.
- `chunks == 0` — there was nothing to write.

If atomicity matters to you, assert on `chunks` rather than assuming it.

### Conditional merges

`merge_into_expecting` refuses unless the target is still where you last saw it — so you can make a
merge conditional on exactly the state you previewed:

```rust
let report = source.preview_merge_into(&target)?;
// ... a human approves the report ...
let outcome = source
    .merge_into_expecting(&target, MergeStrategy::Strict, Some(expected_head))
    .await?;
```

### Merging part of a branch

`merge_range` merges only the changes whose keys fall in a range:

```rust
use std::ops::Bound;
let outcome = source
    .merge_range(&target, MergeStrategy::SourceWins,
                 Bound::Included(b"user:".to_vec()), Bound::Excluded(b"user;".to_vec()))
    .await?;
```

**A scoped merge deliberately records no promotion edge.** The edge is sequence-based and says "the
target has everything the source did through sequence N" — which a partial apply has not earned.
Recording one would silently drop the unmerged keys from the next full merge. So after a
`merge_range`, a later full `merge_into` still considers everything since the original base, and
will re-apply the range you already merged (converging on it, since the values now match).

### Range bounds

`merge_range` and `revert_range` are the only APIs here that take `std::ops::Bound`, and all four
combinations mean exactly what they say, on **user keys**:

| Bound | Lower | Upper |
|---|---|---|
| `Included(k)` | starts at `k` | ends at `k`, `k` included |
| `Excluded(k)` | starts after every version of `k` | ends before `k` |
| `Unbounded` | from the first key | to the last key |

All versions of a user key are on the same side of a bound — you cannot merge "half a key". A range
that selects nothing applies nothing and returns `applied: 0`; it is not an error.

> Until 2026-08-15 an `Included` upper and an `Excluded` lower were widened for rows still in
> memory, so a scoped merge or revert could reach one key beyond its range, and the same call could
> behave differently before and after a flush. Both are fixed and covered by tests at the memtable,
> diff and public-API layers. If you are running an older build, prefer `Included` lower with
> `Excluded` upper, which was always exact.

## Reverting

`revert_range` writes back what the branch inherited at its anchor, for the keys it changed in a
range:

```rust
let restored = source.revert_range(Bound::Unbounded, Bound::Unbounded).await?;
```

It is a **compensating commit**, not a rewrite: history is append-only and the reverted values are
new writes at new sequences. Nothing that read the old values ever stops being able to.

## Time travel

Reading a branch as of a past point does not need a fork:

```rust
let txn = tree.begin_on("feature")?;
let value = txn.get_at(b"key", timestamp)?;
for version in txn.history(b"key")? { /* ... */ }
```

## Lifecycle

```rust
tree.create_branch("scratch")?;            // empty, inherits nothing — NOT a fork
tree.set_branch_ttl("scratch", Some(Duration::from_secs(3600)))?;
tree.delete_branch("scratch")?;
for info in tree.list_branches()? { /* name, id, generation, parent, last_write_seq, expires_at */ }
```

- **`create_branch` is not a fork.** It starts empty with no parent. Use `fork_branch` for
  copy-on-write.
- **TTL is swept, not timed.** An expired branch is tombstoned by the maintenance sweep, so expiry
  is observed at the next pass rather than exactly at the deadline. Between the two it is fenced,
  not readable.
- **A parent with live children cannot be deleted.** Delete the children first, or detach them.
- **Names are reusable; incarnations are not.** Re-creating a deleted name gives a new
  `generation`, and a handle to the old one is fenced rather than silently re-bound to the new
  branch.

## Named limits

| Limit | Value | What happens |
|---|---|---|
| Fork chain depth | 64 | deeper forks are refused |
| Branches in the catalog | 4096 | further creates are refused |
| Branch name length | 255 bytes | refused; names must also be non-empty, trimmed, and free of control characters |
| Promotion edges per target | one per source branch | a new merge from the same source replaces the old edge |

## Errors: retry, or don't

**Retryable — the same call may succeed if you make it again:**

| Error | Meaning |
|---|---|
| `TransactionWriteConflict` | another transaction wrote a key yours did; retry from a fresh snapshot |
| `TransactionRetry` | your snapshot is older than the commit oracle's GC window; begin again |
| `ForkFenceTimeout` | the commit pipeline did not drain in time; retry the fork |
| `CompactionPinRaced` | a compaction and a fork raced; the compaction discarded its output |

**Terminal — retrying does the same thing again:**

| Error | Meaning |
|---|---|
| `BranchFenced` | the branch is deleted, or your handle's generation is stale — get a new handle |
| `BranchesUnrelated` | the two branches share no ancestry, so there is no base to merge against |
| `BelowRetentionFloor { requested, floor }` | the data below `floor` has been compacted away; fork at or above `floor` |
| `MergeConflicts { count }` | `Strict` refused; choose another strategy or resolve the keys |
| `MergeTooLarge { estimated_bytes, budget_bytes }` | one chunk of the merge would exceed the batch budget |
| `TimestampBelowHorizon` | `AtTimestamp` fell below what the timeline can still answer |

## Observability

```rust
let m = tree.metrics()?;
```

| Field | Read it for |
|---|---|
| `forks`, `fork_drain_nanos` | how often you fork and what the store-wide fence costs |
| `merges`, `chunked_merges` | how many merges were not atomic |
| `detaches` | branches materialized away from their parent |
| `branches_reclaimed`, `tables_reclaimed` | what deleting branches actually freed |
| `pin_retained_versions` | **the storage price of your live branches** |
| `compaction_pin_races` | forking contending with compaction |
| `live_branches` | branches in the catalog now, `main` included |
| `timeline_horizon` | the range `AtTimestamp` can still answer inside |
| `wal_pinned_segments` | WAL segments held by branches that have not flushed |

## Not supported

`rebase`, tags, notes, and criss-cross / schema-aware / JSON / graph merges are explicit non-goals.
Bring work across branches with `merge_into`, `merge_range`, or by re-applying writes.
