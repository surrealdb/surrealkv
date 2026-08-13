# P1 Contracts and Testkit Evidence

Completed: 2026-08-13

P1 is deliberately private. The existing public `Tree` cannot reach the rewrite kernel; P3 owns
that cutover. This phase establishes the contracts and independent oracle needed to prevent table,
branch, and backend work from growing platform-specific policy.

## Delivered boundaries

- `rewrite/api`: fixed-width identifiers, generations, versions, timestamps, authority fences,
  durability classes, stable error codes, and typed results.
- `rewrite/storage`: `ObjectStore`, `CommitStore`, `Platform`, `Bindings`, complete capability
  negotiation, and explicit publication/reconciliation outcomes.
- `rewrite/storage/memory`: ranged immutable objects, unique/idempotent publication, deterministic
  pagination, idempotent deletion, single-writer authority fencing, exact operation retries, and
  rejection of operation-ID reuse with different bytes.
- `rewrite/storage/sim`: test-only injected before/after failures and crash materialization. Durable
  objects, authority lineage, clock, and entropy progress survive a crash; queued work and fault
  scripts do not.
- `rewrite/branch`: an intentionally simple copy-on-fork logical oracle for global commit order,
  exact selectors, branch isolation, generations, delete/recreate, tombstones, TTL, scans, and
  history. It does not share the future LSM representation.
- Empty ownership modules reserve `format`, `table`, `commit`, and `lifecycle` without introducing a
  second reachable runtime.

## Paired test evidence

Direct owner regressions:

- `storage::memory::tests::object_owner_regression_covers_range_unique_put_and_delete`
- `storage::memory::tests::authority_owner_regression_covers_conflict_and_writer_fence`
- `storage::memory::tests::authority_owner_regression_rejects_operation_id_reuse_with_new_bytes`
- `branch::tests::owner_model_masks_older_values_with_tombstones_and_ttl`
- `branch::tests::owner_model_accepts_global_selector_above_sparse_branch_head`

Highest-boundary tests:

- the same object and authority contract functions run against Memory and SimStorage;
- capability mismatch is rejected before any object mutation, while SimStorage proves the complete
  crash-durable requirement set succeeds;
- timeout before authority mutation reconciles as not committed; timeout after mutation reconciles
  as confirmed before and after crash;
- pre/post object failures prove distinct durable results after crash;
- crash preserves clock and entropy progress while discarding process-local scheduled work;
- the branch script performs multi-key commits, exact fork, isolation, historical reads, tombstone
  masking, TTL masking, scan/history, delete, and same-name recreation, with a step-count
  non-vacuity assertion;
- architecture guards prove SimStorage and its fault hooks are compiled only under `cfg(test)`.

## Adversarial review findings resolved

1. Initial capability validation omitted pagination, idempotent deletion, and unknown-outcome
   reconciliation. These are now explicit requirements rather than assumptions.
2. Initial SimStorage crash construction reset its deterministic random seed and could therefore
   reuse generated identifiers. The durable simulation image now carries entropy progress.
3. The first model rejected a global version that was newer than a sparse branch's last local
   commit. Global selectors now validate against the published database version; branch history
   naturally remains sparse.
4. The first model evaluated latest-read TTL at the branch head timestamp, so values could not
   expire while the database was idle. Latest reads now use the model's monotonic current time;
   version and timestamp reads remain deterministic.
5. Exact authority retries originally had no negative proof for operation-ID reuse with different
   bytes. Both direct and backend-contract tests now distinguish those cases.

## Exit commands

```text
cargo test rewrite --quiet
16 passed; 0 failed

cargo test --quiet
927 passed; 0 failed; 1 ignored
doctests: 4 passed; 0 failed; 5 ignored

cargo clippy --all-targets --all-features -- -D warnings
passed
```

P1 exit gate: satisfied. Memory and SimStorage share contracts; backend code contains no branch or
table policy; the oracle executes branch and temporal behavior without an LSM; fault machinery is
test-only.
