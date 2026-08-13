# P2 Remote-Ready Immutable Table Format Evidence

Completed: 2026-08-13

P2 supplies one branch-pure immutable format and one range reader shared by Memory, SimStorage, and
Local object adapters. It remains private; P3 owns the first reachable database spine.

## Delivered format

- Branch-free internal key: user bytes, descending global commit version, row kind. A semantic
  comparator handles prefix keys; callers never compare the raw suffix encoding as a user-key
  boundary.
- Bounded row codec: commit timestamp, optional expiry, inline value, strict tombstone shape, and
  fail-closed decoding for truncation, invalid tags, trailing bytes, and configured lengths.
- Branch-pure table header/footer: owner branch and generation, table ID, format version, section
  ranges, version/timestamp bounds, and checksums.
- Streaming table builder: chunked immutable body, generated or supplied unique table ID, full
  SHA-256 digest, sorted-row enforcement, duplicate user-key/version rejection, and mixed-owner
  rejection.
- Independently checksummed LZ4 data blocks, checksum-protected index/filter/properties sections,
  user-key bloom filter, and fixed-width footer.
- Range-only reader: header/footer/metadata ranges, indexed block reads, bounded adjacent-block
  coalescing capped at 256 KiB, and no unconditional full-object read.
- Bounded validating cache: cache keys use table ID and offset; corrupted cached bytes are evicted
  and refetched; decoded rows are checked against the index and descriptor.
- Local immutable object adapter: encoded safe filenames, streamed chunk writes, one atomic
  hard-link publication, fsync of file and directory, ranged body reads, durable metadata in the
  same container, paginated listing, and idempotent deletion. Raw filesystem APIs remain confined
  to this adapter.

The P1 object body was narrowly amended from one contiguous `Bytes` to bounded chunks. Memory may
coalesce internally; Local writes chunks directly; remote adapters can upload them without adding a
fourth core role.

## Paired test evidence

Direct owner regressions cover:

- byte-exact branch-free key golden and semantic prefix/descending-version ordering;
- exhaustive row truncation, trailing bytes, invalid tombstone payload, and future format refusal;
- mixed owners, unsorted rows, duplicate user-key/version rows, and malicious LZ4 size prefixes;
- fixed footer location, full digest, owner/generation/descriptor mismatch, corrupt index/data
  checksums, and torn Local containers;
- corrupted cache eviction/refetch and bounded multi-block prefetch.

Highest-boundary tests cover:

- generated sorted maps compared against a `BTreeMap` point-read oracle;
- Memory, SimStorage, and Local publish and read identical table bytes through one `TableReader`;
- Local passes the same complete object-store contract as Memory and SimStorage and reopens its
  objects;
- a recording object store proves point reads and scans use strict subranges, scans coalesce
  adjacent blocks, and no request exceeds the prefetch cap;
- architecture guards prove rewrite filesystem dependencies do not escape the Local adapter.

## Adversarial review findings resolved

1. `put_unique(Bytes)` contradicted the streaming-builder goal by requiring one contiguous table
   allocation. It now receives an immutable chunked body without changing the three-role seam.
2. Same-key/same-version rows with distinct kinds were initially accepted. That is ambiguous and is
   now rejected by both builder and decoder.
3. A malicious compressed-size prefix could request an oversized decompression allocation. The
   prefix and compressed block are bounded before decompression.
4. Cache integrity alone did not prove cached rows matched the manifest/index. Every decoded block
   now revalidates last key, key/version/timestamp bounds, and sorted uniqueness.
5. The first range reader fetched one block per call and did not satisfy the explicit coalescing
   claim. Raw range scans now group adjacent blocks under a hard 256 KiB cap.
6. Table digest was calculated but initially not checked at open. Immutable object metadata must
   carry the descriptor digest and is validated before table reads. Per-section/block checksums
   localize later corruption.

## Exit commands

```text
cargo test rewrite --quiet
32 passed; 0 failed

cargo test --quiet
943 passed; 0 failed; 1 ignored
doctests: 4 passed; 0 failed; 5 ignored

cargo clippy --all-targets --all-features -- -D warnings
passed
```

P2 exit gate: satisfied. All required object adapters read the same ranged immutable format; table
code contains no filesystem semantics, branch-key rewriting, mmap, append, or rename assumption.
