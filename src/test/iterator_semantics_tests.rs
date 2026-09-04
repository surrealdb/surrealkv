use tempfile::TempDir;
use test_log::test;

use crate::lsm::Tree;
use crate::{LSMIterator, TreeBuilder};

fn create_store() -> (Tree, TempDir) {
	let temp_dir = TempDir::new().unwrap();
	let tree = TreeBuilder::new().with_path(temp_dir.path().to_path_buf()).build().unwrap();
	(tree, temp_dir)
}

/// After stepping forward, `prev()` must move to the key immediately preceding
/// the CURRENT POSITION in sorted order — not back to the previously returned
/// key. This is the RocksDB convention.
#[test(tokio::test)]
async fn prev_after_next_yields_preceding_key() {
	let (tree, _tmp) = create_store();

	let mut txn = tree.begin().unwrap();
	for k in ["a", "b", "c", "d"] {
		txn.set(k.as_bytes(), b"v".as_ref()).unwrap();
	}
	txn.commit().await.unwrap();

	let txn = tree.begin().unwrap();
	let mut iter = txn.range("a", "z").unwrap();

	assert!(iter.seek_first().unwrap());
	assert_eq!(iter.key().user_key(), b"a");
	assert!(iter.next().unwrap());
	assert_eq!(iter.key().user_key(), b"b");
	assert!(iter.next().unwrap());
	assert_eq!(iter.key().user_key(), b"c");

	assert!(iter.prev().unwrap());
	assert_eq!(iter.key().user_key(), b"b", "prev() must yield the key preceding the cursor");
}

/// Direction switching across tombstones. This is the shape of bug #380
/// (stack overflow on backward iteration over tombstoned keys).
#[test(tokio::test)]
async fn direction_switch_across_tombstones() {
	let (tree, _tmp) = create_store();

	let mut txn = tree.begin().unwrap();
	for k in ["a", "b", "c", "d", "e"] {
		txn.set(k.as_bytes(), b"v".as_ref()).unwrap();
	}
	txn.commit().await.unwrap();

	let mut txn = tree.begin().unwrap();
	for k in ["b", "c", "d"] {
		txn.delete(k.as_bytes()).unwrap();
	}
	txn.commit().await.unwrap();

	// Visible keys are now exactly ["a", "e"].
	let txn = tree.begin().unwrap();
	let mut iter = txn.range("a", "z").unwrap();

	assert!(iter.seek_first().unwrap());
	assert_eq!(iter.key().user_key(), b"a");
	assert!(iter.next().unwrap());
	assert_eq!(iter.key().user_key(), b"e");
	assert!(iter.prev().unwrap());
	assert_eq!(iter.key().user_key(), b"a");
	assert!(!iter.prev().unwrap(), "prev() from the first key must invalidate");
	assert!(!iter.valid());
}

/// `seek_last` then walking backward must enumerate every visible key in
/// reverse, and stepping past the start must invalidate rather than wrap.
#[test(tokio::test)]
async fn seek_last_walks_backward_to_exhaustion() {
	let (tree, _tmp) = create_store();

	let mut txn = tree.begin().unwrap();
	for k in ["a", "b", "c"] {
		txn.set(k.as_bytes(), b"v".as_ref()).unwrap();
	}
	txn.commit().await.unwrap();

	let txn = tree.begin().unwrap();
	let mut iter = txn.range("a", "z").unwrap();

	assert!(iter.seek_last().unwrap());
	let mut seen = Vec::new();
	while iter.valid() {
		seen.push(iter.key().user_key().to_vec());
		iter.prev().unwrap();
	}
	assert_eq!(seen, vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]);
}

/// `range` is start-inclusive and end-exclusive.
#[test(tokio::test)]
async fn range_bounds_are_start_inclusive_end_exclusive() {
	let (tree, _tmp) = create_store();

	let mut txn = tree.begin().unwrap();
	for k in ["a", "b", "c"] {
		txn.set(k.as_bytes(), b"v".as_ref()).unwrap();
	}
	txn.commit().await.unwrap();

	let txn = tree.begin().unwrap();
	let mut iter = txn.range("a", "c").unwrap();

	let mut seen = Vec::new();
	iter.seek_first().unwrap();
	while iter.valid() {
		seen.push(iter.key().user_key().to_vec());
		iter.next().unwrap();
	}
	assert_eq!(seen, vec![b"a".to_vec(), b"b".to_vec()]);
}

/// `seek_first` lands on the range's only visible key;
/// `next()` correctly runs past it and invalidates the iterator; but
/// `seek_last()`, called on that invalidated iterator, fails to reposition —
/// it stays invalid instead of landing back on the one visible key. This is
/// the #370-shaped bug: a direction switch after the iterator has been
/// driven off one end does not recover.
///
/// Reproduction requires a committed key beyond the range's exclusive end
/// (`k11` here, outside `[k00, k09)`): with only `k08` set, the bug does not
/// reproduce. Value size is irrelevant (reproduces with tiny values too, not
/// just the original counterexample's 2048-byte ones).
///
/// Root cause (fixed): `SkiplistIterator::last()` guarded its
/// walk-back-from-`tail` loop with `is_valid()`, which is false for a cursor
/// parked on the memoised `upper_node` sentinel — exactly the node the loop has
/// to step back off. `k11` is what makes the list's physically last node be that
/// sentinel.
#[test(tokio::test)]
async fn seek_last_recovers_after_next_runs_past_end() {
	let (tree, _tmp) = create_store();

	let mut txn = tree.begin().unwrap();
	txn.set(b"k08", b"v").unwrap();
	txn.set(b"k11", b"v").unwrap(); // outside [k00, k09) — required to reproduce
	txn.commit().await.unwrap();

	let txn = tree.begin().unwrap();
	let mut iter = txn.range(b"k00".to_vec(), b"k09".to_vec()).unwrap();

	assert!(iter.seek_first().unwrap());
	assert_eq!(iter.key().user_key(), b"k08");

	assert!(!iter.next().unwrap(), "next() past the last entry must invalidate");
	assert!(!iter.valid());

	assert!(iter.seek_last().unwrap(), "seek_last() must recover after next() ran past the end");
	assert_eq!(iter.key().user_key(), b"k08");
}

/// Post-flush variant of `seek_last_recovers_after_next_runs_past_end`. The
/// memtable-path repro above is a skiplist-iterator defect (a sticky
/// out-of-bounds node sentinel poisoning `SkiplistIterator::last`'s backward
/// walk); the root-cause investigation explicitly did NOT determine whether the
/// SSTable / B+tree iterators share it. Forcing a flush before reading takes the
/// memtable out of the picture entirely and answers that. It passed even before
/// the skiplist fix — the SSTable path re-seeks from scratch and caches no
/// out-of-bounds sentinel — so it stands as a guard, not a repro.
#[test(tokio::test)]
async fn seek_last_recovers_after_next_runs_past_end_post_flush() {
	let (tree, _tmp) = create_store();

	let mut txn = tree.begin().unwrap();
	txn.set(b"k08", b"v").unwrap();
	txn.set(b"k11", b"v").unwrap(); // outside [k00, k09)
	txn.commit().await.unwrap();
	tree.flush().unwrap(); // data now lives in an SST, not the memtable

	let txn = tree.begin().unwrap();
	let mut iter = txn.range(b"k00".to_vec(), b"k09".to_vec()).unwrap();

	assert!(iter.seek_first().unwrap());
	assert_eq!(iter.key().user_key(), b"k08");

	assert!(!iter.next().unwrap(), "next() past the last entry must invalidate");
	assert!(!iter.valid());

	assert!(iter.seek_last().unwrap(), "seek_last() must recover after next() ran past the end");
	assert_eq!(iter.key().user_key(), b"k08");
}

/// Symptom D of the write-set direction-switch defect: with the current entry
/// coming from the WRITE-SET and the snapshot iterator exhausted-forward, the
/// forward -> backward fixup used to reposition only the write-set cursor and
/// never re-anchor the snapshot iterator, so the committed key behind the cursor
/// became unreachable and `prev()` invalidated one step early.
///
/// Requires a MIX of committed and uncommitted data: the committed `k01` supplies
/// the snapshot entry that gets lost, the uncommitted `k02` puts `current_source`
/// on the write-set at the moment direction reverses.
#[test(tokio::test)]
async fn prev_recovers_committed_key_behind_uncommitted_key() {
	let (tree, _tmp) = create_store();

	let mut txn = tree.begin().unwrap();
	txn.set(b"k01", b"v").unwrap();
	txn.commit().await.unwrap();

	let mut txn = tree.begin().unwrap();
	txn.set(b"k02", b"v").unwrap();
	// (left uncommitted on purpose — k02 must come from the write-set)
	let mut iter = txn.range(b"k00".to_vec(), b"k03".to_vec()).unwrap();

	assert!(iter.seek_first().unwrap());
	assert_eq!(iter.key().user_key(), b"k01"); // from the snapshot
	assert!(iter.next().unwrap());
	assert_eq!(iter.key().user_key(), b"k02"); // from the write-set

	assert!(iter.prev().unwrap(), "prev() must step back onto the committed k01");
	assert_eq!(iter.key().user_key(), b"k01");
	assert!(!iter.prev().unwrap(), "prev() from the first key must invalidate");
}

/// This test's characterisation has been wrong twice before, both
/// times by reading cause out of a repro's incidental details:
///
/// - first as a "direction switch" bug (`seek_first()` then a mid-walk `seek_last()`) — disproved,
///   it reproduces with `seek_last()` as the FIRST positioning call;
/// - then as a tombstone bug ("a `Delete` immediately preceding the range's last live key") — also
///   disproved: swapping the `delete(b"k08")` for a plain `set(b"k08", ..)` reproduces identically.
///   The tombstone's only role was to be the second write-set entry.
///
/// The ACTUAL trigger is write-set *cardinality*: two or more write-set entries
/// in range with the cursor on the extreme one for the current direction, and an
/// invalid snapshot iterator. That made the direction-change fixup's
/// `!snapshot_iter.valid()` guard fire and teleport the write-set cursor back to
/// index 0, which `advance_ws()` then walked straight back to where it started —
/// so `next()` re-emitted `k09` instead of invalidating. An uncommitted
/// transaction with no committed data makes the snapshot permanently invalid,
/// which is why no commit and no memtable rotation are needed.
#[test(tokio::test)]
async fn next_invalidates_after_seek_last_on_multi_entry_write_set() {
	let (tree, _tmp) = create_store();

	let mut txn = tree.begin().unwrap();
	txn.set(b"k08", b"v").unwrap(); // second entry is all that matters
	txn.set(b"k09", b"v").unwrap();
	let mut iter = txn.range(b"k00".to_vec(), b"k10".to_vec()).unwrap();

	assert!(iter.seek_last().unwrap());
	assert_eq!(iter.key().user_key(), b"k09");

	assert!(!iter.next().unwrap(), "next() past the last entry must invalidate");
	assert!(!iter.valid());
}

/// The original tombstone-shaped spelling of the test above, kept as distinct
/// coverage rather than deleted. The tombstone is not the trigger (see that
/// test's doc comment) and `seek_last()` alone does not touch it — that lands
/// `ws_pos` on `k09`, the live entry. The trailing `prev()` is what walks the
/// cursor back onto the tombstone and so actually drives `position_to_max`'s
/// tombstone-skipping arm; without it this test routes through exactly the same
/// code as its non-tombstone sibling.
#[test(tokio::test)]
async fn next_invalidates_after_seek_last_when_tombstone_precedes_last_key() {
	let (tree, _tmp) = create_store();

	let mut txn = tree.begin().unwrap();
	// or soft_delete() — identical.
	txn.delete(b"k08").unwrap();
	txn.set(b"k09", b"v").unwrap();
	let mut iter = txn.range(b"k00".to_vec(), b"k10".to_vec()).unwrap();

	assert!(iter.seek_last().unwrap());
	assert_eq!(iter.key().user_key(), b"k09");

	assert!(!iter.next().unwrap(), "next() past the last entry must invalidate");
	assert!(!iter.valid());

	// Re-seek (an invalid iterator stays invalid until an explicit seek), then
	// step back onto the tombstone: it must be skipped, not emitted, and with
	// nothing live behind it the iterator must invalidate.
	assert!(iter.seek_last().unwrap());
	assert_eq!(iter.key().user_key(), b"k09");
	assert!(!iter.prev().unwrap(), "the k08 tombstone must be skipped, not emitted");
	assert!(!iter.valid());
}

/// The exact mirror of `next_invalidates_after_seek_last_on_multi_entry_write_set`:
/// same defect, same `!snapshot_iter.valid()` guard, the forward -> backward copy
/// of the block instead of the backward -> forward one. Discovered separately,
/// which is why it is a separate test.
///
/// `seek_first()` lands on the range's first visible key; `prev()` used to
/// teleport the write-set cursor to the LAST entry and then walk it back to index
/// 0, re-emitting `k01` instead of invalidating. Needs 2+ write-set entries for
/// the same reason as its mirror — at cardinality 1 the teleport is a no-op — and
/// an uncommitted transaction so the snapshot iterator stays invalid.
#[test(tokio::test)]
async fn prev_invalidates_from_first_key_seek_first_uncommitted() {
	let (tree, _tmp) = create_store();

	let mut txn = tree.begin().unwrap();
	txn.set(b"k01", b"v").unwrap();
	txn.set(b"k02", b"v").unwrap();
	// (transaction left uncommitted — read-your-own-writes only; required to
	// reproduce, see the doc comment above)
	let mut iter = txn.range(b"k00".to_vec(), b"k03".to_vec()).unwrap();

	assert!(iter.seek_first().unwrap());
	assert_eq!(iter.key().user_key(), b"k01");

	assert!(!iter.prev().unwrap(), "prev() from the first key must invalidate");
}
