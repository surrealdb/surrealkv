//! The memory memtables hold is bounded by `max_memtable_size`, values included.

use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::{Options, Tree};

/// The bytes the memtables of `tree` hold: the active one and those waiting for a flush.
fn held(tree: &Tree) -> u64 {
	let active = tree.core.inner.active_memtable.read().unwrap().used();
	let immutable = tree.core.inner.immutable_memtables.read().unwrap().total_size() as u64;
	active + immutable
}

/// Rows of ordinary size whose values add up to ten memtables, and then one value larger than a
/// memtable. The arena of the rows alone would take every row without filling, so a bound on the
/// arena alone would let the memtable hold all of them. Counting the values, the memtable rotates,
/// and what the memtables hold stays within the memtables a writer may queue before it stalls.
#[test(tokio::test)]
async fn the_memtables_hold_no_more_than_their_size_whatever_the_values_weigh() {
	const MEMTABLE: usize = 4 << 20;
	const VALUE: usize = 2 << 10;
	let dir = TempDir::new("memtable_memory").unwrap();
	let options = Arc::new(Options {
		path: dir.path().to_path_buf(),
		max_memtable_size: MEMTABLE,
		..Default::default()
	});
	let tree = Tree::new(Arc::clone(&options)).unwrap();
	// The active memtable, the ones a writer may queue before it stalls, and one commit more.
	let bound = ((options.memtable_stall_threshold + 1) * MEMTABLE + 64 * VALUE) as u64;

	let rows = 10 * MEMTABLE / VALUE;
	let mut most = 0;
	for chunk in 0..rows / 64 {
		let mut tx = tree.begin().unwrap();
		for i in 0..64 {
			let row = chunk * 64 + i;
			tx.set(format!("row{row:08}").as_bytes(), vec![(row % 251) as u8; VALUE]).unwrap();
		}
		tx.commit().await.unwrap();
		most = most.max(held(&tree));
		assert!(most <= bound, "the memtables hold {most} bytes, more than {bound}");
	}
	let tables = tree.core.inner.level_manifest.read().unwrap().get_all_tables().len();
	assert!(tables > 0, "the memtable never rotated");

	// One value larger than a memtable is written to a table of its own, not held.
	let mut tx = tree.begin().unwrap();
	tx.set(b"huge", vec![7u8; 2 * MEMTABLE]).unwrap();
	tx.commit().await.unwrap();
	assert!(held(&tree) <= bound, "the memtables hold {} bytes after the huge value", held(&tree));

	let tx = tree.begin().unwrap();
	assert_eq!(tx.get(b"huge").unwrap().map(|v| v.len()), Some(2 * MEMTABLE));
	assert_eq!(tx.get(b"row00000000").unwrap().map(|v| v.len()), Some(VALUE));
	drop(tx);
	tree.close().await.unwrap();
}
