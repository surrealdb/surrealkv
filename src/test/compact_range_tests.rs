//! `Tree::compact_range`: the tables it merges, the data it reclaims, and what it leaves to
//! readers.

use std::path::Path;
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::{CompactRangeOptions, Error, Options, Tree};

async fn quiet(path: &Path) -> Arc<Tree> {
	let tree = Arc::new(
		Tree::new(Arc::new(Options {
			path: path.to_path_buf(),
			// Nothing is flushed or compacted unless a test does it.
			level0_max_files: 100,
			l0_stall_threshold: 1000,
			memtable_stall_threshold: 1000,
			flush_on_close: false,
			..Default::default()
		}))
		.unwrap(),
	);
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	tree
}

async fn put(tree: &Tree, prefix: &str, count: usize) {
	let mut txn = tree.begin().unwrap();
	for i in 0..count {
		txn.set(format!("{prefix}{i:04}").as_bytes(), b"value").unwrap();
	}
	txn.commit().await.unwrap();
}

async fn remove(tree: &Tree, prefix: &str, count: usize) {
	let mut txn = tree.begin().unwrap();
	for i in 0..count {
		txn.delete(format!("{prefix}{i:04}").as_bytes()).unwrap();
	}
	txn.commit().await.unwrap();
}

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin().unwrap().get(key.as_bytes()).unwrap()
}

/// The number of tables in each level.
fn shape(tree: &Tree) -> Vec<usize> {
	let manifest = tree.core.inner.level_manifest.read().unwrap();
	manifest.levels.0.iter().map(|level| level.tables.len()).collect()
}

fn no_flush_target(level: u8) -> CompactRangeOptions {
	CompactRangeOptions {
		target_level: Some(level),
		rewrite_bottommost: false,
		..Default::default()
	}
}

#[test(tokio::test)]
async fn a_range_moves_only_the_tables_that_overlap_it() {
	let dir = TempDir::new("compact_range_scope").unwrap();
	let tree = quiet(dir.path()).await;

	// Two disjoint tables in level 1.
	put(&tree, "a", 20).await;
	tree.compact_range(None, None, &no_flush_target(1)).unwrap();
	put(&tree, "z", 20).await;
	tree.compact_range(Some(b"z"), None, &no_flush_target(1)).unwrap();
	assert_eq!(shape(&tree)[..3], [0, 2, 0]);

	tree.compact_range(Some(b"z"), None, &no_flush_target(2)).unwrap();
	assert_eq!(shape(&tree)[..3], [0, 1, 1], "only the table of z moves down");
	assert_eq!(get(&tree, "a0003"), Some(b"value".to_vec()));
	assert_eq!(get(&tree, "z0003"), Some(b"value".to_vec()));
}

#[test(tokio::test)]
async fn a_range_after_the_data_compacts_nothing() {
	let dir = TempDir::new("compact_range_miss").unwrap();
	let tree = quiet(dir.path()).await;
	put(&tree, "a", 20).await;
	tree.compact_range(None, None, &no_flush_target(1)).unwrap();
	let before = shape(&tree);

	tree.compact_range(Some(b"m"), Some(b"n"), &CompactRangeOptions::default()).unwrap();
	assert_eq!(shape(&tree), before);
}

#[test(tokio::test)]
async fn compacting_deleted_data_to_the_last_level_leaves_no_tables() {
	let dir = TempDir::new("compact_range_reclaim").unwrap();
	let tree = quiet(dir.path()).await;
	put(&tree, "k", 50).await;
	tree.compact_range(None, None, &CompactRangeOptions::default()).unwrap();
	assert!(shape(&tree).iter().sum::<usize>() > 0);

	remove(&tree, "k", 50).await;
	tree.compact_range(None, None, &CompactRangeOptions::default()).unwrap();
	assert_eq!(shape(&tree).iter().sum::<usize>(), 0, "tombstones and values are dropped");
	assert_eq!(get(&tree, "k0001"), None);
}

#[test(tokio::test)]
async fn compaction_keeps_what_an_open_transaction_can_read() {
	let dir = TempDir::new("compact_range_snapshot").unwrap();
	let tree = quiet(dir.path()).await;
	put(&tree, "k", 10).await;
	let reader = tree.begin().unwrap();

	remove(&tree, "k", 10).await;
	tree.compact_range(None, None, &CompactRangeOptions::default()).unwrap();

	assert_eq!(reader.get(b"k0001").unwrap(), Some(b"value".to_vec()));
	assert_eq!(get(&tree, "k0001"), None);
}

#[test(tokio::test)]
async fn data_in_the_memtable_is_compacted_unless_the_flush_is_off() {
	let dir = TempDir::new("compact_range_flush").unwrap();
	let tree = quiet(dir.path()).await;
	put(&tree, "k", 10).await;
	let off = CompactRangeOptions {
		flush_memtable: false,
		..Default::default()
	};
	tree.compact_range(None, None, &off).unwrap();
	assert_eq!(shape(&tree).iter().sum::<usize>(), 0);

	tree.compact_range(None, None, &CompactRangeOptions::default()).unwrap();
	assert!(shape(&tree).iter().sum::<usize>() > 0);
	assert_eq!(get(&tree, "k0001"), Some(b"value".to_vec()));
}

#[test(tokio::test)]
async fn a_reversed_range_is_refused_and_a_closed_tree_stops() {
	let dir = TempDir::new("compact_range_errors").unwrap();
	let tree = Tree::new(Arc::new(Options {
		path: dir.path().to_path_buf(),
		..Default::default()
	}))
	.unwrap();
	let result = tree.compact_range(Some(b"b"), Some(b"a"), &CompactRangeOptions::default());
	assert!(matches!(result, Err(Error::InvalidArgument(_))));

	tree.close().await.unwrap();
	let result = tree.compact_range(None, None, &CompactRangeOptions::default());
	assert!(matches!(result.as_ref().map_err(Error::stopped_by), Err(Error::DatabaseStopped(_))));
}
