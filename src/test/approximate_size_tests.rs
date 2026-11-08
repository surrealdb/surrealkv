//! `Tree::approximate_size`: the table bytes it attributes to a key range, from the metadata of
//! the tables.

use std::path::Path;
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::{CompactRangeOptions, Error, Options, Tree};

/// The data block size of the trees here: small, so that a few thousand keys span many blocks and
/// the share of a table that a range counts is finer than a block.
const BLOCK: usize = 4096;

async fn quiet(path: &Path) -> Arc<Tree> {
	let tree = Arc::new(
		Tree::new(Arc::new(Options {
			path: path.to_path_buf(),
			// Nothing is flushed or compacted unless a test does it.
			level0_max_files: 100,
			l0_stall_threshold: 1000,
			memtable_stall_threshold: 1000,
			flush_on_close: false,
			block_size: BLOCK,
			..Default::default()
		}))
		.unwrap(),
	);
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	tree
}

/// Writes `count` keys `{prefix}{i:05}`, each with a value of 100 distinct bytes, so that the
/// table spans many data blocks and compresses little.
async fn put(tree: &Tree, prefix: &str, count: usize) {
	let mut txn = tree.begin().unwrap();
	for i in 0..count {
		let value: Vec<u8> =
			(0..100u32).map(|j| (i as u32).wrapping_mul(31).wrapping_add(j * 7) as u8).collect();
		txn.set(format!("{prefix}{i:05}").as_bytes(), &value).unwrap();
	}
	txn.commit().await.unwrap();
}

/// Moves every write into tables in the last level.
fn settle(tree: &Tree) {
	tree.compact_range(None, None, &CompactRangeOptions::default()).unwrap();
}

/// The sum of the file sizes of every table.
fn files(tree: &Tree) -> u64 {
	let manifest = tree.core.inner.level_manifest.read().unwrap();
	manifest.levels.0.iter().flat_map(|level| level.tables.iter()).map(|t| t.file_size).sum()
}

/// How far off a bound may be in each table it cuts.
fn block() -> u64 {
	BLOCK as u64
}

fn size(tree: &Tree, start: Option<&str>, end: Option<&str>) -> u64 {
	tree.approximate_size(start.map(str::as_bytes), end.map(str::as_bytes)).unwrap()
}

fn assert_near(actual: u64, expected: u64, tolerance: u64, what: &str) {
	assert!(
		actual.abs_diff(expected) <= tolerance,
		"{what}: {actual} is not within {tolerance} of {expected}"
	);
}

#[test(tokio::test)]
async fn an_empty_tree_has_no_size() {
	let dir = TempDir::new("approximate_size_empty").unwrap();
	let tree = quiet(dir.path()).await;
	assert_eq!(size(&tree, None, None), 0);
	assert_eq!(size(&tree, Some("a"), Some("z")), 0);
}

#[test(tokio::test)]
async fn the_open_range_is_the_size_of_every_table() {
	let dir = TempDir::new("approximate_size_all").unwrap();
	let tree = quiet(dir.path()).await;
	put(&tree, "a", 2000).await;
	put(&tree, "m", 2000).await;
	settle(&tree);
	put(&tree, "z", 500).await;
	tree.compact_range(
		None,
		None,
		&CompactRangeOptions {
			target_level: Some(0),
			..Default::default()
		},
	)
	.unwrap();

	let total = files(&tree);
	assert!(total > 0);
	assert_eq!(size(&tree, None, None), total);
	// Bounds outside every key cover every table whole.
	assert_eq!(size(&tree, Some(""), Some("zz")), total);
}

#[test(tokio::test)]
async fn writes_in_the_memtable_are_not_counted() {
	let dir = TempDir::new("approximate_size_memtable").unwrap();
	let tree = quiet(dir.path()).await;
	put(&tree, "a", 1000).await;
	assert_eq!(size(&tree, None, None), 0);
	settle(&tree);
	assert!(size(&tree, None, None) > 0);
}

#[test(tokio::test)]
async fn a_range_counts_the_share_of_a_table_between_its_bounds() {
	let dir = TempDir::new("approximate_size_share").unwrap();
	let tree = quiet(dir.path()).await;
	put(&tree, "k", 10_000).await;
	settle(&tree);
	let total = files(&tree);
	assert!(total > 100 * block(), "the table spans many data blocks");

	let half = size(&tree, Some("k00000"), Some("k05000"));
	assert_near(half, total / 2, total / 20, "the first half of the keys");
	let quarter = size(&tree, Some("k05000"), Some("k07500"));
	assert_near(quarter, total / 4, total / 20, "a quarter of the keys in the middle");
}

#[test(tokio::test)]
async fn adjacent_ranges_add_up_to_their_union() {
	let dir = TempDir::new("approximate_size_adjacent").unwrap();
	let tree = quiet(dir.path()).await;
	put(&tree, "a", 3000).await;
	put(&tree, "b", 3000).await;
	put(&tree, "c", 3000).await;
	settle(&tree);
	let total = size(&tree, None, None);

	let parts = size(&tree, None, Some("b"))
		+ size(&tree, Some("b"), Some("b01500"))
		+ size(&tree, Some("b01500"), Some("c"))
		+ size(&tree, Some("c"), None);
	// Each inner bound cuts at most one table per level, and is off by up to one data block,
	// scaled by the share of the file the index and filter take.
	let levels = tree.core.inner.level_manifest.read().unwrap().levels.0.len() as u64;
	assert_near(parts, total, 3 * levels * 2 * block(), "the parts against the whole");
}

#[test(tokio::test)]
async fn a_range_between_the_keys_counts_next_to_nothing() {
	let dir = TempDir::new("approximate_size_gap").unwrap();
	let tree = quiet(dir.path()).await;
	put(&tree, "a", 3000).await;
	put(&tree, "z", 3000).await;
	settle(&tree);

	assert!(size(&tree, Some("m"), Some("n")) <= 2 * block(), "no key is between m and n");
	assert_eq!(size(&tree, Some("zz"), None), 0, "every key is before zz");
	assert_eq!(size(&tree, None, Some("a")), 0, "every key is at or after a");
}

#[test(tokio::test)]
async fn a_start_after_the_end_is_refused() {
	let dir = TempDir::new("approximate_size_inverted").unwrap();
	let tree = quiet(dir.path()).await;
	let err = tree.approximate_size(Some(b"z"), Some(b"a")).unwrap_err();
	assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");
	assert_eq!(size(&tree, Some("a"), Some("a")), 0, "an empty range is allowed");
}
