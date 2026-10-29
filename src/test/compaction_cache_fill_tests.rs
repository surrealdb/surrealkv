//! Tests that a compaction does not fill the block cache with the blocks of the tables it merges.
//!
//! Those tables are deleted when the compaction finishes, and their blocks are keyed by table
//! instance, so nothing reads them from the cache again. Put there, they would only take the
//! place of blocks that readers use, and under a steady write load keep the cache at its capacity.

use std::path::Path;
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::sstable::table::{Table, TableWriter};
use crate::{CompactRangeOptions, InternalKey, InternalKeyKind, LSMIterator, Options, Tree};

const KEYS: usize = 2000;

fn write_table(opts: &Arc<Options>) -> Vec<u8> {
	let mut file = Vec::new();
	{
		let mut writer = TableWriter::new(&mut file, 1, Arc::clone(opts), 0);
		for i in 0..KEYS {
			let key = InternalKey::new(
				format!("key{i:05}").into_bytes(),
				(i + 1) as u64,
				InternalKeyKind::Set,
			);
			writer.add(key, format!("value-{i:05}-{}", "x".repeat(64)).as_bytes()).unwrap();
		}
		writer.finish().unwrap();
	}
	file
}

/// Scans every entry of the table, and returns how many there were.
fn scan(table: &Table, fill_cache: bool) -> usize {
	let iter = table.iter(None).unwrap();
	let mut iter = if fill_cache {
		iter
	} else {
		iter.without_cache_fill()
	};
	iter.seek_first().unwrap();
	let mut entries = 0;
	while iter.valid() {
		entries += 1;
		iter.next().unwrap();
	}
	entries
}

#[test]
fn a_scan_without_cache_fill_leaves_the_cache_as_it_was() {
	let opts = Arc::new(Options::default());
	let file = write_table(&opts);
	let table =
		Table::new(1, Arc::clone(&opts), Arc::new(file.clone()), file.len() as u64).unwrap();

	assert_eq!(scan(&table, false), KEYS);
	let stats = opts.block_cache.get_stats();
	assert!(stats.data_misses > 1, "the table has more than one data block");
	assert_eq!(stats.data_inserts, 0);

	// A second such scan reads every block from disk again.
	opts.block_cache.reset_stats();
	assert_eq!(scan(&table, false), KEYS);
	let again = opts.block_cache.get_stats();
	assert_eq!(again.data_hits, 0);
	assert_eq!(again.data_misses, stats.data_misses);
	assert_eq!(again.data_inserts, 0);
}

#[test]
fn a_scan_without_cache_fill_reads_the_blocks_that_the_cache_holds() {
	let opts = Arc::new(Options::default());
	let file = write_table(&opts);
	let table =
		Table::new(1, Arc::clone(&opts), Arc::new(file.clone()), file.len() as u64).unwrap();

	assert_eq!(scan(&table, true), KEYS);
	let filled = opts.block_cache.get_stats();
	assert_eq!(filled.data_inserts, filled.data_misses);

	opts.block_cache.reset_stats();
	assert_eq!(scan(&table, false), KEYS);
	let stats = opts.block_cache.get_stats();
	assert_eq!(stats.data_misses, 0);
	assert_eq!(stats.data_hits, filled.data_misses);
}

async fn quiet(path: &Path) -> Arc<Tree> {
	let tree = Arc::new(
		Tree::new(Arc::new(Options {
			path: path.to_path_buf(),
			// Nothing is flushed or compacted unless the test does it.
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

async fn put(tree: &Tree, tag: &str) {
	let mut txn = tree.begin().unwrap();
	for i in 0..KEYS {
		txn.set(format!("key{i:05}").as_bytes(), format!("{tag}-{}", "x".repeat(64)).as_bytes())
			.unwrap();
	}
	txn.commit().await.unwrap();
}

fn no_flush_target(level: u8) -> CompactRangeOptions {
	CompactRangeOptions {
		target_level: Some(level),
		rewrite_bottommost: false,
		..Default::default()
	}
}

#[test(tokio::test)]
async fn a_compaction_puts_no_data_block_into_the_cache() {
	let dir = TempDir::new("compaction_cache_fill").unwrap();
	let tree = quiet(dir.path()).await;

	// A table in level 1, and a newer version of every key over it, merged into level 2.
	put(&tree, "old").await;
	tree.compact_range(None, None, &no_flush_target(1)).unwrap();
	put(&tree, "new").await;
	tree.core.opts.block_cache.reset_stats();
	tree.compact_range(None, None, &no_flush_target(2)).unwrap();

	let stats = tree.core.opts.block_cache.get_stats();
	assert!(stats.data_misses > 1, "the compaction read the data blocks of its inputs");
	assert_eq!(stats.data_inserts, 0, "the compaction put blocks of its inputs into the cache");

	// Readers still fill it.
	let txn = tree.begin().unwrap();
	assert_eq!(txn.get(b"key00007").unwrap(), Some(format!("new-{}", "x".repeat(64)).into_bytes()));
	assert!(tree.core.opts.block_cache.get_stats().data_inserts > 0);
}
