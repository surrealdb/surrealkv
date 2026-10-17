//! Tests that the blocks in the block cache are found only by the table instance that read them.
//!
//! A table id names a file of a manifest, and another file can have the same id: after a crash
//! the next id that the manifest hands out is one that an orphan may already have used, and a
//! restore hands the same ids out again. A cache that is shared (a clone of an `Options` shares
//! it) must not give the blocks of the one to the other.

use std::sync::Arc;

use tempfile::TempDir;

use crate::compaction::leveled::Strategy;
use crate::sstable::block::BlockHandle;
use crate::sstable::index_block::IndexIterator;
use crate::sstable::table::{Footer, IndexType, Table, TableWriter, TABLE_FULL_FOOTER_LENGTH};
use crate::vlog::VLog;
use crate::{
	InternalKey,
	InternalKeyKind,
	LSMIterator,
	Options,
	Tree,
	VLogChecksumLevel,
	INTERNAL_KEY_SEQ_NUM_MAX,
};

const KEYS: usize = 60;

/// The value of key `i` in the table that `tag` names. Equal in length for every tag, so that two
/// tables of the same keys have their blocks at the same places.
fn value(tag: char, i: usize) -> Vec<u8> {
	format!("{tag}-value-{i:04}-{}", "x".repeat(16)).into_bytes()
}

fn lookup(i: usize) -> InternalKey {
	InternalKey::new(
		format!("key{i:04}").into_bytes(),
		INTERNAL_KEY_SEQ_NUM_MAX,
		InternalKeyKind::Set,
	)
}

/// A table file of `KEYS` keys, with the id 1, whose values the `tag` tells apart.
fn write_table(opts: &Arc<Options>, tag: char) -> Vec<u8> {
	let mut file = Vec::new();
	{
		let mut writer = TableWriter::new(&mut file, 1, Arc::clone(opts), 0);
		for i in 0..KEYS {
			let key = InternalKey::new(
				format!("key{i:04}").into_bytes(),
				(i + 1) as u64,
				InternalKeyKind::Set,
			);
			writer.add(key, &value(tag, i)).unwrap();
		}
		writer.finish().unwrap();
	}
	file
}

/// Opens a table, with the id 1 that all of these have, over a copy of `file`.
fn open(opts: &Arc<Options>, file: &[u8]) -> Table {
	Table::new(1, Arc::clone(opts), Arc::new(file.to_vec()), file.len() as u64).unwrap()
}

fn get(table: &Table, i: usize) -> Vec<u8> {
	table.get(&lookup(i)).unwrap().expect("the key is in the table").1
}

fn scan(table: &Table) -> Vec<Vec<u8>> {
	let mut iter = table.iter(None).unwrap();
	iter.seek_first().unwrap();
	let mut values = Vec::new();
	while iter.valid() {
		values.push(iter.value_encoded().unwrap().to_vec());
		iter.next().unwrap();
	}
	values
}

/// The handles of the data blocks of a file, read through a cache of their own.
fn data_handles(file: &[u8]) -> Vec<BlockHandle> {
	let table = open(&Arc::new(Options::default()), file);
	let IndexType::Partitioned(ref index) = table.index_block;
	let mut iter = IndexIterator::new(index);
	iter.seek_to_first().unwrap();
	let mut handles = Vec::new();
	while iter.valid() {
		handles.push(iter.block_handle().unwrap());
		iter.next().unwrap();
	}
	handles
}

fn footer(file: &[u8]) -> Footer {
	Footer::decode(&file[file.len() - TABLE_FULL_FOOTER_LENGTH..]).unwrap()
}

/// Two files that are not the same, and have a block at every offset where the other has one.
fn twin_tables(opts: &Arc<Options>) -> (Vec<u8>, Vec<u8>) {
	let (a, b) = (write_table(opts, 'A'), write_table(opts, 'B'));
	assert_ne!(a, b);
	assert_eq!(a.len(), b.len());
	let (fa, fb) = (footer(&a), footer(&b));
	assert_eq!(fa.index, fb.index);
	assert_eq!(fa.meta_index, fb.meta_index);
	let handles = data_handles(&a);
	assert!(!handles.is_empty());
	assert_eq!(handles, data_handles(&b));
	(a, b)
}

#[test]
fn tables_of_one_id_in_one_cache_each_read_their_own_blocks() {
	let opts = Arc::new(Options::default());
	let (a, b) = twin_tables(&opts);
	let table_a = open(&opts, &a);
	let table_b = open(&opts, &b);

	// Everything that A needs is in the cache.
	for i in 0..KEYS {
		assert_eq!(get(&table_a, i), value('A', i));
	}

	// B's first read finds none of it, though its blocks are where A's are, and its table has
	// the id that A's has.
	let before = opts.block_cache.get_stats();
	assert_eq!(get(&table_b, 0), value('B', 0));
	let after = opts.block_cache.get_stats();
	assert_eq!(after.data_hits, before.data_hits, "B was given a data block of A");
	assert_eq!(after.index_hits, before.index_hits, "B was given an index block of A");
	assert!(after.data_misses > before.data_misses);

	for i in 0..KEYS {
		assert_eq!(get(&table_b, i), value('B', i));
	}
	// And B did not take A's blocks over either.
	for i in 0..KEYS {
		assert_eq!(get(&table_a, i), value('A', i));
	}
}

#[test]
fn a_table_that_takes_the_id_of_a_dropped_one_is_not_served_its_blocks() {
	let opts = Arc::new(Options::default());
	let (a, b) = twin_tables(&opts);

	let table_a = open(&opts, &a);
	for i in 0..KEYS {
		assert_eq!(get(&table_a, i), value('A', i));
	}
	drop(table_a);

	// The same cache through a clone of the options, as another tree would have it, and the same
	// id for other contents, as after a crash or a restore.
	let cloned = Arc::new((*opts).clone());
	assert!(Arc::ptr_eq(&opts.block_cache, &cloned.block_cache));
	let table_b = open(&cloned, &b);
	let before = opts.block_cache.get_stats();
	assert_eq!(get(&table_b, 0), value('B', 0));
	assert_eq!(opts.block_cache.get_stats().data_hits, before.data_hits);
	assert_eq!(scan(&table_b), (0..KEYS).map(|i| value('B', i)).collect::<Vec<_>>());

	// The file that was dropped, opened again: a table instance of its own, which reads its
	// blocks again from the file instead of finding the ones that the dropped table left.
	let table_a = open(&opts, &a);
	let before = opts.block_cache.get_stats();
	assert_eq!(get(&table_a, 0), value('A', 0));
	let after = opts.block_cache.get_stats();
	assert_eq!(after.data_hits, before.data_hits);
	assert_eq!(after.index_hits, before.index_hits);
	assert_eq!(scan(&table_a), (0..KEYS).map(|i| value('A', i)).collect::<Vec<_>>());
}

#[test]
fn a_scan_of_one_table_is_not_served_the_blocks_of_a_scan_of_another() {
	let opts = Arc::new(Options::default());
	let (a, b) = twin_tables(&opts);

	assert_eq!(scan(&open(&opts, &a)), (0..KEYS).map(|i| value('A', i)).collect::<Vec<_>>());
	assert_eq!(scan(&open(&opts, &b)), (0..KEYS).map(|i| value('B', i)).collect::<Vec<_>>());
}

#[tokio::test]
async fn the_asynchronous_path_is_not_served_the_blocks_of_another_table_either() {
	let opts = Arc::new(Options::default());
	let (a, b) = twin_tables(&opts);
	let (table_a, table_b) = (open(&opts, &a), open(&opts, &b));

	for i in (0..KEYS).step_by(5) {
		let (_, found) = table_a.get_async(&lookup(i)).await.unwrap().unwrap();
		assert_eq!(found, value('A', i));
	}
	for i in (0..KEYS).step_by(5) {
		let (_, found) = table_b.get_async(&lookup(i)).await.unwrap().unwrap();
		assert_eq!(found, value('B', i));
	}
}

#[test]
fn clones_of_a_table_share_its_blocks_and_instances_of_a_file_do_not() {
	let opts = Arc::new(Options::default());
	let (a, _) = twin_tables(&opts);
	let table = open(&opts, &a);
	let clone = table.clone();
	let other = open(&opts, &a);

	assert_eq!(table.source.cache_id(), clone.source.cache_id());
	assert_ne!(table.source.cache_id(), other.source.cache_id());
	// The index of a table keeps its partitions in the namespace of the table.
	let IndexType::Partitioned(ref index) = table.index_block;
	assert_eq!(index.source.cache_id(), table.source.cache_id());

	assert_eq!(get(&table, 3), value('A', 3));

	// What the table read is what its clone finds.
	let before = opts.block_cache.get_stats();
	assert_eq!(get(&clone, 3), value('A', 3));
	let after = opts.block_cache.get_stats();
	assert!(after.data_hits > before.data_hits, "a clone did not find the block of its table");
	assert!(after.index_hits > before.index_hits);

	// And the instance of the same file, which has the same id, finds nothing of it.
	let before = opts.block_cache.get_stats();
	assert_eq!(get(&other, 3), value('A', 3));
	let after = opts.block_cache.get_stats();
	assert_eq!(after.data_hits, before.data_hits);
	assert_eq!(after.index_hits, before.index_hits);
}

/// A value log in a directory of its own, with the options `base`.
fn log_in(base: &Options) -> (VLog, TempDir, Arc<Options>) {
	let dir = TempDir::new().unwrap();
	let mut opts = base.clone();
	opts.path = dir.path().to_path_buf();
	crate::test::tfs::create_dir_all(&opts, opts.vlog_dir()).unwrap();
	let opts = Arc::new(opts);
	let log = VLog::new(Arc::clone(&opts)).unwrap();
	(log, dir, opts)
}

#[tokio::test]
async fn value_logs_in_one_cache_each_read_their_own_values() {
	// Two logs that are given clones of the same options have the same file ids and, for values
	// of the same size, the same offsets, and one block cache.
	let base = Options {
		vlog_checksum_verification: VLogChecksumLevel::Full,
		..Default::default()
	};
	let (log_a, _dir_a, opts_a) = log_in(&base);
	let (log_b, _dir_b, opts_b) = log_in(&base);
	assert!(Arc::ptr_eq(&opts_a.block_cache, &opts_b.block_cache));

	let key = b"the-same-key";
	let pointer_a = log_a.append(key, &[b'A'; 300]).unwrap();
	let pointer_b = log_b.append(key, &[b'B'; 300]).unwrap();
	log_a.sync().unwrap();
	log_b.sync().unwrap();
	assert_eq!(
		(pointer_a.file_id, pointer_a.offset, pointer_a.key_size, pointer_a.value_size),
		(pointer_b.file_id, pointer_b.offset, pointer_b.key_size, pointer_b.value_size),
		"the values are not where the test needs them to be"
	);

	assert_ne!(log_a.cache_id(pointer_a.file_id), log_b.cache_id(pointer_b.file_id));
	assert_eq!(log_a.get(&pointer_a).unwrap(), vec![b'A'; 300]);
	assert_eq!(log_b.get(&pointer_b).unwrap(), vec![b'B'; 300]);
	// Read again, from the cache.
	assert_eq!(log_a.get(&pointer_a).unwrap(), vec![b'A'; 300]);
	assert_eq!(log_b.get(&pointer_b).unwrap(), vec![b'B'; 300]);
}

// ========== Trees ==========

const TREE_KEYS: usize = 60;

fn tree_key(i: usize) -> Vec<u8> {
	format!("key{i:04}").into_bytes()
}

/// The value of key `i` in the tree that `tag` names, as written in `round`. Equal in length for
/// every tag and round, so that the tables of two trees have their blocks at the same places.
fn tree_value(tag: char, round: char, i: usize) -> Vec<u8> {
	format!("{tag}{round}-value-{i:04}-{}", "x".repeat(16)).into_bytes()
}

/// A tree in `dir` whose options are `base` and so share its block cache, and which neither
/// flushes nor compacts unless the test says so.
fn tree_sharing_the_cache_of(base: &Options, dir: &TempDir) -> (Tree, Arc<Options>) {
	let opts = Arc::new(Options {
		path: dir.path().to_path_buf(),
		max_memtable_size: 16 * 1024 * 1024,
		memtable_stall_threshold: 1000,
		level0_max_files: 2,
		l0_stall_threshold: 1000,
		..base.clone()
	});
	(Tree::new(Arc::clone(&opts)).unwrap(), opts)
}

/// Commits the keys in `from..to` with the values of `round`, and flushes them to a table.
async fn write_round(tree: &Tree, tag: char, round: char, from: usize, to: usize) {
	let mut txn = tree.begin().unwrap();
	for i in from..to {
		txn.set(tree_key(i), tree_value(tag, round, i)).unwrap();
	}
	txn.commit().await.unwrap();
	tree.flush().unwrap();
}

/// Reads every key of the tree: the first half was written again in round 2.
fn read_every_key(tree: &Tree, tag: char) {
	let txn = tree.begin().unwrap();
	for i in 0..TREE_KEYS {
		let round = if i < TREE_KEYS / 2 {
			'2'
		} else {
			'1'
		};
		let found = txn.get(tree_key(i).as_slice()).unwrap().expect("the key is in the tree");
		assert_eq!(found, tree_value(tag, round, i), "tree {tag}, key {i}");
	}
}

/// The ids of the tables that the manifest of the tree lists.
fn table_ids(tree: &Tree) -> Vec<u64> {
	let mut ids: Vec<u64> =
		tree.core.inner.level_manifest.read().unwrap().iter().map(|t| t.id).collect();
	ids.sort_unstable();
	ids
}

#[tokio::test]
async fn trees_that_share_a_cache_read_their_own_tables_before_and_after_a_compaction() {
	// Two trees built from clones of one set of options share a block cache, and each hands out
	// table ids from the start, so the files of their tables have the same ids, and here the same
	// keys, the same sizes and the same offsets, and values that differ.
	let base = Options::default();
	let (dir_a, dir_b) = (TempDir::new().unwrap(), TempDir::new().unwrap());
	let (tree_a, opts_a) = tree_sharing_the_cache_of(&base, &dir_a);
	let (tree_b, opts_b) = tree_sharing_the_cache_of(&base, &dir_b);
	assert!(Arc::ptr_eq(&opts_a.block_cache, &opts_b.block_cache));

	// The ids are taken as soon as a tree has its two tables, with no wait in between: a
	// background task that was woken when the tree was opened may merge the tables of a tree at
	// the next wait, and that must not change what the test compares.
	let mut before = Vec::new();
	for (tree, tag) in [(&tree_a, 'A'), (&tree_b, 'B')] {
		write_round(tree, tag, '1', 0, TREE_KEYS).await;
		write_round(tree, tag, '2', 0, TREE_KEYS / 2).await;
		before.push(table_ids(tree));
	}
	assert!(!before[0].is_empty());
	assert_eq!(before[0], before[1], "the trees must have tables of the same ids");

	read_every_key(&tree_a, 'A');
	read_every_key(&tree_b, 'B');
	read_every_key(&tree_a, 'A');

	// The merge of the tables of each tree writes a table whose id is the next that its manifest
	// hands out, which is the same id in both.
	for (tree, opts) in [(&tree_a, &opts_a), (&tree_b, &opts_b)] {
		tree.compact(Arc::new(Strategy::from_options(Arc::clone(opts)))).unwrap();
	}
	let after = table_ids(&tree_a);
	assert_eq!(after, table_ids(&tree_b), "the trees must have tables of the same ids");
	assert!(
		after.iter().any(|id| !before[0].contains(id)),
		"the merge wrote a table: {:?} became {after:?}",
		before[0]
	);

	read_every_key(&tree_b, 'B');
	read_every_key(&tree_a, 'A');
	read_every_key(&tree_b, 'B');

	tree_a.close().await.unwrap();
	tree_b.close().await.unwrap();
}
