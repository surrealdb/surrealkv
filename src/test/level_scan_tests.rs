//! Range reads over levels of many tables, each level read as one sorted run, with newer
//! versions and deletes above them in other levels and in the memtable.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::{CompactRangeOptions, LSMIterator, Mode, Options, Tree};

/// A tree that flushes and compacts only when a test asks, with small tables, so that a level
/// holds many of them.
async fn quiet(path: &Path) -> Arc<Tree> {
	let tree = Arc::new(
		Tree::new(Arc::new(Options {
			path: path.to_path_buf(),
			level0_max_files: 100,
			l0_stall_threshold: 1000,
			memtable_stall_threshold: 1000,
			flush_on_close: false,
			target_file_size: 4 * 1024,
			block_size: 512,
			..Default::default()
		}))
		.unwrap(),
	);
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	tree
}

fn key(i: usize) -> Vec<u8> {
	format!("key{i:05}").into_bytes()
}

fn shape(tree: &Tree) -> Vec<usize> {
	let manifest = tree.core.inner.level_manifest.read().unwrap();
	manifest.levels.0.iter().map(|level| level.tables.len()).collect()
}

fn to_level(level: u8) -> CompactRangeOptions {
	CompactRangeOptions {
		target_level: Some(level),
		rewrite_bottommost: false,
		..Default::default()
	}
}

/// Writes every key into level 2, rewrites a third of them into level 1, deletes some of those
/// in level 0, and rewrites others in the memtable. Returns the tree and what it holds.
async fn layered(dir: &Path) -> (Arc<Tree>, BTreeMap<Vec<u8>, Vec<u8>>) {
	let tree = quiet(dir).await;
	let mut expected = BTreeMap::new();
	fn write(
		txn: &mut crate::Transaction,
		expected: &mut BTreeMap<Vec<u8>, Vec<u8>>,
		i: usize,
		gen: u8,
	) {
		let value = vec![gen; 40 + i % 30];
		txn.set(key(i), &value).unwrap();
		expected.insert(key(i), value);
	}
	let mut txn = tree.begin().unwrap();
	for i in 0..2000 {
		write(&mut txn, &mut expected, i, 1);
	}
	txn.commit().await.unwrap();
	tree.compact_range(None, None, &to_level(2)).unwrap();

	let mut txn = tree.begin().unwrap();
	for i in (0..2000).step_by(3) {
		write(&mut txn, &mut expected, i, 2);
	}
	txn.commit().await.unwrap();
	tree.compact_range(None, None, &to_level(1)).unwrap();

	let mut txn = tree.begin().unwrap();
	for i in (0..2000).step_by(7) {
		txn.delete(key(i)).unwrap();
	}
	txn.commit().await.unwrap();
	for i in (0..2000).step_by(7) {
		expected.remove(&key(i));
	}
	tree.flush().unwrap();

	let mut txn = tree.begin().unwrap();
	for i in (1..2000).step_by(11) {
		write(&mut txn, &mut expected, i, 3);
	}
	txn.commit().await.unwrap();

	let shape = shape(&tree);
	assert!(shape[0] >= 1 && shape[1] > 10 && shape[2] > 10, "levels of many tables: {shape:?}");
	(tree, expected)
}

fn forward(it: &mut impl LSMIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
	let mut out = Vec::new();
	it.seek_first().unwrap();
	while it.valid() {
		out.push((it.key().user_key().to_vec(), it.value().unwrap()));
		it.next().unwrap();
	}
	out
}

fn backward(it: &mut impl LSMIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
	let mut out = Vec::new();
	it.seek_last().unwrap();
	while it.valid() {
		out.push((it.key().user_key().to_vec(), it.value().unwrap()));
		it.prev().unwrap();
	}
	out
}

fn within(
	expected: &BTreeMap<Vec<u8>, Vec<u8>>,
	lo: &[u8],
	hi: &[u8],
) -> Vec<(Vec<u8>, Vec<u8>)> {
	expected.range(lo.to_vec()..hi.to_vec()).map(|(k, v)| (k.clone(), v.clone())).collect()
}

#[test(tokio::test)]
async fn a_range_reads_every_level_in_order() {
	let dir = TempDir::new("level_scan_order").unwrap();
	let (tree, expected) = layered(dir.path()).await;
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let all: Vec<_> = expected.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

	let mut it = txn.range(b"key".as_slice(), b"kez".as_slice()).unwrap();
	assert_eq!(forward(&mut it), all);
	let mut reversed = all;
	reversed.reverse();
	assert_eq!(backward(&mut it), reversed);

	// Ranges that start and end inside tables of every level.
	let mut rng = fastrand::Rng::with_seed(11);
	for _ in 0..50 {
		let (a, b) = (rng.usize(0..2100), rng.usize(0..2100));
		let (lo, hi) = (key(a.min(b)), key(a.max(b)));
		let want = within(&expected, &lo, &hi);
		let mut it = txn.range(lo.as_slice(), hi.as_slice()).unwrap();
		assert_eq!(forward(&mut it), want, "{a}..{b}");
		let mut reversed = want.clone();
		reversed.reverse();
		assert_eq!(backward(&mut it), reversed, "{a}..{b} backward");
	}
}

#[test(tokio::test)]
async fn a_range_turns_and_seeks_across_tables() {
	let dir = TempDir::new("level_scan_turns").unwrap();
	let (tree, expected) = layered(dir.path()).await;
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let keys: Vec<&Vec<u8>> = expected.keys().collect();
	let mut it = txn.range(b"key".as_slice(), b"kez".as_slice()).unwrap();

	// A walk that steps forward and back at random, checked against the expected position.
	let mut rng = fastrand::Rng::with_seed(5);
	for _ in 0..20 {
		let mut at = rng.usize(0..keys.len());
		assert!(it.seek(keys[at]).unwrap());
		for _ in 0..300 {
			assert_eq!(it.key().user_key(), keys[at].as_slice());
			assert_eq!(it.value().unwrap(), expected[keys[at]]);
			if rng.bool() {
				if at + 1 == keys.len() {
					assert!(!it.next().unwrap());
					break;
				}
				assert!(it.next().unwrap());
				at += 1;
			} else {
				if at == 0 {
					assert!(!it.prev().unwrap());
					break;
				}
				assert!(it.prev().unwrap());
				at -= 1;
			}
		}
	}

	// A seek to a key no level holds lands on the next key that is held.
	for i in (0..2000).step_by(7) {
		let want = expected.range(key(i)..).next().map(|(k, _)| k.clone());
		let found = it.seek(&key(i)).unwrap().then(|| it.key().user_key().to_vec());
		assert_eq!(found, want, "seek to deleted key{i:05}");
	}
}

#[test(tokio::test)]
async fn a_range_read_in_chunks_sees_every_entry_once() {
	let dir = TempDir::new("level_scan_chunks").unwrap();
	let (tree, expected) = layered(dir.path()).await;
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let all: Vec<_> = expected.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

	// As SurrealDB reads a scan: a new iterator for every chunk, from just after the last key.
	let mut seen = Vec::new();
	let mut lo = b"key".to_vec();
	loop {
		let mut it = txn.range(lo.as_slice(), b"kez".as_slice()).unwrap();
		it.seek_first().unwrap();
		let mut n = 0;
		while it.valid() && n < 37 {
			seen.push((it.key().user_key().to_vec(), it.value().unwrap()));
			it.next().unwrap();
			n += 1;
		}
		if !it.valid() {
			break;
		}
		lo = it.key().user_key().to_vec();
	}
	assert_eq!(seen, all);
}
