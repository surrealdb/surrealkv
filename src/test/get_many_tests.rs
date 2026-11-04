use tempdir::TempDir;
use test_log::test;

use crate::{CompactRangeOptions, Error, Mode, Tree, TreeBuilder};

fn key(i: usize) -> Vec<u8> {
	format!("k{i:03}").into_bytes()
}

async fn commit(tree: &Tree, f: impl FnOnce(&mut crate::Transaction)) {
	let mut tx = tree.begin().unwrap();
	f(&mut tx);
	tx.commit().await.unwrap();
}

/// Builds a tree whose keys live in every place a read looks: tables of level 0, tables below
/// it, and the active memtable, with point deletes and range deletes over each.
async fn layered_tree() -> (Tree, TempDir) {
	let dir = TempDir::new("get_many").unwrap();
	let tree = TreeBuilder::new().with_path(dir.path().to_path_buf()).build().unwrap();
	commit(&tree, |tx| {
		for i in 0..100 {
			tx.set(key(i), format!("v1-{i}").into_bytes()).unwrap();
		}
	})
	.await;
	tree.flush().unwrap();
	commit(&tree, |tx| {
		for i in 10..20 {
			tx.set(key(i), format!("v2-{i}").into_bytes()).unwrap();
		}
		for i in 20..30 {
			tx.delete(key(i)).unwrap();
		}
		tx.delete_range(key(30), key(40)).unwrap();
	})
	.await;
	tree.flush().unwrap();
	// Push everything down, so that the next flush is a level 0 table over lower levels
	tree.compact_range(None, None, &CompactRangeOptions::default()).unwrap();
	commit(&tree, |tx| {
		for i in 50..60 {
			tx.set(key(i), format!("v3-{i}").into_bytes()).unwrap();
		}
		tx.delete(key(60)).unwrap();
		tx.delete_range(key(62), key(66)).unwrap();
		// A write after the range delete of the same commit
		tx.set(key(64), b"v3-64".to_vec()).unwrap();
	})
	.await;
	tree.flush().unwrap();
	// Left in the active memtable
	commit(&tree, |tx| {
		tx.set(key(70), b"v4-70".to_vec()).unwrap();
		tx.delete_range(key(80), key(85)).unwrap();
		tx.delete_range(key(2), key(4)).unwrap();
		tx.set(key(82), b"v4-82".to_vec()).unwrap();
	})
	.await;
	(tree, dir)
}

fn probe_keys() -> Vec<Vec<u8>> {
	// Every key, some past the data, and every tenth key twice
	let mut keys: Vec<Vec<u8>> = (0..110).map(key).collect();
	keys.extend((0..110).step_by(10).map(key));
	keys.push(b"a-before-everything".to_vec());
	keys.push(b"z-after-everything".to_vec());
	keys
}

fn per_key(tx: &crate::Transaction, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
	keys.iter().map(|k| tx.get(k.as_slice()).unwrap()).collect()
}

#[test(tokio::test)]
async fn get_many_answers_as_get_for_every_layer() {
	let (tree, _dir) = layered_tree().await;
	let tx = tree.begin().unwrap();
	let keys = probe_keys();
	let many = tx.get_many(&keys).unwrap();
	assert_eq!(many, per_key(&tx, &keys));
	// The comparison is not vacuous: each layer answers
	let at = |i: usize| many[i].clone();
	assert_eq!(at(5), Some(b"v1-5".to_vec()));
	assert_eq!(at(15), Some(b"v2-15".to_vec()));
	assert_eq!(at(25), None);
	assert_eq!(at(35), None);
	assert_eq!(at(55), Some(b"v3-55".to_vec()));
	assert_eq!(at(60), None);
	assert_eq!(at(63), None);
	assert_eq!(at(64), Some(b"v3-64".to_vec()));
	assert_eq!(at(70), Some(b"v4-70".to_vec()));
	assert_eq!(at(3), None);
	assert_eq!(at(82), Some(b"v4-82".to_vec()));
	assert_eq!(at(81), None);
	assert_eq!(at(105), None);
}

#[test(tokio::test)]
async fn get_many_reads_the_write_set_over_the_snapshot() {
	let (tree, _dir) = layered_tree().await;
	let mut tx = tree.begin().unwrap();
	tx.set(key(1), b"own".to_vec()).unwrap();
	tx.delete(key(5)).unwrap();
	tx.set(key(25), b"own-25".to_vec()).unwrap();
	tx.delete_range(key(90), key(95)).unwrap();
	tx.set(key(92), b"own-92".to_vec()).unwrap();
	tx.delete_range(key(52), key(54)).unwrap();
	tx.delete(key(70)).unwrap();
	tx.set(key(70), b"own-70".to_vec()).unwrap();
	let keys = probe_keys();
	let many = tx.get_many(&keys).unwrap();
	assert_eq!(many, per_key(&tx, &keys));
	assert_eq!(many[1], Some(b"own".to_vec()));
	assert_eq!(many[5], None);
	assert_eq!(many[25], Some(b"own-25".to_vec()));
	assert_eq!(many[91], None);
	assert_eq!(many[92], Some(b"own-92".to_vec()));
	assert_eq!(many[52], None);
	assert_eq!(many[70], Some(b"own-70".to_vec()));
}

#[test(tokio::test)]
async fn get_many_of_nothing_is_nothing() {
	let (tree, _dir) = layered_tree().await;
	let tx = tree.begin().unwrap();
	assert!(tx.get_many::<Vec<u8>>(&[]).unwrap().is_empty());
}

#[test(tokio::test)]
async fn get_many_fails_as_get_does() {
	let (tree, _dir) = layered_tree().await;
	let keys = vec![key(1), key(2)];

	let mut tx = tree.begin().unwrap();
	tx.rollback();
	assert!(matches!(tx.get_many(&keys), Err(Error::TransactionClosed)));

	let tx = tree.begin_with_mode(Mode::WriteOnly).unwrap();
	assert!(matches!(tx.get_many(&keys), Err(Error::TransactionWriteOnly)));

	let tx = tree.begin().unwrap();
	assert!(matches!(tx.get_many(&[key(1), Vec::new()]), Err(Error::EmptyKey)));
}

// ===== Versioned batches =====

const TIMES: [u64; 9] = [0, 5, 10, 15, 20, 25, 30, 35, u64::MAX];

/// A versioned tree with no background task, so that a test says when a memtable is sealed, a
/// table is written and tables are merged.
async fn versioned_tree() -> (Tree, TempDir) {
	let dir = TempDir::new("get_many_at").unwrap();
	let tree = TreeBuilder::new()
		.with_path(dir.path().to_path_buf())
		.with_versioning(true, 0)
		.build()
		.unwrap();
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	(tree, dir)
}

fn seal(tree: &Tree) {
	tree.core.inner.rotate_memtable().unwrap();
}

fn flush_sealed(tree: &Tree) {
	tree.core.inner.flush_all_immutables_sync().unwrap();
}

/// Writes versions to every place a read looks, at times 10, 20 and 30.
async fn layered_versioned_tree() -> (Tree, TempDir) {
	let (tree, dir) = versioned_tree().await;
	commit(&tree, |tx| {
		for i in 0..60 {
			tx.set_at(key(i), format!("t10-{i}").into_bytes(), 10).unwrap();
		}
	})
	.await;
	seal(&tree);
	flush_sealed(&tree);
	commit(&tree, |tx| {
		for i in 10..20 {
			tx.set_at(key(i), format!("t20-{i}").into_bytes(), 20).unwrap();
		}
		// Deleted from the time of the commit on
		for i in 20..30 {
			tx.soft_delete(key(i)).unwrap();
		}
		tx.delete_range(key(30), key(35)).unwrap();
	})
	.await;
	seal(&tree);
	flush_sealed(&tree);
	tree.compact_range(None, None, &CompactRangeOptions::default()).unwrap();
	// A table over the tables below it, and a version older than the one a table holds
	commit(&tree, |tx| {
		for i in 40..50 {
			tx.set_at(key(i), format!("t30-{i}").into_bytes(), 30).unwrap();
		}
		tx.set_at(key(45), b"t5-45".to_vec(), 5).unwrap();
	})
	.await;
	seal(&tree);
	flush_sealed(&tree);
	// A sealed memtable that is not flushed yet
	commit(&tree, |tx| {
		for i in 50..60 {
			tx.set_at(key(i), format!("t30-{i}").into_bytes(), 30).unwrap();
		}
		tx.delete_range(key(56), key(58)).unwrap();
		tx.set_at(key(57), b"t25-57".to_vec(), 25).unwrap();
	})
	.await;
	seal(&tree);
	// The active memtable
	commit(&tree, |tx| {
		tx.set_at(key(70), b"t25-70".to_vec(), 25).unwrap();
		tx.set_at(key(5), b"t35-5".to_vec(), 35).unwrap();
		tx.delete_range(key(1), key(3)).unwrap();
	})
	.await;
	(tree, dir)
}

fn per_key_at(tx: &crate::Transaction, keys: &[Vec<u8>], at: u64) -> Vec<Option<Vec<u8>>> {
	keys.iter().map(|k| tx.get_at(k.as_slice(), at).unwrap()).collect()
}

fn assert_batches_match(tx: &crate::Transaction, keys: &[Vec<u8>]) {
	for at in TIMES {
		assert_eq!(tx.get_many_at(keys, at).unwrap(), per_key_at(tx, keys, at), "at {at}");
	}
}

#[test(tokio::test)]
async fn get_many_at_answers_as_get_at_for_every_layer() {
	let (tree, _dir) = layered_versioned_tree().await;
	let tx = tree.begin().unwrap();
	let keys = probe_keys();
	assert_batches_match(&tx, &keys);
	// The comparison is not vacuous
	let at = |t: u64, i: usize| tx.get_many_at(&keys, t).unwrap()[i].clone();
	assert_eq!(at(10, 15), Some(b"t10-15".to_vec()));
	assert_eq!(at(25, 15), Some(b"t20-15".to_vec()));
	assert_eq!(at(5, 15), None);
	assert_eq!(at(25, 25), Some(b"t10-25".to_vec()));
	assert_eq!(at(u64::MAX, 25), None);
	assert_eq!(at(25, 32), None);
	assert_eq!(at(25, 42), Some(b"t10-42".to_vec()));
	assert_eq!(at(30, 42), Some(b"t30-42".to_vec()));
	assert_eq!(at(5, 45), Some(b"t5-45".to_vec()));
	assert_eq!(at(35, 51), Some(b"t30-51".to_vec()));
	assert_eq!(at(35, 70), Some(b"t25-70".to_vec()));
	assert_eq!(at(35, 5), Some(b"t35-5".to_vec()));
	assert_eq!(at(30, 5), Some(b"t10-5".to_vec()));
	assert_eq!(at(35, 2), None);
}

#[test(tokio::test)]
async fn get_many_at_reads_the_write_set_over_the_snapshot() {
	let (tree, _dir) = layered_versioned_tree().await;
	let mut tx = tree.begin().unwrap();
	tx.set_at(key(11), b"own-11".to_vec(), 12).unwrap();
	tx.set_at(key(12), b"own-12-late".to_vec(), 50).unwrap();
	tx.soft_delete(key(13)).unwrap();
	tx.set_at(key(26), b"own-26".to_vec(), 15).unwrap();
	tx.delete_range(key(90), key(95)).unwrap();
	tx.set_at(key(92), b"own-92".to_vec(), 5).unwrap();
	tx.delete_range(key(42), key(44)).unwrap();
	tx.replace(key(46), b"own-46".to_vec()).unwrap();
	assert_batches_match(&tx, &probe_keys());
}

#[test(tokio::test)]
async fn get_many_at_holds_through_a_flush_and_a_compaction() {
	let (tree, _dir) = layered_versioned_tree().await;
	let tx = tree.begin().unwrap();
	let keys = probe_keys();
	let before: Vec<_> = TIMES.iter().map(|at| tx.get_many_at(&keys, *at).unwrap()).collect();
	let before_latest = tx.get_many(&keys).unwrap();
	// What the transaction reads does not move when the tree under it does: the sealed memtable
	// is written out, tables are merged, and a writer commits over the keys
	flush_sealed(&tree);
	commit(&tree, |tx| {
		for i in 0..60 {
			tx.set_at(key(i), b"later".to_vec(), 40).unwrap();
		}
	})
	.await;
	seal(&tree);
	flush_sealed(&tree);
	tree.compact_range(None, None, &CompactRangeOptions::default()).unwrap();
	for (at, want) in TIMES.iter().zip(&before) {
		assert_eq!(&tx.get_many_at(&keys, *at).unwrap(), want, "at {at}");
		assert_eq!(&per_key_at(&tx, &keys, *at), want, "at {at}");
	}
	assert_eq!(tx.get_many(&keys).unwrap(), before_latest);
	// A transaction begun after sees the writer
	let tx = tree.begin().unwrap();
	assert_batches_match(&tx, &keys);
	assert_eq!(tx.get_many_at(&[key(3)], 40).unwrap(), vec![Some(b"later".to_vec())]);
}

#[test(tokio::test)]
async fn get_many_at_fails_as_get_at_does() {
	let (tree, _dir) = layered_versioned_tree().await;
	let keys = vec![key(1), key(2)];

	let mut tx = tree.begin().unwrap();
	tx.rollback();
	assert!(matches!(tx.get_many_at(&keys, 10), Err(Error::TransactionClosed)));

	let tx = tree.begin_with_mode(Mode::WriteOnly).unwrap();
	assert!(matches!(tx.get_many_at(&keys, 10), Err(Error::TransactionWriteOnly)));

	let tx = tree.begin().unwrap();
	assert!(matches!(tx.get_many_at(&[key(1), Vec::new()], 10), Err(Error::EmptyKey)));
	assert!(tx.get_many_at::<Vec<u8>>(&[], 10).unwrap().is_empty());

	// A tree that keeps no versions refuses a versioned read
	let (plain, _plain_dir) = layered_tree().await;
	let tx = plain.begin().unwrap();
	assert!(matches!(tx.get_many_at(&keys, 10), Err(Error::InvalidArgument(_))));
	assert!(matches!(tx.get_at(key(1), 10), Err(Error::InvalidArgument(_))));
}
