//! The level task works off a backlog: one wake-up compacts until the strategy picks nothing that
//! moves tables down a level.

use std::sync::Arc;
use std::time::Duration;

use tempdir::TempDir;
use test_log::test;

use crate::compaction::leveled::Strategy;
use crate::lsm::CompactionOperations;
use crate::{LSMIterator, Options, Tree};

#[test(tokio::test)]
async fn one_wake_up_compacts_until_nothing_moves_down() {
	let temp_dir = TempDir::new("compaction_drain").unwrap();
	// Small tables and a small first level, so that the tables of the flushes below take many
	// units to move down: each unit past level 0 moves one table and what it overlaps.
	let options = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		max_memtable_size: 1024 * 1024,
		target_file_size: 4 * 1024,
		max_bytes_for_level: 8 * 1024,
		level0_max_files: 2,
		..Default::default()
	});
	let tree = Arc::new(Tree::new(Arc::clone(&options)).unwrap());
	for i in 0..8 {
		let mut tx = tree.begin().unwrap();
		for j in 0..400 {
			let k = format!("k_{:04}_{:02}", j, i).into_bytes();
			tx.set(k, vec![b'v'; 64]).unwrap();
		}
		tx.commit().await.unwrap();
		tree.flush().unwrap();
	}
	let strategy = Strategy::from_options(Arc::clone(&options));
	assert!(tree.core.inner.compaction_pending(&strategy), "the flushes leave a backlog");

	if let Some(ref tm) = *tree.core.task_manager.lock().unwrap() {
		tm.wake_up_level();
	}
	let start = std::time::Instant::now();
	while tree.core.inner.compaction_pending(&strategy) {
		assert!(
			start.elapsed() < Duration::from_secs(30),
			"the level task stopped with a compaction still pending"
		);
		tokio::time::sleep(Duration::from_millis(20)).await;
	}

	// Every entry is still there, once.
	let tx = tree.begin().unwrap();
	let mut iter = tx.range(b"k_".as_slice(), b"k`".as_slice()).unwrap();
	let mut seen = 0;
	let mut ok = iter.seek_first().unwrap();
	while ok {
		seen += 1;
		ok = iter.next().unwrap();
	}
	assert_eq!(seen, 8 * 400);
	tree.close().await.unwrap();
}
