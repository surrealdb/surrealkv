//! Tests for what happens when a WAL record cannot be written.
//!
//! A WAL append that fails part-way can leave the start of a record in the segment. Recovery
//! stops at the first damaged record, so every record acknowledged after it would be dropped. A
//! failed append must cut the segment back to the last complete record and refuse further appends
//! until the writer is replaced by a rotation.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use crate::batch::Batch;
use crate::wal::manager::Wal;
use crate::wal::parallel_recovery::decode_segment_batches;
use crate::wal::{segment_name, Options as WalOptions};
use crate::{InternalKeyKind, Options, Tree, WalRecoveryMode};

// ---------------------------------------------------------------------------
// a failed append
// ---------------------------------------------------------------------------

fn wal_record(seq: u64, key: &str, value_len: usize) -> Vec<u8> {
	let mut batch = Batch::new(seq);
	batch
		.add_record(InternalKeyKind::Set, key.as_bytes().to_vec(), Some(vec![b'v'; value_len]), 0)
		.unwrap();
	batch.encode().unwrap()
}

const SEGMENT: &str = "00000000000000000000.wal";

fn file_len(path: &Path) -> u64 {
	fs::metadata(path).unwrap().len()
}

/// Keys of every record in every segment, in order. Fails on a segment that
/// does not parse end to end.
fn recovered_keys(dir: &Path) -> Vec<String> {
	let mut segments: Vec<PathBuf> = fs::read_dir(dir)
		.unwrap()
		.map(|e| e.unwrap().path())
		.filter(|p| p.extension().is_some_and(|ext| ext == "wal"))
		.collect();
	segments.sort();
	let mut keys = Vec::new();
	for segment in segments {
		let id: u64 = segment.file_stem().unwrap().to_str().unwrap().parse().unwrap();
		let (_, batches) = decode_segment_batches(&segment, id)
			.unwrap_or_else(|e| panic!("{} must parse end to end: {e}", segment.display()));
		for batch in batches {
			keys.extend(batch.entries.iter().map(|e| String::from_utf8(e.key.clone()).unwrap()));
		}
	}
	keys
}

/// Fails the append of a record at every point where it can fail. After each
/// failure the segment holds exactly the acknowledged records, the writer
/// refuses further appends, and a rotation gives a working writer again.
fn append_failure_sweep(value_len: usize, min_failure_points: usize) {
	// Each pass fails one write later, so `fail_after` is the number of writes
	// that have been failed so far when the append finally succeeds.
	for fail_after in 0..64 {
		let dir = TempDir::new().unwrap();
		let segment = dir.path().join(SEGMENT);
		let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
		for i in 0..3 {
			wal.append(&wal_record(i + 1, &format!("acked{i}"), 100)).unwrap();
		}
		let acked_len = file_len(&segment);

		wal.fail_writes_after(fail_after);
		let result = wal.append(&wal_record(4, "failed", value_len));
		if result.is_ok() {
			// Past the last write of this record: the control.
			assert!(
				fail_after >= min_failure_points,
				"the record has only {fail_after} failure points, expected {min_failure_points}"
			);
			assert_eq!(recovered_keys(dir.path()).len(), 4);
			return;
		}

		let at = format!("failure after {fail_after} writes of a {value_len} byte value");
		assert_eq!(
			file_len(&segment),
			acked_len,
			"{at}: part of the record is left in the segment"
		);
		assert!(
			wal.append(&wal_record(5, "later", 100)).is_err(),
			"{at}: a poisoned writer must refuse further appends"
		);
		assert_eq!(file_len(&segment), acked_len, "{at}: a refused append wrote to the segment");

		// The segment is still sound for recovery, and a rotation replaces the writer.
		wal.rotate().unwrap();
		wal.append(&wal_record(6, "after", 100)).unwrap();
		wal.close().unwrap();
		assert_eq!(
			recovered_keys(dir.path()),
			["acked0", "acked1", "acked2", "after"],
			"{at}: recovery must see exactly the acknowledged records"
		);
	}
	panic!("the append never succeeded");
}

#[test]
fn a_failed_append_of_a_small_record_leaves_nothing_behind() {
	append_failure_sweep(64, 3);
}

#[test]
fn a_failed_append_of_a_two_block_record_leaves_nothing_behind() {
	append_failure_sweep(40_000, 6);
}

/// A 100 KB record is four fragments, so a failure on the Nth write lands
/// inside the record with earlier fragments already in the file.
#[test]
fn a_failed_append_inside_a_multi_fragment_record_leaves_nothing_behind() {
	append_failure_sweep(100_000, 10);
}

/// A record whose append failed was never acknowledged. It must not come back
/// at recovery because a later flush wrote out what was left in the buffer.
#[test]
fn a_failed_append_is_never_resurrected() {
	let dir = TempDir::new().unwrap();
	let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
	wal.append(&wal_record(1, "acked", 100)).unwrap();

	// Header and payload are buffered, and the flush at the end of the append fails.
	wal.fail_writes_after(2);
	assert!(wal.append(&wal_record(2, "failed", 100)).is_err());

	// Rotating syncs the old segment, which flushes whatever the writer still holds.
	wal.rotate().unwrap();
	wal.append(&wal_record(3, "after", 100)).unwrap();
	wal.close().unwrap();
	assert_eq!(recovered_keys(dir.path()), ["acked", "after"]);
}

/// The same through the whole tree: a commit whose WAL append fails is an
/// error, later commits fail cleanly, and a restart recovers exactly the
/// commits that were acknowledged.
#[test_log::test(tokio::test)]
async fn tree_recovers_exactly_the_acknowledged_commits_after_a_failed_append() {
	let dir = TempDir::new().unwrap();
	let o = Arc::new(Options {
		path: dir.path().to_path_buf(),
		flush_on_close: false,
		wal_recovery_mode: WalRecoveryMode::AbsoluteConsistency,
		..Default::default()
	});

	let tree = Tree::new(Arc::clone(&o)).unwrap();
	// Opening continues in a fresh segment, so the one commits go to is not segment 0.
	let active = tree.core.inner.wal.read().get_active_log_number();
	let segment = o.wal_dir().join(segment_name(active, "wal"));
	for i in 0..5 {
		let mut txn = tree.begin().unwrap();
		txn.set(format!("acked{i}").as_bytes(), &[b'x'; 64]).unwrap();
		txn.commit().await.unwrap();
	}
	let acked_len = file_len(&segment);

	// A commit large enough to span blocks, failing inside the record.
	tree.core.inner.wal.write().fail_writes_after(8);
	let mut txn = tree.begin().unwrap();
	txn.set(b"failed", vec![b'y'; 100_000]).unwrap();
	let outcome = tokio::time::timeout(Duration::from_secs(20), txn.commit()).await;
	assert!(matches!(outcome, Ok(Err(_))), "the commit must report the failed append: {outcome:?}");
	assert_eq!(file_len(&segment), acked_len, "no part of the failed record may stay");

	// The writer is poisoned: later commits fail rather than land after damage.
	let mut txn = tree.begin().unwrap();
	txn.set(b"later", &[b'z'; 64]).unwrap();
	let outcome = tokio::time::timeout(Duration::from_secs(20), txn.commit()).await;
	assert!(matches!(outcome, Ok(Err(_))), "a later commit must fail cleanly: {outcome:?}");
	assert_eq!(file_len(&segment), acked_len);

	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	drop(tree);

	// Recovery in the strictest mode: nothing to repair, nothing lost, nothing extra.
	let tree = Tree::new(Arc::clone(&o)).expect("the segment must recover without repair");
	let txn = tree.begin().unwrap();
	for i in 0..5 {
		assert_eq!(txn.get(format!("acked{i}").as_bytes()).unwrap(), Some(vec![b'x'; 64]));
	}
	assert_eq!(txn.get(b"failed").unwrap(), None);
	assert_eq!(txn.get(b"later").unwrap(), None);
}
