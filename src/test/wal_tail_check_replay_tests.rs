//! The replay of a log with the check of its last segment, called as the open and the restore of a
//! checkpoint call it, over a log of one segment and over a log of several.
//!
//! The log is made by hand: values appended to a value log, and batches whose values are pointers
//! to them, in segments that a rotation ends. A log of several segments is replayed by the
//! parallel replay, and one of a single segment by the serial one: the check is made before either.

use std::path::Path;
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use crate::batch::Batch;
use crate::lsm::Core;
use crate::memtable::MemTable;
use crate::vlog::{VLog, ValueLocation, ValuePointer};
use crate::wal::manager::Wal;
use crate::wal::tail::TailCheck;
use crate::wal::{Options as WalOptions, WalContext};
use crate::{InternalKeyKind, Options, WalRecoveryMode};

const ARENA: usize = 1 << 20;
const ENTRIES: usize = 9;

fn key(i: usize) -> Vec<u8> {
	format!("key{i:05}").into_bytes()
}

fn value(i: usize) -> Vec<u8> {
	vec![i as u8; 200]
}

/// A value log with `ENTRIES` entries, and a log in `segments` segments of the batches that point
/// to them, one batch for each entry, in order.
fn build(dir: &Path, segments: usize) -> (VLog, Vec<ValuePointer>) {
	let mut opts = Options {
		path: dir.to_path_buf(),
		vlog_max_file_size: 1 << 20,
		..Options::default()
	};
	opts.fs = crate::vfs::default_fs();
	std::fs::create_dir_all(opts.vlog_dir()).unwrap();
	let vlog = VLog::new(Arc::new(opts)).unwrap();
	let pointers: Vec<_> = (0..ENTRIES).map(|i| vlog.append(&key(i), &value(i)).unwrap()).collect();
	vlog.flush().unwrap();

	let wal_dir = dir.join("wal");
	std::fs::create_dir_all(&wal_dir).unwrap();
	let mut wal = Wal::open(&wal_dir, WalOptions::default()).unwrap();
	let per_segment = ENTRIES.div_ceil(segments);
	for (i, pointer) in pointers.iter().enumerate() {
		if i > 0 && i % per_segment == 0 {
			wal.rotate().unwrap();
		}
		let mut batch = Batch::new(i as u64 + 1);
		batch
			.add_record(
				InternalKeyKind::Set,
				key(i),
				Some(ValueLocation::with_pointer(pointer.clone()).encode()),
				0,
			)
			.unwrap();
		wal.append(&batch.encode().unwrap()).unwrap();
	}
	wal.close().unwrap();
	(vlog, pointers)
}

struct Replayed {
	memtables: Vec<Arc<MemTable>>,
	max_seq: Option<u64>,
	report: crate::WalRecoveryReport,
}

impl Replayed {
	fn has(&self, i: usize) -> bool {
		self.memtables.iter().any(|memtable| memtable.get(&key(i), None).is_some())
	}

	fn keys(&self) -> Vec<usize> {
		(0..ENTRIES).filter(|i| self.has(*i)).collect()
	}
}

fn replay(dir: &Path, vlog: Option<&VLog>, mode: WalRecoveryMode) -> crate::Result<Replayed> {
	let mut memtables = Vec::new();
	let mut tail = vlog.map(TailCheck::new);
	let (max_seq, active, report) = Core::replay_wal_in(
		&WalContext::plaintext(),
		&dir.join("wal"),
		0,
		"test",
		mode,
		ARENA,
		tail.as_mut(),
		|memtable, _, _| {
			memtables.push(memtable);
			Ok(())
		},
	)?;
	memtables.extend(active);
	Ok(Replayed {
		memtables,
		max_seq,
		report,
	})
}

fn cut_value_log(vlog_dir: &Path, pointer: &ValuePointer, keep: usize) {
	let file = vlog_dir.join("vlog").join(format!("{:020}.vlog", pointer.file_id));
	let bytes = std::fs::read(&file).unwrap();
	std::fs::write(&file, &bytes[..pointer.offset as usize + keep]).unwrap();
}

#[test]
fn a_log_whose_values_are_there_is_replayed_whole_by_the_serial_and_the_parallel_replay() {
	for segments in [1, 3] {
		let dir = TempDir::new().unwrap();
		let (vlog, _) = build(dir.path(), segments);
		for mode in [
			WalRecoveryMode::AbsoluteConsistency,
			WalRecoveryMode::TolerateTornTail,
			WalRecoveryMode::SalvageValidPrefix,
		] {
			let replayed = replay(dir.path(), Some(&vlog), mode).unwrap();
			assert_eq!(replayed.keys(), (0..ENTRIES).collect::<Vec<_>>(), "{segments} segments");
			assert_eq!(replayed.max_seq, Some(ENTRIES as u64));
			assert_eq!(replayed.report.records_dropped, 0);
			assert_eq!(replayed.report.segments_replayed, segments);
		}
	}
}

#[test]
fn the_last_segment_ends_before_the_first_value_that_is_missing_whatever_the_number_of_segments() {
	for segments in [1, 3] {
		for mode in [WalRecoveryMode::SalvageValidPrefix, WalRecoveryMode::TolerateTornTail] {
			let dir = TempDir::new().unwrap();
			let (vlog, pointers) = build(dir.path(), segments);
			// The value log ends in the middle of the seventh entry, which is in the last segment.
			cut_value_log(dir.path(), &pointers[6], 20);

			let replayed = replay(dir.path(), Some(&vlog), mode).unwrap();
			let what = format!("{segments} segments, {mode:?}");
			assert_eq!(replayed.keys(), (0..6).collect::<Vec<_>>(), "{what}");
			assert_eq!(replayed.max_seq, Some(6), "{what}: the last record that is kept");
			assert_eq!(replayed.report.records_dropped, 3, "{what}");
			assert_eq!(replayed.report.quarantined_files.len(), 1, "{what}");
			assert!(replayed.report.bytes_trimmed > 0, "{what}");

			// The log that is left holds what was kept, and replays whole.
			let again = replay(dir.path(), None, WalRecoveryMode::AbsoluteConsistency).unwrap();
			assert_eq!(again.keys(), (0..6).collect::<Vec<_>>(), "{what}");
		}
	}
}

#[test]
fn absolute_consistency_names_the_segment_and_the_record_for_every_number_of_segments() {
	for segments in [1, 3] {
		let dir = TempDir::new().unwrap();
		let (vlog, pointers) = build(dir.path(), segments);
		cut_value_log(dir.path(), &pointers[7], 0);
		let before = std::fs::read_dir(dir.path().join("wal")).unwrap().count();

		let error = replay(dir.path(), Some(&vlog), WalRecoveryMode::AbsoluteConsistency)
			.err()
			.expect("refused");
		let last = std::fs::read_dir(dir.path().join("wal"))
			.unwrap()
			.map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
			.max()
			.unwrap();
		match error {
			crate::Error::WalCorruption {
				segment_id,
				..
			} => assert_eq!(format!("{segment_id:020}.wal"), last, "{segments} segments"),
			other => panic!("{segments} segments: {other:?}"),
		}
		assert_eq!(std::fs::read_dir(dir.path().join("wal")).unwrap().count(), before);
	}
}

/// A value that is missing in a segment that a rotation ended is not looked for: the rotation
/// synced the value log before it, and the open does not read an older segment's entries.
#[test]
fn a_value_that_is_missing_before_the_last_segment_is_not_looked_for() {
	let dir = TempDir::new().unwrap();
	let (vlog, pointers) = build(dir.path(), 3);
	// The value log ends in the second entry, and every entry after it is gone with it, those of
	// the older segments and those of the last.
	cut_value_log(dir.path(), &pointers[1], 5);
	let replayed = replay(dir.path(), Some(&vlog), WalRecoveryMode::SalvageValidPrefix).unwrap();
	// The last segment is cut at its first record, and the older segments are replayed whole.
	assert_eq!(replayed.keys(), (0..6).collect::<Vec<_>>());
	assert_eq!(replayed.report.records_dropped, 3);
}

#[test]
fn a_log_without_a_value_log_is_not_held_to_one() {
	let dir = TempDir::new().unwrap();
	let (_vlog, pointers) = build(dir.path(), 3);
	cut_value_log(dir.path(), &pointers[6], 20);
	let replayed = replay(dir.path(), None, WalRecoveryMode::AbsoluteConsistency).unwrap();
	assert_eq!(replayed.keys(), (0..ENTRIES).collect::<Vec<_>>());
	assert_eq!(replayed.report.records_dropped, 0);
}

// ---------------------------------------------------------------------------------------------
// The restore of a checkpoint
// ---------------------------------------------------------------------------------------------

fn tree_options(path: &Path, mode: WalRecoveryMode) -> Arc<Options> {
	let mut opts = Options {
		path: path.to_path_buf(),
		enable_vlog: true,
		vlog_value_threshold: 64,
		vlog_checksum_verification: crate::VLogChecksumLevel::Full,
		wal_recovery_mode: mode,
		..Options::default()
	};
	opts.fs = crate::vfs::default_fs();
	Arc::new(opts)
}

fn copy_dir(from: &Path, to: &Path) {
	std::fs::create_dir_all(to).unwrap();
	for entry in std::fs::read_dir(from).unwrap() {
		let entry = entry.unwrap();
		let target = to.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &target);
		} else {
			std::fs::copy(entry.path(), &target).unwrap();
		}
	}
}

/// A checkpoint is restored through the same replay as an open: the records of the last segment
/// that it holds are held to the value log that it holds. A checkpoint is made with an empty log,
/// so the log of this one is put in it, with a value log whose tail is cut.
#[test(tokio::test)]
async fn a_restore_ends_the_log_before_a_value_that_the_checkpoint_does_not_hold() {
	use super::fs_engine_tests::{commit_range, key_of, value_of};
	use crate::Durability;

	let dir = TempDir::new().unwrap();

	// A database that was left without a close, with twelve Eventual commits.
	let crashed = dir.path().join("crashed");
	let tree = crate::Tree::new(tree_options(&crashed, WalRecoveryMode::TolerateTornTail)).unwrap();
	commit_range(&tree, 0..12, 300, Durability::Eventual).await;
	tree.abandon();
	drop(tree);

	// A checkpoint of an empty database, and in it the log and the value log of the other, the
	// value log cut in the middle of the eighth entry. A restore links the files of a checkpoint
	// and a cut of the log is made in place, so every restore has a checkpoint of its own.
	let segment = std::fs::read_dir(crashed.join("wal"))
		.unwrap()
		.map(|entry| entry.unwrap().path())
		.filter(|path| std::fs::metadata(path).unwrap().len() > 0)
		.max()
		.unwrap();
	let checkpoint_for = |name: &str| {
		let empty = dir.path().join(format!("empty-{name}"));
		let tree =
			crate::Tree::new(tree_options(&empty, WalRecoveryMode::TolerateTornTail)).unwrap();
		let checkpoint = dir.path().join(format!("checkpoint-{name}"));
		tree.create_checkpoint(&checkpoint).unwrap();
		tree.abandon();
		drop(tree);
		std::fs::remove_dir_all(checkpoint.join("wal")).unwrap();
		std::fs::create_dir_all(checkpoint.join("wal")).unwrap();
		std::fs::copy(&segment, checkpoint.join("wal").join("00000000000000000099.wal")).unwrap();
		let _ = std::fs::remove_dir_all(checkpoint.join("vlog"));
		copy_dir(&crashed.join("vlog"), &checkpoint.join("vlog"));
		let file = checkpoint.join("vlog").join("00000000000000000001.vlog");
		let bytes = std::fs::read(&file).unwrap();
		std::fs::write(&file, &bytes[..31 + 7 * 330 + 100]).unwrap();
		checkpoint
	};

	for (mode, kept) in [
		(WalRecoveryMode::SalvageValidPrefix, Some(7)),
		(WalRecoveryMode::TolerateTornTail, Some(7)),
		(WalRecoveryMode::AbsoluteConsistency, None),
	] {
		let checkpoint = checkpoint_for(&format!("{mode:?}"));
		let live = dir.path().join(format!("live-{mode:?}"));
		let tree = crate::Tree::new(tree_options(&live, mode)).unwrap();
		let restored = tree.restore_from_checkpoint(&checkpoint);
		match kept {
			Some(kept) => {
				restored.unwrap_or_else(|e| panic!("{mode:?}: {e}"));
				let report = tree.recovery_report();
				assert_eq!(report.records_dropped, 12 - kept, "{mode:?}: {report:?}");
				assert_eq!(report.quarantined_files.len(), 1, "{mode:?}: {report:?}");
				let txn = tree.begin_with_mode(crate::Mode::ReadOnly).unwrap();
				for i in 0..12 {
					let got = txn.get(key_of(i)).unwrap();
					assert_eq!(got, (i < kept).then(|| value_of(i, 300)), "{mode:?}: key {i}");
				}
				drop(txn);
				commit_range(&tree, 100..105, 300, Durability::Immediate).await;
			}
			None => {
				let error = restored.expect_err("refused");
				assert!(matches!(error, crate::Error::WalCorruption { .. }), "{mode:?}: {error}");
			}
		}
		tree.abandon();
	}
}
