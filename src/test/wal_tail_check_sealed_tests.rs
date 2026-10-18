//! The check of the last segment of an encrypted log against the value log.
//!
//! What a sealed entry can be held to without the key is its length and the checksum of what is
//! stored, which the check does for it. The records of the segment are authenticated when they are
//! read, so the modes act as they do for a plaintext log: the default mode ends the segment before
//! the first record whose value is missing if no value after it is there, and refuses a hole,
//! `SalvageValidPrefix` ends it before the record whatever follows, and `AbsoluteConsistency` fails
//! the open. A segment that is cut is written again under a new salt, and the one that it was is
//! kept.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use super::fs_engine_tests::{commit_range, key_of, value_of, within};
use super::wal_sealed_tests::{context, sealed_context, snapshot as snapshot_of, SUITES};
use crate::batch::Batch;
use crate::tde::{CipherSuite, EncryptionContext};
use crate::wal::reader::Reader;
use crate::{Durability, Error, Mode, Options, Tree, WalRecoveryMode};

const COMMITS: usize = 12;
const VALUE: usize = 300;

fn options(dir: &Path, enc: &Arc<EncryptionContext>, mode: WalRecoveryMode) -> Arc<Options> {
	let mut opts = Options {
		path: dir.to_path_buf(),
		enable_vlog: true,
		vlog_value_threshold: 64,
		vlog_checksum_verification: crate::VLogChecksumLevel::Full,
		max_memtable_size: 1 << 20,
		wal_recovery_mode: mode,
		..Options::default()
	}
	.with_encryption_context(Arc::clone(enc));
	// A sealed log is on the disk, whatever file system the run of the tests uses by default.
	opts.fs = crate::vfs::default_fs();
	Arc::new(opts)
}

/// A sealed database with `COMMITS` Eventual commits that was left without a close, in a directory
/// of its own.
async fn crashed(enc: &Arc<EncryptionContext>) -> (TempDir, PathBuf) {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("db");
	let tree = Tree::new(options(&path, enc, WalRecoveryMode::TolerateTornTail)).unwrap();
	commit_range(&tree, 0..COMMITS, VALUE, Durability::Eventual).await;
	tree.abandon();
	drop(tree);
	(dir, path)
}

/// Every file under `dir` with its bytes, but the lock of the database, which an open takes.
fn snapshot(dir: &Path) -> std::collections::BTreeMap<String, Vec<u8>> {
	let mut files = snapshot_of(dir);
	files.retain(|name, _| !name.ends_with("LOCK"));
	files
}

fn copy_dir(from: &Path, to: &Path) {
	std::fs::create_dir_all(to).unwrap();
	for entry in std::fs::read_dir(from).unwrap() {
		let entry = entry.unwrap();
		if entry.file_name() == "LOCK" {
			continue;
		}
		let target = to.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &target);
		} else {
			std::fs::copy(entry.path(), &target).unwrap();
		}
	}
}

/// The segment of the log that holds the commits, and each of its records: the batch and where it
/// ends.
fn records(enc: &Arc<EncryptionContext>, path: &Path) -> (u64, PathBuf, Vec<(Batch, usize)>) {
	let segment = std::fs::read_dir(path.join("wal"))
		.unwrap()
		.map(|entry| entry.unwrap().path())
		.filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
		.max_by_key(|path| (std::fs::metadata(path).unwrap().len() > 0, path.clone()))
		.unwrap();
	let id: u64 = segment.file_stem().unwrap().to_str().unwrap().parse().unwrap();
	let context = sealed_context(enc);
	let mut reader =
		Reader::with_context(std::fs::File::open(&segment).unwrap(), None, id, &context);
	let mut records = Vec::new();
	while let Ok((data, end)) = reader.read() {
		records.push((Batch::decode(data).unwrap(), end as usize));
	}
	(id, segment, records)
}

fn vlog_file(path: &Path) -> PathBuf {
	path.join("vlog").join("00000000000000000001.vlog")
}

fn keys(tree: &Tree, kept: usize, what: &str) {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	for i in 0..COMMITS {
		let got = txn.get(key_of(i)).unwrap_or_else(|e| panic!("{what}: key {i}: {e}"));
		if i < kept {
			assert_eq!(got, Some(value_of(i, VALUE)), "{what}: key {i}");
		} else {
			assert_eq!(got, None, "{what}: key {i}");
		}
	}
}

/// What the three modes do with an encrypted log whose value log is damaged from `first_failed`:
/// `lost_its_tail` is whether no value after that record is in the value log.
async fn the_modes(
	suite: CipherSuite,
	damage: fn(&Path, &[(Batch, usize)]),
	first_failed: usize,
	lost_its_tail: bool,
) {
	let what = format!("{suite:?}, from record {first_failed}");
	let enc = context(suite);
	let (_dir, path) = crashed(&enc).await;
	let (segment_id, segment, records) = records(&enc, &path);
	assert_eq!(records.len(), COMMITS, "{what}");
	let original = std::fs::read(&segment).unwrap();
	damage(&path, &records);
	let cut_at = if first_failed == 0 {
		0
	} else {
		records[first_failed - 1].1
	};

	// Absolute consistency: the error names the segment and where the record begins, and nothing
	// is touched.
	let copy = TempDir::new().unwrap();
	copy_dir(&path, &copy.path().join("db"));
	let before = snapshot(copy.path());
	let error =
		Tree::new(options(&copy.path().join("db"), &enc, WalRecoveryMode::AbsoluteConsistency))
			.err()
			.unwrap_or_else(|| panic!("{what}: the open succeeded"));
	assert!(
		matches!(
			&error,
			Error::WalCorruption { segment_id: found, offset, .. }
				if *found as u64 == segment_id && *offset == cut_at
		),
		"{what}: {error}"
	);
	assert_eq!(snapshot(copy.path()), before, "{what}: absolute consistency changed a file");

	for mode in [WalRecoveryMode::TolerateTornTail, WalRecoveryMode::SalvageValidPrefix] {
		let what = format!("{what}, {mode:?}");
		let copy = TempDir::new().unwrap();
		let db = copy.path().join("db");
		copy_dir(&path, &db);
		let before = snapshot(copy.path());

		// The default mode ends the segment where the value log lost its tail, and refuses a hole.
		if mode == WalRecoveryMode::TolerateTornTail && !lost_its_tail {
			let error = Tree::new(options(&db, &enc, mode))
				.err()
				.unwrap_or_else(|| panic!("{what}: the open succeeded"));
			let text = error.to_string();
			assert!(matches!(error, Error::Other(_)), "{what}: {text}");
			assert!(text.contains("SalvageValidPrefix"), "{what}: {text}");
			assert!(text.contains(&format!("{segment_id:020}.wal")), "{what}: {text}");
			assert_eq!(snapshot(copy.path()), before, "{what}: the open changed a file");
			continue;
		}

		// The segment ends before the record, the segment as it was is kept, and the database
		// goes on.
		let tree = Tree::new(options(&db, &enc, mode)).unwrap_or_else(|e| panic!("{what}: {e}"));
		let report = tree.recovery_report();
		assert_eq!(report.records_dropped, COMMITS - first_failed, "{what}: {report:?}");
		assert_eq!(report.records_replayed, 0, "sealed segments are not counted: {report:?}");
		let kept: Vec<&PathBuf> = report.quarantined_files.iter().collect();
		assert_eq!(kept.len(), 1, "{what}: {report:?}");
		assert_eq!(std::fs::read(kept[0]).unwrap(), original, "{what}: the segment as it was");
		assert!(
			(report.bytes_trimmed as usize) <= original.len() - cut_at.min(original.len()),
			"{what}: {report:?}"
		);
		keys(&tree, first_failed, &what);
		commit_range(&tree, 100..110, VALUE, Durability::Immediate).await;
		tree.close().await.unwrap();
		drop(tree);

		let tree = Tree::new(options(&db, &enc, WalRecoveryMode::AbsoluteConsistency))
			.unwrap_or_else(|e| panic!("{what}: {e}"));
		keys(&tree, first_failed, &what);
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for i in 100..110 {
			assert_eq!(
				txn.get(key_of(i)).unwrap(),
				Some(value_of(i, VALUE)),
				"{what}: new key {i}"
			);
		}
		drop(txn);
		tree.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn an_encrypted_log_whose_value_log_lost_its_tail_ends_before_the_first_missing_value() {
	within("sealed, cut value log", async {
		for suite in SUITES {
			the_modes(
				suite,
				|path, records| {
					let pointer = records[8].0.value_pointers().next().unwrap();
					let file = vlog_file(path);
					let bytes = std::fs::read(&file).unwrap();
					std::fs::write(&file, &bytes[..pointer.offset as usize + 10]).unwrap();
				},
				8,
				true,
			)
			.await;
		}
	})
	.await;
}

#[test(tokio::test)]
async fn an_encrypted_log_whose_value_log_has_a_hole_is_refused_by_the_default_mode_and_cut_by_salvage(
) {
	within("sealed, hole", async {
		the_modes(
			CipherSuite::XChaCha20Poly1305,
			|path, records| {
				let pointer = records[3].0.value_pointers().next().unwrap();
				let file = vlog_file(path);
				let mut bytes = std::fs::read(&file).unwrap();
				let middle = pointer.offset as usize + pointer.total_entry_size() as usize / 2;
				bytes[middle..middle + 20].fill(0);
				std::fs::write(&file, &bytes).unwrap();
			},
			3,
			false,
		)
		.await;
	})
	.await;
}

/// A log that is whole is opened as it was, and what it points to is made durable.
#[test(tokio::test)]
async fn an_encrypted_log_whose_values_are_all_there_is_opened_as_it_was() {
	within("sealed, whole", async {
		let enc = context(CipherSuite::Aes256Gcm);
		let (_dir, path) = crashed(&enc).await;
		for mode in [
			WalRecoveryMode::AbsoluteConsistency,
			WalRecoveryMode::TolerateTornTail,
			WalRecoveryMode::SalvageValidPrefix,
		] {
			let copy = TempDir::new().unwrap();
			copy_dir(&path, &copy.path().join("db"));
			let tree = Tree::new(options(&copy.path().join("db"), &enc, mode))
				.unwrap_or_else(|e| panic!("{mode:?}: {e}"));
			let report = tree.recovery_report();
			assert_eq!(
				(report.records_dropped, report.bytes_trimmed, report.quarantined_files.len()),
				(0, 0, 0),
				"{mode:?}"
			);
			keys(&tree, COMMITS, &format!("{mode:?}"));
			tree.close().await.unwrap();
		}
	})
	.await;
}
