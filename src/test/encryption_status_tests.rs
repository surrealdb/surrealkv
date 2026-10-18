//! Which keys cover which files: the status of a tree that is open, the report of a database from
//! its files alone, and what each says when the directory is not as it should be.
//!
//! The expected counts are taken from the directory with the standard library, file by file, and
//! never from the code that is tested.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;

use super::encryption_open_tests::{
	create,
	encrypted,
	files_with_extension,
	manager,
	plain,
	write,
	KEYS,
	ROOT,
	SUITES,
};
use super::sstable_encryption_tests::{many_points, options, write_table};
use super::tfs;
use crate::inspect::{encryption_report, EncryptionStatus};
use crate::tde::{
	describe_manifest_file,
	CipherSuite,
	DbId,
	KeyManager,
	KeyState,
	SoftwareKeyManager,
	HEADER_LEN,
	ROOT_KEY_LEN,
};
use crate::{CompressionType, Error, Options, Tree};

fn opts_of(root: &Path) -> Options {
	Options {
		path: root.to_path_buf(),
		..Default::default()
	}
}

/// How many files and bytes the directory holds of each kind, in the order of the manifest, the
/// tables, the value log and the write-ahead log.
fn on_disk(root: &Path) -> [(u64, u64); 4] {
	let opts = opts_of(root);
	let counted = |files: Vec<PathBuf>| {
		let bytes = files.iter().map(|file| fs::metadata(file).unwrap().len()).sum();
		(files.len() as u64, bytes)
	};
	[
		counted(vec![opts.manifest_file_path(0)]),
		counted(files_with_extension(&opts.sstable_dir(), "sst")),
		counted(files_with_extension(&opts.vlog_dir(), "vlog")),
		counted(files_with_extension(&opts.wal_dir(), "wal")),
	]
}

/// The files and bytes that `status` says of each kind under `key_id`, in the same order.
fn counted_under(status: &EncryptionStatus, key_id: u32) -> [(u64, u64); 4] {
	let usage = status.key(key_id).unwrap_or_else(|| panic!("no key {key_id} in {status:?}"));
	[usage.manifest, usage.tables, usage.value_log, usage.wal].map(|t| (t.files, t.bytes))
}

fn db_id_of(root: &Path) -> DbId {
	describe_manifest_file(opts_of(root).manifest_file_path(0)).unwrap().db_id.unwrap()
}

fn sstable(root: &Path, name: u64) -> PathBuf {
	opts_of(root).sstable_dir().join(format!("{name:020}.sst"))
}

fn first_table(root: &Path) -> PathBuf {
	files_with_extension(&opts_of(root).sstable_dir(), "sst").remove(0)
}

#[tokio::test]
async fn a_tree_says_which_key_covers_which_files() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		create(dir.path(), manager(), suite).await;
		let opts = encrypted(dir.path(), manager(), suite);
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		// What was recovered from the log is flushed now, so that nothing is left for a task of the
		// tree to do while the directory is read.
		tree.flush().unwrap();
		let expected = on_disk(dir.path());
		let status = tree.encryption_status().unwrap();

		assert!(status.encrypted, "{suite:?}");
		assert_eq!(status.suite, Some(suite));
		assert_eq!(status.db_id, Some(db_id_of(dir.path())));
		assert_eq!(status.active_key_id, Some(1));
		assert_eq!(status.manifest_key_id, Some(1));
		assert!(status.manifest_key_is_active);

		assert_eq!(status.keys.len(), 1, "{status:?}");
		assert_eq!(status.keys[0].state, Some(KeyState::Active));
		assert_eq!(counted_under(&status, 1), expected, "{suite:?}");
		assert!(expected.iter().all(|(files, bytes)| *files > 0 && *bytes > 0), "{expected:?}");
		assert!(!status.keys[0].covers_no_files());
		assert_eq!(
			status.keys[0].total().files,
			expected.iter().map(|(files, _)| files).sum::<u64>()
		);
		assert!(status.key(2).is_none());

		// Everything is where it should be.
		assert!(status.is_consistent(), "{:?}", status.warnings);
		assert_eq!((status.unattributed.files, status.plaintext.files), (0, 0));
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_key_that_was_replaced_still_covers_what_was_written_under_it() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let km = Arc::new(SoftwareKeyManager::new(ROOT));
		km.add_key(2, [0x44; ROOT_KEY_LEN]).unwrap();
		let as_manager: Arc<dyn KeyManager> = Arc::<SoftwareKeyManager>::clone(&km);
		let tree = Tree::new(encrypted(dir.path(), as_manager, suite)).unwrap();
		write(&tree, '1', 0..KEYS).await;
		tree.flush().unwrap();

		// Under key 1 only, and the key that is added and not yet active covers nothing and is
		// not listed.
		let status = tree.encryption_status().unwrap();
		assert_eq!(status.keys.len(), 1, "{suite:?}: {status:?}");
		let old_tables = counted_under(&status, 1)[1].0;
		assert!(old_tables >= 1);

		// The key is replaced: new files go under key 2, and the manifest, which is written when
		// something changes, still names key 1 until then.
		km.set_active_key_id(2).unwrap();
		tree.prepare_keys().unwrap();
		let status = tree.encryption_status().unwrap();
		assert_eq!(status.active_key_id, Some(2), "{suite:?}");
		assert_eq!(status.manifest_key_id, Some(1), "{suite:?}");
		assert!(!status.manifest_key_is_active, "{suite:?}");
		assert_eq!(status.keys.len(), 2, "{suite:?}: {status:?}");
		assert_eq!(status.key(1).unwrap().state, Some(KeyState::DecryptOnly));
		assert_eq!(status.key(2).unwrap().state, Some(KeyState::Active));
		assert!(status.key(2).unwrap().covers_no_files(), "{suite:?}: {status:?}");
		assert_eq!(status.key(1).unwrap().manifest.files, 1);

		// A flush writes a table and the manifest under the new key, and leaves the old table
		// where it was: key 1 is still needed to read it, and says so.
		write(&tree, '2', 0..KEYS / 2).await;
		tree.flush().unwrap();
		let status = tree.encryption_status().unwrap();
		assert_eq!(status.manifest_key_id, Some(2), "{suite:?}");
		assert!(status.manifest_key_is_active, "{suite:?}");
		let (one, two) = (status.key(1).unwrap(), status.key(2).unwrap());
		assert_eq!((one.manifest.files, two.manifest.files), (0, 1), "{suite:?}");
		assert_eq!(one.tables.files, old_tables, "{suite:?}");
		assert!(two.tables.files >= 1 && two.wal.files >= 1, "{suite:?}: {two:?}");
		assert!(!one.covers_no_files() && !two.covers_no_files());

		// What each key covers adds up to what is in the directory.
		let on_disk = on_disk(dir.path());
		for (kind, total) in on_disk.iter().enumerate() {
			let files: u64 =
				status.keys.iter().map(|usage| counted_under(&status, usage.key_id)[kind].0).sum();
			let bytes: u64 =
				status.keys.iter().map(|usage| counted_under(&status, usage.key_id)[kind].1).sum();
			assert_eq!((files, bytes), *total, "{suite:?}, kind {kind}");
		}
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_tree_that_is_not_encrypted_has_nothing_to_say_about_keys() {
	let dir = TempDir::new().unwrap();
	let tree = Tree::new(plain(dir.path())).unwrap();
	write(&tree, '1', 0..KEYS).await;
	let status = tree.encryption_status().unwrap();
	assert!(!status.encrypted);
	assert!(status.keys.is_empty() && status.suite.is_none() && status.db_id.is_none());
	assert!(status.active_key_id.is_none() && status.manifest_key_id.is_none());
	assert!(!status.manifest_key_is_active && status.is_consistent());
	tree.close().await.unwrap();
}

#[tokio::test]
async fn a_report_made_from_the_files_alone_says_the_same_without_a_key() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		create(dir.path(), manager(), suite).await;
		let report = encryption_report(dir.path()).unwrap();

		assert!(report.encrypted);
		assert_eq!((report.suite, report.db_id), (Some(suite), Some(db_id_of(dir.path()))));
		assert_eq!(report.manifest_key_id, Some(1));
		// Nothing was asked of a key manager, so what it would say is not said.
		assert_eq!(report.active_key_id, None);
		assert!(!report.manifest_key_is_active);
		assert_eq!(report.keys.len(), 1, "{report:?}");
		assert_eq!(report.keys[0].state, None);
		assert_eq!(counted_under(&report, 1), on_disk(dir.path()), "{suite:?}");
		assert!(report.is_consistent() && report.notes.is_empty(), "{report:?}");
	}

	// A directory that is not a database has no manifest, and says so.
	let empty = TempDir::new().unwrap();
	match encryption_report(empty.path()) {
		Err(Error::InvalidArgument(message)) => {
			assert!(message.contains("no manifest"), "{message}")
		}
		other => panic!("expected InvalidArgument, got {other:?}"),
	}
}

/// A database that was written by a tree, under XChaCha20-Poly1305.
async fn database() -> (TempDir, CipherSuite) {
	let dir = TempDir::new().unwrap();
	create(dir.path(), manager(), CipherSuite::XChaCha20Poly1305).await;
	(dir, CipherSuite::XChaCha20Poly1305)
}

fn plain_table(id: u64) -> Vec<u8> {
	write_table(&options(None, CompressionType::None, true), id, &many_points(10))
}

#[tokio::test]
async fn a_file_in_the_clear_in_an_encrypted_database_is_reported_and_counted() {
	if tfs::skip_listed() {
		return;
	}
	let (dir, _) = database().await;
	let before = on_disk(dir.path());
	fs::write(sstable(dir.path(), 99), plain_table(99)).unwrap();

	let report = encryption_report(dir.path()).unwrap();
	assert_eq!(
		report.warnings,
		["sstables/00000000000000000099.sst is in the clear in an encrypted database"]
	);
	assert_eq!(report.plaintext.files, 1);
	assert!(report.plaintext.bytes > 0 && report.unattributed.files == 0);
	assert!(!report.is_consistent());
	// It is not under any key: what is counted under key 1 is what was there.
	assert_eq!(counted_under(&report, 1), before);
}

#[tokio::test]
async fn a_file_of_another_database_and_suite_and_a_renamed_file_are_reported() {
	if tfs::skip_listed() {
		return;
	}
	let (dir, _) = database().await;
	let before = on_disk(dir.path());

	// A table of another database, sealed with another suite, under a number of its own.
	let other = TempDir::new().unwrap();
	create(other.path(), manager(), CipherSuite::Aes256Gcm).await;
	let foreign = fs::read(first_table(other.path())).unwrap();
	let foreign_id: u64 =
		first_table(other.path()).file_name().unwrap().to_str().unwrap()[..20].parse().unwrap();
	fs::write(sstable(dir.path(), 98), &foreign).unwrap();

	// And a table of this database under the number of another one.
	let own = first_table(dir.path());
	let own_id: u64 = own.file_name().unwrap().to_str().unwrap()[..20].parse().unwrap();
	fs::copy(&own, sstable(dir.path(), own_id + 500)).unwrap();

	let report = encryption_report(dir.path()).unwrap();
	let label = |id: u64| format!("sstables/{id:020}.sst");
	let mut expected = vec![
		format!("{} belongs to another database, {}", label(98), db_id_of(other.path())),
		format!("{} is sealed with Aes256Gcm, and the database with XChaCha20Poly1305", label(98)),
		format!("{} is named for 98, and its header says that it is {foreign_id}", label(98)),
		format!(
			"{} is named for {}, and its header says that it is {own_id}",
			label(own_id + 500),
			own_id + 500
		),
	];
	expected.sort();
	let mut found = report.warnings.clone();
	found.sort();
	assert_eq!(found, expected);
	assert!(!report.is_consistent());

	// Both are sealed under key 1, which is what their headers name, so they are counted there: it
	// is key 1 that is needed to read them.
	let under_key_1 = counted_under(&report, 1);
	assert_eq!(under_key_1[1].0, before[1].0 + 2);
	assert_eq!(
		under_key_1[1].1,
		before[1].1 + foreign.len() as u64 + fs::metadata(&own).unwrap().len()
	);
}

/// The engine opens a table by its path and lists the segments of the log through `metadata`, so a
/// file that is a link is live, and a key that covers it is needed. A report that skipped links
/// would say that the key covers nothing.
#[cfg(unix)]
#[tokio::test]
async fn a_file_that_is_a_link_is_counted_as_the_file_that_it_leads_to() {
	if tfs::skip_listed() {
		return;
	}
	let (dir, _) = database().await;
	let before = on_disk(dir.path());
	let table = first_table(dir.path());
	let size = fs::metadata(&table).unwrap().len();

	// The table is moved out of the directory and a link to it takes its place, under its name.
	let elsewhere = TempDir::new().unwrap();
	let moved = elsewhere.path().join(table.file_name().unwrap());
	fs::rename(&table, &moved).unwrap();
	std::os::unix::fs::symlink(&moved, &table).unwrap();

	let report = encryption_report(dir.path()).unwrap();
	assert!(report.is_consistent(), "{:?}", report.warnings);
	assert_eq!(counted_under(&report, 1), before);
	assert_eq!(counted_under(&report, 1), on_disk(dir.path()));

	// A link that leads nowhere is no file, as it is not one for the engine.
	fs::remove_file(&moved).unwrap();
	let report = encryption_report(dir.path()).unwrap();
	assert!(report.is_consistent(), "{:?}", report.warnings);
	let under_key_1 = counted_under(&report, 1);
	assert_eq!(under_key_1[1], (before[1].0 - 1, before[1].1 - size));
	assert_eq!((under_key_1[0], under_key_1[2], under_key_1[3]), (before[0], before[2], before[3]));
}

#[tokio::test]
async fn a_header_that_cannot_be_read_is_reported_and_belongs_to_no_key() {
	if tfs::skip_listed() {
		return;
	}
	let (dir, _) = database().await;
	let before = on_disk(dir.path());
	let mut table = fs::read(first_table(dir.path())).unwrap();
	// A byte of the salt, which the checksum of the header covers.
	table[40] ^= 1;
	assert!(table.len() > HEADER_LEN);
	fs::write(sstable(dir.path(), 97), table).unwrap();

	let report = encryption_report(dir.path()).unwrap();
	assert_eq!(
		report.warnings,
		["sstables/00000000000000000097.sst: its encryption header cannot be read: malformed encryption header"]
	);
	assert_eq!(report.unattributed.files, 1);
	assert_eq!(counted_under(&report, 1), before);
	assert!(!report.is_consistent());
}

#[tokio::test]
async fn what_a_healthy_database_leaves_is_noted_and_does_not_make_it_inconsistent() {
	if tfs::skip_listed() {
		return;
	}
	let (dir, _) = database().await;
	let opts = opts_of(dir.path());
	let before = on_disk(dir.path());

	// What a flush or a segment that was not finished leaves: files with nothing in them.
	fs::write(sstable(dir.path(), 96), b"").unwrap();
	fs::write(opts.wal_dir().join(format!("{:020}.wal", 95)), b"").unwrap();
	fs::write(opts.vlog_dir().join(format!("{:020}.vlog", 94)), b"").unwrap();
	// What a repair keeps of a segment, and a log file that was set aside, which are copies.
	let segment = files_with_extension(&opts.wal_dir(), "wal").remove(0);
	fs::copy(&segment, opts.wal_dir().join(format!("{:020}.wal.corrupt", 1))).unwrap();
	let log = files_with_extension(&opts.vlog_dir(), "vlog").remove(0);
	fs::copy(&log, opts.vlog_dir().join(format!("{:020}.vlog.torn", 1))).unwrap();
	// The place a repair works in, and something that was put there by hand.
	fs::create_dir_all(opts.wal_dir().join("repair_temp")).unwrap();
	fs::write(opts.sstable_dir().join("notes.txt"), b"a note that someone left here").unwrap();

	let report = encryption_report(dir.path()).unwrap();
	assert!(report.is_consistent(), "{:?}", report.warnings);
	assert_eq!(report.unattributed.files, 3);
	assert_eq!(report.plaintext.files, 0);
	// Nothing of them is counted under a key.
	assert_eq!(counted_under(&report, 1), before);
	assert_eq!(report.notes.len(), 6, "{:#?}", report.notes);
	assert_eq!(report.notes.iter().filter(|note| note.contains("kept aside")).count(), 2);
	assert_eq!(report.notes.iter().filter(|note| note.contains("empty or ends inside")).count(), 3);
	assert!(report.notes.iter().any(|note| note.contains("notes.txt")), "{:#?}", report.notes);
}

#[tokio::test]
async fn a_sealed_file_in_a_database_that_is_not_encrypted_is_reported() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let tree = Tree::new(plain(dir.path())).unwrap();
	write(&tree, '1', 0..KEYS).await;
	tree.flush().unwrap();
	tree.close().await.unwrap();

	let sealed = TempDir::new().unwrap();
	create(sealed.path(), manager(), CipherSuite::Aes256Gcm).await;
	fs::copy(first_table(sealed.path()), sstable(dir.path(), 99)).unwrap();

	let report = encryption_report(dir.path()).unwrap();
	assert!(!report.encrypted);
	assert!(
		report
			.warnings
			.iter()
			.any(|warning| warning.contains("is sealed, in a database that is not encrypted")),
		"{:?}",
		report.warnings
	);
	assert!(!report.is_consistent());
	// The empty segments that a database in the clear leaves are no concern of keys, and are not
	// said to be.
	assert!(report.notes.is_empty(), "{:#?}", report.notes);
	assert_eq!(report.unattributed.files, 0);
}
