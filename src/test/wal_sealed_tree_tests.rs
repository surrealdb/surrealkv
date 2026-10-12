//! A tree whose WAL is sealed: that no key and no value is in any byte of it, that a torn tail
//! recovers as it does for a plaintext WAL, that a restore reads the WAL it restores with the key
//! of the live tree, and that a key that cannot be had is the error of the rotation that needed it.
//!
//! The context is put in the options directly, as the engine will when it has read the
//! descriptor of a database, so the tables and the value log of these trees are sealed as well.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use super::sstable_encryption_tests::FlakyManager;
use super::wal_sealed_tests::{
	context,
	names,
	read_records,
	sealed_context,
	sealed_options,
	segment_path,
	snapshot,
	DB,
	HEADER_RECORD_LEN,
	SUITES,
};
use crate::batch::Batch;
use crate::error::{EncryptionError, Error, KeyProviderFailure};
use crate::tde::{CipherSuite, DbId, EncryptionContext, KeyManager};
use crate::vlog::ValueLocation;
use crate::wal::manager::Wal;
use crate::wal::{segment_name, CompressionType, BLOCK_SIZE, HEADER_SIZE};
use crate::{Durability, InternalKeyKind, Mode, Options, Tree, WalRecoveryMode};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn tree_options(
	path: &Path,
	enc: Option<&Arc<EncryptionContext>>,
	mode: WalRecoveryMode,
) -> Arc<Options> {
	let opts = Options {
		path: path.to_path_buf(),
		enable_vlog: false,
		max_memtable_size: 64 * 1024 * 1024,
		flush_on_close: false,
		wal_recovery_mode: mode,
		// The background flush is stopped, so a rotation that queues a memtable must not stall the
		// commits behind it.
		memtable_stall_threshold: 1000,
		..Default::default()
	};
	Arc::new(match enc {
		Some(enc) => opts.with_encryption_context(Arc::clone(enc)),
		None => opts,
	})
}

/// A tree whose background tasks are stopped, so that no segment is flushed and removed behind the
/// test's back, and that is ended with `crash`. Shared through an `Arc`, never `Tree::clone`:
/// dropping any clone spawns a close.
async fn open_tree(
	path: &Path,
	enc: Option<&Arc<EncryptionContext>>,
	mode: WalRecoveryMode,
) -> Arc<Tree> {
	let tree = Arc::new(Tree::new(tree_options(path, enc, mode)).unwrap());
	let tasks = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tasks.stop().await;
	tree.core.is_closed.store(true, Ordering::SeqCst);
	tree
}

/// Ends a tree the way a crash does: no close, no flush.
fn crash(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}

async fn put(tree: &Tree, key: &str, value: &[u8]) {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(Durability::Immediate);
	txn.set(key.as_bytes(), value).unwrap();
	txn.commit().await.unwrap();
}

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key.as_bytes()).unwrap()
}

fn active(tree: &Tree) -> u64 {
	tree.core.inner.wal.read().get_active_log_number()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
	haystack.windows(needle.len()).any(|window| window == needle)
}

fn copy_dir(src: &Path, dst: &Path) {
	fs::create_dir_all(dst).unwrap();
	for entry in fs::read_dir(src).unwrap() {
		let entry = entry.unwrap();
		if entry.file_name() == "LOCK" {
			continue;
		}
		let to = dst.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &to);
		} else {
			fs::copy(entry.path(), &to).unwrap();
		}
	}
}

fn set_len(path: &Path, len: u64) {
	fs::OpenOptions::new().write(true).open(path).unwrap().set_len(len).unwrap();
}

fn wal_dir(path: &Path) -> PathBuf {
	path.join("wal")
}

// ---------------------------------------------------------------------------
// the canary
// ---------------------------------------------------------------------------

const KEY_MARKER: &[u8] = b"CANARY-KEY-";
const VALUE_MARKER: &[u8] = b"CANARY-VALUE-";

fn canaries() -> Vec<(String, Vec<u8>)> {
	(0..8)
		.map(|i| {
			(
				format!("CANARY-KEY-{i:02}-f3a1"),
				format!("CANARY-VALUE-{i:02}-8c2e-").repeat(5).into_bytes(),
			)
		})
		.collect()
}

/// Writes the canaries to a tree, four in one segment and four in the next, and crashes. The
/// directory of the WAL is returned.
async fn write_canaries(
	path: &Path,
	enc: Option<&Arc<EncryptionContext>>,
	entries: &[(String, Vec<u8>)],
) -> PathBuf {
	let tree = open_tree(path, enc, WalRecoveryMode::AbsoluteConsistency).await;
	for (key, value) in &entries[..4] {
		put(&tree, key, value).await;
	}
	// A rotation that queues the memtable and keeps the segment: nothing flushes it.
	tree.core.inner.rotate_memtable().unwrap();
	for (key, value) in &entries[4..] {
		put(&tree, key, value).await;
	}
	crash(&tree);
	wal_dir(path)
}

#[test(tokio::test)]
async fn no_key_and_no_value_is_in_any_byte_of_the_wal_of_a_sealed_tree() {
	let entries = canaries();
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let enc = context(suite);
		let wal = write_canaries(dir.path(), Some(&enc), &entries).await;

		let files = snapshot(&wal);
		assert!(files.len() >= 2, "{suite:?}: the writes went to two segments");
		for (name, bytes) in &files {
			assert_eq!(
				&bytes[HEADER_SIZE..HEADER_SIZE + 4],
				b"SKTD",
				"{suite:?}: {name} is sealed"
			);
			assert!(!contains(bytes, KEY_MARKER), "{suite:?}: {name} holds a key");
			assert!(!contains(bytes, VALUE_MARKER), "{suite:?}: {name} holds a value");
			for (key, value) in &entries {
				assert!(
					!contains(bytes, key.as_bytes()) && !contains(bytes, value),
					"{suite:?}: {name}"
				);
			}
		}

		// And what is not in any byte is in the tree: it recovers with the key.
		let tree = open_tree(dir.path(), Some(&enc), WalRecoveryMode::AbsoluteConsistency).await;
		for (key, value) in &entries {
			assert_eq!(get(&tree, key).as_ref(), Some(value), "{suite:?}: {key}");
		}
		crash(&tree);
	}
}

/// The control: the same writes to a tree whose WAL is plaintext leave every key and every value in
/// the files, so that finding none in a sealed one means something.
#[test(tokio::test)]
async fn the_markers_are_in_the_wal_of_a_plaintext_tree_so_that_their_absence_means_something() {
	let entries = canaries();
	let dir = TempDir::new().unwrap();
	let wal = write_canaries(dir.path(), None, &entries).await;

	let files = snapshot(&wal);
	assert!(files.len() >= 2);
	for (name, bytes) in &files {
		assert!(!contains(bytes, b"SKTD"), "{name} is not sealed");
	}
	for (key, value) in &entries {
		assert!(files.values().any(|bytes| contains(bytes, key.as_bytes())), "{key}");
		assert!(files.values().any(|bytes| contains(bytes, value)), "{key}");
	}
}

/// Compression is applied before sealing, and what is compressed is sealed as well; the control is
/// the same segment in plaintext, which still holds the first of each marker.
///
/// LZ4 keeps a string as it is only where nothing before it in the record repeats a piece of it,
/// and the value marker repeats the start of the key marker. So each canary is written twice, with
/// the key first and with the value first: the start of a record is always kept, and the control
/// finds each marker where it starts one.
#[test]
fn a_compressed_segment_hides_the_markers_as_well_and_the_control_shows_them() {
	let records: Vec<Vec<u8>> = canaries()
		.into_iter()
		.flat_map(|(key, value)| {
			[[key.clone().into_bytes(), value.clone()].concat(), [value, key.into_bytes()].concat()]
		})
		.collect();
	for suite in SUITES {
		let sealed_dir = TempDir::new().unwrap();
		let enc = context(suite);
		let mut wal =
			Wal::open(sealed_dir.path(), sealed_options(&enc, CompressionType::Lz4)).unwrap();
		for record in &records {
			wal.append(record).unwrap();
		}
		wal.close().unwrap();
		let sealed = fs::read(segment_path(sealed_dir.path(), 0)).unwrap();
		assert!(!contains(&sealed, KEY_MARKER) && !contains(&sealed, VALUE_MARKER), "{suite:?}");

		let plain_dir = TempDir::new().unwrap();
		let options = crate::wal::Options::default().with_compression(CompressionType::Lz4);
		let mut wal = Wal::open(plain_dir.path(), options).unwrap();
		for record in &records {
			wal.append(record).unwrap();
		}
		wal.close().unwrap();
		let plain = fs::read(segment_path(plain_dir.path(), 0)).unwrap();
		assert!(contains(&plain, KEY_MARKER) && contains(&plain, VALUE_MARKER), "the control");
	}
}

// ---------------------------------------------------------------------------
// a torn tail
// ---------------------------------------------------------------------------

const GOOD: [u8; 64] = [b'x'; 64];
const NEW: [u8; 64] = [b'z'; 64];

#[derive(Clone, Copy)]
enum Cut {
	/// Keep this many bytes of the torn record.
	Bytes(u64),
	/// Keep the torn record up to the end of the first block, and this many bytes of the next.
	Block(u64),
}

#[derive(Clone, Copy)]
struct Shape {
	/// The size of the value of the torn commit.
	value_len: usize,
	cut: Cut,
	/// Whether the reader takes the end as clean: complete fragments of a record whose next one is
	/// in a block that was never written.
	clean: bool,
}

fn assert_keys(tree: &Tree, prefix: &str, count: usize, value: &[u8], when: &str) {
	for i in 0..count {
		let key = format!("{prefix}{i}");
		assert_eq!(get(tree, &key).as_deref(), Some(value), "{when}: lost {key}");
	}
}

/// Ten commits that are acknowledged and one that is cut short, recovery, five more commits, a
/// crash, recovery, three more, a crash, recovery: what was acknowledged is there after each, the
/// commit that was cut short never is, and the damaged segment is repaired once and kept.
async fn tear_scenario(suite: CipherSuite, shape: Shape) {
	let dir = TempDir::new().unwrap();
	let enc = context(suite);
	let tolerate = WalRecoveryMode::TolerateCorruptedWithRepair;

	let tree = open_tree(dir.path(), Some(&enc), tolerate).await;
	let first = active(&tree);
	let segment = segment_path(&wal_dir(dir.path()), first);
	for i in 0..10 {
		put(&tree, &format!("good{i}"), &GOOD).await;
	}
	let good_len = fs::metadata(&segment).unwrap().len();
	put(&tree, "torn", &vec![b'y'; shape.value_len]).await;
	crash(&tree);
	drop(tree);

	let full_len = fs::metadata(&segment).unwrap().len();
	let cut = match shape.cut {
		Cut::Bytes(n) => n,
		Cut::Block(n) => BLOCK_SIZE as u64 - good_len + n,
	};
	assert!(good_len + cut < full_len, "cut {cut} is out of range ({good_len}, {full_len})");
	set_len(&segment, good_len + cut);
	let cut_bytes = fs::read(&segment).unwrap();
	let original_salt = cut_bytes[HEADER_SIZE + 40..HEADER_SIZE + 72].to_vec();

	// Absolute consistency, on a copy: a tail that the reader takes for damage is refused with the
	// WAL left as it was, and one that it takes for the end of the log is opened.
	let copy = TempDir::new().unwrap();
	copy_dir(dir.path(), copy.path());
	let before = snapshot(&wal_dir(copy.path()));
	let absolute = tree_options(copy.path(), Some(&enc), WalRecoveryMode::AbsoluteConsistency);
	match Tree::new(absolute) {
		Err(Error::WalCorruption {
			segment_id,
			..
		}) if !shape.clean && segment_id as u64 == first => {
			assert_eq!(snapshot(&wal_dir(copy.path())), before, "a refused open changed the WAL");
		}
		Err(error) => panic!("{suite:?}: absolute consistency, clean={}: {error}", shape.clean),
		Ok(tree) => {
			assert!(shape.clean, "{suite:?}: absolute consistency opened a tail that is damaged");
			assert_keys(&tree, "good", 10, &GOOD, "absolute consistency");
			tree.close().await.unwrap();
		}
	}

	// Recovery 1: the commit that was cut short was not acknowledged, the rest was.
	let tree = open_tree(dir.path(), Some(&enc), tolerate).await;
	assert_keys(&tree, "good", 10, &GOOD, "recovery #1");
	assert!(get(&tree, "torn").is_none(), "{suite:?}: recovery #1 exposed the torn commit");
	let second = active(&tree);
	assert!(second > first, "commits must not go to the damaged segment");
	let context = sealed_context(&enc);
	if shape.clean {
		assert_eq!(
			fs::read(&segment).unwrap(),
			cut_bytes,
			"a tail that is not damaged is not rewritten"
		);
		assert_eq!(
			names(&wal_dir(dir.path())).len(),
			2,
			"the damaged copy is kept only for a repair"
		);
	} else {
		let kept = format!("{}.corrupt", segment_name(first, "wal"));
		assert_eq!(fs::read(wal_dir(dir.path()).join(kept)).unwrap(), cut_bytes);
		let repaired = fs::read(&segment).unwrap();
		assert_ne!(repaired[HEADER_SIZE + 40..HEADER_SIZE + 72], original_salt[..]);
		assert!(!wal_dir(dir.path()).join("repair_temp").exists());
	}
	assert_eq!(read_records(&context, &segment, first).unwrap().len(), 10, "{suite:?}");
	for i in 0..5 {
		put(&tree, &format!("new{i}"), &NEW).await;
	}
	assert_eq!(
		read_records(&context, &segment_path(&wal_dir(dir.path()), second), second).unwrap().len(),
		5
	);
	crash(&tree);
	drop(tree);

	// Recovery 2: the damaged segment is no longer the last one.
	let tree = open_tree(dir.path(), Some(&enc), tolerate).await;
	assert_keys(&tree, "good", 10, &GOOD, "recovery #2");
	assert_keys(&tree, "new", 5, &NEW, "recovery #2");
	assert!(get(&tree, "torn").is_none(), "{suite:?}: recovery #2 exposed the torn commit");
	for i in 0..3 {
		put(&tree, &format!("later{i}"), &NEW).await;
	}
	crash(&tree);
	drop(tree);

	// Recovery 3, in the mode that refuses damage: the repaired WAL has none.
	let tree = open_tree(dir.path(), Some(&enc), WalRecoveryMode::AbsoluteConsistency).await;
	assert_keys(&tree, "good", 10, &GOOD, "recovery #3");
	assert_keys(&tree, "new", 5, &NEW, "recovery #3");
	assert_keys(&tree, "later", 3, &NEW, "recovery #3");
	assert!(get(&tree, "torn").is_none());
	crash(&tree);

	// Whatever was kept of the damaged segment is sealed too: no value of the torn commit is in
	// any file of the WAL, the kept copy included.
	for (name, bytes) in snapshot(&wal_dir(dir.path())) {
		assert!(!contains(&bytes, b"yyyyyyyyyyyyyyyy"), "{suite:?}: {name}");
		assert!(!contains(&bytes, &GOOD[..16]), "{suite:?}: {name}");
	}
}

#[test(tokio::test)]
async fn a_tail_cut_inside_a_header_or_a_payload_is_repaired_and_the_commits_go_on() {
	for suite in SUITES {
		for cut in [3u64, 7, 20] {
			tear_scenario(
				suite,
				Shape {
					value_len: 64,
					cut: Cut::Bytes(cut),
					clean: false,
				},
			)
			.await;
		}
	}
}

#[test(tokio::test)]
async fn a_record_that_spans_blocks_and_is_cut_in_the_first_or_a_later_block_is_repaired() {
	for suite in SUITES {
		for cut in [Cut::Bytes(1000), Cut::Block(3), Cut::Block(7)] {
			tear_scenario(
				suite,
				Shape {
					value_len: 100_000,
					cut,
					clean: false,
				},
			)
			.await;
		}
	}
}

#[test(tokio::test)]
async fn a_record_whose_first_fragment_ends_a_block_is_a_clean_end_that_is_not_repaired() {
	for suite in SUITES {
		tear_scenario(
			suite,
			Shape {
				value_len: 100_000,
				cut: Cut::Block(0),
				clean: true,
			},
		)
		.await;
	}
}

// ---------------------------------------------------------------------------
// the restore
// ---------------------------------------------------------------------------

/// A WAL record for `key`, as the commit pipeline logs it.
fn tree_record(seq: u64, key: &str, value: &[u8]) -> Vec<u8> {
	let mut batch = Batch::new(seq);
	let location = ValueLocation::with_inline_value(value.to_vec()).encode();
	batch.add_record(InternalKeyKind::Set, key.as_bytes().to_vec(), Some(location), 0).unwrap();
	batch.encode().unwrap()
}

/// A restore replays the segments of the checkpoint under the context of the tree that is
/// restored, and the segment that it continues in is a sealed one of that database.
#[test(tokio::test)]
async fn a_restore_reads_the_wal_of_the_checkpoint_with_the_key_of_the_live_tree() {
	for suite in SUITES {
		let root = TempDir::new().unwrap();
		let live = root.path().join("live");
		let checkpoint = root.path().join("checkpoint");
		let enc = context(suite);
		let live_options = tree_options(&live, Some(&enc), WalRecoveryMode::AbsoluteConsistency);
		let tree = Tree::new(Arc::clone(&live_options)).unwrap();
		put(&tree, "one", &GOOD).await;
		tree.create_checkpoint(&checkpoint).unwrap();
		// The number that the segments of the checkpoint begin at. The checkpoint flushed first and
		// the commit below only appends to the WAL, so it is the number of the live manifest now;
		// the manifest of an encrypted database is sealed and cannot be parsed from its bytes.
		let log_number = tree.core.inner.level_manifest.read().unwrap().get_log_number();
		put(&tree, "two", &GOOD).await;

		// A sealed segment in the WAL of the checkpoint, where the log number says it begins: what
		// a checkpoint that kept its WAL would hold.
		let staging = TempDir::new().unwrap();
		let mut wal = Wal::open_with_min_log_number(
			staging.path(),
			log_number,
			sealed_options(&enc, CompressionType::None),
		)
		.unwrap();
		wal.append(&tree_record(1_000_000, "injected", &NEW)).unwrap();
		wal.close().unwrap();
		fs::create_dir_all(wal_dir(&checkpoint)).unwrap();
		fs::copy(
			segment_path(staging.path(), log_number),
			segment_path(&wal_dir(&checkpoint), log_number),
		)
		.unwrap();

		tree.restore_from_checkpoint(&checkpoint).unwrap();
		assert_eq!(get(&tree, "one"), Some(GOOD.to_vec()), "{suite:?}");
		assert!(get(&tree, "two").is_none(), "{suite:?}: a commit after the checkpoint is gone");
		assert_eq!(
			get(&tree, "injected"),
			Some(NEW.to_vec()),
			"{suite:?}: the restored WAL was replayed"
		);

		// Every segment of the WAL after the restore is sealed, for this database, and the commits
		// that follow go to a segment of their own.
		let files = snapshot(&wal_dir(&live));
		assert!(files.len() >= 2, "{suite:?}");
		for (name, bytes) in &files {
			assert_eq!(&bytes[HEADER_SIZE..HEADER_SIZE + 4], b"SKTD", "{name}");
			assert_eq!(&bytes[HEADER_SIZE + 16..HEADER_SIZE + 32], &DB, "{name}: the database");
		}
		put(&tree, "three", &GOOD).await;
		tree.close().await.unwrap();
		drop(tree);

		// A restart replays the WAL that the restore left, and what was replayed by the restore is
		// where it was.
		let recovered = Tree::new(live_options).unwrap();
		for key in ["one", "injected", "three"] {
			assert!(get(&recovered, key).is_some(), "{suite:?}: {key}");
		}
		assert!(get(&recovered, "two").is_none());
		recovered.close().await.unwrap();
	}
}

// ---------------------------------------------------------------------------
// a key that cannot be had
// ---------------------------------------------------------------------------

/// The key of the segment that a rotation makes is asked for when it is made. A provider that is
/// away makes the rotation fail with an error that says so, and nothing is queued or lost: the
/// commits are where they were, and the same call works when the provider is back.
#[test(tokio::test)]
async fn a_provider_that_is_away_is_the_error_of_the_rotation_and_it_can_be_tried_again() {
	let dir = TempDir::new().unwrap();
	let manager = FlakyManager::new();
	let enc = Arc::new(EncryptionContext::new(
		Arc::clone(&manager) as Arc<dyn KeyManager>,
		DbId::from_bytes([0xD5; 16]),
		CipherSuite::Aes256Gcm,
	));
	let tree =
		Tree::new(tree_options(dir.path(), Some(&enc), WalRecoveryMode::AbsoluteConsistency))
			.unwrap();
	put(&tree, "key", &GOOD).await;
	let segments = names(&wal_dir(dir.path()));

	manager.set_unavailable(true);
	let error = tree.flush().unwrap_err();
	assert!(
		matches!(
			&error,
			Error::Encryption(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable))
		),
		"{error:?}"
	);
	assert_eq!(names(&wal_dir(dir.path())), segments, "no segment was made or removed");
	assert_eq!(get(&tree, "key"), Some(GOOD.to_vec()));

	manager.set_unavailable(false);
	tree.flush().unwrap();
	assert_eq!(get(&tree, "key"), Some(GOOD.to_vec()));
	assert_eq!(
		fs::metadata(segment_path(&wal_dir(dir.path()), active(&tree))).unwrap().len() as usize,
		HEADER_RECORD_LEN
	);
	tree.close().await.unwrap();
}
