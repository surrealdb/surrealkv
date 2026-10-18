//! What the background tasks of an encrypted database do when a key problem comes up: a key
//! provider that is away is waited for, and a key that does not fit stops writes and touches no
//! file.
//!
//! The flush of a memtable and the compaction of tables make files and write the manifest, which
//! needs a key for each. A provider that cannot be reached for a while must cost the database no
//! more than the work that waits for it, and must not stop it for good. A key that is refused for
//! good, or bytes that do not authenticate, are another matter: trying again does not change
//! them, so the task does not, writes stop with the error as it is, and no file is repaired,
//! moved or removed because of it.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;

use super::encryption_open_tests::{
	files_with_extension,
	read as read_all,
	rounds,
	snapshot,
	write,
	KEYS,
	SUITES,
};
use super::sstable_encryption_tests::{first_of, flip, walk};
use super::tfs;
use crate::error::{EncryptionError, KeyProviderFailure};
use crate::sstable::block_io::{BlockKind, TrailerLayout};
use crate::tde::{
	CipherSuite,
	CryptoHeader,
	KeyManager,
	KeyState,
	SoftwareKeyManager,
	Zeroizing,
	ROOT_KEY_LEN,
};
use crate::{BackgroundErrorReason, Error, ErrorSeverity, Options, Tree};

/// The key id of the databases here, one that nothing else in an error or a path would show.
const KEY_ID: u32 = 4242;

const ROOT_OF_KEY: [u8; ROOT_KEY_LEN] = [0x5C; ROOT_KEY_LEN];

/// How a `SwitchManager` answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
	/// It gives every key it has.
	Up,
	/// The provider cannot be reached: a key is `KeyProvider(Unavailable)`.
	Away,
	/// The provider answers that it will not give the key: `KeyProvider(Failed)`.
	Refuses,
	/// The key that new files are written under is retired: `KeyRetired`.
	ActiveRetired,
}

impl Mode {
	fn code(self) -> u8 {
		self as u8
	}

	fn from_code(code: u8) -> Self {
		match code {
			0 => Mode::Up,
			1 => Mode::Away,
			2 => Mode::Refuses,
			_ => Mode::ActiveRetired,
		}
	}
}

/// A key manager whose answers are switched from outside while a database is open, and that counts
/// the questions it refused.
pub(crate) struct SwitchManager {
	inner: SoftwareKeyManager,
	mode: AtomicU8,
	refused: AtomicUsize,
}

impl SwitchManager {
	pub(crate) fn new() -> Arc<Self> {
		let inner = SoftwareKeyManager::new([0x11; ROOT_KEY_LEN]);
		inner.add_key(KEY_ID, ROOT_OF_KEY).unwrap();
		inner.set_active_key_id(KEY_ID).unwrap();
		Arc::new(Self {
			inner,
			mode: AtomicU8::new(Mode::Up.code()),
			refused: AtomicUsize::new(0),
		})
	}

	pub(crate) fn set(&self, mode: Mode) {
		self.mode.store(mode.code(), Ordering::SeqCst);
	}

	fn mode(&self) -> Mode {
		Mode::from_code(self.mode.load(Ordering::SeqCst))
	}

	/// How many questions it has refused: keys, preparations and the state of the key that is
	/// retired.
	pub(crate) fn refused(&self) -> usize {
		self.refused.load(Ordering::SeqCst)
	}

	fn refusal(&self) -> Option<EncryptionError> {
		let error = match self.mode() {
			Mode::Away => EncryptionError::KeyProvider(KeyProviderFailure::Unavailable),
			Mode::Refuses => EncryptionError::KeyProvider(KeyProviderFailure::Failed),
			Mode::Up | Mode::ActiveRetired => return None,
		};
		self.refused.fetch_add(1, Ordering::SeqCst);
		Some(error)
	}
}

impl KeyManager for SwitchManager {
	fn active_key_id(&self) -> u32 {
		self.inner.active_key_id()
	}

	fn key_state(&self, key_id: u32) -> KeyState {
		if self.mode() == Mode::ActiveRetired && key_id == KEY_ID {
			self.refused.fetch_add(1, Ordering::SeqCst);
			return KeyState::Retired;
		}
		self.inner.key_state(key_id)
	}

	fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
		match self.refusal() {
			Some(error) => Err(error),
			None => self.inner.root_key(key_id),
		}
	}

	fn prepare(&self, key_ids: &[u32]) -> Result<(), EncryptionError> {
		match self.refusal() {
			Some(error) => Err(error),
			None => self.inner.prepare(key_ids),
		}
	}
}

/// Options of a database in `dir` that keeps its values in the tables and compacts when
/// `level0_max_files` tables of level zero are there.
fn options(
	dir: &Path,
	key_manager: &Arc<SwitchManager>,
	suite: CipherSuite,
	level0_max_files: usize,
) -> Arc<Options> {
	let opts = Options {
		path: dir.to_path_buf(),
		flush_on_close: false,
		memtable_stall_threshold: 1000,
		l0_stall_threshold: 1000,
		level0_max_files,
		..Default::default()
	};
	Arc::new(opts.with_encryption(Arc::clone(key_manager) as Arc<dyn KeyManager>, suite))
}

/// Waits for `condition`, which a background task makes true.
async fn until(what: &str, condition: impl Fn() -> bool) {
	let waited = tokio::time::timeout(Duration::from_secs(120), async {
		while !condition() {
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
	})
	.await;
	assert!(waited.is_ok(), "timed out waiting for {what}");
}

fn wake_flush(tree: &Tree) {
	tree.core.task_manager.lock().unwrap().as_ref().unwrap().wake_up_memtable();
}

fn wake_level(tree: &Tree) {
	tree.core.task_manager.lock().unwrap().as_ref().unwrap().wake_up_level();
}

fn stopped(tree: &Tree) -> bool {
	tree.core.inner.error_handler.is_db_stopped()
}

fn no_error_on_record(tree: &Tree) -> bool {
	tree.core.inner.error_handler.get_error().is_none()
}

/// The level-1 tables of the tree.
fn tables_below_level_zero(tree: &Tree) -> usize {
	let manifest = tree.core.inner.level_manifest.read().unwrap();
	manifest.levels.get_levels().iter().skip(1).map(|level| level.tables.len()).sum()
}

/// What a commit that is refused says, which is the error that stopped the database.
async fn commit_error(tree: &Tree) -> Error {
	let mut txn = tree.begin().unwrap();
	txn.set(b"refused".as_slice(), b"value".as_slice()).unwrap();
	txn.commit().await.expect_err("the database is stopped")
}

fn expected_after_two_rounds() -> BTreeMap<usize, char> {
	let mut expected = rounds('1', 0..KEYS);
	expected.extend(rounds('2', 0..KEYS / 2));
	expected
}

/// The files that a failed attempt added to `before`, and a check that none of the others
/// changed: a file that was there is there, with the bytes it had.
fn added_files(
	before: &BTreeMap<PathBuf, Option<Vec<u8>>>,
	after: &BTreeMap<PathBuf, Option<Vec<u8>>>,
	what: &str,
) -> Vec<(PathBuf, Option<Vec<u8>>)> {
	for (path, bytes) in before {
		assert_eq!(after.get(path), Some(bytes), "{what}: {} changed or went", path.display());
	}
	after
		.iter()
		.filter(|(path, _)| !before.contains_key(*path))
		.map(|(path, bytes)| (path.clone(), bytes.clone()))
		.collect()
}

#[tokio::test]
async fn a_flush_that_waits_for_the_key_provider_runs_again_until_the_data_is_in_a_table() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let km = SwitchManager::new();
		let opts = options(dir.path(), &km, suite, 100);
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		write(&tree, '1', 0..KEYS).await;
		// The memtable is queued while the provider is there, and is flushed while it is not.
		tree.core.inner.rotate_memtable().unwrap();
		assert_eq!(tree.core.inner.immutable_count(), 1, "{suite:?}");

		km.set(Mode::Away);
		let asked = km.refused();
		wake_flush(&tree);
		// Every attempt asks for a key once and, before the next, for the keys again: two attempts
		// are four refusals.
		until("two flushes that the provider refused", || km.refused() >= asked + 4).await;

		// Nothing is in a table or the manifest, the data is still queued, the failure is on record
		// as one that stops nothing, and the database takes commits.
		assert_eq!(tree.core.inner.l0_file_count(), 0, "{suite:?}");
		assert_eq!(tree.core.inner.immutable_count(), 1, "{suite:?}");
		let recorded = tree.core.inner.error_handler.get_error().expect("the failure is recorded");
		assert_eq!(recorded.severity, ErrorSeverity::SoftError, "{suite:?}");
		assert_eq!(recorded.reason, BackgroundErrorReason::MemtablaFlush, "{suite:?}");
		assert!(
			matches!(
				recorded.error,
				Error::Encryption(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable))
			),
			"{suite:?}: {:?}",
			recorded.error
		);
		assert!(!stopped(&tree), "{suite:?}");
		write(&tree, '2', 0..KEYS / 2).await;

		// The provider is back: the same flush is run again, and the data is in a table.
		km.set(Mode::Up);
		until("the table", || {
			tree.core.inner.l0_file_count() == 1 && tree.core.inner.immutable_count() == 0
		})
		.await;
		until("the failure to end", || no_error_on_record(&tree)).await;
		assert!(!stopped(&tree), "{suite:?}");
		read_all(&tree, &expected_after_two_rounds());

		// What was written is sealed, and the database opens again with what it holds.
		let tables = files_with_extension(&opts.sstable_dir(), "sst");
		assert_eq!(tables.len(), 1, "{suite:?}");
		assert!(CryptoHeader::has_magic(&fs::read(&tables[0]).unwrap()), "{suite:?}");
		tree.close().await.unwrap();
		let tree = Tree::new(opts).unwrap();
		read_all(&tree, &expected_after_two_rounds());
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_compaction_that_waits_for_the_key_provider_runs_again_and_the_database_goes_on() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let km = SwitchManager::new();
		// Two tables of level zero start a compaction, which the test asks for when it is ready:
		// the flushes below are made on this thread and do not wake the task that compacts.
		let opts = options(dir.path(), &km, suite, 2);
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		write(&tree, '1', 0..KEYS).await;
		tree.flush().unwrap();
		write(&tree, '2', 0..KEYS / 2).await;
		tree.flush().unwrap();
		assert_eq!(tree.core.inner.l0_file_count(), 2, "{suite:?}");
		let manifest = fs::read(opts.manifest_file_path(0)).unwrap();

		km.set(Mode::Away);
		let asked = km.refused();
		wake_level(&tree);
		until("two compactions that the provider refused", || km.refused() >= asked + 4).await;

		// The tables are as they were, so is the manifest, and the failure is on record as one that
		// stops nothing: a commit goes through while the compaction waits.
		assert_eq!(tree.core.inner.l0_file_count(), 2, "{suite:?}");
		assert_eq!(tables_below_level_zero(&tree), 0, "{suite:?}");
		assert_eq!(fs::read(opts.manifest_file_path(0)).unwrap(), manifest, "{suite:?}");
		let recorded = tree.core.inner.error_handler.get_error().expect("the failure is recorded");
		assert_eq!(recorded.severity, ErrorSeverity::SoftError, "{suite:?}");
		assert_eq!(recorded.reason, BackgroundErrorReason::Compaction, "{suite:?}");
		assert!(!stopped(&tree), "{suite:?}");
		write(&tree, '3', KEYS..KEYS + 4).await;

		// The provider is back: the compaction is run again and merges the tables.
		km.set(Mode::Up);
		until("the compaction", || {
			tree.core.inner.l0_file_count() == 0 && tables_below_level_zero(&tree) >= 1
		})
		.await;
		until("the failure to end", || no_error_on_record(&tree)).await;
		assert!(!stopped(&tree), "{suite:?}");
		let mut expected = expected_after_two_rounds();
		expected.extend(rounds('3', KEYS..KEYS + 4));
		read_all(&tree, &expected);

		tree.close().await.unwrap();
		let tree = Tree::new(opts).unwrap();
		read_all(&tree, &expected);
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_key_error_that_trying_again_cannot_cure_stops_writes_at_a_flush_and_changes_no_file() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		for (mode, expected) in [
			(Mode::Refuses, EncryptionError::KeyProvider(KeyProviderFailure::Failed)),
			(Mode::ActiveRetired, EncryptionError::KeyRetired),
		] {
			let what = format!("{suite:?} {mode:?}");
			let dir = TempDir::new().unwrap();
			let km = SwitchManager::new();
			let opts = options(dir.path(), &km, suite, 100);
			let tree = Tree::new(Arc::clone(&opts)).unwrap();
			write(&tree, '1', 0..KEYS).await;
			tree.core.inner.rotate_memtable().unwrap();
			let before = snapshot(dir.path());

			km.set(mode);
			wake_flush(&tree);
			until("the database to stop", || stopped(&tree)).await;

			// The attempt was made once, and is not made again: the key is the same the next time.
			let asked = km.refused();
			tokio::time::sleep(Duration::from_millis(500)).await;
			assert_eq!(km.refused(), asked, "{what}: the flush was tried again");

			// It is on record as a hard error of the flush, with the error as it was, and a
			// commit is refused with that error.
			let recorded = tree.core.inner.error_handler.get_error().unwrap();
			assert_eq!(recorded.severity, ErrorSeverity::HardError, "{what}");
			assert_eq!(recorded.reason, BackgroundErrorReason::MemtablaFlush, "{what}");
			match (&recorded.error, commit_error(&tree).await) {
				(Error::Encryption(on_record), Error::Encryption(told)) => {
					assert_eq!(*on_record, expected, "{what}");
					assert_eq!(told, expected, "{what}");
				}
				other => panic!("{what}: {other:?}"),
			}

			// What the error says names no key, no key id and no path.
			let (shown, debugged) = (recorded.error.to_string(), format!("{:?}", recorded.error));
			for text in [&shown, &debugged] {
				assert!(!text.contains(&KEY_ID.to_string()), "{what}: {text}");
				assert!(!text.contains(&dir.path().display().to_string()), "{what}: {text}");
				for bytes in [ROOT_OF_KEY[..4].to_vec(), vec![0x11; 4]] {
					let decimal = format!("{}, {}", bytes[0], bytes[1]);
					assert!(!text.contains(&decimal), "{what}: {text}");
				}
			}

			// No file that was there changed or went, and the one thing that came is the file of
			// the table that could not be written, which holds nothing.
			let added = added_files(&before, &snapshot(dir.path()), &what);
			assert!(
				added.iter().all(|(path, bytes)| {
					path.extension().is_some_and(|ext| ext == "sst")
						&& bytes.as_ref().is_some_and(|bytes| bytes.is_empty())
				}),
				"{what}: {added:?}"
			);
			// The data is where it was.
			read_all(&tree, &rounds('1', 0..KEYS));

			// The key is fixed and a flush that is asked for works: that ends the stop, as it ends
			// any hard error, and the table that the flush makes replaces the empty file.
			km.set(Mode::Up);
			wake_flush(&tree);
			until("the table", || tree.core.inner.l0_file_count() == 1).await;
			until("the stop to end", || !stopped(&tree)).await;
			write(&tree, '2', 0..KEYS / 2).await;
			read_all(&tree, &expected_after_two_rounds());
			tree.close().await.unwrap();
		}
	}
}

#[tokio::test]
async fn a_key_error_that_trying_again_cannot_cure_stops_writes_at_a_compaction_and_changes_no_file(
) {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let km = SwitchManager::new();
		let opts = options(dir.path(), &km, suite, 2);
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		write(&tree, '1', 0..KEYS).await;
		tree.flush().unwrap();
		write(&tree, '2', 0..KEYS / 2).await;
		tree.flush().unwrap();
		let before = snapshot(dir.path());

		km.set(Mode::Refuses);
		wake_level(&tree);
		until("the database to stop", || stopped(&tree)).await;
		let asked = km.refused();
		tokio::time::sleep(Duration::from_millis(500)).await;
		assert_eq!(km.refused(), asked, "{suite:?}: the compaction was tried again");

		let recorded = tree.core.inner.error_handler.get_error().unwrap();
		assert_eq!(recorded.severity, ErrorSeverity::HardError, "{suite:?}");
		assert_eq!(recorded.reason, BackgroundErrorReason::Compaction, "{suite:?}");
		assert!(
			matches!(
				commit_error(&tree).await,
				Error::Encryption(EncryptionError::KeyProvider(KeyProviderFailure::Failed))
			),
			"{suite:?}"
		);

		// The tables, the manifest, the log and the value log are as they were, and what a
		// compaction that could not finish left is an empty file that no manifest lists.
		let added = added_files(&before, &snapshot(dir.path()), &format!("{suite:?}"));
		assert!(
			added.iter().all(|(path, bytes)| {
				path.extension().is_some_and(|ext| ext == "sst")
					&& bytes.as_ref().is_some_and(|bytes| bytes.is_empty())
			}),
			"{suite:?}: {added:?}"
		);
		assert_eq!(tree.core.inner.l0_file_count(), 2, "{suite:?}");
		read_all(&tree, &expected_after_two_rounds());
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_table_that_fails_authentication_in_a_compaction_is_reported_as_such_and_is_left_alone() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let km = SwitchManager::new();
		let creating = options(dir.path(), &km, suite, 100);
		let tree = Tree::new(Arc::clone(&creating)).unwrap();
		write(&tree, '1', 0..KEYS).await;
		tree.flush().unwrap();
		write(&tree, '2', 0..KEYS / 2).await;
		tree.flush().unwrap();
		// The options as the tree runs with them, which carry the identity of the database.
		let resolved = Arc::clone(&tree.core.inner.opts);
		let ids: Vec<u64> = {
			let mut ids: Vec<u64> =
				tree.core.inner.level_manifest.read().unwrap().iter().map(|t| t.id).collect();
			ids.sort_unstable();
			ids
		};
		tree.close().await.unwrap();

		// Somebody changes a data block of one table and makes its checksum fit, which needs no
		// key: only the seal can tell.
		let path = resolved.sstable_file_path(ids[0]);
		let bytes = fs::read(&path).unwrap();
		let data = first_of(&walk(&resolved, &bytes, ids[0]), BlockKind::Data);
		fs::write(&path, flip(&bytes, &data, 0, TrailerLayout::Sealed, true)).unwrap();
		let before = snapshot(dir.path());

		// Opened with the compaction of two tables due, which the task makes at once.
		let compacting = options(dir.path(), &km, suite, 2);
		let tree = Tree::new(compacting).unwrap();
		until("the database to stop", || stopped(&tree)).await;

		let recorded = tree.core.inner.error_handler.get_error().unwrap();
		assert_eq!(recorded.severity, ErrorSeverity::Unrecoverable, "{suite:?}");
		assert_eq!(recorded.reason, BackgroundErrorReason::Compaction, "{suite:?}");
		assert!(
			matches!(recorded.error, Error::Encryption(EncryptionError::AuthenticationFailed)),
			"{suite:?}: {:?}",
			recorded.error
		);
		assert!(
			matches!(
				commit_error(&tree).await,
				Error::Encryption(EncryptionError::AuthenticationFailed)
			),
			"{suite:?}"
		);

		// Nothing was merged, repaired or removed: the changed table is where it was, with its
		// change, and so is every other table and the manifest.
		for dir in [resolved.sstable_dir(), resolved.manifest_dir()] {
			let now = snapshot(&dir);
			for (file, content) in &before {
				// The snapshot of a directory lists what it holds, not the directory itself.
				if file.starts_with(&dir) && *file != dir {
					assert_eq!(now.get(file), Some(content), "{suite:?}: {}", file.display());
				}
			}
		}
		assert_eq!(fs::read(&path).unwrap().len(), bytes.len(), "{suite:?}");
		assert_eq!(tree.core.inner.l0_file_count(), 2, "{suite:?}");
		tree.close().await.unwrap();
	}
}
