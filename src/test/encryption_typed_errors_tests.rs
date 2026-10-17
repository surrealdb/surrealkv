//! A key problem keeps its type wherever it is handed on, so that the caller can tell a provider
//! that is away, which asking again may cure, from a key that does not fit, which nothing will.
//!
//! The commit path and the background tasks have their own tests. These are the other places that
//! used to turn an encryption error into a text: the rotations of the write-ahead log, the repair
//! of a torn segment that a restart makes, and the open of a tree, which must also leave nothing
//! behind that keeps the database locked when it fails.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tempfile::TempDir;

use super::encryption_background_tests::{Mode, SwitchManager};
use super::encryption_commit_path_tests::{
	active_segment,
	assert_running,
	commit,
	crash,
	get,
	open,
	options,
};
use super::encryption_open_tests::{snapshot, SUITES};
use super::tfs;
use crate::error::{EncryptionError, KeyProviderFailure};
use crate::lsm::Core;
use crate::tde::{
	DbId,
	EncryptionContext,
	KeyManager,
	KeyState,
	SoftwareKeyManager,
	Zeroizing,
	ROOT_KEY_LEN,
};
use crate::wal::WalContext;
use crate::{Error, WalRecoveryMode};

fn is_unavailable<T>(result: &crate::Result<T>) -> bool {
	matches!(
		result,
		Err(Error::Encryption(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable)))
	)
}

fn unavailable() -> EncryptionError {
	EncryptionError::KeyProvider(KeyProviderFailure::Unavailable)
}

fn context(km: &Arc<SwitchManager>, suite: crate::tde::CipherSuite) -> Arc<EncryptionContext> {
	Arc::new(EncryptionContext::new(
		Arc::clone(km) as Arc<dyn KeyManager>,
		DbId::from_bytes([0xC9; 16]),
		suite,
	))
}

#[tokio::test]
async fn a_key_that_cannot_be_had_is_the_error_of_every_rotation_of_the_wal_and_moves_nothing() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let what = format!("{suite:?}");
		let dir = TempDir::new().unwrap();
		let km = SwitchManager::new();
		let enc = context(&km, suite);
		let tree = open(options(dir.path(), &enc, false)).await;
		commit(&tree, "a", 40).await.unwrap();
		let segment = active_segment(&tree);
		let inner = &tree.core.inner;

		// The memtable has data: the rotation of the memtable, and the sealing of its segment for a
		// write straight to level zero, which rotates the memtable, need a segment.
		km.set(Mode::Away);
		let rotated = inner.rotate_memtable();
		assert!(is_unavailable(&rotated), "{what}: {rotated:?}");
		let sealed = inner.seal_active_wal_segment();
		assert!(is_unavailable(&sealed), "{what}: {sealed:?}");
		assert_eq!(active_segment(&tree), segment, "{what}");
		assert_eq!(inner.immutable_count(), 0, "{what}: the memtable was not queued");
		assert_eq!(get(&tree, "a"), Some(vec![b'v'; 40]), "{what}");

		// Rotated once the key is back, so that the active memtable is empty: sealing its segment
		// is then a rotation of the log alone.
		km.set(Mode::Up);
		inner.rotate_memtable().unwrap();
		assert_eq!(inner.immutable_count(), 1, "{what}");
		let segment = active_segment(&tree);
		km.set(Mode::Away);
		let sealed = inner.seal_active_wal_segment();
		assert!(is_unavailable(&sealed), "{what}: {sealed:?}");
		assert_eq!(active_segment(&tree), segment, "{what}");

		// A failed segment, with nothing in the active memtable, is replaced by a rotation of the
		// log alone as well.
		km.set(Mode::Up);
		inner.wal.write().fail_writes_after(1);
		assert!(commit(&tree, "failed", 40).await.is_err(), "{what}");
		assert!(inner.wal.read().needs_replacement(), "{what}");
		km.set(Mode::Away);
		let replaced = inner.replace_failed_wal();
		assert!(is_unavailable(&replaced), "{what}: {replaced:?}");
		assert!(inner.wal.read().needs_replacement(), "{what}: nothing was replaced");

		// Nothing of this made the database refuse what comes after it.
		km.set(Mode::Up);
		commit(&tree, "after", 40).await.unwrap();
		assert_eq!(get(&tree, "after"), Some(vec![b'v'; 40]), "{what}");
		assert!(inner.error_handler.check_error().is_ok(), "{what}");
		crash(&tree);
	}
}

/// A seal that the log refuses, which only a bug that rewinds the count of a segment can make, is
/// told to the committer as what it is: not a provider that may come back, and not an I/O error.
#[tokio::test]
async fn a_seal_that_the_log_refuses_is_the_error_of_the_commit_and_the_segment_is_replaced() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let what = format!("{suite:?}");
		let dir = TempDir::new().unwrap();
		let km = SwitchManager::new();
		let enc = context(&km, suite);
		let tree = open(options(dir.path(), &enc, false)).await;
		commit(&tree, "a", 40).await.unwrap();

		// The count of the segment goes back to the batch that was sealed, as a bug would make it.
		tree.core.inner.wal.write().set_next_index(1);
		let refused = commit(&tree, "b", 40).await;
		assert!(
			matches!(refused, Err(Error::Encryption(EncryptionError::PositionReuse))),
			"{what}: {refused:?}"
		);
		assert_running(&tree, &what);
		assert!(tree.core.inner.wal.read().needs_replacement(), "{what}");
		assert_eq!(get(&tree, "b"), None, "{what}");

		// The segment that refused is replaced, under a key of its own, and the commits go on.
		commit(&tree, "c", 40).await.unwrap();
		assert!(!tree.core.inner.wal.read().needs_replacement(), "{what}");
		assert_eq!(get(&tree, "a"), Some(vec![b'v'; 40]), "{what}");
		assert_eq!(get(&tree, "c"), Some(vec![b'v'; 40]), "{what}");
		assert_running(&tree, &what);
		crash(&tree);
	}
}

// ---------------------------------------------------------------------------
// the repair of a torn segment
// ---------------------------------------------------------------------------

/// A key manager that gives its key, except while it is told to be away and `when` says that the
/// moment has come: a function of the files on disk, so that it fails at the step of a repair that
/// the test names and not at the nth question.
struct FailWhen {
	inner: SoftwareKeyManager,
	armed: AtomicBool,
	when: Box<dyn Fn() -> bool + Send + Sync>,
}

impl FailWhen {
	fn new(when: impl Fn() -> bool + Send + Sync + 'static) -> Arc<Self> {
		Arc::new(Self {
			inner: SoftwareKeyManager::new([0x31; ROOT_KEY_LEN]),
			armed: AtomicBool::new(false),
			when: Box::new(when),
		})
	}

	fn arm(&self, armed: bool) {
		self.armed.store(armed, Ordering::SeqCst);
	}
}

impl KeyManager for FailWhen {
	fn active_key_id(&self) -> u32 {
		self.inner.active_key_id()
	}

	fn key_state(&self, key_id: u32) -> KeyState {
		self.inner.key_state(key_id)
	}

	fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
		if self.armed.load(Ordering::SeqCst) && (self.when)() {
			return Err(unavailable());
		}
		self.inner.root_key(key_id)
	}

	fn prepare(&self, key_ids: &[u32]) -> Result<(), EncryptionError> {
		self.inner.prepare(key_ids)
	}
}

const ARENA: usize = 1 << 20;

/// A sealed log in `dir` that holds the commits `a`, `b` and `c`, the last of which was cut short
/// as a crash in the middle of its write leaves it. Returns the path of the segment.
async fn torn_wal(dir: &Path, enc: &Arc<EncryptionContext>) -> PathBuf {
	let tree = open(options(dir, enc, false)).await;
	for key in ["a", "b", "c"] {
		commit(&tree, key, 40).await.unwrap();
	}
	let segment = dir.join("wal").join(format!("{:020}.wal", active_segment(&tree)));
	crash(&tree);
	drop(tree);
	let len = fs::metadata(&segment).unwrap().len();
	fs::OpenOptions::new().write(true).open(&segment).unwrap().set_len(len - 5).unwrap();
	segment
}

/// Replays the log in `wal_dir` as a tree does, and says which of `a`, `b` and `c` it recovered.
fn replay(
	enc: &Arc<EncryptionContext>,
	wal_dir: &Path,
	mode: WalRecoveryMode,
) -> crate::Result<[bool; 3]> {
	let mut memtables = Vec::new();
	let (_, active, _) = Core::replay_wal_in(
		&WalContext::sealed(Arc::clone(enc)),
		wal_dir,
		0,
		"test",
		mode,
		ARENA,
		|memtable, _, _| {
			memtables.push(memtable);
			Ok(())
		},
	)?;
	memtables.extend(active);
	Ok(["a", "b", "c"]
		.map(|key| memtables.iter().any(|memtable| memtable.get(key.as_bytes(), None).is_some())))
}

#[tokio::test]
async fn a_key_that_cannot_be_had_to_repair_a_torn_segment_is_the_error_of_the_open_and_nothing_changes(
) {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let what = format!("{suite:?}");
		let dir = TempDir::new().unwrap();
		let wal_dir = dir.path().join("wal");
		// The key is away once the repair has made its directory, which is before it reads the
		// segment it repairs.
		let temp = wal_dir.join("repair_temp");
		let manager = FailWhen::new(move || temp.exists());
		let enc = Arc::new(EncryptionContext::new(
			Arc::clone(&manager) as Arc<dyn KeyManager>,
			DbId::from_bytes([0xCA; 16]),
			suite,
		));
		torn_wal(dir.path(), &enc).await;
		let before = snapshot(&wal_dir);

		manager.arm(true);
		let err = replay(&enc, &wal_dir, WalRecoveryMode::TolerateCorruptedWithRepair)
			.expect_err("the repair needs a key");
		assert!(
			matches!(&err, Error::Encryption(found) if *found == unavailable()),
			"{what}: {err:?}"
		);
		assert!(
			snapshot(&wal_dir) == before,
			"{what}: a repair that could not start changed a file"
		);

		// The key is back: the same recovery repairs the segment and finds what comes before the
		// tear.
		manager.arm(false);
		let recovered = replay(&enc, &wal_dir, WalRecoveryMode::TolerateCorruptedWithRepair)
			.expect("the repair works once the key is back");
		assert_eq!(recovered, [true, true, false], "{what}");
	}
}

#[tokio::test]
async fn a_key_that_cannot_be_had_once_a_segment_is_repaired_is_the_error_of_the_open_and_nothing_is_lost(
) {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let what = format!("{suite:?}");
		let dir = TempDir::new().unwrap();
		let wal_dir = dir.path().join("wal");
		// The key is away once the damaged segment has been kept aside, which is when the repair is
		// done and the log is read again.
		let watched = wal_dir.clone();
		let manager = FailWhen::new(move || {
			fs::read_dir(&watched)
				.unwrap()
				.flatten()
				.any(|entry| entry.file_name().to_string_lossy().ends_with(".corrupt"))
		});
		let enc = Arc::new(EncryptionContext::new(
			Arc::clone(&manager) as Arc<dyn KeyManager>,
			DbId::from_bytes([0xCB; 16]),
			suite,
		));
		let segment = torn_wal(dir.path(), &enc).await;
		let before = snapshot(&wal_dir);

		manager.arm(true);
		let err = replay(&enc, &wal_dir, WalRecoveryMode::TolerateCorruptedWithRepair)
			.expect_err("the log cannot be read again");
		assert!(
			matches!(&err, Error::Encryption(found) if *found == unavailable()),
			"{what}: {err:?}"
		);

		// The segment was repaired and the damaged one is kept as it was: nothing is gone.
		let after = snapshot(&wal_dir);
		let kept = PathBuf::from(format!("{}.corrupt", segment.display()));
		assert_eq!(after.get(&kept), before.get(&segment), "{what}: the damaged copy");
		assert_ne!(after.get(&segment), before.get(&segment), "{what}: the repaired segment");
		for (path, content) in &before {
			if *path != segment {
				assert_eq!(after.get(path), Some(content), "{what}: {}", path.display());
			}
		}

		// The key is back: the repaired log is read, and holds what came before the tear.
		manager.arm(false);
		let recovered = replay(&enc, &wal_dir, WalRecoveryMode::TolerateCorruptedWithRepair)
			.expect("the repaired log is read once the key is back");
		assert_eq!(recovered, [true, true, false], "{what}");
	}
}
