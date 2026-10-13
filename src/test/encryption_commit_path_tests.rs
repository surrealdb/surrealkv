//! A key problem on the commit path of an encrypted database.
//!
//! A commit may have to start a file: a segment of the write-ahead log when the memtable rotates
//! or a failed segment is replaced, a file of the value log, a table and the manifest of a write
//! that goes straight to level zero. Each of them needs a key. The root keys are served from what
//! `prepare` made available when the database was opened, so a provider that is away reaches none
//! of them. What is left is a manager that has lost a key, or whose active key changed, and for
//! that a commit that has not yet applied anything of its group is refused with the error as it
//! is and the database goes on, while a failure after part of a group was applied stops it, as it
//! does for any other error.

use std::fs;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tempfile::TempDir;

use super::encryption_background_tests::{Mode, SwitchManager};
use super::encryption_open_tests::{files_with_extension, SUITES};
use crate::error::{EncryptionError, KeyProviderFailure};
use crate::ring::PipelineHook;
use crate::tde::{CipherSuite, DbId, EncryptionContext, KeyManager};
use crate::{
	BackgroundErrorReason,
	Durability,
	Error,
	ErrorSeverity,
	Mode as TxnMode,
	Options,
	Tree,
};

/// A memtable that holds about 21 small commits, so that a group of 30 fills it part-way.
pub(super) const SMALL: usize = 4096;

fn context(km: &Arc<SwitchManager>, suite: CipherSuite) -> Arc<EncryptionContext> {
	Arc::new(EncryptionContext::new(
		Arc::clone(km) as Arc<dyn KeyManager>,
		DbId::from_bytes([0xC7; 16]),
		suite,
	))
}

/// Options of a database in `path` with a memtable that holds a few commits, and a value log, if
/// there is one, whose files are full after one value.
pub(super) fn options(path: &Path, enc: &Arc<EncryptionContext>, vlog: bool) -> Arc<Options> {
	Arc::new(
		Options {
			path: path.to_path_buf(),
			max_memtable_size: SMALL,
			flush_on_close: false,
			enable_vlog: vlog,
			vlog_value_threshold: 64,
			vlog_max_file_size: 1024,
			// No background flush runs in these tests, so a rotation that queues a memtable must
			// not stall the commits behind it.
			memtable_stall_threshold: 1000,
			..Default::default()
		}
		.with_encryption_context(Arc::clone(enc)),
	)
}

/// A tree whose background tasks are stopped, so that nothing is flushed or removed behind the
/// test's back, and that is marked closed: the test ends it with `crash`. Shared through an `Arc`,
/// never `Tree::clone`: dropping any clone spawns a close.
pub(super) async fn open(opts: Arc<Options>) -> Arc<Tree> {
	let tree = Arc::new(Tree::new(opts).unwrap());
	// These tests form groups that hold an oversized batch with others, or rotate the memtable more
	// than once, to exercise what the pipeline does then: a group is only bounded by its estimates
	// at the sizes where it cannot, see `group_est_budget`.
	tree.core.commit_pipeline.lift_group_estimate_cap();
	let tasks = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tasks.stop().await;
	tree.core.is_closed.store(true, Ordering::SeqCst);
	// Every replacement of a failed segment is let through at once, whatever came before.
	tree.core
		.commit_pipeline
		.set_replace_backoff(std::time::Duration::ZERO, std::time::Duration::ZERO);
	tree
}

/// Ends a tree the way a crash does: no close, no flush.
pub(super) fn crash(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
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

pub(super) async fn commit(tree: &Tree, key: &str, len: usize) -> crate::Result<()> {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(Durability::Immediate);
	txn.set(key.as_bytes(), vec![b'v'; len]).unwrap();
	txn.commit().await
}

async fn commit_owned(tree: Arc<Tree>, key: String, len: usize) -> crate::Result<()> {
	commit(&tree, &key, len).await
}

/// One single-key transaction per entry, all spawned before the flusher is polled: on the
/// current-thread runtime that is one commit group, in order.
async fn commit_group(tree: &Arc<Tree>, entries: &[(String, usize)]) -> Vec<crate::Result<()>> {
	let handles: Vec<_> = entries
		.iter()
		.map(|(key, len)| tokio::spawn(commit_owned(Arc::clone(tree), key.clone(), *len)))
		.collect();
	let mut results = Vec::new();
	for handle in handles {
		results.push(handle.await.unwrap());
	}
	results
}

pub(super) fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin_with_mode(TxnMode::ReadOnly).unwrap().get(key.as_bytes()).unwrap()
}

pub(super) fn active_segment(tree: &Tree) -> u64 {
	tree.core.inner.wal.read().get_active_log_number()
}

pub(super) fn is_unavailable(result: &crate::Result<()>) -> bool {
	matches!(
		result,
		Err(Error::Encryption(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable)))
	)
}

/// The database was not stopped, by a commit group or by anything else.
pub(super) fn assert_running(tree: &Tree, what: &str) {
	let handler = &tree.core.inner.error_handler;
	assert!(handler.check_error().is_ok(), "{what}");
	assert!(handler.get_error().is_none(), "{what}");
	assert!(handler.commit_group_error().is_none(), "{what}");
}

// ---------------------------------------------------------------------------
// a group that is refused before any of it is applied
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_group_that_a_key_problem_refuses_before_it_is_applied_fails_and_the_database_goes_on() {
	for suite in SUITES {
		let what = format!("{suite:?}");
		let dir = TempDir::new().unwrap();
		let live = dir.path().join("live");
		let km = SwitchManager::new();
		let enc = context(&km, suite);
		let tree = open(options(&live, &enc, false)).await;

		// The memtable holds one commit, and the next of that size does not fit: the memtable and
		// the write-ahead log rotate before the group is logged, and the rotation needs a key.
		commit(&tree, "first", 2500).await.unwrap();
		let visible = tree.core.seq_num();
		let segment = active_segment(&tree);
		let group: Vec<(String, usize)> = (1..=3).map(|i| (format!("g{i}"), 2500)).collect();

		km.set(Mode::Away);
		let results = commit_group(&tree, &group).await;
		for result in &results {
			assert!(is_unavailable(result), "{what}: {result:?}");
		}

		// Every committer of the group was told the error as it is, and the database runs: nothing
		// was applied, nothing is published, the log did not move and reads are as they were.
		assert_running(&tree, &what);
		assert_eq!(tree.core.seq_num(), visible, "{what}");
		assert_eq!(active_segment(&tree), segment, "{what}");
		assert_eq!(get(&tree, "first"), Some(vec![b'v'; 2500]), "{what}");
		for (key, _) in &group {
			assert_eq!(get(&tree, key), None, "{what}: {key}");
		}

		// A crash image. The group may have been logged before the rotation that needed the key,
		// and a commit that failed is not promised to be absent from a recovery (an error does
		// not mean not applied), so recovery finds the whole group or none of it, and the commit
		// before it either way.
		let image = dir.path().join("image");
		copy_dir(&live, &image);
		km.set(Mode::Up);
		let recovered = Tree::new(options(&image, &enc, false)).unwrap();
		assert_eq!(get(&recovered, "first"), Some(vec![b'v'; 2500]), "{what}");
		let found: Vec<Option<Vec<u8>>> =
			group.iter().map(|(key, _)| get(&recovered, key)).collect();
		let none = found.iter().all(|value| value.is_none());
		let all = found.iter().all(|value| value.as_deref() == Some(&[b'v'; 2500][..]));
		assert!(none || all, "{what}: a recovery found part of the group");
		crash(&recovered);

		// The provider is back: the same group goes through, and so does the next commit.
		let results = commit_group(&tree, &group).await;
		for result in &results {
			assert!(result.is_ok(), "{what}: {result:?}");
		}
		commit(&tree, "later", 40).await.unwrap();
		for (key, _) in &group {
			assert_eq!(get(&tree, key), Some(vec![b'v'; 2500]), "{what}: {key}");
		}
		assert_running(&tree, &what);
		crash(&tree);
	}
}

#[tokio::test]
async fn a_key_problem_after_part_of_a_group_was_applied_stops_the_database_as_any_failure_does() {
	for suite in SUITES {
		let what = format!("{suite:?}");
		let dir = TempDir::new().unwrap();
		let km = SwitchManager::new();
		let enc = context(&km, suite);
		let tree = open(options(dir.path(), &enc, false)).await;
		for i in 0..10 {
			commit(&tree, &format!("base{i}"), 40).await.unwrap();
		}
		let visible = tree.core.seq_num();

		// The provider goes away as the group starts to be applied: the first batches fit the
		// memtable, and the rotation that the one that does not fit needs is refused.
		let provider = Arc::clone(&km);
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
			if matches!(
				point,
				PipelineHook::BeforeApplyRound {
					round: 0
				}
			) {
				provider.set(Mode::Away);
			}
		})));
		let group: Vec<(String, usize)> = (0..30).map(|i| (format!("group{i}"), 40)).collect();
		let results = commit_group(&tree, &group).await;
		tree.core.commit_pipeline.set_hook(None);

		for result in &results {
			match result {
				Err(Error::DatabaseStopped(message)) => {
					assert!(
						message.contains("the key provider is unavailable"),
						"{what}: {message}"
					);
				}
				other => panic!("{what}: expected the database to be stopped, got {other:?}"),
			}
		}
		let handler = &tree.core.inner.error_handler;
		let recorded = handler.get_error().expect("a background error");
		assert_eq!(recorded.reason, BackgroundErrorReason::CommitGroup, "{what}");
		assert_eq!(recorded.severity, ErrorSeverity::Unrecoverable, "{what}");
		assert!(handler.is_db_stopped(), "{what}");
		assert_eq!(tree.core.seq_num(), visible, "{what}");
		for (key, _) in &group {
			assert_eq!(get(&tree, key), None, "{what}: {key}");
		}

		// The provider is back, and the database is still stopped: only opening it again decides
		// the outcome of the group.
		km.set(Mode::Up);
		let later = commit(&tree, "later", 40).await;
		assert!(matches!(later, Err(Error::DatabaseStopped(_))), "{what}: {later:?}");
		crash(&tree);
	}
}

// ---------------------------------------------------------------------------
// a segment that is replaced
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_failed_segment_is_replaced_once_the_key_can_be_had_and_the_commit_that_found_it_fails() {
	for suite in SUITES {
		let what = format!("{suite:?}");
		let dir = TempDir::new().unwrap();
		let km = SwitchManager::new();
		let enc = context(&km, suite);
		let tree = open(options(dir.path(), &enc, false)).await;
		commit(&tree, "before", 40).await.unwrap();

		// A write fails, the commit with it, and the segment refuses every append after it.
		tree.core.inner.wal.write().fail_writes_after(1);
		assert!(commit(&tree, "failed", 40).await.is_err(), "{what}");
		assert!(tree.core.inner.wal.read().needs_replacement(), "{what}");

		// The next group has to replace the segment, which needs a key. It fails with the error as
		// it is, before anything of it is logged, and the database runs.
		km.set(Mode::Away);
		let refused = commit(&tree, "next", 40).await;
		assert!(is_unavailable(&refused), "{what}: {refused:?}");
		assert_running(&tree, &what);
		assert!(tree.core.inner.wal.read().needs_replacement(), "{what}: nothing was replaced");

		// The provider is back: the segment is replaced and the commit goes through.
		km.set(Mode::Up);
		commit(&tree, "after", 40).await.unwrap();
		assert!(!tree.core.inner.wal.read().needs_replacement(), "{what}");
		assert_running(&tree, &what);
		assert_eq!(get(&tree, "before"), Some(vec![b'v'; 40]), "{what}");
		assert_eq!(get(&tree, "after"), Some(vec![b'v'; 40]), "{what}");
		assert_eq!(get(&tree, "failed"), None, "{what}");
		assert_eq!(get(&tree, "next"), None, "{what}");
		crash(&tree);
	}
}

#[tokio::test]
async fn a_held_segment_whose_memtables_cannot_be_flushed_for_want_of_a_key_fails_with_that_error()
{
	for suite in SUITES {
		let what = format!("{suite:?}");
		let dir = TempDir::new().unwrap();
		let km = SwitchManager::new();
		let enc = context(&km, suite);
		let tree = open(options(dir.path(), &enc, false)).await;
		commit(&tree, "before", 40).await.unwrap();

		// An fsync fails: the segment is replaced by one that is held until the memtable that is
		// tagged with the failed one is in a table.
		tree.core.inner.wal.write().fail_syncs_after(0);
		assert!(commit(&tree, "failed", 40).await.is_err(), "{what}");

		// The replacement itself is made, and then the provider goes away: the flush of that
		// memtable, which the heal waits for, is refused.
		let provider = Arc::clone(&km);
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
			if matches!(point, PipelineHook::AfterWalReplace) {
				provider.set(Mode::Away);
			}
		})));
		let refused = commit(&tree, "next", 40).await;
		tree.core.commit_pipeline.set_hook(None);
		assert!(is_unavailable(&refused), "{what}: {refused:?}");
		assert_running(&tree, &what);
		assert!(tree.core.inner.wal.read().is_held(), "{what}");

		// The provider is back: the memtable is flushed, the writer released, and the commit made.
		km.set(Mode::Up);
		commit(&tree, "after", 40).await.unwrap();
		assert!(!tree.core.inner.wal.read().is_held(), "{what}");
		assert_eq!(get(&tree, "before"), Some(vec![b'v'; 40]), "{what}");
		assert_eq!(get(&tree, "after"), Some(vec![b'v'; 40]), "{what}");
		assert_running(&tree, &what);
		crash(&tree);
	}
}

// ---------------------------------------------------------------------------
// a file of the value log, and a table written straight to level zero
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_value_log_file_that_cannot_be_started_fails_the_commit_and_leaves_no_file() {
	for suite in SUITES {
		let what = format!("{suite:?}");
		let dir = TempDir::new().unwrap();
		let km = SwitchManager::new();
		let enc = context(&km, suite);
		let opts = options(dir.path(), &enc, true);
		let tree = open(Arc::clone(&opts)).await;
		// The log has no file yet: the first value that goes to it starts one.
		km.set(Mode::Away);
		let refused = commit(&tree, "big0", 1500).await;
		assert!(is_unavailable(&refused), "{what}: {refused:?}");
		assert!(files_with_extension(&opts.vlog_dir(), "vlog").is_empty(), "{what}");
		assert_running(&tree, &what);

		km.set(Mode::Up);
		commit(&tree, "big0", 1500).await.unwrap();
		assert_eq!(files_with_extension(&opts.vlog_dir(), "vlog").len(), 1, "{what}");

		// That file is full after one value, so the next value starts another.
		km.set(Mode::Away);
		let refused = commit(&tree, "big1", 1500).await;
		assert!(is_unavailable(&refused), "{what}: {refused:?}");
		assert_eq!(files_with_extension(&opts.vlog_dir(), "vlog").len(), 1, "{what}");
		assert_running(&tree, &what);
		assert_eq!(get(&tree, "big1"), None, "{what}");

		km.set(Mode::Up);
		commit(&tree, "big1", 1500).await.unwrap();
		assert_eq!(files_with_extension(&opts.vlog_dir(), "vlog").len(), 2, "{what}");
		assert_eq!(get(&tree, "big0"), Some(vec![b'v'; 1500]), "{what}");
		assert_eq!(get(&tree, "big1"), Some(vec![b'v'; 1500]), "{what}");
		assert_running(&tree, &what);
		crash(&tree);
	}
}

#[tokio::test]
async fn a_table_whose_manifest_cannot_be_sealed_fails_the_commit_that_wrote_it_and_is_removed() {
	for suite in SUITES {
		let what = format!("{suite:?}");
		let dir = TempDir::new().unwrap();
		let km = SwitchManager::new();
		let enc = context(&km, suite);
		let opts = options(dir.path(), &enc, false);
		let tree = open(Arc::clone(&opts)).await;
		commit(&tree, "small", 40).await.unwrap();

		// A commit larger than the memtable is written straight to a table of level zero. The
		// provider goes away once that table is on disk, which is when the manifest is sealed.
		let provider = Arc::clone(&km);
		*tree.core.inner.direct_table_hook.lock() = Some(Arc::new(move |_| {
			provider.set(Mode::Away);
			Ok(())
		}));
		let refused = commit(&tree, "big", SMALL + 1000).await;
		*tree.core.inner.direct_table_hook.lock() = None;
		assert!(is_unavailable(&refused), "{what}: {refused:?}");
		assert_running(&tree, &what);
		assert_eq!(get(&tree, "big"), None, "{what}");

		// The file of the table that was not registered is its own, and is gone: what is on disk
		// is what the manifest lists.
		let mut listed: Vec<u64> =
			tree.core.inner.level_manifest.read().unwrap().iter().map(|table| table.id).collect();
		listed.sort_unstable();
		let mut on_disk: Vec<u64> = files_with_extension(&opts.sstable_dir(), "sst")
			.iter()
			.map(|path| path.file_stem().unwrap().to_string_lossy().parse().unwrap())
			.collect();
		on_disk.sort_unstable();
		assert_eq!(on_disk, listed, "{what}");

		km.set(Mode::Up);
		commit(&tree, "big", SMALL + 1000).await.unwrap();
		assert_eq!(get(&tree, "big"), Some(vec![b'v'; SMALL + 1000]), "{what}");
		assert_eq!(get(&tree, "small"), Some(vec![b'v'; 40]), "{what}");
		assert_running(&tree, &what);
		crash(&tree);
	}
}
