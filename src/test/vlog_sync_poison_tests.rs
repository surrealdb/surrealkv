//! A failed fsync of the value log ends it, and stops the database.
//!
//! After an fsync error the kernel drops the dirty pages it could not write and no longer counts
//! them as dirty, so a later fsync of the same file reports success without them. A value written
//! before the failure can then never be made durable, and a commit, a table or a WAL record that
//! points at one must not be told it is. So the first failed fsync of the log fails every later
//! sync of it, and records a fatal error that every commit fails with until the database is
//! opened again, when recovery decides what survived.
//!
//! The failure of a writeback is modelled by `MemRaw::writeback_error_after`, which drops what the
//! file was to flush, as Linux does; `VLog::fail_next_sync` fails one fsync without writing or
//! dropping anything.

use std::path::Path;
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use super::fs_engine_tests::memory_with_pool;
use crate::vfs::CrashMode;
use crate::vlog::VLog;
use crate::{Durability, Error, FileSystem, Mode, Options, Tree, VLogChecksumLevel};

fn vlog_options(fs: &FileSystem) -> Arc<Options> {
	let opts = Options {
		path: "db".into(),
		vlog_checksum_verification: VLogChecksumLevel::Full,
		..Options::default()
	}
	.with_file_system(fs.clone());
	super::tfs::create_dir_all(&opts, opts.vlog_dir()).unwrap();
	Arc::new(opts)
}

fn value_of(i: u8, len: usize) -> Vec<u8> {
	vec![i; len]
}

/// A sync after one whose writeback failed would report the value it lost as durable, and a
/// crash would then lose a value that a commit was told is on disk. It fails instead, and so does
/// every sync after it, of values appended later too.
#[test]
fn a_sync_after_a_failed_writeback_does_not_report_its_values_durable() {
	let (fs, pool) = memory_with_pool();
	let vlog = VLog::new(vlog_options(&fs)).unwrap();
	let kept = vlog.append(b"kept", &value_of(1, 600)).unwrap();
	vlog.sync().unwrap();
	let lost = vlog.append(b"lost", &value_of(2, 600)).unwrap();

	pool.raw().writeback_error_after(0);
	assert!(vlog.sync().is_err(), "the writeback failed");
	let retried = vlog.sync();

	let crashed = fs.simulate_crash(CrashMode::LoseUnsynced).unwrap();
	let reopened = VLog::new(vlog_options(&crashed)).unwrap();
	assert_eq!(reopened.get(&kept).unwrap(), value_of(1, 600));
	let survived = reopened.get(&lost).is_ok_and(|v| v == value_of(2, 600));
	assert!(
		retried.is_err() || survived,
		"a sync reported the value durable, and the crash lost it"
	);
	assert!(retried.is_err(), "a sync after a failed one succeeded");
	assert!(vlog.sync_failed());

	vlog.append(b"later", &value_of(3, 600)).unwrap();
	assert!(vlog.sync().is_err(), "a value appended after the failure was reported durable");
	assert!(vlog.sync_dirty().is_err());
}

fn tree_options(path: &Path) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		flush_on_close: false,
		enable_vlog: true,
		vlog_value_threshold: 100,
		..Default::default()
	})
}

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key.as_bytes()).unwrap()
}

async fn try_put(
	tree: &Tree,
	key: &str,
	value: &[u8],
	durability: Durability,
) -> crate::Result<()> {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(durability);
	txn.set(key.as_bytes(), value).unwrap();
	txn.commit().await
}

/// The commit whose group fsyncs the value log when the fsync fails fails, and so does every
/// commit after it, synced or not, until the database is opened again. What was acknowledged
/// before is there after the reopen, and nothing of what failed is.
#[test(tokio::test)]
async fn a_failed_value_log_fsync_fails_its_commit_and_stops_the_database() {
	let dir = TempDir::new("vlog_sync_poison").unwrap();
	let tree = Tree::new(tree_options(dir.path())).unwrap();
	try_put(&tree, "before", &value_of(1, 600), Durability::Immediate).await.unwrap();

	let vlog = Arc::clone(tree.core.inner.vlog.as_ref().expect("the tree has a value log"));
	vlog.fail_next_sync();
	let failed = try_put(&tree, "failed", &value_of(2, 600), Durability::Immediate).await;
	assert!(failed.is_err(), "the commit whose fsync failed was acknowledged");
	assert_eq!(get(&tree, "failed"), None, "a failed commit is visible");
	assert!(vlog.sync_failed());

	for (key, durability) in [("after", Durability::Immediate), ("eventual", Durability::Eventual)]
	{
		match try_put(&tree, key, &value_of(3, 600), durability).await {
			Err(Error::DatabaseStopped(message)) => {
				assert!(message.contains("value log"), "{message}")
			}
			other => {
				panic!("a commit after the failure: expected a stopped database, got {other:?}")
			}
		}
	}
	assert!(tree.flush_wal(true).is_err(), "the WAL was synced after the value log failed");
	let _ = tree.close().await;
	drop(tree);

	let reopened = Tree::new(tree_options(dir.path())).unwrap();
	assert_eq!(get(&reopened, "before"), Some(value_of(1, 600)));
	for key in ["failed", "after", "eventual"] {
		assert_eq!(get(&reopened, key), None, "{key}");
	}
	try_put(&reopened, "reopened", &value_of(4, 600), Durability::Immediate).await.unwrap();
	reopened.close().await.unwrap();
}
