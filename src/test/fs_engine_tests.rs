//! The engine over a pool of files: `FileSystem::memory()` under a `Tree`.
//!
//! The pool is the file system of a browser with memory under it, so what these prove about the
//! engine (opening, committing across rotations, flushing, replacing the manifest, compacting,
//! closing, opening again, crashing and opening again) is what holds in a browser, up to the
//! backend. The helpers are shared with the tests of the crash points and of the exhaustion of the
//! pool.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use test_log::test;

use crate::vfs::{self, Fs, MemRaw, PoolConfig, PoolFs};
use crate::{Durability, FileSystem, Options, Tree, TreeBuilder};

/// The longest a scenario may take: a wait that is never woken fails the test instead of hanging
/// the suite.
pub(super) const WATCHDOG: Duration = Duration::from_secs(120);

/// Runs `scenario` under the watchdog.
pub(super) async fn within<F: Future>(what: &str, scenario: F) -> F::Output {
	match tokio::time::timeout(WATCHDOG, scenario).await {
		Ok(output) => output,
		Err(_) => panic!("{what}: no answer in {WATCHDOG:?}, something waits for ever"),
	}
}

pub(super) fn key_of(i: usize) -> Vec<u8> {
	format!("key_{i:06}").into_bytes()
}

pub(super) fn value_of(i: usize, len: usize) -> Vec<u8> {
	let mut v = format!("val_{i:06}_").into_bytes();
	v.resize(len.max(v.len()), b'x');
	v
}

/// Options for a database in `fs`: a small memtable, so that a few hundred commits rotate it and
/// flush it, and few level-0 files, so that a few flushes compact.
pub(super) fn options_in(fs: &FileSystem, max_memtable_size: usize) -> Options {
	Options {
		path: "db".into(),
		max_memtable_size,
		level0_max_files: 2,
		..Options::default()
	}
	.with_file_system(fs.clone())
}

/// A pool of memory that is refilled only by what waits for it, and holds `initial` files to
/// begin with and never more than `max_files`: the file system that makes a missing wait for the
/// spare files fail in a test and not in a browser. Current-thread tests only.
pub(super) fn strict_fs(initial: usize, max_files: usize) -> (FileSystem, Arc<PoolFs<MemRaw>>) {
	let (pool, _) =
		PoolFs::<MemRaw>::mount_sync(MemRaw::new(), PoolConfig::strict(initial, max_files))
			.expect("a strict pool mounts over memory");
	(FileSystem(Arc::clone(&pool) as Arc<dyn Fs>), pool)
}

/// A memory pool, and the pool itself, for a test that needs the memory underneath (to mount what a
/// crash leaves of it in a pool of another kind).
pub(super) fn memory_with_pool() -> (FileSystem, Arc<PoolFs<MemRaw>>) {
	let (pool, _) = PoolFs::<MemRaw>::mount_sync(MemRaw::new(), PoolConfig::memory())
		.expect("a memory pool mounts over memory");
	(FileSystem(Arc::clone(&pool) as Arc<dyn Fs>), pool)
}

/// A strict pool over the memory that `raw` is: what a crash left, mounted in a pool that is
/// refilled only by what waits for it.
pub(super) fn strict_over(
	raw: MemRaw,
	initial: usize,
	max_files: usize,
) -> (FileSystem, Arc<PoolFs<MemRaw>>) {
	let (pool, _) = PoolFs::<MemRaw>::mount_sync(raw, PoolConfig::strict(initial, max_files))
		.expect("a strict pool mounts over what a crash left");
	(FileSystem(Arc::clone(&pool) as Arc<dyn Fs>), pool)
}

/// Commits `range`, one transaction each, in `durability`. A commit that is let go because a flush
/// failed while the writers were stalled on it (`Error::PipelineStall`, as it always was) is made
/// again after a moment: the tests that fail flushes on purpose have writers that are stalled.
pub(super) async fn commit_range(
	tree: &Tree,
	range: std::ops::Range<usize>,
	value_len: usize,
	durability: Durability,
) {
	for i in range {
		let mut attempts = 0;
		loop {
			let mut txn = tree.begin().expect("a transaction begins");
			txn.set_durability(durability);
			txn.set(key_of(i), value_of(i, value_len)).expect("a write is buffered");
			match txn.commit().await {
				Ok(()) => break,
				Err(crate::Error::PipelineStall) if attempts < 1000 => {
					attempts += 1;
					tokio::time::sleep(Duration::from_millis(10)).await;
				}
				Err(e) => panic!("commit {i} failed: {e}"),
			}
		}
	}
}

/// What `range` reads as in `tree`: every key, with its value, or `None`.
pub(super) fn read_range(
	tree: &Tree,
	range: std::ops::Range<usize>,
) -> Vec<(usize, Option<Vec<u8>>)> {
	let txn = tree.begin_with_mode(crate::Mode::ReadOnly).expect("a snapshot begins");
	range.map(|i| (i, txn.get(key_of(i)).expect("a read answers"))).collect()
}

/// Asserts that every key of `range` is in `tree`, with the value it was written with.
pub(super) fn assert_present(tree: &Tree, range: std::ops::Range<usize>, value_len: usize) {
	for (i, got) in read_range(tree, range) {
		assert_eq!(got, Some(value_of(i, value_len)), "key {i}");
	}
}

pub(super) async fn open(fs: &FileSystem, max_memtable_size: usize) -> Tree {
	TreeBuilder::with_options(options_in(fs, max_memtable_size))
		.build_async()
		.await
		.expect("the database opens")
}

#[test(tokio::test)]
async fn a_database_in_a_memory_pool_commits_closes_and_opens_again() {
	within("smoke", async {
		let fs = FileSystem::memory();
		let tree = open(&fs, 1 << 20).await;
		commit_range(&tree, 0..50, 100, Durability::Immediate).await;
		assert_present(&tree, 0..50, 100);
		tree.close().await.unwrap();
		drop(tree);
		let tree = open(&fs, 1 << 20).await;
		assert_present(&tree, 0..50, 100);
		tree.close().await.unwrap();
		assert!(vfs::default_fs().kind() == vfs::FsKind::Std);
	})
	.await;
}

/// Polls until `done`, or fails when the watchdog would.
pub(super) async fn until(what: &str, mut done: impl FnMut() -> bool) {
	let started = std::time::Instant::now();
	while !done() {
		assert!(started.elapsed() < Duration::from_secs(60), "{what}: not within a minute");
		tokio::time::sleep(Duration::from_millis(5)).await;
	}
}

/// How many tables each level of `tree` holds.
pub(super) fn tables_per_level(tree: &Tree) -> Vec<usize> {
	let manifest = tree.core.level_manifest.read().unwrap();
	manifest.levels.get_levels().iter().map(|level| level.tables.len()).collect()
}

/// Waits until the queue of memtables is empty and a level-1 table exists.
pub(super) async fn until_compacted(tree: &Tree) {
	until("a flush and a compaction", || {
		tree.core.immutable_count() == 0 && tables_per_level(tree).iter().skip(1).any(|n| *n > 0)
	})
	.await;
}

#[test(tokio::test)]
async fn commits_rotate_flush_and_compact_in_a_pool_and_come_back_after_a_close() {
	within("rotate, flush, compact", async {
		for vlog in [false, true] {
			let fs = FileSystem::memory();
			let mut options = options_in(&fs, 64 * 1024);
			options.enable_vlog = vlog;
			options.vlog_value_threshold = 128;
			options.vlog_max_file_size = 256 * 1024;
			let tree = TreeBuilder::with_options(options.clone()).build_async().await.unwrap();
			// 3000 commits of 300 bytes: far more than a 64 KiB memtable takes.
			commit_range(&tree, 0..3000, 300, Durability::Eventual).await;
			until_compacted(&tree).await;
			assert_present(&tree, 0..3000, 300);
			assert!(
				fs.list_for_test("db/sstables").iter().any(|name| name.ends_with(".sst")),
				"tables were written, vlog={vlog}"
			);
			if vlog {
				assert!(
					fs.list_for_test("db/vlog").len() > 1,
					"the value log rolled, vlog={vlog}: {:?}",
					fs.list_for_test("db/vlog")
				);
			}
			tree.close().await.unwrap();
			drop(tree);

			let tree = TreeBuilder::with_options(options).build_async().await.unwrap();
			assert_present(&tree, 0..3000, 300);
			tree.close().await.unwrap();
		}
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// Closing, opening again, remounting
// ---------------------------------------------------------------------------------------------

/// What a pool is mounted as after the process died: the crash image of `fs` in `mode`, a pool that
/// makes its files when it is asked for.
fn remount(fs: &FileSystem, mode: crate::CrashMode) -> FileSystem {
	fs.simulate_crash(mode).expect("a memory pool crashes")
}

#[test(tokio::test)]
async fn a_closed_database_is_the_same_after_the_pool_is_closed_and_mounted_again() {
	within("close the pool", async {
		for vlog in [false, true] {
			let fs = FileSystem::memory();
			let mut options = options_in(&fs, 64 * 1024);
			options.enable_vlog = vlog;
			options.vlog_value_threshold = 128;
			options.vlog_max_file_size = 256 * 1024;
			let tree = TreeBuilder::with_options(options.clone()).build_async().await.unwrap();
			commit_range(&tree, 0..1500, 300, Durability::Eventual).await;
			tree.close().await.unwrap();
			drop(tree);
			// Closing the pool flushes what has to be flushed and releases every handle.
			fs.close().await.unwrap();

			// Everything is as the close left it, whichever way the crash goes: every header is
			// flushed and every file is synced by the close.
			for mode in [crate::CrashMode::LoseUnsynced, crate::CrashMode::ZeroTail] {
				let again = remount(&fs, mode);
				assert!(again.mount_report().is_some_and(|report| report.orphans_kept.is_empty()));
				let options = Options {
					fs: Arc::clone(&again.0),
					..options.clone()
				};
				let tree = TreeBuilder::with_options(options).build_async().await.unwrap();
				assert_present(&tree, 0..1500, 300);
				assert_eq!(tree.recovery_report().bytes_trimmed, 0);
				tree.close().await.unwrap();
			}
		}
	})
	.await;
}

#[test(tokio::test)]
async fn an_abandoned_tree_and_a_remounted_pool_give_back_every_commit_that_was_acknowledged() {
	within("abandon and remount", async {
		for vlog in [false, true] {
			// A crash that keeps a prefix of what was not synced, however it cuts, leaves a torn
			// tail, which the default recovery repairs. One that keeps a later write and loses an
			// earlier one (`Reorder`, `LoseTear`) leaves a hole with intact records behind it,
			// which is not a tail and which the default recovery refuses, as it should:
			// see the test below.
			for mode in [
				crate::CrashMode::LoseUnsynced,
				crate::CrashMode::ZeroTail,
				crate::CrashMode::Prefix(0),
				crate::CrashMode::Prefix(1),
				crate::CrashMode::Prefix(37),
				crate::CrashMode::Prefix(usize::MAX),
				crate::CrashMode::Tear(3, 5),
				crate::CrashMode::Tear(20, 1),
			] {
				let fs = FileSystem::memory();
				let mut options = options_in(&fs, 64 * 1024);
				options.enable_vlog = vlog;
				options.vlog_value_threshold = 128;
				options.vlog_max_file_size = 256 * 1024;
				let tree = TreeBuilder::with_options(options.clone()).build_async().await.unwrap();
				// Rotations and flushes happen on the way: the acknowledged set is across them.
				commit_range(&tree, 0..1200, 300, Durability::Immediate).await;
				// And some that were not synced: these may be lost, but only from the end.
				commit_range(&tree, 1200..1260, 300, Durability::Eventual).await;
				tree.abandon();
				fs.abandon();
				drop(tree);

				let again = remount(&fs, mode);
				let options = Options {
					fs: Arc::clone(&again.0),
					..options
				};
				let tree =
					TreeBuilder::with_options(options).build_async().await.unwrap_or_else(|e| {
						panic!("the database opens after {mode:?}, vlog={vlog}: {e}")
					});
				assert_present(&tree, 0..1200, 300);
				// What came after is a prefix: a key is there only if every one before it is.
				let found = read_range(&tree, 1200..1260);
				let lost_from =
					found.iter().position(|(_, got)| got.is_none()).unwrap_or(found.len());
				assert!(
					found[lost_from..].iter().all(|(_, got)| got.is_none()),
					"{mode:?}, vlog={vlog}: the commits that survive are a prefix"
				);
				for (i, got) in &found[..lost_from] {
					assert_eq!(got, &Some(value_of(*i, 300)));
				}
				// And the database is whole: it takes more commits, and closes.
				commit_range(&tree, 1260..1300, 300, Durability::Immediate).await;
				tree.close().await.unwrap();
			}
		}
	})
	.await;
}

#[test(tokio::test)]
async fn a_torn_tail_of_the_log_is_repaired_in_a_pool_and_the_commits_before_it_are_kept() {
	within("torn tail", async {
		let fs = FileSystem::memory();
		let options = options_in(&fs, 1 << 20);
		let tree = TreeBuilder::with_options(options.clone()).build_async().await.unwrap();
		commit_range(&tree, 0..200, 100, Durability::Immediate).await;
		// The active segment is the highest that has bytes in it.
		let segments = fs.list_for_test("db/wal");
		let active = segments
			.iter()
			.rev()
			.map(|name| format!("db/wal/{name}"))
			.find(|path| fs.file_len_for_test(path).is_some_and(|len| len > 0))
			.expect("the log holds the commits");
		let acked_len = fs.file_len_for_test(&active).unwrap();
		commit_range(&tree, 200..260, 100, Durability::Eventual).await;
		let total_len = fs.file_len_for_test(&active).unwrap();
		assert!(total_len - acked_len >= 64, "the unsynced commits are in the file");
		tree.abandon();
		fs.abandon();
		drop(tree);

		// Everything is in the file, as the browser keeps what was written; then it is cut in the
		// middle of a record, which is what a write that was torn leaves.
		let again = remount(&fs, crate::CrashMode::Prefix(usize::MAX));
		again.raw_truncate_for_test(&active, acked_len + 3).unwrap();
		assert_eq!(again.file_len_for_test(&active), Some(acked_len + 3));
		let options = Options {
			fs: Arc::clone(&again.0),
			..options
		};
		let tree = TreeBuilder::with_options(options).build_async().await.unwrap();
		let report = tree.recovery_report();
		assert!(report.bytes_trimmed > 0, "{report:?}");
		assert!(!report.quarantined_files.is_empty(), "a copy of the segment was kept");
		assert!(
			again.list_for_test("db/wal").iter().any(|name| name.contains(".corrupt")),
			"{:?}",
			again.list_for_test("db/wal")
		);
		assert_present(&tree, 0..200, 100);
		for (_, got) in read_range(&tree, 200..260) {
			assert!(got.is_none(), "the torn record is not replayed");
		}
		tree.close().await.unwrap();
		// The copy of the damaged segment goes with the segment, once the log is past it: a copy
		// that stayed would hold a file of the pool for good, and a tab that is killed
		// leaves one each time.
		assert!(
			!again.list_for_test("db/wal").iter().any(|name| name.contains(".corrupt")),
			"{:?}",
			again.list_for_test("db/wal")
		);
	})
	.await;
}

#[test(tokio::test)]
async fn a_crash_that_reorders_unsynced_writes_keeps_every_synced_commit_when_asked_to_salvage() {
	within("reorder", async {
		for seed in [1u64, 7, 99] {
			let fs = FileSystem::memory();
			let options = options_in(&fs, 1 << 20);
			let tree = TreeBuilder::with_options(options.clone()).build_async().await.unwrap();
			commit_range(&tree, 0..300, 100, Durability::Immediate).await;
			commit_range(&tree, 300..400, 100, Durability::Eventual).await;
			tree.abandon();
			fs.abandon();
			drop(tree);

			let again = remount(&fs, crate::CrashMode::Reorder(seed));
			let options = Options {
				fs: Arc::clone(&again.0),
				wal_recovery_mode: crate::WalRecoveryMode::SalvageValidPrefix,
				..options
			};
			let tree = TreeBuilder::with_options(options).build_async().await.unwrap();
			assert_present(&tree, 0..300, 100);
			tree.close().await.unwrap();
		}
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// Dropping a tree, the lock, and what a pool refuses
// ---------------------------------------------------------------------------------------------

#[test(tokio::test)]
async fn the_last_handle_closes_the_database_although_a_transaction_outlives_it() {
	within("the handle token", async {
		let fs = FileSystem::memory();
		let options = Arc::new(options_in(&fs, 1 << 20));
		// A tree that is never dropped (it would close the database, as it does on a native build):
		// only its core is kept.
		let tree = std::mem::ManuallyDrop::new(Tree::new(Arc::clone(&options)).unwrap());
		let core = Arc::clone(&tree.core);

		// A transaction holds the core, and so a count of the references to it never reaches one.
		let mut txn = crate::Transaction::new(
			Arc::clone(&core),
			crate::transaction::TransactionOptions::new(),
		)
		.unwrap();
		txn.set(b"k", b"v").unwrap();
		let handle = crate::lsm::TreeHandle::new(Arc::clone(&core));
		assert!(Arc::strong_count(&core) > 2, "the transaction and the handle hold the core");
		drop(core);

		// The handle is the last of the trees to go: the database is closed in the background, with
		// the transaction alive.
		assert!(fs.0.lock_file(std::path::Path::new("db")).acquire().is_err(), "it is open");
		drop(handle);
		let reopened = loop {
			match TreeBuilder::with_options((*options).clone()).build_async().await {
				Ok(tree) => break tree,
				Err(_) => tokio::time::sleep(Duration::from_millis(5)).await,
			}
		};
		// The transaction that outlived it finds a database that is shut: its commit fails, as it
		// does after `close`, and leaks nothing.
		assert!(matches!(txn.commit().await, Err(crate::Error::PipelineStall)));
		reopened.close().await.unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn closing_the_tree_cleans_the_files_that_were_removed_and_a_flush_leaves_few_dirty() {
	within("maintain", async {
		let (fs, pool) = memory_with_pool();
		let tree = open(&fs, 64 * 1024).await;
		commit_range(&tree, 0..3000, 300, Durability::Eventual).await;
		until_compacted(&tree).await;
		// The segments of the memtables that were flushed, the inputs of the compactions and the
		// manifests that were replaced are dead files: they are cleaned as the tasks go, a few at a
		// time, and not left for ever.
		assert!(pool.stats().dirty <= 64, "{:?}", pool.stats());
		tree.close().await.unwrap();
		let stats = pool.stats();
		assert_eq!(stats.dirty, 0, "close cleans every file that was removed: {stats:?}");
		assert_eq!(stats.poisoned, 0, "{stats:?}");
	})
	.await;
}

#[test(tokio::test)]
async fn an_abandoned_tree_flushes_nothing_when_it_is_dropped_and_gives_back_its_lock() {
	within("abandon", async {
		let fs = FileSystem::memory();
		let tree = open(&fs, 1 << 20).await;
		commit_range(&tree, 0..50, 100, Durability::Immediate).await;
		let manifest = "db/manifest/00000000000000000000.manifest";
		let manifest_len = fs.file_len_for_test(manifest);
		tree.abandon();
		// Abandoning is the end of the tree: a second time is nothing, and so is the drop.
		tree.abandon();
		drop(tree);
		tokio::time::sleep(Duration::from_millis(100)).await;
		assert!(
			!fs.list_for_test("db/sstables").iter().any(|name| name.ends_with(".sst")),
			"nothing was flushed: {:?}",
			fs.list_for_test("db/sstables")
		);
		assert_eq!(fs.file_len_for_test(manifest), manifest_len, "no manifest was written");
		// The lock is given back, so the same pool can open the database again.
		let mut lock = fs.0.lock_file(std::path::Path::new("db"));
		lock.acquire().expect("the lock of the abandoned tree was released");
		lock.release().unwrap();
		let again = open(&fs, 1 << 20).await;
		assert_present(&again, 0..50, 100);
		again.close().await.unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn a_failed_open_gives_back_the_lock_and_the_next_open_works() {
	within("a failed open", async {
		let (probe, fs) = super::fs_probe::ProbeFs::wrap(&FileSystem::memory());
		let tree = open(&fs, 1 << 20).await;
		commit_range(&tree, 0..30, 100, Durability::Immediate).await;
		tree.close().await.unwrap();
		drop(tree);

		// The open fails after it took the lock: its segment of the log cannot be made.
		probe.before(|call| {
			if call.makes_a_segment() {
				return super::fs_probe::Verdict::Fail(std::io::Error::other("no segment today"));
			}
			super::fs_probe::Verdict::Go
		});
		let built = TreeBuilder::with_options(options_in(&fs, 1 << 20)).build_async().await;
		assert!(built.is_err(), "the open fails");
		let mut lock = fs.0.lock_file(std::path::Path::new("db"));
		lock.acquire().expect("the entry of the lock was given back");
		lock.release().unwrap();

		probe.clear_hooks();
		let tree = open(&fs, 1 << 20).await;
		assert_present(&tree, 0..30, 100);
		tree.close().await.unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn a_second_tree_in_the_same_pool_and_directory_is_refused() {
	within("two trees", async {
		let fs = FileSystem::memory();
		let tree = open(&fs, 1 << 20).await;
		let second = TreeBuilder::with_options(options_in(&fs, 1 << 20)).build_async().await;
		assert!(second.is_err(), "the lock is held by the first");
		// Another directory is another database.
		let other = TreeBuilder::with_options(Options {
			path: "other".into(),
			..options_in(&fs, 1 << 20)
		})
		.build_async()
		.await
		.expect("another directory");
		other.close().await.unwrap();
		tree.close().await.unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn what_a_pool_does_not_do_yet_is_refused_before_anything_is_created() {
	within("refusals", async {
		use super::encryption_open_tests::manager;
		use crate::tde::CipherPreference;

		let fs = FileSystem::memory();
		let refused = |built: crate::Result<Tree>| match built {
			Err(crate::Error::InvalidArgument(message)) => {
				assert!(message.contains("not supported on this file system yet"), "{message}");
				message
			}
			Err(other) => panic!("a typed refusal was expected: {other:?}"),
			Ok(_) => panic!("a refusal was expected"),
		};
		let message = refused(
			TreeBuilder::with_options(options_in(&fs, 1 << 20))
				.with_encryption(manager(), CipherPreference::Auto)
				.build_async()
				.await,
		);
		assert!(message.contains("encryption"), "{message}");
		let message = refused(
			TreeBuilder::with_options(options_in(&fs, 1 << 20))
				.with_encryption(manager(), CipherPreference::Auto)
				.build(),
		);
		assert!(message.contains("encryption"), "{message}");
		assert!(
			fs.list_for_test("db").is_empty(),
			"nothing was created: {:?}",
			fs.list_for_test("db")
		);
		assert!(fs.mount_report().is_some_and(|report| report.files == 0));

		// A tree that is open refuses the two operations that copy and replace files by path.
		let tree = open(&fs, 1 << 20).await;
		let checkpoint = tree.create_checkpoint("checkpoint");
		assert!(
			matches!(&checkpoint, Err(crate::Error::InvalidArgument(m)) if m.contains("checkpoint"))
		);
		let restore = tree.restore_from_checkpoint("checkpoint");
		assert!(
			matches!(&restore, Err(crate::Error::InvalidArgument(m)) if m.contains("checkpoint"))
		);
		assert!(fs.list_for_test("checkpoint").is_empty());
		tree.close().await.unwrap();
	})
	.await;
}

#[test]
fn building_a_tree_asynchronously_makes_a_future_that_can_be_sent_between_threads() {
	fn is_send<T: Send>(_: &T) {}
	is_send(&TreeBuilder::new().build_async());
	is_send(&TreeBuilder::new().build_with_options_async());
}

// ---------------------------------------------------------------------------------------------
// Runtimes
// ---------------------------------------------------------------------------------------------

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_database_in_a_pool_works_on_a_runtime_with_threads_and_concurrent_committers() {
	within("multi-thread", async {
		let fs = FileSystem::memory();
		let tree = Arc::new(open(&fs, 64 * 1024).await);
		let mut tasks = Vec::new();
		for task in 0..8usize {
			let tree = Arc::clone(&tree);
			tasks.push(tokio::spawn(async move {
				for j in 0..250usize {
					let i = task * 1000 + j;
					loop {
						let mut txn = tree.begin().unwrap();
						txn.set(key_of(i), value_of(i, 200)).unwrap();
						match txn.commit().await {
							Ok(()) => break,
							Err(crate::Error::PipelineStall) => {
								tokio::time::sleep(Duration::from_millis(5)).await
							}
							Err(e) => panic!("commit {i}: {e}"),
						}
					}
				}
			}));
		}
		for task in tasks {
			task.await.unwrap();
		}
		for task in 0..8usize {
			assert_present(&tree, task * 1000..task * 1000 + 250, 200);
		}
		tree.close().await.unwrap();
		assert!(!tree.is_stopped());
	})
	.await;
}

/// Runs a future that never waits for anything outside the process, with no runtime at all.
fn poll_to_the_end<F: Future>(future: F) -> F::Output {
	let waker = std::task::Waker::noop();
	let mut context = std::task::Context::from_waker(waker);
	let mut future = std::pin::pin!(future);
	for _ in 0..1000 {
		if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut context) {
			return output;
		}
	}
	panic!("the future waits for something that no runtime brings");
}

/// A pool needs no runtime and no thread, and the parts of an open that refuse come before the
/// tasks of a tree, which need one: on a thread that is in no runtime they answer, and do not
/// panic.
#[test]
fn the_memory_pool_and_the_refusals_of_an_open_need_no_runtime() {
	// The pool: made, used and closed from a plain function.
	let fs = FileSystem::memory();
	fs.0.create_dir_all(std::path::Path::new("d")).unwrap();
	let file = fs.0.create_new(std::path::Path::new("d/f"), vfs::CreateOpts::default()).unwrap();
	file.append(b"bytes").unwrap();
	file.sync().unwrap();
	assert_eq!(fs.file_len_for_test("d/f"), Some(5));
	poll_to_the_end(fs.close()).unwrap();

	// The refusals, which are the first thing `build_async` does and so are before any task.
	let nothing = FileSystem(Arc::new(vfs::UnavailableFs::no_file_system()));
	let built = poll_to_the_end(TreeBuilder::new().with_file_system(nothing).build_async());
	assert!(
		matches!(built, Err(crate::Error::Io(ref e)) if e.kind() == std::io::ErrorKind::Unsupported)
	);
	let pool = FileSystem::memory();
	let built = poll_to_the_end(
		TreeBuilder::with_options(options_in(&pool, 1 << 20))
			.with_encryption(
				super::encryption_open_tests::manager(),
				crate::tde::CipherPreference::Auto,
			)
			.build_async(),
	);
	assert!(matches!(built, Err(crate::Error::InvalidArgument(_))), "{:?}", built.map(|_| ()));
}
