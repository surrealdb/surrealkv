//! A pool that runs out of files under a database.
//!
//! The pool has a fixed number of files to create from. A creation it cannot serve fails with the
//! typed `Error::FilePoolExhausted`, which every layer keeps (or the exhaustion witness catches,
//! where one does not), and which means that the work is done again once the pool is refilled: a
//! flush and a compaction wait and retry, a commit that met it before anything of it was applied
//! fails and may be tried again, and nothing stops the database. A pool that is at its limit of
//! files is not short of one, and its error does not pass.
//!
//! The file system under these tests is `ProbeFs`, which fails a call at an exact place and says
//! what the engine asked of it.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use test_log::test;

use super::fs_engine_tests::{
	assert_present,
	commit_range,
	key_of,
	memory_with_pool,
	options_in,
	strict_fs,
	strict_over,
	until,
	until_compacted,
	value_of,
	within,
};
use super::fs_probe::{Call, ProbeFs, Verdict, Wait};
use crate::error::ErrorSeverity;
use crate::lsm::FLUSH_NEED;
use crate::vfs::{self, SpareClass};
use crate::{Durability, Error, FileSystem, Tree, TreeBuilder};

const MEMTABLE: usize = 64 * 1024;

async fn open_probed(fs: &FileSystem, memtable: usize, vlog: bool) -> Tree {
	let mut options = options_in(fs, memtable);
	options.enable_vlog = vlog;
	options.vlog_value_threshold = 128;
	options.vlog_max_file_size = 256 * 1024;
	TreeBuilder::with_options(options).build_async().await.expect("the database opens")
}

/// Commits, one transaction at a time, until `done` says stop or a commit fails, and returns the
/// error of the commit that failed, if one did, with how many were acknowledged before it.
async fn commit_until(
	tree: &Tree,
	from: usize,
	limit: usize,
	value_len: usize,
	mut done: impl FnMut() -> bool,
) -> (usize, Option<Error>) {
	let mut next = from;
	while next < limit && !done() {
		let mut txn = tree.begin().unwrap();
		txn.set(key_of(next), value_of(next, value_len)).unwrap();
		match txn.commit().await {
			Ok(()) => next += 1,
			Err(e) => return (next, Some(e)),
		}
	}
	(next, None)
}

fn exhausted() -> io::Error {
	vfs::pool_exhausted()
}

// ---------------------------------------------------------------------------------------------
// What the engine asks of its file system, and where
// ---------------------------------------------------------------------------------------------

#[test(tokio::test)]
async fn spare_files_are_awaited_at_the_five_sites_and_nowhere_else() {
	within("the five sites", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_probed(&fs, MEMTABLE, false).await;
		let group_need = tree.core.inner.group_need.load(Ordering::Acquire);
		assert!(group_need > FLUSH_NEED, "a group may need more than a flush: {group_need}");

		// 1. The open: 8 and one for the manifest of a database that is new, no segment to recover.
		assert_eq!(probe.asked(), vec![9], "build_async asks for 2K + 1 + 8 files, K = 0");

		commit_range(&tree, 0..2500, 300, Durability::Eventual).await;
		until_compacted(&tree).await;
		tree.close().await.unwrap();

		let asked = probe.asked();
		// 2. The group site, before every group, with `group_need`; 3. the flush site with
		// `FLUSH_NEED`; 4. the compaction site, with the files that the commit path keeps back and
		// a first hint of 4 or more; 5. the close, `3 * (queued + 1) + 4`.
		assert!(asked.contains(&group_need), "{asked:?}");
		assert!(asked.contains(&FLUSH_NEED), "{asked:?}");
		assert!(asked.iter().any(|n| *n >= group_need + 4 && *n != group_need), "{asked:?}");
		assert_eq!(
			*asked.last().unwrap(),
			3 + 4,
			"the close asks for what the shutdown flush makes"
		);
		for n in &asked {
			assert!(
				*n == 9
					|| *n == group_need
					|| *n == FLUSH_NEED
					|| *n >= group_need + 4
					|| *n == 7
					|| *n == 3 * 2 + 4
					|| *n == 3 * 3 + 4,
				"a request that no site makes: {n} of {asked:?}"
			);
		}
	})
	.await;
}

#[test(tokio::test)]
async fn a_database_on_std_never_asks_for_a_spare_file() {
	within("std asks for nothing", async {
		let dir = tempfile::TempDir::new().unwrap();
		let tree = TreeBuilder::new()
			.with_path(dir.path().join("db"))
			.with_max_memtable_size(MEMTABLE)
			.build_async()
			.await
			.unwrap();
		assert!(!tree.core.inner.pooled);
		commit_range(&tree, 0..500, 300, Durability::Eventual).await;
		assert_eq!(tree.core.inner.group_need.load(Ordering::Acquire), 0, "no sizing is made");
		assert!(tree.core.inner.opts.fs.ensure_spares(1_000_000).is_none());
		assert_eq!(tree.core.inner.opts.fs.pool_exhaustions(), 0);
		tree.close().await.unwrap();
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// A pool that is short for a moment: nothing stops
// ---------------------------------------------------------------------------------------------

#[test(tokio::test)]
async fn a_flush_that_meets_an_empty_pool_waits_and_the_database_is_not_stopped() {
	within("flush meets an empty pool", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_probed(&fs, MEMTABLE, false).await;
		let refused = Arc::new(AtomicUsize::new(0));
		let counter = Arc::clone(&refused);
		probe.before(move |call| {
			// The first three tables of the flushes are refused, as a pool with no file free
			// refuses.
			if call.makes_a_table()
				&& matches!(call, Call::CreateTruncate(_, SpareClass::Reserved))
				&& counter.fetch_add(1, Ordering::SeqCst) < 3
			{
				return Verdict::Fail(exhausted());
			}
			Verdict::Go
		});
		commit_range(&tree, 0..1500, 300, Durability::Eventual).await;
		until("the flushes", || {
			tree.core.inner.immutable_count() == 0 && refused.load(Ordering::SeqCst) >= 3
		})
		.await;
		assert!(refused.load(Ordering::SeqCst) >= 3);
		assert!(!tree.is_stopped(), "a pool that was short did not stop the database");
		assert!(tree.background_error_count() >= 1, "the flush that failed was recorded");
		assert_eq!(
			tree.background_error().map(|e| e.severity).filter(|s| *s > ErrorSeverity::SoftError),
			None
		);
		assert_present(&tree, 0..1500, 300);
		tree.close().await.unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn a_flush_whose_manifest_meets_an_empty_pool_waits_and_the_database_is_not_stopped() {
	within("manifest meets an empty pool", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_probed(&fs, MEMTABLE, false).await;
		let refused = Arc::new(AtomicUsize::new(0));
		let counter = Arc::clone(&refused);
		probe.before(move |call| {
			if call.replaces_the_manifest() && counter.fetch_add(1, Ordering::SeqCst) < 3 {
				return Verdict::Fail(exhausted());
			}
			Verdict::Go
		});
		commit_range(&tree, 0..1500, 300, Durability::Eventual).await;
		until("the flushes", || tree.core.inner.immutable_count() == 0).await;
		assert!(refused.load(Ordering::SeqCst) >= 3, "the manifest was refused three times");
		assert!(!tree.is_stopped());
		until_compacted(&tree).await;
		assert_present(&tree, 0..1500, 300);
		tree.close().await.unwrap();
		drop(tree);
		let tree = open_probed(&fs, MEMTABLE, false).await;
		assert_present(&tree, 0..1500, 300);
		tree.close().await.unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn a_rotation_that_meets_an_empty_pool_before_a_group_is_applied_fails_it_retryably() {
	within("rotation meets an empty pool", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		// A memtable that takes about twenty commits, so that the rotation comes soon.
		let tree = open_probed(&fs, 4096, false).await;
		commit_range(&tree, 0..5, 100, Durability::Immediate).await;
		let armed = Arc::new(AtomicBool::new(true));
		let gate = Arc::clone(&armed);
		probe.before(move |call| {
			if call.makes_a_segment() && gate.swap(false, Ordering::SeqCst) {
				return Verdict::Fail(exhausted());
			}
			Verdict::Go
		});
		let (reached, failed) = commit_until(&tree, 5, 400, 100, || false).await;
		let error = failed.expect("a commit meets the refused rotation");
		assert!(
			matches!(error, Error::FilePoolExhausted),
			"typed, as the committer sees it: {error:?}"
		);
		assert!(!armed.load(Ordering::SeqCst), "the rotation was the refused call");
		assert!(!tree.is_stopped(), "nothing of the group was applied, so nothing stops");

		// The same commit goes through when it is tried again, and so do the ones after it.
		let (next, failed) = commit_until(&tree, reached, 400, 100, || false).await;
		assert!(failed.is_none(), "{failed:?}");
		assert_eq!(next, 400);
		assert_present(&tree, 0..400, 100);
		tree.close().await.unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn a_compaction_output_that_meets_an_empty_pool_waits_with_a_bigger_hint() {
	within("compaction output meets an empty pool", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_probed(&fs, MEMTABLE, false).await;
		let refused = Arc::new(AtomicUsize::new(0));
		let counter = Arc::clone(&refused);
		probe.before(move |call| {
			// An output of a compaction is made `Opportunistic`: it is refused while the pool keeps
			// its files back for the commit path.
			if matches!(call, Call::CreateTruncate(p, SpareClass::Opportunistic)
				if p.extension().is_some_and(|e| e == "sst"))
				&& counter.fetch_add(1, Ordering::SeqCst) < 2
			{
				return Verdict::Fail(exhausted());
			}
			Verdict::Go
		});
		commit_range(&tree, 0..2500, 300, Durability::Eventual).await;
		until_compacted(&tree).await;
		assert!(refused.load(Ordering::SeqCst) >= 2, "two outputs were refused");
		assert!(!tree.is_stopped());
		let group_need = tree.core.inner.group_need.load(Ordering::Acquire);
		let asked = probe.asked();
		// The wait of the compaction grew after a refusal: 4, then 8.
		assert!(asked.contains(&(group_need + 4)), "{asked:?}");
		assert!(asked.contains(&(group_need + 8)), "{asked:?}");
		assert_present(&tree, 0..2500, 300);
		tree.close().await.unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn a_compaction_waits_for_about_as_many_files_as_the_tables_it_merges_can_fill() {
	within("compaction outputs bound", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let mut options = options_in(&fs, MEMTABLE);
		// Tables of 4 KiB: what a compaction merges fills many.
		options.target_file_size = 4096;
		let tree = TreeBuilder::with_options(options).build_async().await.unwrap();
		let group_need = tree.core.inner.group_need.load(Ordering::Acquire);
		commit_range(&tree, 0..4000, 300, Durability::Eventual).await;
		until("the flushes", || tree.core.inner.immutable_count() == 0).await;
		tokio::time::sleep(Duration::from_millis(100)).await;
		let most = probe.asked().into_iter().max().unwrap();
		assert!(
			most > group_need + 4,
			"a compaction of many tables asked for more than the first hint: {most} of {:?}",
			probe.asked()
		);
		assert!(!tree.is_stopped());
		assert_present(&tree, 0..4000, 300);
		tree.close().await.unwrap();
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// The waits themselves, forced to answer with an error at each site
// ---------------------------------------------------------------------------------------------

#[test(tokio::test)]
async fn a_wait_for_spares_that_fails_with_the_typed_error_fails_the_group_and_stops_nothing() {
	within("group wait fails, typed", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_probed(&fs, MEMTABLE, false).await;
		let group_need = tree.core.inner.group_need.load(Ordering::Acquire);
		let fail = Arc::new(AtomicBool::new(true));
		let gate = Arc::clone(&fail);
		probe.spares(move |n| {
			(n == group_need && gate.load(Ordering::SeqCst)).then(|| Err(exhausted()))
		});

		let mut txn = tree.begin().unwrap();
		txn.set(b"k", b"v").unwrap();
		let error = txn.commit().await.expect_err("the group is not started");
		assert!(matches!(error, Error::FilePoolExhausted), "{error:?}");
		assert!(!tree.is_stopped());

		fail.store(false, Ordering::SeqCst);
		let mut txn = tree.begin().unwrap();
		txn.set(b"k", b"v").unwrap();
		txn.commit().await.expect("the same commit goes through once the pool has files");
		assert_eq!(tree.begin().unwrap().get(b"k").unwrap(), Some(b"v".to_vec()));
		tree.close().await.unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn a_wait_for_spares_that_fails_because_the_pool_is_full_is_typed_and_does_not_pass() {
	within("group wait fails, pool full", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_probed(&fs, MEMTABLE, false).await;
		let group_need = tree.core.inner.group_need.load(Ordering::Acquire);
		probe.spares(move |n| (n == group_need).then(|| Err(vfs::pool_full(2048))));
		for _ in 0..3 {
			let mut txn = tree.begin().unwrap();
			txn.set(b"k", b"v").unwrap();
			let error = txn.commit().await.expect_err("a pool at its limit has no file to give");
			assert!(error.is_pool_full(), "typed, and the same each time: {error:?}");
		}
		assert!(!tree.is_stopped(), "the group was refused before anything of it began");
		tree.close().await.unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn a_flush_whose_wait_fails_with_the_typed_error_is_tried_again() {
	within("flush wait fails, typed", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_probed(&fs, MEMTABLE, false).await;
		let failures = Arc::new(AtomicUsize::new(0));
		let counter = Arc::clone(&failures);
		probe.spares(move |n| {
			(n == FLUSH_NEED && counter.fetch_add(1, Ordering::SeqCst) < 2)
				.then(|| Err(exhausted()))
		});
		commit_range(&tree, 0..1500, 300, Durability::Eventual).await;
		until("the flushes", || tree.core.inner.immutable_count() == 0).await;
		assert!(failures.load(Ordering::SeqCst) >= 3, "the wait failed twice and then worked");
		assert!(!tree.is_stopped());
		assert_present(&tree, 0..1500, 300);
		tree.close().await.unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn a_flush_whose_wait_fails_because_the_pool_is_full_is_not_tried_again() {
	within("flush wait fails, pool full", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_probed(&fs, MEMTABLE, false).await;
		probe.spares(move |n| (n == FLUSH_NEED).then(|| Err(vfs::pool_full(2048))));
		let (reached, _) =
			commit_until(&tree, 0, 3000, 300, || tree.background_error().is_some()).await;
		until("the flush task to give up", || tree.background_error().is_some()).await;
		let error = tree.background_error().unwrap();
		assert!(error.error.is_pool_full(), "{:?}", error.error);
		assert_eq!(error.severity, ErrorSeverity::FatalError, "an I/O error of a flush is fatal");
		let count = tree.background_error_count();
		tokio::time::sleep(Duration::from_millis(300)).await;
		assert_eq!(tree.background_error_count(), count, "nothing is attempted again");
		// What was acknowledged is still readable, and is in the log.
		assert_present(&tree, 0..reached, 300);
		tree.abandon();
	})
	.await;
}

#[test(tokio::test)]
async fn a_compaction_whose_wait_fails_is_tried_again_and_the_database_goes_on() {
	within("compaction wait fails", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_probed(&fs, MEMTABLE, false).await;
		let group_need = tree.core.inner.group_need.load(Ordering::Acquire);
		let failures = Arc::new(AtomicUsize::new(0));
		let counter = Arc::clone(&failures);
		probe.spares(move |n| {
			(n > group_need && counter.fetch_add(1, Ordering::SeqCst) < 2).then(|| Err(exhausted()))
		});
		commit_range(&tree, 0..2500, 300, Durability::Eventual).await;
		until_compacted(&tree).await;
		assert!(failures.load(Ordering::SeqCst) >= 2);
		assert!(!tree.is_stopped());
		assert_present(&tree, 0..2500, 300);
		tree.close().await.unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn a_close_whose_wait_fails_ends_with_that_error_and_loses_nothing() {
	within("close wait fails", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_probed(&fs, MEMTABLE, false).await;
		commit_range(&tree, 0..300, 300, Durability::Immediate).await;
		// The close is the only caller that asks for fewer than a flush does.
		probe.spares(|n| (n < FLUSH_NEED).then(|| Err(vfs::pool_full(2048))));
		let closed = tree.close().await;
		assert!(matches!(closed, Err(ref e) if e.is_pool_full()), "{closed:?}");
		tree.abandon();
		drop(tree);
		probe.clear_hooks();

		// The commits are in the log, and the database opens again with every one of them.
		let again = fs.simulate_crash(crate::CrashMode::LoseUnsynced).unwrap();
		let tree = open_probed(&again, MEMTABLE, false).await;
		assert_present(&tree, 0..300, 300);
		tree.close().await.unwrap();
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// The second net: a failure that does not look like one that passes
// ---------------------------------------------------------------------------------------------

/// A table that cannot be created, with an error that is nothing like the pool's own (so that the
/// flush wraps it into text), while the pool counts a refusal. The witness reads the counter and
/// takes the failure for one that passes, and without a refusal it is the hard error it looks like.
async fn a_table_that_fails_with_text(refused_by_the_pool: bool) -> (ErrorSeverity, bool) {
	let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
	let tree = open_probed(&fs, MEMTABLE, false).await;
	let failing = Arc::new(AtomicBool::new(true));
	let gate = Arc::clone(&failing);
	let counted = Arc::clone(&probe);
	probe.before(move |call| {
		if call.makes_a_table() && gate.load(Ordering::SeqCst) {
			if refused_by_the_pool {
				counted.count_exhaustion();
			}
			return Verdict::Fail(io::Error::other("the disk is on fire"));
		}
		Verdict::Go
	});
	let (reached, _) =
		commit_until(&tree, 0, 3000, 300, || tree.background_error().is_some()).await;
	until("the flush to fail", || tree.background_error().is_some()).await;
	let error = tree.background_error().unwrap();
	let stopped = tree.is_stopped();
	failing.store(false, Ordering::SeqCst);
	until("the flush to recover", || {
		tree.core.inner.immutable_count() == 0 && tree.background_error().is_none()
	})
	.await;
	assert_present(&tree, 0..reached, 300);
	tree.close().await.unwrap();
	(error.severity, stopped)
}

#[test(tokio::test)]
async fn a_failure_that_came_with_a_refusal_of_the_pool_is_soft_whatever_it_looks_like() {
	within("the witness", async {
		let (severity, stopped) = a_table_that_fails_with_text(true).await;
		assert_eq!(severity, ErrorSeverity::SoftError, "the witness saw the pool refuse a file");
		assert!(!stopped, "and so the database was not stopped");
	})
	.await;
}

#[test(tokio::test)]
async fn the_same_failure_without_a_refusal_of_the_pool_stops_writes_until_it_passes() {
	within("the witness control", async {
		let (severity, stopped) = a_table_that_fails_with_text(false).await;
		assert_eq!(severity, ErrorSeverity::HardError, "the control: nothing says it passes");
		assert!(stopped, "writes were stopped while it lasted, and the flush that works ended it");
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// A pool that is at its limit
// ---------------------------------------------------------------------------------------------

#[test(tokio::test)]
async fn a_pool_at_its_limit_fails_a_commit_with_a_typed_error_that_does_not_pass() {
	within("pool full", async {
		// The group of a commit asks for more files than a pool of 20 has left once the database
		// holds a few of them: its tables, its log and its manifest stay.
		let (fs, pool) = strict_fs(20, 20);
		let tree = open_probed(&fs, MEMTABLE, false).await;
		let (reached, failed) = commit_until(&tree, 0, 20_000, 400, || false).await;
		let error = failed.expect("a database that grows fills a pool of 20 files");
		assert!(error.is_pool_full(), "typed as a pool that is at its limit: {error:?}");
		assert!(reached > 10, "it worked until then: {reached}");
		// And it does not pass by waiting: the same commit fails the same way.
		let (_, again) = commit_until(&tree, reached, reached + 1, 400, || false).await;
		assert!(again.is_some_and(|e| e.is_pool_full()));
		assert_eq!(pool.stats().total, 20, "the pool did not grow past its limit");
		// What was acknowledged is there.
		assert_present(&tree, 0..reached, 400);
		tree.abandon();
	})
	.await;
}

/// The memory of a database that was left by a crash with a long log: the bytes of a pool. Its
/// flushes are refused all the time (and the writers are never stalled for them), so that every
/// memtable that rotated is a segment of the log that is still to be recovered.
async fn a_log_that_is_long(memtable: usize, commits: usize) -> crate::vfs::MemRaw {
	let (fs, pool) = memory_with_pool();
	let (probe, fs) = ProbeFs::wrap(&fs);
	probe.before(|call| {
		if call.makes_a_table() {
			return Verdict::Fail(exhausted());
		}
		Verdict::Go
	});
	let mut options = options_in(&fs, memtable);
	options.flush_on_close = false;
	options.memtable_stall_threshold = 30;
	let tree = TreeBuilder::with_options(options).build_async().await.unwrap();
	commit_range(&tree, 0..commits, 1000, Durability::Immediate).await;
	// A crash: nothing is flushed, and the log is what holds the commits.
	tree.abandon();
	let image = pool.raw().crash(crate::CrashMode::LoseUnsynced);
	drop(tree);
	image
}

#[test(tokio::test)]
async fn an_open_that_is_short_of_files_is_tried_again_with_more() {
	within("build_async attempts", async {
		let image = a_log_that_is_long(1 << 20, 12_000).await;
		let (strict, pool) = strict_over(image, 12, 4096);
		let (probe, fs) = ProbeFs::wrap(&strict);

		// Opened with a memtable sixteen times smaller, every segment of the log becomes sixteen
		// tables: far more files than `2K + 1 + 8` for the K segments that are there.
		let tree = open_probed(&fs, 64 * 1024, false).await;
		let asked = probe.asked();
		assert!(asked.len() >= 2, "the open asked again: {asked:?}");
		assert_eq!(asked[1], 2 * asked[0], "with twice as many files: {asked:?}");
		assert!(pool.stats().exhaustions > 0, "the pool did run short");
		assert_present(&tree, 0..12_000, 1000);
		assert!(!tree.is_stopped());
		tree.close().await.unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn an_open_stops_trying_when_the_pool_is_at_its_limit() {
	within("build_async stops", async {
		let image = a_log_that_is_long(1 << 20, 24_000).await;
		let (strict, pool) = strict_over(image, 12, 40);
		let (probe, fs) = ProbeFs::wrap(&strict);
		let built = TreeBuilder::with_options(options_in(&fs, 64 * 1024)).build_async().await;
		let error = built.map(|_| ()).expect_err("forty files are too few for this recovery");
		assert!(
			error.is_pool_full() || matches!(error, Error::FilePoolExhausted),
			"typed: {error:?}"
		);
		assert!(
			probe.asked().len() < 16,
			"it did not go on for sixteen attempts: {:?}",
			probe.asked()
		);
		assert!(pool.stats().total <= 40);
		// And the lock was given back each time: the same database opens in a bigger pool.
		let again = fs.simulate_crash(crate::CrashMode::LoseUnsynced).unwrap();
		let _ = again;
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// The arbiter of the sizing: a pool that is refilled only by what waits for it
// ---------------------------------------------------------------------------------------------

/// What a transaction of a workload writes.
#[derive(Clone, Copy, Debug)]
struct Shape {
	name: &'static str,
	keys: usize,
	key_len: usize,
	value_len: usize,
	/// Every this many transactions, one writes a single value of `OVERSIZED` bytes, more than a
	/// memtable takes, which goes to a table of its own. 0 for none.
	oversized_every: usize,
}

/// More than the arena of a memtable of `MEMTABLE` bytes has to give.
const OVERSIZED: usize = 3 * MEMTABLE;

const SHAPES: &[Shape] = &[
	Shape {
		name: "4B/0B",
		keys: 40,
		key_len: 4,
		value_len: 0,
		oversized_every: 0,
	},
	Shape {
		name: "8B/8B",
		keys: 10,
		key_len: 8,
		value_len: 8,
		oversized_every: 0,
	},
	Shape {
		name: "50B/100B",
		keys: 5,
		key_len: 50,
		value_len: 100,
		oversized_every: 0,
	},
	Shape {
		name: "16B/1KiB",
		keys: 3,
		key_len: 16,
		value_len: 1024,
		oversized_every: 0,
	},
	Shape {
		name: "solo oversized",
		keys: 1,
		key_len: 16,
		value_len: OVERSIZED,
		oversized_every: 1,
	},
	Shape {
		name: "normal and oversized",
		keys: 5,
		key_len: 20,
		value_len: 200,
		oversized_every: 7,
	},
];

fn shaped_key(task: usize, n: usize, key_len: usize) -> Vec<u8> {
	let mut key = format!("{task}:{n:06}:").into_bytes();
	key.resize(key_len.max(key.len()), b'k');
	key
}

/// Commits `transactions` transactions of `shape` from each of `tasks` tasks at once, so that the
/// flusher forms groups of several, and reads everything back.
async fn run_shape(tree: &Arc<Tree>, shape: Shape, tasks: usize, transactions: usize) {
	let mut handles = Vec::new();
	for task in 0..tasks {
		let tree = Arc::clone(tree);
		handles.push(tokio::spawn(async move {
			for n in 0..transactions {
				loop {
					let mut txn = tree.begin().unwrap();
					if shape.oversized_every > 0 && n % shape.oversized_every == 0 {
						txn.set(
							shaped_key(task, n, 24),
							vec![b'o'; OVERSIZED.max(shape.value_len)],
						)
						.unwrap();
					} else {
						for k in 0..shape.keys {
							txn.set(
								shaped_key(task, n * 1000 + k, shape.key_len),
								vec![b'v'; shape.value_len],
							)
							.unwrap();
						}
					}
					match txn.commit().await {
						Ok(()) => break,
						Err(Error::PipelineStall) => {
							tokio::time::sleep(Duration::from_millis(2)).await
						}
						Err(e) => panic!("{}: task {task}, transaction {n}: {e}", shape.name),
					}
				}
			}
		}));
	}
	for handle in handles {
		handle.await.unwrap();
	}
}

/// No site of the engine may create a file that it did not wait for: a pool that has only the files
/// that the waits asked for, and that no task refills, is never found empty, whatever the shape of
/// the commits and with the value log on or off. A group that was found short after part of it was
/// applied would have stopped the database, which is the failure that this is the check of.
#[test(tokio::test)]
async fn a_pool_that_only_the_waits_refill_is_never_found_empty() {
	within("strict arbiter", async {
		for vlog in [false, true] {
			for shape in SHAPES {
				// What a group may need, and room for what a flush takes while the group runs (on a
				// runtime that has a global thread pool, the other tasks run inside a group).
				let (fs, pool) = strict_fs(17 + 2 * FLUSH_NEED, 2048);
				let mut options = options_in(&fs, MEMTABLE);
				options.enable_vlog = vlog;
				options.vlog_value_threshold = 128;
				options.vlog_max_file_size = 256 * 1024;
				let tree =
					Arc::new(TreeBuilder::with_options(options).build_async().await.unwrap());
				let need = tree.core.inner.group_need.load(Ordering::Acquire);
				assert!((16..=17).contains(&need), "the sizing is what the test assumes: {need}");

				run_shape(&tree, *shape, 8, 20).await;
				until("the flushes", || tree.core.inner.immutable_count() == 0).await;
				// Whatever compaction these flushes started.
				tokio::time::sleep(Duration::from_millis(100)).await;

				assert_eq!(
					pool.stats().exhaustions,
					0,
					"{}, vlog={vlog}: a creation found the pool empty",
					shape.name
				);
				assert!(!tree.is_stopped(), "{}, vlog={vlog}", shape.name);
				assert!(tree.core.inner.error_handler.commit_group_error().is_none());
				assert_eq!(tree.background_error_count(), 0, "{}, vlog={vlog}", shape.name);
				tree.close().await.unwrap();
			}
		}
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// A refill that never answers must not keep the database from closing
// ---------------------------------------------------------------------------------------------

#[test(tokio::test)]
async fn a_flush_that_waits_for_a_refill_that_never_comes_does_not_keep_close_from_ending() {
	within("stuck flush wait", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = Arc::new(open_probed(&fs, MEMTABLE, false).await);
		probe.spares_wait(|n| (n == FLUSH_NEED).then_some(Wait::Never));

		// The writers go on until the queue of memtables is full, and then they are stalled, as
		// they are behind a flush that does not come.
		let writer = {
			let tree = Arc::clone(&tree);
			tokio::spawn(async move { commit_until(&tree, 0, 100_000, 300, || false).await })
		};
		until("the queue of memtables to be full", || tree.core.inner.immutable_count() >= 2).await;
		tokio::time::sleep(Duration::from_millis(100)).await;
		assert!(!writer.is_finished(), "the writer is stalled");

		// The flush task is stuck in its wait. `close` stops it and flushes what is queued itself,
		// and ends; the writer that was stalled is let go with an error.
		tree.close().await.expect("close ends, with every memtable flushed");
		let (reached, error) = writer.await.unwrap();
		assert!(error.is_some(), "the stalled writer was let go");
		assert!(reached > 0);
		probe.clear_hooks();
		drop(tree);
		let again = open_probed(&fs, MEMTABLE, false).await;
		assert_present(&again, 0..reached, 300);
		again.close().await.unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn a_close_that_waits_for_a_refill_that_never_comes_flushes_with_the_files_the_pool_has() {
	within("stuck close wait", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_probed(&fs, MEMTABLE, false).await;
		commit_range(&tree, 0..300, 300, Durability::Immediate).await;
		// The close asks for fewer files than a flush does, and its wait never ends.
		probe.spares_wait(|n| (n < FLUSH_NEED).then_some(Wait::Never));
		tree.close().await.expect("close goes on after a while, and the pool had the files");
		drop(tree);
		probe.clear_hooks();
		let again = open_probed(&fs, MEMTABLE, false).await;
		assert_present(&again, 0..300, 300);
		again.close().await.unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn a_group_that_waits_for_a_refill_that_never_comes_fails_when_the_tree_is_closed() {
	within("stuck group wait", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = Arc::new(open_probed(&fs, MEMTABLE, false).await);
		commit_range(&tree, 0..20, 100, Durability::Immediate).await;
		let group_need = tree.core.inner.group_need.load(Ordering::Acquire);
		probe.spares_wait(move |n| (n == group_need).then_some(Wait::Never));

		// A commit that is stuck behind a wait that never ends.
		let stuck = {
			let tree = Arc::clone(&tree);
			tokio::spawn(async move {
				let mut txn = tree.begin().unwrap();
				txn.set(b"stuck", b"v").unwrap();
				txn.commit().await
			})
		};
		tokio::time::sleep(Duration::from_millis(50)).await;
		assert!(!stuck.is_finished(), "the commit waits");

		// `close` shuts the pipeline, and the wait of the group is bounded from then on: the commit
		// fails, and the close ends.
		tree.close().await.expect("close ends");
		let result = stuck.await.unwrap();
		assert!(result.is_err(), "the commit that never began fails: {result:?}");
		probe.clear_hooks();
		drop(tree);
		let again = open_probed(&fs, MEMTABLE, false).await;
		assert_present(&again, 0..20, 100);
		assert_eq!(again.begin().unwrap().get(b"stuck").unwrap(), None);
		again.close().await.unwrap();
	})
	.await;
}
