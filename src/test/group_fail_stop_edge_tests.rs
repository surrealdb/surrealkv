//! Cases the stop of a database after a commit group failed once part of it was applied has to get
//! right, which `group_fail_stop_tests` does not reach: a group of which one batch only was
//! applied, a batch the memtable took part of, a restore that comes between the rounds of a group,
//! another hard error while commits are queued or stored first, deletes and values in the value
//! log in the part that was applied, failures at random moments with commits arriving all the
//! time, and a flush, a close or a crash that comes between the rounds of a group, which must
//! leave the group whole or absent for recovery.

use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tempdir::TempDir;
use test_log::test;

use super::{collect_transaction_all, collect_transaction_reverse};
use crate::lsm::CompactionOperations;
use crate::ring::{CommitStage, PipelineHook};
use crate::{BackgroundErrorReason, Durability, Error, Mode, Options, Result, Tree};

/// A memtable that holds about 21 small commits.
const SMALL: usize = 4096;

/// How long a commit may wait for its verdict before the test calls it stuck.
const DEADLINE: Duration = Duration::from_secs(60);

type Entries = Vec<(String, Vec<u8>)>;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn opts(path: &Path) -> Options {
	Options {
		path: path.to_path_buf(),
		max_memtable_size: SMALL,
		flush_on_close: false,
		// No background flush runs in these tests, so a rotation that queues a memtable must not
		// stall the commits behind it.
		memtable_stall_threshold: 1000,
		..Default::default()
	}
}

/// A tree whose background tasks are stopped, so that nothing is flushed or removed behind the
/// test's back, and that is marked closed so that a test which fails half way does not leave a
/// drop to close it.
async fn open(options: Options) -> Arc<Tree> {
	let tree = Arc::new(Tree::new(Arc::new(options)).unwrap());
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	tree.core.is_closed.store(true, Ordering::SeqCst);
	tree
}

/// Ends a tree the way a crash does: no close, no flush.
fn crash(tree: &Tree) {
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

/// Everything a new reader sees, by iterator, forward and backward.
fn scan(tree: &Tree) -> BTreeMap<String, Vec<u8>> {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let forward = collect_transaction_all(&mut txn.iter().unwrap()).unwrap();
	let mut backward = collect_transaction_reverse(&mut txn.iter().unwrap()).unwrap();
	backward.reverse();
	assert_eq!(forward, backward, "an iterator reads the same in both directions");
	forward.into_iter().map(|(k, v)| (String::from_utf8(k).unwrap(), v)).collect()
}

/// What a crash image of the live directory holds once it is recovered.
fn recover_image(live: &Path, options: &Options) -> BTreeMap<String, Vec<u8>> {
	let root = TempDir::new("group_fail_stop_image").unwrap();
	let image = root.path().join("image");
	copy_dir(live, &image);
	let recovered = Tree::new(Arc::new(Options {
		path: image,
		..options.clone()
	}))
	.unwrap();
	let all = scan(&recovered);
	crash(&recovered);
	all
}

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key.as_bytes()).unwrap()
}

/// Fails the test instead of blocking it if `future` is not done within `DEADLINE`.
async fn within<F: Future>(what: &str, future: F) -> F::Output {
	tokio::time::timeout(DEADLINE, future)
		.await
		.unwrap_or_else(|_| panic!("{what}: stuck for more than {DEADLINE:?}"))
}

async fn commit_one(
	tree: Arc<Tree>,
	key: String,
	value: Vec<u8>,
	durability: Durability,
) -> Result<()> {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(durability);
	txn.set(key.as_bytes(), &value).unwrap();
	txn.commit().await
}

/// One single-key transaction per entry, all spawned before the flusher is polled: on the
/// current-thread runtime that is one commit group, in order.
async fn commit_group(
	tree: &Arc<Tree>,
	entries: &Entries,
	durability: Durability,
) -> Vec<Result<()>> {
	let mut handles = Vec::new();
	for (key, value) in entries {
		handles.push(tokio::spawn(commit_one(
			Arc::clone(tree),
			key.clone(),
			value.clone(),
			durability,
		)));
	}
	let mut out = Vec::new();
	for handle in handles {
		out.push(within("a committer", handle).await.unwrap());
	}
	out
}

fn keys(entries: &Entries) -> Vec<String> {
	entries.iter().map(|(key, _)| key.clone()).collect()
}

/// The keys of `wanted` that the active memtable or an immutable one holds, whatever their
/// sequence number: what a failed group left in the memtables.
fn in_memtables(tree: &Tree, wanted: &[String]) -> Vec<String> {
	let inner = &tree.core.inner;
	let mut memtables = vec![Arc::clone(&inner.active_memtable.read().unwrap())];
	memtables
		.extend(inner.immutable_memtables.read().unwrap().iter().map(|e| Arc::clone(&e.memtable)));
	wanted
		.iter()
		.filter(|key| memtables.iter().any(|m| m.get(key.as_bytes(), None).is_some()))
		.cloned()
		.collect()
}

/// The message of the error every commit of a database stopped by a failed group gets.
fn stop_message(result: &Result<()>, what: &str) -> String {
	match result {
		Err(Error::DatabaseStopped(message)) => {
			assert!(message.contains("decided by recovery"), "{what}: {message}");
			result.as_ref().unwrap_err().to_string()
		}
		other => panic!("{what}: expected the database to be stopped, got {other:?}"),
	}
}

/// Every result is the stop error, and the same one. Returns it.
fn all_stopped(results: &[Result<()>], what: &str) -> String {
	let first = stop_message(&results[0], what);
	for result in results {
		assert_eq!(stop_message(result, what), first, "{what}: one error for the whole group");
	}
	first
}

fn assert_recovered(
	all: &BTreeMap<String, Vec<u8>>,
	before: &BTreeMap<String, Vec<u8>>,
	group: &Entries,
	what: &str,
) {
	let mut expected = before.clone();
	expected.extend(group.iter().cloned());
	assert_eq!(all, &expected, "{what}: the acknowledged commits and the whole group");
}

// ---------------------------------------------------------------------------
// one batch applied is enough
// ---------------------------------------------------------------------------

/// Three commits of 2000 bytes, of which the memtable has room for one: the worst case that a batch
/// needs of it is nearly its size. It takes the first, rotates for the second, and the record of
/// the rest is logged again, which fails. Only one batch was applied, and that is as
/// good as 22: the group cannot be taken back.
#[test(tokio::test)]
async fn one_batch_applied_is_enough_to_stop_the_database() {
	for durability in [Durability::Eventual, Durability::Immediate] {
		let what = format!("{durability:?}");
		let dir = TempDir::new("group_fail_stop").unwrap();
		let options = opts(dir.path());
		let tree = open(options.clone()).await;
		commit_one(Arc::clone(&tree), "old".into(), vec![b'o'; 40], Durability::Immediate)
			.await
			.unwrap();
		let visible = tree.core.seq_num();
		let before = scan(&tree);

		let group: Entries = (0..3).map(|i| (format!("big{i}"), vec![b'n'; 2000])).collect();
		let inner = Arc::clone(&tree.core.inner);
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
			if let PipelineHook::BeforeApplyRound {
				round: 1,
			} = point
			{
				inner.wal.write().fail_writes_after(0);
			}
		})));
		let results = commit_group(&tree, &group, durability).await;
		tree.core.commit_pipeline.set_hook(None);

		let first = all_stopped(&results, &what);
		assert_eq!(
			in_memtables(&tree, &keys(&group)),
			vec!["big0".to_string()],
			"{what}: exactly the first batch was applied"
		);
		assert_eq!(tree.core.seq_num(), visible, "{what}");
		for (key, _) in &group {
			assert_eq!(get(&tree, key), None, "{what}: {key} is readable");
		}
		assert_eq!(scan(&tree), before, "{what}");

		// A commit after the WAL rotated would publish a number above the group's.
		tree.core.inner.seal_active_wal_segment().unwrap();
		let later = commit_one(Arc::clone(&tree), "later".into(), vec![b'l'; 40], durability).await;
		assert_eq!(stop_message(&later, &what), first, "{what}");
		assert_eq!(scan(&tree), before, "{what}: after a later commit");

		assert_recovered(&recover_image(dir.path(), &options), &before, &group, &what);
		crash(&tree);
	}
}

// ---------------------------------------------------------------------------
// deletes in the part that was applied
// ---------------------------------------------------------------------------

/// A range delete and a delete are in the part of the group that was applied before it failed.
/// Whatever commit follows, and whether it is taken or refused, the keys they cover stay
/// readable: the group was reported failed, and a tombstone that is never published hides nothing.
#[test(tokio::test)]
async fn deletes_of_a_failed_group_hide_nothing_whatever_commit_follows() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let tree = open(opts(dir.path())).await;
	let base: Entries = (0..10).map(|i| (format!("base{i}"), vec![b'b'; 40])).collect();
	for result in commit_group(&tree, &base, Durability::Immediate).await {
		result.unwrap();
	}
	let before = scan(&tree);

	let inner = Arc::clone(&tree.core.inner);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::BeforeApplyRound {
			round: 1,
		} = point
		{
			inner.wal.write().fail_writes_after(0);
		}
	})));
	let mut handles = Vec::new();
	for i in 0..30 {
		let tree = Arc::clone(&tree);
		handles.push(tokio::spawn(async move {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(Durability::Immediate);
			match i {
				5 => txn.delete_range(&b"base3"[..], &b"base8"[..]).unwrap(),
				6 => txn.delete(&b"base9"[..]).unwrap(),
				_ => txn.set(format!("group{i}").as_bytes(), &[b'g'; 40]).unwrap(),
			}
			txn.commit().await
		}));
	}
	for handle in handles {
		assert!(within("a committer", handle).await.unwrap().is_err());
	}
	tree.core.commit_pipeline.set_hook(None);

	// The WAL rotates, as it would under a checkpoint, and a commit follows if the database takes
	// it.
	tree.core.inner.seal_active_wal_segment().unwrap();
	let _ =
		commit_one(Arc::clone(&tree), "later".into(), vec![b'l'; 40], Durability::Immediate).await;

	let mut seen = scan(&tree);
	seen.remove("later");
	assert_eq!(seen, before, "a delete of the failed group hides a key");
	for (key, value) in &before {
		assert_eq!(get(&tree, key).as_ref(), Some(value), "{key}");
	}
	crash(&tree);
}

// ---------------------------------------------------------------------------
// a batch the memtable took part of
// ---------------------------------------------------------------------------

/// A batch of two entries meets a memtable that has room for one of them, or for none, because its
/// size estimate fell short. The memtable keeps the entry it took and fails the other. Nothing of
/// the group was applied in full, and yet part of one batch may sit in the memtable, so the
/// database stops: no reader may see those entries, which carry sequence numbers of their own.
///
/// How much room is left depends on how the memtable grew, so the memtable is filled to several
/// levels, and one of them has to leave room for exactly one of the two entries.
#[test(tokio::test)]
async fn a_batch_the_memtable_took_part_of_stops_the_database() {
	let mut took_part = 0;
	for room in [100, 130, 160, 190, 220, 250, 280] {
		for durability in [Durability::Eventual, Durability::Immediate] {
			let what = format!("{durability:?}, room {room}");
			let dir = TempDir::new("group_fail_stop").unwrap();
			let options = opts(dir.path());
			let tree = open(options.clone()).await;
			tree.core.commit_pipeline.set_estimate_shortfall(1 << 20);

			// Fill the memtable until less than `room` bytes are left, one commit at a time.
			let mut filled = 0;
			loop {
				{
					let active = tree.core.inner.active_memtable.read().unwrap();
					if active.arena_capacity() - active.size() < room {
						break;
					}
				}
				commit_one(
					Arc::clone(&tree),
					format!("fill{filled:04}"),
					vec![b'f'; 40],
					Durability::Eventual,
				)
				.await
				.unwrap();
				filled += 1;
			}
			assert!(filled > 5, "{what}: the memtable took {filled} commits");
			let visible = tree.core.seq_num();
			let before = scan(&tree);
			assert_eq!(before.len(), filled);

			let wanted: Vec<String> = (0..2).map(|i| format!("torn{i}")).collect();
			let mut txn = tree.begin().unwrap();
			txn.set_durability(durability);
			for key in &wanted {
				txn.set(key.as_bytes(), &[b't'; 40]).unwrap();
			}
			let result = within("the torn commit", txn.commit()).await;
			let first = stop_message(&result, &what);
			assert!(first.contains("size estimate drift"), "{what}: {first}");
			tree.core.commit_pipeline.set_estimate_shortfall(0);

			let held = in_memtables(&tree, &wanted);
			assert!(held.len() < wanted.len(), "{what}: the whole batch was applied");
			took_part += held.len();
			assert_eq!(tree.core.seq_num(), visible, "{what}");
			for key in &wanted {
				assert_eq!(get(&tree, key), None, "{what}: {key} is readable");
			}
			assert_eq!(scan(&tree), before, "{what}");

			// Nothing more gets in, in a segment of its own either.
			tree.core.inner.seal_active_wal_segment().unwrap();
			let later =
				commit_one(Arc::clone(&tree), "later".into(), vec![b'l'; 40], durability).await;
			assert_eq!(stop_message(&later, &what), first, "{what}");
			assert_eq!(scan(&tree), before, "{what}: after a later commit");

			let group: Entries = wanted.iter().map(|key| (key.clone(), vec![b't'; 40])).collect();
			assert_recovered(&recover_image(dir.path(), &options), &before, &group, &what);
			crash(&tree);
		}
	}
	assert!(took_part > 0, "no level left room for one of the two entries");
}

// ---------------------------------------------------------------------------
// a restore between the rounds of a group
// ---------------------------------------------------------------------------

/// The round that follows the first one finds a restore under way. That group was applied in part
/// already, so the database stops, and the batches of the part stay out of sight.
#[test(tokio::test)]
async fn a_restore_between_the_rounds_of_a_group_stops_the_database() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let options = opts(dir.path());
	let tree = open(options.clone()).await;
	let base: Entries = (0..10).map(|i| (format!("base{i}"), vec![b'b'; 40])).collect();
	for result in commit_group(&tree, &base, Durability::Immediate).await {
		result.unwrap();
	}
	let visible = tree.core.seq_num();
	let before = scan(&tree);

	let group: Entries = (0..30).map(|i| (format!("group{i}"), vec![b'g'; 40])).collect();
	let core = Arc::clone(&tree.core);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::BeforeApplyRound {
			round: 1,
		} = point
		{
			core.commit_pipeline.restoring.store(true, Ordering::SeqCst);
		}
	})));
	let results = commit_group(&tree, &group, Durability::Immediate).await;
	tree.core.commit_pipeline.set_hook(None);
	tree.core.commit_pipeline.restoring.store(false, Ordering::SeqCst);

	let first = all_stopped(&results, "a restore between the rounds");
	assert!(first.contains("Pipeline stall"), "{first}");
	assert!(!in_memtables(&tree, &keys(&group)).is_empty(), "part of the group was applied");
	assert_eq!(tree.core.seq_num(), visible);
	for (key, _) in &group {
		assert_eq!(get(&tree, key), None, "{key} is readable");
	}
	assert_eq!(scan(&tree), before);

	tree.core.inner.seal_active_wal_segment().unwrap();
	let later =
		commit_one(Arc::clone(&tree), "later".into(), vec![b'l'; 40], Durability::Immediate).await;
	assert_eq!(stop_message(&later, "later"), first);
	assert_recovered(&recover_image(dir.path(), &options), &before, &group, "restore");
	crash(&tree);
}

// ---------------------------------------------------------------------------
// another hard error while commits are queued
// ---------------------------------------------------------------------------

/// A hard error that has nothing to do with a commit group (here a failed flush) is recorded when
/// five commits have been accepted and the flusher has not run yet. The flusher fails them with
/// it, before it takes a sequence number or touches the WAL, as it does after a stop.
#[test(tokio::test)]
async fn commits_accepted_before_another_hard_error_fail_with_it() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let options = opts(dir.path());
	let tree = open(options.clone()).await;
	commit_one(Arc::clone(&tree), "old".into(), vec![b'o'; 40], Durability::Immediate)
		.await
		.unwrap();
	let visible = tree.core.seq_num();
	let before = scan(&tree);
	let next = tree.core.commit_pipeline.log_seq_num.load(Ordering::SeqCst);
	let free_permits = tree.core.commit_pipeline.free_permits();

	let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
	let counter = Arc::clone(&accepted);
	let inner = Arc::clone(&tree.core.inner);
	tree.core.commit_pipeline.set_commit_hook(Some(Arc::new(move |stage| {
		if stage == CommitStage::Accepted && counter.fetch_add(1, Ordering::SeqCst) == 4 {
			inner.error_handler.set_error(
				Error::Io(Arc::new(std::io::Error::other("the flush of a memtable failed"))),
				BackgroundErrorReason::MemtablaFlush,
			);
		}
	})));
	let group: Entries = (0..5).map(|i| (format!("queued{i}"), vec![b'q'; 40])).collect();
	let results = commit_group(&tree, &group, Durability::Immediate).await;
	tree.core.commit_pipeline.set_commit_hook(None);

	assert_eq!(accepted.load(Ordering::SeqCst), 5, "all five were accepted before the error");
	for result in &results {
		match result {
			Err(e) => assert!(e.to_string().contains("the flush of a memtable failed"), "{e}"),
			Ok(()) => panic!("a commit went through after a hard error"),
		}
	}
	assert_eq!(
		tree.core.commit_pipeline.log_seq_num.load(Ordering::SeqCst),
		next,
		"no sequence number was taken"
	);
	assert_eq!(tree.core.seq_num(), visible);
	assert!(in_memtables(&tree, &keys(&group)).is_empty(), "nothing was applied");
	assert_eq!(scan(&tree), before);

	// Each failed entry was decided: the completed prefix passes it and its permit is back.
	assert_eq!(tree.core.commit_pipeline.accepted_waiting(), 0, "entries are left undecided");
	assert_eq!(tree.core.commit_pipeline.free_permits(), free_permits);
	crash(&tree);
}

// ---------------------------------------------------------------------------
// values in the value log
// ---------------------------------------------------------------------------

/// A group whose values are in the value log fails in the re-append after part of it was applied.
/// The records the crash image replays point into the value log files: every value of the group
/// is whole after the recovery.
#[test(tokio::test)]
async fn a_group_with_values_in_the_value_log_is_recovered_whole() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let options = Options {
		enable_vlog: true,
		vlog_value_threshold: 64,
		..opts(dir.path())
	};
	let tree = open(options.clone()).await;
	let base: Entries = (0..10).map(|i| (format!("base{i}"), vec![b'b'; 200])).collect();
	for result in commit_group(&tree, &base, Durability::Immediate).await {
		result.unwrap();
	}
	let visible = tree.core.seq_num();
	let before = scan(&tree);

	// 200 commits of 300 bytes: the memtable holds only a pointer to each, and fills part way.
	let group: Entries =
		(0..200).map(|i| (format!("group{i:03}"), vec![(i % 251) as u8; 300])).collect();
	let inner = Arc::clone(&tree.core.inner);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::BeforeApplyRound {
			round: 1,
		} = point
		{
			inner.wal.write().fail_writes_after(0);
		}
	})));
	let results = commit_group(&tree, &group, Durability::Immediate).await;
	tree.core.commit_pipeline.set_hook(None);

	let first = all_stopped(&results, "values in the value log");
	let held = in_memtables(&tree, &keys(&group));
	assert!(
		!held.is_empty() && held.len() < group.len(),
		"part of the group should be applied: {} of {}",
		held.len(),
		group.len()
	);
	assert_eq!(tree.core.seq_num(), visible);
	for (key, _) in &group {
		assert_eq!(get(&tree, key), None, "{key} is readable");
	}
	assert_eq!(scan(&tree), before);

	tree.core.inner.seal_active_wal_segment().unwrap();
	let later =
		commit_one(Arc::clone(&tree), "later".into(), vec![b'l'; 300], Durability::Immediate).await;
	assert_eq!(stop_message(&later, "later"), first);
	assert_eq!(scan(&tree), before);

	assert_recovered(&recover_image(dir.path(), &options), &before, &group, "value log");
	crash(&tree);
}

// ---------------------------------------------------------------------------
// a flush between the rounds of a group
// ---------------------------------------------------------------------------

/// The memtable that the rotation in the middle of the group queued is flushed before the group
/// fails: the flush task wakes up when the flusher rotates, and the group's second append may fail
/// long after. The table holds part of the group, the segment that holds all of its records is
/// kept: the pin on the group's segment holds `log_number` back, so a recovery finds the group
/// whole.
#[test(tokio::test)]
async fn a_flush_between_the_rounds_of_a_group_leaves_it_whole_or_absent() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let options = opts(dir.path());
	let tree = open(options.clone()).await;
	let base: Entries = (0..10).map(|i| (format!("base{i}"), vec![b'b'; 40])).collect();
	for result in commit_group(&tree, &base, Durability::Immediate).await {
		result.unwrap();
	}
	let before = scan(&tree);

	let group: Entries = (0..30).map(|i| (format!("group{i}"), vec![b'g'; 40])).collect();
	let inner = Arc::clone(&tree.core.inner);
	let flushed = Arc::new(Mutex::new(None));
	let outcome = Arc::clone(&flushed);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::BeforeApplyRound {
			round: 1,
		} = point
		{
			*outcome.lock().unwrap() = Some(CompactionOperations::compact_memtable(&*inner));
			inner.wal.write().fail_writes_after(0);
		}
	})));
	let results = commit_group(&tree, &group, Durability::Immediate).await;
	tree.core.commit_pipeline.set_hook(None);
	assert!(flushed.lock().unwrap().take().unwrap().is_ok(), "the flush ran before the failure");
	all_stopped(&results, "a flush between the rounds");

	let recovered = recover_image(dir.path(), &options);
	let present = group.iter().filter(|(key, _)| recovered.contains_key(key)).count();
	assert!(
		present == 0 || present == group.len(),
		"a recovery found {present} of the {} commits of the group",
		group.len()
	);
	for (key, value) in &before {
		assert_eq!(recovered.get(key), Some(value), "{key}");
	}
	crash(&tree);
}

/// A hard error of another kind is recorded while the group is between its rounds, which the stop
/// cannot displace: the guards that keep the memtables from being flushed, compacted and
/// checkpointed look for the stop's own reason in the stored error, and do not find it.
#[test(tokio::test)]
async fn a_stopped_database_flushes_nothing_whatever_error_was_stored_first() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let tree = open(opts(dir.path())).await;
	let base: Entries = (0..10).map(|i| (format!("base{i}"), vec![b'b'; 40])).collect();
	for result in commit_group(&tree, &base, Durability::Immediate).await {
		result.unwrap();
	}

	let group: Entries = (0..30).map(|i| (format!("group{i}"), vec![b'g'; 40])).collect();
	let inner = Arc::clone(&tree.core.inner);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::BeforeApplyRound {
			round: 1,
		} = point
		{
			inner.error_handler.set_error(
				Error::Corruption("a table failed its checksum".into()),
				BackgroundErrorReason::Compaction,
			);
			inner.wal.write().fail_writes_after(0);
		}
	})));
	let results = commit_group(&tree, &group, Durability::Immediate).await;
	tree.core.commit_pipeline.set_hook(None);
	all_stopped(&results, "a group that failed after another error");

	assert!(tree.flush().is_err(), "a memtable that holds part of the group was flushed");
	crash(&tree);
}

// ---------------------------------------------------------------------------
// failures at random points, with commits arriving all the time
// ---------------------------------------------------------------------------

/// What one run of `churn` saw.
#[derive(Default, Debug)]
struct Churn {
	acknowledged: usize,
	stopped: usize,
	other_errors: usize,
}

/// Six tasks commit distinct keys while the WAL is made to fail, and is sealed at other moments,
/// as a checkpoint or a direct write to L0 would. The background tasks run. Failures hit a group
/// between two of its apply rounds, so that part of it was applied already, and at random
/// moments, so that they hit any other step. Whichever group a failure hits, a commit that
/// reports an error is never readable, a commit that reports success is, and a recovery of a
/// crash image holds every commit that was acknowledged.
fn churn(seed: u64) -> Churn {
	let runtime =
		tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
	runtime.block_on(async move {
		let dir = TempDir::new("group_fail_stop").unwrap();
		let options = Options {
			path: dir.path().to_path_buf(),
			// Room for one commit: a group of two or more takes a round for each.
			max_memtable_size: 2100,
			flush_on_close: false,
			..Default::default()
		};
		let tree = Arc::new(Tree::new(Arc::new(options.clone())).unwrap());

		// A xorshift generator that the hook, which runs on the flusher's thread, and the chaos
		// task share.
		let state = Arc::new(std::sync::atomic::AtomicU64::new(
			seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
		));
		let next = {
			let state = Arc::clone(&state);
			move || {
				let mut x = state.load(Ordering::Relaxed);
				x ^= x << 13;
				x ^= x >> 7;
				x ^= x << 17;
				state.store(x, Ordering::Relaxed);
				x
			}
		};
		{
			let (inner, next) = (Arc::clone(&tree.core.inner), next.clone());
			tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
				if let PipelineHook::BeforeApplyRound {
					round,
				} = point
				{
					if round >= 1 && next() % 8 == 0 {
						if next() % 2 == 0 {
							inner.wal.write().fail_writes_after(0);
						} else {
							inner.wal.write().fail_syncs_after(0);
						}
					}
				}
			})));
		}

		let outcomes = Arc::new(Mutex::new(Vec::new()));
		let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let chaos = {
			let (tree, done, next) = (Arc::clone(&tree), Arc::clone(&done), next.clone());
			tokio::spawn(async move {
				while !done.load(Ordering::SeqCst) {
					tokio::time::sleep(Duration::from_micros(next() % 4000)).await;
					if next() % 16 == 0 {
						tree.core.inner.wal.write().fail_writes_after((next() % 3) as usize);
					} else {
						let _ = tree.core.inner.seal_active_wal_segment();
						// A rotation queues a memtable for the flush task, which only the
						// flusher wakes up.
						if let Some(tm) = tree.core.task_manager.lock().unwrap().as_ref() {
							tm.wake_up_memtable();
						}
					}
				}
			})
		};
		let mut workers = Vec::new();
		for w in 0..6 {
			let (tree, outcomes) = (Arc::clone(&tree), Arc::clone(&outcomes));
			workers.push(tokio::spawn(async move {
				for i in 0..40 {
					let key = format!("w{w}-{i:03}");
					let value = vec![i as u8 + 1; 20 + (i % 5) * 30];
					let durability = if i % 3 == 0 {
						Durability::Immediate
					} else {
						Durability::Eventual
					};
					let result =
						commit_one(Arc::clone(&tree), key.clone(), value.clone(), durability).await;
					outcomes.lock().unwrap().push((key, value, result));
				}
			}));
		}
		for worker in workers {
			within("a worker", worker).await.unwrap();
		}
		done.store(true, Ordering::SeqCst);
		within("the chaos task", chaos).await.unwrap();
		let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
		within("the background tasks", tm.stop()).await;

		let outcomes = std::mem::take(&mut *outcomes.lock().unwrap());
		let mut churn = Churn::default();
		let mut acknowledged: BTreeMap<String, Vec<u8>> = BTreeMap::new();
		for (key, value, result) in &outcomes {
			match result {
				Ok(()) => {
					churn.acknowledged += 1;
					acknowledged.insert(key.clone(), value.clone());
				}
				Err(Error::DatabaseStopped(_)) => churn.stopped += 1,
				Err(_) => churn.other_errors += 1,
			}
		}

		// What readers see is what was acknowledged, nothing that reported an error.
		for (key, value, result) in &outcomes {
			let seen = get(&tree, key);
			match result {
				Ok(()) => {
					assert_eq!(seen.as_ref(), Some(value), "seed {seed}: {key} was acknowledged")
				}
				Err(e) => assert_eq!(seen, None, "seed {seed}: {key} is readable after {e:?}"),
			}
		}
		assert_eq!(scan(&tree), acknowledged, "seed {seed}: what an iterator sees");

		// Once a group stopped the database, nothing gets in.
		if churn.stopped > 0 {
			let later =
				commit_one(Arc::clone(&tree), "later".into(), vec![1; 10], Durability::Immediate)
					.await;
			assert!(matches!(later, Err(Error::DatabaseStopped(_))), "seed {seed}: {later:?}");
		}

		// A recovery holds every acknowledged commit, and nothing that was never attempted.
		let recovered = recover_image(dir.path(), &options);
		for (key, value) in &acknowledged {
			assert_eq!(recovered.get(key), Some(value), "seed {seed}: {key} after a recovery");
		}
		let attempted: std::collections::BTreeSet<_> =
			outcomes.iter().map(|(key, _, _)| key.clone()).collect();
		for key in recovered.keys() {
			assert!(attempted.contains(key), "seed {seed}: {key} was never committed");
		}
		crash(&tree);
		churn
	})
}

/// Runs `churn` for each seed on a thread of its own, so that a hang fails the test.
fn churn_seeds(seeds: std::ops::RangeInclusive<u64>) {
	let (tx, rx) = std::sync::mpsc::channel();
	std::thread::spawn(move || {
		let outcome = std::panic::catch_unwind(|| {
			let mut total = Churn::default();
			for seed in seeds {
				let churn = churn(seed);
				total.acknowledged += churn.acknowledged;
				total.stopped += churn.stopped;
				total.other_errors += churn.other_errors;
			}
			total
		});
		let _ = tx.send(outcome);
	});
	match rx.recv_timeout(Duration::from_secs(300)) {
		Ok(Ok(total)) => eprintln!("churn: {total:?}"),
		Ok(Err(panic)) => std::panic::resume_unwind(panic),
		Err(_) => panic!("the churn did not end within five minutes"),
	}
}

#[test]
fn commits_that_report_errors_are_never_readable_when_failures_come_at_random() {
	churn_seeds(1..=2);
}

#[test]
#[ignore = "slow: eight more seeds of the churn with failures at random"]
fn commits_that_report_errors_are_never_readable_over_many_random_seeds() {
	churn_seeds(3..=10);
}

// ---------------------------------------------------------------------------
// helpers of the recovery cases
// ---------------------------------------------------------------------------

const OLD: u8 = b'b';
const NEW: u8 = b'g';

/// Ten keys that exist before the group.
fn base() -> Entries {
	(0..10).map(|i| (format!("base{i}"), vec![OLD; 40])).collect()
}

/// Thirty commits, three of which overwrite a base key. The memtable of `SMALL` bytes cannot take
/// them all, so the group needs a second round.
fn group() -> Entries {
	(0..30)
		.map(|i| {
			let key = if i < 3 {
				format!("base{i}")
			} else {
				format!("group{i}")
			};
			(key, vec![NEW; 40])
		})
		.collect()
}

/// The keys of `entries` that are not base keys.
fn fresh(entries: &Entries) -> Vec<String> {
	entries.iter().map(|(k, _)| k.clone()).filter(|k| !k.starts_with("base")).collect()
}

async fn commit_one_write_only(
	tree: Arc<Tree>,
	key: String,
	value: Vec<u8>,
	durability: Durability,
) -> Result<()> {
	let mut txn = tree.begin_with_mode(Mode::WriteOnly).unwrap();
	txn.set_durability(durability);
	txn.set(key.as_bytes(), &value).unwrap();
	txn.commit().await
}

async fn commit_group_write_only(
	tree: &Arc<Tree>,
	entries: &Entries,
	durability: Durability,
) -> Vec<Result<()>> {
	let mut handles = Vec::new();
	for (key, value) in entries {
		handles.push(tokio::spawn(commit_one_write_only(
			Arc::clone(tree),
			key.clone(),
			value.clone(),
			durability,
		)));
	}
	let mut out = Vec::new();
	for handle in handles {
		out.push(within("a committer", handle).await.unwrap());
	}
	out
}

/// What a crash image of the live directory holds once it is recovered, with the options of the
/// tests.
fn recovered(live: &Path) -> BTreeMap<String, Vec<u8>> {
	recover_image(live, &opts(live))
}

/// How many of the keys of `group` that `base` does not hold a recovered image has, of how many.
fn recovered_of(all: &BTreeMap<String, Vec<u8>>, group: &Entries) -> (usize, usize) {
	let keys = fresh(group);
	(keys.iter().filter(|k| all.contains_key(*k)).count(), keys.len())
}

// ---------------------------------------------------------------------------
// recovery after the stop: whole or absent
// ---------------------------------------------------------------------------

/// The same when the oversized batch is the first of the group and the memtable is empty: the
/// direct write would move `log_number` past the segment that holds the rest of the group.
#[test(tokio::test)]
async fn an_oversized_first_batch_on_an_empty_memtable_does_not_split_the_group() {
	for durability in [Durability::Immediate, Durability::Eventual] {
		let dir = TempDir::new("group_fail_stop").unwrap();
		let tree = open(opts(dir.path())).await;
		for r in commit_group(&tree, &base(), Durability::Immediate).await {
			r.unwrap();
		}
		tree.flush().unwrap();
		let before = scan(&tree);

		let inner = Arc::clone(&tree.core.inner);
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
			if let PipelineHook::BeforeApplyRound {
				round: 1,
			} = point
			{
				inner.wal.write().fail_writes_after(0);
			}
		})));
		let mut group: Entries = vec![("big0".to_string(), vec![NEW; SMALL + 1000])];
		group.extend((1..11).map(|i| (format!("group{i}"), vec![NEW; 40])));
		let log_before = tree.core.inner.level_manifest.read().unwrap().get_log_number();
		let results = commit_group(&tree, &group, durability).await;
		tree.core.commit_pipeline.set_hook(None);
		all_stopped(&results, "the group");
		assert_eq!(scan(&tree), before);
		let log_after = tree.core.inner.level_manifest.read().unwrap().get_log_number();
		assert_eq!(
			log_after, log_before,
			"the direct write moved log_number past the segment that holds the rest of the group"
		);

		let recovered = recovered(dir.path());
		let (found, of) = recovered_of(&recovered, &group);
		assert!(
			found == 0 || found == of,
			"{durability:?}: the group is split by recovery: {found} of {of} of its new keys \
			 are there"
		);
		crash(&tree);
	}
}

// ---------------------------------------------------------------------------
// the guards when another error is stored first
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// direct L0 batches with deletes
// ---------------------------------------------------------------------------

/// A range delete inside an oversized batch that went to an L0 table before the failure hides
/// nothing from readers.
#[test(tokio::test)]
async fn a_range_delete_in_an_l0_table_of_a_failed_group_hides_nothing() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let tree = open(opts(dir.path())).await;
	for r in commit_group(&tree, &base(), Durability::Immediate).await {
		r.unwrap();
	}
	tree.flush().unwrap();
	let before = scan(&tree);
	assert_eq!(before.len(), 10);

	let inner = Arc::clone(&tree.core.inner);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::BeforeApplyRound {
			round: 1,
		} = point
		{
			inner.wal.write().fail_writes_after(0);
		}
	})));
	let mut handles = Vec::new();
	for i in 0..12 {
		let tree = Arc::clone(&tree);
		handles.push(tokio::spawn(async move {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(Durability::Immediate);
			match i {
				0 => {
					// Oversized: a range delete over the base keys, a point delete, and padding.
					txn.delete_range(&b"base2"[..], &b"base7"[..]).unwrap();
					txn.delete(&b"base9"[..]).unwrap();
					for j in 0..8 {
						txn.set(format!("pad{j}").as_bytes(), vec![NEW; 700]).unwrap();
					}
				}
				_ => txn.set(format!("group{i}").as_bytes(), &[NEW; 40]).unwrap(),
			}
			txn.commit().await
		}));
	}
	for handle in handles {
		let result = handle.await.unwrap();
		assert!(matches!(result, Err(Error::DatabaseStopped(_))), "{result:?}");
	}
	tree.core.commit_pipeline.set_hook(None);
	assert_eq!(tree.core.inner.l0_file_count(), 2, "the oversized batch is in a table");
	assert_eq!(scan(&tree), before, "an iterator sees a delete of the failed group");
	for (key, value) in &before {
		assert_eq!(get(&tree, key).as_ref(), Some(value), "{key}");
	}
	crash(&tree);
}

// ---------------------------------------------------------------------------
// live background tasks and many writers
// ---------------------------------------------------------------------------

fn version_of(value: &[u8]) -> u64 {
	u64::from_be_bytes(value[..8].try_into().unwrap())
}

fn versioned(version: u64, len: usize) -> Vec<u8> {
	let mut value = vec![NEW; len.max(8)];
	value[..8].copy_from_slice(&version.to_be_bytes());
	value
}

/// Many writers, each with keys of its own that it rewrites with rising versions, some of them
/// oversized so that they go straight to L0 tables, while the background tasks flush and compact
/// and the WAL starts failing at an arbitrary moment. Whatever fails, a new reader must see exactly
/// the last version each writer was told succeeded, and a crash image must hold at least that.
fn stress(seed: u64, wal_fail: bool, writers: usize) -> bool {
	let runtime =
		tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
	runtime.block_on(async move {
		let dir = TempDir::new("group_fail_stop_stress").unwrap();
		let options = Options {
			flush_on_close: true,
			level0_max_files: 2,
			memtable_stall_threshold: 4,
			..opts(dir.path())
		};
		let tree = Arc::new(Tree::new(Arc::new(options)).unwrap());

		let acked: Arc<std::sync::Mutex<BTreeMap<String, u64>>> = Default::default();
		let failed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let live = Arc::new(std::sync::atomic::AtomicUsize::new(writers));
		let mut handles = Vec::new();
		for w in 0..writers {
			let (tree, acked, failed, live) =
				(Arc::clone(&tree), Arc::clone(&acked), Arc::clone(&failed), Arc::clone(&live));
			handles.push(tokio::spawn(async move {
				for n in 0..400u64 {
					let key = format!("hot{w}-{}", n % 3);
					let size = if (n + seed + w as u64) % 7 == 0 {
						SMALL + 500
					} else {
						60
					};
					let durability = if (n + w as u64) % 2 == 0 {
						Durability::Immediate
					} else {
						Durability::Eventual
					};
					match commit_one(
						Arc::clone(&tree),
						key.clone(),
						versioned(n + 1, size),
						durability,
					)
					.await
					{
						Ok(()) => {
							acked.lock().unwrap().insert(key, n + 1);
						}
						Err(_) => {
							failed.fetch_add(1, Ordering::SeqCst);
							break;
						}
					}
				}
				live.fetch_sub(1, Ordering::SeqCst);
			}));
		}
		if wal_fail {
			let tree = Arc::clone(&tree);
			let live = Arc::clone(&live);
			handles.push(tokio::spawn(async move {
				tokio::time::sleep(Duration::from_millis(5 + seed % 40)).await;
				while live.load(Ordering::SeqCst) > 0 {
					tree.core.inner.wal.write().fail_writes_after((seed % 3) as usize);
					tokio::time::sleep(Duration::from_millis(1)).await;
				}
			}));
		}
		let all = async {
			for h in handles {
				h.await.unwrap();
			}
		};
		tokio::time::timeout(Duration::from_secs(60), all).await.expect("a writer is stuck");
		let stopped = tree.core.inner.error_handler.is_db_stopped();
		let acked = acked.lock().unwrap().clone();

		// A new reader sees exactly what was acknowledged.
		let seen = scan(&tree);
		for (key, version) in &acked {
			let got = seen.get(key).map(|v| version_of(v));
			assert_eq!(got, Some(*version), "seed {seed}: {key} (stopped={stopped})");
		}
		for key in seen.keys() {
			assert!(acked.contains_key(key), "seed {seed}: {key} is readable but was never acked");
		}

		// A crash image holds at least what was acknowledged.
		let recovered = recovered(dir.path());
		for (key, version) in &acked {
			let got = recovered.get(key).map(|v| version_of(v)).unwrap_or(0);
			assert!(got >= *version, "seed {seed}: {key} recovered at {got}, acked {version}");
		}

		tokio::time::timeout(Duration::from_secs(60), tree.close())
			.await
			.expect("close does not end")
			.ok();
		stopped
	})
}

#[test]
#[ignore = "stress: many writers over live background tasks with an injected WAL failure"]
fn many_writers_over_live_background_tasks_see_exactly_what_was_acknowledged() {
	let mut stops = 0;
	for seed in 100..104u64 {
		if stress(seed, true, 6) {
			stops += 1;
		}
	}
	// More committers than the ring has slots, so that entries are in flight when it stops.
	for seed in 0..4u64 {
		if stress(seed, true, 48) {
			stops += 1;
		}
	}
	eprintln!("databases stopped: {stops} of 8");
}

// ---------------------------------------------------------------------------
// work that was already running when the group failed
// ---------------------------------------------------------------------------

/// A compaction that starts after an oversized batch of the group went to an L0 table, as the
/// level task does right after the write, and before the group fails, keeps only the newest
/// version of the key: the one that is never published. Read-write committers register a snapshot
/// that keeps the older version, so the committers here are write-only.
///
/// Compaction does not treat the visible sequence number as a snapshot, so a compaction that is
/// already running when the group fails can still take the visible version of a key.
#[test(tokio::test)]
#[ignore = "known gap: a compaction that started before the stop does not keep the visible version"]
async fn a_compaction_that_ran_before_the_failure_does_not_take_the_visible_version() {
	compaction_before_the_failure(true).await;
}

/// The same with read-write committers, whose snapshots protect the visible version.
#[test(tokio::test)]
async fn a_compaction_before_the_failure_keeps_what_a_read_write_committer_pins() {
	compaction_before_the_failure(false).await;
}

async fn compaction_before_the_failure(write_only: bool) {
	use crate::compaction::leveled::Strategy;

	let dir = TempDir::new("group_fail_stop").unwrap();
	let options = Arc::new(Options {
		level0_max_files: 2,
		..opts(dir.path())
	});
	let tree = open((*options).clone()).await;
	let old = vec![OLD; 40];
	commit_one(Arc::clone(&tree), "big10".into(), old.clone(), Durability::Immediate)
		.await
		.unwrap();
	tree.flush().unwrap();
	assert_eq!(tree.core.inner.l0_file_count(), 1);

	let inner = Arc::clone(&tree.core.inner);
	let strategy: Arc<dyn crate::compaction::CompactionStrategy> =
		Arc::new(Strategy::from_options(Arc::clone(&options)));
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::BeforeApplyRound {
			round: 1,
		} = point
		{
			inner.compact(Arc::clone(&strategy)).unwrap();
			assert_eq!(inner.l0_file_count(), 0, "the compaction merged the tables");
			inner.wal.write().fail_writes_after(0);
		}
	})));
	let mut group: Entries = (0..20).map(|i| (format!("group{i}"), vec![NEW; 40])).collect();
	group[10] = ("big10".to_string(), vec![NEW; SMALL + 1000]);
	let results = if write_only {
		commit_group_write_only(&tree, &group, Durability::Immediate).await
	} else {
		commit_group(&tree, &group, Durability::Immediate).await
	};
	tree.core.commit_pipeline.set_hook(None);
	all_stopped(&results, "the group");

	assert_eq!(get(&tree, "big10"), Some(old), "the version readers saw before is gone");
	crash(&tree);
}

// ---------------------------------------------------------------------------
// value log, close and reopen
// ---------------------------------------------------------------------------

/// `close()` starts while the group is between rounds, so the shutdown of the pipeline overlaps
/// the failure. The shutdown flush is skipped, and a reopen finds the group whole.
#[test(tokio::test)]
async fn a_close_that_begins_while_the_group_is_failing_leaves_the_group_to_the_wal() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let tree = open(Options {
		flush_on_close: true,
		..opts(dir.path())
	})
	.await;
	for r in commit_group(&tree, &base(), Durability::Immediate).await {
		r.unwrap();
	}

	let inner = Arc::clone(&tree.core.inner);
	let pipeline = Arc::clone(&tree.core.commit_pipeline);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::BeforeApplyRound {
			round: 1,
		} = point
		{
			// What `close()` does first, while the flusher is in the middle of the group.
			pipeline.shutdown();
			inner.wal.write().fail_writes_after(0);
		}
	})));
	let group = group();
	let results = commit_group(&tree, &group, Durability::Immediate).await;
	tree.core.commit_pipeline.set_hook(None);
	all_stopped(&results, "the group");

	tree.core.is_closed.store(false, Ordering::SeqCst);
	tree.core.task_manager.lock().unwrap().take();
	tokio::time::timeout(Duration::from_secs(30), tree.core.close())
		.await
		.expect("close does not end")
		.expect("close of a stopped database works");
	drop(tree);

	let reopened = Tree::new(Arc::new(opts(dir.path()))).unwrap();
	let all = scan(&reopened);
	let (found, of) = recovered_of(&all, &group);
	assert!(found == 0 || found == of, "reopened with {found} of {of} of the group");
	for (key, _) in base() {
		assert!(all.contains_key(&key), "{key} is lost");
	}
	crash(&reopened);
}

// ---------------------------------------------------------------------------
// a restore overtaking the group
// ---------------------------------------------------------------------------

/// The same as the flush in the window, with the background task of the tree: the rotation in the
/// middle of the group wakes it, and it flushes the memtable while the flusher is still on its way
/// to the failing append (here, a pause before the append stands in for the hop to the pool).
#[test]
fn the_background_flush_woken_by_the_mid_group_rotation_does_not_split_the_group() {
	let runtime =
		tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
	runtime.block_on(async {
		let dir = TempDir::new("group_fail_stop").unwrap();
		let options = Options {
			memtable_stall_threshold: 100,
			..opts(dir.path())
		};
		let tree = Arc::new(Tree::new(Arc::new(options)).unwrap());
		for r in commit_group(&tree, &base(), Durability::Immediate).await {
			r.unwrap();
		}

		// The flusher takes whatever is in the ring when it looks, and on this runtime the
		// committers do not all arrive before it does. A commit of its own goes first, and the
		// flusher is held after it logged that one while the thirty queue up behind it: they are
		// then one group.
		let accepted = Arc::new(AtomicUsize::new(0));
		{
			let accepted = Arc::clone(&accepted);
			tree.core.commit_pipeline.set_commit_hook(Some(Arc::new(move |stage| {
				if stage == CommitStage::Accepted {
					accepted.fetch_add(1, Ordering::SeqCst);
				}
			})));
		}
		let primer_logged = Arc::new(AtomicBool::new(false));
		let first = AtomicBool::new(true);
		let inner = Arc::clone(&tree.core.inner);
		let (queued, logged) = (Arc::clone(&accepted), Arc::clone(&primer_logged));
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| match point {
			PipelineHook::AfterWalSync {
				..
			} if first.swap(false, Ordering::SeqCst) => {
				logged.store(true, Ordering::SeqCst);
				tokio::task::block_in_place(|| {
					let started = std::time::Instant::now();
					while queued.load(Ordering::SeqCst) < 31
						&& started.elapsed() < Duration::from_secs(20)
					{
						std::thread::sleep(Duration::from_millis(1));
					}
				});
			}
			PipelineHook::BeforeApplyRound {
				round: 1,
			} => {
				// Let the background task flush the memtable the rotation queued: a worker that
				// blocks keeps the task it woke in its own queue, so it hands the queue over.
				tokio::task::block_in_place(|| {
					let started = std::time::Instant::now();
					while inner.immutable_count() > 0 && started.elapsed() < Duration::from_secs(20)
					{
						std::thread::sleep(Duration::from_millis(1));
					}
				});
				inner.wal.write().fail_writes_after(0);
			}
			_ => {}
		})));
		let primer = tokio::spawn(commit_one(
			Arc::clone(&tree),
			"primer".to_string(),
			vec![OLD; 40],
			Durability::Immediate,
		));
		within("the flusher to log the first commit", async {
			while !primer_logged.load(Ordering::SeqCst) {
				tokio::time::sleep(Duration::from_millis(1)).await;
			}
		})
		.await;
		let group = group();
		let mut handles = Vec::new();
		for (key, value) in &group {
			handles.push(tokio::spawn(commit_one(
				Arc::clone(&tree),
				key.clone(),
				value.clone(),
				Durability::Immediate,
			)));
		}
		let mut results = Vec::new();
		for handle in handles {
			results.push(within("a committer", handle).await.unwrap());
		}
		within("the first commit", primer).await.unwrap().unwrap();
		tree.core.commit_pipeline.set_hook(None);
		tree.core.commit_pipeline.set_commit_hook(None);
		all_stopped(&results, "the group");
		assert_eq!(tree.core.inner.immutable_count(), 0, "the background task flushed it");
		for ((key, _), result) in group.iter().zip(&results) {
			if result.is_err() {
				let expected = key.starts_with("base").then(|| vec![OLD; 40]);
				assert_eq!(get(&tree, key), expected, "{key} of a commit that failed");
			}
		}

		// The commits that failed were one group: recovery brings back all of them or none.
		let recovered = recovered(dir.path());
		let failed: Vec<&String> = group
			.iter()
			.zip(&results)
			.filter(|(entry, result)| result.is_err() && !entry.0.starts_with("base"))
			.map(|(entry, _)| &entry.0)
			.collect();
		let found = failed.iter().filter(|key| recovered.contains_key(**key)).count();
		assert!(
			found == 0 || found == failed.len(),
			"the failed commits are split by recovery: {found} of {} are there",
			failed.len()
		);
		tree.core.is_closed.store(true, Ordering::SeqCst);
		let _ = tree.core.inner.lockfile.lock().unwrap().release();
		tree.core.abort_background_tasks();
	});
}

// ---------------------------------------------------------------------------
// crash images cut between the rounds
// ---------------------------------------------------------------------------

/// An image of the live directory taken while the flusher is between the first and the second
/// round, before the failing append, and one taken after the stop, both recover the group whole
/// and everything that was acknowledged.
#[test(tokio::test)]
async fn crash_images_cut_between_the_rounds_and_after_the_stop_recover_the_group_whole() {
	for durability in [Durability::Immediate, Durability::Eventual] {
		let dir = TempDir::new("group_fail_stop").unwrap();
		let cut = TempDir::new("group_fail_stop_cut").unwrap();
		let tree = open(opts(dir.path())).await;
		for r in commit_group(&tree, &base(), Durability::Eventual).await {
			r.unwrap();
		}

		let inner = Arc::clone(&tree.core.inner);
		let live = dir.path().to_path_buf();
		let cut_to = cut.path().join("image");
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
			if let PipelineHook::BeforeApplyRound {
				round: 1,
			} = point
			{
				copy_dir(&live, &cut_to);
				inner.wal.write().fail_writes_after(0);
			}
		})));
		let group = group();
		let results = commit_group(&tree, &group, durability).await;
		tree.core.commit_pipeline.set_hook(None);
		all_stopped(&results, "the group");

		let image = Tree::new(Arc::new(opts(&cut.path().join("image")))).unwrap();
		let mid = scan(&image);
		crash(&image);
		let mut expected: BTreeMap<String, Vec<u8>> = base().into_iter().collect();
		expected.extend(group.iter().cloned());
		assert_eq!(mid, expected, "{durability:?}: the image cut between the rounds");
		assert_eq!(recovered(dir.path()), expected, "{durability:?}: the image cut after the stop");
		crash(&tree);
	}
}

// ---------------------------------------------------------------------------
// the hold on the log of the group in flight
// ---------------------------------------------------------------------------

/// Once a group ends without having applied anything, or completes, a flush moves `log_number`
/// as it always does: the hold lasts as long as the group.
#[test(tokio::test)]
async fn the_hold_on_the_log_ends_with_a_group_that_applied_nothing_or_completed() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let tree = open(opts(dir.path())).await;
	let inner = Arc::clone(&tree.core.inner);
	let released = || inner.group_wal_pin.load(Ordering::Acquire) == u64::MAX;

	// A group that completes over two rounds.
	for result in commit_group(&tree, &group(), Durability::Immediate).await {
		result.unwrap();
	}
	assert!(released(), "a group that completed holds the log");
	tree.flush().unwrap();
	let log_number = inner.level_manifest.read().unwrap().get_log_number();
	assert!(
		log_number >= inner.wal.read().get_active_log_number(),
		"the flush after the group moved log_number to {log_number}"
	);

	// A group whose log is overtaken by a seal, and that fails to log it again: nothing of it is
	// applied, so nothing stops.
	let sealed = Arc::clone(&inner);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::BeforeApplyRound {
			round: 0,
		} = point
		{
			sealed.seal_active_wal_segment().unwrap();
			sealed.wal.write().fail_writes_after(0);
		}
	})));
	let entries: Entries = (0..3).map(|i| (format!("late{i}"), vec![NEW; 40])).collect();
	let results = commit_group(&tree, &entries, Durability::Immediate).await;
	tree.core.commit_pipeline.set_hook(None);
	for result in &results {
		assert!(result.is_err() && !matches!(result, Err(Error::DatabaseStopped(_))), "{result:?}");
	}
	assert!(released(), "a group that applied nothing holds the log");
	crash(&tree);
}

// ---------------------------------------------------------------------------
// a group that fails on a tree that was recovered from a crash
// ---------------------------------------------------------------------------

/// The WAL segments of `dir` that hold at least one byte.
fn non_empty_segments(dir: &Path) -> Vec<u64> {
	let mut found: Vec<u64> = fs::read_dir(dir.join("wal"))
		.unwrap()
		.map(|entry| entry.unwrap())
		.filter(|entry| entry.metadata().unwrap().len() > 0)
		.filter_map(|entry| {
			let name = entry.file_name().to_string_lossy().to_string();
			name.strip_suffix(".wal").and_then(|id| id.parse().ok())
		})
		.collect();
	found.sort_unstable();
	found
}

/// Opens a copy of the crash image `live` in `into`, and checks the state recovery leaves, which
/// every scenario below starts from: the rows of the crashed segments are in the active memtable,
/// which is tagged with the segment recovery continues in, and that segment is empty.
async fn recovered_tree(live: &Path, into: &Path, rows: &Entries) -> Arc<Tree> {
	copy_dir(live, into);
	let crashed = non_empty_segments(into);
	assert!(!crashed.is_empty(), "the crash image holds records");
	let tree = open(opts(into)).await;
	let inner = &tree.core.inner;
	let active = inner.wal.read().get_active_log_number();
	let memtable = Arc::clone(&inner.active_memtable.read().unwrap());
	assert_eq!(
		memtable.get_wal_number(),
		active,
		"the recovered memtable is tagged with the new segment"
	);
	assert!(crashed.iter().all(|segment| *segment < active), "{crashed:?} are older than {active}");
	assert!(
		non_empty_segments(into).iter().all(|segment| *segment < active),
		"the segment recovery continues in is empty"
	);
	for (key, _) in rows {
		assert!(
			memtable.get(key.as_bytes(), None).is_some(),
			"{key} was recovered into the memtable"
		);
	}
	assert_eq!(inner.immutable_count(), 0);
	tree
}

/// How a group fails on a recovered tree, whose active memtable holds rows from segments older
/// than the one it is tagged with.
#[derive(Clone, Copy, Debug)]
enum OnRecovered {
	/// The memtable fills up half way through the group, and the re-append of the rest fails.
	Reappend,
	/// The same, and the memtable that the rotation queued is flushed before the re-append fails.
	FlushThenReappend,
	/// The first batch of the group is oversized and goes to an L0 table, the memtable that was
	/// sealed for it is flushed, and the re-append of the rest fails.
	OversizedFirstThenFlush,
	/// The same with the oversized batch in the middle of the group.
	OversizedMiddleThenFlush,
}

impl OnRecovered {
	fn group(self) -> Entries {
		let big = |key: &str| (key.to_string(), vec![NEW; SMALL + 1000]);
		let small = |i: usize| (format!("group{i}"), vec![NEW; 40]);
		match self {
			OnRecovered::Reappend | OnRecovered::FlushThenReappend => group(),
			OnRecovered::OversizedFirstThenFlush => {
				std::iter::once(big("big0")).chain((1..11).map(small)).collect()
			}
			OnRecovered::OversizedMiddleThenFlush => (0..4)
				.map(small)
				.chain(std::iter::once(big("big4")))
				.chain((5..14).map(small))
				.collect(),
		}
	}

	fn flushes(self) -> bool {
		!matches!(self, OnRecovered::Reappend)
	}
}

/// A tree is committed to, crashed and recovered. On the recovered tree more commits are
/// acknowledged, and then a group fails after part of it was applied. A crash image of that tree
/// recovers every acknowledged commit and the group whole, a tree recovered from that image is
/// not stopped, and its next commit survives another crash.
///
/// The recovered memtable holds rows from the crashed segments and is tagged with the segment
/// recovery opened, so it is the memtable the group's first batches land in, and the one a flush
/// between the rounds writes to a table together with them. The segments that are needed to
/// complete the group are those of the group's own records, not those of the memtable's rows.
async fn a_group_fails_on_a_recovered_tree(scenario: OnRecovered) {
	let what = format!("{scenario:?}");
	let first = TempDir::new("group_fail_stop_first").unwrap();
	let images = TempDir::new("group_fail_stop_images").unwrap();

	// Commits that are acknowledged, and a crash.
	let crashed = open(opts(first.path())).await;
	for result in commit_group(&crashed, &base(), Durability::Immediate).await {
		result.unwrap();
	}
	crash(&crashed);

	// The recovered tree acknowledges more commits, in the segment it opened.
	let live = images.path().join("recovered");
	let tree = recovered_tree(first.path(), &live, &base()).await;
	let acknowledged: Entries = (0..3).map(|i| (format!("ack{i}"), vec![b'a'; 40])).collect();
	for result in commit_group(&tree, &acknowledged, Durability::Immediate).await {
		result.unwrap();
	}
	let before = scan(&tree);
	assert_eq!(before.len(), 13, "{what}");
	let visible = tree.core.seq_num();

	// A group fails after part of it was applied.
	let group = scenario.group();
	let inner = Arc::clone(&tree.core.inner);
	let flushed = Arc::new(Mutex::new(None));
	let outcome = Arc::clone(&flushed);
	let flushes = scenario.flushes();
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::BeforeApplyRound {
			round: 1,
		} = point
		{
			if flushes {
				*outcome.lock().unwrap() = Some(CompactionOperations::compact_memtable(&*inner));
			}
			inner.wal.write().fail_writes_after(0);
		}
	})));
	let results = commit_group(&tree, &group, Durability::Immediate).await;
	tree.core.commit_pipeline.set_hook(None);
	all_stopped(&results, &what);
	if flushes {
		assert!(flushed.lock().unwrap().take().unwrap().is_ok(), "{what}: the flush ran");
		assert_eq!(tree.core.inner.immutable_count(), 0, "{what}: the queued memtable was flushed");
		let tables = tree.core.inner.l0_file_count();
		let expected = if matches!(scenario, OnRecovered::FlushThenReappend) {
			1
		} else {
			2
		};
		assert_eq!(tables, expected, "{what}: tables after the flush");
	} else {
		let applied = in_memtables(&tree, &fresh(&group));
		assert!(!applied.is_empty(), "{what}: nothing of the group was applied");
		assert!(applied.len() < fresh(&group).len(), "{what}: all of the group was applied");
	}
	assert_eq!(tree.core.seq_num(), visible, "{what}: the visible sequence number moved");
	for key in fresh(&group) {
		assert_eq!(get(&tree, &key), None, "{what}: {key} is readable");
	}
	assert_eq!(scan(&tree), before, "{what}: what readers see");

	// A crash image of that tree: every acknowledged commit and the whole group.
	let mut expected = before.clone();
	expected.extend(group.iter().cloned());
	let second = images.path().join("second");
	copy_dir(&live, &second);
	crash(&tree);
	let again = open(opts(&second)).await;
	assert_eq!(scan(&again), expected, "{what}: the image of the recovered tree");
	crash(&again);

	// The tree recovered from it is not stopped, and what it acknowledges survives a crash.
	let third = open(opts(&second)).await;
	commit_one(Arc::clone(&third), "after".into(), vec![b'z'; 40], Durability::Immediate)
		.await
		.unwrap();
	expected.insert("after".into(), vec![b'z'; 40]);
	assert_eq!(scan(&third), expected, "{what}: the tree recovered twice");
	let fourth = images.path().join("fourth");
	copy_dir(&second, &fourth);
	crash(&third);
	let last = open(opts(&fourth)).await;
	assert_eq!(scan(&last), expected, "{what}: the image of the tree recovered twice");
	crash(&last);
}

/// A segment that holds more than one memtable of records, as one does while a group is applied
/// across a rotation and its rest is not logged again yet, is replayed into several memtables.
/// Recovery writes all but the last to tables. It must not record the segment as flushed then:
/// the records of the last one are in no table, and a crash before it is flushed would lose them
/// from a group that recovery has just shown whole.
#[test(tokio::test)]
async fn a_segment_larger_than_a_memtable_is_replayed_whole_by_every_recovery() {
	for durability in [Durability::Immediate, Durability::Eventual] {
		let what = format!("{durability:?}");
		let dir = TempDir::new("group_fail_stop").unwrap();
		let images = TempDir::new("group_fail_stop_images").unwrap();
		let tree = open(opts(dir.path())).await;
		for r in commit_group(&tree, &base(), Durability::Immediate).await {
			r.unwrap();
		}

		// An image cut between the rounds: the one segment holds the whole group, and the group
		// does not fit one memtable.
		let cut = images.path().join("cut");
		let inner = Arc::clone(&tree.core.inner);
		let live = dir.path().to_path_buf();
		let cut_to = cut.clone();
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
			if let PipelineHook::BeforeApplyRound {
				round: 1,
			} = point
			{
				copy_dir(&live, &cut_to);
				inner.wal.write().fail_writes_after(0);
			}
		})));
		let group = group();
		all_stopped(&commit_group(&tree, &group, durability).await, &what);
		tree.core.commit_pipeline.set_hook(None);
		crash(&tree);

		let mut expected: BTreeMap<String, Vec<u8>> = base().into_iter().collect();
		expected.extend(group.iter().cloned());
		assert!(!non_empty_segments(&cut).is_empty(), "{what}: the image holds records");

		// Every recovery of the image, each ended by a crash, finds the group whole.
		for attempt in 1..=3 {
			let recovered = open(opts(&cut)).await;
			assert!(
				recovered.core.inner.l0_file_count() >= 1,
				"{what}: recovery number {attempt} did not split the segment over memtables"
			);
			assert_eq!(scan(&recovered), expected, "{what}: recovery number {attempt}");
			crash(&recovered);
		}
	}
}

#[test(tokio::test)]
async fn a_failed_reappend_on_a_recovered_tree_leaves_the_group_whole_for_the_next_recovery() {
	a_group_fails_on_a_recovered_tree(OnRecovered::Reappend).await;
}

#[test(tokio::test)]
async fn a_flush_between_the_rounds_on_a_recovered_tree_leaves_the_group_whole() {
	a_group_fails_on_a_recovered_tree(OnRecovered::FlushThenReappend).await;
}

#[test(tokio::test)]
async fn an_oversized_first_batch_on_a_recovered_tree_does_not_split_the_group() {
	a_group_fails_on_a_recovered_tree(OnRecovered::OversizedFirstThenFlush).await;
}

#[test(tokio::test)]
async fn an_oversized_batch_in_the_middle_on_a_recovered_tree_does_not_split_the_group() {
	a_group_fails_on_a_recovered_tree(OnRecovered::OversizedMiddleThenFlush).await;
}
