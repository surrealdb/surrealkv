//! Acknowledged writes must survive an unclean stop across memtable rotations.
//!
//! The commit flusher appends a group to the WAL segment that is active at that
//! moment and then applies it to the active memtable. Every memtable is tagged
//! with the segment that was active when it was created, and flushing the
//! memtable tagged N moves the manifest's `log_number` to N + 1, after which
//! segments up to N are deleted.
//!
//! If a rotation happens between the append and the apply (the batch does not
//! fit and the memtable is rotated, an oversized batch seals the segment, a
//! checkpoint rotates and flushes), the batch's only record is in segment N while
//! the batch sits in a memtable tagged N + 1. Flushing the memtable tagged N
//! then deletes the record, and a crash before the memtable holding the batch
//! reaches an SST loses an acknowledged write. The invariant the fix keeps:
//!
//! > a batch is applied to a memtable only if its WAL record is in the segment
//! > that memtable is tagged with.
//!
//! Every scenario keeps its controls (the flush never completes, nothing rotates,
//! the rotation happens between commits) so that it cannot pass because nothing
//! was lost to begin with.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;

use super::{collect_transaction_all, collect_transaction_reverse};
use crate::batch::Batch;
use crate::compaction::leveled::Strategy;
use crate::memtable::{max_entry_bytes, MemTable};
use crate::ring::PipelineHook;
use crate::vlog::ValueLocation;
use crate::wal::manager::Wal;
use crate::wal::parallel_recovery::decode_segment_batches;
use crate::wal::{self, segment_name};
use crate::{Durability, Mode, Options, Tree};

const VALUE_LEN: usize = 100;

/// A user key and its value.
type Pair = (Vec<u8>, Vec<u8>);

/// `max_memtable_size` at which about 21 single-key commits fill the arena, so a
/// few hundred commits rotate it many times.
const ROTATING: usize = 4096;

/// Large enough that no scenario that is not about rotation ever rotates.
const NEVER_ROTATING: usize = 64 * 1024 * 1024;

/// Wall-clock bound for a whole scenario: a lost wakeup or a lock that is never
/// released fails the test instead of hanging the suite. (It cannot interrupt a
/// loop that never awaits.)
const WATCHDOG: Duration = Duration::from_secs(120);

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn key_of(i: usize) -> Vec<u8> {
	format!("key_{i:06}").into_bytes()
}

fn val_of(i: usize) -> Vec<u8> {
	let mut v = format!("val_{i:06}_").into_bytes();
	v.resize(VALUE_LEN, b'x');
	v
}

fn pair_of(i: usize) -> Pair {
	(key_of(i), val_of(i))
}

fn base_opts(path: &Path, max_memtable_size: usize) -> Options {
	Options {
		path: path.to_path_buf(),
		max_memtable_size,
		// Crash semantics: nothing may be flushed on drop or close.
		flush_on_close: false,
		// Keep write stalls out of the way.
		memtable_stall_threshold: 1000,
		// No background compaction: once the flushes are done nothing touches the
		// directory any more, so a copy of it is a valid crash image and not a torn one.
		level0_max_files: 10_000,
		l0_stall_threshold: 10_000,
		..Default::default()
	}
}

fn open(path: &Path, max_memtable_size: usize) -> Tree {
	Tree::new(Arc::new(base_opts(path, max_memtable_size))).unwrap()
}

fn log_number(tree: &Tree) -> u64 {
	tree.core.inner.level_manifest.read().unwrap().get_log_number()
}

fn active_wal(tree: &Tree) -> u64 {
	tree.core.inner.active_memtable.read().unwrap().get_wal_number()
}

fn n_imm(tree: &Tree) -> usize {
	tree.core.inner.immutable_memtables.read().unwrap().iter().count()
}

fn n_sst(tree: &Tree) -> usize {
	tree.core.inner.level_manifest.read().unwrap().get_all_tables().len()
}

fn wal_segments(path: &Path) -> Vec<u64> {
	let mut ids = Vec::new();
	if let Ok(rd) = std::fs::read_dir(path.join("wal")) {
		for e in rd.flatten() {
			let name = e.file_name().to_string_lossy().to_string();
			if let Some(stem) = name.strip_suffix(".wal") {
				if let Ok(id) = stem.parse::<u64>() {
					ids.push(id);
				}
			}
		}
	}
	ids.sort();
	ids
}

/// Every decoded WAL record by segment, the way recovery decodes them (one
/// `Batch::decode` per record).
fn wal_batches(path: &Path) -> BTreeMap<u64, Vec<Batch>> {
	wal_segments(path)
		.into_iter()
		.map(|id| {
			let file = path.join("wal").join(format!("{id:020}.wal"));
			(id, decode_segment_batches(&file, id).unwrap().1)
		})
		.collect()
}

fn keys_of(batches: &[Batch]) -> HashSet<Vec<u8>> {
	batches.iter().flat_map(|b| b.entries.iter().map(|e| e.key.clone())).collect()
}

/// User keys with a WAL record, by segment.
fn wal_keys(path: &Path) -> BTreeMap<u64, HashSet<Vec<u8>>> {
	wal_batches(path).into_iter().map(|(id, b)| (id, keys_of(&b))).collect()
}

fn copy_dir_all(src: &Path, dst: &Path) {
	std::fs::create_dir_all(dst).unwrap();
	for entry in std::fs::read_dir(src).unwrap() {
		let entry = entry.unwrap();
		let name = entry.file_name();
		if name == "LOCK" {
			continue;
		}
		let to = dst.join(&name);
		if entry.file_type().unwrap().is_dir() {
			copy_dir_all(&entry.path(), &to);
		} else {
			std::fs::copy(entry.path(), &to).unwrap();
		}
	}
}

/// Prevents `Drop` from spawning a best-effort `close()` on a tree that was
/// deliberately crashed or leaked.
fn mark_closed(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
}

fn release_lock(tree: &Tree) {
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
}

async fn stop_background_tasks(tree: &Tree) {
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
}

fn wake_flusher(tree: &Tree) {
	tree.core.task_manager.lock().unwrap().as_ref().unwrap().wake_up_memtable();
}

/// Waits until every immutable memtable is flushed, then for the (asynchronous)
/// cleanup to delete every WAL segment older than the manifest's `log_number`.
///
/// The cleanup is only given a grace period: a direct-to-L0 write that moves
/// `log_number` past an EMPTY memtable leaves the segment it sealed on disk until
/// the next memtable flush deletes it, and recovery ignores it meanwhile.
async fn wait_flush_complete(tree: &Tree, path: &Path) {
	let start = Instant::now();
	while n_imm(tree) != 0 {
		assert!(start.elapsed() < Duration::from_secs(30), "flush did not complete in time");
		tokio::time::sleep(Duration::from_millis(5)).await;
	}
	let cleanup = Instant::now();
	while wal_segments(path).iter().any(|&s| s < log_number(tree))
		&& cleanup.elapsed() < Duration::from_millis(500)
	{
		tokio::time::sleep(Duration::from_millis(5)).await;
	}
}

/// Waits (blocking) for the asynchronous cleanup of every segment below
/// `log_number`, so that a directory copy taken next is a state a crash long after
/// the flushes would leave, and no file vanishes while it is copied.
fn wait_for_cleanup_blocking(tree: &Tree, path: &Path) {
	let start = Instant::now();
	while wal_segments(path).iter().any(|&s| s < log_number(tree))
		&& start.elapsed() < Duration::from_secs(2)
	{
		std::thread::sleep(Duration::from_millis(2));
	}
}

async fn within<F: std::future::Future>(fut: F) -> F::Output {
	tokio::time::timeout(WATCHDOG, fut).await.expect("scenario exceeded its watchdog")
}

/// The expected keys that `tree` does not return with the expected value.
fn missing_in(tree: &Tree, expected: &[Pair]) -> Vec<String> {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	expected
		.iter()
		.filter(|(k, v)| txn.get(k).unwrap().as_deref() != Some(v.as_slice()))
		.map(|(k, _)| String::from_utf8_lossy(k).into_owned())
		.collect()
}

/// Opens `path` (crash recovery) and returns the expected keys that are missing,
/// then closes the recovered tree without flushing. For throwaway copies.
async fn recover_copy(path: &Path, max_mt: usize, expected: &[Pair]) -> Vec<String> {
	let recovered = open(path, max_mt);
	let missing = missing_in(&recovered, expected);
	recovered.close().await.unwrap();
	missing
}

/// Copies the live directory (a crash image) and recovers the copy.
async fn crash_copy_and_recover(path: &Path, max_mt: usize, expected: &[Pair]) -> Vec<String> {
	let snap = TempDir::new("wal_rotation_snap").unwrap();
	copy_dir_all(path, snap.path());
	recover_copy(snap.path(), max_mt, expected).await
}

/// Crashes by dropping the tree without `close()` after releasing the lock file,
/// then reopens the same directory. There must be no await between the drop and
/// the reopen, so the spawned best-effort close of the dropped tree never runs
/// (current-thread runtime), and nothing after it may await either.
fn crash_by_drop_and_recover(
	tree: Tree,
	path: &Path,
	max_mt: usize,
	expected: &[Pair],
) -> Vec<String> {
	release_lock(&tree);
	mark_closed(&tree);
	drop(tree);
	let reopened = open(path, max_mt);
	let missing = missing_in(&reopened, expected);
	mark_closed(&reopened);
	release_lock(&reopened);
	missing
}

/// Commits one single-key transaction per call and reports whether the commit
/// rotated the active memtable (the WAL tag changed across it).
async fn commit_key(tree: &Tree, i: usize, durability: Durability) -> (bool, bool) {
	let before = active_wal(tree);
	let mut txn = tree.begin().unwrap();
	txn.set_durability(durability);
	txn.set(key_of(i), val_of(i)).unwrap();
	let acked = txn.commit().await.is_ok();
	(acked, active_wal(tree) != before)
}

/// Writes keys `range` one at a time. Returns the acknowledged pairs and the keys
/// whose commit rotated the memtable.
async fn write_keys(
	tree: &Tree,
	range: std::ops::Range<usize>,
	durability: Durability,
) -> (Vec<Pair>, Vec<usize>) {
	let mut acked = Vec::new();
	let mut triggers = Vec::new();
	for i in range {
		let (ok, rotated) = commit_key(tree, i, durability).await;
		if ok {
			acked.push(pair_of(i));
		}
		if rotated {
			triggers.push(i);
		}
	}
	(acked, triggers)
}

/// One mutation of a transaction.
#[derive(Clone, Debug)]
enum Op {
	Set(Vec<u8>, Vec<u8>),
	Delete(Vec<u8>),
	DeleteRange(Vec<u8>, Vec<u8>),
}

fn set(i: usize) -> Op {
	Op::Set(key_of(i), val_of(i))
}

/// Commits every `ops` list as its own transaction, all spawned before the
/// flusher is polled. On the current-thread runtime that is deterministic: each
/// spawned commit runs up to its verdict await, so every one is accepted into the
/// commit ring before the flusher task runs, and the whole round is ONE group in
/// spawn order.
async fn commit_group(
	tree: &Arc<Tree>,
	durability: Durability,
	txns: Vec<Vec<Op>>,
) -> Vec<crate::Result<()>> {
	let mut handles = Vec::new();
	for ops in txns {
		let tree = Arc::clone(tree);
		handles.push(tokio::spawn(async move {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(durability);
			for op in ops {
				match op {
					Op::Set(k, v) => txn.set(k, v).unwrap(),
					Op::Delete(k) => txn.delete(k).unwrap(),
					Op::DeleteRange(a, b) => txn.delete_range(a, b).unwrap(),
				}
			}
			txn.commit().await
		}));
	}
	let mut out = Vec::new();
	for h in handles {
		out.push(h.await.unwrap());
	}
	out
}

/// What the commit pipeline reported about the groups it flushed.
#[derive(Default)]
struct GroupLog {
	/// Size of every group that reached the WAL, in order.
	sizes: Vec<usize>,
	/// Apply rounds that followed a repair of the group (a rotation, a re-append of
	/// stale records or a direct-to-L0 write), as opposed to a group's first round.
	repair_rounds: usize,
}

/// Installs an observer that records the groups the pipeline flushes, so a test
/// can assert it really formed the group it means to test.
fn observe_groups(tree: &Tree) -> Arc<Mutex<GroupLog>> {
	let log = Arc::new(Mutex::new(GroupLog::default()));
	let sink = Arc::clone(&log);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| match point {
		PipelineHook::AfterWalSync {
			batches,
		} => sink.lock().unwrap().sizes.push(batches),
		PipelineHook::BeforeApplyRound {
			round,
		} => {
			if round > 0 {
				sink.lock().unwrap().repair_rounds += 1;
			}
		}
	})));
	log
}

/// Invariant: every user key held by an unflushed memtable has a WAL record in the
/// segment that memtable is tagged with. (Older segments may also hold stale
/// copies; they die with their segment and are never relied on.) Only valid on a
/// tree that was not recovered, where no memtable holds records of several
/// segments.
fn assert_records_match_memtable_tags(tree: &Tree, path: &Path, keys: &[Vec<u8>], what: &str) {
	let index = wal_keys(path);
	let mut memtables: Vec<(u64, Arc<MemTable>)> = tree
		.core
		.inner
		.immutable_memtables
		.read()
		.unwrap()
		.iter()
		.map(|e| (e.wal_number, Arc::clone(&e.memtable)))
		.collect();
	{
		let active = tree.core.inner.active_memtable.read().unwrap();
		memtables.push((active.get_wal_number(), Arc::clone(&active)));
	}
	for key in keys {
		for (tag, memtable) in &memtables {
			if memtable.get(key, None).is_some() {
				assert!(
					index.get(tag).is_some_and(|s| s.contains(key)),
					"{what}: {} is in a memtable tagged WAL segment {tag} but has no record there \
					 (segments with a record: {:?})",
					String::from_utf8_lossy(key),
					index
						.iter()
						.filter(|(_, s)| s.contains(key))
						.map(|(id, _)| *id)
						.collect::<Vec<_>>()
				);
			}
		}
	}
}

// ---------------------------------------------------------------------------
// single sequential writer
// ---------------------------------------------------------------------------

/// After every rotation, once the old memtable has been flushed and its WAL
/// segment deleted, crash-copy the directory and recover the copy: nothing
/// acknowledged may be missing from any snapshot.
#[test(tokio::test)]
async fn rotation_sweep_loses_nothing_across_snapshots() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = open(&path, ROTATING);
		let initial_wal = active_wal(&tree);

		let mut acked = Vec::new();
		let mut triggers = Vec::new();
		let mut snapshots = 0usize;
		let mut missing_total = Vec::new();
		for i in 0..2000 {
			let (a, t) = write_keys(&tree, i..i + 1, Durability::Immediate).await;
			acked.extend(a);
			if t.is_empty() {
				continue;
			}
			triggers.extend(t);
			wait_flush_complete(&tree, &path).await;
			let missing = crash_copy_and_recover(&path, ROTATING, &acked).await;
			snapshots += 1;
			if !missing.is_empty() {
				missing_total.push((i, missing));
			}
			if triggers.len() >= 8 {
				break;
			}
		}
		assert!(
			active_wal(&tree) - initial_wal >= 8 && snapshots >= 8,
			"the sweep must cross at least 8 rotations, saw {} ({snapshots} snapshots)",
			active_wal(&tree) - initial_wal
		);
		assert!(
			missing_total.is_empty(),
			"acknowledged writes lost in {} of {snapshots} snapshots (snapshot after key, \
			 missing keys): {missing_total:?}; rotation triggers {triggers:?}",
			missing_total.len()
		);
		mark_closed(&tree);
	})
	.await;
}

/// Writes single keys until the memtable has rotated `rotations` times.
async fn write_until_rotations(
	tree: &Tree,
	rotations: u64,
	durability: Durability,
) -> (Vec<Pair>, Vec<usize>) {
	let initial = active_wal(tree);
	let mut acked = Vec::new();
	let mut triggers = Vec::new();
	let mut i = 0;
	while active_wal(tree) - initial < rotations {
		assert!(i < 20_000, "no rotation after {i} commits");
		let (a, t) = write_keys(tree, i..i + 1, durability).await;
		acked.extend(a);
		triggers.extend(t);
		i += 1;
	}
	(acked, triggers)
}

/// Writes through several rotations, waits for every flush to complete, then
/// crashes twice: a copy of the live directory, then a drop without `close()`.
async fn final_state_survives_both_crash_methods(max_mt: usize, durability: Durability) {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = open(&path, max_mt);

		let (acked, triggers) = write_until_rotations(&tree, 4, durability).await;
		wait_flush_complete(&tree, &path).await;
		assert!(!triggers.is_empty());
		assert!(n_sst(&tree) >= 1, "flushes must have reached SSTs");

		let missing_copy = crash_copy_and_recover(&path, max_mt, &acked).await;
		let missing_drop = crash_by_drop_and_recover(tree, &path, max_mt, &acked);
		assert!(
			missing_copy.is_empty() && missing_drop.is_empty(),
			"acknowledged writes lost (max_memtable_size {max_mt}, {durability:?}, rotation \
			 triggers {triggers:?}): copy-dir {missing_copy:?}, drop-without-close {missing_drop:?}"
		);
	})
	.await;
}

#[test(tokio::test)]
async fn final_state_survives_both_crash_methods_4096() {
	final_state_survives_both_crash_methods(ROTATING, Durability::Immediate).await;
}

#[test(tokio::test)]
async fn final_state_survives_both_crash_methods_2200() {
	final_state_survives_both_crash_methods(2200, Durability::Immediate).await;
}

#[test(tokio::test)]
async fn final_state_survives_both_crash_methods_8192() {
	final_state_survives_both_crash_methods(8192, Durability::Eventual).await;
}

#[test(tokio::test)]
async fn final_state_survives_both_crash_methods_65536() {
	// ~650 commits fill the arena at this size; Eventual keeps the run fast and a
	// crash image taken from the OS cache does not depend on the fsync anyway.
	final_state_survives_both_crash_methods(65536, Durability::Eventual).await;
}

/// Control A: the same workload with the background flush stopped up front, so
/// every WAL segment is still on disk at the crash. Nothing is lost, with or
/// without the fix, and every segment is still there.
#[test(tokio::test)]
async fn control_flush_never_completes_keeps_every_segment() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = open(&path, ROTATING);
		let initial_wal = active_wal(&tree);
		stop_background_tasks(&tree).await;

		let (acked, triggers) = write_keys(&tree, 0..80, Durability::Immediate).await;
		let rotations = active_wal(&tree) - initial_wal;
		assert!(rotations >= 3, "expected several rotations, saw {rotations}");
		assert!(!triggers.is_empty());
		assert_eq!(n_sst(&tree), 0, "no flush may have completed");
		// The segment before `initial_wal` is the one opening the tree moved away from.
		let retained = wal_segments(&path).into_iter().filter(|id| *id >= initial_wal).count();
		assert_eq!(retained as u64, rotations + 1, "every segment is retained");

		let missing_copy = crash_copy_and_recover(&path, ROTATING, &acked).await;
		let missing_drop = crash_by_drop_and_recover(tree, &path, ROTATING, &acked);
		assert!(
			missing_copy.is_empty() && missing_drop.is_empty(),
			"control lost writes: copy-dir {missing_copy:?}, drop-without-close {missing_drop:?}"
		);
	})
	.await;
}

/// The invariant itself, with the flush stopped so nothing hides a violation:
/// every key sits in a memtable whose tag is a segment that holds the key's
/// record. A rotation by a full arena must therefore not leave the key that
/// triggered it in the segment of the memtable it just left.
#[test(tokio::test)]
async fn every_applied_key_has_a_record_in_its_memtables_segment() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = open(&path, ROTATING);
		stop_background_tasks(&tree).await;

		let (acked, triggers) = write_until_rotations(&tree, 5, Durability::Immediate).await;
		assert!(!triggers.is_empty());
		let keys: Vec<Vec<u8>> = acked.iter().map(|(k, _)| k.clone()).collect();
		assert_records_match_memtable_tags(&tree, &path, &keys, "single writer");

		// A lone writer has no stale copy to repair: the batch that hits the full
		// arena is moved to the new segment before it is logged, so each key has
		// exactly one record.
		let index = wal_keys(&path);
		for key in &keys {
			let copies = index.values().filter(|s| s.contains(key)).count();
			assert_eq!(copies, 1, "{} has {copies} WAL records", String::from_utf8_lossy(key));
		}
		mark_closed(&tree);
	})
	.await;
}

/// Control B: a memtable large enough that nothing rotates. Nothing is lost.
#[test(tokio::test)]
async fn control_no_rotation_loses_nothing() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = open(&path, NEVER_ROTATING);
		let initial_wal = active_wal(&tree);

		let (acked, triggers) = write_keys(&tree, 0..60, Durability::Immediate).await;
		assert_eq!(active_wal(&tree), initial_wal, "this control must never rotate");
		assert!(triggers.is_empty());

		let missing_copy = crash_copy_and_recover(&path, NEVER_ROTATING, &acked).await;
		let missing_drop = crash_by_drop_and_recover(tree, &path, NEVER_ROTATING, &acked);
		assert!(
			missing_copy.is_empty() && missing_drop.is_empty(),
			"control lost writes: copy-dir {missing_copy:?}, drop-without-close {missing_drop:?}"
		);
	})
	.await;
}

/// Control C: rotations that are not caused by a full arena (an explicit
/// `rotate_memtable()` between commits, before the next batch is appended). The
/// old memtable's flush completes and its segment is deleted exactly as in the
/// sweep. Nothing is lost.
#[test(tokio::test)]
async fn control_rotation_between_commits_loses_nothing() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = open(&path, ROTATING);
		let initial_wal = active_wal(&tree);

		let mut acked = Vec::new();
		let mut arena_triggers = Vec::new();
		let mut i = 0;
		while i < 60 {
			// 10 keys then an explicit rotation: well under the ~21 that fill the arena.
			let (a, t) = write_keys(&tree, i..i + 10, Durability::Immediate).await;
			acked.extend(a);
			arena_triggers.extend(t);
			i += 10;
			tree.core.inner.rotate_memtable().unwrap();
			wake_flusher(&tree);
		}
		wait_flush_complete(&tree, &path).await;
		assert!(active_wal(&tree) - initial_wal >= 6, "explicit rotations must have happened");
		assert!(n_sst(&tree) >= 1, "their flushes must have reached SSTs");
		assert!(
			arena_triggers.is_empty(),
			"the arena must not fill in this control, but commits {arena_triggers:?} rotated"
		);

		let missing_copy = crash_copy_and_recover(&path, ROTATING, &acked).await;
		let missing_drop = crash_by_drop_and_recover(tree, &path, ROTATING, &acked);
		assert!(
			missing_copy.is_empty() && missing_drop.is_empty(),
			"control lost writes: copy-dir {missing_copy:?}, drop-without-close {missing_drop:?}"
		);
	})
	.await;
}

/// The window: after rotation 1's old memtable is flushed (its segment deleted),
/// stop flushing and write until the memtable that holds rotation 1's trigger
/// key rotates too, without being flushed. That key's record must still be on
/// disk in a segment that is retained, because the memtable holding it is not in
/// an SST yet. The later trigger key is safe either way.
#[test(tokio::test)]
async fn window_stays_closed_until_the_memtable_holding_the_batch_is_flushed() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = open(&path, ROTATING);

		// Phase 1: write until the first rotation, wait for its flush and cleanup.
		let mut acked = Vec::new();
		let mut triggers = Vec::new();
		let mut i = 0usize;
		while triggers.is_empty() {
			let (a, t) = write_keys(&tree, i..i + 1, Durability::Immediate).await;
			acked.extend(a);
			triggers.extend(t);
			i += 1;
		}
		wait_flush_complete(&tree, &path).await;
		let first_trigger = triggers[0];

		// Phase 2: stop flushing; write until the memtable holding that key rotates.
		stop_background_tasks(&tree).await;
		let mut second = Vec::new();
		while second.is_empty() {
			let (a, t) = write_keys(&tree, i..i + 1, Durability::Immediate).await;
			acked.extend(a);
			second.extend(t);
			i += 1;
		}
		assert_eq!(
			n_imm(&tree),
			1,
			"the memtable holding the first trigger is immutable and unflushed"
		);
		assert_eq!(
			wal_segments(&path)[0],
			log_number(&tree),
			"segments from log_number on must all still exist, and none below"
		);

		let missing = crash_copy_and_recover(&path, ROTATING, &acked).await;
		assert!(
			missing.is_empty(),
			"acknowledged writes lost with the flusher stopped (first trigger key {first_trigger}, \
			 second {second:?}): {missing:?}"
		);
		mark_closed(&tree);
	})
	.await;
}

// ---------------------------------------------------------------------------
// concurrent writers and commit groups
// ---------------------------------------------------------------------------

/// Deterministic group commit on the current-thread runtime: each round commits K
/// single-key transactions that form ONE group, so the group straddles a
/// rotation whenever the arena fills partway through it. After every rotation
/// whose old memtable has been flushed (segment deleted), crash-copy, recover and
/// list what is lost.
async fn grouped_writers_sweep(k: usize, durability: Durability) {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = Arc::new(open(&path, ROTATING));
		let groups = observe_groups(&tree);

		let mut acked = Vec::new();
		let mut next = 0usize;
		let mut rotations = 0usize;
		let mut missing_total = Vec::new();
		for _round in 0..60 {
			let before = active_wal(&tree);
			let round: Vec<usize> = (next..next + k).collect();
			next += k;
			let results =
				commit_group(&tree, durability, round.iter().map(|&i| vec![set(i)]).collect())
					.await;
			for (&i, r) in round.iter().zip(&results) {
				r.as_ref().unwrap();
				acked.push(pair_of(i));
			}
			assert_eq!(
				groups.lock().unwrap().sizes.last(),
				Some(&k),
				"the round must have been committed as one group of {k}"
			);

			if active_wal(&tree) != before {
				rotations += 1;
				wait_flush_complete(&tree, &path).await;
				let missing = crash_copy_and_recover(&path, ROTATING, &acked).await;
				if !missing.is_empty() {
					missing_total.push((round[0]..=round[k - 1], missing));
				}
				if rotations >= 6 {
					break;
				}
			}
		}
		assert!(rotations >= 6, "the sweep must cross at least 6 rotations, saw {rotations}");
		assert!(
			missing_total.is_empty(),
			"K={k} {durability:?}: acknowledged writes lost after {} of {rotations} rotations \
			 (round keys, missing): {missing_total:?}",
			missing_total.len()
		);
		assert!(
			groups.lock().unwrap().repair_rounds > 0,
			"K={k}: no group straddled a rotation, so the sweep did not exercise what it is for"
		);
		mark_closed(&tree);
	})
	.await;
}

/// The invariant for groups, with the flush stopped so nothing hides a violation:
/// after every round, every key sits in a memtable whose tag is a segment that
/// holds the key's record, including the keys of groups that straddled a rotation.
#[test(tokio::test)]
async fn grouped_writers_keep_every_record_in_its_memtables_segment() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = Arc::new(open(&path, ROTATING));
		stop_background_tasks(&tree).await;
		let groups = observe_groups(&tree);

		let k = 8;
		let initial = active_wal(&tree);
		let mut keys = Vec::new();
		let mut next = 0;
		while active_wal(&tree) - initial < 5 {
			assert!(next < 2000);
			let round: Vec<usize> = (next..next + k).collect();
			next += k;
			let results = commit_group(
				&tree,
				Durability::Immediate,
				round.iter().map(|&i| vec![set(i)]).collect(),
			)
			.await;
			results.iter().for_each(|r| {
				r.as_ref().unwrap();
			});
			keys.extend(round.iter().map(|&i| key_of(i)));
			assert_records_match_memtable_tags(&tree, &path, &keys, "grouped writers");
		}
		assert!(
			groups.lock().unwrap().repair_rounds > 0,
			"no group straddled a rotation, so the test did not exercise what it is for"
		);
		mark_closed(&tree);
	})
	.await;
}

#[test(tokio::test)]
async fn grouped_writers_sweep_k8() {
	grouped_writers_sweep(8, Durability::Immediate).await;
}

#[test(tokio::test)]
async fn grouped_writers_sweep_k16() {
	grouped_writers_sweep(16, Durability::Immediate).await;
}

#[test(tokio::test)]
async fn grouped_writers_sweep_k16_eventual() {
	grouped_writers_sweep(16, Durability::Eventual).await;
}

/// Eight concurrent writers on a multi-threaded runtime, so batches share group
/// commits at unpredictable boundaries and the trigger's group straddles a
/// rotation. The crash image is taken after every flush completes.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn concurrent_writers_across_rotations_lose_nothing() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		// Shared through an `Arc`, never `Tree::clone()`: dropping any clone of a
		// `Tree` spawns `core.close()`.
		let tree = Arc::new(open(&path, ROTATING));
		let initial_wal = active_wal(&tree);

		let writers = 8usize;
		let per = 30usize;
		let mut handles = Vec::new();
		for w in 0..writers {
			let tree = Arc::clone(&tree);
			handles.push(tokio::spawn(async move {
				let mut acked = Vec::new();
				for j in 0..per {
					let i = w * per + j;
					let mut txn = tree.begin().unwrap();
					txn.set_durability(Durability::Immediate);
					txn.set(key_of(i), val_of(i)).unwrap();
					if txn.commit().await.is_ok() {
						acked.push(pair_of(i));
					}
				}
				acked
			}));
		}
		let mut acked = Vec::new();
		for h in handles {
			acked.extend(h.await.unwrap());
		}
		assert_eq!(acked.len(), writers * per);
		assert!(
			active_wal(&tree) - initial_wal >= 5,
			"the writers must have rotated several times"
		);
		wait_flush_complete(&tree, &path).await;

		// Control: the live tree serves every acknowledged key, so a loss below is
		// a durability loss and not a visibility bug.
		assert!(missing_in(&tree, &acked).is_empty(), "the live tree lost acknowledged writes");

		let missing = crash_copy_and_recover(&path, ROTATING, &acked).await;
		assert!(missing.is_empty(), "acknowledged writes lost: {missing:?}");
		mark_closed(&tree);
	})
	.await;
}

// ---------------------------------------------------------------------------
// group shapes: direct-to-L0 and straddling groups
// ---------------------------------------------------------------------------

/// How big one single-key transaction of a group is.
#[derive(Clone, Copy, Debug)]
enum Size {
	/// A normal commit, about 100 bytes.
	Small,
	/// More than `max_memtable_size`: bypasses the memtable and is written
	/// straight to an L0 table.
	Oversized,
	/// Not more than `max_memtable_size`, yet more than a fresh memtable has
	/// room for: the arena is "full" even when empty.
	ArenaOversized,
}

/// A value whose single-key batch has the given `Size` for `max_mt`.
fn value_for(size: Size, key: &[u8], max_mt: usize, i: usize) -> Vec<u8> {
	let len = match size {
		Size::Small => return val_of(i),
		Size::Oversized => max_mt + 600,
		Size::ArenaOversized => {
			let fresh = MemTable::new(max_mt);
			let avail = (fresh.arena_capacity() - fresh.size()) as u64;
			// Smallest value whose estimate (on the framed value the flusher applies)
			// no longer fits a fresh memtable.
			let framed = |len: usize| ValueLocation::with_inline_value(vec![0; len]).encode().len();
			let len = (0..max_mt)
				.find(|&len| max_entry_bytes(key.len(), framed(len)) > avail)
				.expect("some value is arena-oversized");
			assert!(
				max_entry_bytes(key.len(), framed(len)) <= max_mt as u64,
				"an arena-oversized batch must not also be oversized"
			);
			len
		}
	};
	let mut v = format!("val_{i:06}_").into_bytes();
	v.resize(len, b'x');
	v
}

fn shape_pairs(sizes: &[Size], max_mt: usize, first: usize) -> Vec<Pair> {
	sizes
		.iter()
		.enumerate()
		.map(|(n, &size)| {
			let i = first + n;
			let key = key_of(i);
			let value = value_for(size, &key, max_mt, i);
			(key, value)
		})
		.collect()
}

/// What the active memtable holds before the group under test is committed.
#[derive(Clone, Copy, Debug)]
enum Prefill {
	/// This many small commits (none is a rotation trigger at `ROTATING`).
	Smalls(usize),
	/// Small commits until the next single-key commit no longer fits the arena, so
	/// it is the next commit that rotates.
	UntilFull,
}

/// Whether the single-key commit for key `i` still fits the active memtable.
fn next_commit_fits(tree: &Tree, i: usize) -> bool {
	let mut batch = Batch::new(0);
	batch
		.add_record(
			crate::InternalKeyKind::Set,
			key_of(i),
			Some(ValueLocation::with_inline_value(val_of(i)).encode()),
			0,
		)
		.unwrap();
	let active = tree.core.inner.active_memtable.read().unwrap();
	(active.arena_capacity() - active.size()) as u64 >= batch.memtable_size_estimate()
}

/// Commits single keys (from key 0) until the next one no longer fits.
async fn fill_until_full(tree: &Tree, durability: Durability) -> Vec<Pair> {
	let wal = active_wal(tree);
	let mut acked = Vec::new();
	let mut i = 0;
	while next_commit_fits(tree, i) {
		let (a, rotated) = write_keys(tree, i..i + 1, durability).await;
		assert!(rotated.is_empty(), "filling the arena must not rotate it");
		acked.extend(a);
		i += 1;
	}
	assert_eq!(active_wal(tree), wal);
	assert!(i > 5, "the arena filled after only {i} commits");
	acked
}

/// Commits `prefill` one at a time, then `group` as one commit group, and crashes
/// at every interesting point:
///
/// * at the group's WAL sync (everything is on disk, nothing is applied),
/// * right after the commits return (flushes possibly not started),
/// * after every flush and cleanup has completed (old segments deleted),
/// * by dropping the tree without `close()`.
///
/// All acknowledged keys must be recovered, and so must the keys of the in-flight
/// group at the sync point (its records are all durable).
async fn run_shape(max_mt: usize, prefill: Prefill, group: &[Size], durability: Durability) {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = Arc::new(open(&path, max_mt));

		let prefill_pairs = match prefill {
			Prefill::Smalls(n) => {
				let pairs = shape_pairs(&vec![Small; n], max_mt, 0);
				for (k, v) in &pairs {
					let mut txn = tree.begin().unwrap();
					txn.set_durability(durability);
					txn.set(k, v).unwrap();
					txn.commit().await.unwrap();
				}
				pairs
			}
			Prefill::UntilFull => fill_until_full(&tree, durability).await,
		};
		let group_pairs = shape_pairs(group, max_mt, 1000);

		// The group is observed, and imaged at its WAL sync and at the start of every
		// apply round after it (after a rotation, a direct-to-L0 write or a re-append
		// of stale records). Nothing else runs while the flusher is inside the hook.
		let groups = Arc::new(Mutex::new(Vec::new()));
		let at_steps: Arc<Mutex<Vec<TempDir>>> = Arc::new(Mutex::new(Vec::new()));
		{
			let groups = Arc::clone(&groups);
			let at_steps = Arc::clone(&at_steps);
			let src = path.clone();
			tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
				if let PipelineHook::AfterWalSync {
					batches,
				} = point
				{
					groups.lock().unwrap().push(batches);
				}
				let snap = TempDir::new("wal_rotation_step").unwrap();
				copy_dir_all(&src, snap.path());
				at_steps.lock().unwrap().push(snap);
			})));
		}

		let results = commit_group(
			&tree,
			durability,
			group_pairs.iter().map(|(k, v)| vec![Op::Set(k.clone(), v.clone())]).collect(),
		)
		.await;
		for r in &results {
			r.as_ref().unwrap();
		}
		assert_eq!(
			*groups.lock().unwrap(),
			vec![group.len()],
			"the group {group:?} must have been committed as exactly one group"
		);

		let everything: Vec<_> = prefill_pairs.iter().chain(&group_pairs).cloned().collect();
		let label = format!(
			"prefill {prefill:?}, group {group:?}, max_memtable_size {max_mt}, {durability:?}"
		);

		// Crash images taken inside the group commit. Everything acknowledged before the
		// group must be recovered from each of them. The group itself is not
		// acknowledged yet: what is recovered of it must be all of it, or a prefix in
		// commit order (a direct-to-L0 write in the middle of the group can advance the
		// manifest's `log_number` past the segment that holds the records after it).
		let step_images: Vec<TempDir> = std::mem::take(&mut *at_steps.lock().unwrap());
		assert!(!step_images.is_empty());
		for (n, image) in step_images.iter().enumerate() {
			let missing = recover_copy(image.path(), max_mt, &everything).await;
			let lost_before_group: Vec<_> = prefill_pairs
				.iter()
				.map(|(k, _)| String::from_utf8_lossy(k).into_owned())
				.filter(|k| missing.contains(k))
				.collect();
			assert!(
				lost_before_group.is_empty(),
				"{label}: acknowledged writes lost in the crash image taken at step {n} of {} \
				 of the group commit: {lost_before_group:?}",
				step_images.len()
			);
			let mut gap = false;
			for (k, _) in &group_pairs {
				let lost = missing.contains(&String::from_utf8_lossy(k).into_owned());
				assert!(
					!(gap && !lost),
					"{label}: step {n} of {}: the group's recovered part is not a prefix in commit \
					 order, missing {missing:?}",
					step_images.len()
				);
				gap |= lost;
			}
		}

		// Crash image right after the commits returned, flushes still in flight.
		let missing_early = crash_copy_and_recover(&path, max_mt, &everything).await;
		assert!(
			missing_early.is_empty(),
			"{label}: lost right after the commits: {missing_early:?}"
		);

		// Crash image once every flush completed and the old segments are deleted.
		wait_flush_complete(&tree, &path).await;
		let missing_late = crash_copy_and_recover(&path, max_mt, &everything).await;
		assert!(
			missing_late.is_empty(),
			"{label}: lost after the flushes completed: {missing_late:?}"
		);

		let missing_drop = crash_by_drop_and_recover(
			Arc::try_unwrap(tree).ok().expect("the writers are done"),
			&path,
			max_mt,
			&everything,
		);
		assert!(missing_drop.is_empty(), "{label}: lost on drop without close: {missing_drop:?}");
	})
	.await;
}

use Size::{ArenaOversized, Oversized, Small};

/// The case from the plan: [small A, oversized B, small C] with an active memtable
/// that already holds earlier writes. A is applied, B takes the direct-to-L0 path
/// and seals the segment, C lands in the next memtable.
#[test(tokio::test)]
async fn shape_small_oversized_small_with_a_non_empty_memtable() {
	run_shape(ROTATING, Prefill::Smalls(2), &[Small, Oversized, Small], Durability::Immediate)
		.await;
}

#[test(tokio::test)]
async fn shape_small_oversized_small_eventual() {
	run_shape(ROTATING, Prefill::Smalls(2), &[Small, Oversized, Small], Durability::Eventual).await;
}

/// [B, C] with an EMPTY active memtable: B's seal only rotates the WAL, the
/// manifest's `log_number` moves past it at once, and C, applied afterwards, has
/// its record in a segment recovery no longer reads. Nothing needs to flush for C
/// to be lost.
#[test(tokio::test)]
async fn shape_oversized_small_with_an_empty_memtable() {
	run_shape(ROTATING, Prefill::Smalls(0), &[Oversized, Small], Durability::Immediate).await;
}

#[test(tokio::test)]
async fn shape_small_oversized() {
	run_shape(ROTATING, Prefill::Smalls(1), &[Small, Oversized], Durability::Immediate).await;
}

#[test(tokio::test)]
async fn shape_oversized_small() {
	run_shape(ROTATING, Prefill::Smalls(1), &[Oversized, Small], Durability::Immediate).await;
}

#[test(tokio::test)]
async fn shape_two_oversized_then_small() {
	run_shape(
		ROTATING,
		Prefill::Smalls(2),
		&[Oversized, Oversized, Small, Small],
		Durability::Immediate,
	)
	.await;
}

/// A group of small commits that straddles an arena fill, with nothing oversized.
#[test(tokio::test)]
async fn shape_group_straddling_an_arena_fill() {
	run_shape(ROTATING, Prefill::Smalls(10), &[Small; 24], Durability::Immediate).await;
}

/// Every batch of this group is small, but the first one does not fit what is left
/// of the memtable, so the rotation happens before anything is applied.
#[test(tokio::test)]
async fn shape_group_whose_first_batch_does_not_fit() {
	run_shape(ROTATING, Prefill::UntilFull, &[Small; 5], Durability::Immediate).await;
}

/// A batch that is not oversized but is larger than a fresh memtable has room for.
/// It cannot be applied to any memtable, however often it rotates, so it has to
/// take the direct-to-L0 path. A rotation loop that does not recognise this would
/// spin forever on an empty memtable.
#[test(tokio::test)]
async fn shape_arena_oversized_batch_with_a_non_empty_memtable() {
	run_shape(ROTATING, Prefill::Smalls(2), &[Small, ArenaOversized, Small], Durability::Immediate)
		.await;
}

#[test(tokio::test)]
async fn shape_arena_oversized_batch_with_an_empty_memtable() {
	run_shape(ROTATING, Prefill::Smalls(0), &[ArenaOversized, Small], Durability::Immediate).await;
}

/// The same group shapes with the background flush stopped (control): the
/// segments that hold the records are all retained, so nothing is lost from the
/// WAL, except where the direct-to-L0 write advanced `log_number` itself.
#[test(tokio::test)]
async fn direct_l0_mid_group_with_the_flusher_stopped() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = Arc::new(open(&path, ROTATING));
		stop_background_tasks(&tree).await;

		let prefill = shape_pairs(&[Small, Small], ROTATING, 0);
		for (k, v) in &prefill {
			let mut txn = tree.begin().unwrap();
			txn.set(k, v).unwrap();
			txn.commit().await.unwrap();
		}
		let groups = observe_groups(&tree);
		let group = shape_pairs(&[Small, Oversized, Small], ROTATING, 1000);
		let results = commit_group(
			&tree,
			Durability::Immediate,
			group.iter().map(|(k, v)| vec![Op::Set(k.clone(), v.clone())]).collect(),
		)
		.await;
		results.iter().for_each(|r| {
			r.as_ref().unwrap();
		});
		assert_eq!(groups.lock().unwrap().sizes, vec![3]);
		// A direct-to-L0 write first flushes every pending immutable memtable (#423's
		// `flush_lock` path in `write_batch_direct_to_l0_sst`), so the memtable the seal
		// rotated out is flushed to an L0 table before the oversized batch's own table.
		assert_eq!(n_sst(&tree), 2, "A's memtable and then the oversized batch are in L0 tables");
		assert_eq!(n_imm(&tree), 0, "A's memtable was rotated by the seal and flushed first");

		let keys: Vec<Vec<u8>> = prefill.iter().chain(&group).map(|(k, _)| k.clone()).collect();
		assert_records_match_memtable_tags(&tree, &path, &keys, "direct L0 mid-group");

		let everything: Vec<_> = prefill.iter().chain(&group).cloned().collect();
		let missing = crash_by_drop_and_recover(
			Arc::try_unwrap(tree).ok().expect("the writers are done"),
			&path,
			ROTATING,
			&everything,
		);
		assert!(missing.is_empty(), "lost with the flusher stopped: {missing:?}");
	})
	.await;
}

// ---------------------------------------------------------------------------
// a rotation by someone else (checkpoint, Tree::flush) between WAL sync and apply
// ---------------------------------------------------------------------------

/// What the foreign actor does at the group's WAL sync.
#[derive(Clone, Copy, Debug)]
enum Foreign {
	/// Rotate the memtable only.
	Rotate,
	/// Rotate, then flush every immutable memtable: exactly what a checkpoint does.
	RotateAndFlush,
	/// Rotate twice. The second rotation finds an empty memtable and must be a no-op.
	RotateTwice,
}

async fn foreign_rotation_between_sync_and_apply(foreign: Foreign) {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let max_mt = 64 * 1024;
		let tree = Arc::new(open(&path, max_mt));

		// The active memtable must hold something, or the rotation is a no-op.
		let earlier = vec![pair_of(0), pair_of(1)];
		for (k, v) in &earlier {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(Durability::Immediate);
			txn.set(k, v).unwrap();
			txn.commit().await.unwrap();
		}
		let wal_before = active_wal(&tree);

		let inner = Arc::clone(&tree.core.inner);
		let seen = Arc::new(Mutex::new(Vec::new()));
		{
			let seen = Arc::clone(&seen);
			tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
				let PipelineHook::AfterWalSync {
					batches,
				} = point
				else {
					return;
				};
				seen.lock().unwrap().push(batches);
				inner.rotate_memtable().unwrap();
				let after_first = inner.active_memtable.read().unwrap().get_wal_number();
				match foreign {
					Foreign::Rotate => {}
					Foreign::RotateAndFlush => inner.flush_all_immutables_sync().unwrap(),
					Foreign::RotateTwice => {
						inner.rotate_memtable().unwrap();
						assert_eq!(
							inner.active_memtable.read().unwrap().get_wal_number(),
							after_first,
							"rotating an empty memtable must be a no-op"
						);
					}
				}
			})));
		}

		let group: Vec<_> = (10..14).map(pair_of).collect();
		let results = commit_group(
			&tree,
			Durability::Immediate,
			group.iter().map(|(k, v)| vec![Op::Set(k.clone(), v.clone())]).collect(),
		)
		.await;
		results.iter().for_each(|r| {
			r.as_ref().unwrap();
		});
		assert_eq!(*seen.lock().unwrap(), vec![group.len()]);
		assert_eq!(active_wal(&tree), wal_before + 1, "the foreign rotation moved the WAL on once");

		let everything: Vec<_> = earlier.iter().chain(&group).cloned().collect();
		let group_keys: Vec<Vec<u8>> = group.iter().map(|(k, _)| k.clone()).collect();

		if matches!(foreign, Foreign::RotateAndFlush) {
			// The old memtable is in an SST and its segment is gone, so the group's
			// records can only be in the new segment.
			wait_flush_complete(&tree, &path).await;
			assert_eq!(log_number(&tree), wal_before + 1);
			assert!(wal_segments(&path).iter().all(|&s| s >= log_number(&tree)));
		}
		// Whatever the actor did, every batch of the group is applied to the memtable
		// tagged with the new segment, so each must have its record in that segment.
		let keys = wal_keys(&path);
		for key in &group_keys {
			assert!(
				keys.get(&active_wal(&tree)).is_some_and(|s| s.contains(key)),
				"{foreign:?}: {} is applied to the memtable tagged {} but has no record in that \
				 segment (segments with a record: {:?})",
				String::from_utf8_lossy(key),
				active_wal(&tree),
				keys.iter().filter(|(_, s)| s.contains(key)).map(|(id, _)| *id).collect::<Vec<_>>()
			);
		}

		let missing = crash_copy_and_recover(&path, max_mt, &everything).await;
		assert!(missing.is_empty(), "{foreign:?}: acknowledged writes lost: {missing:?}");
		let missing = crash_by_drop_and_recover(
			Arc::try_unwrap(tree).ok().expect("the writers are done"),
			&path,
			max_mt,
			&everything,
		);
		assert!(missing.is_empty(), "{foreign:?}: lost on drop without close: {missing:?}");
	})
	.await;
}

/// A checkpoint (or `Tree::flush`) rotates and flushes the memtable between the
/// group's WAL sync and its apply: the segment the group was logged in is deleted.
#[test(tokio::test)]
async fn foreign_rotate_and_flush_between_sync_and_apply() {
	foreign_rotation_between_sync_and_apply(Foreign::RotateAndFlush).await;
}

#[test(tokio::test)]
async fn foreign_rotate_between_sync_and_apply() {
	foreign_rotation_between_sync_and_apply(Foreign::Rotate).await;
}

#[test(tokio::test)]
async fn foreign_double_rotate_between_sync_and_apply() {
	foreign_rotation_between_sync_and_apply(Foreign::RotateTwice).await;
}

// ---------------------------------------------------------------------------
// a group is logged in one segment, and repaired with one more group
// ---------------------------------------------------------------------------

/// The keys of every record of `batches`, in order.
fn ordered_keys(batches: &[Batch]) -> Vec<Vec<u8>> {
	batches.iter().flat_map(|b| b.entries.iter().map(|e| e.key.clone())).collect()
}

/// How many `write(2)` calls the active WAL segment has taken so far.
fn active_wal_writes(tree: &Tree) -> usize {
	tree.core.inner.wal.read().file_writes()
}

/// A group whose tail does not fit what is left of the arena. The whole group is logged in
/// the segment that was active, as one append, so no record of it can land in a segment
/// other than the one the apply compares with the memtable's tag. The memtable rotates
/// partway through the group and the batches after the rotation are logged again, all of
/// them in one append, in the segment of the new memtable: one write, in order, and each of
/// them once.
#[test(tokio::test)]
async fn a_stale_subset_is_re_appended_as_one_group() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = Arc::new(open(&path, ROTATING));
		// The segments stay on disk: nothing flushes the memtable that rotates.
		stop_background_tasks(&tree).await;

		let prefill = shape_pairs(&[Small; 10], ROTATING, 0);
		for (k, v) in &prefill {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(Durability::Immediate);
			txn.set(k, v).unwrap();
			txn.commit().await.unwrap();
		}
		let first = active_wal(&tree);
		let groups = observe_groups(&tree);

		let group = shape_pairs(&[Small; 24], ROTATING, 1000);
		let results = commit_group(
			&tree,
			Durability::Immediate,
			group.iter().map(|(k, v)| vec![Op::Set(k.clone(), v.clone())]).collect(),
		)
		.await;
		results.iter().for_each(|r| {
			r.as_ref().unwrap();
		});
		assert_eq!(groups.lock().unwrap().sizes, vec![24], "one group of 24");
		assert_eq!(active_wal(&tree), first + 1, "the arena filled once, partway through");
		assert!(groups.lock().unwrap().repair_rounds > 0);

		let logged = wal_batches(&path);
		assert_eq!(logged.range(first..).count(), 2, "the segment of the group and the next one");
		assert_eq!(
			logged[&first].len(),
			prefill.len() + group.len(),
			"the group is whole in the segment that was active when it was appended"
		);
		let stale = ordered_keys(&logged[&(first + 1)]);
		assert!(!stale.is_empty() && stale.len() < group.len(), "{} stale", stale.len());
		let tail: Vec<Vec<u8>> =
			group[group.len() - stale.len()..].iter().map(|(k, _)| k.clone()).collect();
		assert_eq!(stale, tail, "the stale tail of the group, in order, each batch once");
		assert_eq!(
			logged[&(first + 1)].len(),
			stale.len(),
			"one record per stale batch, and nothing else"
		);
		assert_eq!(active_wal_writes(&tree), 1, "the stale tail is one write, not one per batch");

		let keys: Vec<Vec<u8>> = prefill.iter().chain(&group).map(|(k, _)| k.clone()).collect();
		assert_records_match_memtable_tags(&tree, &path, &keys, "stale subset");
		let everything: Vec<_> = prefill.iter().chain(&group).cloned().collect();
		let missing = crash_by_drop_and_recover(
			Arc::try_unwrap(tree).ok().expect("the writers are done"),
			&path,
			ROTATING,
			&everything,
		);
		assert!(missing.is_empty(), "lost on drop without close: {missing:?}");
	})
	.await;
}

/// A rotation between the group's append and its apply makes every batch of the group
/// stale: all of it is appended again, as one group in the new segment, and applied to the
/// memtable tagged with it.
#[test(tokio::test)]
async fn a_rotation_after_the_append_logs_the_whole_group_again_as_one_group() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = Arc::new(open(&path, 64 * 1024));
		stop_background_tasks(&tree).await;

		let earlier = vec![pair_of(0), pair_of(1)];
		for (k, v) in &earlier {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(Durability::Immediate);
			txn.set(k, v).unwrap();
			txn.commit().await.unwrap();
		}
		let first = active_wal(&tree);

		let inner = Arc::clone(&tree.core.inner);
		let rotated = Arc::new(Mutex::new(0usize));
		{
			let rotated = Arc::clone(&rotated);
			tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
				if matches!(point, PipelineHook::AfterWalSync { .. }) {
					*rotated.lock().unwrap() += 1;
					inner.rotate_memtable().unwrap();
				}
			})));
		}

		let group: Vec<_> = (10..16).map(pair_of).collect();
		let results = commit_group(
			&tree,
			Durability::Immediate,
			group.iter().map(|(k, v)| vec![Op::Set(k.clone(), v.clone())]).collect(),
		)
		.await;
		results.iter().for_each(|r| {
			r.as_ref().unwrap();
		});
		assert_eq!(*rotated.lock().unwrap(), 1, "one group");
		assert_eq!(active_wal(&tree), first + 1);

		let logged = wal_batches(&path);
		let group_keys: Vec<Vec<u8>> = group.iter().map(|(k, _)| k.clone()).collect();
		assert_eq!(logged[&first].len(), earlier.len() + group.len());
		assert_eq!(ordered_keys(&logged[&(first + 1)]), group_keys);
		assert_eq!(logged[&(first + 1)].len(), group.len(), "one record per batch");
		assert_eq!(active_wal_writes(&tree), 1, "the whole group is logged again in one write");

		let keys: Vec<Vec<u8>> = earlier.iter().chain(&group).map(|(k, _)| k.clone()).collect();
		assert_records_match_memtable_tags(&tree, &path, &keys, "rotation after the append");
		mark_closed(&tree);
	})
	.await;
}

// ---------------------------------------------------------------------------
// the stale copies a repaired group leaves behind are harmless
// ---------------------------------------------------------------------------

/// A group that straddles a foreign rotation is logged twice: stale copies in the
/// old segment and the real copies in the new one. With the flusher stopped the
/// old segment is replayed next to the new one after a crash, so every record of
/// the group is replayed twice, into two memtables, one of which is flushed. Point
/// reads, forward and backward scans must still see each key once with the right
/// value, a deleted key and a deleted range must stay deleted, and compacting the
/// resulting duplicate (key, sequence) entries must neither fail nor change the
/// data.
#[test(tokio::test)]
async fn duplicate_records_from_a_repaired_group_are_harmless() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let max_mt = 64 * 1024;
		let tree = Arc::new(open(&path, max_mt));
		stop_background_tasks(&tree).await;

		let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
		// Earlier commits, in the segment that is about to be rotated away.
		for i in 0..10 {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(Durability::Immediate);
			txn.set(key_of(i), val_of(i)).unwrap();
			txn.commit().await.unwrap();
			model.insert(key_of(i), val_of(i));
		}

		let inner = Arc::clone(&tree.core.inner);
		let rotated = Arc::new(Mutex::new(0usize));
		{
			let rotated = Arc::clone(&rotated);
			tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
				if matches!(point, PipelineHook::AfterWalSync { .. }) {
					*rotated.lock().unwrap() += 1;
					inner.rotate_memtable().unwrap();
				}
			})));
		}

		// One group: overwrite, delete, delete-range over earlier keys, new keys.
		let group = vec![
			vec![Op::Set(key_of(3), b"three-new".to_vec())],
			vec![Op::Delete(key_of(5))],
			vec![Op::DeleteRange(key_of(7), key_of(9))],
			vec![set(100), set(101)],
			vec![Op::Set(key_of(102), b"hundred-two".to_vec())],
		];
		for ops in &group {
			for op in ops {
				match op {
					Op::Set(k, v) => {
						model.insert(k.clone(), v.clone());
					}
					Op::Delete(k) => {
						model.remove(k);
					}
					Op::DeleteRange(a, b) => model.retain(|k, _| !(k >= a && k < b)),
				}
			}
		}
		let results = commit_group(&tree, Durability::Immediate, group).await;
		results.iter().for_each(|r| {
			r.as_ref().unwrap();
		});
		assert_eq!(*rotated.lock().unwrap(), 1);
		let expected: Vec<Pair> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

		// Image with the flusher stopped: segment N (earlier commits and the group's
		// stale copies) and segment N+1 (the group) are both replayed.
		let snap = TempDir::new("wal_rotation_snap").unwrap();
		copy_dir_all(&path, snap.path());
		mark_closed(&tree);
		// The repair logged the group twice: stale copies in the segment that was
		// rotated away, real copies in the new one. Without it there is nothing to
		// test here, and this is the first thing to fail.
		let logged = wal_batches(snap.path());
		for ops in [102, 100, 101] {
			let copies = logged
				.values()
				.flatten()
				.filter(|b| b.entries.iter().any(|e| e.key == key_of(ops)))
				.count();
			assert_eq!(copies, 2, "key {ops} must be logged in the old and in the new segment");
		}

		// Two L0 tables are enough for the compaction below to run.
		let opts = Arc::new(Options {
			level0_max_files: 2,
			l0_stall_threshold: 12,
			..base_opts(snap.path(), max_mt)
		});
		let recovered = Tree::new(Arc::clone(&opts)).unwrap();
		// The flush and compaction below are driven by hand.
		stop_background_tasks(&recovered).await;

		let check = |tree: &Tree, when: &str| {
			let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
			for (k, v) in &expected {
				assert_eq!(txn.get(k).unwrap().as_deref(), Some(v.as_slice()), "{when}: get {k:?}");
			}
			for gone in [key_of(5), key_of(7), key_of(8)] {
				assert_eq!(txn.get(&gone).unwrap(), None, "{when}: {gone:?} stays deleted");
			}
			let forward: Vec<_> = collect_transaction_all(&mut txn.iter().unwrap()).unwrap();
			let backward: Vec<_> = collect_transaction_reverse(&mut txn.iter().unwrap()).unwrap();
			assert_eq!(forward, expected, "{when}: forward scan returns each key once");
			let mut reversed = expected.clone();
			reversed.reverse();
			assert_eq!(backward, reversed, "{when}: backward scan returns each key once");
		};
		check(&recovered, "after recovery");

		// Flush everything and compact the duplicates together.
		recovered.flush().unwrap();
		check(&recovered, "after flush");
		assert!(n_sst(&recovered) >= 2, "recovery and the flush each left an L0 table");
		recovered.compact(Arc::new(Strategy::from_options(Arc::clone(&opts)))).unwrap();
		{
			// A skipped compaction also returns Ok: check that it merged the tables.
			let manifest = recovered.core.inner.level_manifest.read().unwrap();
			let levels = manifest.levels.get_levels();
			assert!(levels[0].tables.is_empty(), "compaction must have emptied L0");
			assert_eq!(levels[1].tables.len(), 1, "and merged the duplicates into one table");
		}
		check(&recovered, "after compaction");
		mark_closed(&recovered);
		release_lock(&recovered);
	})
	.await;
}

// ---------------------------------------------------------------------------
// the tag of the active memtable always names the active WAL segment
// ---------------------------------------------------------------------------

/// The commit pipeline compares the segment a record was written to with the tag
/// of the active memtable, so the tag must equal the active WAL number after every
/// transition that creates, replaces or rotates either of them. (A memtable that
/// is created without a tag has tag 0 and every commit against it would fail.)
#[test(tokio::test)]
async fn active_memtable_tag_equals_the_wal_number_after_every_transition() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tag_matches = |tree: &Tree, what: &str| {
			assert_eq!(
				active_wal(tree),
				tree.core.inner.wal.read().get_active_log_number(),
				"after {what}"
			);
		};

		let tree = open(&path, ROTATING);
		tag_matches(&tree, "open");

		write_keys(&tree, 0..3, Durability::Immediate).await;
		tree.core.inner.rotate_memtable().unwrap();
		tag_matches(&tree, "rotating a non-empty memtable");
		tree.core.inner.rotate_memtable().unwrap();
		tag_matches(&tree, "rotating an empty memtable (a no-op)");

		write_keys(&tree, 3..6, Durability::Immediate).await;
		tree.core.inner.seal_active_wal_segment().unwrap();
		tag_matches(&tree, "sealing a non-empty memtable's segment");
		tree.core.inner.seal_active_wal_segment().unwrap();
		tag_matches(&tree, "sealing an empty memtable's segment");

		let arena_fills = write_keys(&tree, 6..60, Durability::Immediate).await.1;
		assert!(!arena_fills.is_empty());
		tag_matches(&tree, "arena-full rotations");

		let big = vec![b'x'; ROTATING + 600];
		let mut txn = tree.begin().unwrap();
		txn.set(key_of(1000), &big).unwrap();
		txn.commit().await.unwrap();
		tag_matches(&tree, "a direct-to-L0 write");

		let checkpoint = dir.path().join("checkpoint");
		tree.create_checkpoint(&checkpoint).unwrap();
		tag_matches(&tree, "a checkpoint");
		write_keys(&tree, 60..70, Durability::Immediate).await;
		tree.restore_from_checkpoint(&checkpoint).unwrap();
		tag_matches(&tree, "a restore");
		let (acked, _) = write_keys(&tree, 100..140, Durability::Immediate).await;
		assert_eq!(acked.len(), 40, "commits work after a restore");

		// And after crash recovery, with and without a recovered memtable.
		let missing = {
			let snap = TempDir::new("wal_rotation_snap").unwrap();
			copy_dir_all(&path, snap.path());
			let recovered = open(snap.path(), ROTATING);
			tag_matches(&recovered, "recovery");
			let missing = missing_in(&recovered, &acked);
			recovered.close().await.unwrap();
			missing
		};
		assert!(missing.is_empty(), "lost after restore: {missing:?}");
		mark_closed(&tree);
	})
	.await;
}

/// Recovery continues in a fresh segment and tags the recovered memtable with it, so a commit
/// after a recovery is logged once, in that segment, and applied in its first round. A tag that
/// lagged behind the segment would find every commit stale and log it again until the group
/// failed.
#[test(tokio::test)]
async fn commits_after_recovery_are_logged_once_in_the_fresh_segment() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = open(&path, NEVER_ROTATING);
		let (mut acked, _) = write_keys(&tree, 0..5, Durability::Immediate).await;
		let crashed = tree.core.inner.wal.read().get_active_log_number();
		release_lock(&tree);
		mark_closed(&tree);
		drop(tree);

		let tree = Arc::new(open(&path, NEVER_ROTATING));
		let fresh = tree.core.inner.wal.read().get_active_log_number();
		assert_eq!(fresh, crashed + 1, "recovery continues in a fresh segment");
		assert_eq!(active_wal(&tree), fresh, "and the recovered memtable is tagged with it");
		assert!(missing_in(&tree, &acked).is_empty(), "the crashed segment was replayed");
		assert!(
			!tree.core.inner.active_memtable.read().unwrap().is_empty(),
			"a recovered memtable"
		);

		let groups = observe_groups(&tree);
		let group = shape_pairs(&[Small; 6], NEVER_ROTATING, 100);
		let results = commit_group(
			&tree,
			Durability::Immediate,
			group.iter().map(|(k, v)| vec![Op::Set(k.clone(), v.clone())]).collect(),
		)
		.await;
		results.iter().for_each(|r| {
			r.as_ref().unwrap();
		});
		assert_eq!(groups.lock().unwrap().sizes, vec![6], "one group of 6");
		assert_eq!(groups.lock().unwrap().repair_rounds, 0, "applied in the first round");
		assert_eq!(active_wal(&tree), fresh, "nothing rotated");

		let index = wal_keys(&path);
		for (k, _) in &group {
			let segments: Vec<u64> =
				index.iter().filter(|(_, keys)| keys.contains(k)).map(|(id, _)| *id).collect();
			assert_eq!(segments, vec![fresh], "{} is logged once, in the fresh segment", {
				String::from_utf8_lossy(k)
			});
		}

		acked.extend(group);
		let missing = crash_by_drop_and_recover(
			Arc::try_unwrap(tree).ok().expect("the writers are done"),
			&path,
			NEVER_ROTATING,
			&acked,
		);
		assert!(missing.is_empty(), "lost on drop without close: {missing:?}");
	})
	.await;
}

// ---------------------------------------------------------------------------
// failure handling
// ---------------------------------------------------------------------------

/// A failed rotation must leave the WAL as it was: the next segment's number is
/// taken only once its file exists, or the WAL number runs ahead of the writer and
/// of every memtable tag.
#[test]
fn wal_rotate_failure_leaves_number_and_writer_consistent() {
	let dir = TempDir::new("wal_rotation").unwrap();
	let mut wal = Wal::open(dir.path(), wal::Options::default()).unwrap();
	wal.append(b"before").unwrap();
	let n = wal.get_active_log_number();

	// An obstacle where the next segment file has to be created.
	let next = dir.path().join(segment_name(n + 1, "wal"));
	std::fs::create_dir(&next).unwrap();
	assert!(wal.rotate().is_err(), "rotation cannot create the next segment");
	assert_eq!(wal.get_active_log_number(), n, "a failed rotation leaves the number alone");

	// The writer is still on segment n and still works.
	wal.append(b"after").unwrap();
	wal.sync().unwrap();
	let current = std::fs::read(dir.path().join(segment_name(n, "wal"))).unwrap();
	assert!(current.windows(5).any(|w| w == b"after"), "the append landed in segment {n}");

	// Once the obstacle is gone the rotation succeeds and moves exactly one on.
	std::fs::remove_dir(&next).unwrap();
	assert_eq!(wal.rotate().unwrap(), n + 1);
	assert_eq!(wal.get_active_log_number(), n + 1);
	wal.append(b"later").unwrap();
	wal.sync().unwrap();
	let rotated = std::fs::read(dir.path().join(segment_name(n + 1, "wal"))).unwrap();
	assert!(rotated.windows(5).any(|w| w == b"later"));
	assert!(!rotated.windows(5).any(|w| w == b"after"));
}

/// If the WAL cannot be rotated while a group needs it (the memtable is full and
/// the next segment cannot be created), the commit fails and the tag still equals
/// the WAL number, so the next commit after the fault is gone just works.
#[test(tokio::test)]
async fn failed_rotation_during_a_commit_fails_cleanly() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = open(&path, ROTATING);
		stop_background_tasks(&tree).await;

		// Fill the arena to just before it rotates.
		let wal = active_wal(&tree);
		let mut acked = fill_until_full(&tree, Durability::Immediate).await;
		let i = acked.len();
		assert_eq!(active_wal(&tree), wal, "the arena is full but has not rotated yet");

		// Block the next segment, then commit the batch that needs a rotation.
		let obstacle = path.join("wal").join(segment_name(wal + 1, "wal"));
		std::fs::create_dir(&obstacle).unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set_durability(Durability::Immediate);
		txn.set(key_of(i), val_of(i)).unwrap();
		assert!(txn.commit().await.is_err(), "the rotation cannot succeed");
		assert_eq!(
			tree.core.inner.wal.read().get_active_log_number(),
			active_wal(&tree),
			"the WAL number and the active memtable's tag must not drift apart"
		);

		// The fault is gone: the next commit works and is durable.
		std::fs::remove_dir(&obstacle).unwrap();
		let (a, rotated) = write_keys(&tree, i + 1..i + 2, Durability::Immediate).await;
		assert!(!rotated.is_empty(), "the next commit rotates once the fault is gone");
		acked.extend(a);
		let missing = crash_copy_and_recover(&path, ROTATING, &acked).await;
		assert!(missing.is_empty(), "lost: {missing:?}");
		mark_closed(&tree);
	})
	.await;
}

/// If the tag of the active memtable never matches the segment a record was
/// written to (a bug elsewhere), the commit must fail after a bounded number of
/// attempts, not spin or hang, and the database must recover once the tag is right.
#[test(tokio::test)]
async fn unresolvable_tag_mismatch_fails_the_commit_instead_of_spinning() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = open(&path, 64 * 1024);

		let (a, _) = write_keys(&tree, 0..1, Durability::Immediate).await;
		let mut acked = a;

		let active = Arc::clone(&*tree.core.inner.active_memtable.read().unwrap());
		let real = active.get_wal_number();
		active.set_wal_number(real + 1000);
		let result = tokio::time::timeout(
			Duration::from_secs(30),
			commit_key(&tree, 1, Durability::Immediate),
		)
		.await
		.expect("a commit that cannot satisfy the tag invariant must fail, not hang");
		assert!(!result.0, "the commit must not be acknowledged against a wrong tag");

		// A bounded number of copies, not one per spin.
		let copies = wal_batches(&path)
			.values()
			.flatten()
			.filter(|b| b.entries.iter().any(|e| e.key == key_of(1)))
			.count();
		assert!((1..=16).contains(&copies), "the failed group was logged {copies} times");

		active.set_wal_number(real);
		let (ok, _) = commit_key(&tree, 2, Durability::Immediate).await;
		assert!(ok, "commits work again once the tag is right");
		acked.push(pair_of(2));
		let missing = crash_copy_and_recover(&path, 64 * 1024, &acked).await;
		assert!(missing.is_empty(), "lost: {missing:?}");
		mark_closed(&tree);
	})
	.await;
}

/// A restore replaces the WAL and the memtables. A group that is already past its
/// WAL sync when one starts must fail, not keep logging and applying into the
/// restored state: its records would otherwise be replayed into the restored
/// database after the next crash.
#[test(tokio::test)]
async fn a_restore_under_way_fails_a_group_that_needs_repair() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let tree = Arc::new(open(&path, 64 * 1024));
		let (earlier, _) = write_keys(&tree, 0..2, Durability::Immediate).await;
		let wal = active_wal(&tree);

		// At the group's WAL sync a restore starts (and the memtable is rotated, as the
		// restore replaces it), so the group's records are stale and the group would
		// need to be logged again.
		let pipeline = Arc::clone(&tree.core.commit_pipeline);
		let inner = Arc::clone(&tree.core.inner);
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
			if matches!(point, PipelineHook::AfterWalSync { .. }) {
				pipeline.restoring.store(true, Ordering::SeqCst);
				inner.rotate_memtable().unwrap();
			}
		})));
		let results = commit_group(&tree, Durability::Immediate, vec![vec![set(10)]]).await;
		assert!(results[0].is_err(), "a group must not be acknowledged across a restore");
		assert_eq!(active_wal(&tree), wal + 1);

		// It was logged once and never again, and it was not applied.
		let copies = wal_batches(&path)
			.values()
			.flatten()
			.filter(|b| b.entries.iter().any(|e| e.key == key_of(10)))
			.count();
		assert_eq!(copies, 1, "nothing may be logged into the WAL that a restore replaces");
		assert!(!missing_in(&tree, &[pair_of(10)]).is_empty(), "the group must not be applied");

		// The restore is over: commits work again.
		tree.core.commit_pipeline.restoring.store(false, Ordering::SeqCst);
		tree.core.commit_pipeline.set_hook(None);
		let (later, _) = write_keys(&tree, 20..22, Durability::Immediate).await;
		assert_eq!(later.len(), 2);
		let expected: Vec<_> = earlier.iter().chain(&later).cloned().collect();
		let missing = crash_copy_and_recover(&path, 64 * 1024, &expected).await;
		assert!(missing.is_empty(), "lost: {missing:?}");
		mark_closed(&tree);
	})
	.await;
}

// ---------------------------------------------------------------------------
// stress: writers racing rotations, flushes and checkpoints
// ---------------------------------------------------------------------------

/// One consistent crash image taken while the run was in progress, with the
/// commits that had been acknowledged when it was taken.
struct Image {
	dir: PathBuf,
	acked: Vec<Pair>,
}

/// Writers on a multi-threaded runtime race a foreign actor that keeps doing what
/// a checkpoint or `Tree::flush` does to the memtables: rotate, rotate and flush,
/// take a real checkpoint. Every few rounds the actor also pauses the writers
/// (waits for their in-flight commits, then holds them off), lets the cleanup of
/// flushed segments finish and takes a crash image, so images are taken in
/// states where groups straddled foreign rotations and flushes. Afterwards:
/// * every image recovers every key acknowledged when it was taken,
/// * the final crash image recovers every acknowledged key, and scans return each key once, forward
///   and backward,
/// * every checkpoint opens and holds what was acknowledged before it began.
/// The whole scenario runs under a watchdog, so a deadlock fails it.
///
/// Only the foreign actor flushes (the background tasks are stopped): a background
/// flush racing a checkpoint's flush on the same immutable memtable is a separate,
/// existing hazard that this test is not about.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn writers_racing_rotations_flushes_and_checkpoints_lose_nothing() {
	within(async {
		let dir = TempDir::new("wal_rotation").unwrap();
		let path = dir.path().to_path_buf();
		let max_mt = 8192;
		let tree = Arc::new(open(&path, max_mt));
		stop_background_tasks(&tree).await;

		let scratch = TempDir::new("wal_rotation_scratch").unwrap();
		let gate = Arc::new(tokio::sync::RwLock::new(()));
		let acked_all: Arc<Mutex<Vec<Pair>>> = Arc::new(Mutex::new(Vec::new()));
		let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let runtime = tokio::runtime::Handle::current();

		let foreign = {
			let tree = Arc::clone(&tree);
			let gate = Arc::clone(&gate);
			let acked_all = Arc::clone(&acked_all);
			let done = Arc::clone(&done);
			let scratch = scratch.path().to_path_buf();
			let path = path.clone();
			std::thread::spawn(move || {
				// Flushing spawns the WAL cleanup on the runtime.
				let _entered = runtime.enter();
				let mut images = Vec::new();
				let mut checkpoints = Vec::new();
				let mut round = 0usize;
				while !done.load(Ordering::Acquire) {
					match round % 4 {
						0 => tree.core.inner.rotate_memtable().unwrap(),
						1 => {
							let before = acked_all.lock().unwrap().clone();
							let cp = scratch.join(format!("checkpoint{round}"));
							tree.create_checkpoint(&cp).expect("create_checkpoint");
							checkpoints.push((cp, before));
						}
						2 => {
							tree.core.inner.rotate_memtable().unwrap();
							tree.core.inner.flush_all_immutables_sync().unwrap();
						}
						_ if round % 8 == 3 => {
							let _paused = runtime.block_on(gate.write());
							let acked = acked_all.lock().unwrap().clone();
							// A crash long after the flushes: their cleanup has run.
							wait_for_cleanup_blocking(&tree, &path);
							let image = scratch.join(format!("image{round}"));
							copy_dir_all(&path, &image);
							images.push(Image {
								dir: image,
								acked,
							});
						}
						_ => {}
					}
					round += 1;
					std::thread::sleep(Duration::from_millis(4));
				}
				(images, checkpoints, round)
			})
		};

		let writers = 6usize;
		let mut handles = Vec::new();
		for w in 0..writers {
			let tree = Arc::clone(&tree);
			let gate = Arc::clone(&gate);
			let acked_all = Arc::clone(&acked_all);
			handles.push(tokio::spawn(async move {
				let durability = if w % 2 == 0 {
					Durability::Immediate
				} else {
					Durability::Eventual
				};
				for j in 0..60 {
					let i = w * 1000 + j;
					let _running = gate.read().await;
					let mut txn = tree.begin().unwrap();
					txn.set_durability(durability);
					txn.set(key_of(i), val_of(i)).unwrap();
					txn.commit().await.unwrap();
					acked_all.lock().unwrap().push(pair_of(i));
				}
			}));
		}
		for h in handles {
			h.await.unwrap();
		}
		done.store(true, Ordering::Release);
		let (images, checkpoints, rounds) = foreign.join().unwrap();
		println!(
			"stress: {rounds} foreign rounds, {} crash images, {} checkpoints, WAL segment {}",
			images.len(),
			checkpoints.len(),
			active_wal(&tree)
		);
		assert!(rounds >= 8, "the foreign actor must have raced the writers, rounds {rounds}");
		assert!(!images.is_empty() && !checkpoints.is_empty());

		let acked: Vec<_> = acked_all.lock().unwrap().clone();
		assert_eq!(acked.len(), writers * 60);
		assert!(missing_in(&tree, &acked).is_empty(), "the live tree lost acknowledged writes");

		for (n, image) in images.iter().enumerate() {
			let missing = recover_copy(&image.dir, max_mt, &image.acked).await;
			assert!(
				missing.is_empty(),
				"crash image {n} of {} lost {} of the {} writes acknowledged when it was taken: {:?}",
				images.len(),
				missing.len(),
				image.acked.len(),
				&missing[..missing.len().min(5)]
			);
		}

		// The final crash image, with scans.
		wait_for_cleanup_blocking(&tree, &path);
		let last = scratch.path().join("final");
		copy_dir_all(&path, &last);
		let recovered = open(&last, max_mt);
		let missing = missing_in(&recovered, &acked);
		assert!(missing.is_empty(), "final crash image lost acknowledged writes: {missing:?}");
		let mut expected = acked.clone();
		expected.sort();
		let txn = recovered.begin_with_mode(Mode::ReadOnly).unwrap();
		let forward = collect_transaction_all(&mut txn.iter().unwrap()).unwrap();
		let mut backward = collect_transaction_reverse(&mut txn.iter().unwrap()).unwrap();
		backward.reverse();
		assert_eq!(forward, expected, "forward scan after recovery");
		assert_eq!(backward, expected, "backward scan after recovery");
		drop(txn);
		recovered.close().await.unwrap();

		// Every checkpoint opens and holds what was acknowledged before it began.
		for (cp, before) in &checkpoints {
			let copy = TempDir::new("wal_rotation_cp").unwrap();
			copy_dir_all(cp, copy.path());
			let opened = open(copy.path(), max_mt);
			let missing = missing_in(&opened, before);
			assert!(
				missing.is_empty(),
				"checkpoint {cp:?} lacks {} keys acknowledged before it began: {:?}",
				missing.len(),
				&missing[..missing.len().min(5)]
			);
			opened.close().await.unwrap();
		}
		mark_closed(&tree);
	})
	.await;
}
