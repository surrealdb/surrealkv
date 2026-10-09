//! A commit group that fails after part of it was applied stops the database.
//!
//! The flusher applies a group to the memtables in rounds: a memtable that fills up rotates, a
//! batch whose record is in an older segment is logged again, an oversized batch goes to an L0
//! table. A failure in a later round leaves the batches of the earlier ones in the memtables,
//! where the next group that publishes a higher sequence number would make them readable: a commit
//! that was reported as failed would take effect, and be written to a table.
//!
//! A group that fails once any of it is applied therefore stops the database. Every committer of
//! the group gets `Error::DatabaseStopped`, so does every commit that comes after and every one
//! that was accepted behind the group, and nothing is published past it: the batches that were
//! applied stay out of sight of every reader of the process. What readers could see before still
//! reads. Nothing moves from the memtables to a table and nothing is compacted either, and `close`
//! leaves the memtables to the WAL, so that recovery finds the whole group in its records.
//!
//! These tests fail the group at every step that can come after the first batch was applied, with
//! Immediate, Eventual and mixed groups, and check what readers, later commits, the committers
//! queued behind the group, writers stalled at the time, flushes, compactions, checkpoints and
//! `close` see, and what a crash image recovers.

use std::collections::BTreeMap;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;

use super::{collect_transaction_all, collect_transaction_reverse};
use crate::compaction::leveled::Strategy;
use crate::lsm::{CompactionOperations, FlushHook};
use crate::ring::{CommitStage, PipelineHook, UNFENCED_STALE_ROUNDS};
use crate::{BackgroundErrorReason, Durability, Error, ErrorSeverity, Mode, Options, Result, Tree};

/// A memtable that holds about 21 small commits, so that a group of 30 fills it part-way.
const SMALL: usize = 4096;

/// How long a scenario may take before it is declared stuck.
const DEADLINE: Duration = Duration::from_secs(90);

/// The value of a key that was committed before the group.
const OLD: u8 = b'b';

/// The value the group writes.
const NEW: u8 = b'g';

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Runs `scenario` on a thread of its own and fails, instead of blocking, if it does not end
/// within `DEADLINE`: a committer that is never answered blocks its task for good.
fn bounded<F>(scenario: F)
where
	F: Future<Output = ()> + Send + 'static,
{
	let (tx, rx) = mpsc::channel();
	std::thread::spawn(move || {
		let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
		let outcome =
			std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| runtime.block_on(scenario)));
		let _ = tx.send(outcome);
	});
	match rx.recv_timeout(DEADLINE) {
		Ok(Ok(())) => {}
		Ok(Err(panic)) => std::panic::resume_unwind(panic),
		Err(_) => panic!("the scenario did not end within {DEADLINE:?}: a committer is stuck"),
	}
}

fn opts(path: &Path, flush_on_close: bool) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		max_memtable_size: SMALL,
		flush_on_close,
		// No background flush runs in these tests, so a rotation that queues a memtable must not
		// stall the commits behind it.
		memtable_stall_threshold: 1000,
		..Default::default()
	})
}

/// A tree whose background tasks are stopped, so that nothing is flushed or removed behind the
/// test's back, and that is marked closed: a test that fails half way must not leave a spawned
/// `close()` to stop the task manager a second time.
async fn open(opts: Arc<Options>) -> Arc<Tree> {
	let tree = Arc::new(Tree::new(opts).unwrap());
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

fn seg_path(path: &Path, id: u64) -> PathBuf {
	path.join("wal").join(format!("{id:020}.wal"))
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

/// The ids of the WAL segments of `path` that hold at least one byte.
fn non_empty_segments(path: &Path) -> Vec<u64> {
	let mut found: Vec<u64> = fs::read_dir(path.join("wal"))
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

fn files_with_extension(path: &Path, dir: &str, extension: &str) -> Vec<String> {
	let mut names: Vec<String> = fs::read_dir(path.join(dir))
		.unwrap()
		.map(|e| e.unwrap().file_name().to_string_lossy().to_string())
		.filter(|name| name.ends_with(extension))
		.collect();
	names.sort();
	names
}

type Entries = Vec<(String, Vec<u8>)>;

/// Ten keys that are committed, and acknowledged, before the group.
fn base() -> Entries {
	(0..10).map(|i| (format!("base{i}"), vec![OLD; 40])).collect()
}

/// `count` commits that overwrite the first three keys of `base` and add the others.
fn numbered(count: usize) -> Entries {
	(0..count)
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

/// A group of 30 commits: with `SMALL`, it fills a memtable part-way.
fn group() -> Entries {
	numbered(30)
}

/// A group of 20 commits with an oversized one in the middle, which bypasses the memtable.
fn group_with_oversized() -> Entries {
	let mut entries = numbered(20);
	entries[10] = ("big10".to_string(), vec![NEW; SMALL + 1000]);
	entries
}

/// How the commits of a group ask for durability.
#[derive(Clone, Copy, Debug)]
enum Mix {
	Eventual,
	Immediate,
	/// Every other commit is Immediate.
	Mixed,
}

impl Mix {
	fn of(self, i: usize) -> Durability {
		match self {
			Mix::Eventual => Durability::Eventual,
			Mix::Immediate => Durability::Immediate,
			Mix::Mixed if i % 2 == 0 => Durability::Immediate,
			Mix::Mixed => Durability::Eventual,
		}
	}
}

const ALL_MIXES: [Mix; 3] = [Mix::Eventual, Mix::Immediate, Mix::Mixed];

/// The mixes whose group is fsynced.
const SYNCED_MIXES: [Mix; 2] = [Mix::Immediate, Mix::Mixed];

/// One single-key transaction per entry, all spawned before the flusher is polled: on the
/// current-thread runtime that is one commit group, in order.
async fn commit_group(tree: &Arc<Tree>, entries: &Entries, mix: Mix) -> Vec<Result<()>> {
	let mut handles = Vec::new();
	for (i, (key, value)) in entries.iter().enumerate() {
		handles.push(tokio::spawn(commit_one(
			Arc::clone(tree),
			key.clone(),
			value.clone(),
			mix.of(i),
		)));
	}
	let mut out = Vec::new();
	for handle in handles {
		out.push(handle.await.unwrap());
	}
	out
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

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key.as_bytes()).unwrap()
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

/// The keys the group introduced (the ones that are not in `base`) that the active or an
/// immutable memtable holds, whatever their sequence number: what the failed group left behind.
fn in_memtables(tree: &Tree, entries: &Entries) -> Vec<String> {
	let inner = &tree.core.inner;
	let mut memtables = vec![Arc::clone(&inner.active_memtable.read().unwrap())];
	memtables
		.extend(inner.immutable_memtables.read().unwrap().iter().map(|e| Arc::clone(&e.memtable)));
	fresh(entries)
		.into_iter()
		.filter(|key| memtables.iter().any(|m| m.get(key.as_bytes(), None).is_some()))
		.collect()
}

/// The keys of `entries` that are not in `base`.
fn fresh(entries: &Entries) -> Vec<String> {
	entries.iter().map(|(key, _)| key.clone()).filter(|key| !key.starts_with("base")).collect()
}

/// The error every commit of the stopped database gets.
fn stop_error(result: &Result<()>, what: &str) -> String {
	match result {
		Err(Error::DatabaseStopped(message)) => {
			assert!(message.contains("decided by recovery"), "{what}: {message}");
			result.as_ref().unwrap_err().to_string()
		}
		other => panic!("{what}: expected the database to be stopped, got {other:?}"),
	}
}

/// What a crash image of the live directory holds: the copy is recovered and scanned.
fn recover_image(live: &Path) -> BTreeMap<String, Vec<u8>> {
	let root = TempDir::new("group_fail_stop_image").unwrap();
	let image = root.path().join("image");
	copy_dir(live, &image);
	let recovered = Tree::new(opts(&image, false)).unwrap();
	let all = scan(&recovered);
	crash(&recovered);
	all
}

/// The recovered state after the group: every acknowledged commit, and the group whole.
fn assert_group_recovered(all: &BTreeMap<String, Vec<u8>>, group: &Entries, what: &str) {
	let mut expected: BTreeMap<String, Vec<u8>> = base().into_iter().collect();
	expected.extend(group.iter().cloned());
	assert_eq!(all.len(), expected.len(), "{what}: keys after recovery");
	for (key, value) in &expected {
		assert_eq!(all.get(key), Some(value), "{what}: {key} after recovery");
	}
}

// ---------------------------------------------------------------------------
// the steps after the first batch was applied
// ---------------------------------------------------------------------------

/// A failure injected at a step of a group that comes after its first batch was applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Failure {
	/// The append that logs the rest of the group again, in the segment the rotation opened,
	/// fails.
	ReappendWrite,
	/// The fsync of that re-append fails.
	ReappendSync,
	/// The fsync of the old segment, in the rotation in the middle of the group, fails.
	RotationSync,
	/// The rotation in the middle of the group cannot create the new segment.
	RotationCreate,
	/// The write of an oversized batch to an L0 table fails, after batches before it were applied.
	DirectL0,
	/// The write of the second of two oversized batches fails: the first is in an L0 table, and
	/// nothing is in a memtable.
	SecondDirectL0,
	/// An oversized batch went to an L0 table, and then the re-append of the batches after it
	/// fails.
	AfterDirectL0,
	/// The flush of the memtable that an oversized batch sealed, which comes before the batch is
	/// written, fails after the batches in front of it were applied to that memtable.
	FlushBeforeDirectL0,
	/// The append of a fenced round fails.
	FencedWrite,
	/// The fsync of a fenced round fails.
	FencedSync,
}

impl Failure {
	fn group(self) -> Entries {
		match self {
			Failure::DirectL0 | Failure::AfterDirectL0 | Failure::FlushBeforeDirectL0 => {
				group_with_oversized()
			}
			Failure::SecondDirectL0 => {
				vec![
					("big0".to_string(), vec![NEW; SMALL + 1000]),
					("big1".to_string(), vec![NEW; SMALL + 1000]),
				]
			}
			_ => group(),
		}
	}

	/// The apply rounds the group gets through before it fails, the failing one included.
	fn rounds(self) -> usize {
		match self {
			Failure::ReappendWrite | Failure::ReappendSync | Failure::AfterDirectL0 => 2,
			Failure::RotationSync
			| Failure::RotationCreate
			| Failure::DirectL0
			| Failure::FlushBeforeDirectL0 => 1,
			Failure::SecondDirectL0 => 2,
			// Rounds 1 to `UNFENCED_STALE_ROUNDS + 1` are overtaken by a rotation, the next is
			// fenced.
			Failure::FencedWrite | Failure::FencedSync => UNFENCED_STALE_ROUNDS as usize + 3,
		}
	}

	/// A part of the error that failed the group, if the failpoint says one.
	fn cause(self) -> Option<&'static str> {
		match self {
			Failure::ReappendWrite | Failure::AfterDirectL0 | Failure::FencedWrite => {
				Some("injected write failure")
			}
			Failure::FlushBeforeDirectL0 => Some("injected flush failure"),
			Failure::ReappendSync | Failure::RotationSync | Failure::FencedSync => {
				Some("injected fsync failure")
			}
			// The operating system says why a file cannot be created where a directory is.
			Failure::RotationCreate | Failure::DirectL0 | Failure::SecondDirectL0 => None,
		}
	}
}

/// What an injected failure leaves for the test to look at and clean up.
#[derive(Default)]
struct Injected {
	/// Apply rounds started.
	rounds: AtomicUsize,
	/// Directories that stand where the file a step creates should go.
	blockers: Mutex<Vec<PathBuf>>,
}

impl Injected {
	fn remove_blockers(&self) {
		for blocker in self.blockers.lock().unwrap().drain(..) {
			fs::remove_dir(blocker).unwrap();
		}
	}
}

/// Installs the observer that makes `failure` happen. The hook of a step that cannot fail by
/// itself arms a failpoint of the WAL, or puts a directory where a file is to be created.
fn inject(tree: &Tree, failure: Failure) -> Arc<Injected> {
	let injected = Arc::new(Injected::default());
	let seen = Arc::clone(&injected);
	let inner = Arc::clone(&tree.core.inner);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| match point {
		PipelineHook::BeforeApplyRound {
			round,
		} => {
			seen.rounds.fetch_add(1, Ordering::SeqCst);
			let fenced_round = UNFENCED_STALE_ROUNDS as usize + 2;
			match failure {
				Failure::ReappendWrite | Failure::AfterDirectL0 if round == 1 => {
					inner.wal.write().fail_writes_after(0)
				}
				Failure::ReappendSync if round == 1 => inner.wal.write().fail_syncs_after(0),
				Failure::RotationCreate if round == 0 => {
					let next = inner.wal.read().get_active_log_number() + 1;
					let blocker = seg_path(&inner.opts.path, next);
					fs::create_dir(&blocker).unwrap();
					seen.blockers.lock().unwrap().push(blocker);
				}
				Failure::FlushBeforeDirectL0 if round == 0 => {
					let hook: FlushHook =
						Arc::new(|_| Err(Error::Other("injected flush failure".into())));
					*inner.flush_hook.lock() = Some(hook);
				}
				Failure::DirectL0 | Failure::SecondDirectL0 if round == 0 => {
					// Table ids come from a counter. Sealing a memtable that holds something queues
					// it, which takes an id, and each table written takes one. The oversized batch
					// that is to fail comes after the memtable that holds what precedes it was
					// queued: the one the group's small batches are in or, if the group starts
					// with oversized ones, the one that holds the base keys.
					let next =
						inner.level_manifest.read().unwrap().next_table_id.load(Ordering::SeqCst);
					let queued_before = !inner.active_memtable.read().unwrap().is_empty() as u64;
					let blocked = match failure {
						Failure::SecondDirectL0 => next + 1 + queued_before,
						_ => next + 1,
					};
					let blocker = inner.opts.sstable_file_path(blocked);
					fs::create_dir(&blocker).unwrap();
					seen.blockers.lock().unwrap().push(blocker);
				}
				// Every round before the fenced one is overtaken by a rotation, as a checkpoint
				// would, so the group gets stale again and again until a fenced round takes it.
				Failure::FencedWrite | Failure::FencedSync => {
					if (1..fenced_round).contains(&round) {
						inner.seal_active_wal_segment().unwrap();
					} else if round == fenced_round {
						match failure {
							Failure::FencedWrite => inner.wal.write().fail_writes_after(0),
							_ => inner.wal.write().fail_syncs_after(0),
						}
					}
				}
				_ => {}
			}
		}
		PipelineHook::AfterWalSync {
			..
		} if failure == Failure::RotationSync => inner.wal.write().fail_syncs_after(0),
		_ => {}
	})));
	injected
}

/// Fails a group at `failure`, and checks what the committers, a reader, a later commit and a
/// recovery of a crash image see of it.
async fn run(failure: Failure, mix: Mix) {
	let what = format!("{failure:?} {mix:?}");
	let dir = TempDir::new("group_fail_stop").unwrap();
	let tree = open(opts(dir.path(), false)).await;

	for result in commit_group(&tree, &base(), Mix::Immediate).await {
		result.unwrap();
	}
	let visible = tree.core.seq_num();
	let before = scan(&tree);
	assert_eq!(before.len(), 10);

	let group = failure.group();
	let injected = inject(&tree, failure);
	let results = commit_group(&tree, &group, mix).await;
	tree.core.commit_pipeline.set_hook(None);
	*tree.core.inner.flush_hook.lock() = None;
	injected.remove_blockers();

	// Every committer of the group is told, whatever it asked for, and told why.
	assert_eq!(results.len(), group.len());
	let errors: Vec<String> = results.iter().map(|r| stop_error(r, &what)).collect();
	assert!(errors.windows(2).all(|w| w[0] == w[1]), "{what}: one error for the whole group");
	if let Some(cause) = failure.cause() {
		assert!(errors[0].contains(cause), "{what}: the cause is kept: {}", errors[0]);
	}
	assert_eq!(injected.rounds.load(Ordering::SeqCst), failure.rounds(), "{what}: rounds");

	// The failure came after a batch was applied: something of the group is in a memtable, or, once
	// an oversized batch was written, in an L0 table. That write flushes the memtable it seals
	// first, so the batches before it are in a table too, and no memtable holds any of the group.
	let tables = tree.core.inner.l0_file_count();
	match failure {
		Failure::DirectL0 => {
			assert_eq!(tables, 1, "{what}: the batches before the oversized one are in a table");
			assert!(in_memtables(&tree, &group).is_empty(), "{what}");
		}
		Failure::AfterDirectL0 => {
			assert_eq!(
				tables, 2,
				"{what}: the batches before the oversized one and the oversized batch are in tables"
			);
			assert!(in_memtables(&tree, &group).is_empty(), "{what}");
		}
		Failure::SecondDirectL0 => {
			assert_eq!(
				tables, 2,
				"{what}: the base keys, which the first batch sealed and flushed, and the first batch \
				 are in tables"
			);
			assert!(in_memtables(&tree, &group).is_empty(), "{what}");
		}
		Failure::FlushBeforeDirectL0 => {
			assert_eq!(tables, 0, "{what}: the flush failed, so no table was written");
			assert_eq!(
				tree.core.inner.immutable_count(),
				1,
				"{what}: the memtable of the batches before the oversized one is still queued"
			);
			let applied = in_memtables(&tree, &group);
			assert!(!applied.is_empty(), "{what}: nothing was applied before the failure");
			assert!(applied.len() < fresh(&group).len(), "{what}: the group was applied in full");
		}
		_ => {
			let applied = in_memtables(&tree, &group);
			assert!(!applied.is_empty(), "{what}: nothing was applied before the failure");
			assert!(applied.len() < fresh(&group).len(), "{what}: the group was applied in full");
		}
	}

	// The database is stopped, for the reason and with the severity that say so.
	let background = tree.core.inner.error_handler.get_error().expect("a background error");
	assert_eq!(background.reason, BackgroundErrorReason::CommitGroup, "{what}");
	assert_eq!(background.severity, ErrorSeverity::Unrecoverable, "{what}");
	assert!(tree.core.inner.error_handler.is_db_stopped(), "{what}");

	// Nothing was published past what readers could see.
	assert_eq!(tree.core.seq_num(), visible, "{what}: the visible sequence number moved");

	// None of the group is readable, by a new transaction or an iterator, and what was committed
	// before still reads, the keys the group overwrote with the values they had.
	let assert_unchanged = |when: &str| {
		for key in fresh(&group) {
			assert_eq!(get(&tree, &key), None, "{what} {when}: {key} is readable");
		}
		for (key, value) in &before {
			assert_eq!(get(&tree, key).as_ref(), Some(value), "{what} {when}: {key}");
		}
		assert_eq!(scan(&tree), before, "{what} {when}: an iterator sees the group");
	};
	assert_unchanged("right after the failure");

	// Every later commit fails with the same error, and so does one in a segment of its own, which
	// would have published a sequence number above the group's.
	for durability in [Durability::Eventual, Durability::Immediate] {
		let later = commit_one(Arc::clone(&tree), "later".into(), vec![NEW; 40], durability).await;
		assert_eq!(stop_error(&later, &what), errors[0], "{what} {durability:?}");
	}
	let _ = tree.core.inner.seal_active_wal_segment();
	let later = commit_one(Arc::clone(&tree), "later".into(), vec![NEW; 40], Durability::Immediate);
	assert_eq!(stop_error(&later.await, &what), errors[0], "{what}");
	assert_eq!(get(&tree, "later"), None, "{what}");
	assert_unchanged("after later commits");
	assert_eq!(tree.core.seq_num(), visible, "{what}: a later commit moved the visible number");

	// A recovery of a crash image finds every acknowledged commit and the group whole: its
	// records were all in the WAL before anything was applied.
	let recovered = recover_image(dir.path());
	assert_group_recovered(&recovered, &group, &what);

	// The database still shuts down, the WAL having been rotated to a segment of its own by now.
	tree.core.is_closed.store(false, Ordering::SeqCst);
	tree.core.task_manager.lock().unwrap().take();
	tokio::time::timeout(Duration::from_secs(30), tree.core.close())
		.await
		.unwrap_or_else(|_| panic!("{what}: close does not end"))
		.unwrap_or_else(|e| panic!("{what}: close of a stopped database failed: {e}"));
}

#[test(tokio::test)]
async fn a_failed_reappend_after_part_of_the_group_was_applied_stops_the_database() {
	for mix in ALL_MIXES {
		run(Failure::ReappendWrite, mix).await;
	}
}

#[test(tokio::test)]
async fn a_failed_fsync_of_the_reappend_after_part_of_the_group_was_applied_stops_the_database() {
	for mix in SYNCED_MIXES {
		run(Failure::ReappendSync, mix).await;
	}
}

/// Only a group that is not fsynced has anything pending in the old segment when the rotation
/// syncs it.
#[test(tokio::test)]
async fn a_failed_fsync_of_the_old_segment_in_a_mid_group_rotation_stops_the_database() {
	run(Failure::RotationSync, Mix::Eventual).await;
}

#[test(tokio::test)]
async fn a_mid_group_rotation_that_cannot_open_its_segment_stops_the_database() {
	for mix in ALL_MIXES {
		run(Failure::RotationCreate, mix).await;
	}
}

#[test(tokio::test)]
async fn a_failed_direct_write_to_l0_after_part_of_the_group_was_applied_stops_the_database() {
	for mix in ALL_MIXES {
		run(Failure::DirectL0, mix).await;
	}
}

/// The flush that comes before the write of an oversized batch fails with part of the group applied
/// to the memtable it flushes: the group stops the database like any other that fails then, and
/// the memtable stays queued.
#[test(tokio::test)]
async fn a_failed_flush_before_a_direct_write_after_part_of_the_group_was_applied_stops_the_database(
) {
	for mix in ALL_MIXES {
		run(Failure::FlushBeforeDirectL0, mix).await;
	}
}

#[test(tokio::test)]
async fn a_failed_second_direct_write_to_l0_leaves_the_first_one_out_of_sight() {
	for mix in ALL_MIXES {
		run(Failure::SecondDirectL0, mix).await;
	}
}

#[test(tokio::test)]
async fn a_failed_reappend_after_a_direct_write_to_l0_stops_the_database() {
	for mix in ALL_MIXES {
		run(Failure::AfterDirectL0, mix).await;
	}
}

#[test(tokio::test)]
async fn a_failed_fenced_append_after_part_of_the_group_was_applied_stops_the_database() {
	for mix in ALL_MIXES {
		run(Failure::FencedWrite, mix).await;
	}
}

#[test(tokio::test)]
async fn a_failed_fenced_fsync_after_part_of_the_group_was_applied_stops_the_database() {
	for mix in SYNCED_MIXES {
		run(Failure::FencedSync, mix).await;
	}
}

/// A range delete and a delete in the part of the group that was applied must hide nothing from
/// the readers of the stopped database, whose view stops short of them, and take effect, with the
/// rest of the group, when it is recovered.
#[test(tokio::test)]
async fn deletes_of_a_failed_group_hide_nothing() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let tree = open(opts(dir.path(), false)).await;
	commit_group(&tree, &base(), Mix::Immediate).await.into_iter().for_each(|r| r.unwrap());
	let before = scan(&tree);

	inject(&tree, Failure::ReappendWrite);
	let mut handles = Vec::new();
	for i in 0..30 {
		let tree = Arc::clone(&tree);
		handles.push(tokio::spawn(async move {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(Durability::Immediate);
			match i {
				5 => txn.delete_range(&b"base3"[..], &b"base8"[..]).unwrap(),
				6 => txn.delete(&b"base9"[..]).unwrap(),
				_ => txn.set(format!("group{i}").as_bytes(), &[NEW; 40]).unwrap(),
			}
			txn.commit().await
		}));
	}
	for handle in handles {
		stop_error(&handle.await.unwrap(), "the group");
	}
	tree.core.commit_pipeline.set_hook(None);

	let tombstones_applied = tree
		.core
		.inner
		.immutable_memtables
		.read()
		.unwrap()
		.iter()
		.any(|e| e.memtable.has_range_deletions() && e.memtable.get(b"base9", None).is_some());
	assert!(tombstones_applied, "the deletes were applied before the failure");
	let later = commit_one(Arc::clone(&tree), "later".into(), vec![NEW; 40], Durability::Immediate);
	stop_error(&later.await, "a later commit");
	assert_eq!(scan(&tree), before, "a delete of the group hides a key readers see");
	for (key, value) in &before {
		assert_eq!(get(&tree, key).as_ref(), Some(value), "{key}");
	}

	// The range delete covers `base3` up to, and not including, `base8`.
	let mut expected: BTreeMap<String, Vec<u8>> =
		[0, 1, 2, 8].iter().map(|i| (format!("base{i}"), vec![OLD; 40])).collect();
	expected.extend(
		(0..30).filter(|i| ![5, 6].contains(i)).map(|i| (format!("group{i}"), vec![NEW; 40])),
	);
	assert_eq!(recover_image(dir.path()), expected, "the group, deletes included, recovered whole");
	crash(&tree);
}

// ---------------------------------------------------------------------------
// what a group that fails before anything is applied keeps doing
// ---------------------------------------------------------------------------

/// A group that fails before any of it is applied leaves nothing behind, and the database keeps
/// taking commits: the stop is for a group that cannot be taken back.
#[test(tokio::test)]
async fn a_group_that_fails_before_anything_is_applied_does_not_stop_the_database() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let tree = open(opts(dir.path(), false)).await;
	commit_group(&tree, &base(), Mix::Immediate).await.into_iter().for_each(|r| r.unwrap());

	// The append of the whole group fails.
	tree.core.inner.wal.write().fail_writes_after(0);
	let results = commit_group(&tree, &group(), Mix::Immediate).await;
	for result in &results {
		let error = result.as_ref().unwrap_err();
		assert!(!matches!(error, Error::DatabaseStopped(_)), "{error}");
	}
	assert!(tree.core.inner.error_handler.check_error().is_ok());
	assert!(in_memtables(&tree, &group()).is_empty());

	// A direct write to L0 of the first batch fails.
	tree.core.inner.seal_active_wal_segment().unwrap();
	let next = tree.core.inner.level_manifest.read().unwrap().next_table_id.load(Ordering::SeqCst);
	let blocker = tree.core.inner.opts.sstable_file_path(next);
	fs::create_dir(&blocker).unwrap();
	let oversized = vec![("big0".to_string(), vec![NEW; SMALL + 1000])];
	let results = commit_group(&tree, &oversized, Mix::Immediate).await;
	let error = results[0].as_ref().unwrap_err();
	assert!(!matches!(error, Error::DatabaseStopped(_)), "{error}");
	fs::remove_dir(blocker).unwrap();
	assert!(tree.core.inner.error_handler.check_error().is_ok());

	// Commits go on once the WAL rotated.
	commit_one(Arc::clone(&tree), "later".into(), vec![NEW; 40], Durability::Immediate)
		.await
		.unwrap();
	assert_eq!(get(&tree, "later"), Some(vec![NEW; 40]));
	assert_eq!(get(&tree, "big0"), None);
	crash(&tree);
}

// ---------------------------------------------------------------------------
// who else is waiting when the group fails
// ---------------------------------------------------------------------------

/// A second runtime for the commits that have to run while the flusher's thread is held in a
/// hook.
fn side_runtime() -> tokio::runtime::Runtime {
	tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().unwrap()
}

/// Committers that are accepted while the group is being applied, behind it in the ring, all get
/// an error, and promptly: the flusher fails them instead of publishing them past the group.
#[test]
fn committers_accepted_behind_a_failing_group_all_get_the_stop_error() {
	const QUEUED: usize = 8;
	let side = side_runtime();
	let handle = side.handle().clone();
	bounded(async move {
		let dir = TempDir::new("group_fail_stop").unwrap();
		let tree = open(opts(dir.path(), true)).await;
		commit_group(&tree, &base(), Mix::Immediate).await.into_iter().for_each(|r| r.unwrap());
		let visible = tree.core.seq_num();

		let accepted = Arc::new(AtomicUsize::new(0));
		let counter = Arc::clone(&accepted);
		tree.core.commit_pipeline.set_commit_hook(Some(Arc::new(move |stage| {
			if stage == CommitStage::Accepted {
				counter.fetch_add(1, Ordering::SeqCst);
			}
		})));

		// Once the group has been applied up to the fill and rotated, while the flusher is held,
		// more commits are accepted. Then the re-append fails.
		let queued = Arc::new(Mutex::new(Vec::new()));
		let all_accepted = Arc::new(AtomicBool::new(true));
		{
			let (inner, tree, queued, accepted, all_accepted) = (
				Arc::clone(&tree.core.inner),
				Arc::clone(&tree),
				Arc::clone(&queued),
				Arc::clone(&accepted),
				Arc::clone(&all_accepted),
			);
			tree.core.commit_pipeline.set_hook(Some(Arc::new({
				let tree = Arc::clone(&tree);
				move |point| {
					if let PipelineHook::BeforeApplyRound {
						round: 1,
					} = point
					{
						inner.wal.write().fail_writes_after(0);
						let wanted = accepted.load(Ordering::SeqCst) + QUEUED;
						for i in 0..QUEUED {
							let tree = Arc::clone(&tree);
							queued.lock().unwrap().push(handle.spawn(commit_one(
								tree,
								format!("queued{i}"),
								vec![NEW; 40],
								Durability::Immediate,
							)));
						}
						let started = Instant::now();
						while accepted.load(Ordering::SeqCst) < wanted {
							if started.elapsed() > Duration::from_secs(30) {
								all_accepted.store(false, Ordering::SeqCst);
								break;
							}
							std::thread::sleep(Duration::from_millis(1));
						}
					}
				}
			})));
		}

		let results = commit_group(&tree, &group(), Mix::Mixed).await;
		tree.core.commit_pipeline.set_hook(None);
		assert!(all_accepted.load(Ordering::SeqCst), "the queued commits were never accepted");
		let first = stop_error(&results[0], "the group");
		for result in &results {
			assert_eq!(stop_error(result, "the group"), first);
		}

		let handles: Vec<_> = queued.lock().unwrap().drain(..).collect();
		assert_eq!(handles.len(), QUEUED);
		for handle in handles {
			let result = tokio::time::timeout(Duration::from_secs(30), handle)
				.await
				.expect("a queued committer was never answered")
				.unwrap();
			assert_eq!(stop_error(&result, "a queued commit"), first);
		}

		assert_eq!(tree.core.seq_num(), visible);
		for i in 0..QUEUED {
			assert_eq!(get(&tree, &format!("queued{i}")), None);
		}
		assert_eq!(scan(&tree).len(), 10, "only what was visible before reads");

		// The database still shuts down, with every committer answered.
		tree.core.is_closed.store(false, Ordering::SeqCst);
		tree.core.task_manager.lock().unwrap().take();
		tokio::time::timeout(Duration::from_secs(30), tree.core.close())
			.await
			.expect("close does not end")
			.unwrap();
	});
	drop(side);
}

/// A writer that is stalled for a flush when the group fails would wait for ever, because the
/// stopped database does not flush: the stop wakes it.
#[test]
fn a_writer_stalled_when_the_group_fails_is_woken_with_an_error() {
	let side = side_runtime();
	let handle = side.handle().clone();
	bounded(async move {
		let dir = TempDir::new("group_fail_stop").unwrap();
		let mut options = (*opts(dir.path(), false)).clone();
		options.memtable_stall_threshold = 2;
		let tree = open(Arc::new(options)).await;
		commit_group(&tree, &base(), Mix::Immediate).await.into_iter().for_each(|r| r.unwrap());
		// One memtable is queued already: the rotation in the middle of the group makes two, which
		// stalls the writers that come after.
		tree.core.inner.rotate_memtable().unwrap();

		let writer = Arc::new(Mutex::new(None));
		let stalled = Arc::new(AtomicBool::new(true));
		{
			let (inner, tree, writer, stalled) = (
				Arc::clone(&tree.core.inner),
				Arc::clone(&tree),
				Arc::clone(&writer),
				Arc::clone(&stalled),
			);
			tree.core.commit_pipeline.set_hook(Some(Arc::new({
				let tree = Arc::clone(&tree);
				move |point| {
					if let PipelineHook::BeforeApplyRound {
						round: 1,
					} = point
					{
						inner.wal.write().fail_writes_after(0);
						*writer.lock().unwrap() = Some(handle.spawn(commit_one(
							Arc::clone(&tree),
							"stalled".into(),
							vec![NEW; 40],
							Durability::Immediate,
						)));
						let started = Instant::now();
						while !tree.core.write_stall.is_stalled() {
							if started.elapsed() > Duration::from_secs(30) {
								stalled.store(false, Ordering::SeqCst);
								break;
							}
							std::thread::sleep(Duration::from_millis(1));
						}
					}
				}
			})));
		}

		let results = commit_group(&tree, &group(), Mix::Immediate).await;
		tree.core.commit_pipeline.set_hook(None);
		assert!(stalled.load(Ordering::SeqCst), "the writer never stalled");
		results.iter().for_each(|r| {
			stop_error(r, "the group");
		});

		let writer = writer.lock().unwrap().take().unwrap();
		let result = tokio::time::timeout(Duration::from_secs(30), writer)
			.await
			.expect("the stalled writer was never woken")
			.unwrap();
		assert!(result.is_err(), "a writer that was stalled got through");
		assert_eq!(get(&tree, "stalled"), None);
		crash(&tree);
	});
	drop(side);
}

// ---------------------------------------------------------------------------
// what a stopped database does not do
// ---------------------------------------------------------------------------

/// Runs a group that fails in the re-append, leaving the first part of it in a memtable that has
/// been rotated out, and returns the tree and the group.
async fn stopped_tree(dir: &Path) -> (Arc<Tree>, Entries) {
	let tree = open(opts(dir, false)).await;
	commit_group(&tree, &base(), Mix::Immediate).await.into_iter().for_each(|r| r.unwrap());
	let group = group();
	inject(&tree, Failure::ReappendWrite);
	for result in commit_group(&tree, &group, Mix::Immediate).await {
		stop_error(&result, "the group");
	}
	tree.core.commit_pipeline.set_hook(None);
	(tree, group)
}

/// A memtable that holds part of the group must not be written to a table: the table would hold
/// part of the group, and the WAL segment that holds the rest is removed once its memtable is
/// flushed. So no flush, no checkpoint and no compaction runs, and recovery finds the group whole.
#[test(tokio::test)]
async fn a_stopped_database_flushes_compacts_and_checkpoints_nothing() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let (tree, group) = stopped_tree(dir.path()).await;
	let queued = tree.core.inner.immutable_count();
	assert_eq!(queued, 1, "the rotation in the middle of the group queued a memtable");
	let segments = files_with_extension(dir.path(), "wal", ".wal");
	let logged = non_empty_segments(dir.path());
	assert!(!logged.is_empty(), "the group's records are in a segment");
	let log_number = tree.core.inner.level_manifest.read().unwrap().get_log_number();

	assert!(matches!(tree.flush(), Err(Error::DatabaseStopped(_))));
	assert!(matches!(
		CompactionOperations::compact_memtable(&*tree.core.inner),
		Err(Error::DatabaseStopped(_))
	));
	let strategy = Arc::new(Strategy::from_options(Arc::clone(&tree.core.inner.opts)));
	assert!(matches!(tree.compact(strategy), Err(Error::DatabaseStopped(_))));
	let checkpoint = TempDir::new("group_fail_stop_checkpoint").unwrap();
	assert!(matches!(
		tree.create_checkpoint(checkpoint.path().join("cp")),
		Err(Error::DatabaseStopped(_))
	));

	assert_eq!(tree.core.inner.immutable_count(), queued, "the queued memtable stays");
	assert!(files_with_extension(dir.path(), "sstables", ".sst").is_empty());
	assert_eq!(files_with_extension(dir.path(), "wal", ".wal"), segments, "no segment is removed");
	assert_eq!(non_empty_segments(dir.path()), logged, "the segments that hold records are kept");
	assert_eq!(tree.core.inner.level_manifest.read().unwrap().get_log_number(), log_number);
	assert_eq!(scan(&tree).len(), 10, "reads still work");

	assert_group_recovered(&recover_image(dir.path()), &group, "a stopped database");
	crash(&tree);
}

/// An oversized batch of the group that was written to an L0 table is in the tables a checkpoint
/// copies, though no memtable holds anything of the group: a restore of the checkpoint would show
/// it.
#[test(tokio::test)]
async fn a_stopped_database_does_not_checkpoint_a_table_that_holds_part_of_the_group() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let tree = open(opts(dir.path(), false)).await;
	let injected = inject(&tree, Failure::SecondDirectL0);
	for result in commit_group(&tree, &Failure::SecondDirectL0.group(), Mix::Immediate).await {
		stop_error(&result, "the group");
	}
	tree.core.commit_pipeline.set_hook(None);
	injected.remove_blockers();
	assert_eq!(tree.core.inner.l0_file_count(), 1, "the first oversized batch is in a table");
	assert_eq!(tree.core.inner.immutable_count(), 0);

	let checkpoint = TempDir::new("group_fail_stop_checkpoint").unwrap();
	let cp = checkpoint.path().join("cp");
	assert!(matches!(tree.create_checkpoint(&cp), Err(Error::DatabaseStopped(_))));
	assert!(!cp.exists(), "nothing was copied");
	crash(&tree);
}

/// An oversized batch of the group that was written to an L0 table before the group failed is in
/// the tables of the stopped database. A compaction that merged it with the table that holds the
/// version readers see would keep only the newest version of its key, which is the one that was
/// never published.
#[test(tokio::test)]
async fn a_compaction_cannot_replace_what_readers_see_with_what_was_never_published() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let options = {
		let mut options = (*opts(dir.path(), false)).clone();
		options.level0_max_files = 2;
		Arc::new(options)
	};
	let tree = open(Arc::clone(&options)).await;

	// The version readers see of "big10" is in a table already.
	let old = vec![OLD; 40];
	commit_one(Arc::clone(&tree), "big10".into(), old.clone(), Durability::Immediate)
		.await
		.unwrap();
	tree.flush().unwrap();
	assert_eq!(tree.core.inner.l0_file_count(), 1);

	// The group writes "big10" again, to a table of its own, and then fails.
	inject(&tree, Failure::AfterDirectL0);
	for result in commit_group(&tree, &group_with_oversized(), Mix::Immediate).await {
		stop_error(&result, "the group");
	}
	tree.core.commit_pipeline.set_hook(None);
	assert_eq!(
		tree.core.inner.l0_file_count(),
		3,
		"the old table, the one the batches before the oversized batch were flushed to, and the \
		 oversized batch's"
	);
	assert_eq!(get(&tree, "big10"), Some(old.clone()));

	let strategy = Arc::new(Strategy::from_options(options));
	assert!(matches!(tree.compact(strategy), Err(Error::DatabaseStopped(_))));
	assert_eq!(tree.core.inner.l0_file_count(), 3, "nothing was compacted");
	assert_eq!(get(&tree, "big10"), Some(old), "the version readers see is still there");
	crash(&tree);
}

/// `close` leaves the memtables of a stopped database to the WAL, however `flush_on_close` is set:
/// a table made from the memtable that holds part of the group would end with a record of the
/// group removed, and recovery would find the group in pieces.
#[test(tokio::test)]
async fn close_of_a_stopped_database_leaves_the_group_to_the_wal() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let tree = open(opts(dir.path(), true)).await;
	commit_group(&tree, &base(), Mix::Immediate).await.into_iter().for_each(|r| r.unwrap());

	// The rotation in the middle of the group fails before the WAL is touched, so the memtable
	// that holds part of the group is the active one, and the WAL can be closed.
	let group = group();
	let injected = inject(&tree, Failure::RotationCreate);
	for result in commit_group(&tree, &group, Mix::Immediate).await {
		stop_error(&result, "the group");
	}
	tree.core.commit_pipeline.set_hook(None);
	injected.remove_blockers();
	assert!(!in_memtables(&tree, &group).is_empty());

	tree.core.is_closed.store(false, Ordering::SeqCst);
	tree.core.task_manager.lock().unwrap().take();
	tokio::time::timeout(Duration::from_secs(30), tree.core.close())
		.await
		.expect("close does not end")
		.expect("close of a stopped database works");
	assert!(files_with_extension(dir.path(), "sstables", ".sst").is_empty(), "nothing was flushed");
	drop(tree);

	let reopened = Tree::new(opts(dir.path(), false)).unwrap();
	assert_group_recovered(&scan(&reopened), &group, "after close");
	crash(&reopened);
}

/// A restore replaces the memtables and the WAL, so the batches the failed group applied go with
/// them, and the database shows the state of the checkpoint. It is not started again: that takes
/// a reopen.
#[test(tokio::test)]
async fn a_restore_discards_what_a_failed_group_applied() {
	let dir = TempDir::new("group_fail_stop").unwrap();
	let checkpoint = TempDir::new("group_fail_stop_checkpoint").unwrap();
	let tree = open(opts(dir.path(), false)).await;
	commit_group(&tree, &base(), Mix::Immediate).await.into_iter().for_each(|r| r.unwrap());
	let cp = checkpoint.path().join("cp");
	tree.create_checkpoint(&cp).unwrap();

	let group = group();
	let injected = inject(&tree, Failure::RotationCreate);
	for result in commit_group(&tree, &group, Mix::Immediate).await {
		stop_error(&result, "the group");
	}
	tree.core.commit_pipeline.set_hook(None);
	injected.remove_blockers();

	tree.restore_from_checkpoint(&cp).unwrap();
	assert!(in_memtables(&tree, &group).is_empty());
	assert_eq!(scan(&tree), base().into_iter().collect());
	let later = commit_one(Arc::clone(&tree), "later".into(), vec![NEW; 40], Durability::Immediate);
	stop_error(&later.await, "a commit after the restore");
	crash(&tree);
}
