//! A commit whose records are logged is never reported as failed because the WAL rotated under
//! it.
//!
//! The flusher appends a group to the active WAL segment and then applies it to the active
//! memtable, which it may only do if the memtable is tagged with that segment. A rotation
//! between the two (a checkpoint, a direct-to-L0 write, anyone rotating the memtable) makes
//! the records stale, and they are appended again in the new segment. Failing a group that
//! rotations keep overtaking is wrong, because its records are in the WAL by then: the commit
//! would be reported as failed and replayed by a restart.
//!
//! A group that finds itself stale more than `UNFENCED_STALE_ROUNDS` times in a row appends
//! the rest again under the active memtable's read guard instead, which a rotation needs
//! exclusively: the tag and the WAL's active segment cannot part, so the group makes progress
//! however fast the WAL rotates. These tests rotate at every step and check that
//!
//! * the commit succeeds, is visible, and a crash image recovers exactly the acknowledged commits;
//! * every memtable holds only batches whose record is in the segment it is tagged with, and no
//!   segment holds a record twice or out of sequence order;
//! * an Immediate group is fsynced before it is applied on this path as well, and a rotation cannot
//!   slip in between the fsync and the apply;
//! * the fenced round takes the memtable and then the WAL, as a rotation does, and holds neither
//!   while it waits for the other;
//! * a tag that is unrelated to the WAL fails the group before it is logged again, and a failed
//!   re-append fails it as a unit.
//!
//! Every scenario runs on a thread of its own with a deadline, and every hook that rotates makes
//! the group give up at a round far beyond what a group needs. A fix that never lets the group
//! through, or one that takes two locks in the wrong order, fails with a message where the
//! scenario would otherwise block the test binary for good.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;

use super::collect_transaction_all;
use crate::batch::Batch;
use crate::memtable::MemTable;
use crate::ring::{PipelineHook, UNFENCED_STALE_ROUNDS};
use crate::wal::reader::Reader;
use crate::wal::Error as WalError;
use crate::{Durability, Mode, Options, Tree};

/// A memtable that holds about 20 small commits.
const SMALL: usize = 4096;

/// Large enough that nothing but the hooks rotates.
const BIG: usize = 64 * 1024 * 1024;

/// How long a scenario may take before it is declared stuck.
const DEADLINE: Duration = Duration::from_secs(90);

/// The round at which a hook gives up on a group that is still going: far more than any group
/// needs, so reaching it means the pipeline does not make progress.
const RUNAWAY_ROUND: usize = 60;

/// A pair of a key and its value.
type Pair = (Vec<u8>, Vec<u8>);

/// For each key, the records (segment, starting sequence number) that held it.
type Logged = BTreeMap<Vec<u8>, HashSet<(u64, u64)>>;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Runs `scenario` on a thread of its own and fails, instead of blocking, if it does not end
/// within `DEADLINE`: a flusher that is stuck on a lock blocks its runtime thread for good, so
/// the runtime of a `tokio::test` could not even be dropped to report it. A scenario that
/// panics fails the test with its own message.
fn bounded<F>(multi_thread: bool, scenario: F)
where
	F: Future<Output = ()> + Send + 'static,
{
	let (tx, rx) = mpsc::channel();
	std::thread::spawn(move || {
		let mut builder = if multi_thread {
			let mut builder = tokio::runtime::Builder::new_multi_thread();
			builder.worker_threads(4);
			builder
		} else {
			tokio::runtime::Builder::new_current_thread()
		};
		let runtime = builder.enable_all().build().unwrap();
		let outcome =
			std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| runtime.block_on(scenario)));
		let _ = tx.send(outcome);
	});
	match rx.recv_timeout(DEADLINE) {
		Ok(Ok(())) => {}
		Ok(Err(panic)) => std::panic::resume_unwind(panic),
		Err(_) => panic!("the scenario did not end within {DEADLINE:?}: the flusher is stuck"),
	}
}

fn opts(path: &Path, max_memtable_size: usize, vlog: bool) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		max_memtable_size,
		// Crash semantics: nothing may be flushed on drop or close.
		flush_on_close: false,
		memtable_stall_threshold: 100_000,
		level0_max_files: 10_000,
		l0_stall_threshold: 10_000,
		enable_vlog: vlog,
		vlog_value_threshold: 100,
		vlog_max_file_size: 64 * 1024,
		..Default::default()
	})
}

/// A value of `len` bytes that no other `index` shares.
fn value(index: usize, len: usize) -> Vec<u8> {
	let mut x = (index as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
	(0..len)
		.map(|_| {
			x ^= x << 13;
			x ^= x >> 7;
			x ^= x << 17;
			x as u8
		})
		.collect()
}

/// `count` entries of `len` bytes, numbered from `from`.
fn entries(from: usize, count: usize, len: usize) -> Vec<(String, Vec<u8>)> {
	(from..from + count).map(|i| (format!("key{i:03}"), value(i, len))).collect()
}

fn pairs(entries: &[(String, Vec<u8>)]) -> Vec<Pair> {
	let mut pairs: Vec<Pair> =
		entries.iter().map(|(k, v)| (k.as_bytes().to_vec(), v.clone())).collect();
	pairs.sort();
	pairs
}

fn keys_of_pairs(pairs: &[Pair]) -> Vec<Vec<u8>> {
	pairs.iter().map(|(k, _)| k.clone()).collect()
}

fn mark_closed(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
}

fn release_lock(tree: &Tree) {
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}

/// Disposes of a tree that was only ever crashed in a copy.
fn dispose(tree: &Tree) {
	mark_closed(tree);
	release_lock(tree);
}

async fn stop_background_tasks(tree: &Tree) {
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
}

fn tag_of(tree: &Tree) -> u64 {
	tree.core.inner.active_memtable.read().unwrap().get_wal_number()
}

fn seg_path(dir: &Path, id: u64) -> PathBuf {
	dir.join("wal").join(format!("{id:020}.wal"))
}

fn segment_ids(dir: &Path) -> Vec<u64> {
	let mut ids: Vec<u64> = fs::read_dir(dir.join("wal"))
		.unwrap()
		.filter_map(|e| {
			let name = e.unwrap().file_name().to_string_lossy().to_string();
			name.strip_suffix(".wal").and_then(|s| s.parse().ok())
		})
		.collect();
	ids.sort();
	ids
}

/// Every record of a segment, decoded, in file order.
fn records(path: &Path) -> Vec<Batch> {
	let mut reader = Reader::new(fs::File::open(path).unwrap());
	let mut out = Vec::new();
	loop {
		match reader.read() {
			Ok((record, _)) => out.push(Batch::decode(record).unwrap()),
			Err(WalError::IO(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return out,
			Err(e) => panic!("{} must read end to end: {e}", path.display()),
		}
	}
}

fn segments(dir: &Path) -> BTreeMap<u64, Vec<Batch>> {
	segment_ids(dir).into_iter().map(|id| (id, records(&seg_path(dir, id)))).collect()
}

fn keys_of(batch: &Batch) -> HashSet<Vec<u8>> {
	batch.entries.iter().map(|e| e.key.clone()).collect()
}

/// How many records, in all the segments together, hold `key`.
fn copies_of(dir: &Path, key: &[u8]) -> usize {
	segments(dir)
		.values()
		.flatten()
		.filter(|batch| batch.entries.iter().any(|e| e.key == key))
		.count()
}

/// No segment holds a record twice or out of sequence order: the sequence numbers of its
/// records increase.
fn assert_segments_ordered(dir: &Path, what: &str) {
	for (id, batches) in segments(dir) {
		let seqs: Vec<u64> = batches.iter().map(|b| b.starting_seq_num).collect();
		assert!(
			seqs.windows(2).all(|w| w[0] < w[1]),
			"{what}: segment {id} holds a record twice or out of order: {seqs:?}"
		);
	}
}

/// Every key a memtable holds has a record in the segment the memtable is tagged with. (Older
/// segments may also hold stale copies. They go away with their segment and nothing relies on
/// them.) Only valid on a tree that was not recovered. Returns how many memtables hold any of
/// the keys.
fn assert_memtables_match_their_segments(
	tree: &Tree,
	dir: &Path,
	keys: &[Vec<u8>],
	what: &str,
) -> usize {
	let in_segment: BTreeMap<u64, HashSet<Vec<u8>>> = segments(dir)
		.into_iter()
		.map(|(id, batches)| (id, batches.iter().flat_map(keys_of).collect()))
		.collect();
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
	let mut holding = HashSet::new();
	for key in keys {
		for (n, (tag, memtable)) in memtables.iter().enumerate() {
			if memtable.get(key, None).is_some() {
				holding.insert(n);
				assert!(
					in_segment.get(tag).is_some_and(|keys| keys.contains(key)),
					"{what}: {} is in a memtable tagged with segment {tag} and has no record there \
					 (segments with a record: {:?})",
					String::from_utf8_lossy(key),
					in_segment
						.iter()
						.filter(|(_, keys)| keys.contains(key))
						.map(|(id, _)| *id)
						.collect::<Vec<_>>()
				);
			}
		}
	}
	holding.len()
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

/// Copies the live directory, which is a crash image because the WAL and the value log are
/// written through to their files, recovers the copy, and returns everything it holds.
fn recover_image(live: &Path, max_memtable_size: usize, vlog: bool) -> Vec<Pair> {
	let image = live.with_file_name("image");
	let _ = fs::remove_dir_all(&image);
	copy_dir(live, &image);
	let recovered = Tree::new(opts(&image, max_memtable_size, vlog)).unwrap();
	let txn = recovered.begin_with_mode(Mode::ReadOnly).unwrap();
	let all = collect_transaction_all(&mut txn.iter().unwrap()).unwrap();
	drop(txn);
	dispose(&recovered);
	all
}

fn get(tree: &Tree, key: &[u8]) -> Option<Vec<u8>> {
	tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key).unwrap()
}

/// One single-key transaction per entry, all spawned before the flusher is polled: on the
/// current-thread runtime that is one commit group, in order.
async fn commit_group(
	tree: &Arc<Tree>,
	entries: &[(String, Vec<u8>)],
	durability: Durability,
) -> Vec<crate::Result<()>> {
	let mut handles = Vec::new();
	for (key, value) in entries {
		let (tree, key, value) = (Arc::clone(tree), key.clone(), value.clone());
		handles.push(tokio::spawn(async move {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(durability);
			txn.set(key.as_bytes(), &value).unwrap();
			txn.commit().await
		}));
	}
	let mut out = Vec::new();
	for h in handles {
		out.push(h.await.unwrap());
	}
	out
}

/// What the hook of a scenario saw.
#[derive(Default)]
struct Probe {
	/// Apply rounds started.
	rounds: AtomicUsize,
	/// Fenced rounds that logged what was stale and were about to apply.
	fenced: AtomicUsize,
	/// A round at or past `RUNAWAY_ROUND` started: the group did not end.
	runaway: AtomicBool,
}

/// Installs the observer the scenarios share. `before_round` runs at the start of every apply
/// round, with its number, and `in_fence` when a fenced round has logged what was stale and has
/// not applied anything yet. A group still going at `RUNAWAY_ROUND` is given up on, by making
/// the flusher see a restore, so it fails the scenario instead of blocking it.
fn install(
	tree: &Tree,
	before_round: impl Fn(usize) + Send + Sync + 'static,
	in_fence: impl Fn() + Send + Sync + 'static,
) -> Arc<Probe> {
	let probe = Arc::new(Probe::default());
	let seen = Arc::clone(&probe);
	let pipeline = Arc::downgrade(&tree.core.commit_pipeline);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| match point {
		PipelineHook::BeforeApplyRound {
			round,
		} => {
			seen.rounds.fetch_add(1, Ordering::SeqCst);
			if round >= RUNAWAY_ROUND {
				seen.runaway.store(true, Ordering::SeqCst);
				if let Some(pipeline) = pipeline.upgrade() {
					pipeline.restoring.store(true, Ordering::SeqCst);
				}
			} else {
				before_round(round);
			}
		}
		PipelineHook::BeforeFencedApply => {
			seen.fenced.fetch_add(1, Ordering::SeqCst);
			in_fence();
		}
		PipelineHook::AfterWalSync {
			..
		} => {}
	})));
	probe
}

/// Seals the WAL segment at the start of the apply rounds `seal` picks, as a checkpoint or a
/// direct-to-L0 write would, so the segment a group's records are in is not the active one when
/// the round applies.
fn seal_when(tree: &Tree, seal: impl Fn(usize) -> bool + Send + Sync + 'static) -> Arc<Probe> {
	let inner = Arc::clone(&tree.core.inner);
	install(
		tree,
		move |round| {
			if seal(round) {
				inner.seal_active_wal_segment().unwrap();
			}
		},
		|| {},
	)
}

fn assert_no_runaway(probe: &Probe, what: &str) {
	assert!(
		!probe.runaway.load(Ordering::SeqCst),
		"{what}: the group was still going at round {RUNAWAY_ROUND}"
	);
}

/// Commits `group`, and checks that the group ended on its own and that every commit succeeded.
async fn commit_all(
	tree: &Arc<Tree>,
	group: &[(String, Vec<u8>)],
	durability: Durability,
	probe: &Probe,
	what: &str,
) {
	let results = commit_group(tree, group, durability).await;
	assert_no_runaway(probe, what);
	for (i, result) in results.iter().enumerate() {
		assert!(
			result.is_ok(),
			"{what} {durability:?}: commit {i} was reported failed: {result:?}"
		);
	}
}

// ---------------------------------------------------------------------------
// a seal at every round
// ---------------------------------------------------------------------------

/// A group committed while the WAL is sealed at the start of every one of its apply rounds.
struct SealedGroup {
	what: &'static str,
	max_memtable_size: usize,
	vlog: bool,
	group: Vec<(String, Vec<u8>)>,
	/// In how many memtables the group is expected to end up at least.
	memtables: usize,
	/// How many L0 tables the batches that cannot go to a memtable are written to.
	l0_tables: usize,
	/// Whether the group is overtaken often enough to be fenced.
	fenced: bool,
}

impl SealedGroup {
	/// The commit succeeds, is visible and survives a crash image, and the memtables and the
	/// segments agree. Nothing but the acknowledged commits is recovered.
	async fn run(self, durability: Durability) {
		let Self {
			what,
			max_memtable_size,
			vlog,
			group,
			memtables,
			l0_tables,
			fenced,
		} = self;
		let dir = TempDir::new("wal_stale_rounds").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(opts(&live, max_memtable_size, vlog)).unwrap());
		stop_background_tasks(&tree).await;
		let first_segment = tag_of(&tree);
		let probe = seal_when(&tree, |_| true);

		commit_all(&tree, &group, durability, &probe, what).await;
		let rounds = probe.rounds.load(Ordering::SeqCst);
		assert_eq!(
			probe.fenced.load(Ordering::SeqCst) > 0,
			fenced,
			"{what} {durability:?}: fenced rounds ({rounds} rounds)"
		);
		if fenced {
			assert!(
				rounds > UNFENCED_STALE_ROUNDS as usize,
				"{what} {durability:?}: the group must outlast the unfenced rounds ({rounds})"
			);
		}
		assert!(
			tag_of(&tree) >= first_segment + rounds as u64,
			"{what}: the WAL was sealed at every round"
		);

		let expected = pairs(&group);
		for (k, v) in &expected {
			assert_eq!(get(&tree, k).as_ref(), Some(v), "{what} {durability:?}: visible");
		}
		let holding =
			assert_memtables_match_their_segments(&tree, &live, &keys_of_pairs(&expected), what);
		assert!(
			holding >= memtables,
			"{what} {durability:?}: the group is in {holding} memtables, not {memtables}"
		);
		assert_eq!(
			tree.core.inner.level_manifest.read().unwrap().levels.total_tables(),
			l0_tables,
			"{what} {durability:?}: only the batches that cannot go to a memtable, and the memtables \
			 rotated out before them, are written to tables"
		);
		assert_segments_ordered(&live, what);
		assert_eq!(
			recover_image(&live, max_memtable_size, vlog),
			expected,
			"{what} {durability:?}: a crash image recovers exactly the acknowledged commits"
		);
		dispose(&tree);
	}
}

fn sealed_groups() -> Vec<SealedGroup> {
	let mut oversized_in_the_middle = entries(0, 6, 100);
	oversized_in_the_middle[2].1 = value(99, SMALL + 600);
	let mut lone_oversized = entries(0, 1, 100);
	lone_oversized[0].1 = value(98, SMALL + 900);
	vec![
		SealedGroup {
			what: "one commit",
			max_memtable_size: BIG,
			vlog: false,
			group: entries(0, 1, 100),
			memtables: 1,
			l0_tables: 0,
			fenced: true,
		},
		SealedGroup {
			what: "group",
			max_memtable_size: BIG,
			vlog: false,
			group: entries(0, 6, 100),
			memtables: 1,
			l0_tables: 0,
			fenced: true,
		},
		// A group that does not fit a memtable: it also rotates by itself, inside the fence.
		SealedGroup {
			what: "straddling group",
			max_memtable_size: SMALL,
			vlog: false,
			group: entries(0, 24, 100),
			memtables: 2,
			l0_tables: 0,
			fenced: true,
		},
		// A batch that cannot go to a memtable, in the middle: written to an L0 table.
		SealedGroup {
			what: "oversized in the middle",
			max_memtable_size: SMALL,
			vlog: false,
			group: oversized_in_the_middle,
			memtables: 1,
			// The oversized batch's own table, and the memtable the seal rotated out before it:
			// a direct-to-L0 write first flushes every pending immutable memtable (#423's
			// `flush_lock` path in `write_batch_direct_to_l0_sst`).
			l0_tables: 2,
			fenced: true,
		},
		// Nothing is stale by the time it is logged again, so there is nothing to fence.
		SealedGroup {
			what: "lone oversized",
			max_memtable_size: SMALL,
			vlog: false,
			group: lone_oversized,
			memtables: 0,
			l0_tables: 1,
			fenced: false,
		},
		SealedGroup {
			what: "values in the value log",
			max_memtable_size: BIG,
			vlog: true,
			group: entries(0, 3, 5_000),
			memtables: 1,
			l0_tables: 0,
			fenced: true,
		},
	]
}

async fn seal_at_every_round(durability: Durability) {
	for group in sealed_groups() {
		group.run(durability).await;
	}
}

#[test]
fn a_seal_at_every_apply_round_does_not_fail_a_logged_immediate_commit() {
	bounded(false, seal_at_every_round(Durability::Immediate));
}

#[test]
fn a_seal_at_every_apply_round_does_not_fail_a_logged_eventual_commit() {
	bounded(false, seal_at_every_round(Durability::Eventual));
}

/// A group that fewer rotations overtake than the limit is repaired by logging it again off the
/// memtable's lock, once for every round that was overtaken. The fence, which holds the memtable
/// across an fsync, is not entered: not for one overtake, and not for as many as the limit.
#[test]
fn a_group_overtaken_fewer_times_than_the_limit_is_repaired_without_the_fence() {
	bounded(false, async {
		for seals in [1, UNFENCED_STALE_ROUNDS as usize] {
			for durability in [Durability::Immediate, Durability::Eventual] {
				let what = format!("{seals} overtakes {durability:?}");
				let dir = TempDir::new("wal_stale_rounds").unwrap();
				let live = dir.path().join("live");
				let tree = Arc::new(Tree::new(opts(&live, BIG, false)).unwrap());
				stop_background_tasks(&tree).await;
				let probe = seal_when(&tree, move |round| round < seals);

				let group = entries(0, 3, 100);
				commit_all(&tree, &group, durability, &probe, &what).await;
				assert_eq!(probe.fenced.load(Ordering::SeqCst), 0, "{what}: fenced");
				assert_eq!(probe.rounds.load(Ordering::SeqCst), seals + 1, "{what}: rounds");
				assert_eq!(
					copies_of(&live, b"key000"),
					seals + 1,
					"{what}: the record and one more for every round that was overtaken"
				);
				assert_segments_ordered(&live, &what);
				assert_eq!(recover_image(&live, BIG, false), pairs(&group), "{what}");
				dispose(&tree);
			}
		}
	});
}

/// Seals that go on for a while, and then stop, instead of for ever. The group is logged once,
/// once more for each unfenced round that was overtaken, and once in the fenced round, which
/// ends it: the fence is entered after `UNFENCED_STALE_ROUNDS` stale rounds and not later, and
/// every copy that the fenced round applies has its record in the segment of its memtable.
#[test]
fn a_group_overtaken_for_a_while_ends_in_the_first_fenced_round() {
	bounded(false, async {
		let unfenced = UNFENCED_STALE_ROUNDS as usize;
		for durability in [Durability::Immediate, Durability::Eventual] {
			let dir = TempDir::new("wal_stale_rounds").unwrap();
			let live = dir.path().join("live");
			let tree = Arc::new(Tree::new(opts(&live, BIG, false)).unwrap());
			stop_background_tasks(&tree).await;
			let probe = seal_when(&tree, |round| round < 8);

			let group = entries(0, 4, 100);
			commit_all(&tree, &group, durability, &probe, "overtaken").await;
			assert_eq!(probe.fenced.load(Ordering::SeqCst), 1, "{durability:?}: fenced rounds");
			// The rounds that were overtaken, the one that switched to the fence, the fence.
			assert_eq!(probe.rounds.load(Ordering::SeqCst), unfenced + 2, "{durability:?}: rounds");
			assert_eq!(
				copies_of(&live, b"key000"),
				unfenced + 2,
				"{durability:?}: the record, one more per unfenced round, one in the fence"
			);
			let expected = pairs(&group);
			assert_memtables_match_their_segments(
				&tree,
				&live,
				&keys_of_pairs(&expected),
				"overtaken",
			);
			assert_segments_ordered(&live, "overtaken");
			assert_eq!(recover_image(&live, BIG, false), expected, "{durability:?}");
			dispose(&tree);
		}
	});
}

/// A group that only rotates the memtable by itself, because it does not fit, makes progress at
/// every step and is never fenced, however many memtables it spans.
#[test]
fn a_group_that_only_overflows_memtables_never_fences() {
	bounded(false, async {
		let dir = TempDir::new("wal_stale_rounds").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(opts(&live, SMALL, false)).unwrap());
		stop_background_tasks(&tree).await;
		let probe = seal_when(&tree, |_| false);

		let group = entries(0, 120, 100);
		commit_all(&tree, &group, Durability::Immediate, &probe, "overflow").await;
		assert_eq!(probe.fenced.load(Ordering::SeqCst), 0, "no external rotation, no fence");
		let expected = pairs(&group);
		let holding = assert_memtables_match_their_segments(
			&tree,
			&live,
			&keys_of_pairs(&expected),
			"overflow",
		);
		assert!(holding >= 4, "the group spans {holding} memtables, not enough to prove it");
		assert_segments_ordered(&live, "overflow");
		assert_eq!(recover_image(&live, SMALL, false), expected);
		dispose(&tree);
	});
}

/// Commits that were applied and acknowledged before the group are in a memtable that the first
/// seal swaps out, so it is a real rotation, not only a new WAL segment. The group that was
/// overtaken is applied to a memtable of its own, and a crash image holds all of it.
#[test]
fn a_group_overtaken_by_a_rotation_of_a_full_memtable_is_applied_in_its_own_segment() {
	bounded(false, async {
		let dir = TempDir::new("wal_stale_rounds").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(opts(&live, BIG, false)).unwrap());
		stop_background_tasks(&tree).await;

		let first = entries(0, 3, 100);
		for r in commit_group(&tree, &first, Durability::Immediate).await {
			r.unwrap();
		}
		let probe = seal_when(&tree, |_| true);
		let second = entries(10, 5, 100);
		commit_all(&tree, &second, Durability::Immediate, &probe, "full memtable").await;
		assert!(probe.fenced.load(Ordering::SeqCst) >= 1);
		assert!(
			!tree.core.inner.immutable_memtables.read().unwrap().is_empty(),
			"the first seal swapped the memtable that held the first group"
		);
		let all: Vec<_> = first.iter().chain(second.iter()).cloned().collect();
		let expected = pairs(&all);
		let holding = assert_memtables_match_their_segments(
			&tree,
			&live,
			&keys_of_pairs(&expected),
			"full memtable",
		);
		assert!(holding >= 2, "{holding} memtables hold the two groups");
		assert_segments_ordered(&live, "full memtable");
		assert_eq!(recover_image(&live, BIG, false), expected);
		dispose(&tree);
	});
}

/// The fence of one group does not carry over to the next: the second group, which one seal
/// overtakes, is repaired without it.
#[test]
fn the_fence_of_one_group_does_not_leak_into_the_next() {
	bounded(false, async {
		let dir = TempDir::new("wal_stale_rounds").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(opts(&live, BIG, false)).unwrap());
		stop_background_tasks(&tree).await;
		let group_no = Arc::new(AtomicUsize::new(0));
		let probe = {
			let (inner, group_no) = (Arc::clone(&tree.core.inner), Arc::clone(&group_no));
			install(
				&tree,
				move |round| {
					if round == 0 {
						group_no.fetch_add(1, Ordering::SeqCst);
					}
					// The first group is sealed at every round, the second once.
					if group_no.load(Ordering::SeqCst) == 1 || round == 0 {
						inner.seal_active_wal_segment().unwrap();
					}
				},
				|| {},
			)
		};

		let first = entries(0, 2, 100);
		commit_all(&tree, &first, Durability::Immediate, &probe, "first").await;
		assert_eq!(probe.fenced.load(Ordering::SeqCst), 1);
		let second = entries(2, 2, 100);
		commit_all(&tree, &second, Durability::Immediate, &probe, "second").await;
		assert_eq!(
			probe.fenced.load(Ordering::SeqCst),
			1,
			"a group that one seal overtakes must not be fenced"
		);
		let all: Vec<_> = first.iter().chain(second.iter()).cloned().collect();
		assert_eq!(recover_image(&live, BIG, false), pairs(&all));
		dispose(&tree);
	});
}

/// A rotation that comes in the very round that flips a group to fenced, and one that comes only
/// after the fenced round: nothing differs, and the unfenced rounds before are the only ones that
/// logged more copies.
#[test]
fn rotations_exactly_at_the_flip_and_after_the_fence_change_nothing() {
	bounded(false, async {
		let flip = UNFENCED_STALE_ROUNDS as usize;
		for (what, rounds_that_seal) in [
			("the flip round only", vec![flip]),
			("the flip round and the fenced round", vec![flip, flip + 1]),
			("the first and the flip round", vec![0, flip]),
			("after the fence", vec![flip + 2, flip + 3]),
		] {
			for durability in [Durability::Immediate, Durability::Eventual] {
				let dir = TempDir::new("wal_stale_rounds").unwrap();
				let live = dir.path().join("live");
				let tree = Arc::new(Tree::new(opts(&live, BIG, false)).unwrap());
				stop_background_tasks(&tree).await;
				let sealing = rounds_that_seal.clone();
				let probe = seal_when(&tree, move |round| sealing.contains(&round));

				let group = entries(0, 4, 100);
				commit_all(&tree, &group, durability, &probe, what).await;
				for (k, v) in pairs(&group) {
					assert_eq!(get(&tree, &k), Some(v), "{what} {durability:?}");
				}
				assert_segments_ordered(&live, what);
				assert_eq!(
					recover_image(&live, BIG, false),
					pairs(&group),
					"{what} {durability:?}"
				);
				dispose(&tree);
			}
		}
	});
}

/// A group with batches too big for a memtable between ordinary ones, with a rotation at every
/// round: every oversized batch goes to L0 and seals the segment, which makes the batches behind
/// it stale again. Nothing behind an oversized batch is logged before it is through, so an
/// ordinary batch is logged once with the group and once more when it is applied, and the first
/// one also once per unfenced round. An oversized batch is logged once, with the group.
#[test]
fn nothing_behind_an_oversized_batch_is_logged_before_it_is_through() {
	bounded(false, async {
		let dir = TempDir::new("wal_stale_rounds").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(opts(&live, SMALL, false)).unwrap());
		stop_background_tasks(&tree).await;
		// A direct-to-L0 write flushes the pending immutable memtables first, which removes their
		// WAL segments, so the records that were logged are collected as the rounds go by instead
		// of being counted in the segments that are left at the end.
		let logged: Arc<Mutex<Logged>> = Arc::default();
		let collect = {
			let (live, logged) = (live.clone(), Arc::clone(&logged));
			move || {
				let mut logged = logged.lock().unwrap();
				for (id, batches) in segments(&live) {
					for batch in batches {
						for entry in &batch.entries {
							logged
								.entry(entry.key.clone())
								.or_default()
								.insert((id, batch.starting_seq_num));
						}
					}
				}
			}
		};
		let inner = Arc::clone(&tree.core.inner);
		let at_round = collect.clone();
		let probe = install(
			&tree,
			move |_| {
				at_round();
				inner.seal_active_wal_segment().unwrap();
			},
			|| {},
		);

		let mut group = entries(0, 9, 100);
		for (i, entry) in group.iter_mut().enumerate() {
			if i % 2 == 1 {
				entry.1 = value(500 + i, SMALL + 600);
			}
		}
		commit_all(&tree, &group, Durability::Immediate, &probe, "oversized").await;
		collect();
		assert_segments_ordered(&live, "oversized");
		for (i, (k, v)) in group.iter().enumerate() {
			assert_eq!(get(&tree, k.as_bytes()).as_ref(), Some(v), "{k}");
			let expected = match i {
				0 => 2 + UNFENCED_STALE_ROUNDS as usize,
				_ if i % 2 == 1 => 1,
				_ => 2,
			};
			let copies = logged.lock().unwrap().get(k.as_bytes()).map_or(0, HashSet::len);
			assert_eq!(copies, expected, "{k}: copies in the WAL");
		}
		assert_eq!(recover_image(&live, SMALL, false), pairs(&group));
		dispose(&tree);
	});
}

// ---------------------------------------------------------------------------
// inside the fence
// ---------------------------------------------------------------------------

/// What a fenced round looked like at the point it had logged what was stale and had not
/// applied anything yet.
#[derive(Debug)]
struct Fence {
	/// The active memtable cannot be taken exclusively: the read guard is held.
	guard_held: bool,
	/// The segment the active memtable is tagged with, and the WAL's active segment.
	tag: Option<u64>,
	wal_segment: u64,
	/// Whether anything was appended to the active segment since its last fsync.
	pending_sync: bool,
}

fn observe_fence(tree: &Tree) -> (Arc<Probe>, Arc<Mutex<Vec<Fence>>>) {
	let seen = Arc::new(Mutex::new(Vec::new()));
	let (inner, sink) = (Arc::clone(&tree.core.inner), Arc::clone(&seen));
	let sealing = Arc::clone(&inner);
	let probe = install(
		tree,
		move |_| {
			sealing.seal_active_wal_segment().unwrap();
		},
		move || {
			let guard_held = matches!(
				inner.active_memtable.try_write(),
				Err(std::sync::TryLockError::WouldBlock)
			);
			let tag = inner.active_memtable.try_read().ok().map(|a| a.get_wal_number());
			let wal = inner.wal.read();
			sink.lock().unwrap().push(Fence {
				guard_held,
				tag,
				wal_segment: wal.get_active_log_number(),
				pending_sync: wal.pending_sync(),
			});
		},
	);
	(probe, seen)
}

/// A fenced round holds the memtable's read guard from the re-append to the apply, the WAL's
/// active segment is the memtable's tag, and an Immediate group is fsynced before it is applied,
/// in every fenced round of a group that spans several memtables too. The Eventual control leaves
/// the append unsynced, so the probe can see one.
#[test]
fn a_fenced_round_has_synced_what_it_logged_and_holds_the_guard_until_it_applies() {
	bounded(false, async {
		// The durability, the value log, the memtable size, the group, and how many fenced
		// rounds it needs at least.
		let cases = [
			(Durability::Immediate, false, BIG, entries(0, 3, 100), 1),
			(Durability::Immediate, true, BIG, entries(0, 3, 5_000), 1),
			(Durability::Eventual, false, BIG, entries(0, 3, 100), 1),
			(Durability::Eventual, true, BIG, entries(0, 3, 5_000), 1),
			(Durability::Immediate, false, SMALL, entries(0, 80, 100), 3),
		];
		for (durability, vlog, max_memtable_size, group, fences) in cases {
			let what = format!("{durability:?} vlog {vlog} memtable {max_memtable_size}");
			let dir = TempDir::new("wal_stale_rounds").unwrap();
			let live = dir.path().join("live");
			let tree = Arc::new(Tree::new(opts(&live, max_memtable_size, vlog)).unwrap());
			stop_background_tasks(&tree).await;
			let (probe, seen) = observe_fence(&tree);

			commit_all(&tree, &group, durability, &probe, &what).await;
			let seen = seen.lock().unwrap();
			assert!(seen.len() >= fences, "{what}: {} fenced rounds: {seen:?}", seen.len());
			if fences == 1 {
				assert_eq!(seen.len(), 1, "{what}: one fenced round: {seen:?}");
			}
			for fence in seen.iter() {
				assert!(fence.guard_held, "{what}: {fence:?}");
				assert_eq!(fence.tag, Some(fence.wal_segment), "{what}: {fence:?}");
				assert_eq!(
					fence.pending_sync,
					durability == Durability::Eventual,
					"{what}: {fence:?}"
				);
			}
			dispose(&tree);
		}
	});
}

/// A rotation that another thread attempts while a fenced round is between its fsync and its
/// apply waits for the round to end: it cannot move the tag under the apply. The batches end up
/// in the memtable that rotation then seals, in the segment the round logged them in.
#[test]
fn a_rotation_attempted_inside_a_fenced_round_waits_for_the_apply() {
	bounded(false, async {
		let dir = TempDir::new("wal_stale_rounds").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(opts(&live, BIG, false)).unwrap());
		stop_background_tasks(&tree).await;
		let finished_in_the_fence = Arc::new(AtomicBool::new(true));
		let rotator: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> = Arc::new(Mutex::new(None));
		let probe = {
			let (inner, sealing) = (Arc::clone(&tree.core.inner), Arc::clone(&tree.core.inner));
			let (observed, rotator) = (Arc::clone(&finished_in_the_fence), Arc::clone(&rotator));
			install(
				&tree,
				move |_| {
					sealing.seal_active_wal_segment().unwrap();
				},
				move || {
					let (started_tx, started_rx) = mpsc::channel();
					let done = Arc::new(AtomicBool::new(false));
					let (inner, done_in_thread) = (Arc::clone(&inner), Arc::clone(&done));
					*rotator.lock().unwrap() = Some(std::thread::spawn(move || {
						started_tx.send(()).unwrap();
						inner.seal_active_wal_segment().unwrap();
						done_in_thread.store(true, Ordering::SeqCst);
					}));
					started_rx.recv().unwrap();
					// Long enough for a rotation that is not held back to be done.
					std::thread::sleep(Duration::from_millis(200));
					observed.store(done.load(Ordering::SeqCst), Ordering::SeqCst);
				},
			)
		};

		let group = entries(0, 3, 100);
		commit_all(&tree, &group, Durability::Immediate, &probe, "rotation in the fence").await;
		rotator.lock().unwrap().take().expect("the fenced round ran").join().unwrap();
		assert!(
			!finished_in_the_fence.load(Ordering::SeqCst),
			"a rotation finished while the fenced round held the memtable"
		);
		let expected = pairs(&group);
		assert_memtables_match_their_segments(
			&tree,
			&live,
			&keys_of_pairs(&expected),
			"rotation inside the fence",
		);
		assert_segments_ordered(&live, "rotation inside the fence");
		assert_eq!(recover_image(&live, BIG, false), expected);
		dispose(&tree);
	});
}

/// A rotation takes the active memtable and then the WAL. A fenced round that has to wait for the
/// memtable must therefore not hold the WAL while it waits: a rotator that holds the memtable
/// and asks for the WAL would wait for the round, which waits for the rotator.
///
/// The hook makes that interleaving certain. At the round that is about to be fenced it takes the
/// memtable exclusively on another thread and lets the flusher run into it; the thread then asks
/// for the WAL, as a rotation does, and must get it.
#[test]
fn a_fenced_round_waiting_for_the_memtable_does_not_hold_the_wal() {
	bounded(false, async {
		let dir = TempDir::new("wal_stale_rounds").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(opts(&live, BIG, false)).unwrap());
		stop_background_tasks(&tree).await;

		let wal_was_free = Arc::new(AtomicBool::new(false));
		let rotator = Arc::new(Mutex::new(None));
		let fenced_round = UNFENCED_STALE_ROUNDS as usize + 1;
		let probe = {
			let inner = Arc::clone(&tree.core.inner);
			let (wal_was_free, rotator) = (Arc::clone(&wal_was_free), Arc::clone(&rotator));
			install(
				&tree,
				move |round| {
					if round <= fenced_round {
						inner.seal_active_wal_segment().unwrap();
					}
					if round == fenced_round {
						let (ready_tx, ready_rx) = mpsc::channel();
						let (inner, wal_was_free) = (Arc::clone(&inner), Arc::clone(&wal_was_free));
						*rotator.lock().unwrap() = Some(std::thread::spawn(move || {
							let active = inner.active_memtable.write().unwrap();
							ready_tx.send(()).unwrap();
							// Long enough for the flusher to reach the memtable's lock.
							std::thread::sleep(Duration::from_millis(300));
							let wal = inner.wal.inner.try_write_for(Duration::from_secs(5));
							wal_was_free.store(wal.is_some(), Ordering::SeqCst);
							drop(wal);
							drop(active);
						}));
						ready_rx.recv().unwrap();
					}
				},
				|| {},
			)
		};

		let group = entries(0, 2, 100);
		commit_all(&tree, &group, Durability::Immediate, &probe, "lock order").await;
		rotator.lock().unwrap().take().expect("the fenced round ran").join().unwrap();
		assert!(
			wal_was_free.load(Ordering::SeqCst),
			"a rotator holding the memtable could not take the WAL: the fenced round held the WAL \
			 while it waited for the memtable"
		);
		assert_eq!(recover_image(&live, BIG, false), pairs(&group));
		dispose(&tree);
	});
}

/// A rotation that is already waiting for the memtable when the fenced round takes it stays
/// waiting through the round's log, its fsync and its apply: the fenced round must not let go of
/// the memtable in between, or the rotation would move the tag under the apply, and it must not
/// take the memtable a second time while the rotation waits for it.
///
/// The hook holds the WAL on another thread, so the flusher takes the memtable and then waits for
/// the WAL. While it waits, a rotation is started, and it queues for the memtable. Then the WAL
/// is released.
#[test]
fn a_rotation_queued_for_the_memtable_waits_through_the_log_and_the_sync() {
	bounded(false, async {
		let dir = TempDir::new("wal_stale_rounds").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(opts(&live, BIG, false)).unwrap());
		stop_background_tasks(&tree).await;

		let holder = Arc::new(Mutex::new(None));
		let fenced_round = UNFENCED_STALE_ROUNDS as usize + 1;
		let probe = {
			let inner = Arc::clone(&tree.core.inner);
			let holder = Arc::clone(&holder);
			install(
				&tree,
				move |round| {
					if round <= fenced_round {
						inner.seal_active_wal_segment().unwrap();
					}
					if round == fenced_round {
						let (ready_tx, ready_rx) = mpsc::channel();
						let inner = Arc::clone(&inner);
						*holder.lock().unwrap() = Some(std::thread::spawn(move || {
							let wal = inner.wal.write();
							ready_tx.send(()).unwrap();
							// Long enough for the flusher to take the memtable and wait for the
							// WAL.
							std::thread::sleep(Duration::from_millis(250));
							let rotating = {
								let inner = Arc::clone(&inner);
								std::thread::spawn(move || {
									inner.seal_active_wal_segment().unwrap();
								})
							};
							// Long enough for the rotation to queue for the memtable.
							std::thread::sleep(Duration::from_millis(250));
							drop(wal);
							rotating.join().unwrap();
						}));
						ready_rx.recv().unwrap();
					}
				},
				|| {},
			)
		};

		let group = entries(0, 3, 100);
		commit_all(&tree, &group, Durability::Immediate, &probe, "queued rotation").await;
		holder.lock().unwrap().take().expect("the fenced round ran").join().unwrap();
		let expected = pairs(&group);
		assert_memtables_match_their_segments(
			&tree,
			&live,
			&keys_of_pairs(&expected),
			"queued rotation",
		);
		assert_segments_ordered(&live, "queued rotation");
		assert_eq!(recover_image(&live, BIG, false), expected);
		dispose(&tree);
	});
}

// ---------------------------------------------------------------------------
// a failure inside the fence
// ---------------------------------------------------------------------------

/// A re-append that fails after `ops` appends and flushes of the WAL writer in the fenced round,
/// whatever it had written by then: the group fails as a unit, the segment is exactly as it was,
/// the memtable has nothing of the group, the writer refuses more until the WAL rotates, and a
/// crash image holds the group whole or not at all.
#[test]
fn a_re_append_that_fails_anywhere_in_the_fence_leaves_the_segment_as_it_was() {
	bounded(false, async {
		let fenced_round = UNFENCED_STALE_ROUNDS as usize + 1;
		let mut failed_at = Vec::new();
		for ops in 0..40usize {
			let dir = TempDir::new("wal_stale_rounds").unwrap();
			let live = dir.path().join("live");
			let tree = Arc::new(Tree::new(opts(&live, BIG, false)).unwrap());
			stop_background_tasks(&tree).await;
			let probe = {
				let inner = Arc::clone(&tree.core.inner);
				install(
					&tree,
					move |round| {
						inner.seal_active_wal_segment().unwrap();
						if round == fenced_round {
							inner.wal.write().fail_writes_after(ops);
						}
					},
					|| {},
				)
			};

			// About 42 KiB: more than one 32 KiB block, so the write is in several pieces.
			let group = entries(0, 3, 14_000);
			let results = commit_group(&tree, &group, Durability::Immediate).await;
			assert_no_runaway(&probe, "failed re-append");
			let failed = results.iter().filter(|r| r.is_err()).count();
			assert!(
				failed == 0 || failed == group.len(),
				"ops {ops}: the group is a unit: {results:?}"
			);
			if failed == 0 {
				// The failure point is past the end of the append: nothing more to find.
				dispose(&tree);
				break;
			}
			failed_at.push(ops);
			assert_eq!(
				probe.rounds.load(Ordering::SeqCst),
				fenced_round + 1,
				"ops {ops}: the fenced round is the one that failed"
			);
			let active = tag_of(&tree);
			assert_eq!(
				fs::metadata(seg_path(&live, active)).unwrap().len(),
				0,
				"ops {ops}: the failed group left bytes in the active segment"
			);
			for (k, _) in &group {
				assert_eq!(get(&tree, k.as_bytes()), None, "ops {ops}: {k} was applied");
			}
			// The failed group's original record is whole in the segment it was first logged in.
			if ops % 3 == 0 {
				let image = recover_image(&live, BIG, false);
				assert!(
					image.is_empty() || image == pairs(&group),
					"ops {ops}: a crash image holds a part of the group ({} of {} keys)",
					image.len(),
					group.len()
				);
			}
			// The writer stays poisoned until the WAL rotates.
			tree.core.commit_pipeline.set_hook(None);
			let later = commit_group(&tree, &entries(1, 1, 50), Durability::Immediate).await;
			assert!(later[0].is_err(), "ops {ops}: a poisoned writer refuses the next commit");
			tree.core.inner.seal_active_wal_segment().unwrap();
			for r in commit_group(&tree, &entries(1, 1, 50), Durability::Immediate).await {
				r.unwrap();
			}
			assert_eq!(get(&tree, b"key001"), Some(value(1, 50)), "ops {ops}");
			dispose(&tree);
		}
		assert!(
			failed_at.len() >= 3,
			"the failpoint reached the fenced append at {failed_at:?} only"
		);
	});
}

// ---------------------------------------------------------------------------
// a tag the WAL cannot explain
// ---------------------------------------------------------------------------

/// A memtable tagged with a segment that the WAL never had, or no longer has, is not a race: no
/// rotation will bring the tag and the WAL together. The group fails the first time it finds the
/// records stale, before it is logged again, and the pipeline works again once the tag is right.
#[test]
fn a_tag_unrelated_to_the_wal_fails_the_group_before_it_is_logged_again() {
	bounded(false, async {
		for (what, ahead) in [("tag ahead of the WAL", true), ("tag behind the WAL", false)] {
			let dir = TempDir::new("wal_stale_rounds").unwrap();
			let live = dir.path().join("live");
			let tree = Arc::new(Tree::new(opts(&live, BIG, false)).unwrap());
			stop_background_tasks(&tree).await;
			tree.core.inner.seal_active_wal_segment().unwrap();
			tree.core.inner.seal_active_wal_segment().unwrap();
			let real = tag_of(&tree);
			assert!(real >= 2);
			let segments_before = segment_ids(&live);
			let wrong = if ahead {
				real + 1000
			} else {
				0
			};
			tree.core.inner.active_memtable.read().unwrap().set_wal_number(wrong);
			let probe = seal_when(&tree, |_| false);

			let results = commit_group(&tree, &entries(0, 1, 100), Durability::Immediate).await;
			assert_no_runaway(&probe, what);
			assert!(results[0].is_err(), "{what}: the commit must fail, not spin or succeed");
			assert_eq!(probe.rounds.load(Ordering::SeqCst), 1, "{what}: rounds");
			assert_eq!(probe.fenced.load(Ordering::SeqCst), 0, "{what}: fenced rounds");
			assert_eq!(copies_of(&live, b"key000"), 1, "{what}: the group was logged again");
			assert_eq!(segment_ids(&live), segments_before, "{what}: no segment was added");
			assert_eq!(get(&tree, b"key000"), None, "{what}: nothing of the group was applied");

			// Nothing is poisoned: with the tag right the next commit goes through.
			tree.core.inner.active_memtable.read().unwrap().set_wal_number(real);
			for r in commit_group(&tree, &entries(1, 1, 50), Durability::Immediate).await {
				r.unwrap();
			}
			assert_eq!(get(&tree, b"key001"), Some(value(1, 50)), "{what}");
			dispose(&tree);
		}
	});
}

/// A tag that parts from the WAL while a group is being repaired is found by the fenced round,
/// under the guard, where the tag is the WAL's active segment or the state is impossible. The
/// round fails the group before it logs anything, and what the unfenced rounds logged is all
/// there is.
#[test]
fn a_fenced_round_that_finds_the_tag_apart_from_the_wal_fails_before_it_logs() {
	bounded(false, async {
		let dir = TempDir::new("wal_stale_rounds").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(opts(&live, BIG, false)).unwrap());
		stop_background_tasks(&tree).await;
		let fenced_round = UNFENCED_STALE_ROUNDS as usize + 1;
		let real = Arc::new(AtomicU64::new(0));
		let probe = {
			let (inner, real) = (Arc::clone(&tree.core.inner), Arc::clone(&real));
			install(
				&tree,
				move |round| {
					if round < fenced_round {
						inner.seal_active_wal_segment().unwrap();
					} else {
						let active = inner.active_memtable.read().unwrap();
						real.store(active.get_wal_number(), Ordering::SeqCst);
						active.set_wal_number(active.get_wal_number() + 1000);
					}
				},
				|| {},
			)
		};

		let results = commit_group(&tree, &entries(0, 1, 100), Durability::Immediate).await;
		assert_no_runaway(&probe, "tag apart from the WAL");
		assert!(results[0].is_err(), "the commit must fail");
		assert_eq!(
			probe.fenced.load(Ordering::SeqCst),
			0,
			"the fenced round failed before it logged"
		);
		assert_eq!(
			probe.rounds.load(Ordering::SeqCst),
			fenced_round + 1,
			"the fenced round is the last"
		);
		assert_eq!(
			copies_of(&live, b"key000"),
			1 + UNFENCED_STALE_ROUNDS as usize,
			"the record and one more for every unfenced round, and none from the fenced round"
		);
		assert_eq!(get(&tree, b"key000"), None, "nothing of the group was applied");

		tree.core.commit_pipeline.set_hook(None);
		tree.core.inner.active_memtable.read().unwrap().set_wal_number(real.load(Ordering::SeqCst));
		for r in commit_group(&tree, &entries(1, 1, 50), Durability::Immediate).await {
			r.unwrap();
		}
		assert_eq!(get(&tree, b"key001"), Some(value(1, 50)));
		dispose(&tree);
	});
}

// ---------------------------------------------------------------------------
// storms
// ---------------------------------------------------------------------------

/// What one writer commits: one to three keys, values of every size from a few bytes to more than
/// a memtable holds (those go to an L0 table), and the writer's durability.
fn writer_plan(w: usize, commits: usize) -> Vec<Vec<(String, Vec<u8>)>> {
	const LENS: [usize; 6] = [50, 200, 900, 2500, 9000, 60];
	(0..commits)
		.map(|i| {
			let keys = 1 + (i + w) % 3;
			(0..keys)
				.map(|k| {
					let len = LENS[(w + i + k * 2) % LENS.len()];
					(format!("w{w:02}_{i:03}_{k}"), value(w * 10_000 + i * 10 + k, len))
				})
				.collect()
		})
		.collect()
}

/// What rotates the WAL in a storm.
#[derive(Clone, Copy, Debug)]
enum Rotator {
	/// `seal_active_wal_segment`, as a direct-to-L0 write does.
	Seal,
	/// `rotate_memtable`.
	Memtable,
}

struct Storm {
	label: &'static str,
	writers: usize,
	commits: usize,
	with_readers: bool,
	/// Every apply round waits for another rotation, so every unfenced round is overtaken.
	gated: bool,
	vlog: bool,
	rotators: Vec<Rotator>,
}

impl Storm {
	fn new(label: &'static str, writers: usize, commits: usize) -> Self {
		Self {
			label,
			writers,
			commits,
			with_readers: true,
			gated: true,
			vlog: false,
			rotators: vec![Rotator::Seal, Rotator::Memtable],
		}
	}

	/// Writers of both durabilities commit multi-key transactions of every size, in memtables
	/// that hold a few of them, while threads rotate the WAL without a pause and readers read.
	/// Whatever the interleaving: no commit fails, a crash image recovers exactly the
	/// acknowledged commits, the memtables and the segments agree, and no segment holds a record
	/// twice or out of order.
	async fn run(self) {
		let Self {
			label,
			writers,
			commits,
			with_readers,
			gated,
			vlog,
			rotators,
		} = self;
		let dir = TempDir::new("wal_stale_rounds").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(opts(&live, 8192, vlog)).unwrap());
		stop_background_tasks(&tree).await;

		let stop = Arc::new(AtomicBool::new(false));
		let rotations = Arc::new(AtomicU64::new(0));
		let mut threads = Vec::new();
		for rotator in rotators {
			let (tree, stop, rotations) =
				(Arc::clone(&tree), Arc::clone(&stop), Arc::clone(&rotations));
			threads.push(std::thread::spawn(move || {
				while !stop.load(Ordering::Relaxed) {
					match rotator {
						Rotator::Seal => {
							tree.core.inner.seal_active_wal_segment().unwrap();
						}
						Rotator::Memtable => tree.core.inner.rotate_memtable().unwrap(),
					}
					rotations.fetch_add(1, Ordering::SeqCst);
					std::thread::sleep(Duration::from_millis(1));
				}
			}));
		}
		let reads = Arc::new(AtomicU64::new(0));
		if with_readers {
			for r in 0..2usize {
				let (tree, stop, reads) =
					(Arc::clone(&tree), Arc::clone(&stop), Arc::clone(&reads));
				threads.push(std::thread::spawn(move || {
					let mut i = 0usize;
					while !stop.load(Ordering::Relaxed) {
						let key = format!("w{:02}_{:03}_0", (i + r) % writers, i % commits);
						let _ = tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key.as_bytes());
						reads.fetch_add(1, Ordering::Relaxed);
						i += 1;
					}
				}));
			}
		}
		let probe = {
			let inner = Arc::clone(&tree.core.inner);
			install(
				&tree,
				move |_| {
					if !gated {
						return;
					}
					// Wait for the active segment to change, so the records of this group are in an
					// older one by the time they are applied. A rotation of an empty memtable
					// changes nothing, so the number of rotations would not guarantee it.
					let seen = inner.wal.read().get_active_log_number();
					let waited = Instant::now();
					while inner.wal.read().get_active_log_number() == seen
						&& waited.elapsed() < Duration::from_secs(5)
					{
						std::thread::sleep(Duration::from_micros(100));
					}
				},
				|| {},
			)
		};

		let mut tasks = Vec::new();
		for w in 0..writers {
			let tree = Arc::clone(&tree);
			tasks.push(tokio::spawn(async move {
				let durability = if w % 2 == 0 {
					Durability::Immediate
				} else {
					Durability::Eventual
				};
				let mut acked = Vec::new();
				for commit in writer_plan(w, commits) {
					let mut txn = tree.begin().unwrap();
					txn.set_durability(durability);
					for (k, v) in &commit {
						txn.set(k.as_bytes(), v).unwrap();
					}
					let result = txn.commit().await;
					assert!(
						result.is_ok(),
						"{:?} {durability:?} was reported failed: {result:?}",
						commit.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>()
					);
					acked.extend(commit.into_iter().map(|(k, v)| (k.into_bytes(), v)));
				}
				acked
			}));
		}
		let mut acked = Vec::new();
		let mut failure = None;
		for task in tasks {
			match task.await {
				Ok(more) => acked.extend(more),
				Err(e) => failure = Some(e),
			}
		}
		stop.store(true, Ordering::Relaxed);
		for thread in threads {
			thread.join().unwrap();
		}
		assert_no_runaway(&probe, label);
		if let Some(e) = failure {
			std::panic::resume_unwind(e.into_panic());
		}
		acked.sort();
		let (rounds, fenced) =
			(probe.rounds.load(Ordering::SeqCst), probe.fenced.load(Ordering::SeqCst));
		if gated {
			assert!(fenced > 0, "{label}: no group was ever fenced ({rounds} rounds)");
		}
		assert!(rotations.load(Ordering::SeqCst) > 5, "{label}: the storm barely rotated");
		if with_readers {
			assert!(reads.load(Ordering::Relaxed) > 0, "{label}: the readers made no progress");
		}

		for (k, v) in &acked {
			assert_eq!(get(&tree, k).as_ref(), Some(v), "{label}: {}", String::from_utf8_lossy(k));
		}
		assert_segments_ordered(&live, label);
		assert_eq!(
			recover_image(&live, 8192, vlog),
			acked,
			"{label}: a crash image recovers exactly the acknowledged commits ({rounds} rounds, \
			 {fenced} fenced)"
		);
		dispose(&tree);
	}
}

/// Every apply round waits for another rotation, so groups straddle memtables inside the fence
/// and batches too big for a memtable go to L0 from it.
#[test]
fn a_rotation_storm_never_fails_a_logged_commit() {
	bounded(true, async {
		Storm::new("storm", 4, 6).run().await;
	});
}

/// The storm without the gate: the rotations and the groups race as they happen to, so groups
/// are overtaken at every point, not just before an apply round. There may be no fenced round
/// at all.
#[ignore = "slow: rotators that nothing holds back leave a long WAL to recover"]
#[test]
fn an_ungated_rotation_storm_never_fails_a_logged_commit() {
	bounded(true, async {
		Storm {
			gated: false,
			..Storm::new("ungated storm", 4, 8)
		}
		.run()
		.await;
	});
}

/// The same on the current-thread runtime, where the fenced round blocks the only thread the
/// writers' tasks run on.
#[test]
fn a_rotation_storm_never_fails_a_logged_commit_on_a_current_thread_runtime() {
	bounded(false, async {
		Storm::new("current thread storm", 3, 4).run().await;
	});
}

/// A deterministic pseudo-random number: the same seed and step give the same value.
fn mix(seed: u64, step: u64) -> u64 {
	let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ step.wrapping_mul(0xD6E8_FEB8_6659_FD93);
	x ^= x >> 32;
	x = x.wrapping_mul(0xD6E8_FEB8_6659_FD93);
	x ^= x >> 29;
	x
}

/// Groups of random sizes and values, among them values too big for a memtable and values for
/// the value log, are committed in both durabilities while a seeded schedule seals the WAL,
/// rotates the memtable or leaves the round alone at every apply round, until the round at which
/// it stops. Whatever the schedule: no commit fails, every key is visible, the memtables and the
/// segments agree, no segment holds a record twice, and a crash image holds exactly the
/// acknowledged commits.
#[test]
fn a_sweep_of_rotation_schedules_never_fails_a_logged_commit() {
	const SEEDS: u64 = 4;
	bounded(false, async {
		let mut fenced_rounds = 0;
		for seed in 0..SEEDS {
			let what = format!("seed {seed}");
			let max_memtable_size = if mix(seed, 1) % 2 == 0 {
				SMALL
			} else {
				BIG
			};
			let vlog = mix(seed, 2) % 3 == 0;
			let dir = TempDir::new("wal_stale_rounds").unwrap();
			let live = dir.path().join("live");
			let tree = Arc::new(Tree::new(opts(&live, max_memtable_size, vlog)).unwrap());
			stop_background_tasks(&tree).await;

			// Which group the rounds belong to, and what the schedule does in each.
			let group_no = Arc::new(AtomicUsize::new(0));
			let probe = {
				let (inner, group_no) = (Arc::clone(&tree.core.inner), Arc::clone(&group_no));
				install(
					&tree,
					move |round| {
						if round == 0 {
							group_no.fetch_add(1, Ordering::SeqCst);
						}
						if round < 10 {
							let step = (group_no.load(Ordering::SeqCst) * 64 + round) as u64;
							match mix(seed, step) % 4 {
								0 | 1 => {
									inner.seal_active_wal_segment().unwrap();
								}
								2 => inner.rotate_memtable().unwrap(),
								_ => {}
							}
						}
					},
					|| {},
				)
			};

			let mut expected = Vec::new();
			for g in 0..3u64 {
				let count = 1 + (mix(seed, 10 + g) % 14) as usize;
				let durability = if mix(seed, 20 + g) % 2 == 0 {
					Durability::Immediate
				} else {
					Durability::Eventual
				};
				let group: Vec<(String, Vec<u8>)> = (0..count)
					.map(|i| {
						let len = match mix(seed, 100 + g * 32 + i as u64) % 8 {
							0 if max_memtable_size == SMALL => SMALL + 500,
							1 if vlog => 3_000,
							2 => 300,
							_ => 60,
						};
						(
							format!("s{seed:02}g{g}k{i:02}"),
							value((seed * 1000 + g * 50) as usize + i, len),
						)
					})
					.collect();
				commit_all(&tree, &group, durability, &probe, &what).await;
				expected.extend(group);
			}
			let expected = pairs(&expected);
			for (k, v) in &expected {
				assert_eq!(
					get(&tree, k).as_ref(),
					Some(v),
					"{what}: {}",
					String::from_utf8_lossy(k)
				);
			}
			assert_memtables_match_their_segments(&tree, &live, &keys_of_pairs(&expected), &what);
			assert_segments_ordered(&live, &what);
			assert_eq!(
				recover_image(&live, max_memtable_size, vlog),
				expected,
				"{what}: a crash image holds exactly the acknowledged commits"
			);
			fenced_rounds += probe.fenced.load(Ordering::SeqCst);
			dispose(&tree);
		}
		assert!(fenced_rounds > 0, "no schedule of the sweep reached the fence");
	});
}

// ---------------------------------------------------------------------------
// with the global affinity pool
// ---------------------------------------------------------------------------

/// With the process-wide affinity pool installed (as a server does), the unfenced rounds hop to
/// a pool thread and the fenced round runs on the flusher's, and a group that every rotation
/// overtakes still commits, with a storm of writers too. Installing the pool is process-wide, so
/// this runs alone, by its full path, with `--ignored --exact`.
#[ignore = "installs the process-wide affinity pool: run it alone with --ignored --exact"]
#[test]
fn a_group_overtaken_by_every_rotation_commits_with_a_global_pool() {
	// An error means another test of this process installed one: the check is the same.
	let _ = affinitypool::Threadpool::new(1).build_global();
	bounded(true, async {
		for durability in [Durability::Immediate, Durability::Eventual] {
			let dir = TempDir::new("wal_stale_rounds").unwrap();
			let live = dir.path().join("live");
			let tree = Arc::new(Tree::new(opts(&live, SMALL, true)).unwrap());
			stop_background_tasks(&tree).await;
			let first = tag_of(&tree);
			let probe = seal_when(&tree, |_| true);

			let mut group = entries(0, 10, 100);
			group[3].1 = value(3, 3_000);
			group[6].1 = value(6, SMALL + 700);
			commit_all(&tree, &group, durability, &probe, "global pool").await;
			assert!(tag_of(&tree) > first + UNFENCED_STALE_ROUNDS as u64);
			for (k, v) in pairs(&group) {
				assert_eq!(get(&tree, &k), Some(v), "{durability:?}");
			}
			assert_eq!(recover_image(&live, SMALL, true), pairs(&group), "{durability:?}");
			dispose(&tree);
		}
		// The flusher awaits the pool for its appends and fsyncs while the fenced round runs
		// inline.
		Storm {
			vlog: true,
			..Storm::new("global pool storm", 4, 6)
		}
		.run()
		.await;
	});
}
