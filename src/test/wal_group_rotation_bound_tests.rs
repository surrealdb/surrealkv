//! A commit group rotates the memtable a bounded number of times.
//!
//! The memtable takes a batch only if the worst-case arena bytes of the batch (its estimate, which
//! is some 2 KiB per entry however small the entry) still fit, so a group of many small batches
//! used to rotate the memtable once per batch, logging the rest of the group again into the new
//! segment each time: a group of 64 commits of 32 entries at a 64 KiB memtable rotated 62 times and
//! wrote 31 times its payload. The flusher closes a group, as it does at `MAX_GROUP_BYTES`, when
//! its estimates would pass what a fresh memtable takes, so the rest of a group that rotated always
//! fits the memtable it rotated to.
//!
//! The groups are formed by the flusher from commits that pile up behind a held first group, so
//! each scenario goes through real commits (`flush_group` bypasses the gather). A rotation is
//! counted from inside the group: every `ArenaFull` rotation of the apply is one more apply round,
//! and the rotation before the append is the part of the segment change that the group before it
//! does not explain.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};

use tempdir::TempDir;
use test_log::test;
use tokio::task::JoinHandle;

use super::wal_group_budget_tests::{
	assert_all_acked,
	copy_dir,
	group_plan,
	mark_closed,
	options as default_options,
	outcome,
	release_lock,
	until,
	value_of,
	wal_batches,
};
use super::wal_sealed_tests::{context, read_records, sealed_context};
use super::{collect_transaction_all, tfs};
use crate::batch::Batch;
use crate::memtable::max_entry_bytes;
use crate::ring::PipelineHook;
use crate::tde::CipherSuite;
use crate::{Durability, InternalKeyKind, Mode, Options, Tree};

const KIB: usize = 1 << 10;
const MIB: usize = 1 << 20;

/// What a fresh arena of `max_memtable_size` takes of one group: the arena is rounded down to a
/// multiple of 64 bytes and the first 64 are its null region.
fn fresh_room(max_memtable_size: usize) -> u64 {
	((max_memtable_size & !63) - 64) as u64
}

fn options(path: &Path, max_memtable_size: usize, encrypted: bool) -> Arc<Options> {
	let opts = Options {
		path: path.to_path_buf(),
		max_memtable_size,
		// Crash semantics: nothing is flushed or removed on close.
		flush_on_close: false,
		// Nothing flushes, so nothing may stall behind the memtables that queue: the groups are
		// the subject, not the stall.
		memtable_stall_threshold: 1_000_000,
		l0_stall_threshold: 1_000_000,
		level0_max_files: 1_000_000,
		..Default::default()
	};
	Arc::new(if encrypted {
		opts.with_encryption_context(context(CipherSuite::Aes256Gcm))
	} else {
		opts
	})
}

/// Stops the background tasks, so that nothing is flushed or removed behind the test's back. The
/// commit flusher is not one of them.
async fn stop_background(tree: &Tree) {
	let tasks = tree.core.task_manager.lock().unwrap().take().unwrap();
	tasks.stop().await;
}

// ---------------------------------------------------------------------------
// what a group did
// ---------------------------------------------------------------------------

/// One group, as the pipeline hook saw it.
#[derive(Clone, Debug)]
struct GroupRecord {
	/// The batches of the group.
	batches: usize,
	/// The WAL segment that was active when the group's records were synced.
	segment: u64,
	/// The apply rounds of the group: one, and more for the rotations of the apply and the
	/// records they make the group log again.
	rounds: usize,
	/// The WAL segment that was active when the group's last apply round began. A rotation is
	/// always followed by a round, so nothing rotates after it.
	last_round_segment: u64,
}

/// Holds the flusher at the end of the WAL step of its first group and records every group.
struct Probe {
	groups: Arc<Mutex<Vec<GroupRecord>>>,
	held: Arc<AtomicBool>,
	release: mpsc::Sender<()>,
	/// The images of the directory taken at the hooks, if asked for: the index of the group, the
	/// apply round (none at the end of the WAL step) and the copy.
	images: Arc<Mutex<Vec<Image>>>,
}

/// An image of a crash at a hook of a group: its index, the apply round it was taken before
/// (none: the WAL step just ended), and the copy of the directory.
type Image = (usize, Option<usize>, std::path::PathBuf);

impl Probe {
	fn install(tree: &Tree) -> Probe {
		Self::install_taking_images(tree, None)
	}

	/// As `install`, and a copy of the directory `live` at every hook, into `images`.
	fn install_taking_images(
		tree: &Tree,
		take: Option<(std::path::PathBuf, std::path::PathBuf)>,
	) -> Probe {
		let images: Arc<Mutex<Vec<Image>>> = Arc::default();
		let sink_images = Arc::clone(&images);
		let copy = move |groups: &[GroupRecord], round: Option<usize>| {
			if let Some((live, root)) = &take {
				let mut images = sink_images.lock().unwrap();
				let to = root.join(format!("image_{}", images.len()));
				copy_dir(live, &to);
				images.push((groups.len() - 1, round, to));
			}
		};
		let groups: Arc<Mutex<Vec<GroupRecord>>> = Arc::default();
		let held = Arc::new(AtomicBool::new(false));
		let armed = Arc::new(AtomicBool::new(true));
		let (release, gate) = mpsc::channel::<()>();
		let gate = Mutex::new(gate);
		let (sink, flag) = (Arc::clone(&groups), Arc::clone(&held));
		let inner = Arc::clone(&tree.core.inner);
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| match point {
			PipelineHook::AfterWalSync {
				batches,
			} => {
				let segment = inner.wal.read().get_active_log_number();
				let mut groups = sink.lock().unwrap();
				groups.push(GroupRecord {
					batches,
					segment,
					rounds: 0,
					last_round_segment: segment,
				});
				copy(&groups, None);
				drop(groups);
				if armed.swap(false, Ordering::SeqCst) {
					flag.store(true, Ordering::SeqCst);
					// Dropping the sender (a failed test) lets the flusher go too.
					let _ = gate.lock().unwrap().recv();
					flag.store(false, Ordering::SeqCst);
				}
			}
			PipelineHook::BeforeApplyRound {
				..
			} => {
				let segment = inner.wal.read().get_active_log_number();
				let mut groups = sink.lock().unwrap();
				let group = groups.last_mut().expect("a group is synced before it is applied");
				group.rounds += 1;
				group.last_round_segment = segment;
				let round = group.rounds - 1;
				copy(&groups, Some(round));
			}
			_ => {}
		})));
		Probe {
			groups,
			held,
			release,
			images,
		}
	}

	fn is_held(&self) -> bool {
		self.held.load(Ordering::SeqCst)
	}

	fn release(&self) {
		self.release.send(()).unwrap();
	}

	fn groups(&self) -> Vec<GroupRecord> {
		self.groups.lock().unwrap().clone()
	}
}

/// Rotations of the memtable that each group made, before its append and during its apply, from
/// the segment each group was synced in and the one its last apply round began in. The change from
/// where the group before ended to where this one was synced is the rotation before its append.
fn rotations_per_group(groups: &[GroupRecord], first_segment: u64) -> Vec<(u64, u64)> {
	let mut out = Vec::new();
	let mut end_of_previous = first_segment;
	for group in groups {
		let before = group.segment - end_of_previous;
		let during = group.last_round_segment - group.segment;
		out.push((before, during));
		end_of_previous = group.last_round_segment;
	}
	out
}

fn key_of(commit: usize, entry: usize) -> Vec<u8> {
	format!("{commit:04}{entry:05}").into_bytes()
}

/// A commit of `entries` entries of 9 B and 9 B.
fn spawn_commit(tree: &Arc<Tree>, commit: usize, entries: usize) -> JoinHandle<crate::Result<()>> {
	let tree = Arc::clone(tree);
	tokio::spawn(async move {
		let mut txn = tree.begin().unwrap();
		// The image of a crash holds what a commit that is not synced left in the writer's buffer.
		txn.set_durability(Durability::Immediate);
		for e in 0..entries {
			txn.set(key_of(commit, e), key_of(commit, e)).unwrap();
		}
		txn.commit().await
	})
}

/// The estimate of the memtable for one commit of `entries` entries of 9 B and 9 B.
fn estimate_of(entries: usize) -> u64 {
	entries as u64 * max_entry_bytes(9, 9)
}

/// The groups that `batches` commits pile up into behind a held first group.
struct Pile {
	groups: Vec<GroupRecord>,
	images: Vec<Image>,
	first_segment: u64,
	dir: TempDir,
	tree: Arc<Tree>,
}

async fn pile_up(
	max_memtable_size: usize,
	entries: usize,
	batches: usize,
	encrypted: bool,
) -> Pile {
	pile_up_taking_images(max_memtable_size, entries, batches, encrypted, false).await
}

async fn pile_up_taking_images(
	max_memtable_size: usize,
	entries: usize,
	batches: usize,
	encrypted: bool,
	images: bool,
) -> Pile {
	let dir = TempDir::new("wal_group_rotation").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(options(&live, max_memtable_size, encrypted)).unwrap());
	stop_background(&tree).await;
	let take = images.then(|| {
		let root = dir.path().join("images");
		super::sfs::create_dir_all(&root).unwrap();
		(live.clone(), root)
	});
	let probe = Probe::install_taking_images(&tree, take);
	let first_segment = tree.core.inner.wal.read().get_active_log_number();

	let gate = spawn_commit(&tree, 0, entries);
	until("the flusher to hold its first group", || probe.is_held()).await;
	let pipeline = &tree.core.commit_pipeline;
	let handles: Vec<_> = (1..=batches).map(|c| spawn_commit(&tree, c, entries)).collect();
	// The held group's commit is accepted and not complete.
	until("the commits to be accepted", || pipeline.accepted_waiting() == batches + 1).await;
	probe.release();
	outcome(gate).await.unwrap();
	assert_all_acked(handles).await;
	pipeline.set_hook(None);

	// Every commit is readable.
	{
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for c in 0..=batches {
			for e in 0..entries {
				assert_eq!(
					txn.get(key_of(c, e)).unwrap().as_deref(),
					Some(key_of(c, e).as_slice()),
					"commit {c} entry {e}"
				);
			}
		}
	}
	let images = probe.images.lock().unwrap().clone();
	Pile {
		groups: probe.groups(),
		images,
		first_segment,
		dir,
		tree,
	}
}

impl Pile {
	fn live(&self) -> std::path::PathBuf {
		self.dir.path().join("live")
	}

	/// Leaves the tree as a crash does, and returns its directory.
	fn end(self) -> TempDir {
		mark_closed(&self.tree);
		release_lock(&self.tree);
		self.dir
	}
}

/// The memtable sizes, entries per commit and number of commits of the cells. A commit estimates
/// to about 95% of a fresh memtable, where one is that big, so that every commit but the first
/// of a group would rotate the memtable. 16 MiB is the default memtable of the browser backend,
/// 32 MiB is the last size at which a group may fill one fresh memtable, and 32 MiB and 64 bytes
/// is the first at which it may fill two. At most 512 commits are admitted while the flusher is
/// held.
const CELLS: [(usize, usize, usize); 7] = [
	(64 * KIB, 32, 64),
	(64 * KIB, 16, 64),
	(MIB, 500, 64),
	(4 * MIB, 2000, 64),
	(16 * MIB, 8000, 16),
	(32 * MIB, 16_000, 8),
	(32 * MIB + 64, 16_000, 8),
];

/// How many fresh memtables of room a group may fill, as `group_est_budget` decides it.
fn multiple_of(max_memtable_size: usize) -> u64 {
	(fresh_room(max_memtable_size) / (16 * MIB as u64)).clamp(1, 6)
}

/// A group rotates the memtable at most once, whatever its commits are, and a group of more than
/// one commit is no bigger than a fresh memtable takes.
async fn a_group_rotates_at_most_once(encrypted: bool) {
	for (max_memtable_size, entries, batches) in CELLS {
		let pile = pile_up(max_memtable_size, entries, batches, encrypted).await;
		let room = fresh_room(max_memtable_size);
		let est = estimate_of(entries);
		let rotations = rotations_per_group(&pile.groups, pile.first_segment);
		println!(
			"memtable {} KiB, {entries} entries per commit (estimate {est}), room {room}: groups {:?}, \
			 rotations (before, during) {:?}",
			max_memtable_size / KIB,
			pile.groups.iter().map(|g| g.batches).collect::<Vec<_>>(),
			rotations
		);
		let k = multiple_of(max_memtable_size);
		for (i, ((before, during), group)) in rotations.iter().zip(&pile.groups).enumerate() {
			// One memtable of room: a group rotates once. More: the rest of a group fits a fresh
			// memtable only after as many rotations as there are memtables of room, so it is
			// bounded by twice that and the one before the append, and a batch over the memtable
			// can share a group (see `group_est_budget`).
			let most = if k == 1 {
				1
			} else {
				2 * k + 1
			};
			assert!(
				before + during <= most,
				"memtable {} KiB: group {i} of {} commits rotated {} times before its append and {} \
				 times while it was applied",
				max_memtable_size / KIB,
				group.batches,
				before,
				during
			);
			assert!(
				group.batches == 1 || group.batches as u64 * est <= k * room,
				"memtable {} KiB: group {i} holds {} commits of estimate {est}, over the room {room}",
				max_memtable_size / KIB,
				group.batches
			);
		}
		assert_eq!(
			pile.groups.iter().map(|g| g.batches).sum::<usize>(),
			batches + 1,
			"every commit is in a group"
		);
		pile.end();
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_group_rotates_at_most_once_plain() {
	a_group_rotates_at_most_once(false).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_group_rotates_at_most_once_encrypted() {
	if tfs::skip_listed() {
		return;
	}
	a_group_rotates_at_most_once(true).await;
}

/// A batch is logged at most twice: where the flusher appended it, and once more if the memtable
/// rotated before it was applied. A group that rotated once per batch logged the rest of the group
/// again each time.
async fn a_batch_is_logged_at_most_twice(encrypted: bool) {
	for (max_memtable_size, entries, batches) in
		[(64 * KIB, 32, 64), (64 * KIB, 17, 64), (MIB, 32, 64)]
	{
		let pile = pile_up(max_memtable_size, entries, batches, encrypted).await;
		let mut records: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
		let live = pile.live();
		for batch in wal_batches_of(&live, encrypted) {
			*records.entry(batch.entries[0].key.clone()).or_default() += 1;
		}
		let worst = records.iter().max_by_key(|(_, n)| **n).unwrap();
		println!(
			"memtable {} KiB, {entries} entries: {} commits, most logged {} times ({})",
			max_memtable_size / KIB,
			records.len(),
			worst.1,
			String::from_utf8_lossy(worst.0)
		);
		assert_eq!(records.len(), batches + 1, "every commit is in the WAL");
		assert!(
			*worst.1 <= 2,
			"memtable {} KiB: the commit of key {} is in the WAL {} times",
			max_memtable_size / KIB,
			String::from_utf8_lossy(worst.0),
			worst.1
		);
		pile.end();
	}
}

/// The records of every segment of a live directory, in segment order.
fn wal_batches_of(live: &Path, encrypted: bool) -> Vec<Batch> {
	if !encrypted {
		return wal_batches(live);
	}
	let wal_context = sealed_context(&context(CipherSuite::Aes256Gcm));
	let mut segments = Vec::new();
	for entry in super::sfs::read_dir(live.join("wal")).unwrap() {
		let path = entry.unwrap().path();
		if path.extension().is_some_and(|ext| ext == "wal") {
			let id: u64 = path.file_stem().unwrap().to_str().unwrap().parse().unwrap();
			segments.push((id, path));
		}
	}
	segments.sort();
	let mut out = Vec::new();
	for (id, path) in segments {
		for record in read_records(&wal_context, &path, id).unwrap() {
			out.push(Batch::decode(&record).unwrap());
		}
	}
	out
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_batch_is_logged_at_most_twice_plain() {
	a_batch_is_logged_at_most_twice(false).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_batch_is_logged_at_most_twice_encrypted() {
	if tfs::skip_listed() {
		return;
	}
	a_batch_is_logged_at_most_twice(true).await;
}

// ---------------------------------------------------------------------------
// recovery of a crashed pile-up
// ---------------------------------------------------------------------------

/// The non-empty WAL segments of a directory.
fn non_empty_segments(dir: &Path) -> usize {
	super::sfs::read_dir(dir.join("wal"))
		.unwrap()
		.filter(|e| {
			let path = e.as_ref().unwrap().path();
			path.extension().is_some_and(|ext| ext == "wal")
				&& super::sfs::metadata(&path).unwrap().len() > 0
		})
		.count()
}

/// A crash right after a pile-up, with nothing flushed: replay turns each segment into a table,
/// and a segment into more than one only if its records overflow a memtable. The number of
/// non-empty segments K is counted from the directory listing before anything is opened, and the
/// open creates at most 2K - 1 tables: a segment holds its own batches and, if the memtable rotated
/// under its group, the rest of that group once more, and a memtable takes the whole of such a
/// segment twice at the most. This is the rule a pool that must know its files in advance sizes
/// itself by, whatever the queue was when the process died.
async fn recovery_creates_at_most_two_tables_per_segment(encrypted: bool) {
	for (max_memtable_size, entries, batches) in
		[(64 * KIB, 32, 64), (64 * KIB, 17, 64), (MIB, 17, 200)]
	{
		let pile = pile_up(max_memtable_size, entries, batches, encrypted).await;
		let live = pile.live();
		let image = pile.dir.path().join("image");
		copy_dir(&live, &image);
		let k = non_empty_segments(&image) as u64;
		let dir = pile.end();

		let tree = Tree::new(options(&image, max_memtable_size, encrypted)).unwrap();
		stop_background(&tree).await;
		let tables = tree.core.inner.l0_file_count() as u64;
		let queued = tree.core.inner.immutable_count();
		println!(
			"memtable {} KiB, {entries} entries, {batches} commits: K = {k} non-empty segments, \
			 recovery created {tables} tables ({queued} still queued)",
			max_memtable_size / KIB
		);
		assert!(
			tables < 2 * k,
			"recovery of {k} segments created {tables} tables (2K - 1 = {})",
			2 * k - 1
		);
		// Every commit survived the crash.
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for c in 0..=batches {
			for e in 0..entries {
				assert!(
					txn.get(key_of(c, e)).unwrap().is_some(),
					"commit {c} entry {e} after recovery"
				);
			}
		}
		drop(txn);
		mark_closed(&tree);
		release_lock(&tree);
		drop(dir);
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn recovery_creates_at_most_two_tables_per_segment_plain() {
	recovery_creates_at_most_two_tables_per_segment(false).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn recovery_creates_at_most_two_tables_per_segment_encrypted() {
	if tfs::skip_listed() {
		return;
	}
	recovery_creates_at_most_two_tables_per_segment(true).await;
}

// ---------------------------------------------------------------------------
// a crash at every hook of the groups the bound forms
// ---------------------------------------------------------------------------

/// Commits of 10 entries estimate to a third of a 64 KiB memtable, so the groups are of three, and
/// the memtable fills under them: some group is cut by a rotation before its append or in the
/// middle of its apply, and its tail is logged again. A crash image at the end of the WAL step of
/// a group and before each of its apply rounds recovers every commit of the groups before it and of
/// the group itself, whole (the group was synced when the image was taken), and none of the later
/// ones, and opens with at most two tables for each segment it replays.
async fn a_crash_at_every_hook_of_the_groups_the_bound_forms_recovers_a_prefix(encrypted: bool) {
	let (max_memtable_size, entries, batches) = (64 * KIB, 10, 200);
	let pile = pile_up_taking_images(max_memtable_size, entries, batches, encrypted, true).await;
	let cumulative: Vec<usize> = pile
		.groups
		.iter()
		.scan(0, |sum, g| {
			*sum += g.batches;
			Some(*sum)
		})
		.collect();
	let rounds: Vec<usize> = pile.groups.iter().map(|g| g.rounds).collect();
	println!(
		"groups {:?}, apply rounds {rounds:?}",
		pile.groups.iter().map(|g| g.batches).collect::<Vec<_>>()
	);
	assert!(
		pile.groups.iter().skip(1).all(|g| g.batches <= 3),
		"the bound forms groups of at most three"
	);
	assert!(
		rounds.iter().any(|r| *r > 1),
		"control: some group was cut by a rotation in the middle of its apply: {rounds:?}"
	);
	let images = pile.images.clone();
	let dir = pile.end();
	assert!(images.len() > rounds.len(), "an image per group and per round: {}", images.len());
	for (group, round, image) in images {
		let k = non_empty_segments(&image) as u64;
		let tree = Tree::new(options(&image, max_memtable_size, encrypted)).unwrap();
		stop_background(&tree).await;
		let tables = tree.core.inner.l0_file_count() as u64;
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		let mut recovered = 0;
		for c in 0..=batches {
			let found = (0..entries).filter(|e| txn.get(key_of(c, *e)).unwrap().is_some()).count();
			assert!(
				found == 0 || found == entries,
				"group {group} round {round:?}: commit {c} is torn, {found} of {entries} entries"
			);
			recovered += usize::from(found == entries);
		}
		assert_eq!(
			recovered, cumulative[group],
			"group {group} round {round:?}: the commits of the groups up to this one, and no others"
		);
		assert!(
			tables < 2 * k,
			"group {group} round {round:?}: {k} segments replayed into {tables} tables"
		);
		drop(txn);
		mark_closed(&tree);
		release_lock(&tree);
	}
	drop(dir);
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_crash_at_every_hook_of_the_groups_the_bound_forms_recovers_a_prefix_plain() {
	a_crash_at_every_hook_of_the_groups_the_bound_forms_recovers_a_prefix(false).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_crash_at_every_hook_of_the_groups_the_bound_forms_recovers_a_prefix_encrypted() {
	if tfs::skip_listed() {
		return;
	}
	a_crash_at_every_hook_of_the_groups_the_bound_forms_recovers_a_prefix(true).await;
}

/// The write that logs a cut group's tail again fails: the commits of the group are told so, the
/// database is stopped for the ones after it, and a crash image holds every commit that was
/// acknowledged and every other one whole or not at all. The groups here are the ones the bound
/// forms, and the first of them to be cut by a rotation is the one that fails.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_failure_in_the_log_again_of_a_cut_group_stops_the_database_and_recovers_whole_commits() {
	let (max_memtable_size, entries, batches) = (64 * KIB, 10, 200);
	let dir = TempDir::new("wal_group_relog_failure").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(options(&live, max_memtable_size, false)).unwrap());
	stop_background(&tree).await;

	let held = Arc::new(AtomicBool::new(false));
	let first = Arc::new(AtomicBool::new(true));
	let armed = Arc::new(AtomicBool::new(false));
	let (release, gate) = mpsc::channel::<()>();
	let gate = Mutex::new(gate);
	{
		let (held, first, armed) = (Arc::clone(&held), Arc::clone(&first), Arc::clone(&armed));
		let inner = Arc::clone(&tree.core.inner);
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| match point {
			PipelineHook::AfterWalSync {
				..
			} if first.swap(false, Ordering::SeqCst) => {
				held.store(true, Ordering::SeqCst);
				let _ = gate.lock().unwrap().recv();
			}
			PipelineHook::BeforeApplyRound {
				round: 1,
			} if !armed.swap(true, Ordering::SeqCst) => {
				inner.wal.write().fail_writes_after(0);
			}
			_ => {}
		})));
	}
	let gate_commit = spawn_commit(&tree, 0, entries);
	until("the flusher to hold its first group", || held.load(Ordering::SeqCst)).await;
	let pipeline = &tree.core.commit_pipeline;
	let handles: Vec<_> = (1..=batches).map(|c| spawn_commit(&tree, c, entries)).collect();
	until("the commits to be accepted", || pipeline.accepted_waiting() == batches + 1).await;
	release.send(()).unwrap();

	let mut results = vec![outcome(gate_commit).await];
	for handle in handles {
		results.push(outcome(handle).await);
	}
	assert!(armed.load(Ordering::SeqCst), "control: a group was cut by a rotation");
	let failed = results.iter().filter(|r| r.is_err()).count();
	assert!(failed > 0 && failed < results.len(), "{failed} of {} commits failed", results.len());
	assert!(
		results
			.iter()
			.filter_map(|r| r.as_ref().err())
			.all(|e| matches!(e, crate::Error::DatabaseStopped(_) | crate::Error::Io(_))),
		"the failure is the stop of the database: {results:?}"
	);

	let image = dir.path().join("image");
	copy_dir(&live, &image);
	pipeline.set_hook(None);
	mark_closed(&tree);
	release_lock(&tree);
	let recovered = Tree::new(options(&image, max_memtable_size, false)).unwrap();
	stop_background(&recovered).await;
	let txn = recovered.begin_with_mode(Mode::ReadOnly).unwrap();
	for (c, result) in results.iter().enumerate() {
		let found = (0..entries).filter(|e| txn.get(key_of(c, *e)).unwrap().is_some()).count();
		assert!(found == 0 || found == entries, "commit {c} is torn: {found} of {entries}");
		if result.is_ok() {
			assert_eq!(found, entries, "commit {c} was acknowledged and is not in the image");
		}
	}
	drop(txn);
	mark_closed(&recovered);
	release_lock(&recovered);
}

/// A group that a rotation cut logged its tail again, so for as long as the first segment is on
/// disk, a batch is in two segments, and a crash then replays both. The copies have the same keys,
/// sequence numbers and values, so the replay of the second changes nothing a read can see: the
/// commits of the images where that is so read back exactly, once, and no batch is in the WAL more
/// than twice.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_tail_that_is_in_two_segments_replays_without_effect() {
	let (max_memtable_size, entries, batches) = (64 * KIB, 10, 200);
	let pile = pile_up_taking_images(max_memtable_size, entries, batches, false, true).await;
	let images: Vec<Image> = pile.images.clone();
	let _dir = pile.end();
	let mut twice = 0;
	for (group, round, image) in images {
		if round.is_none() {
			continue;
		}
		let mut records: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
		for batch in wal_batches_of(&image, false) {
			*records.entry(batch.entries[0].key.clone()).or_default() += 1;
		}
		assert!(records.values().all(|n| *n <= 2), "group {group}: a batch is in the WAL 3 times");
		let doubled: Vec<Vec<u8>> =
			records.iter().filter(|(_, n)| **n == 2).map(|(k, _)| k.clone()).collect();
		if doubled.is_empty() {
			continue;
		}
		twice += 1;

		let tree = Tree::new(options(&image, max_memtable_size, false)).unwrap();
		stop_background(&tree).await;
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for key in doubled {
			// The first key of a commit says which commit it is: `{commit:04}{entry:05}`.
			let commit: usize = String::from_utf8_lossy(&key[..4]).parse().unwrap();
			for e in 0..entries {
				assert_eq!(
					txn.get(key_of(commit, e)).unwrap().as_deref(),
					Some(key_of(commit, e).as_slice()),
					"group {group}: commit {commit} entry {e} after its tail was replayed twice"
				);
			}
		}
		// No key of a commit that was never logged appears, and none is there twice.
		let all = collect_transaction_all(&mut txn.iter().unwrap()).unwrap();
		assert!(all.len() <= (batches + 1) * entries, "group {group}: {} rows", all.len());
		drop(txn);
		mark_closed(&tree);
		release_lock(&tree);
	}
	assert!(twice > 0, "control: some image holds a batch in two segments");
}

// ---------------------------------------------------------------------------
// the room of a fresh memtable, to the byte
// ---------------------------------------------------------------------------

/// A batch of one entry whose estimate is `estimate`, by the size of its value.
fn batch_with_estimate(seq: u64, estimate: u64) -> Batch {
	let mut batch = Batch::new(seq);
	let value = vec![7u8; (estimate - max_entry_bytes(8, 0)) as usize];
	batch.add_record(InternalKeyKind::Set, b"margin_k".to_vec(), Some(value), seq).unwrap();
	assert_eq!(batch.memtable_size_estimate(), estimate);
	batch
}

/// A batch that is as big as a fresh memtable takes is applied to the memtable that a rotation
/// brings, and goes down no direct path; one byte more cannot be applied to any memtable and does.
/// The bound that closes a group is exactly this room, so the rest of a group that rotated fits.
/// The memtable is asked for the estimate of the batch after the flusher wrapped its value (two
/// bytes more), so the batches here are two and one byte short of the room before they are encoded.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_batch_at_the_room_of_a_fresh_memtable_is_not_written_directly() {
	for max_memtable_size in [64 * KIB, MIB, 16 * MIB] {
		for (short, direct) in [(2u64, false), (1, true)] {
			let dir = TempDir::new("wal_group_margin").unwrap();
			let live = dir.path().join("live");
			let tree = Tree::new(options(&live, max_memtable_size, false)).unwrap();
			stop_background(&tree).await;
			// Something in the memtable, so that the batch needs a rotation first.
			let mut txn = tree.begin().unwrap();
			txn.set(b"seed", b"seed").unwrap();
			txn.commit().await.unwrap();

			let seq = tree.core.commit_pipeline.log_seq_num.fetch_add(1, Ordering::SeqCst);
			let batches = vec![batch_with_estimate(seq, fresh_room(max_memtable_size) - short)];
			tree.core.commit_pipeline.flush_group(&batches, true).await.unwrap();
			// A table of level zero is a direct write: a rotation only queues the memtable, and
			// nothing flushes it.
			let made = tree.core.inner.l0_file_count();
			assert_eq!(
				made > 0,
				direct,
				"memtable {} KiB, room - {short} before the value is wrapped: {made} tables written \
				 directly",
				max_memtable_size / KIB
			);
			mark_closed(&tree);
			release_lock(&tree);
		}
	}
}

// ---------------------------------------------------------------------------
// typical workloads are not changed
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Shape {
	name: &'static str,
	key: usize,
	value: usize,
	entries: usize,
	tombstone: bool,
}

const SHAPES: [Shape; 6] = [
	Shape {
		name: "50 B / 100 B",
		key: 50,
		value: 100,
		entries: 1,
		tombstone: false,
	},
	Shape {
		name: "8 B / 8 B",
		key: 8,
		value: 8,
		entries: 1,
		tombstone: false,
	},
	Shape {
		name: "16 B / 1 KiB",
		key: 16,
		value: 1024,
		entries: 1,
		tombstone: false,
	},
	Shape {
		name: "1000 x 8 B / 8 B",
		key: 8,
		value: 8,
		entries: 1000,
		tombstone: false,
	},
	Shape {
		name: "1000 x 8 B tombstones",
		key: 8,
		value: 0,
		entries: 1000,
		tombstone: true,
	},
	Shape {
		name: "1000 x 4 B / 0 B",
		key: 4,
		value: 0,
		entries: 1000,
		tombstone: false,
	},
];

/// A key of `shape.key` bytes that no other commit or entry shares.
fn shape_key(shape: Shape, commit: usize, entry: usize) -> Vec<u8> {
	let unique = ((commit as u64) << 16) | entry as u64;
	let bytes = unique.to_be_bytes();
	let mut key = vec![0u8; shape.key];
	let n = shape.key.min(8);
	key[shape.key - n..].copy_from_slice(&bytes[8 - n..]);
	key
}

fn shape_batch(shape: Shape, commit: usize) -> Batch {
	let mut batch = Batch::new(0);
	for e in 0..shape.entries {
		let key = shape_key(shape, commit, e);
		if shape.tombstone {
			batch.add_record(InternalKeyKind::Delete, key, None, 0).unwrap();
		} else {
			batch.add_record(InternalKeyKind::Set, key, Some(value_of(e, shape.value)), 0).unwrap();
		}
	}
	batch
}

/// The groups at the default memtable are the ones the byte rule gives, and the commits cost the
/// WAL what they did before: one record each, in one segment, with no rotation.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn groups_at_the_default_memtable_are_the_groups_of_the_byte_rule() {
	for shape in SHAPES {
		// Fewer than the ring admits while the flusher is held.
		let commits = 400;
		let dir = TempDir::new("wal_group_default").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(default_options(&live, false)).unwrap());
		let probe = Probe::install(&tree);
		let segment = tree.core.inner.wal.read().get_active_log_number();

		let gate = spawn_shape(&tree, shape, 0);
		until("the flusher to hold its first group", || probe.is_held()).await;
		let pipeline = &tree.core.commit_pipeline;
		let handles: Vec<_> = (1..=commits).map(|c| spawn_shape(&tree, shape, c)).collect();
		// The held group's commit is accepted and not complete.
		until("the commits to be accepted", || pipeline.accepted_waiting() == commits + 1).await;
		let hints: Vec<usize> =
			(1..=commits).map(|c| shape_batch(shape, c).encoded_len_hint()).collect();
		probe.release();
		outcome(gate).await.unwrap();
		assert_all_acked(handles).await;

		let groups = probe.groups();
		let mut expected = vec![1];
		expected.extend(group_plan(&hints));
		// Every commit of a shape is the same size, so the plan does not depend on ring order.
		let sizes: Vec<usize> = groups.iter().map(|g| g.batches).collect();
		let total_hint: usize = hints.iter().sum();
		println!("{}: {commits} commits, {total_hint} hint bytes, groups {sizes:?}", shape.name);
		assert_eq!(sizes.iter().sum::<usize>(), commits + 1);
		assert!(
			groups.iter().all(|g| g.rounds == 1 && g.segment == segment),
			"{}: a group applied in more than one round or rotated: {groups:?}",
			shape.name
		);
		assert_eq!(sizes, expected, "{}: groups of the byte rule", shape.name);
		pipeline.set_hook(None);
		mark_closed(&tree);
		release_lock(&tree);
	}
}

fn spawn_shape(tree: &Arc<Tree>, shape: Shape, commit: usize) -> JoinHandle<crate::Result<()>> {
	let tree = Arc::clone(tree);
	tokio::spawn(async move {
		let mut txn = tree.begin().unwrap();
		txn.set_durability(Durability::Eventual);
		for e in 0..shape.entries {
			let key = shape_key(shape, commit, e);
			if shape.tombstone {
				txn.delete(key).unwrap();
			} else {
				txn.set(key, value_of(e, shape.value)).unwrap();
			}
		}
		txn.commit().await
	})
}

/// The shapes of the typical workloads and the sizes of the batches, at the memtables of the tests
/// and of the smallest configurations: no group rotates the memtable more than once, whatever it is
/// made of. 40 piles of 64 commits each, 60 cells in all, through the real flusher.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn no_shape_of_commit_makes_a_group_rotate_more_than_once() {
	let mut worst = 0;
	let mut cells = 0;
	for max_memtable_size in [64 * KIB, MIB] {
		for (name, key, value, tombstone) in [
			("50 B / 100 B", 50, 100, false),
			("8 B / 8 B", 8, 8, false),
			("16 B / 1 KiB", 16, 1024, false),
			("8 B tombstones", 8, 0, true),
			("4 B / 0 B", 4, 0, false),
		] {
			for entries in [1, 4, 10, 16, 17, 32] {
				let shape = Shape {
					name,
					key,
					value,
					entries,
					tombstone,
				};
				let dir = TempDir::new("wal_group_sweep").unwrap();
				let live = dir.path().join("live");
				let tree = Arc::new(Tree::new(options(&live, max_memtable_size, false)).unwrap());
				stop_background(&tree).await;
				let probe = Probe::install(&tree);
				let first_segment = tree.core.inner.wal.read().get_active_log_number();

				let gate = spawn_shape(&tree, shape, 0);
				until("the flusher to hold its first group", || probe.is_held()).await;
				let pipeline = &tree.core.commit_pipeline;
				let handles: Vec<_> = (1..=64).map(|c| spawn_shape(&tree, shape, c)).collect();
				until("the commits to be accepted", || pipeline.accepted_waiting() == 65).await;
				probe.release();
				outcome(gate).await.unwrap();
				assert_all_acked(handles).await;

				for (i, (before, during)) in
					rotations_per_group(&probe.groups(), first_segment).iter().enumerate()
				{
					worst = worst.max(before + during);
					assert!(
						before + during <= 1,
						"{name}, {entries} entries, memtable {} KiB: group {i} rotated {before} + \
						 {during} times",
						max_memtable_size / KIB
					);
				}
				cells += 1;
				pipeline.set_hook(None);
				mark_closed(&tree);
				release_lock(&tree);
			}
		}
	}
	println!("{cells} cells, most rotations in one group: {worst}");
}
