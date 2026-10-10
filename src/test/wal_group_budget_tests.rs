//! A commit group is bounded in bytes.
//!
//! The commit flusher gathers the decided entries of the commit ring into one group, appends the
//! group to the WAL from one buffer, syncs once and applies it. Without a bound on the bytes of a
//! group, a pile-up of large commits needs one contiguous buffer as big as all of them together
//! (and the allocator aborts the process when it cannot give one). The flusher stops gathering at
//! `MAX_GROUP_BYTES` of batches, always takes at least one entry, and leaves the rest of the
//! decided entries to the next group.
//!
//! The flusher is held after the WAL write of a first group while the commits behind it queue, so
//! the groups that follow are formed from a known ring. Each scenario checks that the commits are
//! acknowledged and recoverable from a crash image, that the groups are the ones the byte rule
//! gives, and that the ring and the admission permits end up where a single group would leave
//! them. The scenarios that refuse an allocation, and the randomised workload, are in
//! `wal_group_budget_failure_tests`, which shares the helpers here.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;
use tokio::task::JoinHandle;

use crate::batch::Batch;
use crate::ring::{
	PipelineHook,
	COMMIT_RING_CAPACITY,
	MAX_GROUP_BYTES,
	RETIRE_EVERY_ENTRIES,
	RETIRE_EVERY_GROUPS,
};
use crate::wal::parallel_recovery::decode_segment_batches;
use crate::{Durability, Error, InternalKeyKind, Mode, Options, Tree};

pub(super) const MIB: usize = 1 << 20;

/// How long a scenario waits for the flusher: a commit that is never flushed fails the test
/// instead of hanging the suite.
pub(super) const WATCHDOG: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

pub(super) fn options(path: &Path, vlog: bool) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		// Crash semantics: nothing may be flushed on drop or close.
		flush_on_close: false,
		enable_vlog: vlog,
		..Default::default()
	})
}

/// Stops a deliberately crashed or throwaway tree from spawning a best-effort `close()` when it
/// is dropped.
pub(super) fn mark_closed(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
}

pub(super) fn release_lock(tree: &Tree) {
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}

pub(super) fn copy_dir(src: &Path, dst: &Path) {
	std::fs::create_dir_all(dst).unwrap();
	for entry in std::fs::read_dir(src).unwrap() {
		let entry = entry.unwrap();
		if entry.file_name() == "LOCK" {
			continue;
		}
		let to = dst.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &to);
		} else {
			std::fs::copy(entry.path(), &to).unwrap();
		}
	}
}

/// A value of `len` bytes that no other `index` shares.
pub(super) fn value_of(index: usize, len: usize) -> Vec<u8> {
	let mut x = (index as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
	(0..len)
		.map(|_| {
			x ^= x << 13;
			x ^= x >> 7;
			x ^= x << 17;
			(x >> 24) as u8
		})
		.collect()
}

/// Polls `done` until it holds, and fails the test if it never does: a commit that is never
/// flushed must not hang the suite.
pub(super) async fn until(what: &str, done: impl Fn() -> bool) {
	let deadline = Instant::now() + WATCHDOG;
	while !done() {
		assert!(Instant::now() < deadline, "timed out waiting for {what}");
		tokio::time::sleep(Duration::from_millis(2)).await;
	}
}

/// The outcome of a commit task, or a failure if the flusher never decided it.
pub(super) async fn outcome(handle: JoinHandle<crate::Result<()>>) -> crate::Result<()> {
	tokio::time::timeout(WATCHDOG, handle)
		.await
		.expect("a commit was never flushed: the flusher left it in the ring")
		.unwrap()
}

/// Every commit is acknowledged.
pub(super) async fn assert_all_acked(
	handles: impl IntoIterator<Item = JoinHandle<crate::Result<()>>>,
) {
	for handle in handles {
		outcome(handle).await.unwrap();
	}
}

/// What the flusher counts a commit of one `key` and `value` as against the budget.
pub(super) fn hint_of(key: &str, value: &[u8]) -> usize {
	let mut batch = Batch::new(0);
	batch
		.add_record(InternalKeyKind::Set, key.as_bytes().to_vec(), Some(value.to_vec()), 0)
		.unwrap();
	batch.encoded_len_hint()
}

/// The groups the byte rule forms from commits of these sizes, in ring order: a group takes the
/// next commit if the group is empty or the commit still fits the budget.
pub(super) fn group_plan(hints: &[usize]) -> Vec<usize> {
	let mut groups = Vec::new();
	let (mut count, mut bytes) = (0usize, 0usize);
	for hint in hints {
		if count > 0 && bytes + hint > MAX_GROUP_BYTES {
			groups.push(count);
			(count, bytes) = (0, 0);
		}
		count += 1;
		bytes += hint;
	}
	if count > 0 {
		groups.push(count);
	}
	groups
}

/// Holds the flusher after the WAL write of a group, and records every group.
pub(super) struct Flusher {
	/// The batch count of every group that reached the end of its WAL step.
	groups: Arc<Mutex<Vec<usize>>>,
	/// How many groups reached the end of their WAL step, the one being held included.
	reached: Arc<AtomicUsize>,
	/// Whether the flusher is parked in the hook.
	held: Arc<AtomicBool>,
	/// Whether the next group is held.
	armed: Arc<AtomicBool>,
	/// Whether the WAL had unsynced bytes at the end of an Immediate group's WAL step.
	unsynced: Arc<Mutex<Vec<bool>>>,
	/// At the same points: the free admission permits, and the entries published but not yet
	/// complete, each of which holds one.
	permits: Arc<Mutex<Vec<(usize, u64)>>>,
	release: mpsc::Sender<()>,
}

impl Flusher {
	pub(super) fn is_held(&self) -> bool {
		self.held.load(Ordering::SeqCst)
	}

	pub(super) fn reached(&self) -> usize {
		self.reached.load(Ordering::SeqCst)
	}

	pub(super) fn groups(&self) -> Vec<usize> {
		self.groups.lock().unwrap().clone()
	}

	/// Holds the next group too, once the one held is released.
	fn arm(&self) {
		self.armed.store(true, Ordering::SeqCst);
	}

	pub(super) fn release(&self) {
		self.release.send(()).unwrap();
	}

	/// A group gives back the permits of its entries only once they are complete: every entry
	/// that is published and not complete still holds its permit, so at no group's WAL step are
	/// there more free permits than the rest allow.
	pub(super) fn assert_permits_held_until_complete(&self, what: &str) {
		let total = COMMIT_RING_CAPACITY / 2;
		for (free, incomplete) in self.permits.lock().unwrap().iter() {
			assert!(
				*free + *incomplete as usize <= total,
				"{what}: {free} free permits with {incomplete} entries published and not complete"
			);
		}
	}
}

/// Holds the flusher after the WAL write of its first group.
pub(super) fn hold_first_group(tree: &Tree) -> Flusher {
	hold_groups(tree, false, || {})
}

/// Holds the flusher after the WAL write of every group, one release at a time.
fn hold_every_group(tree: &Tree) -> Flusher {
	hold_groups(tree, true, || {})
}

fn hold_groups(tree: &Tree, every: bool, probe: impl Fn() + Send + Sync + 'static) -> Flusher {
	let groups = Arc::new(Mutex::new(Vec::new()));
	let reached = Arc::new(AtomicUsize::new(0));
	let held = Arc::new(AtomicBool::new(false));
	let armed = Arc::new(AtomicBool::new(true));
	let unsynced = Arc::new(Mutex::new(Vec::new()));
	let permits = Arc::new(Mutex::new(Vec::new()));
	let (release, gate) = mpsc::channel::<()>();
	let gate = Mutex::new(gate);
	let (sink, count, flag, arm) =
		(Arc::clone(&groups), Arc::clone(&reached), Arc::clone(&held), Arc::clone(&armed));
	let (pending, permit_probe) = (Arc::clone(&unsynced), Arc::clone(&permits));
	let inner = Arc::clone(&tree.core.inner);
	// A weak handle: the pipeline owns the hook.
	let pipeline = Arc::downgrade(&tree.core.commit_pipeline);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::AfterWalSync {
			batches,
		} = point
		{
			sink.lock().unwrap().push(batches);
			pending.lock().unwrap().push(inner.wal.read().pending_sync());
			if let Some(pipeline) = pipeline.upgrade() {
				// Nothing gives a permit back while the flusher is parked here, so an entry that
				// the published count includes still holds its permit when the free ones are
				// read after it. In this order the sum cannot pass the total by a race.
				let (completed, ..) = pipeline.watermarks();
				let incomplete = pipeline.ring.published() - completed;
				permit_probe.lock().unwrap().push((pipeline.free_permits(), incomplete));
			}
			probe();
			count.fetch_add(1, Ordering::SeqCst);
			if arm.swap(false, Ordering::SeqCst) {
				flag.store(true, Ordering::SeqCst);
				// Dropping the sender (a failed test) lets the flusher go too.
				let _ = gate.lock().unwrap().recv();
				flag.store(false, Ordering::SeqCst);
				if every {
					arm.store(true, Ordering::SeqCst);
				}
			}
		}
	})));
	Flusher {
		groups,
		reached,
		held,
		armed,
		unsynced,
		permits,
		release,
	}
}

pub(super) fn commit(tree: &Arc<Tree>, key: &str, value: Vec<u8>) -> JoinHandle<crate::Result<()>> {
	commit_with(tree, key, value, Durability::Immediate)
}

pub(super) fn commit_with(
	tree: &Arc<Tree>,
	key: &str,
	value: Vec<u8>,
	durability: Durability,
) -> JoinHandle<crate::Result<()>> {
	let (tree, key) = (Arc::clone(tree), key.to_owned());
	tokio::spawn(async move {
		let mut txn = tree.begin().unwrap();
		txn.set_durability(durability);
		txn.set(key.as_bytes(), &value).unwrap();
		txn.commit().await
	})
}

/// Starts the commits one at a time, each only once the one before it is accepted into the ring,
/// so that ring order is the order of `items`. `already` entries are accepted and waiting.
pub(super) async fn queue_in_order(
	tree: &Arc<Tree>,
	items: &[(String, Vec<u8>)],
	already: usize,
) -> Vec<JoinHandle<crate::Result<()>>> {
	let pipeline = &tree.core.commit_pipeline;
	let mut handles = Vec::new();
	for (i, (key, value)) in items.iter().enumerate() {
		handles.push(commit(tree, key, value.clone()));
		until("a commit to be accepted", || pipeline.accepted_waiting() == already + i + 1).await;
	}
	handles
}

/// Every WAL record in `dir`, decoded as recovery decodes it, in segment order.
pub(super) fn wal_batches(dir: &Path) -> Vec<Batch> {
	let mut segments: Vec<(u64, PathBuf)> = Vec::new();
	for entry in std::fs::read_dir(dir.join("wal")).unwrap() {
		let path = entry.unwrap().path();
		if path.extension().is_some_and(|ext| ext == "wal") {
			let id: u64 = path.file_stem().unwrap().to_str().unwrap().parse().unwrap();
			segments.push((id, path));
		}
	}
	segments.sort();
	let mut out = Vec::new();
	for (id, path) in segments {
		out.extend(decode_segment_batches(&path, id).unwrap().1);
	}
	out
}

/// The bytes of every WAL segment in `dir`.
pub(super) fn wal_bytes(dir: &Path) -> BTreeMap<String, Vec<u8>> {
	std::fs::read_dir(dir.join("wal"))
		.unwrap()
		.map(|e| {
			let e = e.unwrap();
			(e.file_name().to_string_lossy().into_owned(), std::fs::read(e.path()).unwrap())
		})
		.collect()
}

fn keys_of(batches: &[Batch]) -> Vec<String> {
	batches
		.iter()
		.map(|b| {
			assert_eq!(b.entries.len(), 1, "every commit here is one entry");
			String::from_utf8(b.entries[0].key.clone()).unwrap()
		})
		.collect()
}

/// The WAL holds the commits in ring order, one record each, with sequence numbers that increase
/// without a gap across every group boundary.
pub(super) fn assert_wal_in_order(dir: &Path, expected_keys: &[String], what: &str) {
	let batches = wal_batches(dir);
	assert_eq!(keys_of(&batches), expected_keys, "{what}: records in commit order");
	for pair in batches.windows(2) {
		assert_eq!(
			pair[1].starting_seq_num,
			pair[0].starting_seq_num + u64::from(pair[0].count()),
			"{what}: sequence numbers are contiguous and increasing"
		);
	}
}

/// Copies the live directory (every acknowledged Immediate commit has reached the file and been
/// fsynced), opens the copy, and checks that every `(key, value)` is there and no `absent` key is.
pub(super) fn assert_recovers(
	live: &Path,
	image: &Path,
	vlog: bool,
	expected: &[(String, Vec<u8>)],
	absent: &[String],
	what: &str,
) {
	copy_dir(live, image);
	let recovered = Tree::new(options(image, vlog)).unwrap();
	{
		let txn = recovered.begin_with_mode(Mode::ReadOnly).unwrap();
		for (key, value) in expected {
			assert_eq!(
				txn.get(key.as_bytes()).unwrap().as_deref(),
				Some(value.as_slice()),
				"{what}: {key} after a crash"
			);
		}
		for key in absent {
			assert_eq!(txn.get(key.as_bytes()).unwrap(), None, "{what}: {key} after a crash");
		}
	}
	mark_closed(&recovered);
	release_lock(&recovered);
}

/// Opens a copy of the live directory, at the path of `opts`, and reads `keys` from it.
pub(super) fn recover_keys(
	live: &Path,
	opts: Arc<Options>,
	keys: &[String],
) -> Vec<Option<Vec<u8>>> {
	let _ = std::fs::remove_dir_all(&opts.path);
	copy_dir(live, &opts.path);
	let recovered = Tree::new(opts).unwrap();
	let values = {
		let txn = recovered.begin_with_mode(Mode::ReadOnly).unwrap();
		keys.iter().map(|key| txn.get(key.as_bytes()).unwrap()).collect()
	};
	mark_closed(&recovered);
	release_lock(&recovered);
	values
}

/// Exits the test binary if the guarded test does not finish in time. A flusher that strands an
/// entry leaves the committers that lap its ring slot spinning on the runtime's own threads, and
/// no timer on that runtime can fail the test then, so the limit is kept by a thread of its own.
struct ProcessWatchdog(Arc<AtomicBool>);

impl ProcessWatchdog {
	fn start(test: &'static str) -> ProcessWatchdog {
		let done = Arc::new(AtomicBool::new(false));
		let flag = Arc::clone(&done);
		std::thread::spawn(move || {
			let deadline = Instant::now() + 2 * WATCHDOG;
			while Instant::now() < deadline {
				if flag.load(Ordering::SeqCst) {
					return;
				}
				std::thread::sleep(Duration::from_millis(100));
			}
			eprintln!("{test}: the commit flusher stalled and blocked the runtime");
			std::process::exit(101);
		});
		ProcessWatchdog(done)
	}
}

impl Drop for ProcessWatchdog {
	fn drop(&mut self) {
		self.0.store(true, Ordering::SeqCst);
	}
}

/// No group of more than one commit is over the budget. `groups` are the sizes of the groups
/// after the held one, `hints` the sizes of the commits behind it, in ring order.
pub(super) fn assert_groups_within_budget(groups: &[usize], hints: &[usize], what: &str) {
	let mut offset = 0;
	for size in groups {
		let bytes: usize = hints[offset..offset + size].iter().sum();
		assert!(
			*size == 1 || bytes <= MAX_GROUP_BYTES,
			"{what}: a group of {size} commits is {bytes} bytes, over {MAX_GROUP_BYTES}"
		);
		offset += size;
	}
	assert_eq!(offset, hints.len(), "{what}: every commit is in a group");
}

/// Waits for every admission permit to come back, and checks the ring: every entry completed,
/// none left waiting, the retired watermark not past the completed prefix.
pub(super) async fn assert_ring_settled(tree: &Tree, entries: u64, what: &str) {
	let pipeline = &tree.core.commit_pipeline;
	until("every permit to come back", || pipeline.free_permits() == COMMIT_RING_CAPACITY / 2)
		.await;
	let (completed, taken, overflow) = pipeline.watermarks();
	assert_eq!(completed, entries, "{what}: every entry is complete");
	assert_eq!(pipeline.ring.published(), entries, "{what}: every entry is published");
	assert!(taken <= completed, "{what}: retired {taken} past completed {completed}");
	assert_eq!(overflow, 0, "{what}: nothing overflowed");
	assert_eq!(pipeline.accepted_waiting(), 0, "{what}: no entry is left waiting");
}

// ---------------------------------------------------------------------------
// the byte rule
// ---------------------------------------------------------------------------

/// Ten commits of about 1 MiB pile up behind a held group. The budget is 4 MiB, so they are
/// flushed three at a time, and the last alone: no group holds more than the budget, the commits
/// of a group are neighbours in ring order, and nothing is lost.
async fn pile_up_of_large_commits(vlog: bool) {
	const COMMITS: usize = 10;
	let dir = TempDir::new("wal_group_budget").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(options(&live, vlog)).unwrap());
	let flusher = hold_first_group(&tree);

	let first = ("first".to_string(), value_of(0, 100));
	let gate = commit(&tree, &first.0, first.1.clone());
	until("the flusher to hold its first group", || flusher.is_held()).await;

	let items: Vec<(String, Vec<u8>)> =
		(0..COMMITS).map(|i| (format!("big_{i:02}"), value_of(i + 1, MIB))).collect();
	let handles = queue_in_order(&tree, &items, 1).await;

	let hints: Vec<usize> = items.iter().map(|(k, v)| hint_of(k, v)).collect();
	let plan = group_plan(&hints);
	assert!(
		plan.iter().any(|n| *n > 1),
		"control: the budget lets commits share a group: {plan:?}"
	);
	assert!(plan.len() > 1, "control: the pile-up does not fit one group: {plan:?}");

	flusher.release();
	outcome(gate).await.unwrap();
	assert_all_acked(handles).await;

	let mut expected_groups = vec![1];
	expected_groups.extend(&plan);
	assert_eq!(flusher.groups(), expected_groups, "groups of at most {MAX_GROUP_BYTES} bytes");
	assert_groups_within_budget(&flusher.groups()[1..], &hints, "pile-up");
	flusher.assert_permits_held_until_complete("pile-up");
	assert!(
		flusher.unsynced.lock().unwrap().iter().all(|pending| !pending),
		"an Immediate group is synced before it is applied"
	);
	assert_ring_settled(&tree, 1 + COMMITS as u64, "pile-up").await;

	let mut all = vec![first];
	all.extend(items);
	let keys: Vec<String> = all.iter().map(|(k, _)| k.clone()).collect();
	assert_wal_in_order(&live, &keys, "pile-up");
	let last = wal_batches(&live).last().unwrap().get_highest_seq_num();
	assert_eq!(
		tree.core.inner.visible_seq_num.load(Ordering::SeqCst),
		last,
		"the last group made its commits visible"
	);
	{
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for (key, value) in &all {
			assert_eq!(
				txn.get(key.as_bytes()).unwrap().as_deref(),
				Some(value.as_slice()),
				"{key}"
			);
		}
	}
	assert_recovers(&live, &dir.path().join("image"), vlog, &all, &[], "pile-up");

	tree.core.commit_pipeline.set_hook(None);
	mark_closed(&tree);
	release_lock(&tree);
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_pile_up_of_large_commits_is_flushed_in_groups_within_the_byte_budget() {
	pile_up_of_large_commits(false).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_pile_up_of_large_commits_with_a_value_log_is_flushed_in_groups_within_the_byte_budget() {
	pile_up_of_large_commits(true).await;
}

/// A commit over the budget is a group of its own, wherever it sits in the queue: it does not
/// join the commits before it, and the commits after it do not join it.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_batch_over_the_budget_is_a_group_of_its_own() {
	let big = || value_of(99, MAX_GROUP_BYTES + MIB);
	let small = |i: usize| value_of(i, 100);
	let cases: Vec<(&str, Vec<Vec<u8>>)> = vec![
		("big between small", vec![small(1), big(), small(2), small(3)]),
		("big first", vec![big(), small(1), small(2)]),
		("big last", vec![small(1), small(2), big()]),
		("two big", vec![big(), big(), small(1)]),
	];
	for (name, values) in cases {
		let dir = TempDir::new("wal_group_budget").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(options(&live, false)).unwrap());
		let flusher = hold_first_group(&tree);

		let first = ("first".to_string(), value_of(0, 100));
		let gate = commit(&tree, &first.0, first.1.clone());
		until("the flusher to hold its first group", || flusher.is_held()).await;

		let items: Vec<(String, Vec<u8>)> =
			values.into_iter().enumerate().map(|(i, v)| (format!("item_{i}"), v)).collect();
		let handles = queue_in_order(&tree, &items, 1).await;
		let hints: Vec<usize> = items.iter().map(|(k, v)| hint_of(k, v)).collect();
		let plan = group_plan(&hints);
		assert!(
			hints.iter().any(|hint| *hint > MAX_GROUP_BYTES),
			"{name}: control: one commit is over the budget"
		);

		flusher.release();
		outcome(gate).await.unwrap();
		assert_all_acked(handles).await;

		let mut expected_groups = vec![1];
		expected_groups.extend(&plan);
		assert_eq!(flusher.groups(), expected_groups, "{name}");
		assert_groups_within_budget(&flusher.groups()[1..], &hints, name);
		flusher.assert_permits_held_until_complete(name);
		assert_ring_settled(&tree, 1 + items.len() as u64, name).await;

		let mut all = vec![first];
		all.extend(items);
		let keys: Vec<String> = all.iter().map(|(k, _)| k.clone()).collect();
		assert_wal_in_order(&live, &keys, name);
		assert_recovers(&live, &dir.path().join("image"), false, &all, &[], name);

		tree.core.commit_pipeline.set_hook(None);
		mark_closed(&tree);
		release_lock(&tree);
	}
}

/// Every admission permit is taken and more commits wait for one. The accepted entries are split
/// at the budget, each group gives its permits back as it completes, and the commits that were
/// blocked on admission are let in and flushed too.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_pile_up_that_exhausts_admission_is_split_and_every_permit_comes_back() {
	let permits = COMMIT_RING_CAPACITY / 2;
	let extra = 100;
	let value_len = 32 * 1024;
	let dir = TempDir::new("wal_group_budget").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(options(&live, false)).unwrap());
	let pipeline = &tree.core.commit_pipeline;
	let flusher = hold_first_group(&tree);

	let first = ("first".to_string(), value_of(0, 100));
	let gate = commit(&tree, &first.0, first.1.clone());
	until("the flusher to hold its first group", || flusher.is_held()).await;

	let items: Vec<(String, Vec<u8>)> = (0..permits - 1 + extra)
		.map(|i| (format!("key_{i:05}"), value_of(i + 1, value_len)))
		.collect();
	let handles: Vec<_> = items.iter().map(|(k, v)| commit(&tree, k, v.clone())).collect();
	until("admission to run out", || pipeline.free_permits() == 0).await;
	until("every permit holder to be accepted", || pipeline.accepted_waiting() == permits).await;
	let per_group = MAX_GROUP_BYTES / hint_of("key_00000", &value_of(1, value_len));
	assert!(permits - 1 > 3 * per_group, "control: a full ring is several groups");

	flusher.release();
	outcome(gate).await.unwrap();
	assert_all_acked(handles).await;

	let groups = flusher.groups();
	assert_eq!(groups[0], 1);
	assert_eq!(groups.iter().sum::<usize>(), 1 + items.len(), "every commit was flushed once");
	assert!(
		groups.iter().all(|n| *n <= per_group),
		"no group is over {per_group} commits of {value_len} bytes: {groups:?}"
	);
	assert!(
		groups.len() > items.len().div_ceil(per_group),
		"the {} commits took {} groups: {groups:?}",
		items.len(),
		groups.len()
	);
	flusher.assert_permits_held_until_complete("admission");
	assert_ring_settled(&tree, 1 + items.len() as u64, "admission").await;

	let mut all = vec![first];
	all.extend(items);
	let image = dir.path().join("image");
	assert_recovers(&live, &image, false, &all, &[], "admission");
	// The blocked commits took their ring slots in an order of their own; the WAL still holds
	// every commit once, with sequence numbers that increase.
	let batches = wal_batches(&live);
	assert_eq!(batches.len(), all.len());
	for pair in batches.windows(2) {
		assert_eq!(pair[1].starting_seq_num, pair[0].starting_seq_num + 1, "contiguous");
	}

	tree.core.commit_pipeline.set_hook(None);
	mark_closed(&tree);
	release_lock(&tree);
}

/// `close` while the flusher is held and commits are queued behind it: the flusher flushes every
/// accepted entry, across as many groups as the budget makes, before it exits. Nothing accepted
/// is dropped, and a reopened tree has all of it.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn shutdown_flushes_every_accepted_entry_across_split_groups() {
	const COMMITS: usize = 10;
	let dir = TempDir::new("wal_group_budget").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(options(&live, false)).unwrap());
	let flusher = hold_first_group(&tree);

	let first = ("first".to_string(), value_of(0, 100));
	let gate = commit(&tree, &first.0, first.1.clone());
	until("the flusher to hold its first group", || flusher.is_held()).await;

	let items: Vec<(String, Vec<u8>)> =
		(0..COMMITS).map(|i| (format!("big_{i:02}"), value_of(i + 1, MIB))).collect();
	let handles = queue_in_order(&tree, &items, 1).await;
	let hints: Vec<usize> = items.iter().map(|(k, v)| hint_of(k, v)).collect();
	let plan = group_plan(&hints);
	assert!(plan.len() > 1, "control: the entries are several groups: {plan:?}");

	let closer = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move { tree.close().await })
	};
	until("the pipeline to shut down", || {
		tree.core.commit_pipeline.shutdown.load(Ordering::SeqCst)
	})
	.await;
	// Still held: nothing past the first group has been flushed.
	assert_eq!(flusher.groups(), [1]);
	flusher.release();

	tokio::time::timeout(WATCHDOG, closer)
		.await
		.expect("close never finished: the flusher left entries behind")
		.unwrap()
		.unwrap();
	outcome(gate).await.unwrap();
	assert_all_acked(handles).await;
	let mut expected_groups = vec![1];
	expected_groups.extend(&plan);
	assert_eq!(flusher.groups(), expected_groups);
	flusher.assert_permits_held_until_complete("shutdown");

	let mut all = vec![first];
	all.extend(items);
	let keys: Vec<String> = all.iter().map(|(k, _)| k.clone()).collect();
	assert_wal_in_order(&live, &keys, "shutdown");

	tree.core.commit_pipeline.set_hook(None);
	drop(tree);
	let reopened = Tree::new(options(&live, false)).unwrap();
	{
		let txn = reopened.begin_with_mode(Mode::ReadOnly).unwrap();
		for (key, value) in &all {
			assert_eq!(
				txn.get(key.as_bytes()).unwrap().as_deref(),
				Some(value.as_slice()),
				"{key} after close and reopen"
			);
		}
	}
	mark_closed(&reopened);
	release_lock(&reopened);
}

// ---------------------------------------------------------------------------
// entries that are already complete on either side of a cut
// ---------------------------------------------------------------------------

enum Item {
	/// A commit of this many bytes.
	Big(usize),
	/// A commit that writes the key of item `n` (`GATE` for the first commit) from a snapshot
	/// older than it: it is aborted by validation, so its ring entry is complete and has no
	/// payload.
	Conflict(usize),
}

const GATE: usize = usize::MAX;

/// Aborted entries are consumed with the group they sit in, or wait for the next one, and cost
/// nothing against the budget: whatever their position next to a cut or inside a group, the
/// accepted commits form the groups the byte rule gives, every aborted commit gets its error, and
/// every permit is back at the end.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn aborted_entries_around_a_cut_cost_nothing_and_leave_no_permit_behind() {
	use Item::*;
	let big = |n: usize| Big(n * MIB / 2);
	let layouts: Vec<(&str, Vec<Item>)> = vec![
		(
			"aborted before, at and after the cut",
			vec![big(3), big(3), Conflict(0), big(3), big(3), Conflict(3), Conflict(4), big(3)],
		),
		("aborted first", vec![Conflict(GATE), Conflict(GATE), big(3), big(3), big(3)]),
		(
			"aborted inside a group that fits the budget",
			vec![big(2), Conflict(0), big(2), Conflict(2), big(2)],
		),
		("aborted last", vec![big(3), big(3), big(3), Conflict(0), Conflict(1)]),
		(
			"aborted between over-budget entries",
			vec![big(9), Conflict(0), big(9), Conflict(2), big(1), Conflict(4), big(9)],
		),
	];
	for (name, items) in layouts {
		let dir = TempDir::new("wal_group_budget").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(options(&live, false)).unwrap());
		let pipeline = &tree.core.commit_pipeline;
		let flusher = hold_first_group(&tree);

		// Every transaction that will conflict starts before anything else is committed.
		let mut stale: Vec<Option<crate::Transaction>> = items
			.iter()
			.map(|item| matches!(item, Conflict(_)).then(|| tree.begin().unwrap()))
			.collect();

		let first = commit(&tree, "first", value_of(0, 100));
		until("the flusher to hold its first group", || flusher.is_held()).await;

		let mut handles: Vec<(usize, JoinHandle<crate::Result<()>>)> = Vec::new();
		let mut published = 1u64;
		let mut accepted = 1usize;
		let mut hints = Vec::new();
		let mut expected: Vec<(String, Vec<u8>)> = vec![("first".into(), value_of(0, 100))];
		for (i, item) in items.iter().enumerate() {
			match item {
				Big(len) => {
					let key = format!("big_{i:02}");
					let value = value_of(i + 1, *len);
					hints.push(hint_of(&key, &value));
					expected.push((key.clone(), value.clone()));
					handles.push((i, commit(&tree, &key, value)));
					accepted += 1;
					published += 1;
					until("a commit to be accepted", || pipeline.accepted_waiting() == accepted)
						.await;
				}
				Conflict(target) => {
					let key = if *target == GATE {
						"first".to_string()
					} else {
						format!("big_{target:02}")
					};
					let mut txn = stale[i].take().unwrap();
					handles.push((
						i,
						tokio::spawn(async move {
							txn.set(key.as_bytes(), b"conflicting").unwrap();
							txn.commit().await
						}),
					));
					published += 1;
					until("a conflicting commit to be decided", || {
						pipeline.ring.published() == published
					})
					.await;
				}
			}
		}
		let plan = group_plan(&hints);

		flusher.release();
		outcome(first).await.unwrap();
		for (i, handle) in handles {
			let result = outcome(handle).await;
			match &items[i] {
				Big(_) => result.unwrap_or_else(|e| panic!("{name}: item {i} failed: {e:?}")),
				Conflict(_) => assert!(
					matches!(result, Err(Error::TransactionWriteConflict)),
					"{name}: item {i} should conflict, got {result:?}"
				),
			}
		}

		let mut expected_groups = vec![1];
		expected_groups.extend(&plan);
		assert_eq!(flusher.groups(), expected_groups, "{name}: groups of accepted commits only");
		flusher.assert_permits_held_until_complete(name);
		assert_ring_settled(&tree, published, name).await;

		let keys: Vec<String> = expected.iter().map(|(k, _)| k.clone()).collect();
		assert_wal_in_order(&live, &keys, name);
		assert_recovers(&live, &dir.path().join("image"), false, &expected, &[], name);

		pipeline.set_hook(None);
		mark_closed(&tree);
		release_lock(&tree);
	}
}

/// Admission runs out with aborted entries among the accepted ones: every permit is held, by a
/// commit waiting to be flushed or by an aborted entry the flusher has not passed yet, and more
/// commits wait for one. The flusher splits what it holds at the budget, passes the aborted
/// entries on the way, each sub-group gives back the permits of the entries it consumed, and the
/// waiting commits are admitted and flushed.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn admission_exhausted_by_accepted_and_aborted_entries_drains_through_split_groups() {
	let permits = COMMIT_RING_CAPACITY / 2;
	let (bigs, conflicts, extra) = (permits / 2 - 1, permits / 2, 100usize);
	assert_eq!(1 + bigs + conflicts, permits);
	let value_len = 32 * 1024;
	let dir = TempDir::new("wal_group_budget").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(options(&live, false)).unwrap());
	let pipeline = &tree.core.commit_pipeline;
	let flusher = hold_first_group(&tree);

	// The transactions that will conflict start before the commit they conflict with.
	let mut stale: Vec<crate::Transaction> =
		(0..conflicts).map(|_| tree.begin().unwrap()).collect();

	let first = ("first".to_string(), value_of(0, 100));
	let gate = commit(&tree, &first.0, first.1.clone());
	until("the flusher to hold its first group", || flusher.is_held()).await;

	let items: Vec<(String, Vec<u8>)> =
		(0..bigs + extra).map(|i| (format!("big_{i:04}"), value_of(i + 1, value_len))).collect();
	let conflict = |mut txn: crate::Transaction| {
		tokio::spawn(async move {
			txn.set(b"big_0000", b"conflicting").unwrap();
			txn.commit().await
		})
	};
	// Ring order: the gate commit, then a commit and an aborted one in turn, then the last
	// aborted one.
	let mut handles: Vec<JoinHandle<crate::Result<()>>> = Vec::new();
	let mut conflicting: Vec<JoinHandle<crate::Result<()>>> = Vec::new();
	for (i, (key, value)) in items.iter().take(bigs).enumerate() {
		handles.push(commit(&tree, key, value.clone()));
		until("a commit to be accepted", || pipeline.accepted_waiting() == 2 + i).await;
		conflicting.push(conflict(stale.pop().unwrap()));
		until("an aborted entry to be published", || {
			pipeline.ring.published() == (2 + 2 * i) as u64 + 1
		})
		.await;
	}
	conflicting.push(conflict(stale.pop().unwrap()));
	until("admission to run out", || pipeline.free_permits() == 0).await;
	assert_eq!(pipeline.ring.published(), permits as u64);
	assert_eq!(pipeline.accepted_waiting(), 1 + bigs);

	// These wait for a permit.
	for (key, value) in items.iter().skip(bigs) {
		handles.push(commit(&tree, key, value.clone()));
	}
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert_eq!(pipeline.ring.published(), permits as u64, "the rest wait for admission");

	flusher.release();
	outcome(gate).await.unwrap();
	assert_all_acked(handles).await;
	for handle in conflicting {
		let result = outcome(handle).await;
		assert!(matches!(result, Err(Error::TransactionWriteConflict)), "{result:?}");
	}

	let groups = flusher.groups();
	let per_group = MAX_GROUP_BYTES / hint_of(&items[0].0, &items[0].1);
	assert_eq!(groups[0], 1);
	assert_eq!(groups.iter().sum::<usize>(), 1 + bigs + extra, "every accepted commit once");
	assert!(groups.iter().all(|n| *n <= per_group), "{groups:?} against {per_group} per group");
	assert!(groups.len() > (bigs + extra).div_ceil(per_group), "split at the budget: {groups:?}");
	flusher.assert_permits_held_until_complete("aborted and accepted");
	assert_ring_settled(&tree, (permits + extra) as u64, "aborted and accepted").await;

	let all: Vec<(String, Vec<u8>)> = std::iter::once(first).chain(items).collect();
	assert_recovers(&live, &dir.path().join("image"), false, &all, &[], "aborted and accepted");
	let batches = wal_batches(&live);
	assert_eq!(batches.len(), all.len(), "one record per accepted commit");
	for pair in batches.windows(2) {
		assert_eq!(pair[1].starting_seq_num, pair[0].starting_seq_num + 1, "contiguous");
	}

	pipeline.set_hook(None);
	mark_closed(&tree);
	release_lock(&tree);
}

// ---------------------------------------------------------------------------
// a ring that is lapped, and the retire interval
// ---------------------------------------------------------------------------

/// More commits than the ring has slots, behind a held flusher, so the ring is lapped while the
/// accepted entries are split at the budget: every commit is flushed once and acknowledged, the
/// completed prefix reaches the last slot, and the retired watermark does not pass it.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_pile_up_that_laps_the_ring_is_split_and_the_ring_stays_consistent() {
	let _watchdog = ProcessWatchdog::start(
		"a_pile_up_that_laps_the_ring_is_split_and_the_ring_stays_consistent",
	);
	let permits = COMMIT_RING_CAPACITY / 2;
	let total = COMMIT_RING_CAPACITY + permits;
	let value_len = 16 * 1024;
	let dir = TempDir::new("wal_group_budget").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(options(&live, false)).unwrap());
	let pipeline = &tree.core.commit_pipeline;
	let flusher = hold_first_group(&tree);

	let first = ("first".to_string(), value_of(0, 100));
	let gate = commit(&tree, &first.0, first.1.clone());
	until("the flusher to hold its first group", || flusher.is_held()).await;

	let items: Vec<(String, Vec<u8>)> =
		(0..total).map(|i| (format!("key_{i:05}"), value_of(i + 1, value_len))).collect();
	let handles: Vec<_> = items.iter().map(|(k, v)| commit(&tree, k, v.clone())).collect();
	until("admission to run out", || pipeline.free_permits() == 0).await;
	until("every permit holder to be accepted", || pipeline.accepted_waiting() == permits).await;
	let per_group = MAX_GROUP_BYTES / hint_of("key_00000", &value_of(1, value_len));

	flusher.release();
	outcome(gate).await.unwrap();
	assert_all_acked(handles).await;

	let groups = flusher.groups();
	assert_eq!(groups[0], 1);
	assert_eq!(groups.iter().sum::<usize>(), 1 + total, "every commit was flushed once");
	assert!(groups.iter().all(|n| *n <= per_group), "a group is over {per_group}: {groups:?}");
	assert!(groups.len() > total.div_ceil(per_group), "the commits were split: {groups:?}");

	until("every permit to come back", || pipeline.free_permits() == permits).await;
	let published = pipeline.ring.published();
	assert_eq!(published as usize, 1 + total);
	assert!(published as usize > COMMIT_RING_CAPACITY, "control: the ring was lapped");
	let (completed, taken, overflow) = pipeline.watermarks();
	assert_eq!(completed, published, "every entry is complete");
	assert!(taken <= completed, "retired {taken} past completed {completed}");
	assert!(
		overflow as u64 + COMMIT_RING_CAPACITY as u64 <= published,
		"{overflow} entries are kept in the overflow map, more than the ring has lapped"
	);
	assert_eq!(pipeline.accepted_waiting(), 0);

	let mut all = vec![first];
	all.extend(items);
	let batches = wal_batches(&live);
	assert_eq!(batches.len(), all.len());
	for pair in batches.windows(2) {
		assert_eq!(pair[1].starting_seq_num, pair[0].starting_seq_num + 1, "contiguous");
	}
	assert_recovers(&live, &dir.path().join("image"), false, &all, &[], "lapped ring");

	pipeline.set_hook(None);
	mark_closed(&tree);
	release_lock(&tree);
}

/// Retire is due after `RETIRE_EVERY_ENTRIES` drained entries as well as after
/// `RETIRE_EVERY_GROUPS` groups, and a group that is cut drains only the entries it took. Waves
/// of commits that each split into two groups drain more than the entry limit in fewer groups
/// than the group limit, so the retired watermark moves only if the flusher counts the entries
/// of every group it forms.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn split_groups_count_their_entries_towards_the_retire_interval() {
	const WAVE: usize = 300;
	const WAVES: usize = 5;
	const VALUE: usize = 16 * 1024;
	let _watchdog =
		ProcessWatchdog::start("split_groups_count_their_entries_towards_the_retire_interval");
	let permits = COMMIT_RING_CAPACITY / 2;
	let dir = TempDir::new("wal_group_budget").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(options(&live, false)).unwrap());
	let pipeline = &tree.core.commit_pipeline;
	let flusher = hold_first_group(&tree);
	let per_group = MAX_GROUP_BYTES / hint_of("w0_0000", &value_of(1, VALUE));
	assert!(per_group < WAVE && WAVE < 2 * per_group, "control: a wave is two groups");
	let groups_per_wave = 3;
	assert!(WAVES * groups_per_wave < RETIRE_EVERY_GROUPS as usize, "control: too few groups");
	assert!(
		(WAVES * (1 + WAVE)) as u64 >= RETIRE_EVERY_ENTRIES,
		"control: enough entries for the entry limit"
	);

	let mut expected_groups = Vec::new();
	for wave in 0..WAVES {
		if wave > 0 {
			flusher.arm();
		}
		// Begun after the waves before it completed, so the transactions of this wave pin the
		// retired watermark no lower than where they ended.
		let gate = commit(&tree, &format!("gate_{wave}"), value_of(wave, 100));
		until("the flusher to hold a group", || flusher.is_held()).await;
		let items: Vec<(String, Vec<u8>)> = (0..WAVE)
			.map(|i| (format!("w{wave}_{i:04}"), value_of(wave * WAVE + i + 1, VALUE)))
			.collect();
		let handles: Vec<_> = items.iter().map(|(k, v)| commit(&tree, k, v.clone())).collect();
		until("the wave to be accepted", || pipeline.accepted_waiting() == 1 + WAVE).await;
		flusher.release();
		outcome(gate).await.unwrap();
		assert_all_acked(handles).await;
		until("every permit to come back", || pipeline.free_permits() == permits).await;
		expected_groups.extend([1, per_group, WAVE - per_group]);
	}
	assert_eq!(flusher.groups(), expected_groups);

	let (completed, taken, _) = pipeline.watermarks();
	assert_eq!(completed as usize, WAVES * (1 + WAVE));
	assert!(taken <= completed);
	assert!(
		taken > 0,
		"{completed} entries were drained in {} groups and nothing was retired",
		expected_groups.len()
	);

	pipeline.set_hook(None);
	mark_closed(&tree);
	release_lock(&tree);
}

// ---------------------------------------------------------------------------
// a crash at every group boundary
// ---------------------------------------------------------------------------

/// The flusher is held at the end of the WAL step of every group in turn. At each boundary the
/// directory is copied and the copy recovered: it holds a prefix of the commits in ring order, at
/// least through the last group that synced and at most through the group that is held, and the
/// live tree shows exactly the groups before the held one.
async fn crash_image_at_every_boundary(durabilities: &[Durability]) {
	const COMMITS: usize = 12;
	let dir = TempDir::new("wal_group_budget").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(options(&live, false)).unwrap());
	let flusher = hold_every_group(&tree);
	let pipeline = &tree.core.commit_pipeline;

	let first = ("first".to_string(), value_of(0, 100));
	let gate = commit(&tree, &first.0, first.1.clone());
	until("the flusher to hold its first group", || flusher.reached() == 1).await;

	let items: Vec<(String, Vec<u8>)> =
		(0..COMMITS).map(|i| (format!("big_{i:02}"), value_of(i + 1, MIB + i * 1000))).collect();
	let durability_of = |i: usize| durabilities[i % durabilities.len()];
	let mut handles = Vec::new();
	for (i, (key, value)) in items.iter().enumerate() {
		handles.push(commit_with(&tree, key, value.clone(), durability_of(i)));
		until("a commit to be accepted", || pipeline.accepted_waiting() == i + 2).await;
	}
	let hints: Vec<usize> = items.iter().map(|(k, v)| hint_of(k, v)).collect();
	let plan = group_plan(&hints);
	assert!(plan.len() >= 4, "control: several groups: {plan:?}");

	// Commit index (0 is `first`) of the last commit of each group, and whether it synced.
	let mut ends = vec![(1usize, true)];
	let mut at = 0;
	for n in &plan {
		let sync = (at..at + n).any(|i| durability_of(i) == Durability::Immediate);
		ends.push((ends.last().unwrap().0 + n, sync));
		at += n;
	}
	let all: Vec<(String, Vec<u8>)> = std::iter::once(first.clone()).chain(items.clone()).collect();
	let keys: Vec<String> = all.iter().map(|(k, _)| k.clone()).collect();

	for k in 1..=ends.len() {
		until("the flusher to hold the next group", || flusher.reached() == k).await;
		let (upper, _) = ends[k - 1];
		let lower = ends[..k].iter().rev().find(|(_, sync)| *sync).map_or(0, |(end, _)| *end);

		let image = dir.path().join(format!("image_{k}"));
		let recovered = recover_keys(&live, options(&image, false), &keys);
		let present: Vec<bool> = recovered.iter().map(Option::is_some).collect();
		let count = present.iter().take_while(|p| **p).count();
		assert!(
			present[count..].iter().all(|p| !*p),
			"boundary {k}: the image holds a hole: {present:?}"
		);
		assert!(
			(lower..=upper).contains(&count),
			"boundary {k}: image holds the first {count} commits, expected {lower}..={upper} \
			 (groups {plan:?})"
		);
		for ((key, value), got) in all.iter().zip(&recovered).take(count) {
			assert_eq!(got.as_deref(), Some(value.as_slice()), "boundary {k}: {key}");
		}

		// The live tree: only what the groups before this one made visible.
		let (before, _) = if k > 1 {
			ends[k - 2]
		} else {
			(0, false)
		};
		{
			let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
			for (i, (key, value)) in all.iter().enumerate() {
				let got = txn.get(key.as_bytes()).unwrap();
				if i < before {
					assert_eq!(
						got.as_deref(),
						Some(value.as_slice()),
						"boundary {k}: {key} visible"
					);
				} else {
					assert_eq!(got, None, "boundary {k}: {key} is not applied yet");
				}
			}
		}
		flusher.release();
	}
	outcome(gate).await.unwrap();
	assert_all_acked(handles).await;
	assert_eq!(flusher.groups().iter().skip(1).copied().collect::<Vec<_>>(), plan);
	flusher.assert_permits_held_until_complete("boundaries");
	assert_ring_settled(&tree, 1 + COMMITS as u64, "boundaries").await;

	pipeline.set_hook(None);
	mark_closed(&tree);
	release_lock(&tree);
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_crash_image_at_every_group_boundary_is_a_ring_order_prefix() {
	crash_image_at_every_boundary(&[Durability::Immediate]).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_crash_image_at_every_boundary_of_mixed_durability_groups_is_a_ring_order_prefix() {
	crash_image_at_every_boundary(&[
		Durability::Eventual,
		Durability::Eventual,
		Durability::Immediate,
		Durability::Eventual,
	])
	.await;
}

// ---------------------------------------------------------------------------
// close while large commits are in flight
// ---------------------------------------------------------------------------

/// `close` while large commits are in flight, with nothing holding the flusher: it finishes, and
/// every commit that returned Ok is in the reopened tree, whichever of the commits were accepted
/// before the shutdown.
async fn close_during_a_pile_up() {
	const COMMITS: usize = 12;
	let dir = TempDir::new("wal_group_budget").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(options(&live, false)).unwrap());
	let items: Vec<(String, Vec<u8>)> =
		(0..COMMITS).map(|i| (format!("big_{i:02}"), value_of(i, MIB + 5000 * i))).collect();
	let handles: Vec<_> = items.iter().map(|(k, v)| commit(&tree, k, v.clone())).collect();
	let closer = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move { tree.close().await })
	};
	tokio::time::timeout(WATCHDOG, closer).await.expect("close never finished").unwrap().unwrap();
	let mut acked = Vec::new();
	for ((key, value), handle) in items.iter().zip(handles) {
		if outcome(handle).await.is_ok() {
			acked.push((key.clone(), value.clone()));
		}
	}
	drop(tree);
	let reopened = Tree::new(options(&live, false)).unwrap();
	{
		let txn = reopened.begin_with_mode(Mode::ReadOnly).unwrap();
		for (key, value) in &acked {
			assert_eq!(
				txn.get(key.as_bytes()).unwrap().as_deref(),
				Some(value.as_slice()),
				"{key}"
			);
		}
	}
	mark_closed(&reopened);
	release_lock(&reopened);
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_during_a_pile_up_of_large_commits_flushes_every_acknowledged_one() {
	for _ in 0..2 {
		close_during_a_pile_up().await;
	}
}

#[test(tokio::test)]
async fn close_during_a_pile_up_of_large_commits_on_a_current_thread_runtime() {
	for _ in 0..2 {
		close_during_a_pile_up().await;
	}
}
