//! Tests for freeing what a retire has passed: the overflow entries and ring slots of a long-lived
//! transaction's window go a chunk at a time, so that no commit waits behind a large free.
//!
//! What they pin down:
//!
//! * The bound: however many entries a transaction leaves behind, the flusher frees a bounded
//!   number between one group and the next, and does free some after every group.
//! * Completeness: every retired entry is freed, with no further commits, and then the flusher
//!   parks again: it does not spin, and nothing wakes it up on its own.
//! * Fairness: a drain does not hold up a commit that was accepted meanwhile, nor, on a runtime
//!   with few threads, the other tasks.
//! * Safety: nothing above the retired watermark is ever freed, so validation finds every conflict
//!   while the drain runs, whenever a transaction begins, and across close and restore.
//!
//! Every hook that parks the flusher gives its worker's queue away first and gives up after a
//! deadline, and the test opens the gate when it ends, so a failing assertion fails the test
//! instead of leaving the runtime waiting for the flusher.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tempdir::TempDir;

use crate::lsm::Tree;
use crate::ring::{
	FreeStats,
	PipelineHook,
	COMMIT_RING_CAPACITY,
	RETIRED_FREE_CHUNK,
	RETIRE_EVERY_ENTRIES,
	RETIRE_EVERY_GROUPS,
};
use crate::test::retire_isolation_tests::{commit_unique, create_store, overflow_drained, within};
use crate::Error;

const CAP: u64 = COMMIT_RING_CAPACITY as u64;

// The tests lap the ring and need several chunks to a lap of it.
const _: () = assert!(4 * RETIRED_FREE_CHUNK <= COMMIT_RING_CAPACITY);

fn completed(store: &Tree) -> u64 {
	store.core.commit_pipeline.ring.completed()
}

fn taken(store: &Tree) -> u64 {
	store.core.commit_pipeline.ring.taken()
}

fn overflow_len(store: &Tree) -> usize {
	store.core.commit_pipeline.watermarks().2
}

/// Blocks the flusher, from inside a hook, until `go` holds, or for a minute at the most. The
/// worker hands its queue over first: a task the flusher woke just before it parked sits in the
/// worker's own slot, which no other worker takes from, and would not run until the flusher is
/// released.
fn park_until(go: impl Fn() -> bool) {
	let deadline = Instant::now() + Duration::from_secs(60);
	tokio::task::block_in_place(|| {
		while !go() && Instant::now() < deadline {
			std::thread::sleep(Duration::from_millis(1));
		}
	});
}

/// Sets a gate to a value when dropped, so that a test which fails while the flusher is parked
/// on the gate lets it go on the way out.
struct OpenOnDrop(Arc<AtomicUsize>, usize);

impl Drop for OpenOnDrop {
	fn drop(&mut self) {
		self.0.store(self.1, Ordering::SeqCst);
	}
}

/// Waits until `done` holds.
async fn until(secs: u64, what: &str, done: impl Fn() -> bool) {
	within(secs, what, async {
		while !done() {
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
	})
	.await;
}

/// Counts every call of `free_retired`.
fn count_free_calls(store: &Tree) -> Arc<AtomicUsize> {
	let calls = Arc::new(AtomicUsize::new(0));
	let hook_calls = Arc::clone(&calls);
	store.core.commit_pipeline.set_free_retired_hook(Some(Arc::new(move || {
		hook_calls.fetch_add(1, Ordering::SeqCst);
	})));
	calls
}

/// Waits until the flusher has stopped calling `free_retired`: it has freed what it could and
/// parked. Three samples in a row must agree, so that a flusher the machine has put to sleep for
/// a moment is not taken for a parked one.
async fn flusher_is_quiet(calls: &AtomicUsize) {
	within(20, "the flusher to stop calling free_retired", async {
		let (mut seen, mut agreed) = (calls.load(Ordering::SeqCst), 0);
		while agreed < 3 {
			tokio::time::sleep(Duration::from_millis(100)).await;
			let now = calls.load(Ordering::SeqCst);
			agreed = if now == seen {
				agreed + 1
			} else {
				0
			};
			seen = now;
		}
	})
	.await;
}

/// Commits `n` single-key transactions from 16 tasks at once, so that they go through the flusher
/// in groups rather than one by one.
async fn commit_many(store: &Arc<Tree>, tag: &str, n: u64) {
	let tasks: Vec<_> = (0..16)
		.map(|t| {
			let store = Arc::clone(store);
			let tag = format!("{tag}{t}_");
			tokio::spawn(async move { commit_unique(&store, &tag, n / 16).await })
		})
		.collect();
	for task in tasks {
		task.await.unwrap();
	}
}

/// How many ring slots hold an entry that the retired watermark has passed, which the flusher
/// has not released yet.
fn retired_slots_held(store: &Tree) -> usize {
	let ring = &store.core.commit_pipeline.ring;
	let unretired = ring.published().saturating_sub(ring.taken()).min(ring.capacity());
	ring.occupied().saturating_sub(unretired as usize)
}

/// Waits until the completed prefix has caught up with the last commit: a committer is woken
/// before the flusher advances the prefix over its entry.
async fn completed_prefix_caught_up(store: &Tree) {
	let pipeline = &store.core.commit_pipeline;
	until(30, "the completed prefix to reach the last commit", || {
		pipeline.watermarks().0 == pipeline.ring.published()
	})
	.await;
}

/// Parks the flusher in a retire, so that the commit that started it is the last one anyone
/// makes: nothing follows it but the free of what it passed.
struct ParkedRetire {
	/// 0 armed, 1 the flusher is parked in a retire, 2 released
	gate: Arc<AtomicUsize>,
	_open: OpenOnDrop,
}

impl ParkedRetire {
	fn install(store: &Tree) -> Self {
		let gate = Arc::new(AtomicUsize::new(2));
		let hook_gate = Arc::clone(&gate);
		store.core.commit_pipeline.set_retire_gap_hook(Some(Arc::new(move || {
			if hook_gate.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
				park_until(|| hook_gate.load(Ordering::SeqCst) == 2);
			}
		})));
		Self {
			_open: OpenOnDrop(Arc::clone(&gate), 2),
			gate,
		}
	}

	/// Commits until a retire is under way, and not one more.
	async fn commit_until_parked(&self, store: &Tree) {
		self.gate.store(0, Ordering::SeqCst);
		let mut commits = 0;
		while self.gate.load(Ordering::SeqCst) != 1 {
			commit_unique(store, "n", 1).await;
			commits += 1;
			assert!(commits <= 4 * RETIRE_EVERY_GROUPS, "no retire after {commits} commits");
			let deadline = Instant::now() + Duration::from_millis(50);
			while self.gate.load(Ordering::SeqCst) != 1 && Instant::now() < deadline {
				tokio::time::sleep(Duration::from_millis(1)).await;
			}
		}
	}

	fn release(&self) {
		self.gate.store(2, Ordering::SeqCst);
	}
}

// ---------------------------------------------------------------------------------------------
// The bound
// ---------------------------------------------------------------------------------------------

/// What the flusher freed between a retire and the next group, out of what it held.
struct FreedBeforeTheNextGroup {
	/// Overflow entries gone, of `overflow` held.
	overflow_freed: usize,
	overflow: usize,
	/// Ring slots emptied, of `slots` held.
	slots_freed: usize,
	slots: usize,
}

/// Parks the flusher inside the first retire after a long-lived transaction ends, with the next
/// commit queued behind it, and returns how much it freed by the time that commit's group
/// began. The commit is accepted before the flusher resumes, so nothing but the retire (and
/// whatever it leaves for later) runs in between.
async fn freed_before_the_next_group(store: &Arc<Tree>) -> FreedBeforeTheNextGroup {
	let pipeline = &store.core.commit_pipeline;
	// 0 armed, 1 the flusher is parked in the retire, 2 released
	let gate = Arc::new(AtomicUsize::new(2));
	let _open = OpenOnDrop(Arc::clone(&gate), 2);
	{
		let gate = Arc::clone(&gate);
		pipeline.set_retire_gap_hook(Some(Arc::new(move || {
			if gate.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
				park_until(|| gate.load(Ordering::SeqCst) == 2);
			}
		})));
	}

	let held = store.begin().unwrap();
	commit_many(store, "fill", 12 * CAP).await;
	let (_, _, retained) = pipeline.watermarks();
	assert!(retained as u64 > 10 * CAP, "setup: only {retained} entries are kept");
	// The flusher is done with the retire that follows the last commit
	completed_prefix_caught_up(store).await;
	tokio::time::sleep(Duration::from_millis(100)).await;

	gate.store(0, Ordering::SeqCst);
	drop(held);
	// Commits make the flusher retire. The one that follows the retire waits for it.
	let driver = {
		let store = Arc::clone(store);
		tokio::spawn(async move {
			commit_unique(&store, "drive", 2 * u64::from(RETIRE_EVERY_GROUPS)).await
		})
	};
	until(30, "a retire to park the flusher", || gate.load(Ordering::SeqCst) == 1).await;
	let next = completed(store) + 1;
	until(10, "the next commit to be accepted", || pipeline.is_accepted(next)).await;
	// What the flusher holds now, with the commit that waits behind the retire published
	let (_, _, parked) = pipeline.watermarks();
	let parked_slots = pipeline.ring.occupied();
	assert_eq!(pipeline.free_stats().total, 0, "setup: an earlier retire already freed entries");

	let at_next_group = Arc::new((AtomicUsize::new(usize::MAX), AtomicUsize::new(usize::MAX)));
	{
		let (store, at_next_group) = (Arc::clone(store), Arc::clone(&at_next_group));
		pipeline.set_hook(Some(Arc::new(move |point| {
			if matches!(point, PipelineHook::BeforeApplyRound { .. }) {
				let pipeline = &store.core.commit_pipeline;
				let (entries, slots) = (pipeline.watermarks().2, pipeline.ring.occupied());
				let _ = at_next_group.0.compare_exchange(
					usize::MAX,
					entries,
					Ordering::SeqCst,
					Ordering::SeqCst,
				);
				let _ = at_next_group.1.compare_exchange(
					usize::MAX,
					slots,
					Ordering::SeqCst,
					Ordering::SeqCst,
				);
			}
		})));
	}

	gate.store(2, Ordering::SeqCst);
	within(30, "the commits behind the parked flusher", driver).await.unwrap();
	pipeline.set_hook(None);
	pipeline.set_retire_gap_hook(None);
	FreedBeforeTheNextGroup {
		overflow_freed: parked.saturating_sub(at_next_group.0.load(Ordering::SeqCst)),
		overflow: parked,
		slots_freed: parked_slots.saturating_sub(at_next_group.1.load(Ordering::SeqCst)),
		slots: parked_slots,
	}
}

/// However many entries a long-lived transaction left in the overflow map and the ring, the
/// flusher frees a bounded number between one group and the next, and does free one chunk: a
/// flusher that frees nothing while commits keep it busy satisfies an upper bound alone, and
/// would hold the entries for as long as the load lasts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_retire_after_a_long_transaction_frees_a_bounded_number_of_entries() {
	let (store, _dir) = create_store();
	let freed = freed_before_the_next_group(&store).await;
	let (overflow, slots) = (freed.overflow, freed.slots);
	assert!(overflow > 8 * RETIRED_FREE_CHUNK, "setup: {overflow} entries are not many chunks");
	assert!(slots > 2 * RETIRED_FREE_CHUNK, "setup: {slots} slots are not many chunks");
	assert_eq!(
		freed.overflow_freed, RETIRED_FREE_CHUNK,
		"{} of {overflow} overflow entries were freed between two groups, not one chunk",
		freed.overflow_freed
	);
	// The commit waiting behind the retire overwrote the oldest slot, which the chunk would have
	// released, so the chunk may empty one slot fewer
	assert!(
		freed.slots_freed > 0 && freed.slots_freed <= RETIRED_FREE_CHUNK,
		"{} of {slots} ring slots were emptied between two groups, not up to one chunk",
		freed.slots_freed
	);
	overflow_drained(&store).await;
}

// ---------------------------------------------------------------------------------------------
// Completeness
// ---------------------------------------------------------------------------------------------

/// The entries a long-lived transaction left behind are freed a chunk at a time, and all of them
/// are freed after it ends, though the commits that follow stop long before the entries run out:
/// the flusher keeps freeing while it has nothing else to do.
#[tokio::test]
async fn retired_entries_are_freed_a_chunk_at_a_time_until_none_remain() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let held = store.begin().unwrap();
	commit_many(&store, "fill", 10 * CAP).await;
	let (_, _, retained) = pipeline.watermarks();
	assert!(retained > 32 * RETIRED_FREE_CHUNK, "setup: only {retained} entries are kept");
	assert_eq!(pipeline.free_stats(), FreeStats::default(), "nothing is retired while it is open");

	drop(held);
	commit_unique(&store, "resume", 2 * u64::from(RETIRE_EVERY_GROUPS)).await;
	assert!(retained > 2 * RETIRE_EVERY_GROUPS as usize * RETIRED_FREE_CHUNK, "setup");
	overflow_drained(&store).await;

	let stats = pipeline.free_stats();
	assert!(stats.most <= RETIRED_FREE_CHUNK, "one step freed {} entries", stats.most);
	// What was kept when the transaction ended, and the few the commits after it lapped
	assert!(stats.total >= retained, "{} freed of {retained}", stats.total);
	assert!(stats.total <= retained + 2 * RETIRE_EVERY_GROUPS as usize, "{}", stats.total);
	assert!(
		stats.steps >= retained.div_ceil(RETIRED_FREE_CHUNK),
		"{} entries freed in {} steps",
		stats.total,
		stats.steps
	);
	assert_eq!(pipeline.retired_in_overflow(), 0);
	// The ring is emptied of retired entries too, apart from the commits made since the last retire
	until(30, "the retired ring slots to be released", || {
		pipeline.ring.occupied() <= 2 * RETIRE_EVERY_GROUPS as usize + 2
	})
	.await;
}

/// A retire that passes more ring slots than one chunk, with almost nothing in the overflow map
/// and no commit after it, leaves the slots to the flusher while it has nothing to flush: the
/// idle ring holds only what has not been retired. The flusher is parked in the retire so that
/// the last commit is the one that triggers it, and no group follows to release a chunk.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_ring_slots_a_retire_passed_are_released_when_no_commit_follows() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let retire = ParkedRetire::install(&store);

	let held = store.begin().unwrap();
	// The ring is full of entries the transaction holds, and few of them were lapped
	commit_many(&store, "fill", CAP + CAP / 8).await;
	completed_prefix_caught_up(&store).await;
	drop(held);
	retire.commit_until_parked(&store).await;
	let lapped = overflow_len(&store);
	let held_slots = pipeline.ring.occupied();
	assert!(lapped < 2 * RETIRED_FREE_CHUNK, "setup: {lapped} lapped entries are many chunks");
	assert!(held_slots > 3 * RETIRED_FREE_CHUNK, "setup: {held_slots} slots are not many chunks");
	retire.release();

	until(30, "the retired ring slots to be released", || pipeline.ring.occupied() <= 2).await;
	overflow_drained(&store).await;
	pipeline.set_retire_gap_hook(None);
}

/// Once everything retired is freed the flusher parks: nothing wakes it up on its own, so the
/// calls to `free_retired` stop. Checked after retires that cannot pass an open transaction and
/// so free nothing, after a drain that the last commit started and that the flusher finishes by
/// itself, and after a burst of concurrent commits that retire a lot at once. A flusher that does
/// not notice it has finished never parks again, and keeps calling `free_retired`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_flusher_parks_once_nothing_is_left_to_free() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let calls = count_free_calls(&store);
	let retire = ParkedRetire::install(&store);

	let held = store.begin().unwrap();
	commit_many(&store, "fill", 6 * CAP).await;
	let retained = overflow_len(&store);
	assert!(retained > 16 * RETIRED_FREE_CHUNK, "setup: only {retained} entries are kept");
	flusher_is_quiet(&calls).await;
	let quiet = calls.load(Ordering::SeqCst);
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert_eq!(calls.load(Ordering::SeqCst), quiet, "the flusher woke up on its own");

	drop(held);
	retire.commit_until_parked(&store).await;
	let retained = overflow_len(&store);
	assert!(retained > 8 * RETIRED_FREE_CHUNK, "setup: {retained} entries are not many chunks");
	retire.release();
	overflow_drained(&store).await;
	assert!(calls.load(Ordering::SeqCst) > quiet, "setup: nothing was freed");
	flusher_is_quiet(&calls).await;
	until(30, "the retired ring slots to be released", || pipeline.ring.occupied() <= 2).await;
	let quiet = calls.load(Ordering::SeqCst);
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert_eq!(calls.load(Ordering::SeqCst), quiet, "the flusher woke up on its own");
	pipeline.set_retire_gap_hook(None);

	let tasks: Vec<_> = (0..64)
		.map(|t| {
			let store = Arc::clone(&store);
			tokio::spawn(async move { commit_unique(&store, &format!("burst{t}_"), 40).await })
		})
		.collect();
	for task in tasks {
		task.await.unwrap();
	}
	flusher_is_quiet(&calls).await;
	assert_eq!(overflow_len(&store), 0);
	pipeline.set_free_retired_hook(None);
}

/// A group of more entries than a chunk is followed by the release of more ring slots than a
/// chunk: a retire passes as many slots as the groups before it drained, and a flusher that
/// releases a chunk a group falls behind them. Three groups of 400 entries (a ring of 1024 slots)
/// are made by parking the flusher inside the group of a first commit while the rest queue up and
/// are decided, so that they are gathered together, and the last of them is the one that passes
/// `RETIRE_EVERY_ENTRIES`. Nothing is committed after it, so a ring slot only ever empties
/// because a step released it, and the most slots one step released is read off the slots held at
/// the start of each step and once the last step is over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_larger_than_a_chunk_is_followed_by_a_release_of_more_slots_than_a_chunk() {
	const WAVE: u64 = 400;
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	// 1 the flusher waits before every group, 0 it does not
	let closed = Arc::new(AtomicUsize::new(0));
	let _open = OpenOnDrop(Arc::clone(&closed), 0);
	// How many groups the flusher has waited in front of
	let waited = Arc::new(AtomicUsize::new(0));
	{
		let (closed, waited) = (Arc::clone(&closed), Arc::clone(&waited));
		pipeline.set_hook(Some(Arc::new(move |point| {
			if matches!(point, PipelineHook::BeforeApplyRound { .. })
				&& closed.load(Ordering::SeqCst) == 1
			{
				waited.fetch_add(1, Ordering::SeqCst);
				park_until(|| closed.load(Ordering::SeqCst) == 0);
			}
		})));
	}
	let samples = Arc::new(Mutex::new(Vec::new()));
	{
		let (store, samples) = (Arc::clone(&store), Arc::clone(&samples));
		pipeline.set_free_retired_hook(Some(Arc::new(move || {
			samples.lock().unwrap().push(store.core.commit_pipeline.ring.occupied());
		})));
	}

	// A wave is `WAVE + 1` entries in two groups, so the third is the first to pass
	// `RETIRE_EVERY_ENTRIES`, with far fewer groups than `RETIRE_EVERY_GROUPS`
	const _: () =
		assert!(2 * (WAVE + 1) < RETIRE_EVERY_ENTRIES && 3 * (WAVE + 1) >= RETIRE_EVERY_ENTRIES);
	for wave in 0..3 {
		closed.store(1, Ordering::SeqCst);
		let (first, waited_before) = (pipeline.ring.published() + 1, waited.load(Ordering::SeqCst));
		let prime = {
			let store = Arc::clone(&store);
			tokio::spawn(async move { commit_unique(&store, &format!("prime{wave}_"), 1).await })
		};
		until(30, "the flusher to wait in front of the first group", || {
			waited.load(Ordering::SeqCst) > waited_before
		})
		.await;
		let tasks: Vec<_> = (0..WAVE)
			.map(|t| {
				let store = Arc::clone(&store);
				tokio::spawn(
					async move { commit_unique(&store, &format!("wave{wave}_{t}_"), 1).await },
				)
			})
			.collect();
		until(30, "the wave to be decided", || {
			(first + 1..=first + WAVE).all(|seq| pipeline.is_accepted(seq))
		})
		.await;
		closed.store(0, Ordering::SeqCst);
		prime.await.unwrap();
		for task in tasks {
			task.await.unwrap();
		}
	}
	// Slots hold nothing retired before the retire too, so the watermark must have moved
	until(30, "the retired ring slots to be released", || {
		taken(&store) > 0 && retired_slots_held(&store) == 0
	})
	.await;
	pipeline.set_hook(None);
	pipeline.set_free_retired_hook(None);

	let mut samples = samples.lock().unwrap();
	// What the last step left, which no later step samples
	samples.push(pipeline.ring.occupied());
	let most = samples.windows(2).map(|pair| pair[0].saturating_sub(pair[1])).max().unwrap_or(0);
	assert!(
		most > RETIRED_FREE_CHUNK,
		"no step released more than a chunk of slots ({most} at the most, over {} steps)",
		samples.len()
	);
}

// ---------------------------------------------------------------------------------------------
// Fairness
// ---------------------------------------------------------------------------------------------

/// Parks the flusher inside a step of a drain: `install` before the drain starts, `park` once
/// it is running, `release` to let it go. Until it is parked every step takes a few
/// milliseconds, so that the drain outlasts the steps of the test.
struct ParkedDrain {
	/// 0 slow, 1 armed, 2 parked, 3 released
	gate: Arc<AtomicUsize>,
	calls: Arc<AtomicUsize>,
}

impl ParkedDrain {
	fn install(store: &Tree) -> Self {
		let gate = Arc::new(AtomicUsize::new(0));
		let calls = Arc::new(AtomicUsize::new(0));
		let (hook_gate, hook_calls) = (Arc::clone(&gate), Arc::clone(&calls));
		store.core.commit_pipeline.set_free_retired_hook(Some(Arc::new(move || {
			hook_calls.fetch_add(1, Ordering::SeqCst);
			if hook_gate.compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
				park_until(|| hook_gate.load(Ordering::SeqCst) == 3);
			} else if hook_gate.load(Ordering::SeqCst) == 0 {
				std::thread::sleep(Duration::from_millis(3));
			}
		})));
		Self {
			gate,
			calls,
		}
	}

	async fn park(&self) {
		self.gate.store(1, Ordering::SeqCst);
		until(30, "the flusher to park inside a step of the drain", || {
			self.gate.load(Ordering::SeqCst) == 2
		})
		.await;
	}

	fn release(&self) {
		self.gate.store(3, Ordering::SeqCst);
	}
}

impl Drop for ParkedDrain {
	fn drop(&mut self) {
		self.release();
	}
}

/// Sets a long-lived transaction up so that many chunks are left to free, ends it, and makes the
/// commits that let a retire run (one more than `RETIRE_EVERY_GROUPS`, so that one does). Returns
/// once the last of them has returned, with the flusher freeing in the background.
async fn start_a_drain(store: &Arc<Tree>, laps: u64) {
	let held = store.begin().unwrap();
	commit_many(store, "fill", laps * CAP).await;
	let retained = overflow_len(store);
	assert!(retained > 16 * RETIRED_FREE_CHUNK, "setup: only {retained} entries are kept");
	drop(held);
	commit_unique(store, "resume", u64::from(RETIRE_EVERY_GROUPS) + 1).await;
}

/// The idle drain frees one step and then looks for work again before the next step. A commit
/// that was accepted while a step was running is flushed before another step starts, so a
/// committer never waits for the whole drain. The flusher is parked inside a step of the idle
/// drain, a commit is accepted behind it, and no step may have begun by the time that commit's
/// group does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_accepted_during_the_idle_drain_is_flushed_before_the_next_step() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let drain = ParkedDrain::install(&store);

	start_a_drain(&store, 12).await;
	drain.park().await;
	let parked_at = drain.calls.load(Ordering::SeqCst);
	assert!(overflow_len(&store) > 0, "setup: the drain is over");

	let next_group_at = Arc::new(AtomicUsize::new(usize::MAX));
	{
		let (calls, next_group_at) = (Arc::clone(&drain.calls), Arc::clone(&next_group_at));
		pipeline.set_hook(Some(Arc::new(move |point| {
			if matches!(point, PipelineHook::BeforeApplyRound { .. }) {
				let _ = next_group_at.compare_exchange(
					usize::MAX,
					calls.load(Ordering::SeqCst),
					Ordering::SeqCst,
					Ordering::SeqCst,
				);
			}
		})));
	}
	let committer = {
		let store = Arc::clone(&store);
		tokio::spawn(async move { commit_unique(&store, "during", 1).await })
	};
	let next = completed(&store) + 1;
	until(10, "the commit to be accepted", || pipeline.is_accepted(next)).await;

	drain.release();
	within(30, "the commit behind the parked flusher", committer).await.unwrap();
	pipeline.set_hook(None);
	assert_eq!(
		next_group_at.load(Ordering::SeqCst),
		parked_at,
		"another step of the drain began before the accepted commit's group"
	);
	until(60, "the drain to finish", || overflow_len(&store) == 0).await;
}

/// The flusher frees retired entries with nothing to flush, so it must give the runtime up between
/// chunks, or on a runtime with few threads the committers and every other task wait for it for as
/// long as the entries are many. A task that is always ready counts how often it ran, and the
/// longest run of calls to `free_retired` with none of its turns in between is read off.
#[tokio::test]
async fn the_flusher_gives_way_to_other_tasks_between_chunks_when_it_has_nothing_to_flush() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let held = store.begin().unwrap();
	commit_many(&store, "fill", 6 * CAP).await;
	let (_, _, retained) = pipeline.watermarks();
	assert!(retained > 16 * RETIRED_FREE_CHUNK, "setup: only {retained} entries are kept");

	let turns = Arc::new(AtomicUsize::new(0));
	let probe = {
		let turns = Arc::clone(&turns);
		tokio::spawn(async move {
			loop {
				turns.fetch_add(1, Ordering::SeqCst);
				tokio::task::yield_now().await;
			}
		})
	};
	let (last, run, longest, calls) = (
		Arc::new(AtomicUsize::new(usize::MAX)),
		Arc::new(AtomicUsize::new(0)),
		Arc::new(AtomicUsize::new(0)),
		Arc::new(AtomicUsize::new(0)),
	);
	{
		let (turns, last, run, longest, calls) = (
			Arc::clone(&turns),
			Arc::clone(&last),
			Arc::clone(&run),
			Arc::clone(&longest),
			Arc::clone(&calls),
		);
		pipeline.set_free_retired_hook(Some(Arc::new(move || {
			calls.fetch_add(1, Ordering::SeqCst);
			let now = turns.load(Ordering::SeqCst);
			if last.swap(now, Ordering::SeqCst) == now {
				let length = run.fetch_add(1, Ordering::SeqCst) + 1;
				longest.fetch_max(length, Ordering::SeqCst);
			} else {
				run.store(0, Ordering::SeqCst);
			}
		})));
	}
	drop(held);
	commit_unique(&store, "resume", 2 * u64::from(RETIRE_EVERY_GROUPS)).await;
	overflow_drained(&store).await;
	pipeline.set_free_retired_hook(None);
	probe.abort();

	let (calls, longest) = (calls.load(Ordering::SeqCst), longest.load(Ordering::SeqCst));
	assert!(calls > retained / RETIRED_FREE_CHUNK, "setup: {calls} calls freed {retained} entries");
	// A few calls can follow each other (the one after a group, and the one that finds nothing to
	// flush), but not the dozens that freeing the entries of a long transaction takes
	assert!(
		longest <= 8,
		"{longest} calls to free_retired in a row without another task getting a turn"
	);
}

// ---------------------------------------------------------------------------------------------
// Safety
// ---------------------------------------------------------------------------------------------

/// The flusher is parked part-way through freeing the entries a long-lived transaction left (two
/// chunks are gone, the rest are still in the map). Transactions that began after it, whose
/// windows reach lapped entries above the watermark, validate without the flusher: the one that
/// conflicts finds its conflict, and the one that does not is not failed for a missing entry.
/// The retired entries go lowest first, so this is the state of a drain that has not reached the
/// entries above the watermark yet; the next test is the one that holds once it has freed
/// everything below them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn validators_find_every_conflict_while_retired_entries_are_being_freed() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;

	let old = store.begin().unwrap();
	commit_many(&store, "before", 3 * CAP).await;
	let mut conflicting = store.begin().unwrap();
	assert!(conflicting.get_for_update(b"contended").unwrap().is_none());
	let mut disjoint = store.begin().unwrap();
	assert!(disjoint.get_for_update(b"untouched").unwrap().is_none());
	let window = conflicting.commit_window_for_test().min(disjoint.commit_window_for_test());
	let mut other = store.begin().unwrap();
	other.set(b"contended", b"other").unwrap();
	other.commit().await.unwrap();
	commit_many(&store, "after", 3 * CAP).await;
	completed_prefix_caught_up(&store).await;
	let (completed, _, retained) = pipeline.watermarks();
	assert!(completed - window >= 3 * CAP, "the windows span laps: {window}..{completed}");
	assert!(retained as u64 > 5 * CAP, "setup: only {retained} entries are kept");

	// 0 armed, 1 the flusher is parked before its third chunk, 2 released
	let gate = Arc::new(AtomicUsize::new(0));
	let _open = OpenOnDrop(Arc::clone(&gate), 2);
	{
		let (store, gate) = (Arc::clone(&store), Arc::clone(&gate));
		pipeline.set_free_retired_hook(Some(Arc::new(move || {
			if store.core.commit_pipeline.free_stats().steps == 2
				&& gate.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok()
			{
				park_until(|| gate.load(Ordering::SeqCst) == 2);
			}
		})));
	}
	drop(old);
	// Commits make the flusher retire, and then free a chunk after each group. The last ones
	// wait for it to be released.
	let driver = {
		let store = Arc::clone(&store);
		tokio::spawn(async move {
			commit_unique(&store, "drive", 2 * u64::from(RETIRE_EVERY_GROUPS)).await
		})
	};
	until(30, "the flusher to park part-way through freeing", || gate.load(Ordering::SeqCst) == 1)
		.await;

	assert!(taken(&store) <= window, "retired watermark passed the live window {window}");
	assert_eq!(pipeline.free_stats().total, 2 * RETIRED_FREE_CHUNK);
	assert!(pipeline.retired_in_overflow() > 0, "setup: nothing is left to free");
	pipeline.assert_lapped_entries_are_kept();

	// Neither needs the flusher, which is parked: a commit with no writes only validates
	within(10, "the disjoint commit", disjoint.commit())
		.await
		.expect("a lapped entry in the window was freed: a missing entry reads as a conflict");
	let verdict = within(10, "the conflicting commit", conflicting.commit()).await;
	assert!(matches!(verdict, Err(Error::TransactionWriteConflict)), "{verdict:?}");

	gate.store(2, Ordering::SeqCst);
	within(30, "the commits behind the parked flusher", driver).await.unwrap();
	pipeline.set_free_retired_hook(None);
	until(30, "the retired entries to be freed", || pipeline.retired_in_overflow() == 0).await;
	pipeline.assert_lapped_entries_are_kept();
}

/// Everything the watermark has passed is freed and the flusher has parked, and the transactions
/// that hold the watermark are still open. The entries above it, which their windows reach, must
/// all still be there: the transaction that read a key written in its window conflicts, and the
/// one that read a key nobody wrote must not be failed for want of an entry. The retired entries
/// go first, so a free that overshoots the watermark only reaches these once the retired ones are
/// gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_entries_above_the_watermark_survive_the_free_of_everything_below_it() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let calls = count_free_calls(&store);

	let old = store.begin().unwrap();
	commit_many(&store, "before", 3 * CAP).await;
	let mut conflicting = store.begin().unwrap();
	assert!(conflicting.get_for_update(b"contended").unwrap().is_none());
	let mut disjoint = store.begin().unwrap();
	assert!(disjoint.get_for_update(b"untouched").unwrap().is_none());
	let window = conflicting.commit_window_for_test().min(disjoint.commit_window_for_test());
	let mut other = store.begin().unwrap();
	other.set(b"contended", b"other").unwrap();
	other.commit().await.unwrap();
	commit_many(&store, "after", 3 * CAP).await;
	drop(old);
	// A retire passes what `old` held, up to the windows of the two transactions still open
	commit_unique(&store, "drive", 2 * u64::from(RETIRE_EVERY_GROUPS)).await;
	assert!(calls.load(Ordering::SeqCst) > 0, "setup: nothing was freed");
	until(30, "the retired entries to be freed", || pipeline.retired_in_overflow() == 0).await;
	flusher_is_quiet(&calls).await;
	pipeline.set_free_retired_hook(None);

	let (completed, taken, kept) = pipeline.watermarks();
	assert!(taken <= window, "retired watermark {taken} passed the live window {window}");
	assert!(taken + CAP > window, "setup: the watermark {taken} should have reached {window}");
	assert_eq!(pipeline.retired_in_overflow(), 0);
	let lapped_above = completed.saturating_sub(CAP).saturating_sub(taken);
	assert!(
		kept as u64 >= lapped_above,
		"{kept} entries are left, though {lapped_above} lapped ones above the watermark are \
		 within the windows"
	);
	within(30, "the disjoint commit", disjoint.commit())
		.await
		.expect("a lapped entry within the window was freed: a missing entry reads as a conflict");
	let verdict = within(30, "the conflicting commit", conflicting.commit()).await;
	assert!(matches!(verdict, Err(Error::TransactionWriteConflict)), "{verdict:?}");
}

/// A transaction that begins inside a step of the drain (from the flusher's own thread, right
/// after a retire passed a lot) has a window at or above the watermark, so the rest of the drain
/// must not take what its window reaches: it still finds its conflict after more laps, and a
/// disjoint transaction begun at the same moment is not failed for a lost entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transactions_begun_inside_a_step_keep_the_entries_their_window_reaches() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let begun = Arc::new(Mutex::new(Vec::new()));
	let draining = Arc::new(AtomicBool::new(false));
	{
		let (store, begun, draining) =
			(Arc::clone(&store), Arc::clone(&begun), Arc::clone(&draining));
		let calls = Arc::new(AtomicUsize::new(0));
		pipeline.set_free_retired_hook(Some(Arc::new(move || {
			if !draining.load(Ordering::SeqCst) || calls.fetch_add(1, Ordering::SeqCst) % 4 != 1 {
				return;
			}
			let mut begun = begun.lock().unwrap();
			let n = begun.len();
			let mut conflicting = store.begin().unwrap();
			assert!(conflicting.get_for_update(b"contended").unwrap().is_none());
			conflicting.set(format!("conflicting-out{n}").as_bytes(), b"v").unwrap();
			let mut disjoint = store.begin().unwrap();
			assert!(disjoint.get_for_update(format!("untouched{n}").as_bytes()).unwrap().is_none());
			disjoint.set(format!("disjoint-out{n}").as_bytes(), b"v").unwrap();
			begun.push((conflicting, disjoint));
		})));
	}

	let held = store.begin().unwrap();
	commit_many(&store, "fill", 6 * CAP).await;
	draining.store(true, Ordering::SeqCst);
	drop(held);
	commit_unique(&store, "resume", u64::from(RETIRE_EVERY_GROUPS) + 1).await;
	until(30, "the retired entries to be freed", || {
		pipeline.free_stats().total > 0 && pipeline.retired_in_overflow() == 0
	})
	.await;
	draining.store(false, Ordering::SeqCst);
	// Everyone that began inside a step is open while the ring laps again
	let mut other = store.begin().unwrap();
	other.set(b"contended", b"other").unwrap();
	other.commit().await.unwrap();
	commit_many(&store, "lap", 4 * CAP).await;
	pipeline.set_free_retired_hook(None);

	let begun = std::mem::take(&mut *begun.lock().unwrap());
	assert!(begun.len() >= 2, "setup: only {} transactions began inside a step", begun.len());
	for (i, (mut conflicting, mut disjoint)) in begun.into_iter().enumerate() {
		assert!(
			matches!(conflicting.commit().await, Err(Error::TransactionWriteConflict)),
			"transaction {i} began inside a step and missed its conflict"
		);
		disjoint.commit().await.unwrap_or_else(|e| {
			panic!("transaction {i} began inside a step and lost an entry: {e:?}")
		});
	}
	commit_unique(&store, "drain", 2 * u64::from(RETIRE_EVERY_GROUPS)).await;
	overflow_drained(&store).await;
}

/// Committers lap the ring while transactions are held open across laps and end, so entries are
/// retired and freed all the time. A monitor checks over and over that every entry the ring lapped
/// above the watermark is still in the overflow map, and the held transactions, which write only
/// keys of their own, must never be told they conflict (a lost entry reads as a conflict).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn nothing_above_the_watermark_is_freed_while_transactions_come_and_go() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let stop = Arc::new(AtomicBool::new(false));

	let committers: Vec<_> = (0..16)
		.map(|t| {
			let store = Arc::clone(&store);
			tokio::spawn(async move { commit_unique(&store, &format!("c{t}_"), 640).await })
		})
		.collect();
	let holders: Vec<_> = (0..2)
		.map(|t| {
			let (store, stop) = (Arc::clone(&store), Arc::clone(&stop));
			tokio::spawn(async move {
				for round in 0..4 {
					let mut txn = store.begin().unwrap();
					let window = txn.commit_window_for_test();
					let from = completed(&store);
					while completed(&store) < from + 2 * CAP && !stop.load(Ordering::SeqCst) {
						let t = taken(&store);
						assert!(t <= window, "retired watermark {t} passed a live window {window}");
						tokio::time::sleep(Duration::from_micros(500)).await;
					}
					txn.get_for_update(format!("h{t}_{round}_lock").as_bytes()).unwrap();
					txn.set(format!("h{t}_{round}").as_bytes(), b"v").unwrap();
					txn.commit().await.expect("a transaction with its own keys failed");
				}
			})
		})
		.collect();
	let monitor = {
		let (store, stop) = (Arc::clone(&store), Arc::clone(&stop));
		tokio::spawn(async move {
			loop {
				store.core.commit_pipeline.assert_lapped_entries_are_kept();
				if stop.load(Ordering::SeqCst) {
					break;
				}
				tokio::time::sleep(Duration::from_millis(2)).await;
			}
		})
	};
	within(180, "the committers", async {
		for c in committers {
			c.await.unwrap();
		}
	})
	.await;
	stop.store(true, Ordering::SeqCst);
	within(60, "the held transactions", async {
		for h in holders {
			h.await.unwrap();
		}
	})
	.await;
	monitor.await.unwrap();

	commit_unique(&store, "drain", 2 * u64::from(RETIRE_EVERY_GROUPS)).await;
	overflow_drained(&store).await;
	pipeline.assert_lapped_entries_are_kept();
}

/// A clean close while the flusher is parked part-way through a drain returns, and the flusher
/// frees nothing once it has been closed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_during_the_drain_returns_and_the_flusher_stops_freeing() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let drain = ParkedDrain::install(&store);

	start_a_drain(&store, 8).await;
	drain.park().await;
	assert!(overflow_len(&store) > 0, "setup: the drain is over");

	let closing = {
		let store = Arc::clone(&store);
		tokio::spawn(async move { store.close().await })
	};
	tokio::time::sleep(Duration::from_millis(50)).await;
	drain.release();
	within(30, "close during a drain", closing).await.unwrap().unwrap();
	let steps = pipeline.free_stats().steps;
	tokio::time::sleep(Duration::from_millis(200)).await;
	assert_eq!(pipeline.free_stats().steps, steps, "the flusher kept freeing after close");
}

/// A restore replaces the overflow map while the flusher is part-way through freeing it. The
/// drain ends (nothing is left), the pipeline keeps validating, and the flusher parks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_part_way_through_the_drain_leaves_a_working_pipeline() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	commit_unique(&store, "base", 10).await;
	store.flush().unwrap();
	let checkpoints = TempDir::new("retire_checkpoint").unwrap();
	let checkpoint = checkpoints.path().join("checkpoint");
	store.create_checkpoint(&checkpoint).unwrap();
	let drain = ParkedDrain::install(&store);

	start_a_drain(&store, 8).await;
	drain.park().await;
	assert!(overflow_len(&store) > 0, "setup: the drain is over");
	let restoring = {
		let store = Arc::clone(&store);
		let checkpoint = checkpoint.clone();
		tokio::task::spawn_blocking(move || store.restore_from_checkpoint(&checkpoint))
	};
	tokio::time::sleep(Duration::from_millis(50)).await;
	drain.release();
	within(60, "the restore", restoring).await.unwrap().unwrap();
	pipeline.set_free_retired_hook(None);

	until(30, "the overflow map to be empty", || overflow_len(&store) == 0).await;
	let mut conflicting = store.begin().unwrap();
	conflicting.set(b"contended", b"mine").unwrap();
	let mut other = store.begin().unwrap();
	other.set(b"contended", b"other").unwrap();
	other.commit().await.unwrap();
	commit_many(&store, "lap", 3 * CAP).await;
	assert!(matches!(conflicting.commit().await, Err(Error::TransactionWriteConflict)));
	commit_unique(&store, "drain", 2 * u64::from(RETIRE_EVERY_GROUPS)).await;
	overflow_drained(&store).await;
}
