//! Pins the facts the load-independent commit-pipeline tests rely on, so that a change to the
//! pipeline that breaks one fails here, with a message that names it, and not as a stall in a test
//! that only uses it.
//!
//! * The entries of the group the flusher holds still count as accepted, so a wait for
//!   `accepted_waiting() >= n` sees them (`gather_groups`, the huge-group test).
//! * A commit made on its own is one flusher pass, and a retire follows every
//!   `RETIRE_EVERY_GROUPS`th pass since the last one (`ParkedRetire`).
//! * A burst whose groups each wait for everything admission has let in forms few groups, however
//!   slowly the committers run.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::lsm::Tree;
use crate::ring::{
	CommitStage,
	PipelineHook,
	ADMISSION_PERMITS,
	COMMIT_RING_CAPACITY,
	RETIRE_EVERY_GROUPS,
};
use crate::test::retire_free_tests::{park_until, OpenOnDrop};
use crate::test::retire_isolation_tests::{commit_unique, create_store, gather_groups, within};

const CAP: u64 = COMMIT_RING_CAPACITY as u64;

async fn until(secs: u64, what: &str, done: impl Fn() -> bool) {
	within(secs, what, async {
		while !done() {
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
	})
	.await;
}

fn spawn_commits(store: &Arc<Tree>, tag: &'static str, n: u64) -> Vec<tokio::task::JoinHandle<()>> {
	(0..n)
		.map(|i| {
			let store = Arc::clone(store);
			tokio::spawn(async move {
				let mut txn = store.begin().unwrap();
				txn.set(format!("{tag}{i}").as_bytes(), b"v").unwrap();
				txn.commit().await.unwrap();
			})
		})
		.collect()
}

/// The entries of a group the flusher holds after logging it (neither applied nor complete) are
/// still accepted and above the completed prefix, so `accepted_waiting` counts them with the
/// commits queued behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_entries_of_a_held_group_still_count_as_accepted() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	// 0 armed, 1 the flusher holds a group, 2 released
	let gate = Arc::new(AtomicUsize::new(0));
	let _open = OpenOnDrop(Arc::clone(&gate), 2);
	{
		let gate = Arc::clone(&gate);
		pipeline.set_hook(Some(Arc::new(move |point| {
			if matches!(point, PipelineHook::AfterWalSync { .. })
				&& gate.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_ok()
			{
				park_until(|| gate.load(Ordering::SeqCst) == 2);
			}
		})));
	}

	let n = 48;
	let tasks = spawn_commits(&store, "held", n);
	until(30, "the flusher to hold a group", || gate.load(Ordering::SeqCst) == 1).await;
	until(30, "every commit to be accepted", || pipeline.accepted_waiting() == n as usize).await;
	assert_eq!(pipeline.ring.completed(), 0, "nothing completes while the group is held");

	gate.store(2, Ordering::SeqCst);
	within(30, "the commits", async {
		for task in tasks {
			task.await.unwrap();
		}
	})
	.await;
	pipeline.set_hook(None);
	assert_eq!(pipeline.accepted_waiting(), 0);
}

/// Commits made one after the other are one flusher pass each, and a retire follows every
/// `RETIRE_EVERY_GROUPS`th of them: the completed prefix a retire reads is the ring sequence of
/// the commit that began it, and consecutive retires are exactly that many commits apart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retire_follows_every_retire_every_groups_th_lone_commit() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let retired_at = Arc::new(Mutex::new(Vec::new()));
	{
		let (retired_at, weak) = (Arc::clone(&retired_at), Arc::downgrade(pipeline));
		pipeline.set_retire_gap_hook(Some(Arc::new(move || {
			if let Some(pipeline) = weak.upgrade() {
				retired_at.lock().unwrap().push(pipeline.ring.completed());
			}
		})));
	}

	let every = u64::from(RETIRE_EVERY_GROUPS);
	let commits = 5 * every + 3;
	commit_unique(&store, "lone", commits).await;
	until(30, "the flusher to finish its last pass", || pipeline.ring.completed() == commits).await;
	pipeline.set_retire_gap_hook(None);

	assert_eq!(
		retired_at.lock().unwrap().clone(),
		(1..=5).map(|n| n * every).collect::<Vec<_>>(),
		"the sequences the retires followed, for {commits} lone commits"
	);
}

/// A burst of committers that are slow to be accepted, as on a loaded machine, still forms few
/// groups when the flusher waits for them at every group: two groups in a row take at least
/// `ADMISSION_PERMITS` commits, or all that is left.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn slow_committers_do_not_stretch_a_gathered_burst_into_more_groups() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let burst = CAP + CAP / 2;
	pipeline.set_commit_hook(Some(Arc::new(|stage| {
		if stage == CommitStage::Validated {
			std::thread::sleep(Duration::from_millis(2));
		}
	})));
	let groups = gather_groups(&store, burst);

	let tasks = spawn_commits(&store, "slow", burst);
	within(120, "the burst", async {
		for task in tasks {
			task.await.unwrap();
		}
	})
	.await;
	pipeline.set_hook(None);
	pipeline.set_commit_hook(None);

	let groups = groups.load(Ordering::SeqCst);
	let bound = 2 * burst.div_ceil(ADMISSION_PERMITS as u64) as usize;
	assert!(groups <= bound, "the burst took {groups} groups, more than the {bound} allowed");
}
