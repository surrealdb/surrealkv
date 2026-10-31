//! `Tree::flush` between commits must not leave a later commit parked.
//!
//! Issue #487: a commit waited forever when this test-only flush raced the
//! background flusher. A small memtable and a memtable stall threshold of 2
//! are the conditions the report names.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tempdir::TempDir;
use test_log::test;
use tokio::time::timeout;

use crate::batch::Batch;
use crate::{CompactRangeOptions, Durability, Options, Tree};

fn options(path: &std::path::Path) -> Options {
	Options {
		path: path.to_path_buf(),
		max_memtable_size: 8 * 1024,
		memtable_stall_threshold: 2,
		level0_max_files: 4,
		l0_stall_threshold: 12,
		flush_on_close: false,
		..Default::default()
	}
}

async fn commit_value(tree: &Tree, key: &[u8], value: &[u8]) -> crate::Result<()> {
	let mut txn = tree.begin()?;
	txn.set_durability(Durability::Immediate);
	txn.set(key, value)?;
	txn.commit().await
}

fn note_stall_state(tree: &Tree) -> String {
	let queued = tree.core.inner.immutable_count();
	let l0 = tree.core.inner.l0_file_count();
	let tasks = tree
		.core
		.task_manager
		.lock()
		.ok()
		.and_then(|guard| guard.as_ref().map(|tm| format!("{tm:?}")))
		.unwrap_or_else(|| "task manager gone".to_string());
	format!("immutables={queued} l0={l0} {tasks}")
}

/// Puts one key into the active memtable. The value is a location, which is what a
/// flush of the memtable reads back.
fn put_active(tree: &Tree, key: &[u8]) {
	let mut batch = Batch::new(1);
	let value = crate::vlog::ValueLocation::with_inline_value(b"value".to_vec()).encode();
	batch.set(key.to_vec(), value, 0).unwrap();
	let active = tree.core.inner.active_memtable.read().unwrap();
	active.add(&batch).unwrap();
}

/// Rotates the active memtable the way `Tree::flush` does, without waking the flush task.
fn queue_memtable(tree: &Tree, key: &[u8]) {
	put_active(tree, key);
	tree.core.inner.rotate_memtable().unwrap();
}

/// The flush task is the only thing left that removes these memtables. It has to drain
/// the queue after a stall, not stop once the first signal lets the writer continue.
/// The budget covers a slow debug fsync. A task that was never woken stays past it.
async fn assert_queue_drains(tree: &Tree) {
	let drained = timeout(Duration::from_secs(15), async {
		loop {
			if tree.core.inner.immutable_count() == 0 {
				break;
			}
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
	})
	.await;
	assert!(
		drained.is_ok(),
		"memtable flush task did not drain the queue ({})",
		note_stall_state(tree)
	);
}

/// The background flush of a stalled queue records that the writer was in
/// `MemtableLimit` and that this task, not a sync flush, was the one running.
fn watch_memtable_stall(tree: &Arc<Tree>) -> Arc<AtomicBool> {
	let saw = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&saw);
	let hook_tree = Arc::clone(tree);
	tree.core.inner.flush_hook.lock().replace(Arc::new(move |_| {
		if flag.swap(true, Ordering::SeqCst) {
			return Ok(());
		}
		let state = note_stall_state(&hook_tree);
		assert!(
			hook_tree.core.write_stall.is_stalled(),
			"the flush task ran with no writer stalled ({state})"
		);
		assert!(
			hook_tree.core.inner.immutable_count() >= 2,
			"the flush task ran below the memtable stall ({state})"
		);
		assert!(
			hook_tree.core.inner.l0_file_count() < hook_tree.core.inner.opts.l0_stall_threshold,
			"the stall was level 0, not the memtables ({state})"
		);
		assert!(
			state.contains("memtable_running: true"),
			"the memtable task was not running during its flush ({state})"
		);
		Ok(())
	}));
	saw
}

/// Two memtables queued with no wake, which is what a sync flush leaves behind when
/// its snapshot stops short of a rotation. The next commit must not wait forever.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_commit_does_not_wait_forever_on_memtables_queued_without_a_wake() {
	let dir = TempDir::new("flush_race_stall").unwrap();
	let tree = Arc::new(Tree::new(Arc::new(options(dir.path()))).unwrap());

	queue_memtable(&tree, b"one");
	queue_memtable(&tree, b"two");
	assert_eq!(
		tree.core.inner.immutable_count(),
		2,
		"the queued memtables were flushed before the commit"
	);
	assert!(
		note_stall_state(&tree).contains("memtable_running: false"),
		"the flush task was already running ({})",
		note_stall_state(&tree)
	);
	let saw_stall = watch_memtable_stall(&tree);

	let commit = commit_value(&tree, b"three", b"value");
	match timeout(Duration::from_secs(10), commit).await {
		Ok(Ok(())) => {}
		Ok(Err(e)) => panic!("commit failed: {e}"),
		Err(_) => panic!("commit parked with no wakeup ({})", note_stall_state(&tree)),
	}
	assert!(
		saw_stall.load(Ordering::SeqCst),
		"a MemtableLimit stall never woke the flush task ({})",
		note_stall_state(&tree)
	);
	assert!(
		tree.core.inner.immutable_count() < 2,
		"the commit returned with the memtable stall still in force ({})",
		note_stall_state(&tree)
	);
	assert_queue_drains(&tree).await;
	tree.close().await.unwrap();
}

/// Queues two memtables from inside the sync flush, after that flush has chosen
/// how far it will go. Returns whether the hook ran.
fn arm_rotations_after_snapshot(tree: &Arc<Tree>) -> Arc<AtomicBool> {
	let armed = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&armed);
	let hook_tree = Arc::clone(tree);
	tree.core.inner.flush_hook.lock().replace(Arc::new(move |_| {
		if flag.swap(true, Ordering::SeqCst) {
			return Ok(());
		}
		queue_memtable(&hook_tree, b"later-1");
		queue_memtable(&hook_tree, b"later-2");
		Ok(())
	}));
	armed
}

/// The memtables a sync flush left behind have to fall below the stall threshold
/// without a later commit. A commit would wake the task itself, and would hide a
/// call that returned without doing so.
async fn assert_falls_below_stall_without_a_commit(tree: &Tree) {
	let fell = timeout(Duration::from_secs(10), async {
		loop {
			if tree.core.inner.immutable_count() < 2 {
				break;
			}
			tokio::time::sleep(Duration::from_millis(10)).await;
		}
	})
	.await;
	assert!(
		fell.is_ok(),
		"immutables stayed at the stall threshold with no commit to rearm ({})",
		note_stall_state(tree)
	);
}

/// `flush_all_immutables_sync` only flushes memtables already queued when it starts.
/// A rotation that lands after that snapshot is left for the background task.
/// `Tree::flush` wakes that task when any remain. Two such memtables are the stall
/// threshold, so the following commit must not park.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn tree_flush_leaves_later_memtables_for_a_task_it_does_not_wake() {
	let dir = TempDir::new("flush_race_snapshot").unwrap();
	let tree = Arc::new(Tree::new(Arc::new(options(dir.path()))).unwrap());
	let armed = arm_rotations_after_snapshot(&tree);

	put_active(&tree, b"original");
	tree.flush().unwrap();
	assert!(armed.load(Ordering::SeqCst), "the flush did not rotate after its snapshot");

	match timeout(Duration::from_secs(10), commit_value(&tree, b"next", b"value")).await {
		Ok(Ok(())) => {}
		Ok(Err(e)) => panic!("commit failed: {e}"),
		Err(_) => panic!("commit parked with no wakeup ({})", note_stall_state(&tree)),
	}
	// `flush` wakes the task, so the two memtables may already be gone before this
	// commit. What remains has to drain: the stall path and the wake share that.
	assert_queue_drains(&tree).await;
	tree.close().await.unwrap();
}

/// `compact_range(flush_memtable)` uses the same snapshot as `Tree::flush`. Memtables
/// rotated after it have to fall below the stall threshold without a later commit.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn compact_range_wakes_memtables_rotated_after_its_flush() {
	let dir = TempDir::new("flush_race_compact").unwrap();
	let tree = Arc::new(Tree::new(Arc::new(options(dir.path()))).unwrap());
	let armed = arm_rotations_after_snapshot(&tree);
	put_active(&tree, b"original");

	// Target level 0 runs no compaction after the flush, so nothing else signals or wakes.
	let options = CompactRangeOptions {
		flush_memtable: true,
		target_level: Some(0),
		rewrite_bottommost: false,
	};
	tree.compact_range(None, None, &options).unwrap();
	assert!(armed.load(Ordering::SeqCst), "compact_range did not rotate after its snapshot");
	assert_falls_below_stall_without_a_commit(&tree).await;
	tree.close().await.unwrap();
}

/// Checkpoint creation flushes through the same snapshot. Memtables rotated after it
/// have to fall below the stall threshold without a later commit.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn checkpoint_wakes_memtables_rotated_after_its_flush() {
	let dir = TempDir::new("flush_race_checkpoint").unwrap();
	let checkpoint = TempDir::new("flush_race_checkpoint_dest").unwrap();
	let tree = Arc::new(Tree::new(Arc::new(options(dir.path()))).unwrap());
	let armed = arm_rotations_after_snapshot(&tree);
	put_active(&tree, b"original");

	tree.create_checkpoint(checkpoint.path()).unwrap();
	assert!(armed.load(Ordering::SeqCst), "the checkpoint did not rotate after its snapshot");
	assert_falls_below_stall_without_a_commit(&tree).await;
	tree.close().await.unwrap();
}

/// One immutable is below the stall threshold. A commit must finish, and nothing
/// periodically flushes that table. Two immutables are the threshold: the stalled
/// commit wakes the flush, so the single table does not sit for good.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn one_immutable_does_not_park_a_commit() {
	let dir = TempDir::new("flush_race_one").unwrap();
	let tree = Arc::new(Tree::new(Arc::new(options(dir.path()))).unwrap());

	queue_memtable(&tree, b"only");
	assert_eq!(tree.core.inner.immutable_count(), 1);
	assert!(
		note_stall_state(&tree).contains("memtable_running: false"),
		"the flush task was already running ({})",
		note_stall_state(&tree)
	);

	match timeout(Duration::from_secs(3), commit_value(&tree, b"next", b"value")).await {
		Ok(Ok(())) => {}
		Ok(Err(e)) => panic!("commit failed below the stall threshold: {e}"),
		Err(_) => panic!("a commit parked below the stall threshold ({})", note_stall_state(&tree)),
	}
	tokio::time::sleep(Duration::from_millis(200)).await;
	assert_eq!(
		tree.core.inner.immutable_count(),
		1,
		"one immutable was flushed with no wake ({})",
		note_stall_state(&tree)
	);

	queue_memtable(&tree, b"second");
	assert_eq!(tree.core.inner.immutable_count(), 2);
	assert!(
		note_stall_state(&tree).contains("memtable_running: false"),
		"queueing the second memtable woke the flush task ({})",
		note_stall_state(&tree)
	);
	let saw_stall = watch_memtable_stall(&tree);
	match timeout(Duration::from_secs(10), commit_value(&tree, b"after", b"value")).await {
		Ok(Ok(())) => {}
		Ok(Err(e)) => panic!("commit failed at the stall threshold: {e}"),
		Err(_) => panic!("commit parked once two memtables were queued ({})", note_stall_state(&tree)),
	}
	assert!(
		saw_stall.load(Ordering::SeqCst),
		"reaching the stall threshold did not wake the flush task ({})",
		note_stall_state(&tree)
	);
	assert_queue_drains(&tree).await;
	tree.close().await.unwrap();
}

/// Several writers commit, and every other commit is followed by `Tree::flush`.
/// A commit that is still waiting after a few seconds is the hang in #487.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn flush_between_commits_does_not_leave_a_commit_waiting() {
	let dir = TempDir::new("flush_race").unwrap();
	let tree = Arc::new(Tree::new(Arc::new(options(dir.path()))).unwrap());
	let hangs = Arc::new(AtomicUsize::new(0));
	let commits = Arc::new(AtomicUsize::new(0));
	let value = vec![b'v'; 1500];

	let mut writers = Vec::new();
	for id in 0..4 {
		let tree = Arc::clone(&tree);
		let hangs = Arc::clone(&hangs);
		let commits = Arc::clone(&commits);
		let value = value.clone();
		writers.push(tokio::spawn(async move {
			for n in 0..40 {
				let key = format!("{id}-{n}");
				match timeout(Duration::from_secs(3), commit_value(&tree, key.as_bytes(), &value))
					.await
				{
					Ok(Ok(())) => {
						commits.fetch_add(1, Ordering::Relaxed);
					}
					Ok(Err(e)) => panic!("commit failed: {e}"),
					Err(_) => {
						eprintln!("commit parked: {}", note_stall_state(&tree));
						hangs.fetch_add(1, Ordering::Relaxed);
						return;
					}
				}
				if n % 2 == 0 {
					// Same thread as the commits, which is how the harness calls it.
					// A flush that itself never returns is a hang too.
					let flush = {
						let tree = Arc::clone(&tree);
						tokio::task::spawn_blocking(move || tree.flush())
					};
					match timeout(Duration::from_secs(3), flush).await {
						Ok(Ok(Ok(()))) => {}
						Ok(Ok(Err(e))) => panic!("flush failed: {e}"),
						Ok(Err(e)) => panic!("flush task join failed: {e}"),
						Err(_) => {
							eprintln!("flush parked: {}", note_stall_state(&tree));
							hangs.fetch_add(1, Ordering::Relaxed);
							return;
						}
					}
				}
			}
		}));
	}

	for writer in writers {
		match timeout(Duration::from_secs(8), writer).await {
			Ok(Ok(())) => {}
			Ok(Err(e)) => panic!("writer panicked: {e}"),
			Err(_) => {
				eprintln!("writer still parked: {}", note_stall_state(&tree));
				hangs.fetch_add(1, Ordering::Relaxed);
			}
		}
	}

	let hung = hangs.load(Ordering::Relaxed);
	let done = commits.load(Ordering::Relaxed);
	assert!(
		hung == 0,
		"a commit waited with no wakeup ({hung} hung, {done} commits finished, {})",
		note_stall_state(&tree)
	);
	assert!(done > 0, "no commit finished");
	// A hung writer may still hold the tree. Closing waits for the flusher, so
	// only close when every writer returned.
	if hung == 0 {
		tree.close().await.unwrap();
	}
}
