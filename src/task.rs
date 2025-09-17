use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_channel::Receiver;
use tokio::sync::Notify;

use crate::compaction::leveled::Strategy;
use crate::compaction::CompactionStrategy;
use crate::lsm::CompactionOperations;
use crate::sstable::InternalKeyTrait;
use crate::util::spawn;

/// Manages background tasks for the LSM tree
pub(crate) struct TaskManager {
	/// Flag to signal tasks to stop
	stop_flag: Arc<AtomicBool>,

	/// Notification for memtable compaction task
	memtable_notify: Arc<Notify>,

	/// Notification for level compaction task
	level_notify: Arc<Notify>,

	/// Flag indicating if memtable compaction is running
	memtable_running: Arc<AtomicBool>,

	/// Flag indicating if level compaction is running
	level_running: Arc<AtomicBool>,

	/// Channel to receive task completion signals
	task_completion_receiver: Receiver<()>,
}

impl fmt::Debug for TaskManager {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("TaskManager")
			.field("memtable_running", &self.memtable_running.load(Ordering::Acquire))
			.field("level_running", &self.level_running.load(Ordering::Acquire))
			.finish()
	}
}

impl TaskManager {
	pub(crate) fn new<K: InternalKeyTrait>(core: Arc<dyn CompactionOperations<K>>) -> Self {
		let stop_flag = Arc::new(AtomicBool::new(false));
		let memtable_notify = Arc::new(Notify::new());
		let level_notify = Arc::new(Notify::new());
		let memtable_running = Arc::new(AtomicBool::new(false));
		let level_running = Arc::new(AtomicBool::new(false));

		// Create channels for task completion signaling
		let (task_completion_sender, task_completion_receiver) = async_channel::bounded(2);

		// Spawn memtable compaction task
		{
			let core = core.clone();
			let stop_flag = stop_flag.clone();
			let notify = memtable_notify.clone();
			let running = memtable_running.clone();
			let level_notify = level_notify.clone();
			let completion_sender = task_completion_sender.clone();

			spawn(async move {
				loop {
					// Wait for notification
					notify.notified().await;

					if stop_flag.load(Ordering::SeqCst) {
						break;
					}

					running.store(true, Ordering::SeqCst);
					if let Err(e) = core.compact_memtable() {
						// TODO: Handle error appropriately
						eprintln!("\n Memtable compaction task error: {e:?}");
					} else {
						// If memtable compaction succeeded, trigger level compaction
						level_notify.notify_one();
					}
					running.store(false, Ordering::SeqCst);
				}
				// Signal task completion
				let _ = completion_sender.send(()).await;
			});
		}

		// Spawn level compaction task
		{
			let core = core.clone();
			let stop_flag = stop_flag.clone();
			let notify = level_notify.clone();
			let running = level_running.clone();
			let completion_sender = task_completion_sender.clone();

			spawn(async move {
				loop {
					// Wait for notification
					notify.notified().await;

					if stop_flag.load(Ordering::SeqCst) {
						break;
					}

					running.store(true, Ordering::SeqCst);
					// Use leveled compaction strategy
					let strategy: Arc<dyn CompactionStrategy<K>> = Arc::new(Strategy::default());
					if let Err(e) = core.compact(strategy) {
						// TODO: Handle error appropriately
						eprintln!("\n Level compaction task error: {e:?}");
					}
					running.store(false, Ordering::SeqCst);
				}
				// Signal task completion
				let _ = completion_sender.send(()).await;
			});
		}

		Self {
			stop_flag,
			memtable_notify,
			level_notify,
			memtable_running,
			level_running,
			task_completion_receiver,
		}
	}

	pub(crate) fn wake_up_memtable(&self) {
		// Only notify if not already running
		if !self.memtable_running.load(Ordering::Acquire) {
			self.memtable_notify.notify_one();
		}
	}

	#[cfg(test)]
	pub(crate) fn wake_up_level(&self) {
		// Only notify if not already running
		if !self.level_running.load(Ordering::Acquire) {
			self.level_notify.notify_one();
		}
	}

	pub async fn stop(&self) {
		// Set the stop flag to prevent new operations from starting
		self.stop_flag.store(true, Ordering::SeqCst);

		// Wake up any waiting tasks so they can check the stop flag and exit
		self.memtable_notify.notify_one();
		self.level_notify.notify_one();

		// Wait for any in-progress compactions to complete (no timeout - wait
		// indefinitely)
		while self.memtable_running.load(Ordering::Acquire)
			|| self.level_running.load(Ordering::Acquire)
		{
			// Yield to other tasks and wait a short time before checking again
			tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
		}

		// Wait for both tasks to signal completion
		for _ in 0..2 {
			if let Err(e) = self.task_completion_receiver.recv().await {
				eprintln!("Error receiving task completion signal on task manager: {e:?}");
				break;
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::error::Result;
	use crate::sstable::InternalKey;
	use crate::Error;
	use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
	use std::sync::Arc;
	use std::time::Duration;
	use tokio::time;

	use crate::compaction::CompactionStrategy;
	use crate::lsm::CompactionOperations;
	use crate::task::TaskManager;
	use crate::util::spawn;

	// Mock CoreInner for testing
	struct MockCoreInner {
		memtable_compactions: Arc<AtomicUsize>,
		level_compactions: Arc<AtomicUsize>,
		// Add delay configuration to test different scenarios
		memtable_delay_ms: u64,
		level_delay_ms: u64,
		fail_memtable: Arc<AtomicBool>,
		fail_level: Arc<AtomicBool>,
	}

	impl MockCoreInner {
		fn new() -> Self {
			Self {
				memtable_compactions: Arc::new(AtomicUsize::new(0)),
				level_compactions: Arc::new(AtomicUsize::new(0)),
				memtable_delay_ms: 20,
				level_delay_ms: 20,
				fail_memtable: Arc::new(AtomicBool::new(false)),
				fail_level: Arc::new(AtomicBool::new(false)),
			}
		}

		fn with_delays(memtable_delay_ms: u64, level_delay_ms: u64) -> Self {
			Self {
				memtable_compactions: Arc::new(AtomicUsize::new(0)),
				level_compactions: Arc::new(AtomicUsize::new(0)),
				memtable_delay_ms,
				level_delay_ms,
				fail_memtable: Arc::new(AtomicBool::new(false)),
				fail_level: Arc::new(AtomicBool::new(false)),
			}
		}
	}

	impl CompactionOperations<InternalKey> for MockCoreInner {
		fn compact_memtable(&self) -> Result<()> {
			let start = std::time::Instant::now();
			while start.elapsed() < Duration::from_millis(self.memtable_delay_ms) {
				std::hint::spin_loop();
			}

			// Check if should fail
			if self.fail_memtable.load(Ordering::SeqCst) {
				return Err(Error::Other("memtable error".into()));
			}

			// Only increment counter on success
			self.memtable_compactions.fetch_add(1, Ordering::SeqCst);
			Ok(())
		}

		fn compact(&self, _strategy: Arc<dyn CompactionStrategy<InternalKey>>) -> Result<()> {
			let start = std::time::Instant::now();
			while start.elapsed() < Duration::from_millis(self.level_delay_ms) {
				std::hint::spin_loop();
			}

			// Check if should fail
			if self.fail_level.load(Ordering::SeqCst) {
				return Err(Error::Other("level error".into()));
			}

			// Only increment counter on success
			self.level_compactions.fetch_add(1, Ordering::SeqCst);
			Ok(())
		}
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_wake_up_memtable() {
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(core.clone());

		task_manager.wake_up_memtable();
		time::sleep(Duration::from_millis(100)).await; // Allow time for task to complete

		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 1);
		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 1); // Level compaction should follow

		task_manager.stop().await;
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_multiple_wake_up_memtable() {
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(core.clone());

		for _ in 0..3 {
			task_manager.wake_up_memtable();
			time::sleep(Duration::from_millis(100)).await; // Allow time for task to
			                                      // complete
		}

		// Wait for all operations to complete
		time::sleep(Duration::from_millis(300)).await;

		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 3);
		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 3); // Each memtable compaction triggers a level compaction

		task_manager.stop().await;
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_wake_up_level() {
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(core.clone());

		task_manager.wake_up_level();
		time::sleep(Duration::from_millis(100)).await; // Allow time for task to complete

		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 1);
		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 0); // Memtable should not be affected

		task_manager.stop().await;
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_multiple_wake_up_level() {
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(core.clone());

		for _ in 0..3 {
			task_manager.wake_up_level();
			time::sleep(Duration::from_millis(100)).await; // Allow time for task to
			                                      // complete
		}

		// Wait for all operations to complete
		time::sleep(Duration::from_millis(300)).await;

		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 3);
		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 0); // Memtable should not be affected

		task_manager.stop().await;
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_alternating_compactions() {
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(core.clone());

		// Alternating between memtable and level compactions
		for i in 0..4 {
			if i % 2 == 0 {
				task_manager.wake_up_memtable();
			} else {
				task_manager.wake_up_level();
			}
			time::sleep(Duration::from_millis(100)).await;
		}

		// Wait for all operations to complete
		time::sleep(Duration::from_millis(300)).await;

		// 2 direct memtable compactions + 2 triggered by level (which is 2)
		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 2);
		// 2 direct level compactions + 2 triggered by memtable
		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 4);

		task_manager.stop().await;
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_already_running_tasks() {
		// Create core with longer delays to ensure tasks are still running when we try
		// to wake them again
		let core = Arc::new(MockCoreInner::with_delays(100, 100));
		let task_manager = TaskManager::new(core.clone());

		// Wake up memtable and immediately try again while it's still running
		task_manager.wake_up_memtable();
		task_manager.wake_up_memtable(); // This should be ignored as task is already running

		// Wait for first task to complete but not the second (if it were scheduled)
		time::sleep(Duration::from_millis(150)).await;

		// Only one compaction should have occurred
		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 1);

		// Do the same test with level compaction
		time::sleep(Duration::from_millis(150)).await; // Ensure previous level compaction is done

		task_manager.wake_up_level();
		task_manager.wake_up_level(); // This should be ignored

		time::sleep(Duration::from_millis(150)).await;

		// 1 from memtable + 1 directly triggered = 2
		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 2);

		task_manager.stop().await;
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_concurrent_wake_up_memtable() {
		let core = Arc::new(MockCoreInner::new());
		let task_manager = Arc::new(TaskManager::new(core.clone()));

		for _ in 0..10 {
			let tm = task_manager.clone();
			spawn(async move {
				tm.wake_up_memtable();
			});
			// Small sleep to ensure some interleaving
			time::sleep(Duration::from_millis(100)).await; // Allow time for all tasks to
			                                      // complete
		}

		task_manager.stop().await;

		// Allow time for all tasks to complete
		time::sleep(Duration::from_millis(1000)).await;

		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 10);
		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 10);

		Arc::try_unwrap(task_manager).expect("Task manager still has references").stop().await;
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_concurrent_wake_up_level() {
		let core = Arc::new(MockCoreInner::new());
		let task_manager = Arc::new(TaskManager::new(core.clone()));

		for _ in 0..5 {
			let tm = task_manager.clone();
			spawn(async move {
				tm.wake_up_level();
				// Small sleep to ensure some interleaving
				time::sleep(Duration::from_millis(5)).await;
			});
		}

		// Allow time for all tasks to complete
		time::sleep(Duration::from_millis(500)).await;

		// Stop the task manager to ensure all background tasks complete
		task_manager.stop().await;

		// Due to the running flag mechanism, we expect fewer compactions than wake-ups
		let level_count = core.level_compactions.load(Ordering::SeqCst);

		assert!(
			level_count > 0 && level_count <= 5,
			"Expected between 1-5 level compactions, got {level_count}"
		);
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_error_handling() {
		// This test verifies the correct error handling behavior

		// Create a mock with error simulation capability
		let core = Arc::new(MockCoreInner::new());

		// First, check how the mock behaves with a direct call to verify our
		// understanding
		core.fail_memtable.store(true, Ordering::SeqCst);
		let direct_result = core.compact_memtable();
		assert!(direct_result.is_err(), "Should return an error when fail_memtable is true");
		let memtable_count_after_direct = core.memtable_compactions.load(Ordering::SeqCst);
		println!("Memtable count after direct call: {memtable_count_after_direct}");

		// Reset counters
		core.memtable_compactions.store(0, Ordering::SeqCst);
		core.level_compactions.store(0, Ordering::SeqCst);

		// Now create the task manager and run the actual test
		let task_manager = TaskManager::new(core.clone());

		// Trigger memtable compaction that will fail
		task_manager.wake_up_memtable();
		time::sleep(Duration::from_millis(100)).await;

		// Read the actual counts to understand the real behavior
		let memtable_count = core.memtable_compactions.load(Ordering::SeqCst);
		let level_count = core.level_compactions.load(Ordering::SeqCst);

		println!(
            "After wake_up_memtable with failure: memtable_count={memtable_count}, level_count={level_count}"
        );

		assert!(
			level_count == 0,
			"Level compaction was triggered after memtable failure. Expected 0, got {level_count}"
		);

		// Reset counters
		core.memtable_compactions.store(0, Ordering::SeqCst);
		core.level_compactions.store(0, Ordering::SeqCst);

		// Set memtable to succeed but level to fail
		core.fail_memtable.store(false, Ordering::SeqCst);
		core.fail_level.store(true, Ordering::SeqCst);

		// Trigger memtable compaction (which should succeed)
		task_manager.wake_up_memtable();
		time::sleep(Duration::from_millis(100)).await;

		// Read counts again
		let memtable_count = core.memtable_compactions.load(Ordering::SeqCst);
		let level_count = core.level_compactions.load(Ordering::SeqCst);
		println!("After wake_up_memtable with success but level failure: memtable_count={memtable_count}, level_count={level_count}");

		// Reset counters
		core.memtable_compactions.store(0, Ordering::SeqCst);
		core.level_compactions.store(0, Ordering::SeqCst);

		// Fix level compaction and try direct level compaction
		core.fail_level.store(false, Ordering::SeqCst);
		task_manager.wake_up_level();
		time::sleep(Duration::from_millis(100)).await;

		// Read final counts
		let level_count = core.level_compactions.load(Ordering::SeqCst);
		println!("After wake_up_level with success: level_count={level_count}");

		task_manager.stop().await;
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_task_from_fail() {
		// Create a core that will fail on the first attempt but succeed on subsequent
		// attempts
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(core.clone());

		// Make first memtable compaction fail
		core.fail_memtable.store(true, Ordering::SeqCst);

		// Trigger a failing compaction
		task_manager.wake_up_memtable();
		time::sleep(Duration::from_millis(100)).await;

		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 0);

		// Make next memtable compaction succeed
		core.fail_memtable.store(false, Ordering::SeqCst);

		// Trigger another compaction
		task_manager.wake_up_memtable();
		time::sleep(Duration::from_millis(100)).await;

		// This should succeed
		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 1);

		// Task should still be responsive after error
		task_manager.stop().await;
	}
}
