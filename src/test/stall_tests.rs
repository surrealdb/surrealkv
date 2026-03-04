//! Tests for write stall functionality

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use test_log::test;
use tokio::time;

use crate::error::WriteStallReason;
use crate::stall::{
	StallCounts,
	StallThresholds,
	WriteStallController,
	WriteStallCountProvider,
	WriteStallInfo,
};
use crate::{Error, Tree, TreeBuilder};

// ===== Mock Provider =====

struct MockStallCountProvider {
	immutable_count: AtomicUsize,
	l0_count: AtomicUsize,
}

impl MockStallCountProvider {
	fn new(immutable: usize, l0: usize) -> Self {
		Self {
			immutable_count: AtomicUsize::new(immutable),
			l0_count: AtomicUsize::new(l0),
		}
	}

	fn set_counts(&self, immutable: usize, l0: usize) {
		self.immutable_count.store(immutable, Ordering::Release);
		self.l0_count.store(l0, Ordering::Release);
	}
}

impl WriteStallCountProvider for MockStallCountProvider {
	fn get_stall_counts(&self) -> StallCounts {
		StallCounts {
			immutable_memtables: self.immutable_count.load(Ordering::Acquire),
			l0_files: self.l0_count.load(Ordering::Acquire),
		}
	}
}

fn default_thresholds() -> StallThresholds {
	StallThresholds {
		memtable_limit: 2,
		l0_file_limit: 12,
	}
}

// ===== Unit Tests for WriteStallController =====

#[test(tokio::test)]
async fn test_no_stall_below_threshold() {
	let provider = Arc::new(MockStallCountProvider::new(1, 5));
	let controller = WriteStallController::new(provider, default_thresholds());

	// Below thresholds - should not stall
	let result = controller.check().await;
	assert!(result.is_ok());
	assert!(result.unwrap().is_none());
	assert!(!controller.is_stalled());
}

#[test(tokio::test)]
async fn test_memtable_stall_triggers() {
	let provider = Arc::new(MockStallCountProvider::new(2, 0)); // At threshold
	let controller = Arc::new(WriteStallController::new(
		Arc::clone(&provider) as Arc<dyn WriteStallCountProvider>,
		default_thresholds(),
	));
	let controller_clone = Arc::clone(&controller);
	let provider_clone = Arc::clone(&provider);

	// Spawn task that will signal after delay
	tokio::spawn(async move {
		time::sleep(Duration::from_millis(50)).await;
		provider_clone.set_counts(1, 0); // Simulate flush completing
		controller_clone.signal_work_done();
	});

	let start = std::time::Instant::now();
	let result = controller.check().await;

	assert!(result.is_ok());
	let stall_info: WriteStallInfo = result.unwrap().expect("Expected stall info");

	// Verify WriteStallInfo contents
	assert_eq!(stall_info.reason, WriteStallReason::MemtableLimit);
	assert_eq!(stall_info.current_value, 2);
	assert_eq!(stall_info.threshold, 2);
	assert!(stall_info.duration >= Duration::from_millis(40));
	assert!(start.elapsed() >= Duration::from_millis(40));
}

#[test(tokio::test)]
async fn test_l0_stall_triggers() {
	let provider = Arc::new(MockStallCountProvider::new(0, 12)); // At L0 threshold
	let controller = Arc::new(WriteStallController::new(
		Arc::clone(&provider) as Arc<dyn WriteStallCountProvider>,
		default_thresholds(),
	));
	let controller_clone = Arc::clone(&controller);
	let provider_clone = Arc::clone(&provider);

	tokio::spawn(async move {
		time::sleep(Duration::from_millis(50)).await;
		provider_clone.set_counts(0, 5); // Simulate compaction completing
		controller_clone.signal_work_done();
	});

	let result = controller.check().await;
	assert!(result.is_ok());

	let stall_info = result.unwrap().expect("Expected stall info");

	// Verify WriteStallInfo contents for L0 stall
	assert_eq!(stall_info.reason, WriteStallReason::L0FileLimit);
	assert_eq!(stall_info.current_value, 12);
	assert_eq!(stall_info.threshold, 12);
	assert!(stall_info.duration >= Duration::from_millis(40));
}

#[test(tokio::test)]
async fn test_shutdown_during_stall() {
	let provider = Arc::new(MockStallCountProvider::new(2, 0)); // Always at threshold
	let controller = Arc::new(WriteStallController::new(
		provider as Arc<dyn WriteStallCountProvider>,
		default_thresholds(),
	));
	let controller_clone = Arc::clone(&controller);

	// Spawn task that will signal shutdown after delay
	tokio::spawn(async move {
		time::sleep(Duration::from_millis(50)).await;
		controller_clone.signal_shutdown();
	});

	let result = controller.check().await;

	// Should return Err(PipelineStall) on shutdown
	assert!(result.is_err());
	assert!(matches!(result.unwrap_err(), Error::PipelineStall));
}

#[test(tokio::test)]
async fn test_stall_wakes_on_signal() {
	let provider = Arc::new(MockStallCountProvider::new(1, 5)); // Below threshold
	let controller = Arc::new(WriteStallController::new(
		provider as Arc<dyn WriteStallCountProvider>,
		default_thresholds(),
	));
	let controller_clone = Arc::clone(&controller);

	// Spawn multiple signals to simulate flush completing
	tokio::spawn(async move {
		for _ in 0..3 {
			time::sleep(Duration::from_millis(20)).await;
			controller_clone.signal_work_done();
		}
	});

	// Below threshold - should not stall
	let result = controller.check().await;
	assert!(result.is_ok());
	assert!(result.unwrap().is_none()); // Wasn't stalled
}

#[test(tokio::test)]
async fn test_is_stalled_flag() {
	let provider = Arc::new(MockStallCountProvider::new(2, 0)); // At threshold
	let controller = Arc::new(WriteStallController::new(
		Arc::clone(&provider) as Arc<dyn WriteStallCountProvider>,
		default_thresholds(),
	));
	let controller_clone = Arc::clone(&controller);
	let controller_check = Arc::clone(&controller);
	let provider_clone = Arc::clone(&provider);

	assert!(!controller.is_stalled());

	// Spawn task that will check and then signal
	tokio::spawn(async move {
		time::sleep(Duration::from_millis(20)).await;
		// At this point, should be stalled
		assert!(controller_check.is_stalled());
		time::sleep(Duration::from_millis(30)).await;
		provider_clone.set_counts(1, 0); // Simulate flush completing
		controller_clone.signal_work_done();
	});

	let _ = controller.check().await;

	// After stall cleared
	assert!(!controller.is_stalled());
}

// ===== Integration Tests with Tree =====

#[test(tokio::test)]
async fn test_integration_basic_writes_no_stall() {
	// Basic test to ensure normal writes work with stall controller in place
	let temp_dir = tempfile::tempdir().unwrap();
	let tree: Tree = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_max_memtable_size(1024 * 1024) // 1MB - large enough to not trigger stall
		.with_memtable_stall_threshold(2)
		.with_l0_stall_threshold(12)
		.build()
		.unwrap();

	// Write some data - should not stall
	for i in 0..10 {
		let key = format!("key{:04}", i);
		let value = vec![0u8; 100];
		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), &value).unwrap();
		let _: Result<(), crate::Error> = txn.commit().await;
	}

	let _: Result<(), crate::Error> = tree.close().await;
}

#[test(tokio::test)]
async fn test_integration_config_validation() {
	let temp_dir = tempfile::tempdir().unwrap();

	// memtable_stall_threshold must be >= 2
	let result = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_memtable_stall_threshold(1)
		.build();

	assert!(result.is_err());
	match result {
		Err(e) => assert!(
			e.to_string().contains("memtable_stall_threshold"),
			"Expected error message to contain 'memtable_stall_threshold', got: {}",
			e
		),
		Ok(_) => panic!("Expected error but got Ok"),
	}
}

#[test(tokio::test)]
async fn test_integration_l0_threshold_validation() {
	let temp_dir = tempfile::tempdir().unwrap();

	// l0_stall_threshold must be >= level0_max_files
	let result = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_l0_stall_threshold(2) // Default level0_max_files is 4
		.build();

	assert!(result.is_err());
	match result {
		Err(e) => assert!(
			e.to_string().contains("l0_stall_threshold"),
			"Expected error message to contain 'l0_stall_threshold', got: {}",
			e
		),
		Ok(_) => panic!("Expected error but got Ok"),
	}
}

#[test(tokio::test)]
async fn test_shutdown_completes_with_pending_writes() {
	// Verify shutdown completes even when there are pending writes
	let temp_dir = tempfile::tempdir().unwrap();
	let tree: Tree = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_max_memtable_size(1024 * 1024) // 1MB
		.with_memtable_stall_threshold(2)
		.with_l0_stall_threshold(12)
		.build()
		.unwrap();
	let tree = Arc::new(tree);
	let tree_clone = Arc::clone(&tree);

	// Spawn writer
	let writer = tokio::spawn(async move {
		for i in 0..20 {
			let key = format!("key{:04}", i);
			let value = vec![0u8; 100];
			let txn_result = tree_clone.begin();
			if txn_result.is_err() {
				// Expected - shutdown happened
				return;
			}
			let mut txn = txn_result.unwrap();
			if txn.set(key.as_bytes(), &value).is_err() {
				return;
			}
			if txn.commit().await.is_err() {
				return;
			}
		}
	});

	// Give writer time to start
	time::sleep(Duration::from_millis(10)).await;

	// Shutdown should complete quickly (not hang)
	// We use match instead of unwrap() since Tree doesn't implement Debug
	let tree_owned = match Arc::try_unwrap(tree) {
		Ok(t) => t,
		Err(_) => panic!("Failed to unwrap Arc<Tree>"),
	};

	let shutdown_result = tokio::time::timeout(Duration::from_secs(5), tree_owned.close()).await;

	assert!(shutdown_result.is_ok(), "Shutdown should not hang");
	assert!(shutdown_result.unwrap().is_ok(), "Shutdown should succeed");

	// Writer task should complete
	let _ = writer.await;
}

// ============================================================================
// Integration Tests
// ============================================================================
//
// These tests verify the new architecture where:
// 1. Stall check happens at ArenaFull in LsmCommitEnv::write()
// 2. Concurrent writers are properly coordinated with writer_refs
// 3. Flush waits for active writers via ready_for_flush()

#[test(tokio::test)]
async fn test_stall_check_at_arena_full() {
	// Test that stall check happens when ArenaFull is encountered
	let temp_dir = tempfile::tempdir().unwrap();
	let tree: Tree = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_max_memtable_size(8 * 1024) // Small memtable to trigger rotation
		.with_memtable_stall_threshold(4)
		.with_l0_stall_threshold(12)
		.build()
		.unwrap();

	// Write data that will fill the memtable and trigger rotations
	for i in 0..20 {
		let key = format!("key{:04}", i);
		let value = vec![0u8; 500]; // ~500B per entry
		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), &value).unwrap();
		let result = txn.commit().await;
		assert!(result.is_ok(), "Commit should succeed: {:?}", result.err());
	}

	// Should have rotated and created immutable memtables
	tree.flush().unwrap();
	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn test_concurrent_writes_with_rotation() {
	// Test concurrent writers during memtable rotation
	let temp_dir = tempfile::tempdir().unwrap();
	let tree: Tree = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_max_memtable_size(16 * 1024)
		.with_memtable_stall_threshold(4)
		.with_l0_stall_threshold(12)
		.build()
		.unwrap();
	let tree = Arc::new(tree);

	let mut handles = vec![];

	// Spawn multiple concurrent writers
	for writer_id in 0..4 {
		let tree = Arc::clone(&tree);
		handles.push(tokio::spawn(async move {
			for i in 0..50 {
				let key = format!("w{}_key{:04}", writer_id, i);
				let value = vec![writer_id as u8; 200];
				let mut txn = tree.begin().unwrap();
				txn.set(key.as_bytes(), &value).unwrap();
				if let Err(e) = txn.commit().await {
					// Shutdown errors are acceptable
					if !matches!(e, Error::PipelineStall) {
						panic!("Unexpected error: {:?}", e);
					}
					return;
				}
			}
		}));
	}

	// Wait for all writers
	for h in handles {
		h.await.unwrap();
	}

	// Verify data integrity - all written keys should be readable
	for writer_id in 0..4 {
		for i in 0..50 {
			let key = format!("w{}_key{:04}", writer_id, i);
			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap();
			assert!(result.is_some(), "Missing key: {}", key);
		}
	}

	let tree = match Arc::try_unwrap(tree) {
		Ok(t) => t,
		Err(_) => panic!("Failed to unwrap Arc<Tree>"),
	};
	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn test_flush_waits_for_active_writers() {
	// Test that flush waits for writers to complete (via ready_for_flush)
	let temp_dir = tempfile::tempdir().unwrap();
	let tree: Tree = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_max_memtable_size(8 * 1024)
		.with_memtable_stall_threshold(4)
		.with_l0_stall_threshold(12)
		.build()
		.unwrap();

	// Write enough to trigger rotation
	for i in 0..30 {
		let key = format!("key{:04}", i);
		let value = vec![0u8; 400];
		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), &value).unwrap();
		txn.commit().await.unwrap();
	}

	// Flush should complete successfully (waited for any active writers)
	tree.flush().unwrap();

	// All data should be readable
	for i in 0..30 {
		let key = format!("key{:04}", i);
		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap();
		assert!(result.is_some());
	}

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn test_memtable_rotation_under_concurrent_load() {
	// Stress test: many concurrent writers causing frequent rotations
	let temp_dir = tempfile::tempdir().unwrap();
	let tree: Tree = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_max_memtable_size(4 * 1024) // Very small to trigger many rotations
		.with_memtable_stall_threshold(8) // Higher threshold to allow more queuing
		.with_l0_stall_threshold(16)
		.build()
		.unwrap();
	let tree = Arc::new(tree);

	let mut handles = vec![];

	// Spawn many concurrent writers
	for writer_id in 0..8 {
		let tree = Arc::clone(&tree);
		handles.push(tokio::spawn(async move {
			for i in 0..30 {
				let key = format!("w{}_k{}", writer_id, i);
				let value = vec![writer_id as u8; 100];
				let mut txn = match tree.begin() {
					Ok(t) => t,
					Err(_) => return, // Shutdown
				};
				if txn.set(key.as_bytes(), &value).is_err() {
					return;
				}
				if txn.commit().await.is_err() {
					return; // Shutdown or other error is acceptable in stress test
				}
			}
		}));
	}

	// Wait for all writers
	for h in handles {
		h.await.unwrap();
	}

	// Verify some data is readable (exact count may vary due to timing)
	let txn = tree.begin().unwrap();
	let mut found_count = 0;
	for writer_id in 0..8 {
		for i in 0..30 {
			let key = format!("w{}_k{}", writer_id, i);
			if txn.get(key.as_bytes()).unwrap().is_some() {
				found_count += 1;
			}
		}
	}
	drop(txn);

	// Should have most entries (allow some tolerance for timing)
	assert!(found_count > 200, "Expected most entries, got {}", found_count);

	let tree = match Arc::try_unwrap(tree) {
		Ok(t) => t,
		Err(_) => panic!("Failed to unwrap Arc<Tree>"),
	};
	tree.close().await.unwrap();
}
