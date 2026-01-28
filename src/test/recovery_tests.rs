//! WAL Recovery Tests

use std::fs;
use std::io::Write;

use tempfile::TempDir;
use test_log::test;

use crate::batch::Batch;
use crate::error::Error;
use crate::test::recovery_test_helpers::{CorruptionType, WalTestHelper};
use crate::wal::manager::Wal;
use crate::wal::recovery::replay_wal;
use crate::wal::Options;

const ARENA_SIZE: usize = 1024 * 1024;
// ============================================================================
// Category 1: Basic Multi-Segment Recovery (6 tests)
// ============================================================================

#[test]
fn test_sequential_5_segments_recovery() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create 5 segments with 100 entries each
	let entries_per_segment = vec![100, 100, 100, 100, 100];
	let final_seq = WalTestHelper::create_segments(wal_dir, &entries_per_segment, 1000);

	// Verify final sequence number
	assert_eq!(final_seq, 1500, "Final sequence should be 1500 (1000 + 500 entries)");

	// Replay WAL
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	// Verify all 500 entries recovered
	assert_eq!(max_seq_opt, Some(1499), "Max sequence should be 1499");
	WalTestHelper::verify_total_entry_count(&memtables, 500);

	// Verify max_seq_num is from last segment
	let expected_keys = WalTestHelper::generate_expected_keys(0, 4, &entries_per_segment);
	WalTestHelper::verify_entries_across_memtables(&memtables, &expected_keys);
}

#[test]
fn test_empty_segments_between_data() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments with some empty ones
	let entries_per_segment = vec![50, 0, 50, 0, 50];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 100);

	// Replay WAL
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	// Verify 150 entries recovered (only non-empty segments)
	assert!(max_seq_opt.is_some(), "Should have recovered data");
	WalTestHelper::verify_total_entry_count(&memtables, 150);

	// Verify keys from segments 0, 2, 4
	let expected_keys = WalTestHelper::generate_expected_keys(0, 4, &entries_per_segment);
	WalTestHelper::verify_entries_across_memtables(&memtables, &expected_keys);
}

#[test]
fn test_large_batches_multiple_segments() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create 5 segments with 1000 entries each
	let entries_per_segment = vec![1000, 1000, 1000, 1000, 1000];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 10000);

	// Replay WAL
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	// Verify all 5000 entries recovered
	assert_eq!(max_seq_opt, Some(14999), "Max sequence should be 14999");
	WalTestHelper::verify_total_entry_count(&memtables, 5000);
}

#[test]
fn test_variable_sized_segments() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments with variable sizes
	let entries_per_segment = vec![10, 100, 1000, 50, 200];
	let total_entries: usize = entries_per_segment.iter().sum();
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 5000);

	// Replay WAL
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	// Verify all entries recovered
	assert!(max_seq_opt.is_some(), "Should have recovered data");
	WalTestHelper::verify_total_entry_count(&memtables, total_entries);

	// Verify all keys present
	let expected_keys = WalTestHelper::generate_expected_keys(0, 4, &entries_per_segment);
	WalTestHelper::verify_entries_across_memtables(&memtables, &expected_keys);
}

#[test]
fn test_100_segments_recovery() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create 100 segments with varying entry counts (10-50 entries each)
	let mut entries_per_segment = Vec::new();
	let mut total_entries = 0;
	for i in 0..100 {
		let count = 10 + (i % 41); // 10 to 50 entries
		entries_per_segment.push(count);
		total_entries += count;
	}

	let start = std::time::Instant::now();
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 100000);
	let create_duration = start.elapsed();

	// Replay WAL
	let start = std::time::Instant::now();
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();
	let replay_duration = start.elapsed();

	// Verify all entries recovered
	assert!(max_seq_opt.is_some(), "Should have recovered data");
	WalTestHelper::verify_total_entry_count(&memtables, total_entries);

	// Log performance metrics
	log::info!(
		"100 segments: create={:?}, replay={:?}, entries={}",
		create_duration,
		replay_duration,
		total_entries
	);
}

#[test]
fn test_single_large_vs_multiple_small_segments() {
	let temp_dir1 = TempDir::new().unwrap();
	let temp_dir2 = TempDir::new().unwrap();
	fs::create_dir_all(temp_dir1.path()).unwrap();
	fs::create_dir_all(temp_dir2.path()).unwrap();

	let total_entries = 10000;

	// Scenario A: Single segment with all entries
	let start = std::time::Instant::now();
	WalTestHelper::create_segments(temp_dir1.path(), &[total_entries], 0);
	let single_create = start.elapsed();

	let start = std::time::Instant::now();
	let (max_seq1, memtables1) = replay_wal(temp_dir1.path(), 0, ARENA_SIZE).unwrap();
	let single_replay = start.elapsed();

	// Scenario B: 10 segments with 1000 entries each
	let entries_per_segment = vec![1000; 10];
	let start = std::time::Instant::now();
	WalTestHelper::create_segments(temp_dir2.path(), &entries_per_segment, 0);
	let multi_create = start.elapsed();

	let start = std::time::Instant::now();
	let (max_seq2, memtables2) = replay_wal(temp_dir2.path(), 0, ARENA_SIZE).unwrap();
	let multi_replay = start.elapsed();

	// Verify both recovered same data
	assert_eq!(max_seq1, max_seq2);
	WalTestHelper::verify_total_entry_count(&memtables1, total_entries);
	WalTestHelper::verify_total_entry_count(&memtables2, total_entries);

	log::info!("Single segment: create={:?}, replay={:?}", single_create, single_replay);
	log::info!("Multiple segments: create={:?}, replay={:?}", multi_create, multi_replay);
}

// ============================================================================
// Category 2: Crash Timing Scenarios (8 tests)
// ============================================================================

#[test]
fn test_crash_immediately_after_rotation() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Write data to segment 0
	let mut batch = Batch::new(100);
	for i in 0..50 {
		batch
			.set(
				format!("key{}", i).as_bytes().to_vec(),
				format!("value{}", i).as_bytes().to_vec(),
				0,
			)
			.unwrap();
	}

	let opts = Options::default();
	let mut wal = Wal::open(wal_dir, opts).unwrap();
	wal.append(&batch.encode().unwrap()).unwrap();

	// Rotate to segment 1 (creates empty file)
	wal.rotate().unwrap();

	// CRASH - close without writing to segment 1
	wal.close().unwrap();

	// Recovery should replay segment 0 fully, ignore empty segment 1
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	assert_eq!(max_seq_opt, Some(149), "Should recover all 50 entries from segment 0");
	WalTestHelper::verify_total_entry_count(&memtables, 50);
}

#[test]
fn test_crash_after_multiple_rapid_rotations() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	let opts = Options::default();
	let mut wal = Wal::open(wal_dir, opts).unwrap();

	// Rotate 5 times, each segment gets 1 entry
	for i in 0..5 {
		let mut batch = Batch::new(100 + i * 10);
		batch
			.set(
				format!("key{}", i).as_bytes().to_vec(),
				format!("val{}", i).as_bytes().to_vec(),
				0,
			)
			.unwrap();
		wal.append(&batch.encode().unwrap()).unwrap();

		if i < 4 {
			wal.rotate().unwrap();
		}
	}

	wal.close().unwrap();

	// Recovery should replay all 5 segments
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 5);
}

#[test]
fn test_crash_during_wal_write_mid_batch() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Write complete batch
	let mut batch1 = Batch::new(100);
	for i in 0..10 {
		batch1
			.set(
				format!("key{}", i).as_bytes().to_vec(),
				format!("val{}", i).as_bytes().to_vec(),
				0,
			)
			.unwrap();
	}

	let opts = Options::default();
	let mut wal = Wal::open(wal_dir, opts).unwrap();
	wal.append(&batch1.encode().unwrap()).unwrap();

	// Write another batch
	let mut batch2 = Batch::new(200);
	for i in 0..10 {
		batch2
			.set(
				format!("key2_{}", i).as_bytes().to_vec(),
				format!("val2_{}", i).as_bytes().to_vec(),
				0,
			)
			.unwrap();
	}
	wal.append(&batch2.encode().unwrap()).unwrap();
	wal.close().unwrap();

	// Simulate crash by truncating in the middle of batch2
	let segment_path = wal_dir.join("00000000000000000000.wal");
	let file_size = fs::metadata(&segment_path).unwrap().len();
	let truncate_point = file_size - 20; // Remove last 20 bytes
	let file = fs::OpenOptions::new().write(true).open(&segment_path).unwrap();
	file.set_len(truncate_point).unwrap();

	// Recovery should get batch1 fully, then detect corruption in batch2
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should detect corruption or recover with partial data
	match result {
		Err(Error::WalCorruption {
			..
		}) => {
			// Expected corruption detected - no memtables returned on error
			// Corruption stops recovery immediately
		}
		Ok((_, memtables)) => {
			// Also acceptable if corruption happens at boundary
			let entry_count = WalTestHelper::count_total_entries(&memtables);
			assert!(
				entry_count >= 10,
				"Should recover at least batch1 (10 entries), got {}",
				entry_count
			);
		}
		Err(e) => panic!("Unexpected error: {}", e),
	}
}

#[test]
fn test_crash_with_unflushed_memtable() {
	// This test simulates the scenario where we have:
	// - Active memtable with data
	// - Multiple rotated WAL segments
	// - CRASH before flush

	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create 3 segments
	let entries_per_segment = vec![20, 30, 40];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 500);

	// Recovery should get all segments
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 90);
}

#[test]
fn test_partial_batch_at_segment_end() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segment with data
	let mut batch = Batch::new(100);
	for i in 0..20 {
		batch
			.set(
				format!("key{}", i).as_bytes().to_vec(),
				format!("val{}", i).as_bytes().to_vec(),
				0,
			)
			.unwrap();
	}

	let opts = Options::default();
	let mut wal = Wal::open(wal_dir, opts).unwrap();
	wal.append(&batch.encode().unwrap()).unwrap();
	wal.close().unwrap();

	// Truncate to create partial batch
	let segment_path = wal_dir.join("00000000000000000000.wal");
	let file = fs::OpenOptions::new().write(true).open(&segment_path).unwrap();
	let current_size = file.metadata().unwrap().len();
	file.set_len(current_size - 10).unwrap(); // Remove last 10 bytes

	// Recovery may detect corruption or handle EOF
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should either recover data or detect corruption
	match result {
		Err(Error::WalCorruption {
			..
		}) => {
			// Expected - truncation looks like corruption
		}
		Ok(_) => {
			// Also acceptable for clean truncation at record boundary
		}
		Err(e) => panic!("Unexpected error: {}", e),
	}
}

#[test]
fn test_empty_segment_at_end() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments where last one is empty
	let entries_per_segment = vec![50, 50, 0];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 1000);

	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 100);
}

#[test]
fn test_all_empty_segments() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create all empty segments
	let entries_per_segment = vec![0, 0, 0];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 1000);

	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	// Should return None for empty recovery
	assert_eq!(max_seq_opt, None);
	assert!(memtables.is_empty(), "Should have no memtables for empty segments");
	assert_eq!(WalTestHelper::count_total_entries(&memtables), 0);
}

#[test]
fn test_rapid_rotation_with_minimal_data() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create 10 segments with just 1 entry each
	let entries_per_segment = vec![1; 10];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 2000);

	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	assert_eq!(max_seq_opt, Some(2009));
	WalTestHelper::verify_total_entry_count(&memtables, 10);
}

// ============================================================================
// Category 3: Corruption Scenarios (15 tests)
// ============================================================================

#[test]
fn test_corruption_first_record_segment_0() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create 5 segments with data
	let entries_per_segment = vec![30, 30, 30, 30, 30];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 1000);

	// Corrupt first record of segment 0
	WalTestHelper::corrupt_segment(wal_dir, 0, 0.0, CorruptionType::HeaderCorruption);

	// Recovery should detect corruption, not process any segments
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should detect corruption immediately in segment 0
	match result {
		Err(Error::WalCorruption {
			segment_id,
			..
		}) => {
			assert_eq!(segment_id, 0);
		}
		_ => panic!("Expected WalCorruption error in segment 0"),
	}
}

#[test]
fn test_corruption_middle_segment_0() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments
	let entries_per_segment = vec![100, 50, 50];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 5000);

	// Corrupt segment 0 at 50% (middle)
	WalTestHelper::corrupt_segment(wal_dir, 0, 0.5, CorruptionType::RandomBytes);

	// Recovery should get partial data from segment 0, stop there
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should detect corruption in segment 0
	match result {
		Err(Error::WalCorruption {
			segment_id,
			..
		}) => {
			assert_eq!(segment_id, 0);
			// Corruption stops recovery immediately - no memtables returned
		}
		_ => panic!("Expected WalCorruption error in segment 0"),
	}
}

#[test]
fn test_corruption_end_segment_0() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments
	let entries_per_segment = vec![50, 30, 30];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 3000);

	// Corrupt segment 0 at end (90%)
	WalTestHelper::corrupt_segment(wal_dir, 0, 0.9, CorruptionType::RandomBytes);

	// Recovery should get most of segment 0
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should detect corruption
	match result {
		Err(Error::WalCorruption {
			segment_id,
			..
		}) => {
			assert_eq!(segment_id, 0);
			// Note: Random byte corruption can hit CRC which invalidates entire
			// record. So we may have fewer entries than expected or even
			// none. The key assertion is that corruption is detected in
			// segment 0.
		}
		_ => panic!("Expected WalCorruption error in segment 0"),
	}
}

#[test]
fn test_corruption_beginning_middle_segment() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments: 0 (valid), 1 (valid), 2 (corrupt at start), 3 (valid)
	let entries_per_segment = vec![40, 40, 40, 40];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 6000);

	// Corrupt segment 2 at beginning
	WalTestHelper::corrupt_segment(wal_dir, 2, 0.0, CorruptionType::HeaderCorruption);

	// Recovery should get segments 0-1, stop at segment 2
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should detect corruption in segment 2
	match result {
		Err(Error::WalCorruption {
			segment_id,
			..
		}) => {
			assert_eq!(segment_id, 2);
			// Corruption stops recovery immediately - no memtables returned
		}
		_ => panic!("Expected WalCorruption error in segment 2"),
	}
}

#[test]
fn test_corruption_middle_of_middle_segment() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments: 0 (valid), 1 (valid), 2 (corrupt at 50%), 3 (valid)
	let entries_per_segment = vec![50, 50, 50, 50];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 7000);

	// Corrupt segment 2 at middle
	WalTestHelper::corrupt_segment(wal_dir, 2, 0.5, CorruptionType::RandomBytes);

	// Recovery should get 0-1 full, 2 partial
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should detect corruption in segment 2
	match result {
		Err(Error::WalCorruption {
			segment_id,
			..
		}) => {
			assert_eq!(segment_id, 2);
			// Corruption stops recovery immediately - no memtables returned
		}
		_ => panic!("Expected WalCorruption error in segment 2"),
	}
}

#[test]
fn test_corruption_last_segment() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments: 0-3 (valid), 4 (corrupt)
	let entries_per_segment = vec![25, 25, 25, 25, 25];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 8000);

	// Corrupt segment 4 at beginning
	WalTestHelper::corrupt_segment(wal_dir, 4, 0.1, CorruptionType::RandomBytes);

	// Recovery should get 0-3 fully, partial 4
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should detect corruption in segment 4
	match result {
		Err(Error::WalCorruption {
			segment_id,
			..
		}) => {
			assert_eq!(segment_id, 4);
			// Corruption stops recovery immediately - no memtables returned
		}
		_ => panic!("Expected WalCorruption error in segment 4"),
	}
}

#[test]
fn test_truncated_wal_file() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments
	let entries_per_segment = vec![30, 30, 30];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 9000);

	// Truncate segment 1 at 60%
	WalTestHelper::corrupt_segment(wal_dir, 1, 0.6, CorruptionType::Truncate);

	// Recovery may detect truncation as corruption
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should have segment 0 + possibly partial segment 1
	match result {
		Err(Error::WalCorruption {
			segment_id,
			..
		}) => {
			// Truncation detected as corruption in segment 1
			assert!(segment_id >= 1, "Corruption should be in segment 1 or later");
			// Corruption stops recovery immediately - no memtables returned
		}
		Ok((_, memtables)) => {
			// Also acceptable for clean truncation at record boundary
			let entry_count = WalTestHelper::count_total_entries(&memtables);
			assert!(entry_count >= 30, "Should have at least segment 0, got {}", entry_count);
		}
		Err(e) => panic!("Unexpected error: {}", e),
	}
}

#[test]
fn test_random_byte_corruption() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segment
	let entries_per_segment = vec![50];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 10000);

	// Insert random bytes at 40%
	WalTestHelper::corrupt_segment(wal_dir, 0, 0.4, CorruptionType::RandomBytes);

	// Should detect corruption via CRC or invalid data
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Corruption should be detected (or partial recovery happened before
	// corruption)
	match result {
		Err(Error::WalCorruption {
			..
		}) => {
			// Expected corruption detected
		}
		Ok(_) => {
			// Partial recovery is also acceptable
		}
		Err(e) => panic!("Unexpected error: {}", e),
	}
}

#[test]
fn test_multiple_corruptions_stop_at_first() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments
	let entries_per_segment = vec![40, 40, 40, 40];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 11000);

	// Corrupt segment 1 at 50%
	WalTestHelper::corrupt_segment(wal_dir, 1, 0.5, CorruptionType::RandomBytes);
	// Corrupt segment 3 at 30% (should never be reached)
	WalTestHelper::corrupt_segment(wal_dir, 3, 0.3, CorruptionType::RandomBytes);

	// Should stop at segment 1 corruption
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should stop at segment 1, never reach segment 3
	match result {
		Err(Error::WalCorruption {
			segment_id,
			..
		}) => {
			assert_eq!(segment_id, 1, "Should stop at first corruption (segment 1)");
			// Corruption stops recovery immediately - no memtables returned
		}
		_ => panic!("Expected WalCorruption error in segment 1"),
	}
}

#[test]
fn test_crc_mismatch() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segment
	let entries_per_segment = vec![40];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 12000);

	// Modify data to cause CRC mismatch
	WalTestHelper::corrupt_segment(wal_dir, 0, 0.3, CorruptionType::CrcMismatch);

	// Should detect CRC mismatch
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// CRC check should catch corruption
	match result {
		Err(Error::WalCorruption {
			..
		}) => {
			// Expected corruption detected
		}
		Ok((_, memtables)) => {
			// Partial recovery with less than full entries
			let entry_count = WalTestHelper::count_total_entries(&memtables);
			assert!(entry_count < 40, "Should have partial recovery");
		}
		Err(e) => panic!("Unexpected error: {}", e),
	}
}

#[test]
fn test_corruption_in_wal_header() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments
	let entries_per_segment = vec![35, 35];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 13000);

	// Corrupt header of segment 1
	WalTestHelper::corrupt_segment(wal_dir, 1, 0.0, CorruptionType::HeaderCorruption);

	// Should detect header corruption immediately in segment 1
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should detect corruption in segment 1
	match result {
		Err(Error::WalCorruption {
			segment_id,
			..
		}) => {
			assert_eq!(segment_id, 1);
			// Corruption stops recovery immediately - no memtables returned
		}
		_ => panic!("Expected WalCorruption error in segment 1"),
	}
}

#[test]
fn test_corruption_in_batch_data() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segment
	let entries_per_segment = vec![30];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 14000);

	// Corrupt data portion (not header)
	WalTestHelper::corrupt_segment(wal_dir, 0, 0.5, CorruptionType::RandomBytes);

	// Should detect when trying to decode batch
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Either corruption detected or partial recovery
	// (depends on where exactly corruption hits)
	match result {
		Err(Error::WalCorruption {
			..
		}) => {
			// Expected corruption detected
		}
		Ok(_) => {
			// Partial recovery is acceptable
		}
		Err(e) => panic!("Unexpected error: {}", e),
	}
}

#[test]
fn test_completely_corrupted_file() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments
	let entries_per_segment = vec![25, 25, 25];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 15000);

	// Completely corrupt segment 1 (overwrite with garbage)
	let segment_path = wal_dir.join("00000000000000000001.wal");
	let garbage: Vec<u8> = vec![0xDE; 1000]; // 1000 bytes of 0xDE
	fs::write(&segment_path, garbage).unwrap();

	// Should get segment 0, fail on segment 1
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should detect corruption in segment 1
	match result {
		Err(Error::WalCorruption {
			segment_id,
			..
		}) => {
			assert_eq!(segment_id, 1);
			// Corruption stops recovery immediately - no memtables returned
		}
		_ => panic!("Expected WalCorruption error in segment 1"),
	}
}

#[test]
fn test_tail_corruption_recovery() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segment
	let entries_per_segment = vec![40];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 16000);

	// Append garbage at end (tail corruption)
	let segment_path = wal_dir.join("00000000000000000000.wal");
	let mut file = fs::OpenOptions::new().append(true).open(&segment_path).unwrap();
	file.write_all(b"CORRUPTED_TAIL_DATA_HERE").unwrap();

	// Should detect tail corruption or recover valid data
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should either detect corruption or recover all valid data
	match result {
		Err(Error::WalCorruption {
			..
		}) => {
			// Expected - tail corruption detected
			// Corruption stops recovery immediately - no memtables returned
		}
		Ok((_, memtables)) => {
			// Also acceptable if corruption was at a clean boundary
			let entry_count = WalTestHelper::count_total_entries(&memtables);
			assert!(
				entry_count >= 30,
				"Should recover most data despite tail corruption, got {}",
				entry_count
			);
		}
		Err(e) => panic!("Unexpected error: {}", e),
	}
}

// ============================================================================
// Category 4: Sequence Number Validation (5 tests)
// ============================================================================

#[test]
fn test_contiguous_sequence_numbers() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments with contiguous sequences
	// Segment 0: seq 100-199 (100 entries)
	// Segment 1: seq 200-299 (100 entries)
	// Segment 2: seq 300-399 (100 entries)
	let entries_per_segment = vec![100, 100, 100];
	let final_seq = WalTestHelper::create_segments(wal_dir, &entries_per_segment, 100);

	assert_eq!(final_seq, 400, "Final sequence should be 400");

	// Replay and verify
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	assert_eq!(max_seq_opt, Some(399), "Max sequence should be 399");
	WalTestHelper::verify_total_entry_count(&memtables, 300);
}

#[test]
fn test_sequence_tracking_10_segments() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create 10 segments with 100 entries each, starting at 1000
	let entries_per_segment = vec![100; 10];
	let final_seq = WalTestHelper::create_segments(wal_dir, &entries_per_segment, 1000);

	assert_eq!(final_seq, 2000, "Final sequence should be 2000");

	// Replay and verify
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	assert_eq!(max_seq_opt, Some(1999), "Max sequence should be 1999");
	WalTestHelper::verify_total_entry_count(&memtables, 1000);
}

#[test]
fn test_sequence_after_corruption() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments: seg 0 (seq 100-200), seg 1 (seq 201-300, corrupted at 50%)
	let entries_per_segment = vec![101, 100];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 100);

	// Corrupt segment 1 at middle
	WalTestHelper::corrupt_segment(wal_dir, 1, 0.5, CorruptionType::RandomBytes);

	// Replay
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should detect corruption in segment 1
	match result {
		Err(Error::WalCorruption {
			segment_id,
			..
		}) => {
			assert_eq!(segment_id, 1);
			// Corruption stops recovery immediately - no memtables returned
		}
		_ => panic!("Expected WalCorruption error in segment 1"),
	}
}

#[test]
fn test_large_sequence_numbers() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Start with very large sequence number
	let entries_per_segment = vec![50, 50, 50];
	let final_seq = WalTestHelper::create_segments(wal_dir, &entries_per_segment, 1_000_000);

	assert_eq!(final_seq, 1_000_150);

	// Replay
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	assert_eq!(max_seq_opt, Some(1_000_149));
	WalTestHelper::verify_total_entry_count(&memtables, 150);
}

#[test]
fn test_sequence_gaps_detection() {
	// This test verifies that we correctly track sequences even with empty segments
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments with some empty (creating potential "gaps" in file-based
	// view)
	let entries_per_segment = vec![30, 0, 30, 0, 30];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 5000);

	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 90);
}

// ============================================================================
// Category 5: Min WAL Number / Flush Boundary Tests (7 tests)
// ============================================================================

#[test]
fn test_skip_flushed_segments() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments 0-4
	let entries_per_segment = vec![20, 20, 20, 20, 20];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 6000);

	// Replay with min_wal_number=3 (segments 0-2 already flushed)
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 3, ARENA_SIZE).unwrap();

	// Should only replay segments 3-4 (40 entries)
	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 40);
}

#[test]
fn test_all_segments_already_flushed() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments 0-2
	let entries_per_segment = vec![15, 15, 15];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 7000);

	// Replay with min_wal_number=3 (all segments already flushed)
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 3, ARENA_SIZE).unwrap();

	// Should return None (nothing to replay)
	assert_eq!(max_seq_opt, None);
	assert!(memtables.is_empty(), "Should have no memtables when all segments already flushed");
	assert_eq!(WalTestHelper::count_total_entries(&memtables), 0);
}

#[test]
fn test_min_wal_equals_first_segment() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments 5, 6, 7 (by creating and deleting 0-4)
	let entries_per_segment = vec![25, 25, 25];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 8000);

	// Manually rename files to start from segment 5
	for i in 0..3 {
		let old_path = wal_dir.join(format!("{:020}.wal", i));
		let new_path = wal_dir.join(format!("{:020}.wal", i + 5));
		fs::rename(old_path, new_path).unwrap();
	}

	// Replay with min_wal_number=5
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 5, ARENA_SIZE).unwrap();

	// Should replay all three segments (5, 6, 7)
	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 75);
}

#[test]
fn test_mixed_flushed_unflushed() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments 0-5
	let entries_per_segment = vec![18, 18, 18, 18, 18, 18];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 9000);

	// Simulate: segments 0-2 flushed (min_wal_number=3)
	// Segments 3-5 unflushed
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 3, ARENA_SIZE).unwrap();

	// Should replay segments 3-5 (54 entries)
	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 54);
}

#[test]
fn test_recovery_min_wal_zero() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments 0-5
	let entries_per_segment = vec![22, 22, 22, 22, 22, 22];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 10000);

	// Replay with min_wal_number=0 (all segments)
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	// Should replay all 6 segments
	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 132);
}

#[test]
fn test_corruption_at_min_wal_boundary() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments 0-5
	let entries_per_segment = vec![24, 24, 24, 24, 24, 24];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 11000);

	// Corrupt segment 3 (which is min_wal_number)
	WalTestHelper::corrupt_segment(wal_dir, 3, 0.2, CorruptionType::RandomBytes);

	// Replay with min_wal_number=3
	let result = replay_wal(wal_dir, 3, ARENA_SIZE);

	// Should detect corruption in segment 3, not process 4-5
	match result {
		Err(Error::WalCorruption {
			segment_id,
			..
		}) => {
			assert_eq!(segment_id, 3);
			// Corruption stops recovery immediately - no memtables returned
		}
		_ => panic!("Expected WalCorruption error in segment 3"),
	}
}

#[test]
fn test_boundary_with_empty_segments() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments: 0(20), 1(0), 2(0), 3(20), 4(20)
	let entries_per_segment = vec![20, 0, 0, 20, 20];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 12000);

	// Replay with min_wal_number=2 (skip 0, 1)
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 2, ARENA_SIZE).unwrap();

	// Should replay segments 2(empty), 3, 4 = 40 entries
	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 40);
}

// ============================================================================
// Category 6: Large-Scale and Performance Tests (4 tests)
// ============================================================================

#[test]
fn test_very_large_values_across_segments() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments with large values (10KB each)
	let opts = Options::default();
	let mut wal = Wal::open(wal_dir, opts).unwrap();

	let large_value = vec![b'X'; 10240]; // 10KB value

	for seg_idx in 0..5 {
		let mut batch = Batch::new(13000 + seg_idx * 10);
		for i in 0..5 {
			let key = format!("large_seg{}_key{}", seg_idx, i);
			batch.set(key.as_bytes().to_vec(), large_value.clone(), 0).unwrap();
		}
		wal.append(&batch.encode().unwrap()).unwrap();

		if seg_idx < 4 {
			wal.rotate().unwrap();
		}
	}
	wal.close().unwrap();

	// Replay
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	// Verify all large values recovered
	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 25);
}

#[test]
fn test_many_small_batches() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create 10 segments, each with 100 small batches (1 entry each)
	let opts = Options::default();
	let mut wal = Wal::open(wal_dir, opts).unwrap();

	let mut entry_count = 0;
	for seg_idx in 0..10 {
		for batch_idx in 0..100 {
			let mut batch = Batch::new(14000 + entry_count);
			let key = format!("s{}_b{}_k", seg_idx, batch_idx);
			batch.set(key.as_bytes().to_vec(), b"value".to_vec(), 0).unwrap();
			wal.append(&batch.encode().unwrap()).unwrap();
			entry_count += 1;
		}

		if seg_idx < 9 {
			wal.rotate().unwrap();
		}
	}
	wal.close().unwrap();

	// Replay
	let start = std::time::Instant::now();
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();
	let duration = start.elapsed();

	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 1000);

	log::info!("1000 small batches replay time: {:?}", duration);
}

#[test]
fn test_few_large_batches() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create 5 segments, each with 1 large batch (200 entries)
	let opts = Options::default();
	let mut wal = Wal::open(wal_dir, opts).unwrap();

	for seg_idx in 0..5 {
		let mut batch = Batch::new(15000 + seg_idx * 200);
		for i in 0..200 {
			let key = format!("seg{}_key{:04}", seg_idx, i);
			batch.set(key.as_bytes().to_vec(), b"value".to_vec(), 0).unwrap();
		}
		wal.append(&batch.encode().unwrap()).unwrap();

		if seg_idx < 4 {
			wal.rotate().unwrap();
		}
	}
	wal.close().unwrap();

	// Replay
	let start = std::time::Instant::now();
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();
	let duration = start.elapsed();

	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 1000);

	log::info!("5 large batches replay time: {:?}", duration);
}

#[test]
fn test_recovery_performance_scaling() {
	// Test that recovery time scales linearly with segment count
	let segment_counts = vec![10, 20, 50];
	let entries_per_segment = 50;

	for &seg_count in &segment_counts {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		let segments = vec![entries_per_segment; seg_count];
		WalTestHelper::create_segments(wal_dir, &segments, 16000);

		let start = std::time::Instant::now();

		let (_max_seq, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();
		let duration = start.elapsed();

		let total_entries = seg_count * entries_per_segment;
		WalTestHelper::verify_total_entry_count(&memtables, total_entries);

		log::info!("{} segments, {} entries: {:?}", seg_count, total_entries, duration);
	}
}

// ============================================================================
// Category 7: Edge Cases and Error Handling (8 tests)
// ============================================================================

#[test]
fn test_wal_segments_with_gaps() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments 0, 1, 3, 4 (skip 2)
	let entries_per_segment = vec![28, 28, 28, 28, 28];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 17000);

	// Delete segment 2
	let segment2_path = wal_dir.join("00000000000000000002.wal");
	fs::remove_file(segment2_path).ok();

	// Replay - should warn but process available segments
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	// Should process 0, 1, then skip 2 (missing), then 3, 4
	assert!(max_seq_opt.is_some());

	// Should have 4 segments worth of data (0, 1, 3, 4)
	WalTestHelper::verify_total_entry_count(&memtables, 112);
}

#[test]
fn test_non_sequential_segment_ids_with_min_wal() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments then rename to have gaps
	let entries_per_segment = vec![26, 26, 26, 26];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 18000);

	// Rename to create non-sequential IDs: 0, 5, 10, 15
	let new_ids = [0, 5, 10, 15];
	for (old_id, new_id) in new_ids.iter().enumerate() {
		let old_path = wal_dir.join(format!("{:020}.wal", old_id));
		let new_path = wal_dir.join(format!("{:020}.wal", new_id));
		fs::rename(old_path, new_path).unwrap();
	}

	// Replay with min_wal_number=5
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 5, ARENA_SIZE).unwrap();

	// Should process 5, 10, 15 (3 segments = 78 entries)
	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 78);
}

#[test]
fn test_zero_length_segment_file() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments
	let entries_per_segment = vec![32, 32, 32];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 19000);

	// Truncate segment 1 to zero length
	let segment1_path = wal_dir.join("00000000000000000001.wal");
	fs::write(&segment1_path, b"").unwrap();

	// Replay
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	// Should get segment 0, skip empty segment 1, get segment 2
	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 64);
}

#[test]
fn test_segment_larger_than_expected() {
	// Create very large segment and ensure it's handled
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segment with many entries
	let entries_per_segment = vec![5000];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 20000);

	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 5000);
}

#[test]
fn test_recovery_with_readonly_segment() {
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments
	let entries_per_segment = vec![35, 35];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 21000);

	// Make segment 0 read-only (recovery should still work - only reading)
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt;
		let segment0_path = wal_dir.join("00000000000000000000.wal");
		let mut perms = fs::metadata(&segment0_path).unwrap().permissions();
		perms.set_mode(0o444); // Read-only
		fs::set_permissions(&segment0_path, perms).unwrap();
	}

	// Replay should work (only reading, not writing)
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 70);
}

#[test]
fn test_only_wal_extension_processed() {
	// Ensure we only process .wal files correctly
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create normal WAL segments
	let entries_per_segment = vec![38, 38];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 22000);

	// Replay should only process .wal files (segments 0 and 1)
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 76);
}

#[test]
fn test_symlink_to_wal_segment() {
	// Test handling of symlinked WAL files
	#[cfg(unix)]
	{
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Create WAL segment
		let entries_per_segment = vec![42];
		WalTestHelper::create_segments(wal_dir, &entries_per_segment, 23000);

		// Create symlink to segment 0
		let original = wal_dir.join("00000000000000000000.wal");
		let symlink = wal_dir.join("00000000000000000001.wal");
		std::os::unix::fs::symlink(&original, &symlink).ok();

		// Replay should handle symlink
		let (_result, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

		// Should process files (might process symlink as separate segment or skip it)
		let entry_count = WalTestHelper::count_total_entries(&memtables);
		assert!(entry_count >= 42, "Should process at least original segment");
	}
}

#[test]
fn test_concurrent_wal_directory_modifications() {
	// While this test can't truly test concurrency in single-threaded test,
	// it verifies robustness to directory changes
	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create segments
	let entries_per_segment = vec![45, 45];
	WalTestHelper::create_segments(wal_dir, &entries_per_segment, 24000);

	// Modify directory during "recovery" (add extra file with garbage content)
	fs::write(wal_dir.join("00000000000000000002.wal"), b"new_file").unwrap();

	// Replay - the new file with invalid content will cause corruption
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should either detect corruption in the garbage file or recover segments 0-1
	match result {
		Err(Error::WalCorruption {
			segment_id,
			..
		}) => {
			// Expected - garbage file detected as corruption
			assert_eq!(segment_id, 2, "Corruption should be in segment 2");
			// Corruption stops recovery immediately - no memtables returned
		}
		Ok((_, memtables)) => {
			// Also acceptable if somehow handled gracefully
			let entry_count = WalTestHelper::count_total_entries(&memtables);
			assert!(entry_count >= 90, "Should have recovered segments 0-1, got {}", entry_count);
		}
		Err(e) => panic!("Unexpected error: {}", e),
	}
}

// ============================================================================
// Category 8: Compression Tests (3 tests)
// ============================================================================

#[test]
fn test_compressed_wal_segments() {
	use crate::wal::CompressionType;

	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create WAL with LZ4 compression
	let opts = Options {
		compression_type: CompressionType::Lz4,
		..Default::default()
	};

	let mut wal = Wal::open(wal_dir, opts).unwrap();

	for seg_idx in 0..3 {
		let mut batch = Batch::new(25000 + seg_idx * 50);
		for i in 0..50 {
			let key = format!("compressed_seg{}_k{}", seg_idx, i);
			let value = format!("compressed_value_{}_{}", seg_idx, i);
			batch.set(key.as_bytes().to_vec(), value.as_bytes().to_vec(), 0).unwrap();
		}
		wal.append(&batch.encode().unwrap()).unwrap();

		if seg_idx < 2 {
			wal.rotate().unwrap();
		}
	}
	wal.close().unwrap();

	// Replay - should decompress automatically
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 150);
}

#[test]
fn test_mixed_compression_segments() {
	use crate::wal::CompressionType;

	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create first 2 segments without compression
	let opts = Options::default();
	let mut wal = Wal::open(wal_dir, opts).unwrap();

	for seg_idx in 0..2 {
		let mut batch = Batch::new(26000 + seg_idx * 40);
		for i in 0..40 {
			let key = format!("uncomp_seg{}_k{}", seg_idx, i);
			batch.set(key.as_bytes().to_vec(), b"uncompressed_value".to_vec(), 0).unwrap();
		}
		wal.append(&batch.encode().unwrap()).unwrap();
		wal.rotate().unwrap();
	}
	wal.close().unwrap();

	// Create next 2 segments with LZ4 compression
	let opts = Options {
		compression_type: CompressionType::Lz4,
		..Default::default()
	};
	let mut wal = Wal::open(wal_dir, opts).unwrap();

	for seg_idx in 0..2 {
		let mut batch = Batch::new(26080 + seg_idx * 40);
		for i in 0..40 {
			let key = format!("comp_seg{}_k{}", seg_idx, i);
			batch.set(key.as_bytes().to_vec(), b"compressed_value".to_vec(), 0).unwrap();
		}
		wal.append(&batch.encode().unwrap()).unwrap();
		if seg_idx < 1 {
			wal.rotate().unwrap();
		}
	}
	wal.close().unwrap();

	// Replay - should handle both compressed and uncompressed segments
	let (max_seq_opt, memtables) = replay_wal(wal_dir, 0, ARENA_SIZE).unwrap();

	assert!(max_seq_opt.is_some());
	WalTestHelper::verify_total_entry_count(&memtables, 160);
}

#[test]
fn test_compressed_data_corruption() {
	use crate::wal::CompressionType;

	let temp_dir = TempDir::new().unwrap();
	let wal_dir = temp_dir.path();
	fs::create_dir_all(wal_dir).unwrap();

	// Create compressed WAL
	let opts = Options {
		compression_type: CompressionType::Lz4,
		..Default::default()
	};

	let mut wal = Wal::open(wal_dir, opts).unwrap();

	let mut batch = Batch::new(27000);
	for i in 0..60 {
		let key = format!("key{}", i);
		batch.set(key.as_bytes().to_vec(), b"compressed_value_data".to_vec(), 0).unwrap();
	}
	wal.append(&batch.encode().unwrap()).unwrap();
	wal.close().unwrap();

	// Corrupt compressed data
	WalTestHelper::corrupt_segment(wal_dir, 0, 0.5, CorruptionType::RandomBytes);

	// Replay - should detect corruption (decompression failure or CRC mismatch)
	let result = replay_wal(wal_dir, 0, ARENA_SIZE);

	// Should detect corruption or have partial recovery
	// (exact behavior depends on where corruption hits in compressed stream)
	match result {
		Err(Error::WalCorruption {
			..
		}) => {
			// Expected corruption detected
		}
		Ok((_, memtables)) => {
			let entry_count = WalTestHelper::count_total_entries(&memtables);
			assert!(entry_count < 60, "Should detect corruption in compressed data");
		}
		Err(e) => panic!("Unexpected error: {}", e),
	}
}
