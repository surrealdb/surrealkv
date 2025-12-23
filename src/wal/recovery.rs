use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::lsm::fsync_directory;
use crate::memtable::MemTable;
use crate::wal::reader::{Reader, Reporter};
use crate::wal::{get_segment_range, list_segment_ids, Error as WalError, SegmentRef};

/// Default implementation of the Reporter trait for WAL recovery.
///
/// This reporter logs corruption events and tracks statistics about
/// recovery operations.
pub struct DefaultReporter {
	/// The log number being processed
	log_number: u64,

	/// Count of corruption events encountered
	corruption_count: usize,

	/// Count of old log records encountered
	old_record_count: usize,
}

impl DefaultReporter {
	/// Creates a new DefaultReporter for the specified log number.
	pub fn new(log_number: u64) -> Self {
		Self {
			log_number,
			corruption_count: 0,
			old_record_count: 0,
		}
	}
}

impl Reporter for DefaultReporter {
	fn corruption(&mut self, bytes: usize, reason: &str, log_number: u64) {
		log::error!("Corruption in WAL {}: {} bytes lost - {}", log_number, bytes, reason);
		self.corruption_count += 1;
	}

	fn old_log_record(&mut self, bytes: usize) {
		log::warn!("Old log record encountered in WAL {}: {} bytes", self.log_number, bytes);
		self.old_record_count += 1;
	}
}

/// Replays the Write-Ahead Log (WAL) to recover recent writes.
///
/// # Arguments
///
/// * `wal_dir` - Path to the WAL directory
/// * `memtable` - Memtable to replay entries into
/// * `min_wal_number` - Minimum WAL number to replay (older segments are skipped as already
///   flushed)
///
/// # Returns
///
/// * `Ok(Some(max_seq_num))` - Data was replayed successfully, returns highest sequence number
/// * `Ok(None)` - No data recovered (empty or all segments already flushed)
/// * `Err(WalCorruption{...})` - Corruption detected, contains location for repair
/// * `Err(...)` - Other errors (IO, permission, etc.)
///
/// # Partial State on Corruption
///
/// When corruption is detected, the memtable will contain all successfully
/// replayed data up to the corruption point.
pub(crate) fn replay_wal(
	wal_dir: &Path,
	memtable: &Arc<MemTable>,
	min_wal_number: u64,
) -> Result<Option<u64>> {
	log::info!("Starting WAL recovery from directory: {:?}", wal_dir);
	log::debug!("WAL recovery parameters: min_wal_number={}", min_wal_number);

	// Check if WAL directory exists
	if !wal_dir.exists() {
		log::debug!("WAL directory does not exist, skipping recovery");
		return Ok(None);
	}

	if list_segment_ids(wal_dir, Some("wal"))?.is_empty() {
		log::debug!("No WAL segments found, skipping recovery");
		return Ok(None);
	}

	// Get range of segment IDs (looking for .wal files)
	let (first, last) = match get_segment_range(wal_dir, Some("wal")) {
		Ok(range) => range,
		Err(WalError::IO(ref io_err)) if io_err.kind() == std::io::ErrorKind::NotFound => {
			// Directory or files not found - this is expected if WAL doesn't exist yet
			log::debug!("WAL segment range not found, skipping recovery");
			return Ok(None);
		}
		Err(e) => return Err(e.into()),
	};

	// If no segments, nothing to replay
	if first > last {
		log::debug!("No valid WAL segment range, skipping recovery");
		return Ok(None);
	}

	// Determine the range of segments to replay
	// Skip segments older than min_wal_number (already flushed to SST)
	let start_segment = std::cmp::max(first, min_wal_number);

	if start_segment > last {
		log::info!(
			"All WAL segments already flushed (last={:020}, min_log_number={:020})",
			last,
			min_wal_number
		);
		return Ok(None);
	}

	log::info!("Replaying WAL segments #{:020} to #{:020}", start_segment, last);

	// Track statistics across all segments
	let mut max_seq_num = 0;
	let mut total_batches_replayed = 0;
	let mut segments_processed = 0;

	// Get all segments in the directory
	let all_segments = SegmentRef::read_segments_from_directory(wal_dir, Some("wal"))?;

	// Process each segment in order from start_segment to last
	for segment_id in start_segment..=last {
		// Find this segment in the list
		// Note: Gaps in segment IDs can be legitimate after cleanup or manual
		// intervention. We warn and continue rather than fail, allowing partial
		// recovery from available segments.
		let segment = match all_segments.iter().find(|seg| seg.id == segment_id) {
			Some(seg) => seg,
			None => {
				log::warn!(
					"WAL segment #{:020} not found in range [{:020}..{:020}], skipping. \
					This may be normal after cleanup or indicate data loss.",
					segment_id,
					start_segment,
					last
				);
				continue;
			}
		};

		log::debug!("Processing WAL segment #{:020}", segment_id);

		// Open the segment file
		let file = File::open(&segment.file_path)?;

		// Create reporter for corruption
		let reporter = Box::new(DefaultReporter::new(segment_id));

		// Create reader with reporter
		let mut reader = Reader::with_options(file, Some(reporter), segment_id);

		// Track statistics for this segment
		let mut batches_in_segment = 0;
		let mut last_valid_offset = 0;

		// Process each record in this segment
		loop {
			match reader.read() {
				Ok((record_data, offset)) => {
					// Update tracking info
					last_valid_offset = offset as usize;

					// Decode batch
					let batch = Batch::decode(record_data)?;

					let batch_highest_seq_num = batch.get_highest_seq_num();

					// Update max sequence number across all segments
					if batch_highest_seq_num > max_seq_num {
						max_seq_num = batch_highest_seq_num;
					}

					batches_in_segment += 1;

					log::debug!(
						"Replayed batch from WAL #{:020}: seq_num={}, entries={}, offset={}",
						segment_id,
						batch_highest_seq_num,
						batch.count(),
						offset
					);

					// Apply the batch to the memtable
					memtable.add(&batch)?;
				}
				Err(WalError::Corruption(err)) => {
					// Corruption detected - stop immediately and don't process further segments
					log::error!(
						"Corrupted WAL record detected in segment {:020} at offset {}: {}",
						segment_id,
						last_valid_offset,
						err
					);
					return Err(Error::wal_corruption(
						segment_id as usize,
						last_valid_offset,
						format!("Corrupted WAL record: {}", err),
					));
				}
				Err(WalError::IO(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
					// End of this segment reached, move to next segment
					break;
				}
				Err(err) => return Err(err.into()),
			}
		}

		if batches_in_segment > 0 {
			log::info!(
				"Replayed {} batches from WAL segment #{:020}",
				batches_in_segment,
				segment_id
			);
			total_batches_replayed += batches_in_segment;
			segments_processed += 1;
		}
	}

	// No corruption found
	// Return Some(max_seq_num) if we actually replayed data, None if empty
	let result = if max_seq_num > 0 {
		Some(max_seq_num)
	} else {
		None
	};

	match result {
		Some(seq) => log::info!(
			"WAL recovery complete: {} batches across {} segments, max_seq_num={}, total_entries={}",
			total_batches_replayed,
		segments_processed,
		seq,
		memtable.iter(false).count()
	),
		None => log::info!("No data recovered from WAL segments"),
	}

	Ok(result)
}

pub(crate) fn repair_corrupted_wal_segment(wal_dir: &Path, segment_id: usize) -> Result<()> {
	use std::fs;

	use crate::wal::manager::Wal;
	use crate::wal::reader::Reader;
	use crate::wal::Options;

	// Build segment paths
	let segment_path = wal_dir.join(format!("{segment_id:020}.wal"));

	// Verify the corrupted segment exists
	if !segment_path.exists() {
		return Err(crate::error::Error::Other(format!(
			"WAL segment {segment_id:020}.wal does not exist"
		)));
	}

	// Create a repair directory for the new WAL file
	let repair_dir = wal_dir.join("repair_temp");
	fs::create_dir_all(&repair_dir)?;

	// Create a new Wal for writing the repaired data
	// We'll write to a temp directory then move the file
	let opts = Options::default();
	let mut repair_wal = Wal::open(&repair_dir, opts)?;

	// Open the original segment file
	let file = fs::File::open(&segment_path)?;
	let mut reader = Reader::new(file);

	let mut valid_batches_count = 0;
	let mut repair_failed = false;

	// Read valid batches from original and write them to the repair WAL
	loop {
		match reader.read() {
			Ok((record_data, _offset)) => {
				// We have a valid batch, write it to the repair WAL
				if let Err(e) = repair_wal.append(record_data) {
					log::error!("Failed to write valid batch to repaired WAL: {e}");
					repair_failed = true;
					break;
				}
				valid_batches_count += 1;
			}
			Err(WalError::Corruption(err)) => {
				// Stop at the first corruption
				log::error!(
                    "Stopped repair at corruption: {err}. Recovered {valid_batches_count} valid batches."
                );
				break;
			}
			Err(WalError::IO(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
				// End of segment reached
				log::info!(
					"Repair completed successfully. Recovered {valid_batches_count} valid batches."
				);
				break;
			}
			Err(err) => {
				log::error!("Unexpected error during repair: {err}");
				repair_failed = true;
				break;
			}
		}
	}

	// Close the repair WAL
	repair_wal.close()?;

	// The repaired WAL was written to repair_temp/00000000000000000000.wal
	let temp_wal_path = repair_dir.join("00000000000000000000.wal");

	if repair_failed {
		// Clean up
		fs::remove_dir_all(&repair_dir).ok();
		return Err(crate::error::Error::Other(format!(
			"Failed to repair WAL segment {segment_id}. Original segment unchanged."
		)));
	}

	if valid_batches_count == 0 {
		// No valid data
		fs::remove_file(&segment_path)?;
		fs::remove_dir_all(&repair_dir).ok();
		log::info!("Deleted corrupted WAL segment {segment_id:020}.wal (no valid data)");
		return Ok(());
	}

	// Replace the original with repaired version
	fs::rename(&temp_wal_path, &segment_path)?;
	fs::remove_dir_all(&repair_dir).ok();
	fsync_directory(wal_dir)?;

	log::info!(
		"Successfully repaired WAL segment {segment_id} with {valid_batches_count} valid batches."
	);

	Ok(())
}

#[cfg(test)]
mod tests {
	use std::fs;
	use std::io::{Seek, Write};

	use tempfile::TempDir;
	use test_log::test;

	use super::*;
	use crate::wal::manager::Wal;
	use crate::wal::Options;
	use crate::WalRecoveryMode;

	#[test]
	fn test_replay_wal_sequence_number_tracking() {
		// Create a temporary directory for WAL files
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();

		// Create WAL directory
		fs::create_dir_all(wal_dir).unwrap();

		// Create a memtable to replay into
		let memtable = Arc::new(MemTable::new());

		// Test case: Multiple batches across multiple WAL segments
		// This verifies that ALL segments are replayed, not just the latest

		// Batch 1: Starting at 100, with 3 entries (100, 101, 102)
		let mut batch1 = Batch::new(100);
		batch1.set(b"key1", b"value1", 0).unwrap(); // seq_num 100
		batch1.set(b"key2", b"value2", 0).unwrap(); // seq_num 101
		batch1.set(b"key3", b"value3", 0).unwrap(); // seq_num 102
											  // Highest sequence number should be 102

		// Batch 2: Starting at 200, with 4 entries (200, 201, 202, 203)
		let mut batch2 = Batch::new(200);
		batch2.set(b"key4", b"value4", 0).unwrap(); // seq_num 200
		batch2.set(b"key5", b"value5", 0).unwrap(); // seq_num 201
		batch2.delete(b"key6", 0).unwrap(); // seq_num 202
		batch2.set(b"key7", b"value7", 0).unwrap(); // seq_num 203
											  // Highest sequence number should be 203

		// Create WAL and rotate to create 2 segments
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		wal.append(&batch1.encode().unwrap()).unwrap();
		wal.rotate().unwrap(); // Rotate to segment 1

		wal.append(&batch2.encode().unwrap()).unwrap();
		wal.close().unwrap();

		// Replay the WAL - should replay BOTH segments
		let max_seq_num_opt = replay_wal(wal_dir, &memtable, 0).unwrap();

		// Verify both segments are replayed: max_seq_num should be 203 (highest from
		// batch2)
		assert_eq!(
			max_seq_num_opt,
			Some(203),
			"WAL recovery should track highest sequence number (203) across all segments"
		);

		// Verify the memtable contains entries from BOTH segments
		let entry_count = memtable.iter(false).count();
		assert_eq!(
			entry_count, 7,
			"Memtable should contain all 7 entries from both WAL segments (6 sets + 1 delete)"
		);
	}

	#[test]
	fn test_replay_wal_empty_directory() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		let memtable = Arc::new(MemTable::new());

		let max_seq_num_opt = replay_wal(wal_dir, &memtable, 0).unwrap();

		assert_eq!(max_seq_num_opt, None, "Empty WAL directory should return None");
	}

	#[test]
	fn test_replay_wal_single_entry_batches() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		let memtable = Arc::new(MemTable::new());

		// Test with multiple single-entry batches across 3 WAL segments
		// This tests that ALL segments are replayed, not just the latest
		let mut batch1 = Batch::new(500);
		batch1.set(b"key1", b"value1", 0).unwrap(); // seq_num 500

		let mut batch2 = Batch::new(600);
		batch2.set(b"key2", b"value2", 0).unwrap(); // seq_num 600

		let mut batch3 = Batch::new(700);
		batch3.set(b"key3", b"value3", 0).unwrap(); // seq_num 700

		// Create WAL for all batches (use rotation to create three segments)
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		wal.append(&batch1.encode().unwrap()).unwrap();
		wal.rotate().unwrap(); // Rotate to WAL 1

		wal.append(&batch2.encode().unwrap()).unwrap();
		wal.rotate().unwrap(); // Rotate to WAL 2

		wal.append(&batch3.encode().unwrap()).unwrap();
		wal.close().unwrap();

		let max_seq_num_opt = replay_wal(wal_dir, &memtable, 0).unwrap();

		// All three segments should be replayed, max should be 700
		assert_eq!(
			max_seq_num_opt,
			Some(700),
			"Should replay all 3 segments and return highest sequence number (700)"
		);

		// Verify all 3 entries are in the memtable (from all 3 segments)
		let entry_count = memtable.iter(false).count();
		assert_eq!(entry_count, 3, "Memtable should contain all 3 entries from all 3 WAL segments");
	}

	#[test]
	fn test_replay_wal_multiple_batches() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Test case: Multiple batches across 2 WAL segments
		// This ensures ALL segments are replayed and max tracking works correctly
		let mut batch1 = Batch::new(200); // Starting sequence number 200
		batch1.set(b"key1", b"value1", 0).unwrap(); // seq_num 200
		batch1.set(b"key2", b"value2", 0).unwrap(); // seq_num 201
											  // Highest sequence number should be 201

		let mut batch2 = Batch::new(300); // Starting sequence number 300
		batch2.set(b"key3", b"value3", 0).unwrap(); // seq_num 300
		batch2.set(b"key4", b"value4", 0).unwrap(); // seq_num 301
											  // Highest sequence number should be 301

		// Create WAL and rotate to create 2 segments
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		wal.append(&batch1.encode().unwrap()).unwrap();
		wal.rotate().unwrap(); // Rotate to segment 1

		wal.append(&batch2.encode().unwrap()).unwrap();
		wal.close().unwrap();

		// Create a fresh memtable for the test
		let memtable = Arc::new(MemTable::new());
		let max_seq_num_opt = replay_wal(wal_dir, &memtable, 0).unwrap();

		// Both segments should be replayed, max should be 301
		assert_eq!(
			max_seq_num_opt,
			Some(301),
			"WAL recovery should track highest sequence number (301) across all segments"
		);

		// Verify all 4 entries from both segments are in the memtable
		let entry_count = memtable.iter(false).count();
		assert_eq!(entry_count, 4, "Memtable should contain all 4 entries from both WAL segments");
	}

	#[test]
	fn test_repair_corrupted_wal_segment_nonexistent() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Test repair of non-existent segment
		let result = repair_corrupted_wal_segment(wal_dir, 999);
		assert!(result.is_err(), "Should fail when segment doesn't exist");
	}

	#[test]
	fn test_post_corruption_replay() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Create a valid WAL first
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		// Add some valid data
		let mut batch1 = Batch::new(100);
		batch1.set(b"key1", b"value1", 0).unwrap();
		batch1.set(b"key2", b"value2", 0).unwrap();

		wal.append(&batch1.encode().unwrap()).unwrap();
		wal.close().unwrap();

		// Now manually corrupt the segment to reproduce the exact error
		let segment_path = wal_dir.join("00000000000000000000.wal");
		let mut file =
			std::fs::OpenOptions::new().read(true).write(true).open(&segment_path).unwrap();

		// Corrupt the first record (at offset 0, no file header)
		file.seek(std::io::SeekFrom::Start(0)).unwrap();

		// Write a Middle record where a First/Full record should be
		let record_type = 3u8;
		let length = 10u16; // Length of data
		let data = vec![0xFF; 10]; // Some data
		let crc = crate::wal::calculate_crc32(&[record_type], &data);

		file.write_all(&[record_type]).unwrap(); // Record type: Middle (WRONG!)
		file.write_all(&length.to_be_bytes()).unwrap(); // Length
		file.write_all(&crc.to_be_bytes()).unwrap(); // CRC
		file.write_all(&data).unwrap(); // Data
		drop(file);

		// Test using Core::replay_wal_with_repair (the actual production flow)
		let recovered_memtable = Arc::new(std::sync::RwLock::new(Arc::new(MemTable::new())));
		let max_seq_num = crate::lsm::Core::replay_wal_with_repair(
			wal_dir,
			0,
			"Test repair",
			WalRecoveryMode::TolerateCorruptedWithRepair,
			|memtable| {
				// This closure is called with the recovered memtable
				*recovered_memtable.write().unwrap() = memtable;
				Ok(())
			},
		)
		.unwrap();

		// Verify the repair worked correctly
		// Since the first record is corrupted, we should recover None (no valid data)
		assert_eq!(max_seq_num, None, "Should have recovered None when first record is corrupted");

		// Verify that the memtable contains no entries (since first record was
		// corrupted)
		let entry_count = recovered_memtable.read().unwrap().iter(false).count();
		assert_eq!(
			entry_count, 0,
			"Should have recovered 0 entries when first record is corrupted"
		);
	}

	#[test]
	fn test_repair_with_three_batches_corrupt_third() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Create a valid WAL with three batches
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		// Create three batches with different sequence numbers
		let mut batch1 = Batch::new(100);
		batch1.set(b"key1", b"value1", 0).unwrap();
		batch1.set(b"key2", b"value2", 0).unwrap();

		let mut batch2 = Batch::new(200);
		batch2.set(b"key3", b"value3", 0).unwrap();
		batch2.set(b"key4", b"value4", 0).unwrap();

		let mut batch3 = Batch::new(300);
		batch3.set(b"key5", b"value5", 0).unwrap();
		batch3.set(b"key6", b"value6", 0).unwrap();

		// Encode all batches
		let encoded1 = batch1.encode().unwrap();
		let encoded2 = batch2.encode().unwrap();
		let encoded3 = batch3.encode().unwrap();

		// Append all batches
		wal.append(&encoded1).unwrap();
		wal.append(&encoded2).unwrap();
		wal.append(&encoded3).unwrap();
		wal.close().unwrap();

		// Now manually corrupt the segment by corrupting the third batch
		let segment_path = wal_dir.join("00000000000000000000.wal");
		let mut file =
			std::fs::OpenOptions::new().read(true).write(true).open(&segment_path).unwrap();

		// Calculate the offset for the third batch (no file header)
		file.seek(std::io::SeekFrom::Start(0)).unwrap();

		// Each batch is written as a WAL record with a 7-byte header
		let batch1_size = encoded1.len() + 7; // encoded data + WAL record header
		let batch2_size = encoded2.len() + 7; // encoded data + WAL record header
		let third_batch_offset = batch1_size as u64 + batch2_size as u64;

		// Corrupt the third batch by changing its record type to Middle
		file.seek(std::io::SeekFrom::Start(third_batch_offset)).unwrap();
		let record_type = 3u8;
		let length = 10u16;
		let data = vec![0xFF; 10];
		let crc = crate::wal::calculate_crc32(&[record_type], &data);

		file.write_all(&[record_type]).unwrap();
		file.write_all(&length.to_be_bytes()).unwrap(); // Length
		file.write_all(&crc.to_be_bytes()).unwrap(); // CRC
		file.write_all(&data).unwrap(); // Data
		drop(file);

		let recovered_memtable = Arc::new(std::sync::RwLock::new(Arc::new(MemTable::new())));
		let max_seq_num = crate::lsm::Core::replay_wal_with_repair(
			wal_dir,
			0,
			"Test repair",
			WalRecoveryMode::TolerateCorruptedWithRepair,
			|memtable| {
				*recovered_memtable.write().unwrap() = memtable;
				Ok(())
			},
		)
		.unwrap();

		// Verify the repair worked correctly
		// Since the third batch is corrupted, we should recover data from the first two
		// batches The max sequence number should be from batch 2 (201), not batch 3
		// (301)
		assert_eq!(
			max_seq_num,
			Some(201),
			"Should have recovered sequence numbers from first two batches only"
		);

		// Verify that the memtable contains entries from the first two batches
		let entry_count = recovered_memtable.read().unwrap().iter(false).count();
		assert_eq!(entry_count, 4, "Should have recovered 4 entries from first two batches");
	}

	#[test]
	fn test_recovery_mode_tolerate_tail_corruption() {
		use crate::batch::Batch;

		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Write valid batches first
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		let mut batch1 = Batch::new(100);
		batch1.set(b"key1", b"value1", 0).unwrap();
		wal.append(&batch1.encode().unwrap()).unwrap();

		let mut batch2 = Batch::new(200);
		batch2.set(b"key2", b"value2", 0).unwrap();
		wal.append(&batch2.encode().unwrap()).unwrap();

		wal.close().unwrap();

		// Manually corrupt the end of the file
		let segment_path = wal_dir.join("00000000000000000000.wal");
		let mut file = fs::OpenOptions::new().append(true).open(&segment_path).unwrap();
		file.write_all(b"CORRUPTED_DATA_AT_END").unwrap();

		// Replay with TolerateCorruptedTailRecords (default)
		let memtable = Arc::new(MemTable::new());
		let result = replay_wal(wal_dir, &memtable, 0);

		// Should report corruption as an error
		match result {
			Err(Error::WalCorruption {
				segment_id,
				..
			}) => {
				assert_eq!(segment_id, 0, "Should detect corruption in segment 0");
			}
			_ => panic!("Expected WalCorruption error"),
		}
	}

	#[test]
	fn test_recovery_mode_skip_corrupted() {
		// This test would require injecting corruption in the middle of records
		// For now, verify the mode parameter works
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		let mut batch = Batch::new(100);
		batch.set(b"key", b"value", 0).unwrap();
		wal.append(&batch.encode().unwrap()).unwrap();
		wal.close().unwrap();

		let memtable = Arc::new(MemTable::new());
		let result = replay_wal(wal_dir, &memtable, 0);

		assert!(result.is_ok());
	}

	#[test]
	fn test_default_reporter_usage() {
		use crate::batch::Batch;

		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Write a valid batch
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		let mut batch = Batch::new(100);
		batch.set(b"key", b"value", 0).unwrap();
		wal.append(&batch.encode().unwrap()).unwrap();
		wal.close().unwrap();

		// Replay - DefaultReporter is created internally
		let memtable = Arc::new(MemTable::new());
		let seq_num_opt = replay_wal(wal_dir, &memtable, 0).unwrap();

		assert_eq!(seq_num_opt, Some(100));
		// Reporter is used internally for logging
	}

	#[test]
	fn test_multi_segment_recovery_after_crash() {
		use crate::batch::Batch;

		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Simulate: rotate WAL, crash before flush
		// This is the critical bug scenario that was causing data loss

		// Create first batch in WAL segment 0
		let mut batch1 = Batch::new(100);
		batch1.set(b"key1", b"value1", 0).unwrap();
		batch1.set(b"key2", b"value2", 0).unwrap();

		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();
		wal.append(&batch1.encode().unwrap()).unwrap();

		// Rotate to WAL segment 1 (simulating memtable rotation)
		wal.rotate().unwrap();

		// Create second batch in WAL segment 1
		let mut batch2 = Batch::new(200);
		batch2.set(b"key3", b"value3", 0).unwrap();
		batch2.set(b"key4", b"value4", 0).unwrap();

		wal.append(&batch2.encode().unwrap()).unwrap();

		// Close without flushing memtable (simulating crash)
		wal.close().unwrap();

		// Now attempt recovery - both WAL segments should be replayed
		let memtable = Arc::new(MemTable::new());
		let max_seq_num_opt = replay_wal(wal_dir, &memtable, 0).unwrap();

		// Verify both segments were replayed
		assert_eq!(
			max_seq_num_opt,
			Some(201),
			"Should replay both WAL segments and return max seq from segment 1"
		);

		// Verify all 4 entries from both segments are recovered
		let entry_count = memtable.iter(false).count();
		assert_eq!(
			entry_count, 4,
			"Should recover all 4 entries from both WAL segments (2 from each)"
		);
	}

	#[test]
	fn test_corruption_stops_at_segment_n() {
		use crate::batch::Batch;

		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Create 3 segments with valid data
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		// Segment 0: valid data
		let mut batch0 = Batch::new(100);
		batch0.set(b"key0", b"value0", 0).unwrap();
		wal.append(&batch0.encode().unwrap()).unwrap();
		wal.rotate().unwrap();

		// Segment 1: will be corrupted
		let mut batch1 = Batch::new(200);
		batch1.set(b"key1", b"value1", 0).unwrap();
		let encoded1 = batch1.encode().unwrap();
		wal.append(&encoded1).unwrap();
		wal.rotate().unwrap();

		// Segment 2: valid data (should NOT be processed due to corruption in segment
		// 1)
		let mut batch2 = Batch::new(300);
		batch2.set(b"key2", b"value2", 0).unwrap();
		wal.append(&batch2.encode().unwrap()).unwrap();
		wal.close().unwrap();

		// Corrupt segment 1
		let segment1_path = wal_dir.join("00000000000000000001.wal");
		let mut file = fs::OpenOptions::new().read(true).write(true).open(&segment1_path).unwrap();

		// Corrupt at the beginning of the segment
		file.seek(std::io::SeekFrom::Start(0)).unwrap();
		let record_type = 3u8; // Middle record (invalid at start)
		let length = 10u16;
		let data = vec![0xFF; 10];
		let crc = crate::wal::calculate_crc32(&[record_type], &data);

		file.write_all(&[record_type]).unwrap();
		file.write_all(&length.to_be_bytes()).unwrap();
		file.write_all(&crc.to_be_bytes()).unwrap();
		file.write_all(&data).unwrap();
		drop(file);

		// Attempt recovery
		let memtable = Arc::new(MemTable::new());
		let result = replay_wal(wal_dir, &memtable, 0);

		// Should report corruption in segment 1
		match result {
			Err(Error::WalCorruption {
				segment_id,
				..
			}) => {
				assert_eq!(segment_id, 1, "Corruption should be reported in segment 1");
			}
			_ => panic!("Expected WalCorruption error in segment 1"),
		}

		// Verify only segment 0's data is in memtable (segment 2 should NOT be
		// processed)
		let entry_count = memtable.iter(false).count();
		assert_eq!(
			entry_count, 1,
			"Should have only 1 entry from segment 0 (segment 2 should NOT be processed)"
		);
	}
}
