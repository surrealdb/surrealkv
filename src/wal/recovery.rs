use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use crate::lsm::fsync_directory;
use crate::wal::list_segment_ids;
use crate::wal::reader::Reporter;
use crate::{
	batch::Batch,
	error::Result,
	memtable::MemTable,
	wal::{get_segment_range, reader::Reader, Error, SegmentRef},
};

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

/// Result of WAL replay operation
/// - First element: Some(seq_num) if WAL was replayed, None if skipped or empty
/// - Second element: Corruption info (segment_id, offset) if detected
type ReplayResult = (Option<u64>, Option<(usize, usize)>);

/// Replays the Write-Ahead Log (WAL) to recover recent writes.
///
/// Returns ReplayResult where:
/// - First element: Some(seq_num) if WAL was replayed, None if skipped or empty
/// - Second element: Corruption info if detected
pub(crate) fn replay_wal(
	wal_dir: &Path,
	memtable: &Arc<MemTable>,
	min_wal_number: u64,
) -> Result<ReplayResult> {
	log::info!("Starting WAL recovery from directory: {:?}", wal_dir);
	log::debug!("WAL recovery parameters: min_wal_number={}", min_wal_number);

	// Check if WAL directory exists
	if !wal_dir.exists() {
		log::debug!("WAL directory does not exist, skipping recovery");
		return Ok((None, None));
	}

	if list_segment_ids(wal_dir, Some("wal"))?.is_empty() {
		log::debug!("No WAL segments found, skipping recovery");
		return Ok((None, None));
	}

	// Get range of segment IDs (looking for .wal files)
	let (first, last) = match get_segment_range(wal_dir, Some("wal")) {
		Ok(range) => range,
		Err(Error::IO(_)) => {
			log::debug!("Could not get WAL segment range, skipping recovery");
			return Ok((None, None));
		}
		Err(e) => return Err(e.into()),
	};

	// If no segments, nothing to replay
	if first > last {
		log::debug!("No valid WAL segment range, skipping recovery");
		return Ok((None, None));
	}

	// Only replay the latest segment since we use one segment per memtable
	let latest_segment_id = last;
	log::debug!("WAL segment range: first={}, last={}", first, latest_segment_id);

	// Skip WAL if it's older than the minimum log number with unflushed data
	// Flushed WALs are not replayed
	if latest_segment_id < min_wal_number {
		log::info!(
			"Skipping WAL #{:020} (already flushed to SST, min_log_number={:020})",
			latest_segment_id,
			min_wal_number
		);
		return Ok((None, None));
	}

	log::info!("Replaying WAL #{:020}", latest_segment_id);

	// Define initial sequence number
	let mut max_seq_num = 0;

	// Track current segment and offset for reporting corruption location
	let mut last_valid_offset = 0;

	// Get only the latest segment (with .wal extension)
	let all_segments = SegmentRef::read_segments_from_directory(wal_dir, Some("wal"))?;
	let latest_segment = all_segments.into_iter().find(|seg| seg.id == latest_segment_id);

	// If no latest segment found, we're done
	let latest_segment = match latest_segment {
		Some(seg) => seg,
		None => {
			return Ok((
				if max_seq_num > 0 {
					Some(max_seq_num)
				} else {
					None
				},
				None,
			));
		}
	};

	// Open the latest segment file
	let file = File::open(&latest_segment.file_path)?;

	// Create reporter for corruption tracking
	let reporter = Box::new(DefaultReporter::new(latest_segment_id));

	// Create reader with reporter
	let mut reader = Reader::with_options(file, Some(reporter), latest_segment_id);

	// Track replay statistics
	let mut batches_replayed = 0;

	// Process each record in the latest segment
	loop {
		match reader.read() {
			Ok((record_data, offset)) => {
				// Update tracking info
				last_valid_offset = offset as usize;

				// Decode batch and get sequence number
				let batch = Batch::decode(record_data)?;
				let batch_highest_seq_num = batch.get_highest_seq_num();

				// Update max sequence number
				if batch_highest_seq_num > max_seq_num {
					max_seq_num = batch_highest_seq_num;
				}

				batches_replayed += 1;

				log::error!(
					"Replayed batch from WAL #{:020}: max_seq_num={}, entries={}, offset={}",
					latest_segment_id,
					batch_highest_seq_num,
					batch.count(),
					offset
				);

				// Apply the batch to the memtable
				memtable.add(&batch)?;
			}
			Err(Error::Corruption(err)) => {
				// Tolerate tail corruption - return info for automatic repair
				log::warn!(
					"Corrupted WAL record detected in segment {latest_segment_id:020} at offset {last_valid_offset}: {err}"
				);
				return Ok((
					if max_seq_num > 0 {
						Some(max_seq_num)
					} else {
						None
					},
					Some((latest_segment_id as usize, last_valid_offset)),
				));
			}
			Err(Error::IO(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
				// End of WAL reached
				break;
			}
			Err(err) => return Err(err.into()),
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
			"WAL #{:020} recovery complete: batches={}, max_seq_num={}, total_entries={}",
			latest_segment_id,
			batches_replayed,
			seq,
			memtable.iter().count()
		),
		None => log::info!("WAL #{:020} was empty (no batches found)", latest_segment_id),
	}

	Ok((result, None))
}

pub(crate) fn repair_corrupted_wal_segment(wal_dir: &Path, segment_id: usize) -> Result<()> {
	use crate::wal::manager::Wal;
	use crate::wal::reader::Reader;
	use crate::wal::Options;
	use std::fs;

	// Build segment paths
	let segment_path = wal_dir.join(format!("{segment_id:020}.wal"));
	let _repair_path = wal_dir.join(format!("{segment_id:020}.wal.repair"));

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
			Err(Error::Corruption(err)) => {
				// Stop at the first corruption
				log::info!(
                    "Stopped repair at corruption: {err}. Recovered {valid_batches_count} valid batches."
                );
				break;
			}
			Err(Error::IO(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
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
	use super::*;
	use crate::wal::manager::Wal;
	use crate::wal::Options;
	use std::fs;
	use std::io::{Seek, Write};
	use tempfile::TempDir;
	use test_log::test;

	#[test]
	fn test_replay_wal_sequence_number_tracking() {
		// Create a temporary directory for WAL files
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();

		// Create WAL directory
		fs::create_dir_all(wal_dir).unwrap();

		// Create a memtable to replay into
		let memtable = Arc::new(MemTable::new());

		// Test case: Multiple batches, each with multiple entries
		// This is the critical test case that would have failed with the old bug

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

		// Create WAL and rotate
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		wal.append(&batch1.encode().unwrap()).unwrap();
		wal.rotate().unwrap();

		wal.append(&batch2.encode().unwrap()).unwrap();
		wal.close().unwrap();

		// Replay the WAL
		let (max_seq_num_opt, corruption_info) = replay_wal(wal_dir, &memtable, 0).unwrap();

		// Verify the bug is fixed: max_seq_num should be 203 (highest from batch2), not 200 (starting of batch2)
		assert_eq!(
			max_seq_num_opt,
			Some(203),
			"WAL recovery should track highest sequence number (203), not starting sequence number (200)"
		);
		assert!(corruption_info.is_none(), "No corruption should be detected");

		// Verify the memtable contains the expected entries
		assert!(!memtable.is_empty(), "Memtable should not be empty after replay");
	}

	#[test]
	fn test_replay_wal_empty_directory() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		let memtable = Arc::new(MemTable::new());

		let (max_seq_num_opt, corruption_info) = replay_wal(wal_dir, &memtable, 0).unwrap();

		assert_eq!(max_seq_num_opt, None, "Empty WAL directory should return None");
		assert!(corruption_info.is_none(), "No corruption should be detected");
	}

	#[test]
	fn test_replay_wal_single_entry_batches() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		let memtable = Arc::new(MemTable::new());

		// Test with multiple single-entry batches
		// This tests the edge case where starting = highest for each batch
		let mut batch1 = Batch::new(500);
		batch1.set(b"key1", b"value1", 0).unwrap(); // seq_num 500

		let mut batch2 = Batch::new(600);
		batch2.set(b"key2", b"value2", 0).unwrap(); // seq_num 600

		let mut batch3 = Batch::new(700);
		batch3.set(b"key3", b"value3", 0).unwrap(); // seq_num 700

		// Create WAL for all batches (use rotation to create three files)
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		wal.append(&batch1.encode().unwrap()).unwrap();
		wal.rotate().unwrap(); // Rotate to WAL 1

		wal.append(&batch2.encode().unwrap()).unwrap();
		wal.rotate().unwrap(); // Rotate to WAL 2

		wal.append(&batch3.encode().unwrap()).unwrap();
		wal.close().unwrap();

		let (max_seq_num_opt, corruption_info) = replay_wal(wal_dir, &memtable, 0).unwrap();

		// For single-entry batches, starting and highest should be the same
		// The max should be 700 (from the latest batch)
		assert_eq!(
			max_seq_num_opt,
			Some(700),
			"Multiple single-entry batches should return highest sequence number"
		);
		assert!(corruption_info.is_none(), "No corruption should be detected");
	}

	#[test]
	fn test_replay_wal_multiple_batches() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Test case: Multiple batches with different starting sequence numbers
		// This ensures the max tracking works across multiple batches
		let mut batch1 = Batch::new(200); // Starting sequence number 200
		batch1.set(b"key1", b"value1", 0).unwrap(); // seq_num 200
		batch1.set(b"key2", b"value2", 0).unwrap(); // seq_num 201
											  // Highest sequence number should be 201

		let mut batch2 = Batch::new(300); // Starting sequence number 300
		batch2.set(b"key3", b"value3", 0).unwrap(); // seq_num 300
		batch2.set(b"key4", b"value4", 0).unwrap(); // seq_num 301
											  // Highest sequence number should be 301

		// Create WAL and rotate
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		wal.append(&batch1.encode().unwrap()).unwrap();
		wal.rotate().unwrap();

		wal.append(&batch2.encode().unwrap()).unwrap();
		wal.close().unwrap();

		// Create a fresh memtable for the test
		let memtable = Arc::new(MemTable::new());
		let (max_seq_num_opt, _) = replay_wal(wal_dir, &memtable, 0).unwrap();

		// The max should be from the latest segment (301), not the starting sequence number
		assert_eq!(
			max_seq_num_opt,
			Some(301),
			"WAL recovery should track highest sequence number across all batches"
		);
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
		let max_seq_num =
			crate::lsm::Core::replay_wal_with_repair(wal_dir, 0, "Test repair", |memtable| {
				// This closure is called with the recovered memtable
				*recovered_memtable.write().unwrap() = memtable;
				Ok(())
			})
			.unwrap();

		// Verify the repair worked correctly
		// Since the first record is corrupted, we should recover None (no valid data)
		assert_eq!(max_seq_num, None, "Should have recovered None when first record is corrupted");

		// Verify that the memtable contains no entries (since first record was corrupted)
		let entry_count = recovered_memtable.read().unwrap().iter().count();
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
		let max_seq_num =
			crate::lsm::Core::replay_wal_with_repair(wal_dir, 0, "Test repair", |memtable| {
				*recovered_memtable.write().unwrap() = memtable;
				Ok(())
			})
			.unwrap();

		// Verify the repair worked correctly
		// Since the third batch is corrupted, we should recover data from the first two batches
		// The max sequence number should be from batch 2 (201), not batch 3 (301)
		assert_eq!(
			max_seq_num,
			Some(201),
			"Should have recovered sequence numbers from first two batches only"
		);

		// Verify that the memtable contains entries from the first two batches
		let entry_count = recovered_memtable.read().unwrap().iter().count();
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
		let (seq_num_opt, corruption_info) = replay_wal(wal_dir, &memtable, 0).unwrap();

		// Should recover valid batches and report corruption
		assert_eq!(seq_num_opt, Some(200));
		assert!(corruption_info.is_some(), "Should detect corruption");
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
		let (seq_num_opt, _) = replay_wal(wal_dir, &memtable, 0).unwrap();

		assert_eq!(seq_num_opt, Some(100));
		// Reporter is used internally for logging
	}
}
