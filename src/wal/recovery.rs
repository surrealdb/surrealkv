use std::path::Path;
use std::sync::Arc;

use crate::wal::segment::list_segment_ids;
use crate::{
	batch::Batch,
	error::Result,
	memtable::MemTable,
	wal::{
		reader::Reader,
		segment::{get_segment_range, Error, MultiSegmentReader, SegmentRef},
	},
};

/// Replays the Write-Ahead Log (WAL) to recover recent writes.
///
/// This function reads only the latest WAL segment to recover recent writes,
/// since we use one segment per memtable and only the latest segment contains
/// unflushed data.
///
/// # Parameters
/// * `wal_dir` - The directory containing WAL segments
/// * `memtable` - The memtable to apply recovered operations to
///
/// # Returns
/// * `Result<(u64, Option<(usize, usize)>)>` - Tuple of (max_seq_num, optional tuple of (segment_id, last_valid_offset))
///
/// If no corruption is found, the second element will be None.
/// If corruption is found, the second element contains (segment_id, last_valid_offset) for repair.
pub(crate) fn replay_wal(
	wal_dir: &Path,
	memtable: &Arc<MemTable>,
) -> Result<(u64, Option<(usize, usize)>)> {
	// Check if WAL directory exists
	if !wal_dir.exists() {
		return Ok((0, None));
	}

	if list_segment_ids(wal_dir)?.is_empty() {
		return Ok((0, None));
	}

	// Get range of segment IDs
	let (first, last) = match get_segment_range(wal_dir) {
		Ok(range) => range,
		Err(Error::IO(_)) => return Ok((0, None)),
		Err(e) => return Err(e.into()),
	};

	// If no segments, nothing to replay
	if first > last {
		return Ok((0, None));
	}

	// Only replay the latest segment since we use one segment per memtable
	let latest_segment_id = last;

	// Define initial sequence number
	let mut max_seq_num = 0;

	// Track current segment and offset for reporting corruption location
	let mut last_valid_offset = 0;

	// Get only the latest segment
	let all_segments = SegmentRef::read_segments_from_directory(wal_dir)?;
	let latest_segments =
		all_segments.into_iter().filter(|seg| seg.id == latest_segment_id).collect::<Vec<_>>();

	// If no latest segment found, we're done
	if latest_segments.is_empty() {
		return Ok((max_seq_num, None));
	}

	// Create MultiSegmentReader with only the latest segment
	let segments = MultiSegmentReader::new(latest_segments)?;
	let mut reader = Reader::new(segments);

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

				// Apply the batch to the memtable
				memtable.add(&batch)?;
			}
			Err(Error::Corruption(err)) => {
				// Return the corruption information with the segment ID and last valid offset
				eprintln!(
                    "Corrupted WAL record detected in segment {latest_segment_id:020} at offset {last_valid_offset}: {err}"
                );
				return Ok((max_seq_num, Some((latest_segment_id as usize, last_valid_offset))));
			}
			Err(Error::IO(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
				// End of WAL reached
				break;
			}
			Err(err) => return Err(err.into()),
		}
	}

	// No corruption found
	Ok((max_seq_num, None))
}

pub(crate) fn repair_corrupted_wal_segment(wal_dir: &Path, segment_id: usize) -> Result<()> {
	use crate::wal::reader::Reader;
	use crate::wal::segment::{Options, Segment};
	use std::fs;

	// Build segment paths
	let segment_path = wal_dir.join(format!("{segment_id:020}"));
	let temp_path = wal_dir.join(format!("{segment_id:020}"));

	// Verify the corrupted segment exists
	if !segment_path.exists() {
		return Err(crate::error::Error::Other(format!(
			"WAL segment {segment_id:020} does not exist"
		)));
	}

	// Create a new segment for writing the repaired data
	let opts = Options {
		file_extension: Some("repair".to_string()),
		..Options::default()
	};
	let mut new_segment = Segment::open(wal_dir, segment_id as u64, &opts)?;

	// Create a SegmentRef directly pointing to the original file
	let original_segment = crate::wal::segment::SegmentRef {
		id: segment_id as u64,
		file_path: segment_path.clone(),
		file_header_offset: {
			// Read the header to get the correct offset
			let mut file = fs::File::open(&segment_path)?;
			let header = crate::wal::segment::read_file_header(&mut file)?;
			(4 + header.len()) as u64
		},
	};

	let segments = MultiSegmentReader::new(vec![original_segment])?;
	let mut reader = Reader::new(segments);

	let mut valid_batches_count = 0;
	let mut repair_failed = false;

	// Read valid batches from original and write them to the temp segment
	loop {
		match reader.read() {
			Ok((record_data, _offset)) => {
				// We have a valid batch, write it to the new segment
				if let Err(e) = new_segment.append(record_data) {
					eprintln!("Failed to write valid batch to repaired segment: {e}");
					repair_failed = true;
					break;
				}
				valid_batches_count += 1;
			}
			Err(Error::Corruption(err)) => {
				// Stop at the first corruption - we've written all valid batches
				eprintln!(
                    "Stopped repair at corruption: {err}. Recovered {valid_batches_count} valid batches."
                );
				break;
			}
			Err(Error::IO(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
				// End of segment reached - all data was valid
				eprintln!(
					"Repair completed successfully. Recovered {valid_batches_count} valid batches."
				);
				break;
			}
			Err(err) => {
				eprintln!("Unexpected error during repair: {err}");
				repair_failed = true;
				break;
			}
		}
	}

	// Close the new segment to ensure all data is flushed
	new_segment.close()?;

	if repair_failed {
		// Clean up the failed repair attempt
		let _ = fs::remove_file(&temp_path);

		return Err(crate::error::Error::Other(format!(
			"Failed to repair WAL segment {segment_id}. Original segment unchanged."
		)));
	}

	// Atomically replace the original with the repaired version
	fs::rename(&temp_path, &segment_path)?;

	eprintln!(
		"Successfully repaired WAL segment {segment_id} with {valid_batches_count} valid batches."
	);

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::wal::segment::{Options, Segment};
	use std::fs;
	use tempfile::TempDir;

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

		// Create WAL segments for both batches
		let opts = Options::default();
		let mut segment1 = Segment::open(wal_dir, 1, &opts).unwrap();
		let mut segment2 = Segment::open(wal_dir, 2, &opts).unwrap();

		segment1.append(&batch1.encode().unwrap()).unwrap();
		segment2.append(&batch2.encode().unwrap()).unwrap();

		segment1.close().unwrap();
		segment2.close().unwrap();

		// Replay the WAL
		let (max_seq_num, corruption_info) = replay_wal(wal_dir, &memtable).unwrap();

		// Verify the bug is fixed: max_seq_num should be 203 (highest from batch2), not 200 (starting of batch2)
		assert_eq!(
			max_seq_num, 203,
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

		let (max_seq_num, corruption_info) = replay_wal(wal_dir, &memtable).unwrap();

		assert_eq!(max_seq_num, 0, "Empty WAL directory should return 0");
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

		// Create WAL segments for all batches
		let opts = Options::default();
		let mut segment1 = Segment::open(wal_dir, 1, &opts).unwrap();
		let mut segment2 = Segment::open(wal_dir, 2, &opts).unwrap();
		let mut segment3 = Segment::open(wal_dir, 3, &opts).unwrap();

		segment1.append(&batch1.encode().unwrap()).unwrap();
		segment2.append(&batch2.encode().unwrap()).unwrap();
		segment3.append(&batch3.encode().unwrap()).unwrap();

		segment1.close().unwrap();
		segment2.close().unwrap();
		segment3.close().unwrap();

		let (max_seq_num, corruption_info) = replay_wal(wal_dir, &memtable).unwrap();

		// For single-entry batches, starting and highest should be the same
		// The max should be 700 (from the latest batch)
		assert_eq!(
			max_seq_num, 700,
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

		// Create WAL segments for both batches
		let opts = Options::default();
		let mut segment1 = Segment::open(wal_dir, 1, &opts).unwrap();
		let mut segment2 = Segment::open(wal_dir, 2, &opts).unwrap();

		segment1.append(&batch1.encode().unwrap()).unwrap();
		segment2.append(&batch2.encode().unwrap()).unwrap();

		segment1.close().unwrap();
		segment2.close().unwrap();

		// Create a fresh memtable for the test
		let memtable = Arc::new(MemTable::new());
		let (max_seq_num, _) = replay_wal(wal_dir, &memtable).unwrap();

		// The max should be from the latest segment (301), not the starting sequence number
		assert_eq!(
			max_seq_num, 301,
			"WAL recovery should track highest sequence number across all batches"
		);
	}

	#[test]
	fn test_repair_corrupted_wal_segment() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Create a valid WAL segment first
		let opts = Options::default();
		let mut segment = Segment::open(wal_dir, 0, &opts).unwrap();

		// Add some valid data
		let mut batch1 = Batch::new(100);
		batch1.set(b"key1", b"value1", 0).unwrap();
		batch1.set(b"key2", b"value2", 0).unwrap();

		let mut batch2 = Batch::new(200);
		batch2.set(b"key3", b"value3", 0).unwrap();
		batch2.set(b"key4", b"value4", 0).unwrap();

		segment.append(&batch1.encode().unwrap()).unwrap();
		segment.append(&batch2.encode().unwrap()).unwrap();
		segment.close().unwrap();

		// Now manually corrupt the segment by truncating it in the middle
		let segment_path = wal_dir.join("00000000000000000000");
		let file = std::fs::OpenOptions::new().write(true).open(&segment_path).unwrap();

		// Get the file size and truncate it to simulate corruption
		let file_size = file.metadata().unwrap().len();
		file.set_len(file_size - 100).unwrap(); // Truncate by 100 bytes to create corruption
		drop(file);

		// Now test the repair function
		let result = repair_corrupted_wal_segment(wal_dir, 0);

		// The repair should succeed and recover the valid batches
		match result {
			Ok(_) => {
				// Verify the repaired segment exists and is valid
				assert!(segment_path.exists(), "Repaired segment should exist");

				// Try to read from the repaired segment
				let memtable = Arc::new(MemTable::new());
				let (max_seq_num, corruption_info) = replay_wal(wal_dir, &memtable).unwrap();

				// Should have recovered some data and no corruption
				assert!(max_seq_num > 0, "Should have recovered some sequence numbers");
				assert!(corruption_info.is_none(), "Should not detect corruption after repair");
			}
			Err(e) => {
				panic!("Repair failed with error: {:?}", e);
			}
		}
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
}
