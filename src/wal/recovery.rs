use std::path::Path;
use std::sync::Arc;

use crate::batch::{Batch, BatchReader};
use crate::error::Result;
use crate::memtable::MemTable;
use crate::sstable::InternalKeyTrait;
use crate::wal::reader::Reader;
use crate::wal::segment::{
	get_segment_range, list_segment_ids, Error, MultiSegmentReader, SegmentRef,
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
/// * `Result<(u64, Option<(usize, usize)>)>` - Tuple of (max_seq_num, optional
///   tuple of (segment_id, last_valid_offset))
///
/// If no corruption is found, the second element will be None.
/// If corruption is found, the second element contains (segment_id,
/// last_valid_offset) for repair.
pub(crate) fn replay_wal<K: InternalKeyTrait>(
	wal_dir: &Path,
	memtable: &Arc<MemTable<K>>,
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

				// Parse the batch
				let batch_reader = BatchReader::new(record_data)?;
				let seq_num = batch_reader.get_seq_num();

				// Update max sequence number
				if seq_num > max_seq_num {
					max_seq_num = seq_num;
				}

				// Convert BatchReader to Batch
				let mut batch = Batch::new();
				let mut reader = BatchReader::new(record_data)?;
				while let Some((kind, key, value)) = reader.read_record()? {
					batch.add_record(kind, key, value)?;
				}

				// Apply the batch to the memtable
				memtable.add(&batch, seq_num)?;
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
	use crate::wal::segment::{Options, Segment, WAL_RECORD_HEADER_SIZE};
	use std::fs;

	// Build segment paths
	let segment_path = wal_dir.join(format!("{segment_id:020}"));
	let temp_path = wal_dir.join(format!("{segment_id:020}.repair"));

	// Verify the corrupted segment exists
	if !segment_path.exists() {
		return Err(crate::error::Error::Other(format!(
			"WAL segment {segment_id:020} does not exist"
		)));
	}

	// Create a new segment for writing the repaired data
	let opts = Options::default();
	let mut new_segment =
		Segment::<WAL_RECORD_HEADER_SIZE>::open(&temp_path, segment_id as u64, &opts)?;

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
