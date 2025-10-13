use std::fs;
use std::path::Path;

use crate::wal::segment::{get_segment_range, list_segment_ids, Result};

/// Cleans up old WAL segments, keeping only the latest segment.
///
/// This function removes all WAL segments except the latest one, since we use
/// one segment per memtable and only the latest segment contains unflushed data.
/// All older segments have been flushed to SSTables and are no longer needed.
///
/// # Arguments
///
/// * `wal_dir` - The directory containing the WAL segments
///
/// # Returns
///
/// A result with the count of removed segments or an error
pub(crate) fn cleanup_old_segments(wal_dir: &Path) -> Result<usize> {
	// Check if WAL directory exists
	if !wal_dir.exists() {
		return Ok(0);
	}

	// Get range of segment IDs (looking for .wal files)
	let (first, last) = match get_segment_range(wal_dir, Some("wal")) {
		Ok(range) => range,
		Err(_) => return Ok(0), // No segments to clean
	};

	// If no segments or only one segment, nothing to clean
	if first >= last {
		return Ok(0);
	}

	// List all segment IDs with .wal extension
	let segment_ids = list_segment_ids(wal_dir, Some("wal"))?;
	let mut removed_count = 0;

	// Remove all segments except the latest one
	for segment_id in segment_ids {
		if segment_id < last {
			let segment_path = wal_dir.join(format!("{segment_id:020}.wal"));

			match fs::remove_file(&segment_path) {
				Ok(_) => {
					removed_count += 1;
				}
				Err(e) => {
					// Log error but continue trying to remove other segments
					log::warn!("Error removing old WAL segment {segment_id}: {e}");
				}
			}
		}
	}

	Ok(removed_count)
}
