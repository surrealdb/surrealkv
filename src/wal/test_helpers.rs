use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use crate::batch::Batch;
use crate::memtable::MemTable;
use crate::wal::manager::Wal;
use crate::wal::Options;

/// Helper utilities for WAL recovery tests
pub struct WalTestHelper;

#[derive(Debug, Clone, Copy)]
pub enum CorruptionType {
	/// Insert random bytes at the corruption point
	RandomBytes,
	/// Truncate the file at the corruption point
	Truncate,
	/// Modify data to cause CRC mismatch
	CrcMismatch,
	/// Corrupt the record header (type, length, or CRC fields)
	HeaderCorruption,
}

impl WalTestHelper {
	/// Create N segments with specified number of entries per segment
	///
	/// # Arguments
	/// * `dir` - Directory to create WAL segments in
	/// * `entries_per_segment` - Number of entries for each segment
	/// * `starting_seq` - Starting sequence number
	///
	/// # Returns
	/// The final sequence number after all segments are created
	pub fn create_segments(dir: &Path, entries_per_segment: &[usize], starting_seq: u64) -> u64 {
		let opts = Options::default();
		let mut wal = Wal::open(dir, opts).unwrap();
		let mut current_seq = starting_seq;

		for (seg_idx, &entry_count) in entries_per_segment.iter().enumerate() {
			if entry_count > 0 {
				let mut batch = Batch::new(current_seq);
				for i in 0..entry_count {
					let key = format!("seg{}_key{:04}", seg_idx, i);
					let value = format!("seg{}_value{:04}", seg_idx, i);
					batch.set(key.as_bytes(), value.as_bytes(), 0).unwrap();
					current_seq += 1;
				}
				wal.append(&batch.encode().unwrap()).unwrap();
			}

			// Rotate to next segment (except for last one)
			if seg_idx < entries_per_segment.len() - 1 {
				wal.rotate().unwrap();
			}
		}

		wal.close().unwrap();
		current_seq
	}

	/// Create a segment with a specific sequence range
	pub fn create_segment_with_seq_range(
		dir: &Path,
		segment_id: u64,
		seq_start: u64,
		seq_end: u64,
	) {
		// If segment_id > 0, we need to create previous segments first
		if segment_id > 0 {
			let opts = Options::default();
			let mut wal = Wal::open(dir, opts).unwrap();

			// Rotate to reach the desired segment_id
			for _ in 0..segment_id {
				wal.rotate().unwrap();
			}

			// Now create the batch for this segment
			let entry_count = (seq_end - seq_start + 1) as usize;
			let mut batch = Batch::new(seq_start);
			for i in 0..entry_count {
				let key = format!("key{}", seq_start + i as u64);
				let value = format!("value{}", seq_start + i as u64);
				batch.set(key.as_bytes(), value.as_bytes(), 0).unwrap();
			}
			wal.append(&batch.encode().unwrap()).unwrap();
			wal.close().unwrap();
		} else {
			// Segment 0 - create directly
			let opts = Options::default();
			let mut wal = Wal::open(dir, opts).unwrap();

			let entry_count = (seq_end - seq_start + 1) as usize;
			let mut batch = Batch::new(seq_start);
			for i in 0..entry_count {
				let key = format!("key{}", seq_start + i as u64);
				let value = format!("value{}", seq_start + i as u64);
				batch.set(key.as_bytes(), value.as_bytes(), 0).unwrap();
			}
			wal.append(&batch.encode().unwrap()).unwrap();
			wal.close().unwrap();
		}
	}

	/// Corrupt a segment at a specific offset percentage
	///
	/// # Arguments
	/// * `dir` - WAL directory
	/// * `segment_id` - Which segment to corrupt
	/// * `offset_percent` - Where to corrupt (0.0 to 1.0)
	/// * `corruption_type` - Type of corruption to apply
	pub fn corrupt_segment(
		dir: &Path,
		segment_id: u64,
		offset_percent: f64,
		corruption_type: CorruptionType,
	) {
		let segment_path = dir.join(format!("{:020}.wal", segment_id));

		if !segment_path.exists() {
			panic!("Segment {} does not exist", segment_id);
		}

		let file_size = fs::metadata(&segment_path).unwrap().len();
		let corruption_offset = (file_size as f64 * offset_percent) as u64;

		match corruption_type {
			CorruptionType::RandomBytes => {
				let mut file =
					OpenOptions::new().read(true).write(true).open(&segment_path).unwrap();
				file.seek(SeekFrom::Start(corruption_offset)).unwrap();
				// Write random corrupted bytes
				let corrupt_data = vec![0xFF, 0xAA, 0x55, 0x00, 0xFF];
				file.write_all(&corrupt_data).unwrap();
			}
			CorruptionType::Truncate => {
				let file = OpenOptions::new().write(true).open(&segment_path).unwrap();
				file.set_len(corruption_offset).unwrap();
			}
			CorruptionType::CrcMismatch => {
				// Modify data bytes but not CRC
				let mut file =
					OpenOptions::new().read(true).write(true).open(&segment_path).unwrap();
				// Skip header to corrupt only data
				file.seek(SeekFrom::Start(corruption_offset + 7)).unwrap();
				file.write_all(&[0xFF, 0xAA]).unwrap();
			}
			CorruptionType::HeaderCorruption => {
				// Corrupt the record header
				let mut file =
					OpenOptions::new().read(true).write(true).open(&segment_path).unwrap();
				file.seek(SeekFrom::Start(corruption_offset)).unwrap();
				// Write an invalid record type
				file.write_all(&[3u8]).unwrap(); // Middle record at start
			}
		}
	}

	/// Verify that memtable contains expected keys
	pub fn verify_entries(memtable: &Arc<MemTable>, expected_keys: &[String]) {
		let mut found_keys = Vec::new();
		for entry in memtable.iter() {
			// entry is (Arc<InternalKey>, Bytes)
			let key = String::from_utf8(entry.0.user_key.to_vec()).unwrap();
			found_keys.push(key);
		}
		found_keys.sort();

		let mut expected_sorted = expected_keys.to_vec();
		expected_sorted.sort();

		assert_eq!(
			found_keys.len(),
			expected_sorted.len(),
			"Key count mismatch. Found: {}, Expected: {}",
			found_keys.len(),
			expected_sorted.len()
		);

		for (found, expected) in found_keys.iter().zip(expected_sorted.iter()) {
			assert_eq!(found, expected, "Key mismatch");
		}
	}

	/// Verify entry count in memtable
	pub fn verify_entry_count(memtable: &Arc<MemTable>, expected_count: usize) {
		let actual_count = memtable.iter().count();
		assert_eq!(
			actual_count, expected_count,
			"Entry count mismatch. Found: {}, Expected: {}",
			actual_count, expected_count
		);
	}

	/// Generate expected keys for a segment range
	pub fn generate_expected_keys(
		start_seg: usize,
		end_seg: usize,
		entries_per_segment: &[usize],
	) -> Vec<String> {
		let mut keys = Vec::new();
		for seg_idx in start_seg..=end_seg {
			if seg_idx < entries_per_segment.len() {
				let entry_count = entries_per_segment[seg_idx];
				for i in 0..entry_count {
					keys.push(format!("seg{}_key{:04}", seg_idx, i));
				}
			}
		}
		keys
	}
}
