use std::fs::File;
use std::io::Write as IoWrite;
use std::path::Path;

use tempdir::TempDir;
use test_log::test;

use crate::batch::Batch;
use crate::wal::manager::Wal;
use crate::wal::reader::Reader;
use crate::wal::recovery::replay_wal;
use crate::wal::{
	cleanup_old_segments,
	get_segment_range,
	list_segment_ids,
	parse_segment_name,
	segment_name,
	should_include_file,
	CompressionType,
	Options,
	RecordType,
	SegmentRef,
};
use crate::LSMIterator;

fn create_temp_directory() -> TempDir {
	TempDir::new("test").unwrap()
}

fn create_segment_file(dir: &Path, name: &str) {
	let file_path = dir.join(name);
	let mut file = File::create(file_path).unwrap();
	file.write_all(b"dummy content").unwrap();
}

// Utility function to create a temporary WAL directory
fn create_test_wal_dir() -> TempDir {
	TempDir::new("wal_test").unwrap()
}

#[test]
fn test_cleanup_old_segments() {
	let temp_dir = create_test_wal_dir();

	// Create WAL with manual rotation to create multiple segments
	let opts = Options::default();
	let mut wal = Wal::open(temp_dir.path(), opts).unwrap();

	// Create 5 segments with manual rotation
	for segment in 0..5 {
		// Write records to current segment
		for i in 0..10 {
			let data = format!("seg{segment}_record_{i:02}").into_bytes();
			wal.append(&data).unwrap();
		}

		// Rotate to next segment (except for the last one)
		if segment < 4 {
			wal.rotate().unwrap();
		}
	}

	// Get segment IDs and verify we have at least 5
	let segment_ids_before = list_segment_ids(temp_dir.path(), Some("wal")).unwrap();
	assert!(
		segment_ids_before.len() >= 5,
		"Expected at least 5 segments, got {}",
		segment_ids_before.len()
	);

	// Run cleanup - should remove all segments older than the latest one
	// Set min_wal_number to the latest segment ID to keep only that segment
	let latest_segment_id = *segment_ids_before.iter().max().unwrap();
	let removed_count = cleanup_old_segments(temp_dir.path(), latest_segment_id).unwrap();

	// Verify at least 4 segments were removed (keeping only segments >=
	// latest_segment_id)
	assert!(removed_count >= 4, "Expected at least 4 segments to be removed, got {removed_count}");

	// Verify only the latest segment remains
	let remaining_segment_ids = list_segment_ids(temp_dir.path(), Some("wal")).unwrap();

	// Should have exactly 1 segment remaining
	assert_eq!(
		remaining_segment_ids.len(),
		1,
		"Expected exactly 1 segment remaining, got {}",
		remaining_segment_ids.len()
	);

	// The remaining segment should be the latest one
	assert_eq!(
		remaining_segment_ids[0], latest_segment_id,
		"Expected latest segment {} to remain, but got {}",
		latest_segment_id, remaining_segment_ids[0]
	);
}

#[test]
fn test_wal_replay_all_segments() {
	let temp_dir = create_test_wal_dir();

	// Create WAL with manual rotation to create multiple segments
	let opts = Options::default();
	let mut wal = Wal::open(temp_dir.path(), opts).unwrap();

	// Write batch to first segment (segment 0)
	let mut batch1 = Batch::new(100);
	for i in 0..3 {
		let key = format!("key_seg0_{i:02}");
		let value = format!("value_seg0_{i:02}");
		batch1.set(key.into_bytes(), value.into_bytes(), 0).unwrap();
	}
	wal.append(&batch1.encode().unwrap()).unwrap();

	// Manually rotate to create second segment
	wal.rotate().unwrap();

	// Write batch to second segment (segment 1)
	let mut batch2 = Batch::new(200);
	for i in 0..4 {
		let key = format!("key_seg1_{i:02}");
		let value = format!("value_seg1_{i:02}");
		batch2.set(key.into_bytes(), value.into_bytes(), 0).unwrap();
	}
	wal.append(&batch2.encode().unwrap()).unwrap();

	// Manually rotate to create third segment
	wal.rotate().unwrap();

	// Write batch to third segment (segment 2)
	let mut batch3 = Batch::new(300);
	for i in 0..5 {
		let key = format!("key_seg2_{i:02}");
		let value = format!("value_seg2_{i:02}");
		batch3.set(key.into_bytes(), value.into_bytes(), 0).unwrap();
	}
	wal.append(&batch3.encode().unwrap()).unwrap();

	// Close WAL to ensure everything is flushed
	drop(wal);

	// Get segment IDs and verify we have at least 3
	let segment_ids = list_segment_ids(temp_dir.path(), Some("wal")).unwrap();
	assert!(segment_ids.len() >= 3, "Expected at least 3 segments, got {}", segment_ids.len());

	// Replay WAL - should replay all segments
	let arena_size = 1024 * 1024; // 1MB for tests
	let (sequence_number_opt, memtables) = replay_wal(temp_dir.path(), 0, arena_size).unwrap();

	let sequence_number = sequence_number_opt.unwrap_or(0);

	// Count actual entries across ALL returned memtables
	let mut entry_count = 0;
	for (memtable, _) in memtables {
		let mut iter = memtable.iter();
		while iter.valid() {
			entry_count += 1;
			iter.next().unwrap();
		}
	}

	// Verify sequence number is from the latest segment (304 = 300 + 4)
	assert_eq!(sequence_number, 304, "Expected max sequence number from all segments");

	// Verify the memtable contains data from ALL segments (3 + 4 + 5 = 12 entries)
	assert_eq!(entry_count, 12, "Expected 12 entries from all 3 segments combined");
}

#[test]
fn test_wal_with_zero_padding_eof_handling() {
	let temp_dir = create_temp_directory();

	// Create a WAL with small data that doesn't fill the block
	let opts = Options::default();
	let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

	// Write a small record that won't fill the entire block
	let small_data = b"hello";
	wal.append(small_data).expect("should append data");

	// Close the WAL to ensure it's flushed with zero padding
	wal.close().expect("should close WAL");

	// Now try to read from the segment using the Reader
	let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
		.expect("should read segments");

	let file = File::open(&segments[0].file_path).expect("should open file");
	let mut reader = Reader::new(file);

	let result = reader.read();
	match result {
		Ok((data, _offset)) => {
			assert_eq!(data, small_data);
		}
		Err(e) => {
			panic!("Failed to read data: {e}");
		}
	}

	// Try to read again
	let result2 = reader.read();
	match result2 {
		Ok(_) => {
			panic!("Expected EOF, but got data");
		}
		Err(e) => {
			match e {
				crate::wal::Error::IO(io_err) => {
					if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
						// Expected EOF error
					} else {
						panic!("Got unexpected IO error: {io_err}");
					}
				}
				crate::wal::Error::Corruption(_) => {
					panic!("Got corruption error when expecting EOF: {e}");
				}
				_ => {
					panic!("Got unexpected error type: {e}");
				}
			}
		}
	}
}

#[test]
fn test_empty_wal_segment() {
	let temp_dir = create_temp_directory();

	// Create an empty WAL
	let opts = Options::default();
	let wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

	// Close immediately without writing anything
	drop(wal);

	// Try to read from the empty segment
	let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
		.expect("should read segments");

	let file = File::open(&segments[0].file_path).expect("should open file");
	let mut reader = Reader::new(file);

	let result = reader.read();
	match result {
		Ok(_) => {
			panic!("Expected EOF from empty segment, but got data");
		}
		Err(e) => {
			match e {
				crate::wal::Error::IO(io_err) => {
					if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
						// Expected EOF error from empty segment
					} else {
						panic!("Got unexpected IO error from empty segment: {io_err}");
					}
				}
				crate::wal::Error::Corruption(_) => {
					panic!("Got corruption error from empty segment when expecting EOF: {e}");
				}
				_ => {
					panic!("Got unexpected error type from empty segment: {e}");
				}
			}
		}
	}
}

// ===== Format Tests =====

#[test]
fn test_record_type_from_u8() {
	assert_eq!(RecordType::from_u8(0).unwrap(), RecordType::Empty);
	assert_eq!(RecordType::from_u8(1).unwrap(), RecordType::Full);
	assert_eq!(RecordType::from_u8(9).unwrap(), RecordType::SetCompressionType);
	assert!(RecordType::from_u8(5).is_err()); // Recyclable types no longer supported
	assert!(RecordType::from_u8(255).is_err());
}

#[test]
fn test_compression_type_from_u8() {
	assert_eq!(CompressionType::from_u8(0).unwrap(), CompressionType::None);
	assert!(CompressionType::from_u8(255).is_err());
}

// ===== Options Tests =====

#[test]
fn test_options_validation() {
	let mut opts = Options::default();
	assert!(opts.validate().is_ok());

	opts.max_file_size = 0;
	assert!(opts.validate().is_err());
}

#[test]
fn test_options_builder() {
	let opts = Options::default()
		.with_max_file_size(50 * 1024 * 1024)
		.with_compression(CompressionType::Lz4)
		.with_extension("log".to_string());

	assert_eq!(opts.max_file_size, 50 * 1024 * 1024);
	assert_eq!(opts.compression_type, CompressionType::Lz4);
	assert_eq!(opts.file_extension, Some("log".to_string()));
}

// ===== Segment Utility Tests =====

#[test]
fn segment_name_with_extension() {
	let index = 100;
	let ext = "log";
	let expected = format!("{index:020}.{ext}");
	assert_eq!(segment_name(index, ext), expected);
}

#[test]
fn segment_name_without_extension() {
	let index = 100;
	let expected = format!("{index:020}");
	assert_eq!(segment_name(index, ""), expected);
}

#[test]
fn segment_name_with_compound_extension() {
	let index = 5;
	let name = segment_name(index, "wal.repair");
	assert_eq!(name, "00000000000000000005.wal.repair");

	let (parsed_id, parsed_ext) = parse_segment_name(&name).unwrap();
	assert_eq!(parsed_id, index);
	assert_eq!(parsed_ext, Some("wal.repair".to_string()));
}

#[test]
fn parse_segment_name_with_extension() {
	let name = "00000000000000000010.log";
	let result = parse_segment_name(name).unwrap();
	assert_eq!(result, (10, Some("log".to_string())));
}

#[test]
fn parse_segment_name_without_extension() {
	let name = "00000000000000000010";
	let result = parse_segment_name(name).unwrap();
	assert_eq!(result, (10, None));
}

#[test]
fn parse_segment_name_with_compound_extension() {
	let name = "00000000000000000010.wal.repair";
	let result = parse_segment_name(name).unwrap();
	assert_eq!(result, (10, Some("wal.repair".to_string())));
}

#[test]
fn parse_segment_name_invalid_format() {
	let name = "invalid_name";
	let result = parse_segment_name(name);
	assert!(result.is_err());
}

#[test]
fn segments_empty_directory() {
	let temp_dir = create_temp_directory();
	let dir = temp_dir.path().to_path_buf();

	let result = get_segment_range(&dir, Some("wal")).unwrap();
	assert_eq!(result, (0, 0));
}

#[test]
fn segments_non_empty_directory() {
	let temp_dir = create_temp_directory();
	let dir = temp_dir.path().to_path_buf();

	create_segment_file(&dir, "00000000000000000001.log");
	create_segment_file(&dir, "00000000000000000003.log");
	create_segment_file(&dir, "00000000000000000002.log");
	create_segment_file(&dir, "00000000000000000004.log");

	let result = get_segment_range(&dir, Some("log")).unwrap();
	assert_eq!(result, (1, 4));
}

#[test]
fn test_get_segment_range_with_compound_extension() {
	let temp_dir = create_temp_directory();
	let dir = temp_dir.path().to_path_buf();

	create_segment_file(&dir, "00000000000000000001.wal");
	create_segment_file(&dir, "00000000000000000002.wal");
	create_segment_file(&dir, "00000000000000000003.wal.repair");
	create_segment_file(&dir, "00000000000000000004.wal.repair");
	create_segment_file(&dir, "00000000000000000005.wal.repair");
	create_segment_file(&dir, "00000000000000000006.wal");

	let result = get_segment_range(&dir, Some("wal")).unwrap();
	assert_eq!(result, (1, 6));

	let result = get_segment_range(&dir, Some("wal.repair")).unwrap();
	assert_eq!(result, (3, 5));

	let wal_segments = list_segment_ids(&dir, Some("wal")).unwrap();
	assert_eq!(wal_segments, vec![1, 2, 6]);

	let repair_segments = list_segment_ids(&dir, Some("wal.repair")).unwrap();
	assert_eq!(repair_segments, vec![3, 4, 5]);
}

#[test]
fn test_list_segment_ids() {
	let temp_dir = TempDir::new("test").expect("should create temp dir");
	let dir_path = temp_dir.path();

	create_segment_file(dir_path, &segment_name(1, ""));
	create_segment_file(dir_path, &segment_name(2, ""));
	create_segment_file(dir_path, &segment_name(10, ""));

	let segment_ids = list_segment_ids(dir_path, None).unwrap();
	assert_eq!(segment_ids, vec![1, 2, 10]);
}

#[test]
fn test_list_segment_ids_with_extension_filter() {
	let temp_dir = TempDir::new("test").expect("should create temp dir");
	let dir_path = temp_dir.path();

	create_segment_file(dir_path, &segment_name(1, ""));
	create_segment_file(dir_path, &segment_name(2, "repair"));
	create_segment_file(dir_path, &segment_name(3, "tmp"));
	create_segment_file(dir_path, &segment_name(4, "repair"));

	let segment_ids_no_ext = list_segment_ids(dir_path, None).unwrap();
	assert_eq!(segment_ids_no_ext, vec![1]);

	let segment_ids_repair = list_segment_ids(dir_path, Some("repair")).unwrap();
	assert_eq!(segment_ids_repair, vec![2, 4]);

	let segment_ids_tmp = list_segment_ids(dir_path, Some("tmp")).unwrap();
	assert_eq!(segment_ids_tmp, vec![3]);
}

#[test]
fn test_should_include_file_helper() {
	assert!(should_include_file(None, None));
	assert!(!should_include_file(None, Some("repair".to_string())));
	assert!(!should_include_file(None, Some("tmp".to_string())));

	assert!(!should_include_file(Some("repair"), None));
	assert!(should_include_file(Some("repair"), Some("repair".to_string())));
	assert!(!should_include_file(Some("repair"), Some("tmp".to_string())));
	assert!(!should_include_file(Some("tmp"), Some("repair".to_string())));
}

#[test]
fn test_cleanup_respects_min_wal_number() {
	let temp_dir = create_test_wal_dir();

	// Create WAL with manual rotation to create 5 segments (0-4)
	let opts = Options::default();
	let mut wal = Wal::open(temp_dir.path(), opts).unwrap();

	for segment in 0..5 {
		// Write records to current segment
		for i in 0..10 {
			let data = format!("seg{segment}_record_{i:02}").into_bytes();
			wal.append(&data).unwrap();
		}

		// Rotate to next segment (except for the last one)
		if segment < 4 {
			wal.rotate().unwrap();
		}
	}

	wal.close().unwrap();

	// Get segment IDs before cleanup
	let segment_ids_before = list_segment_ids(temp_dir.path(), Some("wal")).unwrap();
	assert_eq!(segment_ids_before.len(), 5, "Should have 5 segments initially");

	// Run cleanup with min_wal_number = 3
	// This should delete segments 0, 1, 2 and keep 3, 4
	let removed_count = cleanup_old_segments(temp_dir.path(), 3).unwrap();

	assert_eq!(removed_count, 3, "Should remove segments 0, 1, 2");

	// Verify remaining segments
	let remaining_segment_ids = list_segment_ids(temp_dir.path(), Some("wal")).unwrap();
	assert_eq!(remaining_segment_ids.len(), 2, "Should have 2 segments remaining");
	assert_eq!(remaining_segment_ids[0], 3, "Segment 3 should remain");
	assert_eq!(remaining_segment_ids[1], 4, "Segment 4 should remain");
}
