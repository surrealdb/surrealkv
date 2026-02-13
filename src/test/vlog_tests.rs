use std::sync::atomic::Ordering;
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use crate::clock::LogicalClock;
use crate::test::{count_history_versions_in_range, point_in_time_from_history_in_range};
use crate::vlog::{
	VLog,
	VLogFileHeader,
	VLogWriter,
	ValueLocation,
	ValuePointer,
	BIT_VALUE_POINTER,
	VALUE_LOCATION_VERSION,
	VALUE_POINTER_SIZE,
	VLOG_FORMAT_VERSION,
};
use crate::{CompressionType, Options, VLogChecksumLevel};

fn create_test_vlog(opts: Option<Options>) -> (VLog, TempDir, Arc<Options>) {
	let temp_dir = TempDir::new().unwrap();

	let mut opts = opts.unwrap_or(Options {
		vlog_checksum_verification: VLogChecksumLevel::Full,
		..Default::default()
	});

	opts.path = temp_dir.path().to_path_buf();

	// Create vlog subdirectory
	std::fs::create_dir_all(opts.vlog_dir()).unwrap();

	let opts = Arc::new(opts);
	let opts_clone = Arc::clone(&opts);
	let vlog = VLog::new(opts).unwrap();
	(vlog, temp_dir, opts_clone)
}

#[test]
fn test_value_pointer_encoding() {
	let pointer = ValuePointer::new(123, 456, 111, 789, 0xdeadbeef);
	let encoded = pointer.encode();
	let decoded = ValuePointer::decode(&encoded).unwrap();
	assert_eq!(pointer, decoded);
}

#[test]
fn test_value_pointer_utility_methods() {
	let pointer = ValuePointer::new(123, 456, 11, 789, 42);
	let encoded = pointer.encode();

	// Test with valid pointer
	assert!(ValuePointer::decode(&encoded).is_ok());
	assert_eq!(ValuePointer::decode(&encoded).unwrap(), pointer);

	// Test with wrong size data
	let wrong_size = vec![0u8; 20];
	assert!(ValuePointer::decode(&wrong_size).is_err());

	// Test with random data of correct size
	let random_data = vec![0x42u8; VALUE_POINTER_SIZE];
	// With random data, decode might work but produce a nonsense pointer
	// We just check it doesn't crash
	let _ = ValuePointer::decode(&random_data);
}

#[test(tokio::test)]
async fn test_vlog_append_and_get() {
	let (vlog, _temp_dir, _) = create_test_vlog(None);

	let key = b"test_key";
	let value = vec![1u8; 300]; // Large enough for vlog
	let pointer = vlog.append(key, &value).unwrap();
	vlog.sync().unwrap();

	let retrieved = vlog.get(&pointer).unwrap();
	assert_eq!(value, *retrieved);
}

#[test(tokio::test)]
async fn test_vlog_small_value_acceptance() {
	let (vlog, _temp_dir, _) = create_test_vlog(None);

	let key = b"test_key";
	let small_value = vec![1u8; 10]; // Small value should now be accepted
	let pointer = vlog.append(key, &small_value).unwrap();
	vlog.sync().unwrap();

	let retrieved = vlog.get(&pointer).unwrap();
	assert_eq!(small_value, *retrieved);
}

#[test(tokio::test)]
async fn test_vlog_caching() {
	let (vlog, _temp_dir, _) = create_test_vlog(None);

	let key = b"cache_test_key";
	let value = vec![42u8; 1000]; // Large enough value
	let pointer = vlog.append(key, &value).unwrap();
	vlog.sync().unwrap();

	// First read - should read from disk and cache the value
	let retrieved1 = vlog.get(&pointer).unwrap();
	assert_eq!(value, *retrieved1);

	// Second read - should read from cache
	let retrieved2 = vlog.get(&pointer).unwrap();
	assert_eq!(value, *retrieved2);
	assert_eq!(retrieved1, retrieved2);

	// Verify that the cache contains the value
	let cached_value = vlog.opts.block_cache.get_vlog(pointer.file_id, pointer.offset);
	assert!(cached_value.is_some());
	assert_eq!(cached_value.unwrap(), value);
}

#[test(tokio::test)]
async fn test_prefill_file_handles() {
	let opts = Options {
		vlog_max_file_size: 1024, // Small file size to force multiple files
		vlog_checksum_verification: VLogChecksumLevel::Full,
		..Default::default()
	};
	// Create initial VLog and add data to create multiple files
	let (vlog1, _temp_dir, opts) = create_test_vlog(Some(opts));

	// Add data to create multiple VLog files
	let mut file_ids = Vec::new();
	let mut pointers = Vec::new();

	// Add enough data to create at least 3 files
	for i in 0..10 {
		let key = format!("key_{i}").into_bytes();
		let value = vec![i as u8; 200]; // Large enough to fill files quickly
		let pointer = vlog1.append(&key, &value).unwrap();
		file_ids.push(pointer.file_id);
		pointers.push(pointer);

		// Sync after each append to ensure data is written to disk
		vlog1.sync().unwrap();
	}

	// Verify we created multiple files
	let unique_file_ids: std::collections::HashSet<_> = file_ids.iter().collect();
	assert!(
		unique_file_ids.len() > 1,
		"Should have created multiple VLog files, got {}",
		unique_file_ids.len()
	);

	// Drop the first VLog to close all file handles
	vlog1.close().unwrap();

	// Create a new VLog instance - this should trigger prefill_file_handles
	let vlog2 = VLog::new(opts).unwrap();

	// Verify that all existing files can be read
	for (i, pointer) in pointers.iter().enumerate() {
		let retrieved = vlog2.get(pointer).unwrap();
		let expected_value = vec![i as u8; 200];
		assert_eq!(*retrieved, expected_value);
	}

	// Check that the next_file_id is set correctly (should be one past the highest
	// file ID)
	let next_file_id = vlog2.next_file_id.load(Ordering::SeqCst);
	let max_file_id = unique_file_ids.iter().max().unwrap();
	assert_eq!(
		next_file_id,
		**max_file_id + 1,
		"next_file_id should be one past the highest file ID"
	);

	// Add new data and verify it goes to the correct active file
	let new_key = b"new_key";
	let new_value = vec![255u8; 200];
	let new_pointer = vlog2.append(new_key, &new_value).unwrap();

	// Sync to ensure data is written to disk
	vlog2.sync().unwrap();

	// The new pointer should have the correct file_id (should be the active writer)
	let active_writer_id = vlog2.active_writer_id.load(Ordering::SeqCst);
	assert_eq!(
		new_pointer.file_id, active_writer_id,
		"New pointer should be written to the active writer file"
	);

	// Verify the new data can be read
	let retrieved_new = vlog2.get(&new_pointer).unwrap();
	assert_eq!(*retrieved_new, new_value);

	// Verify that the new file_id is not one of the old ones
	assert!(
		!unique_file_ids.contains(&new_pointer.file_id),
		"New data should not be written to an old file"
	);

	// Check that file handles are properly cached
	let file_handles = vlog2.file_handles.read();
	assert!(
		file_handles.len() >= unique_file_ids.len(),
		"Should have cached handles for all existing files"
	);

	// Verify we can still read from old files after adding new data
	for (i, pointer) in pointers.iter().enumerate() {
		let retrieved = vlog2.get(pointer).unwrap();
		let expected_value = vec![i as u8; 200];
		assert_eq!(*retrieved, expected_value);
	}
}

#[test]
fn test_value_pointer_encode_into_decode() {
	let pointer = ValuePointer::new(123, 456, 111, 789, 0xdeadbeef);

	// Test encode
	let encoded = pointer.encode();
	assert_eq!(encoded.len(), VALUE_POINTER_SIZE);

	// Test decode
	let decoded = ValuePointer::decode(&encoded).unwrap();
	assert_eq!(pointer, decoded);
}

#[test]
fn test_value_pointer_codec() {
	let pointer = ValuePointer::new(1, 2, 3, 4, 0);

	// Test with zero values
	let encoded = pointer.encode();
	let decoded = ValuePointer::decode(&encoded).unwrap();
	assert_eq!(pointer, decoded);

	// Test with maximum values
	let max_pointer = ValuePointer::new(u32::MAX, u64::MAX, u32::MAX, u32::MAX, u32::MAX);
	let encoded = max_pointer.encode();
	let decoded = ValuePointer::decode(&encoded).unwrap();
	assert_eq!(max_pointer, decoded);
}

#[test]
fn test_value_pointer_decode_insufficient_data() {
	let incomplete_data = vec![0u8; VALUE_POINTER_SIZE - 1];
	let result = ValuePointer::decode(&incomplete_data);
	assert!(result.is_err());
}

#[test]
fn test_value_location_inline_encoding() {
	let test_data = b"hello world";
	let location = ValueLocation::with_inline_value(test_data.to_vec());

	// Test encode
	let encoded = location.encode();
	assert_eq!(encoded.len(), 1 + 1 + test_data.len()); // meta + version (1 byte) + data
	assert_eq!(encoded[0], 0); // meta should be 0 for inline
	assert_eq!(encoded[1], VALUE_LOCATION_VERSION); // version should be 1
	assert_eq!(&encoded[2..], test_data); // Skip meta (1) and version (1)

	// Test decode
	let decoded = ValueLocation::decode(&encoded).unwrap();
	assert_eq!(location, decoded);

	// Verify it's inline
	assert!(!decoded.is_value_pointer());
}

#[test]
fn test_value_location_vlog_encoding() {
	let pointer = ValuePointer::new(1, 1024, 8, 256, 0x12345678);
	let location = ValueLocation::with_pointer(pointer.clone());

	// Test encode
	let encoded = location.encode();
	assert_eq!(encoded.len(), 1 + 1 + VALUE_POINTER_SIZE); // meta + version (1 byte) + pointer size
	assert_eq!(encoded[0], BIT_VALUE_POINTER); // meta should have BIT_VALUE_POINTER set

	// Test decode
	let decoded = ValueLocation::decode(&encoded).unwrap();
	assert_eq!(location, decoded);

	// Verify it's a value pointer
	assert!(decoded.is_value_pointer());

	// Verify pointer matches by decoding the value
	let decoded_pointer = ValuePointer::decode(&decoded.value).unwrap();
	assert_eq!(pointer, decoded_pointer);
}

#[test]
fn test_value_location_encode_into_decode() {
	let test_cases = vec![
		ValueLocation::with_inline_value(b"small data".to_vec()),
		ValueLocation::with_pointer(ValuePointer::new(1, 100, 10, 50, 0xabcdef)),
		ValueLocation::with_inline_value(Vec::new()), // empty data
	];

	for location in test_cases {
		// Test encode_into
		let mut encoded = Vec::new();
		location.encode_into(&mut encoded).unwrap();

		// Test decode
		let decoded = ValueLocation::decode(&encoded).unwrap();

		assert_eq!(location, decoded);
	}
}

#[test]
fn test_value_location_size_calculation() {
	// Test inline size
	let inline_data = b"test data";
	let inline_location = ValueLocation::with_inline_value(inline_data.to_vec());
	assert_eq!(inline_location.encoded_size(), 1 + 1 + inline_data.len()); // meta + version + data

	// Test VLog size
	let pointer = ValuePointer::new(1, 100, 8, 256, 0x12345);
	let vlog_location = ValueLocation::with_pointer(pointer);
	assert_eq!(vlog_location.encoded_size(), 1 + 1 + VALUE_POINTER_SIZE); // meta + version + pointer
}

#[test(tokio::test)]
async fn test_value_location_resolve_inline() {
	let (vlog, _temp_dir, _) = create_test_vlog(None);
	let vlog = Arc::new(vlog);
	let test_data = b"inline test data";
	let location = ValueLocation::with_inline_value(test_data.to_vec());

	let resolved = location.resolve_value(Some(&vlog)).unwrap();
	assert_eq!(&*resolved, test_data);
}

#[test(tokio::test)]
async fn test_value_location_resolve_vlog() {
	let (vlog, _temp_dir, _) = create_test_vlog(None);
	let vlog = Arc::new(vlog);
	let key = b"test_key";
	let value = b"test_value_for_vlog_resolution";

	// Append to vlog to get a real pointer
	let pointer = vlog.append(key, value).unwrap();
	vlog.sync().unwrap(); // Sync to ensure data is written to disk
	let location = ValueLocation::with_pointer(pointer);

	// Resolve should return the original value
	let resolved = location.resolve_value(Some(&vlog)).unwrap();
	assert_eq!(&*resolved, value);
}

#[test]
fn test_value_location_from_encoded_value_inline() {
	let test_data = b"encoded inline data";
	let location = ValueLocation::with_inline_value(test_data.to_vec());
	let encoded = location.encode();

	// Should work without VLog for inline data
	let decoded_location = ValueLocation::decode(&encoded).unwrap();
	let resolved = decoded_location.resolve_value(None).unwrap();
	assert_eq!(&*resolved, test_data);
}

#[test(tokio::test)]
async fn test_value_location_from_encoded_value_vlog() {
	let (vlog, _temp_dir, _) = create_test_vlog(None);
	let key = b"test_key";
	let value = b"test_value_for_encoded_resolution";

	// Append to vlog and create encoded VLog location
	let pointer = vlog.append(key, value).unwrap();
	vlog.sync().unwrap(); // Sync to ensure data is written to disk
	let location = ValueLocation::with_pointer(pointer);
	let encoded = location.encode();

	// Should resolve with VLog
	let decoded_location = ValueLocation::decode(&encoded).unwrap();
	let resolved = decoded_location.clone().resolve_value(Some(&Arc::new(vlog))).unwrap();
	assert_eq!(&*resolved, value);

	// Should fail without VLog
	let result = decoded_location.resolve_value(None);
	assert!(result.is_err());
}

#[test]
fn test_value_location_edge_cases() {
	// Test with maximum size inline data
	let max_inline = vec![0xffu8; u16::MAX as usize];
	let location = ValueLocation::with_inline_value(max_inline);
	let encoded = location.encode();
	let decoded = ValueLocation::decode(&encoded).unwrap();
	assert_eq!(location, decoded);

	// Test with pointer containing edge values
	let edge_pointer = ValuePointer::new(1, u64::MAX, 0, u32::MAX, 0);
	let location = ValueLocation::with_pointer(edge_pointer);
	let encoded = location.encode();
	let decoded = ValueLocation::decode(&encoded).unwrap();
	assert_eq!(location, decoded);
}

#[test]
fn test_vlog_file_header_encoding() {
	let opts = Options::default();
	let header = VLogFileHeader::new(123, opts.vlog_max_file_size, CompressionType::None as u8);
	let encoded = header.encode();
	let decoded = VLogFileHeader::decode(&encoded).unwrap();

	assert_eq!(header.magic, decoded.magic);
	assert_eq!(header.version, decoded.version);
	assert_eq!(header.file_id, decoded.file_id);
	assert_eq!(header.created_at, decoded.created_at);
	assert_eq!(header.max_file_size, decoded.max_file_size);
	assert_eq!(header.compression, decoded.compression);
	assert_eq!(header.reserved, decoded.reserved);
	assert!(decoded.is_compatible());
}

#[test]
fn test_vlog_file_header_invalid_magic() {
	let opts = Options::default();
	let mut header = VLogFileHeader::new(123, opts.vlog_max_file_size, CompressionType::None as u8);
	header.magic = 0x12345678; // Invalid magic
	let encoded = header.encode();

	assert!(VLogFileHeader::decode(&encoded).is_err());
}

#[test]
fn test_vlog_file_header_invalid_size() {
	let opts = Options::default();
	let header = VLogFileHeader::new(123, opts.vlog_max_file_size, CompressionType::None as u8);
	let mut encoded = header.encode().to_vec();
	encoded.pop(); // Remove one byte to make it invalid size

	assert!(VLogFileHeader::decode(&encoded).is_err());
}

#[test]
fn test_vlog_file_header_version_compatibility() {
	let opts = Options::default();
	let mut header = VLogFileHeader::new(123, opts.vlog_max_file_size, CompressionType::None as u8);
	header.version = VLOG_FORMAT_VERSION;
	assert!(header.is_compatible());

	header.version = VLOG_FORMAT_VERSION + 1;
	assert!(!header.is_compatible());
}

#[test(tokio::test)]
async fn test_vlog_with_file_header() {
	let (vlog, _temp_dir, _) = create_test_vlog(None);

	// Append some data to create a VLog file with header
	let key = b"test_key";
	let value = b"test_value";
	let pointer = vlog.append(key, value).unwrap();
	vlog.sync().unwrap();

	// Retrieve the value to ensure header validation works
	let retrieved_value = vlog.get(&pointer).unwrap();
	assert_eq!(&retrieved_value, value);
}

#[test(tokio::test)]
async fn test_vlog_restart_continues_last_file() {
	let temp_dir = TempDir::new().unwrap();
	let vlog_max_file_size = 2048;
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		vlog_max_file_size,
		vlog_checksum_verification: VLogChecksumLevel::Full,
		..Default::default()
	};

	// Create vlog subdirectory
	std::fs::create_dir_all(opts.vlog_dir()).unwrap();

	let opts = Arc::new(opts);

	let mut pointers = Vec::new();

	// Phase 1: Create initial VLog and add some data (but not enough to fill the
	// file)
	{
		let vlog1 = VLog::new(Arc::clone(&opts)).unwrap();

		// Add some data to the first file
		for i in 0..3 {
			let key = format!("key_{i}").into_bytes();
			let value = vec![i as u8; 100];
			let pointer = vlog1.append(&key, &value).unwrap();
			pointers.push((pointer, value));
		}
		vlog1.sync().unwrap();

		// Verify we're writing to file 1
		let first_file_id = pointers[0].0.file_id;
		assert_eq!(first_file_id, 1, "First data should go to file 1");

		// All data should be in the same file since we're not filling it
		assert!(
			pointers.iter().all(|(p, _)| p.file_id == first_file_id),
			"All initial data should be in the same file"
		);

		// Get the file size to ensure it's not at max capacity
		let file_path = vlog1.vlog_file_path(first_file_id);
		let file_size = std::fs::metadata(&file_path).unwrap().len();
		assert!(file_size < vlog_max_file_size, "File should not be at max capacity");

		// Check that active_writer_id is correctly set
		let active_writer_id = vlog1.active_writer_id.load(Ordering::SeqCst);
		assert_eq!(
			active_writer_id, first_file_id,
			"Active writer ID should be set to the first file"
		);

		// Verify writer is set up
		assert!(vlog1.writer.read().is_some(), "Writer should be set up");
		vlog1.close().unwrap();
	}

	// Phase 2: Restart VLog and verify it continues with the last file
	{
		let vlog2 = VLog::new(Arc::clone(&opts)).unwrap();

		// Check that active_writer_id is set to the last file (file 0)
		let active_writer_id = vlog2.active_writer_id.load(Ordering::SeqCst);
		assert_eq!(
			active_writer_id, 0,
			"After restart, active writer ID should be set to the last file (0)"
		);

		// Check that writer is set up
		assert!(
			vlog2.writer.read().is_some(),
			"After restart, writer should be set up for the last file"
		);

		// Check that next_file_id is correct
		let next_file_id = vlog2.next_file_id.load(Ordering::SeqCst);
		assert_eq!(
			next_file_id, 1,
			"Next file ID should be 1 (one past the highest existing file)"
		);

		// Verify all previous data can still be read
		for (pointer, expected_value) in &pointers {
			let retrieved = vlog2.get(pointer).unwrap();
			assert_eq!(
				*retrieved, *expected_value,
				"Should be able to read existing data after restart"
			);
		}

		// Add new data and verify it goes to the same file (not a new file)
		let new_key = b"new_key_after_restart";
		let new_value = vec![99u8; 100];
		let new_pointer = vlog2.append(new_key, &new_value).unwrap();
		vlog2.sync().unwrap();

		// New data should go to the same file (file 0) since it has space
		assert_eq!(
			new_pointer.file_id, 0,
			"New data after restart should go to the existing file with space"
		);

		// Verify the new data can be read
		let retrieved_new = vlog2.get(&new_pointer).unwrap();
		assert_eq!(
			*retrieved_new, new_value,
			"Should be able to read new data added after restart"
		);

		// Add more data to fill the file and trigger creation of a new file
		let mut large_pointers = Vec::new();
		for i in 0..10 {
			let key = format!("large_key_{i}").into_bytes();
			let value = vec![i as u8; 200]; // Larger values to fill the file
			let pointer = vlog2.append(&key, &value).unwrap();
			large_pointers.push((pointer, value));
		}
		vlog2.sync().unwrap();

		// Check if we eventually created a new file (file 1)
		let has_file_1 = large_pointers.iter().any(|(p, _)| p.file_id == 1);
		if has_file_1 {
			// If we created file 1, verify the active writer ID updated
			let final_active_writer_id = vlog2.active_writer_id.load(Ordering::SeqCst);
			assert_eq!(final_active_writer_id, 1, "Active writer ID should update to the new file");
		}

		// Verify all data (old and new) can still be read
		for (pointer, expected_value) in &pointers {
			let retrieved = vlog2.get(pointer).unwrap();
			assert_eq!(*retrieved, *expected_value);
		}
		for (pointer, expected_value) in &large_pointers {
			let retrieved = vlog2.get(pointer).unwrap();
			assert_eq!(*retrieved, *expected_value);
		}
	}
}

#[test(tokio::test)]
async fn test_vlog_restart_with_multiple_files() {
	let temp_dir = TempDir::new().unwrap();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		vlog_max_file_size: 800,
		vlog_checksum_verification: VLogChecksumLevel::Full,
		..Default::default()
	};

	// Create vlog subdirectory
	std::fs::create_dir_all(opts.vlog_dir()).unwrap();

	let opts = Arc::new(opts);

	let mut all_pointers = Vec::new();
	let mut highest_file_id = 0;

	// Phase 1: Create VLog and add enough data to create 5+ files
	{
		let vlog1 = VLog::new(Arc::clone(&opts)).unwrap();

		// Add enough data to create at least 5 VLog files
		for i in 0..50 {
			let key = format!("multifile_key_{:04}", i).into_bytes();
			let value = vec![i as u8; 80]; // Data to fill files moderately
			let pointer = vlog1.append(&key, &value).unwrap();
			all_pointers.push((pointer.clone(), key, value));
			highest_file_id = highest_file_id.max(pointer.file_id);
		}
		vlog1.sync().unwrap();

		// Verify we created at least 5 files
		let unique_file_ids: std::collections::HashSet<_> =
			all_pointers.iter().map(|(p, _, _)| p.file_id).collect();
		assert!(
			unique_file_ids.len() >= 5,
			"Should have created at least 5 VLog files, got {}",
			unique_file_ids.len()
		);

		// Add a final small entry that shouldn't fill the last file completely
		let final_key = b"final_small_entry";
		let final_value = vec![255u8; 20]; // Small entry
		let final_pointer = vlog1.append(final_key, &final_value).unwrap();
		all_pointers.push((final_pointer.clone(), final_key.to_vec(), final_value));
		highest_file_id = highest_file_id.max(final_pointer.file_id);

		vlog1.sync().unwrap();
		vlog1.close().unwrap();
	}

	// Phase 2: Restart VLog and verify it picks up the correct active writer
	{
		let vlog2 = VLog::new(Arc::clone(&opts)).unwrap();

		// Check that active_writer_id is set to the highest file ID
		let active_writer_id = vlog2.active_writer_id.load(Ordering::SeqCst);
		assert_eq!(
			active_writer_id, highest_file_id,
			"After restart, active writer ID should be set to the highest file ID"
		);

		// Check that writer is set up
		assert!(
			vlog2.writer.read().is_some(),
			"After restart, writer should be set up for the last file"
		);

		// Check that next_file_id is correct
		let next_file_id = vlog2.next_file_id.load(Ordering::SeqCst);
		assert_eq!(
			next_file_id,
			highest_file_id + 1,
			"Next file ID should be one past the highest existing file ID"
		);

		// Verify all previous data can still be read
		for (pointer, expected_key, expected_value) in &all_pointers {
			let retrieved = vlog2.get(pointer).unwrap();
			assert_eq!(
				*retrieved,
				*expected_value,
				"Should be able to read existing data for key {:?} after restart",
				String::from_utf8_lossy(expected_key)
			);
		}

		// Add new data and verify it goes to the correct file
		let new_key = b"new_data_after_restart";
		let new_value = vec![42u8; 100];
		let new_pointer = vlog2.append(new_key, &new_value).unwrap();
		vlog2.sync().unwrap();

		// New data should go to the highest file ID (since we set it up as active
		// writer)
		assert_eq!(
			new_pointer.file_id, highest_file_id,
			"New data after restart should go to the last existing file"
		);

		// Verify the new data can be read
		let retrieved_new = vlog2.get(&new_pointer).unwrap();
		assert_eq!(
			*retrieved_new, new_value,
			"Should be able to read new data added after restart"
		);

		// Add a lot more data to eventually trigger creation of a new file
		let mut more_pointers = Vec::new();
		for i in 0..20 {
			let key = format!("bulk_new_key_{:04}", i).into_bytes();
			let value = vec![(i % 256) as u8; 100]; // Large values to fill files
			let pointer = vlog2.append(&key, &value).unwrap();
			more_pointers.push((pointer, value));
		}
		vlog2.sync().unwrap();

		// Check if we eventually created a new file
		let has_new_file = more_pointers.iter().any(|(p, _)| p.file_id > highest_file_id);
		if has_new_file {
			// If we created a new file, verify the active writer ID updated
			let new_active_writer_id = vlog2.active_writer_id.load(Ordering::SeqCst);
			assert!(
				new_active_writer_id > highest_file_id,
				"Active writer ID should update to the new file"
			);
		}

		// Verify all data (original, restart-added, and bulk-added) can still be read
		for (pointer, expected_value) in &more_pointers {
			let retrieved = vlog2.get(pointer).unwrap();
			assert_eq!(*retrieved, *expected_value, "Should be able to read bulk-added data");
		}

		// Final verification: count total number of files
		let final_file_ids: std::collections::HashSet<_> = all_pointers
			.iter()
			.map(|(p, _, _)| p.file_id)
			.chain(std::iter::once(new_pointer.file_id))
			.chain(more_pointers.iter().map(|(p, _)| p.file_id))
			.collect();

		assert!(
			final_file_ids.len() >= 5,
			"Should maintain at least 5 VLog files throughout the test"
		);
	}
}

#[test(tokio::test)]
async fn test_vlog_writer_reopen_append_only_behavior() {
	let temp_dir = TempDir::new().unwrap();
	let vlog_max_file_size = 2048;
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		vlog_max_file_size,
		vlog_checksum_verification: VLogChecksumLevel::Full,
		..Default::default()
	});

	// Create a test file path
	let test_file_path = temp_dir.path().join("vlog_writer_test.log");
	let file_id = 10;

	let mut phase1_pointers = Vec::new();
	let mut phase1_data = Vec::new();

	// Phase 1: Create VLogWriter and write initial data
	{
		let mut writer1 = VLogWriter::new(
			&test_file_path,
			file_id,
			opts.vlog_max_file_size,
			CompressionType::None as u8,
		)
		.unwrap();

		// Write several entries in the first phase
		for i in 0..5 {
			let key = format!("phase1_key_{:02}", i).into_bytes();
			let value = vec![i as u8; 50 + i * 10]; // Variable-sized values
			let pointer = writer1.append(&key, &value).unwrap();
			phase1_pointers.push(pointer);
			phase1_data.push((key, value));
		}

		// Flush to ensure data is written
		writer1.sync().unwrap();

		// Get file size and last entry offset for verification
		let file_size_after_phase1 = std::fs::metadata(&test_file_path).unwrap().len();
		println!(
			"Phase 1: File size after writing {} entries: {} bytes",
			phase1_pointers.len(),
			file_size_after_phase1
		);

		// Verify all phase 1 entries have offsets < file size
		for (i, pointer) in phase1_pointers.iter().enumerate() {
			assert!(
				pointer.offset < file_size_after_phase1,
				"Phase 1 entry {} offset ({}) should be < file size ({})",
				i,
				pointer.offset,
				file_size_after_phase1
			);
		}
	} // writer1 is dropped here, file is closed

	let file_size_between_phases = std::fs::metadata(&test_file_path).unwrap().len();

	// Phase 2: Reopen the same file with a new VLogWriter and append more data
	let mut phase2_pointers = Vec::new();
	let mut phase2_data = Vec::new();
	{
		let mut writer2 = VLogWriter::new(
			&test_file_path,
			file_id,
			opts.vlog_max_file_size,
			CompressionType::None as u8,
		)
		.unwrap();

		// Verify the writer starts at the end of the existing file
		assert_eq!(
			writer2.current_offset, file_size_between_phases,
			"Writer should start at the end of existing file (offset: {}, file size: {})",
			writer2.current_offset, file_size_between_phases
		);

		// Write more entries in the second phase
		for i in 0..4 {
			let key = format!("phase2_key_{:02}", i).into_bytes();
			let value = vec![(100 + i) as u8; 40 + i * 5]; // Different pattern
			let pointer = writer2.append(&key, &value).unwrap();

			// CRITICAL: Verify new entries have offsets >= original file size
			assert!(pointer.offset >= file_size_between_phases,
					"Phase 2 entry {} offset ({}) should be >= original file size ({}), proving append not overwrite",
					i, pointer.offset, file_size_between_phases);

			phase2_pointers.push(pointer);
			phase2_data.push((key, value));
		}

		// Flush to ensure data is written
		writer2.sync().unwrap();

		// Verify file size increased
		let file_size_after_phase2 = std::fs::metadata(&test_file_path).unwrap().len();
		assert!(
			file_size_after_phase2 > file_size_between_phases,
			"File size should have increased (before: {}, after: {})",
			file_size_between_phases,
			file_size_after_phase2
		);

		println!(
			"Phase 2: File size grew from {} to {} bytes after adding {} entries",
			file_size_between_phases,
			file_size_after_phase2,
			phase2_pointers.len()
		);
	} // writer2 is dropped here

	// Phase 3: Verification - Read all data back using VLog to ensure no
	// corruption/overwriting
	{
		// Create a VLog instance that can read from our test file
		let vlog_dir = temp_dir.path().join("vlog");
		std::fs::create_dir_all(&vlog_dir).unwrap();

		// Copy our test file to the VLog directory with the expected naming convention
		let vlog_file_path = opts.vlog_file_path(file_id as u64);
		std::fs::copy(&test_file_path, &vlog_file_path).unwrap();

		let vlog = VLog::new(opts).unwrap();

		// Verify ALL phase 1 data can still be read (proving no overwrite occurred)
		for (i, (pointer, (_, expected_value))) in
			phase1_pointers.iter().zip(phase1_data.iter()).enumerate()
		{
			let retrieved = vlog.get(pointer).unwrap();
			assert_eq!(
				*retrieved, *expected_value,
				"Phase 1 entry {} should be readable after phase 2 writes",
				i
			);
		}

		// Verify ALL phase 2 data can be read correctly
		for (i, (pointer, (_, expected_value))) in
			phase2_pointers.iter().zip(phase2_data.iter()).enumerate()
		{
			let retrieved = vlog.get(pointer).unwrap();
			assert_eq!(*retrieved, *expected_value, "Phase 2 entry {} should be readable", i);
		}

		println!(
			"Verification passed: All {} phase 1 + {} phase 2 entries readable",
			phase1_pointers.len(),
			phase2_pointers.len()
		);
	}

	// Phase 4: Additional verification - Test a third reopen to ensure pattern
	// continues
	{
		let mut writer3 = VLogWriter::new(
			&test_file_path,
			file_id,
			vlog_max_file_size,
			CompressionType::None as u8,
		)
		.unwrap();
		let current_file_size = std::fs::metadata(&test_file_path).unwrap().len();

		// Verify writer3 starts at the current end of file
		assert_eq!(
			writer3.current_offset, current_file_size,
			"Third writer should start at current end of file"
		);

		// Add one more entry
		let final_key = b"final_verification_entry";
		let final_value = vec![255u8; 30];
		let final_pointer = writer3.append(final_key, &final_value).unwrap();

		// Verify this entry also has offset >= previous file size
		assert!(
			final_pointer.offset >= current_file_size,
			"Final entry offset ({}) should be >= file size before append ({})",
			final_pointer.offset,
			current_file_size
		);

		writer3.sync().unwrap();

		println!(
			"Phase 4: Successfully appended final entry at offset {} (file was {} bytes)",
			final_pointer.offset, current_file_size
		);
	}

	// Final verification: Check that offsets are strictly increasing across phases
	let all_offsets: Vec<u64> =
		phase1_pointers.iter().chain(phase2_pointers.iter()).map(|p| p.offset).collect();

	for i in 1..all_offsets.len() {
		assert!(
			all_offsets[i] > all_offsets[i - 1],
			"Offsets should be strictly increasing: offset[{}]={} should be > offset[{}]={}",
			i,
			all_offsets[i],
			i - 1,
			all_offsets[i - 1]
		);
	}
}

// #[test(tokio::test)]
// async fn test_vlog_gc_with_versioned_index_cleanup_integration() {
// 	use crate::clock::MockLogicalClock;
// 	use crate::compaction::leveled::Strategy;
// 	use crate::lsm::{CompactionOperations, TreeBuilder};

// 	// Create test environment
// 	let temp_dir = TempDir::new().unwrap();

// 	// Set up mock clock and retention period
// 	let mock_clock = Arc::new(MockLogicalClock::with_timestamp(1000));
// 	let retention_ns = 2000; // 2 seconds retention - very short for testing

// 	let opts = Options {
// 		clock: Arc::clone(&mock_clock) as Arc<dyn LogicalClock>,
// 		vlog_max_file_size: 950, /* Small files to force frequent rotations (like VLog
// 		                          * compaction test) */
// 		level_count: 2,             // Two levels for compaction strategy
// 		..Options::default()
// 	};

// 	// Create Tree with versioning enabled
// 	let tree = TreeBuilder::with_options(opts)
// 		.with_path(temp_dir.path().to_path_buf())
// 		.with_versioning(true, retention_ns)
// 		.build()
// 		.unwrap();

// 	// Insert multiple versions of the same key with LARGE values to fill VLog files
// 	let user_key = b"test_key";

// 	// Insert versions with large values to fill VLog files quickly
// 	for (i, ts) in [1000, 2000, 3000, 4000].iter().enumerate() {
// 		let value = format!("value_{}_large_data", i).repeat(50); // Large value to fill VLog
// 		let mut tx = tree.begin().unwrap();
// 		tx.set_at(user_key, value.as_bytes(), *ts).unwrap();
// 		tx.commit().await.unwrap();
// 		tree.flush().unwrap(); // Force flush after each version
// 	}

// 	// Insert and delete a different key to create stale entries
// 	{
// 		let mut tx = tree.begin().unwrap();
// 		tx.set_at(b"other_key", b"other_value_large_data".repeat(50), 5000).unwrap();
// 		tx.commit().await.unwrap();
// 		tree.flush().unwrap();

// 		// Delete other_key to make it stale
// 		let mut tx = tree.begin().unwrap();
// 		tx.delete(b"other_key").unwrap();
// 		tx.commit().await.unwrap();
// 		tree.flush().unwrap();
// 	}

// 	// Verify all versions exist using the public API
// 	let tx = tree.begin().unwrap();
// 	let mut end_key = user_key.to_vec();
// 	end_key.push(0);
// 	let version_count = count_history_versions_in_range(&tx, user_key.as_ref(), &end_key).unwrap();
// 	assert_eq!(version_count, 4, "Should have 4 versions before GC");

// 	drop(tx);

// 	// Advance time to make some versions expire
// 	mock_clock.set_time(6000);

// 	// Trigger LSM compaction first
// 	let strategy = Arc::new(Strategy::default());
// 	tree.core.compact(strategy).unwrap();

// 	// Check VLog file stats before GC
// 	if let Some(vlog) = &tree.core.vlog {
// 		let stats = vlog.get_all_file_stats().unwrap();
// 		for (file_id, total_size, discard_bytes, ratio) in stats {
// 			println!(
// 				"  File {}: total={}, discard={}, ratio={:.2}",
// 				file_id, total_size, discard_bytes, ratio
// 			);
// 		}
// 	}

// 	// Now run VLog garbage collection multiple times to process all files
// 	let mut all_deleted_files = Vec::new();

// 	loop {
// 		let deleted_files = tree.garbage_collect_vlog().await.unwrap();
// 		if deleted_files.is_empty() {
// 			break;
// 		}

// 		all_deleted_files.extend(deleted_files);
// 	}

// 	// Verify that some versions were cleaned up using the public API
// 	let tx = tree.begin().unwrap();
// 	let mut end_key = user_key.to_vec();
// 	end_key.push(0);
// 	let version_count_after =
// 		count_history_versions_in_range(&tx, user_key.as_ref(), &end_key).unwrap();

// 	// We should have at least some versions remaining
// 	assert!(version_count_after > 0, "Should have at least some versions remaining");

// 	// If GC deleted files, we should have fewer versions
// 	if !all_deleted_files.is_empty() {
// 		// We should have fewer versions after GC
// 		assert!(
// 			version_count_after < 6,
// 			"Should have fewer versions after GC deleted files. Before: 6, After: {}",
// 			version_count_after
// 		);
// 	}

// 	// Test specific timestamp queries to verify which versions were deleted
// 	let mut end_key = user_key.to_vec();
// 	end_key.push(0);
// 	let scan_at_ts1 =
// 		point_in_time_from_history_in_range(&tx, user_key.as_ref(), &end_key, 1000).unwrap();
// 	let scan_at_ts2 =
// 		point_in_time_from_history_in_range(&tx, user_key.as_ref(), &end_key, 2000).unwrap();
// 	let scan_at_ts3 =
// 		point_in_time_from_history_in_range(&tx, user_key.as_ref(), &end_key, 3000).unwrap();
// 	let scan_at_ts4 =
// 		point_in_time_from_history_in_range(&tx, user_key.as_ref(), &end_key, 4000).unwrap();
// 	let scan_at_ts5 =
// 		point_in_time_from_history_in_range(&tx, user_key.as_ref(), &end_key, 6000).unwrap();
// 	let scan_at_ts6 =
// 		point_in_time_from_history_in_range(&tx, user_key.as_ref(), &end_key, 6001).unwrap();

// 	// The key insight: VLog GC only processes files with high discard ratios
// 	// Files 0, 1, 2 had high discard ratios (0.97) and were processed
// 	// Files 3, 4, 5, 6 had zero discard ratios (0.00) and were NOT processed
// 	// This means versions in files 0, 1, 2 were cleaned up, but versions in files
// 	// 3+ were not

// 	// Verify that versions in processed files (1000, 2000, 3000) are NOT accessible
// 	assert_eq!(
// 		scan_at_ts1.len(),
// 		0,
// 		"Version at timestamp 1000 should be deleted (was in processed file)"
// 	);
// 	assert_eq!(
// 		scan_at_ts2.len(),
// 		0,
// 		"Version at timestamp 2000 should be deleted (was in processed file)"
// 	);
// 	assert_eq!(
// 		scan_at_ts3.len(),
// 		0,
// 		"Version at timestamp 3000 should be deleted (was in processed file)"
// 	);

// 	// Verify that recent versions (6000, 6001) are still accessible
// 	assert_eq!(scan_at_ts4.len(), 1, "Version at timestamp 4000 should still exist (recent)");
// 	assert_eq!(scan_at_ts5.len(), 1, "Version at timestamp 6000 should still exist (recent)");
// 	assert_eq!(scan_at_ts6.len(), 1, "Version at timestamp 6001 should still exist (recent)");
// }
