use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tempfile::TempDir;
use test_log::test;

use crate::vlog::{
	VLog,
	VLogFileHeader,
	VLogWriter,
	ValueLocation,
	ValuePointer,
	BIT_VALUE_POINTER,
	SYNCED_VLOG_FILES,
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
	assert_eq!(vlog_location.encoded_size(), 1 + 1 + VALUE_POINTER_SIZE); // meta + version +
	                                                                      // pointer
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

		// Check that active_writer_id is set to the last file (file 1)
		let active_writer_id = vlog2.active_writer_id.load(Ordering::SeqCst);
		assert_eq!(
			active_writer_id, 1,
			"After restart, active writer ID should be set to the last file (1)"
		);

		// Check that writer is set up
		assert!(
			vlog2.writer.read().is_some(),
			"After restart, writer should be set up for the last file"
		);

		// Check that next_file_id is correct
		let next_file_id = vlog2.next_file_id.load(Ordering::SeqCst);
		assert_eq!(
			next_file_id, 2,
			"Next file ID should be 2 (one past the highest existing file)"
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

		// New data should go to the same file (file 1) since it has space
		assert_eq!(
			new_pointer.file_id, 1,
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
			assert_eq!(final_active_writer_id, 2, "Active writer ID should update to the new file");
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

#[test]
fn test_peek_pointer_payload() {
	use crate::vlog::{ValueLocation, ValuePointer};

	let pointer = ValuePointer::new(42, 1024, 10, 256, 0);
	let location = ValueLocation::with_pointer(pointer);
	let encoded = location.encode();

	let peeked = ValueLocation::peek_pointer_payload(&encoded);
	assert!(peeked.is_some(), "expected pointer payload");
	let decoded_ptr = ValuePointer::decode(peeked.unwrap()).unwrap();
	assert_eq!(decoded_ptr.file_id, 42);
	assert_eq!(decoded_ptr.offset, 1024);
	assert_eq!(decoded_ptr.value_size, 256);

	// Test with inline value: peek should return None
	let inline_loc = ValueLocation::with_inline_value(b"regular inline value".to_vec());
	let encoded_inline = inline_loc.encode();
	assert!(ValueLocation::peek_pointer_payload(&encoded_inline).is_none());
}

// ---------------------------------------------------------------------------
// a failed write, a rollover and an fsync
// ---------------------------------------------------------------------------

fn entry_bytes(key: &[u8], value: &[u8]) -> Vec<u8> {
	let mut bytes = Vec::new();
	bytes.extend_from_slice(&(key.len() as u32).to_be_bytes());
	bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
	bytes.extend_from_slice(key);
	bytes.extend_from_slice(value);
	let mut hasher = crc32fast::Hasher::new();
	hasher.update(key);
	hasher.update(value);
	bytes.extend_from_slice(&hasher.finalize().to_be_bytes());
	bytes
}

fn value_of(seed: u8, len: usize) -> Vec<u8> {
	(0..len).map(|i| seed.wrapping_add((i % 251) as u8)).collect()
}

fn header_len() -> usize {
	VLogFileHeader::new(0, 0, 0).encode().len()
}

/// The failure points a sweep over `total` bytes tries: the first and last bytes, the bytes
/// around each of `edges`, and a stride in between.
fn failure_points(total: usize, edges: &[usize]) -> Vec<usize> {
	let mut points: Vec<usize> = (0..12).chain(total.saturating_sub(12)..=total).collect();
	for edge in edges {
		points.extend(edge.saturating_sub(6)..=edge + 6);
	}
	points.extend((0..total).step_by(1_999));
	points.retain(|point| *point <= total);
	points.sort_unstable();
	points.dedup();
	points
}

/// A write that fails in the middle of an entry, wherever that is (in the buffer's flush of the
/// entries before it, or in the write of the entry itself), leaves exactly the entries that were
/// appended: the file and the buffer are cut back, the entries before it survive whole, and the
/// entries after it are where their pointers say.
#[test]
fn a_failed_append_leaves_exactly_the_entries_that_were_appended() {
	let dir = TempDir::new().unwrap();
	// (value length of the entry before the failing one, of the failing one)
	let shapes = [(100, 100), (8_000, 300), (6_000, 6_000), (6_000, 20_000), (20_000, 20_000)];
	let (mut failed_appends, mut failed_flushes) = (0, 0);
	for (shape, (before, failing)) in shapes.into_iter().enumerate() {
		let total = 8 + 5 + before + 4 + 8 + 5 + failing + 4;
		let edges = [8 + 5 + before + 4, 8192, 8192 + 13, total - 4];
		for point in failure_points(total, &edges) {
			let path = dir.path().join(format!("{shape}_{point}.vlog"));
			let mut writer =
				VLogWriter::new(&path, 1, 1 << 30, CompressionType::None as u8).unwrap();
			let mut expected = Vec::new();
			let mut pointers = Vec::new();
			let appends = [
				(&b"key-a"[..], value_of(1, before)),
				(&b"key-b"[..], value_of(2, failing)),
				(&b"key-c"[..], value_of(3, 700)),
			];
			writer.fail_writes_after(point);
			for (key, value) in &appends {
				match writer.append(key, value) {
					Ok(pointer) => {
						expected.push(entry_bytes(key, value));
						pointers.push(pointer);
					}
					Err(_) => failed_appends += 1,
				}
			}
			// Whatever is still buffered reaches the file, and a failed flush is retried.
			if writer.flush().is_err() {
				failed_flushes += 1;
				writer.flush().unwrap();
			}
			let after = writer.append(b"key-d", &value_of(4, 50)).unwrap();
			expected.push(entry_bytes(b"key-d", &value_of(4, 50)));
			pointers.push(after);
			writer.flush().unwrap();

			let bytes = std::fs::read(&path).unwrap();
			let stream: Vec<u8> = expected.concat();
			assert_eq!(
				&bytes[header_len()..],
				stream.as_slice(),
				"shape {shape}, failure after {point} bytes: the file is not exactly the entries \
				 that were appended"
			);
			assert_eq!(writer.current_offset, bytes.len() as u64, "shape {shape}, point {point}");
			for (pointer, entry) in pointers.iter().zip(&expected) {
				let at = pointer.offset as usize;
				assert_eq!(
					&bytes[at..at + entry.len()],
					entry.as_slice(),
					"shape {shape}, point {point}: a pointer does not point at its entry"
				);
			}
		}
	}
	assert!(failed_appends > 100, "the sweep must fail appends: {failed_appends}");
	assert!(failed_flushes > 10, "the sweep must fail flushes too: {failed_flushes}");
}

/// A failed append whose cut fails too poisons the writer, which takes no more, and the next
/// append to the log goes to a new file. What was appended before the failure reads back, and so
/// does the append after it.
#[test(tokio::test)]
async fn a_failed_cut_poisons_the_writer_and_the_next_append_goes_to_a_new_file() {
	let (vlog, _dir, _) = create_test_vlog(None);
	let first = vlog.append(b"key-a", &value_of(1, 300)).unwrap();
	{
		let mut guard = vlog.writer.write();
		let writer = guard.as_mut().unwrap();
		writer.fail_writes_after(100);
		writer.fail_next_cut_back();
	}
	assert!(vlog.writer.write().as_mut().unwrap().append(b"key-b", &value_of(2, 20_000)).is_err());
	assert!(
		vlog.writer.write().as_mut().unwrap().append(b"key-c", b"x").is_err(),
		"a writer whose file could not be cut back takes no append"
	);

	let third = vlog.append(b"key-d", &value_of(4, 300)).unwrap();
	assert_ne!(first.file_id, third.file_id, "the poisoned file is not written to again");
	assert_eq!(vlog.active_writer_id.load(Ordering::SeqCst), third.file_id);
	vlog.sync().unwrap();
	assert_eq!(vlog.get(&first).unwrap(), value_of(1, 300));
	assert_eq!(vlog.get(&third).unwrap(), value_of(4, 300));
}

fn synced_under(dir: &Path) -> Vec<PathBuf> {
	SYNCED_VLOG_FILES.lock().iter().filter(|path| path.starts_with(dir)).cloned().collect()
}

/// A vlog whose files hold two entries of 600 bytes before they roll over, with three entries
/// appended: two files.
fn vlog_across_a_rollover() -> (VLog, TempDir, Vec<(ValuePointer, Vec<u8>)>) {
	let (vlog, dir, _) = create_test_vlog(Some(Options {
		vlog_max_file_size: 1024,
		vlog_checksum_verification: VLogChecksumLevel::Full,
		..Default::default()
	}));
	let mut entries = Vec::new();
	for i in 0..3u8 {
		let value = value_of(i, 600);
		entries.push((vlog.append(format!("key-{i}").as_bytes(), &value).unwrap(), value));
	}
	assert_eq!(entries[0].0.file_id, entries[1].0.file_id);
	assert_ne!(entries[1].0.file_id, entries[2].0.file_id, "the third entry rolls the file over");
	(vlog, dir, entries)
}

/// The file the writer rolled over from is fsynced by the next sync, once, and not again by the
/// ones after it.
#[test(tokio::test)]
async fn a_sync_fsyncs_the_file_the_writer_rolled_over_from_once() {
	let (vlog, dir, entries) = vlog_across_a_rollover();
	let (old, new) = (vlog.vlog_file_path(entries[0].0.file_id), vlog.vlog_file_path(2));
	assert!(synced_under(dir.path()).is_empty(), "a rollover does not fsync by itself");

	vlog.sync().unwrap();
	let first = synced_under(dir.path());
	assert!(first.len() == 2 && first.contains(&old) && first.contains(&new), "{first:?}");
	vlog.sync().unwrap();
	assert_eq!(synced_under(dir.path())[2..], [new], "the second sync fsyncs the active file only");
	for (pointer, value) in &entries {
		assert_eq!(&vlog.get(pointer).unwrap(), value);
	}
}

/// Runs `f` on a thread of its own and waits for it: a call that blocks on a lock the caller
/// holds fails the test instead of hanging it.
fn on_another_thread<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
	let (done, wait) = std::sync::mpsc::channel();
	std::thread::spawn(move || {
		let _ = done.send(f());
	});
	wait.recv_timeout(Duration::from_secs(20)).expect("the call did not return: a lock is held")
}

/// A rollover that happens while a sync is between taking its fds and fsyncing them queues the
/// file again, for the bytes the sync may not cover. The sync must leave that entry in the
/// queue, so that the next one fsyncs the file once more.
#[test(tokio::test)]
async fn a_sync_leaves_a_file_that_rolled_over_while_it_ran_queued() {
	let (vlog, dir, _) = create_test_vlog(Some(Options {
		vlog_max_file_size: 1024,
		vlog_checksum_verification: VLogChecksumLevel::Full,
		..Default::default()
	}));
	let vlog = Arc::new(vlog);
	let first = vlog.append(b"key-0", &value_of(0, 600)).unwrap();
	let file = vlog.vlog_file_path(first.file_id);

	let appender = Arc::downgrade(&vlog);
	vlog.set_sync_gap(Some(Arc::new(move || {
		// Fills the file the sync is about to fsync, and rolls over from it.
		let appender = appender.upgrade().unwrap();
		on_another_thread(move || {
			appender.append(b"key-1", &value_of(1, 600)).unwrap();
			appender.append(b"key-2", &value_of(2, 600)).unwrap();
		});
	})));
	vlog.sync().unwrap();
	vlog.set_sync_gap(None);
	vlog.sync().unwrap();

	let synced = synced_under(dir.path());
	assert_eq!(
		synced.iter().filter(|path| **path == file).count(),
		2,
		"the file the writer rolled over from during a sync is fsynced by the next one: {synced:?}"
	);
}

/// A sync that runs while another has its fds and has not fsynced yet (an Immediate group's sync
/// during a memtable flush's) fsyncs the file the writer rolled over from itself: it does not
/// return, and acknowledge a commit, on the strength of an fsync that is still in flight in the
/// other.
#[test(tokio::test)]
async fn a_sync_that_overlaps_another_fsyncs_the_rolled_over_file_itself() {
	let (vlog, dir, entries) = vlog_across_a_rollover();
	let vlog = Arc::new(vlog);
	let (old, new) = (vlog.vlog_file_path(entries[0].0.file_id), vlog.vlog_file_path(2));
	let seen = Arc::new(Mutex::new(None));
	let entered = Arc::new(AtomicBool::new(false));
	let (overlapping, under) = (Arc::downgrade(&vlog), dir.path().to_path_buf());
	let hook_seen = Arc::clone(&seen);
	vlog.set_sync_gap(Some(Arc::new(move || {
		if entered.swap(true, Ordering::SeqCst) {
			return;
		}
		let (overlapping, under) = (overlapping.upgrade().unwrap(), under.clone());
		*hook_seen.lock().unwrap() = Some(on_another_thread(move || {
			let before = synced_under(&under).len();
			overlapping.sync().unwrap();
			synced_under(&under)[before..].to_vec()
		}));
	})));
	vlog.sync().unwrap();

	let seen = seen.lock().unwrap().take().expect("the second sync ran");
	assert!(
		seen.contains(&old),
		"the overlapping sync did not fsync the rolled over file: {seen:?}"
	);
	assert!(seen.contains(&new), "the overlapping sync did not fsync the active file: {seen:?}");
}

/// A file that is deleted is not fsynced afterwards.
#[test(tokio::test)]
async fn a_deleted_file_is_not_fsynced_by_the_next_sync() {
	let (vlog, dir, entries) = vlog_across_a_rollover();
	let (old, new) = (vlog.vlog_file_path(entries[0].0.file_id), vlog.vlog_file_path(2));
	vlog.cleanup_obsolete_files(2).unwrap();
	assert!(!old.exists());
	vlog.sync().unwrap();
	assert_eq!(synced_under(dir.path()), vec![new]);
}

/// A sync that fails leaves the file the writer rolled over from queued, so the next one
/// fsyncs it.
#[test(tokio::test)]
async fn a_failed_sync_leaves_the_rolled_over_file_queued() {
	let (vlog, dir, entries) = vlog_across_a_rollover();
	vlog.fail_next_sync();
	assert!(vlog.sync().is_err());
	assert!(synced_under(dir.path()).is_empty());

	vlog.sync().unwrap();
	let synced = synced_under(dir.path());
	assert!(synced.contains(&vlog.vlog_file_path(entries[0].0.file_id)), "{synced:?}");
	assert!(synced.contains(&vlog.vlog_file_path(2)), "{synced:?}");
}

/// A rollover whose flush of the old file fails is an error, and the old file's entries are not
/// lost: the writer stays where it is, and the next append rolls over after all.
#[test(tokio::test)]
async fn a_failed_flush_at_a_rollover_is_not_swallowed() {
	let (vlog, _dir, _) = create_test_vlog(Some(Options {
		vlog_max_file_size: 1024,
		vlog_checksum_verification: VLogChecksumLevel::Full,
		..Default::default()
	}));
	let values: Vec<Vec<u8>> = (0..3u8).map(|i| value_of(i, 600)).collect();
	let first = vlog.append(b"key-0", &values[0]).unwrap();
	let second = vlog.append(b"key-1", &values[1]).unwrap();
	assert_eq!(first.file_id, second.file_id);

	vlog.fail_writes_after(0);
	assert!(vlog.append(b"key-2", &values[2]).is_err(), "the flush of the old file failed");
	assert_eq!(vlog.active_writer_id.load(Ordering::SeqCst), first.file_id);
	assert!(!vlog.vlog_file_path(first.file_id + 1).exists(), "no new file was started");

	let third = vlog.append(b"key-2", &values[2]).unwrap();
	assert_ne!(third.file_id, first.file_id);
	vlog.sync().unwrap();
	for (pointer, value) in [(&first, &values[0]), (&second, &values[1]), (&third, &values[2])] {
		assert_eq!(&vlog.get(pointer).unwrap(), value);
	}
}

/// A deterministic generator, so a failing seed can be replayed.
struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		self.0 ^= self.0 << 13;
		self.0 ^= self.0 >> 7;
		self.0 ^= self.0 << 17;
		self.0
	}

	fn below(&mut self, n: usize) -> usize {
		(self.next() % n as u64) as usize
	}
}

/// Appends, write failures at any byte, poisoned writers, rollovers (also failed ones), flushes,
/// syncs and failed syncs in a random order, against a model:
/// * every file is its header followed by exactly the entries whose append returned `Ok`, each at
///   the offset its pointer names, and every value reads back;
/// * a sync that returns `Ok` has fsynced every file that was written to since the last one that
///   did.
#[test]
fn a_random_sequence_of_appends_failures_rollovers_and_syncs_matches_the_model() {
	let sizes = [0usize, 1, 40, 40, 300, 700, 700, 2_000, 4_000, 8_180, 8_192, 9_000, 15_000];
	let (mut failed_appends, mut failed_syncs, mut rollovers) = (0, 0, 0);
	for seed in 1..=4u64 {
		let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
		let (vlog, dir, _) = create_test_vlog(Some(Options {
			vlog_max_file_size: 20_000,
			vlog_checksum_verification: VLogChecksumLevel::Full,
			..Default::default()
		}));
		let mut files: BTreeMap<u32, Vec<u8>> = BTreeMap::new();
		let mut pointers: Vec<(ValuePointer, Vec<u8>)> = Vec::new();
		let mut dirty: BTreeSet<u32> = BTreeSet::new();

		let sync = |dirty: &mut BTreeSet<u32>| -> bool {
			let before = synced_under(dir.path()).len();
			if vlog.sync().is_err() {
				return false;
			}
			let fsynced = synced_under(dir.path())[before..].to_vec();
			for id in dirty.iter() {
				assert!(
					fsynced.contains(&vlog.vlog_file_path(*id)),
					"seed {seed}: a sync that succeeded did not fsync file {id}, written to \
					 since the last one that did: {fsynced:?}"
				);
			}
			dirty.clear();
			true
		};

		for step in 0..120usize {
			match rng.below(16) {
				0..=8 => {
					let key = format!("key-{step}").into_bytes();
					let value = value_of(step as u8, sizes[rng.below(sizes.len())]);
					let result = vlog.append(&key, &value);
					let active = vlog.active_writer_id.load(Ordering::SeqCst);
					if active != 0 {
						dirty.insert(active);
					}
					match result {
						Ok(pointer) => {
							let file = files.entry(pointer.file_id).or_default();
							assert_eq!(
								pointer.offset as usize,
								header_len() + file.len(),
								"seed {seed}, step {step}: the pointer is not where the entry goes"
							);
							file.extend(entry_bytes(&key, &value));
							dirty.insert(pointer.file_id);
							pointers.push((pointer, value));
						}
						Err(_) => failed_appends += 1,
					}
				}
				9 | 10 => {
					if vlog.active_writer_id.load(Ordering::SeqCst) != 0 {
						// Mostly a failure inside what is buffered, which is the hard case.
						let after = match rng.below(4) {
							0 => rng.below(40),
							1 => rng.below(700),
							2 => rng.below(9_000),
							_ => rng.below(20_000),
						};
						vlog.fail_writes_after(after);
					}
				}
				11 | 12 => {
					let _ = vlog.flush();
				}
				13 => {
					if !sync(&mut dirty) {
						failed_syncs += 1;
					}
				}
				14 => {
					if let Some(writer) = vlog.writer.write().as_mut() {
						writer.poison();
					}
				}
				_ => {
					if vlog.active_writer_id.load(Ordering::SeqCst) != 0 {
						vlog.fail_next_sync();
						if !sync(&mut dirty) {
							failed_syncs += 1;
						}
					}
				}
			}
		}

		// Whatever failpoint is still armed fires at most once more: retry until a sync takes.
		assert!(
			(0..8).any(|_| sync(&mut dirty)),
			"seed {seed}: no sync succeeded once the failpoints were spent"
		);

		rollovers += files.len().saturating_sub(1);
		for (id, expected) in &files {
			let bytes = std::fs::read(vlog.vlog_file_path(*id)).unwrap();
			assert_eq!(
				&bytes[header_len()..],
				expected.as_slice(),
				"seed {seed}: file {id} is not exactly the entries that were appended to it"
			);
		}
		for (pointer, value) in &pointers {
			assert_eq!(&vlog.get(pointer).unwrap(), value, "seed {seed}: {pointer:?}");
		}
	}
	assert!(failed_appends > 8, "the sequences must fail appends: {failed_appends}");
	assert!(failed_syncs > 8, "the sequences must fail syncs: {failed_syncs}");
	assert!(rollovers > 15, "the sequences must roll over: {rollovers}");
}

/// Appenders that roll the file over all the time, a thread that syncs and a thread that flushes,
/// at once: nothing deadlocks (the rollover takes the queue lock under the writer lock, the sync
/// takes it under the writer lock and again after it), every entry reads back, and every file
/// that holds one has been fsynced by the time the last sync returned.
#[test]
fn concurrent_appends_syncs_and_flushes_neither_deadlock_nor_lose_an_entry() {
	let (vlog, dir, _) = create_test_vlog(Some(Options {
		vlog_max_file_size: 16 * 1024,
		vlog_checksum_verification: VLogChecksumLevel::Full,
		..Default::default()
	}));
	let vlog = Arc::new(vlog);
	let done = Arc::new(AtomicBool::new(false));

	let appended = on_another_thread({
		let (vlog, done) = (Arc::clone(&vlog), Arc::clone(&done));
		move || {
			let appenders: Vec<_> = (0..3u8)
				.map(|thread| {
					let vlog = Arc::clone(&vlog);
					std::thread::spawn(move || {
						(0..150usize)
							.map(|i| {
								let key = format!("t{thread}-{i}").into_bytes();
								let seed = thread.wrapping_mul(31).wrapping_add(i as u8);
								let value = value_of(seed, 200 + i * 7);
								(vlog.append(&key, &value).unwrap(), value)
							})
							.collect::<Vec<_>>()
					})
				})
				.collect();
			let syncer = {
				let (vlog, done) = (Arc::clone(&vlog), Arc::clone(&done));
				std::thread::spawn(move || {
					let mut syncs = 0;
					while !done.load(Ordering::SeqCst) && syncs < 25 {
						vlog.sync().unwrap();
						syncs += 1;
					}
				})
			};
			let flusher = {
				let (vlog, done) = (Arc::clone(&vlog), Arc::clone(&done));
				std::thread::spawn(move || {
					while !done.load(Ordering::SeqCst) {
						vlog.flush().unwrap();
						std::thread::yield_now();
					}
				})
			};
			let appended: Vec<_> = appenders.into_iter().flat_map(|h| h.join().unwrap()).collect();
			done.store(true, Ordering::SeqCst);
			syncer.join().unwrap();
			flusher.join().unwrap();
			appended
		}
	});

	vlog.sync().unwrap();
	let synced = synced_under(dir.path());
	assert_eq!(appended.len(), 450);
	for (pointer, value) in &appended {
		assert_eq!(&vlog.get(pointer).unwrap(), value, "{pointer:?}");
		assert!(
			synced.contains(&vlog.vlog_file_path(pointer.file_id)),
			"file {} was never fsynced",
			pointer.file_id
		);
	}
}
