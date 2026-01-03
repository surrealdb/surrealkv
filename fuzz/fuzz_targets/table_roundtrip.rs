#![no_main]
use std::io::Write;
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::sstable::table::TableWriter;
use surrealkv::sstable::{InternalKey, InternalKeyKind};
use surrealkv::Options;
use tempfile::NamedTempFile;

#[path = "mod.rs"]
mod helpers;
use helpers::{sort_and_deduplicate_entries, to_internal_key_kind, FuzzEntry};

#[derive(Arbitrary, Debug)]
struct FuzzTableInput {
	entries: Vec<FuzzEntryRaw>,
	block_size: u16,
	index_partition_size: u16,
	use_compression: bool,
	use_filter: bool,
}

#[derive(Arbitrary, Debug)]
struct FuzzEntryRaw {
	user_key: Vec<u8>,
	value: Vec<u8>,
	seq_num: u64,
	kind: u8,
}

impl FuzzEntryRaw {
	fn to_fuzz_entry(&self) -> FuzzEntry {
		FuzzEntry {
			user_key: self.user_key.clone(),
			value: self.value.clone(),
			seq_num: self.seq_num,
			kind: to_internal_key_kind(self.kind),
		}
	}
}

fuzz_target!(|data: FuzzTableInput| {
	// Limit input size
	if data.entries.len() > 500 {
		return;
	}

	// Convert and sort entries
	let mut entries: Vec<FuzzEntry> = data.entries.iter().map(|e| e.to_fuzz_entry()).collect();
	sort_and_deduplicate_entries(&mut entries);

	if entries.is_empty() {
		return;
	}

	// Create options with fuzzed parameters
	let mut opts = Options::default();
	opts.block_size = (data.block_size as usize).max(256).min(65536);
	opts.index_partition_size = (data.index_partition_size as usize).max(256).min(16384);

	if data.use_filter {
		opts.filter_policy = Some(Arc::new(surrealkv::sstable::bloom::LevelDBBloomFilter::new(10)));
	}

	let opts = Arc::new(opts);

	// Create temp file for writing
	let mut temp_file = NamedTempFile::new().unwrap();
	let file_path = temp_file.path().to_path_buf();

	// Build table
	let mut writer = TableWriter::new(
		&mut temp_file,
		1, // table id
		opts.clone(),
		0, // target level
	);

	for entry in &entries {
		let internal_key = InternalKey::new(entry.user_key.clone(), entry.seq_num, entry.kind, 0);
		writer.add(internal_key, &entry.value).unwrap();
	}

	let file_size = writer.finish().unwrap();

	// Read table back
	use std::fs::File;
	use std::io::Read;
	let mut file = File::open(&file_path).unwrap();
	let mut file_data = Vec::new();
	file.read_to_end(&mut file_data).unwrap();
	drop(temp_file);

	// Create in-memory file for reading
	let file_arc: Arc<dyn surrealkv::vfs::File> = Arc::new(file_data);

	let table =
		surrealkv::sstable::table::Table::new(1, opts.clone(), file_arc, file_size as u64).unwrap();

	// Verify all entries via get()
	for entry in &entries {
		let internal_key = InternalKey::new(entry.user_key.clone(), entry.seq_num, entry.kind, 0);

		let result = table.get(&internal_key).unwrap();

		if entry.kind == InternalKeyKind::Delete
			|| entry.kind == InternalKeyKind::SoftDelete
			|| entry.kind == InternalKeyKind::RangeDelete
		{
			// Deletes may not return values
			continue;
		}

		if let Some((found_key, found_value)) = result {
			assert_eq!(
				found_key.user_key.as_slice(),
				entry.user_key.as_slice(),
				"User key mismatch"
			);
			assert_eq!(found_value, entry.value, "Value mismatch");
		}
	}

	// Test forward iteration
	let table_arc = Arc::new(table);
	let mut iter = table_arc.iter(false, None);
	let mut iterated_entries = Vec::new();

	for kv in (&mut iter).flatten() {
		iterated_entries.push(kv);
	}

	// Verify iteration order and completeness
	assert_eq!(iterated_entries.len(), entries.len(), "Iteration should return all entries");

	for (i, (iter_key, iter_value)) in iterated_entries.iter().enumerate() {
		let expected = &entries[i];
		assert_eq!(
			iter_key.user_key.as_slice(),
			expected.user_key.as_slice(),
			"Iteration key mismatch at index {}",
			i
		);
		assert_eq!(iter_value, &expected.value, "Iteration value mismatch at index {}", i);
	}

	// Test reverse iteration (if supported)
	let mut rev_iter = table_arc.iter(false, None);
	let mut rev_entries = Vec::new();

	// Collect in reverse using next_back
	while let Some(item) = rev_iter.next_back() {
		if let Ok(kv) = item {
			rev_entries.push(kv);
		}
	}

	// Verify reverse order
	assert_eq!(rev_entries.len(), entries.len(), "Reverse iteration should return all entries");

	for (i, (rev_key, rev_value)) in rev_entries.iter().enumerate() {
		let expected_idx = entries.len() - 1 - i;
		let expected = &entries[expected_idx];
		assert_eq!(
			rev_key.user_key.as_slice(),
			expected.user_key.as_slice(),
			"Reverse iteration key mismatch at index {}",
			i
		);
		assert_eq!(rev_value, &expected.value, "Reverse iteration value mismatch at index {}", i);
	}

	// Test filter if enabled
	if let Some(ref filter_reader) = table_arc.filter_reader {
		for entry in &entries {
			let may_contain = filter_reader.may_contain(&entry.user_key, 0);
			assert!(may_contain, "Filter should contain all added keys (no false negatives)");
		}
	}
});
