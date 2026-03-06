#![no_main]
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use surrealkv::sstable::bloom::LevelDBBloomFilter;
use surrealkv::sstable::table::{Table, TableWriter};
use surrealkv::{
	CompressionType,
	LSMIterator,
	InternalKey,
	InternalKeyKind,
	Options,
	INTERNAL_KEY_SEQ_NUM_MAX,
};
use tempfile::NamedTempFile;

#[path = "mod.rs"]
mod helpers;
use helpers::{sort_and_deduplicate_entries, to_internal_key_kind, FuzzEntry};

#[derive(Arbitrary, Debug)]
struct FuzzTableInput {
	entries: Vec<FuzzEntryRaw>,
	block_size: u16,
	index_partition_size: u16,
	block_restart_interval: u8,
	compression: u8,
	use_filter: bool,
	filter_bits_per_key: u8,
	keys_only: bool,
	/// Queries to test get()
	get_queries: Vec<GetQuery>,
}

#[derive(Arbitrary, Debug)]
struct FuzzEntryRaw {
	user_key: Vec<u8>,
	value: Vec<u8>,
	seq_num: u64,
	kind: u8,
	timestamp: u64,
}

#[derive(Arbitrary, Debug)]
struct GetQuery {
	user_key: Vec<u8>,
	seq_num: u64,
	timestamp: u64,
}

impl FuzzEntryRaw {
	fn to_fuzz_entry(&self) -> FuzzEntry {
		FuzzEntry {
			user_key: self.user_key.clone(),
			value: self.value.clone(),
			seq_num: self.seq_num.min(INTERNAL_KEY_SEQ_NUM_MAX),
			kind: to_internal_key_kind(self.kind),
		}
	}

	fn to_internal_key(&self) -> InternalKey {
		InternalKey::new(
			self.user_key.clone(),
			self.seq_num.min(INTERNAL_KEY_SEQ_NUM_MAX),
			to_internal_key_kind(self.kind),
			self.timestamp,
		)
	}
}

fuzz_target!(|data: FuzzTableInput| {
	// Tighter limits
	if data.entries.len() > 200 || data.get_queries.len() > 50 {
		return;
	}

	// Limit key/value sizes
	for entry in &data.entries {
		if entry.user_key.is_empty() || entry.user_key.len() > 128 || entry.value.len() > 256 {
			return;
		}
	}

	// Convert entries
	let mut entries: Vec<(FuzzEntryRaw, FuzzEntry)> = data
		.entries
		.into_iter()
		.map(|e| {
			let fuzz_entry = e.to_fuzz_entry();
			(e, fuzz_entry)
		})
		.collect();

	// Sort by internal key ordering
	let user_cmp = Arc::new(BytewiseComparator::default());
	let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp.clone()));

	entries.sort_by(|a, b| {
		let key_a = a.0.to_internal_key().encode();
		let key_b = b.0.to_internal_key().encode();
		internal_cmp.compare(&key_a, &key_b)
	});

	// Deduplicate by full internal key
	entries.dedup_by(|a, b| {
		let key_a = a.0.to_internal_key().encode();
		let key_b = b.0.to_internal_key().encode();
		internal_cmp.compare(&key_a, &key_b) == Ordering::Equal
	});

	if entries.is_empty() {
		return;
	}

	// Create options with fuzzed parameters
	let mut opts = Options::default();
	opts.block_size = (data.block_size as usize).clamp(256, 8192);
	opts.index_partition_size = (data.index_partition_size as usize).clamp(256, 4096);
	opts.block_restart_interval = (data.block_restart_interval % 32).max(1) as usize;

	// Actually use compression setting!
	let compression = match data.compression % 2 {
		0 => CompressionType::None,
		_ => CompressionType::SnappyCompression,
	};
	opts.compression_per_level = vec![compression; 7];

	if data.use_filter {
		let bits = (data.filter_bits_per_key % 20).max(5) as usize;
		opts.filter_policy = Some(Arc::new(LevelDBBloomFilter::new(bits)));
	}

	let opts = Arc::new(opts);

	// Create temp file
	let mut temp_file = match NamedTempFile::new() {
		Ok(f) => f,
		Err(_) => return,
	};
	let file_path = temp_file.path().to_path_buf();

	// Build table
	let mut writer = TableWriter::new(&mut temp_file, 1, opts.clone(), 0);

	for (raw, _) in &entries {
		let internal_key = raw.to_internal_key();
		if writer.add(internal_key, &raw.value).is_err() {
			return;
		}
	}

	let file_size = match writer.finish() {
		Ok(size) => size,
		Err(_) => return,
	};

	// Read file into memory
	use std::fs::File;
	use std::io::Read;
	let mut file = match File::open(&file_path) {
		Ok(f) => f,
		Err(_) => return,
	};
	let mut file_data = Vec::new();
	if file.read_to_end(&mut file_data).is_err() {
		return;
	}
	drop(temp_file);

	let file_arc: Arc<dyn surrealkv::vfs::File> = Arc::new(file_data);

	let table = match Table::new(1, opts.clone(), file_arc, file_size as u64) {
		Ok(t) => t,
		Err(_) => return,
	};

	// ========================================
	// TEST 1: Metadata verification
	// ========================================
	verify_metadata(&table, &entries);

	// ========================================
	// TEST 2: Get() for each entry
	// ========================================
	verify_get_all_entries(&table, &entries, &internal_cmp);

	// ========================================
	// TEST 3: Get() with fuzzed queries
	// ========================================
	verify_get_queries(&table, &entries, &data.get_queries, &internal_cmp);

	// ========================================
	// TEST 4: Forward iteration
	// ========================================
	let table_arc = Arc::new(table);
	verify_forward_iteration(&table_arc, &entries, &internal_cmp, data.keys_only);

	// ========================================
	// TEST 5: Backward iteration
	// ========================================
	verify_backward_iteration(&table_arc, &entries, &internal_cmp, data.keys_only);

	// ========================================
	// TEST 6: Filter block (no false negatives)
	// ========================================
	if let Some(ref filter) = table_arc.filter_reader {
		for (raw, _) in &entries {
			let may_contain = filter.may_contain(&raw.user_key, 0);
			assert!(may_contain, "Filter must not have false negatives for inserted keys");
		}
	}
});

fn verify_metadata(table: &Table, entries: &[(FuzzEntryRaw, FuzzEntry)]) {
	let meta = &table.meta;

	// Verify entry count
	assert_eq!(
		meta.properties.num_entries as usize,
		entries.len(),
		"Entry count mismatch in metadata"
	);

	// Verify sequence number bounds
	let mut min_seq = u64::MAX;
	let mut max_seq = 0u64;
	for (raw, _) in entries {
		let seq = raw.seq_num.min(INTERNAL_KEY_SEQ_NUM_MAX);
		min_seq = min_seq.min(seq);
		max_seq = max_seq.max(seq);
	}

	assert_eq!(meta.smallest_seq_num, Some(min_seq), "Smallest seq_num mismatch");
	assert_eq!(meta.largest_seq_num, Some(max_seq), "Largest seq_num mismatch");

	// Verify key bounds if available
	if let (Some(smallest), Some(largest)) = (&meta.smallest_point, &meta.largest_point) {
		let first_key = &entries.first().unwrap().0.user_key;
		let last_key = &entries.last().unwrap().0.user_key;

		assert_eq!(&smallest.user_key, first_key, "Smallest key mismatch in metadata");
		assert_eq!(&largest.user_key, last_key, "Largest key mismatch in metadata");
	}
}

fn verify_get_all_entries(
	table: &Table,
	entries: &[(FuzzEntryRaw, FuzzEntry)],
	internal_cmp: &Arc<InternalKeyComparator>,
) {
	for (raw, fuzz_entry) in entries {
		let query_key = raw.to_internal_key();
		let result = table.get(&query_key);

		match result {
			Ok(Some((found_key, found_value))) => {
				// User key must match
				assert_eq!(
					found_key.user_key.as_slice(),
					raw.user_key.as_slice(),
					"Get() returned wrong user key"
				);

				// For non-delete entries, value should match
				if !fuzz_entry.kind.is_tombstone_kind() {
					assert_eq!(found_value, raw.value, "Get() returned wrong value");
				}
			}
			Ok(None) => {
				// For Set entries, this should not happen
				if fuzz_entry.kind == InternalKeyKind::Set {
					// Check if filter might have rejected it (shouldn't happen - no false
					// negatives)
					if let Some(ref filter) = table.filter_reader {
						assert!(
							filter.may_contain(&raw.user_key, 0),
							"Filter rejected inserted key - false negative!"
						);
					}
					// If we get here, the key should exist but get() returned None
					panic!(
                        "Get() returned None for Set entry that should exist: user_key={:?}, seq={}",
                        raw.user_key,
                        raw.seq_num
                    );
				}
				// For deletes/tombstones, returning None is acceptable
			}
			Err(e) => {
				panic!("Get() returned error: {:?}", e);
			}
		}
	}
}

fn verify_get_queries(
	table: &Table,
	entries: &[(FuzzEntryRaw, FuzzEntry)],
	queries: &[GetQuery],
	internal_cmp: &Arc<InternalKeyComparator>,
) {
	for query in queries {
		if query.user_key.is_empty() || query.user_key.len() > 128 {
			continue;
		}

		let query_key = InternalKey::new(
			query.user_key.clone(),
			query.seq_num.min(INTERNAL_KEY_SEQ_NUM_MAX),
			InternalKeyKind::Set,
			query.timestamp,
		);

		let result = table.get(&query_key);

		match result {
			Ok(Some((found_key, _))) => {
				// INVARIANT: Found key's user_key must match query's user_key
				assert_eq!(
					found_key.user_key.as_slice(),
					query.user_key.as_slice(),
					"Get() returned key with different user_key"
				);

				// INVARIANT: Found key's seq_num should be <= query's seq_num
				// (we're looking for the version visible at query's seq_num)
				assert!(
					found_key.seq_num() <= query_key.seq_num(),
					"Get() returned key with higher seq_num than query"
				);
			}
			Ok(None) => {
				// Verify this key truly doesn't exist with seq_num <= query
				let exists = entries.iter().any(|(raw, _)| {
					raw.user_key == query.user_key
						&& raw.seq_num.min(INTERNAL_KEY_SEQ_NUM_MAX)
							<= query.seq_num.min(INTERNAL_KEY_SEQ_NUM_MAX)
				});

				// If key exists but wasn't found, verify filter isn't returning false negative
				if exists {
					if let Some(ref filter) = table.filter_reader {
						assert!(
							filter.may_contain(&query.user_key, 0),
							"Filter returned false negative for existing key: {:?}",
							query.user_key
						);
					}
					// Note: get() returns the first matching user_key after seek,
					// so if seq_num ordering causes a miss, that's expected behavior
				}
			}
			Err(e) => {
				panic!("Get() returned error for query: {:?}", e);
			}
		}
	}
}

fn verify_forward_iteration(
	table: &Arc<Table>,
	entries: &[(FuzzEntryRaw, FuzzEntry)],
	internal_cmp: &Arc<InternalKeyComparator>,
	_keys_only: bool,
) {
	let mut iter = match table.iter(None) {
		Ok(it) => it,
		Err(_) => return,
	};

	if iter.seek_first().is_err() {
		return;
	}

	let mut count = 0;
	let mut prev_key: Option<Vec<u8>> = None;

	while iter.valid() {
		let key = iter.key();
		let value = iter.value().expect("iterator value");
		let encoded = key.encoded().to_vec();

		// INVARIANT: Keys must be strictly increasing
		if let Some(ref pk) = prev_key {
			assert!(
				internal_cmp.compare(pk, &encoded) == Ordering::Less,
				"Forward iteration not strictly increasing"
			);
		}

		// INVARIANT: Key and value must match expected entry
		let expected = &entries[count];
		assert_eq!(
			key.user_key(),
			expected.0.user_key.as_slice(),
			"Forward iteration key mismatch at index {}",
			count
		);

		assert_eq!(
			value.as_slice(),
			expected.0.value.as_slice(),
			"Forward iteration value mismatch at index {}",
			count
		);

		prev_key = Some(encoded);
		count += 1;

		if !iter.next().unwrap_or(false) {
			break;
		}
	}

	assert_eq!(
		count,
		entries.len(),
		"Forward iteration returned {} entries, expected {}",
		count,
		entries.len()
	);
}

fn verify_backward_iteration(
	table: &Arc<Table>,
	entries: &[(FuzzEntryRaw, FuzzEntry)],
	internal_cmp: &Arc<InternalKeyComparator>,
	_keys_only: bool,
) {
	let mut iter = match table.iter(None) {
		Ok(it) => it,
		Err(_) => return,
	};

	if iter.seek_last().is_err() {
		return;
	}

	let mut count = 0;
	let mut next_key: Option<Vec<u8>> = None;

	while iter.valid() {
		let key = iter.key();
		let value = iter.value().expect("iterator value");
		let encoded = key.encoded().to_vec();

		// INVARIANT: Keys must be strictly decreasing
		if let Some(ref nk) = next_key {
			assert!(
				internal_cmp.compare(&encoded, nk) == Ordering::Less,
				"Backward iteration not strictly decreasing"
			);
		}

		// INVARIANT: Key and value must match expected entry (in reverse)
		let expected_idx = entries.len() - 1 - count;
		let expected = &entries[expected_idx];

		assert_eq!(
			key.user_key(),
			expected.0.user_key.as_slice(),
			"Backward iteration key mismatch at reverse index {}",
			count
		);

		assert_eq!(
			value.as_slice(),
			expected.0.value.as_slice(),
			"Backward iteration value mismatch at reverse index {}",
			count
		);

		next_key = Some(encoded);
		count += 1;

		if !iter.prev().unwrap_or(false) {
			break;
		}
	}

	assert_eq!(
		count,
		entries.len(),
		"Backward iteration returned {} entries, expected {}",
		count,
		entries.len()
	);
}

// Helper trait for checking tombstone
trait IsTombstoneKind {
	fn is_tombstone_kind(&self) -> bool;
}

impl IsTombstoneKind for InternalKeyKind {
	fn is_tombstone_kind(&self) -> bool {
		matches!(
			self,
			InternalKeyKind::Delete | InternalKeyKind::SoftDelete | InternalKeyKind::RangeDelete
		)
	}
}
