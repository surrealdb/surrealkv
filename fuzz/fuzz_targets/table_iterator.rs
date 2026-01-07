#![no_main]
use std::io::Write;
use std::ops::Bound;
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::comparator::{BytewiseComparator, Comparator};
use surrealkv::sstable::table::TableWriter;
use surrealkv::sstable::{InternalKey, InternalKeyKind};
use surrealkv::{InternalKeyRange, Options};
use tempfile::NamedTempFile;

#[path = "mod.rs"]
mod helpers;
use helpers::{sort_and_deduplicate_entries, to_internal_key_kind, FuzzEntry};

#[derive(Arbitrary, Debug)]
struct FuzzTableIterInput {
	entries: Vec<FuzzEntryRaw>,
	range: FuzzRange,
	operations: Vec<TableIterOp>,
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

#[derive(Arbitrary, Debug)]
struct FuzzRange {
	lower: Option<(Vec<u8>, bool)>, // (key, is_inclusive)
	upper: Option<(Vec<u8>, bool)>,
}

#[derive(Arbitrary, Debug)]
enum TableIterOp {
	Next,
	NextBack,
}

impl FuzzRange {
	fn to_internal_key_range(&self) -> InternalKeyRange {
		let lower = self
			.lower
			.as_ref()
			.map(|(key, inclusive)| {
				let ikey = InternalKey::new(key.clone(), 0, InternalKeyKind::Set, 0);
				if *inclusive {
					Bound::Included(ikey)
				} else {
					Bound::Excluded(ikey)
				}
			})
			.unwrap_or(Bound::Unbounded);

		let upper = self
			.upper
			.as_ref()
			.map(|(key, inclusive)| {
				let ikey = InternalKey::new(key.clone(), 0, InternalKeyKind::Set, 0);
				if *inclusive {
					Bound::Included(ikey)
				} else {
					Bound::Excluded(ikey)
				}
			})
			.unwrap_or(Bound::Unbounded);

		(lower, upper)
	}
}

fuzz_target!(|data: FuzzTableIterInput| {
	// Limit input size
	if data.entries.len() > 500 || data.operations.len() > 100 {
		return;
	}

	// Convert and sort entries
	let mut entries: Vec<FuzzEntry> = data.entries.iter().map(|e| e.to_fuzz_entry()).collect();
	sort_and_deduplicate_entries(&mut entries);

	if entries.is_empty() {
		return;
	}

	// Create options
	let opts = Arc::new(Options::default());

	// Create temp file for writing
	let mut temp_file = NamedTempFile::new().unwrap();
	let file_path = temp_file.path().to_path_buf();

	// Build table
	let mut writer = TableWriter::new(&mut temp_file, 1, opts.clone(), 0);

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

	let file_arc: Arc<dyn surrealkv::vfs::File> = Arc::new(file_data);

	let table =
		surrealkv::sstable::table::Table::new(1, opts.clone(), file_arc, file_size as u64).unwrap();

	// Convert range
	let range = data.range.to_internal_key_range();
	let table_arc = Arc::new(table);
	let mut iter = table_arc.iter(false, Some(range.clone()));

	// Track direction to avoid interleaving
	let mut direction: Option<bool> = None; // None = unset, true = forward, false = backward

	// Execute operations
	let mut results = Vec::new();
	for op in &data.operations {
		match op {
			TableIterOp::Next => {
				if direction == Some(false) {
					// Can't mix directions, skip
					continue;
				}
				direction = Some(true);
				if let Some(item) = iter.next() {
					if let Ok(kv) = item {
						results.push(kv);
					}
				}
			}
			TableIterOp::NextBack => {
				if direction == Some(true) {
					// Can't mix directions, skip
					continue;
				}
				direction = Some(false);
				if let Some(item) = iter.next_back() {
					if let Ok(kv) = item {
						results.push(kv);
					}
				}
			}
		}
	}

	// Verify all results satisfy range bounds
	let user_cmp = Arc::new(BytewiseComparator::default());
	for (key, _value) in &results {
		// Check lower bound
		match &range.0 {
			Bound::Included(start) => {
				assert!(
					Comparator::compare(user_cmp.as_ref(), &key.user_key, &start.user_key)
						!= std::cmp::Ordering::Less,
					"Result key should be >= lower bound"
				);
			}
			Bound::Excluded(start) => {
				assert!(
					Comparator::compare(user_cmp.as_ref(), &key.user_key, &start.user_key)
						== std::cmp::Ordering::Greater,
					"Result key should be > lower bound"
				);
			}
			Bound::Unbounded => {}
		}

		// Check upper bound
		match &range.1 {
			Bound::Included(end) => {
				assert!(
					Comparator::compare(user_cmp.as_ref(), &key.user_key, &end.user_key)
						!= std::cmp::Ordering::Greater,
					"Result key should be <= upper bound"
				);
			}
			Bound::Excluded(end) => {
				assert!(
					Comparator::compare(user_cmp.as_ref(), &key.user_key, &end.user_key)
						== std::cmp::Ordering::Less,
					"Result key should be < upper bound"
				);
			}
			Bound::Unbounded => {}
		}
	}

	// Verify ordering within results
	if direction == Some(true) {
		// Forward iteration should be ascending
		for i in 1..results.len() {
			let prev_key = &results[i - 1].0.user_key;
			let curr_key = &results[i].0.user_key;
			assert!(
				Comparator::compare(user_cmp.as_ref(), prev_key, curr_key)
					!= std::cmp::Ordering::Greater,
				"Forward iteration should be ascending"
			);
		}
	} else if direction == Some(false) {
		// Backward iteration should be descending
		for i in 1..results.len() {
			let prev_key = &results[i - 1].0.user_key;
			let curr_key = &results[i].0.user_key;
			assert!(
				Comparator::compare(user_cmp.as_ref(), prev_key, curr_key)
					!= std::cmp::Ordering::Less,
				"Backward iteration should be descending"
			);
		}
	}
});
