#![no_main]
use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use surrealkv::sstable::table::{Table, TableWriter};
use surrealkv::{
	InternalIterator,
	InternalKey,
	InternalKeyKind,
	InternalKeyRange,
	Options,
	INTERNAL_KEY_SEQ_NUM_MAX,
};
use tempfile::NamedTempFile;

#[path = "mod.rs"]
mod helpers;
use helpers::{sort_and_deduplicate_entries, to_internal_key_kind, FuzzEntry};

#[derive(Arbitrary, Debug)]
struct FuzzRangeIterInput {
	entries: Vec<FuzzEntryRaw>,
	ranges: Vec<FuzzRange>,
	keys_only: bool,
}

#[derive(Arbitrary, Debug)]
struct FuzzEntryRaw {
	user_key: Vec<u8>,
	value: Vec<u8>,
	seq_num: u64,
	kind: u8,
}

#[derive(Arbitrary, Debug)]
struct FuzzRange {
	lower_key: Option<Vec<u8>>,
	lower_inclusive: bool,
	upper_key: Option<Vec<u8>>,
	upper_inclusive: bool,
	test_forward: bool,
	test_backward: bool,
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
			0,
		)
	}
}

impl FuzzRange {
	fn to_internal_key_range(&self) -> InternalKeyRange {
		let lower = match &self.lower_key {
			Some(key) if !key.is_empty() && key.len() <= 64 => {
				// For inclusive lower: use MAX seq_num (first version of key)
				// For exclusive lower: use 0 seq_num (after all versions of key)
				let seq = if self.lower_inclusive {
					INTERNAL_KEY_SEQ_NUM_MAX
				} else {
					0
				};
				let ikey = InternalKey::new(key.clone(), seq, InternalKeyKind::Set, 0);
				if self.lower_inclusive {
					Bound::Included(ikey)
				} else {
					Bound::Excluded(ikey)
				}
			}
			_ => Bound::Unbounded,
		};

		let upper = match &self.upper_key {
			Some(key) if !key.is_empty() && key.len() <= 64 => {
				// For inclusive upper: use 0 seq_num (last version of key)
				// For exclusive upper: use MAX seq_num (before all versions of key)
				let seq = if self.upper_inclusive {
					0
				} else {
					INTERNAL_KEY_SEQ_NUM_MAX
				};
				let ikey = InternalKey::new(key.clone(), seq, InternalKeyKind::Set, 0);
				if self.upper_inclusive {
					Bound::Included(ikey)
				} else {
					Bound::Excluded(ikey)
				}
			}
			_ => Bound::Unbounded,
		};

		(lower, upper)
	}

	// key_in_range stays the same - it only compares user_keys
	fn key_in_range(&self, user_key: &[u8], cmp: &BytewiseComparator) -> bool {
		let lower_ok = match &self.lower_key {
			Some(lower) if !lower.is_empty() => {
				let ord = cmp.compare(user_key, lower);
				if self.lower_inclusive {
					ord != Ordering::Less
				} else {
					ord == Ordering::Greater
				}
			}
			_ => true,
		};

		let upper_ok = match &self.upper_key {
			Some(upper) if !upper.is_empty() => {
				let ord = cmp.compare(user_key, upper);
				if self.upper_inclusive {
					ord != Ordering::Greater
				} else {
					ord == Ordering::Less
				}
			}
			_ => true,
		};

		lower_ok && upper_ok
	}

	fn expected_entries<'a>(
		&self,
		entries: &'a [(FuzzEntryRaw, FuzzEntry)],
		user_cmp: &BytewiseComparator,
	) -> Vec<&'a (FuzzEntryRaw, FuzzEntry)> {
		entries.iter().filter(|(raw, _)| self.key_in_range(&raw.user_key, user_cmp)).collect()
	}
}

fuzz_target!(|data: FuzzRangeIterInput| {
	// Limits
	if data.entries.len() > 100 || data.ranges.len() > 20 {
		return;
	}

	// Limit key/value sizes
	for entry in &data.entries {
		if entry.user_key.is_empty() || entry.user_key.len() > 64 || entry.value.len() > 128 {
			return;
		}
	}

	for range in &data.ranges {
		if let Some(k) = &range.lower_key {
			if k.len() > 64 {
				return;
			}
		}
		if let Some(k) = &range.upper_key {
			if k.len() > 64 {
				return;
			}
		}
	}

	let user_cmp = Arc::new(BytewiseComparator::default());
	let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp.clone()));

	// Convert and sort entries
	let mut entries: Vec<(FuzzEntryRaw, FuzzEntry)> = data
		.entries
		.into_iter()
		.map(|e| {
			let fuzz_entry = e.to_fuzz_entry();
			(e, fuzz_entry)
		})
		.collect();

	entries.sort_by(|a, b| {
		let key_a = a.0.to_internal_key().encode();
		let key_b = b.0.to_internal_key().encode();
		internal_cmp.compare(&key_a, &key_b)
	});

	entries.dedup_by(|a, b| {
		let key_a = a.0.to_internal_key().encode();
		let key_b = b.0.to_internal_key().encode();
		internal_cmp.compare(&key_a, &key_b) == Ordering::Equal
	});

	if entries.is_empty() {
		return;
	}

	// Build table
	let opts = Arc::new(Options::default());

	let mut temp_file = match NamedTempFile::new() {
		Ok(f) => f,
		Err(_) => return,
	};
	let file_path = temp_file.path().to_path_buf();

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

	// Read back
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
		Ok(t) => Arc::new(t),
		Err(_) => return,
	};

	// Test each range
	for range in &data.ranges {
		let ik_range = range.to_internal_key_range();
		let expected = range.expected_entries(&entries, &user_cmp);

		// ========================================
		// Forward iteration test
		// ========================================
		if range.test_forward {
			let mut iter = match table.iter(Some(ik_range.clone())) {
				Ok(it) => it,
				Err(_) => return,
			};

			if iter.seek_first().is_err() {
				return;
			}

			let mut results: Vec<(InternalKey, Vec<u8>)> = Vec::new();

			while iter.valid() {
				let key = iter.key().to_owned();
				let value = iter.value().to_vec();
				results.push((key, value));

				if !iter.next().unwrap_or(false) {
					break;
				}
			}

			// Verify count
			assert_eq!(
				results.len(),
				expected.len(),
				"Forward range iteration count mismatch: got {}, expected {}",
				results.len(),
				expected.len()
			);

			// Verify ordering (ascending)
			for i in 1..results.len() {
				let prev = &results[i - 1].0.user_key;
				let curr = &results[i].0.user_key;
				assert!(
					user_cmp.compare(prev, curr) != Ordering::Greater,
					"Forward iteration not ascending"
				);
			}

			// Verify all results satisfy bounds
			for (key, _) in &results {
				assert!(
					range.key_in_range(&key.user_key, &user_cmp),
					"Forward iteration returned key outside range"
				);
			}

			// Verify completeness - all expected entries are present
			for (raw, _) in &expected {
				let found =
					results.iter().any(|(k, _)| k.user_key.as_slice() == raw.user_key.as_slice());
				assert!(found, "Forward iteration missing expected key: {:?}", raw.user_key);
			}
		}

		// ========================================
		// Backward iteration test
		// ========================================
		if range.test_backward {
			let mut iter = match table.iter(Some(ik_range.clone())) {
				Ok(it) => it,
				Err(_) => return,
			};

			if iter.seek_last().is_err() {
				return;
			}

			let mut results: Vec<(InternalKey, Vec<u8>)> = Vec::new();

			while iter.valid() {
				let key = iter.key().to_owned();
				let value = iter.value().to_vec();
				results.push((key, value));

				if !iter.prev().unwrap_or(false) {
					break;
				}
			}

			// Verify count
			assert_eq!(results.len(), expected.len(), "Backward range iteration count mismatch");

			// Verify ordering (descending)
			for i in 1..results.len() {
				let prev = &results[i - 1].0.user_key;
				let curr = &results[i].0.user_key;
				assert!(
					user_cmp.compare(prev, curr) != Ordering::Less,
					"Backward iteration not descending"
				);
			}

			// Verify all results satisfy bounds
			for (key, _) in &results {
				assert!(
					range.key_in_range(&key.user_key, &user_cmp),
					"Backward iteration returned key outside range"
				);
			}
		}
	}
});
