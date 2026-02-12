#![no_main]
use std::cmp::Ordering;
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use surrealkv::sstable::table::{Table, TableWriter};
use surrealkv::{
	LSMIterator,
	InternalKey,
	InternalKeyKind,
	Options,
	INTERNAL_KEY_SEQ_NUM_MAX,
};
use tempfile::NamedTempFile;

#[path = "mod.rs"]
mod helpers;
use helpers::to_internal_key_kind;

#[derive(Arbitrary, Debug)]
struct FuzzSeekInput {
	entries: Vec<FuzzEntryRaw>,
	seek_operations: Vec<SeekOp>,
}

#[derive(Arbitrary, Debug, Clone)]
struct FuzzEntryRaw {
	user_key: Vec<u8>,
	value: Vec<u8>,
	seq_num: u64,
	kind: u8,
}

#[derive(Arbitrary, Debug)]
enum SeekOp {
	SeekToFirst,
	SeekToLast,
	Seek {
		user_key: Vec<u8>,
		seq_num: u64,
	},
	SeekToEntry(u8),
	SeekBeforeFirst,
	SeekAfterLast,
	SeekBetween(u8, u8),
	AdvanceAfterSeek(u8),
	PrevAfterSeek(u8),
}

impl FuzzEntryRaw {
	fn to_internal_key(&self) -> InternalKey {
		InternalKey::new(
			self.user_key.clone(),
			self.seq_num.min(INTERNAL_KEY_SEQ_NUM_MAX),
			to_internal_key_kind(self.kind),
			0,
		)
	}
}

fuzz_target!(|data: FuzzSeekInput| {
	// Limits
	if data.entries.len() > 100 || data.seek_operations.len() > 50 {
		return;
	}

	// Validate entries
	for entry in &data.entries {
		if entry.user_key.is_empty() || entry.user_key.len() > 64 || entry.value.len() > 128 {
			return;
		}
	}

	let user_cmp = Arc::new(BytewiseComparator::default());
	let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp.clone()));

	// Convert and sort - store (raw_entry, internal_key) pairs
	let mut entries: Vec<(FuzzEntryRaw, InternalKey)> = data
		.entries
		.into_iter()
		.map(|e| {
			let internal_key = e.to_internal_key();
			(e, internal_key)
		})
		.collect();

	entries.sort_by(|a, b| internal_cmp.compare(&a.1.encode(), &b.1.encode()));

	entries.dedup_by(|a, b| internal_cmp.compare(&a.1.encode(), &b.1.encode()) == Ordering::Equal);

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

	for (raw, key) in &entries {
		if writer.add(key.clone(), &raw.value).is_err() {
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

	// Execute seek operations
	for op in &data.seek_operations {
		let mut iter = match table.iter(None) {
			Ok(it) => it,
			Err(_) => return,
		};

		match op {
			SeekOp::SeekToFirst => {
				if iter.seek_to_first().is_ok() && iter.valid() {
					let key = iter.key();
					assert_eq!(
						key.user_key(),
						entries[0].0.user_key.as_slice(),
						"SeekToFirst should land on first entry"
					);
				}
			}

			SeekOp::SeekToLast => {
				if iter.seek_to_last().is_ok() && iter.valid() {
					let key = iter.key();
					assert_eq!(
						key.user_key(),
						entries.last().unwrap().0.user_key.as_slice(),
						"SeekToLast should land on last entry"
					);
				}
			}

			SeekOp::Seek {
				user_key,
				seq_num,
			} => {
				if user_key.is_empty() || user_key.len() > 64 {
					continue;
				}

				let seek_key = InternalKey::new(
					user_key.clone(),
					(*seq_num).min(INTERNAL_KEY_SEQ_NUM_MAX),
					InternalKeyKind::Set,
					0,
				);
				let encoded = seek_key.encode();

				if iter.seek(&encoded).is_ok() && iter.valid() {
					let found_encoded = iter.key().encoded().to_vec();

					// INVARIANT: Found key >= seek key
					assert!(
						internal_cmp.compare(&found_encoded, &encoded) != Ordering::Less,
						"Seek should find key >= target"
					);

					// INVARIANT: Should be first such key
					let expected_idx = entries.iter().position(|(_, k)| {
						internal_cmp.compare(&k.encode(), &encoded) != Ordering::Less
					});

					if let Some(idx) = expected_idx {
						assert_eq!(
							found_encoded,
							entries[idx].1.encode(),
							"Seek should find first key >= target"
						);
					}
				}
			}

			SeekOp::SeekToEntry(idx) => {
				let entry_idx = (*idx as usize) % entries.len();
				let target = &entries[entry_idx].1; // Now this is InternalKey
				let encoded = target.encode();

				if iter.seek(&encoded).is_ok() && iter.valid() {
					let found = iter.key();
					assert_eq!(
						found.user_key(),
						target.user_key.as_slice(),
						"Seek to exact entry should find it"
					);
				}
			}

			SeekOp::SeekBeforeFirst => {
				let seek_key =
					InternalKey::new(vec![], INTERNAL_KEY_SEQ_NUM_MAX, InternalKeyKind::Set, 0);

				if iter.seek(&seek_key.encode()).is_ok() && iter.valid() {
					let key = iter.key();
					assert_eq!(
						key.user_key(),
						entries[0].0.user_key.as_slice(),
						"Seek before first should land on first"
					);
				}
			}

			SeekOp::SeekAfterLast => {
				let mut after_key = entries.last().unwrap().0.user_key.clone();
				after_key.push(0xFF);

				let seek_key = InternalKey::new(after_key, 0, InternalKeyKind::Set, 0);

				if iter.seek(&seek_key.encode()).is_ok() {
					if iter.valid() {
						let found = iter.key().encoded();
						assert!(
							internal_cmp.compare(found, &seek_key.encode()) != Ordering::Less,
							"If valid after seek past end, key must be >= target"
						);
					}
				}
			}

			SeekOp::SeekBetween(idx1, idx2) => {
				let i1 = (*idx1 as usize) % entries.len();
				let i2 = (*idx2 as usize) % entries.len();

				if i1 >= i2 || i2 - i1 < 2 {
					continue;
				}

				let key1 = &entries[i1].0.user_key;
				let key2 = &entries[i1 + 1].0.user_key;

				if let Some(between) = create_key_between(key1, key2) {
					let seek_key = InternalKey::new(
						between,
						INTERNAL_KEY_SEQ_NUM_MAX,
						InternalKeyKind::Set,
						0,
					);

					if iter.seek(&seek_key.encode()).is_ok() && iter.valid() {
						let found = iter.key();
						let found_encoded = found.encoded();

						assert!(
							internal_cmp.compare(found_encoded, &seek_key.encode())
								!= Ordering::Less,
							"Seek between should find key >= target"
						);
					}
				}
			}

			SeekOp::AdvanceAfterSeek(idx) => {
				let entry_idx = (*idx as usize) % entries.len();
				let target = &entries[entry_idx].1; // InternalKey

				if iter.seek(&target.encode()).is_ok() && iter.valid() {
					if iter.next().unwrap_or(false) && iter.valid() {
						if entry_idx + 1 < entries.len() {
							let expected_next = &entries[entry_idx + 1].1; // InternalKey
							let found = iter.key();
							assert_eq!(
								found.encoded(),
								expected_next.encode().as_slice(),
								"Advance after seek should go to next entry"
							);
						}
					}
				}
			}

			SeekOp::PrevAfterSeek(idx) => {
				let entry_idx = (*idx as usize) % entries.len();
				let target = &entries[entry_idx].1; // InternalKey

				if iter.seek(&target.encode()).is_ok() && iter.valid() {
					if iter.prev().unwrap_or(false) && iter.valid() {
						if entry_idx > 0 {
							let expected_prev = &entries[entry_idx - 1].1; // InternalKey
							let found = iter.key();
							assert_eq!(
								found.encoded(),
								expected_prev.encode().as_slice(),
								"Prev after seek should go to previous entry"
							);
						}
					}
				}
			}
		}
	}
});

fn create_key_between(a: &[u8], b: &[u8]) -> Option<Vec<u8>> {
	for (i, (&byte_a, &byte_b)) in a.iter().zip(b.iter()).enumerate() {
		if byte_a < byte_b {
			if byte_a < byte_b - 1 {
				let mut result = a[..=i].to_vec();
				result[i] = byte_a + 1;
				return Some(result);
			}
		}
	}

	if a.len() < b.len() {
		let mut result = a.to_vec();
		result.push(0x00);
		return Some(result);
	}

	None
}
