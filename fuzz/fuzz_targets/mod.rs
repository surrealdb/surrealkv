// Shared helper module for fuzz targets

use std::sync::Arc;

use surrealkv::comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use surrealkv::sstable::{InternalKey, InternalKeyKind};

/// Creates an encoded InternalKey for fuzzing
pub fn make_internal_key(user_key: &[u8], seq_num: u64, kind: InternalKeyKind) -> Vec<u8> {
	InternalKey::new(user_key.to_vec(), seq_num, kind, 0).encode()
}

/// Represents a fuzzed entry with all necessary fields
#[derive(Debug, Clone)]
pub struct FuzzEntry {
	pub user_key: Vec<u8>,
	pub value: Vec<u8>,
	pub seq_num: u64,
	pub kind: InternalKeyKind,
}

impl FuzzEntry {
	pub fn to_internal_key(&self) -> Vec<u8> {
		make_internal_key(&self.user_key, self.seq_num, self.kind)
	}
}

/// Sorts and deduplicates entries using InternalKeyComparator
/// Ensures entries are in ascending order as required by BlockWriter
pub fn sort_and_deduplicate_entries(entries: &mut Vec<FuzzEntry>) {
	let user_cmp = Arc::new(BytewiseComparator::default());
	let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp));

	// Sort by encoded internal key
	entries.sort_by(|a, b| {
		let key_a = a.to_internal_key();
		let key_b = b.to_internal_key();
		internal_cmp.compare(&key_a, &key_b)
	});

	// Deduplicate: keep first entry for each unique user_key+seq_num combination
	let mut i = 0;
	while i < entries.len() {
		let mut j = i + 1;
		while j < entries.len() {
			let key_i = entries[i].to_internal_key();
			let key_j = entries[j].to_internal_key();
			if internal_cmp.compare(&key_i, &key_j) == std::cmp::Ordering::Equal {
				entries.remove(j);
			} else {
				j += 1;
			}
		}
		i += 1;
	}
}

/// Converts arbitrary key kind to InternalKeyKind
pub fn to_internal_key_kind(kind: u8) -> InternalKeyKind {
	match kind % 8 {
		0 => InternalKeyKind::Delete,
		1 => InternalKeyKind::SoftDelete,
		2 => InternalKeyKind::Set,
		3 => InternalKeyKind::Merge,
		4 => InternalKeyKind::LogData,
		5 => InternalKeyKind::RangeDelete,
		6 => InternalKeyKind::Replace,
		7 => InternalKeyKind::Separator,
		_ => InternalKeyKind::Set,
	}
}
