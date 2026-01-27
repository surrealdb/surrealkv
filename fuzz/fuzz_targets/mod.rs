// Shared helper module for fuzz targets

use std::cmp::Ordering;
use std::sync::Arc;

use surrealkv::comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use surrealkv::{InternalKey, InternalKeyKind, INTERNAL_KEY_SEQ_NUM_MAX};

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
	pub fn to_internal_key(&self) -> InternalKey {
		InternalKey::new(
			self.user_key.clone(),
			self.seq_num.min(INTERNAL_KEY_SEQ_NUM_MAX),
			self.kind,
			0,
		)
	}
}

/// Sorts and deduplicates entries using InternalKeyComparator
/// Ensures entries are in ascending order as required by BlockWriter
pub fn sort_and_deduplicate_entries(entries: &mut Vec<FuzzEntry>) {
	let user_cmp = Arc::new(BytewiseComparator::default());
	let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp));

	// Sort using encoded keys (bytes)
	entries.sort_by(|a, b| {
		let key_a = a.to_internal_key().encode();
		let key_b = b.to_internal_key().encode();
		internal_cmp.compare(&key_a, &key_b)
	});

	// Deduplicate using dedup_by which is more efficient
	entries.dedup_by(|a, b| {
		let key_a = a.to_internal_key().encode();
		let key_b = b.to_internal_key().encode();
		internal_cmp.compare(&key_a, &key_b) == Ordering::Equal
	});
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
