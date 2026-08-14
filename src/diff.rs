//! Branch diff: what a branch changed relative to the point it was forked at.
//!
//! A diff is read entirely from the branch's OWN components — its memtables and
//! its level set — because those hold exactly what it wrote and nothing its
//! ancestors did. What it is *not* is "everything the branch owns": detach
//! materializes inherited rows into the branch's own tables, so entries are
//! filtered by sequence against the fork anchor rather than assumed to be above
//! it. That filter is the difference between a diff and a lie.
//!
//! The result is sorted by key, one entry per key (the branch's newest write
//! wins), and tombstone-inclusive — a delete is a change, and it is the change a
//! merge most needs to be told about.

use std::ops::Bound;

use crate::error::{Error, Result};
use crate::snapshot::{KMergeIterator, Snapshot};
use crate::{InternalKeyKind, LSMIterator, Value};

/// What a branch did to one key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiffOp {
	/// The branch wrote this value.
	Set(Value),
	/// The branch removed the key and its history.
	Delete,
	/// The branch removed the key, keeping earlier versions readable as history.
	SoftDelete,
}

impl DiffOp {
	/// Whether this operation removes the key.
	pub fn is_delete(&self) -> bool {
		matches!(self, Self::Delete | Self::SoftDelete)
	}

	/// The written value, or `None` for either kind of delete.
	pub fn value(&self) -> Option<&Value> {
		match self {
			Self::Set(value) => Some(value),
			Self::Delete | Self::SoftDelete => None,
		}
	}
}

/// One key the branch changed, with the sequence and timestamp it changed at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiffEntry {
	pub key: Vec<u8>,
	pub op: DiffOp,
	/// Commit sequence of the branch's newest write to this key. Always above
	/// the fork anchor.
	pub seq: u64,
	pub timestamp: u64,
}

/// A branch's changes since it was forked.
///
/// Holds the snapshot the changes are read from, so the view stays fixed for as
/// long as this value lives — iterate it once or many times and the answer is
/// the same. [`BranchDiff::iter`] gives a streaming cursor;
/// [`BranchDiff::collect`] is the convenience for small diffs.
pub struct BranchDiff {
	snapshot: Snapshot,
	base: u64,
}

impl std::fmt::Debug for BranchDiff {
	/// The base only: the snapshot behind a diff is the whole read stack, which
	/// is neither printable nor what a caller wants to see.
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("BranchDiff").field("base_seq", &self.base).finish()
	}
}

impl BranchDiff {
	pub(crate) fn new(snapshot: Snapshot, base: u64) -> Self {
		Self {
			snapshot,
			base,
		}
	}

	/// The fork anchor every change is measured against.
	pub fn base_seq(&self) -> u64 {
		self.base
	}

	/// A streaming cursor over the changes, in key order.
	pub fn iter(&self) -> Result<DiffIter<'_>> {
		DiffIter::new(&self.snapshot, self.base, Bound::Unbounded, Bound::Unbounded)
	}

	/// The changes within a key range, in key order.
	///
	/// Bounded at the iterator rather than filtered afterwards, so a diff over a
	/// narrow range of a large branch reads a narrow range.
	pub fn iter_range(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<DiffIter<'_>> {
		DiffIter::new(&self.snapshot, self.base, lower, upper)
	}

	/// Every change, materialized. Fine for inspection and tests; large diffs
	/// should stream through [`BranchDiff::iter`].
	pub fn collect(&self) -> Result<Vec<DiffEntry>> {
		self.iter()?.collect()
	}
}

/// A cursor over one [`BranchDiff`], resumable via [`DiffIter::seek`].
pub struct DiffIter<'a> {
	merge: KMergeIterator<'a>,
	/// Fork anchor. Entries at or below it were inherited, not written here.
	base: u64,
	/// Newest key already emitted, so older versions of it are skipped.
	emitted: Vec<u8>,
	started: bool,
	/// Set by `seek`: the merge is already positioned, so the next step must not
	/// move past it.
	pending_position: bool,
	/// Set once the merge runs out. Tracked explicitly rather than inferred from
	/// `current`, which a consumer may have taken.
	exhausted: bool,
}

impl<'a> DiffIter<'a> {
	fn new(
		snapshot: &'a Snapshot,
		base: u64,
		lower: Bound<&[u8]>,
		upper: Bound<&[u8]>,
	) -> Result<Self> {
		let iter_state = snapshot.collect_iter_state()?;
		let range = crate::user_range_to_internal_range(lower, upper);
		Ok(Self {
			merge: KMergeIterator::new_from(iter_state, range),
			base,
			emitted: Vec::new(),
			started: false,
			pending_position: false,
			exhausted: false,
		})
	}

	/// Positions the cursor at the first change at or after `key`, so a consumer
	/// that stopped can resume without replaying what it already saw.
	///
	/// The entry at that position is returned by the next call to
	/// [`Iterator::next`].
	pub fn seek(&mut self, key: &[u8]) -> Result<()> {
		self.exhausted = false;
		let target = crate::InternalKey::new(
			key.to_vec(),
			crate::INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Max,
			0,
		);
		self.merge.seek(&target.encode())?;
		self.emitted.clear();
		self.started = true;
		self.pending_position = true;
		Ok(())
	}

	/// Walks to the next entry that is both above the base and the newest write
	/// to its key.
	fn advance(&mut self) -> Result<Option<DiffEntry>> {
		while self.merge.valid() {
			let key_ref = self.merge.key();
			let user_key = key_ref.user_key();

			// An older version of a key already emitted: the branch's newest
			// write to it is what a diff reports.
			if user_key == self.emitted.as_slice() {
				self.merge.next()?;
				continue;
			}

			// At or below the fork anchor: inherited, not written by this
			// branch. Reachable because detach materializes inherited rows into
			// the branch's own tables.
			if key_ref.seq_num() <= self.base {
				self.merge.next()?;
				continue;
			}

			let seq = key_ref.seq_num();
			let timestamp = key_ref.timestamp();
			let kind = key_ref.kind();
			self.emitted.clear();
			self.emitted.extend_from_slice(user_key);

			let op = match kind {
				InternalKeyKind::Delete => DiffOp::Delete,
				InternalKeyKind::SoftDelete => DiffOp::SoftDelete,
				// `Set` and `Replace` both leave a value behind; `Replace`
				// additionally drops earlier versions, which is a retention
				// effect the diff's newest-per-key result already reflects.
				InternalKeyKind::Set | InternalKeyKind::Replace => {
					DiffOp::Set(Value::from(self.merge.value_encoded()?.to_vec()))
				}
				other => {
					return Err(Error::Corruption(format!(
						"branch diff encountered unexpected key kind {other:?}"
					)))
				}
			};

			return Ok(Some(DiffEntry {
				key: self.emitted.clone(),
				op,
				seq,
				timestamp,
			}));
		}
		self.exhausted = true;
		Ok(None)
	}
}

impl Iterator for DiffIter<'_> {
	type Item = Result<DiffEntry>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.exhausted {
			return None;
		}
		// First use, or a `seek` that positioned the merge but has not yielded
		// yet: in both cases the merge is already on the right entry and must
		// not be stepped past it.
		if !self.started {
			self.started = true;
			if let Err(error) = self.merge.seek_first() {
				self.exhausted = true;
				return Some(Err(error));
			}
		} else if !self.pending_position {
			if let Err(error) = self.merge.next() {
				self.exhausted = true;
				return Some(Err(error));
			}
		}
		self.pending_position = false;
		match self.advance() {
			Ok(Some(entry)) => Some(Ok(entry)),
			Ok(None) => None,
			Err(error) => {
				self.exhausted = true;
				Some(Err(error))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// `DiffOp` is the vocabulary a consumer acts on, and the two deletes are
	/// the part that is easy to get wrong: they differ in what they do to
	/// *history*, and not at all in what they do to the current value.
	#[test]
	fn both_deletes_remove_the_key_and_neither_carries_a_value() {
		assert!(DiffOp::Delete.is_delete());
		assert!(DiffOp::SoftDelete.is_delete());
		assert!(!DiffOp::Set(b"v".to_vec()).is_delete());

		assert_eq!(DiffOp::Delete.value(), None);
		assert_eq!(DiffOp::SoftDelete.value(), None);
		assert_eq!(DiffOp::Set(b"v".to_vec()).value(), Some(&b"v".to_vec()));
	}

	/// A `Set` of empty bytes is a value, not an absence. Merge's decision table
	/// compares `Option<Value>`, so collapsing the two would turn "the source
	/// wrote an empty string" into "the source deleted it".
	#[test]
	fn an_empty_value_is_not_a_delete() {
		let empty = DiffOp::Set(Vec::new());
		assert!(!empty.is_delete());
		assert_eq!(empty.value(), Some(&Vec::new()));
	}
}
