//! What the open holds the last segment of the log to: the values its records point to are in the
//! value log.
//!
//! A commit that waited for a sync made the value log and the log durable up to its record, and a
//! rotation of the log and the flush of a memtable do the same for what lies before them. So the
//! records of the last segment are the only ones whose values the open has to look for. A record
//! whose value is not in the value log ends the log there: it and the records after it were not
//! acknowledged as durable.
//!
//! A value can be missing for two reasons. A power cut loses any of the pages of the value log
//! that were written but not synced, in any order, so it can lose the value of one record and keep
//! the value of a later one: a hole in values that were never synced, which nothing acknowledged
//! as durable points to. Or bytes of the value log that were synced are gone, which a power cut
//! does not do. The value log keeps no record of how far it was synced, but the segment does, in
//! its sync marks (`RecordType::SyncMark`): an fsync of the segment that covered a record came
//! after the values of that record were made durable, see `SyncGate::sync`, so a mark that claims
//! a synced length at or past the end of a record says that its values were synced.
//!
//! The open reads the records of that segment before it replays them, and holds the entry that
//! each pointer names to what the pointer says, see [`VLog::check_entry`]. The segment ends at the
//! first record that fails, as far as the recovery mode lets it:
//!
//! * `AbsoluteConsistency` fails the open, and names the segment and the offset of the record.
//! * `SalvageValidPrefix` ends the segment before that record.
//! * `TolerateTornTail` does the same, unless a record after it points to a value that is in the
//!   log and the record that failed may have been synced: a hole in values that may have been
//!   synced, which it refuses. A segment that holds marks says how far it was synced, so a hole in
//!   what no mark covers is cut. One without marks says nothing, so a hole in it is refused as
//!   before marks existed: a sealed segment, and one whose head holds no mark, of a binary that
//!   writes none. One whose first page a power cut lost has no record left to check: recovery cuts
//!   it whole, as it was never synced (`classify_wal_damage_in`). A sync that completed after the
//!   last mark was written is not told either, so a value that it made durable and that is gone
//!   since is cut, not refused: a power cut cannot leave that, only a disk that loses synced bytes
//!   can.
//!
//! An encrypted log is held to the same rule: the records of its segment are authenticated when
//! they are read, and an entry of a sealed file is held to its length and to the checksum of what
//! is stored, which is what can be told without the key.
//!
//! The records that are kept are made durable before the open starts another segment: first the
//! files that their pointers name, see [`TailCheck::make_durable`], and then the segment itself,
//! with every other segment that the open replays.

use std::collections::BTreeSet;
use std::path::Path;

use super::reader::Reader;
use super::recovery::{scan_after_in, segment_holds_marks_in};
use super::{directory_present, Error as WalError, SegmentRef, WalContext};
use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::vfs::Fs;
use crate::vlog::{VLog, ValuePointer};
use crate::WalRecoveryMode;

/// Where the last segment of the log ends, once the values that its records point to were looked
/// for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TailEnd {
	/// Every record is kept.
	Whole,
	/// The segment ends before the record `record` (the first is 0), at byte `offset`. That record
	/// and the ones after it, `dropped` in all, are left out.
	Cut {
		record: usize,
		offset: usize,
		dropped: usize,
	},
}

/// The check of the last segment of the log against the value log.
pub(crate) struct TailCheck<'a> {
	vlog: &'a VLog,
	/// The one buffer that every entry is read into.
	buf: Vec<u8>,
	/// The files that the pointers of the records that are kept name.
	files: BTreeSet<u32>,
	/// The files that the pointers of the record that is looked at name.
	record_files: Vec<u32>,
	/// Whether the record that failed may have been synced, once it was asked: asked only when a
	/// value is missing and a later one is there, and of one record per segment.
	synced: Option<bool>,
}

/// The last segment of the log in `wal_dir`, if the replay reads it: one that is not below
/// `min_wal_number`, which is in tables.
pub(crate) fn last_segment(
	fs: &dyn Fs,
	wal_dir: &Path,
	min_wal_number: u64,
) -> Result<Option<SegmentRef>> {
	if !directory_present(fs, wal_dir)? {
		return Ok(None);
	}
	let mut segments = SegmentRef::read_segments_from_directory_in(fs, wal_dir, Some("wal"))?;
	Ok(segments.pop().filter(|segment| segment.id >= min_wal_number))
}

impl<'a> TailCheck<'a> {
	pub(crate) fn new(vlog: &'a VLog) -> Self {
		Self {
			vlog,
			buf: Vec::new(),
			files: BTreeSet::new(),
			record_files: Vec::new(),
			synced: None,
		}
	}

	/// Whether `segment` may have been synced up to `end`: a mark of it says so, or it holds no
	/// marks to say otherwise. A mark claims no more than its own offset, so one that claims `end`
	/// lies at or past it.
	fn synced_to(
		&mut self,
		context: &WalContext,
		segment: &SegmentRef,
		end: usize,
	) -> Result<bool> {
		if let Some(synced) = self.synced {
			return Ok(synced);
		}
		let fs = &*context.fs;
		let synced = context.is_sealed()
			|| !segment_holds_marks_in(fs, &segment.file_path)?
			|| scan_after_in(fs, &segment.file_path, end as u64)?
				.highest_mark
				.is_some_and(|synced| synced >= end as u64);
		Ok(*self.synced.insert(synced))
	}

	/// The first pointer of `batch` whose entry is not in the value log, if there is one. The
	/// files of the pointers that precede it are in `record_files`.
	fn first_missing(&mut self, batch: &Batch) -> Result<Option<ValuePointer>> {
		self.record_files.clear();
		for pointer in batch.value_pointers() {
			if !self.vlog.check_entry(&pointer, &mut self.buf)? {
				return Ok(Some(pointer));
			}
			self.record_files.push(pointer.file_id);
		}
		Ok(None)
	}

	/// Whether an entry that a pointer of `batch` names is in the value log.
	fn holds_a_value(&mut self, batch: &Batch) -> Result<bool> {
		for pointer in batch.value_pointers() {
			if self.vlog.check_entry(&pointer, &mut self.buf)? {
				return Ok(true);
			}
		}
		Ok(false)
	}

	/// Reads the records of `segment`, the last one of the log, and says where it ends.
	///
	/// A read that stops before the end of the segment, because the segment is damaged, ends the
	/// check there: the replay that follows meets the same damage and repairs it as it always
	/// did, and what was read is what is kept.
	pub(crate) fn run(
		&mut self,
		context: &WalContext,
		segment: &SegmentRef,
		mode: WalRecoveryMode,
	) -> Result<TailEnd> {
		self.files.clear();
		self.synced = None;

		let file = context.fs.open_read(&segment.file_path)?;
		let mut reader = Reader::with_context(file, None, segment.id, context);
		let segment_id = segment.id as usize;

		// Where the record that is read begins: the end of the one before it.
		let mut start = 0;
		let mut record = 0;
		// The record that failed, where it begins, and where it ends.
		let mut failed: Option<(usize, usize, usize)> = None;
		let mut dropped = 0;
		loop {
			let (data, end) = match reader.read() {
				Ok(read) => read,
				// The end of the segment, or damage that the replay that follows meets again and
				// repairs as it always did.
				Err(WalError::Corruption(_)) => break,
				Err(WalError::IO(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
				Err(e) => return Err(e.into()),
			};
			let end = end as usize;
			let batch = Batch::decode(data)?;
			match failed {
				None => {
					if let Some(missing) = self.first_missing(&batch)? {
						let place = format!(
							"the value that record {record} points to is not in the value log \
							 (file {}, offset {})",
							missing.file_id, missing.offset
						);
						if mode == WalRecoveryMode::AbsoluteConsistency {
							return Err(Error::wal_corruption(segment_id, start, place));
						}
						failed = Some((record, start, end));
						dropped = 1;
					} else {
						self.files.extend(self.record_files.iter().copied());
					}
				}
				Some((first, offset, first_end)) => {
					dropped += 1;
					if mode == WalRecoveryMode::TolerateTornTail
						&& self.holds_a_value(&batch)?
						&& self.synced_to(context, segment, first_end)?
					{
						return Err(Error::Other(format!(
							"WAL segment {:020}.wal at offset {offset}: the value that record {first} \
							 points to is not in the value log, a record after it points to one \
							 that is, and a sync mark says that the segment was synced past record \
							 {first}. The log is not cut automatically: configure \
							 WalRecoveryMode::SalvageValidPrefix to end it at that record.",
							segment.id
						)));
					}
				}
			}
			start = end;
			record += 1;
		}

		Ok(match failed {
			None => TailEnd::Whole,
			Some((record, offset, _)) => TailEnd::Cut {
				record,
				offset,
				dropped,
			},
		})
	}

	/// Makes durable what the records that are kept point to: the files of the value log that
	/// their pointers name. Nothing is synced if no kept record points into the value log. Called
	/// once the segment has been cut if it was to be, and before the segment itself is synced, so
	/// that no record of it is on disk without its value: a segment that is not the last one is not
	/// checked when the database is opened again.
	pub(crate) fn make_durable(&self) -> Result<()> {
		self.vlog.sync_files(&self.files)
	}
}
