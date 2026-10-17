//! Recovery of a sealed WAL: what is replayed, what is repaired, and what stops the open.
//!
//! A plaintext WAL has one kind of failure, damage, and one answer to it, to keep what comes before
//! the damage. A sealed WAL has others, and recovery sorts every failure into one of three before
//! it acts on any of them:
//!
//! * A tear: a physical record that is cut short, fails its checksum or is zeroed, with nothing
//!   that passes its checksum after it. It is what a crash during an append leaves at the end of
//!   the last segment that has records. The records before it are all authenticated and are kept,
//!   as for a plaintext WAL, and the segment is repaired by writing them again under a new salt.
//! * An integrity failure: a key, a header or a tag that says no, a record that passes its checksum
//!   and is not what a writer writes, or a record index that does not fit. Nothing about the shape
//!   of the data after it says what to keep, so nothing is kept, and nothing is touched: not
//!   repaired, not rewritten, not renamed, not deleted, in any recovery mode.
//! * An index gap: a segment of the sequence that is missing, damage that is followed by records
//!   that pass their checksum, or a segment with records after the one that is damaged. Data was
//!   lost from the middle, which a crash does not do. A hard error as well.
//!
//! The rule that follows from it is that a failure that has to do with a key is never a tear. A
//! wrong key, no key, another database, a header that was changed, all fail before any record is
//! looked at, and all fail the same way whatever the recovery mode is.

#![allow(
	clippy::disallowed_methods,
	reason = "recovery of a sealed log is reached only for an encrypted database, which a file system that is not std refuses"
)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::recovery::{apply_batch_with_oversized_fallback, DefaultReporter, ReplayResult};
use super::{
	list_segment_ids,
	segment_name,
	CompressionType,
	Error as WalError,
	Options,
	SegmentRef,
	WalContext,
};
use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::lsm::fsync_directory;
use crate::memtable::MemTable;
use crate::wal::manager::Wal;
use crate::wal::reader::Reader;

/// Refuses a file system that is not `std`: the replay and the repair of a sealed log use `std::fs`
/// (a hard link and a rename that replaces, which the seam does not have), and so does the check of
/// a segment before it is removed.
fn require_std(context: &WalContext) -> Result<()> {
	if context.fs.kind() != crate::vfs::FsKind::Std {
		return Err(super::sealed_needs_std().into());
	}
	Ok(())
}

/// How a segment ends, once everything in it that authenticates was read.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SegmentEnd {
	/// Where the segment is meant to end: at the end of its last record.
	Clean,
	/// With a write that was cut short: the offset of the last record that was whole, and what was
	/// found after it.
	Torn {
		offset: usize,
		message: String,
	},
}

/// What is in one sealed segment.
#[derive(Debug)]
pub(crate) struct DecodedSegment {
	pub(crate) id: u64,
	pub(crate) max_seq: u64,
	pub(crate) batches: Vec<Batch>,
	pub(crate) end: SegmentEnd,
}

/// Decodes the sealed segment `id` in `path`: every batch that authenticates, and how the segment
/// ends. A failure that is not a tear is the error of the call, and nothing of the segment is
/// returned.
pub(crate) fn decode_segment(context: &WalContext, path: &Path, id: u64) -> Result<DecodedSegment> {
	let file = context.fs.open_read(path)?;
	let reporter = Box::new(DefaultReporter::new(id));
	let mut reader = Reader::with_context(file, Some(reporter), id, context);

	let mut batches = Vec::new();
	let mut max_seq = 0u64;
	let mut last_whole = 0usize;
	let end = loop {
		match reader.read() {
			Ok((record, offset)) => {
				last_whole = offset as usize;
				let batch = Batch::decode(record)?;
				max_seq = max_seq.max(batch.get_highest_seq_num());
				batches.push(batch);
			}
			Err(WalError::Corruption(err)) => {
				break SegmentEnd::Torn {
					offset: last_whole,
					message: format!("Corrupted WAL record: {err}"),
				};
			}
			Err(WalError::IO(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
				break SegmentEnd::Clean;
			}
			Err(err) => return Err(err.into()),
		}
	};
	Ok(DecodedSegment {
		id,
		max_seq,
		batches,
		end,
	})
}

/// The error for a segment that is missing from the sequence.
fn missing_segment(id: u64) -> Error {
	WalError::IndexGap {
		segment_id: id,
		offset: 0,
	}
	.into()
}

/// Replays a sealed WAL: the segments from `min_wal_number` to the last, one memtable for each as
/// for a plaintext one.
///
/// Reads only. The numbers of the segments must have no gap, since a segment that is not there
/// held records, and every segment must authenticate. A tear is reported as `WalCorruption`, which
/// the caller may repair, only if it is in the last segment that has records: every segment after
/// it must be whole and hold none.
pub(crate) fn replay(
	context: &WalContext,
	wal_dir: &Path,
	min_wal_number: u64,
	arena_size: usize,
) -> Result<ReplayResult> {
	// The sealed log is read and repaired through `std::fs`, below this point: on another file
	// system it would find no segment and take the log for empty.
	require_std(context)?;
	if !wal_dir.exists() {
		return Ok((None, vec![]));
	}
	let ids = list_segment_ids(wal_dir, Some("wal"))?;
	let Some(&last) = ids.last() else {
		return Ok((None, vec![]));
	};
	if min_wal_number > last {
		tracing::debug!("All WAL segments are already flushed (last={last:020})");
		return Ok((None, vec![]));
	}

	// Every number from the first one that holds unflushed data to the last must be there.
	let from = ids.partition_point(|&id| id < min_wal_number);
	let needed = ids.get(from..).unwrap_or(&[]);
	if needed.first() != Some(&min_wal_number) {
		return Err(missing_segment(min_wal_number));
	}
	if let Some(gap) = needed.windows(2).find(|pair| pair[0].checked_add(1) != Some(pair[1])) {
		return Err(missing_segment(gap[0].saturating_add(1)));
	}

	let segments: Vec<SegmentRef> = needed
		.iter()
		.map(|&id| SegmentRef {
			file_path: wal_dir.join(segment_name(id, "wal")),
			id,
		})
		.collect();
	let decoded = crate::wal::parallel_recovery::decode_sealed_segments(context, &segments)?;

	// The first tear, and then nothing but whole segments without records.
	let mut tear: Option<(u64, usize, String)> = None;
	for segment in &decoded {
		if let Some((torn, offset, _)) = &tear {
			if !segment.batches.is_empty() || segment.end != SegmentEnd::Clean {
				tracing::error!(
					"WAL segment {torn:020} is damaged and segment {:020} follows it with data",
					segment.id
				);
				return Err(WalError::IndexGap {
					segment_id: *torn,
					offset: *offset as u64,
				}
				.into());
			}
		} else if let SegmentEnd::Torn {
			offset,
			message,
		} = &segment.end
		{
			tear = Some((segment.id, *offset, message.clone()));
		}
	}
	if let Some((segment_id, offset, message)) = tear {
		return Err(Error::wal_corruption(segment_id as usize, offset, message));
	}

	let mut max_seq = 0u64;
	let mut memtables: Vec<(Arc<MemTable>, bool, u64)> = Vec::new();
	for segment in decoded {
		if segment.batches.is_empty() {
			continue;
		}
		max_seq = max_seq.max(segment.max_seq);
		let mut current = Arc::new(MemTable::new(arena_size));
		for batch in &segment.batches {
			current = apply_batch_with_oversized_fallback(
				current,
				batch,
				arena_size,
				segment.id,
				&mut memtables,
			)?;
		}
		if !current.is_empty() {
			memtables.push((current, false, segment.id));
		}
	}
	Ok((Some(max_seq).filter(|&seq| seq > 0), memtables))
}

/// Repairs the sealed segment `segment_id`, which `replay` found to end in a tear, by writing the
/// batches before the tear again in a new segment of the same number, under a new salt, and putting
/// it in the place of the old one.
///
/// The new segment is made in a directory of its own, exclusively, with a header, and is complete
/// and durable before anything else happens. The old one is then kept under another name, a link to
/// the same file, and the new one takes its name in one rename, so that the number of the segment
/// is never without a file. The old segment is kept: it is the only copy of the tail that was cut,
/// and of whatever was in it that did not authenticate.
///
/// A segment that holds no batch is replaced by a segment that holds none, which keeps the numbers
/// of the segments without a gap. Nothing is deleted, and nothing is touched if anything fails
/// before the replacement: a failure that is not a tear is the error of the call, whatever it is.
pub(crate) fn repair(context: &WalContext, wal_dir: &Path, segment_id: usize) -> Result<()> {
	repair_keeping(context, wal_dir, segment_id, None).map(|_| ())
}

/// Ends the sealed segment `segment_id` after its first `batches` batches, as [`repair`] does at a
/// tear, for a segment that has no tear: the batches after them authenticate, and are left out.
/// Gives the name that the segment as it was is kept under.
pub(crate) fn keep_first_batches(
	context: &WalContext,
	wal_dir: &Path,
	segment_id: usize,
	batches: usize,
) -> Result<PathBuf> {
	repair_keeping(context, wal_dir, segment_id, Some(batches))
}

/// [`repair`], keeping at most `limit` batches if there is a limit. Gives the name that the old
/// segment is kept under.
fn repair_keeping(
	context: &WalContext,
	wal_dir: &Path,
	segment_id: usize,
	limit: Option<usize>,
) -> Result<PathBuf> {
	require_std(context)?;
	let id = segment_id as u64;
	let segment_path = wal_dir.join(segment_name(id, "wal"));
	if !segment_path.exists() {
		return Err(Error::Other(format!("WAL segment {id:020}.wal does not exist")));
	}

	// A repair that did not finish leaves its directory behind: it holds only what a repair wrote.
	let repair_dir = wal_dir.join("repair_temp");
	if repair_dir.exists() {
		fs::remove_dir_all(&repair_dir)?;
	}
	fs::create_dir_all(&repair_dir)?;

	let result = write_repaired(context, &segment_path, &repair_dir, id, limit)
		.and_then(|()| replace(wal_dir, &segment_path, &repair_dir.join(segment_name(id, "wal"))));
	fs::remove_dir_all(&repair_dir).ok();
	let kept = result?;

	tracing::info!("Repaired the sealed WAL segment {id:020} to the batches before its damage");
	Ok(kept)
}

/// Writes the batches of `segment_path` that authenticate, up to the first tear or the first
/// `limit` of them, to a new segment of the same number in `repair_dir`, and makes it durable.
fn write_repaired(
	context: &WalContext,
	segment_path: &Path,
	repair_dir: &Path,
	id: u64,
	limit: Option<usize>,
) -> Result<()> {
	let mut reader = Reader::with_context(context.fs.open_read(segment_path)?, None, id, context);
	let new_options = |compression: CompressionType| Options {
		context: context.clone(),
		compression_type: compression,
		..Options::default()
	};

	let mut repaired: Option<Wal> = None;
	let mut written = 0usize;
	loop {
		if limit == Some(written) {
			break;
		}
		match reader.read() {
			Ok((record, _)) => match repaired.as_mut() {
				Some(wal) => {
					wal.append(record)?;
					written += 1;
				}
				None => {
					// The new segment is compressed as this one is, which the reader knows once it
					// has read the header.
					let first = record.to_vec();
					let wal = Wal::open_with_min_log_number(
						repair_dir,
						id,
						new_options(reader.get_compression_type()),
					)?;
					repaired.insert(wal).append(&first)?;
					written += 1;
				}
			},
			// The tear: what comes before it is kept.
			Err(WalError::Corruption(err)) => {
				tracing::warn!("Stopped the repair of WAL segment {id:020} at the tear: {err}");
				break;
			}
			Err(WalError::IO(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
			// Anything else is not damage, and nothing is written for it.
			Err(err) => return Err(err.into()),
		}
	}

	let mut repaired = match repaired {
		Some(wal) => wal,
		None => {
			// No batch authenticated: a segment with a header and nothing else.
			let mut wal =
				Wal::open_with_min_log_number(repair_dir, id, new_options(CompressionType::None))?;
			wal.rotate_with(|| Ok(()))?;
			wal
		}
	};
	repaired.close()?;
	Ok(())
}

/// Puts `repaired` in the place of `segment_path`, which is kept under another name, and gives it.
fn replace(wal_dir: &Path, segment_path: &Path, repaired: &Path) -> Result<PathBuf> {
	let kept = unused_name(segment_path)?;
	fs::hard_link(segment_path, &kept)?;
	fs::rename(repaired, segment_path)?;
	fsync_directory(wal_dir)?;
	tracing::info!("The damaged segment is kept as {kept:?}");
	// The newest few copies of all segments stay, as for a log that is not sealed: here the copy
	// with the highest number is the newest of a segment.
	crate::wal::recovery::prune_quarantine_in(
		&*crate::vfs::default_fs(),
		wal_dir,
		crate::wal::recovery::QUARANTINE_COPIES_KEPT,
		&kept,
		true,
	);
	Ok(kept)
}

/// A name for the damaged copy of a segment: the name of the segment and `.corrupt`, and after it
/// the number that follows the highest of the copies that earlier repairs left (`.corrupt` is 0),
/// so that the number of a copy says how new it is, also when older ones were removed.
fn unused_name(segment_path: &Path) -> Result<PathBuf> {
	let base = segment_path.as_os_str().to_owned();
	let named = |number: u32| {
		let mut name = base.clone();
		name.push(".corrupt");
		if number > 0 {
			name.push(format!(".{number}"));
		}
		PathBuf::from(name)
	};
	let mut highest = None;
	for number in 0..1000u32 {
		if named(number).try_exists()? {
			highest = Some(number);
		}
	}
	match highest {
		None => Ok(named(0)),
		Some(number) if number + 1 < 1000 => Ok(named(number + 1)),
		Some(_) => {
			Err(Error::Other("too many damaged copies of a WAL segment are kept".to_string()))
		}
	}
}
