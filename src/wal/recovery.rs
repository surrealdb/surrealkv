use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::memtable::MemTable;
use crate::vfs::Fs;
use crate::wal::manager::Wal;
use crate::wal::reader::{Reader, Reporter};
use crate::wal::{
	calculate_crc32,
	directory_present,
	parse_sync_mark,
	sealed_needs_std,
	CompressionType,
	Error as WalError,
	RecordType,
	SegmentRef,
	WalContext,
	BLOCK_SIZE,
	HEADER_SIZE,
	SYNC_MARK_LEN,
};

/// Detailed summary of actions taken during WAL recovery.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalRecoveryReport {
	/// Number of WAL segments replayed during recovery.
	pub segments_replayed: usize,
	/// Number of valid records/batches replayed across all segments.
	pub records_replayed: usize,
	/// Number of intact records dropped due to repair/salvage.
	pub records_dropped: usize,
	/// Number of bytes trimmed from WAL segment(s).
	pub bytes_trimmed: u64,
	/// Quarantined backup files created before modifying corrupted segments.
	pub quarantined_files: Vec<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WalDamage {
	/// A torn tail at the end of the last segment with no valid records following it.
	TornTail {
		last_valid_offset: usize,
		file_size: u64,
	},
	/// Damage in the last segment, which holds sync marks, that intact records follow, none of
	/// whose marks claims that the damage was synced: what a power cut to bytes that were
	/// never synced leaves, as the pages of the file are written back in any order. Every record
	/// acknowledged as durable was synced before it was acknowledged, and so lies below the
	/// damage, which is cut off as a torn tail is, the intact records after it with it.
	UnsyncedTail {
		last_valid_offset: usize,
		intact_records_after: usize,
		file_size: u64,
	},
	/// Interior corruption: valid records follow the corruption, or corruption occurs in an older
	/// segment.
	InteriorCorruption {
		last_valid_offset: usize,
		intact_records_after: usize,
		is_last_segment: bool,
		file_size: u64,
	},
}

/// Default implementation of the Reporter trait for standard
/// recovery operations.
pub struct DefaultReporter {
	/// Count of corruption events encountered
	corruption_count: usize,
}

impl DefaultReporter {
	/// Creates a new DefaultReporter.
	pub fn new(_log_number: u64) -> Self {
		Self {
			corruption_count: 0,
		}
	}
}

impl Reporter for DefaultReporter {
	fn corruption(&mut self, bytes: usize, reason: &str, log_number: u64) {
		tracing::error!("Corruption in WAL {}: {} bytes lost - {}", log_number, bytes, reason);
		self.corruption_count += 1;
	}
}

/// Applies `batch` to `current_memtable`, handling the case where it doesn't fit.
///
/// On `Error::ArenaFull`:
/// - If `current_memtable` is already empty, the batch alone exceeds `arena_size` (this happens
///   when a live commit used the direct-to-L0 path for an oversized batch, see
///   `CoreInner::write_batch_direct_to_l0_sst`, and a crash then forces WAL replay of that same
///   batch). A dedicated memtable sized to fit exactly this batch is created and pushed to
///   `memtables` so recovery can still succeed, instead of failing hard.
/// - Otherwise, `current_memtable` is pushed to `memtables` as-is and a fresh,
///   `arena_size`-capacity memtable is created and the batch retried on it; if the batch still
///   doesn't fit on a fresh, empty memtable, the oversized-memtable fallback above is applied to it
///   instead.
///
/// Returns the memtable that should keep receiving subsequent batches for this segment
/// (either `current_memtable` unchanged, or a fresh `arena_size`-capacity replacement).
pub(crate) fn apply_batch_with_oversized_fallback(
	current_memtable: Arc<MemTable>,
	batch: &Batch,
	arena_size: usize,
	segment_id: u64,
	memtables: &mut Vec<(Arc<MemTable>, bool, u64)>,
) -> Result<Arc<MemTable>> {
	match current_memtable.add(batch) {
		Ok(()) => Ok(current_memtable),
		Err(Error::ArenaFull) => {
			if current_memtable.is_empty() {
				let needed = oversized_arena_size(batch)?;
				let oversized = Arc::new(MemTable::new(needed));
				oversized.add(batch)?;
				memtables.push((oversized, true, segment_id));
				return Ok(Arc::new(MemTable::new(arena_size)));
			}

			tracing::warn!(
				"WAL segment #{:020} exceeds single memtable capacity, splitting",
				segment_id
			);
			memtables.push((Arc::clone(&current_memtable), false, segment_id));
			let fresh = Arc::new(MemTable::new(arena_size));
			match fresh.add(batch) {
				Ok(()) => Ok(fresh),
				Err(Error::ArenaFull) => {
					let needed = oversized_arena_size(batch)?;
					let oversized = Arc::new(MemTable::new(needed));
					oversized.add(batch)?;
					memtables.push((oversized, true, segment_id));
					Ok(Arc::new(MemTable::new(arena_size)))
				}
				Err(e) => Err(e),
			}
		}
		Err(e) => Err(e),
	}
}

/// The arena that a batch too large for a memtable gets of its own: its estimate and some room. An
/// estimate that does not fit in an address (a 32-bit target) is an error, and not a wrapped size.
fn oversized_arena_size(batch: &Batch) -> Result<usize> {
	usize::try_from(batch.memtable_size_estimate())
		.ok()
		.and_then(|estimate| estimate.checked_add(4096))
		.ok_or_else(|| {
			Error::Other(format!(
				"a recovered batch of {} bytes does not fit in memory on this platform",
				batch.memtable_size_estimate()
			))
		})
}

/// Replays the Write-Ahead Log (WAL) to recover recent writes.
///
/// Creates one memtable per WAL segment, matching the original design where
/// each WAL segment corresponds to one memtable.
///
/// # Arguments
/// * `wal_dir` - Path to the WAL directory
/// * `min_wal_number` - Minimum WAL number to replay (older segments are skipped)
/// * `arena_size` - Size for each memtable arena
///
/// # Returns
/// * `Ok((Some(max_seq_num), memtables))` - Memtables with their WAL numbers
/// * `Ok((None, vec![]))` - No data recovered
/// * `Err(...)` - Error during replay
pub(crate) type ReplayResult = (Option<u64>, Vec<(Arc<MemTable>, bool, u64)>);
pub(crate) type ReplayInnerResult = (Option<u64>, Vec<(Arc<MemTable>, bool, u64)>, usize, usize);

/// Replays the WAL whose segments are as `context` says, and counts what it replayed.
pub(crate) fn replay_wal_inner_with(
	context: &WalContext,
	wal_dir: &Path,
	min_wal_number: u64,
	arena_size: usize,
) -> Result<ReplayInnerResult> {
	tracing::debug!("Starting WAL recovery from directory: {:?}", wal_dir);
	tracing::debug!(
		"WAL recovery parameters: min_wal_number={}, arena_size={}",
		min_wal_number,
		arena_size
	);

	// A sealed WAL is replayed by its own code, which fails on anything that is not a tear. It
	// does not count the batches it replays. That code reads through `std::fs`, so a file system
	// that is not `std` is refused for it: it would find no segment and take the log for empty.
	if context.is_sealed() {
		if context.fs.kind() != crate::vfs::FsKind::Std {
			return Err(sealed_needs_std().into());
		}
		let (max_seq, memtables) =
			crate::wal::sealed_recovery::replay(context, wal_dir, min_wal_number, arena_size)?;
		let segments =
			memtables.iter().map(|(_, _, id)| *id).collect::<std::collections::HashSet<_>>();
		return Ok((max_seq, memtables, 0, segments.len()));
	}

	let fs = &*context.fs;

	// Check if WAL directory exists. A directory that cannot be looked at is not one that is
	// absent: see `directory_present`.
	if !directory_present(fs, wal_dir)? {
		tracing::debug!("WAL directory does not exist, skipping recovery");
		return Ok((None, vec![], 0, 0));
	}

	// Every segment in the directory (looking for .wal files), in order, from one listing. A
	// listing that fails is an error whatever it says, "not found" included: the directory was
	// there a moment ago, and a listing that found one of its entries gone is not an empty log.
	let all_segments = SegmentRef::read_segments_from_directory_in(fs, wal_dir, Some("wal"))?;
	let (Some(first_segment), Some(last_segment)) = (all_segments.first(), all_segments.last())
	else {
		tracing::debug!("No WAL segments found, skipping recovery");
		return Ok((None, vec![], 0, 0));
	};
	let (first, last) = (first_segment.id, last_segment.id);

	// Determine the range of segments to replay
	let start_segment = std::cmp::max(first, min_wal_number);

	if start_segment > last {
		tracing::debug!(
			"All WAL segments already flushed (last={:020}, min_log_number={:020})",
			last,
			min_wal_number
		);
		return Ok((None, vec![], 0, 0));
	}

	tracing::debug!("Replaying WAL segments #{:020} to #{:020}", start_segment, last);

	// Track statistics
	let mut max_seq_num: u64 = 0;
	let mut total_batches_replayed = 0;
	let mut segments_processed = 0;

	// Collect memtables - one per WAL segment
	let mut memtables: Vec<(Arc<MemTable>, bool, u64)> = Vec::new();

	// Collect segments to replay in order
	let mut segments_to_replay: Vec<SegmentRef> = Vec::new();
	for segment_id in start_segment..=last {
		if let Some(seg) = all_segments.iter().find(|seg| seg.id == segment_id) {
			segments_to_replay.push((*seg).clone());
		}
	}

	// Use parallel WAL decoder if multiple segments exist
	if segments_to_replay.len() > 1 {
		return crate::wal::parallel_recovery::replay_segments_sync(
			context,
			&segments_to_replay,
			arena_size,
		);
	}

	// Process single segment in order
	for segment_id in start_segment..=last {
		// Find this segment in the list
		let segment = match all_segments.iter().find(|seg| seg.id == segment_id) {
			Some(seg) => seg,
			None => {
				tracing::warn!(
					"WAL segment #{:020} not found in range [{:020}..{:020}], skipping.",
					segment_id,
					start_segment,
					last
				);
				continue;
			}
		};

		tracing::debug!("Processing WAL segment #{:020}", segment_id);

		// Create a new memtable for this segment
		let mut current_memtable = Arc::new(MemTable::new(arena_size));

		// Open the segment file
		let file = fs.open_read(&segment.file_path)?;
		let reporter = Box::new(DefaultReporter::new(segment_id));
		let mut reader = Reader::with_context(file, Some(reporter), segment_id, context);

		let mut batches_in_segment = 0;
		let mut last_valid_offset = 0;

		// Process each record in this segment
		loop {
			match reader.read() {
				Ok((record_data, offset)) => {
					last_valid_offset = offset as usize;
					let batch = Batch::decode(record_data)?;
					let batch_highest_seq_num = batch.get_highest_seq_num();

					if batch_highest_seq_num > max_seq_num {
						max_seq_num = batch_highest_seq_num;
					}

					batches_in_segment += 1;

					tracing::debug!(
						"Replayed batch from WAL #{:020}: seq_num={}, entries={}, offset={}",
						segment_id,
						batch_highest_seq_num,
						batch.count(),
						offset
					);

					// Apply batch to current memtable, falling back to a dedicated
					// oversized memtable if it (or a fresh replacement) can't hold it.
					current_memtable = apply_batch_with_oversized_fallback(
						current_memtable,
						&batch,
						arena_size,
						segment_id,
						&mut memtables,
					)?;
				}
				Err(WalError::Corruption(err)) => {
					tracing::error!(
						"Corrupted WAL record detected in segment {:020} at offset {}: {}",
						segment_id,
						last_valid_offset,
						err
					);
					return Err(Error::wal_corruption(
						segment_id as usize,
						last_valid_offset,
						format!("Corrupted WAL record: {}", err),
					));
				}
				Err(WalError::IO(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
					break; // End of this segment
				}
				Err(err) => return Err(err.into()),
			}
		}

		// Save this segment's memtable if it has data
		if !current_memtable.is_empty() {
			memtables.push((current_memtable, false, segment_id));
		}

		if batches_in_segment > 0 {
			tracing::debug!(
				"Replayed {} batches from WAL segment #{:020}",
				batches_in_segment,
				segment_id
			);
			total_batches_replayed += batches_in_segment;
			segments_processed += 1;
		}
	}

	// Return results
	let result = if max_seq_num > 0 {
		Some(max_seq_num)
	} else {
		None
	};

	tracing::debug!(
		"WAL recovery complete: {} batches across {} segments, {} memtables created, max_seq_num={:?}",
		total_batches_replayed,
		segments_processed,
		memtables.len(),
		result
	);

	Ok((result, memtables, total_batches_replayed, segments_processed))
}

/// Replays a plaintext WAL.
#[cfg(test)]
pub(crate) fn replay_wal(
	wal_dir: &Path,
	min_wal_number: u64,
	arena_size: usize,
) -> Result<ReplayResult> {
	let (max_seq, memtables, _, _) =
		replay_wal_inner_with(&WalContext::plaintext(), wal_dir, min_wal_number, arena_size)?;
	Ok((max_seq, memtables))
}

/// Whether the file at `path` starts the way a sealed segment does: the type of the header record
/// or the start of the encryption header that is its payload. A WAL that is not sealed has no
/// business repairing such a file, whatever else is wrong with it. A file that does not exist is
/// not one.
fn segment_looks_sealed(fs: &dyn Fs, path: &Path) -> Result<bool> {
	let head = fs.read_prefix(path, 11)?.unwrap_or_default();
	Ok(crate::wal::sealed::starts_like_sealed(&head))
}

fn is_valid_batch_payload(payload: &[u8], compression: CompressionType) -> bool {
	if Batch::decode(payload).is_ok() {
		return true;
	}
	if compression == CompressionType::Lz4 {
		lz4_flex::decompress_size_prepended(payload)
			.map(|decompressed| Batch::decode(&decompressed).is_ok())
			.unwrap_or(false)
	} else {
		lz4_flex::decompress_size_prepended(payload)
			.map(|decompressed| Batch::decode(&decompressed).is_ok())
			.unwrap_or(false)
	}
}

fn try_validate_record(
	data: &[u8],
	start_offset: u64,
	pos: usize,
	compression: CompressionType,
) -> Option<usize> {
	if pos + HEADER_SIZE > data.len() {
		return None;
	}

	let abs_pos = start_offset + pos as u64;
	let offset_in_block = (abs_pos % BLOCK_SIZE as u64) as usize;

	if offset_in_block + HEADER_SIZE > BLOCK_SIZE {
		return None;
	}

	let header = &data[pos..pos + HEADER_SIZE];
	let crc = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
	let length = u16::from_be_bytes([header[4], header[5]]) as usize;
	let type_byte = header[6];

	if offset_in_block + HEADER_SIZE + length > BLOCK_SIZE
		|| pos + HEADER_SIZE + length > data.len()
	{
		return None;
	}

	let payload = &data[pos + HEADER_SIZE..pos + HEADER_SIZE + length];
	let expected_crc = calculate_crc32(&[type_byte], payload);
	if crc != expected_crc {
		return None;
	}

	if type_byte == RecordType::Full as u8 {
		if is_valid_batch_payload(payload, compression) {
			return Some(pos + HEADER_SIZE + length);
		}
		return None;
	}

	if type_byte == RecordType::First as u8 {
		let mut accumulated = payload.to_vec();
		let mut cur_pos = pos + HEADER_SIZE + length;

		loop {
			let cur_abs_pos = start_offset + cur_pos as u64;
			let cur_offset_in_block = (cur_abs_pos % BLOCK_SIZE as u64) as usize;

			let remaining_in_block = BLOCK_SIZE - cur_offset_in_block;
			if remaining_in_block < HEADER_SIZE {
				if cur_pos + remaining_in_block > data.len() {
					return None;
				}
				let padding = &data[cur_pos..cur_pos + remaining_in_block];
				if !padding.iter().all(|&b| b == 0) {
					return None;
				}
				cur_pos += remaining_in_block;
			} else if cur_pos + HEADER_SIZE <= data.len()
				&& data[cur_pos + 6] == RecordType::Empty as u8
			{
				if cur_pos + remaining_in_block > data.len() {
					return None;
				}
				cur_pos += remaining_in_block;
			}

			if cur_pos + HEADER_SIZE > data.len() {
				return None;
			}

			let frag_abs_pos = start_offset + cur_pos as u64;
			let frag_offset_in_block = (frag_abs_pos % BLOCK_SIZE as u64) as usize;
			if frag_offset_in_block + HEADER_SIZE > BLOCK_SIZE {
				return None;
			}

			let frag_header = &data[cur_pos..cur_pos + HEADER_SIZE];
			let frag_crc = u32::from_be_bytes([
				frag_header[0],
				frag_header[1],
				frag_header[2],
				frag_header[3],
			]);
			let frag_length = u16::from_be_bytes([frag_header[4], frag_header[5]]) as usize;
			let frag_type = frag_header[6];

			if frag_offset_in_block + HEADER_SIZE + frag_length > BLOCK_SIZE
				|| cur_pos + HEADER_SIZE + frag_length > data.len()
			{
				return None;
			}

			let frag_payload = &data[cur_pos + HEADER_SIZE..cur_pos + HEADER_SIZE + frag_length];
			let frag_expected_crc = calculate_crc32(&[frag_type], frag_payload);
			if frag_crc != frag_expected_crc {
				return None;
			}

			if frag_type == RecordType::Middle as u8 {
				accumulated.extend_from_slice(frag_payload);
				cur_pos += HEADER_SIZE + frag_length;
				continue;
			} else if frag_type == RecordType::Last as u8 {
				accumulated.extend_from_slice(frag_payload);
				cur_pos += HEADER_SIZE + frag_length;
				if is_valid_batch_payload(&accumulated, compression) {
					return Some(cur_pos);
				} else {
					return None;
				}
			} else {
				return None;
			}
		}
	}

	None
}

/// The synced length of the [`RecordType::SyncMark`] whose header is at `data[pos..]`, `data`
/// starting at offset `start_offset` of the segment: `None` unless a whole mark is there, within
/// its block, its checksum holds, and it claims no more of the segment than lies before it, which
/// is all that a writer can have synced when it wrote the mark.
pub(crate) fn sync_mark_in(data: &[u8], start_offset: u64, pos: usize) -> Option<u64> {
	let end = pos.checked_add(HEADER_SIZE + SYNC_MARK_LEN)?;
	let record = data.get(pos..end)?;
	let at = start_offset + pos as u64;
	let offset_in_block = (at % BLOCK_SIZE as u64) as usize;
	if offset_in_block + HEADER_SIZE + SYNC_MARK_LEN > BLOCK_SIZE {
		return None;
	}
	let crc = u32::from_be_bytes([record[0], record[1], record[2], record[3]]);
	let length = u16::from_be_bytes([record[4], record[5]]);
	let mark = parse_sync_mark(crc, length, record[6], &record[HEADER_SIZE..])?;
	(mark <= at).then_some(mark)
}

/// Whether the mark at `data[pos..]` (see [`sync_mark_in`]) stands where a writer puts one:
/// followed by the start of a record (a full one or its first fragment) whose checksum holds, as
/// every mark but the trailing one is, or at the end of the segment, as the trailing one is.
///
/// The scan steps through bytes it cannot read as records one at a time, the payload of a damaged
/// record or of a fragment whose start was lost included, and bytes of a value can form a mark.
/// Such a mark is followed by more of the value, not by a record, and is not taken for one.
fn mark_is_framed(data: &[u8], start_offset: u64, pos: usize) -> bool {
	let mut next = pos + HEADER_SIZE + SYNC_MARK_LEN;
	if next == data.len() {
		return true;
	}
	// A record that does not fit a header in what is left of the block starts the next one.
	let offset_in_block = ((start_offset + next as u64) % BLOCK_SIZE as u64) as usize;
	if BLOCK_SIZE - offset_in_block < HEADER_SIZE {
		next += BLOCK_SIZE - offset_in_block;
	}
	record_starts_at(data, start_offset, next)
}

/// Whether a full record, or the first fragment of one, whose checksum holds starts at
/// `data[pos..]`, within its block.
fn record_starts_at(data: &[u8], start_offset: u64, pos: usize) -> bool {
	let Some(header) = data.get(pos..pos + HEADER_SIZE) else {
		return false;
	};
	let length = usize::from(u16::from_be_bytes([header[4], header[5]]));
	let type_byte = header[6];
	let offset_in_block = ((start_offset + pos as u64) % BLOCK_SIZE as u64) as usize;
	let starts_a_record =
		type_byte == RecordType::Full as u8 || type_byte == RecordType::First as u8;
	if !starts_a_record || offset_in_block + HEADER_SIZE + length > BLOCK_SIZE {
		return false;
	}
	let crc = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
	data.get(pos + HEADER_SIZE..pos + HEADER_SIZE + length)
		.is_some_and(|payload| calculate_crc32(&[type_byte], payload) == crc)
}

/// Whether the segment at `path` holds sync marks: whether it starts with a leading mark, after
/// the record of its compression if it has one, see [`RecordType::SyncMark`]. A segment written
/// by a binary that writes no marks has none, and neither has one whose first bytes were lost:
/// both are read as segments without marks. Reads only the head of the segment.
pub(crate) fn segment_holds_marks_in(fs: &dyn Fs, path: &Path) -> Result<bool> {
	/// The record of the compression of a segment: a header and the byte of the compression.
	const COMPRESSION_RECORD_LEN: usize = HEADER_SIZE + 1;
	let head = fs
		.read_prefix(path, COMPRESSION_RECORD_LEN + HEADER_SIZE + SYNC_MARK_LEN)?
		.unwrap_or_default();
	let leading = if head.get(6) == Some(&(RecordType::SetCompressionType as u8)) {
		COMPRESSION_RECORD_LEN
	} else {
		0
	};
	Ok(sync_mark_in(&head, 0, leading).is_some())
}

/// What [`scan_after_in`] found after an offset of a segment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct AfterDamage {
	/// The intact records, sync marks not counted.
	pub(crate) intact_records: usize,
	/// The highest synced length of the intact sync marks that stand where a writer puts one,
	/// see [`mark_is_framed`].
	pub(crate) highest_mark: Option<u64>,
}

/// The intact records of the WAL segment file from `start_offset` on, and the highest synced
/// length that an intact sync mark among them claims.
pub(crate) fn scan_after_in(
	fs: &dyn Fs,
	file_path: &Path,
	start_offset: u64,
) -> Result<AfterDamage> {
	let file = fs.open_read(file_path)?;
	let file_size = file.len()?;
	if start_offset >= file_size {
		return Ok(AfterDamage::default());
	}

	// The same file answers for the compression of the segment, so a pool is not asked for a
	// second handle of it.
	let compression = Wal::detect_compression_of(&*file).unwrap_or(CompressionType::None);

	// Everything after `start_offset`: read in one go, as it always was.
	let rest = usize::try_from(file_size - start_offset).map_err(|_| {
		Error::Other(format!(
			"the WAL segment {file_path:?} is too large to scan on this platform: {file_size} bytes"
		))
	})?;
	let mut data = vec![0u8; rest];
	let read = crate::vfs::File::read_up_to(&*file, start_offset, &mut data)?;
	data.truncate(read);

	let mut intact_count = 0;
	let mut highest_mark: Option<u64> = None;
	let mut pos = 0;

	while pos + HEADER_SIZE <= data.len() {
		let abs_pos = start_offset + pos as u64;
		let offset_in_block = (abs_pos % BLOCK_SIZE as u64) as usize;

		// Header cannot cross a block boundary
		if offset_in_block + HEADER_SIZE > BLOCK_SIZE {
			let bytes_to_next_block = BLOCK_SIZE - offset_in_block;
			pos += bytes_to_next_block;
			continue;
		}

		if let Some(mark) = sync_mark_in(&data, start_offset, pos)
			.filter(|_| mark_is_framed(&data, start_offset, pos))
		{
			highest_mark = Some(highest_mark.map_or(mark, |h| h.max(mark)));
			pos += HEADER_SIZE + SYNC_MARK_LEN;
			continue;
		}

		if let Some(next_pos) = try_validate_record(&data, start_offset, pos, compression) {
			intact_count += 1;
			pos = next_pos;
		} else {
			pos += 1;
		}
	}

	Ok(AfterDamage {
		intact_records: intact_count,
		highest_mark,
	})
}

#[allow(dead_code, reason = "kept for callers that have not moved to the _in form")]
pub(crate) fn classify_wal_damage(
	file_path: &Path,
	last_valid_offset: usize,
	is_last_segment: bool,
) -> Result<WalDamage> {
	classify_wal_damage_in(
		&*crate::vfs::default_fs(),
		file_path,
		last_valid_offset,
		is_last_segment,
	)
}

/// [`classify_wal_damage`] in the file system `fs`.
///
/// Damage that an intact sync mark after it says was synced, by claiming a synced length past
/// `last_valid_offset`, is interior corruption: a power cut does not undo an fsync. Otherwise,
/// damage at the end of the last segment, with no intact record after it, is a torn tail. Damage
/// in the last segment that intact records follow is an unsynced tail if the segment holds marks
/// (it starts with a leading mark): the last fsync to complete before any of those records was
/// written ended at or below the damage, so nothing at or after it was synced when they were, and
/// a record acknowledged as durable after them would have had to wait for an fsync that covered
/// the damage too. Everything else is interior corruption, which a segment without marks and an
/// older segment always are.
///
/// What the marks cannot see is an fsync that completed after the last mark was written, and
/// was not followed by an append or a close: damage below the length it covered, followed by
/// records of the same stretch, is cut as an unsynced tail although it was synced. A power cut
/// cannot leave such damage; only a disk that loses synced bytes can.
///
/// The marks are checksummed, not authenticated: a value that holds the bytes of a mark followed
/// by those of a record, inside a record that is damaged, can still pass for a mark, and so make
/// damage that a power cut left be refused. It cannot make a segment without marks read as one
/// with them, which is decided by the head of the segment alone.
pub(crate) fn classify_wal_damage_in(
	fs: &dyn Fs,
	file_path: &Path,
	last_valid_offset: usize,
	is_last_segment: bool,
) -> Result<WalDamage> {
	let file_size = fs.file_len(file_path)?;

	let after = scan_after_in(fs, file_path, last_valid_offset as u64)?;
	let intact_records_after = after.intact_records;
	let synced = after.highest_mark.is_some_and(|mark| mark > last_valid_offset as u64);

	if is_last_segment && !synced && intact_records_after == 0 {
		return Ok(WalDamage::TornTail {
			last_valid_offset,
			file_size,
		});
	}
	if is_last_segment && !synced && segment_holds_marks_in(fs, file_path)? {
		Ok(WalDamage::UnsyncedTail {
			last_valid_offset,
			intact_records_after,
			file_size,
		})
	} else {
		Ok(WalDamage::InteriorCorruption {
			last_valid_offset,
			intact_records_after,
			is_last_segment,
			file_size,
		})
	}
}

/// The copies of damaged segments that a repair made: `<id>.wal.corrupt`, `.corrupt.1` and
/// `.corrupt.2`, with the number of the segment each was made of.
fn quarantined_segments_in(fs: &dyn Fs, wal_dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
	let mut copies = Vec::new();
	for entry in fs.read_dir(wal_dir)? {
		if entry.kind != crate::vfs::EntryKind::File {
			continue;
		}
		let name = entry.name.to_string_lossy();
		let Ok((id, Some(extension))) = crate::wal::parse_segment_name(&name) else {
			continue;
		};
		if extension == "wal.corrupt" || extension.starts_with("wal.corrupt.") {
			copies.push((id, wal_dir.join(&entry.name)));
		}
	}
	Ok(copies)
}

/// Removes the quarantine copies of the segments that are below `min_wal_number`, which are in
/// tables by now. Best effort: a copy that stays is taken again by the next call, and nothing is
/// done with a listing that fails.
///
/// What a repair keeps is a copy of the damaged tail for someone to look at. In a pool each copy is
/// a slot, a handle and a segment's worth of quota for as long as it is there, and with a default
/// of no fsync a torn tail is what every killed tab leaves, so the copies would grow with each of
/// them and with no bound. This is only called for a file system that is not `std`; on `std` the
/// newest [`QUARANTINE_COPIES_KEPT`] copies stay, whatever the log number, and
/// [`prune_quarantine_in`] removes the others when a copy is made.
pub(crate) fn reclaim_quarantine_in(fs: &dyn Fs, wal_dir: &Path, min_wal_number: u64) {
	let copies = match quarantined_segments_in(fs, wal_dir) {
		Ok(copies) => copies,
		Err(e) => {
			tracing::warn!("Could not list the quarantined WAL segments of {wal_dir:?}: {e}");
			return;
		}
	};
	for (id, path) in copies {
		if id < min_wal_number {
			match fs.remove_file(&path) {
				Ok(()) => tracing::debug!("Removed the quarantined copy {path:?}"),
				Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
				Err(e) => tracing::warn!("Could not remove the quarantined copy {path:?}: {e}"),
			}
		}
	}
}

/// How many copies of damaged segments a log directory keeps, of all segments together: the
/// newest, on every file system.
pub(crate) const QUARANTINE_COPIES_KEPT: usize = 3;

/// Removes the copies of damaged segments but the `keep` newest, which are the ones of the highest
/// segment numbers and, within a number, the one without a suffix first (the order that
/// [`quarantine_wal_segment_in`] makes them in) or, if `highest_number_is_newest`, the one with
/// the highest suffix first (the order of the copies of an encrypted log). The copy at `protect`,
/// which a repair has just made, is never removed, whatever its segment number: the repair says
/// where it is. Best effort.
pub(crate) fn prune_quarantine_in(
	fs: &dyn Fs,
	wal_dir: &Path,
	keep: usize,
	protect: &Path,
	highest_number_is_newest: bool,
) {
	let mut copies = match quarantined_segments_in(fs, wal_dir) {
		Ok(copies) => copies,
		Err(e) => {
			tracing::warn!("Could not list the quarantined WAL segments of {wal_dir:?}: {e}");
			return;
		}
	};
	// Newest first: by segment number, and within one by generation (`.corrupt` is the newest).
	let generation = |path: &Path| {
		path.extension()
			.and_then(|ext| ext.to_str())
			.and_then(|ext| ext.parse::<u32>().ok())
			.unwrap_or(0)
	};
	copies.sort_by(|(a_id, a), (b_id, b)| {
		b_id.cmp(a_id).then_with(|| {
			if highest_number_is_newest {
				generation(b).cmp(&generation(a))
			} else {
				generation(a).cmp(&generation(b))
			}
		})
	});
	if let Some(at) = copies.iter().position(|(_, path)| path == protect) {
		let copy = copies.remove(at);
		copies.insert(0, copy);
	}
	for (_, path) in copies.into_iter().skip(keep) {
		match fs.remove_file(&path) {
			Ok(()) => tracing::debug!("Removed the old quarantined copy {path:?}"),
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
			Err(e) => tracing::warn!("Could not remove the quarantined copy {path:?}: {e}"),
		}
	}
}

#[cfg_attr(
	not(test),
	allow(dead_code, reason = "kept for callers that have not moved to the _in form")
)]
pub(crate) fn quarantine_wal_segment(wal_dir: &Path, segment_id: usize) -> Result<PathBuf> {
	quarantine_wal_segment_in(&*crate::vfs::default_fs(), wal_dir, segment_id)
}

/// [`quarantine_wal_segment`] in the file system `fs`.
///
/// The copy of a pool is synced here, because the pool orders the content of a slot before the
/// directory sync only by a flush of that file. `std::fs::copy` is not synced, and it stays that
/// way on the std file system: the native path never had that fsync and gets no new one. (A cut
/// that is durable while its only copy is not could lose a torn tail on a power cut that falls
/// between the two; that is how it was, and it is for the quarantine copy, which is a courtesy
/// for a person.)
pub(crate) fn quarantine_wal_segment_in(
	fs: &dyn Fs,
	wal_dir: &Path,
	segment_id: usize,
) -> Result<PathBuf> {
	let segment_path = wal_dir.join(format!("{segment_id:020}.wal"));
	if fs.entry(&segment_path)?.is_none() {
		return Err(Error::Other(format!("WAL segment {segment_id:020}.wal does not exist")));
	}

	// Retention: keep the last 3 quarantine files per segment ID:
	// <id>.wal.corrupt, <id>.wal.corrupt.1, <id>.wal.corrupt.2
	let corrupt_base = wal_dir.join(format!("{segment_id:020}.wal.corrupt"));
	let corrupt_1 = wal_dir.join(format!("{segment_id:020}.wal.corrupt.1"));
	let corrupt_2 = wal_dir.join(format!("{segment_id:020}.wal.corrupt.2"));

	if fs.exists(&corrupt_2) {
		let _ = fs.remove_file(&corrupt_2);
	}
	if fs.exists(&corrupt_1) {
		let _ = fs.rename(&corrupt_1, &corrupt_2);
	}
	if fs.exists(&corrupt_base) {
		let _ = fs.rename(&corrupt_base, &corrupt_1);
	}

	fs.copy(&segment_path, &corrupt_base)?;
	// A pool holds the copy in a slot whose content is only ordered before the directory sync by
	// a flush of the file; the std file system never synced the copy (a quarantine copy is a
	// courtesy for a person, not part of recovery), and keeps not doing so: no new fsync on a
	// native path.
	if fs.kind() != crate::vfs::FsKind::Std {
		fs.sync_file(&corrupt_base)?;
	}
	fs.sync_dir(wal_dir)?;

	// The newest few copies of all segments are kept, however many crashes there were: the reclaim
	// of `cleanup_old_segments_for_in` only runs once the log number has passed a segment, and with
	// tiny commits and a process that is killed again and again it may not have.
	prune_quarantine_in(fs, wal_dir, QUARANTINE_COPIES_KEPT, &corrupt_base, false);

	Ok(corrupt_base)
}

/// Makes durable every segment of the log in `wal_dir` that a replay from `min_wal_number` reads,
/// and the directory that names them. Called once the replay has read and repaired them, before
/// the open takes a commit.
///
/// What a replay reads is what the file system serves, which is not only what is on the disk: a
/// process that appended a commit and was killed before its fsync leaves the bytes in the page
/// cache, where every later process reads them. The replay applies them as committed, and a commit
/// that reads them is logged in a later segment and synced. Unless the replayed segments are
/// synced first, a power loss takes the replayed records and keeps the commits that were made on
/// top of them, and the log no longer describes an order of commits. The rotation that follows the
/// replay does not sync the segment it leaves, because this process appended nothing to it.
///
/// Every mode of syncing gets this, the one that never syncs a commit included: it is one fsync
/// per replayed segment at open, and it keeps what this process acknowledges from depending on
/// records that may not be on the disk. A plaintext segment and a sealed one are synced alike: a
/// sealed segment is never appended to again, so the fsync is the only thing that makes its
/// replayed records durable.
///
/// An fsync makes durable what the file system still holds as dirty. A segment whose fsync failed
/// in an earlier process may have pages that the kernel marked clean without writing them, which
/// this fsync does not write and which a power loss still takes. Such a process failed the commits
/// that waited for that fsync and held the log until the memtables of the segment were in tables,
/// so what it held there past its last good fsync was never acknowledged as durable; if it was
/// killed before those tables were made, this open replays such records and they stay exposed to a
/// power cut. Covering that case too would take a rewrite of every replayed segment to a new file,
/// which is a copy of the whole unflushed log at every open that replays one, for a failure that
/// only a device that fails its writes produces.
pub(crate) fn sync_replayed_segments_in(
	fs: &dyn Fs,
	wal_dir: &Path,
	min_wal_number: u64,
) -> Result<()> {
	if !directory_present(fs, wal_dir)? {
		return Ok(());
	}
	let segments = SegmentRef::read_segments_from_directory_in(fs, wal_dir, Some("wal"))?;
	let mut synced = false;
	for segment in segments.iter().filter(|segment| segment.id >= min_wal_number) {
		fs.sync_file(&segment.file_path)?;
		synced = true;
	}
	if synced {
		fs.sync_dir(wal_dir)?;
	}
	Ok(())
}

#[cfg_attr(
	not(test),
	allow(dead_code, reason = "kept for callers that have not moved to the _in form")
)]
pub(crate) fn repair_corrupted_wal_segment(
	wal_dir: &Path,
	segment_id: usize,
	last_valid_offset: usize,
) -> Result<(PathBuf, u64)> {
	repair_corrupted_wal_segment_in(
		&*crate::vfs::default_fs(),
		wal_dir,
		segment_id,
		last_valid_offset,
	)
}

/// [`repair_corrupted_wal_segment`] in the file system `fs`.
pub(crate) fn repair_corrupted_wal_segment_in(
	fs: &dyn Fs,
	wal_dir: &Path,
	segment_id: usize,
	last_valid_offset: usize,
) -> Result<(PathBuf, u64)> {
	let segment_path = wal_dir.join(format!("{segment_id:020}.wal"));
	if fs.entry(&segment_path)?.is_none() {
		return Err(Error::Other(format!("WAL segment {segment_id:020}.wal does not exist")));
	}
	// This truncates plaintext at an offset of plaintext: it never runs on a sealed segment, which
	// a key that does not fit makes look like a damaged one.
	if segment_looks_sealed(fs, &segment_path)? {
		return Err(crate::error::EncryptionError::NotConfigured.into());
	}

	// 1. Quarantine original segment before modifying it. The copy is durable when this returns.
	let quarantine_path = quarantine_wal_segment_in(fs, wal_dir, segment_id)?;

	// 2. Open file for in-place truncation on the same inode. The segment is there, so nothing is
	// created: a segment that has gone since is a `NotFound`, as it was when this opened it with
	// `OpenOptions::write` and no `create`.
	let file = fs.open_append_existing(&segment_path)?;
	let old_len = file.len()?;
	file.set_len(last_valid_offset as u64)?;
	file.sync()?;
	fs.sync_dir(wal_dir)?;

	// 3. Clean up stale repair directory if present
	let repair_dir = wal_dir.join("repair_temp");
	if fs.exists(&repair_dir) {
		let _ = fs.remove_dir_all(&repair_dir);
	}

	let bytes_trimmed = old_len.saturating_sub(last_valid_offset as u64);
	Ok((quarantine_path, bytes_trimmed))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
	use std::fs;
	use std::io::{Seek, Write};

	use tempfile::TempDir;
	use test_log::test;

	use super::*;
	use crate::wal::manager::Wal;
	use crate::wal::Options;
	use crate::{LSMIterator, WalRecoveryMode};

	#[test]
	fn test_replay_wal_sequence_number_tracking() {
		// Create a temporary directory for WAL files
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();

		// Create WAL directory
		fs::create_dir_all(wal_dir).unwrap();

		// No need to create memtable - replay_wal creates its own

		// Test case: Multiple batches across multiple WAL segments
		// This verifies that ALL segments are replayed, not just the latest

		// Batch 1: Starting at 100, with 3 entries (100, 101, 102)
		let mut batch1 = Batch::new(100);
		batch1.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap(); // seq_num 100
		batch1.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap(); // seq_num 101
		batch1.set(b"key3".to_vec(), b"value3".to_vec(), 0).unwrap(); // seq_num 102
																// Highest sequence number
																// should be 102

		// Batch 2: Starting at 200, with 4 entries (200, 201, 202, 203)
		let mut batch2 = Batch::new(200);
		batch2.set(b"key4".to_vec(), b"value4".to_vec(), 0).unwrap(); // seq_num 200
		batch2.set(b"key5".to_vec(), b"value5".to_vec(), 0).unwrap(); // seq_num 201
		batch2.delete(b"key6".to_vec(), 0).unwrap(); // seq_num 202
		batch2.set(b"key7".to_vec(), b"value7".to_vec(), 0).unwrap(); // seq_num 203
																// Highest sequence number
																// should be 203

		// Create WAL and rotate to create 2 segments
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		wal.append(&batch1.encode().unwrap()).unwrap();
		wal.rotate().unwrap(); // Rotate to segment 1

		wal.append(&batch2.encode().unwrap()).unwrap();
		wal.close().unwrap();

		// Replay the WAL - should replay BOTH segments
		let arena_size = 1024 * 1024; // 1MB for tests
		let (max_seq_num_opt, memtables) = replay_wal(wal_dir, 0, arena_size).unwrap();

		// Verify both segments are replayed: max_seq_num should be 203 (highest from
		// batch2)
		assert_eq!(
			max_seq_num_opt,
			Some(203),
			"WAL recovery should track highest sequence number (203) across all segments"
		);

		// Verify we created one memtable per segment
		assert_eq!(memtables.len(), 2, "Should create one memtable per WAL segment");

		// Verify the memtables contain entries from BOTH segments
		let mut entry_count = 0;
		for (memtable, _, _) in memtables {
			let mut iter = memtable.iter();
			while iter.valid() {
				entry_count += 1;
				iter.next().unwrap();
			}
		}
		assert_eq!(
			entry_count, 7,
			"Memtables should contain all 7 entries from both WAL segments (6 sets + 1 delete)"
		);
	}

	#[test]
	fn test_replay_wal_empty_directory() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		let arena_size = 1024 * 1024; // 1MB for tests
		let (max_seq_num_opt, memtables) = replay_wal(wal_dir, 0, arena_size).unwrap();

		assert_eq!(max_seq_num_opt, None, "Empty WAL directory should return None");
		assert_eq!(memtables.len(), 0, "Empty WAL directory should return no memtables");
	}

	#[test]
	fn test_replay_wal_single_entry_batches() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Test with multiple single-entry batches across 3 WAL segments
		// This tests that ALL segments are replayed, not just the latest
		let mut batch1 = Batch::new(500);
		batch1.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap(); // seq_num 500

		let mut batch2 = Batch::new(600);
		batch2.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap(); // seq_num 600

		let mut batch3 = Batch::new(700);
		batch3.set(b"key3".to_vec(), b"value3".to_vec(), 0).unwrap(); // seq_num 700

		// Create WAL for all batches (use rotation to create three segments)
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		wal.append(&batch1.encode().unwrap()).unwrap();
		wal.rotate().unwrap(); // Rotate to WAL 1

		wal.append(&batch2.encode().unwrap()).unwrap();
		wal.rotate().unwrap(); // Rotate to WAL 2

		wal.append(&batch3.encode().unwrap()).unwrap();
		wal.close().unwrap();

		let arena_size = 1024 * 1024; // 1MB for tests
		let (max_seq_num_opt, memtables) = replay_wal(wal_dir, 0, arena_size).unwrap();

		// All three segments should be replayed, max should be 700
		assert_eq!(
			max_seq_num_opt,
			Some(700),
			"Should replay all 3 segments and return highest sequence number (700)"
		);

		// Verify we created one memtable per segment
		assert_eq!(memtables.len(), 3, "Should create one memtable per WAL segment");

		// Verify all 3 entries are in the memtables (from all 3 segments)
		let mut entry_count = 0;
		for (memtable, _, _) in memtables {
			let mut iter = memtable.iter();
			while iter.valid() {
				entry_count += 1;
				iter.next().unwrap();
			}
		}
		assert_eq!(
			entry_count, 3,
			"Memtables should contain all 3 entries from all 3 WAL segments"
		);
	}

	#[test]
	fn test_replay_wal_multiple_batches() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Test case: Multiple batches across 2 WAL segments
		// This ensures ALL segments are replayed and max tracking works correctly
		let mut batch1 = Batch::new(200); // Starting sequence number 200
		batch1.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap(); // seq_num 200
		batch1.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap(); // seq_num 201
																// Highest sequence number
																// should be 201

		let mut batch2 = Batch::new(300); // Starting sequence number 300
		batch2.set(b"key3".to_vec(), b"value3".to_vec(), 0).unwrap(); // seq_num 300
		batch2.set(b"key4".to_vec(), b"value4".to_vec(), 0).unwrap(); // seq_num 301
																// Highest sequence number
																// should be 301

		// Create WAL and rotate to create 2 segments
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		wal.append(&batch1.encode().unwrap()).unwrap();
		wal.rotate().unwrap(); // Rotate to segment 1

		wal.append(&batch2.encode().unwrap()).unwrap();
		wal.close().unwrap();

		// Replay WAL
		let arena_size = 1024 * 1024; // 1MB for tests
		let (max_seq_num_opt, memtables) = replay_wal(wal_dir, 0, arena_size).unwrap();

		// Both segments should be replayed, max should be 301
		assert_eq!(
			max_seq_num_opt,
			Some(301),
			"WAL recovery should track highest sequence number (301) across all segments"
		);

		// Verify we created one memtable per segment
		assert_eq!(memtables.len(), 2, "Should create one memtable per WAL segment");

		// Verify all 4 entries from both segments are in the memtables
		let mut entry_count = 0;
		for (memtable, _, _) in memtables {
			let mut iter = memtable.iter();
			while iter.valid() {
				entry_count += 1;
				iter.next().unwrap();
			}
		}

		assert_eq!(entry_count, 4, "Memtables should contain all 4 entries from both WAL segments");
	}

	#[test]
	fn test_repair_corrupted_wal_segment_nonexistent() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Test repair of non-existent segment
		let result = repair_corrupted_wal_segment(wal_dir, 999, 0);
		assert!(result.is_err(), "Should fail when segment doesn't exist");
	}

	#[test]
	fn test_post_corruption_replay() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Create a valid WAL first
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		// Add some valid data
		let mut batch1 = Batch::new(100);
		batch1.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
		batch1.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap();

		wal.append(&batch1.encode().unwrap()).unwrap();
		wal.close().unwrap();

		// Now manually corrupt the segment to reproduce the exact error
		let segment_path = wal_dir.join("00000000000000000000.wal");
		let mut file =
			std::fs::OpenOptions::new().read(true).write(true).open(&segment_path).unwrap();

		// Corrupt the first record (at offset 0, no file header)
		file.seek(std::io::SeekFrom::Start(0)).unwrap();

		// Write a Middle record where a First/Full record should be
		let record_type = 3u8;
		let length = 10u16; // Length of data
		let data = vec![0xFF; 10]; // Some data
		let crc = crate::wal::calculate_crc32(&[record_type], &data);

		file.write_all(&[record_type]).unwrap(); // Record type: Middle (WRONG!)
		file.write_all(&length.to_be_bytes()).unwrap(); // Length
		file.write_all(&crc.to_be_bytes()).unwrap(); // CRC
		file.write_all(&data).unwrap(); // Data
		drop(file);

		// Test using Core::replay_wal_with_repair (the actual production flow)
		let (max_seq_num, memtable_opt, _report) = crate::lsm::Core::replay_wal_with_repair(
			wal_dir,
			0,
			"Test repair",
			WalRecoveryMode::TolerateCorruptedWithRepair,
			1024,
			|_memtable, _wal_number, _continues| {
				// Flush callback - not needed for this test
				Ok(())
			},
		)
		.unwrap();

		// Verify the repair worked correctly
		// Since the first record is corrupted, we should recover None (no valid data)
		assert_eq!(max_seq_num, None, "Should have recovered None when first record is corrupted");

		// Verify that no memtable was returned (since first record was corrupted)
		assert!(memtable_opt.is_none(), "Should have no memtable when first record is corrupted");
	}

	#[test]
	fn test_repair_with_three_batches_corrupt_third() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Create a valid WAL with three batches, and no sync marks, which would move them
		let opts = Options {
			sync_marks: false,
			..Options::default()
		};
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		// Create three batches with different sequence numbers
		let mut batch1 = Batch::new(100);
		batch1.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
		batch1.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap();

		let mut batch2 = Batch::new(200);
		batch2.set(b"key3".to_vec(), b"value3".to_vec(), 0).unwrap();
		batch2.set(b"key4".to_vec(), b"value4".to_vec(), 0).unwrap();

		let mut batch3 = Batch::new(300);
		batch3.set(b"key5".to_vec(), b"value5".to_vec(), 0).unwrap();
		batch3.set(b"key6".to_vec(), b"value6".to_vec(), 0).unwrap();

		// Encode all batches
		let encoded1 = batch1.encode().unwrap();
		let encoded2 = batch2.encode().unwrap();
		let encoded3 = batch3.encode().unwrap();

		// Append all batches
		wal.append(&encoded1).unwrap();
		wal.append(&encoded2).unwrap();
		wal.append(&encoded3).unwrap();
		wal.close().unwrap();

		// Now manually corrupt the segment by corrupting the third batch
		let segment_path = wal_dir.join("00000000000000000000.wal");
		let mut file =
			std::fs::OpenOptions::new().read(true).write(true).open(&segment_path).unwrap();

		// Calculate the offset for the third batch (no file header)
		file.seek(std::io::SeekFrom::Start(0)).unwrap();

		// Each batch is written as a WAL record with a 7-byte header
		let batch1_size = encoded1.len() + 7; // encoded data + WAL record header
		let batch2_size = encoded2.len() + 7; // encoded data + WAL record header
		let third_batch_offset = batch1_size as u64 + batch2_size as u64;

		// Corrupt the third batch by changing its record type to Middle
		file.seek(std::io::SeekFrom::Start(third_batch_offset)).unwrap();
		let record_type = 3u8;
		let length = 10u16;
		let data = vec![0xFF; 10];
		let crc = crate::wal::calculate_crc32(&[record_type], &data);

		file.write_all(&[record_type]).unwrap();
		file.write_all(&length.to_be_bytes()).unwrap(); // Length
		file.write_all(&crc.to_be_bytes()).unwrap(); // CRC
		file.write_all(&data).unwrap(); // Data
		drop(file);

		let (max_seq_num, memtable_opt, _report) = crate::lsm::Core::replay_wal_with_repair(
			wal_dir,
			0,
			"Test repair",
			WalRecoveryMode::TolerateCorruptedWithRepair,
			64 * 1024,
			|_memtable, _wal_number, _continues| {
				// Flush callback - not needed for this test
				Ok(())
			},
		)
		.unwrap();

		// Verify the repair worked correctly
		// Since the third batch is corrupted, we should recover data from the first two
		// batches The max sequence number should be from batch 2 (201), not batch 3
		// (301)
		assert_eq!(
			max_seq_num,
			Some(201),
			"Should have recovered sequence numbers from first two batches only"
		);

		// Verify that we got a memtable with entries from the first two batches
		if let Some(memtable) = memtable_opt {
			let mut entry_count = 0;
			let mut iter = memtable.iter();
			while iter.valid() {
				entry_count += 1;
				iter.next().unwrap();
			}

			assert_eq!(entry_count, 4, "Should have recovered 4 entries from first two batches");
		} else {
			panic!("Should have recovered a memtable");
		}
	}

	#[test]
	fn test_recovery_mode_tolerate_tail_corruption() {
		use crate::batch::Batch;

		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Write valid batches first
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		let mut batch1 = Batch::new(100);
		batch1.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
		wal.append(&batch1.encode().unwrap()).unwrap();

		let mut batch2 = Batch::new(200);
		batch2.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap();
		wal.append(&batch2.encode().unwrap()).unwrap();

		wal.close().unwrap();

		// Manually corrupt the end of the file
		let segment_path = wal_dir.join("00000000000000000000.wal");
		let mut file = fs::OpenOptions::new().append(true).open(&segment_path).unwrap();
		file.write_all(b"CORRUPTED_DATA_AT_END").unwrap();

		// Replay with TolerateCorruptedTailRecords (default)
		let arena_size = 1024 * 1024; // 1MB for tests
		let result = replay_wal(wal_dir, 0, arena_size);

		// Should report corruption as an error
		match result {
			Err(Error::WalCorruption {
				segment_id,
				..
			}) => {
				assert_eq!(segment_id, 0, "Should detect corruption in segment 0");
			}
			_ => panic!("Expected WalCorruption error"),
		}
	}

	#[test]
	fn test_recovery_mode_skip_corrupted() {
		// This test would require injecting corruption in the middle of records
		// For now, verify the mode parameter works
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		let mut batch = Batch::new(100);
		batch.set(b"key".to_vec(), b"value".to_vec(), 0).unwrap();
		wal.append(&batch.encode().unwrap()).unwrap();
		wal.close().unwrap();

		let arena_size = 1024 * 1024; // 1MB for tests
		let result = replay_wal(wal_dir, 0, arena_size);

		assert!(result.is_ok());
	}

	#[test]
	fn test_default_reporter_usage() {
		use crate::batch::Batch;

		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Write a valid batch
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		let mut batch = Batch::new(100);
		batch.set(b"key".to_vec(), b"value".to_vec(), 0).unwrap();
		wal.append(&batch.encode().unwrap()).unwrap();
		wal.close().unwrap();

		// Replay - DefaultReporter is created internally
		let arena_size = 1024 * 1024; // 1MB for tests
		let (seq_num_opt, memtables) = replay_wal(wal_dir, 0, arena_size).unwrap();

		assert_eq!(seq_num_opt, Some(100));
		assert_eq!(memtables.len(), 1, "Should create one memtable for single segment");
		// Reporter is used internally for logging
	}

	#[test]
	fn test_multi_segment_recovery_after_crash() {
		use crate::batch::Batch;

		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Simulate: rotate WAL, crash before flush
		// This is the critical bug scenario that was causing data loss

		// Create first batch in WAL segment 0
		let mut batch1 = Batch::new(100);
		batch1.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
		batch1.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap();

		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();
		wal.append(&batch1.encode().unwrap()).unwrap();

		// Rotate to WAL segment 1 (simulating memtable rotation)
		wal.rotate().unwrap();

		// Create second batch in WAL segment 1
		let mut batch2 = Batch::new(200);
		batch2.set(b"key3".to_vec(), b"value3".to_vec(), 0).unwrap();
		batch2.set(b"key4".to_vec(), b"value4".to_vec(), 0).unwrap();

		wal.append(&batch2.encode().unwrap()).unwrap();

		// Close without flushing memtable (simulating crash)
		wal.close().unwrap();

		// Now attempt recovery - both WAL segments should be replayed
		let arena_size = 1024 * 1024; // 1MB for tests
		let (max_seq_num_opt, memtables) = replay_wal(wal_dir, 0, arena_size).unwrap();

		// Verify both segments were replayed
		assert_eq!(
			max_seq_num_opt,
			Some(201),
			"Should replay both WAL segments and return max seq from segment 1"
		);

		// Verify we created one memtable per segment
		assert_eq!(memtables.len(), 2, "Should create one memtable per WAL segment");

		// Verify all 4 entries from both segments are recovered
		let mut entry_count = 0;
		for (memtable, _, _) in memtables {
			let mut iter = memtable.iter();
			while iter.valid() {
				entry_count += 1;
				iter.next().unwrap();
			}
		}

		assert_eq!(
			entry_count, 4,
			"Should recover all 4 entries from both WAL segments (2 from each)"
		);
	}

	#[test]
	fn test_corruption_stops_at_segment_n() {
		use crate::batch::Batch;

		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Create 3 segments with valid data
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		// Segment 0: valid data
		let mut batch0 = Batch::new(100);
		batch0.set(b"key0".to_vec(), b"value0".to_vec(), 0).unwrap();
		wal.append(&batch0.encode().unwrap()).unwrap();
		wal.rotate().unwrap();

		// Segment 1: will be corrupted
		let mut batch1 = Batch::new(200);
		batch1.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
		let encoded1 = batch1.encode().unwrap();
		wal.append(&encoded1).unwrap();
		wal.rotate().unwrap();

		// Segment 2: valid data (should NOT be processed due to corruption in segment
		// 1)
		let mut batch2 = Batch::new(300);
		batch2.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap();
		wal.append(&batch2.encode().unwrap()).unwrap();
		wal.close().unwrap();

		// Corrupt segment 1
		let segment1_path = wal_dir.join("00000000000000000001.wal");
		let mut file = fs::OpenOptions::new().read(true).write(true).open(&segment1_path).unwrap();

		// Corrupt at the beginning of the segment
		file.seek(std::io::SeekFrom::Start(0)).unwrap();
		let record_type = 3u8; // Middle record (invalid at start)
		let length = 10u16;
		let data = vec![0xFF; 10];
		let crc = crate::wal::calculate_crc32(&[record_type], &data);

		file.write_all(&[record_type]).unwrap();
		file.write_all(&length.to_be_bytes()).unwrap();
		file.write_all(&crc.to_be_bytes()).unwrap();
		file.write_all(&data).unwrap();
		drop(file);

		// Attempt recovery
		let arena_size = 1024 * 1024; // 1MB for tests
		let result = replay_wal(wal_dir, 0, arena_size);

		// Should report corruption in segment 1
		match result {
			Err(Error::WalCorruption {
				segment_id,
				..
			}) => {
				assert_eq!(segment_id, 1, "Corruption should be reported in segment 1");
			}
			_ => panic!("Expected WalCorruption error in segment 1"),
		}

		// On corruption, we should have only segment 0's memtable
		// (replay stops at corruption, so no memtables returned)
		// Actually, replay_wal returns error immediately on corruption, so no memtables
		// This is correct behavior - corruption stops recovery
	}

	#[test]
	fn test_multi_wal_recovery_creates_multiple_memtables() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		// Create 3 WAL segments with data
		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		// Segment 0
		let mut batch0 = Batch::new(100);
		batch0.set(b"key0".to_vec(), b"value0".to_vec(), 0).unwrap();
		wal.append(&batch0.encode().unwrap()).unwrap();
		wal.rotate().unwrap();

		// Segment 1
		let mut batch1 = Batch::new(200);
		batch1.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
		wal.append(&batch1.encode().unwrap()).unwrap();
		wal.rotate().unwrap();

		// Segment 2
		let mut batch2 = Batch::new(300);
		batch2.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap();
		wal.append(&batch2.encode().unwrap()).unwrap();
		wal.close().unwrap();

		// Replay - should create 3 memtables
		let arena_size = 1024 * 1024;
		let (max_seq, memtables) = replay_wal(wal_dir, 0, arena_size).unwrap();

		assert_eq!(max_seq, Some(300), "Max seq should be from last batch");
		assert_eq!(memtables.len(), 3, "Should create one memtable per WAL segment");

		// Verify each memtable has correct WAL number
		assert_eq!(memtables[0].2, 0, "First memtable should have WAL 0");
		assert_eq!(memtables[1].2, 1, "Second memtable should have WAL 1");
		assert_eq!(memtables[2].2, 2, "Third memtable should have WAL 2");

		// Verify each memtable has correct data
		{
			let mut iter = memtables[0].0.iter();
			iter.seek_first().unwrap();
			let mut count = 0;
			while iter.valid() {
				count += 1;
				if !iter.next().unwrap() {
					break;
				}
			}
			assert_eq!(count, 1);
		}
		{
			let mut iter = memtables[1].0.iter();
			iter.seek_first().unwrap();
			let mut count = 0;
			while iter.valid() {
				count += 1;
				if !iter.next().unwrap() {
					break;
				}
			}
			assert_eq!(count, 1);
		}
		{
			let mut iter = memtables[2].0.iter();
			iter.seek_first().unwrap();
			let mut count = 0;
			while iter.valid() {
				count += 1;
				if !iter.next().unwrap() {
					break;
				}
			}
			assert_eq!(count, 1);
		}
	}

	/// Regression test: with >= 2 WAL segments to replay, `replay_wal` delegates to
	/// `parallel_recovery::replay_segments_sync`. Before the fix, that path still had
	/// the old "fail hard when a batch alone exceeds arena_size" logic that
	/// `replay_wal`'s own single-segment loop had already been fixed to handle
	/// gracefully -- so this exact scenario (an oversized batch, in one of several
	/// pending WAL segments, which is the common case after any WAL rotation before a
	/// crash) made the whole database fail to open.
	#[test]
	fn test_multi_segment_recovery_with_oversized_batch_does_not_fail() {
		let temp_dir = TempDir::new().unwrap();
		let wal_dir = temp_dir.path();
		fs::create_dir_all(wal_dir).unwrap();

		let opts = Options::default();
		let mut wal = Wal::open(wal_dir, opts).unwrap();

		// Segment 0: a normal, small batch.
		let mut batch0 = Batch::new(100);
		batch0.set(b"key0".to_vec(), b"value0".to_vec(), 0).unwrap();
		wal.append(&batch0.encode().unwrap()).unwrap();
		wal.rotate().unwrap();

		// Segment 1: a single batch whose own size exceeds `arena_size` -- what a live
		// commit's direct-to-L0 path (`CoreInner::write_batch_direct_to_l0_sst`) would
		// have durably written to the WAL before a crash.
		let arena_size = 4096usize;
		let big_value = vec![0xEFu8; arena_size * 2];
		let mut batch1 = Batch::new(200);
		batch1.set(b"key1".to_vec(), big_value.clone(), 0).unwrap();
		wal.append(&batch1.encode().unwrap()).unwrap();
		wal.close().unwrap();

		let (max_seq_num_opt, memtables) = replay_wal(wal_dir, 0, arena_size).unwrap();

		assert_eq!(max_seq_num_opt, Some(200));
		assert_eq!(
			memtables.len(),
			2,
			"expected one memtable for segment 0 and one oversized memtable for segment 1"
		);

		let (_ikey0, val0) = memtables
			.iter()
			.find_map(|(mt, _, _)| mt.get(b"key0", None))
			.expect("key0 must be recovered");
		assert_eq!(val0, b"value0".to_vec());

		let (_ikey1, val1) = memtables
			.iter()
			.find_map(|(mt, _, _)| mt.get(b"key1", None))
			.expect("key1 must be recovered");
		assert_eq!(val1, big_value);
	}
}
