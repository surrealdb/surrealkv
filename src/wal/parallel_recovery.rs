//! Parallel WAL Replay for sub-millisecond crash recovery.
//!
//! Because SurrealKV V2 is a single-version KV store without in-tree timestamps,
//! WAL segments and record batches across disjoint sequence numbers can be decoded
//! and prepared concurrently across available CPU cores via thread pools.

use std::path::Path;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::memtable::MemTable;
use crate::wal::reader::Reader;
use crate::wal::recovery::{apply_batch_with_oversized_fallback, DefaultReporter};
use crate::wal::sealed_recovery::{self, DecodedSegment};
use crate::wal::{Error as WalError, SegmentRef, WalContext};

/// Decodes a single plaintext WAL segment file into batches and the segment's maximum sequence
/// number.
#[cfg(test)]
pub(crate) fn decode_segment_batches(
	file_path: &Path,
	segment_id: u64,
) -> Result<(u64, Vec<Batch>)> {
	decode_segment_batches_with(&WalContext::plaintext(), file_path, segment_id)
}

/// [`decode_segment_ending_with`] without the offset where the reader stopped.
#[cfg(test)]
pub(crate) fn decode_segment_batches_with(
	context: &WalContext,
	file_path: &Path,
	segment_id: u64,
) -> Result<(u64, Vec<Batch>)> {
	let (max_seq, batches, _) = decode_segment_ending_with(context, file_path, segment_id)?;
	Ok((max_seq, batches))
}

/// Decodes a single WAL segment file, read as `context` says, into batches, the segment's maximum
/// sequence number, and the offset where the last complete record ends. The reader stops at the
/// end of the records, which is not the end of the file when the file holds zeros after them.
pub(crate) fn decode_segment_ending_with(
	context: &WalContext,
	file_path: &Path,
	segment_id: u64,
) -> Result<(u64, Vec<Batch>, u64)> {
	let file = context.fs.open_read(file_path)?;
	let reporter = Box::new(DefaultReporter::new(segment_id));
	let mut reader = Reader::with_context(file, Some(reporter), segment_id, context);

	let mut batches = Vec::new();
	let mut max_seq = 0u64;
	let mut last_valid_offset = 0;

	loop {
		match reader.read() {
			Ok((record_data, offset)) => {
				last_valid_offset = offset as usize;
				let batch = Batch::decode(record_data)?;
				let seq = batch.get_highest_seq_num();
				if seq > max_seq {
					max_seq = seq;
				}
				batches.push(batch);
			}
			Err(WalError::Corruption(err)) => {
				return Err(Error::wal_corruption(
					segment_id as usize,
					last_valid_offset,
					format!("Corrupted WAL record: {}", err),
				));
			}
			Err(WalError::IO(err)) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
				break;
			}
			Err(err) => return Err(err.into()),
		}
	}

	Ok((max_seq, batches, last_valid_offset as u64))
}

/// Decodes the sealed segments of `segments`, in order: in parallel on the workers of the pool if
/// there are several, where each worker has the context, and the first failure that is not a tear
/// is the error. Nothing is read through a worker that was not given the context.
pub(crate) fn decode_sealed_segments(
	context: &WalContext,
	segments: &[SegmentRef],
) -> Result<Vec<DecodedSegment>> {
	let decode = |segment: &SegmentRef| {
		sealed_recovery::decode_segment(context, &segment.file_path, segment.id)
	};
	if segments.len() <= 1 {
		return segments.iter().map(decode).collect();
	}

	#[cfg(target_arch = "wasm32")]
	{
		segments.iter().map(decode).collect()
	}

	#[cfg(not(target_arch = "wasm32"))]
	{
		if let Ok(handle) = tokio::runtime::Handle::try_current() {
			if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
				return tokio::task::block_in_place(|| {
					handle.block_on(decode_sealed_segments_parallel(context, segments))
				});
			}
		}

		let segs = segments.to_vec();
		let context = context.clone();
		std::thread::spawn(move || {
			let rt = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.map_err(|e| Error::Other(format!("Failed to build temporary runtime: {e}")))?;
			rt.block_on(decode_sealed_segments_parallel(&context, &segs))
		})
		.join()
		.map_err(|_| Error::Other("WAL parallel recovery thread panicked".to_string()))?
	}
}

#[cfg(not(target_arch = "wasm32"))]
async fn decode_sealed_segments_parallel(
	context: &WalContext,
	segments: &[SegmentRef],
) -> Result<Vec<DecodedSegment>> {
	let mut tasks = Vec::with_capacity(segments.len());
	for segment in segments {
		let path = segment.file_path.clone();
		let id = segment.id;
		let context = context.clone();
		tasks.push(affinitypool::spawn(move || {
			sealed_recovery::decode_segment(&context, &path, id)
		}));
	}
	let mut decoded = Vec::with_capacity(tasks.len());
	for task in tasks {
		decoded.push(task.await?);
	}
	Ok(decoded)
}

/// The newest segment of the log, as the replay read it: its number, and the offset where its
/// last complete record ends. See `ReplayInnerResult`.
pub(crate) type ParallelReplayResult =
	(Option<u64>, Vec<(Arc<MemTable>, bool, u64)>, usize, usize, Option<(u64, u64)>);

/// Synchronous entry point for parallel segment replay.
pub(crate) fn replay_segments_sync(
	context: &WalContext,
	segments: &[SegmentRef],
	arena_size: usize,
) -> Result<ParallelReplayResult> {
	#[cfg(target_arch = "wasm32")]
	{
		replay_segments_serial(context, segments, arena_size)
	}

	#[cfg(not(target_arch = "wasm32"))]
	{
		if let Ok(handle) = tokio::runtime::Handle::try_current() {
			if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
				return tokio::task::block_in_place(|| {
					handle.block_on(replay_segments_parallel(context, segments, arena_size))
				});
			}
		}

		let segs = segments.to_vec();
		let context = context.clone();
		std::thread::spawn(move || {
			let rt = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.map_err(|e| Error::Other(format!("Failed to build temporary runtime: {e}")))?;
			rt.block_on(replay_segments_parallel(&context, &segs, arena_size))
		})
		.join()
		.map_err(|_| Error::Other("WAL parallel recovery thread panicked".to_string()))?
	}
}

/// Replays `segments` one after the other on the calling thread: the replay of a target that has
/// no threads to decode on. It is the same replay as `replay_segments_parallel`, a memtable for
/// each segment that has batches and none for a segment whose last batch was an oversized one
/// that took a memtable of its own. Kept apart from the cfg of the target so that the native
/// tests can hold it to the parallel replay.
#[cfg(any(test, target_arch = "wasm32"))]
fn replay_segments_serial(
	context: &WalContext,
	segments: &[SegmentRef],
	arena_size: usize,
) -> Result<ParallelReplayResult> {
	let mut memtables = Vec::new();
	let mut max_seq: Option<u64> = None;
	let mut total_batches = 0;
	let mut segments_processed = 0;
	let mut newest_end = None;
	for seg in segments {
		let (seg_max, batches, end) = decode_segment_ending_with(context, &seg.file_path, seg.id)?;
		// The segments are in order, so the last one that is read is the newest.
		newest_end = Some((seg.id, end));
		if batches.is_empty() {
			continue;
		}
		if seg_max > 0 {
			max_seq = Some(max_seq.map_or(seg_max, |m| m.max(seg_max)));
		}
		segments_processed += 1;
		total_batches += batches.len();
		let mut current_memtable = Arc::new(MemTable::new(arena_size));
		for batch in batches {
			current_memtable = apply_batch_with_oversized_fallback(
				current_memtable,
				&batch,
				arena_size,
				seg.id,
				&mut memtables,
			)?;
		}
		// A memtable that an oversized batch left empty is not replayed: a flush of it would
		// write a table with no entries.
		if !current_memtable.is_empty() {
			memtables.push((current_memtable, false, seg.id));
		}
	}
	Ok((max_seq, memtables, total_batches, segments_processed, newest_end))
}

/// Replays a slice of segments in parallel using affinitypool, applying batches
/// sequentially into memtables to maintain strict WAL order.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn replay_segments_parallel(
	context: &WalContext,
	segments: &[SegmentRef],
	arena_size: usize,
) -> Result<ParallelReplayResult> {
	if segments.is_empty() {
		return Ok((None, Vec::new(), 0, 0, None));
	}

	// 1. Concurrently decode all WAL segments in parallel on worker threads
	let mut decode_tasks = Vec::with_capacity(segments.len());
	for seg in segments {
		let path = seg.file_path.clone();
		let id = seg.id;
		let context = context.clone();
		let task = affinitypool::spawn(move || decode_segment_ending_with(&context, &path, id));
		decode_tasks.push(task);
	}

	let mut decoded_segments = Vec::with_capacity(decode_tasks.len());
	for task in decode_tasks {
		let res = task.await;
		decoded_segments.push(res?);
	}

	// 2. Sequentially populate memtables from ordered segments
	let global_max_seq = AtomicU64::new(0);
	let mut memtables = Vec::new();
	let mut total_batches = 0;
	let mut segments_processed = 0;

	let mut newest_end = None;
	for (seg_idx, (seg_max_seq, batches, end)) in decoded_segments.into_iter().enumerate() {
		let segment_id = segments[seg_idx].id;
		// The segments are in order, so the last one that is read is the newest.
		newest_end = Some((segment_id, end));
		if batches.is_empty() {
			continue;
		}

		global_max_seq.fetch_max(seg_max_seq, Ordering::SeqCst);
		segments_processed += 1;
		total_batches += batches.len();

		let mut current_memtable = Arc::new(MemTable::new(arena_size));
		for batch in batches {
			current_memtable = apply_batch_with_oversized_fallback(
				current_memtable,
				&batch,
				arena_size,
				segment_id,
				&mut memtables,
			)?;
		}

		if !current_memtable.is_empty() {
			memtables.push((current_memtable, false, segment_id));
		}
	}

	let max_seq = global_max_seq.load(Ordering::SeqCst);
	let res_seq = if max_seq > 0 {
		Some(max_seq)
	} else {
		None
	};

	Ok((res_seq, memtables, total_batches, segments_processed, newest_end))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
	use std::fs::File;

	use tempfile::TempDir;

	use super::*;
	use crate::InternalKeyKind;

	#[tokio::test]
	async fn test_parallel_wal_replay_single_segment() {
		let temp_dir = TempDir::new().unwrap();
		let wal_path = temp_dir.path().join("00000000000000000001.wal");

		// Write test batches to a segment file
		{
			let file = File::create(&wal_path).unwrap();
			let buf_writer = crate::wal::BufferedFileWriter::new(file, crate::wal::BLOCK_SIZE);
			let mut writer = crate::wal::writer::Writer::new(
				buf_writer,
				false,
				crate::wal::CompressionType::None,
				0,
			);

			let mut b1 = Batch::new(10);
			b1.add_record(InternalKeyKind::Set, b"k1".to_vec(), Some(b"v1".to_vec()), 0).unwrap();
			let mut enc = Vec::new();
			b1.encode_into(&mut enc).unwrap();
			writer.add_record(&enc).unwrap();
			writer.close().unwrap();
		}

		let seg = SegmentRef {
			id: 1,
			file_path: wal_path,
		};

		let (max_seq, memtables, total_batches, segments_processed, _) =
			replay_segments_parallel(&WalContext::plaintext(), &[seg], 1024 * 1024).await.unwrap();
		assert_eq!(max_seq, Some(10));
		assert_eq!(memtables.len(), 1);
		assert_eq!(total_batches, 1);
		assert_eq!(segments_processed, 1);

		let val = memtables[0].0.get(b"k1", None).unwrap();
		assert_eq!(val.1, b"v1");
	}

	/// Writes a segment with a normal batch, rotates, writes one that is larger than `arena_size`
	/// and so takes a memtable of its own, rotates, and writes another normal batch. The segment
	/// of the oversized batch ends on a fresh, empty memtable.
	fn write_segments_with_an_oversized_batch(dir: &Path, arena_size: usize) -> Vec<SegmentRef> {
		let mut wal = crate::wal::manager::Wal::open(dir, crate::wal::Options::default()).unwrap();
		let mut small = Batch::new(100);
		small.set(b"key0".to_vec(), b"value0".to_vec(), 0).unwrap();
		wal.append(&small.encode().unwrap()).unwrap();
		wal.rotate().unwrap();

		let mut big = Batch::new(200);
		big.set(b"key1".to_vec(), vec![0xEF; arena_size * 2], 0).unwrap();
		wal.append(&big.encode().unwrap()).unwrap();
		wal.rotate().unwrap();

		let mut last = Batch::new(300);
		last.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap();
		wal.append(&last.encode().unwrap()).unwrap();
		wal.close().unwrap();

		SegmentRef::read_segments_from_directory(dir, Some("wal"))
			.unwrap()
			.into_iter()
			.filter(|segment| {
				decode_segment_batches(&segment.file_path, segment.id)
					.map(|(_, batches)| !batches.is_empty())
					.unwrap_or(false)
			})
			.collect()
	}

	/// The replay of a target without threads is the replay of the others: a segment whose last
	/// batch was an oversized one does not leave an empty memtable behind, which a flush would
	/// write as a table with no entries.
	#[tokio::test]
	async fn the_serial_replay_matches_the_parallel_replay() {
		let arena_size = 4096;
		let temp_dir = TempDir::new().unwrap();
		let segments = write_segments_with_an_oversized_batch(temp_dir.path(), arena_size);
		assert_eq!(segments.len(), 3, "three segments with batches");
		let context = WalContext::plaintext();

		let parallel = replay_segments_parallel(&context, &segments, arena_size).await.unwrap();
		let serial = replay_segments_serial(&context, &segments, arena_size).unwrap();

		assert_eq!((parallel.0, parallel.2, parallel.3), (Some(300), 3, 3));
		assert_eq!((serial.0, serial.2, serial.3), (parallel.0, parallel.2, parallel.3));
		assert_eq!(
			serial.1.len(),
			parallel.1.len(),
			"one memtable per segment, and the oversized one is not followed by an empty one"
		);
		assert_eq!(serial.1.len(), 3);
		for ((serial_table, serial_oversized, serial_segment), (table, oversized, segment)) in
			serial.1.iter().zip(parallel.1.iter())
		{
			assert_eq!((serial_oversized, serial_segment), (oversized, segment));
			assert!(!serial_table.is_empty());
			assert_eq!(serial_table.is_empty(), table.is_empty());
		}
		for key in [&b"key0"[..], b"key1", b"key2"] {
			assert!(
				serial.1.iter().any(|(table, _, _)| table.get(key, None).is_some()),
				"{key:?} is replayed"
			);
		}
	}
}
