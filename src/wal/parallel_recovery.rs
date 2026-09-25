//! Parallel WAL Replay for sub-millisecond crash recovery.
//!
//! Because SurrealKV V2 is a single-version KV store without in-tree timestamps,
//! WAL segments and record batches across disjoint sequence numbers can be decoded
//! and prepared concurrently across available CPU cores via thread pools.

use std::fs::File;
use std::path::Path;
#[cfg(not(target_arch = "wasm32"))]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::memtable::MemTable;
use crate::wal::reader::Reader;
use crate::wal::recovery::DefaultReporter;
use crate::wal::{Error as WalError, SegmentRef};

/// Decodes a single WAL segment file into batches and the segment's maximum sequence number.
pub(crate) fn decode_segment_batches(
	file_path: &Path,
	segment_id: u64,
) -> Result<(u64, Vec<Batch>)> {
	let file = File::open(file_path)?;
	let reporter = Box::new(DefaultReporter::new(segment_id));
	let mut reader = Reader::with_options(file, Some(reporter), segment_id);

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

	Ok((max_seq, batches))
}

pub(crate) type ParallelReplayResult = (Option<u64>, Vec<(Arc<MemTable>, u64)>);

/// Synchronous entry point for parallel segment replay.
pub(crate) fn replay_segments_sync(
	segments: &[SegmentRef],
	arena_size: usize,
) -> Result<ParallelReplayResult> {
	#[cfg(target_arch = "wasm32")]
	{
		let mut memtables = Vec::new();
		let mut max_seq: Option<u64> = None;
		for seg in segments {
			let (seg_max, batches) = decode_segment_batches(&seg.file_path, seg.id)?;
			if batches.is_empty() {
				continue;
			}
			if seg_max > 0 {
				max_seq = Some(max_seq.map_or(seg_max, |m| m.max(seg_max)));
			}
			let mut current_memtable = Arc::new(MemTable::new(arena_size));
			for batch in batches {
				match current_memtable.add(&batch) {
					Ok(()) => {}
					Err(Error::ArenaFull) => {
						if current_memtable.is_empty() {
							return Err(Error::Other(format!(
								"Batch too large for memtable (arena_size={arena_size})"
							)));
						}
						memtables.push((Arc::clone(&current_memtable), seg.id));
						current_memtable = Arc::new(MemTable::new(arena_size));
						current_memtable.add(&batch)?;
					}
					Err(e) => return Err(e),
				}
			}
			memtables.push((current_memtable, seg.id));
		}
		Ok((max_seq, memtables))
	}

	#[cfg(not(target_arch = "wasm32"))]
	{
		if let Ok(handle) = tokio::runtime::Handle::try_current() {
			if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
				return tokio::task::block_in_place(|| {
					handle.block_on(replay_segments_parallel(segments, arena_size))
				});
			}
		}

		let segs = segments.to_vec();
		std::thread::spawn(move || {
			let rt = tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.map_err(|e| Error::Other(format!("Failed to build temporary runtime: {e}")))?;
			rt.block_on(replay_segments_parallel(&segs, arena_size))
		})
		.join()
		.map_err(|_| Error::Other("WAL parallel recovery thread panicked".to_string()))?
	}
}

/// Replays a slice of segments in parallel using affinitypool, applying batches
/// sequentially into memtables to maintain strict WAL order.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) async fn replay_segments_parallel(
	segments: &[SegmentRef],
	arena_size: usize,
) -> Result<ParallelReplayResult> {
	if segments.is_empty() {
		return Ok((None, Vec::new()));
	}

	// 1. Concurrently decode all WAL segments in parallel on worker threads
	let mut decode_tasks = Vec::with_capacity(segments.len());
	for seg in segments {
		let path = seg.file_path.clone();
		let id = seg.id;
		let task = affinitypool::spawn(move || decode_segment_batches(&path, id));
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

	for (seg_idx, (seg_max_seq, batches)) in decoded_segments.into_iter().enumerate() {
		let segment_id = segments[seg_idx].id;
		if batches.is_empty() {
			continue;
		}

		global_max_seq.fetch_max(seg_max_seq, Ordering::SeqCst);

		let mut current_memtable = Arc::new(MemTable::new(arena_size));
		for batch in batches {
			match current_memtable.add(&batch) {
				Ok(()) => {}
				Err(Error::ArenaFull) => {
					if current_memtable.is_empty() {
						return Err(Error::Other(format!(
							"Batch too large for memtable (arena_size={arena_size})"
						)));
					}
					memtables.push((Arc::clone(&current_memtable), segment_id));
					current_memtable = Arc::new(MemTable::new(arena_size));
					current_memtable.add(&batch)?;
				}
				Err(e) => return Err(e),
			}
		}

		if !current_memtable.is_empty() {
			memtables.push((current_memtable, segment_id));
		}
	}

	let max_seq = global_max_seq.load(Ordering::SeqCst);
	let res_seq = if max_seq > 0 {
		Some(max_seq)
	} else {
		None
	};

	Ok((res_seq, memtables))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
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

		let (max_seq, memtables) = replay_segments_parallel(&[seg], 1024 * 1024).await.unwrap();
		assert_eq!(max_seq, Some(10));
		assert_eq!(memtables.len(), 1);

		let val = memtables[0].0.get(b"k1", None).unwrap();
		assert_eq!(val.1, b"v1");
	}
}
