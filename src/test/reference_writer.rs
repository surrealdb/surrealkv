//! The SSTable writer as it was before blocks went through a `BlockSink`, kept as test code.
//!
//! The bytes of a plaintext table are a compatibility promise: a file written by one version
//! has to be readable by the next, and a change to how blocks are framed, placed or counted
//! must not change a byte of it. Tests write the same inputs through this writer and through
//! `TableWriter` and compare the files.
//!
//! The code below is a copy of the writer, the partitioned index writer and the function that
//! frames a block, taken as they were, with their documentation left out. The pieces that
//! decide what goes in a block (`BlockWriter`, `PartitionedFilterWriter`, `TableMetadata`,
//! `compress_block`) are shared with the writer under test: this file is the oracle for how
//! blocks are framed, placed and indexed, not for how they are encoded. The footer, the
//! checksum and its mask are written out again here, from the format, instead of calling the
//! code that the writer uses.

use std::cmp::Ordering;
use std::io::Write;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crc32fast::Hasher as Crc32;

use crate::compression::CompressionSelector;
use crate::error::{Error, Result};
use crate::sstable::block::{BlockData, BlockHandle, BlockWriter};
use crate::sstable::error::SSTableError;
use crate::sstable::meta::TableMetadata;
use crate::sstable::partitioned_filter::PartitionedFilterWriter;
use crate::sstable::table::{compress_block, BLOCK_CKSUM_LEN, BLOCK_COMPRESS_LEN};
use crate::vlog::{ValueLocation, ValuePointer};
use crate::{Comparator, CompressionType, InternalKey, InternalKeyKind, Key, Options};

fn reference_checksum(block: &[u8], compression_type: CompressionType) -> Crc32 {
	let mut cksum = Crc32::new();
	cksum.update(block);
	cksum.update(&[compression_type as u8; BLOCK_COMPRESS_LEN]);
	cksum
}

fn reference_mask(crc: u32) -> u32 {
	crc.rotate_right(15).wrapping_add(0xa282_ead8)
}

/// The footer of a table: format 2 (LSMV2), checksum type 1, the two handles, zeros up to 42 bytes
/// and the magic number.
fn reference_footer(meta_index: &BlockHandle, index: &BlockHandle) -> Vec<u8> {
	let mut footer = vec![2u8, 1u8];
	footer.extend_from_slice(&meta_index.encode());
	footer.extend_from_slice(&index.encode());
	assert!(footer.len() <= 42, "the handles of a footer take at most 40 bytes");
	footer.resize(42, 0);
	footer.extend_from_slice(&[0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb]);
	assert_eq!(footer.len(), 50);
	footer
}

pub(crate) struct ReferenceIndexWriter {
	opts: Arc<Options>,

	index_blocks: Vec<BlockWriter>,

	current_block: BlockWriter,

	max_block_size: usize,

	cut_requested: bool,

	// Statistics (populated during finish())
	index_size: u64,
	num_partitions: u64,
	top_level_index_size: u64,
}

impl ReferenceIndexWriter {
	pub(crate) fn new(opts: Arc<Options>, max_block_size: usize) -> ReferenceIndexWriter {
		ReferenceIndexWriter {
			opts: Arc::clone(&opts),
			index_blocks: Vec::new(),
			current_block: BlockWriter::new(
				opts.block_size,
				opts.block_restart_interval,
				Arc::clone(&opts.internal_comparator),
			),
			max_block_size,
			cut_requested: false,
			index_size: 0,
			num_partitions: 0,
			top_level_index_size: 0,
		}
	}

	pub(crate) fn request_partition_cut(&mut self) {
		self.cut_requested = true;
	}

	pub(crate) fn index_size(&self) -> u64 {
		self.index_size
	}

	pub(crate) fn num_partitions(&self) -> u64 {
		self.num_partitions
	}

	pub(crate) fn top_level_index_size(&self) -> u64 {
		self.top_level_index_size
	}

	pub(crate) fn add(&mut self, key: &[u8], handle: &[u8]) -> Result<Option<Vec<u8>>> {
		// Start a new partition if the current one is full, or if a cut was
		// requested (only meaningful when the current partition has entries;
		// an empty partition cannot be cut).
		let mut finished_partition_last_key = None;
		if (self.cut_requested || self.current_block.size_estimate() >= self.max_block_size)
			&& self.current_block.entries() > 0
		{
			finished_partition_last_key = Some(self.finish_current_block());
		}
		self.cut_requested = false;
		self.current_block.add(key, handle)?;
		Ok(finished_partition_last_key)
	}

	fn finish_current_block(&mut self) -> Vec<u8> {
		let new_block = BlockWriter::new(
			self.opts.block_size,
			self.opts.block_restart_interval,
			Arc::clone(&self.opts.internal_comparator),
		);
		let mut finished_block = std::mem::replace(&mut self.current_block, new_block);
		// Finished partitions are retained until finish(); without this, each
		// keeps its full pre-sized buffer capacity for a few hundred bytes of
		// content, and with many partitions the empty capacity dominates
		// writer memory.
		finished_block.shrink_to_fit();
		let last_key = finished_block.last_key.clone();
		self.index_blocks.push(finished_block);
		last_key
	}

	fn write_compressed_block<W: Write>(
		writer: &mut W,
		block: BlockData,
		compression_type: CompressionType,
		offset: usize,
	) -> Result<(BlockHandle, usize)> {
		let compressed_block = compress_block(block, compression_type)?;
		reference_write_block_at_offset(writer, compressed_block, compression_type, offset)
	}

	pub(crate) fn finish<W: Write>(
		&mut self,
		writer: &mut W,
		compression_type: CompressionType,
		mut offset: usize,
	) -> Result<(BlockHandle, usize)> {
		let start_offset = offset;

		// Finish current block if it has entries
		if self.current_block.entries() > 0 {
			self.finish_current_block();
		}

		// Handle edge case: no blocks created
		if self.index_blocks.is_empty() {
			let new_block = BlockWriter::new(
				self.opts.block_size,
				self.opts.block_restart_interval,
				Arc::clone(&self.opts.internal_comparator),
			);
			let old_block = std::mem::replace(&mut self.current_block, new_block);
			self.index_blocks.push(old_block);
		}

		// Build top-level index
		let mut top_level_index = BlockWriter::new(
			self.opts.block_size,
			self.opts.block_restart_interval,
			Arc::clone(&self.opts.internal_comparator),
		);

		self.num_partitions = self.index_blocks.len() as u64;

		// Write each partition block and record in top-level
		let index_blocks = std::mem::take(&mut self.index_blocks);
		for block in index_blocks {
			// Use last key of partition as separator for top-level
			let separator_key = block.last_key.clone();
			let block_data = block.finish()?;

			// Skip empty blocks
			if separator_key.is_empty() {
				continue;
			}

			// Write partition block to file
			let (block_handle, new_offset) =
				Self::write_compressed_block(writer, block_data, compression_type, offset)?;
			offset = new_offset;

			// Add to top-level: separator_key → partition_handle
			top_level_index.add(&separator_key, &block_handle.encode())?;
		}

		// Write top-level index block
		let top_level_data = top_level_index.finish()?;
		self.top_level_index_size = top_level_data.len() as u64;
		let (handle, final_offset) =
			Self::write_compressed_block(writer, top_level_data, compression_type, offset)?;

		self.index_size = (final_offset - start_offset) as u64;

		Ok((handle, final_offset))
	}
}

pub(crate) struct ReferenceTableWriter<W: Write> {
	writer: W,
	opts: Arc<Options>,
	compression_selector: CompressionSelector,
	target_level: u8,

	pub(crate) meta: TableMetadata,

	offset: usize,

	prev_block_last_key: Vec<u8>,

	data_block: Option<BlockWriter>,

	partitioned_index: ReferenceIndexWriter,

	filter_block: Option<PartitionedFilterWriter>,

	last_index_separator: Vec<u8>,

	internal_cmp: Arc<dyn Comparator>,

	min_vlog_file_id: Option<u32>,
}

impl<W: Write> ReferenceTableWriter<W> {
	pub(crate) fn new(writer: W, id: u64, opts: Arc<Options>, target_level: u8) -> Self {
		let fb = opts
			.filter_policy
			.clone()
			.map(|policy| PartitionedFilterWriter::new(policy, opts.index_partition_size));

		let compression_selector = CompressionSelector::new(opts.compression_per_level.clone());

		let mut meta = TableMetadata::new();
		meta.properties.id = id;
		meta.properties.compression = compression_selector.select_compression(target_level);

		ReferenceTableWriter {
			writer,
			opts: Arc::clone(&opts),
			compression_selector,
			target_level,
			offset: 0,
			meta,
			prev_block_last_key: Vec::new(),

			data_block: Some(BlockWriter::new(
				opts.block_size,
				opts.block_restart_interval,
				Arc::clone(&opts.internal_comparator),
			)),
			partitioned_index: ReferenceIndexWriter::new(
				Arc::clone(&opts),
				opts.index_partition_size,
			),
			filter_block: fb,
			last_index_separator: Vec::new(),
			internal_cmp: Arc::clone(&opts.internal_comparator) as Arc<dyn Comparator>,
			min_vlog_file_id: None,
		}
	}

	pub(crate) fn add(&mut self, key: InternalKey, val: &[u8]) -> Result<()> {
		assert!(self.data_block.is_some());
		let enc_key = key.encode();

		// Validate ascending order
		if !self.prev_block_last_key.is_empty() {
			let order = self.internal_cmp.compare(&self.prev_block_last_key, &enc_key);
			assert_eq!(order, Ordering::Less, "Keys must be in ascending order");
		}

		// Track minimum vlog file_id if value is a vlog pointer. Peek the
		// meta byte instead of decoding: ValueLocation::decode copies the
		// entire value, which in this per-entry path costs a full copy of
		// every byte streamed through flush/compaction. The value of a range
		// delete is its end key, never a pointer.
		if key.kind() != InternalKeyKind::RangeDelete {
			if let Some(payload) = ValueLocation::peek_pointer_payload(val) {
				if let Ok(pointer) = ValuePointer::decode(payload) {
					let file_id = pointer.file_id;
					self.min_vlog_file_id =
						Some(self.min_vlog_file_id.map_or(file_id, |min| min.min(file_id)));
				}
			}
		}

		// Update metadata
		self.update_meta_properties(&key, val);

		// Flush block if it exceeds target size
		if self.data_block.as_ref().unwrap().size_estimate() > self.opts.block_size {
			self.write_data_block(&enc_key)?;
		}

		let dblock = self.data_block.as_mut().expect("No data block available");

		// Add to bloom filter
		if let Some(fblock) = self.filter_block.as_mut() {
			fblock.add_key(key.user_key.as_slice());
		}

		// Add to data block
		dblock.add(&enc_key, val)?;

		Ok(())
	}

	pub(crate) fn add_range_deletion(&mut self, start: Key, end: Key, seq_num: u64) {
		if !self
			.meta
			.range_deletions
			.iter()
			.any(|(s, e, sq)| s == &start && e == &end && *sq == seq_num)
		{
			self.meta.properties.num_range_deletions += 1;
			self.meta.range_deletions.push((start, end, seq_num));
		}
	}

	fn write_data_block(&mut self, next_key: &[u8]) -> Result<()> {
		assert!(self.data_block.is_some(), "No data block available to write.");

		let block = self.data_block.take().expect("Failed to take the existing data block");

		// Update statistics
		let props = &mut self.meta.properties;
		props.num_data_blocks += 1;
		props.block_count += 1;

		if props.block_size == 0 {
			props.block_size = block.size_estimate() as u32;
		}

		// Compute separator key between this block and the next
		// IMPORTANT: This creates an upper bound, not the exact last key!
		let separator_key = self.internal_cmp.separator(&block.last_key, next_key);
		self.prev_block_last_key.clone_from(&block.last_key);

		// Compress and write the block
		let contents = block.finish()?;
		let compression_type = self.compression_selector.select_compression(self.target_level);
		let handle = self.write_compressed_block(contents, compression_type)?;

		// Add index entry: separator_key → block_handle. If the filter's
		// current partition is full, ask the index to cut here so filter and
		// index partitions are created on the same boundary whichever limit
		// (index bytes or filter keys) is reached first.
		let handle_encoded = handle.encode();
		if self.filter_block.as_ref().is_some_and(|f| f.wants_cut()) {
			self.partitioned_index.request_partition_cut();
		}
		let finished_index_partition =
			self.partitioned_index.add(&separator_key, &handle_encoded)?;

		// Keep the bloom filter's partitions aligned with the index
		// partitions: when this entry finished an index partition, cut a
		// filter partition keyed by that partition's last separator, THEN
		// absorb the just-flushed block's key hashes (they belong to the
		// same partition as this block's index entry).
		if let Some(fblock) = self.filter_block.as_mut() {
			if let Some(partition_last_key) = &finished_index_partition {
				fblock.cut(partition_last_key);
			}
			fblock.absorb_block();
		}
		self.last_index_separator = separator_key;

		// Prepare new empty data block
		self.data_block = Some(BlockWriter::new(
			self.opts.block_size,
			self.opts.block_restart_interval,
			Arc::clone(&self.opts.internal_comparator),
		));

		Ok(())
	}

	pub(crate) fn finish(mut self) -> Result<usize> {
		// Set creation timestamp
		self.meta.properties.created_at = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map_err(|e| {
				let err = Error::from(SSTableError::FailedToGetSystemTime {
					source: e.to_string(),
				});
				tracing::error!("[TABLE_WRITER] {}", err);
				err
			})?
			.as_nanos();

		self.meta.properties.seqnos =
			(self.meta.smallest_seq_num.unwrap_or(0), self.meta.largest_seq_num.unwrap_or(0));

		// Store the minimum vlog file_id referenced by this SST.
		// 0 means no vlog references (sentinel value).
		self.meta.properties.oldest_vlog_file_id = self.min_vlog_file_id.unwrap_or(0) as u64;

		// Flush last data block if it has entries
		if self.data_block.as_ref().is_some_and(|db| db.entries() > 0) {
			let key_past_last =
				self.internal_cmp.successor(&self.data_block.as_ref().unwrap().last_key);
			self.write_data_block(&key_past_last)?;
		}

		// Build meta index block
		let mut meta_ix_block = BlockWriter::new(
			self.opts.block_size,
			self.opts.block_restart_interval,
			Arc::clone(&self.opts.internal_comparator),
		);

		// Write the partitioned bloom filter: one block per filter partition
		// (aligned with the index partitions), then the filter's top-level
		// index mapping separator keys to partition handles. The meta-index
		// entry is recorded here but added AFTER the "meta" entry below,
		// since meta-index keys must be added in ascending order and
		// "meta" < "partitionedfilter.…".
		let mut filter_index_entry: Option<(String, BlockHandle)> = None;
		if let Some(fblock) = self.filter_block.take() {
			let filter_name = format!("partitionedfilter.{}", fblock.filter_name());
			let last_separator = std::mem::take(&mut self.last_index_separator);
			let partitions = fblock.finish(&last_separator);

			if !partitions.is_empty() {
				let mut filter_index = BlockWriter::new(
					self.opts.index_partition_size,
					self.opts.block_restart_interval,
					Arc::clone(&self.opts.internal_comparator),
				);
				let mut filter_bytes_total = 0u64;
				for (separator, bits) in partitions {
					let handle = self.write_compressed_block(bits, CompressionType::None)?;
					filter_bytes_total += handle.size as u64;
					filter_index.add(&separator, &handle.encode())?;
				}
				let filter_index_data = filter_index.finish()?;
				let fi_handle =
					self.write_compressed_block(filter_index_data, CompressionType::None)?;
				self.meta.properties.filter_size = filter_bytes_total + fi_handle.size as u64;
				filter_index_entry = Some((filter_name, fi_handle));
			}
		}

		// Write partitioned index blocks and top-level index
		let compression_type = self.compression_selector.select_compression(self.target_level);
		let (ix_handle, new_offset) =
			self.partitioned_index.finish(&mut self.writer, compression_type, self.offset)?;
		self.offset = new_offset;

		// Record index statistics
		self.meta.properties.index_size = self.partitioned_index.index_size();
		self.meta.properties.index_partitions = self.partitioned_index.num_partitions();
		self.meta.properties.top_level_index_size = self.partitioned_index.top_level_index_size();

		// Write metadata to meta index
		let meta_key = InternalKey::new(Vec::from(b"meta"), 0, InternalKeyKind::Set);
		let meta_value = self.meta.encode();
		meta_ix_block.add(&meta_key.encode(), &meta_value)?;

		// Reference the partitioned filter's top-level index (added after
		// "meta" to keep the meta-index keys ascending)
		if let Some((name, handle)) = filter_index_entry {
			let mut handle_enc = vec![0u8; 16];
			let enc_len = handle.encode_into(&mut handle_enc);
			let key = InternalKey::new(Vec::from(name.as_bytes()), 0, InternalKeyKind::Set);
			meta_ix_block.add(&key.encode(), &handle_enc[0..enc_len])?;
		}

		// Write meta index block
		let meta_block = meta_ix_block.finish()?;
		let meta_ix_handle = self.write_compressed_block(meta_block, CompressionType::None)?;

		// Write footer
		let buf = reference_footer(&meta_ix_handle, &ix_handle);

		self.offset += self.writer.write(&buf[..])?;
		self.writer.flush()?;
		Ok(self.offset)
	}

	fn write_compressed_block(
		&mut self,
		block_data: BlockData,
		compression_type: CompressionType,
	) -> Result<BlockHandle> {
		let compressed_block = compress_block(block_data, compression_type)?;
		let (handle, new_offset) = reference_write_block_at_offset(
			&mut self.writer,
			compressed_block,
			compression_type,
			self.offset,
		)?;
		self.offset = new_offset;

		Ok(handle)
	}

	fn update_meta_properties(&mut self, key: &InternalKey, value: &[u8]) {
		let seq_num = key.seq_num();
		self.meta.update_seq_num(seq_num);

		if self.meta.smallest_point.is_none() {
			self.meta.set_smallest_point_key(key.clone());
		}
		self.meta.set_largest_point_key(key.clone());

		let props = &mut self.meta.properties;
		props.num_entries += 1;
		props.item_count += 1;
		props.raw_key_size += key.size() as u64;
		props.raw_value_size += value.len() as u64;

		// The times of the versions the table holds: a barrier, or a version that has no time,
		// spans every time.
		let (oldest, newest) = match key.kind() {
			InternalKeyKind::Delete | InternalKeyKind::RangeDelete | InternalKeyKind::Replace => {
				(0, u64::MAX)
			}
			kind => match ValueLocation::entry_time(kind, value) {
				Some(ts) => (ts, ts),
				None => (0, u64::MAX),
			},
		};
		props.oldest_key_time = Some(props.oldest_key_time.map_or(oldest, |t| t.min(oldest)));
		props.newest_key_time = Some(props.newest_key_time.map_or(newest, |t| t.max(newest)));

		if key.kind() == InternalKeyKind::RangeDelete {
			props.num_range_deletions += 1;
			self.meta.range_deletions.push((key.user_key.clone(), value.to_vec(), key.seq_num()));
		}

		if key.is_tombstone() {
			props.num_deletions += 1;
			props.tombstone_count += 1;
			if key.kind() == InternalKeyKind::SoftDelete {
				props.num_soft_deletes += 1;
			}
		}
		props.key_count += 1;
		props.data_size += (key.encoded_len() + value.len()) as u64;
	}
}

pub(crate) fn reference_write_block_at_offset<W: Write>(
	writer: &mut W,
	block: BlockData,
	compression_type: CompressionType,
	offset: usize,
) -> Result<(BlockHandle, usize)> {
	let cksum = reference_checksum(&block, compression_type);
	writer.write_all(&block)?;
	writer.write_all(&[compression_type as u8; BLOCK_COMPRESS_LEN])?;
	writer.write_all(&reference_mask(cksum.finalize()).to_le_bytes())?;

	let handle = BlockHandle::new(offset, block.len());
	let new_offset = offset + block.len() + BLOCK_CKSUM_LEN + BLOCK_COMPRESS_LEN;

	Ok((handle, new_offset))
}
