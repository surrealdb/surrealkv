//! # SSTable (Sorted String Table) Implementation
//!
//! This module implements the core SSTable format for persistent key-value storage.
//! An SSTable is an immutable, sorted file format used in LSM-tree based databases.
//!
//! ## File Structure
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        SSTable File Layout                       │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  [Data Block 1]           ← Contains sorted key-value pairs     │
//! │  [Data Block 2]                                                  │
//! │  ...                                                             │
//! │  [Data Block N]                                                  │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  [Index Block - Partition 1]  ← Points to data blocks           │
//! │  [Index Block - Partition 2]                                     │
//! │  ...                                                             │
//! │  [Index Block - Partition M]                                     │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  [Top-Level Index Block]  ← Points to partition index blocks    │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  [Meta Index Block]       ← Contains filter block handle, etc.  │
//! ├─────────────────────────────────────────────────────────────────┤
//! │  [Footer]                 ← Contains handles to meta/index      │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Concept: Separator Keys
//!
//! Index entries use "separator keys" as upper bounds, NOT the actual last key.
//!
//! ### Example: How Separators Work
//!
//! Given data blocks:
//! ```text
//! DataBlock 1: ["apple", "apricot"]  (actual last key: "apricot")
//! DataBlock 2: ["date", "fig"]       (actual last key: "fig")
//! ```
//!
//! When writing DataBlock 1, we compute a separator between "apricot" and "date":
//! ```text
//! separator = shortest key S where: "apricot" <= S < "date"
//!           = "b" or "c" (both valid, let's use "c")
//! ```
//!
//! Index becomes:
//! ```text
//! Index Entry 0: separator="c"   → DataBlock 1
//! Index Entry 1: separator="fig" → DataBlock 2
//! ```
//!
//! The separator "c" is an UPPER BOUND, meaning:
//! - Any key <= "c" MIGHT be in DataBlock 1
//! - But DataBlock 1 only contains keys up to "apricot"
//! - The range ("apricot", "c"] is a "gap" with no actual keys
//!
//! ### Why This Matters for Seeking
//!
//! When seeking for "banana":
//! 1. Index lookup: "banana" <= "c"? Yes → points to DataBlock 1
//! 2. Seek in DataBlock 1: "apple" < "banana", "apricot" < "banana", END!
//! 3. Iterator becomes INVALID (sought past all entries)
//! 4. Must advance to DataBlock 2 to find "date" (first key >= "banana")
//!
//! This is why `advance_to_valid_entry` exists!

use std::cmp::Ordering;
use std::io::Write;
use std::ops::Bound;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crc32fast::Hasher as Crc32;
use integer_encoding::{FixedInt, FixedIntWriter};
use snap::raw::max_compress_len;

use crate::compression::CompressionSelector;
use crate::error::{Error, Result};
use crate::sstable::block::{Block, BlockData, BlockHandle, BlockIterator, BlockWriter};
use crate::sstable::error::SSTableError;
use crate::sstable::filter_block::{FilterBlockReader, FilterBlockWriter};
use crate::sstable::index_block::{Index, IndexIterator, IndexWriter};
use crate::sstable::meta::TableMetadata;
use crate::vfs::File;
use crate::{
	Comparator,
	CompressionType,
	FilterPolicy,
	InternalKey,
	InternalKeyKind,
	InternalKeyRange,
	InternalKeyRef,
	LSMIterator,
	Options,
	Value,
	INTERNAL_KEY_SEQ_NUM_MAX,
	INTERNAL_KEY_TIMESTAMP_MAX,
};

// =============================================================================
// CONSTANTS
// =============================================================================

/// Footer is 42 bytes: format(2) + meta_handle(16) + index_handle(16) + magic(8)
const TABLE_FOOTER_LENGTH: usize = 42;

/// Full footer includes checksum: 42 + 8 = 50 bytes
const TABLE_FULL_FOOTER_LENGTH: usize = TABLE_FOOTER_LENGTH + 8;

/// Magic number to identify valid SSTable files (arbitrary but unique)
const TABLE_MAGIC_FOOTER_ENCODED: [u8; 8] = [0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb];

/// Checksum is CRC32 = 4 bytes
pub const BLOCK_CKSUM_LEN: usize = 4;

/// Compression type indicator = 1 byte
pub const BLOCK_COMPRESS_LEN: usize = 1;

/// Delta for masking CRC values (prevents weak checksums for simple data)
const MASK_DELTA: u32 = 0xa282_ead8;

// =============================================================================
// CHECKSUM UTILITIES
// =============================================================================

/// Masks a CRC32 checksum to make it more robust against simple patterns.
///
/// The masking operation:
/// 1. Rotates the CRC right by 15 bits
/// 2. Adds a constant delta
///
/// This prevents checksums of all-zeros or simple patterns from being weak.
pub(crate) fn mask(crc: u32) -> u32 {
	crc.rotate_right(15).wrapping_add(MASK_DELTA)
}

/// Unmasks a previously masked CRC32 checksum.
pub(crate) fn unmask(masked: u32) -> u32 {
	let rot = masked.wrapping_sub(MASK_DELTA);
	rot.rotate_left(15)
}

// =============================================================================
// TABLE FORMAT TYPES
// =============================================================================

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ChecksumType {
	CRC32c = 1,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TableFormat {
	LSMV1 = 1,
}

impl TableFormat {
	pub(crate) fn from_u8(val: u8) -> Result<Self> {
		match val {
			1 => Ok(TableFormat::LSMV1),
			_ => Err(Error::InvalidTableFormat),
		}
	}
}

// =============================================================================
// FOOTER
// =============================================================================

/// The Footer is the entry point for reading an SSTable.
///
/// ## Layout (50 bytes total)
///
/// ```text
/// Offset  Size  Field
/// ------  ----  -----
/// 0       1     Format version (1 = LSMV1)
/// 1       1     Checksum type (1 = CRC32c)
/// 2       16    Meta index block handle (varint encoded offset + size)
/// 18      16    Index block handle (varint encoded offset + size)
/// 34      8     Padding (zeros)
/// 42      8     Magic number (0x57fb808b247547db)
/// ```
///
/// ## Reading Process
///
/// 1. Read last 50 bytes of file
/// 2. Verify magic number
/// 3. Decode block handles
/// 4. Use handles to read index and meta blocks
#[derive(Debug, Clone)]
pub(crate) struct Footer {
	pub format: TableFormat,
	pub checksum: ChecksumType,
	pub meta_index: BlockHandle, // Points to meta index block
	pub index: BlockHandle,      // Points to top-level index block
}

impl Footer {
	pub(crate) fn new(metaix: BlockHandle, index: BlockHandle) -> Footer {
		Footer {
			meta_index: metaix,
			index,
			format: TableFormat::LSMV1,
			checksum: ChecksumType::CRC32c,
		}
	}

	/// Reads the footer bytes from the end of the file.
	pub(crate) fn read_from(reader: Arc<dyn File>, file_size: usize) -> Result<Vec<u8>> {
		if file_size < TABLE_FULL_FOOTER_LENGTH {
			return Err(Error::from(SSTableError::FileTooSmall {
				file_size,
				min_size: TABLE_FULL_FOOTER_LENGTH,
			}));
		}

		let mut buf = vec![0; TABLE_FULL_FOOTER_LENGTH];
		let offset = file_size - TABLE_FULL_FOOTER_LENGTH;
		reader.read_at(offset as u64, &mut buf)?;

		Ok(buf)
	}

	/// Decodes a footer from raw bytes.
	pub(crate) fn decode(buf: &[u8]) -> Result<Footer> {
		// Step 1: Validate magic number (last 8 bytes)
		let magic = &buf[buf.len() - TABLE_MAGIC_FOOTER_ENCODED.len()..];
		if magic != TABLE_MAGIC_FOOTER_ENCODED {
			return Err(Error::from(SSTableError::BadMagicNumber {
				magic: magic.to_vec(),
			}));
		}

		if buf.len() < TABLE_FOOTER_LENGTH {
			return Err(Error::from(SSTableError::FooterTooShort {
				footer_len: buf.len(),
			}));
		}

		// Step 2: Read format and checksum type
		let format = TableFormat::from_u8(buf[0])?;
		let checksum = match buf[1] {
			1 => ChecksumType::CRC32c,
			_ => {
				return Err(Error::from(SSTableError::InvalidChecksumType {
					checksum_type: buf[1],
				}))
			}
		};

		// Step 3: Decode block handles (varint encoded)
		let (meta_index, metalen) = BlockHandle::decode(&buf[2..])?;
		if metalen == 0 {
			return Err(Error::from(SSTableError::BadMetaIndexBlockHandle));
		}

		let (index_handle, _) = BlockHandle::decode(&buf[2 + metalen..])?;

		Ok(Footer {
			format,
			checksum,
			meta_index,
			index: index_handle,
		})
	}

	/// Encodes the footer into a byte buffer.
	pub(crate) fn encode(&self, dst: &mut [u8]) {
		match self.format {
			TableFormat::LSMV1 => {
				dst[..TABLE_FOOTER_LENGTH].fill(0);

				// Format version (1 byte)
				dst[0] = self.format as u8;

				// Checksum type (1 byte)
				dst[1] = self.checksum as u8;

				// Block handles (variable length, max 16 bytes each)
				let n = self.meta_index.encode_into(&mut dst[2..]);
				self.index.encode_into(&mut dst[2 + n..]);

				// Magic number at the end
				dst[TABLE_FOOTER_LENGTH..TABLE_FULL_FOOTER_LENGTH]
					.copy_from_slice(&TABLE_MAGIC_FOOTER_ENCODED);
			}
		}
	}
}

// =============================================================================
// TABLE WRITER
// =============================================================================

/// Writes key-value pairs to an SSTable file.
///
/// ## Writing Process
///
/// 1. Add key-value pairs via `add()` (must be in sorted order!)
/// 2. When a data block fills up, it's flushed and an index entry is created
/// 3. Call `finish()` to write remaining data, index blocks, and footer
///
/// ## Example: Writing 4 Keys
///
/// ```text
/// add("apple", "v1")   → Added to DataBlock 1
/// add("apricot", "v2") → Added to DataBlock 1
///                        Block full! Flush DataBlock 1, create index entry
/// add("date", "v3")    → Added to DataBlock 2
/// add("fig", "v4")     → Added to DataBlock 2
/// finish()             → Flush DataBlock 2, write index, write footer
/// ```
///
/// ## Index Entry Creation
///
/// When flushing DataBlock 1 (last_key="apricot") before adding "date":
///
/// ```text
/// separator = comparator.separator("apricot", "date")
///           = "b" (or any S where "apricot" <= S < "date")
///
/// Index entry: separator="b" → DataBlock 1 handle
/// ```
pub(crate) struct TableWriter<W: Write> {
	writer: W,
	opts: Arc<Options>,
	compression_selector: CompressionSelector,
	target_level: u8,

	pub(crate) meta: TableMetadata,

	/// Current write offset in the file
	offset: usize,

	/// Last key of the previous block (for ordering validation)
	prev_block_last_key: Vec<u8>,

	/// Current data block being built
	data_block: Option<BlockWriter>,

	/// Partitioned index writer
	partitioned_index: IndexWriter,

	/// Optional bloom filter writer
	filter_block: Option<FilterBlockWriter>,

	/// Comparator for internal keys
	internal_cmp: Arc<dyn Comparator>,
}

impl<W: Write> TableWriter<W> {
	pub(crate) fn new(writer: W, id: u64, opts: Arc<Options>, target_level: u8) -> Self {
		let fb = {
			if let Some(policy) = opts.filter_policy.clone() {
				let mut f = FilterBlockWriter::new(Arc::clone(&policy));
				f.start_block(0);
				Some(f)
			} else {
				None
			}
		};

		let compression_selector = CompressionSelector::new(opts.compression_per_level.clone());

		let mut meta = TableMetadata::new();
		meta.properties.id = id;
		meta.properties.compression = compression_selector.select_compression(target_level);

		TableWriter {
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
			partitioned_index: IndexWriter::new(Arc::clone(&opts), opts.index_partition_size),
			filter_block: fb,
			internal_cmp: Arc::clone(&opts.internal_comparator) as Arc<dyn Comparator>,
		}
	}

	/// Adds a key-value pair to the table.
	///
	/// ## Requirements
	/// - Keys MUST be added in strictly ascending order
	/// - Violating order causes assertion failure
	///
	/// ## Process
	/// 1. Validate key ordering
	/// 2. Add to bloom filter (if enabled)
	/// 3. If current block is full, flush it
	/// 4. Add key-value to current block
	pub(crate) fn add(&mut self, key: InternalKey, val: &[u8]) -> Result<()> {
		assert!(self.data_block.is_some());
		let enc_key = key.encode();

		// Validate ascending order
		if !self.prev_block_last_key.is_empty() {
			let order = self.internal_cmp.compare(&self.prev_block_last_key, &enc_key);
			assert_eq!(order, Ordering::Less, "Keys must be in ascending order");
		}

		// Initialize filter block on first key if needed
		if self.filter_block.is_none() {
			if let Some(filter_policy) = self.opts.filter_policy.as_ref() {
				let mut filter_block = FilterBlockWriter::new(Arc::clone(filter_policy));
				filter_block.start_block(0);
				self.filter_block = Some(filter_block);
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

	/// Flushes the current data block and creates an index entry.
	///
	/// ## Index Entry Creation
	///
	/// The index entry uses a "separator key" as an upper bound:
	///
	/// ```text
	/// Given:
	///   block.last_key = "apricot" (last key in this block)
	///   next_key = "date" (first key of next block)
	///
	/// Separator = shortest S where: "apricot" <= S < "date"
	///           = "b" (for example)
	///
	/// Index entry: separator="b" → this block's handle
	/// ```
	///
	/// ## Why Separators Matter
	///
	/// Using separators instead of actual last keys:
	/// 1. Reduces index size (shorter keys)
	/// 2. Maintains correctness (separator >= last key)
	/// 3. Creates "gaps" that require special handling during seeks
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

		// Add index entry: separator_key → block_handle
		let handle_encoded = handle.encode();
		self.partitioned_index.add(&separator_key, &handle_encoded)?;

		// Prepare new empty data block
		self.data_block = Some(BlockWriter::new(
			self.opts.block_size,
			self.opts.block_restart_interval,
			Arc::clone(&self.opts.internal_comparator),
		));

		Ok(())
	}

	/// Finalizes the SSTable by writing all remaining blocks and the footer.
	pub(crate) fn finish(mut self) -> Result<usize> {
		// Set creation timestamp
		self.meta.properties.created_at = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map_err(|e| {
				let err = Error::from(SSTableError::FailedToGetSystemTime {
					source: e.to_string(),
				});
				log::error!("[TABLE_WRITER] {}", err);
				err
			})?
			.as_nanos();

		self.meta.properties.seqnos =
			(self.meta.smallest_seq_num.unwrap_or(0), self.meta.largest_seq_num.unwrap_or(0));

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

		// Write filter block
		if let Some(fblock) = self.filter_block.take() {
			let filter_key = format!("filter.{}", fblock.filter_name());
			let fblock_data = fblock.finish();

			if !fblock_data.is_empty() {
				let fblock_handle =
					self.write_compressed_block(fblock_data, CompressionType::None)?;
				self.meta.properties.filter_size = fblock_handle.size as u64;

				let mut handle_enc = vec![0u8; 16];
				let enc_len = fblock_handle.encode_into(&mut handle_enc);
				let filter_key =
					InternalKey::new(Vec::from(filter_key.as_bytes()), 0, InternalKeyKind::Set, 0);
				meta_ix_block.add(&filter_key.encode(), &handle_enc[0..enc_len])?;
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
		let meta_key = InternalKey::new(Vec::from(b"meta"), 0, InternalKeyKind::Set, 0);
		let meta_value = self.meta.encode();
		meta_ix_block.add(&meta_key.encode(), &meta_value)?;

		// Write meta index block
		let meta_block = meta_ix_block.finish()?;
		let meta_ix_handle = self.write_compressed_block(meta_block, CompressionType::None)?;

		// Write footer
		let footer = Footer::new(meta_ix_handle, ix_handle);
		let mut buf = vec![0u8; TABLE_FULL_FOOTER_LENGTH];
		footer.encode(&mut buf);

		self.offset += self.writer.write(&buf[..])?;
		self.writer.flush()?;
		Ok(self.offset)
	}

	/// Compresses and writes a block, returning its handle.
	fn write_compressed_block(
		&mut self,
		block_data: BlockData,
		compression_type: CompressionType,
	) -> Result<BlockHandle> {
		let compressed_block = compress_block(block_data, compression_type)?;
		let (handle, new_offset) = write_block_at_offset(
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

		let ts = key.timestamp;
		props.oldest_key_time = Some(props.oldest_key_time.map_or(ts, |t| t.min(ts)));
		props.newest_key_time = Some(props.newest_key_time.map_or(ts, |t| t.max(ts)));

		if key.kind() == InternalKeyKind::RangeDelete {
			props.num_range_deletions += 1;
		}

		if key.is_tombstone() {
			props.num_deletions += 1;
			props.tombstone_count += 1;
			if key.kind() == InternalKeyKind::SoftDelete {
				props.num_soft_deletes += 1;
			}
		}
		props.key_count += 1;
		props.data_size += (key.encode().len() + value.len()) as u64;
	}
}

// =============================================================================
// BLOCK I/O UTILITIES
// =============================================================================

/// Writes a block to the file with compression type and checksum.
///
/// ## Block Format on Disk
///
/// ```text
/// +------------------+-------------------+-----------------+
/// | Block Data       | Compression Type  | Masked CRC32    |
/// | (variable)       | (1 byte)          | (4 bytes)       |
/// +------------------+-------------------+-----------------+
/// ```
pub(crate) fn write_block_at_offset<W: Write>(
	writer: &mut W,
	block: BlockData,
	compression_type: CompressionType,
	offset: usize,
) -> Result<(BlockHandle, usize)> {
	let cksum = calculate_checksum(&block, compression_type);
	writer.write_all(&block)?;
	writer.write_all(&[compression_type as u8; BLOCK_COMPRESS_LEN])?;
	writer.write_fixedint(mask(cksum.finalize()))?;

	let handle = BlockHandle::new(offset, block.len());
	let new_offset = offset + block.len() + BLOCK_CKSUM_LEN + BLOCK_COMPRESS_LEN;

	Ok((handle, new_offset))
}

/// Compresses block data using the specified compression type.
pub(crate) fn compress_block(
	raw_block: BlockData,
	compression: CompressionType,
) -> Result<BlockData> {
	match compression {
		CompressionType::SnappyCompression => {
			let mut enc = snap::raw::Encoder::new();
			let mut buffer = vec![0; max_compress_len(raw_block.len())];
			match enc.compress(&raw_block, buffer.as_mut_slice()) {
				Ok(size) => buffer.truncate(size),
				Err(e) => return Err(Error::Compression(e.to_string())),
			}
			Ok(buffer)
		}
		CompressionType::None => Ok(raw_block),
	}
}

/// Decompresses block data.
pub(crate) fn decompress_block(
	compressed_block: &[u8],
	compression: CompressionType,
) -> Result<Vec<u8>> {
	match compression {
		CompressionType::SnappyCompression => {
			let mut dec = snap::raw::Decoder::new();
			dec.decompress_vec(compressed_block).map_err(|e| Error::Decompression(e.to_string()))
		}
		CompressionType::None => Ok(Vec::from(compressed_block)),
	}
}

/// Reads and decodes the footer from a file.
fn read_footer(f: Arc<dyn File>, file_size: usize) -> Result<Footer> {
	let buf = Footer::read_from(f, file_size)?;
	Footer::decode(&buf)
}

/// Reads raw bytes at a block handle's location.
fn read_bytes(f: Arc<dyn File>, location: &BlockHandle) -> Result<Vec<u8>> {
	let mut buf = vec![0; location.size()];
	f.read_at(location.offset() as u64, &mut buf).map(|_| buf)
}

/// Calculates CRC32 checksum for a block.
pub(crate) fn calculate_checksum(block: &[u8], compression_type: CompressionType) -> Crc32 {
	let mut cksum = Crc32::new();
	cksum.update(block);
	cksum.update(&[compression_type as u8; BLOCK_COMPRESS_LEN]);
	cksum
}

/// Reads a filter block from the file.
pub(crate) fn read_filter_block(
	src: Arc<dyn File>,
	location: &BlockHandle,
	policy: Arc<dyn FilterPolicy>,
) -> Result<FilterBlockReader> {
	if location.size() == 0 {
		return Err(Error::FilterBlockEmpty);
	}
	let buf = read_bytes(src, location)?;
	Ok(FilterBlockReader::new(buf, policy))
}

fn read_writer_meta_properties(metaix: &Block) -> Result<Option<TableMetadata>> {
	let meta_key = InternalKey::new(Vec::from(b"meta"), 0, InternalKeyKind::Set, 0).encode();
	let mut metaindexiter = metaix.iter()?;
	metaindexiter.seek_internal(&meta_key)?;

	if metaindexiter.is_valid() {
		let k = metaindexiter.key();
		assert_eq!(k.user_key(), b"meta");
		let buf_bytes = metaindexiter.value_encoded()?;
		return Ok(Some(TableMetadata::decode(buf_bytes)?));
	}
	Ok(None)
}

/// Reads and verifies a table block from the file.
pub(crate) fn read_table_block(
	comparator: Arc<dyn Comparator>,
	f: Arc<dyn File>,
	location: &BlockHandle,
) -> Result<Block> {
	// Read block data
	let buf = read_bytes(Arc::clone(&f), location)?;

	// Read compression type (1 byte after block data)
	let compress = read_bytes(
		Arc::clone(&f),
		&BlockHandle::new(location.offset() + location.size(), BLOCK_COMPRESS_LEN),
	)?;

	// Read checksum (4 bytes after compression type)
	let cksum = read_bytes(
		Arc::clone(&f),
		&BlockHandle::new(
			location.offset() + location.size() + BLOCK_COMPRESS_LEN,
			BLOCK_CKSUM_LEN,
		),
	)?;

	// Verify checksum
	if !verify_table_block(&buf, compress[0], unmask(u32::decode_fixed(&cksum).unwrap())) {
		return Err(Error::from(SSTableError::ChecksumVerificationFailed {
			block_offset: location.offset() as u64,
		}));
	}

	// Decompress
	let block = decompress_block(&buf, CompressionType::try_from(compress[0])?)?;

	Ok(Block::new(block, comparator))
}

/// Verifies a block's checksum.
fn verify_table_block(block: &[u8], compression_type: u8, want: u32) -> bool {
	let mut cksum = Crc32::new();
	cksum.update(block);
	cksum.update(&[compression_type; BLOCK_COMPRESS_LEN]);
	cksum.finalize() == want
}

// =============================================================================
// TABLE (SSTable Reader)
// =============================================================================

#[derive(Clone)]
pub enum IndexType {
	Partitioned(Index),
}

/// An immutable SSTable reader.
///
/// ## Opening a Table
///
/// 1. Read footer
/// 2. Load top-level index block
/// 3. Load meta index block
/// 4. Extract table metadata and filter block
///
/// ## Point Lookup Flow
///
/// ```text
/// get("banana"):
///   1. Bloom filter check (early rejection if definitely not present)
///   2. Top-level index lookup → find partition block
///   3. Partition block lookup → find data block handle
///   4. Data block lookup → find key-value pair
/// ```
///
/// ## Range Scan Flow
///
/// Uses TableIterator which coordinates:
/// - first_level: IndexIterator (navigates index entries)
/// - second_level: BlockIterator (navigates data block entries)
#[derive(Clone)]
pub(crate) struct Table {
	pub id: u64,
	pub file: Arc<dyn File>,
	#[allow(unused)]
	pub file_size: u64,

	pub(crate) opts: Arc<Options>,
	pub(crate) meta: TableMetadata,

	pub(crate) index_block: IndexType,
	pub(crate) filter_reader: Option<FilterBlockReader>,
}

impl Table {
	/// Opens an SSTable file for reading.
	pub(crate) fn new(
		id: u64,
		opts: Arc<Options>,
		file: Arc<dyn File>,
		file_size: u64,
	) -> Result<Table> {
		// Step 1: Read footer
		let footer = read_footer(Arc::clone(&file), file_size as usize)?;

		// Step 2: Load partitioned index
		let index_block = {
			let partitioned_index =
				Index::new(id, Arc::clone(&opts), Arc::clone(&file), &footer.index)?;
			IndexType::Partitioned(partitioned_index)
		};

		// Step 3: Load meta index block
		let metaindexblock = read_table_block(
			Arc::clone(&opts.internal_comparator),
			Arc::clone(&file),
			&footer.meta_index,
		)?;

		// Step 4: Extract metadata
		let writer_metadata =
			read_writer_meta_properties(&metaindexblock)?.ok_or(Error::TableMetadataNotFound)?;

		// Step 5: Load filter block if configured
		let filter_reader = if opts.filter_policy.is_some() {
			Self::read_filter_block(&metaindexblock, Arc::clone(&file), &opts)?
		} else {
			None
		};

		Ok(Table {
			id,
			file,
			file_size,
			opts,
			filter_reader,
			index_block,
			meta: writer_metadata,
		})
	}

	fn read_filter_block(
		metaix: &Block,
		file: Arc<dyn File>,
		options: &Options,
	) -> Result<Option<FilterBlockReader>> {
		let filter_name = format!("filter.{}", options.filter_policy.as_ref().unwrap().name());
		let filter_key =
			InternalKey::new(Vec::from(filter_name.as_bytes()), 0, InternalKeyKind::Set, 0);

		let mut metaindexiter = metaix.iter()?;
		metaindexiter.seek_internal(&filter_key.encode())?;

		if metaindexiter.is_valid() {
			let k = metaindexiter.key();
			assert_eq!(k.user_key(), filter_name.as_bytes());
			let val = metaindexiter.value_encoded()?;

			let fbl = BlockHandle::decode(val);
			let filter_block_location = match fbl {
				Err(e) => {
					return Err(Error::from(SSTableError::FailedToDecodeBlockHandle {
						value_bytes: val.to_vec(),
						context: format!("error: {:?}", e),
					}));
				}
				Ok(res) => res.0,
			};
			if filter_block_location.size() > 0 {
				return Ok(Some(read_filter_block(
					file,
					&filter_block_location,
					Arc::clone(options.filter_policy.as_ref().unwrap()),
				)?));
			}
		}
		Ok(None)
	}

	/// Reads a data block, using cache if available.
	pub(crate) fn read_block(&self, location: &BlockHandle) -> Result<Arc<Block>> {
		// Check cache first
		if let Some(block) = self.opts.block_cache.get_data_block(self.id, location.offset() as u64)
		{
			return Ok(block);
		}

		// Cache miss: read from disk
		let b = read_table_block(
			Arc::clone(&self.opts.internal_comparator),
			Arc::clone(&self.file),
			location,
		)?;
		let b = Arc::new(b);

		// Insert into cache
		self.opts.block_cache.insert_data_block(self.id, location.offset() as u64, Arc::clone(&b));

		Ok(b)
	}

	/// Reads a data block with a custom comparator, using the history cache.
	///
	/// History blocks are cached separately from normal data blocks since they
	/// use different comparators for iteration.
	pub(crate) fn read_block_with_comparator(
		&self,
		location: &BlockHandle,
		comparator: Arc<dyn Comparator>,
	) -> Result<Arc<Block>> {
		// Check history cache first
		if let Some(block) =
			self.opts.block_cache.get_data_block_history(self.id, location.offset() as u64)
		{
			return Ok(block);
		}

		// Cache miss: read from disk
		let b = read_table_block(comparator, Arc::clone(&self.file), location)?;
		let b = Arc::new(b);

		// Insert into history cache
		self.opts.block_cache.insert_data_block_history(
			self.id,
			location.offset() as u64,
			Arc::clone(&b),
		);

		Ok(b)
	}

	/// Point lookup for a single key.
	///
	/// ## Lookup Process
	///
	/// ```text
	/// get("banana"):
	///
	/// Step 1: Bloom filter
	///   → may_contain("banana")? If NO → return None (definitely not here)
	///
	/// Step 2: Find partition in top-level index
	///   → Binary search for first separator >= "banana"
	///   → Load that partition block
	///
	/// Step 3: Find data block in partition
	///   → Seek for first entry >= "banana"
	///   → Entry value = data block handle
	///
	/// Step 4: Search data block
	///   → Load data block
	///   → Seek for "banana"
	///   → If found and user_key matches → return value
	/// ```
	pub(crate) fn get(&self, key: &InternalKey) -> Result<Option<(InternalKey, Value)>> {
		let key_encoded = key.encode();

		// Step 1: Bloom filter for early rejection
		if let Some(ref filters) = self.filter_reader {
			if !filters.may_contain(key.user_key.as_slice(), 0) {
				return Ok(None);
			}
		}

		let IndexType::Partitioned(partitioned_index) = &self.index_block;

		// Step 2: Find partition that could contain this key
		let Some((_, partition_handle)) =
			partitioned_index.find_block_handle_by_key(&key_encoded)?
		else {
			return Ok(None);
		};

		// Step 3: Load partition block and seek
		let partition_block = partitioned_index.load_block(partition_handle)?;
		let mut partition_iter = partition_block.iter()?;
		partition_iter.seek_internal(&key_encoded)?;

		if !partition_iter.is_valid() {
			return Ok(None);
		}

		// Decode data block handle from partition entry
		let (data_handle, _) = BlockHandle::decode(partition_iter.value_bytes()).map_err(|e| {
			Error::from(SSTableError::FailedToDecodeBlockHandle {
				value_bytes: partition_iter.value_bytes().to_vec(),
				context: format!("Failed to decode BlockHandle in get(): {e}"),
			})
		})?;

		// Step 4: Read data block and search
		let data_block = self.read_block(&data_handle)?;
		let mut iter = data_block.iter()?;
		iter.seek_internal(&key_encoded)?;

		if iter.is_valid() && iter.user_key() == key.user_key.as_slice() {
			Ok(Some((iter.key().to_owned(), iter.value_encoded()?.to_vec())))
		} else {
			Ok(None)
		}
	}

	/// Creates an iterator over the table with optional range bounds.
	pub(crate) fn iter(&self, range: Option<InternalKeyRange>) -> Result<TableIterator<'_>> {
		let range = range.unwrap_or((Bound::Unbounded, Bound::Unbounded));
		TableIterator::new(self, range)
	}

	/// Creates an iterator over the table with a custom comparator.
	pub(crate) fn iter_with_comparator(
		&self,
		range: Option<InternalKeyRange>,
		comparator: Arc<dyn Comparator>,
	) -> Result<TableIterator<'_>> {
		let range = range.unwrap_or((Bound::Unbounded, Bound::Unbounded));
		TableIterator::new_with_comparator(self, range, comparator)
	}

	pub(crate) fn is_key_in_key_range(&self, key: &InternalKey) -> bool {
		let Some(smallest) = &self.meta.smallest_point else {
			return true;
		};
		let Some(largest) = &self.meta.largest_point else {
			return true;
		};

		self.opts.comparator.compare(key.user_key.as_slice(), &smallest.user_key) >= Ordering::Equal
			&& self.opts.comparator.compare(key.user_key.as_slice(), &largest.user_key)
				<= Ordering::Equal
	}

	/// Checks if this table is completely BEFORE the query range.
	///
	/// Used to skip tables that can't possibly contain any keys in the range.
	/// A table is "before" the range if ALL its keys are less than the range start.
	///
	/// ## Bound Handling
	///
	/// | Range Lower Bound | Condition for "before" | Example |
	/// |-------------------|------------------------|---------|
	/// | Unbounded         | Never before           | - |
	/// | Included(k)       | table.largest < k      | Table ["a","b"], range starts at Included("c") → before |
	/// | Excluded(k)       | table.largest <= k     | Table ["a","b"], range starts at Excluded("b") → before |
	///
	/// ## Why the difference?
	///
	/// - `Included(k)`: Range includes k, so table is before only if largest < k
	/// - `Excluded(k)`: Range starts AFTER k, so table is before if largest <= k (even if largest
	///   == k, the range doesn't include k)
	///
	/// ```text
	/// Example: Table has keys ["apple", "banana"]  (largest = "banana")
	///
	/// is_before(Included("cherry"))?
	///   "banana" < "cherry"? Yes → table is before range ✓
	///
	/// is_before(Included("banana"))?
	///   "banana" < "banana"? No → table overlaps (has "banana") ✓
	///
	/// is_before(Excluded("banana"))?
	///   "banana" <= "banana"? Yes → table is before range ✓
	///   (range starts AFTER "banana", table ends AT "banana")
	///
	/// is_before(Excluded("apple"))?
	///   "banana" <= "apple"? No → table overlaps (has keys > "apple") ✓
	/// ```
	pub(crate) fn is_before_range(&self, range: &InternalKeyRange) -> bool {
		let Some(largest) = &self.meta.largest_point else {
			return false; // No metadata = conservatively assume overlap
		};

		match &range.0 {
			// Lower bound
			Bound::Unbounded => false, // Unbounded range includes everything
			Bound::Included(k) => {
				// Range includes k, table is before if largest < k
				self.opts.comparator.compare(&largest.user_key, &k.user_key) == Ordering::Less
			}
			Bound::Excluded(k) => {
				// Range starts AFTER k, table is before if largest <= k
				self.opts.comparator.compare(&largest.user_key, &k.user_key) != Ordering::Greater
			}
		}
	}

	/// Checks if this table is completely AFTER the query range.
	///
	/// Used to skip tables that can't possibly contain any keys in the range.
	/// A table is "after" the range if ALL its keys are greater than the range end.
	///
	/// ## Bound Handling
	///
	/// | Range Upper Bound | Condition for "after" | Example |
	/// |-------------------|----------------------|---------|
	/// | Unbounded         | Never after          | - |
	/// | Included(k)       | table.smallest > k   | Table ["d","e"], range ends at Included("c") → after |
	/// | Excluded(k)       | table.smallest >= k  | Table ["c","d"], range ends at Excluded("c") → after |
	///
	/// ## Why the difference?
	///
	/// - `Included(k)`: Range includes k, so table is after only if smallest > k
	/// - `Excluded(k)`: Range ends BEFORE k, so table is after if smallest >= k (even if smallest
	///   == k, the range doesn't include k)
	///
	/// ```text
	/// Example: Table has keys ["cherry", "date"]  (smallest = "cherry")
	///
	/// is_after(Included("banana"))?
	///   "cherry" > "banana"? Yes → table is after range ✓
	///
	/// is_after(Included("cherry"))?
	///   "cherry" > "cherry"? No → table overlaps (has "cherry") ✓
	///
	/// is_after(Excluded("cherry"))?
	///   "cherry" >= "cherry"? Yes → table is after range ✓
	///   (range ends BEFORE "cherry", table starts AT "cherry")
	///
	/// is_after(Excluded("date"))?
	///   "cherry" >= "date"? No → table overlaps (has keys < "date") ✓
	/// ```
	pub(crate) fn is_after_range(&self, range: &InternalKeyRange) -> bool {
		let Some(smallest) = &self.meta.smallest_point else {
			return false; // No metadata = conservatively assume overlap
		};

		match &range.1 {
			// Upper bound
			Bound::Unbounded => false, // Unbounded range includes everything
			Bound::Included(k) => {
				// Range includes k, table is after if smallest > k
				self.opts.comparator.compare(&smallest.user_key, &k.user_key) == Ordering::Greater
			}
			Bound::Excluded(k) => {
				// Range ends BEFORE k, table is after if smallest >= k
				self.opts.comparator.compare(&smallest.user_key, &k.user_key) != Ordering::Less
			}
		}
	}

	/// Checks if this table potentially overlaps with the query range.
	///
	/// Returns true if the table is neither completely before nor completely after the range.
	pub(crate) fn overlaps_with_range(&self, range: &InternalKeyRange) -> bool {
		!self.is_before_range(range) && !self.is_after_range(range)
	}
}

// =============================================================================
// Table ITERATOR
// =============================================================================

/// An iterator for efficient SSTable traversal.
///
/// ## Architecture
///
/// ```text
/// TableIterator
/// ├── first_level: IndexIterator
/// │   └── Iterates over index entries: separator_key → data_block_handle
/// ├── second_level: BlockIterator  
/// │   └── Iterates over data entries: key → value
/// └── range: bounds for filtering results
/// ```
///
/// ## Key Insight: Why We Need advance_to_valid_entry
///
/// Index entries use SEPARATOR KEYS as upper bounds, not actual last keys.
/// This creates "gaps" where a seek target falls within the separator range
/// but beyond all actual keys in the data block.
///
/// ### Example: The Gap Problem
///
/// ```text
/// DataBlock 1: ["apple", "apricot"]  (last key = "apricot")
/// DataBlock 2: ["date", "fig"]       (last key = "fig")
///
/// Index (using separator "c" between blocks):
///   Entry 0: separator="c"   → DataBlock 1
///   Entry 1: separator="fig" → DataBlock 2
///
/// Seek("banana"):
///   1. Index lookup: "banana" <= "c"? YES → DataBlock 1
///   2. Seek in DataBlock 1:
///      - "apple" < "banana" → continue
///      - "apricot" < "banana" → continue  
///      - END OF BLOCK! Iterator invalid.
///   3. advance_to_valid_entry():
///      - Current block exhausted, move to next
///      - Load DataBlock 2, seek_to_first()
///      - Land on "date" ← First key >= "banana"
/// ```
///
/// Without advance_to_valid_entry, the iterator would incorrectly
/// report no results when "date" and "fig" are valid matches!
pub(crate) struct TableIterator<'a> {
	table: &'a Table,

	/// First level: iterates over partition index entries
	/// Each entry: separator_key → data_block_handle
	first_level: IndexIterator<'a>,

	/// Second level: iterates over data block entries
	/// Each entry: key → value
	second_level: Option<BlockIterator>,

	/// Range bounds for filtering
	range: InternalKeyRange,

	/// Whether the iterator has been exhausted
	exhausted: bool,

	/// Optional custom comparator for iteration.
	/// When set, blocks are read with this comparator instead of the default.
	/// Used for specialized iteration patterns like history queries.
	custom_comparator: Option<Arc<dyn Comparator>>,
}

impl<'a> TableIterator<'a> {
	pub(crate) fn new(table: &'a Table, range: InternalKeyRange) -> Result<Self> {
		let IndexType::Partitioned(ref partitioned_index) = table.index_block;

		Ok(Self {
			table,
			first_level: IndexIterator::new(partitioned_index),
			second_level: None,
			range,
			exhausted: false,
			custom_comparator: None,
		})
	}

	/// Creates a new TableIterator with a custom comparator.
	///
	/// The custom comparator will be used for block iteration, enabling
	/// specialized iteration patterns like timestamp-based history queries.
	pub(crate) fn new_with_comparator(
		table: &'a Table,
		range: InternalKeyRange,
		comparator: Arc<dyn Comparator>,
	) -> Result<Self> {
		let IndexType::Partitioned(ref partitioned_index) = table.index_block;

		Ok(Self {
			table,
			first_level: IndexIterator::new(partitioned_index),
			second_level: None,
			range,
			exhausted: false,
			custom_comparator: Some(comparator),
		})
	}

	fn is_valid(&self) -> bool {
		!self.exhausted && self.second_level.as_ref().is_some_and(|iter| iter.is_valid())
	}

	fn mark_exhausted(&mut self) {
		self.exhausted = true;
		self.second_level = None;
	}

	/// Loads the data block pointed to by the current first-level position.
	///
	/// ## Process
	///
	/// 1. Get block handle from current first_level entry
	/// 2. Read the data block (with caching)
	/// 3. Create a BlockIterator for it
	fn init_data_block(&mut self) -> Result<()> {
		if !self.first_level.valid() {
			self.second_level = None;
			return Ok(());
		}

		let handle = self.first_level.block_handle()?;

		// Load data block - use custom comparator if set, otherwise use cached version
		let iter = if let Some(ref comparator) = self.custom_comparator {
			let block = self.table.read_block_with_comparator(&handle, Arc::clone(comparator))?;
			block.iter()?
		} else {
			let block = self.table.read_block(&handle)?;
			block.iter()?
		};
		self.second_level = Some(iter);
		Ok(())
	}

	/// Advances to the next valid entry when current position is invalid.
	///
	/// ## Why This Function Exists
	///
	/// After seeking, the data block iterator may be invalid because:
	///
	/// 1. **Seek overshot**: Target key > all keys in the block (due to separator key being an
	///    upper bound, not exact last key)
	///
	/// 2. **Block load failed**: I/O error or corruption
	///
	/// 3. **Block exhausted**: Called next() past last entry
	///
	/// ## Algorithm
	///
	/// ```text
	/// while (second_level is invalid):
	///     advance first_level to next index entry
	///     if first_level exhausted: return (no more data)
	///     load new data block
	///     position at first entry
	/// ```
	///
	/// ## Concrete Example
	///
	/// ```text
	/// Index: [("c", D1), ("fig", D2)]
	/// D1: ["apple", "apricot"]
	/// D2: ["date", "fig"]
	///
	/// After seek("banana") in D1:
	///   - second_level is INVALID (sought past "apricot")
	///
	/// advance_to_valid_entry():
	///   - Check: second_level.is_valid()? NO
	///   - first_level.next() → now points to ("fig", D2)
	///   - init_data_block() → load D2
	///   - second_level.seek_to_first() → points to "date"
	///   - Check: second_level.is_valid()? YES → done!
	///
	/// Result: iterator now at "date" (correct answer for seek("banana"))
	/// ```
	fn advance_to_valid_entry(&mut self) -> Result<()> {
		loop {
			// Success: current position is valid
			if self.second_level.as_ref().is_some_and(|iter| iter.is_valid()) {
				return Ok(());
			}

			// Move to next index entry
			if !self.first_level.valid() {
				self.second_level = None;
				return Ok(());
			}

			self.first_level.next()?;
			self.init_data_block()?;

			// Position at first entry of new block
			if let Some(ref mut iter) = self.second_level {
				iter.seek_to_first()?;
			}
		}
	}

	/// Retreats to the previous valid entry when current position is invalid.
	///
	/// Mirror of advance_to_valid_entry for backward iteration.
	fn retreat_to_valid_entry(&mut self) -> Result<()> {
		loop {
			if self.second_level.as_ref().is_some_and(|iter| iter.is_valid()) {
				return Ok(());
			}

			if !self.first_level.valid() {
				self.second_level = None;
				return Ok(());
			}

			self.first_level.prev()?;
			self.init_data_block()?;

			if let Some(ref mut iter) = self.second_level {
				iter.seek_to_last()?;
			}
		}
	}

	/// Checks if a user key satisfies the lower bound constraint.
	///
	/// ## Bound Semantics
	///
	/// | Bound Type | Condition | Example (bound="banana") |
	/// |------------|-----------|--------------------------|
	/// | Included   | key >= bound | "banana", "cherry" pass; "apple" fails |
	/// | Excluded   | key > bound  | "cherry" passes; "apple", "banana" fail |
	/// | Unbounded  | always true  | all keys pass |
	///
	/// ## Why User Key Comparison?
	///
	/// We compare USER keys, not internal keys, because:
	/// - Bounds are specified by users in terms of user keys
	/// - All versions of a user key should be treated uniformly
	/// - The seek functions handle version ordering separately
	fn satisfies_lower_bound(&self, user_key: &[u8]) -> bool {
		match &self.range.0 {
			Bound::Included(start) => {
				// key >= start (not less than)
				self.table.opts.comparator.compare(user_key, &start.user_key) != Ordering::Less
			}
			Bound::Excluded(start) => {
				// key > start (strictly greater)
				self.table.opts.comparator.compare(user_key, &start.user_key) == Ordering::Greater
			}
			Bound::Unbounded => true,
		}
	}

	/// Checks if a user key satisfies the upper bound constraint.
	///
	/// ## Bound Semantics
	///
	/// | Bound Type | Condition | Example (bound="banana") |
	/// |------------|-----------|--------------------------|
	/// | Included   | key <= bound | "apple", "banana" pass; "cherry" fails |
	/// | Excluded   | key < bound  | "apple" passes; "banana", "cherry" fail |
	/// | Unbounded  | always true  | all keys pass |
	fn satisfies_upper_bound(&self, user_key: &[u8]) -> bool {
		match &self.range.1 {
			Bound::Included(end) => {
				// key <= end (not greater than)
				self.table.opts.comparator.compare(user_key, &end.user_key) != Ordering::Greater
			}
			Bound::Excluded(end) => {
				// key < end (strictly less)
				self.table.opts.comparator.compare(user_key, &end.user_key) == Ordering::Less
			}
			Bound::Unbounded => true,
		}
	}

	fn current_user_key(&self) -> &[u8] {
		self.second_level.as_ref().unwrap().user_key()
	}

	/// Seeks to the first entry within bounds.
	///
	/// ## Internal Key Ordering Reminder
	///
	/// ```text
	/// For same user_key, seq_num is sorted DESCENDING:
	///   (user_key, MAX_SEQ) < (user_key, 5) < (user_key, 3) < (user_key, 0)
	///   ↑ SMALLEST (first)                                   ↑ LARGEST (last)
	/// ```
	///
	/// ## Bound Handling
	///
	/// ### Unbounded
	/// Simply seek to absolute first entry.
	///
	/// ### Included(key)
	/// Goal: Position at first entry with user_key >= bound.user_key
	///
	/// The bound's internal_key has seq_num=MAX_SEQ (from user_range_to_internal_range),
	/// which is the SMALLEST internal key for that user_key.
	/// Seeking to it lands on the first version of that user_key (or next user_key).
	///
	/// ```text
	/// Example: Included("banana") with data [("apple",5), ("banana",7), ("banana",2)]
	///
	/// seek(("banana", MAX_SEQ)):
	///   ("apple", 5) < ("banana", MAX_SEQ)  → skip
	///   ("banana", 7) >= ("banana", MAX_SEQ) → land here ✓
	///
	/// Result: ("banana", 7) - first entry with user_key >= "banana"
	/// ```
	///
	/// ### Excluded(key)
	/// Goal: Position at first entry with user_key > bound.user_key
	///
	/// We create seek_key with seq_num=0 (LARGEST internal key for that user_key).
	/// This seeks PAST all versions of the excluded key.
	///
	/// ```text
	/// Example: Excluded("apple") with data [("apple",5), ("apple",3), ("banana",7)]
	///
	/// seek(("apple", 0)):
	///   ("apple", 5) < ("apple", 0)  → skip (5 > 0 descending)
	///   ("apple", 3) < ("apple", 0)  → skip
	///   ("banana", 7) >= ("apple", 0) → land here ✓
	///
	/// Check: user_key == "apple"? No, it's "banana" → done
	/// Result: ("banana", 7) - first entry with user_key > "apple"
	/// ```
	///
	/// Edge case: If ("apple", 0) actually exists, we land on it and advance.
	pub(crate) fn seek_to_first(&mut self) -> Result<()> {
		self.exhausted = false;

		let lower_bound = self.range.0.clone();

		match lower_bound {
			Bound::Unbounded => {
				// No lower bound: seek to absolute first
				self.first_level.seek_to_first()?;
				self.init_data_block()?;
				if let Some(ref mut iter) = self.second_level {
					iter.seek_to_first()?;
				}
				self.advance_to_valid_entry()?;
			}
			Bound::Included(ref internal_key) => {
				// internal_key has seq_num=MAX_SEQ (smallest for this user_key)
				// Seeking to it finds the first version of this user_key
				self.seek_internal(&internal_key.encode())?;
			}
			Bound::Excluded(ref internal_key) => {
				// Create seek key with seq_num=0 (LARGEST for this user_key)
				// This seeks PAST all versions of the excluded key
				let seek_key = InternalKey::new(
					internal_key.user_key.clone(),
					0, // Largest internal key for this user_key
					InternalKeyKind::Set,
					0,
				);
				self.seek_internal(&seek_key.encode())?;

				// Edge case: if we somehow landed on exactly (user_key, 0), advance past it
				if self.is_valid() && self.current_user_key() == internal_key.user_key.as_slice() {
					self.advance_internal()?;
				}
			}
		}

		// Verify we didn't overshoot the upper bound
		if self.is_valid() && !self.satisfies_upper_bound(self.current_user_key()) {
			self.mark_exhausted();
		}
		Ok(())
	}

	/// Seeks to the last entry within bounds.
	///
	/// Unlike seek_to_first (which finds first key >= target), seek_to_last
	/// find the last key <= target.
	///
	/// ## Bound Handling
	///
	/// ### Unbounded
	/// Simply seek to absolute last entry.
	///
	/// ### Included(key)
	/// Goal: Position at last entry with user_key <= bound.user_key
	///
	/// Strategy: Seek to (user_key, 0) which is the LARGEST internal key
	/// for that user_key. This will land on the next user_key (or past end),
	/// then we back up to get the last version of the included key.
	///
	/// ```text
	/// Example: Included("banana") with data [("apple",3), ("banana",7), ("banana",2), ("cherry",4)]
	///
	/// seek(("banana", 0)):
	///   ("apple", 3) < ("banana", 0)    → skip
	///   ("banana", 7) < ("banana", 0)   → skip (7 > 0 descending)
	///   ("banana", 2) < ("banana", 0)   → skip (2 > 0 descending)
	///   ("cherry", 4) >= ("banana", 0)  → land here
	///
	/// Check: "cherry" > "banana"? Yes → prev()
	/// Result: ("banana", 2) - last entry with user_key <= "banana" ✓
	/// ```
	///
	/// ### Excluded(key)
	/// Goal: Position at last entry with user_key < bound.user_key
	///
	/// Strategy: Seek to (user_key, MAX_SEQ) which is the SMALLEST internal key
	/// for that user_key. This lands on the first version of the excluded key
	/// (or past it), then we back up to get entries strictly before it.
	///
	/// ```text
	/// Example: Excluded("banana") with data [("apple",5), ("apple",3), ("banana",7)]
	///
	/// seek(("banana", MAX_SEQ)):
	///   ("apple", 5) < ("banana", MAX_SEQ)  → skip
	///   ("apple", 3) < ("banana", MAX_SEQ)  → skip
	///   ("banana", 7) >= ("banana", MAX_SEQ) → land here
	///
	/// Check: "banana" < "banana"? No (Equal, not Less) → prev()
	/// Result: ("apple", 3) - last entry with user_key < "banana" ✓
	/// ```
	pub(crate) fn seek_to_last(&mut self) -> Result<()> {
		self.exhausted = false;

		let upper_bound = self.range.1.clone();

		match upper_bound {
			Bound::Unbounded => {
				// No upper bound: seek to absolute last
				self.first_level.seek_to_last()?;
				self.init_data_block()?;
				if let Some(ref mut iter) = self.second_level {
					iter.seek_to_last()?;
				}
				self.retreat_to_valid_entry()?;
			}
			Bound::Included(ref internal_key) => {
				// Seek to (user_key, 0) = LARGEST internal key for this user_key
				// This positions past all versions, then we back up
				let bound_user_key = internal_key.user_key.clone();
				let seek_key =
					InternalKey::new(internal_key.user_key.clone(), 0, InternalKeyKind::Set, 0);
				self.seek_internal(&seek_key.encode())?;

				if !self.is_valid() {
					// Seek went past end of table, position at absolute last
					self.position_to_absolute_last()?;
				} else {
					// Check if we landed past the bound (on a later user_key)
					let cmp = self
						.table
						.opts
						.comparator
						.compare(self.current_user_key(), bound_user_key.as_slice());
					if cmp == Ordering::Greater {
						// We're past the bound, back up one entry
						self.prev_internal()?;
					}
					// If cmp is Equal or Less, we're already on a valid position
					// (this can happen if (user_key, 0) exists or we landed on earlier key)
				}
			}
			Bound::Excluded(ref internal_key) => {
				// Seek to (user_key, MAX_SEQ) = SMALLEST internal key for this user_key
				// This positions at first version of excluded key, then we back up
				let bound_user_key = internal_key.user_key.clone();
				let seek_key = InternalKey::new(
					internal_key.user_key.clone(),
					INTERNAL_KEY_SEQ_NUM_MAX,
					InternalKeyKind::Max,
					INTERNAL_KEY_TIMESTAMP_MAX,
				);
				self.seek_internal(&seek_key.encode())?;

				if !self.is_valid() {
					// Seek went past end, position at absolute last
					self.position_to_absolute_last()?;
				}

				// For Excluded: back up if we're AT or PAST the bound
				// (we need to be strictly BEFORE it)
				if self.is_valid() {
					let cmp = self
						.table
						.opts
						.comparator
						.compare(self.current_user_key(), bound_user_key.as_slice());
					if cmp != Ordering::Less {
						// We're on the excluded key or past it, back up
						self.prev_internal()?;
					}
				}
			}
		}

		// Verify we didn't back up past the lower bound
		if self.is_valid() && !self.satisfies_lower_bound(self.current_user_key()) {
			self.mark_exhausted();
		}
		Ok(())
	}

	/// Internal seek implementation.
	///
	/// ## Process
	///
	/// ```text
	/// seek_internal("banana"):
	///   1. first_level.seek("banana")
	///      → Finds partition entry with separator >= "banana"
	///      → That entry points to a data block
	///
	///   2. init_data_block()
	///      → Load the data block
	///
	///   3. second_level.seek_internal("banana")
	///      → Seek within data block for key >= "banana"
	///      → MAY GO PAST ALL ENTRIES if "banana" > all keys!
	///
	///   4. advance_to_valid_entry()
	///      → If step 3 went past all entries, advance to next block
	/// ```
	fn seek_internal(&mut self, target: &[u8]) -> Result<()> {
		// Step 1: Position first_level at the right index entry
		self.first_level.seek(target)?;

		// Step 2: Load the data block it points to
		self.init_data_block()?;

		// Step 3: Seek within the data block
		if let Some(ref mut iter) = self.second_level {
			iter.seek_internal(target)?;
		}

		// Step 4: Handle case where seek went past all entries
		// This is NECESSARY because separator keys are upper bounds!
		self.advance_to_valid_entry()?;
		Ok(())
	}

	fn position_to_absolute_last(&mut self) -> Result<()> {
		self.first_level.seek_to_last()?;
		self.init_data_block()?;

		if let Some(ref mut iter) = self.second_level {
			iter.seek_to_last()?;
		}

		self.retreat_to_valid_entry()?;
		Ok(())
	}

	fn advance_internal(&mut self) -> Result<bool> {
		if let Some(ref mut iter) = self.second_level {
			if iter.advance()? {
				return Ok(true);
			}
		}
		self.advance_to_valid_entry()?;
		Ok(self.is_valid())
	}

	fn prev_internal(&mut self) -> Result<bool> {
		if let Some(ref mut iter) = self.second_level {
			if iter.prev_internal()? {
				return Ok(true);
			}
		}
		self.retreat_to_valid_entry()?;
		Ok(self.is_valid())
	}
}

impl LSMIterator for TableIterator<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.exhausted = false;
		self.seek_internal(target)?;

		if self.is_valid() && !self.satisfies_upper_bound(self.current_user_key()) {
			self.mark_exhausted();
		}
		Ok(self.is_valid())
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.seek_to_first()?;
		Ok(self.is_valid())
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.seek_to_last()?;
		Ok(self.is_valid())
	}

	fn next(&mut self) -> Result<bool> {
		// Auto-position on first call
		if !self.is_valid() && !self.exhausted {
			return self.seek_first();
		}

		if !self.is_valid() {
			return Ok(false);
		}

		self.advance_internal()?;

		if !self.is_valid() || !self.satisfies_upper_bound(self.current_user_key()) {
			self.mark_exhausted();
		}
		Ok(self.is_valid())
	}

	fn prev(&mut self) -> Result<bool> {
		// Auto-position on first call
		if !self.is_valid() && !self.exhausted {
			return self.seek_last();
		}

		if !self.is_valid() {
			return Ok(false);
		}

		self.prev_internal()?;

		if !self.is_valid() || !self.satisfies_lower_bound(self.current_user_key()) {
			self.mark_exhausted();
		}
		Ok(self.is_valid())
	}

	fn valid(&self) -> bool {
		self.is_valid()
	}

	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.valid());
		InternalKeyRef::from_encoded(self.second_level.as_ref().unwrap().key_bytes())
	}

	fn value_encoded(&self) -> Result<&[u8]> {
		debug_assert!(self.valid());
		Ok(self.second_level.as_ref().unwrap().value_bytes())
	}
}
