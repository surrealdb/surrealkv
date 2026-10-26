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
//! A sealed table, which is what the checksum type of its footer says, has a header before its
//! first data block and sealed blocks, see `block_io`. The footer is never sealed and never holds
//! anything secret: it is two block handles, the format and the layout of the blocks.
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

use snap::raw::max_compress_len;

use crate::compression::CompressionSelector;
use crate::error::{Error, Result};
use crate::sstable::block::{
	Block,
	BlockData,
	BlockHandle,
	BlockIterator,
	BlockWriter,
	SmallKeyBuf,
};
#[cfg(test)]
use crate::sstable::block_io::Protection;
use crate::sstable::block_io::{BlockKind, BlockSink, BlockSource};
use crate::sstable::error::SSTableError;
use crate::sstable::filter_block::FilterBlockReader;
use crate::sstable::index_block::{Index, IndexIterator, IndexWriter};
use crate::sstable::meta::TableMetadata;
use crate::sstable::partitioned_filter::{PartitionedFilterReader, PartitionedFilterWriter};
use crate::sstable::sealed::SstStarter;
use crate::vfs::File;
use crate::vlog::{ValueLocation, ValuePointer};
use crate::{
	Comparator,
	CompressionType,
	InternalKey,
	InternalKeyKind,
	InternalKeyRange,
	InternalKeyRef,
	Key,
	LSMIterator,
	Options,
	Value,
	INTERNAL_KEY_SEQ_NUM_MAX,
};

// =============================================================================
// CONSTANTS
// =============================================================================

/// Footer is 42 bytes: format(2) + meta_handle(16) + index_handle(16) + magic(8)
const TABLE_FOOTER_LENGTH: usize = 42;

/// Full footer includes checksum: 42 + 8 = 50 bytes
pub(crate) const TABLE_FULL_FOOTER_LENGTH: usize = TABLE_FOOTER_LENGTH + 8;

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

/// How the blocks of a table are checked, which the footer says and which fixes the layout of
/// every block of the file.
///
/// The second value is not a different checksum: it is the signal that a table is sealed. Its
/// blocks are authenticated ciphertext followed by the trailer of nine bytes, and the file begins
/// with a header. It is a value of this byte, and not a flag somewhere else in the footer, because
/// a reader that does not know it must stop at the footer: bytes that were ignored, such as the
/// padding, would be read as a plain table by a reader that has never heard of sealing.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ChecksumType {
	/// Plain blocks, each followed by its compression type and a masked CRC32.
	CRC32c = 1,
	/// Sealed blocks, each followed by its compression type, its length and a masked CRC32. The
	/// CRC is the same one, and it only tells damage from tampering, which the seal tells.
	SealedCRC32c = 2,
}

/// The table format recorded in the footer.
///
/// Both formats share one container (blocks, checksums, partitioned index, filters, table
/// metadata); they differ in the layout of the internal keys inside it.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TableFormat {
	/// Written by SurrealKV up to v0.21.4, `user_key ‖ trailer ‖ timestamp` (16-byte suffix), and
	/// by development builds of the current engine between the removal of the key timestamp
	/// and the introduction of `LSMV2`, with the flat layout. The footer cannot tell the two
	/// apart, the keys do: this code reads the flat ones, and refuses the others
	/// (`Error::LegacyFormat`), which are read by the standalone `surrealkv-compat-v1` and
	/// converted by `crate::migrate`.
	LSMV1 = 1,
	/// Written since the introduction of this format: the flat layout, `user_key ‖ trailer`
	/// (8-byte suffix), always.
	LSMV2 = 2,
}

impl TableFormat {
	pub(crate) fn from_u8(val: u8) -> Result<Self> {
		match val {
			1 => Ok(TableFormat::LSMV1),
			2 => Ok(TableFormat::LSMV2),
			_ => Err(Error::InvalidTableFormat),
		}
	}
}

/// The key under which the table writer stores the table properties in the meta-index block.
const META_PROPERTIES_KEY: &[u8] = b"meta";

/// Length of the suffix of an internal key of this engine: the trailer.
const FLAT_SUFFIX_LEN: usize = 8;
/// Length of the suffix of an internal key of LSMV1, the format of SurrealKV up to v0.21.4: the
/// trailer and the timestamp of the write.
const LEGACY_SUFFIX_LEN: usize = 16;

/// Refuses the table whose meta-index block is `metaix` if its keys are in the layout of LSMV1.
///
/// This code does not read that layout: `Tree::new` converts such tables to LSMV2 with the help of
/// `surrealkv-compat-v1` before it opens them, and a table that is met here has not been, so it
/// is refused instead of being misread. Nothing but the length of a key is looked at.
///
/// Neither the footer nor the table properties record the layout, but the writer stores the
/// properties under the internal key of the literal user key `meta`, so the length of that one
/// key is exactly `4 + suffix`: 12 bytes when flat, 20 when LSMV1. A user key cannot influence
/// it, which is why this beats sniffing data keys. The meta-index block is never compressed.
fn refuse_legacy_keys(id: u64, format: TableFormat, metaix: &Block) -> Result<()> {
	let mut iter = metaix.iter()?;
	iter.seek_to_first()?;
	while iter.is_valid() {
		let key = iter.key_bytes();
		if key.starts_with(META_PROPERTIES_KEY) {
			return match key.len() - META_PROPERTIES_KEY.len() {
				FLAT_SUFFIX_LEN => Ok(()),
				// LSMV2 tables are flat by definition, and no engine wrote this under an LSMV2
				// footer: the file was changed.
				LEGACY_SUFFIX_LEN if format == TableFormat::LSMV2 => {
					Err(Error::CorruptedTableMetadata(format!(
						"table {id}: the footer says LSMV2 but the keys use the layout of LSMV1"
					)))
				}
				LEGACY_SUFFIX_LEN => Err(Error::LegacyFormat(format!(
					"table {id} is in LSMV1, the format of SurrealKV up to v0.21.4; open the \
					 database directory with Tree::new to convert it"
				))),
				other => Err(Error::CorruptedTableMetadata(format!(
					"the table properties key has a {other}-byte suffix, expected \
					 {FLAT_SUFFIX_LEN} or {LEGACY_SUFFIX_LEN}"
				))),
			};
		}
		iter.advance()?;
	}
	Err(Error::TableMetadataNotFound)
}

/// Opens the footer and meta-index block of the table in `file` and refuses it, as `Table::new`
/// does, if it is in LSMV1. Reads only those two: no index, filter or data block, so it is cheap
/// enough to ask of every table of a directory before anything of it is replaced.
pub(crate) fn refuse_legacy_table(
	id: u64,
	opts: &Options,
	file: Arc<dyn File>,
	file_size: u64,
) -> Result<()> {
	let (source, footer) = BlockSource::open_table(file, file_size, id, opts.encryption_context())?;
	let metaix = source.read_block(
		BlockKind::MetaIndex,
		&footer.meta_index,
		Arc::clone(&opts.internal_comparator),
	)?;
	refuse_legacy_keys(id, footer.format, &metaix)
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
/// 0       1     Format version (1 = LSMV1, 2 = LSMV2)
/// 1       1     Checksum type (1 = CRC32c, 2 = sealed blocks)
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
///
/// The footer is the one part of a table that is read before anything can be checked, and
/// nothing authenticates it: whoever can write the file can write any footer. What it says about
/// the layout of the blocks is therefore a claim that the reader of a database whose tables are
/// sealed must not take at its word.
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
			format: TableFormat::LSMV2,
			checksum: ChecksumType::CRC32c,
		}
	}

	/// Decodes a footer from raw bytes.
	pub(crate) fn decode(buf: &[u8]) -> Result<Footer> {
		// The length is checked first: the magic number is read from the end of the buffer.
		if buf.len() < TABLE_FOOTER_LENGTH {
			return Err(Error::from(SSTableError::FooterTooShort {
				footer_len: buf.len(),
			}));
		}

		// Step 1: Validate magic number (last 8 bytes)
		let magic = &buf[buf.len() - TABLE_MAGIC_FOOTER_ENCODED.len()..];
		if magic != TABLE_MAGIC_FOOTER_ENCODED {
			return Err(Error::from(SSTableError::BadMagicNumber {
				magic: magic.to_vec(),
			}));
		}

		// Step 2: Read format and checksum type
		let format = TableFormat::from_u8(buf[0])?;
		let checksum = match buf[1] {
			1 => ChecksumType::CRC32c,
			2 => ChecksumType::SealedCRC32c,
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
			TableFormat::LSMV1 | TableFormat::LSMV2 => {
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
	/// Writes the blocks and the footer, and knows where the next block goes.
	sink: BlockSink<W>,
	opts: Arc<Options>,
	compression_selector: CompressionSelector,
	target_level: u8,

	pub(crate) meta: TableMetadata,

	/// Last key of the previous block (for ordering validation)
	prev_block_last_key: Vec<u8>,

	/// Current data block being built
	data_block: Option<BlockWriter>,

	/// Partitioned index writer
	partitioned_index: IndexWriter,

	/// Optional partitioned bloom filter writer (partitions aligned with the
	/// index partitions; see `partitioned_filter.rs`)
	filter_block: Option<PartitionedFilterWriter>,

	/// Last separator key handed to the partitioned index — the final filter
	/// partition is keyed by it at finish()
	last_index_separator: Vec<u8>,

	/// Comparator for internal keys
	internal_cmp: Arc<dyn Comparator>,

	/// Minimum vlog file_id seen across all ValuePointers in this SST.
	/// Used to track which vlog files this SST references.
	/// None if no vlog pointers have been seen yet.
	min_vlog_file_id: Option<u32>,
}

impl<W: Write> TableWriter<W> {
	/// A writer of table `id`. If the options carry the encryption context of the database the
	/// table is sealed, and its file is started when its first block is written: the key of the
	/// file is taken then, so a key that cannot be had is the error of `add` or `finish` and
	/// not of this call, and no key is held by a writer that never writes.
	///
	/// Every writer takes a key of its own, also for an id that was used before: a table that is
	/// written again after a crash, or by a compaction that was retried, is another file.
	pub(crate) fn new(writer: W, id: u64, opts: Arc<Options>, target_level: u8) -> Self {
		let sink = match opts.encryption_context() {
			Some(context) => {
				BlockSink::deferred(writer, Box::new(SstStarter::new(Arc::clone(context), id)))
			}
			None => BlockSink::new(writer),
		};
		Self::with_sink(sink, id, opts, target_level)
	}

	/// A writer that writes its blocks through `sink`.
	pub(crate) fn with_sink(
		sink: BlockSink<W>,
		id: u64,
		opts: Arc<Options>,
		target_level: u8,
	) -> Self {
		let fb = opts
			.filter_policy
			.clone()
			.map(|policy| PartitionedFilterWriter::new(policy, opts.index_partition_size));

		let compression_selector = CompressionSelector::new(opts.compression_per_level.clone());

		let mut meta = TableMetadata::new();
		meta.properties.id = id;
		meta.properties.compression = compression_selector.select_compression(target_level);

		TableWriter {
			sink,
			opts: Arc::clone(&opts),
			compression_selector,
			target_level,
			meta,
			prev_block_last_key: Vec::new(),

			data_block: Some(BlockWriter::new(
				opts.block_size,
				opts.block_restart_interval,
				Arc::clone(&opts.internal_comparator),
			)),
			partitioned_index: IndexWriter::new(Arc::clone(&opts), opts.index_partition_size),
			filter_block: fb,
			last_index_separator: Vec::new(),
			internal_cmp: Arc::clone(&opts.internal_comparator) as Arc<dyn Comparator>,
			min_vlog_file_id: None,
		}
	}

	/// Estimated size of the table file so far: bytes already written plus
	/// the in-progress data block. Used by compaction to decide when to roll
	/// over to a new output file.
	pub(crate) fn estimated_file_size(&self) -> u64 {
		let pending = self.data_block.as_ref().map_or(0, |b| b.size_estimate());
		(self.sink.offset() + pending) as u64
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

	/// Adds a range deletion to the table metadata without inserting into point-key data blocks.
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
		let handle = self.write_block(BlockKind::Data, contents, compression_type)?;

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

	/// Finalizes the SSTable by writing all remaining blocks and the footer.
	pub(crate) fn finish(mut self) -> Result<usize> {
		// Set creation timestamp
		self.meta.properties.created_at = u128::from(crate::rt::now_unix_ns());

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
					let handle =
						self.write_block(BlockKind::FilterPartition, bits, CompressionType::None)?;
					filter_bytes_total += handle.size as u64;
					filter_index.add(&separator, &handle.encode())?;
				}
				let filter_index_data = filter_index.finish()?;
				let fi_handle = self.write_block(
					BlockKind::FilterIndex,
					filter_index_data,
					CompressionType::None,
				)?;
				self.meta.properties.filter_size = filter_bytes_total + fi_handle.size as u64;
				filter_index_entry = Some((filter_name, fi_handle));
			}
		}

		// Write partitioned index blocks and top-level index
		let compression_type = self.compression_selector.select_compression(self.target_level);
		let ix_handle = self.partitioned_index.finish(&mut self.sink, compression_type)?;

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
		let meta_ix_handle =
			self.write_block(BlockKind::MetaIndex, meta_block, CompressionType::None)?;

		// Write footer
		let footer = Footer::new(meta_ix_handle, ix_handle);
		self.sink.write_footer(&footer)
	}

	/// Compresses and writes a block of the given kind, returning its handle.
	fn write_block(
		&mut self,
		kind: BlockKind,
		block_data: BlockData,
		compression_type: CompressionType,
	) -> Result<BlockHandle> {
		self.sink.write_block(kind, block_data, compression_type)
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

		// The times of the versions the table holds, which `history` uses to leave out a table
		// whose versions are all outside the range it was asked for. A barrier hides the versions
		// under it whatever its own time, and a version without a time reads as written when the
		// versions before it were, which this table cannot say: a table that holds either can
		// never be left out, it spans every time.
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

// =============================================================================
// BLOCK I/O UTILITIES
// =============================================================================

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
			};
			Ok(buffer)
		}
		CompressionType::ZstdCompression => zstd::stream::encode_all(&raw_block[..], 3)
			.map_err(|e| Error::Compression(e.to_string())),
		CompressionType::None => Ok(raw_block),
	}
}

pub(crate) fn decompress_block(
	compressed_block: &[u8],
	compression: CompressionType,
) -> Result<Vec<u8>> {
	match compression {
		CompressionType::SnappyCompression => {
			let mut dec = snap::raw::Decoder::new();
			dec.decompress_vec(compressed_block).map_err(|e| Error::Decompression(e.to_string()))
		}
		CompressionType::ZstdCompression => zstd::stream::decode_all(compressed_block)
			.map_err(|e| Error::Decompression(e.to_string())),
		CompressionType::None => Ok(Vec::from(compressed_block)),
	}
}

pub(crate) fn read_writer_meta_properties(metaix: &Block) -> Result<Option<TableMetadata>> {
	let meta_key = InternalKey::new(Vec::from(b"meta"), 0, InternalKeyKind::Set).encode();
	let mut metaindexiter = metaix.iter()?;
	metaindexiter.seek_internal(&meta_key)?;

	if metaindexiter.is_valid() {
		let k = metaindexiter.key();
		if k.user_key() != META_PROPERTIES_KEY {
			return Err(Error::CorruptedTableMetadata(
				"the meta-index entry found for the table properties has another key".to_string(),
			));
		}
		let buf_bytes = metaindexiter.value_encoded()?;
		return Ok(Some(TableMetadata::decode(buf_bytes)?));
	}
	Ok(None)
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
	/// The key of this instance's blocks in the block cache: unlike `id`, never given to another
	/// table, see `BlockSource::cache_id`.
	cache_id: u64,
	/// Reads the footer and the blocks of the table's file.
	pub(crate) source: BlockSource,
	pub file_size: u64,

	pub(crate) opts: Arc<Options>,
	pub(crate) meta: TableMetadata,

	pub(crate) index_block: IndexType,
	pub(crate) filter: TableFilter,
	pub(crate) range_deletions: Arc<parking_lot::RwLock<Vec<(Key, Key, u64)>>>,
	pub(crate) has_range_deletions: Arc<std::sync::atomic::AtomicBool>,

	/// The format of the file, from its footer.
	pub(crate) format: TableFormat,

	/// Set for a table that was opened from disk and names no value-log file
	/// (`oldest_vlog_file_id` is 0) although it was written by an engine that may not have kept
	/// the property right: the table may still point into the log. SurrealKV before v0.20 stored
	/// a sequence number of 0 in the place of the property, and development builds of the
	/// current engine before `TableFormat::LSMV2` are not known to have kept it either. A table
	/// of `LSMV2`, which every writer of this engine produces, has the property right: 0 means
	/// that it points into no file. See `LevelManifest::load_table`.
	pub(crate) vlog_pointers_unknown: bool,
}

/// Per-file bloom filter reader, dispatched on which meta-index entry the
/// file carries. Files written before partitioned filters existed hold one
/// monolithic filter (`filter.<policy>`) and keep their exact old read
/// path; new files hold partition-per-index-partition filters
/// (`partitionedfilter.<policy>`). Both kinds coexist in one store with no
/// migration.
#[derive(Clone)]
pub(crate) enum TableFilter {
	None,
	Legacy(FilterBlockReader),
	Partitioned(PartitionedFilterReader),
}

impl Table {
	/// Opens the file of table `id` for reading.
	///
	/// A sealed table is opened only by a database that is encrypted, with the key of its file, and
	/// a plain table only by one that is not: see `BlockSource::open_table`. Every table of a
	/// database is opened here, whether a flush, a compaction or a manifest wrote it.
	pub(crate) fn new(
		id: u64,
		opts: Arc<Options>,
		file: Arc<dyn File>,
		file_size: u64,
	) -> Result<Table> {
		let (source, footer) =
			BlockSource::open_table(file, file_size, id, opts.encryption_context())?;
		Self::from_source(id, opts, file_size, source, footer)
	}

	/// Opens a table file whose blocks are protected as `protection_for` says once the footer is
	/// read, which a table with sealed blocks needs and `new` does not.
	#[cfg(test)]
	pub(crate) fn open_with(
		id: u64,
		opts: Arc<Options>,
		file: Arc<dyn File>,
		file_size: u64,
		protection_for: impl FnOnce(&Footer) -> Result<Protection>,
	) -> Result<Table> {
		let (source, footer) = BlockSource::open_with(file, file_size, protection_for)?;
		Self::from_source(id, opts, file_size, source, footer)
	}

	/// Opens a table from the source of its blocks and its footer.
	pub(crate) fn from_source(
		id: u64,
		opts: Arc<Options>,
		file_size: u64,
		source: BlockSource,
		footer: Footer,
	) -> Result<Table> {
		// Step 1: The footer was read when the source was opened

		let cache_id = source.cache_id();

		// Step 2: Load meta index block, and refuse a table in LSMV1 before anything keyed by its
		// keys is read. Opening a database through `Tree::new` converts such tables first, so
		// this only meets one on a path that bypasses the conversion.
		let metaindexblock = source.read_block(
			BlockKind::MetaIndex,
			&footer.meta_index,
			Arc::clone(&opts.internal_comparator),
		)?;
		refuse_legacy_keys(id, footer.format, &metaindexblock)?;

		// Step 3: Load partitioned index
		let index_block = {
			let partitioned_index =
				Index::new(id, Arc::clone(&opts), source.clone(), &footer.index)?;
			IndexType::Partitioned(partitioned_index)
		};

		// Step 4: Extract metadata
		let writer_metadata =
			read_writer_meta_properties(&metaindexblock)?.ok_or(Error::TableMetadataNotFound)?;

		// Step 5: Load the bloom filter, dispatching per file on which
		// meta-index entry exists: files written before partitioned filters
		// carry a monolithic filter under "filter.<policy>" and keep their
		// exact old read path (zero migration); new files carry
		// "partitionedfilter.<policy>" (only the small top-level filter
		// index is loaded here — partitions are fetched on demand through
		// the block cache).
		let filter = if let Some(policy) = opts.filter_policy.as_ref() {
			let name = policy.name();
			if let Some(handle) =
				Self::find_meta_handle(&metaindexblock, &format!("partitionedfilter.{name}"))?
			{
				TableFilter::Partitioned(PartitionedFilterReader::new(
					id,
					Arc::clone(&opts),
					source.clone(),
					&handle,
					Arc::clone(policy),
				)?)
			} else if let Some(handle) =
				Self::find_meta_handle(&metaindexblock, &format!("filter.{name}"))?
			{
				if handle.size() == 0 {
					return Err(Error::FilterBlockEmpty);
				}
				TableFilter::Legacy(FilterBlockReader::new(
					source.read_legacy_filter(&handle)?,
					Arc::clone(policy),
				))
			} else {
				TableFilter::None
			}
		} else {
			TableFilter::None
		};

		let has_rd = !writer_metadata.range_deletions.is_empty();
		Ok(Table {
			id,
			cache_id,
			source,
			file_size,
			opts,
			filter,
			index_block,
			range_deletions: Arc::new(parking_lot::RwLock::new(
				writer_metadata.range_deletions.clone(),
			)),
			has_range_deletions: Arc::new(std::sync::atomic::AtomicBool::new(has_rd)),
			meta: writer_metadata,
			format: footer.format,
			vlog_pointers_unknown: false,
		})
	}

	/// Seeks the meta index for an exact entry name and decodes its handle.
	/// Returns None when the entry is absent or empty (the meta index may
	/// contain other entries — "meta", either filter kind — so a seek
	/// landing on a different key means "not present", not corruption).
	fn find_meta_handle(metaix: &Block, name: &str) -> Result<Option<BlockHandle>> {
		let seek_key = InternalKey::new(Vec::from(name.as_bytes()), 0, InternalKeyKind::Set);

		let mut metaindexiter = metaix.iter()?;
		metaindexiter.seek_internal(&seek_key.encode())?;

		if metaindexiter.is_valid() && metaindexiter.key().user_key() == name.as_bytes() {
			let val = metaindexiter.value_encoded()?;
			let (handle, _) = BlockHandle::decode(val).map_err(|e| {
				Error::from(SSTableError::FailedToDecodeBlockHandle {
					value_bytes: val.to_vec(),
					context: format!("error: {:?}", e),
				})
			})?;
			if handle.size() > 0 {
				return Ok(Some(handle));
			}
		}
		Ok(None)
	}

	/// The number of reads of the table's file that were asked for, by the table, its clones and
	/// its index and filter, see `BlockSource::raw_reads`.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn raw_reads(&self) -> u64 {
		self.source.raw_reads()
	}

	/// The number of bytes that `raw_reads` asked for.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn raw_read_bytes(&self) -> u64 {
		self.source.raw_read_bytes()
	}

	/// The number of lookups that were answered without the filter of the table because one of
	/// its partitions could not be read.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn filter_reads_degraded(&self) -> u64 {
		match &self.filter {
			TableFilter::Partitioned(filters) => filters.degraded_reads(),
			TableFilter::None | TableFilter::Legacy(_) => 0,
		}
	}

	/// Reads a data block, using cache if available.
	pub(crate) fn read_block(&self, location: &BlockHandle) -> Result<Arc<Block>> {
		// Check cache first
		if let Some(block) =
			self.opts.block_cache.get_data_block(self.cache_id, location.offset() as u64)
		{
			return Ok(block);
		}

		// Cache miss: read from disk
		let b = self.source.read_block(
			BlockKind::Data,
			location,
			Arc::clone(&self.opts.internal_comparator),
		)?;
		let b = Arc::new(b);

		// Insert into cache
		self.opts.block_cache.insert_data_block(
			self.cache_id,
			location.offset() as u64,
			Arc::clone(&b),
		);

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
			self.opts.block_cache.get_data_block_history(self.cache_id, location.offset() as u64)
		{
			return Ok(block);
		}

		// Cache miss: read from disk
		let b = self.source.read_block(BlockKind::Data, location, comparator)?;
		let b = Arc::new(b);

		// Insert into history cache
		self.opts.block_cache.insert_data_block_history(
			self.cache_id,
			location.offset() as u64,
			Arc::clone(&b),
		);

		Ok(b)
	}

	/// Reads a data block asynchronously, using cache if available.
	pub(crate) async fn read_block_async(&self, location: &BlockHandle) -> Result<Arc<Block>> {
		// Check cache first
		if let Some(block) =
			self.opts.block_cache.get_data_block(self.cache_id, location.offset() as u64)
		{
			return Ok(block);
		}

		// The block and its trailer are read in one request through the object store
		let block = self
			.source
			.read_block_async(BlockKind::Data, location, Arc::clone(&self.opts.internal_comparator))
			.await?;
		let b = Arc::new(block);

		// Insert into cache
		self.opts.block_cache.insert_data_block(
			self.cache_id,
			location.offset() as u64,
			Arc::clone(&b),
		);

		Ok(b)
	}

	/// Asynchronous point lookup for a single key.
	#[cfg(test)]
	pub(crate) async fn get_async(
		&self,
		key: &InternalKey,
	) -> Result<Option<(InternalKey, Value)>> {
		let key_encoded =
			SmallKeyBuf::from_internal_parts(&key.user_key, key.seq_num(), key.kind());
		match self.get_encoded_async(&key.user_key, key_encoded.as_slice()).await? {
			Some((trailer, value)) => Ok(Some((
				InternalKey {
					user_key: key.user_key.clone(),
					trailer,
				},
				value,
			))),
			None => Ok(None),
		}
	}

	/// Asynchronous point lookup using pre-encoded internal key bytes, returning `(trailer, value)`
	/// without allocating a copy of `user_key`.
	pub(crate) async fn get_encoded_async(
		&self,
		user_key: &[u8],
		key_encoded: &[u8],
	) -> Result<Option<(u64, Value)>> {
		if self.holds_only_range_deletions() {
			return Ok(None);
		}

		// Step 1: Bloom filter for early rejection
		match &self.filter {
			TableFilter::Legacy(filters) => {
				if !filters.may_contain(user_key, 0) {
					return Ok(None);
				}
			}
			TableFilter::Partitioned(filters) => {
				if !filters.may_contain(key_encoded, user_key)? {
					return Ok(None);
				}
			}
			TableFilter::None => {}
		}

		let IndexType::Partitioned(partitioned_index) = &self.index_block;

		// Step 2: Find partition that could contain this key
		let Some((_, partition_handle)) =
			partitioned_index.find_block_handle_by_key(key_encoded)?
		else {
			return Ok(None);
		};

		// Step 3: Load partition block and seek
		let partition_block = partitioned_index.load_block(partition_handle)?;
		let mut key_buf = SmallKeyBuf::new();
		let Some((_, partition_val)) = partition_block.seek_with_buf(key_encoded, &mut key_buf)?
		else {
			return Ok(None);
		};

		// Decode data block handle from partition entry
		let (data_handle, _) = BlockHandle::decode(partition_val).map_err(|e| {
			Error::from(SSTableError::FailedToDecodeBlockHandle {
				value_bytes: partition_val.to_vec(),
				context: format!("Failed to decode BlockHandle in get_async(): {e}"),
			})
		})?;

		// Step 4: Read data block asynchronously and search
		let data_block = self.read_block_async(&data_handle).await?;
		let Some((found_key, value_bytes)) = data_block.seek_with_buf(key_encoded, &mut key_buf)?
		else {
			return Ok(None);
		};

		if InternalKey::user_key_from_encoded(found_key) == user_key {
			Ok(Some((InternalKey::trailer_from_encoded(found_key), value_bytes.to_vec())))
		} else {
			Ok(None)
		}
	}

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
	pub(crate) fn has_range_deletions(&self) -> bool {
		self.has_range_deletions.load(std::sync::atomic::Ordering::Relaxed)
	}

	/// Whether the table holds point entries.
	pub(crate) fn has_points(&self) -> bool {
		self.meta.smallest_point.is_some() || self.meta.properties.num_entries > 0
	}

	/// Whether the table holds range deletions and nothing else.
	///
	/// A compaction that drops every point but keeps a range deletion writes such a table: no
	/// data block, an empty index and no key range. That happens in the bottom level, where the
	/// range deletion covers every point and its own entry is dropped; in a level above it,
	/// where tables that carry a range deletion are compacted without the table that holds its
	/// entry; and in L0 of a tree of one level, which is the bottom level.
	///
	/// Its range deletions are read from the metadata of every table, so a point read or a scan
	/// has nothing to look for in it, and it reads as empty. A table with no entry of any kind
	/// is never written, and the reads of one still report its empty index as corrupt.
	pub(crate) fn holds_only_range_deletions(&self) -> bool {
		!self.has_points() && !self.meta.range_deletions.is_empty()
	}

	pub(crate) fn is_user_key_in_range(&self, key: &[u8]) -> bool {
		if self.holds_only_range_deletions() {
			return false;
		}
		let Some(smallest) = &self.meta.smallest_point else {
			return true;
		};
		let Some(largest) = &self.meta.largest_point else {
			return true;
		};

		self.opts.comparator.compare(key, smallest.user_key.as_slice()) >= Ordering::Equal
			&& self.opts.comparator.compare(key, largest.user_key.as_slice()) <= Ordering::Equal
	}

	#[cfg(test)]
	pub(crate) fn get(&self, key: &InternalKey) -> Result<Option<(InternalKey, Value)>> {
		let key_encoded =
			SmallKeyBuf::from_internal_parts(&key.user_key, key.seq_num(), key.kind());
		match self.get_encoded(&key.user_key, key_encoded.as_slice())? {
			Some((trailer, value)) => Ok(Some((
				InternalKey {
					user_key: key.user_key.clone(),
					trailer,
				},
				value,
			))),
			None => Ok(None),
		}
	}

	/// Point lookup using pre-encoded internal key bytes, returning `(trailer, value)` without
	/// allocating a copy of `user_key`.
	pub(crate) fn get_encoded(
		&self,
		user_key: &[u8],
		key_encoded: &[u8],
	) -> Result<Option<(u64, Value)>> {
		if self.holds_only_range_deletions() {
			return Ok(None);
		}

		// Step 1: Bloom filter for early rejection
		match &self.filter {
			TableFilter::Legacy(filters) => {
				if !filters.may_contain(user_key, 0) {
					return Ok(None);
				}
			}
			TableFilter::Partitioned(filters) => {
				if !filters.may_contain(key_encoded, user_key)? {
					return Ok(None);
				}
			}
			TableFilter::None => {}
		}

		let IndexType::Partitioned(partitioned_index) = &self.index_block;

		// Step 2: Find partition that could contain this key
		let Some((_, partition_handle)) =
			partitioned_index.find_block_handle_by_key(key_encoded)?
		else {
			return Ok(None);
		};

		// Step 3: Load partition block and seek
		let partition_block = partitioned_index.load_block(partition_handle)?;
		let mut key_buf = SmallKeyBuf::new();
		let Some((_, partition_val)) = partition_block.seek_with_buf(key_encoded, &mut key_buf)?
		else {
			return Ok(None);
		};

		// Decode data block handle from partition entry
		let (data_handle, _) = BlockHandle::decode(partition_val).map_err(|e| {
			Error::from(SSTableError::FailedToDecodeBlockHandle {
				value_bytes: partition_val.to_vec(),
				context: format!("Failed to decode BlockHandle in get(): {e}"),
			})
		})?;

		// Step 4: Read data block and search
		let data_block = self.read_block(&data_handle)?;
		let Some((found_key, value_bytes)) = data_block.seek_with_buf(key_encoded, &mut key_buf)?
		else {
			return Ok(None);
		};

		if InternalKey::user_key_from_encoded(found_key) == user_key {
			Ok(Some((InternalKey::trailer_from_encoded(found_key), value_bytes.to_vec())))
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

	#[cfg(test)]
	pub(crate) fn is_key_in_key_range(&self, key: &InternalKey) -> bool {
		self.is_user_key_in_range(key.user_key.as_slice())
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

	/// Whether every point key of the table satisfies the range's upper bound, and its lower
	/// bound: then a step needs no check of that bound.
	upper_holds: bool,
	lower_holds: bool,
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
			upper_holds: false,
			lower_holds: false,
		}
		.with_bounds_checked())
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
			upper_holds: false,
			lower_holds: false,
		}
		.with_bounds_checked())
	}

	/// Notes which bounds of the range every point key of the table satisfies: those between its
	/// smallest and largest point keys.
	fn with_bounds_checked(mut self) -> Self {
		let meta = &self.table.meta;
		self.upper_holds =
			meta.largest_point.as_ref().is_some_and(|k| self.satisfies_upper_bound(&k.user_key));
		self.lower_holds =
			meta.smallest_point.as_ref().is_some_and(|k| self.satisfies_lower_bound(&k.user_key));
		self
	}

	fn is_valid(&self) -> bool {
		!self.exhausted && self.second_level.as_ref().is_some_and(|iter| iter.is_valid())
	}

	fn mark_exhausted(&mut self) {
		self.exhausted = true;
		if let Some(ref mut iter) = self.second_level {
			iter.reset();
		}
	}

	/// Loads the data block pointed to by the current first-level position.
	///
	/// ## Process
	///
	/// 1. Get block handle from current first_level entry
	/// 2. Read the data block (with caching)
	/// 3. Create or rebind a BlockIterator for it
	fn init_data_block(&mut self) -> Result<()> {
		if !self.first_level.valid() {
			if let Some(ref mut iter) = self.second_level {
				iter.reset();
			}
			return Ok(());
		}

		let handle = self.first_level.block_handle()?;

		// Load data block - use custom comparator if set, otherwise use cached version
		let block = if let Some(ref comparator) = self.custom_comparator {
			self.table.read_block_with_comparator(&handle, Arc::clone(comparator))?
		} else {
			self.table.read_block(&handle)?
		};
		if let Some(ref mut iter) = self.second_level {
			iter.reset_with_block(&block, self.custom_comparator.as_ref())?;
		} else {
			self.second_level = Some(block.iter()?);
		}
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
				if let Some(ref mut iter) = self.second_level {
					iter.reset();
				}
				return Ok(());
			}

			self.first_level.next()?;
			if !self.first_level.valid() {
				if let Some(ref mut iter) = self.second_level {
					iter.reset();
				}
				return Ok(());
			}
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
				if let Some(ref mut iter) = self.second_level {
					iter.reset();
				}
				return Ok(());
			}

			self.first_level.prev()?;
			if !self.first_level.valid() {
				if let Some(ref mut iter) = self.second_level {
					iter.reset();
				}
				return Ok(());
			}
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
		// A table of range deletions alone has no data block to position on.
		if self.table.holds_only_range_deletions() {
			self.mark_exhausted();
			return Ok(());
		}
		self.exhausted = false;

		enum LowerSeek {
			Unbounded,
			Included(SmallKeyBuf),
			Excluded(SmallKeyBuf),
		}

		let lower_seek = match &self.range.0 {
			Bound::Unbounded => LowerSeek::Unbounded,
			Bound::Included(internal_key) => LowerSeek::Included(SmallKeyBuf::from_internal_parts(
				&internal_key.user_key,
				internal_key.seq_num(),
				internal_key.kind(),
			)),
			Bound::Excluded(internal_key) => LowerSeek::Excluded(SmallKeyBuf::from_internal_parts(
				&internal_key.user_key,
				0,
				InternalKeyKind::Set,
			)),
		};

		match lower_seek {
			LowerSeek::Unbounded => {
				// No lower bound: seek to absolute first
				self.first_level.seek_to_first()?;
				self.init_data_block()?;
				if self.first_level.valid() {
					if let Some(ref mut iter) = self.second_level {
						iter.seek_to_first()?;
					}
				}
				self.advance_to_valid_entry()?;
			}
			LowerSeek::Included(seek_key) => {
				// internal_key has seq_num=MAX_SEQ (smallest for this user_key)
				// Seeking to it finds the first version of this user_key
				self.seek_internal(seek_key.as_slice())?;
			}
			LowerSeek::Excluded(seek_key) => {
				// Create seek key with seq_num=0 (LARGEST for this user_key)
				// This seeks PAST all versions of the excluded key
				self.seek_internal(seek_key.as_slice())?;

				// Edge case: if we somehow landed on exactly (user_key, 0), advance past it
				let excluded_user_key = InternalKey::user_key_from_encoded(seek_key.as_slice());
				if self.is_valid() && self.current_user_key() == excluded_user_key {
					self.advance_internal()?;
				}
			}
		}

		// Verify we didn't overshoot the upper bound
		if !self.is_valid() || !self.satisfies_upper_bound(self.current_user_key()) {
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
		if self.table.holds_only_range_deletions() {
			self.mark_exhausted();
			return Ok(());
		}
		self.exhausted = false;

		enum UpperSeek {
			Unbounded,
			Included(SmallKeyBuf),
			Excluded(SmallKeyBuf),
		}

		let upper_seek = match &self.range.1 {
			Bound::Unbounded => UpperSeek::Unbounded,
			Bound::Included(internal_key) => UpperSeek::Included(SmallKeyBuf::from_internal_parts(
				&internal_key.user_key,
				0,
				InternalKeyKind::Set,
			)),
			Bound::Excluded(internal_key) => UpperSeek::Excluded(SmallKeyBuf::from_internal_parts(
				&internal_key.user_key,
				INTERNAL_KEY_SEQ_NUM_MAX,
				InternalKeyKind::Max,
			)),
		};

		match upper_seek {
			UpperSeek::Unbounded => {
				// No upper bound: seek to absolute last
				self.first_level.seek_to_last()?;
				self.init_data_block()?;
				if self.first_level.valid() {
					if let Some(ref mut iter) = self.second_level {
						iter.seek_to_last()?;
					}
				}
				self.retreat_to_valid_entry()?;
			}
			UpperSeek::Included(seek_key) => {
				// Seek to (user_key, 0) = LARGEST internal key for this user_key
				// This positions past all versions, then we back up
				self.seek_internal(seek_key.as_slice())?;

				if !self.is_valid() {
					// Seek went past end of table, position at absolute last
					self.position_to_absolute_last()?;
				} else {
					let bound_user_key = InternalKey::user_key_from_encoded(seek_key.as_slice());
					// Check if we landed past the bound (on a later user_key)
					let cmp =
						self.table.opts.comparator.compare(self.current_user_key(), bound_user_key);
					if cmp == Ordering::Greater {
						// We're past the bound, back up one entry
						self.prev_internal()?;
					}
					// If cmp is Equal or Less, we're already on a valid position
					// (this can happen if (user_key, 0) exists or we landed on earlier key)
				}
			}
			UpperSeek::Excluded(seek_key) => {
				// Seek to (user_key, MAX_SEQ) = SMALLEST internal key for this user_key
				// This positions at first version of excluded key, then we back up
				self.seek_internal(seek_key.as_slice())?;

				if !self.is_valid() {
					// Seek went past end, position at absolute last
					self.position_to_absolute_last()?;
				}

				// For Excluded: back up if we're AT or PAST the bound
				// (we need to be strictly BEFORE it)
				if self.is_valid() {
					let bound_user_key = InternalKey::user_key_from_encoded(seek_key.as_slice());
					let cmp =
						self.table.opts.comparator.compare(self.current_user_key(), bound_user_key);
					if cmp != Ordering::Less {
						// We're on the excluded key or past it, back up
						self.prev_internal()?;
					}
				}
			}
		}

		// Verify we didn't back up past the lower bound
		if !self.is_valid() || !self.satisfies_lower_bound(self.current_user_key()) {
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
		if !self.first_level.valid() {
			if let Some(ref mut iter) = self.second_level {
				iter.reset();
			}
			return Ok(());
		}

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

		if self.first_level.valid() {
			if let Some(ref mut iter) = self.second_level {
				iter.seek_to_last()?;
			}
		}

		self.retreat_to_valid_entry()?;
		Ok(())
	}

	fn advance_internal(&mut self) -> Result<bool> {
		if let Some(ref mut iter) = self.second_level {
			if iter.is_valid() && iter.advance()? {
				return Ok(true);
			}
		}
		self.advance_to_valid_entry()?;
		Ok(self.is_valid())
	}

	fn prev_internal(&mut self) -> Result<bool> {
		if let Some(ref mut iter) = self.second_level {
			if iter.is_valid() && iter.prev_internal()? {
				return Ok(true);
			}
		}
		self.retreat_to_valid_entry()?;
		Ok(self.is_valid())
	}
}

impl LSMIterator for TableIterator<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		if self.table.holds_only_range_deletions() {
			self.mark_exhausted();
			return Ok(false);
		}
		self.exhausted = false;
		self.seek_internal(target)?;

		if !self.is_valid() || !self.satisfies_upper_bound(self.current_user_key()) {
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

		if !self.is_valid()
			|| (!self.upper_holds && !self.satisfies_upper_bound(self.current_user_key()))
		{
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

		if !self.is_valid()
			|| (!self.lower_holds && !self.satisfies_lower_bound(self.current_user_key()))
		{
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
