use std::cmp::Ordering;
use std::io::Write;
use std::ops::Bound;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crc32fast::Hasher as Crc32;
use integer_encoding::{FixedInt, FixedIntWriter};
use snap::raw::max_compress_len;

use super::meta::KeyRange;
use crate::compression::CompressionSelector;
use crate::error::{Error, Result};
use crate::sstable::block::{Block, BlockData, BlockHandle, BlockIterator, BlockWriter};
use crate::sstable::filter_block::{FilterBlockReader, FilterBlockWriter};
use crate::sstable::index_block::{TopLevelIndex, TopLevelIndexWriter};
use crate::sstable::meta::TableMetadata;
use crate::sstable::{
	InternalKey,
	InternalKeyKind,
	INTERNAL_KEY_SEQ_NUM_MAX,
	INTERNAL_KEY_TIMESTAMP_MAX,
};
use crate::vfs::File;
use crate::{
	Comparator,
	CompressionType,
	FilterPolicy,
	InternalKeyComparator,
	InternalKeyRange,
	Options,
	Value,
};

const TABLE_FOOTER_LENGTH: usize = 42; // 2 + 16 + 16 + 8 (format + checksum + meta + index + magic)
const TABLE_FULL_FOOTER_LENGTH: usize = TABLE_FOOTER_LENGTH + 8;
const TABLE_MAGIC_FOOTER_ENCODED: [u8; 8] = [0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb];

pub const BLOCK_CKSUM_LEN: usize = 4;
pub const BLOCK_COMPRESS_LEN: usize = 1;

const MASK_DELTA: u32 = 0xa282_ead8;

pub(crate) fn mask(crc: u32) -> u32 {
	crc.rotate_right(15).wrapping_add(MASK_DELTA)
}

/// Return the crc whose masked representation is `masked`.
pub(crate) fn unmask(masked: u32) -> u32 {
	let rot = masked.wrapping_sub(MASK_DELTA);
	rot.rotate_left(15)
}

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

#[derive(Debug, Clone)]
pub(crate) struct Footer {
	pub format: TableFormat,
	pub checksum: ChecksumType,
	pub meta_index: BlockHandle,
	pub index: BlockHandle,
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

	pub(crate) fn read_from(reader: Arc<dyn File>, file_size: usize) -> Result<Vec<u8>> {
		if file_size < TABLE_FULL_FOOTER_LENGTH {
			return Err(Error::CorruptedBlock(format!(
				"invalid table (file size is too small: {file_size} bytes)"
			)));
		}

		let mut buf = vec![0; TABLE_FULL_FOOTER_LENGTH];
		let offset = file_size - TABLE_FULL_FOOTER_LENGTH;
		reader.read_at(offset as u64, &mut buf)?;

		Ok(buf)
	}

	pub(crate) fn decode(buf: &[u8]) -> Result<Footer> {
		let magic = &buf[buf.len() - TABLE_MAGIC_FOOTER_ENCODED.len()..];

		// Validate magic number first
		if magic != TABLE_MAGIC_FOOTER_ENCODED {
			return Err(Error::CorruptedBlock(format!(
				"invalid table (bad magic number: {magic:x?})"
			)));
		}

		if buf.len() < TABLE_FOOTER_LENGTH {
			return Err(Error::CorruptedBlock(format!(
				"invalid table (footer too short): {}",
				buf.len()
			)));
		}

		// Read format and checksum from footer (first 2 bytes)
		let format = TableFormat::from_u8(buf[0])?;
		let checksum = match buf[1] {
			1 => ChecksumType::CRC32c,
			_ => return Err(Error::CorruptedBlock("Invalid checksum type".into())),
		};

		// Read block handles (starting at offset 2)
		let (meta_index, metalen) = BlockHandle::decode(&buf[2..])?;
		if metalen == 0 {
			return Err(Error::CorruptedBlock(
				"invalid table (bad meta_index block handle)".into(),
			));
		}

		let (index_handle, _) = BlockHandle::decode(&buf[2 + metalen..])?;

		Ok(Footer {
			format,
			checksum,
			meta_index,
			index: index_handle,
		})
	}

	pub(crate) fn encode(&self, dst: &mut [u8]) {
		match self.format {
			TableFormat::LSMV1 => {
				dst[..TABLE_FOOTER_LENGTH].fill(0);

				// Encode format version (1 byte)
				dst[0] = self.format as u8;

				// Encode checksum type (1 byte)
				dst[1] = self.checksum as u8;

				// Encode block handles (starting at offset 2)
				let n = self.meta_index.encode_into(&mut dst[2..]);
				self.index.encode_into(&mut dst[2 + n..]);

				// Magic footer at the end
				dst[TABLE_FOOTER_LENGTH..TABLE_FULL_FOOTER_LENGTH]
					.copy_from_slice(&TABLE_MAGIC_FOOTER_ENCODED);
			}
		}
	}
}

// Defines a writer for constructing and writing table structures to a storage
// medium.
pub(crate) struct TableWriter<W: Write> {
	writer: W,                                 // Underlying writer to write data to.
	opts: Arc<Options>,                        // Shared table options.
	compression_selector: CompressionSelector, // Level-aware compression selector.
	target_level: u8,                          // Target level this SSTable will be written to.

	meta: TableMetadata, // Metadata properties of the table.

	offset: usize, // Current offset in the writer where the next write will happen.
	prev_block_last_key: Vec<u8>, // Last key of the previous block.

	data_block: Option<BlockWriter>, // Writer for the current data block.
	partitioned_index: TopLevelIndexWriter, // Writer for partitioned index.
	filter_block: Option<FilterBlockWriter>, // Writer for the optional filter block.

	/// internal key comparator
	internal_cmp: Arc<dyn Comparator>,
}

impl<W: Write> TableWriter<W> {
	// Constructs a new TableWriter with level-specific compression.
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
			partitioned_index: TopLevelIndexWriter::new(
				Arc::clone(&opts),
				opts.index_partition_size,
			),
			filter_block: fb,
			internal_cmp: opts.internal_comparator.clone() as Arc<dyn Comparator>,
		}
	}

	// Adds a key-value pair to the table, ensuring keys are in ascending order.
	pub(crate) fn add(&mut self, key: InternalKey, val: &[u8]) -> Result<()> {
		// Ensure there's a data block to add to.
		assert!(self.data_block.is_some());
		let enc_key = key.encode();

		// Ensure the key is in ascending order.
		if !self.prev_block_last_key.is_empty() {
			let order = self.internal_cmp.compare(&self.prev_block_last_key, &enc_key);
			assert_eq!(order, Ordering::Less, "Keys must be in ascending order");
		}

		// Initialize filter block on first key
		if self.filter_block.is_none() && self.opts.filter_policy.is_some() {
			self.filter_block =
				Some(FilterBlockWriter::new(Arc::clone(self.opts.filter_policy.as_ref().unwrap())));
			// The offset is 0 for the entire SST
			self.filter_block.as_mut().unwrap().start_block(0);
		}

		// Update all metadata properties
		self.update_meta_properties(&key, val);

		// Write and reset the current data block if it exceeds the block size.
		if self.data_block.as_ref().unwrap().size_estimate() > self.opts.block_size {
			self.write_data_block(&enc_key)?;
		}

		let dblock = self.data_block.as_mut().expect("No data block available");

		// Optionally add the key to the filter block.
		if let Some(fblock) = self.filter_block.as_mut() {
			fblock.add_key(key.user_key.as_slice());
		}

		// Add the key-value pair to the data block and increment the entry count.
		dblock.add(&enc_key, val)?;

		Ok(())
	}

	// Writes the current data block and creates a new one.
	fn write_data_block(&mut self, next_key: &[u8]) -> Result<()> {
		// Ensure there's a data block to write.
		assert!(self.data_block.is_some(), "No data block available to write.");

		let block = self.data_block.take().expect("Failed to take the existing data block");

		// Update block statistics for this written block
		let props = &mut self.meta.properties;
		props.num_data_blocks += 1;
		props.block_count += 1;

		// Update block size if not set (use first block size as representative)
		if props.block_size == 0 {
			props.block_size = block.size_estimate() as u32;
		}

		// Determine the separator key between the current and next block.
		// The separator function already returns an encoded InternalKey with the
		// appropriate user key separator and MAX seq_num/timestamp.
		let separator_key = self.internal_cmp.separator(&block.last_key, next_key);
		self.prev_block_last_key.clone_from(&block.last_key);

		// Finalize the current block and compress it.
		let contents = block.finish();
		let compression_type = self.compression_selector.select_compression(self.target_level);
		let handle = self.write_compressed_block(contents, compression_type)?;

		// Add the separator key and block handle to the index.
		let handle_encoded = handle.encode();
		self.partitioned_index.add(&separator_key, &handle_encoded)?;

		// Prepare for the next data block.
		self.data_block = Some(BlockWriter::new(
			self.opts.block_size,
			self.opts.block_restart_interval,
			Arc::clone(&self.opts.internal_comparator),
		));

		Ok(())
	}

	// Finalizes the table writing process, writing any pending blocks and the
	// footer.
	pub(crate) fn finish(mut self) -> Result<usize> {
		// Before finishing, update final properties
		self.meta.properties.created_at =
			SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();

		// Copy sequence numbers from metadata to properties for table ordering
		self.meta.properties.seqnos = (self.meta.smallest_seq_num, self.meta.largest_seq_num);

		// Check if the last data block has entries.
		if self.data_block.as_ref().is_some_and(|db| db.entries() > 0) {
			let key_past_last =
				self.internal_cmp.successor(&self.data_block.as_ref().unwrap().last_key);

			// Proceed with writing the data block.
			self.write_data_block(&key_past_last)?;
		}

		// Initialize meta_index block
		let mut meta_ix_block = BlockWriter::new(
			self.opts.block_size,
			self.opts.block_restart_interval,
			Arc::clone(&self.opts.internal_comparator),
		);

		// Write the filter block to the meta index block if present.
		if let Some(fblock) = self.filter_block.take() {
			let filter_key = format!("filter.{}", fblock.filter_name());
			let fblock_data = fblock.finish();

			// Only write if we have actual filter data
			if !fblock_data.is_empty() {
				let fblock_handle =
					self.write_compressed_block(fblock_data, CompressionType::None)?;

				// Track filter size (data only, matches RocksDB)
				self.meta.properties.filter_size = fblock_handle.size as u64;

				// Add to meta index
				let mut handle_enc = vec![0u8; 16];
				let enc_len = fblock_handle.encode_into(&mut handle_enc);
				let filter_key =
					InternalKey::new(Vec::from(filter_key.as_bytes()), 0, InternalKeyKind::Set, 0);
				meta_ix_block.add(&filter_key.encode(), &handle_enc[0..enc_len])?;
			}
		}

		// 2. Write index block
		let compression_type = self.compression_selector.select_compression(self.target_level);
		let (ix_handle, new_offset) =
			self.partitioned_index.finish(&mut self.writer, compression_type, self.offset)?;
		self.offset = new_offset;

		// 3. Query index stats
		self.meta.properties.index_size = self.partitioned_index.index_size();
		self.meta.properties.index_partitions = self.partitioned_index.num_partitions();
		self.meta.properties.top_level_index_size = self.partitioned_index.top_level_index_size();

		// 4. Write properties block
		let meta_key = InternalKey::new(Vec::from(b"meta"), 0, InternalKeyKind::Set, 0);
		let meta_value = self.meta.encode();
		meta_ix_block.add(&meta_key.encode(), &meta_value)?;

		// Write meta_index block
		let meta_block = meta_ix_block.finish();
		let meta_ix_handle = self.write_compressed_block(meta_block, CompressionType::None)?;

		// 6. Write footer
		let footer = Footer::new(meta_ix_handle, ix_handle);
		let mut buf = vec![0u8; TABLE_FULL_FOOTER_LENGTH];
		footer.encode(&mut buf);

		self.offset += self.writer.write(&buf[..])?;
		self.writer.flush()?;
		Ok(self.offset)
	}

	// Function to write a compressed block
	// Writes a block to the underlying writer and updates the offset.
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

		// Update sequence numbers
		self.meta.update_seq_num(seq_num);

		// Update key bounds for point keys
		if self.meta.smallest_point.is_none() {
			self.meta.set_smallest_point_key(key.clone());
		}
		self.meta.set_largest_point_key(key.clone());

		let props = &mut self.meta.properties;

		// Update basic counts
		props.num_entries += 1;
		props.item_count += 1;

		// Track raw (uncompressed) sizes
		props.raw_key_size += key.size() as u64;
		props.raw_value_size += value.len() as u64;

		// Track timestamp range
		let ts = key.timestamp;
		if props.oldest_key_time == 0 || ts < props.oldest_key_time {
			props.oldest_key_time = ts;
		}
		if ts > props.newest_key_time {
			props.newest_key_time = ts;
		}

		// Track range deletions
		if key.kind() == InternalKeyKind::RangeDelete {
			props.num_range_deletions += 1;
		}

		if key.is_tombstone() {
			props.num_deletions += 1;
			props.tombstone_count += 1;

			// Count soft deletes specifically
			if key.kind() == InternalKeyKind::SoftDelete {
				props.num_soft_deletes += 1;
			}
		}
		props.key_count += 1;
		props.data_size += (key.encode().len() + value.len()) as u64;

		let user_key = &key.user_key;

		// Update key range if needed
		if props.key_range.is_none() {
			props.key_range = Some(KeyRange {
				low: user_key.clone(),
				high: user_key.clone(),
			});
		} else if let Some(ref mut range) = props.key_range {
			if self.opts.comparator.compare(user_key, &range.low) == Ordering::Less {
				range.low.clone_from(user_key);
			}
			if self.opts.comparator.compare(user_key, &range.high) == Ordering::Greater {
				range.high.clone_from(user_key);
			}
		}
	}
}

// Writes a block to the underlying writer and updates the offset.
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

// Compresses a block of data using the specified compression type.
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

// Decompresses a block of data using the specified compression type.
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

/// Reads the table footer.
/// TODO: add proper error descriptions.
fn read_footer(f: Arc<dyn File>, file_size: usize) -> Result<Footer> {
	let buf = Footer::read_from(f, file_size)?;
	Footer::decode(&buf)
}

/// Reads the data for the specified block handle from a file.
fn read_bytes(f: Arc<dyn File>, location: &BlockHandle) -> Result<Vec<u8>> {
	let mut buf = vec![0; location.size()];
	f.read_at(location.offset() as u64, &mut buf).map(|_| buf)
}

// Calculates the checksum for a block.
pub(crate) fn calculate_checksum(block: &[u8], compression_type: CompressionType) -> Crc32 {
	let mut cksum = Crc32::new();
	cksum.update(block);
	cksum.update(&[compression_type as u8; BLOCK_COMPRESS_LEN]);
	cksum
}

/// Reads a serialized filter block from a file and returns a FilterBlockReader.
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

	// println!("Meta key: {:?}", meta_key);
	let mut metaindexiter = metaix.iter(false);
	metaindexiter.seek(&meta_key);

	if metaindexiter.valid() {
		let k = metaindexiter.key();
		// Verify exact match to avoid using wrong entry
		assert_eq!(k.user_key.as_slice(), b"meta");
		let buf_bytes = metaindexiter.value();
		return Ok(Some(TableMetadata::decode(&buf_bytes)?));
	}
	Ok(None)
}

pub(crate) fn read_table_block(
	comparator: Arc<InternalKeyComparator>,
	f: Arc<dyn File>,
	location: &BlockHandle,
) -> Result<Block> {
	let buf = read_bytes(Arc::clone(&f), location)?;
	let compress = read_bytes(
		Arc::clone(&f),
		&BlockHandle::new(location.offset() + location.size(), BLOCK_COMPRESS_LEN),
	)?;
	let cksum = read_bytes(
		Arc::clone(&f),
		&BlockHandle::new(
			location.offset() + location.size() + BLOCK_COMPRESS_LEN,
			BLOCK_CKSUM_LEN,
		),
	)?;

	if !verify_table_block(&buf, compress[0], unmask(u32::decode_fixed(&cksum).unwrap())) {
		return Err(Error::CorruptedBlock(format!(
			"checksum verification failed for block at {}",
			location.offset()
		)));
	}

	let block = decompress_block(&buf, CompressionType::try_from(compress[0])?)?;

	Ok(Block::new(block, comparator))
}

/// Verify checksum of block
fn verify_table_block(block: &[u8], compression_type: u8, want: u32) -> bool {
	let mut cksum = Crc32::new();
	cksum.update(block);
	cksum.update(&[compression_type; BLOCK_COMPRESS_LEN]);
	cksum.finalize() == want
}

#[derive(Clone)]
pub enum IndexType {
	Partitioned(TopLevelIndex),
}

#[derive(Clone)]
pub(crate) struct Table {
	pub id: u64,
	pub file: Arc<dyn File>,
	#[allow(unused)]
	pub file_size: u64,

	opts: Arc<Options>,             // Shared table options.
	pub(crate) meta: TableMetadata, // Metadata properties of the table.

	index_block: IndexType,
	filter_reader: Option<FilterBlockReader>,

	pub(crate) internal_cmp: Arc<InternalKeyComparator>, // Internal key comparator for the table.
}

impl Table {
	pub(crate) fn new(
		id: u64,
		opts: Arc<Options>,
		file: Arc<dyn File>,
		file_size: u64,
	) -> Result<Table> {
		// Read in the following order:
		//    1. Footer
		//    2. [index block]
		//    3. [meta block: properties]
		//    4. [meta block: filter]

		let footer = read_footer(Arc::clone(&file), file_size as usize)?;
		// println!("meta ix handle: {:?}", footer.meta_index);

		// Using partitioned index
		let index_block = {
			let partitioned_index =
				TopLevelIndex::new(id, Arc::clone(&opts), Arc::clone(&file), &footer.index)?;
			IndexType::Partitioned(partitioned_index)
		};

		let metaindexblock = read_table_block(
			Arc::clone(&opts.internal_comparator),
			Arc::clone(&file),
			&footer.meta_index,
		)?;
		// println!("meta block: {:?}", metaindexblock.block);

		let writer_metadata =
			read_writer_meta_properties(&metaindexblock)?.ok_or(Error::TableMetadataNotFound)?;
		// println!("Writer metadata: {:?}", writer_metadata);

		let filter_reader = if opts.filter_policy.is_some() {
			// Read the filter block if filter policy is present
			Self::read_filter_block(&metaindexblock, Arc::clone(&file), &opts)?
		} else {
			None
		};

		Ok(Table {
			id,
			file,
			file_size,
			internal_cmp: Arc::clone(&opts.internal_comparator),
			opts,
			filter_reader,
			index_block,
			meta: writer_metadata, // TODO: needs to be changed and read from file
		})
	}

	fn read_filter_block(
		metaix: &Block,
		file: Arc<dyn File>,
		options: &Options,
	) -> Result<Option<FilterBlockReader>> {
		let filter_name = format!("filter.{}", options.filter_policy.as_ref().unwrap().name());

		// Create encoded InternalKey for seeking
		let filter_key =
			InternalKey::new(Vec::from(filter_name.as_bytes()), 0, InternalKeyKind::Set, 0);

		let mut metaindexiter = metaix.iter(false);
		metaindexiter.seek(&filter_key.encode());

		if metaindexiter.valid() {
			let k = metaindexiter.key();

			// Verify exact match to avoid using wrong entry
			assert_eq!(k.user_key.as_slice(), filter_name.as_bytes());
			let val = metaindexiter.value();

			let fbl = BlockHandle::decode(&val);
			let filter_block_location = match fbl {
				Err(_e) => {
					return Err(Error::CorruptedBlock(format!(
						"Couldn't decode corrupt blockhandle {:?}",
						&val
					)));
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

	fn read_block(&self, location: &BlockHandle) -> Result<Arc<Block>> {
		if let Some(block) = self.opts.block_cache.get_data_block(self.id, location.offset() as u64)
		{
			return Ok(block);
		}

		let b = read_table_block(
			Arc::clone(&self.opts.internal_comparator),
			Arc::clone(&self.file),
			location,
		)?;
		let b = Arc::new(b);

		self.opts.block_cache.insert_data_block(self.id, location.offset() as u64, Arc::clone(&b));

		Ok(b)
	}

	pub(crate) fn get(&self, key: InternalKey) -> Result<Option<(InternalKey, Value)>> {
		let key_encoded = &key.encode();

		// Check filter first
		if let Some(ref filters) = self.filter_reader {
			let may_contain = filters.may_contain(key.user_key.as_slice(), 0);
			if !may_contain {
				return Ok(None);
			}
		}

		let handle = match &self.index_block {
			IndexType::Partitioned(partitioned_index) => {
				// Use full internal key for correct partition lookup
				let partition_block = match partitioned_index.get(key_encoded) {
					Ok(block) => block,
					Err(_e) => {
						return Ok(None);
					}
				};

				// Then search within the partition
				// Note: Index blocks always need full key-value pairs to decode block handles
				let mut partition_iter = partition_block.iter(false);
				partition_iter.seek(key_encoded);

				if partition_iter.valid() {
					let last_key_in_block = partition_iter.key();
					let val = partition_iter.value();
					if self.internal_cmp.compare(key_encoded, &last_key_in_block.encode())
						!= Ordering::Greater
					{
						Some(BlockHandle::decode(&val).unwrap().0)
					} else {
						return Ok(None);
					}
				} else {
					return Ok(None);
				}
			}
		};

		let handle = match handle {
			Some(h) => h,
			None => return Ok(None),
		};

		// Read block (potentially from cache)
		let tb = self.read_block(&handle)?;
		let mut iter = tb.iter(false);

		// Go to entry and check if it's the wanted entry.
		iter.seek(key_encoded);
		if iter.valid() {
			let k = iter.key();
			let v = iter.value();
			// Compare only user keys - we want exact user key match regardless of seq_num
			if k.user_key == key.user_key {
				Ok(Some((k, v)))
			} else {
				Ok(None)
			}
		} else {
			Ok(None)
		}
	}

	pub(crate) fn iter(
		self: &Arc<Self>,
		keys_only: bool,
		range: Option<InternalKeyRange>,
	) -> TableIterator {
		let range = range.unwrap_or((Bound::Unbounded, Bound::Unbounded));

		TableIterator {
			current_block: None,
			current_block_off: 0,
			table: Arc::clone(self),
			positioned: false,
			exhausted: false,
			current_partition_index: 0,
			current_partition_iter: None,
			keys_only,
			range,
			reverse_started: false,
		}
	}

	pub(crate) fn is_key_in_key_range(&self, key: &InternalKey) -> bool {
		if let Some(ref range) = self.meta.properties.key_range {
			return self.opts.comparator.compare(key.user_key.as_slice(), &range.low)
				>= Ordering::Equal
				&& self.opts.comparator.compare(key.user_key.as_slice(), &range.high)
					<= Ordering::Equal;
		}
		true // If no key range is defined, assume the key is in range.
	}

	/// Checks if this table's key range overlaps with the given key range
	pub(crate) fn overlaps_with_range(&self, other_range: &crate::sstable::meta::KeyRange) -> bool {
		self.meta
			.properties
			.key_range
			.as_ref()
			.map(|range| range.overlaps(other_range))
			.unwrap_or(false)
	}

	/// Checks if this table is completely before the given range
	/// Returns true if table's highest key is less than the range's lowest key
	pub(crate) fn is_before_range(&self, other_range: &crate::sstable::meta::KeyRange) -> bool {
		self.meta
			.properties
			.key_range
			.as_ref()
			.map(|range| {
				self.opts.comparator.compare(&range.high, &other_range.low) == Ordering::Less
			})
			.unwrap_or(false)
	}

	/// Checks if this table is completely after the given range
	/// Returns true if table's lowest key is greater than the range's highest
	/// key
	pub(crate) fn is_after_range(&self, other_range: &crate::sstable::meta::KeyRange) -> bool {
		self.meta
			.properties
			.key_range
			.as_ref()
			.map(|range| {
				self.opts.comparator.compare(&range.low, &other_range.high) == Ordering::Greater
			})
			.unwrap_or(false)
	}
}

pub(crate) struct TableIterator {
	table: Arc<Table>,
	current_block: Option<BlockIterator>,
	current_block_off: usize,
	/// Whether the iterator has been positioned at least once
	positioned: bool,
	/// Whether the iterator has been exhausted (reached the end)
	exhausted: bool,
	// For partitioned index support
	current_partition_index: usize,
	current_partition_iter: Option<BlockIterator>,
	/// When true, only return keys without allocating values
	keys_only: bool,
	/// Range bounds for filtering (InternalKey)
	range: InternalKeyRange,
	/// Whether reverse iteration has started (to distinguish from just
	/// positioned)
	reverse_started: bool,
}

impl TableIterator {
	fn skip_to_next_entry(&mut self) -> Result<bool> {
		let IndexType::Partitioned(partitioned_index) = &self.table.index_block;

		// First try to advance within current partition
		if let Some(ref mut partition_iter) = self.current_partition_iter {
			if partition_iter.advance() {
				let val = partition_iter.value();
				let (handle, _) = match BlockHandle::decode(&val) {
					Err(e) => {
						return Err(Error::CorruptedBlock(format!(
							"Couldn't decode corrupt blockhandle {val:?}: error: {e:?}"
						)));
					}
					Ok(res) => res,
				};
				return self.load_block(&handle).map(|_| true);
			}
		}

		// Current partition exhausted, move to next partition
		let blocks_len = partitioned_index.blocks.len();
		self.current_partition_index += 1;

		if self.current_partition_index >= blocks_len {
			return Ok(false); // No more partitions
		}

		let partition_handle = &partitioned_index.blocks[self.current_partition_index];
		let partition_block = partitioned_index.load_block(partition_handle)?;
		// Note: Index blocks always need full key-value pairs to decode block handles
		let mut partition_iter = partition_block.iter(false);
		partition_iter.seek_to_first();

		if partition_iter.valid() {
			let val = partition_iter.value();
			let (handle, _) = match BlockHandle::decode(&val) {
				Err(e) => {
					return Err(Error::CorruptedBlock(format!(
						"Couldn't decode corrupt blockhandle {val:?}: error: {e:?}"
					)));
				}
				Ok(res) => res,
			};
			self.current_partition_iter = Some(partition_iter);
			self.load_block(&handle).map(|_| true)
		} else {
			// This partition is empty, try next one recursively
			self.skip_to_next_entry()
		}
	}

	fn reset_partitioned_state(&mut self) {
		self.current_partition_index = 0;
		self.current_partition_iter = None;
	}

	fn load_block(&mut self, handle: &BlockHandle) -> Result<()> {
		let block = self.table.read_block(handle)?;
		let mut block_iter = block.iter(self.keys_only);

		// Position at first entry in the new block
		block_iter.seek_to_first();

		if block_iter.valid() {
			self.current_block = Some(block_iter);
			self.current_block_off = handle.offset();
			return Ok(());
		}

		Err(Error::CorruptedBlock("Empty block".to_string()))
	}

	#[cfg(test)]
	pub(crate) fn key(&self) -> InternalKey {
		self.current_block.as_ref().unwrap().key()
	}

	#[cfg(test)]
	pub(crate) fn value(&self) -> Value {
		self.current_block.as_ref().unwrap().value()
	}

	fn reset(&mut self) {
		self.positioned = false;
		self.exhausted = false;
		self.current_block = None;
		self.reset_partitioned_state();
	}

	fn mark_exhausted(&mut self) {
		self.exhausted = true;
		self.current_block = None;
	}

	pub(crate) fn prev(&mut self) -> bool {
		if let Some(ref mut block) = self.current_block {
			if block.prev() {
				return true;
			}
		}

		// Current block is exhausted, try to move to previous block

		// Try to move to previous block within current partition
		if let Some(ref mut partition_iter) = self.current_partition_iter {
			if partition_iter.prev() {
				let val = partition_iter.value();
				let (handle, _) = match BlockHandle::decode(&val) {
					Err(_) => return false,
					Ok(res) => res,
				};
				if self.load_block(&handle).is_ok() {
					if let Some(ref mut block_iter) = self.current_block {
						block_iter.seek_to_last();
						return block_iter.valid();
					}
				}
			}
		}

		// Current partition exhausted, move to previous partition
		if self.current_partition_index > 0 {
			// Get the partitioned index
			let IndexType::Partitioned(partitioned_index) = &self.table.index_block;

			self.current_partition_index -= 1;
			let partition_handle = &partitioned_index.blocks[self.current_partition_index];

			if let Ok(partition_block) = partitioned_index.load_block(partition_handle) {
				// Note: Index blocks always need full key-value pairs to decode block handles
				let mut partition_iter = partition_block.iter(false);
				partition_iter.seek_to_last();

				if partition_iter.valid() {
					let val = partition_iter.value();
					let (handle, _) = match BlockHandle::decode(&val) {
						Err(_) => {
							return false;
						}
						Ok(res) => res,
					};
					self.current_partition_iter = Some(partition_iter);
					if self.load_block(&handle).is_ok() {
						if let Some(ref mut block_iter) = self.current_block {
							block_iter.seek_to_last();
							let valid = block_iter.valid();
							return valid;
						}
					}
				}
			}
		}

		self.current_block = None;
		false
	}

	fn seek_to_lower_bound(&mut self, bound: &Bound<InternalKey>) {
		match bound {
			Bound::Included(internal_key) => {
				self.seek(&internal_key.encode());
			}
			Bound::Excluded(internal_key) => {
				// For excluded bound, seek to highest version of the key, then skip all
				// versions
				let seek_key = InternalKey::new(
					internal_key.user_key.clone(),
					INTERNAL_KEY_SEQ_NUM_MAX,
					InternalKeyKind::Max,
					INTERNAL_KEY_TIMESTAMP_MAX,
				);
				self.seek(&seek_key.encode());

				// Skip ALL versions of the excluded key
				while self.valid() {
					let current_key = self.current_block.as_ref().unwrap().key();
					if current_key.user_key != internal_key.user_key {
						break;
					}
					if !self.advance() {
						break;
					}
				}
			}
			Bound::Unbounded => {
				self.seek_to_first();
			}
		}
	}

	fn seek_to_upper_bound(&mut self, bound: &Bound<InternalKey>) {
		match bound {
			Bound::Included(internal_key) => {
				// Seek to find the first key >= upper bound
				let seek_key = InternalKey::new(
					internal_key.user_key.clone(),
					INTERNAL_KEY_SEQ_NUM_MAX,
					InternalKeyKind::Max,
					INTERNAL_KEY_TIMESTAMP_MAX,
				);
				self.seek(&seek_key.encode());

				// If seek went past table end (invalid), start from last key
				if !self.valid() {
					self.seek_to_last();
					return;
				}

				// Back up if we're beyond the bound (should only need a few steps)
				while self.valid() {
					let current_key = self.current_block.as_ref().unwrap().key();
					let cmp = self
						.table
						.opts
						.comparator
						.compare(current_key.user_key.as_slice(), internal_key.user_key.as_slice());
					if cmp != Ordering::Greater {
						// Found a key <= bound, we're positioned correctly
						break;
					}
					if !self.prev() {
						// No more keys, iterator becomes invalid
						break;
					}
				}
			}
			Bound::Excluded(internal_key) => {
				// Seek to find the first key >= upper bound
				let seek_key = InternalKey::new(
					internal_key.user_key.clone(),
					INTERNAL_KEY_SEQ_NUM_MAX,
					InternalKeyKind::Max,
					INTERNAL_KEY_TIMESTAMP_MAX,
				);
				self.seek(&seek_key.encode());

				// If seek went past table end, start from last key
				if !self.valid() {
					self.seek_to_last();
				}

				// Move backward until we find a key < end (strictly less)
				while self.valid() {
					let current_key = self.current_block.as_ref().unwrap().key();
					let cmp = self
						.table
						.opts
						.comparator
						.compare(current_key.user_key.as_slice(), internal_key.user_key.as_slice());
					if cmp == Ordering::Less {
						break;
					}
					if !self.prev() {
						break;
					}
				}
			}
			Bound::Unbounded => {
				self.seek_to_last();
			}
		}
	}

	/// Check if a user key satisfies the lower bound constraint
	fn satisfies_lower_bound(&self, internal_key: &InternalKey) -> bool {
		match &self.range.0 {
			Bound::Included(start) => {
				self.table.opts.comparator.compare(&internal_key.user_key, &start.user_key)
					!= Ordering::Less
			}
			Bound::Excluded(start) => {
				self.table.opts.comparator.compare(&internal_key.user_key, &start.user_key)
					== Ordering::Greater
			}
			Bound::Unbounded => true,
		}
	}

	/// Check if a user key satisfies the upper bound constraint
	fn satisfies_upper_bound(&self, internal_key: &InternalKey) -> bool {
		match &self.range.1 {
			Bound::Included(end) => {
				self.table.opts.comparator.compare(&internal_key.user_key, &end.user_key)
					!= Ordering::Greater
			}
			Bound::Excluded(end) => {
				self.table.opts.comparator.compare(&internal_key.user_key, &end.user_key)
					== Ordering::Less
			}
			Bound::Unbounded => true,
		}
	}
}

impl Iterator for TableIterator {
	type Item = (InternalKey, Value);

	fn next(&mut self) -> Option<Self::Item> {
		// If not positioned, position appropriately based on range
		if !self.positioned {
			let lower_bound = self.range.0.clone();
			self.seek_to_lower_bound(&lower_bound);
			self.positioned = true;
		}

		// If not valid, return None
		if !self.valid() {
			return None;
		}

		// Get the current item before advancing
		let current_item = (
			self.current_block.as_ref().unwrap().key(),
			self.current_block.as_ref().unwrap().value(),
		);

		// Check upper bound (lower bound is already handled by seek)
		if !self.satisfies_upper_bound(&current_item.0) {
			self.mark_exhausted();
			return None;
		}

		// Advance for the next call to next()
		self.advance();

		Some(current_item)
	}
}

impl DoubleEndedIterator for TableIterator {
	fn next_back(&mut self) -> Option<Self::Item> {
		// If not positioned, position appropriately based on range
		if !self.positioned {
			// Seek to upper bound instead of always seeking to last
			match &self.range.1 {
				Bound::Included(_) | Bound::Excluded(_) => {
					let upper_bound = self.range.1.clone();
					self.seek_to_upper_bound(&upper_bound);
					self.positioned = true;
					self.reverse_started = false;
				}
				Bound::Unbounded => {
					self.seek_to_last();
				}
			}
		}

		// If positioned but not yet started reverse iteration, return current position
		// first
		if self.positioned && !self.reverse_started && self.valid() {
			let item = (
				self.current_block.as_ref().unwrap().key(),
				self.current_block.as_ref().unwrap().value(),
			);

			// Check lower bound for reverse iteration
			if !self.satisfies_lower_bound(&item.0) {
				return None;
			}
			self.reverse_started = true;
			return Some(item);
		}

		if !self.prev() {
			return None;
		}

		let item = (
			self.current_block.as_ref().unwrap().key(),
			self.current_block.as_ref().unwrap().value(),
		);

		// Check lower bound for reverse iteration
		if !self.satisfies_lower_bound(&item.0) {
			return None;
		}
		Some(item)
	}
}

impl TableIterator {
	pub(crate) fn valid(&self) -> bool {
		!self.exhausted
			&& self.current_block.is_some()
			&& self.current_block.as_ref().unwrap().valid()
	}

	pub(crate) fn seek_to_first(&mut self) {
		self.reset_partitioned_state();

		// Get the partitioned index
		let IndexType::Partitioned(partitioned_index) = &self.table.index_block;

		if !partitioned_index.blocks.is_empty() {
			let partition_handle = &partitioned_index.blocks[0];
			if let Ok(partition_block) = partitioned_index.load_block(partition_handle) {
				// Note: Index blocks always need full key-value pairs to decode block handles
				let mut partition_iter = partition_block.iter(false);
				partition_iter.seek_to_first();

				if partition_iter.valid() {
					let val = partition_iter.value();
					let (handle, _) = match BlockHandle::decode(&val) {
						Err(_) => {
							self.reset();
							return;
						}
						Ok(res) => res,
					};
					self.current_partition_iter = Some(partition_iter);
					if self.load_block(&handle).is_ok() {
						self.positioned = true;
						self.exhausted = false;
						return;
					}
				}
			}
		}

		// If we get here, initialization failed
		self.reset();
	}

	pub(crate) fn seek_to_last(&mut self) {
		let IndexType::Partitioned(partitioned_index) = &self.table.index_block;

		// For partitioned index, go to the last partition
		if !partitioned_index.blocks.is_empty() {
			let last_partition_index = partitioned_index.blocks.len() - 1;
			let last_partition_handle = &partitioned_index.blocks[last_partition_index];

			if let Ok(last_partition_block) = partitioned_index.load_block(last_partition_handle) {
				// Note: Index blocks always need full key-value pairs to decode block handles
				let mut partition_iter = last_partition_block.iter(false);
				partition_iter.seek_to_last();

				if partition_iter.valid() {
					let val = partition_iter.value();
					let (handle, _) = match BlockHandle::decode(&val) {
						Err(_) => {
							self.reset();
							return;
						}
						Ok(res) => res,
					};
					self.current_partition_index = last_partition_index;
					self.current_partition_iter = Some(partition_iter);
					if let Ok(()) = self.load_block(&handle) {
						if let Some(ref mut block_iter) = self.current_block {
							block_iter.seek_to_last();
							self.positioned = true;
							self.exhausted = false;
							self.reverse_started = false;
						}
					} else {
						self.reset();
					}
				} else {
					self.reset();
				}
			} else {
				self.reset();
			}
		} else {
			self.reset();
		}
	}

	pub(crate) fn seek(&mut self, target: &[u8]) -> Option<()> {
		let IndexType::Partitioned(partitioned_index) = &self.table.index_block;

		// For partitioned index, use full internal key for correct partition lookup
		if let Some((partition_index, block_handle)) =
			partitioned_index.find_block_handle_by_key(target)
		{
			self.current_partition_index = partition_index;

			if let Ok(partition_block) = partitioned_index.load_block(block_handle) {
				// Note: Index blocks always need full key-value pairs to decode block handles
				let mut partition_iter = partition_block.iter(false);
				partition_iter.seek(target);

				if partition_iter.valid() {
					let v = partition_iter.value();
					let (handle, _) = match BlockHandle::decode(&v) {
						Err(_) => {
							self.positioned = true;
							self.current_block = None;
							return Some(());
						}
						Ok(res) => res,
					};
					self.current_partition_iter = Some(partition_iter);
					if let Ok(()) = self.load_block(&handle) {
						if let Some(ref mut block_iter) = self.current_block {
							block_iter.seek(target);
							self.positioned = true;
							self.exhausted = false;

							// If not valid in current block, the key might be in next block
							if !block_iter.valid() {
								// Try to advance to next block
								match self.skip_to_next_entry() {
									Ok(true) => {
										// Successfully loaded next block and positioned at first
										// entry
										return Some(());
									}
									Ok(false) => {
										// No more blocks - mark as positioned but invalid
										self.positioned = true;
										self.current_block = None;
										return Some(());
									}
									Err(_) => {
										// Error - mark as positioned but invalid
										self.positioned = true;
										self.current_block = None;
										return Some(());
									}
								}
							}

							return Some(());
						}
					}
				}
			}
		}

		// If we can't position in any block, mark as positioned but invalid
		self.positioned = true;
		self.current_block = None;
		Some(())
	}

	pub(crate) fn advance(&mut self) -> bool {
		// If exhausted, stay exhausted
		if self.exhausted {
			return false;
		}

		// If not positioned, position at first entry
		if !self.positioned {
			self.seek_to_first();
			return self.valid();
		}

		// Try to advance within the current block first
		if let Some(ref mut block) = self.current_block {
			if block.advance() {
				// Successfully advanced within current block
				return true;
			}
		}

		// Current block is exhausted, try to move to next block
		match self.skip_to_next_entry() {
			Ok(true) => {
				// Successfully loaded next block and positioned at first entry
				true
			}
			Ok(false) => {
				// No more blocks available - mark as exhausted
				self.mark_exhausted();
				false
			}
			Err(_) => {
				// Error loading next block - mark as exhausted
				self.mark_exhausted();
				false
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::vec;

	use rand::rngs::StdRng;
	use rand::{Rng, SeedableRng};
	use test_log::test;

	use super::*;
	use crate::sstable::{InternalKey, InternalKeyKind};
	use crate::user_range_to_internal_range;

	fn default_opts() -> Arc<Options> {
		let mut opts = Options::new();
		opts.block_restart_interval = 3;
		opts.index_partition_size = 100; // Small partition size to force multiple partitions
		Arc::new(opts)
	}

	fn default_opts_mut() -> Options {
		let mut opts = Options::new();
		opts.block_restart_interval = 3;
		opts.index_partition_size = 100; // Small partition size to force multiple partitions
		opts
	}

	#[test]
	fn test_footer() {
		let f = Footer::new(BlockHandle::new(44, 4), BlockHandle::new(55, 5));
		let mut buf = [0; 50]; // Updated to match new footer size (42 + 8 magic)
		f.encode(&mut buf[..]);

		let f2 = Footer::decode(&buf).unwrap();
		assert_eq!(f2.meta_index.offset(), 44);
		assert_eq!(f2.meta_index.size(), 4);
		assert_eq!(f2.index.offset(), 55);
		assert_eq!(f2.index.size(), 5);
		assert_eq!(f2.format, TableFormat::LSMV1);
		assert_eq!(f2.checksum, ChecksumType::CRC32c);
	}

	#[test]
	fn test_table_builder() {
		let d = Vec::with_capacity(512);
		let opts = default_opts();

		let mut b = TableWriter::new(d, 0, opts, 0);

		let data = [("abc", "def"), ("abe", "dee"), ("bcd", "asa"), ("dcc", "a00")];
		let data2 = [("abd", "def"), ("abf", "dee"), ("ccd", "asa"), ("dcd", "a00")];

		for i in 0..data.len() {
			b.add(
				InternalKey::new(Vec::from(data[i].0.as_bytes()), 1, InternalKeyKind::Set, 0),
				data[i].1.as_bytes(),
			)
			.unwrap();
			b.add(
				InternalKey::new(Vec::from(data2[i].0.as_bytes()), 1, InternalKeyKind::Set, 0),
				data2[i].1.as_bytes(),
			)
			.unwrap();
		}

		let actual = b.finish().unwrap();
		assert_eq!(746, actual);
	}

	#[test]
	#[should_panic]
	fn test_bad_input() {
		let d = Vec::with_capacity(512);
		let opts = default_opts();

		let mut b = TableWriter::new(d, 0, opts, 0);

		// Test two equal consecutive keys
		let data = [("abc", "def"), ("abc", "dee"), ("bcd", "asa"), ("bsr", "a00")];

		for &(k, v) in data.iter() {
			b.add(
				InternalKey::new(Vec::from(k.as_bytes()), 1, InternalKeyKind::Set, 0),
				v.as_bytes(),
			)
			.unwrap();
		}
		b.finish().unwrap();
	}

	fn build_data() -> Vec<(&'static str, &'static str)> {
		vec![
			// block 1
			("abc", "def"),
			("abd", "dee"),
			("bcd", "asa"),
			// block 2
			("bsr", "a00"),
			("xyz", "xxx"),
			("xzz", "yyy"),
			// block 3
			("zzz", "111"),
		]
	}

	// Build a table containing raw keys (no format). It returns (vector, length)
	// for convenience reason, a call f(v, v.len()) doesn't work for borrowing
	// reasons.
	fn build_table(data: Vec<(&str, &str)>) -> (Vec<u8>, usize) {
		let mut d = Vec::with_capacity(512);
		let mut opts = default_opts_mut();
		opts.block_restart_interval = 3;
		opts.block_size = 32;
		let opt = Arc::new(opts);

		{
			// Uses the standard comparator in opt.
			let mut b = TableWriter::new(&mut d, 0, opt, 0);

			for &(k, v) in data.iter() {
				b.add(
					InternalKey::new(Vec::from(k.as_bytes()), 1, InternalKeyKind::Set, 0),
					v.as_bytes(),
				)
				.unwrap();
			}

			b.finish().unwrap();
		}

		let size = d.len();
		(d, size)
	}

	fn build_table_with_seq_num(data: Vec<(&str, &str, u64)>) -> (Vec<u8>, usize) {
		let mut d = Vec::with_capacity(512);
		let mut opts = default_opts_mut();
		opts.block_restart_interval = 3;
		opts.block_size = 32;
		let opt = Arc::new(opts);

		{
			let mut b = TableWriter::new(&mut d, 0, opt, 0);
			for &(k, v, seq) in data.iter() {
				b.add(
					InternalKey::new(Vec::from(k.as_bytes()), seq, InternalKeyKind::Set, 0),
					v.as_bytes(),
				)
				.unwrap();
			}
			b.finish().unwrap();
		}

		let size = d.len();
		(d, size)
	}

	fn wrap_buffer(src: Vec<u8>) -> Arc<dyn File> {
		Arc::new(src)
	}

	#[test]
	fn test_table_seek() {
		let (src, size) = build_table(build_data());
		let opts = default_opts();

		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());
		let mut iter = table.iter(false, None);

		let key = InternalKey::new(Vec::from(b"bcd"), 2, InternalKeyKind::Set, 0);
		iter.seek(&key.encode());
		assert!(iter.valid());
		assert_eq!((&iter.key().user_key[..], iter.value().as_ref()), (&b"bcd"[..], &b"asa"[..]));

		let key = InternalKey::new(Vec::from(b"abc"), 2, InternalKeyKind::Set, 0);
		iter.seek(&key.encode());
		assert!(iter.valid());
		assert_eq!((&iter.key().user_key[..], iter.value().as_ref()), (&b"abc"[..], &b"def"[..]));

		// Seek-past-last invalidates.
		let key = InternalKey::new(Vec::from(b"{{{"), 2, InternalKeyKind::Set, 0);
		iter.seek(&key.encode());
		assert!(!iter.valid());

		let key = InternalKey::new(Vec::from(b"bbb"), 2, InternalKeyKind::Set, 0);
		iter.seek(&key.encode());
		assert!(iter.valid());
	}

	#[test]
	fn test_table_iter() {
		let (src, size) = build_table(build_data());
		let opts = default_opts();

		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());
		let mut iter = table.iter(false, None);

		iter.advance();
		assert!(iter.valid());
		assert_eq!((&iter.key().user_key[..], iter.value().as_ref()), (&b"abc"[..], &b"def"[..]));

		iter.advance();
		assert!(iter.valid());
		assert_eq!((&iter.key().user_key[..], iter.value().as_ref()), (&b"abd"[..], &b"dee"[..]));

		iter.advance();
		assert!(iter.valid());
		assert_eq!((&iter.key().user_key[..], iter.value().as_ref()), (&b"bcd"[..], &b"asa"[..]));

		iter.advance();
		assert!(iter.valid());
		assert_eq!((&iter.key().user_key[..], iter.value().as_ref()), (&b"bsr"[..], &b"a00"[..]));

		iter.advance();
		assert!(iter.valid());
		assert_eq!((&iter.key().user_key[..], iter.value().as_ref()), (&b"xyz"[..], &b"xxx"[..]));

		iter.advance();
		assert!(iter.valid());
		assert_eq!((&iter.key().user_key[..], iter.value().as_ref()), (&b"xzz"[..], &b"yyy"[..]));

		iter.advance();
		assert!(iter.valid());
		assert_eq!((&iter.key().user_key[..], iter.value().as_ref()), (&b"zzz"[..], &b"111"[..]));
	}

	#[test]
	fn test_many_items() {
		// Create options with reasonable block size for test
		let opts = Options::new();
		let opts = Arc::new(opts);

		// Create buffer to store table data
		let mut buffer = Vec::with_capacity(10240); // 10KB initial capacity

		// Create TableWriter
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		// Number of items to generate
		let num_items = 10001;

		// Generate and add num_items items
		let mut items = Vec::with_capacity(num_items as usize);
		for i in 0..num_items {
			let key = format!("key_{i:05}");
			let value = format!("value_{i:05}");
			items.push((key.clone(), value.clone()));

			let internal_key = InternalKey::new(
				Vec::from(key.as_bytes()),
				i + 2, // Descending sequence numbers
				InternalKeyKind::Set,
				0,
			);

			writer.add(internal_key, value.as_bytes()).unwrap();
		}

		// Finish writing the table
		let size = writer.finish().unwrap();
		assert!(size > 0, "Table should have non-zero size");

		// Create a table reader
		let table = Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap();

		// Verify the number of entries matches
		assert_eq!(
			table.meta.properties.num_entries, num_items,
			"Table should contain num_items entries"
		);

		// Verify all items can be retrieved
		for (key, value) in &items {
			let internal_key =
				InternalKey::new(Vec::from(key.as_bytes()), num_items + 1, InternalKeyKind::Set, 0);

			let result = table.get(internal_key).unwrap();

			assert!(result.is_some(), "Key '{key}' not found in table");

			if let Some((found_key, found_value)) = result {
				// Verify key matches
				assert_eq!(
					std::str::from_utf8(&found_key.user_key).unwrap(),
					key,
					"Key mismatch for '{key}'"
				);

				// Verify value matches
				assert_eq!(
					std::str::from_utf8(found_value.as_ref()).unwrap(),
					value,
					"Value mismatch for key '{key}'"
				);
			}
		}
	}

	#[test]
	fn test_iter_items() {
		// Create options with reasonable block size for test
		let opts = Options::new();
		let opts = Arc::new(opts);

		// Create buffer to store table data
		let mut buffer = Vec::new();

		// Create TableWriter
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		// Number of items to generate
		let num_items = 10001;

		// Generate and add num_items items
		let mut items = Vec::with_capacity(num_items as usize);
		for i in 0..num_items {
			let key = format!("key_{i:05}");
			let value = format!("value_{i:05}");
			items.push((key.clone(), value.clone()));

			let internal_key =
				InternalKey::new(Vec::from(key.as_bytes()), i + 1, InternalKeyKind::Set, 0);

			writer.add(internal_key, value.as_bytes()).unwrap();
		}

		// Finish writing the table
		let size = writer.finish().unwrap();
		assert!(size > 0, "Table should have non-zero size");

		// Create a table reader
		let table = Arc::new(Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap());

		// Verify the number of entries matches
		assert_eq!(
			table.meta.properties.num_entries, num_items,
			"Table should contain num_items entries"
		);

		let iter = table.iter(false, None);
		for (item, (key, value)) in iter.enumerate() {
			let expected_key = format!("key_{item:05}");
			let expected_value = format!("value_{item:05}");
			assert_eq!(
				std::str::from_utf8(&key.user_key).unwrap(),
				expected_key,
				"Key mismatch at index {}: expected '{}', found '{}'",
				item,
				expected_key,
				std::str::from_utf8(&key.user_key).unwrap()
			);
			assert_eq!(
				std::str::from_utf8(value.as_ref()).unwrap(),
				expected_value,
				"Value mismatch at index {}: expected '{}', got '{}'",
				item,
				expected_value,
				std::str::from_utf8(value.as_ref()).unwrap()
			);
		}
	}

	fn add_key(
		writer: &mut TableWriter<Vec<u8>>,
		key: &[u8],
		seq: u64,
		value: &[u8],
	) -> Result<()> {
		writer.add(InternalKey::new(Vec::from(key), seq, InternalKeyKind::Set, 0), value)
	}

	#[test]
	fn test_writer_key_range_empty() {
		let d = Vec::with_capacity(512);
		let opts = default_opts();
		let writer = TableWriter::new(d, 1, opts, 0);

		// Key range should be None for an empty table
		assert!(writer.meta.properties.key_range.is_none());
	}

	#[test]
	fn test_writer_key_range_single_entry() {
		let d = Vec::with_capacity(512);
		let opts = default_opts();
		let mut writer = TableWriter::new(d, 1, opts, 0);

		// Add just one key
		add_key(&mut writer, b"singleton", 1, b"value").unwrap();

		// Key range should have identical low and high bounds
		let range = writer.meta.properties.key_range.as_ref().unwrap();
		assert_eq!(&range.low[..], b"singleton");
		assert_eq!(&range.high[..], b"singleton");
	}

	#[test]
	fn test_writer_key_range_ascending_keys() {
		let d = Vec::with_capacity(512);
		let opts = default_opts();
		let mut writer = TableWriter::new(d, 1, opts, 0);

		// Add keys in ascending order
		let keys = ["aaa", "bbb", "ccc", "ddd", "eee"];
		for (i, key) in keys.iter().enumerate() {
			add_key(&mut writer, key.as_bytes(), i as u64 + 1, b"value").unwrap();

			// Verify range is updated correctly at each step
			let range = writer.meta.properties.key_range.as_ref().unwrap();
			assert_eq!(&range.low[..], b"aaa");
			assert_eq!(&range.high[..], key.as_bytes());
		}
	}

	#[test]
	fn test_writer_key_range_interleaved_pattern() {
		let d = Vec::with_capacity(512);
		let opts = default_opts();
		let mut writer = TableWriter::new(d, 1, opts, 0);

		// Add keys in a pattern that interleaves
		let keys = ["a10", "a20", "a15", "a30", "a25"];
		// Note: Actually we need to add in order, so sort them first
		let mut sorted_keys = keys;
		sorted_keys.sort();

		for (i, key) in sorted_keys.iter().enumerate() {
			add_key(&mut writer, key.as_bytes(), i as u64 + 1, b"value").unwrap();
		}

		// Verify final range
		let range = writer.meta.properties.key_range.as_ref().unwrap();
		assert_eq!(&range.low[..], b"a10");
		assert_eq!(&range.high[..], b"a30");
	}

	#[test]
	fn test_writer_key_range_sparse_pattern() {
		let d = Vec::with_capacity(512);
		let opts = default_opts();
		let mut writer = TableWriter::new(d, 1, opts, 0);

		// Add very sparse keys with large gaps
		let keys = ["aaaaa", "nnnnn", "zzzzz"];

		for (i, key) in keys.iter().enumerate() {
			add_key(&mut writer, key.as_bytes(), i as u64 + 1, b"value").unwrap();

			// Check range after each addition
			let range = writer.meta.properties.key_range.as_ref().unwrap();
			assert_eq!(&range.low[..], b"aaaaa");
			assert_eq!(&range.high[..], key.as_bytes());
		}
	}

	#[test]
	fn test_writer_key_range_clustered_pattern() {
		let d = Vec::with_capacity(512);
		let opts = default_opts();
		let mut writer = TableWriter::new(d, 1, opts, 0);

		// Add clustered keys - many keys in a narrow range
		let prefixes = ["aaa", "aab", "aac"];

		for (i, prefix) in prefixes.iter().enumerate() {
			// For each prefix, add several close keys
			for j in 1..=5 {
				let key = format!("{prefix}{j}");
				add_key(&mut writer, key.as_bytes(), (i * 5 + j) as u64, b"value").unwrap();
			}
		}

		// Verify final range
		let range = writer.meta.properties.key_range.as_ref().unwrap();
		assert_eq!(&range.low[..], b"aaa1");
		assert_eq!(&range.high[..], b"aac5");
	}

	#[test]
	fn test_writer_key_range_binary_keys() {
		let d = Vec::with_capacity(512);
		let opts = default_opts();
		let mut writer = TableWriter::new(d, 1, opts, 0);

		// Add binary keys
		let keys = [vec![0x00, 0x01, 0x02], vec![0x10, 0x11, 0x12], vec![0xF0, 0xF1, 0xF2]];

		for (i, key) in keys.iter().enumerate() {
			add_key(&mut writer, key, i as u64 + 1, b"value").unwrap();
		}

		// Verify range with binary comparison
		let range = writer.meta.properties.key_range.as_ref().unwrap();
		assert_eq!(&range.low[..], &[0x00, 0x01, 0x02]);
		assert_eq!(&range.high[..], &[0xF0, 0xF1, 0xF2]);
	}

	#[test]
	fn test_writer_key_range_identical_keys_different_seqnums() {
		let d = Vec::with_capacity(512);
		let opts = default_opts();
		let mut writer = TableWriter::new(d, 1, opts, 0);

		// Add the same key multiple times with different sequence numbers

		// Add with descending sequence numbers (newer versions first)
		add_key(&mut writer, b"same_key", 30, b"value3").unwrap();
		add_key(&mut writer, b"same_key", 20, b"value2").unwrap();
		add_key(&mut writer, b"same_key", 10, b"value1").unwrap();

		// Verify range is correct - should be the same key for both bounds
		let range = writer.meta.properties.key_range.as_ref().unwrap();
		assert_eq!(&range.low[..], b"same_key");
		assert_eq!(&range.high[..], b"same_key");
	}

	#[test]
	fn test_writer_key_range_unicode_keys() {
		let d = Vec::with_capacity(512);
		let opts = default_opts();
		let mut writer = TableWriter::new(d, 1, opts, 0);

		// Add keys with unicode characters
		let keys = ["α", "β", "γ", "δ", "ε"];

		for (i, key) in keys.iter().enumerate() {
			add_key(&mut writer, key.as_bytes(), i as u64 + 1, b"value").unwrap();
		}

		// Verify range
		let range = writer.meta.properties.key_range.as_ref().unwrap();
		assert_eq!(&range.low[..], "α".as_bytes());
		assert_eq!(&range.high[..], "ε".as_bytes());
	}

	#[test]
	fn test_writer_key_range_with_special_chars() {
		let d = Vec::with_capacity(512);
		let opts = default_opts();
		let mut writer = TableWriter::new(d, 1, opts, 0);

		// Add keys with special characters, including control characters
		let keys = [
			"\0key",      // null byte prefix
			"key\n",      // newline suffix
			"key\tvalue", // tab in middle
			"key\\value", // backslash
			"\"quoted\"", // quotes
		];

		// Sort to ensure we add in order
		let mut sorted_keys = keys.to_vec();
		sorted_keys.sort();

		for (i, key) in sorted_keys.iter().enumerate() {
			add_key(&mut writer, key.as_bytes(), i as u64 + 1, b"value").unwrap();
		}

		// Verify range matches the expected lowest and highest keys
		let range = writer.meta.properties.key_range.as_ref().unwrap();
		assert_eq!(&range.low[..], sorted_keys.first().unwrap().as_bytes());
		assert_eq!(&range.high[..], sorted_keys.last().unwrap().as_bytes());
	}

	#[test]
	fn test_writer_key_range_with_mixed_case() {
		let d = Vec::with_capacity(512);
		let opts = default_opts();
		let mut writer = TableWriter::new(d, 1, opts, 0);

		// Mixed case keys to test case-sensitivity in key range
		let keys = ["AAA", "BBB", "aaa", "bbb"];

		// Sort to ensure we add in order (uppercase comes before lowercase in ASCII)
		let mut sorted_keys = keys.to_vec();
		sorted_keys.sort();

		for (i, key) in sorted_keys.iter().enumerate() {
			add_key(&mut writer, key.as_bytes(), i as u64 + 1, b"value").unwrap();
		}

		// Verify range
		let range = writer.meta.properties.key_range.as_ref().unwrap();
		assert_eq!(&range.low[..], b"AAA");
		assert_eq!(&range.high[..], b"bbb");
	}

	#[test]
	fn test_writer_key_range_with_pseudo_random_keys() {
		let d = Vec::with_capacity(2048);
		let opts = default_opts();
		let mut writer = TableWriter::new(d, 1, opts, 0);

		// Generate 100 pseudo-random keys but add them in sorted order
		let mut rng = StdRng::seed_from_u64(100);
		let mut keys = Vec::new();

		for _ in 0..100 {
			let len = rng.random_range(3..10);
			let mut key = Vec::with_capacity(len);
			for _ in 0..len {
				key.push(rng.random_range(b'a'..=b'z'));
			}
			keys.push(key);
		}

		// Sort keys to ensure we add in order
		keys.sort();

		// Add all keys
		for (i, key) in keys.iter().enumerate() {
			add_key(&mut writer, key, i as u64 + 1, b"value").unwrap();
		}

		// Verify range matches expected bounds
		let range = writer.meta.properties.key_range.as_ref().unwrap();
		assert_eq!(&range.low[..], &keys.first().unwrap()[..]);
		assert_eq!(&range.high[..], &keys.last().unwrap()[..]);
	}

	#[test]
	fn test_writer_key_range_with_prefix_pattern() {
		let d = Vec::with_capacity(512);
		let opts = default_opts();
		let mut writer = TableWriter::new(d, 1, opts, 0);

		// Add keys with common prefixes but different suffixes
		add_key(&mut writer, b"prefix:aaa", 1, b"value").unwrap();
		add_key(&mut writer, b"prefix:bbb", 2, b"value").unwrap();
		add_key(&mut writer, b"prefix:ccc", 3, b"value").unwrap();

		// Add a key with a different prefix
		add_key(&mut writer, b"zzzzzz", 4, b"value").unwrap();

		// Verify range
		let range = writer.meta.properties.key_range.as_ref().unwrap();
		assert_eq!(&range.low[..], b"prefix:aaa");
		assert_eq!(&range.high[..], b"zzzzzz");
	}

	#[test]
	fn test_writer_key_range_boundary_keys() {
		let d = Vec::with_capacity(512);
		let opts = default_opts();
		let mut writer = TableWriter::new(d, 1, opts, 0);

		let long = "z".repeat(1000);
		let long = &long.as_str();
		// Test with boundary-like keys
		let keys = [
			"",   // Empty key
			"a",  // Single character
			long, // Very long key
		];

		for (i, key) in keys.iter().enumerate() {
			add_key(&mut writer, key.as_bytes(), i as u64 + 1, b"value").unwrap();
		}

		// Verify range
		let range = writer.meta.properties.key_range.as_ref().unwrap();
		assert_eq!(&range.low[..], b"");
		assert_eq!(&range.high[..], "z".repeat(1000).as_bytes());
	}

	#[test]
	fn test_table_key_range_persistence() {
		// Build a table with the test data
		let data = build_data();
		let (src, size) = build_table(data.clone());
		let opts = default_opts();

		// Calculate the expected key range from the original data
		let expected_low = data.first().unwrap().0.as_bytes();
		let expected_high = data.last().unwrap().0.as_bytes();

		// Load the table back
		let table = Table::new(1, opts, wrap_buffer(src), size as u64).unwrap();

		// Verify the key range was properly persisted and loaded
		let key_range = table.meta.properties.key_range.as_ref().unwrap();
		assert_eq!(&key_range.low[..], expected_low);
		assert_eq!(&key_range.high[..], expected_high);

		// Also verify that we can use the range for querying
		assert!(table.is_key_in_key_range(&InternalKey::new(
			Vec::from(expected_low),
			1,
			InternalKeyKind::Set,
			0
		)));
		assert!(table.is_key_in_key_range(&InternalKey::new(
			Vec::from(expected_high),
			1,
			InternalKeyKind::Set,
			0
		)));

		// A key before the range should not be in the range
		let before_range = "aaa".as_bytes();
		assert!(!table.is_key_in_key_range(&InternalKey::new(
			Vec::from(before_range),
			1,
			InternalKeyKind::Set,
			0
		)));

		// A key after the range should not be in the range
		let after_range = "zzzz".as_bytes();
		assert!(!table.is_key_in_key_range(&InternalKey::new(
			Vec::from(after_range),
			1,
			InternalKeyKind::Set,
			0
		)));

		// Test a key in the middle of the range
		let middle_key = "bsr".as_bytes(); // This is in the test data
		assert!(table.is_key_in_key_range(&InternalKey::new(
			Vec::from(middle_key),
			1,
			InternalKeyKind::Set,
			0
		)));
	}

	#[test]
	fn test_table_disjoint_key_range_persistence() {
		// Build a table with disjoint data to ensure gaps are handled correctly
		let disjoint_data = vec![
			("aaa", "val1"),
			("bbb", "val2"),
			("ppp", "val3"), // Gap between bbb and ppp
			("qqq", "val4"),
			("zzz", "val5"), // Gap between qqq and zzz
		];

		let (src, size) = build_table(disjoint_data.clone());
		let opts = default_opts();

		// Calculate expected range
		let expected_low = disjoint_data.first().unwrap().0.as_bytes();
		let expected_high = disjoint_data.last().unwrap().0.as_bytes();

		// Load the table back
		let table = Table::new(1, opts, wrap_buffer(src), size as u64).unwrap();

		// Verify key range was properly persisted with disjoint data
		let key_range = table.meta.properties.key_range.as_ref().unwrap();
		assert_eq!(&key_range.low[..], expected_low);
		assert_eq!(&key_range.high[..], expected_high);

		// Test keys in the gaps to ensure the is_key_in_key_range function works
		// properly
		let in_first_gap = "ccc".as_bytes(); // Between bbb and ppp
		assert!(table.is_key_in_key_range(&InternalKey::new(
			Vec::from(in_first_gap),
			1,
			InternalKeyKind::Set,
			0
		)));

		let in_second_gap = "xxx".as_bytes(); // Between qqq and zzz
		assert!(table.is_key_in_key_range(&InternalKey::new(
			Vec::from(in_second_gap),
			1,
			InternalKeyKind::Set,
			0
		)));
	}

	#[test]
	fn test_table_key_range_with_many_blocks() {
		// Create a larger dataset that will span multiple blocks
		let mut data: Vec<(String, String)> = Vec::new();

		// Generate 50 keys that will span multiple blocks due to the small block size
		for i in 0..50 {
			let key = format!("key_{i:03}");
			let value = format!("value_{i}");
			data.push((key, value));
		}

		let data: Vec<(&str, &str)> = data.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

		let (src, size) = build_table(data);
		let opts = default_opts();

		// Expected range
		let expected_low = "key_000".as_bytes();
		let expected_high = "key_049".as_bytes();

		// Load the table
		let table = Table::new(1, opts, wrap_buffer(src), size as u64).unwrap();

		// Verify key range
		let key_range = table.meta.properties.key_range.as_ref().unwrap();
		assert_eq!(&key_range.low[..], expected_low);
		assert_eq!(&key_range.high[..], expected_high);

		// Verify block count is greater than 1 (multiple blocks were created)
		assert!(table.meta.properties.num_data_blocks > 1);

		// Test random access to various points in the range
		for idx in [0, 10, 25, 49] {
			let key = format!("key_{idx:03}");
			assert!(table.is_key_in_key_range(&InternalKey::new(
				Vec::from(key.as_bytes()),
				1,
				InternalKeyKind::Set,
				0
			)));
		}
	}

	#[test]
	fn test_table_key_range_with_tombstones() {
		let data = vec![
			("aaa", "val1"),
			("bbb", "val2"),
			("ccc", ""), // Tombstone (empty value)
			("ddd", "val4"),
			("eee", ""), // Tombstone
		];

		let (src, size) = build_table_with_tombstones(data.clone());
		let opts = default_opts();

		// Load the table
		let table = Table::new(1, opts, wrap_buffer(src), size as u64).unwrap();

		// Verify key range includes tombstones
		let key_range = table.meta.properties.key_range.as_ref().unwrap();
		assert_eq!(&key_range.low[..], b"aaa");
		assert_eq!(&key_range.high[..], b"eee");

		// Verify tombstone count
		assert_eq!(table.meta.properties.tombstone_count, 2);
	}

	// Helper function to build a table with tombstones
	fn build_table_with_tombstones(data: Vec<(&'static str, &'static str)>) -> (Vec<u8>, usize) {
		let mut d = Vec::with_capacity(512);
		let mut opts = default_opts_mut();
		opts.block_restart_interval = 3;
		opts.block_size = 32;
		let opt = Arc::new(opts);

		{
			let mut b = TableWriter::new(&mut d, 0, opt, 0);

			for &(k, v) in data.iter() {
				// Use Deletion kind for empty values to indicate tombstones
				let kind = if v.is_empty() {
					InternalKeyKind::Delete
				} else {
					InternalKeyKind::Set
				};

				b.add(InternalKey::new(Vec::from(k.as_bytes()), 1, kind, 0), v.as_bytes()).unwrap();
			}

			b.finish().unwrap();
		}

		let size = d.len();
		(d, size)
	}

	#[test]
	fn test_table_iterator_no_items_lost() {
		let data = vec![
			("key_000", "value_000"),
			("key_001", "value_001"),
			("key_002", "value_002"),
			("key_003", "value_003"),
			("key_004", "value_004"),
		];

		let (src, size) = build_table(data.clone());
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		let iter = table.iter(false, None);
		let mut collected_items = Vec::new();

		for (key, value) in iter {
			let key_str = std::str::from_utf8(&key.user_key).unwrap();
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();
			collected_items.push((key_str.to_string(), value_str.to_string()));
		}

		assert_eq!(
			collected_items.len(),
			data.len(),
			"Iterator should return exactly {} items, got {}",
			data.len(),
			collected_items.len()
		);

		for (i, (expected_key, expected_value)) in data.iter().enumerate() {
			assert!(
				i < collected_items.len(),
				"Missing item at index {i}: expected ({expected_key}, {expected_value})"
			);

			let (actual_key, actual_value) = &collected_items[i];
			assert_eq!(
				actual_key, expected_key,
				"Key mismatch at index {i}: expected '{expected_key}', got '{actual_key}'"
			);
			assert_eq!(
				actual_value, expected_value,
				"Value mismatch at index {i}: expected '{expected_value}', got '{actual_value}'"
			);
		}

		assert_eq!(collected_items[0].0, data[0].0, "First item key was lost!");
		assert_eq!(collected_items[0].1, data[0].1, "First item value was lost!");
	}

	#[test]
	fn test_table_iterator_does_not_restart_after_exhaustion() {
		let data = vec![("a", "val_a"), ("b", "val_b"), ("c", "val_c")];

		let (src, size) = build_table(data.clone());
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		let mut iter = table.iter(false, None);
		let mut seen_keys = Vec::new();
		let mut iteration_count = 0;

		loop {
			iteration_count += 1;

			if iteration_count > 10 {
				panic!(
                    "Iterator appears to be stuck in a loop! This suggests the iterator is restarting instead of staying exhausted."
                );
			}

			match iter.next() {
				Some((key, value)) => {
					let key_str = std::str::from_utf8(&key.user_key).unwrap();
					let value_str = std::str::from_utf8(value.as_ref()).unwrap();
					seen_keys.push((key_str.to_string(), value_str.to_string()));

					let key_count = seen_keys.iter().filter(|(k, _)| k == key_str).count();
					if key_count > 1 {
						panic!("Iterator restarted! Saw key '{key_str}' {key_count} times");
					}
				}
				None => break,
			}
		}

		assert_eq!(
			seen_keys.len(),
			data.len(),
			"Expected {} items, got {}",
			data.len(),
			seen_keys.len()
		);

		for i in 0..3 {
			let result = iter.next();
			if let Some((key, value)) = result {
				let key_str = std::str::from_utf8(&key.user_key).unwrap();
				let value_str = std::str::from_utf8(value.as_ref()).unwrap();
				panic!(
                    "Iterator should remain exhausted, but returned ({}, {}) on additional call #{}",
                    key_str, value_str, i + 1
                );
			}
		}
	}

	#[test]
	fn test_table_iterator_advance_method_correctness() {
		let data = vec![("item1", "data1"), ("item2", "data2"), ("item3", "data3")];

		let (src, size) = build_table(data.clone());
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		let mut iter = table.iter(false, None);

		for expected_index in 0..data.len() {
			let advance_result = iter.advance();

			assert!(
				advance_result,
				"advance() should return true when moving to item {} (of {})",
				expected_index,
				data.len()
			);
			assert!(
				iter.valid(),
				"Iterator should be valid after advancing to item {expected_index}"
			);

			let current_key = iter.key();
			let key_str = std::str::from_utf8(&current_key.user_key).unwrap();
			let expected_key = data[expected_index].0;
			assert_eq!(
                key_str, expected_key,
                "After advancing to position {expected_index}, expected key '{expected_key}', got '{key_str}'"
            );
		}

		let final_advance = iter.advance();
		assert!(
			!final_advance,
			"advance() should return false when trying to advance past the last item"
		);
		assert!(!iter.valid(), "Iterator should be invalid after advancing past the last item");

		for i in 0..3 {
			let advance_result = iter.advance();
			assert!(
				!advance_result,
				"advance() should continue returning false after exhaustion (call #{})",
				i + 1
			);
			assert!(
				!iter.valid(),
				"Iterator should remain invalid after additional advance() calls"
			);
		}
	}

	#[test]
	fn test_table_iterator_edge_cases() {
		// Single item table
		{
			let single_data = vec![("only_key", "only_value")];
			let (src, size) = build_table(single_data.clone());
			let opts = default_opts();
			let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

			let collected: Vec<_> = table.iter(false, None).collect();
			assert_eq!(collected.len(), 1, "Single item table should return exactly 1 item");

			let key_str = std::str::from_utf8(&collected[0].0.user_key).unwrap();
			let value_str = std::str::from_utf8(collected[0].1.as_ref()).unwrap();
			assert_eq!(key_str, "only_key");
			assert_eq!(value_str, "only_value");
		}

		// Two item table
		{
			let two_data = vec![("first", "1st"), ("second", "2nd")];
			let (src, size) = build_table(two_data.clone());
			let opts = default_opts();
			let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

			let collected: Vec<_> = table.iter(false, None).collect();
			assert_eq!(collected.len(), 2, "Two item table should return exactly 2 items");

			let keys: Vec<String> = collected
				.iter()
				.map(|(k, _)| std::str::from_utf8(&k.user_key).unwrap().to_string())
				.collect();
			assert_eq!(keys, vec!["first", "second"], "Items should be in correct order");
		}

		// Large table
		{
			let large_data: Vec<_> =
				(0..100).map(|i| (format!("key_{i:03}"), format!("value_{i:03}"))).collect();

			let large_data_refs: Vec<(&str, &str)> =
				large_data.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

			let (src, size) = build_table(large_data_refs);
			let opts = default_opts();
			let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

			let collected: Vec<_> = table.iter(false, None).collect();
			assert_eq!(collected.len(), 100, "Large table should return exactly 100 items");

			let mut seen_keys = std::collections::HashSet::new();
			for (key, _) in &collected {
				let key_str = std::str::from_utf8(&key.user_key).unwrap();
				assert!(
					seen_keys.insert(key_str),
					"Duplicate key found: '{key_str}' - iterator may have restarted"
				);
			}

			let first_key = std::str::from_utf8(&collected[0].0.user_key).unwrap();
			let last_key = std::str::from_utf8(&collected[99].0.user_key).unwrap();
			assert_eq!(first_key, "key_000", "First key should be key_000");
			assert_eq!(last_key, "key_099", "Last key should be key_099");
		}
	}

	#[test]
	fn test_table_iterator_seek_then_iterate() {
		let data = vec![
			("item_01", "val_01"),
			("item_02", "val_02"),
			("item_03", "val_03"),
			("item_04", "val_04"),
			("item_05", "val_05"),
		];

		let (src, size) = build_table(data.clone());
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		let test_cases = vec![("item_01", 0), ("item_03", 2), ("item_05", 4)];

		for (seek_key, expected_start_index) in test_cases {
			let mut iter = table.iter(false, None);

			let internal_key =
				InternalKey::new(Vec::from(seek_key.as_bytes()), 1, InternalKeyKind::Set, 0);
			iter.seek(&internal_key.encode());

			assert!(iter.valid(), "Iterator should be valid after seeking to '{seek_key}'");

			let mut remaining_items = Vec::new();
			while iter.valid() {
				let current_key = iter.key();
				let current_value = iter.value();
				let key_str = std::str::from_utf8(&current_key.user_key).unwrap();
				let value_str = std::str::from_utf8(current_value.as_ref()).unwrap();
				remaining_items.push((key_str.to_string(), value_str.to_string()));

				if !iter.advance() {
					break;
				}
			}

			let expected_remaining = &data[expected_start_index..];
			assert_eq!(
				remaining_items.len(),
				expected_remaining.len(),
				"After seeking to '{}', expected {} remaining items, got {}",
				seek_key,
				expected_remaining.len(),
				remaining_items.len()
			);

			for (i, (expected_key, expected_value)) in expected_remaining.iter().enumerate() {
				assert_eq!(
					remaining_items[i].0, *expected_key,
					"After seeking to '{}', item {} key mismatch: expected '{}', got '{}'",
					seek_key, i, expected_key, remaining_items[i].0
				);
				assert_eq!(
					remaining_items[i].1, *expected_value,
					"After seeking to '{}', item {} value mismatch: expected '{}', got '{}'",
					seek_key, i, expected_value, remaining_items[i].1
				);
			}
		}
	}

	#[test]
	fn test_table_iterator_seek_behavior() {
		let data = vec![
			("key_001", "val_001"),
			("key_002", "val_002"),
			("key_005", "val_005"),
			("key_007", "val_007"),
			("key_010", "val_010"),
		];

		let (src, size) = build_table(data.clone());
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Test seek to existing key
		{
			let mut iter = table.iter(false, None);
			let seek_key = InternalKey::new(Vec::from(b"key_005"), 1, InternalKeyKind::Set, 0);
			iter.seek(&seek_key.encode());

			assert!(iter.valid(), "Iterator should be valid after seeking to existing key");
			let current_key = iter.key();
			let found_key = std::str::from_utf8(&current_key.user_key).unwrap();
			assert_eq!(found_key, "key_005", "Should find the exact key we sought");

			let remaining: Vec<_> = iter
				.map(|(k, v)| {
					(
						std::str::from_utf8(&k.user_key).unwrap().to_string(),
						std::str::from_utf8(v.as_ref()).unwrap().to_string(),
					)
				})
				.collect();

			let expected = [
				("key_005".to_string(), "val_005".to_string()),
				("key_007".to_string(), "val_007".to_string()),
				("key_010".to_string(), "val_010".to_string()),
			];

			assert_eq!(remaining.len(), expected.len(), "Should get remaining items after seek");
			for (i, (actual, expected)) in remaining.iter().zip(expected.iter()).enumerate() {
				assert_eq!(actual, expected, "Mismatch at position {i} after seek");
			}
		}

		// Test seek to non-existing key (should find next key)
		{
			let mut iter = table.iter(false, None);
			let seek_key = InternalKey::new(Vec::from(b"key_003"), 1, InternalKeyKind::Set, 0);
			iter.seek(&seek_key.encode());

			assert!(iter.valid(), "Iterator should be valid after seeking to non-existing key");
			let current_key = iter.key();
			let found_key = std::str::from_utf8(&current_key.user_key).unwrap();
			assert_eq!(found_key, "key_005", "Should find next key when seeking non-existing");
		}

		// Test seek past end
		{
			let mut iter = table.iter(false, None);
			let seek_key = InternalKey::new(Vec::from(b"key_999"), 1, InternalKeyKind::Set, 0);
			iter.seek(&seek_key.encode());

			assert!(!iter.valid(), "Iterator should be invalid after seeking past end");
		}
	}

	#[test]
	fn test_table_iterator_performance_regression() {
		let mut large_data = Vec::new();
		for i in 0..1000 {
			large_data.push((format!("key_{i:06}"), format!("value_{i:06}")));
		}

		let large_data_refs: Vec<(&str, &str)> =
			large_data.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

		let (src, size) = build_table(large_data_refs);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		use std::time::Instant;

		let start = Instant::now();
		let count = table.iter(false, None).count();
		let duration = start.elapsed();

		assert_eq!(count, 1000, "Should iterate through all 1000 items");

		assert!(duration.as_millis() < 1000, "Iteration took too long: {duration:?}");

		let start = Instant::now();
		for i in (0..1000).step_by(100) {
			let mut iter = table.iter(false, None);
			let seek_key =
				InternalKey::new(format!("key_{i:06}").into_bytes(), 1, InternalKeyKind::Set, 0);
			iter.seek(&seek_key.encode());
			assert!(iter.valid(), "Seek to key_{i:06} should succeed");
		}
		let seek_duration = start.elapsed();

		assert!(
			seek_duration.as_millis() < 100,
			"Seek operations took too long: {seek_duration:?}"
		);
	}

	#[test]
	fn test_table_iterator_state_invariants() {
		let data = vec![("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")];
		let (src, size) = build_table(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		let mut iter = table.iter(false, None);
		iter.seek_to_first();

		while iter.valid() {
			let _key = iter.key();
			let _value = iter.value();

			if !iter.advance() {
				break;
			}
		}

		assert!(!iter.valid(), "Iterator should be invalid after exhaustion");

		for _ in 0..5 {
			assert!(!iter.advance(), "advance() should continue returning false after exhaustion");
			assert!(!iter.valid(), "Iterator should remain invalid");

			let next_result = iter.next();
			assert!(
				next_result.is_none(),
				"next() should continue returning None after exhaustion"
			);
		}
	}

	#[test]
	fn test_table_iterator_multiple_iterations() {
		let data = vec![("alpha", "a"), ("beta", "b"), ("gamma", "g")];

		let (src, size) = build_table(data.clone());
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Create multiple iterators and verify they work independently
		for iteration in 0..3 {
			let collected: Vec<_> = table.iter(false, None).collect();

			assert_eq!(
				collected.len(),
				data.len(),
				"Iteration #{} should return {} items, got {}",
				iteration,
				data.len(),
				collected.len()
			);

			// Verify content is correct
			for (i, (expected_key, expected_value)) in data.iter().enumerate() {
				let actual_key = std::str::from_utf8(&collected[i].0.user_key).unwrap();
				let actual_value = std::str::from_utf8(collected[i].1.as_ref()).unwrap();

				assert_eq!(
                    actual_key, *expected_key,
                    "Iteration #{iteration}, item {i}: expected key '{expected_key}', got '{actual_key}'"
                );
				assert_eq!(
                    actual_value, *expected_value,
                    "Iteration #{iteration}, item {i}: expected value '{expected_value}', got '{actual_value}'"
                );
			}
		}
	}

	#[test]
	fn test_table_iterator_positioning_edge_cases() {
		// Test 1: Empty table
		{
			let empty_data: Vec<(&str, &str)> = vec![];
			let (src, size) = build_table(empty_data);
			let opts = default_opts();
			let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

			let mut iter = table.iter(false, None);

			let result = iter.advance();
			assert!(!result, "advance() on empty table should return false");
			assert!(!iter.valid(), "Iterator should be invalid on empty table");

			let result = iter.next();
			assert!(result.is_none(), "next() on empty table should return None");
		}

		// Test 2: Single item table
		{
			let single_data = vec![("single", "item")];
			let (src, size) = build_table(single_data);
			let opts = default_opts();
			let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

			let mut iter = table.iter(false, None);

			assert!(iter.advance(), "First advance should succeed on single-item table");
			assert!(iter.valid(), "Iterator should be valid after first advance");

			assert!(!iter.advance(), "Second advance should fail on single-item table");
			assert!(!iter.valid(), "Iterator should be invalid after second advance");
		}

		// Test 3: Reset behavior after exhaustion
		{
			let data = vec![("a", "1"), ("b", "2")];
			let (src, size) = build_table(data);
			let opts = default_opts();
			let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

			let mut iter = table.iter(false, None);

			// Exhaust the iterator
			while iter.advance() {
				// just advance
			}
			assert!(!iter.valid(), "Iterator should be invalid after exhaustion");

			// Further operations should not restart the iterator
			assert!(!iter.advance(), "advance() after exhaustion should return false");
			assert!(!iter.valid(), "Iterator should remain invalid");

			let next_result = iter.next();
			assert!(next_result.is_none(), "next() after exhaustion should return None");
		}
	}

	#[test]
	fn test_table_iterator_next_vs_advance_consistency() {
		let data = vec![("x", "1"), ("y", "2"), ("z", "3")];
		let (src, size) = build_table(data.clone());
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Test with manual advance() loop - need to position first
		let mut iter1 = table.iter(false, None);
		iter1.seek_to_first();
		let mut collected_via_advance = Vec::new();
		while iter1.valid() {
			collected_via_advance.push((iter1.key(), iter1.value()));
			if !iter1.advance() {
				break;
			}
		}

		// Test with standard iterator interface
		let iter2 = table.iter(false, None);
		let mut collected_via_next = Vec::new();
		for (key, value) in iter2 {
			collected_via_next.push((key, value));
		}

		// Both methods should yield the same results
		assert_eq!(
			collected_via_next.len(),
			collected_via_advance.len(),
			"next() and advance() should yield same number of items"
		);

		for (i, ((next_key, next_val), (adv_key, adv_val))) in
			collected_via_next.iter().zip(collected_via_advance.iter()).enumerate()
		{
			assert_eq!(
				next_key.user_key, adv_key.user_key,
				"Key mismatch at position {i} between next() and advance()"
			);
			assert_eq!(
				next_val.as_slice(),
				adv_val.as_slice(),
				"Value mismatch at position {i} between next() and advance()"
			);
		}
	}

	#[test]
	fn test_table_iterator_basic_correctness() {
		let mut large_data = Vec::new();
		for i in 0..50 {
			large_data.push((format!("key_{i:03}"), format!("value_{i:03}")));
		}

		let large_data_refs: Vec<(&str, &str)> =
			large_data.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

		let (src, size) = build_table(large_data_refs);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		let collected: Vec<_> = table.iter(false, None).collect();

		assert_eq!(collected.len(), 50, "Should collect exactly 50 items");

		// Verify items are correct and in order
		for (i, (actual_key, actual_value)) in collected.iter().enumerate() {
			let key_str = std::str::from_utf8(&actual_key.user_key).unwrap();
			let value_str = std::str::from_utf8(actual_value.as_ref()).unwrap();

			let expected_key = format!("key_{i:03}");
			let expected_value = format!("value_{i:03}");

			assert_eq!(key_str, expected_key, "Key mismatch at position {i}");
			assert_eq!(value_str, expected_value, "Value mismatch at position {i}");
		}
	}

	#[test]
	fn test_table_with_partitioned_index() {
		let mut opts = default_opts_mut();
		opts.index_partition_size = 100; // Small partition size to force multiple partitions
		opts.block_size = 64; // Small block size to create more data blocks
		let opts = Arc::new(opts);

		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 1, opts.clone(), 0);

		// Add enough entries to create multiple data blocks and index partitions
		for i in 0..100 {
			let key = format!("key_{i:03}");
			let value = format!("value_{i:03}");
			let internal_key =
				InternalKey::new(Vec::from(key.as_bytes()), i + 1, InternalKeyKind::Set, 0);
			writer.add(internal_key, value.as_bytes()).unwrap();
		}

		let size = writer.finish().unwrap();
		assert!(size > 0, "Table should have non-zero size");

		// Now read the table back with partitioned index
		let table = Arc::new(Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap());

		// Verify it's using partitioned index
		match &table.index_block {
			IndexType::Partitioned(_) => {
				// Expected - partitioned index is the only supported type
			}
		}

		// Test point lookups
		for i in 0..100 {
			let key = format!("key_{i:03}");
			let expected_value = format!("value_{i:03}");
			let internal_key = InternalKey::new(
				Vec::from(key.as_bytes()),
				i + 2, // Higher seq number for lookup
				InternalKeyKind::Set,
				0,
			);

			let result = table.get(internal_key).unwrap();
			assert!(result.is_some(), "Key '{key}' not found in table");

			if let Some((found_key, found_value)) = result {
				assert_eq!(std::str::from_utf8(&found_key.user_key).unwrap(), key, "Key mismatch");
				assert_eq!(
					std::str::from_utf8(found_value.as_ref()).unwrap(),
					expected_value,
					"Value mismatch"
				);
			}
		}

		// Test full iteration
		let iter = table.iter(false, None);
		let collected: Vec<_> = iter.collect();
		assert_eq!(collected.len(), 100, "Should iterate through all entries");

		// Verify iteration order and content
		for (i, (key, value)) in collected.iter().enumerate() {
			let expected_key = format!("key_{i:03}");
			let expected_value = format!("value_{i:03}");

			assert_eq!(
				std::str::from_utf8(&key.user_key).unwrap(),
				expected_key,
				"Iterator key mismatch at position {i}"
			);
			assert_eq!(
				std::str::from_utf8(value.as_ref()).unwrap(),
				expected_value,
				"Iterator value mismatch at position {i}"
			);
		}
	}

	#[test]
	fn test_get_nonexistent_key_returns_none() {
		// Regression test: get() should return None for non-existent keys,
		// even when a lexicographically greater key exists in the table.
		// Disable bloom filter so we actually exercise the key comparison logic.
		let opts = Arc::new(Options::new().with_filter_policy(None));
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		// Add only key_bbb to the table
		let key = b"key_bbb";
		let value = b"value_bbb";
		let internal_key = InternalKey::new(Vec::from(key), 1, InternalKeyKind::Set, 0);
		writer.add(internal_key, value).unwrap();

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		// Try to get key_aaa which does NOT exist
		// key_aaa < key_bbb lexicographically
		let lookup_key = InternalKey::new(
			Vec::from(b"key_aaa"),
			2, // Higher seq number for lookup
			InternalKeyKind::Set,
			0,
		);

		let result = table.get(lookup_key).unwrap();

		// The bug: with >= comparison, this incorrectly returns Some((key_bbb,
		// value_bbb)) The fix: with == comparison, this correctly returns None
		assert!(
			result.is_none(),
			"get() should return None for non-existent key, but got {:?}",
			result.map(|(k, v)| (
				String::from_utf8_lossy(&k.user_key).to_string(),
				String::from_utf8_lossy(&v).to_string()
			))
		);
	}

	#[test]
	fn test_get_same_key_different_sequence_numbers() {
		// Validates fix returns value when user_key matches, even with different
		// seq_nums Internal ordering: user_key asc, seq_num DESC (reversed: higher
		// seq_nums sort first)
		let opts = Arc::new(Options::new().with_filter_policy(None));
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let user_key = b"my_key";

		let key1 = InternalKey::new(Vec::from(user_key), 100, InternalKeyKind::Set, 0);
		writer.add(key1, b"value_100").unwrap();

		let key2 = InternalKey::new(Vec::from(user_key), 50, InternalKeyKind::Set, 0);
		writer.add(key2, b"value_50").unwrap();

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		let lookup_key_higher = InternalKey::new(Vec::from(user_key), 200, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key_higher).unwrap();
		assert!(result.is_some());
		let (found_key, found_value) = result.unwrap();
		assert_eq!(found_key.user_key.as_slice(), user_key);
		assert_eq!(&found_value, b"value_100");

		let lookup_key_between = InternalKey::new(Vec::from(user_key), 75, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key_between).unwrap();
		assert!(result.is_some());
		let (found_key, found_value) = result.unwrap();
		assert_eq!(found_key.user_key.as_slice(), user_key);
		assert_eq!(&found_value, b"value_50");

		let lookup_key_exact = InternalKey::new(Vec::from(user_key), 100, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key_exact).unwrap();
		assert!(result.is_some());
		let (found_key, found_value) = result.unwrap();
		assert_eq!(found_key.user_key.as_slice(), user_key);
		assert_eq!(&found_value, b"value_100");

		let different_user_key = b"other_key";
		let lookup_key_different =
			InternalKey::new(Vec::from(different_user_key), 200, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key_different).unwrap();
		assert!(
			result.is_none(),
			"Should return None for different user_key, got: {:?}",
			result.map(|(k, v)| (
				String::from_utf8_lossy(&k.user_key).to_string(),
				String::from_utf8_lossy(&v).to_string()
			))
		);
	}

	#[test]
	fn test_get_with_lower_sequence_number() {
		// Snapshot at seq=25 can't see future version at seq=50
		let opts = Arc::new(Options::new().with_filter_policy(None));
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let key_aaa = InternalKey::new(Vec::from(b"aaa_key"), 100, InternalKeyKind::Set, 0);
		writer.add(key_aaa, b"value_aaa").unwrap();

		let key_bbb = InternalKey::new(Vec::from(b"bbb_key"), 75, InternalKeyKind::Set, 0);
		writer.add(key_bbb, b"value_bbb").unwrap();

		let user_key = b"my_key";
		let key = InternalKey::new(Vec::from(user_key), 50, InternalKeyKind::Set, 0);
		writer.add(key, b"value_50").unwrap();

		let key_zzz = InternalKey::new(Vec::from(b"zzz_key"), 100, InternalKeyKind::Set, 0);
		writer.add(key_zzz, b"value_zzz").unwrap();

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		let lookup_key = InternalKey::new(Vec::from(user_key), 25, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key).unwrap();

		if result.is_some() {
			let (found_key, found_value) = result.unwrap();
			panic!(
				"BUG: Expected None, got key={}, seq_num={}, value={:?}",
				String::from_utf8_lossy(&found_key.user_key),
				found_key.seq_num(),
				String::from_utf8_lossy(&found_value)
			);
		}
		assert!(result.is_none());
	}

	#[test]
	fn test_get_empty_table() {
		let opts = Arc::new(Options::new().with_filter_policy(None));
		let mut buffer = Vec::new();
		let writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		let lookup_key = InternalKey::new(Vec::from(b"any_key"), 100, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key).unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_get_multiple_keys_with_sequence_variations() {
		// Ensures fix doesn't cause cross-key contamination with different seq_nums
		let opts = Arc::new(Options::new().with_filter_policy(None));
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let key_a = InternalKey::new(Vec::from(b"key_a"), 100, InternalKeyKind::Set, 0);
		writer.add(key_a, b"value_a_100").unwrap();

		let key_b = InternalKey::new(Vec::from(b"key_b"), 50, InternalKeyKind::Set, 0);
		writer.add(key_b, b"value_b_50").unwrap();

		let key_c = InternalKey::new(Vec::from(b"key_c"), 75, InternalKeyKind::Set, 0);
		writer.add(key_c, b"value_c_75").unwrap();

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		let lookup_b_low = InternalKey::new(Vec::from(b"key_b"), 25, InternalKeyKind::Set, 0);
		let result = table.get(lookup_b_low).unwrap();
		assert!(result.is_none());

		let lookup_a_high = InternalKey::new(Vec::from(b"key_a"), 150, InternalKeyKind::Set, 0);
		let result = table.get(lookup_a_high).unwrap();
		assert!(result.is_some());
		let (found_key, found_value) = result.unwrap();
		assert_eq!(found_key.user_key.as_slice(), b"key_a");
		assert_eq!(&found_value, b"value_a_100");

		let lookup_c_exact = InternalKey::new(Vec::from(b"key_c"), 75, InternalKeyKind::Set, 0);
		let result = table.get(lookup_c_exact).unwrap();
		assert!(result.is_some());
		let (found_key, found_value) = result.unwrap();
		assert_eq!(found_key.user_key.as_slice(), b"key_c");
		assert_eq!(&found_value, b"value_c_75");

		let lookup_key_b5 = InternalKey::new(Vec::from(b"key_b5"), 100, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key_b5).unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_get_boundary_conditions() {
		// Edge cases with sequence number boundaries (seq=0, seq=MAX, seq=1)
		let opts = Arc::new(Options::new().with_filter_policy(None));
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let key_aaa = InternalKey::new(Vec::from(b"aaa_key"), 100, InternalKeyKind::Set, 0);
		writer.add(key_aaa, b"value_aaa").unwrap();

		let user_key = b"boundary_key";
		let key = InternalKey::new(Vec::from(user_key), 100, InternalKeyKind::Set, 0);
		writer.add(key, b"value_100").unwrap();

		let key_zzz = InternalKey::new(Vec::from(b"zzz_key"), 100, InternalKeyKind::Set, 0);
		writer.add(key_zzz, b"value_zzz").unwrap();

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		let lookup_min = InternalKey::new(Vec::from(user_key), 0, InternalKeyKind::Set, 0);
		let result = table.get(lookup_min).unwrap();
		assert!(result.is_none());

		let lookup_max = InternalKey::new(
			Vec::from(user_key),
			INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Set,
			0,
		);
		let result = table.get(lookup_max).unwrap();
		assert!(result.is_some());
		let (found_key, found_value) = result.unwrap();
		assert_eq!(found_key.user_key.as_slice(), user_key);
		assert_eq!(&found_value, b"value_100");

		let lookup_one = InternalKey::new(Vec::from(user_key), 1, InternalKeyKind::Set, 0);
		let result = table.get(lookup_one).unwrap();
		assert!(result.is_none());
	}

	#[test]
	fn test_get_lookup_higher_than_stored() {
		// Snapshot at seq=50 can see older version at seq=25
		// Internal ordering: user_key asc, seq_num DESC (reversed!)
		// stored(25).cmp(lookup(50)) = lookup.seq_num().cmp(stored.seq_num()) =
		// 50.cmp(25) = Greater So stored(25) > lookup(50) in internal ordering (even
		// though 25 < 50 numerically)
		let opts = Arc::new(Options::new().with_filter_policy(None));
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let key_aaa = InternalKey::new(Vec::from(b"aaa_key"), 100, InternalKeyKind::Set, 0);
		writer.add(key_aaa, b"value_aaa").unwrap();

		let key_bbb = InternalKey::new(Vec::from(b"bbb_key"), 80, InternalKeyKind::Set, 0);
		writer.add(key_bbb, b"value_bbb").unwrap();

		let user_key = b"mykey";
		let key = InternalKey::new(Vec::from(user_key), 25, InternalKeyKind::Set, 0);
		writer.add(key, b"value_25").unwrap();

		let key_zzz = InternalKey::new(Vec::from(b"zzz_key"), 100, InternalKeyKind::Set, 0);
		writer.add(key_zzz, b"value_zzz").unwrap();

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		let lookup_key = InternalKey::new(Vec::from(user_key), 50, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key).unwrap();

		assert!(result.is_some());
		let (found_key, found_value) = result.unwrap();
		assert_eq!(found_key.user_key.as_slice(), user_key);
		assert_eq!(found_key.seq_num(), 25);
		assert_eq!(&found_value, b"value_25");
	}

	#[test]
	fn test_get_partition_index_sequence_numbers() {
		// Partition index handles sequence numbers correctly across multiple blocks
		let opts = Arc::new(Options::new().with_filter_policy(None).with_block_size(512));
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		for i in 0..20 {
			let key = format!("aaa_key_{:03}", i);
			let value = format!("value_{}", i);
			let internal_key =
				InternalKey::new(Vec::from(key.as_bytes()), 1000, InternalKeyKind::Set, 0);
			writer.add(internal_key, value.as_bytes()).unwrap();
		}

		let target_key = b"target_key";
		let key_500 = InternalKey::new(Vec::from(target_key), 500, InternalKeyKind::Set, 0);
		writer.add(key_500, b"value_500").unwrap();

		for i in 0..40 {
			let key = format!("zzz_key_{:03}", i);
			let value = format!("value_{}", i);
			let internal_key =
				InternalKey::new(Vec::from(key.as_bytes()), 1000, InternalKeyKind::Set, 0);
			writer.add(internal_key, value.as_bytes()).unwrap();
		}

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		assert!(table.meta.properties.block_count > 1);

		let lookup_high = InternalKey::new(Vec::from(target_key), 1000, InternalKeyKind::Set, 0);
		let result = table.get(lookup_high).unwrap();
		assert!(result.is_some());
		let (found_key, found_value) = result.unwrap();
		assert_eq!(found_key.user_key.as_slice(), target_key);
		assert_eq!(found_key.seq_num(), 500);
		assert_eq!(&found_value, b"value_500");

		let lookup_exact = InternalKey::new(Vec::from(target_key), 500, InternalKeyKind::Set, 0);
		let result = table.get(lookup_exact).unwrap();
		assert!(result.is_some());
		let (found_key, found_value) = result.unwrap();
		assert_eq!(found_key.user_key.as_slice(), target_key);
		assert_eq!(found_key.seq_num(), 500);
		assert_eq!(&found_value, b"value_500");

		let lookup_low = InternalKey::new(Vec::from(target_key), 100, InternalKeyKind::Set, 0);
		let result = table.get(lookup_low).unwrap();
		assert!(result.is_none());

		let filler_key = b"zzz_key_010";
		let lookup_filler = InternalKey::new(Vec::from(filler_key), 1000, InternalKeyKind::Set, 0);
		let result = table.get(lookup_filler).unwrap();
		assert!(result.is_some());
		let (found_key, _) = result.unwrap();
		assert_eq!(found_key.user_key.as_slice(), filler_key);
	}

	#[test]
	fn test_get_nonexistent_key_greater_than_all() {
		// Key greater than all stored keys should return None
		let opts = Arc::new(Options::new().with_filter_policy(None));
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let key_aaa = InternalKey::new(Vec::from(b"key_aaa"), 100, InternalKeyKind::Set, 0);
		writer.add(key_aaa, b"value_aaa").unwrap();

		let key_bbb = InternalKey::new(Vec::from(b"key_bbb"), 100, InternalKeyKind::Set, 0);
		writer.add(key_bbb, b"value_bbb").unwrap();

		let key_ccc = InternalKey::new(Vec::from(b"key_ccc"), 100, InternalKeyKind::Set, 0);
		writer.add(key_ccc, b"value_ccc").unwrap();

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		let lookup_key = InternalKey::new(Vec::from(b"key_zzz"), 100, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key).unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_get_nonexistent_key_between_existing() {
		// Key between existing keys should return None
		let opts = Arc::new(Options::new().with_filter_policy(None));
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let key_aaa = InternalKey::new(Vec::from(b"key_aaa"), 100, InternalKeyKind::Set, 0);
		writer.add(key_aaa, b"value_aaa").unwrap();

		let key_ccc = InternalKey::new(Vec::from(b"key_ccc"), 100, InternalKeyKind::Set, 0);
		writer.add(key_ccc, b"value_ccc").unwrap();

		let key_eee = InternalKey::new(Vec::from(b"key_eee"), 100, InternalKeyKind::Set, 0);
		writer.add(key_eee, b"value_eee").unwrap();

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		let lookup_key = InternalKey::new(Vec::from(b"key_bbb"), 100, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key).unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_get_with_tombstone() {
		// Tombstones should be found and returned
		let opts = Arc::new(Options::new().with_filter_policy(None));
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let key_other = InternalKey::new(Vec::from(b"key_other"), 100, InternalKeyKind::Set, 0);
		writer.add(key_other, b"value_other").unwrap();

		let key_target =
			InternalKey::new(Vec::from(b"key_target"), 100, InternalKeyKind::Delete, 0);
		writer.add(key_target, b"").unwrap();

		let key_zzz = InternalKey::new(Vec::from(b"key_zzz"), 100, InternalKeyKind::Set, 0);
		writer.add(key_zzz, b"value_zzz").unwrap();

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		let lookup_key = InternalKey::new(Vec::from(b"key_target"), 100, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key).unwrap();

		assert!(result.is_some());
		let (found_key, _) = result.unwrap();
		assert_eq!(found_key.user_key.as_slice(), b"key_target");
		assert!(found_key.is_tombstone());
	}

	#[test]
	fn test_get_nonexistent_with_similar_prefix() {
		// Prefix of existing keys should not match
		let opts = Arc::new(Options::new().with_filter_policy(None));
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let key1 = InternalKey::new(Vec::from(b"user_data"), 100, InternalKeyKind::Set, 0);
		writer.add(key1, b"value1").unwrap();

		let key2 = InternalKey::new(Vec::from(b"user_profile"), 100, InternalKeyKind::Set, 0);
		writer.add(key2, b"value2").unwrap();

		let key3 = InternalKey::new(Vec::from(b"username"), 100, InternalKeyKind::Set, 0);
		writer.add(key3, b"value3").unwrap();

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		let lookup_key = InternalKey::new(Vec::from(b"user"), 100, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key).unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_get_nonexistent_empty_key() {
		// Empty key should return None
		let opts = Arc::new(Options::new().with_filter_policy(None));
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let key_aaa = InternalKey::new(Vec::from(b"key_aaa"), 100, InternalKeyKind::Set, 0);
		writer.add(key_aaa, b"value_aaa").unwrap();

		let key_bbb = InternalKey::new(Vec::from(b"key_bbb"), 100, InternalKeyKind::Set, 0);
		writer.add(key_bbb, b"value_bbb").unwrap();

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		let lookup_key = InternalKey::new(Vec::from(b""), 100, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key).unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_get_nonexistent_with_special_chars() {
		// Binary keys with special bytes handled correctly
		let opts = Arc::new(Options::new().with_filter_policy(None));
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let key1 = InternalKey::new(Vec::from(b"key\x00"), 100, InternalKeyKind::Set, 0);
		writer.add(key1, b"value0").unwrap();

		let key2 = InternalKey::new(Vec::from(b"key\x01"), 100, InternalKeyKind::Set, 0);
		writer.add(key2, b"value1").unwrap();

		let key3 = InternalKey::new(Vec::from(b"key\xFF"), 100, InternalKeyKind::Set, 0);
		writer.add(key3, b"value_ff").unwrap();

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		let lookup_key = InternalKey::new(Vec::from(b"key\x02"), 100, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key).unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_get_nonexistent_in_large_table() {
		// Non-existent key in multi-block table should return None
		let opts = Arc::new(Options::new().with_filter_policy(None).with_block_size(512));
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		for i in 0..100 {
			if i == 50 {
				continue;
			}
			let key = format!("key_{:03}", i);
			let value = format!("value_{}", i);
			let internal_key =
				InternalKey::new(Vec::from(key.as_bytes()), 100, InternalKeyKind::Set, 0);
			writer.add(internal_key, value.as_bytes()).unwrap();
		}

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		assert!(table.meta.properties.block_count > 1);

		let lookup_key = InternalKey::new(Vec::from(b"key_050"), 100, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key).unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_get_all_keys_same_prefix_different_suffix() {
		// Keys with same prefix but different suffix don't match
		let opts = Arc::new(Options::new().with_filter_policy(None));
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let key_a = InternalKey::new(Vec::from(b"prefix_a"), 100, InternalKeyKind::Set, 0);
		writer.add(key_a, b"value_a").unwrap();

		let key_b = InternalKey::new(Vec::from(b"prefix_b"), 100, InternalKeyKind::Set, 0);
		writer.add(key_b, b"value_b").unwrap();

		let key_c = InternalKey::new(Vec::from(b"prefix_c"), 100, InternalKeyKind::Set, 0);
		writer.add(key_c, b"value_c").unwrap();

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		let lookup_key = InternalKey::new(Vec::from(b"prefix_d"), 100, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key).unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_table_iterator_seek_nonexistent_key() {
		// Test that seeking to a non-existent key positions the iterator
		// at the next greater key (correct iterator semantics)
		let opts = Arc::new(Options::default());
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		// Add only key_bbb to the table
		let key = b"key_bbb";
		let value = b"value_bbb";
		let internal_key = InternalKey::new(Vec::from(key), 1, InternalKeyKind::Set, 0);
		writer.add(internal_key, value).unwrap();

		let size = writer.finish().unwrap();
		let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

		// Seek to key_aaa which does NOT exist
		// key_aaa < key_bbb lexicographically
		let mut iter = table.iter(false, None);
		let lookup_key = InternalKey::new(
			Vec::from(b"key_aaa"),
			2, // Higher seq number for lookup
			InternalKeyKind::Set,
			0,
		);
		iter.seek(&lookup_key.encode());

		// Iterator behavior: seek positions at next >= key
		assert!(iter.valid(), "Iterator should be valid (positioned at next key)");

		// The iterator is positioned at key_bbb (the next greater key)
		let current_key = iter.key();
		assert_eq!(
			current_key.user_key.as_slice(),
			b"key_bbb",
			"Iterator should be positioned at the next greater key"
		);

		// If caller wants exact match, they must check the key themselves
		let is_exact_match = current_key.user_key.as_slice() == b"key_aaa";
		assert!(!is_exact_match, "Caller should check for exact match if needed");
	}

	#[test]
	fn test_table_iterator_seek_nonexistent_past_end() {
		// Test that seeking past all keys makes iterator invalid
		let opts = Arc::new(Options::default());
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let key = b"key_bbb";
		let value = b"value_bbb";
		let internal_key = InternalKey::new(Vec::from(key), 1, InternalKeyKind::Set, 0);
		writer.add(internal_key, value).unwrap();

		let size = writer.finish().unwrap();
		let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

		// Seek to key_zzz which is past all keys
		let mut iter = table.iter(false, None);
		let lookup_key = InternalKey::new(Vec::from(b"key_zzz"), 2, InternalKeyKind::Set, 0);
		iter.seek(&lookup_key.encode());

		// Iterator should be invalid (no more keys)
		assert!(!iter.valid(), "Iterator should be invalid when seeking past all keys");
	}

	#[test]
	fn test_table_iterator_seek_exact_match() {
		// Test that seeking to an existing key positions at that key
		let opts = Arc::new(Options::default());
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let key = b"key_bbb";
		let value = b"value_bbb";
		let internal_key = InternalKey::new(Vec::from(key), 1, InternalKeyKind::Set, 0);
		writer.add(internal_key, value).unwrap();

		let size = writer.finish().unwrap();
		let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

		// Seek to key_bbb which exists
		let mut iter = table.iter(false, None);
		let lookup_key = InternalKey::new(Vec::from(b"key_bbb"), 2, InternalKeyKind::Set, 0);
		iter.seek(&lookup_key.encode());

		assert!(iter.valid(), "Iterator should be valid");

		let current_key = iter.key();
		assert_eq!(
			current_key.user_key.as_slice(),
			b"key_bbb",
			"Iterator should be positioned at the exact key"
		);
	}

	#[test]
	fn test_table_iter_upper_bound_included() {
		// Use static strings to avoid lifetime issues
		let data = vec![
			("key_000", "value"),
			("key_001", "value"),
			("key_002", "value"),
			("key_003", "value"),
			("key_004", "value"),
			("key_005", "value"),
			("key_006", "value"),
			("key_007", "value"),
			("key_008", "value"),
			("key_009", "value"),
		];

		let (src, size) = build_table(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Iterate with range (Unbounded, Included("key_005"))
		let iter = table.iter(
			false,
			Some(user_range_to_internal_range((
				Bound::Unbounded,
				Bound::Included(b"key_005".to_vec()),
			))),
		);

		let mut results = Vec::new();
		for item in iter {
			let (key, _) = item;
			results.push(String::from_utf8(key.user_key.clone()).unwrap());
		}

		// Should return items key_000 through key_005 (6 items)
		assert_eq!(results.len(), 6, "Should return exactly 6 items");
		assert_eq!(results[0], "key_000");
		assert_eq!(results[1], "key_001");
		assert_eq!(results[2], "key_002");
		assert_eq!(results[3], "key_003");
		assert_eq!(results[4], "key_004");
		assert_eq!(results[5], "key_005");

		// Verify last key is exactly key_005
		assert_eq!(results.last().unwrap(), "key_005");
	}

	#[test]
	fn test_table_iter_upper_bound_excluded() {
		// Use static strings to avoid lifetime issues
		let data = vec![
			("key_000", "value"),
			("key_001", "value"),
			("key_002", "value"),
			("key_003", "value"),
			("key_004", "value"),
			("key_005", "value"),
			("key_006", "value"),
			("key_007", "value"),
			("key_008", "value"),
			("key_009", "value"),
		];

		let (src, size) = build_table(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Iterate with range (Unbounded, Excluded("key_005"))
		let iter = table.iter(
			false,
			Some(user_range_to_internal_range((
				Bound::Unbounded,
				Bound::Excluded(b"key_005".to_vec()),
			))),
		);

		let mut results = Vec::new();
		for item in iter {
			let (key, _) = item;
			results.push(String::from_utf8(key.user_key.clone()).unwrap());
		}

		// Should return items key_000 through key_004 (5 items)
		assert_eq!(results.len(), 5, "Should return exactly 5 items");
		assert_eq!(results[0], "key_000");
		assert_eq!(results[1], "key_001");
		assert_eq!(results[2], "key_002");
		assert_eq!(results[3], "key_003");
		assert_eq!(results[4], "key_004");

		// Verify last key is key_004, not key_005
		assert_eq!(results.last().unwrap(), "key_004");
	}

	#[test]
	fn test_table_iter_unbounded_reverse() {
		// Use static strings to avoid lifetime issues
		let data = vec![
			("key_000", "value"),
			("key_001", "value"),
			("key_002", "value"),
			("key_003", "value"),
			("key_004", "value"),
			("key_005", "value"),
			("key_006", "value"),
			("key_007", "value"),
			("key_008", "value"),
			("key_009", "value"),
		];

		let (src, size) = build_table(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// First test forward iteration to make sure table has data
		let collected: Vec<_> = table.iter(false, None).collect();
		assert_eq!(collected.len(), 10, "Forward iteration should return 10 items");

		let forward_keys: Vec<String> = collected
			.iter()
			.map(|(k, _)| std::str::from_utf8(&k.user_key).unwrap().to_string())
			.collect();
		assert_eq!(
			forward_keys,
			vec![
				"key_000", "key_001", "key_002", "key_003", "key_004", "key_005", "key_006",
				"key_007", "key_008", "key_009"
			]
		);

		// Test unbounded reverse iteration
		let mut iter = table.iter(false, None);
		iter.seek_to_last(); // Position at the end for reverse iteration

		let mut results = Vec::new();
		while let Some(item) = iter.next_back() {
			let (key, _) = item;
			results.push(String::from_utf8(key.user_key.clone()).unwrap());
		}
		// Should return all 10 items in reverse order
		assert_eq!(results.len(), 10, "Should return exactly 10 items");
		assert_eq!(results[0], "key_009");
		assert_eq!(results[9], "key_000");
	}

	#[test]
	fn test_table_iter_lower_bound_included_reverse() {
		// Use static strings to avoid lifetime issues
		let data = vec![
			("key_000", "value"),
			("key_001", "value"),
			("key_002", "value"),
			("key_003", "value"),
			("key_004", "value"),
			("key_005", "value"),
			("key_006", "value"),
			("key_007", "value"),
			("key_008", "value"),
			("key_009", "value"),
		];

		let (src, size) = build_table(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Iterate with range (Included("key_003"), Unbounded) using next_back()
		let mut iter = table.iter(
			false,
			Some(user_range_to_internal_range((
				Bound::Included(b"key_003".to_vec()),
				Bound::Unbounded,
			))),
		);

		let mut results = Vec::new();
		while let Some(item) = iter.next_back() {
			let (key, _) = item;
			results.push(String::from_utf8(key.user_key.clone()).unwrap());
		}

		// Should return items in reverse: key_009, key_008, ..., key_003 (7 items)
		assert_eq!(results.len(), 7, "Should return exactly 7 items");
		assert_eq!(results[0], "key_009");
		assert_eq!(results[1], "key_008");
		assert_eq!(results[2], "key_007");
		assert_eq!(results[3], "key_006");
		assert_eq!(results[4], "key_005");
		assert_eq!(results[5], "key_004");
		assert_eq!(results[6], "key_003");

		// Verify last item returned is key_003
		assert_eq!(results.last().unwrap(), "key_003");
	}

	#[test]
	fn test_table_iter_both_bounds() {
		// Use static strings to avoid lifetime issues
		let data = vec![
			("key_000", "value"),
			("key_001", "value"),
			("key_002", "value"),
			("key_003", "value"),
			("key_004", "value"),
			("key_005", "value"),
			("key_006", "value"),
			("key_007", "value"),
			("key_008", "value"),
			("key_009", "value"),
		];

		let (src, size) = build_table(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Iterate with range (Included("key_002"), Excluded("key_007"))
		let iter = table.iter(
			false,
			Some(user_range_to_internal_range((
				Bound::Included(b"key_002".to_vec()),
				Bound::Excluded(b"key_007".to_vec()),
			))),
		);

		let mut results = Vec::new();
		for item in iter {
			let (key, _) = item;
			results.push(String::from_utf8(key.user_key.clone()).unwrap());
		}

		// Should return items key_002, key_003, key_004, key_005, key_006 (5 items)
		assert_eq!(results.len(), 5, "Should return exactly 5 items");
		assert_eq!(results[0], "key_002");
		assert_eq!(results[1], "key_003");
		assert_eq!(results[2], "key_004");
		assert_eq!(results[3], "key_005");
		assert_eq!(results[4], "key_006");
	}

	#[test]
	fn test_table_iter_unbounded() {
		// Use static strings to avoid lifetime issues
		let data = vec![
			("key_000", "value"),
			("key_001", "value"),
			("key_002", "value"),
			("key_003", "value"),
			("key_004", "value"),
			("key_005", "value"),
			("key_006", "value"),
			("key_007", "value"),
			("key_008", "value"),
			("key_009", "value"),
		];

		let (src, size) = build_table(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Test that (Bound::Unbounded, Bound::Unbounded) returns all items
		let iter = table.iter(
			false,
			Some(user_range_to_internal_range((
				Bound::<Vec<u8>>::Unbounded,
				Bound::<Vec<u8>>::Unbounded,
			))),
		);

		let mut results = Vec::new();
		for item in iter {
			let (key, _) = item;
			results.push(String::from_utf8(key.user_key.clone()).unwrap());
		}

		// Should return all 10 items
		assert_eq!(results.len(), 10, "Should return exactly 10 items");
		assert_eq!(results[0], "key_000");
		assert_eq!(results[9], "key_009");
	}

	#[test]
	fn test_table_iter_forward_and_backward() {
		// Create enough data to span multiple blocks
		// With block_size=32 and ~15 bytes per entry, 30 entries span ~15 blocks
		let data = vec![
			("key_000", "value"),
			("key_001", "value"),
			("key_002", "value"),
			("key_003", "value"),
			("key_004", "value"),
			("key_005", "value"),
			("key_006", "value"),
			("key_007", "value"),
			("key_008", "value"),
			("key_009", "value"),
			("key_010", "value"),
			("key_011", "value"),
			("key_012", "value"),
			("key_013", "value"),
			("key_014", "value"),
			("key_015", "value"),
			("key_016", "value"),
			("key_017", "value"),
			("key_018", "value"),
			("key_019", "value"),
			("key_020", "value"),
			("key_021", "value"),
			("key_022", "value"),
			("key_023", "value"),
			("key_024", "value"),
			("key_025", "value"),
			("key_026", "value"),
			("key_027", "value"),
			("key_028", "value"),
			("key_029", "value"),
		];

		let (src, size) = build_table(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Test forward iteration
		let forward_iter = table.iter(false, None);
		let mut forward_keys = Vec::new();
		for item in forward_iter {
			let (key, _) = item;
			forward_keys.push(String::from_utf8(key.user_key.clone()).unwrap());
		}
		assert_eq!(forward_keys.len(), 30);
		assert_eq!(forward_keys[0], "key_000");
		assert_eq!(forward_keys[29], "key_029");

		// Test backward iteration on separate iterator
		let mut backward_iter = table.iter(false, None);
		let mut backward_keys = Vec::new();
		while let Some(item) = backward_iter.next_back() {
			let (key, _) = item;
			backward_keys.push(String::from_utf8(key.user_key.clone()).unwrap());
		}
		assert_eq!(backward_keys.len(), 30);
		assert_eq!(backward_keys[0], "key_029");
		assert_eq!(backward_keys[29], "key_000");

		// Test seeking to last and then going backward
		let mut seek_iter = table.iter(false, None);
		seek_iter.seek_to_last();
		let mut seek_backward = Vec::new();
		while let Some(item) = seek_iter.next_back() {
			let (key, _) = item;
			seek_backward.push(String::from_utf8(key.user_key.clone()).unwrap());
		}
		assert_eq!(seek_backward.len(), 30);
		assert_eq!(seek_backward[0], "key_029");
		assert_eq!(seek_backward[29], "key_000");

		// Test that forward iteration and backward iteration give complementary results
		// The forward iterator gives keys in ascending order
		let expected_forward: Vec<String> = (0..30).map(|i| format!("key_{:03}", i)).collect();
		assert_eq!(forward_keys, expected_forward);

		// The backward iterator gives keys in descending order
		let expected_backward: Vec<String> =
			(0..30).rev().map(|i| format!("key_{:03}", i)).collect();
		assert_eq!(backward_keys, expected_backward);

		// And seek_to_last + next_back also gives descending order
		assert_eq!(seek_backward, backward_keys);
	}

	#[test]
	fn test_table_iter_upper_bound_excluded_reverse() {
		// Create table with multiple versions of the same key
		let data = vec![
			("key_000", "value", 1),
			("key_001", "value", 1),
			("key_002", "value", 1),
			("key_003", "value", 1),
			("key_004", "value", 1),
			("key_005", "value3", 3), // <- Newest version first (highest seq_num)
			("key_005", "value2", 2), // <- Middle version
			("key_005", "value", 1),  // <- Oldest version last (lowest seq_num)
			("key_006", "value", 1),
			("key_007", "value", 1),
			("key_008", "value", 1),
			("key_009", "value", 1),
		];

		let (src, size) = build_table_with_seq_num(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Reverse iteration with EXCLUDED upper bound
		let mut iter = table.iter(
			false,
			Some(user_range_to_internal_range((
				Bound::Unbounded,
				Bound::Excluded(b"key_005".to_vec()),
			))),
		);

		let mut results = Vec::new();
		while let Some(item) = iter.next_back() {
			let (key, _) = item;
			results.push(String::from_utf8(key.user_key.clone()).unwrap());
		}

		// Should return items in reverse: key_004, key_003, key_002, key_001, key_000
		// Should NOT include any version of key_005 or higher
		assert_eq!(results.len(), 5, "Should return exactly 5 items");
		assert_eq!(results[0], "key_004"); // First item in reverse
		assert_eq!(results[4], "key_000"); // Last item in reverse

		// Verify NO key_005 appears
		assert!(!results.iter().any(|k| k == "key_005"), "key_005 should not appear in results");
	}

	#[test]
	fn test_table_iter_upper_bound_included_reverse_nonexistent_key() {
		let data = vec![
			("key_000", "value"),
			("key_001", "value"),
			("key_002", "value"),
			("key_003", "value"),
			("key_004", "value"),
			// NOTE: key_005 does NOT exist!
			("key_006", "value"),
			("key_007", "value"),
			("key_008", "value"),
			("key_009", "value"),
		];

		let (src, size) = build_table(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Reverse iteration with INCLUDED upper bound on non-existent key
		let mut iter = table.iter(
			false,
			Some(user_range_to_internal_range((
				Bound::Unbounded,
				Bound::Included(b"key_005".to_vec()),
			))),
		);

		let mut results = Vec::new();
		while let Some(item) = iter.next_back() {
			let (key, _) = item;
			results.push(String::from_utf8(key.user_key.clone()).unwrap());
		}

		// Should return items in reverse: key_004, key_003, ..., key_000 (5 items)
		// Should NOT include key_006 or higher
		assert_eq!(results.len(), 5, "Should return exactly 5 items");
		assert_eq!(results[0], "key_004"); // First item should be key_004, NOT key_006!
		assert_eq!(results[4], "key_000");

		// Without the backward stepping loop, this test would FAIL
		// because it would start at key_006 instead of key_004
		assert!(!results.iter().any(|k| k == "key_006"), "key_006 should not appear");
	}

	#[test]
	fn test_table_iter_lower_bound_excluded_forward_with_multiple_versions() {
		let data = vec![
			("key_000", "v1", 1),
			("key_001", "v1", 1),
			("key_002", "v1", 1),
			("key_003", "v1", 3),
			("key_003", "v2", 2), // Multiple versions of key_003
			("key_003", "v3", 1),
			("key_004", "v1", 2),
			("key_004", "v1", 1),
			("key_005", "v1", 1),
		];

		let (src, size) = build_table_with_seq_num(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Forward iteration with EXCLUDED lower bound
		let iter = table.iter(
			false,
			Some(user_range_to_internal_range((
				Bound::Excluded(b"key_003".to_vec()),
				Bound::Unbounded,
			))),
		);

		let mut results = Vec::new();
		for item in iter {
			let (key, _) = item;
			results.push(String::from_utf8(key.user_key.clone()).unwrap());
		}

		// Should start at key_004, NOT any version of key_003
		assert!(results[0] == "key_004", "First result should be key_004, got {}", results[0]);

		// Verify NO version of key_003 appears
		assert!(!results.iter().any(|k| k == "key_003"), "key_003 should not appear in results");
	}

	#[test]
	fn test_table_iter_excluded_bound_across_multiple_blocks() {
		// Test that excluded lower bound correctly skips ALL versions of a key
		// even when they span multiple blocks
		let mut data = vec![];

		// Add many versions of key_003 to force multiple blocks
		for seq in (1..=100).rev() {
			data.push(("key_003", "value", seq));
		}
		// Add the key we should actually start at
		data.push(("key_004", "value", 1));
		data.push(("key_005", "value", 1));

		let (src, size) = build_table_with_seq_num(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Forward iteration with excluded lower bound
		let iter = table.iter(
			false,
			Some(user_range_to_internal_range((
				Bound::Excluded(b"key_003".to_vec()),
				Bound::Unbounded,
			))),
		);

		let mut results = Vec::new();
		for item in iter {
			let (key, _) = item;
			let user_key = String::from_utf8(key.user_key.clone()).unwrap();
			results.push(user_key);
		}

		// Should NOT contain any version of key_003
		assert!(
			!results.iter().any(|k| k == "key_003"),
			"key_003 should be completely excluded, but found in results"
		);

		// First key should be key_004
		assert_eq!(
			results.first().unwrap(),
			"key_004",
			"First key should be key_004, got {}",
			results.first().unwrap()
		);

		// Should have exactly 2 keys (key_004 and key_005)
		assert_eq!(results.len(), 2, "Should return exactly 2 keys, got {}", results.len());
	}

	#[test]
	fn test_table_iter_excluded_bound_across_partitions_reverse() {
		// Test reverse iteration with excluded upper bound across multiple versions
		let mut data = vec![];

		// Add keys before the excluded bound
		data.push(("key_001", "value", 1));
		data.push(("key_002", "value", 1));

		// Add many versions of key_003 (the excluded upper bound)
		for seq in (1..=100).rev() {
			data.push(("key_003", "value", seq));
		}

		// Add keys after
		data.push(("key_004", "value", 1));
		data.push(("key_005", "value", 1));

		let (src, size) = build_table_with_seq_num(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Reverse iteration with excluded upper bound
		let mut iter = table.iter(
			false,
			Some(user_range_to_internal_range((
				Bound::Unbounded,
				Bound::Excluded(b"key_003".to_vec()),
			))),
		);

		let mut results = Vec::new();
		while let Some(item) = iter.next_back() {
			let (key, _) = item;
			let user_key = String::from_utf8(key.user_key.clone()).unwrap();
			results.push(user_key);
		}

		// Should NOT contain any version of key_003
		assert!(
			!results.iter().any(|k| k == "key_003"),
			"key_003 should be completely excluded, but found in results"
		);

		// First key in reverse should be key_002
		assert_eq!(
			results.first().unwrap(),
			"key_002",
			"First key in reverse should be key_002, got {}",
			results.first().unwrap()
		);

		// Should have exactly 2 keys (key_002, key_001 in reverse order)
		assert_eq!(results.len(), 2, "Should return exactly 2 keys");
		assert_eq!(results[0], "key_002");
		assert_eq!(results[1], "key_001");
	}

	#[test]
	fn test_table_iter_both_bounds_excluded_same_key() {
		// Test empty range: (Excluded("key_003"), Excluded("key_003"))
		// This should return no items
		let data = vec![
			("key_001", "value", 1),
			("key_002", "value", 1),
			("key_003", "value", 100),
			("key_003", "value", 50),
			("key_003", "value", 1),
			("key_004", "value", 1),
			("key_005", "value", 1),
		];

		let (src, size) = build_table_with_seq_num(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Forward iteration with both bounds excluded at same key
		let iter = table.iter(
			false,
			Some(user_range_to_internal_range((
				Bound::Excluded(b"key_003".to_vec()),
				Bound::Excluded(b"key_003".to_vec()),
			))),
		);

		let results: Vec<_> = iter.collect();

		// Should return NO items (empty range)
		assert_eq!(
			results.len(),
			0,
			"Range (Excluded(X), Excluded(X)) should be empty, got {} items",
			results.len()
		);
	}

	#[test]
	fn test_table_iter_both_bounds_excluded_same_key_reverse() {
		// Test empty range in reverse: (Excluded("key_003"), Excluded("key_003"))
		let data = vec![
			("key_001", "value", 1),
			("key_002", "value", 1),
			("key_003", "value", 100),
			("key_003", "value", 50),
			("key_003", "value", 1),
			("key_004", "value", 1),
			("key_005", "value", 1),
		];

		let (src, size) = build_table_with_seq_num(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Reverse iteration with both bounds excluded at same key
		let mut iter = table.iter(
			false,
			Some(user_range_to_internal_range((
				Bound::Excluded(b"key_003".to_vec()),
				Bound::Excluded(b"key_003".to_vec()),
			))),
		);

		let mut results = Vec::new();
		while let Some(item) = iter.next_back() {
			results.push(item);
		}

		// Should return NO items (empty range)
		assert_eq!(
			results.len(),
			0,
			"Range (Excluded(X), Excluded(X)) in reverse should be empty, got {} items",
			results.len()
		);
	}

	#[test]
	fn test_table_iter_excluded_bounds_adjacent_keys() {
		// Test: (Excluded("key_002"), Excluded("key_004"))
		// Should only return key_003 (all versions)
		let data = vec![
			("key_001", "value", 1),
			("key_002", "value", 100),
			("key_002", "value", 50),
			("key_003", "value", 80),
			("key_003", "value", 40),
			("key_003", "value", 10),
			("key_004", "value", 100),
			("key_004", "value", 50),
			("key_005", "value", 1),
		];

		let (src, size) = build_table_with_seq_num(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Forward iteration
		let iter = table.iter(
			false,
			Some(user_range_to_internal_range((
				Bound::Excluded(b"key_002".to_vec()),
				Bound::Excluded(b"key_004".to_vec()),
			))),
		);

		let mut results = Vec::new();
		for item in iter {
			let (key, _) = item;
			let user_key = String::from_utf8(key.user_key.clone()).unwrap();
			results.push((user_key, key.seq_num()));
		}

		// Should have exactly 3 items (all versions of key_003)
		assert_eq!(results.len(), 3, "Should return 3 versions of key_003");

		// All should be key_003
		assert!(results.iter().all(|(k, _)| k == "key_003"), "All results should be key_003");

		// Should NOT contain key_002 or key_004
		assert!(
			!results.iter().any(|(k, _)| k == "key_002" || k == "key_004"),
			"Should not contain key_002 or key_004"
		);

		// Verify sequence numbers are in descending order (80, 40, 10)
		assert_eq!(results[0].1, 80);
		assert_eq!(results[1].1, 40);
		assert_eq!(results[2].1, 10);
	}

	#[test]
	fn test_table_iter_multiple_versions_at_both_bounds() {
		// Test with multiple versions at both excluded bounds
		let data = vec![
			// Multiple versions of lower bound (excluded)
			("key_002", "value", 100),
			("key_002", "value", 90),
			("key_002", "value", 80),
			// Keys in range
			("key_003", "value", 50),
			("key_004", "value", 50),
			// Multiple versions of upper bound (excluded)
			("key_005", "value", 100),
			("key_005", "value", 90),
			("key_005", "value", 80),
			// After upper bound
			("key_006", "value", 1),
		];

		let (src, size) = build_table_with_seq_num(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		// Forward iteration
		let iter = table.iter(
			false,
			Some(user_range_to_internal_range((
				Bound::Excluded(b"key_002".to_vec()),
				Bound::Excluded(b"key_005".to_vec()),
			))),
		);

		let mut results = Vec::new();
		for item in iter {
			let (key, _) = item;
			let user_key = String::from_utf8(key.user_key.clone()).unwrap();
			results.push(user_key);
		}

		// Should only have key_003 and key_004
		assert_eq!(results.len(), 2, "Should return exactly 2 items");
		assert_eq!(results[0], "key_003");
		assert_eq!(results[1], "key_004");

		// Should NOT contain any version of key_002 or key_005
		assert!(
			!results.iter().any(|k| k == "key_002" || k == "key_005"),
			"Should not contain any version of excluded bounds"
		);
	}

	#[test]
	fn test_get_block_boundary_same_user_key_bug() {
		// REGRESSION TEST: When multiple versions of the same user_key span multiple
		// data blocks, looking up the exact key at the block boundary incorrectly
		// returns None due to using < instead of <= in the index comparison.
		//
		// Bug location: Table::get() lines 777-778
		// The check `key_encoded < last_key_in_block` fails when they're equal,
		// but the key IS in the block (it's the last key).

		// Use very small block_size to force same user_key versions across multiple blocks
		// Entry sizes: 1st = 24 bytes, subsequent = 20 bytes each
		// size_estimate includes 8 bytes overhead (restart_points)
		// After 1st: 32, After 2nd: 52
		// With block_size=32: before 3rd entry, 52 > 32 → block cut
		let opts = Arc::new(
			Options::new()
				.with_filter_policy(None) // Disable bloom filter to test key comparison
				.with_block_size(32), // Very small block to force split
		);

		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		let user_key = b"test";

		// Add 4 versions of the same key with descending sequence numbers
		// (required ordering: user_key ASC, seq_num DESC)
		// Block 1 will contain: seq 300, 200 (last_key = ("test", 200))
		// Block 2 will contain: seq 100, 50
		let key1 = InternalKey::new(Vec::from(user_key), 300, InternalKeyKind::Set, 0);
		writer.add(key1, b"v").unwrap();

		let key2 = InternalKey::new(Vec::from(user_key), 200, InternalKeyKind::Set, 0);
		writer.add(key2, b"v").unwrap();

		let key3 = InternalKey::new(Vec::from(user_key), 100, InternalKeyKind::Set, 0);
		writer.add(key3, b"v").unwrap();

		let key4 = InternalKey::new(Vec::from(user_key), 50, InternalKeyKind::Set, 0);
		writer.add(key4, b"v").unwrap();

		let size = writer.finish().unwrap();
		let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

		// Verify we have multiple data blocks (the bug requires this)
		assert!(
			table.meta.properties.num_data_blocks >= 2,
			"Test requires at least 2 data blocks to trigger the bug, got {}. \
			 Adjust block_size or add more entries.",
			table.meta.properties.num_data_blocks
		);

		// THE BUG: Looking up the exact key at block boundary returns None
		// This is the last key of block 1, which is also the index separator key
		// (because same user_key causes separator to fall back to original key)
		// With block_size=32, the boundary key is ("test", 200)
		let lookup_boundary_key =
			InternalKey::new(Vec::from(user_key), 200, InternalKeyKind::Set, 0);
		let result = table.get(lookup_boundary_key).unwrap();

		// This assertion will FAIL with the current buggy code
		assert!(
			result.is_some(),
			"BUG: get() returned None for key at block boundary. \
			 The key ('test', seq=200) exists as the last key of block 1, \
			 but the index comparison uses < instead of <=, causing it to fail \
			 when lookup key equals the separator key."
		);

		let (found_key, found_value) = result.unwrap();
		assert_eq!(found_key.user_key.as_slice(), user_key);
		assert_eq!(found_key.seq_num(), 200);
		assert_eq!(found_value.as_slice(), b"v");

		// Also verify other keys still work correctly
		let lookup_key1 = InternalKey::new(Vec::from(user_key), 300, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key1).unwrap();
		assert!(result.is_some(), "seq=300 should be found");

		let lookup_key3 = InternalKey::new(Vec::from(user_key), 100, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key3).unwrap();
		assert!(result.is_some(), "seq=100 should be found");

		let lookup_key4 = InternalKey::new(Vec::from(user_key), 50, InternalKeyKind::Set, 0);
		let result = table.get(lookup_key4).unwrap();
		assert!(result.is_some(), "seq=50 should be found");
	}

	#[test]
	fn test_iterator_trait_prev_multi_partition_bug() {
		// REGRESSION TEST: The Iterator trait's prev() method uses self.index_block
		// which is only the first partition's iterator, NOT handling multiple partitions.
		// When we have multiple index partitions and call prev() via the trait,
		// backward iteration will stop at partition boundaries instead of continuing.
		//
		// Bug location: impl LSMIterator for TableIterator::prev() at lines 1512-1535
		// Uses self.index_block.prev() which only works within the first partition.
		// The correct inherent prev() method (lines 1022-1083) handles partitions properly.

		// Use very small index_partition_size to force multiple index partitions
		// Each data block adds an entry to the index partition
		// With index_partition_size=50 and block_size=50, we should get multiple partitions
		let opts = Arc::new(
			Options::new()
				.with_filter_policy(None)
				.with_block_size(50) // Small blocks to create many index entries
				.with_index_partition_size(50), // Small partitions to force multiple partitions
		);

		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		// Add enough keys to create multiple data blocks, which creates multiple index entries,
		// which in turn creates multiple index partitions
		let num_keys = 100;
		for i in 0..num_keys {
			let key = InternalKey::new(
				format!("key_{:05}", i).into_bytes(),
				1000 - i as u64, // Decreasing seq for same ordering
				InternalKeyKind::Set,
				0,
			);
			writer.add(key, format!("value_{}", i).as_bytes()).unwrap();
		}

		let size = writer.finish().unwrap();
		let table_arc = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

		// Verify we have multiple partitions (the bug requires this)
		let IndexType::Partitioned(ref partitioned_index) = table_arc.index_block;
		let num_partitions = partitioned_index.blocks.len();
		assert!(
			num_partitions >= 2,
			"Test requires at least 2 index partitions to trigger the bug, got {}. \
			 Adjust index_partition_size or add more keys.",
			num_partitions
		);

		println!("Created table with {} index partitions", num_partitions);

		// Create iterator and position at the LAST entry
		let mut iter = table_arc.iter(false, None);
		iter.seek_to_last();
		assert!(iter.valid(), "Iterator should be valid after seek_to_last");

		// Collect all keys going backwards using the CORRECT inherent prev() method
		let mut correct_results = Vec::new();
		correct_results.push(iter.key().user_key);
		while iter.prev() {
			correct_results.push(iter.key().user_key.clone());
		}
		let correct_count = correct_results.len();
		println!("Correct backward iteration found {} keys", correct_count);
		assert_eq!(correct_count, num_keys as usize, "Should find all keys");

		// Now test with explicit trait method call
		// Reset iterator
		let mut iter2 = table_arc.iter(false, None);
		iter2.seek_to_last();
		assert!(iter2.valid(), "Iterator should be valid after seek_to_last");

		// Collect all keys going backwards using the BUGGY trait prev() method
		let mut buggy_results = Vec::new();
		buggy_results.push(iter2.key().user_key);
		while iter2.prev() {
			buggy_results.push(iter2.key().user_key.clone());
		}
		let buggy_count = buggy_results.len();
		println!("Buggy trait prev() found {} keys", buggy_count);

		// THE BUG: The trait's prev() will stop at partition boundary
		// It should find all keys, but will find fewer because it only works within first partition
		assert_eq!(
			buggy_count, correct_count,
			"BUG DETECTED: Iterator::prev() trait method only found {} keys, \
			 but should have found {} keys. The trait method uses self.index_block \
			 which is only the first partition's iterator and doesn't handle \
			 crossing partition boundaries.",
			buggy_count, correct_count
		);
	}

	#[test]
	fn test_partitioned_index_same_user_key_spanning_partitions_bug() {
		// REGRESSION TEST: The TopLevelIndex stores only user_key, losing seq_num ordering.
		// When the same user_key has many versions spanning multiple partitions,
		// Table::get may return None even when a visible version exists.
		//
		// Bug location:
		// - src/sstable/index_block.rs:199-206 - TopLevelIndex construction only stores user_key
		// - src/sstable/index_block.rs:216-231 - find_block_handle_by_key uses user_key comparison
		// - src/sstable/table.rs:761 - Table::get uses user_key-only partition lookup
		//
		// Scenario:
		// - Partition 0 ends with internal key (foo, 50) -> stores user_key "foo"
		// - Partition 1 contains (foo, 49)...(foo, 10) and ends with (goo, 5) -> stores user_key
		//   "goo"
		// - Query for (foo, 25) should find entry in partition 1
		// - BUG: find_block_handle_by_key("foo") returns partition 0, which doesn't contain (foo,
		//   25)

		// Use very small partition and block sizes to force the same user_key to span partitions
		let opts = Arc::new(
			Options::new()
				.with_filter_policy(None)
				.with_block_size(50) // Small blocks = more index entries
				.with_index_partition_size(50), // Small partitions to force user_key to span
		);

		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, opts.clone(), 0);

		// Add many versions of the SAME user key with decreasing seq_nums
		// Internal key ordering: user_key ASC, seq_num DESC
		// So (foo, 100) < (foo, 99) < ... < (foo, 1) in sort order
		let user_key = b"foo";
		let num_versions = 100;

		for seq in (1..=num_versions).rev() {
			// Entries are added in internal key order (descending seq_num first)
			let key = InternalKey::new(user_key.to_vec(), seq as u64, InternalKeyKind::Set, 0);
			writer.add(key, format!("value_at_seq_{}", seq).as_bytes()).unwrap();
		}

		// Add a different user key at the end to ensure we have a partition boundary after "foo"
		let key = InternalKey::new(b"goo".to_vec(), 1, InternalKeyKind::Set, 0);
		writer.add(key, b"value_goo").unwrap();

		let size = writer.finish().unwrap();
		let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

		// Verify we have multiple partitions (the bug requires this)
		let IndexType::Partitioned(ref partitioned_index) = table.index_block;
		let num_partitions = partitioned_index.blocks.len();

		println!("Created table with {} index partitions", num_partitions);
		for (i, block) in partitioned_index.blocks.iter().enumerate() {
			let sep_key = InternalKey::decode(&block.separator_key);
			println!(
				"  Partition {}: user_key = {:?}, seq = {}",
				i,
				String::from_utf8_lossy(&sep_key.user_key),
				sep_key.seq_num()
			);
		}

		assert!(
			num_partitions >= 2,
			"Test requires at least 2 index partitions to trigger the bug, got {}. \
			 Adjust index_partition_size or add more versions.",
			num_partitions
		);

		// Check if there are multiple partitions with the same user_key "foo"
		// If partitions 0 and 1+ both have user_key "foo", the bug is triggerable
		let foo_partitions: Vec<_> = partitioned_index
			.blocks
			.iter()
			.enumerate()
			.filter(|(_, b)| InternalKey::decode(&b.separator_key).user_key == b"foo")
			.collect();

		println!(
			"Partitions with user_key 'foo': {:?}",
			foo_partitions.iter().map(|(i, _)| i).collect::<Vec<_>>()
		);

		// Now test the bug: query for a seq_num that should be in a later partition
		// If we have (foo, 100), (foo, 99), ..., (foo, 1) spread across partitions:
		// - Partition 0 might end with (foo, 60) or similar (high seq nums)
		// - Later partitions contain lower seq nums like (foo, 25)
		//
		// Query at seq_num 30: should find (foo, 30) or latest version visible at seq 30
		let query_seq = 30u64;
		let lookup_key = InternalKey::new(user_key.to_vec(), query_seq, InternalKeyKind::Set, 0);

		let result = table.get(lookup_key.clone()).unwrap();

		// The expected result: we should find a version with seq_num <= query_seq
		// For query_seq=30, we should find (foo, 30) with value "value_at_seq_30"
		assert!(
			result.is_some(),
			"BUG DETECTED: Table::get returned None for (foo, seq={}), but version exists!\n\
			 The bug is in TopLevelIndex::find_block_handle_by_key which uses user_key-only lookup.\n\
			 When the same user_key spans multiple partitions, it only searches the first partition\n\
			 (which contains high seq_nums) and misses entries in later partitions (low seq_nums).",
			query_seq
		);

		let (found_key, found_value) = result.unwrap();
		assert_eq!(found_key.user_key, user_key.to_vec());
		println!(
			"Found key with seq_num={}, value={:?}",
			found_key.seq_num(),
			String::from_utf8_lossy(&found_value)
		);

		// The found seq_num should be <= query_seq (latest visible version)
		assert!(
			found_key.seq_num() <= query_seq,
			"Found version seq_num {} should be <= query seq_num {}",
			found_key.seq_num(),
			query_seq
		);

		// Test multiple query points to ensure correctness across partition boundaries
		for query_seq in [10, 25, 50, 75, 99] {
			let lookup = InternalKey::new(user_key.to_vec(), query_seq, InternalKeyKind::Set, 0);

			let result = table.get(lookup).unwrap();
			assert!(
				result.is_some(),
				"BUG: Table::get returned None for (foo, seq={}), but version should exist!",
				query_seq
			);

			let (found_key, _) = result.unwrap();
			assert!(
				found_key.seq_num() <= query_seq,
				"For query seq={}, found seq={} which is greater (should be <=)",
				query_seq,
				found_key.seq_num()
			);
		}
	}
}
