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
use crate::sstable::index_block::{PartitionedIndexIterator, TopLevelIndex, TopLevelIndexWriter};
use crate::sstable::meta::TableMetadata;
use crate::sstable::{
	InternalIterator,
	InternalKey,
	InternalKeyKind,
	InternalKeyRef,
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

	pub(crate) fn decode(buf: &[u8]) -> Result<Footer> {
		let magic = &buf[buf.len() - TABLE_MAGIC_FOOTER_ENCODED.len()..];

		// Validate magic number first
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

		// Read format and checksum from footer (first 2 bytes)
		let format = TableFormat::from_u8(buf[0])?;
		let checksum = match buf[1] {
			1 => ChecksumType::CRC32c,
			_ => {
				return Err(Error::from(SSTableError::InvalidChecksumType {
					checksum_type: buf[1],
				}))
			}
		};

		// Read block handles (starting at offset 2)
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

	pub(crate) meta: TableMetadata, // Metadata properties of the table.

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
			internal_cmp: Arc::clone(&opts.internal_comparator) as Arc<dyn Comparator>,
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
		let contents = block.finish()?;
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

				// Track filter size
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
		let meta_block = meta_ix_block.finish()?;
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
	let mut metaindexiter = metaix.iter()?;
	metaindexiter.seek_internal(&meta_key)?;

	if metaindexiter.is_valid() {
		let k = metaindexiter.key();
		// Verify exact match to avoid using wrong entry
		assert_eq!(k.user_key(), b"meta");
		let buf_bytes = metaindexiter.value();
		return Ok(Some(TableMetadata::decode(buf_bytes)?));
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
		return Err(Error::from(SSTableError::ChecksumVerificationFailed {
			block_offset: location.offset() as u64,
		}));
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

	pub(crate) opts: Arc<Options>,  // Shared table options.
	pub(crate) meta: TableMetadata, // Metadata properties of the table.

	pub(crate) index_block: IndexType,
	pub(crate) filter_reader: Option<FilterBlockReader>,
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

		let mut metaindexiter = metaix.iter()?;
		metaindexiter.seek_internal(&filter_key.encode())?;

		if metaindexiter.is_valid() {
			let k = metaindexiter.key();

			// Verify exact match to avoid using wrong entry
			assert_eq!(k.user_key(), filter_name.as_bytes());
			let val = metaindexiter.value();

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

	pub(crate) fn read_block(&self, location: &BlockHandle) -> Result<Arc<Block>> {
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

	pub(crate) fn get(&self, key: &InternalKey) -> Result<Option<(InternalKey, Value)>> {
		let key_encoded = key.encode();

		// Check bloom filter first for early rejection
		if let Some(ref filters) = self.filter_reader {
			if !filters.may_contain(key.user_key.as_slice(), 0) {
				return Ok(None);
			}
		}

		let IndexType::Partitioned(partitioned_index) = &self.index_block;

		// Find partition that could contain this key
		let Some((_, partition_handle)) =
			partitioned_index.find_block_handle_by_key(&key_encoded)?
		else {
			return Ok(None);
		};

		// Load partition block and seek to find data block handle
		let partition_block = partitioned_index.load_block(partition_handle)?;
		let mut partition_iter = partition_block.iter()?;
		partition_iter.seek_internal(&key_encoded)?;

		if !partition_iter.is_valid() {
			return Ok(None);
		}

		// Decode data block handle
		let (data_handle, _) = BlockHandle::decode(partition_iter.value_bytes()).map_err(|e| {
			Error::from(SSTableError::FailedToDecodeBlockHandle {
				value_bytes: partition_iter.value_bytes().to_vec(),
				context: format!("Failed to decode BlockHandle in get(): {e}"),
			})
		})?;

		// Read data block and search for key
		let data_block = self.read_block(&data_handle)?;
		let mut iter = data_block.iter()?;
		iter.seek_internal(&key_encoded)?;

		if iter.is_valid() && iter.user_key() == key.user_key.as_slice() {
			Ok(Some((iter.key().to_owned(), iter.value().to_vec())))
		} else {
			Ok(None)
		}
	}

	pub(crate) fn iter(&self, range: Option<InternalKeyRange>) -> Result<TwoLevelIterator<'_>> {
		let range = range.unwrap_or((Bound::Unbounded, Bound::Unbounded));
		TwoLevelIterator::new(self, range)
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

	/// Checks if table is completely before the query range
	pub(crate) fn is_before_range(&self, range: &InternalKeyRange) -> bool {
		let Some(largest) = &self.meta.largest_point else {
			return false;
		};

		match &range.0 {
			// lower bound
			Bound::Unbounded => false,
			Bound::Included(k) => {
				self.opts.comparator.compare(&largest.user_key, &k.user_key) == Ordering::Less
			}
			Bound::Excluded(k) => {
				// Range starts AFTER k, so table is before if table.largest <= k
				self.opts.comparator.compare(&largest.user_key, &k.user_key) != Ordering::Greater
			}
		}
	}

	/// Checks if table is completely after the query range
	pub(crate) fn is_after_range(&self, range: &InternalKeyRange) -> bool {
		let Some(smallest) = &self.meta.smallest_point else {
			return false;
		};

		match &range.1 {
			// upper bound
			Bound::Unbounded => false,
			Bound::Included(k) => {
				self.opts.comparator.compare(&smallest.user_key, &k.user_key) == Ordering::Greater
			}
			Bound::Excluded(k) => {
				// Range ends BEFORE k, so table is after if table.smallest >= k
				self.opts.comparator.compare(&smallest.user_key, &k.user_key) != Ordering::Less
			}
		}
	}

	/// Checks if table overlaps with query range
	pub(crate) fn overlaps_with_range(&self, range: &InternalKeyRange) -> bool {
		!self.is_before_range(range) && !self.is_after_range(range)
	}
}

/// Two-level iterator for SSTable with built-in bounds support.
///
/// This follows RocksDB's TwoLevelIterator pattern but with bounds checking
/// built-in.
///
/// - first_level: PartitionedIndexIterator (navigates partition index entries)
/// - second_level: BlockIterator (navigates data block entries)
pub(crate) struct TwoLevelIterator<'a> {
	table: &'a Table,
	/// First level: iterates over partition index entries
	first_level: PartitionedIndexIterator<'a>,
	/// Second level: iterates over data block entries
	second_level: Option<BlockIterator>,
	/// Range bounds for filtering
	range: InternalKeyRange,
	/// Whether the iterator has been exhausted
	exhausted: bool,
}

impl<'a> TwoLevelIterator<'a> {
	/// Create a new TwoLevelIterator over the given table with the specified range.
	pub(crate) fn new(table: &'a Table, range: InternalKeyRange) -> Result<Self> {
		let IndexType::Partitioned(ref partitioned_index) = table.index_block;

		Ok(Self {
			table,
			first_level: PartitionedIndexIterator::new(partitioned_index),
			second_level: None,
			range,
			exhausted: false,
		})
	}

	/// Check if the iterator is positioned on a valid entry
	fn is_valid(&self) -> bool {
		!self.exhausted && self.second_level.as_ref().map_or(false, |iter| iter.is_valid())
	}

	/// Mark the iterator as exhausted
	fn mark_exhausted(&mut self) {
		self.exhausted = true;
		self.second_level = None;
	}

	/// Initialize the data block from the current first-level position.
	fn init_data_block(&mut self) -> Result<()> {
		if !self.first_level.valid() {
			self.second_level = None;
			return Ok(());
		}

		let handle = self.first_level.block_handle()?;

		// Load the new data block
		let block = self.table.read_block(&handle)?;
		let iter = block.iter()?;
		self.second_level = Some(iter);
		Ok(())
	}

	/// Skip empty data blocks forward until we find a valid entry or exhaust.
	fn skip_empty_data_blocks_forward(&mut self) -> Result<()> {
		loop {
			// Check if current second_level is valid
			if self.second_level.as_ref().map_or(false, |iter| iter.is_valid()) {
				return Ok(());
			}

			// Need to move to next block
			if !self.first_level.valid() {
				self.second_level = None;
				return Ok(());
			}

			self.first_level.next()?;
			self.init_data_block()?;

			if let Some(ref mut iter) = self.second_level {
				iter.seek_to_first()?;
			}
		}
	}

	/// Skip empty data blocks backward until we find a valid entry or exhaust.
	fn skip_empty_data_blocks_backward(&mut self) -> Result<()> {
		loop {
			// Check if current second_level is valid
			if self.second_level.as_ref().map_or(false, |iter| iter.is_valid()) {
				return Ok(());
			}

			// Need to move to previous block
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

	/// Check if a user key satisfies the lower bound constraint
	fn satisfies_lower_bound(&self, user_key: &[u8]) -> bool {
		match &self.range.0 {
			Bound::Included(start) => {
				self.table.opts.comparator.compare(user_key, &start.user_key) != Ordering::Less
			}
			Bound::Excluded(start) => {
				self.table.opts.comparator.compare(user_key, &start.user_key) == Ordering::Greater
			}
			Bound::Unbounded => true,
		}
	}

	/// Check if a user key satisfies the upper bound constraint
	fn satisfies_upper_bound(&self, user_key: &[u8]) -> bool {
		match &self.range.1 {
			Bound::Included(end) => {
				self.table.opts.comparator.compare(user_key, &end.user_key) != Ordering::Greater
			}
			Bound::Excluded(end) => {
				self.table.opts.comparator.compare(user_key, &end.user_key) == Ordering::Less
			}
			Bound::Unbounded => true,
		}
	}

	/// Get the current user key (requires valid iterator)
	fn current_user_key(&self) -> &[u8] {
		self.second_level.as_ref().unwrap().user_key()
	}

	/// Seek to first entry within bounds
	pub(crate) fn seek_to_first(&mut self) -> Result<()> {
		self.exhausted = false;

		// Clone bound to avoid borrow conflict
		let lower_bound = self.range.0.clone();

		match lower_bound {
			Bound::Unbounded => {
				// Raw seek to first
				self.first_level.seek_to_first()?;
				self.init_data_block()?;
				if let Some(ref mut iter) = self.second_level {
					iter.seek_to_first()?;
				}
				self.skip_empty_data_blocks_forward()?;
			}
			Bound::Included(ref internal_key) => {
				// Seek to the lower bound key
				self.seek_internal(&internal_key.encode())?;
			}
			Bound::Excluded(ref internal_key) => {
				// Seek to (user_key, seq_num=0) which sorts AFTER all versions
				// because keys sort by (user_key ASC, seq_num DESC)
				let seek_key = InternalKey::new(
					internal_key.user_key.clone(),
					0, // min seq_num sorts last for this user_key
					InternalKeyKind::Set,
					0,
				);
				self.seek_internal(&seek_key.encode())?;

				// If we landed on the excluded key, advance once
				if self.is_valid() && self.current_user_key() == internal_key.user_key.as_slice() {
					self.advance_internal()?;
				}
			}
		}

		// Verify within upper bound
		if self.is_valid() && !self.satisfies_upper_bound(self.current_user_key()) {
			self.mark_exhausted();
		}
		Ok(())
	}

	/// Seek to last entry within bounds
	pub(crate) fn seek_to_last(&mut self) -> Result<()> {
		self.exhausted = false;

		// Clone bound to avoid borrow conflict
		let upper_bound = self.range.1.clone();

		match upper_bound {
			Bound::Unbounded => {
				// Raw seek to last
				self.first_level.seek_to_last()?;
				self.init_data_block()?;
				if let Some(ref mut iter) = self.second_level {
					iter.seek_to_last()?;
				}
				self.skip_empty_data_blocks_backward()?;
			}
			Bound::Included(ref internal_key) => {
				// SeekForPrev semantics: find last entry <= bound
				let bound_user_key = internal_key.user_key.clone();
				let seek_key = InternalKey::new(
					internal_key.user_key.clone(),
					INTERNAL_KEY_SEQ_NUM_MAX,
					InternalKeyKind::Max,
					INTERNAL_KEY_TIMESTAMP_MAX,
				);
				self.seek_internal(&seek_key.encode())?;

				if !self.is_valid() {
					// Went past end, position at absolute last
					self.position_to_absolute_last()?;
				} else {
					// Check if we're past the bound
					let cmp = self
						.table
						.opts
						.comparator
						.compare(self.current_user_key(), bound_user_key.as_slice());
					if cmp == Ordering::Greater {
						self.prev_internal()?;
					}
				}
			}
			Bound::Excluded(ref internal_key) => {
				// SeekForPrev for excluded: find last entry < bound
				let bound_user_key = internal_key.user_key.clone();
				let seek_key = InternalKey::new(
					internal_key.user_key.clone(),
					INTERNAL_KEY_SEQ_NUM_MAX,
					InternalKeyKind::Max,
					INTERNAL_KEY_TIMESTAMP_MAX,
				);
				self.seek_internal(&seek_key.encode())?;

				if !self.is_valid() {
					self.position_to_absolute_last()?;
				}

				// For excluded: if at or past bound, back up
				if self.is_valid() {
					let cmp = self
						.table
						.opts
						.comparator
						.compare(self.current_user_key(), bound_user_key.as_slice());
					if cmp != Ordering::Less {
						self.prev_internal()?;
					}
				}
			}
		}

		// Verify within lower bound
		if self.is_valid() && !self.satisfies_lower_bound(self.current_user_key()) {
			self.mark_exhausted();
		}
		Ok(())
	}

	/// Internal seek to target key
	fn seek_internal(&mut self, target: &[u8]) -> Result<()> {
		self.first_level.seek(target)?;
		self.init_data_block()?;

		if let Some(ref mut iter) = self.second_level {
			iter.seek_internal(target)?;
		}

		self.skip_empty_data_blocks_forward()?;
		Ok(())
	}

	/// Position to absolute last entry (for SeekForPrev fallback)
	fn position_to_absolute_last(&mut self) -> Result<()> {
		self.first_level.seek_to_last()?;
		self.init_data_block()?;

		if let Some(ref mut iter) = self.second_level {
			iter.seek_to_last()?;
		}

		self.skip_empty_data_blocks_backward()?;
		Ok(())
	}

	/// Advance to next entry (internal, no bounds check)
	fn advance_internal(&mut self) -> Result<bool> {
		if let Some(ref mut iter) = self.second_level {
			if iter.advance()? {
				return Ok(true);
			}
		}
		self.skip_empty_data_blocks_forward()?;
		Ok(self.is_valid())
	}

	/// Move to previous entry (internal, no bounds check)
	fn prev_internal(&mut self) -> Result<bool> {
		if let Some(ref mut iter) = self.second_level {
			if iter.prev_internal()? {
				return Ok(true);
			}
		}
		self.skip_empty_data_blocks_backward()?;
		Ok(self.is_valid())
	}
}

impl InternalIterator for TwoLevelIterator<'_> {
	/// Seek to first entry >= target within bounds
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.exhausted = false;
		self.seek_internal(target)?;

		// Check upper bound
		if self.is_valid() && !self.satisfies_upper_bound(self.current_user_key()) {
			self.mark_exhausted();
		}
		Ok(self.is_valid())
	}

	/// Seek to first entry within bounds
	fn seek_first(&mut self) -> Result<bool> {
		self.seek_to_first()?;
		Ok(self.is_valid())
	}

	/// Seek to last entry within bounds
	fn seek_last(&mut self) -> Result<bool> {
		self.seek_to_last()?;
		Ok(self.is_valid())
	}

	/// Move to next entry within bounds
	fn next(&mut self) -> Result<bool> {
		// If not positioned yet and not exhausted, seek to first entry
		// This matches the old TableIterator behavior where next() on a fresh
		// iterator positions on the first item
		if !self.is_valid() && !self.exhausted {
			return self.seek_first();
		}

		if !self.is_valid() {
			return Ok(false);
		}

		self.advance_internal()?;

		// Check upper bound or mark exhausted if no more entries
		if !self.is_valid() {
			self.mark_exhausted();
		} else if !self.satisfies_upper_bound(self.current_user_key()) {
			self.mark_exhausted();
		}
		Ok(self.is_valid())
	}

	/// Move to previous entry within bounds
	fn prev(&mut self) -> Result<bool> {
		// If not positioned yet and not exhausted, seek to last entry
		// This matches the old TableIterator behavior
		if !self.is_valid() && !self.exhausted {
			return self.seek_last();
		}

		if !self.is_valid() {
			return Ok(false);
		}

		self.prev_internal()?;

		// Check lower bound or mark exhausted if no more entries
		if !self.is_valid() {
			self.mark_exhausted();
		} else if !self.satisfies_lower_bound(self.current_user_key()) {
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

	fn value(&self) -> &[u8] {
		debug_assert!(self.valid());
		self.second_level.as_ref().unwrap().value_bytes()
	}
}
