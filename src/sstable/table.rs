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
	let mut metaindexiter = metaix.iter(false)?;
	metaindexiter.seek(&meta_key)?;

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

		let mut metaindexiter = metaix.iter(false)?;
		metaindexiter.seek(&filter_key.encode())?;

		if metaindexiter.valid() {
			let k = metaindexiter.key();

			// Verify exact match to avoid using wrong entry
			assert_eq!(k.user_key.as_slice(), filter_name.as_bytes());
			let val = metaindexiter.value();

			let fbl = BlockHandle::decode(&val);
			let filter_block_location = match fbl {
				Err(e) => {
					return Err(Error::from(SSTableError::FailedToDecodeBlockHandle {
						value_bytes: val,
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

	pub(crate) fn get(&self, key: &InternalKey) -> Result<Option<(InternalKey, Value)>> {
		// CALL ONCE and store
		let key_encoded = key.encode();

		// Check filter first
		if let Some(ref filters) = self.filter_reader {
			let may_contain = filters.may_contain(key.user_key.as_slice(), 0);
			if !may_contain {
				return Ok(None);
			}
		}

		let handle = match &self.index_block {
			IndexType::Partitioned(partitioned_index) => {
				// Use internal key comparison to find the partition block
				let partition_block = match partitioned_index.get(&key_encoded) {
					Ok(block) => block,
					Err(e) => {
						// Treat "not found" cases as key not found:
						// - BlockNotFound: key is beyond all partitions
						// All other errors (I/O, corruption) should be propagated
						match &e {
							Error::BlockNotFound => return Ok(None),
							_ => return Err(e),
						}
					}
				};

				// Then search within the partition
				// Note: Index blocks always need full key-value pairs to decode block handles
				let mut partition_iter = partition_block.iter(false)?;
				partition_iter.seek(&key_encoded)?;

				if partition_iter.valid() {
					if self.internal_cmp.compare(&key_encoded, partition_iter.key_bytes())
						!= Ordering::Greater
					{
						let val = partition_iter.value_bytes();
						match BlockHandle::decode(val) {
							Ok((handle, _)) => Some(handle),
							Err(e) => {
								let err = Error::from(SSTableError::FailedToDecodeBlockHandle {
									value_bytes: val.to_vec(),
									context: format!(
										"Failed to decode BlockHandle in get(): {}",
										e
									),
								});
								log::error!("[TABLE] {}", err);
								return Err(err);
							}
						}
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
		let mut iter = tb.iter(false)?;

		// Go to entry and check if it's the wanted entry.
		iter.seek(&key_encoded)?;
		if iter.valid() {
			if iter.user_key() == key.user_key.as_slice() {
				Ok(Some((iter.key(), iter.value())))
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
			Bound::Included(k) | Bound::Excluded(k) => {
				self.opts.comparator.compare(&largest.user_key, &k.user_key) == Ordering::Less
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
			Bound::Included(k) | Bound::Excluded(k) => {
				self.opts.comparator.compare(&smallest.user_key, &k.user_key) == Ordering::Greater
			}
		}
	}

	/// Checks if table overlaps with query range
	pub(crate) fn overlaps_with_range(&self, range: &InternalKeyRange) -> bool {
		!self.is_before_range(range) && !self.is_after_range(range)
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
			if partition_iter.advance()? {
				let val = partition_iter.value();
				let (handle, _) = match BlockHandle::decode(&val) {
					Err(e) => {
						return Err(Error::from(SSTableError::FailedToDecodeBlockHandle {
							value_bytes: val,
							context: format!("error: {:?}", e),
						}));
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
		let mut partition_iter = partition_block.iter(false)?;
		partition_iter.seek_to_first()?;

		if partition_iter.valid() {
			let val = partition_iter.value();
			let (handle, _) = match BlockHandle::decode(&val) {
				Err(e) => {
					return Err(Error::from(SSTableError::FailedToDecodeBlockHandle {
						value_bytes: val,
						context: format!("error: {:?}", e),
					}));
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
		let mut block_iter = block.iter(self.keys_only)?;

		// Position at first entry in the new block
		block_iter.seek_to_first()?;

		if block_iter.valid() {
			self.current_block = Some(block_iter);
			self.current_block_off = handle.offset();
			return Ok(());
		}

		Err(Error::from(SSTableError::EmptyBlock))
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

	pub(crate) fn prev(&mut self) -> Result<bool> {
		if let Some(ref mut block) = self.current_block {
			if block.prev()? {
				return Ok(true);
			}
		}

		// Current block is exhausted, try to move to previous block

		// Try to move to previous block within current partition
		if let Some(ref mut partition_iter) = self.current_partition_iter {
			if partition_iter.prev()? {
				let val = partition_iter.value();
				let (handle, _) = match BlockHandle::decode(&val) {
					Err(e) => {
						let err = Error::from(SSTableError::FailedToDecodeBlockHandle {
							value_bytes: val,
							context: format!("Block corruption in prev(): failed to decode BlockHandle. Error: {:?}", e),
						});
						log::error!("[TABLE_ITER] {}", err);
						return Err(err);
					}
					Ok(res) => res,
				};
				self.load_block(&handle)?;
				if let Some(ref mut block_iter) = self.current_block {
					block_iter.seek_to_last()?;
					return Ok(block_iter.valid());
				}
			}
		}

		// Current partition exhausted, move to previous partition
		if self.current_partition_index > 0 {
			// Get the partitioned index
			let IndexType::Partitioned(partitioned_index) = &self.table.index_block;

			self.current_partition_index -= 1;
			let partition_handle = &partitioned_index.blocks[self.current_partition_index];

			let partition_block = partitioned_index.load_block(partition_handle)?;
			// Note: Index blocks always need full key-value pairs to decode block handles
			let mut partition_iter = partition_block.iter(false)?;
			partition_iter.seek_to_last()?;

			if partition_iter.valid() {
				let val = partition_iter.value();
				let (handle, _) = match BlockHandle::decode(&val) {
					Err(e) => {
						let err = Error::from(SSTableError::FailedToDecodeBlockHandle {
							value_bytes: val,
							context: format!("Block corruption in prev(): failed to decode BlockHandle in partition {}. Error: {:?}", self.current_partition_index, e),
						});
						log::error!("[TABLE_ITER] {}", err);
						return Err(err);
					}
					Ok(res) => res,
				};
				self.current_partition_iter = Some(partition_iter);
				self.load_block(&handle)?;
				if let Some(ref mut block_iter) = self.current_block {
					block_iter.seek_to_last()?;
					return Ok(block_iter.valid());
				}
			}
		}

		self.current_block = None;
		Ok(false)
	}

	fn seek_to_lower_bound(&mut self, bound: &Bound<InternalKey>) -> Result<()> {
		match bound {
			Bound::Included(internal_key) => {
				self.seek(&internal_key.encode())?;
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
				self.seek(&seek_key.encode())?;

				// Skip ALL versions of the excluded key
				while self.valid() {
					let current_key = self.current_block.as_ref().unwrap().key();
					if current_key.user_key != internal_key.user_key {
						break;
					}
					if !self.advance()? {
						break;
					}
				}
			}
			Bound::Unbounded => {
				self.seek_to_first()?;
			}
		}
		Ok(())
	}

	fn seek_to_upper_bound(&mut self, bound: &Bound<InternalKey>) -> Result<()> {
		match bound {
			Bound::Included(internal_key) => {
				// Seek to find the first key >= upper bound
				let seek_key = InternalKey::new(
					internal_key.user_key.clone(),
					INTERNAL_KEY_SEQ_NUM_MAX,
					InternalKeyKind::Max,
					INTERNAL_KEY_TIMESTAMP_MAX,
				);
				self.seek(&seek_key.encode())?;

				// If seek went past table end (invalid), start from last key
				if !self.valid() {
					self.seek_to_last()?;
					return Ok(());
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
					if !self.prev()? {
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
				self.seek(&seek_key.encode())?;

				// If seek went past table end, start from last key
				if !self.valid() {
					self.seek_to_last()?;
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
					if !self.prev()? {
						break;
					}
				}
			}
			Bound::Unbounded => {
				self.seek_to_last()?;
			}
		}
		Ok(())
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
}

impl Iterator for TableIterator {
	type Item = Result<(InternalKey, Value)>;

	fn next(&mut self) -> Option<Self::Item> {
		// If not positioned, position appropriately based on range
		if !self.positioned {
			let lower_bound = self.range.0.clone();
			match self.seek_to_lower_bound(&lower_bound) {
				Ok(()) => {
					self.positioned = true;
				}
				Err(e) => {
					log::error!("[TABLE_ITER] Error in seek_to_lower_bound(): {}", e);
					return Some(Err(e));
				}
			}
		}

		// If not valid, return None
		if !self.valid() {
			return None;
		}

		let block = self.current_block.as_ref().unwrap();
		if !self.satisfies_upper_bound(block.user_key()) {
			self.mark_exhausted();
			return None;
		}

		let current_item = (block.key(), block.value());

		// Advance for the next call to next()
		match self.advance() {
			Ok(_) => Some(Ok(current_item)),
			Err(e) => {
				log::error!("[TABLE_ITER] Error in advance(): {}", e);
				Some(Err(e))
			}
		}
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
					match self.seek_to_upper_bound(&upper_bound) {
						Ok(()) => {
							self.positioned = true;
							self.reverse_started = false;
						}
						Err(e) => {
							log::error!("[TABLE_ITER] Error in seek_to_upper_bound(): {}", e);
							return Some(Err(e));
						}
					}
				}
				Bound::Unbounded => match self.seek_to_last() {
					Ok(()) => {
						self.positioned = true;
					}
					Err(e) => {
						log::error!("[TABLE_ITER] Error in seek_to_last(): {}", e);
						return Some(Err(e));
					}
				},
			}
		}

		// If positioned but not yet started reverse iteration, return current position
		// first
		if self.positioned && !self.reverse_started && self.valid() {
			let block = self.current_block.as_ref().unwrap();
			if !self.satisfies_lower_bound(block.user_key()) {
				return None;
			}

			let item = (block.key(), block.value());
			self.reverse_started = true;
			return Some(Ok(item));
		}

		match self.prev() {
			Ok(true) => {
				let block = self.current_block.as_ref().unwrap();
				if !self.satisfies_lower_bound(block.user_key()) {
					return None;
				}

				let item = (block.key(), block.value());
				Some(Ok(item))
			}
			Ok(false) => None,
			Err(e) => {
				log::error!("[TABLE_ITER] Error in prev(): {}", e);
				Some(Err(e))
			}
		}
	}
}

impl TableIterator {
	pub(crate) fn valid(&self) -> bool {
		!self.exhausted
			&& self.current_block.is_some()
			&& self.current_block.as_ref().unwrap().valid()
	}

	pub(crate) fn seek_to_first(&mut self) -> Result<()> {
		self.reset_partitioned_state();

		// Get the partitioned index
		let IndexType::Partitioned(partitioned_index) = &self.table.index_block;

		if !partitioned_index.blocks.is_empty() {
			let partition_handle = &partitioned_index.blocks[0];
			let partition_block = partitioned_index.load_block(partition_handle)?;
			// Note: Index blocks always need full key-value pairs to decode block handles
			let mut partition_iter = partition_block.iter(false)?;
			partition_iter.seek_to_first()?;

			if partition_iter.valid() {
				let val = partition_iter.value();
				let (handle, _) = match BlockHandle::decode(&val) {
					Err(e) => {
						let err = Error::from(SSTableError::FailedToDecodeBlockHandle {
							value_bytes: val,
							context: format!(
								"Failed to decode BlockHandle in seek_to_first(): {}",
								e
							),
						});
						log::error!("[TABLE_ITER] {}", err);
						return Err(err);
					}
					Ok(res) => res,
				};
				self.current_partition_iter = Some(partition_iter);
				self.load_block(&handle)?;
				self.positioned = true;
				self.exhausted = false;
				return Ok(());
			}
		}

		// If we get here, initialization failed
		self.reset();
		Err(Error::from(SSTableError::NoValidBlocksForSeek {
			operation: "first".to_string(),
		}))
	}

	pub(crate) fn seek_to_last(&mut self) -> Result<()> {
		let IndexType::Partitioned(partitioned_index) = &self.table.index_block;

		// For partitioned index, go to the last partition
		if !partitioned_index.blocks.is_empty() {
			let last_partition_index = partitioned_index.blocks.len() - 1;
			let last_partition_handle = &partitioned_index.blocks[last_partition_index];

			let last_partition_block = partitioned_index.load_block(last_partition_handle)?;
			// Note: Index blocks always need full key-value pairs to decode block handles
			let mut partition_iter = last_partition_block.iter(false)?;
			partition_iter.seek_to_last()?;

			if partition_iter.valid() {
				let val = partition_iter.value();
				let (handle, _) = match BlockHandle::decode(&val) {
					Err(e) => {
						let err = Error::from(SSTableError::FailedToDecodeBlockHandle {
							value_bytes: val,
							context: format!(
								"Failed to decode BlockHandle in seek_to_last(): {}",
								e
							),
						});
						log::error!("[TABLE_ITER] {}", err);
						self.reset();
						return Err(err);
					}
					Ok(res) => res,
				};
				self.current_partition_index = last_partition_index;
				self.current_partition_iter = Some(partition_iter);
				self.load_block(&handle)?;
				if let Some(ref mut block_iter) = self.current_block {
					block_iter.seek_to_last()?;
					self.positioned = true;
					self.exhausted = false;
					self.reverse_started = false;
					return Ok(());
				}
			}
		}

		self.reset();
		Err(Error::from(SSTableError::NoValidBlocksForSeek {
			operation: "last".to_string(),
		}))
	}

	pub(crate) fn seek(&mut self, target: &[u8]) -> Result<Option<()>> {
		let IndexType::Partitioned(partitioned_index) = &self.table.index_block;

		// For partitioned index, use full internal key for correct partition lookup
		match partitioned_index.find_block_handle_by_key(target) {
			Ok(Some((partition_index, block_handle))) => {
				self.current_partition_index = partition_index;

				let partition_block = partitioned_index.load_block(block_handle)?;
				// Note: Index blocks always need full key-value pairs to decode block handles
				let mut partition_iter = partition_block.iter(false)?;
				partition_iter.seek(target)?;

				if partition_iter.valid() {
					let v = partition_iter.value();
					let (handle, _) = match BlockHandle::decode(&v) {
						Err(e) => {
							let err = Error::from(SSTableError::FailedToDecodeBlockHandle {
								value_bytes: v,
								context: format!("Failed to decode BlockHandle in seek(): {}", e),
							});
							log::error!("[TABLE_ITER] {}", err);
							self.positioned = true;
							self.current_block = None;
							return Err(err);
						}
						Ok(res) => res,
					};
					self.current_partition_iter = Some(partition_iter);
					self.load_block(&handle)?;
					if let Some(ref mut block_iter) = self.current_block {
						block_iter.seek(target)?;
						self.positioned = true;
						self.exhausted = false;

						// If not valid in current block, the key might be in next block
						if !block_iter.valid() {
							// Try to advance to next block
							match self.skip_to_next_entry() {
								Ok(true) => {
									// Successfully loaded next block and positioned at first entry
									return Ok(Some(()));
								}
								Ok(false) => {
									// No more blocks - mark as positioned but invalid
									self.positioned = true;
									self.current_block = None;
									return Ok(Some(()));
								}
								Err(e) => {
									log::error!(
										"[TABLE_ITER] Error in skip_to_next_entry(): {}",
										e
									);
									// Error - mark as positioned but invalid
									self.positioned = true;
									self.current_block = None;
									return Err(e);
								}
							}
						}

						return Ok(Some(()));
					}
				}
			}
			Ok(None) => {
				// Key not found in any partition
				self.positioned = true;
				self.current_block = None;
				return Ok(Some(()));
			}
			Err(e) => {
				log::error!("[TABLE_ITER] Error finding block handle: {}", e);
				return Err(e);
			}
		}

		// If we can't position in any block, mark as positioned but invalid
		self.positioned = true;
		self.current_block = None;
		Ok(Some(()))
	}

	pub(crate) fn advance(&mut self) -> Result<bool> {
		// If exhausted, stay exhausted
		if self.exhausted {
			return Ok(false);
		}

		// If not positioned, position at first entry
		if !self.positioned {
			self.seek_to_first()?;
			return Ok(self.valid());
		}

		// Try to advance within the current block first
		if let Some(ref mut block) = self.current_block {
			if block.advance()? {
				// Successfully advanced within current block
				return Ok(true);
			}
		}

		// Current block is exhausted, try to move to next block
		match self.skip_to_next_entry() {
			Ok(true) => {
				// Successfully loaded next block and positioned at first entry
				Ok(true)
			}
			Ok(false) => {
				// No more blocks available - mark as exhausted
				self.mark_exhausted();
				Ok(false)
			}
			Err(e) => {
				log::error!("[TABLE_ITER] Error in advance(): {}", e);
				// Error loading next block - mark as exhausted
				self.mark_exhausted();
				Err(e)
			}
		}
	}
}
