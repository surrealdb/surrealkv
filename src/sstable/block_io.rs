//! # Raw block reads of one SSTable
//!
//! `BlockSource` owns the file of a table and every read of its bytes: the footer, and each
//! block named by a handle. The index and the filter readers hold a source instead of the file,
//! so there is one place that knows how a block sits on disk and what is checked before its
//! bytes are used.
//!
//! ## One request per block
//!
//! A block is followed by a trailer that holds its compression type and a checksum, and the two
//! are read together with it, in one request:
//!
//! ```text
//! +----------------------+------------------+-----------------+
//! | block (handle.size)  | compression (1)  | masked CRC (4)  |
//! +----------------------+------------------+-----------------+
//! ```
//!
//! ## What is checked before bytes are used
//!
//! - The handle must lie inside the part of the file that holds blocks, which ends where the footer
//!   begins. This is checked before the buffer is allocated, so a handle that was damaged cannot
//!   ask for gigabytes.
//! - The read must return every byte that was asked for. A file may return fewer bytes than the
//!   buffer holds without being at its end, and a file that ends early is an error rather than a
//!   buffer with a tail of zeros.
//! - The checksum must match before the block is decompressed or decoded.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::sstable::block::{Block, BlockHandle};
use crate::sstable::error::SSTableError;
use crate::sstable::table::{
	decompress_block,
	unmask,
	Footer,
	BLOCK_CKSUM_LEN,
	BLOCK_COMPRESS_LEN,
	TABLE_FULL_FOOTER_LENGTH,
};
use crate::storage::{AffinityObjectStore, ObjectStore};
use crate::vfs::{short_read, File};
use crate::{Comparator, CompressionType};

/// The bytes after a block: its compression type and the masked CRC32.
pub(crate) const BLOCK_TRAILER_LEN: usize = BLOCK_COMPRESS_LEN + BLOCK_CKSUM_LEN;

/// Reads the footer and the blocks of one SSTable file.
///
/// Cheap to clone: the clones read the same file.
#[derive(Clone)]
pub(crate) struct BlockSource {
	file: Arc<dyn File>,
	/// The same file for the asynchronous reads of data blocks.
	store: Arc<dyn ObjectStore>,
	file_size: u64,
}

impl BlockSource {
	/// A source over `file`, which holds `file_size` bytes. The footer is not read.
	pub(crate) fn new(file: Arc<dyn File>, file_size: u64) -> Self {
		let store: Arc<dyn ObjectStore> = Arc::new(AffinityObjectStore::new(Arc::clone(&file)));
		Self {
			file,
			store,
			file_size,
		}
	}

	/// Opens a table file: reads its footer and returns it with the source that reads its
	/// blocks, for both the synchronous and the asynchronous paths.
	pub(crate) fn open(file: Arc<dyn File>, file_size: u64) -> Result<(Self, Footer)> {
		let source = Self::new(file, file_size);
		let footer = source.read_footer()?;
		Ok((source, footer))
	}

	fn read_footer(&self) -> Result<Footer> {
		let footer_len = TABLE_FULL_FOOTER_LENGTH as u64;
		if self.file_size < footer_len {
			return Err(Error::from(SSTableError::FileTooSmall {
				file_size: self.file_size as usize,
				min_size: TABLE_FULL_FOOTER_LENGTH,
			}));
		}
		let mut buf = [0u8; TABLE_FULL_FOOTER_LENGTH];
		self.file.read_exact_at(self.file_size - footer_len, &mut buf)?;
		Footer::decode(&buf)
	}

	/// The length of the block at `handle` with its trailer, if all of it lies in the part of
	/// the file that holds blocks. `trailer_len` is what follows the block.
	fn extent(&self, handle: &BlockHandle, trailer_len: usize) -> Result<usize> {
		let limit = self.file_size.saturating_sub(TABLE_FULL_FOOTER_LENGTH as u64);
		let (offset, size) = (handle.offset() as u64, handle.size() as u64);
		let fits = size
			.checked_add(trailer_len as u64)
			.and_then(|len| len.checked_add(offset))
			.is_some_and(|end| end <= limit);
		if !fits {
			return Err(Error::from(SSTableError::BlockHandleOutOfBounds {
				block_offset: offset,
				block_size: size,
				limit,
			}));
		}
		// The sum is at most `limit`, so it fits where the handle's own size did.
		Ok(handle.size() + trailer_len)
	}

	/// Reads the block at `handle` with its trailer, in one request.
	fn fetch(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
		let mut unit = vec![0u8; self.extent(handle, BLOCK_TRAILER_LEN)?];
		self.file.read_exact_at(handle.offset() as u64, &mut unit)?;
		Ok(unit)
	}

	/// Reads the block at `handle` with its trailer through the object store, in one request.
	async fn fetch_async(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
		let len = self.extent(handle, BLOCK_TRAILER_LEN)?;
		let offset = handle.offset() as u64;
		let bytes = self.store.read_at(offset, len).await?;
		if bytes.len() != len {
			return Err(short_read(offset, len, bytes.len()));
		}
		Ok(Vec::from(bytes))
	}

	/// Reads a block, verifies its checksum and decompresses it, and returns its contents.
	/// Filter partitions have an encoding of their own, so they are used as bytes.
	pub(crate) fn read_contents(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
		open_unit(self.fetch(handle)?, handle)
	}

	/// Reads a block as `read_contents` does and decodes it as a block of sorted entries.
	pub(crate) fn read_block(
		&self,
		handle: &BlockHandle,
		comparator: Arc<dyn Comparator>,
	) -> Result<Block> {
		into_block(self.read_contents(handle)?, comparator)
	}

	/// `read_block` for the asynchronous path.
	pub(crate) async fn read_block_async(
		&self,
		handle: &BlockHandle,
		comparator: Arc<dyn Comparator>,
	) -> Result<Block> {
		into_block(open_unit(self.fetch_async(handle).await?, handle)?, comparator)
	}

	/// Reads the monolithic filter of a table that was written before filters were
	/// partitioned. Its block has no trailer that is read here, and its bytes are used as they
	/// are.
	pub(crate) fn read_legacy_filter(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
		let mut buf = vec![0u8; self.extent(handle, 0)?];
		self.file.read_exact_at(handle.offset() as u64, &mut buf)?;
		Ok(buf)
	}
}

/// Decodes the contents of a block that passed its checks as a block of sorted entries.
fn into_block(contents: Vec<u8>, comparator: Arc<dyn Comparator>) -> Result<Block> {
	// `Block::new` asserts on this, and a block whose checksum is right can still be too small:
	// the checksum says the bytes are the ones that were written, not that they are a block.
	if contents.len() <= 4 {
		return Err(Error::from(SSTableError::BlockTooSmall {
			size: contents.len(),
			min_size: 5,
		}));
	}
	Ok(Block::new(contents, comparator))
}

/// Verifies the trailer of a block that was read with it and returns the block's contents,
/// decompressed.
fn open_unit(mut unit: Vec<u8>, handle: &BlockHandle) -> Result<Vec<u8>> {
	let size =
		unit.len().checked_sub(BLOCK_TRAILER_LEN).ok_or(SSTableError::CorruptedBlockHandle)?;
	let (covered, stored) = unit.split_at(size + BLOCK_COMPRESS_LEN);
	let stored: [u8; BLOCK_CKSUM_LEN] =
		stored.try_into().map_err(|_| SSTableError::CorruptedBlockHandle)?;
	if crc32fast::hash(covered) != unmask(u32::from_le_bytes(stored)) {
		return Err(Error::from(SSTableError::ChecksumVerificationFailed {
			block_offset: handle.offset() as u64,
		}));
	}

	let compression = CompressionType::try_from(unit[size])?;
	if compression == CompressionType::None {
		// What was read is the block, followed by its trailer.
		unit.truncate(size);
		return Ok(unit);
	}
	decompress_block(&unit[..size], compression)
}
