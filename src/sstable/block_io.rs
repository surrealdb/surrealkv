//! # Block I/O of one SSTable
//!
//! `BlockSource` owns the file of a table and every read of its bytes: the footer, and each
//! block named by a handle. The index and the filter readers hold a source instead of the file,
//! so there is one place that knows how a block sits on disk and what is checked before its
//! bytes are used. `BlockSink` is the same for writing: every block of a table is written
//! through it, with the kind of block that it is.
//!
//! ## A block on disk
//!
//! A block is followed by a trailer, and which trailer is a property of the file. The handle
//! of a block says where it starts and how long it is without the trailer, and a block and its
//! trailer are read in one request.
//!
//! ```text
//! Plain (5 bytes):
//! +----------------------+-----------------+-----------------+
//! | block (handle.size)  | compression (1) | masked CRC (4)  |
//! +----------------------+-----------------+-----------------+
//!
//! Sealed (9 bytes):
//! +----------------------------+-----------------+-----------+-----------------+
//! | sealed block (handle.size) | compression (1) | len (4)   | masked CRC (4)  |
//! +----------------------------+-----------------+-----------+-----------------+
//! ```
//!
//! The CRC32 covers every byte before it, so in the sealed layout it covers the sealed block, the
//! compression byte and the length. The length is the length of the sealed block, and it is
//! little endian like the CRC. A reader checks it against the handle, and a mismatch is
//! corruption: the trailer and the handle are two records of one fact. A sealed block is what a
//! `BlockSealer` made of the block after it was compressed, and the compression byte stays
//! outside of it, so the sealer is given it to bind.
//!
//! A table has the plain layout unless whoever opens it, or writes it, says otherwise. The footer
//! says which layout the blocks of a file have (`ChecksumType`), and a file with the sealed one
//! also begins with a header, so its blocks start after it:
//!
//! ```text
//! Plain:   [block][block]...[block][footer]
//! Sealed:  [header][sealed block][sealed block]...[sealed block][footer]
//! ```
//!
//! Every byte of a file between its start and its footer belongs to exactly one of the header
//! and the blocks, and a handle that names bytes of the header is refused. The footer and the
//! header are the only parts that are not blocks, and neither holds anything secret.
//!
//! ## What is checked before bytes are used
//!
//! - The handle must lie inside the part of the file that holds blocks, which starts after the
//!   header if the file has one and ends where the footer begins. This is checked before the buffer
//!   is allocated, so a handle that was damaged cannot ask for gigabytes.
//! - The read must return every byte that was asked for. A file may return fewer bytes than the
//!   buffer holds without being at its end, and a file that ends early is an error rather than a
//!   buffer with a tail of zeros.
//! - The checksum must match, and in the sealed layout the length must agree with the handle.
//! - In a file with a sealer, the block must open: it is authenticated before it is decompressed or
//!   decoded, and a block that does not open is an error that stops there.
//!
//! The reads that are not `pub(crate)` here are the only ones that return bytes that have not
//! been through all of that, and none of the functions that are returns them: what a caller
//! gets is the contents of a block, or a `Block` made of them.

use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::{EncryptionError, Error, Result};
use crate::sstable::block::{Block, BlockData, BlockHandle};
use crate::sstable::error::SSTableError;
use crate::sstable::table::{
	compress_block,
	decompress_block,
	mask,
	unmask,
	ChecksumType,
	Footer,
	BLOCK_CKSUM_LEN,
	BLOCK_COMPRESS_LEN,
	TABLE_FULL_FOOTER_LENGTH,
};
use crate::storage::{AffinityObjectStore, ObjectStore};
use crate::vfs::{short_read, File};
use crate::{Comparator, CompressionType};

/// The bytes after a block in the plain layout: its compression type and the masked CRC32.
pub(crate) const PLAIN_TRAILER_LEN: usize = BLOCK_COMPRESS_LEN + BLOCK_CKSUM_LEN;

/// The bytes after a block in the sealed layout: the plain trailer with the length of the sealed
/// block between the compression type and the checksum.
pub(crate) const SEALED_TRAILER_LEN: usize = PLAIN_TRAILER_LEN + SEALED_LEN_BYTES;

/// Size of the length field of the sealed layout.
const SEALED_LEN_BYTES: usize = 4;

// =============================================================================
// KINDS AND LAYOUTS
// =============================================================================

/// What a block of an SSTable is for. The writer says it for every block that it writes and the
/// reader for every block that it reads, so a sealer can bind it: a block of one kind must not
/// open as a block of another.
///
/// The numbers (`kind as u8`) are part of what a sealer authenticates once there is one, and are
/// not to be renumbered after that.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum BlockKind {
	/// A block of key-value entries.
	Data = 1,
	/// A block of an index partition, which maps keys to data blocks.
	IndexPartition = 2,
	/// The block that maps keys to the index partitions.
	TopLevelIndex = 3,
	/// The bits of a filter partition.
	FilterPartition = 4,
	/// The block that maps keys to the filter partitions.
	FilterIndex = 5,
	/// The block that holds the properties of the table and the handle of the filter index.
	MetaIndex = 6,
}

/// How a block is followed on disk, which is the same for every block of a file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrailerLayout {
	/// `[compression:1][masked crc32:4]`
	Plain,
	/// `[compression:1][len:4][masked crc32:4]`
	Sealed,
}

impl TrailerLayout {
	/// The length of the trailer in bytes.
	pub(crate) const fn len(self) -> usize {
		match self {
			Self::Plain => PLAIN_TRAILER_LEN,
			Self::Sealed => SEALED_TRAILER_LEN,
		}
	}
}

/// The trailer of one block, encoded.
pub(crate) struct Trailer {
	bytes: [u8; SEALED_TRAILER_LEN],
	len: usize,
}

impl Trailer {
	/// The trailer for `payload`, the bytes that precede it in the file, which were compressed
	/// with `compression`.
	pub(crate) fn encode(layout: TrailerLayout, payload: &[u8], compression: u8) -> Result<Self> {
		let mut crc = crc32fast::Hasher::new();
		crc.update(payload);
		crc.update(&[compression]);

		let mut bytes = [0u8; SEALED_TRAILER_LEN];
		bytes[0] = compression;
		let crc_at = match layout {
			TrailerLayout::Plain => PLAIN_TRAILER_LEN - BLOCK_CKSUM_LEN,
			TrailerLayout::Sealed => {
				let len = u32::try_from(payload.len()).map_err(|_| {
					Error::from(SSTableError::BlockTooLarge {
						size: payload.len() as u64,
					})
				})?;
				crc.update(&len.to_le_bytes());
				bytes[1..1 + SEALED_LEN_BYTES].copy_from_slice(&len.to_le_bytes());
				SEALED_TRAILER_LEN - BLOCK_CKSUM_LEN
			}
		};
		bytes[crc_at..crc_at + BLOCK_CKSUM_LEN]
			.copy_from_slice(&mask(crc.finalize()).to_le_bytes());
		Ok(Self {
			bytes,
			len: layout.len(),
		})
	}

	pub(crate) fn as_slice(&self) -> &[u8] {
		&self.bytes[..self.len]
	}
}

/// Checks the trailer at the end of `unit`, which is a block as it was read with its trailer,
/// and returns the compression byte that the trailer holds. The checksum is checked first, and
/// then, in the sealed layout, the length against the length of the block: the length is not
/// trusted before it is known to be the one that was written.
///
/// `block_offset` is where the block starts, for the error.
pub(crate) fn verify_trailer(layout: TrailerLayout, unit: &[u8], block_offset: u64) -> Result<u8> {
	let corrupt = || Error::from(SSTableError::CorruptedBlockHandle);
	let size = unit.len().checked_sub(layout.len()).ok_or_else(corrupt)?;
	let (covered, stored) = unit.split_at(unit.len() - BLOCK_CKSUM_LEN);
	let stored: [u8; BLOCK_CKSUM_LEN] = stored.try_into().map_err(|_| corrupt())?;
	if crc32fast::hash(covered) != unmask(u32::from_le_bytes(stored)) {
		return Err(Error::from(SSTableError::ChecksumVerificationFailed {
			block_offset,
		}));
	}

	let compression = *unit.get(size).ok_or_else(corrupt)?;
	if layout == TrailerLayout::Sealed {
		let len = unit.get(size + 1..size + 1 + SEALED_LEN_BYTES).ok_or_else(corrupt)?;
		let len = u32::from_le_bytes(len.try_into().map_err(|_| corrupt())?);
		if u64::from(len) != size as u64 {
			return Err(Error::from(SSTableError::TrailerLengthMismatch {
				block_offset,
				block_size: size as u64,
				trailer_len: u64::from(len),
			}));
		}
	}
	Ok(compression)
}

// =============================================================================
// SEALING
// =============================================================================

/// What a sealer is told about the block that it seals or opens, which is what it binds to the
/// block besides the file that it keeps: where the block is, what it is for, and how it was
/// compressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockContext {
	pub(crate) kind: BlockKind,
	/// Where the block starts in the file.
	pub(crate) offset: u64,
	/// The compression byte of the trailer, which is outside of the sealed block.
	pub(crate) compression: u8,
}

/// Seals and opens the blocks of one file.
///
/// A block is compressed and then sealed, and opened and then decompressed. A sealer must never
/// hand back bytes from `open` that it has not authenticated.
///
/// A sealer that is used to write belongs to one file. A block is sealed with its position in
/// the file in what it binds, and a position can be the nonce of a cipher, so two files that
/// were sealed by one sealer would use the same positions under the same key. Readers of a file
/// share its sealer, and nothing but the code that makes the sealer of a file can stop a second
/// sink from being given it.
pub(crate) trait BlockSealer: Send + Sync {
	/// How many bytes sealing adds to a block, which is the authentication tag.
	fn overhead(&self) -> usize;

	/// Replaces the compressed block in `block` with the sealed one.
	fn seal(&self, context: &BlockContext, block: &mut Vec<u8>) -> Result<()>;

	/// Replaces the sealed block in `block`, which is at least `overhead` bytes, with the
	/// compressed one, or fails and leaves nothing that can be used.
	fn open(&self, context: &BlockContext, block: &mut Vec<u8>) -> Result<()>;
}

/// How the blocks of a file are protected, which also says how they are followed.
#[derive(Clone)]
pub(crate) enum Protection {
	/// A CRC32 and nothing else, with the plain trailer.
	Plain,
	/// Sealed blocks with the sealed trailer.
	Sealed(Arc<dyn BlockSealer>),
}

impl Protection {
	pub(crate) fn layout(&self) -> TrailerLayout {
		match self {
			Self::Plain => TrailerLayout::Plain,
			Self::Sealed(_) => TrailerLayout::Sealed,
		}
	}

	/// What the footer of a file with this protection says about its blocks.
	pub(crate) fn checksum_type(&self) -> ChecksumType {
		match self {
			Self::Plain => ChecksumType::CRC32c,
			Self::Sealed(_) => ChecksumType::SealedCRC32c,
		}
	}
}

/// What a file starts with and how its blocks are sealed, which is only known when the file
/// is started.
pub(crate) struct FileStart {
	/// The bytes that come before the first block, written as they are.
	pub(crate) header: Vec<u8>,
	/// Seals the blocks of this file and no other. It is made for the file, so that a position in
	/// the file is never given to it for another.
	pub(crate) sealer: Arc<dyn BlockSealer>,
}

/// Starts the file of a sink when it is first written to, instead of when the sink is made.
///
/// Starting a sealed file takes a key, which can fail, and a table writer is made where there is
/// nowhere to report that. A sink that was given a starter asks it for the start of the file
/// when its first block or its footer is to be written, so the failure is the failure of that
/// call, and nothing is written for a file that could not be started.
pub(crate) trait FileStarter: Send + Sync {
	/// The length of the header that `start` returns. The first block of the file goes after
	/// it, so the sink knows where blocks are before it has asked for the header.
	fn header_len(&self) -> usize;

	/// Makes the start of the file. A sink calls it once, and ends if it fails.
	fn start(self: Box<Self>) -> Result<FileStart>;
}

// =============================================================================
// BLOCK SOURCE
// =============================================================================

/// Reads the footer and the blocks of one SSTable file.
///
/// Cheap to clone: the clones read the same file.
#[derive(Clone)]
pub(crate) struct BlockSource {
	file: Arc<dyn File>,
	/// The same file for the asynchronous reads of data blocks.
	store: Arc<dyn ObjectStore>,
	file_size: u64,
	protection: Protection,
	/// Where the first block of the file starts: after its header, if it has one.
	blocks_start: u64,
	/// The key under which the block cache keeps the blocks that this source reads, see
	/// `cache_id`.
	cache_id: u64,
	/// What was read from the file, shared by the clones.
	reads: Arc<ReadCounters>,
}

/// How much a source asked of its file. A request is counted when it is made, which is a read of
/// a block with its trailer, of the footer, or of a legacy filter, so what a short read costs
/// the file over and above that is not in it.
#[derive(Default)]
struct ReadCounters {
	requests: AtomicU64,
	bytes: AtomicU64,
}

/// A block that was checked against everything that its file can say about it, and that is
/// ready to be decompressed. It can only be made by `BlockSource::authenticate`.
struct Authenticated {
	/// The compressed block, with the trailer and the seal taken off.
	bytes: Vec<u8>,
	compression: u8,
}

impl BlockSource {
	/// A source over `file`, which holds `file_size` bytes, for a file with plain blocks. The
	/// footer is not read.
	pub(crate) fn new(file: Arc<dyn File>, file_size: u64) -> Self {
		Self::with_protection(file, file_size, Protection::Plain)
	}

	/// A source over `file` for blocks that are protected as `protection` says.
	pub(crate) fn with_protection(
		file: Arc<dyn File>,
		file_size: u64,
		protection: Protection,
	) -> Self {
		let store: Arc<dyn ObjectStore> = Arc::new(AffinityObjectStore::new(Arc::clone(&file)));
		Self {
			file,
			store,
			file_size,
			protection,
			blocks_start: 0,
			cache_id: crate::cache::next_instance_id(),
			reads: Arc::new(ReadCounters::default()),
		}
	}

	/// The same source for a file whose first block starts at `header_len` instead of at its
	/// first byte, which is what a file that begins with a header is. No handle may name a byte
	/// before it.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn after_header(mut self, header_len: u64) -> Self {
		self.blocks_start = header_len;
		self
	}

	/// The number of reads of the file that this source and its clones asked for: the footer when
	/// the table was opened, and a block with its trailer for each block that was not found in
	/// the block cache. A test of a feature that is meant to change what is read can assert that
	/// reads happened, and how many.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn raw_reads(&self) -> u64 {
		self.reads.requests.load(Ordering::Relaxed)
	}

	/// The number of bytes that `raw_reads` asked for.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn raw_read_bytes(&self) -> u64 {
		self.reads.bytes.load(Ordering::Relaxed)
	}

	fn count_read(&self, len: usize) {
		self.reads.requests.fetch_add(1, Ordering::Relaxed);
		self.reads.bytes.fetch_add(len as u64, Ordering::Relaxed);
	}

	/// The identity under which the block cache keeps the blocks that are read through this
	/// source and its clones, which is never the identity of another source: a table id names a
	/// file of a manifest and can come back for another file, but a source is made once, for the
	/// file that it was given, and the id of the instance is not given again. The readers of the
	/// index and the filter take it from here instead of being told, so a reader cannot be built
	/// to look for the blocks of another table.
	pub(crate) fn cache_id(&self) -> u64 {
		self.cache_id
	}

	/// Opens a table file: reads its footer and returns it with the source that reads its
	/// blocks, for both the synchronous and the asynchronous paths. The layout of the blocks is
	/// the one that the footer says.
	///
	/// This is the way to open a table that nobody holds a key for, so it opens a file whose footer
	/// says that its blocks are plain, and refuses one that says that they are sealed: there is
	/// nothing here to open them with. A footer is not authenticated, and whoever can write the
	/// file can make it say that its blocks are plain, with checksums that anyone can compute. A
	/// database whose tables are meant to be sealed therefore cannot use this function: it must
	/// refuse a footer that says plain, which is what the opener of tables with a key is for.
	pub(crate) fn open(file: Arc<dyn File>, file_size: u64) -> Result<(Self, Footer)> {
		Self::open_with(file, file_size, |footer| match footer.checksum {
			ChecksumType::CRC32c => Ok(Protection::Plain),
			ChecksumType::SealedCRC32c => Err(Error::Encryption(EncryptionError::NotConfigured)),
		})
	}

	/// Opens a table file as `open` does, with `protection_for` choosing how its blocks are
	/// protected once its footer is known.
	pub(crate) fn open_with(
		file: Arc<dyn File>,
		file_size: u64,
		protection_for: impl FnOnce(&Footer) -> Result<Protection>,
	) -> Result<(Self, Footer)> {
		let mut source = Self::new(file, file_size);
		let footer = source.read_footer()?;
		source.protection = protection_for(&footer)?;
		Ok((source, footer))
	}

	/// How the blocks that this source reads are followed.
	pub(crate) fn layout(&self) -> TrailerLayout {
		self.protection.layout()
	}

	/// The footer is the only thing in a table that is not a block, and the only read that is
	/// made without a handle.
	fn read_footer(&self) -> Result<Footer> {
		let footer_len = TABLE_FULL_FOOTER_LENGTH as u64;
		if self.file_size < footer_len {
			return Err(Error::from(SSTableError::FileTooSmall {
				file_size: self.file_size as usize,
				min_size: TABLE_FULL_FOOTER_LENGTH,
			}));
		}
		let mut buf = [0u8; TABLE_FULL_FOOTER_LENGTH];
		self.count_read(buf.len());
		self.file.read_exact_at(self.file_size - footer_len, &mut buf)?;
		Footer::decode(&buf)
	}

	/// The length of the block at `handle` with its trailer, if all of it lies in the part of
	/// the file that holds blocks, which is after the header and before the footer.
	/// `trailer_len` is what follows the block.
	fn extent(&self, handle: &BlockHandle, trailer_len: usize) -> Result<usize> {
		let limit = self.file_size.saturating_sub(TABLE_FULL_FOOTER_LENGTH as u64);
		let first = self.blocks_start;
		let (offset, size) = (handle.offset() as u64, handle.size() as u64);
		let end = size.checked_add(trailer_len as u64).and_then(|len| len.checked_add(offset));
		// The length that is read is a `usize`, which is 32 bits on some targets: a size that
		// passes the test against the file can still not take its trailer in that width.
		match (end, handle.size().checked_add(trailer_len)) {
			(Some(end), Some(len)) if offset >= first && end <= limit => Ok(len),
			_ => Err(Error::from(SSTableError::BlockHandleOutOfBounds {
				block_offset: offset,
				block_size: size,
				first_block: first,
				limit,
			})),
		}
	}

	/// Reads the block at `handle` with its trailer, in one request.
	fn fetch(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
		let mut unit = vec![0u8; self.extent(handle, self.layout().len())?];
		self.count_read(unit.len());
		self.file.read_exact_at(handle.offset() as u64, &mut unit)?;
		Ok(unit)
	}

	/// Reads the block at `handle` with its trailer through the object store, in one request.
	async fn fetch_async(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
		let len = self.extent(handle, self.layout().len())?;
		let offset = handle.offset() as u64;
		self.count_read(len);
		let bytes = self.store.read_at(offset, len).await?;
		if bytes.len() != len {
			return Err(short_read(offset, len, bytes.len()));
		}
		Ok(Vec::from(bytes))
	}

	/// Checks a block that was read with its trailer, and opens it if the file has a sealer. This
	/// is the only way to the bytes of a block, and it ends at bytes that are fit to be
	/// decompressed.
	fn authenticate(
		&self,
		mut unit: Vec<u8>,
		kind: BlockKind,
		handle: &BlockHandle,
	) -> Result<Authenticated> {
		let layout = self.layout();
		let block_offset = handle.offset() as u64;
		let compression = verify_trailer(layout, &unit, block_offset)?;

		// What was read is the block, followed by its trailer.
		unit.truncate(unit.len() - layout.len());
		if let Protection::Sealed(sealer) = &self.protection {
			if unit.len() < sealer.overhead() {
				return Err(Error::from(SSTableError::SealedBlockTooShort {
					block_offset,
					size: unit.len() as u64,
					min_size: sealer.overhead() as u64,
				}));
			}
			let context = BlockContext {
				kind,
				offset: block_offset,
				compression,
			};
			sealer.open(&context, &mut unit)?;
		}
		Ok(Authenticated {
			bytes: unit,
			compression,
		})
	}

	/// Reads a block, authenticates it and decompresses it, and returns its contents. Filter
	/// partitions have an encoding of their own, so they are used as bytes.
	pub(crate) fn read_contents(&self, kind: BlockKind, handle: &BlockHandle) -> Result<Vec<u8>> {
		decompress(self.authenticate(self.fetch(handle)?, kind, handle)?)
	}

	/// Reads a block as `read_contents` does and decodes it as a block of sorted entries.
	pub(crate) fn read_block(
		&self,
		kind: BlockKind,
		handle: &BlockHandle,
		comparator: Arc<dyn Comparator>,
	) -> Result<Block> {
		into_block(self.read_contents(kind, handle)?, comparator)
	}

	/// `read_block` for the asynchronous path.
	pub(crate) async fn read_block_async(
		&self,
		kind: BlockKind,
		handle: &BlockHandle,
		comparator: Arc<dyn Comparator>,
	) -> Result<Block> {
		let unit = self.fetch_async(handle).await?;
		into_block(decompress(self.authenticate(unit, kind, handle)?)?, comparator)
	}

	/// Reads the monolithic filter of a table that was written before filters were
	/// partitioned. Its block has no trailer that is read here, and its bytes are used as they
	/// are, which is why a table with sealed blocks must not have one.
	pub(crate) fn read_legacy_filter(&self, handle: &BlockHandle) -> Result<Vec<u8>> {
		if self.layout() != TrailerLayout::Plain {
			return Err(Error::Corruption("a table with sealed blocks has a legacy filter".into()));
		}
		let mut buf = vec![0u8; self.extent(handle, 0)?];
		self.count_read(buf.len());
		self.file.read_exact_at(handle.offset() as u64, &mut buf)?;
		Ok(buf)
	}
}

/// Decompresses a block that was authenticated.
fn decompress(block: Authenticated) -> Result<Vec<u8>> {
	let compression = CompressionType::try_from(block.compression)?;
	if compression == CompressionType::None {
		return Ok(block.bytes);
	}
	decompress_block(&block.bytes, compression)
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

// =============================================================================
// BLOCK SINK
// =============================================================================

/// Writes the blocks of one SSTable file, one after the other, and keeps track of where the next
/// one goes.
///
/// A block is compressed, sealed if the file has a sealer, and followed by its trailer. Its
/// position is that of the running end of the file and is never used twice: a block that fails
/// once its position was given to the sealer, whether in sealing, in making its trailer or in
/// the write, leaves the sink refusing all that follows, since what is in the file after it is
/// not known, and the position cannot be taken again.
///
/// A file may begin with a header, which a starter makes when the file is first written to: the
/// header goes first and the first block after it, and `offset` counts the header from the
/// beginning. The footer says which layout the blocks have, so the sink writes it and the caller
/// does not choose.
pub(crate) struct BlockSink<W: Write> {
	writer: W,
	protection: Protection,
	/// Makes the start of the file when it is first written to, until then.
	starter: Option<Box<dyn FileStarter>>,
	/// Where the next block goes.
	offset: usize,
	/// A block failed after its position was given to the sealer, or a write failed: no position
	/// after it can be trusted, and none may be taken twice.
	failed: bool,
}

impl<W: Write> BlockSink<W> {
	/// A sink for a file with plain blocks.
	pub(crate) fn new(writer: W) -> Self {
		Self::with_protection(writer, Protection::Plain)
	}

	/// A sink for blocks that are protected as `protection` says, in a file that begins with its
	/// first block.
	pub(crate) fn with_protection(writer: W, protection: Protection) -> Self {
		Self {
			writer,
			protection,
			starter: None,
			offset: 0,
			failed: false,
		}
	}

	/// A sink for a file that begins with the header that `starter` makes, and whose blocks are
	/// sealed by what the starter returns with it. Nothing is asked of the starter, and nothing
	/// is written, until the first block or the footer is.
	pub(crate) fn deferred(writer: W, starter: Box<dyn FileStarter>) -> Self {
		Self {
			writer,
			protection: Protection::Plain,
			offset: starter.header_len(),
			starter: Some(starter),
			failed: false,
		}
	}

	/// Starts the file if it has a starter that was not asked yet: writes its header, and takes
	/// its sealer for the blocks. A failure ends the sink, and the starter is not asked again, so
	/// a key that was made for this file is never used for another.
	fn start(&mut self) -> Result<()> {
		let Some(starter) = self.starter.take() else {
			return Ok(());
		};
		let result = starter.start().and_then(|start| {
			// The first block goes where the sink was told that it would.
			if start.header.len() != self.offset {
				return Err(Error::Other(
					"the header of a table is not the length that it was told".into(),
				));
			}
			self.writer.write_all(&start.header)?;
			Ok(start.sealer)
		});
		match result {
			Ok(sealer) => {
				self.protection = Protection::Sealed(sealer);
				Ok(())
			}
			Err(e) => {
				self.failed = true;
				Err(e)
			}
		}
	}

	/// The number of bytes written, which is where the next block goes.
	pub(crate) fn offset(&self) -> usize {
		self.offset
	}

	/// Compresses, seals and writes a block, and returns its handle: the block is
	/// `handle.size` bytes, and its trailer follows it.
	pub(crate) fn write_block(
		&mut self,
		kind: BlockKind,
		contents: BlockData,
		compression: CompressionType,
	) -> Result<BlockHandle> {
		if self.failed {
			return Err(Error::Other("a write to the table failed earlier".into()));
		}
		self.start()?;

		let payload = compress_block(contents, compression)?;
		let offset = self.offset;
		let compression = compression as u8;

		// From the moment that a sealer is asked to seal at `offset`, that position is spoken
		// for: a cipher that derives its nonce from it must not be asked for it again with other
		// bytes, whether the seal itself failed, or the trailer could not be made, or the write
		// did not happen. Nothing is written for such a block, so the sink ends here.
		let (payload, trailer) =
			match Self::frame(&self.protection, kind, offset, compression, payload) {
				Ok(framed) => framed,
				Err(e) => {
					self.failed = true;
					return Err(e);
				}
			};
		let layout = self.protection.layout();

		let written = self
			.writer
			.write_all(&payload)
			.and_then(|()| self.writer.write_all(trailer.as_slice()));
		if let Err(e) = written {
			self.failed = true;
			return Err(e.into());
		}
		self.offset = offset + payload.len() + layout.len();
		Ok(BlockHandle::new(offset, payload.len()))
	}

	/// Seals a compressed block that is to go at `offset`, if the file has a sealer, and makes the
	/// trailer that follows it.
	fn frame(
		protection: &Protection,
		kind: BlockKind,
		offset: usize,
		compression: u8,
		mut payload: Vec<u8>,
	) -> Result<(Vec<u8>, Trailer)> {
		if let Protection::Sealed(sealer) = protection {
			let context = BlockContext {
				kind,
				offset: offset as u64,
				compression,
			};
			sealer.seal(&context, &mut payload)?;
		}
		let trailer = Trailer::encode(protection.layout(), &payload, compression)?;
		Ok((payload, trailer))
	}

	/// Writes the footer, which is the one part of the file that is neither a block nor the
	/// header, flushes, and returns the size of the file.
	pub(crate) fn write_footer(&mut self, footer: &Footer) -> Result<usize> {
		if self.failed {
			return Err(Error::Other("a write to the table failed earlier".into()));
		}
		self.start()?;
		// The layout of the blocks is that of the file, so it is not for the caller to say: a
		// sealed file must not be written with a footer that says its blocks are plain.
		let mut footer = footer.clone();
		footer.checksum = self.protection.checksum_type();
		let mut buf = [0u8; TABLE_FULL_FOOTER_LENGTH];
		footer.encode(&mut buf);
		let written = self.writer.write_all(&buf).and_then(|()| self.writer.flush());
		if let Err(e) = written {
			self.failed = true;
			return Err(e.into());
		}
		self.offset += TABLE_FULL_FOOTER_LENGTH;
		Ok(self.offset)
	}
}
