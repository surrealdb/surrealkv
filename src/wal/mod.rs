use std::fmt;
use std::fs::{self, read_dir, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, PoisonError};

use crc32fast::Hasher;

use crate::error::EncryptionError;
use crate::tde::EncryptionContext;

pub mod manager;
pub(crate) mod parallel_recovery;
pub mod reader;
pub mod recovery;
pub(crate) mod sealed;
pub(crate) mod sealed_recovery;
pub mod writer;

pub use manager::Wal;
pub(crate) use manager::WalManager;

// ===== Format Constants and Types =====

/// The size of a single block in bytes (32KB).
///
/// Records are written in blocks of this size. If a record doesn't fit in the
/// remaining space of a block, it will be fragmented across multiple blocks.
///
/// # File Format
///
/// A WAL file is broken down into 32KB blocks. Each block contains one or more
/// records. If a record doesn't fit in the remaining space of a block, it's
/// fragmented across multiple blocks.
///
/// ```text
/// File Layout:
///   +-----+-------------+--+----+----------+------+-- ... ----+
///   | r0  |     r1      |P | r2 |    r3    |  r4  |           |
///   +-----+-------------+--+----+----------+------+-- ... ----+
///   <--- kBlockSize ------>|<-- kBlockSize ------>|
/// ```
///
/// Where:
/// - rN = variable size records
/// - P = Padding (zeros)
///
/// # Record Format (7 bytes header):
/// ```text
/// +---------+-----------+-----------+--- ... ---+
/// |CRC (4B) | Size (2B) | Type (1B) | Payload   |
/// +---------+-----------+-----------+--- ... ---+
/// ```
///
/// Where:
/// - CRC = 32-bit CRC computed over the record type and payload (big-endian)
/// - Size = Length of the payload data (big-endian)
/// - Type = Type of record (see RecordType enum)
/// - Payload = Byte stream of the specified size
pub const BLOCK_SIZE: usize = 32 * 1024;

/// Length of the record header in bytes.
///
/// Header format: CRC (4 bytes) + Length (2 bytes) + Type (1 byte) = 7 bytes
pub const HEADER_SIZE: usize = 7;

/// Enum representing different types of records in the Write-Ahead Log.
///
/// Records can be:
/// - Full records that fit entirely in one block
/// - Fragmented records split across multiple blocks (First, Middle, Last)
/// - Metadata records for compression and other settings
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
#[repr(u8)]
pub enum RecordType {
	/// Indicates that the rest of the block is empty (zero-filled padding).
	Empty = 0,

	/// A complete record that fits entirely within a single block.
	Full = 1,

	/// The first fragment of a record that spans multiple blocks.
	First = 2,

	/// A middle fragment of a record (not first, not last).
	Middle = 3,

	/// The final fragment of a record.
	Last = 4,

	// Metadata record types
	/// Indicates the compression type used for subsequent records.
	SetCompressionType = 9,

	/// The first record of a sealed segment, which holds its encryption header. A plaintext
	/// segment never has one, and a sealed segment has no other record of the metadata types.
	SegmentHeader = 10,
}

impl RecordType {
	/// Converts a u8 value to a RecordType.
	///
	/// # Errors
	///
	/// Returns an error if the value doesn't correspond to a valid RecordType.
	pub fn from_u8(value: u8) -> Result<Self> {
		match value {
			0 => Ok(RecordType::Empty),
			1 => Ok(RecordType::Full),
			2 => Ok(RecordType::First),
			3 => Ok(RecordType::Middle),
			4 => Ok(RecordType::Last),
			9 => Ok(RecordType::SetCompressionType),
			10 => Ok(RecordType::SegmentHeader),
			_ => Err(Error::IO(IOError::new(io::ErrorKind::InvalidInput, "Invalid Record Type"))),
		}
	}
}

/// Compression types supported by the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
	/// No compression.
	None = 0,

	/// LZ4 compression using lz4_flex.
	Lz4 = 1,
}

impl CompressionType {
	/// Converts a u8 value to a CompressionType.
	pub fn from_u8(value: u8) -> Result<Self> {
		match value {
			0 => Ok(CompressionType::None),
			1 => Ok(CompressionType::Lz4),
			_ => Err(Error::IO(IOError::new(
				io::ErrorKind::InvalidInput,
				"Invalid Compression Type",
			))),
		}
	}
}

// ===== Options =====

/// Default file mode for newly created files.
const DEFAULT_FILE_MODE: u32 = 0o644;

/// Default maximum size of the segment file (100MB).
const DEFAULT_FILE_SIZE: u64 = 100 * 1024 * 1024;

/// What a WAL needs to know about encryption to write, read and repair its segments.
///
/// The default is plaintext. A tree builds one with [`WalContext::for_options`] and nothing else
/// decides whether segments are sealed, so every place that opens a WAL, replays one or repairs one
/// is handed the same answer. A site that forgot to take it would read or write plaintext in an
/// encrypted tree, which is why the plaintext constructor is for tests.
///
/// Cheap to clone, and shared by the workers that decode segments in parallel.
#[derive(Clone, Default)]
pub(crate) struct WalContext {
	/// What creates and opens the encrypted files of the database, if the WAL is sealed.
	cipher: Option<Arc<EncryptionContext>>,
}

impl WalContext {
	/// A plaintext WAL. Only tests build one this way: a tree takes its context from its options.
	#[cfg(test)]
	pub(crate) fn plaintext() -> Self {
		Self::default()
	}

	/// A sealed WAL under `context`. Only tests build one this way: a tree takes its context from
	/// its options.
	#[cfg(test)]
	pub(crate) fn sealed(context: Arc<EncryptionContext>) -> Self {
		Self {
			cipher: Some(context),
		}
	}

	/// The context of the WAL of the tree that `opts` configure: sealed if the options carry a
	/// resolved encryption context, plaintext otherwise. Options that name a key manager and have
	/// no context are refused, since a plaintext log would be written for a database that was
	/// asked to be encrypted.
	pub(crate) fn for_options(opts: &crate::Options) -> crate::Result<Self> {
		opts.ensure_encryption_resolved()?;
		Ok(Self {
			cipher: opts.encryption_context().cloned(),
		})
	}

	/// Whether the segments of this WAL are sealed.
	pub(crate) fn is_sealed(&self) -> bool {
		self.cipher.is_some()
	}

	/// What creates and opens the encrypted files of the database, if the WAL is sealed.
	pub(crate) fn encryption(&self) -> Option<&Arc<EncryptionContext>> {
		self.cipher.as_ref()
	}
}

impl fmt::Debug for WalContext {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("WalContext").field("sealed", &self.cipher.is_some()).finish()
	}
}

/// Test-only: where the creation of the next sealed segment is made to fail, as a disk that fails
/// would.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CreationFault {
	/// The write of the header record fails.
	HeaderWrite,
	/// The fsync of the header fails.
	HeaderSync,
	/// The fsync of the directory fails, after the header is durable.
	Directory,
}

/// Options for configuring a WAL segment.
///
/// This struct provides configuration options for WAL segments, including
/// file permissions, compression settings, metadata, and file size limits.
#[derive(Clone)]
pub struct Options {
	/// Whether the segments are sealed, and under what.
	pub(crate) context: WalContext,

	/// Test-only: where the creation of the next sealed segment fails, once.
	#[cfg(test)]
	pub(crate) creation_fault: Arc<parking_lot::Mutex<Option<CreationFault>>>,

	/// The permission mode for creating directories (Unix only).
	#[cfg_attr(not(unix), allow(dead_code))]
	pub(crate) dir_mode: Option<u32>,

	/// The file mode to set for the segment file (Unix only).
	#[cfg_attr(not(unix), allow(dead_code))]
	pub(crate) file_mode: Option<u32>,

	/// The compression type to apply to the segment's data.
	pub(crate) compression_type: CompressionType,

	/// The extension to use for the segment file.
	pub(crate) file_extension: Option<String>,

	/// The maximum size of the segment file.
	pub(crate) max_file_size: u64,
}

impl Default for Options {
	fn default() -> Self {
		Options {
			context: WalContext::default(),
			#[cfg(test)]
			creation_fault: Arc::default(),
			dir_mode: Some(0o750),
			file_mode: Some(DEFAULT_FILE_MODE),
			compression_type: CompressionType::None,
			file_extension: Some("wal".to_string()),
			max_file_size: DEFAULT_FILE_SIZE,
		}
	}
}

impl Options {
	/// The options of the WAL of the tree that `opts` configure: the one constructor that a tree's
	/// own sites (open, restore, repair) use.
	pub(crate) fn for_tree(opts: &crate::Options) -> crate::Result<Self> {
		Ok(Self {
			context: WalContext::for_options(opts)?,
			..Self::default()
		})
	}

	/// Validates the options.
	pub(crate) fn validate(&self) -> Result<()> {
		if self.max_file_size == 0 {
			return Err(Error::IO(IOError::new(
				io::ErrorKind::InvalidInput,
				"invalid max_file_size",
			)));
		}

		Ok(())
	}

	/// Sets the maximum file size.
	#[cfg(test)]
	pub(crate) fn with_max_file_size(mut self, max_file_size: u64) -> Self {
		self.max_file_size = max_file_size;
		self
	}

	/// Sets the compression type.
	#[cfg(test)]
	pub(crate) fn with_compression(mut self, compression_type: CompressionType) -> Self {
		self.compression_type = compression_type;
		self
	}

	/// Sets the file extension.
	#[cfg(test)]
	pub(crate) fn with_extension(mut self, extension: String) -> Self {
		self.file_extension = Some(extension);
		self
	}
}

// ===== Error Types =====

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
	Corruption(CorruptionError),
	SegmentClosed,
	EmptyBuffer,
	Eof(usize),
	IO(IOError),
	Poison(String),
	RecordTooLarge,
	SegmentNotFound,
	/// A key, a header or an authentication tag said no. Never damage, so never a reason to repair
	/// or delete anything.
	Encryption(EncryptionError),
	/// A sealed segment holds records that pass their checksum after a block that is zero-filled
	/// or damaged, or a segment of the sequence is missing: data was lost from the middle of
	/// the WAL, which is not what a torn write at its end looks like.
	IndexGap {
		segment_id: u64,
		offset: u64,
	},
	/// Something that passes its checksum, and so was written on purpose, and that a writer of
	/// this segment never writes: a record of another kind, one out of sequence, one that does not
	/// decompress after it authenticated.
	Unexpected {
		segment_id: u64,
		offset: u64,
		what: &'static str,
	},
}

impl Error {
	/// Whether the error is a statement about keys or about what was written on purpose, as
	/// opposed to damage. Recovery never repairs, rewrites or deletes a segment for one.
	pub(crate) fn is_integrity_failure(&self) -> bool {
		matches!(self, Error::Encryption(_) | Error::IndexGap { .. } | Error::Unexpected { .. })
	}
}

impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Error::Corruption(err) => write!(f, "Corruption error: {err}"),
			Error::SegmentClosed => write!(f, "Segment is closed"),
			Error::EmptyBuffer => write!(f, "Buffer is empty"),
			Error::IO(err) => write!(f, "IO error: {err}"),
			Error::Eof(n) => write!(f, "EOF error after reading {n} bytes"),
			Error::Poison(msg) => write!(f, "Lock Poison: {msg}"),
			Error::RecordTooLarge => {
				write!(f, "Record is too large to fit in a segment. Increase max segment size")
			}
			Error::SegmentNotFound => write!(f, "Segment not found"),
			Error::Encryption(err) => write!(f, "Encryption error: {err}"),
			Error::IndexGap {
				segment_id,
				offset,
			} => write!(
				f,
				"Index gap in segment {segment_id:020} at offset {offset}: records that pass their \
				 checksum follow a zero-filled or damaged block, or a segment is missing"
			),
			Error::Unexpected {
				segment_id,
				offset,
				what,
			} => write!(
				f,
				"Unexpected content in segment {segment_id:020} at offset {offset}: {what}"
			),
		}
	}
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
	fn from(e: io::Error) -> Error {
		Error::IO(IOError {
			kind: e.kind(),
			message: e.to_string(),
		})
	}
}

impl From<EncryptionError> for Error {
	fn from(e: EncryptionError) -> Error {
		Error::Encryption(e)
	}
}

impl<T: Sized> From<PoisonError<T>> for Error {
	fn from(e: PoisonError<T>) -> Error {
		Error::Poison(e.to_string())
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct IOError {
	kind: io::ErrorKind,
	message: String,
}

impl IOError {
	pub(crate) fn new(kind: io::ErrorKind, message: &str) -> Self {
		IOError {
			kind,
			message: message.to_string(),
		}
	}

	pub(crate) fn kind(&self) -> io::ErrorKind {
		self.kind
	}
}

impl fmt::Display for IOError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "kind={}, message={}", self.kind, self.message)
	}
}

#[derive(Debug, Clone, PartialEq)]
pub struct CorruptionError {
	kind: io::ErrorKind,
	message: String,
	pub segment_id: u64,
	pub offset: u64,
}

impl CorruptionError {
	pub(crate) fn new(kind: io::ErrorKind, message: &str, segment_id: u64, offset: u64) -> Self {
		CorruptionError {
			kind,
			message: message.to_string(),
			segment_id,
			offset,
		}
	}
}

impl fmt::Display for CorruptionError {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"kind={}, message={}, segment_id={}, offset={}",
			self.kind, self.message, self.segment_id, self.offset
		)
	}
}

impl std::error::Error for CorruptionError {}

// ===== Segment Utilities =====

/// Generates a segment file name from an index and extension.
pub(crate) fn segment_name(index: u64, ext: &str) -> String {
	if ext.is_empty() {
		format!("{index:020}")
	} else {
		format!("{index:020}.{ext}")
	}
}

/// Parses a segment file name to extract the ID and extension.
pub(crate) fn parse_segment_name(name: &str) -> Result<(u64, Option<String>)> {
	if let Some(dot_pos) = name.find('.') {
		let (id_part, ext_part) = name.split_at(dot_pos);
		let extension = ext_part.trim_start_matches('.');

		let index = id_part.parse::<u64>().map_err(|_| {
			Error::IO(IOError::new(io::ErrorKind::InvalidInput, "Invalid segment name format"))
		})?;

		return Ok((index, Some(extension.to_string())));
	}

	let index = name.parse::<u64>().map_err(|_| {
		Error::IO(IOError::new(io::ErrorKind::InvalidInput, "Invalid segment name format"))
	})?;

	Ok((index, None))
}

/// Gets the range of segment IDs present in the specified directory.
pub(crate) fn get_segment_range(dir: &Path, allowed_extension: Option<&str>) -> Result<(u64, u64)> {
	let refs = list_segment_ids(dir, allowed_extension)?;
	if refs.is_empty() {
		return Ok((0, 0));
	}
	Ok((refs[0], refs[refs.len() - 1]))
}

/// Lists the segment IDs found in the specified directory.
pub(crate) fn list_segment_ids(dir: &Path, allowed_extension: Option<&str>) -> Result<Vec<u64>> {
	let mut refs: Vec<u64> = Vec::new();
	let entries = read_dir(dir)?;

	for entry in entries {
		let file = entry?;

		if std::fs::metadata(file.path())?.is_file() {
			let fn_name = file.file_name();
			let fn_str = fn_name.to_string_lossy();
			let (index, extension) = parse_segment_name(&fn_str)?;

			if !should_include_file(allowed_extension, extension) {
				continue;
			}

			refs.push(index);
		}
	}

	refs.sort();
	Ok(refs)
}

/// Helper function to check if a file should be included based on extension
/// filtering.
pub(crate) fn should_include_file(
	allowed_extension: Option<&str>,
	file_extension: Option<String>,
) -> bool {
	match (&allowed_extension, &file_extension) {
		(None, Some(_)) => false,
		(Some(allowed), Some(ext)) if allowed != ext => false,
		(Some(_), None) => false,
		_ => true,
	}
}

#[derive(Debug, Clone)]
pub(crate) struct SegmentRef {
	pub file_path: PathBuf,
	pub id: u64,
}

impl SegmentRef {
	/// Creates a vector of SegmentRef instances by reading segments in the
	/// specified directory.
	pub(crate) fn read_segments_from_directory(
		directory_path: &Path,
		allowed_extension: Option<&str>,
	) -> Result<Vec<SegmentRef>> {
		let mut segment_refs = Vec::new();

		let files = read_dir(directory_path)?;
		for file in files {
			let entry = file?;
			if entry.file_type()?.is_file() {
				let file_path = entry.path();
				let fn_name = entry.file_name();
				let fn_str = fn_name.to_string_lossy();
				let (index, extension) = parse_segment_name(&fn_str)?;

				if !should_include_file(allowed_extension, extension) {
					continue;
				}

				let segment_ref = SegmentRef {
					file_path,
					id: index,
				};

				segment_refs.push(segment_ref);
			}
		}

		segment_refs.sort_by_key(|a| a.id);
		Ok(segment_refs)
	}
}

/// Calculates CRC32 checksum over record type and data.
pub(crate) fn calculate_crc32(record_type: &[u8], data: &[u8]) -> u32 {
	let mut hasher = Hasher::new();
	hasher.update(record_type);
	hasher.update(data);
	hasher.finalize()
}

/// Validates that a record type is valid for its position in a sequence.
pub(crate) fn validate_record_type(record_type: RecordType, i: usize) -> Result<()> {
	match record_type {
		RecordType::Full => {
			if i != 0 {
				return Err(Error::IO(IOError::new(
					io::ErrorKind::Other,
					"Unexpected full record: first record expected",
				)));
			}
		}
		RecordType::First => {
			if i != 0 {
				return Err(Error::IO(IOError::new(
					io::ErrorKind::Other,
					"Unexpected first record: no previous records expected",
				)));
			}
		}
		RecordType::Middle => {
			if i == 0 {
				return Err(Error::IO(IOError::new(
					io::ErrorKind::Other,
					"Unexpected middle record: missing previous records",
				)));
			}
		}
		RecordType::Last => {
			if i == 0 {
				return Err(Error::IO(IOError::new(
					io::ErrorKind::Other,
					"Unexpected last record: missing previous records",
				)));
			}
		}
		_ => {
			return Err(Error::IO(IOError::new(io::ErrorKind::Other, "Invalid record type")));
		}
	}

	Ok(())
}

// ===== Buffered File Writer =====

/// Trait for writable files.
pub trait WritableFile: Send {
	/// Appends data to the file.
	fn append(&mut self, data: &[u8]) -> Result<()>;

	/// Flushes buffered data to OS cache (not to disk).
	fn flush(&mut self) -> Result<()>;

	/// Syncs all data to disk (flush + fsync).
	///
	/// After a failed fsync the file refuses every later sync, see `SyncGate`.
	fn sync(&mut self) -> Result<()>;

	/// Closes the file, flushing and syncing any remaining buffered data.
	fn close(&mut self) -> Result<()>;
}

/// The error of every append and sync on a segment after one of its fsyncs failed.
pub(crate) fn sync_poisoned() -> Error {
	Error::IO(IOError::new(
		io::ErrorKind::Other,
		"WAL writer is poisoned by an earlier failed sync",
	))
}

/// The error of every append and sync on a segment that is held, see `Writer::hold`.
pub(crate) fn segment_held() -> Error {
	Error::IO(IOError::new(
		io::ErrorKind::Other,
		"WAL segment is held until the memtables of the failed segment are in tables",
	))
}

/// The fsyncs of one WAL segment: the writer's own, and the ones `WalManager::sync` makes
/// outside the WAL lock through a clone of the file.
///
/// They never overlap, and none runs after one failed. The kernel reports a writeback error
/// to a single fsync and drops the pages it could not write, so a second fsync, or one that
/// overlaps the failing one, can report success for data that never reached the disk, and
/// everything acknowledged after it would sit behind a hole that recovery stops at. A failed
/// fsync therefore ends the segment: its writer refuses every later append and sync, and
/// only a rotation, whose new segment has a gate of its own, lets commits through again. The
/// commit pipeline replaces such a segment before it logs its next group, and the segment that
/// replaces it is held, see `Wal::rotate_with`.
///
/// A leaf lock: an fsync holds nothing but the gate, so it is safe to take under the WAL lock.
#[derive(Default)]
pub(crate) struct SyncGate {
	/// Held across each fsync.
	serial: parking_lot::Mutex<()>,
	/// Set by the first fsync that fails, never cleared.
	failed: AtomicBool,
	/// Fault injection for tests: how many more fsyncs succeed before one fails.
	#[cfg(test)]
	fail_after: parking_lot::Mutex<Option<usize>>,
	/// Test-only observer called inside every fsync, with the gate held.
	#[cfg(test)]
	observer: parking_lot::Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
	/// Test-only count of the calls that reached the gate, the ones waiting for it included.
	#[cfg(test)]
	arrivals: AtomicUsize,
}

impl SyncGate {
	/// Whether an fsync of the segment has failed.
	pub(crate) fn failed(&self) -> bool {
		self.failed.load(Ordering::Acquire)
	}

	/// fsyncs `file`, unless an earlier fsync of the segment failed. The first failure is
	/// returned as it happened, and every later call fails with [`sync_poisoned`].
	pub(crate) fn sync(&self, file: &File) -> Result<()> {
		#[cfg(test)]
		self.arrivals.fetch_add(1, Ordering::SeqCst);
		let _serial = self.serial.lock();
		if self.failed() {
			return Err(sync_poisoned());
		}
		let result = self.fsync(file);
		if result.is_err() {
			self.failed.store(true, Ordering::Release);
		}
		result
	}

	fn fsync(&self, file: &File) -> Result<()> {
		#[cfg(test)]
		{
			let observer = self.observer.lock().clone();
			if let Some(observer) = observer {
				observer();
			}
			self.injected_failure()?;
		}
		file.sync_all()?;
		Ok(())
	}

	/// Makes the fsync after the next `syncs` of them fail once, as a failing disk would.
	#[cfg(test)]
	pub(crate) fn fail_after(&self, syncs: usize) {
		*self.fail_after.lock() = Some(syncs);
	}

	/// Installs (or clears) the observer called inside every fsync.
	#[cfg(test)]
	pub(crate) fn set_observer(&self, observer: Option<Arc<dyn Fn() + Send + Sync>>) {
		*self.observer.lock() = observer;
	}

	/// How many calls reached the gate so far, the ones waiting for it included.
	#[cfg(test)]
	pub(crate) fn arrivals(&self) -> usize {
		self.arrivals.load(Ordering::SeqCst)
	}

	#[cfg(test)]
	fn injected_failure(&self) -> Result<()> {
		let mut fail_after = self.fail_after.lock();
		match fail_after.as_mut() {
			Some(0) => {
				*fail_after = None;
				Err(Error::IO(IOError::new(io::ErrorKind::Other, "injected fsync failure")))
			}
			Some(syncs) => {
				*syncs -= 1;
				Ok(())
			}
			None => Ok(()),
		}
	}
}

/// Buffered file writer wrapping BufWriter<File>.
pub struct BufferedFileWriter {
	writer: BufWriter<File>,
	pending_sync: bool,
	/// Shared with the clone of the file `WalManager::sync` fsyncs through.
	gate: Arc<SyncGate>,
	/// Length of the file once everything buffered is written, or `None` if it
	/// could not be read when the writer was created.
	len: Option<u64>,
	/// Fault injection for tests: how many more appends and flushes succeed
	/// before one fails.
	#[cfg(test)]
	fail_after_ops: Option<usize>,
	/// Fault injection for tests: the next cut back of the file fails.
	#[cfg(test)]
	fail_truncate: bool,
	/// Test-only count of the `write(2)` calls that reached the file, see `file_writes`.
	#[cfg(test)]
	file_writes: usize,
}

impl BufferedFileWriter {
	/// Creates a new BufferedFileWriter with the specified buffer size.
	pub fn new(file: File, buffer_size: usize) -> Self {
		let len = file.metadata().ok().map(|m| m.len());
		Self {
			writer: BufWriter::with_capacity(buffer_size, file),
			pending_sync: false,
			gate: Arc::default(),
			len,
			#[cfg(test)]
			fail_after_ops: None,
			#[cfg(test)]
			fail_truncate: false,
			#[cfg(test)]
			file_writes: 0,
		}
	}

	/// Length of the file once everything buffered is written, if known.
	pub(crate) fn len(&self) -> Option<u64> {
		self.len
	}

	/// Whether everything appended has reached the file.
	pub(crate) fn is_flushed(&self) -> bool {
		self.writer.buffer().is_empty()
	}

	/// The gate every fsync of this file goes through.
	pub(crate) fn gate(&self) -> Arc<SyncGate> {
		Arc::clone(&self.gate)
	}

	/// Whether an fsync of the file failed, which ends its use for appends and syncs.
	pub(crate) fn sync_failed(&self) -> bool {
		self.gate.failed()
	}

	/// Test-only: whether anything was appended since the last successful fsync.
	#[cfg(test)]
	pub(crate) fn pending_sync(&self) -> bool {
		self.pending_sync
	}

	/// Drops everything still buffered and cuts the file back to `len`, the end
	/// of the last complete record.
	///
	/// `len` must not be past the end of the file.
	pub(crate) fn truncate(&mut self, len: u64) -> Result<()> {
		#[cfg(test)]
		if std::mem::take(&mut self.fail_truncate) {
			return Err(Error::IO(IOError::new(io::ErrorKind::Other, "injected cut failure")));
		}

		// Rebuild the buffer around the same file. The old one is taken apart rather than
		// dropped, because dropping a `BufWriter` writes out what it still holds.
		let capacity = self.writer.capacity();
		let file = self.writer.get_ref().try_clone()?;
		let stale = std::mem::replace(&mut self.writer, BufWriter::with_capacity(capacity, file));
		drop(stale.into_parts());

		let file = self.writer.get_ref();
		if len > file.metadata()?.len() {
			return Err(Error::IO(IOError::new(
				io::ErrorKind::InvalidInput,
				"refusing to extend the file while truncating it",
			)));
		}
		file.set_len(len)?;
		self.gate.sync(file)?;
		self.len = Some(len);
		self.pending_sync = false;
		Ok(())
	}

	/// Makes the append or flush after the next `ops` of them fail once, as a
	/// full disk would.
	#[cfg(test)]
	pub(crate) fn fail_after_ops(&mut self, ops: usize) {
		self.fail_after_ops = Some(ops);
	}

	/// Makes the next cut back of the file fail once.
	#[cfg(test)]
	pub(crate) fn fail_truncate(&mut self) {
		self.fail_truncate = true;
	}

	/// How many `write(2)` calls have reached the file: one for every flush of a non-empty
	/// buffer, and one more for an append that was too long for the buffer, which makes
	/// `BufWriter` write the buffer out and then the data itself if it is a buffer long.
	#[cfg(test)]
	pub(crate) fn file_writes(&self) -> usize {
		self.file_writes
	}

	#[cfg(test)]
	fn count_flush(&mut self) {
		if !self.writer.buffer().is_empty() {
			self.file_writes += 1;
		}
	}

	#[cfg(test)]
	fn injected_failure(&mut self) -> Result<()> {
		match self.fail_after_ops.as_mut() {
			Some(0) => {
				self.fail_after_ops = None;
				Err(Error::IO(IOError::new(io::ErrorKind::Other, "injected write failure")))
			}
			Some(ops) => {
				*ops -= 1;
				Ok(())
			}
			None => Ok(()),
		}
	}
}

impl WritableFile for BufferedFileWriter {
	fn append(&mut self, data: &[u8]) -> Result<()> {
		#[cfg(test)]
		self.injected_failure()?;
		#[cfg(test)]
		let buffered = self.writer.buffer().len();
		self.writer.write_all(data)?;
		#[cfg(test)]
		if self.writer.buffer().len() != buffered + data.len() {
			self.file_writes +=
				usize::from(buffered > 0) + usize::from(data.len() >= self.writer.capacity());
		}
		self.pending_sync = true;
		if let Some(len) = self.len.as_mut() {
			*len += data.len() as u64;
		}
		Ok(())
	}

	fn flush(&mut self) -> Result<()> {
		#[cfg(test)]
		self.injected_failure()?;
		#[cfg(test)]
		self.count_flush();
		self.writer.flush()?;
		Ok(())
	}

	fn sync(&mut self) -> Result<()> {
		// Ahead of the shortcut: after a failed fsync nothing is synced, whatever is pending.
		if self.gate.failed() {
			return Err(sync_poisoned());
		}
		if !self.pending_sync {
			return Ok(());
		}
		#[cfg(test)]
		self.count_flush();
		self.writer.flush()?;
		self.gate.sync(self.writer.get_ref())?;
		self.pending_sync = false;
		Ok(())
	}

	fn close(&mut self) -> Result<()> {
		self.sync()?;
		Ok(())
	}
}

// ===== Cleanup =====

/// Cleans up old WAL segments based on the minimum log number with unflushed
/// data.
///
/// This function removes all WAL segments with ID < min_wal_number, since they
/// have been flushed to SSTables and are no longer needed. The manifest tracks
/// which WALs have been flushed.
///
/// # Arguments
///
/// * `wal_dir` - The directory containing the WAL segments
/// * `min_wal_number` - The minimum WAL number that contains unflushed data
///
/// # Returns
///
/// A result with the count of removed segments or an error
#[cfg(test)]
pub(crate) fn cleanup_old_segments(wal_dir: &Path, min_wal_number: u64) -> Result<usize> {
	cleanup_old_segments_for(None, wal_dir, min_wal_number)
}

/// Like `cleanup_old_segments`, for the WAL of a database that is encrypted if `context` is given.
///
/// A segment is removed by its number, and without being read: it is below the log number, so the
/// database has no use for it. In an encrypted database that is not enough to know that it is the
/// database's. The segments of another database that shares the key, or one that is not sealed at
/// all, that someone put there, or left behind by a restore from the wrong place, are not looked at
/// by the replay either, which starts at the log number. Each segment is therefore read from its
/// first record (see `sealed::check_first_record`) before it is removed, and one that is not this
/// database's is left where it is and said so, since nothing here knows what it is.
pub(crate) fn cleanup_old_segments_for(
	context: Option<&EncryptionContext>,
	wal_dir: &Path,
	min_wal_number: u64,
) -> Result<usize> {
	// Check if WAL directory exists
	if !wal_dir.exists() {
		return Ok(0);
	}

	// List all segment IDs with .wal extension
	let segment_ids = match list_segment_ids(wal_dir, Some("wal")) {
		Ok(ids) => ids,
		Err(_) => return Ok(0), // No segments to clean
	};

	// If no segments, nothing to clean
	if segment_ids.is_empty() {
		return Ok(0);
	}

	let mut removed_count = 0;

	// Remove all segments older than min_wal_number (already flushed to SST)
	for segment_id in segment_ids {
		if segment_id < min_wal_number {
			let segment_path = wal_dir.join(format!("{segment_id:020}.wal"));

			if let Some(context) = context {
				if let Err(e) = sealed::check_segment_file(context.db_id(), &segment_path) {
					tracing::error!(
						"Kept the WAL segment {segment_id:020}, which is below the log number: \
						 it is not a segment of this database: {e}"
					);
					continue;
				}
			}

			match fs::remove_file(&segment_path) {
				Ok(_) => {
					removed_count += 1;
					tracing::debug!(
						"Removed flushed WAL segment {segment_id:020} (older than min {min_wal_number:020})"
					);
				}
				Err(e) => {
					// Log error but continue trying to remove other segments
					tracing::warn!("Error removing old WAL segment {segment_id}: {e}");
				}
			}
		}
	}

	Ok(removed_count)
}
