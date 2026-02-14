use std::fmt;
use std::fs::{self, read_dir, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::PoisonError;

use crc32fast::Hasher;

pub mod manager;
pub mod reader;
pub mod recovery;
pub mod writer;

pub use manager::Wal;

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

/// Options for configuring a WAL segment.
///
/// This struct provides configuration options for WAL segments, including
/// file permissions, compression settings, metadata, and file size limits.
#[derive(Clone)]
pub struct Options {
	/// The permission mode for creating directories.
	pub(crate) dir_mode: Option<u32>,

	/// The file mode to set for the segment file.
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
			dir_mode: Some(0o750),
			file_mode: Some(DEFAULT_FILE_MODE),
			compression_type: CompressionType::None,
			file_extension: Some("wal".to_string()),
			max_file_size: DEFAULT_FILE_SIZE,
		}
	}
}

impl Options {
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
	#[allow(unused)]
	pub(crate) fn with_max_file_size(mut self, max_file_size: u64) -> Self {
		self.max_file_size = max_file_size;
		self
	}

	/// Sets the compression type.
	#[allow(unused)]
	pub(crate) fn with_compression(mut self, compression_type: CompressionType) -> Self {
		self.compression_type = compression_type;
		self
	}

	/// Sets the file extension.
	#[allow(unused)]
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

#[derive(Debug)]
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
	fn sync(&mut self) -> Result<()>;

	/// Closes the file, flushing and syncing any remaining buffered data.
	fn close(&mut self) -> Result<()>;
}

/// Buffered file writer wrapping BufWriter<File>.
pub struct BufferedFileWriter {
	writer: BufWriter<File>,
	pending_sync: bool,
}

impl BufferedFileWriter {
	/// Creates a new BufferedFileWriter with the specified buffer size.
	pub fn new(file: File, buffer_size: usize) -> Self {
		Self {
			writer: BufWriter::with_capacity(buffer_size, file),
			pending_sync: false,
		}
	}
}

impl WritableFile for BufferedFileWriter {
	fn append(&mut self, data: &[u8]) -> Result<()> {
		self.writer.write_all(data)?;
		self.pending_sync = true;
		Ok(())
	}

	fn flush(&mut self) -> Result<()> {
		self.writer.flush()?;
		Ok(())
	}

	fn sync(&mut self) -> Result<()> {
		if !self.pending_sync {
			return Ok(());
		}
		self.writer.flush()?;
		self.writer.get_ref().sync_all()?;
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
pub(crate) fn cleanup_old_segments(wal_dir: &Path, min_wal_number: u64) -> Result<usize> {
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

			match fs::remove_file(&segment_path) {
				Ok(_) => {
					removed_count += 1;
					log::debug!(
						"Removed flushed WAL segment {segment_id:020} (older than min {min_wal_number:020})"
					);
				}
				Err(e) => {
					// Log error but continue trying to remove other segments
					log::warn!("Error removing old WAL segment {segment_id}: {e}");
				}
			}
		}
	}

	Ok(removed_count)
}
