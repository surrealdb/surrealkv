use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use byteorder::{ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use parking_lot::RwLock;

use crate::error::{Error, Result};
use crate::{vfs, CompressionType, Options, VLogChecksumLevel, Value};

/// VLog format version
pub const VLOG_FORMAT_VERSION: u16 = 1;

/// Size of a ValuePointer when encoded (1 + 4 + 8 + 4 + 4 + 4 bytes)
pub const VALUE_POINTER_SIZE: usize = 25;

/// Bit flags for meta operations
pub const BIT_VALUE_POINTER: u8 = 1; // Set if the value is NOT stored directly next to key
pub const VALUE_LOCATION_VERSION: u8 = 1;
pub const VALUE_POINTER_VERSION: u8 = 1;

/// VLog file header with comprehensive metadata
#[derive(Debug, Clone)]
pub(crate) struct VLogFileHeader {
	pub magic: u32,         // "VLOG"
	pub version: u16,       // Format version
	pub file_id: u32,       // File ID
	pub created_at: u64,    // Creation timestamp
	pub max_file_size: u64, // Maximum file size from options
	pub compression: u8,    // Compression type
	pub reserved: u32,      // Reserved for future use
}

impl VLogFileHeader {
	const MAGIC: u32 = 0x564C_4F47;
	// "VLOG" in hex
	const SIZE: usize = 31;

	// 4 + 2 + 4 + 8 + 8 + 1 + 4 = 31 bytes

	pub(crate) fn new(file_id: u32, max_file_size: u64, compression: u8) -> Self {
		Self {
			magic: Self::MAGIC,
			version: VLOG_FORMAT_VERSION,
			file_id,
			created_at: std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_millis() as u64,
			max_file_size,
			compression,
			reserved: 0,
		}
	}

	pub(crate) fn encode(&self) -> [u8; Self::SIZE] {
		let mut encoded = [0u8; Self::SIZE];
		let mut offset = 0;

		// Magic (4 bytes)
		encoded[offset..offset + 4].copy_from_slice(&self.magic.to_be_bytes());
		offset += 4;

		// Version (2 bytes)
		encoded[offset..offset + 2].copy_from_slice(&self.version.to_be_bytes());
		offset += 2;

		// File ID (4 bytes)
		encoded[offset..offset + 4].copy_from_slice(&self.file_id.to_be_bytes());
		offset += 4;

		// Created at (8 bytes)
		encoded[offset..offset + 8].copy_from_slice(&self.created_at.to_be_bytes());
		offset += 8;

		// Max file size (8 bytes)
		encoded[offset..offset + 8].copy_from_slice(&self.max_file_size.to_be_bytes());
		offset += 8;

		// Compression (1 byte)
		encoded[offset] = self.compression;
		offset += 1;

		// Reserved (4 bytes)
		encoded[offset..offset + 4].copy_from_slice(&self.reserved.to_be_bytes());

		encoded
	}

	pub(crate) fn decode(data: &[u8]) -> Result<Self> {
		if data.len() != Self::SIZE {
			return Err(Error::Corruption(format!(
				"Invalid VLog header size: expected {} bytes, got {} bytes",
				Self::SIZE,
				data.len()
			)));
		}

		let mut offset = 0;

		// Magic (4 bytes)
		let magic = u32::from_be_bytes([
			data[offset],
			data[offset + 1],
			data[offset + 2],
			data[offset + 3],
		]);
		offset += 4;

		if magic != Self::MAGIC {
			return Err(Error::Corruption(format!(
				"Invalid VLog magic number: expected 0x{:08X}, got 0x{:08X}",
				Self::MAGIC,
				magic
			)));
		}

		// Version (2 bytes)
		let version = u16::from_be_bytes([data[offset], data[offset + 1]]);
		offset += 2;

		// File ID (4 bytes)
		let file_id = u32::from_be_bytes([
			data[offset],
			data[offset + 1],
			data[offset + 2],
			data[offset + 3],
		]);
		offset += 4;

		// Created at (8 bytes)
		let created_at = u64::from_be_bytes([
			data[offset],
			data[offset + 1],
			data[offset + 2],
			data[offset + 3],
			data[offset + 4],
			data[offset + 5],
			data[offset + 6],
			data[offset + 7],
		]);
		offset += 8;

		// Max file size (8 bytes)
		let max_file_size = u64::from_be_bytes([
			data[offset],
			data[offset + 1],
			data[offset + 2],
			data[offset + 3],
			data[offset + 4],
			data[offset + 5],
			data[offset + 6],
			data[offset + 7],
		]);
		offset += 8;

		// Compression (1 byte)
		let compression = data[offset];
		offset += 1;

		// Reserved (4 bytes)
		let reserved = u32::from_be_bytes([
			data[offset],
			data[offset + 1],
			data[offset + 2],
			data[offset + 3],
		]);

		Ok(Self {
			magic,
			version,
			file_id,
			created_at,
			max_file_size,
			compression,
			reserved,
		})
	}

	pub(crate) fn is_compatible(&self) -> bool {
		self.version == VLOG_FORMAT_VERSION
	}

	pub(crate) fn validate(&self, file_id: u32) -> Result<()> {
		if self.file_id != file_id {
			return Err(Error::Corruption(format!(
				"File ID mismatch: expected {}, got {}",
				file_id, self.file_id
			)));
		}

		Ok(())
	}
}
/// ValueLocation represents the value info that can be associated with a key,
/// including the internal Meta field for various flags and operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValueLocation {
	/// Meta flags for various operations (value pointer etc.)
	pub meta: u8,
	/// The actual value data
	pub value: Value,
	/// Version number for the value
	pub version: u8,
}

impl ValueLocation {
	/// Creates a new ValueLocation
	pub(crate) fn new(meta: u8, value: Value, version: u8) -> Self {
		Self {
			meta,
			value,
			version,
		}
	}

	/// Creates a ValueLocation that points to a value in VLog
	pub(crate) fn with_pointer(pointer: ValuePointer) -> Self {
		let encoded_pointer = pointer.encode();
		Self::new(BIT_VALUE_POINTER, encoded_pointer, VALUE_LOCATION_VERSION)
	}

	/// Creates a ValueLocation with inline value
	pub(crate) fn with_inline_value(value: Value) -> Self {
		Self::new(0, value, VALUE_LOCATION_VERSION)
	}

	/// Checks if the value is a pointer to VLog
	pub(crate) fn is_value_pointer(&self) -> bool {
		(self.meta & BIT_VALUE_POINTER) != 0
	}

	/// Calculates the encoded size of this ValueLocation
	pub(crate) fn encoded_size(&self) -> usize {
		// meta (1 byte) + version (1 byte) + value length
		1 + 1 + self.value.len()
	}

	/// Encodes the ValueLocation into a byte vector
	pub(crate) fn encode(&self) -> Vec<u8> {
		let mut encoded = Vec::with_capacity(self.encoded_size());
		self.encode_into(&mut encoded).unwrap();
		encoded
	}

	/// Encodes the ValueLocation into a writer
	pub(crate) fn encode_into<W: Write>(&self, writer: &mut W) -> Result<()> {
		// Write meta byte
		writer.write_u8(self.meta)?;

		// Write version as u8
		writer.write_u8(self.version)?;

		// Write value
		writer.write_all(&self.value)?;

		Ok(())
	}

	/// Decodes a ValueLocation from bytes
	pub(crate) fn decode(data: &[u8]) -> Result<Self> {
		let mut cursor = Cursor::new(data);
		Self::decode_from(&mut cursor)
	}

	/// Decodes a ValueLocation from a reader
	fn decode_from<R: Read>(reader: &mut R) -> Result<Self> {
		// Read meta byte
		let meta = reader.read_u8()?;

		// Read version as u8
		let version = reader.read_u8()?;

		// Read remaining data as value
		let mut value = Vec::new();
		reader.read_to_end(&mut value)?;

		Ok(Self::new(meta, value, version))
	}

	/// Resolves the actual value, handling both inline and pointer cases
	// TODO:: Check if this pattern copies the value unnecessarily.
	pub(crate) fn resolve_value(self, vlog: Option<&Arc<VLog>>) -> Result<Value> {
		if self.is_value_pointer() {
			if let Some(vlog) = vlog {
				let pointer = ValuePointer::decode(&self.value)?;
				vlog.get(&pointer).map_err(|e| {
					Error::Other(format!(
						"Failed to resolve value from VLog: {e}. ValuePointer: {:?}",
						pointer
					))
				})
			} else {
				Err(Error::Other("VLog not available for pointer resolution".to_string()))
			}
		} else {
			// Value is stored inline
			Ok(self.value)
		}
	}
}

/// A pointer to a value stored in the value log
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ValuePointer {
	/// Version of the ValuePointer format
	pub version: u8,
	/// ID of the VLog file containing the value
	pub file_id: u32,
	/// Offset within the file where the value starts
	pub offset: u64,
	/// Size of the key in bytes
	pub key_size: u32,
	/// Size of the value in bytes
	pub value_size: u32,
	/// CRC32 checksum of the value for integrity verification
	pub checksum: u32,
}

impl ValuePointer {
	/// Creates a new ValuePointer
	pub(crate) fn new(
		file_id: u32,
		offset: u64,
		key_size: u32,
		value_size: u32,
		checksum: u32,
	) -> Self {
		Self {
			version: VALUE_POINTER_VERSION,
			file_id,
			offset,
			key_size,
			value_size,
			checksum,
		}
	}

	/// Encodes the pointer as bytes for storage
	pub(crate) fn encode(&self) -> Vec<u8> {
		let mut encoded = Vec::with_capacity(VALUE_POINTER_SIZE);
		encoded.push(self.version);
		encoded.extend_from_slice(&self.file_id.to_be_bytes());
		encoded.extend_from_slice(&self.offset.to_be_bytes());
		encoded.extend_from_slice(&self.key_size.to_be_bytes());
		encoded.extend_from_slice(&self.value_size.to_be_bytes());
		encoded.extend_from_slice(&self.checksum.to_be_bytes());
		encoded
	}

	/// Decodes a pointer from bytes
	pub(crate) fn decode(data: &[u8]) -> Result<Self> {
		if data.len() != VALUE_POINTER_SIZE {
			return Err(Error::Corruption(format!(
				"Invalid ValuePointer size: expected {} bytes, got {} bytes",
				VALUE_POINTER_SIZE,
				data.len()
			)));
		}

		let version = data[0];
		let file_id = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
		let offset = u64::from_be_bytes([
			data[5], data[6], data[7], data[8], data[9], data[10], data[11], data[12],
		]);
		let key_size = u32::from_be_bytes([data[13], data[14], data[15], data[16]]);
		let value_size = u32::from_be_bytes([data[17], data[18], data[19], data[20]]);
		let checksum = u32::from_be_bytes([data[21], data[22], data[23], data[24]]);

		Ok(Self {
			version,
			file_id,
			offset,
			key_size,
			value_size,
			checksum,
		})
	}

	/// Calculates the total entry size (header + key + value + crc32)
	pub(crate) fn total_entry_size(&self) -> u64 {
		8 + self.key_size as u64 + self.value_size as u64 + 4
	}
}

/// Represents a VLog file that can be read from
#[derive(Debug)]
pub(crate) struct VLogFile {
	/// File path
	pub path: PathBuf,
}

impl VLogFile {
	fn new(_id: u32, path: PathBuf, _size: u64) -> Self {
		Self {
			path,
		}
	}
}

/// Writer for a single VLog file
pub(crate) struct VLogWriter {
	/// Buffered writer for the file
	writer: BufWriter<File>,
	/// Cloned file descriptor for fsync outside the write lock.
	/// Points to the same inode as the BufWriter's file.
	sync_fd: Arc<File>,
	/// Current offset in the file
	pub(crate) current_offset: u64,
	/// ID of this VLog file
	file_id: u32,
	/// Total bytes written to this file
	bytes_written: u64,
}

impl VLogWriter {
	/// Creates a new VLogWriter for the specified file
	pub(crate) fn new(
		path: &Path,
		file_id: u32,
		max_file_size: u64,
		compression: u8,
	) -> Result<Self> {
		let file_exists = path.exists();
		let file = OpenOptions::new().create(true).append(true).open(path)?;

		// Clone the fd before BufWriter consumes ownership. This cloned fd
		// is used to perform fsync outside the write lock.
		let sync_fd = Arc::new(file.try_clone()?);

		let current_offset = file.metadata()?.len();
		let mut writer = BufWriter::new(file);

		// If this is a new file, write the header
		if !file_exists || current_offset == 0 {
			let header = VLogFileHeader::new(file_id, max_file_size, compression);
			let header_bytes = header.encode();
			writer.write_all(&header_bytes)?;
			writer.flush()?;
		}

		// Update offset to account for header
		let current_offset = if !file_exists || current_offset == 0 {
			VLogFileHeader::SIZE as u64
		} else {
			current_offset
		};

		Ok(Self {
			writer,
			sync_fd,
			current_offset,
			file_id,
			bytes_written: current_offset,
		})
	}

	/// Appends a key+value entry to the file and returns a pointer to it
	/// Layout: +--------+-----+-------+-------+
	///         | header  | key | value | crc32 |
	///         +--------+-----+-------+--------+
	pub(crate) fn append(&mut self, key: &[u8], value: &[u8]) -> Result<ValuePointer> {
		let key_len = key.len() as u32;
		let value_len = value.len() as u32;
		let offset = self.current_offset;

		// Calculate CRC32 of key + value
		let mut hasher = Hasher::new();
		hasher.update(key);
		hasher.update(value);
		let crc32 = hasher.finalize();

		// Write header: [key_len: 4 bytes][value_len: 4 bytes]
		self.writer.write_all(&key_len.to_be_bytes())?;
		self.writer.write_all(&value_len.to_be_bytes())?;

		// Write key
		self.writer.write_all(key)?;

		// Write value
		self.writer.write_all(value)?;

		// Write CRC32
		self.writer.write_all(&crc32.to_be_bytes())?;

		let entry_size = 8 + key.len() as u64 + value.len() as u64 + 4; // header + key + value + crc32
		self.current_offset += entry_size;
		self.bytes_written += entry_size;

		Ok(ValuePointer::new(self.file_id, offset, key_len, value_len, crc32))
	}

	/// Flushes and syncs the writer (flush to OS cache + fsync to disk).
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn sync(&mut self) -> Result<()> {
		self.writer.flush()?;
		self.writer.get_ref().sync_all()?;
		Ok(())
	}

	/// Flushes buffered data to OS cache (not to disk).
	pub(crate) fn flush(&mut self) -> Result<()> {
		self.writer.flush()?;
		Ok(())
	}

	/// Returns a clone of the sync file descriptor Arc for
	/// performing fsync outside the write lock.
	pub(crate) fn sync_fd(&self) -> Arc<File> {
		Arc::clone(&self.sync_fd)
	}

	/// Gets the current size of the file
	fn size(&self) -> u64 {
		self.current_offset
	}
}

/// Value Log (VLog) for WiscKey-style key-value separation with GC
///
/// This implementation includes:
/// - Iterator reference counting for safe garbage collection
/// - Discard statistics tracking for intelligent GC candidate selection
/// - Atomic file replacement during compaction
/// - LSM integration for staleness detection
pub(crate) struct VLog {
	/// Base directory for VLog files
	path: PathBuf,

	/// Maximum size for each VLog file before rotation
	max_file_size: u64,

	/// Checksum verification level
	checksum_level: VLogChecksumLevel,

	/// Next file ID to be assigned
	pub(crate) next_file_id: AtomicU32,

	/// ID of the file currently open for writing
	pub(crate) active_writer_id: AtomicU32,

	/// Writer for the current active file
	pub(crate) writer: RwLock<Option<VLogWriter>>,

	/// Maps file_id to VLogFile metadata
	files_map: RwLock<HashMap<u32, Arc<VLogFile>>>,

	/// Cached open file handles for reading
	pub(crate) file_handles: RwLock<HashMap<u32, Arc<File>>>,

	/// Options for VLog configuration
	pub(crate) opts: Arc<Options>,
}

impl VLog {
	/// Returns the file path for a VLog file with the given ID
	pub(crate) fn vlog_file_path(&self, file_id: u32) -> PathBuf {
		self.opts.vlog_file_path(file_id as u64)
	}

	/// Creates a new VLog instance
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		let vlog = Self {
			path: opts.vlog_dir(),
			max_file_size: opts.vlog_max_file_size,
			checksum_level: opts.vlog_checksum_verification,
			next_file_id: AtomicU32::new(1),
			active_writer_id: AtomicU32::new(0),
			writer: RwLock::new(None),
			files_map: RwLock::new(HashMap::new()),
			file_handles: RwLock::new(HashMap::new()),
			opts,
		};

		// PRE-FILL ALL EXISTING FILE HANDLES ON STARTUP
		vlog.prefill_file_handles()?;

		Ok(vlog)
	}

	/// Appends a key+value pair to the log and returns a ValuePointer
	pub(crate) fn append(&self, key: &[u8], value: &[u8]) -> Result<ValuePointer> {
		// Ensure we have a writer
		let _new_file_created = {
			let mut writer = self.writer.write();

			if writer.is_none() || writer.as_ref().unwrap().size() >= self.max_file_size {
				// Create new file
				let file_id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
				let file_path = self.vlog_file_path(file_id);
				let new_writer = VLogWriter::new(
					&file_path,
					file_id,
					self.opts.vlog_max_file_size,
					CompressionType::None as u8,
				)?;

				// Register the new file for GC safety
				self.register_vlog_file(file_id, file_path, 0); // Start with size 0

				// Update the active writer ID
				self.active_writer_id.store(file_id, Ordering::SeqCst);

				*writer = Some(new_writer);
				true
			} else {
				false
			}
		};

		// Now append the key+value pair
		let mut writer = self.writer.write();
		let writer = writer.as_mut().unwrap();

		let pointer = writer.append(key, value)?;

		Ok(pointer)
	}

	/// Pre-fills the file_handles cache with all existing VLog files
	fn prefill_file_handles(&self) -> Result<()> {
		let entries = match std::fs::read_dir(&self.path) {
			Ok(entries) => entries,
			Err(_) => return Ok(()), // Directory doesn't exist yet
		};

		let mut max_file_id: Option<u32> = None;
		let mut max_file_path: Option<PathBuf> = None;
		let mut file_handles = self.file_handles.write();
		let mut files_map = self.files_map.write();

		for entry in entries {
			let entry = entry?;
			let file_name = entry.file_name();
			let file_name_str = file_name.to_string_lossy();

			// Look for VLog files
			if let Some(file_id) = self.opts.extract_vlog_file_id(&file_name_str) {
				let file_path = entry.path();
				let file_size = entry.metadata()?.len();

				// Track the file with maximum ID for active writer setup
				if max_file_id.is_none_or(|current_max| file_id >= current_max) {
					max_file_id = Some(file_id);
					max_file_path = Some(file_path.clone());
				}

				// Pre-open the file handle and validate header
				match File::open(&file_path) {
					Ok(file) => {
						// Validate file header for existing files
						if file_size > 0 {
							let mut header_data = vec![0u8; VLogFileHeader::SIZE];
							vfs::File::read_at(&file, 0, &mut header_data)?;
							let header = VLogFileHeader::decode(&header_data)?;
							header.validate(file_id)?;
						}

						let handle = Arc::new(file);
						file_handles.insert(file_id, handle);

						// Also register in files_map
						files_map.insert(
							file_id,
							Arc::new(VLogFile::new(file_id, file_path, file_size)),
						);
					}
					Err(e) => {
						log::error!("Failed to pre-open VLog file {file_name_str}: {e}");
						return Err(Error::Io(Arc::new(e)));
					}
				}
			}
		}

		// Set next_file_id based on whether we found existing files
		if let Some(highest_file_id) = max_file_id {
			// Found existing files, set next_file_id to one past the highest existing file
			// ID
			self.next_file_id.store(highest_file_id + 1, Ordering::SeqCst);

			// Set the last file up as the active writer
			let writer = VLogWriter::new(
				&max_file_path.unwrap(),
				highest_file_id,
				self.opts.vlog_max_file_size,
				CompressionType::None as u8,
			)?;
			self.active_writer_id.store(highest_file_id, Ordering::SeqCst);
			*self.writer.write() = Some(writer);
		}

		// println!(
		//     "Pre-filled {} VLog file handles, next_file_id set to {}",
		//     file_handles.len(),
		//     max_file_id + 1
		// );

		Ok(())
	}

	/// Retrieves (and caches) an open file handle for the given file ID
	fn get_file_handle(&self, file_id: u32) -> Result<Arc<File>> {
		{
			let handles = self.file_handles.read();
			if let Some(handle) = handles.get(&file_id) {
				return Ok(Arc::clone(handle));
			}
		}

		// Open and validate the file
		let file_path = self.vlog_file_path(file_id);
		let file = File::open(&file_path).map_err(|e| {
			log::error!(
				"Failed to open VLog file: file_id={}, path={:?}, error={}",
				file_id,
				file_path,
				e
			);
			Error::Other(format!(
				"Failed to open VLog file (file_id={}, path={:?}): {}",
				file_id, file_path, e
			))
		})?;

		// Validate file header for existing files
		let file_size = file
			.metadata()
			.map_err(|e| {
				log::error!(
					"Failed to get VLog file metadata: file_id={}, path={:?}, error={}",
					file_id,
					file_path,
					e
				);
				Error::Other(format!(
					"Failed to get VLog file metadata (file_id={}, path={:?}): {}",
					file_id, file_path, e
				))
			})?
			.len();

		if file_size > 0 {
			// Only validate header if file has content (not a new empty file)
			let mut header_data = vec![0u8; VLogFileHeader::SIZE];
			vfs::File::read_at(&file, 0, &mut header_data).map_err(|e| {
				log::error!(
					"Failed to read VLog header: file_id={}, path={:?}, error={}",
					file_id,
					file_path,
					e
				);
				Error::Other(format!(
					"Failed to read VLog header (file_id={}, path={:?}): {}",
					file_id, file_path, e
				))
			})?;

			let header = VLogFileHeader::decode(&header_data).map_err(|e| {
				log::error!(
					"Failed to decode VLog header: file_id={}, path={:?}, error={}",
					file_id,
					file_path,
					e
				);
				Error::Other(format!(
					"Failed to decode VLog header (file_id={}, path={:?}): {}",
					file_id, file_path, e
				))
			})?;

			if !header.is_compatible() {
				log::error!(
					"Incompatible VLog file version: file_id={}, path={:?}, version={} (expected {})",
					file_id, file_path, header.version, VLOG_FORMAT_VERSION
				);
				return Err(Error::Corruption(format!(
					"Incompatible VLog file version (file_id={}, path={:?}): {} (expected {})",
					file_id, file_path, header.version, VLOG_FORMAT_VERSION
				)));
			}

			// Validate header against current options
			header.validate(file_id).map_err(|e| {
				log::error!(
					"VLog header validation failed: file_id={}, path={:?}, error={}",
					file_id,
					file_path,
					e
				);
				Error::Other(format!(
					"VLog header validation failed (file_id={}, path={:?}): {}",
					file_id, file_path, e
				))
			})?;
		}

		let handle = Arc::new(file);

		let mut handles = self.file_handles.write();
		// Another thread might have inserted it while we were opening the file
		let entry = handles.entry(file_id).or_insert_with(|| Arc::clone(&handle));
		Ok(Arc::clone(entry))
	}

	/// Retrieves a value using a ValuePointer
	pub(crate) fn get(&self, pointer: &ValuePointer) -> Result<Value> {
		// Check unified block cache first
		if let Some(cached_value) = self.opts.block_cache.get_vlog(pointer.file_id, pointer.offset)
		{
			return Ok(cached_value);
		}

		let file = self.get_file_handle(pointer.file_id)?;

		// Read the entire entry in a single call using sizes from the pointer
		let key_len = pointer.key_size;
		let value_len = pointer.value_size;
		let total_size = pointer.total_entry_size();

		let mut entry_data_vec = vec![0u8; total_size as usize];
		vfs::File::read_at(&*file, pointer.offset, &mut entry_data_vec).map_err(|e| {
			log::error!("Failed to read VLog entry: pointer={:?}, error={}", pointer, e);
			Error::Other(format!(
				"Failed to read VLog entry (file_id={}, offset={}, key_size={}, value_size={}): {}",
				pointer.file_id, pointer.offset, pointer.key_size, pointer.value_size, e
			))
		})?;

		// Verify header sizes
		let header_key_len = u32::from_be_bytes([
			entry_data_vec[0],
			entry_data_vec[1],
			entry_data_vec[2],
			entry_data_vec[3],
		]);
		let header_value_len = u32::from_be_bytes([
			entry_data_vec[4],
			entry_data_vec[5],
			entry_data_vec[6],
			entry_data_vec[7],
		]);

		if header_key_len != key_len || header_value_len != value_len {
			log::error!(
				"VLog header size mismatch: pointer={:?}, header_key_len={}, header_value_len={}",
				pointer,
				header_key_len,
				header_value_len
			);
			return Err(Error::Corruption(format!(
				"Header size mismatch (file_id={}, offset={}): expected key {} value {}, got key {} value {}",
				pointer.file_id, pointer.offset, key_len, value_len, header_key_len, header_value_len
			)));
		}

		// Parse offsets from the buffer
		let key_start = 8;
		let value_start = key_start + key_len as usize;
		let crc_start = value_start + value_len as usize;

		let stored_crc32 = u32::from_be_bytes([
			entry_data_vec[crc_start],
			entry_data_vec[crc_start + 1],
			entry_data_vec[crc_start + 2],
			entry_data_vec[crc_start + 3],
		]);

		// Checksum verification
		if self.checksum_level != VLogChecksumLevel::Disabled && stored_crc32 != pointer.checksum {
			log::error!(
				"VLog CRC32 mismatch (stored vs pointer): pointer={:?}, stored_crc32={}",
				pointer,
				stored_crc32
			);
			return Err(Error::Corruption(format!(
				"CRC32 mismatch (file_id={}, offset={}): expected {}, got {} (stored in file)",
				pointer.file_id, pointer.offset, pointer.checksum, stored_crc32
			)));
		}

		if self.checksum_level == VLogChecksumLevel::Full {
			let key = &entry_data_vec[key_start..value_start];
			let value = &entry_data_vec[value_start..crc_start];
			let mut hasher = Hasher::new();
			hasher.update(key);
			hasher.update(value);
			let calculated_crc32 = hasher.finalize();
			if calculated_crc32 != pointer.checksum {
				log::error!(
					"VLog Key+Value CRC32 mismatch: pointer={:?}, calculated_crc32={}",
					pointer,
					calculated_crc32
				);
				return Err(Error::Corruption(format!(
					"Key+Value CRC32 mismatch (file_id={}, offset={}): expected {}, calculated {}",
					pointer.file_id, pointer.offset, pointer.checksum, calculated_crc32
				)));
			}
		}

		// Extract value slice from entry data
		let value_bytes = entry_data_vec[value_start..crc_start].to_vec();

		// Cache the value in unified block cache for future reads
		self.opts.block_cache.insert_vlog(pointer.file_id, pointer.offset, value_bytes.clone());

		Ok(value_bytes)
	}

	/// Cleans up obsolete vlog files based on the global minimum oldest_vlog_file_id.
	///
	/// A vlog file is safe to delete when:
	/// - Its file_id < min_oldest_vlog (no SST references values in it)
	/// - It is not the active writer
	/// - No iterators are active
	///
	/// This implements the "global minimum" GC approach where files are deleted
	/// once no SST can possibly reference them. If iterators are active, cleanup
	/// is skipped and will be retried on the next GC trigger.
	pub(crate) fn cleanup_obsolete_files(&self, min_oldest_vlog: u32) -> Result<()> {
		let active = self.active_writer_id.load(Ordering::SeqCst);

		// Collect files that are safe to delete
		let to_delete: Vec<u32> = {
			let files_map = self.files_map.read();
			files_map.keys().filter(|&&id| id < min_oldest_vlog && id != active).copied().collect()
		};

		if to_delete.is_empty() {
			return Ok(());
		}

		// No active iterators, safe to delete
		let mut files_map = self.files_map.write();
		let mut file_handles = self.file_handles.write();

		for file_id in to_delete {
			file_handles.remove(&file_id);
			if let Some(vlog_file) = files_map.remove(&file_id) {
				if let Err(e) = std::fs::remove_file(&vlog_file.path) {
					log::error!(
						"Failed to delete obsolete VLog file: file_id={}, path={:?}, error={}",
						file_id,
						vlog_file.path,
						e
					);
					// Continue with other files, don't fail the entire cleanup
				} else {
					log::info!(
						"Deleted obsolete VLog file: file_id={}, path={:?}",
						file_id,
						vlog_file.path
					);
				}
			}
		}

		Ok(())
	}

	/// Syncs all data to disk.
	///
	/// The write lock is held only for the BufWriter flush (draining the
	/// internal buffer to OS page cache, ~microseconds). The expensive
	/// fsync is performed outside the lock using a pre-cloned file
	/// descriptor, allowing concurrent VLog appends to proceed.
	pub(crate) fn sync(&self) -> Result<()> {
		// Phase 1: Under write lock — flush BufWriter and grab sync fd
		let sync_fd = {
			let mut guard = self.writer.write();
			if let Some(ref mut writer) = *guard {
				writer.flush()?;
				Some(writer.sync_fd())
			} else {
				None
			}
			// write lock released here
		};

		// Phase 2: Outside lock — fsync to disk (slow, doesn't block appends)
		if let Some(fd) = sync_fd {
			fd.sync_all()?;
		}
		Ok(())
	}

	/// Registers a VLog file in the files map for tracking
	fn register_vlog_file(&self, file_id: u32, path: PathBuf, size: u64) {
		let mut files_map = self.files_map.write();
		files_map.insert(file_id, Arc::new(VLogFile::new(file_id, path, size)));
	}

	pub(crate) fn close(&self) -> Result<()> {
		self.sync()?;
		Ok(())
	}
}
