use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
use std::sync::Arc;

use byteorder::{ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use crc32fast::Hasher;
use parking_lot::{Mutex, RwLock};

use crate::batch::Batch;
use crate::bplustree::tree::DiskBPlusTree;
use crate::commit::CommitPipeline;
use crate::discard::DiscardStats;
use crate::error::{Error, Result};
use crate::sstable::InternalKey;
use crate::{vfs, CompressionType, Options, Tree, TreeBuilder, VLogChecksumLevel, Value};

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
			return Err(Error::Corruption("Invalid header size".to_string()));
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
			return Err(Error::Corruption("Invalid magic number".to_string()));
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
		Self::new(BIT_VALUE_POINTER, Bytes::from(encoded_pointer), VALUE_LOCATION_VERSION)
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
	fn encode_into<W: Write>(&self, writer: &mut W) -> Result<()> {
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

		Ok(Self::new(meta, Bytes::from(value), version))
	}

	/// Resolves the actual value, handling both inline and pointer cases
	// TODO:: Check if this pattern copies the value unnecessarily.
	pub(crate) fn resolve_value(self, vlog: Option<&Arc<VLog>>) -> Result<Value> {
		if self.is_value_pointer() {
			if let Some(vlog) = vlog {
				let pointer = ValuePointer::decode(&self.value)?;
				vlog.get(&pointer)
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
			return Err(Error::Corruption("Invalid ValuePointer size".to_string()));
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
struct VLogWriter {
	/// Buffered writer for the file
	writer: BufWriter<File>,
	/// Current offset in the file
	current_offset: u64,
	/// ID of this VLog file
	file_id: u32,
	/// Total bytes written to this file
	bytes_written: u64,
}

impl VLogWriter {
	/// Creates a new VLogWriter for the specified file
	fn new(path: &Path, file_id: u32, max_file_size: u64, compression: u8) -> Result<Self> {
		let file_exists = path.exists();
		let file = OpenOptions::new().create(true).append(true).open(path)?;

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
			current_offset,
			file_id,
			bytes_written: current_offset,
		})
	}

	/// Appends a key+value entry to the file and returns a pointer to it
	/// Layout: +--------+-----+-------+-------+
	///         | header  | key | value | crc32 |
	///         +--------+-----+-------+--------+
	fn append(&mut self, key: &[u8], value: &[u8]) -> Result<ValuePointer> {
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

	/// Flushes and syncs the writer
	fn sync(&mut self) -> Result<()> {
		self.writer.flush()?;
		self.writer.get_ref().sync_all()?;
		Ok(())
	}

	fn flush(&mut self) -> Result<()> {
		self.writer.flush()?;
		Ok(())
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

	/// Discard ratio threshold for triggering garbage collection (0.0 - 1.0)
	gc_discard_ratio: f64,

	/// Next file ID to be assigned
	next_file_id: AtomicU32,

	/// ID of the file currently open for writing
	active_writer_id: AtomicU32,

	/// Writer for the current active file
	writer: RwLock<Option<VLogWriter>>,

	/// Number of active iterators (for GC safety)
	num_active_iterators: AtomicI32,

	/// Maps file_id to VLogFile metadata
	files_map: RwLock<HashMap<u32, Arc<VLogFile>>>,

	/// Cached open file handles for reading
	file_handles: RwLock<HashMap<u32, Arc<File>>>,

	/// Files marked for deletion but waiting for iterators to finish
	files_to_be_deleted: RwLock<Vec<u32>>,

	/// Prevents concurrent garbage collection
	gc_in_progress: AtomicBool,

	/// Discard statistics for GC candidate selection
	discard_stats: Mutex<DiscardStats>,

	/// Global delete list LSM tree: tracks <stale_seqno, value_size> pairs
	/// across all segments
	pub(crate) delete_list: Arc<DeleteList>,

	/// Options for VLog configuration
	opts: Arc<Options>,

	/// Reference to versioned index for atomic cleanup during GC
	versioned_index: Option<Arc<RwLock<DiskBPlusTree>>>,
}

impl VLog {
	/// Returns the file path for a VLog file with the given ID
	fn vlog_file_path(&self, file_id: u32) -> PathBuf {
		self.opts.vlog_file_path(file_id as u64)
	}

	/// Creates a new VLog instance
	pub(crate) fn new(
		opts: Arc<Options>,
		versioned_index: Option<Arc<RwLock<DiskBPlusTree>>>,
	) -> Result<Self> {
		// Initialize the global delete list tree
		let delete_list = Arc::new(DeleteList::new(opts.delete_list_dir())?);

		let vlog = Self {
			path: opts.vlog_dir(),
			max_file_size: opts.vlog_max_file_size,
			checksum_level: opts.vlog_checksum_verification,
			gc_discard_ratio: opts.vlog_gc_discard_ratio,
			next_file_id: AtomicU32::new(0),
			active_writer_id: AtomicU32::new(0),
			writer: RwLock::new(None),
			num_active_iterators: AtomicI32::new(0),
			files_map: RwLock::new(HashMap::new()),
			file_handles: RwLock::new(HashMap::new()),
			files_to_be_deleted: RwLock::new(Vec::new()),
			gc_in_progress: AtomicBool::new(false),
			discard_stats: Mutex::new(DiscardStats::new(opts.discard_stats_dir())?),
			delete_list,
			opts,
			versioned_index,
		};

		// PRE-FILL ALL EXISTING FILE HANDLES ON STARTUP
		vlog.prefill_file_handles()?;

		// Clean up any leftover temporary files from previous interrupted compactions
		vlog.cleanup_temp_files()?;

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
		let file = File::open(&file_path)?;

		// Validate file header for existing files
		let file_size = file.metadata()?.len();
		if file_size > 0 {
			// Only validate header if file has content (not a new empty file)
			let mut header_data = vec![0u8; VLogFileHeader::SIZE];
			vfs::File::read_at(&file, 0, &mut header_data)?;

			let header = VLogFileHeader::decode(&header_data)?;
			if !header.is_compatible() {
				return Err(Error::Corruption(format!(
					"Incompatible VLog file version: {} (expected {})",
					header.version, VLOG_FORMAT_VERSION
				)));
			}

			// Validate header against current options
			header.validate(file_id)?;
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
		vfs::File::read_at(&*file, pointer.offset, &mut entry_data_vec)?;

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
			return Err(Error::Corruption(format!(
                "Header size mismatch: expected key {key_len} value {value_len}, got key {header_key_len} value {header_value_len}"
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
			return Err(Error::Corruption(format!(
				"CRC32 mismatch: expected {}, got {}",
				pointer.checksum, stored_crc32
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
				return Err(Error::Corruption(format!(
					"Key+Value CRC32 mismatch: expected {}, got {}",
					pointer.checksum, calculated_crc32
				)));
			}
		}

		// Extract value slice from entry data
		let value_bytes = Bytes::copy_from_slice(&entry_data_vec[value_start..crc_start]);

		// Cache the value in unified block cache for future reads
		self.opts.block_cache.insert_vlog(pointer.file_id, pointer.offset, value_bytes.clone());

		Ok(value_bytes)
	}

	/// Increments the active iterator count when a transaction/iterator starts
	pub(crate) fn incr_iterator_count(&self) {
		self.num_active_iterators.fetch_add(1, Ordering::SeqCst);
	}

	/// Decrements the active iterator count when a transaction/iterator ends
	/// If count reaches zero, processes deferred file deletions
	pub(crate) fn decr_iterator_count(&self) -> Result<()> {
		let count = self.num_active_iterators.fetch_sub(1, Ordering::SeqCst);

		if count == 1 {
			// We were the last iterator, safe to delete pending files
			self.delete_pending_files()?;
		}

		Ok(())
	}

	/// Gets the current number of active iterators  
	pub(crate) fn iterator_count(&self) -> i32 {
		self.num_active_iterators.load(Ordering::SeqCst)
	}

	/// Deletes files that were marked for deletion when no iterators were
	/// active
	fn delete_pending_files(&self) -> Result<()> {
		let mut files_to_delete = self.files_to_be_deleted.write();
		let mut files_map = self.files_map.write();
		let mut file_handles = self.file_handles.write();

		for file_id in files_to_delete.drain(..) {
			file_handles.remove(&file_id);
			if let Some(vlog_file) = files_map.remove(&file_id) {
				if let Err(e) = std::fs::remove_file(&vlog_file.path) {
					log::error!("Failed to delete VLog file {:?}: {}", vlog_file.path, e);
					return Err(Error::Io(Arc::new(e)));
				}
			}
		}

		Ok(())
	}

	/// Selects files based on discard statistics and compacts them
	/// Attempts to compact the best candidate file (highest discard bytes that
	/// meets criteria) Only processes one file per invocation
	pub async fn garbage_collect(&self, commit_pipeline: Arc<CommitPipeline>) -> Result<Vec<u32>> {
		// Try to set the gc_in_progress flag to true, if it's already true, return
		// error
		if self
			.gc_in_progress
			.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
			.is_err()
		{
			// GC already in progress
			return Err(Error::VlogGCAlreadyInProgress);
		}

		// Make sure we reset the flag when we're done
		let _guard = scopeguard::guard((), |_| {
			self.gc_in_progress.store(false, Ordering::SeqCst);
		});

		// Get all files with discardable bytes, sorted by discard bytes (descending)
		let candidates = {
			let discard_stats = self.discard_stats.lock();
			discard_stats.get_gc_candidates()
		};

		if candidates.is_empty() {
			return Ok(vec![]);
		}

		// Get active writer ID once
		let active_writer_id = self.active_writer_id.load(Ordering::SeqCst);

		// Find the best candidate to compact (first one that meets all criteria)
		let candidate_to_compact = candidates.into_iter().find(|(file_id, discard_bytes)| {
			// Skip if no discard bytes
			if *discard_bytes == 0 {
				return false;
			}

			// Check if this file meets the discard ratio threshold
			let (total_size, _, discard_ratio) = self.get_file_stats(*file_id);
			if total_size == 0 || discard_ratio < self.gc_discard_ratio {
				return false;
			}

			// Skip if this is the active writer
			if *file_id == active_writer_id {
				return false;
			}

			// Skip if file is already marked for deletion
			let files_to_delete = self.files_to_be_deleted.read();
			if files_to_delete.contains(file_id) {
				return false;
			}

			true
		});

		// If we found a candidate, try to compact it
		if let Some((file_id, _)) = candidate_to_compact {
			let compacted =
				self.compact_vlog_file_safe(file_id, Arc::clone(&commit_pipeline)).await?;

			if compacted {
				Ok(vec![file_id])
			} else {
				Ok(vec![])
			}
		} else {
			// No suitable candidate found
			Ok(vec![])
		}
	}

	/// Compacts a single value log file
	async fn compact_vlog_file_safe(
		&self,
		file_id: u32,
		commit_pipeline: Arc<CommitPipeline>,
	) -> Result<bool> {
		// Perform the actual compaction
		let compacted = self.compact_vlog_file(file_id, Arc::clone(&commit_pipeline)).await?;

		if compacted {
			// Schedule for safe deletion based on iterator count
			if self.iterator_count() == 0 {
				// No active iterators, safe to delete immediately
				let mut files_map = self.files_map.write();
				let mut file_handles = self.file_handles.write();
				file_handles.remove(&file_id);
				if let Some(vlog_file) = files_map.remove(&file_id) {
					if let Err(e) = std::fs::remove_file(&vlog_file.path) {
						log::error!("Failed to delete VLog file {:?}: {}", vlog_file.path, e);
						return Err(Error::Io(Arc::new(e)));
					}
				}
			} else {
				// There are active iterators, defer deletion
				let mut files_to_delete = self.files_to_be_deleted.write();
				files_to_delete.push(file_id);
				self.file_handles.write().remove(&file_id);
			}
		}

		Ok(compacted)
	}

	/// Compacts a single value log file, copying only live values
	async fn compact_vlog_file(
		&self,
		file_id: u32,
		commit_pipeline: Arc<CommitPipeline>,
	) -> Result<bool> {
		let source_path = self.vlog_file_path(file_id);

		// Check if file exists
		if !source_path.exists() {
			return Ok(false);
		}

		// Get current discard statistics for this file
		let (total_size, expected_discard, _) = self.get_file_stats(file_id);

		if total_size == 0 || expected_discard == 0 {
			// No point in compacting if there's nothing to discard
			return Ok(false);
		}

		// Open source file
		let mut source_file = BufReader::new(File::open(&source_path)?);

		// Start reading after the file header
		let mut offset = VLogFileHeader::SIZE as u64;
		let mut values_skipped = 0;
		let mut stale_seq_nums: Vec<u64> = Vec::new();
		let mut stale_internal_keys: Vec<InternalKey> = Vec::new();

		let mut batch = Batch::default();
		let mut batch_size = 0;
		const MAX_BATCH_SIZE: usize = 4 * 1024 * 1024; // 4MB batch size limit
		const MAX_BATCH_COUNT: usize = 1000; // Maximum entries per batch

		// Read the file and process entries
		loop {
			// Try to read header: [key_len: 4 bytes][value_len: 4 bytes]
			source_file.seek(SeekFrom::Start(offset))?;
			let mut key_len_buf = [0u8; 4];
			match source_file.read_exact(&mut key_len_buf) {
				Ok(()) => {}
				Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
					break;
				}
				Err(e) => return Err(e.into()),
			}

			let key_len = u32::from_be_bytes(key_len_buf);

			let mut value_len_buf = [0u8; 4];
			source_file.read_exact(&mut value_len_buf)?;
			let value_len = u32::from_be_bytes(value_len_buf);

			// Read key
			let mut key = vec![0u8; key_len as usize];
			source_file.read_exact(&mut key)?;

			// Read value
			let mut value = vec![0u8; value_len as usize];
			source_file.read_exact(&mut value)?;

			// Read CRC32
			let mut crc32_buf = [0u8; 4];
			source_file.read_exact(&mut crc32_buf)?;
			let _crc32 = u32::from_be_bytes(crc32_buf);

			let entry_size = 8 + key_len as u64 + value_len as u64 + 4; // header + key + value + crc32

			let internal_key = InternalKey::decode(&key);

			// Check if this user key is stale using the global delete list
			let is_stale = match self.delete_list.is_stale(internal_key.seq_num()) {
				Ok(stale) => stale,
				Err(e) => {
					let user_key = internal_key.user_key.clone();
					log::error!("Failed to check delete list for user key {user_key:?}: {e}");
					return Err(e);
				}
			};

			if is_stale {
				// Skip this entry (it's stale)
				values_skipped += 1;
				stale_seq_nums.push(internal_key.seq_num());
				stale_internal_keys.push(internal_key.clone());
			} else {
				// Add this entry to the batch to be rewritten with the correct operation type
				// This preserves the original operation (Set, Delete, Merge, etc.)
				// This will cause the value to be written to the active VLog file
				// and a new pointer to be stored in the LSM

				let size = internal_key.user_key.len() + value.len();
				let val = if value.is_empty() {
					None
				} else {
					Some(Bytes::from(value))
				};

				batch.add_record(
					internal_key.kind(),
					internal_key.user_key,
					val,
					internal_key.timestamp,
				)?;

				// Update batch size tracking
				batch_size += size;

				// If batch is full, commit it
				if batch.count() >= MAX_BATCH_COUNT as u32 || batch_size >= MAX_BATCH_SIZE {
					// Commit the batch to LSM (and VLog for large values)
					commit_pipeline.commit(batch, true).await?;

					// Reset batch
					batch = Batch::default();
					batch_size = 0;
				}
			}

			offset += entry_size;
		}

		// Commit any remaining entries in the batch
		if !batch.is_empty() {
			commit_pipeline.commit(batch, true).await?;
		}

		// Clean up versioned index BEFORE cleaning delete list
		// This ensures that when VLog entries are deleted, B+ tree entries are also
		// cleaned up
		if !stale_internal_keys.is_empty() {
			if let Err(e) = self.cleanup_versioned_index_for_keys(&stale_internal_keys) {
				log::error!("Failed to clean up versioned index during VLog GC: {e}");
				return Err(e);
			}
		}

		// Clean up delete list entries for merged stale data
		if !stale_seq_nums.is_empty() {
			if let Err(e) = self.delete_list.delete_entries_batch(stale_seq_nums) {
				log::error!("Failed to clean up delete list after merge: {e}");
				return Err(e);
			}
		}

		// Only consider the file compacted if we skipped some values
		if values_skipped > 0 {
			// Schedule the old file for deletion
			// The actual file will be deleted when there are no active iterators

			// Update discard stats (file will be deleted, so reset stats)
			{
				let mut discard_stats = self.discard_stats.lock();
				discard_stats.remove_file(file_id);
			}

			Ok(true)
		} else {
			// No stale data found, nothing to do
			Ok(false)
		}
	}

	/// Syncs all data to disk
	pub(crate) fn sync(&self) -> Result<()> {
		if let Some(ref mut writer) = *self.writer.write() {
			writer.sync()?;
		}

		self.discard_stats.lock().sync()?;

		Ok(())
	}

	pub(crate) fn flush(&self) -> Result<()> {
		if let Some(ref mut writer) = *self.writer.write() {
			writer.flush()?;
		}

		Ok(())
	}

	/// Gets statistics for a specific file
	fn get_file_stats(&self, file_id: u32) -> (u64, u64, f64) {
		let discard_stats = self.discard_stats.lock();
		let discard_bytes = discard_stats.get_file_stats(file_id);

		// Get file size from filesystem
		let file_path = self.vlog_file_path(file_id);
		let total_size = if let Ok(metadata) = std::fs::metadata(file_path) {
			metadata.len()
		} else {
			0
		};

		let discard_ratio = if total_size > 0 {
			discard_bytes as f64 / total_size as f64
		} else {
			0.0
		};

		(total_size, discard_bytes, discard_ratio)
	}

	/// Registers a VLog file in the files map for tracking
	fn register_vlog_file(&self, file_id: u32, path: PathBuf, size: u64) {
		let mut files_map = self.files_map.write();
		files_map.insert(file_id, Arc::new(VLogFile::new(file_id, path, size)));
	}

	/// Cleans up any leftover temporary files from interrupted compactions
	fn cleanup_temp_files(&self) -> Result<()> {
		let entries = match std::fs::read_dir(&self.path) {
			Ok(entries) => entries,
			Err(_) => return Ok(()), // Directory doesn't exist yet
		};

		for entry in entries {
			let entry = entry?;
			let file_name = entry.file_name();
			let file_name_str = file_name.to_string_lossy();

			// Look for .tmp files that match our VLog pattern
			if file_name_str.len() == 28 && file_name_str.ends_with(".log.tmp") {
				let temp_path = entry.path();

				// Try to remove the temporary file
				if let Err(e) = std::fs::remove_file(&temp_path) {
					log::warn!("Failed to cleanup temp file {temp_path:?}: {e}");
				}
			}
		}

		Ok(())
	}

	/// Updates discard statistics for a file
	/// This should be called during LSM compaction when outdated VLog pointers
	/// are found
	pub(crate) fn update_discard_stats(&self, stats: &HashMap<u32, i64>) {
		let mut discard_stats = self.discard_stats.lock();

		for (file_id, discard_bytes) in stats {
			discard_stats.update(*file_id, *discard_bytes);
		}
	}

	/// Adds multiple stale entries to the global delete list in a batch
	pub(crate) fn add_batch_to_delete_list(&self, entries: Vec<(u64, u64)>) -> Result<()> {
		if entries.is_empty() {
			return Ok(());
		}

		// Call the synchronous method directly - no async needed
		self.delete_list.add_stale_entries_batch(entries)
	}

	/// Gets statistics for all VLog files (for debugging)
	#[allow(unused)]
	pub(crate) fn get_all_file_stats(&self) -> Vec<(u32, u64, u64, f64)> {
		let mut all_stats = Vec::new();

		// Check files from 0 to next counter
		let next_file_id = self.next_file_id.load(std::sync::atomic::Ordering::SeqCst);

		for file_id in 0..next_file_id {
			let (total_size, discard_bytes, discard_ratio) = self.get_file_stats(file_id);
			if total_size > 0 {
				// Only include files that exist
				all_stats.push((file_id, total_size, discard_bytes, discard_ratio));
			}
		}

		all_stats
	}

	/// Checks if a sequence number is marked as stale in the delete list
	/// This is primarily used for testing to verify delete list behavior
	#[allow(unused)]
	pub(crate) fn is_stale(&self, seq_num: u64) -> Result<bool> {
		self.delete_list.is_stale(seq_num)
	}

	/// Cleans up deleted keys from the versioned index atomically during VLog
	/// GC This ensures that when VLog entries are deleted, corresponding B+
	/// tree entries are also cleaned up
	fn cleanup_versioned_index_for_keys(&self, deleted_keys: &[InternalKey]) -> Result<()> {
		if !self.opts.enable_versioning || deleted_keys.is_empty() {
			return Ok(());
		}

		if let Some(ref versioned_index) = self.versioned_index {
			// Take write lock and delete the specific versions
			let mut write_index = versioned_index.write();
			let mut total_deleted = 0;

			for internal_key in deleted_keys {
				// Encode the specific internal key to delete
				let encoded_key = internal_key.encode();

				if let Err(e) = write_index.delete(&encoded_key) {
					log::error!("Failed to delete versioned key: {}", e);
					return Err(e.into());
				} else {
					total_deleted += 1;
				}
			}

			if total_deleted > 0 {
				log::info!("VLog GC: Cleaned up {} versioned index entries", total_deleted);
			}
		}

		Ok(())
	}

	pub(crate) async fn close(&self) -> Result<()> {
		self.sync()?;
		self.delete_list.close().await?;
		Ok(())
	}
}

// ===== VLog GC Manager =====
/// Manages VLog garbage collection as an independent task
pub(crate) struct VLogGCManager {
	/// Reference to the VLog
	vlog: Arc<VLog>,

	/// Reference to the commit pipeline for coordinating writes during GC
	commit_pipeline: Arc<CommitPipeline>,

	/// Background error handler for reporting GC errors
	error_handler: Arc<crate::error::BackgroundErrorHandler>,

	/// Flag to signal the GC task to stop
	stop_flag: Arc<std::sync::atomic::AtomicBool>,

	/// Flag indicating if GC is running
	running: Arc<std::sync::atomic::AtomicBool>,

	/// Notification for GC task
	notify: Arc<tokio::sync::Notify>,

	/// Task handle for cleanup
	task_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl VLogGCManager {
	/// Creates a new VLog GC manager
	pub(crate) fn new(
		vlog: Arc<VLog>,
		commit_pipeline: Arc<CommitPipeline>,
		error_handler: Arc<crate::error::BackgroundErrorHandler>,
	) -> Self {
		let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let running = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let notify = Arc::new(tokio::sync::Notify::new());
		let task_handle = Mutex::new(None);

		Self {
			vlog,
			commit_pipeline,
			error_handler,
			stop_flag,
			running,
			notify,
			task_handle,
		}
	}

	/// Starts the VLog GC background task
	pub(crate) fn start(&self) {
		let vlog = Arc::clone(&self.vlog);
		let commit_pipeline = Arc::clone(&self.commit_pipeline);
		let error_handler = Arc::clone(&self.error_handler);
		let stop_flag = Arc::clone(&self.stop_flag);
		let notify = Arc::clone(&self.notify);
		let running = Arc::clone(&self.running);

		let handle = tokio::spawn(async move {
			loop {
				// Wait for notification OR timeout for periodic checks (every 5 minutes)
				tokio::select! {
					_ = notify.notified() => {},
					_ = tokio::time::sleep(tokio::time::Duration::from_secs(300)) => {},
				}

				if stop_flag.load(Ordering::SeqCst) {
					break;
				}

				running.store(true, Ordering::SeqCst);
				if let Err(e) = vlog.garbage_collect(Arc::clone(&commit_pipeline)).await {
					log::error!("Error in VLog GC: {e:?}");
					error_handler.set_error(e, crate::error::BackgroundErrorReason::VLogGC);
				}
				running.store(false, Ordering::SeqCst);
			}
		});

		*self.task_handle.lock() = Some(handle);
	}

	/// Stops the VLog GC background task
	pub(crate) async fn stop(&self) -> Result<()> {
		// Set the stop flag to prevent new operations from starting
		self.stop_flag.store(true, Ordering::SeqCst);

		// Wake up any waiting tasks so they can check the stop flag and exit
		self.notify.notify_one();

		// Wait for any in-progress GC to complete
		while self.running.load(Ordering::Acquire) {
			// Yield to other tasks and wait a short time before checking again
			tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
		}

		// Now it's safe to wait for the task to complete
		// Get the handle outside the await to avoid holding the lock across await
		let handle_opt = {
			let mut lock = self.task_handle.lock();
			lock.take()
		};

		if let Some(handle) = handle_opt {
			if let Err(e) = handle.await {
				log::error!("Error shutting down VLog GC task: {e:?}");
				// Continue to close the VLog
			}
		}

		// Close the VLog
		if let Err(e) = self.vlog.close().await {
			log::error!("Error closing VLog: {e:?}");
			return Err(e);
		}

		Ok(())
	}
}

/// Global Delete List using B+ Tree
/// Uses a dedicated B+ tree for tracking stale user keys.
/// This provides better performance and consistency with the main LSM tree
/// design.
pub(crate) struct DeleteList {
	/// B+ tree for storing delete list entries (user_key -> value_size)
	tree: Arc<Tree>,
}

impl DeleteList {
	pub(crate) fn new(delete_list_dir: PathBuf) -> Result<Self> {
		// Create a dedicated LSM tree for the delete list
		let delete_list_opts = Options {
			path: delete_list_dir,
			// Disable VLog for the delete list to avoid circular dependency
			enable_vlog: false,
			..Default::default()
		};

		let delete_list_tree =
			Arc::new(TreeBuilder::with_options(delete_list_opts).build().map_err(|e| {
				Error::Other(format!("Failed to create global delete list LSM: {e}"))
			})?);

		Ok(Self {
			tree: delete_list_tree,
		})
	}

	/// Adds multiple stale entries in batch (synchronous)
	/// This is called from CompactionIterator during LSM compaction
	pub(crate) fn add_stale_entries_batch(&self, entries: Vec<(u64, u64)>) -> Result<()> {
		if entries.is_empty() {
			return Ok(());
		}

		let mut batch = Batch::new(0);

		for (seq_num, value_size) in entries {
			// Convert sequence number to a byte array key
			let seq_key = Bytes::from(seq_num.to_be_bytes().to_vec());
			// Store sequence number -> value_size mapping
			batch.add_record(
				crate::sstable::InternalKeyKind::Set,
				seq_key,
				Some(Bytes::from(value_size.to_be_bytes().to_vec())),
				0,
			)?;
		}

		// Commit the batch to the LSM tree using sync commit
		self.tree
			.sync_commit(batch, true)
			.map_err(|e| Error::Other(format!("Failed to add stale entries: {e}")))?;

		Ok(())
	}

	/// Checks if a sequence number is in the delete list (stale)
	pub(crate) fn is_stale(&self, seq_num: u64) -> Result<bool> {
		let tx = self
			.tree
			.begin_with_mode(crate::Mode::ReadOnly)
			.map_err(|e| Error::Other(format!("Failed to begin transaction: {e}")))?;

		// Convert sequence number to bytes for lookup
		let seq_key = seq_num.to_be_bytes().to_vec();

		match tx.get(&seq_key) {
			Ok(Some(_)) => Ok(true),
			Ok(None) => Ok(false),
			Err(e) => Err(Error::Other(format!("Failed to search delete list: {e}"))),
		}
	}

	/// Deletes multiple entries from the delete list in batch
	/// This is called after successful compaction to clean up the delete list
	pub(crate) fn delete_entries_batch(&self, seq_nums: Vec<u64>) -> Result<()> {
		if seq_nums.is_empty() {
			return Ok(());
		}

		let mut batch = Batch::new(0);

		for seq_num in seq_nums {
			// Convert sequence number to key format
			let seq_key = Bytes::from(seq_num.to_be_bytes().to_vec());
			batch.add_record(crate::sstable::InternalKeyKind::Delete, seq_key, None, 0)?;
		}

		// Commit the batch to the LSM tree using sync commit
		self.tree
			.sync_commit(batch, true)
			.map_err(|e| Error::Other(format!("Failed to delete from delete list: {e}")))
	}

	async fn close(&self) -> Result<()> {
		// Close the delete list tree (uses Box::pin to avoid async recursion)
		// The delete list has its own Tree with enable_vlog: false, so no actual
		// infinite recursion
		Box::pin(self.tree.close()).await
	}
}
#[cfg(test)]
mod tests {
	use tempfile::TempDir;
	use test_log::test;

	use super::*;

	fn create_test_vlog(opts: Option<Options>) -> (VLog, TempDir, Arc<Options>) {
		let temp_dir = TempDir::new().unwrap();

		let mut opts = opts.unwrap_or(Options {
			vlog_checksum_verification: VLogChecksumLevel::Full,
			..Default::default()
		});

		opts.path = temp_dir.path().to_path_buf();

		// Create vlog subdirectory
		std::fs::create_dir_all(opts.vlog_dir()).unwrap();
		std::fs::create_dir_all(opts.discard_stats_dir()).unwrap();
		std::fs::create_dir_all(opts.delete_list_dir()).unwrap();

		let opts = Arc::new(opts);
		let vlog = VLog::new(opts.clone(), None).unwrap();
		(vlog, temp_dir, opts)
	}

	#[test]
	fn test_value_pointer_encoding() {
		let pointer = ValuePointer::new(123, 456, 111, 789, 0xdeadbeef);
		let encoded = pointer.encode();
		let decoded = ValuePointer::decode(&encoded).unwrap();
		assert_eq!(pointer, decoded);
	}

	#[test]
	fn test_value_pointer_utility_methods() {
		let pointer = ValuePointer::new(123, 456, 11, 789, 42);
		let encoded = pointer.encode();

		// Test with valid pointer
		assert!(ValuePointer::decode(&encoded).is_ok());
		assert_eq!(ValuePointer::decode(&encoded).unwrap(), pointer);

		// Test with wrong size data
		let wrong_size = vec![0u8; 20];
		assert!(ValuePointer::decode(&wrong_size).is_err());

		// Test with random data of correct size
		let random_data = vec![0x42u8; VALUE_POINTER_SIZE];
		// With random data, decode might work but produce a nonsense pointer
		// We just check it doesn't crash
		let _ = ValuePointer::decode(&random_data);
	}

	#[test]
	fn test_discard_stats_operations() {
		let temp_dir = TempDir::new().unwrap();
		// Create a vlog subdirectory and use its parent for discard stats
		let vlog_dir = temp_dir.path().join("vlog");
		std::fs::create_dir_all(&vlog_dir).unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		// Add some discardable bytes
		stats.update(1, 100);
		stats.update(1, 50);

		let discard_bytes = stats.get_file_stats(1);
		assert_eq!(discard_bytes, 150);

		// Test another file
		stats.update(2, 300); // Higher discard

		let candidates = stats.get_gc_candidates();
		let (max_file, max_discard) = candidates[0];
		assert_eq!(max_file, 2);
		assert_eq!(max_discard, 300);
	}

	#[test]
	fn test_gc_threshold_with_discard_stats() {
		let temp_dir = TempDir::new().unwrap();
		// Create a vlog subdirectory and use its parent for discard stats
		let vlog_dir = temp_dir.path().join("vlog");
		std::fs::create_dir_all(&vlog_dir).unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		// File 1: Add discardable data
		stats.update(1, 600);

		let discard_bytes = stats.get_file_stats(1);
		assert_eq!(discard_bytes, 600);

		// File 2: Lower discard
		stats.update(2, 200);

		let discard_bytes_2 = stats.get_file_stats(2);
		assert_eq!(discard_bytes_2, 200);

		// Test max discard selection (used by conservative GC)
		let candidates = stats.get_gc_candidates();
		let (max_file, max_discard) = candidates[0];
		assert_eq!(max_file, 1, "File 1 should have maximum discard bytes");
		assert_eq!(max_discard, 600);
	}

	#[test(tokio::test)]
	async fn test_vlog_append_and_get() {
		let (vlog, _temp_dir, _) = create_test_vlog(None);

		let key = b"test_key";
		let value = vec![1u8; 300]; // Large enough for vlog
		let pointer = vlog.append(key, &value).unwrap();
		vlog.sync().unwrap();

		let retrieved = vlog.get(&pointer).unwrap();
		assert_eq!(value, *retrieved);
	}

	#[test(tokio::test)]
	async fn test_vlog_small_value_acceptance() {
		let (vlog, _temp_dir, _) = create_test_vlog(None);

		let key = b"test_key";
		let small_value = vec![1u8; 10]; // Small value should now be accepted
		let pointer = vlog.append(key, &small_value).unwrap();
		vlog.sync().unwrap();

		let retrieved = vlog.get(&pointer).unwrap();
		assert_eq!(small_value, *retrieved);
	}

	#[test(tokio::test)]
	async fn test_vlog_caching() {
		let (vlog, _temp_dir, _) = create_test_vlog(None);

		let key = b"cache_test_key";
		let value = vec![42u8; 1000]; // Large enough value
		let pointer = vlog.append(key, &value).unwrap();
		vlog.sync().unwrap();

		// First read - should read from disk and cache the value
		let retrieved1 = vlog.get(&pointer).unwrap();
		assert_eq!(value, *retrieved1);

		// Second read - should read from cache
		let retrieved2 = vlog.get(&pointer).unwrap();
		assert_eq!(value, *retrieved2);
		assert_eq!(retrieved1, retrieved2);

		// Verify that the cache contains the value
		let cached_value = vlog.opts.block_cache.get_vlog(pointer.file_id, pointer.offset);
		assert!(cached_value.is_some());
		assert_eq!(cached_value.unwrap(), value);
	}

	#[test(tokio::test)]
	async fn test_prefill_file_handles() {
		let opts = Options {
			vlog_max_file_size: 1024, // Small file size to force multiple files
			vlog_checksum_verification: VLogChecksumLevel::Full,
			..Default::default()
		};
		// Create initial VLog and add data to create multiple files
		let (vlog1, _temp_dir, opts) = create_test_vlog(Some(opts));

		// Add data to create multiple VLog files
		let mut file_ids = Vec::new();
		let mut pointers = Vec::new();

		// Add enough data to create at least 3 files
		for i in 0..10 {
			let key = format!("key_{i}").into_bytes();
			let value = vec![i as u8; 200]; // Large enough to fill files quickly
			let pointer = vlog1.append(&key, &value).unwrap();
			file_ids.push(pointer.file_id);
			pointers.push(pointer);

			// Sync after each append to ensure data is written to disk
			vlog1.sync().unwrap();
		}

		// Verify we created multiple files
		let unique_file_ids: std::collections::HashSet<_> = file_ids.iter().collect();
		assert!(
			unique_file_ids.len() > 1,
			"Should have created multiple VLog files, got {}",
			unique_file_ids.len()
		);

		// Drop the first VLog to close all file handles
		vlog1.close().await.unwrap();

		// Create a new VLog instance - this should trigger prefill_file_handles
		let vlog2 = VLog::new(opts.clone(), None).unwrap();

		// Verify that all existing files can be read
		for (i, pointer) in pointers.iter().enumerate() {
			let retrieved = vlog2.get(pointer).unwrap();
			let expected_value = vec![i as u8; 200];
			assert_eq!(*retrieved, expected_value);
		}

		// Check that the next_file_id is set correctly (should be one past the highest
		// file ID)
		let next_file_id = vlog2.next_file_id.load(Ordering::SeqCst);
		let max_file_id = unique_file_ids.iter().max().unwrap();
		assert_eq!(
			next_file_id,
			**max_file_id + 1,
			"next_file_id should be one past the highest file ID"
		);

		// Add new data and verify it goes to the correct active file
		let new_key = b"new_key";
		let new_value = vec![255u8; 200];
		let new_pointer = vlog2.append(new_key, &new_value).unwrap();

		// Sync to ensure data is written to disk
		vlog2.sync().unwrap();

		// The new pointer should have the correct file_id (should be the active writer)
		let active_writer_id = vlog2.active_writer_id.load(Ordering::SeqCst);
		assert_eq!(
			new_pointer.file_id, active_writer_id,
			"New pointer should be written to the active writer file"
		);

		// Verify the new data can be read
		let retrieved_new = vlog2.get(&new_pointer).unwrap();
		assert_eq!(*retrieved_new, new_value);

		// Verify that the new file_id is not one of the old ones
		assert!(
			!unique_file_ids.contains(&new_pointer.file_id),
			"New data should not be written to an old file"
		);

		// Check that file handles are properly cached
		let file_handles = vlog2.file_handles.read();
		assert!(
			file_handles.len() >= unique_file_ids.len(),
			"Should have cached handles for all existing files"
		);

		// Verify we can still read from old files after adding new data
		for (i, pointer) in pointers.iter().enumerate() {
			let retrieved = vlog2.get(pointer).unwrap();
			let expected_value = vec![i as u8; 200];
			assert_eq!(*retrieved, expected_value);
		}
	}

	#[test]
	fn test_value_pointer_encode_into_decode() {
		let pointer = ValuePointer::new(123, 456, 111, 789, 0xdeadbeef);

		// Test encode
		let encoded = pointer.encode();
		assert_eq!(encoded.len(), VALUE_POINTER_SIZE);

		// Test decode
		let decoded = ValuePointer::decode(&encoded).unwrap();
		assert_eq!(pointer, decoded);
	}

	#[test]
	fn test_value_pointer_codec() {
		let pointer = ValuePointer::new(1, 2, 3, 4, 0);

		// Test with zero values
		let encoded = pointer.encode();
		let decoded = ValuePointer::decode(&encoded).unwrap();
		assert_eq!(pointer, decoded);

		// Test with maximum values
		let max_pointer = ValuePointer::new(u32::MAX, u64::MAX, u32::MAX, u32::MAX, u32::MAX);
		let encoded = max_pointer.encode();
		let decoded = ValuePointer::decode(&encoded).unwrap();
		assert_eq!(max_pointer, decoded);
	}

	#[test]
	fn test_value_pointer_decode_insufficient_data() {
		let incomplete_data = vec![0u8; VALUE_POINTER_SIZE - 1];
		let result = ValuePointer::decode(&incomplete_data);
		assert!(result.is_err());
	}

	#[test]
	fn test_value_location_inline_encoding() {
		let test_data = b"hello world";
		let location = ValueLocation::with_inline_value(test_data.to_vec());

		// Test encode
		let encoded = location.encode();
		assert_eq!(encoded.len(), 1 + 1 + test_data.len()); // meta + version (1 byte) + data
		assert_eq!(encoded[0], 0); // meta should be 0 for inline
		assert_eq!(encoded[1], VALUE_LOCATION_VERSION); // version should be 1
		assert_eq!(&encoded[2..], test_data); // Skip meta (1) and version (1)

		// Test decode
		let decoded = ValueLocation::decode(&encoded).unwrap();
		assert_eq!(location, decoded);

		// Verify it's inline
		assert!(!decoded.is_value_pointer());
	}

	#[test]
	fn test_value_location_vlog_encoding() {
		let pointer = ValuePointer::new(1, 1024, 8, 256, 0x12345678);
		let location = ValueLocation::with_pointer(pointer.clone());

		// Test encode
		let encoded = location.encode();
		assert_eq!(encoded.len(), 1 + 1 + VALUE_POINTER_SIZE); // meta + version (1 byte) + pointer size
		assert_eq!(encoded[0], BIT_VALUE_POINTER); // meta should have BIT_VALUE_POINTER set

		// Test decode
		let decoded = ValueLocation::decode(&encoded).unwrap();
		assert_eq!(location, decoded);

		// Verify it's a value pointer
		assert!(decoded.is_value_pointer());

		// Verify pointer matches by decoding the value
		let decoded_pointer = ValuePointer::decode(&decoded.value).unwrap();
		assert_eq!(pointer, decoded_pointer);
	}

	#[test]
	fn test_value_location_encode_into_decode() {
		let test_cases = vec![
			ValueLocation::with_inline_value(b"small data".to_vec()),
			ValueLocation::with_pointer(ValuePointer::new(1, 100, 10, 50, 0xabcdef)),
			ValueLocation::with_inline_value(Vec::new()), // empty data
		];

		for location in test_cases {
			// Test encode_into
			let mut encoded = Vec::new();
			location.encode_into(&mut encoded).unwrap();

			// Test decode
			let decoded = ValueLocation::decode(&encoded).unwrap();

			assert_eq!(location, decoded);
		}
	}

	#[test]
	fn test_value_location_size_calculation() {
		// Test inline size
		let inline_data = b"test data";
		let inline_location = ValueLocation::with_inline_value(inline_data.to_vec());
		assert_eq!(inline_location.encoded_size(), 1 + 1 + inline_data.len()); // meta + version + data

		// Test VLog size
		let pointer = ValuePointer::new(1, 100, 8, 256, 0x12345);
		let vlog_location = ValueLocation::with_pointer(pointer);
		assert_eq!(vlog_location.encoded_size(), 1 + 1 + VALUE_POINTER_SIZE); // meta + version + pointer
	}

	#[test(tokio::test)]
	async fn test_value_location_resolve_inline() {
		let (vlog, _temp_dir, _) = create_test_vlog(None);
		let vlog = Arc::new(vlog);
		let test_data = b"inline test data";
		let location = ValueLocation::with_inline_value(test_data.to_vec());

		let resolved = location.resolve_value(Some(&vlog)).unwrap();
		assert_eq!(&*resolved, test_data);
	}

	#[test(tokio::test)]
	async fn test_value_location_resolve_vlog() {
		let (vlog, _temp_dir, _) = create_test_vlog(None);
		let vlog = Arc::new(vlog);
		let key = b"test_key";
		let value = b"test_value_for_vlog_resolution";

		// Append to vlog to get a real pointer
		let pointer = vlog.append(key, value).unwrap();
		vlog.sync().unwrap(); // Sync to ensure data is written to disk
		let location = ValueLocation::with_pointer(pointer);

		// Resolve should return the original value
		let resolved = location.resolve_value(Some(&vlog)).unwrap();
		assert_eq!(&*resolved, value);
	}

	#[test]
	fn test_value_location_from_encoded_value_inline() {
		let test_data = b"encoded inline data";
		let location = ValueLocation::with_inline_value(test_data.to_vec());
		let encoded = location.encode();

		// Should work without VLog for inline data
		let decoded_location = ValueLocation::decode(&encoded).unwrap();
		let resolved = decoded_location.resolve_value(None).unwrap();
		assert_eq!(&*resolved, test_data);
	}

	#[test(tokio::test)]
	async fn test_value_location_from_encoded_value_vlog() {
		let (vlog, _temp_dir, _) = create_test_vlog(None);
		let key = b"test_key";
		let value = b"test_value_for_encoded_resolution";

		// Append to vlog and create encoded VLog location
		let pointer = vlog.append(key, value).unwrap();
		vlog.sync().unwrap(); // Sync to ensure data is written to disk
		let location = ValueLocation::with_pointer(pointer);
		let encoded = location.encode();

		// Should resolve with VLog
		let decoded_location = ValueLocation::decode(&encoded).unwrap();
		let resolved = decoded_location.clone().resolve_value(Some(&Arc::new(vlog))).unwrap();
		assert_eq!(&*resolved, value);

		// Should fail without VLog
		let result = decoded_location.resolve_value(None);
		assert!(result.is_err());
	}

	#[test]
	fn test_value_location_edge_cases() {
		// Test with maximum size inline data
		let max_inline = vec![0xffu8; u16::MAX as usize];
		let location = ValueLocation::with_inline_value(max_inline);
		let encoded = location.encode();
		let decoded = ValueLocation::decode(&encoded).unwrap();
		assert_eq!(location, decoded);

		// Test with pointer containing edge values
		let edge_pointer = ValuePointer::new(1, u64::MAX, 0, u32::MAX, 0);
		let location = ValueLocation::with_pointer(edge_pointer);
		let encoded = location.encode();
		let decoded = ValueLocation::decode(&encoded).unwrap();
		assert_eq!(location, decoded);
	}

	#[test]
	fn test_vlog_file_header_encoding() {
		let opts = Options::default();
		let header = VLogFileHeader::new(123, opts.vlog_max_file_size, CompressionType::None as u8);
		let encoded = header.encode();
		let decoded = VLogFileHeader::decode(&encoded).unwrap();

		assert_eq!(header.magic, decoded.magic);
		assert_eq!(header.version, decoded.version);
		assert_eq!(header.file_id, decoded.file_id);
		assert_eq!(header.created_at, decoded.created_at);
		assert_eq!(header.max_file_size, decoded.max_file_size);
		assert_eq!(header.compression, decoded.compression);
		assert_eq!(header.reserved, decoded.reserved);
		assert!(decoded.is_compatible());
	}

	#[test]
	fn test_vlog_file_header_invalid_magic() {
		let opts = Options::default();
		let mut header =
			VLogFileHeader::new(123, opts.vlog_max_file_size, CompressionType::None as u8);
		header.magic = 0x12345678; // Invalid magic
		let encoded = header.encode();

		assert!(VLogFileHeader::decode(&encoded).is_err());
	}

	#[test]
	fn test_vlog_file_header_invalid_size() {
		let opts = Options::default();
		let header = VLogFileHeader::new(123, opts.vlog_max_file_size, CompressionType::None as u8);
		let mut encoded = header.encode().to_vec();
		encoded.pop(); // Remove one byte to make it invalid size

		assert!(VLogFileHeader::decode(&encoded).is_err());
	}

	#[test]
	fn test_vlog_file_header_version_compatibility() {
		let opts = Options::default();
		let mut header =
			VLogFileHeader::new(123, opts.vlog_max_file_size, CompressionType::None as u8);
		header.version = VLOG_FORMAT_VERSION;
		assert!(header.is_compatible());

		header.version = VLOG_FORMAT_VERSION + 1;
		assert!(!header.is_compatible());
	}

	#[test(tokio::test)]
	async fn test_vlog_with_file_header() {
		let (vlog, _temp_dir, _) = create_test_vlog(None);

		// Append some data to create a VLog file with header
		let key = b"test_key";
		let value = b"test_value";
		let pointer = vlog.append(key, value).unwrap();
		vlog.sync().unwrap();

		// Retrieve the value to ensure header validation works
		let retrieved_value = vlog.get(&pointer).unwrap();
		assert_eq!(&retrieved_value, value);
	}

	#[test(tokio::test)]
	async fn test_vlog_restart_continues_last_file() {
		let temp_dir = TempDir::new().unwrap();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			vlog_max_file_size: 2048,
			vlog_checksum_verification: VLogChecksumLevel::Full,
			..Default::default()
		};

		// Create vlog subdirectory
		std::fs::create_dir_all(opts.vlog_dir()).unwrap();
		std::fs::create_dir_all(opts.discard_stats_dir()).unwrap();
		std::fs::create_dir_all(opts.delete_list_dir()).unwrap();

		let mut pointers = Vec::new();

		// Phase 1: Create initial VLog and add some data (but not enough to fill the
		// file)
		{
			let vlog1 = VLog::new(Arc::new(opts.clone()), None).unwrap();

			// Add some data to the first file
			for i in 0..3 {
				let key = format!("key_{i}").into_bytes();
				let value = vec![i as u8; 100];
				let pointer = vlog1.append(&key, &value).unwrap();
				pointers.push((pointer, value));
			}
			vlog1.sync().unwrap();

			// Verify we're writing to file 0
			let first_file_id = pointers[0].0.file_id;
			assert_eq!(first_file_id, 0, "First data should go to file 0");

			// All data should be in the same file since we're not filling it
			assert!(
				pointers.iter().all(|(p, _)| p.file_id == first_file_id),
				"All initial data should be in the same file"
			);

			// Get the file size to ensure it's not at max capacity
			let file_path = vlog1.vlog_file_path(first_file_id);
			let file_size = std::fs::metadata(&file_path).unwrap().len();
			assert!(file_size < opts.vlog_max_file_size, "File should not be at max capacity");

			// Check that active_writer_id is correctly set
			let active_writer_id = vlog1.active_writer_id.load(Ordering::SeqCst);
			assert_eq!(
				active_writer_id, first_file_id,
				"Active writer ID should be set to the first file"
			);

			// Verify writer is set up
			assert!(vlog1.writer.read().is_some(), "Writer should be set up");
			vlog1.close().await.unwrap();
		}

		// Phase 2: Restart VLog and verify it continues with the last file
		{
			let vlog2 = VLog::new(Arc::new(opts.clone()), None).unwrap();

			// Check that active_writer_id is set to the last file (file 0)
			let active_writer_id = vlog2.active_writer_id.load(Ordering::SeqCst);
			assert_eq!(
				active_writer_id, 0,
				"After restart, active writer ID should be set to the last file (0)"
			);

			// Check that writer is set up
			assert!(
				vlog2.writer.read().is_some(),
				"After restart, writer should be set up for the last file"
			);

			// Check that next_file_id is correct
			let next_file_id = vlog2.next_file_id.load(Ordering::SeqCst);
			assert_eq!(
				next_file_id, 1,
				"Next file ID should be 1 (one past the highest existing file)"
			);

			// Verify all previous data can still be read
			for (pointer, expected_value) in &pointers {
				let retrieved = vlog2.get(pointer).unwrap();
				assert_eq!(
					*retrieved, *expected_value,
					"Should be able to read existing data after restart"
				);
			}

			// Add new data and verify it goes to the same file (not a new file)
			let new_key = b"new_key_after_restart";
			let new_value = vec![99u8; 100];
			let new_pointer = vlog2.append(new_key, &new_value).unwrap();
			vlog2.sync().unwrap();

			// New data should go to the same file (file 0) since it has space
			assert_eq!(
				new_pointer.file_id, 0,
				"New data after restart should go to the existing file with space"
			);

			// Verify the new data can be read
			let retrieved_new = vlog2.get(&new_pointer).unwrap();
			assert_eq!(
				*retrieved_new, new_value,
				"Should be able to read new data added after restart"
			);

			// Add more data to fill the file and trigger creation of a new file
			let mut large_pointers = Vec::new();
			for i in 0..10 {
				let key = format!("large_key_{i}").into_bytes();
				let value = vec![i as u8; 200]; // Larger values to fill the file
				let pointer = vlog2.append(&key, &value).unwrap();
				large_pointers.push((pointer, value));
			}
			vlog2.sync().unwrap();

			// Check if we eventually created a new file (file 1)
			let has_file_1 = large_pointers.iter().any(|(p, _)| p.file_id == 1);
			if has_file_1 {
				// If we created file 1, verify the active writer ID updated
				let final_active_writer_id = vlog2.active_writer_id.load(Ordering::SeqCst);
				assert_eq!(
					final_active_writer_id, 1,
					"Active writer ID should update to the new file"
				);
			}

			// Verify all data (old and new) can still be read
			for (pointer, expected_value) in &pointers {
				let retrieved = vlog2.get(pointer).unwrap();
				assert_eq!(*retrieved, *expected_value);
			}
			for (pointer, expected_value) in &large_pointers {
				let retrieved = vlog2.get(pointer).unwrap();
				assert_eq!(*retrieved, *expected_value);
			}
		}
	}

	#[test(tokio::test)]
	async fn test_vlog_restart_with_multiple_files() {
		let temp_dir = TempDir::new().unwrap();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			vlog_max_file_size: 800,
			vlog_checksum_verification: VLogChecksumLevel::Full,
			..Default::default()
		};

		// Create vlog subdirectory
		std::fs::create_dir_all(opts.vlog_dir()).unwrap();
		std::fs::create_dir_all(opts.discard_stats_dir()).unwrap();
		std::fs::create_dir_all(opts.delete_list_dir()).unwrap();

		let mut all_pointers = Vec::new();
		let mut highest_file_id = 0;

		// Phase 1: Create VLog and add enough data to create 5+ files
		{
			let vlog1 = VLog::new(Arc::new(opts.clone()), None).unwrap();

			// Add enough data to create at least 5 VLog files
			for i in 0..50 {
				let key = format!("multifile_key_{:04}", i).into_bytes();
				let value = vec![i as u8; 80]; // Data to fill files moderately
				let pointer = vlog1.append(&key, &value).unwrap();
				all_pointers.push((pointer.clone(), key, value));
				highest_file_id = highest_file_id.max(pointer.file_id);
			}
			vlog1.sync().unwrap();

			// Verify we created at least 5 files
			let unique_file_ids: std::collections::HashSet<_> =
				all_pointers.iter().map(|(p, _, _)| p.file_id).collect();
			assert!(
				unique_file_ids.len() >= 5,
				"Should have created at least 5 VLog files, got {}",
				unique_file_ids.len()
			);

			// Add a final small entry that shouldn't fill the last file completely
			let final_key = b"final_small_entry";
			let final_value = vec![255u8; 20]; // Small entry
			let final_pointer = vlog1.append(final_key, &final_value).unwrap();
			all_pointers.push((final_pointer.clone(), final_key.to_vec(), final_value));
			highest_file_id = highest_file_id.max(final_pointer.file_id);

			vlog1.sync().unwrap();
			vlog1.close().await.unwrap();
		}

		// Phase 2: Restart VLog and verify it picks up the correct active writer
		{
			let vlog2 = VLog::new(Arc::new(opts.clone()), None).unwrap();

			// Check that active_writer_id is set to the highest file ID
			let active_writer_id = vlog2.active_writer_id.load(Ordering::SeqCst);
			assert_eq!(
				active_writer_id, highest_file_id,
				"After restart, active writer ID should be set to the highest file ID"
			);

			// Check that writer is set up
			assert!(
				vlog2.writer.read().is_some(),
				"After restart, writer should be set up for the last file"
			);

			// Check that next_file_id is correct
			let next_file_id = vlog2.next_file_id.load(Ordering::SeqCst);
			assert_eq!(
				next_file_id,
				highest_file_id + 1,
				"Next file ID should be one past the highest existing file ID"
			);

			// Verify all previous data can still be read
			for (pointer, expected_key, expected_value) in &all_pointers {
				let retrieved = vlog2.get(pointer).unwrap();
				assert_eq!(
					*retrieved,
					*expected_value,
					"Should be able to read existing data for key {:?} after restart",
					String::from_utf8_lossy(expected_key)
				);
			}

			// Add new data and verify it goes to the correct file
			let new_key = b"new_data_after_restart";
			let new_value = vec![42u8; 100];
			let new_pointer = vlog2.append(new_key, &new_value).unwrap();
			vlog2.sync().unwrap();

			// New data should go to the highest file ID (since we set it up as active
			// writer)
			assert_eq!(
				new_pointer.file_id, highest_file_id,
				"New data after restart should go to the last existing file"
			);

			// Verify the new data can be read
			let retrieved_new = vlog2.get(&new_pointer).unwrap();
			assert_eq!(
				*retrieved_new, new_value,
				"Should be able to read new data added after restart"
			);

			// Add a lot more data to eventually trigger creation of a new file
			let mut more_pointers = Vec::new();
			for i in 0..20 {
				let key = format!("bulk_new_key_{:04}", i).into_bytes();
				let value = vec![(i % 256) as u8; 100]; // Large values to fill files
				let pointer = vlog2.append(&key, &value).unwrap();
				more_pointers.push((pointer, value));
			}
			vlog2.sync().unwrap();

			// Check if we eventually created a new file
			let has_new_file = more_pointers.iter().any(|(p, _)| p.file_id > highest_file_id);
			if has_new_file {
				// If we created a new file, verify the active writer ID updated
				let new_active_writer_id = vlog2.active_writer_id.load(Ordering::SeqCst);
				assert!(
					new_active_writer_id > highest_file_id,
					"Active writer ID should update to the new file"
				);
			}

			// Verify all data (original, restart-added, and bulk-added) can still be read
			for (pointer, expected_value) in &more_pointers {
				let retrieved = vlog2.get(pointer).unwrap();
				assert_eq!(*retrieved, *expected_value, "Should be able to read bulk-added data");
			}

			// Final verification: count total number of files
			let final_file_ids: std::collections::HashSet<_> = all_pointers
				.iter()
				.map(|(p, _, _)| p.file_id)
				.chain(std::iter::once(new_pointer.file_id))
				.chain(more_pointers.iter().map(|(p, _)| p.file_id))
				.collect();

			assert!(
				final_file_ids.len() >= 5,
				"Should maintain at least 5 VLog files throughout the test"
			);
		}
	}

	#[test(tokio::test)]
	async fn test_vlog_writer_reopen_append_only_behavior() {
		let temp_dir = TempDir::new().unwrap();
		let opts = Arc::new(Options {
			path: temp_dir.path().to_path_buf(),
			vlog_max_file_size: 2048,
			vlog_checksum_verification: VLogChecksumLevel::Full,
			..Default::default()
		});
		std::fs::create_dir_all(opts.discard_stats_dir()).unwrap();
		std::fs::create_dir_all(opts.delete_list_dir()).unwrap();

		// Create a test file path
		let test_file_path = temp_dir.path().join("vlog_writer_test.log");
		let file_id = 10;

		let mut phase1_pointers = Vec::new();
		let mut phase1_data = Vec::new();

		// Phase 1: Create VLogWriter and write initial data
		{
			let mut writer1 = VLogWriter::new(
				&test_file_path,
				file_id,
				opts.vlog_max_file_size,
				CompressionType::None as u8,
			)
			.unwrap();

			// Write several entries in the first phase
			for i in 0..5 {
				let key = format!("phase1_key_{:02}", i).into_bytes();
				let value = vec![i as u8; 50 + i * 10]; // Variable-sized values
				let pointer = writer1.append(&key, &value).unwrap();
				phase1_pointers.push(pointer);
				phase1_data.push((key, value));
			}

			// Flush to ensure data is written
			writer1.sync().unwrap();

			// Get file size and last entry offset for verification
			let file_size_after_phase1 = std::fs::metadata(&test_file_path).unwrap().len();
			println!(
				"Phase 1: File size after writing {} entries: {} bytes",
				phase1_pointers.len(),
				file_size_after_phase1
			);

			// Verify all phase 1 entries have offsets < file size
			for (i, pointer) in phase1_pointers.iter().enumerate() {
				assert!(
					pointer.offset < file_size_after_phase1,
					"Phase 1 entry {} offset ({}) should be < file size ({})",
					i,
					pointer.offset,
					file_size_after_phase1
				);
			}
		} // writer1 is dropped here, file is closed

		let file_size_between_phases = std::fs::metadata(&test_file_path).unwrap().len();

		// Phase 2: Reopen the same file with a new VLogWriter and append more data
		let mut phase2_pointers = Vec::new();
		let mut phase2_data = Vec::new();
		{
			let mut writer2 = VLogWriter::new(
				&test_file_path,
				file_id,
				opts.vlog_max_file_size,
				CompressionType::None as u8,
			)
			.unwrap();

			// Verify the writer starts at the end of the existing file
			assert_eq!(
				writer2.current_offset, file_size_between_phases,
				"Writer should start at the end of existing file (offset: {}, file size: {})",
				writer2.current_offset, file_size_between_phases
			);

			// Write more entries in the second phase
			for i in 0..4 {
				let key = format!("phase2_key_{:02}", i).into_bytes();
				let value = vec![(100 + i) as u8; 40 + i * 5]; // Different pattern
				let pointer = writer2.append(&key, &value).unwrap();

				// CRITICAL: Verify new entries have offsets >= original file size
				assert!(pointer.offset >= file_size_between_phases,
					"Phase 2 entry {} offset ({}) should be >= original file size ({}), proving append not overwrite",
					i, pointer.offset, file_size_between_phases);

				phase2_pointers.push(pointer);
				phase2_data.push((key, value));
			}

			// Flush to ensure data is written
			writer2.sync().unwrap();

			// Verify file size increased
			let file_size_after_phase2 = std::fs::metadata(&test_file_path).unwrap().len();
			assert!(
				file_size_after_phase2 > file_size_between_phases,
				"File size should have increased (before: {}, after: {})",
				file_size_between_phases,
				file_size_after_phase2
			);

			println!(
				"Phase 2: File size grew from {} to {} bytes after adding {} entries",
				file_size_between_phases,
				file_size_after_phase2,
				phase2_pointers.len()
			);
		} // writer2 is dropped here

		// Phase 3: Verification - Read all data back using VLog to ensure no
		// corruption/overwriting
		{
			// Create a VLog instance that can read from our test file
			let vlog_dir = temp_dir.path().join("vlog");
			std::fs::create_dir_all(&vlog_dir).unwrap();

			// Copy our test file to the VLog directory with the expected naming convention
			let vlog_file_path = opts.vlog_file_path(file_id as u64);
			std::fs::copy(&test_file_path, &vlog_file_path).unwrap();

			let vlog = VLog::new(opts.clone(), None).unwrap();

			// Verify ALL phase 1 data can still be read (proving no overwrite occurred)
			for (i, (pointer, (_, expected_value))) in
				phase1_pointers.iter().zip(phase1_data.iter()).enumerate()
			{
				let retrieved = vlog.get(pointer).unwrap();
				assert_eq!(
					*retrieved, *expected_value,
					"Phase 1 entry {} should be readable after phase 2 writes",
					i
				);
			}

			// Verify ALL phase 2 data can be read correctly
			for (i, (pointer, (_, expected_value))) in
				phase2_pointers.iter().zip(phase2_data.iter()).enumerate()
			{
				let retrieved = vlog.get(pointer).unwrap();
				assert_eq!(*retrieved, *expected_value, "Phase 2 entry {} should be readable", i);
			}

			println!(
				"Verification passed: All {} phase 1 + {} phase 2 entries readable",
				phase1_pointers.len(),
				phase2_pointers.len()
			);
		}

		// Phase 4: Additional verification - Test a third reopen to ensure pattern
		// continues
		{
			let mut writer3 = VLogWriter::new(
				&test_file_path,
				file_id,
				opts.vlog_max_file_size,
				CompressionType::None as u8,
			)
			.unwrap();
			let current_file_size = std::fs::metadata(&test_file_path).unwrap().len();

			// Verify writer3 starts at the current end of file
			assert_eq!(
				writer3.current_offset, current_file_size,
				"Third writer should start at current end of file"
			);

			// Add one more entry
			let final_key = b"final_verification_entry";
			let final_value = vec![255u8; 30];
			let final_pointer = writer3.append(final_key, &final_value).unwrap();

			// Verify this entry also has offset >= previous file size
			assert!(
				final_pointer.offset >= current_file_size,
				"Final entry offset ({}) should be >= file size before append ({})",
				final_pointer.offset,
				current_file_size
			);

			writer3.sync().unwrap();

			println!(
				"Phase 4: Successfully appended final entry at offset {} (file was {} bytes)",
				final_pointer.offset, current_file_size
			);
		}

		// Final verification: Check that offsets are strictly increasing across phases
		let all_offsets: Vec<u64> =
			phase1_pointers.iter().chain(phase2_pointers.iter()).map(|p| p.offset).collect();

		for i in 1..all_offsets.len() {
			assert!(
				all_offsets[i] > all_offsets[i - 1],
				"Offsets should be strictly increasing: offset[{}]={} should be > offset[{}]={}",
				i,
				all_offsets[i],
				i - 1,
				all_offsets[i - 1]
			);
		}
	}

	#[test(tokio::test)]
	async fn test_vlog_gc_with_versioned_index_cleanup_integration() {
		use crate::clock::MockLogicalClock;
		use crate::compaction::leveled::Strategy;
		use crate::lsm::{CompactionOperations, TreeBuilder};

		// Create test environment
		let temp_dir = TempDir::new().unwrap();

		// Set up mock clock and retention period
		let mock_clock = Arc::new(MockLogicalClock::with_timestamp(1000));
		let retention_ns = 2000; // 2 seconds retention - very short for testing

		let opts = Options {
			clock: mock_clock.clone(),
			vlog_max_file_size: 950, /* Small files to force frequent rotations (like VLog
			                          * compaction test) */
			vlog_gc_discard_ratio: 0.0, // Disable discard ratio to preserve all values initially
			level_count: 2,             // Two levels for compaction strategy
			..Options::default()
		};

		// Create Tree with versioning enabled
		let tree = TreeBuilder::with_options(opts)
			.with_path(temp_dir.path().to_path_buf())
			.with_versioning(true, retention_ns)
			.build()
			.unwrap();

		// Insert multiple versions of the same key with LARGE values to fill VLog files
		let user_key = b"test_key";

		// Insert versions with large values to fill VLog files quickly
		for (i, ts) in [1000, 2000, 3000, 4000].iter().enumerate() {
			let value = format!("value_{}_large_data", i).repeat(50); // Large value to fill VLog
			let mut tx = tree.begin().unwrap();
			tx.set_at_version(user_key, value.as_bytes(), *ts).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap(); // Force flush after each version
		}

		// Insert and delete a different key to create stale entries
		{
			let mut tx = tree.begin().unwrap();
			tx.set_at_version(b"other_key", b"other_value_large_data".repeat(50), 5000).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();

			// Delete other_key to make it stale
			let mut tx = tree.begin().unwrap();
			tx.delete(b"other_key").unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// Verify all versions exist using the public API
		let tx = tree.begin().unwrap();
		let mut end_key = user_key.to_vec();
		end_key.push(0);
		let scan_all = tx.scan_all_versions(user_key.as_ref(), &end_key, None).unwrap();
		assert_eq!(scan_all.len(), 4, "Should have 4 versions before GC");

		drop(tx);

		// Advance time to make some versions expire
		mock_clock.set_time(6000);

		// Trigger LSM compaction first
		let strategy = Arc::new(Strategy::default());
		tree.core.compact(strategy).unwrap();

		// Check VLog file stats before GC
		if let Some(vlog) = &tree.core.vlog {
			let stats = vlog.get_all_file_stats();
			for (file_id, total_size, discard_bytes, ratio) in stats {
				println!(
					"  File {}: total={}, discard={}, ratio={:.2}",
					file_id, total_size, discard_bytes, ratio
				);
			}
		}

		// Now run VLog garbage collection multiple times to process all files
		let mut all_deleted_files = Vec::new();

		loop {
			let deleted_files = tree.garbage_collect_vlog().await.unwrap();
			if deleted_files.is_empty() {
				break;
			}

			all_deleted_files.extend(deleted_files);
		}

		// Verify that some versions were cleaned up using the public API
		let tx = tree.begin().unwrap();
		let mut end_key = user_key.to_vec();
		end_key.push(0);
		let scan_after = tx.scan_all_versions(user_key.as_ref(), &end_key, None).unwrap();

		// We should have at least some versions remaining
		assert!(!scan_after.is_empty(), "Should have at least some versions remaining");

		// If GC deleted files, we should have fewer versions
		if !all_deleted_files.is_empty() {
			// We should have fewer versions after GC
			assert!(
				scan_after.len() < 6,
				"Should have fewer versions after GC deleted files. Before: 6, After: {}",
				scan_after.len()
			);
		}

		// Test specific timestamp queries to verify which versions were deleted
		let mut end_key = user_key.to_vec();
		end_key.push(0);
		let scan_at_ts1 = tx
			.range_at_version(user_key.as_ref(), &end_key, 1000)
			.unwrap()
			.collect::<std::result::Result<Vec<_>, _>>()
			.unwrap();
		let scan_at_ts2 = tx
			.range_at_version(user_key.as_ref(), &end_key, 2000)
			.unwrap()
			.collect::<std::result::Result<Vec<_>, _>>()
			.unwrap();
		let scan_at_ts3 = tx
			.range_at_version(user_key.as_ref(), &end_key, 3000)
			.unwrap()
			.collect::<std::result::Result<Vec<_>, _>>()
			.unwrap();
		let scan_at_ts4 = tx
			.range_at_version(user_key.as_ref(), &end_key, 4000)
			.unwrap()
			.collect::<std::result::Result<Vec<_>, _>>()
			.unwrap();
		let scan_at_ts5 = tx
			.range_at_version(user_key.as_ref(), &end_key, 6000)
			.unwrap()
			.collect::<std::result::Result<Vec<_>, _>>()
			.unwrap();
		let scan_at_ts6 = tx
			.range_at_version(user_key.as_ref(), &end_key, 6001)
			.unwrap()
			.collect::<std::result::Result<Vec<_>, _>>()
			.unwrap();

		// The key insight: VLog GC only processes files with high discard ratios
		// Files 0, 1, 2 had high discard ratios (0.97) and were processed
		// Files 3, 4, 5, 6 had zero discard ratios (0.00) and were NOT processed
		// This means versions in files 0, 1, 2 were cleaned up, but versions in files
		// 3+ were not

		// Verify that versions in processed files (1000, 2000, 3000) are NOT accessible
		assert_eq!(
			scan_at_ts1.len(),
			0,
			"Version at timestamp 1000 should be deleted (was in processed file)"
		);
		assert_eq!(
			scan_at_ts2.len(),
			0,
			"Version at timestamp 2000 should be deleted (was in processed file)"
		);
		assert_eq!(
			scan_at_ts3.len(),
			0,
			"Version at timestamp 3000 should be deleted (was in processed file)"
		);

		// Verify that recent versions (6000, 6001) are still accessible
		assert_eq!(scan_at_ts4.len(), 1, "Version at timestamp 4000 should still exist (recent)");
		assert_eq!(scan_at_ts5.len(), 1, "Version at timestamp 6000 should still exist (recent)");
		assert_eq!(scan_at_ts6.len(), 1, "Version at timestamp 6001 should still exist (recent)");
	}
}
