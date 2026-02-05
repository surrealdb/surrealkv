use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
use std::sync::Arc;

use byteorder::{ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use parking_lot::{Mutex, RwLock};

use crate::batch::Batch;
use crate::bplustree::tree::DiskBPlusTree;
use crate::commit::CommitPipeline;
use crate::discard::DiscardStats;
use crate::error::{Error, Result};
use crate::{
	vfs,
	CompressionType,
	InternalKey,
	InternalKeyKind,
	Options,
	Tree,
	TreeBuilder,
	VLogChecksumLevel,
	Value,
};

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
pub(crate) struct VLogWriter {
	/// Buffered writer for the file
	writer: BufWriter<File>,
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

	/// Flushes and syncs the writer
	pub(crate) fn sync(&mut self) -> Result<()> {
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
	pub(crate) next_file_id: AtomicU32,

	/// ID of the file currently open for writing
	pub(crate) active_writer_id: AtomicU32,

	/// Writer for the current active file
	pub(crate) writer: RwLock<Option<VLogWriter>>,

	/// Number of active iterators (for GC safety)
	num_active_iterators: AtomicI32,

	/// Maps file_id to VLogFile metadata
	files_map: RwLock<HashMap<u32, Arc<VLogFile>>>,

	/// Cached open file handles for reading
	pub(crate) file_handles: RwLock<HashMap<u32, Arc<File>>>,

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
	pub(crate) opts: Arc<Options>,

	/// Reference to versioned index for atomic cleanup during GC
	versioned_index: Option<Arc<RwLock<DiskBPlusTree>>>,
}

impl VLog {
	/// Returns the file path for a VLog file with the given ID
	pub(crate) fn vlog_file_path(&self, file_id: u32) -> PathBuf {
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
		let value_bytes = entry_data_vec[value_start..crc_start].to_vec();

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
					Some(value.clone())
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
	pub(crate) fn update_discard_stats(&self, stats: &HashMap<u32, i64>) -> Result<()> {
		let mut discard_stats = self.discard_stats.lock();

		for (file_id, discard_bytes) in stats {
			discard_stats.update(*file_id, *discard_bytes)?;
		}
		Ok(())
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

/// Maximum number of entries per batch when adding to DeleteList.
/// This ensures batches fit within memtable capacity.
/// Each entry is ~16 bytes (8 byte key + 8 byte value) plus skiplist overhead (~50-80 bytes).
/// With 10K entries, batch is ~1MB which safely fits in default 100MB memtable.
const DELETE_LIST_CHUNK_SIZE: usize = 10_000;

/// Global Delete List using a dedicated LSM tree.
/// Uses a separate LSM tree for tracking stale sequence numbers.
/// This provides better performance and consistency with the main LSM tree
/// design.
pub(crate) struct DeleteList {
	/// LSM tree for storing delete list entries (seq_num -> value_size)
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

		// Process in chunks to avoid overwhelming memtable
		for chunk in entries.chunks(DELETE_LIST_CHUNK_SIZE) {
			let mut batch = Batch::new(0);

			for (seq_num, value_size) in chunk {
				// Convert sequence number to a byte array key
				let seq_key = seq_num.to_be_bytes().to_vec();
				// Store sequence number -> value_size mapping
				batch.add_record(
					InternalKeyKind::Set,
					seq_key,
					Some(value_size.to_be_bytes().to_vec()),
					0,
				)?;
			}

			// Commit the batch to the LSM tree using sync commit
			self.tree
				.sync_commit(batch, true)
				.map_err(|e| Error::Other(format!("Failed to add stale entries: {e}")))?;
		}

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

		// Process in chunks to avoid overwhelming memtable
		for chunk in seq_nums.chunks(DELETE_LIST_CHUNK_SIZE) {
			let mut batch = Batch::new(0);

			for seq_num in chunk {
				// Convert sequence number to key format
				let seq_key = seq_num.to_be_bytes().to_vec();
				batch.add_record(InternalKeyKind::Delete, seq_key, None, 0)?;
			}

			// Commit the batch to the LSM tree using sync commit
			self.tree
				.sync_commit(batch, true)
				.map_err(|e| Error::Other(format!("Failed to delete from delete list: {e}")))?;
		}

		Ok(())
	}

	async fn close(&self) -> Result<()> {
		// Close the delete list tree (uses Box::pin to avoid async recursion)
		// The delete list has its own Tree with enable_vlog: false, so no actual
		// infinite recursion
		Box::pin(self.tree.close()).await
	}
}
