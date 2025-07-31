use std::{
	collections::HashMap,
	fs::{File, OpenOptions},
	io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write},
	path::{Path, PathBuf},
	sync::{
		atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering},
		Arc, Mutex, RwLock,
	},
};

use crate::cache::VLogCache;
use crate::{batch::Batch, discard::DiscardStats, Value};
use crate::{sstable::InternalKey, vfs, Options, Tree, VLogChecksumLevel};

use byteorder::{ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;

use crate::commit::CommitPipeline;
use crate::error::{Error, Result};

/// Size of a ValuePointer when encoded (8 + 8 + 4 + 4 + 4 bytes)
pub const VALUE_POINTER_SIZE: usize = 28;

/// Represents where a value is stored - either inline in the SSTable or in the value log
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueLocation {
	/// Value is stored inline in the SSTable
	Inline(Value),
	/// Value is stored in the value log, referenced by a pointer
	VLog(ValuePointer),
}

const TAG_INLINE: u8 = 0;
const TAG_VLOG: u8 = 1;

impl ValueLocation {
	/// Creates a ValueLocation from encoded value data, decoding based on tag
	pub fn from_encoded_value(encoded: &[u8], vlog: Option<&VLog>) -> Result<Value> {
		let location = Self::decode(encoded)?;
		match location {
			ValueLocation::Inline(data) => Ok(data),
			ValueLocation::VLog(pointer) => {
				if let Some(vlog) = vlog {
					vlog.get(&pointer)
				} else {
					Err(Error::Corruption("VLog not available for pointer resolution".to_string()))
				}
			}
		}
	}

	/// Encodes the ValueLocation for storage with tag prefix
	pub fn encode(&self) -> Result<Value> {
		let mut encoded = Vec::new();
		self.encode_into(&mut encoded)?;
		Ok(Arc::from(encoded))
	}

	/// Encodes the ValueLocation into a writer
	pub fn encode_into<W: Write>(&self, writer: &mut W) -> Result<()> {
		match self {
			ValueLocation::Inline(data) => {
				writer.write_u8(TAG_INLINE)?;
				writer.write_all(data)?;
			}
			ValueLocation::VLog(pointer) => {
				writer.write_u8(TAG_VLOG)?;
				pointer.encode_into(writer)?;
			}
		}
		Ok(())
	}

	/// Decodes a ValueLocation from stored data using tag
	pub fn decode(data: &[u8]) -> Result<Self> {
		let mut cursor = Cursor::new(data);
		Self::decode_from(&mut cursor)
	}

	/// Decodes a ValueLocation from a reader
	pub fn decode_from<R: Read>(reader: &mut R) -> Result<Self> {
		let tag = reader.read_u8()?;

		match tag {
			TAG_INLINE => {
				let mut data = Vec::new();
				reader.read_to_end(&mut data)?;
				Ok(ValueLocation::Inline(Arc::from(data)))
			}
			TAG_VLOG => {
				let pointer = ValuePointer::decode_from(reader)?;
				Ok(ValueLocation::VLog(pointer))
			}
			x => Err(Error::Corruption(format!("Invalid ValueLocation tag: {x}"))),
		}
	}

	/// Returns the size of the value without resolving it
	pub fn size(&self) -> Result<usize> {
		match self {
			ValueLocation::Inline(data) => Ok(data.len()),
			ValueLocation::VLog(_) => Ok(VALUE_POINTER_SIZE),
		}
	}

	/// Resolves the actual value, reading from VLog if necessary
	pub fn resolve(&self, vlog: Arc<VLog>) -> Result<Value> {
		match self {
			ValueLocation::Inline(data) => Ok(data.clone()),
			ValueLocation::VLog(pointer) => vlog.get(pointer),
		}
	}

	/// Returns true if the value is stored inline
	pub fn is_inline(&self) -> bool {
		matches!(self, ValueLocation::Inline(_))
	}

	/// Returns true if the value is stored in VLog
	pub fn is_vlog(&self) -> bool {
		matches!(self, ValueLocation::VLog(_))
	}
}

/// A pointer to a value stored in the value log
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ValuePointer {
	/// ID of the VLog file containing the value
	pub file_id: u64,
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
	pub fn new(file_id: u64, offset: u64, key_size: u32, value_size: u32, checksum: u32) -> Self {
		Self {
			file_id,
			offset,
			key_size,
			value_size,
			checksum,
		}
	}

	/// Encodes the pointer as bytes for storage
	pub fn encode(&self) -> Vec<u8> {
		let mut encoded = Vec::with_capacity(VALUE_POINTER_SIZE);
		encoded.extend_from_slice(&self.file_id.to_be_bytes());
		encoded.extend_from_slice(&self.offset.to_be_bytes());
		encoded.extend_from_slice(&self.key_size.to_be_bytes());
		encoded.extend_from_slice(&self.value_size.to_be_bytes());
		encoded.extend_from_slice(&self.checksum.to_be_bytes());
		encoded
	}

	/// Encodes the pointer into a writer
	pub fn encode_into<W: Write>(&self, writer: &mut W) -> Result<()> {
		writer.write_all(&self.file_id.to_be_bytes())?;
		writer.write_all(&self.offset.to_be_bytes())?;
		writer.write_all(&self.key_size.to_be_bytes())?;
		writer.write_all(&self.value_size.to_be_bytes())?;
		writer.write_all(&self.checksum.to_be_bytes())?;
		Ok(())
	}

	/// Decodes a pointer from bytes
	pub fn decode(data: &[u8]) -> Result<Self> {
		if data.len() != VALUE_POINTER_SIZE {
			return Err(Error::Corruption("Invalid ValuePointer size".to_string()));
		}

		let file_id = u64::from_be_bytes([
			data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
		]);
		let offset = u64::from_be_bytes([
			data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
		]);
		let key_size = u32::from_be_bytes([data[16], data[17], data[18], data[19]]);
		let value_size = u32::from_be_bytes([data[20], data[21], data[22], data[23]]);
		let checksum = u32::from_be_bytes([data[24], data[25], data[26], data[27]]);

		Ok(Self::new(file_id, offset, key_size, value_size, checksum))
	}

	/// Decodes a pointer from a reader
	pub fn decode_from<R: Read>(reader: &mut R) -> Result<Self> {
		let mut buf = [0u8; VALUE_POINTER_SIZE];
		reader.read_exact(&mut buf)?;

		let file_id =
			u64::from_be_bytes([buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7]]);
		let offset = u64::from_be_bytes([
			buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
		]);
		let key_size = u32::from_be_bytes([buf[16], buf[17], buf[18], buf[19]]);
		let value_size = u32::from_be_bytes([buf[20], buf[21], buf[22], buf[23]]);
		let checksum = u32::from_be_bytes([buf[24], buf[25], buf[26], buf[27]]);

		Ok(Self::new(file_id, offset, key_size, value_size, checksum))
	}

	/// Calculates the total entry size (header + key + value + crc32)
	pub fn total_entry_size(&self) -> u64 {
		8 + self.key_size as u64 + self.value_size as u64 + 4
	}
}

/// Represents a VLog file that can be read from
#[derive(Debug)]
pub struct VLogFile {
	/// File ID
	pub id: u64,
	/// File path
	pub path: PathBuf,
	/// File size
	pub size: u64,
}

impl VLogFile {
	pub fn new(id: u64, path: PathBuf, size: u64) -> Self {
		Self {
			id,
			path,
			size,
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
	file_id: u64,
	/// Total bytes written to this file
	bytes_written: u64,
}

impl VLogWriter {
	/// Creates a new VLogWriter for the specified file
	fn new(path: &Path, file_id: u64) -> Result<Self> {
		let file = OpenOptions::new().create(true).append(true).open(path)?;

		let current_offset = file.metadata()?.len();
		let writer = BufWriter::new(file);

		Ok(Self {
			writer,
			current_offset,
			file_id,
			bytes_written: current_offset,
		})
	}

	/// Appends a key+value entry to the file and returns a pointer to it
	/// Layout: +--------+-----+-------+-------+
	///         | header | key | value | crc32 |
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
pub struct VLog {
	/// Base directory for VLog files
	path: PathBuf,

	/// Maximum size for each VLog file before rotation
	max_file_size: u64,

	/// Checksum verification level
	checksum_level: VLogChecksumLevel,

	/// Discard ratio threshold for triggering garbage collection (0.0 - 1.0)
	gc_discard_ratio: f64,

	/// Next file ID to be assigned
	next_file_id: AtomicU64,

	/// ID of the file currently open for writing
	active_writer_id: AtomicU64,

	/// Writer for the current active file
	writer: RwLock<Option<VLogWriter>>,

	/// Number of active iterators (for GC safety)
	num_active_iterators: AtomicI32,

	/// Maps file_id to VLogFile metadata
	files_map: RwLock<HashMap<u64, Arc<VLogFile>>>,

	/// Cached open file handles for reading
	file_handles: RwLock<HashMap<u64, Arc<File>>>,

	/// Files marked for deletion but waiting for iterators to finish
	files_to_be_deleted: RwLock<Vec<u64>>,

	/// Prevents concurrent garbage collection
	gc_in_progress: AtomicBool,

	/// Discard statistics for GC candidate selection
	discard_stats: Mutex<DiscardStats>,

	/// Global delete list LSM tree: tracks <stale_seqno, value_size> pairs across all segments
	pub(crate) global_delete_list: Arc<GlobalDeleteListLSM>,

	/// Dedicated cache for VLog values to avoid repeated disk reads
	cache: Arc<VLogCache>,
}

impl VLog {
	/// Returns the file path for a VLog file with the given ID
	fn vlog_file_path(&self, file_id: u64) -> PathBuf {
		self.path.join(format!("vlog_{file_id:08}.log"))
	}

	/// Creates a new VLog instance
	pub fn new<P: AsRef<Path>>(
		dir: P,
		max_file_size: u64,
		checksum_level: VLogChecksumLevel,
		gc_discard_ratio: f64,
		cache: Arc<VLogCache>,
	) -> Result<Self> {
		let dir = dir.as_ref().to_path_buf();
		std::fs::create_dir_all(&dir)?;

		// Get the parent directory for the discard stats file
		// This ensures the DISCARD file lives in the main database directory,
		// not in the VLog subdirectory
		let discard_stats_dir =
			dir.parent().ok_or_else(|| Error::Other("VLog directory has no parent".to_string()))?;

		// Initialize the global delete list LSM tree
		let global_delete_list = Arc::new(GlobalDeleteListLSM::new(dir.clone())?);

		let vlog = Self {
			path: dir.clone(),
			max_file_size,
			checksum_level,
			gc_discard_ratio, // Use the provided ratio
			next_file_id: AtomicU64::new(0),
			active_writer_id: AtomicU64::new(0),
			writer: RwLock::new(None),
			num_active_iterators: AtomicI32::new(0),
			files_map: RwLock::new(HashMap::new()),
			file_handles: RwLock::new(HashMap::new()),
			files_to_be_deleted: RwLock::new(Vec::new()),
			gc_in_progress: AtomicBool::new(false),
			discard_stats: Mutex::new(DiscardStats::new(discard_stats_dir)?),
			global_delete_list,
			cache,
		};

		// PRE-FILL ALL EXISTING FILE HANDLES ON STARTUP
		vlog.prefill_file_handles()?;

		// Clean up any leftover temporary files from previous interrupted compactions
		vlog.cleanup_temp_files()?;

		Ok(vlog)
	}

	/// Appends a key+value pair to the log and returns a ValuePointer
	pub fn append(&self, key: &[u8], value: &[u8]) -> Result<ValuePointer> {
		// Ensure we have a writer
		let _new_file_created = {
			let mut writer = self.writer.write().unwrap();

			if writer.is_none() || writer.as_ref().unwrap().size() >= self.max_file_size {
				// Create new file
				let file_id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
				let file_path = self.vlog_file_path(file_id);
				let new_writer = VLogWriter::new(&file_path, file_id)?;

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
		let mut writer = self.writer.write().unwrap();
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

		let mut max_file_id = 0u64;
		let mut file_handles = self.file_handles.write().unwrap();
		let mut files_map = self.files_map.write().unwrap();

		for entry in entries {
			let entry = entry?;
			let file_name = entry.file_name();
			let file_name_str = file_name.to_string_lossy();

			// Look for VLog files: vlog_12345678.log
			if file_name_str.starts_with("vlog_") && file_name_str.ends_with(".log") {
				// Extract file ID from filename
				if let Some(id_part) =
					file_name_str.strip_prefix("vlog_").and_then(|s| s.strip_suffix(".log"))
				{
					if let Ok(file_id) = id_part.parse::<u64>() {
						max_file_id = max_file_id.max(file_id);

						let file_path = entry.path();
						let file_size = entry.metadata()?.len();

						// Pre-open the file handle
						match File::open(&file_path) {
							Ok(file) => {
								let handle = Arc::new(file);
								file_handles.insert(file_id, handle);

								// Also register in files_map
								files_map.insert(
									file_id,
									Arc::new(VLogFile::new(file_id, file_path, file_size)),
								);
							}
							Err(e) => {
								eprintln!(
									"Warning: Failed to pre-open VLog file {file_name_str}: {e}"
								);
							}
						}
					}
				}
			}
		}

		// Set next_file_id to one past the highest existing file ID
		self.next_file_id.store(max_file_id + 1, Ordering::SeqCst);

		// println!(
		//     "Pre-filled {} VLog file handles, next_file_id set to {}",
		//     file_handles.len(),
		//     max_file_id + 1
		// );

		Ok(())
	}

	/// Retrieves (and caches) an open file handle for the given file ID
	fn get_file_handle(&self, file_id: u64) -> Result<Arc<File>> {
		{
			let handles = self.file_handles.read().unwrap();
			if let Some(handle) = handles.get(&file_id) {
				return Ok(handle.clone());
			}
		}

		// Open and insert the handle
		let file_path = self.vlog_file_path(file_id);
		let file = File::open(&file_path)?;
		let handle = Arc::new(file);

		let mut handles = self.file_handles.write().unwrap();
		// Another thread might have inserted it while we were opening the file
		let entry = handles.entry(file_id).or_insert_with(|| handle.clone());
		Ok(entry.clone())
	}

	/// Retrieves a value using a ValuePointer
	pub fn get(&self, pointer: &ValuePointer) -> Result<Value> {
		// Check cache first
		if let Some(cached_value) = self.cache.get(pointer.file_id, pointer.offset) {
			return Ok(cached_value);
		}

		let file = self.get_file_handle(pointer.file_id)?;

		// Read the entire entry in a single call using sizes from the pointer
		let key_len = pointer.key_size;
		let value_len = pointer.value_size;
		let total_size = pointer.total_entry_size();
		let mut entry_data = vec![0u8; total_size as usize];
		vfs::File::read_at(&*file, pointer.offset, &mut entry_data)?;

		// Verify header sizes
		let header_key_len =
			u32::from_be_bytes([entry_data[0], entry_data[1], entry_data[2], entry_data[3]]);
		let header_value_len =
			u32::from_be_bytes([entry_data[4], entry_data[5], entry_data[6], entry_data[7]]);

		if header_key_len != key_len || header_value_len != value_len {
			return Err(Error::Corruption(format!(
                "Header size mismatch: expected key {key_len} value {value_len}, got key {header_key_len} value {header_value_len}"
            )));
		}

		// Parse from the buffer
		let key_start = 8;
		let value_start = key_start + key_len as usize;
		let crc_start = value_start + value_len as usize;

		let key = &entry_data[key_start..value_start];
		let value = &entry_data[value_start..crc_start];
		let stored_crc32 = u32::from_be_bytes([
			entry_data[crc_start],
			entry_data[crc_start + 1],
			entry_data[crc_start + 2],
			entry_data[crc_start + 3],
		]);

		// Checksum verification
		if self.checksum_level != VLogChecksumLevel::Disabled && stored_crc32 != pointer.checksum {
			return Err(Error::Corruption(format!(
				"CRC32 mismatch: expected {}, got {}",
				pointer.checksum, stored_crc32
			)));
		}

		if self.checksum_level == VLogChecksumLevel::Full {
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

		// Cache the value for future reads
		let value_bytes: Value = value.into();
		self.cache.insert(pointer.file_id, pointer.offset, value_bytes.clone());

		Ok(value_bytes)
	}

	/// Increments the active iterator count when a transaction/iterator starts
	pub fn incr_iterator_count(&self) {
		self.num_active_iterators.fetch_add(1, Ordering::SeqCst);
	}

	/// Decrements the active iterator count when a transaction/iterator ends
	/// If count reaches zero, processes deferred file deletions
	pub fn decr_iterator_count(&self) -> Result<()> {
		let count = self.num_active_iterators.fetch_sub(1, Ordering::SeqCst);

		if count == 1 {
			// We were the last iterator, safe to delete pending files
			self.delete_pending_files()?;
		}

		Ok(())
	}

	/// Gets the current number of active iterators  
	pub fn iterator_count(&self) -> i32 {
		self.num_active_iterators.load(Ordering::SeqCst)
	}

	/// Deletes files that were marked for deletion when no iterators were active
	fn delete_pending_files(&self) -> Result<()> {
		let mut files_to_delete = self.files_to_be_deleted.write().unwrap();
		let mut files_map = self.files_map.write().unwrap();
		let mut file_handles = self.file_handles.write().unwrap();

		for file_id in files_to_delete.drain(..) {
			file_handles.remove(&file_id);
			if let Some(vlog_file) = files_map.remove(&file_id) {
				if let Err(e) = std::fs::remove_file(&vlog_file.path) {
					eprintln!("Warning: Failed to delete VLog file {:?}: {}", vlog_file.path, e);
				}
			}
		}

		Ok(())
	}

	/// Selects files based on discard statistics and compacts them
	pub async fn garbage_collect(&self, commit_pipeline: Arc<CommitPipeline>) -> Result<Vec<u64>> {
		// Try to set the gc_in_progress flag to true, if it's already true, return error
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

		// Get the file with maximum discardable bytes
		let (file_id, discard_bytes) = {
			let discard_stats = self.discard_stats.lock().unwrap();
			let result = discard_stats.max_discard();

			match result {
				Ok((file_id, discard_bytes)) => (file_id, discard_bytes),
				Err(_) => {
					return Ok(vec![]);
				}
			}
		};

		if discard_bytes == 0 {
			// No files with discardable data
			return Ok(vec![]);
		}

		// Check if this file meets the discard ratio threshold
		let should_compact = {
			let (total_size, _, discard_ratio) = self.get_file_stats(file_id);

			if total_size == 0 {
				false
			} else {
				discard_ratio >= self.gc_discard_ratio
			}
		};

		if !should_compact {
			return Ok(vec![]);
		}

		// Compact the selected file
		let compacted = self.compact_vlog_file_safe(file_id, commit_pipeline.clone()).await?;

		if compacted {
			Ok(vec![file_id])
		} else {
			Ok(vec![])
		}
	}

	/// Compacts a single value log file
	async fn compact_vlog_file_safe(
		&self,
		file_id: u64,
		commit_pipeline: Arc<CommitPipeline>,
	) -> Result<bool> {
		// Check if file is already marked for deletion
		{
			let files_to_delete = self.files_to_be_deleted.read().unwrap();
			if files_to_delete.contains(&file_id) {
				return Err(Error::Other(format!(
					"Value log file already marked for deletion fid: {file_id}"
				)));
			}
		}

		// Check if this is the currently open file for writing - we shouldn't compact/delete it
		let active_writer_id = self.active_writer_id.load(Ordering::SeqCst);
		if file_id == active_writer_id {
			// Skip compacting the active file as it is currently open for writing
			return Ok(false);
		}

		// Perform the actual compaction
		let compacted = self.compact_vlog_file(file_id, commit_pipeline.clone()).await?;

		if compacted {
			// Schedule for safe deletion based on iterator count
			if self.iterator_count() == 0 {
				// No active iterators, safe to delete immediately
				let mut files_map = self.files_map.write().unwrap();
				let mut file_handles = self.file_handles.write().unwrap();
				file_handles.remove(&file_id);
				if let Some(vlog_file) = files_map.remove(&file_id) {
					if let Err(e) = std::fs::remove_file(&vlog_file.path) {
						eprintln!(
							"Warning: Failed to delete VLog file {:?}: {}",
							vlog_file.path, e
						);
					}
				}
			} else {
				// There are active iterators, defer deletion
				let mut files_to_delete = self.files_to_be_deleted.write().unwrap();
				files_to_delete.push(file_id);
				self.file_handles.write().unwrap().remove(&file_id);
			}
		}

		Ok(compacted)
	}

	/// Compacts a single value log file, copying only live values
	async fn compact_vlog_file(
		&self,
		file_id: u64,
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

		let mut offset = 0;
		let mut values_skipped = 0;
		let mut stale_seq_nums = Vec::new();

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
			let user_key = internal_key.user_key.to_vec();

			// Check if this user key is stale using the global delete list
			let is_stale = match self.global_delete_list.is_stale(internal_key.seq_num()) {
				Ok(stale) => stale,
				Err(e) => {
					eprintln!(
						"Warning: Failed to check delete list for user key {user_key:?}: {e}"
					);
					false // Conservative: assume live on error
				}
			};

			if is_stale {
				// Skip this entry (it's stale)
				values_skipped += 1;
				stale_seq_nums.push(internal_key.seq_num());
			} else {
				let internal_key = InternalKey::decode(&key);

				// Add this entry to the batch to be rewritten with the correct operation type
				// This preserves the original operation (Set, Delete, Merge, etc.)
				// This will cause the value to be written to the active VLog file
				// and a new pointer to be stored in the LSM
				let kind = internal_key.kind();
				let val = if value.is_empty() {
					None
				} else {
					Some(value.as_slice())
				};
				batch.add_record(kind, &internal_key.user_key, val)?;

				// Update batch size tracking
				batch_size += internal_key.user_key.len() + value.len();

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

		// Clean up delete list entries for merged stale data
		if !stale_seq_nums.is_empty() {
			if let Err(e) = self.global_delete_list.delete_entries_batch(stale_seq_nums) {
				eprintln!("Warning: Failed to clean up delete list after merge: {e}");
			}
		}

		// Only consider the file compacted if we skipped some values
		if values_skipped > 0 {
			// Schedule the old file for deletion
			// The actual file will be deleted when there are no active iterators

			// Update discard stats (file will be deleted, so reset stats)
			{
				let mut discard_stats = self.discard_stats.lock().unwrap();
				discard_stats.remove_file(file_id);
			}

			Ok(true)
		} else {
			// No stale data found, nothing to do
			Ok(false)
		}
	}

	/// Syncs all data to disk
	pub fn sync(&self) -> Result<()> {
		if let Some(ref mut writer) = *self.writer.write().unwrap() {
			writer.sync()?;
		}

		self.discard_stats.lock().unwrap().sync()?;

		Ok(())
	}

	/// Gets the maximum file size
	pub fn max_file_size(&self) -> u64 {
		self.max_file_size
	}

	/// Gets statistics for a specific file
	pub fn get_file_stats(&self, file_id: u64) -> (u64, u64, f64) {
		let discard_stats = self.discard_stats.lock().unwrap();
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

	/// Gets statistics for all VLog files (for debugging)
	pub fn get_all_file_stats(&self) -> Vec<(u64, u64, u64, f64)> {
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

	/// Registers a VLog file in the files map for tracking
	pub fn register_vlog_file(&self, file_id: u64, path: PathBuf, size: u64) {
		let mut files_map = self.files_map.write().unwrap();
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
			if file_name_str.starts_with("vlog_") && file_name_str.ends_with(".log.tmp") {
				let temp_path = entry.path();

				// Try to remove the temporary file
				if let Err(e) = std::fs::remove_file(&temp_path) {
					eprintln!("Warning: Failed to cleanup temp file {temp_path:?}: {e}");
				}
			}
		}

		Ok(())
	}

	/// Updates discard statistics for a file
	/// This should be called during LSM compaction when outdated VLog pointers are found
	pub fn update_discard_stats(&self, stats: &HashMap<u64, i64>) {
		let mut discard_stats = self.discard_stats.lock().unwrap();

		for (file_id, discard_bytes) in stats {
			discard_stats.update(*file_id, *discard_bytes);
		}
	}

	/// Adds multiple stale entries to the global delete list in a batch
	pub fn add_batch_to_delete_list(&self, entries: Vec<(u64, u64)>) -> Result<()> {
		if entries.is_empty() {
			return Ok(());
		}

		// Call the synchronous method directly - no async needed
		self.global_delete_list.add_stale_entries_batch(entries)
	}

	/// Checks if a sequence number is marked as stale in the delete list
	/// This is primarily used for testing to verify delete list behavior
	pub fn is_stale(&self, seq_num: u64) -> Result<bool> {
		self.global_delete_list.is_stale(seq_num)
	}
}

// ===== VLog GC Manager =====
/// Manages VLog garbage collection as an independent task
pub struct VLogGCManager {
	/// Reference to the VLog
	vlog: Arc<VLog>,

	/// Reference to the commit pipeline for coordinating writes during GC
	commit_pipeline: Arc<CommitPipeline>,

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
	pub fn new(vlog: Arc<VLog>, commit_pipeline: Arc<CommitPipeline>) -> Self {
		let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let running = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let notify = Arc::new(tokio::sync::Notify::new());
		let task_handle = Mutex::new(None);

		Self {
			vlog,
			commit_pipeline,
			stop_flag,
			running,
			notify,
			task_handle,
		}
	}

	/// Starts the VLog GC background task
	pub fn start(&self) {
		let vlog = self.vlog.clone();
		let commit_pipeline = self.commit_pipeline.clone();
		let stop_flag = self.stop_flag.clone();
		let notify = self.notify.clone();
		let running = self.running.clone();

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
				// TODO: Use commit_pipeline during GC when VLog is enhanced to support it
				if let Err(e) = vlog.garbage_collect(commit_pipeline.clone()).await {
					// TODO: Handle error appropriately
					eprintln!("\n VLog GC task error: {e:?}");
				}
				running.store(false, Ordering::SeqCst);
			}
		});

		*self.task_handle.lock().unwrap() = Some(handle);
	}

	/// Triggers VLog garbage collection manually
	pub fn trigger_gc(&self) {
		// Only notify if not already running
		if !self.running.load(Ordering::Acquire) {
			self.notify.notify_one();
		}
	}

	/// Stops the VLog GC background task
	pub async fn stop(&self) {
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
			let mut lock = self.task_handle.lock().unwrap();
			lock.take()
		};

		if let Some(handle) = handle_opt {
			if let Err(e) = handle.await {
				eprintln!("Error shutting down VLog GC task: {e:?}");
			}
		}
	}
}

/// Global Delete List using LSM Tree
/// Uses a dedicated LSM tree for tracking stale user keys.
/// This provides better performance and consistency with the main LSM tree design.
pub struct GlobalDeleteListLSM {
	/// LSM tree for storing delete list entries (user_key -> value_size)
	delete_list_tree: Arc<Tree>,
}

impl GlobalDeleteListLSM {
	pub fn new(base_path: PathBuf) -> Result<Self> {
		let delete_list_path = base_path.join("global_delete_list");

		// Create a dedicated LSM tree for the delete list
		let delete_list_opts = Options {
			path: delete_list_path,
			// Disable VLog for the delete list to avoid circular dependency
			disable_vlog: true,
			..Default::default()
		};

		let delete_list_tree =
			Arc::new(Tree::new(Arc::new(delete_list_opts)).map_err(|e| {
				Error::Other(format!("Failed to create global delete list LSM: {e}"))
			})?);

		Ok(Self {
			delete_list_tree,
		})
	}

	/// Adds multiple stale entries in batch (synchronous)
	/// This is called from CompactionIterator during LSM compaction
	pub fn add_stale_entries_batch(&self, entries: Vec<(u64, u64)>) -> Result<()> {
		if entries.is_empty() {
			return Ok(());
		}

		let mut batch = Batch::new();

		for (seq_num, value_size) in entries {
			// Convert sequence number to a byte array key
			let seq_key = seq_num.to_be_bytes().to_vec();
			// Store sequence number -> value_size mapping
			batch.set(&seq_key, &value_size.to_be_bytes())?;
		}

		// Commit the batch to the LSM tree using sync commit
		self.delete_list_tree
			.sync_commit(batch, true)
			.map_err(|e| Error::Other(format!("Failed to insert into delete list: {e}")))
	}

	/// Checks if a sequence number is in the delete list (stale)
	pub fn is_stale(&self, seq_num: u64) -> Result<bool> {
		let tx = self
			.delete_list_tree
			.begin()
			.map_err(|e| Error::Other(format!("Failed to begin transaction: {e}")))?;

		// Convert sequence number to bytes for lookup
		let seq_key = seq_num.to_be_bytes().to_vec();

		match tx.get(&seq_key) {
			Ok(Some(_)) => Ok(true),
			Ok(None) => Ok(false),
			Err(e) => Err(Error::Other(format!("Failed to search delete list: {e}"))),
		}
	}

	/// Syncs the delete list to disk
	pub fn sync(&self) -> Result<()> {
		self.delete_list_tree
			.flush()
			.map_err(|e| Error::Other(format!("Failed to sync delete list: {e}")))?;
		Ok(())
	}

	/// Deletes multiple entries from the delete list in batch
	/// This is called after successful compaction to clean up the delete list
	pub fn delete_entries_batch(&self, seq_nums: Vec<u64>) -> Result<()> {
		if seq_nums.is_empty() {
			return Ok(());
		}

		let mut batch = Batch::new();

		for seq_num in seq_nums {
			// Convert sequence number to key format
			let seq_key = seq_num.to_be_bytes().to_vec();
			batch.delete(&seq_key)?;
		}

		// Commit the batch to the LSM tree using sync commit
		self.delete_list_tree
			.sync_commit(batch, true)
			.map_err(|e| Error::Other(format!("Failed to delete from delete list: {e}")))
	}
}
#[cfg(test)]
mod tests {
	use super::*;
	use tempfile::TempDir;

	fn create_test_vlog() -> (VLog, TempDir) {
		let temp_dir = TempDir::new().unwrap();
		let opts = Options::default();
		// Create vlog subdirectory to simulate the real directory structure
		let vlog_dir = temp_dir.path().join("vlog");
		std::fs::create_dir_all(&vlog_dir).unwrap();

		// Create a dedicated VLog cache with default size (same as BlockCache)
		let vlog_cache = Arc::new(VLogCache::with_capacity_bytes(1024 * 1024)); // 1MB default

		let vlog = VLog::new(
			&vlog_dir,
			opts.vlog_max_file_size,
			VLogChecksumLevel::Full,
			opts.vlog_gc_discard_ratio, // Use default discard ratio from options
			vlog_cache,
		)
		.unwrap();
		(vlog, temp_dir)
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

		let (max_file, max_discard) = stats.max_discard().unwrap();
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
		let (max_file, max_discard) = stats.max_discard().unwrap();
		assert_eq!(max_file, 1, "File 1 should have maximum discard bytes");
		assert_eq!(max_discard, 600);
	}

	#[tokio::test]
	async fn test_vlog_append_and_get() {
		let (vlog, _temp_dir) = create_test_vlog();

		let key = b"test_key";
		let value = vec![1u8; 300]; // Large enough for vlog
		let pointer = vlog.append(key, &value).unwrap();
		vlog.sync().unwrap();

		let retrieved = vlog.get(&pointer).unwrap();
		assert_eq!(value, *retrieved);
	}

	#[tokio::test]
	async fn test_vlog_small_value_acceptance() {
		let (vlog, _temp_dir) = create_test_vlog();

		let key = b"test_key";
		let small_value = vec![1u8; 10]; // Small value should now be accepted
		let pointer = vlog.append(key, &small_value).unwrap();
		vlog.sync().unwrap();

		let retrieved = vlog.get(&pointer).unwrap();
		assert_eq!(small_value, *retrieved);
	}

	#[tokio::test]
	async fn test_vlog_caching() {
		let (vlog, _temp_dir) = create_test_vlog();

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
		let cached_value = vlog.cache.get(pointer.file_id, pointer.offset);
		assert!(cached_value.is_some());
		assert_eq!(cached_value.unwrap(), value.into());
	}

	#[tokio::test]
	async fn test_prefill_file_handles() {
		let temp_dir = TempDir::new().unwrap();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			vlog_max_file_size: 1024, // Small file size to force multiple files
			..Default::default()
		};

		// Create vlog subdirectory
		let vlog_dir = temp_dir.path().join("vlog");
		std::fs::create_dir_all(&vlog_dir).unwrap();

		// Create a dedicated VLog cache
		let vlog_cache = Arc::new(VLogCache::with_capacity_bytes(1024 * 1024));

		// Create initial VLog and add data to create multiple files
		let vlog1 = VLog::new(
			&vlog_dir,
			opts.vlog_max_file_size,
			VLogChecksumLevel::Full,
			opts.vlog_gc_discard_ratio,
			vlog_cache.clone(),
		)
		.unwrap();

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
		drop(vlog1);

		// Create a new VLog instance - this should trigger prefill_file_handles
		let vlog2 = VLog::new(
			&vlog_dir,
			opts.vlog_max_file_size,
			VLogChecksumLevel::Full,
			opts.vlog_gc_discard_ratio,
			vlog_cache.clone(),
		)
		.unwrap();

		// Verify that all existing files can be read
		for (i, pointer) in pointers.iter().enumerate() {
			let retrieved = vlog2.get(pointer).unwrap();
			let expected_value = vec![i as u8; 200];
			assert_eq!(*retrieved, expected_value);
		}

		// Check that the next_file_id is set correctly (should be one past the highest file ID)
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
		let file_handles = vlog2.file_handles.read().unwrap();
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
	fn test_value_pointer_encode_into_decode_from() {
		let pointer = ValuePointer::new(123, 456, 111, 789, 0xdeadbeef);

		// Test encode_into
		let mut encoded = Vec::new();
		pointer.encode_into(&mut encoded).unwrap();
		assert_eq!(encoded.len(), VALUE_POINTER_SIZE);

		// Test decode_from
		let mut cursor = Cursor::new(&encoded);
		let decoded = ValuePointer::decode_from(&mut cursor).unwrap();
		assert_eq!(pointer, decoded);
	}

	#[test]
	fn test_value_pointer_codec() {
		let pointer = ValuePointer::new(0, 0, 0, 0, 0);

		// Test with zero values
		let mut encoded = Vec::new();
		pointer.encode_into(&mut encoded).unwrap();
		let mut cursor = Cursor::new(&encoded);
		let decoded = ValuePointer::decode_from(&mut cursor).unwrap();
		assert_eq!(pointer, decoded);

		// Test with maximum values
		let max_pointer = ValuePointer::new(u64::MAX, u64::MAX, u32::MAX, u32::MAX, u32::MAX);
		let mut encoded = Vec::new();
		max_pointer.encode_into(&mut encoded).unwrap();
		let mut cursor = Cursor::new(&encoded);
		let decoded = ValuePointer::decode_from(&mut cursor).unwrap();
		assert_eq!(max_pointer, decoded);
	}

	#[test]
	fn test_value_pointer_decode_from_insufficient_data() {
		let incomplete_data = vec![0u8; VALUE_POINTER_SIZE - 1];
		let mut cursor = Cursor::new(&incomplete_data);
		let result = ValuePointer::decode_from(&mut cursor);
		assert!(result.is_err());
	}

	#[test]
	fn test_value_location_inline_encoding() {
		let test_data = b"hello world";
		let location = ValueLocation::Inline(Arc::from(test_data.as_slice()));

		// Test encode
		let encoded = location.encode().unwrap();
		assert_eq!(encoded.len(), 1 + test_data.len()); // tag + data
		assert_eq!(encoded[0], TAG_INLINE);
		assert_eq!(&encoded[1..], test_data);

		// Test decode
		let decoded = ValueLocation::decode(&encoded).unwrap();
		assert_eq!(location, decoded);

		// Verify it's inline
		assert!(decoded.is_inline());
		assert!(!decoded.is_vlog());
	}

	#[test]
	fn test_value_location_vlog_encoding() {
		let pointer = ValuePointer::new(1, 1024, 8, 256, 0x12345678);
		let location = ValueLocation::VLog(pointer.clone());

		// Test encode
		let encoded = location.encode().unwrap();
		assert_eq!(encoded.len(), 1 + VALUE_POINTER_SIZE); // tag + pointer size
		assert_eq!(encoded[0], TAG_VLOG);

		// Test decode
		let decoded = ValueLocation::decode(&encoded).unwrap();
		assert_eq!(location, decoded);

		// Verify it's vlog
		assert!(!decoded.is_inline());
		assert!(decoded.is_vlog());

		// Verify pointer matches
		if let ValueLocation::VLog(decoded_pointer) = decoded {
			assert_eq!(pointer, decoded_pointer);
		} else {
			panic!("Expected VLog variant");
		}
	}

	#[test]
	fn test_value_location_encode_into_decode_from() {
		let test_cases = vec![
			ValueLocation::Inline(Arc::from(b"small data".as_slice())),
			ValueLocation::VLog(ValuePointer::new(1, 100, 10, 50, 0xabcdef)),
			ValueLocation::Inline(Arc::from(b"".as_slice())), // empty data
		];

		for location in test_cases {
			// Test encode_into
			let mut encoded = Vec::new();
			location.encode_into(&mut encoded).unwrap();

			// Test decode_from
			let mut cursor = Cursor::new(&encoded);
			let decoded = ValueLocation::decode_from(&mut cursor).unwrap();

			assert_eq!(location, decoded);
		}
	}

	#[test]
	fn test_value_location_invalid_tag() {
		let invalid_data = vec![99u8, 1, 2, 3]; // Invalid tag 99
		let result = ValueLocation::decode(&invalid_data);
		assert!(result.is_err());

		if let Err(Error::Corruption(msg)) = result {
			assert!(msg.contains("Invalid ValueLocation tag"));
		} else {
			panic!("Expected corruption error");
		}
	}

	#[test]
	fn test_value_location_size_calculation() {
		// Test inline size
		let inline_data = b"test data";
		let inline_location = ValueLocation::Inline(Arc::from(inline_data.as_slice()));
		assert_eq!(inline_location.size().unwrap(), inline_data.len());

		// Test VLog size (without actual VLog)
		let pointer = ValuePointer::new(1, 100, 8, 256, 0x12345);
		let vlog_location = ValueLocation::VLog(pointer);
		assert_eq!(vlog_location.size().unwrap(), VALUE_POINTER_SIZE);
	}

	#[tokio::test]
	async fn test_value_location_resolve_inline() {
		let (vlog, _temp_dir) = create_test_vlog();
		let vlog = Arc::new(vlog);
		let test_data = b"inline test data";
		let location = ValueLocation::Inline(Arc::from(test_data.as_slice()));

		let resolved = location.resolve(vlog).unwrap();
		assert_eq!(&*resolved, test_data);
	}

	#[tokio::test]
	async fn test_value_location_resolve_vlog() {
		let (vlog, _temp_dir) = create_test_vlog();
		let vlog = Arc::new(vlog);
		let key = b"test_key";
		let value = b"test_value_for_vlog_resolution";

		// Append to vlog to get a real pointer
		let pointer = vlog.append(key, value).unwrap();
		vlog.sync().unwrap(); // Sync to ensure data is written to disk
		let location = ValueLocation::VLog(pointer);

		// Resolve should return the original value
		let resolved = location.resolve(vlog).unwrap();
		assert_eq!(&*resolved, value);
	}

	#[test]
	fn test_value_location_from_encoded_value_inline() {
		let test_data = b"encoded inline data";
		let location = ValueLocation::Inline(Arc::from(test_data.as_slice()));
		let encoded = location.encode().unwrap();

		// Should work without VLog for inline data
		let resolved = ValueLocation::from_encoded_value(&encoded, None).unwrap();
		assert_eq!(&*resolved, test_data);
	}

	#[tokio::test]
	async fn test_value_location_from_encoded_value_vlog() {
		let (vlog, _temp_dir) = create_test_vlog();
		let key = b"test_key";
		let value = b"test_value_for_encoded_resolution";

		// Append to vlog and create encoded VLog location
		let pointer = vlog.append(key, value).unwrap();
		vlog.sync().unwrap(); // Sync to ensure data is written to disk
		let location = ValueLocation::VLog(pointer);
		let encoded = location.encode().unwrap();

		// Should resolve with VLog
		let resolved = ValueLocation::from_encoded_value(&encoded, Some(&vlog)).unwrap();
		assert_eq!(&*resolved, value);

		// Should fail without VLog
		let result = ValueLocation::from_encoded_value(&encoded, None);
		assert!(result.is_err());
	}

	#[test]
	fn test_value_location_edge_cases() {
		// Test with maximum size inline data
		let max_inline = vec![0xffu8; u16::MAX as usize];
		let location = ValueLocation::Inline(Arc::from(max_inline.as_slice()));
		let encoded = location.encode().unwrap();
		let decoded = ValueLocation::decode(&encoded).unwrap();
		assert_eq!(location, decoded);

		// Test with pointer containing edge values
		let edge_pointer = ValuePointer::new(0, u64::MAX, 0, u32::MAX, 0);
		let location = ValueLocation::VLog(edge_pointer);
		let encoded = location.encode().unwrap();
		let decoded = ValueLocation::decode(&encoded).unwrap();
		assert_eq!(location, decoded);
	}
}
