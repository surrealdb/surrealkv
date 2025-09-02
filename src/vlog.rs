use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
};

use crate::discard::DiscardStats;
use crate::vfs;

use crc32fast::Hasher;

use crate::commit::CommitPipeline;
use crate::error::{Error, Result};
use crate::levels::LevelManifest;
use crate::memtable::{ImmutableMemtables, MemTable};

const GC_DISCARD_RATIO_THRESHOLD: f64 = 0.5; // 50% discardable data triggers GC

/// Size of a ValuePointer when encoded (8 + 8 + 4 + 4 + 4 bytes)
pub const VALUE_POINTER_SIZE: usize = 28;

/// Represents where a value is stored - either inline in the SSTable or in the value log
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueLocation {
    /// Value is stored inline in the SSTable
    Inline(Vec<u8>),
    /// Value is stored in the value log, referenced by a pointer
    VLog(ValuePointer),
}

impl ValueLocation {
    #[cfg(test)]
    fn from_value(value: &[u8], threshold: usize) -> Self {
        if value.len() < threshold {
            ValueLocation::Inline(value.to_vec())
        } else {
            // For values that should go to VLog, we return a default pointer
            // The actual VLog storage will be handled by the LSM tree
            ValueLocation::VLog(ValuePointer::default())
        }
    }

    /// Creates a ValueLocation from encoded value data, attempting to decode VLog pointers
    pub fn from_encoded_value(encoded: &[u8], vlog: Option<&VLog>) -> Result<Vec<u8>> {
        if let Some(pointer) = ValuePointer::try_decode(encoded)? {
            // Valid VLog pointer, resolve it
            if let Some(vlog) = vlog {
                return vlog.get(&pointer);
            }
        }
        // Not a VLog pointer or no VLog available, return as-is
        Ok(encoded.to_vec())
    }

    /// Encodes the ValueLocation for storage
    pub fn encode(&self) -> Vec<u8> {
        match self {
            ValueLocation::Inline(data) => data.clone(),
            ValueLocation::VLog(pointer) => pointer.encode(),
        }
    }

    /// Decodes a ValueLocation from stored data
    pub fn decode(data: &[u8]) -> Result<Self> {
        if let Some(pointer) = ValuePointer::try_decode(data)? {
            return Ok(ValueLocation::VLog(pointer));
        }
        Ok(ValueLocation::Inline(data.to_vec()))
    }

    /// Resolves the actual value, reading from VLog if necessary
    pub fn resolve(&self, vlog: &VLog) -> Result<Vec<u8>> {
        match self {
            ValueLocation::Inline(data) => Ok(data.clone()),
            ValueLocation::VLog(pointer) => vlog.get(pointer),
        }
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

    /// Checks if the given data could potentially be a ValuePointer
    /// This is a lightweight check that only verifies the size
    pub fn could_be_pointer(data: &[u8]) -> bool {
        data.len() == VALUE_POINTER_SIZE
    }

    /// Attempts to decode data as a ValuePointer, returning None if it's clearly not a pointer
    /// This performs additional validation beyond just size checking
    pub fn try_decode(data: &[u8]) -> Result<Option<Self>> {
        if !Self::could_be_pointer(data) {
            return Ok(None);
        }

        if let Ok(pointer) = Self::decode(data) {
            // Additional validation: a valid VLog pointer should have non-zero checksum
            if pointer.checksum != 0 && pointer.total_entry_size() > 0 {
                Ok(Some(pointer))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Encodes the pointer as bytes for storage
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(VALUE_POINTER_SIZE);
        encoded.extend_from_slice(&self.file_id.to_le_bytes());
        encoded.extend_from_slice(&self.offset.to_le_bytes());
        encoded.extend_from_slice(&self.key_size.to_le_bytes());
        encoded.extend_from_slice(&self.value_size.to_le_bytes());
        encoded.extend_from_slice(&self.checksum.to_le_bytes());
        encoded
    }

    /// Decodes a pointer from bytes
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() != VALUE_POINTER_SIZE {
            return Err(Error::Corruption("Invalid ValuePointer size".to_string()));
        }

        let file_id = u64::from_le_bytes([
            data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
        ]);
        let offset = u64::from_le_bytes([
            data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
        ]);
        let key_size = u32::from_le_bytes([data[16], data[17], data[18], data[19]]);
        let value_size = u32::from_le_bytes([data[20], data[21], data[22], data[23]]);
        let checksum = u32::from_le_bytes([data[24], data[25], data[26], data[27]]);

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
        Self { id, path, size }
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
        self.writer.write_all(&key_len.to_le_bytes())?;
        self.writer.write_all(&value_len.to_le_bytes())?;

        // Write key
        self.writer.write_all(key)?;

        // Write value
        self.writer.write_all(value)?;

        // Write CRC32
        self.writer.write_all(&crc32.to_le_bytes())?;

        let entry_size = 8 + key.len() as u64 + value.len() as u64 + 4; // header + key + value + crc32
        self.current_offset += entry_size;
        self.bytes_written += entry_size;

        Ok(ValuePointer::new(
            self.file_id,
            offset,
            key_len,
            value_len,
            crc32,
        ))
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

    /// Threshold for storing values in VLog vs LSM tree
    value_threshold: u32,

    /// Maximum size for each VLog file before rotation
    max_file_size: u64,

    /// Checksum verification level
    checksum_level: crate::VLogChecksumLevel,

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

    /// LSM components for staleness detection during GC
    active_memtable: Arc<RwLock<Arc<MemTable>>>,
    immutable_memtables: Arc<RwLock<ImmutableMemtables>>,
    levels: Arc<RwLock<LevelManifest>>,
}

impl VLog {
    /// Returns the file path for a VLog file with the given ID
    fn vlog_file_path(&self, file_id: u64) -> PathBuf {
        self.path.join(format!("vlog_{:08}.log", file_id))
    }

    /// Creates a new VLog instance
    pub fn new<P: AsRef<Path>>(
        dir: P,
        value_threshold: usize,
        max_file_size: u64,
        checksum_level: crate::VLogChecksumLevel,
        active_memtable: Arc<RwLock<Arc<MemTable>>>,
        immutable_memtables: Arc<RwLock<ImmutableMemtables>>,
        levels: Arc<RwLock<LevelManifest>>,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // Get the parent directory for the discard stats file
        // This ensures the DISCARD file lives in the main database directory,
        // not in the VLog subdirectory
        let discard_stats_dir = dir
            .parent()
            .ok_or_else(|| Error::Other("VLog directory has no parent".to_string()))?;

        let vlog = Self {
            path: dir.clone(),
            value_threshold: value_threshold as u32,
            max_file_size,
            checksum_level,
            next_file_id: AtomicU64::new(0),
            active_writer_id: AtomicU64::new(0),
            writer: RwLock::new(None),
            num_active_iterators: AtomicI32::new(0),
            files_map: RwLock::new(HashMap::new()),
            file_handles: RwLock::new(HashMap::new()),
            files_to_be_deleted: RwLock::new(Vec::new()),
            gc_in_progress: AtomicBool::new(false),
            discard_stats: Mutex::new(DiscardStats::new(discard_stats_dir)?),
            active_memtable,
            immutable_memtables,
            levels,
        };

        // Clean up any leftover temporary files from previous interrupted compactions
        vlog.cleanup_temp_files()?;

        Ok(vlog)
    }

    /// Appends a key+value pair to the log and returns a ValuePointer
    pub fn append(&self, key: &[u8], value: &[u8]) -> Result<ValuePointer> {
        // Only store in vlog if value is large enough
        if value.len() < self.value_threshold as usize {
            return Err(Error::Other("Value too small for vlog storage".to_string()));
        }

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
    pub fn get(&self, pointer: &ValuePointer) -> Result<Vec<u8>> {
        let file = self.get_file_handle(pointer.file_id)?;

        // Read the entire entry in a single call using sizes from the pointer
        let key_len = pointer.key_size;
        let value_len = pointer.value_size;
        let total_size = pointer.total_entry_size();
        let mut entry_data = vec![0u8; total_size as usize];
        vfs::File::read_at(&*file, pointer.offset, &mut entry_data)?;

        // Verify header sizes
        let header_key_len =
            u32::from_le_bytes([entry_data[0], entry_data[1], entry_data[2], entry_data[3]]);
        let header_value_len =
            u32::from_le_bytes([entry_data[4], entry_data[5], entry_data[6], entry_data[7]]);

        if header_key_len != key_len || header_value_len != value_len {
            return Err(Error::Corruption(format!(
                "Header size mismatch: expected key {} value {}, got key {} value {}",
                key_len, value_len, header_key_len, header_value_len
            )));
        }

        // Parse from the buffer
        let key_start = 8;
        let value_start = key_start + key_len as usize;
        let crc_start = value_start + value_len as usize;

        let key = &entry_data[key_start..value_start];
        let value = &entry_data[value_start..crc_start];
        let stored_crc32 = u32::from_le_bytes([
            entry_data[crc_start],
            entry_data[crc_start + 1],
            entry_data[crc_start + 2],
            entry_data[crc_start + 3],
        ]);

        // Checksum verification
        if self.checksum_level != crate::VLogChecksumLevel::Disabled
            && stored_crc32 != pointer.checksum
        {
            return Err(Error::Corruption(format!(
                "CRC32 mismatch: expected {}, got {}",
                pointer.checksum, stored_crc32
            )));
        }

        if self.checksum_level == crate::VLogChecksumLevel::Full {
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

        Ok(value.to_vec())
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
                    eprintln!(
                        "Warning: Failed to delete VLog file {:?}: {}",
                        vlog_file.path, e
                    );
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
            let (total_size, _expected_discard, discard_ratio) = self.get_file_stats(file_id);

            if total_size == 0 {
                false
            } else {
                discard_ratio >= GC_DISCARD_RATIO_THRESHOLD
            }
        };

        if !should_compact {
            return Ok(vec![]);
        }

        // Compact the selected file
        let compacted = self
            .compact_vlog_file_safe(file_id, commit_pipeline.clone())
            .await?;

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
                    "Value log file already marked for deletion fid: {}",
                    file_id
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
        let compacted = self
            .compact_vlog_file(file_id, commit_pipeline.clone())
            .await?;

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

        let mut batch = crate::batch::Batch::default();
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

            let key_len = u32::from_le_bytes(key_len_buf);

            let mut value_len_buf = [0u8; 4];
            source_file.read_exact(&mut value_len_buf)?;
            let value_len = u32::from_le_bytes(value_len_buf);

            // Read key
            let mut key = vec![0u8; key_len as usize];
            source_file.read_exact(&mut key)?;

            // Read value
            let mut value = vec![0u8; value_len as usize];
            source_file.read_exact(&mut value)?;

            // Read CRC32
            let mut crc32_buf = [0u8; 4];
            source_file.read_exact(&mut crc32_buf)?;
            let _crc32 = u32::from_le_bytes(crc32_buf);

            let entry_size = 8 + key_len as u64 + value_len as u64 + 4; // header + key + value + crc32

            // Check if the LSM tree still points to this exact location
            let should_rewrite = {
                // The key stored in VLog is the encoded internal key (user_key + seq_num + kind)
                // We need to extract the user key to query the LSM tree
                if key.len() >= 8 {
                    let internal_key = crate::sstable::InternalKey::decode(&key);

                    match self.get_from_tree(&internal_key.user_key) {
                        Ok(Some(current_value)) => {
                            // Key exists in LSM - check if the current version matches
                            if let Some(current_pointer) =
                                crate::vlog::ValuePointer::try_decode(&current_value)?
                            {
                                // This entry is still valid if the LSM points to this exact VLog location

                                current_pointer.file_id == file_id
                                    && current_pointer.offset == offset
                            } else {
                                // LSM value is not a valid VLog pointer (inline or invalid), so this VLog entry is stale
                                false
                            }
                        }
                        Ok(None) => {
                            // Key no longer exists in LSM, so this VLog entry is stale
                            false
                        }
                        Err(e) => {
                            eprintln!("Error reading from LSM during compaction: {:?}", e);
                            // Error reading from LSM - conservatively keep the entry
                            true
                        }
                    }
                } else {
                    // Key too short to be a valid internal key - conservatively keep the entry
                    true
                }
            };

            if !should_rewrite {
                // Skip this entry (it's stale)
                values_skipped += 1;
            } else {
                // Extract the user key from the internal key
                if key.len() >= 8 {
                    let internal_key = crate::sstable::InternalKey::decode(&key);

                    // Add this entry to the batch to be rewritten
                    // This will cause the value to be written to the active VLog file
                    // and a new pointer to be stored in the LSM
                    batch.set(&internal_key.user_key, &value)?;

                    // Update batch size tracking
                    batch_size += internal_key.user_key.len() + value.len();

                    // If batch is full, commit it
                    if batch.count() >= MAX_BATCH_COUNT as u32 || batch_size >= MAX_BATCH_SIZE {
                        // Commit the batch to LSM (and VLog for large values)
                        commit_pipeline.commit(batch, true).await?;

                        // Reset batch
                        batch = crate::batch::Batch::default();
                        batch_size = 0;
                    }
                }
            }

            offset += entry_size;
        }

        // Commit any remaining entries in the batch
        if !batch.is_empty() {
            commit_pipeline.commit(batch, true).await?;
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

    /// Gets the value size threshold
    pub fn value_threshold(&self) -> usize {
        self.value_threshold as usize
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
                    eprintln!(
                        "Warning: Failed to cleanup temp file {:?}: {}",
                        temp_path, e
                    );
                }
            }
        }

        Ok(())
    }

    /// Updates discard statistics for a file
    /// This should be called during LSM compaction when outdated VLog pointers are found
    pub fn update_discard_stats(&self, stats: HashMap<u64, i64>) {
        let mut discard_stats = self.discard_stats.lock().unwrap();
        for (file_id, discard_bytes) in stats {
            discard_stats.update(file_id, discard_bytes);
        }
    }

    /// Internal get method for VLog GC to check if entries are live
    /// Returns the current value for a key from the LSM tree
    fn get_from_tree(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        use crate::sstable::{InternalKey, InternalKeyKind};

        // Use a high sequence number for GC reads (we want the most recent state)
        let seq_num = u64::MAX;

        // Check active memtable first
        {
            let active_memtable = self.active_memtable.read().unwrap();
            if let Some((internal_key, value)) = active_memtable.get(key, Some(seq_num)) {
                if internal_key.is_tombstone() {
                    return Ok(None); // Key is deleted
                } else {
                    return Ok(Some(value.to_vec())); // Key found
                }
            }
        }

        // Check immutable memtables
        {
            let immutable_memtables = self.immutable_memtables.read().unwrap();
            for (_, memtable) in immutable_memtables.iter().rev() {
                if let Some((internal_key, value)) = memtable.get(key, Some(seq_num)) {
                    if internal_key.is_tombstone() {
                        return Ok(None); // Key is deleted
                    } else {
                        return Ok(Some(value.to_vec())); // Key found
                    }
                }
            }
        }

        // Check SSTables in levels
        {
            let levels = self.levels.read().unwrap();
            for level in levels.levels.get_levels() {
                for table in &level.tables {
                    // Create an InternalKey for the search
                    let search_key = InternalKey::new(key.to_vec(), seq_num, InternalKeyKind::Max);
                    if let Some((internal_key, value)) = table.get(search_key)? {
                        if internal_key.is_tombstone() {
                            return Ok(None); // Key is deleted
                        } else {
                            return Ok(Some(value.to_vec())); // Key found
                        }
                    }
                }
            }
        }

        Ok(None)
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
                    eprintln!("\n VLog GC task error: {:?}", e);
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
                eprintln!("Error shutting down VLog GC task: {:?}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_vlog() -> (VLog, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let opts = crate::Options::default();
        // Create vlog subdirectory to simulate the real directory structure
        let vlog_dir = temp_dir.path().join("vlog");
        std::fs::create_dir_all(&vlog_dir).unwrap();
        let vlog = VLog::new(
            &vlog_dir,
            opts.vlog_value_threshold,
            opts.vlog_max_file_size,
            crate::VLogChecksumLevel::Full,
            Arc::new(RwLock::new(Arc::new(MemTable::default()))),
            Arc::new(RwLock::new(ImmutableMemtables::default())),
            Arc::new(RwLock::new(
                LevelManifest::new(opts.clone().into()).unwrap(),
            )),
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
        // Test with valid VLog pointer
        let pointer = ValuePointer::new(123, 456, 11, 789, 0xdeadbeef);
        let encoded = pointer.encode();

        assert!(ValuePointer::could_be_pointer(&encoded));
        assert_eq!(ValuePointer::try_decode(&encoded).unwrap(), Some(pointer));

        // Test with wrong size data
        let wrong_size = vec![0u8; 20];
        assert!(!ValuePointer::could_be_pointer(&wrong_size));
        assert_eq!(ValuePointer::try_decode(&wrong_size).unwrap(), None);

        // Test with correct size but invalid content (zero checksum)
        let invalid_pointer = ValuePointer::new(123, 456, 11, 789, 0); // zero checksum
        let invalid_encoded = invalid_pointer.encode();
        assert!(ValuePointer::could_be_pointer(&invalid_encoded));
        assert_eq!(ValuePointer::try_decode(&invalid_encoded).unwrap(), None);

        // Test with random VALUE_POINTER_SIZE-byte data
        let random_data = vec![0x42u8; VALUE_POINTER_SIZE];
        assert!(ValuePointer::could_be_pointer(&random_data));
        // is_likely_pointer should be more conservative and might reject this
    }

    #[test]
    fn test_value_location_encoding() {
        // Test inline value
        let inline_value = ValueLocation::Inline(b"hello".to_vec());
        let encoded = inline_value.encode();
        let decoded = ValueLocation::decode(&encoded).unwrap();
        assert_eq!(inline_value, decoded);

        // Test vlog pointer
        let pointer = ValuePointer::new(1, 100, 3, 5, 0x12345678);
        let vlog_value = ValueLocation::VLog(pointer);
        let encoded = vlog_value.encode();
        let decoded = ValueLocation::decode(&encoded).unwrap();
        assert_eq!(vlog_value, decoded);
    }

    #[test]
    fn test_value_location_from_value() {
        let opts = crate::Options::default();

        // Small value should be inline
        let small_value = vec![0u8; 100];
        let location = ValueLocation::from_value(&small_value, opts.vlog_value_threshold);
        assert!(matches!(location, ValueLocation::Inline(_)));

        // Large value should use vlog
        let large_value = vec![0u8; 300];
        let location = ValueLocation::from_value(&large_value, opts.vlog_value_threshold);
        assert!(matches!(location, ValueLocation::VLog(_)));
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

    #[test]
    fn test_vlog_append_and_get() {
        let (vlog, _temp_dir) = create_test_vlog();

        let key = b"test_key";
        let value = vec![1u8; 300]; // Large enough for vlog
        let pointer = vlog.append(key, &value).unwrap();
        vlog.sync().unwrap();

        let retrieved = vlog.get(&pointer).unwrap();
        assert_eq!(value, retrieved);
    }

    #[test]
    fn test_vlog_small_value_rejection() {
        let (vlog, _temp_dir) = create_test_vlog();

        let key = b"test_key";
        let small_value = vec![1u8; 100]; // Too small for vlog
        let result = vlog.append(key, &small_value);
        assert!(result.is_err());
    }
}
