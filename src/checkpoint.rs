use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::error::{Error, Result};
use crate::lsm::{CompactionOperations, CoreInner, LEVELS_MANIFEST_FILE, TABLE_FOLDER};

/// Current checkpoint metadata format version
const CHECKPOINT_VERSION: u32 = 1;

/// Checkpoint file name
const CHECKPOINT_METADATA_FILE: &str = "CHECKPOINT_METADATA";

/// Database checkpoint metadata
#[derive(Debug, Clone)]
pub struct CheckpointMetadata {
    /// Format version for compatibility checking
    pub version: u32,
    /// Timestamp when the checkpoint was created
    pub timestamp: u64,
    /// Sequence number at the time of checkpoint
    pub sequence_number: u64,
    /// Number of SSTables included in the checkpoint
    pub sstable_count: usize,
    /// Total size of the checkpoint in bytes
    pub total_size: u64,
}

impl CheckpointMetadata {
    /// Creates new checkpoint metadata with current version
    pub fn new(
        timestamp: u64,
        sequence_number: u64,
        sstable_count: usize,
        total_size: u64,
    ) -> Self {
        Self {
            version: CHECKPOINT_VERSION,
            timestamp,
            sequence_number,
            sstable_count,
            total_size,
        }
    }

    /// Checks if this metadata version is compatible with current implementation
    pub fn is_compatible(&self) -> bool {
        // For now, we only support version 1
        // In the future, you can add backwards compatibility logic here
        self.version == CHECKPOINT_VERSION
    }

    /// Serializes the metadata to binary format
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Write version first for compatibility checking
        buf.write_u32::<BigEndian>(self.version)
            .map_err(|e| Error::Io(Arc::new(e)))?;

        // Write timestamp
        buf.write_u64::<BigEndian>(self.timestamp)
            .map_err(|e| Error::Io(Arc::new(e)))?;

        // Write sequence number
        buf.write_u64::<BigEndian>(self.sequence_number)
            .map_err(|e| Error::Io(Arc::new(e)))?;

        // Write sstable count (convert usize to u64 for portability)
        buf.write_u64::<BigEndian>(self.sstable_count as u64)
            .map_err(|e| Error::Io(Arc::new(e)))?;

        // Write total size
        buf.write_u64::<BigEndian>(self.total_size)
            .map_err(|e| Error::Io(Arc::new(e)))?;

        Ok(buf)
    }

    /// Deserializes metadata from binary format
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let mut reader = std::io::Cursor::new(data);

        // Read version first
        let version = reader
            .read_u32::<BigEndian>()
            .map_err(|e| Error::Other(format!("Failed to read version: {e}")))?;

        // Check if we can handle this version
        if version > CHECKPOINT_VERSION {
            return Err(Error::Other(format!(
                "Unsupported checkpoint version: {version}. Current version: {CHECKPOINT_VERSION}"
            )));
        }

        // Read remaining fields
        let timestamp = reader
            .read_u64::<BigEndian>()
            .map_err(|e| Error::Other(format!("Failed to read timestamp: {e}")))?;

        let sequence_number = reader
            .read_u64::<BigEndian>()
            .map_err(|e| Error::Other(format!("Failed to read sequence_number: {e}")))?;

        let sstable_count = reader
            .read_u64::<BigEndian>()
            .map_err(|e| Error::Other(format!("Failed to read sstable_count: {e}")))?
            as usize;

        let total_size = reader
            .read_u64::<BigEndian>()
            .map_err(|e| Error::Other(format!("Failed to read total_size: {e}")))?;

        Ok(CheckpointMetadata {
            version,
            timestamp,
            sequence_number,
            sstable_count,
            total_size,
        })
    }
}

/// Database checkpoint manager for creating consistent point-in-time snapshots
pub struct DatabaseCheckpoint {
    /// Reference to the LSM core
    core: Arc<CoreInner>,
}

impl DatabaseCheckpoint {
    /// Creates a new database checkpoint manager
    pub fn new(core: Arc<CoreInner>) -> Self {
        Self { core }
    }

    /// Creates a new database checkpoint at the specified directory.
    ///
    /// This creates a consistent point-in-time snapshot that includes:
    /// - All SSTables from all levels
    /// - Current WAL segments
    /// - Level manifest
    /// - Checkpoint metadata
    ///
    /// # Arguments
    /// * `checkpoint_dir` - Directory where the checkpoint will be created
    ///
    /// # Returns
    /// Metadata about the created checkpoint
    pub fn create_checkpoint<P: AsRef<Path>>(
        &self,
        checkpoint_dir: P,
    ) -> Result<CheckpointMetadata> {
        let checkpoint_path = checkpoint_dir.as_ref();

        // Create checkpoint directory
        fs::create_dir_all(checkpoint_path).map_err(|e| Error::Io(Arc::new(e)))?;

        // Step 1: Flush all memtables to ensure consistency
        self.flush_all_memtables()?;

        // Step 2: Get current sequence number from the levels
        let sequence_number = {
            let levels_guard = self.core.levels.read().unwrap();
            levels_guard.lsn()
        };

        // Step 3: Create checkpoint subdirectories
        let sstables_dir = checkpoint_path.join(TABLE_FOLDER);
        let wal_dir = checkpoint_path.join("wal");
        fs::create_dir_all(&sstables_dir).map_err(|e| Error::Io(Arc::new(e)))?;
        fs::create_dir_all(&wal_dir).map_err(|e| Error::Io(Arc::new(e)))?;

        // Step 4: Copy all SSTables
        let (sstable_count, sstables_size) = self.copy_sstables(&sstables_dir)?;

        // Step 5: Copy WAL segments
        self.create_new_wal(&wal_dir)?;

        // Step 6: Copy level manifest
        let manifest_size = self.copy_level_manifest(checkpoint_path)?;

        // Step 7: Create checkpoint metadata
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let metadata = CheckpointMetadata::new(
            timestamp,
            sequence_number,
            sstable_count,
            sstables_size + manifest_size,
        );

        // Step 8: Write metadata file
        self.write_checkpoint_metadata(checkpoint_path, &metadata)?;

        Ok(metadata)
    }

    /// Restores the database from a checkpoint directory.
    /// This will overwrite the current database state
    pub fn restore_from_checkpoint<P: AsRef<Path>>(
        &self,
        checkpoint_dir: P,
    ) -> Result<CheckpointMetadata> {
        let checkpoint_path = checkpoint_dir.as_ref();

        // Verify checkpoint exists and is valid
        let metadata = self.read_checkpoint_metadata(checkpoint_path)?;

        // Clear current database state
        self.clear_current_state()?;

        // Restore SSTables
        let sstables_source = checkpoint_path.join(TABLE_FOLDER);
        let sstables_dest = self.core.opts.path.join(TABLE_FOLDER);
        if sstables_source.exists() {
            Self::copy_directory_sync(&sstables_source, &sstables_dest)?;
        }

        // Restore WAL segments
        let wal_source = checkpoint_path.join("wal");
        let wal_dest = self.core.opts.path.join("wal");
        if wal_source.exists() {
            Self::copy_directory_sync(&wal_source, &wal_dest)?;
        }

        // Restore level manifest
        let manifest_source = checkpoint_path.join(LEVELS_MANIFEST_FILE);
        let manifest_dest = self.core.opts.path.join(LEVELS_MANIFEST_FILE);
        if manifest_source.exists() {
            fs::copy(&manifest_source, &manifest_dest).map_err(|e| Error::Io(Arc::new(e)))?;
        }

        Ok(metadata)
    }

    /// Flushes all memtables to ensure checkpoint consistency
    fn flush_all_memtables(&self) -> Result<()> {
        // Keep calling compact_memtable until all memtables (active + immutable) are flushed
        // compact_memtable already handles the logic of checking if there's anything to flush
        loop {
            // Check if there are any memtables to flush
            let has_active = {
                let active_guard = self.core.active_memtable.read().unwrap();
                !active_guard.is_empty()
            };

            let has_immutable = {
                let immutable_guard = self.core.immutable_memtables.read().unwrap();
                !immutable_guard.is_empty()
            };

            if !has_active && !has_immutable {
                break; // All memtables are flushed
            }

            // Flush one memtable (active or immutable)
            self.core.compact_memtable()?;
        }

        Ok(())
    }

    /// Copies all SSTables to the checkpoint directory
    fn copy_sstables(&self, dest_dir: &Path) -> Result<(usize, u64)> {
        let levels_guard = self.core.levels.read().unwrap();
        let mut total_size = 0u64;
        let mut count = 0usize;

        // Use the iterator method to iterate over all tables
        for table in levels_guard.iter() {
            // Construct the source path using the table ID, similar to load_table
            let sstable_dir = self.core.opts.path.join(TABLE_FOLDER);
            let source_path = sstable_dir.join(format!("{}", table.id));

            let filename = source_path
                .file_name()
                .ok_or_else(|| Error::Other("Invalid SSTable path".to_string()))?;
            let dest_path = dest_dir.join(filename);

            // Create hard link if possible (faster), otherwise copy
            if fs::hard_link(&source_path, &dest_path).is_err() {
                fs::copy(&source_path, &dest_path).map_err(|e| Error::Io(Arc::new(e)))?;
            }

            // Add to size count
            if let Ok(metadata) = fs::metadata(&dest_path) {
                total_size += metadata.len();
            }
            count += 1;
        }

        Ok((count, total_size))
    }

    /// Creates a new empty WAL directory structure for the checkpoint
    fn create_new_wal(&self, dest_dir: &Path) -> Result<()> {
        // Since we flush all memtables before creating a checkpoint,
        // all data is already persisted in SSTables. We don't need to copy
        // any WAL segments as they would only contain data that's already
        // in the SSTables.
        //
        // We create an empty WAL directory structure for the restored database.
        fs::create_dir_all(dest_dir).map_err(|e| Error::Io(Arc::new(e)))?;

        // Create an empty checkpoint subdirectory for WAL checkpoint tracking
        let checkpoint_subdir = dest_dir.join("checkpoint");
        fs::create_dir_all(&checkpoint_subdir).map_err(|e| Error::Io(Arc::new(e)))?;

        Ok(())
    }

    /// Copies the level manifest to the checkpoint directory
    fn copy_level_manifest(&self, dest_dir: &Path) -> Result<u64> {
        let source_path = self.core.opts.path.join(LEVELS_MANIFEST_FILE);
        let dest_path = dest_dir.join(LEVELS_MANIFEST_FILE);

        if source_path.exists() {
            fs::copy(&source_path, &dest_path).map_err(|e| Error::Io(Arc::new(e)))?;

            if let Ok(metadata) = fs::metadata(&dest_path) {
                return Ok(metadata.len());
            }
        }

        Ok(0)
    }

    /// Writes checkpoint metadata to a file
    fn write_checkpoint_metadata(
        &self,
        checkpoint_dir: &Path,
        metadata: &CheckpointMetadata,
    ) -> Result<()> {
        let metadata_path = checkpoint_dir.join(CHECKPOINT_METADATA_FILE);
        let mut file = File::create(&metadata_path).map_err(|e| Error::Io(Arc::new(e)))?;

        let data = metadata.to_bytes()?;
        file.write_all(&data).map_err(|e| Error::Io(Arc::new(e)))?;
        file.flush().map_err(|e| Error::Io(Arc::new(e)))?;

        Ok(())
    }

    /// Reads checkpoint metadata from a file
    fn read_checkpoint_metadata(&self, checkpoint_dir: &Path) -> Result<CheckpointMetadata> {
        let metadata_path = checkpoint_dir.join(CHECKPOINT_METADATA_FILE);
        let data = fs::read(&metadata_path).map_err(|e| Error::Io(Arc::new(e)))?;

        CheckpointMetadata::from_bytes(&data)
    }

    /// Helper function to copy a directory recursively (synchronous version to avoid recursion issues)
    fn copy_directory_sync(source: &Path, dest: &Path) -> Result<u64> {
        if !source.exists() {
            return Ok(0);
        }

        fs::create_dir_all(dest).map_err(|e| Error::Io(Arc::new(e)))?;

        let mut total_size = 0u64;

        for entry in fs::read_dir(source).map_err(|e| Error::Io(Arc::new(e)))? {
            let entry = entry.map_err(|e| Error::Io(Arc::new(e)))?;

            let source_path = entry.path();
            let dest_path = dest.join(entry.file_name());

            if source_path.is_file() {
                // Create hard link if possible, otherwise copy
                if fs::hard_link(&source_path, &dest_path).is_err() {
                    fs::copy(&source_path, &dest_path).map_err(|e| Error::Io(Arc::new(e)))?;
                }

                if let Ok(metadata) = fs::metadata(&dest_path) {
                    total_size += metadata.len();
                }
            } else if source_path.is_dir() {
                total_size += Self::copy_directory_sync(&source_path, &dest_path)?;
            }
        }

        Ok(total_size)
    }

    /// Clears the current database state (for restoration)
    fn clear_current_state(&self) -> Result<()> {
        // Clear SSTables directory
        let sstables_dir = self.core.opts.path.join(TABLE_FOLDER);
        if sstables_dir.exists() {
            fs::remove_dir_all(&sstables_dir).map_err(|e| Error::Io(Arc::new(e)))?;
        }

        // Clear WAL directory
        let wal_dir = self.core.opts.path.join("wal");
        if wal_dir.exists() {
            fs::remove_dir_all(&wal_dir).map_err(|e| Error::Io(Arc::new(e)))?;
        }

        // Remove level manifest
        let manifest_path = self.core.opts.path.join(LEVELS_MANIFEST_FILE);
        if manifest_path.exists() {
            fs::remove_file(&manifest_path).map_err(|e| Error::Io(Arc::new(e)))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_metadata_serialization() {
        let original = CheckpointMetadata::new(
            1234567890,  // timestamp
            100,         // sequence_number
            100,         // sstable_count
            1024 * 1024, // total_size (1MB)
        );

        // Test round-trip serialization
        let bytes = original.to_bytes().expect("Serialization should succeed");
        let deserialized =
            CheckpointMetadata::from_bytes(&bytes).expect("Deserialization should succeed");

        assert_eq!(original.version, deserialized.version);
        assert_eq!(original.timestamp, deserialized.timestamp);
        assert_eq!(original.sequence_number, deserialized.sequence_number);
        assert_eq!(original.sstable_count, deserialized.sstable_count);
        assert_eq!(original.total_size, deserialized.total_size);
    }

    #[test]
    fn test_checkpoint_version_compatibility() {
        let metadata = CheckpointMetadata::new(0, 0, 0, 0);
        assert!(metadata.is_compatible());

        // Test future version rejection
        let mut future_data = Vec::new();
        future_data.extend_from_slice(&999u32.to_be_bytes()); // version 999
        future_data.extend_from_slice(&[0u8; 32]); // dummy data

        let result = CheckpointMetadata::from_bytes(&future_data);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Unsupported checkpoint version"));
    }

    #[test]
    fn test_checkpoint_metadata_binary_format() {
        let metadata = CheckpointMetadata::new(100, 200, 300, 400);
        let bytes = metadata.to_bytes().unwrap();

        // Verify the binary format structure
        // 4 bytes version + 8 bytes timestamp + 8 bytes seq + 8 bytes count + 8 bytes size = 36 bytes
        assert_eq!(bytes.len(), 36);

        // Check that version is at the beginning (big endian)
        assert_eq!(&bytes[0..4], &1u32.to_be_bytes());
    }
}
