use std::{
    collections::{HashMap, HashSet},
    fs::File as SysFile,
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use crate::{
    error::Error,
    lsm::{LEVELS_MANIFEST_FILE, TABLE_FOLDER},
    sstable::table::Table,
    vfs::File,
    Options, Result,
};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use iter::LevelManifestIterator;
pub(crate) use level::{Level, Levels};

mod iter;
mod level;

pub type HiddenSet = HashSet<u64>;

/// Represents the levels of a log-structured merge tree.
pub struct LevelManifest {
    /// Path of level manifest file
    pub path: PathBuf,

    /// Levels of the LSM tree
    pub levels: Levels,

    /// Set of hidden tables that should not appear during compaction
    pub(crate) hidden_set: HiddenSet,

    /// Next table ID to use (persisted to disk for safe recovery)
    pub(crate) next_table_id: Arc<AtomicU64>,
}

impl LevelManifest {
    pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
        assert!(opts.level_count > 0, "level_count should be >= 1");

        let level_path = opts.path.join(LEVELS_MANIFEST_FILE);
        let sstable_path = opts.path.join(TABLE_FOLDER);

        // Check if the manifest file already exists
        if level_path.exists() {
            // Load existing manifest
            let (loaded_levels, next_id) =
                Self::load_with_tables(&level_path, &sstable_path, opts)?;

            // Initialize with the loaded next_table_id
            let manifest = Self {
                path: level_path.to_path_buf(),
                levels: loaded_levels,
                hidden_set: HashSet::with_capacity(10),
                next_table_id: Arc::new(AtomicU64::new(next_id)),
            };

            // Ensure manifest is written to disk with the counter
            write_levels_to_disk(
                &manifest.path,
                &manifest.levels,
                manifest.next_table_id.load(Ordering::SeqCst),
            )?;

            return Ok(manifest);
        }

        // If no manifest exists, create a new one

        // Initialize levels with default values
        let levels = Self::initialize_levels(opts.level_count);

        // Start with next_table_id = 1 (0 is often reserved)
        let next_table_id = Arc::new(AtomicU64::new(1));

        let manifest = Self {
            path: level_path.to_path_buf(),
            levels,
            hidden_set: HashSet::with_capacity(10),
            next_table_id,
        };

        // Write levels to disk with the counter
        write_levels_to_disk(
            &manifest.path,
            &manifest.levels,
            manifest.next_table_id.load(Ordering::SeqCst),
        )?;

        Ok(manifest)
    }

    pub(crate) fn lsn(&self) -> u64 {
        let level0 = self.levels.get_levels().first();
        if let Some(level) = level0 {
            if !level.tables.is_empty() {
                let last_table = level.tables.last().unwrap();
                return last_table.meta.largest_seq_num;
            }
        }
        0
    }

    /// Initializes levels with default values
    fn initialize_levels(level_count: u8) -> Levels {
        let levels = (0..level_count)
            .map(|_| Arc::new(Level::default()))
            .collect::<Vec<_>>();

        Levels(levels)
    }

    fn load_with_tables<P: AsRef<Path>>(
        manifest_path: P,
        sstable_path: P,
        opts: Arc<Options>,
    ) -> Result<(Levels, u64)> {
        // First load the raw level data (IDs)
        let (level_data, next_id) = Self::load(manifest_path)?;

        // Now convert the level data into actual Level objects with Table instances
        let mut levels_vec = Vec::with_capacity(opts.level_count as usize);

        // Make sure we have the expected number of levels
        for level_idx in 0..opts.level_count {
            let level_tables = if level_idx < level_data.len() as u8 {
                // Load tables for this level
                let table_ids = &level_data[level_idx as usize];
                let mut tables = Vec::with_capacity(table_ids.len());

                for &table_id in table_ids {
                    // Load the actual table from disk
                    match Self::load_table(sstable_path.as_ref(), table_id, opts.clone()) {
                        Ok(table) => tables.push(table),
                        Err(err) => {
                            eprintln!("Error loading table {}: {:?}", table_id, err);
                            return Err(Error::LoadManifestFail(err.to_string()));
                        }
                    }
                }

                // Validate sequence numbers based on level
                if level_idx > 0 && !tables.is_empty() {
                    Self::validate_table_sequence_numbers(level_idx, &tables)?;
                }

                tables
            } else {
                // This level wasn't in the manifest
                return Err(Error::LoadManifestFail(format!(
                    "Level index {} exceeds loaded level data length {}",
                    level_idx,
                    level_data.len()
                )));
            };

            // Create the level with the loaded tables
            let mut level = Level::default();
            level.tables = level_tables;
            levels_vec.push(Arc::new(level));
        }

        Ok((Levels(levels_vec), next_id))
    }

    fn validate_table_sequence_numbers(level_idx: u8, tables: &[Arc<Table>]) -> Result<()> {
        // Basic sanity check for all tables
        for table in tables {
            // Ensure smallest_seq_num is not greater than largest_seq_num
            if table.meta.smallest_seq_num > table.meta.largest_seq_num {
                return Err(Error::LoadManifestFail(format!(
                    "Table {} has invalid sequence numbers: smallest({}) > largest({})",
                    table.id, table.meta.smallest_seq_num, table.meta.largest_seq_num
                )));
            }
        }

        // If we have multiple tables, check sequence continuity across all tables
        if tables.len() > 1 {
            for i in 0..tables.len() - 1 {
                let current = &tables[i];
                let next = &tables[i + 1];

                // Check if sequence numbers maintain continuity
                if next.meta.smallest_seq_num <= current.meta.largest_seq_num {
                    eprintln!(
                    "Warning: Level {} tables have overlapping sequence numbers: Table {} ({}-{}) and Table {} ({}-{})",
                    level_idx,
                    current.id, current.meta.smallest_seq_num, current.meta.largest_seq_num,
                    next.id, next.meta.smallest_seq_num, next.meta.largest_seq_num
                );
                }
            }
        }

        Ok(())
    }

    /// Helper to load a single table by ID
    fn load_table(sstable_path: &Path, table_id: u64, opts: Arc<Options>) -> Result<Arc<Table>> {
        let table_file_path = sstable_path.join(format!("{}", table_id));

        // Open the table file
        let file = SysFile::open(&table_file_path)?;
        let file: Arc<dyn File> = Arc::new(file);
        let file_size = file.size()?;

        // Create and return the table
        let table = Arc::new(Table::new(table_id, opts, file, file_size)?);
        Ok(table)
    }

    // The original load function stays the same, it just loads IDs
    pub(crate) fn load<P: AsRef<Path>>(path: P) -> Result<(Vec<Vec<u64>>, u64)> {
        let mut level_manifest = Cursor::new(std::fs::read(&path)?);

        // Read the next table ID
        let next_table_id = level_manifest.read_u64::<BigEndian>()?;

        // Read the number of levels
        let level_count = level_manifest.read_u8()?;

        // Load levels from the manifest
        let levels = Self::load_levels(&mut level_manifest, level_count)?;

        Ok((levels, next_table_id))
    }

    /// Loads levels from the manifest
    fn load_levels<R: Read>(reader: &mut R, level_count: u8) -> Result<Vec<Vec<u64>>> {
        let mut levels = vec![];

        for _ in 0..level_count {
            let mut level = vec![];
            let table_count = reader.read_u32::<BigEndian>()?;

            for _ in 0..table_count {
                let id = reader.read_u64::<BigEndian>()?;
                level.push(id);
            }

            levels.push(level);
        }

        Ok(levels)
    }

    pub fn depth(&self) -> u8 {
        let len = self.levels.as_ref().len() as u8;

        len
    }

    pub fn last_level_index(&self) -> u8 {
        self.depth() - 1
    }

    pub fn iter(&self) -> impl Iterator<Item = Arc<Table>> + '_ {
        LevelManifestIterator::new(self)
    }

    pub(crate) fn get_all_tables(&self) -> HashMap<u64, Arc<Table>> {
        let mut output = HashMap::new();

        for table in self.iter() {
            output.insert(table.meta.properties.id, table);
        }

        output
    }

    pub(crate) fn unhide_tables(&mut self, keys: &[u64]) {
        for key in keys {
            self.hidden_set.remove(key);
        }
    }

    pub(crate) fn hide_tables(&mut self, keys: &[u64]) {
        for key in keys {
            self.hidden_set.insert(*key);
        }
    }
}

/// Safely updates a file's content.
pub fn replace_file_content<P: AsRef<Path>>(
    file_path: P,
    new_content: &[u8],
) -> std::io::Result<()> {
    let target_path = file_path.as_ref();
    let directory = target_path.parent().ok_or(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "Parent directory not found",
    ))?;

    // Create a temporary file in the same directory to ensure it's on the same filesystem.
    let mut temp_file = tempfile::Builder::new().tempfile_in(directory)?;
    temp_file.write_all(new_content)?;

    // Attempt to persist the temporary file to the target path.
    // This operation replaces the target file with the temporary file in an atomic operation on most platforms.
    temp_file.persist(target_path).map_err(|e| e.error)?;

    // Optionally, open and sync the updated file to ensure all changes are flushed to disk.
    let updated_file = SysFile::open(target_path)?;
    updated_file.sync_all()?;

    Ok(())
}

pub fn write_levels_to_disk<P: AsRef<Path>>(
    path: P,
    levels: &Levels,
    next_table_id: u64,
) -> Result<()> {
    let path = path.as_ref();

    let mut enc_bytes = vec![];

    // Write the next table ID
    enc_bytes.write_u64::<BigEndian>(next_table_id)?;

    // Write the level data
    levels.encode(&mut enc_bytes)?;

    replace_file_content(path, &enc_bytes)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::sstable::{table::TableWriter, InternalKey, InternalKeyKind};

    use super::*;
    use std::{
        fs::{self, File as SysFile},
        path::Path,
        sync::atomic::Ordering,
    };

    // Helper function to create a test table with direct file IO
    fn create_test_table(
        sstable_path: &Path,
        table_id: u64,
        num_items: u64,
        opts: Arc<Options>,
    ) -> Result<Arc<Table>> {
        let table_file_path = sstable_path.join(format!("{}", table_id));

        let mut file = SysFile::create(&table_file_path)?;

        // Create TableWriter that writes directly to the file
        let mut writer = TableWriter::new(&mut file, table_id, opts.clone());

        // Generate and add items
        for i in 0..num_items {
            let key = format!("key_{:05}", i);
            let value = format!("value_{:05}", i);

            let internal_key =
                InternalKey::new(key.as_bytes().to_vec(), i + 1, InternalKeyKind::Set);

            writer.add(internal_key.into(), value.as_bytes())?;
        }

        // Finish writing the table
        let size = writer.finish()?;

        // Open the file for reading
        let file = SysFile::open(&table_file_path)?;
        file.sync_all()?;
        let file: Arc<dyn File> = Arc::new(file);

        // Create the table
        let table = Table::new(table_id, opts.clone(), file, size as u64)?;

        Ok(Arc::new(table))
    }

    #[test]
    fn test_level_manifest_persistence() {
        let mut opts = Options::default();
        // Set up temporary directory for test
        let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
        let repo_path = temp_dir.path().to_path_buf();
        opts.path = repo_path.clone();
        opts.level_count = 3; // Set level count for the manifest
        let opts = Arc::new(opts);

        // Create sstables directory
        let sstable_path = repo_path.join(TABLE_FOLDER);
        fs::create_dir_all(&sstable_path).expect("Failed to create sstables directory");

        // Create a new manifest with 3 levels
        let mut manifest = LevelManifest::new(opts.clone()).expect("Failed to create manifest");

        // Create tables and add them to the manifest
        // Create 2 tables for level 0
        let table_id1 = 1;
        let table1 = create_test_table(&sstable_path, table_id1, 100, opts.clone())
            .expect("Failed to create table 1");

        let table_id2 = 2;
        let table2 = create_test_table(&sstable_path, table_id2, 200, opts.clone())
            .expect("Failed to create table 2");

        // Add tables to level 0
        {
            let level0 = Arc::make_mut(&mut manifest.levels.get_levels_mut()[0]);
            level0.insert(table1.clone());
            level0.insert(table2.clone());
        }

        // Create a table for level 1
        let table_id3 = 3;
        let table3 = create_test_table(&sstable_path, table_id3, 300, opts.clone())
            .expect("Failed to create table 3");

        // Add table to level 1
        {
            let level1 = Arc::make_mut(&mut manifest.levels.get_levels_mut()[1]);
            level1.insert(table3.clone());
        }

        // Set a specific next_table_id value for testing
        let expected_next_id = 100;
        manifest
            .next_table_id
            .store(expected_next_id, Ordering::SeqCst);

        // Persist the manifest with our custom next_table_id
        write_levels_to_disk(&manifest.path, &manifest.levels, expected_next_id)
            .expect("Failed to write to disk");

        // Load the data directly using load() to verify raw persistence
        let manifest_path = repo_path.join(LEVELS_MANIFEST_FILE);
        let (loaded_levels_data, loaded_next_id) =
            LevelManifest::load(&manifest_path).expect("Failed to load manifest");

        // Verify next_table_id was persisted correctly
        assert_eq!(
            loaded_next_id, expected_next_id,
            "Next table ID not persisted correctly"
        );

        // Verify level count matches what we created
        assert_eq!(
            loaded_levels_data.len(),
            opts.level_count as usize,
            "Incorrect number of levels loaded"
        );

        // Verify table IDs were persisted correctly
        assert_eq!(
            loaded_levels_data[0].len(),
            2,
            "Level 0 should have 2 tables"
        );
        assert!(
            loaded_levels_data[0].contains(&table_id1),
            "Level 0 should contain table_id1"
        );
        assert!(
            loaded_levels_data[0].contains(&table_id2),
            "Level 0 should contain table_id2"
        );

        assert_eq!(
            loaded_levels_data[1].len(),
            1,
            "Level 1 should have 1 table"
        );
        assert!(
            loaded_levels_data[1].contains(&table_id3),
            "Level 1 should contain table_id3"
        );

        // Create a new manifest from the same path (simulating restart/recovery)
        let new_manifest =
            LevelManifest::new(opts.clone()).expect("Failed to create manifest from existing file");

        // Verify the next_table_id was loaded correctly in the new manifest
        assert_eq!(
            new_manifest.next_table_id.load(Ordering::SeqCst),
            expected_next_id,
            "Next table ID not loaded correctly in new manifest"
        );

        // Verify the number of levels in the new manifest
        assert_eq!(
            new_manifest.levels.as_ref().len(),
            opts.level_count as usize,
            "Incorrect number of levels in new manifest"
        );

        // Verify tables were loaded correctly
        let level0 = &new_manifest.levels.as_ref()[0];
        assert_eq!(level0.tables.len(), 2, "Level 0 should have 2 tables");
        assert!(
            level0.tables.iter().any(|t| t.id == table_id1),
            "Level 0 should contain table with ID {}",
            table_id1
        );
        assert!(
            level0.tables.iter().any(|t| t.id == table_id2),
            "Level 0 should contain table with ID {}",
            table_id2
        );

        let level1 = &new_manifest.levels.as_ref()[1];
        assert_eq!(level1.tables.len(), 1, "Level 1 should have 1 table");
        assert!(
            level1.tables.iter().any(|t| t.id == table_id3),
            "Level 1 should contain table with ID {}",
            table_id3
        );

        // Verify table data was loaded correctly by checking properties
        let table1_reloaded = level0
            .tables
            .iter()
            .find(|t| t.id == table_id1)
            .expect("Table 1 not found");
        assert_eq!(
            table1_reloaded.meta.properties.num_entries, 100,
            "Table 1 should have 100 entries"
        );

        let table2_reloaded = level0
            .tables
            .iter()
            .find(|t| t.id == table_id2)
            .expect("Table 2 not found");
        assert_eq!(
            table2_reloaded.meta.properties.num_entries, 200,
            "Table 2 should have 200 entries"
        );

        let table3_reloaded = level1
            .tables
            .iter()
            .find(|t| t.id == table_id3)
            .expect("Table 3 not found");
        assert_eq!(
            table3_reloaded.meta.properties.num_entries, 300,
            "Table 3 should have 300 entries"
        );
    }
}
