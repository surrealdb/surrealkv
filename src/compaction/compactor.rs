use crate::{
    compaction::{CompactionChoice, CompactionInput, CompactionStrategy},
    error::Result,
    iter::{BoxedIterator, CompactionIterator, MergeIterator},
    levels::{write_levels_to_disk, LevelManifest},
    lsm::TABLE_FOLDER,
    memtable::ImmutableMemtables,
    sstable::table::{Table, TableWriter},
    vfs::File,
    vlog::VLog,
    Options as LSMOptions,
};

use std::{
    collections::HashMap,
    fs::File as SysFile,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock, RwLockWriteGuard,
    },
};

/// Compaction options
pub struct CompactionOptions {
    pub table_id_counter: Arc<AtomicU64>,
    pub lopts: Arc<LSMOptions>,
    pub levels: Arc<RwLock<LevelManifest>>,
    pub immutable_memtables: Arc<RwLock<ImmutableMemtables>>,
    pub vlog: Arc<VLog>,
}

impl CompactionOptions {
    pub fn from(tree: &crate::lsm::CoreInner) -> Self {
        Self {
            table_id_counter: tree.table_id_counter.clone(),
            lopts: tree.opts.clone(),
            immutable_memtables: tree.immutable_memtables.clone(),
            levels: tree.levels.clone(),
            vlog: tree.vlog.clone(),
        }
    }
}

/// Handles the compaction state and operations
pub struct Compactor {
    pub(crate) options: CompactionOptions,
    pub(crate) strategy: Arc<dyn CompactionStrategy>,
}

impl Compactor {
    pub fn new(options: CompactionOptions, strategy: Arc<dyn CompactionStrategy>) -> Self {
        Self { options, strategy }
    }

    pub fn compact(&self) -> Result<()> {
        let levels_guard = self.options.levels.write().unwrap();
        let choice = self.strategy.pick_levels(&levels_guard);

        match choice {
            CompactionChoice::Merge(input) => self.merge_tables(levels_guard, &input),
            CompactionChoice::Skip => Ok(()),
        }
    }

    fn merge_tables(
        &self,
        mut levels: RwLockWriteGuard<'_, LevelManifest>,
        input: &CompactionInput,
    ) -> Result<()> {
        let merge_result = {
            let merge_iter = self.create_merge_iterator(&levels, input)?;

            // Hide tables that are being merged
            levels.hide_tables(&input.tables_to_merge);
            drop(levels);

            // Create new table
            let new_table_id = self.generate_table_id();
            let new_table_path = self.get_table_path(new_table_id);

            // Write merged data and collect discard statistics
            let discard_stats =
                self.write_merged_table(&new_table_path, new_table_id, merge_iter, input)?;

            let new_table = self.open_table(new_table_id, &new_table_path)?;
            Ok((new_table, new_table_id, discard_stats))
        };

        match merge_result {
            Ok((new_table, _, discard_stats)) => {
                self.update_manifest(input, new_table)?;
                self.cleanup_old_tables(input);

                // Update VLog discard statistics in bulk
                if !discard_stats.is_empty() {
                    self.options.vlog.update_discard_stats(discard_stats);
                }

                Ok(())
            }
            Err(e) => {
                // Restore the original state
                let mut levels = self.options.levels.write().unwrap();
                levels.unhide_tables(&input.tables_to_merge);
                Err(e)
            }
        }
    }

    fn create_merge_iterator<'a>(
        &self,
        levels: &RwLockWriteGuard<'_, LevelManifest>,
        input: &CompactionInput,
    ) -> Result<MergeIterator<'a>> {
        let tables = levels.get_all_tables();

        let to_merge: Vec<_> = input
            .tables_to_merge
            .iter()
            .filter_map(|&id| {
                let table_opt = tables.get(&id);
                table_opt.cloned()
            })
            .collect();

        let iterators = to_merge
            .into_iter()
            .map(|table| Box::new(table.iter()) as BoxedIterator<'_>)
            .collect();

        Ok(MergeIterator::new(iterators))
    }

    fn write_merged_table(
        &self,
        path: &Path,
        table_id: u64,
        merge_iter: MergeIterator,
        input: &CompactionInput,
    ) -> Result<HashMap<u64, i64>> {
        let file = SysFile::create(path)?;
        let mut writer = TableWriter::new(file, table_id, self.options.lopts.clone());

        // Create a compaction iterator that filters tombstones
        let max_level = self.options.lopts.level_count - 1;

        let is_bottom_level = input.target_level >= max_level;
        let mut comp_iter = CompactionIterator::new(merge_iter, is_bottom_level);

        for (key, value) in &mut comp_iter {
            writer.add(key, &value)?;
        }

        writer.finish()?;

        // Return collected discard statistics
        Ok(comp_iter.discard_stats)
    }

    fn update_manifest(&self, input: &CompactionInput, new_table: Arc<Table>) -> Result<()> {
        let mut manifest = self.options.levels.write().unwrap();
        let _imm_guard = self.options.immutable_memtables.write();

        let new_table_id = new_table.id;

        // Check for table ID collision before making any changes
        if input.tables_to_merge.contains(&new_table_id) {
            return Err(crate::error::Error::TableIDCollision(new_table_id));
        }

        let mut levels = manifest.levels.clone();

        // Add new table to target level
        Arc::make_mut(&mut levels.get_levels_mut()[input.target_level as usize])
            .insert(new_table.clone());

        // Remove old tables
        for level in levels.get_levels_mut().iter_mut() {
            for &table_id in &input.tables_to_merge {
                Arc::make_mut(level).remove(table_id);
            }
        }

        // Get current next_table_id counter value
        let next_table_id = manifest.next_table_id.load(Ordering::SeqCst);

        // Persist changes with the current counter value
        if let Err(e) = write_levels_to_disk(&manifest.path, &levels, next_table_id) {
            manifest.unhide_tables(&input.tables_to_merge);
            return Err(e);
        }

        manifest.levels = levels;
        manifest.unhide_tables(&input.tables_to_merge);

        Ok(())
    }

    fn generate_table_id(&self) -> u64 {
        self.options
            .table_id_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    fn get_table_path(&self, table_id: u64) -> PathBuf {
        self.options
            .lopts
            .path
            .join(TABLE_FOLDER)
            .join(table_id.to_string())
    }

    fn cleanup_old_tables(&self, input: &CompactionInput) {
        let base_path = self.options.lopts.path.join(TABLE_FOLDER);
        for &table_id in &input.tables_to_merge {
            let path = base_path.join(table_id.to_string());
            if let Err(e) = std::fs::remove_file(path) {
                // Log error but continue with cleanup
                eprintln!("Failed to remove old table file: {}", e);
            }
        }
    }

    fn open_table(&self, table_id: u64, table_path: &Path) -> Result<Arc<Table>> {
        let file = SysFile::open(table_path)?;
        let file: Arc<dyn File> = Arc::new(file);
        let file_size = file.size()?;

        Ok(Arc::new(Table::new(
            table_id,
            self.options.lopts.clone(),
            file,
            file_size,
        )?))
    }
}
