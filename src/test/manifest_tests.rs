use std::fs::{self, File as SysFile};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use test_log::test;

use crate::batch::BatchOwner;
use crate::levels::{LevelManifest, ManifestChangeSet};
use crate::sstable::table::{Table, TableWriter};
use crate::vfs::File;
use crate::{InternalKey, InternalKeyKind, Options, Result};

// Helper function to create a test table with direct file IO
fn authority_for(opts: &Arc<Options>) -> crate::authority::store::AuthorityStore {
	crate::authority::store::AuthorityStore::new(opts.path.clone(), [7; 16])
}

/// The catalog version these in-memory fixture catalogs stand for: whatever is
/// published on disk, or 0 when nothing is.
fn latest_published_catalog_version(opts: &Arc<Options>) -> u64 {
	crate::authority::store::AuthorityStore::load_latest_catalog(&opts.path)
		.unwrap()
		.map(|manifest| manifest.catalog_version)
		.unwrap_or(0)
}

/// Reload through the production path: newest root -> state lineages ->
/// hydrated manifest, against a default-only catalog.
fn reload_default_manifest(opts: &Arc<Options>) -> LevelManifest {
	let authority = authority_for(opts);
	let catalog = crate::branch::BranchCatalog::new(crate::BranchId::DEFAULT);
	let root = authority.load_latest_root().unwrap();
	let catalog_version = latest_published_catalog_version(opts);
	LevelManifest::hydrate(
		Arc::clone(opts),
		authority,
		&catalog,
		catalog_version,
		root.as_ref(),
		std::sync::Arc::new(crate::timeline::Timeline::new()),
	)
	.unwrap()
}

fn fresh_manifest(opts: &Arc<Options>) -> LevelManifest {
	LevelManifest::fresh(Arc::clone(opts), authority_for(opts))
}

fn create_test_table(table_id: u64, num_items: u64, opts: Arc<Options>) -> Result<Arc<Table>> {
	let table_file_path = opts.sstable_file_path(table_id);

	let mut file = SysFile::create(&table_file_path)?;

	// Create TableWriter that writes directly to the file
	let mut writer = TableWriter::new(&mut file, table_id, Arc::clone(&opts), 0); // L0 for test

	// Generate and add items
	for i in 0..num_items {
		let key = format!("key_{i:05}");
		let value = format!("value_{i:05}");

		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), i + 1, InternalKeyKind::Set, 0);

		writer.add(internal_key, value.as_bytes())?;
	}

	// Finish writing the table
	let size = writer.finish()?;

	// Open the file for reading
	let file = SysFile::open(&table_file_path)?;
	file.sync_all()?;
	let file: Arc<dyn File> = Arc::new(file);

	// Create the table
	let table = Table::new(table_id, opts, file, size as u64)?;

	Ok(Arc::new(table))
}

#[test]
fn test_level_manifest_persistence() {
	let mut opts = Options::default();
	// Set up temporary directory for test
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3; // Set level count for the manifest
	let opts = Arc::new(opts);

	// Create sstables directory
	let sstable_path = opts.sstable_dir();
	fs::create_dir_all(&sstable_path).expect("Failed to create sstables directory");

	// Create manifest directory

	// Create a new manifest with 3 levels
	let mut manifest = fresh_manifest(&opts);

	// Create tables and add them to the manifest
	// Create 2 tables for level 0
	let table_id1 = 1;
	let table1 =
		create_test_table(table_id1, 100, Arc::clone(&opts)).expect("Failed to create table 1");

	let table_id2 = 2;
	let table2 =
		create_test_table(table_id2, 200, Arc::clone(&opts)).expect("Failed to create table 2");

	// Create a table for level 1
	let table_id3 = 3;
	let table3 =
		create_test_table(table_id3, 300, Arc::clone(&opts)).expect("Failed to create table 3");

	let expected_next_id = 100;
	manifest.next_table_id.store(expected_next_id, Ordering::SeqCst);

	let changeset = ManifestChangeSet {
		new_tables: vec![
			(0, table1), // Level 0
			(0, table2), // Level 0
			(1, table3), // Level 1
		],
		..Default::default()
	};

	// Apply changeset to manifest
	manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	// Verify last_sequence was updated correctly (table3 has largest_seq_num = 300)
	assert_eq!(manifest.get_last_sequence(), 300, "last_sequence should be 300");

	// Persist: one state version for the default owner plus a root version.
	manifest.persist_owner_update(BatchOwner::DEFAULT).expect("Failed to persist");

	// Reload through the production hydrate path.
	let loaded_manifest = reload_default_manifest(&opts);

	// The table-id allocator resumes at the root's block-reserved watermark:
	// strictly above every allocated id, at the next block boundary.
	let watermark = loaded_manifest.next_table_id.load(Ordering::SeqCst);
	assert!(
		watermark > expected_next_id && watermark % 1024 == 0,
		"allocator must resume at a block boundary above {expected_next_id}, got {watermark}"
	);

	// Verify level count matches what we created
	assert_eq!(
		loaded_manifest.default_owner_levels().as_ref().len(),
		opts.level_count as usize,
		"Incorrect number of levels loaded"
	);

	// Verify table IDs were persisted correctly
	let loaded_level0 = &loaded_manifest.default_owner_levels().as_ref()[0];
	let loaded_level1 = &loaded_manifest.default_owner_levels().as_ref()[1];

	assert_eq!(loaded_level0.tables.len(), 2, "Level 0 should have 2 tables");
	assert!(
		loaded_level0.tables.iter().any(|t| t.id == table_id1),
		"Level 0 should contain table_id1"
	);
	assert!(
		loaded_level0.tables.iter().any(|t| t.id == table_id2),
		"Level 0 should contain table_id2"
	);

	assert_eq!(loaded_level1.tables.len(), 1, "Level 1 should have 1 table");
	assert!(
		loaded_level1.tables.iter().any(|t| t.id == table_id3),
		"Level 1 should contain table_id3"
	);

	// Reload a second time (simulating a second restart): hydration is
	// deterministic and idempotent.
	let new_manifest = reload_default_manifest(&opts);
	assert_eq!(
		new_manifest.next_table_id.load(Ordering::SeqCst),
		watermark,
		"the watermark must be stable across repeated reloads"
	);

	// Verify the number of levels in the new manifest
	assert_eq!(
		new_manifest.default_owner_levels().as_ref().len(),
		opts.level_count as usize,
		"Incorrect number of levels in new manifest"
	);

	// Verify tables were loaded correctly
	let level0 = &new_manifest.default_owner_levels().as_ref()[0];
	assert_eq!(level0.tables.len(), 2, "Level 0 should have 2 tables");
	assert!(
		level0.tables.iter().any(|t| t.id == table_id1),
		"Level 0 should contain table with ID {table_id1}"
	);
	assert!(
		level0.tables.iter().any(|t| t.id == table_id2),
		"Level 0 should contain table with ID {table_id2}"
	);

	let level1 = &new_manifest.default_owner_levels().as_ref()[1];
	assert_eq!(level1.tables.len(), 1, "Level 1 should have 1 table");
	assert!(
		level1.tables.iter().any(|t| t.id == table_id3),
		"Level 1 should contain table with ID {table_id3}"
	);

	// Verify table data was loaded correctly by checking all properties
	let table1_reloaded =
		level0.tables.iter().find(|t| t.id == table_id1).expect("Table 1 not found");

	// Check Table1 basic properties
	assert_eq!(table1_reloaded.id, table_id1, "Table 1 ID mismatch");
	assert_eq!(table1_reloaded.file_size, 3854, "Table 1 file size should include owner metadata");

	// Check Table1 metadata properties
	let props1 = &table1_reloaded.meta.properties;
	assert_eq!(props1.id, table_id1, "Table 1 properties ID mismatch");
	assert_eq!(props1.num_entries, 100, "Table 1 should have 100 entries");
	assert!(props1.data_size > 0, "Table 1 data size should be greater than 0");
	assert!(props1.created_at > 0, "Table 1 created_at should be set");
	assert_eq!(props1.item_count, 100, "Table 1 item count should match entries");
	assert_eq!(props1.key_count, 100, "Table 1 key count should match entries");
	assert!(props1.block_count > 0, "Table 1 should have at least one block");
	assert_eq!(props1.seqnos, (1, 100), "Table 1 sequence numbers should be (1, 100)");

	// Check Table1 metadata fields
	assert_eq!(
		table1_reloaded.meta.smallest_seq_num,
		Some(1),
		"Table 1 smallest seq num should be 1"
	);
	assert_eq!(
		table1_reloaded.meta.largest_seq_num,
		Some(100),
		"Table 1 largest seq num should be 100"
	);
	assert!(table1_reloaded.meta.has_point_keys.unwrap_or(false), "Table 1 should have point keys");
	assert!(
		table1_reloaded.meta.smallest_point.is_some(),
		"Table 1 should have smallest point key"
	);
	assert!(table1_reloaded.meta.largest_point.is_some(), "Table 1 should have largest point key");

	let table2_reloaded =
		level0.tables.iter().find(|t| t.id == table_id2).expect("Table 2 not found");

	// Check Table2 basic properties
	assert_eq!(table2_reloaded.id, table_id2, "Table 2 ID mismatch");
	assert_eq!(table2_reloaded.file_size, 7161, "Table 2 file size should include owner metadata");

	// Check Table2 metadata properties
	let props2 = &table2_reloaded.meta.properties;
	assert_eq!(props2.id, table_id2, "Table 2 properties ID mismatch");
	assert_eq!(props2.num_entries, 200, "Table 2 should have 200 entries");
	assert!(props2.data_size > 0, "Table 2 data size should be greater than 0");
	assert!(props2.created_at > 0, "Table 2 created_at should be set");
	assert_eq!(props2.item_count, 200, "Table 2 item count should match entries");
	assert_eq!(props2.key_count, 200, "Table 2 key count should match entries");
	assert!(props2.block_count > 0, "Table 2 should have at least one block");
	assert_eq!(props2.seqnos, (1, 200), "Table 2 sequence numbers should be (1, 200)");

	// Check Table2 metadata fields
	assert_eq!(
		table2_reloaded.meta.smallest_seq_num,
		Some(1),
		"Table 2 smallest seq num should be 1"
	);
	assert_eq!(
		table2_reloaded.meta.largest_seq_num,
		Some(200),
		"Table 2 largest seq num should be 200"
	);
	assert!(table2_reloaded.meta.has_point_keys.unwrap_or(false), "Table 2 should have point keys");
	assert!(
		table2_reloaded.meta.smallest_point.is_some(),
		"Table 2 should have smallest point key"
	);
	assert!(table2_reloaded.meta.largest_point.is_some(), "Table 2 should have largest point key");

	let table3_reloaded =
		level1.tables.iter().find(|t| t.id == table_id3).expect("Table 3 not found");

	// Check Table3 basic properties
	assert_eq!(table3_reloaded.id, table_id3, "Table 3 ID mismatch");
	assert_eq!(table3_reloaded.file_size, 10468, "Table 3 file size should include owner metadata");

	// Check Table3 metadata properties
	let props3 = &table3_reloaded.meta.properties;
	assert_eq!(props3.id, table_id3, "Table 3 properties ID mismatch");
	assert_eq!(props3.num_entries, 300, "Table 3 should have 300 entries");
	assert!(props3.data_size > 0, "Table 3 data size should be greater than 0");
	assert!(props3.created_at > 0, "Table 3 created_at should be set");
	assert_eq!(props3.item_count, 300, "Table 3 item count should match entries");
	assert_eq!(props3.key_count, 300, "Table 3 key count should match entries");
	assert!(props3.block_count > 0, "Table 3 should have at least one block");
	assert_eq!(props3.seqnos, (1, 300), "Table 3 sequence numbers should be (1, 300)");

	// Check Table3 metadata fields
	assert_eq!(
		table3_reloaded.meta.smallest_seq_num,
		Some(1),
		"Table 3 smallest seq num should be 1"
	);
	assert_eq!(
		table3_reloaded.meta.largest_seq_num,
		Some(300),
		"Table 3 largest seq num should be 300"
	);
	assert!(table3_reloaded.meta.has_point_keys.unwrap_or(false), "Table 3 should have point keys");
	assert!(
		table3_reloaded.meta.smallest_point.is_some(),
		"Table 3 should have smallest point key"
	);
	assert!(table3_reloaded.meta.largest_point.is_some(), "Table 3 should have largest point key");

	// Verify table format and compression are set correctly
	assert_eq!(
		props1.table_format,
		crate::sstable::table::TableFormat::LSMV3,
		"Table 1 format should be LSMV3"
	);
	assert_eq!(
		props2.table_format,
		crate::sstable::table::TableFormat::LSMV3,
		"Table 2 format should be LSMV3"
	);
	assert_eq!(
		props3.table_format,
		crate::sstable::table::TableFormat::LSMV3,
		"Table 3 format should be LSMV3"
	);

	// Verify no deletions in test tables
	assert_eq!(props1.num_deletions, 0, "Table 1 should have no deletions");
	assert_eq!(props2.num_deletions, 0, "Table 2 should have no deletions");
	assert_eq!(props3.num_deletions, 0, "Table 3 should have no deletions");

	// Verify tombstone counts
	assert_eq!(props1.tombstone_count, 0, "Table 1 should have no tombstones");
	assert_eq!(props2.tombstone_count, 0, "Table 2 should have no tombstones");
	assert_eq!(props3.tombstone_count, 0, "Table 3 should have no tombstones");
}

// Helper function to create a test table with specific sequence numbers
fn create_test_table_with_seq_nums(
	table_id: u64,
	seq_start: u64,
	seq_end: u64,
	opts: Arc<Options>,
) -> Result<Arc<Table>> {
	let table_file_path = opts.sstable_file_path(table_id);

	let mut file = SysFile::create(&table_file_path)?;

	// Create TableWriter that writes directly to the file
	let mut writer = TableWriter::new(&mut file, table_id, Arc::clone(&opts), 0); // L0 for test

	// Generate and add items with specific sequence numbers
	for seq_num in seq_start..=seq_end {
		let key = format!("key_{seq_num:05}");
		let value = format!("value_{seq_num:05}");

		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), seq_num, InternalKeyKind::Set, 0);

		writer.add(internal_key, value.as_bytes())?;
	}

	// Finish writing the table
	let size = writer.finish()?;

	// Open the file for reading
	let file = SysFile::open(&table_file_path)?;
	file.sync_all()?;
	let file: Arc<dyn File> = Arc::new(file);

	// Create the table
	let table = Table::new(table_id, opts, file, size as u64)?;

	Ok(Arc::new(table))
}

#[test]
fn test_lsn_with_multiple_l0_tables() {
	let mut opts = Options::default();
	// Set up temporary directory for test
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	// Create sstables directory
	let sstable_path = opts.sstable_dir();
	fs::create_dir_all(&sstable_path).expect("Failed to create sstables directory");

	// Create manifest directory

	// Create a new manifest
	let mut manifest = fresh_manifest(&opts);

	// Test 1: Empty manifest should have last_sequence of 0
	assert_eq!(manifest.get_last_sequence(), 0, "Empty manifest should return last_sequence of 0");

	// Test 2: Add single table via changeset
	// Create table with sequence numbers 1-10 (largest_seq_num = 10)
	let table1 = create_test_table_with_seq_nums(1, 1, 10, Arc::clone(&opts))
		.expect("Failed to create table 1");

	let changeset1 = ManifestChangeSet {
		new_tables: vec![(0, table1)],
		..Default::default()
	};
	manifest.apply_changeset(&changeset1).expect("Failed to apply changeset");

	assert_eq!(manifest.get_last_sequence(), 10, "Single table should update last_sequence");

	// Test 3: Add table with higher sequence numbers
	// Create table with sequence numbers 11-20 (largest_seq_num = 20)
	let table2 = create_test_table_with_seq_nums(2, 11, 20, Arc::clone(&opts))
		.expect("Failed to create table 2");

	let changeset2 = ManifestChangeSet {
		new_tables: vec![(0, table2)],
		..Default::default()
	};
	manifest.apply_changeset(&changeset2).expect("Failed to apply changeset");

	assert_eq!(
		manifest.get_last_sequence(),
		20,
		"Should update to highest sequence from new table"
	);

	// Test 4: Add table with even higher sequence numbers
	// Create table with sequence numbers 21-30 (largest_seq_num = 30)
	let table3 = create_test_table_with_seq_nums(3, 21, 30, Arc::clone(&opts))
		.expect("Failed to create table 3");

	let changeset3 = ManifestChangeSet {
		new_tables: vec![(0, table3)],
		..Default::default()
	};
	manifest.apply_changeset(&changeset3).expect("Failed to apply changeset");

	assert_eq!(
		manifest.get_last_sequence(),
		30,
		"Should update to highest sequence after adding new table"
	);

	// Test 5: Verify table ordering - tables should be sorted by largest_seq_num
	// descending
	{
		let level0 = &manifest.default_owner_levels().get_levels()[0];
		assert_eq!(level0.tables.len(), 3, "Should have 3 tables in L0");

		// Tables should be in descending order of their largest sequence number
		assert_eq!(
			level0.tables[0].meta.largest_seq_num,
			Some(30),
			"First table should have highest seq num"
		);
		assert_eq!(
			level0.tables[1].meta.largest_seq_num,
			Some(20),
			"Second table should have middle seq num"
		);
		assert_eq!(
			level0.tables[2].meta.largest_seq_num,
			Some(10),
			"Third table should have lowest seq num"
		);
	}

	// Test 6: Add table with lower sequence numbers (simulating out-of-order
	// insertion) Create table with sequence numbers 5-8 (largest_seq_num = 8)
	let table4 = create_test_table_with_seq_nums(4, 5, 8, Arc::clone(&opts))
		.expect("Failed to create table 4");

	let changeset4 = ManifestChangeSet {
		new_tables: vec![(0, table4)],
		..Default::default()
	};
	manifest.apply_changeset(&changeset4).expect("Failed to apply changeset");

	// last_sequence should still be 30 (highest among all tables)
	assert_eq!(
		manifest.get_last_sequence(),
		30,
		"last_sequence should remain highest even after adding table with lower seq nums"
	);

	// Verify correct ordering after out-of-order insertion
	{
		let level0 = &manifest.default_owner_levels().get_levels()[0];
		assert_eq!(level0.tables.len(), 4, "Should have 4 tables in L0");

		// Tables should still be in descending order of their largest sequence number
		assert_eq!(
			level0.tables[0].meta.largest_seq_num,
			Some(30),
			"First table should have seq num 30"
		);
		assert_eq!(
			level0.tables[1].meta.largest_seq_num,
			Some(20),
			"Second table should have seq num 20"
		);
		assert_eq!(
			level0.tables[2].meta.largest_seq_num,
			Some(10),
			"Third table should have seq num 10"
		);
		assert_eq!(
			level0.tables[3].meta.largest_seq_num,
			Some(8),
			"Fourth table should have seq num 8"
		);
	}

	// Test 7: Test with overlapping sequence ranges
	// Create table with sequence numbers 25-35 (largest_seq_num = 35, overlaps with
	// table3)
	let table5 =
		create_test_table_with_seq_nums(5, 25, 35, opts).expect("Failed to create table 5");

	let changeset5 = ManifestChangeSet {
		new_tables: vec![(0, table5)],
		..Default::default()
	};
	manifest.apply_changeset(&changeset5).expect("Failed to apply changeset");

	assert_eq!(
		manifest.get_last_sequence(),
		35,
		"Should return new highest last_sequence from overlapping ranges"
	);

	// Verify final ordering
	{
		let level0 = &manifest.default_owner_levels().get_levels()[0];
		assert_eq!(level0.tables.len(), 5, "Should have 5 tables in L0");

		// First table should have the highest sequence number
		assert_eq!(
			level0.tables[0].meta.largest_seq_num,
			Some(35),
			"First table should have highest seq num 35"
		);
	}
}

#[test]
fn test_last_sequence_persistence_across_manifest_reload() {
	let mut opts = Options::default();
	// Set up temporary directory for test
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	// Create sstables directory
	let sstable_path = opts.sstable_dir();
	fs::create_dir_all(&sstable_path).expect("Failed to create sstables directory");

	// Create manifest directory

	let expected_last_sequence = 50;

	// Create manifest with tables via changeset and verify last_sequence
	{
		let mut manifest = fresh_manifest(&opts);

		// Create tables with different sequence ranges
		let table1 = create_test_table_with_seq_nums(1, 1, 20, Arc::clone(&opts))
			.expect("Failed to create table 1");
		let table2 =
			create_test_table_with_seq_nums(2, 21, expected_last_sequence, Arc::clone(&opts))
				.expect("Failed to create table 2");
		let table3 = create_test_table_with_seq_nums(3, 10, 30, Arc::clone(&opts))
			.expect("Failed to create table 3");

		// Add all tables via a single changeset
		let changeset = ManifestChangeSet {
			new_tables: vec![(0, table1), (0, table2), (0, table3)],
			..Default::default()
		};
		manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

		// Verify last_sequence before persistence
		assert_eq!(
			manifest.get_last_sequence(),
			expected_last_sequence,
			"last_sequence should be {} before persistence",
			expected_last_sequence
		);

		// Persist the manifest
		manifest.persist_owner_update(BatchOwner::DEFAULT).expect("Failed to persist");
	}

	// Reload manifest and verify last_sequence is preserved
	{
		let reloaded_manifest = reload_default_manifest(&opts);

		// Verify last_sequence after reload
		assert_eq!(
			reloaded_manifest.get_last_sequence(),
			expected_last_sequence,
			"last_sequence should be {} after reload",
			expected_last_sequence
		);

		// Verify table count and ordering
		let level0 = &reloaded_manifest.default_owner_levels().get_levels()[0];
		assert_eq!(level0.tables.len(), 3, "Should have 3 tables after reload");

		// Verify tables are still properly ordered
		assert_eq!(
			level0.tables[0].meta.largest_seq_num,
			Some(50),
			"First table should have highest seq num"
		);
		assert_eq!(
			level0.tables[1].meta.largest_seq_num,
			Some(30),
			"Second table should have middle seq num"
		);
		assert_eq!(
			level0.tables[2].meta.largest_seq_num,
			Some(20),
			"Third table should have lowest seq num"
		);
	}
}

#[test]
fn test_manifest_v2_with_log_number_and_last_sequence() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	// Create required directories
	fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");

	// Create a manifest with log_number and last_sequence set
	let mut manifest = fresh_manifest(&opts);

	// Add a table to ensure non-trivial state
	// Table with sequence numbers 100-200, so last_sequence should be 200
	let table = create_test_table_with_seq_nums(1, 100, 200, Arc::clone(&opts))
		.expect("Failed to create table");

	// Use changeset to atomically set log_number and add table
	// The changeset will automatically update last_sequence from the table's
	// largest_seq_num
	let changeset = ManifestChangeSet {
		log_number: Some(42),
		new_tables: vec![(0, table)],
		..Default::default()
	};
	manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	// Verify last_sequence was updated by the changeset
	assert_eq!(manifest.get_last_sequence(), 200, "last_sequence should be updated by changeset");

	// Persist to the lineages
	manifest.persist_owner_update(BatchOwner::DEFAULT).expect("Failed to persist");

	// Reload and verify
	let loaded_manifest = reload_default_manifest(&opts);

	// Verify new fields persisted correctly
	assert_eq!(loaded_manifest.get_log_number(), 42, "log_number should persist");
	assert_eq!(loaded_manifest.get_last_sequence(), 200, "last_sequence should persist");

	// Verify table loaded correctly
	assert_eq!(loaded_manifest.default_owner_levels().get_levels()[0].tables.len(), 1);
}

#[test]
fn test_revert_empty_changeset() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");

	let mut manifest = fresh_manifest(&opts);

	let initial_last_sequence = manifest.get_last_sequence();
	let initial_log_number = manifest.get_log_number();

	let changeset = ManifestChangeSet::default();
	let rollback = manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.get_last_sequence(),
		initial_last_sequence,
		"last_sequence should be unchanged"
	);
	assert_eq!(manifest.get_log_number(), initial_log_number, "log_number should be unchanged");
}

#[test]
fn test_revert_added_tables_only() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");

	let mut manifest = fresh_manifest(&opts);

	let table1 = create_test_table_with_seq_nums(1, 1, 10, Arc::clone(&opts))
		.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(2, 11, 20, Arc::clone(&opts))
		.expect("Failed to create table 2");
	let table3 = create_test_table_with_seq_nums(3, 21, 30, Arc::clone(&opts))
		.expect("Failed to create table 3");

	let initial_table_count_l0 = manifest.default_owner_levels().get_levels()[0].tables.len();
	let initial_table_count_l1 = manifest.default_owner_levels().get_levels()[1].tables.len();

	let changeset = ManifestChangeSet {
		new_tables: vec![(0, table1), (1, table2), (1, table3)],
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	assert_eq!(
		manifest.default_owner_levels().get_levels()[0].tables.len(),
		initial_table_count_l0 + 1
	);
	assert_eq!(
		manifest.default_owner_levels().get_levels()[1].tables.len(),
		initial_table_count_l1 + 2
	);

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.default_owner_levels().get_levels()[0].tables.len(),
		initial_table_count_l0,
		"L0 table count should be restored"
	);
	assert_eq!(
		manifest.default_owner_levels().get_levels()[1].tables.len(),
		initial_table_count_l1,
		"L1 table count should be restored"
	);
}

#[test]
fn test_revert_deleted_tables_only() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");

	let mut manifest = fresh_manifest(&opts);

	// Add some tables first
	let table1 = create_test_table_with_seq_nums(1, 1, 10, Arc::clone(&opts))
		.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(2, 11, 20, Arc::clone(&opts))
		.expect("Failed to create table 2");
	let table3 = create_test_table_with_seq_nums(3, 21, 30, Arc::clone(&opts))
		.expect("Failed to create table 3");

	let add_changeset = ManifestChangeSet {
		new_tables: vec![(0, table1), (0, table2), (1, table3)],
		..Default::default()
	};
	let _ = manifest.apply_changeset(&add_changeset).expect("Failed to apply changeset");

	let initial_table_count_l0 = manifest.default_owner_levels().get_levels()[0].tables.len();
	let initial_table_count_l1 = manifest.default_owner_levels().get_levels()[1].tables.len();

	// Now delete them
	let delete_changeset = ManifestChangeSet {
		deleted_tables: std::collections::HashSet::from([(0, 1), (0, 2), (1, 3)]),
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&delete_changeset).expect("Failed to apply changeset");

	assert_eq!(
		manifest.default_owner_levels().get_levels()[0].tables.len(),
		initial_table_count_l0 - 2
	);
	assert_eq!(
		manifest.default_owner_levels().get_levels()[1].tables.len(),
		initial_table_count_l1 - 1
	);

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.default_owner_levels().get_levels()[0].tables.len(),
		initial_table_count_l0,
		"L0 table count should be restored"
	);
	assert_eq!(
		manifest.default_owner_levels().get_levels()[1].tables.len(),
		initial_table_count_l1,
		"L1 table count should be restored"
	);

	// Verify table IDs are present
	assert!(manifest.default_owner_levels().get_levels()[0].tables.iter().any(|t| t.id == 1));
	assert!(manifest.default_owner_levels().get_levels()[0].tables.iter().any(|t| t.id == 2));
	assert!(manifest.default_owner_levels().get_levels()[1].tables.iter().any(|t| t.id == 3));
}

#[test]
fn test_revert_mixed_add_delete() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");

	let mut manifest = fresh_manifest(&opts);

	// Add initial tables
	let table1 = create_test_table_with_seq_nums(1, 1, 10, Arc::clone(&opts))
		.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(2, 11, 20, Arc::clone(&opts))
		.expect("Failed to create table 2");

	let add_changeset = ManifestChangeSet {
		new_tables: vec![(0, table1), (0, table2)],
		..Default::default()
	};
	let _ = manifest.apply_changeset(&add_changeset).expect("Failed to apply changeset");

	let initial_table_count_l0 = manifest.default_owner_levels().get_levels()[0].tables.len();

	// Mixed: delete table1, add table3
	let table3 = create_test_table_with_seq_nums(3, 21, 30, Arc::clone(&opts))
		.expect("Failed to create table 3");

	let mixed_changeset = ManifestChangeSet {
		deleted_tables: std::collections::HashSet::from([(0, 1)]),
		new_tables: vec![(0, table3)],
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&mixed_changeset).expect("Failed to apply changeset");

	assert_eq!(
		manifest.default_owner_levels().get_levels()[0].tables.len(),
		initial_table_count_l0
	);
	assert!(manifest.default_owner_levels().get_levels()[0].tables.iter().any(|t| t.id == 3));
	assert!(!manifest.default_owner_levels().get_levels()[0].tables.iter().any(|t| t.id == 1));

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.default_owner_levels().get_levels()[0].tables.len(),
		initial_table_count_l0,
		"L0 table count should be restored"
	);
	assert!(manifest.default_owner_levels().get_levels()[0].tables.iter().any(|t| t.id == 1));
	assert!(!manifest.default_owner_levels().get_levels()[0].tables.iter().any(|t| t.id == 3));
}

#[test]
fn test_revert_preserves_table_ordering_l0() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");

	let mut manifest = fresh_manifest(&opts);

	// Add tables with different sequence numbers
	let table1 = create_test_table_with_seq_nums(1, 1, 10, Arc::clone(&opts))
		.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(2, 11, 20, Arc::clone(&opts))
		.expect("Failed to create table 2");
	let table3 = create_test_table_with_seq_nums(3, 21, 30, Arc::clone(&opts))
		.expect("Failed to create table 3");

	let add_changeset = ManifestChangeSet {
		new_tables: vec![(0, table1), (0, table2), (0, table3)],
		..Default::default()
	};
	let _ = manifest.apply_changeset(&add_changeset).expect("Failed to apply changeset");

	// Capture initial ordering
	let initial_order: Vec<u64> =
		manifest.default_owner_levels().get_levels()[0].tables.iter().map(|t| t.id).collect();

	// Delete and re-add to test ordering preservation
	let delete_changeset = ManifestChangeSet {
		deleted_tables: std::collections::HashSet::from([(0, 2)]),
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&delete_changeset).expect("Failed to apply changeset");

	manifest.revert_changeset(rollback);

	// Verify ordering is preserved
	let restored_order: Vec<u64> =
		manifest.default_owner_levels().get_levels()[0].tables.iter().map(|t| t.id).collect();

	assert_eq!(initial_order, restored_order, "L0 table ordering should be preserved");
}

#[test]
fn test_revert_preserves_table_ordering_l1() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");

	let mut manifest = fresh_manifest(&opts);

	// Add tables to L1 (sorted by key)
	let table1 = create_test_table_with_seq_nums(1, 1, 10, Arc::clone(&opts))
		.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(2, 11, 20, Arc::clone(&opts))
		.expect("Failed to create table 2");
	let table3 = create_test_table_with_seq_nums(3, 21, 30, Arc::clone(&opts))
		.expect("Failed to create table 3");

	let add_changeset = ManifestChangeSet {
		new_tables: vec![(1, table1), (1, table2), (1, table3)],
		..Default::default()
	};
	let _ = manifest.apply_changeset(&add_changeset).expect("Failed to apply changeset");

	// Capture initial ordering
	let initial_order: Vec<u64> =
		manifest.default_owner_levels().get_levels()[1].tables.iter().map(|t| t.id).collect();

	// Delete and re-add to test ordering preservation
	let delete_changeset = ManifestChangeSet {
		deleted_tables: std::collections::HashSet::from([(1, 2)]),
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&delete_changeset).expect("Failed to apply changeset");

	manifest.revert_changeset(rollback);

	// Verify ordering is preserved
	let restored_order: Vec<u64> =
		manifest.default_owner_levels().get_levels()[1].tables.iter().map(|t| t.id).collect();

	assert_eq!(initial_order, restored_order, "L1+ table ordering should be preserved");
}

#[test]
fn test_revert_log_number_change() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");

	let mut manifest = fresh_manifest(&opts);

	let initial_log_number = manifest.get_log_number();
	let new_log_number = 42;

	let changeset = ManifestChangeSet {
		log_number: Some(new_log_number),
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.get_log_number(), new_log_number, "log_number should be updated");

	manifest.revert_changeset(rollback);

	assert_eq!(manifest.get_log_number(), initial_log_number, "log_number should be reverted");
}

#[test]
fn test_revert_log_number_not_changed() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");

	let mut manifest = fresh_manifest(&opts);

	// Set log_number to a higher value first
	let higher_log_number = 50;
	let changeset1 = ManifestChangeSet {
		log_number: Some(higher_log_number),
		..Default::default()
	};
	let _ = manifest.apply_changeset(&changeset1).expect("Failed to apply changeset");

	let current_log_number = manifest.get_log_number();

	// Try to set it to a lower value (should not change)
	let lower_log_number = 30;
	let changeset2 = ManifestChangeSet {
		log_number: Some(lower_log_number),
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&changeset2).expect("Failed to apply changeset");

	assert_eq!(manifest.get_log_number(), current_log_number, "log_number should not decrease");

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.get_log_number(),
		current_log_number,
		"log_number should remain unchanged after revert"
	);
}

#[test]
fn test_revert_last_sequence() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");

	let mut manifest = fresh_manifest(&opts);

	let initial_last_sequence = manifest.get_last_sequence();

	let table = create_test_table_with_seq_nums(1, 1, 100, Arc::clone(&opts))
		.expect("Failed to create table");

	let changeset = ManifestChangeSet {
		new_tables: vec![(0, table)],
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.get_last_sequence(), 100, "last_sequence should be updated");

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.get_last_sequence(),
		initial_last_sequence,
		"last_sequence should be reverted"
	);
}

#[test]
fn test_revert_multiple_tables_same_level() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");

	let mut manifest = fresh_manifest(&opts);

	let table1 = create_test_table_with_seq_nums(1, 1, 10, Arc::clone(&opts))
		.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(2, 11, 20, Arc::clone(&opts))
		.expect("Failed to create table 2");
	let table3 = create_test_table_with_seq_nums(3, 21, 30, Arc::clone(&opts))
		.expect("Failed to create table 3");
	let table4 = create_test_table_with_seq_nums(4, 31, 40, Arc::clone(&opts))
		.expect("Failed to create table 4");

	let changeset = ManifestChangeSet {
		new_tables: vec![(0, table1), (0, table2), (0, table3), (0, table4)],
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.default_owner_levels().get_levels()[0].tables.len(), 4);

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.default_owner_levels().get_levels()[0].tables.len(),
		0,
		"All tables should be removed"
	);
}

#[test]
fn test_revert_table_only_one_in_level() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");

	let mut manifest = fresh_manifest(&opts);

	let table = create_test_table_with_seq_nums(1, 1, 10, Arc::clone(&opts))
		.expect("Failed to create table");

	let add_changeset = ManifestChangeSet {
		new_tables: vec![(1, table)],
		..Default::default()
	};
	let _ = manifest.apply_changeset(&add_changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.default_owner_levels().get_levels()[1].tables.len(), 1);

	let delete_changeset = ManifestChangeSet {
		deleted_tables: std::collections::HashSet::from([(1, 1)]),
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&delete_changeset).expect("Failed to apply changeset");

	assert_eq!(manifest.default_owner_levels().get_levels()[1].tables.len(), 0);

	manifest.revert_changeset(rollback);

	assert_eq!(
		manifest.default_owner_levels().get_levels()[1].tables.len(),
		1,
		"Single table should be restored"
	);
	assert_eq!(manifest.default_owner_levels().get_levels()[1].tables[0].id, 1);
}

#[test]
fn test_revert_idempotent() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");

	let mut manifest = fresh_manifest(&opts);

	let table = create_test_table_with_seq_nums(1, 1, 10, Arc::clone(&opts))
		.expect("Failed to create table");

	let changeset = ManifestChangeSet {
		new_tables: vec![(0, table)],
		..Default::default()
	};

	let rollback = manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

	assert_eq!(
		manifest.default_owner_levels().get_levels()[0].tables.len(),
		1,
		"Table should be added"
	);

	// Revert once
	manifest.revert_changeset(rollback);

	let state_after_revert = manifest.default_owner_levels().get_levels()[0].tables.len();

	assert_eq!(state_after_revert, 0, "Table should be removed after revert");
}

#[test]
fn test_apply_revert_apply_cycle() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");

	let mut manifest = fresh_manifest(&opts);

	let table1 = create_test_table_with_seq_nums(1, 1, 10, Arc::clone(&opts))
		.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(2, 11, 20, Arc::clone(&opts))
		.expect("Failed to create table 2");

	// First apply
	let changeset1 = ManifestChangeSet {
		new_tables: vec![(0, table1)],
		..Default::default()
	};
	let rollback1 = manifest.apply_changeset(&changeset1).expect("Failed to apply changeset");

	assert_eq!(manifest.default_owner_levels().get_levels()[0].tables.len(), 1);

	// Revert
	manifest.revert_changeset(rollback1);

	assert_eq!(manifest.default_owner_levels().get_levels()[0].tables.len(), 0);

	// Apply again
	let table1_again = create_test_table_with_seq_nums(1, 1, 10, Arc::clone(&opts))
		.expect("Failed to create table 1");
	let changeset2 = ManifestChangeSet {
		new_tables: vec![(0, table1_again), (0, table2)],
		..Default::default()
	};
	let _rollback2 = manifest.apply_changeset(&changeset2).expect("Failed to apply changeset");

	assert_eq!(manifest.default_owner_levels().get_levels()[0].tables.len(), 2);
}

#[test]
fn test_revert_after_disk_write_failure_simulation() {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	let repo_path = temp_dir.path().to_path_buf();
	opts.path = repo_path;
	opts.level_count = 3;
	let opts = Arc::new(opts);

	fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");

	let mut manifest = fresh_manifest(&opts);

	// Add some initial tables
	let table1 = create_test_table_with_seq_nums(1, 1, 10, Arc::clone(&opts))
		.expect("Failed to create table 1");
	let table2 = create_test_table_with_seq_nums(2, 11, 20, Arc::clone(&opts))
		.expect("Failed to create table 2");

	let initial_changeset = ManifestChangeSet {
		new_tables: vec![(0, table1), (0, table2)],
		..Default::default()
	};
	let _ = manifest.apply_changeset(&initial_changeset).expect("Failed to apply changeset");

	// Capture state before the operation that will "fail"
	let state_before = (
		manifest.default_owner_levels().get_levels()[0].tables.len(),
		manifest.default_owner_levels().get_levels()[1].tables.len(),
		manifest.get_last_sequence(),
		manifest.get_log_number(),
	);

	// Simulate a compaction-like operation: delete old tables, add new table
	let table3 = create_test_table_with_seq_nums(3, 21, 100, Arc::clone(&opts))
		.expect("Failed to create table 3");

	let compaction_changeset = ManifestChangeSet {
		deleted_tables: std::collections::HashSet::from([(0, 1), (0, 2)]),
		new_tables: vec![(1, table3)],
		log_number: Some(50),
		..Default::default()
	};

	// Apply changeset (simulating in-memory update)
	let rollback =
		manifest.apply_changeset(&compaction_changeset).expect("Failed to apply changeset");

	// Verify in-memory state changed
	assert_eq!(manifest.default_owner_levels().get_levels()[0].tables.len(), 0);
	assert_eq!(manifest.default_owner_levels().get_levels()[1].tables.len(), 1);
	assert_eq!(manifest.get_last_sequence(), 100);
	assert_eq!(manifest.get_log_number(), 50);

	// Simulate disk write failure - revert the changeset
	manifest.revert_changeset(rollback);

	// Verify state is restored to before the operation
	assert_eq!(
		manifest.default_owner_levels().get_levels()[0].tables.len(),
		state_before.0,
		"L0 table count should be restored"
	);
	assert_eq!(
		manifest.default_owner_levels().get_levels()[1].tables.len(),
		state_before.1,
		"L1 table count should be restored"
	);
	assert_eq!(manifest.get_last_sequence(), state_before.2, "last_sequence should be restored");
	assert_eq!(manifest.get_log_number(), state_before.3, "log_number should be restored");

	// Verify original tables are still present
	assert!(manifest.default_owner_levels().get_levels()[0].tables.iter().any(|t| t.id == 1));
	assert!(manifest.default_owner_levels().get_levels()[0].tables.iter().any(|t| t.id == 2));
	assert!(!manifest.default_owner_levels().get_levels()[1].tables.iter().any(|t| t.id == 3));
}

// ===== BR3: owner-partitioned manifest =====

fn foreign_owner() -> crate::batch::BatchOwner {
	crate::batch::BatchOwner {
		branch: crate::BranchId([7; 16]),
		generation: crate::BranchGeneration(1),
	}
}

// Creates a table with an explicit physical owner and sequence range.
fn create_owned_test_table(
	table_id: u64,
	owner: crate::batch::BatchOwner,
	seq_start: u64,
	num_items: u64,
	opts: Arc<Options>,
) -> Result<Arc<Table>> {
	let table_file_path = opts.sstable_file_path(table_id);
	let mut file = SysFile::create(&table_file_path)?;
	let mut writer = TableWriter::new_owned(&mut file, table_id, Arc::clone(&opts), 0, owner);
	for i in 0..num_items {
		let key = format!("key_{i:05}");
		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), seq_start + i, InternalKeyKind::Set, 0);
		writer.add(internal_key, b"value")?;
	}
	let size = writer.finish()?;
	let file = SysFile::open(&table_file_path)?;
	file.sync_all()?;
	let file: Arc<dyn File> = Arc::new(file);
	Ok(Arc::new(Table::new(table_id, opts, file, size as u64)?))
}

/// Reload with a catalog containing the default branch AND the br3 foreign
/// branch (both must hydrate for the two-owner tests).
fn reload_two_owner_manifest(opts: &Arc<Options>) -> crate::Result<LevelManifest> {
	let authority = authority_for(opts);
	let mut catalog = crate::branch::BranchCatalog::new(crate::BranchId::DEFAULT);
	let record = catalog.create(foreign_owner().branch, "br3/foreign", 0).unwrap();
	assert_eq!(record.generation, foreign_owner().generation, "fixture generation must line up");
	let root = authority.load_latest_root()?;
	let catalog_version = latest_published_catalog_version(opts);
	LevelManifest::hydrate(
		Arc::clone(opts),
		authority,
		&catalog,
		catalog_version,
		root.as_ref(),
		std::sync::Arc::new(crate::timeline::Timeline::new()),
	)
}

fn br3_test_opts() -> Arc<Options> {
	let mut opts = Options::default();
	let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
	opts.path = temp_dir.path().to_path_buf();
	opts.level_count = 3;
	// Leak the TempDir so the directory outlives the returned Options.
	std::mem::forget(temp_dir);
	fs::create_dir_all(opts.sstable_dir()).unwrap();
	fs::create_dir_all(opts.wal_dir()).unwrap();
	Arc::new(opts)
}

/// Two owners holding the same key range is legal: level invariants are
/// evaluated inside one owner only.
#[test]
fn br3_same_key_ranges_in_two_owners_are_legal() {
	let opts = br3_test_opts();
	let mut manifest = fresh_manifest(&opts);

	let default_owner = crate::batch::BatchOwner::DEFAULT;
	let table_a = create_owned_test_table(1, default_owner, 1, 10, Arc::clone(&opts)).unwrap();
	let table_b = create_owned_test_table(2, foreign_owner(), 100, 10, Arc::clone(&opts)).unwrap();

	// Non-vacuity: the two tables really cover the same user-key range.
	assert_eq!(
		table_a.meta.smallest_point.as_ref().map(|k| k.user_key.clone()),
		table_b.meta.smallest_point.as_ref().map(|k| k.user_key.clone()),
		"fixture tables must overlap"
	);

	let changeset_a = ManifestChangeSet {
		owner: default_owner,
		new_tables: vec![(1, Arc::clone(&table_a))],
		..Default::default()
	};
	manifest.apply_changeset(&changeset_a).expect("default-owner apply must succeed");

	let changeset_b = ManifestChangeSet {
		owner: foreign_owner(),
		new_tables: vec![(1, Arc::clone(&table_b))],
		..Default::default()
	};
	manifest.apply_changeset(&changeset_b).expect("foreign-owner apply must succeed");

	manifest.persist_owner_update(BatchOwner::DEFAULT).unwrap();
	manifest.persist_owner_update(foreign_owner()).unwrap();
	let reloaded = reload_two_owner_manifest(&opts).unwrap();

	assert_eq!(reloaded.levels_for(default_owner).unwrap().get_levels()[1].tables.len(), 1);
	assert_eq!(reloaded.levels_for(foreign_owner()).unwrap().get_levels()[1].tables.len(), 1);
}

/// Sabotage twin: a changeset whose table carries another physical owner must
/// fail closed before any mutation.
#[test]
fn br3_changeset_owner_mismatch_fails_closed_before_mutation() {
	let opts = br3_test_opts();
	let mut manifest = fresh_manifest(&opts);

	let foreign_table =
		create_owned_test_table(1, foreign_owner(), 1, 5, Arc::clone(&opts)).unwrap();

	// Non-vacuity: the same table applies cleanly under its own owner.
	let matching = ManifestChangeSet {
		owner: foreign_owner(),
		new_tables: vec![(0, Arc::clone(&foreign_table))],
		..Default::default()
	};
	let rollback = manifest.apply_changeset(&matching).expect("matching owner must apply");
	manifest.revert_changeset(rollback);

	let mismatched = ManifestChangeSet {
		owner: crate::batch::BatchOwner::DEFAULT,
		new_tables: vec![(0, Arc::clone(&foreign_table))],
		..Default::default()
	};
	let before_last_sequence = manifest.get_last_sequence();
	let result = manifest.apply_changeset(&mismatched);
	assert!(result.is_err(), "mixed-owner changeset must be rejected");
	assert_eq!(
		manifest.default_owner_levels().get_levels()[0].tables.len(),
		0,
		"rejected changeset must not mutate the manifest"
	);
	assert_eq!(manifest.get_last_sequence(), before_last_sequence);
}

/// A manifest listing a table under one owner while the table's persisted
/// metadata names another owner is corruption and fails closed on open.
#[test]
fn br3_on_disk_owner_mismatch_fails_closed_on_open() {
	let opts = br3_test_opts();
	let mut manifest = fresh_manifest(&opts);

	let foreign_table =
		create_owned_test_table(1, foreign_owner(), 1, 5, Arc::clone(&opts)).unwrap();

	// Bypass apply-time validation via the test-only mutable accessor to
	// craft the corrupt state: a foreign-owner table inside the DEFAULT set.
	Arc::make_mut(&mut manifest.default_owner_levels_mut().get_levels_mut()[0])
		.insert(Arc::clone(&foreign_table));
	manifest.last_sequence = 5;
	manifest.persist_owner_update(BatchOwner::DEFAULT).unwrap();

	let result = reload_two_owner_manifest(&opts);
	let err = result.err().expect("owner mismatch must fail closed on open").to_string();
	assert!(err.contains("owner"), "error must name the ownership violation: {err}");
}

/// A table listed in more than one place across the owned component sets is
/// corruption and fails closed on open. (A cross-owner duplicate always also
/// trips the owner-mismatch check first — one file has one persisted owner —
/// so the reachable duplicate shape is a double listing under one owner.)
#[test]
fn br3_duplicate_table_listing_fails_closed_on_open() {
	let opts = br3_test_opts();
	let mut manifest = fresh_manifest(&opts);

	let table =
		create_owned_test_table(1, crate::batch::BatchOwner::DEFAULT, 1, 5, Arc::clone(&opts))
			.unwrap();
	// A double listing WITHIN one state (L0 and L1) is rejected by the
	// format itself, at publish time — even earlier than open.
	Arc::make_mut(&mut manifest.default_owner_levels_mut().get_levels_mut()[0])
		.insert(Arc::clone(&table));
	Arc::make_mut(&mut manifest.default_owner_levels_mut().get_levels_mut()[1])
		.insert(Arc::clone(&table));
	manifest.last_sequence = 5;
	let err = manifest
		.persist_owner_update(BatchOwner::DEFAULT)
		.expect_err("within-state duplicate must fail the publish")
		.to_string();
	assert!(err.contains("listed more than once"), "error must name the duplication: {err}");

	// A CROSS-state duplicate (the same table id under two owners) is
	// hydrate's check: craft two states each listing table 1.
	let opts = br3_test_opts();
	let table_default =
		create_owned_test_table(1, crate::batch::BatchOwner::DEFAULT, 1, 5, Arc::clone(&opts))
			.unwrap();
	let mut manifest = fresh_manifest(&opts);
	Arc::make_mut(&mut manifest.default_owner_levels_mut().get_levels_mut()[0])
		.insert(table_default);
	manifest.last_sequence = 5;
	manifest.persist_owner_update(BatchOwner::DEFAULT).unwrap();
	// Publish a foreign state claiming the same table id directly.
	let foreign_state = crate::authority::format::BranchStateManifest {
		branch: foreign_owner().branch,
		generation: foreign_owner().generation,
		state_version: 1,
		last_sequence: 5,
		flushed_log_number: 0,
		retained_floor_seq: 0,
		levels: vec![vec![1]],
	};
	authority_for(&opts).publish_state(&foreign_state).unwrap();

	let result = reload_two_owner_manifest(&opts);
	let err = result.err().expect("cross-state duplicate must fail closed on open").to_string();
	assert!(err.contains("listed more than once"), "error must name the duplication: {err}");
}

/// Any state-manifest format version other than the rewrite line's is
/// rejected by identity at open, before structural decoding — a hand-crafted
/// on-disk file exercises the full open path (the format-level twin lives in
/// the authority module's own tests).
#[test]
fn fk1_foreign_state_version_is_rejected_by_identity_on_open() {
	let opts = br3_test_opts();
	let mut manifest = fresh_manifest(&opts);
	let table =
		create_owned_test_table(1, crate::batch::BatchOwner::DEFAULT, 1, 5, Arc::clone(&opts))
			.unwrap();
	Arc::make_mut(&mut manifest.default_owner_levels_mut().get_levels_mut()[0]).insert(table);
	manifest.last_sequence = 5;
	manifest.persist_owner_update(BatchOwner::DEFAULT).unwrap();

	// Patch the published state's format version to a future value and
	// refresh the trailing checksum so ONLY the version identity trips.
	let state_dir = opts.path.join("branch").join("00000000000000000000000000000000");
	let state_path = state_dir.join("00000000000000000001.state");
	let mut bytes = fs::read(&state_path).unwrap();
	bytes[4..6].copy_from_slice(&2u16.to_le_bytes());
	let body_end = bytes.len() - 4;
	let crc = crc32fast::hash(&bytes[..body_end]);
	bytes[body_end..].copy_from_slice(&crc.to_le_bytes());
	fs::write(&state_path, &bytes).unwrap();

	let result = reload_two_owner_manifest(&opts);
	let err = result.err().expect("future state version must be rejected").to_string();
	assert!(err.contains("future format version 2"), "rejection must name the version: {err}");
}
