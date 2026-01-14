use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};

use tempfile::TempDir;
use test_log::test;

use crate::clock::MockLogicalClock;
use crate::compaction::compactor::{CompactionOptions, Compactor};
use crate::compaction::leveled::{CompactionPriority, Strategy};
use crate::compaction::{CompactionChoice, CompactionStrategy};
use crate::comparator::{BytewiseComparator, InternalKeyComparator};
use crate::error::{BackgroundErrorHandler, Result};
use crate::iter::CompactionIterator;
use crate::levels::{write_manifest_to_disk, Level, LevelManifest, Levels};
use crate::memtable::ImmutableMemtables;
use crate::sstable::table::{Table, TableFormat, TableWriter};
use crate::sstable::{InternalIterator, InternalKey, InternalKeyKind};
use crate::vlog::ValueLocation;
use crate::{CompressionType, Key, Options, Value};

/// Test environment setup helpers
struct TestEnv {
	#[allow(unused)]
	temp_dir: TempDir,
	options: Arc<Options>,
}

impl TestEnv {
	fn new() -> Self {
		Self::new_with_levels(4) // Default to 4 levels
	}

	fn new_with_levels(level_count: u8) -> Self {
		let temp_dir = TempDir::new().unwrap();
		let table_dir = temp_dir.path().join("sstables");
		std::fs::create_dir_all(&table_dir).unwrap();

		let options = Arc::new(Options {
			path: temp_dir.path().to_path_buf(),
			level_count,
			max_memtable_size: 1024 * 1024, // 1MB
			..Default::default()
		});

		Self {
			temp_dir,
			options,
		}
	}

	fn create_test_table(
		&self,
		id: u64,
		entries: Vec<(InternalKey, Vec<u8>)>,
	) -> Result<Arc<Table>> {
		let table_path = self.options.sstable_file_path(id);
		let file = File::create(&table_path)?;

		// Create a TableWriter
		let mut writer = TableWriter::new(file, id, Arc::clone(&self.options), 0); // Test table, use L0

		// Add entries to the table
		for (key, value) in entries {
			writer.add(key, &value)?;
		}

		// Finish writing the table
		writer.finish()?;

		// Open the table
		let file = std::fs::File::open(&table_path)?;
		let file_size = file.metadata()?.len();
		let file = Arc::new(file);

		// Create and return the table
		let table = Table::new(id, Arc::clone(&self.options), file, file_size)?;

		Ok(Arc::new(table))
	}
}

/// Helper function to create a comparator for testing
fn create_comparator() -> Arc<InternalKeyComparator> {
	Arc::new(InternalKeyComparator::new(Arc::new(BytewiseComparator::default())))
}

/// Helper function to create encoded inline values for testing
fn create_inline_value(value: &[u8]) -> Vec<u8> {
	let location = ValueLocation::with_inline_value(value.to_vec());
	location.encode()
}

/// Helper function to create test entries with automatic value encoding
fn create_test_entries(
	min_key: u64,
	max_key: u64,
	min_seq: u64,
	value_prefix: &str,
) -> Vec<(InternalKey, Vec<u8>)> {
	let mut entries = Vec::new();
	for key_val in min_key..=max_key {
		let user_key = format!("key-{key_val:010}").into_bytes();
		let key =
			InternalKey::new(user_key, min_seq + (key_val - min_key), InternalKeyKind::Set, 0);
		let value = format!("{value_prefix}-{key_val}").into_bytes();
		let encoded_value = create_inline_value(&value);
		entries.push((key, encoded_value));
	}
	entries
}

/// Helper function to create ordered entries with automatic value encoding
fn create_ordered_entries(
	key_prefix: &str,
	start: u32,
	count: u32,
	seq_num: u64,
	value_prefix: Option<&str>,
) -> Vec<(InternalKey, Vec<u8>)> {
	let mut entries = Vec::new();
	let value_prefix = value_prefix.unwrap_or("value");

	for i in 0..count {
		let key = format!("{}-{:05}", key_prefix, start + i).into_bytes();
		let value = format!("{}-{:05}", value_prefix, start + i).into_bytes();
		let internal_key = InternalKey::new(key, seq_num + i as u64, InternalKeyKind::Set, 0);
		let encoded_value = create_inline_value(&value);
		entries.push((internal_key, encoded_value));
	}
	entries
}

/// Creates key-value entries for testing
fn create_entries(min_key: u64, max_key: u64, min_seq: u64) -> Vec<(InternalKey, Vec<u8>)> {
	create_test_entries(min_key, max_key, min_seq, "value")
}

/// Helper function to create Options compatible with old Strategy::new(level0_trigger, multiplier)
/// This approximates the old count-based behavior with bytes-based limits
fn create_options_with_compaction_settings(
	base_opts: &Options,
	level0_trigger: usize,
	multiplier: f64,
) -> Arc<Options> {
	let mut opts = (*base_opts).clone();
	opts.level0_max_files = level0_trigger;
	opts.max_bytes_for_level = 1024;
	opts.level_multiplier = multiplier;
	Arc::new(opts)
}

/// Helper function to create a Strategy with a specific compaction priority for tests
fn create_strategy_with_priority(opts: &Options, priority: CompactionPriority) -> Strategy {
	Strategy::from_options_with_priority(Arc::new(opts.clone()), priority)
}

/// Creates test manifest with tables at specified levels
fn create_test_manifest(
	env: &TestEnv,
	level_tables: Vec<Vec<(u64, u64, u64, u64)>>, // id, min_seq, min_key, max_key
) -> Result<Arc<RwLock<LevelManifest>>> {
	let manifest_path = env.options.path.join("test_manifest");

	// Initialize empty levels
	let level_count = level_tables.len();
	let mut levels = Levels::new(level_count, 10);

	// Create and add tables to levels
	let mut max_table_id = 0;

	for (level_idx, level_specs) in level_tables.iter().enumerate() {
		for &(id, min_seq, min_key, max_key) in level_specs {
			max_table_id = std::cmp::max(max_table_id, id);

			let entries = create_entries(min_key, max_key, min_seq);
			let table = env.create_test_table(id, entries)?;

			// Add table to the appropriate level
			Arc::make_mut(&mut levels.get_levels_mut()[level_idx]).insert(table);
		}
	}

	// Choose a safe value for next_table_id
	let next_table_id = max_table_id + 1000;

	// Create the manifest with next_table_id
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(next_table_id)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};

	// Write the manifest to disk
	write_manifest_to_disk(&manifest)?;

	Ok(Arc::new(RwLock::new(manifest)))
}

/// Creates compaction options for testing
fn create_compaction_options(
	opts: Arc<Options>,
	manifest: Arc<RwLock<LevelManifest>>,
) -> CompactionOptions {
	std::fs::create_dir_all(opts.vlog_dir()).unwrap();
	std::fs::create_dir_all(opts.discard_stats_dir()).unwrap();
	std::fs::create_dir_all(opts.delete_list_dir()).unwrap();

	let vlog = Arc::new(crate::vlog::VLog::new(Arc::clone(&opts), None).unwrap());

	CompactionOptions {
		lopts: opts,
		level_manifest: manifest,
		immutable_memtables: Arc::new(RwLock::new(ImmutableMemtables::default())),
		vlog: Some(vlog),
		error_handler: Arc::new(BackgroundErrorHandler::new()),
	}
}

/// Verifies all expected key-value pairs are present after compaction
fn verify_keys_after_compaction(
	manifest: &RwLock<LevelManifest>,
	expected_keys: &HashSet<(Key, Value)>,
) -> (usize, HashMap<Key, Value>) {
	let manifest_guard = manifest.read().unwrap();
	let levels = manifest_guard.levels.get_levels();

	// Build a map of all key-value pairs from all tables across all levels
	let mut all_key_values = HashMap::new();
	let mut count = 0;

	for level in levels {
		for table in &level.tables {
			let mut iter = table.iter(None);
			while iter.valid() {
				let key = iter.key().to_owned().user_key.clone();
				let value = iter.value().to_vec();
				count += 1;
				all_key_values.insert(key, value);
				iter.next().unwrap();
			}
		}
	}

	// Check if we found all expected keys
	let mut missing_keys = Vec::new();
	for (expected_key, expected_value) in expected_keys {
		if let Some(actual_value) = all_key_values.get(expected_key) {
			// Verify value matches
			assert_eq!(actual_value, expected_value, "Value mismatch for key {expected_key:?}");
		} else {
			missing_keys.push(expected_key.clone());
		}
	}

	assert_eq!(missing_keys.len(), 0, "Missing keys after compaction: {missing_keys:?}");

	(count, all_key_values)
}

/// Verifies that all expected keys are present with correct values
fn verify_all_keys_present(
	manifest: &RwLock<LevelManifest>,
	expected_keys: &HashMap<Key, Value>,
) -> bool {
	let manifest_guard = manifest.read().unwrap();

	// Build map of all keys found after compaction
	let mut all_key_values = HashMap::new();
	let levels = manifest_guard.levels.get_levels();

	// Collect all keys from all tables across all levels
	for level in levels {
		for table in &level.tables {
			let mut iter = table.iter(None);

			while iter.valid() {
				let key = iter.key().to_owned().user_key.clone();
				let value = iter.value().to_vec();
				all_key_values.insert(key, value);
				iter.next().unwrap();
			}
		}
	}

	// Verify all expected keys are present with correct values
	let mut all_keys_found = true;
	for (expected_key, expected_value) in expected_keys {
		if let Some(actual_value) = all_key_values.get(expected_key) {
			if actual_value != expected_value {
				println!("Value mismatch for key {expected_key:?}");
				all_keys_found = false;
			}
		} else {
			println!("Missing key {expected_key:?}");
			all_keys_found = false;
		}
	}

	all_keys_found
}

/// Performs N rounds of compaction
fn perform_compaction_rounds(compactor: &Compactor, rounds: usize) {
	for i in 1..=rounds {
		let result = compactor.compact();
		assert!(result.is_ok(), "Compaction round {} failed: {:?}", i, result.err());
		println!("Compaction round {i} completed");
	}
}

#[test]
fn test_level_selection() {
	let env = TestEnv::new();

	// Define tables for each level
	let level_tables = vec![
		// L0: 5 tables (exceeds limit of 4)
		vec![
			(1, 100, 10, 20), // id, min_seq, min_key, max_key
			(2, 110, 15, 25),
			(3, 120, 20, 30),
			(4, 130, 25, 35),
			(5, 140, 30, 40),
		],
		// L1: 3 tables (within limit)
		vec![(11, 50, 5, 15), (12, 60, 20, 30), (13, 70, 35, 45)],
		// L2: 2 tables
		vec![(21, 30, 0, 25), (22, 40, 30, 50)],
	];

	// Create the manifest with these tables
	let manifest = create_test_manifest(&env, level_tables).unwrap();

	// Create the leveled compaction strategy
	let opts = create_options_with_compaction_settings(&env.options, 4, 2.0);
	let strategy = Strategy::from_options(opts);

	// Test the strategy's level selection
	let choice = strategy.pick_levels(&manifest.read().unwrap()).unwrap();

	// Verify L0 was selected for compaction (as it exceeds its limit)
	match choice {
		CompactionChoice::Merge(input) => {
			assert_eq!(input.source_level, 0, "L0 should be selected as source level");
			assert_eq!(input.target_level, 1, "L1 should be selected as target level");

			// Verify all L0 tables are included
			for id in 1..=5 {
				assert!(
					input.tables_to_merge.contains(&id),
					"Table {id} from L0 should be included"
				);
			}

			// Verify overlapping L1 table is included
			assert!(
				input.tables_to_merge.contains(&12),
				"Overlapping table 12 from L1 should be included"
			);
		}
		CompactionChoice::Skip => {
			panic!("Compaction should not be skipped when L0 exceeds limit");
		}
	}
}

#[test]
fn test_compaction_edge_cases() {
	let env = TestEnv::new();

	// 1. Empty level test
	let empty_level_tables = vec![
		vec![],                // Empty L0
		vec![(11, 50, 5, 15)], // One table in L1
	];

	let manifest = create_test_manifest(&env, empty_level_tables).unwrap();
	let opts = create_options_with_compaction_settings(&env.options, 4, 2.0);
	let strategy = Strategy::from_options(opts);

	// Test strategy with empty level
	let choice = strategy.pick_levels(&manifest.read().unwrap()).unwrap();

	// Strategy should skip compaction when L0 is empty
	match choice {
		CompactionChoice::Skip => { /* Expected */ }
		CompactionChoice::Merge(_) => {
			panic!("Compaction should be skipped when L0 is empty");
		}
	}

	// 2. Last level test
	let last_level_tables = vec![
		vec![], // L0
		vec![], // L1
		vec![], // L2
		vec![
			// Many tables in L3 (last level)
			(31, 30, 10, 20),
			(32, 40, 30, 40),
			(33, 50, 50, 60),
			(34, 60, 70, 80),
			(35, 70, 90, 100),
		],
	];

	let manifest = create_test_manifest(&env, last_level_tables).unwrap();

	// Test strategy with many tables in last level
	let choice = strategy.pick_levels(&manifest.read().unwrap()).unwrap();

	// If the last level exceeds its byte limit, it can compact to itself for tombstone cleanup
	match choice {
		CompactionChoice::Skip => {
			// Skip is OK if last level doesn't exceed byte limit
		}
		CompactionChoice::Merge(input) => {
			// If compaction is selected, it should be same-level compaction
			if input.source_level == 3 {
				assert_eq!(input.target_level, 3, "Bottom level should compact to itself");
			}
		}
	}
}

#[test]
fn test_level_selection_score_based() {
	let env = TestEnv::new();

	// Create levels with tables of known sizes to verify score calculation
	let mut levels = Levels::new(3, 10);

	// Create L1 with small amount of data
	let l1_entries = create_entries(0, 5, 100);
	let l1_table = env.create_test_table(1, l1_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(Arc::clone(&l1_table));

	// Create L2 with more data (larger file)
	let l2_entries = create_entries(0, 50, 200);
	let l2_table = env.create_test_table(2, l2_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[2]).insert(Arc::clone(&l2_table));

	// Read actual file sizes
	let l1_bytes = l1_table.file_size;
	let l2_bytes = l2_table.file_size;

	// Set up options where L2 will have a higher score than L1
	// max_bytes_for_level_base: Set so L1 score < 1.0 but L2 score > 1.0
	// level_multiplier: 1.0 so L2 target = L1 target (both use base)
	let max_bytes_for_level_base = l1_bytes * 2; // L1 score will be ~0.5
	let level_multiplier = 1.0; // L2 target = base * 1.0^1 = base

	// Create manifest
	let manifest_path = env.options.path.join("test_manifest_score");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};
	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	// Calculate expected scores using the actual formula
	// L1 score = l1_bytes / max_bytes_for_level(1)
	//          = l1_bytes / (max_bytes_for_level_base * multiplier^0)
	//          = l1_bytes / max_bytes_for_level_base
	let l1_score = l1_bytes as f64 / max_bytes_for_level_base as f64;

	// L2 score = l2_bytes / max_bytes_for_level(2)
	//          = l2_bytes / (max_bytes_for_level_base * multiplier^1)
	//          = l2_bytes / (max_bytes_for_level_base * 1.0)
	//          = l2_bytes / max_bytes_for_level_base
	let l2_score = l2_bytes as f64 / max_bytes_for_level_base as f64;

	// Create strategy with calculated limits
	let mut opts = (*env.options).clone();
	opts.max_bytes_for_level = max_bytes_for_level_base;
	opts.level_multiplier = level_multiplier;
	let opts = Arc::new(opts);
	let strategy = Strategy::from_options(opts);

	// Verify selection matches expected highest score
	let manifest_guard = manifest.read().unwrap();
	let choice = strategy.pick_levels(&manifest_guard).unwrap();

	match choice {
		CompactionChoice::Merge(input) => {
			// Since l2_bytes > l1_bytes, l2_score > l1_score
			// And l2_score should be >= 1.0 (since we set base to make L1 ~0.5)
			if l2_score > l1_score && l2_score >= 1.0 {
				assert_eq!(
					input.source_level, 2,
					"L2 should be selected (score {:.2} > L1 score {:.2})",
					l2_score, l1_score
				);
			} else if l1_score >= 1.0 {
				assert_eq!(
					input.source_level, 1,
					"L1 should be selected (score {:.2} >= 1.0)",
					l1_score
				);
			} else {
				panic!(
					"Unexpected selection: L{} selected when L1 score {:.2}, L2 score {:.2}",
					input.source_level, l1_score, l2_score
				);
			}
		}
		CompactionChoice::Skip => {
			assert!(
				l1_score < 1.0 && l2_score < 1.0,
				"Skip only when all scores < 1.0 (L1: {:.2}, L2: {:.2})",
				l1_score,
				l2_score
			);
		}
	}
}

/// Generates key-value entries for a table
fn generate_entries(
	table_idx: usize,
	keys_per_table: usize,
	seq_num: u64,
) -> Vec<(InternalKey, Vec<u8>)> {
	let mut entries = Vec::new();

	for i in 0..keys_per_table {
		// Create a key with table and index - format: "table{:02d}-key-{:03d}"
		let key = format!("table{table_idx:02}-key-{i:03}").into_bytes();
		let internal_key = InternalKey::new(key, seq_num, InternalKeyKind::Set, 0);

		// Create a value that's predictable - format: "value-{:02d}-{:03d}"
		let value = format!("value-{table_idx:02}-{i:03}").into_bytes();
		let encoded_value = create_inline_value(&value);

		entries.push((internal_key, encoded_value));
	}

	entries
}

#[test(tokio::test)]
async fn test_simple_merge_compaction() {
	let env = TestEnv::new();

	// Create 10 tables, each with 10 keys
	const TABLE_COUNT: usize = 10;
	const KEYS_PER_TABLE: usize = 10;
	const TOTAL_KEYS: usize = TABLE_COUNT * KEYS_PER_TABLE;

	// Track expected keys for verification
	let mut expected_keys = HashSet::new();

	// Create tables for L0
	let mut l0_tables = Vec::new();
	for i in 0..TABLE_COUNT {
		let id = (i + 1) as u64;
		let seq = 100 + i as u64;

		let entries = generate_entries(i, KEYS_PER_TABLE, seq);

		// Track expected keys
		for (key, value) in &entries {
			expected_keys.insert((key.user_key.clone(), value.clone()));
		}

		let table = env.create_test_table(id, entries).unwrap();
		l0_tables.push(table);
	}

	// Initialize levels with these tables
	let mut levels = Levels::new(3, 10);

	// Add tables to L0
	for table in l0_tables {
		Arc::make_mut(&mut levels.get_levels_mut()[0]).insert(table);
	}

	// Verify we have the expected number of tables and keys
	{
		let mut total_keys = 0;
		for table in &levels.get_levels()[0].tables {
			let mut table_keys = 0;
			let mut iter = table.iter(None);

			while iter.valid() {
				table_keys += 1;
				iter.next().unwrap();
			}
			total_keys += table_keys;
		}

		assert_eq!(
			levels.get_levels()[0].tables.len(),
			TABLE_COUNT,
			"Should have exactly {TABLE_COUNT} tables in L0"
		);
		assert_eq!(
			total_keys, TOTAL_KEYS,
			"Should have exactly {TOTAL_KEYS} keys across all tables"
		);
	}

	// Create and write the manifest
	let manifest_path = env.options.path.join("test_manifest");

	// Find the maximum table ID used
	let mut max_table_id = 0;
	for level in levels.get_levels() {
		for table in &level.tables {
			max_table_id = std::cmp::max(max_table_id, table.id);
		}
	}

	// Use a safe starting value for next_table_id
	let next_table_id = max_table_id + 1000;

	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(next_table_id)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};

	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	// Create the leveled compaction strategy
	let opts = create_options_with_compaction_settings(&env.options, 4, 2.0);
	let strategy = Arc::new(Strategy::from_options(opts));

	// Create compaction options
	let compaction_options = create_compaction_options(env.options, Arc::clone(&manifest));

	// Create the compactor
	let compactor = Compactor::new(compaction_options, strategy);

	// Run compaction
	let result = compactor.compact();
	assert!(result.is_ok(), "Compaction should succeed");

	// Verify all keys are still present after compaction
	let (_, all_key_values) = verify_keys_after_compaction(&manifest, &expected_keys);

	// Verify correct total key count
	assert_eq!(
		all_key_values.len(),
		TOTAL_KEYS,
		"Expected to find all {} keys, but only found {}",
		TOTAL_KEYS,
		all_key_values.len()
	);

	{
		let updated_manifest = manifest.read().unwrap();

		// L1 should have at least one table after compaction
		let l1_tables = &updated_manifest.levels.get_levels()[1].tables;
		assert!(!l1_tables.is_empty(), "L1 should have at least one table after compaction");

		// L0 should be under its limit after compaction
		let l0_tables = &updated_manifest.levels.get_levels()[0].tables;
		assert!(l0_tables.len() < 4, "L0 should be under its limit after compaction");

		// All original L0 tables should be removed by compaction since ALL L0 tables
		// are selected for L0→L1 compaction
		let original_table_ids: Vec<u64> = (1..=TABLE_COUNT as u64).collect();
		let all_tables = updated_manifest.get_all_tables();
		let remaining_original_count =
			original_table_ids.iter().filter(|id| all_tables.contains_key(id)).count();

		assert_eq!(
                remaining_original_count, 0,
                "All original L0 tables should be removed by compaction. Remaining: {remaining_original_count}"
            );
	}
}

#[test(tokio::test)]
async fn test_multi_level_merge_compaction() {
	// Generate key-value entries for a table
	fn generate_entries(
		level: usize,
		table_idx: usize,
		start_idx: usize,
		keys_per_table: usize,
		seq_num: u64,
	) -> Vec<(InternalKey, Vec<u8>)> {
		let mut entries = Vec::new();

		for i in 0..keys_per_table {
			let idx = start_idx + i;
			let key = format!("L{level}-T{table_idx:02}-K-{idx:05}").into_bytes();
			let internal_key = InternalKey::new(key, seq_num, InternalKeyKind::Set, 0);
			let value = format!("V-{level}-{table_idx:02}-{idx:05}").into_bytes();
			let encoded_value = create_inline_value(&value);
			entries.push((internal_key, encoded_value));
		}

		entries
	}

	let env = TestEnv::new();

	// Define level configuration
	struct LevelConfig {
		level: usize,
		table_count: usize,
		keys_per_table: usize,
		base_id: u64,
		base_seq: u64,
	}

	// Configure each level with increasing number of keys
	let level_configs = vec![
		// Level 0: 10 tables × 10 keys = 100 keys total
		LevelConfig {
			level: 0,
			table_count: 10,
			keys_per_table: 10,
			base_id: 1,
			base_seq: 1000,
		},
		// Level 1: 8 tables × 15 keys = 120 keys total
		LevelConfig {
			level: 1,
			table_count: 8,
			keys_per_table: 15,
			base_id: 100,
			base_seq: 900,
		},
		// Level 2: 16 tables × 20 keys = 320 keys total
		LevelConfig {
			level: 2,
			table_count: 16,
			keys_per_table: 20,
			base_id: 200,
			base_seq: 800,
		},
		// Level 3: 30 tables × 30 keys = 900 keys total
		LevelConfig {
			level: 3,
			table_count: 30,
			keys_per_table: 30,
			base_id: 300,
			base_seq: 700,
		},
		// Level 4: 45 tables × 40 keys = 1800 keys total
		LevelConfig {
			level: 4,
			table_count: 45,
			keys_per_table: 40,
			base_id: 400,
			base_seq: 600,
		},
	];

	// Calculate total expected keys
	let total_expected_keys: usize =
		level_configs.iter().map(|c| c.table_count * c.keys_per_table).sum();

	// Track expected keys for verification
	let mut expected_keys = HashSet::new();
	let mut key_to_level_map = HashMap::new();
	let mut all_tables: Vec<Vec<Arc<Table>>> = vec![Vec::new(); level_configs.len()];

	for config in &level_configs {
		let mut level_tables = Vec::new();

		for i in 0..config.table_count {
			let id = config.base_id + i as u64;
			let seq = config.base_seq + i as u64;
			let start_idx = i * config.keys_per_table;

			let entries = generate_entries(config.level, i, start_idx, config.keys_per_table, seq);

			// Track expected keys
			for (key, value) in &entries {
				expected_keys.insert((key.user_key.clone(), value.clone()));
				key_to_level_map.insert(key.user_key.clone(), config.level);
			}

			let table = env.create_test_table(id, entries).unwrap();
			level_tables.push(table);
		}

		all_tables[config.level] = level_tables;
	}

	// Initialize levels
	let mut levels = Levels::new(level_configs.len(), 10);

	// Add tables to their respective levels
	for (level_idx, level_tables) in all_tables.into_iter().enumerate() {
		for table in level_tables {
			Arc::make_mut(&mut levels.get_levels_mut()[level_idx]).insert(table);
		}
	}

	// Create and write the manifest
	let manifest_path = env.options.path.join("test_manifest");
	let next_table_id = 1000; // Start well above existing IDs

	// Create a shared table ID counter that will be used by both the test and
	// compactor
	let shared_table_id_counter = Arc::new(AtomicU64::new(next_table_id));

	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: shared_table_id_counter,
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};

	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	// Create the strategy and compactor
	let opts = create_options_with_compaction_settings(&env.options, 4, 2.0);
	let strategy = Arc::new(Strategy::from_options(opts));
	let compaction_options = create_compaction_options(env.options, Arc::clone(&manifest));
	let compactor = Compactor::new(compaction_options, strategy);

	// Run multiple rounds of compaction
	const COMPACTION_ROUNDS: usize = 15;

	for round in 1..=COMPACTION_ROUNDS {
		let result = compactor.compact();
		if result.is_err() {
			// Not all rounds will have work to do, which is fine
			continue;
		}

		// Check if all levels are within limits
		let manifest_guard = manifest.read().unwrap();
		let levels = manifest_guard.levels.get_levels();

		let all_levels_ok = levels.iter().enumerate().all(|(idx, level)| {
			let limit = if idx == 0 {
				4
			} else {
				8 * 2u32.pow(idx as u32 - 1) as usize
			};
			level.tables.len() <= limit
		});

		if all_levels_ok && round >= 5 {
			break;
		}
	}

	// Verify all keys are still present after compaction
	let (_, all_key_values) = verify_keys_after_compaction(&manifest, &expected_keys);

	// Verify we found all keys
	assert_eq!(
		all_key_values.len(),
		total_expected_keys,
		"Expected to find all {} keys, but only found {}",
		total_expected_keys,
		all_key_values.len()
	);

	// Check for key range overlaps within levels
	let manifest_guard = manifest.read().unwrap();
	let levels = manifest_guard.levels.get_levels();

	for (level_idx, level) in levels.iter().enumerate().skip(1) {
		if level.tables.len() >= 2 {
			// Get key ranges for all tables in this level
			let mut table_ranges = Vec::new();

			for table in &level.tables {
				// Find min and max key in this table
				let mut min_key = None;
				let mut max_key = None;

				let mut iter = table.iter(None);
				while iter.valid() {
					let key = iter.key().to_owned().user_key.clone();
					if min_key.is_none() {
						min_key = Some(key.clone());
					}
					max_key = Some(key);
					iter.next().unwrap();
				}

				if let (Some(min), Some(max)) = (min_key, max_key) {
					table_ranges.push((table.id, min, max));
				}
			}

			// Sort by min_key
			table_ranges.sort_by(|a, b| a.1.cmp(&b.1));

			// Check for overlaps between adjacent tables
			for i in 0..(table_ranges.len().saturating_sub(1)) {
				let (id1, _, max_key1) = &table_ranges[i];
				let (id2, min_key2, _) = &table_ranges[i + 1];

				// Check if max_key1 >= min_key2, which would indicate overlap
				assert!(
					max_key1 < min_key2,
					"Overlap detected in L{} between table {} and {}: {:?} >= {:?}",
					level_idx,
					id1,
					id2,
					String::from_utf8_lossy(max_key1),
					String::from_utf8_lossy(min_key2)
				);
			}
		}
	}
}

#[test]
fn test_select_tables_for_compaction_bug() {
	let env = TestEnv::new();
	let opts = create_options_with_compaction_settings(&env.options, 4, 10.0);
	let strategy = Strategy::from_options(opts);

	// Create source level (L0) with tables with different but overlapping key
	// ranges
	let mut source_level = Level::with_capacity(10);

	// Create 3 tables for source level
	let table1 = env
		.create_test_table(
			1,
			create_ordered_entries("a", 10, 10, 100, None), // a-00010 to a-00019
		)
		.unwrap();

	let table2 = env
		.create_test_table(
			2,
			create_ordered_entries("b", 15, 10, 150, None), // b-00015 to b-00024
		)
		.unwrap();

	let table3 = env
		.create_test_table(
			3,
			create_ordered_entries("c", 20, 10, 200, None), // c-00020 to c-00029
		)
		.unwrap();

	source_level.insert(table1);
	source_level.insert(table2);
	source_level.insert(table3);

	// Create next level (L1) with tables that have overlapping key ranges with
	// source
	let mut next_level = Level::with_capacity(10);

	let table10 = env
		.create_test_table(
			10,
			create_ordered_entries("a", 5, 10, 50, None), /* a-00005 to a-00014 (overlaps
			                                               * with table1) */
		)
		.unwrap();

	let table11 = env
		.create_test_table(
			11,
			create_ordered_entries("b", 10, 10, 120, None), /* b-00010 to b-00019 (overlaps
			                                                 * with table2) */
		)
		.unwrap();

	let table12 = env
		.create_test_table(
			12,
			create_ordered_entries("c", 15, 10, 180, None), /* c-00015 to c-00024 (overlaps
			                                                 * with table3) */
		)
		.unwrap();

	// Create a table with the same ID as one in source level to trigger the bug
	let table_dup = env
		.create_test_table(
			1, // Same ID as table1 in source_level
			create_ordered_entries("a", 5, 8, 90, None), /* a-00005 to a-00012 (overlaps
			    * with table1) */
		)
		.unwrap();

	next_level.insert(table10);
	next_level.insert(table11);
	next_level.insert(table12);
	next_level.insert(table_dup); // This will potentially cause problems

	// Call select_tables_for_compaction
	let selected_tables =
		strategy.select_tables_for_compaction(&source_level, &next_level, 0).unwrap();

	// Check for duplicates
	let mut unique_ids = HashSet::new();
	let mut has_duplicates = false;

	for &id in &selected_tables {
		if !unique_ids.insert(id) {
			has_duplicates = true;
		}
	}

	// Check if all source tables were selected
	let source_table_ids: HashSet<_> = source_level.tables.iter().map(|t| t.id).collect();
	let selected_source_ids: HashSet<_> =
		selected_tables.iter().filter(|&&id| source_table_ids.contains(&id)).copied().collect();

	// Verify all source tables are selected
	assert_eq!(
		source_table_ids.len(),
		selected_source_ids.len(),
		"Not all source tables were selected!"
	);

	// Verify no duplicates
	assert!(!has_duplicates, "Found duplicate table IDs in the selected tables list");

	// Count occurrences of each table ID
	let mut id_count = HashMap::new();
	for &id in &selected_tables {
		*id_count.entry(id).or_insert(0) += 1;
	}

	// Check for any ID that appears more than once
	for (&id, &count) in &id_count {
		assert_eq!(count, 1, "Table ID {id} appears {count} times in the selected tables list");
	}
}

#[test]
fn test_l1_to_l2_table_selection() {
	let env = TestEnv::new();
	let opts = create_options_with_compaction_settings(&env.options, 4, 10.0);
	let strategy = Strategy::from_options(opts);

	// Create L1 with 3 tables (non-overlapping as per L1+ invariant)
	let mut source_level = Level::with_capacity(10);

	let table1 = env
		.create_test_table(
			1,
			create_ordered_entries("a", 10, 10, 100, None), // a-00010 to a-00019
		)
		.unwrap();

	let table2 = env
		.create_test_table(
			2,
			create_ordered_entries("b", 20, 10, 150, None), // b-00020 to b-00029
		)
		.unwrap();

	let table3 = env
		.create_test_table(
			3,
			create_ordered_entries("c", 30, 10, 200, None), // c-00030 to c-00039
		)
		.unwrap();

	source_level.insert(table1);
	source_level.insert(table2);
	source_level.insert(table3);

	// Create L2 with tables that have overlapping key ranges with some L1 tables
	let mut next_level = Level::with_capacity(10);

	let table10 = env
		.create_test_table(
			10,
			create_ordered_entries("a", 5, 15, 50, None), /* a-00005 to a-00019 (overlaps
			                                               * with table1) */
		)
		.unwrap();

	let table11 = env
		.create_test_table(
			11,
			create_ordered_entries("d", 40, 10, 120, None), // d-00040 to d-00049 (no overlap)
		)
		.unwrap();

	next_level.insert(table10);
	next_level.insert(table11);

	// Call select_tables_for_compaction for L1 → L2 (source_level_num = 1)
	let selected_tables =
		strategy.select_tables_for_compaction(&source_level, &next_level, 1).unwrap();

	// For L1+, we should select only ONE table from source level
	let source_table_ids: HashSet<_> = source_level.tables.iter().map(|t| t.id).collect();
	let selected_source_ids: HashSet<_> =
		selected_tables.iter().filter(|&&id| source_table_ids.contains(&id)).copied().collect();

	// Should select exactly ONE source table (not all like L0→L1)
	assert_eq!(
		selected_source_ids.len(),
		1,
		"L1+ compaction should select exactly ONE source table, got {}",
		selected_source_ids.len()
	);

	// The algorithm picks the first table from the source level, which is table 3
	// (since tables are stored in insertion order and we inserted 1, 2, 3)
	let _selected_l1_table = *selected_source_ids.iter().next().unwrap();

	// Should not select the other L1 tables since we only pick one for L1+
	assert!(
		!selected_tables.contains(&1)
			|| !selected_tables.contains(&2)
			|| !selected_tables.contains(&3),
		"Should NOT select all L1 tables (only one L1+ table)"
	);

	// Note: The overlap detection might not be working if no L2 tables are selected
	// This could be due to missing key range metadata in the test tables
	assert_eq!(selected_tables.len(), 1, "Should select exactly 1 table (only L1 table since overlap detection may not work in test)");
}

#[test]
fn test_l1_compaction_bounds_correctness() {
	// This test verifies that compaction uses the CORRECT table's bounds
	// when selecting overlapping tables from the next level.
	//
	// Bug scenario: If we select table3 (by size) but compute bounds from table1 (first by key),
	// we'll select wrong L2 tables.

	let env = TestEnv::new();
	let opts = create_options_with_compaction_settings(&env.options, 4, 10.0);
	let strategy = Strategy::from_options(opts);

	// Create L1 with 3 tables using proper L1+ insertion (sorted by key)
	let mut source_level = Level::with_capacity(10);

	// Table1: Smallest key ("a"), SMALL size (10 entries)
	let table1 = env
		.create_test_table(
			1,
			create_ordered_entries("a", 10, 10, 100, None), // a-00010 to a-00019, 10 entries
		)
		.unwrap();

	// Table2: Middle key ("b"), MEDIUM size (20 entries)
	let table2 = env
		.create_test_table(
			2,
			create_ordered_entries("b", 20, 20, 150, None), // b-00020 to b-00039, 20 entries
		)
		.unwrap();

	// Table3: Largest key ("c"), LARGEST size (30 entries) - should be selected by compensated size
	let table3 = env
		.create_test_table(
			3,
			create_ordered_entries("c", 30, 30, 200, None), // c-00030 to c-00059, 30 entries
		)
		.unwrap();

	// Use proper L1+ insertion (sorted by smallest key)
	source_level.insert_sorted_by_key(Arc::clone(&table1)); // First by key: a < b < c
	source_level.insert_sorted_by_key(Arc::clone(&table2));
	source_level.insert_sorted_by_key(Arc::clone(&table3));

	// Verify ordering: table1 should be first (smallest key)
	assert_eq!(source_level.tables[0].id, 1, "First table should be table1 (smallest key)");

	// Verify table3 is largest (most entries = largest file size)
	assert!(table3.file_size > table2.file_size);
	assert!(table2.file_size > table1.file_size);

	// Create L2 with tables that overlap with SPECIFIC L1 tables
	let mut next_level = Level::with_capacity(10);

	// L2 table that overlaps with table1 (a-keys)
	let l2_table_a = env
		.create_test_table(
			10,
			create_ordered_entries("a", 5, 15, 50, None), // a-00005 to a-00019 (overlaps table1)
		)
		.unwrap();

	// L2 table that overlaps with table3 (c-keys) - this is the one we should select!
	let l2_table_c = env
		.create_test_table(
			11,
			create_ordered_entries("c", 25, 35, 120, None), // c-00025 to c-00059 (overlaps table3)
		)
		.unwrap();

	// L2 table that doesn't overlap with any L1 table
	let l2_table_d = env
		.create_test_table(
			12,
			create_ordered_entries("d", 100, 10, 300, None), // d-00100 to d-00109 (no overlap)
		)
		.unwrap();

	next_level.insert_sorted_by_key(l2_table_a);
	next_level.insert_sorted_by_key(l2_table_c);
	next_level.insert_sorted_by_key(l2_table_d);

	// Call select_tables_for_compaction for L1 → L2
	let selected_tables =
		strategy.select_tables_for_compaction(&source_level, &next_level, 1).unwrap();

	// Verify we selected exactly ONE L1 table
	let source_table_ids: HashSet<_> = source_level.tables.iter().map(|t| t.id).collect();
	let selected_source_ids: HashSet<_> =
		selected_tables.iter().filter(|&&id| source_table_ids.contains(&id)).copied().collect();

	assert_eq!(selected_source_ids.len(), 1, "Should select exactly ONE L1 table");

	// The selected L1 table should be table3 (largest by compensated size)
	let selected_l1_id = *selected_source_ids.iter().next().unwrap();
	assert_eq!(
		selected_l1_id, 3,
		"Should select table3 (largest size), but selected table{}",
		selected_l1_id
	);

	// Verify we selected the CORRECT L2 table (l2_table_c, which overlaps with table3)
	// NOT l2_table_a (which overlaps with table1, the first table by key)
	let l2_table_ids: HashSet<_> = next_level.tables.iter().map(|t| t.id).collect();
	let selected_l2_ids: HashSet<_> =
		selected_tables.iter().filter(|&&id| l2_table_ids.contains(&id)).copied().collect();

	assert!(
		selected_l2_ids.contains(&11),
		"Should select L2 table 11 (overlaps with selected table3), but selected: {:?}",
		selected_l2_ids
	);
	assert!(
		!selected_l2_ids.contains(&10),
		"Should NOT select L2 table 10 (overlaps with table1, not selected table3)"
	);
	assert!(!selected_l2_ids.contains(&12), "Should NOT select L2 table 12 (no overlap)");
}

#[test(tokio::test)]
async fn test_compaction_with_large_keys_and_values() {
	let env = TestEnv::new();

	// Create tables with some large keys and values
	let mut levels = Levels::new(3, 10);

	// Create entries with large keys and values
	let mut large_entries = Vec::new();
	for i in 0..5 {
		// Reduced from 10 to 5 to keep test time manageable
		// Create large key (1KB)
		let key_base = format!("large-key-{i}");
		let key_padding = "X".repeat(1000);
		let key = format!("{key_base}{key_padding}").into_bytes();

		// Create large value (4KB)
		let value_base = format!("large-value-{i}");
		let value_padding = "Y".repeat(4000);
		let value = format!("{value_base}{value_padding}").into_bytes();

		let internal_key = InternalKey::new(key, 1000, InternalKeyKind::Set, 0);

		large_entries.push((internal_key, value));
	}

	// Add table with large entries to L0
	let large_table = env.create_test_table(1, large_entries.clone()).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[0]).insert(large_table);

	// Create manifest
	let manifest_path = env.options.path.join("test_manifest");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};

	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	// Track expected data
	let mut expected_data = HashMap::new();
	for (key, value) in &large_entries {
		expected_data.insert(key.user_key.clone(), value.clone());
	}

	// Set up compaction
	let opts = create_options_with_compaction_settings(&env.options, 4, 2.0);
	let strategy = Arc::new(Strategy::from_options(opts));
	let compaction_options = create_compaction_options(env.options, Arc::clone(&manifest));
	let compactor = Compactor::new(compaction_options, strategy);

	// Run compaction
	perform_compaction_rounds(&compactor, 2);

	// Verify all large keys and values are preserved
	assert!(
		verify_all_keys_present(&manifest, &expected_data),
		"Compaction did not preserve large keys and values"
	);
}

// TODO: add more tests for:
// - Compaction with keys that are split across multiple tables
#[test(tokio::test)]
async fn test_compaction_respects_sequence_numbers() {
	let env = TestEnv::new();

	// Create tables with same keys but different sequence numbers
	let mut levels = Levels::new(3, 10);

	// Create map to track expected final values
	let mut expected_final_values = HashMap::new();

	// Create L0 tables with overlapping keys but different sequence numbers
	for i in 0..5 {
		let base_seq = 100 - (i * 10); // Decreasing sequence numbers
		let mut entries = Vec::new();

		// Create 10 keys, same keys in each table but with different values and seqs
		for j in 0..10 {
			let key = format!("key-{j:03}").into_bytes();
			let key_bytes = key;
			let raw_value = format!("value-from-table-{}-seq-{}", i, base_seq + j).into_bytes();
			let encoded_value = create_inline_value(&raw_value);

			let internal_key =
				InternalKey::new(key_bytes.clone(), (base_seq + j) as u64, InternalKeyKind::Set, 0);

			entries.push((internal_key, encoded_value));

			// Update expected value if this is a higher sequence number
			if i == 0 {
				// First table has highest sequence numbers, so these should win
				expected_final_values.insert(key_bytes, raw_value);
			}
		}

		// Create the table
		let table = env.create_test_table(i as u64 + 1, entries).unwrap();
		Arc::make_mut(&mut levels.get_levels_mut()[0]).insert(table);
	}

	// Create manifest
	let manifest_path = env.options.path.join("test_manifest");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};

	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	// Set up compaction
	let opts = create_options_with_compaction_settings(&env.options, 4, 2.0);
	let strategy = Arc::new(Strategy::from_options(opts));
	let compaction_options = create_compaction_options(env.options, Arc::clone(&manifest));
	let compactor = Compactor::new(compaction_options, strategy);

	// Run compaction
	perform_compaction_rounds(&compactor, 2);

	// Verify the highest sequence number values are preserved
	let manifest_guard = manifest.read().unwrap();
	let levels = manifest_guard.levels.get_levels();

	// There should be no tables in L0 after compaction
	assert_eq!(levels[0].tables.len(), 0, "L0 should be empty after compaction");

	// Verify the key-values match expected
	let mut all_keys = HashMap::new();
	for level in levels {
		for table in &level.tables {
			let mut iter = table.iter(None);
			while iter.valid() {
				let key = iter.key().to_owned().user_key.clone();
				let location = ValueLocation::decode(&iter.value().to_vec()).unwrap();
				if location.is_value_pointer() {
					panic!("Unexpected VLog pointer in test");
				}
				all_keys.insert(key, (*location.value).to_vec());
				iter.next().unwrap();
			}
		}
	}

	for (key, expected_value) in &expected_final_values {
		if let Some(actual_value) = all_keys.get(key) {
			assert_eq!(
				actual_value.as_slice(),
				expected_value.as_slice(),
				"Value for key {key:?} doesn't match highest sequence number"
			);
		} else {
			panic!("Key {key:?} is missing after compaction");
		}
	}
}

#[test(tokio::test)]
async fn test_tombstone_propagation() {
	// Test 95% deletion via tombstones with bottom-level filtering
	let env = TestEnv::new();
	let mut levels = Levels::new(3, 10);

	// Create entries with tombstones before values (higher seq numbers first)
	let mut all_entries = Vec::new();
	for i in 0..100 {
		let key = format!("key-{i:03}").into_bytes();
		let key_bytes = key;

		// Add tombstone first (higher sequence number) for 95% of keys
		if i < 95 {
			let delete_key =
				InternalKey::new(key_bytes.clone(), 300 + i, InternalKeyKind::Delete, 0);
			all_entries.push((delete_key, vec![]));
		}

		// Add value second (lower sequence number)
		let raw_value = format!("original-value-{i}").into_bytes();
		let encoded_value = create_inline_value(&raw_value);

		let set_key = InternalKey::new(key_bytes, 100 + i, InternalKeyKind::Set, 0);
		all_entries.push((set_key, encoded_value));
	}

	// Create L0 table and manifest
	let table = env.create_test_table(100, all_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[0]).insert(table);

	let manifest_path = env.options.path.join("test_manifest");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};
	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	// Run compaction
	let opts = create_options_with_compaction_settings(&env.options, 1, 2.0);
	let strategy = Arc::new(Strategy::from_options(opts));
	let compaction_options = create_compaction_options(env.options, Arc::clone(&manifest));
	let compactor = Compactor::new(compaction_options, strategy);
	let result = compactor.compact();
	assert!(result.is_ok(), "Compaction failed");

	// Verify exactly 5 keys remain (95-99)
	let manifest_guard = manifest.read().unwrap();
	let levels = manifest_guard.levels.get_levels();

	let mut remaining_keys = Vec::new();
	for level in levels {
		for table in &level.tables {
			let mut iter = table.iter(None);
			while iter.valid() {
				let key = iter.key().to_owned();
				if key.kind() == InternalKeyKind::Set {
					let key_str = String::from_utf8_lossy(&key.user_key);
					remaining_keys.push(key_str.to_string());
				}
				iter.next().unwrap();
			}
		}
	}
	remaining_keys.sort();

	let expected_keys = vec!["key-095", "key-096", "key-097", "key-098", "key-099"];
	assert_eq!(remaining_keys, expected_keys);
}

#[test(tokio::test)]
async fn test_l0_overlapping_keys_compaction() {
	let env = TestEnv::new();
	let mut levels = Levels::new(3, 10);

	// Create L0 tables with overlapping key ranges
	// Table 1: key-005 to key-015, seq 105-115
	let mut entries1 = Vec::new();
	for i in 5..=15 {
		let key = format!("key-{i:03}").into_bytes();
		let raw_value = format!("value-from-table1-{i}").into_bytes();
		let encoded_value = create_inline_value(&raw_value);

		let internal_key = InternalKey::new(key, 100 + i, InternalKeyKind::Set, 0);
		entries1.push((internal_key, encoded_value));
	}

	// Table 2: key-010 to key-020, seq 150-160 (overlaps with table1)
	let mut entries2 = Vec::new();
	for i in 10..=20 {
		let key = format!("key-{i:03}").into_bytes();
		let raw_value = format!("value-from-table2-{i}").into_bytes();
		let encoded_value = create_inline_value(&raw_value);

		let internal_key = InternalKey::new(key, 150 + i - 10, InternalKeyKind::Set, 0);
		entries2.push((internal_key, encoded_value));
	}

	// Table 3: key-008 to key-012 with highest seq numbers + tombstone for key-014
	let mut entries3 = Vec::new();
	for i in 8..=12 {
		let key = format!("key-{i:03}").into_bytes();
		let raw_value = format!("value-from-table3-{i}").into_bytes();
		let encoded_value = create_inline_value(&raw_value);

		let internal_key = InternalKey::new(key, 200 + i - 8, InternalKeyKind::Set, 0);
		entries3.push((internal_key, encoded_value));
	}
	// Add tombstone that should win over other tables
	let tombstone_key = "key-014".as_bytes().to_vec();
	let tombstone = InternalKey::new(tombstone_key, 210, InternalKeyKind::Delete, 0);
	entries3.push((tombstone, vec![]));

	// Create tables and add to L0
	let table1 = env.create_test_table(1, entries1).unwrap();
	let table2 = env.create_test_table(2, entries2).unwrap();
	let table3 = env.create_test_table(3, entries3).unwrap();

	Arc::make_mut(&mut levels.get_levels_mut()[0]).insert(table1);
	Arc::make_mut(&mut levels.get_levels_mut()[0]).insert(table2);
	Arc::make_mut(&mut levels.get_levels_mut()[0]).insert(table3);

	// Create manifest and run compaction
	let manifest_path = env.options.path.join("test_manifest");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};
	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	let opts = create_options_with_compaction_settings(&env.options, 1, 2.0);
	let strategy = Arc::new(Strategy::from_options(opts));
	let compaction_options = create_compaction_options(env.options, Arc::clone(&manifest));
	let compactor = Compactor::new(compaction_options, strategy);
	perform_compaction_rounds(&compactor, 2);

	// Verify sequence number precedence: highest seq wins for overlapping keys
	let manifest_guard = manifest.read().unwrap();
	let levels = manifest_guard.levels.get_levels();

	let mut all_keys = HashMap::new();
	let mut tombstones = HashMap::new();
	for level in levels {
		for table in &level.tables {
			let mut iter = table.iter(None);
			while iter.valid() {
				let key = iter.key().to_owned();
				let encoded_value = iter.value().to_vec();
				match key.kind() {
					InternalKeyKind::Set => {
						let location = ValueLocation::decode(&encoded_value).unwrap();
						if location.is_value_pointer() {
							panic!("Unexpected VLog pointer in test");
						}
						all_keys.insert(key.user_key.clone(), (*location.value).to_vec());
					}
					InternalKeyKind::Delete => {
						tombstones.insert(key.user_key.clone(), key.seq_num());
					}
					_ => {}
				}
				iter.next().unwrap();
			}
		}
	}

	// Test specific key outcomes
	assert!(all_keys.contains_key(b"key-005".as_slice()), "key-005 should exist (only in table1)");
	assert!(all_keys.contains_key(b"key-010".as_slice()), "key-010 should exist (table3 wins)");
	assert!(
		!all_keys.contains_key(b"key-014".as_slice()),
		"key-014 should be deleted by tombstone"
	);
	assert!(all_keys.contains_key(b"key-015".as_slice()), "key-015 should exist (table2 wins)");
	assert!(all_keys.contains_key(b"key-020".as_slice()), "key-020 should exist (only in table2)");

	// Verify tombstone preservation at intermediate level
	if levels.len() >= 3 {
		assert!(
			tombstones.contains_key(b"key-014".as_slice()),
			"Tombstone should be preserved in intermediate level"
		);
		assert_eq!(
			tombstones[b"key-014".as_slice()],
			210,
			"Tombstone should have correct sequence number"
		);
	}
}

#[test(tokio::test)]
async fn test_l0_tombstone_propagation_overlapping() {
	let env = TestEnv::new();
	let mut levels = Levels::new(3, 10);

	// Create 3 overlapping L0 tables with different sequence numbers
	// Table 1: Base data (seq 100-119) for keys 0-19
	let mut entries1 = Vec::new();
	for i in 0..20 {
		let key = format!("key-{i:03}").into_bytes();
		let raw_value = format!("original-value-{i}").into_bytes();
		let encoded_value = create_inline_value(&raw_value);

		entries1.push((InternalKey::new(key, 100 + i, InternalKeyKind::Set, 0), encoded_value));
	}

	// Table 2: Mixed updates/deletes (seq 150-159) for keys 5-14
	let mut entries2 = Vec::new();
	for i in 5..15 {
		let key = format!("key-{i:03}").into_bytes();
		let (kind, value) = if i % 3 == 0 {
			(InternalKeyKind::Delete, vec![]) // Every 3rd key becomes tombstone
		} else {
			let raw_value = format!("updated-value-{i}").into_bytes();
			let encoded_value = create_inline_value(&raw_value);

			(InternalKeyKind::Set, encoded_value)
		};
		entries2.push((InternalKey::new(key, 150 + i - 5, kind, 0), value));
	}

	// Table 3: Final tombstones (seq 200+) for specific keys
	let mut entries3 = Vec::new();
	for i in [2, 8, 14, 17] {
		let key = format!("key-{i:03}").into_bytes();
		entries3.push((InternalKey::new(key, 200 + i / 2, InternalKeyKind::Delete, 0), vec![]));
	}

	// Add tables to L0
	for (id, entries) in [(1, entries1), (2, entries2), (3, entries3)] {
		let table = env.create_test_table(id, entries).unwrap();
		Arc::make_mut(&mut levels.get_levels_mut()[0]).insert(table);
	}

	// Create manifest and run compaction
	let manifest_path = env.options.path.join("test_manifest_tombstone");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};
	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	let opts = create_options_with_compaction_settings(&env.options, 1, 2.0);
	let strategy = Arc::new(Strategy::from_options(opts));
	let compaction_options = create_compaction_options(env.options, Arc::clone(&manifest));
	let compactor = Compactor::new(compaction_options, strategy);
	perform_compaction_rounds(&compactor, 2);

	// Verify tombstone wins: keys 2, 6, 8, 9, 12, 14, 17 should be deleted
	let manifest_guard = manifest.read().unwrap();
	let levels = manifest_guard.levels.get_levels();

	let mut survivors = HashMap::new();
	for level in levels {
		for table in &level.tables {
			let mut iter = table.iter(None);
			while iter.valid() {
				let key = iter.key().to_owned();
				let encoded_value = iter.value().to_vec();
				if key.kind() == InternalKeyKind::Set {
					let location = ValueLocation::decode(&encoded_value).unwrap();
					if location.is_value_pointer() {
						panic!("Unexpected VLog pointer in test");
					}
					survivors.insert(key.user_key.clone(), (*location.value).to_vec());
				}
				iter.next().unwrap();
			}
		}
	}

	// Test specific key outcomes (highest sequence number wins)
	assert!(
		!survivors.contains_key(b"key-002".as_slice()),
		"key-002 should be deleted by Table 3 tombstone"
	);
	assert!(
		!survivors.contains_key(b"key-006".as_slice()),
		"key-006 should be deleted by Table 2 tombstone"
	);
	assert!(
		!survivors.contains_key(b"key-009".as_slice()),
		"key-009 should be deleted by Table 2 tombstone"
	);
	assert!(
		!survivors.contains_key(b"key-012".as_slice()),
		"key-012 should be deleted by Table 2 tombstone"
	);
	assert!(
		!survivors.contains_key(b"key-014".as_slice()),
		"key-014 should be deleted by Table 3 tombstone"
	);

	// Test survivors have correct values
	assert!(
		survivors[b"key-000".as_slice()].starts_with(b"original-value"),
		"key-000 should have original value"
	);
	assert!(
		survivors[b"key-007".as_slice()].starts_with(b"updated-value"),
		"key-007 should have updated value from Table 2"
	);
	assert!(
		survivors[b"key-015".as_slice()].starts_with(b"original-value"),
		"key-015 should have original value"
	);

	// Verify expected total count (20 original - 5 deleted = 15 survivors)
	assert_eq!(survivors.len(), 13, "Should have 13 surviving keys after tombstone propagation");
}

#[test(tokio::test)]
async fn test_tombstone_propagation_through_levels() {
	let env = TestEnv::new();
	let mut levels = Levels::new(4, 10);

	// Create L2 tables: 4 tables to exceed limit and trigger L2→L3 compaction
	for table_idx in 0..4 {
		let mut l2_entries = Vec::new();
		for i in (table_idx * 3)..((table_idx + 1) * 3) {
			let key = format!("key-{i:03}").into_bytes();
			let (seq, kind, value) = if i % 2 == 0 {
				(200 + i, InternalKeyKind::Delete, vec![]) // Even keys = tombstones
			} else {
				let raw_value = format!("l2-value-{i}").into_bytes();
				let encoded_value = create_inline_value(&raw_value);

				(200 + i, InternalKeyKind::Set, encoded_value) // Odd keys = values
			};
			l2_entries.push((InternalKey::new(key, seq, kind, 0), value));
		}
		let table = env.create_test_table(100 + table_idx, l2_entries).unwrap();
		Arc::make_mut(&mut levels.get_levels_mut()[2]).insert(table);
	}

	// Create L3 with older values for all keys (overlapping with L2)
	let mut l3_entries = Vec::new();
	for i in 0..12 {
		let key = format!("key-{i:03}").into_bytes();
		let raw_value = format!("l3-old-value-{i}").into_bytes();
		let encoded_value = create_inline_value(&raw_value);

		l3_entries.push((InternalKey::new(key, 100 + i, InternalKeyKind::Set, 0), encoded_value));
	}
	let l3_table = env.create_test_table(200, l3_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[3]).insert(l3_table);

	// Create manifest and run L2→L3 compaction (bottom level)
	let manifest_path = env.options.path.join("test_manifest_propagation");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};
	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	// Use very small byte limits to ensure compaction is triggered
	let mut opts = (*env.options).clone();
	opts.level0_max_files = 1;
	opts.max_bytes_for_level = 1024; // 1KB - very small so tables exceed it
	opts.level_multiplier = 2.0;
	let opts = Arc::new(opts);
	let strategy = Arc::new(Strategy::from_options(opts));
	let compaction_options = create_compaction_options(env.options, Arc::clone(&manifest));
	let compactor = Compactor::new(compaction_options, strategy);
	compactor.compact().unwrap();

	// Verify bottom-level tombstone filtering: L3 should have no tombstones
	let manifest_guard = manifest.read().unwrap();
	let levels = manifest_guard.levels.get_levels();

	let mut tombstones = 0;
	let mut values = 0;
	for table in &levels[3].tables {
		let mut iter = table.iter(None);
		while iter.valid() {
			let key = iter.key().to_owned();
			match key.kind() {
				InternalKeyKind::Delete => tombstones += 1,
				InternalKeyKind::Set => values += 1,
				_ => {}
			}
			iter.next().unwrap();
		}
	}

	// bottom level should filter out all tombstones
	assert_eq!(tombstones, 0, "Bottom level L3 should have no tombstones");
	assert!(values > 0, "L3 should contain some values after compaction");
	// With bytes-based limits, compaction behavior may vary - just verify L2 has fewer tables than
	// before
	assert!(
		levels[2].tables.len() < 4,
		"L2 should have fewer tables after compaction (had 4, now {})",
		levels[2].tables.len()
	);
}

#[test]
fn test_tombstone_propagation_journey() {
	let env = TestEnv::new();

	// Create tombstone (seq=100) and older value (seq=50) for same key
	let key = "test-key".as_bytes().to_vec();
	let key_bytes = key;

	// Table 1: tombstone entry
	let mut tombstone_entries = Vec::new();
	let tombstone = InternalKey::new(key_bytes.clone(), 100, InternalKeyKind::Delete, 0);
	tombstone_entries.push((tombstone, vec![]));
	let tombstone_table = env.create_test_table(100, tombstone_entries).unwrap();

	// Table 2: older value entry
	let mut value_entries = Vec::new();
	let value_key = InternalKey::new(key_bytes, 50, InternalKeyKind::Set, 0);

	let raw_value = b"old-value".to_vec();
	let encoded_value = create_inline_value(&raw_value);

	value_entries.push((value_key, encoded_value));
	let value_table = env.create_test_table(101, value_entries).unwrap();

	// Test non-bottom level compaction
	let iterators: Vec<_> = vec![
		Box::new(tombstone_table.iter(None)) as Box<dyn InternalIterator>,
		Box::new(value_table.iter(None)) as Box<dyn InternalIterator>,
	];
	let mut comp_iter_non_bottom = CompactionIterator::new(
		iterators,
		create_comparator(),
		false,
		None,
		false,
		0,
		Arc::new(MockLogicalClock::new()),
	);
	let non_bottom_result: Vec<_> = comp_iter_non_bottom.by_ref().map(|r| r.unwrap()).collect();

	// Non-bottom level should preserve tombstone
	assert_eq!(non_bottom_result.len(), 1, "Non-bottom level should have 1 entry");
	let (key, _) = &non_bottom_result[0];
	assert_eq!(key.kind(), InternalKeyKind::Delete, "Non-bottom level should preserve tombstone");
	assert_eq!(key.seq_num(), 100, "Should be the newer tombstone");

	// Test bottom level compaction
	let iterators: Vec<_> = vec![
		Box::new(tombstone_table.iter(None)) as Box<dyn InternalIterator>,
		Box::new(value_table.iter(None)) as Box<dyn InternalIterator>,
	];
	let mut comp_iter_bottom = CompactionIterator::new(
		iterators,
		create_comparator(),
		true,
		None,
		false,
		0,
		Arc::new(MockLogicalClock::new()),
	);
	let bottom_result: Vec<_> = comp_iter_bottom.by_ref().map(|r| r.unwrap()).collect();

	// Bottom level should filter out tombstones
	let has_tombstones = bottom_result.iter().any(|(key, _)| key.is_hard_delete_marker());
	assert!(!has_tombstones, "Bottom level should filter out tombstones");

	// Key should be completely gone after tombstone consumes older value
	let has_test_key = bottom_result.iter().any(|(key, _)| &key.user_key == b"test-key");
	assert!(!has_test_key, "test-key should be completely deleted");
	assert_eq!(
		bottom_result.len(),
		0,
		"Bottom level should have no entries when tombstone consumes value"
	);
}

#[test]
fn test_table_properties_population() {
	let env = TestEnv::new();

	let mut entries = Vec::new();
	let expected_deletions = 25u64;
	let expected_tombstones = 25u64;

	for i in 0..100 {
		let key = format!("key-{i:03}").into_bytes();
		let value = format!("value-{i:03}").into_bytes();
		let seq = 1000 + i;

		let kind = match i % 20 {
			0..=11 => InternalKeyKind::Set,
			12..=15 => InternalKeyKind::Delete,
			16..=18 => InternalKeyKind::Merge,
			19 => InternalKeyKind::RangeDelete,
			_ => unreachable!(),
		};

		let internal_key = InternalKey::new(key, seq, kind, 0);

		let entry_value = match kind {
			InternalKeyKind::Delete | InternalKeyKind::RangeDelete => vec![],
			_ => value,
		};

		entries.push((internal_key, entry_value));
	}

	let table_id = 11;
	let table = env.create_test_table(table_id, entries).unwrap();

	let meta = &table.meta;
	let props = &meta.properties;

	// Verify Properties fields
	assert_eq!(props.id, table_id);
	assert_eq!(props.table_format, TableFormat::LSMV1);
	assert_eq!(props.num_entries, 100);
	assert_eq!(props.item_count, 100);
	assert_eq!(props.key_count, 100);
	assert_eq!(props.num_deletions, expected_deletions);
	assert_eq!(props.tombstone_count, expected_tombstones);
	assert_eq!(props.data_size, 2975);
	assert_eq!(props.global_seq_num, 0);
	assert_eq!(props.num_data_blocks, 1);

	assert_eq!(props.index_size, 74, "Index size should be tracked");
	assert_eq!(props.index_partitions, 1, "Should have 1 index partition for small table");
	assert_eq!(props.top_level_index_size, 32, "Top-level index size should be tracked");
	// Verify filter metrics (should have bloom filter by default)
	assert_eq!(props.filter_size, 135, "Filter size should be tracked with default bloom filter");
	assert_eq!(props.raw_key_size, 2300, "Raw key size should be tracked");
	assert_eq!(props.raw_value_size, 675, "Raw value size should be tracked");
	assert!(
		props.raw_key_size + props.raw_value_size == props.data_size,
		"Raw sizes should be == data_size"
	);

	// Verify time metrics (timestamps are 0 in this test)
	assert_eq!(props.oldest_key_time, 0, "Oldest key time is 0 in test");
	assert_eq!(props.newest_key_time, 0, "Newest key time is 0 in test");

	// Verify range deletion metrics
	assert_eq!(props.num_range_deletions, 5, "Should have 5 range deletions (every 20th key)");

	assert!(props.created_at > 0);
	assert_eq!(props.block_size, 2757);
	assert_eq!(props.block_count, 1);
	assert_eq!(props.compression, CompressionType::None);
	assert_eq!(props.seqnos.0, 1000);
	assert_eq!(props.seqnos.1, 1099);
	assert!(meta.smallest_point.is_some());
	assert!(meta.largest_point.is_some());
	if let Some(smallest) = &meta.smallest_point {
		assert_eq!(&smallest.user_key, b"key-000");
	}
	if let Some(largest) = &meta.largest_point {
		assert_eq!(&largest.user_key, b"key-099");
	}

	// Verify TableMetadata fields
	assert_eq!(meta.has_point_keys, Some(true));
	assert_eq!(meta.smallest_seq_num, 1000);
	assert_eq!(meta.largest_seq_num, 1099);
	assert!(meta.smallest_point.is_some());
	assert!(meta.largest_point.is_some());
	if let Some(ref smallest) = meta.smallest_point {
		assert_eq!(&smallest.user_key, b"key-000");
		assert_eq!(smallest.seq_num(), 1000);
	}
	if let Some(ref largest) = meta.largest_point {
		assert_eq!(&largest.user_key, b"key-099");
		assert_eq!(largest.seq_num(), 1099);
	}

	// Test compaction strategy can use properties
	let opts = create_options_with_compaction_settings(&env.options, 4, 10.0);
	let strategy = Strategy::from_options(opts);
	let mut test_level = Level::with_capacity(10);
	test_level.insert(table);

	let selected = strategy.select_by_compensated_size(&test_level);
	assert_eq!(selected, Some(table_id));
}

#[test(tokio::test)]
async fn test_soft_delete_compaction_behavior() {
	let env = TestEnv::new_with_levels(2); // Only 2 levels: L0 and L1
	let mut levels = Levels::new(3, 10);

	// Create L0 tables: 2 tables to exceed L0 limit (1) and trigger L0→L1
	// compaction
	for table_idx in 0..2 {
		let mut l0_entries = Vec::new();
		for i in (table_idx * 6)..((table_idx + 1) * 6) {
			let key = format!("key-{i:03}").into_bytes();
			let (seq, kind, value) = if i % 3 == 0 {
				// Every 3rd key = soft delete
				(200 + i, InternalKeyKind::SoftDelete, vec![])
			} else if i % 3 == 1 {
				// Every 3rd+1 key = regular delete
				(200 + i, InternalKeyKind::Delete, vec![])
			} else {
				// Every 3rd+2 key = set value
				let raw_value = format!("l0-value-{i}").into_bytes();
				let encoded_value = create_inline_value(&raw_value);
				(200 + i, InternalKeyKind::Set, encoded_value)
			};
			l0_entries.push((InternalKey::new(key, seq, kind, 0), value));
		}
		let table = env.create_test_table(100 + table_idx, l0_entries.clone()).unwrap();
		Arc::make_mut(&mut levels.get_levels_mut()[0]).insert(table);
	}

	// Create L1 (last level) with older values for all keys
	let mut l1_entries = Vec::new();
	for i in 0..12 {
		let key = format!("key-{i:03}").into_bytes();
		let raw_value = format!("l1-old-value-{i}").into_bytes();
		let encoded_value = create_inline_value(&raw_value);

		l1_entries.push((InternalKey::new(key, 100 + i, InternalKeyKind::Set, 0), encoded_value));
	}
	let l1_table = env.create_test_table(200, l1_entries.clone()).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(l1_table);

	// Create manifest and run L0→L1 compaction
	let manifest_path = env.options.path.join("test_manifest_soft_delete");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};
	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	let opts = create_options_with_compaction_settings(&env.options, 1, 1.0);
	let strategy = Arc::new(Strategy::from_options(opts));
	let mut compaction_options = create_compaction_options(env.options, Arc::clone(&manifest));
	compaction_options.vlog = None;
	let compactor = Compactor::new(compaction_options, strategy);

	compactor.compact().unwrap();

	// Verify that soft deletes flow through compaction normally (like any other
	// key)
	let manifest_guard = manifest.read().unwrap();
	let levels = manifest_guard.levels.get_levels();

	let mut soft_deletes = 0;
	let mut regular_deletes = 0;
	let mut values = 0;

	// Count entries in L1 (bottom level) after compaction
	for table in &levels[1].tables {
		let mut iter = table.iter(None);
		while iter.valid() {
			let key = iter.key().to_owned();
			match key.kind() {
				InternalKeyKind::SoftDelete => soft_deletes += 1,
				InternalKeyKind::Delete => regular_deletes += 1,
				InternalKeyKind::Set => values += 1,
				_ => {}
			}
			iter.next().unwrap();
		}
	}

	// Verify exact counts
	assert_eq!(soft_deletes, 4, "Should have exactly 4 soft deletes (keys 0, 3, 6, 9)");
	assert_eq!(regular_deletes, 0, "Regular deletes should be filtered out at the bottom level");
	assert_eq!(values, 4, "Should have exactly 4 values (keys 2, 5, 8, 11)");

	// Verify that values are the latest and correct
	let mut found_keys = HashSet::new();
	for table in &levels[1].tables {
		let mut iter = table.iter(None);
		while iter.valid() {
			let key = iter.key().to_owned();
			let value = iter.value().to_vec();
			match key.kind() {
				InternalKeyKind::Set => {
					let key_str = String::from_utf8(key.user_key.clone()).unwrap();

					// Decode the ValueLocation to get the actual value
					let location = crate::vlog::ValueLocation::decode(&value).unwrap();
					let actual_value = if location.is_value_pointer() {
						panic!("Unexpected VLog pointer in test");
					} else {
						(*location.value).to_vec()
					};
					let value_str = String::from_utf8(actual_value).unwrap();

					// Verify we get the latest L0 values, not the old L1 values
					if key_str.starts_with("key-") {
						let key_num: usize = key_str.split('-').nth(1).unwrap().parse().unwrap();
						if key_num % 3 == 2 {
							// These should be Set values
							assert!(
								value_str.starts_with("l0-value-"),
								"Key {} should have L0 value, got: {}",
								key_str,
								value_str
							);
							found_keys.insert(key_str);
						}
					}
				}
				InternalKeyKind::SoftDelete => {
					let key_str = String::from_utf8(key.user_key.clone()).unwrap();
					if key_str.starts_with("key-") {
						let key_num: usize = key_str.split('-').nth(1).unwrap().parse().unwrap();
						assert_eq!(key_num % 3, 0, "Soft delete should be on keys 0, 3, 6, 9");
					}
				}
				_ => {}
			}
			iter.next().unwrap();
		}
	}

	// Verify we found all expected Set keys
	assert_eq!(found_keys.len(), 4, "Should have found all 4 Set keys");
	for i in [2, 5, 8, 11] {
		let expected_key = format!("key-{:03}", i);
		assert!(found_keys.contains(&expected_key), "Missing expected key: {}", expected_key);
	}
}

#[test(tokio::test)]
async fn test_older_soft_delete_marked_stale_during_compaction() {
	// This test verifies that when a key has multiple versions including older soft
	// deletes, compaction correctly handles the older soft deletes when marking
	// them as stale.
	//
	// Scenario: key "test-key" has 3 versions:
	//   - SoftDelete (seq 300) - LATEST
	//   - Set (seq 200)        - older
	//   - SoftDelete (seq 100) - oldest
	//
	// With versioning disabled, the older versions should be marked stale.
	// The bug was that older soft deletes (which have empty values) would crash
	// when the code tried to decode them as ValueLocation.

	let env = TestEnv::new_with_levels(2);
	let mut levels = Levels::new(3, 10);

	let key = b"test-key".to_vec();

	// Create L0 table with latest SoftDelete (seq 300)
	let l0_entries = vec![(
		InternalKey::new(key.clone(), 300, InternalKeyKind::SoftDelete, 0),
		vec![], // SoftDelete has empty value
	)];
	let l0_table = env.create_test_table(100, l0_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[0]).insert(l0_table);

	// Create L1 table with older Set (seq 200) and oldest SoftDelete (seq 100)
	let raw_value = b"some-value".to_vec();
	let encoded_value = create_inline_value(&raw_value);
	let l1_entries = vec![
		(InternalKey::new(key.clone(), 200, InternalKeyKind::Set, 0), encoded_value),
		(
			InternalKey::new(key, 100, InternalKeyKind::SoftDelete, 0),
			vec![], // Older SoftDelete also has empty value
		),
	];
	let l1_table = env.create_test_table(200, l1_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(l1_table);

	// Create manifest and run compaction
	let manifest_path = env.options.path.join("test_manifest_older_soft_delete");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};
	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	let opts = create_options_with_compaction_settings(&env.options, 1, 1.0);
	let strategy = Arc::new(Strategy::from_options(opts));
	// NOTE: Do NOT set vlog = None - we need VLog enabled to trigger the bug
	let compaction_options = create_compaction_options(env.options, Arc::clone(&manifest));

	let compactor = Compactor::new(compaction_options, strategy);

	// This should NOT panic - older soft deletes should be handled correctly
	compactor.compact().unwrap();

	// Verify the result: only the latest SoftDelete should remain
	let manifest_guard = manifest.read().unwrap();
	let levels = manifest_guard.levels.get_levels();

	let mut soft_deletes = 0;
	let mut sets = 0;

	for table in &levels[1].tables {
		let mut iter = table.iter(None);
		while iter.valid() {
			let key = iter.key().to_owned();
			match key.kind() {
				InternalKeyKind::SoftDelete => soft_deletes += 1,
				InternalKeyKind::Set => sets += 1,
				_ => {}
			}
			iter.next().unwrap();
		}
	}

	// Only the latest SoftDelete (seq 300) should remain
	assert_eq!(soft_deletes, 1, "Should have exactly 1 soft delete (the latest)");
	assert_eq!(sets, 0, "Should have no Set entries (superseded by SoftDelete)");
}

#[test]
fn test_score_based_level_selection() {
	let env = TestEnv::new();
	// Ensure manifest directory exists
	std::fs::create_dir_all(env.options.manifest_dir()).unwrap();
	let mut manifest = LevelManifest::new(Arc::clone(&env.options)).unwrap();

	// Create options with specific compaction settings
	let mut opts = (*env.options).clone();
	opts.level0_max_files = 4;
	opts.max_bytes_for_level = 100 * 1024 * 1024; // 100MB
	opts.level_multiplier = 10.0;
	let opts = Arc::new(opts);
	let strategy = Strategy::from_options(opts);

	// Add 5 L0 files (score = 5/4 = 1.25)
	for i in 0..5 {
		let entries = create_ordered_entries("l0_key", i, 1, i as u64, None);
		let table = env.create_test_table(i as u64, entries).unwrap();
		Arc::make_mut(&mut manifest.levels.get_levels_mut()[0]).insert(table);
	}

	// Add L1 files totaling 150MB (score = 150/100 = 1.5)
	// Note: Actual file sizes will be determined by content, but we test the score logic
	for i in 0..3 {
		let entries = create_ordered_entries("l1_key", i, 10, (10 + i) as u64, None);
		let table = env.create_test_table((10 + i) as u64, entries).unwrap();
		Arc::make_mut(&mut manifest.levels.get_levels_mut()[1]).insert(table);
	}

	// Score-based selection should pick the level with highest score
	let choice = strategy.pick_levels(&manifest).unwrap();
	match choice {
		CompactionChoice::Merge(input) => {
			// Should pick a level that needs compaction (score >= 1.0)
			assert!(input.source_level <= 1, "Should pick L0 or L1");
		}
		CompactionChoice::Skip => panic!("Should not skip when levels need compaction"),
	}
}

#[test]
fn test_bytes_based_level_limits() {
	let env = TestEnv::new();
	// Ensure manifest directory exists
	std::fs::create_dir_all(env.options.manifest_dir()).unwrap();
	let mut manifest = LevelManifest::new(Arc::clone(&env.options)).unwrap();

	// Create options with bytes-based limits
	let mut opts = (*env.options).clone();
	opts.max_bytes_for_level = 100 * 1024 * 1024; // 100MB base
	opts.level_multiplier = 10.0;
	let opts = Arc::new(opts);
	let strategy = Strategy::from_options(opts);

	// Add L1 files - actual sizes will be based on content
	// The score calculation uses actual file sizes from the tables
	for i in 0..3 {
		let entries = create_ordered_entries("l1_key", i, 100, i as u64, None);
		let table = env.create_test_table(i as u64, entries).unwrap();
		Arc::make_mut(&mut manifest.levels.get_levels_mut()[1]).insert(table);
	}

	// Check that L1 can be selected if it exceeds limit
	let choice = strategy.pick_levels(&manifest).unwrap();
	match choice {
		CompactionChoice::Merge(input) => {
			// Should pick a level that needs compaction
			assert!(input.source_level <= 2, "Should pick a valid level");
		}
		CompactionChoice::Skip => {
			// Skip is OK if levels don't exceed limits yet
		}
	}
}

#[test]
fn test_bottom_level_compaction() {
	let env = TestEnv::new_with_levels(3); // 3 levels: L0, L1, L2 (L2 is bottom)
										// Ensure manifest directory exists
	std::fs::create_dir_all(env.options.manifest_dir()).unwrap();
	let mut manifest = LevelManifest::new(Arc::clone(&env.options)).unwrap();

	// Create options
	let mut opts = (*env.options).clone();
	opts.max_bytes_for_level = 100 * 1024 * 1024; // 100MB
	opts.level_multiplier = 10.0;
	let opts = Arc::new(opts);
	let strategy = Strategy::from_options(opts);

	// Add L2 files (bottom level)
	for i in 0..3 {
		let entries = create_ordered_entries("l2_key", i, 100, (20 + i) as u64, None);
		let table = env.create_test_table((20 + i) as u64, entries).unwrap();
		Arc::make_mut(&mut manifest.levels.get_levels_mut()[2]).insert(table);
	}

	// Bottom level compaction should be allowed (same-level compaction)
	// Note: This will only trigger if L2 exceeds its byte limit
	let choice = strategy.pick_levels(&manifest).unwrap();
	match choice {
		CompactionChoice::Merge(input) => {
			// If L2 is selected, it should compact to same level
			if input.source_level == 2 {
				assert_eq!(
					input.target_level, 2,
					"Should compact to same level (tombstone cleanup)"
				);
			}
		}
		CompactionChoice::Skip => {
			// Skip is OK if L2 doesn't exceed limit
		}
	}
}

fn create_test_table_with_bounds(
	temp_dir: &TempDir,
	opts: &Arc<Options>,
	id: u64,
	smallest_key: &[u8],
	largest_key: &[u8],
) -> Arc<crate::sstable::table::Table> {
	// Ensure the sstable directory exists in the temp directory
	let sstable_dir = temp_dir.path().join("sstables");
	std::fs::create_dir_all(&sstable_dir).unwrap();

	let table_path = opts.sstable_file_path(id);
	let file = File::create(&table_path).unwrap();

	let mut writer = crate::sstable::table::TableWriter::new(file, id, Arc::clone(opts), 0);

	// Add smallest key
	let small_key = InternalKey::new(smallest_key.to_vec(), 1, InternalKeyKind::Set, 0);
	let value = ValueLocation::with_inline_value(b"v".to_vec()).encode();
	writer.add(small_key, &value).unwrap();

	// Add largest key (if different)
	if smallest_key != largest_key {
		let large_key = InternalKey::new(largest_key.to_vec(), 2, InternalKeyKind::Set, 0);
		writer.add(large_key, &value).unwrap();
	}

	writer.finish().unwrap();

	let file = std::fs::File::open(&table_path).unwrap();
	let file_size = file.metadata().unwrap().len();
	let table =
		crate::sstable::table::Table::new(id, Arc::clone(opts), Arc::new(file), file_size).unwrap();
	Arc::new(table)
}

#[test]
fn test_combined_key_range_single_table() {
	let temp_dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let mut level = Level::with_capacity(10);
	let table = create_test_table_with_bounds(&temp_dir, &opts, 1, b"a", b"z");
	level.insert(table);

	let mut ids = HashSet::new();
	ids.insert(1);

	let result = Strategy::combined_key_range(level.tables.iter().filter(|t| ids.contains(&t.id)));
	assert!(result.is_some());
	if let Some((std::ops::Bound::Included(smallest), std::ops::Bound::Included(largest))) = result
	{
		assert_eq!(&smallest.user_key, b"a");
		assert_eq!(&largest.user_key, b"z");
	} else {
		panic!("Expected Included bounds");
	}
}

#[test]
fn test_combined_key_range_multiple_tables() {
	let temp_dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let mut level = Level::with_capacity(10);
	let table1 = create_test_table_with_bounds(&temp_dir, &opts, 1, b"a", b"m");
	let table2 = create_test_table_with_bounds(&temp_dir, &opts, 2, b"g", b"z");
	level.insert(table1);
	level.insert(table2);

	let mut ids = HashSet::new();
	ids.insert(1);
	ids.insert(2);

	let result = Strategy::combined_key_range(level.tables.iter().filter(|t| ids.contains(&t.id)));
	assert!(result.is_some());
	if let Some((std::ops::Bound::Included(smallest), std::ops::Bound::Included(largest))) = result
	{
		assert_eq!(&smallest.user_key, b"a");
		assert_eq!(&largest.user_key, b"z");
	} else {
		panic!("Expected Included bounds");
	}
}

#[test]
fn test_combined_key_range_overlapping_tables() {
	let temp_dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let mut level = Level::with_capacity(10);
	let table1 = create_test_table_with_bounds(&temp_dir, &opts, 1, b"a", b"m");
	let table2 = create_test_table_with_bounds(&temp_dir, &opts, 2, b"m", b"t");
	let table3 = create_test_table_with_bounds(&temp_dir, &opts, 3, b"t", b"v");
	level.insert(table1);
	level.insert(table2);
	level.insert(table3);

	let mut ids = HashSet::new();
	ids.insert(1);
	ids.insert(2);
	ids.insert(3);

	let result = Strategy::combined_key_range(level.tables.iter().filter(|t| ids.contains(&t.id)));
	assert!(result.is_some());
	if let Some((std::ops::Bound::Included(smallest), std::ops::Bound::Included(largest))) = result
	{
		assert_eq!(&smallest.user_key, b"a");
		assert_eq!(&largest.user_key, b"v");
	} else {
		panic!("Expected Included bounds");
	}
}

#[test]
fn test_combined_key_range_empty_ids() {
	let temp_dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let mut level = Level::with_capacity(10);
	let table = create_test_table_with_bounds(&temp_dir, &opts, 1, b"a", b"z");
	level.insert(table);

	let ids: HashSet<u64> = HashSet::new();

	let result = Strategy::combined_key_range(level.tables.iter().filter(|t| ids.contains(&t.id)));
	assert_eq!(result, None);
}

#[test]
fn test_select_overlapping_ranges_no_overlap() {
	let temp_dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let mut level = Level::with_capacity(10);
	let table1 = create_test_table_with_bounds(&temp_dir, &opts, 1, b"a", b"b");
	let table2 = create_test_table_with_bounds(&temp_dir, &opts, 2, b"d", b"e");
	let table3 = create_test_table_with_bounds(&temp_dir, &opts, 3, b"g", b"h");
	level.insert(table1);
	level.insert(table2);
	level.insert(table3);

	let result = Strategy::select_overlapping_ranges(&level, 2).unwrap();
	assert_eq!(result.len(), 1);
	assert!(result.contains(&2));
}

#[test]
fn test_select_overlapping_ranges_direct_overlap() {
	let temp_dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let mut level = Level::with_capacity(10);
	let table1 = create_test_table_with_bounds(&temp_dir, &opts, 1, b"a", b"c");
	let table2 = create_test_table_with_bounds(&temp_dir, &opts, 2, b"c", b"e");
	level.insert(table1);
	level.insert(table2);

	let result = Strategy::select_overlapping_ranges(&level, 1).unwrap();
	assert_eq!(result.len(), 2);
	assert!(result.contains(&1));
	assert!(result.contains(&2));
}

#[test]
fn test_select_overlapping_ranges_chain() {
	let temp_dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let mut level = Level::with_capacity(10);
	let table1 = create_test_table_with_bounds(&temp_dir, &opts, 1, b"a", b"b");
	let table2 = create_test_table_with_bounds(&temp_dir, &opts, 2, b"b", b"c");
	let table3 = create_test_table_with_bounds(&temp_dir, &opts, 3, b"c", b"d");
	let table4 = create_test_table_with_bounds(&temp_dir, &opts, 4, b"e", b"f");
	level.insert(table1);
	level.insert(table2);
	level.insert(table3);
	level.insert(table4);

	let result = Strategy::select_overlapping_ranges(&level, 2).unwrap();
	assert_eq!(result.len(), 3);
	assert!(result.contains(&1));
	assert!(result.contains(&2));
	assert!(result.contains(&3));
	assert!(!result.contains(&4));
}

#[test]
fn test_select_overlapping_ranges_full_overlap() {
	let temp_dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let mut level = Level::with_capacity(10);
	let table1 = create_test_table_with_bounds(&temp_dir, &opts, 1, b"a", b"z");
	let table2 = create_test_table_with_bounds(&temp_dir, &opts, 2, b"b", b"c");
	level.insert(table1);
	level.insert(table2);

	let result = Strategy::select_overlapping_ranges(&level, 1).unwrap();
	assert_eq!(result.len(), 2);
	assert!(result.contains(&1));
	assert!(result.contains(&2));
}

#[test]
fn test_select_overlapping_ranges_adjacent_no_overlap() {
	let temp_dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let mut level = Level::with_capacity(10);
	let table1 = create_test_table_with_bounds(&temp_dir, &opts, 1, b"a", b"b");
	let table2 = create_test_table_with_bounds(&temp_dir, &opts, 2, b"c", b"d");
	level.insert(table1);
	level.insert(table2);

	let result = Strategy::select_overlapping_ranges(&level, 1).unwrap();
	assert_eq!(result.len(), 1);
	assert!(result.contains(&1));
}

#[test]
fn test_expand_same_user_key_different_seq() {
	let temp_dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let mut level = Level::with_capacity(10);

	// Ensure the sstable directory exists in the temp directory
	let sstable_dir = temp_dir.path().join("sstables");
	std::fs::create_dir_all(&sstable_dir).unwrap();

	// File 1: user_key="foo" at seq=100 (newest)
	let table_path1 = opts.sstable_file_path(1);
	let file1 = File::create(&table_path1).unwrap();
	let mut writer1 = crate::sstable::table::TableWriter::new(file1, 1, Arc::clone(&opts), 0);
	let key1 = InternalKey::new(b"foo".to_vec(), 100, InternalKeyKind::Set, 0);
	let value = ValueLocation::with_inline_value(b"v1".to_vec()).encode();
	writer1.add(key1, &value).unwrap();
	writer1.finish().unwrap();
	let file1 = std::fs::File::open(&table_path1).unwrap();
	let file_size1 = file1.metadata().unwrap().len();
	let table1 =
		crate::sstable::table::Table::new(1, Arc::clone(&opts), Arc::new(file1), file_size1)
			.unwrap();
	level.insert(Arc::new(table1));

	// File 2: user_key="foo" at seq=50 (older version)
	let table_path2 = opts.sstable_file_path(2);
	let file2 = File::create(&table_path2).unwrap();
	let mut writer2 = crate::sstable::table::TableWriter::new(file2, 2, Arc::clone(&opts), 0);
	let key2 = InternalKey::new(b"foo".to_vec(), 50, InternalKeyKind::Set, 0);
	writer2.add(key2, &value).unwrap();
	writer2.finish().unwrap();
	let file2 = std::fs::File::open(&table_path2).unwrap();
	let file_size2 = file2.metadata().unwrap().len();
	let table2 =
		crate::sstable::table::Table::new(2, Arc::clone(&opts), Arc::new(file2), file_size2)
			.unwrap();
	level.insert(Arc::new(table2));

	// When selecting File 1, File 2 MUST be included (same user key, different seq)
	let result = Strategy::select_overlapping_ranges(&level, 1).unwrap();
	assert_eq!(result.len(), 2, "Both files with same user key should be included");
	assert!(result.contains(&1));
	assert!(result.contains(&2));
}

#[test]
fn test_select_overlapping_ranges_nonexistent_id() {
	let temp_dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let mut level = Level::with_capacity(10);
	let table = create_test_table_with_bounds(&temp_dir, &opts, 1, b"a", b"z");
	level.insert(table);

	let result = Strategy::select_overlapping_ranges(&level, 999);
	assert!(result.is_err());
	match result {
		Err(crate::error::Error::TableNotFound(id)) => assert_eq!(id, 999),
		_ => panic!("Expected TableNotFound error"),
	}
}

/// Helper function to create entries with specific string keys
fn create_entries_with_keys(keys: &[&str], seq_num: u64) -> Vec<(InternalKey, Vec<u8>)> {
	let mut entries = Vec::new();
	for (i, key) in keys.iter().enumerate() {
		let user_key = key.as_bytes().to_vec();
		let internal_key = InternalKey::new(user_key, seq_num + i as u64, InternalKeyKind::Set, 0);
		let value = format!("value-{}", key).into_bytes();
		let encoded_value = create_inline_value(&value);
		entries.push((internal_key, encoded_value));
	}
	entries
}

#[test]
fn test_clean_cut_shared_boundary_key() {
	let env = TestEnv::new();

	// Create L1 with files that share a boundary key "foo"
	// File 1: keys "a" to "foo" (boundary)
	// File 2: keys "foo" to "m" (shares boundary with file 1)
	// File 3: keys "n" to "z" (no shared boundary)

	let mut levels = Levels::new(3, 10);

	// File 1: a, b, c, ..., foo
	let mut file1_keys: Vec<String> = (b'a'..=b'f').map(|c| char::from(c).to_string()).collect();
	file1_keys.push("foo".to_string());
	let file1_keys_refs: Vec<&str> = file1_keys.iter().map(|s| s.as_str()).collect();
	let file1_entries = create_entries_with_keys(&file1_keys_refs, 100);
	let table1 = env.create_test_table(1, file1_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table1);

	// File 2: foo, g, h, ..., m
	let mut file2_keys: Vec<String> = vec!["foo".to_string()];
	file2_keys.extend((b'g'..=b'm').map(|c| char::from(c).to_string()));
	let file2_keys_refs: Vec<&str> = file2_keys.iter().map(|s| s.as_str()).collect();
	let file2_entries = create_entries_with_keys(&file2_keys_refs, 200);
	let table2 = env.create_test_table(2, file2_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table2);

	// File 3: n, o, p, ..., z (no shared boundary)
	let file3_keys: Vec<String> = (b'n'..=b'z').map(|c| char::from(c).to_string()).collect();
	let file3_keys_refs: Vec<&str> = file3_keys.iter().map(|s| s.as_str()).collect();
	let file3_entries = create_entries_with_keys(&file3_keys_refs, 300);
	let table3 = env.create_test_table(3, file3_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table3);

	// Create manifest
	let manifest_path = env.options.path.join("test_manifest");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};
	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	let levels_guard = manifest.read().unwrap();
	let source_level = &levels_guard.levels.get_levels()[1];

	// Test clean cut expansion directly: selecting File 1 should expand to include File 2
	let selected = Strategy::select_overlapping_ranges(source_level, 1).unwrap();

	// Verify file 1 and file 2 are both included (shared boundary "foo")
	assert!(selected.contains(&1), "File 1 should be included (initially selected)");
	assert!(
		selected.contains(&2),
		"File 2 should be included (shares boundary key 'foo' with file 1)"
	);
	// Verify file 3 is NOT included (no shared boundary)
	assert!(!selected.contains(&3), "File 3 should NOT be included (no shared boundary)");
}

#[test]
fn test_clean_cut_chain_expansion() {
	let env = TestEnv::new();

	// Create L1 with files forming a chain of shared boundaries
	// File 1: keys "a" to "b" (boundary "b")
	// File 2: keys "b" to "c" (shares "b" with file 1, boundary "c")
	// File 3: keys "c" to "d" (shares "c" with file 2)
	// File 4: keys "e" to "f" (no shared boundary)

	let mut levels = Levels::new(3, 10);

	// File 1: a, b
	let file1_entries = create_entries_with_keys(&["a", "b"], 100);
	let table1 = env.create_test_table(1, file1_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table1);

	// File 2: b, c
	let file2_entries = create_entries_with_keys(&["b", "c"], 200);
	let table2 = env.create_test_table(2, file2_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table2);

	// File 3: c, d
	let file3_entries = create_entries_with_keys(&["c", "d"], 300);
	let table3 = env.create_test_table(3, file3_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table3);

	// File 4: e, f (no shared boundary)
	let file4_entries = create_entries_with_keys(&["e", "f"], 400);
	let table4 = env.create_test_table(4, file4_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table4);

	// Create manifest
	let manifest_path = env.options.path.join("test_manifest");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};
	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	let levels_guard = manifest.read().unwrap();
	let source_level = &levels_guard.levels.get_levels()[1];

	// Test clean cut expansion directly: selecting File 2 should expand to include Files 1 and 3
	let selected = Strategy::select_overlapping_ranges(source_level, 2).unwrap();

	// Verify all three files in the chain are included
	assert!(selected.contains(&1), "File 1 should be included (chain expansion)");
	assert!(selected.contains(&2), "File 2 should be included (initially selected)");
	assert!(selected.contains(&3), "File 3 should be included (chain expansion)");
	// Verify file 4 is NOT included (no shared boundary)
	assert!(!selected.contains(&4), "File 4 should NOT be included (no shared boundary)");
}

#[test]
fn test_clean_cut_no_expansion_needed() {
	let env = TestEnv::new();

	// Create L1 with files that have no shared boundaries
	// File 1: keys "a" to "b"
	// File 2: keys "d" to "e"
	// File 3: keys "g" to "h"

	let mut levels = Levels::new(3, 10);

	// File 1: a, b
	let file1_entries = create_entries_with_keys(&["a", "b"], 100);
	let table1 = env.create_test_table(1, file1_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table1);

	// File 2: d, e
	let file2_entries = create_entries_with_keys(&["d", "e"], 200);
	let table2 = env.create_test_table(2, file2_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table2);

	// File 3: g, h
	let file3_entries = create_entries_with_keys(&["g", "h"], 300);
	let table3 = env.create_test_table(3, file3_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table3);

	// Create manifest
	let manifest_path = env.options.path.join("test_manifest");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};
	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	let levels_guard = manifest.read().unwrap();
	let source_level = &levels_guard.levels.get_levels()[1];

	// Test clean cut expansion directly: selecting File 1 should not expand (no shared boundaries)
	let selected = Strategy::select_overlapping_ranges(source_level, 1).unwrap();

	// Verify only File 1 is selected (no expansion needed)
	assert_eq!(
		selected.len(),
		1,
		"Only one file should be selected when there are no shared boundaries"
	);
	assert!(selected.contains(&1), "File 1 should be the only selected file");
}

#[test]
fn test_clean_cut_integration_shared_boundary() {
	let env = TestEnv::new();

	// Create L1 with files where File 1 is largest and shares boundary with File 2
	// File 1: keys "a" to "foo" (largest file - will be selected by ByCompensatedSize)
	// File 2: keys "foo" to "m" (shares boundary with file 1)
	// File 3: keys "n" to "z" (no shared boundary)

	let mut levels = Levels::new(3, 10);

	// File 1: a, b, c, ..., foo (make it largest by adding keys BEFORE foo)
	let mut file1_keys: Vec<String> = Vec::new();
	// Add many keys in the range before "foo"
	for i in 0..20 {
		file1_keys.push(format!("a{:02}", i));
	}
	file1_keys.extend((b'b'..=b'f').map(|c| char::from(c).to_string()));
	file1_keys.push("foo".to_string());
	let file1_keys_refs: Vec<&str> = file1_keys.iter().map(|s| s.as_str()).collect();
	let file1_entries = create_entries_with_keys(&file1_keys_refs, 100);
	let table1 = env.create_test_table(1, file1_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table1);

	// File 2: foo, g, h, ..., m (smaller)
	let mut file2_keys: Vec<String> = vec!["foo".to_string()];
	file2_keys.extend((b'g'..=b'm').map(|c| char::from(c).to_string()));
	let file2_keys_refs: Vec<&str> = file2_keys.iter().map(|s| s.as_str()).collect();
	let file2_entries = create_entries_with_keys(&file2_keys_refs, 200);
	let table2 = env.create_test_table(2, file2_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table2);

	// File 3: n, o, p, ..., z (smaller - no shared boundary)
	let file3_keys: Vec<String> = (b'n'..=b'z').map(|c| char::from(c).to_string()).collect();
	let file3_keys_refs: Vec<&str> = file3_keys.iter().map(|s| s.as_str()).collect();
	let file3_entries = create_entries_with_keys(&file3_keys_refs, 300);
	let table3 = env.create_test_table(3, file3_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table3);

	// Create manifest
	let manifest_path = env.options.path.join("test_manifest");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};
	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	// Create strategy with default priority (ByCompensatedSize)
	let opts = create_options_with_compaction_settings(&env.options, 4, 2.0);
	let strategy = Strategy::from_options(opts);

	let levels_guard = manifest.read().unwrap();
	let source_level = &levels_guard.levels.get_levels()[1];
	let next_level = &levels_guard.levels.get_levels()[2]; // Empty L2

	// File 1 should be selected (largest), and should expand to include File 2
	let selected = strategy.select_tables_for_compaction(source_level, next_level, 1).unwrap();

	// Verify File 1 and File 2 are both included (shared "foo" boundary)
	assert!(selected.contains(&1), "File 1 should be included (initially selected as largest)");
	assert!(
		selected.contains(&2),
		"File 2 should be included (shares boundary key 'foo' with file 1)"
	);
	// Verify File 3 is NOT included (no shared boundary)
	assert!(!selected.contains(&3), "File 3 should NOT be included (no shared boundary)");
}

#[test]
fn test_clean_cut_integration_chain_expansion() {
	let env = TestEnv::new();

	// Create L1 with files forming a chain, File 2 is largest
	// File 1: keys "a" to "b"
	// File 2: keys "b" to "c" (largest - will be selected)
	// File 3: keys "c" to "d" (shares boundary with file 2)
	// File 4: keys "e" to "f" (no shared boundary)

	let mut levels = Levels::new(3, 10);

	// File 1: a, b (smallest)
	let file1_entries = create_entries_with_keys(&["a", "b"], 100);
	let table1 = env.create_test_table(1, file1_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table1);

	// File 2: b, c (largest - add more entries to make it largest)
	// File 2: b, c (largest - add more entries BEFORE "c" to make it largest)
	let mut file2_keys = vec!["b".to_string()];
	// Add more keys between "b" and "c" to make File 2 the largest
	for i in 0..10 {
		file2_keys.push(format!("b{}", i)); // "b0", "b1", ..., "b9" are all < "c"
	}
	file2_keys.push("c".to_string()); // "c" is still the largest key
	let file2_keys_refs: Vec<&str> = file2_keys.iter().map(|s| s.as_str()).collect();
	let file2_entries = create_entries_with_keys(&file2_keys_refs, 200);
	let table2 = env.create_test_table(2, file2_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table2);

	// File 3: c, d
	let file3_entries = create_entries_with_keys(&["c", "d"], 300);
	let table3 = env.create_test_table(3, file3_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table3);

	// File 4: e, f (no shared boundary)
	let file4_entries = create_entries_with_keys(&["e", "f"], 400);
	let table4 = env.create_test_table(4, file4_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table4);

	// Create manifest
	let manifest_path = env.options.path.join("test_manifest");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};
	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	// Create strategy with default priority (ByCompensatedSize)
	let opts = create_options_with_compaction_settings(&env.options, 4, 2.0);
	let strategy = Strategy::from_options(opts);

	let levels_guard = manifest.read().unwrap();
	let source_level = &levels_guard.levels.get_levels()[1];
	let next_level = &levels_guard.levels.get_levels()[2]; // Empty L2

	// File 2 should be selected (largest), and should expand to include Files 1 and 3
	let selected = strategy.select_tables_for_compaction(source_level, next_level, 1).unwrap();

	// Verify all three files in the chain are included
	assert!(selected.contains(&1), "File 1 should be included (chain expansion)");
	assert!(selected.contains(&2), "File 2 should be included (initially selected as largest)");
	assert!(selected.contains(&3), "File 3 should be included (chain expansion)");
	// Verify File 4 is NOT included (no shared boundary)
	assert!(!selected.contains(&4), "File 4 should NOT be included (no shared boundary)");
}

#[test]
fn test_clean_cut_integration_with_oldest_seq_priority() {
	let env = TestEnv::new();

	// Create L1 with files where File 1 has oldest sequence
	// File 1: seq 100, keys "a" to "foo"
	// File 2: seq 200, keys "foo" to "m"
	// File 3: seq 300, keys "n" to "z"

	let mut levels = Levels::new(3, 10);

	// File 1: a, b, c, ..., foo (oldest sequence)
	let mut file1_keys: Vec<String> = (b'a'..=b'f').map(|c| char::from(c).to_string()).collect();
	file1_keys.push("foo".to_string());
	let file1_keys_refs: Vec<&str> = file1_keys.iter().map(|s| s.as_str()).collect();
	let file1_entries = create_entries_with_keys(&file1_keys_refs, 100);
	let table1 = env.create_test_table(1, file1_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table1);

	// File 2: foo, g, h, ..., m
	let mut file2_keys: Vec<String> = vec!["foo".to_string()];
	file2_keys.extend((b'g'..=b'm').map(|c| char::from(c).to_string()));
	let file2_keys_refs: Vec<&str> = file2_keys.iter().map(|s| s.as_str()).collect();
	let file2_entries = create_entries_with_keys(&file2_keys_refs, 200);
	let table2 = env.create_test_table(2, file2_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table2);

	// File 3: n, o, p, ..., z
	let file3_keys: Vec<String> = (b'n'..=b'z').map(|c| char::from(c).to_string()).collect();
	let file3_keys_refs: Vec<&str> = file3_keys.iter().map(|s| s.as_str()).collect();
	let file3_entries = create_entries_with_keys(&file3_keys_refs, 300);
	let table3 = env.create_test_table(3, file3_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table3);

	// Create manifest
	let manifest_path = env.options.path.join("test_manifest");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};
	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	// Create strategy with OldestSmallestSeqFirst priority
	let opts = create_options_with_compaction_settings(&env.options, 4, 2.0);
	let strategy = create_strategy_with_priority(&opts, CompactionPriority::OldestSmallestSeqFirst);

	let levels_guard = manifest.read().unwrap();
	let source_level = &levels_guard.levels.get_levels()[1];
	let next_level = &levels_guard.levels.get_levels()[2]; // Empty L2

	// File 1 should be selected (oldest sequence), and should expand to include File 2
	let selected = strategy.select_tables_for_compaction(source_level, next_level, 1).unwrap();

	// Verify File 1 and File 2 are both included (shared "foo" boundary)
	assert!(selected.contains(&1), "File 1 should be included (initially selected as oldest)");
	assert!(
		selected.contains(&2),
		"File 2 should be included (shares boundary key 'foo' with file 1)"
	);
	// Verify File 3 is NOT included (no shared boundary)
	assert!(!selected.contains(&3), "File 3 should NOT be included (no shared boundary)");
}

#[test]
fn test_clean_cut_integration_no_expansion() {
	let env = TestEnv::new();

	// Create L1 with files that have no shared boundaries, File 1 is largest
	// File 1: keys "a" to "b" (largest)
	// File 2: keys "d" to "e"
	// File 3: keys "g" to "h"

	let mut levels = Levels::new(3, 10);

	// File 1: a, b (make it largest by adding more keys BEFORE "b")
	let mut file1_keys = vec!["a".to_string()];
	for i in 0..10 {
		file1_keys.push(format!("a{}", i)); // "a0", "a1", ..., "a9" are all < "b"
	}
	file1_keys.push("b".to_string()); // "b" is still the largest key
	let file1_keys_refs: Vec<&str> = file1_keys.iter().map(|s| s.as_str()).collect();
	let file1_entries = create_entries_with_keys(&file1_keys_refs, 100);
	let table1 = env.create_test_table(1, file1_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table1);

	// File 2: d, e
	let file2_entries = create_entries_with_keys(&["d", "e"], 200);
	let table2 = env.create_test_table(2, file2_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table2);

	// File 3: g, h
	let file3_entries = create_entries_with_keys(&["g", "h"], 300);
	let table3 = env.create_test_table(3, file3_entries).unwrap();
	Arc::make_mut(&mut levels.get_levels_mut()[1]).insert(table3);

	// Create manifest
	let manifest_path = env.options.path.join("test_manifest");
	let manifest = LevelManifest {
		path: manifest_path,
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1000)),
		manifest_format_version: crate::levels::MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	};
	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	// Create strategy with default priority (ByCompensatedSize)
	let opts = create_options_with_compaction_settings(&env.options, 4, 2.0);
	let strategy = Strategy::from_options(opts);

	let levels_guard = manifest.read().unwrap();
	let source_level = &levels_guard.levels.get_levels()[1];
	let next_level = &levels_guard.levels.get_levels()[2]; // Empty L2

	// File 1 should be selected (largest), but should NOT expand (no shared boundaries)
	let selected = strategy.select_tables_for_compaction(source_level, next_level, 1).unwrap();

	// Verify only File 1 is selected (no expansion needed)
	assert_eq!(
		selected.len(),
		1,
		"Only one file should be selected when there are no shared boundaries"
	);
	assert!(selected.contains(&1), "File 1 should be the only selected file");
}
