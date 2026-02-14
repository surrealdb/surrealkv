//! Tests for table range overlap and level table selection logic
//!
//! Add this to src/test/level_range_tests.rs and include in src/test/mod.rs

use std::collections::HashSet;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use crate::levels::{Level, LevelManifest, Levels, MANIFEST_FORMAT_VERSION_V1};
use crate::sstable::table::{Table, TableWriter};
use crate::{
	InternalKey,
	InternalKeyKind,
	InternalKeyRange,
	Options,
	INTERNAL_KEY_SEQ_NUM_MAX,
	INTERNAL_KEY_TIMESTAMP_MAX,
};

/// Helper to create an InternalKeyRange
fn make_range(
	lower: Option<(&[u8], bool)>, // (key, inclusive)
	upper: Option<(&[u8], bool)>, // (key, inclusive)
) -> InternalKeyRange {
	let start = match lower {
		None => Bound::Unbounded,
		Some((k, true)) => Bound::Included(InternalKey::new(
			k.to_vec(),
			INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Max,
			INTERNAL_KEY_TIMESTAMP_MAX,
		)),
		Some((k, false)) => {
			Bound::Excluded(InternalKey::new(k.to_vec(), 0, InternalKeyKind::Set, 0))
		}
	};
	let end = match upper {
		None => Bound::Unbounded,
		Some((k, true)) => {
			Bound::Included(InternalKey::new(k.to_vec(), 0, InternalKeyKind::Set, 0))
		}
		Some((k, false)) => Bound::Excluded(InternalKey::new(
			k.to_vec(),
			INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Max,
			INTERNAL_KEY_TIMESTAMP_MAX,
		)),
	};
	(start, end)
}

/// Creates a table with specific key range for testing
fn create_test_table(id: u64, keys: &[&str], opts: Arc<Options>) -> Arc<Table> {
	let mut buf = Vec::new();
	let mut writer = TableWriter::new(&mut buf, id, Arc::clone(&opts), 0);

	for (i, key) in keys.iter().enumerate() {
		let ikey =
			InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);
		writer.add(ikey, b"value").unwrap();
	}

	let size = writer.finish().unwrap();

	let file: Arc<dyn crate::vfs::File> = Arc::new(buf);
	Arc::new(Table::new(id, opts, file, size as u64).unwrap())
}

/// Creates a level with multiple non-overlapping tables for testing
fn create_test_level(table_ranges: &[(&str, &str)], opts: Arc<Options>) -> Level {
	let mut tables = Vec::new();

	for (id, (smallest, largest)) in table_ranges.iter().enumerate() {
		// Create table with just the boundary keys
		let table = create_test_table((id + 1) as u64, &[smallest, largest], Arc::clone(&opts));
		tables.push(table);
	}

	Level {
		tables,
	}
}

// ============================================================================
// TESTS FOR is_before_range
// ============================================================================

mod is_before_range_tests {
	use super::*;

	#[test]
	fn table_clearly_before_range() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["a", "c"], opts);

		// Table [a,c], Range [e,g] - clearly before
		let range = make_range(Some((b"e", true)), Some((b"g", true)));
		assert!(table.is_before_range(&range));
	}

	#[test]
	fn table_clearly_not_before_range() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["e", "g"], opts);

		// Table [e,g], Range [a,c] - table is after, not before
		let range = make_range(Some((b"a", true)), Some((b"c", true)));
		assert!(!table.is_before_range(&range));
	}

	#[test]
	fn table_touches_range_at_boundary_included() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["a", "c"], opts);

		// Table [a,c], Range [c,e] (inclusive) - they touch at 'c'
		// Table should NOT be before range (they overlap at boundary)
		let range = make_range(Some((b"c", true)), Some((b"e", true)));
		assert!(
			!table.is_before_range(&range),
			"Table [a,c] touches [c,e] at 'c' (inclusive), should NOT be before"
		);
	}

	#[test]
	fn table_at_boundary_excluded() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["a", "c"], opts);

		// Table [a,c], Range (c,e] (exclusive lower) - 'c' is excluded from range
		// Table's largest is 'c', but range excludes 'c'
		// So table IS completely before the range
		let range = make_range(Some((b"c", false)), Some((b"e", true)));

		assert!(
			table.is_before_range(&range),
			"Table [a,c] should be before (c,e] since 'c' is excluded from range"
		);
	}

	#[test]
	fn table_overlaps_with_range() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["a", "e"], opts);

		// Table [a,e], Range [c,g] - overlapping
		let range = make_range(Some((b"c", true)), Some((b"g", true)));
		assert!(!table.is_before_range(&range));
	}

	#[test]
	fn unbounded_lower_range() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["a", "c"], opts);

		// Range [*, e] - unbounded lower means table is never "before"
		let range = make_range(None, Some((b"e", true)));
		assert!(!table.is_before_range(&range));
	}
}

// ============================================================================
// TESTS FOR is_after_range
// ============================================================================

mod is_after_range_tests {
	use super::*;

	#[test]
	fn table_clearly_after_range() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["e", "g"], opts);

		// Table [e,g], Range [a,c] - clearly after
		let range = make_range(Some((b"a", true)), Some((b"c", true)));
		assert!(table.is_after_range(&range));
	}

	#[test]
	fn table_clearly_not_after_range() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["a", "c"], opts);

		// Table [a,c], Range [e,g] - table is before, not after
		let range = make_range(Some((b"e", true)), Some((b"g", true)));
		assert!(!table.is_after_range(&range));
	}

	#[test]
	fn table_touches_range_at_boundary_included() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["c", "e"], opts);

		// Table [c,e], Range [a,c] (inclusive) - they touch at 'c'
		// Table should NOT be after range (they overlap at boundary)
		let range = make_range(Some((b"a", true)), Some((b"c", true)));
		assert!(
			!table.is_after_range(&range),
			"Table [c,e] touches [a,c] at 'c' (inclusive), should NOT be after"
		);
	}

	#[test]
	fn table_at_boundary_excluded() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["c", "e"], opts);

		// Table [c,e], Range [a,c) (exclusive upper) - 'c' is excluded from range
		// Table's smallest is 'c', but range excludes 'c'
		// So table IS completely after the range
		let range = make_range(Some((b"a", true)), Some((b"c", false)));

		assert!(
			table.is_after_range(&range),
			"Table [c,e] should be after [a,c) since 'c' is excluded from range"
		);
	}

	#[test]
	fn unbounded_upper_range() {
		let opts = Arc::new(Options::default());
		let table = create_test_table(1, &["e", "g"], opts);

		// Range [a, *] - unbounded upper means table is never "after"
		let range = make_range(Some((b"a", true)), None);
		assert!(!table.is_after_range(&range));
	}
}

// ============================================================================
// TESTS FOR find_first_overlapping_table and find_last_overlapping_table
// ============================================================================

#[test]
fn select_middle_tables() {
	let opts = Arc::new(Options::default());

	// Create 5 non-overlapping tables:
	// T1: [aa, ac], T2: [ba, bc], T3: [ca, cc], T4: [da, dc], T5: [ea, ec]
	let level = create_test_level(
		&[("aa", "ac"), ("ba", "bc"), ("ca", "cc"), ("da", "dc"), ("ea", "ec")],
		opts,
	);

	// Query [bb, cb] - should select T2 and T3
	let range = make_range(Some((b"bb", true)), Some((b"cb", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<u64> = level.tables[start..end].iter().map(|t| t.id).collect();

	assert_eq!(selected_ids, vec![2, 3], "Query [bb,cb] should select T2 and T3");
}

#[test]
fn select_first_table_only() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts);

	// Query [aa, ab] - should select only T1
	let range = make_range(Some((b"aa", true)), Some((b"ab", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<u64> = level.tables[start..end].iter().map(|t| t.id).collect();

	assert_eq!(selected_ids, vec![1]);
}

#[test]
fn select_last_table_only() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts);

	// Query [cb, cd] - should select only T3
	let range = make_range(Some((b"cb", true)), Some((b"cd", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<u64> = level.tables[start..end].iter().map(|t| t.id).collect();

	assert_eq!(selected_ids, vec![3]);
}

#[test]
fn select_all_tables() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts);

	// Query [aa, cc] - should select all tables
	let range = make_range(Some((b"aa", true)), Some((b"cc", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<u64> = level.tables[start..end].iter().map(|t| t.id).collect();

	assert_eq!(selected_ids, vec![1, 2, 3]);
}

#[test]
fn select_no_tables_before_all() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("ba", "bc"), ("ca", "cc"), ("da", "dc")], opts);

	// Query [aa, az] - before all tables
	let range = make_range(Some((b"aa", true)), Some((b"az", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	assert_eq!(start, end, "No tables should be selected");
	assert_eq!(start, 0, "Should point to beginning");
}

#[test]
fn select_no_tables_after_all() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts);

	// Query [da, dz] - after all tables
	let range = make_range(Some((b"da", true)), Some((b"dz", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	assert_eq!(start, end);
	assert_eq!(start, 3, "Should point past end");
}

#[test]
fn select_no_tables_in_gap() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(
		&[("aa", "ac"), ("da", "dc")], // Gap between ac and da
		opts,
	);

	// Query [ba, ca] - in the gap
	let range = make_range(Some((b"ba", true)), Some((b"ca", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	assert_eq!(start, end, "No tables in the gap");
}

#[test]
fn select_with_boundary_touching_included() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc")], opts);

	// Query [ac, ba] - touches both table boundaries
	let range = make_range(Some((b"ac", true)), Some((b"ba", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<u64> = level.tables[start..end].iter().map(|t| t.id).collect();

	// Both tables should be selected (T1 has 'ac', T2 has 'ba')
	assert_eq!(selected_ids, vec![1, 2], "Both tables touch the range boundaries");
}

#[test]
fn select_with_exclusive_bounds_at_boundaries() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc")], opts);

	// Query (ac, ba) - exclusive at both boundaries
	// This is keys STRICTLY BETWEEN 'ac' and 'ba'
	// T1's largest is 'ac' (excluded), T2's smallest is 'ba' (excluded)
	// Neither table should be selected!
	let range = make_range(Some((b"ac", false)), Some((b"ba", false)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	assert_eq!(start, end, "No tables should be selected for exclusive range (ac, ba)");
}

#[test]
fn select_with_exclusive_lower_bound() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts);

	// Query (ac, cc] - exclusive lower at T1's largest
	// T1 should NOT be selected, T2 and T3 should be selected
	let range = make_range(Some((b"ac", false)), Some((b"cc", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<u64> = level.tables[start..end].iter().map(|t| t.id).collect();

	assert_eq!(selected_ids, vec![2, 3], "T1 should be excluded for range (ac, cc]");
}

#[test]
fn select_with_exclusive_upper_bound() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts);

	// Query [aa, ca) - exclusive upper at T3's smallest
	// T3 should NOT be selected, T1 and T2 should be selected
	let range = make_range(Some((b"aa", true)), Some((b"ca", false)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<u64> = level.tables[start..end].iter().map(|t| t.id).collect();

	assert_eq!(selected_ids, vec![1, 2], "T3 should be excluded for range [aa, ca)");
}

#[test]
fn unbounded_ranges() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts);

	// Query [*, bc] - unbounded lower
	let range = make_range(None, Some((b"bc", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);
	let selected: Vec<u64> = level.tables[start..end].iter().map(|t| t.id).collect();
	assert_eq!(selected, vec![1, 2]);

	// Query [bc, *] - unbounded upper
	let range = make_range(Some((b"bc", true)), None);
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);
	let selected: Vec<u64> = level.tables[start..end].iter().map(|t| t.id).collect();
	assert_eq!(selected, vec![2, 3]);

	// Query [*, *] - fully unbounded
	let range = make_range(None, None);
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);
	let selected: Vec<u64> = level.tables[start..end].iter().map(|t| t.id).collect();
	assert_eq!(selected, vec![1, 2, 3]);
}

#[test]
fn single_point_query() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("aa", "ac"), ("ba", "bc"), ("ca", "cc")], opts);

	// Query exactly for key 'bb' → [bb, bb]
	let range = make_range(Some((b"bb", true)), Some((b"bb", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	let selected_ids: Vec<u64> = level.tables[start..end].iter().map(|t| t.id).collect();

	assert_eq!(selected_ids, vec![2], "Only T2 contains 'bb'");
}

#[test]
fn empty_level() {
	let level = Level {
		tables: vec![],
	};

	let range = make_range(Some((b"aa", true)), Some((b"zz", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);

	assert_eq!(start, 0);
	assert_eq!(end, 0);
}

#[test]
fn single_table_level() {
	let opts = Arc::new(Options::default());
	let level = create_test_level(&[("ba", "bc")], opts);

	// Query overlapping
	let range = make_range(Some((b"bb", true)), Some((b"cc", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);
	assert_eq!(level.tables[start..end].len(), 1);

	// Query before
	let range = make_range(Some((b"aa", true)), Some((b"az", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);
	assert_eq!(start, end);

	// Query after
	let range = make_range(Some((b"ca", true)), Some((b"cz", true)));
	let start = level.find_first_overlapping_table(&range);
	let end = level.find_last_overlapping_table(&range);
	assert_eq!(start, end);
}

// ============================================================================
// TESTS FOR min_oldest_vlog_file_id
// ============================================================================

/// Creates a test table with a specific oldest_vlog_file_id
fn create_test_table_with_vlog_id(
	id: u64,
	keys: &[&str],
	opts: Arc<Options>,
	vlog_file_id: u64,
) -> Arc<Table> {
	let mut buf = Vec::new();
	let mut writer = TableWriter::new(&mut buf, id, Arc::clone(&opts), 0);

	for (i, key) in keys.iter().enumerate() {
		let ikey =
			InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);
		writer.add(ikey, b"value").unwrap();
	}

	let size = writer.finish().unwrap();

	let file: Arc<dyn crate::vfs::File> = Arc::new(buf);
	let mut table = Table::new(id, opts, file, size as u64).unwrap();
	table.meta.properties.oldest_vlog_file_id = vlog_file_id;
	Arc::new(table)
}

/// Creates a LevelManifest from a list of levels without disk I/O
fn create_test_manifest(levels: Vec<Level>) -> LevelManifest {
	let levels = Levels(levels.into_iter().map(Arc::new).collect());
	LevelManifest {
		path: PathBuf::new(),
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(1)),
		manifest_format_version: MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 0,
	}
}

mod min_oldest_vlog_file_id_tests {
	use super::*;

	#[test]
	fn empty_manifest_returns_zero() {
		let manifest = create_test_manifest(vec![Level::default()]);
		assert_eq!(manifest.min_oldest_vlog_file_id(), 0);
	}

	#[test]
	fn all_tables_have_zero_vlog_id() {
		let opts = Arc::new(Options::default());
		// Tables with default vlog_file_id = 0 (no vlog references)
		let level = Level {
			tables: vec![
				create_test_table_with_vlog_id(1, &["a", "b"], Arc::clone(&opts), 0),
				create_test_table_with_vlog_id(2, &["c", "d"], Arc::clone(&opts), 0),
			],
		};
		let manifest = create_test_manifest(vec![level]);
		assert_eq!(manifest.min_oldest_vlog_file_id(), 0);
	}

	#[test]
	fn single_table_with_vlog_id() {
		let opts = Arc::new(Options::default());
		let level = Level {
			tables: vec![create_test_table_with_vlog_id(1, &["a", "b"], Arc::clone(&opts), 5)],
		};
		let manifest = create_test_manifest(vec![level]);
		assert_eq!(manifest.min_oldest_vlog_file_id(), 5);
	}

	#[test]
	fn returns_minimum_across_tables() {
		let opts = Arc::new(Options::default());
		let level = Level {
			tables: vec![
				create_test_table_with_vlog_id(1, &["a", "b"], Arc::clone(&opts), 10),
				create_test_table_with_vlog_id(2, &["c", "d"], Arc::clone(&opts), 3),
				create_test_table_with_vlog_id(3, &["e", "f"], Arc::clone(&opts), 7),
			],
		};
		let manifest = create_test_manifest(vec![level]);
		assert_eq!(manifest.min_oldest_vlog_file_id(), 3);
	}

	#[test]
	fn ignores_zero_vlog_ids() {
		let opts = Arc::new(Options::default());
		// Mix of zero and non-zero vlog_file_ids
		let level = Level {
			tables: vec![
				create_test_table_with_vlog_id(1, &["a", "b"], Arc::clone(&opts), 0),
				create_test_table_with_vlog_id(2, &["c", "d"], Arc::clone(&opts), 5),
				create_test_table_with_vlog_id(3, &["e", "f"], Arc::clone(&opts), 0),
				create_test_table_with_vlog_id(4, &["g", "h"], Arc::clone(&opts), 2),
			],
		};
		let manifest = create_test_manifest(vec![level]);
		assert_eq!(manifest.min_oldest_vlog_file_id(), 2);
	}

	#[test]
	fn tables_across_multiple_levels() {
		let opts = Arc::new(Options::default());
		let level0 = Level {
			tables: vec![
				create_test_table_with_vlog_id(1, &["a", "b"], Arc::clone(&opts), 10),
				create_test_table_with_vlog_id(2, &["c", "d"], Arc::clone(&opts), 8),
			],
		};
		let level1 = Level {
			tables: vec![
				create_test_table_with_vlog_id(3, &["e", "f"], Arc::clone(&opts), 15),
				create_test_table_with_vlog_id(4, &["g", "h"], Arc::clone(&opts), 4),
			],
		};
		let manifest = create_test_manifest(vec![level0, level1]);
		// Minimum across all levels: 4
		assert_eq!(manifest.min_oldest_vlog_file_id(), 4);
	}

	#[test]
	fn all_tables_same_nonzero_vlog_id() {
		let opts = Arc::new(Options::default());
		let level = Level {
			tables: vec![
				create_test_table_with_vlog_id(1, &["a", "b"], Arc::clone(&opts), 42),
				create_test_table_with_vlog_id(2, &["c", "d"], Arc::clone(&opts), 42),
				create_test_table_with_vlog_id(3, &["e", "f"], Arc::clone(&opts), 42),
			],
		};
		let manifest = create_test_manifest(vec![level]);
		assert_eq!(manifest.min_oldest_vlog_file_id(), 42);
	}

	#[test]
	fn tables_unordered() {
		let opts = Arc::new(Options::default());
		let level0 = Level {
			tables: vec![
				create_test_table_with_vlog_id(1, &["a", "b"], Arc::clone(&opts), 8),
				create_test_table_with_vlog_id(2, &["c", "d"], Arc::clone(&opts), 10),
			],
		};
		let level1 = Level {
			tables: vec![
				create_test_table_with_vlog_id(3, &["e", "f"], Arc::clone(&opts), 4),
				create_test_table_with_vlog_id(4, &["g", "h"], Arc::clone(&opts), 15),
			],
		};
		let manifest = create_test_manifest(vec![level0, level1]);
		// Minimum across all levels: 4
		assert_eq!(manifest.min_oldest_vlog_file_id(), 4);
	}
}
