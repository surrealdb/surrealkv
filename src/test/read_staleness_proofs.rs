//! Proof tests for the point-read staleness race.
//!
//! `CommitPipeline::commit` applies batches to the memtable OUTSIDE its
//! write mutex (commit.rs — the pipelining that keeps commits fast), and
//! memtable rotation triggers on `ArenaFull` from whichever apply hits it
//! (lsm.rs). Two concurrent committers can therefore apply out of sequence
//! order around a rotation, leaving a key's NEWER version in a source with
//! a LOWER max-seqno than an OLDER version of the same key — across two L0
//! files, an L0 file vs a deeper level, or two memtables. Point reads that
//! stop at the first source containing the key then return the stale
//! version.
//!
//! The proof constructs exactly the on-disk layout such a race produces and
//! reads it through the real store open + transaction path.

use std::collections::HashSet;
use std::fs::File as SysFile;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};

use tempfile::TempDir;
use test_log::test;

use crate::levels::{write_manifest_to_disk, LevelManifest, Levels, MANIFEST_FORMAT_VERSION_V1};
use crate::sstable::table::{Table, TableWriter};
use crate::vfs::File;
use crate::vlog::ValueLocation;
use crate::{InternalKey, InternalKeyKind, Options};

fn write_l0_style_table(
	opts: &Arc<Options>,
	id: u64,
	entries: &[(&str, u64, &str)], // (user_key, seq, value) — ascending key order
) -> Arc<Table> {
	let path = opts.sstable_file_path(id);
	let file = SysFile::create(&path).unwrap();
	let mut writer = TableWriter::new(file, id, Arc::clone(opts), 0);
	for (user_key, seq, value) in entries {
		let key = InternalKey::new(user_key.as_bytes().to_vec(), *seq, InternalKeyKind::Set, 0);
		let value = ValueLocation::with_inline_value(value.as_bytes().to_vec()).encode();
		writer.add(key, &value).unwrap();
	}
	writer.finish().unwrap();

	let file = SysFile::open(&path).unwrap();
	let file_size = file.metadata().unwrap().len();
	Arc::new(
		Table::new(id, Arc::clone(opts), Arc::new(file) as Arc<dyn File>, file_size).unwrap(),
	)
}

/// Builds a store whose layout is what out-of-order applies around a
/// rotation produce:
///
/// - L0 table T1 (seqnos 60..=120): "dup"@95 -> "old95",
///   "dup2"@60 -> "old60", filler keys at seqs 111..=120.
/// - L0 table T2 (seqnos 105..=105): "dup"@105 -> "new105".
///   T1 sorts BEFORE T2 in L0 (higher max seq) yet holds the OLDER "dup".
/// - L1 table T3: "dup2"@115 -> "new115" — newer than L0's "dup2"@60.
fn build_interleaved_store() -> (TempDir, Arc<Options>) {
	let temp_dir = TempDir::new().unwrap();
	let mut opts = Options::new();
	opts.path = temp_dir.path().to_path_buf();
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.sstable_dir()).unwrap();
	std::fs::create_dir_all(opts.manifest_dir()).unwrap();
	std::fs::create_dir_all(opts.vlog_dir()).unwrap();

	let mut t1_entries: Vec<(String, u64, String)> = vec![
		("dup".to_string(), 95, "old95".to_string()),
		("dup2".to_string(), 60, "old60".to_string()),
	];
	for i in 0..10u64 {
		t1_entries.push((format!("zz{i:02}"), 111 + i, format!("filler{i}")));
	}
	let t1_refs: Vec<(&str, u64, &str)> =
		t1_entries.iter().map(|(k, s, v)| (k.as_str(), *s, v.as_str())).collect();

	let t1 = write_l0_style_table(&opts, 1, &t1_refs);
	let t2 = write_l0_style_table(&opts, 2, &[("dup", 105, "new105")]);
	let t3 = write_l0_style_table(&opts, 3, &[("dup2", 115, "new115")]);

	let mut levels = Levels::new(opts.level_count as usize, 10);
	std::sync::Arc::make_mut(&mut levels.get_levels_mut()[0]).insert(t1);
	std::sync::Arc::make_mut(&mut levels.get_levels_mut()[0]).insert(t2);
	std::sync::Arc::make_mut(&mut levels.get_levels_mut()[1]).insert_sorted_by_key(t3);

	let manifest = LevelManifest {
		path: opts.manifest_file_path(0),
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(4)),
		manifest_format_version: MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: 120, // max seq across all tables (filler tops out at 120)
	};
	write_manifest_to_disk(&manifest).unwrap();

	(temp_dir, opts)
}

/// The staleness proof: reads through the REAL open + transaction path must
/// return the NEWEST visible version of a key even when its versions are
/// interleaved across sources.
#[test(tokio::test)]
async fn proof_interleaved_l0_reads_return_newest_version() {
	let (dir, _opts) = build_interleaved_store();

	let tree = crate::TreeBuilder::new().with_path(dir.path().to_path_buf()).build().unwrap();
	let txn = tree.begin().unwrap();

	// "dup": versions @95 (L0 table with max seq 120) and @105 (L0 table
	// with max seq 105). The newest is @105.
	let dup = txn.get(b"dup").unwrap();
	// "dup2": versions @60 (L0) and @115 (L1). The newest is @115.
	let dup2 = txn.get(b"dup2").unwrap();

	drop(txn);
	tree.close().await.unwrap();

	// BUG (present on main): first-hit source order returns the STALE
	// versions — "old95" (from the higher-max-seq L0 table) and "old60"
	// (from L0, shadowing the newer L1 version).
	assert_eq!(
		dup.as_deref(),
		Some(b"old95".as_slice()),
		"expected the stale-read bug for interleaved L0 tables; if this fails with new105, the \
		 fix landed and this proof must flip"
	);
	assert_eq!(
		dup2.as_deref(),
		Some(b"old60".as_slice()),
		"expected the stale-read bug across L0 vs L1; if this fails with new115, the fix landed \
		 and this proof must flip"
	);
}
