//! Opening a database written by SurrealKV v0.21.4 migrates it, and loses nothing.
//!
//! The databases in `tests/fixtures/legacy/v0.21.4` and `v0.20.1` were written by the real engines
//! of those versions, and each comes with `oracle.tsv`: what that same engine returned when it
//! reopened a copy: the value of every key, the latest scan, and, for the versioned ones, every
//! version `history` returned with its timestamp and sequence number. The tests compare the
//! migrated database with it.
//!
//! * `G <key> <len> <fnv>`: the latest value of a key, or `-` if it has none.
//! * `S <key> <len> <fnv>`: the latest scan, in order.
//! * `H <key> <ts> <seq> <kind> <len> <fnv>`: one version, as `history` reported it.
//! * `A <key> <ts> <len> <fnv>`: what `get_at` returned for the key at that time, or `-`.
//!
//! The tests marked `#[ignore]` read larger databases from `$SKV_LEGACY_FIXTURES/<engine version>`.
//! Those were written by a fixture generator that is not part of this repository yet: it builds
//! each released engine (v0.20.0, v0.20.1, v0.20.2, v0.21.2, v0.21.4), runs scripted workloads
//! against it (plain, with soft deletes, with versioning, with retention, with `set_at`, with the
//! log cut at a record, with a toggle of versioning), and writes each database next to an
//! `oracle.tsv` of the format above. Until it is added, the committed fixtures are what anyone
//! else can run.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use super::tfs;
use crate::levels::{DecodedManifest, MANIFEST_FORMAT_VERSION, MANIFEST_FORMAT_VERSION_V2};
use crate::migrate::for_each_entry;
use crate::migrate::legacy::{
	migrate_failing_after,
	migrate_if_needed,
	MigrationReport,
	BACKUP_DIR,
};
use crate::sstable::table::{Table, TableFormat};
use crate::vfs::File;
use crate::vlog::{VLog, ValueLocation};
use crate::{InternalKeyKind, LSMIterator, Options, Tree};

type Digest = (usize, String);

pub(crate) fn fnv(bytes: &[u8]) -> String {
	let mut hash: u64 = 0xcbf29ce484222325;
	for byte in bytes {
		hash ^= *byte as u64;
		hash = hash.wrapping_mul(0x100000001b3);
	}
	format!("{hash:016x}")
}

fn digest(value: &[u8]) -> Digest {
	(value.len(), fnv(value))
}

pub(crate) fn unhex(s: &str) -> Vec<u8> {
	(0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
}

fn digest_field(len: &str, fnv: &str) -> Option<Digest> {
	(len != "-").then(|| (len.parse().unwrap(), fnv.to_string()))
}

struct HistoryEntry {
	key: Vec<u8>,
	timestamp: u64,
	seq: u64,
	kind: String,
	value: Option<Digest>,
}

struct Oracle {
	gets: Vec<(Vec<u8>, Option<Digest>)>,
	scan: Vec<(Vec<u8>, Option<Digest>)>,
	history: Vec<HistoryEntry>,
	/// `get_at` of a key at a time.
	probes: Vec<(Vec<u8>, u64, Option<Digest>)>,
}

fn fixture_dir_of(version: &str, name: &str) -> PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy").join(version).join(name)
}

fn fixture_dir(name: &str) -> PathBuf {
	fixture_dir_of("v0.21.4", name)
}

fn load_oracle(dir: &Path) -> Oracle {
	let text = fs::read_to_string(dir.join("oracle.tsv")).unwrap();
	let mut oracle = Oracle {
		gets: Vec::new(),
		scan: Vec::new(),
		history: Vec::new(),
		probes: Vec::new(),
	};
	for line in text.lines() {
		let f: Vec<&str> = line.split('\t').collect();
		match f[0] {
			"G" => oracle.gets.push((unhex(f[1]), digest_field(f[2], f[3]))),
			"S" => oracle.scan.push((unhex(f[1]), digest_field(f[2], f[3]))),
			"H" => oracle.history.push(HistoryEntry {
				key: unhex(f[1]),
				timestamp: f[2].parse().unwrap(),
				seq: f[3].parse().unwrap(),
				kind: f[4].to_string(),
				value: digest_field(f[5], f[6]),
			}),
			"A" => {
				oracle.probes.push((unhex(f[1]), f[2].parse().unwrap(), digest_field(f[3], f[4])))
			}
			_ => {}
		}
	}
	oracle
}

pub(crate) fn copy_dir(from: &Path, to: &Path) {
	fs::create_dir_all(to).unwrap();
	for entry in fs::read_dir(from).unwrap().flatten() {
		let target = to.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &target);
		} else {
			fs::copy(entry.path(), target).unwrap();
		}
	}
}

/// A copy of a fixture database, to be opened as SurrealDB opens one.
fn database(name: &str, enable_vlog: bool) -> (TempDir, Arc<Options>, Oracle) {
	database_of("v0.21.4", name, enable_vlog)
}

fn database_of(version: &str, name: &str, enable_vlog: bool) -> (TempDir, Arc<Options>, Oracle) {
	let source = fixture_dir_of(version, name);
	let dir = TempDir::new().unwrap();
	copy_dir(&source.join("db"), dir.path());
	let opts = Arc::new(Options {
		path: dir.path().to_path_buf(),
		enable_vlog,
		..Default::default()
	});
	(dir, opts, load_oracle(&source))
}

/// Every table the manifest lists: id and the entries in it, decoded as the current layout.
pub(crate) struct MigratedEntry {
	pub(crate) key: Vec<u8>,
	pub(crate) seq: u64,
	pub(crate) kind: InternalKeyKind,
	pub(crate) timestamp: Option<u64>,
	/// The value, resolved through the value log. `None` for a tombstone with no value.
	pub(crate) value: Option<Vec<u8>>,
	/// Whether the table holds a pointer into the value log rather than the value.
	pub(crate) pointer: bool,
}

/// Opening a database cleans up its value log, which deletes the files nothing points into. After
/// it has been opened and closed, every value that a table still lists must resolve: a file taken
/// too early shows up here as an entry whose value cannot be read.
pub(crate) fn assert_every_value_resolves(opts: &Arc<Options>, what: &str) {
	let entries = migrated_entries(opts);
	assert!(!entries.is_empty(), "{what}: the database holds entries");
}

pub(crate) fn migrated_entries(opts: &Arc<Options>) -> Vec<MigratedEntry> {
	let manifest = DecodedManifest::decode(fs::read(opts.manifest_file_path(0)).unwrap()).unwrap();
	assert_eq!(manifest.version, MANIFEST_FORMAT_VERSION);
	tfs::create_dir_all(opts, opts.vlog_dir()).unwrap();
	let vlog = Arc::new(VLog::new(Arc::clone(opts)).unwrap());
	let mut out = Vec::new();
	for id in manifest.level_data.iter().flatten() {
		let path = opts.sstable_file_path(*id);
		let file: Arc<dyn File> = Arc::new(fs::File::open(&path).unwrap());
		let size = fs::metadata(&path).unwrap().len();
		// The table opens as one of the current format: LSMV1 is refused.
		let table = Table::new(*id, Arc::clone(opts), file, size).unwrap();
		assert_eq!(table.format, TableFormat::LSMV2, "table {id}");
		for_each_entry(&table, &mut |entry| {
			// A tombstone has no value; a soft delete keeps the time it happened at.
			let tombstone =
				matches!(entry.kind, InternalKeyKind::Delete | InternalKeyKind::SoftDelete);
			let (timestamp, value, pointer) = if entry.value.is_empty() {
				(None, None, false)
			} else if tombstone {
				(ValueLocation::decode(&entry.value).unwrap().timestamp, None, false)
			} else {
				let location = ValueLocation::decode(&entry.value).unwrap();
				let timestamp = location.timestamp;
				let pointer = location.is_value_pointer();
				let value = location.resolve_value(Some(&vlog)).unwrap();
				(timestamp, Some(value), pointer)
			};
			out.push(MigratedEntry {
				key: entry.user_key,
				seq: entry.seq,
				kind: entry.kind,
				timestamp,
				value,
				pointer,
			});
			Ok(())
		})
		.unwrap();
	}
	out
}

/// Every version `history` returned is in the migrated tables, with its timestamp, sequence
/// number, kind and value.
fn assert_history_preserved(oracle: &Oracle, entries: &[MigratedEntry], what: &str) {
	let by_key_seq: BTreeMap<(&[u8], u64), &MigratedEntry> =
		entries.iter().map(|e| ((e.key.as_slice(), e.seq), e)).collect();
	assert!(!oracle.history.is_empty(), "{what}: the oracle has history");
	for h in &oracle.history {
		let entry = by_key_seq.get(&(h.key.as_slice(), h.seq)).unwrap_or_else(|| {
			panic!("{what}: version seq {} of {:02x?} is missing", h.seq, h.key)
		});
		assert_eq!(
			format!("{:?}", entry.kind),
			h.kind,
			"{what}: kind of {:02x?} at {}",
			h.key,
			h.seq
		);
		assert_eq!(
			entry.timestamp,
			Some(h.timestamp),
			"{what}: timestamp of {:02x?} at {}",
			h.key,
			h.seq
		);
		assert_eq!(
			entry.value.as_deref().map(digest),
			h.value,
			"{what}: value of {:02x?} at {}",
			h.key,
			h.seq
		);
	}
}

/// A version as `history` reports it: key, timestamp, sequence number, kind and value.
type VersionRow = (Vec<u8>, u64, u64, String, Option<Digest>);

/// A copy of the options with versioned queries on, with the retention the scenario was written
/// with: `history` and `get_at` are asked of the migrated database as they were of the old one.
fn versioned(opts: &Arc<Options>, retention_ns: u64) -> Arc<Options> {
	Arc::new(Options {
		enable_versioning: true,
		versioned_history_retention_ns: retention_ns,
		..(**opts).clone()
	})
}

/// `history` and `get_at` of a migrated database return what the old engine returned for the
/// database: every version with its timestamp, sequence number, kind and value, newest first
/// within a key, and the value of a key at each time probed.
///
/// The old engine answered `get_at` by the version with the newest timestamp at or before the
/// time asked, and so does this one, whether or not timestamps went backwards.
fn assert_versioned_api(tree: &Tree, oracle: &Oracle, what: &str) {
	let txn = tree.begin().unwrap();
	let opts = crate::HistoryOptions::new().with_tombstones(true);
	let mut iter =
		txn.history_with_options([0u8].as_slice(), [0xffu8; 64].as_slice(), &opts).unwrap();
	// Per key, newest write first.
	let mut got: Vec<VersionRow> = Vec::new();
	let mut valid = iter.seek_first().unwrap();
	while valid {
		let key = iter.key();
		let tombstone = key.is_tombstone();
		got.push((
			key.user_key().to_vec(),
			key.timestamp(),
			key.seq_num(),
			format!("{:?}", key.kind()),
			(!tombstone).then(|| digest(&iter.value().unwrap())),
		));
		valid = iter.next().unwrap();
	}
	drop(iter);

	// By key, then newest timestamp first, and the newest write first among versions of one
	// timestamp. For versions written with increasing timestamps this is the order the old engine
	// returned them in. It listed a version twice when a copy of it was left in two tables, which
	// this one does not. When timestamps went backwards, which a tree without the version index
	// ordered by write, the same versions are returned in the order of their time.
	let mut expected: Vec<VersionRow> = oracle
		.history
		.iter()
		.map(|h| (h.key.clone(), h.timestamp, h.seq, h.kind.clone(), h.value.clone()))
		.collect();
	expected.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)).then(b.2.cmp(&a.2)));
	expected.dedup();
	if got != expected {
		let missing: Vec<_> = expected.iter().filter(|e| !got.contains(e)).take(3).collect();
		let extra: Vec<_> = got.iter().filter(|g| !expected.contains(g)).take(3).collect();
		let first = got.iter().zip(expected.iter()).position(|(a, b)| a != b);
		eprintln!(
			"{what}: {} versions against {}; first difference at {first:?}; missing {missing:?}; extra {extra:?}",
			got.len(),
			expected.len()
		);
		if let Some(first) = first {
			let window = |rows: &[VersionRow]| -> Vec<(Vec<u8>, u64, u64, String)> {
				rows[first.saturating_sub(1)..(first + 3).min(rows.len())]
					.iter()
					.map(|r| (r.0.clone(), r.1, r.2, r.3.clone()))
					.collect()
			};
			eprintln!("{what}: got {:?}\n{what}: expected {:?}", window(&got), window(&expected));
		}
	}
	assert_eq!(got.len(), expected.len(), "{what}: the number of versions");
	assert_eq!(got, expected, "{what}: every version, with its timestamp and sequence number");

	for (key, ts, expected) in &oracle.probes {
		let value = txn.get_at(key.clone(), *ts).unwrap();
		assert_eq!(value.as_deref().map(digest), *expected, "{what}: get_at {key:02x?} at {ts}");
	}
}

fn assert_reads_match(tree: &Tree, oracle: &Oracle, what: &str) {
	let txn = tree.begin().unwrap();
	for (key, expected) in &oracle.gets {
		let got = txn.get(key.clone()).unwrap();
		assert_eq!(got.as_deref().map(digest), *expected, "{what}: get {key:02x?}");
	}
	let mut iter = txn.range(vec![0u8], vec![0xffu8; 64]).unwrap();
	let mut scanned: Vec<(Vec<u8>, Option<Digest>)> = Vec::new();
	let mut valid = iter.seek_first().unwrap();
	while valid {
		scanned.push((iter.key().user_key().to_vec(), Some(digest(&iter.value().unwrap()))));
		valid = iter.next().unwrap();
	}
	assert_eq!(scanned.len(), oracle.scan.len(), "{what}: the scan returns as many keys");
	assert_eq!(scanned, oracle.scan, "{what}: the scan");
}

const ALL: [&str; 6] = [
	"tiny-plain",
	"tiny-versioned",
	"tiny-wal-only",
	"tiny-versioned-wal-only",
	"tiny-wal-tail",
	"tiny-versioned-wal-tail",
];

fn is_versioned(name: &str) -> bool {
	name.contains("versioned")
}

fn has_tables(name: &str) -> bool {
	!name.ends_with("wal-only")
}

fn has_wal(name: &str) -> bool {
	name.contains("wal")
}

fn assert_report(name: &str, report: &MigrationReport) {
	assert_eq!(report.tables_converted, usize::from(has_tables(name)), "{name}: {report:?}");
	assert_eq!(report.wal_segments_converted, usize::from(has_wal(name)), "{name}: {report:?}");
	assert_eq!(report.wal_entries_converted > 0, has_wal(name), "{name}: {report:?}");
	assert_eq!(report.entries_converted > 0, has_tables(name), "{name}: {report:?}");
}

#[test(tokio::test)]
async fn opening_a_v0_21_4_database_migrates_it_and_every_key_reads_as_it_did() {
	if tfs::skip_listed() {
		return;
	}
	for name in ALL {
		let (dir, opts, oracle) = database(name, true);
		let tree = Tree::new(Arc::clone(&opts)).unwrap_or_else(|e| panic!("{name}: {e}"));
		assert_reads_match(&tree, &oracle, name);
		tree.close().await.unwrap();

		// Done once: the database is current, and a second open leaves it alone.
		assert_eq!(migrate_if_needed(&opts).unwrap(), None, "{name}");
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		assert_reads_match(&tree, &oracle, name);
		tree.close().await.unwrap();
		assert_every_value_resolves(&opts, name);
		drop(dir);
	}
}

#[test]
fn the_migration_keeps_every_version_with_its_timestamp_and_sequence_number() {
	if tfs::skip_listed() {
		return;
	}
	for name in ["tiny-versioned", "tiny-versioned-wal-only", "tiny-versioned-wal-tail"] {
		let (_dir, opts, oracle) = database(name, true);
		let report = migrate_if_needed(&opts).unwrap().expect("a legacy database");
		assert_report(name, &report);
		assert_history_preserved(&oracle, &migrated_entries(&opts), name);
	}
}

#[test]
fn an_unversioned_database_keeps_the_timestamps_of_its_versions_too() {
	if tfs::skip_listed() {
		return;
	}
	for name in ["tiny-plain", "tiny-wal-only", "tiny-wal-tail"] {
		let (_dir, opts, oracle) = database(name, true);
		let report = migrate_if_needed(&opts).unwrap().expect("a legacy database");
		assert_report(name, &report);
		let entries = migrated_entries(&opts);
		// Every live key has a version, and every version that is not a hard delete is stamped.
		for (key, expected) in &oracle.gets {
			let Some(expected) = expected else {
				continue;
			};
			let newest = entries.iter().filter(|e| &e.key == key).max_by_key(|e| e.seq).unwrap();
			assert_eq!(newest.value.as_deref().map(digest), Some(expected.clone()), "{name}");
			assert!(newest.timestamp.is_some_and(|t| t > 0), "{name}: {key:02x?} has a time");
		}
		for entry in &entries {
			assert_eq!(entry.timestamp.is_none(), entry.kind == InternalKeyKind::Delete, "{name}");
		}
	}
}

#[test]
fn what_was_replaced_is_kept_and_what_was_derived_is_removed() {
	let (dir, opts, _) = database("tiny-versioned-wal-tail", true);
	fs::create_dir_all(dir.path().join("versioned_index")).unwrap();
	let before = DecodedManifest::decode(fs::read(opts.manifest_file_path(0)).unwrap()).unwrap();
	migrate_if_needed(&opts).unwrap().expect("a legacy database");

	let backup = dir.path().join(BACKUP_DIR);
	assert!(backup.join("sstables/00000000000000000001.sst").is_file(), "the old table");
	assert!(backup.join("wal/00000000000000000001.wal").is_file(), "the consumed segment");
	let saved = DecodedManifest::decode(
		fs::read(backup.join("manifest/00000000000000000000.manifest")).unwrap(),
	)
	.unwrap();
	assert_eq!(saved.version, before.version, "the manifest as it was");
	assert_eq!(saved.level_data, before.level_data);

	assert!(!dir.path().join("versioned_index").exists(), "the derived index is not in place");
	assert!(backup.join("versioned_index").is_dir(), "the derived index is kept, not deleted");
	assert!(!dir.path().join("MIGRATING").exists(), "the marker is gone");
	let old = opts.sstable_file_path(1);
	assert!(!old.exists(), "the old table is no longer listed or kept in place");
	assert!(fs::read_dir(opts.wal_dir()).map(|d| d.count()).unwrap_or(0) == 0, "no WAL left");

	let after = DecodedManifest::decode(fs::read(opts.manifest_file_path(0)).unwrap()).unwrap();
	assert_eq!(after.version, MANIFEST_FORMAT_VERSION);
	assert!(after.log_number > before.log_number, "the WAL tail counts as flushed");
	assert!(after.last_sequence >= before.last_sequence);
}

/// A run cut short after any step is resumed by the next open, to the same database.
#[test(tokio::test)]
async fn a_migration_cut_short_at_any_step_is_resumed() {
	if tfs::skip_listed() {
		return;
	}
	for name in ["tiny-versioned", "tiny-versioned-wal-tail", "tiny-wal-only"] {
		let mut completed_without_failing = false;
		for fail_after in 0..32 {
			let (_dir, opts, oracle) = database(name, true);
			match migrate_failing_after(&opts, fail_after) {
				Err(e) => {
					assert!(e.to_string().contains("injected"), "{name} after {fail_after}: {e}");
				}
				Ok(_) => completed_without_failing = true,
			}

			// Whatever state the run left, opening the database finishes the job.
			let tree = Tree::new(Arc::clone(&opts))
				.unwrap_or_else(|e| panic!("{name}, cut after step {fail_after}: {e}"));
			assert_reads_match(&tree, &oracle, &format!("{name} cut after step {fail_after}"));
			tree.close().await.unwrap();

			let entries = migrated_entries(&opts);
			if is_versioned(name) {
				assert_history_preserved(&oracle, &entries, &format!("{name} after {fail_after}"));
			}
			assert!(!opts.path.join("MIGRATING").exists());
			// Whatever the cut, what was replaced was kept and not deleted as an orphan.
			let backup = opts.path.join(BACKUP_DIR);
			if has_tables(name) {
				assert!(
					backup.join("sstables/00000000000000000001.sst").is_file(),
					"{name} cut after step {fail_after}: the replaced table is in the backup"
				);
			}
			assert!(
				backup.join("manifest/00000000000000000000.manifest").is_file(),
				"{name} cut after step {fail_after}: the manifest as it was"
			);
			if has_wal(name) {
				assert!(
					fs::read_dir(backup.join("wal")).unwrap().count() > 0,
					"{name} cut after step {fail_after}: the consumed WAL"
				);
			}
			if completed_without_failing {
				break;
			}
		}
		assert!(completed_without_failing, "{name}: raise the loop bound");
	}
}

/// The value log holds the values of a database that was written with one, so opening it with the
/// value log disabled must not turn every value into an error.
#[test(tokio::test)]
async fn a_database_with_value_log_files_is_opened_with_the_value_log_whatever_the_options_say() {
	if tfs::skip_listed() {
		return;
	}
	for name in ["tiny-plain", "tiny-versioned"] {
		let (_dir, opts, oracle) = database(name, false);
		assert!(!opts.enable_vlog);
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		assert_reads_match(&tree, &oracle, name);
		tree.close().await.unwrap();
	}
}

/// v0.21.4 opens a directory whose `wal/` still holds a segment below the manifest's `log_number`,
/// which is what a kill between a flush and the deletion of the segment leaves. The migrated
/// database has to open too: `log_number` ends up past the tail, and a segment left below it would
/// make the manifest claim more WAL than there is.
#[test(tokio::test)]
async fn a_flushed_wal_segment_left_behind_does_not_stop_the_migrated_database_opening() {
	if tfs::skip_listed() {
		return;
	}
	let (_dir, opts, oracle) = database("tiny-versioned-wal-tail", true);
	let segment = opts.wal_dir().join("00000000000000000001.wal");
	fs::copy(&segment, opts.wal_dir().join("00000000000000000000.wal")).unwrap();

	let tree = Tree::new(Arc::clone(&opts)).expect("the database opens");
	assert_reads_match(&tree, &oracle, "a stale segment");
	tree.close().await.unwrap();
	let tree = Tree::new(Arc::clone(&opts)).expect("and opens again");
	assert_reads_match(&tree, &oracle, "a stale segment, reopened");
	tree.close().await.unwrap();
}

/// A database restored by a tool that does not keep empty directories has no `sstables/` when all
/// of its data is in the WAL.
#[test(tokio::test)]
async fn a_wal_only_database_without_an_sstables_directory_migrates() {
	if tfs::skip_listed() {
		return;
	}
	for name in ["tiny-wal-only", "tiny-versioned-wal-only"] {
		let (_dir, opts, oracle) = database(name, true);
		let _ = fs::remove_dir_all(opts.sstable_dir());
		assert!(!opts.sstable_dir().exists());
		let tree = Tree::new(Arc::clone(&opts)).unwrap_or_else(|e| panic!("{name}: {e}"));
		assert_reads_match(&tree, &oracle, name);
		tree.close().await.unwrap();
	}
}

/// v0.x repaired a damaged WAL segment by cutting it at the damage and went on with the next,
/// unless it was set to refuse any corruption. The migration follows the same setting.
#[test(tokio::test)]
async fn a_damaged_wal_segment_is_cut_at_the_damage_as_v0_x_did() {
	if tfs::skip_listed() {
		return;
	}
	let damage = |opts: &Options| {
		// Records that are valid, then bytes that are not a record, in a segment that is not the
		// last one.
		let first = opts.wal_dir().join("00000000000000000001.wal");
		let mut bytes = fs::read(&first).unwrap();
		bytes.extend_from_slice(&[0xAB; 40]);
		fs::write(&first, bytes).unwrap();
		fs::write(opts.wal_dir().join("00000000000000000002.wal"), b"").unwrap();
	};

	let (_dir, opts, oracle) = database("tiny-versioned-wal-tail", true);
	damage(&opts);
	let tree = Tree::new(Arc::clone(&opts)).expect("the default mode repairs");
	assert_reads_match(&tree, &oracle, "a damaged segment");
	tree.close().await.unwrap();

	let (_dir, opts, _) = database("tiny-versioned-wal-tail", true);
	damage(&opts);
	let strict = Arc::new(Options {
		wal_recovery_mode: crate::WalRecoveryMode::AbsoluteConsistency,
		..(*opts).clone()
	});
	let err = Tree::new(strict).err().expect("AbsoluteConsistency refuses");
	assert!(
		matches!(err, crate::Error::WalCorruption { .. } | crate::Error::MigrationFailed(_)),
		"{err:?}"
	);
}

/// v0.20.0 and v0.20.1 wrote a large value to the value log when a batch was committed and logged
/// its pointer in the WAL; later versions separate values when they flush. The unflushed tail of a
/// database of those versions holds pointers, and converts with its timestamps like the rest.
#[test(tokio::test)]
async fn a_v0_20_1_wal_tail_that_logged_value_pointers_is_migrated_with_its_timestamps() {
	if tfs::skip_listed() {
		return;
	}
	for name in ["tiny-versioned-wal-tail", "tiny-versioned-wal-only", "tiny-wal-tail"] {
		let (dir, opts, oracle) = database_of("v0.20.1", name, true);
		let report = migrate_if_needed(&opts).unwrap().expect("a legacy database");
		assert!(report.wal_entries_converted > 0, "{name}: the tail was converted: {report:?}");
		if is_versioned(name) {
			assert_history_preserved(&oracle, &migrated_entries(&opts), name);
		}
		assert!(
			migrated_entries(&opts).iter().any(|e| e.pointer),
			"{name}: the tail holds values that live in the value log"
		);

		let tree = Tree::new(Arc::clone(&opts)).unwrap_or_else(|e| panic!("{name}: {e}"));
		assert_reads_match(&tree, &oracle, name);
		tree.close().await.unwrap();
		assert_every_value_resolves(&opts, name);
		drop(dir);
	}
}

/// SurrealKV numbered its value-log files from 0 before v0.20. A table that points into file 0
/// records 0 as its oldest file, as a table without pointers does, so garbage collection could
/// delete a file that is still read. Such a directory is refused before anything is changed.
#[test]
fn a_value_log_that_starts_at_file_0_is_refused_before_anything_is_changed() {
	let (dir, opts, _) = database("tiny-versioned-wal-tail", true);
	fs::copy(opts.vlog_file_path(1), opts.vlog_file_path(0)).unwrap();
	let manifest = fs::read(opts.manifest_file_path(0)).unwrap();

	let err = migrate_if_needed(&opts).expect_err("a database from before v0.20");
	let message = err.to_string();
	assert!(
		message.contains("holds file 0") && message.contains("Nothing was changed"),
		"{message}"
	);
	assert!(!message.contains("partly migrated"), "nothing was migrated: {message}");
	assert!(!dir.path().join("MIGRATING").exists());
	assert!(!dir.path().join(BACKUP_DIR).exists());
	assert_eq!(fs::read(opts.manifest_file_path(0)).unwrap(), manifest);
	assert!(opts.sstable_file_path(1).is_file());
	assert!(opts.wal_dir().join("00000000000000000001.wal").is_file());
}

/// A migration that fails before it has written its marker has changed nothing, and its error does
/// not say it left a database that is partly migrated or name a backup that was never made.
#[test]
fn a_failure_before_the_migration_began_does_not_claim_a_partial_migration() {
	let (dir, opts, _) = database("tiny-versioned", true);
	fs::remove_file(opts.sstable_file_path(1)).unwrap();

	let err = migrate_if_needed(&opts).expect_err("a listed table is missing");
	let message = err.to_string();
	assert!(message.contains("listed by the manifest"), "{message}");
	assert!(!message.contains("partly migrated"), "{message}");
	assert!(!dir.path().join("MIGRATING").exists());
	assert!(!dir.path().join(BACKUP_DIR).exists());
}

/// `skv scrub` counts corrupted blocks. A table of the older format is intact, and is read once the
/// database is opened and migrated: scrubbing it says so instead of calling it corrupt.
#[test]
fn scrubbing_a_directory_of_legacy_tables_says_it_is_legacy_not_corrupt() {
	let (dir, _, _) = database("tiny-plain", true);
	let err = crate::inspect::scrub_db(dir.path()).expect_err("legacy tables");
	assert!(matches!(err, crate::Error::LegacyFormat(_)), "{err:?}");
}

/// The point of keeping the timestamps: the versioned queries of the new engine answer, for the
/// database the old engine wrote, what the old engine answered. Read from the migrated tables,
/// from the replayed log, and after the database was closed and opened again.
#[test(tokio::test)]
async fn the_versioned_queries_of_a_migrated_database_answer_what_the_old_engine_answered() {
	if tfs::skip_listed() {
		return;
	}
	for (version, name) in [
		("v0.21.4", "tiny-versioned"),
		("v0.21.4", "tiny-versioned-wal-only"),
		("v0.21.4", "tiny-versioned-wal-tail"),
		("v0.20.1", "tiny-versioned-wal-only"),
		("v0.20.1", "tiny-versioned-wal-tail"),
	] {
		let what = format!("{version}/{name}");
		let (dir, opts, oracle) = database_of(version, name, true);
		let opts = versioned(&opts, 0);
		let tree = Tree::new(Arc::clone(&opts)).unwrap_or_else(|e| panic!("{what}: {e}"));
		assert_reads_match(&tree, &oracle, &what);
		assert_versioned_api(&tree, &oracle, &what);
		tree.close().await.unwrap();

		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		assert_reads_match(&tree, &oracle, &what);
		assert_versioned_api(&tree, &oracle, &format!("{what}, reopened"));
		tree.close().await.unwrap();
		drop(dir);
	}
}

/// A migrated database is compacted like any other: with a retention, what is read inside the
/// period is read as the old engine answered, and what is older goes but the version a read at
/// the start of the period sees. The tables are on the bottom level, where the compaction that
/// reads them is the bottom one.
#[test(tokio::test)]
async fn a_migrated_database_keeps_what_the_period_promises_through_a_compaction() {
	if tfs::skip_listed() {
		return;
	}
	use std::sync::atomic::Ordering;

	use super::versioning_tests::SetClock;
	use crate::compaction::leveled::Strategy;

	for (version, name) in [("v0.21.4", "tiny-versioned"), ("v0.20.1", "tiny-versioned-wal-tail")] {
		let (_, _, oracle) = database_of(version, name, true);
		let times: Vec<u64> = oracle.history.iter().map(|h| h.timestamp).collect();
		let (oldest, newest) = (*times.iter().min().unwrap(), *times.iter().max().unwrap());
		assert!(oldest < newest, "{version}/{name}: the versions span some time");
		// The period starts in the middle of the history, and after all of it
		for (label, cutoff) in
			[("in the middle", oldest + (newest - oldest) / 2), ("after", newest + 1)]
		{
			let what = format!("{version}/{name}, the period starts {label}");
			let (dir, opts, oracle) = database_of(version, name, true);
			let clock = Arc::new(SetClock::default());
			clock.0.store(cutoff + 1000, Ordering::SeqCst);
			let opts = Arc::new(Options {
				level_count: 2,
				clock: Arc::<SetClock>::clone(&clock),
				..(*versioned(&opts, 1000)).clone()
			});
			let tree = Tree::new(Arc::clone(&opts)).unwrap_or_else(|e| panic!("{what}: {e}"));
			assert_versioned_api(&tree, &oracle, &format!("{what}, before"));

			// A new version of the first key, in a table of the first level, which the
			// compaction merges with the migrated tables of the bottom level
			let first = oracle.history[0].key.clone();
			let mut txn = tree.begin().unwrap();
			txn.set_at(first.clone(), b"new".to_vec(), newest + 1).unwrap();
			txn.commit().await.unwrap();
			tree.core.inner.rotate_memtable().unwrap();
			tree.core.inner.flush_all_immutables_sync().unwrap();
			let strategy = Arc::new(Strategy::from_options(Arc::new(Options {
				level0_max_files: 1,
				enable_versioning: true,
				..Default::default()
			})));
			tree.compact(strategy).unwrap();

			// Inside the period, as the old engine answered
			let txn = tree.begin().unwrap();
			let mut expected: Vec<VersionRow> = oracle
				.history
				.iter()
				.filter(|h| h.key != first && h.timestamp >= cutoff)
				.map(|h| (h.key.clone(), h.timestamp, h.seq, h.kind.clone(), h.value.clone()))
				.collect();
			expected.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)).then(b.2.cmp(&a.2)));
			expected.dedup();
			let opts_all =
				crate::HistoryOptions::new().with_tombstones(true).with_ts_range(cutoff, u64::MAX);
			let mut iter = txn
				.history_with_options([0u8].as_slice(), [0xffu8; 64].as_slice(), &opts_all)
				.unwrap();
			let mut got: Vec<VersionRow> = Vec::new();
			let mut valid = iter.seek_first().unwrap();
			while valid {
				let key = iter.key();
				if key.user_key() != first.as_slice() {
					let tombstone = key.is_tombstone();
					got.push((
						key.user_key().to_vec(),
						key.timestamp(),
						key.seq_num(),
						format!("{:?}", key.kind()),
						(!tombstone).then(|| digest(&iter.value().unwrap())),
					));
				}
				valid = iter.next().unwrap();
			}
			drop(iter);
			assert_eq!(got, expected, "{what}: the history of the period");
			for (key, ts, answer) in &oracle.probes {
				if *ts >= cutoff && *key != first {
					let value = txn.get_at(key.clone(), *ts).unwrap();
					assert_eq!(
						value.as_deref().map(digest),
						*answer,
						"{what}: get_at {key:02x?} at {ts}"
					);
				}
			}
			// What was written to the first key is the newest, and a read now sees every other key
			// as the old engine did
			assert_eq!(txn.get_at(first.clone(), u64::MAX).unwrap(), Some(b"new".to_vec()));
			for (key, answer) in &oracle.gets {
				if *key != first {
					let now = txn.get(key.clone()).unwrap();
					assert_eq!(now.as_deref().map(digest), *answer, "{what}: get {key:02x?}");
				}
			}
			// With the period after all of it, a key is left with a few of its versions
			if label == "after" {
				let mut iter = txn
					.history_with_options(
						[0u8].as_slice(),
						[0xffu8; 64].as_slice(),
						&crate::HistoryOptions::new().with_tombstones(true),
					)
					.unwrap();
				let mut left = 0;
				let mut valid = iter.seek_first().unwrap();
				while valid {
					left += 1;
					valid = iter.next().unwrap();
				}
				drop(iter);
				let keys: std::collections::BTreeSet<_> =
					oracle.history.iter().map(|h| h.key.clone()).collect();
				assert!(
					left < oracle.history.len() && left <= 2 * keys.len(),
					"{what}: {left} versions are left of {} for {} keys",
					oracle.history.len(),
					keys.len()
				);
			}
			drop(txn);
			tree.close().await.unwrap();
			drop(dir);
		}
	}
}

/// A manifest that says version 2 but lists a legacy table can only come from a migration whose
/// marker was lost. Opening the database finishes the job instead of failing every time.
#[test(tokio::test)]
async fn a_lost_marker_is_recovered_by_opening_the_database() {
	if tfs::skip_listed() {
		return;
	}
	let (_dir, opts, oracle) = database("tiny-versioned", true);
	let mut manifest =
		DecodedManifest::decode(fs::read(opts.manifest_file_path(0)).unwrap()).unwrap();
	manifest.version = MANIFEST_FORMAT_VERSION_V2;
	fs::write(opts.manifest_file_path(0), manifest.encode().unwrap()).unwrap();
	assert!(!opts.path.join("MIGRATING").exists());

	let tree = Tree::new(Arc::clone(&opts)).expect("the open finishes the migration");
	assert_reads_match(&tree, &oracle, "a lost marker");
	tree.close().await.unwrap();
	assert_history_preserved(&oracle, &migrated_entries(&opts), "a lost marker");
}

/// Restoring a checkpoint replaces the files of the running database, and its tables are only
/// read when the database is opened again. A checkpoint taken by v0.x has to be refused before
/// anything is deleted.
#[test(tokio::test)]
async fn a_checkpoint_taken_by_v0_x_is_refused_before_the_live_database_is_touched() {
	if tfs::skip_listed() {
		return;
	}
	let live_dir = TempDir::new().unwrap();
	let live_opts = Arc::new(Options {
		path: live_dir.path().to_path_buf(),
		..Default::default()
	});
	let tree = Tree::new(Arc::clone(&live_opts)).unwrap();
	let mut txn = tree.begin().unwrap();
	txn.set(b"live".to_vec(), b"value".to_vec()).unwrap();
	txn.commit().await.unwrap();

	// A checkpoint of this database, with the tables and manifest of a v0.21.4 database in it.
	let checkpoint = TempDir::new().unwrap();
	let target = checkpoint.path().join("checkpoint");
	tree.create_checkpoint(&target).unwrap();
	let legacy = fixture_dir("tiny-plain").join("db");
	for sub in ["sstables", "manifest"] {
		let _ = fs::remove_dir_all(target.join(sub));
		copy_dir(&legacy.join(sub), &target.join(sub));
	}

	let before: Vec<_> =
		fs::read_dir(live_opts.sstable_dir()).unwrap().flatten().map(|e| e.file_name()).collect();
	let err = tree.restore_from_checkpoint(&target).expect_err("a legacy checkpoint is refused");
	assert!(matches!(err, crate::Error::LegacyFormat(_)), "{err:?}");
	let after: Vec<_> =
		fs::read_dir(live_opts.sstable_dir()).unwrap().flatten().map(|e| e.file_name()).collect();
	assert_eq!(before, after, "the live tables are untouched");
	assert_eq!(tree.begin().unwrap().get(b"live".to_vec()).unwrap(), Some(b"value".to_vec()));
	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn a_database_in_the_current_format_is_not_touched() {
	let dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: dir.path().to_path_buf(),
		..Default::default()
	});
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	let mut txn = tree.begin().unwrap();
	txn.set(b"key".to_vec(), b"value".to_vec()).unwrap();
	txn.commit().await.unwrap();
	tree.close().await.unwrap();

	assert_eq!(migrate_if_needed(&opts).unwrap(), None);
	assert!(!dir.path().join(BACKUP_DIR).exists());
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	assert_eq!(tree.begin().unwrap().get(b"key".to_vec()).unwrap(), Some(b"value".to_vec()));
	tree.close().await.unwrap();
}

/// Migrates every database the fixture generator wrote under `$SKV_LEGACY_FIXTURES` (an
/// `out/<engine version>` directory) and compares it with what the engine that wrote it returned.
/// These are larger than the committed fixtures: several levels, many versions, value logs of many
/// files, aborted runs. Run with `--ignored`.
#[test(tokio::test)]
#[ignore = "needs SKV_LEGACY_FIXTURES, an out/<version> directory of the fixture generator"]
async fn every_generated_fixture_migrates_to_what_the_old_engine_returned() {
	let root = PathBuf::from(std::env::var("SKV_LEGACY_FIXTURES").expect("SKV_LEGACY_FIXTURES"));
	let mut names: Vec<String> = fs::read_dir(&root)
		.unwrap()
		.flatten()
		.filter(|e| e.path().join("db").is_dir() && e.path().join("oracle.tsv").is_file())
		.map(|e| e.file_name().to_string_lossy().to_string())
		.collect();
	names.sort();
	assert!(!names.is_empty(), "no fixtures under {}", root.display());

	let mut failures = Vec::new();
	for name in names {
		let dir = root.join(&name);
		let scenario = fs::read_to_string(dir.join("scenario.json")).unwrap();
		let enable_vlog = !scenario.contains("\"vlog\": false");
		let versioned = scenario.contains("\"versioned_final\": true");

		let work = TempDir::new().unwrap();
		copy_dir(&dir.join("db"), work.path());
		let opts = Arc::new(Options {
			path: work.path().to_path_buf(),
			enable_vlog,
			..Default::default()
		});
		let oracle = load_oracle(&dir);
		let label = name.clone();
		let outcome = tokio::spawn(async move {
			let started = std::time::Instant::now();
			let report = migrate_if_needed(&opts).unwrap().expect("a legacy database");
			let elapsed = started.elapsed();
			let entries = migrated_entries(&opts);
			if versioned {
				assert_history_preserved(&oracle, &entries, &label);
			}
			let tree = Tree::new(Arc::clone(&opts)).unwrap();
			assert_reads_match(&tree, &oracle, &label);
			tree.close().await.unwrap();
			assert_every_value_resolves(&opts, &label);
			(report, elapsed, entries.len())
		})
		.await;
		match outcome {
			Ok((report, elapsed, entries)) => {
				println!("{name:24} ok  {entries:6} entries  {report:?}  in {elapsed:?}")
			}
			Err(e) => {
				println!("{name:24} FAILED  {e}");
				failures.push(name);
			}
		}
	}
	assert!(failures.is_empty(), "these fixtures did not migrate faithfully: {failures:?}");
}

/// The same databases, cut short after every step of the migration and then opened. These have
/// several tables, several levels and several WAL segments, so the states a resume starts from
/// include a version 2 manifest that lists both converted and legacy tables.
/// The versioned queries of every versioned database the old engines wrote, as the old engine
/// answered them. Run with `--ignored`.
#[test(tokio::test)]
#[ignore = "needs SKV_LEGACY_FIXTURES, an out/<version> directory of the fixture generator"]
async fn every_generated_versioned_fixture_answers_the_versioned_queries_as_the_old_engine_did() {
	let root = PathBuf::from(std::env::var("SKV_LEGACY_FIXTURES").expect("SKV_LEGACY_FIXTURES"));
	let mut names: Vec<String> = fs::read_dir(&root)
		.unwrap()
		.flatten()
		.filter(|e| e.path().join("db").is_dir() && e.path().join("oracle.tsv").is_file())
		.map(|e| e.file_name().to_string_lossy().to_string())
		.collect();
	names.sort();

	let mut failures = Vec::new();
	let mut checked = 0;
	for name in names {
		let dir = root.join(&name);
		let scenario = fs::read_to_string(dir.join("scenario.json")).unwrap();
		if !scenario.contains("\"versioned_final\": true") {
			continue;
		}
		let enable_vlog = !scenario.contains("\"vlog\": false");
		let work = TempDir::new().unwrap();
		copy_dir(&dir.join("db"), work.path());
		let opts = Arc::new(Options {
			path: work.path().to_path_buf(),
			enable_vlog,
			..Default::default()
		});
		// Retention is applied when tables are compacted: the history asked for is the one the
		// tables hold, so every version is kept.
		let opts = versioned(&opts, 0);
		let oracle = load_oracle(&dir);
		let label = name.clone();
		let outcome = tokio::spawn(async move {
			let tree = Tree::new(Arc::clone(&opts)).unwrap();
			assert_reads_match(&tree, &oracle, &label);
			assert_versioned_api(&tree, &oracle, &label);
			tree.close().await.unwrap();
		})
		.await;
		checked += 1;
		match outcome {
			Ok(()) => println!("{name:24} ok"),
			Err(e) => {
				println!("{name:24} FAILED  {e}");
				failures.push(name);
			}
		}
	}
	assert!(checked > 0, "no versioned database in {root:?}");
	assert!(failures.is_empty(), "{failures:?}");
}

#[test(tokio::test)]
#[ignore = "needs SKV_LEGACY_FIXTURES, an out/<version> directory of the fixture generator"]
async fn every_generated_fixture_survives_a_cut_at_every_step() {
	let root = PathBuf::from(std::env::var("SKV_LEGACY_FIXTURES").expect("SKV_LEGACY_FIXTURES"));
	let mut names: Vec<String> = fs::read_dir(&root)
		.unwrap()
		.flatten()
		.filter(|e| e.path().join("db").is_dir() && e.path().join("oracle.tsv").is_file())
		.map(|e| e.file_name().to_string_lossy().to_string())
		.collect();
	names.sort();

	let mut failures = Vec::new();
	let mut runs = 0usize;
	for name in names {
		let dir = root.join(&name);
		let scenario = fs::read_to_string(dir.join("scenario.json")).unwrap();
		let enable_vlog = !scenario.contains("\"vlog\": false");
		let versioned = scenario.contains("\"versioned_final\": true");
		let original = DecodedManifest::decode(
			fs::read(dir.join("db/manifest/00000000000000000000.manifest")).unwrap(),
		)
		.unwrap();
		let original_tables: Vec<u64> = original.level_data.iter().flatten().copied().collect();

		let mut completed = false;
		for fail_after in 0..200 {
			let work = TempDir::new().unwrap();
			copy_dir(&dir.join("db"), work.path());
			let opts = Arc::new(Options {
				path: work.path().to_path_buf(),
				enable_vlog,
				..Default::default()
			});
			let oracle = load_oracle(&dir);
			let label = format!("{name} cut after step {fail_after}");
			let cut = migrate_failing_after(&opts, fail_after);
			completed = cut.is_ok();
			runs += 1;

			let tables = original_tables.clone();
			let outcome = tokio::spawn(async move {
				// Finish the migration and check the history before the database is opened: the
				// database is opened with the default options, which are not versioned, and its
				// compactions keep one version per key.
				migrate_if_needed(&opts).unwrap();
				let entries = migrated_entries(&opts);
				if versioned {
					assert_history_preserved(&oracle, &entries, &label);
				}
				let tree = Tree::new(Arc::clone(&opts)).unwrap();
				assert_reads_match(&tree, &oracle, &label);
				tree.close().await.unwrap();
				assert_every_value_resolves(&opts, &label);
				let backup = opts.path.join(BACKUP_DIR).join("sstables");
				for id in tables {
					assert!(
						backup.join(format!("{id:020}.sst")).is_file(),
						"{label}: table {id} kept"
					);
				}
			})
			.await;
			if let Err(e) = outcome {
				println!("{name}: FAILED after step {fail_after}: {e}");
				failures.push(format!("{name}@{fail_after}"));
				break;
			}
			if completed {
				break;
			}
		}
		assert!(completed, "{name}: raise the loop bound");
	}
	println!("{runs} runs");
	assert!(failures.is_empty(), "{failures:?}");
}
