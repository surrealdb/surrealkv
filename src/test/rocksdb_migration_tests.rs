//! Opening a RocksDB database written by SurrealDB migrates it, and loses nothing.
//!
//! The databases in `tests/fixtures/rocksdb` were written by the real RocksDB (the C++ 11.0.0 fork
//! SurrealDB v3.1-v3.3 links) with SurrealDB's options, and each comes with `dump.tsv`: what
//! RocksDB itself returned when it opened a copy: every version with its timestamp for the
//! versioned ones (`V` lines), and the latest scan (`S` lines).
//!
//! * `V <key> <ts> <seq> <type> <len> <fnv> <resolved len> <resolved fnv>`: types 1 value, 17 blob,
//!   20 versioned delete.
//! * `S <key> <len> <fnv>`: the latest value of every live key.
//!
//! The test marked `#[ignore]` reads larger databases from `$SKV_ROCKSDB_FIXTURES`. They were
//! written by a fixture generator that is not part of this repository yet (a small program that
//! links the same RocksDB and runs scripted workloads: plain, versioned, trimmed history, blobs of
//! every size, a killed process, several compressions, formats and checksums). Until it is added,
//! the committed fixtures are what anyone else can run.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use super::tfs;
use crate::migrate::rocksdb::{hlc_to_nanos, migrate_failing_after, migrate_if_needed, BACKUP_DIR};
use crate::test::legacy_migration_tests::{
	assert_every_value_resolves,
	copy_dir,
	migrated_entries,
	unhex,
	MigratedEntry,
};
use crate::{Error, InternalKeyKind, LSMIterator, Options, Tree};

type Digest = (usize, String);

fn fnv(bytes: &[u8]) -> String {
	crate::test::legacy_migration_tests::fnv(bytes)
}

fn digest(value: &[u8]) -> Digest {
	(value.len(), fnv(value))
}

struct Dump {
	/// Set when RocksDB's own iterator stopped on an error: the dump is only a prefix of what the
	/// database holds, and the checks are for what it did return.
	truncated: bool,
	/// `(key, hlc timestamp, is delete, value)`, newest first within a key, with a version that
	/// shares a key and timestamp with a newer one dropped, as RocksDB serves them.
	versions: Vec<(Vec<u8>, u64, bool, Option<Digest>)>,
	latest: BTreeMap<Vec<u8>, Digest>,
}

fn fixture_dir(name: &str) -> PathBuf {
	Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/rocksdb").join(name)
}

fn load_dump(dir: &Path) -> Dump {
	let text = fs::read_to_string(dir.join("dump.tsv")).unwrap();
	let mut versions: Vec<(Vec<u8>, u64, bool, Option<Digest>)> = Vec::new();
	let mut latest = BTreeMap::new();
	let mut truncated = false;
	for line in text.lines() {
		let f: Vec<&str> = line.split('\t').collect();
		match f[0] {
			"E" => truncated = true,
			"V" => {
				let kind: u8 = f[4].parse().unwrap();
				let is_delete = kind == 0 || kind == 0x14;
				let key = unhex(f[1]);
				let ts: u64 = f[2].parse().unwrap();
				if versions.last().is_some_and(|(k, t, _, _)| *k == key && *t == ts) {
					continue;
				}
				let value = (!is_delete).then(|| (f[5].parse().unwrap(), f[6].to_string()));
				versions.push((key, ts, is_delete, value));
			}
			"S" => {
				latest.insert(unhex(f[1]), (f[2].parse().unwrap(), f[3].to_string()));
			}
			_ => {}
		}
	}
	Dump {
		truncated,
		versions,
		latest,
	}
}

const ALL: [&str; 5] = [
	"tiny-plain",
	"tiny-versioned",
	"tiny-plain-wal-only",
	"tiny-versioned-wal-only",
	"tiny-versioned-wal-tail",
];

fn is_versioned(name: &str) -> bool {
	name.contains("versioned")
}

fn database(name: &str, enable_vlog: bool) -> (TempDir, Arc<Options>, Dump) {
	let source = fixture_dir(name);
	let dir = TempDir::new().unwrap();
	copy_dir(&source.join("db"), dir.path());
	let opts = Arc::new(Options {
		path: dir.path().to_path_buf(),
		enable_vlog,
		..Default::default()
	});
	(dir, opts, load_dump(&source))
}

fn assert_latest_reads(tree: &Tree, dump: &Dump, what: &str) {
	let txn = tree.begin().unwrap();
	for (key, expected) in &dump.latest {
		let got = txn.get(key.clone()).unwrap();
		assert_eq!(got.as_deref().map(digest).as_ref(), Some(expected), "{what}: get {key:02x?}");
	}
	let mut iter = txn.range(vec![0u8], vec![0xffu8; 64]).unwrap();
	let mut scanned = BTreeMap::new();
	let mut valid = iter.seek_first().unwrap();
	while valid {
		scanned.insert(iter.key().user_key().to_vec(), digest(&iter.value().unwrap()));
		valid = iter.next().unwrap();
	}
	assert_eq!(scanned, dump.latest, "{what}: the scan returns the live keys and nothing else");
}

/// Every version RocksDB returned is in the migrated tables, in the same order, with its
/// timestamp converted and its value; and nothing else is.
fn assert_versions_preserved(dump: &Dump, entries: &[MigratedEntry], what: &str) {
	assert_versions(dump, entries, what, true);
}

/// With `exact` false the versions RocksDB returned are a prefix of the migrated ones: its
/// iterator gave up at an entry it could not read, and the migration did not.
fn assert_versions(dump: &Dump, entries: &[MigratedEntry], what: &str, exact: bool) {
	let mut ours: Vec<&MigratedEntry> = entries.iter().collect();
	// Table order: by key, newest first (the sequence numbers run newest highest).
	ours.sort_by(|a, b| a.key.cmp(&b.key).then(b.seq.cmp(&a.seq)));
	if exact {
		assert_eq!(ours.len(), dump.versions.len(), "{what}: the number of versions");
	} else {
		assert!(ours.len() > dump.versions.len(), "{what}: more versions than RocksDB returned");
	}
	for (entry, (key, ts, is_delete, value)) in ours.iter().zip(dump.versions.iter()) {
		assert_eq!(&entry.key, key, "{what}: key order");
		let kind = if *is_delete {
			InternalKeyKind::SoftDelete
		} else {
			InternalKeyKind::Set
		};
		assert_eq!(entry.kind, kind, "{what}: kind of {key:02x?} at {ts}");
		assert_eq!(entry.timestamp, Some(hlc_to_nanos(*ts)), "{what}: timestamp of {key:02x?}");
		assert_eq!(
			entry.value.as_deref().map(digest),
			*value,
			"{what}: value of {key:02x?} at {ts}"
		);
	}
}

/// `history` and `get_at` of a migrated database answer what RocksDB did for the database: the
/// versions of every key newest timestamp first, each with its time converted, and the value of
/// each key at each time that a version was written, and the time before it.
fn assert_versioned_queries(tree: &Tree, dump: &Dump, what: &str) {
	let txn = tree.begin().unwrap();
	let opts = crate::HistoryOptions::new().with_tombstones(true);
	let mut iter =
		txn.history_with_options([0u8].as_slice(), [0xffu8; 64].as_slice(), &opts).unwrap();
	let mut got: Vec<(Vec<u8>, u64, bool, Option<Digest>)> = Vec::new();
	let mut valid = iter.seek_first().unwrap();
	while valid {
		let key = iter.key();
		let tombstone = key.is_tombstone();
		got.push((
			key.user_key().to_vec(),
			key.timestamp(),
			tombstone,
			(!tombstone).then(|| digest(&iter.value().unwrap())),
		));
		valid = iter.next().unwrap();
	}
	drop(iter);
	let expected: Vec<(Vec<u8>, u64, bool, Option<Digest>)> = dump
		.versions
		.iter()
		.map(|(key, ts, is_delete, value)| {
			(key.clone(), hlc_to_nanos(*ts), *is_delete, value.clone())
		})
		.collect();
	assert_eq!(got.len(), expected.len(), "{what}: the number of versions");
	assert_eq!(got, expected, "{what}: every version, newest timestamp first");

	for (key, ts, _, _) in &dump.versions {
		let at = hlc_to_nanos(*ts);
		for probe in [at.saturating_sub(1), at, at + 1] {
			// What RocksDB returns at a time: the newest version at or before it.
			let want = dump
				.versions
				.iter()
				.find(|(k, t, ..)| k == key && hlc_to_nanos(*t) <= probe)
				.and_then(|(_, _, is_delete, value)| {
					if *is_delete {
						None
					} else {
						value.clone()
					}
				});
			let value = txn.get_at(key.clone(), probe).unwrap();
			assert_eq!(value.as_deref().map(digest), want, "{what}: get_at {key:02x?} at {probe}");
		}
	}
}

/// The versions of a database that SurrealDB ran in versioned mode, with their times, are what
/// the versioned queries answer once it is migrated: read from the tables, and after a restart.
#[test(tokio::test)]
async fn the_versioned_queries_of_a_migrated_rocksdb_database_answer_what_rocksdb_answered() {
	if tfs::skip_listed() {
		return;
	}
	for name in ["tiny-versioned", "tiny-versioned-wal-only", "tiny-versioned-wal-tail"] {
		let (_dir, opts, dump) = database(name, true);
		let opts = Arc::new(Options {
			enable_versioning: true,
			versioned_history_retention_ns: 0,
			..(*opts).clone()
		});
		let tree = Tree::new(Arc::clone(&opts)).unwrap_or_else(|e| panic!("{name}: {e}"));
		assert_latest_reads(&tree, &dump, name);
		assert_versioned_queries(&tree, &dump, name);
		tree.close().await.unwrap();

		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		assert_versioned_queries(&tree, &dump, &format!("{name}, reopened"));
		tree.close().await.unwrap();
	}
}

#[test]
fn the_conversion_keeps_order_and_zero() {
	let a = (1_790_000_000_001u64 << 16) | 65_535;
	let b = 1_790_000_000_002u64 << 16;
	assert!(hlc_to_nanos(a) < hlc_to_nanos(b), "a later millisecond is later whatever the counter");
	assert!(hlc_to_nanos(b) < hlc_to_nanos(b + 1), "the counter orders within a millisecond");
	assert_eq!(hlc_to_nanos(0), 0, "a garbage-collected base stays at zero");
	assert_eq!(hlc_to_nanos(1_790_000_000_001 << 16), 1_790_000_000_001_000_000);
	assert_eq!(hlc_to_nanos(u64::MAX), u64::MAX, "no panic on garbage");
}

#[test(tokio::test)]
async fn opening_a_rocksdb_database_migrates_it_and_every_key_reads_as_rocksdb_returned() {
	if tfs::skip_listed() {
		return;
	}
	for name in ALL {
		let (dir, opts, dump) = database(name, true);
		let tree = Tree::new(Arc::clone(&opts)).unwrap_or_else(|e| panic!("{name}: {e}"));
		assert_latest_reads(&tree, &dump, name);
		tree.close().await.unwrap();

		// The RocksDB files are kept, and none is left where the next open would take it for
		// a RocksDB database again.
		let backup = dir.path().join(BACKUP_DIR);
		assert!(backup.join("CURRENT").is_file(), "{name}");
		assert!(fs::read_dir(&backup)
			.unwrap()
			.flatten()
			.any(|e| { e.file_name().to_string_lossy().starts_with("MANIFEST-") }));
		assert!(!dir.path().join("CURRENT").exists(), "{name}");
		assert!(!dir.path().join("MIGRATING_ROCKSDB").exists(), "{name}");
		assert!(!dir.path().join("_rocksdb_migration").exists(), "{name}");
		assert!(
			fs::read_dir(dir.path()).unwrap().flatten().all(|e| {
				let n = e.file_name().to_string_lossy().to_string();
				!n.ends_with(".sst") && !n.ends_with(".blob") && !n.ends_with(".log")
			}),
			"{name}: no RocksDB file is left in place"
		);

		// Done once: a second open leaves it alone.
		assert_eq!(migrate_if_needed(&opts).unwrap(), None, "{name}");
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		assert_latest_reads(&tree, &dump, name);
		tree.close().await.unwrap();
		assert_every_value_resolves(&opts, name);
	}
}

#[test]
fn a_versioned_database_keeps_every_version_with_its_converted_timestamp() {
	if tfs::skip_listed() {
		return;
	}
	for name in ["tiny-versioned", "tiny-versioned-wal-only", "tiny-versioned-wal-tail"] {
		let (_dir, opts, dump) = database(name, true);
		let report = migrate_if_needed(&opts).unwrap().expect("a RocksDB database");
		assert!(report.versioned, "{name}");
		assert_eq!(report.versions as usize, dump.versions.len(), "{name}: {report:?}");
		assert_versions_preserved(&dump, &migrated_entries(&opts), name);
	}
}

#[test]
fn an_unversioned_database_keeps_the_newest_value_of_each_key_and_no_timestamps() {
	if tfs::skip_listed() {
		return;
	}
	for name in ["tiny-plain", "tiny-plain-wal-only"] {
		let (_dir, opts, dump) = database(name, true);
		let report = migrate_if_needed(&opts).unwrap().expect("a RocksDB database");
		assert!(!report.versioned, "{name}");
		let entries = migrated_entries(&opts);
		assert_eq!(entries.len(), dump.latest.len(), "{name}: one entry per live key");
		for entry in &entries {
			assert_eq!(entry.kind, InternalKeyKind::Set, "{name}");
			assert_eq!(entry.timestamp, None, "{name}: there is no time to keep");
			assert_eq!(entry.value.as_deref().map(digest).as_ref(), dump.latest.get(&entry.key));
		}
	}
}

/// Values that RocksDB kept in blob files go to the value log, where a large value belongs here.
#[test(tokio::test)]
async fn blob_values_go_to_the_value_log_or_stay_inline_as_the_options_say() {
	if tfs::skip_listed() {
		return;
	}
	for (enable_vlog, threshold) in [(true, 1024usize), (true, usize::MAX / 2), (false, 1024)] {
		let (dir, opts, dump) = database("tiny-versioned", enable_vlog);
		let opts = Arc::new(Options {
			vlog_value_threshold: threshold,
			..(*opts).clone()
		});
		let report = migrate_if_needed(&opts).unwrap().expect("a RocksDB database");
		let vlog_files = fs::read_dir(opts.vlog_dir())
			.map(|d| {
				d.flatten().filter(|e| e.path().extension().is_some_and(|x| x == "vlog")).count()
			})
			.unwrap_or(0);
		if enable_vlog && threshold == 1024 {
			assert!(report.values_in_value_log > 0, "large values go to the value log");
			assert!(vlog_files > 0);
		} else {
			assert_eq!(report.values_in_value_log, 0, "{enable_vlog} {threshold}");
		}
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		assert_latest_reads(&tree, &dump, &format!("vlog {enable_vlog} threshold {threshold}"));
		tree.close().await.unwrap();
		assert_every_value_resolves(&opts, &format!("vlog {enable_vlog} threshold {threshold}"));
		drop(dir);
	}
}

/// History trimming rewrites the oldest version of a key with a zero timestamp, and RocksDB 11 then
/// fails on the blob behind it ("Key mismatch when reading blob"): its iterator stops there. The
/// migration reads the blob, checking it against its checksums and its key, and goes on.
#[test(tokio::test)]
async fn a_trimmed_history_is_migrated_past_the_blob_rocksdb_itself_cannot_read() {
	if tfs::skip_listed() {
		return;
	}
	let (_dir, opts, dump) = database("tiny-versioned-gc", true);
	assert!(dump.truncated, "the fixture is the case where RocksDB's iterator gives up");
	let report = migrate_if_needed(&opts).unwrap().expect("a RocksDB database");
	assert!(report.versioned);
	assert!(report.versions as usize > dump.versions.len(), "{report:?}");
	let entries = migrated_entries(&opts);
	assert_versions(&dump, &entries, "tiny-versioned-gc", false);
	assert!(
		entries.iter().any(|e| e.timestamp == Some(0)),
		"the versions the trimming left at timestamp zero are kept as they are"
	);
	assert!(
		entries.iter().filter(|e| e.kind == InternalKeyKind::Set).all(|e| e.value.is_some()),
		"every value of every version was read"
	);

	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	let txn = tree.begin().unwrap();
	for (key, expected) in &dump.latest {
		let got = txn.get(key.clone()).unwrap();
		assert_eq!(got.as_deref().map(digest).as_ref(), Some(expected), "get {key:02x?}");
	}
	drop(txn);
	tree.close().await.unwrap();
	assert_every_value_resolves(&opts, "tiny-versioned-gc");
}

/// A run cut short after any step is finished or undone by the next open, to the same database.
#[test(tokio::test)]
async fn a_migration_cut_short_at_any_step_is_resumed() {
	if tfs::skip_listed() {
		return;
	}
	for name in ["tiny-versioned", "tiny-plain-wal-only", "tiny-versioned-wal-tail"] {
		let mut completed = false;
		for fail_after in 0..40 {
			let (dir, opts, dump) = database(name, true);
			match migrate_failing_after(&opts, fail_after) {
				Err(e) => {
					assert!(e.to_string().contains("injected"), "{name} after {fail_after}: {e}")
				}
				Ok(_) => completed = true,
			}
			let what = format!("{name} cut after step {fail_after}");
			// Where a run stopped is told by what is on disk, not by what the marker says.
			let marker = dir.path().join("MIGRATING_ROCKSDB");
			if fail_after % 2 == 1 && marker.exists() {
				fs::write(&marker, b"\xff not a phase").unwrap();
			}

			// Whatever state the run left, opening the database finishes or undoes the job.
			let tree = Tree::new(Arc::clone(&opts)).unwrap_or_else(|e| panic!("{what}: {e}"));
			assert_latest_reads(&tree, &dump, &what);
			tree.close().await.unwrap();
			if is_versioned(name) {
				assert_versions_preserved(&dump, &migrated_entries(&opts), &what);
			}
			assert!(!dir.path().join("MIGRATING_ROCKSDB").exists(), "{what}");
			assert!(!dir.path().join("_rocksdb_migration").exists(), "{what}");
			assert!(
				dir.path().join(BACKUP_DIR).join("CURRENT").is_file(),
				"{what}: the RocksDB files are kept"
			);
			if completed {
				break;
			}
		}
		assert!(completed, "{name}: raise the loop bound");
	}
}

/// RocksDB takes its `LOCK` file with a POSIX record lock; the lock SurrealKV takes does not
/// conflict with it. A migration must not run under a live RocksDB process.
#[cfg(unix)]
#[test]
fn a_database_a_rocksdb_process_has_open_is_not_migrated() {
	if tfs::skip_listed() {
		return;
	}
	let (dir, opts, _) = database("tiny-versioned", true);
	let lock = dir.path().join("LOCK");
	let child = Command::new("python3")
		.arg("-c")
		.arg(
			"import fcntl,sys,time\nf=open(sys.argv[1],'a+')\nfcntl.lockf(f, fcntl.LOCK_EX|fcntl.LOCK_NB)\nprint('locked',flush=True)\ntime.sleep(60)",
		)
		.arg(&lock)
		.stdout(Stdio::piped())
		.spawn();
	let Ok(mut child) = child else {
		eprintln!("python3 is not available: skipping");
		return;
	};
	let mut line = String::new();
	std::io::BufRead::read_line(
		&mut std::io::BufReader::new(child.stdout.take().unwrap()),
		&mut line,
	)
	.unwrap();
	assert_eq!(line.trim(), "locked");

	let err = migrate_if_needed(&opts).expect_err("a live RocksDB process holds the lock");
	assert!(matches!(&err, Error::MigrationFailed(m) if m.contains("in use")), "{err:?}");
	assert!(dir.path().join("CURRENT").is_file(), "nothing was touched");
	assert!(!dir.path().join("_rocksdb_migration").exists());

	child.kill().unwrap();
	child.wait().unwrap();
	assert!(migrate_if_needed(&opts).unwrap().is_some(), "once it is gone the migration runs");
}

/// The output goes to `sstables/`, `vlog/` and `wal/`; files already there are not the migration's
/// to remove if it is rolled back, nor to replay against a manifest that says nothing of them.
#[test]
fn files_where_the_output_goes_make_the_directory_one_that_is_refused() {
	for sub in ["sstables", "vlog", "wal", "manifest"] {
		let (dir, opts, _) = database("tiny-plain", true);
		fs::create_dir_all(dir.path().join(sub)).unwrap();
		fs::write(dir.path().join(sub).join("stray"), b"x").unwrap();
		let err = migrate_if_needed(&opts).expect_err("files beside the RocksDB database");
		assert!(
			matches!(&err, Error::MigrationFailed(m) if m.contains("holds files")),
			"{sub}: {err:?}"
		);
		assert!(dir.path().join("CURRENT").is_file(), "{sub}: nothing was touched");
		assert!(dir.path().join(sub).join("stray").is_file(), "{sub}: nor the file");
		assert!(!dir.path().join("MIGRATING_ROCKSDB").exists(), "{sub}: and no run was begun");
		assert!(!dir.path().join("_rocksdb_migration").exists(), "{sub}");
	}
}

/// A directory that holds both a SurrealKV manifest and RocksDB files is not a database to guess
/// at.
#[test]
fn a_directory_with_both_kinds_of_files_is_refused() {
	let (dir, opts, _) = database("tiny-plain", true);
	fs::create_dir_all(dir.path().join("manifest")).unwrap();
	fs::write(opts.manifest_file_path(0), b"x").unwrap();
	let err = migrate_if_needed(&opts).expect_err("ambiguous");
	assert!(
		matches!(&err, Error::MigrationFailed(m) if m.contains("SurrealKV manifest")),
		"{err:?}"
	);
	assert!(dir.path().join("CURRENT").is_file(), "nothing was touched");
}

/// Every database the fixture generator wrote under `$SKV_ROCKSDB_FIXTURES`, through `Tree::new`:
/// larger than the committed ones, with blobs of every size, several levels, deletes, a killed
/// process and a database whose history was trimmed. Run with `--ignored`.
#[test(tokio::test)]
#[ignore = "needs SKV_ROCKSDB_FIXTURES, a directory of databases from the fixture generator"]
async fn every_generated_rocksdb_database_migrates_to_what_rocksdb_returned() {
	let root = PathBuf::from(std::env::var("SKV_ROCKSDB_FIXTURES").expect("SKV_ROCKSDB_FIXTURES"));
	let mut names: Vec<String> = fs::read_dir(&root)
		.unwrap()
		.flatten()
		.filter(|e| e.path().join("db").is_dir() && e.path().join("dump.tsv").is_file())
		.map(|e| e.file_name().to_string_lossy().to_string())
		.collect();
	names.sort();
	assert!(!names.is_empty());

	let mut failures = Vec::new();
	for name in names {
		let dir = root.join(&name);
		let scenario = fs::read_to_string(dir.join("scenario.txt")).unwrap_or_default();
		let versioned = scenario.contains("versioned=true");
		let work = TempDir::new().unwrap();
		copy_dir(&dir.join("db"), work.path());
		let opts = Arc::new(Options {
			path: work.path().to_path_buf(),
			..Default::default()
		});
		let dump = load_dump(&dir);
		let label = name.clone();
		let outcome = tokio::spawn(async move {
			let started = std::time::Instant::now();
			let report = match migrate_if_needed(&opts) {
				Ok(report) => report.expect("a RocksDB database"),
				// RocksDB's Zlib blocks are refused, by design: SurrealDB never writes them.
				Err(Error::MigrationFailed(m)) if m.contains("Zlib") => return None,
				Err(e) => panic!("{label}: {e}"),
			};
			let elapsed = started.elapsed();
			let entries = migrated_entries(&opts);
			if versioned && !dump.truncated {
				assert_versions_preserved(&dump, &entries, &label);
			}
			let tree = Tree::new(Arc::clone(&opts)).unwrap();
			let txn = tree.begin().unwrap();
			for (key, expected) in &dump.latest {
				let got = txn.get(key.clone()).unwrap();
				assert_eq!(
					got.as_deref().map(digest).as_ref(),
					Some(expected),
					"{label}: {key:02x?}"
				);
			}
			if !dump.truncated {
				let mut iter = txn.range(vec![0u8], vec![0xffu8; 64]).unwrap();
				let mut scanned = BTreeMap::new();
				let mut valid = iter.seek_first().unwrap();
				while valid {
					scanned.insert(iter.key().user_key().to_vec(), digest(&iter.value().unwrap()));
					valid = iter.next().unwrap();
				}
				assert_eq!(scanned, dump.latest, "{label}: the live keys");
			}
			drop(txn);
			tree.close().await.unwrap();
			assert_every_value_resolves(&opts, &label);
			Some((report, elapsed, entries.len()))
		})
		.await;
		match outcome {
			Ok(Some((report, elapsed, entries))) => {
				println!("{name:28} ok  {entries:6} entries  {report:?}  in {elapsed:?}")
			}
			Ok(None) => println!("{name:28} refused (Zlib)"),
			Err(e) => {
				println!("{name:28} FAILED {e}");
				failures.push(name);
			}
		}
	}
	assert!(failures.is_empty(), "{failures:?}");
}
