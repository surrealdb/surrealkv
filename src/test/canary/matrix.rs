//! The canary matrix: markers are written through the public interface of a tree, in every
//! place that the data of a database can be, and no byte of any file that the database leaves
//! holds one.
//!
//! # What is run
//!
//! A configuration is a compression of the tables (none, Snappy or zstd), a value log that is
//! off, on with values that stay in the table because they are small, or on with values that are
//! large enough to be separated, and a durability of the commits (eventual or immediate): 18
//! configurations. For each one a database is taken through the places that its data can be in,
//! one after the other, and after each one every file under its directory is scanned:
//!
//! | step         | where the data is                                                           |
//! |--------------|-----------------------------------------------------------------------------|
//! | `WalOnly`    | committed, in the WAL (and, for large values with the log on, in the log)   |
//! | `Rotated`    | the WAL moved to a new segment, and more is committed, so there are two     |
//! | `Crashed`    | a last commit is made, and the tree is abandoned without a close           |
//! | `Recovered`  | the last record is cut in half, and the tree is opened again, which repairs |
//! | `Flushed`    | rewritten and flushed to a table                                            |
//! | `Compacted`  | the tables merged into new ones                                             |
//! | `DirectL0`   | a commit too large for a memtable, which is written as a table of its own   |
//! | `Checkpoint` | a checkpoint of the database, in a directory of its own                     |
//! | `LateCommit` | a commit after the checkpoint, which the restore is to take away            |
//! | `Restored`   | the checkpoint restored into the tree                                       |
//! | `Closed`     | the tree closed                                                             |
//!
//! Each configuration is run three ways, with the same seed and so with the very same keys and
//! values: with no key manager, which is the control; with AES-256-GCM or XChaCha20-Poly1305
//! (each pair of compression and value log is run under both, one under each durability); and
//! the two configurations that touch every surface are run under the suite that they were not
//! run under, so that both suites are seen on both. The suite does not change where data is, only
//! how it is sealed, which is why it is not another dimension of the product.
//!
//! # The controls come first
//!
//! The plaintext run is the control. At each step it must hold the markers where the data is:
//! the keys in the WAL or in the tables, the values in the WAL, in the tables, or, if they were
//! separated, in the log. If it does not, the step fails and the encrypted run is not made: a
//! scan that finds nothing in a database that has everything in the clear says nothing about one
//! that has nothing. The encrypted run is also held to having files in the directories where the
//! control is asked to find each marker, so that "found nothing" is never "had nothing to look at".
//!
//! What the control may ask is only what is certain. Where a flush and a recovery can each leave
//! data (after a recovery), it asks for either; after a flush or a compaction it asks for the
//! values that are live, since an overwritten version may go; and it never asks for the bounds of
//! a range deletion after a compaction, which may drop them.
//!
//! # What the markers are, and why
//!
//! See `scan`. Each value begins with its marker, and each key with one byte that puts it in
//! order and then its marker, which is where a block that shortens a key against the one before
//! it, and a codec that cuts where the data repeats, keep it whole. Every marker is made of random
//! printable bytes that nothing else holds, drawn fresh for each run from a seed that is part of
//! every message. A key that is the smallest of its table, one that is the largest, some in the
//! middle, a value, a key inside the value that is separated into the log, and the two bounds of a
//! range deletion are marked, so that each place that a table or a log keeps a key is covered: the
//! blocks of data, and the properties block that has the smallest and the largest key and the
//! bounds. The index and the filter hold shortened keys and hashes, and are sealed with the rest.
//!
//! A table that was compressed in the clear cannot be read for markers in its raw bytes: zstd
//! codes literals. So the control scans the tables of the plaintext run as they are read, block
//! by block, and the encrypted run is scanned as it is on disk and, for what a raw scan cannot
//! tell, checked for being sealed: see `files`.
//!
//! # The work
//!
//! What a run costs is counted here from the constants of this file, and has not been measured:
//! nothing of this package was run. A run opens two trees, makes seven commits (the last of the
//! first tree is the one that is cut), one of them of `FILLERS` entries of `LARGE` or `SMALL`
//! bytes, a flush, a compaction, a checkpoint and a restore, and is scanned eleven times. The
//! keys and values come to under 50 KB, which the WAL and then a table or the value log each hold
//! once, so an encrypted run seals on the order of 100 to 200 KB. At the 4 to 6 MB/s that AEAD
//! runs at in the debug profile that is tens of milliseconds, so the cost is the syncs that a
//! flush, a manifest write, a checkpoint and a restore make and the opening of two trees. There
//! are 40 runs: 18 controls, 18 encrypted and, for the two configurations that touch every
//! surface, one more control and one more encrypted run each. The product is split into three
//! tests by compression, so that they run on different threads.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use super::files::{assert_every_file_is_sealed, last_file, unsealed_files};
use super::scan::{scan_dir, scan_dir_with, Markers, Needles, Scan, DEFAULT_CHUNK};
use crate::compaction::leveled::Strategy;
use crate::sstable::block_io::BlockSource;
use crate::tde::{CipherSuite, SoftwareKeyManager};
use crate::test::sstable_encryption_tests::walk;
use crate::{CompressionType, Durability, Mode, Options, Tree, WalRecoveryMode};

const ROOT: [u8; 32] = [0x5C; 32];

/// The value log takes a value longer than this.
const THRESHOLD: usize = 64;
/// A value that is separated when the log is on, and one that is not.
const LARGE: usize = 140;
const SMALL: usize = 40;

/// The memtable is small enough for a commit of `FILLERS` entries to be too large for it: the
/// least that an entry can take of it is a kilobyte, so the batch is several times as large.
const MEMTABLE: usize = 64 * 1024;
const FILLERS: usize = 220;

/// How much of the last record of the WAL the crash cuts off.
const TEAR: u64 = 10;

/// The directories that a scan of a run is asked about: the database and its checkpoint.
const PLACES: [&str; 8] = [
	"db/wal",
	"db/sstables",
	"db/vlog",
	"db/manifest",
	"cp/wal",
	"cp/sstables",
	"cp/vlog",
	"cp/manifest",
];

// ---------------------------------------------------------------------------
// what is run
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VlogMode {
	Off,
	/// On, with values that are too small to be separated.
	Inline,
	/// On, with values that are separated.
	Separated,
}

#[derive(Clone, Copy, Debug)]
struct Config {
	compression: CompressionType,
	vlog: VlogMode,
	durability: Durability,
}

impl Config {
	/// Whether the values of this configuration are long.
	fn large(self) -> bool {
		self.vlog != VlogMode::Inline
	}

	/// Whether the values are in the log and not in the WAL and the tables.
	fn separated(self) -> bool {
		self.vlog == VlogMode::Separated
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Flavor {
	/// No key manager: the control.
	Plain,
	Sealed(CipherSuite),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Step {
	WalOnly,
	Rotated,
	Crashed,
	Recovered,
	Flushed,
	Compacted,
	DirectL0,
	Checkpoint,
	LateCommit,
	Restored,
	Closed,
}

fn configs() -> Vec<Config> {
	let mut all = Vec::new();
	for compression in [
		CompressionType::None,
		CompressionType::SnappyCompression,
		CompressionType::ZstdCompression,
	] {
		for vlog in [VlogMode::Off, VlogMode::Inline, VlogMode::Separated] {
			for durability in [Durability::Eventual, Durability::Immediate] {
				all.push(Config {
					compression,
					vlog,
					durability,
				});
			}
		}
	}
	all
}

/// The suite that a configuration is run under: each pair of compression and value log is run
/// under both, one under each durability.
fn suite_of(config: Config) -> CipherSuite {
	match config.durability {
		Durability::Eventual => CipherSuite::Aes256Gcm,
		Durability::Immediate => CipherSuite::XChaCha20Poly1305,
	}
}

fn options(config: Config, flavor: Flavor, path: &Path, mode: WalRecoveryMode) -> Arc<Options> {
	let opts = Options {
		path: path.to_path_buf(),
		enable_vlog: config.vlog != VlogMode::Off,
		vlog_value_threshold: THRESHOLD,
		vlog_max_file_size: 4096,
		// What was committed stays where the commit put it: nothing is flushed by a close.
		flush_on_close: false,
		max_memtable_size: MEMTABLE,
		// Nothing but the test flushes or compacts.
		memtable_stall_threshold: 1000,
		level0_max_files: 100,
		l0_stall_threshold: 1000,
		compression_per_level: vec![config.compression],
		wal_recovery_mode: mode,
		..Default::default()
	};
	Arc::new(match flavor {
		Flavor::Plain => opts,
		Flavor::Sealed(suite) => {
			opts.with_encryption(Arc::new(SoftwareKeyManager::new(ROOT)), suite)
		}
	})
}

// ---------------------------------------------------------------------------
// the trees
// ---------------------------------------------------------------------------

/// A tree whose background tasks are stopped, so that nothing is flushed or compacted behind the
/// test's back, and that is marked closed: a drop of it must not spawn a close.
pub(super) async fn open_stopped(opts: Arc<Options>) -> Tree {
	let tree = Tree::new(opts).unwrap();
	let tasks = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tasks.stop().await;
	tree.core.is_closed.store(true, Ordering::SeqCst);
	tree
}

/// Ends a tree the way a crash does: no close and no flush.
pub(super) fn crash(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}

fn newest_segment(wal: &Path) -> PathBuf {
	let mut segments: Vec<PathBuf> = fs::read_dir(wal)
		.unwrap()
		.map(|entry| entry.unwrap().path())
		.filter(|path| path.extension().is_some_and(|extension| extension == "wal"))
		.collect();
	segments.sort();
	segments.pop().expect("the WAL has a segment")
}

pub(super) fn set_len(path: &Path, len: u64) {
	fs::OpenOptions::new().write(true).open(path).unwrap().set_len(len).unwrap();
}

fn table_ids(tree: &Tree) -> BTreeSet<u64> {
	tree.core.inner.level_manifest.read().unwrap().iter().map(|table| table.id).collect()
}

fn tables_below_level_zero(tree: &Tree) -> usize {
	let levels = tree.core.inner.level_manifest.read().unwrap();
	levels.levels.get_levels().iter().skip(1).map(|level| level.tables.len()).sum()
}

// ---------------------------------------------------------------------------
// what is written
// ---------------------------------------------------------------------------

/// A key and a value, each with a marker, and a marker inside the value.
#[derive(Clone)]
struct Row {
	key: Vec<u8>,
	value: Vec<u8>,
	key_marker: usize,
	value_marker: usize,
	/// A marker that is a key, inside the value.
	inner: Option<usize>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Fate {
	/// The commit is acknowledged and stays.
	Kept,
	/// The commit is the one that the crash cuts in half: it never is in the database.
	Torn,
}

/// What a scan is asked to find: a marker, and where.
struct Expect {
	marker: usize,
	places: Vec<&'static str>,
}

/// What was seen after a step.
struct Observation {
	step: Step,
	/// How many bytes the scan searched.
	bytes: u64,
}

struct Run {
	config: Config,
	flavor: Flavor,
	markers: Markers,
	root: TempDir,
	/// Every row that was committed, in order.
	rows: Vec<Row>,
	/// The rows that the database holds: the key, and the index in `rows` of its latest value.
	live: BTreeMap<Vec<u8>, usize>,
	/// Keys that the database must not have.
	gone: Vec<Vec<u8>>,
	/// The markers of the two bounds of each range deletion.
	bounds: Vec<(usize, usize)>,
	observed: Vec<Observation>,
}

#[derive(Clone, Copy)]
enum Kind {
	Key,
	Value,
	Bound,
}

/// Where a marker of this kind is after `step`, for a configuration whose values are in the log
/// if `separated`.
fn places(step: Step, kind: Kind, separated: bool) -> Vec<&'static str> {
	let (homes, log): (&[&'static str], &[&'static str]) = match step {
		Step::WalOnly | Step::Rotated | Step::Crashed | Step::LateCommit => {
			(&["db/wal"], &["db/vlog"])
		}
		// A recovery flushes the segments that are not the last, and keeps the last in the WAL.
		Step::Recovered => (&["db/wal", "db/sstables"], &["db/vlog"]),
		Step::Flushed | Step::Compacted | Step::DirectL0 | Step::Restored | Step::Closed => {
			(&["db/sstables"], &["db/vlog"])
		}
		Step::Checkpoint => (&["cp/sstables"], &["cp/vlog"]),
	};
	match kind {
		Kind::Value if separated => log.to_vec(),
		_ => homes.to_vec(),
	}
}

impl Run {
	fn new(config: Config, flavor: Flavor, seed: u64) -> Self {
		Self {
			config,
			flavor,
			markers: Markers::new(seed),
			root: TempDir::new().unwrap(),
			rows: Vec::new(),
			live: BTreeMap::new(),
			gone: Vec::new(),
			bounds: Vec::new(),
			observed: Vec::new(),
		}
	}

	fn db(&self) -> PathBuf {
		self.root.path().join("db")
	}

	fn checkpoint(&self) -> PathBuf {
		self.root.path().join("cp")
	}

	fn name(&self, step: Step) -> String {
		format!("{:?} {:?} step {step:?}, seed {}", self.flavor, self.config, self.markers.seed())
	}

	/// A key made of a marker, after a byte that puts it where the test wants it to sort.
	fn key_of(&self, prefix: u8, marker: usize) -> Vec<u8> {
		let mut key = vec![prefix];
		key.extend_from_slice(&self.markers.get(marker).text());
		key
	}

	/// A row for a key that is new.
	fn row(&mut self, prefix: u8, label: &str) -> Row {
		let key_marker = self.markers.mint(format!("{label}-key"));
		self.row_for(prefix, key_marker, label)
	}

	/// A row for a key that was written before, with a value of its own.
	fn rewrite(&mut self, row: &Row, label: &str) -> Row {
		let prefix = row.key[0];
		self.row_for(prefix, row.key_marker, label)
	}

	fn row_for(&mut self, prefix: u8, key_marker: usize, label: &str) -> Row {
		let large = self.config.large();
		let value_marker = self.markers.mint(format!("{label}-value"));
		let mut value = self.markers.get(value_marker).text();
		let inner = large.then(|| {
			let inner = self.markers.mint(format!("{label}-inner"));
			value.push(b'|');
			value.extend_from_slice(&self.markers.get(inner).text());
			inner
		});
		value.resize(
			if large {
				LARGE
			} else {
				SMALL
			},
			b'_',
		);
		Row {
			key: self.key_of(prefix, key_marker),
			value,
			key_marker,
			value_marker,
			inner,
		}
	}

	/// The two keys that bound a range deletion.
	fn bound(&mut self, label: &str, start: u8, end: u8) -> (Vec<u8>, Vec<u8>) {
		let first = self.markers.mint(format!("{label}-start"));
		let last = self.markers.mint(format!("{label}-end"));
		self.bounds.push((first, last));
		(self.key_of(start, first), self.key_of(end, last))
	}

	/// Commits `rows`, and a range deletion if there is one, in one transaction.
	async fn write(
		&mut self,
		tree: &Tree,
		rows: Vec<Row>,
		range: Option<(Vec<u8>, Vec<u8>)>,
		fate: Fate,
	) {
		let mut txn = tree.begin().unwrap();
		txn.set_durability(self.config.durability);
		for row in &rows {
			txn.set(row.key.clone(), row.value.clone()).unwrap();
		}
		if let Some((start, end)) = range {
			txn.delete_range(start, end).unwrap();
		}
		txn.commit().await.unwrap();
		for row in rows {
			let at = self.rows.len();
			match fate {
				Fate::Kept => {
					self.live.insert(row.key.clone(), at);
				}
				Fate::Torn => self.gone.push(row.key.clone()),
			}
			self.rows.push(row);
		}
	}

	/// The database no longer has `row`: a range deletion took it.
	fn forget(&mut self, row: &Row) {
		self.live.remove(&row.key);
		self.gone.push(row.key.clone());
	}

	/// Reads every key that the database holds and every key that it must not.
	fn verify(&self, tree: &Tree, when: &str) {
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for (key, &at) in &self.live {
			let found = txn.get(key.as_slice()).unwrap();
			assert_eq!(found.as_ref(), Some(&self.rows[at].value), "{when}: the value of a key");
		}
		for key in &self.gone {
			assert!(txn.get(key.as_slice()).unwrap().is_none(), "{when}: a key that is gone");
		}
	}

	// -----------------------------------------------------------------------
	// what is looked for
	// -----------------------------------------------------------------------

	fn expectations(&self, step: Step) -> Vec<Expect> {
		let separated = self.config.separated();
		let mut asked = Vec::new();
		let rows: Vec<&Row> = match step {
			Step::WalOnly | Step::Rotated | Step::Crashed => self.rows.iter().collect(),
			Step::LateCommit => self.rows.last().into_iter().collect(),
			_ => self.live.values().map(|&at| &self.rows[at]).collect(),
		};
		for row in rows {
			asked.push(Expect {
				marker: row.key_marker,
				places: places(step, Kind::Key, separated),
			});
			for value in [Some(row.value_marker), row.inner].into_iter().flatten() {
				asked.push(Expect {
					marker: value,
					places: places(step, Kind::Value, separated),
				});
			}
		}
		// The bounds of the range deletions are in the WAL, and in the tables that were written
		// from it, until a compaction, which may drop a deletion that nothing is left under.
		if matches!(step, Step::WalOnly | Step::Rotated | Step::Crashed | Step::Recovered) {
			for &(start, end) in &self.bounds {
				for marker in [start, end] {
					asked.push(Expect {
						marker,
						places: places(step, Kind::Bound, separated),
					});
				}
			}
		}
		asked
	}

	/// Scans everything under the root and holds the scan to what the flavor of the run must show.
	async fn observe(&mut self, step: Step, tree: Option<&Tree>) {
		if let Some(tree) = tree {
			// What was committed but is still in a buffer of the WAL or the log is not in a file.
			tree.flush_wal(false).unwrap();
		}
		let needles = self.markers.needles();
		let what = self.name(step);
		let scan = match self.flavor {
			Flavor::Plain => {
				// What reads a table: the options that its blocks are decoded with, made once.
				let decoder = Arc::new(Options::default().with_block_cache_capacity(1 << 20));
				let view = |path: &Path| decoded_table(&decoder, path);
				scan_dir_with(self.root.path(), &needles, DEFAULT_CHUNK, &view).unwrap()
			}
			Flavor::Sealed(_) => scan_dir(self.root.path(), &needles).unwrap(),
		};
		match self.flavor {
			Flavor::Plain => self.assert_control(step, &scan, &needles, &what),
			Flavor::Sealed(_) => {
				assert!(
					scan.findings.is_empty(),
					"{what}: an encrypted database holds a marker: {}",
					scan.describe(&needles)
				);
				assert_every_file_is_sealed(self.root.path(), &what);
				self.assert_looked_where_the_data_is(step, &scan, &what);
			}
		}
		self.observed.push(Observation {
			step,
			bytes: scan.bytes(),
		});
	}

	/// The control holds every marker that it is asked for, where it is asked for it.
	fn assert_control(&self, step: Step, scan: &Scan, needles: &Needles, what: &str) {
		// Where the tables are not compressed, a scan of their raw bytes finds what a scan of the
		// blocks does: the scanner that the encrypted run is held to sees the markers in a
		// plaintext table, and not only the decoding of it.
		if self.config.compression == CompressionType::None {
			let raw = scan_dir(self.root.path(), needles).unwrap();
			let tables = ["db/sstables", "cp/sstables"];
			assert_eq!(
				raw.groups_in(needles, &tables),
				scan.groups_in(needles, &tables),
				"{what}: a raw scan of uncompressed tables finds other markers than their blocks hold"
			);
		}
		let by_place: BTreeMap<&str, BTreeSet<usize>> =
			PLACES.iter().map(|&place| (place, scan.groups_in(needles, &[place]))).collect();
		let looked: Vec<(&str, usize)> =
			PLACES.iter().map(|&place| (place, scan.files_in(place))).collect();
		let asked = self.expectations(step);
		assert!(!asked.is_empty(), "{what}: the control asks for nothing");
		for expect in &asked {
			let found = expect.places.iter().any(|place| by_place[place].contains(&expect.marker));
			assert!(
				found,
				"{what}: the control did not find {} in any of {:?}; it looked at {looked:?} files; \
				 {}",
				self.markers.get(expect.marker).label,
				expect.places,
				scan.describe(needles)
			);
		}
		// The WAL has moved to a second segment: the markers are in the two.
		if matches!(step, Step::Rotated | Step::Crashed) {
			let segments: BTreeSet<&PathBuf> = scan
				.findings
				.iter()
				.map(|finding| &finding.file)
				.filter(|file| file.starts_with("db/wal"))
				.collect();
			assert!(
				segments.len() >= 2,
				"{what}: the markers are in {} segment(s)",
				segments.len()
			);
		}
	}

	/// A scan that finds nothing has looked at something: for each marker that the control is
	/// asked to find, the encrypted run has a file in a directory where it is asked to find it.
	/// The control found each of those markers at this step of the same run, with the same keys
	/// and values, so these are the files that would have held them.
	fn assert_looked_where_the_data_is(&self, step: Step, scan: &Scan, what: &str) {
		for expect in self.expectations(step) {
			assert!(
				expect.places.iter().any(|place| scan.files_in(place) > 0),
				"{what}: nothing was looked at in {:?}, where {} is in the control",
				expect.places,
				self.markers.get(expect.marker).label
			);
		}
	}
}

/// The contents of a table as its blocks hold them, decompressed, in place of its bytes, for a
/// table that is not encrypted: a codec can hide a marker from a scan of the raw bytes.
fn decoded_table(opts: &Arc<Options>, path: &Path) -> Option<Vec<u8>> {
	if path.extension()? != "sst" {
		return None;
	}
	let id: u64 = path.file_stem()?.to_str()?.parse().ok()?;
	let bytes = fs::read(path).ok()?;
	let blocks = walk(opts, &bytes, id);
	let (source, _) =
		BlockSource::open_table(Arc::new(bytes.clone()), bytes.len() as u64, id, None).ok()?;
	let mut shown = Vec::new();
	for (kind, handle) in &blocks {
		shown.extend(source.read_contents(*kind, handle).ok()?);
	}
	Some(shown)
}

// ---------------------------------------------------------------------------
// the run
// ---------------------------------------------------------------------------

/// Takes a database through every place that its data can be, scanning after each step.
async fn lifecycle(config: Config, flavor: Flavor, seed: u64) -> Vec<Observation> {
	let mut run = Run::new(config, flavor, seed);
	let db = run.db();
	let wal = db.join("wal");

	// The first tree: its tasks are stopped, and what it is given stays in the WAL.
	let tree =
		open_stopped(options(config, flavor, &db, WalRecoveryMode::AbsoluteConsistency)).await;
	let smallest = run.row(b'a', "W-smallest");
	let victim = run.row(b'd', "W-victim");
	let middle = run.row(b'h', "W-middle-h");
	let largest = run.row(b'z', "W-largest");
	run.write(
		&tree,
		vec![smallest.clone(), victim.clone(), middle, largest.clone()],
		None,
		Fate::Kept,
	)
	.await;
	// The deletion covers the key between the two bounds, which is the victim's and no other's.
	let range = run.bound("W-range", b'c', b'e');
	let more = run.row(b'm', "W-middle-m");
	run.write(&tree, vec![more], Some(range), Fate::Kept).await;
	run.forget(&victim);
	run.verify(&tree, "before the first observation");
	run.observe(Step::WalOnly, Some(&tree)).await;

	// The WAL moves to a segment of its own, and the memtable is queued where nothing flushes it.
	tree.core.inner.rotate_memtable().unwrap();
	let after_rotation = run.row(b'p', "R-middle");
	run.write(&tree, vec![after_rotation], None, Fate::Kept).await;
	run.observe(Step::Rotated, Some(&tree)).await;
	let segment = newest_segment(&wal);
	let before_torn = fs::metadata(&segment).unwrap().len();

	// A last commit, and the end of the tree: no close, no flush.
	let torn = run.row(b'q', "T-torn");
	run.write(&tree, vec![torn], None, Fate::Torn).await;
	run.observe(Step::Crashed, Some(&tree)).await;
	let full = fs::metadata(&segment).unwrap().len();
	crash(&tree);
	drop(tree);

	// The last record loses its end, as it does when a write is cut short.
	assert!(before_torn + TEAR < full, "the last record is more than {TEAR} bytes long");
	set_len(&segment, full - TEAR);

	// The second tree repairs the segment, and has every commit that was acknowledged.
	let opts = options(config, flavor, &db, WalRecoveryMode::TolerateCorruptedWithRepair);
	let tree = Tree::new(opts).unwrap();
	run.verify(&tree, "after the recovery");
	run.observe(Step::Recovered, Some(&tree)).await;

	// Two of the keys are written again, and a new one, and the memtable is flushed to a table.
	let smallest_again = run.rewrite(&smallest, "F-smallest");
	let largest_again = run.rewrite(&largest, "F-largest");
	let flushed = run.row(b'k', "F-middle-k");
	run.write(&tree, vec![smallest_again, largest_again, flushed], None, Fate::Kept).await;
	tree.flush().unwrap();
	run.verify(&tree, "after the flush");
	run.observe(Step::Flushed, Some(&tree)).await;

	// Two tables of level zero are merged into new ones.
	let before = table_ids(&tree);
	assert!(
		before.len() >= 2,
		"{}: {before:?} are the tables before the compaction",
		run.name(Step::Compacted)
	);
	let eager = Arc::new(Options {
		level0_max_files: 2,
		..(*tree.core.inner.opts).clone()
	});
	tree.compact(Arc::new(Strategy::from_options(eager))).unwrap();
	assert_ne!(
		table_ids(&tree),
		before,
		"{}: the compaction wrote no table",
		run.name(Step::Compacted)
	);
	assert!(
		tables_below_level_zero(&tree) > 0,
		"{}: nothing is below level zero",
		run.name(Step::Compacted)
	);
	run.verify(&tree, "after the compaction");
	run.observe(Step::Compacted, Some(&tree)).await;

	// A commit that cannot be held by a memtable is written as a table, and is in none.
	let tables = table_ids(&tree).len();
	let first = run.row(b'b', "D-first");
	let middle = run.row(b'g', "D-middle");
	let last = run.row(b'y', "D-last");
	let mut txn = tree.begin().unwrap();
	txn.set_durability(config.durability);
	for row in [&first, &middle, &last] {
		txn.set(row.key.clone(), row.value.clone()).unwrap();
	}
	for i in 0..FILLERS {
		let mut value = format!("filler-{i:05}").into_bytes();
		value.resize(
			if config.large() {
				LARGE
			} else {
				SMALL
			},
			b'_',
		);
		txn.set(format!("f-filler-{i:05}").into_bytes(), value).unwrap();
	}
	txn.commit().await.unwrap();
	for row in [first, middle, last] {
		let at = run.rows.len();
		run.live.insert(row.key.clone(), at);
		run.rows.push(row);
	}
	assert!(
		table_ids(&tree).len() > tables,
		"{}: the commit did not become a table of its own",
		run.name(Step::DirectL0)
	);
	run.verify(&tree, "after the commit that is a table");
	run.observe(Step::DirectL0, Some(&tree)).await;

	// A checkpoint, and a commit after it that the restore takes away.
	let kept = run.live.clone();
	tree.create_checkpoint(run.checkpoint()).unwrap();
	run.observe(Step::Checkpoint, Some(&tree)).await;
	let late = run.row(b'x', "Z-late");
	run.write(&tree, vec![late.clone()], None, Fate::Kept).await;
	run.observe(Step::LateCommit, Some(&tree)).await;

	tree.restore_from_checkpoint(run.checkpoint()).unwrap();
	run.live = kept;
	run.forget(&late);
	run.verify(&tree, "after the restore");
	run.observe(Step::Restored, Some(&tree)).await;

	tree.close().await.unwrap();
	run.observe(Step::Closed, None).await;
	run.observed
}

/// A run with no key manager, which must hold every marker where the data is, and then one under
/// each of `suites`, of the same configuration with the same seed, which must hold none.
async fn run_both_ways(config: Config, seed: u64, suites: &[CipherSuite]) {
	let control = lifecycle(config, Flavor::Plain, seed).await;
	assert_eq!(control.len(), 11, "every step was observed");
	assert!(control.iter().map(|seen| seen.bytes).sum::<u64>() > 0);
	for &suite in suites {
		let sealed = lifecycle(config, Flavor::Sealed(suite), seed).await;
		assert_eq!(
			sealed.iter().map(|seen| seen.step).collect::<Vec<_>>(),
			control.iter().map(|seen| seen.step).collect::<Vec<_>>()
		);
	}
}

async fn the_whole_product_for(compression: CompressionType) {
	let seed = Markers::from_environment().seed();
	for config in configs().into_iter().filter(|config| config.compression == compression) {
		run_both_ways(config, seed, &[suite_of(config)]).await;
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn no_marker_is_in_any_file_of_a_database_whose_tables_are_not_compressed() {
	the_whole_product_for(CompressionType::None).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn no_marker_is_in_any_file_of_a_database_whose_tables_are_compressed_with_snappy() {
	the_whole_product_for(CompressionType::SnappyCompression).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn no_marker_is_in_any_file_of_a_database_whose_tables_are_compressed_with_zstd() {
	the_whole_product_for(CompressionType::ZstdCompression).await;
}

/// The two configurations that touch every surface, under the suite that the product did not run
/// them under: both suites have seen them all.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_configurations_that_touch_every_surface_are_clean_under_the_other_suite_as_well() {
	let seed = Markers::from_environment().seed();
	for (compression, durability) in [
		(CompressionType::ZstdCompression, Durability::Eventual),
		(CompressionType::SnappyCompression, Durability::Immediate),
	] {
		let config = Config {
			compression,
			vlog: VlogMode::Separated,
			durability,
		};
		let other = match suite_of(config) {
			CipherSuite::Aes256Gcm => CipherSuite::XChaCha20Poly1305,
			_ => CipherSuite::Aes256Gcm,
		};
		run_both_ways(config, seed, &[other]).await;
	}
}

// ---------------------------------------------------------------------------
// the canary can fail
// ---------------------------------------------------------------------------

/// A database that is closed with a table, a value-log file if there is a log, and a commit that
/// is in the WAL and in no table.
async fn small_database(run: &mut Run) {
	let opts = options(run.config, run.flavor, &run.db(), WalRecoveryMode::AbsoluteConsistency);
	let tree = Tree::new(opts).unwrap();
	let first = vec![run.row(b'a', "P-first"), run.row(b'm', "P-middle")];
	run.write(&tree, first, None, Fate::Kept).await;
	tree.flush().unwrap();
	let last = run.row(b'z', "P-last");
	run.write(&tree, vec![last], None, Fate::Kept).await;
	tree.flush_wal(false).unwrap();
	tree.close().await.unwrap();
}

/// What the matrix says is only worth something if it would have said otherwise: files of a
/// database that is not encrypted, put among those of one that is, are found by the scan for
/// markers, and by the check of what each file is, which finds them even where a codec keeps a
/// marker from the scan.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn the_canary_fails_for_a_plaintext_file_that_is_planted_in_an_encrypted_database() {
	let seed = Markers::from_environment().seed();
	for compression in [CompressionType::None, CompressionType::ZstdCompression] {
		let config = Config {
			compression,
			vlog: VlogMode::Separated,
			durability: Durability::Eventual,
		};
		// The same seed, and so the same keys and values, in a database of each kind.
		let mut plain = Run::new(config, Flavor::Plain, seed);
		let mut sealed = Run::new(config, Flavor::Sealed(CipherSuite::XChaCha20Poly1305), seed);
		small_database(&mut plain).await;
		small_database(&mut sealed).await;
		let needles = plain.markers.needles();
		let (plain_db, db) = (plain.db(), sealed.db());
		let what = format!("{compression:?}, seed {seed}");

		// Before anything is planted, the database that is encrypted is clean to both checks.
		let clean = scan_dir(&db, &needles).unwrap();
		assert!(clean.findings.is_empty(), "{what}: {}", clean.describe(&needles));
		assert!(clean.files_in("sstables") > 0 && clean.files_in("vlog") > 0, "{what}");
		assert!(unsealed_files(&db).unwrap().is_empty(), "{what}");

		// One file of each kind of the plaintext database, under a name of its own.
		for (dir, extension) in [("sstables", "sst"), ("vlog", "vlog"), ("wal", "wal")] {
			let planted = db.join(dir).join(format!("{:020}.{extension}", 99));
			fs::copy(last_file(&plain_db, dir, extension), planted).unwrap();
		}
		let scan = scan_dir(&db, &needles).unwrap();
		assert!(!scan.groups_in(&needles, &["vlog"]).is_empty(), "{what}: the log");
		assert!(!scan.groups_in(&needles, &["wal"]).is_empty(), "{what}: the WAL");
		if compression == CompressionType::None {
			assert!(!scan.groups_in(&needles, &["sstables"]).is_empty(), "{what}: the table");
		}
		// Whatever the codec keeps from the scan, each of the three is named by what it is.
		let wrong = unsealed_files(&db).unwrap();
		assert_eq!(wrong.len(), 3, "{what}: {wrong:?}");
		for extension in ["sst", "vlog", "wal"] {
			let name = format!("{:020}.{extension}", 99);
			assert!(wrong.iter().any(|line| line.contains(&name)), "{what}: {wrong:?}");
		}
	}
}
