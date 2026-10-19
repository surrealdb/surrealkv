//! The move of an IndexedDB database into a database of a pool of files, over the memory pool and a
//! source of rows that stands in for IndexedDB (see `crate::migrate::indexeddb`).
//!
//! What they show, in the order of the file:
//!
//! * the move copies every row, in pieces of the sizes that the limits say, with the value log on
//!   and off and with rows that are empty, binary, as large as the threshold of the value log, and
//!   larger than the limit of a chunk;
//! * a database that exists is never copied over, and the source is not looked at for it;
//! * a copy that was cut short is made again, and from the source as it is by then; if the source
//!   is gone, the call fails and makes no database;
//! * the verification fails a copy that is not the source, and what it leaves is not a database;
//! * the errors are typed, and the lock of another page keeps its type;
//! * a crash at every operation of the call, in every way the memory pool can crash, leaves a state
//!   that the next call turns into a database that is the source, and never one that it takes for a
//!   database while it is a part of one.

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use surrealkv_compat_indxdb::{Chunk, ChunkFill, ChunkLimits};
use test_log::test;

use super::fs_engine_tests::within;
use super::fs_probe::{Call, ProbeFs, Verdict};
use crate::error::Result;
use crate::migrate::indexeddb::{
	check_account,
	compare,
	refusal as row_refusal,
	run,
	Counted,
	ImportReport,
	ImportTuning,
	IndexedDbMigration,
	Mounter,
	Presence,
	Refusal,
	Rows as Summary,
	Source,
	Tally as RowTally,
	MARKER,
};
use crate::vfs::{self, EntryKind, Fs, MemRaw, PoolConfig, PoolFs};
use crate::{CrashMode, Error, FileSystem, LSMIterator, Mode, Options, Tree, TreeBuilder};

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

// ---------------------------------------------------------------------------------------------
// A source of rows
// ---------------------------------------------------------------------------------------------

/// The rows of a store, served in chunks as the reader of IndexedDB serves them, and counted.
struct FakeSource {
	state: Mutex<FakeState>,
	probes: AtomicUsize,
	reads: AtomicUsize,
}

struct FakeState {
	/// The rows as the store gives them: in the order of its keys, unless a test says otherwise.
	rows: Rows,
	present: bool,
	/// There is no IndexedDB in this context: the probe says so, and a read fails.
	unavailable: bool,
	/// There is no IndexedDB from the probe after this many probes on.
	unavailable_after: Option<usize>,
	/// The probe gives this error at this probe, counted from one: any failure of the source
	/// other than there being no IndexedDB.
	probe_fails_at: Option<usize>,
	/// The source is gone after this many reads of a chunk.
	vanishes_after: Option<usize>,
	/// The source gives this error at this read of a chunk, counted from one.
	fails_at: Option<usize>,
}

impl FakeSource {
	/// A source with `rows`, which are sorted by key as a store sorts binary keys.
	fn new(mut rows: Rows) -> Self {
		rows.sort();
		Self::in_order(rows)
	}

	/// A source that gives the rows in the order that they are in, which can repeat a key.
	fn in_order(rows: Rows) -> Self {
		Self {
			state: Mutex::new(FakeState {
				rows,
				present: true,
				unavailable: false,
				unavailable_after: None,
				probe_fails_at: None,
				vanishes_after: None,
				fails_at: None,
			}),
			probes: AtomicUsize::new(0),
			reads: AtomicUsize::new(0),
		}
	}

	fn missing() -> Self {
		let source = Self::in_order(Vec::new());
		source.state.lock().unwrap().present = false;
		source
	}

	/// A source in a context that has no IndexedDB.
	fn unavailable() -> Self {
		let source = Self::missing();
		source.state.lock().unwrap().unavailable = true;
		source
	}

	fn probes(&self) -> usize {
		self.probes.load(Ordering::SeqCst)
	}

	fn reads(&self) -> usize {
		self.reads.load(Ordering::SeqCst)
	}

	fn calls(&self) -> usize {
		self.probes() + self.reads()
	}

	fn set_present(&self, present: bool) {
		self.state.lock().unwrap().present = present;
	}

	fn set_unavailable(&self, unavailable: bool) {
		self.state.lock().unwrap().unavailable = unavailable;
	}

	fn unavailable_after(&self, probes: usize) {
		self.state.lock().unwrap().unavailable_after = Some(probes);
	}

	fn fail_probe_at(&self, probe: usize) {
		self.state.lock().unwrap().probe_fails_at = Some(probe);
	}

	fn set_rows(&self, rows: Rows) {
		let mut state = self.state.lock().unwrap();
		state.rows = rows;
		state.rows.sort();
	}

	fn vanish_after(&self, reads: usize) {
		self.state.lock().unwrap().vanishes_after = Some(reads);
	}

	fn fail_at(&self, read: usize) {
		self.state.lock().unwrap().fails_at = Some(read);
	}
}

impl Source for FakeSource {
	/// The position of the next row, which the move does not look at.
	type Cursor = usize;

	async fn presence(&self) -> Result<Presence> {
		let probe = self.probes.fetch_add(1, Ordering::SeqCst) + 1;
		let state = self.state.lock().unwrap();
		if state.probe_fails_at == Some(probe) {
			return Err(Error::MigrationFailed("the probe of the source failed".to_string()));
		}
		if state.unavailable || state.unavailable_after.is_some_and(|probes| probe > probes) {
			return Ok(Presence::Unavailable);
		}
		Ok(if state.present {
			Presence::Present
		} else {
			Presence::Absent
		})
	}

	async fn read_chunk(
		&self,
		after: Option<&usize>,
		limits: &ChunkLimits,
	) -> Result<Option<Chunk<usize>>> {
		let read = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
		let mut state = self.state.lock().unwrap();
		if state.vanishes_after.is_some_and(|reads| read > reads) {
			state.present = false;
		}
		if state.fails_at == Some(read) {
			return Err(Error::MigrationFailed("the source failed".to_string()));
		}
		if state.unavailable {
			return Err(Error::MigrationFailed("IndexedDB is not available".to_string()));
		}
		if !state.present {
			return Ok(None);
		}
		// The loop of the reader of IndexedDB: an entry that does not fit is left for the next
		// chunk, a chunk that is full is not read past, and the end is known when a read finds it.
		let mut fill = ChunkFill::new(*limits);
		let mut entries = Vec::new();
		let mut at = after.copied().unwrap_or(0);
		let mut done = false;
		loop {
			let Some((key, value)) = state.rows.get(at) else {
				done = true;
				break;
			};
			let weight = key.len() + value.len();
			if !fill.admits(weight) {
				break;
			}
			fill.take(weight);
			entries.push((key.clone(), value.clone()));
			at += 1;
			if fill.is_full() {
				break;
			}
		}
		Ok(Some(Chunk {
			bytes: fill.bytes(),
			last: (!entries.is_empty()).then_some(at),
			entries,
			done,
		}))
	}
}

// ---------------------------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------------------------

type OnProbe = Arc<dyn Fn(&Arc<ProbeFs>) + Send + Sync>;

/// A change to the database that is made as the files are mounted, as a fault of the storage would
/// make it.
#[derive(Clone)]
enum Tamper {
	Delete(Vec<u8>),
	Set(Vec<u8>, Vec<u8>),
}

/// The memory that a database is kept in, mounted as often as the move asks.
struct MemMounter {
	raw: MemRaw,
	mounts: AtomicUsize,
	/// Done to the database when the mount with this number is made, counted from one.
	tamper: Mutex<Option<(usize, Tamper, Options)>>,
	/// Called with the probe that wraps each mount, to put hooks on it.
	on_probe: Mutex<Option<OnProbe>>,
	/// The mount with this number fails with this error.
	mount_fails: Mutex<Option<(usize, Error)>>,
	/// After the mount with this number, the memory dies after this many more operations.
	dies_after_mount: Mutex<Option<(usize, u64)>>,
	/// The operations that the memory had seen when each mount began and when it was done.
	started: Mutex<Vec<u64>>,
	finished: Mutex<Vec<u64>>,
}

impl MemMounter {
	fn new(raw: MemRaw) -> Self {
		Self {
			raw,
			mounts: AtomicUsize::new(0),
			tamper: Mutex::new(None),
			on_probe: Mutex::new(None),
			mount_fails: Mutex::new(None),
			dies_after_mount: Mutex::new(None),
			started: Mutex::new(Vec::new()),
			finished: Mutex::new(Vec::new()),
		}
	}

	fn die_after_mount(&self, number: usize, operations: u64) {
		*self.dies_after_mount.lock().unwrap() = Some((number, operations));
	}

	fn started(&self) -> Vec<u64> {
		self.started.lock().unwrap().clone()
	}

	fn finished(&self) -> Vec<u64> {
		self.finished.lock().unwrap().clone()
	}

	fn mounts(&self) -> usize {
		self.mounts.load(Ordering::SeqCst)
	}

	fn tamper_at_mount(&self, number: usize, change: Tamper, options: Options) {
		*self.tamper.lock().unwrap() = Some((number, change, options));
	}

	fn on_probe(&self, hook: impl Fn(&Arc<ProbeFs>) + Send + Sync + 'static) {
		*self.on_probe.lock().unwrap() = Some(Arc::new(hook));
	}

	fn fail_mount(&self, number: usize, error: Error) {
		*self.mount_fails.lock().unwrap() = Some((number, error));
	}
}

impl Mounter for MemMounter {
	async fn exists(&self) -> Result<bool> {
		Ok(!self.raw.names().is_empty())
	}

	async fn mount(&self) -> Result<FileSystem> {
		let number = self.mounts.fetch_add(1, Ordering::SeqCst) + 1;
		self.started.lock().unwrap().push(self.raw.ops());
		let failure = {
			let mut fails = self.mount_fails.lock().unwrap();
			if fails.as_ref().is_some_and(|(at, _)| *at == number) {
				fails.take().map(|(_, error)| error)
			} else {
				None
			}
		};
		if let Some(error) = failure {
			return Err(error);
		}
		let (pool, _) = PoolFs::<MemRaw>::mount_sync(self.raw.clone(), PoolConfig::memory())?;
		let mut fs = FileSystem(Arc::clone(&pool) as Arc<dyn Fs>);
		let probe_hook = self.on_probe.lock().unwrap().clone();
		if let Some(hook) = probe_hook {
			let (probe, wrapped) = ProbeFs::wrap(&fs);
			hook(&probe);
			fs = wrapped;
		}
		let tamper = self.tamper.lock().unwrap().clone();
		if let Some((at, change, options)) = tamper {
			if at == number {
				change_database(&fs, &options, &change).await;
			}
		}
		self.finished.lock().unwrap().push(self.raw.ops());
		let dies = *self.dies_after_mount.lock().unwrap();
		if let Some((at, operations)) = dies {
			if at == number {
				self.raw.die_after(operations);
			}
		}
		Ok(fs)
	}
}

/// Opens the database in `fs` as an application would and makes `change` to it.
async fn change_database(fs: &FileSystem, options: &Options, change: &Tamper) {
	let options = Options {
		fs: Arc::clone(&fs.0),
		..options.clone()
	};
	// Past the refusal of the builder: the marker is there, as it is while a copy is checked.
	let tree = Tree::open_async(Arc::new(options)).await.unwrap();
	let mut txn = tree.begin().unwrap();
	match change {
		Tamper::Delete(key) => txn.delete(&key[..]).unwrap(),
		Tamper::Set(key, value) => txn.set(&key[..], &value[..]).unwrap(),
	}
	txn.commit().await.unwrap();
	tree.close().await.unwrap();
}

/// A mount that fails the way the mount of a database that another page has open does.
fn locked() -> Error {
	Error::Io(Arc::new(vfs::database_locked(std::io::Error::new(
		std::io::ErrorKind::PermissionDenied,
		"another page holds the lock",
	))))
}

// ---------------------------------------------------------------------------------------------
// The data of the tests
// ---------------------------------------------------------------------------------------------

/// The key of row `i`: binary, with bytes that no text has, and in an order that is not the order
/// of `i`.
fn key_of(i: usize) -> Vec<u8> {
	let mut key = (i as u32).wrapping_mul(2_654_435_761).to_be_bytes().to_vec();
	key.extend_from_slice(&[0x00, 0xff, (i % 251) as u8]);
	key
}

/// The value of row `i`: of a length that depends on `i`, among them none, and the lengths on both
/// sides of the threshold of the value log of the tests.
fn value_of(i: usize) -> Vec<u8> {
	let len = match i % 8 {
		0 => 0,
		1 => 1,
		2 => VLOG_THRESHOLD - 1,
		3 => VLOG_THRESHOLD,
		4 => VLOG_THRESHOLD + 1,
		5 => 300,
		6 => 2_000,
		_ => 17,
	};
	(0..len).map(|b| (i.wrapping_mul(31).wrapping_add(b) % 256) as u8).collect()
}

/// The threshold of the value log of the tests.
const VLOG_THRESHOLD: usize = 64;

/// `count` rows of every length that `value_of` has, and one row of three million bytes.
fn rows_of(count: usize) -> Rows {
	let mut rows: Rows = (0..count).map(|i| (key_of(i), value_of(i))).collect();
	if count > 0 {
		let big: Vec<u8> = (0..3_000_000usize).map(|b| (b % 253) as u8).collect();
		rows.push((b"big".to_vec(), big));
	}
	rows.sort();
	rows
}

fn small_rows(count: usize) -> Rows {
	let mut rows: Rows = (0..count).map(|i| (key_of(i), value_of(i))).collect();
	rows.sort();
	rows
}

fn options(memtable: usize, vlog: bool) -> Options {
	Options {
		path: "db".into(),
		max_memtable_size: memtable,
		level0_max_files: 2,
		enable_vlog: vlog,
		vlog_value_threshold: VLOG_THRESHOLD,
		vlog_max_file_size: 64 * 1024,
		..Options::default()
	}
}

/// Pieces that are small enough for a few dozen rows to take several of each.
fn small_tuning() -> ImportTuning {
	ImportTuning {
		read: ChunkLimits::new(2_500, 9),
		write: ChunkLimits::new(1_500, 4),
		commit_pause: std::time::Duration::ZERO,
		abandon_after_chunks: None,
	}
}

fn weight(rows: &Rows) -> u64 {
	rows.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum()
}

fn imported(rows: &Rows) -> IndexedDbMigration {
	imported_skipping(rows, 0)
}

/// What a copy of a source that has the rows `written` and `skipped` more that the engine refuses
/// says.
fn imported_skipping(written: &Rows, skipped: u64) -> IndexedDbMigration {
	IndexedDbMigration::Imported {
		entries: written.len() as u64,
		bytes: weight(written),
		skipped,
	}
}

// ---------------------------------------------------------------------------------------------
// Looking at what the move leaves
// ---------------------------------------------------------------------------------------------

/// A pool mounted over a copy of `raw` as it is now, flushed or not, so that `raw` is not touched.
fn look_at(raw: &MemRaw) -> FileSystem {
	let (pool, _) = PoolFs::<MemRaw>::mount_sync(raw.snapshot(), PoolConfig::memory())
		.expect("a copy of the memory mounts");
	FileSystem(pool as Arc<dyn Fs>)
}

/// Every file of the pool, below both of its roots.
fn files_of(fs: &FileSystem) -> Vec<String> {
	fn walk(fs: &dyn Fs, dir: &Path, out: &mut Vec<String>) {
		let Ok(entries) = fs.read_dir(dir) else {
			return;
		};
		for entry in entries {
			let path = dir.join(&entry.name);
			match entry.kind {
				EntryKind::Dir => walk(fs, &path, out),
				_ => out.push(path.to_string_lossy().into_owned()),
			}
		}
	}
	let mut files = Vec::new();
	walk(&*fs.0, Path::new(""), &mut files);
	walk(&*fs.0, Path::new("/"), &mut files);
	files.sort();
	files
}

/// What is in the memory: whether the marker is there, and which other files are.
struct Contents {
	marker: bool,
	files: Vec<String>,
}

fn contents(raw: &MemRaw) -> Contents {
	let fs = look_at(raw);
	let mut files = files_of(&fs);
	let marker = files.iter().any(|f| f == MARKER);
	files.retain(|f| f != MARKER);
	Contents {
		marker,
		files,
	}
}

/// Every row of the database in `raw`, which is opened as an application opens it, or `None` if it
/// does not open.
async fn database(raw: &MemRaw, options: &Options) -> Option<Rows> {
	let fs = look_at(raw);
	let options = Options {
		fs: Arc::clone(&fs.0),
		..options.clone()
	};
	let tree = TreeBuilder::with_options(options).build_async().await.ok()?;
	let mut rows = Vec::new();
	{
		let txn = tree.begin_with_mode(Mode::ReadOnly).ok()?;
		let mut iter = txn.iter().ok()?;
		let mut valid = iter.seek_first().ok()?;
		while valid {
			rows.push((iter.key().user_key().to_vec(), iter.value().ok()?));
			valid = iter.next().ok()?;
		}
	}
	tree.close().await.ok()?;
	Some(rows)
}

/// Writes `rows` to the database in `raw` as an application would, with no move.
async fn app_writes(raw: &MemRaw, options: &Options, rows: &Rows) {
	let (pool, _) = PoolFs::<MemRaw>::mount_sync(raw.clone(), PoolConfig::memory()).unwrap();
	let fs = FileSystem(Arc::clone(&pool) as Arc<dyn Fs>);
	let options = Options {
		fs: Arc::clone(&fs.0),
		..options.clone()
	};
	let tree = TreeBuilder::with_options(options).build_async().await.unwrap();
	let mut txn = tree.begin().unwrap();
	for (key, value) in rows {
		txn.set(&key[..], &value[..]).unwrap();
	}
	txn.commit().await.unwrap();
	tree.close().await.unwrap();
	fs.close().await.unwrap();
}

/// One call of the move.
async fn call<M: Mounter>(
	source: &FakeSource,
	mounter: &M,
	options: &Options,
	tuning: &ImportTuning,
) -> (Result<IndexedDbMigration>, ImportReport) {
	let mut report = ImportReport::default();
	let result =
		within("a call of the move", run(source, mounter, options, tuning, &mut report)).await;
	(result, report)
}

/// A call with the tuning of the small tests.
async fn small_call(
	source: &FakeSource,
	raw: &MemRaw,
	options: &Options,
) -> Result<IndexedDbMigration> {
	call(source, &MemMounter::new(raw.clone()), options, &small_tuning()).await.0
}

fn assert_rows_eq(found: &Rows, wanted: &Rows, what: &str) {
	assert_eq!(found.len(), wanted.len(), "{what}: the number of rows");
	for (at, (f, w)) in found.iter().zip(wanted).enumerate() {
		assert!(f.0 == w.0, "{what}: the key of row {at} differs");
		assert!(
			f.1 == w.1,
			"{what}: the value of row {at} differs ({} and {} bytes)",
			f.1.len(),
			w.1.len()
		);
	}
}

/// The database in `raw` is the rows `wanted`, and there is no marker.
async fn assert_database(raw: &MemRaw, options: &Options, wanted: &Rows, what: &str) {
	let found = database(raw, options).await.unwrap_or_else(|| panic!("{what}: no database opens"));
	assert_rows_eq(&found, wanted, what);
	assert!(!contents(raw).marker, "{what}: the marker is still there");
}

// ---------------------------------------------------------------------------------------------
// A copy
// ---------------------------------------------------------------------------------------------

#[test(tokio::test)]
async fn every_row_is_copied_and_the_next_call_finds_the_database() {
	for (memtable, vlog) in [(4 * 1024, false), (4 * 1024, true), (64 * 1024, true)] {
		let options = options(memtable, vlog);
		let rows = rows_of(600);
		let source = FakeSource::new(rows.clone());
		let raw = MemRaw::new();
		let mounter = MemMounter::new(raw.clone());
		let tuning = ImportTuning {
			read: ChunkLimits::new(256 * 1024, 100),
			write: ChunkLimits::new(128 * 1024, 40),
			commit_pause: std::time::Duration::ZERO,
			abandon_after_chunks: None,
		};

		let (result, report) = call(&source, &mounter, &options, &tuning).await;
		let what = format!("memtable {memtable}, vlog {vlog}");
		assert_eq!(result.unwrap(), imported(&rows), "{what}");
		assert!(report.chunks > 1, "{what}: {report:?}");
		assert!(report.transactions > report.chunks, "{what}: {report:?}");
		assert_database(&raw, &options, &rows, &what).await;
		let held = contents(&raw);
		assert!(!held.files.is_empty(), "{what}: the database has files");
		if vlog {
			assert!(held.files.iter().any(|f| f.contains("vlog")), "{what}: {:?}", held.files);
		}
		assert_eq!(mounter.mounts(), 2, "{what}: one mount to copy and one to check");

		// The second call finds it, and does not look at the source.
		let (reads, probes) = (source.reads(), source.probes());
		let (again, report) = call(&source, &MemMounter::new(raw.clone()), &options, &tuning).await;
		assert_eq!(again.unwrap(), IndexedDbMigration::Existing, "{what}");
		assert_eq!(
			(source.reads(), source.probes()),
			(reads, probes),
			"{what}: the source was used"
		);
		assert_eq!(report, ImportReport::default(), "{what}");
		assert_database(&raw, &options, &rows, &what).await;
	}
}

#[test(tokio::test)]
async fn an_empty_store_is_an_empty_database_and_the_next_call_finds_it() {
	let options = options(4 * 1024, false);
	let source = FakeSource::new(Vec::new());
	let raw = MemRaw::new();

	let result = small_call(&source, &raw, &options).await;
	assert_eq!(
		result.unwrap(),
		IndexedDbMigration::Imported {
			entries: 0,
			bytes: 0,
			skipped: 0
		}
	);
	assert_database(&raw, &options, &Vec::new(), "an empty store").await;
	assert!(
		!contents(&raw).files.is_empty(),
		"an empty database has files, for the next call to find"
	);

	let reads = source.calls();
	let result = small_call(&source, &raw, &options).await;
	assert_eq!(result.unwrap(), IndexedDbMigration::Existing);
	assert_eq!(source.calls(), reads);
}

#[test(tokio::test)]
async fn a_pool_without_a_database_and_without_a_source_stays_without_a_database() {
	let options = options(4 * 1024, false);
	// Nothing is mounted, so nothing is made: one probe of the source.
	let raw = MemRaw::new();
	let source = FakeSource::missing();
	let mounter = MemMounter::new(raw.clone());
	let (result, _) = call(&source, &mounter, &options, &small_tuning()).await;
	assert_eq!(result.unwrap(), IndexedDbMigration::NoSource);
	assert_eq!(mounter.mounts(), 0);
	assert!(raw.names().is_empty(), "{:?}", raw.names());
	assert_eq!((source.probes(), source.reads()), (1, 0));

	// A pool that was mounted before and holds nothing.
	let held = MemRaw::new();
	let (pool, _) = PoolFs::<MemRaw>::mount_sync(held.clone(), PoolConfig::memory()).unwrap();
	FileSystem(pool as Arc<dyn Fs>).close().await.unwrap();
	assert!(!held.names().is_empty());
	let source = FakeSource::missing();
	assert_eq!(small_call(&source, &held, &options).await.unwrap(), IndexedDbMigration::NoSource);
	assert_eq!((source.probes(), source.reads()), (1, 0));
	let held_now = contents(&held);
	assert!(!held_now.marker && held_now.files.is_empty());
}

// ---------------------------------------------------------------------------------------------
// A database that is there
// ---------------------------------------------------------------------------------------------

#[test(tokio::test)]
async fn a_database_that_exists_is_never_copied_over_and_the_source_is_not_looked_at() {
	let options = options(4 * 1024, false);
	// An application that wrote to its database, and a source that holds other rows.
	let mine = small_rows(40);
	let raw = MemRaw::new();
	app_writes(&raw, &options, &mine).await;
	let theirs = FakeSource::new(
		(1000..1060)
			.map(|i| (key_of(i), value_of(i)))
			.chain([(key_of(3), b"other".to_vec())])
			.collect(),
	);

	let result = small_call(&theirs, &raw, &options).await;
	assert_eq!(result.unwrap(), IndexedDbMigration::Existing);
	assert_eq!(theirs.calls(), 0, "the source was looked at");
	assert_database(&raw, &options, &mine, "a database of an application").await;

	// The same with a database that holds nothing, and with a source that is not there.
	let raw = MemRaw::new();
	app_writes(&raw, &options, &Vec::new()).await;
	let result = small_call(&theirs, &raw, &options).await;
	assert_eq!(result.unwrap(), IndexedDbMigration::Existing);
	let result = small_call(&FakeSource::missing(), &raw, &options).await;
	assert_eq!(result.unwrap(), IndexedDbMigration::Existing);
	assert_eq!(theirs.calls(), 0);
	assert_database(&raw, &options, &Vec::new(), "an empty database of an application").await;
}

#[test(tokio::test)]
async fn writes_after_a_copy_are_not_overwritten_by_the_next_call_whatever_the_source_holds() {
	let options = options(4 * 1024, false);
	let rows = small_rows(50);
	let source = FakeSource::new(rows.clone());
	let raw = MemRaw::new();
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), imported(&rows));

	// The application writes, and deletes a row that the copy made.
	let mut now = rows.clone();
	now.retain(|(k, _)| *k != rows[7].0);
	now.push((b"new".to_vec(), b"written after".to_vec()));
	now.sort();
	{
		let (pool, _) = PoolFs::<MemRaw>::mount_sync(raw.clone(), PoolConfig::memory()).unwrap();
		let fs = FileSystem(Arc::clone(&pool) as Arc<dyn Fs>);
		let tree = TreeBuilder::with_options(Options {
			fs: Arc::clone(&fs.0),
			..options.clone()
		})
		.build_async()
		.await
		.unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set(&b"new"[..], &b"written after"[..]).unwrap();
		txn.delete(&rows[7].0[..]).unwrap();
		txn.commit().await.unwrap();
		tree.close().await.unwrap();
		fs.close().await.unwrap();
	}
	// The source is still there, and has what it had, and then something else.
	let reads = source.calls();
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), IndexedDbMigration::Existing);
	source.set_rows(vec![(b"another".to_vec(), b"row".to_vec())]);
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), IndexedDbMigration::Existing);
	assert_eq!(source.calls(), reads);
	assert_database(&raw, &options, &now, "writes after the copy").await;
}

// ---------------------------------------------------------------------------------------------
// A copy that did not finish
// ---------------------------------------------------------------------------------------------

#[test(tokio::test)]
async fn a_copy_that_was_cut_short_is_made_again_in_full() {
	for vlog in [false, true] {
		let options = options(2 * 1024, vlog);
		let rows = small_rows(120);
		let source = FakeSource::new(rows.clone());
		// How many chunks a whole copy has.
		let whole = {
			let raw = MemRaw::new();
			let mounter = MemMounter::new(raw);
			call(&source, &mounter, &options, &small_tuning()).await.1.chunks
		};
		assert!(whole >= 6, "{whole}");

		for stop in 1..whole {
			let raw = MemRaw::new();
			let tuning = ImportTuning {
				abandon_after_chunks: Some(stop),
				..small_tuning()
			};
			let (result, report) =
				call(&source, &MemMounter::new(raw.clone()), &options, &tuning).await;
			let what = format!("vlog {vlog}, stopped after {stop} chunks");
			assert!(matches!(result, Err(Error::MigrationFailed(_))), "{what}: {result:?}");
			assert_eq!(report.chunks, stop, "{what}");
			let left = contents(&raw);
			assert!(left.marker, "{what}: the marker is there");
			assert!(!left.files.is_empty(), "{what}: part of the database is there");

			let result = small_call(&source, &raw, &options).await;
			assert_eq!(result.unwrap(), imported(&rows), "{what}");
			assert_database(&raw, &options, &rows, &what).await;
		}
	}
}

#[test(tokio::test)]
async fn a_source_that_changed_since_the_cut_short_copy_is_copied_as_it_is_now() {
	let options = options(2 * 1024, false);
	let before = small_rows(100);
	let source = FakeSource::new(before.clone());
	let raw = MemRaw::new();
	let tuning = ImportTuning {
		abandon_after_chunks: Some(3),
		..small_tuning()
	};
	let (result, _) = call(&source, &MemMounter::new(raw.clone()), &options, &tuning).await;
	assert!(result.is_err());
	assert!(contents(&raw).marker);

	// Rows that the first run copied are gone, others are new, and one has another value.
	let mut after: Rows = (50..170).map(|i| (key_of(i), value_of(i))).collect();
	after.push((before[0].0.clone(), b"changed".to_vec()));
	after.sort();
	after.dedup_by(|a, b| a.0 == b.0);
	let source = FakeSource::new(after.clone());
	let result = small_call(&source, &raw, &options).await;
	assert_eq!(result.unwrap(), imported(&after));
	assert_database(&raw, &options, &after, "a source that changed").await;
	assert!(
		database(&raw, &options)
			.await
			.unwrap()
			.iter()
			.all(|(k, _)| after.iter().any(|(a, _)| a == k)),
		"nothing of the first run was kept"
	);
}

#[test(tokio::test)]
async fn an_unfinished_copy_with_no_source_fails_and_leaves_no_database() {
	let options = options(2 * 1024, false);
	let rows = small_rows(100);
	let source = FakeSource::new(rows.clone());
	let raw = MemRaw::new();
	let tuning = ImportTuning {
		abandon_after_chunks: Some(4),
		..small_tuning()
	};
	let (result, _) = call(&source, &MemMounter::new(raw.clone()), &options, &tuning).await;
	assert!(result.is_err());
	assert!(!contents(&raw).files.is_empty());

	source.set_present(false);
	for _ in 0..3 {
		let result = small_call(&source, &raw, &options).await;
		let Err(Error::MigrationFailed(message)) = result else {
			panic!("the call must fail with a typed error: {result:?}");
		};
		assert!(message.contains("not finished") && message.contains("not there"), "{message}");
		let left = contents(&raw);
		assert!(left.marker, "the marker stays");
		assert!(left.files.is_empty(), "no file of a database is left: {:?}", left.files);
	}
	// The source comes back, and the copy is made.
	source.set_present(true);
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), imported(&rows));
	assert_database(&raw, &options, &rows, "the source came back").await;
}

// ---------------------------------------------------------------------------------------------
// The verification
// ---------------------------------------------------------------------------------------------

/// After a failure of the verification: the database is gone, the marker is there, and a call with
/// a source that does not mislead makes the database.
async fn assert_wiped_then_copied(raw: &MemRaw, options: &Options, rows: &Rows, what: &str) {
	let left = contents(raw);
	assert!(left.marker, "{what}: the marker stays");
	assert!(left.files.is_empty(), "{what}: the database was removed: {:?}", left.files);
	let source = FakeSource::new(rows.clone());
	let result = small_call(&source, raw, options).await;
	assert_eq!(result.unwrap(), imported(rows), "{what}");
	assert_database(raw, options, rows, what).await;
}

#[test(tokio::test)]
async fn a_source_that_gives_a_row_twice_with_two_values_fails_the_verification() {
	let options = options(2 * 1024, false);
	let rows = small_rows(60);
	// The same key in the first chunk and in a later one, with another value: the database holds
	// the second, and the source gave both.
	let mut served = rows.clone();
	served.push((rows[0].0.clone(), b"a second value for the first key".to_vec()));
	let source = FakeSource::in_order(served);
	let raw = MemRaw::new();
	let result = small_call(&source, &raw, &options).await;
	let Err(Error::MigrationFailed(message)) = result else {
		panic!("the verification must fail the copy: {result:?}");
	};
	assert!(message.contains("61 rows were written") && message.contains("holds 60"), "{message}");
	assert_wiped_then_copied(&raw, &options, &rows, "a key twice").await;
}

/// Changes the database after the copy is closed, as a fault of the storage would: the mount that
/// the verification makes is the second.
async fn tamper_after_copy(rows: Rows, change: Tamper) -> (Result<IndexedDbMigration>, MemRaw) {
	let options = options(2 * 1024, false);
	let source = FakeSource::new(rows);
	let raw = MemRaw::new();
	let mounter = MemMounter::new(raw.clone());
	mounter.tamper_at_mount(2, change, options.clone());
	let (result, _) = call(&source, &mounter, &options, &small_tuning()).await;
	(result, raw)
}

#[test(tokio::test)]
async fn a_database_that_lost_a_row_after_the_copy_fails_the_verification_and_is_removed() {
	let rows = small_rows(60);
	let gone = rows[10].0.clone();
	let (result, raw) = tamper_after_copy(rows.clone(), Tamper::Delete(gone)).await;
	let Err(Error::MigrationFailed(message)) = result else {
		panic!("{result:?}");
	};
	assert!(message.contains("60 rows were written") && message.contains("holds 59"), "{message}");
	assert_wiped_then_copied(&raw, &options(2 * 1024, false), &rows, "a row lost").await;
}

#[test(tokio::test)]
async fn a_database_with_a_row_more_than_the_source_fails_the_verification_and_is_removed() {
	let rows = small_rows(60);
	let (result, raw) =
		tamper_after_copy(rows.clone(), Tamper::Set(b"an extra row".to_vec(), b"v".to_vec())).await;
	let Err(Error::MigrationFailed(message)) = result else {
		panic!("{result:?}");
	};
	assert!(message.contains("60 rows were written") && message.contains("holds 61"), "{message}");
	assert_wiped_then_copied(&raw, &options(2 * 1024, false), &rows, "a row more").await;
}

#[test(tokio::test)]
async fn a_database_with_another_value_of_the_same_length_fails_on_the_digest() {
	let rows = small_rows(60);
	let (key, value) = rows[20].clone();
	assert!(!value.is_empty());
	let mut other = value.clone();
	other[0] ^= 0xff;
	let (result, raw) = tamper_after_copy(rows.clone(), Tamper::Set(key, other)).await;
	let Err(Error::MigrationFailed(message)) = result else {
		panic!("{result:?}");
	};
	assert!(message.contains("contents differ") && message.contains("60 rows"), "{message}");
	assert_wiped_then_copied(&raw, &options(2 * 1024, false), &rows, "another value").await;
}

// ---------------------------------------------------------------------------------------------
// Rows that the engine refuses
// ---------------------------------------------------------------------------------------------

/// The value of a row that the engine refuses: something that a log must not show.
const REFUSED_VALUE: &[u8] = b"a value that is never logged";

/// `good` with a row that has an empty key at each of `positions` of the sequence that results:
/// the rows that the engine takes, in the order they were in, with those that it refuses among
/// them.
fn with_refused_at(good: &Rows, positions: &[usize]) -> Rows {
	let mut rows = Rows::new();
	let mut next = good.iter();
	for at in 0..good.len() + positions.len() {
		if positions.contains(&at) {
			rows.push((Vec::new(), REFUSED_VALUE.to_vec()));
		} else {
			rows.push(next.next().expect("a row for every position").clone());
		}
	}
	assert!(next.next().is_none());
	rows
}

/// A tuning that cuts the source into chunks of four rows and the chunks into transactions of
/// three.
fn tuning_of_four_and_three() -> ImportTuning {
	ImportTuning {
		read: ChunkLimits::new(usize::MAX, 4),
		write: ChunkLimits::new(usize::MAX, 3),
		commit_pause: std::time::Duration::ZERO,
		abandon_after_chunks: None,
	}
}

#[test(tokio::test)]
async fn a_row_with_an_empty_key_is_skipped_and_counted_and_the_other_rows_are_copied() {
	let options = options(2 * 1024, false);
	let good = small_rows(30);
	let source = FakeSource::in_order(with_refused_at(&good, &[7]));
	let raw = MemRaw::new();

	let result = small_call(&source, &raw, &options).await;
	assert_eq!(result.unwrap(), imported_skipping(&good, 1));
	assert_database(&raw, &options, &good, "a row with an empty key").await;

	// The database is there, and the next call finds it and does not look at the source.
	let calls = source.calls();
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), IndexedDbMigration::Existing);
	assert_eq!(source.calls(), calls);
}

#[test(tokio::test)]
async fn rows_that_the_engine_refuses_are_skipped_wherever_they_are_in_the_source() {
	let options = options(64 * 1024, false);
	// Chunks of four rows, in transactions of three: the positions below are the edges of both.
	let cases: Vec<(&str, usize, Vec<usize>)> = vec![
		("the first row", 10, vec![0]),
		("the last row", 10, vec![10]),
		("a row in the middle", 10, vec![5]),
		("several rows in a row", 10, vec![4, 5, 6]),
		("the two ends of a chunk and the two ends of the next", 12, vec![3, 4, 7, 8]),
		("the last of a chunk and the first of the next", 10, vec![3, 4]),
		("every row of a transaction", 9, vec![0, 1, 2]),
		("every row of a chunk, in the middle", 12, vec![4, 5, 6, 7]),
		("every other row", 10, vec![1, 3, 5, 7, 9]),
		("the first and the last", 10, vec![0, 11]),
		("a row alone", 0, vec![0]),
		("rows alone, more than a chunk of them", 0, vec![0, 1, 2, 3, 4, 5, 6]),
	];
	for (what, count, positions) in cases {
		let good = small_rows(count);
		let served = with_refused_at(&good, &positions);
		let source = FakeSource::in_order(served.clone());
		let raw = MemRaw::new();
		let (result, report) =
			call(&source, &MemMounter::new(raw.clone()), &options, &tuning_of_four_and_three())
				.await;
		assert_eq!(result.unwrap(), imported_skipping(&good, positions.len() as u64), "{what}");
		assert_database(&raw, &options, &good, what).await;

		// A transaction is committed if the engine took a row of it: one whose rows were all
		// refused has nothing to commit.
		let mut transactions = 0;
		for chunk in served.chunks(4) {
			transactions +=
				chunk.chunks(3).filter(|piece| piece.iter().any(|(k, _)| !k.is_empty())).count();
		}
		assert_eq!(report.transactions, transactions, "{what}: transactions");

		// The same source, copied again after the database was removed, is the same copy.
		assert_eq!(
			small_call(&source, &raw, &options).await.unwrap(),
			IndexedDbMigration::Existing,
			"{what}"
		);
	}
}

#[test(tokio::test)]
async fn a_source_of_rows_that_the_engine_refuses_only_is_an_empty_database() {
	let options = options(2 * 1024, false);
	for count in [1usize, 3, 25] {
		let source =
			FakeSource::in_order(with_refused_at(&Vec::new(), &(0..count).collect::<Vec<_>>()));
		let raw = MemRaw::new();
		let result = small_call(&source, &raw, &options).await;
		assert_eq!(
			result.unwrap(),
			IndexedDbMigration::Imported {
				entries: 0,
				bytes: 0,
				skipped: count as u64
			},
			"{count} rows"
		);
		assert_database(&raw, &options, &Vec::new(), "only refused rows").await;
		assert!(!contents(&raw).files.is_empty(), "an empty database has files, to be found");
		assert_eq!(
			small_call(&source, &raw, &options).await.unwrap(),
			IndexedDbMigration::Existing
		);
	}
}

/// Collects what is logged on this thread while it is held, with the level.
#[derive(Clone, Default)]
struct Logged(Arc<Mutex<Vec<(tracing::Level, String)>>>);

impl tracing::Subscriber for Logged {
	fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
		true
	}

	fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
		tracing::span::Id::from_u64(1)
	}

	fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

	fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

	fn event(&self, event: &tracing::Event<'_>) {
		struct Message(String);
		impl tracing::field::Visit for Message {
			fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
				if field.name() == "message" {
					self.0 = format!("{value:?}");
				}
			}
		}
		let mut message = Message(String::new());
		event.record(&mut message);
		self.0.lock().unwrap().push((*event.metadata().level(), message.0));
	}

	fn enter(&self, _: &tracing::span::Id) {}

	fn exit(&self, _: &tracing::span::Id) {}
}

#[test(tokio::test)]
async fn a_row_that_is_skipped_is_logged_once_at_warn_with_the_length_of_its_key_only() {
	let options = options(2 * 1024, false);
	let good = small_rows(20);
	let logged = Logged::default();
	let _guard = tracing::subscriber::set_default(logged.clone());
	let source = FakeSource::in_order(with_refused_at(&good, &[0, 9, 20]));
	let raw = MemRaw::new();
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), imported_skipping(&good, 3));

	let skipped: Vec<_> = logged
		.0
		.lock()
		.unwrap()
		.iter()
		.filter(|(_, message)| message.contains("not copied"))
		.cloned()
		.collect();
	assert_eq!(skipped.len(), 3, "once for each row: {skipped:?}");
	for (level, message) in &skipped {
		assert_eq!(*level, tracing::Level::WARN);
		assert!(message.contains("its key is 0 bytes long"), "{message}");
		assert!(message.contains("empty"), "the reason: {message}");
		assert!(!message.contains("never logged"), "the value is not in the log: {message}");
	}
}

#[test]
fn the_engine_refuses_a_row_for_an_empty_key_and_for_its_size_and_for_nothing_that_is_not_the_row()
{
	assert_eq!(row_refusal(&Error::EmptyKey), Some(Refusal::EmptyKey));
	assert_eq!(row_refusal(&Error::BatchTooLarge), Some(Refusal::TooLarge));
	// What is not about the row fails the copy.
	let io = Error::Io(Arc::new(std::io::Error::other("a disk")));
	let full =
		Error::Io(Arc::new(std::io::Error::new(std::io::ErrorKind::StorageFull, "the quota")));
	for error in [
		io,
		full,
		Error::Abort,
		Error::FilePoolExhausted,
		Error::PipelineStall,
		Error::ArenaFull,
		Error::TransactionClosed,
		Error::TransactionWriteConflict,
		Error::DatabaseStopped("stopped".to_string()),
		Error::InvalidArgument("an argument".to_string()),
		Error::Other("another".to_string()),
		Error::Corruption("a block".to_string()),
		Error::MigrationFailed("a lock".to_string()),
		Error::CommitFail("a commit".to_string()),
	] {
		assert_eq!(row_refusal(&error), None, "{error:?}");
	}
}

#[test(tokio::test)]
async fn the_engine_refuses_an_empty_key_as_the_copy_expects_and_takes_keys_and_values_of_any_size()
{
	let options = options(64 * 1024, true);
	let (pool, _) = PoolFs::<MemRaw>::mount_sync(MemRaw::new(), PoolConfig::memory()).unwrap();
	let fs = FileSystem(Arc::clone(&pool) as Arc<dyn Fs>);
	let tree = TreeBuilder::with_options(Options {
		fs: Arc::clone(&fs.0),
		..options.clone()
	})
	.build_async()
	.await
	.unwrap();
	// What the copy relies on: the refusal is at `set`, with this error, and the transaction goes
	// on.
	let mut txn = tree.begin().unwrap();
	let refused = txn.set(&b""[..], &b"v"[..]).expect_err("an empty key is refused");
	assert_eq!(row_refusal(&refused), Some(Refusal::EmptyKey), "{refused:?}");
	txn.set(&b"k"[..], &b"v"[..]).expect("the transaction takes the next row");
	txn.commit().await.unwrap();
	tree.close().await.unwrap();
	fs.close().await.unwrap();

	// Keys and values of every size that a browser holds are taken, so nothing else is skipped: a
	// key longer than a `u16` can count, a megabyte, and a value of several.
	let odd: Rows = vec![
		(vec![0], Vec::new()),
		(vec![0xff; 300], vec![1]),
		(vec![7; 70_000], vec![2; 10]),
		(vec![8; 1_000_000], Vec::new()),
		(vec![9; 100], vec![3; 5_000_000]),
	];
	let mut wanted = odd.clone();
	wanted.sort();
	let source = FakeSource::in_order(odd);
	let raw = MemRaw::new();
	let result = small_call(&source, &raw, &options).await;
	assert_eq!(result.unwrap(), imported(&wanted));
	assert_database(&raw, &options, &wanted, "keys and values of every size").await;
}

/// How far a sweep of a write that fails goes: past the last write of a copy of the rows of the
/// test, which is about a hundred.
const WRITES_OF_A_SMALL_COPY: usize = 400;

#[test(tokio::test)]
async fn a_commit_that_fails_for_one_row_is_a_failure_of_the_copy_and_never_a_skipped_row() {
	// A commit of one row that is refused for the size of the row is that row's refusal. A commit
	// of one row that fails for any other reason (a write that fails, which the quota of the
	// browser does too) is not: if it were taken for one, rows would be dropped, and the call
	// would say that it copied the rest. Transactions of one row make every commit one of
	// those, and a write fails once at each point of the call in turn.
	let options = options(64 * 1024, false);
	let rows = small_rows(60);
	let tuning = ImportTuning {
		read: ChunkLimits::new(usize::MAX, 9),
		write: ChunkLimits::new(usize::MAX, 1),
		commit_pause: std::time::Duration::ZERO,
		abandon_after_chunks: None,
	};
	let (mut complete, mut in_a_commit, mut elsewhere) = (0, 0, 0);
	let mut dropped_rows = Vec::new();
	let mut last = None;
	for at in 0..WRITES_OF_A_SMALL_COPY {
		let raw = MemRaw::new();
		raw.fail_write_after(at);
		let source = FakeSource::new(rows.clone());
		let (result, _) = call(&source, &MemMounter::new(raw.clone()), &options, &tuning).await;
		last = Some(result.is_ok());
		match result {
			Ok(done) if done == imported(&rows) => complete += 1,
			Ok(done) => dropped_rows.push((at, format!("{done:?}"))),
			Err(Error::MigrationFailed(message)) if message.contains("writing rows") => {
				in_a_commit += 1;
			}
			Err(Error::MigrationFailed(_)) => elsewhere += 1,
			Err(other) => dropped_rows.push((at, format!("not a failure of the copy: {other:?}"))),
		}
	}
	assert!(
		dropped_rows.is_empty(),
		"a write that failed once was taken for a row that is skipped, or ended the call another way: {dropped_rows:?}"
	);
	// What the sweep shows is what it reached: the commits, and the rest of the call, and the end
	// of the call, past which a write that fails is never met.
	assert!(in_a_commit > 0, "no write that failed was met by a commit of one row");
	assert!(elsewhere > 0, "no write that failed was met outside a commit");
	assert!(complete > 0 && last == Some(true), "the sweep did not go past the last write");
}

#[test]
fn the_account_of_a_copy_must_add_up_to_the_rows_that_were_read() {
	let rows: [(&[u8], &[u8]); 3] = [(b"a", b"1"), (b"b", b"22"), (b"c", b"")];
	let account = |read: u64, skipped: u64| Counted {
		read,
		skipped,
		written: tallied(&rows),
	};
	check_account(&account(3, 0)).expect("every row was written");
	check_account(&account(5, 2)).expect("two rows were skipped");
	let message = |counted: Counted| match check_account(&counted) {
		Err(Error::MigrationFailed(message)) => message,
		other => panic!("{other:?}"),
	};
	// A lie about the number of rows that were skipped, either way, and a row that was lost.
	let fewer = message(account(5, 1));
	assert!(
		fewer.contains("5 rows were read")
			&& fewer.contains("3 were written")
			&& fewer.contains("1 skipped"),
		"{fewer}"
	);
	let more = message(account(5, 3));
	assert!(more.contains("3 were written") && more.contains("3 skipped"), "{more}");
	assert!(message(account(2, 0)).contains("2 rows were read"));
	assert!(message(account(3, u64::MAX)).contains("rows were read"), "no overflow");
}

#[test(tokio::test)]
async fn a_copy_with_skipped_rows_is_checked_against_the_rows_that_were_written() {
	let options = options(2 * 1024, false);
	let good = small_rows(60);
	let served = with_refused_at(&good, &[0, 30, 61]);

	// A row lost after the copy, and a row more: the message names what was written.
	for (change, message_has) in [
		(Tamper::Delete(good[10].0.clone()), ["60 rows were written", "holds 59"]),
		(
			Tamper::Set(b"an extra row".to_vec(), b"v".to_vec()),
			["60 rows were written", "holds 61"],
		),
	] {
		let source = FakeSource::in_order(served.clone());
		let raw = MemRaw::new();
		let mounter = MemMounter::new(raw.clone());
		mounter.tamper_at_mount(2, change, options.clone());
		let (result, _) = call(&source, &mounter, &options, &small_tuning()).await;
		let Err(Error::MigrationFailed(message)) = result else {
			panic!("{result:?}");
		};
		for wanted in message_has {
			assert!(message.contains(wanted), "{message}");
		}
		// The database is removed, and the call that follows makes it, with the same skipped rows.
		let left = contents(&raw);
		assert!(left.marker && left.files.is_empty(), "{:?}", left.files);
		let source = FakeSource::in_order(served.clone());
		assert_eq!(small_call(&source, &raw, &options).await.unwrap(), imported_skipping(&good, 3));
		assert_database(&raw, &options, &good, "after the verification failed").await;
	}

	// Another value of the same length.
	let (key, value) = good[20].clone();
	let mut other = value.clone();
	other[0] ^= 0xff;
	let source = FakeSource::in_order(served.clone());
	let raw = MemRaw::new();
	let mounter = MemMounter::new(raw.clone());
	mounter.tamper_at_mount(2, Tamper::Set(key, other), options.clone());
	let (result, _) = call(&source, &mounter, &options, &small_tuning()).await;
	let Err(Error::MigrationFailed(message)) = result else {
		panic!("{result:?}");
	};
	assert!(message.contains("contents differ") && message.contains("60 rows"), "{message}");
}

#[test(tokio::test)]
async fn two_keys_that_are_the_same_bytes_still_fail_the_copy_when_rows_are_skipped_too() {
	let options = options(2 * 1024, false);
	let good = small_rows(30);
	// A string key and a binary key of the same bytes come out of a store as two rows of one key,
	// and one of them would replace the other: the copy fails, and the skipped rows do not hide it.
	let mut served = with_refused_at(&good, &[0, 12]);
	served.push((good[3].0.clone(), b"the other row of that key".to_vec()));
	let source = FakeSource::in_order(served);
	let raw = MemRaw::new();
	let result = small_call(&source, &raw, &options).await;
	let Err(Error::MigrationFailed(message)) = result else {
		panic!("{result:?}");
	};
	assert!(message.contains("rows were written") && message.contains("same key"), "{message}");
	let left = contents(&raw);
	assert!(left.marker && left.files.is_empty(), "{:?}", left.files);

	// Two empty keys, a string and a binary one, are both refused: neither replaces the other, and
	// both are skipped.
	let source = FakeSource::in_order(with_refused_at(&good, &[0, 1]));
	let raw = MemRaw::new();
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), imported_skipping(&good, 2));
}

/// Several rows that the engine refuses, among them the first, in the middle, in a row, and the
/// last: in the order that the source gives them.
fn refused(vlog: bool) -> Shape {
	let good = small_rows(12);
	Shape {
		name: if vlog {
			"refused rows, with the value log"
		} else {
			"refused rows"
		},
		rows: with_refused_at(&good, &[0, 4, 5, 10, 14]),
		options: options(1024, vlog),
		tuning: small_tuning(),
	}
}

#[test(tokio::test)]
async fn a_crash_at_every_operation_of_a_copy_with_skipped_rows_is_made_a_database_of_with_the_same_rows_skipped(
) {
	for vlog in [false, true] {
		let shape = refused(vlog);
		assert_eq!(shape.imported(), imported_skipping(&small_rows(12), 5));
		let marks = marks_of(&shape).await;
		let stride = if vlog {
			2
		} else {
			1
		};
		let (tally, _) = sweep(&shape, &marks, &every(&marks, stride), true, 8).await;
		assert_covered(&tally, &marks, shape.name);
	}
}

#[test(tokio::test)]
async fn a_copy_that_was_cut_short_with_skipped_rows_skips_them_again_when_it_is_made_again() {
	let shape = refused(false);
	let source = shape.source();
	let whole = {
		let raw = MemRaw::new();
		call(&source, &MemMounter::new(raw), &shape.options, &shape.tuning).await.1.chunks
	};
	assert!(whole >= 2, "{whole}");
	for stop in 1..whole {
		let raw = MemRaw::new();
		let tuning = ImportTuning {
			abandon_after_chunks: Some(stop),
			..shape.tuning
		};
		let (result, _) =
			call(&source, &MemMounter::new(raw.clone()), &shape.options, &tuning).await;
		assert!(
			matches!(result, Err(Error::MigrationFailed(_))),
			"stopped after {stop}: {result:?}"
		);
		let result =
			call(&source, &MemMounter::new(raw.clone()), &shape.options, &shape.tuning).await;
		assert_eq!(result.0.unwrap(), shape.imported(), "stopped after {stop}");
		assert_database(&raw, &shape.options, &shape.written(), "made again").await;
	}
}

// ---------------------------------------------------------------------------------------------
// A context that has no IndexedDB
// ---------------------------------------------------------------------------------------------

#[test(tokio::test)]
async fn without_indexeddb_and_without_a_database_the_call_is_no_source_and_nothing_is_made() {
	let options = options(4 * 1024, false);

	// No pool at all: nothing is mounted, so nothing is made.
	let raw = MemRaw::new();
	let source = FakeSource::unavailable();
	let mounter = MemMounter::new(raw.clone());
	let (result, report) = call(&source, &mounter, &options, &small_tuning()).await;
	assert_eq!(result.unwrap(), IndexedDbMigration::NoSource);
	assert_eq!(report, ImportReport::default());
	assert_eq!(mounter.mounts(), 0);
	assert!(raw.names().is_empty(), "{:?}", raw.names());
	assert_eq!((source.probes(), source.reads()), (1, 0));

	// A pool that was mounted before and holds nothing: no marker, no file of a database.
	let held = MemRaw::new();
	let (pool, _) = PoolFs::<MemRaw>::mount_sync(held.clone(), PoolConfig::memory()).unwrap();
	FileSystem(pool as Arc<dyn Fs>).close().await.unwrap();
	let source = FakeSource::unavailable();
	assert_eq!(small_call(&source, &held, &options).await.unwrap(), IndexedDbMigration::NoSource);
	assert_eq!((source.probes(), source.reads()), (1, 0));
	let now = contents(&held);
	assert!(!now.marker && now.files.is_empty(), "{:?}", now.files);

	// And the application, which starts with a new database, finds it at its next start, as it
	// does after any other start: the call that follows is `Existing`, with IndexedDB back or not.
	app_writes(&held, &options, &small_rows(3)).await;
	for unavailable in [true, false] {
		let source = FakeSource::new(small_rows(9));
		source.set_unavailable(unavailable);
		assert_eq!(
			small_call(&source, &held, &options).await.unwrap(),
			IndexedDbMigration::Existing
		);
		assert_eq!(source.calls(), 0, "the source is not asked for a database that exists");
	}
}

#[test(tokio::test)]
async fn with_an_unfinished_copy_and_without_indexeddb_the_call_fails_and_no_database_is_made() {
	let options = options(2 * 1024, false);
	let rows = small_rows(100);
	let source = FakeSource::new(rows.clone());
	let raw = MemRaw::new();
	let tuning = ImportTuning {
		abandon_after_chunks: Some(4),
		..small_tuning()
	};
	let (result, _) = call(&source, &MemMounter::new(raw.clone()), &options, &tuning).await;
	assert!(result.is_err());
	assert!(!contents(&raw).files.is_empty());

	// IndexedDB is not available at the next start: the partial files are removed, the marker
	// stays, and the error says that the copy cannot be made again for that reason.
	source.set_unavailable(true);
	for _ in 0..3 {
		let result = small_call(&source, &raw, &options).await;
		let Err(Error::MigrationFailed(message)) = result else {
			panic!("the call must fail with a typed error: {result:?}");
		};
		assert!(
			message.contains("cannot be made again")
				&& message.contains("IndexedDB is not available"),
			"{message}"
		);
		assert!(!message.contains("is not there to be copied"), "{message}");
		let left = contents(&raw);
		assert!(left.marker, "the marker stays");
		assert!(left.files.is_empty(), "no file of a database is left: {:?}", left.files);
	}
	// No database opens from what is there.
	assert!(database(&raw, &options).await.is_none());

	// IndexedDB is back, and the copy is made.
	source.set_unavailable(false);
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), imported(&rows));
	assert_database(&raw, &options, &rows, "IndexedDB came back").await;
}

#[test(tokio::test)]
async fn a_failure_of_the_source_is_never_taken_for_there_being_no_indexeddb() {
	let options = options(2 * 1024, false);
	let rows = small_rows(30);

	// The probe fails with no pool: the call fails, and nothing is mounted or made.
	let raw = MemRaw::new();
	let source = FakeSource::new(rows.clone());
	source.fail_probe_at(1);
	let mounter = MemMounter::new(raw.clone());
	let (result, _) = call(&source, &mounter, &options, &small_tuning()).await;
	assert!(
		matches!(result, Err(Error::MigrationFailed(ref m)) if m.contains("the probe of the source failed")),
		"{result:?}"
	);
	assert_eq!(mounter.mounts(), 0);
	assert!(raw.names().is_empty(), "{:?}", raw.names());
	// The next start is not the one that skipped the move: it makes it.
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), imported(&rows));
	assert_database(&raw, &options, &rows, "the call after a failed probe").await;

	// The same in a pool that was made and holds nothing: there the first look is the second one.
	let held = MemRaw::new();
	let (pool, _) = PoolFs::<MemRaw>::mount_sync(held.clone(), PoolConfig::memory()).unwrap();
	FileSystem(pool as Arc<dyn Fs>).close().await.unwrap();
	let source = FakeSource::new(rows.clone());
	source.fail_probe_at(1);
	assert!(matches!(
		small_call(&source, &held, &options).await,
		Err(Error::MigrationFailed(ref m)) if m.contains("the probe of the source failed")
	));
	let now = contents(&held);
	assert!(!now.marker && now.files.is_empty(), "{:?}", now.files);
	assert_eq!(small_call(&source, &held, &options).await.unwrap(), imported(&rows));

	// A failure of the look after the mount, when the look before it found the source.
	let raw = MemRaw::new();
	let source = FakeSource::new(rows.clone());
	source.fail_probe_at(2);
	assert!(matches!(
		small_call(&source, &raw, &options).await,
		Err(Error::MigrationFailed(ref m)) if m.contains("the probe of the source failed")
	));
	let now = contents(&raw);
	assert!(!now.marker && now.files.is_empty(), "{:?}", now.files);
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), imported(&rows));

	// With a marker, the failure is a failure too, and no database is made.
	let raw = MemRaw::new();
	let source = FakeSource::new(rows.clone());
	let tuning = ImportTuning {
		abandon_after_chunks: Some(2),
		..small_tuning()
	};
	let (result, _) = call(&source, &MemMounter::new(raw.clone()), &options, &tuning).await;
	assert!(result.is_err());
	source.fail_probe_at(source.probes() + 1);
	assert!(matches!(
		small_call(&source, &raw, &options).await,
		Err(Error::MigrationFailed(ref m)) if m.contains("the probe of the source failed")
	));
	assert!(contents(&raw).marker);
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), imported(&rows));
}

#[test(tokio::test)]
async fn indexeddb_that_goes_away_while_the_call_runs_is_a_failure_and_not_a_new_database() {
	let options = options(2 * 1024, false);
	let rows = small_rows(30);
	let raw = MemRaw::new();
	let source = FakeSource::new(rows.clone());
	// There when the call looks first, not there when it looks again after the mount.
	source.unavailable_after(1);
	let result = small_call(&source, &raw, &options).await;
	let Err(Error::MigrationFailed(message)) = result else {
		panic!("{result:?}");
	};
	assert!(message.contains("not any more"), "{message}");
	let now = contents(&raw);
	assert!(!now.marker && now.files.is_empty(), "{:?}", now.files);

	// The next call finds it again, and copies.
	let source = FakeSource::new(rows.clone());
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), imported(&rows));
	assert_database(&raw, &options, &rows, "after IndexedDB came back").await;
}

// ---------------------------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------------------------

#[test(tokio::test)]
async fn an_error_of_the_source_is_a_migration_error_and_the_next_call_starts_again() {
	let options = options(2 * 1024, false);
	let rows = small_rows(80);
	let source = FakeSource::new(rows.clone());
	let raw = MemRaw::new();
	source.fail_at(3);
	let result = small_call(&source, &raw, &options).await;
	assert!(
		matches!(result, Err(Error::MigrationFailed(ref m)) if m.contains("the source failed")),
		"{result:?}"
	);
	let left = contents(&raw);
	assert!(left.marker);
	assert!(left.files.is_empty(), "the files of the copy were removed: {:?}", left.files);

	// A source that is there for the probe and gone when it is read.
	let raw = MemRaw::new();
	let source = FakeSource::new(rows.clone());
	source.vanish_after(2);
	let result = small_call(&source, &raw, &options).await;
	assert!(
		matches!(result, Err(Error::MigrationFailed(ref m)) if m.contains("went away")),
		"{result:?}"
	);
	let left = contents(&raw);
	assert!(left.marker);
	assert!(left.files.is_empty(), "the files of the copy were removed: {:?}", left.files);

	// And a source that is there again.
	source.set_present(true);
	assert_eq!(
		small_call(&FakeSource::new(rows.clone()), &raw, &options).await.unwrap(),
		imported(&rows)
	);
	assert_database(&raw, &options, &rows, "after errors of the source").await;
}

#[test(tokio::test)]
async fn a_lock_held_by_another_page_is_the_typed_error_wherever_the_mount_meets_it() {
	let options = options(2 * 1024, false);
	let rows = small_rows(40);
	let source = FakeSource::new(rows.clone());

	// The first mount, in a pool that is not there, and in one that is.
	for existing in [false, true] {
		let raw = MemRaw::new();
		if existing {
			let (pool, _) =
				PoolFs::<MemRaw>::mount_sync(raw.clone(), PoolConfig::memory()).unwrap();
			FileSystem(pool as Arc<dyn Fs>).close().await.unwrap();
		}
		let mounter = MemMounter::new(raw.clone());
		mounter.fail_mount(1, locked());
		let (result, _) = call(&source, &mounter, &options, &small_tuning()).await;
		let error = result.unwrap_err();
		assert!(error.is_database_locked(), "{error:?}");
		assert!(!matches!(error, Error::MigrationFailed(_)));
		assert!(!contents(&raw).marker && contents(&raw).files.is_empty());
	}

	// The mount of the verification: the copy is there, and is not counted.
	let raw = MemRaw::new();
	let mounter = MemMounter::new(raw.clone());
	mounter.fail_mount(2, locked());
	let (result, _) = call(&source, &mounter, &options, &small_tuning()).await;
	let error = result.unwrap_err();
	assert!(error.is_database_locked(), "{error:?}");
	assert!(contents(&raw).marker, "the copy was not committed");
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), imported(&rows));

	// A pool that another holder has: the lock of the memory pool is exclusive too.
	let raw = MemRaw::new();
	let (pool, _) = PoolFs::<MemRaw>::mount_sync(raw.clone(), PoolConfig::memory()).unwrap();
	let holder = FileSystem(pool as Arc<dyn Fs>);
	let error = small_call(&source, &raw, &options).await.unwrap_err();
	assert!(error.is_database_locked(), "{error:?}");
	holder.close().await.unwrap();
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), imported(&rows));

	// Any other failure to mount is a migration error.
	let raw = MemRaw::new();
	let mounter = MemMounter::new(raw.clone());
	mounter.fail_mount(1, Error::Io(Arc::new(std::io::Error::other("no storage"))));
	let (result, _) = call(&source, &mounter, &options, &small_tuning()).await;
	assert!(
		matches!(result, Err(Error::MigrationFailed(ref m)) if m.contains("no storage")),
		"{result:?}"
	);
}

#[test(tokio::test)]
async fn the_path_of_the_database_cannot_be_the_root_of_the_pool_or_the_marker() {
	let rows = small_rows(5);
	let source = FakeSource::new(rows);
	for path in ["", ".", "/", MARKER] {
		let options = Options {
			path: path.into(),
			..options(2 * 1024, false)
		};
		let raw = MemRaw::new();
		let result = small_call(&source, &raw, &options).await;
		assert!(matches!(result, Err(Error::MigrationFailed(_))), "{path:?}: {result:?}");
		assert!(raw.names().is_empty(), "{path:?}: nothing is made");
	}
	assert_eq!(source.calls(), 0);
}

// ---------------------------------------------------------------------------------------------
// The sizes of the pieces
// ---------------------------------------------------------------------------------------------

/// The chunks of the rows by weight, as the rule says, by hand.
fn model_chunks(weights: &[usize], limits: ChunkLimits) -> Vec<usize> {
	let mut chunks = Vec::new();
	let mut at = 0;
	while at < weights.len() {
		let (mut count, mut bytes) = (0usize, 0usize);
		while at < weights.len()
			&& (count == 0
				|| (count < limits.max_entries && bytes + weights[at] <= limits.max_bytes))
		{
			count += 1;
			bytes += weights[at];
			at += 1;
			if count >= limits.max_entries || bytes >= limits.max_bytes {
				break;
			}
		}
		chunks.push(count);
	}
	chunks
}

/// Rows whose keys and values weigh as `weights` say (a key of eight bytes, a value of the rest).
fn weighted(weights: &[usize]) -> Rows {
	weights
		.iter()
		.enumerate()
		.map(|(i, w)| {
			assert!(*w >= 8);
			((i as u64).to_be_bytes().to_vec(), vec![i as u8; w - 8])
		})
		.collect()
}

#[test(tokio::test)]
async fn the_copy_is_right_at_every_edge_of_a_chunk_and_of_a_transaction() {
	let options = options(64 * 1024, false);
	let limit = 400;
	let cases: Vec<(&str, Vec<usize>, ChunkLimits, ChunkLimits)> = vec![
		(
			"chunks of one entry",
			vec![40; 12],
			ChunkLimits::new(usize::MAX, 1),
			ChunkLimits::new(usize::MAX, 1),
		),
		(
			"chunks of two entries",
			vec![40; 11],
			ChunkLimits::new(usize::MAX, 2),
			ChunkLimits::new(usize::MAX, 2),
		),
		(
			"exactly the limit",
			vec![100; 8],
			ChunkLimits::new(limit, 100),
			ChunkLimits::new(limit, 100),
		),
		(
			"the limit and a byte",
			vec![100, 100, 100, 101, 50],
			ChunkLimits::new(limit, 100),
			ChunkLimits::new(limit, 100),
		),
		(
			"a byte under the limit",
			vec![100, 100, 100, 99, 50],
			ChunkLimits::new(limit, 100),
			ChunkLimits::new(limit, 100),
		),
		(
			"an entry larger than the limit",
			vec![50, 2_000, 50, 50, 3_000, 3_000, 50],
			ChunkLimits::new(limit, 100),
			ChunkLimits::new(limit, 100),
		),
		(
			"an entry exactly the limit",
			vec![400, 400, 10, 400],
			ChunkLimits::new(limit, 100),
			ChunkLimits::new(limit, 100),
		),
		("one row", vec![100], ChunkLimits::new(limit, 100), ChunkLimits::new(limit, 100)),
		(
			"transactions smaller than chunks",
			vec![60; 20],
			ChunkLimits::new(600, 100),
			ChunkLimits::new(130, 100),
		),
		(
			"transactions larger than chunks",
			vec![60; 20],
			ChunkLimits::new(130, 100),
			ChunkLimits::new(10_000, 3),
		),
	];
	for (name, weights, read, write) in cases {
		let rows = weighted(&weights);
		let source = FakeSource::new(rows.clone());
		let raw = MemRaw::new();
		let tuning = ImportTuning {
			read,
			write,
			commit_pause: std::time::Duration::ZERO,
			abandon_after_chunks: None,
		};
		let (result, report) =
			call(&source, &MemMounter::new(raw.clone()), &options, &tuning).await;
		assert_eq!(result.unwrap(), imported(&rows), "{name}");

		// The chunks that the rule makes, and the empty one that tells the end of a store that a
		// full chunk ended on.
		let chunks = model_chunks(&weights, read);
		let last_is_full = chunks.last().is_some_and(|n| {
			let tail = &weights[weights.len() - n..];
			*n >= read.max_entries || tail.iter().sum::<usize>() >= read.max_bytes
		});
		assert_eq!(report.chunks, chunks.len() + usize::from(last_is_full), "{name}: chunks");
		let mut transactions = 0;
		let mut at = 0;
		for n in &chunks {
			transactions += model_chunks(&weights[at..at + n], write).len();
			at += n;
		}
		assert_eq!(report.transactions, transactions, "{name}: transactions");
		assert_database(&raw, &options, &rows, name).await;
	}
}

// ---------------------------------------------------------------------------------------------
// The marker is made durable first, and the data before it goes
// ---------------------------------------------------------------------------------------------

#[test(tokio::test)]
async fn the_marker_is_durable_before_any_other_file_exists_and_all_is_durable_before_it_goes() {
	let options = options(2 * 1024, true);
	let rows = small_rows(120);
	let source = FakeSource::new(rows.clone());
	let raw = MemRaw::new();
	let mounter = MemMounter::new(raw.clone());
	// What the hooks see is kept for the test to check: a panic in a hook would be one in a task
	// of the engine, and would not fail the test.
	let trace: Arc<Mutex<Vec<String>>> = Arc::default();
	let violations: Arc<Mutex<Vec<String>>> = Arc::default();
	mounter.on_probe({
		let (raw, trace, violations) = (raw.clone(), Arc::clone(&trace), Arc::clone(&violations));
		move |probe| {
			let (raw, trace, violations) =
				(raw.clone(), Arc::clone(&trace), Arc::clone(&violations));
			probe.before(move |call| {
				let marker = call.path() == Path::new(MARKER);
				let kind = match call {
					Call::CreateNew(_) if marker => "create marker",
					Call::SyncFile(_) if marker => "sync marker",
					Call::RemoveFile(_) if marker => {
						// Every file of the database was flushed already: nothing that a crash
						// could still lose is left.
						if raw.unflushed_ops() != 0 {
							violations.lock().unwrap().push(format!(
								"{} operations are not durable when the marker is removed",
								raw.unflushed_ops()
							));
						}
						"remove marker"
					}
					Call::CreateNew(_)
					| Call::OpenAppend(_)
					| Call::CreateTruncate(..)
					| Call::ReplaceAtomic(_) => {
						// The marker is durable by now: a crash that loses every unflushed
						// operation still finds it.
						let image = raw.crash(CrashMode::LoseUnsynced);
						if !contents(&image).marker {
							violations
								.lock()
								.unwrap()
								.push(format!("{call:?} while the marker is not durable"));
						}
						"database file"
					}
					_ => return Verdict::Go,
				};
				trace.lock().unwrap().push(kind.to_string());
				Verdict::Go
			});
		}
	});
	let (result, _) = call(&source, &mounter, &options, &small_tuning()).await;
	assert_eq!(result.unwrap(), imported(&rows));
	assert_eq!(*violations.lock().unwrap(), Vec::<String>::new());
	let trace = trace.lock().unwrap().clone();
	assert_eq!(trace.first().map(String::as_str), Some("create marker"), "{trace:?}");
	assert_eq!(trace.last().map(String::as_str), Some("remove marker"), "{trace:?}");
	assert!(trace.iter().filter(|k| *k == "database file").count() > 10, "{trace:?}");
	assert!(trace.contains(&"sync marker".to_string()), "{trace:?}");
}

// ---------------------------------------------------------------------------------------------
// Crashes
// ---------------------------------------------------------------------------------------------

/// What a sweep found: how many images it took, what the next call made of them, and where in the
/// call the crash was.
#[derive(Debug, Default)]
struct Tally {
	images: usize,
	imported: usize,
	existing: usize,
	/// Images with the marker in them, with files of a database and no marker, or with neither.
	with_marker: usize,
	with_files_and_no_marker: usize,
	with_nothing: usize,
	/// How many crashes were in each phase of the call.
	by_phase: [usize; PHASES.len()],
}

/// The phases of a call, by what the pool has seen of it.
const PHASES: [&str; 6] = [
	"before the marker",
	"the marker is being made",
	"the marker is durable, no file of the database yet",
	"the copy",
	"the verification and the barrier",
	"the commit",
];

/// A copy to crash: the rows, in the order that the source gives them, the options of the database
/// and the size of the pieces.
struct Shape {
	name: &'static str,
	rows: Rows,
	options: Options,
	tuning: ImportTuning,
}

impl Shape {
	/// The source of the copy: the rows, in the order that they are in.
	fn source(&self) -> FakeSource {
		FakeSource::in_order(self.rows.clone())
	}

	/// The rows that the database holds once it is made: those of the source that the engine does
	/// not refuse, in the order of their keys.
	fn written(&self) -> Rows {
		let mut rows: Rows = self.rows.iter().filter(|(k, _)| !k.is_empty()).cloned().collect();
		rows.sort();
		rows
	}

	/// What a call of the copy says.
	fn imported(&self) -> IndexedDbMigration {
		let skipped = self.rows.iter().filter(|(k, _)| k.is_empty()).count();
		imported_skipping(&self.written(), skipped as u64)
	}
}

/// Where in a call each phase begins, in operations of the memory, from a call that was not
/// crashed. A crash after `n` operations is in the last phase that begins before `n`.
#[derive(Debug)]
struct Marks {
	total: u64,
	starts: [u64; PHASES.len()],
}

impl Marks {
	fn phase_of(&self, n: u64) -> usize {
		(1..PHASES.len()).rev().find(|p| n > self.starts[*p]).unwrap_or(0)
	}
}

/// Runs the call of `shape` once, and says where its phases are.
async fn marks_of(shape: &Shape) -> Marks {
	let raw = MemRaw::new();
	let mounter = MemMounter::new(raw.clone());
	let at: Arc<Mutex<[Option<u64>; 4]>> = Arc::default();
	mounter.on_probe({
		let (raw, at) = (raw.clone(), Arc::clone(&at));
		move |probe| {
			let (raw, at) = (raw.clone(), Arc::clone(&at));
			probe.before(move |call| {
				let marker = call.path() == Path::new(MARKER);
				let slot = match call {
					Call::CreateNew(_) if marker => 0,
					Call::SyncFile(_) if marker => 1,
					Call::RemoveFile(_) if marker => 3,
					Call::CreateNew(_)
					| Call::OpenAppend(_)
					| Call::CreateTruncate(..)
					| Call::ReplaceAtomic(_) => 2,
					_ => return Verdict::Go,
				};
				at.lock().unwrap()[slot].get_or_insert(raw.ops());
				Verdict::Go
			});
		}
	});
	let source = shape.source();
	let (result, _) = call(&source, &mounter, &shape.options, &shape.tuning).await;
	result.unwrap();
	let at = *at.lock().unwrap();
	let at_or = |slot: usize, other: usize| at[slot].or(at[other]).expect("the call has this step");
	let at = |slot: usize, what: &str| at[slot].unwrap_or_else(|| panic!("the call never {what}"));
	assert_eq!(mounter.mounts(), 2);
	Marks {
		total: raw.ops(),
		starts: [
			0,
			at(0, "makes the marker"),
			// A call that never syncs the marker has no phase for it: the phase is empty.
			at_or(1, 2),
			at(2, "makes a file of the database"),
			mounter.started()[1],
			at(3, "removes the marker"),
		],
	}
}

/// Runs the call of `shape` on fresh memory that dies after `n` operations, and returns it.
async fn run_until_dead(shape: &Shape, n: u64) -> MemRaw {
	let raw = MemRaw::new();
	raw.die_after(n);
	let source = shape.source();
	let _ = call(&source, &MemMounter::new(raw.clone()), &shape.options, &shape.tuning).await;
	raw
}

/// What a crash of `raw`, which died after `n` operations, may have left, in several ways: what
/// was flushed and nothing else, everything, a file that kept its length and not its bytes, and
/// one more that changes with `n` (a prefix of what was not flushed, a reordering, a write that was
/// lost while a later one was torn, a write that was torn).
fn images_of(raw: &MemRaw, n: u64, wide: bool) -> Vec<(String, MemRaw)> {
	let unflushed = raw.unflushed_ops();
	let one_more = match n % 4 {
		0 => CrashMode::Prefix(unflushed / 2),
		1 => CrashMode::Reorder(n),
		2 => CrashMode::LoseTear {
			lost: unflushed / 3,
			torn: unflushed.saturating_sub(1),
			n: 3,
		},
		_ => CrashMode::Tear(unflushed / 2, 4),
	};
	let modes = if wide {
		vec![CrashMode::LoseUnsynced, CrashMode::Prefix(usize::MAX), CrashMode::ZeroTail, one_more]
	} else {
		vec![CrashMode::LoseUnsynced, one_more]
	};
	modes
		.into_iter()
		.map(|mode| {
			(format!("{mode:?} after {n} operations ({unflushed} not flushed)"), raw.crash(mode))
		})
		.collect()
}

/// The next call, from `image`, with a source that is there: it must make the database that the
/// source is, whatever the crash left, and it must never take part of one for the whole.
async fn finish(shape: &Shape, image: &MemRaw, what: &str, tally: &mut Tally, again: bool) {
	let held = contents(image);
	tally.images += 1;
	if held.marker {
		tally.with_marker += 1;
	} else if !held.files.is_empty() {
		tally.with_files_and_no_marker += 1;
	} else {
		tally.with_nothing += 1;
	}
	let source = shape.source();
	let (result, _) =
		call(&source, &MemMounter::new(image.clone()), &shape.options, &shape.tuning).await;
	match result {
		Ok(IndexedDbMigration::Imported {
			entries,
			bytes,
			skipped,
			..
		}) => {
			assert_eq!(
				shape.imported(),
				IndexedDbMigration::Imported {
					entries,
					bytes,
					skipped
				},
				"{what}"
			);
			tally.imported += 1;
		}
		Ok(IndexedDbMigration::Existing) => {
			// A database with no marker is found, and so it must be the whole of the source.
			assert_eq!(source.calls(), 0, "{what}: the source was used for a database that exists");
			assert!(
				!held.marker && !held.files.is_empty(),
				"{what}: a database was found where there was none ({} files)",
				held.files.len()
			);
			tally.existing += 1;
		}
		other => panic!("{what}: the call ended with {other:?}"),
	}
	assert_database(image, &shape.options, &shape.written(), what).await;
	if again {
		let source = shape.source();
		let (result, _) =
			call(&source, &MemMounter::new(image.clone()), &shape.options, &shape.tuning).await;
		assert!(matches!(result, Ok(IndexedDbMigration::Existing)), "{what}: {result:?}");
		assert_eq!(source.calls(), 0, "{what}");
	}
}

/// A crash after each of `points` operations of the call of `shape`, in the ways of `images_of`,
/// and the next call from each. Returns what was found, and some of the images that had a marker
/// and files of a database, which the next sweeps crash again.
async fn sweep(
	shape: &Shape,
	marks: &Marks,
	points: &[u64],
	wide: bool,
	again_every: u64,
) -> (Tally, Vec<MemRaw>) {
	let mut tally = Tally::default();
	let mut partial = Vec::new();
	let mut taken = 0u64;
	for &n in points {
		let raw = run_until_dead(shape, n).await;
		tally.by_phase[marks.phase_of(n)] += 1;
		for (what, image) in images_of(&raw, n, wide) {
			let what = format!("{}: {what}", shape.name);
			taken += 1;
			if partial.len() < 12 && taken % 7 == 0 {
				let held = contents(&image);
				if held.marker && !held.files.is_empty() {
					partial.push(image.snapshot());
				}
			}
			finish(shape, &image, &what, &mut tally, taken % again_every == 0).await;
		}
	}
	(tally, partial)
}

/// Every `stride`th operation of a call, the operations around its phases, and the one after its
/// last.
fn every(marks: &Marks, stride: u64) -> Vec<u64> {
	let mut points: std::collections::BTreeSet<u64> =
		(0..=marks.total).step_by(stride as usize).collect();
	points.extend(around_the_phases(marks, 1));
	points.into_iter().collect()
}

/// The operations around the places where a call changes phase, and `evenly` more that are spread
/// over the whole call: for a call that has too many operations to crash at each.
fn around_the_phases(marks: &Marks, evenly: u64) -> Vec<u64> {
	let mut points = std::collections::BTreeSet::new();
	for start in &marks.starts {
		for n in start.saturating_sub(2)..=start + 2 {
			points.insert(n.min(marks.total + 1));
		}
	}
	for k in 0..=evenly {
		points.insert(marks.total * k / evenly);
	}
	points.insert(marks.total);
	points.insert(marks.total + 1);
	points.into_iter().collect()
}

/// What every sweep over every operation must show.
fn assert_covered(tally: &Tally, marks: &Marks, name: &str) {
	for (phase, count) in tally.by_phase.iter().enumerate() {
		assert!(
			*count > 0,
			"{name}: no crash in the phase {:?}: {tally:?} {marks:?}",
			PHASES[phase]
		);
	}
	assert!(tally.imported > 0 && tally.existing > 0, "{name}: {tally:?}");
	assert!(tally.with_marker > 0, "{name}: {tally:?}");
	assert!(tally.with_nothing > 0, "{name}: {tally:?}");
	assert!(tally.with_files_and_no_marker > 0, "{name}: {tally:?}");
	// What was found as a database was a database, and what was not was copied: the two add up.
	assert_eq!(tally.images, tally.imported + tally.existing, "{name}: {tally:?}");
	assert_eq!(tally.existing, tally.with_files_and_no_marker, "{name}: {tally:?}");
}

fn tiny(vlog: bool) -> Shape {
	Shape {
		name: if vlog {
			"tiny, with the value log"
		} else {
			"tiny"
		},
		rows: small_rows(12),
		options: options(1024, vlog),
		tuning: small_tuning(),
	}
}

#[test(tokio::test)]
async fn a_crash_at_every_operation_of_a_copy_leaves_what_the_next_call_makes_a_database_of() {
	let shape = tiny(false);
	let marks = marks_of(&shape).await;
	let (tally, _) = sweep(&shape, &marks, &every(&marks, 1), true, 8).await;
	assert_covered(&tally, &marks, shape.name);
}

#[test(tokio::test)]
async fn a_crash_at_every_second_operation_of_a_copy_with_the_value_log_is_made_a_database_of() {
	let shape = tiny(true);
	let marks = marks_of(&shape).await;
	let (tally, _) = sweep(&shape, &marks, &every(&marks, 2), true, 16).await;
	assert_covered(&tally, &marks, shape.name);
}

/// Several thousand rows of every length, among them one of three million bytes, in a memtable that
/// the copy fills many times over.
fn thousands(vlog: bool) -> Shape {
	Shape {
		name: if vlog {
			"thousands, with the value log"
		} else {
			"thousands"
		},
		rows: rows_of(3000),
		options: options(64 * 1024, vlog),
		tuning: ImportTuning {
			read: ChunkLimits::new(256 * 1024, 500),
			write: ChunkLimits::new(64 * 1024, 200),
			commit_pause: std::time::Duration::ZERO,
			abandon_after_chunks: None,
		},
	}
}

#[test(tokio::test)]
async fn the_copy_of_thousands_of_rows_rotates_the_log_flushes_tables_and_rolls_the_value_log() {
	for vlog in [false, true] {
		let shape = thousands(vlog);
		let raw = MemRaw::new();
		let mounter = MemMounter::new(raw.clone());
		let made: Arc<Mutex<(usize, usize, usize)>> = Arc::default();
		mounter.on_probe({
			let made = Arc::clone(&made);
			move |probe| {
				let made = Arc::clone(&made);
				probe.before(move |call| {
					let extension = |name: &str| call.path().extension().is_some_and(|e| e == name);
					let mut made = made.lock().unwrap();
					match call {
						Call::CreateNew(_) | Call::OpenAppend(_) if extension("wal") => made.0 += 1,
						Call::CreateTruncate(..) if extension("sst") => made.1 += 1,
						Call::CreateNew(_) | Call::OpenAppend(_) if extension("vlog") => {
							made.2 += 1
						}
						_ => {}
					}
					Verdict::Go
				});
			}
		});
		let source = shape.source();
		let (result, _) = call(&source, &mounter, &shape.options, &shape.tuning).await;
		assert_eq!(result.unwrap(), imported(&shape.rows));
		let (segments, tables, vlog_files) = *made.lock().unwrap();
		assert!(segments >= 3, "the log rotated: {segments} segments, vlog {vlog}");
		assert!(tables >= 3, "memtables were flushed: {tables} tables, vlog {vlog}");
		if vlog {
			assert!(vlog_files >= 2, "the value log rolled: {vlog_files} files");
		} else {
			assert_eq!(vlog_files, 0);
		}
		assert_database(&raw, &shape.options, &shape.rows, shape.name).await;
	}
}

#[test(tokio::test)]
async fn a_crash_around_every_phase_of_a_copy_of_thousands_of_rows_is_made_a_database_of() {
	for vlog in [false, true] {
		let shape = thousands(vlog);
		let marks = marks_of(&shape).await;
		let points = around_the_phases(&marks, 24);
		let (tally, _) = sweep(&shape, &marks, &points, false, 5).await;
		assert_covered(&tally, &marks, shape.name);
	}
}

// ---------------------------------------------------------------------------------------------
// A crash of the call that follows a crash
// ---------------------------------------------------------------------------------------------

/// Images that a crash left in the middle of a copy of `shape`: the marker, and part of a database.
async fn unfinished_copies(shape: &Shape, marks: &Marks) -> Vec<MemRaw> {
	let mut images = Vec::new();
	for k in [3u64, 6, 9] {
		let n = marks.starts[3] + (marks.starts[4] - marks.starts[3]) * k / 10;
		let image = run_until_dead(shape, n).await.crash(CrashMode::LoseUnsynced);
		let held = contents(&image);
		assert!(
			held.marker && !held.files.is_empty(),
			"an unfinished copy at {n}: {:?}",
			held.files
		);
		images.push(image);
	}
	images
}

/// How many operations the call from `image` makes before it makes the first file of the new
/// copy: the length of the part of it that removes the old one.
async fn operations_to_wipe(shape: &Shape, image: &MemRaw) -> u64 {
	let copy = image.snapshot();
	let mounter = MemMounter::new(copy.clone());
	let first: Arc<Mutex<Option<u64>>> = Arc::default();
	mounter.on_probe({
		let (copy, first) = (copy.clone(), Arc::clone(&first));
		move |probe| {
			let (copy, first) = (copy.clone(), Arc::clone(&first));
			probe.before(move |call| {
				let makes_a_file = matches!(
					call,
					Call::CreateNew(_)
						| Call::OpenAppend(_)
						| Call::CreateTruncate(..)
						| Call::ReplaceAtomic(_)
				);
				if makes_a_file && call.path() != Path::new(MARKER) {
					first.lock().unwrap().get_or_insert(copy.ops());
				}
				Verdict::Go
			});
		}
	});
	let source = shape.source();
	let _ = call(&source, &mounter, &shape.options, &shape.tuning).await;
	let at = *first.lock().unwrap();
	at.expect("the call makes the files of a new copy")
}

#[test(tokio::test)]
async fn a_crash_while_the_call_removes_an_unfinished_copy_leaves_what_the_next_call_makes_a_database_of(
) {
	let shape = tiny(false);
	let marks = marks_of(&shape).await;
	let mut tally = Tally::default();
	for image in unfinished_copies(&shape, &marks).await {
		let window = operations_to_wipe(&shape, &image).await;
		assert!(window >= 10, "the removal has operations to crash: {window}");
		for m in 0..=window + 1 {
			let copy = image.snapshot();
			copy.die_after(m);
			let source = shape.source();
			let _ =
				call(&source, &MemMounter::new(copy.clone()), &shape.options, &shape.tuning).await;
			for (what, again) in images_of(&copy, m, false) {
				let what = format!("the removal, {what}");
				finish(&shape, &again, &what, &mut tally, m % 10 == 0).await;
			}
		}
	}
	assert!(tally.images > 60 && tally.imported == tally.images, "{tally:?}");
}

#[test(tokio::test)]
async fn a_crash_while_the_call_removes_a_copy_whose_source_is_gone_leaves_no_database_and_a_way_on(
) {
	let shape = tiny(false);
	let marks = marks_of(&shape).await;
	let mut tally = Tally::default();
	let mut errors = 0;
	for image in unfinished_copies(&shape, &marks).await {
		let whole = {
			let copy = image.snapshot();
			let (result, _) = call(
				&FakeSource::missing(),
				&MemMounter::new(copy.clone()),
				&shape.options,
				&shape.tuning,
			)
			.await;
			assert!(matches!(result, Err(Error::MigrationFailed(_))), "{result:?}");
			copy.ops()
		};
		for m in 0..=whole + 1 {
			let copy = image.snapshot();
			copy.die_after(m);
			let gone = FakeSource::missing();
			let _ =
				call(&gone, &MemMounter::new(copy.clone()), &shape.options, &shape.tuning).await;
			for (what, again) in images_of(&copy, m, false) {
				let what = format!("the removal, source gone, {what}");
				// The source is not back yet: the call fails, and no file of a database is left.
				if m % 4 == 0 {
					let held = again.snapshot();
					let (result, _) = call(
						&FakeSource::missing(),
						&MemMounter::new(held.clone()),
						&shape.options,
						&shape.tuning,
					)
					.await;
					assert!(
						matches!(result, Err(Error::MigrationFailed(ref e)) if e.contains("not finished")),
						"{what}: {result:?}"
					);
					let left = contents(&held);
					assert!(left.marker && left.files.is_empty(), "{what}: {:?}", left.files);
					errors += 1;
				}
				// It is back.
				finish(&shape, &again, &what, &mut tally, false).await;
			}
		}
	}
	assert!(errors > 20 && tally.images > 100 && tally.imported == tally.images, "{tally:?}");
}

#[test(tokio::test)]
async fn a_crash_while_the_call_removes_a_copy_that_failed_the_verification_is_made_a_database_of()
{
	let shape = tiny(false);
	let tamper = Tamper::Set(shape.rows[3].0.clone(), b"another value".to_vec());
	let window = {
		let raw = MemRaw::new();
		let mounter = MemMounter::new(raw.clone());
		mounter.tamper_at_mount(2, tamper.clone(), shape.options.clone());
		let source = shape.source();
		let (result, _) = call(&source, &mounter, &shape.options, &shape.tuning).await;
		assert!(matches!(result, Err(Error::MigrationFailed(_))), "{result:?}");
		assert_wiped_then_copied(&raw, &shape.options, &shape.rows, "a copy that failed").await;
		// The operations of the verification, the removal and what follows the second mount.
		let raw = MemRaw::new();
		let mounter = MemMounter::new(raw.clone());
		mounter.tamper_at_mount(2, tamper.clone(), shape.options.clone());
		let source = shape.source();
		let _ = call(&source, &mounter, &shape.options, &shape.tuning).await;
		raw.ops() - mounter.finished()[1]
	};
	assert!(window > 20, "{window}");

	let mut tally = Tally::default();
	for m in 0..=window + 1 {
		let raw = MemRaw::new();
		let mounter = MemMounter::new(raw.clone());
		mounter.tamper_at_mount(2, tamper.clone(), shape.options.clone());
		mounter.die_after_mount(2, m);
		let source = shape.source();
		let _ = call(&source, &mounter, &shape.options, &shape.tuning).await;
		for (what, image) in images_of(&raw, m, true) {
			let what = format!("the removal after a failed verification, {what}");
			finish(&shape, &image, &what, &mut tally, m % 10 == 0).await;
		}
	}
	// A copy that did not pass is never taken for a database: the marker was there in all.
	assert!(tally.images > 100 && tally.imported == tally.images, "{tally:?}");
}

#[test(tokio::test)]
async fn once_the_call_returns_no_crash_brings_the_marker_or_a_partial_database_back() {
	for vlog in [false, true] {
		let shape = tiny(vlog);
		let raw = MemRaw::new();
		let source = shape.source();
		let (result, _) =
			call(&source, &MemMounter::new(raw.clone()), &shape.options, &shape.tuning).await;
		assert_eq!(result.unwrap(), imported(&shape.rows));

		let mut modes = vec![
			CrashMode::LoseUnsynced,
			CrashMode::ZeroTail,
			CrashMode::Prefix(0),
			CrashMode::Prefix(usize::MAX),
			CrashMode::LoseTear {
				lost: 0,
				torn: usize::MAX,
				n: 0,
			},
		];
		modes.extend((0..24).map(CrashMode::Reorder));
		for mode in modes {
			let image = raw.crash(mode);
			// The application removes the IndexedDB database after the call: nothing is left to
			// copy from, and the database that the crash left is the one that is found.
			let gone = FakeSource::missing();
			let (result, _) =
				call(&gone, &MemMounter::new(image.clone()), &shape.options, &shape.tuning).await;
			assert!(matches!(result, Ok(IndexedDbMigration::Existing)), "{mode:?}: {result:?}");
			assert_eq!(gone.calls(), 0, "{mode:?}");
			assert_database(&image, &shape.options, &shape.rows, &format!("{mode:?}")).await;
		}
	}
}

// ---------------------------------------------------------------------------------------------
// Other pages
// ---------------------------------------------------------------------------------------------

/// A mounter whose answer to "are the files there" is the one that a page got before another page
/// made them: no.
struct StaleExists(MemMounter);

impl Mounter for StaleExists {
	async fn exists(&self) -> Result<bool> {
		Ok(false)
	}

	async fn mount(&self) -> Result<FileSystem> {
		self.0.mount().await
	}
}

#[test(tokio::test)]
async fn files_that_were_made_after_the_look_are_looked_at_again_and_not_copied_over() {
	let options = options(4 * 1024, true);
	let rows = small_rows(40);
	let source = FakeSource::new(rows.clone());
	let raw = MemRaw::new();
	// Another page made the database, its application wrote to it, and it let go, all after this
	// page asked whether the files were there.
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), imported(&rows));
	let mut newer = small_rows(5);
	newer.iter_mut().for_each(|row| row.0.push(7));
	app_writes(&raw, &options, &newer).await;
	let mut now = rows.clone();
	now.extend(newer);
	now.sort();

	let reads = source.reads();
	let stale = StaleExists(MemMounter::new(raw.clone()));
	let (result, report) = call(&source, &stale, &options, &small_tuning()).await;
	assert_eq!(result.unwrap(), IndexedDbMigration::Existing);
	assert_eq!(source.reads(), reads, "no row was read");
	assert_eq!(report, ImportReport::default());
	assert_database(&raw, &options, &now, "the database of the application").await;
}

#[test(tokio::test)]
async fn an_unfinished_copy_that_was_made_after_the_look_is_removed_and_made_again() {
	let options = options(2 * 1024, false);
	let before = small_rows(100);
	let source = FakeSource::new(before.clone());
	let raw = MemRaw::new();
	let tuning = ImportTuning {
		abandon_after_chunks: Some(3),
		..small_tuning()
	};
	let (result, _) = call(&source, &MemMounter::new(raw.clone()), &options, &tuning).await;
	assert!(result.is_err());
	let left = contents(&raw);
	assert!(left.marker && !left.files.is_empty());

	// The source has other rows by now: what the unfinished copy holds must not be kept.
	let after: Rows = (500..560).map(|i| (key_of(i), value_of(i))).collect();
	let source = FakeSource::new(after.clone());
	let stale = StaleExists(MemMounter::new(raw.clone()));
	let (result, _) = call(&source, &stale, &options, &small_tuning()).await;
	let after = {
		let mut sorted = after;
		sorted.sort();
		sorted
	};
	assert_eq!(result.unwrap(), imported(&after));
	assert_database(&raw, &options, &after, "an unfinished copy after the look").await;
}

#[test(tokio::test)]
async fn an_empty_pool_that_was_made_after_the_look_gets_the_copy() {
	let options = options(2 * 1024, false);
	let rows = small_rows(50);
	let source = FakeSource::new(rows.clone());
	let raw = MemRaw::new();
	let (pool, _) = PoolFs::<MemRaw>::mount_sync(raw.clone(), PoolConfig::memory()).unwrap();
	FileSystem(pool as Arc<dyn Fs>).close().await.unwrap();

	let stale = StaleExists(MemMounter::new(raw.clone()));
	let (result, _) = call(&source, &stale, &options, &small_tuning()).await;
	assert_eq!(result.unwrap(), imported(&rows));
	assert_database(&raw, &options, &rows, "an empty pool").await;
}

/// What another page does between the first mount of a call and the second, when the lock is let
/// go: the call that this one is made in.
enum Interloper {
	/// A whole call, and then its application writes these rows.
	Finishes(Rows, Rows),
	/// A call that stops at its second mount, as one that meets a lock does: a copy that is whole,
	/// and not committed, of the source with these rows.
	LeavesACopy(Rows),
	/// Every file of the pool is removed.
	Destroys,
}

struct Interloped {
	inner: MemMounter,
	raw: MemRaw,
	options: Options,
	action: Interloper,
}

impl Interloped {
	fn new(raw: &MemRaw, options: &Options, action: Interloper) -> Self {
		Self {
			inner: MemMounter::new(raw.clone()),
			raw: raw.clone(),
			options: options.clone(),
			action,
		}
	}
}

impl Mounter for Interloped {
	async fn exists(&self) -> Result<bool> {
		self.inner.exists().await
	}

	async fn mount(&self) -> Result<FileSystem> {
		// The second mount of the call: the first one has been let go.
		if self.inner.mounts() == 1 {
			match &self.action {
				Interloper::Finishes(rows, app) => {
					let source = FakeSource::new(rows.clone());
					let result = small_call(&source, &self.raw, &self.options).await;
					assert_eq!(result.unwrap(), imported(rows));
					if !app.is_empty() {
						app_writes(&self.raw, &self.options, app).await;
					}
				}
				Interloper::LeavesACopy(rows) => {
					let source = FakeSource::new(rows.clone());
					let mounter = MemMounter::new(self.raw.clone());
					mounter.fail_mount(2, locked());
					let (result, _) = call(&source, &mounter, &self.options, &small_tuning()).await;
					assert!(result.unwrap_err().is_database_locked());
				}
				Interloper::Destroys => {
					for name in self.raw.names() {
						self.raw.remove(&name);
					}
				}
			}
		}
		self.inner.mount().await
	}
}

#[test(tokio::test)]
async fn a_database_that_another_page_finished_between_the_two_mounts_is_left_as_it_is() {
	let options = options(4 * 1024, true);
	let rows = small_rows(40);
	for write in [false, true] {
		let source = FakeSource::new(rows.clone());
		let raw = MemRaw::new();
		let app = if write {
			vec![(b"app-key".to_vec(), b"app-value".to_vec())]
		} else {
			Vec::new()
		};
		let other =
			Interloped::new(&raw, &options, Interloper::Finishes(rows.clone(), app.clone()));
		let (result, _) = call(&source, &other, &options, &small_tuning()).await;
		let what = format!("application writes: {write}");
		// This call has nothing to check, to remove or to commit: the database is the other page's.
		assert_eq!(result.unwrap(), IndexedDbMigration::Existing, "{what}");
		let mut wanted = rows.clone();
		wanted.extend(app);
		wanted.sort();
		assert_database(&raw, &options, &wanted, &what).await;
	}
}

#[test(tokio::test)]
async fn files_that_were_removed_between_the_two_mounts_are_not_a_database_and_the_next_call_copies(
) {
	let options = options(4 * 1024, false);
	let rows = small_rows(40);
	let source = FakeSource::new(rows.clone());
	let raw = MemRaw::new();
	let other = Interloped::new(&raw, &options, Interloper::Destroys);
	let (result, _) = call(&source, &other, &options, &small_tuning()).await;
	let Err(Error::MigrationFailed(message)) = result else {
		panic!("{result:?}");
	};
	assert!(message.contains("removed by another page"), "{message}");
	let left = contents(&raw);
	assert!(!left.marker && left.files.is_empty(), "{:?}", left.files);

	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), imported(&rows));
	assert_database(&raw, &options, &rows, "after the files were removed").await;
}

#[test(tokio::test)]
async fn a_whole_copy_that_another_page_left_is_checked_against_what_this_call_read() {
	let options = options(4 * 1024, true);
	let rows = small_rows(40);

	// The same rows: it is the copy that this call made, and it is committed.
	let raw = MemRaw::new();
	let source = FakeSource::new(rows.clone());
	let other = Interloped::new(&raw, &options, Interloper::LeavesACopy(rows.clone()));
	let (result, _) = call(&source, &other, &options, &small_tuning()).await;
	assert_eq!(result.unwrap(), imported(&rows));
	assert_database(&raw, &options, &rows, "the same rows").await;

	// Other rows: it is not, and what is there is removed, with the marker kept.
	let raw = MemRaw::new();
	let source = FakeSource::new(rows.clone());
	let others = small_rows(30);
	let other = Interloped::new(&raw, &options, Interloper::LeavesACopy(others));
	let (result, _) = call(&source, &other, &options, &small_tuning()).await;
	let Err(Error::MigrationFailed(message)) = result else {
		panic!("{result:?}");
	};
	assert!(message.contains("40 rows were written") && message.contains("holds 30"), "{message}");
	assert_wiped_then_copied(&raw, &options, &rows, "another copy").await;
}

// ---------------------------------------------------------------------------------------------
// What a copy that did not finish leaves
// ---------------------------------------------------------------------------------------------

#[test(tokio::test)]
async fn a_copy_that_fails_removes_the_files_that_it_made_and_keeps_the_marker() {
	let options = options(2 * 1024, false);
	let rows = small_rows(80);
	// Rows are written before each of these fails. The last fails the verification, at the end.
	let mut with_key_twice = rows.clone();
	with_key_twice.push((rows[0].0.clone(), b"another value".to_vec()));
	let failing_source = FakeSource::new(rows.clone());
	failing_source.fail_at(5);
	let vanishing_source = FakeSource::new(rows.clone());
	vanishing_source.vanish_after(4);
	let cases = [
		("a key twice at the end", FakeSource::in_order(with_key_twice)),
		("an error of the source", failing_source),
		("a source that goes away", vanishing_source),
	];
	for (what, source) in cases {
		let raw = MemRaw::new();
		let (result, report) =
			call(&source, &MemMounter::new(raw.clone()), &options, &small_tuning()).await;
		assert!(matches!(result, Err(Error::MigrationFailed(_))), "{what}: {result:?}");
		assert!(report.transactions > 0, "{what}: rows were written before it failed: {report:?}");
		let left = contents(&raw);
		assert!(left.marker, "{what}: the marker stays");
		assert!(
			left.files.is_empty(),
			"{what}: the files of the copy were removed: {:?}",
			left.files
		);

		let mended = FakeSource::new(rows.clone());
		assert_eq!(small_call(&mended, &raw, &options).await.unwrap(), imported(&rows), "{what}");
		assert_database(&raw, &options, &rows, what).await;
	}
}

/// The error of an open that is refused.
fn refusal<T>(result: Result<T>) -> Error {
	match result {
		Err(error) => error,
		Ok(_) => panic!("the open was not refused"),
	}
}

fn assert_refused_for_the_copy(error: &Error, what: &str) {
	let Error::MigrationFailed(message) = error else {
		panic!("{what}: {error:?}");
	};
	assert!(
		message.contains("not finished") && message.contains(MARKER),
		"{what}: the refusal names the unfinished copy: {message}"
	);
}

#[test(tokio::test)]
async fn the_engine_does_not_open_a_database_while_a_copy_of_it_is_not_finished() {
	let options = options(2 * 1024, true);
	let rows = small_rows(60);
	let source = FakeSource::new(rows.clone());

	// A copy that was cut short leaves files and the marker. An application that opens the database
	// in spite of the error of the call, or without calling, is refused: nothing is read, written
	// or made, and what the application would have written is not lost to the next call.
	let raw = MemRaw::new();
	let tuning = ImportTuning {
		abandon_after_chunks: Some(3),
		..small_tuning()
	};
	let (result, _) = call(&source, &MemMounter::new(raw.clone()), &options, &tuning).await;
	assert!(result.is_err());
	let before = contents(&raw);
	assert!(before.marker && !before.files.is_empty());

	let (pool, _) = PoolFs::<MemRaw>::mount_sync(raw.clone(), PoolConfig::memory()).unwrap();
	let fs = FileSystem(Arc::clone(&pool) as Arc<dyn Fs>);
	let app = Options {
		fs: Arc::clone(&fs.0),
		..options.clone()
	};
	let builder = || TreeBuilder::with_options(app.clone());
	assert_refused_for_the_copy(&refusal(builder().build()), "build");
	assert_refused_for_the_copy(&refusal(builder().build_async().await), "build_async");
	assert_refused_for_the_copy(&refusal(builder().build_with_options()), "build_with_options");
	assert_refused_for_the_copy(
		&refusal(builder().build_with_options_async().await),
		"build_with_options_async",
	);
	fs.close().await.unwrap();
	let after = contents(&raw);
	assert!(after.marker, "the marker is still there");
	assert_eq!(after.files, before.files, "the refusal changed nothing");

	// The call makes the copy again, and then the database opens.
	assert_eq!(small_call(&source, &raw, &options).await.unwrap(), imported(&rows));
	assert_database(&raw, &options, &rows, "after the copy was made again").await;
	app_writes(&raw, &options, &vec![(b"app-key".to_vec(), b"v".to_vec())]).await;

	// A copy that failed, whose files were removed and whose marker is kept, is refused as well.
	let raw = MemRaw::new();
	let failing = FakeSource::new(rows.clone());
	failing.fail_at(3);
	assert!(small_call(&failing, &raw, &options).await.is_err());
	let left = contents(&raw);
	assert!(left.marker && left.files.is_empty());
	let (pool, _) = PoolFs::<MemRaw>::mount_sync(raw.clone(), PoolConfig::memory()).unwrap();
	let fs = FileSystem(Arc::clone(&pool) as Arc<dyn Fs>);
	let app = Options {
		fs: Arc::clone(&fs.0),
		..options.clone()
	};
	assert_refused_for_the_copy(
		&refusal(TreeBuilder::with_options(app).build_async().await),
		"after a failed copy",
	);
	fs.close().await.unwrap();
	assert!(contents(&raw).files.is_empty(), "nothing was made by the refused open");
}

// ---------------------------------------------------------------------------------------------
// What the verification compares
// ---------------------------------------------------------------------------------------------

fn tallied(rows: &[(&[u8], &[u8])]) -> Summary {
	let mut tally = RowTally::new();
	for (key, value) in rows {
		tally.add(key, value);
	}
	tally.finish()
}

#[test]
fn a_tally_says_whether_the_keys_came_in_the_order_of_the_bytes() {
	assert!(tallied(&[]).ascending, "no row is in order");
	assert!(tallied(&[(b"a", b"1")]).ascending);
	assert!(tallied(&[(b"a", b"1"), (b"a\0", b"2"), (b"b", b"")]).ascending);
	assert!(!tallied(&[(b"b", b"1"), (b"a", b"2")]).ascending, "a key before the one before");
	assert!(!tallied(&[(b"a", b"1"), (b"a", b"2")]).ascending, "the same key twice");
	assert!(!tallied(&[(b"a", b"1"), (b"c", b"2"), (b"b", b"3"), (b"d", b"4")]).ascending);
	// Once it is out of order, it stays so.
	assert!(!tallied(&[(b"b", b"1"), (b"a", b"2"), (b"c", b"3")]).ascending);
}

#[test]
fn the_comparison_of_a_copy_names_what_differs_and_compares_the_sequence_of_an_ordered_source() {
	let rows: [(&[u8], &[u8]); 3] = [(b"a", b"11"), (b"b", b"22"), (b"c", b"")];
	let read = tallied(&rows);
	compare(&read, &tallied(&rows)).expect("the same rows");

	let message = |found: &Summary| match compare(&read, found) {
		Err(Error::MigrationFailed(message)) => message,
		other => panic!("{other:?}"),
	};
	assert!(message(&tallied(&rows[..2])).contains("3 rows were written and the database holds 2"));
	assert!(
		message(&tallied(&[(b"a", b"11"), (b"b", b"2"), (b"c", b"")])).contains("bytes of keys"),
		"one value is shorter"
	);
	// The same count and the same length, with the values of two rows swapped, and with a byte of
	// a value changed: the sum of the hashes of the rows tells.
	assert!(
		message(&tallied(&[(b"a", b"22"), (b"b", b"11"), (b"c", b"")])).contains("contents differ")
	);
	assert!(
		message(&tallied(&[(b"a", b"11"), (b"b", b"23"), (b"c", b"")])).contains("contents differ")
	);

	// The sum is the same and the sequence is not: only the digest of the sequence tells.
	let mut other_order = read;
	other_order.chain ^= 1;
	assert!(message(&other_order).contains("digest of the sequence"));

	// A database whose rows are not in the order of their keys is not one, whatever it holds.
	let mut unordered = read;
	unordered.ascending = false;
	assert!(message(&unordered).contains("another order"));
}

#[test]
fn a_source_that_is_not_in_the_order_of_the_bytes_is_compared_as_a_set() {
	// The same rows in the order of the UTF-16 units of the keys, which is the order of a store of
	// strings, and in the order of the bytes, which is the order of a database.
	let early: &[u8] = "\u{1F600}".as_bytes();
	let late: &[u8] = "\u{FF5E}".as_bytes();
	assert!(late < early, "UTF-8 puts the character of a plane above the base one after it");
	let utf16_order = tallied(&[(early, b"emoji"), (late, b"tilde")]);
	let byte_order = tallied(&[(late, b"tilde"), (early, b"emoji")]);
	assert!(!utf16_order.ascending && byte_order.ascending);
	compare(&utf16_order, &byte_order).expect("the same rows");
	let mut changed = byte_order;
	changed.sum ^= 1;
	assert!(compare(&utf16_order, &changed).is_err(), "and the set is still compared");
}

#[test(tokio::test)]
async fn a_source_of_strings_in_the_order_of_utf16_is_copied_and_checked() {
	let options = options(2 * 1024, false);
	let mut rows: Rows =
		(0..30).map(|i| (format!("key{i:03}").into_bytes(), value_of(i))).collect();
	// What a store of strings gives last is what a database has first.
	rows.push(("\u{1F600}".as_bytes().to_vec(), b"emoji".to_vec()));
	rows.push(("\u{FF5E}".as_bytes().to_vec(), b"tilde".to_vec()));
	let source = FakeSource::in_order(rows.clone());
	let raw = MemRaw::new();
	let result = small_call(&source, &raw, &options).await;
	assert_eq!(result.unwrap(), imported(&rows));
	let mut sorted = rows.clone();
	sorted.sort();
	assert_database(&raw, &options, &sorted, "the order of the units of UTF-16").await;
}

// ---------------------------------------------------------------------------------------------
// The README
// ---------------------------------------------------------------------------------------------

/// The lines of `text` that have something in them, without their indentation.
fn lines_of(text: &str) -> Vec<String> {
	text.lines().map(str::trim).filter(|line| !line.is_empty()).map(str::to_string).collect()
}

/// The code of the README that `migrate_from_indexeddb` and `delete_indexeddb` are called in cannot
/// be built by the doctests of the crate: it needs a browser. `tests/opfs_migrate_test.rs` holds
/// the same code and is built for `wasm32-unknown-unknown` (by `wasm-pack test`, and by `cargo
/// check --tests` for that target), and this fails when the two are not the same, so that the
/// README cannot say something that does not build.
#[test]
fn the_examples_of_the_readme_are_the_code_that_the_browser_test_builds() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let readme = std::fs::read_to_string(root.join("README.md")).unwrap();
	let built = std::fs::read_to_string(root.join("tests/opfs_migrate_test.rs")).unwrap();

	let section = readme
		.split("### Moving data from IndexedDB")
		.nth(1)
		.expect("the README has the section on moving data from IndexedDB");
	// The section ends at the next heading of its own level.
	let section = section.split("\n### ").next().unwrap();
	let mut examples = Vec::new();
	let mut rest = section;
	while let Some((_, after_fence)) = rest.split_once("```rust,ignore\n") {
		let (example, after) = after_fence.split_once("```").expect("the example ends");
		examples.push(lines_of(example));
		rest = after;
	}
	assert_eq!(examples.len(), 2, "the section has the example of the move and of the removal");
	assert!(examples[0].iter().any(|l| l.contains("migrate_from_indexeddb(")), "{:?}", examples[0]);
	assert!(examples[1].iter().any(|l| l.contains("delete_indexeddb(")), "{:?}", examples[1]);

	let built = lines_of(&built);
	for example in examples {
		let found = built.windows(example.len()).any(|window| window == example.as_slice());
		assert!(
			found,
			"an example of the README is not in tests/opfs_migrate_test.rs, where it is built for a \
			 browser, line for line (indentation aside):\n{}",
			example.join("\n")
		);
	}
}
