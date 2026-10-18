//! Follow-ups of the engine over a file pool: what a database on `std` does not do
//! (the calls that only a pool needs), what `abandon` decides, what a pool that is at its limit
//! of files still lets through, the order in which `close` removes the log, and what a pool
//! refuses when a directory would need a migration.

use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tempdir::TempDir;
use test_log::test;

use super::fs_engine_tests::{
	assert_present,
	commit_range,
	key_of,
	options_in,
	until,
	value_of,
	within,
};
use super::fs_probe::{Call, ProbeFs, Verdict, Wait};
use crate::error::{ErrorSeverity, Result};
use crate::lockfile::LockFile;
use crate::lsm::FLUSH_NEED;
use crate::vfs::{
	CreateOpts,
	DirEntry,
	EntryKind,
	Fs,
	FsFile,
	FsKind,
	MemRaw,
	PoolConfig,
	PoolControl,
	PoolFs,
	SpareWait,
};
use crate::{CrashMode, Durability, Error, FileSystem, Options, Tree, TreeBuilder};

// ---------------------------------------------------------------------------------------------
// A database on `std` does not make the calls that only a pool needs
// ---------------------------------------------------------------------------------------------

/// `std` underneath, which counts the calls that a database on a pool makes and one on `std` does
/// not need to: the questions about spare files and the exhaustion counter, and the check that
/// the file system is there, which an open makes once.
#[derive(Debug)]
struct CountingStd {
	inner: Arc<dyn Fs>,
	pool_exhaustions: AtomicUsize,
	pool_control: AtomicUsize,
	ensure_spares: AtomicUsize,
	check_available: AtomicUsize,
}

impl CountingStd {
	fn wrap() -> (Arc<CountingStd>, FileSystem) {
		let counting = Arc::new(CountingStd {
			inner: Arc::clone(&FileSystem::std().0),
			pool_exhaustions: AtomicUsize::new(0),
			pool_control: AtomicUsize::new(0),
			ensure_spares: AtomicUsize::new(0),
			check_available: AtomicUsize::new(0),
		});
		let fs = FileSystem(Arc::clone(&counting) as Arc<dyn Fs>);
		(counting, fs)
	}

	fn count(counter: &AtomicUsize) -> usize {
		counter.load(Ordering::SeqCst)
	}
}

impl Fs for CountingStd {
	fn kind(&self) -> FsKind {
		self.inner.kind()
	}

	fn check_available(&self) -> io::Result<()> {
		self.check_available.fetch_add(1, Ordering::SeqCst);
		self.inner.check_available()
	}

	fn entry(&self, path: &Path) -> io::Result<Option<EntryKind>> {
		self.inner.entry(path)
	}

	fn try_exists(&self, path: &Path) -> io::Result<bool> {
		self.inner.try_exists(path)
	}

	fn exists(&self, path: &Path) -> bool {
		self.inner.exists(path)
	}

	fn create_dir_all(&self, path: &Path) -> io::Result<()> {
		self.inner.create_dir_all(path)
	}

	fn read_dir(&self, path: &Path) -> io::Result<Vec<DirEntry>> {
		self.inner.read_dir(path)
	}

	fn remove_file(&self, path: &Path) -> io::Result<()> {
		self.inner.remove_file(path)
	}

	fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
		self.inner.remove_dir_all(path)
	}

	fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
		self.inner.rename(from, to)
	}

	fn sync_dir(&self, path: &Path) -> io::Result<()> {
		self.inner.sync_dir(path)
	}

	fn open_read(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		self.inner.open_read(path)
	}

	fn open_append(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.inner.open_append(path, opts)
	}

	fn open_append_existing(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		self.inner.open_append_existing(path)
	}

	fn create_new(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.inner.create_new(path, opts)
	}

	fn create_truncate(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.inner.create_truncate(path, opts)
	}

	fn sync_file(&self, path: &Path) -> io::Result<()> {
		self.inner.sync_file(path)
	}

	fn replace_atomic(&self, path: &Path, content: &[u8]) -> Result<()> {
		self.inner.replace_atomic(path, content)
	}

	fn file_len(&self, path: &Path) -> io::Result<u64> {
		self.inner.file_len(path)
	}

	fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
		self.inner.read(path)
	}

	fn read_prefix(&self, path: &Path, n: usize) -> io::Result<Option<Vec<u8>>> {
		self.inner.read_prefix(path, n)
	}

	fn copy(&self, from: &Path, to: &Path) -> io::Result<u64> {
		self.inner.copy(from, to)
	}

	fn lock_file(&self, dir: &Path) -> LockFile {
		self.inner.lock_file(dir)
	}

	fn ensure_spares(&self, n: usize) -> Option<SpareWait> {
		self.ensure_spares.fetch_add(1, Ordering::SeqCst);
		self.inner.ensure_spares(n)
	}

	fn pool_exhaustions(&self) -> u64 {
		self.pool_exhaustions.fetch_add(1, Ordering::SeqCst);
		self.inner.pool_exhaustions()
	}

	fn pool(&self) -> Option<&dyn PoolControl> {
		self.pool_control.fetch_add(1, Ordering::SeqCst);
		self.inner.pool()
	}
}

#[test(tokio::test)]
async fn a_database_on_std_makes_none_of_the_calls_that_only_a_pool_needs() {
	within("std makes no pool call", async {
		for vlog in [false, true] {
			let dir = TempDir::new("std-no-pool-calls").unwrap();
			let (counting, fs) = CountingStd::wrap();
			let options = Options {
				path: dir.path().join("db"),
				max_memtable_size: 64 * 1024,
				level0_max_files: 2,
				enable_vlog: vlog,
				vlog_value_threshold: 128,
				..Options::default()
			}
			.with_file_system(fs);
			let tree = TreeBuilder::with_options(options).build_async().await.unwrap();
			// Enough to rotate, flush and compact, so that every background task has run.
			commit_range(&tree, 0..1500, 200, Durability::Eventual).await;
			let stop_signal = tree
				.core
				.task_manager
				.lock()
				.unwrap()
				.as_ref()
				.map(|tasks| tasks.has_stop_signal());
			tree.close().await.unwrap();

			assert_eq!(
				CountingStd::count(&counting.check_available),
				1,
				"an open asks once whether the file system is there, vlog={vlog}"
			);
			assert_eq!(
				CountingStd::count(&counting.pool_exhaustions),
				0,
				"a database on std never reads the exhaustion counter of a pool, vlog={vlog}"
			);
			assert_eq!(CountingStd::count(&counting.ensure_spares), 0, "vlog={vlog}");
			assert_eq!(CountingStd::count(&counting.pool_control), 0, "vlog={vlog}");
			assert_eq!(
				stop_signal,
				Some(false),
				"a database on std has no signal for a wait that it never makes, vlog={vlog}"
			);
		}
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// A full storage keeps its type at every place that would turn an error into text
// ---------------------------------------------------------------------------------------------

/// The error of a browser that is out of quota, as the OPFS backend gives it: the kind
/// `StorageFull` and no code of an operating system.
fn quota() -> io::Error {
	io::Error::new(io::ErrorKind::StorageFull, "QuotaExceededError")
}

const MEMTABLE: usize = 64 * 1024;

pub(super) fn options_for(fs: &FileSystem, vlog: bool) -> Options {
	let mut options = options_in(fs, MEMTABLE);
	options.enable_vlog = vlog;
	options.vlog_value_threshold = 128;
	options.vlog_max_file_size = 256 * 1024;
	// Memtables may queue up behind a flush that waits, without the writers stalling on them.
	options.memtable_stall_threshold = 16;
	options
}

async fn open_tree(fs: &FileSystem, vlog: bool) -> Tree {
	TreeBuilder::with_options(options_for(fs, vlog))
		.build_async()
		.await
		.expect("the database opens")
}

/// Commits one transaction at a time from `from` until `limit` or until a commit fails, and gives
/// how many were acknowledged and the error that ended it.
async fn commit_until_error(
	tree: &Tree,
	from: usize,
	limit: usize,
	value_len: usize,
) -> (usize, Option<Error>) {
	let mut next = from;
	while next < limit {
		let mut txn = tree.begin().unwrap();
		txn.set(key_of(next), value_of(next, value_len)).unwrap();
		match txn.commit().await {
			Ok(()) => next += 1,
			Err(e) => return (next, Some(e)),
		}
	}
	(next, None)
}

/// A flush whose call at `site` meets a quota three times does not stop the database: the error
/// is soft, the memtable stays queued, the flush is tried again, and nothing that was
/// acknowledged is lost. Each site is a place of the flush that turns an error into text unless it
/// is given the arm that keeps a full storage as it is.
async fn a_flush_that_meets_a_quota_at(site: fn(&Call) -> bool) {
	let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
	let tree = open_tree(&fs, false).await;
	let failures = Arc::new(AtomicUsize::new(0));
	let counter = Arc::clone(&failures);
	probe.before(move |call| {
		if site(call) && counter.fetch_add(1, Ordering::SeqCst) < 3 {
			return Verdict::Fail(quota());
		}
		Verdict::Go
	});
	let mut reached = 0;
	while tree.background_error().is_none() && !tree.is_stopped() && reached < 3000 {
		let (next, error) = commit_until_error(&tree, reached, reached + 20, 300).await;
		reached = next;
		assert!(error.is_none(), "a commit failed before any flush did: {error:?}");
	}
	until("the flush to meet the quota", || tree.background_error().is_some()).await;
	let error = tree.background_error().unwrap();
	assert_eq!(error.severity, ErrorSeverity::SoftError, "{:?}", error.error);
	assert!(!tree.is_stopped(), "a quota is not a stopped database");
	until("the flushes", || {
		failures.load(Ordering::SeqCst) > 3 && tree.core.inner.immutable_count() == 0
	})
	.await;
	assert!(!tree.is_stopped());
	assert_present(&tree, 0..reached, 300);
	let (next, error) = commit_until_error(&tree, reached, reached + 50, 300).await;
	assert!(error.is_none(), "{error:?}");
	assert_present(&tree, 0..next, 300);
	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn a_quota_when_the_table_of_a_flush_is_made_is_soft() {
	within("quota at the table", a_flush_that_meets_a_quota_at(Call::makes_a_table)).await;
}

#[test(tokio::test)]
async fn a_quota_when_the_manifest_of_a_flush_is_replaced_is_soft() {
	within("quota at the manifest", a_flush_that_meets_a_quota_at(Call::replaces_the_manifest))
		.await;
}

#[test(tokio::test)]
async fn a_quota_when_the_log_is_rotated_fails_the_commit_with_its_own_type_and_stops_nothing() {
	within("quota at the rotation", async {
		for vlog in [false, true] {
			let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
			let tree = open_tree(&fs, vlog).await;
			let armed = Arc::new(AtomicBool::new(true));
			let gate = Arc::clone(&armed);
			probe.before(move |call| {
				if call.makes_a_segment() && gate.load(Ordering::SeqCst) {
					return Verdict::Fail(quota());
				}
				Verdict::Go
			});
			// A 64 KiB memtable rotates after a couple of hundred commits: the commit that finds
			// no room for its batch rotates the log, and the new segment is refused.
			let (reached, error) = commit_until_error(&tree, 0, 3000, 300).await;
			let error = error.expect("the memtable fills and the log has to be rotated");
			assert!(
				error.is_storage_full(),
				"the committer hears a full storage, not text, vlog={vlog}: {error:?}"
			);
			assert!(
				!tree.is_stopped(),
				"a quota before anything of the group was applied, vlog={vlog}"
			);
			armed.store(false, Ordering::SeqCst);
			// The quota passes (the page freed space), and the same commits work.
			let (next, error) = commit_until_error(&tree, reached, reached + 200, 300).await;
			assert!(error.is_none(), "vlog={vlog}: {error:?}");
			assert_present(&tree, 0..next, 300);
			tree.close().await.unwrap();
		}
	})
	.await;
}

/// The error of a close whose shutdown flush met a quota is the quota, not text about it: the
/// caller of `close` is the one who can tell a page that has no room from a disk that failed.
#[test(tokio::test)]
async fn a_quota_during_the_shutdown_flush_ends_the_close_with_that_error_and_loses_nothing() {
	within("quota at the shutdown flush", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_tree(&fs, false).await;
		commit_range(&tree, 0..300, 300, Durability::Immediate).await;
		probe.before(|call| {
			if call.makes_a_table() {
				return Verdict::Fail(quota());
			}
			Verdict::Go
		});
		let closed = tree.close().await;
		assert!(matches!(closed, Err(ref e) if e.is_storage_full()), "{closed:?}");
		tree.abandon();
		drop(tree);
		probe.clear_hooks();
		let again = fs.simulate_crash(crate::CrashMode::LoseUnsynced).unwrap();
		let tree = open_tree(&again, false).await;
		assert_present(&tree, 0..300, 300);
		tree.close().await.unwrap();
	})
	.await;
}

/// The same close, when the memtables that it flushes are queued and the active one is empty: the
/// manifest is written once more at the end, to move the log number past the segments that were
/// flushed, and that write is a place of its own.
#[test(tokio::test)]
async fn a_quota_when_the_close_moves_the_log_number_ends_it_with_that_error() {
	within("quota at the last manifest of the close", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_tree(&fs, false).await;
		// Nothing flushes in the background, so the memtables stay queued for the close.
		probe.spares_wait(|n| (n == FLUSH_NEED).then_some(Wait::Never));
		commit_range(&tree, 0..100, 300, Durability::Immediate).await;
		tree.core.inner.rotate_memtable().unwrap();
		commit_range(&tree, 100..200, 300, Durability::Immediate).await;
		tree.core.inner.rotate_memtable().unwrap();
		let queued = tree.core.inner.immutable_count();
		assert!(queued >= 2, "the memtables are queued: {queued}");
		// The close flushes each of them with a manifest, and then writes the manifest once more:
		// that is the one that meets the quota.
		let writes = Arc::new(AtomicUsize::new(0));
		let counter = Arc::clone(&writes);
		probe.before(move |call| {
			if call.replaces_the_manifest() && counter.fetch_add(1, Ordering::SeqCst) == queued {
				return Verdict::Fail(quota());
			}
			Verdict::Go
		});
		probe.spares_wait(|_| None);
		let closed = tree.close().await;
		assert!(writes.load(Ordering::SeqCst) > queued, "the close wrote the last manifest");
		assert!(matches!(closed, Err(ref e) if e.is_storage_full()), "{closed:?}");
		tree.abandon();
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// A directory that would need a migration is refused in a pool, with a type
// ---------------------------------------------------------------------------------------------

/// Puts a file with `content` at `path` in `fs`, as a backend that holds the files of another
/// store would.
fn plant(fs: &FileSystem, path: &Path, content: &[u8]) {
	if let Some(parent) = path.parent() {
		fs.0.create_dir_all(parent).unwrap();
	}
	let file = fs.0.create_truncate(path, CreateOpts::default()).unwrap();
	file.append(content).unwrap();
	file.sync().unwrap();
}

/// Every path in `fs` below `dir`, sorted: what is there, to see that a refusal left it alone.
fn tree_of(fs: &FileSystem, dir: &Path) -> Vec<String> {
	let mut found = Vec::new();
	if let Ok(entries) = fs.0.read_dir(dir) {
		for entry in entries {
			let path = dir.join(&entry.name);
			found.push(path.display().to_string());
			if entry.kind == EntryKind::Dir {
				found.extend(tree_of(fs, &path));
			}
		}
	}
	found.sort();
	found
}

async fn assert_refused_in_a_pool(what: &str, planted: &[(&str, &[u8])]) {
	let fs = FileSystem::memory();
	for (name, content) in planted {
		plant(&fs, &Path::new("db").join(name), content);
	}
	let before = tree_of(&fs, Path::new("db"));
	for build_async in [true, false] {
		let options = Options {
			path: "db".into(),
			..Options::default()
		}
		.with_file_system(fs.clone());
		let opened = if build_async {
			TreeBuilder::with_options(options).build_async().await
		} else {
			TreeBuilder::with_options(options).build()
		};
		match opened {
			Err(Error::MigrationFailed(message)) => {
				assert!(message.contains(what), "the message names the store: {message}")
			}
			Err(other) => {
				panic!("{what}, build_async={build_async}: not a typed refusal: {other:?}")
			}
			Ok(_) => panic!("{what}, build_async={build_async}: a database opened over it"),
		}
		assert_eq!(
			tree_of(&fs, Path::new("db")),
			before,
			"{what}, build_async={build_async}: the refusal came before anything was created"
		);
	}
}

#[test(tokio::test)]
async fn a_pool_that_holds_a_rocksdb_directory_refuses_it_with_a_typed_error() {
	within("rocksdb in a pool", async {
		assert_refused_in_a_pool(
			"RocksDB",
			&[("CURRENT", b"MANIFEST-000005\n"), ("MANIFEST-000005", b"x"), ("000004.sst", b"x")],
		)
		.await;
		// A migration that was cut short leaves its marker.
		assert_refused_in_a_pool("RocksDB", &[("MIGRATING_ROCKSDB", b"")]).await;
	})
	.await;
}

#[test(tokio::test)]
async fn a_pool_that_holds_an_indexeddb_dump_or_a_v0_migration_in_progress_refuses_it() {
	within("dump and marker in a pool", async {
		assert_refused_in_a_pool("IndexedDB", &[("indxdb_dump.bin", b"INDXDB01 and the rest")])
			.await;
		assert_refused_in_a_pool("SurrealKV v0.x", &[("MIGRATING", b"")]).await;
	})
	.await;
}

/// What looks a little like the files of another store, and is not, opens.
#[test(tokio::test)]
async fn a_pool_that_holds_files_that_only_look_like_another_store_opens_the_database() {
	within("no false refusal", async {
		let fs = FileSystem::memory();
		// A file called CURRENT whose content does not name a RocksDB manifest, and a file of
		// the dump's name without its magic.
		plant(&fs, Path::new("db/CURRENT"), b"not a manifest name");
		plant(&fs, Path::new("db/indxdb_dump.bin"), b"no magic");
		let tree = TreeBuilder::with_options(
			Options {
				path: "db".into(),
				..Options::default()
			}
			.with_file_system(fs.clone()),
		)
		.build_async()
		.await
		.expect("the database opens");
		commit_range(&tree, 0..10, 20, Durability::Immediate).await;
		tree.close().await.unwrap();
	})
	.await;
}

/// Copies the files of the directory `from` of the operating system into `to` of `fs`.
fn copy_into(fs: &FileSystem, from: &Path, to: &Path) {
	fs.0.create_dir_all(to).unwrap();
	for entry in std::fs::read_dir(from).unwrap().flatten() {
		let target = to.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_into(fs, &entry.path(), &target);
		} else {
			plant(fs, &target, &std::fs::read(entry.path()).unwrap());
		}
	}
}

/// The files of a v0.x database of the operating system, with their bytes.
fn bytes_of(dir: &Path) -> Vec<(String, Vec<u8>)> {
	let mut found = Vec::new();
	for entry in std::fs::read_dir(dir).unwrap().flatten() {
		let path = entry.path();
		if entry.file_type().unwrap().is_dir() {
			found.push((path.display().to_string(), Vec::new()));
			found.extend(bytes_of(&path));
		} else {
			found.push((path.display().to_string(), std::fs::read(&path).unwrap()));
		}
	}
	found.sort();
	found
}

/// A v0.x database in a pool is refused with the type of a legacy format. The migrator reads
/// the operating system's files by path, so it is not asked: a directory of the operating system
/// with the same path is another database, and is not migrated because of what a pool holds.
#[test(tokio::test)]
async fn a_v0_database_in_a_pool_is_refused_and_the_directory_of_the_same_path_is_not_migrated() {
	within("v0.x in a pool", async {
		let source = Path::new(env!("CARGO_MANIFEST_DIR"))
			.join("tests/fixtures/legacy/v0.21.4/tiny-plain/db");
		let dir = TempDir::new("v0-in-a-pool").unwrap();
		let path = dir.path().join("db");
		// The same files in the operating system's directory and in the pool, at the same path.
		copy_into(&FileSystem::std(), &source, &path);
		let fs = FileSystem::memory();
		copy_into(&fs, &source, &path);
		let on_disk = bytes_of(&path);

		let options = Options {
			path: path.clone(),
			..Options::default()
		}
		.with_file_system(fs.clone());
		let opened = TreeBuilder::with_options(options).build_async().await;
		assert!(matches!(opened, Err(Error::LegacyFormat(_))), "{:?}", opened.err());
		assert_eq!(
			bytes_of(&path),
			on_disk,
			"the files of the operating system were not migrated because of what a pool holds"
		);
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// The order in which a close removes the log
// ---------------------------------------------------------------------------------------------

/// A pool over memory that the test keeps a hand on: what a crash would leave of it is read from
/// the memory, and a database is opened in that.
pub(super) fn pool_over(raw: &MemRaw) -> (FileSystem, Arc<PoolFs<MemRaw>>) {
	let (pool, _) = PoolFs::<MemRaw>::mount_sync(raw.clone(), PoolConfig::memory())
		.expect("a pool mounts over memory");
	(FileSystem(Arc::clone(&pool) as Arc<dyn Fs>), pool)
}

/// Opens the database that a crash left in `raw`, and says what is wrong with it, if anything: it
/// has to open, hold every one of the first `commits` keys, and take a commit.
async fn what_is_wrong_with(raw: MemRaw, commits: usize) -> Option<String> {
	let (fs, pool) = pool_over(&raw);
	let mut options = options_for(&fs, false);
	options.wal_recovery_mode = crate::WalRecoveryMode::SalvageValidPrefix;
	let tree = match TreeBuilder::with_options(options).build_async().await {
		Ok(tree) => tree,
		Err(e) => return Some(format!("does not open: {e}")),
	};
	let mut wrong = None;
	{
		let txn = tree.begin_with_mode(crate::Mode::ReadOnly).unwrap();
		for i in 0..commits {
			match txn.get(key_of(i)) {
				Ok(Some(value)) if value == value_of(i, 300) => {}
				other => {
					wrong = Some(format!("key {i} reads as {other:?}"));
					break;
				}
			}
		}
	}
	let _ = tree.close().await;
	drop(tree);
	let _ = pool.close();
	wrong
}

/// A close that removes several segments of the log, imaged at every call it makes and with each
/// of its unflushed operations lost in turn (and in a few reorderings): the database that is
/// left opens and has everything that was acknowledged. A removal that is lost while a later one
/// is kept left only the old segments, with a manifest that is past them, and the open said that
/// the manifest was corrupt.
#[test(tokio::test)]
async fn a_crash_while_the_close_removes_the_log_leaves_a_database_that_opens() {
	within("close removes the log", async {
		let raw = MemRaw::new();
		let (fs, _pool) = pool_over(&raw);
		let (probe, fs) = ProbeFs::wrap(&fs);
		let tree = open_tree(&fs, false).await;
		// The memtables stay queued, so that the close has several segments to remove.
		probe.spares_wait(|n| (n == FLUSH_NEED).then_some(Wait::Never));
		let commits = 300;
		for from in (0..commits).step_by(100) {
			commit_range(&tree, from..from + 100, 300, Durability::Immediate).await;
			tree.core.inner.rotate_memtable().unwrap();
		}
		assert!(tree.core.inner.immutable_count() >= 3);
		probe.spares_wait(|_| None);

		let images: Arc<Mutex<Vec<(String, MemRaw)>>> = Arc::default();
		let capture = {
			let (raw, images) = (raw.clone(), Arc::clone(&images));
			move |call: &Call, when: &str| {
				let mut taken = images.lock().unwrap();
				let what = format!("{when} {call:?}");
				for lost in 0..raw.unflushed_ops().min(24) {
					let mode = CrashMode::LoseTear {
						lost,
						torn: usize::MAX,
						n: 0,
					};
					taken.push((format!("{what}, {mode:?}"), raw.crash(mode)));
				}
				for seed in 0..4 {
					taken.push((
						format!("{what}, Reorder({seed})"),
						raw.crash(CrashMode::Reorder(seed)),
					));
				}
			}
		};
		// After each removal of a segment of the log: the ones before it are not flushed yet.
		probe.after(move |call| {
			if matches!(call, Call::RemoveFile(path) if path.extension().is_some_and(|e| e == "wal"))
			{
				capture(call, "after");
			}
		});
		tree.close().await.unwrap();
		probe.clear_hooks();
		drop(tree);

		let images = std::mem::take(&mut *images.lock().unwrap());
		assert!(images.len() >= 20, "images were taken: {}", images.len());
		let mut failures = Vec::new();
		for (what, image) in images {
			if let Some(wrong) = what_is_wrong_with(image, commits).await {
				failures.push(format!("{what}: {wrong}"));
			}
		}
		assert!(
			failures.is_empty(),
			"{} images do not open as a database with every commit; the first: {:#?}",
			failures.len(),
			failures.iter().take(3).collect::<Vec<_>>()
		);
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// `abandon` decides the commits that are in flight
// ---------------------------------------------------------------------------------------------

/// How long a commit has to answer after `abandon`: it must not wait for ever, and it answers
/// at once or not at all.
const ANSWER: std::time::Duration = std::time::Duration::from_secs(5);

/// Starts `count` commits, each in a task of its own, on the current-thread runtime of the test.
/// The transactions are made here: a task that held a `Tree` would close the database when it
/// ended, as a native `Tree` does when any of its clones is dropped.
fn start_commits(
	tree: &Tree,
	range: std::ops::Range<usize>,
) -> Vec<tokio::task::JoinHandle<std::result::Result<(), Error>>> {
	range
		.map(|i| {
			let mut txn = tree.begin().unwrap();
			txn.set(key_of(i), value_of(i, 100)).unwrap();
			tokio::spawn(async move { txn.commit().await })
		})
		.collect()
}

/// Every commit that was in flight when the tree was abandoned ends, with a typed error: none of
/// them waits for a flusher that is gone.
async fn assert_all_decided(handles: Vec<tokio::task::JoinHandle<std::result::Result<(), Error>>>) {
	for (i, handle) in handles.into_iter().enumerate() {
		match tokio::time::timeout(ANSWER, handle).await {
			Ok(Ok(Ok(()))) => {}
			Ok(Ok(Err(Error::PipelineStall))) => {}
			Ok(Ok(Err(other))) => panic!("commit {i} ended with {other:?}, not PipelineStall"),
			Ok(Err(joined)) => panic!("commit {i} panicked: {joined}"),
			Err(_) => {
				panic!("commit {i} was still pending {ANSWER:?} after the tree was abandoned")
			}
		}
	}
}

#[test(tokio::test)]
async fn commits_that_were_accepted_when_the_tree_is_abandoned_end_with_a_typed_error() {
	within("abandon with accepted commits", async {
		let (probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_tree(&fs, false).await;
		// The flusher takes the first commit and waits for files that never come, so the ones
		// that follow are accepted into the ring and nobody drains them.
		probe.spares_wait(|_| Some(Wait::Never));
		let mut handles = start_commits(&tree, 0..1);
		for _ in 0..8 {
			tokio::task::yield_now().await;
		}
		handles.extend(start_commits(&tree, 1..16));
		for _ in 0..8 {
			tokio::task::yield_now().await;
		}
		assert!(
			tree.core.commit_pipeline.accepted_waiting() >= 15,
			"the commits wait in the ring: {}",
			tree.core.commit_pipeline.accepted_waiting()
		);
		tree.abandon();
		assert_all_decided(handles).await;
	})
	.await;
}

#[test(tokio::test)]
async fn commits_that_are_queued_for_admission_when_the_tree_is_abandoned_end_with_a_typed_error() {
	within("abandon with queued commits", async {
		let (_probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_tree(&fs, false).await;
		// Every admission permit is held, so the commits queue for one. They are given the
		// permits when the hold is dropped, and have not run yet when the tree is abandoned.
		let pipeline = Arc::clone(&tree.core.commit_pipeline);
		let hold = pipeline.hold_admission(u32::try_from(pipeline.free_permits()).unwrap());
		let handles = start_commits(&tree, 0..16);
		for _ in 0..8 {
			tokio::task::yield_now().await;
		}
		drop(hold);
		tree.abandon();
		assert_all_decided(handles).await;
	})
	.await;
}

#[test(tokio::test)]
async fn a_commit_that_comes_after_the_tree_is_abandoned_is_refused() {
	within("commit after abandon", async {
		let (_probe, fs) = ProbeFs::wrap(&FileSystem::memory());
		let tree = open_tree(&fs, false).await;
		commit_range(&tree, 0..10, 100, Durability::Immediate).await;
		tree.abandon();
		let mut txn = tree.begin().unwrap();
		txn.set(b"late", b"v").unwrap();
		let answered = tokio::time::timeout(ANSWER, txn.commit()).await;
		assert!(matches!(answered, Ok(Err(Error::PipelineStall))), "{answered:?}");
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// A pool at its cap of files degrades, and does not wedge
// ---------------------------------------------------------------------------------------------

/// A pool of memory that never has more than `max_files` files.
fn capped_pool(max_files: usize) -> (FileSystem, Arc<PoolFs<MemRaw>>) {
	let cfg = PoolConfig {
		max_files,
		..PoolConfig::memory()
	};
	let (pool, _) = PoolFs::<MemRaw>::mount_sync(MemRaw::new(), cfg).expect("a pool mounts");
	(FileSystem(Arc::clone(&pool) as Arc<dyn Fs>), pool)
}

async fn delete_key(tree: &Tree, i: usize) -> std::result::Result<(), Error> {
	let mut txn = tree.begin().unwrap();
	txn.delete(key_of(i)).unwrap();
	txn.commit().await
}

/// A database that fills a pool whose cap is below the full bound of a commit group: the commits
/// that put data are refused with `PoolFull` (typed, and the database is not stopped), what
/// was acknowledged is there, a delete goes through, and deleting what was written lets the
/// commits that put data work again at the same cap, because the compaction that the deletes
/// bring has the files it needs. Before, every commit failed the same way, and so did the
/// compaction, until the cap was raised.
#[test(tokio::test)]
async fn a_pool_at_a_cap_below_the_bound_refuses_puts_and_still_deletes_and_compacts() {
	within("cap below the bound", async {
		let (fs, pool) = capped_pool(32);
		let tree =
			TreeBuilder::with_options(options_in(&fs, MEMTABLE)).build_async().await.unwrap();
		let group_need = tree.core.inner.group_need.load(Ordering::Acquire);
		assert!(
			pool.stats().max_files < 2 * group_need + 32 / 8,
			"the cap is below the bound: {group_need}"
		);
		assert!(tree.core.inner.is_cap_limited());

		let (reached, error) = commit_until_error(&tree, 0, 200_000, 300).await;
		let error = error.expect("a database that grows fills a pool of 32 files");
		assert!(error.is_pool_full(), "typed as a pool that is at its limit: {error:?}");
		assert!(!tree.is_stopped());
		assert!(reached > 100, "it worked until then: {reached}");
		assert_present(&tree, 0..reached.min(2000), 300);

		// The same put is refused again, and does not stop anything.
		let (_, again) = commit_until_error(&tree, reached, reached + 1, 300).await;
		assert!(again.is_some_and(|e| e.is_pool_full()));

		// A delete goes through.
		delete_key(&tree, 0).await.expect("a delete is not refused at the cap");

		// Deleting what was written brings the compactions that make room.
		for i in 1..reached {
			delete_key(&tree, i).await.expect("a delete is not refused at the cap");
		}
		let mut next = reached;
		let started = std::time::Instant::now();
		loop {
			let (n, error) = commit_until_error(&tree, next, next + 1, 300).await;
			next = n;
			if error.is_none() {
				break;
			}
			let error = error.unwrap();
			assert!(error.is_pool_full(), "{error:?}");
			assert!(
				started.elapsed() < std::time::Duration::from_secs(60),
				"the commits that put data did not come back at the same cap: {:?}, {:?}",
				pool.stats(),
				tree.background_error().map(|e| e.error.to_string())
			);
			tokio::time::sleep(std::time::Duration::from_millis(20)).await;
		}
		assert!(!tree.is_stopped());
		assert_eq!(pool.stats().max_files, 32, "the cap was never raised");
		tree.close().await.unwrap();
	})
	.await;
}

/// A pool with the default cap is not `cap_limited`, and keeps what it always kept back.
#[test(tokio::test)]
async fn a_pool_with_the_default_cap_asks_for_the_full_bound() {
	within("default cap", async {
		let (fs, pool) = capped_pool(2048);
		let tree =
			TreeBuilder::with_options(options_in(&fs, MEMTABLE)).build_async().await.unwrap();
		assert!(!tree.core.inner.is_cap_limited());
		let need = tree.core.inner.group_need.load(Ordering::Acquire);
		let shape = crate::lsm::GroupShape {
			usual: 3,
			delete_only: true,
			oversized: false,
		};
		assert_eq!(
			tree.core.inner.commit_group_need(shape, 2),
			need + 2,
			"a delete asks for it too"
		);
		assert_eq!(pool.stats().max_files, 2048);
		tree.close().await.unwrap();
	})
	.await;
}

/// What a group is told it needs in a pool whose cap is below the bound.
#[test(tokio::test)]
async fn in_a_pool_below_the_bound_a_delete_asks_for_less_than_a_put_and_a_big_batch_for_the_bound()
{
	within("shapes", async {
		let (fs, _pool) = capped_pool(32);
		let tree =
			TreeBuilder::with_options(options_in(&fs, MEMTABLE)).build_async().await.unwrap();
		let inner = &tree.core.inner;
		let full = inner.group_need.load(Ordering::Acquire);
		let shape = |delete_only, oversized| crate::lsm::GroupShape {
			usual: 4,
			delete_only,
			oversized,
		};
		let delete = inner.commit_group_need(shape(true, false), 0);
		let put = inner.commit_group_need(shape(false, false), 0);
		let big = inner.commit_group_need(shape(false, true), 0);
		assert!(delete < put, "{delete} < {put}");
		assert!(put < full, "{put} < {full}");
		assert_eq!(big, full, "a batch of its own needs the bound");
		assert!(delete >= crate::lsm::FLUSH_NEED, "never less than a flush needs: {delete}");
		tree.close().await.unwrap();
	})
	.await;
}

/// A browser that runs out of quota while the log is appended to: the committers hear a full
/// storage, as they do when the pool is full, and the database is not stopped.
///
/// The log keeps a full storage that no operating system raised as its own variant
/// (`wal::Error::StorageFull`), and `storage::log_error` and `Error::from(wal::Error)` give it its
/// type back, so that it is soft and the committer can tell it from a disk that failed.
#[test(tokio::test)]
async fn a_quota_while_the_log_is_appended_to_fails_the_commit_with_its_own_type() {
	within("quota at the append", async {
		let raw = MemRaw::new();
		let (fs, _pool) = pool_over(&raw);
		let tree = open_tree(&fs, false).await;
		commit_range(&tree, 0..20, 300, Durability::Immediate).await;
		// Room for a little more, so that the pool's own files are there and an append is what
		// runs out.
		let used: u64 =
			raw.names().iter().filter_map(|name| raw.image(name)).map(|b| b.len() as u64).sum();
		raw.quota(Some(used + 2048));
		let (reached, error) = commit_until_error(&tree, 20, 10_000, 300).await;
		let error = error.expect("the quota is used up");
		assert!(error.is_storage_full(), "the committer hears a full storage: {error:?}");
		assert!(!tree.is_stopped());
		raw.quota(None);
		let (next, error) = commit_until_error(&tree, reached, reached + 20, 300).await;
		assert!(error.is_none(), "{error:?}");
		assert_present(&tree, 0..next, 300);
		tree.close().await.unwrap();
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// A session that ends without a close does not leave a segment of the log behind for ever
// ---------------------------------------------------------------------------------------------

/// A browser tab cannot run an asynchronous close, so most sessions end without one. Every open
/// starts a new segment of the log, and the segments that a flush at the open moved the manifest
/// past are not read again: they have to be removed, or a database that is opened and left again
/// holds one more file of the pool each time, until the pool is full and the open fails.
#[test(tokio::test)]
async fn sessions_that_end_without_a_close_do_not_leave_the_log_to_grow() {
	within("sessions without a close", async {
		for (writes, mode) in [
			(0usize, CrashMode::Prefix(usize::MAX)),
			(5, CrashMode::Prefix(usize::MAX)),
			(80, CrashMode::Prefix(usize::MAX)),
			// What a crash does not keep is what was not synced: the removals of the open too.
			(5, CrashMode::LoseUnsynced),
			(0, CrashMode::ZeroTail),
		] {
			let mut fs = FileSystem::memory();
			let sessions = 40;
			let mut written = 0;
			let mut most = 0;
			for session in 0..sessions {
				let options = options_for(&fs, false);
				let tree = TreeBuilder::with_options(options).build_async().await.unwrap();
				if written > 0 {
					assert_present(&tree, 0..written, 40);
				}
				commit_range(&tree, written..written + writes, 40, Durability::Immediate).await;
				written += writes;
				most = most.max(fs.list_for_test("db/wal").len());
				tree.abandon();
				fs.abandon();
				drop(tree);
				// Everything that was written is kept, as a browser keeps what a worker wrote.
				fs = fs.simulate_crash(mode).expect("a memory pool crashes");
				let _ = session;
			}
			// One for the commits that are not in a table, one that the session started, and one
			// that the last flush at the open leaves: a few, and never a number that grows with
			// the sessions.
			assert!(
				most <= 6,
				"{writes} writes a session, {mode:?}: {most} segments of the log after {sessions} sessions"
			);
		}
	})
	.await;
}

/// The failure that the growth ends in: a pool that holds a fixed number of files, and sessions
/// that end without a close. Every open, and every commit, has to find a file to take, for as long
/// as the sessions go on, and the files that are not tables must not grow with them.
///
/// A session that writes leaves its records to the next open, which puts them in a table, and the
/// tables are merged only as the database grows: so most of these sessions only look, and the
/// ones that write are few.
#[test(tokio::test)]
async fn sessions_that_end_without_a_close_never_fill_a_pool_with_a_limit_of_files() {
	within("sessions in a pool with a limit", async {
		let mut raw = MemRaw::new();
		let mut written = 0;
		let mut most_other_files = 0;
		for session in 0..200 {
			let config = PoolConfig {
				max_files: 70,
				..PoolConfig::memory()
			};
			let (pool, _) = PoolFs::<MemRaw>::mount_sync(raw.clone(), config)
				.expect("a pool mounts over memory");
			let fs = FileSystem(Arc::clone(&pool) as Arc<dyn Fs>);
			let tree = TreeBuilder::with_options(options_for(&fs, false))
				.build_async()
				.await
				.unwrap_or_else(|e| panic!("session {session}: the database opens: {e}"));
			if written > 0 {
				assert_present(&tree, 0..written, 40);
			}
			if session % 10 == 5 {
				commit_range(&tree, written..written + 20, 40, Durability::Immediate).await;
				written += 20;
			}
			let others: usize = ["db/wal", "db/vlog", "db/manifest"]
				.iter()
				.map(|dir| fs.list_for_test(dir).len())
				.sum();
			most_other_files = most_other_files.max(others);
			tree.abandon();
			pool.abandon();
			drop(tree);
			raw = raw.crash(CrashMode::Prefix(usize::MAX));
		}
		assert!(
			most_other_files <= 6,
			"{most_other_files} files that are not tables after 200 sessions"
		);
	})
	.await;
}
