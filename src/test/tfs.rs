//! The file system the engine tests run over, chosen by `SURREALKV_TEST_FS`.
//!
//! * unset or `std`: `std::fs`, as always. `Options::default()` is what it was, and nothing here
//!   runs.
//! * `mem`: one process-wide in-memory pool (`FileSystem::memory()`, with a cap on its files so
//!   high that it is no limit) is the file system of every `Options::default()`. A test that makes
//!   a `TempDir` and a path in it gets that path in the pool, so the tests do not meet. What a test
//!   leaves behind is reclaimed once its `TempDir` is gone from the disk (`reclaim_vanished`).
//! * `mem-strict`: a pool that is refilled only by what waits for spare files (so that a creation
//!   the engine did not make room for fails the test) and holds a bounded number of files. A new
//!   pool is mounted for every test thread, so the tests do not share spares. It is for tests that
//!   run on one thread (`--test-threads=1`): a test on a multi-thread runtime gets the plain pool
//!   of `mem` instead, and says so.
//!
//! A test that cannot run in a pool (it looks into the directory with `std::fs`, or it uses what a
//! pool does not have: permissions, links, a `flock`, the migrators, the sealed log) is in
//! [`EXCLUDED`] with the reason, and runs over `std` instead, and says so on stderr with the
//! prefix `SKV-TEST-FS`. Nothing is dropped without a line: `grep SKV-TEST-FS` over the output of
//! `cargo test -- --nocapture` lists every test, its file system and why, and
//! `SURREALKV_TEST_FS_LOG=<file>` appends the same lines to a file, which is how the counts of
//! the CI jobs are made.
//!
//! The switch is `Options::default().fs` only. The code that has no `Options` (the write-ahead
//! log wrappers, `write_manifest_to_disk`) keeps asking `vfs::default_fs()`, which stays `std`, as
//! the engine's own calls are all made through `opts.fs`.

use std::cell::Cell;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use crate::vfs::{EntryKind, Fs, MemRaw, PoolConfig, PoolFs};

/// The file system of the run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Mode {
	Std,
	Mem,
	MemStrict,
}

impl Mode {
	pub(crate) fn name(self) -> &'static str {
		match self {
			Mode::Std => "std",
			Mode::Mem => "mem",
			Mode::MemStrict => "mem-strict",
		}
	}
}

/// What `SURREALKV_TEST_FS` says. An unknown value is an error, not the default: a typo would
/// otherwise run the std suite and call it a pool run.
pub(crate) fn mode() -> Mode {
	static MODE: LazyLock<Mode> = LazyLock::new(|| match std::env::var("SURREALKV_TEST_FS") {
		Err(_) => Mode::Std,
		Ok(v) => match v.as_str() {
			"" | "std" => Mode::Std,
			"mem" => Mode::Mem,
			"mem-strict" => Mode::MemStrict,
			other => panic!("SURREALKV_TEST_FS={other:?}: expected std, mem or mem-strict"),
		},
	});
	*MODE
}

/// Why a test is not run in a pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Why {
	/// An encrypted database: the sealed log and the sealed value log stay on `std`, and a pool
	/// refuses a key manager.
	Encryption,
	/// Checkpoint and restore: hard links, directory copies and swaps on `std`.
	Checkpoint,
	/// Unix permission bits, which are outside the file system seam.
	Permissions,
	/// The migrators from the formats of other stores, which read the disk.
	Migrator,
	/// The failure is injected with a tool of `std`: the fail points of the manifest replacement,
	/// a read-only directory, a limit of the kernel on the size of a file, a directory that is
	/// renamed. A pool has failures of its own (`ProbeFs`, see `fs_engine_exhaustion_tests`).
	StdFaults,
	/// The test makes a crash image by copying the directory and damages the copy by the ledger of
	/// the files and directories that `std` made durable (`sync_tracker`, the synced directories).
	/// A pool has no ledger: its crash images are `FileSystem::simulate_crash` (see
	/// `fs_conformance_crash_tests`).
	Ledger,
	/// The test counts or bounds the allocations of the write path, or the bytes it retains. A
	/// pool keeps the bytes of its files, so the counts are not the ones of a file on a disk.
	Allocations,
	/// The API under test reads the disk with `std::fs` itself (`describe`, the sealed log, the
	/// orphan plan of the tests of `levels`).
	StdOnlyApi,
	/// The test is about `std` being the default file system, or runs the engine on it by name.
	StdDefault,
	/// The test makes, copies, damages or counts the files of the value log with `std::fs` and
	/// the fsyncs of the disk itself, and the log it drives is on the disk with them.
	StdFiles,
	/// A pool does this differently on purpose (it replaces the manifest without a temporary
	/// name).
	PoolDesign,
	/// Only in `mem-strict`: the test drives a flush, a compaction or the value log by hand, which
	/// waits for no spare files, so a pool that is refilled only by what waits for them runs dry.
	StrictByHand,
	/// A defect of the engine or the pool that a run found: the test fails in a pool and passes on
	/// `std`. The text says which, and the report lists it.
	#[allow(dead_code, reason = "no defect is open: a test that finds one is put here")]
	EngineBug(&'static str),
}

impl Why {
	pub(crate) fn label(self) -> &'static str {
		match self {
			Why::Encryption => "encryption",
			Why::Checkpoint => "checkpoint",
			Why::Permissions => "permissions",
			Why::Migrator => "migrator",
			Why::StdFaults => "std-faults",
			Why::Ledger => "ledger",
			Why::Allocations => "allocations",
			Why::StdOnlyApi => "std-only-api",
			Why::StdDefault => "std-default",
			Why::StdFiles => "std-files",
			Why::PoolDesign => "pool-design",
			Why::StrictByHand => "strict-by-hand",
			Why::EngineBug(_) => "engine-bug",
		}
	}

	pub(crate) fn text(self) -> &'static str {
		match self {
			Why::Encryption => "an encrypted database keeps its sealed files on std",
			Why::Checkpoint => "checkpoint and restore use links and directory swaps on std",
			Why::Permissions => "unix permission bits are outside the file system seam",
			Why::Migrator => "the migrators read the disk of another store",
			Why::StdFaults => "the failure is injected with a tool of std",
			Why::Ledger => "the crash image is made with the fsync ledger of std",
			Why::Allocations => "the test counts the allocations of the write path",
			Why::StdOnlyApi => "the API under test reads the disk with std::fs",
			Why::StdDefault => "the test is about std being the default file system",
			Why::StdFiles => "the test makes and damages the files of the log with std::fs",
			Why::PoolDesign => "a pool does this differently on purpose",
			Why::StrictByHand => "the test drives the work by hand, which waits for no spare files",
			Why::EngineBug(text) => text,
		}
	}
}

/// A test that runs over `std` in every mode, with the reason. A `pattern` that ends in `::` is
/// every test of that module (the name of the test thread is the path of the test, such as
/// `test::lsm_tests::name`), any other is the one test.
pub(crate) struct Rule {
	pub(crate) pattern: &'static str,
	pub(crate) why: Why,
}

pub(crate) const fn rule(pattern: &'static str, why: Why) -> Rule {
	Rule {
		pattern,
		why,
	}
}

pub(crate) use super::tfs_excluded::{EXCLUDED, EXCLUDED_STRICT};

/// The part of a thread name that says it is a test: the path of the test.
fn test_name() -> Option<String> {
	let thread = std::thread::current();
	let name = thread.name()?;
	name.contains("::").then(|| name.to_string())
}

fn matches(rule: &Rule, name: &str) -> bool {
	if rule.pattern.ends_with("::") {
		name.starts_with(rule.pattern)
	} else {
		name == rule.pattern
	}
}

/// The rule that leaves `name` out of the run, if there is one: the table of every pool run, and in
/// `mem-strict` the one of the tests that need more files than the waits ask for.
fn excluded(name: &str) -> Option<&'static Rule> {
	let strict = (mode() == Mode::MemStrict).then(|| EXCLUDED_STRICT.iter());
	EXCLUDED.iter().chain(strict.into_iter().flatten()).find(|r| matches(r, name))
}

/// Whether the test that calls it is in [`EXCLUDED`] and is to be left out of a pool run. The
/// first line of such a test is `if tfs::skip_listed() { return; }`, which says on stderr that the
/// test was not run, and why, with the prefix `SKV-TEST-FS`. Over `std` it is `false` and costs a
/// comparison.
pub(crate) fn skip_listed() -> bool {
	if mode() == Mode::Std {
		return false;
	}
	let Some(name) = test_name() else {
		return false;
	};
	let Some(rule) = excluded(&name) else {
		return false;
	};
	note(&name, "skipped", &format!("{}: {}", rule.why.label(), rule.why.text()));
	true
}

// ---------------------------------------------------------------------------------------------
// The record of what ran where
// ---------------------------------------------------------------------------------------------

thread_local! {
	static NOTED: Cell<bool> = const { Cell::new(false) };
}

/// Says once per test thread where the test runs and, if it is not in the pool, why. A thread that
/// is not a test (a worker of a runtime) is not a test and says nothing.
fn note(test: &str, fs: &str, why: &str) {
	if test == "-" || NOTED.with(|n| n.replace(true)) {
		return;
	}
	let line = format!("SKV-TEST-FS mode={} fs={fs} test={test} why={why}\n", mode().name());
	// A test that is left out, or that runs over `std`, says so where a passing test's output is
	// not hidden: written to the stream itself, which the capture of the harness does not see.
	// The tests that run in the pool are in the log file only.
	if fs == "skipped" || fs == "std" {
		let _ = std::io::stderr().lock().write_all(line.as_bytes());
	}
	if let Ok(path) = std::env::var("SURREALKV_TEST_FS_LOG") {
		if let Ok(mut file) = std::fs::OpenOptions::new().create(true).append(true).open(path) {
			// One write, so that two records never share a line.
			let _ = file.write_all(line.as_bytes());
		}
	}
}

// ---------------------------------------------------------------------------------------------
// The pools
// ---------------------------------------------------------------------------------------------

/// The most files of the unbounded pool: no limit that a test reaches.
const UNBOUNDED: usize = 1 << 24;

/// The pool of `mem`.
fn mount_unbounded() -> Arc<dyn Fs> {
	let cfg = PoolConfig {
		max_files: UNBOUNDED,
		..PoolConfig::memory()
	};
	let (pool, _) =
		PoolFs::<MemRaw>::mount_sync(MemRaw::new(), cfg).expect("the memory pool mounts");
	pool
}

/// The spares a strict pool starts with and the most files it holds.
pub(crate) const STRICT_INITIAL: usize = 128;
pub(crate) const STRICT_MAX: usize = 4096;

/// A pool of `mem-strict`: refilled only by what waits for spare files.
pub(crate) fn mount_strict() -> Arc<dyn Fs> {
	let (pool, _) =
		PoolFs::<MemRaw>::mount_sync(MemRaw::new(), PoolConfig::strict(STRICT_INITIAL, STRICT_MAX))
			.expect("the strict pool mounts");
	pool
}

static SHARED: LazyLock<Arc<dyn Fs>> = LazyLock::new(mount_unbounded);

/// The strict pool of the test thread that last asked, with the name of that thread.
static STRICT: Mutex<Option<(String, Arc<dyn Fs>)>> = Mutex::new(None);

/// What the test thread that asked last was given, for the threads that are not tests.
static LAST: Mutex<Option<Arc<dyn Fs>>> = Mutex::new(None);

static DEFAULTS: AtomicU64 = AtomicU64::new(0);

/// How many `Options::default()` there are between two reclaims of `mem`.
const RECLAIM_EVERY: u64 = 256;

/// Whether the call is made from a runtime that has more than one thread.
fn on_multi_thread_runtime() -> bool {
	tokio::runtime::Handle::try_current()
		.is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
}

/// The file system of `Options::default()` in a test, or `None` for `std`.
pub(crate) fn options_fs() -> Option<Arc<dyn Fs>> {
	pick(true)
}

/// The file system that the files of the test are in: what `Options::default()` has, asked
/// again, by the code of the tests that looks into the directory of a database (`sfs`). `None`
/// is the disk.
pub(crate) fn ambient() -> Option<Arc<dyn Fs>> {
	pick(false)
}

/// Decides the file system of the thread that asks. `first` is whether this is an
/// `Options::default()`, which is noted and counted; the other askers see the same answer
/// without a word.
fn pick(first: bool) -> Option<Arc<dyn Fs>> {
	let mode = mode();
	if mode == Mode::Std {
		return None;
	}
	let test = test_name();
	if let Some(name) = &test {
		if let Some(rule) = excluded(name) {
			if first {
				note(name, "std", &format!("{}: {}", rule.why.label(), rule.why.text()));
			}
			return None;
		}
	}
	let label = test.as_deref().unwrap_or("-");
	if test.is_some() {
		REAPER.with(|_| ());
	}
	if mode == Mode::MemStrict {
		let Some(name) = test else {
			// A thread that is not a test (a worker of the runtime, a thread that the test made)
			// belongs to the test that is running, which in this mode is the one that asked last.
			return Some(LAST.lock().unwrap().clone().unwrap_or_else(|| Arc::clone(&SHARED)));
		};
		let fs = if on_multi_thread_runtime() {
			if first {
				note(&name, "mem", "multi-thread runtime: a strict pool assumes one thread");
			}
			Arc::clone(&SHARED)
		} else {
			let mut current = STRICT.lock().unwrap();
			match &*current {
				Some((owner, fs)) if *owner == name => Arc::clone(fs),
				_ => {
					let fs = mount_strict();
					*current = Some((name.clone(), Arc::clone(&fs)));
					if first {
						note(&name, "mem-strict", "-");
					}
					fs
				}
			}
		};
		*LAST.lock().unwrap() = Some(Arc::clone(&fs));
		return Some(fs);
	}
	if first {
		note(label, "mem", "-");
		return Some(shared());
	}
	Some(Arc::clone(&SHARED))
}

/// The plain pool, reclaimed now and then.
fn shared() -> Arc<dyn Fs> {
	let fs = Arc::clone(&SHARED);
	if DEFAULTS.fetch_add(1, Ordering::Relaxed) % RECLAIM_EVERY == RECLAIM_EVERY - 1 {
		reclaim_vanished(&*fs);
	}
	fs
}

// ---------------------------------------------------------------------------------------------
// Memory
// ---------------------------------------------------------------------------------------------

/// Removes from `fs` every child of the system's temporary directory that is gone from the disk: a
/// test makes its database in a `TempDir`, and when the test is over the directory is removed from
/// the disk and its files in the pool would stay for ever. Only the children of the temporary
/// directory are asked about: the directories below them were made by the tree, in the pool, and
/// are not on the disk. A test that is running has its `TempDir` on the disk (it is made before
/// the first file), so it is never taken. Returns how many were removed.
pub(crate) fn reclaim_vanished(fs: &dyn Fs) -> usize {
	let temp = std::env::temp_dir();
	let mut roots = vec![temp.clone()];
	if let Ok(canonical) = temp.canonicalize() {
		if canonical != temp {
			roots.push(canonical);
		}
	}
	let mut removed = 0;
	for root in roots {
		let Ok(entries) = fs.read_dir(&root) else {
			continue;
		};
		for entry in entries {
			if entry.kind != EntryKind::Dir {
				continue;
			}
			let path = root.join(&entry.name);
			if !Path::new(&path).exists() && fs.remove_dir_all(&path).is_ok() {
				removed += 1;
			}
		}
	}
	removed
}

/// Dropped when a test thread ends, which is after the test dropped its `TempDir`: reclaims what
/// the test left in the plain pool, so that the pool holds what the running tests hold.
struct Reaper;

impl Drop for Reaper {
	fn drop(&mut self) {
		if mode() != Mode::Std {
			reclaim_vanished(&**SHARED);
		}
	}
}

thread_local! {
	static REAPER: Reaper = const { Reaper };
}

/// `std::fs::create_dir_all` for the directory of a database, through the file system of its
/// options: what the tests that build a `LevelManifest` or a table by hand call before they do.
pub(crate) fn create_dir_all(opts: &crate::Options, dir: impl AsRef<Path>) -> std::io::Result<()> {
	opts.fs.create_dir_all(dir.as_ref())
}

/// A file that a test writes a table to: the disk's, or the pool's.
pub(crate) enum TestWriter {
	Std(std::fs::File),
	Pool(crate::vfs::FsWriter),
}

impl std::io::Write for TestWriter {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		match self {
			TestWriter::Std(file) => file.write(buf),
			TestWriter::Pool(writer) => writer.write(buf),
		}
	}

	fn flush(&mut self) -> std::io::Result<()> {
		match self {
			TestWriter::Std(file) => file.flush(),
			TestWriter::Pool(writer) => writer.flush(),
		}
	}
}

/// A table file to write, made through the file system of `opts`: what `File::create` was for
/// the tests that write a table by hand. On `std` it is `File::create`.
pub(crate) fn create_file(
	opts: &crate::Options,
	path: impl AsRef<Path>,
) -> std::io::Result<TestWriter> {
	if opts.fs.kind() == crate::vfs::FsKind::Std {
		return std::fs::File::create(path).map(TestWriter::Std);
	}
	// A `TempDir` is on the disk and not in a pool.
	if let Some(parent) = path.as_ref().parent() {
		if !parent.as_os_str().is_empty() && !opts.fs.exists(parent) {
			opts.fs.create_dir_all(parent)?;
		}
	}
	opts.fs
		.create_truncate(path.as_ref(), crate::vfs::CreateOpts::default())
		.map(|file| TestWriter::Pool(crate::vfs::FsWriter(file)))
}

/// The file at `path` made durable and opened to read, through the file system of `opts`. On
/// `std` it is `File::open` and `sync_all`.
pub(crate) fn open_synced(
	opts: &crate::Options,
	path: impl AsRef<Path>,
) -> std::io::Result<Arc<dyn crate::vfs::File>> {
	if opts.fs.kind() == crate::vfs::FsKind::Std {
		let file = std::fs::File::open(path)?;
		file.sync_all()?;
		return Ok(Arc::new(file));
	}
	opts.fs.sync_file(path.as_ref())?;
	let file: Arc<dyn crate::vfs::FsFile> = opts.fs.open_read(path.as_ref())?;
	Ok(file)
}

/// `write_manifest_to_disk` through the file system that `Options::default()` has: for the tests
/// that build a manifest by hand.
pub(crate) fn write_manifest_to_disk(manifest: &crate::levels::LevelManifest) -> crate::Result<()> {
	crate::levels::write_manifest_to_disk_in(&*crate::vfs::options_default_fs(), manifest)
}

/// `decode_segment_batches` over the file system of the test: the segment is read where the tree
/// of the test wrote it.
pub(crate) fn decode_segment_batches(
	file_path: &Path,
	segment_id: u64,
) -> crate::Result<(u64, Vec<crate::batch::Batch>)> {
	let mut context = crate::wal::WalContext::plaintext();
	if let Some(fs) = ambient() {
		context.fs = fs;
	}
	crate::wal::parallel_recovery::decode_segment_batches_with(&context, file_path, segment_id)
}

/// The options of a write-ahead log that is opened by hand: the default, in the file system of the
/// test.
pub(crate) fn wal_options() -> crate::wal::Options {
	let mut options = crate::wal::Options::default();
	if let Some(fs) = ambient() {
		options.context.fs = fs;
	}
	options
}

/// `wal::list_segment_ids` in the file system of the test.
pub(crate) fn list_segment_ids(
	dir: &Path,
	allowed_extension: Option<&str>,
) -> crate::wal::Result<Vec<u64>> {
	let fs = ambient().unwrap_or_else(crate::vfs::default_fs);
	crate::wal::list_segment_ids_in(&*fs, dir, allowed_extension)
}

/// [`wal_options`] for a log that is opened in `dir` by hand: the directory is made in the file
/// system of the test if it is not there (`Tree::new` does that for a tree, and a `TempDir` is on
/// the disk and not in a pool).
pub(crate) fn wal_options_for(dir: &Path) -> crate::wal::Options {
	let options = wal_options();
	// A `TempDir` is on the disk and not in a pool; on `std` the log is opened as it always was.
	if options.context.fs.kind() != crate::vfs::FsKind::Std && !options.context.fs.exists(dir) {
		let _ = options.context.fs.create_dir_all(dir);
	}
	options
}

/// `replay_wal` over the file system of the test.
pub(crate) fn replay_wal(
	wal_dir: &Path,
	min_wal_number: u64,
	arena_size: usize,
) -> crate::Result<crate::wal::recovery::ReplayResult> {
	let mut context = crate::wal::WalContext::plaintext();
	if let Some(fs) = ambient() {
		context.fs = fs;
	}
	let (max_seq, memtables, _, _) =
		crate::wal::recovery::replay_wal_inner_with(&context, wal_dir, min_wal_number, arena_size)?;
	Ok((max_seq, memtables))
}

/// `validate_wal_log_number` over the file system of the test.
pub(crate) fn validate_wal_log_number(
	wal_path: &Path,
	manifest_log_number: u64,
) -> crate::Result<()> {
	let fs = ambient().unwrap_or_else(crate::vfs::default_fs);
	crate::levels::validate_wal_log_number_in(&*fs, wal_path, manifest_log_number)
}

/// `VLog::new` in the file system of the options.
pub(crate) fn vlog_new(opts: Arc<crate::Options>) -> crate::Result<crate::vlog::VLog> {
	crate::vlog::VLog::new_in(Arc::clone(&opts.fs), opts)
}

/// `quarantine_wal_segment` in the file system of the test.
pub(crate) fn quarantine_wal_segment(wal_dir: &Path, segment_id: usize) -> crate::Result<PathBuf> {
	match ambient() {
		None => crate::wal::recovery::quarantine_wal_segment(wal_dir, segment_id),
		Some(fs) => crate::wal::recovery::quarantine_wal_segment_in(&*fs, wal_dir, segment_id),
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeSet;

	use tempfile::TempDir;

	use super::*;

	#[test]
	fn the_table_has_no_pattern_twice_and_every_reason_is_said() {
		let mut seen = BTreeSet::new();
		for rule in EXCLUDED.iter().chain(EXCLUDED_STRICT) {
			assert!(seen.insert(rule.pattern), "{} is in the table twice", rule.pattern);
			assert!(!rule.why.text().is_empty() && !rule.why.label().is_empty());
			assert!(
				rule.pattern.contains("::") && !rule.pattern.contains(' '),
				"{:?} is not the path of a test",
				rule.pattern
			);
		}
		assert!(
			EXCLUDED_STRICT.iter().all(|rule| !EXCLUDED.iter().any(|r| r.pattern == rule.pattern)),
			"a test that is left out of every run is not left out of the strict one again"
		);
	}

	/// The skips match the name of the thread of a test, which is its path. A harness that ran
	/// the tests unnamed would match none of them and fail hundreds of tests: this fails once,
	/// and says why.
	#[test]
	fn a_test_thread_is_named_by_the_path_of_its_test() {
		if mode() == Mode::Std {
			return;
		}
		let thread = std::thread::current();
		assert_eq!(
			thread.name(),
			Some("test::tfs::tests::a_test_thread_is_named_by_the_path_of_its_test"),
			"the tests of a pool run are matched to the table by the name of their thread"
		);
	}

	#[test]
	fn a_rule_that_ends_in_two_colons_is_a_module_and_any_other_is_one_test() {
		let module = rule("test::a_module::", Why::Checkpoint);
		assert!(matches(&module, "test::a_module::a_test"));
		assert!(matches(&module, "test::a_module::nested::a_test"));
		assert!(!matches(&module, "test::a_module_two::a_test"));
		let one = rule("test::a_module::a_test", Why::Checkpoint);
		assert!(matches(&one, "test::a_module::a_test"));
		assert!(!matches(&one, "test::a_module::a_test_two"));
	}

	#[test]
	fn what_a_test_left_in_a_pool_is_reclaimed_once_its_temporary_directory_is_gone() {
		// A pool of its own, so that no test in flight is touched.
		let fs = mount_unbounded();
		let root = TempDir::new().unwrap();
		let gone = TempDir::new().unwrap();
		for dir in [root.path(), gone.path()] {
			fs.create_dir_all(&dir.join("db/wal")).unwrap();
			fs.create_new(&dir.join("db/wal/0.wal"), Default::default()).unwrap();
		}
		let gone_path = gone.path().to_path_buf();
		drop(gone);
		assert!(reclaim_vanished(&*fs) >= 1);
		assert!(!fs.exists(&gone_path), "the directory that is gone from the disk is gone");
		assert!(fs.exists(&root.path().join("db/wal/0.wal")), "a directory that is there is kept");
	}

	#[test]
	fn the_pools_are_what_the_modes_say() {
		let strict = mount_strict();
		let stats = strict.pool().unwrap().stats();
		assert_eq!(stats.max_files, STRICT_MAX);
		assert!(stats.clean >= STRICT_INITIAL, "the strict pool starts with its spares");
		let plain = mount_unbounded();
		assert_eq!(plain.pool().unwrap().stats().max_files, UNBOUNDED);
	}
}
