//! The finish line of the browser port: the whole engine, over a database in the origin private
//! file system of a real headless Chrome, in a dedicated worker. Run it, from the repository root,
//! with
//!
//! ```text
//! export CC_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/clang
//! export AR_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/llvm-ar
//! WASM_BINDGEN_TEST_TIMEOUT=120 WASM_BINDGEN_USE_DEDICATED_WORKER=1 \
//!     wasm-pack test --headless --chrome --test opfs_engine_test
//! ```
//!
//! The same sequence runs twice, without the value log and with it (values above its threshold):
//!
//! 1. mount a pool, and see that a second mount of it fails with the typed lock error;
//! 2. build a tree over it, and commit (with `Durability::Immediate`: the acknowledged set) until
//!    the log has rotated, memtables have been flushed to tables, and a compaction has removed
//!    tables, which the test waits for and does not assume;
//! 3. read everything back, with `get` and `get_async`;
//! 4. close the tree and open it again over the same mount, then close both and mount again;
//! 5. commit more, with and without waiting for the flush, and end the tree and the mount the way a
//!    crash does (no close, no flush), mount again and open: every acknowledged commit is there,
//!    and what was not acknowledged is a prefix;
//! 6. cut the log in the middle of a record that was not acknowledged, as a crash in the middle of
//!    a write does, and open: the torn tail is trimmed and a quarantine copy is made;
//! 7. remove the database, and mount again, empty.
//!
//! What this does not show is what survives a power cut, or the death of the worker in the middle
//! of a write. `flush` of the access handle of a browser orders writes, and no claim is made about
//! more than that.

#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

mod opfs_common;

use std::collections::BTreeSet;
use std::io::ErrorKind;
use std::time::Duration;

use opfs_common::{pattern, root, sleep_ms, unique_name, watchdog};
use surrealkv::{Durability, Error, FileSystem, Options, Tree, TreeBuilder};
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;
use web_sys::{FileSystemDirectoryHandle, FileSystemGetDirectoryOptions, FileSystemGetFileOptions};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// Keys in one transaction.
const PER_TXN: usize = 40;

/// The memtable of the test: small, so that the log rotates and tables are made often.
const MEMTABLE: usize = 256 * 1024;

fn key(i: usize) -> Vec<u8> {
	format!("key-{i:08}").into_bytes()
}

/// A value that differs from every other and is above the threshold of the value log when there is
/// one.
fn value(i: usize, vlog: bool) -> Vec<u8> {
	pattern(
		i as u64 + 1,
		if vlog {
			600
		} else {
			160
		},
	)
}

fn builder(fs: &FileSystem, vlog: bool) -> TreeBuilder {
	let mut options = Options::default()
		.with_file_system(fs.clone())
		.with_path("db".into())
		.with_max_memtable_size(MEMTABLE)
		.with_enable_vlog(vlog)
		.with_vlog_value_threshold(256);
	options.level0_max_files = 4;
	TreeBuilder::with_options(options)
}

async fn open(fs: &FileSystem, vlog: bool) -> Tree {
	watchdog("build_async", builder(fs, vlog).build_async())
		.await
		.unwrap_or_else(|e| panic!("build_async failed: {e}"))
}

async fn commit(tree: &Tree, range: std::ops::Range<usize>, vlog: bool, durability: Durability) {
	let all: Vec<usize> = range.collect();
	for chunk in all.chunks(PER_TXN) {
		let mut txn = tree.begin().expect("begin");
		txn.set_durability(durability);
		for &i in chunk {
			txn.set(key(i), value(i, vlog)).expect("set");
		}
		txn.commit().await.unwrap_or_else(|e| panic!("commit of {chunk:?}: {e}"));
	}
}

/// Every key of `range`, with `get` and with `get_async`.
async fn read_back(tree: &Tree, range: std::ops::Range<usize>, vlog: bool) {
	let txn = tree.begin().expect("begin a reader");
	for i in range {
		let want = value(i, vlog);
		assert_eq!(txn.get(key(i)).unwrap().as_deref(), Some(&want[..]), "get of key {i}");
		if i % 7 == 0 {
			let got = txn.get_async(key(i)).await.unwrap();
			assert_eq!(got.as_deref(), Some(&want[..]), "get_async of key {i}");
		}
	}
}

/// Which of the keys of `range` are there.
fn present(tree: &Tree, range: std::ops::Range<usize>, vlog: bool) -> Vec<bool> {
	let txn = tree.begin().expect("begin a reader");
	range
		.map(|i| match txn.get(key(i)).unwrap() {
			Some(got) => {
				assert_eq!(got, value(i, vlog), "key {i} is there with the wrong value");
				true
			}
			None => false,
		})
		.collect()
}

fn names(fs: &FileSystem, dir: &str) -> Vec<String> {
	fs.list_for_test(dir)
}

fn wal_segments(fs: &FileSystem) -> Vec<String> {
	names(fs, "db/wal").into_iter().filter(|name| name.ends_with(".wal")).collect()
}

/// The segment that a commit goes to: the highest one that holds anything.
fn active_segment(fs: &FileSystem) -> String {
	let path = |name: &String| format!("db/wal/{name}");
	wal_segments(fs)
		.into_iter()
		.rev()
		.find(|name| fs.file_len_for_test(&path(name)).unwrap_or(0) > 0)
		.map(|name| path(&name))
		.expect("a log segment that holds something")
}

/// Where the crash of [`crash_round`] cuts the log.
#[derive(Clone, Copy)]
enum Cut {
	/// Nowhere.
	Nothing,
	/// A few bytes into the first record that was not acknowledged.
	InsideTheFirst,
	/// Half way through what was not acknowledged.
	Middle,
}

/// Commits from `start` (one acknowledged batch, then three that are not), ends the tree and the
/// file system the way a crash does, mounts again, cuts the log as `cut` says, and opens the
/// database. Returns what is open now, and where the acknowledged keys and the others end.
async fn crash_round(
	name: &str,
	fs: FileSystem,
	tree: Tree,
	start: usize,
	vlog: bool,
	cut: Cut,
) -> (FileSystem, Tree, usize, usize) {
	let acked_end = start + PER_TXN;
	commit(&tree, start..acked_end, vlog, Durability::Immediate).await;
	let segment = active_segment(&fs);
	let acked_len = fs.file_len_for_test(&segment).unwrap();
	let eventual_end = acked_end + PER_TXN * 3;
	commit(&tree, acked_end..eventual_end, vlog, Durability::Eventual).await;
	assert_eq!(active_segment(&fs), segment, "the log did not rotate");
	let total_len = fs.file_len_for_test(&segment).unwrap();
	assert!(total_len >= acked_len + 64, "{acked_len} {total_len}");
	tree.abandon();
	fs.abandon();
	// The old tree and file system are still here when the new mount asks, so what lets it take
	// the lock is the abandon, and not the last reference going away. A page that comes back
	// mounts with the wait that a reload needs.
	let fresh = FileSystem::opfs(name).await.expect("mount after a crash");
	drop(tree);
	drop(fs);
	let fs = fresh;
	match cut {
		Cut::Nothing => {}
		Cut::InsideTheFirst => {
			fs.raw_truncate_for_test(&segment, acked_len + 3).expect("cut the log inside a record")
		}
		Cut::Middle => fs
			.raw_truncate_for_test(&segment, acked_len + (total_len - acked_len) / 2)
			.expect("cut the log half way"),
	}
	let tree = open(&fs, vlog).await;
	(fs, tree, acked_end, eventual_end)
}

fn is_locked(error: &Error) -> bool {
	error.is_database_locked()
}

async fn finish_line(vlog: bool) {
	let started = js_sys::Date::now();
	let lap = |what: &str| {
		console_log!("  [{:>6.0} ms] {what}", js_sys::Date::now() - started);
	};
	let name = unique_name("skv-engine", "db");

	// 1. Mount, and a second mount of the same pool.
	let fs = FileSystem::opfs(&name).await.expect("mount a fresh pool");
	let report = fs.mount_report().expect("a pool has a mount report");
	assert_eq!((report.files, report.orphans_kept.len()), (0, 0), "{report:?}");
	let second =
		FileSystem::opfs_with_lock_wait(&name, std::time::Duration::from_millis(300)).await;
	match second {
		Err(e) => {
			assert!(is_locked(&e), "the second mount fails with the lock error: {e}");
			assert!(e.to_string().contains("another tab"), "{e}");
		}
		Ok(_) => panic!("a second mount of a mounted pool must fail"),
	}

	lap("1 mounted");
	// 2. A tree, and commits until everything the engine does has happened.
	let tree = open(&fs, vlog).await;
	let mut next = 0usize;
	let mut tables_seen: BTreeSet<String> = BTreeSet::new();
	let mut most_segments = 0usize;
	// The log has rotated when more than one segment was seen: a segment that holds nothing is
	// removed at the open, and one that a flush has passed is removed at once.
	let mut segments_seen: BTreeSet<String> = BTreeSet::new();
	let mut tables_removed = false;
	loop {
		commit(&tree, next..next + PER_TXN * 4, vlog, Durability::Immediate).await;
		next += PER_TXN * 4;
		let now: BTreeSet<String> = names(&fs, "db/sstables").into_iter().collect();
		tables_removed |= tables_seen.iter().any(|seen| !now.contains(seen));
		tables_seen.extend(now);
		most_segments = most_segments.max(wal_segments(&fs).len());
		segments_seen.extend(wal_segments(&fs));
		let flushed = tables_seen.len() >= 5;
		if segments_seen.len() >= 2 && flushed && tables_removed {
			break;
		}
		assert!(next < 60_000, "no compaction after {next} keys: {tables_seen:?}");
		// Let the flush and the compaction run between two batches.
		sleep_ms(1.0).await;
	}
	console_log!(
		"{} keys, {} tables seen, {} log segments seen, most at once {most_segments}, vlog files {:?}",
		next,
		tables_seen.len(),
		segments_seen.len(),
		names(&fs, "db/vlog")
	);
	if vlog {
		assert!(
			names(&fs, "db/vlog")
				.iter()
				.any(|n| fs.file_len_for_test(&format!("db/vlog/{n}")) > Some(0)),
			"the value log holds values"
		);
	} else {
		assert!(names(&fs, "db/vlog")
			.iter()
			.all(|n| fs.file_len_for_test(&format!("db/vlog/{n}")) == Some(0)));
	}
	assert!(!names(&fs, "db/manifest").is_empty(), "a manifest was written");

	lap("2 committed until everything happened");
	// 3. Everything back.
	read_back(&tree, 0..next, vlog).await;

	lap("3 read back");
	// 4. Close the tree, open it again over the same mount, then close both and mount again.
	tree.close().await.expect("close the tree");
	drop(tree);
	let tree = open(&fs, vlog).await;
	read_back(&tree, 0..next, vlog).await;
	tree.close().await.expect("close the reopened tree");
	drop(tree);
	fs.close().await.expect("close the file system");
	fs.close().await.expect("and closing again does nothing");
	// The close released the lock, and the old file system is still here: the next mount takes it
	// at once, without waiting for anything.
	let remounted = FileSystem::opfs_with_lock_wait(&name, std::time::Duration::ZERO)
		.await
		.expect("mount again, at once");
	drop(fs);
	let fs = remounted;
	let report = fs.mount_report().expect("a mount report");
	assert!(report.orphans_kept.is_empty(), "{report:?}");
	assert!(report.files > 0, "{report:?}");
	let tree = open(&fs, vlog).await;
	read_back(&tree, 0..next, vlog).await;

	lap("4 closed, reopened, remounted");
	// 5. More commits, acknowledged and not, and then a crash that loses nothing: the worker is
	// gone, and what it had written is in the file system of the browser.
	let (fs, tree, acked_end, eventual_end) =
		crash_round(&name, fs, tree, next, vlog, Cut::Nothing).await;
	read_back(&tree, 0..acked_end, vlog).await;
	let kept = present(&tree, acked_end..eventual_end, vlog);
	let prefix = kept.iter().take_while(|k| **k).count();
	assert!(kept[prefix..].iter().all(|k| !*k), "what was not acknowledged is a prefix: {kept:?}");
	console_log!("after a crash: {prefix} of {} unacknowledged commits were kept", kept.len());
	// The unacknowledged keys that were lost are written again, so the next steps know what is
	// there.
	commit(&tree, acked_end + prefix..eventual_end, vlog, Durability::Immediate).await;
	next = eventual_end;

	lap("5 crash without a cut");
	// 6. A crash in the middle of a record that was not acknowledged: the log is cut inside the
	//    first
	// of them, below which nothing is touched.
	let (fs, tree, acked_end, eventual_end) =
		crash_round(&name, fs, tree, next, vlog, Cut::InsideTheFirst).await;
	let report = tree.recovery_report();
	assert!(report.bytes_trimmed > 0, "a torn tail was trimmed: {report:?}");
	assert!(!report.quarantined_files.is_empty(), "a quarantine copy was made: {report:?}");
	console_log!("the torn tail: {report:?}");
	for copy in &report.quarantined_files {
		let copy = copy.to_string_lossy().to_string();
		assert!(fs.file_len_for_test(&copy).is_some(), "the quarantine copy {copy} exists");
	}
	read_back(&tree, 0..acked_end, vlog).await;
	let kept = present(&tree, acked_end..eventual_end, vlog);
	assert!(kept.iter().all(|k| !*k), "nothing of what was cut off is there: {kept:?}");
	next = acked_end;
	commit(&tree, next..eventual_end, vlog, Durability::Immediate).await;
	next = eventual_end;

	lap("6 torn tail");
	// 6b. The same, with the cut half way through what was not acknowledged: some of it is there,
	// as whole commits, from the start.
	let (fs, tree, acked_end, eventual_end) =
		crash_round(&name, fs, tree, next, vlog, Cut::Middle).await;
	assert!(tree.recovery_report().bytes_trimmed > 0, "{:?}", tree.recovery_report());
	read_back(&tree, 0..acked_end, vlog).await;
	let kept = present(&tree, acked_end..eventual_end, vlog);
	let prefix = kept.iter().take_while(|k| **k).count();
	assert!(kept[prefix..].iter().all(|k| !*k), "what was not acknowledged is a prefix: {kept:?}");
	assert!(
		prefix > 0 && prefix < kept.len(),
		"a cut in the middle keeps some: {prefix} of {}",
		kept.len()
	);
	assert_eq!(prefix % PER_TXN, 0, "commits are whole");
	console_log!("a cut in the middle kept {prefix} of {} unacknowledged keys", kept.len());

	lap("6b cut half way");
	// 7. Remove the database, and mount again, empty. A database that is open cannot be removed.
	let refused = FileSystem::destroy_opfs(&name).await.expect_err("it is open");
	assert!(is_locked(&refused), "{refused}");
	read_back(&tree, 0..acked_end, vlog).await;
	tree.close().await.expect("close the tree");
	drop(tree);
	fs.close().await.expect("close the file system");
	drop(fs);
	FileSystem::destroy_opfs(&name).await.expect("destroy the database");
	FileSystem::destroy_opfs(&name).await.expect("and destroying again does nothing");
	let fs = FileSystem::opfs(&name).await.expect("mount the removed database again");
	let report = fs.mount_report().unwrap();
	assert_eq!(report.files, 0, "{report:?}");
	assert!(names(&fs, "db/sstables").is_empty() && names(&fs, "db/wal").is_empty());
	fs.close().await.unwrap();
	drop(fs);
	FileSystem::destroy_opfs(&name).await.expect("clean up");
	lap("7 removed");
	console_log!(
		"the finish line with vlog={vlog} took {:.1} s",
		(js_sys::Date::now() - started) / 1000.0
	);
	// The test has a time of its own to keep to: CI gives a binary two minutes.
	assert!(js_sys::Date::now() - started < 100_000.0, "too slow");
}

#[wasm_bindgen_test]
async fn the_engine_runs_over_the_origin_private_file_system_without_the_value_log() {
	finish_line(false).await;
}

#[wasm_bindgen_test]
async fn the_engine_runs_over_the_origin_private_file_system_with_the_value_log() {
	finish_line(true).await;
}

/// The file of the value log that the commits are in: the highest one.
fn value_log_file(fs: &FileSystem) -> String {
	names(fs, "db/vlog")
		.into_iter()
		.rfind(|name| name.ends_with(".vlog"))
		.map(|name| format!("db/vlog/{name}"))
		.expect("a file of the value log")
}

/// The open holds the last segment of the log to the value log. After a crash that kept the log
/// and cut the end of the value log, the log ends before the first record whose value is not in
/// the file, the commits that were acknowledged are all there, and the database takes commits.
#[wasm_bindgen_test]
async fn a_value_log_that_ends_early_ends_the_log_before_the_first_value_that_is_missing() {
	let name = unique_name("skv-engine-value-log-tail", "db");
	let fs = FileSystem::opfs(&name).await.expect("mount");
	let tree = open(&fs, true).await;
	let acked_end = PER_TXN * 2;
	commit(&tree, 0..acked_end, true, Durability::Immediate).await;
	let value_log = value_log_file(&fs);
	let acked_len = fs.file_len_for_test(&value_log).unwrap();
	let eventual_end = acked_end + PER_TXN * 3;
	commit(&tree, acked_end..eventual_end, true, Durability::Eventual).await;
	assert!(fs.file_len_for_test(&value_log).unwrap() > acked_len + 1000);
	tree.abandon();
	fs.abandon();
	let fresh = FileSystem::opfs(&name).await.expect("mount after a crash");
	drop(tree);
	drop(fs);
	let fs = fresh;
	// The value log ends in the middle of the first entry that was not acknowledged.
	fs.raw_truncate_for_test(&value_log, acked_len + 7).expect("cut the value log");

	let tree = open(&fs, true).await;
	let report = tree.recovery_report();
	assert!(report.records_dropped > 0, "the log was cut: {report:?}");
	assert!(report.bytes_trimmed > 0 && !report.quarantined_files.is_empty(), "{report:?}");
	read_back(&tree, 0..acked_end, true).await;
	let kept = present(&tree, acked_end..eventual_end, true);
	assert!(kept.iter().all(|k| !*k), "nothing that points past the end is there: {kept:?}");

	commit(&tree, acked_end..eventual_end, true, Durability::Immediate).await;
	read_back(&tree, 0..eventual_end, true).await;
	tree.close().await.expect("close the tree");
	drop(tree);
	fs.close().await.expect("close the file system");
	drop(fs);
	FileSystem::destroy_opfs(&name).await.expect("clean up");
}

// ---------------------------------------------------------------------------------------------
// The edges of the public interface
// ---------------------------------------------------------------------------------------------

async fn make_dir(parent: &FileSystemDirectoryHandle, name: &str) -> FileSystemDirectoryHandle {
	let options = FileSystemGetDirectoryOptions::new();
	options.set_create(true);
	JsFuture::from(parent.get_directory_handle_with_options(name, &options))
		.await
		.unwrap_or_else(|e| panic!("make the directory {name}: {e:?}"))
		.unchecked_into()
}

async fn make_file(parent: &FileSystemDirectoryHandle, name: &str) {
	let options = FileSystemGetFileOptions::new();
	options.set_create(true);
	JsFuture::from(parent.get_file_handle_with_options(name, &options))
		.await
		.unwrap_or_else(|e| panic!("make the file {name}: {e:?}"));
}

async fn has_entry(parent: &FileSystemDirectoryHandle, name: &str) -> bool {
	JsFuture::from(parent.get_file_handle(name)).await.is_ok()
		|| JsFuture::from(parent.get_directory_handle(name)).await.is_ok()
}

fn io_kind(error: &Error) -> Option<ErrorKind> {
	match error {
		Error::Io(e) => Some(e.kind()),
		_ => None,
	}
}

/// The default wait for a database that another page has open is long enough for a page that is
/// being reloaded, and ends with the typed error. The second mount is made from this very worker,
/// which holds the lock exactly as another worker would.
/// A page that goes away cannot run an asynchronous close, so most sessions in a browser end
/// without one. Each of them opens the database and leaves it: the log must not keep a segment for
/// each, because every segment is a file of a pool that holds a fixed number of them.
#[wasm_bindgen_test]
async fn sessions_that_end_without_a_close_do_not_leave_the_log_to_grow() {
	let name = unique_name("skv-engine-sessions", "db");
	let mut fs = FileSystem::opfs(&name).await.expect("mount");
	let mut written = 0usize;
	let mut most = 0usize;
	for session in 0..30 {
		let tree = open(&fs, false).await;
		if written > 0 {
			read_back(&tree, 0..written, false).await;
		}
		// Some sessions write and some only look.
		if session % 3 != 0 {
			commit(&tree, written..written + 3, false, Durability::Immediate).await;
			written += 3;
		}
		// Every file that is not a table: the tables are merged as the database grows, and a
		// session that writes leaves its records to a table at the next open.
		most = most
			.max(["db/wal", "db/vlog", "db/manifest"].iter().map(|d| names(&fs, d).len()).sum());
		tree.abandon();
		fs.abandon();
		let fresh = FileSystem::opfs(&name).await.expect("mount after the end of a session");
		drop(tree);
		drop(std::mem::replace(&mut fs, fresh));
	}
	assert!(most <= 6, "{most} files that are not tables after 30 sessions without a close");
	fs.close().await.expect("close");
	FileSystem::destroy_opfs(&name).await.expect("clean up");
}

#[wasm_bindgen_test]
async fn a_second_mount_waits_and_then_fails_with_the_lock_error() {
	let name = unique_name("skv-engine-lock", "db");
	let fs = FileSystem::opfs(&name).await.expect("mount");
	let started = js_sys::Date::now();
	let error = FileSystem::opfs(&name).await.expect_err("the database is open");
	let waited = js_sys::Date::now() - started;
	assert!(error.is_database_locked(), "{error}");
	assert_eq!(io_kind(&error), Some(ErrorKind::PermissionDenied));
	assert!(waited >= 4_500.0, "it waited for a page that is being reloaded: {waited} ms");
	assert!(waited < 15_000.0, "and not much longer: {waited} ms");
	// Nothing else is the lock error.
	assert!(!Error::Other("x".into()).is_database_locked());
	// A wait that is over before it starts is one try.
	let started = js_sys::Date::now();
	let error = FileSystem::opfs_with_lock_wait(&name, Duration::ZERO).await.err().unwrap();
	assert!(error.is_database_locked() && js_sys::Date::now() - started < 1_000.0);
	fs.close().await.unwrap();
	drop(fs);
	FileSystem::destroy_opfs(&name).await.expect("clean up");
}

#[wasm_bindgen_test]
async fn a_directory_that_is_not_a_database_is_refused_and_left_alone() {
	let name = unique_name("skv-engine-foreign", "db");
	let root = root().await;
	let dir = make_dir(&root, &name).await;
	make_file(&dir, "notes.txt").await;

	let error = FileSystem::opfs(&name).await.expect_err("somebody else's directory");
	assert_eq!(io_kind(&error), Some(ErrorKind::InvalidInput), "{error}");
	assert!(error.to_string().contains("notes.txt"), "{error}");
	assert!(!error.is_database_locked());
	assert!(!has_entry(&dir, "LOCK").await, "no lock was put among the entries of somebody else");
	let error = FileSystem::destroy_opfs(&name).await.expect_err("it is not a database");
	assert_eq!(io_kind(&error), Some(ErrorKind::InvalidInput), "{error}");
	assert!(has_entry(&dir, "notes.txt").await, "what is not the database's is not removed");

	// Once there is a database in it, the other entries stay, and the removal still leaves them.
	JsFuture::from(dir.remove_entry("notes.txt")).await.unwrap();
	let fs = FileSystem::opfs(&name).await.expect("an empty directory is a database to be");
	make_file(&dir, "notes.txt").await;
	fs.close().await.unwrap();
	drop(fs);
	let error = FileSystem::destroy_opfs(&name).await.expect_err("a foreign entry is there");
	assert_eq!(io_kind(&error), Some(ErrorKind::InvalidInput), "{error}");
	assert!(has_entry(&dir, "notes.txt").await && has_entry(&dir, "LOCK").await);
	JsFuture::from(dir.remove_entry("notes.txt")).await.unwrap();
	FileSystem::destroy_opfs(&name).await.expect("now it is only a database");
	assert!(!has_entry(&root, &name).await);

	// A name that is a file is not a directory.
	make_file(&root, &name).await;
	let error = FileSystem::opfs(&name).await.expect_err("a file");
	assert_eq!(io_kind(&error), Some(ErrorKind::InvalidInput), "{error}");
	JsFuture::from(root.remove_entry(&name)).await.unwrap();

	// Names that are not one path component are refused before anything is made.
	for bad in ["", ".", "..", "a/b", "a\\b"] {
		let error = FileSystem::opfs(bad).await.expect_err("a bad name");
		assert_eq!(io_kind(&error), Some(ErrorKind::InvalidInput), "{bad:?}: {error}");
	}
}

/// A lock that is a directory is damage, not somebody else's entry: the database cannot be mounted
/// (the error says why, and at once), and it can be removed.
#[wasm_bindgen_test]
async fn a_database_whose_lock_is_a_directory_cannot_be_mounted_and_can_be_removed() {
	let name = unique_name("skv-engine-lockdir", "db");
	let fs = FileSystem::opfs(&name).await.expect("mount");
	fs.close().await.unwrap();
	drop(fs);
	let dir = make_dir(&root().await, &name).await;
	JsFuture::from(dir.remove_entry("LOCK")).await.unwrap();
	make_dir(&dir, "LOCK").await;

	let started = js_sys::Date::now();
	let error = FileSystem::opfs(&name).await.expect_err("no lock can be taken");
	assert_eq!(io_kind(&error), Some(ErrorKind::InvalidInput), "{error}");
	assert!(!error.is_database_locked());
	assert!(js_sys::Date::now() - started < 2_000.0, "it fails and does not wait");
	FileSystem::destroy_opfs(&name).await.expect("it can be removed");
	let fs = FileSystem::opfs(&name).await.expect("and made again");
	fs.close().await.unwrap();
	drop(fs);
	FileSystem::destroy_opfs(&name).await.expect("clean up");
}

/// Without synchronous access handles (a page, a shared worker, a service worker) the mount fails
/// with `Unsupported` before it makes anything. The handles are taken away from the worker here for
/// the length of the call, which is what the check looks for: it cannot be tested on the main
/// thread in a dedicated-worker binary.
#[wasm_bindgen_test]
async fn without_synchronous_access_handles_the_mount_fails_before_it_makes_anything() {
	let name = unique_name("skv-engine-nohandles", "db");
	let global = js_sys::global();
	let constructor = js_sys::Reflect::get(&global, &"FileSystemFileHandle".into()).unwrap();
	let prototype = js_sys::Reflect::get(&constructor, &"prototype".into()).unwrap();
	let method = js_sys::Reflect::get(&prototype, &"createSyncAccessHandle".into()).unwrap();
	assert!(method.is_function(), "a dedicated worker has the method");
	let object: &js_sys::Object = prototype.unchecked_ref();
	assert!(js_sys::Reflect::delete_property(object, &"createSyncAccessHandle".into()).unwrap());
	let error = FileSystem::opfs(&name).await.err();
	let destroyed = FileSystem::destroy_opfs(&name).await;
	js_sys::Reflect::set(&prototype, &"createSyncAccessHandle".into(), &method).unwrap();
	let error = error.expect("the mount fails");
	assert_eq!(io_kind(&error), Some(ErrorKind::Unsupported), "{error}");
	assert!(error.to_string().contains("dedicated worker"), "{error}");
	assert!(destroyed.is_ok(), "there is nothing to remove, and removing needs no handle");
	assert!(!has_entry(&root().await, &name).await, "nothing was made");
	// And the method is back: the worker mounts again.
	let fs = FileSystem::opfs(&name).await.expect("mount");
	fs.close().await.unwrap();
	drop(fs);
	FileSystem::destroy_opfs(&name).await.expect("clean up");
}

/// A tree cannot be built over a file system that was closed, and a close is not the end of the
/// database: the next mount has everything.
#[wasm_bindgen_test]
async fn a_closed_file_system_refuses_a_tree_and_the_database_is_whole_at_the_next_mount() {
	let name = unique_name("skv-engine-closed", "db");
	let fs = FileSystem::opfs(&name).await.expect("mount");
	let tree = open(&fs, false).await;
	commit(&tree, 0..PER_TXN, false, Durability::Eventual).await;
	tree.close().await.unwrap();
	drop(tree);
	fs.close().await.unwrap();
	let refused = match builder(&fs, false).build_async().await {
		Ok(_) => panic!("a tree cannot be built over a closed file system"),
		Err(error) => error,
	};
	assert_eq!(io_kind(&refused), Some(ErrorKind::NotConnected), "{refused}");
	drop(fs);
	let fs = FileSystem::opfs_with_lock_wait(&name, Duration::ZERO).await.expect("at once");
	let tree = open(&fs, false).await;
	read_back(&tree, 0..PER_TXN, false).await;
	tree.close().await.unwrap();
	drop(tree);
	fs.close().await.unwrap();
	drop(fs);
	FileSystem::destroy_opfs(&name).await.expect("clean up");
}

#[wasm_bindgen_test]
async fn whether_the_storage_is_persistent_is_answered_in_a_worker() {
	let persisted = FileSystem::persisted().await.expect("a worker can ask");
	console_log!("the storage of this origin is persistent: {persisted}");
}

/// A browser worker starts from sizes that suit it; every one of them can be set.
#[wasm_bindgen_test]
fn the_defaults_of_a_browser_are_smaller() {
	let options = Options::default();
	assert_eq!(options.max_memtable_size, 16 * 1024 * 1024);
	assert_eq!(options.target_file_size, 16 * 1024 * 1024);
	options.validate().expect("and they are valid");
	let options =
		Options::default().with_max_memtable_size(64 << 20).with_target_file_size(8 << 20);
	assert_eq!((options.max_memtable_size, options.target_file_size), (64 << 20, 8 << 20));
	assert_eq!(Durability::default(), Durability::Eventual, "commits do not wait for a flush");
}
