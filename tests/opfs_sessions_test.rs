//! Sessions that end without a close, over a database in the origin private file system of a real
//! headless Chrome, in a dedicated worker. Run it, from the repository root, with
//!
//! ```text
//! export CC_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/clang
//! export AR_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/llvm-ar
//! WASM_BINDGEN_USE_DEDICATED_WORKER=1 WASM_BINDGEN_TEST_TIMEOUT=600 \
//!     wasm-pack test --headless --chrome -- --test opfs_sessions_test
//! ```
//!
//! A session mounts the database, opens it, commits (`Immediate` and `Eventual`), and ends without
//! a close: the tab of a browser does that when it is closed. The next session mounts the same pool
//! and does the same. A session may also end on a partial record of the log, or with its last value
//! cut from the value log. Each run is `SESSIONS` sessions. After every mount the files of the
//! pool, the tables, the segments of the log and the files of the value log are counted. None of
//! them grows with the sessions: each stays within the constant that says it. At the end every key
//! that was committed `Immediate` is there.

#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

mod opfs_common;

use std::time::Duration;

use opfs_common::{unique_name, watchdog};
use surrealkv::{Durability, FileSystem, Options, Tree, TreeBuilder};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// Sessions per run. More than a hundred, so that the bound is tested over many ends of a session.
const SESSIONS: usize = 120;

/// Keys in each `Immediate` transaction, and in each `Eventual` one.
const PER_TXN: usize = 10;

/// Larger than the commits of a whole run, so that no memtable is flushed for being full: what the
/// counts see is what the opens leave behind.
const MEMTABLE: usize = 1024 * 1024;

/// Tables at any mount: the level-0 file limit of these options is 4.
const TABLE_BOUND: usize = 4;

/// Segments of the write-ahead log at any mount.
const WAL_BOUND: usize = 2;

/// Files of the value log at any mount.
const VLOG_BOUND: usize = 2;

/// Files of the pool at any mount: the tables, the manifest, the segments of the log and of the
/// value log, and the rest.
const FILE_BOUND: usize = 16;

fn key(i: usize) -> Vec<u8> {
	format!("session-key-{i:06}").into_bytes()
}

/// A value that differs for every key, above the threshold of the value log when there is one.
fn value(i: usize, vlog: bool) -> Vec<u8> {
	let size = if vlog {
		600
	} else {
		100
	};
	let mut value = format!("session-value-{i:06}:").into_bytes();
	value.resize(size, b'v');
	value
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

/// Mounts the pool `name`, waiting for the lock of a session that ended a moment ago.
async fn mount(name: &str) -> FileSystem {
	FileSystem::opfs_with_lock_wait(name, Duration::from_secs(5))
		.await
		.unwrap_or_else(|e| panic!("mount of {name} failed: {e}"))
}

/// The names in the directory `dir` of `fs`.
fn names(fs: &FileSystem, dir: &str) -> Vec<String> {
	fs.list_for_test(dir)
}

/// The counts of one mount: the files of the pool, the tables, the segments of the log and the
/// files of the value log.
#[derive(Debug, Clone, Copy)]
struct Census {
	files: usize,
	tables: usize,
	wal: usize,
	vlog: usize,
}

fn census(fs: &FileSystem) -> Census {
	let files = fs.mount_report().expect("a pool has a mount report").files;
	Census {
		files,
		tables: names(fs, "db/sstables").len(),
		wal: names(fs, "db/wal").into_iter().filter(|name| name.ends_with(".wal")).count(),
		vlog: names(fs, "db/vlog").len(),
	}
}

/// Commits `keys` keys from `from`, in `durability`, as one transaction.
async fn commit(tree: &Tree, from: usize, keys: usize, durability: Durability, vlog: bool) {
	let mut txn = tree.begin().expect("begin");
	txn.set_durability(durability);
	for i in from..from + keys {
		txn.set(key(i), value(i, vlog)).expect("set");
	}
	txn.commit().await.unwrap_or_else(|e| panic!("commit of the keys from {from}: {e}"));
}

/// How a session ends, besides being abandoned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
	/// At a record boundary, with every value in the value log.
	Clean,
	/// Three bytes of a record are appended to the newest segment of the log after the last
	/// commit.
	TornTail,
	/// The last byte of the newest value-log file is cut, so that the last value of the log is not
	/// in the value log.
	ValueLogCut,
}

/// The newest file of `dir` whose name ends with `ext`, as a path of the pool.
fn newest(fs: &FileSystem, dir: &str, ext: &str) -> String {
	names(fs, dir)
		.into_iter()
		.filter(|name| name.ends_with(ext))
		.max()
		.map(|name| format!("{dir}/{name}"))
		.expect("a file of the database")
}

/// Ends a session that was abandoned, as `ending` says, while its pool is still mounted.
fn end_session(fs: &FileSystem, ending: Ending) {
	match ending {
		Ending::Clean => {}
		Ending::TornTail => {
			let path = newest(fs, "db/wal", ".wal");
			fs.append_for_test(&path, &[0xAB, 0xCD, 0xEF]).expect("the partial record is appended");
		}
		Ending::ValueLogCut => {
			let path = newest(fs, "db/vlog", ".vlog");
			let len = fs.file_len_for_test(&path).expect("the value-log file is there");
			fs.raw_truncate_for_test(&path, len - 1).expect("the last byte is cut");
		}
	}
}

async fn run_sessions(name: &str, vlog: bool, ending: Ending) {
	let mut censuses = Vec::with_capacity(SESSIONS);
	// The keys are numbered across the sessions: `Immediate` ones are acknowledged, and the
	// `Eventual` ones are kept when the session ends, which is what a crash is allowed to lose.
	let mut next = 0usize;
	let mut acknowledged = Vec::new();
	for _ in 0..SESSIONS {
		let fs = mount(name).await;
		censuses.push(census(&fs));
		let tree = open(&fs, vlog).await;
		commit(&tree, next, PER_TXN, Durability::Immediate, vlog).await;
		acknowledged.extend(next..next + PER_TXN);
		next += PER_TXN;
		commit(&tree, next, PER_TXN, Durability::Eventual, vlog).await;
		next += PER_TXN;
		// The session ends without a close, and the pool is released as a closed tab releases it.
		tree.abandon();
		end_session(&fs, ending);
		fs.abandon();
		drop(tree);
		drop(fs);
	}

	let worst = |f: fn(&Census) -> usize| censuses.iter().map(f).max().unwrap_or(0);
	let last = censuses[SESSIONS - 1];
	console_log!("{SESSIONS} sessions, vlog={vlog}: last mount {last:?}");
	assert!(worst(|c| c.tables) <= TABLE_BOUND, "tables grew: {last:?}");
	assert!(worst(|c| c.wal) <= WAL_BOUND, "WAL segments grew: {last:?}");
	assert!(worst(|c| c.vlog) <= VLOG_BOUND, "value-log files grew: {last:?}");
	assert!(worst(|c| c.files) <= FILE_BOUND, "pool files grew: {last:?}");

	// Every `Immediate` key is there, after one more mount.
	let fs = mount(name).await;
	let tree = open(&fs, vlog).await;
	let txn = tree.begin().expect("begin a reader");
	for i in &acknowledged {
		let got = txn.get(key(*i)).expect("a read answers");
		assert_eq!(got.as_deref(), Some(&value(*i, vlog)[..]), "acknowledged key {i}");
	}
	drop(txn);
	tree.close().await.expect("close the tree");
	drop(tree);
	fs.close().await.expect("close the file system");
	drop(fs);
	FileSystem::destroy_opfs(name).await.expect("remove the database");
}

#[wasm_bindgen_test]
async fn sessions_that_end_without_a_close_stay_bounded_without_the_value_log() {
	run_sessions(&unique_name("skv-sessions", "db"), false, Ending::Clean).await;
}

#[wasm_bindgen_test]
async fn sessions_that_end_without_a_close_stay_bounded_with_the_value_log() {
	run_sessions(&unique_name("skv-sessions-vlog", "db"), true, Ending::Clean).await;
}

#[wasm_bindgen_test]
async fn sessions_ending_on_a_partial_record_stay_bounded_without_the_value_log() {
	run_sessions(&unique_name("skv-sessions-torn", "db"), false, Ending::TornTail).await;
}

#[wasm_bindgen_test]
async fn sessions_ending_on_a_partial_record_stay_bounded_with_the_value_log() {
	run_sessions(&unique_name("skv-sessions-torn-vlog", "db"), true, Ending::TornTail).await;
}

#[wasm_bindgen_test]
async fn sessions_ending_with_a_cut_value_log_stay_bounded() {
	run_sessions(&unique_name("skv-sessions-cut", "db"), true, Ending::ValueLogCut).await;
}
