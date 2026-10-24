//! Sessions that end without a close, over a database in each kind of file system.
//!
//! A session opens the database, commits a few keys (three `Immediate` and three `Eventual`, with
//! and without the value log), and ends without a close, as the tab of a browser does. The next
//! session follows it. After every session the tables of the tree, the files of the database, the
//! segments of the write-ahead log and the files of the value log are counted. After every session
//! the tree holds no table and the log holds one segment, and the files of the database and of the
//! value log stay within a small constant over the whole run, as the constants below say and why.
//! Every key that was committed `Immediate` is there at the end.
//!
//! A session may also end on a partial record, on the sync mark of its last commit without the
//! record, or with its last value missing from the value log. The next open moves the newest
//! segment to the number that follows, so these endings leave the same counts as a session that
//! ends at a record boundary. The tests of each ending are below.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use test_log::test;

use super::fs_engine_tests::{memory_with_pool, strict_fs, tables_per_level};
use super::tfs::{STRICT_INITIAL, STRICT_MAX};
use crate::vfs::{MemRaw, PoolFs};
use crate::{Durability, FileSystem, Options, Tree, TreeBuilder};

/// Sessions per run.
const SESSIONS: usize = 300;

/// The longest a run may take. It allows for a file system that syncs each commit, on a machine
/// that runs other suites as well.
const WATCHDOG: Duration = Duration::from_secs(900);

/// Runs `scenario` under [`WATCHDOG`].
async fn within<F: std::future::Future>(what: &str, scenario: F) -> F::Output {
	match tokio::time::timeout(WATCHDOG, scenario).await {
		Ok(output) => output,
		Err(_) => panic!("{what}: no answer in {WATCHDOG:?}"),
	}
}

/// Commits per session: the even ones are `Immediate`, the odd ones `Eventual`.
const COMMITS: usize = 6;

/// Tables of the tree after each session, once its commits are in: none. The open keeps the records
/// of the log in the memtable that it makes active, and the commits go on in the segment they are
/// in, so no session that ends without a close leaves a table behind it.
const TABLES: usize = 0;

/// Segments of the write-ahead log after each session: the one that the commits go to, and no
/// other. The open moves the newest segment to the number that follows it, and does not start a
/// segment beside it.
const WAL_SEGMENTS: usize = 1;

/// Files of the value log: the one that the values go to, and one that a roll has left.
const VLOG_BOUND: usize = 2;

/// Files of the database: the tables, the manifest, the lock, the segments of the log and of the
/// value log. The four tables, two segments and two files of the value log above, and the rest.
const FILE_BOUND: usize = 16;

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
	/// The newest segment of the log is cut to the end of the sync mark of its last commit, so
	/// that the record of that commit is not in it: the state of a session that ended while
	/// the commit was being written. The commit is `Eventual`, so no commit that was
	/// acknowledged `Immediate` is cut.
	MarkWithoutRecord,
}

/// What the database holds after one session.
#[derive(Debug, Clone, Copy)]
struct Census {
	session: usize,
	tables: usize,
	files: usize,
	wal: usize,
	vlog: usize,
	/// What the open of the session repaired: bytes trimmed, records dropped and files
	/// quarantined.
	repaired: u64,
}

/// Larger than the commits of a whole run, so that no memtable is flushed for being full: what the
/// census counts is what the opens leave behind.
const MEMTABLE: usize = 1024 * 1024;

fn options(fs: &FileSystem, path: &Path, vlog: bool) -> Options {
	let mut options = Options {
		path: path.to_path_buf(),
		max_memtable_size: MEMTABLE,
		level0_max_files: 4,
		..Options::default()
	}
	.with_file_system(fs.clone());
	options.enable_vlog = vlog;
	options.vlog_value_threshold = 256;
	options
}

fn key(session: usize, j: usize) -> Vec<u8> {
	format!("session-{session:04}-{j}").into_bytes()
}

/// A value that differs for every key and is above the threshold of the value log when there is
/// one.
fn value(session: usize, j: usize, vlog: bool) -> Vec<u8> {
	let mut value = format!("value-{session:04}-{j}:").into_bytes();
	value.resize(
		if vlog {
			600
		} else {
			64
		},
		b'v',
	);
	value
}

fn durability(j: usize) -> Durability {
	if j % 2 == 0 {
		Durability::Immediate
	} else {
		Durability::Eventual
	}
}

/// The files under `dir`, counted on the disk.
fn files_on_disk(dir: &Path) -> usize {
	let Ok(entries) = std::fs::read_dir(dir) else {
		return 0;
	};
	entries
		.flatten()
		.map(|entry| {
			let path = entry.path();
			if path.is_dir() {
				files_on_disk(&path)
			} else {
				1
			}
		})
		.sum()
}

/// The files of the database: the pool's own count of the names it holds, or the disk's.
fn database_files(pool: Option<&PoolFs<MemRaw>>, path: &Path) -> usize {
	match pool {
		Some(pool) => pool.stats().files,
		None => files_on_disk(path),
	}
}

/// The files of a directory of the file system `fs`, `0` for one that does not exist.
fn files_in(fs: &FileSystem, dir: &Path) -> usize {
	fs.0.read_dir(dir)
		.map(|entries| entries.iter().filter(|e| e.kind == crate::vfs::EntryKind::File).count())
		.unwrap_or(0)
}

fn census(
	tree: &Tree,
	fs: &FileSystem,
	options: &Options,
	pool: Option<&PoolFs<MemRaw>>,
	session: usize,
) -> Census {
	let wal = crate::wal::list_segment_ids_in(&*fs.0, &options.wal_dir(), Some("wal"))
		.map(|ids| ids.len())
		.unwrap_or(0);
	Census {
		session,
		tables: tables_per_level(tree).iter().sum(),
		files: database_files(pool, &options.path),
		wal,
		vlog: if options.enable_vlog {
			files_in(fs, &options.vlog_dir())
		} else {
			0
		},
		repaired: 0,
	}
}

/// The path of the newest file of `dir` with the extension `ext`, if there is one.
fn newest_file(fs: &FileSystem, dir: &Path, ext: &str) -> Option<PathBuf> {
	let entries = fs.0.read_dir(dir).ok()?;
	entries
		.into_iter()
		.filter(|e| e.kind == crate::vfs::EntryKind::File)
		.filter(|e| e.name.to_string_lossy().ends_with(ext))
		.max_by(|a, b| a.name.cmp(&b.name))
		.map(|e| dir.join(e.name))
}

/// Ends a session that was abandoned as `ending` says, after its last commit.
fn end_session(fs: &FileSystem, options: &Options, ending: Ending) {
	match ending {
		Ending::Clean => {}
		Ending::TornTail => {
			let path = newest_file(fs, &options.wal_dir(), ".wal").expect("a segment of the log");
			let file = fs.0.open_append_existing(&path).expect("the segment opens");
			file.append(&[0xAB, 0xCD, 0xEF]).expect("the partial record is appended");
			file.sync().expect("the partial record is synced");
		}
		Ending::ValueLogCut => {
			let path =
				newest_file(fs, &options.vlog_dir(), ".vlog").expect("a file of the value log");
			let file = fs.0.open_append_existing(&path).expect("the value-log file opens");
			let len = file.len().expect("its length");
			file.set_len(len - 1).expect("the last byte is cut");
			file.sync().expect("the cut is synced");
		}
		Ending::MarkWithoutRecord => {
			let path = newest_file(fs, &options.wal_dir(), ".wal").expect("a segment of the log");
			let read = fs.0.open_read(&path).expect("the segment opens for reading");
			let len = usize::try_from(read.len().expect("its length")).expect("a small segment");
			let mut data = vec![0u8; len];
			let got = read.read_at(0, &mut data).expect("the segment reads");
			assert_eq!(got, len, "the segment is read whole");
			// The last sync mark of the segment is the one before the record of the last commit,
			// which the session wrote `Eventual` and so never synced.
			let mark = (0..len)
				.rev()
				.find(|pos| crate::wal::recovery::sync_mark_in(&data, 0, *pos).is_some())
				.expect("the segment holds a sync mark");
			let end = mark + crate::wal::HEADER_SIZE + crate::wal::SYNC_MARK_LEN;
			assert!(end < len, "a record follows the last sync mark");
			let file = fs.0.open_append_existing(&path).expect("the segment opens");
			file.set_len(end as u64).expect("the record is cut");
			file.sync().expect("the cut is synced");
		}
	}
}

/// Runs `SESSIONS` sessions over the database at `path` in `fs`, and checks the counts after each
/// one and the keys at the end.
async fn sessions_without_a_close(
	fs: FileSystem,
	pool: Option<Arc<PoolFs<MemRaw>>>,
	path: PathBuf,
	vlog: bool,
	ending: Ending,
) {
	let options = options(&fs, &path, vlog);
	let mut censuses = Vec::with_capacity(SESSIONS);
	for session in 0..SESSIONS {
		let tree = TreeBuilder::with_options(options.clone())
			.build_async()
			.await
			.unwrap_or_else(|e| panic!("session {session} does not open: {e}"));
		let report = tree.recovery_report();
		let repaired = report.bytes_trimmed
			+ report.records_dropped as u64
			+ report.quarantined_files.len() as u64;
		for j in 0..COMMITS {
			let mut txn = tree.begin().expect("a transaction begins");
			txn.set_durability(durability(j));
			txn.set(key(session, j), value(session, j, vlog)).expect("a write is buffered");
			txn.commit().await.unwrap_or_else(|e| panic!("session {session} commit {j}: {e}"));
		}
		let mut census = census(&tree, &fs, &options, pool.as_deref(), session);
		census.repaired = repaired;
		censuses.push(census);
		// The session ends without a close, as a tab does.
		tree.abandon();
		drop(tree);
		end_session(&fs, &options, ending);
	}

	eprintln!("SKV-SESSIONS vlog={vlog} ending={ending:?} censuses (session tables files wal vlog repaired):");
	for c in censuses.iter().filter(|c| {
		c.session < 4 || c.session % 50 == 49 || c.session == SESSIONS - 1 || c.repaired > 0
	}) {
		eprintln!(
			"SKV-SESSIONS {} {} {} {} {} {}",
			c.session, c.tables, c.files, c.wal, c.vlog, c.repaired
		);
	}

	let worst = |f: fn(&Census) -> usize| censuses.iter().map(f).max().unwrap_or(0);
	if let Some(c) = censuses.iter().find(|c| c.tables != TABLES) {
		panic!(
			"session {} leaves {} tables after its commits, expected {TABLES}",
			c.session, c.tables
		);
	}
	if let Some(c) = censuses.iter().find(|c| c.wal != WAL_SEGMENTS) {
		panic!(
			"session {} leaves {} WAL segments after its commits, expected {WAL_SEGMENTS}",
			c.session, c.wal
		);
	}
	assert!(
		worst(|c| c.files) <= FILE_BOUND,
		"files grew with the sessions: {} files at the last session, at most {FILE_BOUND} allowed",
		censuses[SESSIONS - 1].files
	);
	assert!(
		worst(|c| c.vlog) <= VLOG_BOUND,
		"value-log files grew with the sessions: {} at the last session, at most {VLOG_BOUND} allowed",
		censuses[SESSIONS - 1].vlog
	);

	// The keys that were committed `Immediate` are all there, in a session that opens the database
	// after the last abandoned one.
	let tree = TreeBuilder::with_options(options).build_async().await.expect("the database opens");
	let txn = tree.begin_with_mode(crate::Mode::ReadOnly).expect("a snapshot begins");
	for session in 0..SESSIONS {
		for j in (0..COMMITS).filter(|j| durability(*j) == Durability::Immediate) {
			let got = txn.get(key(session, j)).expect("a read answers");
			assert_eq!(got, Some(value(session, j, vlog)), "session {session} commit {j}");
		}
	}
	drop(txn);
	tree.close().await.expect("the database closes");
}

#[test(tokio::test)]
async fn sessions_without_a_close_stay_bounded_on_std() {
	within("std sessions", async {
		let dir = TempDir::new().unwrap();
		sessions_without_a_close(
			FileSystem::std(),
			None,
			dir.path().join("db"),
			false,
			Ending::Clean,
		)
		.await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_without_a_close_stay_bounded_on_std_with_the_value_log() {
	within("std sessions with the value log", async {
		let dir = TempDir::new().unwrap();
		sessions_without_a_close(
			FileSystem::std(),
			None,
			dir.path().join("db"),
			true,
			Ending::Clean,
		)
		.await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_ending_on_a_partial_record_stay_bounded_on_std() {
	within("std sessions ending on a partial record", async {
		let dir = TempDir::new().unwrap();
		sessions_without_a_close(
			FileSystem::std(),
			None,
			dir.path().join("db"),
			false,
			Ending::TornTail,
		)
		.await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_ending_on_a_partial_record_stay_bounded_on_std_with_the_value_log() {
	within("std sessions ending on a partial record, with the value log", async {
		let dir = TempDir::new().unwrap();
		sessions_without_a_close(
			FileSystem::std(),
			None,
			dir.path().join("db"),
			true,
			Ending::TornTail,
		)
		.await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_ending_with_a_cut_value_log_stay_bounded_on_std() {
	within("std sessions ending with a cut value log", async {
		let dir = TempDir::new().unwrap();
		sessions_without_a_close(
			FileSystem::std(),
			None,
			dir.path().join("db"),
			true,
			Ending::ValueLogCut,
		)
		.await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_without_a_close_stay_bounded_in_a_memory_pool() {
	within("memory pool sessions", async {
		let (fs, pool) = memory_with_pool();
		sessions_without_a_close(fs, Some(pool), "db".into(), false, Ending::Clean).await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_without_a_close_stay_bounded_in_a_memory_pool_with_the_value_log() {
	within("memory pool sessions with the value log", async {
		let (fs, pool) = memory_with_pool();
		sessions_without_a_close(fs, Some(pool), "db".into(), true, Ending::Clean).await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_ending_on_a_partial_record_stay_bounded_in_a_memory_pool() {
	within("memory pool sessions ending on a partial record", async {
		let (fs, pool) = memory_with_pool();
		sessions_without_a_close(fs, Some(pool), "db".into(), false, Ending::TornTail).await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_ending_with_a_cut_value_log_stay_bounded_in_a_memory_pool() {
	within("memory pool sessions ending with a cut value log", async {
		let (fs, pool) = memory_with_pool();
		sessions_without_a_close(fs, Some(pool), "db".into(), true, Ending::ValueLogCut).await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_without_a_close_stay_bounded_in_a_strict_pool() {
	within("strict pool sessions", async {
		let (fs, pool) = strict_fs(STRICT_INITIAL, STRICT_MAX);
		sessions_without_a_close(fs, Some(pool), "db".into(), false, Ending::Clean).await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_without_a_close_stay_bounded_in_a_strict_pool_with_the_value_log() {
	within("strict pool sessions with the value log", async {
		let (fs, pool) = strict_fs(STRICT_INITIAL, STRICT_MAX);
		sessions_without_a_close(fs, Some(pool), "db".into(), true, Ending::Clean).await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_ending_on_a_partial_record_stay_bounded_in_a_strict_pool() {
	within("strict pool sessions ending on a partial record", async {
		let (fs, pool) = strict_fs(STRICT_INITIAL, STRICT_MAX);
		sessions_without_a_close(fs, Some(pool), "db".into(), false, Ending::TornTail).await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_ending_with_a_cut_value_log_stay_bounded_in_a_strict_pool() {
	within("strict pool sessions ending with a cut value log", async {
		let (fs, pool) = strict_fs(STRICT_INITIAL, STRICT_MAX);
		sessions_without_a_close(fs, Some(pool), "db".into(), true, Ending::ValueLogCut).await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_ending_on_a_sync_mark_stay_bounded_on_std() {
	within("std sessions ending on a sync mark", async {
		let dir = TempDir::new().unwrap();
		sessions_without_a_close(
			FileSystem::std(),
			None,
			dir.path().join("db"),
			false,
			Ending::MarkWithoutRecord,
		)
		.await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_ending_on_a_sync_mark_stay_bounded_on_std_with_the_value_log() {
	within("std sessions ending on a sync mark, with the value log", async {
		let dir = TempDir::new().unwrap();
		sessions_without_a_close(
			FileSystem::std(),
			None,
			dir.path().join("db"),
			true,
			Ending::MarkWithoutRecord,
		)
		.await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_ending_on_a_sync_mark_stay_bounded_in_a_memory_pool() {
	within("memory pool sessions ending on a sync mark", async {
		let (fs, pool) = memory_with_pool();
		sessions_without_a_close(fs, Some(pool), "db".into(), false, Ending::MarkWithoutRecord)
			.await;
	})
	.await;
}

#[test(tokio::test)]
async fn sessions_ending_on_a_sync_mark_stay_bounded_in_a_strict_pool() {
	within("strict pool sessions ending on a sync mark", async {
		let (fs, pool) = strict_fs(STRICT_INITIAL, STRICT_MAX);
		sessions_without_a_close(fs, Some(pool), "db".into(), true, Ending::MarkWithoutRecord)
			.await;
	})
	.await;
}
