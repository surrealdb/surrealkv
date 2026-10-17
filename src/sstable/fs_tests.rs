//! The tables, the manifest and the open path through the file system seam.
//!
//! Moving a call site from `std::fs` to `Fs` must not change what the engine does on a native
//! file system: the same calls in the same order, so the same syscalls. These tests record the
//! calls (`RecordingFs` is `StdFs` with a log) and pin the order that the code had before it was
//! moved, which is the order that makes a table durable before the manifest names it:
//!
//! * a memtable flush is create, write, sync the file, sync the directory, open for reading;
//! * a compaction output is the same up to its sync, with one directory sync after all outputs, and
//!   it is created as an opportunistic file, which a flush is not;
//! * a manifest is written as one `replace_atomic`, and read with one `read`;
//! * the checks of the open path are `try_exists` and `exists` and a listing, and a file system
//!   that cannot answer is an error and never "nothing there".
//!
//! The native syscalls themselves were compared with an interposer, before and after the move: see
//! the report of the change. What is pinned here is the order of the calls the engine makes.

use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use tempfile::TempDir;

use super::recording_fs::{short, RecordingFs};
use crate::compaction::{CompactionChoice, CompactionInput, CompactionStrategy};
use crate::error::Error;
use crate::levels::{
	check_manifest_before_open_in,
	plan_orphan_cleanup_in,
	validate_wal_log_number_in,
	write_manifest_to_disk_in,
	LevelManifest,
};
use crate::sstable::table::TableWriter;
use crate::vfs::{CreateOpts, FsFile, FsKind, FsWriter, UnavailableFs};
use crate::{Durability, InternalKey, InternalKeyKind, Options, Result, Tree};

fn options(path: &Path, fs: &Arc<RecordingFs>) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		fs: Arc::new(Arc::clone(fs)),
		flush_on_close: false,
		enable_vlog: false,
		level_count: 4,
		target_file_size: 2 * 1024,
		max_memtable_size: 8 * 1024 * 1024,
		level0_max_files: 10_000,
		l0_stall_threshold: 10_000,
		memtable_stall_threshold: 10_000,
		max_bytes_for_level: 1 << 40,
		..Default::default()
	})
}

async fn put(tree: &Tree, from: usize, to: usize) {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(Durability::Eventual);
	for i in from..to {
		txn.set(format!("key_{i:04}").into_bytes(), vec![b'v'; 100]).unwrap();
	}
	txn.commit().await.unwrap();
}

/// Consecutive appends to one file are one step, `append*`: how many there are depends on the size
/// of the table, and the order of the steps does not.
fn collapse(entries: Vec<String>) -> Vec<String> {
	let mut out: Vec<String> = Vec::new();
	for entry in entries {
		let step = match entry.strip_prefix("append ") {
			Some(rest) => format!("append* {}", rest.split(' ').next().unwrap()),
			None => entry,
		};
		if out.last() != Some(&step) {
			out.push(step);
		}
	}
	out
}

/// The calls that name a table file or the table directory.
fn table_calls(fs: &RecordingFs) -> Vec<String> {
	collapse(
		fs.entries().into_iter().filter(|e| e.contains(".sst") || e.contains("sstables")).collect(),
	)
}

struct MergeLevels {
	source: u8,
	target: u8,
}

impl CompactionStrategy for MergeLevels {
	fn pick_levels(&self, manifest: &LevelManifest) -> Result<CompactionChoice> {
		let levels = manifest.levels.get_levels();
		let mut tables_to_merge: Vec<u64> =
			levels[self.source as usize].tables.iter().map(|t| t.id).collect();
		if self.target != self.source {
			tables_to_merge.extend(levels[self.target as usize].tables.iter().map(|t| t.id));
		}
		if tables_to_merge.is_empty() {
			return Ok(CompactionChoice::Skip);
		}
		Ok(CompactionChoice::Merge(CompactionInput {
			tables_to_merge,
			target_level: self.target,
			source_level: self.source,
		}))
	}
}

fn sst(id: u64) -> String {
	format!("sstables/{id:020}.sst")
}

#[tokio::test(flavor = "current_thread")]
async fn a_memtable_flush_calls_the_file_system_in_the_order_it_always_did() {
	let dir = TempDir::new().unwrap();
	let fs = RecordingFs::new();
	let tree = Tree::new(options(&dir.path().join("db"), &fs)).unwrap();
	put(&tree, 0, 60).await;

	fs.take();
	tree.flush().unwrap();

	// Reserved: a flush may use every file of a pool, which `opportunistic` may not.
	let first = sst(1);
	assert_eq!(
		table_calls(&fs),
		vec![
			format!("create_truncate {first} reserved"),
			format!("append* {first}"),
			format!("sync_file {first}"),
			"sync_dir db/sstables".to_string(),
			format!("open_read {first}"),
		]
	);

	// And the table that was opened is the one that was written.
	assert_eq!(tree.begin().unwrap().get(b"key_0007").unwrap().as_deref(), Some(&[b'v'; 100][..]));
	tree.close().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn a_compaction_writes_its_outputs_as_opportunistic_files_and_syncs_the_directory_once() {
	let dir = TempDir::new().unwrap();
	let fs = RecordingFs::new();
	let tree = Tree::new(options(&dir.path().join("db"), &fs)).unwrap();
	put(&tree, 0, 60).await;
	tree.flush().unwrap();
	put(&tree, 30, 90).await;
	tree.flush().unwrap();

	fs.take();
	tree.compact(Arc::new(MergeLevels {
		source: 0,
		target: 1,
	}))
	.unwrap();
	let calls = table_calls(&fs);

	// The 2 KiB target splits the merge into several outputs.
	let outputs: Vec<&String> = calls.iter().filter(|c| c.starts_with("create_truncate")).collect();
	assert!(outputs.len() >= 2, "{calls:#?}");
	assert!(outputs.iter().all(|c| c.ends_with(" opportunistic")), "{calls:#?}");

	// Each output is created, written and synced, one after the other; then the directory is
	// synced, once; then the outputs are opened; then the inputs, the two flushed tables, go.
	let ids: Vec<String> =
		outputs.iter().map(|c| c.split(' ').nth(1).unwrap().to_string()).collect();
	let mut expected = Vec::new();
	for id in &ids {
		expected.push(format!("create_truncate {id} opportunistic"));
		expected.push(format!("append* {id}"));
		expected.push(format!("sync_file {id}"));
	}
	expected.push("sync_dir db/sstables".to_string());
	for id in &ids {
		expected.push(format!("open_read {id}"));
	}
	// The order of the inputs is the order of the level, the newest table first.
	expected.push(format!("remove_file {}", sst(2)));
	expected.push(format!("remove_file {}", sst(1)));
	assert_eq!(calls, expected);
	tree.close().await.unwrap();
}

/// What a writer asks of its file: each `write`, in order.
struct RecordingWriter {
	writes: Arc<Mutex<Vec<usize>>>,
}

impl Write for RecordingWriter {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		self.writes.lock().unwrap().push(buf.len());
		Ok(buf.len())
	}

	fn flush(&mut self) -> io::Result<()> {
		Ok(())
	}
}

fn add_entries<W: Write>(writer: &mut TableWriter<W>) {
	for i in 0..400u64 {
		let key = InternalKey::new(format!("key{i:05}").into_bytes(), i + 1, InternalKeyKind::Set);
		writer.add(key, &vec![b'x'; 37 + (i as usize % 11)]).unwrap();
	}
}

#[test]
fn a_table_written_through_the_file_system_asks_its_file_for_the_same_writes() {
	let dir = TempDir::new().unwrap();
	let fs = RecordingFs::new();
	let opts = options(dir.path(), &fs);

	// What the sink asks of whatever it writes to: the header, and each block and its trailer,
	// as separate writes, and the footer. This is the sequence of `write(2)` calls that the table
	// made when the file was a `std::fs::File`.
	let writes = Arc::new(Mutex::new(Vec::new()));
	let mut direct = TableWriter::new(
		RecordingWriter {
			writes: Arc::clone(&writes),
		},
		1,
		Arc::clone(&opts),
		0,
	);
	add_entries(&mut direct);
	let size = direct.finish().unwrap();
	let direct_writes = writes.lock().unwrap().clone();
	assert_eq!(direct_writes.iter().sum::<usize>(), size);
	assert!(direct_writes.len() > 6, "a table of several blocks: {direct_writes:?}");

	// The same table through `FsWriter`: one `append` for each of those writes, none merged, none
	// split, and nothing else asked of the file.
	let path = dir.path().join("1.sst");
	let file =
		crate::vfs::Fs::create_truncate(&Arc::clone(&fs), &path, CreateOpts::default()).unwrap();
	let mut through_fs = TableWriter::new(FsWriter(file), 1, Arc::clone(&opts), 0);
	add_entries(&mut through_fs);
	let size_through_fs = through_fs.finish().unwrap();
	let appended: Vec<usize> = fs
		.entries()
		.iter()
		.filter(|entry| entry.starts_with("append ") && entry.contains("/1.sst "))
		.filter_map(|entry| entry.rsplit(' ').next().and_then(|n| n.parse().ok()))
		.collect();
	assert_eq!(appended, direct_writes);
	assert_eq!(size_through_fs, size);
	assert_eq!(std::fs::metadata(&path).unwrap().len() as usize, size);
	assert_eq!(
		fs.entries().iter().filter(|e| !e.starts_with("append")).count(),
		1,
		"only the creation: {:?}",
		fs.entries()
	);
}

#[tokio::test(flavor = "current_thread")]
async fn the_manifest_is_one_replace_and_one_read() {
	let dir = TempDir::new().unwrap();
	let fs = RecordingFs::new();
	let db = dir.path().join("db");
	let opts = options(&db, &fs);
	std::fs::create_dir_all(opts.manifest_dir()).unwrap();
	let name = "manifest/00000000000000000000.manifest";

	// Nothing there: it is looked for with `exists`, which is `Path::exists`, and written with one
	// atomic replacement.
	let manifest = LevelManifest::new_in(&Arc::clone(&fs), Arc::clone(&opts)).unwrap();
	let written = std::fs::read(opts.manifest_file_path(0)).unwrap();
	let entries = fs.take();
	assert_eq!(entries[0], format!("exists {name}"));
	assert_eq!(entries[1], format!("replace_atomic {name} {}", written.len()));
	assert_eq!(entries.len(), 2, "{entries:?}");

	// Written again: the same call, with the content of the manifest.
	write_manifest_to_disk_in(&Arc::clone(&fs), &manifest).unwrap();
	assert_eq!(fs.take(), vec![format!("replace_atomic {name} {}", written.len())]);
	assert_eq!(std::fs::read(opts.manifest_file_path(0)).unwrap(), written);

	// Loaded: one read of the file, one sweep of its directory, one listing of the log.
	let loaded = LevelManifest::new_in(&Arc::clone(&fs), Arc::clone(&opts)).unwrap();
	assert_eq!(loaded.last_sequence, 0);
	assert_eq!(
		fs.take(),
		vec![
			format!("exists {name}"),
			format!("read {name}"),
			"read_dir db/manifest".to_string(),
			"read_dir db/wal".to_string(),
		]
	);
}

#[tokio::test(flavor = "current_thread")]
async fn loading_a_manifest_opens_each_table_through_the_file_system() {
	let dir = TempDir::new().unwrap();
	let fs = RecordingFs::new();
	let db = dir.path().join("db");
	let opts = options(&db, &fs);
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	put(&tree, 0, 60).await;
	tree.flush().unwrap();
	put(&tree, 60, 120).await;
	tree.flush().unwrap();
	tree.close().await.unwrap();
	drop(tree);

	fs.take();
	let manifest = LevelManifest::load_from_file_in(
		&Arc::clone(&fs),
		opts.manifest_file_path(0),
		Arc::clone(&opts),
	)
	.unwrap();
	assert_eq!(manifest.iter().count(), 2);
	let opened: Vec<String> =
		fs.entries().into_iter().filter(|e| e.starts_with("open_read")).collect();
	// The order of the level: the newest table first.
	assert_eq!(opened, vec![format!("open_read {}", sst(2)), format!("open_read {}", sst(1))]);
}

#[test]
fn a_file_system_that_is_not_std_leaves_the_temporary_manifests_alone() {
	for (kind, swept) in [(FsKind::Std, true), (FsKind::Pool, false)] {
		let dir = TempDir::new().unwrap();
		let fs = RecordingFs::of_kind(kind);
		let db = dir.path().join("db");
		let opts = options(&db, &fs);
		std::fs::create_dir_all(opts.manifest_dir()).unwrap();
		LevelManifest::new_in(&Arc::clone(&fs), Arc::clone(&opts)).unwrap();
		let stale = opts.manifest_dir().join(".tmp_123_456");
		std::fs::write(&stale, b"left by a write that did not finish").unwrap();
		let kept = opts.manifest_dir().join("notes.txt");
		std::fs::write(&kept, b"not a temporary file").unwrap();

		fs.take();
		LevelManifest::load_from_file_in(
			&Arc::clone(&fs),
			opts.manifest_file_path(0),
			Arc::clone(&opts),
		)
		.unwrap();

		assert_eq!(stale.exists(), !swept, "{kind:?}");
		assert!(kept.exists());
		assert_eq!(
			fs.entries().contains(&"read_dir db/manifest".to_string()),
			swept,
			"{kind:?}: {:?}",
			fs.entries()
		);
	}
}

#[test]
fn the_log_number_of_a_manifest_is_checked_against_the_segments_the_file_system_lists() {
	let dir = TempDir::new().unwrap();
	let fs = RecordingFs::new();
	let wal = dir.path().join("wal");
	std::fs::create_dir_all(&wal).unwrap();
	let check = |log_number: u64| validate_wal_log_number_in(&Arc::clone(&fs), &wal, log_number);

	// No segments, or a directory that is not there: nothing to check against.
	check(99).unwrap();
	let absent = dir.path().join("absent");
	validate_wal_log_number_in(&Arc::clone(&fs), &absent, 99).unwrap();

	for id in [3u64, 5, 4] {
		std::fs::write(wal.join(format!("{id:020}.wal")), b"").unwrap();
	}
	check(6).unwrap();
	assert!(matches!(check(7), Err(Error::ManifestCorruption(_))));

	// Other extensions are not segments, and a directory is not a file.
	std::fs::write(wal.join(format!("{:020}.wal.corrupt", 9)), b"").unwrap();
	std::fs::create_dir(wal.join(format!("{:020}.wal", 11))).unwrap();
	assert!(matches!(check(7), Err(Error::ManifestCorruption(_))));
	check(6).unwrap();

	// A name that is not a segment's makes the listing an error, which skips the check: as it
	// always did, and which the characterisation keeps.
	std::fs::write(wal.join(".DS_Store"), b"").unwrap();
	check(99).unwrap();
}

#[test]
fn a_directory_with_files_and_no_manifest_is_counted_by_extension_through_the_file_system() {
	let dir = TempDir::new().unwrap();
	let fs = RecordingFs::new();
	let opts = options(&dir.path().join("db"), &fs);

	// Nothing at all: a new database may be made.
	check_manifest_before_open_in(&Arc::clone(&fs), &opts).unwrap();
	assert_eq!(
		fs.take(),
		vec![
			"try_exists manifest/00000000000000000000.manifest".to_string(),
			"read_dir db/sstables".to_string(),
			"read_dir db/wal".to_string(),
			"read_dir db/vlog".to_string(),
		]
	);

	// An entry of the table directory with the extension counts, whatever it is.
	std::fs::create_dir_all(opts.sstable_dir().join("1.sst")).unwrap();
	let refused = check_manifest_before_open_in(&Arc::clone(&fs), &opts).unwrap_err();
	assert!(matches!(&refused, Error::ManifestMissing(text) if text.contains("1 SSTable(s)")));
}

#[test]
fn a_file_system_that_cannot_answer_is_an_error_on_the_open_path_and_not_an_empty_database() {
	let dir = TempDir::new().unwrap();
	let opts = Options {
		path: dir.path().to_path_buf(),
		..Default::default()
	};
	let unavailable = UnavailableFs::new("nothing is mounted");

	let refused = check_manifest_before_open_in(&unavailable, &opts).unwrap_err();
	assert!(
		matches!(&refused, Error::Io(e) if e.kind() == io::ErrorKind::Unsupported),
		"{refused}"
	);
	let refused = crate::checkpoint::check_restore_finished_in(&unavailable, &opts).unwrap_err();
	assert!(
		matches!(&refused, Error::Io(e) if e.kind() == io::ErrorKind::Unsupported),
		"{refused}"
	);
}

#[test]
fn an_interrupted_restore_is_found_through_the_file_system() {
	let dir = TempDir::new().unwrap();
	let fs = RecordingFs::new();
	let opts = options(dir.path(), &fs);
	let marker = dir.path().join("RESTORE_IN_PROGRESS");

	crate::checkpoint::check_restore_finished_in(&Arc::clone(&fs), &opts).unwrap();
	assert_eq!(fs.take(), vec![format!("try_exists {}", short(&marker))]);

	std::fs::write(&marker, b"/backups/cp-7\n").unwrap();
	let refused =
		crate::checkpoint::check_restore_finished_in(&Arc::clone(&fs), &opts).unwrap_err();
	assert!(
		matches!(&refused, Error::RestoreInterrupted(text) if text.contains("'/backups/cp-7'")),
		"{refused}"
	);

	// A marker that is not text is still a marker, with no name to give.
	std::fs::write(&marker, [0xff, 0xfe, 0x00]).unwrap();
	let refused =
		crate::checkpoint::check_restore_finished_in(&Arc::clone(&fs), &opts).unwrap_err();
	assert!(matches!(&refused, Error::RestoreInterrupted(text) if text.contains("checkpoint ''")));
}

#[test]
fn the_tables_that_the_manifest_does_not_list_are_found_by_listing_through_the_file_system() {
	let dir = TempDir::new().unwrap();
	let fs = RecordingFs::new();
	let opts = options(dir.path(), &fs);
	std::fs::create_dir_all(opts.sstable_dir()).unwrap();
	for id in [1u64, 2, 3] {
		std::fs::write(opts.sstable_dir().join(format!("{id:020}.sst")), b"x").unwrap();
	}
	std::fs::write(opts.sstable_dir().join("notes.txt"), b"x").unwrap();
	let live = std::collections::HashSet::from([2u64]);

	let mut orphans: Vec<u64> = plan_orphan_cleanup_in(&Arc::clone(&fs), &opts, &live)
		.unwrap()
		.into_iter()
		.map(|(id, path)| {
			assert_eq!(path, opts.sstable_dir().join(format!("{id:020}.sst")));
			id
		})
		.collect();
	orphans.sort_unstable();
	assert_eq!(orphans, vec![1, 3]);
	// Names only, as for any database that is not encrypted: no file is read.
	let table_dir = short(&opts.sstable_dir());
	assert_eq!(fs.take(), vec![format!("exists {table_dir}"), format!("read_dir {table_dir}")]);

	// No directory, no orphans.
	let none = options(&dir.path().join("elsewhere"), &fs);
	assert!(plan_orphan_cleanup_in(&Arc::clone(&fs), &none, &live).unwrap().is_empty());
}

/// The file that the table writer was given stays the file's own: it is closed by the writer.
#[test]
fn a_table_file_is_a_handle_of_its_own_that_the_writer_gives_up() {
	let dir = TempDir::new().unwrap();
	let fs = RecordingFs::new();
	let path = dir.path().join("1.sst");
	let file: Arc<dyn FsFile> =
		crate::vfs::Fs::create_truncate(&Arc::clone(&fs), &path, CreateOpts::default()).unwrap();
	// The writer takes the only handle: finishing it drops it, so what is left is the path.
	let weak = Arc::downgrade(&file);
	let mut writer = TableWriter::new(FsWriter(file), 1, options(dir.path(), &fs), 0);
	add_entries(&mut writer);
	writer.finish().unwrap();
	assert!(weak.upgrade().is_none(), "the handle that wrote the table is dropped by finish");
}
