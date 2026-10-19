//! What the open does with a record of the last segment of the log whose value is not in the value
//! log, in each recovery mode, on the disk and over the memory file system.
//!
//! Every state is made by hand: Eventual commits with the value log, a tree that is abandoned (the
//! process is gone and the files are as it left them), and then only the value log is damaged. The
//! cases are the ways a value log can fail to hold what the log points to: it lost its tail, a file
//! is gone, a page of a value is zeros, a stored checksum changed. In each, the entries before the
//! first failure are whole and the ones after it are, or are not.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use super::fs_engine_tests::{commit_range, key_of, read_range, value_of, within};
use crate::vfs::CreateOpts;
use crate::wal::reader::Reader;
use crate::wal::WalContext;
use crate::{
	CrashMode,
	Durability,
	Error,
	FileSystem,
	Options,
	Tree,
	TreeBuilder,
	WalRecoveryMode,
};

const COMMITS: usize = 12;
const VALUE: usize = 300;
/// The header of a plaintext file of the value log, where its first entry begins.
const VLOG_HEADER: usize = 31;
/// Small enough for four entries to a file, which gives three files for the commits.
const SMALL_FILES: u64 = 1200;
const BIG_FILES: u64 = 1 << 20;

/// A database in a file system that a test can look into and damage: the memory file system, or a
/// directory.
struct World {
	fs: FileSystem,
	root: PathBuf,
	name: &'static str,
	/// The directory of a world on the disk, which goes with it.
	dir: Option<TempDir>,
}

impl World {
	fn memory() -> Self {
		Self {
			fs: FileSystem::memory(),
			root: PathBuf::from("db"),
			name: "memory",
			dir: None,
		}
	}

	fn on_disk() -> Self {
		let dir = TempDir::new().unwrap();
		Self {
			fs: FileSystem::std(),
			root: dir.path().join("db"),
			name: "disk",
			dir: Some(dir),
		}
	}

	fn all() -> [World; 2] {
		[World::memory(), World::on_disk()]
	}

	fn options(&self, mode: WalRecoveryMode, vlog_file_size: u64) -> Options {
		Options {
			path: self.root.clone(),
			max_memtable_size: 1 << 20,
			level0_max_files: 2,
			enable_vlog: true,
			vlog_value_threshold: 64,
			vlog_max_file_size: vlog_file_size,
			vlog_checksum_verification: crate::VLogChecksumLevel::Full,
			wal_recovery_mode: mode,
			..Options::default()
		}
		.with_file_system(self.fs.clone())
	}

	async fn open(&self, mode: WalRecoveryMode, vlog_file_size: u64) -> crate::Result<Tree> {
		TreeBuilder::with_options(self.options(mode, vlog_file_size)).build_async().await
	}

	/// The state that a process that is gone leaves behind after `COMMITS` Eventual commits: every
	/// byte that was written is in the files.
	async fn after_a_crash(self, vlog_file_size: u64) -> World {
		let tree = self.open(WalRecoveryMode::TolerateTornTail, vlog_file_size).await.unwrap();
		commit_range(&tree, 0..COMMITS, VALUE, Durability::Eventual).await;
		tree.abandon();
		self.fs.abandon();
		drop(tree);
		match self.dir {
			Some(_) => self,
			None => World {
				fs: self.fs.simulate_crash(CrashMode::Prefix(usize::MAX)).unwrap(),
				..self
			},
		}
	}

	/// A copy that the open of one mode cannot change for the next.
	fn fork(&self) -> World {
		match &self.dir {
			None => World {
				fs: self.fs.simulate_crash(CrashMode::Prefix(usize::MAX)).unwrap(),
				root: self.root.clone(),
				name: self.name,
				dir: None,
			},
			Some(dir) => {
				let copy = TempDir::new().unwrap();
				copy_dir(dir.path(), copy.path());
				World {
					fs: self.fs.clone(),
					root: copy.path().join("db"),
					name: self.name,
					dir: Some(copy),
				}
			}
		}
	}

	fn path(&self, relative: &str) -> PathBuf {
		self.root.join(relative)
	}

	fn read(&self, relative: &str) -> Vec<u8> {
		self.fs.0.read(&self.path(relative)).unwrap()
	}

	fn write(&self, relative: &str, bytes: &[u8]) {
		let file = self.fs.0.create_truncate(&self.path(relative), CreateOpts::default()).unwrap();
		file.append(bytes).unwrap();
		file.sync().unwrap();
	}

	fn truncate(&self, relative: &str, len: u64) {
		let file = self.fs.0.open_append_existing(&self.path(relative)).unwrap();
		file.set_len(len).unwrap();
		file.sync().unwrap();
	}

	fn remove(&self, relative: &str) {
		self.fs.0.remove_file(&self.path(relative)).unwrap();
	}

	/// The names in a directory of the database, in order.
	fn list(&self, relative: &str) -> Vec<String> {
		let mut names: Vec<String> = self
			.fs
			.0
			.read_dir(&self.path(relative))
			.unwrap()
			.into_iter()
			.map(|entry| entry.name.to_string_lossy().into_owned())
			.collect();
		names.sort();
		names
	}

	/// Every file of the log and of the value log, with its bytes.
	fn snapshot(&self) -> BTreeMap<String, Vec<u8>> {
		let mut files = BTreeMap::new();
		for directory in ["wal", "vlog"] {
			for name in self.list(directory) {
				let path = format!("{directory}/{name}");
				files.insert(path.clone(), self.read(&path));
			}
		}
		files
	}

	/// The segment of the log that holds the commits: the last, which is not empty.
	fn segment(&self) -> (u64, String) {
		let name = self
			.list("wal")
			.into_iter()
			.filter(|name| name.ends_with(".wal"))
			.rfind(|name| !self.read(&format!("wal/{name}")).is_empty())
			.expect("a segment holds the commits");
		(name[..20].parse().unwrap(), format!("wal/{name}"))
	}

	/// Where each record of the segment ends.
	fn record_ends(&self, segment: &str, segment_id: u64) -> Vec<usize> {
		let context =
			WalContext::for_options(&self.options(WalRecoveryMode::TolerateTornTail, BIG_FILES))
				.unwrap();
		let file = self.fs.0.open_read(&self.path(segment)).unwrap();
		let mut reader = Reader::with_context(file, None, segment_id, &context);
		let mut ends = Vec::new();
		while let Ok((_, end)) = reader.read() {
			ends.push(end as usize);
		}
		ends
	}

	/// The entries of the value log, in the order they were appended: the file, where the entry
	/// begins and how long it is.
	fn entries(&self) -> Vec<Entry> {
		let mut entries = Vec::new();
		for name in self.list("vlog").into_iter().filter(|name| name.ends_with(".vlog")) {
			let file = format!("vlog/{name}");
			let bytes = self.read(&file);
			let mut at = VLOG_HEADER;
			while at + 8 <= bytes.len() {
				let key = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
				let value = u32::from_be_bytes(bytes[at + 4..at + 8].try_into().unwrap()) as usize;
				let len = 8 + key + value + 4;
				if at + len > bytes.len() {
					break;
				}
				entries.push(Entry {
					file: file.clone(),
					offset: at,
					len,
				});
				at += len;
			}
		}
		entries
	}
}

#[derive(Clone, Debug)]
struct Entry {
	file: String,
	offset: usize,
	len: usize,
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
	std::fs::create_dir_all(to).unwrap();
	for entry in std::fs::read_dir(from).unwrap() {
		let entry = entry.unwrap();
		let target = to.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &target);
		} else {
			std::fs::copy(entry.path(), &target).unwrap();
		}
	}
}

/// What was done to the value log, and what the open must make of it.
struct Case {
	what: &'static str,
	vlog_file_size: u64,
	damage: fn(&World, &[Entry]),
	/// The first record whose value is not in the value log.
	first_failed: usize,
	/// Whether no value after it is in the value log: a log that lost its tail, and not a hole.
	lost_its_tail: bool,
}

fn cases() -> Vec<Case> {
	vec![
		Case {
			what: "the file was cut in the middle of an entry",
			vlog_file_size: BIG_FILES,
			damage: |world, entries| {
				let entry = &entries[8];
				world.truncate(&entry.file, (entry.offset + entry.len / 2) as u64);
			},
			first_failed: 8,
			lost_its_tail: true,
		},
		Case {
			what: "the file was cut at the end of an entry",
			vlog_file_size: BIG_FILES,
			damage: |world, entries| {
				let entry = &entries[5];
				world.truncate(&entry.file, entry.offset as u64);
			},
			first_failed: 5,
			lost_its_tail: true,
		},
		Case {
			what: "the newest file is gone",
			vlog_file_size: SMALL_FILES,
			damage: |world, entries| world.remove(&entries[8].file),
			first_failed: 8,
			lost_its_tail: true,
		},
		Case {
			what: "the checksum of the last entry changed",
			vlog_file_size: BIG_FILES,
			damage: |world, entries| {
				let entry = &entries[COMMITS - 1];
				let mut bytes = world.read(&entry.file);
				bytes[entry.offset + entry.len - 1] ^= 0x01;
				world.write(&entry.file, &bytes);
			},
			first_failed: COMMITS - 1,
			lost_its_tail: true,
		},
		Case {
			what: "a page in the middle of a value is zeros",
			vlog_file_size: BIG_FILES,
			damage: |world, entries| {
				let entry = &entries[5];
				let mut bytes = world.read(&entry.file);
				let middle = entry.offset + entry.len / 2;
				bytes[middle..middle + 40].fill(0);
				world.write(&entry.file, &bytes);
			},
			first_failed: 5,
			lost_its_tail: false,
		},
		Case {
			what: "the checksum of an entry in the middle changed",
			vlog_file_size: BIG_FILES,
			damage: |world, entries| {
				let entry = &entries[3];
				let mut bytes = world.read(&entry.file);
				bytes[entry.offset + entry.len - 2] ^= 0x10;
				world.write(&entry.file, &bytes);
			},
			first_failed: 3,
			lost_its_tail: false,
		},
		Case {
			what: "an older file is gone",
			vlog_file_size: SMALL_FILES,
			damage: |world, entries| world.remove(&entries[4].file),
			first_failed: 4,
			lost_its_tail: false,
		},
		Case {
			what: "the header of the first entry says other lengths",
			vlog_file_size: BIG_FILES,
			damage: |world, entries| {
				let entry = &entries[0];
				let mut bytes = world.read(&entry.file);
				bytes[entry.offset + 3] ^= 0x01;
				world.write(&entry.file, &bytes);
			},
			first_failed: 0,
			lost_its_tail: false,
		},
	]
}

/// Opens `world` as it is, in `mode`, and gives the tree or the error: on a copy, so that what an
/// open does to its files does not reach the next.
async fn opened_in(
	world: &World,
	mode: WalRecoveryMode,
	vlog_file_size: u64,
) -> (World, crate::Result<Tree>) {
	let copy = world.fork();
	let result = copy.open(mode, vlog_file_size).await;
	(copy, result)
}

fn assert_keys(tree: &Tree, kept: usize, what: &str) {
	for (i, got) in read_range(tree, 0..COMMITS) {
		if i < kept {
			assert_eq!(got, Some(value_of(i, VALUE)), "{what}: key {i} is kept");
		} else {
			assert_eq!(got, None, "{what}: key {i} is left out");
		}
	}
}

#[test(tokio::test)]
async fn absolute_consistency_refuses_the_open_and_names_the_segment_and_the_record() {
	within("absolute consistency", async {
		for case in cases() {
			for world in World::all() {
				let what = format!("{}: {}", world.name, case.what);
				let world = world.after_a_crash(case.vlog_file_size).await;
				let entries = world.entries();
				assert_eq!(entries.len(), COMMITS, "{what}: an entry for every commit");
				let (segment_id, segment) = world.segment();
				let ends = world.record_ends(&segment, segment_id);
				assert_eq!(ends.len(), COMMITS, "{what}: a record for every commit");
				(case.damage)(&world, &entries);

				let before = world.snapshot();
				let (copy, opened) =
					opened_in(&world, WalRecoveryMode::AbsoluteConsistency, case.vlog_file_size)
						.await;
				let before_open = copy.snapshot();
				assert_eq!(before_open, before);
				let Err(error) = opened else {
					panic!("{what}: the open succeeded");
				};
				let start = if case.first_failed == 0 {
					0
				} else {
					ends[case.first_failed - 1]
				};
				match &error {
					Error::WalCorruption {
						segment_id: found_segment,
						offset,
						message,
					} => {
						assert_eq!(*found_segment as u64, segment_id, "{what}");
						assert_eq!(
							*offset, start,
							"{what}: the start of record {}",
							case.first_failed
						);
						assert!(message.contains("value log"), "{what}: {message}");
					}
					other => panic!("{what}: {other:?}"),
				}
				assert_eq!(copy.snapshot(), before, "{what}: the open left every file as it was");
			}
		}
	})
	.await;
}

#[test(tokio::test)]
async fn the_default_mode_ends_the_log_before_a_value_that_is_missing_when_none_after_it_is_there()
{
	within("tolerate torn tail", async {
		for case in cases() {
			for world in World::all() {
				let what = format!("{}: {}", world.name, case.what);
				let world = world.after_a_crash(case.vlog_file_size).await;
				let entries = world.entries();
				let (segment_id, segment) = world.segment();
				let ends = world.record_ends(&segment, segment_id);
				let original = world.read(&segment);
				(case.damage)(&world, &entries);
				let before = world.snapshot();

				let (copy, opened) =
					opened_in(&world, WalRecoveryMode::TolerateTornTail, case.vlog_file_size).await;
				if !case.lost_its_tail {
					// A value that is missing with values after it that are there is a hole in
					// the log and not its end: the open says so and does not cut.
					let Err(error) = opened else {
						panic!("{what}: the open succeeded");
					};
					let text = error.to_string();
					assert!(matches!(error, Error::Other(_)), "{what}: {text}");
					assert!(text.contains("SalvageValidPrefix"), "{what}: {text}");
					assert!(text.contains(&format!("{segment_id:020}.wal")), "{what}: {text}");
					assert_eq!(
						copy.snapshot(),
						before,
						"{what}: the open left every file as it was"
					);
					continue;
				}
				let tree = opened.unwrap_or_else(|e| panic!("{what}: {e}"));
				assert_cut(&copy, &tree, &case, &segment, &original, &ends, &what);
				tree.close().await.unwrap();
			}
		}
	})
	.await;
}

#[test(tokio::test)]
async fn salvage_ends_the_log_before_the_first_value_that_is_missing_whatever_follows_it() {
	within("salvage", async {
		for case in cases() {
			for world in World::all() {
				let what = format!("{}: {}", world.name, case.what);
				let world = world.after_a_crash(case.vlog_file_size).await;
				let entries = world.entries();
				let (segment_id, segment) = world.segment();
				let ends = world.record_ends(&segment, segment_id);
				let original = world.read(&segment);
				(case.damage)(&world, &entries);

				let (copy, opened) =
					opened_in(&world, WalRecoveryMode::SalvageValidPrefix, case.vlog_file_size)
						.await;
				let tree = opened.unwrap_or_else(|e| panic!("{what}: {e}"));
				assert_cut(&copy, &tree, &case, &segment, &original, &ends, &what);
				tree.close().await.unwrap();
			}
		}
	})
	.await;
}

/// What an open that cut the segment must have left: the report, the segment cut at the start of
/// the first record that failed, the segment as it was kept, and the keys.
fn assert_cut(
	world: &World,
	tree: &Tree,
	case: &Case,
	segment: &str,
	original: &[u8],
	ends: &[usize],
	what: &str,
) {
	let cut = if case.first_failed == 0 {
		0
	} else {
		ends[case.first_failed - 1]
	};
	let report = tree.recovery_report();
	assert_eq!(report.records_dropped, COMMITS - case.first_failed, "{what}: {report:?}");
	assert_eq!(report.bytes_trimmed as usize, original.len() - cut, "{what}: {report:?}");
	assert_eq!(
		report.quarantined_files,
		[world.path(&format!("{segment}.corrupt"))],
		"{what}: the segment as it was is kept"
	);
	if world.dir.is_none() && case.first_failed == 0 {
		// A pool gives a copy back once nothing of its segment is needed, which a cut before
		// the first record leaves true of every segment: the report still names it.
		assert!(world.list("wal").iter().all(|name| !name.contains("corrupt")), "{what}");
	} else {
		assert_eq!(world.read(&format!("{segment}.corrupt")), original, "{what}: with its bytes");
	}
	assert_eq!(report.records_replayed, case.first_failed, "{what}: {report:?}");
	assert_keys(tree, case.first_failed, what);
}

#[test(tokio::test)]
async fn a_database_that_was_cut_takes_commits_and_keeps_them_through_a_close_and_a_new_open() {
	within("cut and go on", async {
		for case in cases().into_iter().filter(|case| case.lost_its_tail) {
			for world in World::all() {
				let what = format!("{}: {}", world.name, case.what);
				let world = world.after_a_crash(case.vlog_file_size).await;
				let entries = world.entries();
				(case.damage)(&world, &entries);

				let tree = world
					.open(WalRecoveryMode::SalvageValidPrefix, case.vlog_file_size)
					.await
					.unwrap_or_else(|e| panic!("{what}: {e}"));
				// The sequence numbers go on from the last record that was kept, and the new
				// commits reuse the places in the value log that the dropped ones had.
				commit_range(&tree, 100..120, VALUE, Durability::Immediate).await;
				assert_keys(&tree, case.first_failed, &what);
				for (i, got) in read_range(&tree, 100..120) {
					assert_eq!(got, Some(value_of(i, VALUE)), "{what}: new key {i}");
				}
				tree.close().await.unwrap();
				drop(tree);

				// Closed, the database opens in the strictest mode, and has nothing to repair.
				let again = world
					.open(WalRecoveryMode::AbsoluteConsistency, case.vlog_file_size)
					.await
					.unwrap_or_else(|e| panic!("{what}: {e}"));
				assert_keys(&again, case.first_failed, &what);
				for (i, got) in read_range(&again, 100..120) {
					assert_eq!(got, Some(value_of(i, VALUE)), "{what}: new key {i}");
				}
				let report = again.recovery_report();
				assert_eq!(
					(report.records_dropped, report.bytes_trimmed, report.quarantined_files.len()),
					(0, 0, 0),
					"{what}: {report:?}"
				);
				again.close().await.unwrap();
			}
		}
	})
	.await;
}

/// A cut that was made, and a crash of the operating system that follows it: every commit that was
/// acknowledged after the cut is there, and the strictest mode opens the result. The memory file
/// system loses what was not synced, which a directory does not.
#[test(tokio::test)]
async fn a_database_that_was_cut_keeps_what_was_acknowledged_after_the_cut_through_a_crash() {
	within("cut and crash", async {
		for case in cases().into_iter().filter(|case| case.lost_its_tail) {
			let what = case.what;
			let world = World::memory().after_a_crash(case.vlog_file_size).await;
			let entries = world.entries();
			(case.damage)(&world, &entries);

			let tree = world
				.open(WalRecoveryMode::SalvageValidPrefix, case.vlog_file_size)
				.await
				.unwrap_or_else(|e| panic!("{what}: {e}"));
			commit_range(&tree, 100..110, VALUE, Durability::Immediate).await;
			tree.abandon();
			world.fs.abandon();
			drop(tree);

			let modes = [
				CrashMode::LoseUnsynced,
				CrashMode::ZeroTail,
				CrashMode::Reorder(1),
				CrashMode::Reorder(2),
				CrashMode::LoseTear {
					lost: 2,
					torn: usize::MAX,
					n: 0,
				},
			];
			for mode in modes {
				let crashed = World {
					fs: world.fs.simulate_crash(mode).unwrap(),
					root: world.root.clone(),
					name: world.name,
					dir: None,
				};
				let tree = crashed
					.open(WalRecoveryMode::AbsoluteConsistency, case.vlog_file_size)
					.await
					.unwrap_or_else(|e| panic!("{what}, {mode:?}: {e}"));
				assert_keys(&tree, case.first_failed, &format!("{what}, {mode:?}"));
				for (i, got) in read_range(&tree, 100..110) {
					assert_eq!(got, Some(value_of(i, VALUE)), "{what}, {mode:?}: new key {i}");
				}
				tree.abandon();
			}
		}
	})
	.await;
}

/// The newest file of the value log whose first bytes are zeros, with the entries after them: the
/// first page of the file was not written back and the later ones were. The open sets the file
/// aside, and no record of the log points into a file that is there, so the segment ends before its
/// first record, or the open fails in the mode that refuses.
#[test(tokio::test)]
async fn a_value_log_file_that_lost_its_first_page_is_set_aside_and_the_log_ends_before_its_first_record(
) {
	within("zeroed header", async {
		for world in World::all() {
			let what = world.name;
			let world = world.after_a_crash(BIG_FILES).await;
			let entries = world.entries();
			let (segment_id, segment) = world.segment();
			let original = world.read(&segment);
			let mut bytes = world.read(&entries[0].file);
			bytes[..VLOG_HEADER].fill(0);
			world.write(&entries[0].file, &bytes);

			let (copy, opened) =
				opened_in(&world, WalRecoveryMode::AbsoluteConsistency, BIG_FILES).await;
			let error = opened.err().unwrap_or_else(|| panic!("{what}: the open succeeded"));
			assert!(
				matches!(
					&error,
					Error::WalCorruption { segment_id: found, offset: 0, .. }
						if *found as u64 == segment_id
				),
				"{what}: {error}"
			);
			assert_eq!(copy.read(&segment), original, "{what}: the log was left as it was");

			for mode in [WalRecoveryMode::TolerateTornTail, WalRecoveryMode::SalvageValidPrefix] {
				let (copy, opened) = opened_in(&world, mode, BIG_FILES).await;
				let tree = opened.unwrap_or_else(|e| panic!("{what}, {mode:?}: {e}"));
				let report = tree.recovery_report();
				assert_eq!(report.records_dropped, COMMITS, "{what}, {mode:?}: {report:?}");
				assert_keys(&tree, 0, what);
				let files = copy.list("vlog");
				assert!(
					files.iter().any(|name| name.ends_with(".vlog.torn")),
					"{what}, {mode:?}: the file was set aside: {files:?}"
				);
				assert!(!files.iter().any(|name| name.ends_with("1.vlog")), "{what}: {files:?}");
				// The next commits go to a file of their own, and are there after a close.
				commit_range(&tree, 100..110, VALUE, Durability::Immediate).await;
				tree.close().await.unwrap();
				drop(tree);
				let again = copy
					.open(WalRecoveryMode::AbsoluteConsistency, BIG_FILES)
					.await
					.unwrap_or_else(|e| panic!("{what}, {mode:?}: {e}"));
				for (i, got) in read_range(&again, 100..110) {
					assert_eq!(got, Some(value_of(i, VALUE)), "{what}, {mode:?}: key {i}");
				}
				again.close().await.unwrap();
			}
		}
	})
	.await;
}

/// A file of the value log that is followed by another, and whose first bytes are zeros with the
/// entries after them: the first page of a file that was rolled over from was not written back. The
/// open sets the file aside as it does the newest, the records that point into it are the first
/// that the value log does not hold, and the log is held to that in each mode.
#[test(tokio::test)]
async fn an_older_value_log_file_that_lost_its_first_page_is_set_aside_and_the_log_ends_there() {
	within("older zeroed header", async {
		// The entries of the first file, and of the second, which is not the newest.
		for (first_failed, what) in [(0, "the oldest file"), (4, "a file between two")] {
			for world in World::all() {
				let what = format!("{}, {what}", world.name);
				let world = world.after_a_crash(SMALL_FILES).await;
				let entries = world.entries();
				assert_eq!(entries.len(), COMMITS, "{what}");
				let (segment_id, segment) = world.segment();
				let ends = world.record_ends(&segment, segment_id);
				let original = world.read(&segment);
				let damaged = &entries[first_failed].file;
				let mut bytes = world.read(damaged);
				bytes[..VLOG_HEADER].fill(0);
				world.write(damaged, &bytes);
				let start = if first_failed == 0 {
					0
				} else {
					ends[first_failed - 1]
				};

				let (copy, opened) =
					opened_in(&world, WalRecoveryMode::AbsoluteConsistency, SMALL_FILES).await;
				let error = opened.err().unwrap_or_else(|| panic!("{what}: the open succeeded"));
				assert!(
					matches!(
						&error,
						Error::WalCorruption { segment_id: found, offset, .. }
							if *found as u64 == segment_id && *offset == start
					),
					"{what}: {error}"
				);
				assert_eq!(copy.read(&segment), original, "{what}: the log was left as it was");

				// Later records point to files that are whole: a hole in the log and not its end,
				// which the default mode leaves to the person who opens the database.
				let (copy, opened) =
					opened_in(&world, WalRecoveryMode::TolerateTornTail, SMALL_FILES).await;
				let error = opened.err().unwrap_or_else(|| panic!("{what}: the open succeeded"));
				assert!(error.to_string().contains("SalvageValidPrefix"), "{what}: {error}");
				assert_eq!(copy.read(&segment), original, "{what}: the log was left as it was");

				let (copy, opened) =
					opened_in(&world, WalRecoveryMode::SalvageValidPrefix, SMALL_FILES).await;
				let tree = opened.unwrap_or_else(|e| panic!("{what}: {e}"));
				let report = tree.recovery_report();
				assert_eq!(report.records_dropped, COMMITS - first_failed, "{what}: {report:?}");
				assert_keys(&tree, first_failed, &what);
				let files = copy.list("vlog");
				assert!(
					files.iter().any(|name| name.ends_with(".vlog.torn")),
					"{what}: the file was set aside: {files:?}"
				);
				// The log goes on, and what it takes now is there after a close.
				commit_range(&tree, 100..110, VALUE, Durability::Immediate).await;
				tree.close().await.unwrap();
				drop(tree);
				let again = copy
					.open(WalRecoveryMode::AbsoluteConsistency, SMALL_FILES)
					.await
					.unwrap_or_else(|e| panic!("{what}: {e}"));
				assert_keys(&again, first_failed, &what);
				for (i, got) in read_range(&again, 100..110) {
					assert_eq!(got, Some(value_of(i, VALUE)), "{what}: key {i}");
				}
				again.close().await.unwrap();
			}
		}
	})
	.await;
}

/// The state that a process that is gone leaves behind after Eventual commits of the given shapes,
/// one transaction for each slice of keys: every byte that was written is in the files.
async fn after_commits_of(world: World, shapes: &[&[usize]]) -> World {
	let tree = world.open(WalRecoveryMode::TolerateTornTail, BIG_FILES).await.unwrap();
	for keys in shapes {
		let mut txn = tree.begin().unwrap();
		txn.set_durability(Durability::Eventual);
		for &i in *keys {
			txn.set(key_of(i), value_of(i, VALUE)).unwrap();
		}
		txn.commit().await.unwrap();
	}
	tree.abandon();
	world.fs.abandon();
	drop(tree);
	match world.dir {
		Some(_) => world,
		None => World {
			fs: world.fs.simulate_crash(CrashMode::Prefix(usize::MAX)).unwrap(),
			..world
		},
	}
}

/// Zeros where the middle of the value of `entry` is: a page of it that was not written back.
fn lose_the_middle_of(world: &World, entry: &Entry) {
	let mut bytes = world.read(&entry.file);
	let middle = entry.offset + entry.len / 2;
	bytes[middle..middle + 40].fill(0);
	world.write(&entry.file, &bytes);
}

/// A record that holds several values is held to the value log for each of them: whichever of its
/// values is not there, the log ends before the record, and the record is not replayed in part.
/// The log is three records, the second of which is one transaction of three keys.
#[test(tokio::test)]
async fn a_record_of_several_values_ends_the_log_where_any_of_its_values_is_not_in_the_value_log() {
	within("one record, three values", async {
		// Each of the three values of the second record, in the order that they were appended.
		for damaged in 1..=3 {
			for world in World::all() {
				let what = format!("{}, value {damaged} of the record", world.name);
				let world = after_commits_of(world, &[&[0], &[1, 2, 3], &[4]]).await;
				let entries = world.entries();
				assert_eq!(entries.len(), 5, "{what}");
				let (segment_id, segment) = world.segment();
				let ends = world.record_ends(&segment, segment_id);
				assert_eq!(ends.len(), 3, "{what}: a record for each transaction");
				lose_the_middle_of(&world, &entries[damaged]);
				let original = world.read(&segment);
				let keys = |tree: &Tree, kept: usize| {
					for (i, got) in read_range(tree, 0..5) {
						let expected = (i < kept).then(|| value_of(i, VALUE));
						assert_eq!(got, expected, "{what}: key {i}");
					}
				};

				// The strictest mode names the record, whichever of its values it is.
				let (copy, opened) =
					opened_in(&world, WalRecoveryMode::AbsoluteConsistency, BIG_FILES).await;
				let error = opened.err().unwrap_or_else(|| panic!("{what}: the open succeeded"));
				assert!(
					matches!(
						&error,
						Error::WalCorruption { segment_id: found, offset, .. }
							if *found as u64 == segment_id && *offset == ends[0]
					),
					"{what}: {error}"
				);
				assert_eq!(copy.read(&segment), original, "{what}: the log was left as it was");

				// The record after it has its value, so the default mode sees a hole.
				let (copy, opened) =
					opened_in(&world, WalRecoveryMode::TolerateTornTail, BIG_FILES).await;
				let error = opened.err().unwrap_or_else(|| panic!("{what}: the open succeeded"));
				assert!(error.to_string().contains("SalvageValidPrefix"), "{what}: {error}");
				assert_eq!(copy.read(&segment), original, "{what}: the log was left as it was");

				// The mode that salvages ends the log before the record, and none of its keys is
				// there: the values of the record that are whole are not replayed either.
				let (copy, opened) =
					opened_in(&world, WalRecoveryMode::SalvageValidPrefix, BIG_FILES).await;
				let tree = opened.unwrap_or_else(|e| panic!("{what}: {e}"));
				let report = tree.recovery_report();
				assert_eq!(report.records_dropped, 2, "{what}: {report:?}");
				assert_eq!(report.records_replayed, 1, "{what}: {report:?}");
				assert_eq!(report.bytes_trimmed as usize, original.len() - ends[0], "{what}");
				keys(&tree, 1);
				assert_eq!(
					copy.read(&segment).len(),
					ends[0],
					"{what}: ends where the record began"
				);
				tree.close().await.unwrap();
			}
		}
	})
	.await;
}

/// What follows the first value that is missing decides what the default mode does, and a record
/// of several values follows it as soon as one of them is there: the log is not the end of a value
/// log that lost its tail, but has a hole, and is not cut. The log is three records: the value of
/// the second is not in the value log, and the third is one transaction of two keys, of which only
/// one value is.
#[test(tokio::test)]
async fn a_later_record_with_any_one_of_its_values_in_the_value_log_makes_a_hole_and_not_an_end() {
	within("a record after the first that is missing", async {
		// Either of the two values of the last record is the one that is there.
		for whole in [2, 3] {
			for world in World::all() {
				let what = format!("{}, value {whole} is there", world.name);
				let world = after_commits_of(world, &[&[0], &[1], &[2, 3]]).await;
				let entries = world.entries();
				assert_eq!(entries.len(), 4, "{what}");
				let (segment_id, segment) = world.segment();
				let ends = world.record_ends(&segment, segment_id);
				assert_eq!(ends.len(), 3, "{what}: a record for each transaction");
				lose_the_middle_of(&world, &entries[1]);
				lose_the_middle_of(&world, &entries[5 - whole]);
				let original = world.read(&segment);

				let (copy, opened) =
					opened_in(&world, WalRecoveryMode::TolerateTornTail, BIG_FILES).await;
				let error = opened.err().unwrap_or_else(|| panic!("{what}: the open succeeded"));
				assert!(
					matches!(&error, Error::Other(_))
						&& error.to_string().contains("SalvageValidPrefix"),
					"{what}: {error}"
				);
				assert_eq!(copy.read(&segment), original, "{what}: the log was left as it was");

				let (_copy, opened) =
					opened_in(&world, WalRecoveryMode::SalvageValidPrefix, BIG_FILES).await;
				let tree = opened.unwrap_or_else(|e| panic!("{what}: {e}"));
				let report = tree.recovery_report();
				assert_eq!((report.records_dropped, report.records_replayed), (2, 1), "{what}");
				for (i, got) in read_range(&tree, 0..4) {
					let expected = (i == 0).then(|| value_of(i, VALUE));
					assert_eq!(got, expected, "{what}: key {i}");
				}
				tree.close().await.unwrap();
			}
		}
	})
	.await;
}

/// The end key of a range delete is the bytes of a key and not a value: whatever it looks like, the
/// open does not look for a value of the value log that it names. 27 bytes of a letter have the
/// length and the flag of a pointer, to a file that is not there.
#[test(tokio::test)]
async fn the_end_key_of_a_range_delete_is_not_looked_for_in_the_value_log() {
	within("range delete", async {
		for world in World::all() {
			let what = world.name;
			let tree = world.open(WalRecoveryMode::TolerateTornTail, BIG_FILES).await.unwrap();
			commit_range(&tree, 0..3, VALUE, Durability::Eventual).await;
			let mut txn = tree.begin().unwrap();
			txn.set_durability(Durability::Eventual);
			txn.delete_range(b"A".to_vec(), vec![b'a'; 27]).unwrap();
			txn.commit().await.unwrap();
			commit_range(&tree, 3..5, VALUE, Durability::Eventual).await;
			tree.abandon();
			world.fs.abandon();
			drop(tree);
			let world = match world.dir {
				Some(_) => world,
				None => World {
					fs: world.fs.simulate_crash(CrashMode::Prefix(usize::MAX)).unwrap(),
					..world
				},
			};
			let (segment_id, segment) = world.segment();
			assert_eq!(world.record_ends(&segment, segment_id).len(), 6, "{what}: a record each");

			for mode in [
				WalRecoveryMode::AbsoluteConsistency,
				WalRecoveryMode::TolerateTornTail,
				WalRecoveryMode::SalvageValidPrefix,
			] {
				let (_copy, opened) = opened_in(&world, mode, BIG_FILES).await;
				let tree = opened.unwrap_or_else(|e| panic!("{what}, {mode:?}: {e}"));
				let report = tree.recovery_report();
				assert_eq!(
					(report.records_dropped, report.bytes_trimmed, report.records_replayed),
					(0, 0, 6),
					"{what}, {mode:?}: {report:?}"
				);
				for (i, got) in read_range(&tree, 0..5) {
					assert_eq!(got, Some(value_of(i, VALUE)), "{what}, {mode:?}: key {i}");
				}
				tree.close().await.unwrap();
			}
		}
	})
	.await;
}

/// A log that was not damaged is not changed: nothing is cut, nothing is kept aside, and the
/// default mode opens what it always opened.
#[test(tokio::test)]
async fn a_log_whose_values_are_all_there_is_opened_as_it_was() {
	within("whole", async {
		for world in World::all() {
			let what = world.name;
			let world = world.after_a_crash(SMALL_FILES).await;
			for mode in [
				WalRecoveryMode::AbsoluteConsistency,
				WalRecoveryMode::TolerateTornTail,
				WalRecoveryMode::SalvageValidPrefix,
			] {
				let (copy, opened) = opened_in(&world, mode, SMALL_FILES).await;
				let tree = opened.unwrap_or_else(|e| panic!("{what}, {mode:?}: {e}"));
				assert_keys(&tree, COMMITS, what);
				let report = tree.recovery_report();
				assert_eq!(
					(report.records_dropped, report.bytes_trimmed, report.quarantined_files.len()),
					(0, 0, 0),
					"{what}, {mode:?}: {report:?}"
				);
				assert_eq!(report.records_replayed, COMMITS, "{what}, {mode:?}");
				assert!(
					copy.list("wal").iter().all(|name| !name.contains("corrupt")),
					"{what}, {mode:?}: nothing was kept aside"
				);
				tree.close().await.unwrap();
			}
		}
	})
	.await;
}

/// The two ends together: a log that is damaged where it ends, and a value log that lost its tail.
/// The log is repaired where it is torn, and the check holds what is left to the value log.
#[test(tokio::test)]
async fn a_torn_log_and_a_value_log_that_lost_its_tail_are_both_repaired() {
	within("both", async {
		for world in World::all() {
			let what = world.name;
			let world = world.after_a_crash(BIG_FILES).await;
			let entries = world.entries();
			let (segment_id, segment) = world.segment();
			let ends = world.record_ends(&segment, segment_id);
			// The value log lost the entries from the eighth on, and the log is torn in the
			// middle of its tenth record.
			let entry = &entries[7];
			world.truncate(&entry.file, (entry.offset + 5) as u64);
			world.truncate(&segment, (ends[8] + 3) as u64);

			let (_copy, opened) =
				opened_in(&world, WalRecoveryMode::TolerateTornTail, BIG_FILES).await;
			let tree = opened.unwrap_or_else(|e| panic!("{what}: {e}"));
			assert_keys(&tree, 7, what);
			let report = tree.recovery_report();
			assert_eq!(report.records_replayed, 7, "{what}: {report:?}");
			assert!(report.bytes_trimmed > 0 && !report.quarantined_files.is_empty(), "{what}");
			tree.close().await.unwrap();
		}
	})
	.await;
}

/// The check is made of the last segment of the log. A rotation of the log syncs the value log
/// before it ends a segment, so a value of an older segment is not looked for: the open does not
/// read the entries of the segments before the last, and leaves them as they are.
#[test(tokio::test)]
async fn only_the_last_segment_of_the_log_is_held_to_the_value_log() {
	within("older segments", async {
		for world in World::all() {
			let what = world.name;
			let tree = world.open(WalRecoveryMode::TolerateTornTail, BIG_FILES).await.unwrap();
			commit_range(&tree, 0..6, VALUE, Durability::Eventual).await;
			// The log moves on to a second segment, with the memtable of the first queued.
			tree.core.inner.rotate_memtable().unwrap();
			commit_range(&tree, 6..COMMITS, VALUE, Durability::Eventual).await;
			tree.abandon();
			world.fs.abandon();
			drop(tree);
			let world = match world.dir {
				Some(_) => world,
				None => World {
					fs: world.fs.simulate_crash(CrashMode::Prefix(usize::MAX)).unwrap(),
					..world
				},
			};
			let segments: Vec<String> = world
				.list("wal")
				.into_iter()
				.filter(|name| name.ends_with(".wal"))
				.filter(|name| !world.read(&format!("wal/{name}")).is_empty())
				.collect();
			assert_eq!(segments.len(), 2, "{what}: two segments hold the commits: {segments:?}");
			let entries = world.entries();
			assert_eq!(entries.len(), COMMITS, "{what}");

			// An entry of the first segment that is gone is not looked for.
			let older = world.fork();
			let entry = &entries[2];
			let mut bytes = older.read(&entry.file);
			let middle = entry.offset + entry.len / 2;
			bytes[middle..middle + 40].fill(0);
			older.write(&entry.file, &bytes);
			let tree = older
				.open(WalRecoveryMode::AbsoluteConsistency, BIG_FILES)
				.await
				.unwrap_or_else(|e| panic!("{what}: {e}"));
			let report = tree.recovery_report();
			assert_eq!((report.records_dropped, report.bytes_trimmed), (0, 0), "{what}");
			let txn = tree.begin_with_mode(crate::Mode::ReadOnly).unwrap();
			assert!(txn.get(super::fs_engine_tests::key_of(2)).is_err(), "{what}: read as it did");
			assert_eq!(
				txn.get(super::fs_engine_tests::key_of(3)).unwrap(),
				Some(value_of(3, VALUE)),
				"{what}"
			);
			drop(txn);
			tree.abandon();
			older.fs.abandon();

			// An entry of the last segment that is gone ends the log there, in the last segment.
			let (segment_id, segment) = world.segment();
			let ends = world.record_ends(&segment, segment_id);
			assert_eq!(ends.len(), 6, "{what}");
			let entry = &entries[8];
			let mut bytes = world.read(&entry.file);
			bytes.truncate(entry.offset + 10);
			world.write(&entry.file, &bytes);
			let tree = world
				.open(WalRecoveryMode::SalvageValidPrefix, BIG_FILES)
				.await
				.unwrap_or_else(|e| panic!("{what}: {e}"));
			let report = tree.recovery_report();
			assert_eq!(report.records_dropped, 4, "{what}: {report:?}");
			assert_keys(&tree, 8, what);
			tree.close().await.unwrap();
		}
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// What the open does not do
// ---------------------------------------------------------------------------------------------

/// What an open of each kind of state finds, to say what the open reads and syncs.
#[derive(Clone, Copy, Debug)]
enum Kind {
	/// A database that was closed: its log is empty.
	Closed,
	/// A database without a value log that was left without a close.
	WithoutValueLog,
	/// A database with a value log whose commits are all inline values.
	InlineValues,
	/// A database with a value log whose commits are all deletes.
	Deletes,
	/// A database that was opened and closed and never written to.
	Empty,
	/// A database with a value log whose commits are separated values: the control, which does
	/// read the entries and does sync what it keeps.
	Separated,
}

impl Kind {
	fn options(self, world: &World) -> Options {
		let mut options = world.options(WalRecoveryMode::TolerateTornTail, BIG_FILES);
		match self {
			Kind::WithoutValueLog => options.enable_vlog = false,
			Kind::InlineValues => options.vlog_value_threshold = 1 << 20,
			_ => {}
		}
		options
	}
}

async fn state(kind: Kind) -> World {
	let world = World::on_disk();
	let tree = TreeBuilder::with_options(kind.options(&world)).build_async().await.unwrap();
	match kind {
		Kind::Empty => {}
		Kind::Deletes => {
			for i in 0..COMMITS {
				let mut txn = tree.begin().unwrap();
				txn.set_durability(Durability::Eventual);
				txn.delete(super::fs_engine_tests::key_of(i)).unwrap();
				txn.commit().await.unwrap();
			}
		}
		_ => commit_range(&tree, 0..COMMITS, VALUE, Durability::Eventual).await,
	}
	match kind {
		Kind::Closed | Kind::Empty => tree.close().await.unwrap(),
		_ => tree.abandon(),
	}
	drop(tree);
	world
}

/// The calls that the open of `world` made of its files, in order, as the recording file system
/// writes them: the reads, the syncs of files and of directories.
async fn calls_of_the_open(world: &World, kind: Kind) -> Vec<String> {
	let recording = crate::sstable::recording_fs::RecordingFs::new();
	recording.record_reads();
	let fs = FileSystem(Arc::new(Arc::clone(&recording)) as Arc<dyn crate::vfs::Fs>);
	let options = kind.options(world).with_file_system(fs);
	let tree = TreeBuilder::with_options(options).build_async().await.unwrap();
	assert_eq!(tree.recovery_report().records_dropped, 0);
	tree.abandon();
	drop(tree);
	recording.entries()
}

/// The offset of a call of the form `read_at <name> <offset> <length>`.
fn offset_of(call: &str) -> u64 {
	call.split(' ').nth(2).unwrap().parse().unwrap()
}

fn calls_to<'a>(calls: &'a [String], prefix: &str, directory: &str) -> Vec<&'a String> {
	calls
		.iter()
		.filter(|call| call.starts_with(prefix) && call[prefix.len()..].starts_with(directory))
		.collect()
}

#[test(tokio::test)]
async fn an_open_that_has_no_value_to_look_for_reads_no_entry_and_syncs_only_what_it_replays() {
	within("not done", async {
		for kind in
			[Kind::Closed, Kind::WithoutValueLog, Kind::InlineValues, Kind::Deletes, Kind::Empty]
		{
			let world = state(kind).await;
			let calls = calls_of_the_open(&world, kind).await;

			// The value log is read at its start, for the header of its files, which every open
			// reads, and for nothing else.
			for read in calls_to(&calls, "read_at ", "vlog/") {
				assert_eq!(offset_of(read), 0, "{kind:?}: a read of the value log: {read}");
			}
			// What is synced by name is the segments of the log that the open replays, each once,
			// and nothing of the value log: no record points into it. No segment is synced through
			// a handle.
			let by_name = calls_to(&calls, "sync_file ", "");
			assert!(
				by_name.iter().all(|call| call.starts_with("sync_file wal/")),
				"{kind:?}: {calls:#?}"
			);
			let segments: std::collections::HashSet<_> = by_name.iter().collect();
			assert_eq!(segments.len(), by_name.len(), "{kind:?}: {calls:#?}");
			assert!(calls_to(&calls, "sync ", "wal/").is_empty(), "{kind:?}: {calls:#?}");
			// The newest file of the value log is synced once if there is one, as it always was.
			assert!(calls_to(&calls, "sync ", "vlog/").len() <= 1, "{kind:?}: {calls:#?}");
			assert!(calls_to(&calls, "sync_dir ", "vlog").len() <= 1, "{kind:?}: {calls:#?}");
		}
	})
	.await;
}

/// The control of the test above: when the segment that is recovered does point into the value log,
/// the open reads each entry and makes what it keeps durable, in the order that holds: the value
/// log before the log.
#[test(tokio::test)]
async fn an_open_that_keeps_values_reads_every_entry_once_and_syncs_the_value_log_before_the_log() {
	within("done", async {
		let world = state(Kind::Separated).await;
		let calls = calls_of_the_open(&world, Kind::Separated).await;

		let reads = calls_to(&calls, "read_at ", "vlog/");
		let entries = reads.iter().filter(|read| offset_of(read) > 0).count();
		assert_eq!(entries, COMMITS, "{calls:#?}");

		let value_log = calls.iter().position(|call| call.starts_with("sync vlog/")).unwrap();
		let log = calls.iter().position(|call| call.starts_with("sync_file wal/")).unwrap();
		assert!(value_log < log, "the value log is synced first: {calls:#?}");
		assert_eq!(calls_to(&calls, "sync_file ", "wal/").len(), 1, "{calls:#?}");
		assert_eq!(calls_to(&calls, "sync ", "vlog/").len(), 1, "once: {calls:#?}");
	})
	.await;
}
