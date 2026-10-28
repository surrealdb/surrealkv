//! The open over the images that a crash of the operating system leaves of Eventual commits with
//! the value log: the log and the value log written back in no order, in pieces, or not at all.
//!
//! The memory file system keeps the writes that were not synced and builds an image from any
//! subset of them (`CrashMode`). Every image is opened, every key is read, and what comes back is
//! a value or nothing, and the keys that come back are the first ones that were committed.

use test_log::test;

use super::fs_engine_followup_tests::{options_for, pool_over};
use super::fs_engine_tests::{commit_range, key_of, value_of, within};
use crate::vfs::MemRaw;
use crate::{CrashMode, Durability, Mode, Tree, TreeBuilder, WalRecoveryMode};

const COMMITS: usize = 120;
const VALUE: usize = 300;

fn options(
	fs: &crate::FileSystem,
	mode: WalRecoveryMode,
	vlog_file_size: Option<u64>,
) -> crate::Options {
	let mut options = options_for(fs, true);
	options.vlog_value_threshold = 64;
	options.vlog_checksum_verification = crate::VLogChecksumLevel::Full;
	if let Some(size) = vlog_file_size {
		options.vlog_max_file_size = size;
	}
	options.wal_recovery_mode = mode;
	options
}

/// A database with `COMMITS` Eventual commits, one key each, which was left without a close.
async fn crashed(vlog_file_size: Option<u64>) -> MemRaw {
	let raw = MemRaw::new();
	let (fs, _pool) = pool_over(&raw);
	let tree =
		TreeBuilder::with_options(options(&fs, WalRecoveryMode::TolerateTornTail, vlog_file_size))
			.build_async()
			.await
			.unwrap();
	commit_range(&tree, 0..COMMITS, VALUE, Durability::Eventual).await;
	tree.abandon();
	fs.abandon();
	drop(tree);
	raw
}

/// The images of a crash, which lose, tear and reorder what was not synced.
fn images(raw: &MemRaw) -> Vec<(String, MemRaw)> {
	let mut images = Vec::new();
	for lost in 0..raw.unflushed_ops().min(64) {
		images.push((
			format!("LoseTear lost {lost}"),
			raw.crash(CrashMode::LoseTear {
				lost,
				torn: usize::MAX,
				n: 0,
			}),
		));
	}
	for seed in 0..64 {
		images.push((format!("Reorder({seed})"), raw.crash(CrashMode::Reorder(seed))));
	}
	images.push(("LoseUnsynced".to_string(), raw.crash(CrashMode::LoseUnsynced)));
	images.push(("ZeroTail".to_string(), raw.crash(CrashMode::ZeroTail)));
	images
}

/// How many of the keys `range` hold a value, and the first one that holds anything else than the
/// value it was written with or nothing.
fn read(tree: &Tree, range: std::ops::Range<usize>) -> (usize, Option<String>) {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let mut present = 0;
	for i in range {
		match txn.get(key_of(i)) {
			Ok(Some(value)) if value == value_of(i, VALUE) => present += 1,
			Ok(None) => {}
			other => return (present, Some(format!("key {i} reads as {other:?}"))),
		}
	}
	(present, None)
}

/// Whether the keys of `range` that are there are its first ones.
fn is_a_prefix(tree: &Tree, range: std::ops::Range<usize>) -> bool {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let present: Vec<bool> = range.map(|i| txn.get(key_of(i)).unwrap().is_some()).collect();
	let first_missing = present.iter().position(|there| !there).unwrap_or(present.len());
	present[first_missing..].iter().all(|there| !there)
}

#[test(tokio::test)]
async fn every_image_opens_in_salvage_mode_and_its_keys_are_values_or_nothing_in_commit_order() {
	within("salvage", async {
		for vlog_file_size in [None, Some(2_000)] {
			let raw = crashed(vlog_file_size).await;
			let images = images(&raw);
			let total = images.len();
			let mut cut = 0;
			let mut kept_all = 0;
			for (what, image) in images {
				let what = format!("{what}, files of {vlog_file_size:?}");
				let (fs, _pool) = pool_over(&image);
				let tree = TreeBuilder::with_options(options(
					&fs,
					WalRecoveryMode::SalvageValidPrefix,
					vlog_file_size,
				))
				.build_async()
				.await
				.unwrap_or_else(|e| panic!("{what}: does not open: {e}"));
				let (present, damage) = read(&tree, 0..COMMITS);
				assert_eq!(damage, None, "{what}");
				assert!(
					is_a_prefix(&tree, 0..COMMITS),
					"{what}: {present} keys that are not a prefix"
				);
				let report = tree.recovery_report();
				if report.records_dropped > 0 {
					cut += 1;
				}
				if present == COMMITS {
					kept_all += 1;
				}
				// The log that is left is what the open replayed: every record that it kept.
				assert!(report.records_replayed >= present, "{what}: {report:?}");
				tree.abandon();
			}
			assert!(
				cut > total / 4,
				"{cut} of {total} images were cut: the check is not what the run tests"
			);
			assert!(kept_all < total, "an image lost nothing at {vlog_file_size:?}: {kept_all}");
		}
	})
	.await;
}

/// Nothing in these images was synced, so the default mode finds no hole that a sync mark says
/// was synced, nor a segment whose damage may have been synced: every image opens, with the values
/// of a prefix of the commits and nothing else. That holds when the crash took the first page of
/// the segment, and the leading mark with it.
#[test(tokio::test)]
async fn every_image_opens_in_the_default_mode_with_values_or_nothing() {
	within("tolerate", async {
		for vlog_file_size in [None, Some(2_000)] {
			let raw = crashed(vlog_file_size).await;
			for (what, image) in images(&raw) {
				let what = format!("{what}, files of {vlog_file_size:?}");
				let (fs, _pool) = pool_over(&image);
				let tree = TreeBuilder::with_options(options(
					&fs,
					WalRecoveryMode::TolerateTornTail,
					vlog_file_size,
				))
				.build_async()
				.await
				.unwrap_or_else(|error| panic!("{what}: {error}"));
				let (present, damage) = read(&tree, 0..COMMITS);
				assert_eq!(damage, None, "{what}");
				assert!(
					is_a_prefix(&tree, 0..COMMITS),
					"{what}: {present} keys that are not a prefix"
				);
				tree.abandon();
			}
		}
	})
	.await;
}

/// Commits in the mix below, and which of them waited for a sync: every 53rd, so the last is the
/// 159th. Every commit up to and including it is acknowledged as durable.
const MIXED: usize = 200;
const LAST_ACKNOWLEDGED: usize = 158;

/// A database of `MIXED` commits, mostly Eventual and some Immediate, over files of the value log
/// that hold a dozen entries each, which was left without a close.
async fn crashed_with_acknowledged_commits() -> MemRaw {
	let raw = MemRaw::new();
	let (fs, _pool) = pool_over(&raw);
	let tree =
		TreeBuilder::with_options(options(&fs, WalRecoveryMode::TolerateTornTail, Some(4_096)))
			.build_async()
			.await
			.unwrap();
	for i in 0..MIXED {
		let durability = if i % 53 == 52 {
			Durability::Immediate
		} else {
			Durability::Eventual
		};
		commit_range(&tree, i..i + 1, VALUE, durability).await;
	}
	tree.abandon();
	fs.abandon();
	drop(tree);
	raw
}

/// Whichever file of a value log of many files lost its first page, and whatever else a crash took
/// of what was not synced, every image opens in the mode that salvages and in the default mode, and
/// every commit that was acknowledged is there. A crash loses only what was not synced, so the
/// default mode finds no hole that a sync mark says was synced, and refuses none.
#[test(tokio::test)]
async fn every_image_of_a_value_log_of_many_files_keeps_every_acknowledged_commit() {
	within("many files", async {
		let raw = crashed_with_acknowledged_commits().await;
		let images = images(&raw);
		let total = images.len();
		let mut cut = 0;
		for (what, image) in images {
			for mode in [WalRecoveryMode::SalvageValidPrefix, WalRecoveryMode::TolerateTornTail] {
				let what = format!("{what}, {mode:?}");
				// A copy, that the open of one mode does not change for the next.
				let (fs, _pool) = pool_over(&image.crash(CrashMode::Prefix(usize::MAX)));
				let opened =
					TreeBuilder::with_options(options(&fs, mode, Some(4_096))).build_async().await;
				let tree = opened.unwrap_or_else(|e| panic!("{what}: {e}"));
				let (present, damage) = read(&tree, 0..MIXED);
				assert_eq!(damage, None, "{what}");
				assert!(
					is_a_prefix(&tree, 0..MIXED),
					"{what}: {present} keys that are not a prefix"
				);
				assert!(
					present > LAST_ACKNOWLEDGED,
					"{what}: {present} keys, and the first {} were acknowledged",
					LAST_ACKNOWLEDGED + 1
				);
				if mode == WalRecoveryMode::SalvageValidPrefix
					&& tree.recovery_report().records_dropped > 0
				{
					cut += 1;
				}
				tree.abandon();
			}
		}
		assert!(
			cut > total / 4,
			"{cut} of {total} images were cut: the check is not what is tested"
		);
	})
	.await;
}

/// The images that only the value log is cut in: the log is whole, and the value log ends before
/// the end of what the log points to. The default mode opens every one of them, with the keys
/// whose entries are whole.
#[test(tokio::test)]
async fn an_image_whose_value_log_alone_lost_its_tail_opens_in_the_default_mode() {
	within("cut value log", async {
		let raw = crashed(None).await;
		let whole = raw.crash(CrashMode::Prefix(usize::MAX));
		let (fs, _pool) = pool_over(&whole);
		let vlog = fs
			.list_for_test("db/vlog")
			.into_iter()
			.find(|name| name.ends_with(".vlog"))
			.map(|name| format!("db/vlog/{name}"))
			.expect("the commits are in a file of the value log");
		let len = fs.file_len_for_test(&vlog).unwrap();
		// The header and an entry are 31 and 330 bytes: every cut from the middle of the header
		// to the end, at a spread of places that include the ends of the entries.
		let mut cuts: Vec<u64> = (0..=len).step_by(97).collect();
		cuts.extend((0..COMMITS as u64).map(|i| 31 + i * 330));
		cuts.extend((1..COMMITS as u64).map(|i| 31 + i * 330 - 1));
		cuts.push(len);
		for cut in cuts {
			let image = whole.crash(CrashMode::Prefix(usize::MAX));
			let (fs, _pool) = pool_over(&image);
			fs.raw_truncate_for_test(&vlog, cut).unwrap();
			let tree =
				TreeBuilder::with_options(options(&fs, WalRecoveryMode::TolerateTornTail, None))
					.build_async()
					.await
					.unwrap_or_else(|e| panic!("cut at {cut}: {e}"));
			let (present, damage) = read(&tree, 0..COMMITS);
			assert_eq!(damage, None, "cut at {cut}");
			// The entries that are whole: an entry is 330 bytes after a header of 31.
			let whole_entries = (cut.saturating_sub(31) / 330) as usize;
			assert_eq!(present, whole_entries.min(COMMITS), "cut at {cut}");
			assert!(is_a_prefix(&tree, 0..COMMITS), "cut at {cut}");
			tree.abandon();
		}
	})
	.await;
}

/// The crash of the operating system that follows a crash of the process. The open that follows
/// the first has made what it recovered durable, as the next open only looks at the last segment:
/// every commit that it kept is still there after the second, and so is every commit that was
/// acknowledged after the open.
#[test(tokio::test)]
async fn what_an_open_recovered_survives_a_second_crash_of_the_operating_system() {
	within("double fault", async {
		for vlog_file_size in [None, Some(2_000)] {
			// The process is gone, and the page cache holds every write: the same memory is
			// opened again, with what was not synced still not synced.
			let raw = crashed(vlog_file_size).await;
			let (fs, pool) = pool_over(&raw);
			let _ = pool;
			let tree = TreeBuilder::with_options(options(
				&fs,
				WalRecoveryMode::TolerateTornTail,
				vlog_file_size,
			))
			.build_async()
			.await
			.unwrap();
			let (kept, damage) = read(&tree, 0..COMMITS);
			assert_eq!((kept, damage), (COMMITS, None), "the process crash lost nothing");
			// What is acknowledged after the open is synced by the commits that ask for it.
			commit_range(&tree, 1000..1010, VALUE, Durability::Immediate).await;
			tree.abandon();
			drop(tree);

			let mut modes = vec![CrashMode::LoseUnsynced, CrashMode::ZeroTail];
			modes.extend((0..24).map(|lost| CrashMode::LoseTear {
				lost,
				torn: usize::MAX,
				n: 0,
			}));
			modes.extend((0..24).map(CrashMode::Reorder));
			for mode in modes {
				let what = format!("{mode:?}, files of {vlog_file_size:?}");
				let image = raw.crash(mode);
				let (fs, _pool) = pool_over(&image);
				let tree = TreeBuilder::with_options(options(
					&fs,
					WalRecoveryMode::AbsoluteConsistency,
					vlog_file_size,
				))
				.build_async()
				.await
				.unwrap_or_else(|e| panic!("{what}: does not open: {e}"));
				assert_eq!(read(&tree, 0..COMMITS), (COMMITS, None), "{what}: what the open kept");
				assert_eq!(read(&tree, 1000..1010), (10, None), "{what}: what was acknowledged");
				tree.abandon();
			}
		}
	})
	.await;
}

/// The same, for a first crash that damaged the log: the open cuts it, more commits are
/// acknowledged, and a second crash follows. What the open kept and what was acknowledged are
/// there, and the strictest mode opens the result.
#[test(tokio::test)]
async fn what_a_cut_kept_survives_a_second_crash_of_the_operating_system() {
	within("cut and double fault", async {
		for vlog_file_size in [None, Some(2_000)] {
			let raw = crashed(vlog_file_size).await;
			let first: Vec<_> = (0..16)
				.map(|seed| (format!("Reorder({seed})"), raw.crash(CrashMode::Reorder(seed))))
				.chain((0..8).map(|lost| {
					(
						format!("LoseTear lost {lost}"),
						raw.crash(CrashMode::LoseTear {
							lost,
							torn: usize::MAX,
							n: 0,
						}),
					)
				}))
				.collect();
			for (what, image) in first {
				let what = format!("{what}, files of {vlog_file_size:?}");
				let (fs, _pool) = pool_over(&image);
				let tree = TreeBuilder::with_options(options(
					&fs,
					WalRecoveryMode::SalvageValidPrefix,
					vlog_file_size,
				))
				.build_async()
				.await
				.unwrap_or_else(|e| panic!("{what}: does not open: {e}"));
				let (kept, damage) = read(&tree, 0..COMMITS);
				assert_eq!(damage, None, "{what}");
				commit_range(&tree, 1000..1010, VALUE, Durability::Immediate).await;
				tree.abandon();
				drop(tree);

				for mode in [
					CrashMode::LoseUnsynced,
					CrashMode::ZeroTail,
					CrashMode::LoseTear {
						lost: 3,
						torn: usize::MAX,
						n: 0,
					},
					CrashMode::Reorder(7),
				] {
					let what = format!("{what}, then {mode:?}");
					let second = image.crash(mode);
					let (fs, _pool) = pool_over(&second);
					let tree = TreeBuilder::with_options(options(
						&fs,
						WalRecoveryMode::AbsoluteConsistency,
						vlog_file_size,
					))
					.build_async()
					.await
					.unwrap_or_else(|e| panic!("{what}: does not open: {e}"));
					assert_eq!(read(&tree, 0..COMMITS), (kept, None), "{what}: what the cut kept");
					assert_eq!(
						read(&tree, 1000..1010),
						(10, None),
						"{what}: what was acknowledged"
					);
					tree.abandon();
				}
			}
		}
	})
	.await;
}

/// The same, for a value log that is cut after a crash of the process, so that what is left of the
/// older files of the value log and of the log was never synced: the open cuts the log, and makes
/// what it kept durable, which a second crash of the operating system then finds.
#[test(tokio::test)]
async fn what_a_cut_kept_after_a_crash_of_the_process_survives_a_crash_of_the_operating_system() {
	within("process crash, cut, and double fault", async {
		for vlog_file_size in [None, Some(2_000)] {
			let raw = crashed(vlog_file_size).await;
			let (fs, _pool) = pool_over(&raw);
			let newest = fs
				.list_for_test("db/vlog")
				.into_iter()
				.filter(|name| name.ends_with(".vlog"))
				.map(|name| format!("db/vlog/{name}"))
				.next_back()
				.expect("the commits are in files of the value log");
			// The newest file ends in the middle of an entry, without a flush of the cut.
			let len = fs.file_len_for_test(&newest).unwrap();
			fs.raw_truncate_for_test(&newest, 31 + (len - 31) / 330 / 2 * 330 + 100).unwrap();

			let tree = TreeBuilder::with_options(options(
				&fs,
				WalRecoveryMode::SalvageValidPrefix,
				vlog_file_size,
			))
			.build_async()
			.await
			.unwrap();
			let (kept, damage) = read(&tree, 0..COMMITS);
			assert_eq!(damage, None, "files of {vlog_file_size:?}");
			assert!(kept > 0 && kept < COMMITS, "{kept} of {COMMITS}: the log was cut");
			commit_range(&tree, 1000..1010, VALUE, Durability::Immediate).await;
			tree.abandon();
			fs.abandon();
			drop(tree);

			let mut modes = vec![CrashMode::LoseUnsynced, CrashMode::ZeroTail];
			modes.extend((0..12).map(CrashMode::Reorder));
			modes.extend((0..12).map(|lost| CrashMode::LoseTear {
				lost,
				torn: usize::MAX,
				n: 0,
			}));
			for mode in modes {
				let what = format!("{mode:?}, files of {vlog_file_size:?}");
				let image = raw.crash(mode);
				let (fs, _pool) = pool_over(&image);
				let tree = TreeBuilder::with_options(options(
					&fs,
					WalRecoveryMode::AbsoluteConsistency,
					vlog_file_size,
				))
				.build_async()
				.await
				.unwrap_or_else(|e| panic!("{what}: does not open: {e}"));
				assert_eq!(read(&tree, 0..COMMITS), (kept, None), "{what}: what the cut kept");
				assert_eq!(read(&tree, 1000..1010), (10, None), "{what}: what was acknowledged");
				tree.abandon();
			}
		}
	})
	.await;
}
