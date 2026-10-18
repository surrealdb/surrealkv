//! The crash safety of the slots of a pool that are cleaned and reused, at every operation of the
//! backend and in every crash that the memory backend can make.
//!
//! The header pages of a slot are the record of what the slot is, and a flush is the only barrier
//! between two operations on its file. So whatever a clean or a mount does to a dead slot has to
//! leave, in every subset of the operations that were not flushed (with any write of them torn),
//! a slot that a mount reads as one of two things: the file that it was, whole, or no file. These
//! tests look for what that rules out:
//!
//! * a slot that is named and has lost its bytes (a removed file that comes back empty);
//! * a name that is gone and comes back (the older header of a slot that was renamed);
//! * a slot that is reused and shows the bytes that it had before, after the end of the new file (a
//!   cut that was lost in a crash that kept the writes after it);
//! * a pool that a mount does not read, or reads with an orphan.
//!
//! Each scenario runs once for every point at which the backend can be killed, and the state it is
//! left in is crashed in the modes of [`CrashMode`] and in every subset of the unflushed
//! operations: the modes are a sample of the subsets, and the subsets are what a device may do.
//! The last tests ask the same of the engine: a database whose recovery is killed at every
//! operation, in a pool that has slots that hold the bytes of files that were removed.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use test_log::test;

use crate::vfs::header::{Header, SlotHeaders, SlotState, HEADER_LEN};
use crate::vfs::mem::Fate;
use crate::vfs::pool::parse_slot_name;
use crate::vfs::{CreateOpts, Fs, FsFile, MemRaw, PoolConfig, PoolFs};
use crate::CrashMode;

type Mem = PoolFs<MemRaw>;

fn p(path: &str) -> &Path {
	Path::new(path)
}

/// What a dead slot held: easy to see in any slot that shows it again.
const STALE: u8 = 0xEE;

fn memory_pool() -> (Arc<Mem>, MemRaw) {
	let raw = MemRaw::new();
	let (fs, _) = PoolFs::mount_sync(raw.clone(), PoolConfig::memory()).expect("mount");
	fs.create_dir_all(p("/d")).unwrap();
	(fs, raw)
}

/// A pool that is mounted over `raw`, as a mount after a crash is.
fn remounted(raw: &MemRaw) -> (Arc<Mem>, MemRaw) {
	let (fs, _) = PoolFs::mount_sync(raw.clone(), PoolConfig::memory()).expect("mount");
	fs.create_dir_all(p("/d")).unwrap();
	(fs, raw.clone())
}

/// Makes a file with this content, synced.
fn put(fs: &Mem, path: &str, content: &[u8]) -> Arc<dyn FsFile> {
	let file = fs.create_truncate(p(path), CreateOpts::default()).expect("create_truncate");
	file.append(content).expect("append");
	file.sync().expect("sync");
	file
}

fn read(fs: &Mem, path: &str) -> Option<Vec<u8>> {
	fs.read(p(path)).ok()
}

/// The names in `/d`, sorted.
fn listed(fs: &Mem) -> Vec<String> {
	let mut names: Vec<String> = fs
		.read_dir(p("/d"))
		.unwrap()
		.into_iter()
		.map(|entry| entry.name.to_string_lossy().into_owned())
		.collect();
	names.sort();
	names
}

/// What must hold of the slots of any pool once a mount has settled them: no path is named by two
/// slots, and a slot that is free has nothing after its headers.
fn assert_sound(raw: &MemRaw, context: &str) {
	let mut named: BTreeMap<String, String> = BTreeMap::new();
	for name in raw.names() {
		if parse_slot_name(&name).is_none() {
			continue;
		}
		let image = raw.image(&name).unwrap();
		if (image.len() as u64) < HEADER_LEN {
			continue;
		}
		match SlotHeaders::decode(&image).current {
			Some((
				_,
				Header {
					state: SlotState::Named(path),
					..
				},
			)) => {
				if let Some(other) = named.insert(path.clone(), name.clone()) {
					panic!("{context}: {path} is named by {other} and {name}");
				}
			}
			Some((
				_,
				Header {
					state: SlotState::Free,
					..
				},
			)) => {
				assert_eq!(
					image.len() as u64,
					HEADER_LEN,
					"{context}: the free slot {name} has a payload"
				);
			}
			None => {}
		}
	}
}

/// The crashes of the modes that the backend has, over the operations that were not flushed.
fn modes(raw: &MemRaw) -> Vec<CrashMode> {
	let pending = raw.unflushed_ops();
	let mut modes = vec![CrashMode::LoseUnsynced, CrashMode::ZeroTail];
	modes.extend((0..=pending).map(CrashMode::Prefix));
	for k in 0..pending {
		for bytes in [1usize, 17, 100, 2048, 4095] {
			modes.push(CrashMode::Tear(k, bytes));
		}
	}
	for lost in 0..pending {
		for torn in 0..pending {
			for n in [1usize, 17, 4095] {
				modes.push(CrashMode::LoseTear {
					lost,
					torn,
					n,
				});
			}
		}
	}
	modes.extend((0..64).map(CrashMode::Reorder));
	modes
}

/// The most operations that are crashed in every subset: three fates for each write.
const MOST_SUBSET_OPS: usize = 8;

/// Every crash of the operations that were not flushed, as the fates of the operations: each is
/// lost or whole, and a write is also torn so that a header page is not valid.
fn subsets(raw: &MemRaw) -> Vec<Vec<Fate>> {
	let shape = raw.unflushed_shape();
	assert!(shape.len() <= MOST_SUBSET_OPS, "{} operations are not flushed", shape.len());
	let mut all: Vec<Vec<Fate>> = vec![Vec::new()];
	for (_, write) in &shape {
		let fates: &[Fate] = if write.is_some() {
			&[Fate::Lost, Fate::Kept, Fate::Torn(17)]
		} else {
			&[Fate::Lost, Fate::Kept]
		};
		all = all
			.into_iter()
			.flat_map(|prefix| {
				fates.iter().map(move |fate| {
					let mut next = prefix.clone();
					next.push(*fate);
					next
				})
			})
			.collect();
	}
	all
}

/// Every crash image of `raw`: the modes, then every subset.
fn images(raw: &MemRaw) -> Vec<(String, MemRaw)> {
	let mut images: Vec<(String, MemRaw)> =
		modes(raw).into_iter().map(|mode| (format!("{mode:?}"), raw.crash(mode))).collect();
	for fates in subsets(raw) {
		let image = raw.crash_by(&|index| fates[index]);
		images.push((format!("{fates:?}"), image));
	}
	images
}

type Check<'a> = &'a dyn Fn(&Arc<Mem>, &[&'static str], &str);

/// Mounts every crash image of `raw`, which has to work and leave a sound pool with no orphan,
/// and gives the pool to `check`. Returns how many images there were.
fn check_images(raw: &MemRaw, acks: &[&'static str], origin: &str, check: Check<'_>) -> usize {
	let mut checked = 0;
	for (how, image) in images(raw) {
		let context = format!("{origin}, {how}, acked {acks:?}");
		let (mounted, report) = PoolFs::mount_sync(image.clone(), PoolConfig::memory())
			.unwrap_or_else(|e| panic!("{context}: a mount is never refused: {e}"));
		assert_sound(&image, &context);
		assert!(
			report.orphans_kept.is_empty(),
			"{context}: the mount kept orphans: {:?}",
			report.orphans_kept
		);
		// The directories are made again, as the engine does at every open.
		mounted.create_dir_all(p("/d")).unwrap();
		check(&mounted, acks, &context);
		checked += 1;
	}
	checked
}

struct Scenario<'a> {
	/// A pool in the state before what is tested, and its memory.
	prepare: &'a dyn Fn() -> (Arc<Mem>, MemRaw),
	/// What is tested. It runs until the backend is killed, and says what it was told was done.
	run: &'a dyn Fn(&Arc<Mem>) -> Vec<&'static str>,
	/// What every mount of every crash image has to satisfy: the pool, what `run` acknowledged,
	/// and where the image came from.
	check: Check<'a>,
}

/// Runs the scenario once for every point where the backend can be killed, mounts every crash
/// image of what is left, and checks it. Returns how many images were checked.
fn enumerate(scenario: &Scenario<'_>) -> usize {
	let (fs, raw) = (scenario.prepare)();
	let before = raw.ops();
	let _ = (scenario.run)(&fs);
	let total = raw.ops() - before;
	drop(fs);
	let mut checked = 0;
	for n in 0..=total {
		let (fs, raw) = (scenario.prepare)();
		raw.die_after(n);
		let acks = (scenario.run)(&fs);
		let origin = format!("killed after {n} of {total} operations");
		checked += check_images(&raw, &acks, &origin, scenario.check);
	}
	checked
}

/// The same for a mount: `image` is mounted, killed at every operation of the mount, and the
/// crash images of what the mount left are mounted and checked.
fn enumerate_mount(image: &dyn Fn() -> MemRaw, check: Check<'_>) -> usize {
	let probe = image();
	let before = probe.ops();
	drop(PoolFs::mount_sync(probe.clone(), PoolConfig::memory()).expect("mount"));
	let total = probe.ops() - before;
	let mut checked = 0;
	for n in 0..=total {
		let raw = image();
		raw.die_after(n);
		let _ = PoolFs::mount_sync(raw.clone(), PoolConfig::memory());
		let origin = format!("a mount killed after {n} of {total} operations");
		checked += check_images(&raw, &[], &origin, check);
	}
	checked
}

/// A removed file is whole or gone: never a file with the name that it had and not its bytes.
fn whole_or_gone(fs: &Mem, name: &str, whole: &[u8], context: &str) {
	if let Some(content) = read(fs, &format!("/d/{name}")) {
		assert_eq!(
			content.len(),
			whole.len(),
			"{context}: /d/{name} came back with {} bytes, and was {}",
			content.len(),
			whole.len()
		);
		assert!(content == whole, "{context}: /d/{name} came back, and is not what it was");
	}
}

/// A file that was made in a slot that held `STALE` bytes shows none of them.
fn shows_nothing_of_the_slot(fs: &Mem, name: &str, context: &str) {
	if let Some(content) = read(fs, &format!("/d/{name}")) {
		assert!(
			!content.contains(&STALE),
			"{context}: /d/{name} shows the old bytes of its slot ({} bytes)",
			content.len()
		);
	}
}

fn clean_all(fs: &Arc<Mem>) -> Vec<&'static str> {
	let mut acks = Vec::new();
	if fs.maintain(8) >= 1 {
		acks.push("cleaned");
	}
	acks
}

// ---- a clean ---------------------------------------------------------------------------------

/// The files that live in the pool and are not touched by what a test does: what they hold, so
/// that every crash image can be checked to hold it still, byte for byte.
fn bystanders() -> Vec<(&'static str, Vec<u8>)> {
	let long: Vec<u8> = (0..6000u32).map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8).collect();
	vec![("keep", b"keep".to_vec()), ("long", long), ("empty", Vec::new())]
}

fn put_bystanders(fs: &Mem) {
	for (name, content) in bystanders() {
		put(fs, &format!("/d/{name}"), &content);
	}
}

fn bystanders_are_intact(fs: &Mem, context: &str) {
	for (name, content) in bystanders() {
		assert_eq!(
			read(fs, &format!("/d/{name}")).as_deref(),
			Some(&content[..]),
			"{context}: /d/{name} is not what it was"
		);
	}
}

#[test]
fn a_slot_that_is_cleaned_is_the_file_it_was_whole_or_no_file_in_every_crash() {
	let whole = vec![STALE; 300];
	let checked = enumerate(&Scenario {
		prepare: &|| {
			let (fs, raw) = memory_pool();
			put(&fs, "/d/a", &[STALE; 300]);
			put_bystanders(&fs);
			fs.remove_file(p("/d/a")).unwrap();
			(fs, raw)
		},
		run: &|fs| clean_all(fs),
		check: &|fs, acks, context| {
			bystanders_are_intact(fs, context);
			whole_or_gone(fs, "a", &whole, context);
			if acks.contains(&"cleaned") {
				assert_eq!(read(fs, "/d/a"), None, "{context}: a clean that returned is durable");
			}
			let others: Vec<String> = listed(fs).into_iter().filter(|name| name != "a").collect();
			assert_eq!(others, ["empty", "keep", "long"], "{context}");
		},
	});
	assert!(checked > 300, "{checked}");
}

#[test]
fn a_slot_that_was_renamed_does_not_bring_its_old_name_back_when_it_is_cleaned() {
	// The slot holds two headers that name a file: the older is the name before the rename. The
	// clean must not leave a crash that tears the page that counts with the older one in charge.
	let whole = vec![STALE; 300];
	let checked = enumerate(&Scenario {
		prepare: &|| {
			let (fs, raw) = memory_pool();
			put(&fs, "/d/before", &[STALE; 300]);
			put(&fs, "/d/keep", b"keep");
			fs.rename(p("/d/before"), p("/d/after")).unwrap();
			fs.remove_file(p("/d/after")).unwrap();
			(fs, raw)
		},
		run: &|fs| clean_all(fs),
		check: &|fs, acks, context| {
			assert_eq!(read(fs, "/d/keep").as_deref(), Some(&b"keep"[..]), "{context}");
			assert_eq!(
				read(fs, "/d/before"),
				None,
				"{context}: a name that was renamed away is back"
			);
			whole_or_gone(fs, "after", &whole, context);
			if acks.contains(&"cleaned") {
				assert_eq!(
					read(fs, "/d/after"),
					None,
					"{context}: a clean that returned is durable"
				);
			}
		},
	});
	assert!(checked > 300, "{checked}");
}

#[test]
fn a_removed_file_with_an_unsynced_tail_comes_back_with_its_synced_part_or_not_at_all() {
	let checked = enumerate(&Scenario {
		prepare: &|| {
			let (fs, raw) = memory_pool();
			let file = put(&fs, "/d/t", &[STALE; 300]);
			file.append(&[STALE; 100]).unwrap();
			put(&fs, "/d/keep", b"keep");
			fs.remove_file(p("/d/t")).unwrap();
			drop(file);
			(fs, raw)
		},
		run: &|fs| clean_all(fs),
		check: &|fs, _, context| {
			assert_eq!(read(fs, "/d/keep").as_deref(), Some(&b"keep"[..]), "{context}");
			if let Some(content) = read(fs, "/d/t") {
				// The tail that was not synced may be there whole, torn, or as zeros (a length that
				// was kept and bytes that were not): the 300 that were synced are all there.
				assert!(
					content.len() >= 300 && content[..300].iter().all(|byte| *byte == STALE),
					"{context}: /d/t came back with {} bytes, and 300 of them were synced",
					content.len()
				);
			}
		},
	});
	assert!(checked > 300, "{checked}");
}

#[test]
fn the_older_manifest_that_a_mount_cleans_is_never_the_one_that_is_read() {
	// A crash that lost the free header of the old manifest leaves two slots that name it, and
	// the mount cleans the one with the lower sequence number: the crash images of that mount are
	// mounted too.
	let checked = enumerate_mount(
		&|| {
			let (fs, raw) = memory_pool();
			fs.replace_atomic(p("/d/MANIFEST"), &[STALE; 200]).unwrap();
			fs.replace_atomic(p("/d/MANIFEST"), &[b'N'; 120]).unwrap();
			fs.pool().unwrap().abandon();
			drop(fs);
			let image = raw.crash(CrashMode::LoseUnsynced);
			let (probe, report) =
				PoolFs::mount_sync(image.snapshot(), PoolConfig::memory()).unwrap();
			assert_eq!(report.losers_cleaned, 1, "the old manifest is a loser");
			drop(probe);
			image
		},
		&|fs, _, context| {
			assert_eq!(
				read(fs, "/d/MANIFEST").as_deref(),
				Some(&[b'N'; 120][..]),
				"{context}: the manifest is the newer one"
			);
			assert_eq!(listed(fs), vec!["MANIFEST".to_string()], "{context}");
		},
	);
	assert!(checked > 40, "{checked}");
}

// ---- a reuse ---------------------------------------------------------------------------------

/// A pool whose only slot held a file of `STALE` bytes that was removed, and is cleaned already
/// or dead, as the test says.
fn reused_after(maintained: bool) -> (Arc<Mem>, MemRaw) {
	let (fs, raw) = memory_pool();
	put(&fs, "/d/old", &[STALE; 5000]);
	fs.remove_file(p("/d/old")).unwrap();
	if maintained {
		assert_eq!(fs.maintain(8), 1);
		assert_eq!(fs.stats().clean, 1);
	} else {
		assert_eq!(fs.stats().dirty, 1);
	}
	(fs, raw)
}

fn named_and_written(fs: &Arc<Mem>) -> Vec<&'static str> {
	let mut acks = Vec::new();
	let Ok(b) = fs.create_new(p("/d/b"), CreateOpts::default()) else {
		return acks;
	};
	if b.append(b"new bytes").is_err() {
		return acks;
	}
	acks.push("appended");
	if b.sync().is_err() {
		return acks;
	}
	acks.push("synced");
	acks
}

fn nothing_of_the_slot_and_what_was_synced(fs: &Arc<Mem>, acks: &[&'static str], context: &str) {
	shows_nothing_of_the_slot(fs, "b", context);
	whole_or_gone(fs, "old", &[STALE; 5000], context);
	if acks.contains(&"synced") {
		assert_eq!(read(fs, "/d/b").as_deref(), Some(&b"new bytes"[..]), "{context}");
	}
}

#[test]
fn a_claim_that_cleans_a_dead_slot_for_itself_shows_nothing_of_what_it_held() {
	let checked = enumerate(&Scenario {
		prepare: &|| reused_after(false),
		run: &named_and_written,
		check: &nothing_of_the_slot_and_what_was_synced,
	});
	assert!(checked > 600, "{checked}");
}

#[test]
fn a_slot_that_was_cleaned_and_is_named_shows_nothing_of_what_it_held() {
	let checked = enumerate(&Scenario {
		prepare: &|| reused_after(true),
		run: &named_and_written,
		check: &nothing_of_the_slot_and_what_was_synced,
	});
	assert!(checked > 200, "{checked}");
}

#[test]
fn a_header_that_landed_whole_and_was_reported_as_failed_is_not_left_to_a_crash_by_the_clean() {
	// A creation whose header write fails after the whole page landed, and whose free header
	// fails too: the page that does not count holds a name, with a higher sequence number than
	// the free header that counts. The clean has to put its own free header over it, and
	// flush it, or a crash that keeps the page and then loses the next name of the slot brings
	// the file back, with the bytes of another.
	let page = crate::vfs::header::PAGE_LEN;
	let checked = enumerate(&Scenario {
		prepare: &|| {
			let (fs, raw) = reused_after(true);
			raw.fail_write_after(1);
			raw.write_then_fail(page);
			assert!(fs.create_new(p("/d/ghost"), CreateOpts::default()).is_err());
			raw.clear_faults();
			assert_eq!(fs.stats().dirty, 1, "the slot was given back, and is dead");
			(fs, raw)
		},
		run: &|fs| {
			let mut acks = clean_all(fs);
			acks.extend(named_and_written(fs));
			acks
		},
		check: &|fs, acks, context| {
			// Before the clean has returned, a crash may keep the name that was refused, which is
			// an outcome that the error of the creation left open, and it has no bytes. After it,
			// the name is not there, whatever the slot is used for next.
			if let Some(ghost) = read(fs, "/d/ghost") {
				assert!(ghost.is_empty(), "{context}: the name that was refused has {ghost:?}");
				assert!(!acks.contains(&"cleaned"), "{context}: a name that was refused is back");
			}
			nothing_of_the_slot_and_what_was_synced(fs, acks, context);
		},
	});
	assert!(checked > 300, "{checked}");
}

#[test]
fn a_cut_made_through_the_handle_of_a_removed_file_is_durable_before_the_slot_is_named_again() {
	// A file is removed, and synced after that, so that its free header is durable; then it is cut
	// to nothing through the handle that is still open. The cut is not flushed, and the clean has
	// nothing to write, since the length already says that there is nothing to cut: it has to flush
	// all the same, or a crash that keeps the header of the file that is named in the slot next,
	// and loses the cut, shows that file with the bytes of the old one.
	let checked = enumerate(&Scenario {
		prepare: &|| {
			let (fs, raw) = memory_pool();
			let old = put(&fs, "/d/old", &[STALE; 5000]);
			fs.remove_file(p("/d/old")).unwrap();
			old.sync().unwrap();
			old.set_len(0).unwrap();
			drop(old);
			assert_eq!(fs.stats().dirty, 1);
			(fs, raw)
		},
		run: &|fs| {
			let mut acks = clean_all(fs);
			acks.extend(named_and_written(fs));
			acks
		},
		check: &nothing_of_the_slot_and_what_was_synced,
	});
	assert!(checked > 200, "{checked}");
}

/// A fault that a test sets on the memory of a pool.
type Fault<'a> = &'a dyn Fn(&MemRaw);

#[test]
fn a_clean_that_failed_after_the_cut_is_made_again_in_full_and_orders_the_cut_before_the_reuse() {
	// The cut is made and its flush, or the write of the other page, fails. The slot goes back to
	// the dead ones, and the next clean finds the length where it is to be and a free header that
	// is flushed already: it must still flush, or the cut that was made is not ordered before the
	// bytes of the file that is named in the slot next, and a crash shows the old bytes after them.
	let faults: [(&str, Fault); 2] = [
		("the flush after the cut", &|raw| raw.fail_flush_after(1)),
		("the write of the other page", &|raw| raw.fail_write_after(0)),
	];
	for (what, fault) in faults {
		let checked = enumerate(&Scenario {
			prepare: &|| {
				let (fs, raw) = reused_after(false);
				fault(&raw);
				assert_eq!(fs.maintain(8), 0, "{what}: the clean fails");
				raw.clear_faults();
				assert_eq!(fs.stats().dirty, 1, "{what}: the slot is dead again");
				(fs, raw)
			},
			run: &|fs| {
				let mut acks = clean_all(fs);
				acks.extend(named_and_written(fs));
				acks
			},
			check: &|fs, acks, context| {
				nothing_of_the_slot_and_what_was_synced(fs, acks, &format!("{what}: {context}"));
			},
		});
		assert!(checked > 100, "{what}: {checked}");
	}
}

#[test]
fn a_slot_that_a_mount_cut_back_and_a_claim_named_shows_nothing_of_what_it_held() {
	let checked = enumerate(&Scenario {
		prepare: &|| {
			let (fs, raw) = reused_after(false);
			// What a crash leaves of a removal that was not cleaned: the free header is there, and
			// the bytes are too, and the mount cuts them off.
			drop(fs);
			let image = raw.snapshot();
			let (fs, raw) = remounted(&image);
			assert_eq!(fs.stats().clean, 1, "the slot is free, and was cut back by the mount");
			(fs, raw)
		},
		run: &named_and_written,
		check: &nothing_of_the_slot_and_what_was_synced,
	});
	assert!(checked > 200, "{checked}");
}

#[test]
fn a_truncating_create_in_a_reused_slot_shows_nothing_of_what_it_held() {
	let checked = enumerate(&Scenario {
		prepare: &|| {
			let (fs, raw) = reused_after(true);
			put(&fs, "/d/b", b"old b");
			(fs, raw)
		},
		run: &|fs| {
			let mut acks = Vec::new();
			let Ok(b) = fs.create_truncate(p("/d/b"), CreateOpts::default()) else {
				return acks;
			};
			acks.push("truncated");
			if b.append(b"new bytes").is_err() || b.sync().is_err() {
				return acks;
			}
			acks.push("synced");
			acks
		},
		check: &|fs, acks, context| {
			shows_nothing_of_the_slot(fs, "b", context);
			let b = read(fs, "/d/b").unwrap_or_else(|| panic!("{context}: b is gone"));
			// The old file, or a new one whose bytes were synced, torn or not there (zeros).
			let new_bytes = b"new bytes";
			assert!(
				b == b"old b"
					|| (b.len() <= new_bytes.len()
						&& (new_bytes.starts_with(&b) || b.iter().all(|byte| *byte == 0))),
				"{context}: b is {b:?}"
			);
			if acks.contains(&"synced") {
				assert_eq!(b, b"new bytes", "{context}");
			}
		},
	});
	assert!(checked > 100, "{checked}");
}

#[test]
fn a_replacement_in_a_reused_slot_is_the_old_content_or_the_new_and_never_the_slot_it_was() {
	let checked = enumerate(&Scenario {
		prepare: &|| {
			let (fs, raw) = reused_after(true);
			fs.replace_atomic(p("/d/MANIFEST"), b"the old manifest").unwrap();
			(fs, raw)
		},
		run: &|fs| {
			let mut acks = Vec::new();
			if fs.replace_atomic(p("/d/MANIFEST"), b"the new manifest, which is longer").is_ok() {
				acks.push("replaced");
			}
			acks
		},
		check: &|fs, acks, context| {
			let content = read(fs, "/d/MANIFEST")
				.unwrap_or_else(|| panic!("{context}: the manifest is gone"));
			assert!(
				content == b"the old manifest" || content == b"the new manifest, which is longer",
				"{context}: {:?}",
				String::from_utf8_lossy(&content)
			);
			if acks.contains(&"replaced") {
				assert_eq!(content, b"the new manifest, which is longer", "{context}");
			}
		},
	});
	assert!(checked > 100, "{checked}");
}

#[test]
fn a_rename_over_a_file_and_the_clean_after_it_are_the_old_names_or_the_new_ones() {
	let checked = enumerate(&Scenario {
		prepare: &|| {
			let (fs, raw) = reused_after(true);
			put(&fs, "/d/a", b"A");
			put(&fs, "/d/b", b"B");
			(fs, raw)
		},
		run: &|fs| {
			let mut acks = Vec::new();
			if fs.rename(p("/d/a"), p("/d/b")).is_ok() {
				acks.push("renamed");
			}
			clean_all(fs);
			acks
		},
		check: &|fs, acks, context| {
			let (a, b) = (read(fs, "/d/a"), read(fs, "/d/b"));
			let before = a.as_deref() == Some(&b"A"[..]) && b.as_deref() == Some(&b"B"[..]);
			let after = a.is_none() && b.as_deref() == Some(&b"A"[..]);
			assert!(before || after, "{context}: a={a:?} b={b:?}");
			if acks.contains(&"renamed") {
				assert!(after, "{context}: a rename that returned is durable");
			}
		},
	});
	assert!(checked > 100, "{checked}");
}

// ---- the engine ------------------------------------------------------------------------------

mod engine {
	use std::time::Duration;

	use test_log::test;

	use super::super::collect_transaction_all;
	use super::super::fs_engine_tests::{
		commit_range,
		key_of,
		memory_with_pool,
		options_in,
		until,
		value_of,
		within,
	};
	use super::{Header, STALE};
	use crate::vfs::header::HEADER_LEN;
	use crate::vfs::pool::parse_slot_name;
	use crate::vfs::{MemRaw, PoolConfig, PoolFs};
	use crate::{CrashMode, Durability, FileSystem, Mode, Options, TreeBuilder, WalRecoveryMode};

	const COMMITS: usize = 300;

	fn options(fs: &FileSystem) -> Options {
		let mut options = options_in(fs, 8 * 1024);
		options.enable_vlog = true;
		options.vlog_value_threshold = 64;
		options.vlog_max_file_size = 8 * 1024;
		options
	}

	/// The database of a first life that was abandoned: what the file system held when the process
	/// stopped, with commits that were not synced.
	async fn abandoned() -> MemRaw {
		let (fs, pool) = memory_with_pool();
		let tree = TreeBuilder::with_options(options(&fs)).build_async().await.unwrap();
		commit_range(&tree, 0..COMMITS, 150, Durability::Eventual).await;
		tokio::task::yield_now().await;
		tree.abandon();
		fs.abandon();
		let raw = pool.raw().clone();
		drop(tree);
		raw
	}

	/// `count` slots that are free and hold `STALE` bytes after their headers, which is what a
	/// crash leaves of the slots of files that were removed, named above every slot that there
	/// is: the files that a recovery makes take them before any other.
	fn with_dead_slots(image: &MemRaw, count: usize, len: usize) -> MemRaw {
		let highest = image
			.names()
			.iter()
			.filter_map(|name| parse_slot_name(name))
			.max()
			.expect("the image has slots");
		for slot in 1..=count as u64 {
			let mut bytes = Header::free(1).encode().unwrap().to_vec();
			bytes.extend_from_slice(&Header::free(0).encode().unwrap());
			assert_eq!(bytes.len() as u64, HEADER_LEN);
			bytes.extend(std::iter::repeat_n(STALE, len));
			image.put(&format!("p{:08x}", highest + slot), &bytes);
		}
		image.snapshot()
	}

	/// Opens the database in `raw`, which has to open and hold the commits in order and whole,
	/// waits until what it recovered is flushed, and closes it or abandons it. Returns how many
	/// commits it holds. The operations of the backend that this makes are what an open that
	/// recovers does.
	async fn recovering_open(
		raw: &MemRaw,
		mode: WalRecoveryMode,
		close: bool,
	) -> Result<usize, String> {
		let (pool, _) = PoolFs::<MemRaw>::mount_sync(raw.clone(), PoolConfig::memory())
			.map_err(|e| format!("mount: {e}"))?;
		let fs = FileSystem(pool);
		let tree = TreeBuilder::with_options(options(&fs).with_wal_recovery_mode(mode))
			.build_async()
			.await
			.map_err(|e| format!("the database does not open: {e}"))?;
		let found = {
			let txn = tree.begin_with_mode(Mode::ReadOnly).map_err(|e| e.to_string())?;
			let mut iter = txn.range(key_of(0), key_of(1_000_000)).map_err(|e| e.to_string())?;
			let all = collect_transaction_all(&mut iter).map_err(|e| e.to_string())?;
			for (at, (key, value)) in all.iter().enumerate() {
				if *key != key_of(at) || *value != value_of(at, 150) {
					return Err(format!("position {at} is not commit {at}"));
				}
			}
			all.len()
		};
		until("the memtables to be flushed", || tree.core.inner.immutable_count() == 0).await;
		if close {
			let _ = tree.close().await;
			let _ = fs.close().await;
		} else {
			tree.abandon();
			fs.abandon();
		}
		drop(tree);
		Ok(found)
	}

	/// The crash images that are tried after an open that was killed: the modes that the backend
	/// has, with seeds for the one that reorders.
	fn images_of(raw: &MemRaw) -> Vec<(String, MemRaw)> {
		let pending = raw.unflushed_ops();
		let mut modes =
			vec![CrashMode::LoseUnsynced, CrashMode::ZeroTail, CrashMode::Prefix(usize::MAX)];
		modes.extend((0..=pending).map(CrashMode::Prefix).step_by(3));
		for k in (0..pending).step_by(2) {
			for bytes in [17usize, 2048] {
				modes.push(CrashMode::Tear(k, bytes));
			}
		}
		modes.extend((0..12).map(CrashMode::Reorder));
		if pending <= 12 {
			for lost in 0..pending {
				for torn in 0..pending {
					modes.push(CrashMode::LoseTear {
						lost,
						torn,
						n: 17,
					});
				}
			}
		}
		modes.into_iter().map(|mode| (format!("{mode:?}"), raw.crash(mode))).collect()
	}

	/// Every kill of an open that recovers `start`, every crash of what it left, and an open of
	/// that in two modes of recovery, which must hold every commit that `start` held.
	async fn every_kill(start: &MemRaw, close: bool, label: &str) -> (usize, Vec<String>) {
		let baseline = recovering_open(&start.snapshot(), WalRecoveryMode::TolerateTornTail, close)
			.await
			.unwrap_or_else(|e| panic!("{label}: the starting image does not open: {e}"));
		let probe = start.snapshot();
		let before = probe.ops();
		let _ = recovering_open(&probe, WalRecoveryMode::TolerateTornTail, close).await;
		let total = probe.ops() - before;
		let (mut opened, mut failures) = (0, Vec::new());
		for n in 0..=total {
			let killed = start.snapshot();
			killed.die_after(n);
			let _ = tokio::time::timeout(
				Duration::from_secs(20),
				recovering_open(&killed, WalRecoveryMode::TolerateTornTail, close),
			)
			.await;
			for (how, image) in images_of(&killed) {
				for mode in [WalRecoveryMode::TolerateTornTail, WalRecoveryMode::SalvageValidPrefix]
				{
					let context =
						format!("{label}: killed after {n} of {total} operations, {how}, {mode:?}");
					let reopened = tokio::time::timeout(
						Duration::from_secs(20),
						recovering_open(&image.snapshot(), mode, false),
					)
					.await;
					match reopened {
						Ok(Ok(found)) if found >= baseline => {}
						Ok(Ok(found)) => {
							failures.push(format!("{context}: {found} commits, {baseline} before"))
						}
						Ok(Err(e)) => failures.push(format!("{context}: {e}")),
						Err(_) => failures.push(format!("{context}: no answer")),
					}
					opened += 1;
				}
			}
		}
		(opened, failures)
	}

	async fn every_kill_of_every_start(dead_slots: usize) {
		let raw = abandoned().await;
		let (mut opened, mut failures) = (0, Vec::new());
		for mode in [CrashMode::LoseUnsynced, CrashMode::Prefix(usize::MAX), CrashMode::ZeroTail] {
			let start = match dead_slots {
				0 => raw.crash(mode),
				count => with_dead_slots(&raw.crash(mode), count, 3000),
			};
			for close in [false, true] {
				let (n, f) = every_kill(&start, close, &format!("{mode:?} close={close}")).await;
				opened += n;
				failures.extend(f);
			}
		}
		assert!(
			failures.is_empty(),
			"{} of {opened}: {:#?}",
			failures.len(),
			&failures[..failures.len().min(6)]
		);
		assert!(opened > 1000, "{opened}");
	}

	#[test(tokio::test)]
	async fn an_open_that_recovers_survives_a_kill_at_every_operation_and_every_crash_after_it() {
		within("every kill", every_kill_of_every_start(0)).await;
	}

	#[test(tokio::test)]
	async fn the_same_in_a_pool_whose_free_slots_hold_the_bytes_of_files_that_were_removed() {
		within("every kill, dead slots", every_kill_of_every_start(8)).await;
	}
}

// ---- the slots of a pool over many crashes --------------------------------------------------

/// The number of slot files that a pool does not lose, over many crashes and mounts.
///
/// A slot is never given up: one that a mount finds is named, or free and claimable, and nothing
/// else. The count of slot files can still rise, by one at a time and less and less often: a
/// claim makes a slot only when none is free, so the count is the most that were in use at once,
/// and a crash brings back files that were removed and not yet cleaned (a removal is not flushed),
/// so what a mount finds in use is sometimes more than before. What is pinned here is both: that
/// every slot on the disk is accounted for at every mount, which is what makes it no leak, and
/// that the count never goes past the most files that a mount has found, plus what one life of
/// the database makes before it removes anything, and plus the spares that the pool is asked to
/// keep when it keeps some.
mod slots_over_crashes {
	use std::collections::BTreeMap;
	use std::path::Path;
	use std::sync::Arc;

	use test_log::test;

	use super::super::fs_engine_tests::{key_of, options_in, until, value_of, within};
	use crate::vfs::pool::{parse_slot_name, poll_ready};
	use crate::vfs::{CreateOpts, Fs, MemRaw, MountReport, PoolConfig, PoolFs};
	use crate::{CrashMode, Durability, FileSystem, TreeBuilder, WalRecoveryMode};

	/// What a mount found, and the slots that there are, must say the same: every slot file is
	/// named or free, and none is an orphan.
	fn accounted_for(report: &MountReport, raw: &MemRaw, context: &str) {
		let on_disk = raw.names().iter().filter(|name| parse_slot_name(name).is_some()).count();
		assert!(report.orphans_kept.is_empty(), "{context}: {:?}", report.orphans_kept);
		assert!(report.skipped.is_empty() && report.ignored.is_empty(), "{context}");
		assert_eq!(
			report.files + report.spares,
			on_disk,
			"{context}: {} files and {} spares, and {on_disk} slots on the disk",
			report.files,
			report.spares
		);
	}

	fn slots(raw: &MemRaw) -> usize {
		raw.names().iter().filter(|name| parse_slot_name(name).is_some()).count()
	}

	fn p(path: &str) -> &Path {
		Path::new(path)
	}

	struct Rng(u64);

	impl Rng {
		fn next(&mut self) -> u64 {
			self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
			let mut x = self.0;
			x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
			x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
			x ^ (x >> 31)
		}
	}

	/// The most files that one life makes before it removes any: a table, a value-log file and a
	/// log segment.
	const A_LIFE_ADDS: usize = 3;

	/// One life of a database over a pool, in the shape of the files that the engine makes: a log
	/// segment per life, tables, a manifest that is replaced, value-log files. It ends as a crash
	/// does, with the handles released and nothing flushed.
	fn life(fs: &Arc<PoolFs<MemRaw>>, n: u64, rng: &mut Rng) {
		for dir in ["/d/wal", "/d/sst", "/d/vlog"] {
			fs.create_dir_all(p(dir)).unwrap();
		}
		let list = |dir: &str| -> Vec<String> {
			let mut names: Vec<String> = fs
				.read_dir(p(dir))
				.unwrap()
				.into_iter()
				.map(|entry| entry.name.to_string_lossy().into_owned())
				.collect();
			names.sort();
			names
		};
		// Recovery: what the log holds goes to a table, the manifest is replaced, the log is
		// removed, and the oldest tables are compacted away. The engine makes the removals of
		// both durable, by cleaning the files that it removed. A value-log file that it removes
		// is left to the clean that comes with the next claim, and a crash may bring it back.
		let segments = list("/d/wal");
		if !segments.is_empty() {
			let table = fs
				.create_truncate(p(&format!("/d/sst/{n:08}.sst")), CreateOpts::default())
				.unwrap();
			table.append(&[7u8; 300]).unwrap();
			table.sync().unwrap();
			fs.sync_dir(p("/d/sst")).unwrap();
			fs.replace_atomic(p("/d/MANIFEST"), format!("tables up to {n}").as_bytes()).unwrap();
			for segment in &segments {
				fs.remove_file(p(&format!("/d/wal/{segment}"))).unwrap();
			}
			// As the engine does after it removes the log: the removals are made durable.
			fs.maintain(usize::MAX);
		}
		let tables = list("/d/sst");
		for table in tables.iter().take(tables.len().saturating_sub(2)) {
			fs.remove_file(p(&format!("/d/sst/{table}"))).unwrap();
		}
		fs.maintain(usize::MAX);
		let vlogs = list("/d/vlog");
		for vlog in vlogs.iter().take(vlogs.len().saturating_sub(1)) {
			if rng.next() % 2 == 0 {
				fs.remove_file(p(&format!("/d/vlog/{vlog}"))).unwrap();
			}
		}
		// The work of this life: a value-log file and a log segment, written and not synced, and
		// sometimes a sync.
		let vlog =
			fs.create_new(p(&format!("/d/vlog/{n:08}.vlog")), CreateOpts::default()).unwrap();
		let segment =
			fs.create_new(p(&format!("/d/wal/{n:08}.wal")), CreateOpts::default()).unwrap();
		for _ in 0..(1 + rng.next() % 4) {
			vlog.append(&[1u8; 200]).unwrap();
			segment.append(&[2u8; 100]).unwrap();
			if rng.next() % 3 == 0 {
				segment.sync().unwrap();
			}
		}
		fs.maintain((rng.next() % 3) as usize);
		fs.pool().unwrap().abandon();
	}

	/// Mounts, lives and crashes `cycles` times, and returns the slots after each life and the
	/// most files that a mount found.
	fn cycles(
		count: u64,
		seed: u64,
		mode: &dyn Fn(u64) -> CrashMode,
		config: &dyn Fn() -> PoolConfig,
		spares: usize,
	) -> (Vec<usize>, usize) {
		let mut image = MemRaw::new();
		let mut rng = Rng(seed);
		let mut counts = Vec::new();
		let mut most_files = 0;
		for n in 0..count {
			let (fs, report) = PoolFs::mount_sync(image.clone(), config()).unwrap();
			accounted_for(&report, &image, &format!("cycle {n}"));
			most_files = most_files.max(report.files);
			if spares > 0 {
				poll_ready(fs.refill(spares)).unwrap().unwrap();
			}
			life(&fs, n, &mut rng);
			let raw = fs.raw().clone();
			counts.push(slots(&raw));
			image = raw.crash(mode(n));
			drop(fs);
		}
		(counts, most_files)
	}

	#[test]
	fn no_slot_is_lost_over_many_crashes_and_the_count_is_bounded_by_the_files_in_use() {
		const CYCLES: u64 = 600;
		let seed = 5;
		let tear = |n: u64| CrashMode::LoseTear {
			lost: (n % 5) as usize,
			torn: (n % 7) as usize,
			n: 17,
		};
		let modes: [(&str, &dyn Fn(u64) -> CrashMode); 4] = [
			("reorder, one seed", &|_| CrashMode::Reorder(5)),
			("reorder, a seed for each", &|n| CrashMode::Reorder(seed + n)),
			("one lost and one torn", &tear),
			("only what was flushed", &|_| CrashMode::LoseUnsynced),
		];
		let mut report = BTreeMap::new();
		for (name, mode) in modes {
			// A pool that makes a slot when it is asked for one: the count is the most in use.
			let (counts, most_files) = cycles(CYCLES, seed, mode, &PoolConfig::memory, 0);
			let last = *counts.last().unwrap();
			assert!(
				last <= most_files + A_LIFE_ADDS,
				"{name}: {last} slots, and no mount found more than {most_files} files"
			);
			// A pool that keeps spares, as the one of a browser does: the spares come on top.
			const SPARES: usize = 6;
			let (counts_with_spares, most_files_with_spares) =
				cycles(CYCLES, seed, mode, &|| PoolConfig::strict(SPARES, 4096), SPARES);
			let last_with_spares = *counts_with_spares.last().unwrap();
			assert!(
				last_with_spares <= most_files_with_spares + A_LIFE_ADDS + SPARES,
				"{name}, with spares: {last_with_spares} slots, and no mount found more than \
				 {most_files_with_spares} files"
			);
			report.insert(name, (last, most_files, last_with_spares, most_files_with_spares));
		}
		assert_eq!(report.len(), 4, "{report:?}");
	}

	/// The pool that the tests of the engine use to see whether a creation that was not made room
	/// for fails: spares are made only by what waits for them.
	fn pool_with_spares() -> PoolConfig {
		PoolConfig::strict(SPARES, 4096)
	}

	/// The spares that `pool_with_spares` makes when it is mounted over an empty directory.
	const SPARES: usize = 8;

	/// A pool for a test: what it is called, how it is configured, and how many spares it keeps.
	type KindOfPool = (&'static str, fn() -> PoolConfig, usize);

	/// The same over the engine: a database that is opened, written to a little (the same few
	/// keys, so that what it holds does not grow), and abandoned, again and again, in a pool that
	/// makes a slot when it is asked for one and in one that keeps spares and is refilled by what
	/// waits for them. In the second the removals of files are made durable only by what calls
	/// `maintain`, so a removal that the engine does not follow with one would show here as files
	/// that come back and are never gone.
	#[test(tokio::test)]
	async fn no_slot_is_lost_by_a_database_that_is_crashed_and_opened_again_and_again() {
		within("crash and open", async {
			let pools: [KindOfPool; 2] = [
				("a pool that makes slots", PoolConfig::memory, 0),
				("a pool that keeps spares", pool_with_spares, SPARES),
			];
			for (pool_name, config, spares) in pools {
				for (name, mode) in [
					("only what was flushed", CrashMode::LoseUnsynced),
					("the length and not the bytes", CrashMode::ZeroTail),
					("everything", CrashMode::Prefix(usize::MAX)),
				] {
					let name = format!("{pool_name}, {name}");
					let mut image = MemRaw::new();
					let (mut next, mut most_files, mut counts) = (0usize, 0usize, Vec::new());
					for cycle in 0..40 {
						let (pool, report) =
							PoolFs::<MemRaw>::mount_sync(image.clone(), config()).unwrap();
						accounted_for(&report, &image, &format!("{name}, cycle {cycle}"));
						most_files = most_files.max(report.files);
						let fs = FileSystem(Arc::clone(&pool) as Arc<dyn Fs>);
						let mut options = options_in(&fs, 8 * 1024);
						options.enable_vlog = true;
						options.vlog_value_threshold = 64;
						options.vlog_max_file_size = 8 * 1024;
						let options =
							options.with_wal_recovery_mode(WalRecoveryMode::SalvageValidPrefix);
						let tree = TreeBuilder::with_options(options).build_async().await.unwrap();
						for _ in 0..100 {
							let mut txn = tree.begin().unwrap();
							txn.set_durability(Durability::Eventual);
							txn.set(key_of(next % 40), value_of(next, 150)).unwrap();
							txn.commit().await.unwrap();
							next += 1;
						}
						until("the memtables to be flushed", || {
							tree.core.inner.immutable_count() == 0
						})
						.await;
						tree.abandon();
						fs.abandon();
						counts.push(slots(pool.raw()));
						image = pool.raw().crash(mode);
						drop(tree);
					}
					let last = *counts.last().unwrap();
					// What a life of the engine makes before it removes anything is more than three
					// files: a segment, the tables of a flush and a compaction, value-log files,
					// the manifest in a slot of its own. A pool that keeps
					// spares has more of them than that, as many as the engine
					// asks it to have, and the count is that many above
					// the files and no more.
					if spares == 0 {
						assert!(
							last <= most_files + 12,
							"{name}: {last} slots, and no mount found more than {most_files} files: \
							 {counts:?}"
						);
					}
					// And whatever the pool, the count has stopped rising by the time that the
					// database has been crashed and opened half of the times.
					assert!(
						last <= counts[19] + 3,
						"{name}: {last} slots after 40 cycles and {} after 20: {counts:?}",
						counts[19]
					);
				}
			}
		})
		.await;
	}
}
