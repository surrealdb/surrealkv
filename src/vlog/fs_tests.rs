//! What the value log does to its files, pinned from outside: the bytes it leaves, the order it
//! syncs them in, the files it removes, and what it does with a log that a crash left damaged.
//!
//! The first group runs the same scenario that was run on the value log before it was moved onto
//! the file system seam (`vfs::Fs`), and compares what it leaves with what the unmoved code left
//! (the expected text below was recorded from the code before the move). The second group counts
//! the calls that the log makes of the seam, with a file system that records them.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tempfile::TempDir;

use super::{VLog, VLogWriter, ValuePointer, SYNCED_VLOG_LENGTHS};
use crate::error::{Error, Result as SkvResult};
use crate::lockfile::LockFile;
use crate::vfs::{CreateOpts, DirEntry, EntryKind, Fs, FsFile, FsKind, StdFs};
use crate::{Options, VLogChecksumLevel};

fn options(dir: &Path) -> Arc<Options> {
	let mut opts = Options {
		vlog_checksum_verification: VLogChecksumLevel::Full,
		vlog_max_file_size: 150,
		..Default::default()
	};
	opts.path = dir.to_path_buf();
	// The files of these tests are on the disk by design: what the log leaves is compared with
	// what it left on `std` before the move onto the seam, and the other group counts its calls.
	opts.fs = crate::vfs::default_fs();
	std::fs::create_dir_all(opts.vlog_dir()).unwrap();
	Arc::new(opts)
}

fn key(i: usize) -> Vec<u8> {
	format!("k{i:03}").into_bytes()
}

fn value(i: usize) -> Vec<u8> {
	(0..40).map(|b| (b + i) as u8).collect()
}

/// The directory as the test sees it: each file with its length and a checksum of its bytes, the
/// creation time in the header left out as it differs from run to run.
fn listing(opts: &Options) -> Vec<String> {
	let mut files: Vec<_> =
		std::fs::read_dir(opts.vlog_dir()).unwrap().map(|entry| entry.unwrap().path()).collect();
	files.sort();
	files
		.into_iter()
		.map(|path| {
			let mut bytes = std::fs::read(&path).unwrap();
			if bytes.len() >= 18 {
				bytes[10..18].fill(0);
			}
			format!(
				"{} len={} crc={:08x}",
				path.file_name().unwrap().to_string_lossy(),
				bytes.len(),
				crc32fast::hash(&bytes)
			)
		})
		.collect()
}

/// The fsyncs of the files of this log that completed, in order, as (file, length).
fn ledger(opts: &Options) -> Vec<String> {
	let dir = opts.vlog_dir();
	SYNCED_VLOG_LENGTHS
		.lock()
		.iter()
		.filter(|(path, _)| path.starts_with(&dir))
		.map(|(path, len)| format!("{} len={len}", path.file_name().unwrap().to_string_lossy()))
		.collect()
}

fn handles(vlog: &VLog) -> Vec<u32> {
	let mut ids: Vec<u32> = vlog.file_handles.read().keys().copied().collect();
	ids.sort_unstable();
	ids
}

/// Appends across several files, syncs, flushes, reads, reopens, appends again and collects the
/// files that nothing points into. Returns everything that can be seen of it from outside.
fn scenario(open: &dyn Fn(Arc<Options>) -> VLog) -> String {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	let mut out = Vec::new();
	let mut pointers = Vec::new();

	{
		let vlog = open(Arc::clone(&opts));
		for i in 0..8 {
			pointers.push(vlog.append(&key(i), &value(i)).unwrap());
			if i == 2 {
				vlog.sync().unwrap();
				out.push(format!("after sync at 2: unsynced={:?}", vlog.unsynced_file_ids()));
			}
			if i == 5 {
				vlog.flush().unwrap();
			}
		}
		vlog.flush().unwrap();
		for (i, pointer) in pointers.iter().enumerate() {
			assert_eq!(vlog.get(pointer).unwrap(), value(i));
		}
		out.push(format!(
			"before the last sync: unsynced={:?} active={}",
			vlog.unsynced_file_ids(),
			vlog.active_writer_id.load(std::sync::atomic::Ordering::SeqCst)
		));
		vlog.sync().unwrap();
		out.push(format!("after the last sync: unsynced={:?}", vlog.unsynced_file_ids()));
		out.push(format!("dir syncs: {}", vlog.dir_syncs()));
	}
	out.push(format!("files: {:#?}", listing(&opts)));
	out.push(format!("ledger: {:#?}", ledger(&opts)));

	{
		let vlog = open(Arc::clone(&opts));
		out.push(format!(
			"reopened: next={} active={} writer={}",
			vlog.next_file_id.load(std::sync::atomic::Ordering::SeqCst),
			vlog.active_writer_id.load(std::sync::atomic::Ordering::SeqCst),
			vlog.writer.read().is_some()
		));
		for (i, pointer) in pointers.iter().enumerate() {
			assert_eq!(vlog.get(pointer).unwrap(), value(i), "after the reopen");
		}
		let more = vlog.append(&key(8), &value(8)).unwrap();
		out.push(format!("one more: file={} offset={}", more.file_id, more.offset));
		vlog.sync().unwrap();
		assert_eq!(vlog.get(&more).unwrap(), value(8));
		out.push(format!("handles: {:?}", handles(&vlog)));

		// Nothing points into a file below 3.
		vlog.cleanup_obsolete_files(3).unwrap();
		out.push(format!("handles after the cleanup: {:?}", handles(&vlog)));
	}
	out.push(format!("files after the cleanup: {:#?}", listing(&opts)));
	out.join("\n")
}

fn std_open(opts: Arc<Options>) -> VLog {
	VLog::new(opts).unwrap()
}

/// What the unmoved code leaves (recorded from `scenario` run on the code before the move).
const EXPECTED: &str = r#"after sync at 2: unsynced=[]
before the last sync: unsynced=[1, 2] active=3
after the last sync: unsynced=[]
dir syncs: 2
files: [
    "00000000000000000001.vlog len=199 crc=5b4089cb",
    "00000000000000000002.vlog len=199 crc=38d38b4c",
    "00000000000000000003.vlog len=143 crc=26b830b5",
]
ledger: [
    "00000000000000000001.vlog len=199",
    "00000000000000000001.vlog len=199",
    "00000000000000000002.vlog len=199",
    "00000000000000000003.vlog len=143",
]
reopened: next=4 active=3 writer=true
one more: file=3 offset=143
handles: [1, 2, 3]
handles after the cleanup: [3]
files after the cleanup: [
    "00000000000000000003.vlog len=199 crc=eca00822",
]"#;

#[test]
fn the_files_a_scenario_leaves_are_the_files_it_always_left() {
	assert_eq!(scenario(&std_open), EXPECTED);
}

/// A file whose last entry a crash cut short is not trimmed, and need not be: a pointer names the
/// offset of its entry, nothing reads a file from its start, and the writer appends at the length
/// of the file. The bytes of the torn entry stay where they are, between the entries around them,
/// and no entry before or after is affected.
#[test]
fn a_torn_tail_is_dead_bytes_between_entries_and_nothing_else() {
	use std::io::Write;

	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	let mut opts_big = (*opts).clone();
	opts_big.vlog_max_file_size = 1 << 20;
	let opts = Arc::new(opts_big);

	let before = {
		let vlog = std_open(Arc::clone(&opts));
		let pointers: Vec<_> = (0..3).map(|i| vlog.append(&key(i), &value(i)).unwrap()).collect();
		vlog.sync().unwrap();
		pointers
	};
	let path = opts.vlog_file_path(1);
	let clean_len = std::fs::metadata(&path).unwrap().len();
	// The first bytes of an entry that was never completed.
	let torn = [0, 0, 0, 4, 0, 0, 0, 40, b'k', b'0'];
	std::fs::OpenOptions::new().append(true).open(&path).unwrap().write_all(&torn).unwrap();

	let vlog = std_open(Arc::clone(&opts));
	let after = vlog.append(&key(3), &value(3)).unwrap();
	vlog.sync().unwrap();
	assert_eq!(after.file_id, 1);
	assert_eq!(after.offset, clean_len + torn.len() as u64, "it goes after the torn bytes");
	for (i, pointer) in before.iter().enumerate() {
		assert_eq!(vlog.get(pointer).unwrap(), value(i));
	}
	assert_eq!(vlog.get(&after).unwrap(), value(3));
}

/// A newest file that a crash cut off inside its header holds no entry: it is set aside under
/// another name, its id is not used again, and the log opens.
#[test]
fn a_newest_file_with_a_partial_header_is_set_aside_and_the_log_opens() {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	{
		let vlog = std_open(Arc::clone(&opts));
		vlog.append(&key(0), &value(0)).unwrap();
		vlog.append(&key(1), &value(1)).unwrap();
		vlog.append(&key(2), &value(2)).unwrap();
		// The next append rolls over to file 2.
		vlog.append(&key(3), &value(3)).unwrap();
		vlog.sync().unwrap();
	}
	let second = opts.vlog_file_path(2);
	let header = std::fs::read(&second).unwrap();
	std::fs::write(&second, &header[..12]).unwrap();

	let vlog = std_open(Arc::clone(&opts));
	assert!(!second.exists(), "the file is gone from its name");
	let mut aside = second.as_os_str().to_owned();
	aside.push(".torn");
	assert_eq!(std::fs::read(&aside).unwrap(), &header[..12], "and kept under another");
	assert_eq!(vlog.next_file_id.load(std::sync::atomic::Ordering::SeqCst), 3);
	assert!(vlog.writer.read().is_none(), "nothing is appended to it");
	let more = vlog.append(&key(9), &value(9)).unwrap();
	assert_eq!(more.file_id, 3, "the id of the file that was set aside is not used again");
}

/// A newest file whose header is zeros and that holds entries after it is what a crash leaves when
/// the first page of a file was not written back and a later one was. None of its entries was
/// synced, since an fsync of the file writes its header with them: it is set aside, its id is not
/// used again, and the log opens.
#[test]
fn a_newest_file_whose_header_is_zeros_is_set_aside_and_the_log_opens() {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	let first = {
		let vlog = std_open(Arc::clone(&opts));
		let first: Vec<_> = (0..3).map(|i| vlog.append(&key(i), &value(i)).unwrap()).collect();
		// The next append rolls over to file 2.
		vlog.append(&key(3), &value(3)).unwrap();
		vlog.append(&key(4), &value(4)).unwrap();
		vlog.sync().unwrap();
		first
	};
	let second = opts.vlog_file_path(2);
	let mut bytes = std::fs::read(&second).unwrap();
	assert!(bytes.len() > 31 + 56, "the file holds entries");
	bytes[..31].fill(0);
	std::fs::write(&second, &bytes).unwrap();

	let vlog = std_open(Arc::clone(&opts));
	assert!(!second.exists(), "the file is gone from its name");
	let mut aside = second.as_os_str().to_owned();
	aside.push(".torn");
	assert_eq!(std::fs::read(&aside).unwrap(), bytes, "and kept under another, as it was");
	assert_eq!(vlog.next_file_id.load(std::sync::atomic::Ordering::SeqCst), 3);
	assert!(vlog.writer.read().is_none(), "nothing is appended to it");
	for (i, pointer) in first.iter().enumerate() {
		assert_eq!(vlog.get(pointer).unwrap(), value(i), "the file before it is read as it was");
	}
	let more = vlog.append(&key(9), &value(9)).unwrap();
	assert_eq!(more.file_id, 3, "the id of the file that was set aside is not used again");
}

/// Eight entries over three files of three, three and two entries, which are synced.
fn three_files(opts: &Arc<Options>) -> Vec<ValuePointer> {
	let vlog = std_open(Arc::clone(opts));
	let pointers: Vec<_> = (0..8).map(|i| vlog.append(&key(i), &value(i)).unwrap()).collect();
	vlog.sync().unwrap();
	assert_eq!(pointers.iter().map(|pointer| pointer.file_id).max(), Some(3));
	pointers
}

fn aside(path: &Path) -> PathBuf {
	let mut name = path.as_os_str().to_owned();
	name.push(".torn");
	PathBuf::from(name)
}

/// A file that is followed by another and whose header is zeros is what a crash leaves too, when
/// the first page of a file that was rolled over from was not written back: no record that is
/// durable names an entry of it, since every file that is named was synced, header and all. It is
/// set aside, with its bytes, the files around it are read as they were, the entries that it held
/// are not in the log, and no id is used again.
#[test]
fn a_file_that_is_not_the_newest_and_whose_header_is_zeros_is_set_aside_and_the_log_opens() {
	for zeroed in [vec![1u64], vec![2], vec![1, 2]] {
		let dir = TempDir::new().unwrap();
		let opts = options(dir.path());
		let pointers = three_files(&opts);
		let mut kept = Vec::new();
		for &id in &zeroed {
			let path = opts.vlog_file_path(id);
			let mut bytes = std::fs::read(&path).unwrap();
			assert!(bytes.len() > 31 + 56, "the file holds entries");
			bytes[..31].fill(0);
			std::fs::write(&path, &bytes).unwrap();
			kept.push((id, bytes));
		}

		let vlog = std_open(Arc::clone(&opts));
		for (id, bytes) in &kept {
			let path = opts.vlog_file_path(*id);
			assert!(!path.exists(), "{zeroed:?}: file {id} is gone from its name");
			assert_eq!(&std::fs::read(aside(&path)).unwrap(), bytes, "{zeroed:?}: kept as it was");
		}
		let mut buf = Vec::new();
		for (i, pointer) in pointers.iter().enumerate() {
			if zeroed.contains(&u64::from(pointer.file_id)) {
				assert!(!vlog.check_entry(pointer, &mut buf).unwrap(), "{zeroed:?}: entry {i}");
				assert!(vlog.get(pointer).is_err(), "{zeroed:?}: entry {i}");
			} else {
				assert!(vlog.check_entry(pointer, &mut buf).unwrap(), "{zeroed:?}: entry {i}");
				assert_eq!(vlog.get(pointer).unwrap(), value(i), "{zeroed:?}: entry {i}");
			}
		}
		let open: Vec<u32> = (1..=3).filter(|id| !zeroed.contains(&u64::from(*id))).collect();
		assert_eq!(handles(&vlog), open, "{zeroed:?}: the files that are left are open");
		assert_eq!(vlog.next_file_id.load(Ordering::SeqCst), 4, "{zeroed:?}");
		let more = vlog.append(&key(9), &value(9)).unwrap();
		assert_eq!(more.file_id, 3, "{zeroed:?}: the newest file is appended to as it was");
		vlog.flush().unwrap();
		assert_eq!(vlog.get(&more).unwrap(), value(9));
	}
}

/// The same for a file that is followed by another and was cut off inside its header.
#[test]
fn a_file_that_is_not_the_newest_and_was_cut_off_inside_its_header_is_set_aside() {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	let pointers = three_files(&opts);
	let first = opts.vlog_file_path(1);
	let header = std::fs::read(&first).unwrap()[..12].to_vec();
	std::fs::write(&first, &header).unwrap();

	let vlog = std_open(Arc::clone(&opts));
	assert!(!first.exists());
	assert_eq!(std::fs::read(aside(&first)).unwrap(), header, "kept as it was");
	assert!(vlog.get(&pointers[0]).is_err());
	assert_eq!(vlog.get(&pointers[3]).unwrap(), value(3));
	assert_eq!(vlog.next_file_id.load(Ordering::SeqCst), 4);
}

/// What is refused stays refused: a file that is followed by another and whose header is damaged
/// without being zeros, or that is short and is not the start of a header, is not what a crash
/// leaves of a file that was never synced. The log does not open, and nothing is changed.
#[test]
fn a_file_that_is_not_the_newest_and_whose_header_is_damaged_is_refused() {
	type Damage = fn(&mut Vec<u8>);
	let damage: [(&str, Damage); 4] = [
		("the magic is another", |bytes| bytes[0] ^= 0x01),
		("the id of the file is another", |bytes| bytes[9] ^= 0x01),
		("the file is short and is not the start of a header", |bytes| {
			bytes.truncate(12);
			bytes[1] ^= 0x40;
		}),
		("the header is zeros but for one byte", |bytes| {
			bytes[..31].fill(0);
			bytes[20] = 1;
		}),
	];
	for (what, damage) in damage {
		let dir = TempDir::new().unwrap();
		let opts = options(dir.path());
		three_files(&opts);
		let first = opts.vlog_file_path(1);
		let mut bytes = std::fs::read(&first).unwrap();
		damage(&mut bytes);
		std::fs::write(&first, &bytes).unwrap();
		let before = listing(&opts);

		let result = VLog::new(Arc::clone(&opts));
		assert!(
			matches!(result, Err(Error::Corruption(_)) | Err(Error::Other(_))),
			"{what}: the log opened or failed in another way"
		);
		assert_eq!(listing(&opts), before, "{what}: nothing was changed");
	}
}

// ---------------------------------------------------------------------------------------------
// The calls the log makes of the file system seam
// ---------------------------------------------------------------------------------------------

/// What a [`TestFs`] keeps: the calls, the handles that are open, and the faults to inject.
#[derive(Default)]
struct State {
	log: Mutex<Vec<String>>,
	/// How many handles of each path are alive: a handle is alive from the call that opened it
	/// until it is dropped.
	alive: Mutex<HashMap<PathBuf, usize>>,
	/// How many times each path was opened, ever.
	opened: Mutex<HashMap<PathBuf, usize>>,
	/// Whether a second handle of a path, and the removal or rename of a path that has one, are
	/// refused: what a browser's file system does.
	exclusive: AtomicBool,
	/// Whether a removal fails.
	fail_remove: AtomicBool,
	/// What the file system says it is.
	pool: AtomicBool,
	/// A file that the listing has and the disk has not, with the length that is given for it: a
	/// file that went between the listing of the directory and the open of it.
	ghost: Mutex<Option<(PathBuf, u64)>>,
}

/// A file system that does what `StdFs` does, writes down each call, and can behave as a file
/// system that gives out one handle per path.
#[derive(Clone, Default)]
struct TestFs(Arc<State>);

impl std::fmt::Debug for TestFs {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("TestFs")
	}
}

/// `1.vlog` for `.../00000000000000000001.vlog`, and the last part of any other path.
fn short(path: &Path) -> String {
	let name = path.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
	let trimmed = name.trim_start_matches('0');
	if trimmed.is_empty() || trimmed.starts_with('.') {
		name
	} else {
		trimmed.to_string()
	}
}

impl TestFs {
	fn new() -> Self {
		Self::default()
	}

	/// Gives out one handle per path.
	fn exclusive() -> Self {
		let fs = Self::default();
		fs.0.exclusive.store(true, Ordering::SeqCst);
		fs
	}

	fn record(&self, entry: String) {
		self.0.log.lock().unwrap().push(entry);
	}

	/// What was recorded, and forgets it.
	fn take(&self) -> Vec<String> {
		std::mem::take(&mut *self.0.log.lock().unwrap())
	}

	fn alive(&self, path: &Path) -> usize {
		self.0.alive.lock().unwrap().get(path).copied().unwrap_or(0)
	}

	fn alive_total(&self) -> usize {
		self.0.alive.lock().unwrap().values().sum()
	}

	fn opened(&self, path: &Path) -> usize {
		self.0.opened.lock().unwrap().get(path).copied().unwrap_or(0)
	}

	fn forget_opens(&self) {
		self.0.opened.lock().unwrap().clear();
	}

	fn open(
		&self,
		call: &str,
		path: &Path,
		make: impl FnOnce() -> io::Result<Arc<dyn FsFile>>,
	) -> io::Result<Arc<dyn FsFile>> {
		self.record(format!("{call} {}", short(path)));
		if self.0.exclusive.load(Ordering::SeqCst) && self.alive(path) > 0 {
			return Err(io::Error::new(
				io::ErrorKind::PermissionDenied,
				format!("{} is open already", path.display()),
			));
		}
		let inner = make()?;
		*self.0.alive.lock().unwrap().entry(path.to_path_buf()).or_default() += 1;
		*self.0.opened.lock().unwrap().entry(path.to_path_buf()).or_default() += 1;
		Ok(Arc::new(TestFile {
			path: path.to_path_buf(),
			inner,
			fs: self.clone(),
		}))
	}

	fn refuse_if_open(&self, path: &Path) -> io::Result<()> {
		if self.0.exclusive.load(Ordering::SeqCst) && self.alive(path) > 0 {
			return Err(io::Error::new(
				io::ErrorKind::PermissionDenied,
				format!("{} is open", path.display()),
			));
		}
		Ok(())
	}
}

struct TestFile {
	path: PathBuf,
	inner: Arc<dyn FsFile>,
	fs: TestFs,
}

impl Drop for TestFile {
	fn drop(&mut self) {
		*self.fs.0.alive.lock().unwrap().get_mut(&self.path).unwrap() -= 1;
	}
}

impl crate::vfs::File for TestFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> SkvResult<usize> {
		self.fs.record(format!("read_at {} {offset} {}", short(&self.path), buf.len()));
		self.inner.read_at(offset, buf)
	}

	fn size(&self) -> SkvResult<u64> {
		self.fs.record(format!("size {}", short(&self.path)));
		self.inner.size()
	}
}

impl FsFile for TestFile {
	fn len(&self) -> io::Result<u64> {
		self.fs.record(format!("len {}", short(&self.path)));
		self.inner.len()
	}

	fn append(&self, buf: &[u8]) -> io::Result<()> {
		self.fs.record(format!("append {} {}", short(&self.path), buf.len()));
		self.inner.append(buf)
	}

	fn set_len(&self, len: u64) -> io::Result<()> {
		self.fs.record(format!("set_len {} {len}", short(&self.path)));
		self.inner.set_len(len)
	}

	fn sync(&self) -> io::Result<()> {
		self.fs.record(format!("sync {}", short(&self.path)));
		self.inner.sync()
	}
}

impl Fs for TestFs {
	fn kind(&self) -> FsKind {
		if self.0.pool.load(Ordering::SeqCst) {
			FsKind::Pool
		} else {
			FsKind::Std
		}
	}

	fn entry(&self, path: &Path) -> io::Result<Option<EntryKind>> {
		StdFs.entry(path)
	}

	fn exists(&self, path: &Path) -> bool {
		self.record(format!("exists {}", short(path)));
		StdFs.exists(path)
	}

	fn create_dir_all(&self, path: &Path) -> io::Result<()> {
		self.record(format!("create_dir_all {}", short(path)));
		StdFs.create_dir_all(path)
	}

	fn read_dir(&self, path: &Path) -> io::Result<Vec<DirEntry>> {
		self.record(format!("read_dir {}", short(path)));
		let mut entries = StdFs.read_dir(path)?;
		if let Some((ghost, _)) = &*self.0.ghost.lock().unwrap() {
			if ghost.parent() == Some(path) {
				entries.push(DirEntry {
					name: ghost.file_name().unwrap().to_owned(),
					kind: EntryKind::File,
				});
			}
		}
		Ok(entries)
	}

	fn remove_file(&self, path: &Path) -> io::Result<()> {
		self.record(format!("remove_file {} open={}", short(path), self.alive(path)));
		self.refuse_if_open(path)?;
		if self.0.fail_remove.load(Ordering::SeqCst) {
			return Err(io::Error::other("injected removal failure"));
		}
		StdFs.remove_file(path)
	}

	fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
		StdFs.remove_dir_all(path)
	}

	fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
		self.record(format!("rename {} {}", short(from), short(to)));
		self.refuse_if_open(from)?;
		StdFs.rename(from, to)
	}

	fn sync_dir(&self, path: &Path) -> io::Result<()> {
		self.record(format!("sync_dir {}", short(path)));
		StdFs.sync_dir(path)
	}

	fn open_read(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		self.open("open_read", path, || StdFs.open_read(path))
	}

	fn open_append(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.open("open_append", path, || StdFs.open_append(path, opts))
	}

	fn open_append_existing(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		self.open("open_append_existing", path, || StdFs.open_append_existing(path))
	}

	fn create_new(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.open("create_new", path, || StdFs.create_new(path, opts))
	}

	fn create_truncate(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.open("create_truncate", path, || StdFs.create_truncate(path, opts))
	}

	fn file_len(&self, path: &Path) -> io::Result<u64> {
		self.record(format!("file_len {}", short(path)));
		if let Some((ghost, len)) = &*self.0.ghost.lock().unwrap() {
			if ghost == path {
				return Ok(*len);
			}
		}
		StdFs.file_len(path)
	}

	fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
		self.record(format!("read {}", short(path)));
		self.refuse_if_open(path)?;
		StdFs.read(path)
	}

	fn replace_atomic(&self, path: &Path, content: &[u8]) -> SkvResult<()> {
		StdFs.replace_atomic(path, content)
	}

	fn lock_file(&self, dir: &Path) -> LockFile {
		StdFs.lock_file(dir)
	}
}

fn open_in(fs: &TestFs, opts: Arc<Options>) -> VLog {
	VLog::new_in(Arc::new(fs.clone()), opts).unwrap()
}

fn lines(expected: &[&str]) -> Vec<String> {
	expected.iter().map(|line| line.to_string()).collect()
}

/// The calls with every run of consecutive `file_len` calls put in name order. The log asks for the
/// length of each file in the order the directory lists them, which is not the same on every file
/// system, and the order is not something the log promises.
fn with_file_lens_sorted(calls: Vec<String>) -> Vec<String> {
	let mut sorted = Vec::with_capacity(calls.len());
	let mut run: Vec<String> = Vec::new();
	for call in calls {
		if call.starts_with("file_len ") {
			run.push(call);
		} else {
			run.sort();
			sorted.append(&mut run);
			sorted.push(call);
		}
	}
	run.sort();
	sorted.append(&mut run);
	sorted
}

/// The calls of an append, a read, a rotation, a sync and a removal, in the order they are made.
/// Each is the syscall that was made before the log went through the seam: `exists` is the
/// `stat`, `open_append` the `open`, `len` the `fstat`, `append` the `write` of the buffer,
/// `read_at` the `pread`, `sync` the `fsync` (the `len` before it is the test ledger's, and is
/// not made outside the tests), `sync_dir` the fsync of the directory. What is gone is what
/// sharing one handle makes unnecessary: the duplicate of the descriptor, the second open of the
/// start of a file, and the open of a file for its first read.
///
/// The sequences of the unmoved code were recorded from a copy of the tree at the commit before
/// the move, in which every call site wrote a line (the descriptor's path from `F_GETPATH`) before
/// it made the call, over the same scenario as below:
///
/// ```text
/// first append  exists 1, open_append 1, dup 1, len 1, append 1 31
/// rotation      append 1 168, exists 2, open_append 2, dup 2, len 2, append 2 31
/// flush         append 2 56
/// first get     open_read 2, len 2, read_at 2 0 31 (header), read_at 2 31 56   (same for file 1)
/// sync          len 1, sync 1, len 2, sync 2, sync_dir          (second sync: len 2, sync 2)
/// removal       remove_file 1
/// reopen        read_dir, file_len 1, file_len 2, open_read 1, read_at 1 0 31, open_read 2,
///               read_at 2 0 31, exists 2, open_append 2, dup 2, len 2, open_read 2, read_at 2 0 6
/// ```
///
/// `dup` is the `fcntl` that `try_clone` makes, `len` the `fstat`, `open_read` an `open`.
#[test]
fn the_calls_of_an_append_a_read_a_rotation_a_sync_and_a_removal() {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	let fs = TestFs::new();
	let vlog = open_in(&fs, Arc::clone(&opts));
	assert_eq!(fs.take(), lines(&["read_dir vlog"]), "an empty directory");

	// The first append makes the file, and an append goes to the buffer, which is not a call.
	let mut pointers = Vec::new();
	pointers.push(vlog.append(&key(0), &value(0)).unwrap());
	assert_eq!(
		fs.take(),
		lines(&["exists 1.vlog", "open_append 1.vlog", "len 1.vlog", "append 1.vlog 31"]),
		"a new file: its header is the first append"
	);
	pointers.push(vlog.append(&key(1), &value(1)).unwrap());
	pointers.push(vlog.append(&key(2), &value(2)).unwrap());
	assert_eq!(fs.take(), Vec::<String>::new(), "entries go to the buffer");

	// The file is full: the buffer goes out, and the next file is made, with no second open.
	pointers.push(vlog.append(&key(3), &value(3)).unwrap());
	assert_eq!(
		fs.take(),
		lines(&[
			"append 1.vlog 168",
			"exists 2.vlog",
			"open_append 2.vlog",
			"len 2.vlog",
			"append 2.vlog 31",
		]),
		"a rotation"
	);

	// A flush is one append, and a read of a file that was never opened to read is one pread.
	vlog.flush().unwrap();
	assert_eq!(fs.take(), lines(&["append 2.vlog 56"]), "a flush");
	assert_eq!(vlog.get(&pointers[3]).unwrap(), value(3));
	assert_eq!(fs.take(), lines(&["read_at 2.vlog 31 56"]), "a read of the active file");
	assert_eq!(vlog.get(&pointers[0]).unwrap(), value(0));
	assert_eq!(fs.take(), lines(&["read_at 1.vlog 31 56"]), "a read of the file rolled over from");
	assert_eq!(vlog.get(&pointers[3]).unwrap(), value(3));
	assert_eq!(fs.take(), Vec::<String>::new(), "a cached value is no call");

	// The file rolled over from is synced first, then the active one, then the directory.
	vlog.sync().unwrap();
	assert_eq!(
		fs.take(),
		lines(&["len 1.vlog", "sync 1.vlog", "len 2.vlog", "sync 2.vlog", "sync_dir vlog"]),
		"a sync"
	);
	vlog.sync().unwrap();
	assert_eq!(fs.take(), lines(&["len 2.vlog", "sync 2.vlog"]), "a sync with no new file");

	// A removal: the handle is let go of before the file is removed.
	vlog.cleanup_obsolete_files(2).unwrap();
	assert_eq!(fs.take(), lines(&["remove_file 1.vlog open=0"]), "a removal");
	assert!(!opts.vlog_file_path(1).exists());
	let missing = vlog.get(&ValuePointer::new(1, 87, 4, 40, 0));
	assert!(missing.is_err());
	assert_eq!(fs.take(), lines(&["open_read 1.vlog"]), "a pointer into a removed file");
}

/// A log that is opened again opens each file once, the newest to append to as well, and reads
/// its header through that handle.
#[test]
fn the_calls_of_opening_a_log_that_has_files() {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	{
		let vlog = std_open(Arc::clone(&opts));
		for i in 0..4 {
			vlog.append(&key(i), &value(i)).unwrap();
		}
		vlog.sync().unwrap();
	}
	let fs = TestFs::new();
	let vlog = open_in(&fs, Arc::clone(&opts));
	assert_eq!(
		with_file_lens_sorted(fs.take()),
		lines(&[
			"read_dir vlog",
			"file_len 1.vlog",
			"file_len 2.vlog",
			"open_read 1.vlog",
			"read_at 1.vlog 0 31",
			"open_append_existing 2.vlog",
			"read_at 2.vlog 0 31",
			"len 2.vlog",
			"read_at 2.vlog 0 6",
		]),
		"one open per file, and none for the writer; the newest is opened to append and is never \
		 made if it has gone since the listing"
	);
	assert!(vlog.writer.read().is_some());
}

/// The newest file is opened to append to, and one that is not there any more is a `NotFound`: it
/// is not made empty, and then taken for a log that has a file with nothing in it.
#[test]
fn a_newest_file_that_went_after_the_listing_is_not_made() {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	{
		let vlog = std_open(Arc::clone(&opts));
		vlog.append(&key(0), &value(0)).unwrap();
		vlog.sync().unwrap();
	}
	let ghost = opts.vlog_file_path(5);
	let fs = TestFs::new();
	*fs.0.ghost.lock().unwrap() = Some((ghost.clone(), 100));
	let error = VLog::new_in(Arc::new(fs.clone()), Arc::clone(&opts))
		.err()
		.expect("a log whose newest file is gone does not open");
	match &error {
		crate::error::Error::Io(e) => assert_eq!(e.kind(), io::ErrorKind::NotFound, "{error}"),
		other => {
			panic!("a newest file that is gone is an I/O error that says not found: {other:?}")
		}
	}
	assert!(!ghost.exists(), "the file was not made");
	assert_eq!(fs.take().last().map(String::as_str), Some("open_append_existing 5.vlog"));
}

/// The handle of a file is the one handle it has, through the whole of its life, on a file system
/// that gives out one handle per path and refuses to remove or rename a file that is open. The
/// same scenario as on `std`, and the same result.
#[test]
fn a_file_system_that_gives_out_one_handle_per_path_runs_the_scenario_unchanged() {
	let fs = TestFs::exclusive();
	let seen = scenario(&|opts| open_in(&fs, opts));
	assert_eq!(seen, EXPECTED);
	assert_eq!(fs.alive_total(), 0, "every handle was let go of when the log was dropped");
}

#[test]
fn a_log_opened_over_a_file_system_with_one_handle_per_path_opens_each_file_once() {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	let fs = TestFs::exclusive();
	let vlog = open_in(&fs, Arc::clone(&opts));
	let pointers: Vec<_> = (0..8).map(|i| vlog.append(&key(i), &value(i)).unwrap()).collect();
	vlog.flush().unwrap();
	for (i, pointer) in pointers.iter().enumerate() {
		assert_eq!(vlog.get(pointer).unwrap(), value(i));
	}
	vlog.sync().unwrap();
	for id in 1..=3 {
		assert_eq!(fs.opened(&opts.vlog_file_path(id)), 1, "file {id} is opened once");
	}
	assert_eq!(fs.alive(&opts.vlog_file_path(1)), 1, "and held once: by the log");

	// Everything is let go of on request, and the files can be moved and removed.
	vlog.release_files().unwrap();
	assert_eq!(fs.alive_total(), 0);
	assert_eq!(vlog.get(&pointers[0]).unwrap(), value(0), "a read opens its file again");
	assert_eq!(fs.opened(&opts.vlog_file_path(1)), 1, "from the cache");
	for (i, pointer) in pointers.iter().enumerate().skip(3) {
		assert_eq!(vlog.get(pointer).unwrap(), value(i), "from the files");
	}
	assert!(fs.opened(&opts.vlog_file_path(2)) >= 1);
	drop(vlog);
	assert_eq!(fs.alive_total(), 0);

	// After a reopen the newest file is opened once more, and nothing else is held.
	fs.forget_opens();
	let vlog = open_in(&fs, Arc::clone(&opts));
	for id in 1..=3 {
		assert_eq!(fs.opened(&opts.vlog_file_path(id)), 1, "file {id} is opened once");
	}
	vlog.append(&key(9), &value(9)).unwrap();
	vlog.sync().unwrap();
	for id in 1..=3 {
		assert_eq!(fs.opened(&opts.vlog_file_path(id)), 1, "file {id} is still opened once");
	}
}

/// A file that is set aside is renamed with no handle on it, on a file system that refuses to
/// rename a file that is open.
#[test]
fn a_file_that_is_set_aside_is_not_open() {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	{
		let vlog = std_open(Arc::clone(&opts));
		for i in 0..4 {
			vlog.append(&key(i), &value(i)).unwrap();
		}
		vlog.sync().unwrap();
	}
	let second = opts.vlog_file_path(2);
	let header = std::fs::read(&second).unwrap();
	std::fs::write(&second, &header[..12]).unwrap();

	let fs = TestFs::exclusive();
	let vlog = open_in(&fs, Arc::clone(&opts));
	let calls = fs.take();
	assert!(calls.contains(&"read 2.vlog".to_string()), "{calls:?}");
	assert!(calls.contains(&"rename 2.vlog 2.vlog.torn".to_string()), "{calls:?}");
	assert!(calls.contains(&"sync_dir vlog".to_string()), "{calls:?}");
	assert_eq!(fs.opened(&second), 0, "it was never opened");
	assert!(vlog.writer.read().is_none());
}

/// A directory that cannot be listed is a log that cannot be opened. Only a directory that is
/// not there is a log with no files.
#[test]
fn only_a_missing_directory_is_a_log_with_no_files() {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	std::fs::remove_dir(opts.vlog_dir()).unwrap();
	let fs = TestFs::new();
	let vlog = open_in(&fs, Arc::clone(&opts));
	assert!(vlog.writer.read().is_none());
	assert_eq!(vlog.next_file_id.load(Ordering::SeqCst), 1);
	drop(vlog);

	// The name of the directory is a file.
	std::fs::write(opts.vlog_dir(), b"not a directory").unwrap();
	let failed = VLog::new_in(Arc::new(fs), Arc::clone(&opts));
	assert!(matches!(failed, Err(Error::Io(_))), "an error that is not NotFound is an error");
	assert_eq!(std::fs::read(opts.vlog_dir()).unwrap(), b"not a directory");
}

/// A file that could not be removed stays in the registry, and is removed by the next cleanup.
#[test]
fn a_file_that_could_not_be_removed_is_removed_by_the_next_cleanup() {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	let fs = TestFs::new();
	let vlog = open_in(&fs, Arc::clone(&opts));
	for i in 0..8 {
		vlog.append(&key(i), &value(i)).unwrap();
	}
	vlog.sync().unwrap();
	assert_eq!(vlog.files_map.read().len(), 3);

	fs.0.fail_remove.store(true, Ordering::SeqCst);
	vlog.cleanup_obsolete_files(3).unwrap();
	assert!(opts.vlog_file_path(1).exists() && opts.vlog_file_path(2).exists());
	assert_eq!(vlog.files_map.read().len(), 3, "both are still known");

	// A file that is gone already (a removal that a crash lost the directory entry of, or one made
	// by hand) is no failure, and is forgotten. File 2 is not the active file, so the cleanup
	// reaches it.
	std::fs::remove_file(opts.vlog_file_path(2)).unwrap();
	fs.0.fail_remove.store(false, Ordering::SeqCst);
	vlog.cleanup_obsolete_files(3).unwrap();
	assert!(!opts.vlog_file_path(1).exists() && !opts.vlog_file_path(2).exists());
	assert_eq!(vlog.files_map.read().len(), 1, "both are gone, one of them was already");
	assert_eq!(fs.take().iter().filter(|call| call.starts_with("remove_file")).count(), 4);
	assert!(opts.vlog_file_path(3).exists(), "the active file is never removed");
}

/// A sealed value log is made with `std::fs`, so it is refused, before anything is made, on a file
/// system that is not `std`.
#[test]
fn a_sealed_log_is_refused_on_a_file_system_that_is_not_std() {
	use crate::tde::{CipherSuite, DbId, EncryptionContext, SoftwareKeyManager};

	let dir = TempDir::new().unwrap();
	let ctx = Arc::new(EncryptionContext::new(
		Arc::new(SoftwareKeyManager::new([0x11; 32])),
		DbId::from_bytes([0xD0; 16]),
		CipherSuite::Aes256Gcm,
	));
	let mut opts = (*options(dir.path())).clone();
	opts.encryption_context = Some(Arc::clone(&ctx));
	let opts = Arc::new(opts);

	let fs = TestFs::new();
	fs.0.pool.store(true, Ordering::SeqCst);
	let refused = VLog::new_in(Arc::new(fs.clone()), Arc::clone(&opts));
	assert!(matches!(refused, Err(Error::InvalidArgument(_))));
	assert!(fs.take().is_empty(), "nothing was asked of the file system");

	let refused = VLogWriter::create_sealed_in(&fs, &opts.vlog_file_path(1), 1, 1 << 20, 0, &ctx);
	assert!(matches!(refused, Err(Error::InvalidArgument(_))));
	assert!(!opts.vlog_file_path(1).exists());
}

/// A pointer that is damaged cannot make a 32-bit target allocate more than its file holds, and
/// costs a 64-bit one no call to ask for the length of the file.
#[test]
fn a_pointer_that_claims_more_than_its_file_holds_is_corruption() {
	use super::{entry_bound, CHECKED_ENTRY_SIZE};

	let big = CHECKED_ENTRY_SIZE;
	let len = || Ok(1_000_000u64);
	// Bounded (a 32-bit target): an entry that the file cannot hold is cut short, one that it can
	// hold is read, and the length of the file is asked for only by a large entry.
	assert_eq!(entry_bound(big, 31, true, len).unwrap(), Some(1_000_000));
	assert_eq!(entry_bound(big, u64::MAX - 5, true, len).unwrap(), Some(1_000_000), "no overflow");
	assert_eq!(entry_bound(big, 31, true, || Ok(big + 31)).unwrap(), None);
	assert_eq!(entry_bound(big, 31, true, || Ok(big + 30)).unwrap(), Some(big + 30));
	let never = || -> SkvResult<u64> { panic!("the length of the file was asked for") };
	assert_eq!(entry_bound(big - 1, 31, true, never).unwrap(), None, "a small entry");
	// Not bounded (a 64-bit target): the length of the file is never asked for.
	assert_eq!(entry_bound(u64::MAX, 31, false, never).unwrap(), None);
	// A failure to ask is an error, not a read.
	assert!(entry_bound(big, 0, true, || Err(Error::Other("no length".into()))).is_err());

	// Through the log: a pointer past the end of its file is an entry that its file cuts short,
	// whatever the width of the target.
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	let vlog = std_open(Arc::clone(&opts));
	vlog.append(&key(0), &value(0)).unwrap();
	vlog.flush().unwrap();
	let past_the_end = ValuePointer::new(1, 1 << 40, 4, 40, 0);
	assert!(matches!(vlog.get(&past_the_end), Err(Error::Corruption(_))));
}

/// A write that lands in part before it fails is a short write to the buffer in front of the file,
/// as it is for a `std::fs::File`, so the entries before the failed one are kept and the rest is
/// cut.
#[test]
fn a_partial_append_is_cut_back_to_the_last_complete_entry() {
	#[derive(Clone, Copy)]
	enum Mode {
		Works,
		/// The next append lands this many bytes and fails, and the ones after it fail.
		LandThenFail(usize),
		Fails,
	}
	struct Partial {
		inner: Arc<dyn FsFile>,
		mode: Mutex<Mode>,
	}
	impl crate::vfs::File for Partial {
		fn read_at(&self, offset: u64, buf: &mut [u8]) -> SkvResult<usize> {
			self.inner.read_at(offset, buf)
		}

		fn size(&self) -> SkvResult<u64> {
			self.inner.size()
		}
	}
	impl FsFile for Partial {
		fn len(&self) -> io::Result<u64> {
			self.inner.len()
		}

		fn append(&self, buf: &[u8]) -> io::Result<()> {
			let mode = *self.mode.lock().unwrap();
			match mode {
				Mode::Works => self.inner.append(buf),
				Mode::LandThenFail(land) => {
					self.inner.append(&buf[..land.min(buf.len())])?;
					*self.mode.lock().unwrap() = Mode::Fails;
					Err(io::Error::other("a write that landed in part"))
				}
				Mode::Fails => Err(io::Error::other("a write that fails")),
			}
		}

		fn set_len(&self, len: u64) -> io::Result<()> {
			self.inner.set_len(len)
		}

		fn sync(&self) -> io::Result<()> {
			self.inner.sync()
		}
	}

	let dir = TempDir::new().unwrap();
	let mut opts = (*options(dir.path())).clone();
	opts.vlog_max_file_size = 1 << 20;
	let opts = Arc::new(opts);
	let path = opts.vlog_file_path(1);
	let inner = StdFs.open_append(&path, CreateOpts::default()).unwrap();
	let partial = Arc::new(Partial {
		inner,
		mode: Mutex::new(Mode::Works),
	});
	let shared: Arc<dyn FsFile> = Arc::clone(&partial) as Arc<dyn FsFile>;
	let mut writer = VLogWriter::from_file(shared, false, 1, 1 << 20, 0).unwrap();
	let first = writer.append(&key(0), &value(0)).unwrap();
	// The buffer holds the header, the first entry and the start of the second, and the value of
	// the second does not fit the rest of it, so the append of the buffer is the one that fails:
	// 40 bytes of it land, and the writer is told so, as a short write does, and then it fails.
	let big = vec![7u8; 9000];
	*partial.mode.lock().unwrap() = Mode::LandThenFail(40);
	assert!(writer.append(&key(1), &big).is_err());
	*partial.mode.lock().unwrap() = Mode::Works;
	assert_eq!(writer.current_offset, 31 + 56, "the failed entry is not counted");
	// The header went out when the writer was made, and 40 bytes of the buffer landed after it.
	assert_eq!(std::fs::metadata(&path).unwrap().len(), 31 + 40, "what landed is in the file");

	let second = writer.append(&key(2), &value(2)).unwrap();
	writer.flush().unwrap();
	assert_eq!(second.offset, 31 + 56);
	let bytes = std::fs::read(&path).unwrap();
	assert_eq!(bytes.len() as u64, 31 + 56 + 56);
	let file = StdFs.open_read(&path).unwrap();
	for (pointer, want) in [(&first, value(0)), (&second, value(2))] {
		let mut entry = vec![0u8; pointer.total_entry_size() as usize];
		crate::vfs::File::read_exact_at(&*file, pointer.offset, &mut entry).unwrap();
		assert_eq!(&entry[8 + 4..8 + 4 + 40], &want[..]);
	}
}
