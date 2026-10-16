//! The behaviour contract of [`Fs`], as one function over `&dyn Fs`.
//!
//! Every backend runs the same function: `StdFs` in `src/test/fs_std_tests.rs`, and the pool over
//! memory and over a browser's origin private file system when they exist. It is the whole of what
//! the engine may rely on, so a case here that a backend cannot pass is a backend that is wrong or
//! a rule of the seam that has to change, and not a case to skip. It uses nothing but the `Fs`
//! API (no `std::fs`, no temporary directory), so that it compiles for a browser: the caller
//! gives it a directory that exists.
//!
//! Each case works in a directory of its own below `root`, and leaves it in place.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{CreateOpts, EntryKind, Fs, FsCursor, FsFile, FsKind, FsWriter};
use crate::error::Error;
use crate::vfs::{self, File};

/// Runs every case of the contract in directories below `root`, which must exist.
pub(crate) fn run_all(fs: &dyn Fs, root: &Path) {
	available(fs);
	namespace(fs, &case_dir(fs, root, "namespace"));
	creation(fs, &case_dir(fs, root, "creation"));
	positional_reads(fs, &case_dir(fs, root, "reads"));
	append_and_set_len(fs, &case_dir(fs, root, "append"));
	cursor_and_writer(fs, &case_dir(fs, root, "cursor"));
	sync(fs, &case_dir(fs, root, "sync"));
	rename(fs, &case_dir(fs, root, "rename"));
	replace_atomic(fs, &case_dir(fs, root, "replace"));
	unlink_while_open(fs, &case_dir(fs, root, "unlink"));
	whole_file_helpers(fs, &case_dir(fs, root, "helpers"));
	copy(fs, &case_dir(fs, root, "copy"));
	lock(fs, &case_dir(fs, root, "lock"));
	spares(fs);
}

fn case_dir(fs: &dyn Fs, root: &Path, name: &str) -> PathBuf {
	let dir = root.join(name);
	fs.create_dir_all(&dir).expect("a case directory is made");
	dir
}

fn make(fs: &dyn Fs, path: &Path, content: &[u8]) {
	let file = fs.create_truncate(path, CreateOpts::default()).expect("create_truncate");
	file.append(content).expect("append");
	file.sync().expect("sync");
}

fn contents(fs: &dyn Fs, path: &Path) -> Vec<u8> {
	fs.read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn read_all(file: &Arc<dyn FsFile>) -> Vec<u8> {
	let len = usize::try_from(file.len().expect("len")).expect("small");
	let mut buf = vec![0u8; len];
	file.read_exact_at(0, &mut buf).expect("read_exact_at");
	buf
}

fn kind_of<T>(result: io::Result<T>) -> io::ErrorKind {
	match result {
		Ok(_) => panic!("expected an error"),
		Err(e) => e.kind(),
	}
}

/// A file system that is not unavailable answers `check_available`.
pub(crate) fn available(fs: &dyn Fs) {
	assert_ne!(fs.kind(), FsKind::Unavailable);
	fs.check_available().expect("check_available");
}

/// Names: what a path is, listings, creation of directories, removal, and the errors of each.
pub(crate) fn namespace(fs: &dyn Fs, dir: &Path) {
	let nested = dir.join("a/b/c");
	fs.create_dir_all(&nested).expect("create_dir_all");
	fs.create_dir_all(&nested).expect("create_dir_all is idempotent");

	let file = dir.join("a/file");
	make(fs, &file, b"x");

	// The three-way answer, and the questions that read an error as false.
	assert_eq!(fs.entry(&nested).unwrap(), Some(EntryKind::Dir));
	assert_eq!(fs.entry(&file).unwrap(), Some(EntryKind::File));
	assert_eq!(fs.entry(&dir.join("a/none")).unwrap(), None);
	assert!(fs.try_exists(&nested).unwrap() && fs.try_exists(&file).unwrap());
	assert!(!fs.try_exists(&dir.join("a/none")).unwrap());
	assert!(fs.exists(&nested) && fs.exists(&file) && !fs.exists(&dir.join("a/none")));
	assert!(fs.is_dir(&nested) && !fs.is_dir(&file) && !fs.is_dir(&dir.join("a/none")));
	assert!(fs.is_file(&file) && !fs.is_file(&nested) && !fs.is_file(&dir.join("a/none")));

	// A listing has every name once, with what it is, in any order.
	let mut listed: Vec<(String, EntryKind)> = fs
		.read_dir(&dir.join("a"))
		.expect("read_dir")
		.into_iter()
		.map(|entry| (entry.name.to_string_lossy().into_owned(), entry.kind))
		.collect();
	listed.sort_by(|x, y| x.0.cmp(&y.0));
	assert_eq!(
		listed,
		vec![("b".to_string(), EntryKind::Dir), ("file".to_string(), EntryKind::File)]
	);
	assert!(fs.read_dir(&nested).expect("an empty directory").is_empty());
	assert_eq!(kind_of(fs.read_dir(&dir.join("a/none"))), io::ErrorKind::NotFound);
	assert!(fs.read_dir(&file).is_err(), "a file is not a directory");

	// A creation in a directory that is not there is NotFound, and creates nothing.
	let orphan = dir.join("missing/child");
	assert_eq!(kind_of(fs.create_new(&orphan, CreateOpts::default())), io::ErrorKind::NotFound);
	assert_eq!(
		kind_of(fs.create_truncate(&orphan, CreateOpts::default())),
		io::ErrorKind::NotFound
	);
	assert_eq!(kind_of(fs.open_append(&orphan, CreateOpts::default())), io::ErrorKind::NotFound);
	assert!(!fs.exists(&dir.join("missing")));

	// Removal: a file, a tree, and what is not there.
	fs.remove_file(&file).expect("remove_file");
	assert!(!fs.exists(&file));
	assert_eq!(kind_of(fs.remove_file(&file)), io::ErrorKind::NotFound);
	make(fs, &nested.join("deep"), b"y");
	fs.remove_dir_all(&dir.join("a")).expect("remove_dir_all");
	assert!(!fs.exists(&dir.join("a")) && !fs.exists(&nested.join("deep")));
	assert_eq!(kind_of(fs.remove_dir_all(&dir.join("a"))), io::ErrorKind::NotFound);
}

/// The four ways to open a file, and what each does to a file that is there and one that is not.
pub(crate) fn creation(fs: &dyn Fs, dir: &Path) {
	let path = dir.join("f");
	assert_eq!(kind_of(fs.open_read(&path)), io::ErrorKind::NotFound);

	let created = fs.create_new(&path, CreateOpts::default()).expect("create_new");
	assert_eq!(created.len().unwrap(), 0);
	created.append(b"ab").unwrap();
	assert_eq!(kind_of(fs.create_new(&path, CreateOpts::default())), io::ErrorKind::AlreadyExists);

	// `open_append` keeps what is there and writes after it, from whichever handle; it never
	// relies on the two handles being one or being two.
	let again = fs.open_append(&path, CreateOpts::default()).expect("open_append");
	again.append(b"cd").unwrap();
	assert_eq!(read_all(&fs.open_read(&path).expect("open_read")), b"abcd");
	let fresh = dir.join("g");
	let appended = fs.open_append(&fresh, CreateOpts::default()).expect("open_append creates");
	assert_eq!(appended.len().unwrap(), 0);

	assert_eq!(contents(fs, &path), b"abcd");

	// `create_truncate` of a file that is there gives an empty one, for the path.
	let truncated =
		fs.create_truncate(&path, CreateOpts::opportunistic(CreateOpts::default())).unwrap();
	assert_eq!(truncated.len().unwrap(), 0);
	truncated.append(b"new").unwrap();
	assert_eq!(contents(fs, &path), b"new");
	assert_eq!(fs.file_len(&path).unwrap(), 3);
	assert_eq!(kind_of(fs.file_len(&dir.join("none"))), io::ErrorKind::NotFound);
}

/// Positional reads: short at the end of the file, empty past it, and a read that must fill fails.
pub(crate) fn positional_reads(fs: &dyn Fs, dir: &Path) {
	let path = dir.join("f");
	make(fs, &path, b"0123456789");
	let file = fs.open_read(&path).unwrap();
	assert_eq!(file.size().unwrap(), 10);
	assert_eq!(file.len().unwrap(), 10);

	let mut buf = [0u8; 4];
	assert_eq!(file.read_up_to(0, &mut buf).unwrap(), 4);
	assert_eq!(&buf, b"0123");
	// A read across the end is short, and says how short.
	assert_eq!(file.read_up_to(8, &mut buf).unwrap(), 2);
	assert_eq!(&buf[..2], b"89");
	// At the end and past it, nothing.
	assert_eq!(file.read_up_to(10, &mut buf).unwrap(), 0);
	assert_eq!(file.read_up_to(1 << 40, &mut buf).unwrap(), 0);
	assert_eq!(file.read_at(10, &mut buf).unwrap(), 0);
	assert_eq!(file.read_up_to(3, &mut []).unwrap(), 0);

	// A read that must fill the buffer and cannot is an unexpected end of file.
	match file.read_exact_at(8, &mut buf) {
		Err(Error::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof),
		other => panic!("read_exact_at past the end: {other:?}"),
	}
	file.read_exact_at(6, &mut buf).expect("a read that fits");
	assert_eq!(&buf, b"6789");

	// The file coerces to the read seam that tables take.
	let as_file: Arc<dyn File> = file;
	assert_eq!(as_file.size().unwrap(), 10);
}

/// The end of a file: `append` writes there, `len` is truthful, `set_len` moves the end both ways.
pub(crate) fn append_and_set_len(fs: &dyn Fs, dir: &Path) {
	// The same holds of a file that was opened to append and of one that was made empty.
	for (name, open) in [("appended", 0), ("truncated", 1), ("created", 2)] {
		let path = dir.join(name);
		let file = match open {
			0 => fs.open_append(&path, CreateOpts::default()),
			1 => fs.create_truncate(&path, CreateOpts::default()),
			_ => fs.create_new(&path, CreateOpts::default()),
		}
		.expect("open");
		assert_eq!(file.len().unwrap(), 0, "{name}");
		file.append(b"hello ").unwrap();
		file.append(b"world").unwrap();
		assert_eq!(file.len().unwrap(), 11, "{name}");
		assert_eq!(read_all(&(Arc::clone(&file))), b"hello world", "{name}: a write is read back");
		file.append(b"").unwrap();
		assert_eq!(file.len().unwrap(), 11, "{name}: an empty append changes nothing");

		// Cut back: the end is the new length, and the next append lands there with no hole.
		file.set_len(5).unwrap();
		assert_eq!(file.len().unwrap(), 5, "{name}");
		file.append(b"!").unwrap();
		assert_eq!(read_all(&file), b"hello!", "{name}: an append after a cut-back is contiguous");

		// Grown: the new part reads as zeros, and the next append lands after it.
		file.set_len(10).unwrap();
		assert_eq!(file.len().unwrap(), 10, "{name}");
		file.append(b"x").unwrap();
		assert_eq!(read_all(&file), b"hello!\0\0\0\0x", "{name}: a grown file reads as zeros");
		assert_eq!(file.size().unwrap(), 11, "{name}: the read seam sees the same length");

		file.set_len(0).unwrap();
		assert_eq!(file.len().unwrap(), 0, "{name}");
		file.append(b"again").unwrap();
		assert_eq!(contents(fs, &path), b"again", "{name}: and the path sees it");
	}
}

/// `FsWriter` appends, and `FsCursor` reads and seeks over a shared file with a position of its
/// own.
pub(crate) fn cursor_and_writer(fs: &dyn Fs, dir: &Path) {
	use std::io::{Read, Seek, SeekFrom, Write};

	let path = dir.join("f");
	let file = fs.create_truncate(&path, CreateOpts::default()).unwrap();
	let mut writer = std::io::BufWriter::new(FsWriter(Arc::clone(&file)));
	writer.write_all(b"0123456789").unwrap();
	writer.flush().unwrap();
	drop(writer);
	assert_eq!(file.len().unwrap(), 10);

	let mut cursor = FsCursor::new(Arc::clone(&file));
	let mut buf = [0u8; 4];
	cursor.read_exact(&mut buf).unwrap();
	assert_eq!(&buf, b"0123");
	assert_eq!(cursor.seek(SeekFrom::Current(2)).unwrap(), 6);
	cursor.read_exact(&mut buf).unwrap();
	assert_eq!(&buf, b"6789");
	assert_eq!(cursor.read(&mut buf).unwrap(), 0, "the end of the file");
	assert_eq!(cursor.seek(SeekFrom::End(-3)).unwrap(), 7);
	let mut rest = Vec::new();
	cursor.read_to_end(&mut rest).unwrap();
	assert_eq!(rest, b"789");
	assert_eq!(cursor.seek(SeekFrom::Start(1)).unwrap(), 1);
	assert_eq!(cursor.seek(SeekFrom::End(0)).unwrap(), 10);
	assert_eq!(cursor.seek(SeekFrom::End(5)).unwrap(), 15, "a position past the end is allowed");
	assert_eq!(cursor.read(&mut buf).unwrap(), 0);
	assert_eq!(
		cursor.seek(SeekFrom::Current(-100)).unwrap_err().kind(),
		io::ErrorKind::InvalidInput,
		"before the start"
	);
	assert_eq!(cursor.seek(SeekFrom::Start(15)).unwrap(), 15, "a refused seek moved nothing");

	// Two cursors over one file do not move each other, and the cursor is a read seam too.
	let mut other = FsCursor::at(Arc::clone(&file), 8);
	let mut two = [0u8; 2];
	other.read_exact(&mut two).unwrap();
	assert_eq!(&two, b"89");
	assert_eq!(File::size(&other).unwrap(), 10);
	assert_eq!(cursor.stream_position().unwrap(), 15);
}

/// Everything that makes bytes durable can be asked for and succeeds on a file that is there.
pub(crate) fn sync(fs: &dyn Fs, dir: &Path) {
	let path = dir.join("f");
	let file = fs.create_truncate(&path, CreateOpts::default()).unwrap();
	file.append(b"durable").unwrap();
	file.sync().expect("FsFile::sync");
	fs.sync_file(&path).expect("sync_file");
	fs.sync_dir(dir).expect("sync_dir");
	assert_eq!(kind_of(fs.sync_file(&dir.join("none"))), io::ErrorKind::NotFound);
	assert_eq!(contents(fs, &path), b"durable");
}

/// `rename` replaces the destination, takes the source away and keeps the bytes.
pub(crate) fn rename(fs: &dyn Fs, dir: &Path) {
	let (from, to) = (dir.join("from"), dir.join("to"));
	make(fs, &from, b"source");
	fs.rename(&from, &to).expect("rename to a new name");
	assert!(!fs.exists(&from));
	assert_eq!(contents(fs, &to), b"source");

	make(fs, &from, b"second");
	fs.rename(&from, &to).expect("rename over a file");
	assert!(!fs.exists(&from));
	assert_eq!(contents(fs, &to), b"second", "the destination is replaced");
	assert_eq!(kind_of(fs.rename(&from, &to)), io::ErrorKind::NotFound);
	assert_eq!(contents(fs, &to), b"second", "a rename that fails changes nothing");
	let names: Vec<_> = fs.read_dir(dir).unwrap().into_iter().map(|e| e.name).collect();
	assert_eq!(names.len(), 1, "one file is left: {names:?}");
}

/// The manifest write: a new file, a longer, a shorter and an empty content, with nothing left
/// over.
pub(crate) fn replace_atomic(fs: &dyn Fs, dir: &Path) {
	let path = dir.join("MANIFEST");
	for content in [&b"first"[..], b"a much longer second content", b"3", b""] {
		fs.replace_atomic(&path, content).expect("replace_atomic");
		assert_eq!(contents(fs, &path), content);
		let names: Vec<_> = fs.read_dir(dir).unwrap().into_iter().map(|e| e.name).collect();
		assert_eq!(names.len(), 1, "no temporary file is left behind: {names:?}");
	}
	// A failure before the commit point is an I/O error that leaves the old content.
	let missing = dir.join("none/MANIFEST");
	match fs.replace_atomic(&missing, b"x") {
		Err(Error::Io(_)) => {}
		other => panic!("replace_atomic in a directory that is not there: {other:?}"),
	}
	assert_eq!(contents(fs, &path), b"");
}

/// A file that is removed while it is open is still read, and its name is gone.
pub(crate) fn unlink_while_open(fs: &dyn Fs, dir: &Path) {
	// A Windows file cannot be removed while it is open.
	if cfg!(windows) && fs.kind() == FsKind::Std {
		return;
	}
	let path = dir.join("f");
	make(fs, &path, b"old bytes");
	let open = fs.open_read(&path).unwrap();
	fs.remove_file(&path).expect("remove_file of an open file");
	assert!(!fs.exists(&path));
	assert_eq!(read_all(&open), b"old bytes", "an open handle keeps reading the old bytes");
	// The name can be used again, for a file of its own.
	make(fs, &path, b"new");
	assert_eq!(contents(fs, &path), b"new");
	assert_eq!(read_all(&open), b"old bytes");
}

/// `read`, `read_prefix`, and `file_len` of whole files.
pub(crate) fn whole_file_helpers(fs: &dyn Fs, dir: &Path) {
	let path = dir.join("f");
	make(fs, &path, b"0123456789");
	assert_eq!(fs.read(&path).unwrap(), b"0123456789");
	assert_eq!(kind_of(fs.read(&dir.join("none"))), io::ErrorKind::NotFound);
	assert_eq!(fs.read_prefix(&path, 4).unwrap().as_deref(), Some(&b"0123"[..]));
	assert_eq!(fs.read_prefix(&path, 10).unwrap().as_deref(), Some(&b"0123456789"[..]));
	assert_eq!(fs.read_prefix(&path, 100).unwrap().as_deref(), Some(&b"0123456789"[..]));
	assert_eq!(fs.read_prefix(&path, 0).unwrap().as_deref(), Some(&b""[..]));
	assert_eq!(fs.read_prefix(&dir.join("none"), 4).unwrap(), None);
	let empty = dir.join("empty");
	make(fs, &empty, b"");
	assert_eq!(fs.read(&empty).unwrap(), b"");
	assert_eq!(fs.read_prefix(&empty, 4).unwrap().as_deref(), Some(&b""[..]));
	assert_eq!(fs.file_len(&path).unwrap(), 10);
}

/// A copy has the bytes of the source, replaces what is at the destination, and leaves the source.
pub(crate) fn copy(fs: &dyn Fs, dir: &Path) {
	let (from, to) = (dir.join("from"), dir.join("to"));
	// Longer than one chunk of the default copy, and not a multiple of it.
	let big: Vec<u8> = (0..150_000u32).map(|i| (i % 251) as u8).collect();
	make(fs, &from, &big);
	assert_eq!(fs.copy(&from, &to).expect("copy"), big.len() as u64);
	assert_eq!(contents(fs, &to), big);
	make(fs, &to, b"something that is longer than nothing, to be replaced");
	make(fs, &from, b"short");
	assert_eq!(fs.copy(&from, &to).expect("copy over a file"), 5);
	assert_eq!(contents(fs, &to), b"short");
	assert_eq!(contents(fs, &from), b"short");
	let empty = dir.join("empty");
	make(fs, &empty, b"");
	assert_eq!(fs.copy(&empty, &to).unwrap(), 0);
	assert_eq!(contents(fs, &to), b"");
	assert_eq!(kind_of(fs.copy(&dir.join("none"), &to)), io::ErrorKind::NotFound);
}

/// One lock per directory: a second is refused while the first is held, and free after a release
/// and after a drop.
pub(crate) fn lock(fs: &dyn Fs, dir: &Path) {
	let mut first = fs.lock_file(dir);
	first.acquire().expect("the first lock");
	let mut second = fs.lock_file(dir);
	assert!(second.acquire().is_err(), "a directory is locked once");
	first.release().expect("release");
	second.acquire().expect("the lock is free after a release");
	drop(second);
	let mut third = fs.lock_file(dir);
	third.acquire().expect("the lock is free after a drop");
	// A lock of another directory is not in the way.
	let other = case_dir(fs, dir, "other");
	let mut elsewhere = fs.lock_file(&other);
	elsewhere.acquire().expect("another directory");
}

/// A file system that has no pool never waits for a file and never refuses one.
pub(crate) fn spares(fs: &dyn Fs) {
	if fs.kind() == FsKind::Std {
		assert!(fs.ensure_spares(0).is_none());
		assert!(fs.ensure_spares(usize::MAX).is_none());
	}
	let before = fs.pool_exhaustions();
	assert_eq!(before, fs.pool_exhaustions());
	// The typed error of a pool is the one that the conversions look for.
	assert!(vfs::is_pool_exhausted(&vfs::pool_exhausted()));
	assert!(!vfs::is_pool_exhausted(&io::Error::other("a different error")));
}
