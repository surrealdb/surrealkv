//! The write-ahead log over the file system seam.
//!
//! The first half pins what the log does to a file today: the writes that a commit group makes, the
//! bytes it leaves, and what the reader makes of them. The numbers in it were taken from the log
//! before it was moved over to the seam, and they did not change by the move. The second half runs
//! the log over a file system that records every call, and over one that is not `std`.

use std::fs;
use std::path::Path;

use tempfile::TempDir;
use test_log::test;

use super::manager::Wal;
use super::reader::Reader;
use super::{Options, BLOCK_SIZE};

/// `len` bytes that depend on `seed` and on the position, so that a byte in the wrong place is
/// seen.
fn payload(seed: usize, len: usize) -> Vec<u8> {
	(0..len).map(|i| ((i * 31 + seed * 7 + i / 251) % 251) as u8 + 1).collect()
}

/// The records of `sizes` as the buffer and the ends that `Wal::append_group` takes.
fn group(seed: usize, sizes: &[usize]) -> (Vec<u8>, Vec<usize>) {
	let mut buf = Vec::new();
	let mut ends = Vec::new();
	for (i, &size) in sizes.iter().enumerate() {
		buf.extend_from_slice(&payload(seed * 100 + i, size));
		ends.push(buf.len());
	}
	(buf, ends)
}

fn crc(bytes: &[u8]) -> u32 {
	crc32fast::hash(bytes)
}

/// The groups the commit path logs, from one small record to a record of several blocks, and what
/// each of them costs in `write(2)` calls. A group is one write when it fits the buffer of the file
/// (one block), and a record that does not fit is written as the buffer and then the rest.
const GROUPS: &[&[usize]] = &[
	&[100],
	&[1024, 1024, 1024],
	&[20_000, 20_000],
	&[70_000],
	&[BLOCK_SIZE - 7 - 7],
	&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
	&[BLOCK_SIZE * 3],
	&[5_000; 12],
];

/// What `GROUPS` made of the file: the `write(2)` calls that each group needed, the length of the
/// file, and the checksum of its bytes.
fn run_groups(dir: &Path, opts: Options, sync_each: bool) -> (Vec<usize>, u64, u32) {
	let mut wal = Wal::open_with_min_log_number(dir, 1, opts).unwrap();
	let mut writes = Vec::new();
	for (i, sizes) in GROUPS.iter().enumerate() {
		let (buf, ends) = group(i, sizes);
		let before = wal.file_writes();
		wal.append_group(&buf, &ends).unwrap();
		if sync_each {
			wal.sync().unwrap();
			assert!(!wal.pending_sync(), "a synced group leaves nothing pending");
		}
		writes.push(wal.file_writes() - before);
	}
	wal.sync().unwrap();
	wal.close().unwrap();
	let bytes = fs::read(dir.join(format!("{:020}.wal", 1))).unwrap();
	(writes, bytes.len() as u64, crc(&bytes))
}

#[test]
fn the_writes_of_a_commit_group_are_the_ones_of_the_log_before_the_seam() {
	let dir = TempDir::new().unwrap();
	let (writes, len, checksum) = run_groups(dir.path(), Options::default(), true);
	assert_eq!(writes, GOLDEN_WRITES, "the write(2) calls of each group changed");
	assert_eq!(len, GOLDEN_LEN, "the length of the segment changed");
	assert_eq!(checksum, GOLDEN_CRC, "the bytes of the segment changed");
}

#[test]
fn syncing_after_each_group_or_once_leaves_the_same_segment() {
	let each = TempDir::new().unwrap();
	let once = TempDir::new().unwrap();
	let (_, len_each, crc_each) = run_groups(each.path(), Options::default(), true);
	let (_, len_once, crc_once) = run_groups(once.path(), Options::default(), false);
	assert_eq!((len_each, crc_each), (len_once, crc_once));
}

/// The records of the segment as the reader returns them: the offset after each, and a checksum of
/// all the payloads.
fn read_all(reader: &mut Reader) -> (Vec<u64>, u32, String) {
	let mut offsets = Vec::new();
	let mut all = Vec::new();
	let end = loop {
		match reader.read() {
			Ok((record, offset)) => {
				offsets.push(offset);
				all.extend_from_slice(record);
			}
			Err(e) => break format!("{e}"),
		}
	};
	(offsets, crc(&all), end)
}

#[test]
fn the_reader_returns_the_records_that_the_groups_wrote_at_the_offsets_it_always_did() {
	let dir = TempDir::new().unwrap();
	run_groups(dir.path(), Options::default(), true);
	let file = fs::File::open(dir.path().join(format!("{:020}.wal", 1))).unwrap();
	let mut reader = Reader::new(file);
	let (offsets, checksum, end) = read_all(&mut reader);
	let records: usize = GROUPS.iter().map(|group| group.len()).sum();
	assert_eq!(offsets.len(), records);
	assert!(offsets.windows(2).all(|pair| pair[0] < pair[1]));
	assert_eq!(
		crc(&offsets.iter().flat_map(|o| o.to_be_bytes()).collect::<Vec<_>>()),
		GOLDEN_OFFSETS
	);
	assert_eq!(checksum, GOLDEN_PAYLOADS);
	assert!(end.contains("end of file"), "a clean segment ends with an end of file: {end}");
}

const GOLDEN_WRITES: &[usize] = &[1, 1, 2, 3, 1, 1, 4, 2];
const GOLDEN_LEN: u64 = 304565;
const GOLDEN_CRC: u32 = 2652922565;
const GOLDEN_OFFSETS: u32 = 2508072385;
const GOLDEN_PAYLOADS: u32 = 2904172601;

// ---- the compression of an existing segment --------------------------------------------------

/// The 7 bytes of a record header of `kind` with a payload of `length` and a checksum that is not
/// looked at.
fn header_of(kind: u8, length: u16) -> Vec<u8> {
	let mut header = vec![0u8; 4];
	header.extend_from_slice(&length.to_be_bytes());
	header.push(kind);
	header
}

fn detect(bytes: &[u8]) -> super::Result<super::CompressionType> {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join(format!("{:020}.wal", 1));
	fs::write(&path, bytes).unwrap();
	Wal::detect_compression_type(&path)
}

#[test]
fn the_compression_of_a_segment_is_read_from_its_first_record() {
	use super::{CompressionType, Error, RecordType};

	assert_eq!(detect(&[]).unwrap(), CompressionType::None);
	assert_eq!(detect(&[0, 0, 0]).unwrap(), CompressionType::None, "a header that is cut short");

	let mut lz4 = header_of(RecordType::SetCompressionType as u8, 1);
	lz4.push(CompressionType::Lz4 as u8);
	assert_eq!(detect(&lz4).unwrap(), CompressionType::Lz4);

	let mut none = header_of(RecordType::SetCompressionType as u8, 1);
	none.push(CompressionType::None as u8);
	assert_eq!(detect(&none).unwrap(), CompressionType::None);

	// A full record first: no compression record, so none.
	let mut full = header_of(RecordType::Full as u8, 3);
	full.extend_from_slice(&[1, 2, 3]);
	assert_eq!(detect(&full).unwrap(), CompressionType::None);

	// A compression record whose byte is missing: the open fails, as it did.
	let cut = header_of(RecordType::SetCompressionType as u8, 1);
	match detect(&cut) {
		Err(Error::IO(e)) => assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof),
		other => panic!("expected an end of file, got {other:?}"),
	}

	// A compression record with a byte that names no compression.
	let mut bad = header_of(RecordType::SetCompressionType as u8, 1);
	bad.push(77);
	assert!(matches!(detect(&bad), Err(Error::IO(_))));

	// A type that names no record.
	assert!(matches!(detect(&header_of(77, 0)), Err(Error::IO(_))));

	// A sealed segment, by the type of its first record or by the magic that starts its payload.
	let sealed_by_type = header_of(RecordType::SegmentHeader as u8, 0);
	assert!(matches!(detect(&sealed_by_type), Err(Error::Encryption(_))));
	let mut sealed_by_magic = header_of(RecordType::Full as u8, 4);
	sealed_by_magic.extend_from_slice(&crate::tde::HEADER_MAGIC);
	assert!(matches!(detect(&sealed_by_magic), Err(Error::Encryption(_))));

	// Exactly a header and fewer than four bytes after it: the magic is not looked for, and the
	// record is read as it is.
	let mut short_payload = header_of(RecordType::Full as u8, 2);
	short_payload.extend_from_slice(&[9, 9]);
	assert_eq!(detect(&short_payload).unwrap(), CompressionType::None);
}

// ---- a file system that records every call ----------------------------------------------------

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use super::recovery::{
	classify_wal_damage_in,
	repair_corrupted_wal_segment_in,
	replay_wal_inner_with,
	scan_valid_records_after_in,
	WalDamage,
};
use super::{
	cleanup_old_segments_for_in,
	list_segment_ids_in,
	CompressionType,
	Error as WalError,
	WalContext,
};
use crate::error::Error;
use crate::lockfile::LockFile;
use crate::vfs::{
	CreateOpts,
	DirEntry,
	EntryKind,
	File as VfsFile,
	Fs,
	FsFile,
	FsKind,
	FsWriter,
	StdFs,
};

/// One call that reached a file or the file system, with the name of the file where there is one.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Op {
	Open(String),
	Append(usize),
	SetLen(u64),
	Sync,
	SyncFile(String),
	SyncDir,
	Remove(String),
	Rename(String, String),
	Copy(String, String),
}

type Log = Arc<Mutex<Vec<Op>>>;

fn name_of(path: &Path) -> String {
	path.file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default()
}

/// `StdFs` that says what it is asked, and answers as `kind` says. A `Pool` kind is the one the
/// log must not take for `std`: no chmod, no sealed log, and the copies of a repair are reclaimed.
#[derive(Debug)]
struct RecordingFs {
	kind: FsKind,
	log: Log,
	/// Makes `entry` and `try_exists` fail with a permission error, as a directory that cannot be
	/// looked at does.
	blind: bool,
	/// The most that a read of a file that this opens returns.
	cap: usize,
	/// While set, a creation fails as a pool that has no free file does.
	exhausted: Arc<AtomicBool>,
}

impl RecordingFs {
	fn recording(kind: FsKind) -> (Arc<dyn Fs>, Log) {
		let log = Log::default();
		let fs = Self {
			log: Arc::clone(&log),
			..Self::plain(kind)
		};
		(Arc::new(fs), log)
	}

	fn plain(kind: FsKind) -> Self {
		Self {
			kind,
			log: Log::default(),
			blind: false,
			cap: usize::MAX,
			exhausted: Arc::default(),
		}
	}

	/// A file system whose creations fail, as those of a pool that has no free file do, while the
	/// returned switch is set.
	fn exhaustible(kind: FsKind) -> (Arc<dyn Fs>, Arc<AtomicBool>) {
		let fs = Self::plain(kind);
		let switch = Arc::clone(&fs.exhausted);
		(Arc::new(fs), switch)
	}

	fn check_exhausted(&self) -> io::Result<()> {
		if self.exhausted.load(Ordering::SeqCst) {
			return Err(crate::vfs::pool_exhausted());
		}
		Ok(())
	}

	fn blind(kind: FsKind) -> Arc<Self> {
		Arc::new(Self {
			blind: true,
			..Self::plain(kind)
		})
	}

	fn wrap(&self, path: &Path, file: Arc<dyn FsFile>) -> Arc<dyn FsFile> {
		self.log.lock().push(Op::Open(name_of(path)));
		let file: Arc<dyn FsFile> = Arc::new(RecordingFile {
			inner: file,
			log: Arc::clone(&self.log),
		});
		if self.cap == usize::MAX {
			file
		} else {
			Arc::new(Capped {
				inner: file,
				cap: self.cap,
			})
		}
	}

	fn denied() -> io::Error {
		io::Error::new(io::ErrorKind::PermissionDenied, "cannot look")
	}
}

struct RecordingFile {
	inner: Arc<dyn FsFile>,
	log: Log,
}

impl VfsFile for RecordingFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> crate::Result<usize> {
		self.inner.read_at(offset, buf)
	}

	fn size(&self) -> crate::Result<u64> {
		self.inner.size()
	}
}

impl FsFile for RecordingFile {
	fn len(&self) -> io::Result<u64> {
		self.inner.len()
	}

	fn append(&self, buf: &[u8]) -> io::Result<()> {
		self.log.lock().push(Op::Append(buf.len()));
		self.inner.append(buf)
	}

	fn set_len(&self, len: u64) -> io::Result<()> {
		self.log.lock().push(Op::SetLen(len));
		self.inner.set_len(len)
	}

	fn sync(&self) -> io::Result<()> {
		self.log.lock().push(Op::Sync);
		self.inner.sync()
	}
}

impl Fs for RecordingFs {
	fn kind(&self) -> FsKind {
		self.kind
	}

	fn entry(&self, path: &Path) -> io::Result<Option<EntryKind>> {
		if self.blind {
			return Err(Self::denied());
		}
		StdFs.entry(path)
	}

	fn try_exists(&self, path: &Path) -> io::Result<bool> {
		if self.blind {
			return Err(Self::denied());
		}
		StdFs.try_exists(path)
	}

	fn exists(&self, path: &Path) -> bool {
		self.try_exists(path).unwrap_or(false)
	}

	fn create_dir_all(&self, path: &Path) -> io::Result<()> {
		StdFs.create_dir_all(path)
	}

	fn read_dir(&self, path: &Path) -> io::Result<Vec<DirEntry>> {
		StdFs.read_dir(path)
	}

	fn remove_file(&self, path: &Path) -> io::Result<()> {
		self.log.lock().push(Op::Remove(name_of(path)));
		StdFs.remove_file(path)
	}

	fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
		StdFs.remove_dir_all(path)
	}

	fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
		self.log.lock().push(Op::Rename(name_of(from), name_of(to)));
		StdFs.rename(from, to)
	}

	fn sync_dir(&self, path: &Path) -> io::Result<()> {
		self.log.lock().push(Op::SyncDir);
		StdFs.sync_dir(path)
	}

	fn open_read(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		Ok(self.wrap(path, StdFs.open_read(path)?))
	}

	fn open_append(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		if !self.exists(path) {
			self.check_exhausted()?;
		}
		Ok(self.wrap(path, StdFs.open_append(path, opts)?))
	}

	fn open_append_existing(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		Ok(self.wrap(path, StdFs.open_append_existing(path)?))
	}

	fn create_new(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.check_exhausted()?;
		Ok(self.wrap(path, StdFs.create_new(path, opts)?))
	}

	fn create_truncate(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		Ok(self.wrap(path, StdFs.create_truncate(path, opts)?))
	}

	fn sync_file(&self, path: &Path) -> io::Result<()> {
		self.log.lock().push(Op::SyncFile(name_of(path)));
		StdFs.sync_file(path)
	}

	fn copy(&self, from: &Path, to: &Path) -> io::Result<u64> {
		self.log.lock().push(Op::Copy(name_of(from), name_of(to)));
		StdFs.copy(from, to)
	}

	fn replace_atomic(&self, path: &Path, content: &[u8]) -> crate::Result<()> {
		StdFs.replace_atomic(path, content)
	}

	fn lock_file(&self, dir: &Path) -> LockFile {
		StdFs.lock_file(dir)
	}
}

fn options_over(fs: Arc<dyn Fs>) -> Options {
	let mut opts = Options::default();
	opts.context.fs = fs;
	opts
}

fn context_over(fs: Arc<dyn Fs>) -> WalContext {
	let mut context = WalContext::plaintext();
	context.fs = fs;
	context
}

fn the_log(log: &Log) -> Vec<Op> {
	std::mem::take(&mut *log.lock())
}

#[test]
fn a_commit_group_is_one_append_per_write_and_one_sync_after_them() {
	let dir = TempDir::new().unwrap();
	let (fs, log) = RecordingFs::recording(FsKind::Std);
	let mut wal = Wal::open_with_min_log_number(dir.path(), 1, options_over(fs)).unwrap();
	the_log(&log);

	let mut writes = Vec::new();
	for (i, sizes) in GROUPS.iter().enumerate() {
		let (buf, ends) = group(i, sizes);
		let before = wal.file_writes();
		wal.append_group(&buf, &ends).unwrap();
		// A commit group that does not sync reaches the file and goes no further.
		let appended = the_log(&log);
		assert!(appended.iter().all(|op| matches!(op, Op::Append(_))), "{appended:?}");
		assert_eq!(appended.len(), wal.file_writes() - before, "group {i}");
		wal.sync().unwrap();
		assert_eq!(the_log(&log), vec![Op::Sync], "the sync of group {i} is one fsync");
		writes.push(appended.len());
	}
	assert_eq!(writes, GOLDEN_WRITES);

	// A sync with nothing written since the last one is not an fsync.
	wal.sync().unwrap();
	assert_eq!(the_log(&log), vec![]);

	// The appends of a group are the framed bytes of its records, in order, and nothing else.
	wal.close().unwrap();
	let bytes = fs::read(dir.path().join(format!("{:020}.wal", 1))).unwrap();
	assert_eq!((bytes.len() as u64, crc(&bytes)), (GOLDEN_LEN, GOLDEN_CRC));
}

#[test]
fn opening_a_segment_makes_the_calls_of_before_and_no_second_handle() {
	let dir = TempDir::new().unwrap();
	let (fs, log) = RecordingFs::recording(FsKind::Std);

	// A new segment: one open (the handle that the writer and the fsync share), no more.
	let wal = Wal::open_with_min_log_number(dir.path(), 1, options_over(Arc::clone(&fs))).unwrap();
	assert_eq!(the_log(&log), vec![Op::Open(format!("{:020}.wal", 1))]);
	drop(wal);
	the_log(&log);

	// An existing one is opened once as well: the compression is read from that handle.
	let _wal = Wal::open_with_min_log_number(dir.path(), 1, options_over(fs)).unwrap();
	let opened = the_log(&log);
	assert_eq!(
		opened.iter().filter(|op| matches!(op, Op::Open(_))).count(),
		1,
		"a segment that is reopened needs one handle: {opened:?}"
	);
}

/// A writer of the shape that `BufWriter<File>` was: the sizes of the `write` calls it makes.
struct Recorder(Arc<Mutex<Vec<usize>>>);

impl io::Write for Recorder {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		self.0.lock().push(buf.len());
		Ok(buf.len())
	}

	fn flush(&mut self) -> io::Result<()> {
		Ok(())
	}
}

#[test]
fn the_buffer_writes_what_a_buffer_over_the_file_wrote() {
	use std::io::Write;

	use super::{BufferedFileWriter, WritableFile};

	enum Step {
		Append(usize),
		Flush,
		Sync,
	}
	let steps = [
		Step::Append(1),
		Step::Append(100),
		Step::Flush,
		Step::Append(BLOCK_SIZE - 1),
		Step::Append(1),
		Step::Append(BLOCK_SIZE),
		Step::Append(BLOCK_SIZE + 1),
		Step::Append(7),
		Step::Sync,
		Step::Append(0),
		Step::Flush,
		Step::Append(3 * BLOCK_SIZE + 5),
		Step::Append(BLOCK_SIZE / 2),
		Step::Append(BLOCK_SIZE / 2),
		Step::Append(BLOCK_SIZE / 2),
		Step::Flush,
		Step::Sync,
	];

	let dir = TempDir::new().unwrap();
	let (fs, log) = RecordingFs::recording(FsKind::Std);
	let file = fs.create_truncate(&dir.path().join("w.wal"), CreateOpts::default()).unwrap();
	the_log(&log);
	let mut writer = BufferedFileWriter::new(file, BLOCK_SIZE);

	let seen = Arc::new(Mutex::new(Vec::new()));
	let mut oracle = std::io::BufWriter::with_capacity(BLOCK_SIZE, Recorder(Arc::clone(&seen)));

	for (i, step) in steps.iter().enumerate() {
		match step {
			Step::Append(len) => {
				let data = payload(i, *len);
				writer.append(&data).unwrap();
				oracle.write_all(&data).unwrap();
			}
			Step::Flush => {
				writer.flush().unwrap();
				oracle.flush().unwrap();
			}
			Step::Sync => {
				writer.sync().unwrap();
				oracle.flush().unwrap();
			}
		}
		let appended: Vec<usize> = the_log(&log)
			.into_iter()
			.filter_map(|op| {
				if let Op::Append(len) = op {
					Some(len)
				} else {
					None
				}
			})
			.collect();
		let expected: Vec<usize> = std::mem::take(&mut *seen.lock());
		assert_eq!(appended, expected, "step {i}");
	}
}

// ---- reads that come back short ---------------------------------------------------------------

/// A file that gives at most `cap` bytes to a read, as `pread` may.
struct Capped {
	inner: Arc<dyn FsFile>,
	cap: usize,
}

impl VfsFile for Capped {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> crate::Result<usize> {
		let n = buf.len().min(self.cap);
		self.inner.read_at(offset, &mut buf[..n])
	}

	fn size(&self) -> crate::Result<u64> {
		self.inner.size()
	}
}

impl FsFile for Capped {
	fn len(&self) -> io::Result<u64> {
		self.inner.len()
	}

	fn append(&self, buf: &[u8]) -> io::Result<()> {
		self.inner.append(buf)
	}

	fn set_len(&self, len: u64) -> io::Result<()> {
		self.inner.set_len(len)
	}

	fn sync(&self) -> io::Result<()> {
		self.inner.sync()
	}
}

fn capped(path: &Path, cap: usize) -> Arc<dyn FsFile> {
	Arc::new(Capped {
		inner: StdFs.open_read(path).unwrap(),
		cap,
	})
}

#[test]
fn a_reader_that_gets_short_reads_returns_what_one_that_gets_full_reads_does() {
	let dir = TempDir::new().unwrap();
	run_groups(dir.path(), Options::default(), true);
	let whole = fs::read(dir.path().join(format!("{:020}.wal", 1))).unwrap();

	// The segment as it is, and cut at the offsets where a crash leaves it: inside a header, at
	// and around a block, in the middle of a fragmented record, and a byte short.
	let cuts = [
		whole.len(),
		whole.len() - 1,
		whole.len() - 100,
		BLOCK_SIZE + 3,
		BLOCK_SIZE,
		BLOCK_SIZE - 1,
		40_000,
		9,
		7,
		3,
		0,
	];
	for cut in cuts {
		let path = dir.path().join(format!("cut-{cut}.wal"));
		fs::write(&path, &whole[..cut]).unwrap();
		let reference = read_all(&mut Reader::new(capped(&path, usize::MAX)));
		for cap in [1, 2, 7, 4097, BLOCK_SIZE - 1, BLOCK_SIZE, BLOCK_SIZE + 1] {
			let got = read_all(&mut Reader::new(capped(&path, cap)));
			assert_eq!(got, reference, "cut at {cut}, reads of at most {cap} bytes");
		}
		// And the reader of before: a `std::fs::File` read by `read(2)`.
		let by_file = read_all(&mut Reader::new(fs::File::open(&path).unwrap()));
		assert_eq!(by_file, reference, "cut at {cut}, a std file");
	}
}

#[test]
fn the_compression_of_a_segment_is_read_the_same_through_short_reads() {
	let mut lz4 = header_of(super::RecordType::SetCompressionType as u8, 1);
	lz4.push(CompressionType::Lz4 as u8);
	let mut magic = header_of(super::RecordType::Full as u8, 4);
	magic.extend_from_slice(&crate::tde::HEADER_MAGIC);
	let dir = TempDir::new().unwrap();
	for (name, bytes) in [("lz4", lz4), ("magic", magic)] {
		let path = dir.path().join(name);
		fs::write(&path, &bytes).unwrap();
		let reference = Wal::detect_compression_of(&*capped(&path, usize::MAX));
		for cap in [1, 2, 3, 7] {
			let got = Wal::detect_compression_of(&*capped(&path, cap));
			assert_eq!(got, reference, "{name}, reads of at most {cap} bytes");
		}
	}
}

// ---- the decisions that a file system that is not std takes differently -----------------------

#[test]
fn a_directory_that_cannot_be_looked_at_is_not_an_empty_log_on_a_pool() {
	let dir = TempDir::new().unwrap();
	run_groups(dir.path(), Options::default(), true);

	// A pool says so.
	let context = context_over(RecordingFs::blind(FsKind::Pool));
	match replay_wal_inner_with(&context, dir.path(), 0, 1 << 20) {
		Err(Error::Io(e)) => assert_eq!(e.kind(), io::ErrorKind::PermissionDenied),
		other => {
			panic!("a replay that cannot look at the log must fail, got {:?}", other.map(|_| ()))
		}
	}

	// `std` still reads it as absent, as `Path::exists` always did (see `directory_present`).
	let context = context_over(RecordingFs::blind(FsKind::Std));
	let (max_seq, memtables, ..) = replay_wal_inner_with(&context, dir.path(), 0, 1 << 20).unwrap();
	assert!(max_seq.is_none() && memtables.is_empty());

	// A directory that is not there is empty on both.
	let gone = dir.path().join("gone");
	for kind in [FsKind::Std, FsKind::Pool] {
		let (fs, _) = RecordingFs::recording(kind);
		let (max_seq, memtables, ..) =
			replay_wal_inner_with(&context_over(Arc::clone(&fs)), &gone, 0, 1 << 20).unwrap();
		assert!(max_seq.is_none() && memtables.is_empty());
		assert_eq!(cleanup_old_segments_for_in(None, &*fs, &gone, 5).unwrap(), 0);
	}
}

#[test]
fn an_open_that_cannot_list_the_directory_fails_and_makes_no_segment() {
	let dir = TempDir::new().unwrap();
	// A file whose name is not a segment's fails the listing, and the open with it: it used to
	// read as "no segments", and the log then began at the number it was given, whatever was there.
	fs::write(dir.path().join("stray.txt"), b"x").unwrap();
	match Wal::open_with_min_log_number(dir.path(), 7, Options::default()) {
		Err(WalError::IO(e)) => assert_eq!(e.kind(), io::ErrorKind::InvalidInput),
		other => panic!("expected the open to fail, got {:?}", other.map(|_| ())),
	}
	assert!(!dir.path().join(format!("{:020}.wal", 7)).exists());

	// What the listing says of the odd name, and of a directory that is not there.
	assert!(list_segment_ids_in(&StdFs, dir.path(), Some("wal")).is_err());
	let gone = dir.path().join("gone");
	match list_segment_ids_in(&StdFs, &gone, Some("wal")) {
		Err(WalError::IO(e)) => assert_eq!(e.kind(), io::ErrorKind::NotFound),
		other => panic!("expected NotFound, got {other:?}"),
	}
}

#[test]
fn the_log_makes_no_directory_change_on_a_pool() {
	// `chmod` of the directory is for `std`: another file system has no permissions to set.
	use std::os::unix::fs::PermissionsExt;
	let dir = TempDir::new().unwrap();
	fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700)).unwrap();
	let (fs, _) = RecordingFs::recording(FsKind::Pool);
	let _wal = Wal::open_with_min_log_number(dir.path(), 1, options_over(fs)).unwrap();
	let mode = fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777;
	assert_eq!(mode, 0o700);

	let other = TempDir::new().unwrap();
	fs::set_permissions(other.path(), fs::Permissions::from_mode(0o700)).unwrap();
	let _wal = Wal::open_with_min_log_number(other.path(), 1, Options::default()).unwrap();
	let mode = fs::metadata(other.path()).unwrap().permissions().mode() & 0o777;
	assert_eq!(mode, 0o750, "on std the directory gets the mode of the options, as before");
}

#[test]
fn a_sealed_log_is_refused_on_a_file_system_that_is_not_std() {
	use crate::test::wal_sealed_tests::{context, sealed_options};

	let enc = context(crate::tde::CipherSuite::Aes256Gcm);
	let dir = TempDir::new().unwrap();
	let (fs, _) = RecordingFs::recording(FsKind::Pool);
	let mut opts = sealed_options(&enc, CompressionType::None);
	opts.context.fs = Arc::clone(&fs);
	match Wal::open_with_min_log_number(dir.path(), 1, opts) {
		Err(WalError::IO(e)) => assert_eq!(e.kind(), io::ErrorKind::Unsupported),
		other => panic!("expected a refusal, got {:?}", other.map(|_| ())),
	}
	// It made nothing, and a cleanup that would have to look at sealed segments through `std`
	// does not delete what it cannot read.
	fs::write(dir.path().join(format!("{:020}.wal", 1)), b"x").unwrap();
	assert!(cleanup_old_segments_for_in(Some(&enc), &*fs, dir.path(), 5).is_err());
	assert!(dir.path().join(format!("{:020}.wal", 1)).exists());
}

// ---- the repair of a torn tail ----------------------------------------------------------------

/// A directory with the segment 1 of a few records, followed by `garbage` bytes that are not
/// a record: what a torn write leaves. Returns the length of the good part.
fn torn_segment(dir: &Path, id: u64, garbage: usize) -> u64 {
	let mut wal = Wal::open_with_min_log_number(dir, id, Options::default()).unwrap();
	for seq in 1..=3u64 {
		wal.append(&crate::test::wal_sealed_tests::encoded_batch(seq, "torn", 3, 100)).unwrap();
	}
	wal.sync().unwrap();
	wal.close().unwrap();
	drop(wal);
	let path = dir.join(format!("{id:020}.wal"));
	let good = fs::metadata(&path).unwrap().len();
	let mut bytes = fs::read(&path).unwrap();
	bytes.extend(std::iter::repeat_n(0xAB, garbage));
	fs::write(&path, bytes).unwrap();
	good
}

#[test]
fn the_copy_of_a_repair_of_a_pool_is_synced_before_the_segment_is_cut_and_that_of_std_is_not() {
	for kind in [FsKind::Std, FsKind::Pool] {
		let dir = TempDir::new().unwrap();
		let good = torn_segment(dir.path(), 1, 50);
		let (fs, log) = RecordingFs::recording(kind);

		let damage = classify_wal_damage_in(
			&*fs,
			&dir.path().join(format!("{:020}.wal", 1)),
			good as usize,
			true,
		)
		.unwrap();
		assert_eq!(
			damage,
			WalDamage::TornTail {
				last_valid_offset: good as usize,
				file_size: good + 50
			}
		);
		the_log(&log);

		let (quarantined, trimmed) =
			repair_corrupted_wal_segment_in(&*fs, dir.path(), 1, good as usize).unwrap();
		assert_eq!(trimmed, 50);
		let calls = the_log(&log);
		let corrupt = format!("{:020}.wal.corrupt", 1);
		let at = |wanted: &Op| calls.iter().position(|op| op == wanted);
		let copied = at(&Op::Copy(format!("{:020}.wal", 1), corrupt.clone())).expect("copied");
		let cut = at(&Op::SetLen(good)).expect("cut back");
		if kind == FsKind::Std {
			// Native recovery gains no fsync: the copy was never synced by name.
			assert!(at(&Op::SyncFile(corrupt)).is_none(), "std does not sync the copy: {calls:?}");
			assert!(copied < cut, "copy, cut: {calls:?}");
		} else {
			let synced = at(&Op::SyncFile(corrupt)).expect("the copy is synced");
			assert!(copied < synced && synced < cut, "copy, sync of the copy, cut: {calls:?}");
		}
		// And the cut is synced, then the directory.
		assert!(
			calls[cut..].contains(&Op::Sync) && calls.last() == Some(&Op::SyncDir),
			"{calls:?}"
		);
		assert_eq!(fs::metadata(dir.path().join(format!("{:020}.wal", 1))).unwrap().len(), good);
		assert_eq!(fs::metadata(&quarantined).unwrap().len(), good + 50);
	}
}

#[test]
fn a_scan_finds_the_same_records_through_short_reads() {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join(format!("{:020}.wal", 1));
	torn_segment(dir.path(), 1, 0);
	// The first of the three records fails its checksum: the other two are what a scan finds after
	// it, which is what makes damage interior and not a torn tail.
	let mut bytes = fs::read(&path).unwrap();
	bytes[super::HEADER_SIZE + 10] ^= 0xFF;
	fs::write(&path, &bytes).unwrap();

	let reference = scan_valid_records_after_in(&StdFs, &path, 0).unwrap();
	assert_eq!(reference, 2);
	for cap in [1, 7, 4097] {
		let fs = Arc::new(RecordingFs {
			cap,
			..RecordingFs::plain(FsKind::Std)
		});
		assert_eq!(scan_valid_records_after_in(&*fs, &path, 0).unwrap(), reference, "cap {cap}");
		let damage = classify_wal_damage_in(&*fs, &path, 0, true).unwrap();
		assert_eq!(
			damage,
			WalDamage::InteriorCorruption {
				last_valid_offset: 0,
				intact_records_after: 2,
				is_last_segment: true,
				file_size: bytes.len() as u64,
			}
		);
	}
}

#[test]
fn the_copies_of_repairs_are_reclaimed_on_a_pool_and_kept_on_std() {
	for (kind, kept) in [(FsKind::Pool, false), (FsKind::Std, true)] {
		let dir = TempDir::new().unwrap();
		let (fs, _) = RecordingFs::recording(kind);
		let mut high_water = 0;
		// A crash and a reopen, twenty times: each repairs a torn tail of its own segment, and the
		// flush that follows the replay moves the log number past it.
		for id in 1..=20u64 {
			let good = torn_segment(dir.path(), id, 30);
			repair_corrupted_wal_segment_in(&*fs, dir.path(), id as usize, good as usize).unwrap();
			cleanup_old_segments_for_in(None, &*fs, dir.path(), id + 1).unwrap();
			let copies = fs::read_dir(dir.path())
				.unwrap()
				.filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().contains(".corrupt"))
				.count();
			high_water = high_water.max(copies);
		}
		if kept {
			assert_eq!(high_water, 20, "std keeps every copy, as it always did");
		} else {
			assert!(
				high_water <= 1,
				"a pool holds the copies of the segments not yet flushed: {high_water}"
			);
		}
		// The segments below the log number went in both.
		assert!(!dir.path().join(format!("{:020}.wal", 5)).exists());
	}
}

#[test]
fn a_pool_keeps_the_copies_of_the_segments_that_are_not_in_tables_yet() {
	let dir = TempDir::new().unwrap();
	let (fs, _) = RecordingFs::recording(FsKind::Pool);
	for id in [3u64, 4, 6] {
		let good = torn_segment(dir.path(), id, 30);
		repair_corrupted_wal_segment_in(&*fs, dir.path(), id as usize, good as usize).unwrap();
	}
	// A second repair of 4 makes `.corrupt.1`.
	torn_segment_again(dir.path(), 4);
	let names = || {
		let mut names: Vec<String> = fs::read_dir(dir.path())
			.unwrap()
			.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
			.collect();
		names.sort();
		names
	};
	cleanup_old_segments_for_in(None, &*fs, dir.path(), 5).unwrap();
	let left = names();
	assert!(
		left.iter().any(|name| name.starts_with(&format!("{:020}.wal.corrupt", 6))),
		"{left:?}"
	);
	assert!(
		!left.iter().any(|name| name.starts_with(&format!("{:020}.wal.corrupt", 3))),
		"{left:?}"
	);
	assert!(
		!left.iter().any(|name| name.starts_with(&format!("{:020}.wal.corrupt", 4))),
		"{left:?}"
	);
}

fn torn_segment_again(dir: &Path, id: u64) {
	let path = dir.join(format!("{id:020}.wal"));
	let mut bytes = fs::read(&path).unwrap();
	bytes.extend(std::iter::repeat_n(0xCD, 20));
	fs::write(&path, bytes).unwrap();
	let good = bytes_len(&path) - 20;
	let fs = StdFs;
	repair_corrupted_wal_segment_in(&fs, dir, id as usize, good as usize).unwrap();
}

fn bytes_len(path: &Path) -> u64 {
	fs::metadata(path).unwrap().len()
}

#[test]
fn a_writer_over_a_file_cuts_back_through_the_one_handle() {
	use super::{BufferedFileWriter, WritableFile};

	let dir = TempDir::new().unwrap();
	let (fs, log) = RecordingFs::recording(FsKind::Std);
	let file = fs.open_append(&dir.path().join("w.wal"), CreateOpts::default()).unwrap();
	let mut writer = BufferedFileWriter::new(Arc::clone(&file), BLOCK_SIZE);
	writer.append(&payload(1, 1000)).unwrap();
	writer.flush().unwrap();
	writer.append(&payload(2, 500)).unwrap();
	the_log(&log);

	writer.truncate(1000).unwrap();
	// The buffered 500 bytes are dropped and not written, the file is cut and synced, in that
	// order, on the handle that the writer has. Nothing else is opened.
	assert_eq!(the_log(&log), vec![Op::SetLen(1000), Op::Sync]);
	assert_eq!(file.len().unwrap(), 1000);
	writer.append(&payload(3, 10)).unwrap();
	writer.flush().unwrap();
	assert_eq!(file.len().unwrap(), 1010, "appends continue from the cut");

	// An `FsWriter` over the same file sees the same end.
	FsWriter(Arc::clone(&file)).0.append(b"x").unwrap();
	assert_eq!(file.len().unwrap(), 1011);
}

#[test]
fn a_pool_with_no_free_file_fails_a_rotation_with_the_typed_error_and_leaves_the_log_as_it_was() {
	let dir = TempDir::new().unwrap();
	let (fs, exhausted) = RecordingFs::exhaustible(FsKind::Pool);
	let mut wal = Wal::open_with_min_log_number(dir.path(), 1, options_over(fs)).unwrap();
	let (buf, ends) = group(1, &[100, 200]);
	wal.append_group(&buf, &ends).unwrap();

	exhausted.store(true, Ordering::SeqCst);
	assert_eq!(wal.rotate().unwrap_err(), WalError::PoolExhausted);
	assert_eq!(wal.get_active_log_number(), 1, "a rotation that failed changed nothing");
	assert!(!dir.path().join(format!("{:020}.wal", 2)).exists());

	// The segment still takes commits, and the rotation succeeds once there is a file.
	wal.append_group(&buf, &ends).unwrap();
	wal.sync().unwrap();
	exhausted.store(false, Ordering::SeqCst);
	assert_eq!(wal.rotate().unwrap(), 2);
	wal.append_group(&buf, &ends).unwrap();

	// An open that finds none says so as well, and the number it would have used stays free.
	exhausted.store(true, Ordering::SeqCst);
	let other = TempDir::new().unwrap();
	let (fs, exhausted) = RecordingFs::exhaustible(FsKind::Pool);
	exhausted.store(true, Ordering::SeqCst);
	let opened = Wal::open_with_min_log_number(other.path(), 1, options_over(fs));
	assert_eq!(opened.err(), Some(WalError::PoolExhausted));
}

// ---- the name of the copy of a repair ---------------------------------------------------------

#[test]
fn the_name_of_the_copy_of_a_repair_is_durable_before_the_segment_is_cut() {
	// A copy whose directory entry is not durable can be gone after a power cut that also keeps
	// the cut of the original: the directory is synced between the two, on every kind of file
	// system. (A pool cannot see the difference: its copy syncs the slot that holds the name, so
	// this runs over `std`, where the order of the calls is the only evidence.)
	for kind in [FsKind::Std, FsKind::Pool] {
		let dir = TempDir::new().unwrap();
		let good = torn_segment(dir.path(), 1, 50);
		let (fs, log) = RecordingFs::recording(kind);
		repair_corrupted_wal_segment_in(&*fs, dir.path(), 1, good as usize).unwrap();
		let calls = the_log(&log);
		let corrupt = format!("{:020}.wal.corrupt", 1);
		let copied =
			calls.iter().position(|op| matches!(op, Op::Copy(_, to) if *to == corrupt)).unwrap();
		let cut = calls.iter().position(|op| *op == Op::SetLen(good)).unwrap();
		assert!(
			calls[copied..cut].contains(&Op::SyncDir),
			"the directory is synced after the copy is made and before the cut: {calls:?}"
		);
	}
}

#[test]
fn the_repair_of_a_segment_that_has_gone_creates_nothing() {
	// A segment that is not there is not made by opening it to cut it: `NotFound`, and not a new
	// empty file that the log would then take for a segment. The repair itself looks first and says
	// so; the open that follows looks again, and says the same.
	let dir = TempDir::new().unwrap();
	let path = dir.path().join(format!("{:020}.wal", 7));
	assert_eq!(
		StdFs.open_append_existing(&path).err().map(|e| e.kind()),
		Some(io::ErrorKind::NotFound)
	);
	assert!(!path.exists(), "nothing was created");
	let error = repair_corrupted_wal_segment_in(&StdFs, dir.path(), 7, 0).unwrap_err();
	assert!(error.to_string().contains("does not exist"), "{error}");
	assert!(!path.exists());
}

// ---- a writer whose write failed --------------------------------------------------------------

/// A pool file over memory, and the backend to make it fail.
fn pool_file(path: &str) -> (crate::vfs::MemRaw, Arc<dyn FsFile>) {
	use crate::vfs::{MemRaw, PoolConfig, PoolFs};

	let raw = MemRaw::new();
	let (fs, _) = PoolFs::mount_sync(raw.clone(), PoolConfig::memory()).unwrap();
	fs.create_dir_all(Path::new("/w")).unwrap();
	let file = fs.create_new(Path::new(path), CreateOpts::default()).unwrap();
	// The pool lives as long as the file does not: the file keeps its slot, which is all it needs.
	std::mem::forget(fs);
	(raw, file)
}

fn contents_of(file: &Arc<dyn FsFile>) -> Vec<u8> {
	let mut bytes = vec![0u8; file.len().unwrap() as usize];
	file.read_exact_at(0, &mut bytes).unwrap();
	bytes
}

#[test]
fn a_buffer_whose_flush_failed_is_not_written_again_when_the_writer_is_dropped() {
	use super::{BufferedFileWriter, WritableFile};

	let (raw, file) = pool_file("/w/a.wal");
	let mut writer = BufferedFileWriter::new(Arc::clone(&file), 64);
	let chunk = vec![7u8; 40];
	writer.append(&chunk).unwrap();
	assert_eq!(file.len().unwrap(), 0, "the chunk is in the buffer");
	// The write of the buffer puts ten bytes in the file and fails.
	raw.write_then_fail(10);
	assert!(writer.flush().is_err());
	assert_eq!(file.len().unwrap(), 10, "ten bytes of the chunk landed");
	drop(writer);
	assert_eq!(
		contents_of(&file),
		vec![7u8; 10],
		"the chunk is not written after its own beginning when the writer is dropped"
	);
}

#[test]
fn a_buffer_whose_append_failed_is_not_written_again_when_the_writer_is_dropped() {
	use super::{BufferedFileWriter, WritableFile};

	let (raw, file) = pool_file("/w/b.wal");
	let mut writer = BufferedFileWriter::new(Arc::clone(&file), 64);
	writer.append(&[1u8; 40]).unwrap();
	// This one does not fit what is left of the buffer: appending it writes the buffer out first,
	// and that write puts five bytes in the file and fails.
	raw.write_then_fail(5);
	assert!(writer.append(&[2u8; 40]).is_err());
	assert_eq!(file.len().unwrap(), 5);
	drop(writer);
	assert_eq!(contents_of(&file), vec![1u8; 5], "nothing is written when the writer is dropped");
}

#[test]
fn a_writer_whose_writes_all_succeeded_still_writes_its_buffer_when_dropped() {
	use super::{BufferedFileWriter, WritableFile};

	// The guard is for a failed write only: a buffer that is whole and was never written is the
	// one that `BufWriter` writes on drop, as it always did.
	let (_raw, file) = pool_file("/w/c.wal");
	let mut writer = BufferedFileWriter::new(Arc::clone(&file), 64);
	writer.append(&[3u8; 40]).unwrap();
	drop(writer);
	assert_eq!(contents_of(&file), vec![3u8; 40]);
}

// ---- links in the directory of the log --------------------------------------------------------
//
// The log listed its directory in two ways before the seam, which differ for a link: the ids of the
// segments (`list_segment_ids`) asked `std::fs::metadata` of each name, which follows a link and
// fails for one that points to nothing, and the references to them (`SegmentRef`) took the type of
// the entry from `DirEntry::file_type`, which does not follow a link, so that a link was not a
// segment. `StdFs::read_dir` says that an entry is a link, and each listing decides, as it did.

#[cfg(unix)]
fn segment_name(id: u64) -> String {
	format!("{id:020}.wal")
}

#[cfg(unix)]
#[test]
fn a_link_to_a_segment_is_a_segment_and_a_link_to_nothing_fails_the_listing_of_ids() {
	use std::os::unix::fs::symlink;

	let dir = TempDir::new().unwrap();
	let elsewhere = TempDir::new().unwrap();
	let wal = dir.path();
	fs::write(wal.join(segment_name(1)), b"one").unwrap();
	let target = elsewhere.path().join("two");
	fs::write(&target, b"two").unwrap();
	symlink(&target, wal.join(segment_name(2))).unwrap();
	// A link to a directory is not a segment, and does not fail the listing.
	symlink(elsewhere.path(), wal.join(segment_name(4))).unwrap();
	assert_eq!(list_segment_ids_in(&StdFs, wal, Some("wal")).unwrap(), [1, 2]);

	// A link to nothing is not adopted: the listing fails, as `metadata` of it did, and nothing is
	// created at the end of it.
	let gone = elsewhere.path().join("gone");
	symlink(&gone, wal.join(segment_name(3))).unwrap();
	assert!(list_segment_ids_in(&StdFs, wal, Some("wal")).is_err());
	assert!(!gone.exists());
}

#[cfg(unix)]
#[test]
fn a_link_is_not_a_segment_reference() {
	use std::os::unix::fs::symlink;

	let dir = TempDir::new().unwrap();
	let elsewhere = TempDir::new().unwrap();
	let wal = dir.path();
	fs::write(wal.join(segment_name(1)), b"one").unwrap();
	let target = elsewhere.path().join("two");
	fs::write(&target, b"two").unwrap();
	symlink(&target, wal.join(segment_name(2))).unwrap();
	let gone = elsewhere.path().join("gone");
	symlink(&gone, wal.join(segment_name(3))).unwrap();

	let refs =
		super::SegmentRef::read_segments_from_directory_in(&StdFs, wal, Some("wal")).unwrap();
	assert_eq!(refs.iter().map(|segment| segment.id).collect::<Vec<_>>(), [1]);
	assert!(!gone.exists());
}

#[cfg(unix)]
#[test]
fn a_log_opened_beside_a_link_to_nothing_named_like_a_segment_creates_no_target() {
	use std::os::unix::fs::symlink;

	let dir = TempDir::new().unwrap();
	let elsewhere = TempDir::new().unwrap();
	torn_segment(dir.path(), 1, 0);
	let gone = elsewhere.path().join("gone");
	symlink(&gone, dir.path().join(segment_name(2))).unwrap();

	// Taking the link for the newest segment would open it to append, and that makes the file that
	// the link points to.
	assert!(Wal::open(dir.path(), Options::default()).is_err());
	assert!(!gone.exists(), "the target of the link was not made");
}

#[cfg(unix)]
#[test]
fn the_check_of_the_log_number_is_skipped_when_the_listing_fails_as_it_always_was() {
	use std::os::unix::fs::symlink;

	let dir = TempDir::new().unwrap();
	let elsewhere = TempDir::new().unwrap();
	fs::write(dir.path().join(segment_name(1)), b"one").unwrap();
	// A log number that is two past the last segment is a corrupt manifest.
	assert!(crate::levels::validate_wal_log_number(dir.path(), 10).is_err());
	// With a link to nothing in the directory the listing fails, and the check does not run.
	symlink(elsewhere.path().join("gone"), dir.path().join(segment_name(5))).unwrap();
	crate::levels::validate_wal_log_number(dir.path(), 10).unwrap();
}
