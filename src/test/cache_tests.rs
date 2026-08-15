//! Behavioural tests for the block cache.
//!
//! The cache used to be checked through six `AtomicU64` counters compiled into
//! its lookup path under `#[cfg(test)]`. That made the object under test a
//! different object from the one that ships — a different struct size, and six
//! atomic read-modify-writes per lookup on the hottest read path in the engine
//! — to serve four assertions.
//!
//! What those counters were reaching for is observable without them. "The
//! second read came from the cache" means "the second read did not touch the
//! file", so: seal the file, so every read of it fails, and read again. If the
//! answer still comes back, it came from the cache. If it does not, it was
//! going to disk.
//!
//! The instrument is [`SealableFile`], an ordinary [`File`] implementation. The
//! table under test therefore runs exactly the code that ships, and only the
//! file beneath it differs.

use std::io::SeekFrom;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use test_log::test;

use crate::comparator::{BytewiseComparator, Comparator, TimestampComparator};
use crate::error::{Error, Result};
use crate::sstable::table::{Table, TableWriter};
use crate::test::collect_all;
use crate::vfs::File;
use crate::{InternalKey, InternalKeyKind, Options};

/// Substring every sealed-read error carries, so a test can tell the seal
/// firing apart from an unrelated failure.
const SEALED: &str = "sealed";

/// A [`File`] that fails every read once it is sealed.
///
/// Sealing is one-way and covers `read_at`, which is the only read the SSTable
/// reader performs. Everything else on the trait is unreachable for a table
/// opened read-only, and says so rather than silently succeeding.
struct SealableFile {
	inner: Arc<dyn File>,
	sealed: AtomicBool,
	reads: AtomicUsize,
}

impl SealableFile {
	fn wrap(inner: Arc<dyn File>) -> Arc<Self> {
		Arc::new(Self {
			inner,
			sealed: AtomicBool::new(false),
			reads: AtomicUsize::new(0),
		})
	}

	fn seal(&self) {
		self.sealed.store(true, Ordering::SeqCst);
	}

	/// Reads that actually reached the file. A test asserts this is non-zero
	/// before sealing, so "the second read succeeded" cannot be explained by
	/// the first read never having needed the file either.
	fn reads(&self) -> usize {
		self.reads.load(Ordering::SeqCst)
	}
}

impl File for SealableFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		if self.sealed.load(Ordering::SeqCst) {
			return Err(Error::Io(Arc::new(std::io::Error::other(format!(
				"file is {SEALED}: a read at offset {offset} should have been served from the block cache"
			)))));
		}
		self.reads.fetch_add(1, Ordering::SeqCst);
		self.inner.read_at(offset, buf)
	}

	fn size(&self) -> Result<u64> {
		self.inner.size()
	}

	fn write(&mut self, _buf: &[u8]) -> Result<usize> {
		unreachable!("SealableFile is a read-only double")
	}

	fn flush(&mut self) -> Result<()> {
		unreachable!("SealableFile is a read-only double")
	}

	fn close(&mut self) -> Result<()> {
		unreachable!("SealableFile is a read-only double")
	}

	fn seek(&mut self, _pos: SeekFrom) -> Result<u64> {
		unreachable!("SealableFile is a read-only double")
	}

	fn read(&mut self, _buf: &mut [u8]) -> Result<usize> {
		unreachable!("the SSTable reader reads through `read_at`")
	}

	fn read_all(&mut self, _buf: &mut Vec<u8>) -> Result<usize> {
		unreachable!("the SSTable reader reads through `read_at`")
	}

	fn lock(&self) -> Result<()> {
		unreachable!("SealableFile is a read-only double")
	}

	fn unlock(&self) -> Result<()> {
		unreachable!("SealableFile is a read-only double")
	}

	fn write_at(&mut self, _offset: u64, _buf: &[u8]) -> Result<usize> {
		unreachable!("SealableFile is a read-only double")
	}

	fn sync(&self) -> Result<()> {
		unreachable!("SealableFile is a read-only double")
	}

	fn sync_data(&self) -> Result<()> {
		unreachable!("SealableFile is a read-only double")
	}
}

/// Keys spanning several data blocks at the block size below: three entries per
/// block, so `abc` and `zzz` are never in the same one.
const ENTRIES: &[(&str, &str)] = &[
	("abc", "def"),
	("abd", "dee"),
	("bcd", "asa"),
	("bsr", "a00"),
	("xyz", "xxx"),
	("xzz", "yyy"),
	("zzz", "111"),
];

/// Small enough that [`ENTRIES`] spans more than one data block, which is what
/// makes "a block this table has never read" reachable in the same test.
const BLOCK_SIZE: usize = 32;

/// Above every sequence number written below, so a lookup seeks to the newest
/// version of its key rather than past it.
const LOOKUP_SEQ: u64 = 100;

fn table_opts() -> Arc<Options> {
	let mut opts = Options::new();
	opts.block_restart_interval = 3;
	opts.block_size = BLOCK_SIZE;
	Arc::new(opts)
}

fn build_table(opts: Arc<Options>) -> (Vec<u8>, u64) {
	let mut buf = Vec::new();
	let size = {
		let mut writer = TableWriter::new(&mut buf, 0, opts, 0);
		for (key, value) in ENTRIES {
			writer
				.add(
					InternalKey::new(key.as_bytes().to_vec(), 1, InternalKeyKind::Set, 0),
					value.as_bytes(),
				)
				.unwrap();
		}
		writer.finish().unwrap()
	};
	(buf, size as u64)
}

/// Opens `ENTRIES` as a table over a sealable file. Each call builds its own
/// [`Options`], so each table gets its own block cache and cannot be served by
/// another test's.
fn open_sealable_table(table_id: u64) -> (Table, Arc<SealableFile>) {
	let opts = table_opts();
	let (buf, size) = build_table(Arc::clone(&opts));
	let file = SealableFile::wrap(Arc::new(buf));
	let table = Table::new(table_id, opts, Arc::clone(&file) as Arc<dyn File>, size).unwrap();
	(table, file)
}

fn lookup_key(user_key: &str) -> InternalKey {
	InternalKey::new(user_key.as_bytes().to_vec(), LOOKUP_SEQ, InternalKeyKind::Set, 0)
}

fn is_sealed_error(err: &Error) -> bool {
	err.to_string().contains(SEALED)
}

#[test]
fn a_block_already_in_the_cache_is_served_without_reading_the_file() {
	let (table, file) = open_sealable_table(1);

	let key = lookup_key("abc");
	let (_, from_disk) = table.get(&key).unwrap().expect("`abc` was written to this table");
	assert!(
		file.reads() > 0,
		"the first lookup must have gone to the file, or the second proves nothing"
	);

	file.seal();

	let (_, from_cache) =
		table.get(&key).unwrap().expect("`abc` is still readable once its block is cached");
	assert_eq!(from_cache, from_disk, "the cached block must yield the value the file yielded");

	// The seal is live in this same test: a data block this table has never
	// read cannot be served, so the assertion above is the cache's doing and
	// not the seal quietly failing to bite.
	let never_read = table.get(&lookup_key("zzz")).unwrap_err();
	assert!(
		is_sealed_error(&never_read),
		"reading an uncached block from a sealed file must fail, got: {never_read}"
	);
}

#[test]
fn a_block_not_in_the_cache_is_read_from_the_file() {
	let (table, file) = open_sealable_table(2);

	// Nothing has been looked up yet, so no data block is cached.
	file.seal();

	let err = table.get(&lookup_key("abc")).unwrap_err();
	assert!(is_sealed_error(&err), "expected the sealed-file error, got: {err}");
}

#[test]
fn the_history_block_cache_serves_a_repeat_scan_without_reading_the_file() {
	let (table, file) = open_sealable_table(3);

	// History reads go through a separate cache keyed by a different kind,
	// because they iterate under a different comparator.
	let cmp: Arc<dyn Comparator> =
		Arc::new(TimestampComparator::new(Arc::new(BytewiseComparator::default())));

	let mut first_pass = table.iter_with_comparator(None, Arc::clone(&cmp)).unwrap();
	let first = collect_all(&mut first_pass).unwrap();
	drop(first_pass);
	assert_eq!(first.len(), ENTRIES.len(), "the scan must see every entry written");
	assert!(file.reads() > 0, "the first scan must have gone to the file");

	file.seal();

	let mut second_pass = table.iter_with_comparator(None, Arc::clone(&cmp)).unwrap();
	let second = collect_all(&mut second_pass).unwrap();
	assert_eq!(second, first, "the second scan must come back identical, from the cache");
}
