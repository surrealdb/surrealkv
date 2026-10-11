//! Tests for how the blocks of an SSTable are read: one request per block, reads that return
//! less than was asked for, and handles that do not fit the file.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::sstable::block::BlockHandle;
use crate::sstable::block_io::{BlockKind, BlockSource};
use crate::sstable::error::SSTableError;
use crate::sstable::table::{mask, Table, TableWriter, TABLE_FULL_FOOTER_LENGTH};
use crate::vfs::File;
use crate::{InternalKey, InternalKeyKind, Options, INTERNAL_KEY_SEQ_NUM_MAX};

/// Counts the `read_at` calls that reach it.
struct CountingFile {
	inner: Vec<u8>,
	reads: Arc<AtomicU64>,
}

impl File for CountingFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		self.reads.fetch_add(1, Ordering::Relaxed);
		self.inner.read_at(offset, buf)
	}

	fn size(&self) -> Result<u64> {
		self.inner.size()
	}
}

/// Hands out at most `max` bytes per read, and interrupts the very first read, as a file may.
struct TrickleFile {
	inner: Vec<u8>,
	max: usize,
	interrupted: AtomicBool,
}

impl TrickleFile {
	fn new(inner: Vec<u8>, max: usize) -> Self {
		Self {
			inner,
			max,
			interrupted: AtomicBool::new(false),
		}
	}
}

impl File for TrickleFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		if !self.interrupted.swap(true, Ordering::Relaxed) {
			return Err(Error::Io(Arc::new(io::Error::from(io::ErrorKind::Interrupted))));
		}
		let n = buf.len().min(self.max);
		self.inner.read_at(offset, &mut buf[..n])
	}

	fn size(&self) -> Result<u64> {
		self.inner.size()
	}
}

/// A file that must not be read.
struct UnreadableFile;

impl File for UnreadableFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		panic!("unexpected read of {} bytes at {offset}", buf.len());
	}

	fn size(&self) -> Result<u64> {
		Ok(0)
	}
}

fn point_key(i: usize) -> InternalKey {
	InternalKey::new(format!("key{i:04}").into_bytes(), (i + 1) as u64, InternalKeyKind::Set)
}

fn lookup_key(i: usize) -> InternalKey {
	InternalKey::new(
		format!("key{i:04}").into_bytes(),
		INTERNAL_KEY_SEQ_NUM_MAX,
		InternalKeyKind::Set,
	)
}

/// A table of `n` small entries, which is one data block, one index partition and one filter
/// partition with the default options, and the number of bytes it takes.
fn build_table(opts: &Arc<Options>, n: usize) -> Vec<u8> {
	let mut buf = Vec::new();
	{
		let mut writer = TableWriter::new(&mut buf, 1, Arc::clone(opts), 0);
		for i in 0..n {
			writer.add(point_key(i), format!("value{i:04}").as_bytes()).unwrap();
		}
		writer.finish().unwrap();
	}
	buf
}

fn is_unexpected_eof(err: &Error) -> bool {
	matches!(err, Error::Io(e) if e.kind() == io::ErrorKind::UnexpectedEof)
}

#[test]
fn in_memory_file_reads_nothing_at_or_past_its_end() {
	let file: Vec<u8> = vec![1, 2, 3, 4];
	let mut buf = [0u8; 8];

	assert_eq!(file.read_at(0, &mut buf).unwrap(), 4);
	assert_eq!(&buf[..4], &[1, 2, 3, 4]);
	assert_eq!(file.read_at(3, &mut buf).unwrap(), 1);
	assert_eq!(file.read_at(4, &mut buf).unwrap(), 0);
	assert_eq!(file.read_at(5, &mut buf).unwrap(), 0);
	assert_eq!(file.read_at(u64::MAX, &mut buf).unwrap(), 0);
}

#[test]
fn read_exact_at_collects_short_reads_and_retries_an_interrupt() {
	let data: Vec<u8> = (0..200u8).collect();
	let file = TrickleFile::new(data.clone(), 7);

	let mut buf = [0u8; 100];
	file.read_exact_at(10, &mut buf).unwrap();
	assert_eq!(&buf[..], &data[10..110]);
}

#[test]
fn read_exact_at_fails_where_the_file_ends() {
	let data: Vec<u8> = (0..200u8).collect();
	let file = TrickleFile::new(data, 7);

	// 50 of the 100 bytes exist: the read is an error, not a buffer whose tail is zeros.
	let mut buf = [0u8; 100];
	let err = file.read_exact_at(150, &mut buf).unwrap_err();
	assert!(is_unexpected_eof(&err), "{err:?}");

	// `read_up_to` is the same read without the error, and says how much it found.
	let mut buf = [0u8; 100];
	assert_eq!(file.read_up_to(150, &mut buf).unwrap(), 50);
	assert_eq!(buf[49], 199);

	let err = file.read_exact_at(u64::MAX, &mut [0u8; 4]).unwrap_err();
	assert!(is_unexpected_eof(&err), "{err:?}");
}

#[test]
fn a_table_reads_correctly_through_a_file_that_returns_short_reads() {
	let opts = Arc::new(Options::default());
	let bytes = build_table(&opts, 50);
	let size = bytes.len() as u64;

	let table =
		Table::new(1, Arc::clone(&opts), Arc::new(TrickleFile::new(bytes, 5)), size).unwrap();
	for i in 0..50 {
		let (key, value) = table.get(&lookup_key(i)).unwrap().expect("key is in the table");
		assert_eq!(key.user_key, format!("key{i:04}").into_bytes());
		assert_eq!(value, format!("value{i:04}").into_bytes());
	}
}

#[tokio::test]
async fn the_asynchronous_path_reads_correctly_through_a_file_that_returns_short_reads() {
	let opts = Arc::new(Options::default());
	let bytes = build_table(&opts, 50);
	let size = bytes.len() as u64;

	let table =
		Table::new(1, Arc::clone(&opts), Arc::new(TrickleFile::new(bytes, 5)), size).unwrap();
	for i in 0..50 {
		let (key, value) =
			table.get_async(&lookup_key(i)).await.unwrap().expect("key is in the table");
		assert_eq!(key.user_key, format!("key{i:04}").into_bytes());
		assert_eq!(value, format!("value{i:04}").into_bytes());
	}
}

#[test]
fn a_block_is_one_read_and_a_cached_block_is_none() {
	let opts = Arc::new(Options::default());
	let bytes = build_table(&opts, 50);
	let size = bytes.len() as u64;
	let reads = Arc::new(AtomicU64::new(0));
	let file = CountingFile {
		inner: bytes,
		reads: Arc::clone(&reads),
	};

	// The footer, the top-level index, the meta-index and the top-level index of the filter:
	// four reads. Reading each block as three (the block, its compression type, its checksum)
	// took ten.
	let table = Table::new(1, Arc::clone(&opts), Arc::new(file), size).unwrap();
	assert_eq!(reads.load(Ordering::Relaxed), 4);

	// A key that is there: the filter partition, the index partition and the data block.
	reads.store(0, Ordering::Relaxed);
	assert!(table.get(&lookup_key(7)).unwrap().is_some());
	assert_eq!(reads.load(Ordering::Relaxed), 3);

	// Everything it needed is in the cache now.
	reads.store(0, Ordering::Relaxed);
	assert!(table.get(&lookup_key(7)).unwrap().is_some());
	assert!(table.get(&lookup_key(8)).unwrap().is_some());
	assert_eq!(reads.load(Ordering::Relaxed), 0);
}

#[test]
fn a_handle_that_does_not_fit_the_file_is_refused_before_anything_is_read_or_allocated() {
	let source = BlockSource::new(Arc::new(UnreadableFile), 1000);
	let cmp = Arc::clone(&Options::default().internal_comparator);

	let refused = |handle: BlockHandle| {
		matches!(
			source.read_contents(BlockKind::Data, &handle),
			Err(Error::SSTable(SSTableError::BlockHandleOutOfBounds { .. }))
		)
	};
	// A size that would be an allocation of petabytes, and offsets and sizes that overflow.
	assert!(refused(BlockHandle::new(0, usize::MAX / 2)));
	assert!(refused(BlockHandle::new(0, usize::MAX)));
	assert!(refused(BlockHandle::new(usize::MAX, 10)));
	assert!(refused(BlockHandle::new(usize::MAX - 2, 1)));
	// The footer is the last 50 bytes: 950 are blocks. The block of 946 bytes and a trailer of
	// five ends one byte into the footer.
	assert!(refused(BlockHandle::new(0, 946)));
	assert!(refused(BlockHandle::new(100, 846)));
	assert!(matches!(
		source.read_block(BlockKind::Data, &BlockHandle::new(0, usize::MAX / 2), Arc::clone(&cmp)),
		Err(Error::SSTable(SSTableError::BlockHandleOutOfBounds { .. }))
	));
}

#[tokio::test]
async fn the_asynchronous_path_refuses_a_handle_that_does_not_fit_the_file() {
	let source = BlockSource::new(Arc::new(UnreadableFile), 1000);
	let cmp = Arc::clone(&Options::default().internal_comparator);

	for handle in [BlockHandle::new(0, usize::MAX / 2), BlockHandle::new(100, 846)] {
		let result = source.read_block_async(BlockKind::Data, &handle, Arc::clone(&cmp)).await;
		assert!(
			matches!(result, Err(Error::SSTable(SSTableError::BlockHandleOutOfBounds { .. }))),
			"{handle:?}"
		);
	}
}

#[test]
fn a_block_that_ends_where_the_footer_begins_is_read() {
	// The block of 945 bytes and its trailer end exactly at the footer, so the handle is
	// accepted, and it is the checksum that refuses the zeros.
	let source = BlockSource::new(Arc::new(vec![0u8; 1000]), 1000);
	let result = source.read_contents(BlockKind::Data, &BlockHandle::new(0, 945));
	assert!(
		matches!(
			result,
			Err(Error::SSTable(SSTableError::ChecksumVerificationFailed {
				block_offset: 0
			}))
		),
		"{result:?}"
	);
}

#[test]
fn a_file_that_is_shorter_than_its_size_is_an_error_not_a_block_of_zeros() {
	// 200 bytes of file, a size of 1000 and a handle that is inside the size and past the file.
	let source = BlockSource::new(Arc::new(vec![0u8; 200]), 1000);
	let err = source.read_contents(BlockKind::Data, &BlockHandle::new(100, 400)).unwrap_err();
	assert!(is_unexpected_eof(&err), "{err:?}");
}

#[tokio::test]
async fn the_asynchronous_path_reports_a_file_that_is_shorter_than_its_size() {
	let source = BlockSource::new(Arc::new(vec![0u8; 200]), 1000);
	let cmp = Arc::clone(&Options::default().internal_comparator);
	let result = source.read_block_async(BlockKind::Data, &BlockHandle::new(100, 400), cmp).await;
	let err = result.err().expect("a block past the end of the file is an error");
	assert!(is_unexpected_eof(&err), "{err:?}");
}

#[test]
fn a_file_smaller_than_a_footer_is_refused_when_it_is_opened() {
	let size = (TABLE_FULL_FOOTER_LENGTH - 1) as u64;
	let result = BlockSource::open(Arc::new(vec![0u8; size as usize]), size);
	assert!(
		matches!(result, Err(Error::SSTable(SSTableError::FileTooSmall { .. }))),
		"a file of {size} bytes was opened"
	);
}

#[test]
fn a_damaged_block_fails_its_checksum() {
	let opts = Arc::new(Options::default());
	let mut bytes = build_table(&opts, 50);
	let size = bytes.len() as u64;
	// The data block is the first block of the file.
	bytes[10] ^= 0x01;

	let table = Table::new(1, Arc::clone(&opts), Arc::new(bytes), size).unwrap();
	let result = table.get(&lookup_key(3));
	assert!(
		matches!(
			result,
			Err(Error::SSTable(SSTableError::ChecksumVerificationFailed {
				block_offset: 0
			}))
		),
		"{result:?}"
	);
}

#[test]
fn a_block_that_is_too_small_to_decode_is_an_error_not_a_panic() {
	// A block of three bytes with the checksum that its trailer is supposed to have. The
	// checksum says that the bytes are the ones that were written, not that they are a block.
	let contents = [1u8, 2, 3];
	let mut file = contents.to_vec();
	file.push(0); // no compression
	let crc = crc32fast::hash(&file);
	file.extend_from_slice(&mask(crc).to_le_bytes());
	file.extend_from_slice(&[0u8; TABLE_FULL_FOOTER_LENGTH]);
	let size = file.len() as u64;
	let source = BlockSource::new(Arc::new(file), size);

	let handle = BlockHandle::new(0, 3);
	assert_eq!(source.read_contents(BlockKind::Data, &handle).unwrap(), contents);

	let cmp = Arc::clone(&Options::default().internal_comparator);
	let result = source.read_block(BlockKind::Data, &handle, cmp);
	assert!(
		matches!(
			result,
			Err(Error::SSTable(SSTableError::BlockTooSmall {
				size: 3,
				..
			}))
		),
		"{:?}",
		result.err()
	);
}
