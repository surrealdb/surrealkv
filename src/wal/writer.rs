use std::io;

use crc32fast::Hasher;

use super::{
	BufferedFileWriter,
	CompressionType,
	Error,
	IOError,
	RecordType,
	Result,
	WritableFile,
	BLOCK_SIZE,
	HEADER_SIZE,
};

/// Writer for WAL records.
pub struct Writer {
	/// The underlying buffered file writer.
	dest: BufferedFileWriter,

	/// Current offset within the current block (0 to BLOCK_SIZE).
	block_offset: usize,

	/// If true, writes are not automatically flushed. User must call
	/// write_buffer().
	manual_flush: bool,

	/// The compression type to use for records.
	compression_type: CompressionType,

	/// Set when an append failed. The writer then refuses appends until it is
	/// replaced, which a rotation does.
	poisoned: bool,
}

impl Writer {
	/// Creates a new Writer for the given buffered file writer.
	///
	/// # Parameters
	/// - `dest`: The buffered file writer to write records to.
	/// - `manual_flush`: If true, user must call write_buffer() to flush.
	/// - `compression_type`: The compression type to use.
	pub fn new(
		dest: BufferedFileWriter,
		manual_flush: bool,
		compression_type: CompressionType,
		block_offset: usize,
	) -> Self {
		Self {
			dest,
			block_offset,
			manual_flush,
			compression_type,
			poisoned: false,
		}
	}

	/// Makes the write after the next `ops` of them fail once (see
	/// [`BufferedFileWriter::fail_after_ops`]).
	#[cfg(test)]
	pub(crate) fn fail_after_ops(&mut self, ops: usize) {
		self.dest.fail_after_ops(ops);
	}

	/// How many `write(2)` calls have reached the segment (see
	/// [`BufferedFileWriter::file_writes`]).
	#[cfg(test)]
	pub(crate) fn file_writes(&self) -> usize {
		self.dest.file_writes()
	}

	/// Adds a record to the WAL.
	///
	/// The record is automatically fragmented if it doesn't fit in the current
	/// block. If manual_flush is false, the data is automatically flushed to
	/// disk.
	///
	/// # Parameters
	/// - `slice`: The data to write.
	///
	/// # Returns
	/// - Ok(()) if successful.
	/// - Err if an I/O error occurs.
	pub fn add_record(&mut self, slice: &[u8]) -> Result<()> {
		self.guard_append(|writer| writer.write_record(slice))
	}

	/// Adds several records, `buf[ends[i - 1]..ends[i]]` being record `i` (`ends[-1]` is 0),
	/// each framed exactly as `add_record` frames it, with one flush for all of them.
	///
	/// All or nothing: if any step fails the whole call counts as one failed append, so
	/// `guard_append` cuts the segment back to where it was before the first record and
	/// poisons the writer. A record of the group that was already complete is cut off too,
	/// because none of the group was acknowledged.
	///
	/// The caller guarantees that every record is non-empty and that `ends` is increasing and
	/// within `buf`.
	pub fn add_records(&mut self, buf: &[u8], ends: &[usize]) -> Result<()> {
		self.guard_append(|writer| {
			// Every append ends in a flush, so the file length is known and the segment can be
			// cut back to it. Checked once the writer is known not to be poisoned: a failed
			// cut can leave bytes in the buffer, and that must be an error, not a panic.
			debug_assert!(writer.dest.is_flushed());
			let mut start = 0;
			for &end in ends {
				writer.write_record_unflushed(&buf[start..end])?;
				start = end;
			}
			if !writer.manual_flush {
				writer.write_buffer()?;
			}
			Ok(())
		})
	}

	fn write_record(&mut self, slice: &[u8]) -> Result<()> {
		self.write_record_unflushed(slice)?;

		// Flush if not in manual mode
		if !self.manual_flush {
			self.write_buffer()?;
		}

		Ok(())
	}

	/// Frames `slice` into the buffer without flushing it. Everything that makes a record
	/// what it is on disk happens here: compression, fragmentation, padding and the headers,
	/// so a group of records can only ever be framed the way a single one is.
	fn write_record_unflushed(&mut self, slice: &[u8]) -> Result<()> {
		// Compress data if compression is enabled
		let compressed;
		let data_to_write = if self.compression_type == CompressionType::Lz4 {
			compressed = lz4_flex::compress_prepend_size(slice);
			&compressed[..]
		} else {
			slice
		};

		let mut ptr = data_to_write;
		let mut begin = true;

		// Fragment the record if necessary and emit it
		while begin || !ptr.is_empty() {
			// Switch to new block if less than HEADER_SIZE bytes remain
			self.maybe_switch_to_new_block()?;

			// Calculate how much data fits in the current block
			let avail = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
			let fragment_length = ptr.len().min(avail);
			let fragment = &ptr[..fragment_length];

			// Determine record type
			let is_end = fragment_length == ptr.len();
			let record_type = if begin && is_end {
				RecordType::Full
			} else if begin {
				RecordType::First
			} else if is_end {
				RecordType::Last
			} else {
				RecordType::Middle
			};

			// Write the physical record
			self.emit_physical_record(record_type, fragment)?;

			// Advance pointer
			ptr = &ptr[fragment_length..];
			begin = false;
		}

		Ok(())
	}

	/// Adds a compression type record at the start of the WAL.
	///
	/// This should be called before any data records are written.
	pub fn add_compression_type_record(&mut self) -> Result<()> {
		self.guard_append(|writer| writer.write_compression_type_record())
	}

	fn write_compression_type_record(&mut self) -> Result<()> {
		// Should be the first record
		if self.block_offset != 0 {
			return Err(Error::IO(IOError::new(
				io::ErrorKind::Other,
				"Compression type record must be first",
			)));
		}

		if self.compression_type == CompressionType::None {
			return Ok(());
		}

		// Encode compression type (just the type as u8 for now)
		let data = [self.compression_type as u8];

		// Emit as SetCompressionType record
		self.emit_physical_record(RecordType::SetCompressionType, &data)?;

		if !self.manual_flush {
			self.write_buffer()?;
		}

		Ok(())
	}

	/// Writes any buffered data to the file.
	///
	/// Flushes to OS cache (fast) but does NOT fsync to disk.
	/// For durability, call sync() explicitly when needed.
	pub fn write_buffer(&mut self) -> Result<()> {
		self.dest.flush() // Fast: OS cache only, no fsync
	}

	/// Syncs data to disk (slow, durable).
	///
	/// Should be called when durability is required (e.g., transaction commit).
	pub fn sync(&mut self) -> Result<()> {
		self.dest.sync() // Slow: flush + fsync to disk
	}

	/// Closes the writer, syncing and flushing all data.
	pub fn close(&mut self) -> Result<()> {
		self.sync()?;
		self.dest.close()
	}

	/// Runs an append. If it fails, part of a record may be in the segment, and
	/// recovery stops at the first damaged record, so every record acknowledged
	/// after it would be dropped. The segment is cut back to the last complete
	/// record, and the writer refuses further appends until a rotation replaces
	/// it: a disk that failed once may fail again, and if the cut itself failed
	/// the state of the segment is unknown.
	fn guard_append<T>(&mut self, append: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
		if self.poisoned {
			return Err(Error::IO(IOError::new(
				io::ErrorKind::Other,
				"WAL writer is poisoned by an earlier failed append",
			)));
		}

		// With bytes still buffered the file is shorter than the writer's length, and
		// cutting it there would extend it.
		let last_good_len = self.dest.len().filter(|_| self.dest.is_flushed());
		let last_good_block_offset = self.block_offset;

		let result = append(self);
		if result.is_err() {
			self.poisoned = true;
			match last_good_len.map(|len| self.dest.truncate(len)) {
				Some(Ok(())) => self.block_offset = last_good_block_offset,
				Some(Err(e)) => {
					tracing::error!("Failed to cut a partial record off the WAL segment: {e}")
				}
				None => tracing::error!(
					"A WAL append failed with the segment length unknown, \
					so a partial record may remain"
				),
			}
		}
		result
	}

	/// Switches to a new block if there's not enough space for a header.
	///
	/// Only pads when `leftover < HEADER_SIZE` (< 7 bytes remaining).
	/// Padding is always less than 7 bytes, which the reader discards
	/// when `buffer_remaining() < HEADER_SIZE`.
	fn maybe_switch_to_new_block(&mut self) -> Result<()> {
		let leftover = BLOCK_SIZE - self.block_offset;

		// Pad when there's not enough space for a header
		if leftover < HEADER_SIZE {
			// Pad remaining space with zeros (will be 1-6 bytes)
			const ZEROS: [u8; HEADER_SIZE] = [0u8; HEADER_SIZE];
			self.dest.append(&ZEROS[..leftover])?;
			self.block_offset = 0;
		}

		Ok(())
	}

	/// Emits a single physical record to the file.
	fn emit_physical_record(&mut self, record_type: RecordType, data: &[u8]) -> Result<()> {
		let length = data.len();
		if length > 0xffff {
			return Err(Error::IO(IOError::new(io::ErrorKind::InvalidInput, "Record too large")));
		}

		// Physical record must fit entirely in current block
		debug_assert!(
			self.block_offset + HEADER_SIZE + length <= BLOCK_SIZE,
			"Record exceeds block boundary: offset={}, header={}, data={}, block_size={}",
			self.block_offset,
			HEADER_SIZE,
			length,
			BLOCK_SIZE
		);

		// Calculate CRC correctly: CRC(type_byte || data)
		// Must match Reader's calculate_crc32 function
		let type_byte = record_type as u8;
		let mut hasher = Hasher::new();
		hasher.update(&[type_byte]); // Add type byte
		hasher.update(data); // Add data
		let crc = hasher.finalize(); // Single CRC over both

		// Write header (7-byte format)
		let mut header = [0u8; HEADER_SIZE];
		header[..4].copy_from_slice(&crc.to_be_bytes());
		header[4..6].copy_from_slice(&(length as u16).to_be_bytes());
		header[6] = record_type as u8;

		self.dest.append(&header)?;
		self.dest.append(data)?;

		self.block_offset += HEADER_SIZE + length;

		Ok(())
	}
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
	use std::fs::File;

	use tempdir::TempDir;

	use super::*;
	use crate::wal::reader::Reader;

	#[test]
	fn test_writer_basic() {
		let temp_dir = TempDir::new("test").unwrap();
		let file_path = temp_dir.path().join("test.wal");
		let file = File::create(&file_path).unwrap();
		let buffered_writer = BufferedFileWriter::new(file, BLOCK_SIZE);

		let mut writer = Writer::new(buffered_writer, false, CompressionType::None, 0);

		// Write a simple record
		writer.add_record(b"Hello, World!").unwrap();

		writer.close().unwrap();

		// Verify file exists and has content
		let metadata = std::fs::metadata(&file_path).unwrap();
		assert!(metadata.len() > 0);
	}

	#[test]
	fn test_manual_flush() {
		let temp_dir = TempDir::new("test").unwrap();
		let file_path = temp_dir.path().join("test.wal");
		let file = File::create(&file_path).unwrap();
		let buffered_writer = BufferedFileWriter::new(file, BLOCK_SIZE);

		let mut writer = Writer::new(buffered_writer, true, CompressionType::None, 0);

		// Write without auto-flush
		writer.add_record(b"Test").unwrap();

		// Manual flush
		writer.write_buffer().unwrap();

		writer.close().unwrap();
	}

	#[test]
	fn test_fragmentation() {
		let temp_dir = TempDir::new("test").unwrap();
		let file_path = temp_dir.path().join("test.wal");
		let file = File::create(&file_path).unwrap();
		let buffered_writer = BufferedFileWriter::new(file, BLOCK_SIZE);

		let mut writer = Writer::new(buffered_writer, false, CompressionType::None, 0);

		// Write a large record that will be fragmented
		let large_data = vec![b'A'; BLOCK_SIZE * 2];
		writer.add_record(&large_data).unwrap();

		writer.close().unwrap();

		let metadata = std::fs::metadata(&file_path).unwrap();
		assert!(metadata.len() > BLOCK_SIZE as u64 * 2);
	}

	/// A record of `len` bytes that no other `index` shares and that does not compress, so a
	/// record written in the wrong place or a fragment of it repeated cannot look right by
	/// accident, and an LZ4 record is as long as it is large.
	fn payload(index: usize, len: usize) -> Vec<u8> {
		let mut x = (index as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
		(0..len)
			.map(|_| {
				x ^= x << 13;
				x ^= x >> 7;
				x ^= x << 17;
				x as u8
			})
			.collect()
	}

	/// Record sizes on both sides of every boundary that matters: empty-ish, a header's
	/// worth, a block less its header (a record that exactly fills a block) and one byte
	/// either side, and records of several blocks.
	const SIZES: [usize; 10] = [1, 2, 7, 100, 32_760, 32_761, 32_762, 40_000, 70_000, 150_000];

	/// Filler record sizes that leave the next record starting at block offsets all over the
	/// block: near the start, mid-block, with 9 down to 7 bytes left (room for a header and
	/// little else), with each number of bytes left that is less than a header (so the next
	/// record is preceded by that much padding) and with no byte left at all.
	const FILLERS: [usize; 16] = [
		1, 6, 33, 1_000, 16_384, 32_000, 32_752, 32_753, 32_754, 32_755, 32_756, 32_757, 32_758,
		32_759, 32_760, 32_761,
	];

	/// Writes the `fillers` and then `records`, each with one `add_record`.
	fn write_one_by_one(
		path: &std::path::Path,
		compression: CompressionType,
		fillers: &[Vec<u8>],
		records: &[Vec<u8>],
	) {
		// No `close()`: every append ends in a flush, and an fsync per file is most of the time
		// the tests below take.
		let mut writer = new_writer(path, compression);
		for record in fillers.iter().chain(records) {
			writer.add_record(record).unwrap();
		}
	}

	fn new_writer(path: &std::path::Path, compression: CompressionType) -> Writer {
		let buffered_writer = BufferedFileWriter::new(File::create(path).unwrap(), BLOCK_SIZE);
		let mut writer = Writer::new(buffered_writer, false, compression, 0);
		if compression != CompressionType::None {
			writer.add_compression_type_record().unwrap();
		}
		writer
	}

	/// The segment bytes of `add_record` for every size and filler: a digest captured before
	/// a group of records could be written in one go, and before the header and the padding
	/// stopped being allocated. Any change to either, to the fragmentation or to the CRC shows
	/// up here. Uncompressed: the bytes of an LZ4 block belong to the `lz4_flex` version.
	#[test]
	fn add_record_framing_is_unchanged() {
		let temp_dir = TempDir::new("test").unwrap();
		let mut bytes = Vec::new();
		for (f, filler) in FILLERS.iter().enumerate() {
			let path = temp_dir.path().join(format!("{f}.wal"));
			let records: Vec<Vec<u8>> =
				SIZES.iter().enumerate().map(|(i, len)| payload(i, *len)).collect();
			write_one_by_one(&path, CompressionType::None, &[payload(f, *filler)], &records);
			bytes.extend(std::fs::read(&path).unwrap());
		}
		let digest =
			(bytes.len() as u64, crc32fast::hash(&bytes), xxhash_rust::xxh3::xxh3_64(&bytes));
		assert_eq!(digest, (6_113_672, 1_569_578_704, 8_097_485_968_888_641_860));
	}

	/// `records` as one buffer and the end of each record in it.
	fn join(records: &[Vec<u8>]) -> (Vec<u8>, Vec<usize>) {
		let mut buf = Vec::new();
		let mut ends = Vec::new();
		for record in records {
			buf.extend_from_slice(record);
			ends.push(buf.len());
		}
		(buf, ends)
	}

	/// Every record in `path`, the way recovery reads them.
	fn read_records(path: &std::path::Path) -> Vec<Vec<u8>> {
		let mut reader = Reader::new(File::open(path).unwrap());
		let mut records = Vec::new();
		loop {
			match reader.read() {
				Ok((record, _)) => records.push(record.to_vec()),
				Err(Error::IO(e)) if e.kind() == io::ErrorKind::UnexpectedEof => return records,
				Err(e) => panic!("{} must read end to end: {e}", path.display()),
			}
		}
	}

	/// A group written by `add_records` is the same bytes as the same records written by one
	/// `add_record` each, wherever in a block the group starts and however the records fall
	/// across block boundaries, and reads back as exactly one record per input.
	fn assert_a_group_is_framed_like_one_append_per_record(
		compression: CompressionType,
		fillers: &[usize],
	) {
		let temp_dir = TempDir::new("test").unwrap();
		let (one, grouped) = (temp_dir.path().join("one.wal"), temp_dir.path().join("group.wal"));
		// Position `i` of a group has its own payload for each size, so a record written in
		// the wrong place does not look right by accident.
		let pool: Vec<Vec<Vec<u8>>> =
			(1..=5).map(|i| SIZES.iter().map(|len| payload(i, *len)).collect()).collect();
		for (f, filler) in fillers.iter().enumerate() {
			let filler = payload(f + 100, *filler);
			for start in 0..SIZES.len() {
				for per_group in 1..=5 {
					let records: Vec<Vec<u8>> = (0..per_group)
						.map(|i| pool[i][(start + i) % SIZES.len()].clone())
						.collect();
					let at = format!(
						"{compression:?}, {} byte filler, {per_group} records from size #{start}",
						filler.len()
					);

					write_one_by_one(&one, compression, std::slice::from_ref(&filler), &records);

					let mut writer = new_writer(&grouped, compression);
					writer.add_record(&filler).unwrap();
					let (buf, ends) = join(&records);
					writer.add_records(&buf, &ends).unwrap();

					assert_eq!(
						std::fs::read(&one).unwrap(),
						std::fs::read(&grouped).unwrap(),
						"{at}: a group must not change a byte"
					);
					let mut want = vec![filler.clone()];
					want.extend(records);
					assert_eq!(read_records(&grouped), want, "{at}: one record per input");
				}
			}
		}
	}

	#[test]
	fn a_group_is_framed_like_one_append_per_record() {
		assert_a_group_is_framed_like_one_append_per_record(CompressionType::None, &FILLERS);
	}

	/// Compression happens per record inside the group, so the records are as long as their
	/// LZ4 blocks and not as long as the input, and still fall where one append puts them.
	#[test]
	fn a_compressed_group_is_framed_like_one_append_per_record() {
		assert_a_group_is_framed_like_one_append_per_record(
			CompressionType::Lz4,
			&[1, 1_000, 32_000, 32_755, 32_760],
		);
	}

	/// The point of a group: one `write(2)` for any number of records that fit the 32 KiB
	/// buffer, where one `add_record` each takes one per record. A larger group is written
	/// in pieces as the buffer fills, and still in far fewer than one per record.
	#[test]
	fn a_group_reaches_the_file_in_one_write() {
		let temp_dir = TempDir::new("test").unwrap();
		let records: Vec<Vec<u8>> = (0..8).map(|i| payload(i, 200 + i)).collect();
		let (buf, ends) = join(&records);

		let mut one_by_one = new_writer(&temp_dir.path().join("one.wal"), CompressionType::None);
		for record in &records {
			one_by_one.add_record(record).unwrap();
		}
		assert_eq!(one_by_one.file_writes(), records.len());

		let mut grouped = new_writer(&temp_dir.path().join("group.wal"), CompressionType::None);
		grouped.add_records(&buf, &ends).unwrap();
		assert_eq!(grouped.file_writes(), 1, "8 records in 32 KiB are one write");
		grouped.add_records(&buf, &ends).unwrap();
		assert_eq!(grouped.file_writes(), 2, "and the next group is one more");

		// Twenty records of 10 KB, 200 KB together: about a write per buffer's worth, and far
		// fewer than one per record.
		let large: Vec<Vec<u8>> = (0..20).map(|i| payload(i, 10_000)).collect();
		let (buf, ends) = join(&large);
		let mut grouped = new_writer(&temp_dir.path().join("large.wal"), CompressionType::None);
		grouped.add_records(&buf, &ends).unwrap();
		let writes = grouped.file_writes();
		assert!(
			writes >= buf.len().div_ceil(BLOCK_SIZE),
			"{writes} writes cannot hold {} bytes",
			buf.len()
		);
		assert!(writes <= large.len() / 2, "{writes} writes for {} records", large.len());
		assert_eq!(read_records(&temp_dir.path().join("large.wal")), large);
	}
}
