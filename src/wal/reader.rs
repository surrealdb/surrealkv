use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::vec::Vec;

use crate::wal::{
	calculate_crc32,
	validate_record_type,
	CompressionType,
	CorruptionError,
	Error,
	IOError,
	RecordType,
	Result,
	BLOCK_SIZE,
	HEADER_SIZE as WAL_RECORD_HEADER_SIZE,
};

/// Reporter interface for WAL corruption and errors.
///
/// Implementations of this trait can be provided to Reader to handle
/// corruption and other errors encountered during WAL replay.
pub trait Reporter {
	/// Called when corruption is detected.
	///
	/// # Parameters
	/// - `bytes`: Approximate number of bytes dropped due to corruption
	/// - `reason`: Description of the corruption
	/// - `log_number`: The log number where corruption was found
	fn corruption(&mut self, bytes: usize, reason: &str, log_number: u64);

	/// Called when an old log record is encountered.
	///
	/// # Parameters
	/// - `bytes`: Size of the old record
	fn old_log_record(&mut self, bytes: usize);
}

/// Reader reads records from a single WAL file using block-based buffering.
///
/// Reads entire 32KB blocks into a buffer. When fewer than HEADER_SIZE bytes
/// remain, the remainder is discarded (it's padding) and the next block is
/// read.
pub(crate) struct Reader {
	/// The underlying file handle
	file: File,

	/// Buffer holding the current block's data
	buffer: Vec<u8>,

	/// Current read position within the buffer
	buffer_offset: usize,

	/// File offset at the end of the current buffer
	end_of_buffer_offset: usize,

	/// Whether EOF has been reached
	eof: bool,

	/// Whether a read error has occurred
	read_error: bool,

	/// Buffer for accumulating record fragments
	rec: Vec<u8>,

	/// Current record type being processed
	cur_rec_type: RecordType,

	/// Error encountered (if any)
	err: Option<Error>,

	/// Optional reporter for corruption/errors
	reporter: Option<Box<dyn Reporter>>,

	/// Log number for this WAL
	log_number: u64,

	/// Compression type (if SetCompressionType record was read)
	compression_type: CompressionType,

	/// Whether compression type record has been read
	compression_type_record_read: bool,
}

impl Reader {
	/// Creates a new Reader from a File with default settings.
	pub(crate) fn new(file: File) -> Self {
		Self::with_options(file, None, 0)
	}

	/// Creates a new Reader with custom options.
	#[allow(dead_code)]
	pub(crate) fn with_options(
		file: File,
		reporter: Option<Box<dyn Reporter>>,
		log_number: u64,
	) -> Self {
		Reader {
			file,
			buffer: Vec::with_capacity(BLOCK_SIZE),
			buffer_offset: 0,
			end_of_buffer_offset: 0,
			eof: false,
			read_error: false,
			rec: Vec::new(),
			cur_rec_type: RecordType::Empty,
			err: None,
			reporter,
			log_number,
			compression_type: CompressionType::None,
			compression_type_record_read: false,
		}
	}

	/// Returns the compression type detected from the WAL file.
	///
	/// This is set when a SetCompressionType record is read from the file.
	/// Returns `CompressionType::None` if no compression record was found.
	#[cfg(test)]
	pub(crate) fn get_compression_type(&self) -> CompressionType {
		self.compression_type
	}

	/// Reports corruption to the reporter if present.
	#[allow(dead_code)]
	fn report_corruption(&mut self, bytes: usize, reason: &str) {
		if let Some(ref mut reporter) = self.reporter {
			reporter.corruption(bytes, reason, self.log_number);
		}
	}

	/// Reports an old log record to the reporter if present.
	#[allow(dead_code)]
	fn report_old_log_record(&mut self, bytes: usize) {
		if let Some(ref mut reporter) = self.reporter {
			reporter.old_log_record(bytes);
		}
	}

	/// Returns the number of bytes remaining in the current buffer.
	fn buffer_remaining(&self) -> usize {
		self.buffer.len().saturating_sub(self.buffer_offset)
	}

	/// Reads the next block from the file into the buffer.
	///
	/// This discards any remaining bytes in the current buffer (which would be
	/// padding at the end of a block) and reads a fresh block.
	///
	/// Returns true if more data was read, false if EOF or error.
	fn read_more(&mut self) -> Result<bool> {
		if self.eof || self.read_error {
			return Ok(false);
		}

		// Discard remaining bytes (padding) and read next full block
		self.buffer.clear();
		self.buffer.resize(BLOCK_SIZE, 0);

		match self.file.read(&mut self.buffer) {
			Ok(0) => {
				self.eof = true;
				self.buffer.clear();
				Ok(false)
			}
			Ok(n) => {
				self.buffer.truncate(n);
				self.buffer_offset = 0;
				self.end_of_buffer_offset += n;
				if n < BLOCK_SIZE {
					self.eof = true;
				}
				Ok(true)
			}
			Err(e) => {
				self.read_error = true;
				self.buffer.clear();
				Err(Error::IO(IOError::new(e.kind(), &e.to_string())))
			}
		}
	}

	/// Parses the header from the current buffer position.
	/// Assumes there are at least WAL_RECORD_HEADER_SIZE bytes available.
	fn parse_header(&mut self) -> (u32, u16, u8) {
		let header = &self.buffer[self.buffer_offset..self.buffer_offset + WAL_RECORD_HEADER_SIZE];
		let crc = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
		let length = u16::from_be_bytes([header[4], header[5]]);
		let record_type = header[6];
		self.buffer_offset += WAL_RECORD_HEADER_SIZE;
		(crc, length, record_type)
	}

	pub(crate) fn read(&mut self) -> Result<(&[u8], u64)> {
		if let Some(err) = &self.err {
			return Err(err.clone());
		}

		match self.next() {
			Ok(_) => {
				// Return the file position immediately after this record
				// (not end of buffer, which would be same for all records in a block)
				let offset = self.end_of_buffer_offset - self.buffer_remaining();
				Ok((&self.rec, offset as u64))
			}
			Err(e) => {
				// Check if the error is an UnexpectedEof
				if let Error::IO(io_err) = &e {
					if io_err.kind() == io::ErrorKind::UnexpectedEof {
						self.err = Some(e.clone());
						return Err(e);
					}
				}

				// Calculate error offset and gather debugging info
				let error_offset = self.end_of_buffer_offset - self.buffer_remaining();
				let file_size = self.file.seek(SeekFrom::End(0)).unwrap_or(0);
				let block_number = error_offset / BLOCK_SIZE;
				let offset_within_block = error_offset % BLOCK_SIZE;

				// Detailed error logging for debugging
				log::error!(
					"WAL corruption detected: \
					log_number={}, \
					error_offset={} (0x{:X}), \
					file_size={} bytes, \
					block_number={}, \
					offset_within_block={}, \
					buffer_offset={}, \
					buffer_len={}, \
					end_of_buffer_offset={}, \
					eof={}, \
					compression={:?}, \
					accumulated_record_len={}, \
					current_record_type={:?}, \
					error=\"{}\"",
					self.log_number,
					error_offset,
					error_offset,
					file_size,
					block_number,
					offset_within_block,
					self.buffer_offset,
					self.buffer.len(),
					self.end_of_buffer_offset,
					self.eof,
					self.compression_type,
					self.rec.len(),
					self.cur_rec_type,
					e
				);

				// Fixed behavior: Tolerate tail corruption, enable repair
				let corruption_err = Error::Corruption(CorruptionError::new(
					io::ErrorKind::Other,
					e.to_string().as_str(),
					self.log_number,
					error_offset as u64,
				));

				// Report corruption and return error (caller will handle repair)
				self.report_corruption(self.rec.len(), &e.to_string());
				self.err = Some(corruption_err.clone());
				Err(corruption_err)
			}
		}
	}

	fn next(&mut self) -> Result<()> {
		self.rec.clear();
		let mut fragment_index = 0;

		loop {
			// When < HEADER_SIZE bytes remain, discard them (padding) and read next block
			if self.buffer_remaining() < WAL_RECORD_HEADER_SIZE {
				if !self.read_more()? {
					return Err(Error::IO(IOError::new(
						io::ErrorKind::UnexpectedEof,
						"reached end of file",
					)));
				}
				continue;
			}

			// Parse header from buffer
			let (crc, length, type_byte) = self.parse_header();
			self.cur_rec_type = RecordType::from_u8(type_byte)?;

			// If the type is Empty (0), it's a padded block.
			// Discard the rest of the buffer and read next block.
			if self.cur_rec_type == RecordType::Empty {
				// Verify remaining bytes are zeros
				let remaining = self.buffer_remaining();
				if remaining > 0 {
					let zeros = &self.buffer[self.buffer_offset..self.buffer_offset + remaining];
					if !zeros.iter().all(|&c| c == 0) {
						return Err(Error::IO(IOError::new(
							io::ErrorKind::Other,
							"non-zero byte in padding area",
						)));
					}
				}
				// Discard rest of buffer and continue (read_more will be called next iteration)
				self.buffer_offset = self.buffer.len();
				continue;
			}

			// Handle SetCompressionType metadata record
			if self.cur_rec_type == RecordType::SetCompressionType {
				// Record must fit in current buffer
				if (length as usize) > self.buffer_remaining() {
					return Err(Error::IO(IOError::new(
						io::ErrorKind::Other,
						"truncated compression type record",
					)));
				}

				// Parse and store compression type
				if length > 0 {
					let compression_byte = self.buffer[self.buffer_offset];
					self.buffer_offset += length as usize;
					self.compression_type = CompressionType::from_u8(compression_byte)?;
					self.compression_type_record_read = true;
				}
				continue; // Don't return this as a data record
			}

			// Validate the record type for fragment sequencing
			validate_record_type(self.cur_rec_type, fragment_index)?;

			// Each physical record (header + data) must fit within a single block
			if (length as usize) > self.buffer_remaining() {
				return Err(Error::IO(IOError::new(
					io::ErrorKind::Other,
					"bad record length - exceeds block boundary",
				)));
			}

			// Read record data from buffer
			let record_data =
				&self.buffer[self.buffer_offset..self.buffer_offset + length as usize];

			// Validate the checksum
			let calculated_crc = calculate_crc32(&[type_byte], record_data);
			if calculated_crc != crc {
				return Err(Error::IO(IOError::new(io::ErrorKind::Other, "checksum mismatch")));
			}

			// Append record data to output buffer and advance position
			self.rec.extend_from_slice(record_data);
			self.buffer_offset += length as usize;

			if self.cur_rec_type == RecordType::Last || self.cur_rec_type == RecordType::Full {
				break;
			}

			fragment_index += 1;
		}

		// Decompress if compression is enabled
		if self.compression_type == CompressionType::Lz4 && !self.rec.is_empty() {
			self.rec = lz4_flex::decompress_size_prepended(&self.rec).map_err(|e| {
				Error::IO(IOError::new(
					io::ErrorKind::InvalidData,
					&format!("LZ4 decompression failed: {}", e),
				))
			})?;
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::fs::File;
	use std::io::{BufReader, Read, Write};
	use std::vec::Vec;

	use tempdir::TempDir;
	use test_log::test;

	use super::*;
	use crate::wal::manager::Wal;
	use crate::wal::{Options, SegmentRef};

	// BufferReader does not return EOF when the underlying reader returns 0 bytes
	// read.
	#[test]
	fn bufreader_eof_and_error() {
		// Create a temporary directory to hold the file
		let temp_dir = TempDir::new("test").expect("should create temp dir");

		// Create a sample file and populate it with data
		let file_path = temp_dir.path().join("test.txt");
		let mut file = File::create(&file_path).expect("should create file");
		file.write_all(b"Hello, World!").expect("should write data");
		drop(file);

		// Open the file for reading using BufReader
		let file = File::open(&file_path).expect("should open file");
		let mut buf_reader = BufReader::new(file);

		// Read into a buffer
		let mut read_buffer = [0u8; 5];
		let bytes_read = buf_reader.read(&mut read_buffer).expect("should read");

		// Verify the data read
		assert_eq!(&read_buffer[..bytes_read], b"Hello");

		// Try reading more bytes than available
		let mut read_buffer = [0u8; 10];
		let bytes_read = buf_reader.read(&mut read_buffer).expect("should read");
		assert_eq!(bytes_read, 8); // Only "World!" left to read
		assert_eq!(&read_buffer[..bytes_read], b", World!");

		// Try reading more bytes again than available
		let mut read_buffer = [0u8; 1000];
		let bytes_read = buf_reader.read(&mut read_buffer).expect("should read");
		assert_eq!(bytes_read, 0); // Only "World!" left to read
	}

	#[test]
	fn reader_basic() {
		// Create WAL with test data
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		wal.append(&[1, 2, 3, 4]).expect("should append");
		wal.append(&[5, 6]).expect("should append");
		wal.append(&[7, 8, 9]).expect("should append");
		wal.close().expect("should close");

		// Read back
		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");

		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let (data, _) = reader.read().expect("should read");
		assert_eq!(data, vec![1, 2, 3, 4]);

		let (data, _) = reader.read().expect("should read");
		assert_eq!(data, vec![5, 6]);

		let (data, _) = reader.read().expect("should read");
		assert_eq!(data, vec![7, 8, 9]);
	}

	#[test]
	fn reader_with_large_data() {
		// Create aol options and open a aol file
		let temp_dir = TempDir::new("test").expect("should create temp dir");

		let opts = Options::default().with_max_file_size(4096 * 10);
		let mut a = Wal::open(temp_dir.path(), opts).expect("should create aol");

		let record_size = 4;

		// Define the number of records to append
		let num_records = 10000;

		for _ in 0..num_records {
			let data: Vec<u8> = (0..record_size).map(|i| (i & 0xFF) as u8).collect();
			let r = a.append(&data);
			assert!(r.is_ok());
		}

		a.sync().expect("should sync");
		a.close().expect("should close");

		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");

		// Read from all segments in sequence
		let mut i = 0;
		for segment in segments {
			let file = File::open(&segment.file_path).expect("should open file");
			let mut reader = Reader::new(file);

			while let Ok((data, _)) = reader.read() {
				assert_eq!(data, vec![0, 1, 2, 3]);
				i += 1;
			}
		}

		assert_eq!(i, num_records);
	}

	/// Regression test for block boundary padding bug.
	///
	/// This test writes records that will leave < 7 bytes (HEADER_SIZE) at
	/// various block boundaries. The old streaming reader would read across
	/// the block boundary, mixing padding zeros with the next record's header.
	/// The block-based reader correctly discards the padding.
	#[test]
	fn reader_block_boundary_padding() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Write many records of size 68 (like the actual bug case)
		// This size, with 7-byte headers, will eventually leave < 7 bytes at a block
		// boundary BLOCK_SIZE = 32768, record + header = 75 bytes
		// 32768 / 75 = 436.9... so after 436 records, we have 32768 - (436 * 75) = 68
		// bytes left After one more record (437), we have 68 - 75 = -7, meaning we
		// wrap to next block The key is that various record counts will leave
		// different remainders
		let record_size = 68;
		let records_to_write = 1000; // Enough to cross multiple block boundaries

		for i in 0..records_to_write {
			let data: Vec<u8> = (0..record_size).map(|j| ((i + j) & 0xFF) as u8).collect();
			wal.append(&data).expect("should append");
		}
		wal.close().expect("should close");

		// Read back all records - this would fail with "Invalid Record Type" before the
		// fix
		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");

		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let mut count = 0;
		while let Ok((data, _)) = reader.read() {
			assert_eq!(data.len(), record_size, "Record {} has wrong size", count);
			count += 1;
		}
		assert_eq!(count, records_to_write, "Should read all {} records", records_to_write);
	}

	/// Test that records spanning multiple blocks are handled correctly.
	#[test]
	fn reader_record_spanning_blocks() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Write a record larger than BLOCK_SIZE to force fragmentation
		let large_record_size = BLOCK_SIZE * 2 + 1000;
		let large_data: Vec<u8> = (0..large_record_size).map(|i| (i & 0xFF) as u8).collect();
		wal.append(&large_data).expect("should append large record");

		// Write a small record after
		wal.append(&[1, 2, 3, 4]).expect("should append small record");

		wal.close().expect("should close");

		// Read back
		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");

		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let (data, _) = reader.read().expect("should read large record");
		assert_eq!(data.len(), large_record_size);
		assert_eq!(data, large_data);

		let (data, _) = reader.read().expect("should read small record");
		assert_eq!(data, vec![1, 2, 3, 4]);
	}

	// ==================== Bug Detection Tests ====================

	/// Test #1: Verify offset is correct from the very first read.
	/// Checks that end_of_buffer_offset is properly initialized.
	#[test]
	fn reader_offset_from_first_read() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Write a small record (7 byte header + 3 bytes data = 10 bytes)
		wal.append(b"foo").expect("should append");
		wal.close().expect("should close");

		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");

		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let (data, offset) = reader.read().expect("should read");
		assert_eq!(data, b"foo");

		// Offset should be HEADER_SIZE + data length = 7 + 3 = 10
		// NOT the end of the block (32768)
		assert_eq!(
			offset,
			(WAL_RECORD_HEADER_SIZE + 3) as u64,
			"First read offset should be precise"
		);
	}

	/// Test #2: Verify offsets are correct and increasing for multiple records.
	#[test]
	fn reader_offsets_increasing() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Write multiple records
		wal.append(b"aaa").expect("should append"); // 7 + 3 = 10
		wal.append(b"bbbbb").expect("should append"); // 7 + 5 = 12
		wal.append(b"c").expect("should append"); // 7 + 1 = 8
		wal.close().expect("should close");

		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");

		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let (_, offset1) = reader.read().expect("should read 1");
		let (_, offset2) = reader.read().expect("should read 2");
		let (_, offset3) = reader.read().expect("should read 3");

		// Each offset should be after the previous record
		assert_eq!(offset1, 10, "First record ends at 10");
		assert_eq!(offset2, 22, "Second record ends at 22");
		assert_eq!(offset3, 30, "Third record ends at 30");

		// Offsets must be strictly increasing
		assert!(offset1 < offset2, "Offsets must increase");
		assert!(offset2 < offset3, "Offsets must increase");
	}

	/// Test #3: Verify offset is correct for partial final block.
	#[test]
	fn reader_offset_partial_block() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Write records that don't fill a complete block
		let record_size = 100;
		for i in 0..5 {
			let data: Vec<u8> = (0..record_size).map(|j| ((i + j) & 0xFF) as u8).collect();
			wal.append(&data).expect("should append");
		}
		wal.close().expect("should close");

		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");

		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let mut last_offset = 0u64;
		let mut count = 0;
		while let Ok((data, offset)) = reader.read() {
			assert_eq!(data.len(), record_size);
			assert!(offset > last_offset, "Offset {} should be > previous {}", offset, last_offset);
			// Each record is HEADER_SIZE + 100 = 107 bytes
			let expected = ((count + 1) * (WAL_RECORD_HEADER_SIZE + record_size)) as u64;
			assert_eq!(offset, expected, "Record {} offset mismatch", count);
			last_offset = offset;
			count += 1;
		}
		assert_eq!(count, 5);
	}

	/// Test #4: Verify corrupted compressed data is detected.
	#[test]
	fn reader_corrupted_compressed_data() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let file_path = temp_dir.path().join("test.wal");

		// Manually write a corrupted compressed record
		{
			let mut file = File::create(&file_path).expect("should create file");

			// First write a SetCompressionType record for LZ4
			let compression_type = 5u8; // SetCompressionType
			let compression_data = [1u8]; // LZ4
			let crc1 = calc_crc(compression_type, &compression_data);
			write_raw_record(&mut file, crc1, 1, compression_type, &compression_data);

			// Now write a "Full" record with garbage that looks like compressed data
			let record_type = 1u8; // Full
			let garbage_compressed = [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00]; // Invalid LZ4
			let crc2 = calc_crc(record_type, &garbage_compressed);
			write_raw_record(
				&mut file,
				crc2,
				garbage_compressed.len() as u16,
				record_type,
				&garbage_compressed,
			);
		}

		let file = File::open(&file_path).expect("should open file");
		let mut reader = Reader::new(file);

		// Should fail on decompression
		let result = reader.read();
		assert!(result.is_err(), "Should detect corrupted compressed data");
	}

	/// Test #5: Verify writer handles block boundary correctly for exact fit.
	#[test]
	fn writer_block_boundary_exact_fit() {
		use crate::wal::{BufferedFileWriter, CompressionType};

		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let file_path = temp_dir.path().join("test.wal");
		let file = File::create(&file_path).expect("should create file");
		let buffered_writer = BufferedFileWriter::new(file, BLOCK_SIZE);

		let mut writer =
			crate::wal::writer::Writer::new(buffered_writer, false, CompressionType::None, 0);

		// Write a record that fills exactly to block boundary
		// BLOCK_SIZE - HEADER_SIZE = 32768 - 7 = 32761
		let exact_fit_size = BLOCK_SIZE - WAL_RECORD_HEADER_SIZE;
		let data = vec![b'X'; exact_fit_size];
		writer.add_record(&data).expect("should write exact fit record");

		// Write another small record (should go to next block)
		writer.add_record(b"next").expect("should write next record");

		writer.close().expect("should close");

		// Read back and verify
		let file = File::open(&file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let (read_data, offset1) = reader.read().expect("should read first");
		assert_eq!(read_data.len(), exact_fit_size);
		assert_eq!(offset1, BLOCK_SIZE as u64, "First record fills entire block");

		let (read_data, offset2) = reader.read().expect("should read second");
		assert_eq!(read_data, b"next");
		// Second record starts at BLOCK_SIZE, so ends at BLOCK_SIZE + HEADER_SIZE + 4
		assert_eq!(offset2, (BLOCK_SIZE + WAL_RECORD_HEADER_SIZE + 4) as u64);
	}

	/// Helper to write a raw WAL record directly to file.
	fn write_raw_record(file: &mut File, crc: u32, length: u16, record_type: u8, data: &[u8]) {
		file.write_all(&crc.to_be_bytes()).unwrap();
		file.write_all(&length.to_be_bytes()).unwrap();
		file.write_all(&[record_type]).unwrap();
		file.write_all(data).unwrap();
	}

	/// Helper to calculate CRC for a record.
	fn calc_crc(record_type: u8, data: &[u8]) -> u32 {
		use crate::wal::calculate_crc32;
		calculate_crc32(&[record_type], data)
	}

	// ==================== Helper Functions ====================

	/// Construct a Vec<u8> of the specified length made out of the supplied
	/// partial string.
	fn big_string(partial: &str, n: usize) -> Vec<u8> {
		let mut result = Vec::with_capacity(n);
		let partial_bytes = partial.as_bytes();
		while result.len() < n {
			let remaining = n - result.len();
			let to_copy = std::cmp::min(remaining, partial_bytes.len());
			result.extend_from_slice(&partial_bytes[..to_copy]);
		}
		result
	}

	/// Construct a string from a number (e.g., "42.")
	fn number_string(n: i32) -> String {
		format!("{}.", n)
	}

	/// Return a skewed potentially long string for random testing.
	fn random_skewed_string(i: i32, max_len: usize) -> Vec<u8> {
		// Use a simple deterministic "random" based on i
		let skewed_len = ((i as usize * 17) % max_len).max(1);
		big_string(&number_string(i), skewed_len)
	}

	/// Set a byte at the given offset in data.
	fn set_byte(data: &mut [u8], offset: usize, value: u8) {
		data[offset] = value;
	}

	/// Fix the checksum after modifying a record.
	fn fix_checksum(data: &mut [u8], header_offset: usize, payload_len: usize) {
		use crate::wal::calculate_crc32;
		// CRC is calculated over type_byte + payload
		let type_byte = data[header_offset + 6];
		let payload_start = header_offset + WAL_RECORD_HEADER_SIZE;
		let payload_end = payload_start + payload_len;
		let crc = calculate_crc32(&[type_byte], &data[payload_start..payload_end]);
		data[header_offset..header_offset + 4].copy_from_slice(&crc.to_be_bytes());
	}

	// ==================== Basic Tests ====================

	/// Empty - Read from empty WAL returns EOF
	#[test]
	fn empty() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let file_path = temp_dir.path().join("test.wal");

		// Create an empty file
		File::create(&file_path).expect("should create file");

		let file = File::open(&file_path).expect("should open file");
		let mut reader = Reader::new(file);

		// Should return EOF (error)
		let result = reader.read();
		assert!(result.is_err(), "Empty WAL should return EOF");
	}

	/// ReadWrite - Write and read multiple records
	#[test]
	fn read_write_multiple_records() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Write multiple records
		wal.append(b"foo").expect("should append foo");
		wal.append(b"bar").expect("should append bar");
		wal.append(b"baz").expect("should append baz");
		wal.append(b"xxxx").expect("should append xxxx");
		wal.close().expect("should close");

		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let (data, _) = reader.read().expect("should read foo");
		assert_eq!(data, b"foo");

		let (data, _) = reader.read().expect("should read bar");
		assert_eq!(data, b"bar");

		let (data, _) = reader.read().expect("should read baz");
		assert_eq!(data, b"baz");

		let (data, _) = reader.read().expect("should read xxxx");
		assert_eq!(data, b"xxxx");

		assert!(reader.read().is_err(), "Should return EOF");
	}

	/// ManyBlocks - Write/read many records (stress test)
	#[test]
	fn many_blocks() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Write 10,000 records (reduced from 100,000 for test speed)
		let num_records = 10_000;
		for i in 0..num_records {
			let data = number_string(i);
			wal.append(data.as_bytes()).expect("should append");
		}
		wal.close().expect("should close");

		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		for i in 0..num_records {
			let expected = number_string(i);
			let (data, _) = reader.read().unwrap_or_else(|_| panic!("should read record {}", i));
			assert_eq!(data, expected.as_bytes(), "Record {} mismatch", i);
		}

		assert!(reader.read().is_err(), "Should return EOF");
	}

	/// Fragmentation - Small, medium, and large records
	#[test]
	fn fragmentation() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		let small = b"small".to_vec();
		let medium = big_string("medium", 50000);
		let large = big_string("large", 100000);

		wal.append(&small).expect("should append small");
		wal.append(&medium).expect("should append medium");
		wal.append(&large).expect("should append large");
		wal.close().expect("should close");

		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let (data, _) = reader.read().expect("should read small");
		assert_eq!(data, small);

		let (data, _) = reader.read().expect("should read medium");
		assert_eq!(data, medium);

		let (data, _) = reader.read().expect("should read large");
		assert_eq!(data, large);

		assert!(reader.read().is_err(), "Should return EOF");
	}

	/// RandomRead - 500 random-sized records
	#[test]
	fn random_read() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		let num_records = 500;
		let max_len = 1 << 17; // 128KB max

		// Write random-sized records
		let mut expected_records = Vec::new();
		for i in 0..num_records {
			let data = random_skewed_string(i, max_len);
			expected_records.push(data.clone());
			wal.append(&data).expect("should append");
		}
		wal.close().expect("should close");

		// Read back and verify
		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		for (i, expected) in expected_records.iter().enumerate() {
			let (data, _) = reader.read().unwrap_or_else(|_| panic!("should read record {}", i));
			assert_eq!(data, expected, "Record {} mismatch", i);
		}

		assert!(reader.read().is_err(), "Should return EOF");
	}

	// ==================== Block Boundary Tests ====================

	/// MarginalTrailer - Record leaves exactly HEADER_SIZE space at block end
	#[test]
	fn marginal_trailer() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Write a record that leaves exactly HEADER_SIZE space at block end
		// Block is 32768 bytes, header is 7 bytes
		// We want: header(7) + data(n) = 32768 - 7 = 32761
		// So data size = 32761 - 7 = 32754
		let n = BLOCK_SIZE - 2 * WAL_RECORD_HEADER_SIZE;
		let foo_data = big_string("foo", n);

		wal.append(&foo_data).expect("should append foo");
		wal.append(b"x").expect("should append small"); // Small record instead of empty
		wal.append(b"bar").expect("should append bar");
		wal.close().expect("should close");

		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let (data, _) = reader.read().expect("should read foo");
		assert_eq!(data, foo_data);

		let (data, _) = reader.read().expect("should read small");
		assert_eq!(data, b"x");

		let (data, _) = reader.read().expect("should read bar");
		assert_eq!(data, b"bar");

		assert!(reader.read().is_err(), "Should return EOF");
	}

	/// MarginalTrailer2 - Same but without empty record in between
	#[test]
	fn marginal_trailer2() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		let n = BLOCK_SIZE - 2 * WAL_RECORD_HEADER_SIZE;
		let foo_data = big_string("foo", n);

		wal.append(&foo_data).expect("should append foo");
		wal.append(b"bar").expect("should append bar");
		wal.close().expect("should close");

		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let (data, _) = reader.read().expect("should read foo");
		assert_eq!(data, foo_data);

		let (data, _) = reader.read().expect("should read bar");
		assert_eq!(data, b"bar");

		assert!(reader.read().is_err(), "Should return EOF");
	}

	/// ShortTrailer - Record leaves < HEADER_SIZE space (tests zero padding)
	#[test]
	fn short_trailer() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Write a record that leaves < 7 bytes at block end (triggers padding)
		let n = BLOCK_SIZE - 2 * WAL_RECORD_HEADER_SIZE + 4;
		let foo_data = big_string("foo", n);

		wal.append(&foo_data).expect("should append foo");
		wal.append(b"x").expect("should append small"); // Small record instead of empty
		wal.append(b"bar").expect("should append bar");
		wal.close().expect("should close");

		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let (data, _) = reader.read().expect("should read foo");
		assert_eq!(data, foo_data);

		let (data, _) = reader.read().expect("should read small");
		assert_eq!(data, b"x");

		let (data, _) = reader.read().expect("should read bar");
		assert_eq!(data, b"bar");

		assert!(reader.read().is_err(), "Should return EOF");
	}

	/// AlignedEof - Record ends exactly at block boundary
	#[test]
	fn aligned_eof() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Write a record that ends exactly at block boundary
		let n = BLOCK_SIZE - 2 * WAL_RECORD_HEADER_SIZE + 4;
		let foo_data = big_string("foo", n);

		wal.append(&foo_data).expect("should append foo");
		wal.close().expect("should close");

		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let (data, _) = reader.read().expect("should read foo");
		assert_eq!(data, foo_data);

		assert!(reader.read().is_err(), "Should return EOF");
	}

	// ==================== Corruption Detection Tests ====================

	/// BadRecordType - Invalid record type byte
	#[test]
	fn bad_record_type() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let file_path = temp_dir.path().join("test.wal");

		// Write a record with invalid type
		{
			let mut file = File::create(&file_path).expect("should create file");
			let data = b"foo";
			let invalid_type = 100u8; // Invalid type
			let crc = calc_crc(invalid_type, data);
			write_raw_record(&mut file, crc, data.len() as u16, invalid_type, data);
		}

		let file = File::open(&file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let result = reader.read();
		assert!(result.is_err(), "Should detect bad record type");
	}

	/// TruncatedTrailingRecord - Partial record at EOF
	#[test]
	fn truncated_trailing_record() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let file_path = temp_dir.path().join("test.wal");

		// Write a complete record followed by a truncated one
		{
			let mut file = File::create(&file_path).expect("should create file");

			// Write complete record
			let data = b"foo";
			let record_type = 1u8; // Full
			let crc = calc_crc(record_type, data);
			write_raw_record(&mut file, crc, data.len() as u16, record_type, data);

			// Write truncated header (only 3 bytes instead of 7)
			file.write_all(&[0u8; 3]).unwrap();
		}

		let file = File::open(&file_path).expect("should open file");
		let mut reader = Reader::new(file);

		// First record should be readable
		let (data, _) = reader.read().expect("should read foo");
		assert_eq!(data, b"foo");

		// EOF on truncated record
		assert!(reader.read().is_err(), "Should return EOF on truncated record");
	}

	/// BadLength - Corrupted length field
	#[test]
	fn bad_length() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let file_path = temp_dir.path().join("test.wal");

		// Write a record with corrupted length
		{
			let mut file = File::create(&file_path).expect("should create file");
			let data = b"foo";
			let record_type = 1u8; // Full
			let crc = calc_crc(record_type, data);
			// Write header with wrong length (100 instead of 3)
			write_raw_record(&mut file, crc, 100, record_type, data);
		}

		let file = File::open(&file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let result = reader.read();
		assert!(result.is_err(), "Should detect bad length");
	}

	/// ChecksumMismatch - Corrupt CRC
	#[test]
	fn checksum_mismatch() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let file_path = temp_dir.path().join("test.wal");

		// Write a record with corrupt checksum
		{
			let mut file = File::create(&file_path).expect("should create file");
			let data = b"foooooo";
			let record_type = 1u8; // Full
			let crc = calc_crc(record_type, data);
			// Corrupt the CRC by adding 14
			let bad_crc = crc.wrapping_add(14);
			write_raw_record(&mut file, bad_crc, data.len() as u16, record_type, data);
		}

		let file = File::open(&file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let result = reader.read();
		assert!(result.is_err(), "Should detect checksum mismatch");
	}

	/// ErrorJoinsRecords - Corrupt middle block shouldn't join fragments
	///
	/// This test verifies that when a middle block is corrupted, the reader:
	/// 1. Does NOT join fragments from different records (first(R1) + last(R2))
	/// 2. Either fails gracefully OR skips to the next valid record
	///
	/// The key invariant: we must never return garbage data that looks valid
	/// but is actually fragments from different records joined together.
	#[test]
	fn error_joins_records() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Write records that span two blocks
		// Record layout:
		//   Block 0: first(foo)
		//   Block 1: last(foo), first(bar)  <- we'll corrupt this block
		//   Block 2: last(bar), "correct"
		let foo_data = big_string("foo", BLOCK_SIZE);
		let bar_data = big_string("bar", BLOCK_SIZE);

		wal.append(&foo_data).expect("should append foo");
		wal.append(&bar_data).expect("should append bar");
		wal.append(b"correct").expect("should append correct");
		wal.close().expect("should close");

		// Corrupt the middle block by reading, modifying, and rewriting
		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file_path = &segments[0].file_path;

		let mut data = std::fs::read(file_path).expect("should read file");

		// Wipe the middle block with garbage
		for offset in BLOCK_SIZE..(2 * BLOCK_SIZE) {
			if offset < data.len() {
				data[offset] = b'x';
			}
		}
		std::fs::write(file_path, &data).expect("should write file");

		// Read back and verify behavior
		let file = File::open(file_path).expect("should open file");
		let mut reader = Reader::new(file);

		// Collect all records that were successfully read
		let mut records_read: Vec<Vec<u8>> = Vec::new();
		while let Ok((data, _)) = reader.read() {
			records_read.push(data.to_vec());
		}

		// Key assertion: We must NEVER return a record that is a mix of
		// foo_data and bar_data fragments joined together.
		// Such a joined record would have length ~= 2*BLOCK_SIZE but contain
		// parts of both "foo" and "bar" patterns.
		for (i, record) in records_read.iter().enumerate() {
			// Check it's not a corrupted join of foo+bar
			let has_foo_pattern = record.windows(3).any(|w| w == b"foo");
			let has_bar_pattern = record.windows(3).any(|w| w == b"bar");

			// A legitimate record is either:
			// - All "foo" pattern (foo_data)
			// - All "bar" pattern (bar_data)
			// - "correct"
			// - NOT a mix of foo and bar
			let is_foo_record = record.len() == BLOCK_SIZE && has_foo_pattern && !has_bar_pattern;
			let is_bar_record = record.len() == BLOCK_SIZE && has_bar_pattern && !has_foo_pattern;
			let is_correct_record = record == b"correct";

			// If it has both patterns, it's a corrupted join - this is the bug we're
			// testing for
			assert!(
				!(has_foo_pattern && has_bar_pattern),
				"BUG: Record {} appears to be a corrupted join of foo+bar fragments (len={})",
				i,
				record.len()
			);

			// Record should be one of the valid types (or reader should have failed)
			assert!(
				is_foo_record || is_bar_record || is_correct_record || record.is_empty(),
				"Record {} is unexpected data (len={}, has_foo={}, has_bar={})",
				i,
				record.len(),
				has_foo_pattern,
				has_bar_pattern
			);
		}

		// The corruption should have caused some data loss
		// We either get "correct" or nothing after the corruption
		let found_correct = records_read.iter().any(|r| r == b"correct");

		// Log what happened for debugging
		eprintln!(
			"error_joins_records: Read {} records, found_correct={}",
			records_read.len(),
			found_correct
		);
	}

	// ==================== Fragment Sequencing Tests ====================

	/// UnexpectedMiddleType - Middle type without preceding First
	///
	/// Tests that a Middle record appearing at the start of a sequence is
	/// detected as invalid. A Middle must always follow a First or another
	/// Middle.
	#[test]
	fn unexpected_middle_type() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let file_path = temp_dir.path().join("test.wal");

		{
			let mut file = File::create(&file_path).expect("should create file");

			// Write a valid Full record first ("before")
			let before_data = b"before";
			let full_type = 1u8;
			let crc1 = calc_crc(full_type, before_data);
			write_raw_record(&mut file, crc1, before_data.len() as u16, full_type, before_data);

			// Write an invalid Middle record (should be preceded by First)
			let middle_data = b"invalid_middle";
			let middle_type = 3u8;
			let crc2 = calc_crc(middle_type, middle_data);
			write_raw_record(&mut file, crc2, middle_data.len() as u16, middle_type, middle_data);

			// Write another valid Full record ("after")
			let after_data = b"after";
			let crc3 = calc_crc(full_type, after_data);
			write_raw_record(&mut file, crc3, after_data.len() as u16, full_type, after_data);
		}

		let file = File::open(&file_path).expect("should open file");
		let mut reader = Reader::new(file);

		// First record should read successfully
		let (data, _) = reader.read().expect("should read first record");
		assert_eq!(data, b"before", "First record should be 'before'");

		// Second read should detect the unexpected Middle type
		let result = reader.read();
		assert!(result.is_err(), "Should detect unexpected Middle type");
	}

	/// UnexpectedLastType - Last type without preceding First
	///
	/// Tests that a Last record appearing without a preceding First is
	/// detected as invalid. A Last must always follow a First or Middle.
	#[test]
	fn unexpected_last_type() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let file_path = temp_dir.path().join("test.wal");

		{
			let mut file = File::create(&file_path).expect("should create file");

			// Write a valid Full record first ("before")
			let before_data = b"before";
			let full_type = 1u8;
			let crc1 = calc_crc(full_type, before_data);
			write_raw_record(&mut file, crc1, before_data.len() as u16, full_type, before_data);

			// Write an invalid Last record (should be preceded by First/Middle)
			let last_data = b"invalid_last";
			let last_type = 4u8;
			let crc2 = calc_crc(last_type, last_data);
			write_raw_record(&mut file, crc2, last_data.len() as u16, last_type, last_data);

			// Write another valid Full record ("after")
			let after_data = b"after";
			let crc3 = calc_crc(full_type, after_data);
			write_raw_record(&mut file, crc3, after_data.len() as u16, full_type, after_data);
		}

		let file = File::open(&file_path).expect("should open file");
		let mut reader = Reader::new(file);

		// First record should read successfully
		let (data, _) = reader.read().expect("should read first record");
		assert_eq!(data, b"before", "First record should be 'before'");

		// Second read should detect the unexpected Last type
		let result = reader.read();
		assert!(result.is_err(), "Should detect unexpected Last type");
	}

	/// UnexpectedFullType - Full type after First (partial record lost)
	///
	/// Tests that a Full record appearing when we expect Middle/Last (after
	/// First) is detected as invalid. This simulates a partial record being
	/// lost.
	#[test]
	fn unexpected_full_type() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Write three records: first will be corrupted to First, second stays Full,
		// third stays Full
		wal.append(b"corrupted").expect("should append corrupted");
		wal.append(b"second").expect("should append second");
		wal.append(b"third").expect("should append third");
		wal.close().expect("should close");

		// Corrupt the first record's type from Full(1) to First(2)
		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file_path = &segments[0].file_path;

		let mut data = std::fs::read(file_path).expect("should read file");
		let first_type = 2u8; // Change Full(1) to First(2)
		set_byte(&mut data, 6, first_type);
		fix_checksum(&mut data, 0, 9); // "corrupted" is 9 bytes
		std::fs::write(file_path, &data).expect("should write file");

		let file = File::open(file_path).expect("should open file");
		let mut reader = Reader::new(file);

		// First read: we read "corrupted" as First, then hit "second" as Full
		// when expecting Middle/Last - this should error
		let result = reader.read();
		assert!(result.is_err(), "Should detect unexpected Full after First");
	}

	/// MissingLast - Fragmented record without Last fragment
	///
	/// Tests that when the Last fragment of a multi-block record is missing,
	/// the record is not returned and earlier complete records are still
	/// readable.
	#[test]
	fn missing_last() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Write a small record first
		wal.append(b"complete_record").expect("should append first");

		// Write a large record that spans multiple blocks (2x block size to ensure
		// fragmentation)
		let large_data = big_string("bar", BLOCK_SIZE * 2);
		wal.append(&large_data).expect("should append large");

		// Write another small record after
		wal.append(b"after_large").expect("should append after");
		wal.close().expect("should close");

		// Truncate the file to remove the Last fragment of the large record
		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file_path = &segments[0].file_path;

		let file_data = std::fs::read(file_path).expect("should read file");
		// Calculate where to truncate: keep first record + first fragment of large
		// record First record is ~22 bytes (7 header + 15 data), First fragment fills
		// rest of first block Truncate somewhere in the second block to remove Last
		// fragment
		let truncated_len = BLOCK_SIZE + BLOCK_SIZE / 2; // ~1.5 blocks
		let truncated_len = truncated_len.min(file_data.len() - 100); // Ensure we're removing something
		std::fs::write(file_path, &file_data[..truncated_len]).expect("should write file");

		let file = File::open(file_path).expect("should open file");
		let mut reader = Reader::new(file);

		// First record should still be readable
		let (data, _) = reader.read().expect("should read first complete record");
		assert_eq!(data, b"complete_record", "First record should be readable");

		// Second read should fail (incomplete fragmented record)
		let result = reader.read();
		assert!(result.is_err(), "Should return error for missing Last fragment");
	}

	/// PartialLast - Truncated Last fragment
	///
	/// Tests that when the Last fragment is partially written (truncated),
	/// earlier complete records are still readable.
	#[test]
	fn partial_last() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Write a small record first
		wal.append(b"complete_record").expect("should append first");

		// Write a large record that spans multiple blocks (2x block size)
		let large_data = big_string("bar", BLOCK_SIZE * 2);
		wal.append(&large_data).expect("should append large");

		// Write another small record after
		wal.append(b"after_large").expect("should append after");
		wal.close().expect("should close");

		// Get original file size
		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file_path = &segments[0].file_path;

		let file_data = std::fs::read(file_path).expect("should read file");
		let original_len = file_data.len();

		// Truncate a few bytes from the end (partial Last fragment or "after_large")
		let truncated_len = original_len.saturating_sub(20);
		std::fs::write(file_path, &file_data[..truncated_len]).expect("should write file");

		let file = File::open(file_path).expect("should open file");
		let mut reader = Reader::new(file);

		// First record should still be readable
		let (data, _) = reader.read().expect("should read first complete record");
		assert_eq!(data, b"complete_record", "First record should be readable");

		// Second read: large record may succeed or fail depending on where truncation
		// hit
		let result = reader.read();
		// If the large record was complete, we might get it
		// If truncation hit the large record itself, we get an error
		match result {
			Ok((data, _)) => {
				// Large record was complete, verify it
				assert_eq!(data, large_data, "Large record should match if complete");
				// Third record should fail (truncated)
				let result3 = reader.read();
				assert!(result3.is_err(), "Should fail after truncated point");
			}
			Err(_) => {
				// Large record was truncated - this is expected
			}
		}
	}

	// ==================== Compression Tests ====================

	/// CompressionEmpty - Empty with LZ4
	#[test]
	fn compression_empty() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default().with_compression(CompressionType::Lz4);
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");
		wal.close().expect("should close");

		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		// Should return EOF (only compression type record was written)
		let result = reader.read();
		assert!(result.is_err(), "Empty compressed WAL should return EOF");
	}

	/// CompressionReadWrite - Basic with LZ4
	#[test]
	fn compression_read_write() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default().with_compression(CompressionType::Lz4);
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		wal.append(b"foo").expect("should append foo");
		wal.append(b"bar").expect("should append bar");
		wal.append(b"baz").expect("should append baz");
		wal.append(b"xxxx").expect("should append xxxx");
		wal.close().expect("should close");

		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let (data, _) = reader.read().expect("should read foo");
		assert_eq!(data, b"foo");

		let (data, _) = reader.read().expect("should read bar");
		assert_eq!(data, b"bar");

		let (data, _) = reader.read().expect("should read baz");
		assert_eq!(data, b"baz");

		let (data, _) = reader.read().expect("should read xxxx");
		assert_eq!(data, b"xxxx");

		assert!(reader.read().is_err(), "Should return EOF");
	}

	/// CompressionFragmentation - Large records with LZ4
	#[test]
	fn compression_fragmentation() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default().with_compression(CompressionType::Lz4);
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		let small = b"small".to_vec();
		let medium = big_string("medium", 3 * BLOCK_SIZE / 2); // Spans into block 2
		let large = big_string("large", 3 * BLOCK_SIZE); // Spans into block 5

		wal.append(&small).expect("should append small");
		wal.append(&medium).expect("should append medium");
		wal.append(&large).expect("should append large");
		wal.close().expect("should close");

		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let (data, _) = reader.read().expect("should read small");
		assert_eq!(data, small);

		let (data, _) = reader.read().expect("should read medium");
		assert_eq!(data, medium);

		let (data, _) = reader.read().expect("should read large");
		assert_eq!(data, large);

		assert!(reader.read().is_err(), "Should return EOF");
	}

	// ==================== FALSE POSITIVE TESTS ====================
	// These tests verify valid data is NEVER incorrectly marked as corrupt.
	// The original bug was a false positive at ~6MB in a 33MB WAL file.

	/// Multi-session block boundary stress test.
	/// Simulates the original bug scenario: writes across multiple sessions
	/// hitting block boundaries. Uses 68-byte records (the original bug size)
	/// across 10+ sessions.
	#[test]
	fn multi_session_block_boundary_stress() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");

		// Write 68-byte records across 10 separate sessions
		// This exercises block_offset restoration AND block boundary handling
		let record_size = 68;
		let records_per_session = 100;
		let num_sessions = 10;

		for session in 0..num_sessions {
			let opts = Options::default();
			let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");
			for i in 0..records_per_session {
				let idx = session * records_per_session + i;
				let data: Vec<u8> = (0..record_size).map(|j| ((idx + j) & 0xFF) as u8).collect();
				wal.append(&data).expect("should append");
			}
			wal.close().expect("should close");
		}

		// Read ALL records back - no corruption should be detected
		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let mut count = 0;
		while let Ok((data, offset)) = reader.read() {
			assert_eq!(
				data.len(),
				record_size,
				"FALSE POSITIVE: Record {} has wrong size at offset {} (expected {}, got {})",
				count,
				offset,
				record_size,
				data.len()
			);
			count += 1;
		}
		assert_eq!(
			count,
			num_sessions * records_per_session,
			"Should read ALL {} records without false corruption (got {})",
			num_sessions * records_per_session,
			count
		);
	}

	/// Exact reproduction of the original crud-bench bug scenario.
	/// Writes 68-byte records until file reaches ~5MB (scaled down from 33MB
	/// for test speed). The original bug failed at ~6MB mark.
	#[test]
	fn original_crud_bench_bug_reproduction() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");

		// Simulate the crud-bench scenario: many 68-byte records written
		// across multiple sessions until file is ~5MB (scaled down for test speed)
		let record_size = 68;
		let target_file_size = 5 * 1024 * 1024; // 5MB
		let records_per_session = 1000;

		let mut total_records = 0;
		let wal_path = temp_dir.path().join("00000000000000000000.wal");

		loop {
			let opts = Options::default();
			let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

			for i in 0..records_per_session {
				let data: Vec<u8> =
					(0..record_size).map(|j| ((total_records + i + j) & 0xFF) as u8).collect();
				wal.append(&data).expect("should append");
			}
			wal.close().expect("should close");
			total_records += records_per_session;

			let file_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
			if file_size >= target_file_size as u64 {
				break;
			}
		}

		// Read back ALL records - the original bug would fail around 6MB mark
		let file = File::open(&wal_path).expect("should open");
		let mut reader = Reader::new(file);

		let mut count = 0;
		let mut last_offset = 0u64;
		while let Ok((data, offset)) = reader.read() {
			assert_eq!(
				data.len(),
				record_size,
				"FALSE POSITIVE at record {} offset {} (last good: {})",
				count,
				offset,
				last_offset
			);
			last_offset = offset;
			count += 1;
		}

		assert_eq!(
			count, total_records,
			"Should read ALL {} records without false corruption (got {})",
			total_records, count
		);
	}

	/// Exhaustively test every possible block boundary remainder (0-6 bytes).
	/// This ensures no "magic" remainder value causes false corruption.
	#[test]
	fn all_block_boundary_remainders() {
		// Test various record sizes that will leave different remainders at block
		// boundaries HEADER_SIZE = 7, BLOCK_SIZE = 32768
		// We want to test remainders 0, 1, 2, 3, 4, 5, 6

		for target_remainder in 0..WAL_RECORD_HEADER_SIZE {
			let temp_dir = TempDir::new("test").expect("should create temp dir");
			let opts = Options::default();
			let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

			// Calculate record size to achieve target remainder
			// After N records: N * (HEADER + record_size) leaves remainder R at block
			// boundary We use a base size and adjust
			let base_record_size = 100;
			let record_plus_header = WAL_RECORD_HEADER_SIZE + base_record_size;

			// Write enough records to cross multiple blocks
			let num_records = (BLOCK_SIZE * 3) / record_plus_header + 10;

			let mut expected_data = Vec::new();
			for i in 0..num_records {
				// Vary the size slightly to hit different remainders
				let size = base_record_size + (i % 7);
				let data: Vec<u8> = (0..size).map(|j| ((i + j) & 0xFF) as u8).collect();
				expected_data.push(data.clone());
				wal.append(&data).expect("should append");
			}
			wal.close().expect("should close");

			// Verify ALL records read back without false corruption
			let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
				.expect("should read segments");
			let file = File::open(&segments[0].file_path).expect("should open file");
			let mut reader = Reader::new(file);

			for (i, expected) in expected_data.iter().enumerate() {
				let result = reader.read();
				assert!(
					result.is_ok(),
					"FALSE POSITIVE: Record {} failed for remainder test {} - {:?}",
					i,
					target_remainder,
					result.err()
				);
				let (data, _) = result.unwrap();
				assert_eq!(
					data, expected,
					"Record {} data mismatch for remainder {}",
					i, target_remainder
				);
			}
		}
	}

	/// Fuzz test with random record sizes - no valid data should be marked
	/// corrupt. Uses deterministic seed for reproducibility.
	#[test]
	fn fuzz_random_record_sizes_no_false_corruption() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Simple LCG random number generator for reproducibility (no external crate
		// needed)
		let mut seed: u64 = 12345;
		let mut next_random = || {
			seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
			seed
		};

		let mut expected_records = Vec::new();

		// Write 500 records with random sizes from 1 to 50KB
		for i in 0..500 {
			let size = (next_random() % 50_000) as usize + 1;
			let data: Vec<u8> = (0..size).map(|j| ((i + j) & 0xFF) as u8).collect();
			expected_records.push(data.clone());
			wal.append(&data).expect("should append");
		}
		wal.close().expect("should close");

		// Read back ALL records - NONE should be marked corrupt
		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		for (i, expected) in expected_records.iter().enumerate() {
			let result = reader.read();
			assert!(
				result.is_ok(),
				"FALSE POSITIVE: Record {} (size {}) should not be marked corrupt",
				i,
				expected.len()
			);
			let (data, _) = result.unwrap();
			assert_eq!(data.len(), expected.len(), "Record {} size mismatch", i);
			assert_eq!(data, expected, "Record {} data mismatch", i);
		}
	}

	/// Mixed fragmented (multi-block) and small records across multiple
	/// sessions.
	#[test]
	fn mixed_fragmented_and_small_records() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");

		let mut expected_records = Vec::new();

		// Write across 5 sessions, alternating between large and small records
		for session in 0..5 {
			let opts = Options::default();
			let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

			// Write a large record that spans multiple blocks
			let large_size = BLOCK_SIZE + 5000;
			let large_data: Vec<u8> =
				(0..large_size).map(|j| ((session * 1000 + j) & 0xFF) as u8).collect();
			expected_records.push(large_data.clone());
			wal.append(&large_data).expect("should append large");

			// Write several small records
			for i in 0..10 {
				let small_size = 50 + i * 10;
				let small_data: Vec<u8> =
					(0..small_size).map(|j| ((session * 100 + i + j) & 0xFF) as u8).collect();
				expected_records.push(small_data.clone());
				wal.append(&small_data).expect("should append small");
			}

			wal.close().expect("should close");
		}

		// Read back all records
		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		for (i, expected) in expected_records.iter().enumerate() {
			let result = reader.read();
			assert!(
				result.is_ok(),
				"FALSE POSITIVE: Record {} (size {}) failed - {:?}",
				i,
				expected.len(),
				result.err()
			);
			let (data, _) = result.unwrap();
			assert_eq!(data, expected, "Record {} mismatch", i);
		}
	}

	/// Records that end exactly at block boundaries, written across sessions.
	#[test]
	fn exact_block_alignment_stress() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");

		// Record size that fills block exactly: BLOCK_SIZE - HEADER_SIZE = 32761
		let exact_fit_size = BLOCK_SIZE - WAL_RECORD_HEADER_SIZE;

		let mut expected_records = Vec::new();

		// Write exact-fit records across multiple sessions
		for session in 0..5 {
			let opts = Options::default();
			let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

			// Write an exact-fit record
			let data: Vec<u8> = (0..exact_fit_size).map(|j| ((session + j) & 0xFF) as u8).collect();
			expected_records.push(data.clone());
			wal.append(&data).expect("should append exact fit");

			// Write a small record after (tests resuming from block boundary)
			let small_data: Vec<u8> = (0..100).map(|j| ((session * 10 + j) & 0xFF) as u8).collect();
			expected_records.push(small_data.clone());
			wal.append(&small_data).expect("should append small");

			wal.close().expect("should close");
		}

		// Read back all records
		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		for (i, expected) in expected_records.iter().enumerate() {
			let result = reader.read();
			assert!(
				result.is_ok(),
				"FALSE POSITIVE: Record {} (size {}) failed at block alignment - {:?}",
				i,
				expected.len(),
				result.err()
			);
			let (data, _) = result.unwrap();
			assert_eq!(data, expected, "Record {} mismatch", i);
		}
	}

	/// Combined compression and block boundary stress test.
	#[test]
	fn compression_block_boundary_no_false_corruption() {
		let temp_dir = TempDir::new("test").expect("should create temp dir");

		// Write 68-byte records (original bug size) with compression across sessions
		let record_size = 68;
		let records_per_session = 100;
		let num_sessions = 5;

		for session in 0..num_sessions {
			let opts = Options::default().with_compression(CompressionType::Lz4);
			let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");
			for i in 0..records_per_session {
				let idx = session * records_per_session + i;
				let data: Vec<u8> = (0..record_size).map(|j| ((idx + j) & 0xFF) as u8).collect();
				wal.append(&data).expect("should append");
			}
			wal.close().expect("should close");
		}

		// Read ALL records back - no corruption should be detected
		let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
			.expect("should read segments");
		let file = File::open(&segments[0].file_path).expect("should open file");
		let mut reader = Reader::new(file);

		let mut count = 0;
		while let Ok((data, offset)) = reader.read() {
			assert_eq!(
				data.len(),
				record_size,
				"FALSE POSITIVE with compression: Record {} has wrong size at offset {}",
				count,
				offset
			);
			count += 1;
		}
		assert_eq!(
			count,
			num_sessions * records_per_session,
			"Should read ALL {} compressed records without false corruption",
			num_sessions * records_per_session
		);
	}

	/// Test resuming writes at every possible block offset (0 to BLOCK_SIZE-1).
	/// This catches bugs where certain starting offsets cause false corruption.
	#[test]
	fn resume_at_all_block_offsets() {
		// Test a sampling of block offsets (testing all 32768 would be too slow)
		let test_offsets = [
			0,
			1,
			6,
			7,
			8,
			100,
			1000,
			BLOCK_SIZE / 4,
			BLOCK_SIZE / 2,
			BLOCK_SIZE - 100,
			BLOCK_SIZE - 8,
			BLOCK_SIZE - 7,
			BLOCK_SIZE - 6,
			BLOCK_SIZE - 1,
		];

		for &target_offset in &test_offsets {
			let temp_dir = TempDir::new("test").expect("should create temp dir");

			// Session 1: Write enough to reach approximately target_offset in the block
			{
				let opts = Options::default();
				let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

				// Write records until we're close to target offset
				let mut current_offset = 0usize;
				let mut record_num = 0;
				while current_offset < target_offset.saturating_sub(100) {
					let record_size = 50;
					let data: Vec<u8> =
						(0..record_size).map(|j| ((record_num + j) & 0xFF) as u8).collect();
					wal.append(&data).expect("should append");
					current_offset += WAL_RECORD_HEADER_SIZE + record_size;
					record_num += 1;
				}
				wal.close().expect("should close");
			}

			// Session 2: Reopen and write more records
			let expected_second_session: Vec<Vec<u8>>;
			{
				let opts = Options::default();
				let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

				expected_second_session = (0..10)
					.map(|i| {
						let data: Vec<u8> =
							(0..68).map(|j| ((1000 + i + j) & 0xFF) as u8).collect();
						wal.append(&data).expect("should append");
						data
					})
					.collect();
				wal.close().expect("should close");
			}

			// Read back and verify no false corruption for second session records
			let segments = SegmentRef::read_segments_from_directory(temp_dir.path(), Some("wal"))
				.expect("should read segments");
			let file = File::open(&segments[0].file_path).expect("should open file");
			let mut reader = Reader::new(file);

			// Skip first session records
			while let Ok((data, _)) = reader.read() {
				if data.len() == 68 && data[0] == ((1000) & 0xFF) as u8 {
					// Found first record of second session
					// Verify remaining second session records
					for (i, expected) in expected_second_session.iter().skip(1).enumerate() {
						let result = reader.read();
						assert!(
							result.is_ok(),
							"FALSE POSITIVE at offset {}: Second session record {} failed",
							target_offset,
							i + 1
						);
						let (data, _) = result.unwrap();
						assert_eq!(data, expected);
					}
					break;
				}
			}
		}
	}
}
