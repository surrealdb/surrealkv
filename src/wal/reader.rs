use std::fs::File;
use std::io::{self, Read};
use std::vec::Vec;

use crate::wal::{
	calculate_crc32, validate_record_type, CompressionType, CorruptionError, Error, IOError,
	RecordType, Result, BLOCK_SIZE, HEADER_SIZE as WAL_RECORD_HEADER_SIZE,
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
/// remain, the remainder is discarded (it's padding) and the next block is read.
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
		let crc = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
		let length = u16::from_le_bytes([header[4], header[5]]);
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

				// Fixed behavior: Tolerate tail corruption, enable repair
				let offset = self.end_of_buffer_offset - self.buffer_remaining();
				let corruption_err = Error::Corruption(CorruptionError::new(
					io::ErrorKind::Other,
					e.to_string().as_str(),
					self.log_number,
					offset as u64,
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
			validate_record_type(&self.cur_rec_type, fragment_index)?;

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
	use super::*;
	use std::fs::File;
	use std::io::{BufReader, Read, Write};
	use std::vec::Vec;
	use test_log::test;

	use crate::wal::manager::Wal;
	use crate::wal::{Options, SegmentRef};
	use tempdir::TempDir;

	// BufferReader does not return EOF when the underlying reader returns 0 bytes read.
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
		// This size, with 7-byte headers, will eventually leave < 7 bytes at a block boundary
		// BLOCK_SIZE = 32768, record + header = 75 bytes
		// 32768 / 75 = 436.9... so after 436 records, we have 32768 - (436 * 75) = 68 bytes left
		// After one more record (437), we have 68 - 75 = -7, meaning we wrap to next block
		// The key is that various record counts will leave different remainders
		let record_size = 68;
		let records_to_write = 1000; // Enough to cross multiple block boundaries

		for i in 0..records_to_write {
			let data: Vec<u8> = (0..record_size).map(|j| ((i + j) & 0xFF) as u8).collect();
			wal.append(&data).expect("should append");
		}
		wal.close().expect("should close");

		// Read back all records - this would fail with "Invalid Record Type" before the fix
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
}
