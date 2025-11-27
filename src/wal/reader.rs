use std::fs::File;
use std::io::{self, BufReader, Read};
use std::vec::Vec;

use crate::wal::format::{
	CompressionType, RecordType, BLOCK_SIZE, HEADER_SIZE as WAL_RECORD_HEADER_SIZE,
};
use crate::wal::segment::{
	calculate_crc32, validate_record_type, CorruptionError, Error, IOError, Result,
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

// Reader reads records from a single WAL file.
pub(crate) struct Reader {
	/// The underlying file reader
	rdr: BufReader<File>,

	/// Buffer for accumulating record fragments
	rec: Vec<u8>,

	/// Buffer for reading blocks
	buf: [u8; BLOCK_SIZE],

	/// Total bytes read so far
	total_read: usize,

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
	///
	#[allow(dead_code)]
	pub(crate) fn with_options(
		file: File,
		reporter: Option<Box<dyn Reporter>>,
		log_number: u64,
	) -> Self {
		Reader {
			rdr: BufReader::with_capacity(BLOCK_SIZE, file),
			rec: Vec::new(),
			buf: [0u8; BLOCK_SIZE],
			total_read: 0,
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

	/// Reads the full record header: [CRC32: 4 bytes][Length: 2 bytes][Type: 1 byte]
	fn read_record_header<R: Read>(rdr: &mut R, buf: &mut [u8]) -> Result<(u32, u16, u8)> {
		match rdr.read_exact(&mut buf[0..WAL_RECORD_HEADER_SIZE]) {
			Ok(_) => {
				// Parse header: CRC (4, LE) + Length (2, LE) + Type (1)
				let crc = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
				let length = u16::from_le_bytes([buf[4], buf[5]]);
				let record_type = buf[6];

				Ok((crc, length, record_type))
			}
			Err(e) => {
				if e.kind() == io::ErrorKind::UnexpectedEof {
					Err(Error::IO(IOError::new(
						io::ErrorKind::UnexpectedEof,
						"reached end of file while reading header",
					)))
				} else {
					Err(Error::IO(IOError::new(
						io::ErrorKind::Other,
						"error reading record header",
					)))
				}
			}
		}
	}

	fn read_and_validate_record<R: Read>(
		rdr: &mut R,
		buf: &mut [u8],
		length: u16,
		crc: u32,
		rec_type: &RecordType,
		type_byte: u8,
		current_index: usize,
	) -> Result<(usize, usize)> {
		// Validate the record type.
		validate_record_type(rec_type, current_index)?;

		let record_start = WAL_RECORD_HEADER_SIZE;
		let record_end = record_start + length as usize;
		match rdr.read_exact(&mut buf[record_start..record_end]) {
			Ok(_) => {
				// Validate the checksum using the type byte and payload
				let calculated_crc = calculate_crc32(&[type_byte], &buf[record_start..record_end]);

				if calculated_crc != crc {
					return Err(Error::IO(IOError::new(
						io::ErrorKind::Other,
						"unexpected checksum",
					)));
				}
				Ok((record_start, record_end))
			}
			Err(e) => {
				if e.kind() == io::ErrorKind::UnexpectedEof {
					Err(Error::IO(IOError::new(
						io::ErrorKind::UnexpectedEof,
						"reached end of file while reading record data",
					)))
				} else {
					Err(Error::IO(IOError::new(io::ErrorKind::Other, "error reading record data")))
				}
			}
		}
	}

	pub(crate) fn read(&mut self) -> Result<(&[u8], u64)> {
		if let Some(err) = &self.err {
			return Err(err.clone());
		}

		match self.next() {
			Ok(_) => Ok((&self.rec, self.total_read as u64)),
			Err(e) => {
				// Check if the error is an UnexpectedEof
				if let Error::IO(io_err) = &e {
					if io_err.kind() == io::ErrorKind::UnexpectedEof {
						self.err = Some(e.clone());
						return Err(e);
					}
				}

				// Fixed behavior: Tolerate tail corruption, enable repair
				let corruption_err = Error::Corruption(CorruptionError::new(
					io::ErrorKind::Other,
					e.to_string().as_str(),
					self.log_number,
					self.total_read as u64,
				));

				// Report corruption and return error (caller will handle repair)
				self.report_corruption(0, &e.to_string());
				self.err = Some(corruption_err.clone());
				Err(corruption_err)
			}
		}
	}

	fn next(&mut self) -> Result<()> {
		self.rec.clear();
		let mut i = 0;

		loop {
			// Read full header: CRC (4) + Length (2) + Type (1)
			let (crc, length, type_byte) = Self::read_record_header(&mut self.rdr, &mut self.buf)?;
			self.total_read += WAL_RECORD_HEADER_SIZE;
			self.cur_rec_type = RecordType::from_u8(type_byte)?;

			// If the type is Empty (0), it's a padded block.
			// Read the rest of the block of zeros and continue.
			if self.cur_rec_type == RecordType::Empty {
				let remaining = BLOCK_SIZE - (self.total_read % BLOCK_SIZE);
				if remaining == BLOCK_SIZE {
					continue;
				}

				let zeros =
					&mut self.buf[WAL_RECORD_HEADER_SIZE..WAL_RECORD_HEADER_SIZE + remaining];
				if self.rdr.read_exact(zeros).is_err() {
					return Err(Error::IO(IOError::new(
						io::ErrorKind::Other,
						"error reading remaining zeros",
					)));
				}
				self.total_read += remaining;

				if !zeros.iter().all(|&c| c == 0) {
					return Err(Error::IO(IOError::new(
						io::ErrorKind::Other,
						"non-zero byte in current block",
					)));
				}
				continue;
			}

			// Handle SetCompressionType metadata record
			if self.cur_rec_type == RecordType::SetCompressionType {
				let record_start = WAL_RECORD_HEADER_SIZE;
				let record_end = record_start + length as usize;
				self.rdr.read_exact(&mut self.buf[record_start..record_end])?;

				// Parse and store compression type
				let compression_byte = self.buf[record_start];
				self.compression_type = CompressionType::from_u8(compression_byte)?;
				self.compression_type_record_read = true;

				continue; // Don't return this as a data record
			}

			// Read and validate the record data.
			let (record_start, record_end) = Self::read_and_validate_record(
				&mut self.rdr,
				&mut self.buf,
				length,
				crc,
				&self.cur_rec_type,
				type_byte,
				i,
			)?;
			self.total_read += length as usize;

			// Copy the record data to the output buffer.
			self.rec.extend_from_slice(&self.buf[record_start..record_end]);

			if self.cur_rec_type == RecordType::Last || self.cur_rec_type == RecordType::Full {
				break;
			}

			i += 1;
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
	use std::io::BufReader;
	use std::io::{Read, Write};
	use std::vec::Vec;
	use test_log::test;

	use crate::wal::manager::Wal;
	use crate::wal::metadata::Options;
	use crate::wal::segment::SegmentRef;
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

	// Reader tests removed - comprehensive tests exist in manager.rs, writer.rs, and integration tests
	// The reader functionality is tested through:
	// - tests.rs integration tests
	// - recovery.rs recovery tests
	// - manager.rs end-to-end tests

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

	// Legacy multi-segment Reader tests removed
	// Comprehensive coverage exists in integration tests (tests.rs, recovery.rs)

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
			// assert_eq!(data.len() + WAL_RECORD_HEADER_SIZE, r.unwrap().1);
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
}
