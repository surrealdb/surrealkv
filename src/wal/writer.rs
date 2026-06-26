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

	/// Buffer for compressed data (allocated when compression is enabled).
	#[allow(dead_code)]
	compressed_buffer: Option<Vec<u8>>,
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
		// Allocate compressed buffer if compression is enabled
		let compressed_buffer = if compression_type != CompressionType::None {
			Some(Vec::with_capacity(BLOCK_SIZE))
		} else {
			None
		};

		Self {
			dest,
			block_offset,
			manual_flush,
			compression_type,
			compressed_buffer,
		}
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

		// Flush if not in manual mode
		if !self.manual_flush {
			self.write_buffer()?;
		}

		Ok(())
	}

	/// Adds a compression type record at the start of the WAL.
	///
	/// This should be called before any data records are written.
	pub fn add_compression_type_record(&mut self) -> Result<()> {
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
			let padding = vec![0u8; leftover];
			self.dest.append(&padding)?;
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
		let mut header = Vec::with_capacity(HEADER_SIZE);
		header.extend_from_slice(&crc.to_be_bytes());
		header.extend_from_slice(&(length as u16).to_be_bytes());
		header.push(record_type as u8);

		self.dest.append(&header)?;
		self.dest.append(data)?;

		self.block_offset += HEADER_SIZE + length;

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::fs::File;

	use tempdir::TempDir;

	use super::*;

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
}
