use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use super::writer::Writer;
use super::{
	get_segment_range,
	segment_name,
	BufferedFileWriter,
	CompressionType,
	Error,
	IOError,
	Options,
	RecordType,
	Result,
	BLOCK_SIZE,
	HEADER_SIZE,
};

/// Write-Ahead Log (Wal) manager for coordinating WAL operations.
pub struct Wal {
	/// The currently active Writer for writing records.
	active_writer: Writer,

	/// The log number of the currently active Writer.
	active_log_number: u64,

	/// The directory where the WAL files are located.
	dir: PathBuf,

	/// Configuration options for the WAL instance.
	opts: Options,

	/// A flag indicating whether the WAL instance is closed or not.
	closed: bool,
}

impl Wal {
	/// Opens or creates a new WAL instance.
	pub(crate) fn open(dir: &Path, opts: Options) -> Result<Self> {
		// Ensure the options are valid
		opts.validate()?;

		// Ensure the directory exists with proper permissions
		Self::prepare_directory(dir, &opts)?;

		// Clean up any stale .wal.repair files from previous crashed repair attempts
		Self::cleanup_stale_repair_files(dir)?;

		// Determine the active log number
		let active_log_number = Self::calculate_active_log_number(dir)?;

		// Create the active Writer
		let active_writer = Self::create_writer(dir, active_log_number, &opts)?;

		Ok(Self {
			active_writer,
			active_log_number,
			dir: dir.to_path_buf(),
			opts,
			closed: false,
		})
	}

	/// Opens or creates a WAL instance starting at a specific log number.
	///
	/// This is used when the manifest indicates a specific log_number should be
	/// used, avoiding the creation of intermediate empty WAL files.
	///
	/// # Arguments
	///
	/// * `dir` - WAL directory path
	/// * `min_log_number` - Minimum log number (actual may be higher if existing segments exist)
	/// * `opts` - WAL options
	///
	/// # Behavior
	///
	/// Uses `max(min_log_number, highest_on_disk)` as the actual starting log
	/// number. This ensures we never write to a log number lower than existing
	/// segments on disk.
	pub(crate) fn open_with_min_log_number(
		dir: &Path,
		min_log_number: u64,
		opts: Options,
	) -> Result<Self> {
		// Ensure the options are valid
		opts.validate()?;

		// Ensure the directory exists with proper permissions
		Self::prepare_directory(dir, &opts)?;

		// Clean up any stale .wal.repair files from previous crashed repair attempts
		Self::cleanup_stale_repair_files(dir)?;

		// Use max of min_log_number and highest existing segment on disk.
		// This prevents writing to an old segment that could be skipped on recovery.
		//
		// Example scenario this fixes:
		// 1. Crash with segments #5, #6, #7 on disk
		// 2. Recovery replays all three into memtable
		// 3. Without this fix, new writes would go to segment #5
		// 4. If memtable flushes (log_number = 6), then crash again
		// 5. Segment #5 would be skipped, losing the new writes
		let highest_on_disk = Self::calculate_active_log_number(dir).unwrap_or(0);
		let active_log_number = std::cmp::max(min_log_number, highest_on_disk);

		if active_log_number > min_log_number {
			log::info!(
				"WAL: advancing from min_log_number {} to {} (highest existing segment)",
				min_log_number,
				active_log_number
			);
		}

		// Create the active Writer at the calculated log number
		let active_writer = Self::create_writer(dir, active_log_number, &opts)?;

		Ok(Self {
			active_writer,
			active_log_number,
			dir: dir.to_path_buf(),
			opts,
			closed: false,
		})
	}

	/// Creates a new Writer for the given log number.
	/// If the segment file already exists, appends to it instead of
	/// overwriting.
	fn create_writer(dir: &Path, log_number: u64, opts: &Options) -> Result<Writer> {
		let extension = opts.file_extension.as_deref().unwrap_or("wal");
		let file_name = segment_name(log_number, extension);
		let file_path = dir.join(&file_name);

		// Open or create the file (append mode ensures writes go to end)
		let file = Self::open_wal_file(&file_path, opts)?;

		// Get file size from the opened file handle
		let existing_size = file.metadata()?.len();

		if existing_size > 0 {
			// Existing file: detect the compression type from the file itself.
			// This prevents compression mismatch bugs when reopening with different
			// options.
			let detected_compression = Self::detect_compression_type(&file_path)?;

			// Calculate block_offset from file size
			let block_offset = (existing_size as usize) % BLOCK_SIZE;

			// Create buffered file writer
			let buffered_writer = BufferedFileWriter::new(file, BLOCK_SIZE);

			Ok(Writer::new(buffered_writer, false, detected_compression, block_offset))
		} else {
			// New file - use compression type from options
			let compression_type = opts.compression_type;

			// Create buffered file writer
			let buffered_writer = BufferedFileWriter::new(file, BLOCK_SIZE);

			let mut writer = Writer::new(buffered_writer, false, compression_type, 0);
			if compression_type != CompressionType::None {
				writer.add_compression_type_record()?;
			}
			Ok(writer)
		}
	}

	/// Detects the compression type used in an existing WAL file.
	///
	/// Reads the first record's header. If it's a SetCompressionType record,
	/// parses and returns that compression type. Otherwise returns None (no
	/// compression).
	fn detect_compression_type(file_path: &Path) -> Result<CompressionType> {
		let mut file = File::open(file_path)?;

		// Read just enough for the header
		let mut header = [0u8; HEADER_SIZE];
		match file.read_exact(&mut header) {
			Ok(_) => {}
			Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
				// File too short, assume no compression
				return Ok(CompressionType::None);
			}
			Err(e) => return Err(Error::IO(IOError::new(e.kind(), &e.to_string()))),
		}

		// Parse the record type from header byte 6
		let record_type_byte = header[6];
		let record_type = RecordType::from_u8(record_type_byte)?;

		if record_type == RecordType::SetCompressionType {
			// Read the compression type byte (length is in bytes 4-5)
			let length = u16::from_be_bytes([header[4], header[5]]);
			if length >= 1 {
				let mut compression_byte = [0u8; 1];
				file.read_exact(&mut compression_byte)?;
				return CompressionType::from_u8(compression_byte[0]);
			}
		}

		// First record is not SetCompressionType, so file has no compression
		Ok(CompressionType::None)
	}

	fn open_wal_file(file_path: &Path, opts: &Options) -> Result<File> {
		let mut open_options = OpenOptions::new();
		open_options.read(true).write(true).create(true).append(true);

		#[cfg(unix)]
		{
			use std::os::unix::fs::OpenOptionsExt;
			if let Some(file_mode) = opts.file_mode {
				open_options.mode(file_mode);
			}
		}

		Ok(open_options.open(file_path)?)
	}

	fn prepare_directory(dir: &Path, opts: &Options) -> Result<()> {
		// Directory should already be created by Tree::new()
		// Just set permissions if needed
		if let Ok(metadata) = fs::metadata(dir) {
			let mut permissions = metadata.permissions();

			#[cfg(unix)]
			{
				use std::os::unix::fs::PermissionsExt;
				permissions.set_mode(opts.dir_mode.unwrap_or(0o750));
			}

			#[cfg(windows)]
			{
				permissions.set_readonly(false);
			}

			fs::set_permissions(dir, permissions)?;
		}

		Ok(())
	}

	fn calculate_active_log_number(dir: &Path) -> Result<u64> {
		let (_, last) = get_segment_range(dir, Some("wal"))?;
		// Return the last segment to append to, or 0 if no segments exist
		Ok(last)
	}

	/// Cleans up stale .wal.repair files from crashed repair processes.
	fn cleanup_stale_repair_files(dir: &Path) -> Result<()> {
		// Check if the directory exists
		if !dir.exists() {
			return Ok(());
		}

		// Read all files in the WAL directory
		let entries = match fs::read_dir(dir) {
			Ok(entries) => entries,
			Err(e) if e.kind() == io::ErrorKind::NotFound => {
				return Ok(());
			}
			Err(e) => {
				return Err(Error::IO(IOError::new(
					io::ErrorKind::Other,
					&format!("Failed to read WAL directory: {}", e),
				)));
			}
		};

		let mut removed_count = 0;

		// Find and remove all .wal.repair files
		for entry in entries.flatten() {
			let path = entry.path();
			if let Some(filename) = path.file_name() {
				let filename_str = filename.to_string_lossy();

				if filename_str.ends_with(".wal.repair") {
					match fs::remove_file(&path) {
						Ok(()) => {
							removed_count += 1;
							log::warn!(
								"Removed stale repair file from previous crashed repair: {}",
								filename_str
							);
						}
						Err(e) => {
							log::error!(
								"Failed to remove stale repair file {}: {}",
								filename_str,
								e
							);
							// Don't fail the entire open operation for this
						}
					}
				}
			}
		}

		if removed_count > 0 {
			log::info!("Cleaned up {} stale .wal.repair files", removed_count);
		}

		Ok(())
	}

	/// Appends a record to the WAL.
	///
	/// # Arguments
	///
	/// * `rec` - A reference to the byte slice containing the record to be appended.
	///
	/// # Returns
	///
	/// A result indicating success or failure.
	///
	/// # Errors
	///
	/// This function may return an error if the WAL is closed, the provided
	/// record is empty, or any I/O error occurs during the write.
	pub(crate) fn append(&mut self, rec: &[u8]) -> Result<u64> {
		if self.closed {
			return Err(Error::IO(IOError::new(io::ErrorKind::Other, "WAL is closed")));
		}

		if rec.is_empty() {
			return Err(Error::IO(IOError::new(io::ErrorKind::Other, "buf is empty")));
		}

		log::trace!("WAL append: log_number={}, bytes={}", self.active_log_number, rec.len());

		self.active_writer.add_record(rec)?;

		// Return 0 for now (offset tracking can be added if needed)
		Ok(0)
	}

	pub(crate) fn sync(&mut self) -> Result<()> {
		if self.closed {
			return Ok(());
		}
		self.active_writer.sync()
	}

	/// Flushes buffered WAL data to OS cache (not to disk).
	/// For durability, call sync() instead.
	pub(crate) fn flush(&mut self) -> Result<()> {
		if self.closed {
			return Ok(());
		}
		self.active_writer.write_buffer()
	}

	pub(crate) fn close(&mut self) -> Result<()> {
		if self.closed {
			return Ok(());
		}

		let log_number = self.active_log_number;
		log::debug!("Closing WAL #{:020}", log_number);

		self.closed = true;

		// Sync and close the active writer (already fsyncs file data)
		self.active_writer.close()?;

		// Fsync the directory to persist metadata changes
		crate::lsm::fsync_directory(&self.dir)
			.map_err(|e| Error::IO(IOError::new(e.kind(), &e.to_string())))?;

		log::debug!("WAL #{:020} closed and synced successfully", log_number);

		Ok(())
	}

	pub(crate) fn get_dir_path(&self) -> &Path {
		&self.dir
	}

	/// Returns the active log number
	pub(crate) fn get_active_log_number(&self) -> u64 {
		self.active_log_number
	}

	/// Explicitly rotates the active WAL to a new file.
	pub(crate) fn rotate(&mut self) -> Result<u64> {
		let old_log_number = self.active_log_number;

		self.active_writer.sync()?;

		// Update the log number
		self.active_log_number += 1;

		log::debug!("WAL rotating: {:020} -> {:020}", old_log_number, self.active_log_number);

		// Create a new Writer for the new log number
		let new_writer = Self::create_writer(&self.dir, self.active_log_number, &self.opts)?;
		self.active_writer = new_writer;

		// Fsync the directory to ensure new file is visible after crash
		crate::lsm::fsync_directory(&self.dir)
			.map_err(|e| Error::IO(IOError::new(e.kind(), &e.to_string())))?;

		log::info!(
			"WAL rotated and fsynced: {:020} -> {:020}",
			old_log_number,
			self.active_log_number
		);

		Ok(self.active_log_number)
	}
}

impl Drop for Wal {
	/// Attempt to fsync data on drop, in case we're running without sync.
	fn drop(&mut self) {
		if !self.closed {
			self.close().ok();
		}
	}
}

#[cfg(test)]
mod tests {
	use tempdir::TempDir;
	use test_log::test;

	use super::*;

	fn create_temp_directory() -> TempDir {
		TempDir::new("test").unwrap()
	}

	#[test]
	fn test_wal_basic_operations() {
		let temp_dir = create_temp_directory();
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Test appending empty buffer fails
		let r = wal.append(&[]);
		assert!(r.is_err());

		// Test appending valid data
		let r = wal.append(&[0, 1, 2, 3]);
		assert!(r.is_ok());

		// Test sync
		let r = wal.sync();
		assert!(r.is_ok());

		// Test appending more data
		let r = wal.append(&[4, 5, 6, 7, 8, 9, 10]);
		assert!(r.is_ok());

		// Test closing
		assert!(wal.close().is_ok());
	}

	#[test]
	fn test_wal_rotation() {
		let temp_dir = create_temp_directory();
		let opts = Options::default();
		let mut wal = Wal::open(temp_dir.path(), opts).expect("should create WAL");

		// Append data
		wal.append(&[0, 1, 2, 3]).expect("should append");

		// Rotate to new WAL file
		let new_log_num = wal.rotate().expect("should rotate");
		assert_eq!(new_log_num, 1); // First rotation goes to log 1

		// Append to new WAL
		wal.append(&[4, 5, 6, 7]).expect("should append to new WAL");

		wal.close().expect("should close");

		// Verify both WAL files exist
		assert!(temp_dir.path().join("00000000000000000000.wal").exists());
		assert!(temp_dir.path().join("00000000000000000001.wal").exists());
	}

	#[test]
	fn test_cleanup_stale_repair_files() {
		use std::fs;

		let temp_dir = create_temp_directory();
		let wal_dir = temp_dir.path();

		// Create stale .wal.repair files (simulating crashed repair attempts)
		fs::write(wal_dir.join("00000000000000000000.wal.repair"), b"stale repair 1").unwrap();
		fs::write(wal_dir.join("00000000000000000001.wal.repair"), b"stale repair 2").unwrap();

		// Verify the stale repair files exist before opening WAL
		assert!(wal_dir.join("00000000000000000000.wal.repair").exists());
		assert!(wal_dir.join("00000000000000000001.wal.repair").exists());

		// Open WAL - this should trigger cleanup of stale .wal.repair files
		let wal = Wal::open(wal_dir, Options::default());
		assert!(wal.is_ok(), "WAL should open successfully after cleanup");

		// Verify stale .wal.repair files were removed
		assert!(
			!wal_dir.join("00000000000000000000.wal.repair").exists(),
			".wal.repair files should be cleaned up"
		);
		assert!(
			!wal_dir.join("00000000000000000001.wal.repair").exists(),
			".wal.repair files should be cleaned up"
		);

		// Verify a new WAL file was created (since directory was empty of valid
		// segments)
		assert!(
			wal_dir.join("00000000000000000000.wal").exists(),
			"New WAL file should be created"
		);
	}

	#[test]
	fn test_wal_append_to_existing() {
		let temp_dir = create_temp_directory();

		// First session - write some data
		{
			let opts = Options::default();
			let mut wal = Wal::open(temp_dir.path(), opts).unwrap();
			wal.append(&[1, 2, 3, 4]).unwrap();
			wal.close().unwrap();
		}

		// Verify only one WAL file exists
		assert!(temp_dir.path().join("00000000000000000000.wal").exists());
		let size_after_first =
			fs::metadata(temp_dir.path().join("00000000000000000000.wal")).unwrap().len();

		// Second session - should append to same file
		{
			let opts = Options::default();
			let mut wal = Wal::open(temp_dir.path(), opts).unwrap();
			wal.append(&[5, 6, 7, 8]).unwrap();
			wal.close().unwrap();
		}

		// Should still be only one WAL file, but larger
		let files: Vec<_> = fs::read_dir(temp_dir.path())
			.unwrap()
			.filter_map(|e| e.ok())
			.filter(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
			.collect();
		assert_eq!(files.len(), 1, "Should still have only one WAL file");

		let size_after_second =
			fs::metadata(temp_dir.path().join("00000000000000000000.wal")).unwrap().len();
		assert!(size_after_second > size_after_first, "File should be larger after appending");
	}

	#[test]
	fn test_wal_block_offset_across_sessions() {
		use std::fs::File;

		use crate::wal::reader::Reader;

		let temp_dir = create_temp_directory();

		// Create test data of varying lengths to stress block alignment
		let test_records: Vec<Vec<u8>> = vec![
			vec![1; 10],    // Small record
			vec![2; 100],   // Medium record
			vec![3; 1000],  // Larger record
			vec![4; 50],    // Another small one
			vec![5; 5000],  // Record that might cross block boundary
			vec![6; 7],     // Tiny record
			vec![7; 2500],  // Medium-large record
			vec![8; 33],    // Odd size
			vec![9; 16000], // Large record (half a block)
			vec![10; 100],  // Final medium record
		];

		// Write records across multiple sessions (close and reopen between each)
		// This tests that block_offset is correctly restored each time
		for (i, record) in test_records.iter().enumerate() {
			let opts = Options::default();
			let mut wal = Wal::open(temp_dir.path(), opts).unwrap();
			wal.append(record).unwrap();
			wal.close().unwrap();

			// Verify file size is growing
			let size =
				fs::metadata(temp_dir.path().join("00000000000000000000.wal")).unwrap().len();
			assert!(size > 0, "File should have content after write {}", i);
		}

		// Verify only one WAL file exists (all appends went to the same file)
		let files: Vec<_> = fs::read_dir(temp_dir.path())
			.unwrap()
			.filter_map(|e| e.ok())
			.filter(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
			.collect();
		assert_eq!(files.len(), 1, "Should have only one WAL file after all sessions");

		// Read back all records and verify they match
		let file =
			File::open(temp_dir.path().join("00000000000000000000.wal")).expect("should open file");
		let mut reader = Reader::new(file);

		for (i, expected) in test_records.iter().enumerate() {
			let result = reader.read();
			assert!(
				result.is_ok(),
				"Should be able to read record {} (block_offset was wrong if this fails)",
				i
			);
			let (data, _) = result.unwrap();
			assert_eq!(
				data,
				expected,
				"Record {} content should match (got {} bytes, expected {} bytes)",
				i,
				data.len(),
				expected.len()
			);
		}

		// Verify no more records
		let result = reader.read();
		assert!(
			result.is_err(),
			"Should have no more records after reading all {} expected",
			test_records.len()
		);
	}

	/// Test: Verify that reopening a WAL with different compression settings
	/// correctly uses the file's original compression type (not the new
	/// options).
	///
	/// This test verifies the fix for a bug where reopening a WAL with
	/// different compression options would cause data corruption - new records
	/// would be compressed differently without a SetCompressionType header,
	/// causing the reader to fail to decompress them correctly.
	#[test]
	fn test_wal_compression_type_detected_on_reopen() {
		use std::fs::File;

		use crate::wal::reader::Reader;

		let temp_dir = create_temp_directory();

		// First session - write WITHOUT compression
		{
			let opts = Options::default(); // No compression
			let mut wal = Wal::open(temp_dir.path(), opts).unwrap();
			wal.append(b"uncompressed_record_1").unwrap();
			wal.append(b"uncompressed_record_2").unwrap();
			wal.close().unwrap();
		}

		// Second session - reopen WITH compression (different from original)
		// The fix should detect that the file was created without compression
		// and continue writing without compression
		{
			let opts = Options::default().with_compression(CompressionType::Lz4);
			let mut wal = Wal::open(temp_dir.path(), opts).unwrap();
			// This should be written without compression to match the original file
			wal.append(b"should_also_be_uncompressed").unwrap();
			wal.close().unwrap();
		}

		// Read back all records - all should be readable without decompression issues
		let file =
			File::open(temp_dir.path().join("00000000000000000000.wal")).expect("should open file");
		let mut reader = Reader::new(file);

		// First two records should read fine (uncompressed)
		let (data1, _) = reader.read().expect("should read first record");
		assert_eq!(data1, b"uncompressed_record_1");

		let (data2, _) = reader.read().expect("should read second record");
		assert_eq!(data2, b"uncompressed_record_2");

		// Third record - with the fix, this should also be uncompressed and readable
		let (data3, _) = reader.read().expect("should read third record");
		assert_eq!(
			data3, b"should_also_be_uncompressed",
			"Third record should be correctly read (was written with detected compression type)"
		);
	}

	/// Test: Verify that compression is correctly detected and preserved
	/// when reopening a compressed WAL.
	#[test]
	fn test_wal_compressed_file_detected_on_reopen() {
		use std::fs::File;

		use crate::wal::reader::Reader;

		let temp_dir = create_temp_directory();

		// First session - write WITH compression
		{
			let opts = Options::default().with_compression(CompressionType::Lz4);
			let mut wal = Wal::open(temp_dir.path(), opts).unwrap();
			wal.append(b"compressed_record_1").unwrap();
			wal.append(b"compressed_record_2").unwrap();
			wal.close().unwrap();
		}

		// Second session - reopen WITHOUT compression in options
		// The fix should detect that the file uses LZ4 and continue with LZ4
		{
			let opts = Options::default(); // No compression in options
			let mut wal = Wal::open(temp_dir.path(), opts).unwrap();
			// This should be written with LZ4 compression to match the original file
			wal.append(b"should_also_be_compressed").unwrap();
			wal.close().unwrap();
		}

		// Read back all records - all should be decompressed correctly
		let file =
			File::open(temp_dir.path().join("00000000000000000000.wal")).expect("should open file");
		let mut reader = Reader::new(file);

		let (data1, _) = reader.read().expect("should read first record");
		assert_eq!(data1, b"compressed_record_1");

		let (data2, _) = reader.read().expect("should read second record");
		assert_eq!(data2, b"compressed_record_2");

		let (data3, _) = reader.read().expect("should read third record");
		assert_eq!(
			data3, b"should_also_be_compressed",
			"Third record should be correctly decompressed"
		);
	}

	/// Test: Verify that when a WAL is opened with compression enabled,
	/// the SetCompressionType record is written and can be read back by the
	/// Reader.
	///
	/// This test opens a WAL with compression, writes data, then verifies
	/// that the Reader correctly detects the compression type from the file.
	#[test]
	fn test_wal_compression_type_readable_by_reader() {
		use std::fs::File;

		use crate::wal::reader::Reader;

		let temp_dir = create_temp_directory();

		// Open WAL with LZ4 compression and write some data
		{
			let opts = Options::default().with_compression(CompressionType::Lz4);
			let mut wal = Wal::open(temp_dir.path(), opts).unwrap();
			wal.append(b"test_data").unwrap();
			wal.close().unwrap();
		}

		// Open the WAL file with Reader and verify compression type is detected
		let wal_path = temp_dir.path().join("00000000000000000000.wal");
		let file = File::open(&wal_path).expect("should open WAL file");
		let mut reader = Reader::new(file);

		// Before reading any records, compression type should be None
		assert_eq!(
			reader.get_compression_type(),
			CompressionType::None,
			"Compression type should be None before reading any records"
		);

		// Read the first data record - this should trigger reading the
		// SetCompressionType record first
		let (data, _) = reader.read().expect("should read first record");
		assert_eq!(data, b"test_data", "Data should match what was written");

		// After reading, the Reader should have detected the compression type
		assert_eq!(
			reader.get_compression_type(),
			CompressionType::Lz4,
			"Reader should detect LZ4 compression type from SetCompressionType record"
		);
	}

	/// Test: Verify that when a WAL is opened WITHOUT compression,
	/// the Reader correctly reports no compression type.
	#[test]
	fn test_wal_no_compression_type_when_disabled() {
		use std::fs::File;

		use crate::wal::reader::Reader;

		let temp_dir = create_temp_directory();

		// Open WAL without compression and write some data
		{
			let opts = Options::default(); // No compression
			let mut wal = Wal::open(temp_dir.path(), opts).unwrap();
			wal.append(b"test_data").unwrap();
			wal.close().unwrap();
		}

		// Open the WAL file with Reader
		let wal_path = temp_dir.path().join("00000000000000000000.wal");
		let file = File::open(&wal_path).expect("should open WAL file");
		let mut reader = Reader::new(file);

		// Read the first data record
		let (data, _) = reader.read().expect("should read first record");
		assert_eq!(data, b"test_data", "Data should match what was written");

		// After reading, the Reader should still report no compression
		assert_eq!(
			reader.get_compression_type(),
			CompressionType::None,
			"Reader should report no compression when WAL was created without compression"
		);
	}

	/// Tests that `open_with_min_log_number` uses `max(min_log_number,
	/// highest_on_disk)`.
	///
	/// This verifies the fix for a data loss bug:
	/// - Scenario: Crash with segments #1, #2, #3 on disk, manifest log_number=1
	/// - Without fix: WAL opens at segment 1, new writes go there
	/// - If flush updates log_number to 2, then crash → segment 1 skipped → DATA LOSS
	/// - With fix: WAL opens at segment 3 (highest), new writes safe
	#[test]
	fn test_open_with_min_log_number_uses_highest_segment() {
		let temp_dir = create_temp_directory();
		let wal_path = temp_dir.path();

		// Step 1: Create WAL segments 1, 2, 3 using the WAL API
		// Create segment 1 (00000000000000000001.wal)
		{
			let opts = Options::default();
			let mut wal = Wal::open_with_min_log_number(wal_path, 1, opts).unwrap();
			assert_eq!(wal.get_active_log_number(), 1);
			wal.append(b"data_in_segment_1").unwrap();
			wal.close().unwrap();
		}

		// Create segment 2
		{
			let opts = Options::default();
			let mut wal = Wal::open_with_min_log_number(wal_path, 2, opts).unwrap();
			// At this point, highest on disk is 1, min is 2, so max(2, 1) = 2
			assert_eq!(wal.get_active_log_number(), 2);
			wal.append(b"data_in_segment_2").unwrap();
			wal.close().unwrap();
		}

		// Create segment 3
		{
			let opts = Options::default();
			let mut wal = Wal::open_with_min_log_number(wal_path, 3, opts).unwrap();
			assert_eq!(wal.get_active_log_number(), 3);
			wal.append(b"data_in_segment_3").unwrap();
			wal.close().unwrap();
		}

		// Verify we have segments 1, 2, 3 on disk
		let segments: Vec<u64> = std::fs::read_dir(wal_path)
			.unwrap()
			.filter_map(|e| e.ok())
			.filter_map(|e| {
				e.path()
					.file_name()
					.and_then(|n| n.to_str())
					.and_then(|n| n.strip_suffix(".wal"))
					.and_then(|n| n.parse::<u64>().ok())
			})
			.collect();

		assert!(segments.contains(&1), "Segment 1 should exist");
		assert!(segments.contains(&2), "Segment 2 should exist");
		assert!(segments.contains(&3), "Segment 3 should exist");

		// Step 2: Test the fix - open with min_log_number=1 (as manifest would say
		// after crash) The fix should cause WAL to open at segment 3 (highest on
		// disk), not 1
		{
			let opts = Options::default();
			let mut wal = Wal::open_with_min_log_number(wal_path, 1, opts).unwrap();

			let active = wal.get_active_log_number();
			log::info!("After open_with_min_log_number(1): active_log_number = {}", active);

			// KEY ASSERTION: WAL should open at highest segment, not min_log_number
			assert_eq!(
				active, 3,
				"WAL should open at highest existing segment (3), not at min_log_number (1)"
			);

			wal.close().unwrap();
		}

		// Step 3: Edge case - open with min_log_number higher than any segment on disk
		// max(5, 3) = 5, so should open at 5
		{
			let opts = Options::default();
			let mut wal = Wal::open_with_min_log_number(wal_path, 5, opts).unwrap();

			assert_eq!(
				wal.get_active_log_number(),
				5,
				"WAL should open at min_log_number (5) when it's higher than any segment on disk (3)"
			);

			wal.close().unwrap();
		}
	}
}
