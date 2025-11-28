//  Copyright (c) 2024 SurrealDB Ltd.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! WAL Manager - High-level WAL file management

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use super::writer::Writer;
use super::{
	get_segment_range, segment_name, BufferedFileWriter, CompressionType, Error, IOError, Options,
	Result, BLOCK_SIZE,
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

	/// Creates a new Writer for the given log number.
	fn create_writer(dir: &Path, log_number: u64, opts: &Options) -> Result<Writer> {
		let extension = opts.file_extension.as_deref().unwrap_or("wal");
		let file_name = segment_name(log_number, extension);
		let file_path = dir.join(&file_name);

		// Open or create the file
		let file = Self::open_wal_file(&file_path, opts)?;

		// Create buffered file writer
		let buffered_writer = BufferedFileWriter::new(file, BLOCK_SIZE);

		// Create Writer (use compression type from options)
		let compression_type = opts.compression_type.unwrap_or(CompressionType::None);

		let manual_flush = false;

		let mut writer = Writer::new(buffered_writer, manual_flush, compression_type);

		// If compression is enabled, write compression type record first
		if compression_type != CompressionType::None {
			writer.add_compression_type_record()?;
		}

		Ok(writer)
	}

	fn open_wal_file(file_path: &Path, opts: &Options) -> Result<File> {
		let mut open_options = OpenOptions::new();
		open_options.read(true).write(true).create(true);

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
		Ok(if last > 0 {
			last + 1
		} else {
			0
		})
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
	/// This function may return an error if the WAL is closed, the provided record
	/// is empty, or any I/O error occurs during the write.
	pub(crate) fn append(&mut self, rec: &[u8]) -> Result<u64> {
		if self.closed {
			return Err(Error::IO(IOError::new(io::ErrorKind::Other, "WAL is closed")));
		}

		if rec.is_empty() {
			return Err(Error::IO(IOError::new(io::ErrorKind::Other, "buf is empty")));
		}

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

	pub(crate) fn close(&mut self) -> Result<()> {
		if self.closed {
			return Ok(());
		}
		self.closed = true;

		self.active_writer.close()?;
		Ok(())
	}

	pub(crate) fn get_dir_path(&self) -> &Path {
		&self.dir
	}

	/// Explicitly rotates the active WAL to a new file.
	pub(crate) fn rotate(&mut self) -> Result<u64> {
		self.active_writer.sync()?;

		// Update the log number
		self.active_log_number += 1;

		// Create a new Writer for the new log number
		let new_writer = Self::create_writer(&self.dir, self.active_log_number, &self.opts)?;
		self.active_writer = new_writer;

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
	use super::*;
	use tempdir::TempDir;
	use test_log::test;

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

		// Verify a new WAL file was created (since directory was empty of valid segments)
		assert!(
			wal_dir.join("00000000000000000000.wal").exists(),
			"New WAL file should be created"
		);
	}
}
