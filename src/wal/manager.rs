use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::recovery::sync_mark_in;
use super::writer::Writer;
use super::{
	directory_present,
	get_segment_range_in,
	list_segment_ids_in,
	sealed,
	sealed_needs_std,
	segment_name,
	BufferedFileWriter,
	CompressionType,
	Error,
	IOError,
	Options,
	RecordType,
	Result,
	SyncGate,
	BLOCK_SIZE,
	HEADER_SIZE,
	SYNC_MARK_LEN,
};
use crate::error::EncryptionError;
use crate::tde::EncryptionContext;
use crate::vfs::{CreateOpts, Fs, FsFile, FsKind};

/// The size of the reads that the check of a tail for zeros makes.
const COPY_CHUNK: usize = 1024 * 1024;

/// Write-Ahead Log (Wal) manager for coordinating WAL operations.
pub struct Wal {
	/// The currently active Writer for writing records. `None` only in a sealed WAL that was
	/// opened and has not made its first segment yet, see `open_with_min_log_number`.
	active_writer: Option<Writer>,

	/// The active WAL file, shared with its writer, used to perform fsync outside the write lock
	/// so concurrent appends are not blocked. The fsync goes through the segment's `SyncGate`, see
	/// `sync_handle`.
	sync_fd: Option<Arc<dyn FsFile>>,

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
	#[allow(dead_code)]
	pub(crate) fn open(dir: &Path, opts: Options) -> Result<Self> {
		// Ensure the options are valid
		opts.validate()?;

		// A sealed log reads and repairs its segments through `std::fs`: refused before anything
		// is touched.
		Self::refuse_sealed_off_std(&opts)?;

		// Ensure the directory exists with proper permissions
		Self::prepare_directory(dir, &opts)?;

		// Clean up any stale .wal.repair files from previous crashed repair attempts
		Self::cleanup_stale_repair_files(&*opts.context.fs, dir)?;

		if let Some(context) = opts.context.encryption() {
			return Self::open_sealed(dir, 0, opts.clone(), context);
		}

		// Determine the active log number
		let active_log_number = Self::calculate_active_log_number(&*opts.context.fs, dir)?;

		// Create the active Writer and sync fd
		let (active_writer, sync_fd) = Self::create_writer(dir, active_log_number, &opts)?;

		Ok(Self {
			active_writer: Some(active_writer),
			sync_fd: Some(sync_fd),
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

		// A sealed log reads and repairs its segments through `std::fs`: refused before anything
		// is touched.
		Self::refuse_sealed_off_std(&opts)?;

		// Ensure the directory exists with proper permissions
		Self::prepare_directory(dir, &opts)?;

		// Clean up any stale .wal.repair files from previous crashed repair attempts
		Self::cleanup_stale_repair_files(&*opts.context.fs, dir)?;

		if let Some(context) = opts.context.encryption() {
			return Self::open_sealed(dir, min_log_number, opts.clone(), context);
		}

		// Use max of min_log_number and highest existing segment on disk.
		// This prevents writing to an old segment that could be skipped on recovery.
		//
		// Example scenario this fixes:
		// 1. Crash with segments #5, #6, #7 on disk
		// 2. Recovery replays all three into memtable
		// 3. Without this fix, new writes would go to segment #5
		// 4. If memtable flushes (log_number = 6), then crash again
		// 5. Segment #5 would be skipped, losing the new writes
		//
		// A listing that fails is the error of the open. It used to read as "no segment": the log
		// then started at `min_log_number`, which is the scenario above, with a segment that the
		// listing could not see.
		let highest_on_disk = Self::calculate_active_log_number(&*opts.context.fs, dir)?;
		let active_log_number = std::cmp::max(min_log_number, highest_on_disk);

		if active_log_number > min_log_number {
			tracing::debug!(
				"WAL: advancing from min_log_number {} to {} (highest existing segment)",
				min_log_number,
				active_log_number
			);
		}

		// Create the active Writer and sync fd at the calculated log number
		let (active_writer, sync_fd) = Self::create_writer(dir, active_log_number, &opts)?;

		Ok(Self {
			active_writer: Some(active_writer),
			sync_fd: Some(sync_fd),
			active_log_number,
			dir: dir.to_path_buf(),
			opts,
			closed: false,
		})
	}

	/// Refuses the options of a sealed log on a file system that is not `std`.
	fn refuse_sealed_off_std(opts: &Options) -> Result<()> {
		if opts.context.is_sealed() && opts.context.fs.kind() != FsKind::Std {
			return Err(sealed_needs_std());
		}
		Ok(())
	}

	/// Opens a sealed WAL: no segment is created and none is opened for appending.
	///
	/// A segment of a sealed WAL is never appended to after the process that wrote it is gone: a
	/// restart writes to a segment of its own, under a salt of its own, and this is the one thing
	/// that keeps a position of a segment's key from being sealed twice across runs. It is also
	/// made only once the segments of the WAL were replayed, because creating a file is not
	/// something that a key that does not fit may have caused: nothing in the directory changes
	/// until then. The number it will have is reserved here, past every segment on disk and not
	/// before `min_log_number`, and the file is created by the first rotation or the first
	/// append.
	fn open_sealed(
		dir: &Path,
		min_log_number: u64,
		opts: Options,
		context: &EncryptionContext,
	) -> Result<Self> {
		let active_log_number =
			match list_segment_ids_in(&*opts.context.fs, dir, Some("wal"))?.last() {
				Some(&highest) => std::cmp::max(min_log_number, highest.saturating_add(1)),
				None => min_log_number,
			};

		// The key of the segments that will be made. Asked for here, where waiting on a key
		// provider is allowed, and not when a segment is made.
		context.prepare(&[context.key_manager().active_key_id()])?;

		Ok(Self {
			active_writer: None,
			sync_fd: None,
			active_log_number,
			dir: dir.to_path_buf(),
			opts,
			closed: false,
		})
	}

	/// Creates a new Writer and sync fd for the given log number.
	/// If the segment file already exists, appends to it instead of
	/// overwriting.
	///
	/// Returns the Writer and the file that it writes, shared: the sync fd can be used to perform
	/// fsync outside the WAL write lock, allowing concurrent appends to proceed during the slow
	/// fsync. On `std` it is one descriptor that the writer and the fsync both use (a `write(2)`
	/// and an `fsync(2)` of one descriptor are what a `dup` of it and the original did, on the one
	/// open file description), and no `dup` is made. A pool has one handle of a file, and this is
	/// the only way to have two users of it.
	fn create_writer(
		dir: &Path,
		log_number: u64,
		opts: &Options,
	) -> Result<(Writer, Arc<dyn FsFile>)> {
		if let Some(context) = opts.context.encryption() {
			return Self::create_sealed_writer(dir, log_number, opts, context);
		}

		let extension = opts.file_extension.as_deref().unwrap_or("wal");
		let file_name = segment_name(log_number, extension);
		let file_path = dir.join(&file_name);

		// Open or create the file (append mode ensures writes go to end)
		let file = Self::open_wal_file(&file_path, opts)?;

		// Get file size from the opened file handle
		let existing_size = file.len()?;

		if existing_size > 0 {
			// Existing file: detect the compression type from the file itself, through the handle
			// that is open, so that no second one is needed. This prevents compression mismatch
			// bugs when reopening with different options.
			let detected_compression = Self::detect_compression_of(&*file)?;

			// Calculate block_offset from file size
			let block_offset = (existing_size % BLOCK_SIZE as u64) as usize;

			// Create buffered file writer
			let buffered_writer = BufferedFileWriter::new(Arc::clone(&file), BLOCK_SIZE);

			let mut writer =
				Writer::new(buffered_writer, false, detected_compression, block_offset);
			// Marks only from the next fsync on, which covers everything in the file: the writer
			// knows nothing of what the segment had synced before.
			if opts.sync_marks {
				writer.write_sync_marks();
			}
			Ok((writer, file))
		} else {
			// New file - use compression type from options
			let compression_type = opts.compression_type;

			// Create buffered file writer
			let buffered_writer = BufferedFileWriter::new(Arc::clone(&file), BLOCK_SIZE);

			let mut writer = Writer::new(buffered_writer, false, compression_type, 0);
			if compression_type != CompressionType::None {
				writer.add_compression_type_record()?;
			}
			if opts.sync_marks {
				writer.write_sync_marks();
			}
			Ok((writer, file))
		}
	}

	/// Creates the segment `log_number` of a sealed WAL, with its header written and fsynced and
	/// the directory entry durable, and returns its writer.
	///
	/// The file is created exclusively: one that is already there is an error and is left alone,
	/// never appended to. The header, with a salt that is drawn now, is made before the file is, so
	/// a key that cannot be had leaves nothing behind, and every segment that is created, a retry
	/// of the same number included, has a key of its own. If anything fails once the file exists it
	/// is removed again: it is this call's, it holds nothing but the header this call wrote, and no
	/// position was sealed under its key, so a later attempt may take the same number.
	fn create_sealed_writer(
		dir: &Path,
		log_number: u64,
		opts: &Options,
		context: &EncryptionContext,
	) -> Result<(Writer, Arc<dyn FsFile>)> {
		let fs = &*opts.context.fs;
		let extension = opts.file_extension.as_deref().unwrap_or("wal");
		let file_path = dir.join(segment_name(log_number, extension));

		let (header, cipher) = sealed::new_header(context, log_number, opts.compression_type)?;
		let file = Self::create_new_wal_file(&file_path, opts)?;

		let started =
			Self::start_sealed_segment(dir, file, &header, cipher, opts.compression_type, opts);
		if started.is_err() {
			if let Err(e) = fs.remove_file(&file_path) {
				tracing::error!(
					"A WAL segment that could not be started was left behind as {file_path:?}: {e}"
				);
			}
		}
		started
	}

	/// The part of `create_sealed_writer` after the file was created. The file is closed again
	/// when this returns an error, which the caller then removes.
	fn start_sealed_segment(
		dir: &Path,
		file: Arc<dyn FsFile>,
		header: &[u8; sealed::HEADER_PAYLOAD_LEN],
		cipher: crate::tde::FileCipher,
		compression_type: CompressionType,
		opts: &Options,
	) -> Result<(Writer, Arc<dyn FsFile>)> {
		#[cfg(test)]
		let fault = opts.creation_fault.lock().take();

		#[cfg_attr(not(test), allow(unused_mut))]
		let mut dest = BufferedFileWriter::new(Arc::clone(&file), BLOCK_SIZE);
		#[cfg(test)]
		match fault {
			Some(super::CreationFault::HeaderWrite) => dest.fail_after_ops(0),
			Some(super::CreationFault::HeaderSync) => dest.gate().fail_after(0),
			_ => {}
		}

		let writer = Writer::start_sealed(dest, header, cipher, compression_type)?;
		#[cfg(test)]
		if fault == Some(super::CreationFault::Directory) {
			return Err(Error::IO(IOError::new(
				io::ErrorKind::Other,
				"injected directory failure",
			)));
		}
		opts.context
			.fs
			.sync_dir(dir)
			.map_err(|e| Error::IO(IOError::new(e.kind(), &e.to_string())))?;
		Ok((writer, file))
	}

	/// Creates the file of a segment, which must not exist yet.
	fn create_new_wal_file(file_path: &Path, opts: &Options) -> Result<Arc<dyn FsFile>> {
		Ok(opts.context.fs.create_new(file_path, CreateOpts::new(opts.file_mode))?)
	}

	/// Detects the compression type used in an existing WAL file.
	///
	/// Reads the first record's header. If it's a SetCompressionType record,
	/// parses and returns that compression type. Otherwise returns None (no
	/// compression).
	#[cfg_attr(
		not(test),
		allow(dead_code, reason = "kept for callers that have not moved to the _in form")
	)]
	pub(crate) fn detect_compression_type(file_path: &Path) -> Result<CompressionType> {
		Self::detect_compression_type_in(&*crate::vfs::default_fs(), file_path)
	}

	/// [`detect_compression_type`](Self::detect_compression_type) in the file system `fs`.
	#[cfg_attr(
		not(test),
		allow(dead_code, reason = "kept for callers that have not moved to the _in form")
	)]
	pub(crate) fn detect_compression_type_in(
		fs: &dyn Fs,
		file_path: &Path,
	) -> Result<CompressionType> {
		let file = fs.open_read(file_path)?;
		Self::detect_compression_of(&*file)
	}

	/// The compression type of the segment that `file` is, from its first record.
	///
	/// Positional reads of the file that is already open, so that a pool, which has one handle of
	/// a file, is not asked for a second. A file that is too short to hold a header has none, a
	/// sealed segment is refused, and a compression record whose byte is missing is an error: what
	/// the open of a segment has always done.
	pub(crate) fn detect_compression_of(file: &dyn crate::vfs::File) -> Result<CompressionType> {
		fn read_at(file: &dyn crate::vfs::File, offset: u64, buf: &mut [u8]) -> Result<usize> {
			crate::vfs::File::read_up_to(file, offset, buf)
				.map_err(|e| Error::from(crate::vfs::into_io_error(e)))
		}

		// Read just enough for the header
		let mut header = [0u8; HEADER_SIZE];
		if read_at(file, 0, &mut header)? < HEADER_SIZE {
			// File too short, assume no compression
			return Ok(CompressionType::None);
		}

		// A sealed segment is not appended to by a WAL that is not sealed, and nothing is done
		// to it: the open fails, and the segment stays as it is. It is known by the type of its
		// first record or by the start of the encryption header that is the payload of that
		// record, which stays when the type byte rots.
		let mut payload_start = [0u8; 4];
		let sealed_by_magic =
			matches!(read_at(file, HEADER_SIZE as u64, &mut payload_start), Ok(4))
				&& payload_start == crate::tde::HEADER_MAGIC;
		if header[6] == RecordType::SegmentHeader as u8 || sealed_by_magic {
			return Err(EncryptionError::NotConfigured.into());
		}

		// Parse the record type from header byte 6
		let record_type_byte = header[6];
		let record_type = RecordType::from_u8(record_type_byte)?;

		if record_type == RecordType::SetCompressionType {
			// Read the compression type byte (length is in bytes 4-5)
			let length = u16::from_be_bytes([header[4], header[5]]);
			if length >= 1 {
				let mut compression_byte = [0u8; 1];
				if read_at(file, HEADER_SIZE as u64, &mut compression_byte)? < 1 {
					return Err(Error::IO(IOError::new(
						io::ErrorKind::UnexpectedEof,
						"failed to fill whole buffer",
					)));
				}
				return CompressionType::from_u8(compression_byte[0]);
			}
		}

		// First record is not SetCompressionType, so file has no compression
		Ok(CompressionType::None)
	}

	fn open_wal_file(file_path: &Path, opts: &Options) -> Result<Arc<dyn FsFile>> {
		Ok(opts.context.fs.open_append(file_path, CreateOpts::new(opts.file_mode))?)
	}

	#[allow(
		clippy::disallowed_methods,
		reason = "permissions are set only when the file system kind is std; a pool or an in-memory file system has none to set"
	)]
	fn prepare_directory(dir: &Path, opts: &Options) -> Result<()> {
		// Directory should already be created by Tree::new()
		// Set permissions on Unix only; Windows NTFS uses ACLs and
		// set_permissions on directories can fail with ERROR_ACCESS_DENIED. Permissions are not
		// part of the file system seam: the directory is on `std::fs` or it is not on a disk at
		// all, and a file system that is not `std` has nothing to set.
		#[cfg(unix)]
		if opts.context.fs.kind() == FsKind::Std {
			if let Ok(metadata) = std::fs::metadata(dir) {
				let mut permissions = metadata.permissions();
				use std::os::unix::fs::PermissionsExt;
				permissions.set_mode(opts.dir_mode.unwrap_or(0o750));
				std::fs::set_permissions(dir, permissions)?;
			}
		}
		#[cfg(not(unix))]
		let _ = (dir, opts);

		Ok(())
	}

	fn calculate_active_log_number(fs: &dyn Fs, dir: &Path) -> Result<u64> {
		let (_, last) = get_segment_range_in(fs, dir, Some("wal"))?;
		// Return the last segment to append to, or 0 if no segments exist
		Ok(last)
	}

	/// Cleans up stale .wal.repair files from crashed repair processes.
	fn cleanup_stale_repair_files(fs: &dyn Fs, dir: &Path) -> Result<()> {
		// Check if the directory exists
		if !directory_present(fs, dir)? {
			return Ok(());
		}

		// Read all files in the WAL directory
		let entries = match fs.read_dir(dir) {
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
		for entry in entries {
			let filename_str = entry.name.to_string_lossy();

			if filename_str.ends_with(".wal.repair") {
				match fs.remove_file(&dir.join(&entry.name)) {
					Ok(()) => {
						removed_count += 1;
						tracing::warn!(
							"Removed stale repair file from previous crashed repair: {}",
							filename_str
						);
					}
					Err(e) => {
						tracing::error!(
							"Failed to remove stale repair file {}: {}",
							filename_str,
							e
						);
						// Don't fail the entire open operation for this
					}
				}
			}
		}

		if removed_count > 0 {
			tracing::debug!("Cleaned up {} stale .wal.repair files", removed_count);
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

		tracing::trace!("WAL append: log_number={}, bytes={}", self.active_log_number, rec.len());

		self.active_writer_for_append()?.add_record(rec)?;

		// Return 0 for now (offset tracking can be added if needed)
		Ok(0)
	}

	/// Appends several records, `buf[ends[i - 1]..ends[i]]` being record `i` (`ends[-1]` is 0),
	/// to the active segment, and returns the segment they were all written to.
	///
	/// Each record is framed exactly as `append` frames it, so the segment holds the same
	/// bytes as one `append` per record, but the group reaches the file in one write (one
	/// per 32 KiB of framed data at most). All or nothing: a failure cuts the segment back to
	/// where it was before the first record and poisons the writer, see `Writer::add_records`.
	/// Nothing is written, and the writer is not poisoned, if a record is empty or `ends` is
	/// not an increasing list of offsets into `buf`.
	///
	/// The WAL only rotates through `&mut self`, so the whole group lands in one segment, the
	/// one returned: the caller holds the lock this is called under and rotation needs.
	pub(crate) fn append_group(&mut self, buf: &[u8], ends: &[usize]) -> Result<u64> {
		if self.closed {
			return Err(Error::IO(IOError::new(io::ErrorKind::Other, "WAL is closed")));
		}

		let mut start = 0;
		for &end in ends {
			if end <= start || end > buf.len() {
				return Err(Error::IO(IOError::new(
					io::ErrorKind::Other,
					"empty or out of range record in a group append",
				)));
			}
			start = end;
		}

		tracing::trace!(
			"WAL group append: log_number={}, records={}, bytes={}",
			self.active_log_number,
			ends.len(),
			start
		);

		if !ends.is_empty() {
			self.active_writer_for_append()?.add_records(buf, ends)?;
		}
		Ok(self.active_log_number)
	}

	/// The writer of the active segment, which a sealed WAL that has none yet makes first: the
	/// first append creates the segment whose number was reserved when the WAL was opened.
	fn active_writer_for_append(&mut self) -> Result<&mut Writer> {
		if self.active_writer.is_none() {
			let (writer, sync_fd) =
				Self::create_writer(&self.dir, self.active_log_number, &self.opts)?;
			self.active_writer = Some(writer);
			self.sync_fd = Some(sync_fd);
		}
		self.active_writer.as_mut().ok_or(Error::SegmentNotFound)
	}

	pub(crate) fn sync(&mut self) -> Result<()> {
		if self.closed {
			return Ok(());
		}
		// A WAL that has no segment yet has nothing to sync.
		self.active_writer.as_mut().map_or(Ok(()), Writer::sync)
	}

	/// Flushes buffered WAL data to OS cache (not to disk).
	/// For durability, call sync() instead.
	pub(crate) fn flush(&mut self) -> Result<()> {
		if self.closed {
			return Ok(());
		}
		self.active_writer.as_mut().map_or(Ok(()), Writer::write_buffer)
	}

	/// Returns what the caller needs to fsync the active segment outside the WAL
	/// write lock, allowing concurrent appends to proceed during the slow fsync.
	/// This is safe because fsync operates on the inode — all dirty pages
	/// from any fd pointing to the same file are persisted.
	///
	/// The handle belongs to the segment it was taken for: if the WAL rotates before the
	/// fsync, it still syncs that segment, and a failure poisons only that segment's writer.
	pub(crate) fn sync_handle(&self) -> SyncHandle {
		SyncHandle {
			target: self
				.active_writer
				.as_ref()
				.zip(self.sync_fd.as_ref())
				.map(|(writer, file)| (Arc::clone(file), writer.sync_gate())),
		}
	}

	/// Makes the write to the active segment after the next `ops` of them fail
	/// once, as a full disk would.
	#[cfg(test)]
	pub(crate) fn fail_writes_after(&mut self, ops: usize) {
		if let Some(writer) = self.active_writer.as_mut() {
			writer.fail_after_ops(ops);
		}
	}

	/// How many `write(2)` calls have reached the active segment.
	#[cfg(test)]
	pub(crate) fn file_writes(&self) -> usize {
		self.active_writer.as_ref().map_or(0, Writer::file_writes)
	}

	/// Test-only: whether anything was appended to the active segment since its last fsync.
	#[cfg(test)]
	pub(crate) fn pending_sync(&self) -> bool {
		self.active_writer.as_ref().is_some_and(Writer::pending_sync)
	}

	/// Makes the fsync of the active segment after the next `syncs` of them fail once, as a
	/// failing disk would.
	#[cfg(test)]
	pub(crate) fn fail_syncs_after(&mut self, syncs: usize) {
		if let Some(writer) = self.active_writer.as_ref() {
			writer.sync_gate().fail_after(syncs);
		}
	}

	/// Installs (or clears) an observer called inside every fsync of the active segment, with
	/// the segment's fsyncs held up for as long as it runs.
	#[cfg(test)]
	pub(crate) fn set_sync_observer(&mut self, observer: Option<Arc<dyn Fn() + Send + Sync>>) {
		if let Some(writer) = self.active_writer.as_ref() {
			writer.sync_gate().set_observer(observer);
		}
	}

	/// Makes the segments that a rotation creates from now on use `compression_type`.
	#[cfg(test)]
	pub(crate) fn set_compression_of_new_segments(&mut self, compression_type: CompressionType) {
		self.opts.compression_type = compression_type;
	}

	/// Test-only: the index that the next batch of the active sealed segment is sealed at.
	#[cfg(test)]
	pub(crate) fn next_index(&self) -> Option<u64> {
		self.active_writer.as_ref().and_then(Writer::next_index)
	}

	/// Test-only: the smallest position that the cipher of the active sealed segment would still
	/// seal.
	#[cfg(test)]
	pub(crate) fn cipher_next_position(&self) -> Option<u64> {
		self.active_writer.as_ref().and_then(Writer::cipher_next_position)
	}

	/// Test-only: sets the index that the next batch of the active sealed segment is sealed at,
	/// as a bug that rewinds the count would.
	#[cfg(test)]
	pub(crate) fn set_next_index(&mut self, index: u64) {
		if let Some(writer) = self.active_writer.as_mut() {
			writer.set_next_index(index);
		}
	}

	/// Test-only: makes the next cut back of the active segment fail once.
	#[cfg(test)]
	pub(crate) fn fail_truncate(&mut self) {
		if let Some(writer) = self.active_writer.as_mut() {
			writer.fail_truncate();
		}
	}

	/// Test-only: makes the creation of the next sealed segment fail at `fault`, once.
	#[cfg(test)]
	pub(crate) fn fail_next_creation(&mut self, fault: super::CreationFault) {
		*self.opts.creation_fault.lock() = Some(fault);
	}

	/// Test-only: whether the first segment of the WAL exists yet, which a sealed WAL makes on
	/// its first rotation or append.
	#[cfg(test)]
	pub(crate) fn has_segment(&self) -> bool {
		self.active_writer.is_some()
	}

	/// Test-only: whether an fsync of the active segment failed, which poisons its writer.
	#[cfg(test)]
	pub(crate) fn sync_failed(&self) -> bool {
		self.active_writer.as_ref().is_some_and(Writer::sync_failed)
	}

	/// Whether an append or an fsync of the active segment failed, so that its writer refuses
	/// every append until a rotation replaces it.
	pub(crate) fn needs_replacement(&self) -> bool {
		self.active_writer.as_ref().is_some_and(Writer::is_poisoned)
	}

	/// Whether the active writer is held, see `rotate_with`.
	pub(crate) fn is_held(&self) -> bool {
		self.hold_below().is_some()
	}

	/// The segment number below which everything must be in tables for the hold of the active
	/// writer to end, if it is held.
	pub(crate) fn hold_below(&self) -> Option<u64> {
		self.active_writer.as_ref().and_then(Writer::held)
	}

	/// Ends the hold of the active writer if it is the one for `below`, the bound the caller read
	/// with `hold_below` while it checked that the segments below it are in tables. A hold that
	/// was set again since is left alone.
	pub(crate) fn release_hold(&mut self, below: u64) {
		if let Some(writer) = self.active_writer.as_mut() {
			if writer.held() == Some(below) {
				writer.release_hold();
			}
		}
	}

	pub(crate) fn close(&mut self) -> Result<()> {
		if self.closed {
			return Ok(());
		}

		let log_number = self.active_log_number;
		tracing::debug!("Closing WAL #{:020}", log_number);

		self.closed = true;

		// Sync and close the active writer (already fsyncs file data)
		let Some(writer) = self.active_writer.as_mut() else {
			// A sealed WAL that never made a segment has written nothing.
			return Ok(());
		};
		writer.close()?;

		// Fsync the directory to persist metadata changes
		self.opts
			.context
			.fs
			.sync_dir(&self.dir)
			.map_err(|e| Error::IO(IOError::new(e.kind(), &e.to_string())))?;

		tracing::debug!("WAL #{:020} closed and synced successfully", log_number);

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
	///
	/// The active log number and writer only change once the new segment exists
	/// and its directory entry is durable, so a failed rotation leaves the WAL
	/// exactly as it was. The memtable tags and the segment a record is logged in
	/// are both read from the active log number, and they must never run ahead of
	/// the writer.
	///
	/// The old segment is synced first. If that fails, the rotation fails and the
	/// segment's writer is poisoned. A segment whose fsync already failed is not synced
	/// again, because the retry could report success for pages the kernel dropped, and the
	/// rotation goes ahead: it is how a poisoned writer is replaced. What the segment holds
	/// past its last good fsync was never acknowledged as durable, and the failure was
	/// reported to whoever ran into it. Its tail may be damaged after a crash. In the default
	/// recovery mode that is repaired by cutting the segment back to its valid prefix before
	/// the segments after it are replayed.
	///
	/// Records of unknown durability in that segment must not be left behind records that are
	/// acknowledged after them, so the new writer is held: it refuses every append and sync
	/// until the caller releases it, once the memtables tagged with the failed segment are in
	/// tables. Whether the sync was skipped, and so whether the writer is held, is read once, under
	/// the lock the caller holds, together with the decision to skip the sync, so every rotation
	/// gets it, whoever makes it. A writer that is held already is empty and is not synced; its
	/// successor inherits the hold, with its bound.
	///
	/// `before_sync` runs just before that fsync, under the caller's WAL lock, so no record
	/// can be appended after it and before the fsync. It is where the caller makes durable what
	/// the records point to: none may reach the disk ahead of it. It does not run for a segment
	/// whose fsync already failed, which is not fsynced again, nor for one that is held, and an
	/// error from it fails the rotation with the WAL as it was.
	pub(crate) fn rotate_with(&mut self, before_sync: impl FnOnce() -> Result<()>) -> Result<u64> {
		let old_log_number = self.active_log_number;

		let Some(active) = self.active_writer.as_mut() else {
			// A sealed WAL that has no segment yet: there is nothing to sync and no record for
			// `before_sync` to make safe, and the number that is reserved is the one that is
			// made, so that the numbers of the segments have no gap.
			let (writer, sync_fd) = Self::create_writer(&self.dir, old_log_number, &self.opts)?;
			self.active_writer = Some(writer);
			self.sync_fd = Some(sync_fd);
			return Ok(old_log_number);
		};
		let new_log_number = old_log_number + 1;

		let held = active.held();
		let sync_failed = active.sync_failed();
		let uncut = active.left_uncut();
		if !sync_failed && held.is_none() {
			before_sync()?;
			active.sync()?;
		}

		tracing::debug!("WAL rotating: {:020} -> {:020}", old_log_number, new_log_number);

		// Create a new Writer and sync fd for the new log number
		let (mut new_writer, new_sync_fd) =
			Self::create_writer(&self.dir, new_log_number, &self.opts)?;

		// Fsync the directory to ensure new file is visible after crash. A sealed segment did it
		// before it was handed over: it must not fail after the file is there, or the number
		// would be taken by a file that nothing refers to.
		if !self.opts.context.is_sealed() {
			self.opts
				.context
				.fs
				.sync_dir(&self.dir)
				.map_err(|e| Error::IO(IOError::new(e.kind(), &e.to_string())))?;
		}

		// A sealed segment that could not be cut back after a failed append may end in damage. A
		// recovery that finds damage in a segment that later ones follow does not repair it, so
		// what replaces it takes no commit until its memtables are in tables, as after a failed
		// fsync, and the segment is below the log number by the time the first one is logged.
		if sync_failed || held.is_some() || uncut {
			new_writer.hold(held.unwrap_or(new_log_number));
		}
		self.active_writer = Some(new_writer);
		self.sync_fd = Some(new_sync_fd);
		self.active_log_number = new_log_number;

		tracing::debug!(
			"WAL rotated and fsynced: {:020} -> {:020}",
			old_log_number,
			new_log_number
		);

		Ok(new_log_number)
	}

	/// Test-only: [`rotate_with`](Self::rotate_with) with nothing to run before the fsync.
	#[cfg(test)]
	pub(crate) fn rotate(&mut self) -> Result<u64> {
		self.rotate_with(|| Ok(()))
	}

	/// Moves the active segment, which the open has replayed, to the number that follows it, and
	/// makes it the active one: the rewrite at open. It is for a log whose newest segment holds the
	/// recovered memtable, so that the open starts no segment beside the one it replayed.
	///
	/// The writer appends after the end of the file, so the file is first cut to `valid_end`, the
	/// offset where the replay stopped, when it is longer. The bytes past that offset must be whole
	/// sync marks and the zeros that pad a block, see [`tail_is_marks_and_padding`]: the replay
	/// reads them as the end of the log. A tail with any other byte is not moved, and `None` is
	/// returned with nothing changed, so that the caller starts a segment of its own. The cut is
	/// synced before the rename.
	///
	/// The move is one rename, which the file system makes at one commit point, followed by a sync
	/// of the directory that names it. The file is not copied, so no state holds the records of a
	/// segment under two names: a crash leaves the segment under its old name or under the new
	/// one, and the next open replays it either way.
	///
	/// `before_sync` runs before the rename, as in [`rotate_with`](Self::rotate_with): it makes
	/// durable what the records point to, so that none of them reaches the disk ahead of it.
	pub(crate) fn rewrite_active_with(
		&mut self,
		valid_end: u64,
		before_sync: impl FnOnce() -> Result<()>,
	) -> Result<Option<u64>> {
		let fs = &*self.opts.context.fs;
		let extension = self.opts.file_extension.as_deref().unwrap_or("wal");
		let old_log_number = self.active_log_number;
		let new_log_number = old_log_number + 1;
		let old_path = self.dir.join(segment_name(old_log_number, extension));
		let new_path = self.dir.join(segment_name(new_log_number, extension));

		if fs.exists(&new_path) {
			return Err(Error::IO(IOError::new(
				io::ErrorKind::AlreadyExists,
				&format!(
					"WAL segment {new_log_number:020}.{extension} exists, so {old_log_number:020}.{extension} cannot be moved to it"
				),
			)));
		}

		let old = fs.open_read(&old_path)?;
		let len = old.len()?;
		if len > valid_end && !tail_is_marks_and_padding(&*old, valid_end, len)? {
			return Ok(None);
		}
		drop(old);

		// The writer of the old name is not used again: nothing is appended to it.
		self.active_writer = None;
		self.sync_fd = None;

		if len > valid_end {
			let file = fs.open_append_existing(&old_path)?;
			file.set_len(valid_end)?;
			file.sync()?;
		}

		before_sync()?;
		fs.rename(&old_path, &new_path)?;
		fs.sync_dir(&self.dir)?;

		let (writer, sync_fd) = Self::create_writer(&self.dir, new_log_number, &self.opts)?;
		self.active_writer = Some(writer);
		self.sync_fd = Some(sync_fd);
		self.active_log_number = new_log_number;

		tracing::debug!("WAL moved {:020} to {:020}", old_log_number, new_log_number);
		Ok(Some(new_log_number))
	}
}

/// Whether the bytes of `file` from `from` to `to` are what a writer leaves after the last record
/// of a segment: whole sync marks, and the zeros that pad the end of a block with too little room
/// for a mark (see `Writer::write_sync_mark_unflushed`). The replay reads these bytes as the end of
/// the log, so a tail made of them holds no record.
///
/// A mark is checked as the reader checks one: its checksum, its length and its block. A tail that
/// is shorter than a mark is accepted only if it is zeros, and a read that comes back short is
/// not accepted.
pub(crate) fn tail_is_marks_and_padding(file: &dyn FsFile, from: u64, to: u64) -> Result<bool> {
	const MARK_LEN: usize = HEADER_SIZE + SYNC_MARK_LEN;
	let mut pos = from;
	while pos < to {
		let mut head = [0u8; MARK_LEN];
		let want = usize::try_from((to - pos).min(MARK_LEN as u64)).unwrap_or(MARK_LEN);
		let read = file
			.read_at(pos, &mut head[..want])
			.map_err(|e| Error::IO(IOError::new(io::ErrorKind::Other, &e.to_string())))?;
		if read != want {
			return Ok(false);
		}
		let padding = want < MARK_LEN || head[HEADER_SIZE - 1] == RecordType::Empty as u8;
		if padding {
			let block_end = pos + (BLOCK_SIZE as u64 - pos % BLOCK_SIZE as u64);
			let end = block_end.min(to);
			if !all_zeros(file, pos, end)? {
				return Ok(false);
			}
			pos = end;
		} else {
			if sync_mark_in(&head, pos, 0).is_none() {
				return Ok(false);
			}
			pos += MARK_LEN as u64;
		}
	}
	Ok(true)
}

/// Whether the bytes of `file` from `from` to `to` are all zeros.
fn all_zeros(file: &dyn FsFile, from: u64, to: u64) -> Result<bool> {
	let mut buf = vec![0u8; COPY_CHUNK];
	let mut offset = from;
	while offset < to {
		let want = usize::try_from((to - offset).min(COPY_CHUNK as u64)).unwrap_or(COPY_CHUNK);
		let read = file
			.read_at(offset, &mut buf[..want])
			.map_err(|e| Error::IO(IOError::new(io::ErrorKind::Other, &e.to_string())))?;
		if read == 0 {
			break;
		}
		if buf[..read].iter().any(|byte| *byte != 0) {
			return Ok(false);
		}
		offset += read as u64;
	}
	Ok(true)
}

impl Drop for Wal {
	/// Attempt to fsync data on drop, in case we're running without sync.
	fn drop(&mut self) {
		if !self.closed {
			self.close().ok();
		}
	}
}

/// What `WalManager::sync` fsyncs through outside the WAL lock: the active segment's file, shared
/// with its writer, and the segment's `SyncGate`.
pub(crate) struct SyncHandle {
	/// The file and its gate, `None` for a sealed WAL that has no segment yet, which has nothing
	/// to sync.
	target: Option<(Arc<dyn FsFile>, Arc<SyncGate>)>,
}

impl SyncHandle {
	/// fsyncs the segment. Fails if an earlier fsync of it failed.
	pub(crate) fn sync(&self) -> Result<()> {
		match &self.target {
			Some((file, gate)) => gate.sync(&**file),
			None => Ok(()),
		}
	}

	/// Makes the fsync of the segment after the next `syncs` of them fail once.
	#[cfg(test)]
	pub(crate) fn fail_after(&self, syncs: usize) {
		if let Some((_, gate)) = &self.target {
			gate.fail_after(syncs);
		}
	}

	/// How many fsyncs of the segment were asked for so far, the ones still waiting for an
	/// fsync in flight included. Read without the WAL lock, which a waiting fsync may hold.
	#[cfg(test)]
	pub(crate) fn arrivals(&self) -> usize {
		self.target.as_ref().map_or(0, |(_, gate)| gate.arrivals())
	}
}

/// Thread-safe WAL handle that encapsulates lock management.
///
/// Provides `sync()` and `flush()` methods that manage the internal
/// RwLock, matching VLog's pattern. The expensive fsync in `sync()`
/// is performed outside the write lock using a pre-cloned file
/// descriptor, allowing concurrent WAL appends to proceed.
pub(crate) struct WalManager {
	pub(crate) inner: Arc<parking_lot::RwLock<Wal>>,
	/// Test-only: the fsyncs `sync` has completed.
	#[cfg(test)]
	fsyncs: std::sync::atomic::AtomicUsize,
}

impl WalManager {
	pub(crate) fn new(wal: Wal) -> Self {
		Self {
			inner: Arc::new(parking_lot::RwLock::new(wal)),
			#[cfg(test)]
			fsyncs: std::sync::atomic::AtomicUsize::new(0),
		}
	}

	/// Test-only: the fsyncs `sync` has completed.
	#[cfg(test)]
	pub(crate) fn fsyncs(&self) -> usize {
		self.fsyncs.load(std::sync::atomic::Ordering::SeqCst)
	}

	/// Syncs WAL data to disk using two-phase pattern:
	/// 1. Under write lock: flush BufWriter to OS page cache
	/// 2. Outside lock: fsync to disk via pre-cloned fd, through the segment's `SyncGate`
	///
	/// Appends, and a rotation, may run between the two phases. The fsync belongs to the segment
	/// of the first phase: if it fails, that segment's writer is poisoned, so an append that
	/// landed in the meantime is never acknowledged by a later fsync (the gate makes it wait for
	/// this one and then fail), and a rotation that got there first has already synced the
	/// segment and leaves the new one alone.
	pub(crate) fn sync(&self) -> Result<()> {
		let handle = {
			let mut wal = self.inner.write();
			// A held segment has nothing to sync, and a sync that finds nothing pending must
			// not read as success for the segment it replaced.
			if wal.is_held() {
				return Err(super::segment_held());
			}
			wal.flush()?;
			wal.sync_handle()
		};
		handle.sync()?;
		#[cfg(test)]
		self.fsyncs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
		Ok(())
	}

	/// Flushes WAL buffer to OS page cache (no fsync).
	pub(crate) fn flush(&self) -> Result<()> {
		let mut wal = self.inner.write();
		wal.flush()?;
		Ok(())
	}

	/// Returns a write guard for direct WAL access (append, rotate, close).
	pub(crate) fn write(&self) -> parking_lot::RwLockWriteGuard<'_, Wal> {
		self.inner.write()
	}

	/// Returns a read guard for read-only WAL access.
	pub(crate) fn read(&self) -> parking_lot::RwLockReadGuard<'_, Wal> {
		self.inner.read()
	}
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
	use std::fs;

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
			tracing::debug!("After open_with_min_log_number(1): active_log_number = {}", active);

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

	/// `sizes` as records of that many bytes each: one buffer and where each one ends.
	fn group(sizes: &[usize]) -> (Vec<u8>, Vec<usize>) {
		let mut buf = Vec::new();
		let mut ends = Vec::new();
		for (i, size) in sizes.iter().enumerate() {
			buf.extend(std::iter::repeat_n(i as u8 + 1, *size));
			ends.push(buf.len());
		}
		(buf, ends)
	}

	fn segment_len(dir: &Path, log_number: u64) -> u64 {
		fs::metadata(dir.join(format!("{log_number:020}.wal"))).unwrap().len()
	}

	/// A group is the records of one `append` each in one write, the segment it reports is
	/// the one it was written to, and a rotation between two groups moves the report on.
	#[test]
	fn test_append_group() {
		let temp_dir = create_temp_directory();
		let mut wal = Wal::open(temp_dir.path(), Options::default()).unwrap();
		let first = wal.get_active_log_number();

		let (buf, ends) = group(&[10, 200, 3, 4000]);
		assert_eq!(wal.append_group(&buf, &ends).unwrap(), first);
		assert_eq!(wal.file_writes(), 1, "four small records are one write");

		// The same records one `append` at a time are one write each, and the same bytes.
		let other = create_temp_directory();
		let mut one_by_one = Wal::open(other.path(), Options::default()).unwrap();
		let mut start = 0;
		for end in &ends {
			one_by_one.append(&buf[start..*end]).unwrap();
			start = *end;
		}
		assert_eq!(one_by_one.file_writes(), ends.len());
		assert_eq!(segment_len(temp_dir.path(), first), segment_len(other.path(), first));
		assert_eq!(
			fs::read(temp_dir.path().join("00000000000000000000.wal")).unwrap(),
			fs::read(other.path().join("00000000000000000000.wal")).unwrap()
		);

		wal.rotate().unwrap();
		assert_eq!(wal.append_group(&buf, &ends).unwrap(), first + 1);
		wal.close().unwrap();
	}

	/// A call that cannot be a group of records writes nothing and does not poison the
	/// writer: there is nothing to cut off, so the next append works.
	#[test]
	fn test_append_group_rejects_malformed_input_without_writing() {
		let temp_dir = create_temp_directory();
		let mut wal = Wal::open(temp_dir.path(), Options::default()).unwrap();
		wal.append(&[1, 2, 3]).unwrap();
		let len = segment_len(temp_dir.path(), 0);

		let (buf, ends) = group(&[10, 20, 30]);
		assert!(wal.append_group(&buf, &[0, 10, 60]).is_err(), "an empty first record");
		assert!(wal.append_group(&buf, &[10, 10, 60]).is_err(), "an empty record");
		assert!(wal.append_group(&buf, &[30, 10, 60]).is_err(), "ends that go backwards");
		assert!(wal.append_group(&buf, &[10, 30, 61]).is_err(), "an end past the buffer");
		assert!(wal.append_group(&[], &[1]).is_err(), "an end past an empty buffer");
		assert_eq!(segment_len(temp_dir.path(), 0), len, "nothing was written");
		assert_eq!(wal.file_writes(), 1, "nothing reached the file");

		// An empty group is a no-op, not an error.
		assert_eq!(wal.append_group(&buf, &[]).unwrap(), 0);
		assert_eq!(segment_len(temp_dir.path(), 0), len);

		assert!(wal.append_group(&buf, &ends).is_ok(), "the writer is not poisoned");
		assert!(wal.append(&[4, 5, 6]).is_ok());

		wal.close().unwrap();
		assert!(wal.append_group(&buf, &ends).is_err(), "a closed WAL takes no group");
	}

	/// `before_sync` runs once, before the old segment's fsync, and the rotation goes on to the
	/// new segment.
	#[test]
	fn test_rotate_with_runs_the_hook_before_the_fsync() {
		let temp_dir = create_temp_directory();
		let mut wal = Wal::open(temp_dir.path(), Options::default()).unwrap();
		wal.append(&[1, 2, 3]).unwrap();

		let events = Arc::new(parking_lot::Mutex::new(Vec::new()));
		let observed = Arc::clone(&events);
		wal.set_sync_observer(Some(Arc::new(move || observed.lock().push("fsync"))));
		let hooked = Arc::clone(&events);
		let rotated = wal.rotate_with(|| {
			hooked.lock().push("hook");
			Ok(())
		});
		assert_eq!(rotated.unwrap(), 1);
		assert_eq!(*events.lock(), ["hook", "fsync"]);
		assert_eq!(wal.get_active_log_number(), 1);
		wal.close().unwrap();
	}

	/// A hook that fails fails the rotation before the segment is fsynced or anything is created:
	/// the WAL is as it was, takes appends, and rotates when the hook works.
	#[test]
	fn test_rotate_with_a_failing_hook_leaves_the_wal_as_it_was() {
		let temp_dir = create_temp_directory();
		let mut wal = Wal::open(temp_dir.path(), Options::default()).unwrap();
		wal.append(&[1, 2, 3]).unwrap();
		wal.flush().unwrap();
		let len = segment_len(temp_dir.path(), 0);
		assert!(len > 0, "the segment holds the record");

		let fsyncs = Arc::new(parking_lot::Mutex::new(0));
		let observed = Arc::clone(&fsyncs);
		wal.set_sync_observer(Some(Arc::new(move || *observed.lock() += 1)));
		let failure =
			|| Err(Error::IO(IOError::new(io::ErrorKind::Other, "injected hook failure")));
		assert!(wal.rotate_with(failure).is_err());
		assert_eq!(*fsyncs.lock(), 0, "the segment was not fsynced");
		assert_eq!(wal.get_active_log_number(), 0);
		assert!(!temp_dir.path().join("00000000000000000001.wal").exists());
		assert!(wal.pending_sync(), "nothing was synced");
		assert!(!wal.sync_failed(), "the failure of the hook is not a failed fsync");
		assert_eq!(segment_len(temp_dir.path(), 0), len);

		wal.append(&[4, 5, 6]).unwrap();
		assert_eq!(wal.rotate_with(|| Ok(())).unwrap(), 1);
		assert_eq!(*fsyncs.lock(), 1);
		wal.close().unwrap();
	}

	/// A segment whose fsync failed is replaced without its hook: it is not fsynced again, so
	/// there is nothing for the hook to come before.
	#[test]
	fn test_rotate_with_skips_the_hook_for_a_poisoned_segment() {
		let temp_dir = create_temp_directory();
		let mut wal = Wal::open(temp_dir.path(), Options::default()).unwrap();
		wal.append(&[1, 2, 3]).unwrap();
		wal.fail_syncs_after(0);
		assert!(wal.sync().is_err());
		assert!(wal.sync_failed());

		let ran = Arc::new(parking_lot::Mutex::new(false));
		let hooked = Arc::clone(&ran);
		let rotated = wal.rotate_with(|| {
			*hooked.lock() = true;
			Ok(())
		});
		assert_eq!(rotated.unwrap(), 1);
		assert!(!*ran.lock(), "the hook ran for a segment that is not fsynced");
		assert!(!wal.sync_failed());
		assert_eq!(wal.hold_below(), Some(1), "the segment that replaces it is held");
		wal.release_hold(1);
		wal.close().unwrap();
	}
}
