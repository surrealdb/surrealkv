use std::fs::File as SysFile;

use crate::error::{Error, Result};

/// Read-only random-access file abstraction for SSTable blocks.
pub trait File: Send + Sync {
	/// Reads bytes from the given offset into the buffer.
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;

	/// Returns total file size in bytes.
	fn size(&self) -> Result<u64>;
}

pub type InMemoryFile = Vec<u8>;

impl File for InMemoryFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		let start = offset as usize;
		let end = std::cmp::min(start + buf.len(), self.len());
		let bytes_read = end - start;
		buf[..bytes_read].copy_from_slice(&self[start..end]);
		Ok(bytes_read)
	}

	fn size(&self) -> Result<u64> {
		Ok(self.len() as u64)
	}
}

impl File for SysFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		#[cfg(unix)]
		{
			std::os::unix::prelude::FileExt::read_at(self, buf, offset)
				.map_err(|e| Error::Io(e.into()))
		}

		#[cfg(windows)]
		{
			std::os::windows::prelude::FileExt::seek_read(self, buf, offset)
				.map_err(|e| Error::Io(e.into()))
		}

		#[cfg(target_arch = "wasm32")]
		{
			Err(Error::Io(
				std::io::Error::new(
					std::io::ErrorKind::Unsupported,
					"read_at is not supported on WASM",
				)
				.into(),
			))
		}
	}

	fn size(&self) -> Result<u64> {
		self.metadata().map(|m| m.len()).map_err(|e| Error::Io(e.into()))
	}
}

/// Opens a file with the access mode required for `sync_all()` to work
/// on all platforms.
///
/// On Windows, `FlushFileBuffers` (called by `sync_all`) requires
/// `GENERIC_WRITE` — a read-only handle returns `ERROR_ACCESS_DENIED`.
/// On Unix, read-only access is sufficient for fsync.
pub fn open_for_sync<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<SysFile> {
	#[cfg(target_os = "windows")]
	{
		std::fs::OpenOptions::new().read(true).write(true).open(path)
	}
	#[cfg(not(target_os = "windows"))]
	{
		SysFile::open(path)
	}
}

/// Fsyncs an existing file's data and metadata to disk (durability barrier).
///
/// Uses `sync_all` deliberately: readers depend on the durable file *size*
/// (e.g. SST footers are located relative to the end of the file), so syncing
/// data alone is not enough.
pub(crate) fn fsync_file<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<()> {
	let file = open_for_sync(&path)?;
	file.sync_all()?;
	#[cfg(test)]
	sync_tracker::record(path.as_ref());
	Ok(())
}

/// Test-only ledger of files made durable via [`fsync_file`].
///
/// Lets crash-consistency tests simulate power loss faithfully: files NOT in
/// the ledger may legally lose their data (truncate to 0), while files in it
/// must survive intact.
#[cfg(test)]
pub(crate) mod sync_tracker {
	use std::collections::HashSet;
	use std::path::{Path, PathBuf};
	use std::sync::{LazyLock, Mutex};

	static SYNCED: LazyLock<Mutex<HashSet<PathBuf>>> = LazyLock::new(Default::default);

	// Canonicalize symmetrically in record() and was_synced(): on macOS,
	// temp dirs under /var/folders canonicalize to /private/var/folders, so
	// one-sided canonicalization would make lookups miss.
	fn key(path: &Path) -> PathBuf {
		path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
	}

	pub(crate) fn record(path: &Path) {
		SYNCED.lock().unwrap().insert(key(path));
	}

	pub(crate) fn was_synced(path: &Path) -> bool {
		SYNCED.lock().unwrap().contains(&key(path))
	}
}
