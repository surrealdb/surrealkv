use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::Path;

use super::tree::Result;

/// Storage abstraction for BPlusTree
pub trait Storage: Send + Sync {
	/// Read bytes at a specific offset
	fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()>;

	/// Write bytes at a specific offset
	fn write_at(&mut self, buf: &[u8], offset: u64) -> Result<()>;

	/// Sync data to stable storage
	fn sync(&self) -> Result<()>;

	/// Sync data to stable storage
	fn sync_data(&self) -> Result<()>;

	/// Get the current length of the storage
	fn len(&self) -> Result<u64>;
}

/// Disk-based storage implementation
pub struct DiskStorage {
	file: File,
}

impl DiskStorage {
	/// Create a new disk-based storage
	pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
		let file =
			OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
		Ok(Self {
			file,
		})
	}
}

impl Storage for DiskStorage {
	fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
		Ok(self.file.read_exact_at(buf, offset)?)
	}

	fn write_at(&mut self, buf: &[u8], offset: u64) -> Result<()> {
		Ok(self.file.write_all_at(buf, offset)?)
	}

	fn sync(&self) -> Result<()> {
		Ok(self.file.sync_all()?)
	}

	fn sync_data(&self) -> Result<()> {
		Ok(self.file.sync_data()?)
	}

	fn len(&self) -> Result<u64> {
		Ok(self.file.metadata()?.len())
	}
}
