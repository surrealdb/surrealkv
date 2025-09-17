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

	/// Check if the storage is empty
	fn is_empty(&self) -> Result<bool> {
		Ok(self.len()? == 0)
	}
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

/// In-memory storage implementation
pub struct MemoryStorage {
	data: Vec<u8>,
}

impl MemoryStorage {
	pub fn new() -> Self {
		MemoryStorage {
			data: Vec::new(),
		}
	}
}

impl Default for MemoryStorage {
	fn default() -> Self {
		Self::new()
	}
}

impl Storage for MemoryStorage {
	fn read_at(&self, buffer: &mut [u8], offset: u64) -> Result<()> {
		let start = offset as usize;
		let end = start + buffer.len();

		if end > self.data.len() {
			return Err(super::tree::BPlusTreeError::InvalidOffset);
		}

		buffer.copy_from_slice(&self.data[start..end]);
		Ok(())
	}

	fn write_at(&mut self, buffer: &[u8], offset: u64) -> Result<()> {
		let start = offset as usize;
		let end = start + buffer.len();

		// Ensure data vector is large enough
		if end > self.data.len() {
			self.data.resize(end, 0);
		}

		self.data[start..end].copy_from_slice(buffer);
		Ok(())
	}

	fn sync(&self) -> Result<()> {
		// No-op for in-memory storage
		Ok(())
	}

	fn sync_data(&self) -> Result<()> {
		// No-op for in-memory storage
		Ok(())
	}

	fn len(&self) -> Result<u64> {
		Ok(self.data.len() as u64)
	}
}
