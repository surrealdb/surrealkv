use crate::error::{Error, Result};

use std::io::Cursor;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;

use fs4::FileExt as LockFileExt;

use std::fs::File as SysFile;

pub trait File: Send + Sync {
	#[allow(dead_code)] // Used in test code and WAL
	fn write(&mut self, buf: &[u8]) -> Result<usize>;
	#[allow(dead_code)] // Used in test code and WAL
	fn flush(&mut self) -> Result<()>;
	#[allow(dead_code)] // Used in test code and WAL
	fn close(&mut self) -> Result<()>;
	#[allow(dead_code)] // Used in test code and WAL
	fn seek(&mut self, pos: SeekFrom) -> Result<u64>;
	#[allow(dead_code)] // Used in test code and WAL
	fn read(&mut self, buf: &mut [u8]) -> Result<usize>;
	#[allow(dead_code)] // Used in test code and WAL
	fn read_all(&mut self, buf: &mut Vec<u8>) -> Result<usize>;
	#[allow(dead_code)] // Used in test code and WAL
	fn lock(&self) -> Result<()>;
	#[allow(dead_code)] // Used in test code and WAL
	fn unlock(&self) -> Result<()>;
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;
	fn size(&self) -> Result<u64>;
}

pub type InMemoryFile = Vec<u8>;

impl File for InMemoryFile {
	fn write(&mut self, buf: &[u8]) -> Result<usize> {
		self.extend_from_slice(buf);
		Ok(buf.len())
	}

	fn flush(&mut self) -> Result<()> {
		Ok(()) // In-memory file doesn't need flushing
	}

	fn close(&mut self) -> Result<()> {
		Ok(()) // No specific action needed for closing an in-memory file
	}

	fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
		let mut cursor = Cursor::new(self);
		cursor.seek(pos).map_err(|e| Error::Io(e.into()))?;
		Ok(cursor.position())
	}

	fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
		let mut cursor = Cursor::new(self);
		cursor.read(buf).map_err(|e| Error::Io(e.into()))
	}

	fn read_all(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
		buf.clear();
		buf.extend_from_slice(self);
		Ok(self.len())
	}

	fn lock(&self) -> Result<()> {
		Ok(()) // In-memory file doesn't support locking
	}

	fn unlock(&self) -> Result<()> {
		Ok(()) // In-memory file doesn't support unlocking
	}

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
	fn write(&mut self, buf: &[u8]) -> Result<usize> {
		Write::write(self, buf).map_err(|e| Error::Io(e.into()))
	}

	fn flush(&mut self) -> Result<()> {
		Write::flush(self).map_err(|e| Error::Io(e.into()))
	}

	fn close(&mut self) -> Result<()> {
		Ok(())
	}

	fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
		Seek::seek(self, pos).map_err(|e| Error::Io(e.into()))
	}

	fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
		std::io::Read::read(self, buf).map_err(|e| Error::Io(e.into()))
	}

	fn read_all(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
		std::io::Read::read_to_end(self, buf).map_err(|e| Error::Io(e.into()))
	}

	fn lock(&self) -> Result<()> {
		SysFile::try_lock_exclusive(self).map_err(|e| Error::Io(e.into()))
	}

	fn unlock(&self) -> Result<()> {
		LockFileExt::unlock(self).map_err(|e| Error::Io(e.into()))
	}

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
	}

	fn size(&self) -> Result<u64> {
		match SysFile::metadata(self) {
			Ok(v) => Ok(v.len()),
			Err(e) => Err(Error::Io(e.into())),
		}
	}
}
