use std::fs::File as SysFile;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};

#[cfg(not(target_arch = "wasm32"))]
use fs2::FileExt as LockFileExt;

use crate::error::{Error, Result};

pub trait File: Send + Sync {
	#[allow(unused)]
	fn write(&mut self, buf: &[u8]) -> Result<usize>;
	#[allow(unused)]
	fn flush(&mut self) -> Result<()>;
	#[allow(unused)]
	fn close(&mut self) -> Result<()>;
	#[allow(unused)]
	fn seek(&mut self, pos: SeekFrom) -> Result<u64>;
	#[allow(unused)]
	fn read(&mut self, buf: &mut [u8]) -> Result<usize>;
	#[allow(unused)]
	fn read_all(&mut self, buf: &mut Vec<u8>) -> Result<usize>;
	#[allow(unused)]
	fn lock(&self) -> Result<()>;
	#[allow(unused)]
	fn unlock(&self) -> Result<()>;
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;
	#[allow(unused)]
	fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<usize>;
	#[allow(unused)]
	fn sync(&self) -> Result<()>;
	#[allow(unused)]
	fn sync_data(&self) -> Result<()>;
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

	fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<usize> {
		let start = offset as usize;
		let end = start + buf.len();

		// Ensure the vector is large enough
		if end > self.len() {
			self.resize(end, 0);
		}

		// Write the data
		self[start..end].copy_from_slice(buf);
		Ok(buf.len())
	}

	fn sync(&self) -> Result<()> {
		Ok(()) // In-memory file doesn't need syncing
	}

	fn sync_data(&self) -> Result<()> {
		Ok(()) // In-memory file doesn't need syncing
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
		#[cfg(not(target_arch = "wasm32"))]
		{
			SysFile::try_lock_exclusive(self).map_err(|e| Error::Io(e.into()))
		}
		#[cfg(target_arch = "wasm32")]
		{
			// File locking is not supported on WASM
			Ok(())
		}
	}

	fn unlock(&self) -> Result<()> {
		#[cfg(not(target_arch = "wasm32"))]
		{
			LockFileExt::unlock(self).map_err(|e| Error::Io(e.into()))
		}
		#[cfg(target_arch = "wasm32")]
		{
			// File unlocking is not supported on WASM
			Ok(())
		}
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

		#[cfg(target_arch = "wasm32")]
		{
			// read_at is not supported on WASM, return an error
			Err(Error::Io(
				std::io::Error::new(
					std::io::ErrorKind::Unsupported,
					"read_at is not supported on WASM",
				)
				.into(),
			))
		}
	}

	fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<usize> {
		#[cfg(unix)]
		{
			std::os::unix::prelude::FileExt::write_all_at(self, buf, offset)
				.map_err(|e| Error::Io(e.into()))?;
			Ok(buf.len())
		}

		#[cfg(windows)]
		{
			std::os::windows::prelude::FileExt::seek_write(self, buf, offset)
				.map_err(|e| Error::Io(e.into()))
		}

		#[cfg(target_arch = "wasm32")]
		{
			// write_at is not supported on WASM, return an error
			Err(Error::Io(
				std::io::Error::new(
					std::io::ErrorKind::Unsupported,
					"write_at is not supported on WASM",
				)
				.into(),
			))
		}
	}

	fn sync(&self) -> Result<()> {
		SysFile::sync_all(self).map_err(|e| Error::Io(e.into()))
	}

	fn sync_data(&self) -> Result<()> {
		SysFile::sync_data(self).map_err(|e| Error::Io(e.into()))
	}

	fn size(&self) -> Result<u64> {
		match SysFile::metadata(self) {
			Ok(v) => Ok(v.len()),
			Err(e) => Err(Error::Io(e.into())),
		}
	}
}
