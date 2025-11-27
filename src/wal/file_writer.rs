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

//! Buffered file writing for WAL segments.
//!
//! This module provides BufferedFileWriter, a simple wrapper around BufWriter<File>
//! that provides the interface needed by Writer.

use std::fs::File;
use std::io::{BufWriter, Write};

use super::segment::Result;

/// Trait for writable files.
pub trait WritableFile: Send {
	/// Appends data to the file.
	fn append(&mut self, data: &[u8]) -> Result<()>;

	/// Flushes buffered data to OS cache (not to disk).
	fn flush(&mut self) -> Result<()>;

	/// Syncs all data to disk (flush + fsync).
	fn sync(&mut self) -> Result<()>;

	/// Closes the file, flushing and syncing any remaining buffered data.
	fn close(&mut self) -> Result<()>;
}

/// Buffered file writer wrapping BufWriter<File>.
pub struct BufferedFileWriter {
	writer: BufWriter<File>,
	pending_sync: bool,
}

impl BufferedFileWriter {
	/// Creates a new BufferedFileWriter with the specified buffer size.
	pub fn new(file: File, buffer_size: usize) -> Self {
		Self {
			writer: BufWriter::with_capacity(buffer_size, file),
			pending_sync: false,
		}
	}
}

impl WritableFile for BufferedFileWriter {
	fn append(&mut self, data: &[u8]) -> Result<()> {
		self.writer.write_all(data)?;
		self.pending_sync = true;
		Ok(())
	}

	fn flush(&mut self) -> Result<()> {
		self.writer.flush()?;
		Ok(())
	}

	fn sync(&mut self) -> Result<()> {
		if !self.pending_sync {
			return Ok(());
		}
		self.writer.flush()?;
		self.writer.get_ref().sync_all()?;
		self.pending_sync = false;
		Ok(())
	}

	fn close(&mut self) -> Result<()> {
		self.sync()?;
		Ok(())
	}
}
