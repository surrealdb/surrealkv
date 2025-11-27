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

//! Configuration options for WAL segments.

use std::io;

use super::format::CompressionType;
use super::segment::{Error, IOError, Result};

/// Default file mode for newly created files.
const DEFAULT_FILE_MODE: u32 = 0o644;

/// Default maximum size of the segment file (100MB).
const DEFAULT_FILE_SIZE: u64 = 100 * 1024 * 1024;

/// Options for configuring a WAL segment.
///
/// This struct provides configuration options for WAL segments, including
/// file permissions, compression settings, metadata, and file size limits.
#[derive(Clone)]
pub struct Options {
	/// The permission mode for creating directories.
	pub(crate) dir_mode: Option<u32>,

	/// The file mode to set for the segment file.
	pub(crate) file_mode: Option<u32>,

	/// The compression type to apply to the segment's data.
	pub(crate) compression_type: Option<CompressionType>,

	/// The extension to use for the segment file.
	pub(crate) file_extension: Option<String>,

	/// The maximum size of the segment file.
	pub(crate) max_file_size: u64,
}

impl Default for Options {
	fn default() -> Self {
		Options {
			dir_mode: Some(0o750),
			file_mode: Some(DEFAULT_FILE_MODE),
			compression_type: Some(CompressionType::None),
			file_extension: Some("wal".to_string()),
			max_file_size: DEFAULT_FILE_SIZE,
		}
	}
}

impl Options {
	/// Validates the options.
	pub(crate) fn validate(&self) -> Result<()> {
		if self.max_file_size == 0 {
			return Err(Error::IO(IOError::new(
				io::ErrorKind::InvalidInput,
				"invalid max_file_size",
			)));
		}

		Ok(())
	}

	/// Sets the maximum file size.
	#[allow(unused)]
	pub(crate) fn with_max_file_size(mut self, max_file_size: u64) -> Self {
		self.max_file_size = max_file_size;
		self
	}

	/// Sets the compression type.
	#[allow(unused)]
	pub(crate) fn with_compression(mut self, compression_type: CompressionType) -> Self {
		self.compression_type = Some(compression_type);
		self
	}

	/// Sets the file extension.
	#[allow(unused)]
	pub(crate) fn with_extension(mut self, extension: String) -> Self {
		self.file_extension = Some(extension);
		self
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_options_validation() {
		let mut opts = Options::default();
		assert!(opts.validate().is_ok());

		opts.max_file_size = 0;
		assert!(opts.validate().is_err());
	}

	#[test]
	fn test_options_builder() {
		let opts = Options::default()
			.with_max_file_size(50 * 1024 * 1024)
			.with_compression(CompressionType::Lz4)
			.with_extension("log".to_string());

		assert_eq!(opts.max_file_size, 50 * 1024 * 1024);
		assert_eq!(opts.compression_type, Some(CompressionType::Lz4));
		assert_eq!(opts.file_extension, Some("log".to_string()));
	}
}
