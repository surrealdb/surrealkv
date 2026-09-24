//! Error types for RocksDB compatibility reader.

use thiserror::Error;

/// Result type for RocksDB compatibility operations.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
	#[error("I/O error: {0}")]
	Io(#[from] std::io::Error),

	#[error("Invalid RocksDB magic number: {0:02x?}")]
	InvalidMagic([u8; 8]),

	#[error("Corrupt or invalid SSTable footer at offset {offset}")]
	CorruptFooter {
		offset: u64,
	},

	#[error("Unsupported RocksDB format version: {0} (supported: 2..=7)")]
	UnsupportedFormatVersion(u32),

	#[error("Unsupported compression type: {0}")]
	UnsupportedCompression(u8),

	#[error("Checksum mismatch: expected {expected:#x}, got {computed:#x}")]
	ChecksumMismatch {
		expected: u32,
		computed: u32,
	},

	#[error("Decompression failed: {0}")]
	DecompressionFailed(String),

	#[error("Corrupt block data at offset {offset}: {reason}")]
	CorruptBlock {
		offset: u64,
		reason: String,
	},

	#[error("Not a RocksDB directory: CURRENT file missing or invalid")]
	NotARocksDbDirectory,
}
