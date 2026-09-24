use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
	#[error("I/O error: {0}")]
	Io(#[from] std::io::Error),

	#[error("Invalid or corrupted magic number: expected {expected:?}, found {found:?}")]
	BadMagic {
		expected: Vec<u8>,
		found: Vec<u8>,
	},

	#[error("Corrupt footer: {0}")]
	CorruptFooter(String),

	#[error("Corrupt block handle at offset {0}")]
	CorruptBlockHandle(usize),

	#[error("Corrupt block: {0}")]
	CorruptBlock(String),

	#[error("Decompression failed: {0}")]
	Decompression(String),

	#[error("Checksum mismatch: expected {expected:#x}, calculated {calculated:#x}")]
	ChecksumMismatch {
		expected: u32,
		calculated: u32,
	},

	#[error("Unsupported table format version: {0}")]
	UnsupportedFormat(u8),
}
