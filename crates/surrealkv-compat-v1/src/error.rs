use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
	#[error("io error: {0}")]
	Io(#[from] std::io::Error),
	/// Bytes that do not decode as what they should be, with what was wrong. Includes a failed
	/// checksum.
	#[error("corrupted: {0}")]
	Corrupted(String),
	/// Something that is valid but that this crate does not read.
	#[error("unsupported: {0}")]
	Unsupported(String),
}
