//! Errors of the RocksDB reader.

use std::io;

#[derive(Debug, thiserror::Error)]
pub enum Error {
	#[error("I/O error: {0}")]
	Io(#[from] io::Error),
	/// The bytes are not what the format says. The message names the file and what is wrong.
	#[error("corrupt RocksDB data: {0}")]
	Corrupt(String),
	/// A process holds the database open: RocksDB's lock on it is taken.
	#[error("{0}")]
	InUse(String),
	/// The database uses something this reader does not handle. Reading it anyway would drop or
	/// misread data, so the migration refuses instead.
	#[error("unsupported RocksDB feature: {0}")]
	Unsupported(String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn corrupt(message: impl Into<String>) -> Error {
	Error::Corrupt(message.into())
}

pub(crate) fn unsupported(message: impl Into<String>) -> Error {
	Error::Unsupported(message.into())
}
