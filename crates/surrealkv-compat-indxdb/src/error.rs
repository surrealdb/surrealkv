use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
	#[error("I/O error: {0}")]
	Io(#[from] std::io::Error),

	#[error("IndexedDB JavaScript error: {0}")]
	Js(String),

	#[error("Object store not found: {0}")]
	ObjectStoreNotFound(String),

	#[error("Database not found: {0}")]
	DatabaseNotFound(String),

	#[error("Corrupt dump format: {0}")]
	CorruptDump(String),
}
