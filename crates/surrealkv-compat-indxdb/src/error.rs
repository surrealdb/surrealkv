use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
#[non_exhaustive]
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

	/// There is no IndexedDB in this context: the global scope has no `indexedDB` (it is
	/// `undefined` or `null`), or reading it throws, as it does in a context where the browser
	/// blocks storage. Nothing about a database is known from this. An error from opening or
	/// reading a database is never this one.
	#[error("IndexedDB is not available in this context: {0}")]
	Unavailable(String),
}
