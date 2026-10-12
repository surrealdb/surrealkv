//! A pure-Rust reader of the RocksDB databases SurrealDB writes, for migrating them to SurrealKV.
//!
//! It reads what RocksDB's own recovery would: the live tables the MANIFEST lists, the blob files
//! their values live in, and the write-ahead logs that no table covers. With the timestamp
//! comparator SurrealDB uses in versioned mode it yields every stored version with its timestamp.
//!
//! It refuses, with [`Error::Unsupported`], anything it does not handle, instead of reading it
//! wrongly: other comparators, other column families, range deletes, merge operands, tables
//! without their timestamps, compression it cannot decode.

mod blob;
mod checksum;
mod coding;
mod compression;
mod db;
mod error;
mod lock;
mod log;
mod manifest;
mod sst;
mod wal;

pub use blob::{BlobFiles, BlobRef};
pub use db::{
	is_rocksdb_dir,
	Payload,
	RocksDb,
	Version,
	VersionKind,
	Versions,
	BYTEWISE_COMPARATOR,
	TIMESTAMP_COMPARATOR,
};
pub use error::{Error, Result};
pub use lock::{lock_directory, DirectoryLock};
pub use manifest::{ManifestState, TableFile};
pub use sst::{Properties, TableReader};
