//! Pure-Rust compatibility reader and migration utilities for RocksDB BlockBasedTable format (v2 through v7).
//!
//! Designed to allow SurrealKV and SurrealDB to start up directly from existing RocksDB
//! databases without linking the C++ RocksDB library or requiring offline manual migrations.

pub mod block;
pub mod database;
pub mod error;
pub mod footer;
pub mod handle;
pub mod table;
pub mod varint;

pub use database::{is_rocksdb_dir, DatabaseIter, RocksDbDatabase};
pub use error::{Error, Result};
pub use footer::{Footer, ROCKSDB_MAGIC, ROCKSDB_LEGACY_MAGIC};
pub use handle::BlockHandle;
pub use table::{RocksDbEntry, SstReader};

#[cfg(test)]
mod tests;

use std::path::Path;

/// Reads all live (non-deleted) latest key-value pairs from a RocksDB directory in pure Rust.
pub fn read_all_latest<P: AsRef<Path>>(path: P) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	let db = RocksDbDatabase::open(path)?;
	let mut records = Vec::new();
	for item in db.iter_latest()? {
		records.push(item?);
	}
	Ok(records)
}
