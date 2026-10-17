//! Pure-Rust and WebAssembly reader for legacy SurrealDB IndexedDB (`indxdb://`) stores.
//!
//! In a browser, `wasm::IdbSource` reads the object store `kv` of a legacy database in chunks,
//! and never creates, changes or deletes it. `surrealkv` moves what it reads into a database in the
//! origin private file system. Outside a browser, [`read_dump`] reads an exported dump.

pub mod chunk;
pub mod dump;
pub mod error;

#[cfg(target_arch = "wasm32")]
pub mod wasm;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

use std::path::Path;

pub use chunk::{Chunk, ChunkFill, ChunkLimits, DEFAULT_CHUNK_BYTES, DEFAULT_CHUNK_ENTRIES};
pub use dump::{export_dump, is_indxdb_dump_dir, read_dump, INDXDB_DUMP_MAGIC};
pub use error::{Error, Result};

/// Checks if an IndexedDB database or exported dump exists for the given name or path.
pub fn is_indxdb_available(path_or_name: &str) -> bool {
	// Check offline dump file first
	if is_indxdb_dump_dir(Path::new(path_or_name)) {
		return true;
	}

	#[cfg(target_arch = "wasm32")]
	{
		wasm::is_indexeddb_available()
	}

	#[cfg(not(target_arch = "wasm32"))]
	{
		false
	}
}

/// Reads all live key-value pairs from an IndexedDB database or dump.
pub async fn read_all_latest(path_or_name: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	let path = Path::new(path_or_name);
	if is_indxdb_dump_dir(path) {
		let dump_file = if path.is_dir() {
			path.join("indxdb_dump.bin")
		} else {
			path.to_path_buf()
		};
		return read_dump(dump_file);
	}

	#[cfg(target_arch = "wasm32")]
	{
		wasm::read_all_from_browser(path_or_name).await
	}

	#[cfg(not(target_arch = "wasm32"))]
	{
		Err(Error::DatabaseNotFound(format!(
			"IndexedDB browser store {path_or_name} not found and native execution requires dump file"
		)))
	}
}
