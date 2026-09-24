//! Pure-Rust and WebAssembly reader and migrator for legacy SurrealDB IndexedDB (`indxdb://`) stores.
//!
//! Enables automated, transparent migration of existing browser `indxdb://` data
//! into high-performance `file://` SurrealKV stores over OPFS without manual export/import scripts.

pub mod dump;
pub mod error;

#[cfg(target_arch = "wasm32")]
pub mod wasm;

#[cfg(test)]
mod tests;

use std::path::Path;

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
