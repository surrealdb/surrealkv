//! Offline and file-based dump parsing for legacy IndexedDB backups.

use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use crate::error::{Error, Result};

pub const INDXDB_DUMP_MAGIC: [u8; 8] = *b"INDXDB01";

/// Checks if a directory contains an exported IndexedDB binary dump.
pub fn is_indxdb_dump_dir<P: AsRef<Path>>(path: P) -> bool {
	let path = path.as_ref();
	if path.is_file() {
		return has_dump_magic(path);
	}
	if path.is_dir() {
		let dump_file = path.join("indxdb_dump.bin");
		if dump_file.exists() {
			return has_dump_magic(&dump_file);
		}
	}
	false
}

fn has_dump_magic(file_path: &Path) -> bool {
	if let Ok(mut f) = File::open(file_path) {
		let mut magic = [0u8; 8];
		if f.read_exact(&mut magic).is_ok() && magic == INDXDB_DUMP_MAGIC {
			return true;
		}
	}
	false
}

/// Serializes key-value pairs into a standardized binary dump format.
pub fn export_dump<P: AsRef<Path>>(file_path: P, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
	let mut file = File::create(file_path)?;
	file.write_all(&INDXDB_DUMP_MAGIC)?;

	let count = entries.len() as u32;
	file.write_all(&count.to_be_bytes())?;

	for (k, v) in entries {
		let k_len = k.len() as u32;
		file.write_all(&k_len.to_be_bytes())?;
		file.write_all(k)?;

		let v_len = v.len() as u32;
		file.write_all(&v_len.to_be_bytes())?;
		file.write_all(v)?;
	}

	file.flush()?;
	Ok(())
}

/// Reads all key-value entries from an IndexedDB binary dump file.
pub fn read_dump<P: AsRef<Path>>(file_path: P) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	let mut file = File::open(file_path)?;
	let mut magic = [0u8; 8];
	file.read_exact(&mut magic)?;

	if magic != INDXDB_DUMP_MAGIC {
		return Err(Error::CorruptDump("Invalid IndexedDB dump magic header".into()));
	}

	let mut count_buf = [0u8; 4];
	file.read_exact(&mut count_buf)?;
	let count = u32::from_be_bytes(count_buf) as usize;

	let mut entries = Vec::with_capacity(count);
	let mut len_buf = [0u8; 4];

	for _ in 0..count {
		file.read_exact(&mut len_buf)?;
		let k_len = u32::from_be_bytes(len_buf) as usize;
		let mut key = vec![0u8; k_len];
		file.read_exact(&mut key)?;

		file.read_exact(&mut len_buf)?;
		let v_len = u32::from_be_bytes(len_buf) as usize;
		let mut value = vec![0u8; v_len];
		file.read_exact(&mut value)?;

		entries.push((key, value));
	}

	Ok(entries)
}
