//! Whole-database scanning and latest-version consolidation for SurrealKV V1.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::error::Result;
use crate::footer::{V1_FOOTER_TOTAL_LEN, V1_MAGIC_FOOTER};
use crate::table::V1TableReader;

/// Returns true if the directory appears to be a SurrealKV V1 database.
pub fn is_v1_dir<P: AsRef<Path>>(path: P) -> bool {
	let path = path.as_ref();
	if !path.is_dir() {
		return false;
	}

	let entries = match fs::read_dir(path) {
		Ok(e) => e,
		Err(_) => return false,
	};

	for entry in entries.flatten() {
		let p = entry.path();
		if p.extension().is_some_and(|ext| ext == "sst") {
			// Check if file has V1 magic footer
			if let Ok(metadata) = fs::metadata(&p) {
				if metadata.len() >= V1_FOOTER_TOTAL_LEN as u64 {
					if let Ok(mut f) = fs::File::open(&p) {
						use std::io::{Read, Seek, SeekFrom};
						if f.seek(SeekFrom::End(-(V1_FOOTER_TOTAL_LEN as i64))).is_ok() {
							let mut buf = [0u8; V1_FOOTER_TOTAL_LEN];
							if f.read_exact(&mut buf).is_ok() {
								let magic = &buf[buf.len() - V1_MAGIC_FOOTER.len()..];
								if magic == V1_MAGIC_FOOTER {
									return true;
								}
							}
						}
					}
				}
			}
		}
	}

	false
}

#[derive(Debug, Clone)]
struct KeyState {
	timestamp: u64,
	seq_num: u64,
	value: Option<Vec<u8>>,
}

/// Reads all live latest key-value pairs across all SSTables in a SurrealKV V1 database directory.
/// Older historical versions and deleted tombstones are purged, leaving only the newest live values.
pub fn read_all_latest<P: AsRef<Path>>(dir: P) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	let dir = dir.as_ref();
	let mut sst_files = Vec::new();

	for entry in fs::read_dir(dir)?.flatten() {
		let p = entry.path();
		if p.extension().is_some_and(|ext| ext == "sst") {
			sst_files.push(p);
		}
	}

	// Sort files deterministically
	sst_files.sort();

	let mut latest_map: BTreeMap<Vec<u8>, KeyState> = BTreeMap::new();

	for file_path in sst_files {
		let mut reader = match V1TableReader::open(&file_path) {
			Ok(r) => r,
			Err(_) => continue,
		};

		if let Ok(records) = reader.read_all_records() {
			for record in records {
				let should_update = match latest_map.get(&record.user_key) {
					None => true,
					Some(existing) => {
						(record.timestamp, record.seq_num) > (existing.timestamp, existing.seq_num)
					}
				};

				if should_update {
					let val = if record.is_tombstone {
						None
					} else {
						Some(record.value)
					};
					latest_map.insert(
						record.user_key,
						KeyState {
							timestamp: record.timestamp,
							seq_num: record.seq_num,
							value: val,
						},
					);
				}
			}
		}
	}

	let live_records =
		latest_map.into_iter().filter_map(|(k, state)| state.value.map(|v| (k, v))).collect();

	Ok(live_records)
}
