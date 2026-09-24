//! Pure-Rust reader and K-way merging iterator for an entire RocksDB database directory.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::table::{RocksDbEntry, SstReader};

/// Checks whether a given directory appears to be a RocksDB database.
pub fn is_rocksdb_dir<P: AsRef<Path>>(path: P) -> bool {
	let path = path.as_ref();
	let current_file = path.join("CURRENT");
	if let Ok(content) = fs::read_to_string(current_file) {
		content.trim().starts_with("MANIFEST-")
	} else {
		false
	}
}

/// A reader for an entire RocksDB database directory.
pub struct RocksDbDatabase {
	path: PathBuf,
	sst_paths: Vec<PathBuf>,
	has_user_timestamps: bool,
}

impl RocksDbDatabase {
	/// Opens a RocksDB database directory for reading.
	pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
		let path = path.as_ref().to_path_buf();
		if !is_rocksdb_dir(&path) {
			return Err(Error::NotARocksDbDirectory);
		}

		let mut sst_paths = Vec::new();
		for entry in fs::read_dir(&path)? {
			let entry = entry?;
			let p = entry.path();
			if p.extension().and_then(|s| s.to_str()) == Some("sst") {
				sst_paths.push(p);
			}
		}

		// Sort SST paths for deterministic scanning
		sst_paths.sort();

		// Auto-detect if SurrealDB user-defined timestamps are present
		let has_user_timestamps = Self::detect_user_timestamps(&sst_paths);

		Ok(Self {
			path,
			sst_paths,
			has_user_timestamps,
		})
	}

	/// Inspects the first SSTable to detect if 8-byte timestamps are present.
	fn detect_user_timestamps(sst_paths: &[PathBuf]) -> bool {
		for sst in sst_paths {
			if let Ok(mut reader) = SstReader::open(sst, false) {
				for entry_res in reader.iter() {
					if let Ok(entry) = entry_res {
						// In SurrealDB, timestamps are monotonically increasing HLC or nanoseconds
						// which are non-zero u64 values at the end of the key.
						if entry.key.len() >= 8 {
							let ts = u64::from_le_bytes(
								entry.key[entry.key.len() - 8..].try_into().unwrap(),
							);
							// Non-zero timestamp in standard range indicates UDT
							if ts > 1_000_000 {
								return true;
							}
						}
					}
					break;
				}
			}
		}
		false
	}

	/// Returns all active, live (non-deleted) key-value records in strictly ascending
	/// lexicographical order, with all older versions dropped.
	pub fn iter_latest(&self) -> Result<DatabaseIter<'_>> {
		let mut readers = Vec::with_capacity(self.sst_paths.len());
		for p in &self.sst_paths {
			readers.push(SstReader::open(p, self.has_user_timestamps)?);
		}

		DatabaseIter::new(readers)
	}

	/// Returns the database directory path.
	pub fn path(&self) -> &Path {
		&self.path
	}

	/// Returns the list of discovered SSTable files.
	pub fn sst_files(&self) -> &[PathBuf] {
		&self.sst_paths
	}
}

/// Item tracked in the K-way merge priority queue.
struct MergeCursor<'a> {
	entry: RocksDbEntry,
	iter: crate::table::SstIter<'a>,
}

impl PartialEq for MergeCursor<'_> {
	fn eq(&self, other: &Self) -> bool {
		self.entry.key == other.entry.key && self.entry.seq_num == other.entry.seq_num
	}
}

impl Eq for MergeCursor<'_> {}

impl PartialOrd for MergeCursor<'_> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for MergeCursor<'_> {
	fn cmp(&self, other: &Self) -> Ordering {
		// Min-heap on key (ascending)
		match other.entry.key.cmp(&self.entry.key) {
			Ordering::Equal => {
				// For identical keys, higher sequence number / newer timestamp has higher priority
				self.entry.seq_num.cmp(&other.entry.seq_num)
			}
			other_ord => other_ord,
		}
	}
}

/// K-way merging iterator over multiple SSTables that collapses duplicate keys
/// down to only their latest version, dropping deletion tombstones.
pub struct DatabaseIter<'a> {
	heap: BinaryHeap<MergeCursor<'a>>,
	last_emitted_key: Option<Vec<u8>>,
}

impl<'a> DatabaseIter<'a> {
	fn new(readers: Vec<SstReader>) -> Result<Self> {
		let mut heap = BinaryHeap::new();

		let boxed_readers: Vec<Box<SstReader>> = readers.into_iter().map(Box::new).collect();
		let static_readers: &'a mut [Box<SstReader>] = Box::leak(boxed_readers.into_boxed_slice());

		for reader in static_readers.iter_mut() {
			let mut iter = reader.iter();
			if let Some(entry_res) = iter.next() {
				let entry = entry_res?;
				heap.push(MergeCursor {
					entry,
					iter,
				});
			}
		}

		Ok(Self {
			heap,
			last_emitted_key: None,
		})
	}
}

impl Iterator for DatabaseIter<'_> {
	type Item = Result<(Vec<u8>, Vec<u8>)>;

	fn next(&mut self) -> Option<Self::Item> {
		while let Some(mut cursor) = self.heap.pop() {
			let current_entry = cursor.entry;

			// Advance the stream and push back into heap if more entries exist
			if let Some(next_res) = cursor.iter.next() {
				match next_res {
					Ok(next_entry) => {
						cursor.entry = next_entry;
						self.heap.push(cursor);
					}
					Err(e) => return Some(Err(e)),
				}
			}

			// Check if we have already emitted a newer version of this key
			if let Some(ref last_key) = self.last_emitted_key {
				if last_key == &current_entry.key {
					// Duplicate older version: skip it!
					continue;
				}
			}

			// This is the latest version of this user key
			self.last_emitted_key = Some(current_entry.key.clone());

			// If the latest version is a tombstone, it was deleted! Do not emit.
			if current_entry.is_tombstone {
				continue;
			}

			return Some(Ok((current_entry.key, current_entry.value)));
		}

		None
	}
}
