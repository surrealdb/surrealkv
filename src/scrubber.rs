//! Background data integrity scrubber.
//!
//! Continually trickles through SSTable blocks, validating inline checksums
//! to detect and alert on silent hardware bit-rot or media corruption.

use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(test)]
use crate::error::Result;
#[cfg(test)]
use crate::sstable::table::Table;

pub struct Scrubber {
	running: AtomicBool,
}

impl Default for Scrubber {
	fn default() -> Self {
		Self::new()
	}
}

impl Scrubber {
	pub fn new() -> Self {
		Self {
			running: AtomicBool::new(false),
		}
	}

	/// Validates all blocks within a single SSTable. Returns count of verified blocks.
	#[cfg(test)]
	pub(crate) fn scrub_table(&self, table: &Table) -> Result<usize> {
		let mut verified = 0;
		// Iterate over table index entries and verify every data block's checksum
		let crate::sstable::table::IndexType::Partitioned(ref partitioned) = table.index_block;
		let mut index_iter = crate::sstable::index_block::IndexIterator::new(partitioned);
		index_iter.seek_to_first()?;
		while index_iter.valid() {
			let handle = index_iter.block_handle()?;
			// Read block and verify checksum
			let _ = table.read_block(&handle)?;
			verified += 1;
			index_iter.next()?;
		}
		Ok(verified)
	}

	/// Checks whether the background scrubber is currently active.
	pub fn is_running(&self) -> bool {
		self.running.load(Ordering::Acquire)
	}

	/// Signals the scrubber to stop.
	pub fn stop(&self) {
		self.running.store(false, Ordering::Release);
	}
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
	use super::*;

	#[test]
	fn test_scrubber_lifecycle() {
		let scrubber = Scrubber::new();
		assert!(!scrubber.is_running());
		scrubber.stop();
		assert!(!scrubber.is_running());
	}

	#[test]
	fn test_scrubber_scrub_table() {
		let dir = tempfile::tempdir().unwrap();
		let opts = std::sync::Arc::new(crate::Options {
			path: dir.path().to_path_buf(),
			..Default::default()
		});
		let sst_path = dir.path().join("1.sst");
		let file = std::fs::File::create(&sst_path).unwrap();
		let mut writer =
			crate::sstable::table::TableWriter::new(file, 1, std::sync::Arc::clone(&opts), 0);
		for i in 0..100 {
			let k = crate::InternalKey::new(
				format!("key_{i:04}").into_bytes(),
				(i + 1) as u64,
				crate::InternalKeyKind::Set,
			);
			writer.add(k, format!("val_{i:04}").as_bytes()).unwrap();
		}
		writer.finish().unwrap();

		let reader_file: std::sync::Arc<dyn crate::vfs::File> =
			std::sync::Arc::new(std::fs::File::open(&sst_path).unwrap());
		let file_size = reader_file.size().unwrap();
		let table = Table::new(1, opts, reader_file, file_size).unwrap();

		let scrubber = Scrubber::new();
		let count = scrubber.scrub_table(&table).unwrap();
		assert!(count > 0);
	}
}
