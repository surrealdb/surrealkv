//! Reading a table of an LSMV1 database: a frozen copy of the read side of v0.21.4's
//! `sstable/table.rs` and `sstable/index_block.rs`.
//!
//! ```text
//! [data block] ... [index partition] ... [top-level index] [meta-index] [footer]
//! ```
//!
//! The top-level index lists the index partitions, each partition lists data blocks, and the data
//! blocks hold the entries: internal keys (`user_key | trailer | timestamp`, see the crate
//! documentation) and values, in the order of the table. Filter blocks are ignored.
//!
//! Which of the two key layouts a table has is not in the footer of the files that SurrealKV wrote
//! between the removal of the timestamp from the key and the introduction of LSMV2, which say 1
//! like the releases do. The table properties tell it: they are stored in the meta-index block
//! under the internal key of the literal user key `meta`, whose length is therefore 4 plus the
//! length of the suffix of a key: 16 for LSMV1, 8 for the flat layout.

use std::collections::VecDeque;
use std::fs::File;
use std::path::Path;
use std::sync::Mutex;

use crate::block::{read_block, BlockEntries, Handle};
use crate::footer::{read_footer, Footer};
use crate::{Entry, Error, Kind, Result, TableLayout};

const META_PROPERTIES_KEY: &[u8] = b"meta";
const LSMV1_SUFFIX_LEN: usize = 16;
const FLAT_SUFFIX_LEN: usize = 8;

fn open_file(path: &Path) -> Result<(File, u64)> {
	let file = File::open(path)?;
	let len = file.metadata()?.len();
	Ok((file, len))
}

fn layout_of(file: &mut File, len: u64) -> Result<(Footer, TableLayout)> {
	let footer = read_footer(file, len)?;
	if !footer.is_lsmv1() {
		return Ok((footer, TableLayout::Other));
	}
	let meta_index = BlockEntries::new(read_block(file, len, footer.meta_index)?)?;
	let layout = layout_of_meta_index(meta_index)?;
	Ok((footer, layout))
}

fn layout_of_meta_index(mut meta_index: BlockEntries) -> Result<TableLayout> {
	while meta_index.advance()? {
		let key = meta_index.key();
		if key.starts_with(META_PROPERTIES_KEY) {
			return match key.len() - META_PROPERTIES_KEY.len() {
				LSMV1_SUFFIX_LEN => Ok(TableLayout::Lsmv1),
				FLAT_SUFFIX_LEN => Ok(TableLayout::Other),
				other => Err(Error::Corrupted(format!(
					"the table properties have a key with a {other}-byte suffix, expected \
					 {LSMV1_SUFFIX_LEN} or {FLAT_SUFFIX_LEN}"
				))),
			};
		}
	}
	Err(Error::Corrupted("the table has no properties".into()))
}

pub(crate) fn table_layout(path: &Path) -> Result<TableLayout> {
	let (mut file, len) = open_file(path)?;
	Ok(layout_of(&mut file, len)?.1)
}

/// An open table of LSMV1.
pub(crate) struct Table {
	file: Mutex<File>,
	len: u64,
	/// The index partitions, in order.
	partitions: Vec<Handle>,
}

impl Table {
	pub(crate) fn open(path: &Path) -> Result<Self> {
		let (mut file, len) = open_file(path)?;
		let (footer, layout) = layout_of(&mut file, len)?;
		if layout != TableLayout::Lsmv1 {
			return Err(Error::Unsupported(format!(
				"{} is not a table of LSMV1 (format byte {}, checksum type {})",
				path.display(),
				footer.format,
				footer.checksum
			)));
		}
		let top = BlockEntries::new(read_block(&mut file, len, footer.index)?)?;
		let partitions = handles(top)?.into_iter().collect();
		Ok(Self {
			file: Mutex::new(file),
			len,
			partitions,
		})
	}

	pub(crate) fn entries(&self) -> Result<Entries<'_>> {
		Ok(Entries {
			table: self,
			next_partition: 0,
			data_blocks: VecDeque::new(),
			block: None,
			done: false,
		})
	}

	fn read(&self, handle: Handle) -> Result<BlockEntries> {
		let mut file = self.file.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
		BlockEntries::new(read_block(&mut file, self.len, handle)?)
	}
}

/// The block handles that are the values of an index block.
fn handles(mut index: BlockEntries) -> Result<VecDeque<Handle>> {
	let mut out = VecDeque::new();
	while index.advance()? {
		let (handle, _) = Handle::decode(index.value())
			.ok_or_else(|| Error::Corrupted("an index entry holds a bad block handle".into()))?;
		out.push_back(handle);
	}
	Ok(out)
}

/// The entries of a table, one at a time, without reading the whole table.
pub(crate) struct Entries<'a> {
	table: &'a Table,
	next_partition: usize,
	/// The data blocks of the partition being read that have not been read yet.
	data_blocks: VecDeque<Handle>,
	block: Option<BlockEntries>,
	/// After the end or the first error.
	done: bool,
}

impl Entries<'_> {
	fn next_entry(&mut self) -> Result<Option<Entry>> {
		loop {
			if let Some(block) = &mut self.block {
				if block.advance()? {
					return decode_entry(block.key(), block.value()).map(Some);
				}
				self.block = None;
			}
			if let Some(handle) = self.data_blocks.pop_front() {
				self.block = Some(self.table.read(handle)?);
				continue;
			}
			let Some(partition) = self.table.partitions.get(self.next_partition) else {
				return Ok(None);
			};
			self.next_partition += 1;
			self.data_blocks = handles(self.table.read(*partition)?)?;
		}
	}
}

impl Iterator for Entries<'_> {
	type Item = Result<Entry>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.done {
			return None;
		}
		match self.next_entry() {
			Ok(Some(entry)) => Some(Ok(entry)),
			Ok(None) => {
				self.done = true;
				None
			}
			Err(e) => {
				self.done = true;
				Some(Err(e))
			}
		}
	}
}

fn decode_entry(key: &[u8], value: &[u8]) -> Result<Entry> {
	if key.len() < LSMV1_SUFFIX_LEN {
		return Err(Error::Corrupted(format!("a table holds a key of {} bytes", key.len())));
	}
	let n = key.len() - LSMV1_SUFFIX_LEN;
	let trailer = u64::from_be_bytes(key[n..n + 8].try_into().expect("8 bytes"));
	let timestamp = u64::from_be_bytes(key[n + 8..].try_into().expect("8 bytes"));
	let kind = Kind::from_byte(trailer as u8).ok_or_else(|| {
		Error::Corrupted(format!("a table holds an entry of the unknown kind {}", trailer as u8))
	})?;
	Ok(Entry {
		user_key: key[..n].to_vec(),
		seq: trailer >> 8,
		kind,
		timestamp,
		value: value.to_vec(),
	})
}

#[cfg(test)]
mod tests {
	use std::fs;

	use super::*;
	use crate::block::testing::{internal_key, table};

	/// The entries of a table in the order it stores them: key ascending, newest first.
	fn entries(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
		let mut out = Vec::new();
		for i in 0..n {
			let key = format!("key{:05}", i / 3);
			let seq = 1000 - i as u64;
			out.push((
				internal_key(key.as_bytes(), seq, 2, 1_000_000 + i as u64),
				vec![0, 1, i as u8],
			));
		}
		out
	}

	fn write(dir: &Path, bytes: &[u8]) -> std::path::PathBuf {
		let path = dir.join("t.sst");
		fs::write(&path, bytes).unwrap();
		path
	}

	#[test]
	fn reads_every_entry_of_tables_with_many_blocks_and_partitions() {
		let dir = tempfile::tempdir().unwrap();
		for snappy in [false, true] {
			for (per_block, per_partition) in [(1, 1), (4, 3), (10, 2), (1000, 1000)] {
				let wanted = entries(250);
				let bytes = table(&wanted, per_block, per_partition, snappy, 1, 16);
				let path = write(dir.path(), &bytes);
				assert_eq!(table_layout(&path).unwrap(), TableLayout::Lsmv1);
				let table = Table::open(&path).unwrap();
				let got: Vec<Entry> = table.entries().unwrap().map(|e| e.unwrap()).collect();
				assert_eq!(got.len(), wanted.len());
				for (entry, (key, value)) in got.iter().zip(&wanted) {
					assert_eq!(entry.user_key, key[..key.len() - 16]);
					assert_eq!(entry.value, *value);
					assert_eq!(entry.kind, Kind::Set);
				}
				assert_eq!(got[0].seq, 1000);
				assert_eq!(got[0].timestamp, 1_000_000);
				assert_eq!(got[249].seq, 751);
			}
		}
	}

	#[test]
	fn an_empty_table_has_no_entries() {
		let dir = tempfile::tempdir().unwrap();
		let path = write(dir.path(), &table(&[], 4, 4, false, 1, 16));
		let table = Table::open(&path).unwrap();
		assert_eq!(table.entries().unwrap().count(), 0);
	}

	#[test]
	fn the_properties_key_tells_the_layout_when_the_footer_cannot() {
		let dir = tempfile::tempdir().unwrap();
		// Format 1 with keys of the flat layout: written by a development build of SurrealKV.
		let flat = table(&entries(10), 4, 4, false, 1, 8);
		let path = write(dir.path(), &flat);
		assert_eq!(table_layout(&path).unwrap(), TableLayout::Other);
		assert!(matches!(Table::open(&path), Err(Error::Unsupported(_))));
		// Format 2: the footer says so, and nothing else is read.
		let current = table(&entries(10), 4, 4, false, 2, 8);
		let path = write(dir.path(), &current);
		assert_eq!(table_layout(&path).unwrap(), TableLayout::Other);
		assert!(matches!(Table::open(&path), Err(Error::Unsupported(_))));
		// A suffix of any other length is no table of SurrealKV.
		let odd = table(&entries(10), 4, 4, false, 1, 12);
		let path = write(dir.path(), &odd);
		assert!(matches!(table_layout(&path), Err(Error::Corrupted(_))));
	}

	#[test]
	fn corruption_is_an_error_and_never_a_panic() {
		let dir = tempfile::tempdir().unwrap();
		for snappy in [false, true] {
			let good = table(&entries(60), 5, 3, snappy, 1, 16);
			let want = Table::open(&write(dir.path(), &good)).unwrap().entries().unwrap().count();
			assert_eq!(want, 60);
			// Every 23rd byte of the file and all of its end, flipped, is found by a checksum, a
			// length or the magic: a table is read to its end or fails with an error.
			for at in (0..good.len()).filter(|at| at % 23 == 0 || *at >= good.len() - 60) {
				let mut bad = good.clone();
				bad[at] ^= 0x5a;
				let path = write(dir.path(), &bad);
				if let Ok(table) = Table::open(&path) {
					let results: Vec<_> = table.entries().unwrap().collect();
					let failed = results.iter().filter(|entry| entry.is_err()).count();
					assert!(failed <= 1, "an error ends the entries");
					if failed == 0 {
						// Only the padding of the footer is not covered by anything.
						assert!(
							at >= good.len() - 50,
							"flipping byte {at} of {} went unnoticed",
							good.len()
						);
					} else {
						assert!(results.last().unwrap().is_err());
					}
				}
			}
			// A cut table.
			for cut in [0, 10, 49, 50, good.len() / 2, good.len() - 1] {
				let path = write(dir.path(), &good[..cut]);
				assert!(Table::open(&path).is_err(), "cut at {cut}");
			}
		}
	}
}
