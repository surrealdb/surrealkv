//! Migration of databases written by older versions of SurrealKV, or by RocksDB, into the current
//! format.
//!
//! The current format is `TableFormat::LSMV2` with a version 2 manifest: flat internal keys
//! (`user_key ‖ trailer`) and a per-version timestamp kept in the `ValueLocation` of the value.
//! SurrealKV up to v0.21.4 wrote LSMV1, with the timestamp as a second 8 bytes of the internal key.
//!
//! The reading of the old formats is not done here. It lives in standalone crates that depend on
//! nothing of SurrealKV, `surrealkv-compat-v1` for LSMV1 and `surrealkv-compat-rocksdb` for
//! RocksDB, so that the format of SurrealKV can move on without taking the ability to read the old
//! ones with it. What is here is the writing side: [`legacy`] converts a database of LSMV1 when
//! `Tree::new` opens it, with every retained version, sequence number and timestamp, and
//! [`rocksdb`] does the same for a RocksDB database. Both fail only if the data itself is
//! unreadable, and then leave a state that opening the database again resumes.

pub(crate) mod legacy;
pub(crate) mod rocksdb;

use xxhash_rust::xxh3::Xxh3;

use crate::error::Result;
use crate::sstable::table::Table;
use crate::{InternalKeyKind, LSMIterator};

/// A hash of the entries that went through a migration, independent of how they were stored: what
/// was read is compared with what the migrated table gives back.
pub(crate) struct Digest {
	hash: Xxh3,
	entries: u64,
}

impl Digest {
	pub(crate) fn new() -> Self {
		Self {
			hash: Xxh3::new(),
			entries: 0,
		}
	}

	pub(crate) fn add(
		&mut self,
		user_key: &[u8],
		seq: u64,
		kind: InternalKeyKind,
		timestamp: Option<u64>,
		pointer: bool,
		payload: &[u8],
	) {
		self.entries += 1;
		self.hash.update(&(user_key.len() as u64).to_le_bytes());
		self.hash.update(user_key);
		self.hash.update(&seq.to_le_bytes());
		self.hash.update(&[kind as u8]);
		match timestamp {
			Some(t) => {
				self.hash.update(&[1]);
				self.hash.update(&t.to_le_bytes());
			}
			None => self.hash.update(&[0]),
		}
		self.hash.update(&[pointer as u8]);
		self.hash.update(&(payload.len() as u64).to_le_bytes());
		self.hash.update(payload);
	}

	pub(crate) fn finish(&self) -> (u64, u64) {
		(self.entries, self.hash.digest())
	}
}

/// An entry of a table of the current format, as stored: before its value is interpreted.
pub(crate) struct RawEntry {
	pub(crate) user_key: Vec<u8>,
	pub(crate) seq: u64,
	pub(crate) kind: InternalKeyKind,
	pub(crate) value: Vec<u8>,
}

/// Reads every entry of `table` in key order, through the iterator the database itself reads a
/// table with. This is how a migration reads back what it wrote before the manifest names it.
pub(crate) fn for_each_entry(
	table: &Table,
	f: &mut dyn FnMut(RawEntry) -> Result<()>,
) -> Result<()> {
	let mut entries = table.iter(None)?;
	let mut valid = entries.seek_first()?;
	while valid {
		let key = entries.key();
		f(RawEntry {
			user_key: key.user_key().to_vec(),
			seq: key.seq_num(),
			kind: key.kind(),
			value: entries.value_encoded()?.to_vec(),
		})?;
		valid = entries.next()?;
	}
	Ok(())
}
