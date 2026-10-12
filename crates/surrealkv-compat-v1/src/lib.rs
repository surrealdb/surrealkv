//! A standalone reader of SurrealKV databases written in the **LSMV1** format, which is every
//! release up to and including v0.21.4, for conversion to the current format (**LSMV2**, the one
//! SurrealKV 1.0 writes).
//!
//! It depends on nothing of SurrealKV itself: the decoders here are a frozen copy of what those
//! releases wrote, so that the format of SurrealKV can move on without taking the ability to read
//! the old one with it. It reads; the conversion is done by SurrealKV, which opens a database
//! with this crate, and the same holds for [`surrealkv-compat-rocksdb`].
//!
//! # What an LSMV1 database is
//!
//! ```text
//! <dir>/manifest/00000000000000000000.manifest   which tables are on which level, and the log
//! <dir>/sstables/<id:020>.sst                     tables; internal keys are user_key | trailer | timestamp
//! <dir>/wal/<id:020>.wal                          the log that no table covers yet
//! <dir>/vlog/<id:020>.vlog                        the value log (values larger than a threshold)
//! ```
//!
//! An internal key of LSMV1 ends in 16 bytes: the trailer (`seq << 8 | kind`, big endian) and the
//! timestamp of the write (big endian, nanoseconds, 0 if the database was not versioned). The
//! value of every entry but a hard delete or a soft delete, which have none, is an encoded value
//! location: `meta | version | bytes`,
//! where the bytes are the value itself or, if the pointer bit of `meta` is set, a pointer into the
//! value log, which this crate does not read and passes on untouched.
//!
//! [`surrealkv-compat-rocksdb`]: https://docs.rs/surrealkv-compat-rocksdb

mod batch;
mod block;
mod error;
mod footer;
mod manifest;
mod table;
mod value;
mod varint;
mod wal;

use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

pub use error::{Error, Result};

/// The kind of an entry: the low byte of the trailer of its key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Kind {
	/// A hard delete: a barrier that hides every older version. Its value is empty.
	Delete = 0,
	/// A delete that is a version of its own, at its timestamp.
	SoftDelete = 1,
	Set = 2,
	Merge = 3,
	LogData = 4,
	/// A range delete: the key is its start, the value its end key (raw, not a value location).
	RangeDelete = 5,
	/// A write that replaces every older version of its key.
	Replace = 6,
}

impl Kind {
	pub fn from_byte(byte: u8) -> Option<Self> {
		match byte {
			0 => Some(Self::Delete),
			1 => Some(Self::SoftDelete),
			2 => Some(Self::Set),
			3 => Some(Self::Merge),
			4 => Some(Self::LogData),
			5 => Some(Self::RangeDelete),
			6 => Some(Self::Replace),
			_ => None,
		}
	}
}

/// One entry of a table, as stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
	pub user_key: Vec<u8>,
	pub seq: u64,
	pub kind: Kind,
	/// The timestamp that is the last 8 bytes of the key: nanoseconds, 0 if the database was not
	/// versioned when it was written.
	pub timestamp: u64,
	/// The value as stored: an encoded value location, empty for a hard delete, and for a range
	/// delete the end key. See [`decode_value`].
	pub value: Vec<u8>,
}

/// What an encoded value location holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Value {
	/// Whether `payload` is an encoded pointer into the value log and not the value.
	pub pointer: bool,
	/// The value, or the encoded pointer, byte for byte as stored.
	pub payload: Vec<u8>,
}

/// Decodes the value of an entry of kind `kind`. A hard delete has none (`Ok(None)`); a range
/// delete's value is not a value location and is refused; every other kind has one, except that
/// a soft delete was written by v0.20.0 to v0.21.4 with no value at all (empty bytes), which is
/// `Ok(None)` too. A pointer must be the 25 bytes of one.
pub fn decode_value(kind: Kind, stored: &[u8]) -> Result<Option<Value>> {
	value::decode_value(kind, stored)
}

/// Which format a table file is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableLayout {
	/// LSMV1: format byte 1 in the footer and 16-byte key suffixes. [`TableReader`] reads it.
	Lsmv1,
	/// Anything else that is a table of SurrealKV: written in the current format (LSMV2), or by a
	/// development build of it. Not read by this crate.
	Other,
}

/// Tells which format the table file at `path` is in from its footer and meta-index block, which
/// is all it reads. A footer that says LSMV2 (or that its blocks are sealed) is
/// [`TableLayout::Other`] without a block being read; a file that is not a table is
/// [`Error::Corrupted`].
pub fn table_layout(path: &Path) -> Result<TableLayout> {
	table::table_layout(path)
}

/// A table of an LSMV1 database.
pub struct TableReader {
	table: table::Table,
}

impl TableReader {
	/// Opens the table at `path`. Refuses a table that is not LSMV1 with [`Error::Unsupported`].
	pub fn open(path: &Path) -> Result<Self> {
		Ok(Self {
			table: table::Table::open(path)?,
		})
	}

	/// The entries of the table, in the order they are stored in: user key ascending and then
	/// sequence number descending. Checksums are verified; corruption is an error.
	pub fn entries(&self) -> Result<TableEntries<'_>> {
		Ok(TableEntries {
			entries: self.table.entries()?,
		})
	}
}

/// The entries of a table, one at a time, without reading the whole table. After an error there
/// are no more.
pub struct TableEntries<'a> {
	entries: table::Entries<'a>,
}

impl Iterator for TableEntries<'_> {
	type Item = Result<Entry>;

	fn next(&mut self) -> Option<Self::Item> {
		self.entries.next()
	}
}

/// The manifest of an LSMV1 database: which tables are on which level.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
	/// The format version of the manifest: 1 for LSMV1.
	pub version: u16,
	/// The ids of the tables of each level, level 0 first, in the order the manifest lists them.
	pub level_data: Vec<Vec<u64>>,
	/// The id the next table gets.
	pub next_table_id: u64,
	/// The sequence number of the newest write that a table holds.
	pub last_sequence: u64,
	/// The first log segment that no table covers: segments below it are flushed.
	pub log_number: u64,
}

/// Decodes the bytes of a manifest file.
pub fn read_manifest(bytes: &[u8]) -> Result<Manifest> {
	manifest::read_manifest(bytes)
}

/// A log segment of the write-ahead log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalSegment {
	pub id: u64,
	pub path: PathBuf,
}

/// The segments in `dir` (`<id:020>.wal`), oldest first. A directory that is not there has none.
pub fn wal_segments(dir: &Path) -> Result<Vec<WalSegment>> {
	wal::segments(dir)
}

/// A write that was logged: a batch of entries with consecutive sequence numbers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalBatch {
	/// The sequence number of the first entry; entry `i` has `starting_seq + i`.
	pub starting_seq: u64,
	pub entries: Vec<WalEntry>,
}

/// An entry of a logged batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalEntry {
	pub kind: Kind,
	pub key: Vec<u8>,
	/// The value as logged: an encoded value location (a pointer if the version of SurrealKV that
	/// wrote it logged the pointer of a value it had already written to the value log), `None`
	/// for a hard delete.
	pub value: Option<Vec<u8>>,
	/// The timestamp of the write: nanoseconds, 0 if the database was not versioned.
	pub timestamp: u64,
}

/// What reading a log segment found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalRead {
	/// The batches that were read intact, in order.
	pub batches: Vec<WalBatch>,
	/// Set if a record was damaged, with what was wrong: the records before it are in `batches`
	/// and the rest of the segment was not read, which is what SurrealKV did to a damaged segment.
	/// A segment that ends in the middle of a record, which is how a crash leaves it, is not
	/// damage.
	pub damage: Option<String>,
}

/// Reads the log segment at `path` as the versions of SurrealKV that wrote LSMV1 read theirs: the
/// batches up to the first damaged record, or the end of the segment.
pub fn read_wal_segment(path: &Path) -> Result<WalRead> {
	wal::read_segment(path)
}

/// The paths of an LSMV1 database, which are the layout the crate documents at its top.
#[derive(Clone, Debug)]
pub struct Layout {
	root: PathBuf,
}

impl Layout {
	pub fn new(root: impl Into<PathBuf>) -> Self {
		Self {
			root: root.into(),
		}
	}

	pub fn root(&self) -> &Path {
		&self.root
	}

	pub fn manifest_path(&self) -> PathBuf {
		self.root.join("manifest").join("00000000000000000000.manifest")
	}

	pub fn sstable_dir(&self) -> PathBuf {
		self.root.join("sstables")
	}

	pub fn sstable_path(&self, id: u64) -> PathBuf {
		self.sstable_dir().join(format!("{id:020}.sst"))
	}

	pub fn wal_dir(&self) -> PathBuf {
		self.root.join("wal")
	}

	pub fn vlog_dir(&self) -> PathBuf {
		self.root.join("vlog")
	}

	pub fn vlog_path(&self, id: u64) -> PathBuf {
		self.vlog_dir().join(format!("{id:020}.vlog"))
	}
}

/// Whether `dir` holds an LSMV1 database: a manifest of version 1.
pub fn is_lsmv1_dir(dir: &Path) -> bool {
	let mut version = [0u8; 2];
	File::open(Layout::new(dir).manifest_path())
		.and_then(|mut file| file.read_exact(&mut version))
		.is_ok()
		&& u16::from_be_bytes(version) == 1
}
