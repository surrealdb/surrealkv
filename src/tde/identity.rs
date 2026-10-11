//! What identifies a file to its cipher: the database, the kind of file, its id and its key.

use std::fmt;

use super::random::fill_nonzero;
use super::suite::CipherSuite;
use crate::error::EncryptionError;

/// Length in bytes of a database id.
pub const DB_ID_LEN: usize = 16;

/// Identity of a database, drawn at random when it is created.
///
/// It is not secret. It is stored in the header of every encrypted file and bound into every
/// file key and every authenticated block, so a file moved to another database fails to open
/// even when both databases share a key.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct DbId([u8; DB_ID_LEN]);

impl DbId {
	/// A database id with the given bytes, as read back from a manifest or a file header.
	pub const fn from_bytes(bytes: [u8; DB_ID_LEN]) -> Self {
		Self(bytes)
	}

	/// The bytes of the id.
	pub const fn as_bytes(&self) -> &[u8; DB_ID_LEN] {
		&self.0
	}

	/// A new random, non-zero id from the operating system's generator.
	pub fn generate() -> Result<Self, EncryptionError> {
		let mut bytes = [0u8; DB_ID_LEN];
		fill_nonzero(&mut bytes)?;
		Ok(Self(bytes))
	}
}

impl fmt::Display for DbId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
	}
}

impl fmt::Debug for DbId {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "DbId({self})")
	}
}

/// The kind of file a key and a header belong to. Part of every file key and every block's
/// authenticated data, so a block cannot be moved between kinds of file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
#[repr(u8)]
pub enum FileKind {
	/// A sorted string table.
	Sst = 1,
	/// A value-log file.
	Vlog = 2,
	/// A write-ahead log segment.
	Wal = 3,
	/// The manifest.
	Manifest = 4,
}

impl FileKind {
	/// The id the kind is stored under.
	pub const fn id(self) -> u8 {
		self as u8
	}

	/// Parses a stored id. An id that was never assigned is `MalformedHeader`, because the only
	/// place a kind is read from is a file header.
	pub const fn from_id(id: u8) -> Result<Self, EncryptionError> {
		match id {
			1 => Ok(Self::Sst),
			2 => Ok(Self::Vlog),
			3 => Ok(Self::Wal),
			4 => Ok(Self::Manifest),
			_ => Err(EncryptionError::MalformedHeader),
		}
	}
}

/// Everything about a file that its key and its sealed blocks are bound to.
///
/// All of it is public: it is stored in the clear in the file header and rebuilt by the reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct FileIdentity {
	/// The suite the file's blocks are sealed with.
	pub suite: CipherSuite,
	/// What kind of file it is.
	pub file_kind: FileKind,
	/// The key id of the root its key derives from.
	pub key_id: u32,
	/// The id of the file: the table id, the vlog file id, the WAL segment id.
	pub file_id: u64,
	/// The database it belongs to.
	pub db_id: DbId,
}

impl FileIdentity {
	/// The identity of a file.
	pub const fn new(
		suite: CipherSuite,
		file_kind: FileKind,
		key_id: u32,
		file_id: u64,
		db_id: DbId,
	) -> Self {
		Self {
			suite,
			file_kind,
			key_id,
			file_id,
			db_id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn file_kind_ids_are_frozen_and_round_trip() {
		let kinds = [FileKind::Sst, FileKind::Vlog, FileKind::Wal, FileKind::Manifest];
		for (expected, kind) in (1u8..).zip(kinds) {
			assert_eq!(kind.id(), expected);
			assert_eq!(FileKind::from_id(expected).unwrap(), kind);
		}
		for id in [0u8, 5, 6, 127, 255] {
			assert_eq!(FileKind::from_id(id).unwrap_err(), EncryptionError::MalformedHeader);
		}
	}

	#[test]
	fn db_ids_are_random_non_zero_and_unique() {
		let first = DbId::generate().unwrap();
		let second = DbId::generate().unwrap();
		assert_ne!(first, DbId::from_bytes([0; DB_ID_LEN]));
		assert_ne!(first, second);
	}

	#[test]
	fn a_db_id_keeps_its_bytes_and_prints_them_in_hex() {
		let bytes: [u8; DB_ID_LEN] = std::array::from_fn(|i| 0xA0 + i as u8);
		let id = DbId::from_bytes(bytes);
		assert_eq!(id.as_bytes(), &bytes);
		assert_eq!(id.to_string(), "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf");
		assert_eq!(format!("{id:?}"), "DbId(a0a1a2a3a4a5a6a7a8a9aaabacadaeaf)");
	}
}
