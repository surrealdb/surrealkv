//! The MANIFEST: a log of version edits that, replayed, gives the live files of the database, the
//! first write-ahead log that may hold unflushed data, and the settings that decide how its keys
//! are read (the comparator, whether timestamps are stored).

use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::coding::{length_prefixed, varint32, varint64};
use crate::error::{corrupt, unsupported, Result};
use crate::log::{End, LogReader, RecordKind};

// Version edit tags (db/version_edit.h).
const COMPARATOR: u32 = 1;
const LOG_NUMBER: u32 = 2;
const NEXT_FILE_NUMBER: u32 = 3;
const LAST_SEQUENCE: u32 = 4;
const COMPACT_CURSOR: u32 = 5;
const DELETED_FILE: u32 = 6;
const NEW_FILE: u32 = 7;
const PREV_LOG_NUMBER: u32 = 9;
const MIN_LOG_NUMBER_TO_KEEP: u32 = 10;
const NEW_FILE2: u32 = 100;
const NEW_FILE3: u32 = 102;
const NEW_FILE4: u32 = 103;
const COLUMN_FAMILY: u32 = 200;
const COLUMN_FAMILY_ADD: u32 = 201;
const COLUMN_FAMILY_DROP: u32 = 202;
const MAX_COLUMN_FAMILY: u32 = 203;
const IN_ATOMIC_GROUP: u32 = 300;
const BLOB_FILE_ADDITION: u32 = 400;
const BLOB_FILE_GARBAGE: u32 = 401;
const DB_ID: u32 = 8193;
const BLOB_FILE_ADDITION_DEPRECATED: u32 = 8194;
const BLOB_FILE_GARBAGE_DEPRECATED: u32 = 8195;
const WAL_ADDITION: u32 = 8196;
const WAL_DELETION: u32 = 8197;
const FULL_HISTORY_TS_LOW: u32 = 8198;
const WAL_ADDITION2: u32 = 8199;
const WAL_DELETION2: u32 = 8200;
const PERSIST_USER_DEFINED_TIMESTAMPS: u32 = 8201;
const SUBCOMPACTION_PROGRESS: u32 = 8202;
/// Tags with this bit that a reader does not know carry a length-prefixed payload and are skipped.
const TAG_SAFE_IGNORE_MASK: u32 = 1 << 13;

// Custom tags of a `NewFile4` record.
const CUSTOM_TERMINATE: u32 = 1;
/// Custom tags with this bit that a reader does not know make the file unreadable.
const CUSTOM_NON_SAFE_IGNORE_MASK: u32 = 1 << 6;

/// A table the manifest lists.
#[derive(Debug, Clone)]
pub struct TableFile {
	pub number: u64,
	pub level: u32,
	pub size: u64,
	/// The smallest internal key of the table, which orders the tables of a level.
	pub smallest: Vec<u8>,
	pub smallest_seq: u64,
	pub largest_seq: u64,
}

/// What replaying the MANIFEST gives.
#[derive(Debug, Default, Clone)]
pub struct ManifestState {
	pub comparator: Option<String>,
	/// The first write-ahead log that may hold data not yet in a table.
	pub log_number: u64,
	pub last_sequence: u64,
	pub next_file_number: u64,
	/// The live tables, by file number.
	pub tables: BTreeMap<u64, TableFile>,
	/// Live blob files and whether anything in the MANIFEST is known of them.
	pub blob_files: BTreeMap<u64, ()>,
	/// 8 bytes little-endian, if history was ever trimmed.
	pub full_history_ts_low: Option<Vec<u8>>,
	/// Whether user-defined timestamps are stored in the tables. True unless a tag says otherwise.
	pub persist_user_defined_timestamps: bool,
}

#[derive(Default)]
struct Edit {
	column_family: u32,
	comparator: Option<String>,
	log_number: Option<u64>,
	next_file_number: Option<u64>,
	last_sequence: Option<u64>,
	deleted: Vec<(u32, u64)>,
	added: Vec<(u32, TableFile)>,
	blob_added: Vec<u64>,
	full_history_ts_low: Option<Vec<u8>>,
	persist_udt: Option<bool>,
	added_column_family: Option<String>,
	dropped_column_family: bool,
}

fn decode_edit(payload: &[u8]) -> Result<Edit> {
	let mut edit = Edit::default();
	let mut pos = 0;
	while pos < payload.len() {
		let tag = varint32(payload, &mut pos)?;
		match tag {
			COMPARATOR => {
				edit.comparator =
					Some(String::from_utf8_lossy(length_prefixed(payload, &mut pos)?).into_owned());
			}
			LOG_NUMBER => edit.log_number = Some(varint64(payload, &mut pos)?),
			NEXT_FILE_NUMBER => edit.next_file_number = Some(varint64(payload, &mut pos)?),
			LAST_SEQUENCE => edit.last_sequence = Some(varint64(payload, &mut pos)?),
			COMPACT_CURSOR => {
				varint32(payload, &mut pos)?;
				length_prefixed(payload, &mut pos)?;
			}
			DELETED_FILE => {
				let level = varint32(payload, &mut pos)?;
				let number = varint64(payload, &mut pos)?;
				edit.deleted.push((level, number));
			}
			NEW_FILE => {
				let level = varint32(payload, &mut pos)?;
				let number = varint64(payload, &mut pos)?;
				let size = varint64(payload, &mut pos)?;
				let smallest = length_prefixed(payload, &mut pos)?.to_vec();
				length_prefixed(payload, &mut pos)?;
				edit.added.push((
					level,
					TableFile {
						number,
						level,
						size,
						smallest,
						smallest_seq: 0,
						largest_seq: 0,
					},
				));
			}
			NEW_FILE2 | NEW_FILE3 | NEW_FILE4 => {
				let level = varint32(payload, &mut pos)?;
				let number = varint64(payload, &mut pos)?;
				if tag == NEW_FILE3 {
					// The path id of the table, which a database with one path always has as 0.
					varint32(payload, &mut pos)?;
				}
				let size = varint64(payload, &mut pos)?;
				let smallest = length_prefixed(payload, &mut pos)?.to_vec();
				length_prefixed(payload, &mut pos)?;
				let smallest_seq = varint64(payload, &mut pos)?;
				let largest_seq = varint64(payload, &mut pos)?;
				if tag == NEW_FILE4 {
					loop {
						let custom = varint32(payload, &mut pos)?;
						if custom == CUSTOM_TERMINATE {
							break;
						}
						let field = length_prefixed(payload, &mut pos)?;
						// 65 is the path id (1 byte); the others are the creation time, checksum,
						// timestamps range, unique id, ... none of which change what is read.
						if custom & CUSTOM_NON_SAFE_IGNORE_MASK != 0 && custom != 65 {
							return Err(unsupported(format!(
								"a table record has a custom field {custom} that this reader does not know"
							)));
						}
						if custom == 16 && field.first().copied() == Some(0) {
							edit.persist_udt = Some(false);
						}
					}
				}
				edit.added.push((
					level,
					TableFile {
						number,
						level,
						size,
						smallest,
						smallest_seq,
						largest_seq,
					},
				));
			}
			PREV_LOG_NUMBER | MIN_LOG_NUMBER_TO_KEEP => {
				varint64(payload, &mut pos)?;
			}
			COLUMN_FAMILY => edit.column_family = varint32(payload, &mut pos)?,
			COLUMN_FAMILY_ADD => {
				edit.added_column_family =
					Some(String::from_utf8_lossy(length_prefixed(payload, &mut pos)?).into_owned());
			}
			COLUMN_FAMILY_DROP => edit.dropped_column_family = true,
			MAX_COLUMN_FAMILY | IN_ATOMIC_GROUP => {
				varint32(payload, &mut pos)?;
			}
			BLOB_FILE_ADDITION | BLOB_FILE_ADDITION_DEPRECATED => {
				let number = varint64(payload, &mut pos)?;
				varint64(payload, &mut pos)?; // total blob count
				varint64(payload, &mut pos)?; // total blob bytes
				length_prefixed(payload, &mut pos)?; // checksum method
				length_prefixed(payload, &mut pos)?; // checksum value
				edit.blob_added.push(number);
				skip_custom_fields(payload, &mut pos)?;
			}
			BLOB_FILE_GARBAGE | BLOB_FILE_GARBAGE_DEPRECATED => {
				varint64(payload, &mut pos)?;
				varint64(payload, &mut pos)?;
				varint64(payload, &mut pos)?;
				skip_custom_fields(payload, &mut pos)?;
			}
			DB_ID => {
				length_prefixed(payload, &mut pos)?;
			}
			WAL_ADDITION => {
				// The log number, then fields up to a terminator: 2 is the synced size.
				varint64(payload, &mut pos)?;
				loop {
					match varint32(payload, &mut pos)? {
						1 => break,
						2 => {
							varint64(payload, &mut pos)?;
						}
						other => {
							return Err(unsupported(format!(
								"a WAL addition record has a field {other} that this reader does not know"
							)))
						}
					}
				}
			}
			WAL_DELETION => {
				varint64(payload, &mut pos)?;
			}
			WAL_ADDITION2 | WAL_DELETION2 => {
				length_prefixed(payload, &mut pos)?;
			}
			FULL_HISTORY_TS_LOW => {
				edit.full_history_ts_low = Some(length_prefixed(payload, &mut pos)?.to_vec());
			}
			PERSIST_USER_DEFINED_TIMESTAMPS => {
				let flag = length_prefixed(payload, &mut pos)?;
				edit.persist_udt = Some(flag.first().copied().unwrap_or(1) != 0);
			}
			SUBCOMPACTION_PROGRESS => {
				length_prefixed(payload, &mut pos)?;
			}
			other if other & TAG_SAFE_IGNORE_MASK != 0 => {
				length_prefixed(payload, &mut pos)?;
			}
			other => {
				return Err(unsupported(format!(
					"the MANIFEST holds a record with tag {other}, which this reader does not know"
				)))
			}
		}
	}
	Ok(edit)
}

fn skip_custom_fields(payload: &[u8], pos: &mut usize) -> Result<()> {
	loop {
		let tag = varint32(payload, pos)?;
		if tag == 0 {
			return Ok(());
		}
		length_prefixed(payload, pos)?;
	}
}

impl ManifestState {
	/// Replays the MANIFEST that `CURRENT` of the database in `dir` names.
	pub fn read(dir: &Path) -> Result<Self> {
		let current = std::fs::read_to_string(dir.join("CURRENT"))?;
		let name = current.trim_end_matches(['\n', '\r']);
		if !name.starts_with("MANIFEST-") {
			return Err(corrupt(format!("CURRENT names {name:?}, not a MANIFEST")));
		}
		let path = dir.join(name);
		let file = BufReader::new(File::open(&path)?);
		let mut reader = LogReader::new(file, name.to_string());

		let mut state = ManifestState {
			persist_user_defined_timestamps: true,
			..Default::default()
		};
		while let Some(record) = reader.next_record()? {
			if record.kind != RecordKind::Data {
				continue;
			}
			let edit = decode_edit(&record.payload)?;
			state.apply(edit)?;
		}
		// RocksDB opens a MANIFEST whose last edit was cut short by a crash and ignores that edit;
		// a record that is there but fails its checksum is another matter.
		if reader.end == End::Damaged {
			return Err(corrupt(format!("{name} holds a damaged record")));
		}
		Ok(state)
	}

	fn apply(&mut self, edit: Edit) -> Result<()> {
		if let Some(name) = &edit.added_column_family {
			if name != "default" {
				return Err(unsupported(format!(
					"the database has a column family {name:?}; only \"default\" is migrated"
				)));
			}
		}
		if edit.dropped_column_family {
			return Err(unsupported("a column family was dropped".to_string()));
		}
		if edit.column_family != 0 {
			// Records of another column family: it must not hold data, or it would be lost.
			if !edit.added.is_empty() || !edit.blob_added.is_empty() {
				return Err(unsupported(format!(
					"column family {} holds tables; only the default column family is migrated",
					edit.column_family
				)));
			}
			return Ok(());
		}
		if let Some(comparator) = edit.comparator {
			self.comparator = Some(comparator);
		}
		if let Some(n) = edit.log_number {
			self.log_number = self.log_number.max(n);
		}
		if let Some(n) = edit.next_file_number {
			self.next_file_number = self.next_file_number.max(n);
		}
		if let Some(n) = edit.last_sequence {
			self.last_sequence = self.last_sequence.max(n);
		}
		for (_, number) in edit.deleted {
			self.tables.remove(&number);
		}
		for (_, table) in edit.added {
			self.tables.insert(table.number, table);
		}
		for number in edit.blob_added {
			self.blob_files.insert(number, ());
		}
		if let Some(ts) = edit.full_history_ts_low {
			self.full_history_ts_low = Some(ts);
		}
		if let Some(persist) = edit.persist_udt {
			if !persist {
				self.persist_user_defined_timestamps = false;
			}
		}
		Ok(())
	}

	/// `full_history_ts_low` as the number it is, if there is one.
	pub fn history_cutoff(&self) -> Option<u64> {
		let bytes = self.full_history_ts_low.as_deref()?;
		Some(u64::from_le_bytes(bytes.get(..8)?.try_into().ok()?))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::error::Error;
	use crate::log::testing::log_bytes;

	fn varstring(out: &mut Vec<u8>, bytes: &[u8]) {
		out.push(bytes.len() as u8);
		out.extend_from_slice(bytes);
	}

	fn base_edit() -> Vec<u8> {
		let mut edit = vec![COMPARATOR as u8];
		varstring(&mut edit, b"leveldb.BytewiseComparator");
		edit.extend_from_slice(&[
			LOG_NUMBER as u8,
			5,
			NEXT_FILE_NUMBER as u8,
			9,
			LAST_SEQUENCE as u8,
			100,
		]);
		edit
	}

	fn later_edit() -> Vec<u8> {
		vec![LOG_NUMBER as u8, 8, LAST_SEQUENCE as u8, 120]
	}

	fn database(manifest: &[u8]) -> tempfile::TempDir {
		let dir = tempfile::tempdir().unwrap();
		std::fs::write(dir.path().join("CURRENT"), "MANIFEST-000004\n").unwrap();
		std::fs::write(dir.path().join("MANIFEST-000004"), manifest).unwrap();
		dir
	}

	#[test]
	fn the_edits_of_a_manifest_are_replayed() {
		let dir = database(&log_bytes(&[base_edit(), later_edit()], None));
		let state = ManifestState::read(dir.path()).unwrap();
		assert_eq!(state.comparator.as_deref(), Some("leveldb.BytewiseComparator"));
		assert_eq!((state.log_number, state.last_sequence, state.next_file_number), (8, 120, 9));
	}

	#[test]
	fn an_edit_torn_by_a_crash_is_ignored_as_rocksdb_ignores_it() {
		let whole = log_bytes(&[base_edit(), later_edit()], None);
		let first_end = log_bytes(&[base_edit()], None).len();
		for cut in first_end + 1..whole.len() {
			let dir = database(&whole[..cut]);
			let state =
				ManifestState::read(dir.path()).unwrap_or_else(|e| panic!("cut {cut}: {e}"));
			assert_eq!((state.log_number, state.last_sequence), (5, 100), "cut at {cut}");
		}
	}

	#[test]
	fn a_damaged_edit_is_an_error() {
		let mut bytes = log_bytes(&[base_edit(), later_edit()], None);
		let n = bytes.len();
		bytes[n - 2] ^= 0x01;
		let dir = database(&bytes);
		assert!(matches!(ManifestState::read(dir.path()), Err(Error::Corrupt(_))));
	}
}
