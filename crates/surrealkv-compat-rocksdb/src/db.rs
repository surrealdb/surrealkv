//! A RocksDB database directory as a stream of versions.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::blob::{decode_blob_index, BlobFiles, BlobRef};
use crate::error::{corrupt, unsupported, Result};
use crate::log::End;
use crate::manifest::ManifestState;
use crate::sst::{TableEntries, TableReader};
use crate::wal::{read_log, WalOp};

pub const BYTEWISE_COMPARATOR: &str = "leveldb.BytewiseComparator";
/// The comparator SurrealDB sets on a database it runs in versioned mode.
pub const TIMESTAMP_COMPARATOR: &str = "surrealdb.TimestampComparator";
const TIMESTAMP_SIZE: usize = 8;

const TYPE_DELETION: u8 = 0x0;
const TYPE_VALUE: u8 = 0x1;
const TYPE_BLOB_INDEX: u8 = 0x11;
const TYPE_DELETION_WITH_TIMESTAMP: u8 = 0x14;

/// What a version holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
	Inline(Vec<u8>),
	/// The value is in a blob file; [`BlobFiles::read`] fetches it.
	Blob(BlobRef),
	/// A delete has no value.
	None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionKind {
	Put,
	Delete,
}

/// One version of one key.
#[derive(Debug, Clone)]
pub struct Version {
	pub user_key: Vec<u8>,
	/// The user-defined timestamp, for a database that has them. As RocksDB stores it: a SurrealDB
	/// hybrid logical clock value.
	pub timestamp: Option<u64>,
	/// RocksDB's sequence number. Only the order of versions that share a key and a timestamp
	/// depends on it.
	pub sequence: u64,
	pub kind: VersionKind,
	pub payload: Payload,
}

/// An open RocksDB directory.
pub struct RocksDb {
	dir: PathBuf,
	pub manifest: ManifestState,
	timestamp_size: usize,
	/// Tables of level 0, which overlap and are each a source of the merge.
	level0: Vec<TableReader>,
	/// The tables of every other level, in key order. They do not overlap, so a level is one
	/// source and only one of its files is open at a time.
	levels: BTreeMap<u32, Vec<TableReader>>,
	wal: Arc<Vec<Version>>,
	/// Whether the write-ahead log ended in a record that was cut short or damaged, or went on
	/// past a hole. RocksDB recovers up to that point and no further, and so does this.
	pub wal_damaged: bool,
	pub wal_files: Vec<PathBuf>,
}

/// A table of a level below 0 with its smallest key, which is what orders the tables of a level.
struct LevelTable {
	key: Vec<u8>,
	timestamp: Option<u64>,
	sequence: u64,
	table: TableReader,
}

/// Whether `dir` holds a RocksDB database: a `CURRENT` file naming a `MANIFEST` that exists.
pub fn is_rocksdb_dir(dir: &Path) -> bool {
	let Ok(current) = std::fs::read_to_string(dir.join("CURRENT")) else {
		return false;
	};
	let name = current.trim_end_matches(['\n', '\r']);
	name.starts_with("MANIFEST-") && dir.join(name).is_file()
}

fn wal_number(name: &str) -> Option<u64> {
	name.strip_suffix(".log").and_then(|stem| stem.parse().ok())
}

/// Splits an internal key into its user key, timestamp and sequence number.
fn split_internal_key(
	key: &[u8],
	timestamp_size: usize,
) -> Result<(Vec<u8>, Option<u64>, u64, u8)> {
	if key.len() < 8 + timestamp_size {
		return Err(corrupt(format!("an internal key of {} bytes", key.len())));
	}
	let n = key.len();
	let trailer = u64::from_le_bytes(key[n - 8..].try_into().expect("8"));
	let mut user_key = key[..n - 8].to_vec();
	let timestamp = if timestamp_size > 0 {
		let ts = u64::from_le_bytes(user_key[user_key.len() - 8..].try_into().expect("8"));
		user_key.truncate(user_key.len() - 8);
		Some(ts)
	} else {
		None
	};
	Ok((user_key, timestamp, trailer >> 8, (trailer & 0xff) as u8))
}

impl RocksDb {
	/// Opens the database in `dir` for reading. Nothing is written to it.
	pub fn open(dir: &Path) -> Result<Self> {
		let manifest = ManifestState::read(dir)?;
		let comparator = manifest
			.comparator
			.clone()
			.ok_or_else(|| corrupt("the MANIFEST names no comparator"))?;
		let timestamp_size = match comparator.as_str() {
			BYTEWISE_COMPARATOR => 0,
			TIMESTAMP_COMPARATOR => TIMESTAMP_SIZE,
			other => return Err(unsupported(format!("the comparator {other:?}"))),
		};
		if timestamp_size > 0 && !manifest.persist_user_defined_timestamps {
			return Err(unsupported(
				"a database whose tables do not store their user-defined timestamps".to_string(),
			));
		}

		let mut level0 = Vec::new();
		let mut by_level: BTreeMap<u32, Vec<LevelTable>> = BTreeMap::new();
		for (number, file) in &manifest.tables {
			let path = dir.join(format!("{number:06}.sst"));
			let table = TableReader::open(&path, *number)?;
			if let Some(name) = &table.properties.comparator {
				if *name != comparator {
					return Err(corrupt(format!(
						"{} was written with the comparator {name:?}, not {comparator:?}",
						path.display()
					)));
				}
			}
			if file.level == 0 {
				level0.push(table);
			} else {
				let (key, timestamp, sequence, _) =
					split_internal_key(&file.smallest, timestamp_size)?;
				by_level.entry(file.level).or_default().push(LevelTable {
					key,
					timestamp,
					sequence,
					table,
				});
			}
		}
		let mut levels = BTreeMap::new();
		for (level, mut tables) in by_level {
			// The order of the comparator: user key, newest timestamp first, newest sequence first.
			tables.sort_by(|a, b| {
				a.key
					.cmp(&b.key)
					.then_with(|| b.timestamp.unwrap_or(0).cmp(&a.timestamp.unwrap_or(0)))
					.then_with(|| b.sequence.cmp(&a.sequence))
			});
			levels.insert(level, tables.into_iter().map(|t| t.table).collect::<Vec<_>>());
		}

		// The logs that may hold writes no table has: those from the manifest's log number on.
		let mut logs: Vec<(u64, PathBuf)> = Vec::new();
		for entry in std::fs::read_dir(dir)?.flatten() {
			let name = entry.file_name().to_string_lossy().to_string();
			if let Some(number) = wal_number(&name) {
				if number >= manifest.log_number {
					logs.push((number, entry.path()));
				}
			}
		}
		logs.sort();
		let (wal, wal_damaged) = replay_logs(&logs, timestamp_size)?;

		Ok(Self {
			dir: dir.to_path_buf(),
			manifest,
			timestamp_size,
			level0,
			levels,
			wal: Arc::new(wal),
			wal_damaged,
			wal_files: logs.into_iter().map(|(_, path)| path).collect(),
		})
	}

	pub fn dir(&self) -> &Path {
		&self.dir
	}

	/// Whether the database keeps user-defined timestamps (SurrealDB's versioned mode).
	pub fn has_user_timestamps(&self) -> bool {
		self.timestamp_size > 0
	}

	fn tables(&self) -> impl Iterator<Item = &TableReader> {
		self.level0.iter().chain(self.levels.values().flatten())
	}

	pub fn table_count(&self) -> usize {
		self.tables().count()
	}

	/// An upper bound on the versions [`Self::versions`] yields: what the tables and the log hold.
	pub fn entry_upper_bound(&self) -> u64 {
		self.tables().map(|t| t.properties.num_entries).sum::<u64>() + self.wal.len() as u64
	}

	pub fn blob_files(&self) -> BlobFiles {
		BlobFiles::new(self.dir.clone(), self.timestamp_size)
	}

	/// Every version of every key, in the order RocksDB keeps them: by user key, then newest
	/// timestamp first, then newest sequence first. Of the versions that share a key and a
	/// timestamp only the newest is yielded, which is the one RocksDB would serve.
	pub fn versions(&self) -> Result<Versions> {
		let mut sources: Vec<Source> = Vec::new();
		for table in &self.level0 {
			sources.push(Source::Level(LevelSource::new(vec![table.clone()], self.timestamp_size)));
		}
		for tables in self.levels.values() {
			sources.push(Source::Level(LevelSource::new(tables.clone(), self.timestamp_size)));
		}
		sources.push(Source::Wal(Arc::clone(&self.wal), 0));
		let mut heap = BinaryHeap::new();
		let mut stream = Versions {
			sources,
			heap: BinaryHeap::new(),
			last: None,
		};
		for source in 0..stream.sources.len() {
			stream.refill(source, &mut heap)?;
		}
		stream.heap = heap;
		Ok(stream)
	}
}

/// Replays write-ahead logs, in order, as RocksDB's point-in-time recovery does: up to the first
/// record that is cut short or damaged, and into the next log only if that one goes on exactly
/// where the sequence numbers stopped. Returns the writes, in the order of the comparator, and
/// whether replay stopped early.
fn replay_logs(logs: &[(u64, PathBuf)], timestamp_size: usize) -> Result<(Vec<Version>, bool)> {
	let mut wal = Vec::new();
	let mut wal_damaged = false;
	// RocksDB's point-in-time recovery: after a record that is cut short or damaged, replay
	// stops, unless the next log goes on exactly where the sequence numbers stopped.
	let mut stopped = false;
	let mut next_sequence: Option<u64> = None;
	for (number, path) in logs {
		let contents = read_log(path, *number)?;
		if stopped {
			if contents.first_sequence.is_some() && contents.first_sequence == next_sequence {
				stopped = false;
			} else {
				wal_damaged = true;
				continue;
			}
		}
		if let Some(declared) = contents.declared_timestamp_size {
			if declared != timestamp_size {
				return Err(corrupt(format!(
					"{} declares a timestamp size of {declared}, the comparator says {timestamp_size}",
					path.display()
				)));
			}
		}
		if contents.next_sequence.is_some() {
			next_sequence = contents.next_sequence;
		}
		for record in contents.records {
			let mut key = record.key;
			let timestamp = if timestamp_size > 0 {
				if key.len() < timestamp_size {
					return Err(corrupt(format!(
						"{} holds a key shorter than a timestamp",
						path.display()
					)));
				}
				let ts = u64::from_le_bytes(key[key.len() - 8..].try_into().expect("8"));
				key.truncate(key.len() - 8);
				Some(ts)
			} else {
				None
			};
			let (kind, payload) = match record.op {
				WalOp::Put => (VersionKind::Put, Payload::Inline(record.value)),
				WalOp::Delete => (VersionKind::Delete, Payload::None),
			};
			wal.push(Version {
				user_key: key,
				timestamp,
				sequence: record.seq,
				kind,
				payload,
			});
		}
		if contents.end != End::Clean {
			wal_damaged = true;
			stopped = true;
		}
	}
	wal.sort_by(order);
	Ok((wal, wal_damaged))
}

fn order(a: &Version, b: &Version) -> Ordering {
	a.user_key
		.cmp(&b.user_key)
		.then_with(|| b.timestamp.unwrap_or(0).cmp(&a.timestamp.unwrap_or(0)))
		.then_with(|| b.sequence.cmp(&a.sequence))
}

/// A version waiting in the merge, ordered so that the smallest comes out of a max-heap first.
struct Pending {
	version: Version,
	source: usize,
}

impl PartialEq for Pending {
	fn eq(&self, other: &Self) -> bool {
		order(&self.version, &other.version) == Ordering::Equal
	}
}
impl Eq for Pending {}
impl PartialOrd for Pending {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}
impl Ord for Pending {
	fn cmp(&self, other: &Self) -> Ordering {
		order(&other.version, &self.version)
	}
}

/// The tables of one level in key order, read one after the other: only one file is open.
struct LevelSource {
	tables: Vec<TableReader>,
	next: usize,
	current: Option<TableEntries>,
	timestamp_size: usize,
}

impl LevelSource {
	fn new(tables: Vec<TableReader>, timestamp_size: usize) -> Self {
		Self {
			tables,
			next: 0,
			current: None,
			timestamp_size,
		}
	}

	fn next(&mut self) -> Option<Result<Version>> {
		loop {
			if let Some(current) = &mut self.current {
				match current.next() {
					Some(Ok(entry)) => {
						return Some(version_from_entry(
							entry.key,
							entry.value,
							self.timestamp_size,
						))
					}
					Some(Err(e)) => return Some(Err(e)),
					None => self.current = None,
				}
			}
			let table = self.tables.get(self.next)?;
			self.next += 1;
			match table.entries() {
				Ok(entries) => self.current = Some(entries),
				Err(e) => return Some(Err(e)),
			}
		}
	}
}

enum Source {
	Level(LevelSource),
	Wal(Arc<Vec<Version>>, usize),
}

impl Source {
	fn next(&mut self) -> Option<Result<Version>> {
		match self {
			Source::Wal(versions, index) => {
				let version = versions.get(*index)?.clone();
				*index += 1;
				Some(Ok(version))
			}
			Source::Level(level) => level.next(),
		}
	}
}

fn version_from_entry(key: Vec<u8>, value: Vec<u8>, timestamp_size: usize) -> Result<Version> {
	let (user_key, timestamp, sequence, value_type) = split_internal_key(&key, timestamp_size)?;
	let (kind, payload) = match value_type {
		TYPE_VALUE => (VersionKind::Put, Payload::Inline(value)),
		TYPE_BLOB_INDEX => (VersionKind::Put, Payload::Blob(decode_blob_index(&value)?)),
		TYPE_DELETION | TYPE_DELETION_WITH_TIMESTAMP => (VersionKind::Delete, Payload::None),
		other => {
			return Err(unsupported(format!("a table holds an entry of value type {other:#04x}")))
		}
	};
	Ok(Version {
		user_key,
		timestamp,
		sequence,
		kind,
		payload,
	})
}

/// The merged stream of versions: see [`RocksDb::versions`].
pub struct Versions {
	sources: Vec<Source>,
	heap: BinaryHeap<Pending>,
	last: Option<(Vec<u8>, Option<u64>)>,
}

impl Versions {
	fn refill(&mut self, source: usize, heap: &mut BinaryHeap<Pending>) -> Result<()> {
		if let Some(next) = self.sources[source].next() {
			heap.push(Pending {
				version: next?,
				source,
			});
		}
		Ok(())
	}
}

impl Iterator for Versions {
	type Item = Result<Version>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let pending = self.heap.pop()?;
			let mut heap = std::mem::take(&mut self.heap);
			let refilled = self.refill(pending.source, &mut heap);
			self.heap = heap;
			if let Err(e) = refilled {
				return Some(Err(e));
			}
			let version = pending.version;
			// An older write of the same key at the same timestamp is shadowed.
			if let Some((key, ts)) = &self.last {
				if *key == version.user_key && *ts == version.timestamp {
					continue;
				}
			}
			self.last = Some((version.user_key.clone(), version.timestamp));
			return Some(Ok(version));
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::log::testing::{batch, log_bytes};

	/// A log file holding one batch per element of `batches`, cut to `keep` bytes if given.
	fn write_log(
		dir: &Path,
		number: u64,
		batches: &[Vec<u8>],
		keep: Option<usize>,
	) -> (u64, PathBuf) {
		let mut bytes = log_bytes(batches, None);
		if let Some(keep) = keep {
			bytes.truncate(keep);
		}
		let path = dir.join(format!("{number:06}.log"));
		std::fs::write(&path, bytes).unwrap();
		(number, path)
	}

	fn keys(versions: &[Version]) -> Vec<String> {
		let mut sorted: Vec<&Version> = versions.iter().collect();
		sorted.sort_by_key(|v| v.sequence);
		sorted.iter().map(|v| String::from_utf8_lossy(&v.user_key).into_owned()).collect()
	}

	#[test]
	fn logs_replay_in_order_and_come_out_in_key_order() {
		let dir = tempfile::tempdir().unwrap();
		let one =
			write_log(dir.path(), 3, &[batch(1, &[(b"b", Some(b"1")), (b"a", Some(b"2"))])], None);
		let two = write_log(dir.path(), 4, &[batch(3, &[(b"b", None)])], None);
		let (versions, damaged) = replay_logs(&[one, two], 0).unwrap();
		assert!(!damaged);
		assert_eq!(keys(&versions), ["b", "a", "b"]);
		// In the order of the comparator: key, then the newest write first.
		let order: Vec<(String, u64)> = versions
			.iter()
			.map(|v| (String::from_utf8_lossy(&v.user_key).into_owned(), v.sequence))
			.collect();
		assert_eq!(order, [("a".to_string(), 2), ("b".to_string(), 3), ("b".to_string(), 1)]);
		assert_eq!(versions[1].kind, VersionKind::Delete);
	}

	#[test]
	fn a_log_that_goes_on_where_a_torn_one_stopped_is_replayed() {
		let dir = tempfile::tempdir().unwrap();
		let first = [batch(1, &[(b"a", Some(b"1"))]), batch(2, &[(b"b", Some(b"2"))])];
		let whole = log_bytes(&first, None).len();
		// The second batch is torn: the first log ends after its first batch, and so does the
		// numbering, so a log that starts at sequence 2 continues it.
		let one = write_log(dir.path(), 3, &first, Some(whole - 3));
		let two = write_log(dir.path(), 4, &[batch(2, &[(b"c", Some(b"3"))])], None);
		let (versions, damaged) = replay_logs(&[one, two], 0).unwrap();
		assert!(damaged);
		assert_eq!(keys(&versions), ["a", "c"]);
	}

	#[test]
	fn a_log_after_a_hole_is_not_replayed() {
		let dir = tempfile::tempdir().unwrap();
		let first = [batch(1, &[(b"a", Some(b"1"))]), batch(2, &[(b"b", Some(b"2"))])];
		let whole = log_bytes(&first, None).len();
		let one = write_log(dir.path(), 3, &first, Some(whole - 3));
		// Sequence 2 is missing: replaying this would show writes that came after a lost one.
		let two = write_log(dir.path(), 4, &[batch(3, &[(b"c", Some(b"3"))])], None);
		let three = write_log(dir.path(), 5, &[batch(4, &[(b"d", Some(b"4"))])], None);
		let (versions, damaged) = replay_logs(&[one, two, three], 0).unwrap();
		assert!(damaged);
		assert_eq!(keys(&versions), ["a"]);
	}

	#[test]
	fn a_timestamp_the_log_does_not_have_room_for_is_an_error() {
		let dir = tempfile::tempdir().unwrap();
		let one = write_log(dir.path(), 3, &[batch(1, &[(b"abc", Some(b"1"))])], None);
		assert!(replay_logs(&[one], 8).is_err());
	}

	#[test]
	fn timestamps_are_split_off_the_keys_of_a_versioned_log() {
		let dir = tempfile::tempdir().unwrap();
		let mut key = b"k".to_vec();
		key.extend_from_slice(&77u64.to_le_bytes());
		let one = write_log(dir.path(), 3, &[batch(1, &[(&key, Some(b"v"))])], None);
		let (versions, _) = replay_logs(&[one], 8).unwrap();
		assert_eq!(versions[0].user_key, b"k");
		assert_eq!(versions[0].timestamp, Some(77));
	}
}
