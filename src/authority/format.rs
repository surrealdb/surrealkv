//! Durable authority formats (FK1): catalog, per-branch state, and root
//! manifests. All three are numbered immutable versions with the layout
//! `magic(4) + format_version(u16 LE) + body + crc32(u32 LE over all
//! preceding bytes)`. Decode fails closed: magic, then version identity,
//! then checksum, then field validation with caps checked BEFORE
//! allocation, canonical ordering enforced, and trailing bytes rejected.

use crate::error::{Error, Result};
use crate::{BranchGeneration, BranchId};

pub(crate) const AUTHORITY_FORMAT_VERSION: u16 = 1;

pub(crate) const CATALOG_MAGIC: [u8; 4] = *b"SKBC";
pub(crate) const STATE_MAGIC: [u8; 4] = *b"SKBM";
pub(crate) const ROOT_MAGIC: [u8; 4] = *b"SKRT";

/// Live + not-yet-reclaimed catalog entries (strata parity; fail-closed).
pub(crate) const MAX_CATALOG_ENTRIES: usize = 4096;
pub(crate) const MAX_BRANCH_NAME_LEN: usize = 255;
/// Timeline fenceposts carried in the root tail.
pub(crate) const MAX_TIMELINE_TAIL: usize = 512;
/// Per-branch state hints in the root (bounded by the catalog cap).
pub(crate) const MAX_STATE_HINTS: usize = MAX_CATALOG_ENTRIES;
/// Tables per level in a state manifest (strata's per-level cap).
pub(crate) const MAX_TABLES_PER_LEVEL: usize = 16_384;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BranchStatus {
	Active,
	Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParentLink {
	pub(crate) parent: BranchId,
	pub(crate) parent_generation: BranchGeneration,
	pub(crate) fork_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CatalogEntry {
	pub(crate) branch: BranchId,
	pub(crate) name: String,
	pub(crate) generation: BranchGeneration,
	pub(crate) status: BranchStatus,
	pub(crate) created_at_seq: u64,
	pub(crate) parent: Option<ParentLink>,
	pub(crate) deleted_at_seq: Option<u64>,
	/// Branch TTL for agentic sandboxes; enforcement is a maintenance sweep.
	pub(crate) expires_at: Option<u64>,
}

/// THE authority: branch existence, generation, parentage, anchors, TTLs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CatalogManifest {
	pub(crate) db_id: [u8; 16],
	/// Must equal the version encoded in the filename.
	pub(crate) catalog_version: u64,
	/// Globally monotone generation allocator: generations are unique across
	/// every branch that ever existed, so tombstone reclamation can never
	/// enable stale-generation acceptance.
	pub(crate) next_generation: u64,
	pub(crate) writer_epoch: u64,
	pub(crate) maintenance_epoch: u64,
	/// Sorted strictly ascending by raw branch id bytes.
	pub(crate) entries: Vec<CatalogEntry>,
}

/// A branch's OWNED durable facts. Absent entirely until the branch first
/// flushes; says nothing about inheritance (views are logical, resolved
/// through the catalog).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BranchStateManifest {
	pub(crate) branch: BranchId,
	pub(crate) generation: BranchGeneration,
	/// Must equal the version encoded in the filename.
	pub(crate) state_version: u64,
	pub(crate) last_sequence: u64,
	pub(crate) flushed_log_number: u64,
	/// Lowest sequence at which a view of this branch is still complete: the
	/// compaction that drops a version raises this past the surviving version
	/// of that key, so a historical fork below it is refused instead of
	/// silently short of rows (design §3.3a, FK4 amendment 2).
	pub(crate) retained_floor_seq: u64,
	/// Table ids per level, in level order; a table id appears at most once
	/// in the whole file. Table facts hydrate from the SSTs themselves.
	pub(crate) levels: Vec<Vec<u64>>,
}

/// Global recovery facts, written on flush-class events, never per commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RootManifest {
	pub(crate) db_id: [u8; 16],
	/// Must equal the version encoded in the filename.
	pub(crate) root_version: u64,
	pub(crate) visible_seq: u64,
	pub(crate) last_commit_ts: u64,
	/// (commit_ts, seq) fenceposts, strictly increasing on both axes.
	pub(crate) timeline_tail: Vec<(u64, u64)>,
	/// Derived from the BR1 dependency-tracker snapshot over ALL
	/// catalog-live branches — never recomputed from state files alone.
	pub(crate) wal_reclaim_floor: u64,
	/// Block-reserved table-id watermark: recovery resumes allocation here,
	/// so ids from lost flushes are never reused.
	pub(crate) next_table_id: u64,
	/// Newest catalog version this root was published against. Open refuses a
	/// catalog lineage whose newest version is BELOW it: that means the
	/// authority was truncated or rolled back underneath a root that already
	/// depended on it. Metadata pruning never trips this, because it only ever
	/// removes versions older than the newest.
	pub(crate) catalog_version_floor: u64,
	/// Latest known state version per branch (open probes forward from
	/// these). Sorted strictly ascending by raw branch id bytes.
	pub(crate) state_hints: Vec<(BranchId, BranchGeneration, u64)>,
}

// ===== encoding helpers (little-endian throughout) =====

fn put_u16(out: &mut Vec<u8>, value: u16) {
	out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
	out.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
	out.extend_from_slice(&value.to_le_bytes());
}

struct Reader<'a> {
	data: &'a [u8],
	pos: usize,
}

impl<'a> Reader<'a> {
	fn new(data: &'a [u8]) -> Self {
		Self {
			data,
			pos: 0,
		}
	}

	fn take(&mut self, len: usize, field: &str) -> Result<&'a [u8]> {
		let end = self.pos.checked_add(len).ok_or_else(|| truncated(field))?;
		let bytes = self.data.get(self.pos..end).ok_or_else(|| truncated(field))?;
		self.pos = end;
		Ok(bytes)
	}

	fn u8(&mut self, field: &str) -> Result<u8> {
		Ok(self.take(1, field)?[0])
	}

	fn u16(&mut self, field: &str) -> Result<u16> {
		Ok(u16::from_le_bytes(self.take(2, field)?.try_into().unwrap()))
	}

	fn u32(&mut self, field: &str) -> Result<u32> {
		Ok(u32::from_le_bytes(self.take(4, field)?.try_into().unwrap()))
	}

	fn u64(&mut self, field: &str) -> Result<u64> {
		Ok(u64::from_le_bytes(self.take(8, field)?.try_into().unwrap()))
	}

	fn array16(&mut self, field: &str) -> Result<[u8; 16]> {
		Ok(self.take(16, field)?.try_into().unwrap())
	}

	/// Rejects trailing bytes — a valid prefix followed by junk is damage.
	fn finish(&self, format: &str) -> Result<()> {
		if self.pos != self.data.len() {
			return Err(Error::Corruption(format!("{format}: trailing bytes after manifest body")));
		}
		Ok(())
	}
}

fn truncated(field: &str) -> Error {
	Error::Corruption(format!("authority manifest truncated at {field}"))
}

fn invalid(format: &str, field: &str, detail: &str) -> Error {
	Error::Corruption(format!("{format}: invalid {field}: {detail}"))
}

/// Shared header check + checksum verification. Returns the body reader.
/// Version identity is checked BEFORE the checksum so a future format
/// reports "future format", not a checksum mismatch over an unknown layout.
fn open_envelope<'a>(format: &str, magic: [u8; 4], data: &'a [u8]) -> Result<Reader<'a>> {
	if data.len() < 4 + 2 + 4 {
		return Err(Error::Corruption(format!("{format}: shorter than the minimum envelope")));
	}
	if data[0..4] != magic {
		return Err(Error::Corruption(format!("{format}: bad magic")));
	}
	let version = u16::from_le_bytes(data[4..6].try_into().unwrap());
	if version == 0 {
		return Err(Error::Corruption(format!("{format}: pre-v1 format version 0")));
	}
	if version != AUTHORITY_FORMAT_VERSION {
		return Err(Error::Corruption(format!("{format}: future format version {version}")));
	}
	let body_end = data.len() - 4;
	let stored_crc = u32::from_le_bytes(data[body_end..].try_into().unwrap());
	let computed = crc32fast::hash(&data[..body_end]);
	if stored_crc != computed {
		return Err(Error::Corruption(format!(
			"{format}: checksum mismatch (stored {stored_crc:#010x}, computed {computed:#010x})"
		)));
	}
	let mut reader = Reader::new(&data[..body_end]);
	// Consume the already-validated envelope prefix.
	reader.take(6, "envelope").expect("envelope length checked above");
	Ok(reader)
}

fn seal_envelope(mut out: Vec<u8>) -> Vec<u8> {
	let crc = crc32fast::hash(&out);
	put_u32(&mut out, crc);
	out
}

// ===== catalog =====

const ENTRY_FLAG_PARENT: u8 = 1 << 0;
const ENTRY_FLAG_DELETED_AT: u8 = 1 << 1;
const ENTRY_FLAG_EXPIRES: u8 = 1 << 2;
const ENTRY_FLAG_KNOWN: u8 = ENTRY_FLAG_PARENT | ENTRY_FLAG_DELETED_AT | ENTRY_FLAG_EXPIRES;

const STATUS_ACTIVE: u8 = 0;
const STATUS_DELETED: u8 = 1;

impl CatalogManifest {
	pub(crate) fn encode(&self) -> Result<Vec<u8>> {
		self.validate("encode")?;
		let mut out = Vec::new();
		out.extend_from_slice(&CATALOG_MAGIC);
		put_u16(&mut out, AUTHORITY_FORMAT_VERSION);
		out.extend_from_slice(&self.db_id);
		put_u64(&mut out, self.catalog_version);
		put_u64(&mut out, self.next_generation);
		put_u64(&mut out, self.writer_epoch);
		put_u64(&mut out, self.maintenance_epoch);
		put_u32(&mut out, self.entries.len() as u32);
		for entry in &self.entries {
			out.extend_from_slice(&entry.branch.0);
			put_u16(&mut out, entry.name.len() as u16);
			out.extend_from_slice(entry.name.as_bytes());
			put_u64(&mut out, entry.generation.0);
			out.push(match entry.status {
				BranchStatus::Active => STATUS_ACTIVE,
				BranchStatus::Deleted => STATUS_DELETED,
			});
			let mut flags = 0u8;
			if entry.parent.is_some() {
				flags |= ENTRY_FLAG_PARENT;
			}
			if entry.deleted_at_seq.is_some() {
				flags |= ENTRY_FLAG_DELETED_AT;
			}
			if entry.expires_at.is_some() {
				flags |= ENTRY_FLAG_EXPIRES;
			}
			out.push(flags);
			put_u64(&mut out, entry.created_at_seq);
			if let Some(parent) = &entry.parent {
				out.extend_from_slice(&parent.parent.0);
				put_u64(&mut out, parent.parent_generation.0);
				put_u64(&mut out, parent.fork_seq);
			}
			if let Some(deleted_at) = entry.deleted_at_seq {
				put_u64(&mut out, deleted_at);
			}
			if let Some(expires) = entry.expires_at {
				put_u64(&mut out, expires);
			}
		}
		Ok(seal_envelope(out))
	}

	pub(crate) fn decode(data: &[u8]) -> Result<Self> {
		const FORMAT: &str = "catalog manifest";
		let mut reader = open_envelope(FORMAT, CATALOG_MAGIC, data)?;
		let db_id = reader.array16("db_id")?;
		let catalog_version = reader.u64("catalog_version")?;
		let next_generation = reader.u64("next_generation")?;
		let writer_epoch = reader.u64("writer_epoch")?;
		let maintenance_epoch = reader.u64("maintenance_epoch")?;
		let entry_count = reader.u32("entry_count")? as usize;
		if entry_count > MAX_CATALOG_ENTRIES {
			return Err(invalid(
				FORMAT,
				"entry_count",
				&format!("{entry_count} exceeds cap {MAX_CATALOG_ENTRIES}"),
			));
		}
		let mut entries = Vec::with_capacity(entry_count);
		for _ in 0..entry_count {
			let branch = BranchId(reader.array16("entry branch id")?);
			let name_len = reader.u16("entry name length")? as usize;
			if name_len == 0 || name_len > MAX_BRANCH_NAME_LEN {
				return Err(invalid(FORMAT, "entry name length", &name_len.to_string()));
			}
			let name_bytes = reader.take(name_len, "entry name")?;
			let name = std::str::from_utf8(name_bytes)
				.map_err(|_| invalid(FORMAT, "entry name", "not valid UTF-8"))?
				.to_owned();
			let generation = BranchGeneration(reader.u64("entry generation")?);
			let status = match reader.u8("entry status")? {
				STATUS_ACTIVE => BranchStatus::Active,
				STATUS_DELETED => BranchStatus::Deleted,
				other => {
					return Err(invalid(FORMAT, "entry status", &other.to_string()));
				}
			};
			let flags = reader.u8("entry flags")?;
			if flags & !ENTRY_FLAG_KNOWN != 0 {
				return Err(invalid(FORMAT, "entry flags", &format!("unknown bits {flags:#04x}")));
			}
			let created_at_seq = reader.u64("entry created_at_seq")?;
			let parent = if flags & ENTRY_FLAG_PARENT != 0 {
				Some(ParentLink {
					parent: BranchId(reader.array16("entry parent id")?),
					parent_generation: BranchGeneration(reader.u64("entry parent generation")?),
					fork_seq: reader.u64("entry fork_seq")?,
				})
			} else {
				None
			};
			let deleted_at_seq = if flags & ENTRY_FLAG_DELETED_AT != 0 {
				Some(reader.u64("entry deleted_at_seq")?)
			} else {
				None
			};
			let expires_at = if flags & ENTRY_FLAG_EXPIRES != 0 {
				Some(reader.u64("entry expires_at")?)
			} else {
				None
			};
			entries.push(CatalogEntry {
				branch,
				name,
				generation,
				status,
				created_at_seq,
				parent,
				deleted_at_seq,
				expires_at,
			});
		}
		reader.finish(FORMAT)?;
		let manifest = Self {
			db_id,
			catalog_version,
			next_generation,
			writer_epoch,
			maintenance_epoch,
			entries,
		};
		manifest.validate("decode")?;
		Ok(manifest)
	}

	fn validate(&self, action: &str) -> Result<()> {
		const FORMAT: &str = "catalog manifest";
		if self.catalog_version == 0 {
			return Err(invalid(FORMAT, "catalog_version", &format!("0 during {action}")));
		}
		if self.entries.len() > MAX_CATALOG_ENTRIES {
			return Err(invalid(
				FORMAT,
				"entry_count",
				&format!("{} exceeds cap {MAX_CATALOG_ENTRIES}", self.entries.len()),
			));
		}
		let mut previous: Option<&BranchId> = None;
		for entry in &self.entries {
			if let Some(previous) = previous {
				if entry.branch.0 <= previous.0 {
					return Err(invalid(
						FORMAT,
						"entry order",
						"entries must be strictly ascending by branch id",
					));
				}
			}
			previous = Some(&entry.branch);
			if entry.name.is_empty()
				|| entry.name.len() > MAX_BRANCH_NAME_LEN
				|| entry.name != entry.name.trim()
				|| entry.name.bytes().any(|byte| byte.is_ascii_control())
			{
				return Err(invalid(FORMAT, "entry name", &entry.name));
			}
			match entry.status {
				BranchStatus::Deleted if entry.deleted_at_seq.is_none() => {
					return Err(invalid(
						FORMAT,
						"entry deleted_at_seq",
						"deleted entries must carry deleted_at_seq",
					));
				}
				BranchStatus::Active if entry.deleted_at_seq.is_some() => {
					return Err(invalid(
						FORMAT,
						"entry deleted_at_seq",
						"active entries must not carry deleted_at_seq",
					));
				}
				_ => {}
			}
			if entry.generation.0 >= self.next_generation && entry.generation.0 != 0 {
				return Err(invalid(
					FORMAT,
					"entry generation",
					"generation at or above the allocator watermark",
				));
			}
		}
		Ok(())
	}
}

// ===== branch state =====

impl BranchStateManifest {
	pub(crate) fn encode(&self) -> Result<Vec<u8>> {
		self.validate("encode")?;
		let mut out = Vec::new();
		out.extend_from_slice(&STATE_MAGIC);
		put_u16(&mut out, AUTHORITY_FORMAT_VERSION);
		out.extend_from_slice(&self.branch.0);
		put_u64(&mut out, self.generation.0);
		put_u64(&mut out, self.state_version);
		put_u64(&mut out, self.last_sequence);
		put_u64(&mut out, self.flushed_log_number);
		put_u64(&mut out, self.retained_floor_seq);
		out.push(self.levels.len() as u8);
		for level in &self.levels {
			put_u32(&mut out, level.len() as u32);
			for &table_id in level {
				put_u64(&mut out, table_id);
			}
		}
		Ok(seal_envelope(out))
	}

	pub(crate) fn decode(data: &[u8]) -> Result<Self> {
		const FORMAT: &str = "branch state manifest";
		let mut reader = open_envelope(FORMAT, STATE_MAGIC, data)?;
		let branch = BranchId(reader.array16("branch id")?);
		let generation = BranchGeneration(reader.u64("generation")?);
		let state_version = reader.u64("state_version")?;
		let last_sequence = reader.u64("last_sequence")?;
		let flushed_log_number = reader.u64("flushed_log_number")?;
		let retained_floor_seq = reader.u64("retained_floor_seq")?;
		let level_count = reader.u8("level_count")? as usize;
		let mut levels = Vec::with_capacity(level_count);
		for _ in 0..level_count {
			let table_count = reader.u32("level table count")? as usize;
			if table_count > MAX_TABLES_PER_LEVEL {
				return Err(invalid(
					FORMAT,
					"level table count",
					&format!("{table_count} exceeds cap {MAX_TABLES_PER_LEVEL}"),
				));
			}
			let mut tables = Vec::with_capacity(table_count);
			for _ in 0..table_count {
				tables.push(reader.u64("table id")?);
			}
			levels.push(tables);
		}
		reader.finish(FORMAT)?;
		let manifest = Self {
			branch,
			generation,
			state_version,
			last_sequence,
			flushed_log_number,
			retained_floor_seq,
			levels,
		};
		manifest.validate("decode")?;
		Ok(manifest)
	}

	fn validate(&self, action: &str) -> Result<()> {
		const FORMAT: &str = "branch state manifest";
		if self.state_version == 0 {
			return Err(invalid(FORMAT, "state_version", &format!("0 during {action}")));
		}
		if self.levels.len() > u8::MAX as usize {
			return Err(invalid(FORMAT, "level_count", &self.levels.len().to_string()));
		}
		// No bound is checked against `last_sequence`: the floor may legitimately
		// sit ABOVE every sequence that survives. A bottom-level compaction that
		// drops a key's newest version — a hard-delete tombstone — must raise the
		// floor past that tombstone's sequence, while `last_sequence` only tracks
		// sequences still present in tables.
		let mut seen = std::collections::HashSet::new();
		for level in &self.levels {
			if level.len() > MAX_TABLES_PER_LEVEL {
				return Err(invalid(FORMAT, "level table count", &level.len().to_string()));
			}
			for table_id in level {
				if !seen.insert(*table_id) {
					return Err(invalid(
						FORMAT,
						"table id",
						&format!("{table_id} listed more than once"),
					));
				}
			}
		}
		Ok(())
	}
}

// ===== root =====

impl RootManifest {
	pub(crate) fn encode(&self) -> Result<Vec<u8>> {
		self.validate("encode")?;
		let mut out = Vec::new();
		out.extend_from_slice(&ROOT_MAGIC);
		put_u16(&mut out, AUTHORITY_FORMAT_VERSION);
		out.extend_from_slice(&self.db_id);
		put_u64(&mut out, self.root_version);
		put_u64(&mut out, self.visible_seq);
		put_u64(&mut out, self.last_commit_ts);
		put_u16(&mut out, self.timeline_tail.len() as u16);
		for (commit_ts, seq) in &self.timeline_tail {
			put_u64(&mut out, *commit_ts);
			put_u64(&mut out, *seq);
		}
		put_u64(&mut out, self.wal_reclaim_floor);
		put_u64(&mut out, self.next_table_id);
		put_u64(&mut out, self.catalog_version_floor);
		put_u32(&mut out, self.state_hints.len() as u32);
		for (branch, generation, state_version) in &self.state_hints {
			out.extend_from_slice(&branch.0);
			put_u64(&mut out, generation.0);
			put_u64(&mut out, *state_version);
		}
		Ok(seal_envelope(out))
	}

	pub(crate) fn decode(data: &[u8]) -> Result<Self> {
		const FORMAT: &str = "root manifest";
		let mut reader = open_envelope(FORMAT, ROOT_MAGIC, data)?;
		let db_id = reader.array16("db_id")?;
		let root_version = reader.u64("root_version")?;
		let visible_seq = reader.u64("visible_seq")?;
		let last_commit_ts = reader.u64("last_commit_ts")?;
		let timeline_count = reader.u16("timeline count")? as usize;
		if timeline_count > MAX_TIMELINE_TAIL {
			return Err(invalid(
				FORMAT,
				"timeline count",
				&format!("{timeline_count} exceeds cap {MAX_TIMELINE_TAIL}"),
			));
		}
		let mut timeline_tail = Vec::with_capacity(timeline_count);
		for _ in 0..timeline_count {
			let commit_ts = reader.u64("timeline commit_ts")?;
			let seq = reader.u64("timeline seq")?;
			timeline_tail.push((commit_ts, seq));
		}
		let wal_reclaim_floor = reader.u64("wal_reclaim_floor")?;
		let next_table_id = reader.u64("next_table_id")?;
		let catalog_version_floor = reader.u64("catalog_version_floor")?;
		let hint_count = reader.u32("state hint count")? as usize;
		if hint_count > MAX_STATE_HINTS {
			return Err(invalid(
				FORMAT,
				"state hint count",
				&format!("{hint_count} exceeds cap {MAX_STATE_HINTS}"),
			));
		}
		let mut state_hints = Vec::with_capacity(hint_count);
		for _ in 0..hint_count {
			let branch = BranchId(reader.array16("hint branch id")?);
			let generation = BranchGeneration(reader.u64("hint generation")?);
			let state_version = reader.u64("hint state_version")?;
			state_hints.push((branch, generation, state_version));
		}
		reader.finish(FORMAT)?;
		let manifest = Self {
			db_id,
			root_version,
			visible_seq,
			last_commit_ts,
			timeline_tail,
			wal_reclaim_floor,
			next_table_id,
			catalog_version_floor,
			state_hints,
		};
		manifest.validate("decode")?;
		Ok(manifest)
	}

	fn validate(&self, action: &str) -> Result<()> {
		const FORMAT: &str = "root manifest";
		if self.root_version == 0 {
			return Err(invalid(FORMAT, "root_version", &format!("0 during {action}")));
		}
		if self.timeline_tail.len() > MAX_TIMELINE_TAIL {
			return Err(invalid(FORMAT, "timeline count", &self.timeline_tail.len().to_string()));
		}
		let mut previous: Option<(u64, u64)> = None;
		for &(commit_ts, seq) in &self.timeline_tail {
			if let Some((previous_ts, previous_seq)) = previous {
				if commit_ts <= previous_ts || seq <= previous_seq {
					return Err(invalid(
						FORMAT,
						"timeline order",
						"fenceposts must strictly increase on both axes",
					));
				}
			}
			previous = Some((commit_ts, seq));
		}
		if self.state_hints.len() > MAX_STATE_HINTS {
			return Err(invalid(FORMAT, "state hint count", &self.state_hints.len().to_string()));
		}
		let mut previous_branch: Option<&BranchId> = None;
		for (branch, _, state_version) in &self.state_hints {
			if let Some(previous_branch) = previous_branch {
				if branch.0 <= previous_branch.0 {
					return Err(invalid(
						FORMAT,
						"state hint order",
						"hints must be strictly ascending by branch id",
					));
				}
			}
			previous_branch = Some(branch);
			if *state_version == 0 {
				return Err(invalid(FORMAT, "state hint version", "0"));
			}
		}
		Ok(())
	}
}
