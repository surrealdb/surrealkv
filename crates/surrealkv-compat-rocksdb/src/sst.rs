//! Block-based tables (`NNNNNN.sst`): the footer, the meta blocks, the index (single level or
//! partitioned), and the data blocks, for format versions 2 to 7.

use std::collections::{BTreeMap, VecDeque};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::checksum::{context_modifier, with_last_byte, ChecksumType};
use crate::coding::{fixed32, varint32, varint64, varsigned64};
use crate::compression;
use crate::error::{corrupt, unsupported, Result};

const BLOCK_TRAILER_SIZE: usize = 5;
const FOOTER_SIZE: usize = 53;
const BLOCK_BASED_MAGIC: u64 = 0x88e2_41b7_85f4_cff7;
const LEGACY_MAGIC: u64 = 0xdb47_7524_8b80_fb57;
const EXTENDED_MAGIC: [u8; 4] = [0x3e, 0x00, 0x7a, 0x00];

/// The place of a block in the file: its data, without the trailer that follows it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BlockHandle {
	pub(crate) offset: u64,
	pub(crate) size: u64,
}

impl BlockHandle {
	fn decode(buf: &[u8], pos: &mut usize) -> Result<Self> {
		Ok(Self {
			offset: varint64(buf, pos)?,
			size: varint64(buf, pos)?,
		})
	}
}

/// What the table says about itself.
#[derive(Debug, Default, Clone)]
pub struct Properties {
	pub num_entries: u64,
	pub num_range_deletions: u64,
	pub num_deletions: u64,
	pub comparator: Option<String>,
	pub index_partitions: u64,
	pub index_value_is_delta_encoded: bool,
	/// True unless the table was written without the timestamps in its keys.
	pub user_defined_timestamps_persisted: bool,
}

/// One entry of a data block: an internal key and its value.
pub(crate) struct RawEntry {
	pub(crate) key: Vec<u8>,
	pub(crate) value: Vec<u8>,
}

/// An open table.
#[derive(Clone)]
pub struct TableReader {
	path: PathBuf,
	file_size: u64,
	pub number: u64,
	checksum: ChecksumType,
	base_context_checksum: u32,
	index: BlockHandle,
	pub properties: Properties,
}

fn read_exact_at(file: &mut File, offset: u64, len: usize) -> Result<Vec<u8>> {
	file.seek(SeekFrom::Start(offset))?;
	let mut buf = vec![0u8; len];
	file.read_exact(&mut buf)?;
	Ok(buf)
}

/// Reads the block at `handle`: verifies its checksum and decompresses it.
fn read_block(
	file: &mut File,
	file_size: u64,
	handle: BlockHandle,
	checksum: ChecksumType,
	base_context_checksum: u32,
	what: &str,
) -> Result<Vec<u8>> {
	// A handle comes from the file itself, and in the formats without a footer checksum a damaged
	// one must not be taken for a size to allocate.
	let end = handle
		.offset
		.checked_add(handle.size)
		.and_then(|e| e.checked_add(BLOCK_TRAILER_SIZE as u64));
	if end.is_none_or(|end| end > file_size) {
		return Err(corrupt(format!("{what} lies outside its file of {file_size} bytes")));
	}
	let size = handle.size as usize;
	let raw = read_exact_at(file, handle.offset, size + BLOCK_TRAILER_SIZE)?;
	let compression_type = raw[size];
	let stored = u32::from_le_bytes(raw[size + 1..size + 5].try_into().expect("4"));
	if checksum != ChecksumType::None {
		let computed = with_last_byte(checksum, &raw[..size], compression_type)
			.wrapping_add(context_modifier(base_context_checksum, handle.offset));
		if computed != stored {
			return Err(corrupt(format!(
				"{what} at offset {} fails its checksum (stored {stored:#010x}, computed {computed:#010x})",
				handle.offset
			)));
		}
	}
	compression::decompress(compression_type, &raw[..size])
}

/// A block's entries region, without the restart array.
fn block_entries_region(block: &[u8]) -> Result<&[u8]> {
	if block.len() < 4 {
		return Err(corrupt("a block is shorter than its footer"));
	}
	let packed = u32::from_le_bytes(block[block.len() - 4..].try_into().expect("4"));
	if packed >> 31 != 0 {
		return Err(unsupported("data blocks with a hash index"));
	}
	let restarts = packed as usize;
	let region_end = block
		.len()
		.checked_sub(4 + restarts * 4)
		.ok_or_else(|| corrupt("a block's restart array does not fit in it"))?;
	Ok(&block[..region_end])
}

/// Entries of an ordinary block: `shared | non_shared | value_len | key delta | value`.
fn plain_entries(block: &[u8]) -> Result<Vec<RawEntry>> {
	let region = block_entries_region(block)?;
	let mut out = Vec::new();
	let mut pos = 0;
	let mut key = Vec::new();
	while pos < region.len() {
		let shared = varint32(region, &mut pos)? as usize;
		let non_shared = varint32(region, &mut pos)? as usize;
		let value_len = varint32(region, &mut pos)? as usize;
		if shared > key.len() || pos + non_shared + value_len > region.len() {
			return Err(corrupt("a block entry does not fit in the block"));
		}
		key.truncate(shared);
		key.extend_from_slice(&region[pos..pos + non_shared]);
		pos += non_shared;
		let value = region[pos..pos + value_len].to_vec();
		pos += value_len;
		out.push(RawEntry {
			key: key.clone(),
			value,
		});
	}
	Ok(out)
}

/// The block handles listed by an index block.
///
/// With delta-encoded values (format version 4 and above) the entry has no value length, and the
/// handle is stored whole at a restart (no shared key prefix) and as the change in size otherwise,
/// the offset following from the previous block.
fn index_handles(block: &[u8], delta_encoded: bool) -> Result<Vec<BlockHandle>> {
	if !delta_encoded {
		return plain_entries(block)?
			.into_iter()
			.map(|e| BlockHandle::decode(&e.value, &mut 0))
			.collect();
	}
	let region = block_entries_region(block)?;
	let mut out: Vec<BlockHandle> = Vec::new();
	let mut pos = 0;
	while pos < region.len() {
		let shared = varint32(region, &mut pos)? as usize;
		let non_shared = varint32(region, &mut pos)? as usize;
		if pos + non_shared > region.len() {
			return Err(corrupt("an index entry does not fit in the block"));
		}
		pos += non_shared;
		let handle = if shared == 0 {
			BlockHandle::decode(region, &mut pos)?
		} else {
			let previous = out
				.last()
				.ok_or_else(|| corrupt("an index entry has a delta and nothing before it"))?;
			let delta = varsigned64(region, &mut pos)?;
			let size = (previous.size as i64 + delta) as u64;
			BlockHandle {
				offset: previous.offset + previous.size + BLOCK_TRAILER_SIZE as u64,
				size,
			}
		};
		out.push(handle);
	}
	Ok(out)
}

struct Footer {
	checksum: ChecksumType,
	base_context_checksum: u32,
	metaindex: BlockHandle,
	index: Option<BlockHandle>,
}

fn read_footer(file: &mut File, file_size: u64, path: &Path) -> Result<Footer> {
	let name = path.display();
	if file_size < FOOTER_SIZE as u64 {
		return Err(corrupt(format!("{name} is shorter than a table footer")));
	}
	let footer_offset = file_size - FOOTER_SIZE as u64;
	let buf = read_exact_at(file, footer_offset, FOOTER_SIZE)?;
	let magic = u64::from_le_bytes(buf[FOOTER_SIZE - 8..].try_into().expect("8"));
	if magic == LEGACY_MAGIC {
		return Err(unsupported(format!("{name} is in the legacy table format")));
	}
	if magic != BLOCK_BASED_MAGIC {
		return Err(corrupt(format!("{name} does not end in the block-based table magic number")));
	}
	let format_version =
		u32::from_le_bytes(buf[FOOTER_SIZE - 12..FOOTER_SIZE - 8].try_into().expect("4"));
	if !(2..=7).contains(&format_version) {
		return Err(unsupported(format!(
			"{name} is in table format version {format_version}; this reader handles 2 to 7"
		)));
	}
	let checksum = ChecksumType::from_u8(buf[0])?;
	if format_version >= 6 {
		if buf[1..5] != EXTENDED_MAGIC {
			return Err(corrupt(format!("{name} has a bad extended magic number")));
		}
		let mut pos = 9;
		let base_context_checksum = fixed32(&buf, &mut pos)?;
		let metaindex_size = u64::from(fixed32(&buf, &mut pos)?);
		let end = footer_offset
			.checked_sub(BLOCK_TRAILER_SIZE as u64 + metaindex_size)
			.ok_or_else(|| corrupt(format!("{name} has a metaindex larger than the file")))?;
		Ok(Footer {
			checksum,
			base_context_checksum,
			metaindex: BlockHandle {
				offset: end,
				size: metaindex_size,
			},
			index: None,
		})
	} else {
		let mut pos = 1;
		let metaindex = BlockHandle::decode(&buf, &mut pos)?;
		let index = BlockHandle::decode(&buf, &mut pos)?;
		Ok(Footer {
			checksum,
			base_context_checksum: 0,
			metaindex,
			index: Some(index),
		})
	}
}

fn parse_properties(block: &[u8]) -> Result<Properties> {
	let mut raw: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
	for entry in plain_entries(block)? {
		raw.insert(entry.key, entry.value);
	}
	let number = |name: &str| -> Result<Option<u64>> {
		match raw.get(name.as_bytes()) {
			Some(value) => Ok(Some(varint64(value, &mut 0)?)),
			None => Ok(None),
		}
	};
	Ok(Properties {
		num_entries: number("rocksdb.num.entries")?.unwrap_or(0),
		num_range_deletions: number("rocksdb.num.range-deletions")?.unwrap_or(0),
		num_deletions: number("rocksdb.deleted.keys")?.unwrap_or(0),
		comparator: raw
			.get(b"rocksdb.comparator".as_slice())
			.map(|v| String::from_utf8_lossy(v).into_owned()),
		index_partitions: number("rocksdb.index.partitions")?.unwrap_or(0),
		index_value_is_delta_encoded: number("rocksdb.index.value.is.delta.encoded")?.unwrap_or(0)
			!= 0,
		// Only written when false.
		user_defined_timestamps_persisted: number("rocksdb.user.defined.timestamps.persisted")?
			.unwrap_or(1)
			!= 0,
	})
}

impl TableReader {
	pub fn open(path: &Path, number: u64) -> Result<Self> {
		let mut file = File::open(path)?;
		let file_size = file.metadata()?.len();
		let footer = read_footer(&mut file, file_size, path)?;
		let what = |kind: &str| format!("the {kind} block of {}", path.display());

		let metaindex_block = read_block(
			&mut file,
			file_size,
			footer.metaindex,
			footer.checksum,
			footer.base_context_checksum,
			&what("metaindex"),
		)?;
		let mut meta: BTreeMap<String, BlockHandle> = BTreeMap::new();
		for entry in plain_entries(&metaindex_block)? {
			meta.insert(
				String::from_utf8_lossy(&entry.key).into_owned(),
				BlockHandle::decode(&entry.value, &mut 0)?,
			);
		}
		if meta.contains_key("rocksdb.compression_dict") {
			return Err(unsupported(format!("{} uses a compression dictionary", path.display())));
		}
		let properties = match meta.get("rocksdb.properties") {
			Some(handle) => parse_properties(&read_block(
				&mut file,
				file_size,
				*handle,
				footer.checksum,
				footer.base_context_checksum,
				&what("properties"),
			)?)?,
			None => return Err(corrupt(format!("{} has no properties block", path.display()))),
		};
		if properties.num_range_deletions != 0 {
			return Err(unsupported(format!(
				"{} holds {} range deletions",
				path.display(),
				properties.num_range_deletions
			)));
		}
		if !properties.user_defined_timestamps_persisted {
			return Err(unsupported(format!(
				"{} was written without its user-defined timestamps",
				path.display()
			)));
		}
		let index = match footer.index {
			Some(handle) => handle,
			None => *meta
				.get("rocksdb.index")
				.ok_or_else(|| corrupt(format!("{} lists no index block", path.display())))?,
		};
		Ok(Self {
			path: path.to_path_buf(),
			file_size,
			number,
			checksum: footer.checksum,
			base_context_checksum: footer.base_context_checksum,
			index,
			properties,
		})
	}

	/// The entries of the table, in the order it keeps them.
	pub(crate) fn entries(&self) -> Result<TableEntries> {
		let mut file = File::open(&self.path)?;
		let delta = self.properties.index_value_is_delta_encoded;
		let index_block = read_block(
			&mut file,
			self.file_size,
			self.index,
			self.checksum,
			self.base_context_checksum,
			&format!("the index of {}", self.path.display()),
		)?;
		let top = index_handles(&index_block, delta)?;
		let partitioned = self.properties.index_partitions > 0;
		Ok(TableEntries {
			file,
			file_size: self.file_size,
			path: self.path.clone(),
			checksum: self.checksum,
			base_context_checksum: self.base_context_checksum,
			delta,
			partitions: if partitioned {
				top.clone().into()
			} else {
				VecDeque::new()
			},
			data_blocks: if partitioned {
				VecDeque::new()
			} else {
				top.into()
			},
			current: Vec::new().into_iter(),
		})
	}
}

/// An iterator over the entries of one table.
pub(crate) struct TableEntries {
	file: File,
	file_size: u64,
	path: PathBuf,
	checksum: ChecksumType,
	base_context_checksum: u32,
	delta: bool,
	partitions: VecDeque<BlockHandle>,
	data_blocks: VecDeque<BlockHandle>,
	current: std::vec::IntoIter<RawEntry>,
}

impl TableEntries {
	fn advance(&mut self) -> Result<bool> {
		loop {
			if let Some(handle) = self.data_blocks.pop_front() {
				let block = read_block(
					&mut self.file,
					self.file_size,
					handle,
					self.checksum,
					self.base_context_checksum,
					&format!("a data block of {}", self.path.display()),
				)?;
				self.current = plain_entries(&block)?.into_iter();
				return Ok(true);
			}
			let Some(partition) = self.partitions.pop_front() else {
				return Ok(false);
			};
			let block = read_block(
				&mut self.file,
				self.file_size,
				partition,
				self.checksum,
				self.base_context_checksum,
				&format!("an index partition of {}", self.path.display()),
			)?;
			self.data_blocks = index_handles(&block, self.delta)?.into();
		}
	}
}

impl Iterator for TableEntries {
	type Item = Result<RawEntry>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			if let Some(entry) = self.current.next() {
				return Some(Ok(entry));
			}
			match self.advance() {
				Ok(true) => {}
				Ok(false) => return None,
				Err(e) => return Some(Err(e)),
			}
		}
	}
}
