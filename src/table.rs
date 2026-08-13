//! Branch-pure immutable table format and ranged reader.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use crc32fast::hash as crc32;
use parking_lot::Mutex;
use sha2::{Digest, Sha256};
use xxhash_rust::xxh3::xxh3_64_with_seed;

use super::api::{
	BranchGeneration, BranchId, CommitTimestamp, CommitVersion, ErrorCode, KernelError,
	KernelResult, TableId,
};
use super::format::{compare_internal, StorageRow, FORMAT_MAGIC, FORMAT_VERSION, MAX_USER_KEY_LEN};
use super::storage::{ByteRange, ObjectBody, ObjectId, ObjectStore, Platform};

const HEADER_LEN: usize = 72;
const FOOTER_LEN: usize = 144;
const FOOTER_MAGIC: [u8; 8] = *b"SKVFOOT1";
const MAX_SECTION_LEN: usize = 96 * 1024 * 1024;
const MAX_PREFETCH_LEN: u64 = 256 * 1024;
const BLOOM_BITS_PER_KEY: usize = 10;
const BLOOM_HASHES: u8 = 6;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TableOwner {
	pub(crate) branch: BranchId,
	pub(crate) generation: BranchGeneration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TableDescriptor {
	pub(crate) table_id: TableId,
	pub(crate) owner: TableOwner,
	pub(crate) smallest_user_key: Bytes,
	pub(crate) largest_user_key: Bytes,
	pub(crate) smallest_version: CommitVersion,
	pub(crate) largest_version: CommitVersion,
	pub(crate) object_len: u64,
	pub(crate) digest: [u8; 32],
	pub(crate) format_version: u16,
	pub(crate) max_commit_timestamp: CommitTimestamp,
	pub(crate) row_count: u64,
}

impl TableDescriptor {
	pub(crate) fn object_id(&self) -> ObjectId {
		ObjectId(format!("tables/{}", hex(&self.table_id.0)))
	}
}

pub(crate) fn encode_descriptor(descriptor: &TableDescriptor) -> KernelResult<Bytes> {
	let mut output = BytesMut::new();
	output.extend_from_slice(&descriptor.table_id.0);
	output.extend_from_slice(&descriptor.owner.branch.0);
	output.put_u64(descriptor.owner.generation.0);
	for key in [&descriptor.smallest_user_key, &descriptor.largest_user_key] {
		output.put_u32(u32::try_from(key.len()).map_err(|_| {
			KernelError::new(ErrorCode::ResourceExhausted, "descriptor key exceeds format")
		})?);
		output.extend_from_slice(key);
	}
	output.put_u64(descriptor.smallest_version.0);
	output.put_u64(descriptor.largest_version.0);
	output.put_u64(descriptor.object_len);
	output.extend_from_slice(&descriptor.digest);
	output.put_u16(descriptor.format_version);
	output.put_u64(descriptor.max_commit_timestamp.0);
	output.put_u64(descriptor.row_count);
	Ok(output.freeze())
}

pub(crate) fn decode_descriptor(bytes: Bytes) -> KernelResult<TableDescriptor> {
	let mut cursor = SliceCursor::new(bytes);
	let mut table_id = [0; 32];
	table_id.copy_from_slice(&cursor.bytes(32)?);
	let mut branch = [0; 16];
	branch.copy_from_slice(&cursor.bytes(16)?);
	let generation = BranchGeneration(cursor.u64()?);
	let mut key = || -> KernelResult<Bytes> {
		let len = cursor.u32()? as usize;
		if len > MAX_USER_KEY_LEN {
			return Err(corruption("descriptor key exceeds limit"));
		}
		cursor.bytes(len)
	};
	let smallest_user_key = key()?;
	let largest_user_key = key()?;
	let smallest_version = CommitVersion(cursor.u64()?);
	let largest_version = CommitVersion(cursor.u64()?);
	let object_len = cursor.u64()?;
	let mut digest = [0; 32];
	digest.copy_from_slice(&cursor.bytes(32)?);
	let format_version = cursor.u16()?;
	let max_commit_timestamp = CommitTimestamp(cursor.u64()?);
	let row_count = cursor.u64()?;
	cursor.finish()?;
	if smallest_user_key > largest_user_key
		|| smallest_version > largest_version
		|| object_len < (HEADER_LEN + FOOTER_LEN) as u64
		|| format_version != FORMAT_VERSION
		|| row_count == 0
	{
		return Err(corruption("invalid table descriptor bounds"));
	}
	Ok(TableDescriptor {
		table_id: TableId(table_id),
		owner: TableOwner {
			branch: BranchId(branch),
			generation,
		},
		smallest_user_key,
		largest_user_key,
		smallest_version,
		largest_version,
		object_len,
		digest,
		format_version,
		max_commit_timestamp,
		row_count,
	})
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BlockHandle {
	offset: u64,
	len: u32,
}

#[derive(Clone, Debug)]
struct IndexEntry {
	last_user_key: Bytes,
	handle: BlockHandle,
}

#[derive(Clone, Copy)]
struct SectionHandle {
	offset: u64,
	len: u64,
}

#[derive(Clone)]
struct Footer {
	index: SectionHandle,
	filter: SectionHandle,
	properties: SectionHandle,
	owner: TableOwner,
	table_id: TableId,
	smallest_version: CommitVersion,
	largest_version: CommitVersion,
	max_commit_timestamp: CommitTimestamp,
}

pub(crate) struct BuiltTable {
	pub(crate) descriptor: TableDescriptor,
	pub(crate) body: ObjectBody,
}

pub(crate) struct TableBuilder {
	owner: TableOwner,
	table_id: TableId,
	block_target: usize,
	current: BytesMut,
	current_last_key: Option<Bytes>,
	blocks: Vec<Bytes>,
	index: Vec<IndexEntry>,
	bloom_keys: Vec<Bytes>,
	last_key: Option<super::format::InternalKey>,
	smallest_user_key: Option<Bytes>,
	largest_user_key: Option<Bytes>,
	smallest_version: CommitVersion,
	largest_version: CommitVersion,
	max_commit_timestamp: CommitTimestamp,
	row_count: u64,
	data_len: u64,
}

impl TableBuilder {
	pub(crate) fn new(
		owner: TableOwner,
		table_id: TableId,
		block_target: usize,
	) -> KernelResult<Self> {
		if block_target == 0 || block_target > MAX_SECTION_LEN {
			return Err(KernelError::new(ErrorCode::InvalidArgument, "invalid table block target"));
		}
		Ok(Self {
			owner,
			table_id,
			block_target,
			current: BytesMut::with_capacity(block_target),
			current_last_key: None,
			blocks: Vec::new(),
			index: Vec::new(),
			bloom_keys: Vec::new(),
			last_key: None,
			smallest_user_key: None,
			largest_user_key: None,
			smallest_version: CommitVersion(u64::MAX),
			largest_version: CommitVersion(0),
			max_commit_timestamp: CommitTimestamp(0),
			row_count: 0,
			data_len: 0,
		})
	}

	pub(crate) fn with_generated_id(
		owner: TableOwner,
		platform: &dyn Platform,
		block_target: usize,
	) -> KernelResult<Self> {
		let mut id = [0; 32];
		platform.fill_random(&mut id)?;
		Self::new(owner, TableId(id), block_target)
	}

	pub(crate) fn add(&mut self, owner: TableOwner, row: StorageRow) -> KernelResult<()> {
		if owner != self.owner {
			return Err(KernelError::new(
				ErrorCode::InvalidArgument,
				"table cannot mix physical owners",
			));
		}
		if let Some(previous) = &self.last_key {
			if previous.user_key == row.key.user_key && previous.version == row.key.version {
				return Err(KernelError::new(
					ErrorCode::InvalidArgument,
					"table contains duplicate user-key/version",
				));
			}
			if compare_internal(previous, &row.key).is_ge() {
				return Err(KernelError::new(
					ErrorCode::InvalidArgument,
					"table rows must be strictly sorted",
				));
			}
		}
		let encoded = row.encode()?;
		let record_len = 4usize.checked_add(encoded.len()).ok_or_else(|| {
			KernelError::new(ErrorCode::ResourceExhausted, "table row is too large")
		})?;
		if record_len > MAX_SECTION_LEN {
			return Err(KernelError::new(
				ErrorCode::ResourceExhausted,
				"encoded row exceeds block limit",
			));
		}
		if !self.current.is_empty()
			&& self.current.len().checked_add(record_len).is_none_or(|len| len > self.block_target)
		{
			self.flush_block()?;
		}
		let encoded_len = u32::try_from(encoded.len())
			.map_err(|_| KernelError::new(ErrorCode::ResourceExhausted, "row is too large"))?;
		self.current.put_u32(encoded_len);
		self.current.extend_from_slice(&encoded);
		self.current_last_key = Some(row.key.user_key.clone());
		if self.bloom_keys.last().is_none_or(|previous| previous != &row.key.user_key) {
			self.bloom_keys.push(row.key.user_key.clone());
		}
		self.smallest_user_key.get_or_insert_with(|| row.key.user_key.clone());
		self.largest_user_key = Some(row.key.user_key.clone());
		self.smallest_version = self.smallest_version.min(row.key.version);
		self.largest_version = self.largest_version.max(row.key.version);
		self.max_commit_timestamp = self.max_commit_timestamp.max(row.commit_timestamp);
		self.last_key = Some(row.key);
		self.row_count = self
			.row_count
			.checked_add(1)
			.ok_or_else(|| KernelError::new(ErrorCode::ResourceExhausted, "too many rows"))?;
		Ok(())
	}

	pub(crate) fn finish(mut self) -> KernelResult<BuiltTable> {
		if self.row_count == 0 {
			return Err(KernelError::new(ErrorCode::InvalidArgument, "empty tables are forbidden"));
		}
		self.flush_block()?;

		let header = encode_header(self.owner, self.table_id);
		let mut chunks = Vec::with_capacity(self.blocks.len() + 5);
		chunks.push(header);
		chunks.extend(self.blocks);
		let mut offset = HEADER_LEN as u64 + self.data_len;

		let index = encode_index(&self.index)?;
		let index_handle = section(&mut offset, index.len())?;
		chunks.push(index);
		let filter = encode_filter(&self.bloom_keys)?;
		let filter_handle = section(&mut offset, filter.len())?;
		chunks.push(filter);
		let properties = encode_properties(
			self.row_count,
			self.smallest_user_key.as_ref().unwrap(),
			self.largest_user_key.as_ref().unwrap(),
			self.smallest_version,
			self.largest_version,
			self.max_commit_timestamp,
		)?;
		let properties_handle = section(&mut offset, properties.len())?;
		chunks.push(properties);

		let footer = Footer {
			index: index_handle,
			filter: filter_handle,
			properties: properties_handle,
			owner: self.owner,
			table_id: self.table_id,
			smallest_version: self.smallest_version,
			largest_version: self.largest_version,
			max_commit_timestamp: self.max_commit_timestamp,
		};
		chunks.push(encode_footer(&footer));
		offset = offset
			.checked_add(FOOTER_LEN as u64)
			.ok_or_else(|| KernelError::new(ErrorCode::ResourceExhausted, "table is too large"))?;

		let mut digest = Sha256::new();
		for chunk in &chunks {
			digest.update(chunk);
		}
		let digest: [u8; 32] = digest.finalize().into();
		let descriptor = TableDescriptor {
			table_id: self.table_id,
			owner: self.owner,
			smallest_user_key: self.smallest_user_key.unwrap(),
			largest_user_key: self.largest_user_key.unwrap(),
			smallest_version: self.smallest_version,
			largest_version: self.largest_version,
			object_len: offset,
			digest,
			format_version: FORMAT_VERSION,
			max_commit_timestamp: self.max_commit_timestamp,
			row_count: self.row_count,
		};
		Ok(BuiltTable {
			descriptor,
			body: ObjectBody::new(chunks)?,
		})
	}

	fn flush_block(&mut self) -> KernelResult<()> {
		if self.current.is_empty() {
			return Ok(());
		}
		let raw = self.current.split().freeze();
		let mut encoded = lz4_flex::compress_prepend_size(&raw);
		encoded.extend_from_slice(&crc32(&encoded).to_be_bytes());
		if encoded.len() > MAX_SECTION_LEN {
			return Err(KernelError::new(
				ErrorCode::ResourceExhausted,
				"compressed block exceeds limit",
			));
		}
		let encoded = Bytes::from(encoded);
		let len = u32::try_from(encoded.len())
			.map_err(|_| KernelError::new(ErrorCode::ResourceExhausted, "block is too large"))?;
		let handle = BlockHandle {
			offset: HEADER_LEN as u64 + self.data_len,
			len,
		};
		self.data_len = self
			.data_len
			.checked_add(len as u64)
			.ok_or_else(|| KernelError::new(ErrorCode::ResourceExhausted, "table is too large"))?;
		self.index.push(IndexEntry {
			last_user_key: self.current_last_key.take().unwrap(),
			handle,
		});
		self.blocks.push(encoded);
		Ok(())
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CacheKey {
	table: TableId,
	offset: u64,
}

struct CachedBlock {
	bytes: Bytes,
	checksum: u32,
}

struct CacheState {
	entries: BTreeMap<CacheKey, CachedBlock>,
	order: VecDeque<CacheKey>,
	bytes: usize,
}

pub(crate) struct BlockCache {
	capacity: usize,
	state: Mutex<CacheState>,
}

impl BlockCache {
	pub(crate) fn new(capacity: usize) -> Self {
		Self {
			capacity,
			state: Mutex::new(CacheState {
				entries: BTreeMap::new(),
				order: VecDeque::new(),
				bytes: 0,
			}),
		}
	}

	fn get(&self, key: CacheKey) -> Option<Bytes> {
		let mut state = self.state.lock();
		let valid =
			state.entries.get(&key).is_some_and(|entry| crc32(&entry.bytes) == entry.checksum);
		if !valid {
			if let Some(removed) = state.entries.remove(&key) {
				state.bytes = state.bytes.saturating_sub(removed.bytes.len());
			}
			state.order.retain(|candidate| *candidate != key);
			return None;
		}
		let bytes = state.entries[&key].bytes.clone();
		state.order.retain(|candidate| *candidate != key);
		state.order.push_back(key);
		Some(bytes)
	}

	fn insert(&self, key: CacheKey, bytes: Bytes) {
		if self.capacity == 0 || bytes.len() > self.capacity {
			return;
		}
		let mut state = self.state.lock();
		if let Some(previous) = state.entries.remove(&key) {
			state.bytes = state.bytes.saturating_sub(previous.bytes.len());
		}
		state.order.retain(|candidate| *candidate != key);
		while state.bytes + bytes.len() > self.capacity {
			let Some(oldest) = state.order.pop_front() else {
				break;
			};
			if let Some(removed) = state.entries.remove(&oldest) {
				state.bytes = state.bytes.saturating_sub(removed.bytes.len());
			}
		}
		let checksum = crc32(&bytes);
		state.bytes += bytes.len();
		state.entries.insert(
			key,
			CachedBlock {
				bytes,
				checksum,
			},
		);
		state.order.push_back(key);
	}

	#[cfg(test)]
	fn corrupt(&self, table: TableId, offset: u64) -> bool {
		let mut state = self.state.lock();
		let Some(entry) = state.entries.get_mut(&CacheKey {
			table,
			offset,
		}) else {
			return false;
		};
		let mut bytes = BytesMut::from(entry.bytes.as_ref());
		if bytes.is_empty() {
			return false;
		}
		bytes[0] ^= 0xff;
		entry.bytes = bytes.freeze();
		true
	}
}

pub(crate) struct TableReader {
	store: Arc<dyn ObjectStore>,
	descriptor: TableDescriptor,
	index: Vec<IndexEntry>,
	filter: BloomFilter,
	cache: Arc<BlockCache>,
}

impl TableReader {
	pub(crate) async fn open(
		store: Arc<dyn ObjectStore>,
		descriptor: TableDescriptor,
		cache: Arc<BlockCache>,
	) -> KernelResult<Self> {
		if descriptor.format_version != FORMAT_VERSION {
			return Err(corruption("unsupported table descriptor version"));
		}
		let metadata = store.metadata(&descriptor.object_id()).await?;
		if metadata.len != descriptor.object_len || metadata.len < (HEADER_LEN + FOOTER_LEN) as u64
		{
			return Err(corruption("table object length mismatch"));
		}
		let expected_digest = hex(&descriptor.digest);
		if metadata.attributes.get("sha256").map(String::as_str) != Some(expected_digest.as_str()) {
			return Err(corruption("table digest metadata mismatch"));
		}
		let header = store
			.read_range(&descriptor.object_id(), ByteRange::new(0, HEADER_LEN as u64)?)
			.await?;
		decode_header(header, descriptor.owner, descriptor.table_id)?;
		let footer_start = descriptor.object_len - FOOTER_LEN as u64;
		let footer = decode_footer(
			store
				.read_range(
					&descriptor.object_id(),
					ByteRange::new(footer_start, descriptor.object_len)?,
				)
				.await?,
		)?;
		validate_footer(&footer, &descriptor, footer_start)?;

		let index = decode_index(read_section(&*store, &descriptor, footer.index).await?)?;
		let filter = decode_filter(read_section(&*store, &descriptor, footer.filter).await?)?;
		let properties =
			decode_properties(read_section(&*store, &descriptor, footer.properties).await?)?;
		validate_properties(&properties, &descriptor)?;
		validate_index(&index, footer.index.offset)?;
		Ok(Self {
			store,
			descriptor,
			index,
			filter,
			cache,
		})
	}

	pub(crate) async fn get(
		&self,
		user_key: &[u8],
		max_version: CommitVersion,
	) -> KernelResult<Option<StorageRow>> {
		if !self.filter.may_contain(user_key) {
			return Ok(None);
		}
		let mut started = false;
		for entry in &self.index {
			if !started && entry.last_user_key.as_ref() < user_key {
				continue;
			}
			started = true;
			for row in self.read_block(entry).await? {
				match row.key.user_key.as_ref().cmp(user_key) {
					std::cmp::Ordering::Less => {}
					std::cmp::Ordering::Equal if row.key.version <= max_version => {
						return Ok(Some(row));
					}
					std::cmp::Ordering::Equal => {}
					std::cmp::Ordering::Greater => return Ok(None),
				}
			}
		}
		Ok(None)
	}

	/// Returns physical rows in internal-key order. Visibility, tombstone, and
	/// TTL policy belongs to the P3 read spine, not the table container.
	pub(crate) async fn scan_rows(
		&self,
		start: &[u8],
		end: &[u8],
		max_version: CommitVersion,
	) -> KernelResult<Vec<StorageRow>> {
		if start > end {
			return Err(KernelError::new(ErrorCode::InvalidArgument, "invalid table scan range"));
		}
		if start == end {
			return Ok(Vec::new());
		}
		let mut selected = Vec::new();
		for entry in &self.index {
			if selected.is_empty() && entry.last_user_key.as_ref() < start {
				continue;
			}
			selected.push(entry);
			if entry.last_user_key.as_ref() >= end {
				break;
			}
		}

		let mut output = Vec::new();
		let mut first = 0;
		while first < selected.len() {
			let mut last = first + 1;
			let range_start = selected[first].handle.offset;
			let mut range_end = range_start + selected[first].handle.len as u64;
			while last < selected.len() {
				let candidate_end = selected[last]
					.handle
					.offset
					.checked_add(selected[last].handle.len as u64)
					.ok_or_else(|| corruption("prefetch range overflow"))?;
				if candidate_end - range_start > MAX_PREFETCH_LEN {
					break;
				}
				range_end = candidate_end;
				last += 1;
			}
			let combined = self
				.store
				.read_range(&self.descriptor.object_id(), ByteRange::new(range_start, range_end)?)
				.await?;
			for entry in &selected[first..last] {
				let relative = usize::try_from(entry.handle.offset - range_start)
					.map_err(|_| corruption("prefetch offset exceeds address space"))?;
				let block_end = relative
					.checked_add(entry.handle.len as usize)
					.ok_or_else(|| corruption("prefetched block range overflow"))?;
				if block_end > combined.len() {
					return Err(corruption("prefetched block is truncated"));
				}
				let raw = decode_block(combined.slice(relative..block_end))?;
				let key = CacheKey {
					table: self.descriptor.table_id,
					offset: entry.handle.offset,
				};
				self.cache.insert(key, raw.clone());
				for row in self.validate_rows(entry, raw)? {
					if row.key.user_key.as_ref() >= start
						&& row.key.user_key.as_ref() < end
						&& row.key.version <= max_version
					{
						output.push(row);
					}
				}
			}
			first = last;
		}
		Ok(output)
	}

	async fn read_block(&self, entry: &IndexEntry) -> KernelResult<Vec<StorageRow>> {
		let handle = entry.handle;
		let key = CacheKey {
			table: self.descriptor.table_id,
			offset: handle.offset,
		};
		let raw = if let Some(raw) = self.cache.get(key) {
			raw
		} else {
			let end = handle
				.offset
				.checked_add(handle.len as u64)
				.ok_or_else(|| corruption("block range overflow"))?;
			let encoded = self
				.store
				.read_range(&self.descriptor.object_id(), ByteRange::new(handle.offset, end)?)
				.await?;
			let raw = decode_block(encoded)?;
			self.cache.insert(key, raw.clone());
			raw
		};
		self.validate_rows(entry, raw)
	}

	fn validate_rows(&self, entry: &IndexEntry, raw: Bytes) -> KernelResult<Vec<StorageRow>> {
		let rows = decode_rows(raw)?;
		if rows.last().map(|row| &row.key.user_key) != Some(&entry.last_user_key)
			|| rows.iter().any(|row| {
				row.key.user_key < self.descriptor.smallest_user_key
					|| row.key.user_key > self.descriptor.largest_user_key
					|| row.key.version < self.descriptor.smallest_version
					|| row.key.version > self.descriptor.largest_version
					|| row.commit_timestamp > self.descriptor.max_commit_timestamp
			}) {
			return Err(corruption("data block contradicts table descriptor or index"));
		}
		Ok(rows)
	}
}

struct Properties {
	row_count: u64,
	smallest_user_key: Bytes,
	largest_user_key: Bytes,
	smallest_version: CommitVersion,
	largest_version: CommitVersion,
	max_commit_timestamp: CommitTimestamp,
}

struct BloomFilter {
	bits: Bytes,
	bit_count: usize,
	hashes: u8,
}

impl BloomFilter {
	fn may_contain(&self, key: &[u8]) -> bool {
		if self.bit_count == 0 {
			return false;
		}
		(0..self.hashes).all(|seed| {
			let bit = xxh3_64_with_seed(key, seed as u64) as usize % self.bit_count;
			self.bits[bit / 8] & (1 << (bit % 8)) != 0
		})
	}
}

fn encode_header(owner: TableOwner, table_id: TableId) -> Bytes {
	let mut output = BytesMut::with_capacity(HEADER_LEN);
	output.extend_from_slice(&FORMAT_MAGIC);
	output.put_u16(FORMAT_VERSION);
	output.put_u16(HEADER_LEN as u16);
	output.extend_from_slice(&owner.branch.0);
	output.put_u64(owner.generation.0);
	output.extend_from_slice(&table_id.0);
	let checksum = crc32(&output);
	output.put_u32(checksum);
	output.freeze()
}

fn decode_header(bytes: Bytes, owner: TableOwner, table_id: TableId) -> KernelResult<()> {
	if bytes.len() != HEADER_LEN || bytes[..8] != FORMAT_MAGIC {
		return Err(corruption("invalid table header"));
	}
	if read_u16(&bytes, 8)? != FORMAT_VERSION || read_u16(&bytes, 10)? as usize != HEADER_LEN {
		return Err(corruption("unsupported table header version"));
	}
	if crc32(&bytes[..HEADER_LEN - 4]) != read_u32(&bytes, HEADER_LEN - 4)? {
		return Err(corruption("table header checksum mismatch"));
	}
	if bytes[12..28] != owner.branch.0
		|| read_u64(&bytes, 28)? != owner.generation.0
		|| bytes[36..68] != table_id.0
	{
		return Err(corruption("table header ownership mismatch"));
	}
	Ok(())
}

fn encode_footer(footer: &Footer) -> Bytes {
	let mut output = BytesMut::with_capacity(FOOTER_LEN);
	output.extend_from_slice(&FOOTER_MAGIC);
	output.put_u16(FORMAT_VERSION);
	output.put_u16(FOOTER_LEN as u16);
	for handle in [footer.index, footer.filter, footer.properties] {
		output.put_u64(handle.offset);
		output.put_u64(handle.len);
	}
	output.extend_from_slice(&footer.owner.branch.0);
	output.put_u64(footer.owner.generation.0);
	output.extend_from_slice(&footer.table_id.0);
	output.put_u64(footer.smallest_version.0);
	output.put_u64(footer.largest_version.0);
	output.put_u64(footer.max_commit_timestamp.0);
	let checksum = crc32(&output);
	output.put_u32(checksum);
	output.freeze()
}

fn decode_footer(bytes: Bytes) -> KernelResult<Footer> {
	if bytes.len() != FOOTER_LEN || bytes[..8] != FOOTER_MAGIC {
		return Err(corruption("invalid table footer"));
	}
	if read_u16(&bytes, 8)? != FORMAT_VERSION || read_u16(&bytes, 10)? as usize != FOOTER_LEN {
		return Err(corruption("unsupported table footer version"));
	}
	if crc32(&bytes[..FOOTER_LEN - 4]) != read_u32(&bytes, FOOTER_LEN - 4)? {
		return Err(corruption("table footer checksum mismatch"));
	}
	let handle = |offset| -> KernelResult<SectionHandle> {
		Ok(SectionHandle {
			offset: read_u64(&bytes, offset)?,
			len: read_u64(&bytes, offset + 8)?,
		})
	};
	let mut branch = [0; 16];
	branch.copy_from_slice(&bytes[60..76]);
	let mut table = [0; 32];
	table.copy_from_slice(&bytes[84..116]);
	Ok(Footer {
		index: handle(12)?,
		filter: handle(28)?,
		properties: handle(44)?,
		owner: TableOwner {
			branch: BranchId(branch),
			generation: BranchGeneration(read_u64(&bytes, 76)?),
		},
		table_id: TableId(table),
		smallest_version: CommitVersion(read_u64(&bytes, 116)?),
		largest_version: CommitVersion(read_u64(&bytes, 124)?),
		max_commit_timestamp: CommitTimestamp(read_u64(&bytes, 132)?),
	})
}

fn validate_footer(
	footer: &Footer,
	descriptor: &TableDescriptor,
	footer_start: u64,
) -> KernelResult<()> {
	if footer.owner != descriptor.owner
		|| footer.table_id != descriptor.table_id
		|| footer.smallest_version != descriptor.smallest_version
		|| footer.largest_version != descriptor.largest_version
		|| footer.max_commit_timestamp != descriptor.max_commit_timestamp
	{
		return Err(corruption("footer descriptor mismatch"));
	}
	let mut prior = HEADER_LEN as u64;
	for handle in [footer.index, footer.filter, footer.properties] {
		if handle.len == 0
			|| handle.len > MAX_SECTION_LEN as u64
			|| handle.offset < prior
			|| handle.offset.checked_add(handle.len).is_none_or(|end| end > footer_start)
		{
			return Err(corruption("invalid table section range"));
		}
		prior = handle.offset + handle.len;
	}
	if prior != footer_start {
		return Err(corruption("table contains unaccounted trailing section bytes"));
	}
	Ok(())
}

fn encode_index(entries: &[IndexEntry]) -> KernelResult<Bytes> {
	let mut output = BytesMut::new();
	output
		.put_u32(u32::try_from(entries.len()).map_err(|_| {
			KernelError::new(ErrorCode::ResourceExhausted, "too many table blocks")
		})?);
	for entry in entries {
		output.put_u32(u32::try_from(entry.last_user_key.len()).map_err(|_| {
			KernelError::new(ErrorCode::ResourceExhausted, "index key is too large")
		})?);
		output.extend_from_slice(&entry.last_user_key);
		output.put_u64(entry.handle.offset);
		output.put_u32(entry.handle.len);
	}
	append_checksum(output)
}

fn decode_index(bytes: Bytes) -> KernelResult<Vec<IndexEntry>> {
	let body = checked_section(bytes)?;
	let mut cursor = SliceCursor::new(body);
	let count = cursor.u32()? as usize;
	if count > MAX_SECTION_LEN / 16 {
		return Err(corruption("index entry count exceeds limit"));
	}
	let mut entries = Vec::with_capacity(count);
	for _ in 0..count {
		let key_len = cursor.u32()? as usize;
		if key_len > MAX_USER_KEY_LEN {
			return Err(corruption("index key exceeds limit"));
		}
		entries.push(IndexEntry {
			last_user_key: cursor.bytes(key_len)?,
			handle: BlockHandle {
				offset: cursor.u64()?,
				len: cursor.u32()?,
			},
		});
	}
	cursor.finish()?;
	Ok(entries)
}

fn validate_index(entries: &[IndexEntry], data_end: u64) -> KernelResult<()> {
	if entries.is_empty() {
		return Err(corruption("table index is empty"));
	}
	let mut prior_end = HEADER_LEN as u64;
	let mut prior_key: Option<&Bytes> = None;
	for entry in entries {
		if prior_key.is_some_and(|key| key > &entry.last_user_key)
			|| entry.handle.len == 0
			|| entry.handle.len as usize > MAX_SECTION_LEN
			|| entry.handle.offset != prior_end
		{
			return Err(corruption("invalid table block index"));
		}
		prior_end = entry
			.handle
			.offset
			.checked_add(entry.handle.len as u64)
			.ok_or_else(|| corruption("block index overflow"))?;
		if prior_end > data_end {
			return Err(corruption("block index exceeds data region"));
		}
		prior_key = Some(&entry.last_user_key);
	}
	if prior_end != data_end {
		return Err(corruption("block index does not cover data region"));
	}
	Ok(())
}

fn encode_filter(keys: &[Bytes]) -> KernelResult<Bytes> {
	let bit_count = keys.len().saturating_mul(BLOOM_BITS_PER_KEY).max(64);
	let byte_count = bit_count.div_ceil(8);
	if byte_count > MAX_SECTION_LEN {
		return Err(KernelError::new(ErrorCode::ResourceExhausted, "filter exceeds limit"));
	}
	let mut bits = vec![0u8; byte_count];
	for key in keys {
		for seed in 0..BLOOM_HASHES {
			let bit = xxh3_64_with_seed(key, seed as u64) as usize % bit_count;
			bits[bit / 8] |= 1 << (bit % 8);
		}
	}
	let mut output = BytesMut::with_capacity(9 + byte_count + 4);
	output.put_u32(u32::try_from(bit_count).map_err(|_| {
		KernelError::new(ErrorCode::ResourceExhausted, "filter bit count exceeds format")
	})?);
	output.put_u8(BLOOM_HASHES);
	output.put_u32(u32::try_from(byte_count).map_err(|_| {
		KernelError::new(ErrorCode::ResourceExhausted, "filter byte count exceeds format")
	})?);
	output.extend_from_slice(&bits);
	append_checksum(output)
}

fn decode_filter(bytes: Bytes) -> KernelResult<BloomFilter> {
	let body = checked_section(bytes)?;
	let mut cursor = SliceCursor::new(body);
	let bit_count = cursor.u32()? as usize;
	let hashes = cursor.u8()?;
	let byte_count = cursor.u32()? as usize;
	if bit_count == 0
		|| hashes == 0
		|| hashes > 16
		|| byte_count != bit_count.div_ceil(8)
		|| byte_count > MAX_SECTION_LEN
	{
		return Err(corruption("invalid bloom filter bounds"));
	}
	let bits = cursor.bytes(byte_count)?;
	cursor.finish()?;
	Ok(BloomFilter {
		bits,
		bit_count,
		hashes,
	})
}

fn encode_properties(
	row_count: u64,
	smallest: &Bytes,
	largest: &Bytes,
	smallest_version: CommitVersion,
	largest_version: CommitVersion,
	max_timestamp: CommitTimestamp,
) -> KernelResult<Bytes> {
	let mut output = BytesMut::new();
	output.put_u64(row_count);
	for key in [smallest, largest] {
		output.put_u32(u32::try_from(key.len()).map_err(|_| {
			KernelError::new(ErrorCode::ResourceExhausted, "property key is too large")
		})?);
		output.extend_from_slice(key);
	}
	output.put_u64(smallest_version.0);
	output.put_u64(largest_version.0);
	output.put_u64(max_timestamp.0);
	append_checksum(output)
}

fn decode_properties(bytes: Bytes) -> KernelResult<Properties> {
	let body = checked_section(bytes)?;
	let mut cursor = SliceCursor::new(body);
	let row_count = cursor.u64()?;
	let mut key = || -> KernelResult<Bytes> {
		let len = cursor.u32()? as usize;
		if len > MAX_USER_KEY_LEN {
			return Err(corruption("property key exceeds limit"));
		}
		cursor.bytes(len)
	};
	let smallest_user_key = key()?;
	let largest_user_key = key()?;
	let smallest_version = CommitVersion(cursor.u64()?);
	let largest_version = CommitVersion(cursor.u64()?);
	let max_commit_timestamp = CommitTimestamp(cursor.u64()?);
	cursor.finish()?;
	Ok(Properties {
		row_count,
		smallest_user_key,
		largest_user_key,
		smallest_version,
		largest_version,
		max_commit_timestamp,
	})
}

fn validate_properties(properties: &Properties, descriptor: &TableDescriptor) -> KernelResult<()> {
	if properties.row_count == 0
		|| properties.row_count != descriptor.row_count
		|| properties.smallest_user_key != descriptor.smallest_user_key
		|| properties.largest_user_key != descriptor.largest_user_key
		|| properties.smallest_version != descriptor.smallest_version
		|| properties.largest_version != descriptor.largest_version
		|| properties.max_commit_timestamp != descriptor.max_commit_timestamp
		|| properties.smallest_user_key > properties.largest_user_key
	{
		return Err(corruption("table properties mismatch"));
	}
	Ok(())
}

async fn read_section(
	store: &dyn ObjectStore,
	descriptor: &TableDescriptor,
	handle: SectionHandle,
) -> KernelResult<Bytes> {
	store
		.read_range(
			&descriptor.object_id(),
			ByteRange::new(
				handle.offset,
				handle
					.offset
					.checked_add(handle.len)
					.ok_or_else(|| corruption("section range overflow"))?,
			)?,
		)
		.await
}

fn decode_block(encoded: Bytes) -> KernelResult<Bytes> {
	if encoded.len() < 8 || encoded.len() > MAX_SECTION_LEN {
		return Err(corruption("compressed block is truncated"));
	}
	let payload_len = encoded.len() - 4;
	if crc32(&encoded[..payload_len]) != read_u32(&encoded, payload_len)? {
		return Err(corruption("block checksum mismatch"));
	}
	let declared_len = u32::from_le_bytes(
		encoded[..4].try_into().map_err(|_| corruption("block size prefix is truncated"))?,
	) as usize;
	if declared_len > MAX_SECTION_LEN {
		return Err(corruption("decompressed block exceeds limit"));
	}
	let raw = lz4_flex::decompress_size_prepended(&encoded[..payload_len])
		.map_err(|_| corruption("block decompression failed"))?;
	if raw.len() > MAX_SECTION_LEN {
		return Err(corruption("decompressed block exceeds limit"));
	}
	Ok(Bytes::from(raw))
}

fn decode_rows(raw: Bytes) -> KernelResult<Vec<StorageRow>> {
	let mut cursor = SliceCursor::new(raw);
	let mut rows = Vec::new();
	let mut previous: Option<super::format::InternalKey> = None;
	while !cursor.is_empty() {
		let len = cursor.u32()? as usize;
		let row = StorageRow::decode(cursor.bytes(len)?)?;
		if previous
			.as_ref()
			.is_some_and(|key| key.user_key == row.key.user_key && key.version == row.key.version)
		{
			return Err(corruption("block contains duplicate user-key/version"));
		}
		if previous.as_ref().is_some_and(|key| compare_internal(key, &row.key).is_ge()) {
			return Err(corruption("block rows are not strictly sorted"));
		}
		previous = Some(row.key.clone());
		rows.push(row);
	}
	if rows.is_empty() {
		return Err(corruption("empty data block"));
	}
	Ok(rows)
}

fn append_checksum(mut body: BytesMut) -> KernelResult<Bytes> {
	if body.len() > MAX_SECTION_LEN - 4 {
		return Err(KernelError::new(ErrorCode::ResourceExhausted, "table section exceeds limit"));
	}
	let checksum = crc32(&body);
	body.put_u32(checksum);
	Ok(body.freeze())
}

fn checked_section(bytes: Bytes) -> KernelResult<Bytes> {
	if bytes.len() < 4 || bytes.len() > MAX_SECTION_LEN {
		return Err(corruption("invalid table section length"));
	}
	let body_len = bytes.len() - 4;
	if crc32(&bytes[..body_len]) != read_u32(&bytes, body_len)? {
		return Err(corruption("table section checksum mismatch"));
	}
	Ok(bytes.slice(..body_len))
}

fn section(offset: &mut u64, len: usize) -> KernelResult<SectionHandle> {
	let len = u64::try_from(len)
		.map_err(|_| KernelError::new(ErrorCode::ResourceExhausted, "section is too large"))?;
	let handle = SectionHandle {
		offset: *offset,
		len,
	};
	*offset = offset
		.checked_add(len)
		.ok_or_else(|| KernelError::new(ErrorCode::ResourceExhausted, "table is too large"))?;
	Ok(handle)
}

struct SliceCursor {
	bytes: Bytes,
	offset: usize,
}

impl SliceCursor {
	fn new(bytes: Bytes) -> Self {
		Self {
			bytes,
			offset: 0,
		}
	}

	fn bytes(&mut self, len: usize) -> KernelResult<Bytes> {
		let end = self
			.offset
			.checked_add(len)
			.ok_or_else(|| corruption("section field length overflow"))?;
		if end > self.bytes.len() {
			return Err(corruption("section field is truncated"));
		}
		let output = self.bytes.slice(self.offset..end);
		self.offset = end;
		Ok(output)
	}

	fn u8(&mut self) -> KernelResult<u8> {
		Ok(self.bytes(1)?[0])
	}

	fn u32(&mut self) -> KernelResult<u32> {
		Ok(u32::from_be_bytes(
			self.bytes(4)?[..].try_into().map_err(|_| corruption("truncated u32"))?,
		))
	}

	fn u16(&mut self) -> KernelResult<u16> {
		Ok(u16::from_be_bytes(
			self.bytes(2)?[..].try_into().map_err(|_| corruption("truncated u16"))?,
		))
	}

	fn u64(&mut self) -> KernelResult<u64> {
		Ok(u64::from_be_bytes(
			self.bytes(8)?[..].try_into().map_err(|_| corruption("truncated u64"))?,
		))
	}

	fn is_empty(&self) -> bool {
		self.offset == self.bytes.len()
	}

	fn finish(&self) -> KernelResult<()> {
		if !self.is_empty() {
			return Err(corruption("table section contains trailing bytes"));
		}
		Ok(())
	}
}

fn read_u16(bytes: &[u8], offset: usize) -> KernelResult<u16> {
	let end = offset.checked_add(2).ok_or_else(|| corruption("u16 offset overflow"))?;
	Ok(u16::from_be_bytes(
		bytes
			.get(offset..end)
			.ok_or_else(|| corruption("u16 is truncated"))?
			.try_into()
			.map_err(|_| corruption("u16 is truncated"))?,
	))
}

fn read_u32(bytes: &[u8], offset: usize) -> KernelResult<u32> {
	let end = offset.checked_add(4).ok_or_else(|| corruption("u32 offset overflow"))?;
	Ok(u32::from_be_bytes(
		bytes
			.get(offset..end)
			.ok_or_else(|| corruption("u32 is truncated"))?
			.try_into()
			.map_err(|_| corruption("u32 is truncated"))?,
	))
}

fn read_u64(bytes: &[u8], offset: usize) -> KernelResult<u64> {
	let end = offset.checked_add(8).ok_or_else(|| corruption("u64 offset overflow"))?;
	Ok(u64::from_be_bytes(
		bytes
			.get(offset..end)
			.ok_or_else(|| corruption("u64 is truncated"))?
			.try_into()
			.map_err(|_| corruption("u64 is truncated"))?,
	))
}

fn corruption(message: &'static str) -> KernelError {
	KernelError::new(ErrorCode::Corruption, message)
}

fn hex(bytes: &[u8]) -> String {
	const DIGITS: &[u8; 16] = b"0123456789abcdef";
	let mut output = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		output.push(DIGITS[(byte >> 4) as usize] as char);
		output.push(DIGITS[(byte & 0x0f) as usize] as char);
	}
	output
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use async_trait::async_trait;
	use proptest::prelude::*;

	use super::*;
	use crate::format::{InternalKey, RowKind};
	use crate::storage::{
		DeleteOutcome, ListCursor, MemoryObjectStore, ObjectCapabilities, ObjectMetadata,
		ObjectPage, ObjectPrefix, PutOutcome, PutRequest,
	};

	fn owner() -> TableOwner {
		TableOwner {
			branch: BranchId::from_u128(7),
			generation: BranchGeneration(3),
		}
	}

	fn row(key: Bytes, version: u64, value: Bytes) -> StorageRow {
		StorageRow::new(
			InternalKey::new(key, CommitVersion(version), RowKind::Value).unwrap(),
			CommitTimestamp(version * 10),
			None,
			value,
		)
		.unwrap()
	}

	fn built(rows: Vec<StorageRow>, block_target: usize) -> BuiltTable {
		let mut builder = TableBuilder::new(owner(), TableId([9; 32]), block_target).unwrap();
		for row in rows {
			builder.add(owner(), row).unwrap();
		}
		builder.finish().unwrap()
	}

	async fn publish(store: &dyn ObjectStore, table: &BuiltTable) {
		store
			.put_unique(PutRequest {
				id: table.descriptor.object_id(),
				body: table.body.clone(),
				attributes: BTreeMap::from([("sha256".to_string(), hex(&table.descriptor.digest))]),
			})
			.await
			.unwrap();
	}

	#[test]
	fn builder_owner_regression_rejects_mixed_owners_and_unsorted_rows() {
		let mut builder = TableBuilder::new(owner(), TableId([1; 32]), 128).unwrap();
		let foreign = TableOwner {
			branch: BranchId::from_u128(8),
			generation: BranchGeneration(0),
		};
		assert_eq!(
			builder
				.add(foreign, row(Bytes::from_static(b"a"), 2, Bytes::from_static(b"x")),)
				.unwrap_err()
				.code,
			ErrorCode::InvalidArgument
		);
		builder.add(owner(), row(Bytes::from_static(b"b"), 1, Bytes::from_static(b"b"))).unwrap();
		assert_eq!(
			builder
				.add(owner(), row(Bytes::from_static(b"a"), 1, Bytes::from_static(b"a")),)
				.unwrap_err()
				.code,
			ErrorCode::InvalidArgument
		);

		let mut duplicate = TableBuilder::new(owner(), TableId([2; 32]), 128).unwrap();
		duplicate
			.add(owner(), row(Bytes::from_static(b"a"), 1, Bytes::from_static(b"value")))
			.unwrap();
		let tombstone = StorageRow::new(
			InternalKey::new(Bytes::from_static(b"a"), CommitVersion(1), RowKind::Tombstone)
				.unwrap(),
			CommitTimestamp(10),
			None,
			Bytes::new(),
		)
		.unwrap();
		assert_eq!(duplicate.add(owner(), tombstone).unwrap_err().code, ErrorCode::InvalidArgument);
	}

	#[test]
	fn malicious_lz4_size_prefix_is_rejected_before_decompression() {
		let mut encoded = u32::MAX.to_le_bytes().to_vec();
		let checksum = crc32(&encoded);
		encoded.extend_from_slice(&checksum.to_be_bytes());
		assert_eq!(decode_block(Bytes::from(encoded)).unwrap_err().code, ErrorCode::Corruption);
	}

	#[test]
	fn footer_is_fixed_width_and_future_formats_fail_closed() {
		let table =
			built(vec![row(Bytes::from_static(b"a"), 1, Bytes::from_static(b"value"))], 128);
		let bytes = table.body.coalesce().unwrap();
		let mut digest = Sha256::new();
		digest.update(&bytes);
		assert_eq!(<[u8; 32]>::from(digest.finalize()), table.descriptor.digest);
		assert_eq!(&bytes[..8], &FORMAT_MAGIC);
		assert_eq!(&bytes[bytes.len() - FOOTER_LEN..bytes.len() - FOOTER_LEN + 8], &FOOTER_MAGIC);
		assert_eq!(
			decode_footer(bytes.slice(bytes.len() - FOOTER_LEN..)).unwrap().table_id,
			TableId([9; 32])
		);
		let mut future_footer = bytes.slice(bytes.len() - FOOTER_LEN..).to_vec();
		future_footer[8..10].copy_from_slice(&(FORMAT_VERSION + 1).to_be_bytes());
		let checksum = crc32(&future_footer[..FOOTER_LEN - 4]);
		future_footer[FOOTER_LEN - 4..].copy_from_slice(&checksum.to_be_bytes());
		assert_eq!(
			decode_footer(Bytes::from(future_footer))
				.err()
				.expect("future footer must be rejected")
				.code,
			ErrorCode::Corruption
		);

		let mut future = table.descriptor;
		future.format_version = FORMAT_VERSION + 1;
		let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
		runtime.block_on(async {
			let error = TableReader::open(
				Arc::new(MemoryObjectStore::new(2)),
				future,
				Arc::new(BlockCache::new(1024)),
			)
			.await
			.err()
			.expect("future descriptor must be rejected");
			assert_eq!(error.code, ErrorCode::Corruption);
		});
	}

	#[tokio::test]
	async fn corrupt_index_section_is_rejected_during_open() {
		let table =
			built(vec![row(Bytes::from_static(b"a"), 1, Bytes::from_static(b"value"))], 128);
		let mut bytes = table.body.coalesce().unwrap().to_vec();
		let footer =
			decode_footer(Bytes::copy_from_slice(&bytes[bytes.len() - FOOTER_LEN..])).unwrap();
		bytes[footer.index.offset as usize + 1] ^= 0xff;
		let store = Arc::new(MemoryObjectStore::new(2));
		store
			.put_unique(PutRequest {
				id: table.descriptor.object_id(),
				body: ObjectBody::from_bytes(Bytes::from(bytes)),
				attributes: BTreeMap::from([("sha256".to_string(), hex(&table.descriptor.digest))]),
			})
			.await
			.unwrap();
		let error = TableReader::open(store, table.descriptor, Arc::new(BlockCache::new(1024)))
			.await
			.err()
			.expect("corrupt index must fail open");
		assert_eq!(error.code, ErrorCode::Corruption);
	}

	#[tokio::test]
	async fn reader_owner_checksum_and_range_regressions_fail_closed() {
		let table = built(
			vec![
				row(Bytes::from_static(b"a"), 3, Bytes::from_static(b"a3")),
				row(Bytes::from_static(b"a"), 1, Bytes::from_static(b"a1")),
				row(Bytes::from_static(b"aa"), 2, Bytes::from_static(b"aa2")),
				row(Bytes::from_static(b"b"), 1, Bytes::from_static(b"b1")),
			],
			48,
		);
		let inner = Arc::new(MemoryObjectStore::new(2));
		publish(&*inner, &table).await;
		let recording = Arc::new(RecordingStore::new(Arc::<MemoryObjectStore>::clone(&inner)));
		let reader = TableReader::open(
			Arc::<RecordingStore>::clone(&recording),
			table.descriptor.clone(),
			Arc::new(BlockCache::new(4096)),
		)
		.await
		.unwrap();
		assert_eq!(
			reader.get(b"a", CommitVersion(2)).await.unwrap().unwrap().value,
			Bytes::from_static(b"a1")
		);
		assert_eq!(
			reader.get(b"a", CommitVersion(3)).await.unwrap().unwrap().value,
			Bytes::from_static(b"a3")
		);
		assert!(recording
			.ranges()
			.iter()
			.all(|range| range.end - range.start < table.descriptor.object_len));
		recording.clear_ranges();
		let scanned = reader.scan_rows(b"a", b"z", CommitVersion(2)).await.unwrap();
		assert_eq!(scanned.len(), 3);
		assert!(reader.index.len() > 1, "fixture must span multiple data blocks");
		assert!(
			recording.ranges().len() < reader.index.len(),
			"bounded prefetch must coalesce adjacent blocks"
		);
		assert!(recording.ranges().iter().all(|range| range.end - range.start <= MAX_PREFETCH_LEN));

		let mut wrong_owner = table.descriptor.clone();
		wrong_owner.owner.generation = BranchGeneration(4);
		assert_eq!(
			TableReader::open(
				Arc::<RecordingStore>::clone(&recording),
				wrong_owner,
				Arc::new(BlockCache::new(4096)),
			)
			.await
			.err()
			.expect("wrong owner must be rejected")
			.code,
			ErrorCode::Corruption
		);

		let mut corrupted = table.body.coalesce().unwrap().to_vec();
		corrupted[HEADER_LEN + 1] ^= 0xff;
		let corrupt_store = Arc::new(MemoryObjectStore::new(2));
		corrupt_store
			.put_unique(PutRequest {
				id: table.descriptor.object_id(),
				body: ObjectBody::from_bytes(Bytes::from(corrupted)),
				attributes: BTreeMap::from([("sha256".to_string(), hex(&table.descriptor.digest))]),
			})
			.await
			.unwrap();
		let corrupt_reader =
			TableReader::open(corrupt_store, table.descriptor, Arc::new(BlockCache::new(4096)))
				.await
				.unwrap();
		assert_eq!(
			corrupt_reader.get(b"a", CommitVersion(3)).await.unwrap_err().code,
			ErrorCode::Corruption
		);
	}

	#[tokio::test]
	async fn corrupt_cache_entry_is_evicted_and_refetched() {
		let table =
			built(vec![row(Bytes::from_static(b"a"), 1, Bytes::from_static(b"value"))], 128);
		let inner = Arc::new(MemoryObjectStore::new(2));
		publish(&*inner, &table).await;
		let recording = Arc::new(RecordingStore::new(inner));
		let cache = Arc::new(BlockCache::new(4096));
		let reader = TableReader::open(
			Arc::<RecordingStore>::clone(&recording),
			table.descriptor.clone(),
			Arc::<BlockCache>::clone(&cache),
		)
		.await
		.unwrap();
		reader.get(b"a", CommitVersion(1)).await.unwrap();
		let reads_before = recording.ranges().len();
		assert!(cache.corrupt(table.descriptor.table_id, HEADER_LEN as u64));
		reader.get(b"a", CommitVersion(1)).await.unwrap();
		assert_eq!(recording.ranges().len(), reads_before + 1);
	}

	proptest! {
		#[test]
		fn sorted_map_point_reads_match_oracle(entries in prop::collection::btree_map(any::<u8>(), any::<u64>(), 1..64)) {
			let rows: Vec<_> = entries
				.iter()
				.map(|(key, value)| row(Bytes::copy_from_slice(&[*key]), 1, Bytes::copy_from_slice(&value.to_be_bytes())))
				.collect();
			let table = built(rows, 64);
			let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
			runtime.block_on(async {
				let store = Arc::new(MemoryObjectStore::new(2));
				publish(&*store, &table).await;
				let reader = TableReader::open(
					store,
					table.descriptor,
					Arc::new(BlockCache::new(16 * 1024)),
				).await.unwrap();
				for key in 0u8..=u8::MAX {
					let actual = reader
						.get(&[key], CommitVersion(1))
						.await
						.unwrap()
						.map(|row| u64::from_be_bytes(row.value[..].try_into().unwrap()));
					prop_assert_eq!(actual, entries.get(&key).copied());
				}
				Ok(())
			})?;
		}
	}

	struct RecordingStore {
		inner: Arc<MemoryObjectStore>,
		ranges: Mutex<Vec<ByteRange>>,
	}

	impl RecordingStore {
		fn new(inner: Arc<MemoryObjectStore>) -> Self {
			Self {
				inner,
				ranges: Mutex::new(Vec::new()),
			}
		}

		fn ranges(&self) -> Vec<ByteRange> {
			self.ranges.lock().clone()
		}

		fn clear_ranges(&self) {
			self.ranges.lock().clear();
		}
	}

	#[async_trait]
	impl ObjectStore for RecordingStore {
		fn capabilities(&self) -> ObjectCapabilities {
			self.inner.capabilities()
		}

		async fn read_range(&self, id: &ObjectId, range: ByteRange) -> KernelResult<Bytes> {
			self.ranges.lock().push(range);
			self.inner.read_range(id, range).await
		}

		async fn put_unique(&self, request: PutRequest) -> KernelResult<PutOutcome> {
			self.inner.put_unique(request).await
		}

		async fn metadata(&self, id: &ObjectId) -> KernelResult<ObjectMetadata> {
			self.inner.metadata(id).await
		}

		async fn list_page(
			&self,
			prefix: &ObjectPrefix,
			cursor: Option<ListCursor>,
		) -> KernelResult<ObjectPage> {
			self.inner.list_page(prefix, cursor).await
		}

		async fn delete(&self, id: &ObjectId) -> KernelResult<DeleteOutcome> {
			self.inner.delete(id).await
		}
	}
}
