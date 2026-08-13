//! Single-authority database and durable root publication spine.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::RwLock;
use tokio::sync::Mutex;

use super::api::{
	AuthorityFence, BranchGeneration, BranchId, CommitTimestamp, CommitVersion, DatabaseId,
	ErrorCode, KernelError, KernelResult, OperationId,
};
use super::branch::{ReadSelector, WriteOperation};
use super::format::{InternalKey, RowKind, StorageRow, MAX_USER_KEY_LEN};
use super::storage::{
	AuthoritySession, BindingRequirements, Bindings, CommitOutcome, CommitProposal, OpenMode,
	PutRequest, ReconcileOutcome,
};
use super::table::{
	decode_descriptor, encode_descriptor, BlockCache, TableBuilder, TableDescriptor, TableOwner,
	TableReader,
};

const ROOT_MAGIC: [u8; 8] = *b"SKVROOT1";
const ROOT_VERSION: u16 = 1;
const MAX_ROOT_LEN: usize = 128 * 1024 * 1024;
const MAX_ROOT_ROWS: usize = 1_000_000;
const MAX_TIMELINE_ENTRIES: usize = 1_000_000;

#[derive(Clone, Debug, PartialEq, Eq)]
struct DatabaseRoot {
	database: DatabaseId,
	main_branch: BranchId,
	main_generation: BranchGeneration,
	global_version: CommitVersion,
	branch_head: CommitVersion,
	last_timestamp: CommitTimestamp,
	timeline: Vec<(CommitTimestamp, CommitVersion)>,
	tables: Vec<TableDescriptor>,
	rows: BTreeMap<Bytes, Vec<StorageRow>>,
}

impl DatabaseRoot {
	fn new(database: DatabaseId, main_branch: BranchId) -> Self {
		Self {
			database,
			main_branch,
			main_generation: BranchGeneration(0),
			global_version: CommitVersion(0),
			branch_head: CommitVersion(0),
			last_timestamp: CommitTimestamp(0),
			timeline: Vec::new(),
			tables: Vec::new(),
			rows: BTreeMap::new(),
		}
	}

	fn encode(&self) -> KernelResult<Bytes> {
		let row_count: usize = self.rows.values().map(Vec::len).sum();
		if row_count > MAX_ROOT_ROWS || self.timeline.len() > MAX_TIMELINE_ENTRIES {
			return Err(KernelError::new(
				ErrorCode::ResourceExhausted,
				"database root exceeds limits",
			));
		}
		let mut output = BytesMut::new();
		output.extend_from_slice(&ROOT_MAGIC);
		output.put_u16(ROOT_VERSION);
		output.extend_from_slice(&self.database.0);
		output.extend_from_slice(&self.main_branch.0);
		output.put_u64(self.main_generation.0);
		output.put_u64(self.global_version.0);
		output.put_u64(self.branch_head.0);
		output.put_u64(self.last_timestamp.0);
		output.put_u32(u32::try_from(self.timeline.len()).map_err(|_| {
			KernelError::new(ErrorCode::ResourceExhausted, "timeline exceeds format")
		})?);
		for (timestamp, version) in &self.timeline {
			output.put_u64(timestamp.0);
			output.put_u64(version.0);
		}
		output.put_u32(u32::try_from(self.tables.len()).map_err(|_| {
			KernelError::new(ErrorCode::ResourceExhausted, "table references exceed format")
		})?);
		for table in &self.tables {
			let encoded = encode_descriptor(table)?;
			output.put_u32(u32::try_from(encoded.len()).map_err(|_| {
				KernelError::new(ErrorCode::ResourceExhausted, "table descriptor exceeds format")
			})?);
			output.extend_from_slice(&encoded);
		}
		output.put_u32(u32::try_from(row_count).map_err(|_| {
			KernelError::new(ErrorCode::ResourceExhausted, "root rows exceed format")
		})?);
		for history in self.rows.values() {
			for row in history {
				let encoded = row.encode()?;
				output.put_u32(u32::try_from(encoded.len()).map_err(|_| {
					KernelError::new(ErrorCode::ResourceExhausted, "root row exceeds format")
				})?);
				output.extend_from_slice(&encoded);
			}
		}
		if output.len() > MAX_ROOT_LEN {
			return Err(KernelError::new(
				ErrorCode::ResourceExhausted,
				"database root exceeds limit",
			));
		}
		let checksum = crc32fast::hash(&output);
		output.put_u32(checksum);
		Ok(output.freeze())
	}

	fn decode(bytes: Bytes) -> KernelResult<Self> {
		if bytes.len() > MAX_ROOT_LEN || bytes.len() < 82 {
			return Err(corruption("invalid database root length"));
		}
		let body_len = bytes.len() - 4;
		if crc32fast::hash(&bytes[..body_len]) != read_u32(&bytes, body_len)? {
			return Err(corruption("database root checksum mismatch"));
		}
		let mut cursor = Cursor::new(bytes.slice(..body_len));
		if cursor.bytes(8)?.as_ref() != ROOT_MAGIC {
			return Err(corruption("invalid database root magic"));
		}
		if cursor.u16()? != ROOT_VERSION {
			return Err(corruption("unsupported database root version"));
		}
		let database = DatabaseId(cursor.array16()?);
		let main_branch = BranchId(cursor.array16()?);
		let main_generation = BranchGeneration(cursor.u64()?);
		let global_version = CommitVersion(cursor.u64()?);
		let branch_head = CommitVersion(cursor.u64()?);
		let last_timestamp = CommitTimestamp(cursor.u64()?);
		if branch_head > global_version {
			return Err(corruption("branch head exceeds global version"));
		}
		let timeline_count = cursor.u32()? as usize;
		if timeline_count > MAX_TIMELINE_ENTRIES {
			return Err(corruption("timeline count exceeds limit"));
		}
		let mut timeline = Vec::with_capacity(timeline_count);
		for _ in 0..timeline_count {
			let entry = (CommitTimestamp(cursor.u64()?), CommitVersion(cursor.u64()?));
			if timeline
				.last()
				.is_some_and(|(timestamp, version)| entry.0 <= *timestamp || entry.1 <= *version)
			{
				return Err(corruption("database timeline is not strictly ordered"));
			}
			timeline.push(entry);
		}
		if timeline.last().copied().unwrap_or((CommitTimestamp(0), CommitVersion(0)))
			!= (last_timestamp, global_version)
		{
			return Err(corruption("database timeline frontier mismatch"));
		}
		let table_count = cursor.u32()? as usize;
		if table_count > MAX_ROOT_ROWS {
			return Err(corruption("table reference count exceeds limit"));
		}
		let mut tables = Vec::with_capacity(table_count);
		for _ in 0..table_count {
			let len = cursor.u32()? as usize;
			if len > MAX_ROOT_LEN {
				return Err(corruption("table descriptor length exceeds limit"));
			}
			let table = decode_descriptor(cursor.bytes(len)?)?;
			if table.owner.branch != main_branch
				|| table.owner.generation != main_generation
				|| table.largest_version > branch_head
				|| table.max_commit_timestamp > last_timestamp
			{
				return Err(corruption("table descriptor exceeds database root frontier"));
			}
			tables.push(table);
		}
		let row_count = cursor.u32()? as usize;
		if row_count > MAX_ROOT_ROWS {
			return Err(corruption("root row count exceeds limit"));
		}
		let mut rows: BTreeMap<Bytes, Vec<StorageRow>> = BTreeMap::new();
		for _ in 0..row_count {
			let len = cursor.u32()? as usize;
			if len > MAX_ROOT_LEN {
				return Err(corruption("root row length exceeds limit"));
			}
			let row = StorageRow::decode(cursor.bytes(len)?)?;
			if row.key.version > branch_head || row.commit_timestamp > last_timestamp {
				return Err(corruption("root row exceeds published frontier"));
			}
			let history = rows.entry(row.key.user_key.clone()).or_default();
			if history.last().is_some_and(|previous| previous.key.version <= row.key.version) {
				return Err(corruption("root row history is not newest-first unique"));
			}
			history.push(row);
		}
		cursor.finish()?;
		Ok(Self {
			database,
			main_branch,
			main_generation,
			global_version,
			branch_head,
			last_timestamp,
			timeline,
			tables,
			rows,
		})
	}

	fn temporal_context(
		&self,
		selector: ReadSelector,
		current_time: CommitTimestamp,
	) -> KernelResult<(CommitVersion, CommitTimestamp)> {
		match selector {
			ReadSelector::Latest => Ok((self.branch_head, current_time.max(self.last_timestamp))),
			ReadSelector::Version(version) => {
				if version > self.global_version {
					return Err(KernelError::new(
						ErrorCode::InvalidArgument,
						"version exceeds published frontier",
					));
				}
				Ok((version, self.timestamp_for_version(version)))
			}
			ReadSelector::Timestamp(timestamp) => Ok((
				self.timeline
					.iter()
					.rev()
					.find(|(candidate, _)| *candidate <= timestamp)
					.map_or(CommitVersion(0), |(_, version)| *version),
				timestamp,
			)),
		}
	}

	fn timestamp_for_version(&self, version: CommitVersion) -> CommitTimestamp {
		self.timeline
			.iter()
			.rev()
			.find(|(_, candidate)| *candidate <= version)
			.map_or(CommitTimestamp(0), |(timestamp, _)| *timestamp)
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitReceipt {
	pub version: CommitVersion,
	pub timestamp: CommitTimestamp,
	pub branch_head: CommitVersion,
	pub authority_fence: AuthorityFence,
}

struct RuntimeState {
	root: DatabaseRoot,
	fence: AuthorityFence,
}

/// Private P3 vertical spine. All mutations serialize through `commit_lock`;
/// authority publication remains the only durable/visible transition.
pub struct KernelDatabase {
	bindings: Bindings,
	session: Mutex<AuthoritySession>,
	state: RwLock<RuntimeState>,
	commit_lock: Mutex<()>,
	cache: Arc<BlockCache>,
}

impl KernelDatabase {
	#[cfg(not(target_arch = "wasm32"))]
	pub async fn memory(memory_budget: u64) -> KernelResult<Self> {
		use super::storage::{MemoryCommitStore, MemoryObjectStore, NativePlatform};

		Self::open_or_create(Bindings {
			objects: Arc::new(MemoryObjectStore::new(128)),
			commits: Arc::new(MemoryCommitStore::new(Bytes::new())),
			platform: Arc::new(NativePlatform::new(memory_budget)),
		})
		.await
	}

	#[cfg(not(target_arch = "wasm32"))]
	pub async fn open_local(
		path: impl AsRef<std::path::Path>,
		memory_budget: u64,
	) -> KernelResult<Self> {
		use super::storage::{LocalCommitStore, LocalObjectStore, NativePlatform};

		let path = path.as_ref();
		Self::open_or_create(Bindings {
			objects: Arc::new(LocalObjectStore::open(path.join("objects"), 128)?),
			commits: Arc::new(LocalCommitStore::open(path.join("authority"))?),
			platform: Arc::new(NativePlatform::new(memory_budget)),
		})
		.await
	}

	pub(crate) async fn open_or_create(bindings: Bindings) -> KernelResult<Self> {
		bindings.validate(BindingRequirements {
			ranged_reads: true,
			unique_put: true,
			paginated_list: true,
			idempotent_delete: true,
			conditional_publish: true,
			writer_fencing: true,
			reconcile_unknown: true,
			minimum_durability: super::api::DurabilityClass::Ephemeral,
		})?;
		let mut session = bindings.commits.open(OpenMode::ReadWrite).await?;
		let mut authority = bindings.commits.load_root(&session).await?;
		let root = if authority.bytes.is_empty() {
			let mut database = [0; 16];
			let mut branch = [0; 16];
			bindings.platform.fill_random(&mut database)?;
			bindings.platform.fill_random(&mut branch)?;
			let root = DatabaseRoot::new(DatabaseId(database), BranchId(branch));
			let encoded_root = root.encode()?;
			let mut operation = [0; 16];
			bindings.platform.fill_random(&mut operation)?;
			let operation = OperationId(operation);
			let outcome = bindings
				.commits
				.commit(
					&mut session,
					authority.fence,
					CommitProposal {
						operation,
						root: encoded_root.clone(),
					},
				)
				.await?;
			let fence = match outcome {
				CommitOutcome::Confirmed(fence) => fence,
				CommitOutcome::Unknown => match bindings.commits.reconcile(operation).await? {
					ReconcileOutcome::Confirmed(fence) => fence,
					ReconcileOutcome::NotCommitted | ReconcileOutcome::Unknown => {
						return Err(KernelError::new(
							ErrorCode::Unavailable,
							"database creation outcome remains indeterminate",
						));
					}
				},
				CommitOutcome::Conflict(_) => {
					return Err(KernelError::new(
						ErrorCode::Conflict,
						"database was concurrently initialized",
					));
				}
				CommitOutcome::Fenced => {
					return Err(KernelError::new(
						ErrorCode::Fenced,
						"writer was fenced during database creation",
					));
				}
			};
			authority = super::storage::AuthorityRoot {
				fence,
				bytes: encoded_root,
			};
			root
		} else {
			DatabaseRoot::decode(authority.bytes.clone())?
		};
		let cache_capacity =
			usize::try_from(bindings.platform.memory_budget().bytes / 4).unwrap_or(usize::MAX);
		Ok(Self {
			bindings,
			session: Mutex::new(session),
			state: RwLock::new(RuntimeState {
				root,
				fence: authority.fence,
			}),
			commit_lock: Mutex::new(()),
			cache: Arc::new(BlockCache::new(cache_capacity)),
		})
	}

	pub fn database_id(&self) -> DatabaseId {
		self.state.read().root.database
	}

	pub fn main_branch(&self) -> (BranchId, BranchGeneration) {
		let state = self.state.read();
		(state.root.main_branch, state.root.main_generation)
	}

	pub fn head(&self) -> CommitVersion {
		self.state.read().root.branch_head
	}

	pub async fn commit(
		&self,
		expected_head: CommitVersion,
		timestamp: CommitTimestamp,
		writes: Vec<WriteOperation>,
	) -> KernelResult<CommitReceipt> {
		let _serial = self.commit_lock.lock().await;
		let (mut candidate, expected_fence) = {
			let state = self.state.read();
			(state.root.clone(), state.fence)
		};
		if candidate.branch_head != expected_head {
			return Err(KernelError::new(ErrorCode::Conflict, "branch head changed"));
		}
		if timestamp <= candidate.last_timestamp {
			return Err(KernelError::new(
				ErrorCode::InvalidArgument,
				"commit timestamp must increase",
			));
		}
		if writes.is_empty() {
			return Err(KernelError::new(ErrorCode::InvalidArgument, "empty commit"));
		}
		let version =
			CommitVersion(candidate.global_version.0.checked_add(1).ok_or_else(|| {
				KernelError::new(ErrorCode::ResourceExhausted, "commit version exhausted")
			})?);
		for write in writes {
			let (key, kind, value, expires_at) = match write {
				WriteOperation::Put {
					key,
					value,
					expires_at,
				} => (key, RowKind::Value, value, expires_at),
				WriteOperation::Delete {
					key,
				} => (key, RowKind::Tombstone, Bytes::new(), None),
			};
			if key.len() > MAX_USER_KEY_LEN {
				return Err(KernelError::new(ErrorCode::ResourceExhausted, "key exceeds limit"));
			}
			let row = StorageRow::new(
				InternalKey::new(key.clone(), version, kind)?,
				timestamp,
				expires_at,
				value,
			)?;
			let history = candidate.rows.entry(key).or_default();
			if history.first().is_some_and(|existing| existing.key.version == version) {
				return Err(KernelError::new(
					ErrorCode::InvalidArgument,
					"commit contains duplicate key",
				));
			}
			history.insert(0, row);
		}
		candidate.global_version = version;
		candidate.branch_head = version;
		candidate.last_timestamp = timestamp;
		candidate.timeline.push((timestamp, version));
		let mutable_bytes = candidate.rows.values().try_fold(0u64, |total, history| {
			history.iter().try_fold(total, |total, row| {
				let encoded = row.encode()?;
				total.checked_add(encoded.len() as u64).ok_or_else(|| {
					KernelError::new(ErrorCode::ResourceExhausted, "write buffer size overflow")
				})
			})
		})?;
		if mutable_bytes > self.bindings.platform.memory_budget().bytes {
			return Err(KernelError::new(
				ErrorCode::ResourceExhausted,
				"database write buffer budget exceeded; flush required",
			));
		}
		let root_bytes = candidate.encode()?;
		let mut operation = [0; 16];
		self.bindings.platform.fill_random(&mut operation)?;
		let operation = OperationId(operation);
		let mut session = self.session.lock().await;
		let outcome = self
			.bindings
			.commits
			.commit(
				&mut session,
				expected_fence,
				CommitProposal {
					operation,
					root: root_bytes,
				},
			)
			.await?;
		let fence = match outcome {
			CommitOutcome::Confirmed(fence) => fence,
			CommitOutcome::Unknown => match self.bindings.commits.reconcile(operation).await? {
				ReconcileOutcome::Confirmed(fence) => fence,
				ReconcileOutcome::NotCommitted | ReconcileOutcome::Unknown => {
					return Err(KernelError::new(
						ErrorCode::Unavailable,
						"commit outcome remains indeterminate",
					));
				}
			},
			CommitOutcome::Conflict(_) => {
				return Err(KernelError::new(ErrorCode::Conflict, "authority root changed"));
			}
			CommitOutcome::Fenced => {
				return Err(KernelError::new(ErrorCode::Fenced, "writer was fenced"));
			}
		};
		*self.state.write() = RuntimeState {
			root: candidate,
			fence,
		};
		Ok(CommitReceipt {
			version,
			timestamp,
			branch_head: version,
			authority_fence: fence,
		})
	}

	pub async fn flush(&self) -> KernelResult<bool> {
		let _serial = self.commit_lock.lock().await;
		let (mut candidate, expected_fence) = {
			let state = self.state.read();
			(state.root.clone(), state.fence)
		};
		if candidate.rows.is_empty() {
			return Ok(false);
		}
		let owner = TableOwner {
			branch: candidate.main_branch,
			generation: candidate.main_generation,
		};
		let mut builder =
			TableBuilder::with_generated_id(owner, &*self.bindings.platform, 64 * 1024)?;
		for history in candidate.rows.values() {
			for row in history {
				builder.add(owner, row.clone())?;
			}
		}
		let built = builder.finish()?;
		let descriptor = built.descriptor;
		self.bindings
			.objects
			.put_unique(PutRequest {
				id: descriptor.object_id(),
				body: built.body,
				attributes: BTreeMap::from([("sha256".to_string(), hex(&descriptor.digest))]),
			})
			.await?;
		candidate.tables.push(descriptor);
		candidate.rows.clear();
		let fence = self.publish_candidate(&candidate, expected_fence).await?;
		*self.state.write() = RuntimeState {
			root: candidate,
			fence,
		};
		Ok(true)
	}

	pub async fn compact_owned(&self) -> KernelResult<bool> {
		let _serial = self.commit_lock.lock().await;
		let (mut candidate, expected_fence) = {
			let state = self.state.read();
			(state.root.clone(), state.fence)
		};
		if candidate.tables.len() < 2 || !candidate.rows.is_empty() {
			return Ok(false);
		}
		let mut rows = Vec::new();
		for descriptor in &candidate.tables {
			let reader = TableReader::open(
				Arc::clone(&self.bindings.objects),
				descriptor.clone(),
				Arc::clone(&self.cache),
			)
			.await?;
			let mut end = descriptor.largest_user_key.to_vec();
			end.push(0);
			rows.extend(
				reader
					.scan_rows(&descriptor.smallest_user_key, &end, CommitVersion(u64::MAX))
					.await?,
			);
		}
		rows.sort_by(|left, right| super::format::compare_internal(&left.key, &right.key));
		let owner = TableOwner {
			branch: candidate.main_branch,
			generation: candidate.main_generation,
		};
		let mut builder =
			TableBuilder::with_generated_id(owner, &*self.bindings.platform, 64 * 1024)?;
		let mut previous: Option<StorageRow> = None;
		for row in rows {
			if let Some(prior) = &previous {
				if prior.key.user_key == row.key.user_key && prior.key.version == row.key.version {
					if prior != &row {
						return Err(corruption("duplicate table rows disagree"));
					}
					continue;
				}
			}
			builder.add(owner, row.clone())?;
			previous = Some(row);
		}
		let built = builder.finish()?;
		let descriptor = built.descriptor;
		self.bindings
			.objects
			.put_unique(PutRequest {
				id: descriptor.object_id(),
				body: built.body,
				attributes: BTreeMap::from([("sha256".to_string(), hex(&descriptor.digest))]),
			})
			.await?;
		candidate.tables = vec![descriptor];
		let fence = self.publish_candidate(&candidate, expected_fence).await?;
		*self.state.write() = RuntimeState {
			root: candidate,
			fence,
		};
		Ok(true)
	}

	async fn publish_candidate(
		&self,
		candidate: &DatabaseRoot,
		expected_fence: AuthorityFence,
	) -> KernelResult<AuthorityFence> {
		let mut operation = [0; 16];
		self.bindings.platform.fill_random(&mut operation)?;
		let operation = OperationId(operation);
		let mut session = self.session.lock().await;
		match self
			.bindings
			.commits
			.commit(
				&mut session,
				expected_fence,
				CommitProposal {
					operation,
					root: candidate.encode()?,
				},
			)
			.await?
		{
			CommitOutcome::Confirmed(fence) => Ok(fence),
			CommitOutcome::Unknown => match self.bindings.commits.reconcile(operation).await? {
				ReconcileOutcome::Confirmed(fence) => Ok(fence),
				ReconcileOutcome::NotCommitted | ReconcileOutcome::Unknown => {
					Err(KernelError::new(
						ErrorCode::Unavailable,
						"maintenance publication remains indeterminate",
					))
				}
			},
			CommitOutcome::Conflict(_) => {
				Err(KernelError::new(ErrorCode::Conflict, "authority root changed"))
			}
			CommitOutcome::Fenced => Err(KernelError::new(ErrorCode::Fenced, "writer was fenced")),
		}
	}

	pub async fn get(&self, key: &[u8], selector: ReadSelector) -> KernelResult<Option<Bytes>> {
		let root = self.state.read().root.clone();
		let (version, evaluation_time) =
			root.temporal_context(selector, CommitTimestamp(self.bindings.platform.now().0))?;
		let mut best = root
			.rows
			.get(key)
			.and_then(|history| history.iter().find(|row| row.key.version <= version))
			.cloned();
		for descriptor in &root.tables {
			if key < descriptor.smallest_user_key.as_ref()
				|| key > descriptor.largest_user_key.as_ref()
			{
				continue;
			}
			let reader = TableReader::open(
				Arc::clone(&self.bindings.objects),
				descriptor.clone(),
				Arc::clone(&self.cache),
			)
			.await?;
			if let Some(row) = reader.get(key, version).await? {
				if best.as_ref().is_none_or(|current| row.key.version > current.key.version) {
					best = Some(row);
				}
			}
		}
		Ok(best.and_then(|row| {
			if row.expires_at.is_some_and(|expiry| expiry <= evaluation_time) {
				return None;
			}
			(row.key.kind == RowKind::Value).then_some(row.value)
		}))
	}

	pub async fn scan(
		&self,
		start: &[u8],
		end: &[u8],
		selector: ReadSelector,
	) -> KernelResult<Vec<(Bytes, Bytes)>> {
		if start > end {
			return Err(KernelError::new(ErrorCode::InvalidArgument, "invalid scan range"));
		}
		let root = self.state.read().root.clone();
		let (version, evaluation_time) =
			root.temporal_context(selector, CommitTimestamp(self.bindings.platform.now().0))?;
		Ok(self
			.collect_rows(&root, start, end, version)
			.await?
			.into_iter()
			.filter_map(|(key, history)| {
				let row = history.into_iter().find(|row| row.key.version <= version)?;
				if row.expires_at.is_some_and(|expiry| expiry <= evaluation_time) {
					return None;
				}
				(row.key.kind == RowKind::Value).then_some((key, row.value))
			})
			.collect())
	}

	pub async fn history(
		&self,
		key: &[u8],
	) -> KernelResult<Vec<(CommitVersion, CommitTimestamp, Option<Bytes>)>> {
		let root = self.state.read().root.clone();
		let mut end = key.to_vec();
		end.push(0);
		let history = self
			.collect_rows(&root, key, &end, CommitVersion(u64::MAX))
			.await?
			.remove(key)
			.unwrap_or_default();
		Ok(history
			.iter()
			.map(|row| {
				(
					row.key.version,
					row.commit_timestamp,
					(row.key.kind == RowKind::Value).then(|| row.value.clone()),
				)
			})
			.collect())
	}

	async fn collect_rows(
		&self,
		root: &DatabaseRoot,
		start: &[u8],
		end: &[u8],
		max_version: CommitVersion,
	) -> KernelResult<BTreeMap<Bytes, Vec<StorageRow>>> {
		let mut histories: BTreeMap<Bytes, Vec<StorageRow>> = BTreeMap::new();
		for (key, rows) in
			root.rows.range(Bytes::copy_from_slice(start)..Bytes::copy_from_slice(end))
		{
			histories.entry(key.clone()).or_default().extend(rows.iter().cloned());
		}
		for descriptor in &root.tables {
			if descriptor.largest_user_key.as_ref() < start
				|| descriptor.smallest_user_key.as_ref() >= end
			{
				continue;
			}
			let reader = TableReader::open(
				Arc::clone(&self.bindings.objects),
				descriptor.clone(),
				Arc::clone(&self.cache),
			)
			.await?;
			for row in reader.scan_rows(start, end, max_version).await? {
				histories.entry(row.key.user_key.clone()).or_default().push(row);
			}
		}
		for history in histories.values_mut() {
			history.sort_by_key(|row| std::cmp::Reverse(row.key.version));
			history.dedup_by_key(|row| row.key.version);
		}
		Ok(histories)
	}
}

struct Cursor {
	bytes: Bytes,
	offset: usize,
}

impl Cursor {
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
			.ok_or_else(|| corruption("database root field overflow"))?;
		if end > self.bytes.len() {
			return Err(corruption("database root field is truncated"));
		}
		let bytes = self.bytes.slice(self.offset..end);
		self.offset = end;
		Ok(bytes)
	}

	fn array16(&mut self) -> KernelResult<[u8; 16]> {
		self.bytes(16)?[..].try_into().map_err(|_| corruption("identifier is truncated"))
	}

	fn u16(&mut self) -> KernelResult<u16> {
		Ok(u16::from_be_bytes(
			self.bytes(2)?[..].try_into().map_err(|_| corruption("u16 is truncated"))?,
		))
	}

	fn u32(&mut self) -> KernelResult<u32> {
		Ok(u32::from_be_bytes(
			self.bytes(4)?[..].try_into().map_err(|_| corruption("u32 is truncated"))?,
		))
	}

	fn u64(&mut self) -> KernelResult<u64> {
		Ok(u64::from_be_bytes(
			self.bytes(8)?[..].try_into().map_err(|_| corruption("u64 is truncated"))?,
		))
	}

	fn finish(&self) -> KernelResult<()> {
		if self.offset != self.bytes.len() {
			return Err(corruption("database root contains trailing bytes"));
		}
		Ok(())
	}
}

fn read_u32(bytes: &[u8], offset: usize) -> KernelResult<u32> {
	let end = offset.checked_add(4).ok_or_else(|| corruption("checksum offset overflow"))?;
	Ok(u32::from_be_bytes(
		bytes
			.get(offset..end)
			.ok_or_else(|| corruption("checksum is truncated"))?
			.try_into()
			.map_err(|_| corruption("checksum is truncated"))?,
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
	use super::*;
	use crate::storage::{CommitStore, MemoryCommitStore, MemoryObjectStore, MemoryPlatform};

	#[test]
	fn root_codec_round_trip_and_truncation_are_fail_closed() {
		let mut root = DatabaseRoot::new(DatabaseId::from_u128(1), BranchId::from_u128(2));
		root.global_version = CommitVersion(1);
		root.branch_head = CommitVersion(1);
		root.last_timestamp = CommitTimestamp(10);
		root.timeline.push((CommitTimestamp(10), CommitVersion(1)));
		root.rows.insert(
			Bytes::from_static(b"a"),
			vec![StorageRow::new(
				InternalKey::new(Bytes::from_static(b"a"), CommitVersion(1), RowKind::Value)
					.unwrap(),
				CommitTimestamp(10),
				None,
				Bytes::from_static(b"value"),
			)
			.unwrap()],
		);
		let encoded = root.encode().unwrap();
		assert_eq!(DatabaseRoot::decode(encoded.clone()).unwrap(), root);
		for cut in 0..encoded.len() {
			assert_eq!(
				DatabaseRoot::decode(encoded.slice(..cut)).unwrap_err().code,
				ErrorCode::Corruption
			);
		}
	}

	#[tokio::test]
	async fn flush_owner_regression_replaces_mutable_rows_with_one_table_reference() {
		let database = KernelDatabase::open_or_create(Bindings {
			objects: Arc::new(MemoryObjectStore::new(2)),
			commits: Arc::new(MemoryCommitStore::new(Bytes::new())),
			platform: Arc::new(MemoryPlatform::new(0, 5, 1024 * 1024)),
		})
		.await
		.unwrap();
		database
			.commit(
				CommitVersion(0),
				CommitTimestamp(10),
				vec![WriteOperation::Put {
					key: Bytes::from_static(b"a"),
					value: Bytes::from_static(b"value"),
					expires_at: None,
				}],
			)
			.await
			.unwrap();
		assert!(database.flush().await.unwrap());
		{
			let state = database.state.read();
			assert!(state.root.rows.is_empty());
			assert_eq!(state.root.tables.len(), 1);
			assert_eq!(state.root.tables[0].owner.branch, state.root.main_branch);
		}
		database
			.commit(
				CommitVersion(1),
				CommitTimestamp(20),
				vec![WriteOperation::Put {
					key: Bytes::from_static(b"b"),
					value: Bytes::from_static(b"second"),
					expires_at: None,
				}],
			)
			.await
			.unwrap();
		assert!(database.flush().await.unwrap());
		assert!(database.compact_owned().await.unwrap());
		let state = database.state.read();
		assert!(state.root.rows.is_empty());
		assert_eq!(state.root.tables.len(), 1);
	}

	#[tokio::test]
	async fn commit_owner_regression_rejects_write_buffer_over_budget_before_publication() {
		let commits = Arc::new(MemoryCommitStore::new(Bytes::new()));
		let database = KernelDatabase::open_or_create(Bindings {
			objects: Arc::new(MemoryObjectStore::new(2)),
			commits: Arc::<MemoryCommitStore>::clone(&commits),
			platform: Arc::new(MemoryPlatform::new(0, 5, 16)),
		})
		.await
		.unwrap();
		let error = database
			.commit(
				CommitVersion(0),
				CommitTimestamp(10),
				vec![WriteOperation::Put {
					key: Bytes::from_static(b"large"),
					value: Bytes::from_static(b"far-over-budget"),
					expires_at: None,
				}],
			)
			.await
			.unwrap_err();
		assert_eq!(error.code, ErrorCode::ResourceExhausted);
		assert_eq!(database.head(), CommitVersion(0));
		let reader = CommitStore::open(&*commits, OpenMode::ReadOnly).await.unwrap();
		let root = DatabaseRoot::decode(commits.load_root(&reader).await.unwrap().bytes).unwrap();
		assert_eq!(root.branch_head, CommitVersion(0));
		assert!(root.rows.is_empty());
	}
}
