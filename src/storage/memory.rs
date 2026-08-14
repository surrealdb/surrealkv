use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use super::{
	AuthorityRoot,
	AuthoritySession,
	ByteRange,
	CommitCapabilities,
	CommitOutcome,
	CommitProposal,
	CommitStore,
	DeleteOutcome,
	ListCursor,
	MaintenanceHint,
	MemoryBudget,
	ObjectCapabilities,
	ObjectId,
	ObjectMetadata,
	ObjectPage,
	ObjectPrefix,
	ObjectStore,
	OpenMode,
	Platform,
	PutOutcome,
	PutRequest,
	ReconcileOutcome,
};
use crate::api::{
	AuthorityFence,
	DurabilityClass,
	ErrorCode,
	KernelError,
	KernelResult,
	MonotonicTime,
	OperationId,
	SessionId,
};

#[derive(Clone)]
pub(super) struct StoredObject {
	pub(super) bytes: Bytes,
	pub(super) attributes: BTreeMap<String, String>,
}

#[derive(Clone)]
pub(super) struct MemoryObjectImage {
	pub(super) objects: BTreeMap<ObjectId, StoredObject>,
	pub(super) page_size: usize,
}

pub(crate) struct MemoryObjectStore {
	objects: RwLock<BTreeMap<ObjectId, StoredObject>>,
	page_size: usize,
	durability: DurabilityClass,
}

impl MemoryObjectStore {
	pub(crate) fn new(page_size: usize) -> Self {
		assert!(page_size > 0);
		Self {
			objects: RwLock::new(BTreeMap::new()),
			page_size,
			durability: DurabilityClass::Ephemeral,
		}
	}

	pub(super) fn with_durability(mut self, durability: DurabilityClass) -> Self {
		self.durability = durability;
		self
	}

	pub(super) fn image(&self) -> MemoryObjectImage {
		MemoryObjectImage {
			objects: self.objects.read().clone(),
			page_size: self.page_size,
		}
	}

	pub(super) fn from_image(image: MemoryObjectImage, durability: DurabilityClass) -> Self {
		Self {
			objects: RwLock::new(image.objects),
			page_size: image.page_size,
			durability,
		}
	}
}

#[async_trait]
impl ObjectStore for MemoryObjectStore {
	fn capabilities(&self) -> ObjectCapabilities {
		ObjectCapabilities {
			ranged_reads: true,
			unique_put: true,
			paginated_list: true,
			idempotent_delete: true,
			durability: self.durability,
		}
	}

	async fn read_range(&self, id: &ObjectId, range: ByteRange) -> KernelResult<Bytes> {
		let objects = self.objects.read();
		let object = objects
			.get(id)
			.ok_or_else(|| KernelError::new(ErrorCode::NotFound, "object not found"))?;
		let len = object.bytes.len() as u64;
		if range.start > range.end || range.end > len {
			return Err(KernelError::new(ErrorCode::InvalidArgument, "range exceeds object"));
		}
		Ok(object.bytes.slice(range.start as usize..range.end as usize))
	}

	async fn put_unique(&self, request: PutRequest) -> KernelResult<PutOutcome> {
		let bytes = request.body.coalesce()?;
		let mut objects = self.objects.write();
		if let Some(existing) = objects.get(&request.id) {
			if existing.bytes == bytes && existing.attributes == request.attributes {
				return Ok(PutOutcome::AlreadyExistsSame);
			}
			return Err(KernelError::new(
				ErrorCode::AlreadyExists,
				"object ID already contains different bytes",
			));
		}
		objects.insert(
			request.id,
			StoredObject {
				bytes,
				attributes: request.attributes,
			},
		);
		Ok(PutOutcome::Created)
	}

	async fn metadata(&self, id: &ObjectId) -> KernelResult<ObjectMetadata> {
		let objects = self.objects.read();
		let object = objects
			.get(id)
			.ok_or_else(|| KernelError::new(ErrorCode::NotFound, "object not found"))?;
		Ok(ObjectMetadata {
			id: id.clone(),
			len: object.bytes.len() as u64,
			attributes: object.attributes.clone(),
		})
	}

	async fn list_page(
		&self,
		prefix: &ObjectPrefix,
		cursor: Option<ListCursor>,
	) -> KernelResult<ObjectPage> {
		let objects = self.objects.read();
		let after = cursor.as_ref().map(|cursor| cursor.0.as_str());
		let mut matching = objects
			.iter()
			.filter(|(id, _)| id.0.starts_with(&prefix.0))
			.filter(|(id, _)| after.is_none_or(|cursor| id.0.as_str() > cursor));
		let mut page = Vec::with_capacity(self.page_size);
		for (id, object) in matching.by_ref().take(self.page_size) {
			page.push(ObjectMetadata {
				id: id.clone(),
				len: object.bytes.len() as u64,
				attributes: object.attributes.clone(),
			});
		}
		let has_more = matching.next().is_some();
		let next = has_more.then(|| ListCursor(page.last().unwrap().id.0.clone()));
		Ok(ObjectPage {
			objects: page,
			next,
		})
	}

	async fn delete(&self, id: &ObjectId) -> KernelResult<DeleteOutcome> {
		Ok(if self.objects.write().remove(id).is_some() {
			DeleteOutcome::Deleted
		} else {
			DeleteOutcome::NotFound
		})
	}
}

#[derive(Clone)]
pub(super) struct MemoryCommitImage {
	pub(super) root: AuthorityRoot,
	pub(super) writer_epoch: u64,
	pub(super) next_session: u64,
	pub(super) operations: BTreeMap<OperationId, (AuthorityFence, Bytes)>,
}

pub(crate) struct MemoryCommitStore {
	state: Mutex<MemoryCommitImage>,
	durability: DurabilityClass,
}

impl MemoryCommitStore {
	pub(crate) fn new(initial_root: Bytes) -> Self {
		Self {
			state: Mutex::new(MemoryCommitImage {
				root: AuthorityRoot {
					fence: AuthorityFence(0),
					bytes: initial_root,
				},
				writer_epoch: 0,
				next_session: 0,
				operations: BTreeMap::new(),
			}),
			durability: DurabilityClass::Ephemeral,
		}
	}

	pub(super) fn with_durability(mut self, durability: DurabilityClass) -> Self {
		self.durability = durability;
		self
	}

	pub(super) fn image(&self) -> MemoryCommitImage {
		self.state.lock().clone()
	}

	pub(super) fn from_image(image: MemoryCommitImage, durability: DurabilityClass) -> Self {
		Self {
			state: Mutex::new(image),
			durability,
		}
	}
}

#[async_trait]
impl CommitStore for MemoryCommitStore {
	fn capabilities(&self) -> CommitCapabilities {
		CommitCapabilities {
			conditional_publish: true,
			writer_fencing: true,
			reconcile_unknown: true,
			durability: self.durability,
		}
	}

	async fn open(&self, mode: OpenMode) -> KernelResult<AuthoritySession> {
		let mut state = self.state.lock();
		state.next_session += 1;
		if mode == OpenMode::ReadWrite {
			state.writer_epoch += 1;
		}
		Ok(AuthoritySession {
			id: SessionId::from_u128(state.next_session as u128),
			writer_epoch: state.writer_epoch,
			mode,
		})
	}

	async fn load_root(&self, session: &AuthoritySession) -> KernelResult<AuthorityRoot> {
		let state = self.state.lock();
		if session.mode == OpenMode::ReadWrite && session.writer_epoch != state.writer_epoch {
			return Err(KernelError::new(ErrorCode::Fenced, "writer session is stale"));
		}
		Ok(state.root.clone())
	}

	async fn commit(
		&self,
		session: &mut AuthoritySession,
		expected: AuthorityFence,
		proposal: CommitProposal,
	) -> KernelResult<CommitOutcome> {
		let mut state = self.state.lock();
		if let Some((fence, root)) = state.operations.get(&proposal.operation) {
			if *root != proposal.root {
				return Err(KernelError::new(
					ErrorCode::InvalidArgument,
					"operation ID reused with a different proposal",
				));
			}
			return Ok(CommitOutcome::Confirmed(*fence));
		}
		if session.mode != OpenMode::ReadWrite || session.writer_epoch != state.writer_epoch {
			return Ok(CommitOutcome::Fenced);
		}
		if expected != state.root.fence {
			return Ok(CommitOutcome::Conflict(state.root.fence));
		}
		let next = AuthorityFence(state.root.fence.0.checked_add(1).ok_or_else(|| {
			KernelError::new(ErrorCode::ResourceExhausted, "authority fence exhausted")
		})?);
		let root = proposal.root;
		state.root = AuthorityRoot {
			fence: next,
			bytes: root.clone(),
		};
		state.operations.insert(proposal.operation, (next, root));
		Ok(CommitOutcome::Confirmed(next))
	}

	async fn reconcile(&self, operation: OperationId) -> KernelResult<ReconcileOutcome> {
		Ok(self
			.state
			.lock()
			.operations
			.get(&operation)
			.map(|(fence, _)| *fence)
			.map_or(ReconcileOutcome::NotCommitted, ReconcileOutcome::Confirmed))
	}
}

pub(crate) struct MemoryPlatform {
	now: AtomicU64,
	random: Mutex<u64>,
	memory_budget: MemoryBudget,
	maintenance: Mutex<VecDeque<MaintenanceHint>>,
}

#[derive(Clone, Copy)]
pub(super) struct MemoryPlatformImage {
	pub(super) now: u64,
	pub(super) random: u64,
	pub(super) memory_budget: MemoryBudget,
}

impl MemoryPlatform {
	pub(crate) fn new(now: u64, random_seed: u64, memory_budget: u64) -> Self {
		Self {
			now: AtomicU64::new(now),
			random: Mutex::new(random_seed),
			memory_budget: MemoryBudget {
				bytes: memory_budget,
			},
			maintenance: Mutex::new(VecDeque::new()),
		}
	}

	pub(super) fn image(&self) -> MemoryPlatformImage {
		MemoryPlatformImage {
			now: self.now.load(Ordering::SeqCst),
			random: *self.random.lock(),
			memory_budget: self.memory_budget,
		}
	}

	pub(super) fn from_image(image: MemoryPlatformImage) -> Self {
		Self {
			now: AtomicU64::new(image.now),
			random: Mutex::new(image.random),
			memory_budget: image.memory_budget,
			maintenance: Mutex::new(VecDeque::new()),
		}
	}

	pub(crate) fn advance(&self, delta: u64) {
		self.now.fetch_add(delta, Ordering::SeqCst);
	}

	pub(crate) fn take_maintenance(&self) -> Vec<MaintenanceHint> {
		self.maintenance.lock().drain(..).collect()
	}
}

#[async_trait]
impl Platform for MemoryPlatform {
	fn now(&self) -> MonotonicTime {
		MonotonicTime(self.now.load(Ordering::SeqCst))
	}

	fn fill_random(&self, out: &mut [u8]) -> KernelResult<()> {
		let mut state = self.random.lock();
		for byte in out {
			// xorshift64*: deterministic and sufficient for injected test entropy.
			*state ^= *state >> 12;
			*state ^= *state << 25;
			*state ^= *state >> 27;
			*byte = state.wrapping_mul(0x2545_f491_4f6c_dd1d) as u8;
		}
		Ok(())
	}

	fn memory_budget(&self) -> MemoryBudget {
		self.memory_budget
	}

	async fn schedule_maintenance(&self, hint: MaintenanceHint) -> KernelResult<()> {
		self.maintenance.lock().push_back(hint);
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn object_owner_regression_covers_range_unique_put_and_delete() {
		let store = MemoryObjectStore::new(2);
		let id = ObjectId("object".to_string());
		let request = PutRequest {
			id: id.clone(),
			body: super::super::ObjectBody::new(vec![
				Bytes::from_static(b"abc"),
				Bytes::from_static(b"def"),
			])
			.unwrap(),
			attributes: BTreeMap::new(),
		};
		assert_eq!(store.put_unique(request.clone()).await.unwrap(), PutOutcome::Created);
		assert_eq!(store.put_unique(request).await.unwrap(), PutOutcome::AlreadyExistsSame);
		assert_eq!(
			store.read_range(&id, ByteRange::new(2, 5).unwrap()).await.unwrap(),
			Bytes::from_static(b"cde")
		);
		assert_eq!(store.delete(&id).await.unwrap(), DeleteOutcome::Deleted);
		assert_eq!(store.delete(&id).await.unwrap(), DeleteOutcome::NotFound);
	}

	#[tokio::test]
	async fn authority_owner_regression_covers_conflict_and_writer_fence() {
		let store = MemoryCommitStore::new(Bytes::new());
		let mut first = store.open(OpenMode::ReadWrite).await.unwrap();
		let confirmed = store
			.commit(
				&mut first,
				AuthorityFence(0),
				CommitProposal {
					operation: OperationId::from_u128(1),
					root: Bytes::from_static(b"one"),
				},
			)
			.await
			.unwrap();
		assert_eq!(confirmed, CommitOutcome::Confirmed(AuthorityFence(1)));

		let mut second = store.open(OpenMode::ReadWrite).await.unwrap();
		assert_eq!(
			store
				.commit(
					&mut first,
					AuthorityFence(1),
					CommitProposal {
						operation: OperationId::from_u128(2),
						root: Bytes::from_static(b"stale"),
					},
				)
				.await
				.unwrap(),
			CommitOutcome::Fenced
		);
		assert_eq!(
			store
				.commit(
					&mut second,
					AuthorityFence(0),
					CommitProposal {
						operation: OperationId::from_u128(3),
						root: Bytes::from_static(b"conflict"),
					},
				)
				.await
				.unwrap(),
			CommitOutcome::Conflict(AuthorityFence(1))
		);
	}

	#[tokio::test]
	async fn authority_owner_regression_rejects_operation_id_reuse_with_new_bytes() {
		let store = MemoryCommitStore::new(Bytes::new());
		let mut session = store.open(OpenMode::ReadWrite).await.unwrap();
		let operation = OperationId::from_u128(7);
		store
			.commit(
				&mut session,
				AuthorityFence(0),
				CommitProposal {
					operation,
					root: Bytes::from_static(b"first"),
				},
			)
			.await
			.unwrap();

		let error = store
			.commit(
				&mut session,
				AuthorityFence(1),
				CommitProposal {
					operation,
					root: Bytes::from_static(b"different"),
				},
			)
			.await
			.unwrap_err();
		assert_eq!(error.code, ErrorCode::InvalidArgument);
		assert_eq!(store.load_root(&session).await.unwrap().bytes, b"first"[..]);
	}
}
