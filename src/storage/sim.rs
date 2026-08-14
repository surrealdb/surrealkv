use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;

use super::memory::{MemoryCommitImage, MemoryObjectImage, MemoryPlatformImage};
use super::{
	AuthorityRoot,
	AuthoritySession,
	Bindings,
	ByteRange,
	CommitCapabilities,
	CommitOutcome,
	CommitProposal,
	CommitStore,
	DeleteOutcome,
	ListCursor,
	MaintenanceHint,
	MemoryBudget,
	MemoryCommitStore,
	MemoryObjectStore,
	MemoryPlatform,
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
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FaultPoint {
	ObjectPutBefore,
	ObjectPutAfter,
	ObjectDeleteBefore,
	ObjectDeleteAfter,
	CommitBefore,
	CommitAfter,
	Maintenance,
}

#[derive(Default)]
struct FaultScript {
	points: Mutex<VecDeque<FaultPoint>>,
}

impl FaultScript {
	fn push(&self, point: FaultPoint) {
		self.points.lock().push_back(point);
	}

	fn take(&self, point: FaultPoint) -> bool {
		let mut points = self.points.lock();
		if points.front() == Some(&point) {
			points.pop_front();
			true
		} else {
			false
		}
	}
}

struct SimObjectStore {
	inner: Arc<MemoryObjectStore>,
	faults: Arc<FaultScript>,
}

#[async_trait]
impl ObjectStore for SimObjectStore {
	fn capabilities(&self) -> ObjectCapabilities {
		self.inner.capabilities()
	}

	async fn read_range(&self, id: &ObjectId, range: ByteRange) -> KernelResult<Bytes> {
		self.inner.read_range(id, range).await
	}

	async fn put_unique(&self, request: PutRequest) -> KernelResult<PutOutcome> {
		if self.faults.take(FaultPoint::ObjectPutBefore) {
			return Err(KernelError::new(ErrorCode::Unavailable, "injected pre-put failure"));
		}
		let result = self.inner.put_unique(request).await;
		if self.faults.take(FaultPoint::ObjectPutAfter) {
			return Err(KernelError::new(ErrorCode::Unavailable, "injected post-put failure"));
		}
		result
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
		if self.faults.take(FaultPoint::ObjectDeleteBefore) {
			return Err(KernelError::new(ErrorCode::Unavailable, "injected pre-delete failure"));
		}
		let result = self.inner.delete(id).await;
		if self.faults.take(FaultPoint::ObjectDeleteAfter) {
			return Err(KernelError::new(ErrorCode::Unavailable, "injected post-delete failure"));
		}
		result
	}
}

struct SimCommitStore {
	inner: Arc<MemoryCommitStore>,
	faults: Arc<FaultScript>,
}

#[async_trait]
impl CommitStore for SimCommitStore {
	fn capabilities(&self) -> CommitCapabilities {
		self.inner.capabilities()
	}

	async fn open(&self, mode: OpenMode) -> KernelResult<AuthoritySession> {
		self.inner.open(mode).await
	}

	async fn load_root(&self, session: &AuthoritySession) -> KernelResult<AuthorityRoot> {
		self.inner.load_root(session).await
	}

	async fn commit(
		&self,
		session: &mut AuthoritySession,
		expected: AuthorityFence,
		proposal: CommitProposal,
	) -> KernelResult<CommitOutcome> {
		if self.faults.take(FaultPoint::CommitBefore) {
			return Ok(CommitOutcome::Unknown);
		}
		let outcome = self.inner.commit(session, expected, proposal).await?;
		if self.faults.take(FaultPoint::CommitAfter) {
			return Ok(CommitOutcome::Unknown);
		}
		Ok(outcome)
	}

	async fn reconcile(&self, operation: OperationId) -> KernelResult<ReconcileOutcome> {
		self.inner.reconcile(operation).await
	}
}

struct SimPlatform {
	inner: Arc<MemoryPlatform>,
	faults: Arc<FaultScript>,
}

#[async_trait]
impl Platform for SimPlatform {
	fn now(&self) -> MonotonicTime {
		self.inner.now()
	}

	fn fill_random(&self, out: &mut [u8]) -> KernelResult<()> {
		self.inner.fill_random(out)
	}

	fn memory_budget(&self) -> MemoryBudget {
		self.inner.memory_budget()
	}

	async fn schedule_maintenance(&self, hint: MaintenanceHint) -> KernelResult<()> {
		if self.faults.take(FaultPoint::Maintenance) {
			return Err(KernelError::new(
				ErrorCode::Unavailable,
				"injected maintenance scheduling failure",
			));
		}
		self.inner.schedule_maintenance(hint).await
	}
}

pub(crate) struct SimHarness {
	objects: Arc<MemoryObjectStore>,
	commits: Arc<MemoryCommitStore>,
	platform: Arc<MemoryPlatform>,
	faults: Arc<FaultScript>,
}

impl SimHarness {
	pub(crate) fn new(page_size: usize) -> Self {
		Self::from_images(
			MemoryObjectStore::new(page_size)
				.with_durability(DurabilityClass::CrashDurable)
				.image(),
			MemoryCommitStore::new(Bytes::new())
				.with_durability(DurabilityClass::CrashDurable)
				.image(),
			MemoryPlatform::new(0, 0x5eed, 64 * 1024 * 1024).image(),
		)
	}

	fn from_images(
		objects: MemoryObjectImage,
		commits: MemoryCommitImage,
		platform: MemoryPlatformImage,
	) -> Self {
		Self {
			objects: Arc::new(MemoryObjectStore::from_image(
				objects,
				DurabilityClass::CrashDurable,
			)),
			commits: Arc::new(MemoryCommitStore::from_image(
				commits,
				DurabilityClass::CrashDurable,
			)),
			platform: Arc::new(MemoryPlatform::from_image(platform)),
			faults: Arc::new(FaultScript::default()),
		}
	}

	pub(crate) fn bindings(&self) -> Bindings {
		Bindings {
			objects: Arc::new(SimObjectStore {
				inner: Arc::clone(&self.objects),
				faults: Arc::clone(&self.faults),
			}),
			commits: Arc::new(SimCommitStore {
				inner: Arc::clone(&self.commits),
				faults: Arc::clone(&self.faults),
			}),
			platform: Arc::new(SimPlatform {
				inner: Arc::clone(&self.platform),
				faults: Arc::clone(&self.faults),
			}),
		}
	}

	pub(crate) fn inject(&self, point: FaultPoint) {
		self.faults.push(point);
	}

	pub(crate) fn advance(&self, delta: u64) {
		self.platform.advance(delta);
	}

	/// Materializes a crash by preserving only durable object/authority state.
	/// Fault scripts and scheduled maintenance are process state and disappear.
	pub(crate) fn crash(&self) -> Self {
		Self::from_images(self.objects.image(), self.commits.image(), self.platform.image())
	}
}
