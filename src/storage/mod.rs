#![allow(dead_code)] // Contracts include lifecycle/adapter operations consumed by later phases.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};

use super::api::{
	AuthorityFence,
	DurabilityClass,
	KernelError,
	KernelResult,
	MonotonicTime,
	OperationId,
	SessionId,
};

mod memory;
#[allow(unused_imports)] // P1 keeps the private backend ready for the P3 runtime cutover.
pub(crate) use memory::{MemoryCommitStore, MemoryObjectStore, MemoryPlatform};

#[cfg(not(target_arch = "wasm32"))]
mod local;
#[cfg(not(target_arch = "wasm32"))]
#[allow(unused_imports)] // P2 backend becomes reachable at the P3 runtime cutover.
pub(crate) use local::LocalObjectStore;
#[cfg(not(target_arch = "wasm32"))]
mod local_commit;
#[cfg(not(target_arch = "wasm32"))]
#[allow(unused_imports)] // P3 private runtime consumes this before public cutover.
pub(crate) use local_commit::LocalCommitStore;
#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod native;
#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
pub(crate) use native::NativePlatform;

#[cfg(test)]
mod sim;
#[cfg(test)]
pub(crate) use sim::{FaultPoint, SimHarness};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ObjectId(pub(crate) String);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObjectPrefix(pub(crate) String);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ListCursor(pub(crate) String);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ByteRange {
	pub(crate) start: u64,
	pub(crate) end: u64,
}

impl ByteRange {
	pub(crate) fn new(start: u64, end: u64) -> KernelResult<Self> {
		if start > end {
			return Err(KernelError::new(
				super::api::ErrorCode::InvalidArgument,
				"range start exceeds end",
			));
		}
		Ok(Self {
			start,
			end,
		})
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObjectBody {
	chunks: Vec<Bytes>,
	len: u64,
}

impl ObjectBody {
	pub(crate) fn new(chunks: Vec<Bytes>) -> KernelResult<Self> {
		let len = chunks.iter().try_fold(0u64, |total, chunk| {
			total.checked_add(chunk.len() as u64).ok_or_else(|| {
				KernelError::new(
					super::api::ErrorCode::ResourceExhausted,
					"object body is too large",
				)
			})
		})?;
		Ok(Self {
			chunks,
			len,
		})
	}

	pub(crate) fn from_bytes(bytes: Bytes) -> Self {
		Self {
			len: bytes.len() as u64,
			chunks: vec![bytes],
		}
	}

	pub(crate) fn len(&self) -> u64 {
		self.len
	}

	pub(crate) fn chunks(&self) -> &[Bytes] {
		&self.chunks
	}

	pub(crate) fn coalesce(&self) -> KernelResult<Bytes> {
		let capacity = usize::try_from(self.len).map_err(|_| {
			KernelError::new(
				super::api::ErrorCode::ResourceExhausted,
				"object exceeds address space",
			)
		})?;
		let mut output = BytesMut::with_capacity(capacity);
		for chunk in &self.chunks {
			output.extend_from_slice(chunk);
		}
		Ok(output.freeze())
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PutRequest {
	pub(crate) id: ObjectId,
	pub(crate) body: ObjectBody,
	pub(crate) attributes: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PutOutcome {
	Created,
	AlreadyExistsSame,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObjectMetadata {
	pub(crate) id: ObjectId,
	pub(crate) len: u64,
	pub(crate) attributes: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ObjectPage {
	pub(crate) objects: Vec<ObjectMetadata>,
	pub(crate) next: Option<ListCursor>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeleteOutcome {
	Deleted,
	NotFound,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ObjectCapabilities {
	pub(crate) ranged_reads: bool,
	pub(crate) unique_put: bool,
	pub(crate) paginated_list: bool,
	pub(crate) idempotent_delete: bool,
	pub(crate) durability: DurabilityClass,
}

#[async_trait]
pub(crate) trait ObjectStore: Send + Sync {
	fn capabilities(&self) -> ObjectCapabilities;
	async fn read_range(&self, id: &ObjectId, range: ByteRange) -> KernelResult<Bytes>;
	async fn put_unique(&self, request: PutRequest) -> KernelResult<PutOutcome>;
	async fn metadata(&self, id: &ObjectId) -> KernelResult<ObjectMetadata>;
	async fn list_page(
		&self,
		prefix: &ObjectPrefix,
		cursor: Option<ListCursor>,
	) -> KernelResult<ObjectPage>;
	async fn delete(&self, id: &ObjectId) -> KernelResult<DeleteOutcome>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OpenMode {
	ReadOnly,
	ReadWrite,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthoritySession {
	pub(crate) id: SessionId,
	pub(crate) writer_epoch: u64,
	pub(crate) mode: OpenMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AuthorityRoot {
	pub(crate) fence: AuthorityFence,
	pub(crate) bytes: Bytes,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CommitProposal {
	pub(crate) operation: OperationId,
	pub(crate) root: Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommitOutcome {
	Confirmed(AuthorityFence),
	Conflict(AuthorityFence),
	Unknown,
	Fenced,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReconcileOutcome {
	Confirmed(AuthorityFence),
	NotCommitted,
	#[allow(dead_code)] // Required for remote authorities that cannot yet prove either outcome.
	Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CommitCapabilities {
	pub(crate) conditional_publish: bool,
	pub(crate) writer_fencing: bool,
	pub(crate) reconcile_unknown: bool,
	pub(crate) durability: DurabilityClass,
}

#[async_trait]
pub(crate) trait CommitStore: Send + Sync {
	fn capabilities(&self) -> CommitCapabilities;
	async fn open(&self, mode: OpenMode) -> KernelResult<AuthoritySession>;
	async fn load_root(&self, session: &AuthoritySession) -> KernelResult<AuthorityRoot>;
	async fn commit(
		&self,
		session: &mut AuthoritySession,
		expected: AuthorityFence,
		proposal: CommitProposal,
	) -> KernelResult<CommitOutcome>;
	async fn reconcile(&self, operation: OperationId) -> KernelResult<ReconcileOutcome>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MemoryBudget {
	pub(crate) bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaintenanceHint {
	Flush,
	Compact,
	Reclaim,
}

#[async_trait]
pub(crate) trait Platform: Send + Sync {
	fn now(&self) -> MonotonicTime;
	fn fill_random(&self, out: &mut [u8]) -> KernelResult<()>;
	fn memory_budget(&self) -> MemoryBudget;
	async fn schedule_maintenance(&self, hint: MaintenanceHint) -> KernelResult<()>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BindingRequirements {
	pub(crate) ranged_reads: bool,
	pub(crate) unique_put: bool,
	pub(crate) paginated_list: bool,
	pub(crate) idempotent_delete: bool,
	pub(crate) conditional_publish: bool,
	pub(crate) writer_fencing: bool,
	pub(crate) reconcile_unknown: bool,
	pub(crate) minimum_durability: DurabilityClass,
}

pub(crate) struct Bindings {
	pub(crate) objects: Arc<dyn ObjectStore>,
	pub(crate) commits: Arc<dyn CommitStore>,
	pub(crate) platform: Arc<dyn Platform>,
}

impl Bindings {
	pub(crate) fn validate(&self, required: BindingRequirements) -> KernelResult<()> {
		use super::api::ErrorCode;

		let objects = self.objects.capabilities();
		let commits = self.commits.capabilities();
		let durability_rank = |value| match value {
			DurabilityClass::Ephemeral => 0,
			DurabilityClass::CrashDurable => 1,
		};

		let valid = (!required.ranged_reads || objects.ranged_reads)
			&& (!required.unique_put || objects.unique_put)
			&& (!required.paginated_list || objects.paginated_list)
			&& (!required.idempotent_delete || objects.idempotent_delete)
			&& (!required.conditional_publish || commits.conditional_publish)
			&& (!required.writer_fencing || commits.writer_fencing)
			&& (!required.reconcile_unknown || commits.reconcile_unknown)
			&& durability_rank(objects.durability) >= durability_rank(required.minimum_durability)
			&& durability_rank(commits.durability) >= durability_rank(required.minimum_durability);

		if !valid {
			return Err(KernelError::new(
				ErrorCode::CapabilityMismatch,
				"bindings do not satisfy database requirements",
			));
		}
		Ok(())
	}
}
