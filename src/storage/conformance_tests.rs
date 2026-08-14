//! Backend conformance for the three injected roles: one contract, asserted
//! identically against every implementation, plus the fault and determinism
//! facts the simulated backend exists to prove.
//!
//! These moved here from the retired prototype's `testkit.rs` (see
//! `docs/removed-surfaces.md`). They depend on `storage` and the shared id types
//! ONLY — no engine, no reference model — which is why they survived the
//! prototype's deletion unchanged. PF3 grows this file into the parity suite by
//! adding the same-assertions-per-backend lane over `Tree`.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use test_log::test;

#[cfg(not(target_arch = "wasm32"))]
use super::LocalObjectStore;
use super::{
	BindingRequirements,
	Bindings,
	ByteRange,
	CommitOutcome,
	CommitProposal,
	CommitStore,
	DeleteOutcome,
	FaultPoint,
	MaintenanceHint,
	MemoryCommitStore,
	MemoryObjectStore,
	MemoryPlatform,
	ObjectBody,
	ObjectId,
	ObjectPrefix,
	ObjectStore,
	OpenMode,
	Platform,
	PutOutcome,
	PutRequest,
	ReconcileOutcome,
	SimHarness,
};
use crate::api::{AuthorityFence, BranchId, DurabilityClass, ErrorCode, OperationId};

fn request(id: &str, value: &'static [u8]) -> PutRequest {
	PutRequest {
		id: ObjectId(id.to_string()),
		body: ObjectBody::from_bytes(Bytes::from_static(value)),
		attributes: BTreeMap::from([("kind".to_string(), "table".to_string())]),
	}
}

async fn assert_object_contract(store: Arc<dyn ObjectStore>) {
	assert_eq!(
		store.put_unique(request("tables/a", b"abcdef")).await.unwrap(),
		PutOutcome::Created
	);
	assert_eq!(
		store.put_unique(request("tables/a", b"abcdef")).await.unwrap(),
		PutOutcome::AlreadyExistsSame
	);
	let collision = store.put_unique(request("tables/a", b"different")).await.unwrap_err();
	assert_eq!(collision.code, ErrorCode::AlreadyExists);

	store.put_unique(request("tables/b", b"b")).await.unwrap();
	store.put_unique(request("tables/c", b"cc")).await.unwrap();
	store.put_unique(request("other/a", b"ignored")).await.unwrap();

	let range = store
		.read_range(&ObjectId("tables/a".to_string()), ByteRange::new(1, 4).unwrap())
		.await
		.unwrap();
	assert_eq!(range, Bytes::from_static(b"bcd"));
	assert_eq!(store.metadata(&ObjectId("tables/c".to_string())).await.unwrap().len, 2);

	let first = store.list_page(&ObjectPrefix("tables/".to_string()), None).await.unwrap();
	assert_eq!(first.objects.len(), 2, "fixture must exercise pagination");
	assert!(first.next.is_some());
	let second = store.list_page(&ObjectPrefix("tables/".to_string()), first.next).await.unwrap();
	assert_eq!(second.objects.len(), 1);
	assert!(second.next.is_none());

	let id = ObjectId("tables/b".to_string());
	assert_eq!(store.delete(&id).await.unwrap(), DeleteOutcome::Deleted);
	assert_eq!(store.delete(&id).await.unwrap(), DeleteOutcome::NotFound);
}

async fn assert_commit_contract(store: Arc<dyn CommitStore>) {
	let mut first = store.open(OpenMode::ReadWrite).await.unwrap();
	let initial = store.load_root(&first).await.unwrap();
	assert_eq!(initial.fence, AuthorityFence(0));

	let operation = OperationId::from_u128(1);
	let outcome = store
		.commit(
			&mut first,
			AuthorityFence(0),
			CommitProposal {
				operation,
				root: Bytes::from_static(b"root-1"),
			},
		)
		.await
		.unwrap();
	assert_eq!(outcome, CommitOutcome::Confirmed(AuthorityFence(1)));
	assert_eq!(
		store
			.commit(
				&mut first,
				AuthorityFence(0),
				CommitProposal {
					operation,
					root: Bytes::from_static(b"root-1"),
				},
			)
			.await
			.unwrap(),
		CommitOutcome::Confirmed(AuthorityFence(1)),
		"an exact operation retry must return its original receipt"
	);
	let reused = store
		.commit(
			&mut first,
			AuthorityFence(1),
			CommitProposal {
				operation,
				root: Bytes::from_static(b"not-root-1"),
			},
		)
		.await
		.unwrap_err();
	assert_eq!(reused.code, ErrorCode::InvalidArgument);
	assert_eq!(
		store.reconcile(operation).await.unwrap(),
		ReconcileOutcome::Confirmed(AuthorityFence(1))
	);

	let conflict = store
		.commit(
			&mut first,
			AuthorityFence(0),
			CommitProposal {
				operation: OperationId::from_u128(2),
				root: Bytes::from_static(b"conflict"),
			},
		)
		.await
		.unwrap();
	assert_eq!(conflict, CommitOutcome::Conflict(AuthorityFence(1)));

	let mut second = store.open(OpenMode::ReadWrite).await.unwrap();
	let fenced = store
		.commit(
			&mut first,
			AuthorityFence(1),
			CommitProposal {
				operation: OperationId::from_u128(3),
				root: Bytes::from_static(b"stale-writer"),
			},
		)
		.await
		.unwrap();
	assert_eq!(fenced, CommitOutcome::Fenced);

	let confirmed = store
		.commit(
			&mut second,
			AuthorityFence(1),
			CommitProposal {
				operation: OperationId::from_u128(4),
				root: Bytes::from_static(b"root-2"),
			},
		)
		.await
		.unwrap();
	assert_eq!(confirmed, CommitOutcome::Confirmed(AuthorityFence(2)));
}

#[test(tokio::test)]
async fn memory_and_sim_objects_pass_the_same_contract() {
	assert_object_contract(Arc::new(MemoryObjectStore::new(2))).await;
	let sim = SimHarness::new(2);
	assert_object_contract(sim.bindings().objects).await;
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn local_objects_pass_the_peer_object_contract() {
	let directory = tempfile::tempdir().unwrap();
	assert_object_contract(Arc::new(
		LocalObjectStore::open(directory.path().join("objects"), 2).unwrap(),
	))
	.await;
}

#[test(tokio::test)]
async fn memory_and_sim_authority_pass_the_same_contract() {
	assert_commit_contract(Arc::new(MemoryCommitStore::new(Bytes::new()))).await;
	let sim = SimHarness::new(2);
	assert_commit_contract(sim.bindings().commits).await;
}

#[test(tokio::test)]
async fn capability_mismatch_fails_before_any_mutation() {
	let objects = Arc::new(MemoryObjectStore::new(2));
	let bindings = Bindings {
		objects: Arc::<MemoryObjectStore>::clone(&objects),
		commits: Arc::new(MemoryCommitStore::new(Bytes::new())),
		platform: Arc::new(MemoryPlatform::new(0, 1, 1024)),
	};
	let error = bindings
		.validate(BindingRequirements {
			ranged_reads: true,
			unique_put: true,
			paginated_list: true,
			idempotent_delete: true,
			conditional_publish: true,
			writer_fencing: true,
			reconcile_unknown: true,
			minimum_durability: DurabilityClass::CrashDurable,
		})
		.unwrap_err();
	assert_eq!(error.code, ErrorCode::CapabilityMismatch);
	let missing = objects.metadata(&ObjectId("never-created".to_string())).await.unwrap_err();
	assert_eq!(missing.code, ErrorCode::NotFound);
}

#[test]
fn sim_declares_and_satisfies_crash_durable_binding_requirements() {
	let sim = SimHarness::new(2);
	sim.bindings()
		.validate(BindingRequirements {
			ranged_reads: true,
			unique_put: true,
			paginated_list: true,
			idempotent_delete: true,
			conditional_publish: true,
			writer_fencing: true,
			reconcile_unknown: true,
			minimum_durability: DurabilityClass::CrashDurable,
		})
		.unwrap();
}

#[test(tokio::test)]
async fn sim_unknown_commit_reconciles_across_crash() {
	let sim = SimHarness::new(2);
	let bindings = sim.bindings();
	let mut session = bindings.commits.open(OpenMode::ReadWrite).await.unwrap();
	let operation = OperationId::from_u128(10);
	sim.inject(FaultPoint::CommitAfter);
	let outcome = bindings
		.commits
		.commit(
			&mut session,
			AuthorityFence(0),
			CommitProposal {
				operation,
				root: Bytes::from_static(b"installed-before-timeout"),
			},
		)
		.await
		.unwrap();
	assert_eq!(outcome, CommitOutcome::Unknown);
	assert_eq!(
		bindings.commits.reconcile(operation).await.unwrap(),
		ReconcileOutcome::Confirmed(AuthorityFence(1))
	);

	let crashed = sim.crash();
	let after = crashed.bindings();
	assert_eq!(
		after.commits.reconcile(operation).await.unwrap(),
		ReconcileOutcome::Confirmed(AuthorityFence(1))
	);
	let reader = after.commits.open(OpenMode::ReadOnly).await.unwrap();
	assert_eq!(
		after.commits.load_root(&reader).await.unwrap().bytes,
		b"installed-before-timeout"[..]
	);
}

#[test(tokio::test)]
async fn sim_unknown_before_commit_reconciles_as_not_committed() {
	let sim = SimHarness::new(2);
	let bindings = sim.bindings();
	let mut session = bindings.commits.open(OpenMode::ReadWrite).await.unwrap();
	let operation = OperationId::from_u128(11);
	sim.inject(FaultPoint::CommitBefore);
	assert_eq!(
		bindings
			.commits
			.commit(
				&mut session,
				AuthorityFence(0),
				CommitProposal {
					operation,
					root: Bytes::from_static(b"not-installed"),
				},
			)
			.await
			.unwrap(),
		CommitOutcome::Unknown
	);
	assert_eq!(
		bindings.commits.reconcile(operation).await.unwrap(),
		ReconcileOutcome::NotCommitted
	);
}

#[test]
fn typed_identifiers_have_stable_byte_order() {
	let first = BranchId::from_u128(1);
	let second = BranchId::from_u128(2);
	assert!(first < second);
	assert_eq!(first.0, 1u128.to_be_bytes());
}

#[test(tokio::test)]
async fn sim_pre_and_post_object_faults_have_distinct_durable_results() {
	let sim = SimHarness::new(2);
	let bindings = sim.bindings();

	sim.inject(FaultPoint::ObjectPutBefore);
	assert!(bindings.objects.put_unique(request("before", b"x")).await.is_err());
	assert_eq!(
		bindings.objects.metadata(&ObjectId("before".to_string())).await.unwrap_err().code,
		ErrorCode::NotFound
	);

	sim.inject(FaultPoint::ObjectPutAfter);
	assert!(bindings.objects.put_unique(request("after", b"y")).await.is_err());
	let crashed = sim.crash();
	assert_eq!(
		crashed
			.bindings()
			.objects
			.read_range(&ObjectId("after".to_string()), ByteRange::new(0, 1).unwrap())
			.await
			.unwrap(),
		Bytes::from_static(b"y")
	);
}

#[test(tokio::test)]
async fn deterministic_platform_clock_randomness_and_scheduler() {
	let first = MemoryPlatform::new(10, 42, 4096);
	let second = MemoryPlatform::new(10, 42, 4096);
	let mut a = [0; 16];
	let mut b = [0; 16];
	first.fill_random(&mut a).unwrap();
	second.fill_random(&mut b).unwrap();
	assert_eq!(a, b);
	assert_ne!(a, [0; 16], "fixture must consume deterministic entropy");

	first.advance(5);
	assert_eq!(first.now().0, 15);
	assert_eq!(first.memory_budget().bytes, 4096);
	first.schedule_maintenance(MaintenanceHint::Flush).await.unwrap();
	assert_eq!(first.take_maintenance(), vec![MaintenanceHint::Flush]);
}

#[test(tokio::test)]
async fn sim_crash_preserves_clock_and_entropy_progress_but_drops_scheduled_work() {
	let sim = SimHarness::new(2);
	let before = sim.bindings();
	let mut first = [0; 16];
	before.platform.fill_random(&mut first).unwrap();
	sim.advance(9);
	before.platform.schedule_maintenance(MaintenanceHint::Compact).await.unwrap();

	let crashed = sim.crash();
	let after = crashed.bindings();
	let mut second = [0; 16];
	after.platform.fill_random(&mut second).unwrap();
	assert_ne!(first, second, "crash must not rewind ID entropy");
	assert_eq!(after.platform.now().0, 9, "durable model time must not rewind");

	crashed.inject(FaultPoint::Maintenance);
	let error = after.platform.schedule_maintenance(MaintenanceHint::Reclaim).await.unwrap_err();
	assert_eq!(error.code, ErrorCode::Unavailable);
}
