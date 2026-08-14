//! Deterministic contract fixtures and reference models.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use test_log::test;

use super::api::{
	AuthorityFence,
	BranchGeneration,
	BranchId,
	CommitTimestamp,
	CommitVersion,
	DurabilityClass,
	ErrorCode,
	OperationId,
	TableId,
};
use super::branch::{BranchModel, ReadSelector, WriteOperation};
use super::database::KernelDatabase;
use super::format::{InternalKey, RowKind, StorageRow};
use super::storage::{
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
#[cfg(not(target_arch = "wasm32"))]
use super::storage::{LocalCommitStore, LocalObjectStore};
use super::table::{BlockCache, TableBuilder, TableOwner, TableReader};

fn request(id: &str, value: &'static [u8]) -> PutRequest {
	PutRequest {
		id: ObjectId(id.to_string()),
		body: ObjectBody::from_bytes(Bytes::from_static(value)),
		attributes: BTreeMap::from([("kind".to_string(), "table".to_string())]),
	}
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

fn memory_bindings(now: u64) -> Bindings {
	Bindings {
		objects: Arc::new(MemoryObjectStore::new(2)),
		commits: Arc::new(MemoryCommitStore::new(Bytes::new())),
		platform: Arc::new(MemoryPlatform::new(now, 0x1234, 64 * 1024 * 1024)),
	}
}

#[test(tokio::test)]
async fn empty_database_identity_is_published_before_open_returns() {
	let commits = Arc::new(MemoryCommitStore::new(Bytes::new()));
	let first = KernelDatabase::open_or_create(Bindings {
		objects: Arc::new(MemoryObjectStore::new(2)),
		commits: Arc::<MemoryCommitStore>::clone(&commits),
		platform: Arc::new(MemoryPlatform::new(0, 11, 1024)),
	})
	.await
	.unwrap();
	let identity = first.database_id();
	drop(first);
	let reopened = KernelDatabase::open_or_create(Bindings {
		objects: Arc::new(MemoryObjectStore::new(2)),
		commits,
		platform: Arc::new(MemoryPlatform::new(0, 99, 1024)),
	})
	.await
	.unwrap();
	assert_eq!(reopened.database_id(), identity);
	assert_eq!(reopened.head(), CommitVersion(0));
}

#[test(tokio::test)]
async fn single_branch_memory_spine_matches_logical_model_after_each_operation() {
	let database = KernelDatabase::open_or_create(memory_bindings(0)).await.unwrap();
	let branch = database.main_branch().0;
	let mut model = BranchModel::new(branch);
	let script = [
		(
			CommitTimestamp(10),
			vec![
				WriteOperation::Put {
					key: Bytes::from_static(b"a"),
					value: Bytes::from_static(b"a1"),
					expires_at: None,
				},
				WriteOperation::Put {
					key: Bytes::from_static(b"ttl"),
					value: Bytes::from_static(b"temporary"),
					expires_at: Some(CommitTimestamp(25)),
				},
			],
		),
		(
			CommitTimestamp(20),
			vec![WriteOperation::Put {
				key: Bytes::from_static(b"a"),
				value: Bytes::from_static(b"a2"),
				expires_at: None,
			}],
		),
		(
			CommitTimestamp(30),
			vec![WriteOperation::Delete {
				key: Bytes::from_static(b"a"),
			}],
		),
	];
	for (step, (timestamp, writes)) in script.into_iter().enumerate() {
		let expected = database.head();
		let receipt = database.commit(expected, timestamp, writes.clone()).await.unwrap();
		let model_receipt = model.commit(branch, expected, timestamp, writes).unwrap();
		assert_eq!(receipt.version, model_receipt.version);
		for selector in [
			ReadSelector::Latest,
			ReadSelector::Version(CommitVersion(1)),
			ReadSelector::Timestamp(CommitTimestamp(15)),
		] {
			for key in [b"a".as_slice(), b"ttl".as_slice(), b"missing".as_slice()] {
				assert_eq!(
					database.get(key, selector).await.unwrap(),
					model.get(branch, key, selector).unwrap()
				);
			}
			assert_eq!(
				database.scan(b"a", b"z", selector).await.unwrap(),
				model.scan(branch, b"a", b"z", selector).unwrap()
			);
		}
		if step == 1 {
			assert!(database.flush().await.unwrap());
			assert!(!database.flush().await.unwrap(), "empty flush must be a no-op");
			for key in [b"a".as_slice(), b"ttl".as_slice()] {
				assert_eq!(
					database.get(key, ReadSelector::Latest).await.unwrap(),
					model.get(branch, key, ReadSelector::Latest).unwrap()
				);
			}
		}
	}
	assert!(database.flush().await.unwrap());
	assert!(database.compact_owned().await.unwrap());
	assert!(!database.compact_owned().await.unwrap(), "one table cannot compact itself");
	for selector in [
		ReadSelector::Latest,
		ReadSelector::Version(CommitVersion(1)),
		ReadSelector::Timestamp(CommitTimestamp(15)),
	] {
		assert_eq!(
			database.scan(b"a", b"z", selector).await.unwrap(),
			model.scan(branch, b"a", b"z", selector).unwrap()
		);
	}
	assert_eq!(database.history(b"a").await.unwrap(), model.history(branch, b"a").unwrap());
	assert_eq!(database.get(b"ttl", ReadSelector::Latest).await.unwrap(), None);
	assert!(model.steps() >= 3, "model parity fixture must execute every commit");
}

#[test(tokio::test)]
async fn single_branch_sim_commit_unknown_reconciles_and_reopens_after_crash() {
	let sim = SimHarness::new(2);
	let database = KernelDatabase::open_or_create(sim.bindings()).await.unwrap();
	let database_id = database.database_id();
	sim.inject(FaultPoint::CommitAfter);
	let receipt = database
		.commit(
			CommitVersion(0),
			CommitTimestamp(10),
			vec![WriteOperation::Put {
				key: Bytes::from_static(b"durable"),
				value: Bytes::from_static(b"yes"),
				expires_at: None,
			}],
		)
		.await
		.unwrap();
	assert_eq!(receipt.version, CommitVersion(1));
	drop(database);

	let crashed = sim.crash();
	let reopened = KernelDatabase::open_or_create(crashed.bindings()).await.unwrap();
	assert_eq!(reopened.database_id(), database_id);
	assert_eq!(reopened.head(), CommitVersion(1));
	assert_eq!(
		reopened.get(b"durable", ReadSelector::Latest).await.unwrap(),
		Some(Bytes::from_static(b"yes"))
	);
}

#[test(tokio::test)]
async fn sim_flush_root_failure_leaks_object_but_recovers_complete_old_state() {
	let sim = SimHarness::new(2);
	let database = KernelDatabase::open_or_create(sim.bindings()).await.unwrap();
	database
		.commit(
			CommitVersion(0),
			CommitTimestamp(10),
			vec![WriteOperation::Put {
				key: Bytes::from_static(b"still-visible"),
				value: Bytes::from_static(b"old-root"),
				expires_at: None,
			}],
		)
		.await
		.unwrap();
	sim.inject(FaultPoint::CommitBefore);
	let error = database.flush().await.unwrap_err();
	assert_eq!(error.code, ErrorCode::Unavailable);
	let leaked =
		sim.bindings().objects.list_page(&ObjectPrefix("tables/".to_string()), None).await.unwrap();
	assert_eq!(leaked.objects.len(), 1, "fixture must upload the uninstalled table");
	drop(database);

	let crashed = sim.crash();
	let reopened = KernelDatabase::open_or_create(crashed.bindings()).await.unwrap();
	assert_eq!(reopened.head(), CommitVersion(1));
	assert_eq!(
		reopened.get(b"still-visible", ReadSelector::Latest).await.unwrap(),
		Some(Bytes::from_static(b"old-root"))
	);
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn single_branch_local_spine_commits_and_reopens_through_durable_authority() {
	let directory = tempfile::tempdir().unwrap();
	let object_path = directory.path().join("objects");
	let authority_path = directory.path().join("authority");
	let database = KernelDatabase::open_or_create(Bindings {
		objects: Arc::new(LocalObjectStore::open(object_path.clone(), 2).unwrap()),
		commits: Arc::new(LocalCommitStore::open(authority_path.clone()).unwrap()),
		platform: Arc::new(MemoryPlatform::new(0, 71, 64 * 1024 * 1024)),
	})
	.await
	.unwrap();
	let identity = database.database_id();
	database
		.commit(
			CommitVersion(0),
			CommitTimestamp(10),
			vec![WriteOperation::Put {
				key: Bytes::from_static(b"local"),
				value: Bytes::from_static(b"durable"),
				expires_at: None,
			}],
		)
		.await
		.unwrap();
	assert!(database.flush().await.unwrap());
	assert_eq!(
		database.get(b"local", ReadSelector::Latest).await.unwrap(),
		Some(Bytes::from_static(b"durable"))
	);
	drop(database);

	let reopened = KernelDatabase::open_or_create(Bindings {
		objects: Arc::new(LocalObjectStore::open(object_path, 2).unwrap()),
		commits: Arc::new(LocalCommitStore::open(authority_path).unwrap()),
		platform: Arc::new(MemoryPlatform::new(20, 99, 64 * 1024 * 1024)),
	})
	.await
	.unwrap();
	assert_eq!(reopened.database_id(), identity);
	assert_eq!(reopened.head(), CommitVersion(1));
	assert_eq!(
		reopened.get(b"local", ReadSelector::Latest).await.unwrap(),
		Some(Bytes::from_static(b"durable"))
	);
}

#[cfg(not(target_arch = "wasm32"))]
#[test(tokio::test)]
async fn memory_sim_and_local_read_identical_immutable_table_bytes_by_range() {
	let owner = TableOwner {
		branch: BranchId::from_u128(50),
		generation: BranchGeneration(2),
	};
	let mut builder = TableBuilder::new(owner, TableId([5; 32]), 48).unwrap();
	for (key, version, value) in [
		(b"a".as_slice(), 3, b"a3".as_slice()),
		(b"a".as_slice(), 1, b"a1".as_slice()),
		(b"b".as_slice(), 2, b"b2".as_slice()),
	] {
		builder
			.add(
				owner,
				StorageRow::new(
					InternalKey::new(
						Bytes::copy_from_slice(key),
						CommitVersion(version),
						RowKind::Value,
					)
					.unwrap(),
					CommitTimestamp(version * 10),
					None,
					Bytes::copy_from_slice(value),
				)
				.unwrap(),
			)
			.unwrap();
	}
	let built = builder.finish().unwrap();
	let sim = SimHarness::new(2);
	let directory = tempfile::tempdir().unwrap();
	let stores = vec![
		Arc::new(MemoryObjectStore::new(2)) as Arc<dyn ObjectStore>,
		sim.bindings().objects,
		Arc::new(LocalObjectStore::open(directory.path().join("objects"), 2).unwrap()),
	];
	for store in stores {
		store
			.put_unique(PutRequest {
				id: built.descriptor.object_id(),
				body: built.body.clone(),
				attributes: BTreeMap::from([("sha256".to_string(), hex(&built.descriptor.digest))]),
			})
			.await
			.unwrap();
		let reader =
			TableReader::open(store, built.descriptor.clone(), Arc::new(BlockCache::new(4096)))
				.await
				.unwrap();
		assert_eq!(
			reader.get(b"a", CommitVersion(2)).await.unwrap().unwrap().value,
			Bytes::from_static(b"a1")
		);
		assert_eq!(
			reader.get(b"b", CommitVersion(2)).await.unwrap().unwrap().value,
			Bytes::from_static(b"b2")
		);
	}
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

#[test]
fn reference_model_executes_branch_and_temporal_script() {
	let main = BranchId::from_u128(1);
	let child = BranchId::from_u128(2);
	let recreated = BranchId::from_u128(3);
	let mut model = BranchModel::new(main);

	let first = model
		.commit(
			main,
			CommitVersion(0),
			CommitTimestamp(10),
			vec![
				WriteOperation::Put {
					key: Bytes::from_static(b"a"),
					value: Bytes::from_static(b"main-v1"),
					expires_at: None,
				},
				WriteOperation::Put {
					key: Bytes::from_static(b"doomed"),
					value: Bytes::from_static(b"delete-me"),
					expires_at: None,
				},
				WriteOperation::Put {
					key: Bytes::from_static(b"ttl"),
					value: Bytes::from_static(b"temporary"),
					expires_at: Some(CommitTimestamp(25)),
				},
			],
		)
		.unwrap();
	assert_eq!(first.version, CommitVersion(1));
	assert_eq!(model.fork(main, ReadSelector::Latest, child, "agent").unwrap().0, 0);

	model
		.commit(
			main,
			CommitVersion(1),
			CommitTimestamp(20),
			vec![WriteOperation::Put {
				key: Bytes::from_static(b"a"),
				value: Bytes::from_static(b"main-v2"),
				expires_at: None,
			}],
		)
		.unwrap();
	assert_eq!(
		model.get(child, b"a", ReadSelector::Latest).unwrap(),
		Some(Bytes::from_static(b"main-v1"))
	);
	assert_eq!(
		model.get(main, b"a", ReadSelector::Timestamp(CommitTimestamp(15))).unwrap(),
		Some(Bytes::from_static(b"main-v1"))
	);

	model
		.commit(
			child,
			CommitVersion(1),
			CommitTimestamp(30),
			vec![
				WriteOperation::Put {
					key: Bytes::from_static(b"b"),
					value: Bytes::from_static(b"child-only"),
					expires_at: None,
				},
				WriteOperation::Delete {
					key: Bytes::from_static(b"doomed"),
				},
			],
		)
		.unwrap();
	assert_eq!(model.get(main, b"b", ReadSelector::Latest).unwrap(), None);
	assert_eq!(model.get(child, b"doomed", ReadSelector::Latest).unwrap(), None);
	assert_eq!(model.get(child, b"ttl", ReadSelector::Latest).unwrap(), None);
	assert_eq!(
		model.get(child, b"ttl", ReadSelector::Timestamp(CommitTimestamp(15))).unwrap(),
		Some(Bytes::from_static(b"temporary"))
	);
	assert_eq!(model.scan(child, b"a", b"z", ReadSelector::Latest).unwrap().len(), 2);
	assert_eq!(model.history(main, b"a").unwrap().len(), 2);

	model.delete_branch(child).unwrap();
	assert_eq!(model.fork(main, ReadSelector::Latest, recreated, "agent").unwrap().0, 1);
	assert_eq!(model.generation(recreated).unwrap().0, 1);
	assert!(model.steps() >= 6, "script must exercise commits, forks and deletion");
}
