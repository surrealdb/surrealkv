//! V3: every branch state machine driven through a failed durable publish.
//!
//! Each of these operations already had a rollback path for its publish
//! failing. None of them had ever executed one, because nothing in the suite
//! could make a publish fail. The contract each test asserts is the same:
//!
//! **a failed publish leaves the in-memory state matching the durable state.**
//!
//! Not "the operation returned an error" — that is the easy half. The half that
//! matters is that the branch catalog in memory did not keep the change whose
//! publish was rejected, because everything downstream reads the in-memory
//! catalog and would otherwise be acting on a decision no restart would agree
//! with.
//!
//! Every test pairs the injected failure with a control arm: the same operation,
//! unarmed, must succeed. Without that, a test could pass because the operation
//! was refused for some unrelated reason.

use tempdir::TempDir;
use test_log::test;

use crate::failpoints::{self, arm_once, CATALOG_PUBLISH, ROOT_PUBLISH};
use crate::lsm::Tree;
use crate::{ForkPoint, MergeStrategy, TreeBuilder};

fn create_store() -> (Tree, TempDir) {
	let temp_dir = TempDir::new("faults").unwrap();
	let path = temp_dir.path().to_path_buf();
	let tree = TreeBuilder::new().with_path(path).with_level_count(2).build().unwrap();
	(tree, temp_dir)
}

fn branch_exists(store: &Tree, name: &str) -> bool {
	store.list_branches().unwrap().iter().any(|info| info.name == name)
}

/// A fork whose catalog publish fails creates nothing, and the name is free to
/// use afterwards.
#[test(tokio::test)]
async fn a_failed_publish_leaves_no_half_created_fork() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();

	{
		let _armed = arm_once(CATALOG_PUBLISH);
		store
			.fork_branch("main", "doomed", ForkPoint::Head)
			.expect_err("the publish failed, so the fork must fail");
		assert!(!failpoints::is_armed(CATALOG_PUBLISH), "the injected fault was consumed");
	}

	assert!(
		!branch_exists(&store, "doomed"),
		"the in-memory catalog kept a branch it never published"
	);

	// Control: the same fork, unarmed, works — and the name was not burned.
	let child = store.fork_branch("main", "doomed", ForkPoint::Head).unwrap();
	assert_eq!(child.info().unwrap().name, "doomed");
	let txn = child.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"v".to_vec()), "and it inherits correctly");
}

/// A delete whose publish fails leaves the branch live and writable — not
/// tombstoned in memory while the durable catalog still lists it.
#[test(tokio::test)]
async fn a_failed_publish_leaves_a_deleted_branch_alive() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();
	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();

	{
		let _armed = arm_once(CATALOG_PUBLISH);
		store.delete_branch("work").expect_err("the publish failed, so the delete must fail");
	}

	assert!(branch_exists(&store, "work"), "the branch was tombstoned in memory only");
	// Still usable: a handle taken before the failed delete is not fenced.
	let mut txn = child.begin().unwrap();
	txn.set(b"after", b"v").unwrap();
	txn.commit().await.expect("a branch whose delete failed must still accept writes");

	// Control.
	store.delete_branch("work").unwrap();
	assert!(!branch_exists(&store, "work"));
}

/// A detach whose publish fails leaves the branch reading through its parent.
///
/// This is the case with a real window: detach materializes the inherited rows
/// into the branch's own tables *before* clearing the parent link. A failure
/// between them leaves rows copied but the link intact — which must read
/// identically, because the copies shadow what they were copied from.
#[test(tokio::test)]
async fn a_failed_publish_leaves_a_detached_branch_reading_through_its_parent() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"inherited", b"from-parent").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"own", b"from-child").unwrap();
	txn.commit().await.unwrap();

	{
		let _armed = arm_once(CATALOG_PUBLISH);
		store.detach_branch("work").expect_err("the publish failed, so the detach must fail");
	}

	assert!(
		child.info().unwrap().parent.is_some(),
		"the parent link was cleared in memory without being published"
	);
	let txn = child.begin().unwrap();
	assert_eq!(txn.get(b"inherited").unwrap(), Some(b"from-parent".to_vec()));
	assert_eq!(txn.get(b"own").unwrap(), Some(b"from-child".to_vec()));
	drop(txn);

	// Control: the retry completes, and reads are unchanged by it.
	store.detach_branch("work").unwrap();
	assert!(child.info().unwrap().parent.is_none());
	let txn = child.begin().unwrap();
	assert_eq!(txn.get(b"inherited").unwrap(), Some(b"from-parent".to_vec()));
	assert_eq!(txn.get(b"own").unwrap(), Some(b"from-child".to_vec()));
}

/// A TTL expiry whose publish fails leaves the branch live, and the next sweep
/// expires it. A sweep that failed must not report work it did not do.
#[test(tokio::test)]
async fn a_failed_publish_leaves_an_expiring_branch_alive_until_the_next_sweep() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();
	store.fork_branch("main", "ephemeral", ForkPoint::Head).unwrap();
	store.set_branch_ttl("ephemeral", Some(std::time::Duration::ZERO)).unwrap();

	{
		let _armed = arm_once(CATALOG_PUBLISH);
		store
			.core
			.inner
			.sweep_branch_maintenance()
			.expect_err("the publish failed, so the sweep must fail");
	}
	assert!(branch_exists(&store, "ephemeral"), "expired in memory without being published");

	// Control: the next sweep completes the expiry.
	let (expired, _) = store.core.inner.sweep_branch_maintenance().unwrap();
	assert_eq!(expired, 1);
	assert!(!branch_exists(&store, "ephemeral"));
}

/// A merge whose promotion edge fails to publish has already written its data.
/// The contract is the one PD2 recorded: re-offering, never silent loss.
#[test(tokio::test)]
async fn a_merge_whose_edge_fails_to_publish_re_offers_rather_than_losing() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"anchor", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"from-child", b"value").unwrap();
	txn.commit().await.unwrap();

	{
		let _armed = arm_once(CATALOG_PUBLISH);
		child
			.merge_into(&main, MergeStrategy::Strict)
			.await
			.expect_err("the edge publish failed, so the merge must report failure");
	}

	// The data landed — the merge commits before it records the edge, on
	// purpose, because the opposite order loses writes.
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"from-child").unwrap(), Some(b"value".to_vec()));
	drop(txn);

	// And with no edge recorded, the merge is offered again rather than being
	// assumed done. It converges, because the target already holds the value.
	let outcome = child.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	assert_eq!(outcome.applied, 0);
	assert_eq!(outcome.converged, 1, "the re-offer recognises what already landed");
}

/// The reclamation sweep removes a branch's level set from the manifest and
/// *then* publishes a root that no longer names it — that order is forced,
/// because the root's contents are derived from the state-version map and
/// cannot describe an owner's absence until the owner is absent.
///
/// So a failed publish leaves memory ahead of disk, and without a rollback the
/// next sweep finds nothing to reclaim while the files sit on disk referenced
/// by a root that was never replaced. The tables leak until the next process
/// start — which is exactly the leak PB1 existed to fix, returning on the
/// failure path.
#[test(tokio::test)]
async fn a_failed_root_publish_during_reclamation_puts_the_owner_back() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "doomed", ForkPoint::Head).unwrap();
	let owner = crate::batch::BatchOwner {
		branch: child.id(),
		generation: child.generation(),
	};
	let mut txn = child.begin().unwrap();
	txn.set(b"own", b"v").unwrap();
	txn.commit().await.unwrap();
	let runtime = store.core.inner.runtimes.get(owner).unwrap();
	store.core.inner.rotate_runtime_memtable(&runtime, 0).unwrap();
	while store.core.inner.flush_oldest_immutable_for_test().unwrap().is_some() {}

	let owned_tables = |store: &Tree| {
		store
			.core
			.inner
			.level_manifest
			.read()
			.unwrap()
			.levels_for(owner)
			.map(|levels| levels.get_levels().iter().map(|level| level.tables.len()).sum::<usize>())
			.unwrap_or(0)
	};
	assert_eq!(owned_tables(&store), 1, "the fixture must give the branch a table of its own");

	store.delete_branch("doomed").unwrap();

	{
		let _armed = arm_once(ROOT_PUBLISH);
		store
			.core
			.inner
			.sweep_branch_maintenance()
			.expect_err("the root publish failed, so the sweep must report it");
	}
	assert_eq!(
		owned_tables(&store),
		1,
		"the level set was removed in memory without being published"
	);
	assert_eq!(store.metrics().unwrap().tables_reclaimed, 0, "nothing was freed");

	// Control: the retry completes and the table is actually released. Without
	// the rollback above this reclaims nothing, because the owner would already
	// be gone from the manifest.
	store.core.inner.sweep_branch_maintenance().unwrap();
	assert_eq!(owned_tables(&store), 0);
	assert_eq!(store.metrics().unwrap().tables_reclaimed, 1, "the retry freed the table");
}

/// The registry itself: a one-shot fires once, an always-armed point fires
/// until it is dropped, and dropping the guard restores normal operation.
///
/// Without this, every test above could be passing because the failpoint fires
/// forever and the "control" arm is unreachable — which would make the control
/// arms silently vacuous rather than loudly wrong.
#[test(tokio::test)]
async fn the_registry_fires_as_armed_and_stops_when_disarmed() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();

	{
		let _armed = arm_once(CATALOG_PUBLISH);
		assert!(store.create_branch("a").is_err(), "the first call must fail");
		assert!(store.create_branch("a").is_ok(), "and the second must not");
	}

	{
		let _armed = failpoints::arm_always(CATALOG_PUBLISH);
		assert!(store.create_branch("b").is_err());
		assert!(store.create_branch("b").is_err(), "an always-armed point keeps firing");
		assert!(failpoints::is_armed(CATALOG_PUBLISH));
	}
	assert!(!failpoints::is_armed(CATALOG_PUBLISH), "the guard disarms on drop");
	store.create_branch("b").expect("normal operation resumes");
}
