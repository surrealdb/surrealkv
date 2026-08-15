//! PC gate tests: branch diff.
//!
//! The property under test throughout is that a diff reports exactly what the
//! branch changed — not what it can read, and not what it happens to own.

use test_log::test;

use crate::test::support::create_store;
use crate::{Error, ForkPoint};

fn keys(entries: &[crate::DiffEntry]) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
	entries.iter().map(|entry| (entry.key.clone(), entry.op.value().cloned())).collect()
}

/// A diff is the branch's own writes: overwrites of inherited keys, brand-new
/// keys, and deletes. Inherited rows the branch never touched are not changes,
/// and neither are the parent's post-fork writes.
#[test(tokio::test)]
async fn diff_reports_exactly_what_the_branch_changed() {
	let (store, _temp) = create_store();

	let mut txn = store.begin().unwrap();
	txn.set(b"untouched", b"parent").unwrap();
	txn.set(b"overwritten", b"parent").unwrap();
	txn.set(b"deleted", b"parent").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();

	let mut txn = child.begin().unwrap();
	txn.set(b"overwritten", b"child").unwrap();
	txn.set(b"brand-new", b"child").unwrap();
	txn.delete(b"deleted").unwrap();
	txn.commit().await.unwrap();

	// The parent moves on; none of this is the child's change.
	let mut txn = store.begin().unwrap();
	txn.set(b"untouched", b"parent-moved-on").unwrap();
	txn.set(b"parent-only", b"parent").unwrap();
	txn.commit().await.unwrap();

	let diff = child.diff().unwrap();
	let entries = diff.collect().unwrap();
	assert_eq!(
		keys(&entries),
		vec![
			(b"brand-new".to_vec(), Some(b"child".to_vec())),
			(b"deleted".to_vec(), None),
			(b"overwritten".to_vec(), Some(b"child".to_vec())),
		],
		"a diff is the branch's own writes, in key order, deletes included"
	);
	assert!(entries[1].op.is_delete(), "a delete must be reported as a delete");
	assert!(entries.iter().all(|entry| entry.seq > diff.base_seq()));
}

/// Only the branch's NEWEST write to a key is a change: a key written three
/// times contributes one entry, and a key written then deleted reports the
/// delete.
#[test(tokio::test)]
async fn diff_reports_the_newest_write_per_key() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"base", b"v").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "rewrites", ForkPoint::Head).unwrap();
	for round in 0..3u32 {
		let mut txn = child.begin().unwrap();
		txn.set(b"rewritten", format!("v{round}").as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}
	let mut txn = child.begin().unwrap();
	txn.set(b"then-deleted", b"v").unwrap();
	txn.commit().await.unwrap();
	let mut txn = child.begin().unwrap();
	txn.delete(b"then-deleted").unwrap();
	txn.commit().await.unwrap();

	let entries = child.diff().unwrap().collect().unwrap();
	assert_eq!(
		keys(&entries),
		vec![(b"rewritten".to_vec(), Some(b"v2".to_vec())), (b"then-deleted".to_vec(), None),],
		"three writes to one key are one change, and a write-then-delete is a delete"
	);
}

/// A diff must not depend on where the data physically is. The same branch,
/// diffed with its writes in memory and again after they are flushed to a
/// table, produces identical results.
#[test(tokio::test)]
async fn diff_is_identical_across_a_flush_boundary() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"inherited", b"parent").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();

	let child = store.fork_branch("main", "flushing", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"a", b"1").unwrap();
	txn.set(b"b", b"2").unwrap();
	txn.delete(b"inherited").unwrap();
	txn.commit().await.unwrap();

	let in_memory = child.diff().unwrap().collect().unwrap();
	assert_eq!(in_memory.len(), 3, "fixture must produce changes while still in memory");

	// Seal and flush the branch's memtable, then ask again.
	let owner = {
		let catalog = store.core.inner.branch_catalog.read().unwrap();
		let record = catalog.get_by_name("flushing").unwrap();
		crate::batch::BatchOwner {
			branch: record.id,
			generation: record.generation,
		}
	};
	let runtime = store.core.inner.runtimes.get(owner).unwrap();
	store.core.inner.rotate_runtime_memtable(&runtime, 0).unwrap();
	store.core.inner.flush_all_immutables_sync().unwrap();
	assert!(
		store
			.core
			.inner
			.level_manifest
			.read()
			.unwrap()
			.levels_for(owner)
			.map(|levels| levels.get_levels()[0].tables.len())
			.unwrap_or(0)
			> 0,
		"the fixture must actually have flushed a table"
	);

	let on_disk = child.diff().unwrap().collect().unwrap();
	assert_eq!(on_disk, in_memory, "a diff must not depend on where the rows live");
}

/// The cursor streams in key order and resumes from any key, so a consumer that
/// stops partway can continue without replaying what it already saw.
#[test(tokio::test)]
async fn diff_streams_in_key_order_and_resumes_from_a_key() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"base", b"v").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "streamed", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	for key in [b"k1", b"k2", b"k3", b"k4"] {
		txn.set(key, b"v").unwrap();
	}
	txn.commit().await.unwrap();

	let diff = child.diff().unwrap();
	let streamed: Vec<_> = diff.iter().unwrap().map(|entry| entry.unwrap().key).collect();
	assert_eq!(
		streamed,
		vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec(), b"k4".to_vec()],
		"the cursor must stream in key order"
	);

	// Resume from the middle.
	let mut cursor = diff.iter().unwrap();
	cursor.seek(b"k3").unwrap();
	let resumed: Vec<_> = cursor.map(|entry| entry.unwrap().key).collect();
	assert_eq!(resumed, vec![b"k3".to_vec(), b"k4".to_vec()], "seek must resume, not restart");
}

/// The view is fixed when the diff is taken: writes that land afterwards do not
/// appear in it, so a consumer iterating a large diff cannot see a moving target.
#[test(tokio::test)]
async fn a_diff_is_a_fixed_view() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"base", b"v").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "fixed", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"first", b"v").unwrap();
	txn.commit().await.unwrap();

	let diff = child.diff().unwrap();

	let mut txn = child.begin().unwrap();
	txn.set(b"second", b"v").unwrap();
	txn.commit().await.unwrap();

	assert_eq!(
		keys(&diff.collect().unwrap()),
		vec![(b"first".to_vec(), Some(b"v".to_vec()))],
		"a write after the diff was taken must not appear in it"
	);
	assert_eq!(
		child.diff().unwrap().collect().unwrap().len(),
		2,
		"but a freshly taken diff sees it"
	);
}

/// Diff refuses a branch with no fork lineage. `main`, a plain branch and a
/// detached branch all have nothing to be diffed against, and answering with
/// "everything it owns" would be a different question wearing this one's name.
///
/// The detached case is the one that matters: detach materializes inherited
/// rows into the branch's own tables, so a diff that trusted "everything I own
/// is a change" would report the entire inheritance as changes.
#[test(tokio::test)]
async fn diff_refuses_a_branch_with_no_lineage() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"a", b"parent").unwrap();
	txn.set(b"b", b"parent").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();

	assert!(store.branch("main").unwrap().diff().is_err(), "main has no lineage");
	store.create_branch("plain").unwrap();
	assert!(store.branch("plain").unwrap().diff().is_err(), "a plain branch has no lineage");

	let child = store.fork_branch("main", "detachable", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"own", b"child").unwrap();
	txn.commit().await.unwrap();
	assert_eq!(child.diff().unwrap().collect().unwrap().len(), 1, "one own write, one change");

	store.detach_branch("detachable").unwrap();
	let error = child.diff().expect_err("a detached branch has no base to diff against");
	assert!(error.to_string().contains("no fork lineage"), "{error}");
}

/// Even if a branch could be diffed after detaching, the inherited rows it now
/// owns are below the fork anchor and are filtered by sequence — the diff does
/// not assume that everything a branch owns was written by it.
#[test(tokio::test)]
async fn the_sequence_filter_excludes_materialized_inherited_rows() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"inherited-1", b"parent").unwrap();
	txn.set(b"inherited-2", b"parent").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();

	let child = store.fork_branch("main", "materialized", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"own", b"child").unwrap();
	txn.commit().await.unwrap();

	// Materialize WITHOUT clearing the lineage: the branch now owns two
	// inherited rows plus its own write, and still has a base to diff against.
	let owner = {
		let catalog = store.core.inner.branch_catalog.read().unwrap();
		let record = catalog.get_by_name("materialized").unwrap();
		crate::batch::BatchOwner {
			branch: record.id,
			generation: record.generation,
		}
	};
	let copied = store.core.inner.materialize_inherited(&store.core, owner).unwrap();
	assert_eq!(copied, 2, "the fixture must put inherited rows into the branch's own tables");

	assert_eq!(
		keys(&child.diff().unwrap().collect().unwrap()),
		vec![(b"own".to_vec(), Some(b"child".to_vec()))],
		"rows the branch owns but did not write must not be reported as changes"
	);
}

/// A branch that wrote nothing has an empty diff — not an error, and not its
/// inherited contents.
#[test(tokio::test)]
async fn a_branch_that_wrote_nothing_has_an_empty_diff() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"a", b"parent").unwrap();
	txn.set(b"b", b"parent").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "idle", ForkPoint::Head).unwrap();
	assert!(child.diff().unwrap().collect().unwrap().is_empty());

	// And a fenced branch reports the fencing, not an empty diff.
	store.delete_branch("idle").unwrap();
	assert!(matches!(child.diff(), Err(Error::BranchFenced)));
}
