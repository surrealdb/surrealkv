//! PD1 gate tests: merge planning against a real store.
//!
//! `merge.rs` unit-tests the decision table against a fake probe. These tests
//! prove the same rules hold when the probe is the actual read stack — that the
//! base really is the target as of the fork, that the target's *view* is what
//! moves (not just its own writes), and that nothing is mutated by planning.

use tempdir::TempDir;
use test_log::test;

use crate::merge::TargetProbe;
use crate::test::support::create_store;
use crate::{ConflictKind, Error, ForkPoint, MergeReport, MergeStrategy, Tree, TreeBuilder};

fn applied(report: &MergeReport) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
	report.applies.iter().map(|entry| (entry.key.clone(), entry.op.value().cloned())).collect()
}

fn conflicted(report: &MergeReport) -> Vec<Vec<u8>> {
	report.conflicts.iter().map(|conflict| conflict.key.clone()).collect()
}

/// The clean case: a target nobody touched takes every source change.
#[test(tokio::test)]
async fn a_merge_into_an_untouched_target_is_entirely_clean() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"a", b"base").unwrap();
	txn.set(b"b", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"a", b"changed").unwrap();
	txn.set(b"new", b"added").unwrap();
	txn.delete(b"b").unwrap();
	txn.commit().await.unwrap();

	let main = store.branch("main").unwrap();
	let report = child.preview_merge_into(&main).unwrap();
	assert!(report.is_clean(), "an untouched target must not conflict");
	assert_eq!(
		applied(&report),
		vec![
			(b"a".to_vec(), Some(b"changed".to_vec())),
			(b"b".to_vec(), None),
			(b"new".to_vec(), Some(b"added".to_vec())),
		],
		"every source change applies, deletes included"
	);
	assert_eq!(report.converged, 0);
}

/// Disjoint edits do not conflict: the target changing OTHER keys is irrelevant.
/// Only a key both sides touched can conflict.
#[test(tokio::test)]
async fn disjoint_edits_on_both_sides_stay_clean() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"source-side", b"base").unwrap();
	txn.set(b"target-side", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"source-side", b"by-source").unwrap();
	txn.commit().await.unwrap();

	let mut txn = store.begin().unwrap();
	txn.set(b"target-side", b"by-target").unwrap();
	txn.commit().await.unwrap();

	let main = store.branch("main").unwrap();
	let report = child.preview_merge_into(&main).unwrap();
	assert!(report.is_clean(), "edits to different keys must not conflict");
	assert_eq!(applied(&report), vec![(b"source-side".to_vec(), Some(b"by-source".to_vec()))]);
}

/// The whole conflict table, against the real read stack.
#[test(tokio::test)]
async fn the_conflict_table_holds_against_the_read_stack() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	for key in
		[b"both-modified".as_slice(), b"src-del", b"tgt-del", b"both-del", b"agreed", b"reverted"]
	{
		txn.set(key, b"base").unwrap();
	}
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"both-modified", b"by-source").unwrap();
	txn.delete(b"src-del").unwrap();
	txn.set(b"tgt-del", b"by-source").unwrap();
	txn.delete(b"both-del").unwrap();
	txn.set(b"agreed", b"agreed").unwrap();
	txn.set(b"reverted", b"by-source").unwrap();
	txn.set(b"created-by-both", b"by-source").unwrap();
	txn.commit().await.unwrap();

	let mut txn = store.begin().unwrap();
	txn.set(b"both-modified", b"by-target").unwrap();
	txn.set(b"src-del", b"by-target").unwrap();
	txn.delete(b"tgt-del").unwrap();
	txn.delete(b"both-del").unwrap();
	txn.set(b"agreed", b"agreed").unwrap();
	txn.set(b"created-by-both", b"by-target").unwrap();
	// Moves away and back: content-identical to base, so not a conflict.
	txn.set(b"reverted", b"detour").unwrap();
	txn.commit().await.unwrap();
	let mut txn = store.begin().unwrap();
	txn.set(b"reverted", b"base").unwrap();
	txn.commit().await.unwrap();

	let main = store.branch("main").unwrap();
	let report = child.preview_merge_into(&main).unwrap();

	assert_eq!(
		conflicted(&report),
		vec![
			b"both-modified".to_vec(),
			b"created-by-both".to_vec(),
			b"src-del".to_vec(),
			b"tgt-del".to_vec(),
		],
		"exactly the divergent keys conflict"
	);
	let kinds: Vec<_> = report.conflicts.iter().map(|c| c.kind.clone()).collect();
	assert_eq!(
		kinds,
		vec![
			ConflictKind::BothModified,
			ConflictKind::BothModified,
			ConflictKind::DeletedBySourceModifiedByTarget,
			ConflictKind::ModifiedBySourceDeletedByTarget,
		]
	);

	assert_eq!(
		applied(&report),
		vec![(b"reverted".to_vec(), Some(b"by-source".to_vec()))],
		"a target that moved and came back still takes the source's change"
	);
	assert_eq!(report.converged, 2, "both-deleted and same-value both converge silently");

	// The conflict report carries all three sides, so a caller need not re-read.
	let both = &report.conflicts[0];
	assert_eq!(both.base.as_deref(), Some(b"base".as_slice()));
	assert_eq!(both.source.as_deref(), Some(b"by-source".as_slice()));
	assert_eq!(both.target.as_deref(), Some(b"by-target".as_slice()));
	// A key neither side had at the fork reports no base.
	let created = &report.conflicts[1];
	assert_eq!(created.base, None);
}

/// A fork child's inherited view is FROZEN at its anchor, so the only thing
/// that can move a target is its own writes.
///
/// This is worth pinning because it is what makes the merge comparison sound and
/// cheap at the same time. Comparing the target's *view* at two caps is the
/// correct question to ask; this property is why that question has the same
/// answer as "did the target write". If inheritance were ever unfrozen — a
/// re-anchor, say — the view comparison would keep being right and a
/// writes-only comparison would start being wrong, which is why the
/// implementation asks about the view.
#[test(tokio::test)]
async fn an_inherited_view_is_frozen_so_only_the_target_s_own_writes_move_it() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"grandparent-base").unwrap();
	txn.set(b"untouched", b"grandparent-base").unwrap();
	txn.commit().await.unwrap();

	// main -> middle -> leaf. The merge under test is leaf into middle.
	let middle = store.fork_branch("main", "middle", ForkPoint::Head).unwrap();
	let leaf = store.fork_branch("middle", "leaf", ForkPoint::Head).unwrap();

	let mut txn = leaf.begin().unwrap();
	txn.set(b"k", b"by-leaf").unwrap();
	txn.commit().await.unwrap();

	// The grandparent writes both keys after middle forked. Middle inherits
	// neither: its view of main is capped at its own anchor, permanently.
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"by-grandparent").unwrap();
	txn.set(b"untouched", b"by-grandparent").unwrap();
	txn.commit().await.unwrap();

	let view = middle.begin().unwrap();
	assert_eq!(
		view.get(b"k").unwrap(),
		Some(b"grandparent-base".to_vec()),
		"the grandparent's later write must be invisible: inheritance is frozen at the anchor"
	);
	assert_eq!(view.get(b"untouched").unwrap(), Some(b"grandparent-base".to_vec()));
	drop(view);

	// So middle has not moved, and the merge is clean even though the
	// grandparent changed the very key being merged.
	let report = leaf.preview_merge_into(&middle).unwrap();
	assert!(
		report.is_clean(),
		"a grandparent write above the target's anchor cannot make the target look modified"
	);
	assert_eq!(applied(&report), vec![(b"k".to_vec(), Some(b"by-leaf".to_vec()))]);

	// And when middle DOES write, it moves and the same merge conflicts.
	let mut txn = middle.begin().unwrap();
	txn.set(b"k", b"by-middle").unwrap();
	txn.commit().await.unwrap();
	let report = leaf.preview_merge_into(&middle).unwrap();
	assert_eq!(
		conflicted(&report),
		vec![b"k".to_vec()],
		"the target's own write is what makes it moved"
	);
}

/// Planning mutates nothing: the store is byte-for-byte unchanged afterwards,
/// and planning the same merge twice gives the same answer.
#[test(tokio::test)]
async fn planning_is_a_pure_read() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"a", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"a", b"by-source").unwrap();
	txn.set(b"b", b"added").unwrap();
	txn.commit().await.unwrap();
	let mut txn = store.begin().unwrap();
	txn.set(b"a", b"by-target").unwrap();
	txn.commit().await.unwrap();

	let main = store.branch("main").unwrap();
	let before_seq = main.info().unwrap().last_write_seq;

	let first = child.preview_merge_into(&main).unwrap();
	let second = child.preview_merge_into(&main).unwrap();
	assert_eq!(first, second, "planning is deterministic");
	assert!(!first.is_clean());

	assert_eq!(main.info().unwrap().last_write_seq, before_seq, "planning must not write anything");
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"a").unwrap(), Some(b"by-target".to_vec()), "the target is untouched");
	assert_eq!(txn.get(b"b").unwrap(), None, "the source's new key was not applied");
	drop(txn);
	let txn = child.begin().unwrap();
	assert_eq!(txn.get(b"a").unwrap(), Some(b"by-source".to_vec()), "the source is untouched");
}

/// Merges are only accepted into the branch the source was forked from.
#[test(tokio::test)]
async fn merging_anywhere_but_the_recorded_parent_is_refused() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"base").unwrap();
	txn.commit().await.unwrap();

	let left = store.fork_branch("main", "left", ForkPoint::Head).unwrap();
	let right = store.fork_branch("main", "right", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();

	// Siblings have no recorded common base.
	let error = left.preview_merge_into(&right).expect_err("siblings must be refused");
	assert!(matches!(error, Error::BranchesUnrelated { .. }), "got {error}");

	// A branch with no lineage cannot be a source.
	let plain = store.create_branch("plain").unwrap();
	let error = plain.preview_merge_into(&main).expect_err("a rootless branch must be refused");
	assert!(matches!(error, Error::BranchesUnrelated { .. }), "got {error}");

	// Its own parent is fine.
	assert!(left.preview_merge_into(&main).is_ok());

	// And the direction matters: the parent is not mergeable into its child.
	let error = main.preview_merge_into(&left).expect_err("a parent is not a source for its child");
	assert!(matches!(error, Error::BranchesUnrelated { .. }), "got {error}");
}

/// Detaching a source releases the pin it held on its parent, after which the
/// parent's compaction is free to collapse the history there — and a branch
/// forked afterwards still plans, because its own anchor is above the floor.
///
/// Named for what it proves. It does NOT reach `BelowRetentionFloor` on the
/// merge path: that check guards a cap the catalog no longer pins, and a live
/// source always pins the cap its merge reads at (FK6). The one window where it
/// can fire is between a merge's data commit and its edge record, which needs
/// fault injection to hit; the predicate itself is unit-tested in `branch.rs`.
#[test(tokio::test)]
async fn detaching_a_source_releases_the_parent_s_pin() {
	let (store, _temp) = create_store_with_levels(2);

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"base").unwrap();
	txn.commit().await.unwrap();
	store.drain_flushes_synchronously().unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"k", b"by-source").unwrap();
	txn.commit().await.unwrap();

	let main = store.branch("main").unwrap();
	assert!(child.preview_merge_into(&main).is_ok(), "the base is intact to begin with");

	// Detach the child so the parent's retention pin is released, then collapse
	// the parent's history past the fork point.
	store.detach_branch("work").unwrap();
	for round in 0..5u32 {
		let mut txn = store.begin().unwrap();
		txn.set(b"k", format!("v{round}").as_bytes()).unwrap();
		txn.commit().await.unwrap();
		store.drain_flushes_synchronously().unwrap();
	}
	let strategy = std::sync::Arc::new(crate::compaction::leveled::Strategy::from_options(
		std::sync::Arc::clone(&store.core.inner.opts),
	));
	store.compact(strategy).unwrap();
	let floor = store
		.core
		.inner
		.level_manifest
		.read()
		.unwrap()
		.retained_floor(crate::batch::BatchOwner::DEFAULT);
	assert!(floor > 0, "the fixture must have collapsed history, or this proves nothing");

	// The detached branch cannot be merged at all now (no lineage), which is a
	// different refusal — so re-fork one that still has a lineage below the floor.
	let stale = store.fork_branch("main", "stale", ForkPoint::Head).unwrap();
	let mut txn = stale.begin().unwrap();
	txn.set(b"k", b"by-stale").unwrap();
	txn.commit().await.unwrap();
	// Its anchor is above the floor, so it plans fine.
	assert!(stale.preview_merge_into(&main).is_ok());
}

fn create_store_with_budget(max_memtable_size: usize) -> (Tree, TempDir) {
	let temp_dir = TempDir::new("merge").unwrap();
	let tree = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_max_memtable_size(max_memtable_size)
		.build()
		.unwrap();
	(tree, temp_dir)
}

fn create_store_with_levels(levels: u8) -> (Tree, TempDir) {
	let temp_dir = TempDir::new("merge").unwrap();
	let path = temp_dir.path().to_path_buf();
	let tree = TreeBuilder::new().with_path(path).with_level_count(levels).build().unwrap();
	(tree, temp_dir)
}

/// A source that changed nothing produces an empty, clean report — not an error.
#[test(tokio::test)]
async fn a_source_with_no_changes_plans_to_nothing() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "idle", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();
	let report = child.preview_merge_into(&main).unwrap();
	assert!(report.is_clean());
	assert_eq!(report.apply_count(), 0);
	assert_eq!(report.converged, 0);
}

// ===== PD2: the merge commit =====

/// The clean case end to end: the target takes every change, the source is
/// untouched, and the result is visible at one point.
#[test(tokio::test)]
async fn a_clean_merge_applies_every_change_to_the_target_only() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"kept", b"base").unwrap();
	txn.set(b"removed", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"kept", b"by-source").unwrap();
	txn.set(b"added", b"by-source").unwrap();
	txn.delete(b"removed").unwrap();
	txn.commit().await.unwrap();

	let main = store.branch("main").unwrap();
	let outcome = child.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	assert_eq!(outcome.applied, 3);
	assert_eq!(outcome.resolved, 0);

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"kept").unwrap(), Some(b"by-source".to_vec()));
	assert_eq!(txn.get(b"added").unwrap(), Some(b"by-source".to_vec()));
	assert_eq!(txn.get(b"removed").unwrap(), None, "a merged delete must remove the key");
	drop(txn);

	// The source is unchanged by merging.
	let txn = child.begin().unwrap();
	assert_eq!(txn.get(b"kept").unwrap(), Some(b"by-source".to_vec()));
	assert_eq!(txn.get(b"removed").unwrap(), None);
}

/// Strict refuses the whole merge on any conflict and writes nothing — the
/// caller must not have to undo a half-applied merge.
#[test(tokio::test)]
async fn strict_refuses_the_whole_merge_and_writes_nothing() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"clean", b"base").unwrap();
	txn.set(b"contested", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"clean", b"by-source").unwrap();
	txn.set(b"contested", b"by-source").unwrap();
	txn.commit().await.unwrap();

	let mut txn = store.begin().unwrap();
	txn.set(b"contested", b"by-target").unwrap();
	txn.commit().await.unwrap();

	let main = store.branch("main").unwrap();
	let error = child
		.merge_into(&main, MergeStrategy::Strict)
		.await
		.expect_err("a conflict must refuse the merge");
	assert!(
		matches!(
			error,
			Error::MergeConflicts {
				count: 1
			}
		),
		"got {error}"
	);

	let txn = store.begin().unwrap();
	assert_eq!(
		txn.get(b"clean").unwrap(),
		Some(b"base".to_vec()),
		"the clean key must NOT have been applied: strict is all or nothing"
	);
	assert_eq!(txn.get(b"contested").unwrap(), Some(b"by-target".to_vec()));
}

/// SourceWins applies the source's value to conflicting keys, and says how many
/// it overrode.
#[test(tokio::test)]
async fn source_wins_resolves_conflicts_and_reports_how_many() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"clean", b"base").unwrap();
	txn.set(b"contested", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"clean", b"by-source").unwrap();
	txn.set(b"contested", b"by-source").unwrap();
	txn.commit().await.unwrap();
	let mut txn = store.begin().unwrap();
	txn.set(b"contested", b"by-target").unwrap();
	txn.commit().await.unwrap();

	let main = store.branch("main").unwrap();
	let outcome = child.merge_into(&main, MergeStrategy::SourceWins).await.unwrap();
	assert_eq!(outcome.resolved, 1, "one conflict was resolved in the source's favour");
	assert_eq!(outcome.applied, 2);

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"clean").unwrap(), Some(b"by-source".to_vec()));
	assert_eq!(txn.get(b"contested").unwrap(), Some(b"by-source".to_vec()));
}

/// Merging twice is not merging twice as much: the recorded edge moves the base
/// forward, so the second merge offers only what changed since the first.
///
/// The last arm is what the edge exists for — without it, a key already merged
/// and then changed by the target comes back as a conflict, which makes
/// iterative merging unusable.
#[test(tokio::test)]
async fn a_second_merge_starts_from_the_first_one() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "iterative", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();

	let mut txn = child.begin().unwrap();
	txn.set(b"k", b"first").unwrap();
	txn.commit().await.unwrap();
	let first = child.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	assert_eq!(first.applied, 1);

	// Nothing new on the source: the merge is a no-op, not a re-application.
	let repeat = child.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	assert_eq!(repeat.applied, 0, "re-merging with no new changes must do nothing");
	assert_eq!(repeat.converged, 0, "and must not even see the already-merged key");

	// New source change: only that one is offered.
	let mut txn = child.begin().unwrap();
	txn.set(b"k", b"second").unwrap();
	txn.set(b"other", b"new").unwrap();
	txn.commit().await.unwrap();
	let second = child.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	assert_eq!(second.applied, 2);
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"second".to_vec()));
	drop(txn);

	// The target edits a merged key afterwards; with no new source changes the
	// next merge is still clean, because the edge means that key is not offered.
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"target-owns-it-now").unwrap();
	txn.commit().await.unwrap();
	let third = child.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	assert_eq!(third.applied, 0, "a target edit after a merge must not resurrect a conflict");
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"target-owns-it-now".to_vec()));
}

/// A merge edge may advance past target-only writes, but those writes are not
/// thereby incorporated into the unchanged source branch. A later source edit
/// to the same key is still based on the fork value and must conflict under
/// strict three-way semantics.
#[test(tokio::test)]
async fn no_op_merge_does_not_reconcile_unseen_target_changes() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();

	let mut txn = main.begin().unwrap();
	txn.set(b"k", b"target").unwrap();
	txn.commit().await.unwrap();

	let first = child.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	assert_eq!(first.applied, 0, "the unchanged source offers no writes");

	let mut txn = child.begin().unwrap();
	txn.set(b"k", b"source").unwrap();
	txn.commit().await.unwrap();

	let error = child
		.merge_into(&main, MergeStrategy::Strict)
		.await
		.expect_err("the source never incorporated the target-only value");
	assert!(
		matches!(
			error,
			Error::MergeConflicts {
				count: 1
			}
		),
		"got {error}"
	);

	let txn = main.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"target".to_vec()));
}

/// The same baseline rule applies when the earlier merge was not a no-op. An
/// edge is global to the source/target pair, but it cannot mark untouched keys
/// as agreed merely because some other source key was promoted.
#[test(tokio::test)]
async fn merge_of_an_unrelated_key_does_not_reconcile_target_changes() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"contested", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();

	let mut txn = main.begin().unwrap();
	txn.set(b"contested", b"target").unwrap();
	txn.commit().await.unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"unrelated", b"source-first").unwrap();
	txn.commit().await.unwrap();

	let first = child.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	assert_eq!(first.applied, 1);

	let mut txn = child.begin().unwrap();
	txn.set(b"contested", b"source-second").unwrap();
	txn.commit().await.unwrap();

	let error = child
		.merge_into(&main, MergeStrategy::Strict)
		.await
		.expect_err("an unrelated promoted key cannot reconcile this key");
	assert!(
		matches!(
			error,
			Error::MergeConflicts {
				count: 1
			}
		),
		"got {error}"
	);

	let txn = main.begin().unwrap();
	assert_eq!(txn.get(b"contested").unwrap(), Some(b"target".to_vec()));
}

/// Edges are per source. One child's merge must never shift the base another
/// child is compared against — otherwise the second merge would overwrite the
/// first one's keys without reporting anything (plan C5).
#[test(tokio::test)]
async fn one_source_s_merge_does_not_shift_another_source_s_base() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"contested", b"base").unwrap();
	txn.commit().await.unwrap();

	let left = store.fork_branch("main", "left", ForkPoint::Head).unwrap();
	let right = store.fork_branch("main", "right", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();

	let mut txn = left.begin().unwrap();
	txn.set(b"contested", b"by-left").unwrap();
	txn.commit().await.unwrap();
	let mut txn = right.begin().unwrap();
	txn.set(b"contested", b"by-right").unwrap();
	txn.commit().await.unwrap();

	left.merge_into(&main, MergeStrategy::Strict).await.unwrap();

	// Right's merge must SEE left's change as a target change and conflict.
	let error = right
		.merge_into(&main, MergeStrategy::Strict)
		.await
		.expect_err("the second source must conflict, not silently overwrite");
	assert!(
		matches!(
			error,
			Error::MergeConflicts {
				count: 1
			}
		),
		"got {error}"
	);
	let txn = store.begin().unwrap();
	assert_eq!(
		txn.get(b"contested").unwrap(),
		Some(b"by-left".to_vec()),
		"the first source's merged value must survive"
	);
}

/// A merge that lands but whose edge is lost — the crash window — must degrade
/// to re-offering what was already applied, never to losing it. Re-offered keys
/// converge when the target still holds them.
#[test(tokio::test)]
async fn a_merge_whose_edge_was_lost_re_offers_rather_than_loses() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"k", b"by-source").unwrap();
	txn.commit().await.unwrap();

	// Exactly the crash state: the data commits, the edge does not.
	let report = child.preview_merge_into(&main).unwrap();
	assert_eq!(report.applies.len(), 1);
	let mut txn = main.begin().unwrap();
	txn.set(b"k", b"by-source").unwrap();
	txn.commit().await.unwrap();

	// The next merge re-offers the key and finds the target already has it.
	let report = child.preview_merge_into(&main).unwrap();
	assert!(report.is_clean(), "a re-offered key must not conflict with its own applied value");
	assert_eq!(report.converged, 1, "it converges instead");
	assert_eq!(report.applies.len(), 0);

	let outcome = child.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	assert_eq!(outcome.applied, 0);
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"by-source".to_vec()), "the value survived");
}

/// Merges survive a reopen: both the applied data and the recorded edge are
/// durable, so a restart does not re-offer what was already merged.
#[test(tokio::test)]
async fn a_merge_and_its_edge_survive_a_reopen() {
	let temp_dir = TempDir::new("merge").unwrap();
	let path = temp_dir.path().to_path_buf();

	{
		let store = TreeBuilder::new().with_path(path.clone()).build().unwrap();
		let mut txn = store.begin().unwrap();
		txn.set(b"k", b"base").unwrap();
		txn.commit().await.unwrap();
		let child = store.fork_branch("main", "durable", ForkPoint::Head).unwrap();
		let mut txn = child.begin().unwrap();
		txn.set(b"k", b"by-source").unwrap();
		txn.commit().await.unwrap();
		let main = store.branch("main").unwrap();
		child.merge_into(&main, MergeStrategy::Strict).await.unwrap();
		store.close().await.unwrap();
	}

	let store = TreeBuilder::new().with_path(path).build().unwrap();
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"by-source".to_vec()), "merged data is durable");
	drop(txn);

	let child = store.branch("durable").unwrap();
	let main = store.branch("main").unwrap();
	let report = child.preview_merge_into(&main).unwrap();
	assert_eq!(
		report.apply_count(),
		0,
		"the recorded edge is durable too, so nothing is re-offered"
	);
	store.close().await.unwrap();
}

/// A merge larger than one batch is refused before anything is written.
/// FK6: the parent compacting does not end merging.
///
/// Before FK6 the merge gate compared the parent's whole-owner retention floor
/// against the fork anchor. That floor rises whenever ANY key loses ANY version,
/// which is what ordinary compaction of an updated key does — so a source could
/// be merged exactly until its parent first compacted, and was refused for ever
/// after. The refusal was false: the source's base was pinned the whole time.
#[test(tokio::test)]
async fn a_source_can_still_be_merged_after_its_parent_has_compacted() {
	let (store, _temp) = create_store_with_levels(2);

	let mut txn = store.begin().unwrap();
	txn.set(b"shared", b"base").unwrap();
	txn.commit().await.unwrap();
	store.drain_flushes_synchronously().unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();

	// The parent overwrites an unrelated key repeatedly and compacts, which is
	// what raises the whole-owner floor past the child's anchor.
	for round in 0..5u32 {
		let mut txn = store.begin().unwrap();
		txn.set(b"churn", format!("v{round}").as_bytes()).unwrap();
		txn.commit().await.unwrap();
		store.drain_flushes_synchronously().unwrap();
	}
	let strategy = std::sync::Arc::new(crate::compaction::leveled::Strategy::from_options(
		std::sync::Arc::clone(&store.core.inner.opts),
	));
	store.compact(strategy).unwrap();
	let floor = store
		.core
		.inner
		.level_manifest
		.read()
		.unwrap()
		.retained_floor(crate::batch::BatchOwner::DEFAULT);
	assert!(floor > 1, "the fixture must have raised the floor past the fork anchor, got {floor}");

	let mut txn = child.begin().unwrap();
	txn.set(b"from-child", b"value").unwrap();
	txn.commit().await.unwrap();

	let outcome = child
		.merge_into(&main, MergeStrategy::Strict)
		.await
		.expect("a compaction that never touched the pinned view must not block the merge");
	assert_eq!(outcome.applied, 1);
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"from-child").unwrap(), Some(b"value".to_vec()));
	assert_eq!(txn.get(b"shared").unwrap(), Some(b"base".to_vec()));
}

/// FK6: what the false refusal above was masking.
///
/// A second merge reads the target as it stood at the first merge's commit. That
/// cap is only readable because the merge edge pins it — without that anchor the
/// version the first merge wrote is superseded and dropped, the base reads as
/// the pre-merge value, and a target that has deliberately reverted the key
/// looks unchanged. The merge would then apply straight over the revert with no
/// conflict at all.
#[test(tokio::test)]
async fn a_target_that_reverted_a_merged_key_conflicts_instead_of_being_overwritten() {
	let (store, _temp) = create_store_with_levels(2);

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"original").unwrap();
	txn.commit().await.unwrap();
	store.drain_flushes_synchronously().unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();

	let mut txn = child.begin().unwrap();
	txn.set(b"k", b"from-child").unwrap();
	txn.commit().await.unwrap();
	child.merge_into(&main, MergeStrategy::Strict).await.unwrap();

	// The target undoes the merged change on purpose, then compacts. The version
	// the merge wrote is now superseded by the revert; only the merge edge's
	// anchor keeps it readable.
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"original").unwrap();
	txn.commit().await.unwrap();
	store.drain_flushes_synchronously().unwrap();
	for round in 0..5u32 {
		let mut txn = store.begin().unwrap();
		txn.set(format!("filler/{round}").as_bytes(), b"f").unwrap();
		txn.commit().await.unwrap();
		store.drain_flushes_synchronously().unwrap();
	}
	assert!(
		store.core.inner.snapshot_tracker.get_all_snapshots().is_empty(),
		"no live snapshot may pin the merged version instead"
	);
	let strategy = std::sync::Arc::new(crate::compaction::leveled::Strategy::from_options(
		std::sync::Arc::clone(&store.core.inner.opts),
	));
	store.compact(strategy).unwrap();

	let mut txn = child.begin().unwrap();
	txn.set(b"k", b"from-child-again").unwrap();
	txn.commit().await.unwrap();

	let report = child.preview_merge_into(&main).unwrap();
	assert_eq!(
		conflicted(&report),
		vec![b"k".to_vec()],
		"the target moved away from what the last merge left, so this is a conflict"
	);
	assert_eq!(
		report.conflicts[0].base,
		Some(b"from-child".to_vec()),
		"the base is the merged value"
	);
	assert_eq!(
		report.conflicts[0].target,
		Some(b"original".to_vec()),
		"and the target has reverted"
	);

	let error = child
		.merge_into(&main, MergeStrategy::Strict)
		.await
		.expect_err("strict must refuse rather than overwrite the revert");
	assert!(
		matches!(
			error,
			Error::MergeConflicts {
				count: 1
			}
		),
		"got {error}"
	);
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"original".to_vec()), "the revert stands");
}

/// The previous source tip is the three-way base for an incremental merge, so
/// source compaction must retain it just as target compaction retains the
/// target-side merge head. Otherwise a target revert can masquerade as an
/// unchanged target and be overwritten.
#[test(tokio::test)]
async fn source_compaction_keeps_the_previous_merge_base_exact() {
	let (store, _temp) = create_store_with_levels(2);

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"original").unwrap();
	txn.commit().await.unwrap();
	store.drain_flushes_synchronously().unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"k", b"source-v1").unwrap();
	txn.commit().await.unwrap();
	crate::test::support::flush_branch_to_table(&store, "work");
	let first = child.merge_into(&main, MergeStrategy::Strict).await.unwrap();

	let mut txn = main.begin().unwrap();
	txn.set(b"k", b"original").unwrap();
	txn.commit().await.unwrap();

	let mut txn = child.begin().unwrap();
	txn.set(b"k", b"source-v2").unwrap();
	txn.commit().await.unwrap();
	crate::test::support::flush_branch_to_table(&store, "work");
	for round in 0..3u32 {
		let mut txn = child.begin().unwrap();
		txn.set(b"churn", format!("v{round}").as_bytes()).unwrap();
		txn.commit().await.unwrap();
		crate::test::support::flush_branch_to_table(&store, "work");
	}

	crate::test::support::compact_leveled(&store);

	let floor = crate::test::support::branch_retained_floor(&store, "work");
	assert!(
		floor > first.source_through_seq,
		"fixture must compact past source cursor: floor={floor}, cursor={}",
		first.source_through_seq
	);

	let error = child
		.merge_into(&main, MergeStrategy::Strict)
		.await
		.expect_err("the retained source-v1 base exposes the target revert");
	assert!(
		matches!(
			error,
			Error::MergeConflicts {
				count: 1
			}
		),
		"got {error}"
	);
	let txn = main.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"original".to_vec()));
}

#[test(tokio::test)]
async fn a_merge_past_the_chunk_budget_completes_in_chunks() {
	let (store, _temp) = create_store_with_budget(4 * 1024);

	let mut txn = store.begin().unwrap();
	txn.set(b"anchor", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "big", ForkPoint::Head).unwrap();
	let payload = vec![b'x'; 512];
	for round in 0..40u32 {
		let mut txn = child.begin().unwrap();
		txn.set(format!("k{round:04}").as_bytes(), payload.as_slice()).unwrap();
		txn.commit().await.unwrap();
	}

	let main = store.branch("main").unwrap();
	let outcome = child.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	assert_eq!(outcome.applied, 40);
	assert!(
		outcome.chunks > 1,
		"~21KB against a 4KB budget must have taken several chunks, got {}",
		outcome.chunks
	);

	// Every chunk landed, and the merge is fully visible on the target.
	let txn = store.begin().unwrap();
	for round in 0..40u32 {
		assert_eq!(
			txn.get(format!("k{round:04}").as_bytes()).unwrap(),
			Some(payload.clone()),
			"key {round} is missing, so a chunk was lost"
		);
	}
	drop(txn);

	// And the edge covers the whole thing: re-merging offers nothing.
	let again = child.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	assert_eq!(again.applied, 0, "a chunked merge must record its full extent");
	assert_eq!(again.chunks, 0);
}

// ===== V2b: selected apply, and compensating restore =====

/// A scoped merge carries only the keys in its range, and leaves the rest to be
/// merged later.
#[test(tokio::test)]
async fn a_scoped_merge_carries_only_its_range() {
	let (store, _temp) = create_store();

	let mut txn = store.begin().unwrap();
	txn.set(b"anchor", b"base").unwrap();
	txn.commit().await.unwrap();
	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();

	let mut txn = child.begin().unwrap();
	for key in ["a/1", "a/2", "b/1", "c/1"] {
		txn.set(key.as_bytes(), b"by-source").unwrap();
	}
	txn.commit().await.unwrap();

	let outcome = child
		.merge_range(
			&main,
			MergeStrategy::Strict,
			std::ops::Bound::Included(b"a/".to_vec()),
			std::ops::Bound::Excluded(b"b/".to_vec()),
		)
		.await
		.unwrap();
	assert_eq!(outcome.applied, 2, "only the a/ keys");

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"a/1").unwrap(), Some(b"by-source".to_vec()));
	assert_eq!(txn.get(b"a/2").unwrap(), Some(b"by-source".to_vec()));
	assert_eq!(txn.get(b"b/1").unwrap(), None, "outside the range");
	assert_eq!(txn.get(b"c/1").unwrap(), None, "outside the range");
}

/// The reason a scoped merge records no edge: a later full merge must still
/// carry the keys the scoped one left behind.
///
/// If a scoped merge recorded an edge, `effective_base` would advance past
/// everything the source wrote up to that sequence — including `b/1` and `c/1`
/// — and they would never be offered again. This is the test that catches that,
/// and the keys it checks for are the ones that would vanish.
#[test(tokio::test)]
async fn a_scoped_merge_does_not_claim_the_source_is_fully_merged() {
	let (store, _temp) = create_store();

	let mut txn = store.begin().unwrap();
	txn.set(b"anchor", b"base").unwrap();
	txn.commit().await.unwrap();
	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();

	let mut txn = child.begin().unwrap();
	for key in ["a/1", "b/1", "c/1"] {
		txn.set(key.as_bytes(), b"by-source").unwrap();
	}
	txn.commit().await.unwrap();

	child
		.merge_range(
			&main,
			MergeStrategy::Strict,
			std::ops::Bound::Included(b"a/".to_vec()),
			std::ops::Bound::Excluded(b"b/".to_vec()),
		)
		.await
		.unwrap();

	// The full merge that follows must still carry everything the scoped one
	// skipped, and must recognise what it already applied.
	let outcome = child.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	assert_eq!(outcome.applied, 2, "b/1 and c/1 were never merged and must be offered");
	assert_eq!(outcome.converged, 1, "a/1 is already there, so it converges rather than rewriting");

	let txn = store.begin().unwrap();
	for key in ["a/1", "b/1", "c/1"] {
		assert_eq!(
			txn.get(key.as_bytes()).unwrap(),
			Some(b"by-source".to_vec()),
			"{key} did not survive scoped-then-full merging"
		);
	}
}

/// Reverting restores what the branch inherited, as new writes. Keys it created
/// since the fork are removed, because it inherited nothing for those.
#[test(tokio::test)]
async fn revert_restores_the_inherited_value_and_removes_what_was_created() {
	let (store, _temp) = create_store();

	let mut txn = store.begin().unwrap();
	txn.set(b"k/kept", b"inherited").unwrap();
	txn.set(b"k/changed", b"inherited").unwrap();
	txn.set(b"k/deleted", b"inherited").unwrap();
	txn.set(b"outside", b"inherited").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"k/changed", b"by-child").unwrap();
	txn.delete(b"k/deleted").unwrap();
	txn.set(b"k/created", b"by-child").unwrap();
	txn.set(b"outside", b"by-child").unwrap();
	txn.commit().await.unwrap();

	let restored = child
		.revert_range(
			std::ops::Bound::Included(b"k/".to_vec()),
			std::ops::Bound::Excluded(b"l".to_vec()),
		)
		.await
		.unwrap();
	assert_eq!(restored, 3, "changed, deleted and created");

	let txn = child.begin().unwrap();
	assert_eq!(txn.get(b"k/changed").unwrap(), Some(b"inherited".to_vec()), "restored");
	assert_eq!(txn.get(b"k/deleted").unwrap(), Some(b"inherited".to_vec()), "un-deleted");
	assert_eq!(txn.get(b"k/created").unwrap(), None, "nothing was inherited for it");
	assert_eq!(txn.get(b"k/kept").unwrap(), Some(b"inherited".to_vec()), "never touched");
	assert_eq!(
		txn.get(b"outside").unwrap(),
		Some(b"by-child".to_vec()),
		"outside the range, so untouched"
	);
	drop(txn);

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"k/changed").unwrap(), Some(b"inherited".to_vec()), "the parent is intact");
	drop(txn);

	// History is not rewritten, and the diff is where that shows. A revert is a
	// compensating WRITE, not an erasure, so the branch still reports four
	// changed keys — it has simply written values equal to what it inherited.
	// Anything that made this number shrink would be rewriting history.
	let changes = child.diff().unwrap().collect().unwrap();
	assert_eq!(changes.len(), 4, "a revert adds writes; it does not remove them");
	let reverted = changes.iter().find(|entry| entry.key == b"k/changed").unwrap();
	assert_eq!(
		reverted.op,
		crate::DiffOp::Set(b"inherited".to_vec()),
		"the branch's newest write to it restores the inherited value"
	);
	let recreated = changes.iter().find(|entry| entry.key == b"k/created").unwrap();
	assert!(recreated.op.is_delete(), "the key it invented is now tombstoned, not absent");
}

/// Reverting twice changes nothing the second time, and a branch already at its
/// inherited state has nothing to compensate for.
#[test(tokio::test)]
async fn revert_is_idempotent_and_writes_nothing_when_there_is_nothing_to_undo() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"inherited").unwrap();
	txn.commit().await.unwrap();
	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();

	assert_eq!(
		child.revert_range(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded).await.unwrap(),
		0,
		"a branch that changed nothing has nothing to restore"
	);

	let mut txn = child.begin().unwrap();
	txn.set(b"k", b"by-child").unwrap();
	txn.commit().await.unwrap();

	assert_eq!(
		child.revert_range(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded).await.unwrap(),
		1
	);
	assert_eq!(
		child.revert_range(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded).await.unwrap(),
		0,
		"the second revert has nothing left to compensate for"
	);
	let txn = child.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"inherited".to_vec()));
}

/// A branch with no lineage inherited nothing, so there is no state to restore
/// to and the request is refused rather than guessed at.
#[test(tokio::test)]
async fn revert_refuses_a_branch_with_no_fork_lineage() {
	let (store, _temp) = create_store();
	let plain = store.create_branch("standalone").unwrap();
	let mut txn = plain.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();

	let error = plain
		.revert_range(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
		.await
		.expect_err("a branch with nothing to inherit cannot be reverted");
	assert!(matches!(error, Error::InvalidArgument(_)), "got {error}");
}

// ===== V2a: the strategy set, and the one place it turns into an action =====

/// Builds a source and target that conflict on exactly one key, and agree
/// everywhere else. Returns (source, target) plus the key that conflicts.
async fn one_conflict_fixture(store: &Tree) -> (crate::BranchHandle, crate::BranchHandle) {
	let mut txn = store.begin().unwrap();
	txn.set(b"clean", b"base").unwrap();
	txn.set(b"fought-over", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();

	let mut txn = child.begin().unwrap();
	txn.set(b"clean", b"by-source").unwrap();
	txn.set(b"fought-over", b"by-source").unwrap();
	txn.commit().await.unwrap();

	let mut txn = store.begin().unwrap();
	txn.set(b"fought-over", b"by-target").unwrap();
	txn.commit().await.unwrap();

	(child, main)
}

/// `TargetWins` keeps the target's value on a conflict and still applies
/// everything that does not conflict. The counters distinguish the two.
#[test(tokio::test)]
async fn target_wins_keeps_the_target_and_still_applies_the_rest() {
	let (store, _temp) = create_store();
	let (child, main) = one_conflict_fixture(&store).await;

	let outcome = child.merge_into(&main, MergeStrategy::TargetWins).await.unwrap();
	assert_eq!(outcome.applied, 1, "the clean key");
	assert_eq!(outcome.resolved, 1, "the conflict was settled, by keeping the target");

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"clean").unwrap(), Some(b"by-source".to_vec()));
	assert_eq!(
		txn.get(b"fought-over").unwrap(),
		Some(b"by-target".to_vec()),
		"TargetWins must not write the source's value"
	);
}

/// The mirror, so that a strategy silently doing the opposite of its name
/// cannot pass. Same fixture, same conflict, opposite outcome on that key —
/// and identical on the key that never conflicted.
#[test(tokio::test)]
async fn source_wins_is_the_exact_mirror_of_target_wins() {
	let (store, _temp) = create_store();
	let (child, main) = one_conflict_fixture(&store).await;

	let outcome = child.merge_into(&main, MergeStrategy::SourceWins).await.unwrap();
	assert_eq!(outcome.applied, 2, "the clean key and the resolved conflict");
	assert_eq!(outcome.resolved, 1);

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"clean").unwrap(), Some(b"by-source".to_vec()));
	assert_eq!(txn.get(b"fought-over").unwrap(), Some(b"by-source".to_vec()));
}

/// A resolver decides per key, and can answer with something neither side
/// proposed. It sees the base, source and target values, which is what makes a
/// real decision possible.
#[test(tokio::test)]
async fn a_resolver_decides_each_conflict_and_may_invent_a_value() {
	let (store, _temp) = create_store();
	let (child, main) = one_conflict_fixture(&store).await;

	let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
	let recorder = std::sync::Arc::clone(&seen);
	let resolver = move |conflict: &crate::Conflict| {
		recorder.lock().unwrap().push((
			conflict.key.clone(),
			conflict.base.clone(),
			conflict.source.clone(),
			conflict.target.clone(),
		));
		crate::ConflictChoice::Value(Some(b"negotiated".to_vec()))
	};

	let outcome = child
		.merge_into(&main, MergeStrategy::Resolve(std::sync::Arc::new(resolver)))
		.await
		.unwrap();
	assert_eq!(outcome.applied, 2);
	assert_eq!(outcome.resolved, 1);

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"fought-over").unwrap(), Some(b"negotiated".to_vec()));
	assert_eq!(txn.get(b"clean").unwrap(), Some(b"by-source".to_vec()), "no resolver for this one");

	// The resolver was handed all three sides, and only for the key that
	// actually conflicted.
	let seen = seen.lock().unwrap();
	assert_eq!(seen.len(), 2, "asked once per pass, for one key");
	let (key, base, source, target) = &seen[0];
	assert_eq!(key, b"fought-over");
	assert_eq!(base.as_deref(), Some(b"base".as_slice()));
	assert_eq!(source.as_deref(), Some(b"by-source".as_slice()));
	assert_eq!(target.as_deref(), Some(b"by-target".as_slice()));
}

/// A resolver can delete, and can refuse the whole merge.
#[test(tokio::test)]
async fn a_resolver_can_delete_a_key_or_refuse_the_merge() {
	let (store, _temp) = create_store();
	let (child, main) = one_conflict_fixture(&store).await;

	let refuse = |_: &crate::Conflict| crate::ConflictChoice::Refuse;
	let error = child
		.merge_into(&main, MergeStrategy::Resolve(std::sync::Arc::new(refuse)))
		.await
		.expect_err("a refusing resolver must refuse the merge");
	assert!(
		matches!(
			error,
			Error::MergeConflicts {
				count: 1
			}
		),
		"got {error}"
	);
	let txn = store.begin().unwrap();
	assert_eq!(
		txn.get(b"clean").unwrap(),
		Some(b"base".to_vec()),
		"a refusal writes nothing at all, not even the keys that did not conflict"
	);
	drop(txn);

	let delete = |_: &crate::Conflict| crate::ConflictChoice::Value(None);
	let outcome =
		child.merge_into(&main, MergeStrategy::Resolve(std::sync::Arc::new(delete))).await.unwrap();
	assert_eq!(outcome.resolved, 1);
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"fought-over").unwrap(), None, "the resolver deleted it");
	assert_eq!(txn.get(b"clean").unwrap(), Some(b"by-source".to_vec()));
}

/// `Source` and `Target` choices are shorthands for the two built-in
/// strategies, per key.
#[test(tokio::test)]
async fn a_resolver_can_choose_either_side_per_key() {
	let (store, _temp) = create_store();

	let mut txn = store.begin().unwrap();
	txn.set(b"a", b"base").unwrap();
	txn.set(b"b", b"base").unwrap();
	txn.commit().await.unwrap();
	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();

	let mut txn = child.begin().unwrap();
	txn.set(b"a", b"by-source").unwrap();
	txn.set(b"b", b"by-source").unwrap();
	txn.commit().await.unwrap();
	let mut txn = store.begin().unwrap();
	txn.set(b"a", b"by-target").unwrap();
	txn.set(b"b", b"by-target").unwrap();
	txn.commit().await.unwrap();

	let per_key = |conflict: &crate::Conflict| {
		if conflict.key == b"a" {
			crate::ConflictChoice::Source
		} else {
			crate::ConflictChoice::Target
		}
	};
	let outcome = child
		.merge_into(&main, MergeStrategy::Resolve(std::sync::Arc::new(per_key)))
		.await
		.unwrap();
	assert_eq!(outcome.resolved, 2, "both conflicts settled");
	assert_eq!(outcome.applied, 1, "only the one that chose Source is written");

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"a").unwrap(), Some(b"by-source".to_vec()));
	assert_eq!(txn.get(b"b").unwrap(), Some(b"by-target".to_vec()));
}

/// A merge can be made conditional on the target not having moved. The oracle
/// only protects the keys the merge writes; this protects the whole branch.
#[test(tokio::test)]
async fn an_expected_head_refuses_a_target_that_moved_anywhere() {
	let (store, _temp) = create_store();

	let mut txn = store.begin().unwrap();
	txn.set(b"anchor", b"base").unwrap();
	txn.commit().await.unwrap();
	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"from-child", b"v").unwrap();
	txn.commit().await.unwrap();

	let head = main.info().unwrap().last_write_seq;

	// A write on a key this merge never touches still moves the branch.
	let mut txn = store.begin().unwrap();
	txn.set(b"unrelated", b"v").unwrap();
	txn.commit().await.unwrap();

	let error = child
		.merge_into_expecting(&main, MergeStrategy::Strict, head)
		.await
		.expect_err("the target moved, so the merge must be refused");
	let Error::UnexpectedHead {
		expected,
		actual,
	} = error
	else {
		panic!("expected UnexpectedHead, got {error}");
	};
	assert_eq!(expected, head);
	assert_ne!(actual, head);

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"from-child").unwrap(), None, "nothing may have been written");
	drop(txn);

	// Control: with the current head it goes through, so the refusal above was
	// the condition and not something else.
	let head_now = main.info().unwrap().last_write_seq;
	let outcome = child.merge_into_expecting(&main, MergeStrategy::Strict, head_now).await.unwrap();
	assert_eq!(outcome.applied, 1);
}

// ===== PD3c: the two probes, and their equivalence =====

/// Builds a store where every row of the decision table is represented, and
/// returns the source and target.
///
/// A merge is only as trustworthy as the agreement between the two ways it can
/// ask "did the target move", so the fixture deliberately covers all of them.
async fn decision_table_fixture(store: &Tree) -> (crate::BranchHandle, crate::BranchHandle) {
	let mut txn = store.begin().unwrap();
	for key in ["clean", "reverted", "converged", "both", "source-deleted", "target-deleted"] {
		txn.set(key.as_bytes(), b"base").unwrap();
	}
	txn.set(b"target-only", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "probe-source", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();

	let mut txn = child.begin().unwrap();
	txn.set(b"clean", b"by-source").unwrap();
	txn.set(b"reverted", b"by-source").unwrap();
	txn.set(b"converged", b"agreed").unwrap();
	txn.set(b"both", b"by-source").unwrap();
	txn.delete(b"source-deleted").unwrap();
	txn.set(b"target-deleted", b"by-source").unwrap();
	txn.set(b"fresh-both", b"by-source").unwrap();
	txn.commit().await.unwrap();

	let mut txn = store.begin().unwrap();
	txn.set(b"reverted", b"base").unwrap(); // moved, then back to the base value
	txn.set(b"converged", b"agreed").unwrap(); // reached the source's value alone
	txn.set(b"both", b"by-target").unwrap();
	txn.set(b"source-deleted", b"by-target").unwrap();
	txn.delete(b"target-deleted").unwrap();
	txn.set(b"fresh-both", b"by-target").unwrap(); // neither side had it at the fork
	txn.set(b"target-only", b"by-target").unwrap(); // the source never touched it
	txn.commit().await.unwrap();

	(child, main)
}

/// The two probes must return the same verdicts on the same state. This is the
/// test that lets the crossover between them be a performance decision rather
/// than a semantic one (plan A7, deferred here from PD1).
#[test(tokio::test)]
async fn the_point_and_scan_probes_agree_on_every_verdict() {
	let (store, _temp) = create_store();
	let (child, main) = decision_table_fixture(&store).await;

	let session = child.merge_session(&main).unwrap();
	let by_point = session.report_with(&mut session.point_probe()).unwrap();
	let by_scan = session.report_with(&mut session.scan_probe().unwrap()).unwrap();

	// The fixture has to actually exercise every branch, or agreement is cheap.
	assert_eq!(applied(&by_point).len(), 2, "clean and reverted");
	assert_eq!(by_point.converged, 1, "converged");
	assert_eq!(
		conflicted(&by_point),
		vec![
			b"both".to_vec(),
			b"fresh-both".to_vec(),
			b"source-deleted".to_vec(),
			b"target-deleted".to_vec()
		],
		"all three conflict kinds, plus one neither side had at the fork"
	);
	let kinds: Vec<_> = by_point.conflicts.iter().map(|c| c.kind.clone()).collect();
	assert!(kinds.contains(&ConflictKind::BothModified));
	assert!(kinds.contains(&ConflictKind::DeletedBySourceModifiedByTarget));
	assert!(kinds.contains(&ConflictKind::ModifiedBySourceDeletedByTarget));

	assert_eq!(by_scan, by_point, "the two probes must not disagree about anything");
}

/// The scan probe walks forward and never back, so a caller that asks out of
/// order gets an error rather than an answer from a cursor that has already
/// gone past the key. Answering would be wrong in the most dangerous way
/// available: reporting a moved key as untouched, which applies over it.
#[test(tokio::test)]
async fn a_scan_probe_refuses_to_answer_out_of_order() {
	let (store, _temp) = create_store();
	let (child, main) = decision_table_fixture(&store).await;
	let session = child.merge_session(&main).unwrap();
	let mut probe = session.scan_probe().unwrap();

	assert!(probe.moved_since_base(b"both").unwrap(), "the target changed this one");
	let error = probe
		.moved_since_base(b"a-key-before-it")
		.expect_err("a backwards question must be refused");
	assert!(matches!(error, Error::InvalidArgument(_)), "got {error}");
	// Asking the same key twice is backwards too: the cursor has answered it.
	assert!(probe.moved_since_base(b"both").is_err());
}

/// A merge big enough to take the scan path end to end, through the public API.
#[test(tokio::test)]
async fn a_merge_above_the_scan_threshold_applies_correctly() {
	const KEYS: usize = crate::merge::SCAN_PROBE_THRESHOLD + 16;
	let (store, _temp) = create_store();

	let mut txn = store.begin().unwrap();
	txn.set(b"anchor", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "wide", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	for key in 0..KEYS {
		txn.set(format!("k{key:05}").as_bytes(), b"by-source").unwrap();
	}
	txn.commit().await.unwrap();

	// The target moves on a few of them, so the scan has entries to match
	// against rather than running dry immediately.
	let mut txn = store.begin().unwrap();
	txn.set(b"k00000", b"by-source").unwrap(); // converges
	txn.set(b"k00100", b"by-target").unwrap(); // conflicts
	txn.commit().await.unwrap();

	let main = store.branch("main").unwrap();
	let outcome = child.merge_into(&main, MergeStrategy::SourceWins).await.unwrap();
	assert_eq!(outcome.converged, 1);
	assert_eq!(outcome.resolved, 1);
	assert_eq!(outcome.applied, KEYS - 1, "every key but the converged one");

	let txn = store.begin().unwrap();
	for key in 0..KEYS {
		assert_eq!(
			txn.get(format!("k{key:05}").as_bytes()).unwrap(),
			Some(b"by-source".to_vec()),
			"key {key} did not take the source's value"
		);
	}
}

// ===== PD3b: what a half-finished chunked merge leaves behind =====

/// Drives a chunked merge that fails part-way, and returns the store, the
/// source, the target, and the payload every key carries.
///
/// The interruption is not injected: a concurrent write to a key in a later
/// chunk makes that chunk fail on the oracle, which leaves exactly the state a
/// crash between chunks would — some chunks durable, no edge recorded.
async fn interrupted_chunked_merge(path: std::path::PathBuf) -> (Tree, Vec<u8>) {
	let store =
		TreeBuilder::new().with_path(path).with_max_memtable_size(4 * 1024).build().unwrap();
	let payload = vec![b'x'; 512];

	let mut txn = store.begin().unwrap();
	txn.set(b"anchor", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	// A second source, forked BEFORE the merge so that its base predates the
	// half-merged writes. One forked afterwards would have them as its base and
	// would rightly see no conflict at all.
	store.fork_branch("main", "other", ForkPoint::Head).unwrap();
	for round in 0..40u32 {
		let mut txn = child.begin().unwrap();
		txn.set(format!("k{round:04}").as_bytes(), payload.as_slice()).unwrap();
		txn.commit().await.unwrap();
	}

	let main = store.branch("main").unwrap();
	let session = child.merge_session(&main).unwrap();
	let preflight = session.preflight(&MergeStrategy::Strict).unwrap();

	// A key late in the key order, so the chunks before it have committed by the
	// time its own chunk is refused.
	let mut txn = store.begin().unwrap();
	txn.set(b"k0035", b"blocked").unwrap();
	txn.commit().await.unwrap();

	let error = session
		.apply(&MergeStrategy::Strict, &preflight)
		.await
		.expect_err("the merge must break off");
	assert!(matches!(error, Error::TransactionWriteConflict), "got {error}");

	let txn = store.begin().unwrap();
	assert_eq!(
		txn.get(b"k0000").unwrap(),
		Some(payload.clone()),
		"the fixture must have committed at least one chunk, or nothing is half-merged"
	);
	assert_eq!(txn.get(b"k0039").unwrap(), None, "and must have left at least one chunk unwritten");
	drop(txn);
	(store, payload)
}

/// A merge that broke off part-way finishes on a re-run, and the keys already
/// written are recognised rather than rewritten.
///
/// `converged` is the load-bearing assertion: it is the evidence that the
/// re-run is completing the merge by comparing values, not by replaying blindly.
#[test(tokio::test)]
async fn an_interrupted_chunked_merge_completes_on_a_re_run() {
	let temp_dir = TempDir::new("merge").unwrap();
	let (store, payload) = interrupted_chunked_merge(temp_dir.path().to_path_buf()).await;

	let child = store.branch("work").unwrap();
	let main = store.branch("main").unwrap();
	let outcome = child.merge_into(&main, MergeStrategy::SourceWins).await.unwrap();
	assert!(
		outcome.converged > 0,
		"the keys the first attempt wrote must converge, not be re-applied"
	);
	assert_eq!(outcome.resolved, 1, "only the blocked key genuinely conflicts");

	let txn = store.begin().unwrap();
	for round in 0..40u32 {
		assert_eq!(
			txn.get(format!("k{round:04}").as_bytes()).unwrap(),
			Some(payload.clone()),
			"key {round} did not survive the interruption and re-run"
		);
	}
	drop(txn);
	store.close().await.unwrap();
}

/// The same, across a close and reopen: the half-merged state is durable, and so
/// is the absence of an edge — so the re-run still has the whole merge to offer.
#[test(tokio::test)]
async fn a_half_merged_target_reopens_and_the_re_run_finishes_it() {
	let temp_dir = TempDir::new("merge").unwrap();
	let path = temp_dir.path().to_path_buf();
	let payload = {
		let (store, payload) = interrupted_chunked_merge(path.clone()).await;
		store.close().await.unwrap();
		payload
	};

	let store =
		TreeBuilder::new().with_path(path).with_max_memtable_size(4 * 1024).build().unwrap();
	let child = store.branch("work").unwrap();
	let main = store.branch("main").unwrap();

	let outcome = child.merge_into(&main, MergeStrategy::SourceWins).await.unwrap();
	assert!(outcome.converged > 0, "the durable half must converge across the reopen");

	let txn = store.begin().unwrap();
	for round in 0..40u32 {
		assert_eq!(
			txn.get(format!("k{round:04}").as_bytes()).unwrap(),
			Some(payload.clone()),
			"key {round} did not survive the reopen and re-run"
		);
	}
	drop(txn);

	// The edge is recorded now, so there is nothing left to offer.
	assert_eq!(child.preview_merge_into(&main).unwrap().apply_count(), 0);
	store.close().await.unwrap();
}

/// A different source merging into a half-merged target sees keys it did not put
/// there. It must conflict rather than overwrite them: an unfinished merge is
/// not an invitation to trample it.
#[test(tokio::test)]
async fn a_second_source_finds_a_half_merged_target_and_conflicts() {
	let temp_dir = TempDir::new("merge").unwrap();
	let (store, _payload) = interrupted_chunked_merge(temp_dir.path().to_path_buf()).await;

	let other = store.branch("other").unwrap();
	let mut txn = other.begin().unwrap();
	txn.set(b"k0000", b"from-other").unwrap();
	txn.commit().await.unwrap();

	let main = store.branch("main").unwrap();
	let report = other.preview_merge_into(&main).unwrap();
	assert_eq!(
		conflicted(&report),
		vec![b"k0000".to_vec()],
		"a key the unfinished merge already wrote is a conflict for anyone else"
	);
	store.close().await.unwrap();
}

/// A merge decides what to write at one moment and writes it at a later one. A/// A merge decides
/// what to write at one moment and writes it at a later one. A target write landing in that gap
/// must not vanish.
///
/// It is not enough for the plan to have missed it — the oracle judges a commit
/// against the transaction's start sequence, so a transaction built *after* the
/// concurrent write would accept it as ancient history and overwrite it with no
/// conflict at all. The merge therefore commits every chunk at the sequence it
/// PLANNED at, which is what turns this into a detected conflict.
///
/// Driving the two halves separately is the only way to make the window
/// deterministic; the halves are the same ones `merge_into` uses.
#[test(tokio::test)]
async fn a_target_write_between_planning_and_writing_conflicts() {
	let (store, _temp) = create_store();

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let main = store.branch("main").unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"k", b"from-child").unwrap();
	txn.commit().await.unwrap();

	// Plan: the target is untouched, so this is clean.
	let session = child.merge_session(&main).unwrap();
	let preflight = session.preflight(&MergeStrategy::Strict).unwrap();
	assert_eq!(
		preflight.conflicts, 0,
		"the plan must be clean, or the window is not what is tested"
	);
	assert_eq!(preflight.writes, 1);

	// The window: someone writes the very key the merge is about to write.
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"concurrent").unwrap();
	txn.commit().await.unwrap();

	let error = session
		.apply(&MergeStrategy::Strict, &preflight)
		.await
		.expect_err("a write inside the planning window must be a conflict, not a casualty");
	assert!(matches!(error, Error::TransactionWriteConflict), "got {error}");

	let txn = store.begin().unwrap();
	assert_eq!(
		txn.get(b"k").unwrap(),
		Some(b"concurrent".to_vec()),
		"the concurrent write must still be there"
	);
}

/// A merge small enough for one batch stays ONE transaction, which is what makes
/// it atomic. Chunking is the exception, not the new normal.
#[test(tokio::test)]
async fn a_merge_within_the_budget_stays_one_atomic_chunk() {
	let (store, _temp) = create_store();

	let mut txn = store.begin().unwrap();
	txn.set(b"anchor", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "small", ForkPoint::Head).unwrap();
	for round in 0..8u32 {
		let mut txn = child.begin().unwrap();
		txn.set(format!("k{round}").as_bytes(), b"v").unwrap();
		txn.commit().await.unwrap();
	}

	let main = store.branch("main").unwrap();
	let outcome = child.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	assert_eq!(outcome.applied, 8);
	assert_eq!(outcome.chunks, 1, "everything under the budget must land at once");
}

/// The one size chunking cannot fix: a single entry bigger than the whole
/// budget. One key cannot be split across two batches, so it is refused up
/// front, with both numbers, before anything is written.
#[test(tokio::test)]
async fn a_single_entry_larger_than_the_budget_is_refused_before_writing() {
	const BUDGET: usize = 4 * 1024;
	let (store, _temp) = create_store_with_budget(BUDGET);

	let mut txn = store.begin().unwrap();
	txn.set(b"anchor", b"base").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "huge", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"small", b"v").unwrap();
	txn.set(b"oversized", vec![b'x'; BUDGET + 1].as_slice()).unwrap();
	txn.commit().await.unwrap();

	let main = store.branch("main").unwrap();
	let error = child
		.merge_into(&main, MergeStrategy::Strict)
		.await
		.expect_err("an unchunkable entry must be refused");
	let Error::MergeTooLarge {
		estimated_bytes,
		budget_bytes,
	} = error
	else {
		panic!("expected MergeTooLarge, got {error}");
	};
	assert_eq!(budget_bytes, BUDGET as u64);
	assert!(
		estimated_bytes > budget_bytes,
		"the error must name the offending entry, not the whole merge: {estimated_bytes}"
	);

	// Refused during the preflight, so not even the entries that would fit were
	// written.
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"small").unwrap(), None, "nothing may have been written");
	assert_eq!(txn.get(b"oversized").unwrap(), None);
}

/// A scoped merge applies only inside its range, at both ends.
///
/// `merge_range` takes caller-supplied `Bound`s, and was the first API in this
/// store that could. An inclusive upper that admitted the next key, or an
/// exclusive lower that admitted the key it excluded, silently applies a
/// branch's changes to keys the caller never named — on the operation with the
/// least room for surprise.
#[test(tokio::test)]
async fn a_scoped_merge_applies_only_inside_its_range() {
	use std::ops::Bound::{Excluded, Included, Unbounded};

	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	for key in [&b"a"[..], b"b", b"c", b"d"] {
		txn.set(key, b"base").unwrap();
	}
	txn.commit().await.unwrap();

	let target = store.branch("main").unwrap();

	// Each arm gets its own source branch, so the arms cannot mask each other.
	for (name, lower, upper, expected) in [
		("incl-upper", Included(b"b".to_vec()), Included(b"c".to_vec()), vec![&b"b"[..], b"c"]),
		("excl-lower", Excluded(b"b".to_vec()), Unbounded, vec![&b"c"[..], b"d"]),
		("excl-upper", Included(b"b".to_vec()), Excluded(b"c".to_vec()), vec![&b"b"[..]]),
	] {
		let source = store.fork_branch("main", name, ForkPoint::Head).unwrap();
		let mut txn = source.begin().unwrap();
		for key in [&b"a"[..], b"b", b"c", b"d"] {
			txn.set(key, name.as_bytes()).unwrap();
		}
		txn.commit().await.unwrap();

		let outcome =
			source.merge_range(&target, MergeStrategy::SourceWins, lower, upper).await.unwrap();
		assert_eq!(
			outcome.applied,
			expected.len(),
			"{name}: merged a different number of keys than the range names"
		);

		let txn = target.begin().unwrap();
		for key in [&b"a"[..], b"b", b"c", b"d"] {
			let got = txn.get(key).unwrap().unwrap();
			let should_be_merged = expected.contains(&key);
			assert_eq!(
				got == name.as_bytes(),
				should_be_merged,
				"{name}: key {:?} was {}",
				String::from_utf8_lossy(key),
				if should_be_merged {
					"not merged but should have been"
				} else {
					"merged but is outside the range"
				}
			);
		}
		drop(txn);

		// Put the target back so the next arm starts from the same state.
		let mut txn = target.begin().unwrap();
		for key in [&b"a"[..], b"b", b"c", b"d"] {
			txn.set(key, b"base").unwrap();
		}
		txn.commit().await.unwrap();
	}
}

/// A revert compensates only inside its range, at both ends. Same hazard as the
/// scoped merge, on the other operation that takes caller-supplied bounds:
/// reverting a key the caller excluded overwrites it with an older value.
#[test(tokio::test)]
async fn a_scoped_revert_compensates_only_inside_its_range() {
	use std::ops::Bound::{Excluded, Unbounded};

	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	for key in [&b"a"[..], b"b", b"c"] {
		txn.set(key, b"base").unwrap();
	}
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "child", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	for key in [&b"a"[..], b"b", b"c"] {
		txn.set(key, b"child").unwrap();
	}
	txn.commit().await.unwrap();

	// (a, ..] — `a` is excluded and must keep the branch's own value.
	let reverted = child.revert_range(Excluded(b"a".to_vec()), Unbounded).await.unwrap();
	assert_eq!(reverted, 2, "only `b` and `c` are inside (a, ..]");

	let txn = child.begin().unwrap();
	assert_eq!(
		txn.get(b"a").unwrap(),
		Some(b"child".to_vec()),
		"the excluded key must not have been reverted"
	);
	assert_eq!(txn.get(b"b").unwrap(), Some(b"base".to_vec()));
	assert_eq!(txn.get(b"c").unwrap(), Some(b"base".to_vec()));
}
