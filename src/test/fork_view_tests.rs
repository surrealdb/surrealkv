//! FK3 gate tests: logical inherited views and the version-retention promise.
//!
//! A fork child holds no physical table references. Its view of an ancestor is
//! recomputed from catalog anchors on every read (design §3.2), which is only
//! sound because the parent's compaction preserves what the child can see
//! (§3.3a). These tests cover both halves, and each retention arm ships the
//! sabotage twin that reddens when the mechanism is switched off.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::authority::store::KEEP_METADATA_VERSIONS;
use crate::batch::BatchOwner;
use crate::branch::{ForkPoint, ForkReceipt, MAX_VIEW_DEPTH};
use crate::compaction::compactor::CompactionOptions;
use crate::compaction::leveled::Strategy;
use crate::error::Result;
use crate::iter::NO_HISTORY_PIN;
use crate::lsm::Tree;
use crate::transaction::{Transaction, TransactionOptions};
use crate::{Error, LSMIterator, TreeBuilder};

fn create_store_with<F>(configure: F) -> (Tree, TempDir)
where
	F: FnOnce(TreeBuilder) -> TreeBuilder,
{
	let temp_dir = TempDir::new("fork-view").unwrap();
	let path = temp_dir.path().to_path_buf();
	let tree = configure(TreeBuilder::new().with_path(path)).build().unwrap();
	(tree, temp_dir)
}

fn begin_owned_rw(store: &Tree, owner: BatchOwner) -> Transaction {
	Transaction::new_owned(Arc::clone(&store.core), TransactionOptions::new(), owner).unwrap()
}

/// The current visible sequence: a durable fork anchor once the writes below it
/// have been flushed. FK4 establishes this under a commit fence; FK3's seam
/// takes it as given, so the tests flush first.
fn visible_seq(store: &Tree) -> u64 {
	store.core.inner.visible_seq_num.load(Ordering::Acquire)
}

fn fork_at(store: &Tree, name: &str, parent: BatchOwner, anchor: u64) -> BatchOwner {
	store.core.inner.create_fork_entry_for_test(name, parent, anchor).unwrap()
}

fn collect_forward(iter: &mut impl LSMIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
	let mut out = Vec::new();
	let mut valid = iter.seek_first().unwrap();
	while valid {
		out.push((iter.key().user_key().to_vec(), iter.value_encoded().unwrap().to_vec()));
		valid = iter.next().unwrap();
	}
	out
}

// ===== Read layering =====

/// The child sees the parent as of its anchor and never past it: rows committed
/// before the fork are inherited, rows committed after it are structurally
/// invisible. The parent arm is the non-vacuity check — it proves the post-fork
/// writes really landed and really are newer.
#[test(tokio::test)]
async fn child_inherits_the_parent_as_of_its_anchor_only() {
	let (store, _temp) = create_store_with(|b| b);

	let mut txn = store.begin().unwrap();
	txn.set(b"before/a", b"a-at-fork").unwrap();
	txn.set(b"before/b", b"b-at-fork").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();

	let anchor = visible_seq(&store);
	let child = fork_at(&store, "fork/reads", BatchOwner::DEFAULT, anchor);

	// Post-fork parent writes: an overwrite and a brand-new key.
	let mut txn = store.begin().unwrap();
	txn.set(b"before/a", b"a-after-fork").unwrap();
	txn.set(b"after/c", b"c-after-fork").unwrap();
	txn.commit().await.unwrap();

	let txn = begin_owned_rw(&store, child);
	assert_eq!(
		txn.get(b"before/a").unwrap(),
		Some(b"a-at-fork".to_vec()),
		"the child must read the version at its anchor, not the parent's newer one"
	);
	assert_eq!(txn.get(b"before/b").unwrap(), Some(b"b-at-fork".to_vec()));
	assert_eq!(
		txn.get(b"after/c").unwrap(),
		None,
		"a key first written after the fork must be invisible to the child"
	);
	drop(txn);

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"before/a").unwrap(), Some(b"a-after-fork".to_vec()));
	assert_eq!(txn.get(b"after/c").unwrap(), Some(b"c-after-fork".to_vec()));
}

/// A child write shadows the inherited row for the child only, and a child
/// tombstone hides an inherited row without reaching the parent. Both directions
/// matter: shadowing proves the child's own layer wins, and the parent arms
/// prove no write crossed the branch boundary.
#[test(tokio::test)]
async fn child_writes_and_tombstones_shadow_the_inherited_view() {
	let (store, _temp) = create_store_with(|b| b);

	let mut txn = store.begin().unwrap();
	txn.set(b"shadowed", b"parent-v").unwrap();
	txn.set(b"hidden", b"parent-v").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();

	let child = fork_at(&store, "fork/shadow", BatchOwner::DEFAULT, visible_seq(&store));

	let mut txn = begin_owned_rw(&store, child);
	txn.set(b"shadowed", b"child-v").unwrap();
	txn.delete(b"hidden").unwrap();
	txn.commit().await.unwrap();

	let txn = begin_owned_rw(&store, child);
	assert_eq!(txn.get(b"shadowed").unwrap(), Some(b"child-v".to_vec()));
	assert_eq!(
		txn.get(b"hidden").unwrap(),
		None,
		"a child tombstone must hide the inherited row from farther layers"
	);
	drop(txn);

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"shadowed").unwrap(), Some(b"parent-v".to_vec()));
	assert_eq!(txn.get(b"hidden").unwrap(), Some(b"parent-v".to_vec()));
}

/// Range iteration merges the child's own layer with the capped inherited layer:
/// same visibility rules as point reads, applied per layer *before* the merge,
/// because a single global sequence filter cannot express a per-layer cap.
#[test(tokio::test)]
async fn range_iteration_merges_layers_under_per_layer_caps() {
	let (store, _temp) = create_store_with(|b| b);

	let mut txn = store.begin().unwrap();
	txn.set(b"k1", b"parent-1").unwrap();
	txn.set(b"k2", b"parent-2").unwrap();
	txn.set(b"k3", b"parent-3").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();

	let child = fork_at(&store, "fork/range", BatchOwner::DEFAULT, visible_seq(&store));

	// Post-fork parent activity the child must not observe.
	let mut txn = store.begin().unwrap();
	txn.set(b"k2", b"parent-2-late").unwrap();
	txn.set(b"k4", b"parent-4-late").unwrap();
	txn.commit().await.unwrap();

	// The child's own overwrite of an inherited key.
	let mut txn = begin_owned_rw(&store, child);
	txn.set(b"k3", b"child-3").unwrap();
	txn.commit().await.unwrap();

	{
		let txn = begin_owned_rw(&store, child);
		let mut iter = txn.range(b"k0", b"k9").unwrap();
		assert_eq!(
			collect_forward(&mut iter),
			vec![
				(b"k1".to_vec(), b"parent-1".to_vec()),
				(b"k2".to_vec(), b"parent-2".to_vec()),
				(b"k3".to_vec(), b"child-3".to_vec()),
			],
			"the child's scan must show inherited rows at the cap, its own newer row, and no post-fork parent rows"
		);
	}

	let txn = store.begin().unwrap();
	let mut iter = txn.range(b"k0", b"k9").unwrap();
	assert_eq!(
		collect_forward(&mut iter),
		vec![
			(b"k1".to_vec(), b"parent-1".to_vec()),
			(b"k2".to_vec(), b"parent-2-late".to_vec()),
			(b"k3".to_vec(), b"parent-3".to_vec()),
			(b"k4".to_vec(), b"parent-4-late".to_vec()),
		],
		"the parent's own scan is unaffected by the child"
	);
}

/// A grandchild's view of its grandparent is capped at the *minimum* anchor on
/// the path, so a row the middle branch could see may still be invisible to the
/// grandchild. Cumulative caps are what make deep chains sound.
#[test(tokio::test)]
async fn grandchild_view_caps_at_the_minimum_anchor_on_the_path() {
	let (store, _temp) = create_store_with(|b| b);

	let mut txn = store.begin().unwrap();
	txn.set(b"early", b"root-early").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();
	let low_anchor = visible_seq(&store);

	// A grandparent row committed after the low anchor: visible to a child
	// forked later, invisible through a link capped at `low_anchor`.
	let mut txn = store.begin().unwrap();
	txn.set(b"late", b"root-late").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();
	let high_anchor = visible_seq(&store);

	let late_child = fork_at(&store, "fork/late", BatchOwner::DEFAULT, high_anchor);
	let txn = begin_owned_rw(&store, late_child);
	assert_eq!(
		txn.get(b"late").unwrap(),
		Some(b"root-late".to_vec()),
		"a child forked above the row must see it"
	);
	drop(txn);

	let early_child = fork_at(&store, "fork/early", BatchOwner::DEFAULT, low_anchor);
	let grandchild = fork_at(&store, "fork/early/grand", early_child, high_anchor);

	let txn = begin_owned_rw(&store, grandchild);
	assert_eq!(txn.get(b"early").unwrap(), Some(b"root-early".to_vec()));
	assert_eq!(
		txn.get(b"late").unwrap(),
		None,
		"the grandchild's cap is the minimum anchor on the path, not its own anchor"
	);
}

/// Chains deeper than the view budget fail closed with a typed error naming
/// materialization, rather than silently truncating the read stack (which would
/// return wrong answers) or walking an unbounded chain per read.
#[test(tokio::test)]
async fn ancestor_chain_beyond_the_view_budget_fails_closed() {
	let (store, _temp) = create_store_with(|b| b);

	let mut txn = store.begin().unwrap();
	txn.set(b"root", b"v").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();
	let anchor = visible_seq(&store);

	let mut owner = BatchOwner::DEFAULT;
	for depth in 0..MAX_VIEW_DEPTH {
		owner = fork_at(&store, &format!("chain/{depth}"), owner, anchor);
	}
	// At exactly the budget the read still resolves.
	let txn = begin_owned_rw(&store, owner);
	assert_eq!(txn.get(b"root").unwrap(), Some(b"v".to_vec()));
	drop(txn);

	let too_deep = fork_at(&store, "chain/over", owner, anchor);
	let error =
		Transaction::new_owned(Arc::clone(&store.core), TransactionOptions::new(), too_deep)
			.and_then(|txn| txn.get(b"root"))
			.expect_err("a chain past the budget must not resolve");
	assert!(
		matches!(error, Error::MaterializationRequired { depth } if depth == MAX_VIEW_DEPTH),
		"expected a typed materialization error, got {error}"
	);
}

// ===== Retention promise (§3.3a) =====

/// End-to-end: the parent hard-deletes a forked key and compacts all the way to
/// its bottom level. The child must still read the pre-fork row, and the parent
/// must still read it as deleted. This is the wiring test for
/// `history_pin_floor` and `force_not_bottom` — the unit arms above cover the
/// rule, this one covers the plumbing that feeds it from the catalog.
#[test(tokio::test)]
async fn parent_compaction_to_bottom_level_preserves_what_the_child_inherits() {
	// Two levels, so an L0 -> L1 compaction targets the bottom level.
	let (store, _temp) = create_store_with(|b| b.with_level_count(2));

	let mut txn = store.begin().unwrap();
	txn.set(b"forked", b"pre-fork").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();

	let child = fork_at(&store, "fork/retain", BatchOwner::DEFAULT, visible_seq(&store));

	// The parent hard-deletes the key, then writes enough further flushes to
	// push L0 past its compaction trigger. Without the pin, the L0 -> L1 merge
	// takes the delete-at-bottom shortcut and erases both versions.
	let mut txn = store.begin().unwrap();
	txn.delete(b"forked").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();
	for round in 0..4u32 {
		let mut txn = store.begin().unwrap();
		txn.set(format!("filler/{round}").as_bytes(), b"f").unwrap();
		txn.commit().await.unwrap();
		store.flush().unwrap();
	}

	let level_table_count = |level: usize| {
		store
			.core
			.inner
			.level_manifest
			.read()
			.unwrap()
			.levels_for(BatchOwner::DEFAULT)
			.map(|levels| levels.get_levels().get(level).map(|l| l.tables.len()).unwrap_or(0))
			.unwrap_or(0)
	};
	assert_eq!(level_table_count(0), 6, "fixture must build six L0 tables");

	// Non-vacuity fact: no snapshot may be live here. A leaked snapshot
	// registration would pin the same versions for an unrelated reason and this
	// test would pass with the inherited-view pin deleted.
	assert!(
		store.core.inner.snapshot_tracker.get_all_snapshots().is_empty(),
		"the fixture must leave no live snapshot, or the pin is not what keeps the version"
	);
	let strategy = Arc::new(Strategy::from_options(Arc::clone(&store.core.inner.opts)));
	store.compact(strategy).unwrap();
	assert!(
		level_table_count(0) < 6 && level_table_count(1) > 0,
		"the fixture must actually compact into the bottom level, or this test proves nothing"
	);

	let txn = begin_owned_rw(&store, child);
	assert_eq!(
		txn.get(b"forked").unwrap(),
		Some(b"pre-fork".to_vec()),
		"the parent's compaction must preserve the version its child inherits"
	);
	drop(txn);

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"forked").unwrap(), None, "the parent still reads its own delete");
}

/// The derivation of `force_not_bottom` from the catalog, in all three shapes:
/// a plain owner has a real bottom level, a parent of an Active child does not
/// (the child's view reads below the tombstones being compacted), and a child
/// does not either (its ancestors sit below its own bottom level — C1(c)).
#[test(tokio::test)]
async fn bottom_level_is_disabled_for_both_sides_of_a_fork() {
	let (store, _temp) = create_store_with(|b| b);

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();
	let anchor = visible_seq(&store);

	let plain = store.core.inner.create_branch("plain").unwrap();
	let options = CompactionOptions::for_owner(&store.core.inner, plain).unwrap();
	assert!(!options.force_not_bottom, "a branch with no fork relation has a real bottom level");
	assert_eq!(options.history_pin_floor, NO_HISTORY_PIN);

	let child = fork_at(&store, "fork/flags", BatchOwner::DEFAULT, anchor);

	let parent_options =
		CompactionOptions::for_owner(&store.core.inner, BatchOwner::DEFAULT).unwrap();
	assert!(parent_options.force_not_bottom, "a parent of an Active child has no bottom level");
	assert_eq!(parent_options.history_pin_floor, anchor);

	let child_options = CompactionOptions::for_owner(&store.core.inner, child).unwrap();
	assert!(child_options.force_not_bottom, "a fork child reads below its own bottom level");
	assert_eq!(
		child_options.history_pin_floor, NO_HISTORY_PIN,
		"a childless child pins no history of its own"
	);
}

// ===== FK4: the fork protocol =====

fn fork(store: &Tree, parent: &str, child: &str, at: ForkPoint) -> Result<ForkReceipt> {
	store.core.fork_branch(parent, child, at)
}

/// The fork point is the parent's exact visible head, including rows that are
/// only in its memtables. This is what removed the bounded delta table from the
/// design: the child resolves the parent's live state under a cap, so nothing
/// has to be written to make unflushed rows inheritable.
#[test(tokio::test)]
async fn fork_at_head_is_exact_including_unflushed_parent_rows() {
	let (store, _temp) = create_store_with(|b| b);

	// Committed and NOT flushed: these rows live only in the parent's memtable.
	let mut txn = store.begin().unwrap();
	txn.set(b"unflushed/a", b"v1").unwrap();
	txn.set(b"unflushed/b", b"v2").unwrap();
	txn.commit().await.unwrap();
	assert!(
		store.core.inner.level_manifest.read().unwrap().get_last_sequence() == 0,
		"fixture must leave the rows unflushed, or this proves nothing"
	);

	let receipt = fork(&store, "main", "fork/exact", ForkPoint::Head).unwrap();
	assert_eq!(receipt.fork_seq, visible_seq(&store), "Head is the drained visible head");
	assert_eq!(receipt.parent, BatchOwner::DEFAULT);

	let mut txn = store.begin().unwrap();
	txn.set(b"unflushed/a", b"v1-later").unwrap();
	txn.commit().await.unwrap();

	let txn = begin_owned_rw(&store, receipt.child);
	assert_eq!(txn.get(b"unflushed/a").unwrap(), Some(b"v1".to_vec()));
	assert_eq!(txn.get(b"unflushed/b").unwrap(), Some(b"v2".to_vec()));
	drop(txn);
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"unflushed/a").unwrap(), Some(b"v1-later".to_vec()));
}

/// Every commit acknowledged before the fork is inside the child's view. The
/// fence drains the commit pipeline, so a batch that was applying when the fork
/// began cannot fall between the anchor and the data.
#[test(tokio::test)]
async fn fork_at_head_includes_every_acknowledged_commit() {
	let (store, _temp) = create_store_with(|b| b);

	let mut handles = Vec::new();
	for i in 0..8u32 {
		let core = Arc::clone(&store.core);
		handles.push(tokio::spawn(async move {
			let mut txn =
				Transaction::new_owned(core, TransactionOptions::new(), BatchOwner::DEFAULT)
					.unwrap();
			txn.set(format!("concurrent/{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}));
	}
	for handle in handles {
		handle.await.unwrap();
	}

	let receipt = fork(&store, "main", "fork/drain", ForkPoint::Head).unwrap();
	let txn = begin_owned_rw(&store, receipt.child);
	for i in 0..8u32 {
		assert_eq!(
			txn.get(format!("concurrent/{i}").as_bytes()).unwrap(),
			Some(format!("v{i}").into_bytes()),
			"commit {i} was acknowledged before the fork and must be inherited"
		);
	}
}

/// `AtVersion` serves a historical view: the child reads the value that was
/// current at that sequence, not the parent's newer one. Requires a versioned
/// parent, since a point-in-time store keeps no history to fork from.
#[test(tokio::test)]
async fn fork_at_version_serves_a_historical_view() {
	let (store, _temp) = create_store_with(|b| b.with_versioning(true, 0));

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"first").unwrap();
	txn.commit().await.unwrap();
	let first_seq = visible_seq(&store);

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"second").unwrap();
	txn.commit().await.unwrap();
	assert!(visible_seq(&store) > first_seq, "the second commit must advance the clock");

	let receipt = fork(&store, "main", "fork/historical", ForkPoint::AtVersion(first_seq)).unwrap();
	assert_eq!(receipt.fork_seq, first_seq);

	let txn = begin_owned_rw(&store, receipt.child);
	assert_eq!(txn.get(b"k").unwrap(), Some(b"first".to_vec()));
	drop(txn);
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"second".to_vec()));
}

/// `AtTimestamp` resolves through the commit timeline, exactly or not at all: a
/// timestamp inside the horizon maps to the sequence committed at or before it,
/// and one below the horizon abstains rather than guessing.
#[test(tokio::test)]
async fn fork_at_timestamp_resolves_exactly_or_abstains() {
	let (store, _temp) = create_store_with(|b| b.with_versioning(true, 0));

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"first").unwrap();
	txn.commit().await.unwrap();
	let first_seq = visible_seq(&store);
	let first_ts = store.core.inner.timeline.last_commit_ts();

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"second").unwrap();
	txn.commit().await.unwrap();
	let second_ts = store.core.inner.timeline.last_commit_ts();
	assert!(second_ts > first_ts, "commit timestamps must strictly increase");

	let receipt = fork(&store, "main", "fork/at-ts", ForkPoint::AtTimestamp(first_ts)).unwrap();
	assert_eq!(receipt.fork_seq, first_seq, "the timestamp must resolve to its exact sequence");
	let txn = begin_owned_rw(&store, receipt.child);
	assert_eq!(txn.get(b"k").unwrap(), Some(b"first".to_vec()));
	drop(txn);

	let error = fork(&store, "main", "fork/too-old", ForkPoint::AtTimestamp(first_ts - 1))
		.expect_err("a timestamp below the horizon must abstain");
	assert!(
		matches!(error, Error::TimestampBelowHorizon { .. }),
		"expected an abstain, got {error}"
	);
}

/// A fork point above the parent's visible head is a request to fork the future.
#[test(tokio::test)]
async fn fork_above_the_visible_head_is_refused() {
	let (store, _temp) = create_store_with(|b| b);
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();

	let head = visible_seq(&store);
	let error = fork(&store, "main", "fork/future", ForkPoint::AtVersion(head + 1))
		.expect_err("forking above the head must be refused");
	assert!(error.to_string().contains("above the parent's visible head"), "{error}");
	// The refusal must leave no trace in the catalog.
	assert!(store.core.inner.branch_catalog.read().unwrap().get_by_name("fork/future").is_err());
}

/// Once a compaction has collapsed history, a historical fork into the
/// collapsed range is refused with the boundary rather than served short of
/// rows. The pre-compaction arm is the non-vacuity check: the same fork point
/// succeeds while the history is still intact.
#[test(tokio::test)]
async fn fork_below_the_retention_floor_is_refused() {
	let (store, _temp) = create_store_with(|b| b.with_level_count(2));

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"first").unwrap();
	txn.commit().await.unwrap();
	let first_seq = visible_seq(&store);
	store.flush().unwrap();

	// Same key overwritten and flushed repeatedly: the compaction that merges
	// these tables must drop the older versions of `k`.
	for round in 0..5u32 {
		let mut txn = store.begin().unwrap();
		txn.set(b"k", format!("v{round}").as_bytes()).unwrap();
		txn.commit().await.unwrap();
		store.flush().unwrap();
	}

	// Intact history: the fork point is still servable.
	fork(&store, "main", "fork/before-floor", ForkPoint::AtVersion(first_seq)).unwrap();

	let strategy = Arc::new(Strategy::from_options(Arc::clone(&store.core.inner.opts)));
	store.compact(strategy).unwrap();

	let floor = store.core.inner.level_manifest.read().unwrap().retained_floor(BatchOwner::DEFAULT);
	assert!(
		floor > first_seq,
		"the compaction must have raised the floor past the collapsed history, got {floor}"
	);

	let error = fork(&store, "main", "fork/after-floor", ForkPoint::AtVersion(first_seq))
		.expect_err("a fork into collapsed history must be refused");
	assert!(
		matches!(error, Error::BelowRetentionFloor { requested, floor: reported }
			if requested == first_seq && reported == floor),
		"expected the boundary in the error, got {error}"
	);
	// Head is always servable, floor or not.
	fork(&store, "main", "fork/head-still-fine", ForkPoint::Head).unwrap();
}

/// A retried fork returns the original receipt instead of creating a second
/// branch; a reused name with different lineage or a different fork point is a
/// conflict and fails closed.
#[test(tokio::test)]
async fn fork_retry_is_idempotent_and_divergence_fails_closed() {
	let (store, _temp) = create_store_with(|b| b);
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();
	let head = visible_seq(&store);

	let first = fork(&store, "main", "fork/retry", ForkPoint::AtVersion(head)).unwrap();
	let retried = fork(&store, "main", "fork/retry", ForkPoint::AtVersion(head)).unwrap();
	assert_eq!(first, retried, "the same fork re-issued must return the same receipt");
	let repeated_head = fork(&store, "main", "fork/retry", ForkPoint::Head).unwrap();
	assert_eq!(
		repeated_head, first,
		"an unqualified retry adopts the recorded anchor rather than moving it"
	);

	let divergent = fork(&store, "main", "fork/retry", ForkPoint::AtVersion(head - 1))
		.expect_err("a different fork point under the same name must fail");
	assert!(divergent.to_string().contains("already exists at fork sequence"), "{divergent}");

	// A different parent under the same child name.
	let sibling = fork(&store, "main", "fork/sibling", ForkPoint::Head).unwrap();
	let _ = sibling;
	let wrong_parent = fork(&store, "fork/sibling", "fork/retry", ForkPoint::Head)
		.expect_err("the same name forked from another parent must fail");
	assert!(wrong_parent.to_string().contains("different parent"), "{wrong_parent}");

	// A plain branch's name is not a fork's name.
	store.core.inner.create_branch("plain/name").unwrap();
	let not_a_fork = fork(&store, "main", "plain/name", ForkPoint::Head)
		.expect_err("an existing non-fork branch must not be adopted");
	assert!(not_a_fork.to_string().contains("is not a fork"), "{not_a_fork}");
}

/// The catalog publish is the whole fork. After it, a crash needs no further
/// work: reopen replays the parent's WAL into the parent's runtime and the
/// child's view is recomputed from the catalog, so unflushed inherited rows come
/// back too.
#[test(tokio::test)]
async fn a_published_fork_needs_no_further_work_to_survive_a_crash() {
	let temp_dir = TempDir::new("fork-view").unwrap();
	let path = temp_dir.path().to_path_buf();

	let child = {
		let store =
			TreeBuilder::new().with_path(path.clone()).with_flush_on_close(false).build().unwrap();
		let mut txn = store.begin().unwrap();
		txn.set(b"inherited", b"pre-fork").unwrap();
		txn.commit().await.unwrap();
		let receipt = fork(&store, "main", "fork/crash", ForkPoint::Head).unwrap();
		// Post-fork parent write, also unflushed: must stay outside the view.
		let mut txn = store.begin().unwrap();
		txn.set(b"after", b"post-fork").unwrap();
		txn.commit().await.unwrap();
		store.close().await.unwrap();
		receipt.child
	};

	let store = TreeBuilder::new().with_path(path).with_flush_on_close(false).build().unwrap();
	let txn = begin_owned_rw(&store, child);
	assert_eq!(
		txn.get(b"inherited").unwrap(),
		Some(b"pre-fork".to_vec()),
		"the child's inherited view must survive reopen with no fork-time data copy"
	);
	assert_eq!(txn.get(b"after").unwrap(), None, "the cap must survive reopen too");
	drop(txn);
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"after").unwrap(), Some(b"post-fork".to_vec()));
	drop(txn);
	store.close().await.unwrap();
}

/// A fork that would sit past the view budget is refused at fork time, not left
/// to fail on every subsequent read. The arm at exactly the budget must succeed,
/// or the check would be off by one and silently cost a level of nesting.
#[test(tokio::test)]
async fn fork_past_the_view_budget_is_refused_before_publication() {
	let (store, _temp) = create_store_with(|b| b);
	let mut txn = store.begin().unwrap();
	txn.set(b"root", b"v").unwrap();
	txn.commit().await.unwrap();

	let mut name = "main".to_owned();
	for depth in 0..MAX_VIEW_DEPTH {
		let child = format!("budget/{depth}");
		fork(&store, &name, &child, ForkPoint::Head).unwrap();
		name = child;
	}
	// The deepest legal child still reads.
	let deepest = store.core.inner.branch_catalog.read().unwrap().get_by_name(&name).unwrap().id;
	let owner = BatchOwner {
		branch: deepest,
		generation: store
			.core
			.inner
			.branch_catalog
			.read()
			.unwrap()
			.get_by_name(&name)
			.unwrap()
			.generation,
	};
	let txn = begin_owned_rw(&store, owner);
	assert_eq!(txn.get(b"root").unwrap(), Some(b"v".to_vec()));
	drop(txn);

	let error = fork(&store, &name, "budget/over", ForkPoint::Head)
		.expect_err("a fork past the budget must be refused");
	assert!(
		matches!(error, Error::MaterializationRequired { depth } if depth == MAX_VIEW_DEPTH + 1),
		"expected a typed budget refusal, got {error}"
	);
	assert!(
		store.core.inner.branch_catalog.read().unwrap().get_by_name("budget/over").is_err(),
		"the refusal must leave nothing in the catalog"
	);
}

// ===== FK5: lifecycle, TTL, metadata GC =====

/// Wall-clock nanoseconds, so a TTL in the past is unambiguously due and one at
/// the far end of the range never is — no mock clock needed for either arm.
const EXPIRED_TTL: u64 = 1;
const DISTANT_TTL: u64 = u64::MAX;

fn branch_exists(store: &Tree, name: &str) -> bool {
	store.core.inner.branch_catalog.read().unwrap().get_by_name(name).is_ok()
}

/// A parent whose child still resolves views through it cannot be tombstoned:
/// the catalog loader rejects exactly that shape, so publishing the tombstone
/// would make the store unopenable. Deleting the child first releases the
/// parent — the arm that proves the guard is about the relationship, not about
/// parents in general.
#[test(tokio::test)]
async fn deleting_a_parent_of_active_children_is_refused() {
	let (store, _temp) = create_store_with(|b| b);
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();

	let parent = fork(&store, "main", "life/parent", ForkPoint::Head).unwrap();
	let child = fork(&store, "life/parent", "life/child", ForkPoint::Head).unwrap();

	let error = store
		.core
		.inner
		.delete_branch(parent.child.branch)
		.expect_err("a parent of an active child must not be deletable");
	assert!(error.to_string().contains("active fork children"), "{error}");
	assert!(branch_exists(&store, "life/parent"));

	store.core.inner.delete_branch(child.child.branch).unwrap();
	store.core.inner.delete_branch(parent.child.branch).unwrap();
	assert!(!branch_exists(&store, "life/parent"));
}

/// The maintenance sweep tombstones branches whose TTL has passed and leaves
/// every other branch alone, including one with a TTL far in the future.
#[test(tokio::test)]
async fn the_maintenance_sweep_expires_only_due_branches() {
	let (store, _temp) = create_store_with(|b| b);

	let due = store.core.inner.create_branch("ttl/due").unwrap();
	let later = store.core.inner.create_branch("ttl/later").unwrap();
	store.core.inner.create_branch("ttl/none").unwrap();
	store.core.inner.set_branch_expiry(due.branch, Some(EXPIRED_TTL)).unwrap();
	store.core.inner.set_branch_expiry(later.branch, Some(DISTANT_TTL)).unwrap();

	let (expired, _) = store.core.inner.sweep_branch_maintenance().unwrap();
	assert_eq!(expired, 1, "exactly the due branch expires");
	assert!(!branch_exists(&store, "ttl/due"));
	assert!(branch_exists(&store, "ttl/later"));
	assert!(branch_exists(&store, "ttl/none"));

	// Re-entrant: a second sweep finds nothing new and changes nothing.
	let (expired, _) = store.core.inner.sweep_branch_maintenance().unwrap();
	assert_eq!(expired, 0);

	// A write to an expired branch is fenced, and the tombstone survives reopen
	// because it went out as a catalog version.
	let fenced = Transaction::new_owned(Arc::clone(&store.core), TransactionOptions::new(), due);
	assert!(
		matches!(fenced, Err(Error::BranchFenced)),
		"an expired branch must be fenced against new writes"
	);
}

/// Expiry does not cascade. An expired parent with an Active child is skipped
/// rather than deleted (which would break the child's view) or deleted along
/// with its subtree (which would destroy a branch whose own TTL never fired).
/// Its tombstone lands on the first sweep after the child is gone.
#[test(tokio::test)]
async fn an_expired_parent_of_active_children_is_skipped_not_cascaded() {
	let (store, _temp) = create_store_with(|b| b);
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();

	let parent = fork(&store, "main", "cascade/parent", ForkPoint::Head).unwrap();
	let child = fork(&store, "cascade/parent", "cascade/child", ForkPoint::Head).unwrap();
	store.core.inner.set_branch_expiry(parent.child.branch, Some(EXPIRED_TTL)).unwrap();

	let (expired, _) = store.core.inner.sweep_branch_maintenance().unwrap();
	assert_eq!(expired, 0, "the expired parent must be skipped while its child is active");
	assert!(branch_exists(&store, "cascade/parent"));
	assert!(branch_exists(&store, "cascade/child"));
	// The child can still read through the parent it holds alive.
	let txn = begin_owned_rw(&store, child.child);
	assert_eq!(txn.get(b"k").unwrap(), Some(b"v".to_vec()));
	drop(txn);

	store.core.inner.delete_branch(child.child.branch).unwrap();
	let (expired, _) = store.core.inner.sweep_branch_maintenance().unwrap();
	assert_eq!(expired, 1, "with the child gone, the parent's expiry finally fires");
	assert!(!branch_exists(&store, "cascade/parent"));
}

fn version_file_count(path: &std::path::Path, ext: &str) -> usize {
	std::fs::read_dir(path)
		.map(|entries| {
			entries
				.filter_map(|entry| entry.ok())
				.filter(|entry| {
					let name = entry.file_name().to_string_lossy().to_string();
					!name.starts_with('.') && name.ends_with(&format!(".{ext}"))
				})
				.count()
		})
		.unwrap_or(0)
}

/// Metadata pruning bounds the authority footprint and never removes what the
/// store needs: the newest version of each lineage stays, so the store reopens
/// and every surviving branch is still there.
#[test(tokio::test)]
async fn metadata_pruning_bounds_the_lineage_and_the_store_still_opens() {
	let temp_dir = TempDir::new("fork-view").unwrap();
	let path = temp_dir.path().to_path_buf();
	let catalog_dir = path.join("catalog");

	let names: Vec<String> = (0..20).map(|i| format!("prune/{i}")).collect();
	{
		let store = TreeBuilder::new().with_path(path.clone()).build().unwrap();
		// One catalog version per create: far more than the retained window.
		for name in &names {
			store.core.inner.create_branch(name).unwrap();
		}
		assert!(
			version_file_count(&catalog_dir, "catalog") > KEEP_METADATA_VERSIONS,
			"fixture must publish more versions than are retained"
		);

		let (_, pruned) = store.core.inner.sweep_branch_maintenance().unwrap();
		assert!(pruned > 0, "the sweep must have removed something");
		assert_eq!(
			version_file_count(&catalog_dir, "catalog"),
			KEEP_METADATA_VERSIONS,
			"the catalog lineage must be pruned to exactly the retained window"
		);
		store.close().await.unwrap();
	}

	// Everything the store needs survived: it reopens and the branches are live.
	let store = TreeBuilder::new().with_path(path).build().unwrap();
	for name in &names {
		assert!(branch_exists(&store, name), "{name} must survive metadata pruning");
	}
	store.close().await.unwrap();
}

/// Scale gate: a thousand branches and a hundred forks off one parent, then a
/// reopen. The point is that per-branch metadata stays bounded and the catalog
/// remains loadable at that cardinality.
#[test(tokio::test)]
async fn a_thousand_branches_and_a_hundred_forks_reopen_within_the_metadata_budget() {
	let temp_dir = TempDir::new("fork-view").unwrap();
	let path = temp_dir.path().to_path_buf();

	{
		let store = TreeBuilder::new().with_path(path.clone()).build().unwrap();
		let mut txn = store.begin().unwrap();
		txn.set(b"root", b"v").unwrap();
		txn.commit().await.unwrap();
		store.flush().unwrap();

		for i in 0..900 {
			store.core.inner.create_branch(&format!("scale/plain/{i}")).unwrap();
		}
		for i in 0..100 {
			fork(&store, "main", &format!("scale/fork/{i}"), ForkPoint::Head).unwrap();
		}
		let (_, pruned) = store.core.inner.sweep_branch_maintenance().unwrap();
		assert!(pruned > 0);
		assert_eq!(version_file_count(&path.join("catalog"), "catalog"), KEEP_METADATA_VERSIONS);
		// Idle branches hold no runtime (BR4's invariant, restated at scale).
		assert_eq!(store.core.inner.runtimes.len(), 1);
		store.close().await.unwrap();
	}

	let store = TreeBuilder::new().with_path(path).build().unwrap();
	let owner = {
		let catalog = store.core.inner.branch_catalog.read().unwrap();
		assert_eq!(
			catalog.all_records().filter(|record| !record.deleted).count(),
			1001,
			"main plus 900 plain branches plus 100 forks must all reload"
		);
		// Every fork's inherited view still resolves after the reopen.
		let sample = catalog.get_by_name("scale/fork/42").unwrap();
		BatchOwner {
			branch: sample.id,
			generation: sample.generation,
		}
	};
	let txn = begin_owned_rw(&store, owner);
	assert_eq!(txn.get(b"root").unwrap(), Some(b"v".to_vec()));
	drop(txn);
	store.close().await.unwrap();
}

/// A catalog lineage truncated below what a root already depended on is damage,
/// not a recoverable state, and open says so. Pruning can never cause this — it
/// only removes versions older than the newest — so the planted sabotage deletes
/// the newest versions instead.
#[test(tokio::test)]
async fn a_catalog_truncated_below_the_root_floor_fails_closed() {
	let temp_dir = TempDir::new("fork-view").unwrap();
	let path = temp_dir.path().to_path_buf();
	let catalog_dir = path.join("catalog");

	{
		let store = TreeBuilder::new().with_path(path.clone()).build().unwrap();
		store.core.inner.create_branch("floor/a").unwrap();
		store.core.inner.create_branch("floor/b").unwrap();
		let mut txn = store.begin().unwrap();
		txn.set(b"k", b"v").unwrap();
		txn.commit().await.unwrap();
		// The flush publishes a root, recording the catalog version it saw.
		store.flush().unwrap();
		store.close().await.unwrap();
	}

	// Control: untouched, the store opens.
	{
		let store = TreeBuilder::new().with_path(path.clone()).build().unwrap();
		assert!(branch_exists(&store, "floor/b"));
		store.close().await.unwrap();
	}

	// Sabotage: drop the newest catalog version so the lineage ends below the
	// floor the root recorded.
	let newest = std::fs::read_dir(&catalog_dir)
		.unwrap()
		.filter_map(|entry| entry.ok())
		.map(|entry| entry.file_name().to_string_lossy().to_string())
		.filter(|name| name.ends_with(".catalog") && !name.starts_with('.'))
		.max()
		.expect("a catalog version must exist");
	std::fs::remove_file(catalog_dir.join(newest)).unwrap();

	let error = TreeBuilder::new()
		.with_path(path)
		.build()
		.err()
		.expect("a truncated catalog lineage must fail closed");
	assert!(
		error.to_string().contains("below the root's reclaim floor"),
		"expected the truncation to be named, got {error}"
	);
}

/// `BranchInfo::last_write_seq` must come from the branch's OWN components —
/// its tables and its memtables — never from the manifest's `last_sequence`,
/// which is global and is copied verbatim into every owner's state.
///
/// This lives here rather than in the public-API tests because it has to force
/// flushes to be discriminating: only once a flush has moved the global
/// sequence does reading the wrong field become visible, and only then is the
/// durable-table half of the derivation exercised at all.
#[test(tokio::test)]
async fn last_write_seq_comes_from_the_branch_s_own_components() {
	let (store, _temp) = create_store_with(|b| b);

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();

	let main_seq = store.branch("main").unwrap().info().unwrap().last_write_seq;
	assert_eq!(main_seq, Some(visible_seq(&store)), "a flushed write is still main's own");
	let global = store.core.inner.level_manifest.read().unwrap().get_last_sequence();
	assert!(
		global > 0,
		"the fixture must move the global sequence, or the next arm proves nothing"
	);

	// The discriminating assertion: a fork that has never written reports None
	// even though the global sequence is now non-zero.
	let child = store.fork_branch("main", "silent", ForkPoint::Head).unwrap();
	assert_eq!(
		child.info().unwrap().last_write_seq,
		None,
		"a branch that never wrote must not inherit the global sequence"
	);

	// The child writes and flushes: its own sequence appears, from its own
	// table, and main's does not move.
	let mut txn = child.begin().unwrap();
	txn.set(b"child", b"c").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();
	let child_seq = child.info().unwrap().last_write_seq.expect("the child has written");
	assert!(child_seq > main_seq.unwrap(), "the child's write is newer than main's");
	assert_eq!(
		store.branch("main").unwrap().info().unwrap().last_write_seq,
		main_seq,
		"a child's write must never be attributed to its parent"
	);
}

// ===== PB1: runtime reclamation of tombstoned branches =====

fn sst_count(store: &Tree) -> usize {
	std::fs::read_dir(store.core.inner.opts.sstable_dir())
		.map(|entries| {
			entries
				.filter_map(|entry| entry.ok())
				.filter(|entry| entry.file_name().to_string_lossy().ends_with(".sst"))
				.count()
		})
		.unwrap_or(0)
}

/// Seals and flushes a branch's own memtable.
///
/// `Tree::flush` rotates only the default runtime's active memtable (it flushes
/// every runtime's *immutables*, but seals only main's active), so a branch's
/// data has to be sealed explicitly — the same thing the trickle policy does for
/// it in production.
fn flush_branch(store: &Tree, name: &str) {
	let owner = owner_of(store, name);
	let runtime = store.core.inner.runtimes.get(owner).expect("the branch must have written");
	store.core.inner.rotate_runtime_memtable(&runtime, 0).unwrap();
	store.core.inner.flush_all_immutables_sync().unwrap();
}

fn owner_of(store: &Tree, name: &str) -> BatchOwner {
	let catalog = store.core.inner.branch_catalog.read().unwrap();
	let record = catalog.get_by_name(name).unwrap();
	BatchOwner {
		branch: record.id,
		generation: record.generation,
	}
}

/// A deleted branch's tables are freed by the maintenance sweep, in the same
/// process — not left until the next open. The live branches' tables are
/// untouched, and what survives is exactly what a restart would have kept.
#[test(tokio::test)]
async fn the_sweep_reclaims_a_deleted_branch_s_tables_without_a_restart() {
	let (store, _temp) = create_store_with(|b| b);

	let mut txn = store.begin().unwrap();
	txn.set(b"main-key", b"v").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();
	let after_main = sst_count(&store);
	assert!(after_main > 0, "main must have flushed a table");

	let doomed = store.fork_branch("main", "doomed", ForkPoint::Head).unwrap();
	let mut txn = doomed.begin().unwrap();
	txn.set(b"doomed-key", b"v").unwrap();
	txn.commit().await.unwrap();
	flush_branch(&store, "doomed");
	let with_branch = sst_count(&store);
	assert!(with_branch > after_main, "the branch must have flushed its own table");

	// Deleting alone frees nothing: the tombstone is a catalog fact.
	store.delete_branch("doomed").unwrap();
	assert_eq!(sst_count(&store), with_branch, "delete must not reclaim synchronously");

	let (_, _) = store.core.inner.sweep_branch_maintenance().unwrap();
	assert_eq!(
		sst_count(&store),
		after_main,
		"the sweep must free exactly the deleted branch's tables"
	);

	// main is unharmed, and the sweep is idempotent.
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"main-key").unwrap(), Some(b"v".to_vec()));
	drop(txn);
	store.core.inner.sweep_branch_maintenance().unwrap();
	assert_eq!(sst_count(&store), after_main, "a second sweep finds nothing left to do");
}

/// Reclamation releases the WAL dependencies of the memtables it discards.
///
/// Without this, freeing a branch's tables would pin its log segments forever —
/// trading a bounded table leak for an unbounded WAL leak, which is strictly
/// worse than the bug being fixed. The observable is the tracker's registered
/// component count: a discarded memtable that keeps its dependency is a ghost
/// that no flush will ever release, because its runtime no longer exists.
#[test(tokio::test)]
async fn reclamation_releases_the_wal_dependencies_it_discards() {
	let (store, _temp) = create_store_with(|b| b);
	let active_segment = || store.core.inner.wal.read().get_active_log_number();
	let components =
		|| store.core.inner.wal_dependencies.snapshot(active_segment()).component_count;

	let mut txn = store.begin().unwrap();
	txn.set(b"anchor", b"v").unwrap();
	txn.commit().await.unwrap();
	let baseline = components();
	assert!(baseline > 0, "main's unflushed write must register a dependency");

	// A branch with an UNFLUSHED write: its memtable registers its own
	// dependency on top of main's.
	let doomed = store.fork_branch("main", "unflushed", ForkPoint::Head).unwrap();
	let mut txn = doomed.begin().unwrap();
	txn.set(b"never-flushed", b"v").unwrap();
	txn.commit().await.unwrap();
	let owner = owner_of(&store, "unflushed");
	assert_eq!(
		components(),
		baseline + 1,
		"the branch's memtable must hold a dependency of its own, or this proves nothing"
	);

	store.delete_branch("unflushed").unwrap();
	store.core.inner.sweep_branch_maintenance().unwrap();

	assert!(
		store.core.inner.runtimes.get(owner).is_none(),
		"the reclaimed branch's runtime must be gone"
	);
	assert_eq!(
		components(),
		baseline,
		"the discarded memtable's WAL dependency must be released, not orphaned"
	);

	// The store still works, and the discarded write went with its branch.
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"never-flushed").unwrap(), None);
	assert_eq!(txn.get(b"anchor").unwrap(), Some(b"v".to_vec()));
}

/// A flush whose branch was deleted while its table was being written must
/// abandon the table, not publish it.
///
/// `apply_changeset` recreates a missing level set by design — that is how a new
/// branch's first flush works — so without a liveness check the deleted branch
/// would be silently resurrected and its data published. The check has to sit
/// INSIDE the manifest critical section: the runtime lookup at the top of the
/// flush runs before that lock and cannot close the window.
///
/// The interleaving is built directly and is the real production one: a branch
/// is tombstoned in the catalog while its runtime still exists, which is the
/// state between `delete_branch` and the next maintenance sweep.
#[test(tokio::test)]
async fn a_flush_for_a_deleted_branch_is_refused_not_resurrected() {
	let (store, _temp) = create_store_with(|b| b);

	let mut txn = store.begin().unwrap();
	txn.set(b"anchor", b"v").unwrap();
	txn.commit().await.unwrap();

	let doomed = store.fork_branch("main", "racing", ForkPoint::Head).unwrap();
	let mut txn = doomed.begin().unwrap();
	txn.set(b"racing-key", b"v").unwrap();
	txn.commit().await.unwrap();

	// Seal the branch's memtable so a flush is pending, exactly as the flush
	// loop would find it.
	let owner = owner_of(&store, "racing");
	let runtime = store.core.inner.runtimes.get(owner).unwrap();
	store.core.inner.rotate_runtime_memtable(&runtime, 0).unwrap();
	let pending = runtime.immutable_memtables.read().unwrap().first().cloned().unwrap();

	// Tombstone the branch WITHOUT sweeping: the runtime is still live, so the
	// flush gets past its runtime lookup and reaches the manifest lock — the
	// window the in-lock guard exists for.
	store.delete_branch("racing").unwrap();
	assert!(
		store.core.inner.runtimes.get(owner).is_some(),
		"the fixture must leave the runtime alive, or this tests the wrong check"
	);
	let before = sst_count(&store);

	let outcome = store.core.inner.flush_immutable_to_sst_for_test(
		Arc::clone(&pending.memtable),
		pending.table_id,
		pending.wal_number,
	);
	assert!(
		matches!(outcome, Err(Error::BranchFenced)),
		"a flush for a deleted branch must be refused, got {:?}",
		outcome.map(|table| table.id)
	);
	assert_eq!(sst_count(&store), before, "the abandoned output must not be left behind");
	assert!(
		store.core.inner.level_manifest.read().unwrap().levels_for(owner).is_none(),
		"the deleted branch's level set must not be resurrected by the flush"
	);

	// The store is otherwise healthy, and the sweep still cleans up after it.
	store.core.inner.sweep_branch_maintenance().unwrap();
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"anchor").unwrap(), Some(b"v".to_vec()));
}

/// Scale: churning branches in one process leaves neither tables nor metadata
/// versions growing without bound, and never needs a restart to collect.
#[test(tokio::test)]
async fn churning_branches_stays_bounded_without_a_restart() {
	let (store, _temp) = create_store_with(|b| b);

	let mut txn = store.begin().unwrap();
	txn.set(b"base", b"v").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();
	let baseline = sst_count(&store);

	for round in 0..40u32 {
		let name = format!("sandbox/{round}");
		let handle = store.fork_branch("main", &name, ForkPoint::Head).unwrap();
		let mut txn = handle.begin().unwrap();
		txn.set(format!("k{round}").as_bytes(), b"v").unwrap();
		txn.commit().await.unwrap();
		flush_branch(&store, &name);
		store.delete_branch(&name).unwrap();
		store.core.inner.sweep_branch_maintenance().unwrap();
	}

	assert_eq!(
		sst_count(&store),
		baseline,
		"40 create/write/flush/delete cycles must leave no table behind"
	);
	assert_eq!(
		store.core.inner.branch_catalog.read().unwrap().list().count(),
		1,
		"only main survives"
	);
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"base").unwrap(), Some(b"v".to_vec()));
}

// ===== PB2: detach (materialize) =====

/// Detach copies the inherited view into the branch's own tables and drops the
/// parent link. Reads must be identical before and after — that equality IS the
/// correctness property; everything else about detach is a cost change.
#[test(tokio::test)]
async fn detach_preserves_every_read_and_drops_the_parent_link() {
	let (store, _temp) = create_store_with(|b| b);

	let mut txn = store.begin().unwrap();
	txn.set(b"kept", b"parent-value").unwrap();
	txn.set(b"shadowed", b"parent-value").unwrap();
	txn.set(b"deleted-later", b"parent-value").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();

	let child = store.fork_branch("main", "detachable", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"shadowed", b"child-value").unwrap();
	txn.set(b"child-only", b"child-value").unwrap();
	txn.delete(b"deleted-later").unwrap();
	txn.commit().await.unwrap();

	// Post-fork parent writes: invisible before AND after the detach.
	let mut txn = store.begin().unwrap();
	txn.set(b"kept", b"parent-moved-on").unwrap();
	txn.set(b"after-fork", b"invisible").unwrap();
	txn.commit().await.unwrap();

	let read_all = |handle: &crate::BranchHandle| {
		let txn = handle.begin().unwrap();
		let keys: Vec<&[u8]> =
			vec![b"kept", b"shadowed", b"child-only", b"deleted-later", b"after-fork"];
		keys.into_iter().map(|key| (key.to_vec(), txn.get(key).unwrap())).collect::<Vec<_>>()
	};
	let before = read_all(&child);
	assert_eq!(
		before,
		vec![
			(b"kept".to_vec(), Some(b"parent-value".to_vec())),
			(b"shadowed".to_vec(), Some(b"child-value".to_vec())),
			(b"child-only".to_vec(), Some(b"child-value".to_vec())),
			(b"deleted-later".to_vec(), None),
			(b"after-fork".to_vec(), None),
		],
		"fixture must cover inherit, shadow, own, tombstone and post-fork invisibility"
	);

	let copied = store.detach_branch("detachable").unwrap();
	assert!(copied > 0, "the branch inherited rows, so detach must have copied some");

	assert_eq!(read_all(&child), before, "detach must not change a single read");
	assert_eq!(
		child.info().unwrap().parent,
		None,
		"a detached branch has no lineage left to resolve"
	);
	// The parent is untouched.
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"kept").unwrap(), Some(b"parent-moved-on".to_vec()));
	assert_eq!(txn.get(b"child-only").unwrap(), None);
}

/// Materialized rows are placed below every level the branch occupies. If they
/// landed above, a branch that had compacted its own newer row down a level
/// would start reading the stale inherited one — the read path returns the first
/// table containing the key, level by level, with no sequence comparison across
/// levels.
#[test(tokio::test)]
async fn detach_places_inherited_rows_below_the_branch_s_own_levels() {
	let (store, _temp) = create_store_with(|b| b.with_level_count(4));

	let mut txn = store.begin().unwrap();
	txn.set(b"contested", b"parent-old").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();

	let child = store.fork_branch("main", "layered", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"contested", b"child-new").unwrap();
	txn.commit().await.unwrap();
	flush_branch(&store, "layered");

	// Push the child's own row off L0, so a naive detach would place the
	// inherited row above it.
	let owner = owner_of(&store, "layered");
	{
		let mut manifest = store.core.inner.level_manifest.write().unwrap();
		let levels = manifest.levels_for(owner).unwrap().clone();
		let table = Arc::clone(levels.get_levels()[0].tables.first().unwrap());
		let changeset = crate::levels::ManifestChangeSet {
			owner,
			deleted_tables: std::collections::HashSet::from([(0u8, table.id)]),
			new_tables: vec![(1u8, table)],
			..crate::levels::ManifestChangeSet::default()
		};
		manifest.apply_changeset(&changeset).unwrap();
	}
	let occupied = |level: usize| {
		store
			.core
			.inner
			.level_manifest
			.read()
			.unwrap()
			.levels_for(owner)
			.map(|levels| levels.get_levels()[level].tables.len())
			.unwrap_or(0)
	};
	assert_eq!((occupied(0), occupied(1)), (0, 1), "the child's own row must sit at L1");

	store.detach_branch("layered").unwrap();
	assert_eq!(occupied(2), 1, "the inherited row must land below the child's deepest level");
	assert_eq!(occupied(0), 0, "and never above it");

	let txn = child.begin().unwrap();
	assert_eq!(
		txn.get(b"contested").unwrap(),
		Some(b"child-new".to_vec()),
		"the branch's own row must still win after detach"
	);
}

/// Detach releases the retention pin the fork anchor placed on the parent, so
/// the parent can finally collapse history it was holding on the branch's
/// behalf. Before detaching, the same compaction must keep it — that arm is what
/// makes this a test of detach rather than of compaction.
#[test(tokio::test)]
async fn detach_releases_the_parent_s_retention_pin() {
	let (store, _temp) = create_store_with(|b| b);

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();

	let child = store.fork_branch("main", "pinning", ForkPoint::Head).unwrap();
	let anchor = child.info().unwrap().parent.unwrap().fork_seq;

	let pin = || {
		store
			.core
			.inner
			.branch_catalog
			.read()
			.unwrap()
			.min_active_child_anchor(BatchOwner::DEFAULT.branch, BatchOwner::DEFAULT.generation)
	};
	assert_eq!(pin(), Some(anchor), "the fork must pin the parent while it is attached");

	store.detach_branch("pinning").unwrap();
	assert_eq!(pin(), None, "detach must release the pin");

	// The parent's compaction now runs with a real bottom level again.
	let options = CompactionOptions::for_owner(&store.core.inner, BatchOwner::DEFAULT).unwrap();
	assert!(!options.force_not_bottom, "with no children the parent regains its bottom level");
	assert_eq!(options.history_pin_floor, NO_HISTORY_PIN);
}

/// Detaching a branch with no parent is a no-op, and a branch that occupies
/// every level is refused with an actionable message rather than served a
/// placement that would read incorrectly.
#[test(tokio::test)]
async fn detach_is_a_no_op_without_a_parent_and_refuses_when_it_cannot_place() {
	let (store, _temp) = create_store_with(|b| b.with_level_count(2));

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();

	// main has no parent.
	assert_eq!(store.detach_branch("main").unwrap(), 0);
	// A plain branch has no parent either.
	store.create_branch("plain").unwrap();
	assert_eq!(store.detach_branch("plain").unwrap(), 0);

	// Detaching twice: the second call has nothing left to do.
	let child = store.fork_branch("main", "twice", ForkPoint::Head).unwrap();
	assert!(store.detach_branch("twice").unwrap() > 0);
	assert_eq!(store.detach_branch("twice").unwrap(), 0, "a detached branch stays detached");
	let txn = child.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"v".to_vec()));
	drop(txn);

	// A branch occupying every level cannot be placed below itself. Two
	// distinct tables are needed for that: moving one table into a second level
	// while leaving it in the first would be corrupt state, not a crowded
	// branch, and the state manifest rejects a table listed twice.
	let crowded = store.fork_branch("main", "crowded", ForkPoint::Head).unwrap();
	for round in 0..2u32 {
		let mut txn = crowded.begin().unwrap();
		txn.set(format!("own{round}").as_bytes(), b"v").unwrap();
		txn.commit().await.unwrap();
		flush_branch(&store, "crowded");
	}
	let owner = owner_of(&store, "crowded");
	{
		let mut manifest = store.core.inner.level_manifest.write().unwrap();
		let levels = manifest.levels_for(owner).unwrap().clone();
		assert_eq!(levels.get_levels()[0].tables.len(), 2, "fixture must build two L0 tables");
		let moved = Arc::clone(&levels.get_levels()[0].tables[0]);
		let changeset = crate::levels::ManifestChangeSet {
			owner,
			deleted_tables: std::collections::HashSet::from([(0u8, moved.id)]),
			new_tables: vec![(1u8, moved)],
			..crate::levels::ManifestChangeSet::default()
		};
		manifest.apply_changeset(&changeset).unwrap();
		// Publishing proves the fixture state is legal, not just in-memory.
		manifest.persist_owner_update(owner).unwrap();
		let levels = manifest.levels_for(owner).unwrap();
		assert_eq!(
			(levels.get_levels()[0].tables.len(), levels.get_levels()[1].tables.len()),
			(1, 1),
			"the branch must legitimately occupy every level"
		);
	}
	let error = store.detach_branch("crowded").expect_err("no level is free below the branch");
	assert!(error.to_string().contains("compact it before detaching"), "{error}");
	assert!(
		store.branch("crowded").unwrap().info().unwrap().parent.is_some(),
		"a refused detach must leave the lineage intact"
	);
}

/// A detached branch survives a reopen on its own tables, with no ancestor to
/// resolve through — the point of materializing in the first place.
#[test(tokio::test)]
async fn a_detached_branch_reopens_without_its_ancestor() {
	let temp_dir = TempDir::new("fork-view").unwrap();
	let path = temp_dir.path().to_path_buf();

	{
		let store = TreeBuilder::new().with_path(path.clone()).build().unwrap();
		let mut txn = store.begin().unwrap();
		txn.set(b"inherited", b"from-parent").unwrap();
		txn.commit().await.unwrap();
		store.flush().unwrap();
		let child = store.fork_branch("main", "standalone", ForkPoint::Head).unwrap();
		let mut txn = child.begin().unwrap();
		txn.set(b"own", b"from-child").unwrap();
		txn.commit().await.unwrap();
		assert!(store.detach_branch("standalone").unwrap() > 0);
		store.close().await.unwrap();
	}

	let store = TreeBuilder::new().with_path(path).build().unwrap();
	let child = store.branch("standalone").unwrap();
	assert_eq!(child.info().unwrap().parent, None, "the branch is still detached after reopen");
	let txn = child.begin().unwrap();
	assert_eq!(txn.get(b"inherited").unwrap(), Some(b"from-parent".to_vec()));
	assert_eq!(txn.get(b"own").unwrap(), Some(b"from-child".to_vec()));
	drop(txn);
	store.close().await.unwrap();
}

/// The crash window between detach's two publishes, made observable.
///
/// Detach lands the branch's state first and its catalog entry second. If the
/// process dies in between, the branch holds BOTH the materialized tables and
/// its parent link — so every inherited row is present twice, at identical
/// sequences with identical values. This asserts that state is harmless, which
/// is the whole reason for that ordering: the opposite order would clear the
/// lineage while the data was still only in the ancestor, losing it.
#[test(tokio::test)]
async fn detach_interrupted_between_its_publishes_reads_correctly() {
	let (store, _temp) = create_store_with(|b| b);

	let mut txn = store.begin().unwrap();
	txn.set(b"inherited", b"parent-value").unwrap();
	txn.set(b"shadowed", b"parent-value").unwrap();
	txn.set(b"gone", b"parent-value").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();

	let child = store.fork_branch("main", "interrupted", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"shadowed", b"child-value").unwrap();
	txn.delete(b"gone").unwrap();
	txn.commit().await.unwrap();

	let before: Vec<_> = {
		let txn = child.begin().unwrap();
		[b"inherited".as_slice(), b"shadowed".as_slice(), b"gone".as_slice()]
			.into_iter()
			.map(|key| txn.get(key).unwrap())
			.collect()
	};

	// Exactly the crash state: the state half lands, the catalog half does not.
	let owner = owner_of(&store, "interrupted");
	let copied = store.core.inner.materialize_inherited(&store.core, owner).unwrap();
	assert!(copied > 0, "the fixture must actually materialize rows");
	assert!(
		child.info().unwrap().parent.is_some(),
		"this must leave the parent link in place, or it is not the crash window"
	);

	let after: Vec<_> = {
		let txn = child.begin().unwrap();
		[b"inherited".as_slice(), b"shadowed".as_slice(), b"gone".as_slice()]
			.into_iter()
			.map(|key| txn.get(key).unwrap())
			.collect()
	};
	assert_eq!(after, before, "duplicated inherited rows must not change a single read");

	// And the interrupted detach can simply be retried to completion.
	store.detach_branch("interrupted").unwrap();
	assert_eq!(child.info().unwrap().parent, None);
	let txn = child.begin().unwrap();
	assert_eq!(txn.get(b"inherited").unwrap(), Some(b"parent-value".to_vec()));
	assert_eq!(txn.get(b"shadowed").unwrap(), Some(b"child-value".to_vec()));
	assert_eq!(txn.get(b"gone").unwrap(), None);
}
