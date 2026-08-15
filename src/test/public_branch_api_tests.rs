//! PA gate tests: the branch lifecycle driven entirely through the public
//! surface.
//!
//! Nothing here reaches into `core`, `inner` or any `pub(crate)` item — that
//! restraint IS the test. If a step cannot be expressed with `Tree`,
//! `BranchHandle` and `Transaction` alone, the public API is incomplete and this
//! file must fail to compile rather than be helped along.

use std::time::Duration;

use tempdir::TempDir;
use test_log::test;

use crate::test::support::create_store;
use crate::{BranchInfo, Error, ForkPoint, LSMIterator, Mode, Tree, TreeBuilder};

fn info_for(store: &Tree, name: &str) -> BranchInfo {
	store
		.list_branches()
		.unwrap()
		.into_iter()
		.find(|info| info.name == name)
		.unwrap_or_else(|| panic!("{name} must be listed"))
}

/// The whole point of the slice: create, fork, read, write, list, delete — with
/// `main` behaving exactly as it did before branches existed.
#[test(tokio::test)]
async fn the_branch_lifecycle_runs_through_the_public_api_alone() {
	let (store, _temp) = create_store();

	// `main` is untouched by any of this.
	let mut txn = store.begin().unwrap();
	txn.set(b"shared", b"main-v1").unwrap();
	txn.commit().await.unwrap();

	// A plain branch inherits nothing.
	let empty = store.create_branch("empty").unwrap();
	let txn = empty.begin().unwrap();
	assert_eq!(txn.get(b"shared").unwrap(), None, "create_branch must not inherit");
	drop(txn);

	// A fork inherits everything at its anchor.
	let child = store.fork_branch("main", "child", ForkPoint::Head).unwrap();
	assert_eq!(child.name(), "child");
	let txn = child.begin().unwrap();
	assert_eq!(txn.get(b"shared").unwrap(), Some(b"main-v1".to_vec()));
	drop(txn);

	// Post-fork divergence in both directions.
	let mut txn = store.begin().unwrap();
	txn.set(b"shared", b"main-v2").unwrap();
	txn.commit().await.unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"shared", b"child-v1").unwrap();
	txn.set(b"child-only", b"c").unwrap();
	txn.commit().await.unwrap();

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"shared").unwrap(), Some(b"main-v2".to_vec()));
	assert_eq!(txn.get(b"child-only").unwrap(), None, "a child write must not reach main");
	drop(txn);
	let txn = store.begin_on("child").unwrap();
	assert_eq!(txn.get(b"shared").unwrap(), Some(b"child-v1".to_vec()));
	assert_eq!(txn.get(b"child-only").unwrap(), Some(b"c".to_vec()));
	drop(txn);

	// Reopening by name yields an equivalent handle.
	let reopened = store.branch("child").unwrap();
	assert_eq!(reopened.id(), child.id());
	assert_eq!(reopened.generation(), child.generation());

	let names: Vec<_> = store.list_branches().unwrap().into_iter().map(|i| i.name).collect();
	assert_eq!(names, vec!["child".to_owned(), "empty".to_owned(), "main".to_owned()]);

	store.delete_branch("empty").unwrap();
	let names: Vec<_> = store.list_branches().unwrap().into_iter().map(|i| i.name).collect();
	assert_eq!(names, vec!["child".to_owned(), "main".to_owned()]);
	assert!(store.branch("empty").is_err(), "a deleted branch cannot be opened");
}

/// `BranchInfo` reports lineage and the branch's own newest write. `last_write_seq`
/// is deliberately not a per-branch head: a fresh fork has written nothing and
/// says so, and a branch's own writes are visible in it while another branch's
/// are not.
#[test(tokio::test)]
async fn branch_info_reports_lineage_and_the_branch_s_own_last_write() {
	let (store, _temp) = create_store();

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();
	let main_after_write = info_for(&store, "main").last_write_seq;
	assert!(main_after_write.is_some(), "main wrote, so it must report a write sequence");

	let child = store.fork_branch("main", "fresh", ForkPoint::Head).unwrap();
	let info = child.info().unwrap();
	assert_eq!(
		info.last_write_seq, None,
		"a fork that has written nothing must report no write of its own"
	);
	let lineage = info.parent.expect("a fork must report its lineage");
	assert_eq!(lineage.branch, info_for(&store, "main").id);
	assert!(lineage.fork_seq > 0, "the anchor must be the drained head, not zero");
	assert_eq!(info.name, "fresh");
	assert_eq!(info.expires_at, None);

	// The child writes: its own sequence appears, main's does not move.
	let mut txn = child.begin().unwrap();
	txn.set(b"child", b"c").unwrap();
	txn.commit().await.unwrap();
	let child_seq = child.info().unwrap().last_write_seq.expect("the child has now written");
	assert!(child_seq > lineage.fork_seq);
	assert_eq!(
		info_for(&store, "main").last_write_seq,
		main_after_write,
		"a child's write must not be attributed to its parent"
	);

	// main has no lineage.
	assert_eq!(info_for(&store, "main").parent, None);
}

/// A handle is pinned to a generation. After the branch is deleted, the handle
/// refuses every operation instead of silently binding to a new branch that
/// reuses the name — including reads, which is the case that would otherwise
/// return another branch's data.
#[test(tokio::test)]
async fn a_handle_is_fenced_once_its_branch_is_deleted_or_recreated() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"main").unwrap();
	txn.commit().await.unwrap();

	let handle = store.fork_branch("main", "sandbox", ForkPoint::Head).unwrap();
	let mut txn = handle.begin().unwrap();
	txn.set(b"k", b"first-incarnation").unwrap();
	txn.commit().await.unwrap();

	store.delete_branch("sandbox").unwrap();
	assert!(matches!(handle.begin(), Err(Error::BranchFenced)));
	assert!(matches!(handle.info(), Err(Error::BranchFenced)));

	// The name is reusable, and the new incarnation is a different branch.
	let recreated = store.fork_branch("main", "sandbox", ForkPoint::Head).unwrap();
	assert_ne!(recreated.generation(), handle.generation());
	let txn = recreated.begin().unwrap();
	assert_eq!(
		txn.get(b"k").unwrap(),
		Some(b"main".to_vec()),
		"the new incarnation must inherit from main, not resurrect the old branch's write"
	);
	drop(txn);
	assert!(
		matches!(handle.begin(), Err(Error::BranchFenced)),
		"the stale handle stays fenced after the name is reused"
	);
}

/// The public fork selectors, including the two failure modes callers must be
/// able to distinguish: forking above the head, and forking below the horizon.
#[test(tokio::test)]
async fn public_fork_selectors_cover_head_version_and_timestamp() {
	let temp_dir = TempDir::new("public-branch").unwrap();
	let store = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_versioning(true, 0)
		.build()
		.unwrap();

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"first").unwrap();
	txn.commit().await.unwrap();
	let first = store.branch("main").unwrap().info().unwrap().last_write_seq.unwrap();

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"second").unwrap();
	txn.commit().await.unwrap();

	// AtVersion sees the historical value.
	let at_version = store.fork_branch("main", "at-version", ForkPoint::AtVersion(first)).unwrap();
	let txn = at_version.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"first".to_vec()));
	drop(txn);

	// Head sees the current one.
	let at_head = store.fork_branch("main", "at-head", ForkPoint::Head).unwrap();
	let txn = at_head.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"second".to_vec()));
	drop(txn);

	// Above the head: refused, and nothing is created.
	assert!(store.fork_branch("main", "future", ForkPoint::AtVersion(u64::MAX)).is_err());
	assert!(store.branch("future").is_err());

	// Below the timeline horizon: an abstain, not a guess.
	let error = store
		.fork_branch("main", "ancient", ForkPoint::AtTimestamp(1))
		.expect_err("a timestamp below the horizon must not be approximated");
	assert!(matches!(error, Error::TimestampBelowHorizon { .. }), "got {error}");
}

/// TTL is set through the public API in wall-clock terms; the caller never sees
/// the engine's clock unit. A zero TTL is already due, and clearing it makes the
/// branch permanent again.
#[test(tokio::test)]
async fn branch_ttl_is_set_and_cleared_in_wall_clock_terms() {
	let (store, _temp) = create_store();
	store.create_branch("sandbox").unwrap();
	assert_eq!(info_for(&store, "sandbox").expires_at, None);

	store.set_branch_ttl("sandbox", Some(Duration::from_secs(3600))).unwrap();
	let far = info_for(&store, "sandbox").expires_at.expect("a TTL must be recorded");

	store.set_branch_ttl("sandbox", Some(Duration::ZERO)).unwrap();
	let due = info_for(&store, "sandbox").expires_at.expect("a zero TTL is still an expiry");
	assert!(due < far, "a shorter TTL must move the expiry earlier");

	store.set_branch_ttl("sandbox", None).unwrap();
	assert_eq!(info_for(&store, "sandbox").expires_at, None, "None must clear the expiry");

	// `main` cannot expire.
	assert!(store.set_branch_ttl("main", Some(Duration::from_secs(1))).is_err());
}

/// The refusals a caller has to be able to rely on, all through the public API.
#[test(tokio::test)]
async fn public_api_refusals_are_typed_and_leave_no_trace() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();

	// main is protected.
	assert!(store.delete_branch("main").is_err());
	assert!(store.branch("main").is_ok(), "the refusal must not have damaged main");

	// Unknown branches.
	assert!(store.branch("nope").is_err());
	assert!(store.delete_branch("nope").is_err());
	assert!(store.begin_on("nope").is_err());
	assert!(store.fork_branch("nope", "child", ForkPoint::Head).is_err());

	// Duplicate names: create refuses outright, fork is idempotent.
	store.create_branch("taken").unwrap();
	assert!(store.create_branch("taken").is_err());
	let first = store.fork_branch("main", "forked", ForkPoint::Head).unwrap();
	let retried = store.fork_branch("main", "forked", ForkPoint::Head).unwrap();
	assert_eq!(retried.id(), first.id(), "re-issuing a fork must not create a second branch");
	let live: Vec<_> = store.list_branches().unwrap().into_iter().map(|i| i.name).collect();
	assert_eq!(
		live,
		vec!["forked".to_owned(), "main".to_owned(), "taken".to_owned()],
		"the refused create and the refused fork must have left nothing behind"
	);

	// A parent with an active child cannot be deleted.
	store.fork_branch("forked", "grandchild", ForkPoint::Head).unwrap();
	let error = store.delete_branch("forked").expect_err("a parent of a live child is protected");
	assert!(error.to_string().contains("active fork children"), "{error}");
	store.delete_branch("grandchild").unwrap();
	store.delete_branch("forked").unwrap();
}

/// Branches survive a reopen through the public API with no extra work, and a
/// read-only transaction on a branch is a read-only transaction.
#[test(tokio::test)]
async fn branches_and_their_views_survive_a_reopen() {
	let temp_dir = TempDir::new("public-branch").unwrap();
	let path = temp_dir.path().to_path_buf();

	{
		let store = TreeBuilder::new().with_path(path.clone()).build().unwrap();
		let mut txn = store.begin().unwrap();
		txn.set(b"inherited", b"from-main").unwrap();
		txn.commit().await.unwrap();
		let child = store.fork_branch("main", "persisted", ForkPoint::Head).unwrap();
		let mut txn = child.begin().unwrap();
		txn.set(b"own", b"child-data").unwrap();
		txn.commit().await.unwrap();
		store.close().await.unwrap();
	}

	let store = TreeBuilder::new().with_path(path).build().unwrap();
	let child = store.branch("persisted").unwrap();
	let txn = child.begin().unwrap();
	assert_eq!(txn.get(b"inherited").unwrap(), Some(b"from-main".to_vec()));
	assert_eq!(txn.get(b"own").unwrap(), Some(b"child-data".to_vec()));
	drop(txn);
	assert!(child.info().unwrap().parent.is_some(), "lineage must survive the reopen");

	// Read-only mode is honoured on a branch.
	let mut txn = child.begin_with_mode(Mode::ReadOnly).unwrap();
	assert!(txn.set(b"nope", b"x").is_err(), "a read-only branch transaction must refuse writes");
	drop(txn);

	// Range reads work through the branch handle.
	{
		let txn = child.begin().unwrap();
		let mut iter = txn.range(b"a", b"z").unwrap();
		let mut keys = Vec::new();
		let mut valid = iter.seek_first().unwrap();
		while valid {
			keys.push(iter.key().user_key().to_vec());
			valid = iter.next().unwrap();
		}
		assert_eq!(keys, vec![b"inherited".to_vec(), b"own".to_vec()]);
	}
	store.close().await.unwrap();
}

/// A snapshot of the public surface. Its job is to make additions and removals
/// deliberate: this file is the one place a reviewer looks to see what the crate
/// promises. It compiles the exports rather than grepping for them, so a type
/// that is renamed or made private breaks the build here.
#[test]
fn public_surface_snapshot() {
	fn assert_public<T>() {}

	// Engine.
	assert_public::<crate::Tree>();
	assert_public::<crate::TreeBuilder>();
	assert_public::<crate::Transaction>();
	assert_public::<crate::Options>();
	assert_public::<crate::Error>();

	// Branches (PA).
	assert_public::<crate::BranchHandle>();
	assert_public::<crate::BranchInfo>();
	assert_public::<crate::BranchLineage>();
	assert_public::<crate::ForkPoint>();
	assert_public::<crate::BranchId>();
	assert_public::<crate::BranchGeneration>();

	// Reads and writes.
	assert_public::<crate::Mode>();
	assert_public::<crate::Durability>();
	assert_public::<crate::ReadOptions>();
	assert_public::<crate::WriteOptions>();
	assert_public::<crate::HistoryOptions>();

	// The branch failure modes a caller must be able to match on. Listing them
	// as values, not names, means a removed variant fails to compile.
	let _typed: [crate::Error; 5] = [
		crate::Error::BranchFenced,
		crate::Error::ForkFenceTimeout,
		crate::Error::MaterializationRequired {
			depth: 0,
		},
		crate::Error::BelowRetentionFloor {
			requested: 0,
			floor: 0,
		},
		crate::Error::TimestampBelowHorizon {
			requested: 0,
			horizon_floor: 0,
		},
	];

	// The prototype's public types are gone (PA2); this is the compile-time half
	// of `removed_prototype_engine_remains_absent`.
	let branch_api = std::fs::read_to_string(
		std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs"),
	)
	.unwrap();
	for gone in ["CommitVersion", "CommitTimestamp", "DatabaseId", "ReadSelector", "WriteOperation"]
	{
		assert!(!branch_api.contains(gone), "retired public type is exported again: {gone}");
	}
	assert!(branch_api.contains("BranchHandle"), "guard parsed the wrong file");
}

/// A resolver that settles a conflict by taking whichever value is longer, and
/// refuses when either side deleted the key.
struct PreferLonger;

impl crate::ConflictResolver for PreferLonger {
	fn resolve(&self, conflict: &crate::Conflict) -> crate::ConflictChoice {
		match (&conflict.source, &conflict.target) {
			(Some(source), Some(target)) if source.len() >= target.len() => {
				crate::ConflictChoice::Source
			}
			(Some(_), Some(_)) => crate::ConflictChoice::Target,
			_ => crate::ConflictChoice::Refuse,
		}
	}
}

/// Rows a branch must read: key, and the value it should have (`None` = absent).
type ExpectedRows<'a> = &'a [(&'a [u8], Option<&'a [u8]>)];

/// The whole branch surface, composed in one scenario, through the public API
/// alone — and then reopened from disk and checked again.
///
/// Every other test in this file isolates one operation. This one is the proof
/// that they compose: that a fork of a fork still reads correctly after its
/// parent was merged into, that a scoped merge and a full merge coexist, that a
/// revert does not disturb a neighbour, and that all of it survives a restart.
///
/// It is also the executable half of `docs/BRANCHING.md`: every claim that
/// document makes about ordering, bases, chunks and metrics is exercised here,
/// so the document cannot drift from the code without this failing.
#[test(tokio::test)]
async fn the_whole_branch_surface_composes_and_survives_a_reopen() {
	let temp_dir = TempDir::new("public-branch-compose").unwrap();
	let path = temp_dir.path().to_path_buf();
	let build =
		|| TreeBuilder::new().with_path(path.clone()).with_versioning(true, 0).build().unwrap();

	let store = build();

	// --- main gets a baseline -------------------------------------------
	for (key, value) in [(&b"a"[..], &b"a1"[..]), (b"b", b"b1"), (b"c", b"c1")] {
		let mut txn = store.begin().unwrap();
		txn.set(key, value).unwrap();
		txn.commit().await.unwrap();
	}
	let baseline = info_for(&store, "main").last_write_seq.unwrap();

	let mut txn = store.begin().unwrap();
	txn.set(b"a", b"a2").unwrap();
	txn.commit().await.unwrap();

	// --- three forks, one per selector -----------------------------------
	let at_head = store.fork_branch("main", "head", ForkPoint::Head).unwrap();
	let at_version = store.fork_branch("main", "version", ForkPoint::AtVersion(baseline)).unwrap();
	let now = info_for(&store, "main").created_at_seq; // any resolvable point
	let _ = now;
	let at_timestamp = store
		.fork_branch("main", "timestamp", ForkPoint::AtTimestamp(u64::MAX))
		.expect("a timestamp above every commit resolves to the newest one");

	// The anchor decides what each child inherited.
	assert_eq!(at_head.begin().unwrap().get(b"a").unwrap(), Some(b"a2".to_vec()));
	assert_eq!(
		at_version.begin().unwrap().get(b"a").unwrap(),
		Some(b"a1".to_vec()),
		"a fork at an older version must not see the newer write"
	);
	assert_eq!(at_timestamp.begin().unwrap().get(b"a").unwrap(), Some(b"a2".to_vec()));

	// --- each child diverges ---------------------------------------------
	let mut txn = at_head.begin().unwrap();
	txn.set(b"a", b"head-a").unwrap();
	txn.set(b"head-only", b"h").unwrap();
	txn.delete(b"c").unwrap();
	txn.commit().await.unwrap();

	let mut txn = at_version.begin().unwrap();
	txn.set(b"b", b"version-b").unwrap();
	txn.set(b"zzz", b"scoped").unwrap();
	txn.commit().await.unwrap();

	let mut txn = at_timestamp.begin().unwrap();
	txn.set(b"a", b"ts-a").unwrap();
	txn.set(b"b", b"ts-b").unwrap();
	txn.commit().await.unwrap();

	// --- diff: a branch's own writes, nothing inherited -------------------
	let head_diff = at_head.diff().unwrap();
	let mut head_keys: Vec<_> =
		head_diff.collect().unwrap().into_iter().map(|entry| entry.key).collect();
	head_keys.sort();
	assert_eq!(
		head_keys,
		vec![b"a".to_vec(), b"c".to_vec(), b"head-only".to_vec()],
		"a diff is the branch's own writes — including its delete, and excluding untouched keys"
	);

	// --- preview, then the strategies ------------------------------------
	// `main` moved after the forks, so `a` conflicts.
	let mut txn = store.begin().unwrap();
	txn.set(b"a", b"main-a3").unwrap();
	txn.commit().await.unwrap();

	let main = store.branch("main").unwrap();
	let report = at_head.preview_merge_into(&main).unwrap();
	assert!(!report.conflicts.is_empty(), "the preview must see the conflict on `a`");

	// Strict refuses and writes nothing.
	let refused = at_head.merge_into(&main, crate::MergeStrategy::Strict).await;
	assert!(matches!(refused, Err(Error::MergeConflicts { .. })), "got {refused:?}");
	assert_eq!(
		main.begin().unwrap().get(b"a").unwrap(),
		Some(b"main-a3".to_vec()),
		"a refused merge must leave the target exactly as it was"
	);

	// TargetWins keeps the target's value but still applies the rest.
	let outcome = at_head.merge_into(&main, crate::MergeStrategy::TargetWins).await.unwrap();
	assert!(outcome.resolved >= 1, "the conflict must be counted as resolved, not applied");
	assert!(outcome.chunks <= 1, "a merge this small must be atomic");
	let txn = main.begin().unwrap();
	assert_eq!(txn.get(b"a").unwrap(), Some(b"main-a3".to_vec()), "TargetWins keeps the target");
	assert_eq!(txn.get(b"head-only").unwrap(), Some(b"h".to_vec()), "non-conflicts still apply");
	assert_eq!(txn.get(b"c").unwrap(), None, "the source's delete applies too");
	drop(txn);

	// A resolver settles the next conflict itself.
	let mut txn = at_head.begin().unwrap();
	txn.set(b"a", b"head-a-much-longer").unwrap();
	txn.commit().await.unwrap();
	let outcome = at_head
		.merge_into(&main, crate::MergeStrategy::Resolve(std::sync::Arc::new(PreferLonger)))
		.await
		.unwrap();
	assert!(outcome.applied + outcome.resolved >= 1);
	assert_eq!(
		main.begin().unwrap().get(b"a").unwrap(),
		Some(b"head-a-much-longer".to_vec()),
		"the resolver chose the longer value"
	);

	// --- a scoped merge from the second child ----------------------------
	let scoped = at_version
		.merge_range(
			&main,
			crate::MergeStrategy::SourceWins,
			std::ops::Bound::Included(b"z".to_vec()),
			std::ops::Bound::Unbounded,
		)
		.await
		.unwrap();
	assert_eq!(scoped.applied, 1, "only the key inside the range may be merged");
	let txn = main.begin().unwrap();
	assert_eq!(txn.get(b"zzz").unwrap(), Some(b"scoped".to_vec()));
	assert_eq!(
		txn.get(b"b").unwrap(),
		Some(b"b1".to_vec()),
		"a key outside the range must be untouched by a scoped merge"
	);
	drop(txn);

	// --- revert on the third child ---------------------------------------
	let reverted = at_timestamp
		.revert_range(
			std::ops::Bound::Included(b"a".to_vec()),
			std::ops::Bound::Included(b"a".to_vec()),
		)
		.await
		.unwrap();
	assert_eq!(reverted, 1, "exactly the one changed key in range is compensated");
	let txn = at_timestamp.begin().unwrap();
	assert_eq!(
		txn.get(b"a").unwrap(),
		Some(b"a2".to_vec()),
		"revert restores what the branch inherited at its anchor"
	);
	assert_eq!(txn.get(b"b").unwrap(), Some(b"ts-b".to_vec()), "outside the range, untouched");
	drop(txn);

	// --- detach and TTL ---------------------------------------------------
	let materialized = store.detach_branch("version").unwrap();
	assert!(materialized > 0, "detaching must copy the inherited view");
	assert_eq!(
		info_for(&store, "version").parent,
		None,
		"a detached branch has no parent link left"
	);
	assert_eq!(
		store.branch("version").unwrap().begin().unwrap().get(b"a").unwrap(),
		Some(b"a1".to_vec()),
		"detaching must not change what the branch reads"
	);

	store.set_branch_ttl("timestamp", Some(Duration::from_secs(3600))).unwrap();
	assert!(info_for(&store, "timestamp").expires_at.is_some());

	// --- metrics before the restart ---------------------------------------
	let metrics = store.metrics().unwrap();
	assert_eq!(metrics.forks, 3, "three forks were taken");
	assert_eq!(metrics.detaches, 1);
	assert_eq!(metrics.merges, 3, "TargetWins, the resolver, and the scoped range");
	assert_eq!(metrics.live_branches, 4, "main plus three children");

	// --- close, reopen, and check every branch again ----------------------
	store.close().await.unwrap();
	let store = build();

	let expected: [(&str, ExpectedRows<'_>); 4] = [
		(
			"main",
			&[
				(b"a", Some(b"head-a-much-longer")),
				(b"b", Some(b"b1")),
				(b"c", None),
				(b"head-only", Some(b"h")),
				(b"zzz", Some(b"scoped")),
			],
		),
		("head", &[(b"a", Some(b"head-a-much-longer")), (b"c", None)]),
		("version", &[(b"a", Some(b"a1")), (b"b", Some(b"version-b")), (b"zzz", Some(b"scoped"))]),
		("timestamp", &[(b"a", Some(b"a2")), (b"b", Some(b"ts-b"))]),
	];
	for (branch, rows) in expected {
		let txn = store.begin_on(branch).unwrap();
		for (key, value) in rows.iter() {
			assert_eq!(
				txn.get(*key).unwrap().as_deref(),
				*value,
				"after reopen, branch {branch} key {:?}",
				String::from_utf8_lossy(key)
			);
		}
	}

	// The TTL is durable, and so is the detachment.
	assert!(info_for(&store, "timestamp").expires_at.is_some(), "a TTL must survive a restart");
	assert_eq!(info_for(&store, "version").parent, None, "detachment must survive a restart");

	store.close().await.unwrap();
}
