//! Shared test support.
//!
//! A helper redefined in three files is three helpers that can disagree. This
//! module is where the shapes every branch test needs live, so there is one
//! definition to read and one to change.
//!
//! Two rules for what belongs here:
//!
//! 1. **A name means one thing.** If two files want the same word for different questions, that is
//!    two names, not one helper with a footnote.
//! 2. **Prefer the public API.** A helper that reaches `store.core.inner` makes every test using it
//!    depend on an internal field name. Where internals are genuinely needed, the reach is confined
//!    here rather than spread across the test files.

use std::sync::Arc;

use tempdir::TempDir;

use crate::vfs::File;
use crate::Tree;

/// A temporary directory for a store, removed when the returned handle drops.
///
/// Every test file used to define its own; they were byte-identical apart from
/// the prefix, which nothing reads. Hold the returned value for as long as the
/// store lives — dropping it deletes the directory out from under an open
/// store.
pub(crate) fn create_temp_directory() -> TempDir {
	TempDir::new("surrealkv-test").unwrap()
}

/// Presents an in-memory buffer as a [`File`], for tests that build an SSTable
/// without touching a disk.
///
/// `Vec<u8>` implements [`File`] already; this exists so the coercion to
/// `Arc<dyn File>` reads as an intent rather than as a turbofish at every call
/// site.
pub(crate) fn wrap_buffer(src: Vec<u8>) -> Arc<dyn File> {
	Arc::new(src)
}

/// A store on a fresh temporary directory, with default options.
///
/// The `TempDir` comes back with it and must be held for the store's lifetime:
/// dropping it removes the directory the store is writing into.
pub(crate) fn create_store() -> (Tree, TempDir) {
	create_store_with(|builder| builder)
}

/// A store whose builder the caller adjusts first.
///
/// The closure takes and returns the [`TreeBuilder`] rather than an `Options`,
/// so a test configures a store the same way a user would.
pub(crate) fn create_store_with<F>(configure: F) -> (Tree, TempDir)
where
	F: FnOnce(crate::TreeBuilder) -> crate::TreeBuilder,
{
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();
	let tree = configure(crate::TreeBuilder::new().with_path(path)).build().unwrap();
	(tree, temp_dir)
}

/// How long [`wait_until`] keeps polling before giving up. Generous on purpose:
/// it is a deadlock backstop, not a timing assertion. A test that needs a
/// *short* deadline to be meaningful is measuring the machine.
const WAIT_UNTIL_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

/// How often [`wait_until`] re-checks. Short enough that a satisfied condition
/// costs nothing noticeable, long enough not to spin a core.
const WAIT_UNTIL_POLL: std::time::Duration = std::time::Duration::from_millis(1);

/// Polls `condition` until it holds, returning whether it ever did.
///
/// This is the replacement for `sleep(n)` followed by an assertion. A sleep
/// encodes a guess about how fast the machine is: too short and the test fails
/// on a loaded box, too long and it wastes wall-clock on every run. A poll
/// encodes what the test is actually waiting for, so it gets *slower* rather
/// than *wrong* under load.
///
/// It cannot be used to assert a negative — "this did not happen" needs a
/// bounded wait plus a positive control proving the machinery was running at
/// all. See `test_no_spurious_small_flush` for that shape.
pub(crate) async fn wait_until(mut condition: impl FnMut() -> bool) -> bool {
	let deadline = std::time::Instant::now() + WAIT_UNTIL_DEADLINE;
	while std::time::Instant::now() < deadline {
		if condition() {
			return true;
		}
		tokio::time::sleep(WAIT_UNTIL_POLL).await;
	}
	condition()
}

/// Whether a live branch of this name exists.
///
/// "Live" means not tombstoned: [`Tree::branch`] resolves through the catalog's
/// live-name index, which a delete removes from, so a deleted branch answers
/// `false` from the moment its tombstone is applied.
///
/// This was two functions until 2026-08-15 — one in `fault_injection_tests.rs`
/// asking `list_branches()`, one in `fork_view_tests.rs` reaching into
/// `core.inner.branch_catalog` — under one name. They happened to agree, because
/// `list()` and `get_by_name` are both driven by the same `live_names` index,
/// but nothing said so, and a test moved between the files would have changed
/// which layer it exercised without changing a line. The `list_branches` form
/// was also the worse of the two in the file that used it: it builds a
/// `BranchInfo` for *every* branch, which walks the level manifest, so in a
/// fault-injection test an unrelated failure turned a clean `false` into a
/// panic.
pub(crate) fn branch_exists(store: &Tree, name: &str) -> bool {
	store.branch(name).is_ok()
}
