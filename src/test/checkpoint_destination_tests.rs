//! Where a checkpoint may be written.
//!
//! A checkpoint replaces the files of an earlier one in its destination, so a destination that is
//! the database directory, inside it or around it, however it is spelled, would be written over
//! the live files. Such a destination is refused before anything is flushed, created, deleted or
//! modified: the database directory is byte-identical afterwards and the tree carries on. Every
//! other destination keeps working, including a directory that already holds an earlier
//! checkpoint.

use std::collections::BTreeMap;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tempdir::TempDir;
use test_log::test;

use super::collect_transaction_all;
use crate::{Error, Mode, Options, Tree};

/// Large enough that nothing rotates unless a test rotates it.
const NEVER_ROTATING: usize = 16 * 1024 * 1024;

/// A close that does not finish within this is a failure, not a hang.
const LONG: Duration = Duration::from_secs(30);

type Contents = BTreeMap<Vec<u8>, Vec<u8>>;

async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
	match tokio::time::timeout(LONG, future).await {
		Ok(value) => value,
		Err(_) => panic!("{what} did not finish within {LONG:?}"),
	}
}

fn key_of(i: usize) -> Vec<u8> {
	format!("key_{i:06}").into_bytes()
}

fn val_of(i: usize) -> Vec<u8> {
	// Every seventh value is large enough to be separated into the value log when it is on.
	let len = if i % 7 == 0 {
		3000
	} else {
		100
	};
	let mut v = format!("val_{i:06}_").into_bytes();
	v.resize(len, b'x');
	v
}

fn open(path: &Path, vlog: bool) -> Arc<Tree> {
	Arc::new(
		Tree::new(Arc::new(Options {
			path: path.to_path_buf(),
			max_memtable_size: NEVER_ROTATING,
			enable_vlog: vlog,
			..Default::default()
		}))
		.unwrap(),
	)
}

/// Commits the keys in `range`, 25 to a transaction, and returns what was written.
async fn put(tree: &Tree, range: Range<usize>) -> Contents {
	let mut written = Contents::new();
	let keys: Vec<usize> = range.collect();
	for chunk in keys.chunks(25) {
		let mut txn = tree.begin().unwrap();
		for &i in chunk {
			txn.set(key_of(i), val_of(i)).unwrap();
			written.insert(key_of(i), val_of(i));
		}
		txn.commit().await.unwrap();
	}
	written
}

fn contents(tree: &Tree) -> Contents {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let all = collect_transaction_all(&mut txn.iter().unwrap()).unwrap();
	all.into_iter().collect()
}

fn assert_contents(tree: &Tree, expected: &Contents, what: &str) {
	let actual = contents(tree);
	assert_eq!(actual.len(), expected.len(), "{what}: number of keys");
	for (key, value) in expected {
		assert_eq!(actual.get(key), Some(value), "{what}: {}", String::from_utf8_lossy(key));
	}
}

/// What a path under `root` holds: a directory, a link to a target, or a file by size and a hash
/// of its bytes.
#[derive(Debug, PartialEq, Eq)]
enum Entry {
	Dir,
	Link(PathBuf),
	File(u64, u64),
}

/// Everything under `root`, by relative path, without following links.
fn snapshot(root: &Path) -> BTreeMap<PathBuf, Entry> {
	fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Entry>) {
		for entry in std::fs::read_dir(dir).unwrap() {
			let entry = entry.unwrap();
			let path = entry.path();
			let relative = path.strip_prefix(root).unwrap().to_path_buf();
			let kind = entry.file_type().unwrap();
			if kind.is_symlink() {
				out.insert(relative, Entry::Link(std::fs::read_link(&path).unwrap()));
			} else if kind.is_dir() {
				out.insert(relative, Entry::Dir);
				walk(root, &path, out);
			} else {
				let bytes = std::fs::read(&path).unwrap();
				let mut hasher = std::collections::hash_map::DefaultHasher::new();
				bytes.hash(&mut hasher);
				out.insert(relative, Entry::File(bytes.len() as u64, hasher.finish()));
			}
		}
	}
	let mut out = BTreeMap::new();
	walk(root, root, &mut out);
	out
}

/// The differences between two snapshots, one line each.
fn changes(before: &BTreeMap<PathBuf, Entry>, after: &BTreeMap<PathBuf, Entry>) -> Vec<String> {
	let mut out = Vec::new();
	for (path, entry) in before {
		match after.get(path) {
			None => out.push(format!("removed {}", path.display())),
			Some(now) if now != entry => out.push(format!("changed {}", path.display())),
			Some(_) => {}
		}
	}
	for path in after.keys().filter(|p| !before.contains_key(*p)) {
		out.push(format!("created {}", path.display()));
	}
	out
}

/// A relative path from the working directory to `target`, which goes up to the root first.
#[cfg(unix)]
fn relative_to_cwd(target: &Path) -> PathBuf {
	let cwd = std::env::current_dir().unwrap().canonicalize().unwrap();
	let target = target.canonicalize().unwrap();
	let up: PathBuf = cwd.components().skip(1).map(|_| "..").collect();
	up.join(target.strip_prefix("/").unwrap())
}

/// A tree in `root/db` with committed and flushed data and committed data still in the
/// memtable, which a checkpoint would flush. Everything under `root` is the test's to inspect.
async fn tree_with_data(root: &Path, vlog: bool) -> (Arc<Tree>, Contents) {
	let tree = open(&root.join("db"), vlog);
	let mut expected = put(&tree, 0..300).await;
	tree.flush().unwrap();
	expected.extend(put(&tree, 300..450).await);
	(tree, expected)
}

/// The checkpoint is refused with `Error::InvalidArgument`, nothing under the temporary root
/// changes, nothing is flushed, and the tree still works: it takes more writes, a checkpoint
/// somewhere else opens with the same contents, and so does the tree after a reopen.
async fn assert_refused(destination: impl Fn(&Path) -> PathBuf) {
	for vlog in [false, true] {
		let root = TempDir::new("ckpt_dest").unwrap();
		let (tree, mut expected) = tree_with_data(root.path(), vlog).await;
		let destination = destination(root.path());
		let before = snapshot(root.path());
		let queued_before = tree.core.inner.immutable_count();

		let outcome = tree.create_checkpoint(&destination);

		let after = snapshot(root.path());
		assert_eq!(
			changes(&before, &after),
			Vec::<String>::new(),
			"the refused checkpoint to {} changed the files (vlog {vlog}): {outcome:?}",
			destination.display()
		);
		assert!(
			matches!(outcome, Err(Error::InvalidArgument(_))),
			"checkpoint to {} (vlog {vlog}): {outcome:?}",
			destination.display()
		);
		assert_eq!(tree.core.inner.immutable_count(), queued_before, "nothing is rotated");
		assert!(!tree.core.inner.active_memtable.read().unwrap().is_empty(), "nothing is flushed");
		assert_contents(&tree, &expected, "tree after the refusal");

		expected.extend(put(&tree, 450..500).await);
		let elsewhere = root.path().join("elsewhere");
		tree.create_checkpoint(&elsewhere).unwrap();
		let copy = open(&elsewhere, vlog);
		assert_contents(&copy, &expected, "checkpoint taken after the refusal");
		within("close()", copy.close()).await.unwrap();

		within("close()", tree.close()).await.unwrap();
		drop(tree);
		let reopened = open(&root.path().join("db"), vlog);
		assert_contents(&reopened, &expected, "tree reopened after the refusal");
		within("close()", reopened.close()).await.unwrap();
	}
}

// ---------------------------------------------------------------------------
// destinations that overlap the database
// ---------------------------------------------------------------------------

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_database_directory_is_refused() {
	assert_refused(|root| root.join("db")).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_database_directory_spelled_with_a_trailing_slash_and_dot_is_refused() {
	assert_refused(|root| root.join("db").join(".")).await;
	assert_refused(|root| PathBuf::from(format!("{}/", root.join("db").display()))).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_child_of_the_database_directory_is_refused() {
	assert_refused(|root| root.join("db").join("checkpoint")).await;
	assert_refused(|root| root.join("db").join("a").join("b").join("c")).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_subdirectory_of_the_database_is_refused() {
	for sub in ["sstables", "wal", "manifest"] {
		assert_refused(|root| root.join("db").join(sub)).await;
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_parent_of_the_database_directory_is_refused() {
	assert_refused(|root| root.to_path_buf()).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_path_through_dot_dot_back_to_the_database_is_refused() {
	assert_refused(|root| root.join("db").join("sstables").join("..")).await;
	assert_refused(|root| root.join("db").join("wal").join("..").join("new")).await;
	assert_refused(|root| root.join("db").join("..").join("db")).await;
	assert_refused(|root| root.join("db").join("sstables").join("..").join("..")).await;
	// Spelled around a directory that exists, which only resolving shows to lead back.
	let side = |root: &Path| {
		std::fs::create_dir(root.join("side")).unwrap();
		root.join("side")
	};
	assert_refused(|root| side(root).join("..").join("db")).await;
	assert_refused(|root| side(root).join("..").join("db").join("new")).await;
	assert_refused(|root| side(root).join("..")).await;
}

#[cfg(unix)]
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_relative_path_to_the_database_is_refused() {
	assert_refused(|root| relative_to_cwd(&root.join("db"))).await;
	assert_refused(|root| relative_to_cwd(&root.join("db")).join("checkpoint")).await;
	assert_refused(relative_to_cwd).await;
}

#[cfg(unix)]
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_symlink_to_the_database_is_refused() {
	use std::os::unix::fs::symlink;

	assert_refused(|root| {
		let link = root.join("link");
		symlink(root.join("db"), &link).unwrap();
		link
	})
	.await;
	// A child that does not exist yet, reached through the link.
	assert_refused(|root| {
		let link = root.join("link");
		symlink(root.join("db"), &link).unwrap();
		link.join("checkpoint")
	})
	.await;
	// A link to a subdirectory.
	assert_refused(|root| {
		let link = root.join("link");
		symlink(root.join("db").join("sstables"), &link).unwrap();
		link
	})
	.await;
	// A link to the parent.
	assert_refused(|root| {
		let link = root.join("link");
		symlink(root, &link).unwrap();
		link
	})
	.await;
	// A link to a link.
	assert_refused(|root| {
		symlink(root.join("db"), root.join("first")).unwrap();
		symlink(root.join("first"), root.join("second")).unwrap();
		root.join("second")
	})
	.await;
}

/// A subdirectory of the database that is a link to a place outside it is still the database's.
#[cfg(unix)]
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_subdirectory_that_is_a_link_out_of_the_database_directory_is_refused() {
	use std::os::unix::fs::symlink;

	for vlog in [false, true] {
		let root = TempDir::new("ckpt_dest_link").unwrap();
		let tree = open(&root.path().join("db"), vlog);
		let expected = put(&tree, 0..200).await;
		within("close()", tree.close()).await.unwrap();
		drop(tree);

		// Moves the SSTables out and leaves a link behind, then reopens.
		let outside = root.path().join("tables_elsewhere");
		let sstables = root.path().join("db").join("sstables");
		std::fs::rename(&sstables, &outside).unwrap();
		symlink(&outside, &sstables).unwrap();
		let tree = open(&root.path().join("db"), vlog);
		assert_contents(&tree, &expected, "tree with its tables behind a link");

		let before = snapshot(root.path());
		let outcome = tree.create_checkpoint(&outside);
		assert_eq!(changes(&before, &snapshot(root.path())), Vec::<String>::new());
		assert!(matches!(outcome, Err(Error::InvalidArgument(_))), "{outcome:?}");
		assert_contents(&tree, &expected, "tree after the refusal");
		within("close()", tree.close()).await.unwrap();
	}
}

/// The message names the destination and the database directory.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_refusal_says_why() {
	let root = TempDir::new("ckpt_dest_msg").unwrap();
	let (tree, _) = tree_with_data(root.path(), false).await;
	let destination = root.path().join("db").join("checkpoint");
	let Err(Error::InvalidArgument(message)) = tree.create_checkpoint(&destination) else {
		panic!("a destination inside the database directory was accepted");
	};
	assert!(message.contains("Checkpoint destination"), "{message}");
	assert!(message.contains(&destination.display().to_string()), "{message}");
	assert!(message.contains("database"), "{message}");
	within("close()", tree.close()).await.unwrap();
}

fn copy_dir(from: &Path, to: &Path) {
	std::fs::create_dir_all(to).unwrap();
	for entry in std::fs::read_dir(from).unwrap() {
		let entry = entry.unwrap();
		let target = to.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &target);
		} else {
			std::fs::copy(entry.path(), &target).unwrap();
		}
	}
}

/// Recovery continues in a fresh WAL segment and tags the recovered memtable with it, although
/// the memtable holds what the crashed segments held. A refused destination leaves a tree
/// recovered that way, with the crashed segment in its WAL directory, as it was: nothing is
/// flushed or rotated, and a checkpoint elsewhere, and a crash image after it, hold the recovered
/// rows and the ones committed since.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_refused_checkpoint_of_a_recovered_tree_changes_nothing() {
	for vlog in [false, true] {
		let root = TempDir::new("ckpt_dest_recovered").unwrap();
		let db = root.path().join("db");

		// The first run is never closed before the copy: what it committed is in its WAL only.
		let first = open(&root.path().join("first"), vlog);
		let mut expected = put(&first, 0..200).await;
		let crashed = first.core.inner.wal.read().get_active_log_number();
		copy_dir(&root.path().join("first"), &db);
		within("close()", first.close()).await.unwrap();
		drop(first);

		let tree = open(&db, vlog);
		assert_eq!(tree.core.inner.wal.read().get_active_log_number(), crashed + 1);
		assert_contents(&tree, &expected, "the recovered tree");
		assert!(!tree.core.inner.active_memtable.read().unwrap().is_empty(), "nothing recovered");
		expected.extend(put(&tree, 200..250).await);

		let before = snapshot(root.path());
		let crashed_segment = before.iter().any(|(path, entry)| {
			path.starts_with("db/wal") && matches!(entry, Entry::File(len, _) if *len > 0)
		});
		assert!(crashed_segment, "the crashed segment is not in the snapshot");
		let queued = tree.core.inner.immutable_count();

		let outcome = tree.create_checkpoint(db.join("checkpoint"));

		assert!(matches!(outcome, Err(Error::InvalidArgument(_))), "{outcome:?}");
		assert_eq!(changes(&before, &snapshot(root.path())), Vec::<String>::new());
		assert_eq!(tree.core.inner.immutable_count(), queued, "nothing is rotated");
		assert!(!tree.core.inner.active_memtable.read().unwrap().is_empty(), "nothing is flushed");
		assert_eq!(tree.core.inner.wal.read().get_active_log_number(), crashed + 1);

		let elsewhere = root.path().join("elsewhere");
		tree.create_checkpoint(&elsewhere).unwrap();
		let copy = open(&elsewhere, vlog);
		assert_contents(&copy, &expected, "checkpoint taken after the refusal");
		within("close()", copy.close()).await.unwrap();

		copy_dir(&db, &root.path().join("image"));
		let image = open(&root.path().join("image"), vlog);
		assert_contents(&image, &expected, "crash image after the checkpoint");
		within("close()", image.close()).await.unwrap();
		within("close()", tree.close()).await.unwrap();
	}
}

// ---------------------------------------------------------------------------
// destinations that stay valid
// ---------------------------------------------------------------------------

/// Checkpoints that must keep working: next to the database, under directories that do not exist
/// yet, in an empty directory, over an earlier checkpoint, through a link to somewhere else, and
/// in a directory whose name only starts like the database's.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn destinations_outside_the_database_still_work() {
	for vlog in [false, true] {
		let root = TempDir::new("ckpt_dest_ok").unwrap();
		let (tree, mut expected) = tree_with_data(root.path(), vlog).await;

		let mut destinations = vec![
			root.path().join("cp"),
			root.path().join("db_copy"),
			root.path().join("db2").join("nested").join("cp"),
			root.path().join("deep").join("..").join("tidy"),
			// Components that are made by the checkpoint, so the `..` goes up from one of them.
			root.path().join("made").join("..").join("lazy"),
		];
		std::fs::create_dir(root.path().join("empty")).unwrap();
		destinations.push(root.path().join("empty"));
		#[cfg(unix)]
		{
			destinations.push(relative_to_cwd(root.path()).join("relative"));
			let target = root.path().join("link_target");
			std::fs::create_dir(&target).unwrap();
			std::os::unix::fs::symlink(&target, root.path().join("link")).unwrap();
			destinations.push(root.path().join("link"));
		}
		// `deep` has to exist for the `..` above to name a place.
		std::fs::create_dir(root.path().join("deep")).unwrap();

		for destination in &destinations {
			tree.create_checkpoint(destination)
				.unwrap_or_else(|e| panic!("checkpoint to {}: {e:?}", destination.display()));
			let copy = open(destination, vlog);
			assert_contents(&copy, &expected, &format!("copy in {}", destination.display()));
			within("close()", copy.close()).await.unwrap();
		}

		// Again over the checkpoints that are there, after more writes.
		expected.extend(put(&tree, 450..600).await);
		for destination in &destinations {
			tree.create_checkpoint(destination).unwrap();
			let copy = open(destination, vlog);
			assert_contents(&copy, &expected, &format!("second copy in {}", destination.display()));
			within("close()", copy.close()).await.unwrap();
		}
		assert_contents(&tree, &expected, "live tree");
		within("close()", tree.close()).await.unwrap();
	}
}

/// A destination that cannot be made a directory fails and changes nothing: a file, and a path
/// under a file.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_destination_that_cannot_be_used_fails_without_changes() {
	let root = TempDir::new("ckpt_dest_file").unwrap();
	let (tree, expected) = tree_with_data(root.path(), false).await;
	std::fs::write(root.path().join("file"), b"not a directory").unwrap();
	let before = snapshot(root.path());

	for destination in [root.path().join("file"), root.path().join("file").join("cp")] {
		let outcome = tree.create_checkpoint(&destination);
		assert!(outcome.is_err(), "checkpoint to {}: {outcome:?}", destination.display());
	}

	assert_eq!(changes(&before, &snapshot(root.path())), Vec::<String>::new());
	assert_contents(&tree, &expected, "tree after the failures");
	within("close()", tree.close()).await.unwrap();
}

// ---------------------------------------------------------------------------
// the database reached through a link, a `..` or a relative path
// ---------------------------------------------------------------------------

/// The database is opened through `open_as`, and the checkpoint is asked for at `destination`,
/// which names the same directories some other way. The directories that the database itself
/// is given are resolved like the destination.
async fn assert_overlap_refused(
	root: &Path,
	open_as: impl Fn(&Path) -> PathBuf,
	destination: impl Fn(&Path) -> PathBuf,
) {
	let tree = open(&open_as(root), false);
	let mut expected = put(&tree, 0..200).await;
	tree.flush().unwrap();
	expected.extend(put(&tree, 200..260).await);
	let destination = destination(root);
	let outcome = tree.create_checkpoint(&destination);
	assert!(
		matches!(outcome, Err(Error::InvalidArgument(_))),
		"checkpoint to {} of a database opened as {}: {outcome:?}",
		destination.display(),
		open_as(root).display()
	);
	assert_contents(&tree, &expected, "tree after the refusal");
	within("close()", tree.close()).await.unwrap();
	drop(tree);
	let reopened = open(&open_as(root), false);
	assert_contents(&reopened, &expected, "tree after the reopen");
	within("close()", reopened.close()).await.unwrap();
}

#[cfg(unix)]
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_database_opened_through_a_link_refuses_its_real_path() {
	use std::os::unix::fs::symlink;

	let open_as = |root: &Path| {
		let link = root.join("link");
		if !link.exists() {
			std::fs::create_dir_all(root.join("real")).unwrap();
			symlink(root.join("real"), &link).unwrap();
		}
		link
	};
	for suffix in ["", "sub", "sstables", "a/b"] {
		let root = TempDir::new("ckpt_dest_open_link").unwrap();
		assert_overlap_refused(root.path(), open_as, |root| root.join("real").join(suffix)).await;
	}
	let root = TempDir::new("ckpt_dest_open_link").unwrap();
	assert_overlap_refused(root.path(), open_as, |root| root.to_path_buf()).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_database_opened_through_a_dot_dot_refuses_its_real_path() {
	let open_as = |root: &Path| {
		std::fs::create_dir_all(root.join("side")).unwrap();
		root.join("side").join("..").join("db")
	};
	let root = TempDir::new("ckpt_dest_open_dots").unwrap();
	assert_overlap_refused(root.path(), open_as, |root| root.join("db")).await;
	let root = TempDir::new("ckpt_dest_open_dots").unwrap();
	assert_overlap_refused(root.path(), open_as, |root| root.join("db").join("new")).await;
}

#[cfg(unix)]
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_database_opened_through_a_relative_path_refuses_its_real_path() {
	let open_as = |root: &Path| {
		std::fs::create_dir_all(root.join("db")).unwrap();
		relative_to_cwd(root).join("db")
	};
	for destination in [
		|root: &Path| root.join("db"),
		|root: &Path| root.join("db").join("sstables"),
		|root: &Path| root.to_path_buf(),
	] {
		let root = TempDir::new("ckpt_dest_open_rel").unwrap();
		assert_overlap_refused(root.path(), open_as, destination).await;
	}
}

/// `..` after a link names the parent of the link's target, not of the link: a destination that
/// goes up from a link to a subdirectory of the database lands in the database.
#[cfg(unix)]
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn dot_dot_after_a_link_is_resolved_through_the_link() {
	use std::os::unix::fs::symlink;

	let root = TempDir::new("ckpt_dest_link_dots").unwrap();
	let (tree, expected) = tree_with_data(root.path(), false).await;
	let link = root.path().join("link");
	symlink(root.path().join("db").join("sstables"), &link).unwrap();
	for destination in [
		link.join(".."),
		link.join("..").join("ck"),
		link.join("..").join("sstables"),
		link.join("..").join("..").join("db"),
	] {
		let before = snapshot(root.path());
		let outcome = tree.create_checkpoint(&destination);
		assert!(matches!(outcome, Err(Error::InvalidArgument(_))), "{outcome:?}");
		assert_eq!(changes(&before, &snapshot(root.path())), Vec::<String>::new());
	}

	// The same spelling after a link to somewhere harmless leads somewhere harmless.
	let harmless = root.path().join("harmless");
	std::fs::create_dir_all(harmless.join("inner")).unwrap();
	symlink(harmless.join("inner"), root.path().join("other")).unwrap();
	tree.create_checkpoint(root.path().join("other").join("..").join("ck")).unwrap();
	let copy = open(&harmless.join("ck"), false);
	assert_contents(&copy, &expected, "checkpoint through a link and ..");
	within("close()", copy.close()).await.unwrap();
	within("close()", tree.close()).await.unwrap();
}

/// The filesystem root contains the database, however it is spelled.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_filesystem_root_is_refused() {
	let root = TempDir::new("ckpt_dest_fsroot").unwrap();
	let (tree, expected) = tree_with_data(root.path(), false).await;
	for spelling in ["/", "/.", "/..", "/../..", "//"] {
		let before = snapshot(root.path());
		let outcome = tree.create_checkpoint(Path::new(spelling));
		assert!(matches!(outcome, Err(Error::InvalidArgument(_))), "{spelling}: {outcome:?}");
		assert_eq!(changes(&before, &snapshot(root.path())), Vec::<String>::new());
	}
	assert_contents(&tree, &expected, "tree after the refusals");
	within("close()", tree.close()).await.unwrap();
}

// ---------------------------------------------------------------------------
// entries of an existing destination that link into the database
// ---------------------------------------------------------------------------

/// A destination outside the database that already holds an entry the checkpoint writes (the
/// tables, the manifest directory, the metadata file) as a link into the live files: the
/// checkpoint is refused before it removes, copies over or truncates any of them.
#[cfg(unix)]
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_destination_entry_that_links_into_the_database_is_refused() {
	for entry in ["sstables", "manifest", "wal", "CHECKPOINT_METADATA"] {
		for vlog in [false, true] {
			let root = TempDir::new("ckpt_dest_entry").unwrap();
			let (tree, expected) = tree_with_data(root.path(), vlog).await;
			tree.flush().unwrap();
			let db = root.path().join("db");
			let target = if entry == "CHECKPOINT_METADATA" {
				// A file of the manifest, which the metadata would be written over.
				let name = std::fs::read_dir(db.join("manifest")).unwrap().next().unwrap().unwrap();
				db.join("manifest").join(name.file_name())
			} else {
				db.join(entry)
			};
			let cp = root.path().join("cp");
			std::fs::create_dir(&cp).unwrap();
			std::os::unix::fs::symlink(&target, cp.join(entry)).unwrap();
			let before = snapshot(root.path());

			let outcome = tree.create_checkpoint(&cp);

			assert_eq!(
				changes(&before, &snapshot(root.path())),
				Vec::<String>::new(),
				"{entry} linking to {} (vlog {vlog}): {outcome:?}",
				target.display()
			);
			assert!(matches!(outcome, Err(Error::InvalidArgument(_))), "{entry}: {outcome:?}");
			assert_contents(&tree, &expected, "tree after the refusal");
			within("close()", tree.close()).await.unwrap();
		}
	}
}

/// A destination that is a dangling link, or lies under one, fails and changes nothing, whether
/// the link points into the database or out of it.
#[cfg(unix)]
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_dangling_link_as_the_destination_fails_without_changes() {
	use std::os::unix::fs::symlink;

	let root = TempDir::new("ckpt_dest_dangling").unwrap();
	let (tree, expected) = tree_with_data(root.path(), false).await;
	let db = root.path().join("db");
	symlink(db.join("not_yet"), root.path().join("into_db")).unwrap();
	symlink(root.path().join("not_yet_either"), root.path().join("out_of_db")).unwrap();
	symlink(root.path().join("loop_b"), root.path().join("loop_a")).unwrap();
	symlink(root.path().join("loop_a"), root.path().join("loop_b")).unwrap();
	let before = snapshot(root.path());
	for destination in [
		root.path().join("loop_a"),
		root.path().join("loop_a").join("sub"),
		root.path().join("into_db"),
		root.path().join("into_db").join("sub"),
		root.path().join("out_of_db"),
		root.path().join("out_of_db").join("sub"),
	] {
		let outcome = tree.create_checkpoint(&destination);
		assert!(outcome.is_err(), "checkpoint to {}: {outcome:?}", destination.display());
	}
	assert_eq!(changes(&before, &snapshot(root.path())), Vec::<String>::new());
	assert_contents(&tree, &expected, "tree after the failures");
	within("close()", tree.close()).await.unwrap();
}

// ---------------------------------------------------------------------------
// the environment of the check
// ---------------------------------------------------------------------------

/// The check does not ask for the working directory when the destination is absolute: a process
/// whose working directory was removed can still checkpoint. The working directory is
/// process-wide, so the test runs itself again in a child process.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_removed_working_directory_does_not_stop_a_checkpoint_to_an_absolute_path() {
	const NAME: &str = "a_removed_working_directory_does_not_stop_a_checkpoint_to_an_absolute_path";
	const CHILD: &str = "CHECKPOINT_DESTINATION_REMOVED_CWD_CHILD";
	if std::env::var_os(CHILD).is_none() {
		let module = module_path!().split_once("::").unwrap().1;
		let output = std::process::Command::new(std::env::current_exe().unwrap())
			.args(["--exact", &format!("{module}::{NAME}"), "--nocapture", "--test-threads=1"])
			.env(CHILD, "1")
			.output()
			.unwrap();
		assert!(
			output.status.success(),
			"the child failed:\n{}\n{}",
			String::from_utf8_lossy(&output.stdout),
			String::from_utf8_lossy(&output.stderr)
		);
		return;
	}

	let root = TempDir::new("ckpt_dest_no_cwd").unwrap();
	let cwd = root.path().join("cwd");
	std::fs::create_dir(&cwd).unwrap();
	std::env::set_current_dir(&cwd).unwrap();
	std::fs::remove_dir(&cwd).unwrap();
	if std::env::current_dir().is_ok() {
		eprintln!("this platform still names a removed working directory: nothing to check");
		return;
	}

	let (tree, expected) = tree_with_data(root.path(), false).await;
	let cp = root.path().join("cp");
	tree.create_checkpoint(&cp).unwrap();
	let copy = open(&cp, false);
	assert_contents(&copy, &expected, "checkpoint taken without a working directory");
	within("close()", copy.close()).await.unwrap();
	within("close()", tree.close()).await.unwrap();
}

/// Checkpoints into one directory from several threads at once leave the live tables alone: a
/// table that another checkpoint has just linked is not copied over, which would truncate it.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn concurrent_checkpoints_into_one_directory_leave_the_live_tables_intact() {
	{
		let root = TempDir::new("ckpt_dest_same").unwrap();
		let tree = open(&root.path().join("db"), false);
		let mut expected = Contents::new();
		for chunk in 0..10 {
			expected.extend(put(&tree, chunk * 40..(chunk + 1) * 40).await);
			tree.flush().unwrap();
		}
		let live = snapshot(&root.path().join("db").join("sstables"));
		assert!(live.len() >= 8, "{} tables", live.len());
		let cp = root.path().join("cp");

		let threads: Vec<_> = (0..3)
			.map(|_| {
				let (tree, cp) = (Arc::clone(&tree), cp.clone());
				std::thread::spawn(move || {
					(0..20).map(|_| tree.create_checkpoint(&cp)).collect::<Vec<_>>()
				})
			})
			.collect();
		for outcome in threads.into_iter().flat_map(|t| t.join().unwrap()) {
			outcome.unwrap();
		}

		// A table is immutable, so one that differs from before was written over. Compaction
		// may retire tables meanwhile, so only the ones still there are compared.
		let after = snapshot(&root.path().join("db").join("sstables"));
		let overwritten: Vec<_> = live
			.iter()
			.filter(|(path, entry)| after.get(*path).is_some_and(|now| now != *entry))
			.map(|(path, _)| path.display().to_string())
			.collect();
		assert!(overwritten.is_empty(), "live tables were written over: {overwritten:?}");
		assert_contents(&tree, &expected, "tree after the checkpoints");
		let copy = open(&cp, false);
		assert_contents(&copy, &expected, "the checkpoint");
		within("close()", copy.close()).await.unwrap();
		within("close()", tree.close()).await.unwrap();
	}
}
