#[cfg(not(target_arch = "wasm32"))]
use std::fs::{File, OpenOptions};
#[cfg(not(target_arch = "wasm32"))]
use std::io::{ErrorKind, Write};
use std::path::{Component, Path, PathBuf};
#[cfg(not(target_arch = "wasm32"))]
use std::process;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::error::{Error, Result};

/// LockFile prevents multiple processes from accessing the same database
/// directory
///
/// It is one of three kinds, and `new` makes the first: `Os` is the lock of a native build,
/// `Registered` the lock of a pool file system, and `Refused` the lock of a file system that has
/// nothing mounted, which cannot be acquired.
///
/// # How `Os` works
///
/// This implementation uses OS-level file locking to
/// ensure that only one process can hold the lock at a time. The lock file
/// contains the PID of the process that currently holds the lock for debugging
/// purposes.
///
/// # Stale Lock Handling
///
/// If a process crashes or is killed while holding the lock, the OS
/// automatically releases the file lock. This means that stale lock files (with
/// old PIDs written in them) do NOT block new processes from acquiring the
/// lock. The file content (PID) is purely informational and has no impact on
/// the actual locking mechanism.
///
/// When a new process acquires the lock, it overwrites the file with its own
/// PID.
///
/// # Lock Release
///
/// The lock is released in two ways:
/// 1. Explicitly by calling `release()` (typically during database close)
/// 2. Automatically when the LockFile goes out of scope
///
/// Even if `release()` is not called explicitly, the OS will release the file
/// lock when the process terminates (normal exit or crash).
///
/// # Targets
///
/// The `Os` lock does nothing on `wasm32`, browser or WASI alike: a browser has no `flock`, and a
/// WASI runtime is left without a lock as it always was. `Registered` and `Refused` are the same
/// on every target, and a `Registered` lock gives its entry back when it is dropped there too.
pub(crate) enum LockFile {
	/// The `flock` of a `LOCK` file, held by this process.
	Os(OsLock),
	/// An entry in a set that a file system keeps: one tree in this process owns a directory.
	Registered(RegisteredLock),
	/// There is nothing to lock: the file system has nothing mounted.
	#[allow(
		dead_code,
		reason = "made by a file system with nothing mounted, which a native build never has"
	)]
	Refused(&'static str),
}

impl LockFile {
	/// Lock file name used in database directories
	pub const LOCK_FILE_NAME: &'static str = "LOCK";

	/// Creates a new lock file at the specified path
	pub fn new<P: AsRef<Path>>(path: P) -> Self {
		Self::Os(OsLock::new(path.as_ref()))
	}

	/// A lock on `dir` in `registry`, for a file system that keeps its files in a registry and so
	/// has no `LOCK` file to lock. The directory is normalised, so `a/./b` and `a/b` are one.
	pub(crate) fn registered(registry: Arc<LockRegistry>, dir: &Path) -> Self {
		Self::Registered(RegisteredLock {
			registry,
			dir: normalise_dir(dir),
			held: false,
		})
	}

	/// A lock that cannot be acquired, with the reason.
	#[allow(dead_code, reason = "handed out by a file system with nothing mounted")]
	pub(crate) fn refused(reason: &'static str) -> Self {
		Self::Refused(reason)
	}

	/// Acquires the lock, returning an error if the database is already in use
	pub fn acquire(&mut self) -> Result<()> {
		match self {
			Self::Os(lock) => lock.acquire(),
			Self::Registered(lock) => lock.acquire(),
			Self::Refused(reason) => {
				Err(Error::Io(Arc::new(crate::vfs::unavailable_error(reason))))
			}
		}
	}

	/// Releases the lock
	pub fn release(&mut self) -> Result<()> {
		match self {
			Self::Os(lock) => lock.release(),
			Self::Registered(lock) => lock.release(),
			Self::Refused(_) => Ok(()),
		}
	}
}

/// The `flock` of a `LOCK` file.
pub(crate) struct OsLock {
	/// The path to the lock file
	#[cfg_attr(target_arch = "wasm32", allow(dead_code))]
	path: PathBuf,
	/// The lock file handle
	#[cfg(not(target_arch = "wasm32"))]
	file: Option<File>,
}

impl OsLock {
	fn new(dir: &Path) -> Self {
		Self {
			path: dir.join(LockFile::LOCK_FILE_NAME),
			#[cfg(not(target_arch = "wasm32"))]
			file: None,
		}
	}

	/// Acquires the lock, returning an error if the database is already in use
	#[cfg(not(target_arch = "wasm32"))]
	fn acquire(&mut self) -> Result<()> {
		// Try to open the lock file with create flag
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(true)
			.open(&self.path)
			.map_err(|e| Error::Io(Arc::new(e)))?;

		// Try to lock the file exclusively
		try_lock_exclusive(&file).map_err(|e| match e.kind() {
			ErrorKind::WouldBlock => Error::Other(format!(
				"Database at {} is already locked by another process",
				self.path.display()
			)),
			_ => Error::Io(Arc::new(e)),
		})?;

		// Write process ID to lock file for debugging
		let pid = process::id();
		let content = format!("{}\n", pid);
		file.set_len(0).map_err(|e| Error::Io(Arc::new(e)))?;
		(&file).write_all(content.as_bytes()).map_err(|e| Error::Io(Arc::new(e)))?;
		(&file).flush().map_err(|e| Error::Io(Arc::new(e)))?;

		self.file = Some(file);
		Ok(())
	}

	/// Releases the lock
	#[cfg(not(target_arch = "wasm32"))]
	fn release(&mut self) -> Result<()> {
		if let Some(file) = self.file.take() {
			let _ = unlock(&file);
			drop(file);
		}
		Ok(())
	}

	/// Acquires the lock (WASM no-op)
	#[cfg(target_arch = "wasm32")]
	fn acquire(&mut self) -> Result<()> {
		Ok(())
	}

	/// Releases the lock (WASM no-op)
	#[cfg(target_arch = "wasm32")]
	fn release(&mut self) -> Result<()> {
		Ok(())
	}
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for OsLock {
	fn drop(&mut self) {
		// Try to release the lock, but don't panic if it fails
		let _ = self.release();
	}
}

/// The directories that are locked in one file system: one tree owns each, in this process. A
/// browser has one worker, so this is the whole of what a lock can mean there; a second tab is
/// kept out by the exclusive handle that the file system holds on its own lock file.
#[derive(Debug, Default)]
pub(crate) struct LockRegistry {
	held: Mutex<Vec<PathBuf>>,
}

impl LockRegistry {
	fn try_insert(&self, dir: &Path) -> bool {
		let mut held = self.held.lock();
		if held.iter().any(|held| held == dir) {
			return false;
		}
		held.push(dir.to_path_buf());
		true
	}

	fn remove(&self, dir: &Path) {
		self.held.lock().retain(|held| held != dir);
	}

	/// Whether `dir` is locked now, for a test.
	#[cfg(test)]
	pub(crate) fn is_held(&self, dir: &Path) -> bool {
		let dir = normalise_dir(dir);
		self.held.lock().contains(&dir)
	}
}

/// An entry in a [`LockRegistry`], taken by `acquire` and given back by `release` and by drop, on
/// every target.
pub(crate) struct RegisteredLock {
	registry: Arc<LockRegistry>,
	dir: PathBuf,
	held: bool,
}

impl RegisteredLock {
	fn acquire(&mut self) -> Result<()> {
		if self.held {
			return Ok(());
		}
		if !self.registry.try_insert(&self.dir) {
			return Err(Error::Other(format!(
				"Database at {} is already locked by another tree on this file system",
				self.dir.display()
			)));
		}
		self.held = true;
		Ok(())
	}

	fn release(&mut self) -> Result<()> {
		if self.held {
			self.registry.remove(&self.dir);
			self.held = false;
		}
		Ok(())
	}
}

impl Drop for RegisteredLock {
	fn drop(&mut self) {
		let _ = self.release();
	}
}

/// The directory with `.` and `..` resolved by name and the separators collapsed, without asking
/// the file system, so that two spellings of one directory take the same entry. A `..` that has
/// nothing to remove is kept on a relative path and dropped at the root.
pub(crate) fn normalise_dir(dir: &Path) -> PathBuf {
	let mut out = PathBuf::new();
	for component in dir.components() {
		match component {
			Component::CurDir => {}
			Component::ParentDir => match out.components().next_back() {
				Some(Component::Normal(_)) => {
					out.pop();
				}
				Some(Component::RootDir | Component::Prefix(_)) => {}
				_ => out.push(".."),
			},
			other => out.push(other.as_os_str()),
		}
	}
	out
}

/// The behaviour of a registered lock, for the native and the browser tests, which run the same
/// check: the entry is taken, refused to a second lock on the same directory, and given back on
/// release and on drop.
#[cfg(test)]
pub(crate) fn check_registered_locks() {
	let registry = Arc::new(LockRegistry::default());
	let dir = Path::new("db/./inner/../");

	let mut first = LockFile::registered(Arc::clone(&registry), dir);
	assert!(!registry.is_held(Path::new("db")));
	first.acquire().expect("the first lock is granted");
	assert!(registry.is_held(Path::new("db")), "the directory is held under its normal form");
	// Acquiring a lock that is held is not an error, and takes nothing more.
	first.acquire().expect("a held lock is held");

	let mut second = LockFile::registered(Arc::clone(&registry), Path::new("db"));
	match second.acquire() {
		Err(Error::Other(message)) => assert!(message.contains("already locked"), "{message}"),
		other => panic!("the second lock on one directory is refused: {other:?}"),
	}
	// Another directory is its own.
	let mut other = LockFile::registered(Arc::clone(&registry), Path::new("db2"));
	other.acquire().expect("another directory is free");

	// A release gives the entry back, and so does a second one, which does nothing.
	first.release().expect("release");
	first.release().expect("release twice");
	assert!(!registry.is_held(Path::new("db")));
	second.acquire().expect("the directory is free after the release");
	drop(second);
	assert!(!registry.is_held(Path::new("db")), "a dropped lock gives its entry back");

	// A lock that was never acquired takes nothing and gives nothing back.
	let mut held = LockFile::registered(Arc::clone(&registry), Path::new("db3"));
	held.acquire().unwrap();
	drop(LockFile::registered(Arc::clone(&registry), Path::new("db3")));
	assert!(registry.is_held(Path::new("db3")), "dropping a lock that was not acquired is free");
	drop(held);
	assert!(!registry.is_held(Path::new("db3")));
	drop(other);
	assert!(!registry.is_held(Path::new("db2")));

	// A lock that is refused cannot be acquired, and releasing it is not an error.
	let mut refused = LockFile::refused("nothing is mounted");
	assert!(matches!(refused.acquire(), Err(Error::Io(_))));
	refused.release().expect("release of a refused lock");
}

#[cfg(unix)]
pub(crate) fn try_lock_exclusive(file: &File) -> std::io::Result<()> {
	use rustix::fs::{flock, FlockOperation};
	flock(file, FlockOperation::NonBlockingLockExclusive).map_err(std::io::Error::from)
}

#[cfg(unix)]
pub(crate) fn unlock(file: &File) -> std::io::Result<()> {
	use rustix::fs::{flock, FlockOperation};
	flock(file, FlockOperation::Unlock).map_err(std::io::Error::from)
}

#[cfg(windows)]
pub(crate) fn try_lock_exclusive(file: &File) -> std::io::Result<()> {
	use std::os::windows::io::AsRawHandle;

	use windows_sys::Win32::Storage::FileSystem::{
		LockFileEx,
		LOCKFILE_EXCLUSIVE_LOCK,
		LOCKFILE_FAIL_IMMEDIATELY,
	};
	use windows_sys::Win32::System::IO::OVERLAPPED;

	let handle = file.as_raw_handle() as _;
	let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
	let res = unsafe {
		LockFileEx(
			handle,
			LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
			0,
			!0,
			!0,
			&mut overlapped,
		)
	};
	if res == 0 {
		Err(std::io::Error::last_os_error())
	} else {
		Ok(())
	}
}

#[cfg(windows)]
pub(crate) fn unlock(file: &File) -> std::io::Result<()> {
	use std::os::windows::io::AsRawHandle;

	use windows_sys::Win32::Storage::FileSystem::UnlockFile;
	let handle = file.as_raw_handle() as _;
	let res = unsafe { UnlockFile(handle, 0, 0, !0, !0) };
	if res == 0 {
		Err(std::io::Error::last_os_error())
	} else {
		Ok(())
	}
}

#[cfg(not(any(unix, windows, target_arch = "wasm32")))]
pub(crate) fn try_lock_exclusive(_file: &File) -> std::io::Result<()> {
	Ok(())
}

#[cfg(not(any(unix, windows, target_arch = "wasm32")))]
pub(crate) fn unlock(_file: &File) -> std::io::Result<()> {
	Ok(())
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
	use std::fs;
	use std::sync::{Arc, Barrier};

	use tempfile::TempDir;
	use test_log::test;

	use super::*;
	use crate::error::Error;
	use crate::lsm::TreeBuilder;

	#[test]
	fn test_lock_acquisition_and_release() {
		let temp_dir = TempDir::new().unwrap();
		let mut lock = LockFile::new(temp_dir.path());

		// Initial acquisition should succeed
		assert!(lock.acquire().is_ok());

		// Lock file should exist
		assert!(temp_dir.path().join(LockFile::LOCK_FILE_NAME).exists());

		// Release should succeed
		assert!(lock.release().is_ok());
	}

	#[test]
	fn test_lock_contention() {
		let temp_dir = TempDir::new().unwrap();

		// Create first lock
		let mut lock1 = LockFile::new(temp_dir.path());
		assert!(lock1.acquire().is_ok());

		// Second lock should fail
		let mut lock2 = LockFile::new(temp_dir.path());
		assert!(lock2.acquire().is_err());

		// After releasing first lock, second should succeed
		assert!(lock1.release().is_ok());
		assert!(lock2.acquire().is_ok());
	}

	#[test]
	fn test_lock_with_process_id() {
		let temp_dir = TempDir::new().unwrap();
		let mut lock = LockFile::new(temp_dir.path());

		assert!(lock.acquire().is_ok());

		// Check that process ID is written to file
		let lock_path = temp_dir.path().join(LockFile::LOCK_FILE_NAME);
		let content = fs::read_to_string(&lock_path).unwrap();
		let pid = process::id().to_string();

		assert!(content.trim() == pid);
	}

	#[test]
	fn test_drop_releases_lock() {
		let temp_dir = TempDir::new().unwrap();

		{
			let mut lock = LockFile::new(temp_dir.path());
			assert!(lock.acquire().is_ok());
			// lock goes out of scope here and should be dropped
		}

		// New lock should succeed because previous was dropped
		let mut lock2 = LockFile::new(temp_dir.path());
		assert!(lock2.acquire().is_ok());
	}

	#[test]
	fn test_stale_lockfile_doesnt_block() {
		let temp_dir = TempDir::new().unwrap();

		// Simulate a stale lock file by creating one and writing a fake PID
		let lock_path = temp_dir.path().join(LockFile::LOCK_FILE_NAME);
		fs::write(&lock_path, "99999\n").unwrap();

		// Despite the file existing with a stale PID, we should be able to acquire the
		// lock because OS-level file locks are automatically released when a process
		// terminates
		let mut lock = LockFile::new(temp_dir.path());
		assert!(
            lock.acquire().is_ok(),
            "Should be able to acquire lock even with stale LOCK file, as OS-level locks are auto-released"
        );

		// Verify the PID was updated to current process
		let content = fs::read_to_string(&lock_path).unwrap();
		let current_pid = process::id().to_string();
		assert!(
			content.trim() == current_pid,
			"Lock file should contain current PID, not stale PID"
		);

		lock.release().unwrap();
	}

	// Integration tests with Tree

	#[test(tokio::test)]
	async fn test_lock_file_prevents_concurrent_access() {
		let temp_dir = TempDir::new().unwrap();
		let temp_path = temp_dir.path().to_path_buf();

		// First instance should succeed
		let tree1 = TreeBuilder::new()
			.with_path(temp_path.clone())
			.build()
			.expect("First tree should be created successfully");

		// Second instance should fail with lock error
		let result = TreeBuilder::new().with_path(temp_path.clone()).build();

		assert!(result.is_err(), "Second tree should fail to acquire lock");
		if let Err(Error::Other(msg)) = result {
			assert!(msg.contains("already locked"), "Error should indicate database is locked");
		} else {
			panic!("Expected a lock error");
		}

		// After closing the first tree, we should be able to open again
		tree1.close().await.unwrap();

		let tree2 = TreeBuilder::new()
			.with_path(temp_path)
			.build()
			.expect("After closing first tree, second should succeed");

		tree2.close().await.unwrap();
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_lock_file_multithreaded() {
		let temp_dir = TempDir::new().unwrap();
		let temp_path = temp_dir.path().to_path_buf();
		let threads = 10;

		// Use a barrier to make all threads try to open the database at roughly the
		// same time
		let barrier = Arc::new(Barrier::new(threads));
		let mut handles = vec![];

		for i in 0..threads {
			let path = temp_path.clone();
			let thread_barrier = Arc::clone(&barrier);

			let handle = tokio::task::spawn_blocking(move || {
				// Wait for all threads to be ready
				thread_barrier.wait();

				// Try to open the database
				let result = TreeBuilder::new().with_path(path).build();

				(i, result)
			});

			handles.push(handle);
		}

		// Collect results
		let mut results = vec![];
		for handle in handles {
			results.push(handle.await.unwrap());
		}

		// Exactly one thread should succeed in acquiring the lock
		let success_count = results.iter().filter(|(_, result)| result.is_ok()).count();
		assert_eq!(success_count, 1, "Exactly one thread should acquire the lock");

		// Close the successful instance
		for (_, result) in results {
			if let Ok(tree) = result {
				tree.close().await.unwrap();
			}
		}
	}

	// #[test(tokio::test(flavor = "multi_thread"))]
	// async fn test_lock_file_multithreaded_with_disk() {
	// 	let temp_dir = TempDir::new().unwrap();
	// 	let temp_path = temp_dir.path().to_path_buf();
	// 	let tasks = 10;

	// 	// Use a barrier to make all tasks try to open the database at roughly
	// the same time 	let barrier = Arc::new(Barrier::new(tasks));
	// 	let mut handles = vec![];

	// 	for i in 0..tasks {
	// 		let path = temp_path.clone();
	// 		let task_barrier = Arc::clone(&barrier);

	// 		let handle = tokio::task::spawn(async move {
	// 			// Wait for all tasks to be ready
	// 			task_barrier.wait();

	// 			// Try to open the database (with VLog disabled to avoid async issues)
	// 			let result =
	// TreeBuilder::new().with_path(path).with_enable_vlog(false).build();

	// 			(i, result)
	// 		});

	// 		handles.push(handle);
	// 	}

	// 	// Collect results
	// 	let mut results = vec![];
	// 	for handle in handles {
	// 		results.push(handle.await.unwrap());
	// 	}

	// 	// Exactly one task should succeed
	// 	let success_count = results.iter().filter(|(_, result)|
	// result.is_ok()).count(); 	assert_eq!(success_count, 1, "Exactly one task
	// should succeed in acquiring the lock");

	// 	// All other tasks should fail with a lock error
	// 	let failures = results.iter().filter(|(_, result)|
	// result.is_err()).count(); 	assert_eq!(failures, tasks - 1, "All other
	// tasks should fail with lock error");

	// 	// Close the successful instance
	// 	for (_, result) in results {
	// 		if let Ok(tree) = result {
	// 			tree.close().await.unwrap();
	// 			break; // Only one should succeed, so we can break after closing it
	// 		}
	// 	}
	// }
}

#[cfg(test)]
mod registered_tests {
	use std::path::Path;

	use super::*;

	/// The check that the browser tests run too: see `check_registered_locks`.
	#[test]
	fn a_registered_lock_is_one_per_directory_and_is_given_back() {
		check_registered_locks();
	}

	#[test]
	fn directories_are_normalised_by_name() {
		for (given, normal) in [
			("a/b", "a/b"),
			("a/./b/", "a/b"),
			("a//b", "a/b"),
			("a/b/..", "a"),
			("a/b/../../c", "c"),
			("../a", "../a"),
			("a/../..", ".."),
			("/a/../..", "/"),
			("/a/./b", "/a/b"),
			("", ""),
			(".", ""),
		] {
			assert_eq!(normalise_dir(Path::new(given)), Path::new(normal), "{given:?}");
		}
	}
}
