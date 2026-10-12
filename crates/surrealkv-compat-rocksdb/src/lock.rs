//! The lock RocksDB takes on a database directory.
//!
//! RocksDB takes `LOCK` with `fcntl(F_SETLK)`, a POSIX record lock. The `flock` that other
//! software takes does not conflict with it on Linux, so only a record lock keeps a live RocksDB
//! process out. POSIX record locks belong to the process and are released when any descriptor of
//! the file is closed in it: hold the guard, and do not open `LOCK` elsewhere meanwhile.

use std::fs::{File, OpenOptions};
use std::path::Path;

use crate::error::Result;

/// Holds the lock until it is dropped.
pub struct DirectoryLock {
	_file: File,
	/// Whether the platform has the lock. Where it does not, nothing keeps a live RocksDB process
	/// out, and [`lock_directory`] says so in this field.
	pub enforced: bool,
}

#[cfg(all(
	unix,
	not(any(
		target_os = "emscripten",
		target_os = "espidf",
		target_os = "fuchsia",
		target_os = "horizon",
		target_os = "redox",
		target_os = "vita",
		target_os = "wasi"
	))
))]
fn lock(file: &File, dir: &Path) -> Result<bool> {
	use rustix::fs::{fcntl_lock, FlockOperation};
	use rustix::io::Errno;

	use crate::error::Error;

	match fcntl_lock(file, FlockOperation::NonBlockingLockExclusive) {
		Ok(()) => Ok(true),
		Err(e) if e == Errno::AGAIN || e == Errno::ACCESS => Err(Error::InUse(format!(
			"the RocksDB database at {} is in use by another process",
			dir.display()
		))),
		Err(e) => Err(Error::Io(std::io::Error::from(e))),
	}
}

#[cfg(not(all(
	unix,
	not(any(
		target_os = "emscripten",
		target_os = "espidf",
		target_os = "fuchsia",
		target_os = "horizon",
		target_os = "redox",
		target_os = "vita",
		target_os = "wasi"
	))
)))]
fn lock(_file: &File, _dir: &Path) -> Result<bool> {
	Ok(false)
}

/// Takes the lock RocksDB takes on `<dir>/LOCK`, or fails with [`crate::Error::InUse`] if a process
/// holds it.
pub fn lock_directory(dir: &Path) -> Result<DirectoryLock> {
	let file = OpenOptions::new()
		.read(true)
		.write(true)
		.create(true)
		.truncate(false)
		.open(dir.join("LOCK"))?;
	let enforced = lock(&file, dir)?;
	Ok(DirectoryLock {
		_file: file,
		enforced,
	})
}
