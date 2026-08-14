//! Conditional-create publication for numbered immutable versions (FK1).
//!
//! Local mechanics are verbatim the proven adapter pattern
//! (`src/storage/local.rs`): temp write → file fsync → `fs::hard_link` to the
//! final name → unlink temp → parent-directory fsync. `hard_link` is
//! no-clobber, so this IS conditional-create — POSIX `rename` (which silently
//! overwrites) is never used for version files. A losing racer that produced
//! byte-identical content observes `AlreadyExistsSame`, which callers treat
//! as idempotent success (fork retries rely on this).

use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PublishOutcome {
	Created,
	/// The version already exists with exactly these bytes — an idempotent
	/// retry of our own publish, not a conflict.
	AlreadyExistsSame,
}

/// `{version:020}.{ext}` — the version id IS the filename; decode validates
/// the header repeats it.
pub(crate) fn version_file_name(version: u64, ext: &str) -> String {
	format!("{version:020}.{ext}")
}

fn version_path(dir: &Path, version: u64, ext: &str) -> PathBuf {
	dir.join(version_file_name(version, ext))
}

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Publishes `bytes` as `dir/{version:020}.{ext}` via conditional create.
pub(crate) fn publish_version(
	dir: &Path,
	version: u64,
	ext: &str,
	bytes: &[u8],
) -> Result<PublishOutcome> {
	fs::create_dir_all(dir)?;
	let final_path = dir.join(version_file_name(version, ext));
	let sequence = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
	let temp_path = dir.join(format!(".tmp-{}-{sequence}-{version:020}.{ext}", std::process::id()));

	let staged = (|| -> Result<()> {
		let mut temp = OpenOptions::new().write(true).create_new(true).open(&temp_path)?;
		temp.write_all(bytes)?;
		temp.sync_all()?;
		Ok(())
	})();
	if let Err(error) = staged {
		let _ = fs::remove_file(&temp_path);
		return Err(error);
	}

	let outcome = match fs::hard_link(&temp_path, &final_path) {
		Ok(()) => {
			let _ = fs::remove_file(&temp_path);
			sync_dir(dir)?;
			Ok(PublishOutcome::Created)
		}
		Err(error) if error.kind() == ErrorKind::AlreadyExists => {
			let same = fs::read(&final_path).map(|existing| existing == bytes).unwrap_or(false);
			let _ = fs::remove_file(&temp_path);
			if same {
				Ok(PublishOutcome::AlreadyExistsSame)
			} else {
				Err(Error::Corruption(format!(
					"authority version {} already exists with different bytes",
					final_path.display()
				)))
			}
		}
		Err(error) => {
			let _ = fs::remove_file(&temp_path);
			Err(error.into())
		}
	};
	outcome
}

fn sync_dir(dir: &Path) -> Result<()> {
	File::open(dir).and_then(|handle| handle.sync_all())?;
	Ok(())
}

/// Every version number present in `dir` for `ext`, ascending. Temp files,
/// foreign extensions and malformed names are ignored exactly as
/// [`latest_version_in`] ignores them, so the two never disagree about what is
/// a version.
pub(crate) fn versions_in(dir: &Path, ext: &str) -> Result<Vec<u64>> {
	let entries = match fs::read_dir(dir) {
		Ok(entries) => entries,
		Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
		Err(error) => return Err(error.into()),
	};
	let suffix = format!(".{ext}");
	let mut versions = Vec::new();
	for entry in entries {
		let entry = entry?;
		let name = entry.file_name();
		let Some(name) = name.to_str() else {
			continue;
		};
		if name.starts_with('.') {
			continue;
		}
		let Some(stem) = name.strip_suffix(&suffix) else {
			continue;
		};
		if stem.len() == 20 {
			if let Ok(version) = stem.parse::<u64>() {
				versions.push(version);
			}
		}
	}
	versions.sort_unstable();
	Ok(versions)
}

/// Deletes all but the newest `keep` versions in `dir`, returning how many files
/// were removed.
///
/// Safe without a minimum age or a reader fence: recovery reads only the newest
/// version of a lineage, and a stale hint resolves by probing FORWARD, so a
/// version below the newest is never needed. A reader that already decoded a
/// version holds its bytes, so unlinking the file cannot affect it.
pub(crate) fn prune_versions(dir: &Path, ext: &str, keep: usize) -> Result<usize> {
	let versions = versions_in(dir, ext)?;
	if versions.len() <= keep {
		return Ok(0);
	}
	let mut removed = 0;
	for &version in &versions[..versions.len() - keep] {
		match fs::remove_file(version_path(dir, version, ext)) {
			Ok(()) => removed += 1,
			// Another sweep raced us to it; the outcome is the same.
			Err(error) if error.kind() == ErrorKind::NotFound => {}
			Err(error) => return Err(error.into()),
		}
	}
	Ok(removed)
}

/// Newest version id present in `dir` for `ext`, ignoring temp files and
/// warning on (not failing over) unparseable names.
pub(crate) fn latest_version_in(dir: &Path, ext: &str) -> Result<Option<u64>> {
	let entries = match fs::read_dir(dir) {
		Ok(entries) => entries,
		Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
		Err(error) => return Err(error.into()),
	};
	let mut latest: Option<u64> = None;
	for entry in entries {
		let entry = entry?;
		let name = entry.file_name();
		let Some(name) = name.to_str() else {
			continue;
		};
		if name.starts_with('.') {
			continue;
		}
		let Some(stem) = name.strip_suffix(&format!(".{ext}")) else {
			continue;
		};
		match stem.parse::<u64>() {
			Ok(version) if stem.len() == 20 => {
				latest = Some(latest.map_or(version, |current| current.max(version)));
			}
			_ => {
				log::warn!("authority: skipping unparseable version file {name}");
			}
		}
	}
	Ok(latest)
}

/// Reads an exact version's bytes; `Ok(None)` only for a missing file.
pub(crate) fn read_version(dir: &Path, version: u64, ext: &str) -> Result<Option<Vec<u8>>> {
	match fs::read(version_path(dir, version, ext)) {
		Ok(bytes) => Ok(Some(bytes)),
		Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
		Err(error) => Err(error.into()),
	}
}

/// Resolve the newest version at or after `hint` by probing forward
/// (SlateDB's consecutive-id pattern), falling back to `hint` itself.
/// Returns `Ok(None)` when not even `hint` exists.
pub(crate) fn resolve_from_hint(
	dir: &Path,
	hint: u64,
	ext: &str,
	max_probes: u32,
) -> Result<Option<(u64, Vec<u8>)>> {
	let mut current: Option<(u64, Vec<u8>)> =
		read_version(dir, hint, ext)?.map(|bytes| (hint, bytes));
	let mut candidate = hint;
	for _ in 0..max_probes {
		candidate += 1;
		match read_version(dir, candidate, ext)? {
			Some(bytes) => current = Some((candidate, bytes)),
			// Versions are consecutive: a missing successor means the
			// current candidate is the latest.
			None => return Ok(current),
		}
	}
	// Probe budget exhausted: fall back to a listing.
	match latest_version_in(dir, ext)? {
		Some(latest) => {
			let bytes = read_version(dir, latest, ext)?.ok_or_else(|| {
				Error::Corruption(format!(
					"authority version {latest} vanished between list and read"
				))
			})?;
			Ok(Some((latest, bytes)))
		}
		None => Ok(current),
	}
}

/// Directory layout for the authority lineages under the store path.
pub(crate) fn catalog_dir(base: &Path) -> PathBuf {
	base.join("catalog")
}

pub(crate) fn root_dir(base: &Path) -> PathBuf {
	base.join("root")
}

pub(crate) fn branch_state_dir(base: &Path, branch: &crate::BranchId) -> PathBuf {
	let mut name = String::with_capacity(32);
	for byte in branch.0 {
		use std::fmt::Write as _;
		let _ = write!(&mut name, "{byte:02x}");
	}
	base.join("branch").join(name)
}
