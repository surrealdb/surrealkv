//! What a scan for markers cannot see, and the image of a directory.
//!
//! A scan for markers finds a marker that is in a file as it was written. It cannot find one in a
//! file that a codec has coded: a zstd table codes the literals of a block, so a table that was
//! written in the clear and compressed can hold every key and value of a database and not one
//! marker in its raw bytes. So that a stray file of that kind cannot pass, the files of an
//! encrypted database are also checked for what they are: every table has the header and the
//! footer of a sealed one, every value-log file the version of a sealed one, every segment of
//! the WAL its header record, every manifest its magic. A file that is none of those must be one
//! of the few that hold no user data, which are named here, and anything else is a finding.
//!
//! The image of a directory is what a refused open is compared against: the name and the bytes
//! of every file, taken before and after.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::{fs, io};

use super::scan::{scan_dir, Needles};
use crate::sstable::table::{ChecksumType, Footer, TABLE_FULL_FOOTER_LENGTH};
use crate::tde::{is_sealed_manifest, CryptoHeader};
use crate::vlog::sealed::is_sealed_start;
use crate::wal::{RecordType, HEADER_SIZE};

/// The files that a database or a checkpoint has and that hold nothing of what a user wrote: the
/// lock, whose only content is the number of a process; the metadata of a checkpoint, which has
/// its time, the sequence number it was taken at and two sizes; and the marker that a restore
/// that was not finished leaves, which has the path of the checkpoint.
pub(crate) const NAMES_WITHOUT_USER_DATA: [&str; 3] =
	["LOCK", "CHECKPOINT_METADATA", "RESTORE_IN_PROGRESS"];

/// What a file of a database is, by its name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
	Table,
	ValueLog,
	Segment,
	Manifest,
	NoUserData,
}

fn class_of(path: &Path) -> Option<Class> {
	let name = path.file_name()?.to_str()?;
	if NAMES_WITHOUT_USER_DATA.contains(&name) {
		Some(Class::NoUserData)
	} else if name.ends_with(".sst") {
		Some(Class::Table)
	} else if name.ends_with(".vlog") {
		Some(Class::ValueLog)
	} else if name.ends_with(".wal") || name.contains(".wal.corrupt") {
		// The copy that a repair keeps of the segment that it replaced is a segment as well.
		Some(Class::Segment)
	} else if name.ends_with(".manifest") {
		Some(Class::Manifest)
	} else {
		None
	}
}

/// Why `bytes`, the contents of a file of class `class`, are not those of a sealed file, if they
/// are not.
fn why_not_sealed(class: Class, bytes: &[u8]) -> Option<String> {
	match class {
		Class::NoUserData => None,
		Class::Table => {
			if !CryptoHeader::has_magic(bytes) {
				return Some("a table without the header of an encrypted file".to_string());
			}
			let footer = bytes
				.len()
				.checked_sub(TABLE_FULL_FOOTER_LENGTH)
				.and_then(|at| Footer::decode(&bytes[at..]).ok());
			match footer {
				Some(footer) if footer.checksum == ChecksumType::SealedCRC32c => None,
				Some(_) => Some("a table whose footer does not say that it is sealed".to_string()),
				None => Some("a table without a footer".to_string()),
			}
		}
		Class::ValueLog => {
			(!is_sealed_start(bytes)).then(|| "a value-log file that is not sealed".to_string())
		}
		Class::Segment => {
			// A segment that a crash left empty holds nothing. Any other starts with the record
			// that has the header of its encryption.
			let sealed = bytes.is_empty()
				|| (bytes.len() > HEADER_SIZE
					&& bytes[HEADER_SIZE - 1] == RecordType::SegmentHeader as u8
					&& CryptoHeader::has_magic(&bytes[HEADER_SIZE..]));
			(!sealed).then(|| "a WAL segment that does not start with a sealed header".to_string())
		}
		Class::Manifest => {
			(!is_sealed_manifest(bytes)).then(|| "a manifest that is not sealed".to_string())
		}
	}
}

/// What is wrong with the files under `root`, which is a database or a checkpoint of one that is
/// encrypted: each file that is not sealed, and each file that is none of the kinds that a
/// database has and is not one of those that hold no user data. Empty if all is as it should be.
pub(crate) fn unsealed_files(root: &Path) -> io::Result<Vec<String>> {
	let nothing = Needles::new(Vec::new());
	let scan = scan_dir(root, &nothing)?;
	let mut wrong = Vec::new();
	for file in &scan.files {
		let place = file.path.display();
		match class_of(&file.path) {
			None => wrong.push(format!("{place}: a file of no kind that a database has")),
			Some(class) => {
				let bytes = fs::read(root.join(&file.path))?;
				if let Some(why) = why_not_sealed(class, &bytes) {
					wrong.push(format!("{place}: {why}"));
				}
			}
		}
	}
	Ok(wrong)
}

/// Fails the test, saying which files, if `root` holds a file that is not sealed.
pub(crate) fn assert_every_file_is_sealed(root: &Path, what: &str) {
	let wrong = unsealed_files(root).unwrap();
	assert!(wrong.is_empty(), "{what}: {} file(s) are not sealed: {wrong:?}", wrong.len());
}

// ---------------------------------------------------------------------------
// the image of a directory
// ---------------------------------------------------------------------------

/// Every directory and every file under a root, with the bytes of each file.
///
/// The lock file is the one file that is left out: its content is the number of the process that
/// holds it, which every open writes again, and it is made by the first open of a directory.
/// A link is recorded by where it points and not followed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DirImage {
	dirs: BTreeSet<PathBuf>,
	files: BTreeMap<PathBuf, Vec<u8>>,
	links: BTreeMap<PathBuf, PathBuf>,
}

impl DirImage {
	pub(crate) fn of(root: &Path) -> Self {
		let mut image = Self {
			dirs: BTreeSet::new(),
			files: BTreeMap::new(),
			links: BTreeMap::new(),
		};
		image.visit(root, Path::new(""));
		image
	}

	fn visit(&mut self, dir: &Path, relative: &Path) {
		for entry in fs::read_dir(dir).unwrap() {
			let entry = entry.unwrap();
			let path = entry.path();
			let relative = relative.join(entry.file_name());
			let kind = entry.file_type().unwrap();
			if kind.is_symlink() {
				self.links.insert(relative, fs::read_link(&path).unwrap());
			} else if kind.is_dir() {
				self.dirs.insert(relative.clone());
				self.visit(&path, &relative);
			} else if relative.file_name().is_some_and(|name| name == "LOCK") {
				continue;
			} else {
				self.files.insert(relative, fs::read(&path).unwrap());
			}
		}
	}

	pub(crate) fn file_count(&self) -> usize {
		self.files.len()
	}

	pub(crate) fn names(&self) -> Vec<String> {
		self.files.keys().map(|path| path.to_string_lossy().replace('\\', "/")).collect()
	}

	/// The bytes of a file, by its path relative to the root.
	pub(crate) fn file(&self, relative: &str) -> Option<&Vec<u8>> {
		self.files.get(Path::new(relative))
	}

	/// What differs from `before`: the names of the files and directories that were added, that
	/// went, and whose bytes changed. Empty if the two are the same.
	pub(crate) fn changes_since(&self, before: &Self) -> Vec<String> {
		let mut changes = Vec::new();
		for dir in before.dirs.difference(&self.dirs) {
			changes.push(format!("directory removed: {}", dir.display()));
		}
		for dir in self.dirs.difference(&before.dirs) {
			changes.push(format!("directory added: {}", dir.display()));
		}
		for (path, old) in &before.files {
			match self.files.get(path) {
				None => changes.push(format!("file removed or renamed: {}", path.display())),
				Some(new) if new != old => changes.push(format!(
					"file changed: {} ({} bytes became {})",
					path.display(),
					old.len(),
					new.len()
				)),
				Some(_) => {}
			}
		}
		for path in self.files.keys().filter(|path| !before.files.contains_key(*path)) {
			changes.push(format!("file added: {}", path.display()));
		}
		if self.links != before.links {
			changes.push("a link changed".to_string());
		}
		changes
	}

	/// Fails the test, saying what changed, if the directory is not what `before` was.
	pub(crate) fn assert_unchanged_since(&self, before: &Self, what: &str) {
		let changes = self.changes_since(before);
		assert!(changes.is_empty(), "{what}: the directory changed: {changes:?}");
	}
}

/// The files of the directory `dir` of `root` with the extension `ext`, by name.
pub(crate) fn files_of(root: &Path, dir: &str, ext: &str) -> Vec<PathBuf> {
	let mut files: Vec<PathBuf> = fs::read_dir(root.join(dir))
		.unwrap()
		.map(|entry| entry.unwrap().path())
		.filter(|path| path.extension().is_some_and(|found| found == ext))
		.collect();
	files.sort();
	files
}

/// The last of the files of `files_of`.
pub(crate) fn last_file(root: &Path, dir: &str, ext: &str) -> PathBuf {
	files_of(root, dir, ext).pop().unwrap_or_else(|| panic!("no .{ext} file in {dir}"))
}

/// Copies the files and directories of `from` to `to`, which is made. The lock is not copied.
pub(crate) fn copy_tree(from: &Path, to: &Path) {
	fs::create_dir_all(to).unwrap();
	for entry in fs::read_dir(from).unwrap() {
		let entry = entry.unwrap();
		if entry.file_name() == "LOCK" {
			continue;
		}
		let target = to.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_tree(&entry.path(), &target);
		} else {
			fs::copy(entry.path(), target).unwrap();
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use tempfile::TempDir;

	use super::*;
	use crate::tde::{CipherSuite, SoftwareKeyManager};
	use crate::{Options, Tree};

	const ROOT: [u8; 32] = [0x77; 32];

	/// A database in `path` that holds a table, a value-log file, a segment of the WAL and a
	/// manifest, and is closed. It is encrypted if `suite` is given.
	async fn database(path: &Path, suite: Option<CipherSuite>) {
		let opts = Options {
			path: path.to_path_buf(),
			enable_vlog: true,
			vlog_value_threshold: 64,
			flush_on_close: false,
			..Default::default()
		};
		let opts = match suite {
			Some(suite) => opts.with_encryption(Arc::new(SoftwareKeyManager::new(ROOT)), suite),
			None => opts,
		};
		let tree = Tree::new(Arc::new(opts)).unwrap();
		let mut txn = tree.begin().unwrap();
		for i in 0..4 {
			txn.set(format!("key-{i}").into_bytes(), vec![b'v'; 100]).unwrap();
		}
		txn.commit().await.unwrap();
		tree.flush().unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set(b"in-the-log".to_vec(), vec![b'w'; 100]).unwrap();
		txn.commit().await.unwrap();
		tree.close().await.unwrap();
	}

	fn classes(root: &Path) -> BTreeSet<String> {
		DirImage::of(root)
			.names()
			.iter()
			.filter_map(|name| class_of(Path::new(name)))
			.map(|class| format!("{class:?}"))
			.collect()
	}

	#[tokio::test]
	async fn every_file_of_an_encrypted_database_is_sealed_and_the_check_sees_every_kind_of_file() {
		for suite in [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305] {
			let dir = TempDir::new().unwrap();
			database(dir.path(), Some(suite)).await;
			assert_eq!(unsealed_files(dir.path()).unwrap(), Vec::<String>::new(), "{suite:?}");
			assert_every_file_is_sealed(dir.path(), "a database");
			// The check had a file of each kind to look at, which is what makes it mean something.
			let seen = classes(dir.path());
			for kind in ["ValueLog", "Segment", "Manifest", "Table"] {
				assert!(seen.contains(kind), "{suite:?}: no {kind} file was looked at: {seen:?}");
			}
		}
	}

	#[tokio::test]
	async fn the_files_of_a_database_that_is_not_encrypted_are_each_found_wanting() {
		let dir = TempDir::new().unwrap();
		database(dir.path(), None).await;
		let wrong = unsealed_files(dir.path()).unwrap();
		for kind in [".sst", ".vlog", ".wal", ".manifest"] {
			assert!(
				wrong.iter().any(|line| line.contains(kind)),
				"nothing was said of a {kind} file: {wrong:?}"
			);
		}
		// The lock is the one file that holds no user data, and it is not found wanting.
		assert!(wrong.iter().all(|line| !line.contains("LOCK")), "{wrong:?}");
	}

	#[tokio::test]
	async fn one_file_that_is_in_the_clear_or_of_no_known_kind_is_the_only_one_found() {
		let sealed = TempDir::new().unwrap();
		database(sealed.path(), Some(CipherSuite::Aes256Gcm)).await;
		let plain = TempDir::new().unwrap();
		database(plain.path(), None).await;
		let image = DirImage::of(sealed.path());
		let plain_image = DirImage::of(plain.path());

		// For each kind: the last file of that kind replaced by the last plaintext one of a
		// database that is not encrypted, which holds something: the first segment of a WAL
		// may be empty, and an empty one is let through. A file of the sealed one is found
		// wanting for each of them, and only it.
		for (place, kind) in
			[("sstables", ".sst"), ("vlog", ".vlog"), ("wal", ".wal"), ("manifest", ".manifest")]
		{
			let name = |image: &DirImage| {
				image
					.names()
					.into_iter()
					.rev()
					.find(|name| name.starts_with(place) && name.ends_with(kind))
					.unwrap_or_else(|| panic!("no {kind} file"))
			};
			let (ours, theirs) = (name(&image), name(&plain_image));
			let copy = TempDir::new().unwrap();
			copy_tree(sealed.path(), copy.path());
			fs::write(copy.path().join(&ours), plain_image.file(&theirs).unwrap()).unwrap();
			let wrong = unsealed_files(copy.path()).unwrap();
			assert_eq!(wrong.len(), 1, "{kind}: {wrong:?}");
			assert!(wrong[0].starts_with(&ours), "{kind}: {wrong:?}");
		}

		// A file with a name that no database gives a file is found, whatever is in it.
		let copy = TempDir::new().unwrap();
		copy_tree(sealed.path(), copy.path());
		fs::write(copy.path().join("wal").join("notes.txt"), b"nothing").unwrap();
		fs::write(copy.path().join("sstables").join("00000000000000000042.sst.old"), b"x").unwrap();
		let wrong = unsealed_files(copy.path()).unwrap();
		assert_eq!(wrong.len(), 2, "{wrong:?}");
		assert!(wrong.iter().all(|line| line.contains("no kind")), "{wrong:?}");

		// The names that hold no user data are let through, and a segment that a crash left empty.
		let copy = TempDir::new().unwrap();
		copy_tree(sealed.path(), copy.path());
		for name in NAMES_WITHOUT_USER_DATA {
			fs::write(copy.path().join(name), b"123\n").unwrap();
		}
		fs::write(copy.path().join("wal").join("00000000000000000099.wal"), b"").unwrap();
		assert!(unsealed_files(copy.path()).unwrap().is_empty());
	}

	#[test]
	fn an_image_of_a_directory_sees_what_changed_and_what_did_not() {
		let dir = TempDir::new().unwrap();
		fs::create_dir_all(dir.path().join("a/b")).unwrap();
		fs::write(dir.path().join("a/one"), b"1").unwrap();
		fs::write(dir.path().join("a/b/two"), b"22").unwrap();
		fs::write(dir.path().join("LOCK"), b"4711\n").unwrap();
		let before = DirImage::of(dir.path());
		assert_eq!(before.file_count(), 2, "the lock is not part of an image");
		assert_eq!(before.names(), vec!["a/b/two".to_string(), "a/one".to_string()]);
		before.assert_unchanged_since(&DirImage::of(dir.path()), "nothing happened");

		// The lock is written again by every open, and is not a change.
		fs::write(dir.path().join("LOCK"), b"4712\n").unwrap();
		assert!(DirImage::of(dir.path()).changes_since(&before).is_empty());

		// One byte, a new file, a new directory, a removed file, a renamed file.
		fs::write(dir.path().join("a/one"), b"2").unwrap();
		assert_eq!(
			DirImage::of(dir.path()).changes_since(&before),
			vec!["file changed: a/one (1 bytes became 1)"
				.replace('/', std::path::MAIN_SEPARATOR_STR)]
		);
		fs::write(dir.path().join("a/one"), b"1").unwrap();
		fs::create_dir_all(dir.path().join("c")).unwrap();
		fs::write(dir.path().join("c/new"), b"").unwrap();
		fs::rename(dir.path().join("a/b/two"), dir.path().join("a/b/three")).unwrap();
		let changes = DirImage::of(dir.path()).changes_since(&before);
		assert_eq!(changes.len(), 4, "{changes:?}");
		assert!(changes.iter().any(|change| change.starts_with("directory added")));
		assert!(changes
			.iter()
			.any(|change| change.starts_with("file added") && change.contains("new")));
		assert!(changes
			.iter()
			.any(|change| change.starts_with("file added") && change.contains("three")));
		assert!(changes
			.iter()
			.any(|change| change.starts_with("file removed") && change.contains("two")));
	}
}
