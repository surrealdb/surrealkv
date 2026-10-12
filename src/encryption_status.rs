//! Which keys cover which files of a database, from the clear headers of its files.
//!
//! The question behind it is the one an operator asks before destroying a key: is anything still
//! sealed under it? The answer is the count of files and bytes under each key id, by kind of file,
//! read from the start of each file and from nothing else: no block is read or opened, and no key
//! is needed, so it can be asked of a database that is running and of one that is not, by the
//! engine and by a tool that has no key.
//!
//! A file is counted under the key id that its own header names, and only the files that have the
//! names that the engine gives them are counted, so a copy that was kept aside under another name
//! is mentioned and not counted. Everything that is not as it should be is reported and not
//! hidden: a file in the clear in an encrypted database, a file of another database, a header that
//! cannot be read.

use std::collections::BTreeMap;
use std::path::Path;

use crate::describe::{kind_from_name, CryptoSummary, FileDescription, FileState};
use crate::error::{Error, Result};
use crate::tde::{CipherSuite, DbId, FileKind, KeyManager, KeyState};
use crate::Options;

/// How many files, and how many bytes in them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct FileTotals {
	/// The number of files.
	pub files: u64,
	/// Their size in bytes, as the file system says it.
	pub bytes: u64,
}

impl FileTotals {
	fn add(&mut self, bytes: u64) {
		self.files += 1;
		self.bytes = self.bytes.saturating_add(bytes);
	}

	fn sum(&self, other: &Self) -> Self {
		Self {
			files: self.files + other.files,
			bytes: self.bytes.saturating_add(other.bytes),
		}
	}
}

/// What one key id covers: the files whose headers name it, by kind of file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct KeyUsage {
	/// The key id.
	pub key_id: u32,
	/// What the key manager says that the key may be used for. `None` when no key manager was
	/// asked, as in a report that is made from the files alone.
	pub state: Option<KeyState>,
	/// The manifest, if it was sealed under this key: the key that was active when it was last
	/// written.
	pub manifest: FileTotals,
	/// The tables.
	pub tables: FileTotals,
	/// The files of the value log.
	pub value_log: FileTotals,
	/// The segments of the write-ahead log.
	pub wal: FileTotals,
}

impl KeyUsage {
	fn new(key_id: u32, state: Option<KeyState>) -> Self {
		Self {
			key_id,
			state,
			manifest: FileTotals::default(),
			tables: FileTotals::default(),
			value_log: FileTotals::default(),
			wal: FileTotals::default(),
		}
	}

	/// The files of every kind together.
	pub fn total(&self) -> FileTotals {
		self.manifest.sum(&self.tables).sum(&self.value_log).sum(&self.wal)
	}

	/// Whether no file of the database is sealed under the key, the manifest included. This is
	/// one of the conditions for destroying the key, not the only one: see [`EncryptionStatus`].
	pub fn covers_no_files(&self) -> bool {
		self.total().files == 0
	}

	fn of_kind(&mut self, kind: FileKind) -> &mut FileTotals {
		match kind {
			FileKind::Manifest => &mut self.manifest,
			FileKind::Sst => &mut self.tables,
			FileKind::Vlog => &mut self.value_log,
			FileKind::Wal => &mut self.wal,
		}
	}
}

/// The encryption of a database, and which keys cover which of its files.
///
/// It is computed from the headers at the start of the files, without a key, and without reading
/// a block of any of them. It says what is on disk now: files that the engine has not yet removed
/// are counted, and a file that is being written is counted once its header is.
///
/// # Retiring a key
///
/// A key may be destroyed only when all of these hold, and this says the first two:
///
/// - It covers no file: [`KeyUsage::covers_no_files`], which includes the manifest. The manifest is
///   rewritten under the active key by the next write that changes it, so a key that has been made
///   inactive still covers the manifest until then, and `manifest_key_id` says it.
/// - It is not the active key, which new files are written under.
/// - No checkpoint and no backup of the database depends on it. A checkpoint is a copy of the
///   files, hard linked where it can be, under the keys that they were written with, and this does
///   not look at any.
///
/// A file whose header cannot be read, `unattributed`, may be under any key, so a count that has
/// some is not a proof that a key is unused. A key that is destroyed while a file needs it makes
/// that file unreadable, and there is no way back.
///
/// Key ids that no file names are not listed, except the active one: a key manager has no way
/// to say which ids it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct EncryptionStatus {
	/// Whether the database is encrypted.
	pub encrypted: bool,
	/// The suite that the database was created with, which new files use.
	pub suite: Option<CipherSuite>,
	/// The identity of the database.
	pub db_id: Option<DbId>,
	/// The key id that new files are written under. `None` when no key manager was asked.
	pub active_key_id: Option<u32>,
	/// The key id that the manifest was last sealed under.
	pub manifest_key_id: Option<u32>,
	/// Whether the manifest is sealed under the key that is active: when it is not, the manifest
	/// still depends on an older key, which cannot be retired until the manifest is written again.
	/// False when no key manager was asked.
	pub manifest_key_is_active: bool,
	/// What each key id that a file names covers, and the active key, in order of key id.
	pub keys: Vec<KeyUsage>,
	/// The files whose header cannot be read, or that are empty or end inside it, which hold what
	/// a key may have been used for and cannot be said to be under any.
	pub unattributed: FileTotals,
	/// The files in the clear in a database that is encrypted. In a database that is not
	/// encrypted, none are counted: they are what it is made of.
	pub plaintext: FileTotals,
	/// What is not as it should be: a file in the clear in an encrypted database, a file of
	/// another database, a file of another suite, a file whose header names another file than its
	/// name does, a header that cannot be read. Empty for a database that is consistent.
	pub warnings: Vec<String>,
	/// What is worth knowing and is not wrong: files that were kept aside and are not counted, and
	/// files that a write that did not finish left.
	pub notes: Vec<String>,
}

impl EncryptionStatus {
	/// The status of a database that is not encrypted.
	pub(crate) fn not_encrypted() -> Self {
		Self {
			encrypted: false,
			suite: None,
			db_id: None,
			active_key_id: None,
			manifest_key_id: None,
			manifest_key_is_active: false,
			keys: Vec::new(),
			unattributed: FileTotals::default(),
			plaintext: FileTotals::default(),
			warnings: Vec::new(),
			notes: Vec::new(),
		}
	}

	/// What `key_id` covers, if any file names it or it is the active key.
	pub fn key(&self, key_id: u32) -> Option<&KeyUsage> {
		self.keys.iter().find(|usage| usage.key_id == key_id)
	}

	/// Whether nothing was found that is not as it should be.
	pub fn is_consistent(&self) -> bool {
		self.warnings.is_empty()
	}
}

/// The directories of a database with the files in them that are counted, and the kind of file that
/// each holds: the extension of the name, and the number of digits of the id that comes before it.
const SURFACES: [(&str, FileKind, &str); 3] = [
	("sstables", FileKind::Sst, "sst"),
	("vlog", FileKind::Vlog, "vlog"),
	("wal", FileKind::Wal, "wal"),
];

/// The id in the name of a file that has the name that the engine gives: twenty digits, a dot and
/// `extension`. Any other name is a file that was put there by something else.
fn id_in_name(name: &str, extension: &str) -> Option<u64> {
	let digits = name.strip_suffix(extension)?.strip_suffix('.')?;
	if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
		return None;
	}
	digits.parse().ok()
}

/// What the files of a database have to be consistent with: its identity and its suite.
struct Expected {
	db_id: DbId,
	suite: CipherSuite,
}

/// The state that is accumulated while the files are read.
struct Tally<'a> {
	key_manager: Option<&'a dyn KeyManager>,
	keys: BTreeMap<u32, KeyUsage>,
	unattributed: FileTotals,
	plaintext: FileTotals,
	warnings: Vec<String>,
	notes: Vec<String>,
}

impl Tally<'_> {
	fn usage(&mut self, key_id: u32) -> &mut KeyUsage {
		let state = self.key_manager.map(|manager| manager.key_state(key_id));
		self.keys.entry(key_id).or_insert_with(|| KeyUsage::new(key_id, state))
	}
}

/// Describes `path` for the report, which a file that has gone is not: it was removed between
/// the listing of its directory and the look at it, which is what a database that is running does.
fn describe_present(path: &Path) -> Result<Option<FileDescription>> {
	match crate::describe::describe_file(path) {
		Ok(description) => Ok(Some(description)),
		Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
		Err(error) => Err(error),
	}
}

/// Counts one file that has the name that the engine gives, of the kind `kind` and the id `id`.
fn count_file(
	tally: &mut Tally<'_>,
	expected: Option<&Expected>,
	kind: FileKind,
	id: u64,
	label: &str,
	description: &FileDescription,
) {
	match &description.state {
		FileState::Sealed(summary) => {
			attribute(tally, expected, kind, id, label, summary, description.file_size);
		}
		FileState::Plaintext => {
			if expected.is_some() {
				tally.plaintext.add(description.file_size);
				tally.warnings.push(format!("{label} is in the clear in an encrypted database"));
			}
		}
		FileState::Damaged(error) => {
			tally.unattributed.add(description.file_size);
			tally.warnings.push(format!("{label}: its encryption header cannot be read: {error}"));
		}
		// In a database that is not encrypted an empty file is no concern of keys.
		FileState::Unfinished if expected.is_some() => {
			tally.unattributed.add(description.file_size);
			tally.notes.push(format!(
				"{label} is empty or ends inside its header, which is what a write that did not \
				 finish leaves"
			));
		}
		FileState::Unfinished => {}
	}
}

/// Counts a sealed file under the key that its header names, and says what in it is not as it
/// should be for the database that `expected` describes.
fn attribute(
	tally: &mut Tally<'_>,
	expected: Option<&Expected>,
	kind: FileKind,
	id: u64,
	label: &str,
	summary: &CryptoSummary,
	size: u64,
) {
	match expected {
		None => {
			tally.warnings.push(format!("{label} is sealed, in a database that is not encrypted"))
		}
		Some(expected) => {
			if summary.db_id != expected.db_id {
				tally
					.warnings
					.push(format!("{label} belongs to another database, {}", summary.db_id));
			}
			if summary.suite != expected.suite {
				tally.warnings.push(format!(
					"{label} is sealed with {:?}, and the database with {:?}",
					summary.suite, expected.suite
				));
			}
		}
	}
	if kind != FileKind::Manifest && summary.file_id != id {
		tally.warnings.push(format!(
			"{label} is named for {id}, and its header says that it is {}",
			summary.file_id
		));
	}
	tally.usage(summary.key_id).of_kind(kind).add(size);
}

/// The report of the database at `root`, from the headers of its files.
///
/// With `key_manager` the state of each key id is given, and the active key is listed. `identity`
/// is the database that the files must belong to, when the caller knows it, and otherwise it is the
/// one that the manifest names. Reads only the start of each file, and of a table its footer.
pub(crate) fn scan_database(
	root: &Path,
	key_manager: Option<&dyn KeyManager>,
	identity: Option<(DbId, CipherSuite)>,
) -> Result<EncryptionStatus> {
	let opts = Options {
		path: root.to_path_buf(),
		..Default::default()
	};
	let manifest_path = opts.manifest_file_path(0);
	let Some(manifest) = describe_present(&manifest_path)? else {
		return Err(Error::InvalidArgument(format!(
			"{} has no manifest, so it is not a database directory",
			root.display()
		)));
	};

	let manifest_summary = match &manifest.state {
		FileState::Sealed(summary) => Some(*summary),
		FileState::Plaintext => None,
		FileState::Damaged(error) => return Err(error.clone().into()),
		FileState::Unfinished => {
			return Err(Error::ManifestCorruption(
				"the manifest is empty or ends inside its header".to_string(),
			))
		}
	};
	let Some(manifest_summary) = manifest_summary else {
		let mut status = EncryptionStatus::not_encrypted();
		// A file that is sealed in a database that is not encrypted is not as it should be, which
		// is for the walk of the files to say.
		walk_files(root, &mut Tally::new(None), None, &mut status)?;
		return Ok(status);
	};

	let expected = identity
		.map(|(db_id, suite)| Expected {
			db_id,
			suite,
		})
		.unwrap_or(Expected {
			db_id: manifest_summary.db_id,
			suite: manifest_summary.suite,
		});

	let mut tally = Tally::new(key_manager);
	if let Some(manager) = key_manager {
		tally.usage(manager.active_key_id());
	}
	count_file(&mut tally, Some(&expected), FileKind::Manifest, 0, "the manifest", &manifest);
	let mut status = EncryptionStatus::not_encrypted();
	status.encrypted = true;
	status.suite = Some(expected.suite);
	status.db_id = Some(expected.db_id);
	status.manifest_key_id = Some(manifest_summary.key_id);
	walk_files(root, &mut tally, Some(&expected), &mut status)?;
	status.active_key_id = key_manager.map(|manager| manager.active_key_id());
	status.manifest_key_is_active = status.active_key_id == status.manifest_key_id;
	Ok(status)
}

impl<'a> Tally<'a> {
	fn new(key_manager: Option<&'a dyn KeyManager>) -> Self {
		Self {
			key_manager,
			keys: BTreeMap::new(),
			unattributed: FileTotals::default(),
			plaintext: FileTotals::default(),
			warnings: Vec::new(),
			notes: Vec::new(),
		}
	}
}

/// Reads the files of the three directories of the database at `root` into `tally`, and the tally
/// into `status`.
fn walk_files(
	root: &Path,
	tally: &mut Tally<'_>,
	expected: Option<&Expected>,
	status: &mut EncryptionStatus,
) -> Result<()> {
	for (directory, kind, extension) in SURFACES {
		let dir = root.join(directory);
		let mut names: Vec<(String, std::path::PathBuf)> = Vec::new();
		match std::fs::read_dir(&dir) {
			Ok(entries) => {
				for entry in entries {
					let entry = entry?;
					// A directory, such as the place that a repair of the log works in, is not a
					// file of the database.
					if entry.file_type()?.is_file() {
						names
							.push((entry.file_name().to_string_lossy().into_owned(), entry.path()));
					}
				}
			}
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
			Err(error) => return Err(error.into()),
		}
		names.sort();

		for (name, path) in names {
			let label = format!("{directory}/{name}");
			let Some(description) = describe_present(&path)? else {
				continue;
			};
			match id_in_name(&name, extension) {
				Some(id) => count_file(tally, expected, kind, id, &label, &description),
				None => note_other_file(tally, expected.is_some(), &label, &path, &description),
			}
		}
	}

	status.keys = tally.keys.values().copied().collect();
	status.unattributed = tally.unattributed;
	status.plaintext = tally.plaintext;
	status.warnings = std::mem::take(&mut tally.warnings);
	status.notes = std::mem::take(&mut tally.notes);
	Ok(())
}

/// Says what a file in one of the directories is that does not have a name that the engine
/// gives: a copy that was kept aside, such as a segment of the log whose repair kept the
/// original, a value-log file that was set aside, or something that was put there by hand. None
/// is counted.
fn note_other_file(
	tally: &mut Tally<'_>,
	encrypted: bool,
	label: &str,
	path: &Path,
	description: &FileDescription,
) {
	// In a database that is not encrypted, only what is sealed is worth a word.
	if !encrypted && !description.is_encrypted() {
		return;
	}
	let aside = kind_from_name(path).is_some();
	let what = match &description.state {
		FileState::Sealed(summary) => format!("sealed under key {}", summary.key_id),
		FileState::Plaintext => "in the clear".to_string(),
		FileState::Damaged(error) => {
			format!("with an encryption header that cannot be read: {error}")
		}
		FileState::Unfinished => "empty or cut short".to_string(),
	};
	let kept = if aside {
		"a copy that was kept aside"
	} else {
		"a file that is not one of the database's"
	};
	tally.notes.push(format!("{label} is {kept}, {what}, and is not counted"));
}
