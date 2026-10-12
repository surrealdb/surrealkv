//! Key managers: where root keys come from, and whether each one may still be used.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use parking_lot::RwLock;
use zeroize::{Zeroize, Zeroizing};

use super::KEY_ID_DEFAULT;
use crate::error::{EncryptionError, Error};

/// Length in bytes of a root key.
pub const ROOT_KEY_LEN: usize = 32;

/// What a key id may be used for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum KeyState {
	/// The key new files are written under. It can also read.
	Active,
	/// The key was active once. Files written under it can still be read, but no new file is
	/// written under it.
	DecryptOnly,
	/// The key was destroyed, so the files written under it can no longer be read.
	Retired,
	/// The manager has never held this key id.
	Unknown,
}

impl KeyState {
	/// Whether files written under a key in this state can be read.
	pub const fn can_read(self) -> bool {
		matches!(self, Self::Active | Self::DecryptOnly)
	}

	/// Whether a new file may be written under a key in this state.
	pub const fn can_write(self) -> bool {
		matches!(self, Self::Active)
	}
}

/// Provider of root keys, one per key id.
///
/// The engine never encrypts with a root key. For every file it draws a fresh random salt and
/// derives a key of its own from the root, the salt and the identity of the file, so a manager
/// cannot cause two files to share a key, however its roots are chosen. What a manager owns is
/// which root belongs to which id and what each id may still be used for.
///
/// # Blocking
///
/// `active_key_id`, `key_state` and `root_key` are called when a WAL segment, a value-log file
/// or an SSTable is started, which is on the commit path, so they must answer from memory and
/// never wait for a network or a disk. A manager that fetches its roots from a provider does
/// that in [`prepare`](Self::prepare), which the engine calls when a database is opened and
/// again when the set of key ids in use changes, and serves `root_key` from the cache that
/// `prepare` filled. A `root_key` call for an id that was not prepared answers
/// `EncryptionError::KeyProvider(KeyProviderFailure::Unavailable)`, which fails that one
/// operation and may be retried; it never stops the database.
///
/// # Stability
///
/// The root returned for an id must be the same bytes for as long as the id exists. A manager
/// that changes the bytes behind an id makes every file written under it unreadable.
pub trait KeyManager: Send + Sync {
	/// The key id new files are written under.
	fn active_key_id(&self) -> u32;

	/// What `key_id` may be used for. An id the manager has never held is
	/// [`KeyState::Unknown`].
	fn key_state(&self, key_id: u32) -> KeyState;

	/// The root key for `key_id`, as a copy that is wiped when dropped.
	///
	/// Fails with `UnknownKeyId` for an id the manager has never held and `KeyRetired` for one
	/// whose key was destroyed. Must not block, see the trait documentation.
	fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError>;

	/// Makes the roots of `key_ids` available to `root_key`, doing whatever slow work that takes.
	///
	/// The default checks that each id is usable and that its root can be read, which is all an
	/// in-memory manager needs. A manager backed by a provider overrides it to fetch and cache
	/// the roots, and to report ids the provider does not know.
	fn prepare(&self, key_ids: &[u32]) -> Result<(), EncryptionError> {
		for &key_id in key_ids {
			match self.key_state(key_id) {
				KeyState::Active | KeyState::DecryptOnly => {}
				KeyState::Retired => return Err(EncryptionError::KeyRetired),
				KeyState::Unknown => return Err(EncryptionError::UnknownKeyId),
			}
			self.root_key(key_id)?;
		}
		Ok(())
	}
}

/// One key id of a [`SoftwareKeyManager`].
struct Slot {
	state: KeyState,
	/// The only copy of the root the manager owns, `None` once the key is retired. Boxed so that
	/// the map growing never copies the bytes to a place that is then freed without being wiped.
	root: Option<Box<Zeroizing<[u8; ROOT_KEY_LEN]>>>,
}

impl Slot {
	fn new(state: KeyState, root: &[u8; ROOT_KEY_LEN]) -> Self {
		let mut owned = Box::new(Zeroizing::new([0u8; ROOT_KEY_LEN]));
		owned.copy_from_slice(root);
		Self {
			state,
			root: Some(owned),
		}
	}
}

struct Inner {
	active: u32,
	keys: HashMap<u32, Slot>,
}

/// The longest a key file may be, read or not: a key in hex, with room for the line ending that
/// an editor adds. A longer file is not a key file, and is refused without being read to its end.
pub const KEY_FILE_MAX_LEN: usize = 256;

/// In-memory key manager with key rotation: the reference implementation of [`KeyManager`], for
/// tests and for simple deployments.
///
/// It keeps every root it is given in memory, so whoever runs it has to bring the keys from
/// somewhere ([`from_key_file`](Self::from_key_file) reads one from a file, which is what the
/// command line tool does) and has to protect that place. A deployment that keeps its roots in a
/// key management service implements [`KeyManager`] over it instead: the trait is the interface
/// of the engine, and this type is what a test or a small deployment uses in its place.
///
/// Roots are held in memory that is wiped when a key is retired or the manager is dropped. They
/// are never part of `Debug` output, which says which key ids exist and what each may be used
/// for, and nothing more.
///
/// A key is not recoverable from the database: whoever loses every copy of the root that a
/// database was created with has lost the database.
pub struct SoftwareKeyManager {
	inner: RwLock<Inner>,
}

// Written by hand because a derived `Debug` prints the key bytes, which then end up in logs
// and panic messages.
impl fmt::Debug for SoftwareKeyManager {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let inner = self.inner.read();
		let mut keys: Vec<(u32, KeyState)> =
			inner.keys.iter().map(|(id, slot)| (*id, slot.state)).collect();
		keys.sort_unstable_by_key(|(id, _)| *id);
		f.debug_struct("SoftwareKeyManager")
			.field("active_key_id", &inner.active)
			.field("keys", &keys)
			.finish_non_exhaustive()
	}
}

impl SoftwareKeyManager {
	/// Creates a manager whose only key is `initial_key`, under key id [`KEY_ID_DEFAULT`], active.
	///
	/// The array is passed by value, so the caller's own copy is theirs to wipe.
	pub fn new(mut initial_key: [u8; ROOT_KEY_LEN]) -> Self {
		let mut keys = HashMap::new();
		keys.insert(KEY_ID_DEFAULT, Slot::new(KeyState::Active, &initial_key));
		initial_key.zeroize();
		Self {
			inner: RwLock::new(Inner {
				active: KEY_ID_DEFAULT,
				keys,
			}),
		}
	}

	/// Creates a manager from a key that is written down: 32 raw bytes, or 64 hex digits that may
	/// be surrounded by white space, such as the line ending of a text file. Its only key is that
	/// key, under key id [`KEY_ID_DEFAULT`], active.
	///
	/// Exactly 32 bytes are taken as they are, whatever they are, so a key may end in a byte that
	/// is a space. The one thing refused among them is 32 hex digits: a hex key that is half the
	/// length of a root key is much more likely a mistake than a root key made of digits, and
	/// accepting it would give a database a key of 128 bits at the most. Anything else, in length
	/// or in content, is `EncryptionError::InvalidInput`, and no error ever repeats what it was
	/// given.
	pub fn from_key_bytes(bytes: &[u8]) -> Result<Self, EncryptionError> {
		decode_key(bytes).map(|key| Self::new(*key))
	}

	/// Creates a manager from a key in hex, 64 digits in either case, which may be surrounded by
	/// white space. Its only key is that key, under key id [`KEY_ID_DEFAULT`], active.
	pub fn from_hex(hex: &str) -> Result<Self, EncryptionError> {
		decode_hex(hex.as_bytes()).map(|key| Self::new(*key))
	}

	/// Creates a manager from the key in the file at `path`, see
	/// [`from_key_bytes`](Self::from_key_bytes) for what it may hold.
	///
	/// Where the file system has permissions of the Unix kind, a file that every user can read or
	/// write is refused: a key that is protected by being stored must at least not be given to
	/// everyone. A file that only the owner and a group can reach is accepted, since sharing a key
	/// between the processes of one group is how a service is usually deployed. The file is
	/// opened once and everything is decided from what was opened, and at most
	/// [`KEY_FILE_MAX_LEN`] bytes of it are read, into memory that is wiped. A file that is not a
	/// regular file, such as a directory, is refused before it is read.
	///
	/// Fails with `Error::Io` if the file cannot be read, `Error::InvalidArgument` if it is not a
	/// regular file or can be reached by every user, and `Error::Encryption` with
	/// `InvalidInput` if what it holds is not a key. None of the messages holds a byte of the
	/// file.
	pub fn from_key_file(path: impl AsRef<Path>) -> crate::error::Result<Self> {
		read_key_file(path.as_ref(), true).map(|key| Self::new(*key))
	}

	/// Like [`from_key_file`](Self::from_key_file), but without the check of who can reach the
	/// file, for a file system whose permissions say nothing about that, or for a key that is
	/// protected in another way.
	pub fn from_key_file_ignoring_permissions(
		path: impl AsRef<Path>,
	) -> crate::error::Result<Self> {
		read_key_file(path.as_ref(), false).map(|key| Self::new(*key))
	}

	/// Adds a key under `key_id`, decrypt-only until `set_active_key_id` makes it active.
	///
	/// An id is registered once for good: adding one that exists, retired or not, is refused,
	/// because the bytes behind an id must never change.
	pub fn add_key(&self, key_id: u32, mut key: [u8; ROOT_KEY_LEN]) -> Result<(), EncryptionError> {
		let result = match self.inner.write().keys.entry(key_id) {
			Entry::Occupied(_) => {
				Err(EncryptionError::InvalidInput("the key id is already registered"))
			}
			Entry::Vacant(vacant) => {
				vacant.insert(Slot::new(KeyState::DecryptOnly, &key));
				Ok(())
			}
		};
		key.zeroize();
		result
	}

	/// Makes `key_id` the key new files are written under. The key that was active becomes
	/// decrypt-only.
	pub fn set_active_key_id(&self, key_id: u32) -> Result<(), EncryptionError> {
		let mut inner = self.inner.write();
		match inner.keys.get(&key_id).map(|slot| slot.state) {
			None => return Err(EncryptionError::UnknownKeyId),
			Some(KeyState::Retired) => return Err(EncryptionError::KeyRetired),
			Some(_) => {}
		}
		let previous = inner.active;
		if previous != key_id {
			if let Some(slot) = inner.keys.get_mut(&previous) {
				if slot.state == KeyState::Active {
					slot.state = KeyState::DecryptOnly;
				}
			}
		}
		if let Some(slot) = inner.keys.get_mut(&key_id) {
			slot.state = KeyState::Active;
		}
		inner.active = key_id;
		Ok(())
	}

	/// Destroys the key under `key_id`: its root is wiped and dropped, and every file written
	/// under it becomes unreadable. Retiring a retired key does nothing. The active key cannot
	/// be retired.
	pub fn retire_key(&self, key_id: u32) -> Result<(), EncryptionError> {
		let mut inner = self.inner.write();
		if inner.active == key_id {
			return Err(EncryptionError::InvalidInput("the active key cannot be retired"));
		}
		let slot = inner.keys.get_mut(&key_id).ok_or(EncryptionError::UnknownKeyId)?;
		slot.state = KeyState::Retired;
		// Dropping the box wipes the root.
		slot.root = None;
		Ok(())
	}

	/// Whether the manager still owns the root of `key_id`.
	#[cfg(test)]
	fn holds_root(&self, key_id: u32) -> bool {
		self.inner.read().keys.get(&key_id).is_some_and(|slot| slot.root.is_some())
	}
}

/// The value of a hex digit, in either case.
const fn hex_digit(digit: u8) -> Option<u8> {
	match digit {
		b'0'..=b'9' => Some(digit - b'0'),
		b'a'..=b'f' => Some(digit - b'a' + 10),
		b'A'..=b'F' => Some(digit - b'A' + 10),
		_ => None,
	}
}

/// A root key written as 64 hex digits, which may be surrounded by white space.
fn decode_hex(text: &[u8]) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
	let text = text.trim_ascii();
	if text.len() != 2 * ROOT_KEY_LEN {
		return Err(EncryptionError::InvalidInput(
			"a root key is 64 hex digits, or 32 raw bytes in a file",
		));
	}
	let mut key = Zeroizing::new([0u8; ROOT_KEY_LEN]);
	for (byte, digits) in key.iter_mut().zip(text.chunks_exact(2)) {
		let &[high, low] = digits else {
			return Err(EncryptionError::InvalidInput("a root key is 64 hex digits"));
		};
		let (Some(high), Some(low)) = (hex_digit(high), hex_digit(low)) else {
			return Err(EncryptionError::InvalidInput(
				"the key has 64 characters, and not all of them are hex digits",
			));
		};
		*byte = (high << 4) | low;
	}
	Ok(key)
}

/// A root key written as 32 raw bytes, or as 64 hex digits. The length decides which: nothing
/// is trimmed from 32 bytes, so a raw key may end in a byte that is white space.
fn decode_key(input: &[u8]) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
	if let Ok(raw) = <&[u8; ROOT_KEY_LEN]>::try_from(input) {
		if raw.iter().all(u8::is_ascii_hexdigit) {
			return Err(EncryptionError::InvalidInput(
				"32 hex digits are a key of 128 bits: a root key is 32 raw bytes or 64 hex digits",
			));
		}
		return Ok(Zeroizing::new(*raw));
	}
	decode_hex(input)
}

/// Reads the key file at `path`, see `SoftwareKeyManager::from_key_file`.
fn read_key_file(
	path: &Path,
	check_permissions: bool,
) -> crate::error::Result<Zeroizing<[u8; ROOT_KEY_LEN]>> {
	let file = File::open(path)?;
	// Everything below is decided from what was opened, not from the name, which can be pointed
	// at another file between two calls.
	let metadata = file.metadata()?;
	if !metadata.is_file() {
		return Err(Error::InvalidArgument(format!(
			"the key file {} is not a regular file",
			path.display()
		)));
	}
	#[cfg(unix)]
	if check_permissions {
		use std::os::unix::fs::PermissionsExt;

		let mode = metadata.permissions().mode() & 0o777;
		if mode & 0o007 != 0 {
			return Err(Error::InvalidArgument(format!(
				"the key file {} can be reached by every user (mode {mode:04o}): restrict it to \
				 its owner, for instance with chmod 600, or choose to accept it as it is",
				path.display()
			)));
		}
	}
	#[cfg(not(unix))]
	let _ = check_permissions;
	if metadata.len() > KEY_FILE_MAX_LEN as u64 {
		return Err(Error::InvalidArgument(format!(
			"the key file {} is longer than a key file can be ({KEY_FILE_MAX_LEN} bytes)",
			path.display()
		)));
	}

	// One allocation that never grows, so that no copy of the key is left behind in memory that
	// was freed without being wiped, and one byte more than a key file can be, to find out that
	// a file grew after its size was read.
	let mut contents = Zeroizing::new(Vec::with_capacity(KEY_FILE_MAX_LEN + 1));
	file.take(KEY_FILE_MAX_LEN as u64 + 1).read_to_end(&mut contents)?;
	if contents.len() > KEY_FILE_MAX_LEN {
		return Err(Error::InvalidArgument(format!(
			"the key file {} is longer than a key file can be ({KEY_FILE_MAX_LEN} bytes)",
			path.display()
		)));
	}
	Ok(decode_key(&contents)?)
}

impl KeyManager for SoftwareKeyManager {
	fn active_key_id(&self) -> u32 {
		self.inner.read().active
	}

	fn key_state(&self, key_id: u32) -> KeyState {
		self.inner.read().keys.get(&key_id).map_or(KeyState::Unknown, |slot| slot.state)
	}

	fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
		let inner = self.inner.read();
		let slot = inner.keys.get(&key_id).ok_or(EncryptionError::UnknownKeyId)?;
		let root = slot.root.as_ref().ok_or(EncryptionError::KeyRetired)?;
		let mut copy = Zeroizing::new([0u8; ROOT_KEY_LEN]);
		copy.copy_from_slice(&***root);
		Ok(copy)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::error::KeyProviderFailure;
	use crate::tde::testutil::{assert_leaks_no_bytes, assert_no_byte_tokens};

	const KEY_1: [u8; ROOT_KEY_LEN] = [0x11; ROOT_KEY_LEN];
	const KEY_2: [u8; ROOT_KEY_LEN] = [0x22; ROOT_KEY_LEN];
	const KEY_3: [u8; ROOT_KEY_LEN] = [0x33; ROOT_KEY_LEN];

	#[test]
	fn a_new_manager_has_one_active_key_under_the_default_id() {
		let km = SoftwareKeyManager::new(KEY_1);
		assert_eq!(km.active_key_id(), KEY_ID_DEFAULT);
		assert_eq!(km.key_state(KEY_ID_DEFAULT), KeyState::Active);
		assert_eq!(*km.root_key(KEY_ID_DEFAULT).unwrap(), KEY_1);
		assert_eq!(km.key_state(9), KeyState::Unknown);
		assert_eq!(km.root_key(9).unwrap_err(), EncryptionError::UnknownKeyId);
	}

	#[test]
	fn rotation_moves_the_active_key_and_keeps_the_old_one_readable() {
		let km = SoftwareKeyManager::new(KEY_1);
		km.add_key(2, KEY_2).unwrap();
		// A key that was added is not written under until it is made active.
		assert_eq!(km.active_key_id(), 1);
		assert_eq!(km.key_state(2), KeyState::DecryptOnly);

		km.set_active_key_id(2).unwrap();
		assert_eq!(km.active_key_id(), 2);
		assert_eq!(km.key_state(1), KeyState::DecryptOnly);
		assert_eq!(km.key_state(2), KeyState::Active);
		assert_eq!(*km.root_key(1).unwrap(), KEY_1);
		assert_eq!(*km.root_key(2).unwrap(), KEY_2);

		// Making the active key active again changes nothing.
		km.set_active_key_id(2).unwrap();
		assert_eq!(km.key_state(2), KeyState::Active);
		// Going back is a rotation like any other.
		km.set_active_key_id(1).unwrap();
		assert_eq!(km.key_state(1), KeyState::Active);
		assert_eq!(km.key_state(2), KeyState::DecryptOnly);
	}

	#[test]
	fn a_key_id_is_registered_once_for_good() {
		let km = SoftwareKeyManager::new(KEY_1);
		let err = km.add_key(1, KEY_2).unwrap_err();
		assert!(matches!(err, EncryptionError::InvalidInput(_)), "{err:?}");
		// The refused add did not replace the root.
		assert_eq!(*km.root_key(1).unwrap(), KEY_1);

		km.add_key(2, KEY_2).unwrap();
		km.retire_key(2).unwrap();
		let err = km.add_key(2, KEY_3).unwrap_err();
		assert!(matches!(err, EncryptionError::InvalidInput(_)), "{err:?}");
		assert_eq!(km.key_state(2), KeyState::Retired);
	}

	#[test]
	fn activation_and_retirement_refuse_ids_that_cannot_take_them() {
		let km = SoftwareKeyManager::new(KEY_1);
		km.add_key(2, KEY_2).unwrap();
		km.retire_key(2).unwrap();

		assert_eq!(km.set_active_key_id(9).unwrap_err(), EncryptionError::UnknownKeyId);
		assert_eq!(km.set_active_key_id(2).unwrap_err(), EncryptionError::KeyRetired);
		// A refused activation left the active key alone.
		assert_eq!(km.active_key_id(), 1);
		assert_eq!(km.key_state(1), KeyState::Active);

		let err = km.retire_key(1).unwrap_err();
		assert!(matches!(err, EncryptionError::InvalidInput(_)), "{err:?}");
		assert_eq!(km.retire_key(9).unwrap_err(), EncryptionError::UnknownKeyId);
	}

	#[test]
	fn retiring_a_key_drops_the_only_owner_of_its_root() {
		let km = SoftwareKeyManager::new(KEY_1);
		km.add_key(2, KEY_2).unwrap();
		assert!(km.holds_root(2));

		km.retire_key(2).unwrap();
		assert!(!km.holds_root(2), "the root of a retired key is still held");
		assert_eq!(km.key_state(2), KeyState::Retired);
		assert_eq!(km.root_key(2).unwrap_err(), EncryptionError::KeyRetired);
		// The other key is untouched, and retiring again does nothing.
		assert!(km.holds_root(1));
		km.retire_key(2).unwrap();
		assert_eq!(*km.root_key(1).unwrap(), KEY_1);
	}

	#[test]
	fn prepare_accepts_usable_ids_and_names_the_first_unusable_one() {
		let km = SoftwareKeyManager::new(KEY_1);
		km.add_key(2, KEY_2).unwrap();
		km.add_key(3, KEY_3).unwrap();
		km.retire_key(3).unwrap();

		km.prepare(&[]).unwrap();
		km.prepare(&[1, 2]).unwrap();
		assert_eq!(km.prepare(&[1, 3, 9]).unwrap_err(), EncryptionError::KeyRetired);
		assert_eq!(km.prepare(&[2, 9, 3]).unwrap_err(), EncryptionError::UnknownKeyId);
	}

	/// A manager in the shape a provider-backed one has: the roots live remotely, `prepare`
	/// fetches them, and `root_key` only reads what was fetched. It implements nothing but what
	/// a manager must, so it also shows the other trait methods have working defaults.
	struct CachingProvider {
		remote: HashMap<u32, [u8; ROOT_KEY_LEN]>,
		cache: RwLock<HashMap<u32, Zeroizing<[u8; ROOT_KEY_LEN]>>>,
	}

	impl KeyManager for CachingProvider {
		fn active_key_id(&self) -> u32 {
			1
		}

		fn key_state(&self, key_id: u32) -> KeyState {
			match key_id {
				1 => KeyState::Active,
				id if self.remote.contains_key(&id) => KeyState::DecryptOnly,
				_ => KeyState::Unknown,
			}
		}

		fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
			self.cache
				.read()
				.get(&key_id)
				.cloned()
				.ok_or(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable))
		}

		fn prepare(&self, key_ids: &[u32]) -> Result<(), EncryptionError> {
			for key_id in key_ids {
				let root = self.remote.get(key_id).ok_or(EncryptionError::UnknownKeyId)?;
				self.cache.write().insert(*key_id, Zeroizing::new(*root));
			}
			Ok(())
		}
	}

	#[test]
	fn a_provider_backed_manager_serves_only_what_prepare_fetched() {
		let km: Box<dyn KeyManager> = Box::new(CachingProvider {
			remote: HashMap::from([(1, KEY_1), (2, KEY_2)]),
			cache: RwLock::new(HashMap::new()),
		});

		// A cache miss is a retryable failure of that one call, not a statement about the key.
		let miss = km.root_key(1).unwrap_err();
		assert_eq!(miss, EncryptionError::KeyProvider(KeyProviderFailure::Unavailable));
		assert!(miss.is_retryable());

		km.prepare(&[1]).unwrap();
		assert_eq!(*km.root_key(1).unwrap(), KEY_1);
		assert!(km.root_key(2).unwrap_err().is_retryable());
		assert_eq!(km.prepare(&[9]).unwrap_err(), EncryptionError::UnknownKeyId);
		km.prepare(&[2]).unwrap();
		assert_eq!(*km.root_key(2).unwrap(), KEY_2);
	}

	#[test]
	fn the_default_prepare_of_a_custom_manager_checks_state_and_root() {
		let km = CachingProvider {
			remote: HashMap::from([(1, KEY_1), (2, KEY_2)]),
			cache: RwLock::new(HashMap::new()),
		};
		// Both ids are usable by state, but nothing is cached yet, so the root read fails the
		// way it would on the commit path.
		let defaulted = DefaultPrepare(&km);
		let err = defaulted.prepare(&[1]).unwrap_err();
		assert!(err.is_retryable(), "{err:?}");
		km.prepare(&[1]).unwrap();
		defaulted.prepare(&[1]).unwrap();
		assert_eq!(defaulted.prepare(&[7]).unwrap_err(), EncryptionError::UnknownKeyId);
	}

	/// Forwards the required methods only, so `prepare` is the trait's default.
	struct DefaultPrepare<'a>(&'a CachingProvider);

	impl KeyManager for DefaultPrepare<'_> {
		fn active_key_id(&self) -> u32 {
			self.0.active_key_id()
		}

		fn key_state(&self, key_id: u32) -> KeyState {
			self.0.key_state(key_id)
		}

		fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
			self.0.root_key(key_id)
		}
	}

	#[test]
	fn key_states_say_what_they_allow() {
		assert!(KeyState::Active.can_read() && KeyState::Active.can_write());
		assert!(KeyState::DecryptOnly.can_read() && !KeyState::DecryptOnly.can_write());
		assert!(!KeyState::Retired.can_read() && !KeyState::Retired.can_write());
		assert!(!KeyState::Unknown.can_read() && !KeyState::Unknown.can_write());
	}

	#[test]
	fn key_managers_are_object_safe_and_shareable() {
		fn assert_send_sync<T: Send + Sync + ?Sized>() {}
		assert_send_sync::<dyn KeyManager>();
		assert_send_sync::<SoftwareKeyManager>();
		let _shared: std::sync::Arc<dyn KeyManager> =
			std::sync::Arc::new(SoftwareKeyManager::new(KEY_1));
	}

	#[test]
	fn debug_output_never_contains_key_bytes() {
		// Non-repeating keys made of large byte values, so every textual form of them is easy to
		// spot and no byte can be mistaken for a key id or a count.
		let key1: [u8; ROOT_KEY_LEN] = std::array::from_fn(|i| 0xC0 + i as u8);
		let key7: [u8; ROOT_KEY_LEN] = std::array::from_fn(|i| 0xE0 + i as u8);
		let key9: [u8; ROOT_KEY_LEN] = std::array::from_fn(|i| 0xA0 + i as u8);
		let km = SoftwareKeyManager::new(key1);
		km.add_key(7, key7).unwrap();
		km.add_key(9, key9).unwrap();
		km.set_active_key_id(7).unwrap();
		km.retire_key(9).unwrap();

		let compact = format!("{km:?}");
		let pretty = format!("{km:#?}");
		for text in [&compact, &pretty] {
			assert_leaks_no_bytes(text, &[&key1, &key7, &key9]);
			assert_no_byte_tokens(text, &[&key1, &key7, &key9]);
		}

		// Key ids and what each may be used for are what an operator needs to see.
		assert!(compact.contains("active_key_id: 7"), "got {compact}");
		assert!(
			compact.contains("keys: [(1, DecryptOnly), (7, Active), (9, Retired)]"),
			"got {compact}"
		);
	}

	/// A key whose bytes are none of them a hex digit, white space or a small number, so that no
	/// form of it can be mistaken for text that the code under test prints.
	fn written_key() -> [u8; ROOT_KEY_LEN] {
		std::array::from_fn(|i| 0xC0 + i as u8)
	}

	/// `written_key` in hex, from an independent computation (`bytes(range(0xc0, 0xe0)).hex()`).
	const WRITTEN_KEY_HEX: &str =
		"c0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf";

	fn root_of(km: &SoftwareKeyManager) -> [u8; ROOT_KEY_LEN] {
		*km.root_key(KEY_ID_DEFAULT).unwrap()
	}

	#[test]
	fn a_key_is_read_from_32_raw_bytes_or_from_64_hex_digits() {
		let key = written_key();
		let raw = SoftwareKeyManager::from_key_bytes(&key).unwrap();
		assert_eq!(root_of(&raw), key);
		assert_eq!(raw.active_key_id(), KEY_ID_DEFAULT);
		assert_eq!(raw.key_state(KEY_ID_DEFAULT), KeyState::Active);

		let mixed: String = WRITTEN_KEY_HEX
			.chars()
			.enumerate()
			.map(|(i, c)| {
				if i % 2 == 0 {
					c.to_ascii_uppercase()
				} else {
					c
				}
			})
			.collect();
		let texts = [
			WRITTEN_KEY_HEX.to_string(),
			WRITTEN_KEY_HEX.to_uppercase(),
			mixed,
			format!("{WRITTEN_KEY_HEX}\n"),
			format!("{WRITTEN_KEY_HEX}\r\n"),
			format!("  {WRITTEN_KEY_HEX} \t\n"),
		];
		for text in texts {
			let from_bytes = SoftwareKeyManager::from_key_bytes(text.as_bytes()).unwrap();
			assert_eq!(root_of(&from_bytes), key, "{text:?}");
			let from_hex = SoftwareKeyManager::from_hex(&text).unwrap();
			assert_eq!(root_of(&from_hex), key, "{text:?}");
		}
	}

	#[test]
	fn exactly_32_bytes_are_a_raw_key_even_when_they_begin_or_end_in_white_space() {
		let mut key = written_key();
		key[0] = b' ';
		key[ROOT_KEY_LEN - 1] = b'\n';
		let km = SoftwareKeyManager::from_key_bytes(&key).unwrap();
		assert_eq!(root_of(&km), key);
	}

	#[test]
	fn what_is_not_a_key_is_refused_by_length_or_content_and_is_never_repeated() {
		let hex = WRITTEN_KEY_HEX;
		let key = written_key();
		// 64 bytes that are 32 characters, none of them a hex digit.
		let multibyte = "\u{e9}".repeat(ROOT_KEY_LEN);
		assert_eq!(multibyte.len(), 2 * ROOT_KEY_LEN);
		let cases: Vec<(&str, Vec<u8>)> = vec![
			("nothing", Vec::new()),
			("white space only", b" \n ".to_vec()),
			("31 bytes", key[..31].to_vec()),
			("33 bytes", [key.as_slice(), &[0xE0]].concat()),
			("63 digits", hex.as_bytes()[..63].to_vec()),
			("65 digits", format!("{hex}0").into_bytes()),
			("64 characters, one of them not a digit", format!("g{}", &hex[1..]).into_bytes()),
			(
				"64 characters with a space inside",
				format!("{} {}", &hex[..31], &hex[32..]).into_bytes(),
			),
			("64 bytes of two-byte characters", multibyte.into_bytes()),
			("32 hex digits, which are a key of 128 bits", hex.as_bytes()[..32].to_vec()),
		];
		for (what, bytes) in cases {
			let err = SoftwareKeyManager::from_key_bytes(&bytes).expect_err(what);
			assert!(matches!(err, EncryptionError::InvalidInput(_)), "{what}: {err:?}");
			let shown = format!("{err} {err:?}");
			assert_leaks_no_bytes(&shown, &[&key]);
			assert!(!shown.contains(&hex[..16]), "{what}: {shown}");
		}

		// The same text through the hex reader, which takes nothing as raw bytes.
		for text in ["", &hex[..63], &hex[..32]] {
			let err = SoftwareKeyManager::from_hex(text).expect_err(text);
			assert!(matches!(err, EncryptionError::InvalidInput(_)), "{text}: {err:?}");
		}
	}

	#[test]
	fn a_manager_made_from_text_shows_no_part_of_the_key() {
		let km = SoftwareKeyManager::from_hex(WRITTEN_KEY_HEX).unwrap();
		let key = written_key();
		let shown = format!("{km:?} {km:#?}");
		assert_leaks_no_bytes(&shown, &[&key]);
		assert_no_byte_tokens(&shown, &[&key]);
		assert!(shown.contains("active_key_id: 1"), "{shown}");
	}

	#[cfg(not(target_arch = "wasm32"))]
	mod key_files {
		use std::fs;
		use std::path::PathBuf;

		use tempfile::TempDir;

		use super::*;

		/// Writes a file with exactly `mode`, whatever the umask of the process is.
		fn write(dir: &TempDir, name: &str, bytes: &[u8], mode: u32) -> PathBuf {
			let path = dir.path().join(name);
			fs::write(&path, bytes).unwrap();
			set_mode(&path, mode);
			path
		}

		#[cfg(unix)]
		fn set_mode(path: &Path, mode: u32) {
			use std::os::unix::fs::PermissionsExt;

			fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
		}

		#[cfg(not(unix))]
		fn set_mode(_path: &Path, _mode: u32) {}

		#[test]
		fn a_key_file_holds_the_key_raw_or_in_hex() {
			let dir = TempDir::new().unwrap();
			let key = written_key();
			let raw = write(&dir, "raw.key", &key, 0o600);
			let hex = write(&dir, "hex.key", format!("{WRITTEN_KEY_HEX}\n").as_bytes(), 0o600);
			for path in [&raw, &hex] {
				let km = SoftwareKeyManager::from_key_file(path).unwrap();
				assert_eq!(root_of(&km), key, "{}", path.display());
				let km = SoftwareKeyManager::from_key_file_ignoring_permissions(path).unwrap();
				assert_eq!(root_of(&km), key, "{}", path.display());
			}
		}

		#[cfg(unix)]
		#[test]
		fn a_key_file_that_every_user_can_reach_is_refused_unless_that_is_chosen() {
			let dir = TempDir::new().unwrap();
			let key = written_key();
			for mode in [0o644, 0o666, 0o604, 0o602, 0o755] {
				let path =
					write(&dir, &format!("open-{mode:o}.key"), WRITTEN_KEY_HEX.as_bytes(), mode);
				let err =
					SoftwareKeyManager::from_key_file(&path).expect_err("a file for everyone");
				let Error::InvalidArgument(message) = &err else {
					panic!("mode {mode:o}: expected InvalidArgument, got {err:?}");
				};
				assert!(message.contains("chmod 600"), "mode {mode:o}: {message}");
				assert!(message.contains(&path.display().to_string()), "mode {mode:o}: {message}");
				assert!(!message.contains(&WRITTEN_KEY_HEX[..16]), "mode {mode:o}: {message}");

				// Chosen, it is read.
				let km = SoftwareKeyManager::from_key_file_ignoring_permissions(&path).unwrap();
				assert_eq!(root_of(&km), key, "mode {mode:o}");
			}

			// What only the owner, or the owner and a group, can reach is accepted.
			for mode in [0o600, 0o400, 0o640, 0o440, 0o660] {
				let path =
					write(&dir, &format!("private-{mode:o}.key"), WRITTEN_KEY_HEX.as_bytes(), mode);
				let km = SoftwareKeyManager::from_key_file(&path).unwrap();
				assert_eq!(root_of(&km), key, "mode {mode:o}");
			}
		}

		#[test]
		fn a_file_that_cannot_be_a_key_file_is_refused_with_the_reason() {
			let dir = TempDir::new().unwrap();

			let missing = SoftwareKeyManager::from_key_file(dir.path().join("none.key"));
			match missing {
				Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::NotFound),
				other => panic!("expected an I/O error, got {other:?}"),
			}

			#[cfg(unix)]
			{
				let err = SoftwareKeyManager::from_key_file(dir.path()).expect_err("a directory");
				let Error::InvalidArgument(message) = &err else {
					panic!("expected InvalidArgument, got {err:?}");
				};
				assert!(message.contains("not a regular file"), "{message}");
			}

			for (what, bytes) in [
				("an empty file", Vec::new()),
				("text that is not a key", b"correct horse battery staple".to_vec()),
				("32 hex digits", WRITTEN_KEY_HEX.as_bytes()[..32].to_vec()),
			] {
				let path = write(&dir, "bad.key", &bytes, 0o600);
				let err = SoftwareKeyManager::from_key_file(&path).expect_err(what);
				assert!(
					matches!(err, Error::Encryption(EncryptionError::InvalidInput(_))),
					"{what}: {err:?}"
				);
			}
		}

		#[test]
		fn a_key_file_is_read_up_to_its_limit_and_no_further() {
			let dir = TempDir::new().unwrap();
			let key = written_key();

			// A key in hex followed by white space up to the limit is a key file, and one byte
			// more is not, whatever it is.
			let mut at_the_limit = WRITTEN_KEY_HEX.as_bytes().to_vec();
			at_the_limit.resize(KEY_FILE_MAX_LEN, b' ');
			let path = write(&dir, "limit.key", &at_the_limit, 0o600);
			let km = SoftwareKeyManager::from_key_file(&path).unwrap();
			assert_eq!(root_of(&km), key);

			at_the_limit.push(b' ');
			let path = write(&dir, "over.key", &at_the_limit, 0o600);
			let err = SoftwareKeyManager::from_key_file(&path).expect_err("one byte too long");
			let Error::InvalidArgument(message) = &err else {
				panic!("expected InvalidArgument, got {err:?}");
			};
			assert!(message.contains("longer than a key file can be"), "{message}");
			assert!(!message.contains(&WRITTEN_KEY_HEX[..16]), "{message}");
		}
	}
}
