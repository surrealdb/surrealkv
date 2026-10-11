//! Key managers: where root keys come from, and whether each one may still be used.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt;

use parking_lot::RwLock;
use zeroize::{Zeroize, Zeroizing};

use super::KEY_ID_DEFAULT;
use crate::error::EncryptionError;

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

/// In-memory key manager with key rotation, for tests and for embedders that bring their own
/// key storage.
///
/// Roots are held in memory that is wiped when a key is retired or the manager is dropped. They
/// are never part of `Debug` output.
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
}
