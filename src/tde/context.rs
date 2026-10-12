//! What the engine holds to create and open encrypted files: the key manager, the database it
//! belongs to and the suite new files use.

use std::fmt;
use std::sync::Arc;

use parking_lot::Mutex;

use super::cipher::FileCipher;
use super::header::CryptoHeader;
use super::identity::{DbId, FileKind};
use super::kdf::FileKey;
use super::keys::KeyManager;
use super::suite::CipherSuite;
use crate::error::EncryptionError;

/// Creates and opens the encrypted files of one database.
///
/// Cheap to clone: it shares the key manager.
#[derive(Clone)]
pub struct EncryptionContext {
	key_manager: Arc<dyn KeyManager>,
	db_id: DbId,
	suite: CipherSuite,
	/// The key ids the manager has been asked to make available, shared by the clones of the
	/// context so that the database as a whole asks once for each.
	prepared: Arc<Mutex<Vec<u32>>>,
}

impl fmt::Debug for EncryptionContext {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("EncryptionContext")
			.field("db_id", &self.db_id)
			.field("suite", &self.suite)
			.finish_non_exhaustive()
	}
}

impl EncryptionContext {
	/// The context of the database `db_id`, whose new files are sealed with `suite`.
	///
	/// `suite` is the one persisted with the database when it was created, never a fresh answer
	/// of `CipherSuite::resolve_auto`. Existing files keep the suite their headers name.
	pub fn new(key_manager: Arc<dyn KeyManager>, db_id: DbId, suite: CipherSuite) -> Self {
		Self {
			key_manager,
			db_id,
			suite,
			prepared: Arc::default(),
		}
	}

	/// The database the context belongs to.
	pub fn db_id(&self) -> DbId {
		self.db_id
	}

	/// The suite new files are sealed with.
	pub fn suite(&self) -> CipherSuite {
		self.suite
	}

	/// The key manager, for callers that need its state.
	pub fn key_manager(&self) -> &Arc<dyn KeyManager> {
		&self.key_manager
	}

	/// Makes the roots of `key_ids` available, which may be slow: call it when the database is
	/// opened, with the active key id and every key id found in the files. Creating and opening
	/// files afterwards never waits on a key provider.
	///
	/// The manager is asked for each key id once: an id that an earlier call made available is not
	/// asked for again, by this context or by a clone of it, so a database whose files are all
	/// under one key asks once however many files it opens, and nothing that happens after the
	/// open reaches a provider. A call that fails marks nothing. To ask again, because the keys in
	/// use changed or because the manager lost what it had, call [`refresh`](Self::refresh).
	pub fn prepare(&self, key_ids: &[u32]) -> Result<(), EncryptionError> {
		let mut missing: Vec<u32> = Vec::new();
		{
			let done = self.prepared.lock();
			for &key_id in key_ids {
				if !done.contains(&key_id) && !missing.contains(&key_id) {
					missing.push(key_id);
				}
			}
		}
		if missing.is_empty() {
			return Ok(());
		}
		self.key_manager.prepare(&missing)?;
		self.mark_prepared(&missing);
		Ok(())
	}

	/// Asks the manager for the roots of `key_ids` whether or not they were asked for before. For
	/// when the keys in use changed, or a manager that serves its roots from a cache has lost one.
	pub fn refresh(&self, key_ids: &[u32]) -> Result<(), EncryptionError> {
		self.key_manager.prepare(key_ids)?;
		self.mark_prepared(key_ids);
		Ok(())
	}

	/// Asks the manager again for the active key and for every key that was asked for before.
	///
	/// The active key must be made available, and its failure is the answer. A key that was asked
	/// for before and that the manager has retired or no longer knows is left out of what the
	/// context remembers, since nothing can be opened with it any more, and is not an error here.
	pub fn refresh_in_use(&self) -> Result<(), EncryptionError> {
		let active = self.key_manager.active_key_id();
		self.refresh(&[active])?;
		let others: Vec<u32> =
			self.prepared.lock().iter().copied().filter(|&key_id| key_id != active).collect();
		for key_id in others {
			match self.key_manager.prepare(&[key_id]) {
				Ok(()) => {}
				Err(EncryptionError::KeyRetired | EncryptionError::UnknownKeyId) => {
					self.prepared.lock().retain(|&done| done != key_id);
				}
				Err(err) => return Err(err),
			}
		}
		Ok(())
	}

	/// Remembers that the manager made the roots of `key_ids` available.
	fn mark_prepared(&self, key_ids: &[u32]) {
		let mut done = self.prepared.lock();
		for &key_id in key_ids {
			if !done.contains(&key_id) {
				done.push(key_id);
			}
		}
	}

	/// Starts a new file of `file_kind` and id `file_id` under the active key: draws a fresh salt,
	/// derives the file's key, seals the header and returns it with the cipher that seals the
	/// file's blocks.
	///
	/// Always call this for a new writer, including a retry of the same file id and the output of
	/// a repair: a new call is a new salt, and a salt is never used twice. `covered` is what the
	/// file wants authenticated along with its header, see `CryptoHeader::seal`.
	pub fn create_file(
		&self,
		file_kind: FileKind,
		file_id: u64,
		covered: &[u8],
	) -> Result<(CryptoHeader, FileCipher), EncryptionError> {
		let mut key = FileKey::generate(
			self.key_manager.as_ref(),
			self.db_id,
			self.suite,
			file_kind,
			file_id,
		)?;
		let header = CryptoHeader::seal(&mut key, covered)?;
		Ok((header, FileCipher::new(key)?))
	}

	/// Opens an existing file that `header` was read from, which can open blocks but not seal.
	///
	/// The checks run in this order, and the first failure is the answer: the header is for this
	/// database (`DbIdMismatch`), for the file the caller is opening (`FileIdMismatch`), under a
	/// key the manager can read with (`UnknownKeyId`, `KeyRetired`, or what the manager fails
	/// with), and authentic under it (`WrongKey`). No block is touched before all of them pass, and
	/// none of them is a reason to repair or delete the file.
	pub fn open_file(
		&self,
		header: &CryptoHeader,
		file_kind: FileKind,
		file_id: u64,
		covered: &[u8],
	) -> Result<FileCipher, EncryptionError> {
		if header.identity.db_id != self.db_id {
			return Err(EncryptionError::DbIdMismatch);
		}
		if header.identity.file_kind != file_kind || header.identity.file_id != file_id {
			return Err(EncryptionError::FileIdMismatch);
		}
		let mut key =
			FileKey::for_reading(self.key_manager.as_ref(), header.identity, header.salt)?;
		header.verify(&mut key, covered)?;
		FileCipher::new(key)
	}

	/// Decodes the header at the start of `bytes` and opens the file it belongs to, see
	/// `open_file`. A header that is not well formed is `MalformedHeader` or
	/// `UnsupportedVersion`, see `CryptoHeader::decode`.
	pub fn open_file_from_bytes(
		&self,
		bytes: &[u8],
		file_kind: FileKind,
		file_id: u64,
		covered: &[u8],
	) -> Result<FileCipher, EncryptionError> {
		self.open_file(&CryptoHeader::decode(bytes)?, file_kind, file_id, covered)
	}
}

#[cfg(test)]
mod tests {
	use zeroize::Zeroizing;

	use super::*;
	use crate::tde::cipher::{BlockContext, BlockKind};
	use crate::tde::header::HEADER_LEN;
	use crate::tde::keys::{KeyState, ROOT_KEY_LEN};
	use crate::tde::SoftwareKeyManager;

	const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];

	fn db_id() -> DbId {
		DbId::from_bytes([0xD0; 16])
	}

	fn manager() -> Arc<SoftwareKeyManager> {
		Arc::new(SoftwareKeyManager::new([0x11; ROOT_KEY_LEN]))
	}

	fn context(km: &Arc<SoftwareKeyManager>, suite: CipherSuite) -> EncryptionContext {
		EncryptionContext::new(Arc::<SoftwareKeyManager>::clone(km), db_id(), suite)
	}

	/// The same header bytes with the CRC recomputed.
	fn with_fixed_crc(mut bytes: [u8; HEADER_LEN]) -> [u8; HEADER_LEN] {
		let crc = crc32fast::hash(&bytes[..HEADER_LEN - 4]);
		bytes[HEADER_LEN - 4..].copy_from_slice(&crc.to_be_bytes());
		bytes
	}

	#[test]
	fn a_created_file_opens_with_the_same_keys_and_seals_what_the_reader_opens() {
		for suite in SUITES {
			for kind in [FileKind::Sst, FileKind::Vlog, FileKind::Wal, FileKind::Manifest] {
				let km = manager();
				let ctx = context(&km, suite);
				let covered = b"fields of the file's own header";
				let (header, writer) = ctx.create_file(kind, 17, covered).unwrap();
				assert!(writer.can_seal());
				assert_eq!(writer.suite(), suite);

				let reader = ctx.open_file_from_bytes(&header.encode(), kind, 17, covered).unwrap();
				assert!(!reader.can_seal());
				assert_eq!(reader.identity(), writer.identity());

				let block_kind = match kind {
					FileKind::Sst => BlockKind::SstData,
					FileKind::Vlog => BlockKind::VlogEntry,
					FileKind::Wal => BlockKind::WalBatch,
					FileKind::Manifest => BlockKind::ManifestBody,
				};
				let at = BlockContext::new(block_kind, 5, 0);
				let mut sealed = b"a block".to_vec();
				writer.seal_append(&at, &mut sealed).unwrap();
				assert_eq!(reader.open_suffix(&at, &mut sealed).unwrap(), b"a block");
			}
		}
	}

	#[test]
	fn every_new_file_gets_a_new_salt_and_so_a_new_header_and_key() {
		let km = manager();
		let ctx = context(&km, CipherSuite::Aes256Gcm);
		let (first, first_cipher) = ctx.create_file(FileKind::Sst, 3, b"").unwrap();
		let (second, second_cipher) = ctx.create_file(FileKind::Sst, 3, b"").unwrap();
		assert_ne!(first.salt, second.salt);
		assert_ne!(first.tag, second.tag);
		assert_eq!(first.identity, second.identity);

		// The same plaintext at the same position seals differently in the two files.
		let at = BlockContext::new(BlockKind::SstData, 100, 0);
		let mut one = b"same bytes".to_vec();
		let mut two = b"same bytes".to_vec();
		first_cipher.seal_append(&at, &mut one).unwrap();
		second_cipher.seal_append(&at, &mut two).unwrap();
		assert_ne!(one, two);
	}

	#[test]
	fn a_file_opens_after_the_active_key_has_moved_on() {
		let km = manager();
		km.add_key(2, [0x22; ROOT_KEY_LEN]).unwrap();
		let ctx = context(&km, CipherSuite::XChaCha20Poly1305);
		let (old_header, old_writer) = ctx.create_file(FileKind::Wal, 1, b"").unwrap();
		assert_eq!(old_header.identity.key_id, 1);

		km.set_active_key_id(2).unwrap();
		let (new_header, _) = ctx.create_file(FileKind::Wal, 2, b"").unwrap();
		assert_eq!(new_header.identity.key_id, 2);

		// Key 1 is decrypt-only now, and the old file still opens under it.
		let reader = ctx.open_file(&old_header, FileKind::Wal, 1, b"").unwrap();
		let at = BlockContext::new(BlockKind::WalBatch, 1, 0);
		let mut sealed = b"written under key 1".to_vec();
		old_writer.seal_append(&at, &mut sealed).unwrap();
		assert_eq!(reader.open_suffix(&at, &mut sealed).unwrap(), b"written under key 1");
	}

	#[test]
	fn opening_a_header_checks_database_file_key_and_tag_in_that_order() {
		let km = manager();
		let ctx = context(&km, CipherSuite::Aes256Gcm);
		let (header, _) = ctx.create_file(FileKind::Sst, 9, b"").unwrap();

		// Another database's file, even with every other field right.
		let other_db = EncryptionContext::new(
			Arc::<SoftwareKeyManager>::clone(&km),
			DbId::from_bytes([0xD1; 16]),
			CipherSuite::Aes256Gcm,
		);
		assert_eq!(
			other_db.open_file(&header, FileKind::Sst, 9, b"").unwrap_err(),
			EncryptionError::DbIdMismatch
		);

		// Not the file the caller is opening: another id or another kind.
		assert_eq!(
			ctx.open_file(&header, FileKind::Sst, 10, b"").unwrap_err(),
			EncryptionError::FileIdMismatch
		);
		assert_eq!(
			ctx.open_file(&header, FileKind::Vlog, 9, b"").unwrap_err(),
			EncryptionError::FileIdMismatch
		);

		// Both wrong: the database is named first.
		assert_eq!(
			other_db.open_file(&header, FileKind::Sst, 10, b"").unwrap_err(),
			EncryptionError::DbIdMismatch
		);
	}

	#[test]
	fn a_missing_retired_or_wrong_key_is_named_and_never_a_panic() {
		let km = manager();
		km.add_key(2, [0x22; ROOT_KEY_LEN]).unwrap();
		km.set_active_key_id(2).unwrap();
		let ctx = context(&km, CipherSuite::Aes256Gcm);
		let (under_two, _) = ctx.create_file(FileKind::Sst, 1, b"").unwrap();
		assert_eq!(under_two.identity.key_id, 2);

		// A manager that has never held key 2.
		let without_two = context(&manager(), CipherSuite::Aes256Gcm);
		assert_eq!(
			without_two.open_file(&under_two, FileKind::Sst, 1, b"").unwrap_err(),
			EncryptionError::UnknownKeyId
		);

		// A manager that holds an id under different key bytes: the header cannot authenticate.
		let other_roots = Arc::new(SoftwareKeyManager::new([0x11; ROOT_KEY_LEN]));
		other_roots.add_key(2, [0x99; ROOT_KEY_LEN]).unwrap();
		assert_eq!(
			context(&other_roots, CipherSuite::Aes256Gcm)
				.open_file(&under_two, FileKind::Sst, 1, b"")
				.unwrap_err(),
			EncryptionError::WrongKey
		);

		// A key that was retired.
		km.set_active_key_id(1).unwrap();
		km.retire_key(2).unwrap();
		assert_eq!(
			ctx.open_file(&under_two, FileKind::Sst, 1, b"").unwrap_err(),
			EncryptionError::KeyRetired
		);
	}

	/// A manager whose active key is in the given state, with a root for every id.
	struct Inactive(KeyState);

	impl KeyManager for Inactive {
		fn active_key_id(&self) -> u32 {
			1
		}

		fn key_state(&self, _key_id: u32) -> KeyState {
			self.0
		}

		fn root_key(&self, _key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
			Ok(Zeroizing::new([0x66; ROOT_KEY_LEN]))
		}
	}

	#[test]
	fn new_files_are_only_written_under_an_active_key() {
		let create = |state| {
			let ctx = EncryptionContext::new(
				Arc::new(Inactive(state)),
				db_id(),
				CipherSuite::XChaCha20Poly1305,
			);
			ctx.create_file(FileKind::Sst, 1, b"").map(|(header, _)| header)
		};
		assert_eq!(create(KeyState::Active).unwrap().identity.key_id, 1);
		assert_eq!(create(KeyState::DecryptOnly).unwrap_err(), EncryptionError::KeyRetired);
		assert_eq!(create(KeyState::Retired).unwrap_err(), EncryptionError::KeyRetired);
		assert_eq!(create(KeyState::Unknown).unwrap_err(), EncryptionError::UnknownKeyId);
	}

	#[test]
	fn a_tampered_header_with_a_fixed_crc_is_a_wrong_key_or_a_named_mismatch() {
		let km = manager();
		// Key 2 holds the same root as key 1, so a header whose key id is flipped from 1 to 2
		// still finds a key and only the tag can object.
		km.add_key(2, [0x11; ROOT_KEY_LEN]).unwrap();
		let ctx = context(&km, CipherSuite::Aes256Gcm);
		let covered = b"covered";
		let (header, _) = ctx.create_file(FileKind::Sst, 0x0102_0304_0506_0708, covered).unwrap();
		let original = header.encode();
		ctx.open_file_from_bytes(&original, FileKind::Sst, 0x0102_0304_0506_0708, covered).unwrap();

		// (offset, what the caller expects to open, the error it must get)
		let file_id = 0x0102_0304_0506_0708u64;
		let cases: [(usize, FileKind, u64, EncryptionError); 7] = [
			// The suite switched to the other valid one: the key and tag change, so the tag fails.
			(5, FileKind::Sst, file_id, EncryptionError::WrongKey),
			// The key id switched to a key with the same root: the key changes, so the tag fails.
			(11, FileKind::Sst, file_id, EncryptionError::WrongKey),
			// The salt, and the tag itself.
			(40, FileKind::Sst, file_id, EncryptionError::WrongKey),
			(72, FileKind::Sst, file_id, EncryptionError::WrongKey),
			// The database id: not this database's file, before any key is derived.
			(16, FileKind::Sst, file_id, EncryptionError::DbIdMismatch),
			// The file id and the kind: not the file the caller opens.
			(39, FileKind::Sst, file_id, EncryptionError::FileIdMismatch),
			(6, FileKind::Sst, file_id, EncryptionError::FileIdMismatch),
		];
		for (at, kind, id, expected) in cases {
			let mut bad = original;
			bad[at] = match at {
				// The other suite, a vlog file instead of an SSTable, and key id 2.
				5 | 6 | 11 => 2,
				_ => original[at] ^ 1,
			};
			assert_eq!(
				ctx.open_file_from_bytes(&bad, kind, id, covered).unwrap_err(),
				EncryptionError::MalformedHeader,
				"byte {at}: the stale CRC must be what stops it"
			);
			assert_eq!(
				ctx.open_file_from_bytes(&with_fixed_crc(bad), kind, id, covered).unwrap_err(),
				expected,
				"byte {at}"
			);
		}

		// The file the caller opens may be the tampered one: a renamed file whose header names
		// the new id fails the tag, because the id is part of the key.
		let mut renamed = original;
		renamed[39] ^= 1;
		assert_eq!(
			ctx.open_file_from_bytes(&with_fixed_crc(renamed), FileKind::Sst, file_id ^ 1, covered)
				.unwrap_err(),
			EncryptionError::WrongKey
		);
	}

	#[test]
	fn a_header_that_is_not_one_is_refused_before_any_key_is_touched() {
		let km = manager();
		let ctx = context(&km, CipherSuite::Aes256Gcm);
		let inputs: [&[u8]; 4] = [b"", b"VLOG", &[0u8; 200], &[0xFF; 92]];
		for bytes in inputs {
			let err = ctx.open_file_from_bytes(bytes, FileKind::Sst, 1, b"").unwrap_err();
			assert_eq!(err, EncryptionError::MalformedHeader, "{bytes:?}");
		}
	}

	/// A manager that says which key ids it was asked to prepare and can refuse to.
	struct Counting {
		inner: SoftwareKeyManager,
		asked: parking_lot::Mutex<Vec<Vec<u32>>>,
		refuse: std::sync::atomic::AtomicBool,
	}

	impl Counting {
		fn new() -> Arc<Self> {
			Arc::new(Self {
				inner: SoftwareKeyManager::new([0x11; ROOT_KEY_LEN]),
				asked: parking_lot::Mutex::new(Vec::new()),
				refuse: std::sync::atomic::AtomicBool::new(false),
			})
		}

		fn asked(&self) -> Vec<Vec<u32>> {
			self.asked.lock().clone()
		}
	}

	impl KeyManager for Counting {
		fn active_key_id(&self) -> u32 {
			self.inner.active_key_id()
		}

		fn key_state(&self, key_id: u32) -> KeyState {
			self.inner.key_state(key_id)
		}

		fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
			self.inner.root_key(key_id)
		}

		fn prepare(&self, key_ids: &[u32]) -> Result<(), EncryptionError> {
			self.asked.lock().push(key_ids.to_vec());
			if self.refuse.load(std::sync::atomic::Ordering::SeqCst) {
				return Err(EncryptionError::KeyProvider(
					crate::error::KeyProviderFailure::Unavailable,
				));
			}
			self.inner.prepare(key_ids)
		}
	}

	fn counting_context(km: &Arc<Counting>) -> EncryptionContext {
		EncryptionContext::new(Arc::<Counting>::clone(km), db_id(), CipherSuite::Aes256Gcm)
	}

	#[test]
	fn a_key_id_is_asked_for_once_by_the_context_and_its_clones() {
		let km = Counting::new();
		km.inner.add_key(2, [0x22; ROOT_KEY_LEN]).unwrap();
		let ctx = counting_context(&km);
		let clone = ctx.clone();

		ctx.prepare(&[1, 2, 1]).unwrap();
		assert_eq!(km.asked(), vec![vec![1, 2]], "an id once, in the order given");

		// Neither the context nor a clone asks again for what was made available, and a call that
		// has nothing new does not reach the manager at all.
		ctx.prepare(&[2]).unwrap();
		clone.prepare(&[1, 2]).unwrap();
		ctx.prepare(&[]).unwrap();
		assert_eq!(km.asked(), vec![vec![1, 2]]);

		// A new id is asked for alone.
		km.inner.add_key(3, [0x33; ROOT_KEY_LEN]).unwrap();
		clone.prepare(&[1, 3]).unwrap();
		assert_eq!(km.asked(), vec![vec![1, 2], vec![3]]);
		ctx.prepare(&[3]).unwrap();
		assert_eq!(km.asked().len(), 2, "the clone's answer is the context's too");

		// A context of another database that shares the manager asks for itself.
		let other = EncryptionContext::new(
			Arc::<Counting>::clone(&km),
			DbId::from_bytes([0xD1; 16]),
			CipherSuite::Aes256Gcm,
		);
		other.prepare(&[1]).unwrap();
		assert_eq!(km.asked().len(), 3);
	}

	#[test]
	fn a_preparation_that_failed_is_asked_for_again() {
		let km = Counting::new();
		let ctx = counting_context(&km);
		km.refuse.store(true, std::sync::atomic::Ordering::SeqCst);
		let err = ctx.prepare(&[1]).unwrap_err();
		assert!(err.is_retryable(), "{err:?}");

		km.refuse.store(false, std::sync::atomic::Ordering::SeqCst);
		ctx.prepare(&[1]).unwrap();
		assert_eq!(km.asked(), vec![vec![1], vec![1]], "nothing was remembered of the failure");

		// An id that the manager refuses for good is not remembered either.
		assert_eq!(ctx.prepare(&[1, 9]).unwrap_err(), EncryptionError::UnknownKeyId);
		assert_eq!(ctx.prepare(&[9]).unwrap_err(), EncryptionError::UnknownKeyId);
		assert_eq!(km.asked().len(), 4);
	}

	#[test]
	fn a_refresh_asks_again_and_the_keys_in_use_are_asked_for_with_the_active_one_first() {
		let km = Counting::new();
		km.inner.add_key(2, [0x22; ROOT_KEY_LEN]).unwrap();
		km.inner.add_key(3, [0x33; ROOT_KEY_LEN]).unwrap();
		let ctx = counting_context(&km);
		ctx.prepare(&[1, 2]).unwrap();
		ctx.refresh(&[1]).unwrap();
		assert_eq!(km.asked(), vec![vec![1, 2], vec![1]]);

		// The active key moved to one that was never asked for: it is asked for first, and then the
		// keys that were in use, one at a time.
		km.inner.set_active_key_id(3).unwrap();
		ctx.refresh_in_use().unwrap();
		assert_eq!(km.asked()[2..].to_vec(), vec![vec![3], vec![1], vec![2]]);
		// It is in use now, so a preparation of it asks nothing.
		ctx.prepare(&[3]).unwrap();
		assert_eq!(km.asked().len(), 5);

		// A key that was retired is not an error and is not asked for again afterwards.
		km.inner.retire_key(1).unwrap();
		ctx.refresh_in_use().unwrap();
		let after_retiring = km.asked().len();
		ctx.refresh_in_use().unwrap();
		assert_eq!(km.asked().len() - after_retiring, 2, "only the active key and key 2 are left");

		// A manager that cannot be reached is the answer for the active key, and for the others.
		km.refuse.store(true, std::sync::atomic::Ordering::SeqCst);
		let err = ctx.refresh_in_use().unwrap_err();
		assert!(err.is_retryable(), "{err:?}");
	}

	#[test]
	fn the_context_prepares_through_the_key_manager_and_prints_no_key() {
		let km = manager();
		km.add_key(2, [0x22; ROOT_KEY_LEN]).unwrap();
		km.retire_key(2).unwrap();
		let ctx = context(&km, CipherSuite::Aes256Gcm);
		ctx.prepare(&[1]).unwrap();
		assert_eq!(ctx.prepare(&[1, 2]).unwrap_err(), EncryptionError::KeyRetired);
		assert_eq!(ctx.prepare(&[3]).unwrap_err(), EncryptionError::UnknownKeyId);

		let text = format!("{ctx:?} {ctx:#?}");
		assert!(text.contains("EncryptionContext"), "{text}");
		assert!(text.contains("Aes256Gcm"), "{text}");
		crate::tde::testutil::assert_leaks_no_bytes(&text, &[&[0x11; ROOT_KEY_LEN]]);

		let cloned = ctx.clone();
		assert_eq!(cloned.db_id(), ctx.db_id());
		assert_eq!(cloned.suite(), ctx.suite());
	}
}
