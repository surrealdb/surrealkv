//! The encryption building blocks as a downstream crate sees them: a key manager written outside
//! this crate, the engine-facing context, and the error type.
//!
//! Nothing here touches a database: these tests pin the building blocks that an embedder or the
//! engine can rely on without `cfg(test)` access.

#![cfg(not(target_arch = "wasm32"))]

use std::collections::HashMap;
use std::sync::Arc;

use surrealkv::tde::{
	BlockContext,
	BlockKind,
	CipherSuite,
	CryptoHeader,
	DbId,
	EncryptionContext,
	FileKind,
	KeyManager,
	KeyState,
	Zeroizing,
	HEADER_LEN,
};
use surrealkv::{EncryptionError, Error, KeyProviderFailure};

/// A key manager as an embedder writes one: it implements only what a manager must.
struct EmbedderKeys {
	roots: HashMap<u32, [u8; 32]>,
	active: u32,
}

impl KeyManager for EmbedderKeys {
	fn active_key_id(&self) -> u32 {
		self.active
	}

	fn key_state(&self, key_id: u32) -> KeyState {
		match key_id {
			id if id == self.active => KeyState::Active,
			id if self.roots.contains_key(&id) => KeyState::DecryptOnly,
			_ => KeyState::Unknown,
		}
	}

	fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; 32]>, EncryptionError> {
		self.roots
			.get(&key_id)
			.map(|root| Zeroizing::new(*root))
			.ok_or(EncryptionError::UnknownKeyId)
	}
}

fn manager(roots: &[(u32, u8)], active: u32) -> Arc<dyn KeyManager> {
	Arc::new(EmbedderKeys {
		roots: roots.iter().map(|&(id, byte)| (id, [byte; 32])).collect(),
		active,
	})
}

fn db_id() -> DbId {
	DbId::generate().expect("the operating system provides random bytes")
}

#[test]
fn an_embedder_key_manager_creates_and_opens_encrypted_files() {
	for suite in [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305] {
		let ctx = EncryptionContext::new(manager(&[(1, 0x11)], 1), db_id(), suite);
		ctx.prepare(&[1]).unwrap();

		let (header, writer) = ctx.create_file(FileKind::Wal, 5, b"").unwrap();
		let bytes = header.encode();
		assert_eq!(bytes.len(), HEADER_LEN);
		assert!(CryptoHeader::has_magic(&bytes));

		let block = BlockContext::new(BlockKind::WalBatch, 1, 0);
		let mut sealed = b"one batch".to_vec();
		writer.seal_append(&block, &mut sealed).unwrap();
		assert_ne!(&sealed[..9], b"one batch");

		// A later session reads the header back from the file and opens it.
		let reader = ctx.open_file_from_bytes(&bytes, FileKind::Wal, 5, b"").unwrap();
		assert_eq!(reader.open_suffix(&block, &mut sealed).unwrap(), b"one batch");
	}
}

#[test]
fn a_key_that_does_not_match_is_a_wrong_key_not_corruption() {
	let db = db_id();
	let written = EncryptionContext::new(manager(&[(1, 0x11)], 1), db, CipherSuite::Aes256Gcm);
	let (header, _) = written.create_file(FileKind::Sst, 8, b"").unwrap();

	let other = EncryptionContext::new(manager(&[(1, 0x99)], 1), db, CipherSuite::Aes256Gcm);
	let err = other.open_file(&header, FileKind::Sst, 8, b"").unwrap_err();
	assert_eq!(err, EncryptionError::WrongKey);

	// It reaches a caller of the database API as the crate error, and a caller can tell what it is.
	let err: Error = err.into();
	match err {
		Error::Encryption(EncryptionError::WrongKey) => {}
		other => panic!("expected a wrong key, got {other:?}"),
	}
}

#[test]
fn what_an_embedder_can_match_on_needs_a_wildcard_because_the_types_may_grow() {
	let ctx = EncryptionContext::new(manager(&[(1, 0x11)], 1), db_id(), CipherSuite::Aes256Gcm);
	let (header, _) = ctx.create_file(FileKind::Vlog, 2, b"").unwrap();
	let err = ctx.open_file(&header, FileKind::Vlog, 3, b"").unwrap_err();
	let class = match err {
		EncryptionError::FileIdMismatch => "wrong file",
		EncryptionError::WrongKey | EncryptionError::AuthenticationFailed => "key or data",
		EncryptionError::KeyProvider(KeyProviderFailure::Unavailable) => "retry later",
		_ => "other",
	};
	assert_eq!(class, "wrong file");
	assert!(!err.is_retryable());
}

#[test]
fn rotating_the_active_key_leaves_old_files_readable_and_losing_the_old_key_does_not() {
	let db = db_id();
	let before =
		EncryptionContext::new(manager(&[(1, 0x11), (2, 0x22)], 1), db, CipherSuite::Aes256Gcm);
	let (old, _) = before.create_file(FileKind::Sst, 1, b"").unwrap();
	assert_eq!(old.identity.key_id, 1);

	let after =
		EncryptionContext::new(manager(&[(1, 0x11), (2, 0x22)], 2), db, CipherSuite::Aes256Gcm);
	let (new, _) = after.create_file(FileKind::Sst, 2, b"").unwrap();
	assert_eq!(new.identity.key_id, 2);
	after.open_file(&old, FileKind::Sst, 1, b"").unwrap();

	// A manager that no longer holds key 1 cannot read the old file.
	let without_old_key =
		EncryptionContext::new(manager(&[(2, 0x22)], 2), db, CipherSuite::Aes256Gcm);
	assert_eq!(
		without_old_key.open_file(&old, FileKind::Sst, 1, b"").unwrap_err(),
		EncryptionError::UnknownKeyId
	);
}

#[test]
fn a_context_is_shared_by_cloning() {
	let ctx =
		EncryptionContext::new(manager(&[(1, 0x11)], 1), db_id(), CipherSuite::XChaCha20Poly1305);
	let clone = ctx.clone();
	let (header, _) = ctx.create_file(FileKind::Manifest, 0, b"").unwrap();
	clone.open_file(&header, FileKind::Manifest, 0, b"").unwrap();
	assert_eq!(clone.db_id(), ctx.db_id());
	assert_eq!(clone.suite(), CipherSuite::XChaCha20Poly1305);
}
