//! Whether a database is encrypted is decided once, when it is created, and is read from its
//! manifest every time it is opened. This is where that is settled, before anything else of the
//! database is looked at.
//!
//! The manifest is the database's descriptor: the first bytes of an encrypted one name the
//! database (a random identity, drawn when it is created), the cipher suite that its new files
//! use (chosen once and never asked of the machine again) and the key it was written with, and
//! the rest only opens with the right key. So opening the manifest is also the check of the key:
//! a root that does not fit is found here, before a log, a table or a value file has been sealed
//! under it, and not by whichever file happens to be opened first.
//!
//! # What the database and the configuration make of each other
//!
//! Encryption is chosen when a database is created and is never added to or taken from one that
//! exists. Strictly:
//!
//! | manifest | key manager | |
//! |---|---|---|
//! | absent | none | a plaintext database, created as before |
//! | absent | configured | an encrypted database, created: its identity and suite are made here |
//! | encrypted | configured | its identity and suite are the manifest's, and the key must open it |
//! | encrypted | none | `NotConfigured` |
//! | plaintext | configured | `NotEncrypted` |
//! | plaintext | none | a plaintext database, opened as before |
//!
//! Directories that hold files but no manifest are refused by `check_manifest_before_open`, which
//! runs after this and keeps its rule whether or not a key manager is configured.
//!
//! Nothing here writes. An encrypted database that is created writes its first manifest, sealed,
//! when the manifest is first made, before the log, the value log or any table: no file exists
//! under an identity until the manifest that carries it is on disk.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Once};

use super::{manifest_kind, refuse_unsealed_manifest, ManifestKind};
use crate::error::{EncryptionError, Result};
use crate::tde::{
	describe_manifest,
	hardware_aes_available,
	open_manifest,
	CipherPreference,
	CipherSuite,
	DbId,
	EncryptionContext,
	KeyManager,
	MANIFEST_PREFIX_LEN,
};
use crate::Options;

/// Settles, before anything is created or moved, whether the database at `opts.path` is
/// encrypted, and returns the options that every writer and reader of its files take their key
/// from: `opts` as they are for a plaintext database, and a copy with the resolved context for an
/// encrypted one.
///
/// A context that is already set on `opts` stands for the database that the caller expects: its
/// key manager is the one used and its identity must be the manifest's. Its suite is not: a
/// database keeps the suite it was created with, so that is the manifest's.
///
/// Reads only. Every refusal leaves the directory as it was.
pub(crate) fn resolve_encryption(opts: Arc<Options>) -> Result<Arc<Options>> {
	let key_manager = opts
		.encryption_context()
		.map(|context| Arc::clone(context.key_manager()))
		.or_else(|| opts.key_manager.clone());
	let manifest_path = opts.manifest_file_path(0);

	let Some(head) = read_prefix(&manifest_path)? else {
		let Some(key_manager) = key_manager else {
			return Ok(opts);
		};
		let context = match opts.encryption_context() {
			Some(context) => Arc::clone(context),
			None => Arc::new(describe_new_database(key_manager, opts.encryption_cipher)?),
		};
		// The key that the first files will be sealed with is made available now, and a manager
		// that cannot give it is found out before anything is created.
		context.prepare(&[context.key_manager().active_key_id()])?;
		warn_if_slow(context.suite());
		return Ok(with_context(opts, context));
	};

	let kind = manifest_kind(&head);
	if kind != ManifestKind::Sealed {
		// Nothing to settle for a plaintext database that is opened as one, and for the manifest
		// of one that is not encrypted, or of no database, where a key is configured: a refusal.
		return match key_manager {
			Some(_) => Err(refuse_unsealed_manifest(kind)),
			None => Ok(opts),
		};
	}

	let Some(key_manager) = key_manager else {
		return Err(EncryptionError::NotConfigured.into());
	};
	let descriptor = describe_manifest(&head)?;
	let (Some(db_id), Some(suite), Some(key_id)) =
		(descriptor.db_id, descriptor.suite, descriptor.key_id)
	else {
		return Err(EncryptionError::MalformedHeader.into());
	};
	// The database is the manifest's, unless the caller said which one it expects, which is then
	// checked against the manifest's by opening it.
	let db_id = opts.encryption_context().map_or(db_id, |expected| expected.db_id());
	let context = Arc::new(EncryptionContext::new(Arc::clone(&key_manager), db_id, suite));

	// The key of the manifest and the one that new files will be sealed with, which are the same
	// until the key is rotated.
	let active = key_manager.active_key_id();
	if active == key_id {
		context.prepare(&[key_id])?;
	} else {
		context.prepare(&[key_id, active])?;
	}
	// The check of the key: the manifest opens, which is the tag of its header and the tag of its
	// body, or the database is refused and nothing was sealed under the key.
	open_manifest(&context, &std::fs::read(&manifest_path)?)?;
	warn_if_slow(suite);
	Ok(with_context(opts, context))
}

/// The context of a database that is being created: an identity that no other database has, and
/// the suite that the preference resolves to, here and never again, since the answer depends on
/// the machine and the database keeps it for good.
fn describe_new_database(
	key_manager: Arc<dyn KeyManager>,
	preference: CipherPreference,
) -> Result<EncryptionContext> {
	let db_id = DbId::generate()?;
	Ok(EncryptionContext::new(key_manager, db_id, preference.resolve()))
}

/// `opts` with `context` as their context: the same options if they already have it.
fn with_context(opts: Arc<Options>, context: Arc<EncryptionContext>) -> Arc<Options> {
	if opts.encryption_context().is_some_and(|set| Arc::ptr_eq(set, &context)) {
		return opts;
	}
	Arc::new((*opts).clone().with_encryption_context(context))
}

/// The first bytes of the manifest at `path`, as many as describe it without a key, or `None` if
/// there is no manifest.
fn read_prefix(path: &Path) -> Result<Option<Vec<u8>>> {
	let file = match File::open(path) {
		Ok(file) => file,
		Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
		Err(e) => return Err(e.into()),
	};
	let mut head = Vec::with_capacity(MANIFEST_PREFIX_LEN);
	file.take(MANIFEST_PREFIX_LEN as u64).read_to_end(&mut head)?;
	Ok(Some(head))
}

/// Whether AES-256-GCM is slow and not constant-time on a machine that has no instructions for
/// it, which is the case that a warning is for.
fn is_slow_here(suite: CipherSuite, hardware_aes: bool) -> bool {
	suite == CipherSuite::Aes256Gcm && !hardware_aes
}

/// Says once, however many databases are opened, that a database uses AES-256-GCM on a machine
/// without hardware support for it. The suite is the database's and is not changed because of
/// the machine: only the operator can decide to move the data.
fn warn_if_slow(suite: CipherSuite) {
	static WARNED: Once = Once::new();
	if is_slow_here(suite, hardware_aes_available()) {
		WARNED.call_once(|| {
			tracing::warn!(
				"The cipher suite of an encrypted database is AES-256-GCM, and this machine has \
				 no hardware support for it: it is slow here, and only as safe as its software \
				 implementation. The suite is chosen when a database is created and is not changed."
			);
		});
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn only_aes_without_hardware_support_is_slow() {
		assert!(is_slow_here(CipherSuite::Aes256Gcm, false));
		assert!(!is_slow_here(CipherSuite::Aes256Gcm, true));
		assert!(!is_slow_here(CipherSuite::XChaCha20Poly1305, false));
		assert!(!is_slow_here(CipherSuite::XChaCha20Poly1305, true));
	}
}
