//! The context that every site which opens a WAL, replays one or repairs one is handed.
//!
//! A site that built a plaintext default instead would read and write plaintext in an encrypted
//! tree, so what these tests pin is that the answer comes from the options of the tree, from one
//! place, and that it can go to the workers that decode segments in parallel.

use std::sync::Arc;

use crate::tde::{CipherSuite, DbId, EncryptionContext, SoftwareKeyManager};
use crate::wal::{Options as WalOptions, WalContext};
use crate::Options;

fn assert_goes_to_pool_workers<T: Send + Sync + Clone + 'static>() {}

fn sealed() -> Arc<EncryptionContext> {
	Arc::new(EncryptionContext::new(
		Arc::new(SoftwareKeyManager::new([0x11; 32])),
		DbId::from_bytes([0xD0; 16]),
		CipherSuite::Aes256Gcm,
	))
}

#[test]
fn the_context_and_the_options_that_carry_it_can_go_to_pool_workers() {
	assert_goes_to_pool_workers::<WalContext>();
	assert_goes_to_pool_workers::<WalOptions>();
}

#[test]
fn the_default_is_a_plaintext_wal() {
	assert!(!WalContext::default().is_sealed());
	assert!(!WalContext::plaintext().is_sealed());
	assert!(!WalOptions::default().context.is_sealed());
}

#[test]
fn the_options_of_a_tree_decide_whether_its_wal_is_sealed() {
	let plain = Options::default();
	assert!(!WalContext::for_options(&plain).is_sealed());
	assert!(!WalOptions::for_tree(&plain).context.is_sealed());

	let encrypted = Options::default().with_encryption_context(sealed());
	assert!(WalContext::for_options(&encrypted).is_sealed());
	let wal_options = WalOptions::for_tree(&encrypted);
	assert!(wal_options.context.is_sealed());

	// Nothing but the context differs from the defaults, so a tree's WAL is configured as it was.
	let defaults = WalOptions::default();
	assert_eq!(wal_options.dir_mode, defaults.dir_mode);
	assert_eq!(wal_options.file_mode, defaults.file_mode);
	assert_eq!(wal_options.compression_type, defaults.compression_type);
	assert_eq!(wal_options.file_extension, defaults.file_extension);
	assert_eq!(wal_options.max_file_size, defaults.max_file_size);

	// The clone a worker takes is sealed too.
	let for_a_worker = wal_options.context.clone();
	assert!(for_a_worker.is_sealed() && wal_options.context.is_sealed());
}

#[test]
fn a_key_manager_without_a_resolved_context_does_not_seal_the_wal() {
	// The context is what the engine resolves from the descriptor of a database, and the tables
	// and the value log follow it too: a manager that was not resolved seals nothing anywhere.
	let key_manager: Arc<dyn crate::tde::KeyManager> =
		Arc::new(SoftwareKeyManager::new([0x11; 32]));
	let opts = Options::default().with_encryption(key_manager, CipherSuite::Aes256Gcm);
	assert!(opts.key_manager.is_some());
	assert!(!WalContext::for_options(&opts).is_sealed());
}

#[test]
fn no_site_of_the_tree_builds_a_plaintext_context_of_its_own() {
	// The tree opens, restores and repairs through `for_tree` and `for_options`, which read the
	// options. A literal default in the file that owns those sites would be a plaintext WAL in an
	// encrypted tree.
	let lsm = include_str!("../lsm.rs");
	assert!(!lsm.contains("wal::Options::default()"));
	assert!(!lsm.contains("WalOptions::default()"));
	assert!(lsm.contains("wal::Options::for_tree("));
	assert!(lsm.contains("wal::WalContext::for_options("));
}
