//! Counterpart of `encryption_guard.rs`: the non-default `encryption-preview` feature compiles
//! the guard out, so a key manager is accepted. Run in CI so the feature-gated path keeps
//! building and the feature keeps doing what it says.
//!
//! Nothing is encrypted yet. This only pins that the guard is removed.

#![cfg(all(not(target_arch = "wasm32"), feature = "encryption-preview"))]

use std::sync::Arc;

use surrealkv::{CipherSuite, KeyManager, Options, SoftwareKeyManager, TreeBuilder};

#[tokio::test]
async fn build_accepts_key_manager() {
	let dir = tempfile::tempdir().unwrap();
	let db_path = dir.path().join("db");

	let key_manager: Arc<dyn KeyManager> = Arc::new(SoftwareKeyManager::new([0x5a; 32]));
	let opts = Options::new()
		.with_path(db_path.clone())
		.with_encryption(key_manager, CipherSuite::default());
	assert!(opts.validate().is_ok());

	let tree = TreeBuilder::with_options(opts).build().unwrap();
	assert!(db_path.join("manifest").exists());
	tree.close().await.unwrap();
}
