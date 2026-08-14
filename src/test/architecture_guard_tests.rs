use std::path::Path;

/// Concrete absence guards only for storage designs intentionally removed.
/// The existing LSM, transaction, WAL, SST, and memtable remain available
/// while branch-native behavior is integrated into them.
#[test]
fn removed_vlog_and_btree_engines_remain_absent() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	for relative in ["src/vlog.rs", "src/test/vlog_tests.rs", "src/bplustree.rs", "src/btree.rs"] {
		assert!(!root.join(relative).exists(), "removed storage path returned: {relative}");
	}
}

#[test]
fn simulated_fault_backend_is_test_only() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let storage = std::fs::read_to_string(root.join("src/storage/mod.rs")).unwrap();
	assert!(
		storage.contains("#[cfg(test)]\nmod sim;"),
		"SimStorage implementation must not compile into production builds"
	);
	assert!(
		storage.contains("#[cfg(test)]\npub(crate) use sim::"),
		"SimStorage exports must remain test-only"
	);
}

#[test]
fn branch_native_filesystem_io_is_confined_to_local_adapter() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	for relative in [
		"src/api.rs",
		"src/branch.rs",
		"src/database.rs",
		"src/format.rs",
		"src/lifecycle.rs",
		"src/table.rs",
		"src/storage/mod.rs",
		"src/storage/memory.rs",
		"src/storage/sim.rs",
	] {
		let source = std::fs::read_to_string(root.join(relative)).unwrap();
		assert!(
			!source.contains("std::fs"),
			"raw filesystem dependency escaped the Local adapter: {relative}"
		);
	}
}

/// BR2 extraction guard: the default branch's LSM component set lives only in
/// `BranchRuntime`. `CoreInner` reaches it through `default_runtime` (and the
/// temporary `Deref`); alias fields for the same components must not return,
/// or later slices would route writes past the runtime registry.
#[test]
fn core_inner_owns_lsm_components_only_through_branch_runtime() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let lsm = std::fs::read_to_string(root.join("src/lsm.rs")).unwrap();

	let start = lsm
		.find("pub(crate) struct CoreInner {")
		.expect("CoreInner struct declaration moved; update this guard");
	let body_len = lsm[start..].find("\n}\n").expect("CoreInner struct body is unterminated");
	let core_inner = &lsm[start..start + body_len];

	// Non-vacuity: the extracted block is the real struct, not a fragment.
	assert!(
		core_inner.contains("default_runtime: Arc<BranchRuntime>"),
		"CoreInner no longer holds the default BranchRuntime"
	);
	assert!(core_inner.contains("wal:"), "guard parsed a fragment, not the CoreInner struct");

	for alias in ["active_memtable:", "immutable_memtables:", "level_manifest:"] {
		assert!(
			!core_inner.contains(alias),
			"CoreInner regrew an alias component field ({alias}); LSM components are owned by BranchRuntime only"
		);
	}

	let runtime = std::fs::read_to_string(root.join("src/branch_runtime.rs")).unwrap();
	for owned in ["active_memtable:", "immutable_memtables:", "level_manifest:"] {
		assert!(
			runtime.contains(owned),
			"BranchRuntime lost its owned component field ({owned}); the extraction was reverted"
		);
	}
}

#[test]
fn crate_root_keeps_existing_engine_during_branch_native_integration() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let source = std::fs::read_to_string(root.join("src/lib.rs")).unwrap();
	for retained in [
		"mod lsm;",
		"mod transaction;",
		"mod wal;",
		"mod sstable;",
		"mod memtable;",
		"pub use crate::lsm",
		"pub use crate::transaction",
	] {
		assert!(source.contains(retained), "existing runtime was bypassed: {retained}");
	}
	for integrated in ["mod branch;", "mod database;", "mod storage;", "mod table;"] {
		assert!(
			source.contains(integrated),
			"branch-native module is not integrated: {integrated}"
		);
	}
	assert!(!source.contains("mod rewrite;"), "temporary rewrite namespace returned");
}
