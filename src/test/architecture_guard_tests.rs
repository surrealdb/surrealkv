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

/// FK1 enforced absence: the single-file manifest engine (whole-file
/// rewrite, rename-replace, snapshot list) must not return. Metadata is
/// numbered immutable lineages published by conditional create.
#[test]
fn removed_manifest_file_engine_remains_absent() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let levels = std::fs::read_to_string(root.join("src/levels/mod.rs")).unwrap();
	for forbidden in ["fn write_manifest_to_disk", "fn load_from_file", "struct SnapshotInfo"] {
		assert!(
			!levels.contains(forbidden),
			"single-file manifest machinery returned to levels/mod.rs: {forbidden}"
		);
	}
	// Non-vacuity: the replacement persistence exists where expected.
	assert!(levels.contains("fn persist_owner_update"), "guard parsed the wrong file");
	let lib = std::fs::read_to_string(root.join("src/lib.rs")).unwrap();
	assert!(
		!lib.contains("fn manifest_file_path"),
		"manifest file-path plumbing returned to Options"
	);
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
fn crate_root_exposes_one_engine_over_the_injected_roles() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let source = std::fs::read_to_string(root.join("src/lib.rs")).unwrap();
	for retained in [
		"mod lsm;",
		"mod transaction;",
		"mod wal;",
		"mod sstable;",
		"mod memtable;",
		"mod branch;",
		"mod storage;",
		"pub use crate::lsm",
		"pub use crate::transaction",
	] {
		assert!(source.contains(retained), "the engine or its seam was bypassed: {retained}");
	}
	assert!(!source.contains("mod rewrite;"), "temporary rewrite namespace returned");
}

/// PA2 enforced absence: the parallel prototype engine is deleted, not
/// deprecated (`docs/removed-surfaces.md`). It carried a second table format and
/// a second codec stack whose filenames shadowed the live ones, so "the format
/// file" was ambiguous. Nothing here may come back without a new design.
#[test]
fn removed_prototype_engine_remains_absent() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	for relative in [
		"src/database.rs",
		"src/table.rs",
		"src/format.rs",
		"src/testkit.rs",
		"src/lifecycle.rs",
		"src/branch_native.rs",
		"src/test/rewrite_public_tests.rs",
	] {
		assert!(!root.join(relative).exists(), "deleted prototype path returned: {relative}");
	}

	let lib = std::fs::read_to_string(root.join("src/lib.rs")).unwrap();
	for forbidden in ["mod database;", "mod table;", "mod format;", "mod testkit;", "branch_native"]
	{
		assert!(!lib.contains(forbidden), "prototype module was re-declared: {forbidden}");
	}

	// The reference model and its selector types went with it; the catalog that
	// shares the file did not.
	let branch = std::fs::read_to_string(root.join("src/branch.rs")).unwrap();
	for forbidden in
		["struct BranchModel", "enum ReadSelector", "enum WriteOperation", "advance_head"]
	{
		assert!(!branch.contains(forbidden), "reference model returned to branch.rs: {forbidden}");
	}
	// Non-vacuity, twice over: this guard reads the real file, and the detector
	// finds a string that IS present in it.
	assert!(branch.contains("struct BranchCatalog"), "guard parsed the wrong file");
	assert!(
		!branch.contains("struct BranchModelXX"),
		"placeholder assertion must not match anything"
	);

	// The seam the prototype existed to prove is retained.
	let storage = std::fs::read_to_string(root.join("src/storage/mod.rs")).unwrap();
	for retained in ["trait ObjectStore", "trait CommitStore", "trait Platform"] {
		assert!(
			storage.contains(retained),
			"the injected role seam was lost with the prototype: {retained}"
		);
	}
}
