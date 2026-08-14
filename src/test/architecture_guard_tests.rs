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

/// The catalog and the typed-identifier layer decide things; they must never
/// also perform IO. Keeping them filesystem-free is what lets branch decisions
/// be tested without a store, and what will let the authority move onto another
/// backend without dragging branch logic with it.
#[test]
fn branch_decisions_stay_free_of_filesystem_io() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	for relative in ["src/api.rs", "src/branch.rs"] {
		let source = std::fs::read_to_string(root.join(relative)).unwrap();
		assert!(
			!source.contains("std::fs"),
			"raw filesystem dependency reached a decision-only module: {relative}"
		);
	}
	// Non-vacuity: the detector finds `std::fs` where it genuinely is.
	let publish = std::fs::read_to_string(root.join("src/authority/publish.rs")).unwrap();
	assert!(publish.contains("std::fs"), "the detector would not notice filesystem use");
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
fn crate_root_exposes_exactly_one_engine() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let source = std::fs::read_to_string(root.join("src/lib.rs")).unwrap();
	for retained in [
		"mod lsm;",
		"mod transaction;",
		"mod wal;",
		"mod sstable;",
		"mod memtable;",
		"mod branch;",
		"pub use crate::lsm",
		"pub use crate::transaction",
	] {
		assert!(source.contains(retained), "the engine was bypassed: {retained}");
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
}

/// V1 enforced absence: the three-role storage seam is deleted, not deprecated
/// (`docs/removed-surfaces.md`). It had zero engine callers and its `ObjectStore`
/// was `async fn` throughout while every call site that would have used it holds
/// a `std` guard across the would-be `await` — so it was incompatible by calling
/// convention, not merely unused. The branch that takes on async IO designs its
/// seam against the engine's real call sites; see
/// `docs/ASYNC_OBJECT_STORE_HANDOVER.md`.
#[test]
fn removed_storage_role_seam_remains_absent() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	assert!(!root.join("src/storage").exists(), "the deleted storage module returned");

	let lib = std::fs::read_to_string(root.join("src/lib.rs")).unwrap();
	assert!(!lib.contains("mod storage;"), "the storage module was re-declared");
	// Non-vacuity: this guard reads the real crate root.
	assert!(lib.contains("mod lsm;"), "guard parsed the wrong file");
}

/// Files allowed to carry an `allow(dead_code)` attribute, each with its reason.
/// The point of the guard below is that adding an entry here has to be a
/// decision someone writes down.
///
/// - `src/wal/mod.rs`: `dir_mode` and `file_mode` are read under `#[cfg(unix)]` and are genuinely
///   unread on Windows. The attribute there is `cfg_attr(not(unix), ...)` — conditional on the
///   platform, not a blanket suppression — but this guard matches on text and cannot tell the
///   difference, so the file is named here rather than the check weakened.
const DEAD_CODE_ALLOWLIST: &[&str] = &["src/wal/mod.rs"];

/// Dead code does not accumulate again.
///
/// Two `allow(dead_code)` markers hid genuinely unreachable code for entire
/// phases of this project — a whole 2,591-line module in one case — because the
/// attribute silences exactly the signal that would have found them. Anything
/// unused is now either wired or deleted, and re-introducing the attribute takes
/// an explicit entry above.
#[test]
fn no_module_suppresses_dead_code_warnings() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut offenders = Vec::new();
	let mut files_scanned = 0usize;

	fn walk(dir: &Path, files: &mut usize, offenders: &mut Vec<String>, root: &Path) {
		for entry in std::fs::read_dir(dir).unwrap().filter_map(Result::ok) {
			let path = entry.path();
			if path.is_dir() {
				walk(&path, files, offenders, root);
			} else if path.extension().is_some_and(|ext| ext == "rs") {
				let relative =
					path.strip_prefix(root).unwrap().to_string_lossy().replace('\\', "/");
				if DEAD_CODE_ALLOWLIST.contains(&relative.as_str()) {
					continue;
				}
				*files += 1;
				let source = std::fs::read_to_string(&path).unwrap();
				for (number, line) in source.lines().enumerate() {
					// Attributes only. This file necessarily mentions the string
					// it is looking for, in prose and in this very comparison.
					let trimmed = line.trim_start();
					if trimmed.starts_with("#[") && trimmed.contains("allow(dead_code)") {
						offenders.push(format!("{relative}:{}", number + 1));
					}
				}
			}
		}
	}
	walk(&root.join("src"), &mut files_scanned, &mut offenders, root);

	// Non-vacuity: the walk really reached the tree, and the detector really
	// matches the string it is looking for.
	assert!(files_scanned > 30, "the walk only saw {files_scanned} files; it is not scanning src/");
	assert!(
		{
			let probe = "  #[allow(dead_code)]";
			let trimmed = probe.trim_start();
			trimmed.starts_with("#[") && trimmed.contains("allow(dead_code)")
		},
		"the detector cannot match its own target"
	);
	assert!(
		offenders.is_empty(),
		"dead code is suppressed rather than removed at: {}",
		offenders.join(", ")
	);
}

/// V5 precondition: nothing in this repository has ever shared a `Tree` across
/// threads, so the first concurrency test would have discovered whether it can.
/// Better to state it as a compile-time fact than to find out inside a race.
#[test]
fn tree_is_shareable_across_threads() {
	fn assert_send_sync<T: Send + Sync>() {}
	assert_send_sync::<crate::Tree>();
	assert_send_sync::<crate::BranchHandle>();
}

/// V5 enforced absence: the catalog publish must not touch the level manifest.
///
/// `Core::fork_branch` and `BranchHandle::record_merge_edge` both hold a
/// `level_manifest` READ guard across their catalog publish, deliberately — it
/// is what excludes a concurrent compaction publish (FK6). `std::sync::RwLock`
/// is not reentrant and a queued writer blocks new readers, so a second
/// acquisition inside the publish deadlocked both operations against any
/// compaction waiting for the write lock. It hung forever, and nothing caught it
/// until a branch operation was first run concurrently with a compaction.
///
/// The catalog version is reached through a shared `Arc<AtomicU64>` instead. A
/// guard rather than a comment, because the failure mode is a hang: no panic, no
/// error, no test output.
#[test]
fn the_catalog_publish_does_not_touch_the_level_manifest() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let lsm = std::fs::read_to_string(root.join("src/lsm.rs")).unwrap();

	let start = lsm
		.find("fn publish_catalog_locked(")
		.expect("publish_catalog_locked moved; update this guard");
	let body_len = lsm[start..].find("\n\t}\n").expect("publish_catalog_locked is unterminated");
	let body = &lsm[start..start + body_len];

	// Non-vacuity: the extracted region is the real function.
	assert!(body.contains("publish_catalog"), "guard parsed the wrong region");
	assert!(
		!body.contains("level_manifest"),
		"the catalog publish reached for the level manifest again; two callers hold a read \
		 guard across it and this deadlocks them against a queued compaction writer"
	);
}
