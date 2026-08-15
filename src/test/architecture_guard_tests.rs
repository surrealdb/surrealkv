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

/// Every module-level `static` in `src/`, keyed `file:NAME`, with the reason it
/// is allowed to be one. Adding an entry has to be a decision someone writes
/// down; `docs/KNOWN_GAPS.md` holds the long form, including what would change
/// each answer.
///
/// The guard below matches `static` *item declarations*, so an immutable one
/// would also need naming — its reason would simply be "immutable".
const AMBIENT_STATE_ALLOWLIST: &[(&str, &str)] = &[
	(
		"src/vfs.rs:SYNCED",
		"the fsync ledger the crash tests read. `#[cfg(test)]` at both the module and the call \
		 site, so zero production cost, and keyed by canonicalised absolute path so two tests \
		 with their own TempDir cannot collide. Injecting it would thread a test-only \
		 observation through every write path in the engine.",
	),
	(
		"src/lsm.rs:BRANCH_ID_COUNTER",
		"folded with the pid into minted branch identities. Defeats reproducible identities \
		 across runs, but `mint_branch_id` loops against the catalog until unused, so this is a \
		 collision-reducer, not a correctness dependency.",
	),
	(
		"src/memtable/mod.rs:NEXT_DEPENDENCY_ID",
		"opaque ids, compared only for equality within one store, no value is a sentinel.",
	),
	(
		"src/memtable/skiplist.rs:PROBABILITIES",
		"a memoized constant — a pure function of two compile-time constants. Not state.",
	),
	(
		"src/authority/publish.rs:TEMP_COUNTER",
		"a monotonic uniqueness source for temp-file names, not state anything reads. Sharing \
		 it across stores is if anything safer: two stores in one directory still get distinct \
		 names.",
	),
	(
		"src/test/iterator_tests.rs:TEST_TABLE_ID_COUNTER",
		"test-only uniqueness source for table ids, same shape as TEMP_COUNTER.",
	),
];

/// Ambient state does not accumulate.
///
/// Every dependency the engine needs but must not *choose* is carried on
/// `Options` as `Arc<dyn Trait>` — `clock` and `fault_policy` are the two worked
/// examples. Before V8a there were also a `thread_local!` failpoint registry and
/// an environment variable that set commit concurrency for every store in the
/// process at once. Both are gone; this keeps them gone, and makes any new
/// global an entry someone had to write.
///
/// The allowlist is checked in both directions: an unlisted global fails, and so
/// does a listed one that no longer exists, so the reasons cannot outlive the
/// code they describe.
#[test]
fn no_ambient_state_outside_the_written_allowlist() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut files_scanned = 0usize;
	let mut found: Vec<String> = Vec::new();
	let mut offenders: Vec<String> = Vec::new();

	/// A `static` item declaration, e.g. `static NAME: Type = ...`, at any
	/// indentation. Returns the name.
	fn static_name(line: &str) -> Option<&str> {
		let trimmed = line.trim_start();
		let rest = trimmed
			.strip_prefix("static ")
			.or_else(|| trimmed.strip_prefix("pub static "))
			.or_else(|| trimmed.strip_prefix("pub(crate) static "))?;
		let name = rest.split(':').next()?.trim();
		// `static mut` would be a different, worse thing; name it as such.
		let name = name.strip_prefix("mut ").unwrap_or(name);
		(!name.is_empty() && name.chars().all(|c| c.is_ascii_uppercase() || c == '_'))
			.then_some(name)
	}

	fn walk(
		dir: &Path,
		root: &Path,
		files: &mut usize,
		found: &mut Vec<String>,
		offenders: &mut Vec<String>,
	) {
		for entry in std::fs::read_dir(dir).unwrap().filter_map(Result::ok) {
			let path = entry.path();
			if path.is_dir() {
				walk(&path, root, files, found, offenders);
				continue;
			}
			if path.extension().is_none_or(|ext| ext != "rs") {
				continue;
			}
			let relative = path.strip_prefix(root).unwrap().to_string_lossy().replace('\\', "/");
			// This file necessarily contains the patterns it looks for, in the
			// allowlist above and in the probes below.
			if relative == "src/test/architecture_guard_tests.rs" {
				continue;
			}
			*files += 1;
			let source = std::fs::read_to_string(&path).unwrap();
			for (number, line) in source.lines().enumerate() {
				let trimmed = line.trim_start();
				if trimmed.starts_with("//") {
					continue;
				}
				let at = format!("{relative}:{}", number + 1);

				if let Some(name) = static_name(line) {
					found.push(format!("{relative}:{name}"));
					if !AMBIENT_STATE_ALLOWLIST
						.iter()
						.any(|(k, _)| *k == format!("{relative}:{name}"))
					{
						offenders.push(format!("{at} unlisted global `{name}`"));
					}
				}
				for pattern in ["thread_local!", "lazy_static!", "once_cell::"] {
					if trimmed.contains(pattern) {
						offenders.push(format!("{at} {pattern}"));
					}
				}
				if trimmed.contains("env::var") {
					offenders.push(format!("{at} reads the environment"));
				}
				if trimmed.contains("SystemTime::now()") && relative != "src/clock.rs" {
					offenders.push(format!("{at} reads the wall clock outside the clock adapter"));
				}
				if (trimmed.contains("rand::rng()") || trimmed.contains("thread_rng()"))
					&& relative != "src/memtable/skiplist.rs"
				{
					offenders.push(format!("{at} draws from an ambient RNG"));
				}
			}
		}
	}
	walk(&root.join("src"), root, &mut files_scanned, &mut found, &mut offenders);

	// Non-vacuity: the walk reached the tree, and each detector matches its own
	// target.
	assert!(files_scanned > 30, "the walk only saw {files_scanned} files; it is not scanning src/");
	assert_eq!(static_name("\tstatic FOO: AtomicU64 = x;"), Some("FOO"), "static detector broken");
	assert_eq!(static_name("pub static BAR: X = y;"), Some("BAR"), "static detector broken");
	assert_eq!(static_name("static mut BAZ: X = y;"), Some("BAZ"), "static detector broken");
	assert_eq!(static_name("pub trait T: Send + 'static {"), None, "static detector over-matches");
	assert!(!found.is_empty(), "the walk found no statics at all; the detector is not working");

	assert!(
		offenders.is_empty(),
		"ambient state outside the allowlist:\n  {}",
		offenders.join("\n  ")
	);

	// The reasons cannot outlive the code: every allowlist entry must still
	// name something that exists.
	let stale: Vec<&str> = AMBIENT_STATE_ALLOWLIST
		.iter()
		.map(|(k, _)| *k)
		.filter(|key| !found.iter().any(|f| f == key))
		.collect();
	assert!(stale.is_empty(), "allowlist entries name globals that no longer exist: {stale:?}");
}

/// Test helpers do not get redefined outside the shared layers.
///
/// `create_temp_directory` had six copies, `wrap_buffer` four, `create_store`
/// nine — and two of those nine answered a different question under the same
/// name. Duplicates are not merely untidy: they drift, and a test moved between
/// files silently changes what it exercises. `branch_exists` was the worked
/// example, defined twice at two different layers.
///
/// The support layer is the list. Anything it provides, no other test file may
/// define — so re-introducing a copy fails here rather than at the next
/// divergence.
#[test]
fn test_helpers_are_not_redefined_outside_the_shared_layers() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));

	// TWO shared locations, both read from the files so this list cannot drift:
	// `support/` holds store and branch fixtures, and `src/test/mod.rs` holds
	// the iterator collectors it predates. Keeping both is a deliberate choice —
	// merging them is a large mechanical change for little gain — but a helper
	// in either is one nobody may redefine.
	let mut provided: Vec<String> = Vec::new();
	for (relative, prefix) in
		[("src/test/support/mod.rs", "pub(crate) fn "), ("src/test/mod.rs", "fn ")]
	{
		let source = std::fs::read_to_string(root.join(relative)).unwrap();
		let before = provided.len();
		provided.extend(
			source
				.lines()
				.filter_map(|line| line.strip_prefix(prefix)?.split(['(', '<']).next())
				.map(str::to_string),
		);
		assert!(
			provided.len() > before,
			"{relative} exported no helpers; this guard is reading it wrong"
		);
	}
	assert!(
		provided.len() >= 10,
		"only {} shared helpers found; this guard is reading the wrong files",
		provided.len()
	);

	let mut offenders = Vec::new();
	let mut files_scanned = 0usize;
	for entry in std::fs::read_dir(root.join("src/test")).unwrap().filter_map(Result::ok) {
		let path = entry.path();
		if path.extension().is_none_or(|ext| ext != "rs") {
			continue;
		}
		let name = path.file_name().unwrap().to_string_lossy().to_string();
		// The shared layers are where these are allowed to be defined.
		if name == "mod.rs" {
			continue;
		}
		files_scanned += 1;
		for (number, line) in std::fs::read_to_string(&path).unwrap().lines().enumerate() {
			// Top-level definitions only. A `fn` nested inside a test body is
			// scoped to that test and cannot be confused with anything.
			let Some(rest) = line.strip_prefix("fn ").or_else(|| line.strip_prefix("pub fn "))
			else {
				continue;
			};
			let Some(defined) = rest.split(['(', '<']).next() else {
				continue;
			};
			if provided.iter().any(|p| p == defined.trim()) {
				offenders.push(format!("{name}:{} redefines `{defined}`", number + 1));
			}
		}
	}

	// Non-vacuity: the walk reached the tree, and the detector matches its own
	// target shape.
	assert!(files_scanned > 10, "only {files_scanned} test files scanned; the walk is wrong");
	assert!(
		provided.iter().any(|p| p == "branch_exists"),
		"the support layer no longer provides `branch_exists`; the guard is reading it wrong"
	);
	assert!(
		offenders.is_empty(),
		"the support layer already provides these, so a second definition can only diverge:\n  {}",
		offenders.join("\n  ")
	);
}

/// Ceiling on how many places outside the support layer reach into
/// `store.core.inner`.
///
/// It was 262. Every reach binds a test to an internal field name, so renaming
/// one is a suite-wide edit — and the async port renames the most-read of them,
/// turning `level_manifest` into atomic-swap versions. `level_manifest` alone
/// was 57 reaches and is now 24.
///
/// **This is a ratchet, not a target.** Lower it when you remove reaches; never
/// raise it. If a new test genuinely needs an internal, add an accessor to
/// `src/test/support/` and reach from there — that is the one place allowed to,
/// so the port has one file to fix rather than fourteen.
const CORE_INNER_REACH_CEILING: usize = 160;

#[test]
fn tests_do_not_reach_further_into_core_inner_than_they_already_do() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut reaches = 0usize;
	let mut files_scanned = 0usize;

	for entry in std::fs::read_dir(root.join("src/test")).unwrap().filter_map(Result::ok) {
		let path = entry.path();
		if path.extension().is_none_or(|ext| ext != "rs") {
			continue;
		}
		files_scanned += 1;
		reaches += std::fs::read_to_string(&path).unwrap().matches("core.inner").count();
	}

	// Non-vacuity: the walk reached the tree, and the detector matches its own
	// target. `support/` is a directory, so `read_dir` over `src/test` skips it —
	// which is the point: its reaches are the sanctioned ones.
	assert!(files_scanned > 10, "only {files_scanned} test files scanned; the walk is wrong");
	assert!(reaches > 0, "no reaches found at all; the detector is not matching");

	assert!(
		reaches <= CORE_INNER_REACH_CEILING,
		"tests now reach into `core.inner` at {reaches} places, above the ceiling of \
		 {CORE_INNER_REACH_CEILING}. Add an accessor to `src/test/support/` instead of reaching \
		 from a test file."
	);
	assert!(
		reaches >= CORE_INNER_REACH_CEILING.saturating_sub(10),
		"reaches dropped to {reaches}, well under the ceiling of {CORE_INNER_REACH_CEILING} — \
		 lower the ceiling so the ratchet keeps its grip"
	);
}
