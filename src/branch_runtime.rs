use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::batch::BatchOwner;
use crate::error::{Error, Result};
use crate::levels::LevelManifest;
use crate::memtable::{ImmutableMemtables, MemTable};

/// Complete mutable and immutable LSM component set for one physical owner.
///
/// The initial extraction contains the existing default-branch objects. Later
/// slices make instances lazy and registry-owned; no data structure or commit
/// path is duplicated here.
pub(crate) struct BranchRuntime {
	owner: BatchOwner,
	pub(crate) active_memtable: Arc<RwLock<Arc<MemTable>>>,
	pub(crate) immutable_memtables: Arc<RwLock<ImmutableMemtables>>,
	pub(crate) level_manifest: Arc<RwLock<LevelManifest>>,
}

impl BranchRuntime {
	pub(crate) fn new(
		owner: BatchOwner,
		active_memtable: Arc<RwLock<Arc<MemTable>>>,
		immutable_memtables: Arc<RwLock<ImmutableMemtables>>,
		level_manifest: Arc<RwLock<LevelManifest>>,
	) -> Self {
		debug_assert_eq!(
			active_memtable.read().map(|table| table.owner()).ok(),
			Some(owner),
			"branch runtime active memtable must have the same physical owner"
		);
		Self {
			owner,
			active_memtable,
			immutable_memtables,
			level_manifest,
		}
	}

	/// Lazily created runtime for a non-default owner: a fresh memtable at
	/// the (small) branch arena size, an empty immutable queue, and the
	/// shared level manifest selected by owner at access time.
	pub(crate) fn new_for_owner(
		owner: BatchOwner,
		arena_capacity: usize,
		level_manifest: Arc<RwLock<LevelManifest>>,
	) -> Self {
		Self {
			owner,
			active_memtable: Arc::new(RwLock::new(Arc::new(MemTable::new_owned(
				arena_capacity,
				owner,
			)))),
			immutable_memtables: Arc::new(RwLock::new(ImmutableMemtables::default())),
			level_manifest,
		}
	}

	pub(crate) fn owner(&self) -> BatchOwner {
		self.owner
	}

	/// Sum of arena capacities held by this runtime (active + immutable),
	/// used by the database-wide write-buffer budget.
	pub(crate) fn arena_capacity_bytes(&self) -> u64 {
		let active = self
			.active_memtable
			.read()
			.map(|memtable| memtable.arena_capacity() as u64)
			.unwrap_or(0);
		let immutable = self
			.immutable_memtables
			.read()
			.map(|imms| imms.iter().map(|e| e.memtable.arena_capacity() as u64).sum())
			.unwrap_or(0);
		active + immutable
	}

	pub(crate) fn validate_component_owners(&self) -> bool {
		let active_matches =
			self.active_memtable.read().map(|active| active.owner() == self.owner).unwrap_or(false);
		let immutables_match = self
			.immutable_memtables
			.read()
			.map(|immutables| immutables.iter().all(|entry| entry.memtable.owner() == self.owner))
			.unwrap_or(false);
		let levels_match = self
			.level_manifest
			.read()
			.map(|manifest| match manifest.levels_for(self.owner) {
				// No durable level set yet (this owner never flushed an
				// SST): vacuously owner-pure.
				None => true,
				Some(levels) => levels
					.get_levels()
					.iter()
					.flat_map(|level| level.tables.iter())
					.all(|table| table.meta.owner == self.owner),
			})
			.unwrap_or(false);
		active_matches && immutables_match && levels_match
	}
}

/// Owner-indexed runtimes. A registry lookup never allocates; a runtime (and
/// with it the first arena) is created only on the owner's first routed write
/// — an idle branch is a catalog record plus one map entry at most.
///
/// Reads are a read-lock + hash on a commit path that already takes several
/// locks; a lock-free structure is a recorded optimization, not a v1 need.
pub(crate) struct BranchRuntimeRegistry {
	runtimes: RwLock<HashMap<BatchOwner, Arc<BranchRuntime>>>,
}

impl BranchRuntimeRegistry {
	pub(crate) fn new(default_runtime: Arc<BranchRuntime>) -> Self {
		let mut runtimes = HashMap::new();
		runtimes.insert(default_runtime.owner(), default_runtime);
		Self {
			runtimes: RwLock::new(runtimes),
		}
	}

	pub(crate) fn get(&self, owner: BatchOwner) -> Option<Arc<BranchRuntime>> {
		self.runtimes.read().ok()?.get(&owner).cloned()
	}

	/// Resolve or lazily create the runtime for an already-validated owner.
	pub(crate) fn get_or_create(
		&self,
		owner: BatchOwner,
		arena_capacity: usize,
		level_manifest: &Arc<RwLock<LevelManifest>>,
	) -> Result<Arc<BranchRuntime>> {
		if let Some(runtime) = self.get(owner) {
			return Ok(runtime);
		}
		let mut runtimes = self
			.runtimes
			.write()
			.map_err(|_| Error::Other("branch runtime registry lock poisoned".to_string()))?;
		// Double-checked: another writer may have created it.
		if let Some(runtime) = runtimes.get(&owner) {
			return Ok(Arc::clone(runtime));
		}
		let runtime = Arc::new(BranchRuntime::new_for_owner(
			owner,
			arena_capacity,
			Arc::clone(level_manifest),
		));
		runtimes.insert(owner, Arc::clone(&runtime));
		Ok(runtime)
	}

	/// Drops a reclaimed branch's runtime and hands back its memtables so the
	/// caller can release their WAL dependencies.
	///
	/// The memtables are discarded, not flushed: the branch is tombstoned, so
	/// anything it had not yet flushed is unreachable by definition. Their WAL
	/// dependencies must still be released or the reclaimed branch pins log
	/// segments forever.
	pub(crate) fn reclaim(&self, owner: BatchOwner) -> Vec<Arc<MemTable>> {
		let Ok(mut runtimes) = self.runtimes.write() else {
			return Vec::new();
		};
		let Some(runtime) = runtimes.remove(&owner) else {
			return Vec::new();
		};
		let mut discarded = Vec::new();
		if let Ok(active) = runtime.active_memtable.read() {
			discarded.push(Arc::clone(&active));
		}
		if let Ok(immutable) = runtime.immutable_memtables.read() {
			discarded.extend(immutable.iter().map(|entry| Arc::clone(&entry.memtable)));
		}
		discarded
	}

	/// All live runtimes (flush selection, budget accounting, shutdown).
	pub(crate) fn all(&self) -> Vec<Arc<BranchRuntime>> {
		self.runtimes.read().map(|map| map.values().cloned().collect()).unwrap_or_default()
	}

	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn len(&self) -> usize {
		self.runtimes.read().map(|map| map.len()).unwrap_or(0)
	}

	/// Total arena bytes across all runtimes, for the write-buffer budget.
	pub(crate) fn total_arena_capacity_bytes(&self) -> u64 {
		self.all().iter().map(|runtime| runtime.arena_capacity_bytes()).sum()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::{Arc, RwLock};

	use tempfile::TempDir;

	use super::BranchRuntime;
	use crate::batch::BatchOwner;
	use crate::levels::LevelManifest;
	use crate::memtable::{ImmutableMemtables, MemTable};
	use crate::Options;

	fn test_runtime(temp: &TempDir) -> BranchRuntime {
		let options = Arc::new(Options {
			path: temp.path().to_path_buf(),
			..Options::default()
		});
		std::fs::create_dir_all(options.sstable_dir()).unwrap();
		std::fs::create_dir_all(options.wal_dir()).unwrap();
		let authority = crate::authority::store::AuthorityStore::new(options.path.clone(), [0; 16]);
		BranchRuntime::new(
			BatchOwner::DEFAULT,
			Arc::new(RwLock::new(Arc::new(MemTable::new_owned(64 * 1024, BatchOwner::DEFAULT)))),
			Arc::new(RwLock::new(ImmutableMemtables::default())),
			Arc::new(RwLock::new(LevelManifest::fresh(options, authority))),
		)
	}

	#[test]
	fn default_runtime_owns_one_complete_owner_pure_component_set() {
		let temp = TempDir::new().unwrap();
		let runtime = test_runtime(&temp);
		assert_eq!(runtime.owner(), BatchOwner::DEFAULT);
		assert!(runtime.validate_component_owners());
	}

	/// Sabotage twin for `validate_component_owners`: planting one component
	/// with a foreign physical owner must redden validation. The pre-sabotage
	/// assertion proves the fixture actually reached a valid state first.
	#[test]
	fn foreign_owner_component_fails_runtime_validation() {
		let temp = TempDir::new().unwrap();
		let runtime = test_runtime(&temp);
		assert!(runtime.validate_component_owners(), "fixture must start owner-pure");

		let foreign = BatchOwner {
			branch: crate::BranchId([1; 16]),
			generation: crate::BranchGeneration(0),
		};
		runtime.immutable_memtables.write().unwrap().add(
			1,
			0,
			Arc::new(MemTable::new_owned(64 * 1024, foreign)),
		);
		assert!(
			!runtime.validate_component_owners(),
			"a foreign-owner immutable memtable must fail owner validation"
		);
	}

	/// The constructor guard must reject an active memtable whose physical
	/// owner differs from the runtime's owner.
	#[cfg(debug_assertions)]
	#[test]
	#[should_panic(expected = "branch runtime active memtable must have the same physical owner")]
	fn constructor_rejects_foreign_owner_active_memtable() {
		let temp = TempDir::new().unwrap();
		let options = Arc::new(Options {
			path: temp.path().to_path_buf(),
			..Options::default()
		});
		std::fs::create_dir_all(options.sstable_dir()).unwrap();
		std::fs::create_dir_all(options.wal_dir()).unwrap();
		let foreign = BatchOwner {
			branch: crate::BranchId([1; 16]),
			generation: crate::BranchGeneration(0),
		};
		let authority = crate::authority::store::AuthorityStore::new(options.path.clone(), [0; 16]);
		let _ = BranchRuntime::new(
			BatchOwner::DEFAULT,
			Arc::new(RwLock::new(Arc::new(MemTable::new_owned(64 * 1024, foreign)))),
			Arc::new(RwLock::new(ImmutableMemtables::default())),
			Arc::new(RwLock::new(LevelManifest::fresh(options, authority))),
		);
	}
}
