use std::sync::{Arc, RwLock};

use crate::batch::BatchOwner;
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

	pub(crate) fn owner(&self) -> BatchOwner {
		self.owner
	}

	pub(crate) fn validate_component_owners(&self) -> bool {
		let active_matches = self
			.active_memtable
			.read()
			.map(|active| active.owner() == self.owner)
			.unwrap_or(false);
		let immutables_match = self
			.immutable_memtables
			.read()
			.map(|immutables| immutables.iter().all(|entry| entry.memtable.owner() == self.owner))
			.unwrap_or(false);
		let levels_match = self
			.level_manifest
			.read()
			.map(|manifest| {
				manifest.levels_for(self.owner).is_some_and(|levels| {
					levels
						.get_levels()
						.iter()
						.flat_map(|level| level.tables.iter())
						.all(|table| table.meta.owner == self.owner)
				})
			})
			.unwrap_or(false);
		active_matches && immutables_match && levels_match
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
		std::fs::create_dir_all(options.manifest_dir()).unwrap();
		BranchRuntime::new(
			BatchOwner::DEFAULT,
			Arc::new(RwLock::new(Arc::new(MemTable::new_owned(
				64 * 1024,
				BatchOwner::DEFAULT,
			)))),
			Arc::new(RwLock::new(ImmutableMemtables::default())),
			Arc::new(RwLock::new(LevelManifest::new(options).unwrap())),
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
		std::fs::create_dir_all(options.manifest_dir()).unwrap();
		let foreign = BatchOwner {
			branch: crate::BranchId([1; 16]),
			generation: crate::BranchGeneration(0),
		};
		let _ = BranchRuntime::new(
			BatchOwner::DEFAULT,
			Arc::new(RwLock::new(Arc::new(MemTable::new_owned(64 * 1024, foreign)))),
			Arc::new(RwLock::new(ImmutableMemtables::default())),
			Arc::new(RwLock::new(LevelManifest::new(options).unwrap())),
		);
	}
}
