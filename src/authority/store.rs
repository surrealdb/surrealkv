//! Typed access to the three authority lineages on local storage: load the
//! newest valid version, publish the next one, validate identity invariants
//! (header version == filename version; db_id equality) fail-closed.

use std::path::{Path, PathBuf};

use super::format::{BranchStateManifest, CatalogManifest, RootManifest};
use super::publish::{
	branch_state_dir,
	catalog_dir,
	latest_version_in,
	prune_versions,
	publish_version,
	read_version,
	resolve_from_hint,
	root_dir,
	sync_dir,
	versions_in,
	PublishOutcome,
};
use crate::error::{Error, Result};
use crate::{BranchGeneration, BranchId};

pub(crate) const CATALOG_EXT: &str = "catalog";
pub(crate) const STATE_EXT: &str = "state";
pub(crate) const ROOT_EXT: &str = "root";
/// Metadata versions retained per lineage. Recovery reads only the newest, so
/// the rest are kept purely for forensics and repair; the count is what bounds
/// the metadata footprint of a store with many branches (FK5 amendment 1). It
/// must stay comfortably below `STATE_PROBE_LIMIT` so a stale hint's forward
/// probe still lands inside the retained window.
pub(crate) const KEEP_METADATA_VERSIONS: usize = 4;
/// Forward probes past a stale root hint (crash after a state publish and
/// before the next root publish leaves the hint exactly one behind per
/// unrecorded publish).
pub(crate) const STATE_PROBE_LIMIT: u32 = 64;

#[derive(Clone, Debug)]
pub(crate) struct AuthorityStore {
	base: PathBuf,
	pub(crate) db_id: [u8; 16],
}

impl AuthorityStore {
	pub(crate) fn new(base: PathBuf, db_id: [u8; 16]) -> Self {
		Self {
			base,
			db_id,
		}
	}

	/// Newest catalog version, if any — used before the store's identity is
	/// known (open learns `db_id` FROM the catalog).
	pub(crate) fn load_latest_catalog(base: &Path) -> Result<Option<CatalogManifest>> {
		let dir = catalog_dir(base);
		let Some(version) = latest_version_in(&dir, CATALOG_EXT)? else {
			return Ok(None);
		};
		let bytes = read_version(&dir, version, CATALOG_EXT)?.ok_or_else(|| {
			Error::Corruption(format!("catalog version {version} vanished between list and read"))
		})?;
		let manifest = CatalogManifest::decode(&bytes)?;
		if manifest.catalog_version != version {
			return Err(Error::Corruption(format!(
				"catalog header version {} does not match filename version {version}",
				manifest.catalog_version
			)));
		}
		Ok(Some(manifest))
	}

	pub(crate) fn publish_catalog(&self, manifest: &CatalogManifest) -> Result<PublishOutcome> {
		if manifest.db_id != self.db_id {
			return Err(Error::Corruption(
				"catalog publish with a foreign database identity".to_owned(),
			));
		}
		let bytes = manifest.encode()?;
		// Decode our own bytes before publish: the durable representation
		// must round-trip to exactly the facts the caller holds.
		let reparsed = CatalogManifest::decode(&bytes)?;
		if &reparsed != manifest {
			return Err(Error::Corruption(
				"catalog manifest does not round-trip its own encoding".to_owned(),
			));
		}
		publish_version(&catalog_dir(&self.base), manifest.catalog_version, CATALOG_EXT, &bytes)
	}

	pub(crate) fn load_latest_root(&self) -> Result<Option<RootManifest>> {
		let dir = root_dir(&self.base);
		let Some(version) = latest_version_in(&dir, ROOT_EXT)? else {
			return Ok(None);
		};
		let bytes = read_version(&dir, version, ROOT_EXT)?.ok_or_else(|| {
			Error::Corruption(format!("root version {version} vanished between list and read"))
		})?;
		let manifest = RootManifest::decode(&bytes)?;
		if manifest.root_version != version {
			return Err(Error::Corruption(format!(
				"root header version {} does not match filename version {version}",
				manifest.root_version
			)));
		}
		if manifest.db_id != self.db_id {
			return Err(Error::Corruption(
				"root manifest carries a foreign database identity".to_owned(),
			));
		}
		Ok(Some(manifest))
	}

	pub(crate) fn publish_root(&self, manifest: &RootManifest) -> Result<PublishOutcome> {
		if manifest.db_id != self.db_id {
			return Err(Error::Corruption(
				"root publish with a foreign database identity".to_owned(),
			));
		}
		let bytes = manifest.encode()?;
		let reparsed = RootManifest::decode(&bytes)?;
		if &reparsed != manifest {
			return Err(Error::Corruption(
				"root manifest does not round-trip its own encoding".to_owned(),
			));
		}
		publish_version(&root_dir(&self.base), manifest.root_version, ROOT_EXT, &bytes)
	}

	/// Newest state for `branch`, probing forward from `hint` (0 = no hint:
	/// fall back to a listing). `Ok(None)` = the branch never flushed.
	pub(crate) fn load_state(
		&self,
		branch: BranchId,
		generation: BranchGeneration,
		hint: u64,
	) -> Result<Option<BranchStateManifest>> {
		let dir = branch_state_dir(&self.base, &branch);
		let resolved = if hint == 0 {
			match latest_version_in(&dir, STATE_EXT)? {
				Some(version) => {
					let bytes = read_version(&dir, version, STATE_EXT)?.ok_or_else(|| {
						Error::Corruption(format!(
							"state version {version} vanished between list and read"
						))
					})?;
					Some((version, bytes))
				}
				None => None,
			}
		} else {
			resolve_from_hint(&dir, hint, STATE_EXT, STATE_PROBE_LIMIT)?
		};
		let Some((version, bytes)) = resolved else {
			return Ok(None);
		};
		let manifest = BranchStateManifest::decode(&bytes)?;
		if manifest.state_version != version {
			return Err(Error::Corruption(format!(
				"state header version {} does not match filename version {version} for branch {branch:?}",
				manifest.state_version
			)));
		}
		if manifest.branch != branch {
			return Err(Error::Corruption(format!(
				"state file under branch {branch:?} names branch {:?}",
				manifest.branch
			)));
		}
		if manifest.generation != generation {
			return Err(Error::Corruption(format!(
				"state for branch {branch:?} carries generation {} but the catalog says {}",
				manifest.generation.0, generation.0
			)));
		}
		Ok(Some(manifest))
	}

	/// Prunes every lineage to [`KEEP_METADATA_VERSIONS`], returning how many
	/// files were removed. `live_owners` are the branches whose state lineages
	/// exist; a deleted branch's lineage is pruned to the same depth rather than
	/// removed outright, so a tombstoned branch still leaves evidence.
	pub(crate) fn prune_metadata(
		&self,
		live_owners: &[(BranchId, BranchGeneration)],
	) -> Result<usize> {
		let mut removed =
			prune_versions(&catalog_dir(&self.base), CATALOG_EXT, KEEP_METADATA_VERSIONS)?;
		removed += prune_versions(&root_dir(&self.base), ROOT_EXT, KEEP_METADATA_VERSIONS)?;
		for (branch, _) in live_owners {
			removed += prune_versions(
				&branch_state_dir(&self.base, branch),
				STATE_EXT,
				KEEP_METADATA_VERSIONS,
			)?;
		}
		Ok(removed)
	}

	/// Removes one deleted owner's now-unreachable state lineage. The durable
	/// tombstone remains in the catalog until this succeeds, so a failed removal
	/// is retried by maintenance and cannot make a live owner disappear.
	pub(crate) fn retire_state_lineage(&self, branch: BranchId) -> Result<usize> {
		let dir = branch_state_dir(&self.base, &branch);
		let versions = versions_in(&dir, STATE_EXT)?.len();
		match std::fs::remove_dir_all(&dir) {
			Ok(()) => {
				if let Some(parent) = dir.parent() {
					sync_dir(parent)?;
				}
				Ok(versions)
			}
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
			Err(error) => Err(error.into()),
		}
	}

	pub(crate) fn publish_state(&self, manifest: &BranchStateManifest) -> Result<PublishOutcome> {
		let bytes = manifest.encode()?;
		let reparsed = BranchStateManifest::decode(&bytes)?;
		if &reparsed != manifest {
			return Err(Error::Corruption(
				"branch state manifest does not round-trip its own encoding".to_owned(),
			));
		}
		publish_version(
			&branch_state_dir(&self.base, &manifest.branch),
			manifest.state_version,
			STATE_EXT,
			&bytes,
		)
	}
}
