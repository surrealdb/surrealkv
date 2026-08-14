//! The branch catalog: the runtime view of the durable authority's branch
//! records, their generations, COW parent links and fork anchors.

use std::collections::BTreeMap;

use super::api::{BranchGeneration, BranchId, ErrorCode, KernelError, KernelResult};

pub(crate) const DEFAULT_BRANCH_NAME: &str = "main";

/// Where a fork cuts its view of the parent. Every point is exact: the child
/// sees precisely the rows a reader of the parent would have seen at that
/// point, including rows still only in the parent's memtables, because the
/// child resolves the parent's live state rather than a set of files
/// (design §3.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForkPoint {
	/// The parent's visible head at the instant of the fork, established by
	/// draining the commit pipeline under the write fence.
	Head,
	/// A specific sequence, which must be at or below the drained head and at
	/// or above the parent's retention floor.
	AtVersion(u64),
	/// The highest sequence committed at or before this timestamp, resolved
	/// exactly or refused (`Error::TimestampBelowHorizon`).
	AtTimestamp(u64),
}

/// What a completed fork returns internally, and what an idempotent retry
/// returns, so a caller that lost the response can re-issue the same request.
///
/// Crate-internal: it names owners, and `BatchOwner` is not part of the public
/// surface. `Tree::fork_branch` hands back a [`BranchHandle`] instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ForkReceipt {
	pub(crate) child: crate::batch::BatchOwner,
	pub(crate) parent: crate::batch::BatchOwner,
	pub(crate) fork_seq: u64,
}

/// Ancestor-chain depth budget (strata-parity, plan D6): resolution beyond
/// this demands materialization rather than degrading.
pub(crate) const MAX_VIEW_DEPTH: usize = 64;

/// Where a branch came from: the parent it was forked off and the anchor its
/// view of that parent is capped at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BranchLineage {
	pub branch: BranchId,
	pub generation: BranchGeneration,
	/// Rows the parent committed after this sequence are invisible to the
	/// child, permanently.
	pub fork_seq: u64,
}

/// A branch's catalog facts, as of the moment it was read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchInfo {
	pub name: String,
	pub id: BranchId,
	/// Incarnations of a name are distinguished by generation; a stale
	/// generation is fenced rather than silently re-bound.
	pub generation: BranchGeneration,
	/// Global commit sequence when this incarnation was created.
	pub created_at_seq: u64,
	/// `None` for a root branch such as `main`.
	pub parent: Option<BranchLineage>,
	/// Newest sequence this branch wrote ITSELF — not a per-branch head, which
	/// a single global commit clock does not have. `None` means the branch has
	/// never written, so it reads purely through its inherited view.
	pub last_write_seq: Option<u64>,
	/// Absolute expiry on the store's clock; the maintenance sweep tombstones
	/// the branch once it passes.
	pub expires_at: Option<u64>,
}

use crate::authority::format::MAX_BRANCH_NAME_LEN;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BranchRecord {
	pub(crate) id: BranchId,
	pub(crate) name: String,
	pub(crate) generation: BranchGeneration,
	pub(crate) deleted: bool,
	/// Global commit sequence when THIS incarnation was created — the
	/// generation-fence anchor (a predecessor generation's records are all at
	/// or below it) and an allocator restart-safety input.
	pub(crate) created_at_seq: u64,
	/// Global commit sequence when this incarnation was deleted.
	pub(crate) deleted_at_seq: Option<u64>,
	/// Branch TTL for agentic sandboxes; enforcement is a maintenance sweep.
	pub(crate) expires_at: Option<u64>,
	/// COW lineage: the parent this branch was forked from, with the fork
	/// anchor `fork_seq` (the child reads the parent capped at this seq).
	pub(crate) parent: Option<crate::authority::format::ParentLink>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BranchCatalog {
	default_branch: BranchId,
	records: BTreeMap<BranchId, BranchRecord>,
	live_names: BTreeMap<String, BranchId>,
	/// Globally monotone generation allocator: generations are unique across
	/// every branch that ever existed (never per-name ordinals), so catalog
	/// tombstone reclamation can never enable stale-generation acceptance.
	/// The default branch holds the reserved generation 0; allocation starts
	/// at 1.
	next_generation: u64,
}

impl BranchCatalog {
	pub(crate) fn new(default_branch: BranchId) -> Self {
		let record = BranchRecord {
			id: default_branch,
			name: DEFAULT_BRANCH_NAME.to_owned(),
			generation: BranchGeneration(0),
			deleted: false,
			created_at_seq: 0,
			deleted_at_seq: None,
			expires_at: None,
			parent: None,
		};
		Self {
			default_branch,
			records: BTreeMap::from([(default_branch, record)]),
			live_names: BTreeMap::from([(DEFAULT_BRANCH_NAME.to_owned(), default_branch)]),
			next_generation: 1,
		}
	}

	#[cfg(test)]
	pub(crate) fn default_branch(&self) -> BranchId {
		self.default_branch
	}

	pub(crate) fn next_generation(&self) -> u64 {
		self.next_generation
	}

	pub(crate) fn create(
		&mut self,
		id: BranchId,
		name: &str,
		created_at_seq: u64,
	) -> KernelResult<BranchRecord> {
		validate_branch_name(name)?;
		if self.records.contains_key(&id) || self.live_names.contains_key(name) {
			return Err(KernelError::new(ErrorCode::AlreadyExists, "branch already exists"));
		}
		let generation = BranchGeneration(self.next_generation);
		self.next_generation = self.next_generation.checked_add(1).ok_or_else(|| {
			KernelError::new(ErrorCode::ResourceExhausted, "branch generation allocator exhausted")
		})?;
		let record = BranchRecord {
			id,
			name: name.to_owned(),
			generation,
			deleted: false,
			created_at_seq,
			deleted_at_seq: None,
			expires_at: None,
			parent: None,
		};
		self.records.insert(id, record.clone());
		self.live_names.insert(name.to_owned(), id);
		Ok(record)
	}

	/// Creates a fork child entry: like [`Self::create`] plus the COW parent
	/// link. FK4's fork protocol is the production caller; FK3 exercises it
	/// through the test seam.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn create_fork(
		&mut self,
		id: BranchId,
		name: &str,
		created_at_seq: u64,
		parent: crate::authority::format::ParentLink,
	) -> KernelResult<BranchRecord> {
		if !self
			.records
			.get(&parent.parent)
			.is_some_and(|record| !record.deleted && record.generation == parent.parent_generation)
		{
			return Err(KernelError::new(
				ErrorCode::NotFound,
				"fork parent is not a live branch at that generation",
			));
		}
		let record = self.create(id, name, created_at_seq)?;
		let record_mut = self.records.get_mut(&id).expect("record was just inserted");
		record_mut.parent = Some(parent);
		Ok(BranchRecord {
			parent: record_mut.parent.clone(),
			..record
		})
	}

	/// Clears a branch's parent link after its inherited view has been copied
	/// into its own tables.
	///
	/// The link is the only thing that makes the branch read through an
	/// ancestor, so dropping it both shortens the chain and releases the
	/// retention pin its anchor placed on the parent.
	pub(crate) fn detach(
		&mut self,
		branch: BranchId,
		generation: BranchGeneration,
	) -> KernelResult<()> {
		let record = self
			.records
			.get_mut(&branch)
			.filter(|record| !record.deleted && record.generation == generation)
			.ok_or_else(|| KernelError::new(ErrorCode::NotFound, "branch not found"))?;
		record.parent = None;
		Ok(())
	}

	/// Minimum fork anchor across Active children of `(parent, generation)` —
	/// the retention-promise pin floor: the parent's compaction must preserve
	/// every version at or below this sequence (§3.3a of the FK design).
	pub(crate) fn min_active_child_anchor(
		&self,
		parent: BranchId,
		parent_generation: BranchGeneration,
	) -> Option<u64> {
		self.records
			.values()
			.filter(|record| !record.deleted)
			.filter_map(|record| record.parent.as_ref())
			.filter(|link| link.parent == parent && link.parent_generation == parent_generation)
			.map(|link| link.fork_seq)
			.min()
	}

	/// Whether `(branch, generation)` itself is a fork child (reads stack
	/// inherited views; its own compaction may never treat its bottom level
	/// as the bottom of its read stack).
	pub(crate) fn record_has_parent(&self, branch: BranchId, generation: BranchGeneration) -> bool {
		self.records.get(&branch).is_some_and(|record| {
			!record.deleted && record.generation == generation && record.parent.is_some()
		})
	}

	/// The ancestor chain for a live owner, nearest-first, as
	/// `(owner, fork_seq_cap_into_that_ancestor)`. Depth-capped fail-closed.
	pub(crate) fn parent_chain(
		&self,
		branch: BranchId,
		generation: BranchGeneration,
		max_depth: usize,
	) -> crate::error::Result<Vec<(BranchId, BranchGeneration, u64)>> {
		let mut chain = Vec::new();
		let mut current = self
			.records
			.get(&branch)
			.filter(|record| !record.deleted && record.generation == generation);
		while let Some(record) = current {
			let Some(link) = record.parent.as_ref() else {
				break;
			};
			if chain.len() >= max_depth {
				return Err(crate::error::Error::MaterializationRequired {
					depth: chain.len(),
				});
			}
			chain.push((link.parent, link.parent_generation, link.fork_seq));
			let parent_record = self.records.get(&link.parent).filter(|parent_record| {
				!parent_record.deleted && parent_record.generation == link.parent_generation
			});
			if parent_record.is_none() {
				return Err(crate::error::Error::Corruption(format!(
					"branch {:?} names a parent that is not live at the linked generation",
					record.id
				)));
			}
			current = parent_record;
		}
		Ok(chain)
	}

	pub(crate) fn get(&self, id: BranchId) -> KernelResult<&BranchRecord> {
		self.records
			.get(&id)
			.filter(|record| !record.deleted)
			.ok_or_else(|| KernelError::new(ErrorCode::NotFound, "branch not found"))
	}

	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn get_by_name(&self, name: &str) -> KernelResult<&BranchRecord> {
		let id = self
			.live_names
			.get(name)
			.ok_or_else(|| KernelError::new(ErrorCode::NotFound, "branch not found"))?;
		self.get(*id)
	}

	pub(crate) fn validate_owner(
		&self,
		id: BranchId,
		generation: BranchGeneration,
	) -> KernelResult<&BranchRecord> {
		let record = self
			.records
			.get(&id)
			.ok_or_else(|| KernelError::new(ErrorCode::Fenced, "branch identity is stale"))?;
		if record.deleted || record.generation != generation {
			return Err(KernelError::new(ErrorCode::Fenced, "branch generation is stale"));
		}
		Ok(record)
	}

	/// Live branches in catalog order (tombstones excluded).
	pub(crate) fn list(&self) -> impl Iterator<Item = &BranchRecord> {
		self.live_names.values().filter_map(|id| self.records.get(id))
	}

	pub(crate) fn delete(&mut self, id: BranchId, deleted_at_seq: u64) -> KernelResult<bool> {
		if id == self.default_branch {
			return Err(KernelError::new(
				ErrorCode::InvalidArgument,
				"default branch cannot be deleted",
			));
		}
		// §3.3b: an Active child's view is resolved through its parent's live
		// state, so a parent may not be tombstoned while any child names it. The
		// catalog loader enforces the same invariant, which means publishing such
		// a tombstone would make the store unopenable.
		let generation = self.records.get(&id).map(|record| record.generation);
		if let Some(generation) = generation {
			if self.min_active_child_anchor(id, generation).is_some() {
				return Err(KernelError::new(
					ErrorCode::Conflict,
					"branch has active fork children and cannot be deleted",
				));
			}
		}
		let record = self
			.records
			.get_mut(&id)
			.ok_or_else(|| KernelError::new(ErrorCode::NotFound, "branch not found"))?;
		if record.deleted {
			return Ok(false);
		}
		record.deleted = true;
		record.deleted_at_seq = Some(deleted_at_seq);
		self.live_names.remove(&record.name);
		Ok(true)
	}

	/// Sets or clears a branch's expiry. `None` makes the branch permanent.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn set_expiry(&mut self, id: BranchId, expires_at: Option<u64>) -> KernelResult<()> {
		if id == self.default_branch {
			return Err(KernelError::new(
				ErrorCode::InvalidArgument,
				"the default branch cannot expire",
			));
		}
		let record = self
			.records
			.get_mut(&id)
			.filter(|record| !record.deleted)
			.ok_or_else(|| KernelError::new(ErrorCode::NotFound, "branch not found"))?;
		record.expires_at = expires_at;
		Ok(())
	}

	/// Tombstones every live branch whose expiry has passed, returning their
	/// names in catalog order.
	///
	/// A parent of Active children is skipped rather than cascaded: deleting it
	/// would break views its children still resolve through, and deleting the
	/// subtree would destroy branches whose own TTL has not fired. Its tombstone
	/// lands on the first sweep after the last child is gone.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn expire_due(&mut self, now: u64, deleted_at_seq: u64) -> Vec<String> {
		let due: Vec<(BranchId, String)> = self
			.records
			.values()
			.filter(|record| !record.deleted)
			.filter(|record| record.expires_at.is_some_and(|expiry| expiry <= now))
			.map(|record| (record.id, record.name.clone()))
			.collect();
		let mut expired = Vec::new();
		for (id, name) in due {
			if self.delete(id, deleted_at_seq).unwrap_or(false) {
				expired.push(name);
			}
		}
		expired
	}

	/// Every record, including tombstones — the durable catalog encodes all.
	pub(crate) fn all_records(&self) -> impl Iterator<Item = &BranchRecord> {
		self.records.values()
	}

	/// Rebuilds the runtime catalog from a decoded durable manifest.
	pub(crate) fn from_manifest(
		default_branch: BranchId,
		manifest: &crate::authority::format::CatalogManifest,
	) -> crate::error::Result<Self> {
		let mut records = BTreeMap::new();
		let mut live_names = BTreeMap::new();
		for entry in &manifest.entries {
			let deleted = entry.status == crate::authority::format::BranchStatus::Deleted;
			let record = BranchRecord {
				id: entry.branch,
				name: entry.name.clone(),
				generation: entry.generation,
				deleted,
				created_at_seq: entry.created_at_seq,
				deleted_at_seq: entry.deleted_at_seq,
				expires_at: entry.expires_at,
				parent: entry.parent.clone(),
			};
			if !deleted && live_names.insert(entry.name.clone(), entry.branch).is_some() {
				return Err(crate::error::Error::Corruption(format!(
					"catalog manifest lists two live branches named {:?}",
					entry.name
				)));
			}
			records.insert(entry.branch, record);
		}
		if records.get(&default_branch).is_none_or(|record| record.deleted) {
			return Err(crate::error::Error::Corruption(
				"catalog manifest is missing the live default branch".to_owned(),
			));
		}
		// Invariant 3 (resolvability), enforced at load ahead of FK5's delete
		// guard: an Active child's parent must be Active at the linked
		// generation, or the child's reads have nothing to resolve through.
		for record in records.values().filter(|record| !record.deleted) {
			if let Some(link) = record.parent.as_ref() {
				let parent_live = records.get(&link.parent).is_some_and(|parent_record| {
					!parent_record.deleted && parent_record.generation == link.parent_generation
				});
				if !parent_live {
					return Err(crate::error::Error::Corruption(format!(
						"catalog manifest: active branch {:?} names a parent that is not live at the linked generation",
						record.id
					)));
				}
			}
		}
		Ok(Self {
			default_branch,
			records,
			live_names,
			next_generation: manifest.next_generation,
		})
	}

	/// Encodes the runtime catalog as durable entries, sorted by branch id.
	pub(crate) fn to_entries(&self) -> Vec<crate::authority::format::CatalogEntry> {
		use crate::authority::format::{BranchStatus, CatalogEntry};
		self.records
			.values()
			.map(|record| CatalogEntry {
				branch: record.id,
				name: record.name.clone(),
				generation: record.generation,
				status: if record.deleted {
					BranchStatus::Deleted
				} else {
					BranchStatus::Active
				},
				created_at_seq: record.created_at_seq,
				parent: record.parent.clone(),
				deleted_at_seq: record.deleted_at_seq,
				expires_at: record.expires_at,
			})
			.collect()
	}

	/// Maximum version anchor across ALL records including tombstones: the
	/// recovered clock must never fall below a catalog-referenced sequence,
	/// or generation fences would eat legitimate new commits.
	pub(crate) fn max_version_anchor(&self) -> u64 {
		self.records
			.values()
			.map(|record| record.created_at_seq.max(record.deleted_at_seq.unwrap_or(0)))
			.max()
			.unwrap_or(0)
	}
}

fn validate_branch_name(name: &str) -> KernelResult<()> {
	if name.is_empty()
		|| name.len() > MAX_BRANCH_NAME_LEN
		|| name != name.trim()
		|| name.bytes().any(|byte| byte.is_ascii_control())
	{
		return Err(KernelError::new(ErrorCode::InvalidArgument, "invalid branch name"));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn catalog_delete_recreate_allocates_globally_fresh_generation_and_fences_stale_owner() {
		let main = BranchId::from_u128(1);
		let first = BranchId::from_u128(2);
		let second = BranchId::from_u128(3);
		let mut catalog = BranchCatalog::new(main);
		let original = catalog.create(first, " agent/work", 0);
		assert_eq!(original.unwrap_err().code, ErrorCode::InvalidArgument);
		let original = catalog.create(first, "agent/work", 4).unwrap();
		assert_eq!(original.generation, BranchGeneration(1), "allocation starts above default's 0");
		assert_eq!(original.created_at_seq, 4);
		assert!(catalog.delete(first, 7).unwrap());
		assert!(!catalog.delete(first, 7).unwrap());
		// Global allocator: the recreated name gets a globally fresh
		// generation — never a per-name ordinal — so tombstone reclamation
		// can never enable stale-generation acceptance.
		let recreated = catalog.create(second, "agent/work", 8).unwrap();
		assert_eq!(recreated.generation, BranchGeneration(2));
		assert!(recreated.generation != original.generation);
		let stale = catalog.validate_owner(second, original.generation);
		assert_eq!(stale.unwrap_err().code, ErrorCode::Fenced);
		// Anchors survive on the tombstone for clock restart safety.
		assert_eq!(catalog.max_version_anchor(), 8);
	}

	#[test]
	fn catalog_protects_default_and_lists_only_live_names() {
		let main = BranchId::from_u128(1);
		let child = BranchId::from_u128(2);
		let mut catalog = BranchCatalog::new(main);
		catalog.create(child, "child", 3).unwrap();
		assert_eq!(catalog.default_branch(), main);
		assert_eq!(catalog.get_by_name("child").unwrap().id, child);
		assert_eq!(catalog.list().count(), 2);
		assert_eq!(catalog.delete(main, 9).unwrap_err().code, ErrorCode::InvalidArgument);
		catalog.delete(child, 9).unwrap();
		assert_eq!(catalog.list().count(), 1);
		assert_eq!(catalog.get_by_name("child").unwrap_err().code, ErrorCode::NotFound);
	}

	#[test]
	fn deleted_branch_owner_is_fenced_even_before_name_recreation() {
		let main = BranchId::from_u128(1);
		let child = BranchId::from_u128(2);
		let mut catalog = BranchCatalog::new(main);
		let record = catalog.create(child, "child", 0).unwrap();
		catalog.delete(child, 9).unwrap();
		let error = catalog.validate_owner(record.id, record.generation).unwrap_err();
		assert_eq!(error.code, ErrorCode::Fenced);
	}
}
