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

/// Every sequence cap at which some durable reader still reads one owner
/// exactly — what its compaction must preserve (design §3.3a).
///
/// Two kinds, making the same promise. A live child's fork anchor: its
/// inherited view is re-resolved at that cap on every read. And a live merge
/// edge's target-side base: the next merge from that source compares the target
/// against how it stood there. The second kind was missing until FK6, which is
/// why a parent compaction could silently move a merge's base.
///
/// A single anchor cannot stand in for several. Pinning only the lowest leaves
/// a child forked higher up reading a version that was never current at its
/// anchor; pinning only the highest drops what the lowest needs; range-pinning
/// to the highest retains the parent's whole history for as long as anything is
/// forked at head. So the set is kept as a set, and compaction preserves the
/// newest version at or below *each* of them.
///
/// Sorted descending and deduplicated, which is what lets [`AnchorWalker`]
/// serve every anchor in one pass over a key's versions.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RetentionAnchors {
	anchors: Vec<u64>,
}

impl RetentionAnchors {
	/// Sorts and deduplicates. Nothing is ever dropped for size: an anchor is a
	/// promise, and the count is already bounded by the catalog's own entry cap.
	fn from_unsorted(mut anchors: Vec<u64>) -> Self {
		anchors.sort_unstable_by(|a, b| b.cmp(a));
		anchors.dedup();
		Self {
			anchors,
		}
	}

	#[cfg(test)]
	pub(crate) fn from_iter_for_test(anchors: impl IntoIterator<Item = u64>) -> Self {
		Self::from_unsorted(anchors.into_iter().collect())
	}

	/// No child, no merge edge: compaction behaves exactly as it would in a
	/// store that never forked.
	pub(crate) fn is_empty(&self) -> bool {
		self.anchors.is_empty()
	}

	/// The highest anchor, or zero when there is none. Sequences start at 1, so
	/// zero pins nothing.
	pub(crate) fn highest(&self) -> u64 {
		self.anchors.first().copied().unwrap_or(0)
	}

	/// Whether any anchor lies in `[low, high)`.
	///
	/// The bottom-level hard-delete shortcut asks this: an anchor inside the
	/// span between a key's oldest version and the tombstone above it is a
	/// reader that would see the data without seeing the delete.
	pub(crate) fn any_in(&self, low: u64, high: u64) -> bool {
		self.anchors.iter().any(|&anchor| anchor >= low && anchor < high)
	}

	/// Whether a view of this owner capped at `cap` still reads exactly.
	///
	/// Either the cap is at or above the retention floor — no key has lost a
	/// version that a view up there would need — or the cap is one of the pinned
	/// anchors, where compaction preserved the answer on purpose. Any other cap
	/// below the floor reads whatever happened to survive, which is a guess.
	pub(crate) fn view_is_complete_at(&self, cap: u64, retained_floor: u64) -> bool {
		cap >= retained_floor || self.anchors.contains(&cap)
	}

	/// An anchor the catalog holds now that `sampled` did not.
	///
	/// A compaction job samples the anchor set before it merges and re-checks it
	/// under the publication lock. An anchor that appeared in between may need a
	/// version the job already discarded, so the output is refused. An anchor
	/// that *vanished* is harmless: the job merely over-retained.
	pub(crate) fn appeared_since(&self, sampled: &Self) -> Option<u64> {
		self.anchors.iter().copied().find(|anchor| !sampled.anchors.contains(anchor))
	}

	/// A cursor for one key's versions, which must be offered newest-first.
	pub(crate) fn walker(&self) -> AnchorWalker<'_> {
		AnchorWalker {
			anchors: &self.anchors,
			next: 0,
		}
	}
}

/// Decides, for one key, which of its versions the anchors pin.
///
/// Anchors descend and versions arrive newest-first, so one shared index is
/// enough: when a version's sequence drops at or below the highest anchor that
/// nothing has served yet, that version is the newest at or below it — every
/// version seen so far was higher — and it serves that anchor and any others it
/// has just passed.
pub(crate) struct AnchorWalker<'a> {
	anchors: &'a [u64],
	next: usize,
}

impl AnchorWalker<'_> {
	/// Whether this version is the newest at or below an anchor no newer
	/// version already answered for.
	pub(crate) fn serves(&mut self, seq: u64) -> bool {
		let before = self.next;
		while self.next < self.anchors.len() && self.anchors[self.next] >= seq {
			self.next += 1;
		}
		self.next > before
	}
}

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
	/// Merges already applied INTO this branch, one per source. Sorted by
	/// source id, which the format also requires.
	pub(crate) merges: Vec<crate::authority::format::MergeEdge>,
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
			merges: Vec::new(),
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
			merges: Vec::new(),
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

	/// Records that `source` has been merged into `branch` up to the given
	/// sequences, replacing any previous edge from the same source.
	///
	/// Per source, never global: a merge from one child must not shift the base
	/// another child's merge is compared against, or the second merge would
	/// silently overwrite the first one's keys (plan C5).
	pub(crate) fn record_merge(
		&mut self,
		branch: BranchId,
		generation: BranchGeneration,
		edge: crate::authority::format::MergeEdge,
	) -> KernelResult<()> {
		if edge.source == branch {
			return Err(KernelError::new(
				ErrorCode::InvalidArgument,
				"a branch cannot record a merge from itself",
			));
		}
		let record = self
			.records
			.get_mut(&branch)
			.filter(|record| !record.deleted && record.generation == generation)
			.ok_or_else(|| KernelError::new(ErrorCode::NotFound, "branch not found"))?;
		match record.merges.binary_search_by(|existing| existing.source.0.cmp(&edge.source.0)) {
			Ok(index) => record.merges[index] = edge,
			Err(index) => record.merges.insert(index, edge),
		}
		Ok(())
	}

	/// The merge edge recorded on `branch` for `source`, if any.
	pub(crate) fn merge_edge(
		&self,
		branch: BranchId,
		source: BranchId,
	) -> Option<crate::authority::format::MergeEdge> {
		let record = self.records.get(&branch)?;
		record
			.merges
			.binary_search_by(|existing| existing.source.0.cmp(&source.0))
			.ok()
			.map(|index| record.merges[index])
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

	/// Every cap `(owner, generation)`'s compaction must keep readable: the fork
	/// anchor of each Active child, and the target-side base of each merge edge
	/// whose source is still live (§3.3a of the FK design, as corrected by FK6).
	///
	/// A stale edge pins nothing. Its source is gone, so no future merge can
	/// compare against that base — and holding history for a branch that no
	/// longer exists is how a store that churns sandbox branches never reclaims
	/// anything.
	pub(crate) fn retention_anchors(
		&self,
		owner: BranchId,
		generation: BranchGeneration,
	) -> RetentionAnchors {
		let mut anchors: Vec<u64> = self
			.records
			.values()
			.filter(|record| !record.deleted)
			.filter_map(|record| record.parent.as_ref())
			.filter(|link| link.parent == owner && link.parent_generation == generation)
			.map(|link| link.fork_seq)
			.collect();
		if let Some(record) = self.live_record(owner, generation) {
			anchors.extend(
				record
					.merges
					.iter()
					.filter(|edge| self.live_record(edge.source, edge.source_generation).is_some())
					.map(|edge| edge.target_through_seq),
			);
		}
		RetentionAnchors::from_unsorted(anchors)
	}

	/// The record for a branch that exists at exactly this generation.
	fn live_record(&self, branch: BranchId, generation: BranchGeneration) -> Option<&BranchRecord> {
		self.records
			.get(&branch)
			.filter(|record| !record.deleted && record.generation == generation)
	}

	/// Minimum fork anchor across Active children of `(parent, generation)`.
	///
	/// The delete guard's question — "does anything still fork off this?" — not
	/// a retention pin: [`BranchCatalog::retention_anchors`] is what compaction
	/// preserves.
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
				merges: entry.merges.clone(),
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
				merges: record.merges.clone(),
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

	// ===== FK6: retention anchors =====

	fn anchors(values: impl IntoIterator<Item = u64>) -> RetentionAnchors {
		RetentionAnchors::from_iter_for_test(values)
	}

	/// Which version of a key each anchor is answered by. Versions descend, as
	/// the compaction iterator offers them, and each anchor must be served by the
	/// newest version at or below it — exactly one version per anchor, never the
	/// range between them.
	#[test]
	fn every_anchor_is_served_by_the_newest_version_at_or_below_it() {
		let pins = anchors([100, 10]);
		let mut walker = pins.walker();
		assert!(!walker.serves(200), "nothing is pinned above the highest anchor");
		assert!(walker.serves(50), "50 is the newest version at or below 100");
		assert!(!walker.serves(20), "100 is already answered and 20 is above 10");
		assert!(walker.serves(5), "5 is the newest version at or below 10");
		assert!(!walker.serves(1), "both anchors are answered");
	}

	/// One version can answer several anchors at once, and does not thereby stop
	/// answering: the walker consumes every anchor it passes.
	#[test]
	fn one_version_can_serve_several_anchors() {
		let pins = anchors([100, 80, 60]);
		let mut walker = pins.walker();
		assert!(walker.serves(50), "50 is the newest version at or below all three");
		assert!(!walker.serves(40), "nothing is left to serve");
	}

	#[test]
	fn anchors_are_deduplicated_and_ordered_and_an_empty_set_pins_nothing() {
		assert_eq!(anchors([10, 100, 10]), anchors([100, 10]));
		assert_eq!(anchors([100, 10]).highest(), 100);
		let empty = anchors([]);
		assert!(empty.is_empty());
		assert_eq!(empty.highest(), 0, "sequences start at 1, so zero pins nothing");
		assert!(!empty.walker().serves(1));
	}

	/// The completeness predicate both the fork check and the merge check ask.
	#[test]
	fn a_view_is_complete_above_the_floor_or_exactly_on_an_anchor() {
		let pins = anchors([100, 10]);
		assert!(pins.view_is_complete_at(500, 200), "at or above the floor");
		assert!(pins.view_is_complete_at(200, 200), "the floor itself");
		assert!(!pins.view_is_complete_at(150, 200), "below the floor, not an anchor");
		assert!(pins.view_is_complete_at(100, 200), "below the floor but pinned");
		assert!(pins.view_is_complete_at(10, 200), "the lower anchor is pinned too");
		assert!(!pins.view_is_complete_at(99, 200), "next to an anchor is not on it");
		assert!(
			anchors([]).view_is_complete_at(150, 0),
			"with no floor advance every cap is complete, anchors or not"
		);
		assert!(
			!anchors([]).view_is_complete_at(150, 200),
			"and with no anchors only the floor saves it"
		);
	}

	/// What a compaction job re-checks before publishing. An anchor that appeared
	/// after the sample may need a version the job discarded; one that vanished
	/// only means the job over-retained.
	#[test]
	fn only_an_anchor_that_appeared_since_the_sample_refuses_a_publish() {
		let sampled = anchors([100, 10]);
		assert_eq!(sampled.appeared_since(&sampled), None);
		assert_eq!(anchors([100]).appeared_since(&sampled), None, "one vanished: safe");
		assert_eq!(anchors([]).appeared_since(&sampled), None, "all vanished: safe");
		assert_eq!(anchors([100, 50, 10]).appeared_since(&sampled), Some(50), "one appeared");
		assert_eq!(anchors([500]).appeared_since(&sampled), Some(500), "even a higher one");
	}

	#[test]
	fn any_in_reports_an_anchor_inside_a_half_open_span() {
		let pins = anchors([100, 10]);
		assert!(pins.any_in(50, 200), "100 is inside");
		assert!(pins.any_in(100, 101), "the low bound is inclusive");
		assert!(!pins.any_in(101, 200), "and 100 is below it");
		assert!(!pins.any_in(11, 100), "the high bound is exclusive, so 100 is out");
		assert!(pins.any_in(5, 100), "but 10 is in");
		assert!(!pins.any_in(11, 99), "neither anchor is in the gap");
	}

	/// Both kinds of anchor, and the liveness rules that release them.
	#[test]
	fn anchors_come_from_live_children_and_live_merge_edges() {
		let main = BranchId::from_u128(1);
		let child = BranchId::from_u128(2);
		let gone = BranchId::from_u128(3);
		let mut catalog = BranchCatalog::new(main);
		let main_generation = catalog.get(main).unwrap().generation;
		let link = |seq| crate::authority::format::ParentLink {
			parent: main,
			parent_generation: main_generation,
			fork_seq: seq,
		};
		let child_record = catalog.create_fork(child, "child", 10, link(10)).unwrap();
		let gone_record = catalog.create_fork(gone, "gone", 20, link(20)).unwrap();
		assert_eq!(catalog.retention_anchors(main, main_generation), anchors([10, 20]));

		let edge = |source, generation, target_through| crate::authority::format::MergeEdge {
			source,
			source_generation: generation,
			source_through_seq: 0,
			target_through_seq: target_through,
		};
		catalog
			.record_merge(main, main_generation, edge(child, child_record.generation, 300))
			.unwrap();
		catalog
			.record_merge(main, main_generation, edge(gone, gone_record.generation, 400))
			.unwrap();
		assert_eq!(
			catalog.retention_anchors(main, main_generation),
			anchors([10, 20, 300, 400]),
			"a merge edge pins the base the next merge from that source reads at"
		);

		catalog.delete(gone, 99).unwrap();
		assert_eq!(
			catalog.retention_anchors(main, main_generation),
			anchors([10, 300]),
			"a deleted source releases both its fork anchor and its edge"
		);
		assert!(
			catalog.retention_anchors(child, child_record.generation).is_empty(),
			"a childless branch nothing merges into pins nothing"
		);
	}
}
