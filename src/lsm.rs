use std::collections::HashSet;
use std::fs::create_dir_all;
#[cfg(not(target_os = "windows"))]
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::batch::Batch;
use crate::branch::{BranchInfo, BranchLineage, ForkPoint};
use crate::branch_runtime::{BranchRuntime, BranchRuntimeRegistry};
use crate::checkpoint::{CheckpointMetadata, DatabaseCheckpoint};
use crate::commit::{CommitEnv, CommitPipeline, PreparedWrite};
use crate::compaction::compactor::{CompactionOptions, Compactor};
use crate::compaction::CompactionStrategy;
use crate::error::{BackgroundErrorHandler, BackgroundErrorReason, Result};
use crate::levels::{LevelManifest, ManifestChangeSet};
use crate::lockfile::LockFile;
use crate::memtable::{ImmutableMemtables, MemTable};
use crate::snapshot::SnapshotTracker;
use crate::sstable::table::Table;
use crate::stall::{StallCounts, StallThresholds, WriteStallCountProvider};
use crate::task::TaskManager;
use crate::transaction::{Mode, Transaction, TransactionOptions};
use crate::wal::dependency::WalDependencyTracker;
use crate::wal::recovery::{repair_corrupted_wal_segment, replay_wal};
use crate::wal::{self, cleanup_old_segments, Wal, WalManager};
use crate::{Comparator, Error, FilterPolicy, Options, WalRecoveryMode};

/// Replayed memtables paired with the WAL segment each was recovered from.
pub(crate) type RecoveredMemtables = Vec<(Arc<MemTable>, u64)>;

// ===== Compaction Operations Trait =====
/// Defines the compaction operations that can be performed on an LSM tree.
/// Compaction is essential for maintaining read performance by merging
/// overlapping SSTables and removing deleted entries.
pub trait CompactionOperations: Send + Sync {
	/// Flushes the active memtable to disk, converting it into an immutable
	/// SSTable. This is the first step in the LSM tree's write path.
	fn compact_memtable(&self) -> Result<()>;

	/// Performs compaction according to the specified strategy.
	/// Compaction merges SSTables to reduce read amplification and remove
	/// tombstones.
	fn compact(&self, strategy: Arc<dyn CompactionStrategy>) -> Result<()>;

	/// Returns a reference to the background error handler
	fn error_handler(&self) -> Arc<BackgroundErrorHandler>;

	/// Returns true if there are immutable memtables pending flush.
	fn has_pending_immutables(&self) -> bool;

	/// Rotates at most ONE runtime whose non-empty active memtable pins a WAL
	/// segment too far behind the active one (the trickle WAL-span policy).
	/// Returns whether a victim was rotated. Default: no policy.
	fn rotate_wal_pinned_runtime(&self) -> Result<bool> {
		Ok(false)
	}

	/// One re-entrant branch-metadata maintenance step: expire due branches and
	/// prune authority lineages. Runs at the tail of a maintenance wake rather
	/// than on a timer, so no executor needs a resident clock. Default: nothing
	/// to maintain.
	fn sweep_branch_maintenance(&self) -> Result<(usize, usize)> {
		Ok((0, 0))
	}
}

// ===== Core LSM Tree Implementation =====
/// The core of an LSM (Log-Structured Merge) tree implementation.
///
/// # LSM Tree Overview
/// An LSM tree optimizes for write performance by buffering writes in memory
/// and periodically flushing them to disk as sorted, immutable files
/// (SSTables). Reads must check multiple locations: the active memtable,
/// immutable memtables, and multiple levels of SSTables.
///
/// # Components
/// - **Active Memtable**: An in-memory, mutable data structure (usually a skip list or B-tree) that
///   receives all new writes.
/// - **Immutable Memtables**: Former active memtables that are full and awaiting flush to disk.
///   They serve reads but accept no new writes.
/// - **SSTables**: Sorted String Tables on disk, organized into levels. Each level has
///   progressively larger SSTables with non-overlapping key ranges (except L0).
/// - **Compaction**: Background process that merges SSTables to maintain read performance and
///   remove deleted entries.
///
/// # Lock Ordering
///
/// To prevent deadlocks, locks must be acquired in this order:
/// 1. `active_memtable` - serializes writes
/// 2. `level_manifest` - SST metadata and table IDs
/// 3. `immutable_memtables` - pending flush queue
///
/// Read locks and write locks follow the same ordering.
/// If a function needs multiple locks, it must acquire them in this order.
/// See `rotate_memtable()`, `flush_immutable_to_sst()` for examples.
/// Serialized catalog-publish facts (version + epochs + lazy-bump flag).
pub(crate) struct CatalogPublishState {
	pub(crate) catalog_version: u64,
	pub(crate) writer_epoch: u64,
	pub(crate) maintenance_epoch: u64,
	session_bumped: bool,
}

static BRANCH_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn mint_identity(opts: &Options) -> [u8; 16] {
	let nanos = opts.clock.now();
	let discriminant = (std::process::id() as u64) << 32
		| BRANCH_ID_COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFFFF_FFFF;
	CoreInner::mint_identity_bytes(nanos, discriminant)
}

pub(crate) struct CoreInner {
	/// Durable-model branch identity catalog. Data routing remains on the
	/// existing LSM components and is integrated incrementally.
	pub(crate) branch_catalog: Arc<RwLock<crate::branch::BranchCatalog>>,
	/// Complete component set for the retained default branch: the active
	/// memtable (write buffer), the immutable-memtable flush queue, and the
	/// owned level manifest. `CoreInner` temporarily dereferences to this
	/// runtime so existing call sites keep using the same objects while the
	/// extraction proceeds. The same `Arc` is the registry's default entry.
	pub(crate) default_runtime: Arc<BranchRuntime>,

	/// Owner-indexed runtimes. Non-default runtimes are created lazily on the
	/// owner's first routed write; an idle branch allocates no arena.
	pub(crate) runtimes: BranchRuntimeRegistry,

	/// Configuration options controlling LSM tree behavior
	pub opts: Arc<Options>,

	/// Tracker for active snapshot sequence numbers for MVCC (Multi-Version
	/// Concurrency Control). Snapshots provide consistent point-in-time views
	/// of the data. The tracker stores actual sequence numbers to enable
	/// snapshot-aware compaction.
	pub(crate) snapshot_tracker: SnapshotTracker,

	/// Tracker for ALL active transaction `start_seq_num`s. Used as the
	/// watermark source for `CommitOracle` GC. Separate from
	/// `snapshot_tracker` because write-only txns need GC protection but
	/// don't hold MVCC snapshots, and the oracle's required watermark may
	/// advance faster than snapshot retention permits.
	pub(crate) active_txn_tracker: Arc<crate::tracker::ActiveTxnTracker>,

	/// Write-Ahead Log (WAL) for durability
	pub(crate) wal: WalManager,

	/// Live dependencies that prevent WAL replay-floor advancement. A commit
	/// is pinned before append and handed to its destination memtable after
	/// successful apply.
	pub(crate) wal_dependencies: WalDependencyTracker,

	/// Lock file to prevent multiple processes from opening the same database
	pub(crate) lockfile: Mutex<LockFile>,

	/// Background error handler
	pub(crate) error_handler: Arc<BackgroundErrorHandler>,

	/// Visible sequence number - the highest sequence number that is visible to readers.
	/// Shared with CommitPipeline for coordinated updates.
	pub(crate) visible_seq_num: Arc<AtomicU64>,

	/// Durable lineage access for catalog publication.
	pub(crate) authority: crate::authority::store::AuthorityStore,

	/// Catalog version + epochs; every durable catalog op serializes here.
	pub(crate) catalog_publish: Mutex<CatalogPublishState>,

	/// Recovered clock floor from catalog anchors and the root's visible
	/// floor; the clock seed takes the max of this, the states, and WAL
	/// replay (a deleted branch's sequences stay allocated forever).
	pub(crate) clock_floor: u64,

	/// The global commit timeline (FK2): stamped per commit under the write
	/// mutex, seeded at open from the root tail + WAL replay, snapshotted
	/// into every root version. Shared with `LevelManifest`.
	pub(crate) timeline: Arc<crate::timeline::Timeline>,
}

/// Transitional compatibility for the default-branch extraction. Field
/// access resolves to the sole `BranchRuntime`; there are no alias fields or
/// duplicated component collections in `CoreInner`.
impl std::ops::Deref for CoreInner {
	type Target = BranchRuntime;

	fn deref(&self) -> &Self::Target {
		&self.default_runtime
	}
}

impl CoreInner {
	/// Creates a new LSM tree core instance
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		Self::new_impl(opts)
	}

	/// Opens the durable authority (catalog lineage first — it is recovery's
	/// fencing authority), hydrates the runtime manifest from state lineages,
	/// and seeds the clock so it can never fall below a catalog anchor.
	fn new_impl(opts: Arc<Options>) -> Result<Self> {
		// Acquire database lock to prevent multiple processes from opening the same
		// database
		let mut lockfile = LockFile::new(&opts.path);
		lockfile.acquire()?;

		// Initialize immutable memtables
		let immutable_memtables = Arc::new(RwLock::new(ImmutableMemtables::default()));

		// Load or create the durable catalog: the sole authority for branch
		// existence, generation, anchors, and TTLs.
		let loaded_catalog =
			crate::authority::store::AuthorityStore::load_latest_catalog(&opts.path)?;
		let (branch_catalog, db_id, catalog_version, writer_epoch, maintenance_epoch) =
			match loaded_catalog {
				Some(manifest) => {
					let catalog = crate::branch::BranchCatalog::from_manifest(
						crate::BranchId::DEFAULT,
						&manifest,
					)?;
					(
						catalog,
						manifest.db_id,
						manifest.catalog_version,
						manifest.writer_epoch,
						manifest.maintenance_epoch,
					)
				}
				None => {
					let db_id = mint_identity(&opts);
					let catalog = crate::branch::BranchCatalog::new(crate::BranchId::DEFAULT);
					let manifest = crate::authority::format::CatalogManifest {
						db_id,
						catalog_version: 1,
						next_generation: catalog.next_generation(),
						writer_epoch: 0,
						maintenance_epoch: 0,
						entries: catalog.to_entries(),
					};
					crate::authority::store::AuthorityStore::new(opts.path.clone(), db_id)
						.publish_catalog(&manifest)?;
					(catalog, db_id, 1, 0, 0)
				}
			};
		// The engine addresses the default branch by the reserved
		// `BranchId::DEFAULT` in every batch, memtable, and SST. A catalog
		// that cannot validate that identity would fence every default
		// transaction, so refuse it at open.
		if branch_catalog
			.validate_owner(
				crate::batch::BatchOwner::DEFAULT.branch,
				crate::batch::BatchOwner::DEFAULT.generation,
			)
			.is_err()
		{
			return Err(Error::InvalidArgument(
				"branch catalog does not validate the reserved default branch identity".to_owned(),
			));
		}

		let authority = crate::authority::store::AuthorityStore::new(opts.path.clone(), db_id);
		let root = authority.load_latest_root()?;
		if let Some(root) = &root {
			if root.catalog_version_floor > catalog_version {
				return Err(Error::Corruption(format!(
					"latest catalog version {catalog_version} is below the root's reclaim floor {}",
					root.catalog_version_floor
				)));
			}
		}
		let timeline = Arc::new(crate::timeline::Timeline::new());
		if let Some(root) = &root {
			timeline.seed(&root.timeline_tail);
			timeline.observe_floor(root.last_commit_ts);
		}
		let manifest = LevelManifest::hydrate(
			Arc::clone(&opts),
			authority.clone(),
			&branch_catalog,
			catalog_version,
			root.as_ref(),
			Arc::clone(&timeline),
		)?;
		let manifest_log_number = manifest.get_log_number();
		// The recovered clock must never fall below a catalog anchor (a
		// deleted branch's sequences stay allocated) or the root's published
		// visible floor — the BR6 fenced-batch rule extended to the catalog.
		let clock_floor = branch_catalog
			.max_version_anchor()
			.max(root.as_ref().map(|r| r.visible_seq).unwrap_or(0));

		// Initialize WAL starting from manifest.log_number
		let wal_path = opts.wal_dir();
		// This avoids creating intermediate empty WAL files
		let wal_instance =
			Wal::open_with_min_log_number(&wal_path, manifest_log_number, wal::Options::default())?;

		// Starts at 0 since no commits have happened yet.
		let visible_seq_num = Arc::new(AtomicU64::new(0));
		let initial_memtable = Arc::new(MemTable::new_owned(
			opts.max_memtable_size,
			crate::batch::BatchOwner::DEFAULT,
		));
		initial_memtable.set_wal_number(wal_instance.get_active_log_number());
		let active_memtable = Arc::new(RwLock::new(initial_memtable));

		let level_manifest = Arc::new(RwLock::new(manifest));

		let default_runtime = Arc::new(BranchRuntime::new(
			crate::batch::BatchOwner::DEFAULT,
			active_memtable,
			immutable_memtables,
			level_manifest,
		));
		if !default_runtime.validate_component_owners() {
			return Err(Error::ManifestCorruption(
				"default branch runtime contains a foreign-owned component".to_owned(),
			));
		}
		let runtimes = BranchRuntimeRegistry::new(Arc::clone(&default_runtime));

		Ok(Self {
			branch_catalog: Arc::new(RwLock::new(branch_catalog)),
			opts,
			default_runtime,
			runtimes,
			snapshot_tracker: SnapshotTracker::new(),
			active_txn_tracker: Arc::new(crate::tracker::ActiveTxnTracker::new()),
			wal: WalManager::new(wal_instance),
			wal_dependencies: WalDependencyTracker::new(),
			lockfile: Mutex::new(lockfile),
			error_handler: Arc::new(BackgroundErrorHandler::new()),
			visible_seq_num,
			authority,
			catalog_publish: Mutex::new(CatalogPublishState {
				catalog_version,
				writer_epoch,
				maintenance_epoch,
				session_bumped: false,
			}),
			clock_floor,
			timeline,
		})
	}

	/// Mints a 16-byte identity from the injected clock, the process id, and
	/// a counter. Not a global-uniqueness claim (the adapter phase mints real
	/// ULIDs); sufficient for identity-mismatch detection across lineages.
	fn mint_identity_bytes(nanos: u64, discriminant: u64) -> [u8; 16] {
		let mut id = [0u8; 16];
		id[..8].copy_from_slice(&nanos.to_le_bytes());
		id[8..].copy_from_slice(&discriminant.to_le_bytes());
		id
	}

	/// Durable branch creation: mutates the runtime catalog and publishes the
	/// next catalog version. The catalog mutation is rolled back if the
	/// publish fails, so the runtime never runs ahead of the authority.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn create_branch(&self, name: &str) -> Result<crate::batch::BatchOwner> {
		let created_at_seq = self.visible_seq_num.load(Ordering::Acquire);
		let mut publish = self.catalog_publish.lock().unwrap();
		let mut catalog = self.branch_catalog.write()?;
		let snapshot = catalog.clone();
		let id = self.mint_branch_id(&catalog);
		let record = catalog.create(id, name, created_at_seq).map_err(|error| {
			Error::InvalidArgument(format!("branch create rejected: {}", error.message))
		})?;
		let owner = crate::batch::BatchOwner {
			branch: id,
			generation: record.generation,
		};
		if let Err(error) = self.publish_catalog_locked(&mut publish, &catalog) {
			*catalog = snapshot;
			return Err(error);
		}
		Ok(owner)
	}

	/// FK3 seam: publishes a fork child's catalog entry and nothing else.
	///
	/// This is the read/retention half of a fork — the half FK3 owns. It takes
	/// `fork_seq` as given instead of establishing it, so it deliberately lacks
	/// everything FK4 adds: the drain-to-visible commit fence that makes the
	/// anchor exact, the bounded delta table covering committed-but-undurable
	/// rows, and crash-recovery of a partial fork. Because of that it may only
	/// be called from tests, with an anchor the test already knows is durable.
	/// FK4 replaces it with the real protocol and this method is deleted.
	#[cfg(test)]
	pub(crate) fn create_fork_entry_for_test(
		&self,
		name: &str,
		parent: crate::batch::BatchOwner,
		fork_seq: u64,
	) -> Result<crate::batch::BatchOwner> {
		let mut publish = self.catalog_publish.lock().unwrap();
		let mut catalog = self.branch_catalog.write()?;
		let snapshot = catalog.clone();
		let id = self.mint_branch_id(&catalog);
		let record = catalog
			.create_fork(
				id,
				name,
				fork_seq,
				crate::authority::format::ParentLink {
					parent: parent.branch,
					parent_generation: parent.generation,
					fork_seq,
				},
			)
			.map_err(|error| {
				Error::InvalidArgument(format!("fork create rejected: {}", error.message))
			})?;
		let owner = crate::batch::BatchOwner {
			branch: id,
			generation: record.generation,
		};
		if let Err(error) = self.publish_catalog_locked(&mut publish, &catalog) {
			*catalog = snapshot;
			return Err(error);
		}
		Ok(owner)
	}

	/// Sets or clears a branch's expiry, publishing one catalog version.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn set_branch_expiry(
		&self,
		branch: crate::BranchId,
		expires_at: Option<u64>,
	) -> Result<()> {
		let mut publish = self.catalog_publish.lock().unwrap();
		let mut catalog = self.branch_catalog.write()?;
		let snapshot = catalog.clone();
		catalog.set_expiry(branch, expires_at).map_err(|error| {
			Error::InvalidArgument(format!("branch expiry rejected: {}", error.message))
		})?;
		if let Err(error) = self.publish_catalog_locked(&mut publish, &catalog) {
			*catalog = snapshot;
			return Err(error);
		}
		Ok(())
	}

	/// One maintenance step over branch metadata: tombstone expired branches,
	/// then prune every authority lineage to its retained depth.
	///
	/// Re-entrant and idempotent by construction — it recomputes what is due from
	/// the catalog each time rather than holding a cursor — so it can run at the
	/// tail of a maintenance wake on any executor, including the request-driven
	/// ones the object and edge adapters will use. Returns
	/// `(branches expired, metadata files removed)`.
	pub(crate) fn sweep_branch_maintenance(&self) -> Result<(usize, usize)> {
		let now = self.opts.clock.now();
		let deleted_at_seq = self.visible_seq_num.load(Ordering::Acquire);

		let mut publish = self.catalog_publish.lock().unwrap();
		let expired = {
			let mut catalog = self.branch_catalog.write()?;
			let snapshot = catalog.clone();
			let expired = catalog.expire_due(now, deleted_at_seq);
			if expired.is_empty() {
				Vec::new()
			} else {
				if let Err(error) = self.publish_catalog_locked(&mut publish, &catalog) {
					*catalog = snapshot;
					return Err(error);
				}
				expired
			}
		};
		if !expired.is_empty() {
			log::debug!("branch maintenance expired {} branch(es): {:?}", expired.len(), expired);
		}

		let reclaimed = self.reclaim_tombstoned_branches()?;
		if reclaimed > 0 {
			log::debug!("branch maintenance reclaimed {reclaimed} table(s) from deleted branches");
		}

		// Pruning runs after the publish, so the version this sweep just created
		// is inside the retained window and can never be its own victim.
		let owners: Vec<_> = self
			.branch_catalog
			.read()?
			.all_records()
			.map(|record| (record.id, record.generation))
			.collect();
		let removed = self.authority.prune_metadata(&owners)?;
		Ok((expired.len(), removed))
	}

	/// Copies everything `owner` inherits into its own tables and clears its
	/// parent link, so it stops resolving through an ancestor.
	///
	/// This is the relief valve for the two costs a long-lived fork imposes: it
	/// shortens the ancestor chain that every read walks, and it releases the
	/// retention pin its anchor placed on the parent, letting the parent's
	/// compaction reclaim versions it was holding.
	///
	/// The materialized tables are placed BELOW every level the branch already
	/// occupies. That is not a detail: reads return the first table containing a
	/// key, level by level, so inherited rows sitting above the branch's own
	/// compacted rows would shadow them with stale values.
	///
	/// Returns the number of rows copied. Detaching a branch with no parent is a
	/// no-op, not an error, so a caller can detach unconditionally.
	pub(crate) fn detach_branch(
		&self,
		core: &Arc<Core>,
		owner: crate::batch::BatchOwner,
	) -> Result<u64> {
		let mut publish = self.catalog_publish.lock().unwrap();
		{
			let catalog = self.branch_catalog.read()?;
			catalog
				.validate_owner(owner.branch, owner.generation)
				.map_err(|_| Error::BranchFenced)?;
			if !catalog.record_has_parent(owner.branch, owner.generation) {
				return Ok(0);
			}
		}

		// State before catalog. A crash between the two leaves the branch
		// holding both the materialized tables and its parent link: every
		// inherited row is then present twice, at identical sequences with
		// identical values, which reads collapse and compaction reclaims. The
		// other order would lose the inherited data outright.
		let rows = self.materialize_inherited(core, owner)?;

		let mut catalog = self.branch_catalog.write()?;
		let snapshot_catalog = catalog.clone();
		catalog.detach(owner.branch, owner.generation).map_err(|error| {
			Error::InvalidArgument(format!("detach rejected: {}", error.message))
		})?;
		if let Err(error) = self.publish_catalog_locked(&mut publish, &catalog) {
			*catalog = snapshot_catalog;
			return Err(error);
		}
		Ok(rows)
	}

	/// The durable half of a detach: copies everything `owner` inherits into its
	/// own tables and publishes its state, WITHOUT touching the parent link.
	///
	/// Separate from `detach_branch` because it is the half that must land
	/// first, and because the state it leaves behind — materialized tables plus
	/// a still-live parent link — is exactly the crash window, so it can be
	/// tested rather than merely argued.
	pub(crate) fn materialize_inherited(
		&self,
		core: &Arc<Core>,
		owner: crate::batch::BatchOwner,
	) -> Result<u64> {
		// Read the inherited view at the current visible head. Anything the
		// branch itself wrote is excluded: it is already in its own tables, and
		// copying it would duplicate rows at their original sequences.
		let visible = self.visible_seq_num.load(Ordering::Acquire);
		let Some(snapshot) =
			crate::snapshot::Snapshot::inherited_only(Arc::clone(core), visible, owner)?
		else {
			return Ok(0);
		};

		let target_level = self.detach_target_level(owner)?;
		let table_id = self.level_manifest.read()?.next_table_id();
		let path = self.opts.sstable_file_path(table_id);

		// Every version and every tombstone comes across, in internal-key order,
		// which is both what makes the result self-contained and what
		// `TableWriter` requires.
		let rows = {
			use crate::LSMIterator as _;
			let iter_state = snapshot.collect_iter_state()?;
			let mut merge = crate::snapshot::KMergeIterator::new_from(
				iter_state,
				crate::user_range_to_internal_range(
					std::ops::Bound::Unbounded,
					std::ops::Bound::Unbounded,
				),
			);
			let file = std::fs::File::create(&path)?;
			let mut writer = crate::sstable::table::TableWriter::new_owned(
				file,
				table_id,
				Arc::clone(&self.opts),
				target_level,
				owner,
			);
			let mut rows = 0u64;
			let mut valid = merge.seek_first()?;
			while valid {
				writer.add(merge.key().to_owned(), merge.value_encoded()?)?;
				rows += 1;
				valid = merge.next()?;
			}
			if rows == 0 {
				drop(writer);
				let _ = std::fs::remove_file(&path);
			} else {
				writer.finish()?;
				crate::vfs::fsync_file(&path)?;
				fsync_directory(self.opts.sstable_dir())?;
			}
			rows
		};

		if rows > 0 {
			let table = {
				let file = std::fs::File::open(&path)?;
				let file: Arc<dyn crate::vfs::File> = Arc::new(file);
				let file_size = file.size()?;
				Arc::new(crate::sstable::table::Table::new(
					table_id,
					Arc::clone(&self.opts),
					file,
					file_size,
				)?)
			};
			let mut manifest = self.level_manifest.write()?;
			let changeset = crate::levels::ManifestChangeSet {
				owner,
				new_tables: vec![(target_level, table)],
				..crate::levels::ManifestChangeSet::default()
			};
			let rollback = manifest.apply_changeset(&changeset)?;
			if let Err(error) = manifest.persist_owner_update(owner) {
				manifest.revert_changeset(rollback);
				let _ = std::fs::remove_file(&path);
				return Err(error);
			}
		}
		Ok(rows)
	}

	/// The level a detach writes into: one below the deepest level the branch
	/// already occupies, so its own rows are always found first.
	fn detach_target_level(&self, owner: crate::batch::BatchOwner) -> Result<u8> {
		let manifest = self.level_manifest.read()?;
		let deepest_occupied = manifest.levels_for(owner).and_then(|levels| {
			levels
				.get_levels()
				.iter()
				.enumerate()
				.filter(|(_, level)| !level.tables.is_empty())
				.map(|(index, _)| index)
				.next_back()
		});
		let target = match deepest_occupied {
			Some(deepest) => deepest + 1,
			None => 0,
		};
		if target >= self.opts.level_count as usize {
			return Err(Error::InvalidArgument(format!(
				"branch {:?} occupies every level; compact it before detaching",
				owner.branch
			)));
		}
		Ok(target as u8)
	}

	/// Frees the data of branches the catalog has tombstoned, returning how many
	/// tables were deleted.
	///
	/// This is the runtime half of a rule the store already applies at open: a
	/// table is live exactly when some non-deleted owner's level set names it
	/// (`LevelManifest::hydrate` skips deleted records, so their tables are
	/// already reclaimed by `cleanup_orphaned_sst_files` on the next start). No
	/// reference counts and no deletion journal are involved — the manifest IS
	/// the proof, because a fork child holds no physical reference to its
	/// parent's tables. See `docs/removed-surfaces.md`.
	///
	/// Files are unlinked with no grace period. An in-flight reader holds an
	/// `Arc<Table>` that owns the descriptor, so its reads keep working after
	/// the unlink — the same property compaction has always relied on when it
	/// deletes its inputs.
	fn reclaim_tombstoned_branches(&self) -> Result<usize> {
		let tombstoned: Vec<crate::batch::BatchOwner> = self
			.branch_catalog
			.read()?
			.all_records()
			.filter(|record| record.deleted)
			.map(|record| crate::batch::BatchOwner {
				branch: record.id,
				generation: record.generation,
			})
			.collect();
		if tombstoned.is_empty() {
			return Ok(0);
		}

		let mut deleted = 0usize;
		for owner in tombstoned {
			// Drop the runtime FIRST: once it is gone the flush loop cannot pick
			// this owner up again, and any flush already past that point is
			// refused by the liveness guard inside the manifest critical section.
			for memtable in self.runtimes.reclaim(owner) {
				// A discarded memtable still holds a WAL dependency. Releasing it
				// is what lets the segments it pinned be reclaimed; skipping this
				// would trade a bounded table leak for an unbounded WAL leak.
				self.wal_dependencies.release_component(memtable.dependency_id());
			}

			let reclaimed = {
				let mut manifest = self.level_manifest.write()?;
				let tables = manifest.reclaim_owner(owner);
				if !tables.is_empty() {
					// The root still carries a state hint for this owner; publish
					// so the durable record stops naming a lineage nothing loads.
					manifest.persist_root()?;
				}
				tables
			};

			for table in reclaimed {
				let path = self.opts.sstable_file_path(table.id);
				match std::fs::remove_file(&path) {
					Ok(()) => deleted += 1,
					// Already gone: the previous open reclaimed it, or a
					// concurrent sweep won the race. Same outcome either way.
					Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
					Err(error) => {
						log::warn!("failed to reclaim table {}: {error}", path.display());
					}
				}
			}
		}
		Ok(deleted)
	}

	/// Durable branch deletion (tombstone). Same rollback-on-publish-failure
	/// contract as creation.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn delete_branch(&self, branch: crate::BranchId) -> Result<()> {
		let deleted_at_seq = self.visible_seq_num.load(Ordering::Acquire);
		let mut publish = self.catalog_publish.lock().unwrap();
		let mut catalog = self.branch_catalog.write()?;
		let snapshot = catalog.clone();
		catalog.delete(branch, deleted_at_seq).map_err(|error| {
			Error::InvalidArgument(format!("branch delete rejected: {}", error.message))
		})?;
		if let Err(error) = self.publish_catalog_locked(&mut publish, &catalog) {
			*catalog = snapshot;
			return Err(error);
		}
		Ok(())
	}

	fn mint_branch_id(&self, catalog: &crate::branch::BranchCatalog) -> crate::BranchId {
		loop {
			let nanos = self.opts.clock.now();
			let discriminant = (std::process::id() as u64) << 32
				| BRANCH_ID_COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFFFF_FFFF;
			let id = crate::BranchId(Self::mint_identity_bytes(nanos, discriminant));
			if id != crate::BranchId::DEFAULT && catalog.get(id).is_err() {
				return id;
			}
		}
	}

	/// Publishes the runtime catalog as the next catalog version. The
	/// writer epoch bumps lazily, folded into the session's first real
	/// catalog write (an open that never writes bumps nothing).
	fn publish_catalog_locked(
		&self,
		publish: &mut CatalogPublishState,
		catalog: &crate::branch::BranchCatalog,
	) -> Result<()> {
		let next_version = publish.catalog_version + 1;
		let writer_epoch = if publish.session_bumped {
			publish.writer_epoch
		} else {
			publish.writer_epoch + 1
		};
		let manifest = crate::authority::format::CatalogManifest {
			db_id: self.authority.db_id,
			catalog_version: next_version,
			next_generation: catalog.next_generation(),
			writer_epoch,
			maintenance_epoch: publish.maintenance_epoch,
			entries: catalog.to_entries(),
		};
		self.authority.publish_catalog(&manifest)?;
		publish.catalog_version = next_version;
		publish.writer_epoch = writer_epoch;
		publish.session_bumped = true;
		// Mirrored for root publication only after the publish succeeded.
		self.level_manifest.read()?.catalog_version.store(next_version, Ordering::Release);
		Ok(())
	}

	/// Smallest `start_seq_num` of any currently-live transaction (read-write
	/// snapshot OR write-only). Used by the `CommitOracle` as its GC
	/// threshold: entries with `committed_seq < oldest_active_start_seq` can
	/// be discarded because no live txn could prove non-conflict against them.
	///
	/// If no txns are alive, falls back to `visible_seq_num`. NOTE: this
	/// fallback is unsafe-by-default — if a caller drives `Core::commit`
	/// without first registering in `active_txn_tracker`, the fallback may
	/// exceed that caller's snapshot. The commit pipeline clamps the value
	/// returned here by the committing txn's `start_seq` (see
	/// `CommitPipeline::commit`), which neutralizes the fallback for all
	/// production paths.
	pub(crate) fn oldest_active_start_seq(&self) -> u64 {
		let snap = self.snapshot_tracker.first();
		let txn = self.active_txn_tracker.oldest();
		match (snap, txn) {
			(Some(a), Some(b)) => a.min(b),
			(Some(a), None) => a,
			(None, Some(b)) => b,
			(None, None) => self.visible_seq_num.load(Ordering::Acquire),
		}
	}

	/// Flushes a memtable to SST and atomically updates the manifest.
	///
	/// This is the core primitive used by all flush operations. It handles:
	/// 1. Flushing the memtable to disk as an SSTable
	/// 2. Creating a changeset with the new SST and log_number update
	/// 3. Atomically applying the changeset to the manifest
	/// 4. Removing the memtable from immutable_memtables tracking
	///
	/// # Arguments
	/// * `memtable` - The memtable to flush
	/// * `table_id` - Table ID for the new SST
	/// * `wal_number` - WAL number to mark as flushed (log_number = wal_number + 1)
	///
	/// # Returns
	/// The flushed SSTable
	/// Test seam for the reclamation race: drives one flush directly so the
	/// "branch deleted while its table was being written" interleaving is
	/// reproducible without timing.
	#[cfg(test)]
	pub(crate) fn flush_immutable_to_sst_for_test(
		&self,
		memtable: Arc<MemTable>,
		table_id: u64,
		wal_number: u64,
	) -> Result<Arc<Table>> {
		self.flush_immutable_to_sst(memtable, table_id, wal_number)
	}

	fn flush_immutable_to_sst(
		&self,
		memtable: Arc<MemTable>,
		table_id: u64,
		wal_number: u64,
	) -> Result<Arc<Table>> {
		// Step 1: Flush the inline-value memtable to an SST.
		let table = memtable.flush(table_id, Arc::clone(&self.opts)).map_err(|e| {
			Error::Other(format!("Failed to flush memtable to SST table_id={}: {}", table_id, e))
		})?;

		log::debug!("Created SST table_id={}, file_size={}", table.id, table.file_size);

		// Step 2: Apply the table and the safe replay floor atomically. A WAL
		// segment can feed multiple memtables (recovery splits and, later,
		// branches), so flushing one component does not prove the whole segment
		// reclaimable.
		let current_wal_segment = self.wal.read().get_active_log_number();
		let dependency_snapshot = self
			.wal_dependencies
			.snapshot_excluding(Some(memtable.dependency_id()), current_wal_segment);
		let owning_runtime = self.runtimes.get(memtable.owner()).ok_or_else(|| {
			Error::Other(format!(
				"flushing memtable for owner {:?} with no live runtime",
				memtable.owner()
			))
		})?;
		let mut manifest = self.level_manifest.write()?;

		// Maintenance may have reclaimed this branch while the table above was
		// being written. Check INSIDE the manifest critical section, which is
		// what serialises this against the sweep — the liveness check at the top
		// of this function runs before the lock and cannot close the window.
		//
		// Without this the flush would not merely fail: `apply_changeset` calls
		// `ensure_owner_levels`, which recreates a missing level set, so the
		// deleted branch would be silently resurrected and published.
		let owner = memtable.owner();
		if self.branch_catalog.read()?.validate_owner(owner.branch, owner.generation).is_err() {
			drop(manifest);
			log::debug!(
				"flush abandoned: owner {owner:?} was reclaimed while its table was written"
			);
			let path = self.opts.sstable_file_path(table_id);
			if let Err(error) = std::fs::remove_file(&path) {
				log::warn!("failed to remove abandoned flush output {}: {error}", path.display());
			}
			// The memtable and its WAL dependency belong to the reclaimed
			// branch; the sweep released them when it dropped the runtime.
			return Err(Error::BranchFenced);
		}

		let mut memtable_lock = owning_runtime.immutable_memtables.write()?;
		let replay_floor = dependency_snapshot.replay_floor;
		let mut changeset = ManifestChangeSet {
			owner: memtable.owner(),
			..ManifestChangeSet::default()
		};
		changeset.new_tables.push((0, Arc::clone(&table)));
		changeset.log_number = Some(replay_floor);

		log::debug!(
			"Changeset prepared: table_id={}, replay_floor={} after flushing WAL #{:020}",
			table_id,
			replay_floor,
			wal_number
		);

		let rollback = manifest.apply_changeset(&changeset)?;
		if let Err(e) = manifest.persist_owner_update(memtable.owner()) {
			manifest.revert_changeset(rollback);
			let error = Error::Other(format!(
				"Failed to atomically update manifest: table_id={}, log_number={}: {}",
				table_id, replay_floor, e
			));
			self.error_handler.set_error(error.clone(), BackgroundErrorReason::ManifestWrite);
			return Err(error);
		}

		// Remove successfully flushed memtable from immutables tracking. The
		// Arc<MemTable> in `memtable` (function parameter) falls out of scope at
		// end of function — its data is now available via the SST that was just
		// added to the manifest, and conflict detection uses the in-memory oracle
		// (independent of memtables), so dropping this Arc is safe.
		memtable_lock.remove(table_id);
		self.wal_dependencies.release_component(memtable.dependency_id());
		drop(memtable);

		log::info!(
			"Manifest updated atomically: table_id={}, log_number={}, last_sequence={}",
			table_id,
			replay_floor,
			manifest.get_last_sequence()
		);

		Ok(table)
	}

	/// Installs replayed memtables per owner, mirroring the single-branch
	/// policy uniformly: for each owner, all-but-last flush to the owner's
	/// level set immediately; the last becomes the owner's runtime active
	/// memtable. Foreign runtimes are created here — they hold data, so they
	/// are not idle. Finally every runtime's active memtable is given the
	/// current WAL baseline (an empty active starts on the current segment; a
	/// recovered active keeps its recovered dependency, since record takes
	/// the min, while noting the current segment for future writes).
	pub(crate) fn install_recovered_memtables(
		&self,
		recovered: RecoveredMemtables,
		context: &str,
	) -> Result<()> {
		let mut recovered_by_owner: Vec<(crate::batch::BatchOwner, RecoveredMemtables)> =
			Vec::new();
		for (memtable, wal_number) in recovered {
			let owner = memtable.owner();
			match recovered_by_owner.iter_mut().find(|(existing, _)| *existing == owner) {
				Some((_, list)) => list.push((memtable, wal_number)),
				None => recovered_by_owner.push((owner, vec![(memtable, wal_number)])),
			}
		}
		for (owner, mut owner_memtables) in recovered_by_owner {
			let runtime = if owner == crate::batch::BatchOwner::DEFAULT {
				Arc::clone(&self.default_runtime)
			} else {
				self.runtimes.get_or_create(
					owner,
					self.opts.branch_memtable_size,
					&self.default_runtime.level_manifest,
				)?
			};

			let (last_memtable, last_wal_number) =
				owner_memtables.pop().expect("group is non-empty");

			for (memtable, wal_number) in owner_memtables {
				if memtable.is_empty() {
					continue;
				}
				self.wal_dependencies.register_component(memtable.dependency_id(), wal_number);
				let table_id = self.level_manifest.read()?.next_table_id();
				self.flush_immutable_to_sst(Arc::clone(&memtable), table_id, wal_number)?;
				log::info!(
					"{context}: flushed memtable to SST table_id={table_id}, owner={owner:?}, wal_number={wal_number}"
				);
			}

			self.wal_dependencies
				.register_component(last_memtable.dependency_id(), last_memtable.get_wal_number());
			log::info!(
				"{context}: installing last memtable (owner={owner:?}, wal={last_wal_number}) as the runtime active"
			);
			let mut active_memtable = runtime.active_memtable.write()?;
			*active_memtable = last_memtable;
		}

		let current_wal_number = self.wal.read().get_active_log_number();
		for runtime in self.runtimes.all() {
			let active_memtable = runtime.active_memtable.read()?;
			if active_memtable.is_empty() {
				active_memtable.set_wal_number(current_wal_number);
			} else {
				active_memtable.record_wal_dependency(current_wal_number);
			}
		}

		Ok(())
	}

	/// Rotates the active memtable to the immutable queue WITHOUT flushing to SST.
	/// This is a fast operation (no disk I/O) that:
	/// 1. Swaps the active memtable with a fresh one
	/// 2. Adds the old memtable to the immutable queue
	///
	/// The default-runtime convenience wrapper; see
	/// [`Self::rotate_runtime_memtable`].
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn rotate_memtable(&self) -> Result<()> {
		self.rotate_runtime_memtable(&self.default_runtime, 0)
	}

	/// Database-wide mutable-memory budget. When configured and the incoming
	/// allocation would exceed it, the largest active memtable across all
	/// runtimes is rotated toward flush. Best-effort back-pressure: failures
	/// here never fail the write (the budget bounds memory, not correctness).
	///
	/// Returns whether a rotation happened. The caller must then schedule the
	/// memtable flush task: it is event-driven, and an unflushed budget
	/// rotation both reclaims no memory and can park the victim's next write
	/// in the stall loop waiting for a flush that was never scheduled.
	pub(crate) fn enforce_write_buffer_budget(&self, incoming_bytes: u64) -> bool {
		let Some(budget) = self.opts.write_buffer_budget else {
			return false;
		};
		let total = self.runtimes.total_arena_capacity_bytes();
		if total.saturating_add(incoming_bytes) <= budget {
			return false;
		}
		// Victim = largest non-empty active memtable.
		let victim = self
			.runtimes
			.all()
			.into_iter()
			.filter(|runtime| {
				runtime.active_memtable.read().map(|memtable| !memtable.is_empty()).unwrap_or(false)
			})
			.max_by_key(|runtime| {
				runtime.active_memtable.read().map(|memtable| memtable.size() as u64).unwrap_or(0)
			});
		if let Some(victim) = victim {
			log::debug!(
				"write-buffer budget exceeded (total={total}, incoming={incoming_bytes}, budget={budget}); rotating owner {:?}",
				victim.owner()
			);
			match self.rotate_runtime_memtable(&victim, 0) {
				Ok(()) => return true,
				Err(error) => log::warn!("budget-driven rotation failed: {error}"),
			}
		}
		false
	}

	/// Rotates at most ONE runtime whose non-empty active memtable pins a WAL
	/// segment at least `wal_pinned_segment_limit` segments behind the active
	/// one — the victim with the OLDEST pinned segment. One victim per call
	/// keeps reclaim a trickle, never a reclaim-triggered mass flush: the
	/// memtable task flushes the rotated victim before asking again.
	pub(crate) fn rotate_wal_pinned_runtime_impl(&self) -> Result<bool> {
		let limit = self.opts.wal_pinned_segment_limit as u64;
		let current_segment = self.wal.read().get_active_log_number();
		let victim = self
			.runtimes
			.all()
			.into_iter()
			.filter_map(|runtime| {
				let pinned = {
					let active = runtime.active_memtable.read().ok()?;
					if active.is_empty() {
						return None;
					}
					active.get_wal_number()
				};
				(pinned.saturating_add(limit) <= current_segment).then_some((pinned, runtime))
			})
			.min_by_key(|(pinned, _)| *pinned);

		match victim {
			Some((pinned, runtime)) => {
				log::debug!(
					"WAL-span trickle: rotating owner {:?} pinned at segment {} (active segment {})",
					runtime.owner(),
					pinned,
					current_segment
				);
				self.rotate_runtime_memtable(&runtime, 0)?;
				Ok(true)
			}
			None => Ok(false),
		}
	}

	/// Rotates one branch runtime's memtable **without rotating the shared
	/// WAL**. Branch rotations are independent: the retired memtable keeps
	/// pinning the WAL segments it actually depends on (BR1 dependency
	/// tracking), and the WAL rotates on its own size policy at append time.
	///
	/// `min_capacity` right-sizes the replacement arena: a rotation forced by
	/// a batch larger than the branch arena must produce an arena that can
	/// hold that batch, or a WAL-durable batch could never apply.
	pub(crate) fn rotate_runtime_memtable(
		&self,
		runtime: &BranchRuntime,
		min_capacity: usize,
	) -> Result<()> {
		// Acquire WRITE lock upfront to prevent race conditions
		let mut active_memtable = runtime.active_memtable.write()?;

		if active_memtable.is_empty() && (min_capacity as u64) <= active_memtable.arena_available()
		{
			return Ok(());
		}

		log::debug!(
			"rotate_runtime_memtable: owner={:?} size={}",
			runtime.owner(),
			active_memtable.size()
		);

		// The retired memtable's recorded WAL number is its earliest actual
		// dependency segment (`record_wal_dependency` keeps the min); it
		// travels with it into the immutable queue, and replay floors come
		// from the dependency tracker.
		let flushed_wal_number = active_memtable.get_wal_number();
		let current_wal_number = self.wal.read().get_active_log_number();
		let retiree_is_empty = active_memtable.is_empty();

		let base_capacity = if runtime.owner() == crate::batch::BatchOwner::DEFAULT {
			self.opts.max_memtable_size
		} else {
			self.opts.branch_memtable_size
		};

		// Right-size against the reservation formula, not raw capacity (see
		// `MemTable::new_owned_admitting`): a replacement sized exactly to
		// the batch estimate could never admit it and a WAL-durable batch
		// would permanently fail to apply.
		let owner = runtime.owner();
		let replacement =
			Arc::new(MemTable::new_owned_admitting(base_capacity, min_capacity as u64, owner));

		// Swap memtable while STILL holding write lock
		let flushed_memtable = std::mem::replace(&mut *active_memtable, replacement);

		// The new (empty) memtable starts on the currently active segment.
		active_memtable.set_wal_number(current_wal_number);

		// An empty retiree (a right-sizing rotation) carries no data and no
		// recorded dependencies; queueing it would only occupy the flush path.
		if retiree_is_empty {
			drop(active_memtable);
			log::debug!("rotate_runtime_memtable: right-sized empty memtable, owner={owner:?}");
			return Ok(());
		}

		// LOCK ORDER: Get table_id from manifest BEFORE acquiring immutable_memtables lock.
		// This maintains consistent ordering: level_manifest -> immutable_memtables
		// which matches flush_immutable_to_sst() and prevents deadlock.
		//
		// Deadlock scenario prevented:
		//   Thread A (rotate): holds imm.write, waits manifest.read
		//   Thread B (flush):  holds manifest.write, waits imm.write
		// By acquiring manifest.read first, we ensure no circular wait.
		let table_id = self.level_manifest.read()?.next_table_id();
		let mut immutable_memtables = runtime.immutable_memtables.write()?;
		immutable_memtables.add(table_id, flushed_wal_number, Arc::clone(&flushed_memtable));

		// Release locks
		drop(active_memtable);
		drop(immutable_memtables);

		log::debug!(
			"rotate_runtime_memtable: completed rotation, owner={:?}, table_id={}, wal_number={}",
			owner,
			table_id,
			flushed_wal_number
		);

		Ok(())
	}

	/// Flushes the oldest immutable memtable of the first runtime with a
	/// non-empty queue (default runtime first) to an SSTable. One memtable
	/// per call; the background task loops until every queue drains.
	/// Returns Ok(Some(table)) if a memtable was flushed, Ok(None) if every
	/// queue was empty.
	fn flush_oldest_immutable_to_sst(&self) -> Result<Option<Arc<Table>>> {
		if let Some(table) = self.flush_oldest_immutable_for_runtime(&self.default_runtime)? {
			return Ok(Some(table));
		}
		for runtime in self.runtimes.all() {
			if runtime.owner() == self.default_runtime.owner() {
				continue;
			}
			if let Some(table) = self.flush_oldest_immutable_for_runtime(&runtime)? {
				return Ok(Some(table));
			}
		}
		Ok(None)
	}

	fn flush_oldest_immutable_for_runtime(
		&self,
		runtime: &BranchRuntime,
	) -> Result<Option<Arc<Table>>> {
		// Get the oldest immutable entry (clone to release lock before I/O)
		let entry = {
			let guard = runtime.immutable_memtables.read()?;
			guard.first().cloned()
		};

		let entry = match entry {
			Some(e) => e,
			None => {
				log::debug!("flush_oldest_immutable_to_sst: no immutables to flush");
				return Ok(None);
			}
		};

		// Skip empty memtables
		if entry.memtable.is_empty() {
			let mut guard = runtime.immutable_memtables.write()?;
			guard.remove(entry.table_id);
			self.wal_dependencies.release_component(entry.memtable.dependency_id());
			log::debug!(
				"flush_oldest_immutable_to_sst: skipped empty memtable table_id={}",
				entry.table_id
			);
			return Ok(None);
		}

		log::debug!(
			"flush_oldest_immutable_to_sst: flushing table_id={}, wal_number={}",
			entry.table_id,
			entry.wal_number
		);

		// Flush to SST (this also removes from immutable queue and updates manifest)
		let table = self.flush_immutable_to_sst(
			Arc::clone(&entry.memtable),
			entry.table_id,
			entry.wal_number,
		)?;

		// Schedule async WAL cleanup
		let wal_dir = self.wal.read().get_dir_path().to_path_buf();
		let min_wal_to_keep = self.level_manifest.read()?.get_log_number();

		tokio::spawn(async move {
			match cleanup_old_segments(&wal_dir, min_wal_to_keep) {
				Ok(count) if count > 0 => {
					log::info!(
						"Cleaned up {} old WAL segments (min_wal_to_keep={})",
						count,
						min_wal_to_keep
					);
				}
				Ok(_) => {}
				Err(e) => {
					log::warn!("Failed to clean up old WAL segments: {}", e);
				}
			}
		});

		log::debug!(
			"flush_oldest_immutable_to_sst: flushed table_id={}, file_size={}",
			table.id,
			table.file_size
		);

		Ok(Some(table))
	}

	#[cfg(test)]
	pub(crate) fn flush_oldest_immutable_for_test(&self) -> Result<Option<Arc<Table>>> {
		self.flush_oldest_immutable_to_sst()
	}

	/// Flushes ALL immutable memtables synchronously.
	/// Used by Tree::flush() and checkpoint for forced/sync flush.
	/// Blocks until all immutables are written to SST.
	pub(crate) fn flush_all_immutables_sync(&self) -> Result<usize> {
		let mut count = 0;
		while self.flush_oldest_immutable_to_sst()?.is_some() {
			count += 1;
		}
		if count > 0 {
			log::debug!("flush_all_immutables_sync: flushed {} immutable memtables", count);
		}
		Ok(count)
	}

	/// Flushes all memtables (immutable and active) during shutdown.
	///
	/// # Critical: Flush Ordering
	///
	/// Immutable memtables MUST be flushed BEFORE the active memtable to
	/// preserve SSTable ordering:
	/// - Immutable memtables contain OLDER data (were swapped out earlier)
	/// - Active memtable contains NEWEST data (currently receiving writes)
	/// - Table IDs must reflect temporal order (newer data = higher table_id)
	///
	/// # Flush Sequence
	///
	/// 1. Flush ALL immutable memtables FIRST using their pre-assigned table_ids
	/// 2. Flush active memtable LAST (gets new highest table_id)
	/// 3. Update manifest log_number to mark all WALs as flushed
	///
	/// # WAL Handling
	///
	/// This method does NOT rotate the WAL. The final log_number in manifest
	/// is set to current_wal + 1, indicating all data up to current WAL is
	/// persisted.
	fn flush_all_memtables_for_shutdown(&self) -> Result<()> {
		log::info!("Flushing all memtables for shutdown...");

		// STEP 1: Rotate every runtime's non-empty active memtable into its
		// immutable queue (memory-only; shutdown never rotates the WAL).
		// Entries already queued keep their older table_ids, so SST ordering
		// is preserved, and foreign runtimes are captured the same way as the
		// default — a dirty branch must not survive only in the WAL.
		let mut rotated_any = false;
		for runtime in self.runtimes.all() {
			let non_empty = !runtime.active_memtable.read()?.is_empty();
			if non_empty {
				self.rotate_runtime_memtable(&runtime, 0)?;
				rotated_any = true;
			}
		}

		// STEP 2: Drain every runtime's queue (default first, then branches).
		// Fail-fast is safe: flushed memtables advanced durable state; a
		// failed one keeps its WAL dependency and replays on restart.
		let flushed_count = self.flush_all_immutables_sync()?;

		if rotated_any || flushed_count > 0 {
			// Commits and maintenance are stopped by the shutdown caller. With all
			// components from this lifecycle covered by SSTs, the closed segment
			// can be skipped in full. Empty reopen/close cycles do not advance it.
			let current_wal = self.wal.read().get_active_log_number();
			let dependency_snapshot = self.wal_dependencies.snapshot(current_wal + 1);
			let changeset = ManifestChangeSet {
				log_number: Some(dependency_snapshot.replay_floor),
				..Default::default()
			};
			let mut manifest = self.level_manifest.write()?;
			let rollback = manifest.apply_changeset(&changeset)?;
			if let Err(error) = manifest.persist_root() {
				manifest.revert_changeset(rollback);
				let error =
					Error::Other(format!("Failed to finalize shutdown replay floor: {error}"));
				self.error_handler.set_error(error.clone(), BackgroundErrorReason::ManifestWrite);
				return Err(error);
			}
		}

		log::info!("All memtables flushed successfully for shutdown");
		Ok(())
	}

	/// Cleans up orphaned SST files not referenced in manifest
	/// Called during database startup to remove files from incomplete flushes
	///
	/// SAFETY: This is only safe because manifest updates are atomic.
	/// An SST file is orphaned if and only if the atomic manifest write
	/// (containing both SST addition and log_number update) never completed.
	/// In that case, the WAL is still alive and will replay the data.
	fn cleanup_orphaned_sst_files(&self) -> Result<()> {
		let sstable_dir = self.opts.sstable_dir();

		if !sstable_dir.exists() {
			return Ok(());
		}

		// Get all table IDs from manifest
		let manifest = self.level_manifest.read()?;
		let live_tables = manifest.get_all_tables();
		let live_table_ids: HashSet<u64> = live_tables.keys().copied().collect();
		drop(manifest);

		// Scan SST directory for orphaned files
		let entries = std::fs::read_dir(&sstable_dir)?;
		let mut removed_count = 0;

		for entry in entries {
			let entry = entry?;
			let filename = entry.file_name();
			let filename_str = filename.to_string_lossy();

			// Parse table ID from filename (format: {id:020}.sst)
			if filename_str.ends_with(".sst") && filename_str.len() == 24 {
				if let Ok(table_id) = filename_str[..20].parse::<u64>() {
					// Delete if not in manifest
					if !live_table_ids.contains(&table_id) {
						let path = entry.path();
						match std::fs::remove_file(&path) {
							Ok(_) => {
								removed_count += 1;
								log::info!("Removed orphaned SST file: table_id={}", table_id);
							}
							Err(e) => {
								log::warn!(
									"Failed to remove orphaned SST table_id={}: {}",
									table_id,
									e
								);
							}
						}
					}
				}
			}
		}

		if removed_count > 0 {
			log::info!("Cleaned up {} orphaned SST files", removed_count);
		} else {
			log::debug!("No orphaned SST files found");
		}

		Ok(())
	}
}

impl CompactionOperations for CoreInner {
	/// Flushes the oldest immutable memtable to SST.
	/// Called by background task. Returns Ok(()) even if nothing to flush.
	fn compact_memtable(&self) -> Result<()> {
		self.flush_oldest_immutable_to_sst().map(|_| ())
	}

	/// Performs compaction to merge SSTables and maintain read performance.
	///
	/// Compaction is crucial for LSM tree performance:
	/// - Merges overlapping SSTables to reduce read amplification
	/// - Removes deleted entries to reclaim space
	/// - Maintains the level invariants (size ratios and key ranges)
	fn compact(&self, strategy: Arc<dyn CompactionStrategy>) -> Result<()> {
		// One scheduler, every owner: each owner's level set gets the same
		// strategy. Without this, a foreign branch's L0 never compacts and
		// its owner-scoped stall would eventually park that branch's writes
		// permanently.
		let owners = self.level_manifest.read()?.owners();
		for owner in owners {
			let options = CompactionOptions::for_owner(self, owner)?;
			let compactor = Compactor::new(options, Arc::clone(&strategy));
			match compactor.compact() {
				Ok(()) => {}
				// A fork pinned lower history mid-merge. Nothing was published
				// and the inputs are unhidden, so this is a retry condition,
				// not damage: the next cycle re-picks them under the new floor.
				// Every other error still fails the cycle.
				Err(Error::CompactionPinRaced {
					sampled_floor,
					current_floor,
				}) => {
					log::debug!(
						"compaction of {owner:?} deferred: inherited-view floor moved {sampled_floor} -> {current_floor}"
					);
				}
				Err(error) => return Err(error),
			}
		}

		Ok(())
	}

	/// Returns a reference to the background error handler
	fn error_handler(&self) -> Arc<BackgroundErrorHandler> {
		Arc::clone(&self.error_handler)
	}

	fn has_pending_immutables(&self) -> bool {
		// Any runtime's backlog keeps the flush loop running, not only the
		// default's — the trickle policy feeds branch queues too.
		self.runtimes.all().iter().any(|runtime| {
			runtime.immutable_memtables.read().map(|guard| !guard.is_empty()).unwrap_or(false)
		})
	}

	fn rotate_wal_pinned_runtime(&self) -> Result<bool> {
		self.rotate_wal_pinned_runtime_impl()
	}

	fn sweep_branch_maintenance(&self) -> Result<(usize, usize)> {
		CoreInner::sweep_branch_maintenance(self)
	}
}

impl WriteStallCountProvider for CoreInner {
	fn get_stall_counts(&self, owner: crate::batch::BatchOwner) -> StallCounts {
		// Branch-scoped: only the owner's own flush backlog and L0 pressure
		// stall its writes. A missing runtime (never-written branch) has no
		// backlog by definition.
		let Some(runtime) = self.runtimes.get(owner) else {
			return StallCounts {
				immutable_memtables: 0,
				l0_files: 0,
			};
		};
		let immutable_memtables =
			runtime.immutable_memtables.read().map(|imm| imm.iter().count()).unwrap_or(0);
		let l0_files = self
			.level_manifest
			.read()
			.map(|m| {
				m.levels_for(owner)
					.and_then(|levels| levels.get_levels().first().map(|l| l.tables.len()))
					.unwrap_or(0)
			})
			.unwrap_or(0);
		StallCounts {
			immutable_memtables,
			l0_files,
		}
	}
}

struct LsmCommitEnv {
	core: Arc<CoreInner>,

	/// Manages background tasks like flushing and compaction
	task_manager: Option<Arc<TaskManager>>,
}

impl LsmCommitEnv {
	/// Creates a new commit environment for the LSM tree
	pub(crate) fn new(core: Arc<CoreInner>, task_manager: Arc<TaskManager>) -> Result<Self> {
		Ok(Self {
			core,
			task_manager: Some(task_manager),
		})
	}
}

impl CommitEnv for LsmCommitEnv {
	// Build the WAL-ready encoding OUTSIDE the commit write_mutex. Values stay
	// inline. The seq is a placeholder (0) here — the per-entry
	// bytes do not depend on it; only the fixed-width header does. The commit
	// pipeline stamps the real seq in `write_prepared` via patch_encoded_seq.
	fn pre_serialize(&self, batch: Batch) -> Result<PreparedWrite> {
		let bytes = batch.encode()?;
		Ok(PreparedWrite {
			processed_batch: batch,
			bytes,
			wal_segment: None,
		})
	}

	// Stamp the allocated seq into the pre-encoded bytes (in place) and into the
	// processed batch (consumed by `apply`), then append to the WAL. This is all
	// that remains under write_mutex — no clone, no re-encode.
	fn write_prepared(&self, prepared: &mut PreparedWrite, seq_num: u64, sync: bool) -> Result<()> {
		// Stamp the commit timestamp with the sequence, under the same write
		// mutex: the strictly monotone clamp orders the timeline exactly like
		// the sequence axis (FK2).
		let highest_seq = seq_num + prepared.processed_batch.count() as u64 - 1;
		let commit_ts = self.core.timeline.stamp(self.core.opts.clock.now(), highest_seq);
		Batch::patch_encoded_header(&mut prepared.bytes, seq_num, commit_ts);
		prepared.processed_batch.set_starting_seq_num(seq_num);
		prepared.processed_batch.set_commit_ts(commit_ts);

		let mut wal_guard = self.core.wal.write();
		let expected_segment = wal_guard.get_active_log_number();
		self.core.wal_dependencies.pin_in_flight(seq_num, expected_segment);
		let actual_segment = match wal_guard.append(&prepared.bytes) {
			Ok(segment) => segment,
			Err(error) => {
				self.core.wal_dependencies.cancel_in_flight(seq_num);
				return Err(error.into());
			}
		};
		debug_assert_eq!(actual_segment, expected_segment);
		prepared.wal_segment = Some(actual_segment);
		if sync {
			wal_guard.sync()?;
		}
		// Size-driven WAL rotation is the only rotation policy now that
		// branch memtable rotations are independent of the shared log. The
		// appended batch stays in the old segment; the next append pins the
		// new one.
		let rotated_for_size = if wal_guard.should_rotate_for_size() {
			wal_guard.rotate()?;
			true
		} else {
			false
		};
		drop(wal_guard);

		// A new segment is when cold branches start falling behind: schedule
		// the flush task so the WAL-span trickle policy runs.
		if rotated_for_size {
			if let Some(ref task_manager) = self.task_manager {
				task_manager.wake_up_memtable();
			}
		}

		Ok(())
	}

	/// Apply batch to memtable with retry on arena full.
	///
	/// Atomicity invariant (since the introduction of `MemTable::try_reserve`):
	/// `active_memtable.add(batch)` either fully applies the batch or returns
	/// `Err(ArenaFull)` with the memtable unchanged. The rotate-and-retry below
	/// is therefore safe: no partial-application prefix can leak into the
	/// immutable queue, and the retry on a fresh memtable will not produce
	/// duplicates of `(user_key, seq_num)` across two SSTs. This matters
	/// specifically because `CommitPipeline::commit` (see commit.rs) runs
	/// `apply()` outside its `write_mutex`, so concurrent calls to this
	/// function on the same active memtable are routine.
	fn apply(&self, prepared: &PreparedWrite) -> Result<()> {
		let batch = &prepared.processed_batch;
		let wal_segment = prepared.wal_segment.ok_or_else(|| {
			Error::Other("WAL-backed apply is missing its actual segment provenance".to_owned())
		})?;

		// Route by the already-validated physical owner. The runtime (and its
		// first arena) is created lazily here on the owner's first write; an
		// idle branch allocates nothing.
		let runtime = if batch.owner == self.core.default_runtime.owner() {
			Arc::clone(&self.core.default_runtime)
		} else {
			if self.core.enforce_write_buffer_budget(self.core.opts.branch_memtable_size as u64) {
				if let Some(ref task_manager) = self.task_manager {
					task_manager.wake_up_memtable();
				}
			}
			self.core.runtimes.get_or_create(
				batch.owner,
				self.core.opts.branch_memtable_size,
				&self.core.default_runtime.level_manifest,
			)?
		};

		// Try to add to the owner's current memtable
		let (result, component_id) = {
			let active_memtable = runtime.active_memtable.read()?;
			let result = active_memtable.add(batch);
			if result.is_ok() {
				active_memtable.record_wal_dependency(wal_segment);
			}
			(result, active_memtable.dependency_id())
		};

		match result {
			Ok(()) => {
				self.core.wal_dependencies.handoff_to_component(
					batch.starting_seq_num,
					component_id,
					wal_segment,
				);
				Ok(())
			}
			Err(Error::ArenaFull) => {
				// Arena is full - rotate this runtime's memtable and retry.
				// The replacement arena is right-sized to the batch so a
				// WAL-durable batch can never permanently fail to apply.
				log::debug!("apply: arena full, rotating memtable for owner {:?}", batch.owner);

				let min_capacity = batch.memtable_size_estimate() as usize;
				// The unconditional wake below schedules the flush for both
				// this budget rotation and the forced one.
				let _budget_rotated = self.core.enforce_write_buffer_budget(min_capacity as u64);
				self.core.rotate_runtime_memtable(&runtime, min_capacity)?;

				// Schedule background flush
				if let Some(ref task_manager) = self.task_manager {
					task_manager.wake_up_memtable();
				}

				// Retry on new memtable - must succeed
				let active_memtable = runtime.active_memtable.read()?;
				let result = active_memtable.add(batch);
				if result.is_ok() {
					active_memtable.record_wal_dependency(wal_segment);
					self.core.wal_dependencies.handoff_to_component(
						batch.starting_seq_num,
						active_memtable.dependency_id(),
						wal_segment,
					);
				}
				result
			}
			Err(e) => Err(e),
		}
	}

	// Check for background errors before committing
	fn check_background_error(&self) -> Result<()> {
		self.core.error_handler.check_error()
	}

	fn on_durable_apply_failure(&self, error: &Error) {
		self.core.error_handler.set_error(error.clone(), BackgroundErrorReason::DurableCommitApply);
	}

	fn oldest_active_start_seq(&self) -> u64 {
		self.core.oldest_active_start_seq()
	}
}

// ===== Core with Background Task Management =====
/// Wraps the LSM tree core with background task management.
///
/// LSM trees rely heavily on background operations:
/// - Memtable flushing: Converting full memtables to SSTables
/// - Compaction: Merging SSTables to maintain read performance
/// - Garbage collection: Removing obsolete SSTables
pub(crate) struct Core {
	/// The inner LSM tree implementation
	pub(crate) inner: Arc<CoreInner>,

	/// The commit pipeline that handles write batches
	pub(crate) commit_pipeline: Arc<CommitPipeline>,

	/// Task manager for background operations (stored in Option so we can take
	/// it for shutdown)
	pub(crate) task_manager: Mutex<Option<Arc<TaskManager>>>,

	/// Write stall controller for backpressure management
	pub(crate) write_stall: Arc<crate::stall::WriteStallController>,
}

impl std::ops::Deref for Core {
	type Target = CoreInner;

	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

/// How long the fork fence waits for in-flight commits to leave the pipeline
/// before giving up. The wait is normally microseconds — the batches are
/// already WAL-durable and only need their memtable apply and publish. A commit
/// whose future was dropped between enqueue and apply would otherwise hold the
/// fence, and with it every writer, forever.
const FORK_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

impl Core {
	/// Forks `child_name` off `parent_name` at `at`.
	///
	/// The protocol is one durable step. Under the branch-op mutex: fence
	/// writes, drain the commit pipeline so the visible head is exact, resolve
	/// the fork sequence, release the fence, then publish ONE catalog version
	/// naming the child, its parent link and its anchor. That publish is the
	/// commit point — before it nothing durable mentions the child, after it the
	/// child is fully readable with no further work, because its view is
	/// computed from the parent's live state (design §3.2).
	///
	/// No data is copied and no table is written, including when the parent has
	/// committed rows that are not yet flushed: the child's read stack resolves
	/// the parent's memtables under the same cap, and a crash replays them into
	/// the parent from the WAL.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn fork_branch(
		&self,
		parent_name: &str,
		child_name: &str,
		at: crate::branch::ForkPoint,
	) -> Result<crate::branch::ForkReceipt> {
		use crate::branch::{ForkPoint, ForkReceipt};

		// Serialises every branch operation, so the parent resolved below cannot
		// change under us while the fence is taken and released.
		let mut publish = self.inner.catalog_publish.lock().unwrap();

		let (parent_owner, existing_child) = {
			let catalog = self.inner.branch_catalog.read()?;
			let parent = catalog.get_by_name(parent_name).map_err(|_| {
				Error::InvalidArgument(format!("fork parent {parent_name:?} is not a live branch"))
			})?;
			let parent_owner = crate::batch::BatchOwner {
				branch: parent.id,
				generation: parent.generation,
			};
			// Refuse up front rather than publishing a branch whose every read
			// would then fail: the child sits one link deeper than its parent, so
			// the parent's own chain must leave room for it.
			let parent_depth = catalog
				.parent_chain(
					parent_owner.branch,
					parent_owner.generation,
					crate::branch::MAX_VIEW_DEPTH,
				)?
				.len();
			if parent_depth + 1 > crate::branch::MAX_VIEW_DEPTH {
				return Err(Error::MaterializationRequired {
					depth: parent_depth + 1,
				});
			}
			let existing = catalog.get_by_name(child_name).ok().map(|child| {
				(
					crate::batch::BatchOwner {
						branch: child.id,
						generation: child.generation,
					},
					child.parent.clone(),
				)
			});
			(parent_owner, existing)
		};

		// Idempotent retry: the same fork, re-issued. A name that exists with a
		// different lineage is a genuine conflict and fails closed rather than
		// being silently reinterpreted.
		if let Some((child_owner, link)) = existing_child {
			let Some(link) = link else {
				return Err(Error::InvalidArgument(format!(
					"branch {child_name:?} already exists and is not a fork"
				)));
			};
			if link.parent != parent_owner.branch
				|| link.parent_generation != parent_owner.generation
			{
				return Err(Error::InvalidArgument(format!(
					"branch {child_name:?} already exists as a fork of a different parent"
				)));
			}
			if let ForkPoint::AtVersion(version) = at {
				if version != link.fork_seq {
					return Err(Error::InvalidArgument(format!(
						"branch {child_name:?} already exists at fork sequence {}, not {version}",
						link.fork_seq
					)));
				}
			}
			return Ok(ForkReceipt {
				child: child_owner,
				parent: parent_owner,
				fork_seq: link.fork_seq,
			});
		}

		// The fence: no new sequence can be allocated while it is held, and once
		// the pipeline has drained, `visible_seq_num` is exactly the highest
		// readable sequence.
		let head = {
			let _fence = self.commit_pipeline.lock_writes();
			let deadline = std::time::Instant::now() + FORK_DRAIN_TIMEOUT;
			while !self.commit_pipeline.is_drained() {
				if std::time::Instant::now() >= deadline {
					return Err(Error::ForkFenceTimeout);
				}
				std::hint::spin_loop();
			}
			self.inner.visible_seq_num.load(Ordering::Acquire)
		};

		let fork_seq = match at {
			ForkPoint::Head => head,
			ForkPoint::AtVersion(version) => {
				if version > head {
					return Err(Error::InvalidArgument(format!(
						"fork sequence {version} is above the parent's visible head {head}"
					)));
				}
				version
			}
			ForkPoint::AtTimestamp(timestamp) => {
				let resolved = self.inner.timeline.resolve(timestamp)?;
				resolved.min(head)
			}
		};

		// Commit point. The manifest read lock is taken FIRST (the order FK3
		// established) and held through the publish: it is the lock compaction
		// publication takes exclusively, so the retention floor read here cannot
		// be raised by a compaction that publishes concurrently. Whichever of
		// the two publishes second sees the other and fails closed — compaction
		// through `CompactionPinRaced`, the fork through the floor check.
		let levels = self.inner.level_manifest.read()?;
		let floor = levels.retained_floor(parent_owner);
		if fork_seq < floor {
			return Err(Error::BelowRetentionFloor {
				requested: fork_seq,
				floor,
			});
		}

		let mut catalog = self.inner.branch_catalog.write()?;
		let snapshot = catalog.clone();
		let id = self.inner.mint_branch_id(&catalog);
		let record = catalog
			.create_fork(
				id,
				child_name,
				fork_seq,
				crate::authority::format::ParentLink {
					parent: parent_owner.branch,
					parent_generation: parent_owner.generation,
					fork_seq,
				},
			)
			.map_err(|error| Error::InvalidArgument(format!("fork rejected: {}", error.message)))?;
		let child = crate::batch::BatchOwner {
			branch: id,
			generation: record.generation,
		};
		if let Err(error) = self.inner.publish_catalog_locked(&mut publish, &catalog) {
			*catalog = snapshot;
			return Err(error);
		}

		Ok(ForkReceipt {
			child,
			parent: parent_owner,
			fork_seq,
		})
	}

	/// Replays WAL with configurable corruption handling.
	///
	/// Creates one memtable per WAL segment. Flushes all but the last memtable
	/// to SST via the provided callback. Returns the last memtable as active.
	///
	/// # Arguments
	/// * `wal_path` - Path to WAL directory
	/// * `min_wal_number` - Minimum WAL to replay
	/// * `context` - Context string for error messages
	/// * `recovery_mode` - How to handle corruption
	/// * `arena_size` - Size for memtable arenas
	/// * `flush_memtable` - Callback to flush intermediate memtables to SST
	pub(crate) fn replay_wal_with_repair(
		wal_path: &Path,
		min_wal_number: u64,
		context: &str,
		recovery_mode: WalRecoveryMode,
		default_arena_size: usize,
		branch_arena_size: usize,
		is_live_owner: &dyn Fn(crate::batch::BatchOwner) -> bool,
	) -> Result<crate::wal::recovery::ReplayOutcome> {
		// Replay WAL - returns branch-pure memtables per (segment, owner)
		let outcome = match replay_wal(
			wal_path,
			min_wal_number,
			default_arena_size,
			branch_arena_size,
			is_live_owner,
		) {
			Ok(result) => result,
			Err(Error::WalCorruption {
				segment_id,
				offset,
				message,
			}) => {
				// Handle corruption based on recovery mode
				match recovery_mode {
					WalRecoveryMode::AbsoluteConsistency => {
						log::error!(
							"WAL corruption detected in segment {} at offset {}: {}. \
							AbsoluteConsistency mode: failing immediately without repair.",
							segment_id,
							offset,
							message
						);
						return Err(Error::WalCorruption {
							segment_id,
							offset,
							message,
						});
					}
					WalRecoveryMode::TolerateCorruptedWithRepair => {
						log::warn!(
							"Detected WAL corruption in segment {} at offset {}: {}. Attempting repair...",
							segment_id,
							offset,
							message
						);

						// Attempt repair
						if let Err(repair_err) = repair_corrupted_wal_segment(wal_path, segment_id)
						{
							log::error!("Failed to repair WAL segment: {repair_err}");
							return Err(Error::Other(format!(
								"{context} failed: WAL segment {segment_id} is corrupted and could not be repaired. {repair_err}"
							)));
						}

						// Retry after repair
						match replay_wal(
							wal_path,
							min_wal_number,
							default_arena_size,
							branch_arena_size,
							is_live_owner,
						) {
							Ok(result) => result,
							Err(Error::WalCorruption {
								segment_id: seg_id,
								offset: off,
								message,
							}) => {
								return Err(Error::Other(format!(
									"{context} failed: WAL segment {seg_id} still corrupted at offset {off} after repair: {message}"
								)));
							}
							Err(retry_err) => {
								return Err(Error::Other(format!(
									"{context} failed: WAL replay failed after repair. {retry_err}"
								)));
							}
						}
					}
				}
			}
			Err(e) => return Err(e),
		};

		Ok(outcome)
	}

	/// Creates a new LSM tree with background task management
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		log::info!("=== Starting LSM tree initialization ===");
		log::info!("Database path: {:?}", opts.path);

		let inner = Arc::new(CoreInner::new_impl(Arc::clone(&opts))?);

		// Create the write stall controller with the provider and thresholds
		let thresholds = StallThresholds {
			memtable_limit: opts.memtable_stall_threshold,
			l0_file_limit: opts.l0_stall_threshold,
		};
		let write_stall = Arc::new(crate::stall::WriteStallController::new(
			Arc::clone(&inner) as Arc<dyn WriteStallCountProvider>,
			thresholds,
		));

		// Initialize background task manager
		let task_manager = Arc::new(TaskManager::new(
			Arc::clone(&inner) as Arc<dyn CompactionOperations>,
			Arc::clone(&opts),
			Arc::clone(&write_stall),
		));

		let commit_env =
			Arc::new(LsmCommitEnv::new(Arc::clone(&inner), Arc::clone(&task_manager))?);

		// Pass the shared visible_seq_num from CoreInner to CommitPipeline
		// Both will use the same atomic for coordinated updates
		let commit_pipeline = CommitPipeline::new(
			commit_env,
			Arc::clone(&inner.visible_seq_num),
			Arc::clone(&write_stall),
		);

		// Path for the WAL directory
		let wal_path = opts.wal_dir();

		// Get min_wal_number from manifest to skip already-flushed WALs
		let min_wal_number = inner.level_manifest.read()?.get_log_number();
		let manifest_last_seq = inner.level_manifest.read()?.get_last_sequence();

		log::info!(
			"Manifest state: log_number={}, last_sequence={}",
			min_wal_number,
			manifest_last_seq
		);

		// Replay WAL with configurable recovery mode. The catalog-at-open is
		// the fencing authority: a batch whose owner it cannot validate
		// (deleted, stale generation, or unknown) is dropped during replay
		// and never installed into any runtime.
		let catalog_fence = |owner: crate::batch::BatchOwner| {
			inner
				.branch_catalog
				.read()
				.map(|catalog| catalog.validate_owner(owner.branch, owner.generation).is_ok())
				.unwrap_or(false)
		};
		let replay_outcome = Self::replay_wal_with_repair(
			&wal_path,
			min_wal_number,
			"Database startup",
			opts.wal_recovery_mode,
			opts.max_memtable_size,
			opts.branch_memtable_size,
			&catalog_fence,
		)?;
		let wal_seq_num_opt = replay_outcome.max_seq_num;

		// Seed the timeline with replayed fenceposts past the root tail's
		// coverage (the tail records commits up to the last root publish;
		// replay re-reads segments at or beyond the floor, so they overlap).
		let tail_end = inner.timeline.last_commit_ts();
		let fresh: Vec<(u64, u64)> = replay_outcome
			.fenceposts
			.iter()
			.copied()
			.filter(|&(commit_ts, _)| commit_ts > tail_end)
			.collect();
		inner.timeline.seed(&fresh);

		inner.install_recovered_memtables(replay_outcome.memtables, "Recovery")?;

		// Get last_sequence from manifest
		let manifest_last_seq = inner.level_manifest.read()?.get_last_sequence();

		// Determine effective sequence number:
		// - If WAL was replayed, use max(manifest, WAL)
		// - If WAL was skipped/empty, use manifest value
		let max_seq_num = match wal_seq_num_opt {
			Some(wal_seq) => {
				let effective = std::cmp::max(manifest_last_seq, wal_seq);
				log::debug!(
					"WAL replayed: manifest_last_seq={}, wal_seq={}, using max={}",
					manifest_last_seq,
					wal_seq,
					effective
				);
				effective
			}
			None => {
				log::debug!("WAL skipped or empty, using manifest_last_seq={}", manifest_last_seq);
				manifest_last_seq
			}
		};
		// The clock never seeds below a catalog anchor or the root's visible
		// floor: a deleted branch's sequences stay allocated, and generation
		// fences would otherwise eat legitimate new commits (invariant 6).
		let max_seq_num = max_seq_num.max(inner.clock_floor);

		// Set visible sequence number (in-memory, will be persisted on next flush)
		commit_pipeline.set_seq_num(max_seq_num);

		// Clean up any orphaned SST files from previous crashes
		// SAFETY: This must happen AFTER WAL replay so data is recovered
		// but BEFORE any new flushes that might create new SSTs
		inner.cleanup_orphaned_sst_files()?;

		// Trigger level compaction check at startup
		task_manager.wake_up_level();

		let core = Self {
			inner: Arc::clone(&inner),
			commit_pipeline: Arc::clone(&commit_pipeline),
			task_manager: Mutex::new(Some(task_manager)),
			write_stall,
		};

		log::info!("=== LSM tree initialization complete ===");

		Ok(core)
	}

	pub(crate) async fn commit(&self, batch: Batch, sync: bool, start_seq: u64) -> Result<()> {
		// Commit the batch using the commit pipeline. `start_seq` is the
		// transaction's snapshot seq (used by the oracle's write-write
		// conflict check). The write keys are derived from `batch.entries`
		// inside the pipeline — no duplicated parallel array.
		self.commit_pipeline.commit(batch, sync, start_seq).await
	}

	pub(crate) fn seq_num(&self) -> u64 {
		self.commit_pipeline.get_visible_seq_num()
	}

	/// Flushes WAL buffers to OS cache.
	///
	/// If `sync` is true, also fsyncs to disk for durability.
	/// This is safe to call concurrently with ongoing transactions.
	pub(crate) fn flush_wal(&self, sync: bool) -> Result<()> {
		if sync {
			self.wal.sync()?;
		} else {
			self.wal.flush()?;
		}
		Ok(())
	}

	/// Safely closes the LSM tree by shutting down all components in the
	/// correct order.
	///
	/// # Shutdown Sequence
	///
	/// 1. Commit pipeline shutdown - stops accepting new writes
	/// 2. Background tasks stopped - waits for ongoing operations
	/// 3. Active memtable flush - if flush_on_close enabled AND memtable non-empty, flush to SST
	///    (NO WAL rotation)
	/// 4. WAL close - sync and close current WAL file
	/// 5. Directory sync - ensure all metadata is persisted
	/// 6. Lock release - allow other processes to open the database
	///
	/// # Critical: No Empty WAL Creation
	///
	/// Unlike `make_room_for_write`, this does NOT rotate the WAL before
	/// flushing. This prevents creating an empty WAL file on clean shutdown.
	pub async fn close(&self) -> Result<()> {
		log::info!("Shutting down LSM tree...");

		// Step 1: Shutdown the commit pipeline to stop accepting new writes
		self.commit_pipeline.shutdown();
		log::debug!("Commit pipeline shutdown complete");

		// Step 2: Signal write stall controller - wake any stalled writers
		self.write_stall.signal_shutdown();
		log::debug!("Write stall shutdown signal sent");

		// Step 3: Wait for and stop all background tasks
		let task_manager = self.task_manager.lock().unwrap().take();
		if let Some(task_manager) = task_manager {
			log::debug!("Stopping background task manager...");
			task_manager.stop().await;
			log::debug!("Background task manager stopped");
		}

		// Step 3: Conditionally flush ALL memtables based on flush_on_close option
		// CRITICAL ORDERING: Immutable memtables must be flushed BEFORE active memtable
		// to preserve SSTable ordering (older data = lower table_ids)
		// IMPORTANT: We do NOT rotate the WAL here to avoid creating an empty WAL file
		if self.inner.opts.flush_on_close {
			log::info!("Flushing all memtables on shutdown (flush_on_close=true)");

			// Flush ALL memtables: immutables first (older data), then active (newest data)
			self.inner.flush_all_memtables_for_shutdown().map_err(|e| {
				Error::Other(format!("Failed to flush memtables during shutdown: {}", e))
			})?;

			log::info!("All memtables flushed successfully on shutdown");
		}

		// Step 4: Close the WAL to ensure all data is flushed
		// This is safe now because all background tasks that could write to WAL are
		// stopped NOTE: WAL must be closed BEFORE cleanup, otherwise cleanup may
		// delete the active WAL file
		let wal_log_number = self.inner.wal.read().get_active_log_number();
		log::info!("Closing WAL: active_log_number={}", wal_log_number);

		let mut wal_guard = self.inner.wal.write();
		wal_guard.close().map_err(|e| Error::Other(format!("Failed to close WAL: {}", e)))?;
		log::debug!("WAL #{:020} closed and synced", wal_log_number);
		drop(wal_guard);

		// Step 4.5: Clean up obsolete WAL files (synchronous cleanup)
		// This happens AFTER closing the WAL to prevent deleting the active WAL file.
		// When memtable flush sets log_number = current_wal + 1, cleanup would delete
		// the active WAL if done before closing it.
		let wal_dir = self.inner.wal.read().get_dir_path().to_path_buf();
		let min_wal_to_keep = self.inner.level_manifest.read()?.get_log_number();

		log::debug!("Cleaning up obsolete WAL files (min_wal_to_keep={})", min_wal_to_keep);

		match cleanup_old_segments(&wal_dir, min_wal_to_keep) {
			Ok(count) if count > 0 => {
				log::info!("Cleaned up {} obsolete WAL files during shutdown", count);
			}
			Ok(_) => {
				log::debug!("No obsolete WAL files to clean up");
			}
			Err(e) => {
				log::warn!("Failed to clean up WAL files during shutdown: {}", e);
			}
		}

		// Step 5: Flush all directories to ensure durability
		log::debug!("Syncing directory structure...");
		sync_directory_structure(&self.inner.opts).map_err(|e| {
			Error::Other(format!("Failed to sync directories during shutdown: {}", e))
		})?;
		log::debug!("Directory sync complete");

		// Step 7: Release the database lock
		let mut lockfile = self.inner.lockfile.lock()?;
		lockfile.release()?;

		// Log final state
		let final_manifest = self.inner.level_manifest.read()?;
		log::info!(
			"=== LSM tree shutdown complete === log_number={}, last_sequence={}",
			final_manifest.get_log_number(),
			final_manifest.get_last_sequence()
		);

		Ok(())
	}
}

#[derive(Clone)]
pub struct Tree {
	pub(crate) core: Arc<Core>,
}

impl Tree {
	/// Creates a new LSM tree with the specified options
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		// Validate options before creating the tree
		opts.validate()?;

		// Create all required directory structure
		Self::create_directory_structure(&opts)?;

		// Create the core LSM tree components
		let core = Core::new(Arc::clone(&opts))?;

		// TODO: Add file to write options manifest
		// TODO: Add version header in file similar to table in WAL

		// Ensure directory changes are persisted
		sync_directory_structure(&opts)?;

		Ok(Self {
			core: Arc::new(core),
		})
	}

	/// Creates all required directory structure for the LSM tree
	fn create_directory_structure(opts: &Options) -> Result<()> {
		// Create base directory
		create_dir_all(&opts.path)?;

		// Create all subdirectories
		create_dir_all(opts.sstable_dir())?;
		create_dir_all(opts.wal_dir())?;

		Ok(())
	}

	/// Transactions provide a consistent, atomic view of the database.
	pub fn begin(&self) -> Result<Transaction> {
		self.begin_with_opts(TransactionOptions::new())
	}

	/// Begins a new transaction with the specified mode
	pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
		self.begin_with_opts(TransactionOptions::new_with_mode(mode))
	}

	/// Begins a new transaction with the provided options
	pub fn begin_with_opts(&self, opts: TransactionOptions) -> Result<Transaction> {
		let txn = Transaction::new(Arc::clone(&self.core), opts)?;
		Ok(txn)
	}

	/// Executes a read-only operation in a consistent snapshot.
	///
	/// This provides a consistent view of the database without blocking writes.
	pub fn view(&self, f: impl FnOnce(&mut Transaction) -> Result<()>) -> Result<()> {
		let mut txn = self.begin_with_mode(Mode::ReadOnly)?;
		f(&mut txn)?;
		Ok(())
	}

	// ===== Branches =====
	//
	// `begin`, `view` and every method above operate on `main`, which always
	// exists and behaves exactly as it did before branching. Everything below is
	// additive.

	/// Opens an existing branch.
	///
	/// The handle is pinned to the branch's current generation, so if the branch
	/// is deleted (and possibly recreated under the same name) every operation
	/// through this handle fails with [`Error::BranchFenced`] rather than
	/// silently binding to the new incarnation.
	pub fn branch(&self, name: &str) -> Result<BranchHandle> {
		let catalog = self.core.inner.branch_catalog.read()?;
		let record = catalog
			.get_by_name(name)
			.map_err(|_| Error::InvalidArgument(format!("branch {name:?} does not exist")))?;
		Ok(BranchHandle {
			core: Arc::clone(&self.core),
			owner: crate::batch::BatchOwner {
				branch: record.id,
				generation: record.generation,
			},
			name: record.name.clone(),
		})
	}

	/// Creates an empty branch that inherits nothing.
	///
	/// This is NOT a fork: the new branch starts with no data and no parent. Use
	/// [`Tree::fork_branch`] for copy-on-write branching.
	pub fn create_branch(&self, name: &str) -> Result<BranchHandle> {
		let owner = self.core.inner.create_branch(name)?;
		Ok(BranchHandle {
			core: Arc::clone(&self.core),
			owner,
			name: name.to_owned(),
		})
	}

	/// Forks `source` into a new branch `name`, cutting its view at `at`.
	///
	/// No data is copied and no table is written, including when the source has
	/// committed rows that are not yet flushed: the child resolves the parent's
	/// live state under a sequence cap. The catalog publish is the whole
	/// operation, so a crash immediately afterwards needs no recovery work.
	///
	/// Briefly blocking: the fork fences writes while it drains the commit
	/// pipeline, so the anchor is the parent's exact visible head. That window
	/// is normally microseconds; if in-flight commits cannot drain it fails with
	/// [`Error::ForkFenceTimeout`] rather than holding writers indefinitely.
	///
	/// Re-issuing the same fork returns the original branch instead of creating
	/// a second one; reusing the name with a different parent or a different
	/// fork point is refused.
	pub fn fork_branch(&self, source: &str, name: &str, at: ForkPoint) -> Result<BranchHandle> {
		let receipt = self.core.fork_branch(source, name, at)?;
		Ok(BranchHandle {
			core: Arc::clone(&self.core),
			owner: receipt.child,
			name: name.to_owned(),
		})
	}

	/// Tombstones a branch. Its data is reclaimed by maintenance, not here.
	///
	/// Refuses `main`, and refuses any branch that still has active fork
	/// children — their views resolve through it.
	pub fn delete_branch(&self, name: &str) -> Result<()> {
		let id = {
			let catalog = self.core.inner.branch_catalog.read()?;
			catalog
				.get_by_name(name)
				.map_err(|_| Error::InvalidArgument(format!("branch {name:?} does not exist")))?
				.id
		};
		self.core.inner.delete_branch(id)
	}

	/// Every live branch, in catalog order.
	pub fn list_branches(&self) -> Result<Vec<BranchInfo>> {
		let records: Vec<_> = {
			let catalog = self.core.inner.branch_catalog.read()?;
			catalog.list().cloned().collect()
		};
		records.into_iter().map(|record| self.core.branch_info(&record)).collect()
	}

	/// Sets or clears a branch's time-to-live, relative to now.
	///
	/// An expired branch is tombstoned by the maintenance sweep, not on a timer,
	/// so expiry is observed at the next maintenance pass rather than exactly at
	/// the deadline. `None` makes the branch permanent again. Refuses `main`.
	pub fn set_branch_ttl(&self, name: &str, ttl: Option<std::time::Duration>) -> Result<()> {
		let id = {
			let catalog = self.core.inner.branch_catalog.read()?;
			catalog
				.get_by_name(name)
				.map_err(|_| Error::InvalidArgument(format!("branch {name:?} does not exist")))?
				.id
		};
		// Absolute expiry on the store's own clock, so the maintenance sweep
		// never has to reason about when the TTL was set.
		let expires_at = ttl.map(|ttl| {
			let nanos = u64::try_from(ttl.as_nanos()).unwrap_or(u64::MAX);
			self.core.inner.opts.clock.now().saturating_add(nanos)
		});
		self.core.inner.set_branch_expiry(id, expires_at)
	}

	/// Begins a transaction on `name`. Sugar for `branch(name)?.begin()`.
	pub fn begin_on(&self, name: &str) -> Result<Transaction> {
		self.branch(name)?.begin()
	}

	/// Materializes a forked branch's inherited data into its own tables and
	/// drops its parent link, returning the number of rows copied.
	///
	/// Reads are unchanged by this — the branch sees exactly what it saw before.
	/// What changes is cost: the branch stops walking its ancestor chain on
	/// every read, and it releases the retention pin its fork anchor placed on
	/// the parent, so the parent's compaction can reclaim the versions it was
	/// holding on the branch's behalf.
	///
	/// This copies data and is proportional to what the branch inherits. A
	/// branch with no parent is already detached, so this returns `Ok(0)` rather
	/// than an error.
	pub fn detach_branch(&self, name: &str) -> Result<u64> {
		let handle = self.branch(name)?;
		self.core.inner.detach_branch(&self.core, handle.owner)
	}

	/// Creates a database checkpoint at the specified directory.
	///
	/// This creates a consistent point-in-time snapshot that includes:
	/// - All SSTables from all levels
	/// - Current WAL segments
	/// - Level manifest
	/// - Checkpoint metadata
	///
	/// # Arguments
	/// * `checkpoint_dir` - Directory where the checkpoint will be created
	///
	/// # Returns
	/// Metadata about the created checkpoint
	pub fn create_checkpoint<P: AsRef<Path>>(
		&self,
		checkpoint_dir: P,
	) -> Result<CheckpointMetadata> {
		let checkpoint = DatabaseCheckpoint::new(Arc::clone(&self.core.inner));
		checkpoint.create_checkpoint(checkpoint_dir)
	}

	/// Restores the database from a checkpoint directory.
	pub fn restore_from_checkpoint<P: AsRef<Path>>(
		&self,
		checkpoint_dir: P,
	) -> Result<CheckpointMetadata> {
		// Block new commits from entering the critical section for the duration
		// of the restore. The restore is a multi-step rewrite of nearly all
		// in-memory state (manifest, memtables, WAL, seq counters, oracle); a
		// concurrent commit racing through any one of those steps would observe
		// torn state. In-flight commits already past `write_mutex` (in their
		// apply phase) will finish against the soon-to-be-replaced memtable —
		// their data is intentionally discarded by the restore.
		let _write_guard = self.core.commit_pipeline.lock_writes();

		// Step 1: Restore files from checkpoint
		let checkpoint = DatabaseCheckpoint::new(Arc::clone(&self.core.inner));
		let metadata = checkpoint.restore_from_checkpoint(checkpoint_dir)?;

		// Step 2: Reload in-memory state to match restored files. Restore is
		// a whole-database swap INCLUDING the catalog (the restored catalog
		// and states are one consistent cut; mixing the live catalog with
		// restored states would cross identities).
		let restored_catalog_manifest =
			crate::authority::store::AuthorityStore::load_latest_catalog(
				&self.core.inner.opts.path,
			)?
			.ok_or_else(|| {
				Error::Corruption("restored checkpoint carries no branch catalog".to_owned())
			})?;
		let restored_catalog = crate::branch::BranchCatalog::from_manifest(
			crate::BranchId::DEFAULT,
			&restored_catalog_manifest,
		)?;
		let restored_authority = crate::authority::store::AuthorityStore::new(
			self.core.inner.opts.path.clone(),
			restored_catalog_manifest.db_id,
		);
		let restored_root = restored_authority.load_latest_root()?;
		// Whole-clock swap: the restored root's tail is the timeline now.
		self.core.inner.timeline.reset(
			restored_root.as_ref().map(|r| r.timeline_tail.as_slice()).unwrap_or(&[]),
			restored_root.as_ref().map(|r| r.last_commit_ts).unwrap_or(0),
		);
		let new_levels = LevelManifest::hydrate(
			Arc::clone(&self.core.inner.opts),
			restored_authority,
			&restored_catalog,
			restored_catalog_manifest.catalog_version,
			restored_root.as_ref(),
			Arc::clone(&self.core.inner.timeline),
		)?;

		// Replace the current levels and catalog with the reloaded ones
		{
			let mut levels_guard = self.core.inner.level_manifest.write()?;
			*levels_guard = new_levels;
		}
		{
			let mut publish = self.core.inner.catalog_publish.lock().unwrap();
			publish.catalog_version = restored_catalog_manifest.catalog_version;
			publish.writer_epoch = restored_catalog_manifest.writer_epoch;
			publish.maintenance_epoch = restored_catalog_manifest.maintenance_epoch;
			publish.session_bumped = false;
			let mut catalog_guard = self.core.inner.branch_catalog.write()?;
			*catalog_guard = restored_catalog;
		}

		// Clear the current memtables since they would be stale after restore
		// This discards any pending writes, which is correct for restore operations
		{
			let mut active_memtable = self.core.inner.active_memtable.write()?;
			*active_memtable = Arc::new(MemTable::new(self.core.inner.opts.max_memtable_size));
		}

		{
			let mut immutable_memtables = self.core.inner.immutable_memtables.write()?;
			*immutable_memtables = ImmutableMemtables::default();
		}
		self.core.inner.wal_dependencies.clear();

		// Reopen the WAL from the restored directory
		let wal_path = self.core.inner.opts.path.join("wal");
		let manifest_log_number = self.core.inner.level_manifest.read()?.get_log_number();

		{
			let mut wal_guard = self.core.inner.wal.write();
			let new_wal = Wal::open_with_min_log_number(
				&wal_path,
				manifest_log_number,
				wal::Options::default(),
			)?;
			*wal_guard = new_wal;
		}

		// Replay any WAL entries that were restored, fenced against the
		// running store's catalog (the authority at this restore point).
		let restore_fence = |owner: crate::batch::BatchOwner| {
			self.core
				.inner
				.branch_catalog
				.read()
				.map(|catalog| catalog.validate_owner(owner.branch, owner.generation).is_ok())
				.unwrap_or(false)
		};
		let replay_outcome = Core::replay_wal_with_repair(
			&wal_path,
			manifest_log_number,
			"Database restore",
			self.core.inner.opts.wal_recovery_mode,
			self.core.inner.opts.max_memtable_size,
			self.core.inner.opts.branch_memtable_size,
			&restore_fence,
		)?;
		let wal_seq_num_opt = replay_outcome.max_seq_num;
		let restored_tail_end = self.core.inner.timeline.last_commit_ts();
		let fresh: Vec<(u64, u64)> = replay_outcome
			.fenceposts
			.iter()
			.copied()
			.filter(|&(commit_ts, _)| commit_ts > restored_tail_end)
			.collect();
		self.core.inner.timeline.seed(&fresh);

		self.core.inner.install_recovered_memtables(replay_outcome.memtables, "Restore")?;

		// Ensure the active memtable has the correct WAL number set
		{
			let active_memtable = self.core.inner.active_memtable.read()?;
			let current_wal_number = self.core.inner.wal.read().get_active_log_number();
			if active_memtable.is_empty() {
				active_memtable.set_wal_number(current_wal_number);
			} else {
				active_memtable.record_wal_dependency(current_wal_number);
			}
		}

		// Get last_sequence from manifest
		let manifest_last_seq = self.core.inner.level_manifest.read()?.get_last_sequence();

		// Determine effective sequence number (same logic as Core::new)
		let restored_clock_floor = self
			.core
			.inner
			.branch_catalog
			.read()?
			.max_version_anchor()
			.max(restored_root.as_ref().map(|r| r.visible_seq).unwrap_or(0));
		let max_seq_num = match wal_seq_num_opt {
			Some(wal_seq) => std::cmp::max(manifest_last_seq, wal_seq).max(restored_clock_floor),
			None => manifest_last_seq,
		};

		// Set visible sequence number AND reset the oracle. The live process
		// may have accumulated oracle entries from pre-restore commits whose
		// seqs are now ghosts of a future that no longer exists; clearing them
		// prevents false write-write conflicts for new post-restore txns.
		self.core.commit_pipeline.set_seq_num(max_seq_num);
		self.core.commit_pipeline.reset_oracle_for_restore(max_seq_num);

		Ok(metadata)
	}

	pub async fn close(&self) -> Result<()> {
		self.core.close().await
	}

	/// Flushes all memtables to disk synchronously.
	/// This is a blocking operation that ensures all data is persisted before returning.
	#[cfg(test)]
	pub(crate) fn flush(&self) -> Result<()> {
		// Step 1: Rotate active memtable if it has data
		{
			let active = self.core.inner.active_memtable.read()?;
			if !active.is_empty() {
				drop(active); // Release read lock before acquiring write lock
				self.core.inner.rotate_memtable()?;
			}
		}

		// Step 2: Flush all immutable memtables synchronously
		self.core.inner.flush_all_immutables_sync()?;

		// Step 3: Signal stall controller that work completed
		self.core.write_stall.signal_work_done();

		Ok(())
	}

	#[cfg(test)]
	pub(crate) fn compact(&self, strategy: Arc<dyn CompactionStrategy>) -> Result<()> {
		self.core.inner.compact(strategy)?;
		self.core.write_stall.signal_work_done();
		Ok(())
	}

	/// Flushes WAL buffers to OS cache.
	///
	/// If `sync` is true, also fsyncs to disk, guaranteeing durability
	/// of all previously committed transactions.
	///
	/// If `sync` is false, only flushes to OS buffer cache (faster but
	/// not durable across power loss).
	///
	/// This is safe to call concurrently with ongoing transactions.
	pub fn flush_wal(&self, sync: bool) -> Result<()> {
		self.core.flush_wal(sync)
	}
}

/// A branch, pinned to the generation it was opened at.
///
/// Every operation validates that generation, so a handle to a branch that has
/// since been deleted fails with [`Error::BranchFenced`] instead of binding to a
/// new branch that reused the name.
#[derive(Clone)]
pub struct BranchHandle {
	core: Arc<Core>,
	owner: crate::batch::BatchOwner,
	name: String,
}

impl std::fmt::Debug for BranchHandle {
	/// Identity only: a handle holds the whole engine, which is neither
	/// printable nor useful in a diagnostic.
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("BranchHandle")
			.field("name", &self.name)
			.field("id", &self.owner.branch)
			.field("generation", &self.owner.generation)
			.finish()
	}
}

impl BranchHandle {
	pub fn name(&self) -> &str {
		&self.name
	}

	pub fn id(&self) -> crate::BranchId {
		self.owner.branch
	}

	pub fn generation(&self) -> crate::BranchGeneration {
		self.owner.generation
	}

	/// Begins a transaction scoped to this branch. Reads resolve this branch's
	/// own data first and then its inherited view; writes land here alone.
	pub fn begin(&self) -> Result<Transaction> {
		Transaction::new_owned(Arc::clone(&self.core), TransactionOptions::new(), self.owner)
	}

	/// Begins a transaction on this branch with the given mode.
	pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
		Transaction::new_owned(
			Arc::clone(&self.core),
			TransactionOptions::new_with_mode(mode),
			self.owner,
		)
	}

	/// This branch's catalog facts as of now.
	pub fn info(&self) -> Result<BranchInfo> {
		let record = {
			let catalog = self.core.inner.branch_catalog.read()?;
			catalog
				.validate_owner(self.owner.branch, self.owner.generation)
				.map_err(|_| Error::BranchFenced)?
				.clone()
		};
		self.core.branch_info(&record)
	}
}

impl Core {
	/// Builds the public view of a catalog record, deriving the facts that are
	/// not stored in the catalog.
	fn branch_info(&self, record: &crate::branch::BranchRecord) -> Result<BranchInfo> {
		let owner = crate::batch::BatchOwner {
			branch: record.id,
			generation: record.generation,
		};
		Ok(BranchInfo {
			name: record.name.clone(),
			id: record.id,
			generation: record.generation,
			created_at_seq: record.created_at_seq,
			parent: record.parent.as_ref().map(|link| BranchLineage {
				branch: link.parent,
				generation: link.parent_generation,
				fork_seq: link.fork_seq,
			}),
			last_write_seq: self.last_write_seq(owner)?,
			expires_at: record.expires_at,
		})
	}

	/// Newest sequence `owner` wrote itself, across its durable tables and its
	/// live memtables. `None` means it has never written anything.
	///
	/// Deliberately not read from the manifest's `last_sequence`: that field is
	/// global, and `persist_owner_update` copies the same value into every
	/// owner's state, so it says nothing about an individual branch.
	fn last_write_seq(&self, owner: crate::batch::BatchOwner) -> Result<Option<u64>> {
		let mut newest = 0u64;
		if let Some(levels) = self.inner.level_manifest.read()?.levels_for(owner) {
			for level in levels.get_levels() {
				for table in &level.tables {
					newest = newest.max(table.meta.largest_seq_num.unwrap_or(0));
				}
			}
		}
		if let Some(runtime) = self.inner.runtimes.get(owner) {
			newest = newest.max(runtime.active_memtable.read()?.lsn());
			for entry in runtime.immutable_memtables.read()?.iter() {
				newest = newest.max(entry.memtable.lsn());
			}
		}
		Ok((newest > 0).then_some(newest))
	}
}

impl Drop for Tree {
	fn drop(&mut self) {
		#[cfg(not(target_arch = "wasm32"))]
		{
			// Native environment - use tokio
			if let Ok(handle) = tokio::runtime::Handle::try_current() {
				// Clone the Arc to move into the async task
				let core = Arc::clone(&self.core);
				handle.spawn(async move {
					if let Err(err) = core.close().await {
						log::error!("Error closing store: {}", err);
					}
				});
			} else {
				log::warn!("No runtime available for closing the store correctly");
			}
		}
	}
}

/// A builder for creating LSM trees with type-safe configuration.
pub struct TreeBuilder {
	opts: Options,
}

impl TreeBuilder {
	/// Creates a new TreeBuilder with default options for the specified key
	/// type.
	pub fn new() -> Self {
		Self {
			opts: Options::default(),
		}
	}

	/// Creates a new TreeBuilder with the specified options.
	///
	/// This method ensures type safety by requiring the options to use the same
	/// key type.
	pub fn with_options(opts: Options) -> Self {
		Self {
			opts,
		}
	}

	/// Sets the database path.
	pub fn with_path(mut self, path: std::path::PathBuf) -> Self {
		self.opts = self.opts.with_path(path);
		self
	}

	/// Sets the block size.
	pub fn with_block_size(mut self, size: usize) -> Self {
		self.opts = self.opts.with_block_size(size);
		self
	}

	/// Sets the block restart interval.
	pub fn with_block_restart_interval(mut self, interval: usize) -> Self {
		self.opts = self.opts.with_block_restart_interval(interval);
		self
	}

	/// Sets the filter policy.
	pub fn with_filter_policy(mut self, policy: Option<Arc<dyn FilterPolicy>>) -> Self {
		self.opts = self.opts.with_filter_policy(policy);
		self
	}

	/// Sets the comparator.
	pub fn with_comparator(mut self, comparator: Arc<dyn Comparator>) -> Self {
		self.opts = self.opts.with_comparator(comparator);
		self
	}

	/// Disables compression for data blocks in SSTables.
	///
	/// Use this when compression overhead is not desired or when
	/// data is already compressed at the application level.
	///
	/// # Example
	///
	/// ```no_run
	/// use surrealkv::TreeBuilder;
	///
	/// let tree = TreeBuilder::new()
	///     .with_path("./data".into())
	///     .without_compression()
	///     .build()
	///     .unwrap();
	/// ```
	pub fn without_compression(mut self) -> Self {
		self.opts = self.opts.without_compression();
		self
	}

	/// Sets the number of levels.
	pub fn with_level_count(mut self, count: u8) -> Self {
		self.opts = self.opts.with_level_count(count);
		self
	}

	/// Sets the maximum memtable size.
	pub fn with_max_memtable_size(mut self, size: usize) -> Self {
		self.opts = self.opts.with_max_memtable_size(size);
		self
	}

	/// Sets the initial arena capacity for non-default branch memtables.
	pub fn with_branch_memtable_size(mut self, size: usize) -> Self {
		self.opts = self.opts.with_branch_memtable_size(size);
		self
	}

	/// Sets the database-wide budget over all branch memtable arenas.
	pub fn with_write_buffer_budget(mut self, budget: Option<u64>) -> Self {
		self.opts = self.opts.with_write_buffer_budget(budget);
		self
	}

	/// Sets how many WAL segments a cold branch memtable may pin before the
	/// trickle policy rotates it toward flush.
	pub fn with_wal_pinned_segment_limit(mut self, limit: usize) -> Self {
		self.opts = self.opts.with_wal_pinned_segment_limit(limit);
		self
	}

	/// Sets the unified block cache capacity (includes data blocks, index
	/// and index blocks).
	pub fn with_block_cache_capacity(mut self, capacity_bytes: u64) -> Self {
		self.opts = self.opts.with_block_cache_capacity(capacity_bytes);
		self
	}

	/// Sets the index partition size.
	pub fn with_index_partition_size(mut self, size: usize) -> Self {
		self.opts = self.opts.with_index_partition_size(size);
		self
	}

	/// Enables or disables versioned queries with timestamp tracking
	pub fn with_versioning(mut self, enable: bool, retention_ns: u64) -> Self {
		self.opts = self.opts.with_versioning(enable, retention_ns);
		self
	}

	/// Controls whether to flush the active memtable during database shutdown.
	pub fn with_flush_on_close(mut self, value: bool) -> Self {
		self.opts = self.opts.with_flush_on_close(value);
		self
	}

	/// Set the memtable stall threshold.
	pub fn with_memtable_stall_threshold(mut self, value: usize) -> Self {
		self.opts = self.opts.with_memtable_stall_threshold(value);
		self
	}

	/// Set the L0 stall threshold.
	pub fn with_l0_stall_threshold(mut self, value: usize) -> Self {
		self.opts = self.opts.with_l0_stall_threshold(value);
		self
	}

	/// Builds the LSM tree with the configured options.
	///
	/// This method ensures type safety by using the same key type K
	/// for both the builder and the resulting tree.
	pub fn build(self) -> Result<Tree> {
		Tree::new(Arc::new(self.opts))
	}

	/// Builds the LSM tree and returns both the tree and the options.
	///
	/// This is useful when you need to keep a reference to the options
	/// after creating the tree.
	pub fn build_with_options(self) -> Result<(Tree, Arc<Options>)> {
		let opts = Arc::new(self.opts);
		let tree = Tree::new(Arc::clone(&opts))?;
		Ok((tree, opts))
	}
}

impl Default for TreeBuilder {
	fn default() -> Self {
		Self::new()
	}
}
/// Syncs a directory to ensure all changes are persisted to disk
pub(crate) fn fsync_directory<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
	let path = path.as_ref();

	// Check if the directory still exists before trying to sync it
	if !path.exists() {
		return Ok(());
	}

	// On Windows, calling sync_all() on a directory handle returns
	// ERROR_ACCESS_DENIED (os error 5) because FlushFileBuffers requires
	// GENERIC_WRITE, which is not available for directories. NTFS journals
	// directory metadata automatically, so this is safe to skip.
	#[cfg(not(target_os = "windows"))]
	{
		let file = File::open(path)?;
		debug_assert!(file.metadata()?.is_dir());
		file.sync_all()?;
	}

	Ok(())
}

/// Syncs all directory structures for the LSM store to ensure durability
/// Returns explicit errors indicating which path failed
fn sync_directory_structure(opts: &Options) -> Result<()> {
	// Sync all subdirectories with explicit error handling
	fsync_directory(opts.sstable_dir()).map_err(|e| {
		Error::Other(format!(
			"Failed to sync SSTable directory '{}': {}",
			opts.sstable_dir().display(),
			e
		))
	})?;

	fsync_directory(opts.wal_dir()).map_err(|e| {
		Error::Other(format!("Failed to sync WAL directory '{}': {}", opts.wal_dir().display(), e))
	})?;

	fsync_directory(crate::authority::publish::catalog_dir(&opts.path)).map_err(|e| {
		Error::Other(format!(
			"Failed to sync catalog directory '{}': {}",
			crate::authority::publish::catalog_dir(&opts.path).display(),
			e
		))
	})?;

	fsync_directory(&opts.path).map_err(|e| {
		Error::Other(format!("Failed to sync base directory '{}': {}", opts.path.display(), e))
	})?;

	Ok(())
}
