//! Compaction system inspired by TigerBeetle's beat/bar pacing model.
//!
//! Compaction is driven synchronously from the commit path rather than
//! by background tasks. Each commit advances one "beat" of compaction work.
//! A "bar" (default 32 beats) represents one full compaction cycle.
//!
//! ## Beat Lifecycle
//!
//! ```text
//! Half-bar start (beat 0 or beat half_bar):
//!   bar_commence → select tables for all active levels, calculate quotas
//!
//! Every beat:
//!   beat_commence → set beat_quota = ceil(remaining / beats_remaining)
//!   merge work    → process beat_quota entries per compaction
//!
//! Half-bar end (last beat of each half):
//!   bar_complete → apply ALL manifest changes at once, cleanup
//! ```
//!
//! Levels are split into two groups that alternate per half-bar:
//! - First half-bar: ODD target levels active (immutable→0, 1→2, 3→4, 5→6)
//! - Second half-bar: EVEN target levels active (0→1, 2→3, 4→5)

pub(crate) mod compactor;
pub(crate) mod leveled;

use std::fs::File as SysFile;
use std::path::PathBuf;
use std::sync::Arc;

use crate::compaction::compactor::{
	cleanup_old_tables,
	update_manifest,
	upload_and_open_table,
	CompactionOptions,
	HiddenTablesGuard,
};
use crate::compaction::leveled::Strategy;
use crate::error::BackgroundErrorHandler;
use crate::iter::{BoxedLSMIterator, CompactionIterator};
use crate::levels::LevelManifest;
use crate::manifest::{write_manifest_to_disk, ManifestUploader};
use crate::memtable::ImmutableMemtables;
use crate::snapshot::SnapshotTracker;
use crate::sstable::sst_id::SstId;
use crate::sstable::table::{OwnedTableIter, Table, TableWriter};
use crate::tablestore::TableStore;
use crate::{Comparator, Options, Result};

/// Represents the input for a compaction operation
#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct CompactionInput {
	pub tables_to_merge: Vec<SstId>,
	pub target_level: u8,
	pub source_level: u8,
}

/// Represents the possible compaction decisions
#[derive(Debug, Eq, PartialEq)]
pub enum CompactionChoice {
	Merge(CompactionInput),
	/// Move table to next level without merge (zero overlap optimization)
	Move(CompactionInput),
	Skip,
}

/// Defines the strategy interface for compaction
pub trait CompactionStrategy: Send + Sync {
	/// Determines which levels should be compacted
	fn pick_levels(&self, manifest: &LevelManifest) -> Result<CompactionChoice>;
}

/// Default number of beats per compaction bar
pub const DEFAULT_BEATS_PER_BAR: u64 = 32;

// ============================================================================
// CompactionStage: explicit state machine for LiveCompaction lifecycle
// ============================================================================

/// Lifecycle stage of a LiveCompaction.
///
/// Transition diagram:
/// ```text
///   Commenced ──► Paused ──► Completed
///       │           ▲  │         ▲
///       │           └──┘         │
///       └────────────────────────┘
/// ```
///
/// - `Commenced`: initial state after `begin()`, no entries processed yet
/// - `Paused`: beat quota consumed, entries remain, waiting for next beat
/// - `Completed`: iterator exhausted, table file finalized, waiting for `commit()`
///
/// Note: There is no `Beat` variant. Since SurrealKV's compaction is synchronous
/// (no async I/O pipeline), the "actively processing" state is transient within
/// `advance()` and never observed externally. TigerBeetle needs `beat` and
/// `beat_quota_done` stages for async I/O draining; we don't.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompactionStage {
	/// Initial state after begin(). Tables selected, iterator+writer created, no entries
	/// processed.
	Commenced,
	/// Beat quota consumed, entries remain. Waiting for next beat.
	Paused,
	/// Iterator exhausted, table file finalized. Waiting for commit() at half-bar end.
	Completed,
}

// ============================================================================
// CompactionQuotas: pacing state for beat/bar quota tracking
// ============================================================================

/// Tracks compaction progress for quota-based pacing.
///
/// At the start of a half-bar, `bar` is set to the total estimated entries.
/// Each beat, `commence_beat()` divides remaining work by remaining beats.
/// `record_entry()` increments both beat and bar counters per entry processed.
#[derive(Debug)]
struct CompactionQuotas {
	/// Total estimated entries for the entire half-bar (from table metadata sum).
	bar: u64,
	/// Entries processed so far across all beats in this half-bar.
	bar_done: u64,
	/// Budget for the current beat (set by `commence_beat()`).
	beat: u64,
	/// Entries processed in the current beat.
	beat_done: u64,
}

impl CompactionQuotas {
	fn new(bar: u64) -> Self {
		Self {
			bar,
			bar_done: 0,
			beat: 0,
			beat_done: 0,
		}
	}

	/// Whether the current beat's quota has been consumed.
	fn beat_exhausted(&self) -> bool {
		debug_assert!(
			self.bar_done <= self.bar || self.bar_done > self.bar,
			"bar_done tracking is consistent"
		);
		self.beat_done >= self.beat
	}

	/// Whether the entire bar's estimated quota has been consumed.
	/// Note: the iterator may still have entries beyond the estimate.
	fn bar_exhausted(&self) -> bool {
		self.bar_done >= self.bar
	}

	/// Set up the beat quota based on remaining work and remaining beats.
	fn commence_beat(&mut self, beats_remaining: u64) {
		debug_assert!(beats_remaining > 0, "cannot commence beat with zero beats remaining");
		let remaining = self.bar.saturating_sub(self.bar_done);
		self.beat = remaining.div_ceil(beats_remaining);
		self.beat_done = 0;
	}

	/// Record one entry processed.
	fn record_entry(&mut self) {
		self.beat_done += 1;
		self.bar_done += 1;
	}

	/// Returns true if any entries were written.
	fn has_output(&self) -> bool {
		self.bar_done > 0
	}
}

// ============================================================================
// LiveCompaction: state for a merge compaction spread across beats
// ============================================================================

/// A compaction in progress, spread across multiple beats within a half-bar.
///
/// - Created at half-bar start (bar_commence) in `Commenced` stage
/// - Advanced by quota each beat — transitions through `Paused`/`Completed`
/// - Committed at half-bar end (bar_complete) — consumes self
struct LiveCompaction {
	/// Lifecycle stage — replaces merge_done / write_done booleans.
	stage: CompactionStage,
	/// Quota tracking — replaces quota_bar / quota_bar_done fields.
	quotas: CompactionQuotas,
	/// Owned compaction iterator — no external borrows, persists across beats.
	iterator: CompactionIterator<'static>,
	/// Table writer — data blocks flush to disk during add(), finish() writes index+footer.
	/// Option because finish() takes self by value.
	writer: Option<TableWriter<SysFile>>,
	/// Path to the new table file being written
	new_table_path: PathBuf,
	/// ID of the new table being written
	new_table_id: SstId,
	/// Compaction input (source/target levels, table IDs to merge)
	input: CompactionInput,
	/// RAII guard — unhides tables if compaction fails before manifest commit
	guard: HiddenTablesGuard,
	/// Keeps source tables alive for the owned iterators
	_tables: Vec<Arc<Table>>,
}

// SAFETY: All fields of LiveCompaction are Send:
// - CompactionIterator<'static> contains MergingIterator<'static> with OwnedTableIter (which holds
//   Arc<Table> + TableIterator with owned data). The only reason the compiler can't prove Send is
//   that BoxedLSMIterator = Box<dyn LSMIterator> doesn't have a Send bound. But our concrete
//   iterators (OwnedTableIter) are Send.
// - TableWriter<SysFile>: SysFile is Send
// - HiddenTablesGuard: holds Arc<RwLock<...>> which is Send
// - Vec<Arc<Table>>: Send
// - All other fields are primitive/owned types
unsafe impl Send for LiveCompaction {}

impl LiveCompaction {
	/// Whether this compaction has finished merging and is waiting for commit.
	fn is_completed(&self) -> bool {
		self.stage == CompactionStage::Completed
	}

	/// bar_commence equivalent: select tables, create iterator + writer, calculate quota.
	///
	/// Returns None if the level doesn't need compaction (strategy returns Skip).
	/// The returned LiveCompaction starts in `Commenced` stage.
	fn begin(opts: &CompactionOptions, source_level: u8, target_level: u8) -> Result<Option<Self>> {
		let strategy = Strategy::with_target_level(Arc::clone(&opts.lopts), source_level);

		let mut levels = opts.level_manifest.write()?;
		let choice = strategy.pick_levels(&levels)?;

		let input = match choice {
			CompactionChoice::Merge(input) => input,
			// Move and Skip are not handled here — caller deals with them
			_ => return Ok(None),
		};

		// Hide tables being merged
		levels.hide_tables(&input.tables_to_merge);

		// Create RAII guard for unhiding on failure
		let guard =
			HiddenTablesGuard::new(Arc::clone(&opts.level_manifest), &input.tables_to_merge);

		// Get the tables as Arc<Table> to keep them alive
		let all_tables = levels.get_all_tables();
		let tables: Vec<Arc<Table>> =
			input.tables_to_merge.iter().filter_map(|&id| all_tables.get(&id).cloned()).collect();

		// Estimate total entries from table metadata
		let quota_bar: u64 = tables.iter().map(|t| t.meta.properties.num_entries).sum();

		drop(levels);

		// Create owned iterators (no lifetime — can be stored across beats)
		let iterators: Vec<BoxedLSMIterator<'static>> = tables
			.iter()
			.filter_map(|table| OwnedTableIter::new(Arc::clone(table), None).ok())
			.map(|iter| Box::new(iter) as BoxedLSMIterator<'static>)
			.collect();

		// Get active snapshots for snapshot-aware compaction
		let snapshots = opts.snapshot_tracker.get_all_snapshots();

		// Create compaction iterator
		let max_level = opts.lopts.level_count - 1;
		let is_bottom_level = target_level >= max_level;
		let iterator = CompactionIterator::new(
			iterators,
			Arc::clone(&opts.lopts.internal_comparator) as Arc<dyn Comparator>,
			is_bottom_level,
			opts.lopts.enable_versioning,
			opts.lopts.versioned_history_retention_ns,
			Arc::clone(&opts.lopts.clock),
			snapshots,
		);

		// Create table writer for the output
		let new_table_id = crate::levels::next_sst_id();
		let new_table_path = opts.lopts.sstable_file_path(new_table_id);
		let file = SysFile::create(&new_table_path)?;
		let writer = TableWriter::new(file, new_table_id, Arc::clone(&opts.lopts), target_level);

		Ok(Some(Self {
			stage: CompactionStage::Commenced,
			quotas: CompactionQuotas::new(quota_bar),
			iterator,
			writer: Some(writer),
			new_table_path,
			new_table_id,
			input,
			guard,
			_tables: tables,
		}))
	}

	/// Process one beat of compaction work.
	///
	/// State transitions:
	///   Commenced/Paused → Paused     (beat quota consumed, entries remain)
	///   Commenced/Paused → Completed  (iterator exhausted)
	///   Completed        → Completed  (no-op)
	async fn advance(&mut self, beats_remaining: u64) -> Result<()> {
		if self.stage == CompactionStage::Completed {
			return Ok(());
		}

		debug_assert!(
			self.stage == CompactionStage::Commenced || self.stage == CompactionStage::Paused,
			"advance() called in invalid stage: {:?}",
			self.stage
		);

		// Set up beat quota
		self.quotas.commence_beat(beats_remaining);

		// Process entries up to beat quota
		while !self.quotas.beat_exhausted() {
			match self.iterator.advance().await? {
				Some((key, value)) => {
					if let Some(ref mut writer) = self.writer {
						writer.add(key, &value)?;
					}
					self.quotas.record_entry();
				}
				None => {
					self.finalize_table()?;
					self.stage = CompactionStage::Completed;
					return Ok(());
				}
			}
		}

		// Beat quota consumed. Check if bar quota also exhausted.
		if self.quotas.bar_exhausted() {
			// Estimate may be imprecise — drain any remaining entries
			self.drain_remaining().await?;
			self.finalize_table()?;
			self.stage = CompactionStage::Completed;
		} else {
			self.stage = CompactionStage::Paused;
		}

		Ok(())
	}

	/// Drain any remaining entries after bar quota is exhausted.
	async fn drain_remaining(&mut self) -> Result<()> {
		loop {
			match self.iterator.advance().await? {
				Some((key, value)) => {
					if let Some(ref mut writer) = self.writer {
						writer.add(key, &value)?;
					}
					self.quotas.record_entry();
				}
				None => break,
			}
		}
		Ok(())
	}

	/// Finalize the table file: call writer.finish() or clean up empty file.
	fn finalize_table(&mut self) -> Result<()> {
		if !self.quotas.has_output() {
			// No entries — drop writer and remove empty file
			self.writer.take();
			let _ = std::fs::remove_file(&self.new_table_path);
		} else if let Some(writer) = self.writer.take() {
			writer.finish()?;
		}
		Ok(())
	}

	/// bar_complete equivalent: open new table, update manifest, cleanup old tables.
	///
	/// Called at half-bar end. Consumes self.
	async fn commit(mut self, opts: &CompactionOptions) -> Result<()> {
		// If not yet completed, drain and finalize (safety net for imprecise estimates)
		if self.stage != CompactionStage::Completed {
			log::warn!("commit() called in {:?} stage — draining remaining entries", self.stage);
			self.drain_remaining().await?;
			self.finalize_table()?;
			self.stage = CompactionStage::Completed;
		}

		debug_assert_eq!(self.stage, CompactionStage::Completed);

		// Upload local file to object store and open the new table (if entries were written)
		let new_table = if self.quotas.has_output() {
			Some(
				upload_and_open_table(
					&opts.lopts,
					self.new_table_id,
					&self.new_table_path,
					&opts.table_store,
				)
				.await?,
			)
		} else {
			None
		};

		// Update manifest and commit guard
		update_manifest(opts, &self.input, new_table, &mut self.guard)?;

		// Cleanup old table files from object store
		cleanup_old_tables(&opts.table_store, &self.input).await;

		Ok(())
	}
}

// ============================================================================
// PendingManifestOp: deferred manifest-only operations
// ============================================================================

/// A manifest-only operation deferred to half-bar end.
enum PendingManifestOp {
	MoveTable {
		source_level: u8,
		target_level: u8,
		table_id: SstId,
	},
}

// ============================================================================
// CompactionScheduler: drives compaction from the commit path
// ============================================================================

/// Compaction scheduler that drives compaction from the commit path.
///
/// Each call to `compact_beat()` does one beat of compaction work.
/// A bar consists of `beats_per_bar` beats, split into two half-bars.
///
/// - Half-bar start: `begin_half_bar()` — select tables, create LiveCompactions
/// - Every beat: advance each LiveCompaction by its quota
/// - Half-bar end: `complete_half_bar()` — commit all manifest changes at once
pub(crate) struct CompactionScheduler {
	/// Number of beats per bar (default 32)
	beats_per_bar: u64,
	/// Half of beats_per_bar
	half_bar: u64,
	/// Global operation counter - incremented on each beat
	op: u64,
	/// Shared options
	opts: Arc<Options>,
	/// Level manifest (shared with CoreInner)
	level_manifest: Arc<std::sync::RwLock<LevelManifest>>,
	/// Immutable memtables (shared with CoreInner)
	immutable_memtables: Arc<std::sync::RwLock<ImmutableMemtables>>,
	/// Error handler
	error_handler: Arc<BackgroundErrorHandler>,
	/// Snapshot tracker for snapshot-aware compaction
	snapshot_tracker: SnapshotTracker,
	/// Table store for SST reads/writes via object store
	table_store: Arc<TableStore>,
	/// Manifest uploader for async object store uploads
	manifest_uploader: Arc<ManifestUploader>,
	/// Active merge compactions being spread across the current half-bar
	active_compactions: Vec<LiveCompaction>,
	/// Deferred manifest-only operations (moves) applied at half-bar end
	pending_manifest_ops: Vec<PendingManifestOp>,
}

impl CompactionScheduler {
	pub(crate) fn new(
		opts: Arc<Options>,
		level_manifest: Arc<std::sync::RwLock<LevelManifest>>,
		immutable_memtables: Arc<std::sync::RwLock<ImmutableMemtables>>,
		error_handler: Arc<BackgroundErrorHandler>,
		snapshot_tracker: SnapshotTracker,
		table_store: Arc<TableStore>,
		manifest_uploader: Arc<ManifestUploader>,
	) -> Self {
		let beats_per_bar = opts.compaction_beats_per_bar;
		Self {
			beats_per_bar,
			half_bar: beats_per_bar / 2,
			op: 0,
			opts,
			level_manifest,
			immutable_memtables,
			error_handler,
			snapshot_tracker,
			table_store,
			manifest_uploader,
			active_compactions: Vec::new(),
			pending_manifest_ops: Vec::new(),
		}
	}

	/// Determines if a target level is active for the given beat.
	///
	/// First half-bar: ODD target levels (immutable→0, 1→2, 3→4, 5→6)
	/// Second half-bar: EVEN target levels (0→1, 2→3, 4→5)
	fn level_active(&self, level_b: u8, beat: u64) -> bool {
		let is_first_half = beat < self.half_bar;
		let is_odd_level = level_b % 2 == 1;
		is_first_half == is_odd_level
	}

	/// Build CompactionOptions from scheduler state.
	fn compaction_opts(&self) -> CompactionOptions {
		CompactionOptions {
			lopts: Arc::clone(&self.opts),
			level_manifest: Arc::clone(&self.level_manifest),
			immutable_memtables: Arc::clone(&self.immutable_memtables),
			error_handler: Arc::clone(&self.error_handler),
			snapshot_tracker: self.snapshot_tracker.clone(),
			table_store: Arc::clone(&self.table_store),
			manifest_uploader: Arc::clone(&self.manifest_uploader),
		}
	}

	/// Called on every commit. Does one beat of compaction work.
	///
	/// Follows TigerBeetle's lifecycle:
	/// 1. Half-bar start: bar_commence (select tables, create compactions)
	/// 2. Every beat: advance compactions by quota
	/// 3. Half-bar end: bar_complete (commit manifest changes)
	pub(crate) async fn compact_beat(&mut self) -> Result<()> {
		let beat = self.op % self.beats_per_bar;
		let half_bar_beat = beat % self.half_bar;
		let is_half_bar_start = beat == 0 || beat == self.half_bar;
		let is_half_bar_end = beat == self.half_bar - 1 || beat == self.beats_per_bar - 1;

		// ── bar_commence: at half-bar start, set up compactions for all active levels ──
		if is_half_bar_start {
			self.begin_half_bar(beat)?;
		}

		// ── beat_commence + merge: every beat, advance each compaction by quota ──
		let beats_remaining = self.half_bar - half_bar_beat;
		for compaction in &mut self.active_compactions {
			if compaction.is_completed() {
				continue;
			}
			compaction.advance(beats_remaining).await?;
		}

		// ── bar_complete: at half-bar end, commit all manifest changes at once ──
		if is_half_bar_end {
			self.complete_half_bar().await?;
		}

		self.op += 1;
		Ok(())
	}

	/// bar_commence: select tables for all active levels, create LiveCompactions.
	///
	/// For each active level, the strategy decides Merge/Move/Skip:
	/// - Merge → create a LiveCompaction (spread across beats)
	/// - Move → defer to pending_manifest_ops (applied at half-bar end)
	/// - Skip → no work needed
	fn begin_half_bar(&mut self, beat: u64) -> Result<()> {
		debug_assert!(
			self.active_compactions.is_empty(),
			"begin_half_bar called with {} active compactions still present",
			self.active_compactions.len()
		);

		let opts = self.compaction_opts();
		let level_count = self.opts.level_count;

		for level_b in 0..level_count {
			if !self.level_active(level_b, beat) {
				continue;
			}

			// Skip level 0 as target — handled by memtable flush
			if level_b == 0 {
				continue;
			}

			let source_level = level_b - 1;
			let target_level = level_b;

			// Check for move-table first via strategy
			let strategy = Strategy::with_target_level(Arc::clone(&self.opts), source_level);
			let choice = {
				let levels = self.level_manifest.read()?;
				strategy.pick_levels(&levels)?
			};

			match choice {
				CompactionChoice::Move(input) => {
					assert_eq!(input.tables_to_merge.len(), 1);
					self.pending_manifest_ops.push(PendingManifestOp::MoveTable {
						source_level: input.source_level,
						target_level: input.target_level,
						table_id: input.tables_to_merge[0],
					});
				}
				CompactionChoice::Merge(_) => {
					// LiveCompaction::begin does its own strategy call and table selection
					match LiveCompaction::begin(&opts, source_level, target_level) {
						Ok(Some(live)) => {
							self.active_compactions.push(live);
						}
						Ok(None) => {
							// Strategy returned Skip on second check (race-free since
							// we hold the lock inside begin)
						}
						Err(e) => {
							log::error!(
								"Failed to begin compaction L{} → L{}: {:?}",
								source_level,
								target_level,
								e
							);
							self.error_handler.set_error(
								e.clone(),
								crate::error::BackgroundErrorReason::Compaction,
							);
							return Err(e);
						}
					}
				}
				CompactionChoice::Skip => {}
			}
		}

		Ok(())
	}

	/// bar_complete: commit all manifest changes at once.
	async fn complete_half_bar(&mut self) -> Result<()> {
		let opts = self.compaction_opts();

		// Commit all active merge compactions
		let compactions = std::mem::take(&mut self.active_compactions);
		for compaction in compactions {
			debug_assert!(
				compaction.stage == CompactionStage::Completed
					|| compaction.stage == CompactionStage::Paused,
				"complete_half_bar: unexpected stage {:?}",
				compaction.stage
			);
			if let Err(e) = compaction.commit(&opts).await {
				log::error!("Failed to commit compaction: {:?}", e);
				self.error_handler
					.set_error(e.clone(), crate::error::BackgroundErrorReason::Compaction);
				return Err(e);
			}
		}

		// Apply deferred move-table operations
		let pending_ops = std::mem::take(&mut self.pending_manifest_ops);
		if !pending_ops.is_empty() {
			let mut levels = self.level_manifest.write()?;
			for op in pending_ops {
				match op {
					PendingManifestOp::MoveTable {
						source_level,
						target_level,
						table_id,
					} => {
						levels.move_table(source_level, target_level, table_id)?;
						log::debug!(
							"Move-table: L{} → L{}, table_id={}",
							source_level,
							target_level,
							table_id
						);
					}
				}
			}
			let bytes = write_manifest_to_disk(&mut levels)?;
			self.manifest_uploader.queue_upload(levels.manifest_id, bytes);
		}

		Ok(())
	}
}
