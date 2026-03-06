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
	open_table,
	update_manifest,
	CompactionOptions,
	HiddenTablesGuard,
};
use crate::compaction::leveled::Strategy;
use crate::error::BackgroundErrorHandler;
use crate::iter::{BoxedLSMIterator, CompactionIterator};
use crate::levels::{write_manifest_to_disk, LevelManifest};
use crate::memtable::ImmutableMemtables;
use crate::snapshot::SnapshotTracker;
use crate::sstable::table::{OwnedTableIter, Table, TableWriter};
use crate::{Comparator, Options, Result};

/// Represents the input for a compaction operation
#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct CompactionInput {
	pub tables_to_merge: Vec<u64>,
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
// LiveCompaction: state for a merge compaction spread across beats
// ============================================================================

/// A compaction in progress, spread across multiple beats within a half-bar.
///
/// - Created at half-bar start (bar_commence)
/// - Advanced by quota each beat (beat_commence + merge work)
/// - Committed at half-bar end (bar_complete)
struct LiveCompaction {
	/// Owned compaction iterator — no external borrows, persists across beats.
	iterator: CompactionIterator<'static>,
	/// Table writer — data blocks flush to disk during add(), finish() writes index+footer.
	/// Option because finish() takes self by value.
	writer: Option<TableWriter<SysFile>>,
	/// Path to the new table file being written
	new_table_path: PathBuf,
	/// ID of the new table being written
	new_table_id: u64,
	/// Compaction input (source/target levels, table IDs to merge)
	input: CompactionInput,
	/// RAII guard — unhides tables if compaction fails before manifest commit
	guard: HiddenTablesGuard,
	/// Keeps source tables alive for the owned iterators
	_tables: Vec<Arc<Table>>,
	/// Bar quota: total estimated entries for this compaction (from table metadata)
	quota_bar: u64,
	/// Entries processed so far across all beats
	quota_bar_done: u64,
	/// Whether the merge iterator is exhausted
	merge_done: bool,
	/// Whether writer.finish() has been called (table file is complete)
	write_done: bool,
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
	/// bar_commence equivalent: select tables, create iterator + writer, calculate quota.
	///
	/// Returns None if the level doesn't need compaction (strategy returns Skip).
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
		let new_table_id = opts.level_manifest.read().unwrap().next_table_id();
		let new_table_path = opts.lopts.sstable_file_path(new_table_id);
		let file = SysFile::create(&new_table_path)?;
		let writer = TableWriter::new(file, new_table_id, Arc::clone(&opts.lopts), target_level);

		Ok(Some(Self {
			iterator,
			writer: Some(writer),
			new_table_path,
			new_table_id,
			input,
			guard,
			_tables: tables,
			quota_bar,
			quota_bar_done: 0,
			merge_done: false,
			write_done: false,
		}))
	}

	/// Process up to `beat_quota` entries from the merge iterator.
	///
	/// Each entry is fed to the TableWriter, which flushes data blocks to disk
	/// incrementally as they fill up. When the iterator is exhausted,
	/// the table file is finalized (index + footer written).
	fn advance(&mut self, beat_quota: u64) -> Result<()> {
		if self.merge_done {
			return Ok(());
		}

		let quota = beat_quota.min(self.quota_bar.saturating_sub(self.quota_bar_done));

		for _ in 0..quota {
			match self.iterator.next() {
				Some(Ok((key, value))) => {
					if let Some(ref mut writer) = self.writer {
						writer.add(key, &value)?;
					}
					self.quota_bar_done += 1;
				}
				Some(Err(e)) => return Err(e),
				None => {
					self.merge_done = true;
					break;
				}
			}
		}

		// If we've hit the bar quota limit, drain the rest
		if !self.merge_done && self.quota_bar_done >= self.quota_bar {
			// The estimate was an upper bound; continue until iterator is actually done
			loop {
				match self.iterator.next() {
					Some(Ok((key, value))) => {
						if let Some(ref mut writer) = self.writer {
							writer.add(key, &value)?;
						}
						self.quota_bar_done += 1;
					}
					Some(Err(e)) => return Err(e),
					None => {
						self.merge_done = true;
						break;
					}
				}
			}
		}

		// Finalize table file when iterator exhausted
		if self.merge_done && !self.write_done {
			if self.quota_bar_done == 0 {
				// No entries — drop writer and remove empty file
				self.writer.take();
				let _ = std::fs::remove_file(&self.new_table_path);
			} else if let Some(writer) = self.writer.take() {
				writer.finish()?;
			}
			self.write_done = true;
		}

		Ok(())
	}

	/// bar_complete equivalent: open new table, update manifest, cleanup old tables.
	///
	/// Called at half-bar end. Consumes self.
	fn commit(mut self, opts: &CompactionOptions) -> Result<()> {
		// If merge isn't done yet (shouldn't happen with correct quota), finish it
		if !self.merge_done {
			// Drain remaining entries
			loop {
				match self.iterator.next() {
					Some(Ok((key, value))) => {
						if let Some(ref mut writer) = self.writer {
							writer.add(key, &value)?;
						}
						self.quota_bar_done += 1;
					}
					Some(Err(e)) => return Err(e),
					None => {
						self.merge_done = true;
						break;
					}
				}
			}
			if self.quota_bar_done == 0 {
				self.writer.take();
				let _ = std::fs::remove_file(&self.new_table_path);
			} else if let Some(writer) = self.writer.take() {
				writer.finish()?;
			}
			self.write_done = true;
		}

		// Open the new table (if entries were written)
		let new_table = if self.quota_bar_done > 0 {
			Some(open_table(&opts.lopts, self.new_table_id, &self.new_table_path)?)
		} else {
			None
		};

		// Update manifest and commit guard
		update_manifest(opts, &self.input, new_table, &mut self.guard)?;

		// Cleanup old table files
		cleanup_old_tables(&opts.lopts, &self.input);

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
		table_id: u64,
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
		}
	}

	/// Called on every commit. Does one beat of compaction work.
	///
	/// Follows TigerBeetle's lifecycle:
	/// 1. Half-bar start: bar_commence (select tables, create compactions)
	/// 2. Every beat: advance compactions by quota
	/// 3. Half-bar end: bar_complete (commit manifest changes)
	pub(crate) fn compact_beat(&mut self) -> Result<()> {
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
			if compaction.merge_done {
				continue;
			}
			let remaining = compaction.quota_bar.saturating_sub(compaction.quota_bar_done);
			let beat_quota = remaining.div_ceil(beats_remaining);
			compaction.advance(beat_quota)?;
		}

		// ── bar_complete: at half-bar end, commit all manifest changes at once ──
		if is_half_bar_end {
			self.complete_half_bar()?;
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
	fn complete_half_bar(&mut self) -> Result<()> {
		let opts = self.compaction_opts();

		// Commit all active merge compactions
		let compactions = std::mem::take(&mut self.active_compactions);
		for compaction in compactions {
			if let Err(e) = compaction.commit(&opts) {
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
			write_manifest_to_disk(&levels)?;
		}

		Ok(())
	}
}
