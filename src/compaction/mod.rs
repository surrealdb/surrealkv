//! Compaction system inspired by TigerBeetle's beat/bar pacing model.
//!
//! Compaction is driven synchronously from the commit path rather than
//! by background tasks. Each commit advances one "beat" of compaction work.
//! A "bar" (default 32 beats) represents one full compaction cycle.
//!
//! Levels are split into two groups that alternate per half-bar:
//! - First half-bar: ODD target levels active (immutable→0, 1→2, 3→4, 5→6)
//! - Second half-bar: EVEN target levels active (0→1, 2→3, 4→5)

pub(crate) mod compactor;
pub(crate) mod leveled;

use std::sync::Arc;

use crate::compaction::compactor::{CompactionOptions, Compactor};
use crate::compaction::leveled::Strategy;
use crate::error::BackgroundErrorHandler;
use crate::levels::LevelManifest;
use crate::memtable::ImmutableMemtables;
use crate::snapshot::SnapshotTracker;
use crate::{Options, Result};

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

/// Default number of beats per compaction bar (matches TigerBeetle's default)
pub const DEFAULT_BEATS_PER_BAR: u64 = 32;

/// Compaction scheduler that drives compaction from the commit path.
///
/// Mirrors TigerBeetle's `CompactionSchedule` from `forest.zig`.
/// Each call to `compact_beat()` does one beat of compaction work.
/// A bar consists of `beats_per_bar` beats, split into two half-bars.
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
		}
	}

	/// Determines if a target level is active for the given beat.
	///
	/// Mirrors TigerBeetle's `level_active()` from `forest.zig:1105-1109`:
	/// ```zig
	/// return (compaction_beat < half_bar_beat_count) == (options.level_b % 2 == 1);
	/// ```
	///
	/// First half-bar: ODD target levels (immutable→0, 1→2, 3→4, 5→6)
	/// Second half-bar: EVEN target levels (0→1, 2→3, 4→5)
	fn level_active(&self, level_b: u8, beat: u64) -> bool {
		let is_first_half = beat < self.half_bar;
		let is_odd_level = level_b % 2 == 1;
		is_first_half == is_odd_level
	}

	/// Called on every commit. Does one beat of compaction work.
	///
	/// Mirrors TigerBeetle's `forest.compact(op)` from `forest.zig:432-479`.
	///
	/// The scheduler determines which levels are active for this beat
	/// and runs compaction for each. Manifest updates are applied at
	/// half-bar boundaries.
	pub(crate) fn compact_beat(&mut self) -> Result<()> {
		let beat = self.op % self.beats_per_bar;
		let is_half_bar_start = beat == 0 || beat == self.half_bar;

		// Only attempt compaction at half-bar boundaries.
		// This is where we select tables and run a full merge for active levels.
		// In TigerBeetle, work is spread across all beats, but for V1 we
		// do the full merge at the half-bar start to keep the implementation simple.
		// The key benefits we get: even/odd level split, least-overlap selection,
		// and move-table optimization.
		if is_half_bar_start {
			self.run_half_bar_compactions(beat)?;
		}

		self.op += 1;
		Ok(())
	}

	/// Run compactions for all active levels in this half-bar.
	fn run_half_bar_compactions(&self, beat: u64) -> Result<()> {
		let level_count = self.opts.level_count;

		// Compact each active level
		for level_b in 0..level_count {
			if !self.level_active(level_b, beat) {
				continue;
			}

			// Skip level 0 as target - it's handled by memtable flush.
			// Level 0 as source → level 1 is an odd target (level_b=1),
			// handled in first half-bar.
			if level_b == 0 {
				continue;
			}

			// Use a strategy that targets this specific source level
			let source_level = level_b - 1;
			let strategy: Arc<dyn CompactionStrategy> =
				Arc::new(Strategy::with_target_level(Arc::clone(&self.opts), source_level));

			// Create compaction options with real shared state
			let compaction_opts = CompactionOptions {
				lopts: Arc::clone(&self.opts),
				level_manifest: Arc::clone(&self.level_manifest),
				immutable_memtables: Arc::clone(&self.immutable_memtables),
				error_handler: Arc::clone(&self.error_handler),
				snapshot_tracker: self.snapshot_tracker.clone(),
			};

			// Use the existing Compactor which handles merge, manifest updates, etc.
			let compactor = Compactor::new(compaction_opts, strategy);
			match compactor.compact() {
				Ok(()) => {}
				Err(e) => {
					// Log error but continue with other levels
					log::error!("Compaction error L{} → L{}: {:?}", source_level, level_b, e);
					self.error_handler
						.set_error(e.clone(), crate::error::BackgroundErrorReason::Compaction);
					return Err(e);
				}
			}
		}

		Ok(())
	}
}
