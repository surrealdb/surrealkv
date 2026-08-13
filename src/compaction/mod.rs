//! Contains compaction strategies

pub(crate) mod compactor;
pub(crate) mod leveled;

use crate::levels::LevelManifest;
use crate::Result;

/// Represents the input for a compaction operation
#[derive(Debug, Eq, PartialEq, Clone)]
pub(crate) struct CompactionInput {
	pub tables_to_merge: Vec<u64>,
	pub target_level: u8,
	pub source_level: u8, // Added to track the source level
}

/// Represents the possible compaction decisions
#[derive(Debug, Eq, PartialEq)]
pub enum CompactionChoice {
	Merge(CompactionInput),
	Skip, // Added to explicitly handle cases where compaction isn't needed
}

/// Defines the strategy interface for compaction
pub trait CompactionStrategy: Send + Sync {
	/// Determines which levels of one physical owner's level set should be
	/// compacted. Strategies only ever see the selected owner's tables, so a
	/// mixed-owner compaction cannot be picked.
	fn pick_levels(
		&self,
		manifest: &LevelManifest,
		owner: crate::batch::BatchOwner,
	) -> Result<CompactionChoice>;
}
