mod bloom;
mod commit_ring;
mod pipeline;
mod queue;
mod sync;

pub(crate) use pipeline::CommitPipeline;
#[cfg(test)]
pub(crate) use pipeline::{
	CommitStage,
	FreeStats,
	PipelineHook,
	MAX_GROUP_BYTES,
	RETIRED_FREE_CHUNK,
	RETIRE_EVERY_ENTRIES,
	RETIRE_EVERY_GROUPS,
	UNFENCED_STALE_ROUNDS,
};

/// The commit ring's capacity, for tests that need to lap it.
#[cfg(test)]
pub(crate) const COMMIT_RING_CAPACITY: usize = commit_ring::DEFAULT_COMMIT_RING_CAPACITY;

/// How many commits admission lets in at once, for tests that fill it.
#[cfg(test)]
pub(crate) const ADMISSION_PERMITS: usize = pipeline::ADMISSION_PERMITS as usize;
