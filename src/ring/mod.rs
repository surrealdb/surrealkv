mod bloom;
mod commit_ring;
mod pipeline;
mod queue;
mod sync;

pub(crate) use pipeline::CommitPipeline;
#[cfg(test)]
pub(crate) use pipeline::{PipelineHook, MAX_GROUP_BYTES, UNFENCED_STALE_ROUNDS};

/// The commit ring's capacity, for tests that need to lap it.
#[cfg(test)]
pub(crate) const COMMIT_RING_CAPACITY: usize = commit_ring::DEFAULT_COMMIT_RING_CAPACITY;
