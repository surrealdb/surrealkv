//! FK2: the global commit timeline — a strictly increasing `commit_ts → seq`
//! fencepost index over the one commit clock.
//!
//! Stamping happens under the commit write mutex (one writer at a time), so
//! the monotone clamp `max(now, last + 1)` is race-free; readers (resolution,
//! root publishes) take the fencepost lock briefly. Coverage is
//! exact-or-abstain: inside the horizon a resolution is exact, below it the
//! typed `TimestampBelowHorizon` error is returned — never an approximation.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use crate::error::{Error, Result};

/// In-memory fencepost window. The durable tail in root versions is capped at
/// `MAX_TIMELINE_TAIL` (512); this larger window keeps recent resolutions
/// exact between root publishes. Entries beyond it move the horizon floor —
/// an observable, typed boundary, not a silent approximation.
pub(crate) const IN_MEMORY_FENCEPOST_CAP: usize = 4096;

#[derive(Debug)]
pub(crate) struct Timeline {
	/// Highest stamped commit timestamp (the clamp input and horizon end).
	last_commit_ts: AtomicU64,
	/// `(commit_ts, highest_seq)` per commit, strictly increasing on both
	/// axes, pruned at the front to the in-memory cap.
	fenceposts: Mutex<VecDeque<(u64, u64)>>,
}

impl Timeline {
	pub(crate) fn new() -> Self {
		Self {
			last_commit_ts: AtomicU64::new(0),
			fenceposts: Mutex::new(VecDeque::new()),
		}
	}

	/// Stamps a commit: clamps `now` to strictly exceed every prior stamp and
	/// records the fencepost. Called under the commit write mutex only.
	pub(crate) fn stamp(&self, now: u64, highest_seq: u64) -> u64 {
		let commit_ts = now.max(self.last_commit_ts.load(Ordering::Acquire).saturating_add(1));
		self.last_commit_ts.store(commit_ts, Ordering::Release);
		let mut posts = self.fenceposts.lock().unwrap();
		posts.push_back((commit_ts, highest_seq));
		while posts.len() > IN_MEMORY_FENCEPOST_CAP {
			posts.pop_front();
		}
		commit_ts
	}

	/// Seeds the timeline during recovery: `pairs` must be strictly
	/// increasing on both axes (root tails and validated WAL replay both
	/// are). Later seeds append after earlier ones.
	pub(crate) fn seed(&self, pairs: &[(u64, u64)]) {
		if pairs.is_empty() {
			return;
		}
		let mut posts = self.fenceposts.lock().unwrap();
		for &(commit_ts, seq) in pairs {
			debug_assert!(
				posts
					.back()
					.is_none_or(|&(last_ts, last_seq)| { commit_ts > last_ts && seq > last_seq }),
				"timeline seed must strictly increase"
			);
			posts.push_back((commit_ts, seq));
		}
		while posts.len() > IN_MEMORY_FENCEPOST_CAP {
			posts.pop_front();
		}
		let last = posts.back().map(|&(ts, _)| ts).unwrap_or(0);
		self.last_commit_ts.fetch_max(last, Ordering::AcqRel);
	}

	/// Raises the clamp floor without recording a fencepost (fenced batches:
	/// their timestamps were allocated, so new stamps must exceed them even
	/// though their data is unreachable).
	pub(crate) fn observe_floor(&self, commit_ts: u64) {
		self.last_commit_ts.fetch_max(commit_ts, Ordering::AcqRel);
	}

	pub(crate) fn last_commit_ts(&self) -> u64 {
		self.last_commit_ts.load(Ordering::Acquire)
	}

	/// Exact-or-abstain resolution: the highest sequence whose commit
	/// timestamp is at or below `timestamp`. Production caller lands with
	/// FK4's `AtTimestamp` fork selector.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn resolve(&self, timestamp: u64) -> Result<u64> {
		let posts = self.fenceposts.lock().unwrap();
		let Some(&(floor_ts, _)) = posts.front() else {
			return Err(Error::TimestampBelowHorizon {
				requested: timestamp,
				horizon_floor: 0,
			});
		};
		if timestamp < floor_ts {
			return Err(Error::TimestampBelowHorizon {
				requested: timestamp,
				horizon_floor: floor_ts,
			});
		}
		// Binary search for the last fencepost with ts <= timestamp.
		let index = posts.partition_point(|&(ts, _)| ts <= timestamp);
		Ok(posts[index - 1].1)
	}

	/// The covered horizon `[floor_ts, last_commit_ts]`, or `None` when no
	/// fencepost is retained. A metered quantity, not an internal detail.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn horizon(&self) -> Option<(u64, u64)> {
		let posts = self.fenceposts.lock().unwrap();
		posts.front().map(|&(floor_ts, _)| (floor_ts, self.last_commit_ts()))
	}

	/// Replaces the timeline wholesale (checkpoint restore is a whole-clock
	/// swap: the restored root's tail IS the timeline).
	pub(crate) fn reset(&self, pairs: &[(u64, u64)], last_commit_ts: u64) {
		let mut posts = self.fenceposts.lock().unwrap();
		posts.clear();
		posts.extend(pairs.iter().copied());
		drop(posts);
		self.last_commit_ts.store(last_commit_ts, Ordering::Release);
	}

	/// Newest `cap` fenceposts for the durable root tail.
	pub(crate) fn tail(&self, cap: usize) -> Vec<(u64, u64)> {
		let posts = self.fenceposts.lock().unwrap();
		let skip = posts.len().saturating_sub(cap);
		posts.iter().skip(skip).copied().collect()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn stamp_clamps_a_backdated_clock_strictly() {
		let timeline = Timeline::new();
		assert_eq!(timeline.stamp(100, 1), 100);
		// The clock steps backwards; the stamp must still strictly increase.
		assert_eq!(timeline.stamp(40, 2), 101);
		assert_eq!(timeline.stamp(40, 3), 102);
		// A forward clock is taken as-is.
		assert_eq!(timeline.stamp(500, 4), 500);
	}

	#[test]
	fn resolution_is_exact_or_abstains_below_the_floor() {
		let timeline = Timeline::new();
		// Empty timeline abstains.
		assert!(matches!(timeline.resolve(10), Err(Error::TimestampBelowHorizon { .. })));
		timeline.stamp(100, 1);
		timeline.stamp(200, 2);
		timeline.stamp(300, 3);
		assert_eq!(timeline.resolve(100).unwrap(), 1);
		assert_eq!(timeline.resolve(250).unwrap(), 2, "between fenceposts: the earlier commit");
		assert_eq!(timeline.resolve(300).unwrap(), 3);
		assert_eq!(timeline.resolve(9_999).unwrap(), 3, "beyond the head: the newest commit");
		let error = timeline.resolve(99).unwrap_err();
		assert!(matches!(
			error,
			Error::TimestampBelowHorizon {
				requested: 99,
				horizon_floor: 100,
			}
		));
		assert_eq!(timeline.horizon(), Some((100, 300)));
	}

	#[test]
	fn pruning_moves_the_horizon_floor_not_the_answers_inside_it() {
		let timeline = Timeline::new();
		for i in 0..(IN_MEMORY_FENCEPOST_CAP as u64 + 8) {
			timeline.stamp(1_000 + i, i + 1);
		}
		let (floor_ts, last_ts) = timeline.horizon().unwrap();
		assert_eq!(floor_ts, 1_008, "eight oldest fenceposts pruned");
		assert_eq!(last_ts, 1_000 + IN_MEMORY_FENCEPOST_CAP as u64 + 7);
		// Below the floor: typed abstain, never an approximation.
		assert!(matches!(timeline.resolve(1_007), Err(Error::TimestampBelowHorizon { .. })));
		// At the floor: exact.
		assert_eq!(timeline.resolve(1_008).unwrap(), 9);
	}

	#[test]
	fn seed_then_stamp_stays_strictly_monotone() {
		let timeline = Timeline::new();
		timeline.seed(&[(100, 1), (200, 2)]);
		timeline.observe_floor(450);
		// The next stamp must exceed both the seeded tail and the observed
		// fenced floor even with a stale clock.
		assert_eq!(timeline.stamp(90, 3), 451);
		assert_eq!(timeline.resolve(200).unwrap(), 2);
	}
}
