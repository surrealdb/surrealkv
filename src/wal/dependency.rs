use std::collections::{BTreeMap, BTreeSet};

use parking_lot::Mutex;

/// Process-local identity for a mutable LSM component that depends on WAL.
///
/// The identity is not durable and is rebuilt during recovery. Its only job is
/// to make dependency transfer/removal unambiguous while the process is live.
pub(crate) type ComponentId = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WalDependencySnapshot {
	pub(crate) revision: u64,
	pub(crate) replay_floor: u64,
	pub(crate) in_flight_count: usize,
	pub(crate) component_count: usize,
	pub(crate) pinned_segment_count: usize,
}

#[derive(Default)]
struct DependencyState {
	revision: u64,
	in_flight: BTreeMap<u64, u64>,
	components: BTreeMap<ComponentId, u64>,
}

/// Single authority for live dependencies on WAL segments.
///
/// A commit is pinned before append and atomically handed to the destination
/// memtable after apply. Active-to-immutable rotation keeps the same component
/// identity; flush releases it only after manifest publication succeeds.
#[derive(Default)]
pub(crate) struct WalDependencyTracker {
	state: Mutex<DependencyState>,
}

impl WalDependencyTracker {
	pub(crate) fn new() -> Self {
		Self::default()
	}

	pub(crate) fn pin_in_flight(&self, commit_seq: u64, wal_segment: u64) {
		let mut state = self.state.lock();
		let previous = state.in_flight.insert(commit_seq, wal_segment);
		debug_assert!(previous.is_none(), "commit sequence pinned twice");
		state.revision = state.revision.wrapping_add(1);
	}

	pub(crate) fn cancel_in_flight(&self, commit_seq: u64) {
		let mut state = self.state.lock();
		if state.in_flight.remove(&commit_seq).is_some() {
			state.revision = state.revision.wrapping_add(1);
		}
	}

	/// Atomically transfers a durable commit dependency to the memtable that
	/// now contains it. The component dependency is installed/lowered before
	/// the in-flight pin disappears, under the same lock.
	pub(crate) fn handoff_to_component(
		&self,
		commit_seq: u64,
		component_id: ComponentId,
		wal_segment: u64,
	) {
		let mut state = self.state.lock();
		let pinned = state.in_flight.get(&commit_seq).copied();
		debug_assert_eq!(pinned, Some(wal_segment), "WAL dependency handoff mismatch");
		state
			.components
			.entry(component_id)
			.and_modify(|current| *current = (*current).min(wal_segment))
			.or_insert(wal_segment);
		state.in_flight.remove(&commit_seq);
		state.revision = state.revision.wrapping_add(1);
	}

	/// Registers a component reconstructed by recovery or a component built by
	/// a path that did not pass through live commit apply.
	pub(crate) fn register_component(&self, component_id: ComponentId, wal_segment: u64) {
		let mut state = self.state.lock();
		state
			.components
			.entry(component_id)
			.and_modify(|current| *current = (*current).min(wal_segment))
			.or_insert(wal_segment);
		state.revision = state.revision.wrapping_add(1);
	}

	pub(crate) fn release_component(&self, component_id: ComponentId) {
		let mut state = self.state.lock();
		if state.components.remove(&component_id).is_some() {
			state.revision = state.revision.wrapping_add(1);
		}
	}

	/// Computes the floor that will be valid after `excluded_component` is
	/// durably flushed. The component remains registered until publication
	/// succeeds, so a failed manifest write leaves the current floor intact.
	pub(crate) fn snapshot_excluding(
		&self,
		excluded_component: Option<ComponentId>,
		current_wal_segment: u64,
	) -> WalDependencySnapshot {
		let state = self.state.lock();
		let in_flight_floor = state.in_flight.values().copied().min();
		let component_floor = state
			.components
			.iter()
			.filter(|(id, _)| Some(**id) != excluded_component)
			.map(|(_, segment)| *segment)
			.min();
		let replay_floor =
			in_flight_floor.into_iter().chain(component_floor).fold(current_wal_segment, u64::min);
		let pinned_segment_count = state
			.in_flight
			.values()
			.chain(
				state
					.components
					.iter()
					.filter(|(id, _)| Some(**id) != excluded_component)
					.map(|(_, segment)| segment),
			)
			.copied()
			.collect::<BTreeSet<_>>()
			.len();
		WalDependencySnapshot {
			revision: state.revision,
			replay_floor,
			in_flight_count: state.in_flight.len(),
			component_count: state.components.len(),
			pinned_segment_count,
		}
	}

	pub(crate) fn snapshot(&self, current_wal_segment: u64) -> WalDependencySnapshot {
		self.snapshot_excluding(None, current_wal_segment)
	}

	pub(crate) fn clear(&self) {
		let mut state = self.state.lock();
		state.in_flight.clear();
		state.components.clear();
		state.revision = state.revision.wrapping_add(1);
	}
}

#[cfg(test)]
mod tests {
	use super::WalDependencyTracker;

	#[test]
	fn handoff_never_unpins_the_actual_wal_segment() {
		let tracker = WalDependencyTracker::new();
		tracker.pin_in_flight(11, 3);
		let pinned = tracker.snapshot(9);
		assert_eq!(pinned.replay_floor, 3);
		assert_eq!(pinned.in_flight_count, 1, "fixture must contain an in-flight pin");

		tracker.handoff_to_component(11, 41, 3);
		let handed_off = tracker.snapshot(9);
		assert_eq!(handed_off.replay_floor, 3);
		assert_eq!(handed_off.in_flight_count, 0);
		assert_eq!(handed_off.component_count, 1);

		let after_flush = tracker.snapshot_excluding(Some(41), 9);
		assert_eq!(after_flush.replay_floor, 9);
		tracker.release_component(41);
		assert_eq!(tracker.snapshot(9).replay_floor, 9);
	}

	#[test]
	fn another_dependency_survives_partial_component_flush() {
		let tracker = WalDependencyTracker::new();
		tracker.register_component(1, 5);
		tracker.register_component(2, 5);
		let excluding_first = tracker.snapshot_excluding(Some(1), 8);
		assert_eq!(excluding_first.component_count, 2, "fixture must contain both components");
		assert_eq!(excluding_first.replay_floor, 5);
	}
}
