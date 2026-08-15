//! What branching costs, measured.
//!
//! Two kinds of number, kept apart on purpose.
//!
//! **Counters** are events — a fork happened, a compaction kept a version it
//! would otherwise have dropped. Nothing else in the store remembers that they
//! occurred, so they are incremented where they happen.
//!
//! **Gauges** are current facts — how many branches exist, how far the timeline
//! reaches. Those already have a source of truth, so they are computed when a
//! snapshot is taken rather than stored. A stored copy is a second place for the
//! answer to be wrong, and the cost of asking the real one is a lock and a
//! count.

use std::sync::atomic::{AtomicU64, Ordering};

/// Event counters for branch operations, held by the engine.
#[derive(Debug, Default)]
pub(crate) struct BranchMetrics {
	forks: AtomicU64,
	fork_drain_nanos: AtomicU64,
	detaches: AtomicU64,
	merges: AtomicU64,
	chunked_merges: AtomicU64,
	branches_reclaimed: AtomicU64,
	tables_reclaimed: AtomicU64,
	pin_retained_versions: AtomicU64,
	compaction_pin_races: AtomicU64,
	memtable_flushes: AtomicU64,
}

impl BranchMetrics {
	/// A completed fork, with the time the write fence was held.
	///
	/// The fence is what makes a fork cost something to writers that have
	/// nothing to do with it, so it is the half worth timing. A fork that failed
	/// to drain is not recorded: it published nothing.
	pub(crate) fn record_fork(&self, drain_nanos: u64) {
		self.forks.fetch_add(1, Ordering::Relaxed);
		self.fork_drain_nanos.fetch_add(drain_nanos, Ordering::Relaxed);
	}

	pub(crate) fn record_detach(&self) {
		self.detaches.fetch_add(1, Ordering::Relaxed);
	}

	/// A completed merge and the number of transactions it took. More than one
	/// means it was not atomic, which is the thing worth counting separately.
	pub(crate) fn record_merge(&self, chunks: usize) {
		self.merges.fetch_add(1, Ordering::Relaxed);
		if chunks > 1 {
			self.chunked_merges.fetch_add(1, Ordering::Relaxed);
		}
	}

	/// One maintenance sweep's reclamation: how many tombstoned branches were
	/// released, and how many SST files that freed.
	///
	/// Two numbers because they answer different questions and routinely differ:
	/// a branch deleted before it ever flushed releases no file at all, so a
	/// table count alone cannot tell "the sweep is not running" from "the sweep
	/// had nothing on disk to free".
	pub(crate) fn record_reclamation(&self, branches: u64, tables: u64) {
		if branches > 0 {
			self.branches_reclaimed.fetch_add(branches, Ordering::Relaxed);
		}
		if tables > 0 {
			self.tables_reclaimed.fetch_add(tables, Ordering::Relaxed);
		}
	}

	/// Versions a compaction kept solely because a retention anchor still reads
	/// at or below them — the running cost of every live fork and merge edge
	/// (design §3.3a).
	pub(crate) fn record_pin_retained(&self, versions: u64) {
		if versions > 0 {
			self.pin_retained_versions.fetch_add(versions, Ordering::Relaxed);
		}
	}

	/// A compaction that discarded its own output because the catalog gained an
	/// anchor it had not sampled. Nothing is lost when this happens, but a store
	/// where it happens often is one where forks and compaction are starving
	/// each other.
	pub(crate) fn record_pin_race(&self) {
		self.compaction_pin_races.fetch_add(1, Ordering::Relaxed);
	}

	/// One memtable became an SSTable.
	///
	/// Recorded at the single point where a table is actually produced, so it
	/// counts flushes rather than attempts: the background flush loop wakes on
	/// notification and calls `compact_memtable` whether or not there is
	/// anything to do, and its own pass counter increments either way.
	pub(crate) fn record_memtable_flush(&self) {
		self.memtable_flushes.fetch_add(1, Ordering::Relaxed);
	}

	fn read(&self) -> CountersOnly {
		CountersOnly {
			forks: self.forks.load(Ordering::Relaxed),
			fork_drain_nanos: self.fork_drain_nanos.load(Ordering::Relaxed),
			detaches: self.detaches.load(Ordering::Relaxed),
			merges: self.merges.load(Ordering::Relaxed),
			chunked_merges: self.chunked_merges.load(Ordering::Relaxed),
			branches_reclaimed: self.branches_reclaimed.load(Ordering::Relaxed),
			tables_reclaimed: self.tables_reclaimed.load(Ordering::Relaxed),
			pin_retained_versions: self.pin_retained_versions.load(Ordering::Relaxed),
			compaction_pin_races: self.compaction_pin_races.load(Ordering::Relaxed),
			memtable_flushes: self.memtable_flushes.load(Ordering::Relaxed),
		}
	}
}

/// The counter half of a reading, assembled into [`BranchMetricsSnapshot`] with
/// the gauges alongside it.
struct CountersOnly {
	forks: u64,
	memtable_flushes: u64,
	fork_drain_nanos: u64,
	detaches: u64,
	merges: u64,
	chunked_merges: u64,
	branches_reclaimed: u64,
	tables_reclaimed: u64,
	pin_retained_versions: u64,
	compaction_pin_races: u64,
}

/// A point-in-time reading of what branching is doing and costing.
///
/// The counters are cumulative since the store was opened; they do not survive a
/// reopen, because they describe this process's work rather than the data's
/// history. The gauges describe the store as it is now.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BranchMetricsSnapshot {
	/// Forks that published.
	pub forks: u64,
	/// Total time the fork write fence was held, in nanoseconds. Divided by
	/// `forks` it is the average pause every writer in the store took for
	/// someone else's branch.
	pub fork_drain_nanos: u64,
	/// Branches materialized away from their parent.
	pub detaches: u64,
	/// Merges that completed.
	pub merges: u64,
	/// Of those, the ones that took more than one transaction and so were
	/// resumable rather than atomic.
	pub chunked_merges: u64,
	/// Deleted branches whose resources the maintenance sweep released — a
	/// runtime, its memtables and WAL dependency, or its tables. A branch
	/// deleted without ever having written holds none of those and is not
	/// counted: this measures what was freed, not tombstones walked.
	pub branches_reclaimed: u64,
	/// SST files those releases freed. Lower than `branches_reclaimed` whenever
	/// a branch was deleted before it flushed.
	pub tables_reclaimed: u64,
	/// Versions compaction has kept alive for a retention anchor. The direct
	/// price of keeping forks and merge bases readable.
	pub pin_retained_versions: u64,
	/// Compactions that threw away their output because an anchor appeared
	/// while they were running.
	pub compaction_pin_races: u64,
	/// Branches in the catalog right now, `main` included.
	pub live_branches: u64,
	/// The timestamp range the timeline can still answer `AtTimestamp` inside,
	/// as `(floor, latest)`. `None` before the first commit.
	pub timeline_horizon: Option<(u64, u64)>,
	/// WAL segments that cannot be reclaimed because some memtable still depends
	/// on them.
	pub wal_pinned_segments: usize,
	/// Memtables that have become SSTables since the store was opened.
	///
	/// Monotonic, and incremented only when a table was actually written — which
	/// is what makes it usable as a durability barrier: observe it, trigger a
	/// flush, and wait for it to advance.
	pub memtable_flushes: u64,
}

impl BranchMetricsSnapshot {
	pub(crate) fn assemble(
		metrics: &BranchMetrics,
		live_branches: u64,
		timeline_horizon: Option<(u64, u64)>,
		wal_pinned_segments: usize,
	) -> Self {
		let counters = metrics.read();
		Self {
			forks: counters.forks,
			fork_drain_nanos: counters.fork_drain_nanos,
			detaches: counters.detaches,
			merges: counters.merges,
			chunked_merges: counters.chunked_merges,
			branches_reclaimed: counters.branches_reclaimed,
			tables_reclaimed: counters.tables_reclaimed,
			pin_retained_versions: counters.pin_retained_versions,
			compaction_pin_races: counters.compaction_pin_races,
			memtable_flushes: counters.memtable_flushes,
			live_branches,
			timeline_horizon,
			wal_pinned_segments,
		}
	}
}
