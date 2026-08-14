//! Merge planning and conflict detection.
//!
//! Planning is a pure read. It produces the full list of what would be applied
//! and what conflicts, and it does so *before* anything is mutated — a merge
//! that would conflict must be refusable without having already written half of
//! itself.
//!
//! The comparison is three-way, per key the source changed:
//!
//! | base (target at the fork) | target now | source | outcome |
//! |---|---|---|---|
//! | any | same version as base | anything | apply |
//! | any | same *value* as base | anything | apply (target moved and came back) |
//! | any | same value as source | — | no-op (both sides converged) |
//! | any | anything else | anything | conflict |
//!
//! Absence counts as a value throughout: that is what makes delete-vs-delete
//! resolve, and delete-vs-modify conflict.
//!
//! The first row is a fast path on sequence alone, so planning a merge into a
//! target nobody touched reads no values at all.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::batch::BatchOwner;
use crate::error::{Error, Result};
use crate::lsm::Core;
use crate::snapshot::{Snapshot, VersionMeta};
use crate::transaction::{Transaction, TransactionOptions};
use crate::{DiffEntry, Value};

/// How a conflicting key differs. Reported so a caller can decide without
/// re-reading the store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConflictKind {
	/// Both sides wrote different values.
	BothModified,
	/// The source deleted the key; the target changed it.
	DeletedBySourceModifiedByTarget,
	/// The source changed the key; the target deleted it.
	ModifiedBySourceDeletedByTarget,
}

/// One key that cannot be merged without a decision.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conflict {
	pub key: Vec<u8>,
	pub kind: ConflictKind,
	/// Value at the fork point. `None` means the key did not exist there.
	pub base: Option<Value>,
	/// The source's value. `None` means the source deleted it.
	pub source: Option<Value>,
	/// The target's current value. `None` means the target deleted it.
	pub target: Option<Value>,
}

/// How a merge treats keys both sides changed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MergeStrategy {
	/// Refuse the whole merge if anything conflicts, listing every conflict
	/// first. Nothing is written. The default, because the alternative to
	/// knowing is guessing.
	#[default]
	Strict,
	/// Apply the source's value to every conflicting key. Chosen deliberately,
	/// per merge — it discards the target's change on those keys.
	SourceWins,
}

/// What a merge did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeOutcome {
	/// Keys written to the target.
	pub applied: usize,
	/// Keys skipped because the target already had the source's value.
	pub converged: usize,
	/// Conflicting keys resolved by the strategy. Zero under `Strict`, which
	/// refuses instead.
	pub resolved: usize,
	/// How many transactions the merge took. One means it landed atomically;
	/// more means each chunk is durable on its own and a failure part-way would
	/// have left the earlier ones applied.
	pub chunks: usize,
	/// The target's visible head after the merge, and the source head the merge
	/// consumed. Recorded on the target so a later merge starts from here.
	pub source_through_seq: u64,
	pub target_through_seq: u64,
}

/// What a merge would do, computed without mutating anything.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MergeReport {
	/// Changes that apply cleanly, in key order.
	pub applies: Vec<DiffEntry>,
	/// Changes that need a decision, in key order.
	pub conflicts: Vec<Conflict>,
	/// Source changes the target already has. Counted, not listed: they are
	/// evidence the merge is partly redundant, not work to do.
	pub converged: usize,
}

impl MergeReport {
	/// Whether the merge can proceed without a decision.
	pub fn is_clean(&self) -> bool {
		self.conflicts.is_empty()
	}

	/// Number of keys the merge would write.
	pub fn apply_count(&self) -> usize {
		self.applies.len()
	}
}

/// Answers, for one key, the two things the decision table needs to know about
/// the target: whether it moved since the base, and what it holds.
///
/// Two implementations, one question. [`PointProbe`] asks the read stack per key;
/// [`ScanProbe`] walks the target's own changes once and answers from the walk.
/// Which is cheaper depends on the size of the merge, so the conflict rules must
/// give the same verdicts either way — that equivalence is a test, not a hope
/// (plan A7).
///
/// **Keys must be offered in ascending order.** [`ScanProbe`] is a forward-only
/// cursor and refuses to answer out of order rather than answering wrongly. The
/// merge's passes read a key-ordered diff, so this costs the callers nothing.
pub(crate) trait TargetProbe {
	/// Whether the target's newest version of this key is newer than the base —
	/// that is, whether the target has changed it since.
	fn moved_since_base(&mut self, key: &[u8]) -> Result<bool>;
	/// Value at the base, `None` if absent or deleted there.
	fn base_value(&mut self, key: &[u8]) -> Result<Option<Value>>;
	/// Value now, `None` if absent or deleted.
	fn target_value(&mut self, key: &[u8]) -> Result<Option<Value>>;
}

/// Per-key point lookups against the target's read stack, at two caps.
///
/// Two metadata reads per key to answer `moved_since_base`, which is cheap when
/// the merge is small and is the reason [`ScanProbe`] exists when it is not.
pub(crate) struct PointProbe<'a> {
	at_base: &'a Snapshot,
	now: &'a Snapshot,
}

impl<'a> PointProbe<'a> {
	pub(crate) fn new(at_base: &'a Snapshot, now: &'a Snapshot) -> Self {
		Self {
			at_base,
			now,
		}
	}

	fn meta(snapshot: &Snapshot, key: &[u8]) -> Result<Option<VersionMeta>> {
		snapshot.get_latest_meta(key)
	}
}

impl TargetProbe for PointProbe<'_> {
	fn moved_since_base(&mut self, key: &[u8]) -> Result<bool> {
		let base = Self::meta(self.at_base, key)?;
		let now = Self::meta(self.now, key)?;
		Ok(base.map(|meta| meta.seq) != now.map(|meta| meta.seq))
	}

	fn base_value(&mut self, key: &[u8]) -> Result<Option<Value>> {
		Ok(self.at_base.get(key)?.map(|(value, _)| value))
	}

	fn target_value(&mut self, key: &[u8]) -> Result<Option<Value>> {
		Ok(self.now.get(key)?.map(|(value, _)| value))
	}
}

/// Answers from one forward walk of the target's own changes above the base.
///
/// "Has the target moved" and "is this key in the target's diff above the base"
/// are the same question: a key the target has not written above the base still
/// has, as its newest version, the one the base sees. So the walk answers
/// `moved_since_base` outright, and hands over the target's current value for
/// free when it moved — the read the point probe would have had to make.
///
/// Inherited rows cannot appear in that walk and do not need to. A fork child's
/// view of its ancestors is frozen at its own anchor, which is at or below any
/// base a merge into it can use, so an inherited row's sequence is always below
/// the filter. Detach materialises those rows into the branch's own tables with
/// their original sequences, which the same filter excludes.
pub(crate) struct ScanProbe<'a> {
	changes: crate::DiffIter<'a>,
	at_base: &'a Snapshot,
	now: &'a Snapshot,
	/// The cursor's current entry, or `None` once the walk is done.
	current: Option<DiffEntry>,
	/// The last key answered, so an out-of-order call is caught rather than
	/// silently answered from a cursor that has already passed it.
	last_asked: Option<Vec<u8>>,
	/// Whether `current` is the key of the most recent `moved_since_base`, so
	/// `target_value` can be answered without a read.
	current_is_asked: bool,
}

impl<'a> ScanProbe<'a> {
	pub(crate) fn new(
		changes: crate::DiffIter<'a>,
		at_base: &'a Snapshot,
		now: &'a Snapshot,
	) -> Result<Self> {
		let mut probe = Self {
			changes,
			at_base,
			now,
			current: None,
			last_asked: None,
			current_is_asked: false,
		};
		probe.step()?;
		Ok(probe)
	}

	fn step(&mut self) -> Result<()> {
		self.current = self.changes.next().transpose()?;
		Ok(())
	}
}

impl TargetProbe for ScanProbe<'_> {
	fn moved_since_base(&mut self, key: &[u8]) -> Result<bool> {
		if let Some(last) = &self.last_asked {
			if key <= last.as_slice() {
				return Err(Error::InvalidArgument(format!(
					"a scan probe was asked about {key:?} after {last:?}; it walks forward only"
				)));
			}
		}
		self.last_asked = Some(key.to_vec());
		while self.current.as_ref().is_some_and(|entry| entry.key.as_slice() < key) {
			self.step()?;
		}
		self.current_is_asked =
			self.current.as_ref().is_some_and(|entry| entry.key.as_slice() == key);
		Ok(self.current_is_asked)
	}

	fn base_value(&mut self, key: &[u8]) -> Result<Option<Value>> {
		Ok(self.at_base.get(key)?.map(|(value, _)| value))
	}

	fn target_value(&mut self, key: &[u8]) -> Result<Option<Value>> {
		// Only reachable for a key that moved, whose entry the cursor is
		// therefore sitting on — the walk already read the value.
		if self.current_is_asked {
			if let Some(entry) = &self.current {
				debug_assert_eq!(entry.key.as_slice(), key);
				return Ok(entry.op.value().cloned());
			}
		}
		Ok(self.now.get(key)?.map(|(value, _)| value))
	}
}

/// Builds the report for one source change.
///
/// Split out so the decision table is one function with no I/O interleaved into
/// it: everything it needs is fetched through the probe, and the order of the
/// rules is the order of the table in this module's documentation.
fn classify(entry: &DiffEntry, probe: &mut dyn TargetProbe) -> Result<Decision> {
	// Fast path: the target's newest version is the one it had at the base, so
	// it has not moved. No value is read.
	if !probe.moved_since_base(&entry.key)? {
		return Ok(Decision::Apply);
	}

	let base_value = probe.base_value(&entry.key)?;
	let target_value = probe.target_value(&entry.key)?;

	// The target moved and came back: in content it is unchanged, so the
	// source's change still applies cleanly.
	if base_value == target_value {
		return Ok(Decision::Apply);
	}

	// Both sides reached the same place, including both having deleted it.
	let source_value = entry.op.value().cloned();
	if source_value == target_value {
		return Ok(Decision::Converged);
	}

	let source_deleted = entry.op.is_delete();
	let target_deleted = target_value.is_none();
	let kind = match (source_deleted, target_deleted) {
		(true, false) => ConflictKind::DeletedBySourceModifiedByTarget,
		(false, true) => ConflictKind::ModifiedBySourceDeletedByTarget,
		// (true, true) is unreachable: both deleted means both values are
		// `None`, which converged above.
		_ => ConflictKind::BothModified,
	};
	Ok(Decision::Conflict(Conflict {
		key: entry.key.clone(),
		kind,
		base: base_value,
		source: source_value,
		target: target_value,
	}))
}

enum Decision {
	Apply,
	Converged,
	Conflict(Conflict),
}

/// Plans a merge: classifies every source change against the target.
///
/// Reads only. The caller decides what to do with the report.
pub(crate) fn plan(
	changes: impl Iterator<Item = Result<DiffEntry>>,
	probe: &mut dyn TargetProbe,
) -> Result<MergeReport> {
	let mut report = MergeReport::default();
	for entry in changes {
		let entry = entry?;
		match classify(&entry, &mut *probe)? {
			Decision::Apply => report.applies.push(entry),
			Decision::Converged => report.converged += 1,
			Decision::Conflict(conflict) => report.conflicts.push(conflict),
		}
	}
	Ok(report)
}

/// One merge, from planning to its last chunk.
///
/// Everything it compares against is fixed when the session is built: one
/// snapshot of the source's changes and one pair of target snapshots. Both
/// passes below read those, never the live store, so a write arriving mid-merge
/// cannot make the second pass disagree with the first about what conflicts.
///
/// The two passes are the price of `Strict` meaning what it says. Conflicts have
/// to be known before anything is written, and a merge too big to hold in memory
/// cannot be classified and buffered in one go — so pass one measures and
/// classifies without buffering, and pass two re-streams and writes.
pub(crate) struct MergeSession {
	core: Arc<Core>,
	target: BatchOwner,
	/// The source's changes above the base.
	diff: crate::BranchDiff,
	/// The target's own changes above the base — what [`ScanProbe`] walks.
	/// Built with the session so both passes see the same fixed set.
	target_changes: crate::BranchDiff,
	at_base: Snapshot,
	now: Snapshot,
	/// The sequence every chunk commits at. See
	/// [`Transaction::new_owned_at`]: it is what makes a target write arriving
	/// after planning a conflict instead of an overwrite.
	start_seq: u64,
	/// Holds the oracle's GC watermark at `start_seq` for the whole merge, so a
	/// gap between chunks cannot let it advance past what the chunks still have
	/// to be judged against.
	_watermark: crate::tracker::ActiveTxnGuard,
	/// Bytes of key plus value one chunk may carry.
	chunk_budget: u64,
}

/// What pass one found, and what pass two needs to know before it writes.
#[derive(Debug, Default)]
pub(crate) struct MergePreflight {
	/// Keys that would be written, including conflicts a strategy resolves.
	pub(crate) writes: usize,
	/// Keys the target already agrees with.
	pub(crate) converged: usize,
	/// Conflicts, up to [`CONFLICT_REPORT_LIMIT`]. `conflicts` is the true
	/// count; this may be shorter.
	pub(crate) sample: Vec<Conflict>,
	pub(crate) conflicts: usize,
	/// Total key + value bytes of everything that would be written.
	pub(crate) bytes: u64,
	/// The largest single entry, which is what decides whether the merge is
	/// chunkable at all.
	pub(crate) largest_entry: u64,
}

impl MergePreflight {
	/// Source changes looked at, whatever the verdict. What pass two sizes
	/// itself against.
	pub(crate) fn examined(&self) -> usize {
		self.writes + self.converged + self.conflicts
	}
}

/// Source changes above which pass two walks the target instead of probing it
/// per key.
///
/// Below this, the point probe's two metadata reads per key beat the fixed cost
/// of opening an iterator over the target's changes; above it, one forward walk
/// beats thousands of random lookups. The exact crossover is hardware, not
/// arithmetic — what matters is that both modes give the same verdicts, which is
/// tested, so a wrong threshold costs time and never correctness.
pub(crate) const SCAN_PROBE_THRESHOLD: usize = 256;

/// How many conflicts a refusal carries back. A merge that conflicts on a
/// million keys does not need a million of them enumerated to be actionable, and
/// holding them all is the memory problem this slice exists to remove.
pub(crate) const CONFLICT_REPORT_LIMIT: usize = 1024;

impl MergeSession {
	pub(crate) fn new(
		core: Arc<Core>,
		source: BatchOwner,
		target: BatchOwner,
		base: EffectiveBase,
		visible: u64,
	) -> Result<Self> {
		let watermark = core.active_txn_tracker.register(visible);
		let own = crate::snapshot::Snapshot::own_only(Arc::clone(&core), visible, source)?;
		let target_own = crate::snapshot::Snapshot::own_only(Arc::clone(&core), visible, target)?;
		let at_base =
			crate::snapshot::Snapshot::new_owned(Arc::clone(&core), base.target_at, target)?;
		let now = crate::snapshot::Snapshot::new_owned(Arc::clone(&core), visible, target)?;
		let chunk_budget = core.inner.opts.max_memtable_size as u64;
		Ok(Self {
			core,
			target,
			diff: crate::BranchDiff::new(own, base.source_through),
			target_changes: crate::BranchDiff::new(target_own, base.target_at),
			at_base,
			now,
			start_seq: visible,
			_watermark: watermark,
			chunk_budget,
		})
	}

	/// The source head this merge consumes. Recorded on the target so a later
	/// merge starts here rather than re-offering what was already applied.
	pub(crate) fn source_through_seq(&self) -> u64 {
		self.start_seq
	}

	/// The source's changes, in key order — what both passes classify.
	pub(crate) fn changes(&self) -> Result<crate::DiffIter<'_>> {
		self.diff.iter()
	}

	pub(crate) fn point_probe(&self) -> PointProbe<'_> {
		PointProbe::new(&self.at_base, &self.now)
	}

	pub(crate) fn scan_probe(&self) -> Result<ScanProbe<'_>> {
		ScanProbe::new(self.target_changes.iter()?, &self.at_base, &self.now)
	}

	/// Pass one: classify everything, buffer nothing.
	///
	/// Refuses before pass two ever runs when a single entry cannot fit in a
	/// chunk — one key cannot be split across two batches, so that is the one
	/// size problem chunking does not solve.
	pub(crate) fn preflight(&self, strategy: MergeStrategy) -> Result<MergePreflight> {
		// Pass one always points: it is the pass that discovers how big the
		// merge is, so it cannot choose by size without begging the question.
		let mut probe = PointProbe::new(&self.at_base, &self.now);
		let mut preflight = MergePreflight::default();
		for entry in self.changes()? {
			let entry = entry?;
			let size = (entry.key.len() + entry.op.value().map_or(0, |value| value.len())) as u64;
			match classify(&entry, &mut probe)? {
				Decision::Apply => {}
				Decision::Converged => {
					preflight.converged += 1;
					continue;
				}
				Decision::Conflict(conflict) => {
					preflight.conflicts += 1;
					if preflight.sample.len() < CONFLICT_REPORT_LIMIT {
						preflight.sample.push(conflict);
					}
					if strategy == MergeStrategy::Strict {
						// Nothing will be written, so measuring the rest is
						// work for an answer no one receives.
						continue;
					}
				}
			}
			preflight.writes += 1;
			preflight.bytes += size;
			preflight.largest_entry = preflight.largest_entry.max(size);
		}
		if preflight.largest_entry > self.chunk_budget {
			return Err(Error::MergeTooLarge {
				estimated_bytes: preflight.largest_entry,
				budget_bytes: self.chunk_budget,
			});
		}
		Ok(preflight)
	}

	/// Pass two: re-stream the same fixed view and write it, in as few chunks as
	/// the budget allows.
	///
	/// Returns the target's visible head afterwards and how many transactions it
	/// took. A merge that fits in one chunk is one transaction and lands
	/// atomically; above that, each chunk is durable on its own and a failure
	/// part-way leaves the earlier ones applied.
	pub(crate) async fn apply(
		&self,
		strategy: MergeStrategy,
		preflight: &MergePreflight,
	) -> Result<(u64, usize)> {
		let mut scan;
		let mut point;
		let probe: &mut dyn TargetProbe = if preflight.examined() >= SCAN_PROBE_THRESHOLD {
			scan = self.scan_probe()?;
			&mut scan
		} else {
			point = self.point_probe();
			&mut point
		};
		let mut chunk: Vec<(Vec<u8>, Option<Value>)> = Vec::new();
		let mut chunk_bytes = 0u64;
		let mut chunks = 0usize;
		for entry in self.changes()? {
			let entry = entry?;
			let value = match classify(&entry, probe)? {
				Decision::Apply => entry.op.value().cloned(),
				Decision::Converged => continue,
				// Under `Strict` this is unreachable: `preflight` refused the
				// merge before pass two began. Under `SourceWins` the source's
				// value is what resolves it.
				Decision::Conflict(conflict) => {
					if strategy == MergeStrategy::Strict {
						return Err(Error::MergeConflicts {
							count: 1,
						});
					}
					conflict.source
				}
			};
			let size = (entry.key.len() + value.as_ref().map_or(0, |value| value.len())) as u64;
			if !chunk.is_empty() && chunk_bytes + size > self.chunk_budget {
				self.commit_chunk(std::mem::take(&mut chunk)).await?;
				chunk_bytes = 0;
				chunks += 1;
			}
			chunk.push((entry.key, value));
			chunk_bytes += size;
		}
		if !chunk.is_empty() {
			self.commit_chunk(chunk).await?;
			chunks += 1;
		}
		Ok((self.core.inner.visible_seq_num.load(Ordering::Acquire), chunks))
	}

	async fn commit_chunk(&self, chunk: Vec<(Vec<u8>, Option<Value>)>) -> Result<()> {
		let mut txn = Transaction::new_owned_at(
			Arc::clone(&self.core),
			TransactionOptions::new(),
			self.target,
			self.start_seq,
		)?;
		for (key, value) in chunk {
			match value {
				Some(value) => txn.set(key.as_slice(), value.as_slice())?,
				None => txn.delete(key.as_slice())?,
			}
		}
		txn.commit().await
	}
}

/// Where a merge measures from: what the source still has to offer, and where
/// the target's side of the three-way comparison is read.
///
/// Before the first merge both are the fork anchor. Afterwards they move to the
/// recorded edge, because the last state the two branches shared is what was
/// merged, not what was forked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct EffectiveBase {
	/// Source changes at or below this sequence have already been merged.
	pub(crate) source_through: u64,
	/// Cap at which the target is read for the base side of the comparison.
	pub(crate) target_at: u64,
}

/// Resolves the base for merging `source` into `target`, given the fork anchor.
pub(crate) fn effective_base(
	catalog: &crate::branch::BranchCatalog,
	source: BatchOwner,
	target: BatchOwner,
	fork_seq: u64,
) -> EffectiveBase {
	match catalog.merge_edge(target.branch, source.branch) {
		// A stale generation's edge is not this branch's history: a source that
		// was deleted and recreated under the same id shares nothing with its
		// predecessor, so the fork anchor is the only honest base.
		Some(edge) if edge.source_generation == source.generation => EffectiveBase {
			source_through: edge.source_through_seq.max(fork_seq),
			target_at: edge.target_through_seq.max(fork_seq),
		},
		_ => EffectiveBase {
			source_through: fork_seq,
			target_at: fork_seq,
		},
	}
}

/// Checks that `source` may be merged into `target` at all, returning the fork
/// anchor the merge is based on.
///
/// One refusal, structural: a merge is only accepted into the branch the source
/// was forked from. Sibling-to-sibling has no common base recorded anywhere, and
/// inventing one would be a guess.
///
/// Whether the base is still *readable* is a separate question, asked of the
/// cap the merge actually reads at rather than of this anchor — see
/// `BranchHandle::merge_preconditions`.
pub(crate) fn validate_lineage(
	catalog: &crate::branch::BranchCatalog,
	source: BatchOwner,
	target: BatchOwner,
) -> Result<u64> {
	let record = catalog
		.validate_owner(source.branch, source.generation)
		.map_err(|_| Error::BranchFenced)?;
	let link = record.parent.as_ref().ok_or_else(|| Error::BranchesUnrelated {
		reason: format!("branch {:?} was not forked from anything", record.name),
	})?;
	if link.parent != target.branch || link.parent_generation != target.generation {
		return Err(Error::BranchesUnrelated {
			reason: format!(
				"branch {:?} was forked from a different branch; a merge is only accepted into its own parent",
				record.name
			),
		});
	}
	Ok(link.fork_seq)
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use super::*;
	use crate::{DiffOp, InternalKeyKind};

	/// A probe backed by literal maps, so the decision table can be exercised
	/// without a store. Every case below is a row of that table.
	#[derive(Clone, Default)]
	struct FakeProbe {
		base: HashMap<Vec<u8>, (u64, Option<Value>)>,
		now: HashMap<Vec<u8>, (u64, Option<Value>)>,
	}

	impl FakeProbe {
		fn meta(entry: Option<&(u64, Option<Value>)>) -> Option<VersionMeta> {
			entry.map(|(seq, value)| VersionMeta {
				seq: *seq,
				kind: if value.is_some() {
					InternalKeyKind::Set
				} else {
					InternalKeyKind::Delete
				},
			})
		}
	}

	impl TargetProbe for FakeProbe {
		fn moved_since_base(&mut self, key: &[u8]) -> Result<bool> {
			let base = Self::meta(self.base.get(key));
			let now = Self::meta(self.now.get(key));
			Ok(base.map(|meta| meta.seq) != now.map(|meta| meta.seq))
		}

		fn base_value(&mut self, key: &[u8]) -> Result<Option<Value>> {
			Ok(self.base.get(key).and_then(|(_, value)| value.clone()))
		}

		fn target_value(&mut self, key: &[u8]) -> Result<Option<Value>> {
			Ok(self.now.get(key).and_then(|(_, value)| value.clone()))
		}
	}

	fn set(key: &str, value: &str, seq: u64) -> DiffEntry {
		DiffEntry {
			key: key.as_bytes().to_vec(),
			op: DiffOp::Set(value.as_bytes().to_vec()),
			seq,
			timestamp: 0,
		}
	}

	fn delete(key: &str, seq: u64) -> DiffEntry {
		DiffEntry {
			key: key.as_bytes().to_vec(),
			op: DiffOp::Delete,
			seq,
			timestamp: 0,
		}
	}

	fn value(text: &str) -> Option<Value> {
		Some(text.as_bytes().to_vec())
	}

	fn decide(entry: DiffEntry, probe: &FakeProbe) -> Decision {
		classify(&entry, &mut probe.clone()).unwrap()
	}

	#[test]
	fn an_untouched_target_applies_without_reading_values() {
		let mut probe = FakeProbe::default();
		probe.base.insert(b"k".to_vec(), (10, value("base")));
		probe.now.insert(b"k".to_vec(), (10, value("base")));
		assert!(matches!(decide(set("k", "source", 20), &probe), Decision::Apply));

		// A key neither side ever had is also "unchanged".
		assert!(matches!(decide(set("fresh", "source", 21), &probe), Decision::Apply));
	}

	#[test]
	fn a_target_that_moved_and_came_back_still_applies() {
		let mut probe = FakeProbe::default();
		probe.base.insert(b"k".to_vec(), (10, value("base")));
		// Different sequence, same content: the target rewrote the same value.
		probe.now.insert(b"k".to_vec(), (30, value("base")));
		assert!(
			matches!(decide(set("k", "source", 20), &probe), Decision::Apply),
			"a revert to the base value must not manufacture a conflict"
		);
	}

	#[test]
	fn converged_sides_are_a_no_op_including_both_deleting() {
		let mut probe = FakeProbe::default();
		probe.base.insert(b"same".to_vec(), (10, value("base")));
		probe.now.insert(b"same".to_vec(), (30, value("agreed")));
		assert!(matches!(decide(set("same", "agreed", 20), &probe), Decision::Converged));

		probe.base.insert(b"gone".to_vec(), (10, value("base")));
		probe.now.insert(b"gone".to_vec(), (30, None));
		assert!(
			matches!(decide(delete("gone", 20), &probe), Decision::Converged),
			"delete on both sides is agreement, not a conflict"
		);
	}

	#[test]
	fn divergent_sides_conflict_and_report_all_three_values() {
		let mut probe = FakeProbe::default();
		probe.base.insert(b"k".to_vec(), (10, value("base")));
		probe.now.insert(b"k".to_vec(), (30, value("target")));
		let Decision::Conflict(conflict) = decide(set("k", "source", 20), &probe) else {
			panic!("divergent values must conflict");
		};
		assert_eq!(conflict.kind, ConflictKind::BothModified);
		assert_eq!(conflict.base, value("base"));
		assert_eq!(conflict.source, value("source"));
		assert_eq!(conflict.target, value("target"));
	}

	#[test]
	fn delete_against_modify_conflicts_in_both_directions() {
		let mut probe = FakeProbe::default();
		probe.base.insert(b"k".to_vec(), (10, value("base")));
		probe.now.insert(b"k".to_vec(), (30, value("target")));
		let Decision::Conflict(conflict) = decide(delete("k", 20), &probe) else {
			panic!("source delete against a modified target must conflict");
		};
		assert_eq!(conflict.kind, ConflictKind::DeletedBySourceModifiedByTarget);
		assert_eq!(conflict.source, None);

		let mut probe = FakeProbe::default();
		probe.base.insert(b"k".to_vec(), (10, value("base")));
		probe.now.insert(b"k".to_vec(), (30, None));
		let Decision::Conflict(conflict) = decide(set("k", "source", 20), &probe) else {
			panic!("source modify against a deleted target must conflict");
		};
		assert_eq!(conflict.kind, ConflictKind::ModifiedBySourceDeletedByTarget);
		assert_eq!(conflict.target, None);
	}

	#[test]
	fn a_key_the_target_created_after_the_fork_conflicts() {
		let mut probe = FakeProbe::default();
		// Absent at the fork, present now: the target created it independently.
		probe.now.insert(b"k".to_vec(), (30, value("target")));
		let Decision::Conflict(conflict) = decide(set("k", "source", 20), &probe) else {
			panic!("independent creation on both sides must conflict");
		};
		assert_eq!(conflict.base, None);
		assert_eq!(conflict.kind, ConflictKind::BothModified);
	}

	#[test]
	fn a_plan_reports_every_class_in_key_order() {
		let mut probe = FakeProbe::default();
		probe.base.insert(b"clean".to_vec(), (10, value("base")));
		probe.now.insert(b"clean".to_vec(), (10, value("base")));
		probe.base.insert(b"conflicted".to_vec(), (10, value("base")));
		probe.now.insert(b"conflicted".to_vec(), (30, value("target")));
		probe.base.insert(b"converged".to_vec(), (10, value("base")));
		probe.now.insert(b"converged".to_vec(), (30, value("agreed")));

		let changes = vec![
			Ok(set("clean", "source", 20)),
			Ok(set("conflicted", "source", 21)),
			Ok(set("converged", "agreed", 22)),
		];
		let report = plan(changes.into_iter(), &mut probe).unwrap();
		assert_eq!(report.applies.len(), 1);
		assert_eq!(report.applies[0].key, b"clean".to_vec());
		assert_eq!(report.conflicts.len(), 1);
		assert_eq!(report.conflicts[0].key, b"conflicted".to_vec());
		assert_eq!(report.converged, 1);
		assert!(!report.is_clean());
		assert_eq!(report.apply_count(), 1);
	}
}
