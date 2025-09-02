use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::error::{Error, Result};
use crate::transaction::Transaction;

/// Entry used for tracking transaction operations in the commit queue
struct CommitEntry {
	writeset: Arc<BTreeMap<Bytes, Option<Bytes>>>,
}

impl CommitEntry {
	/// Returns true if self has no elements in common with other
	pub fn is_disjoint_writeset(&self, other: &Arc<CommitEntry>) -> bool {
		// Create a key iterator for each writeset
		let mut a = self.writeset.keys();
		let mut b = other.writeset.keys();
		// Move to the next value in each iterator
		let mut next_a = a.next();
		let mut next_b = b.next();
		// Advance each iterator independently in order
		while let (Some(ka), Some(kb)) = (next_a, next_b) {
			match ka.cmp(kb) {
				std::cmp::Ordering::Less => next_a = a.next(),
				std::cmp::Ordering::Greater => next_b = b.next(),
				std::cmp::Ordering::Equal => return false,
			}
		}
		// No overlap was found
		true
	}
}

/// Oracle is responsible for managing transaction timestamps and isolation levels.
/// The current implementation uses Snapshot Isolation (SI) for conflict detection
pub(crate) struct Oracle {
	/// Transaction commit queue
	pub(crate) transaction_commit_id: AtomicU64,
	transaction_commit_queue: SkipMap<u64, Arc<CommitEntry>>,

	/// Maximum number of entries to keep in queues
	max_queue_size: usize,

	/// Track active transaction start points
	active_txn_starts: SkipMap<u64, AtomicUsize>,
}

impl Oracle {
	/// Creates a new Oracle with default configuration.
	pub(crate) fn new() -> Self {
		Self {
			transaction_commit_id: AtomicU64::new(0),
			transaction_commit_queue: SkipMap::new(),
			max_queue_size: 10000,
			active_txn_starts: SkipMap::new(),
		}
	}

	/// Gets the current timestamp (can be used for commit timestamp)
	pub(crate) fn now() -> u64 {
		SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64
	}

	/// Register a new transaction start
	pub(crate) fn register_txn_start(&self, start_commit_id: u64) {
		let entry =
			self.active_txn_starts.get_or_insert_with(start_commit_id, || AtomicUsize::new(0));
		entry.value().fetch_add(1, Ordering::Relaxed);
	}

	/// Unregister a transaction when it commits or aborts
	pub(crate) fn unregister_txn_start(&self, start_commit_id: u64) {
		if let Some(entry) = self.active_txn_starts.get(&start_commit_id) {
			let prev_count = entry.value().fetch_sub(1, Ordering::Relaxed);

			// Only remove if we decremented from 1 to 0
			if prev_count == 1 {
				// Double-check by trying to remove only if counter is 0
				if entry.value().load(Ordering::Relaxed) == 0 {
					self.active_txn_starts.remove(&start_commit_id);
				}
			}
		}
	}

	/// Prepares a transaction for commit by checking conflicts and assigning a transaction ID.
	pub(crate) fn prepare_commit(&self, txn: &Transaction) -> Result<u64> {
		// Convert transaction writeset to BTreeMap<Bytes, Option<Bytes>>
		let mut writeset = BTreeMap::new();
		for (key, entry_opt) in &txn.write_set {
			let value = entry_opt.as_ref().and_then(|e| e.value.clone());
			writeset.insert(key.clone(), value);
		}

		// Create commit entry
		let commit_entry = Arc::new(CommitEntry {
			writeset: Arc::new(writeset),
		});

		// Insert into queue with version
		let version = self.atomic_commit(commit_entry.clone())?;

		// Check for conflicts
		let has_conflict = self.check_conflicts(version, txn.start_commit_id, &commit_entry)?;
		if has_conflict {
			self.transaction_commit_queue.remove(&version);
			return Err(Error::TransactionWriteConflict);
		}

		// Clean up old entries
		self.cleanup_queue();

		// No conflicts found, return the tx_id and commit timestamp
		let commit_ts = Self::now();

		Ok(commit_ts)
	}

	/// Atomically insert an entry into the commit queue.
	fn atomic_commit(&self, entry: Arc<CommitEntry>) -> Result<u64> {
		let mut spins = 0;
		loop {
			// Get the next commit version
			let version = self.transaction_commit_id.load(Ordering::Acquire) + 1;

			// Try to insert entry at this version
			let inserted =
				self.transaction_commit_queue.get_or_insert_with(version, || Arc::clone(&entry));

			// Check if entry was inserted
			if Arc::ptr_eq(inserted.value(), &entry) {
				// We successfully inserted entry, increment the commit ID
				// Use Release ordering to ensure all previous operations are visible
				// to other threads before they see the updated commit ID
				self.transaction_commit_id.store(version, Ordering::Release);
				return Ok(version);
			}

			// Contention detected, back off and retry
			spins += 1;
			if spins > 10 {
				std::thread::yield_now();
			}
		}
	}

	/// Check for conflicts with transactions that committed after we started.
	/// Returns true if a conflict was found.
	fn check_conflicts(
		&self,
		version: u64,
		start_commit_id: u64,
		entry: &Arc<CommitEntry>,
	) -> Result<bool> {
		// Check for conflicts with transactions that committed after we started
		for tx in self.transaction_commit_queue.range((start_commit_id + 1)..version) {
			// Check if previous transaction conflicts with writes
			if !tx.value().is_disjoint_writeset(entry) {
				return Ok(true); // Conflict found
			}
		}

		Ok(false) // No conflicts
	}

	/// Clean up the commit queue based on active transactions
	fn cleanup_queue(&self) {
		// Only clean up if we have too many entries
		if self.transaction_commit_queue.len() <= self.max_queue_size {
			return;
		}

		// Find the oldest active transaction start point
		let oldest_active = match self.active_txn_starts.iter().next() {
			Some(entry) => {
				// There are active transactions - we can safely clean up entries
				// older than the oldest active transaction's start point
				*entry.key()
			}
			None => {
				// No active transactions currently.
				// New transactions will get start_commit_id = current transaction_commit_id
				// So we can safely clean up entries older than the current commit ID
				// since no future transaction will have a start_commit_id older than this
				self.transaction_commit_id.load(Ordering::Acquire)
			}
		};

		// Only remove entries that are not needed for conflict detection
		let keys_to_remove: Vec<u64> =
			self.transaction_commit_queue.range(..oldest_active).map(|e| *e.key()).collect();

		for key in keys_to_remove {
			self.transaction_commit_queue.remove(&key);
		}
	}
}
