use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_skiplist::SkipMap;

use crate::clock::LogicalClock;
use crate::error::{Error, Result};
use crate::transaction::Transaction;
use crate::Key;

/// Entry used for tracking transaction operations in the commit queue
struct CommitEntry {
	keys: Arc<Vec<Key>>,
}

impl CommitEntry {
	/// Returns true if self has no elements in common with other
	fn is_disjoint_writeset(&self, other: &Arc<CommitEntry>) -> bool {
		if self.keys.is_empty() || other.keys.is_empty() {
			return true;
		}

		let mut a = self.keys.iter();
		let mut b = other.keys.iter();
		let mut next_a = a.next();
		let mut next_b = b.next();
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

/// Oracle is responsible for managing transaction timestamps and isolation
/// levels. The current implementation uses Snapshot Isolation (SI) for conflict
/// detection
pub(crate) struct Oracle {
	/// Transaction commit queue
	pub(crate) transaction_commit_id: Arc<AtomicU64>,
	transaction_commit_queue: Arc<SkipMap<u64, Arc<CommitEntry>>>,

	/// Track active transaction start points
	active_txn_starts: Arc<SkipMap<u64, AtomicUsize>>,

	/// Logical clock for time-based operations
	clock: Arc<dyn LogicalClock>,

	/// Background cleanup thread fields
	cleanup_enabled: Arc<AtomicBool>,
	cleanup_handle: Mutex<Option<JoinHandle<()>>>,
	cleanup_interval: Duration,
}

impl Drop for Oracle {
	fn drop(&mut self) {
		self.shutdown_cleanup();
	}
}

impl Oracle {
	/// Creates a new Oracle with default configuration.
	pub(crate) fn new(clock: Arc<dyn LogicalClock>) -> Self {
		let oracle = Self {
			transaction_commit_id: Arc::new(AtomicU64::new(0)),
			transaction_commit_queue: Arc::new(SkipMap::new()),
			active_txn_starts: Arc::new(SkipMap::new()),
			clock,
			cleanup_enabled: Arc::new(AtomicBool::new(true)),
			cleanup_handle: Mutex::new(None),
			cleanup_interval: Duration::from_secs(2),
		};
		// Start the background cleanup thread
		oracle.spawn_cleanup_thread();
		oracle
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

	/// Prepares a transaction for commit by checking conflicts and assigning a
	/// transaction ID.
	pub(crate) fn prepare_commit(&self, txn: &Transaction) -> Result<u64> {
		// Extract only the keys from the transaction's writeset
		// Keys are already sorted in the BTreeMap, so we maintain sort order
		let keys: Vec<Key> = txn.write_set.keys().cloned().collect();

		// Create commit entry
		let commit_entry = Arc::new(CommitEntry {
			keys: Arc::new(keys),
		});

		// Insert into queue with version
		let version = self.atomic_commit(Arc::clone(&commit_entry))?;

		// Check for conflicts
		let has_conflict = self.check_conflicts(version, txn.start_commit_id, &commit_entry)?;
		if has_conflict {
			self.transaction_commit_queue.remove(&version);
			return Err(Error::TransactionWriteConflict);
		}

		// No conflicts found, return the tx_id and commit timestamp
		// Note: cleanup happens periodically in background thread
		let commit_ts = self.clock.now();

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
			// Ensure the thread backs off when under contention
			if spins > 10 {
				std::hint::spin_loop();
			} else if spins < 100 {
				std::thread::yield_now();
			} else {
				std::thread::park_timeout(Duration::from_micros(10));
			}
			// Increase the number loop spins we have attempted
			spins += 1;
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

	/// Shutdown the cleanup thread, waiting for it to exit
	fn shutdown_cleanup(&self) {
		// Disable cleanup
		self.cleanup_enabled.store(false, Ordering::Release);

		// Wait for the cleanup thread to exit
		if let Some(handle) = self.cleanup_handle.lock().unwrap().take() {
			handle.thread().unpark();
			let _ = handle.join();
		}
	}

	/// Spawn the background cleanup thread
	fn spawn_cleanup_thread(&self) {
		let commit_queue = Arc::clone(&self.transaction_commit_queue);
		let active_txn_starts = Arc::clone(&self.active_txn_starts);
		let transaction_commit_id = Arc::clone(&self.transaction_commit_id);
		let cleanup_enabled = Arc::clone(&self.cleanup_enabled);
		let interval = self.cleanup_interval;

		let handle = std::thread::spawn(move || {
			while cleanup_enabled.load(Ordering::Acquire) {
				// Wait for the specified interval
				std::thread::park_timeout(interval);

				// Find the oldest active transaction start point
				let oldest_active = match active_txn_starts.iter().next() {
					Some(entry) => *entry.key(),
					None => transaction_commit_id.load(Ordering::Acquire),
				};

				// Only remove entries that are not needed for conflict detection
				let keys_to_remove: Vec<u64> =
					commit_queue.range(..oldest_active).map(|e| *e.key()).collect();

				for key in keys_to_remove {
					commit_queue.remove(&key);
				}
			}
		});

		// Store the thread handle
		*self.cleanup_handle.lock().unwrap() = Some(handle);
	}
}
