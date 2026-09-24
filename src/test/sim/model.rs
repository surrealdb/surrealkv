//! The reference model oracle (`ModelDb`).
//!
//! Maintains a simple, trivially correct in-memory state using `BTreeMap`.
//! Used for differential testing against SurrealKV: every read, range scan,
//! and post-crash recovery is checked against `ModelDb` for 100% byte-for-byte equivalence.

use crate::Key;
use std::collections::BTreeMap;
use std::ops::Bound;

/// A simple, unoptimized in-memory reference model of a Key-Value store.
#[derive(Debug, Default, Clone)]
pub struct ModelDb {
	/// The committed key-value pairs (ground truth).
	data: BTreeMap<Key, Vec<u8>>,
	/// Monotonically increasing commit sequence / version counter.
	version: u64,
	/// History of committed key modifications by version: (version, modified_keys)
	commit_log: Vec<(u64, Vec<Key>)>,
	/// Versioned snapshot history for snapshot isolation reads
	history: BTreeMap<u64, BTreeMap<Key, Vec<u8>>>,
}

impl ModelDb {
	/// Creates a new empty reference model.
	pub fn new() -> Self {
		let mut history = BTreeMap::new();
		history.insert(0, BTreeMap::new());
		Self {
			data: BTreeMap::new(),
			version: 0,
			commit_log: Vec::new(),
			history,
		}
	}

	/// Retrieves the value for a key from the committed state.
	pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
		self.data.get(key).cloned()
	}

	/// Retrieves the value for a key as of a specific historical version.
	pub fn get_at_version(&self, key: &[u8], version: u64) -> Option<Vec<u8>> {
		self.history.get(&version).and_then(|map| map.get(key).cloned())
	}

	/// Performs a range scan over the committed state.
	pub fn range<'a>(
		&'a self,
		start: Bound<&'a [u8]>,
		end: Bound<&'a [u8]>,
	) -> Vec<(Key, Vec<u8>)> {
		let map_start = match start {
			Bound::Included(k) => Bound::Included(k.to_vec()),
			Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
			Bound::Unbounded => Bound::Unbounded,
		};
		let map_end = match end {
			Bound::Included(k) => Bound::Included(k.to_vec()),
			Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
			Bound::Unbounded => Bound::Unbounded,
		};

		self.data.range((map_start, map_end)).map(|(k, v)| (k.clone(), v.clone())).collect()
	}

	/// Begins a model transaction at the current version.
	pub fn begin(&self) -> ModelTxn {
		ModelTxn {
			start_version: self.version,
			ops: Vec::new(),
			savepoints: Vec::new(),
		}
	}

	/// Attempts to commit a model transaction using first-committer-wins conflict detection.
	pub fn commit(&mut self, txn: ModelTxn) -> Result<(), ()> {
		if txn.ops.is_empty() {
			return Ok(());
		}

		// Conflict check: ensure none of the write keys were modified since start_version
		for &(v, ref keys) in &self.commit_log {
			if v > txn.start_version {
				for modified_key in keys {
					for (op_key, _) in &txn.ops {
						if op_key == modified_key {
							return Err(()); // Write-write conflict!
						}
					}
				}
			}
		}

		// Apply writes in exact sequential statement order
		let mut modified_keys = Vec::with_capacity(txn.ops.len());
		for (k, op) in txn.ops {
			modified_keys.push(k.clone());
			match op {
				ModelOp::Put(v) => {
					self.data.insert(k, v);
				}
				ModelOp::Delete => {
					self.data.remove(&k);
				}
				ModelOp::DeleteRange(end_key) => {
					let keys_to_remove: Vec<Key> = self
						.data
						.range((Bound::Included(k), Bound::Excluded(end_key)))
						.map(|(key, _)| key.clone())
						.collect();
					for rk in keys_to_remove {
						self.data.remove(&rk);
					}
				}
			}
		}

		self.version += 1;
		self.commit_log.push((self.version, modified_keys));
		self.history.insert(self.version, self.data.clone());
		Ok(())
	}
}

/// Operation performed within a model transaction.
#[derive(Debug, Clone)]
pub enum ModelOp {
	Put(Vec<u8>),
	Delete,
	DeleteRange(Key),
}

/// A transaction on the reference model.
pub struct ModelTxn {
	pub start_version: u64,
	pub ops: Vec<(Key, ModelOp)>,
	pub savepoints: Vec<usize>,
}

impl ModelTxn {
	pub fn set(&mut self, key: &[u8], value: &[u8]) {
		self.ops.push((key.to_vec(), ModelOp::Put(value.to_vec())));
	}

	pub fn delete(&mut self, key: &[u8]) {
		self.ops.push((key.to_vec(), ModelOp::Delete));
	}

	pub fn delete_range(&mut self, start: &[u8], end: &[u8]) {
		if start < end {
			self.ops.push((start.to_vec(), ModelOp::DeleteRange(end.to_vec())));
		}
	}

	/// Read-Your-Own-Writes: reads within uncommitted transaction, falling back to committed state.
	pub fn get(&self, key: &[u8], committed: &ModelDb) -> Option<Vec<u8>> {
		for (k, op) in self.ops.iter().rev() {
			if k == key {
				return match op {
					ModelOp::Put(v) => Some(v.clone()),
					ModelOp::Delete => None,
					ModelOp::DeleteRange(_) => None,
				};
			}
			if let ModelOp::DeleteRange(end) = op {
				if key >= k.as_slice() && key < end.as_slice() {
					return None;
				}
			}
		}
		committed.get_at_version(key, self.start_version)
	}

	/// Pushes a savepoint and returns the savepoint number.
	pub fn set_savepoint(&mut self) -> u32 {
		let sp = self.savepoints.len() as u32;
		self.savepoints.push(self.ops.len());
		sp
	}

	/// Rolls back operations to the latest savepoint.
	pub fn rollback_to_savepoint(&mut self) {
		if let Some(len) = self.savepoints.pop() {
			self.ops.truncate(len);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_model_basic_crud() {
		let mut model = ModelDb::new();
		let mut tx = model.begin();
		tx.set(b"k1", b"v1");
		tx.set(b"k2", b"v2");
		assert!(model.commit(tx).is_ok());

		assert_eq!(model.get(b"k1"), Some(b"v1".to_vec()));
		assert_eq!(model.get(b"k2"), Some(b"v2".to_vec()));
		assert_eq!(model.get(b"k3"), None);

		// Delete range
		let mut tx = model.begin();
		tx.delete_range(b"k1", b"k3");
		assert!(model.commit(tx).is_ok());

		assert_eq!(model.get(b"k1"), None);
		assert_eq!(model.get(b"k2"), None);
	}

	#[test]
	fn test_model_conflict_detection() {
		let mut model = ModelDb::new();
		let mut tx1 = model.begin();
		let mut tx2 = model.begin();

		tx1.set(b"k1", b"v1");
		tx2.set(b"k1", b"v2");

		assert!(model.commit(tx1).is_ok());
		// tx2 conflicts with tx1 on k1
		assert!(model.commit(tx2).is_err());
	}
}
