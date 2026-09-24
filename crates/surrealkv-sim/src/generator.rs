//! Deterministic, PRNG seed-driven workload generator.

use std::collections::HashMap;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use surrealkv::Key;

#[derive(Debug, Clone)]
pub enum Action {
	Begin(usize),
	Set(usize, Key, Vec<u8>),
	Delete(usize, Key),
	DeleteRange(usize, Key, Key),
	SetSavepoint(usize),
	RollbackToSavepoint(usize, u32),
	TxnRead(usize, Key),
	Commit(usize),
	Rollback(usize),
	PointRead(Key),
	RangeScan(Key, Key),
	Flush,
	CrashAndRestart,
}

pub struct WorkloadGenerator {
	rng: StdRng,
	key_pool_size: usize,
	next_txn_id: usize,
	active_txns: Vec<usize>,
	txn_savepoints: HashMap<usize, Vec<u32>>,
	step_count: usize,
}

impl WorkloadGenerator {
	pub fn new(seed: u64, key_pool_size: usize) -> Self {
		Self {
			rng: StdRng::seed_from_u64(seed),
			key_pool_size,
			next_txn_id: 0,
			active_txns: Vec::new(),
			txn_savepoints: HashMap::new(),
			step_count: 0,
		}
	}

	fn random_key(&mut self) -> Key {
		let mode = self.rng.random_range(0..100);
		if mode < 25 {
			// High-contention hot key (creates heavy OCC validation conflicts)
			let id = self.rng.random_range(0..3.min(self.key_pool_size));
			format!("hot_{id:02}").into_bytes()
		} else if mode < 30 {
			// Binary key with null bytes and boundary 0xff
			let mut k = b"\x00bin_".to_vec();
			k.push(self.rng.random_range(0..=255));
			k.extend_from_slice(b"_\xff");
			k
		} else if mode < 35 {
			// Long key (stresses block prefix compression)
			let id = self.rng.random_range(0..self.key_pool_size);
			format!("long_key_with_extended_prefix_{id:04}_{}", "X".repeat(128)).into_bytes()
		} else {
			// Standard uniform key
			let id = self.rng.random_range(0..self.key_pool_size);
			format!("k_{id:04}").into_bytes()
		}
	}

	fn random_value(&mut self) -> Vec<u8> {
		let mode = self.rng.random_range(0..100);
		if mode < 10 {
			// Empty value
			Vec::new()
		} else if mode < 30 {
			// Large value (stresses VLog pointer separation)
			let len = self.rng.random_range(1024..4096);
			let mut v = vec![0u8; len];
			self.rng.fill(&mut v[..]);
			v
		} else if mode < 55 {
			// Compressible value (repeated pattern, stresses Zstd and LZ4 compression)
			let pattern = b"SURREALDB_COMPACTED_PAYLOAD_CHUNK_";
			let repeats = self.rng.random_range(2..16);
			let mut v = Vec::with_capacity(pattern.len() * repeats);
			for _ in 0..repeats {
				v.extend_from_slice(pattern);
			}
			v
		} else {
			// Standard small/medium value
			let len = self.rng.random_range(8..128);
			let mut v = vec![0u8; len];
			self.rng.fill(&mut v[..]);
			v
		}
	}

	/// Generates the next deterministic action.
	pub fn next_action(&mut self) -> Action {
		self.step_count += 1;

		// If no active transactions, we must begin one
		if self.active_txns.is_empty() {
			let id = self.next_txn_id;
			self.next_txn_id += 1;
			self.active_txns.push(id);
			self.txn_savepoints.insert(id, Vec::new());
			return Action::Begin(id);
		}

		let choice = self.rng.random_range(0..100);

		if choice < 15 {
			// Begin a concurrent transaction (up to 12 concurrent)
			if self.active_txns.len() < 12 {
				let id = self.next_txn_id;
				self.next_txn_id += 1;
				self.active_txns.push(id);
				self.txn_savepoints.insert(id, Vec::new());
				return Action::Begin(id);
			}
		}

		// Pick an active transaction
		let txn_idx = self.rng.random_range(0..self.active_txns.len());
		let txn_id = self.active_txns[txn_idx];

		if choice < 45 {
			// Set
			let key = self.random_key();
			let val = self.random_value();
			Action::Set(txn_id, key, val)
		} else if choice < 53 {
			// In-transaction read (Read-Your-Own-Writes)
			let key = self.random_key();
			Action::TxnRead(txn_id, key)
		} else if choice < 60 {
			// Delete
			let key = self.random_key();
			Action::Delete(txn_id, key)
		} else if choice < 64 {
			// Delete range
			let k1 = self.random_key();
			let k2 = self.random_key();
			let (start, end) = if k1 < k2 {
				(k1, k2)
			} else {
				(k2, k1)
			};
			Action::DeleteRange(txn_id, start, end)
		} else if choice < 68 {
			// Savepoint
			let sps = self.txn_savepoints.entry(txn_id).or_default();
			let sp_id = sps.len() as u32;
			sps.push(sp_id);
			Action::SetSavepoint(txn_id)
		} else if choice < 72 {
			// Rollback to savepoint
			if let Some(sps) = self.txn_savepoints.get_mut(&txn_id) {
				if let Some(sp_id) = sps.pop() {
					return Action::RollbackToSavepoint(txn_id, sp_id);
				}
			}
			let key = self.random_key();
			let val = self.random_value();
			Action::Set(txn_id, key, val)
		} else if choice < 84 {
			// Commit
			self.active_txns.remove(txn_idx);
			self.txn_savepoints.remove(&txn_id);
			Action::Commit(txn_id)
		} else if choice < 88 {
			// Rollback
			self.active_txns.remove(txn_idx);
			self.txn_savepoints.remove(&txn_id);
			Action::Rollback(txn_id)
		} else if choice < 93 {
			// Point read on committed data
			let key = self.random_key();
			Action::PointRead(key)
		} else if choice < 97 {
			// Range scan
			let k1 = self.random_key();
			let k2 = self.random_key();
			let (start, end) = if k1 < k2 {
				(k1, k2)
			} else {
				(k2, k1)
			};
			Action::RangeScan(start, end)
		} else if choice < 99 {
			// Flush memtable
			Action::Flush
		} else {
			// Simulated crash and restart
			self.active_txns.clear();
			self.txn_savepoints.clear();
			Action::CrashAndRestart
		}
	}
}
