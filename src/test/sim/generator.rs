//! Deterministic, PRNG seed-driven workload generator.

use crate::Key;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

#[derive(Debug, Clone)]
pub enum Action {
	Begin(usize),
	Set(usize, Key, Vec<u8>),
	Delete(usize, Key),
	DeleteRange(usize, Key, Key),
	Commit(usize),
	Rollback(usize),
	PointRead(Key),
	RangeScan(Key, Key),
	CrashAndRestart,
}

pub struct WorkloadGenerator {
	rng: StdRng,
	key_pool_size: usize,
	next_txn_id: usize,
	active_txns: Vec<usize>,
}

impl WorkloadGenerator {
	pub fn new(seed: u64, key_pool_size: usize) -> Self {
		Self {
			rng: StdRng::seed_from_u64(seed),
			key_pool_size,
			next_txn_id: 0,
			active_txns: Vec::new(),
		}
	}

	fn random_key(&mut self) -> Key {
		let id = self.rng.random_range(0..self.key_pool_size);
		format!("k_{:04}", id).into_bytes()
	}

	fn random_value(&mut self) -> Vec<u8> {
		let len = self.rng.random_range(8..64);
		let mut v = vec![0u8; len];
		self.rng.fill(&mut v[..]);
		v
	}

	/// Generates the next deterministic action.
	pub fn next_action(&mut self) -> Action {
		// If no active transactions, we must begin one or read
		if self.active_txns.is_empty() {
			let id = self.next_txn_id;
			self.next_txn_id += 1;
			self.active_txns.push(id);
			return Action::Begin(id);
		}

		let choice = self.rng.random_range(0..100);

		if choice < 20 {
			// Begin a concurrent transaction (up to 8 concurrent)
			if self.active_txns.len() < 8 {
				let id = self.next_txn_id;
				self.next_txn_id += 1;
				self.active_txns.push(id);
				return Action::Begin(id);
			}
		}

		// Pick an active transaction
		let txn_idx = self.rng.random_range(0..self.active_txns.len());
		let txn_id = self.active_txns[txn_idx];

		if choice < 55 {
			// Set
			let key = self.random_key();
			let val = self.random_value();
			Action::Set(txn_id, key, val)
		} else if choice < 65 {
			// Delete
			let key = self.random_key();
			Action::Delete(txn_id, key)
		} else if choice < 70 {
			// Delete range
			let k1 = self.random_key();
			let k2 = self.random_key();
			let (start, end) = if k1 < k2 {
				(k1, k2)
			} else {
				(k2, k1)
			};
			Action::DeleteRange(txn_id, start, end)
		} else if choice < 85 {
			// Commit
			self.active_txns.remove(txn_idx);
			Action::Commit(txn_id)
		} else if choice < 90 {
			// Rollback
			self.active_txns.remove(txn_idx);
			Action::Rollback(txn_id)
		} else if choice < 93 {
			// Point read on committed data
			let key = self.random_key();
			Action::PointRead(key)
		} else if choice < 98 {
			// Range scan
			let k1 = self.random_key();
			let k2 = self.random_key();
			let (start, end) = if k1 < k2 {
				(k1, k2)
			} else {
				(k2, k1)
			};
			Action::RangeScan(start, end)
		} else {
			// Simulated crash and restart
			self.active_txns.clear();
			Action::CrashAndRestart
		}
	}
}
