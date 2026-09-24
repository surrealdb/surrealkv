//! Deterministic Simulation & Differential Testing Harness.

use std::collections::HashMap;
use std::ops::Bound;
use std::path::PathBuf;
use tempfile::TempDir;

use surrealkv::{LSMIterator, ReadOptions, Result, Transaction, Tree, TreeBuilder};

use super::generator::{Action, WorkloadGenerator};
use super::model::{ModelDb, ModelTxn};

fn collect_transaction_all(iter: &mut impl LSMIterator) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	iter.seek_first()?;
	let mut result = Vec::new();
	while iter.valid() {
		let key = iter.key().user_key().to_vec();
		let value = iter.value()?;
		result.push((key, value));
		iter.next()?;
	}
	Ok(result)
}

pub struct SimRunner {
	store: Option<Tree>,
	model: ModelDb,
	db_path: PathBuf,
	_temp_dir: TempDir,
}

impl Default for SimRunner {
	fn default() -> Self {
		Self::new()
	}
}

impl SimRunner {
	pub fn new() -> Self {
		let temp_dir = TempDir::new().unwrap();
		let db_path = temp_dir.path().to_path_buf();
		let store = TreeBuilder::new().with_path(db_path.clone()).build().unwrap();

		Self {
			store: Some(store),
			model: ModelDb::new(),
			db_path,
			_temp_dir: temp_dir,
		}
	}

	pub async fn close(&mut self) {
		if let Some(store) = self.store.take() {
			let _ = store.close().await;
		}
	}

	/// Runs deterministic simulation for N actions and checks differential equivalence.
	pub async fn run(&mut self, seed: u64, steps: usize) {
		let mut gen = WorkloadGenerator::new(seed, 30);
		let mut active_txns: HashMap<usize, (Transaction, ModelTxn)> = HashMap::new();

		for step in 0..steps {
			let action = gen.next_action();
			match action {
				Action::Begin(id) => {
					let store_tx = self.store.as_ref().unwrap().begin().unwrap();
					let model_tx = self.model.begin();
					active_txns.insert(id, (store_tx, model_tx));
				}
				Action::Set(id, key, val) => {
					if let Some((ref mut stx, ref mut mtx)) = active_txns.get_mut(&id) {
						stx.set(&key, &val).unwrap();
						mtx.set(&key, &val);
					}
				}
				Action::TxnRead(id, key) => {
					if let Some((ref stx, ref mtx)) = active_txns.get(&id) {
						let s_val = stx.get(&key).unwrap();
						let m_val = mtx.get(&key, &self.model);
						assert_eq!(
							s_val,
							m_val,
							"In-transaction read mismatch at seed {seed}, step {step}, txn {id} on key {:?}",
							String::from_utf8_lossy(&key)
						);
					}
				}
				Action::SetSavepoint(id) => {
					if let Some((ref mut stx, ref mut mtx)) = active_txns.get_mut(&id) {
						stx.set_savepoint().unwrap();
						mtx.set_savepoint();
					}
				}
				Action::RollbackToSavepoint(id, _sp) => {
					if let Some((ref mut stx, ref mut mtx)) = active_txns.get_mut(&id) {
						stx.rollback_to_savepoint().unwrap();
						mtx.rollback_to_savepoint();
					}
				}
				Action::Delete(id, key) => {
					if let Some((ref mut stx, ref mut mtx)) = active_txns.get_mut(&id) {
						stx.delete(&key).unwrap();
						mtx.delete(&key);
					}
				}
				Action::DeleteRange(id, start, end) => {
					if let Some((ref mut stx, ref mut mtx)) = active_txns.get_mut(&id) {
						stx.delete_range(&start, &end).unwrap();
						mtx.delete_range(&start, &end);
					}
				}
				Action::Commit(id) => {
					if let Some((mut stx, mtx)) = active_txns.remove(&id) {
						let s_res = stx.commit().await;
						let m_res = self.model.commit(mtx);

						match (s_res.is_ok(), m_res.is_ok()) {
							(true, true) => {
								// Both succeeded
							}
							(false, false) => {
								// Both conflicted/failed
							}
							(true, false) => {
								panic!(
									"Divergence at seed {seed}, step {step}: SurrealKV committed but Model detected conflict!"
								);
							}
							(false, true) => {
								panic!(
									"Divergence at seed {seed}, step {step}: Model committed but SurrealKV conflicted: {:?}",
									s_res.err()
								);
							}
						}
					}
				}
				Action::Rollback(id) => {
					if let Some((mut stx, _)) = active_txns.remove(&id) {
						stx.rollback();
					}
				}
				Action::PointRead(key) => {
					let stx = self.store.as_ref().unwrap().begin().unwrap();
					let store_val = stx.get(&key).unwrap();
					let store_val_async = stx.get_async(&key).await.unwrap();
					assert_eq!(
						store_val,
						store_val_async,
						"Sync get and async get diverged at seed {seed}, step {step} on key {:?}",
						String::from_utf8_lossy(&key)
					);
					let model_val = self.model.get(&key);

					assert_eq!(
						store_val,
						model_val,
						"Point read mismatch at seed {seed}, step {step} on key {:?}",
						String::from_utf8_lossy(&key)
					);
				}
				Action::RangeScan(start, end) => {
					let stx = self.store.as_ref().unwrap().begin().unwrap();
					let mut iter = stx.range(&start, &end).unwrap();
					let store_items = collect_transaction_all(&mut iter).unwrap();
					let model_items =
						self.model.range(Bound::Included(&start), Bound::Excluded(&end));

					assert_eq!(
						store_items,
						model_items,
						"Range scan mismatch at seed {seed}, step {step} for range [{:?}..{:?}]",
						String::from_utf8_lossy(&start),
						String::from_utf8_lossy(&end)
					);
				}
				Action::Flush => {
					if let Some(ref store) = self.store {
						let _ = store.flush_wal(true);
					}
				}
				Action::CrashAndRestart => {
					// Drop all in-flight uncommitted transactions
					active_txns.clear();

					// Restart the store to trigger recovery
					if let Some(store) = self.store.take() {
						let _ = store.close().await;
					}
					self.store =
						Some(TreeBuilder::new().with_path(self.db_path.clone()).build().unwrap());

					// Verify full state consistency after recovery
					let rtx = self.store.as_ref().unwrap().begin().unwrap();
					let mut iter = rtx.range_with_options(&ReadOptions::default()).unwrap();
					let recovered_items = collect_transaction_all(&mut iter).unwrap();
					let model_items = self.model.range(Bound::Unbounded, Bound::Unbounded);

					assert_eq!(
						recovered_items, model_items,
						"State divergence after restart at seed {seed}, step {step}"
					);
				}
			}
		}
	}
}
