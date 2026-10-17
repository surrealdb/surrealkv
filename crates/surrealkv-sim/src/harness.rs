//! Deterministic Simulation & Differential Testing Harness.

use std::collections::HashMap;
use std::ops::Bound;
use std::path::PathBuf;

use surrealkv::{
	CrashMode,
	FileSystem,
	Key,
	LSMIterator,
	ReadOptions,
	Result,
	Transaction,
	Tree,
	TreeBuilder,
};
use tempfile::TempDir;

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

/// The states a database may be in after a crash, in the runs over memory.
///
/// A crash keeps what was synced and may keep any prefix of what was not, so the database that is
/// recovered is the last state that was made durable, or the state after any commit since, and
/// no other. The model is rewound to the one that was recovered.
#[derive(Default)]
struct Durability {
	/// The state when the log was last synced, or when the database was last recovered.
	durable: Vec<(Key, Vec<u8>)>,
	/// The state after each commit since, oldest first.
	since: Vec<Vec<(Key, Vec<u8>)>>,
}

/// More commits than this since the last sync and the runner syncs the log, which no crash can
/// tell from the run that had synced it earlier: the states to compare with stay few.
const MAX_UNSYNCED_COMMITS: usize = 24;

/// The ways of crashing a run over memory takes in turn, whatever the seed.
const CRASHES: [CrashMode; 6] = [
	CrashMode::LoseUnsynced,
	CrashMode::ZeroTail,
	CrashMode::Prefix(usize::MAX),
	CrashMode::Prefix(0),
	CrashMode::Prefix(7),
	CrashMode::Tear(3, 11),
];

pub struct SimRunner {
	store: Option<Tree>,
	model: ModelDb,
	db_path: PathBuf,
	/// The directory of the database on the disk, for a run on the disk.
	_temp_dir: Option<TempDir>,
	/// The file system of a run over memory, which a crash replaces by what the crash left.
	memory: Option<FileSystem>,
	states: Durability,
	crashes: usize,
	/// The commits that a crash lost, which were acknowledged and not synced.
	lost_commits: usize,
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
			_temp_dir: Some(temp_dir),
			memory: None,
			states: Durability::default(),
			crashes: 0,
			lost_commits: 0,
		}
	}

	/// A run over `FileSystem::memory()`. A restart is a crash: the database is stopped without a
	/// close, the memory is mounted as one of the ways of crashing left it (what was synced, a
	/// prefix of what was not, a zeroed or a torn tail), and the recovered database must be a state
	/// that a crash could leave, which the model is then rewound to.
	pub fn in_memory() -> Self {
		let fs = FileSystem::memory();
		let db_path = PathBuf::from("db");
		let store = TreeBuilder::new()
			.with_path(db_path.clone())
			.with_file_system(fs.clone())
			.build()
			.unwrap();

		Self {
			store: Some(store),
			model: ModelDb::new(),
			db_path,
			_temp_dir: None,
			memory: Some(fs),
			states: Durability::default(),
			crashes: 0,
			lost_commits: 0,
		}
	}

	/// The crash of a run over memory: stops the database as a crash would, mounts the memory as a
	/// way of crashing left it and opens the database there, and checks what recovery made of it.
	fn crash_and_recover(&mut self, seed: u64, step: usize) {
		let fs = self.memory.take().expect("a run over memory");
		let mode = CRASHES[self.crashes % CRASHES.len()];
		self.crashes += 1;
		if let Some(store) = self.store.take() {
			store.abandon();
			fs.abandon();
			drop(store);
		}
		let image = fs.simulate_crash(mode).expect("a memory file system crashes");
		let store = TreeBuilder::new()
			.with_path(self.db_path.clone())
			.with_file_system(image.clone())
			.build()
			.unwrap_or_else(|e| {
				panic!("the database does not open after {mode:?} at seed {seed}, step {step}: {e}")
			});
		let rtx = store.begin().unwrap();
		let mut iter = rtx.range_with_options(&ReadOptions::default()).unwrap();
		let recovered = collect_transaction_all(&mut iter).unwrap();
		drop(iter);
		drop(rtx);

		// The newest state that matches is the one that was recovered.
		let matched = self
			.states
			.since
			.iter()
			.rposition(|s| *s == recovered)
			.map(|at| at + 1)
			.or((recovered == self.states.durable).then_some(0));
		assert!(
			matched.is_some(),
			"State after {mode:?} is neither the last durable state nor one after a later commit, \
			 at seed {seed}, step {step}: {} keys recovered, {} durable, {} commits since",
			recovered.len(),
			self.states.durable.len(),
			self.states.since.len()
		);
		self.lost_commits += self.states.since.len() - matched.unwrap_or_default();
		if mode == CrashMode::Prefix(usize::MAX) {
			// Everything that was written survives, so the last state does.
			let last = self.states.since.last().unwrap_or(&self.states.durable);
			assert_eq!(
				&recovered, last,
				"A crash that loses nothing lost a commit, at seed {seed}, step {step}"
			);
		}
		self.model.restore(recovered.clone());
		self.states = Durability {
			durable: recovered,
			since: Vec::new(),
		};
		self.store = Some(store);
		self.memory = Some(image);
	}

	/// Notes a commit that both the store and the model accepted, in a run over memory.
	fn committed(&mut self) {
		if self.memory.is_none() {
			return;
		}
		self.states.since.push(self.model.state());
		if self.states.since.len() >= MAX_UNSYNCED_COMMITS {
			if let Some(ref store) = self.store {
				if store.flush_wal(true).is_ok() {
					self.synced();
				}
			}
		}
	}

	/// Notes that the log was synced: everything committed is durable.
	fn synced(&mut self) {
		if self.memory.is_none() {
			return;
		}
		self.states = Durability {
			durable: self.model.state(),
			since: Vec::new(),
		};
	}

	/// How many crashes a run over memory made, and how many acknowledged commits they lost.
	pub fn crashes(&self) -> (usize, usize) {
		(self.crashes, self.lost_commits)
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
								self.committed();
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
						if store.flush_wal(true).is_ok() {
							self.synced();
						}
					}
				}
				Action::CrashAndRestart => {
					// Drop all in-flight uncommitted transactions
					active_txns.clear();

					if self.memory.is_some() {
						self.crash_and_recover(seed, step);
						continue;
					}

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
