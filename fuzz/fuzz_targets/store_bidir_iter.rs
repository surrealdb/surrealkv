#![no_main]

use std::collections::BTreeMap;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::{LSMIterator, TreeBuilder};
use tempfile::TempDir;

#[derive(Arbitrary, Debug)]
struct BiDirIterFuzzInput {
	keys: Vec<Vec<u8>>,
	operations: Vec<WriteOp>,
	iter_ops: Vec<IterOp>,
	flush_before_iter: bool,
	small_memtable: bool,
	config: u8,
}

#[derive(Arbitrary, Debug, Clone)]
enum WriteOp {
	Set { key_idx: u8, value: Vec<u8> },
	Delete { key_idx: u8 },
	CommitAndNewTxn,
}

#[derive(Arbitrary, Debug, Clone)]
enum IterOp {
	SeekFirst,
	SeekLast,
	Next,
	Prev,
	SeekToKey { key_idx: u8 },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct VersionedEntry {
	user_key: Vec<u8>,
	timestamp: u64,
	value: Vec<u8>,
	is_tombstone: bool,
}

struct OracleIterator {
	entries: Vec<(Vec<u8>, Vec<u8>)>,
	pos: Option<usize>,
	initialized: bool,
}

impl OracleIterator {
	fn new(map: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> Self {
		let entries: Vec<_> = map
			.iter()
			.filter_map(|(k, v)| v.as_ref().map(|val| (k.clone(), val.clone())))
			.collect();
		Self {
			entries,
			pos: None,
			initialized: false,
		}
	}

	fn valid(&self) -> bool {
		self.pos.map_or(false, |p| p < self.entries.len())
	}

	fn key(&self) -> Option<&[u8]> {
		self.pos.and_then(|p| self.entries.get(p).map(|(k, _)| k.as_slice()))
	}

	fn value(&self) -> Option<&[u8]> {
		self.pos.and_then(|p| self.entries.get(p).map(|(_, v)| v.as_slice()))
	}

	fn seek_first(&mut self) {
		self.initialized = true;
		self.pos = if self.entries.is_empty() { None } else { Some(0) };
	}

	fn seek_last(&mut self) {
		self.initialized = true;
		self.pos = if self.entries.is_empty() { None } else { Some(self.entries.len() - 1) };
	}

	fn next(&mut self) {
		if !self.initialized {
			return self.seek_first();
		}
		if let Some(p) = self.pos {
			if p + 1 < self.entries.len() {
				self.pos = Some(p + 1);
			} else {
				self.pos = None;
			}
		}
	}

	fn prev(&mut self) {
		if !self.initialized {
			return self.seek_last();
		}
		if let Some(p) = self.pos {
			if p > 0 {
				self.pos = Some(p - 1);
			} else {
				self.pos = None;
			}
		}
	}

	fn seek(&mut self, target: &[u8]) {
		self.initialized = true;
		self.pos = self.entries.iter().position(|(k, _)| k.as_slice() >= target);
	}
}

struct VersionedOracleIterator {
	entries: Vec<VersionedEntry>,
	pos: Option<usize>,
	initialized: bool,
}

impl VersionedOracleIterator {
	fn new(versions: &BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>, bool)>>) -> Self {
		let mut entries: Vec<VersionedEntry> = Vec::new();
		for (user_key, version_list) in versions {
			if let Some((_, _, true)) = version_list.last() {
				continue;
			}
			for (ts, val, is_tombstone) in version_list {
				if !*is_tombstone {
					entries.push(VersionedEntry {
						user_key: user_key.clone(),
						timestamp: *ts,
						value: val.clone(),
						is_tombstone: *is_tombstone,
					});
				}
			}
		}
		entries.sort_by(|a, b| match a.user_key.cmp(&b.user_key) {
			std::cmp::Ordering::Equal => b.timestamp.cmp(&a.timestamp),
			other => other,
		});
		Self {
			entries,
			pos: None,
			initialized: false,
		}
	}

	fn valid(&self) -> bool {
		self.pos.map_or(false, |p| p < self.entries.len())
	}

	fn current(&self) -> Option<&VersionedEntry> {
		self.pos.and_then(|p| self.entries.get(p))
	}

	fn seek_first(&mut self) {
		self.initialized = true;
		self.pos = if self.entries.is_empty() { None } else { Some(0) };
	}

	fn seek_last(&mut self) {
		self.initialized = true;
		self.pos = if self.entries.is_empty() { None } else { Some(self.entries.len() - 1) };
	}

	fn next(&mut self) {
		if !self.initialized {
			return self.seek_first();
		}
		if let Some(p) = self.pos {
			if p + 1 < self.entries.len() {
				self.pos = Some(p + 1);
			} else {
				self.pos = None;
			}
		}
	}

	fn prev(&mut self) {
		if !self.initialized {
			return self.seek_last();
		}
		if let Some(p) = self.pos {
			if p > 0 {
				self.pos = Some(p - 1);
			} else {
				self.pos = None;
			}
		}
	}

	fn seek(&mut self, target: &[u8]) {
		self.initialized = true;
		self.pos = self.entries.iter().position(|e| e.user_key.as_slice() >= target);
	}
}

fuzz_target!(|data: BiDirIterFuzzInput| {
	if data.keys.len() > 30 || data.operations.len() > 100 || data.iter_ops.len() > 100 {
		return;
	}

	let keys: Vec<Vec<u8>> =
		data.keys.into_iter().filter(|k| !k.is_empty() && k.len() <= 64).collect();

	if keys.is_empty() {
		return;
	}

	let operations: Vec<WriteOp> = data
		.operations
		.into_iter()
		.filter(|op| match op {
			WriteOp::Set { value, .. } => value.len() <= 256,
			_ => true,
		})
		.collect();

	if operations.is_empty() || data.iter_ops.is_empty() {
		return;
	}

	let config = data.config % 3;

	let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

	rt.block_on(async {
		run_fuzz_test(
			&keys,
			&operations,
			&data.iter_ops,
			data.flush_before_iter,
			data.small_memtable,
			config,
		)
		.await;
	});
});

async fn run_fuzz_test(
	keys: &[Vec<u8>],
	operations: &[WriteOp],
	iter_ops: &[IterOp],
	flush_before_iter: bool,
	small_memtable: bool,
	config: u8,
) {
	let temp_dir = match TempDir::new() {
		Ok(d) => d,
		Err(_) => return,
	};

	let mut builder = TreeBuilder::new().with_path(temp_dir.path().to_path_buf());

	if small_memtable {
		builder = builder.with_max_memtable_size(4 * 1024);
	}

	match config {
		0 => {}
		1 => {
			builder = builder.with_versioning(true, 0);
		}
		2 => {
			builder = builder.with_versioning(true, 0).with_versioned_index(true);
		}
		_ => unreachable!(),
	}

	let tree = match builder.build() {
		Ok(t) => t,
		Err(_) => return,
	};

	let mut expected: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
	let mut versioned_expected: BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>, bool)>> = BTreeMap::new();
	let mut version_counter: u64 = 1;

	// Track which keys were written in the current transaction.
	// When the same key is written again in the same transaction (without explicit timestamps),
	// the database replaces the previous write, so the oracle must do the same.
	let mut current_txn_writes: std::collections::HashMap<Vec<u8>, usize> = std::collections::HashMap::new();

	let mut txn = match tree.begin() {
		Ok(t) => t,
		Err(_) => return,
	};

	for op in operations {
		match op {
			WriteOp::Set { key_idx, value } => {
				let key_idx = (*key_idx as usize) % keys.len();
				let key = &keys[key_idx];

				match txn.set(key, value) {
					Ok(()) => {
						expected.insert(key.clone(), Some(value.clone()));
						if config > 0 {
							// Check if this key was already written in the current transaction
							if let Some(&version_idx) = current_txn_writes.get(key) {
								// Replace the previous write (same transaction deduplication)
								let versions = versioned_expected.get_mut(key).unwrap();
								versions[version_idx] = (versions[version_idx].0, value.clone(), false);
							} else {
								// New write in this transaction
								let versions = versioned_expected.entry(key.clone()).or_default();
								current_txn_writes.insert(key.clone(), versions.len());
								versions.push((version_counter, value.clone(), false));
								version_counter += 1;
							}
						}
					}
					Err(_) => return,
				}
			}

			WriteOp::Delete { key_idx } => {
				let key_idx = (*key_idx as usize) % keys.len();
				let key = &keys[key_idx];

				match txn.delete(key) {
					Ok(()) => {
						expected.insert(key.clone(), None);
						if config > 0 {
							// Check if this key was already written in the current transaction
							if let Some(&version_idx) = current_txn_writes.get(key) {
								// Replace the previous write with this delete
								let versions = versioned_expected.get_mut(key).unwrap();
								versions[version_idx] = (versions[version_idx].0, Vec::new(), true);
							} else {
								// New delete in this transaction
								let versions = versioned_expected.entry(key.clone()).or_default();
								current_txn_writes.insert(key.clone(), versions.len());
								versions.push((version_counter, Vec::new(), true));
								version_counter += 1;
							}
						}
					}
					Err(_) => return,
				}
			}

			WriteOp::CommitAndNewTxn => {
				if txn.commit().await.is_err() {
					return;
				}
				// Reset tracking for the new transaction
				current_txn_writes.clear();
				txn = match tree.begin() {
					Ok(t) => t,
					Err(_) => return,
				};
			}
		}
	}

	if txn.commit().await.is_err() {
		return;
	}

	if flush_before_iter {
		if tree.flush_wal(true).is_err() {
			return;
		}
	}

	let txn = match tree.begin() {
		Ok(t) => t,
		Err(_) => return,
	};

	if config == 0 {
		run_non_versioned_iteration(&txn, &expected, iter_ops, keys);
	} else {
		run_versioned_iteration(&txn, &versioned_expected, iter_ops, keys);
	}

	drop(txn);
	let _ = tree.close().await;
}

fn run_non_versioned_iteration(
	txn: &surrealkv::Transaction,
	expected: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
	iter_ops: &[IterOp],
	keys: &[Vec<u8>],
) {
	let min_key: Vec<u8> = vec![];
	let max_key: Vec<u8> = vec![0xFF; 65];

	let mut iter = match txn.range(min_key.as_slice(), max_key.as_slice()) {
		Ok(it) => it,
		Err(_) => return,
	};

	let mut oracle = OracleIterator::new(expected);

	for op in iter_ops {
		match op {
			IterOp::SeekFirst => {
				let _ = iter.seek_first();
				oracle.seek_first();
			}
			IterOp::SeekLast => {
				let _ = iter.seek_last();
				oracle.seek_last();
			}
			IterOp::Next => {
				let _ = iter.next();
				oracle.next();
			}
			IterOp::Prev => {
				let _ = iter.prev();
				oracle.prev();
			}
			IterOp::SeekToKey { key_idx } => {
				let key_idx = (*key_idx as usize) % keys.len();
				let key = &keys[key_idx];
				let _ = iter.seek(key);
				oracle.seek(key);
			}
		}

		assert_eq!(
			iter.valid(),
			oracle.valid(),
			"valid() mismatch after {:?}: iter={}, oracle={}",
			op,
			iter.valid(),
			oracle.valid()
		);

		if iter.valid() && oracle.valid() {
			let iter_key = iter.key().user_key();
			let oracle_key = oracle.key().unwrap();
			assert_eq!(
				iter_key, oracle_key,
				"key mismatch after {:?}: iter={:?}, oracle={:?}",
				op, iter_key, oracle_key
			);

			let iter_value = match iter.value() {
				Ok(v) => v,
				Err(_) => return,
			};
			let oracle_value = oracle.value().unwrap();
			assert_eq!(
				iter_value.as_slice(),
				oracle_value,
				"value mismatch after {:?}",
				op
			);
		}
	}
}

fn run_versioned_iteration(
	txn: &surrealkv::Transaction,
	versioned_expected: &BTreeMap<Vec<u8>, Vec<(u64, Vec<u8>, bool)>>,
	iter_ops: &[IterOp],
	keys: &[Vec<u8>],
) {
	let min_key: Vec<u8> = vec![];
	let max_key: Vec<u8> = vec![0xFF; 65];

	let mut iter = match txn.history(min_key.as_slice(), max_key.as_slice()) {
		Ok(it) => it,
		Err(_) => return,
	};

	let mut oracle = VersionedOracleIterator::new(versioned_expected);

	for op in iter_ops {
		match op {
			IterOp::SeekFirst => {
				let _ = iter.seek_first();
				oracle.seek_first();
			}
			IterOp::SeekLast => {
				let _ = iter.seek_last();
				oracle.seek_last();
			}
			IterOp::Next => {
				let _ = iter.next();
				oracle.next();
			}
			IterOp::Prev => {
				let _ = iter.prev();
				oracle.prev();
			}
			IterOp::SeekToKey { key_idx } => {
				let key_idx = (*key_idx as usize) % keys.len();
				let key = &keys[key_idx];
				let _ = iter.seek(key);
				oracle.seek(key);
			}
		}

		assert_eq!(
			iter.valid(),
			oracle.valid(),
			"valid() mismatch after {:?}: iter={}, oracle={}",
			op,
			iter.valid(),
			oracle.valid()
		);

		if iter.valid() && oracle.valid() {
			let iter_key = iter.key().user_key();
			let oracle_entry = oracle.current().unwrap();
			assert_eq!(
				iter_key,
				oracle_entry.user_key.as_slice(),
				"key mismatch after {:?}: iter={:?}, oracle={:?}",
				op,
				iter_key,
				oracle_entry.user_key
			);

			let iter_value = match iter.value() {
				Ok(v) => v,
				Err(_) => return,
			};
			assert_eq!(
				iter_value.as_slice(),
				oracle_entry.value.as_slice(),
				"value mismatch after {:?}",
				op
			);
		}
	}
}
