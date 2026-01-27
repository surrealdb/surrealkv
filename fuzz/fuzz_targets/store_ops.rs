#![no_main]

use std::collections::BTreeMap;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::TreeBuilder;
use tempfile::TempDir;

#[derive(Arbitrary, Debug)]
struct StoreFuzzInput {
	/// Pool of keys to use (will be filtered for validity)
	keys: Vec<Vec<u8>>,
	/// Operations to perform
	operations: Vec<StoreOp>,
	/// Flush every N writes (0 = no auto-flush)
	flush_interval: u8,
	/// Use small memtable to trigger frequent flushes to SSTable
	small_memtable: bool,
}

#[derive(Arbitrary, Debug, Clone)]
enum StoreOp {
	/// Set a key to a value
	Set {
		key_idx: u8,
		value: Vec<u8>,
	},
	/// Get a key and verify against expected state
	Get {
		key_idx: u8,
	},
	/// Delete a key
	Delete {
		key_idx: u8,
	},
	/// Range query [start, end) and verify results
	Range {
		start_idx: u8,
		end_idx: u8,
	},
	/// Reverse range query and verify results
	RangeReverse {
		start_idx: u8,
		end_idx: u8,
	},
	/// Force flush memtable to SSTable
	Flush,
	/// Commit current transaction and start new one
	CommitAndNewTxn,
}

fuzz_target!(|data: StoreFuzzInput| {
	// Limits to prevent OOM and excessive runtime
	if data.keys.len() > 30 || data.operations.len() > 200 {
		return;
	}

	// Filter and validate keys
	let keys: Vec<Vec<u8>> =
		data.keys.into_iter().filter(|k| !k.is_empty() && k.len() <= 64).collect();

	if keys.is_empty() {
		return;
	}

	// Filter values in operations
	let operations: Vec<StoreOp> = data
		.operations
		.into_iter()
		.filter(|op| match op {
			StoreOp::Set {
				value,
				..
			} => value.len() <= 256,
			_ => true,
		})
		.collect();

	if operations.is_empty() {
		return;
	}

	// Create tokio runtime - required for Tree operations
	let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();

	// Run the entire test within the runtime
	rt.block_on(async {
		run_fuzz_test(&keys, &operations, data.flush_interval, data.small_memtable).await;
	});
});

async fn run_fuzz_test(
	keys: &[Vec<u8>],
	operations: &[StoreOp],
	flush_interval: u8,
	small_memtable: bool,
) {
	// Create temp directory for the store
	let temp_dir = match TempDir::new() {
		Ok(d) => d,
		Err(_) => return,
	};

	// Build the store with configurable memtable size
	let mut builder = TreeBuilder::new().with_path(temp_dir.path().to_path_buf());

	if small_memtable {
		// Small memtable triggers frequent flushes to SSTable
		builder = builder.with_max_memtable_size(4 * 1024); // 4KB
	}

	let tree = match builder.build() {
		Ok(t) => t,
		Err(_) => return,
	};

	// Oracle: expected state of the store
	// Some(value) = key exists with this value
	// None = key was deleted (tombstone)
	// Key not in map = key never set
	let mut expected: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();

	// Track writes for auto-flush
	let flush_interval_usize = flush_interval.max(1) as usize;
	let mut write_count = 0;

	// Create initial transaction
	let mut txn = match tree.begin() {
		Ok(t) => t,
		Err(_) => return,
	};

	// Execute operations
	for op in operations {
		match op {
			StoreOp::Set {
				key_idx,
				value,
			} => {
				let key_idx = (*key_idx as usize) % keys.len();
				let key = &keys[key_idx];

				match txn.set(key, value) {
					Ok(()) => {
						expected.insert(key.clone(), Some(value.clone()));
						write_count += 1;
					}
					Err(e) => {
						panic!("Set failed for key {:?}: {:?}", key, e);
					}
				}

				// Auto-flush if interval reached
				if flush_interval > 0 && write_count % flush_interval_usize == 0 {
					// Commit current transaction first
					if let Err(e) = txn.commit().await {
						panic!("Commit failed before flush: {:?}", e);
					}

					// Flush
					if let Err(e) = tree.flush() {
						panic!("Flush failed: {:?}", e);
					}

					// Start new transaction
					txn = match tree.begin() {
						Ok(t) => t,
						Err(e) => panic!("Begin after flush failed: {:?}", e),
					};
				}
			}

			StoreOp::Get {
				key_idx,
			} => {
				let key_idx = (*key_idx as usize) % keys.len();
				let key = &keys[key_idx];

				match txn.get(key) {
					Ok(result) => {
						let expected_value = expected.get(key);

						match (result, expected_value) {
							(Some(got), Some(Some(exp))) => {
								// Key exists, value should match
								assert_eq!(
									got.as_slice(),
									exp.as_slice(),
									"Get returned wrong value for key {:?}",
									key
								);
							}
							(None, Some(Some(_))) => {
								// Expected value but got None - BUG
								panic!("Get returned None for key {:?} that should exist", key);
							}
							(None, Some(None)) => {
								// Key was deleted, None is correct
							}
							(None, None) => {
								// Key never set, None is correct
							}
							(Some(got), Some(None)) => {
								// Key was deleted but we got a value - BUG
								panic!("Get returned {:?} for deleted key {:?}", got, key);
							}
							(Some(got), None) => {
								// Key never set but we got a value - BUG
								panic!("Get returned {:?} for never-set key {:?}", got, key);
							}
						}
					}
					Err(e) => {
						panic!("Get failed for key {:?}: {:?}", key, e);
					}
				}
			}

			StoreOp::Delete {
				key_idx,
			} => {
				let key_idx = (*key_idx as usize) % keys.len();
				let key = &keys[key_idx];

				match txn.delete(key) {
					Ok(()) => {
						expected.insert(key.clone(), None); // Mark as deleted
						write_count += 1;
					}
					Err(e) => {
						panic!("Delete failed for key {:?}: {:?}", key, e);
					}
				}
			}

			StoreOp::Range {
				start_idx,
				end_idx,
			} => {
				let start_idx = (*start_idx as usize) % keys.len();
				let end_idx = (*end_idx as usize) % keys.len();

				// Ensure start <= end
				let (start_key, end_key) = if keys[start_idx] <= keys[end_idx] {
					(&keys[start_idx], &keys[end_idx])
				} else {
					(&keys[end_idx], &keys[start_idx])
				};

				// Get results from store
				let mut iter = match txn.range(start_key.as_slice(), end_key.as_slice()) {
					Ok(it) => it,
					Err(e) => panic!("Range query failed: {:?}", e),
				};

				let mut results: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
				if iter.seek_first().unwrap_or(false) {
					while iter.valid() {
						match iter.value() {
							Ok(Some(value)) => {
								results.push((iter.key().to_vec(), value));
							}
							Ok(None) => {
								// Tombstone, skip
							}
							Err(_) => break,
						}
						if !iter.next().unwrap_or(false) {
							break;
						}
					}
				}

				// Get expected results from oracle
				let expected_results: Vec<(&Vec<u8>, &Vec<u8>)> = expected
					.range(start_key.clone()..end_key.clone())
					.filter_map(|(k, v)| v.as_ref().map(|val| (k, val)))
					.collect();

				// INVARIANT: Count must match
				assert_eq!(
					results.len(),
					expected_results.len(),
					"Range [{:?}, {:?}) returned {} entries, expected {}",
					start_key,
					end_key,
					results.len(),
					expected_results.len()
				);

				// INVARIANT: Results must be sorted
				for i in 1..results.len() {
					assert!(
						results[i - 1].0 < results[i].0,
						"Range results not sorted at index {}",
						i
					);
				}

				// INVARIANT: Each result must match expected
				for (i, ((got_key, got_value), (exp_key, exp_value))) in
					results.iter().zip(expected_results.iter()).enumerate()
				{
					assert_eq!(got_key, *exp_key, "Range key mismatch at index {}", i);
					assert_eq!(
						got_value, *exp_value,
						"Range value mismatch at index {} for key {:?}",
						i, got_key
					);
				}
			}

			StoreOp::RangeReverse {
				start_idx,
				end_idx,
			} => {
				let start_idx = (*start_idx as usize) % keys.len();
				let end_idx = (*end_idx as usize) % keys.len();

				// Ensure start <= end
				let (start_key, end_key) = if keys[start_idx] <= keys[end_idx] {
					(&keys[start_idx], &keys[end_idx])
				} else {
					(&keys[end_idx], &keys[start_idx])
				};

				// Get results from store in reverse
				let mut iter = match txn.range(start_key.as_slice(), end_key.as_slice()) {
					Ok(it) => it,
					Err(e) => panic!("RangeReverse query failed: {:?}", e),
				};

				let mut results: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
				if iter.seek_last().unwrap_or(false) {
					while iter.valid() {
						match iter.value() {
							Ok(Some(value)) => {
								results.push((iter.key().to_vec(), value));
							}
							Ok(None) => {
								// Tombstone, skip
							}
							Err(_) => break,
						}
						if !iter.prev().unwrap_or(false) {
							break;
						}
					}
				}

				// Get expected results from oracle in reverse
				let expected_results: Vec<(&Vec<u8>, &Vec<u8>)> = expected
					.range(start_key.clone()..end_key.clone())
					.rev()
					.filter_map(|(k, v)| v.as_ref().map(|val| (k, val)))
					.collect();

				// INVARIANT: Count must match
				assert_eq!(
					results.len(),
					expected_results.len(),
					"RangeReverse [{:?}, {:?}) returned {} entries, expected {}",
					start_key,
					end_key,
					results.len(),
					expected_results.len()
				);

				// INVARIANT: Results must be reverse sorted
				for i in 1..results.len() {
					assert!(
						results[i - 1].0 > results[i].0,
						"RangeReverse results not reverse sorted at index {}",
						i
					);
				}

				// INVARIANT: Each result must match expected
				for (i, ((got_key, got_value), (exp_key, exp_value))) in
					results.iter().zip(expected_results.iter()).enumerate()
				{
					assert_eq!(got_key, *exp_key, "RangeReverse key mismatch at index {}", i);
					assert_eq!(
						got_value, *exp_value,
						"RangeReverse value mismatch at index {} for key {:?}",
						i, got_key
					);
				}
			}

			StoreOp::Flush => {
				// Commit current transaction first
				if let Err(e) = txn.commit().await {
					panic!("Commit before flush failed: {:?}", e);
				}

				// Flush memtable to SSTable
				if let Err(e) = tree.flush() {
					panic!("Flush failed: {:?}", e);
				}

				// Start new transaction
				txn = match tree.begin() {
					Ok(t) => t,
					Err(e) => panic!("Begin after flush failed: {:?}", e),
				};
			}

			StoreOp::CommitAndNewTxn => {
				if let Err(e) = txn.commit().await {
					panic!("Commit failed: {:?}", e);
				}

				txn = match tree.begin() {
					Ok(t) => t,
					Err(e) => panic!("Begin after commit failed: {:?}", e),
				};
			}
		}
	}

	// Final verification: read all keys and verify against oracle
	for (key, expected_value) in &expected {
		match txn.get(key) {
			Ok(result) => {
				match (result, expected_value) {
					(Some(got), Some(exp)) => {
						assert_eq!(
							got.as_slice(),
							exp.as_slice(),
							"Final verification: wrong value for key {:?}",
							key
						);
					}
					(None, Some(_)) => {
						panic!("Final verification: key {:?} should exist but returned None", key);
					}
					(None, None) => {
						// Deleted key, None is correct
					}
					(Some(got), None) => {
						panic!("Final verification: deleted key {:?} returned {:?}", key, got);
					}
				}
			}
			Err(e) => {
				panic!("Final verification: get failed for key {:?}: {:?}", key, e);
			}
		}
	}

	// Cleanup: close the tree gracefully
	// Drop transaction before closing
	drop(txn);

	let _ = tree.close().await;
}
