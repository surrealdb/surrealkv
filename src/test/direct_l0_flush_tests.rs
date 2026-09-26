use tempdir::TempDir;
use test_log::test;

use crate::TreeBuilder;

fn create_temp_directory() -> TempDir {
	TempDir::new("direct_l0_test").unwrap()
}

#[test(tokio::test)]
async fn test_batch_exceeding_max_memtable_size_commits_successfully() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	// Configure a small 64KB max_memtable_size
	const MAX_MEMTABLE: usize = 64 * 1024;
	let tree = TreeBuilder::new()
		.with_path(path.clone())
		.with_max_memtable_size(MAX_MEMTABLE)
		.build()
		.unwrap();

	// Total payload: 400 entries * 300 bytes ≈ 120 KB (> 1.8x max_memtable_size)
	const NUM_ENTRIES: usize = 400;
	let value = vec![0xAB; 300];

	{
		let mut txn = tree.begin().unwrap();
		for i in 0..NUM_ENTRIES {
			let key = format!("oversized_key_{i:06}").into_bytes();
			txn.set(&key, &value).unwrap();
		}
		// Previously, this would fail with Error::ArenaFull because the batch
		// cannot fit in a 64KB arena. Now it should flush directly to an L0 SSTable.
		txn.commit().await.unwrap();
	}

	// Verify all entries can be read back
	{
		let txn = tree.begin().unwrap();
		for i in 0..NUM_ENTRIES {
			let key = format!("oversized_key_{i:06}").into_bytes();
			let val = txn.get(&key).unwrap().expect("entry must exist");
			assert_eq!(val, value);
		}
	}

	// Verify an L0 SSTable was created in the manifest
	{
		let manifest = tree.core.inner.level_manifest.read().unwrap();
		let all_tables = manifest.get_all_tables();
		assert!(!all_tables.is_empty(), "direct-to-L0 flush must create an L0 table");
	}
}

#[test(tokio::test)]
async fn test_mixed_workload_ordering_with_direct_l0_flush() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	const MAX_MEMTABLE: usize = 64 * 1024;
	let tree = TreeBuilder::new()
		.with_path(path.clone())
		.with_max_memtable_size(MAX_MEMTABLE)
		.build()
		.unwrap();

	// 1. Commit small writes into the active memtable
	{
		let mut txn = tree.begin().unwrap();
		txn.set(b"small_1", b"val_small_1").unwrap();
		txn.set(b"small_2", b"val_small_2").unwrap();
		txn.commit().await.unwrap();
	}

	// 2. Commit an oversized batch (exceeds max_memtable_size -> direct to L0)
	const OVERSIZED_COUNT: usize = 300;
	let large_val = vec![0xCD; 300];
	{
		let mut txn = tree.begin().unwrap();
		for i in 0..OVERSIZED_COUNT {
			let k = format!("large_{i:06}").into_bytes();
			txn.set(&k, &large_val).unwrap();
		}
		txn.commit().await.unwrap();
	}

	// 3. Commit more small writes into the new active memtable
	{
		let mut txn = tree.begin().unwrap();
		txn.set(b"small_3", b"val_small_3").unwrap();
		txn.set(b"small_4", b"val_small_4").unwrap();
		txn.commit().await.unwrap();
	}

	// 4. Verify all writes across both memtables and the direct-to-L0 SSTable are readable
	{
		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"small_1").unwrap().as_deref(), Some(&b"val_small_1"[..]));
		assert_eq!(txn.get(b"small_2").unwrap().as_deref(), Some(&b"val_small_2"[..]));
		assert_eq!(txn.get(b"small_3").unwrap().as_deref(), Some(&b"val_small_3"[..]));
		assert_eq!(txn.get(b"small_4").unwrap().as_deref(), Some(&b"val_small_4"[..]));

		for i in 0..OVERSIZED_COUNT {
			let k = format!("large_{i:06}").into_bytes();
			let val = txn.get(&k).unwrap().expect("large entry must exist");
			assert_eq!(val, large_val);
		}
	}
}

#[test(tokio::test)]
async fn test_direct_l0_flush_persists_across_restart() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	const MAX_MEMTABLE: usize = 64 * 1024;
	let value = vec![0xEF; 400];
	const COUNT: usize = 250;

	// Open, write oversized batch, close
	{
		let tree = TreeBuilder::new()
			.with_path(path.clone())
			.with_max_memtable_size(MAX_MEMTABLE)
			.build()
			.unwrap();

		let mut txn = tree.begin().unwrap();
		for i in 0..COUNT {
			let k = format!("restart_key_{i:06}").into_bytes();
			txn.set(&k, &value).unwrap();
		}
		txn.commit().await.unwrap();

		tree.close().await.unwrap();
	}

	// Reopen database at same path and verify data is durable
	{
		let tree = TreeBuilder::new()
			.with_path(path)
			.with_max_memtable_size(MAX_MEMTABLE)
			.build()
			.unwrap();

		let txn = tree.begin().unwrap();
		for i in 0..COUNT {
			let k = format!("restart_key_{i:06}").into_bytes();
			let val = txn.get(&k).unwrap().expect("entry must survive reopen");
			assert_eq!(val, value);
		}
	}
}
