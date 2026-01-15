use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::lsm::Tree;
use crate::test::{collect_transaction_all, collect_transaction_reverse};
use crate::{Error, Key, Mode, Options, ReadOptions, TreeBuilder, WriteOptions};

fn create_temp_directory() -> TempDir {
	TempDir::new("test").unwrap()
}

/// Type alias for a map of keys to their version information
/// Each key maps to a vector of (value, timestamp, is_tombstone) tuples
#[allow(dead_code)]
type KeyVersionsMap = HashMap<Key, Vec<(Vec<u8>, u64, bool)>>;

// Common setup logic for creating a store
fn create_store() -> (Tree, TempDir) {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let tree = TreeBuilder::new().with_path(path).build().unwrap();
	(tree, temp_dir)
}

#[test(tokio::test)]
async fn basic_transaction() {
	let (store, _temp_dir) = create_store();

	// Define key-value pairs for the test
	let key1 = Vec::from("foo1");
	let key2 = Vec::from("foo2");
	let value1 = Vec::from("baz");
	let value2 = Vec::from("bar");

	{
		// Start a new read-write transaction (txn1)
		let mut txn1 = store.begin().unwrap();
		txn1.set(&key1, &value1).unwrap();
		txn1.set(&key2, &value1).unwrap();
		txn1.commit().await.unwrap();
	}

	{
		// Start a read-only transaction (txn3)
		let txn3 = store.begin().unwrap();
		let val = txn3.get(&key1).unwrap().unwrap();
		assert_eq!(&val, &value1);
	}

	{
		// Start another read-write transaction (txn2)
		let mut txn2 = store.begin().unwrap();
		txn2.set(&key1, &value2).unwrap();
		txn2.set(&key2, &value2).unwrap();
		txn2.commit().await.unwrap();
	}

	// Start a read-only transaction (txn4)
	let txn4 = store.begin().unwrap();
	let val = txn4.get(&key1).unwrap().unwrap();

	// Assert that the value retrieved in txn4 matches value2
	assert_eq!(&val, &value2);
}

#[test(tokio::test)]
async fn mvcc_snapshot_isolation() {
	let (store, _) = create_store();

	let key1 = Vec::from("key1");
	let key2 = Vec::from("key2");
	let value1 = Vec::from("baz");
	let value2 = Vec::from("bar");

	// no conflict
	{
		let mut txn1 = store.begin().unwrap();
		let mut txn2 = store.begin().unwrap();

		txn1.set(&key1, &value1).unwrap();
		txn1.commit().await.unwrap();

		assert!(txn2.get(&key2).unwrap().is_none());
		txn2.set(&key2, &value2).unwrap();
		txn2.commit().await.unwrap();
	}

	// blind writes should succeed if key wasn't read first
	{
		let mut txn1 = store.begin().unwrap();
		let mut txn2 = store.begin().unwrap();

		txn1.set(&key1, &value1).unwrap();
		txn2.set(&key1, &value2).unwrap();

		txn1.commit().await.unwrap();
		assert!(match txn2.commit().await {
			Err(err) => {
				matches!(err, Error::TransactionWriteConflict)
			}
			_ => {
				false
			}
		});
	}

	// conflict when the read key was updated by another transaction
	{
		let key = Vec::from("key3");

		let mut txn1 = store.begin().unwrap();
		let mut txn2 = store.begin().unwrap();

		txn1.set(&key, &value1).unwrap();
		txn1.commit().await.unwrap();

		assert!(txn2.get(&key).unwrap().is_none());
		txn2.set(&key, &value1).unwrap();
		assert!(match txn2.commit().await {
			Err(err) => {
				matches!(err, Error::TransactionWriteConflict)
			}
			_ => false,
		});
	}
}

#[test(tokio::test)]
async fn ryow() {
	let (store, _) = create_store();

	let key1 = Vec::from("k1");
	let key2 = Vec::from("k2");
	let key3 = Vec::from("k3");
	let value1 = Vec::from("v1");
	let value2 = Vec::from("v2");

	// Set a key, delete it and read it in the same transaction. Should return None.
	{
		// Start a new read-write transaction (txn1)
		let mut txn1 = store.begin().unwrap();
		txn1.set(&key1, &value1).unwrap();
		txn1.delete(&key1).unwrap();
		let res = txn1.get(&key1).unwrap();
		assert!(res.is_none());
		txn1.commit().await.unwrap();
	}

	{
		let mut txn = store.begin().unwrap();
		txn.set(&key1, &value1).unwrap();
		txn.commit().await.unwrap();
	}

	{
		// Start a new read-write transaction (txn)
		let mut txn = store.begin().unwrap();
		txn.set(&key1, &value2).unwrap();
		assert_eq!(&txn.get(&key1).unwrap().unwrap(), &value2);
		assert!(txn.get(&key3).unwrap().is_none());
		txn.set(&key2, &value1).unwrap();
		assert_eq!(&txn.get(&key2).unwrap().unwrap(), &value1);
		txn.commit().await.unwrap();
	}
}

// Common setup logic for creating a store
async fn create_hermitage_store() -> Tree {
	let (store, _) = create_store();

	let key1 = Vec::from("k1");
	let key2 = Vec::from("k2");
	let value1 = Vec::from("v1");
	let value2 = Vec::from("v2");
	// Start a new read-write transaction (txn)
	let mut txn = store.begin().unwrap();
	txn.set(&key1, &value1).unwrap();
	txn.set(&key2, &value2).unwrap();
	txn.commit().await.unwrap();

	store
}

// The following tests are taken from hermitage (https://github.com/ept/hermitage)
// Specifically, the tests are derived from FoundationDB tests: https://github.com/ept/hermitage/blob/master/foundationdb.md

// G0: Write Cycles (dirty writes)
#[test(tokio::test)]
async fn g0_tests() {
	let store = create_hermitage_store().await;
	let key1 = Vec::from("k1");
	let key2 = Vec::from("k2");
	let value3 = Vec::from("v3");
	let value4 = Vec::from("v4");
	let value5 = Vec::from("v5");
	let value6 = Vec::from("v6");

	{
		let mut txn1 = store.begin().unwrap();
		let mut txn2 = store.begin().unwrap();

		assert!(txn1.get(&key1).is_ok());
		assert!(txn1.get(&key2).is_ok());
		assert!(txn2.get(&key1).is_ok());
		assert!(txn2.get(&key2).is_ok());

		txn1.set(&key1, &value3).unwrap();
		txn2.set(&key1, &value4).unwrap();

		txn1.set(&key2, &value5).unwrap();

		txn1.commit().await.unwrap();

		txn2.set(&key2, &value6).unwrap();
		assert!(match txn2.commit().await {
			Err(err) => {
				matches!(err, Error::TransactionWriteConflict)
			}
			_ => false,
		});
	}

	{
		let txn3 = store.begin().unwrap();
		let val1 = txn3.get(&key1).unwrap().unwrap();
		assert_eq!(&val1, &value3);
		let val2 = txn3.get(&key2).unwrap().unwrap();
		assert_eq!(&val2, &value5);
	}
}

// P4: Lost Update
#[test(tokio::test)]
async fn p4() {
	let store = create_hermitage_store().await;

	let key1 = Vec::from("k1");
	let value3 = Vec::from("v3");

	{
		let mut txn1 = store.begin().unwrap();
		let mut txn2 = store.begin().unwrap();

		assert!(txn1.get(&key1).is_ok());
		assert!(txn2.get(&key1).is_ok());

		txn1.set(&key1, &value3).unwrap();
		txn2.set(&key1, &value3).unwrap();

		txn1.commit().await.unwrap();

		assert!(match txn2.commit().await {
			Err(err) => {
				matches!(err, Error::TransactionWriteConflict)
			}
			_ => false,
		});
	}
}

// G-single: Single Anti-dependency Cycles (read skew)
async fn g_single_tests() {
	let store = create_hermitage_store().await;

	let key1 = Vec::from("k1");
	let key2 = Vec::from("k2");
	let value1 = Vec::from("v1");
	let value2 = Vec::from("v2");
	let value3 = Vec::from("v3");
	let value4 = Vec::from("v4");

	{
		let mut txn1 = store.begin().unwrap();
		let mut txn2 = store.begin().unwrap();

		assert_eq!(txn1.get(&key1).unwrap().unwrap(), value1);
		assert_eq!(txn2.get(&key1).unwrap().unwrap(), value1);
		assert_eq!(txn2.get(&key2).unwrap().unwrap(), value2);
		txn2.set(&key1, &value3).unwrap();
		txn2.set(&key2, &value4).unwrap();

		txn2.commit().await.unwrap();

		assert_eq!(txn1.get(&key2).unwrap().unwrap(), value2);
		txn1.commit().await.unwrap();
	}
}

#[test(tokio::test)]
async fn g_single() {
	g_single_tests().await;
}

fn require_send<T: Send>(_: T) {}
fn require_sync<T: Sync + Send>(_: T) {}

#[test(tokio::test)]
async fn is_send_sync() {
	let (db, _) = create_store();

	let txn = db.begin().unwrap();
	require_send(txn);

	let txn = db.begin().unwrap();
	require_sync(txn);
}

const ENTRIES: usize = 400_000;
const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 150;
const RNG_SEED: u64 = 3;

fn fill_slice(slice: &mut [u8], rng: &mut fastrand::Rng) {
	let mut i = 0;
	while i + size_of::<u128>() < slice.len() {
		let tmp = rng.u128(..);
		slice[i..(i + size_of::<u128>())].copy_from_slice(&tmp.to_be_bytes());
		i += size_of::<u128>()
	}
	if i + size_of::<u64>() < slice.len() {
		let tmp = rng.u64(..);
		slice[i..(i + size_of::<u64>())].copy_from_slice(&tmp.to_be_bytes());
		i += size_of::<u64>()
	}
	if i + size_of::<u32>() < slice.len() {
		let tmp = rng.u32(..);
		slice[i..(i + size_of::<u32>())].copy_from_slice(&tmp.to_be_bytes());
		i += size_of::<u32>()
	}
	if i + size_of::<u16>() < slice.len() {
		let tmp = rng.u16(..);
		slice[i..(i + size_of::<u16>())].copy_from_slice(&tmp.to_be_bytes());
		i += size_of::<u16>()
	}
	if i + size_of::<u8>() < slice.len() {
		slice[i] = rng.u8(..);
	}
}

fn gen_pair(rng: &mut fastrand::Rng) -> ([u8; KEY_SIZE], Vec<u8>) {
	let mut key = [0u8; KEY_SIZE];
	fill_slice(&mut key, rng);
	let mut value = vec![0u8; VALUE_SIZE];
	fill_slice(&mut value, rng);

	(key, value)
}

fn make_rng() -> fastrand::Rng {
	fastrand::Rng::with_seed(RNG_SEED)
}

#[test(tokio::test)]
#[ignore]
async fn insert_large_txn_and_get() {
	let store = create_hermitage_store().await;

	let mut rng = make_rng();

	let mut txn = store.begin().unwrap();
	for _ in 0..ENTRIES {
		let (key, value) = gen_pair(&mut rng);
		txn.set(&key, &value).unwrap();
	}
	txn.commit().await.unwrap();
	drop(txn);

	// Read the keys from the store
	let mut rng = make_rng();
	let txn = store.begin_with_mode(Mode::ReadOnly).unwrap();
	for _i in 0..ENTRIES {
		let (key, _) = gen_pair(&mut rng);
		txn.get(&key).unwrap();
	}
}

#[test(tokio::test)]
async fn sdb_delete_record_id_bug() {
	let (store, _) = create_store();

	// Define key-value pairs for the test
	let key1 = Vec::from(&[
		47, 33, 110, 100, 166, 192, 229, 30, 101, 24, 73, 242, 185, 36, 233, 242, 54, 96, 72, 52,
	]);
	let key2 = Vec::from(&[
		47, 33, 104, 98, 0, 0, 1, 141, 141, 42, 113, 8, 47, 166, 192, 229, 30, 101, 24, 73, 242,
		185, 36, 233, 242, 54, 96, 72, 52,
	]);
	let value1 = Vec::from("baz");

	{
		// Start a new read-write transaction (txn)
		let mut txn = store.begin().unwrap();
		txn.set(&key1, &value1).unwrap();
		txn.set(&key2, &value1).unwrap();
		txn.commit().await.unwrap();
	}

	let key3 = Vec::from(&[47, 33, 117, 115, 114, 111, 111, 116, 0]);
	{
		// Start a new read-write transaction (txn)
		let mut txn = store.begin().unwrap();
		txn.set(&key3, &value1).unwrap();
		txn.commit().await.unwrap();
	}

	let key4 = Vec::from(&[47, 33, 117, 115, 114, 111, 111, 116, 0]);
	let txn1 = store.begin().unwrap();
	txn1.get(&key4).unwrap();

	{
		let mut txn2 = store.begin().unwrap();
		txn2.get(Vec::from(&[
			47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72, 50,
			50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 33, 116, 98,
			117, 115, 101, 114, 0,
		]))
		.unwrap();
		txn2.get(Vec::from(&[47, 33, 110, 115, 116, 101, 115, 116, 45, 110, 115, 0])).unwrap();
		txn2.get(Vec::from(&[47, 33, 110, 115, 116, 101, 115, 116, 45, 110, 115, 0])).unwrap();
		txn2.set(Vec::from(&[47, 33, 110, 115, 116, 101, 115, 116, 45, 110, 115, 0]), &value1)
			.unwrap();

		txn2.get(Vec::from(&[
			47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72, 71,
			72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
		]))
		.unwrap();
		txn2.get(Vec::from(&[
			47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72, 71,
			72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
		]))
		.unwrap();
		txn2.set(
			Vec::from(&[
				47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72,
				71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
			]),
			&value1,
		)
		.unwrap();

		txn2.get(Vec::from(&[
			47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72, 50,
			50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 33, 116, 98,
			117, 115, 101, 114, 0,
		]))
		.unwrap();
		txn2.get(Vec::from(&[
			47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72, 50,
			50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 42, 117, 115,
			101, 114, 0, 42, 0, 0, 0, 1, 106, 111, 104, 110, 0,
		]))
		.unwrap();
		txn2.set(
			Vec::from(&[
				47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72,
				50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 42, 117,
				115, 101, 114, 0, 42, 0, 0, 0, 1, 106, 111, 104, 110, 0,
			]),
			&value1,
		)
		.unwrap();

		txn2.get(Vec::from(&[
			47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72, 71,
			72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
		]))
		.unwrap();

		txn2.commit().await.unwrap();
	}

	{
		let mut txn3 = store.begin().unwrap();
		txn3.get(Vec::from(&[
			47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72, 50,
			50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 42, 117, 115,
			101, 114, 0, 42, 0, 0, 0, 1, 106, 111, 104, 110, 0,
		]))
		.unwrap();
		txn3.get(Vec::from(&[
			47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72, 50,
			50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 33, 116, 98,
			117, 115, 101, 114, 0,
		]))
		.unwrap();
		txn3.delete(Vec::from(&[
			47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72, 50,
			50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 42, 117, 115,
			101, 114, 0, 42, 0, 0, 0, 1, 106, 111, 104, 110, 0,
		]))
		.unwrap();
		txn3.get(Vec::from(&[
			47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72, 71,
			72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
		]))
		.unwrap();
		txn3.get(Vec::from(&[
			47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72, 71,
			72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
		]))
		.unwrap();
		txn3.set(
			Vec::from(&[
				47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72,
				71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
			]),
			&value1,
		)
		.unwrap();
		txn3.commit().await.unwrap();
	}
}

#[test(tokio::test)]
async fn transaction_delete_from_index() {
	let (store, _) = create_store();

	// Define key-value pairs for the test
	let key1 = Vec::from("foo1");
	let value = Vec::from("baz");
	let key2 = Vec::from("foo2");

	{
		// Start a new read-write transaction (txn1)
		let mut txn1 = store.begin().unwrap();
		txn1.set(&key1, &value).unwrap();
		txn1.set(&key2, &value).unwrap();
		txn1.commit().await.unwrap();
	}

	{
		// Start another read-write transaction (txn2)
		let mut txn2 = store.begin().unwrap();
		txn2.delete(&key1).unwrap();
		txn2.commit().await.unwrap();
	}

	{
		// Start a read-only transaction (txn3)
		let txn3 = store.begin().unwrap();
		let val = txn3.get(&key1).unwrap();
		assert!(val.is_none());
		let val = txn3.get(&key2).unwrap().unwrap();
		assert_eq!(&val, &value);
	}

	// Start a read-only transaction (txn4)
	let txn4 = store.begin().unwrap();
	let val = txn4.get(&key1).unwrap();
	assert!(val.is_none());
	let val = txn4.get(&key2).unwrap().unwrap();
	assert_eq!(&val, &value);
}

#[test(tokio::test)]
async fn test_insert_delete_read_key() {
	let (store, _) = create_store();

	// Key-value pair for the test
	let key = Vec::from("test_key");
	let value1 = Vec::from("test_value1");
	let value2 = Vec::from("test_value2");

	// Insert key-value pair in a new transaction
	{
		let mut txn = store.begin().unwrap();
		txn.set(&key, &value1).unwrap();
		txn.commit().await.unwrap();
	}

	{
		let mut txn = store.begin().unwrap();
		txn.set(&key, &value2).unwrap();
		txn.commit().await.unwrap();
	}

	// Clear the key in a separate transaction
	{
		let mut txn = store.begin().unwrap();
		txn.delete(&key).unwrap();
		txn.commit().await.unwrap();
	}

	// Read the key in a new transaction to verify it does not exist
	{
		let txn = store.begin().unwrap();
		assert!(txn.get(&key).unwrap().is_none());
	}
}

#[test(tokio::test)]
async fn test_range_basic_functionality() {
	let (store, _temp_dir) = create_store();

	// Insert some initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.set(b"key2", b"value2").unwrap();
		tx.set(b"key3", b"value3").unwrap();
		tx.set(b"key4", b"value4").unwrap();
		tx.set(b"key5", b"value5").unwrap();
		tx.commit().await.unwrap();
	}

	// Test basic range scan
	{
		let tx = store.begin().unwrap();
		let range = collect_transaction_all(&mut tx.range(b"key2", b"key4").unwrap()).unwrap();

		assert_eq!(range.len(), 2); // key2, key3 (key4 is exclusive)
		assert_eq!(&range[0].0, b"key2");
		assert_eq!(&range[0].1.as_ref().unwrap().as_slice(), b"value2");
		assert_eq!(&range[1].0, b"key3");
		assert_eq!(&range[1].1.as_ref().unwrap().as_slice(), b"value3");
	}
}

#[test(tokio::test)]
async fn test_range_with_bounds() {
	let (store, _temp_dir) = create_store();

	// Insert some initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.set(b"key2", b"value2").unwrap();
		tx.set(b"key3", b"value3").unwrap();
		tx.set(b"key4", b"value4").unwrap();
		tx.set(b"key5", b"value5").unwrap();
		tx.commit().await.unwrap();
	}

	// Test range with start bound as empty
	{
		let tx = store.begin().unwrap();
		let beg = b"".as_slice();
		let range = collect_transaction_all(&mut tx.range(beg, b"key4").unwrap()).unwrap();
		assert_eq!(range.len(), 3); // key1, key2, key3 (key4 is exclusive)
		assert_eq!(&range[0].0, b"key1");
		assert_eq!(&range[1].0, b"key2");
		assert_eq!(&range[2].0, b"key3");
	}

	// Test range with both bounds as empty
	{
		let tx = store.begin().unwrap();
		let beg = b"".as_slice();
		let end = b"".as_slice();
		let range = collect_transaction_all(&mut tx.range(beg, end).unwrap()).unwrap();
		assert_eq!(range.len(), 0);
	}
}

#[test(tokio::test)]
async fn test_range_with_limit() {
	let (store, _temp_dir) = create_store();

	// Insert test data
	{
		let mut tx = store.begin().unwrap();
		for i in 1..=10 {
			let key = format!("key{i:02}");
			let value = format!("value{i}");
			tx.set(key.as_bytes(), value.as_bytes()).unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Test with .take()
	{
		let tx = store.begin().unwrap();
		let all_range =
			collect_transaction_all(&mut tx.range(b"key01", b"key10").unwrap()).unwrap();
		let range: Vec<_> = all_range.into_iter().take(3).collect::<Vec<_>>();

		assert_eq!(range.len(), 3);
		assert_eq!(&range[0].0, b"key01");
		assert_eq!(&range[1].0, b"key02");
		assert_eq!(&range[2].0, b"key03");
	}
}

#[test(tokio::test)]
async fn test_range_read_your_own_writes() {
	let (store, _temp_dir) = create_store();

	// Insert initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"a", b"1").unwrap();
		tx.set(b"c", b"3").unwrap();
		tx.set(b"e", b"5").unwrap();
		tx.commit().await.unwrap();
	}

	// Test RYOW - uncommitted writes should be visible in range
	{
		let mut tx = store.begin().unwrap();

		// Add new keys
		tx.set(b"b", b"2").unwrap();
		tx.set(b"d", b"4").unwrap();

		// Modify existing key
		tx.set(b"c", b"3_modified").unwrap();

		// Range should see all changes ([a, f) to include e)
		let range = collect_transaction_all(&mut tx.range(b"a", b"f").unwrap()).unwrap();

		assert_eq!(range.len(), 5);
		assert_eq!(range[0], (Vec::from(b"a"), Some(Vec::from(b"1"))));
		assert_eq!(range[1], (Vec::from(b"b"), Some(Vec::from(b"2"))));
		assert_eq!(range[2], (Vec::from(b"c"), Some(Vec::from(b"3_modified"))));
		assert_eq!(range[3], (Vec::from(b"d"), Some(Vec::from(b"4"))));
		assert_eq!(range[4], (Vec::from(b"e"), Some(Vec::from(b"5"))));
	}
}

#[test(tokio::test)]
async fn test_range_with_deletes() {
	let (store, _temp_dir) = create_store();

	// Insert initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.set(b"key2", b"value2").unwrap();
		tx.set(b"key3", b"value3").unwrap();
		tx.set(b"key4", b"value4").unwrap();
		tx.set(b"key5", b"value5").unwrap();
		tx.commit().await.unwrap();
	}

	// Test range with deletes in write set
	{
		let mut tx = store.begin().unwrap();

		// Delete some keys
		tx.delete(b"key2").unwrap();
		tx.delete(b"key4").unwrap();

		// Range should not see deleted keys ([key1, key6) to include key5)
		let range = collect_transaction_all(&mut tx.range(b"key1", b"key6").unwrap()).unwrap();

		assert_eq!(range.len(), 3);
		assert_eq!(&range[0].0, b"key1");
		assert_eq!(&range[1].0, b"key3");
		assert_eq!(&range[2].0, b"key5");
	}
}

#[test(tokio::test)]
async fn test_range_delete_then_set() {
	let (store, _temp_dir) = create_store();

	// Insert initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.set(b"key2", b"value2").unwrap();
		tx.set(b"key3", b"value3").unwrap();
		tx.commit().await.unwrap();
	}

	// Test delete followed by set
	{
		let mut tx = store.begin().unwrap();

		// Delete then re-add with new value
		tx.delete(b"key2").unwrap();
		tx.set(b"key2", b"new_value2").unwrap();

		// Range should see the new value ([key1, key4) to include key3)
		let range = collect_transaction_all(&mut tx.range(b"key1", b"key4").unwrap()).unwrap();

		assert_eq!(range.len(), 3);
		assert_eq!(range[1], (Vec::from(b"key2"), Some(Vec::from(b"new_value2"))));
	}
}

#[test(tokio::test)]
async fn test_range_empty_result() {
	let (store, _temp_dir) = create_store();

	// Insert data outside the range we'll query
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"a", b"1").unwrap();
		tx.set(b"z", b"26").unwrap();
		tx.commit().await.unwrap();
	}

	// Query range with no data
	{
		let tx = store.begin().unwrap();
		let range = collect_transaction_all(&mut tx.range(b"m", b"n").unwrap()).unwrap();

		assert_eq!(range.len(), 0);
	}
}

#[test(tokio::test)]
async fn test_range_ordering() {
	let (store, _temp_dir) = create_store();

	// Insert data in non-sequential order
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key5", b"value5").unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.set(b"key3", b"value3").unwrap();
		tx.set(b"key2", b"value2").unwrap();
		tx.set(b"key4", b"value4").unwrap();
		tx.commit().await.unwrap();
	}

	// Verify correct ordering in range ([key1, key6) to include key5)
	{
		let tx = store.begin().unwrap();
		let range = collect_transaction_all(&mut tx.range(b"key1", b"key6").unwrap()).unwrap();

		assert_eq!(range.len(), 5);
		for (i, item) in range.iter().enumerate().take(5) {
			let expected_key = format!("key{}", i + 1);
			assert_eq!(&item.0, expected_key.as_bytes());
		}
	}
}

#[test(tokio::test)]
async fn test_range_boundary_conditions() {
	let (store, _temp_dir) = create_store();

	// Insert test data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.set(b"key2", b"value2").unwrap();
		tx.set(b"key3", b"value3").unwrap();
		tx.commit().await.unwrap();
	}

	// Test range boundaries ([key1, key4) to include key3)
	{
		let tx = store.begin().unwrap();

		// Range includes start but excludes end
		let range = collect_transaction_all(&mut tx.range(b"key1", b"key4").unwrap()).unwrap();

		assert_eq!(range.len(), 3);
		assert_eq!(&range[0].0, b"key1");
		assert_eq!(&range[2].0, b"key3");
	}

	// Test single key range ([key2, key3) to include only key2)
	{
		let tx = store.begin().unwrap();
		let range = collect_transaction_all(&mut tx.range(b"key2", b"key3").unwrap()).unwrap();

		assert_eq!(range.len(), 1);
		assert_eq!(&range[0].0, b"key2");
	}
}

#[test(tokio::test)]
async fn test_range_write_sequence_order() {
	let (store, _temp_dir) = create_store();

	// Test that multiple writes to same key show latest value
	{
		let mut tx = store.begin().unwrap();

		tx.set(b"key", b"value1").unwrap();
		tx.set(b"key", b"value2").unwrap();
		tx.set(b"key", b"value3").unwrap();

		let end_key = b"key\x01";
		let range =
			collect_transaction_all(&mut tx.range(b"key".as_slice(), end_key.as_slice()).unwrap())
				.unwrap();

		assert_eq!(range.len(), 1);
		assert_eq!(range[0].1.as_ref().map(|v| v.as_slice()), Some(b"value3".as_slice())); // Latest value
	}
}

#[test(tokio::test)]
async fn test_keys_method() {
	let (store, _temp_dir) = create_store();

	// Insert test data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.set(b"key2", b"value2").unwrap();
		tx.set(b"key3", b"value3").unwrap();
		tx.set(b"key4", b"value4").unwrap();
		tx.set(b"key5", b"value5").unwrap();
		tx.commit().await.unwrap();
	}

	// Test with RYOW - add a key in the current transaction
	{
		let mut tx = store.begin().unwrap();

		// Add a key in the transaction (not yet committed)
		tx.set(b"key6", b"value6").unwrap();

		// Get keys only
		let all_range = collect_transaction_all(&mut tx.range(b"key1", b"key9").unwrap()).unwrap();
		let keys_only: Vec<_> = all_range.into_iter().map(|(k, _)| k).collect();

		// Verify we got all 6 keys (5 from storage + 1 from write set)
		assert_eq!(keys_only.len(), 6);

		// Check the keys are in order
		for (i, key) in keys_only.iter().enumerate().take(6) {
			let expected_key = format!("key{}", i + 1);
			assert_eq!(key.as_slice(), expected_key.as_bytes());
		}

		// Compare with regular range
		let regular_range =
			collect_transaction_all(&mut tx.range(b"key1", b"key9").unwrap()).unwrap();

		// Should have same number of items
		assert_eq!(regular_range.len(), keys_only.len());

		// Keys should match and regular range values should be correct
		for i in 0..keys_only.len() {
			assert_eq!(keys_only[i], regular_range[i].0, "Keys should match");

			if i < 5 {
				// For keys from storage, check regular values are correct
				assert_eq!(
					regular_range[i].1.as_ref().unwrap().as_slice(),
					format!("value{}", i + 1).as_bytes(),
					"Regular range should have correct values from storage"
				);
			} else {
				// For the key from write set
				assert_eq!(
					regular_range[i].1.as_ref().unwrap().as_slice(),
					b"value6",
					"Regular range should have correct value from write set"
				);
			}
		}

		// Test with a deleted key
		tx.delete(b"key3").unwrap();

		let all_range_after_delete =
			collect_transaction_all(&mut tx.range(b"key1", b"key9").unwrap()).unwrap();
		let keys_after_delete: Vec<_> =
			all_range_after_delete.into_iter().map(|(k, _)| k).collect();

		// Should have 5 keys now (key3 is deleted)
		assert_eq!(keys_after_delete.len(), 5);

		// Verify key3 is not in the results
		let key_names: Vec<_> = keys_after_delete
			.iter()
			.map(|k| String::from_utf8_lossy(k.as_ref()).to_string())
			.collect();

		assert!(!key_names.contains(&"key3".to_string()), "key3 should be removed");
	}
}

#[test(tokio::test)]
async fn test_range_value_pointer_resolution_bug() {
	let temp_dir = create_temp_directory();

	let tree = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_max_memtable_size(64 * 1024)
		.build()
		.unwrap();

	// Create values that will be stored in VLog (> 50 bytes)
	let key1 = b"key1";
	let key2 = b"key2";
	let key3 = b"key3";

	let large_value1 = "X".repeat(100); // > 50 bytes, goes to VLog
	let large_value2 = "Y".repeat(100); // > 50 bytes, goes to VLog
	let large_value3 = "Z".repeat(100); // > 50 bytes, goes to VLog

	// Insert the values
	{
		let mut txn = tree.begin().unwrap();
		txn.set(key1, large_value1.as_bytes()).unwrap();
		txn.set(key2, large_value2.as_bytes()).unwrap();
		txn.set(key3, large_value3.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Force flush to ensure data goes to SSTables (and VLog)
	tree.flush().unwrap();

	// Test 1: Verify get() works correctly
	{
		let txn = tree.begin().unwrap();

		let retrieved1 = txn.get(key1).unwrap().unwrap();
		let retrieved2 = txn.get(key2).unwrap().unwrap();
		let retrieved3 = txn.get(key3).unwrap().unwrap();

		assert_eq!(
			retrieved1.as_slice(),
			large_value1.as_bytes(),
			"get() should resolve value pointers correctly"
		);
		assert_eq!(
			retrieved2.as_slice(),
			large_value2.as_bytes(),
			"get() should resolve value pointers correctly"
		);
		assert_eq!(
			retrieved3.as_slice(),
			large_value3.as_bytes(),
			"get() should resolve value pointers correctly"
		);
	}

	// Test 2: Verify range() also works correctly
	{
		let txn = tree.begin().unwrap();

		let range_results =
			collect_transaction_all(&mut txn.range(b"key1", b"key4").unwrap()).unwrap();

		assert_eq!(range_results.len(), 3, "Should get 3 items from range query");

		// Check that all values are correctly resolved (not value pointers)
		for (i, (returned_key, returned_value)) in range_results.iter().enumerate() {
			let expected_key = match i {
				0 => key1,
				1 => key2,
				2 => key3,
				_ => panic!("Unexpected index"),
			};
			let expected_value = match i {
				0 => &large_value1,
				1 => &large_value2,
				2 => &large_value3,
				_ => panic!("Unexpected index"),
			};

			assert_eq!(returned_key.as_slice(), expected_key, "Key mismatch in range result");

			// The returned value should be the actual value, not a value pointer
			assert_eq!(
				returned_value.as_ref().unwrap().as_slice(),
				expected_value.as_bytes(),
				"Range should return resolved values, not value pointers. \
                     Expected actual value of {} bytes, but got a different value",
				expected_value.len(),
			);
		}
	}
}

// Double-ended iterator tests
mod double_ended_iterator_tests {
	use test_log::test;

	use super::*;

	#[test(tokio::test)]
	async fn test_reverse_iteration_basic() {
		let (store, _temp_dir) = create_store();

		// Insert test data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.set(b"key5", b"value5").unwrap();
			tx.commit().await.unwrap();
		}

		// Test reverse iteration
		{
			let tx = store.begin().unwrap();
			let reverse_results =
				collect_transaction_reverse(&mut tx.range(b"key1", b"key6").unwrap()).unwrap();

			// Should get results in reverse key order
			assert_eq!(reverse_results.len(), 5);

			// Check that keys are in reverse order
			let keys: Vec<Vec<u8>> = reverse_results.into_iter().map(|(k, _)| k).collect();

			assert_eq!(keys[0], b"key5");
			assert_eq!(keys[1], b"key4");
			assert_eq!(keys[2], b"key3");
			assert_eq!(keys[3], b"key2");
			assert_eq!(keys[4], b"key1");
		}
	}

	#[test(tokio::test)]
	async fn test_reverse_iteration_with_writes() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key5", b"value5").unwrap();
			tx.commit().await.unwrap();
		}

		// Test reverse iteration with transaction writes
		{
			let mut tx = store.begin().unwrap();

			// Add some writes to the transaction
			tx.set(b"key2", b"new_value2").unwrap();
			tx.set(b"key4", b"new_value4").unwrap();
			tx.set(b"key6", b"new_value6").unwrap();

			let reverse_results =
				collect_transaction_reverse(&mut tx.range(b"key1", b"key7").unwrap()).unwrap();

			// Should get results in reverse key order including transaction writes
			assert_eq!(reverse_results.len(), 6);

			// Check that keys are in reverse order
			let keys: Vec<Vec<u8>> = reverse_results.into_iter().map(|(k, _)| k).collect();

			assert_eq!(keys[0], b"key6");
			assert_eq!(keys[1], b"key5");
			assert_eq!(keys[2], b"key4");
			assert_eq!(keys[3], b"key3");
			assert_eq!(keys[4], b"key2");
			assert_eq!(keys[5], b"key1");
		}
	}

	#[test(tokio::test)]
	async fn test_reverse_iteration_with_deletes() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.set(b"key5", b"value5").unwrap();
			tx.commit().await.unwrap();
		}

		// Test reverse iteration with deletes in transaction
		{
			let mut tx = store.begin().unwrap();

			// Delete some keys
			tx.delete(b"key2").unwrap();
			tx.delete(b"key4").unwrap();

			let reverse_results =
				collect_transaction_reverse(&mut tx.range(b"key1", b"key6").unwrap()).unwrap();

			// Should get results in reverse key order, excluding deleted keys
			assert_eq!(reverse_results.len(), 3);

			// Check that keys are in reverse order and deleted keys are excluded
			let keys: Vec<Vec<u8>> = reverse_results.into_iter().map(|(k, _)| k).collect();

			assert_eq!(keys[0], b"key5");
			assert_eq!(keys[1], b"key3");
			assert_eq!(keys[2], b"key1");

			// Ensure deleted keys are not present
			assert!(!keys.contains(&b"key2".to_vec()));
			assert!(!keys.contains(&b"key4".to_vec()));
		}
	}

	#[test(tokio::test)]
	async fn test_reverse_iteration_with_soft_deletes() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.set(b"key5", b"value5").unwrap();
			tx.commit().await.unwrap();
		}

		// Test reverse iteration with soft deletes in transaction
		{
			let mut tx = store.begin().unwrap();

			// Soft delete some keys
			tx.soft_delete(b"key2").unwrap();
			tx.soft_delete(b"key4").unwrap();

			let reverse_results =
				collect_transaction_reverse(&mut tx.range(b"key1", b"key6").unwrap()).unwrap();

			// Should get results in reverse key order, excluding soft deleted keys
			assert_eq!(reverse_results.len(), 3);

			// Check that keys are in reverse order and soft deleted keys are excluded
			let keys: Vec<Vec<u8>> = reverse_results.into_iter().map(|(k, _)| k).collect();

			assert_eq!(keys[0], b"key5");
			assert_eq!(keys[1], b"key3");
			assert_eq!(keys[2], b"key1");

			// Ensure soft deleted keys are not present
			assert!(!keys.contains(&b"key2".to_vec()));
			assert!(!keys.contains(&b"key4".to_vec()));
		}
	}

	#[test(tokio::test)]
	async fn test_reverse_iteration_with_limits() {
		let (store, _temp_dir) = create_store();

		// Insert test data
		{
			let mut tx = store.begin().unwrap();
			for i in 1..=10 {
				let key = format!("key{:02}", i);
				let value = format!("value{}", i);
				tx.set(key.as_bytes(), value.as_bytes()).unwrap();
			}
			tx.commit().await.unwrap();
		}

		// Test reverse iteration with take
		{
			let tx = store.begin().unwrap();
			let reverse_results =
				collect_transaction_reverse(&mut tx.range(b"key01", b"key11").unwrap()).unwrap();

			// Take only first 3 results (which are the highest keys in reverse order)
			let limited_results: Vec<_> = reverse_results.into_iter().take(3).collect();

			// Should get exactly 3 results in reverse order
			assert_eq!(limited_results.len(), 3);

			// Check that keys are in reverse order
			let keys: Vec<Vec<u8>> = limited_results.into_iter().map(|(k, _)| k).collect();

			assert_eq!(keys[0], b"key10");
			assert_eq!(keys[1], b"key09");
			assert_eq!(keys[2], b"key08");
		}
	}

	#[test(tokio::test)]
	async fn test_reverse_iteration_keys_only() {
		let (store, _temp_dir) = create_store();

		// Insert test data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.commit().await.unwrap();
		}

		// Test reverse iteration with keys only
		{
			let tx = store.begin().unwrap();
			let reverse_results =
				collect_transaction_reverse(&mut tx.range(b"key1", b"key4").unwrap()).unwrap();

			// Should get results in reverse key order
			assert_eq!(reverse_results.len(), 3);

			// Check that keys are in reverse order
			let keys: Vec<Vec<u8>> = reverse_results.into_iter().map(|(k, _)| k).collect();

			assert_eq!(keys[0], b"key3");
			assert_eq!(keys[1], b"key2");
			assert_eq!(keys[2], b"key1");
		}
	}

	#[test(tokio::test)]
	async fn test_reverse_iteration_mixed_operations() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.set(b"key5", b"value5").unwrap();
			tx.commit().await.unwrap();
		}

		// Test reverse iteration with mixed operations in transaction
		{
			let mut tx = store.begin().unwrap();

			// Mix of operations
			tx.set(b"key0", b"new_value0").unwrap(); // New key
			tx.set(b"key2", b"updated_value2").unwrap(); // Update existing
			tx.delete(b"key3").unwrap(); // Delete existing
			tx.soft_delete(b"key4").unwrap(); // Soft delete existing
			tx.set(b"key6", b"new_value6").unwrap(); // New key

			let reverse_results =
				collect_transaction_reverse(&mut tx.range(b"key0", b"key7").unwrap()).unwrap();

			// Should get results in reverse key order
			assert_eq!(reverse_results.len(), 5);

			// Check that keys are in reverse order
			let keys: Vec<Vec<u8>> = reverse_results.into_iter().map(|(k, _)| k).collect();

			assert_eq!(keys[0], b"key6");
			assert_eq!(keys[1], b"key5");
			assert_eq!(keys[2], b"key2");
			assert_eq!(keys[3], b"key1");
			assert_eq!(keys[4], b"key0");

			// Ensure deleted keys are not present
			assert!(!keys.contains(&b"key3".to_vec()));
			assert!(!keys.contains(&b"key4".to_vec()));
		}
	}

	#[test(tokio::test)]
	async fn test_reverse_iteration_empty_range() {
		let (store, _temp_dir) = create_store();

		// Insert data outside the range we'll query
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key5", b"value5").unwrap();
			tx.commit().await.unwrap();
		}

		// Test reverse iteration on empty range
		{
			let tx = store.begin().unwrap();
			let reverse_results =
				collect_transaction_reverse(&mut tx.range(b"key2", b"key5").unwrap()).unwrap();

			// Should get no results
			assert_eq!(reverse_results.len(), 0);
		}
	}

	#[test(tokio::test)]
	async fn test_reverse_iteration_consistency_with_forward() {
		let (store, _temp_dir) = create_store();

		// Insert test data
		{
			let mut tx = store.begin().unwrap();
			for i in 1..=5 {
				let key = format!("key{}", i);
				let value = format!("value{}", i);
				tx.set(key.as_bytes(), value.as_bytes()).unwrap();
			}
			tx.commit().await.unwrap();
		}

		// Test that reverse iteration gives same results as forward iteration reversed
		{
			let tx = store.begin().unwrap();

			// Forward iteration
			let forward_results =
				collect_transaction_all(&mut tx.range(b"key1", b"key5").unwrap()).unwrap();

			// Reverse iteration
			let reverse_results =
				collect_transaction_reverse(&mut tx.range(b"key1", b"key5").unwrap()).unwrap();

			// Reverse the forward results
			let mut forward_reversed = forward_results;
			forward_reversed.reverse();

			// Results should be identical
			assert_eq!(forward_reversed.len(), reverse_results.len());
			for (forward, reverse) in forward_reversed.iter().zip(reverse_results.iter()) {
				assert_eq!(forward.0, reverse.0);
				assert_eq!(forward.1, reverse.1);
			}
		}
	}
}

// Savepoint tests
mod savepoint_tests {
	use test_log::test;

	use super::*;

	#[test(tokio::test)]
	async fn multiple_savepoints() {
		let (store, _) = create_store();

		// Key-value pair for the test
		let key1 = Vec::from("test_key1");
		let value1 = Vec::from("test_value1");
		let key2 = Vec::from("test_key2");
		let value2 = Vec::from("test_value2");
		let key3 = Vec::from("test_key3");
		let value3 = Vec::from("test_value3");

		// Start the transaction and write key1.
		let mut txn1 = store.begin().unwrap();
		txn1.set(&key1, &value1).unwrap();

		// Set the first savepoint.
		txn1.set_savepoint().unwrap();

		// Write key2 after the savepoint.
		txn1.set(&key2, &value2).unwrap();

		// Set another savepoint, stacking it onto the first one.
		txn1.set_savepoint().unwrap();

		txn1.set(&key3, &value3).unwrap();

		// Just a sanity check that all three keys are present.
		assert_eq!(&txn1.get(&key1).unwrap().unwrap(), &value1);
		assert_eq!(&txn1.get(&key2).unwrap().unwrap(), &value2);
		assert_eq!(&txn1.get(&key3).unwrap().unwrap(), &value3);

		// Rollback to the latest (second) savepoint. This should make key3
		// go away while keeping key1 and key2.
		txn1.rollback_to_savepoint().unwrap();
		assert_eq!(&txn1.get(&key1).unwrap().unwrap(), &value1);
		assert_eq!(&txn1.get(&key2).unwrap().unwrap(), &value2);
		assert!(txn1.get(&key3).unwrap().is_none());

		// Now roll back to the first savepoint. This should only
		// keep key1 around.
		txn1.rollback_to_savepoint().unwrap();
		assert_eq!(&txn1.get(&key1).unwrap().unwrap(), &value1);
		assert!(txn1.get(&key2).unwrap().is_none());
		assert!(txn1.get(&key3).unwrap().is_none());

		// Check that without any savepoints set the error is returned.
		assert!(matches!(txn1.rollback_to_savepoint(), Err(Error::TransactionWithoutSavepoint)));

		// Commit the transaction.
		txn1.commit().await.unwrap();
		drop(txn1);

		// Start another transaction and check again for the keys.
		let txn2 = store.begin().unwrap();
		assert_eq!(&txn2.get(&key1).unwrap().unwrap(), &value1);
		assert!(txn2.get(&key2).unwrap().is_none());
		assert!(txn2.get(&key3).unwrap().is_none());
	}

	#[test(tokio::test)]
	async fn savepoint_rollback_on_updated_key() {
		let (store, _) = create_store();

		let k1 = Vec::from("k1");
		let value1 = Vec::from("value1");
		let value2 = Vec::from("value2");
		let value3 = Vec::from("value3");

		let mut txn1 = store.begin().unwrap();
		txn1.set(&k1, &value1).unwrap();
		txn1.set(&k1, &value2).unwrap();
		txn1.set_savepoint().unwrap();
		txn1.set(&k1, &value3).unwrap();
		txn1.rollback_to_savepoint().unwrap();

		// The read value should be the one before the savepoint.
		assert_eq!(&txn1.get(&k1).unwrap().unwrap(), &value2);
	}

	#[test(tokio::test)]
	async fn savepoint_rollback_with_range_scan() {
		let (store, _) = create_store();

		let k1 = Vec::from("k1");
		let value = Vec::from("value1");
		let value2 = Vec::from("value2");

		let mut txn1 = store.begin().unwrap();
		txn1.set(&k1, &value).unwrap();
		txn1.set_savepoint().unwrap();
		txn1.set(&k1, &value2).unwrap();
		txn1.rollback_to_savepoint().unwrap();

		// The scanned value should be the one before the savepoint.
		let range = collect_transaction_all(&mut txn1.range(b"k1", b"k3").unwrap()).unwrap();
		assert_eq!(range.len(), 1);
		assert_eq!(&range[0].0, &k1);
		assert_eq!(range[0].1.as_ref().unwrap(), &value);
	}

	#[test(tokio::test)]
	async fn savepoint_with_deletes() {
		let (store, _) = create_store();

		let k1 = Vec::from("k1");
		let k2 = Vec::from("k2");
		let value1 = Vec::from("value1");
		let value2 = Vec::from("value2");

		let mut txn1 = store.begin().unwrap();
		txn1.set(&k1, &value1).unwrap();
		txn1.set(&k2, &value2).unwrap();
		txn1.set_savepoint().unwrap();

		// Delete k1 and modify k2
		txn1.delete(&k1).unwrap();
		txn1.set(&k2, b"modified").unwrap();

		// Verify the changes
		assert!(txn1.get(&k1).unwrap().is_none());
		assert_eq!(txn1.get(&k2).unwrap().unwrap(), b"modified");

		// Rollback to savepoint
		txn1.rollback_to_savepoint().unwrap();

		// Verify original values are restored
		assert_eq!(&txn1.get(&k1).unwrap().unwrap(), &value1);
		assert_eq!(&txn1.get(&k2).unwrap().unwrap(), &value2);
	}

	#[test(tokio::test)]
	async fn savepoint_nested_operations() {
		let (store, _) = create_store();

		let k1 = Vec::from("k1");
		let k2 = Vec::from("k2");
		let k3 = Vec::from("k3");
		let value1 = Vec::from("value1");
		let value2 = Vec::from("value2");
		let value3 = Vec::from("value3");

		let mut txn1 = store.begin().unwrap();
		txn1.set(&k1, &value1).unwrap();

		// First savepoint
		txn1.set_savepoint().unwrap();
		txn1.set(&k2, &value2).unwrap();

		// Second savepoint
		txn1.set_savepoint().unwrap();
		txn1.set(&k3, &value3).unwrap();

		// Rollback to second savepoint (should remove k3)
		txn1.rollback_to_savepoint().unwrap();
		assert_eq!(&txn1.get(&k1).unwrap().unwrap(), &value1);
		assert_eq!(&txn1.get(&k2).unwrap().unwrap(), &value2);
		assert!(txn1.get(&k3).unwrap().is_none());

		// Rollback to first savepoint (should remove k2)
		txn1.rollback_to_savepoint().unwrap();
		assert_eq!(&txn1.get(&k1).unwrap().unwrap(), &value1);
		assert!(txn1.get(&k2).unwrap().is_none());
		assert!(txn1.get(&k3).unwrap().is_none());

		// Final rollback should fail
		assert!(matches!(txn1.rollback_to_savepoint(), Err(Error::TransactionWithoutSavepoint)));
	}
}

#[test(tokio::test)]
async fn test_soft_delete_basic_functionality() {
	let (store, _temp_dir) = create_store();

	// Insert initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.set(b"key2", b"value2").unwrap();
		tx.set(b"key3", b"value3").unwrap();
		tx.commit().await.unwrap();
	}

	// Verify data is visible
	{
		let tx = store.begin().unwrap();
		assert_eq!(tx.get(b"key1").unwrap().unwrap(), b"value1");
		assert_eq!(tx.get(b"key2").unwrap().unwrap(), b"value2");
		assert_eq!(tx.get(b"key3").unwrap().unwrap(), b"value3");
	}

	// Soft delete key2
	{
		let mut tx = store.begin().unwrap();
		tx.soft_delete(b"key2").unwrap();
		tx.commit().await.unwrap();
	}

	// Verify soft deleted key is not visible in reads
	{
		let tx = store.begin().unwrap();
		assert_eq!(tx.get(b"key1").unwrap().unwrap(), b"value1");
		assert!(tx.get(b"key2").unwrap().is_none()); // Should be None after soft delete
		assert_eq!(tx.get(b"key3").unwrap().unwrap(), b"value3");
	}

	// Verify soft deleted key is not visible in range scans ([key1, key4) to
	// include key3)
	{
		let tx = store.begin().unwrap();
		let range = collect_transaction_all(&mut tx.range(b"key1", b"key4").unwrap()).unwrap();
		assert_eq!(range.len(), 2); // Only key1 and key3, key2 is filtered out
		assert_eq!(&range[0].0, b"key1");
		assert_eq!(&range[1].0, b"key3");
	}
}

#[test(tokio::test)]
async fn test_soft_delete_vs_hard_delete() {
	let (store, _temp_dir) = create_store();

	// Insert initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.set(b"key2", b"value2").unwrap();
		tx.set(b"key3", b"value3").unwrap();
		tx.commit().await.unwrap();
	}

	// Soft delete key1, hard delete key2
	{
		let mut tx = store.begin().unwrap();
		tx.soft_delete(b"key1").unwrap();
		tx.delete(b"key2").unwrap();
		tx.commit().await.unwrap();
	}

	// Both should be invisible to reads
	{
		let tx = store.begin().unwrap();
		assert!(tx.get(b"key1").unwrap().is_none()); // Soft deleted
		assert!(tx.get(b"key2").unwrap().is_none()); // Hard deleted
		assert_eq!(tx.get(b"key3").unwrap().unwrap(), b"value3");
	}

	// Both should be invisible to range scans ([key1, key4) to include key3)
	{
		let tx = store.begin().unwrap();
		let range = collect_transaction_all(&mut tx.range(b"key1", b"key4").unwrap()).unwrap();
		assert_eq!(range.len(), 1); // Only key3
		assert_eq!(&range[0].0, b"key3");
	}
}

#[test(tokio::test)]
async fn test_soft_delete_in_transaction_write_set() {
	let (store, _temp_dir) = create_store();

	// Insert initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.set(b"key2", b"value2").unwrap();
		tx.commit().await.unwrap();
	}

	// Start a transaction and soft delete within it
	{
		let mut tx = store.begin().unwrap();

		// Soft delete key1 within the transaction
		tx.soft_delete(b"key1").unwrap();

		// Within the same transaction, the soft delete should NOT be visible
		// The key should appear as if it doesn't exist
		assert!(tx.get(b"key1").unwrap().is_none());

		// Range scan within transaction should not see soft deleted key ([key1, key3)
		// to include key2)
		let range = collect_transaction_all(&mut tx.range(b"key1", b"key3").unwrap()).unwrap();
		assert_eq!(range.len(), 1); // Only key2
		assert_eq!(&range[0].0, b"key2");

		tx.commit().await.unwrap();
	}

	// After commit, soft deleted key should still be invisible
	{
		let tx = store.begin().unwrap();
		assert!(tx.get(b"key1").unwrap().is_none());
		assert_eq!(tx.get(b"key2").unwrap().unwrap(), b"value2");
	}
}

#[test(tokio::test)]
async fn test_soft_delete_then_reinsert() {
	let (store, _temp_dir) = create_store();

	// Insert initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.commit().await.unwrap();
	}

	// Soft delete the key
	{
		let mut tx = store.begin().unwrap();
		tx.soft_delete(b"key1").unwrap();
		tx.commit().await.unwrap();
	}

	// Verify it's not visible
	{
		let tx = store.begin().unwrap();
		assert!(tx.get(b"key1").unwrap().is_none());
	}

	// Re-insert the same key with a new value
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1_new").unwrap();
		tx.commit().await.unwrap();
	}

	// Verify the new value is visible
	{
		let tx = store.begin().unwrap();
		assert_eq!(tx.get(b"key1").unwrap().unwrap(), b"value1_new");
	}
}

#[test(tokio::test)]
async fn test_soft_delete_range_scan_filtering() {
	let (store, _temp_dir) = create_store();

	// Insert multiple keys
	{
		let mut tx = store.begin().unwrap();
		for i in 1..=10 {
			let key = format!("key{i:02}");
			let value = format!("value{i}");
			tx.set(key.as_bytes(), value.as_bytes()).unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Soft delete some keys
	{
		let mut tx = store.begin().unwrap();
		tx.soft_delete(b"key02").unwrap();
		tx.soft_delete(b"key05").unwrap();
		tx.soft_delete(b"key08").unwrap();
		tx.commit().await.unwrap();
	}

	// Range scan should not include soft deleted keys ([key01, key11) to include
	// key10)
	{
		let tx = store.begin().unwrap();
		let range = collect_transaction_all(&mut tx.range(b"key01", b"key11").unwrap()).unwrap();

		// Should have 7 keys (10 - 3 soft deleted)
		assert_eq!(range.len(), 7);

		// Verify specific keys are present/absent
		let keys: std::collections::HashSet<_> = range.iter().map(|(k, _)| k.as_slice()).collect();
		assert!(keys.contains(&b"key01".as_ref()));
		assert!(!keys.contains(&b"key02".as_ref())); // Soft deleted
		assert!(keys.contains(&b"key03".as_ref()));
		assert!(keys.contains(&b"key04".as_ref()));
		assert!(!keys.contains(&b"key05".as_ref())); // Soft deleted
		assert!(keys.contains(&b"key06".as_ref()));
		assert!(keys.contains(&b"key07".as_ref()));
		assert!(!keys.contains(&b"key08".as_ref())); // Soft deleted
		assert!(keys.contains(&b"key09".as_ref()));
		assert!(keys.contains(&b"key10".as_ref()));
	}
}

#[test(tokio::test)]
async fn test_soft_delete_mixed_with_other_operations() {
	let (store, _temp_dir) = create_store();

	// Insert initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.set(b"key2", b"value2").unwrap();
		tx.set(b"key3", b"value3").unwrap();
		tx.set(b"key4", b"value4").unwrap();
		tx.commit().await.unwrap();
	}

	// Mix of operations in one transaction
	{
		let mut tx = store.begin().unwrap();
		tx.soft_delete(b"key1").unwrap(); // Soft delete
		tx.delete(b"key2").unwrap(); // Hard delete
		tx.set(b"key3", b"value3_updated").unwrap(); // Update
											   // key4 remains unchanged
		tx.commit().await.unwrap();
	}

	// Verify results
	{
		let tx = store.begin().unwrap();
		assert!(tx.get(b"key1").unwrap().is_none()); // Soft deleted
		assert!(tx.get(b"key2").unwrap().is_none()); // Hard deleted
		assert_eq!(tx.get(b"key3").unwrap().unwrap(), b"value3_updated"); // Updated
		assert_eq!(tx.get(b"key4").unwrap().unwrap(), b"value4"); // Unchanged
	}

	// Range scan should only see updated and unchanged keys ([key1, key5) to
	// include key4)
	{
		let tx = store.begin().unwrap();
		let range = collect_transaction_all(&mut tx.range(b"key1", b"key5").unwrap()).unwrap();
		assert_eq!(range.len(), 2); // Only key3 and key4
		assert_eq!(&range[0].0, b"key3");
		assert_eq!(&range[1].0, b"key4");
	}
}

#[test(tokio::test)]
async fn test_soft_delete_rollback() {
	let (store, _temp_dir) = create_store();

	// Insert initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.commit().await.unwrap();
	}

	// Start transaction and soft delete, then rollback
	{
		let mut tx = store.begin().unwrap();
		tx.soft_delete(b"key1").unwrap();

		// Within transaction, key should be invisible
		assert!(tx.get(b"key1").unwrap().is_none());

		// Rollback the transaction
		tx.rollback();
	}

	// After rollback, key should be visible again
	{
		let tx = store.begin().unwrap();
		assert_eq!(tx.get(b"key1").unwrap().unwrap(), b"value1");
	}
}

#[test(tokio::test)]
async fn test_versioned_queries_basic() {
	let temp_dir = create_temp_directory();
	let opts: Options =
		Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
	let tree = TreeBuilder::with_options(opts).build().unwrap();

	// Use explicit timestamps for better testing
	let ts1 = 100; // First version timestamp
	let ts2 = 200; // Second version timestamp

	// Insert data with explicit timestamps
	let mut tx1 = tree.begin().unwrap();
	tx1.set_at_version(b"key1", b"value1_v1", ts1).unwrap();
	tx1.commit().await.unwrap();

	let mut tx2 = tree.begin().unwrap();
	tx2.set_at_version(b"key1", b"value1_v2", ts2).unwrap();
	tx2.commit().await.unwrap();

	// Test regular get (should return latest)
	let tx = tree.begin().unwrap();
	let value = tx.get(b"key1").unwrap();
	assert_eq!(value, Some(Vec::from(b"value1_v2")));

	// Get all versions to verify timestamps and values
	let versions = tx.scan_all_versions(b"key1", b"key2", None).unwrap();
	assert_eq!(versions.len(), 2);

	// Find versions by timestamp
	let v1 = versions.iter().find(|(_, _, timestamp, _)| *timestamp == ts1).unwrap();
	let v2 = versions.iter().find(|(_, _, timestamp, _)| *timestamp == ts2).unwrap();

	// Verify values match timestamps
	assert_eq!(&v1.1, b"value1_v1");
	assert_eq!(&v2.1, b"value1_v2");

	// Test get at specific timestamp (earlier version)
	let value_at_ts1 = tx.get_at_version(b"key1", ts1).unwrap();
	assert_eq!(value_at_ts1, Some(Vec::from(b"value1_v1")));

	// Test get at later timestamp (should return latest version as of that time)
	let value_at_ts2 = tx.get_at_version(b"key1", ts2).unwrap();
	assert_eq!(value_at_ts2, Some(Vec::from(b"value1_v2")));
}

#[test(tokio::test)]
async fn test_versioned_queries_with_deletes() {
	let temp_dir = create_temp_directory();
	let opts: Options =
		Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
	let clock = Arc::clone(&opts.clock);
	let tree = TreeBuilder::with_options(opts).build().unwrap();

	// Insert first version
	let mut tx1 = tree.begin().unwrap();
	tx1.set(b"key1", b"value1").unwrap();
	tx1.commit().await.unwrap();
	let ts1 = clock.now();

	// Update with second version
	let mut tx2 = tree.begin().unwrap();
	tx2.set(b"key1", b"value2").unwrap();
	tx2.commit().await.unwrap();
	let ts2 = clock.now();

	// Delete the key
	let mut tx3 = tree.begin().unwrap();
	tx3.soft_delete(b"key1").unwrap(); // Hard delete
	tx3.commit().await.unwrap();
	let ts3 = clock.now();

	// Test regular get (should return None due to delete)
	let tx = tree.begin().unwrap();
	let value = tx.get(b"key1").unwrap();
	assert_eq!(value, None);

	// Test scan_all_versions to get all versions including tombstones
	let all_versions = tx.scan_all_versions(b"key1", b"key2", None).unwrap();
	assert_eq!(all_versions.len(), 3);

	// Check values by timestamp
	let val1 = &all_versions[0];
	let val2 = &all_versions[1];
	assert!(val1.2 < val2.2);
	assert_eq!(&val1.1, b"value1");
	assert_eq!(&val2.1, b"value2");

	// Test range_at_version with specific timestamp to get point-in-time view
	let version_at_ts1 = tx
		.range_at_version(b"key1", b"key2", ts1)
		.unwrap()
		.collect::<std::result::Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(version_at_ts1.len(), 1);
	assert_eq!(&version_at_ts1[0].1, b"value1");

	let version_at_ts2 = tx
		.range_at_version(b"key1", b"key2", ts2)
		.unwrap()
		.collect::<std::result::Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(version_at_ts2.len(), 1);
	assert_eq!(&version_at_ts2[0].1, b"value2");

	// Test with timestamp after delete - should show nothing
	let version_at_ts3 = tx
		.range_at_version(b"key1", b"key2", ts3)
		.unwrap()
		.collect::<std::result::Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(version_at_ts3.len(), 0);
}

#[test(tokio::test)]
async fn test_set_at_timestamp() {
	let temp_dir = create_temp_directory();
	let opts: Options =
		Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
	let tree = TreeBuilder::with_options(opts).build().unwrap();

	// Set a value with a specific timestamp
	let custom_timestamp = 10;
	let mut tx = tree.begin().unwrap();
	tx.set_at_version(b"key1", b"value1", custom_timestamp).unwrap();
	tx.commit().await.unwrap();

	// Verify we can get the value at that timestamp
	let tx = tree.begin().unwrap();
	let value = tx.get_at_version(b"key1", custom_timestamp).unwrap();
	assert_eq!(value, Some(Vec::from(b"value1")));

	// Verify we can get the value at a later timestamp
	let later_timestamp = custom_timestamp + 1000000;
	let value = tx.get_at_version(b"key1", later_timestamp).unwrap();
	assert_eq!(value, Some(Vec::from(b"value1")));

	// Verify we can't get the value at an earlier timestamp
	let earlier_timestamp = custom_timestamp - 5;
	let value = tx.get_at_version(b"key1", earlier_timestamp).unwrap();
	assert_eq!(value, None);

	// Verify using scan_all_versions to check the timestamp
	let versions = tx.scan_all_versions(b"key1", b"key2", None).unwrap();
	assert_eq!(versions.len(), 1);
	assert_eq!(versions[0].2, custom_timestamp); // Check the timestamp
	assert_eq!(&versions[0].1, b"value1"); // Check the value
}

#[test(tokio::test)]
async fn test_timestamp_via_write_options() {
	let temp_dir = create_temp_directory();
	let opts: Options =
		Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
	let tree = TreeBuilder::with_options(opts).build().unwrap();

	// Test setting a value with timestamp via WriteOptions
	let custom_timestamp = 100;
	let mut tx = tree.begin().unwrap();
	tx.set_with_options(
		b"key1",
		b"value1",
		&WriteOptions::default().with_timestamp(Some(custom_timestamp)),
	)
	.unwrap();
	tx.commit().await.unwrap();

	// Verify we can read it at that timestamp
	let tx = tree.begin().unwrap();
	let value = tx
		.get_with_options(b"key1", &ReadOptions::default().with_timestamp(Some(custom_timestamp)))
		.unwrap();
	assert_eq!(value, Some(Vec::from(b"value1")));

	// Test soft_delete_with_options with timestamp
	let delete_timestamp = 200;
	let mut tx = tree.begin().unwrap();
	tx.soft_delete_with_options(
		b"key1",
		&WriteOptions::default().with_timestamp(Some(delete_timestamp)),
	)
	.unwrap();
	tx.commit().await.unwrap();

	// Verify the value exists at the earlier timestamp but not at the delete
	// timestamp
	let tx = tree.begin().unwrap();
	let value_before = tx
		.get_at_version_with_options(
			b"key1",
			&ReadOptions::default().with_timestamp(Some(custom_timestamp)),
		)
		.unwrap();
	assert_eq!(value_before, Some(Vec::from(b"value1")));

	let value_after = tx
		.get_at_version_with_options(
			b"key1",
			&ReadOptions::default().with_timestamp(Some(delete_timestamp)),
		)
		.unwrap();
	assert_eq!(value_after, None);
}

#[test(tokio::test)]
async fn test_commit_timestamp_consistency() {
	let temp_dir = create_temp_directory();
	let opts: Options =
		Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
	let tree = TreeBuilder::with_options(opts).build().unwrap();

	// Set multiple values in a single transaction
	let mut tx = tree.begin().unwrap();
	tx.set(b"key1", b"value1").unwrap();
	tx.set(b"key2", b"value2").unwrap();
	tx.set(b"key3", b"value3").unwrap();
	tx.commit().await.unwrap();

	// All keys should have the same timestamp
	let tx = tree.begin().unwrap();
	let versions1 = tx.scan_all_versions(b"key1", b"key2", None).unwrap();
	let versions2 = tx.scan_all_versions(b"key2", b"key3", None).unwrap();
	let versions3 = tx.scan_all_versions(b"key3", b"key4", None).unwrap();

	assert_eq!(versions1.len(), 1);
	assert_eq!(versions2.len(), 1);
	assert_eq!(versions3.len(), 1);

	// Compare timestamps (now the third element in the tuple)
	let ts1 = versions1[0].2;
	let ts2 = versions2[0].2;
	let ts3 = versions3[0].2;
	assert_eq!(ts1, ts2);
	assert_eq!(ts2, ts3);

	// Test mixed explicit and implicit timestamps
	let custom_timestamp = 9876543210000000000;
	let mut tx = tree.begin().unwrap();
	tx.set(b"key4", b"value4").unwrap(); // Will get commit timestamp
	tx.set_at_version(b"key5", b"value5", custom_timestamp).unwrap(); // Explicit timestamp
	tx.set(b"key6", b"value6").unwrap(); // Will get commit timestamp
	tx.commit().await.unwrap();

	let tx = tree.begin().unwrap();
	let versions4 = tx.scan_all_versions(b"key4", b"key5", None).unwrap();
	let versions5 = tx.scan_all_versions(b"key5", b"key6", None).unwrap();
	let versions6 = tx.scan_all_versions(b"key6", b"key7", None).unwrap();

	assert_eq!(versions4.len(), 1);
	assert_eq!(versions5.len(), 1);
	assert_eq!(versions6.len(), 1);

	// Get timestamps from scan results
	let ts4 = versions4[0].2;
	let ts5 = versions5[0].2;
	let ts6 = versions6[0].2;

	// key4 and key6 should have the same timestamp (commit timestamp)
	assert_eq!(ts4, ts6);

	// key5 should have the custom timestamp
	assert_eq!(ts5, custom_timestamp);

	// key4/key6 timestamp should be different from key5 timestamp
	assert_ne!(ts4, ts5);
}

#[test(tokio::test)]
async fn test_keys_at_version() {
	let temp_dir = create_temp_directory();
	let opts: Options =
		Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
	let tree = TreeBuilder::with_options(opts).build().unwrap();

	// Use explicit timestamps for better testing
	let ts1 = 100; // First batch timestamp
	let ts2 = 200; // Second batch timestamp

	// Insert data with first timestamp
	let mut tx1 = tree.begin().unwrap();
	tx1.set_at_version(b"key1", b"value1", ts1).unwrap();
	tx1.set_at_version(b"key2", b"value2", ts1).unwrap();
	tx1.set_at_version(b"key3", b"value3", ts1).unwrap();
	tx1.commit().await.unwrap();

	// Insert data with second timestamp
	let mut tx2 = tree.begin().unwrap();
	tx2.set_at_version(b"key2", b"value2_updated", ts2).unwrap(); // Update existing key
	tx2.set_at_version(b"key4", b"value4", ts2).unwrap(); // Add new key
	tx2.commit().await.unwrap();

	// Test keys_at_version at first timestamp
	let tx = tree.begin().unwrap();
	let keys_at_ts1: Vec<_> =
		tx.keys_at_version(b"key1", b"key5", ts1).unwrap().map(|r| r.unwrap()).collect();
	assert_eq!(keys_at_ts1.len(), 3);
	assert!(keys_at_ts1.iter().any(|k| k.as_slice() == b"key1"));
	assert!(keys_at_ts1.iter().any(|k| k.as_slice() == b"key2"));
	assert!(keys_at_ts1.iter().any(|k| k.as_slice() == b"key3"));
	assert!(!keys_at_ts1.iter().any(|k| k.as_slice() == b"key4")); // key4 didn't exist at ts1

	// Test keys_at_version at second timestamp
	let keys_at_ts2: Vec<_> =
		tx.keys_at_version(b"key1", b"key5", ts2).unwrap().map(|r| r.unwrap()).collect();
	assert_eq!(keys_at_ts2.len(), 4);
	assert!(keys_at_ts2.iter().any(|k| k.as_slice() == b"key1"));
	assert!(keys_at_ts2.iter().any(|k| k.as_slice() == b"key2"));
	assert!(keys_at_ts2.iter().any(|k| k.as_slice() == b"key3"));
	assert!(keys_at_ts2.iter().any(|k| k.as_slice() == b"key4"));

	// Test with .take()
	let keys_limited: Vec<_> =
		tx.keys_at_version(b"key1", b"key5", ts2).unwrap().take(2).map(|r| r.unwrap()).collect();
	assert_eq!(keys_limited.len(), 2);

	// Test with specific key range
	let keys_range: Vec<_> =
		tx.keys_at_version(b"key2", b"key4", ts2).unwrap().map(|r| r.unwrap()).collect();
	assert_eq!(keys_range.len(), 2);
	assert!(keys_range.iter().any(|k| k.as_slice() == b"key2"));
	assert!(keys_range.iter().any(|k| k.as_slice() == b"key3"));
}

#[test(tokio::test)]
async fn test_keys_at_version_with_deletes() {
	let temp_dir = create_temp_directory();
	let opts: Options =
		Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
	let tree = TreeBuilder::with_options(opts).build().unwrap();

	// Insert data
	let mut tx1 = tree.begin().unwrap();
	tx1.set(b"key1", b"value1").unwrap();
	tx1.set(b"key2", b"value2").unwrap();
	tx1.set(b"key3", b"value3").unwrap();
	tx1.commit().await.unwrap();

	// Delete key2 (hard delete) and soft delete key3
	let mut tx2 = tree.begin().unwrap();
	tx2.delete(b"key2").unwrap();
	tx2.soft_delete(b"key3").unwrap();
	tx2.commit().await.unwrap();

	// Test keys_at_version with current timestamp
	// Should only return key1 (key2 was hard deleted, key3 was soft deleted)
	let tx = tree.begin().unwrap();
	let keys: Vec<_> =
		tx.keys_at_version(b"key1", b"key4", u64::MAX).unwrap().map(|r| r.unwrap()).collect();
	assert_eq!(keys.len(), 1, "Should have only 1 key after deletes");
	assert!(keys.iter().any(|k| k.as_slice() == b"key1"));
	assert!(!keys.iter().any(|k| k.as_slice() == b"key2")); // Hard deleted
	assert!(!keys.iter().any(|k| k.as_slice() == b"key3")); // Soft deleted
}

#[test(tokio::test)]
async fn test_range_at_version() {
	let temp_dir = create_temp_directory();
	let opts: Options =
		Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
	let tree = TreeBuilder::with_options(opts).build().unwrap();

	// Use explicit timestamps for better testing
	let ts1 = 100; // First batch timestamp
	let ts2 = 200; // Second batch timestamp

	// Insert data with first timestamp
	let mut tx1 = tree.begin().unwrap();
	tx1.set_at_version(b"key1", b"value1", ts1).unwrap();
	tx1.set_at_version(b"key2", b"value2", ts1).unwrap();
	tx1.set_at_version(b"key3", b"value3", ts1).unwrap();
	tx1.commit().await.unwrap();

	// Insert data with second timestamp
	let mut tx2 = tree.begin().unwrap();
	tx2.set_at_version(b"key2", b"value2_updated", ts2).unwrap(); // Update existing key
	tx2.set_at_version(b"key4", b"value4", ts2).unwrap(); // Add new key
	tx2.commit().await.unwrap();

	// Test range_at_version at first timestamp
	let tx = tree.begin().unwrap();
	let scan_at_ts1 = tx
		.range_at_version(b"key1", b"key5", ts1)
		.unwrap()
		.collect::<std::result::Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(scan_at_ts1.len(), 3);

	// Check that we get key-value pairs
	let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
	for (key, value) in &scan_at_ts1 {
		found_keys.insert(key.as_ref());
		match key.as_slice() {
			b"key1" => assert_eq!(value.as_slice(), b"value1"),
			b"key2" => assert_eq!(value.as_slice(), b"value2"),
			b"key3" => assert_eq!(value.as_slice(), b"value3"),
			_ => panic!("Unexpected key: {:?}", key),
		}
	}
	assert!(found_keys.contains(&b"key1".as_ref()));
	assert!(found_keys.contains(&b"key2".as_ref()));
	assert!(found_keys.contains(&b"key3".as_ref()));
	assert!(!found_keys.contains(&b"key4".as_ref())); // key4 didn't exist at ts1

	// Test range_at_version at second timestamp
	let scan_at_ts2 = tx
		.range_at_version(b"key1", b"key5", ts2)
		.unwrap()
		.collect::<std::result::Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(scan_at_ts2.len(), 4);

	let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
	for (key, value) in &scan_at_ts2 {
		found_keys.insert(key.as_ref());
		match key.as_slice() {
			b"key1" => assert_eq!(value.as_slice(), b"value1"),
			b"key2" => assert_eq!(value.as_slice(), b"value2_updated"),
			b"key3" => assert_eq!(value.as_slice(), b"value3"),
			b"key4" => assert_eq!(value.as_slice(), b"value4"),
			_ => panic!("Unexpected key: {:?}", key),
		}
	}
	assert!(found_keys.contains(&b"key1".as_ref()));
	assert!(found_keys.contains(&b"key2".as_ref()));
	assert!(found_keys.contains(&b"key3".as_ref()));
	assert!(found_keys.contains(&b"key4".as_ref()));

	// Test with .take()
	let scan_limited = tx
		.range_at_version(b"key1", b"key5", ts2)
		.unwrap()
		.take(2)
		.collect::<std::result::Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(scan_limited.len(), 2);

	// Test with specific key range
	let scan_range = tx
		.range_at_version(b"key2", b"key4", ts2)
		.unwrap()
		.collect::<std::result::Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(scan_range.len(), 2);
	let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
	for (key, _) in &scan_range {
		found_keys.insert(key.as_ref());
	}
	assert!(found_keys.contains(&b"key2".as_ref()));
	assert!(found_keys.contains(&b"key3".as_ref()));
}

#[test(tokio::test)]
async fn test_versioned_range_bounds_edge_cases() {
	let temp_dir = create_temp_directory();
	let opts: Options =
		Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
	let tree = TreeBuilder::with_options(opts).build().unwrap();

	// Use explicit timestamp for testing
	let ts = 100;

	// Insert data: keys a, b, c, d, e
	let mut tx = tree.begin().unwrap();
	tx.set_at_version(b"a", b"value_a", ts).unwrap();
	tx.set_at_version(b"b", b"value_b", ts).unwrap();
	tx.set_at_version(b"c", b"value_c", ts).unwrap();
	tx.set_at_version(b"d", b"value_d", ts).unwrap();
	tx.set_at_version(b"e", b"value_e", ts).unwrap();
	tx.commit().await.unwrap();

	let tx = tree.begin().unwrap();

	// Test 1: Range includes exact start key
	// range_at_version(b"b", b"e", ts) should include b, c, d (not e)
	let scan1 = tx
		.range_at_version(b"b", b"e", ts)
		.unwrap()
		.collect::<std::result::Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(scan1.len(), 3);
	let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
	for (key, _) in &scan1 {
		found_keys.insert(key.as_ref());
	}
	assert!(found_keys.contains(&b"b".as_ref()));
	assert!(found_keys.contains(&b"c".as_ref()));
	assert!(found_keys.contains(&b"d".as_ref()));
	assert!(!found_keys.contains(&b"e".as_ref())); // e is excluded

	// Test 2: Range with start key not in data
	// range_at_version(b"aa", b"cc", ts) should include b, c
	let scan2 = tx
		.range_at_version(b"aa", b"cc", ts)
		.unwrap()
		.collect::<std::result::Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(scan2.len(), 2);
	let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
	for (key, _) in &scan2 {
		found_keys.insert(key.as_ref());
	}
	assert!(found_keys.contains(&b"b".as_ref()));
	assert!(found_keys.contains(&b"c".as_ref()));
	assert!(!found_keys.contains(&b"a".as_ref())); // a is before start

	// Test 3: Range with end key not in data
	// range_at_version(b"b", b"dd", ts) should include b, c, d
	let scan3 = tx
		.range_at_version(b"b".as_slice(), b"dd".as_slice(), ts)
		.unwrap()
		.collect::<std::result::Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(scan3.len(), 3);
	let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
	for (key, _) in &scan3 {
		found_keys.insert(key.as_ref());
	}
	assert!(found_keys.contains(&b"b".as_ref()));
	assert!(found_keys.contains(&b"c".as_ref()));
	assert!(found_keys.contains(&b"d".as_ref()));

	// Test 4: Empty range (start >= end)
	// range_at_version(b"d", b"b", ts) should return empty
	let scan4 = tx
		.range_at_version(b"d", b"b", ts)
		.unwrap()
		.collect::<std::result::Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(scan4.len(), 0);

	// Test 5: Single key range
	// range_at_version(b"b", b"c", ts) should include only b
	let scan5 = tx
		.range_at_version(b"b", b"c", ts)
		.unwrap()
		.collect::<std::result::Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(scan5.len(), 1);
	assert_eq!(scan5[0].0.as_slice(), b"b");
}

#[test(tokio::test)]
async fn test_range_at_version_with_deletes() {
	let temp_dir = create_temp_directory();
	let opts: Options =
		Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
	let clock = Arc::clone(&opts.clock);
	let tree = TreeBuilder::with_options(opts).build().unwrap();

	// Insert data without explicit timestamps (will use auto-generated timestamps)
	let mut tx1 = tree.begin().unwrap();
	tx1.set(b"key1", b"value1").unwrap();
	tx1.set(b"key2", b"value2").unwrap();
	tx1.set(b"key3", b"value3").unwrap();
	tx1.commit().await.unwrap();
	let ts_after_insert = clock.now();

	// Query at this point should show all three keys
	let tx_before = tree.begin().unwrap();
	let scan_before = tx_before
		.range_at_version(b"key1", b"key4", ts_after_insert)
		.unwrap()
		.collect::<std::result::Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(scan_before.len(), 3, "Should have all 3 keys before deletes");

	// Delete key2 (hard delete) and soft delete key3
	let mut tx2 = tree.begin().unwrap();
	tx2.delete(b"key2").unwrap();
	tx2.soft_delete(b"key3").unwrap();
	tx2.commit().await.unwrap();
	let ts_after_deletes = clock.now();

	// Test range_at_version at a time after the deletes
	// Should only return key1 (key2 was hard deleted, key3 was soft deleted)
	let tx = tree.begin().unwrap();

	// Verify key2 is completely gone (hard deleted)
	let versions2 = tx.scan_all_versions(b"key2", b"key3", None).unwrap();
	assert_eq!(versions2.len(), 0, "Hard deleted key should have no versions");

	// Perform scan at timestamp after deletes
	let scan_result = tx
		.range_at_version(b"key1", b"key4", ts_after_deletes)
		.unwrap()
		.collect::<std::result::Result<Vec<_>, _>>()
		.unwrap();
	assert_eq!(scan_result.len(), 1, "Should have only 1 key after deletes");

	let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
	for (key, value) in &scan_result {
		found_keys.insert(key.as_ref());
		match key.as_slice() {
			b"key1" => assert_eq!(value.as_slice(), b"value1"),
			_ => panic!("Unexpected key: {:?}", key),
		}
	}
	assert!(found_keys.contains(&b"key1".as_ref()));
	assert!(!found_keys.contains(&b"key2".as_ref())); // Hard deleted
	assert!(!found_keys.contains(&b"key3".as_ref())); // Soft deleted
}

#[test(tokio::test)]
async fn test_scan_all_versions() {
	let temp_dir = create_temp_directory();
	let opts: Options =
		Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
	let tree = TreeBuilder::with_options(opts).build().unwrap();

	// Insert data at different timestamps
	let mut tx1 = tree.begin().unwrap();
	tx1.set(b"key1", b"value1_v1").unwrap();
	tx1.set(b"key2", b"value2_v1").unwrap();
	tx1.commit().await.unwrap();

	let mut tx2 = tree.begin().unwrap();
	tx2.set(b"key1", b"value1_v2").unwrap();
	tx2.set(b"key3", b"value3_v1").unwrap();
	tx2.commit().await.unwrap();

	let mut tx3 = tree.begin().unwrap();
	tx3.set(b"key2", b"value2_v2").unwrap();
	tx3.set(b"key4", b"value4_v1").unwrap();
	tx3.commit().await.unwrap();

	// Test scan_all_versions
	let tx = tree.begin().unwrap();
	let all_versions = tx.scan_all_versions(b"key1", b"key5", None).unwrap();

	// Should get all versions of all keys in the range
	assert_eq!(all_versions.len(), 6); // 2 versions of key1 + 2 versions of key2 + 1 version of key3 + 1 version of
									// key4

	// Group by key to verify we have all versions
	let mut key_versions: KeyVersionsMap = HashMap::new();
	for (key, value, timestamp, is_tombstone) in all_versions {
		key_versions.entry(key).or_default().push((value.clone(), timestamp, is_tombstone));
	}

	// Verify key1 has 2 versions
	let key1_versions = key_versions.get_mut(&Vec::from(b"key1")).unwrap();
	assert_eq!(key1_versions.len(), 2);
	// Sort by timestamp to get chronological order
	key1_versions.sort_by(|a, b| a.1.cmp(&b.1));
	assert_eq!(key1_versions[0].0, b"value1_v1");
	assert_eq!(key1_versions[1].0, b"value1_v2");
	assert!(!key1_versions[0].2); // Not tombstone
	assert!(!key1_versions[1].2); // Not tombstone

	// Verify key2 has 2 versions
	let key2_versions = key_versions.get_mut(&Vec::from(b"key2")).unwrap();
	assert_eq!(key2_versions.len(), 2);
	key2_versions.sort_by(|a, b| a.1.cmp(&b.1));
	assert_eq!(key2_versions[0].0, b"value2_v1");
	assert_eq!(key2_versions[1].0, b"value2_v2");
	assert!(!key2_versions[0].2); // Not tombstone
	assert!(!key2_versions[1].2); // Not tombstone

	// Verify key3 has 1 version
	let key3_versions = &key_versions[&Vec::from(b"key3")];
	assert_eq!(key3_versions.len(), 1);
	assert_eq!(key3_versions[0].0, b"value3_v1");
	assert!(!key3_versions[0].2); // Not tombstone

	// Verify key4 has 1 version
	let key4_versions = &key_versions[&Vec::from(b"key4")];
	assert_eq!(key4_versions.len(), 1);
	assert_eq!(key4_versions[0].0, b"value4_v1");
	assert!(!key4_versions[0].2); // Not tombstone

	// Test with .take() on the results
	let limited_versions: Vec<_> =
		tx.scan_all_versions(b"key1", b"key5", None).unwrap().into_iter().take(6).collect();
	assert_eq!(limited_versions.len(), 6);

	// Test with specific key range
	let range_versions = tx.scan_all_versions(b"key2", b"key4", None).unwrap();
	assert_eq!(range_versions.len(), 3); // 2 versions of key2 + 1 version of key3
}

#[test(tokio::test)]
async fn test_scan_all_versions_with_deletes() {
	let temp_dir = create_temp_directory();
	let opts: Options =
		Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
	let tree = TreeBuilder::with_options(opts).build().unwrap();

	// Insert data
	let mut tx1 = tree.begin().unwrap();
	tx1.set(b"key1", b"value1_v1").unwrap();
	tx1.set(b"key2", b"value2_v1").unwrap();
	tx1.commit().await.unwrap();

	let mut tx2 = tree.begin().unwrap();
	tx2.set(b"key1", b"value1_v2").unwrap();
	tx2.set(b"key2", b"value2_v2").unwrap();
	tx2.commit().await.unwrap();

	let mut tx3 = tree.begin().unwrap();
	tx3.delete(b"key1").unwrap(); // Hard delete
	tx3.soft_delete(b"key2").unwrap(); // Soft delete
	tx3.commit().await.unwrap();

	// Test scan_all_versions
	let tx = tree.begin().unwrap();
	let all_versions = tx.scan_all_versions(b"key1", b"key3", None).unwrap();

	// Should get all versions including soft delete markers, exclude hard-deleted
	// keys
	assert_eq!(all_versions.len(), 3); // 3 versions of key2 (key1 is hard deleted, soft delete marker included)

	// Group by key to verify we have all versions
	let mut key_versions: KeyVersionsMap = HashMap::new();
	for (key, value, timestamp, is_tombstone) in all_versions {
		key_versions.entry(key).or_default().push((value.clone(), timestamp, is_tombstone));
	}

	// Verify key1 is not present (hard deleted)
	assert!(!key_versions.contains_key(&Vec::from(b"key1")));

	// Verify key2 has 3 versions (2 regular values + 1 soft delete marker)
	let key2_versions = key_versions.get_mut(&Vec::from(b"key2")).unwrap();
	assert_eq!(key2_versions.len(), 3);
	key2_versions.sort_by(|a, b| a.1.cmp(&b.1));
	assert_eq!(key2_versions[0].0, b"value2_v1");
	assert!(!key2_versions[0].2); // Not tombstone
	assert_eq!(key2_versions[1].0, b"value2_v2");
	assert!(!key2_versions[1].2); // Not tombstone
	assert_eq!(key2_versions[2].0, b""); // Empty value for soft delete
	assert!(key2_versions[2].2); // Is tombstone
}

#[test(tokio::test)]
async fn test_versioned_queries_without_versioning() {
	let temp_dir = create_temp_directory();
	let opts: Options =
		Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(false, 0);
	let tree = TreeBuilder::with_options(opts).build().unwrap();

	// Test that versioned queries fail when versioning is disabled
	let tx = tree.begin().unwrap();
	assert!(tx.keys_at_version(b"key1", b"key3", 123456789).is_err());
	assert!(tx.range_at_version(b"key1", b"key3", 123456789).is_err());
	assert!(tx.scan_all_versions(b"key1", b"key3", None).is_err());
}

// Version management tests
mod version_tests {
	use std::collections::HashSet;

	use test_log::test;

	use super::*;
	use crate::Value;

	fn create_tree() -> (Tree, TempDir) {
		let temp_dir = create_temp_directory();
		let opts: Options =
			Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
		(TreeBuilder::with_options(opts).build().unwrap(), temp_dir)
	}

	#[test(tokio::test)]
	async fn test_insert_multiple_versions_in_same_tx() {
		let (store, _tmp_dir) = create_tree();
		let key = Vec::from("key1");

		// Insert multiple versions of the same key
		let values = [Vec::from("value1"), Vec::from("value2"), Vec::from("value3")];

		for (i, value) in values.iter().enumerate() {
			let mut txn = store.begin().unwrap();
			let version = (i + 1) as u64; // Incremental version
			txn.set_at_version(&key, value, version).unwrap();
			txn.commit().await.unwrap();
		}

		let txn = store.begin().unwrap();
		let mut end_key = key.clone();
		end_key.push(0);
		let results: Vec<_> = txn.scan_all_versions(key.as_ref(), &end_key, None).unwrap();

		// Verify that the output contains all the versions of the key
		assert_eq!(results.len(), values.len());
		for (i, (k, v, version, is_deleted)) in results.iter().enumerate() {
			assert_eq!(k, &key);
			assert_eq!(v, &values[i]);
			assert_eq!(*version, (i + 1) as u64);
			assert!(!(*is_deleted));
		}
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_single_key_multiple_versions() {
		let (store, _tmp_dir) = create_tree();
		let key = Vec::from("key1");

		// Insert multiple versions of the same key
		let values = [Vec::from("value1"), Vec::from("value2"), Vec::from("value3")];

		for (i, value) in values.iter().enumerate() {
			let mut txn = store.begin().unwrap();
			let version = (i + 1) as u64; // Incremental version
			txn.set_at_version(&key, value, version).unwrap();
			txn.commit().await.unwrap();
		}

		let txn = store.begin().unwrap();
		let mut end_key = key.clone();
		end_key.push(0);
		let results: Vec<_> = txn.scan_all_versions(key.as_ref(), &end_key, None).unwrap();

		// Verify that the output contains all the versions of the key
		assert_eq!(results.len(), values.len());
		for (i, (k, v, version, is_deleted)) in results.iter().enumerate() {
			assert_eq!(k, &key);
			assert_eq!(v, &values[i]);
			assert_eq!(*version, (i + 1) as u64);
			assert!(!(*is_deleted));
		}
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_multiple_keys_single_version_each() {
		let (store, _tmp_dir) = create_tree();
		let keys = vec![Vec::from("key1"), Vec::from("key2"), Vec::from("key3")];
		let value = Vec::from("value1");

		for key in &keys {
			let mut txn = store.begin().unwrap();
			txn.set_at_version(key, &value, 1).unwrap();
			txn.commit().await.unwrap();
		}

		let txn = store.begin().unwrap();
		let mut end_key = keys.last().unwrap().clone();
		end_key.push(0);
		let results: Vec<_> =
			txn.scan_all_versions(keys.first().unwrap().as_ref(), &end_key, None).unwrap();

		assert_eq!(results.len(), keys.len());
		for (i, (k, v, version, is_deleted)) in results.iter().enumerate() {
			assert_eq!(k, &keys[i]);
			assert_eq!(v, &value);
			assert_eq!(*version, 1);
			assert!(!(*is_deleted));
		}
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_multiple_keys_multiple_versions_each() {
		let (store, _tmp_dir) = create_tree();
		let keys = vec![Vec::from("key1"), Vec::from("key2"), Vec::from("key3")];
		let values = [Vec::from("value1"), Vec::from("value2"), Vec::from("value3")];

		for key in &keys {
			for (i, value) in values.iter().enumerate() {
				let mut txn = store.begin().unwrap();
				let version = (i + 1) as u64;
				txn.set_at_version(key, value, version).unwrap();
				txn.commit().await.unwrap();
			}
		}

		let txn = store.begin().unwrap();
		let mut end_key = keys.last().unwrap().clone();
		end_key.push(0);
		let results: Vec<_> =
			txn.scan_all_versions(keys.first().unwrap().as_ref(), &end_key, None).unwrap();

		let mut expected_results = Vec::new();
		for key in &keys {
			for (i, value) in values.iter().enumerate() {
				expected_results.push((key.clone(), value.clone(), (i + 1) as u64, false));
			}
		}

		assert_eq!(results.len(), expected_results.len());
		for (result, expected) in results.iter().zip(expected_results.iter()) {
			let (k, v, version, is_deleted) = result;
			let (expected_key, expected_value, expected_version, expected_is_deleted) = expected;
			assert_eq!(k, expected_key);
			assert_eq!(&v, &expected_value);
			assert_eq!(*version, *expected_version);
			assert_eq!(*is_deleted, *expected_is_deleted);
		}
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_deleted_records() {
		let (store, _tmp_dir) = create_tree();
		let key = Vec::from("key1");
		let value = Vec::from("value1");

		let mut txn = store.begin().unwrap();
		txn.set_at_version(&key, &value, 1).unwrap();
		txn.commit().await.unwrap();

		let mut txn = store.begin().unwrap();
		txn.soft_delete(&key).unwrap();
		txn.commit().await.unwrap();

		let txn = store.begin().unwrap();
		let mut end_key = key.clone();
		end_key.push(0);
		let results: Vec<_> = txn.scan_all_versions(key.as_ref(), &end_key, None).unwrap();

		assert_eq!(results.len(), 2);
		let (k, v, version, is_deleted) = &results[0];
		assert_eq!(k, &key);
		assert_eq!(v, &value);
		assert_eq!(*version, 1);
		assert!(!(*is_deleted));

		let (k, v, _, is_deleted) = &results[1];
		assert_eq!(k, &key);
		assert_eq!(v, &Vec::<u8>::new());
		assert!(*is_deleted);
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_multiple_keys_single_version_each_deleted() {
		let (store, _tmp_dir) = create_tree();
		let keys = vec![Vec::from("key1"), Vec::from("key2"), Vec::from("key3")];
		let value = Vec::from("value1");

		for key in &keys {
			let mut txn = store.begin().unwrap();
			txn.set_at_version(key, &value, 1).unwrap();
			txn.commit().await.unwrap();
		}

		for key in &keys {
			let mut txn = store.begin().unwrap();
			txn.soft_delete(key).unwrap();
			txn.commit().await.unwrap();
		}

		let txn = store.begin().unwrap();
		let mut end_key = keys.last().unwrap().clone();
		end_key.push(0);
		let results: Vec<_> =
			txn.scan_all_versions(keys.first().unwrap().as_ref(), &end_key, None).unwrap();

		assert_eq!(results.len(), keys.len() * 2);
		for (i, (k, v, version, is_deleted)) in results.iter().enumerate() {
			let key_index = i / 2;
			let is_deleted_version = i % 2 == 1;
			assert_eq!(k, &keys[key_index]);
			if is_deleted_version {
				assert_eq!(v.as_slice(), &Vec::<u8>::new());
				assert!(*is_deleted);
			} else {
				assert_eq!(v.as_slice(), &value);
				assert_eq!(*version, 1);
				assert!(!(*is_deleted));
			}
		}
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_multiple_keys_multiple_versions_each_deleted() {
		let (store, _tmp_dir) = create_tree();
		let keys = vec![Vec::from("key1"), Vec::from("key2"), Vec::from("key3")];
		let values = [Vec::from("value1"), Vec::from("value2"), Vec::from("value3")];

		for key in &keys {
			for (i, value) in values.iter().enumerate() {
				let mut txn = store.begin().unwrap();
				let version = (i + 1) as u64;
				txn.set_at_version(key, value, version).unwrap();
				txn.commit().await.unwrap();
			}
		}

		for key in &keys {
			let mut txn = store.begin().unwrap();
			txn.soft_delete(key).unwrap();
			txn.commit().await.unwrap();
		}

		let txn = store.begin().unwrap();
		let mut end_key = keys.last().unwrap().clone();
		end_key.push(0);
		let results: Vec<_> =
			txn.scan_all_versions(keys.first().unwrap().as_ref(), &end_key, None).unwrap();

		let mut expected_results = Vec::new();
		for key in &keys {
			for (i, value) in values.iter().enumerate() {
				expected_results.push((key.clone(), value.clone(), (i + 1) as u64, false));
			}
			expected_results.push((key.clone(), Vec::new(), 0, true));
		}

		assert_eq!(results.len(), expected_results.len());
		for (result, expected) in results.iter().zip(expected_results.iter()) {
			let (k, v, version, is_deleted) = result;
			let (expected_key, expected_value, expected_version, expected_is_deleted) = expected;
			assert_eq!(k, expected_key);
			assert_eq!(&v, &expected_value);
			if !expected_is_deleted {
				assert_eq!(*version, *expected_version);
			}
			assert_eq!(*is_deleted, *expected_is_deleted);
		}
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_soft_and_hard_delete() {
		let (store, _tmp_dir) = create_tree();
		let key = Vec::from("key1");
		let value = Vec::from("value1");

		let mut txn = store.begin().unwrap();
		txn.set_at_version(&key, &value, 1).unwrap();
		txn.commit().await.unwrap();

		let mut txn = store.begin().unwrap();
		txn.soft_delete(&key).unwrap();
		txn.commit().await.unwrap();

		let mut txn = store.begin().unwrap();
		txn.delete(&key).unwrap();
		txn.commit().await.unwrap();

		let txn = store.begin().unwrap();
		let mut end_key = key.clone();
		end_key.push(0);
		let results: Vec<_> = txn.scan_all_versions(key.as_ref(), &end_key, None).unwrap();

		assert_eq!(results.len(), 0);
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_range_boundaries() {
		let (store, _tmp_dir) = create_tree();
		let keys = vec![Vec::from("key1"), Vec::from("key2"), Vec::from("key3")];
		let value = Vec::from("value1");

		for key in &keys {
			let mut txn = store.begin().unwrap();
			txn.set_at_version(key, &value, 1).unwrap();
			txn.commit().await.unwrap();
		}

		// Inclusive range
		let txn = store.begin().unwrap();
		let mut end_key = keys.last().unwrap().clone();
		end_key.push(0);
		let results: Vec<_> =
			txn.scan_all_versions(keys.first().unwrap().as_ref(), &end_key, None).unwrap();
		assert_eq!(results.len(), keys.len());
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_with_limit() {
		let (store, _tmp_dir) = create_tree();
		let keys = vec![Vec::from("key1"), Vec::from("key2"), Vec::from("key3")];
		let value = Vec::from("value1");

		for key in &keys {
			let mut txn = store.begin().unwrap();
			txn.set_at_version(key, &value, 1).unwrap();
			txn.commit().await.unwrap();
		}

		let txn = store.begin().unwrap();
		let mut end_key = keys.last().unwrap().clone();
		end_key.push(0);
		let results: Vec<_> =
			txn.scan_all_versions(keys.first().unwrap().as_ref(), &end_key, Some(2)).unwrap();

		assert_eq!(results.len(), 2);
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_single_key_single_version() {
		let (store, _tmp_dir) = create_tree();
		let key = Vec::from("key1");
		let value = Vec::from("value1");

		let mut txn = store.begin().unwrap();
		txn.set_at_version(&key, &value, 1).unwrap();
		txn.commit().await.unwrap();

		let txn = store.begin().unwrap();
		let mut end_key = key.clone();
		end_key.push(0);
		let results: Vec<_> = txn.scan_all_versions(key.as_ref(), &end_key, None).unwrap();

		assert_eq!(results.len(), 1);
		let (k, v, version, is_deleted) = &results[0];
		assert_eq!(k, &key);
		assert_eq!(v, &value);
		assert_eq!(*version, 1);
		assert!(!(*is_deleted));
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_with_limit_with_multiple_versions_per_key() {
		let (store, _tmp_dir) = create_tree();
		let keys = vec![Vec::from("key1"), Vec::from("key2"), Vec::from("key3")];
		let values = [Vec::from("value1"), Vec::from("value2"), Vec::from("value3")];

		// Insert multiple versions for each key
		for key in &keys {
			for (i, value) in values.iter().enumerate() {
				let mut txn = store.begin().unwrap();
				let version = (i + 1) as u64;
				txn.set_at_version(key, value, version).unwrap();
				txn.commit().await.unwrap();
			}
		}

		let txn = store.begin().unwrap();
		let mut end_key = keys.last().unwrap().clone();
		end_key.push(0);
		let results: Vec<_> =
			txn.scan_all_versions(keys.first().unwrap().as_ref(), &end_key, Some(2)).unwrap();
		assert_eq!(results.len(), 6); // Take 6 results

		// Collect unique keys from the results
		let unique_keys: HashSet<_> = results.iter().map(|(k, _, _, _)| k.clone()).collect();

		// Verify that the number of unique keys is equal to the limit
		assert_eq!(unique_keys.len(), 2);

		// Verify that the results contain all versions for each key
		for key in unique_keys {
			let key_versions: Vec<_> = results.iter().filter(|(k, _, _, _)| k == &key).collect();

			assert_eq!(key_versions.len(), 3); // Should have all 3 versions

			// Check the latest version
			let latest = key_versions.iter().max_by_key(|(_, _, version, _)| version).unwrap();
			assert_eq!(latest.1.as_ref(), *values.last().unwrap());
			assert_eq!(latest.2, values.len() as u64);
		}
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_with_subsets() {
		let (store, _tmp_dir) = create_tree();
		let keys = vec![
			Vec::from("key1"),
			Vec::from("key2"),
			Vec::from("key3"),
			Vec::from("key4"),
			Vec::from("key5"),
			Vec::from("key6"),
			Vec::from("key7"),
		];
		let values = [Vec::from("value1"), Vec::from("value2"), Vec::from("value3")];

		// Insert multiple versions for each key
		for key in &keys {
			for (i, value) in values.iter().enumerate() {
				let mut txn = store.begin().unwrap();
				let version = (i + 1) as u64;
				txn.set_at_version(key, value, version).unwrap();
				txn.commit().await.unwrap();
			}
		}

		// Define subsets of the entire range
		let subsets = vec![
			(&keys[0], &keys[2]),
			(&keys[1], &keys[3]),
			(&keys[2], &keys[4]),
			(&keys[3], &keys[5]),
			(&keys[4], &keys[6]),
		];

		// Scan each subset and collect versions
		for subset in subsets {
			let txn = store.begin().unwrap();
			let mut end_key = subset.1.clone();
			end_key.push(0);
			let results: Vec<_> = txn.scan_all_versions(subset.0, &end_key, None).unwrap();

			// Collect unique keys from the results
			let unique_keys: HashSet<_> = results.iter().map(|(k, _, _, _)| k.clone()).collect();

			// Verify that the results contain all versions for each key in the subset
			for key in unique_keys {
				for (i, value) in values.iter().enumerate() {
					let version = (i + 1) as u64;
					let result = results
						.iter()
						.find(|(k, v, ver, _)| k == &key && v == value && *ver == version)
						.unwrap();
					assert_eq!(result.1.as_ref(), *value);
					assert_eq!(result.2, version);
				}
			}
		}
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_range_bounds() {
		let (store, _tmp_dir) = create_tree();

		// Insert test data with multiple versions
		let mut txn = store.begin().unwrap();
		txn.set_at_version(b"key1", b"value1", 1).unwrap();
		txn.set_at_version(b"key1", b"value1_v2", 2).unwrap();
		txn.set_at_version(b"key2", b"value2", 1).unwrap();
		txn.set_at_version(b"key2", b"value2_v2", 2).unwrap();
		txn.set_at_version(b"key3", b"value3", 1).unwrap();
		txn.set_at_version(b"key4", b"value4", 1).unwrap();
		txn.set_at_version(b"key5", b"value5", 1).unwrap();

		txn.commit().await.unwrap();

		// Test 1: Unbounded range should return all keys
		let txn = store.begin().unwrap();
		let results = txn.scan_all_versions(&b""[..], &b"\xff\xff\xff\xff"[..], None).unwrap();
		assert_eq!(results.len(), 5); // 5 total versions (only latest per key)
								// Check if the results are correct
		assert_eq!(
			results,
			vec![
				(Vec::from(b"key1"), Vec::from(b"value1_v2"), 2, false),
				(Vec::from(b"key2"), Vec::from(b"value2_v2"), 2, false),
				(Vec::from(b"key3"), Vec::from(b"value3"), 1, false),
				(Vec::from(b"key4"), Vec::from(b"value4"), 1, false),
				(Vec::from(b"key5"), Vec::from(b"value5"), 1, false),
			]
		);

		// Test 2: Range from key2 (inclusive) should exclude key1
		let results = txn.scan_all_versions(&b"key2"[..], &b"\xff\xff\xff\xff"[..], None).unwrap();
		assert_eq!(results.len(), 4); // key2, key3, key4, key5 versions

		// Test 3: Range excluding key2 should exclude key2 but include others
		let results =
			txn.scan_all_versions(&b"key2\x00"[..], &b"\xff\xff\xff\xff"[..], None).unwrap();
		assert_eq!(results.len(), 3); // key3, key4, key5 versions

		// Test 4: Range excluding key5 should exclude key5
		let results =
			txn.scan_all_versions(&b"key5\x00"[..], &b"\xff\xff\xff\xff"[..], None).unwrap();
		assert_eq!(results.len(), 0); // Should be empty!
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_with_batches() {
		let (store, _tmp_dir) = create_tree();
		let keys = [
			Vec::from("key1"),
			Vec::from("key2"),
			Vec::from("key3"),
			Vec::from("key4"),
			Vec::from("key5"),
		];
		let versions = [
			vec![Vec::from("v1"), Vec::from("v2"), Vec::from("v3"), Vec::from("v4")],
			vec![Vec::from("v1"), Vec::from("v2")],
			vec![Vec::from("v1"), Vec::from("v2"), Vec::from("v3"), Vec::from("v4")],
			vec![Vec::from("v1")],
			vec![Vec::from("v1")],
		];

		// Insert multiple versions for each key
		for (key, key_versions) in keys.iter().zip(versions.iter()) {
			for (i, value) in key_versions.iter().enumerate() {
				let mut txn = store.begin().unwrap();
				let version = (i + 1) as u64;
				txn.set_at_version(key, value, version).unwrap();
				txn.commit().await.unwrap();
			}
		}

		// Set the batch size
		let batch_size: usize = 2;

		// Define a function to scan in batches
		fn scan_in_batches(store: &Tree, batch_size: usize) -> Vec<Vec<(Key, Value, u64, bool)>> {
			let mut all_results = Vec::new();
			let mut last_key = Vec::new();
			let mut first_iteration = true;

			loop {
				let txn = store.begin().unwrap();

				// Create range using start and end keys
				let (start_key, end_key): (Vec<u8>, Vec<u8>) = if first_iteration {
					(Vec::new(), b"\xff\xff\xff\xff".to_vec())
				} else {
					// For Excluded(last_key), start from just after last_key
					let mut start = last_key.clone();
					start.push(0);
					(start, b"\xff\xff\xff\xff".to_vec())
				};

				let mut batch_results = Vec::new();
				let results =
					txn.scan_all_versions(&start_key, &end_key, Some(batch_size)).unwrap();
				for (k, v, ts, is_deleted) in results {
					batch_results.push((k.clone(), v, ts, is_deleted));

					// Update last_key with a new vector
					last_key = k.clone();
				}

				if batch_results.is_empty() {
					break;
				}

				first_iteration = false;
				all_results.push(batch_results);
			}

			all_results
		}

		// Scan in batches and collect the results
		let all_results = scan_in_batches(&store, batch_size);

		// Verify the results
		let expected_results = [
			vec![
				(Vec::from("key1"), Vec::from("v1"), 1, false),
				(Vec::from("key1"), Vec::from("v2"), 2, false),
				(Vec::from("key1"), Vec::from("v3"), 3, false),
				(Vec::from("key1"), Vec::from("v4"), 4, false),
				(Vec::from("key2"), Vec::from("v1"), 1, false),
				(Vec::from("key2"), Vec::from("v2"), 2, false),
			],
			vec![
				(Vec::from("key3"), Vec::from("v1"), 1, false),
				(Vec::from("key3"), Vec::from("v2"), 2, false),
				(Vec::from("key3"), Vec::from("v3"), 3, false),
				(Vec::from("key3"), Vec::from("v4"), 4, false),
				(Vec::from("key4"), Vec::from("v1"), 1, false),
			],
			vec![(Vec::from("key5"), Vec::from("v1"), 1, false)],
		];

		assert_eq!(all_results.len(), expected_results.len());

		for (batch, expected_batch) in all_results.iter().zip(expected_results.iter()) {
			assert_eq!(batch.len(), expected_batch.len());
			for (result, expected) in batch.iter().zip(expected_batch.iter()) {
				assert_eq!(result, expected);
			}
		}
	}

	#[test(tokio::test)]
	async fn test_replace_basic() {
		let (store, _tmp_dir) = create_tree();

		// Test basic Replace functionality
		let mut txn = store.begin().unwrap();
		txn.replace(b"test_key", b"test_value").unwrap();
		txn.commit().await.unwrap();

		// Verify the value exists
		let txn = store.begin().unwrap();
		let result = txn.get(b"test_key").unwrap().unwrap();
		assert_eq!(&result, b"test_value");

		// Test Replace with options
		let mut txn = store.begin().unwrap();
		txn.replace_with_options(b"test_key2", b"test_value2", &WriteOptions::default()).unwrap();
		txn.commit().await.unwrap();

		// Verify the second value exists
		let txn = store.begin().unwrap();
		let result = txn.get(b"test_key2").unwrap().unwrap();
		assert_eq!(&result, b"test_value2");
	}

	#[test(tokio::test)]
	async fn test_replace_replaces_previous_versions() {
		let (store, _tmp_dir) = create_tree();

		// Create multiple versions of the same key
		for version in 1..=5 {
			let value = format!("value_v{}", version);
			let mut txn = store.begin().unwrap();
			txn.set(b"test_key", value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Verify the latest version exists
		let txn = store.begin().unwrap();
		let result = txn.get(b"test_key").unwrap().unwrap();
		assert_eq!(&result, b"value_v5");

		// Use Replace to replace all previous versions
		let mut txn = store.begin().unwrap();
		txn.replace(b"test_key", b"replaced_value").unwrap();
		txn.commit().await.unwrap();

		// Verify the new value exists
		let txn = store.begin().unwrap();
		let result = txn.get(b"test_key").unwrap().unwrap();
		assert_eq!(&result, b"replaced_value");
	}

	#[test(tokio::test)]
	async fn test_replace_mixed_with_regular_operations() {
		let (store, _tmp_dir) = create_tree();

		// Mix regular set and replace operations
		let mut txn = store.begin().unwrap();
		txn.set(b"key1", b"regular_value1").unwrap();
		txn.replace(b"key2", b"replace_value2").unwrap();
		txn.set(b"key3", b"regular_value3").unwrap();
		txn.commit().await.unwrap();

		// Verify all values exist
		let txn = store.begin().unwrap();
		assert_eq!(txn.get(b"key1").unwrap().unwrap(), b"regular_value1");
		assert_eq!(txn.get(b"key2").unwrap().unwrap(), b"replace_value2");
		assert_eq!(txn.get(b"key3").unwrap().unwrap(), b"regular_value3");

		// Update key2 with regular set
		let mut txn = store.begin().unwrap();
		txn.set(b"key2", b"updated_regular_value2").unwrap();
		txn.commit().await.unwrap();

		// Verify the updated value
		let txn = store.begin().unwrap();
		assert_eq!(txn.get(b"key2").unwrap().unwrap(), b"updated_regular_value2");

		// Use replace on key1
		let mut txn = store.begin().unwrap();
		txn.replace(b"key1", b"final_set_with_delete_value1").unwrap();
		txn.commit().await.unwrap();

		// Verify the final value
		let txn = store.begin().unwrap();
		assert_eq!(txn.get(b"key1").unwrap().unwrap().as_slice(), b"final_set_with_delete_value1");
	}
}
