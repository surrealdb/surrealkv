//! Tests for the commit ring pipeline: conflict windows that outlive a lap of
//! the ring, and serializability of concurrent read-modify-write commits.

use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::lsm::Tree;
use crate::ring::COMMIT_RING_CAPACITY;
use crate::{Error, TreeBuilder};

fn create_store() -> (Tree, TempDir) {
	let temp_dir = TempDir::new("test").unwrap();
	let tree = TreeBuilder::new().with_path(temp_dir.path().to_path_buf()).build().unwrap();
	(tree, temp_dir)
}

/// Commits enough single-key transactions to lap the commit ring several
/// times, overwriting every slot that was in use before.
async fn lap_the_ring(store: &Tree) {
	for i in 0..(3 * COMMIT_RING_CAPACITY) {
		let mut txn = store.begin().unwrap();
		txn.set(format!("filler{i}").as_bytes(), b"v").unwrap();
		txn.commit().await.unwrap();
	}
}

#[test(tokio::test)]
async fn long_lived_txn_commits_across_ring_laps() {
	let (store, _dir) = create_store();

	let mut long = store.begin().unwrap();
	long.set(b"long", b"1").unwrap();
	lap_the_ring(&store).await;
	long.commit().await.unwrap();

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"long").unwrap().unwrap(), b"1");
}

#[test(tokio::test)]
async fn long_lived_txn_detects_a_write_conflict_lapped_by_the_ring() {
	let (store, _dir) = create_store();

	let mut long = store.begin().unwrap();
	long.set(b"contended", b"long").unwrap();

	// The conflicting commit is followed by several laps of others, so its
	// ring slot is overwritten long before `long` validates.
	let mut other = store.begin().unwrap();
	other.set(b"contended", b"other").unwrap();
	other.commit().await.unwrap();
	lap_the_ring(&store).await;

	assert!(matches!(long.commit().await, Err(Error::TransactionWriteConflict)));
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"contended").unwrap().unwrap(), b"other");
}

#[test(tokio::test)]
async fn long_lived_txn_detects_a_locked_read_conflict_lapped_by_the_ring() {
	let (store, _dir) = create_store();

	let mut long = store.begin().unwrap();
	assert!(long.get_for_update(b"locked").unwrap().is_none());
	long.set(b"elsewhere", b"long").unwrap();

	let mut other = store.begin().unwrap();
	other.set(b"locked", b"other").unwrap();
	other.commit().await.unwrap();
	lap_the_ring(&store).await;

	assert!(matches!(long.commit().await, Err(Error::TransactionWriteConflict)));
}

#[test(tokio::test)]
async fn commits_visible_before_begin_never_conflict_across_laps() {
	let (store, _dir) = create_store();

	let mut first = store.begin().unwrap();
	first.set(b"key", b"first").unwrap();
	first.commit().await.unwrap();
	lap_the_ring(&store).await;

	// Everything above is in this transaction's snapshot, so none of it is
	// in its conflict window.
	let mut txn = store.begin().unwrap();
	assert_eq!(txn.get_for_update(b"key").unwrap().unwrap(), b"first");
	txn.set(b"key", b"second").unwrap();
	txn.commit().await.unwrap();
}

/// Runs `TASKS` concurrent tasks that each commit `INCREMENTS` read-modify-
/// write increments of one counter, retrying on conflict, and checks that
/// every successful commit read the value the previous one wrote.
async fn assert_increments_serialize(locked: bool) {
	const TASKS: u64 = 32;
	const INCREMENTS: u64 = 40;

	let (store, _dir) = create_store();
	let store = Arc::new(store);

	let handles: Vec<_> = (0..TASKS)
		.map(|_| {
			let store = Arc::clone(&store);
			tokio::spawn(async move {
				let mut done = 0;
				while done < INCREMENTS {
					let mut txn = store.begin().unwrap();
					let read = if locked {
						txn.get_for_update(b"counter")
					} else {
						txn.get(b"counter")
					};
					let current = read
						.unwrap()
						.map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
						.unwrap_or(0);
					txn.set(b"counter", &(current + 1).to_be_bytes()).unwrap();
					match txn.commit().await {
						Ok(()) => done += 1,
						Err(Error::TransactionWriteConflict) => {}
						Err(e) => panic!("unexpected commit error: {e:?}"),
					}
				}
			})
		})
		.collect();
	for h in handles {
		h.await.unwrap();
	}

	let txn = store.begin().unwrap();
	let counter = txn.get(b"counter").unwrap().unwrap();
	assert_eq!(u64::from_be_bytes(counter.as_slice().try_into().unwrap()), TASKS * INCREMENTS);
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn concurrent_locked_increments_never_lose_updates() {
	assert_increments_serialize(true).await;
}

/// Under snapshot isolation two transactions that both write the counter
/// conflict, so even plain reads serialize these increments.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn concurrent_increments_never_lose_updates() {
	assert_increments_serialize(false).await;
}
