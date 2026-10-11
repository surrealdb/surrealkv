//! Browser tests of the OPFS `LogStore` and `ObjectStore`, run inside a dedicated worker of a real
//! headless Chrome (`WASM_BINDGEN_USE_DEDICATED_WORKER=1 wasm-pack test --headless --chrome
//! --test opfs_store_test`).
//!
//! The storage traits have one contract, whichever backend implements them. The scenarios of the
//! native storage tests (`src/storage/tests.rs`) are written here once as generic contract
//! functions, `log_store_contract` and `object_store_contract`, and run against the in-memory
//! stores (which pins the contract down) and against the OPFS stores (which must obey it). The
//! OPFS-specific tests then look at the file underneath, at what survives a close and reopen, and
//! at what happens when the file is closed.
//!
//! Groups:
//! 1. The shared contract, for the memory and the OPFS stores.
//! 2. OPFS log store: layout in the file, out-of-order polling, durability across a reopen.
//! 3. OPFS object store: reading what a log store wrote, empty files.
//! 4. Closed files: operations fail instead of pretending to succeed.
//!
//! Tests marked `#[ignore]` document behaviour of the backend that is wrong; run them with
//! `-- --include-ignored` to see the failure.

#![cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]

mod opfs_common;

use std::sync::Arc;

use opfs_common::*;
use surrealkv::storage::opfs::{OpfsLogStore, OpfsObjectStore};
use surrealkv::storage::{LogStore, MemLogStore, MemObjectStore, ObjectStore};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

// ---------------------------------------------------------------------------------------------
// The shared contract
// ---------------------------------------------------------------------------------------------

/// The scenarios every `LogStore` must satisfy, starting from an empty log: `append` returns the
/// offset at which the log ends after the record, `size` is that same end, a zero-length append
/// changes nothing, and many appends of every length accumulate. Returns each record with the end
/// offset its append returned, so that the caller can look at where the bytes really are.
async fn log_store_contract(store: &dyn LogStore, seed: u64) -> Vec<(u64, Vec<u8>)> {
	assert_eq!(store.size().await.unwrap(), 0, "a new log is empty");

	// The scenario of the native `test_mem_log_store`.
	assert_eq!(store.append(b"hello ").await.unwrap(), 6);
	assert_eq!(store.append(b"world").await.unwrap(), 11);
	store.sync().await.unwrap();
	assert_eq!(store.size().await.unwrap(), 11);
	let mut records = vec![(6, b"hello ".to_vec()), (11, b"world".to_vec())];

	// An empty record returns the current end and leaves the log as it was.
	assert_eq!(store.append(b"").await.unwrap(), 11);
	assert_eq!(store.size().await.unwrap(), 11);

	// Many appends of every length, empty ones among them.
	let mut rng = Rng::new(seed);
	let mut total = 11u64;
	for i in 0..400 {
		let len = match i % 50 {
			0 => 0,
			1 => 1,
			_ => rng.below(301) as usize,
		};
		let record = pattern(seed + 1000 + i, len);
		total += len as u64;
		let end = store.append(&record).await.unwrap();
		assert_eq!(end, total, "end offset of append {i} ({len} bytes), seed {seed}");
		records.push((end, record));
		if i % 20 == 0 {
			assert_eq!(store.size().await.unwrap(), total, "size after append {i}, seed {seed}");
		}
		if i % 100 == 99 {
			store.sync().await.unwrap();
		}
	}
	store.sync().await.unwrap();
	assert_eq!(store.size().await.unwrap(), total, "final size, seed {seed}");
	records
}

/// The scenarios every `ObjectStore` over `content` must satisfy: `size` is the length of the
/// content, a read returns exactly the requested range, a read that reaches past the end is cut
/// short at the end (it never returns padding), and a read that starts at or past the end is
/// empty.
async fn object_store_contract(store: &dyn ObjectStore, content: &[u8], seed: u64) {
	let n = content.len();
	assert!(n >= 100, "the contract needs some content to read");
	assert_eq!(store.size().await.unwrap(), n as u64, "size of the object");

	// The scenario of the native `test_mem_object_store`, on this content.
	let read = |offset: u64, len: usize| async move { store.read_at(offset, len).await.unwrap() };
	assert_eq!(read(6, 5).await.as_ref(), &content[6..11]);
	assert_eq!(read(0, n).await.as_ref(), content, "the whole object");
	assert_eq!(read(n as u64 - 3, 3).await.as_ref(), &content[n - 3..], "the last three bytes");

	// Reads that reach past the end are short, never padded.
	assert_eq!(read(n as u64 - 3, 10).await.as_ref(), &content[n - 3..], "a read across the end");
	assert_eq!(read(0, n + 1000).await.as_ref(), content, "a read much longer than the object");
	assert_eq!(read(n as u64 - 1, 2).await.as_ref(), &content[n - 1..], "the last byte only");

	// Reads that start at or past the end are empty, and so are zero-length reads.
	assert!(read(n as u64, 10).await.is_empty(), "a read starting at the end");
	assert!(read(n as u64 + 1, 10).await.is_empty(), "a read starting after the end");
	assert!(read(n as u64 + 100_000, 10).await.is_empty(), "a read far after the end");
	assert!(read(0, 0).await.is_empty(), "a zero-length read at the start");
	assert!(read(n as u64 / 2, 0).await.is_empty(), "a zero-length read inside");
	assert!(read(n as u64, 0).await.is_empty(), "a zero-length read at the end");

	// Random ranges, some inside, some across the end, some past it, against the model.
	let mut rng = Rng::new(seed);
	for i in 0..300 {
		let offset = rng.below(n as u64 + 100) as usize;
		let len = rng.below(n as u64 / 2 + 10) as usize;
		let from = offset.min(n);
		let to = offset.saturating_add(len).min(n);
		assert_eq!(
			read(offset as u64, len).await.as_ref(),
			&content[from..to],
			"range {i}: {len} bytes at {offset} of {n}, seed {seed}"
		);
	}
}

#[wasm_bindgen_test]
async fn the_memory_log_store_follows_the_log_store_contract() {
	let store = MemLogStore::new();
	let records = log_store_contract(&store, 0x1061).await;
	assert_eq!(records.last().map(|r| r.0), Some(store.size().await.unwrap()));
}

#[wasm_bindgen_test]
async fn the_memory_object_store_follows_the_object_store_contract() {
	let content = pattern(0x0B1, 5000);
	object_store_contract(&MemObjectStore::new(content.clone()), &content, 0x0B1).await;
}

#[wasm_bindgen_test]
async fn the_opfs_log_store_follows_the_log_store_contract_and_each_offset_ends_its_record() {
	const SEED: u64 = 0x1062;
	let root = root().await;
	let name = unique_name("log-contract", "log");
	let file = Arc::new(create(&root, &name).await);
	let store = OpfsLogStore::new(Arc::clone(&file));

	let records = log_store_contract(&store, SEED).await;

	// What the store reported must be what is in the file: the records back to back, and every
	// returned offset the exact end of its own record.
	let bytes = read_all(&file);
	let expected: Vec<u8> = records.iter().flat_map(|(_, record)| record.iter().copied()).collect();
	assert_eq!(bytes.len(), expected.len(), "length of the log file, seed {SEED}");
	assert!(bytes == expected, "the file is the records back to back, seed {SEED}");
	assert_eq!(file.size().unwrap(), bytes.len() as u64);
	assert_eq!(store.size().await.unwrap(), bytes.len() as u64);
	for (i, (end, record)) in records.iter().enumerate() {
		let start = *end as usize - record.len();
		assert_eq!(&bytes[start..*end as usize], &record[..], "record {i} ends at {end}");
	}

	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn the_opfs_object_store_follows_the_object_store_contract() {
	const SEED: u64 = 0x0B2;
	let root = root().await;
	let name = unique_name("object-contract", "sst");
	let file = Arc::new(create(&root, &name).await);

	// Written in uneven pieces, so that the store reads across the boundaries of the writes.
	let content = pattern(SEED, 5000);
	let mut offset = 0;
	for piece in [1usize, 700, 37, 1500, 4093] {
		let end = (offset + piece).min(content.len());
		file.write_at(offset as u64, &content[offset..end]).unwrap();
		offset = end;
	}
	assert_eq!(offset, content.len());
	file.flush().unwrap();

	object_store_contract(&OpfsObjectStore::new(Arc::clone(&file)), &content, SEED).await;

	file.close();
	remove_and_verify(&root, &name).await;
}

// ---------------------------------------------------------------------------------------------
// OPFS log store
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn append_futures_polled_in_the_reverse_order_still_do_not_overlap() {
	let root = root().await;
	let name = unique_name("log-reverse", "log");
	let file = Arc::new(create(&root, &name).await);
	let store = OpfsLogStore::new(Arc::clone(&file));

	let (rec_a, rec_b, rec_c) = (pattern(1, 8), pattern(2, 8), pattern(3, 8));
	let a = store.append(&rec_a);
	let b = store.append(&rec_b);
	let c = store.append(&rec_c);
	let end_c = c.await.unwrap();
	let end_b = b.await.unwrap();
	let end_a = a.await.unwrap();

	// Whatever the order, three distinct 8-byte ranges tile the 24 bytes of the log...
	let mut ends = [end_a, end_b, end_c];
	ends.sort_unstable();
	assert_eq!(ends, [8, 16, 24], "the three records end at 8, 16 and 24");
	assert_eq!(store.size().await.unwrap(), 24);
	// ... and each returned offset is the end of the range that holds that record's bytes.
	let bytes = read_all(&file);
	assert_eq!(bytes.len(), 24, "no gap and no overlap");
	for (end, record) in [(end_a, &rec_a), (end_b, &rec_b), (end_c, &rec_c)] {
		assert_eq!(&bytes[end as usize - 8..end as usize], &record[..], "record ending at {end}");
	}

	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn synced_appends_survive_a_close_and_reopen_and_the_log_continues_at_its_end() {
	const SEED: u64 = 0x1063;
	let root = root().await;
	let name = unique_name("log-reopen", "log");
	let file = Arc::new(create(&root, &name).await);
	let store = OpfsLogStore::new(Arc::clone(&file));

	let mut expected = Vec::new();
	let mut rng = Rng::new(SEED);
	for i in 0..60 {
		let record = pattern(SEED + i, 1 + rng.below(200) as usize);
		expected.extend_from_slice(&record);
		assert_eq!(store.append(&record).await.unwrap(), expected.len() as u64);
	}
	store.sync().await.unwrap();
	assert_eq!(store.size().await.unwrap(), expected.len() as u64);
	file.close();
	drop(store);

	// A new store over the reopened file starts where the log ended, not at zero.
	let file = Arc::new(reopen(&root, &name).await);
	let store = OpfsLogStore::new(Arc::clone(&file));
	assert_eq!(store.size().await.unwrap(), expected.len() as u64, "size after the reopen");
	let more = pattern(SEED ^ 0xFF, 123);
	expected.extend_from_slice(&more);
	assert_eq!(store.append(&more).await.unwrap(), expected.len() as u64, "append after a reopen");
	store.sync().await.unwrap();
	assert!(read_all(&file) == expected, "old and new records are contiguous, seed {SEED}");
	file.close();

	// And once more, to see that what was appended after the reopen is durable too.
	let file = Arc::new(reopen(&root, &name).await);
	assert!(read_all(&file) == expected, "after the second reopen, seed {SEED}");
	assert_eq!(OpfsLogStore::new(Arc::clone(&file)).size().await.unwrap(), expected.len() as u64);
	file.close();
	remove_and_verify(&root, &name).await;
}

// ---------------------------------------------------------------------------------------------
// OPFS object store
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn an_object_store_reads_what_a_log_store_appended_after_the_file_was_reopened() {
	const SEED: u64 = 0x0B3;
	let root = root().await;
	let name = unique_name("log-then-object", "sst");
	let file = Arc::new(create(&root, &name).await);
	let log = OpfsLogStore::new(Arc::clone(&file));

	let mut content = Vec::new();
	for i in 0..40 {
		let record = pattern(SEED + i, 50 + (i as usize * 13) % 100);
		content.extend_from_slice(&record);
		log.append(&record).await.unwrap();
	}
	log.sync().await.unwrap();
	file.close();

	let file = Arc::new(reopen(&root, &name).await);
	let object = OpfsObjectStore::new(Arc::clone(&file));
	assert_eq!(object.size().await.unwrap(), content.len() as u64, "size after the reopen");
	object_store_contract(&object, &content, SEED).await;

	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn an_object_store_over_an_empty_file_has_size_zero_and_reads_nothing() {
	let root = root().await;
	let name = unique_name("object-empty", "sst");
	let file = Arc::new(create(&root, &name).await);
	let object = OpfsObjectStore::new(Arc::clone(&file));

	assert_eq!(object.size().await.unwrap(), 0);
	for (offset, len) in [(0u64, 0usize), (0, 10), (5, 10), (1_000_000, 4)] {
		assert!(
			object.read_at(offset, len).await.unwrap().is_empty(),
			"{len} bytes at {offset} of an empty object"
		);
	}

	// The same empty file as a log: it starts at zero.
	let log = OpfsLogStore::new(Arc::clone(&file));
	assert_eq!(log.size().await.unwrap(), 0);

	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn the_object_store_sees_bytes_written_to_the_file_after_it_was_created() {
	// The store keeps no copy of the file: what is written through the file is read through the
	// store, and `size` follows.
	let root = root().await;
	let name = unique_name("object-live", "sst");
	let file = Arc::new(create(&root, &name).await);
	let object = OpfsObjectStore::new(Arc::clone(&file));

	let first = pattern(7, 100);
	file.write_at(0, &first).unwrap();
	assert_eq!(object.size().await.unwrap(), 100);
	assert_eq!(object.read_at(10, 20).await.unwrap().as_ref(), &first[10..30]);

	let second = pattern(8, 50);
	file.write_at(100, &second).unwrap();
	assert_eq!(object.size().await.unwrap(), 150);
	assert_eq!(
		object.read_at(90, 30).await.unwrap().as_ref(),
		[&first[90..], &second[..20]].concat()
	);

	let patch = pattern(9, 10);
	file.write_at(20, &patch).unwrap();
	assert_eq!(
		object.read_at(15, 20).await.unwrap().as_ref(),
		[&first[15..20], &patch[..], &first[30..35]].concat()
	);
	assert_eq!(object.size().await.unwrap(), 150, "an overwrite does not change the size");

	file.close();
	remove_and_verify(&root, &name).await;
}

// ---------------------------------------------------------------------------------------------
// Closed files
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn sync_append_and_read_on_a_closed_file_fail_instead_of_pretending_to_succeed() {
	let root = root().await;
	let name = unique_name("stores-closed", "log");
	let file = Arc::new(create(&root, &name).await);
	let log = OpfsLogStore::new(Arc::clone(&file));
	let object = OpfsObjectStore::new(Arc::clone(&file));

	log.append(b"before").await.unwrap();
	log.sync().await.unwrap();
	file.close();

	let err = expect_err(log.sync().await, "LogStore::sync on a closed file");
	assert!(err.contains("InvalidStateError"), "{err}");
	let err = expect_err(log.append(b"after").await, "LogStore::append on a closed file");
	assert!(err.contains("InvalidStateError"), "{err}");
	let err = expect_err(object.read_at(0, 3).await, "ObjectStore::read_at on a closed file");
	assert!(err.contains("InvalidStateError"), "{err}");

	// The failed append did not reach the file.
	let again = reopen(&root, &name).await;
	assert_eq!(read_all(&again), b"before");
	again.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
#[ignore = "a failed OpfsLogStore::append still advances size(), as if the record had been written"]
async fn a_failed_append_does_not_advance_the_size_of_the_log() {
	let root = root().await;
	let name = unique_name("log-failed-append", "log");
	let file = Arc::new(create(&root, &name).await);
	let log = OpfsLogStore::new(Arc::clone(&file));

	assert_eq!(log.append(b"12345").await.unwrap(), 5);
	file.close();
	assert!(log.append(b"abc").await.is_err(), "the append on a closed file fails");

	// Nothing was written, so the log still ends where it did. Today size() is 8, three bytes
	// past the end of the data.
	assert_eq!(log.size().await.unwrap(), 5, "size() after a failed append");

	let again = reopen(&root, &name).await;
	assert_eq!(read_all(&again), b"12345");
	again.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
#[ignore = "an append future dropped before it is polled reserves its byte range and leaves a hole of zeros in the log"]
async fn an_append_future_dropped_unpolled_leaves_no_hole_in_the_log() {
	let root = root().await;
	let name = unique_name("log-dropped-append", "log");
	let file = Arc::new(create(&root, &name).await);
	let log = OpfsLogStore::new(Arc::clone(&file));

	// The memory store writes at the call, the pool-backed store only when polled, so the record
	// of a dropped future may or may not be in the log. What it must not be is a hole.
	drop(log.append(b"lost"));
	let end = log.append(b"kept").await.unwrap();
	log.sync().await.unwrap();

	let bytes = read_all(&file);
	assert!(
		bytes == b"kept" || bytes == b"lostkept",
		"the log must hold the kept record with or without the dropped one, it holds {bytes:?}"
	);
	assert_eq!(end, bytes.len() as u64, "the offset returned is the end of the log");
	assert_eq!(log.size().await.unwrap(), bytes.len() as u64, "size() is the length of the log");

	file.close();
	remove_and_verify(&root, &name).await;
}
