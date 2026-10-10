//! Browser tests of the OPFS sync file (`OpfsSyncFile`), run inside a dedicated worker of a real
//! headless Chrome (`WASM_BINDGEN_USE_DEDICATED_WORKER=1 wasm-pack test --headless --chrome
//! --test opfs_worker_test`).
//!
//! `FileSystemSyncAccessHandle` only exists in a dedicated worker, so this is the one context the
//! backend can work in. The tests hold the sync file to the contract of a random-access file and
//! check it against the browser itself: every size reported by `size()` is cross-checked against
//! the length found by reading to the end.
//!
//! Groups:
//! 1. Smoke: the two original read/write and log/object-store round trips.
//! 2. Sync file contract: write and read back, overwrite, write past the end, short reads,
//!    zero-length operations, a large file in uneven chunks, many small writes.
//! 3. Persistence: data survives close and reopen with `create = false`, flushed or not.
//! 4. Exclusivity: a second handle fails while the file is open, closing twice and using a closed
//!    file return errors and never trap the worker.
//! 5. Open errors: missing files, invalid names, long names, unicode and space-containing names.
//! 6. Root handle: `get_opfs_root` can be called repeatedly.
//!
//! The browser keeps OPFS in its profile, so every test uses names no other test or run uses and
//! removes its files with `remove_and_verify`, which also proves that every handle was closed.
//! Tests marked `#[ignore]` document behaviour of the backend that is wrong; run them with
//! `-- --include-ignored` to see the failure.

#![cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]

mod opfs_common;

use std::sync::Arc;

use opfs_common::*;
use surrealkv::storage::opfs::{get_opfs_root, open_opfs_sync_file, OpfsLogStore, OpfsObjectStore};
use surrealkv::storage::{LogStore, ObjectStore};
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

// ---------------------------------------------------------------------------------------------
// 1. Smoke
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn test_opfs_sync_file_read_write() {
	let root = get_opfs_root().await.expect("Failed to get OPFS root");
	let name = unique_name("smoke-file", "bin");
	let file = open_opfs_sync_file(&root, &name, true).await.expect("Failed to open OPFS file");

	let data = b"Hello from OPFS Web Worker!";
	let written = file.write_at(0, data).expect("Failed to write to OPFS");
	assert_eq!(written, data.len());

	let mut read_buf = vec![0u8; data.len()];
	let read = file.read_at(0, &mut read_buf).expect("Failed to read from OPFS");
	assert_eq!(read, data.len());
	assert_eq!(&read_buf, data);

	file.flush().expect("Failed to flush OPFS");
	assert_eq!(file.size().unwrap(), data.len() as u64);
	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn test_opfs_log_and_object_store() {
	let root = get_opfs_root().await.expect("Failed to get OPFS root");
	let wal_name = unique_name("smoke-wal", "log");
	let log_file = Arc::new(
		open_opfs_sync_file(&root, &wal_name, true).await.expect("Failed to open OPFS WAL"),
	);

	let log_store = OpfsLogStore::new(Arc::clone(&log_file));
	let offset = log_store.append(b"record 1").await.expect("append 1");
	assert_eq!(offset, 8);
	let offset2 = log_store.append(b"record 2").await.expect("append 2");
	assert_eq!(offset2, 16);
	log_store.sync().await.expect("sync");
	assert_eq!(log_store.size().await.unwrap(), 16);
	assert_eq!(read_all(&log_file), b"record 1record 2");

	let sst_name = unique_name("smoke-sst", "sst");
	let obj_file = Arc::new(
		open_opfs_sync_file(&root, &sst_name, true).await.expect("Failed to open OPFS SST"),
	);
	obj_file.write_at(0, b"abcdefghijklmnop").expect("write obj");
	obj_file.flush().expect("flush obj");

	let obj_store = OpfsObjectStore::new(Arc::clone(&obj_file));
	let bytes = obj_store.read_at(4, 6).await.expect("read obj");
	assert_eq!(&bytes[..], b"efghij");
	assert_eq!(obj_store.size().await.unwrap(), 16);

	log_file.close();
	obj_file.close();
	remove_and_verify(&root, &wal_name).await;
	remove_and_verify(&root, &sst_name).await;
}

#[wasm_bindgen_test]
async fn these_tests_run_in_a_dedicated_worker_and_not_on_the_main_thread() {
	let global = js_sys::global();
	assert!(
		global.dyn_ref::<web_sys::WorkerGlobalScope>().is_some(),
		"the OPFS tests must run in a worker, the only place a sync access handle exists"
	);
	assert!(global.dyn_ref::<web_sys::Window>().is_none(), "this target must not run on a window");
}

// ---------------------------------------------------------------------------------------------
// 2. Sync file contract
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn a_new_file_is_empty_and_the_size_follows_every_write() {
	let root = root().await;
	let name = unique_name("sizes", "bin");
	let file = create(&root, &name).await;

	assert_eq!(file.size().unwrap(), 0, "a new file is empty");
	let mut probe = [0xAAu8; 8];
	assert_eq!(file.read_at(0, &mut probe).unwrap(), 0, "an empty file has nothing to read");
	assert_eq!(probe, [0xAA; 8], "a read of nothing must leave the buffer alone");

	let first = pattern(1, 10);
	let second = pattern(2, 15);
	assert_eq!(file.write_at(0, &first).unwrap(), first.len());
	assert_eq!(file.size().unwrap(), 10);
	assert_eq!(file.write_at(10, &second).unwrap(), second.len());
	assert_eq!(file.size().unwrap(), 25);

	let expected = [first, second.clone()].concat();
	assert_eq!(read_all(&file), expected, "the whole file");
	// Reads at a non-zero offset return exactly the bytes of that range.
	assert_eq!(read_exact(&file, 4, 12).as_slice(), &expected[4..16], "a range across both writes");
	assert_eq!(read_exact(&file, 10, 15), second, "exactly the second write");
	assert_eq!(read_exact(&file, 24, 1).as_slice(), &expected[24..], "the last byte");
	assert_eq!(read_exact(&file, 0, 1).as_slice(), &expected[..1], "the first byte");
	assert_eq!(file.size().unwrap(), 25, "reading must not change the size");

	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn overwriting_the_middle_of_a_file_changes_only_those_bytes() {
	let root = root().await;
	let name = unique_name("overwrite", "bin");
	let file = create(&root, &name).await;

	let mut model = pattern(10, 100);
	file.write_at(0, &model).unwrap();

	// In the middle, at the very start and ending exactly at the end: the size never changes.
	for (seed, offset, len) in [(11u64, 30usize, 20usize), (12, 0, 7), (13, 93, 7), (14, 50, 1)] {
		let patch = pattern(seed, len);
		assert_eq!(file.write_at(offset as u64, &patch).unwrap(), len);
		model[offset..offset + len].copy_from_slice(&patch);
		assert_eq!(file.size().unwrap(), 100, "overwrite at {offset} of {len} bytes");
		assert_eq!(read_all(&file), model, "overwrite at {offset} of {len} bytes");
	}

	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn overwriting_across_the_end_extends_the_file() {
	let root = root().await;
	let name = unique_name("straddle", "bin");
	let file = create(&root, &name).await;

	let mut model = pattern(20, 100);
	file.write_at(0, &model).unwrap();

	// Ten bytes starting five bytes before the end: five overwrite, five extend.
	let patch = pattern(21, 10);
	assert_eq!(file.write_at(95, &patch).unwrap(), 10);
	model.truncate(95);
	model.extend_from_slice(&patch);
	assert_eq!(file.size().unwrap(), 105);
	assert_eq!(read_all(&file), model);

	// A write that starts exactly at the end is a plain append.
	let tail = pattern(22, 3);
	assert_eq!(file.write_at(105, &tail).unwrap(), 3);
	model.extend_from_slice(&tail);
	assert_eq!(file.size().unwrap(), 108);
	assert_eq!(read_all(&file), model);

	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn writing_beyond_the_end_zero_fills_the_gap_and_extends_the_size() {
	let root = root().await;
	let name = unique_name("gap", "bin");
	let file = create(&root, &name).await;

	file.write_at(0, b"head").unwrap();
	assert_eq!(file.write_at(1000, b"tail").unwrap(), 4);
	assert_eq!(file.size().unwrap(), 1004, "the size reaches the end of the far write");

	let bytes = read_all(&file);
	assert_eq!(bytes.len(), 1004, "the file really is that long");
	assert_eq!(&bytes[..4], b"head");
	assert!(bytes[4..1000].iter().all(|&b| b == 0), "the gap reads as zeros");
	assert_eq!(&bytes[1000..], b"tail");

	// Filling part of the gap later leaves the rest of it zero.
	file.write_at(500, b"mid").unwrap();
	assert_eq!(file.size().unwrap(), 1004, "a write inside the file does not grow it");
	let bytes = read_all(&file);
	assert_eq!(&bytes[500..503], b"mid");
	assert!(bytes[4..500].iter().all(|&b| b == 0));
	assert!(bytes[503..1000].iter().all(|&b| b == 0));
	file.close();
	remove_and_verify(&root, &name).await;

	// The same on a file that was empty, with the first write far beyond a page boundary.
	let name = unique_name("gap-empty", "bin");
	let file = create(&root, &name).await;
	assert_eq!(file.write_at(70_000, b"x").unwrap(), 1);
	assert_eq!(file.size().unwrap(), 70_001);
	let bytes = read_all(&file);
	assert_eq!(bytes.len(), 70_001);
	assert!(bytes[..70_000].iter().all(|&b| b == 0), "everything before the write is zeros");
	assert_eq!(bytes[70_000], b'x');
	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn reads_across_and_past_the_end_return_short_reads() {
	let root = root().await;
	let name = unique_name("short-read", "bin");
	let file = create(&root, &name).await;
	let data = pattern(30, 20);
	file.write_at(0, &data).unwrap();

	// A read that starts inside the file and ends past it returns what is there and nothing of
	// the buffer beyond that.
	let mut buf = [0xEEu8; 10];
	assert_eq!(file.read_at(15, &mut buf).unwrap(), 5, "5 bytes remain after offset 15");
	assert_eq!(&buf[..5], &data[15..]);
	assert_eq!(&buf[5..], &[0xEE; 5], "the rest of the buffer is untouched");

	let mut buf = [0xEEu8; 2];
	assert_eq!(file.read_at(19, &mut buf).unwrap(), 1, "one byte remains after offset 19");
	assert_eq!(buf, [data[19], 0xEE]);

	// A buffer much larger than the file.
	let mut buf = vec![0xEEu8; 100];
	assert_eq!(file.read_at(0, &mut buf).unwrap(), 20);
	assert_eq!(&buf[..20], &data[..]);
	assert!(buf[20..].iter().all(|&b| b == 0xEE));

	// Exactly at the end, just past it and far past it: nothing, and no error.
	for offset in [20u64, 21, 4096, 1_000_000] {
		let mut buf = [0xEEu8; 4];
		assert_eq!(file.read_at(offset, &mut buf).unwrap(), 0, "read at {offset}, past the end");
		assert_eq!(buf, [0xEE; 4], "a read at {offset} must leave the buffer alone");
	}
	assert_eq!(file.size().unwrap(), 20, "reading past the end must not grow the file");

	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn zero_length_reads_and_writes_inside_the_file_change_nothing() {
	let root = root().await;
	let name = unique_name("zero-length", "bin");
	let file = create(&root, &name).await;
	let data = pattern(40, 10);
	file.write_at(0, &data).unwrap();

	for offset in [0u64, 5, 10] {
		assert_eq!(file.write_at(offset, &[]).unwrap(), 0, "empty write at {offset}");
		assert_eq!(file.size().unwrap(), 10, "empty write at {offset} must not change the size");
		assert_eq!(file.read_at(offset, &mut []).unwrap(), 0, "empty read at {offset}");
	}
	assert_eq!(file.read_at(5000, &mut []).unwrap(), 0, "an empty read far past the end");
	assert_eq!(read_all(&file), data, "empty operations must leave the contents alone");

	// An empty write into an empty file is fine too.
	let empty_name = unique_name("zero-length-empty", "bin");
	let empty = create(&root, &empty_name).await;
	assert_eq!(empty.write_at(0, &[]).unwrap(), 0);
	assert_eq!(empty.size().unwrap(), 0);
	assert_eq!(read_all(&empty), Vec::<u8>::new());
	empty.close();

	file.close();
	remove_and_verify(&root, &name).await;
	remove_and_verify(&root, &empty_name).await;
}

#[wasm_bindgen_test]
#[ignore = "a zero-length write_at beyond the end makes size() report the offset while the file keeps its real length"]
async fn a_zero_length_write_beyond_the_end_does_not_change_the_reported_size() {
	let root = root().await;
	let name = unique_name("zero-length-beyond", "bin");
	let file = create(&root, &name).await;
	file.write_at(10, b"hello").unwrap();
	assert_eq!(file.size().unwrap(), 15);

	// The browser ignores a write of nothing, so the file stays 15 bytes long.
	assert_eq!(file.write_at(100, &[]).unwrap(), 0);
	let real_length = read_all(&file).len() as u64;
	assert_eq!(real_length, 15, "the browser does not extend a file by a write of nothing");
	assert_eq!(file.size().unwrap(), real_length, "size() must report the real length of the file");

	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn offsets_beyond_four_gibibytes_are_not_truncated_to_32_bits() {
	// wasm32 has 32-bit pointers, so a careless `as usize` or `as u32` on an offset silently wraps
	// at 4 GiB. A sparse write past that point costs the browser no storage (the gap is a hole).
	const FAR: u64 = (1 << 32) + 5;
	let root = root().await;
	let name = unique_name("far-offset", "bin");
	let file = create(&root, &name).await;
	file.write_at(0, b"near-start").unwrap();

	let written = match file.write_at(FAR, b"far") {
		Ok(n) => n,
		Err(e) => {
			// A browser that will not grow a file this far (storage quota) cannot be tested here.
			assert!(text(&e).contains("QuotaExceededError"), "write at {FAR}: {e:?}");
			file.close();
			remove_and_verify(&root, &name).await;
			return;
		}
	};
	assert_eq!(written, 3);
	assert_eq!(file.size().unwrap(), FAR + 3, "size() after a write ending at {}", FAR + 3);

	// The bytes are where they were written...
	assert_eq!(read_exact(&file, FAR, 3), b"far");
	// ...and not at the offset a 32-bit truncation would have used (5), where "start" still is.
	assert_eq!(read_exact(&file, 0, 10), b"near-start", "the start of the file is untouched");
	let mut probe = [0xEEu8; 8];
	assert_eq!(file.read_at(FAR, &mut probe).unwrap(), 3, "exactly 3 bytes remain at {FAR}");
	assert_eq!(file.read_at(FAR + 3, &mut probe).unwrap(), 0, "nothing after the far write");

	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn an_eight_mebibyte_file_written_in_uneven_chunks_reads_back_with_the_same_checksum() {
	const TOTAL: usize = 8 * 1024 * 1024;
	const SEED: u64 = 0xB16_F11E;
	let root = root().await;
	let name = unique_name("large", "bin");
	let file = create(&root, &name).await;

	let data = pattern(SEED, TOTAL);
	let expected_crc = crc32fast::hash(&data);

	// Write it in chunks of unrelated, awkward sizes.
	let write_sizes = [1usize, 4093, 65_537, 100_003, 7, 262_149, 33, 1_000_003];
	let mut offset = 0usize;
	let mut round = 0usize;
	while offset < TOTAL {
		let len = write_sizes[round % write_sizes.len()].min(TOTAL - offset);
		let written = file.write_at(offset as u64, &data[offset..offset + len]).unwrap();
		assert_eq!(written, len, "write of {len} bytes at {offset}");
		offset += len;
		assert_eq!(file.size().unwrap(), offset as u64, "size after writing up to {offset}");
		round += 1;
	}

	// Read it back in chunks of different sizes and checksum the stream.
	let read_sizes = [8191usize, 524_287, 3, 1_048_576, 65_536];
	let mut hasher = crc32fast::Hasher::new();
	let mut offset = 0usize;
	let mut round = 0usize;
	while offset < TOTAL {
		let len = read_sizes[round % read_sizes.len()].min(TOTAL - offset);
		let mut buf = vec![0u8; len];
		let n = file.read_at(offset as u64, &mut buf).unwrap();
		assert_eq!(n, len, "read of {len} bytes at {offset}");
		hasher.update(&buf);
		offset += len;
		round += 1;
	}
	assert_eq!(hasher.finalize(), expected_crc, "checksum of the file read back in chunks");
	assert_eq!(file.size().unwrap(), TOTAL as u64);

	// Random windows must equal the same window of the source, wherever they fall.
	let mut rng = Rng::new(SEED);
	for i in 0..100 {
		let len = 1 + rng.below(70_000) as usize;
		let start = rng.below((TOTAL - len) as u64) as usize;
		assert_eq!(
			read_exact(&file, start as u64, len).as_slice(),
			&data[start..start + len],
			"window {i}: {len} bytes at {start}, seed {SEED}"
		);
	}
	// And nothing is left after the last byte.
	let mut tail = [0u8; 16];
	assert_eq!(file.read_at(TOTAL as u64, &mut tail).unwrap(), 0);

	file.flush().unwrap();
	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn thousands_of_small_writes_in_shuffled_order_build_the_expected_file() {
	const RECORDS: usize = 3000;
	const RECORD: usize = 16;
	const SEED: u64 = 0x5EED_0001;
	let root = root().await;
	let name = unique_name("small-writes", "bin");
	let file = create(&root, &name).await;

	let data = pattern(SEED ^ 0xD, RECORDS * RECORD);
	let mut rng = Rng::new(SEED);
	let mut order: Vec<usize> = (0..RECORDS).collect();
	for i in (1..order.len()).rev() {
		order.swap(i, rng.below(i as u64 + 1) as usize);
	}

	// Records land out of order, so the file grows by jumps and leaves holes that later writes
	// fill. The size must always be the end of the furthest write so far.
	let mut furthest = 0u64;
	for &i in &order {
		let offset = (i * RECORD) as u64;
		let record = &data[i * RECORD..(i + 1) * RECORD];
		assert_eq!(file.write_at(offset, record).unwrap(), RECORD, "record {i}, seed {SEED}");
		furthest = furthest.max(offset + RECORD as u64);
		assert_eq!(file.size().unwrap(), furthest, "size after record {i}, seed {SEED}");
	}

	assert_eq!(file.size().unwrap(), (RECORDS * RECORD) as u64);
	assert_eq!(read_all(&file), data, "the file after all records, seed {SEED}");
	for _ in 0..200 {
		let i = rng.below(RECORDS as u64) as usize;
		assert_eq!(
			read_exact(&file, (i * RECORD) as u64, RECORD).as_slice(),
			&data[i * RECORD..(i + 1) * RECORD],
			"record {i}, seed {SEED}"
		);
	}

	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn writes_to_two_open_files_do_not_touch_each_other() {
	let root = root().await;
	let (name_a, name_b) = (unique_name("pair-a", "bin"), unique_name("pair-b", "bin"));
	let a = create(&root, &name_a).await;
	let b = create(&root, &name_b).await;

	let (mut model_a, mut model_b) = (Vec::new(), Vec::new());
	for round in 0..20u64 {
		let piece_a = pattern(100 + round, 37 + round as usize);
		let piece_b = pattern(200 + round, 91 - round as usize);
		a.write_at(model_a.len() as u64, &piece_a).unwrap();
		model_a.extend_from_slice(&piece_a);
		b.write_at(model_b.len() as u64, &piece_b).unwrap();
		model_b.extend_from_slice(&piece_b);
		assert_eq!(a.size().unwrap(), model_a.len() as u64);
		assert_eq!(b.size().unwrap(), model_b.len() as u64);
	}
	assert_eq!(read_all(&a), model_a, "file a");
	assert_eq!(read_all(&b), model_b, "file b");

	a.close();
	b.close();
	remove_and_verify(&root, &name_a).await;
	remove_and_verify(&root, &name_b).await;
}

// ---------------------------------------------------------------------------------------------
// 3. Persistence
// ---------------------------------------------------------------------------------------------

/// Writes three separated segments (so the file has holes) and returns the expected contents.
fn write_segments(file: &surrealkv::storage::opfs::OpfsSyncFile, seed: u64) -> Vec<u8> {
	let mut model = vec![0u8; 4096 + 300];
	for (i, (offset, len)) in [(0usize, 100usize), (150, 50), (4096, 300)].into_iter().enumerate() {
		let piece = pattern(seed + i as u64, len);
		assert_eq!(file.write_at(offset as u64, &piece).unwrap(), len);
		model[offset..offset + len].copy_from_slice(&piece);
	}
	model
}

#[wasm_bindgen_test]
async fn flushed_data_is_readable_after_close_and_reopen_with_create_false() {
	let root = root().await;
	let name = unique_name("persist-flushed", "bin");
	let file = create(&root, &name).await;
	let model = write_segments(&file, 300);
	file.flush().unwrap();
	assert_eq!(file.size().unwrap(), model.len() as u64);
	file.close();

	let again = reopen(&root, &name).await;
	assert_eq!(again.size().unwrap(), model.len() as u64, "the reopened file knows its size");
	assert_eq!(read_all(&again), model, "every flushed byte, holes included, survives a reopen");
	again.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn data_written_without_a_flush_is_readable_after_close_and_reopen() {
	// The backend does not document that unflushed data may be lost, so closing and reopening
	// must show everything that was written.
	let root = root().await;
	let name = unique_name("persist-unflushed", "bin");
	let file = create(&root, &name).await;
	let model = write_segments(&file, 310);
	file.close();

	let again = reopen(&root, &name).await;
	assert_eq!(again.size().unwrap(), model.len() as u64);
	assert_eq!(read_all(&again), model, "data written but not flushed must still be there");
	again.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn reopening_with_create_true_keeps_the_existing_contents() {
	let root = root().await;
	let name = unique_name("persist-create-true", "bin");
	let file = create(&root, &name).await;
	let model = write_segments(&file, 320);
	file.flush().unwrap();
	file.close();

	// `create = true` on an existing file opens it, it does not truncate it.
	let again = create(&root, &name).await;
	assert_eq!(again.size().unwrap(), model.len() as u64);
	assert_eq!(read_all(&again), model);
	again.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn a_file_survives_repeated_reopen_write_close_cycles() {
	const SEED: u64 = 0xC1C1E5;
	let root = root().await;
	let name = unique_name("persist-cycles", "bin");
	let mut rng = Rng::new(SEED);
	let mut model: Vec<u8> = Vec::new();

	for round in 0..8usize {
		let file = if round == 0 {
			create(&root, &name).await
		} else {
			reopen(&root, &name).await
		};
		assert_eq!(file.size().unwrap(), model.len() as u64, "size at round {round}, seed {SEED}");
		assert_eq!(read_all(&file), model, "contents at round {round}, seed {SEED}");

		// Append a chunk at the end...
		let chunk = pattern(SEED + round as u64, 1000 + round * 37);
		assert_eq!(file.write_at(model.len() as u64, &chunk).unwrap(), chunk.len());
		model.extend_from_slice(&chunk);
		// ... and overwrite a few bytes somewhere inside what was there before.
		if round > 0 {
			let len = 10;
			let offset = rng.below((model.len() - chunk.len() - len) as u64) as usize;
			let patch = pattern(SEED ^ round as u64, len);
			file.write_at(offset as u64, &patch).unwrap();
			model[offset..offset + len].copy_from_slice(&patch);
		}
		if round % 2 == 0 {
			file.flush().unwrap();
		}
		assert_eq!(file.size().unwrap(), model.len() as u64);
		file.close();
	}

	let last = reopen(&root, &name).await;
	assert_eq!(read_all(&last), model, "final contents, seed {SEED}");
	last.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn flushing_changes_neither_contents_nor_size_and_works_on_an_empty_file() {
	let root = root().await;
	let name = unique_name("flush", "bin");
	let file = create(&root, &name).await;
	file.flush().expect("flush of an empty file");
	assert_eq!(file.size().unwrap(), 0);

	let data = pattern(330, 500);
	file.write_at(0, &data).unwrap();
	for _ in 0..3 {
		file.flush().expect("flush");
		assert_eq!(file.size().unwrap(), 500);
		assert_eq!(read_all(&file), data);
	}
	file.close();
	remove_and_verify(&root, &name).await;
}

// ---------------------------------------------------------------------------------------------
// 4. Exclusivity and closing
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn a_second_handle_on_an_open_file_fails_and_succeeds_after_close() {
	let root = root().await;
	let name = unique_name("exclusive", "bin");
	let first = create(&root, &name).await;
	let data = pattern(400, 64);
	first.write_at(0, &data).unwrap();

	// The File System Standard reports a second sync access handle as NoModificationAllowedError.
	for create_flag in [false, true] {
		let err = expect_err(
			open_opfs_sync_file(&root, &name, create_flag).await,
			"a second handle on an open file",
		);
		assert!(err.contains("NoModificationAllowedError"), "create = {create_flag}: {err}");
	}

	// The failed attempts did not disturb the first handle.
	assert_eq!(read_exact(&first, 0, 64), data);
	let more = pattern(401, 16);
	first.write_at(64, &more).unwrap();
	assert_eq!(first.size().unwrap(), 80);
	first.close();

	// Once closed, the file can be opened again, by exactly one handle at a time.
	let second = reopen(&root, &name).await;
	assert_eq!(read_all(&second), [data.clone(), more.clone()].concat());
	let err = expect_err(open_opfs_sync_file(&root, &name, false).await, "a handle during the 2nd");
	assert!(err.contains("NoModificationAllowedError"), "{err}");
	second.close();

	let third = reopen(&root, &name).await;
	assert_eq!(third.size().unwrap(), 80);
	third.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn an_open_file_cannot_be_removed_until_it_is_closed() {
	let root = root().await;
	let name = unique_name("remove-while-open", "bin");
	let file = create(&root, &name).await;
	file.write_at(0, b"still here").unwrap();

	let err = JsFuture::from(root.remove_entry(&name)).await.expect_err("remove of an open file");
	assert!(text(&err).contains("NoModificationAllowedError"), "{}", text(&err));
	assert_eq!(read_exact(&file, 0, 10), b"still here", "the failed removal left the file alone");

	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn closing_a_file_twice_is_harmless() {
	let root = root().await;
	let name = unique_name("close-twice", "bin");
	let file = create(&root, &name).await;
	let data = pattern(410, 33);
	file.write_at(0, &data).unwrap();

	file.close();
	file.close();
	file.close();

	// The worker is alive, the lock is released exactly once and the data is intact.
	let again = reopen(&root, &name).await;
	assert_eq!(read_all(&again), data);
	again.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn a_closed_file_rejects_reads_writes_and_flushes_and_keeps_its_data() {
	let root = root().await;
	let name = unique_name("use-after-close", "bin");
	let file = create(&root, &name).await;
	let data = pattern(420, 40);
	file.write_at(0, &data).unwrap();
	file.flush().unwrap();
	file.close();

	let mut buf = [0u8; 8];
	let err = expect_err(file.read_at(0, &mut buf), "read_at on a closed file");
	assert!(err.contains("InvalidStateError"), "{err}");
	let err = expect_err(file.write_at(0, b"XXXX"), "write_at on a closed file");
	assert!(err.contains("InvalidStateError"), "{err}");
	let err = expect_err(file.write_at(1000, b"XXXX"), "write_at beyond the end on a closed file");
	assert!(err.contains("InvalidStateError"), "{err}");
	let err = expect_err(file.flush(), "flush on a closed file");
	assert!(err.contains("InvalidStateError"), "{err}");
	// An empty read is no exception. (An empty write is deliberately not asserted: the browser
	// does reject it, but returning early for empty buffers is the natural fix for the ignored
	// zero-length-write defect above and must stay possible.)
	let err = expect_err(file.read_at(0, &mut []), "an empty read_at on a closed file");
	assert!(err.contains("InvalidStateError"), "{err}");
	assert_eq!(file.size().unwrap(), 40, "a failed write must not move the reported size");

	// None of the failed writes reached the file.
	let again = reopen(&root, &name).await;
	assert_eq!(again.size().unwrap(), 40);
	assert_eq!(read_all(&again), data);
	again.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
#[ignore = "dropping an OpfsSyncFile without calling close() keeps the exclusive handle, so the file cannot be opened again"]
async fn dropping_an_unclosed_file_releases_its_exclusive_handle() {
	let root = root().await;
	let name = unique_name("drop-unclosed", "bin");
	let file = create(&root, &name).await;
	file.write_at(0, b"abc").unwrap();
	drop(file);

	// Nothing refers to the file any more, so it must be possible to open it again. Today the
	// browser still sees an open handle and answers NoModificationAllowedError.
	let again = open_opfs_sync_file(&root, &name, false).await;
	assert!(again.is_ok(), "reopening after a drop failed: {:?}", again.err());
	again.unwrap().close();
	remove_and_verify(&root, &name).await;
}

// ---------------------------------------------------------------------------------------------
// 5. Open errors and names
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn opening_a_missing_file_without_create_fails_and_does_not_create_it() {
	let root = root().await;
	let name = unique_name("missing", "bin");

	for attempt in 0..2 {
		let err =
			expect_err(open_opfs_sync_file(&root, &name, false).await, "open of a missing file");
		assert!(err.contains("NotFoundError"), "attempt {attempt}: {err}");
	}

	// Only `create = true` makes it, empty, and from then on `create = false` finds it.
	let file = create(&root, &name).await;
	assert_eq!(file.size().unwrap(), 0, "a file made by create = true starts empty");
	file.close();
	let file = reopen(&root, &name).await;
	file.close();
	remove_and_verify(&root, &name).await;
}

#[wasm_bindgen_test]
async fn empty_names_and_names_with_separators_or_dot_segments_are_rejected() {
	let root = root().await;
	for name in ["", "/", "a/b", "/leading", "trailing/", "dir/../x", ".", ".."] {
		for create_flag in [true, false] {
			let err = expect_err(
				open_opfs_sync_file(&root, name, create_flag).await,
				&format!("open of the invalid name {name:?} with create = {create_flag}"),
			);
			assert!(err.contains("TypeError"), "name {name:?}, create = {create_flag}: {err}");
		}
	}
}

#[wasm_bindgen_test]
async fn very_long_names_are_rejected_or_behave_like_any_other_name() {
	// The standard sets no limit on name length, so a browser may accept or refuse these. What it
	// must never do is accept a name and then mix it up with another one.
	let root = root().await;
	for len in [300usize, 1000, 4096] {
		let name = format!("{}-{}", "L".repeat(len), unique_name("long", "bin"));
		match open_opfs_sync_file(&root, &name, true).await {
			Ok(file) => {
				let data = pattern(500 + len as u64, 100);
				file.write_at(0, &data).unwrap();
				file.close();
				let again = reopen(&root, &name).await;
				assert_eq!(read_all(&again), data, "a {len}-character name");
				again.close();
				remove_and_verify(&root, &name).await;
			}
			Err(e) => {
				let refused = open_opfs_sync_file(&root, &name, false).await;
				assert!(
					refused.is_err(),
					"a {len}-character name was refused ({e:?}) but then exists"
				);
			}
		}
	}

	// Two names that agree on their first 255 characters must be two files.
	let base = format!("{}{}", "P".repeat(255), unique_name("long-prefix", "x"));
	let (name_a, name_b) = (format!("{base}-a"), format!("{base}-b"));
	let a = open_opfs_sync_file(&root, &name_a, true).await;
	let b = open_opfs_sync_file(&root, &name_b, true).await;
	match (a, b) {
		(Ok(a), Ok(b)) => {
			a.write_at(0, b"first").unwrap();
			b.write_at(0, b"second!").unwrap();
			assert_eq!(read_all(&a), b"first");
			assert_eq!(read_all(&b), b"second!");
			a.close();
			b.close();
			remove_and_verify(&root, &name_a).await;
			remove_and_verify(&root, &name_b).await;
		}
		(Err(_), Err(_)) => {}
		(a, b) => panic!(
			"two names of the same length that differ only in the last character must both open \
			 or both be refused, got {:?} and {:?}",
			a.err(),
			b.err()
		),
	}
}

#[wasm_bindgen_test]
async fn unicode_and_space_names_work_and_stay_distinct() {
	let root = root().await;
	let stem = unique_name("names", "x");
	let names: Vec<String> = [
		"t\u{eb}st \u{65e5}\u{672c}\u{8a9e} \u{1f980} file",
		"with space",
		"double  space",
		"a:b*c?d",
		"\u{1f980}",
		"plain",
		// Names that differ only by case are different files: nothing may fold or normalise them.
		"Case",
		"case",
		"CASE",
	]
	.iter()
	.map(|base| format!("{base}-{stem}.bin"))
	.collect();

	// Each file holds its own name, so a mix-up of two names shows in the contents.
	for name in &names {
		let file = create(&root, name).await;
		file.write_at(0, name.as_bytes()).unwrap();
		file.flush().unwrap();
		file.close();
	}
	for name in &names {
		let file = reopen(&root, name).await;
		assert_eq!(read_all(&file), name.as_bytes(), "the file named {name:?}");
		assert_eq!(file.size().unwrap(), name.len() as u64);
		file.close();
	}
	for name in &names {
		remove_and_verify(&root, name).await;
	}
}

// ---------------------------------------------------------------------------------------------
// 6. Root handle
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn get_opfs_root_can_be_called_repeatedly_and_every_handle_sees_the_same_files() {
	let first = root().await;
	let name = unique_name("root-repeat", "bin");
	let file = create(&first, &name).await;
	file.write_at(0, b"visible everywhere").unwrap();
	file.close();

	for call in 0..25 {
		let handle = root().await;
		let same = JsFuture::from(first.is_same_entry(&handle))
			.await
			.unwrap_or_else(|e| panic!("isSameEntry failed on call {call}: {e:?}"));
		assert_eq!(same.as_bool(), Some(true), "call {call} returned a different directory");

		// And the handle works: it finds the file the first one created.
		let file = reopen(&handle, &name).await;
		assert_eq!(read_all(&file), b"visible everywhere", "through the root of call {call}");
		file.close();
	}
	remove_and_verify(&first, &name).await;
}
