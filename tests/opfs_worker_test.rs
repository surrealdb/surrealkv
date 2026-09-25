#![cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]

use surrealkv::storage::opfs::{get_opfs_root, open_opfs_sync_file, OpfsLogStore, OpfsObjectStore};
use surrealkv::storage::{LogStore, ObjectStore};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[wasm_bindgen_test]
async fn test_opfs_sync_file_read_write() {
	let root = get_opfs_root().await.expect("Failed to get OPFS root");
	let file =
		open_opfs_sync_file(&root, "test_file.bin", true).await.expect("Failed to open OPFS file");

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
}

#[wasm_bindgen_test]
async fn test_opfs_log_and_object_store() {
	let root = get_opfs_root().await.expect("Failed to get OPFS root");
	let log_file = std::sync::Arc::new(
		open_opfs_sync_file(&root, "test_wal.log", true).await.expect("Failed to open OPFS WAL"),
	);

	let log_store = OpfsLogStore::new(log_file);
	let offset = log_store.append(b"record 1").await.expect("append 1");
	assert_eq!(offset, 8);
	let offset2 = log_store.append(b"record 2").await.expect("append 2");
	assert_eq!(offset2, 16);
	log_store.sync().await.expect("sync");
	assert_eq!(log_store.size().await.unwrap(), 16);

	let obj_file = std::sync::Arc::new(
		open_opfs_sync_file(&root, "test_sst.sst", true).await.expect("Failed to open OPFS SST"),
	);
	obj_file.write_at(0, b"abcdefghijklmnop").expect("write obj");
	obj_file.flush().expect("flush obj");

	let obj_store = OpfsObjectStore::new(obj_file);
	let bytes = obj_store.read_at(4, 6).await.expect("read obj");
	assert_eq!(&bytes[..], b"efghij");
	assert_eq!(obj_store.size().await.unwrap(), 16);
}
