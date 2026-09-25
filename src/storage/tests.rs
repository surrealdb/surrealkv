use std::sync::Arc;

use tempfile::TempDir;

use super::*;

#[tokio::test]
async fn test_mem_log_store() {
	let store = MemLogStore::new();
	let off1 = store.append(b"hello ").await.unwrap();
	assert_eq!(off1, 6);
	let off2 = store.append(b"world").await.unwrap();
	assert_eq!(off2, 11);
	store.sync().await.unwrap();
	assert_eq!(store.size().await.unwrap(), 11);
}

#[tokio::test]
async fn test_mem_object_store() {
	let store = MemObjectStore::new(b"hello world".as_slice());
	let slice = store.read_at(6, 5).await.unwrap();
	assert_eq!(slice.as_ref(), b"world");
	assert_eq!(store.size().await.unwrap(), 11);
}

#[tokio::test]
async fn test_affinity_object_store() {
	let temp_dir = TempDir::new().unwrap();
	let file_path = temp_dir.path().join("test_obj.bin");
	std::fs::write(&file_path, b"hello affinity object store").unwrap();

	let file = Arc::new(std::fs::File::open(&file_path).unwrap());
	let store = AffinityObjectStore::new(file);

	let slice = store.read_at(6, 8).await.unwrap();
	assert_eq!(slice.as_ref(), b"affinity");
	let size = store.size().await.unwrap();
	assert_eq!(size, 27);
}

#[tokio::test]
async fn test_affinity_log_store() {
	let temp_dir = TempDir::new().unwrap();
	let wal_opts = crate::wal::Options::default();
	let wal = Arc::new(parking_lot::RwLock::new(
		crate::wal::manager::Wal::open(temp_dir.path(), wal_opts).unwrap(),
	));
	let store = AffinityLogStore::new(wal);

	store.append(b"wal entry 1").await.unwrap();
	store.append(b"wal entry 2").await.unwrap();
	store.sync().await.unwrap();
}
