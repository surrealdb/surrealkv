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

#[tokio::test]
async fn test_affinity_log_store_append_returning_segment() {
	let temp_dir = TempDir::new().unwrap();
	let wal = Arc::new(parking_lot::RwLock::new(
		crate::wal::manager::Wal::open(temp_dir.path(), crate::wal::Options::default()).unwrap(),
	));
	let store = AffinityLogStore::new(Arc::clone(&wal));
	let first = wal.read().get_active_log_number();

	assert_eq!(store.append_returning_segment(b"before-a").await.unwrap(), first);
	assert_eq!(store.append_returning_segment(b"before-b").await.unwrap(), first);

	// A rotation between two appends: the next record reports the new segment.
	wal.write().rotate().unwrap();
	assert_eq!(store.append_returning_segment(b"after").await.unwrap(), first + 1);
	assert_eq!(store.append_returning_segment(b"after-2").await.unwrap(), first + 1);
	store.sync().await.unwrap();

	// Each record is in the segment it was reported in, and only there.
	let segment =
		|id: u64| std::fs::read(temp_dir.path().join(crate::wal::segment_name(id, "wal"))).unwrap();
	let contains = |bytes: &[u8], needle: &[u8]| bytes.windows(needle.len()).any(|w| w == needle);
	assert!(contains(&segment(first), b"before-a") && contains(&segment(first), b"before-b"));
	assert!(!contains(&segment(first), b"after"));
	assert!(contains(&segment(first + 1), b"after") && contains(&segment(first + 1), b"after-2"));
	assert!(!contains(&segment(first + 1), b"before-a"));

	// An append that fails reports the error, not a segment.
	wal.write().close().unwrap();
	assert!(store.append_returning_segment(b"closed").await.is_err());
}
