//! Asynchronous Storage Abstractions (LogStore and ObjectStore).
//!
//! Inspired by ShaleDB and SlateDB architectures:
//! - `LogStore`: Optimized for the WAL (append-only sequential persistence, fast sync).
//! - `ObjectStore`: Optimized for immutable SSTables (write once, seal, concurrent random reads).

#[cfg(target_arch = "wasm32")]
pub mod opfs;

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::error::Result;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// Trait for append-only sequential log persistence (WAL).
pub trait LogStore: Send + Sync + Debug {
	/// Appends data to the log. Returns the new offset or position.
	fn append(&self, data: &[u8]) -> BoxFuture<'_, u64>;

	/// Flushes and syncs all buffered writes to durable storage.
	fn sync(&self) -> BoxFuture<'_, ()>;

	/// Current logical size of the log in bytes.
	fn size(&self) -> BoxFuture<'_, u64>;
}

/// Trait for immutable object/page storage (SSTables).
pub trait ObjectStore: Send + Sync + Debug {
	/// Reads a range of bytes `[offset..offset + len)` from the object.
	fn read_at(&self, offset: u64, len: usize) -> BoxFuture<'_, Bytes>;

	/// Returns total size of the object in bytes.
	fn size(&self) -> BoxFuture<'_, u64>;
}

// ============================================================================
// In-Memory Implementations (For tests & mock simulator)
// ============================================================================

/// In-memory implementation of LogStore.
#[derive(Debug, Default)]
pub struct MemLogStore {
	buf: RwLock<Vec<u8>>,
}

impl MemLogStore {
	pub fn new() -> Self {
		Self {
			buf: RwLock::new(Vec::new()),
		}
	}
}

impl LogStore for MemLogStore {
	fn append(&self, data: &[u8]) -> BoxFuture<'_, u64> {
		let mut buf = self.buf.write();
		buf.extend_from_slice(data);
		let len = buf.len() as u64;
		Box::pin(async move { Ok(len) })
	}

	fn sync(&self) -> BoxFuture<'_, ()> {
		Box::pin(async move { Ok(()) })
	}

	fn size(&self) -> BoxFuture<'_, u64> {
		let len = self.buf.read().len() as u64;
		Box::pin(async move { Ok(len) })
	}
}

/// In-memory implementation of ObjectStore.
#[derive(Debug, Default)]
pub struct MemObjectStore {
	data: Bytes,
}

impl MemObjectStore {
	pub fn new(data: impl Into<Bytes>) -> Self {
		Self {
			data: data.into(),
		}
	}
}

impl ObjectStore for MemObjectStore {
	fn read_at(&self, offset: u64, len: usize) -> BoxFuture<'_, Bytes> {
		let start = offset as usize;
		let end = (offset as usize).saturating_add(len).min(self.data.len());
		let slice = if start <= self.data.len() {
			self.data.slice(start..end)
		} else {
			Bytes::new()
		};
		Box::pin(async move { Ok(slice) })
	}

	fn size(&self) -> BoxFuture<'_, u64> {
		let len = self.data.len() as u64;
		Box::pin(async move { Ok(len) })
	}
}

// ============================================================================
// AffinityPool Storage Implementations (macOS / Windows / Non-io_uring Linux)
// ============================================================================

/// Append-only log store backed by dedicated affinitypool worker threads and WAL manager.
pub struct AffinityLogStore {
	wal: Arc<parking_lot::RwLock<crate::wal::manager::Wal>>,
}

impl std::fmt::Debug for AffinityLogStore {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("AffinityLogStore").finish()
	}
}

impl AffinityLogStore {
	pub fn new(wal: Arc<parking_lot::RwLock<crate::wal::manager::Wal>>) -> Self {
		Self {
			wal,
		}
	}
}

impl LogStore for AffinityLogStore {
	fn append(&self, data: &[u8]) -> BoxFuture<'_, u64> {
		let wal = Arc::clone(&self.wal);
		let data = data.to_vec();
		Box::pin(async move {
			affinitypool::spawn(move || -> Result<u64> {
				let mut guard = wal.write();
				guard.append(&data).map_err(|e| crate::error::Error::Other(e.to_string()))
			})
			.await
		})
	}

	fn sync(&self) -> BoxFuture<'_, ()> {
		let wal = Arc::clone(&self.wal);
		Box::pin(async move {
			affinitypool::spawn(move || -> Result<()> {
				let mut guard = wal.write();
				guard.sync().map_err(|e| crate::error::Error::Other(e.to_string()))
			})
			.await
		})
	}

	fn size(&self) -> BoxFuture<'_, u64> {
		let wal = Arc::clone(&self.wal);
		Box::pin(async move {
			affinitypool::spawn(move || -> Result<u64> {
				let _guard = wal.read();
				Ok(0)
			})
			.await
		})
	}
}

/// Filesystem ObjectStore backed by dedicated affinitypool worker threads.
pub struct AffinityObjectStore {
	file: Arc<dyn crate::vfs::File>,
}

impl std::fmt::Debug for AffinityObjectStore {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("AffinityObjectStore").finish()
	}
}

impl AffinityObjectStore {
	pub fn new(file: Arc<dyn crate::vfs::File>) -> Self {
		Self {
			file,
		}
	}
}

impl ObjectStore for AffinityObjectStore {
	fn read_at(&self, offset: u64, len: usize) -> BoxFuture<'_, Bytes> {
		let file = Arc::clone(&self.file);
		Box::pin(async move {
			affinitypool::spawn(move || -> Result<Bytes> {
				let mut buf = vec![0u8; len];
				file.read_at(offset, &mut buf)?;
				Ok(Bytes::from(buf))
			})
			.await
		})
	}

	fn size(&self) -> BoxFuture<'_, u64> {
		let file = Arc::clone(&self.file);
		Box::pin(async move { affinitypool::spawn(move || -> Result<u64> { file.size() }).await })
	}
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
