use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path;
use object_store::ObjectStore;

use crate::error::Result;

/// Async read-only access to a blob (file) in an object store.
pub(crate) trait ReadOnlyBlob: Send + Sync {
	/// Returns the size of the blob in bytes.
	fn len(&self) -> impl std::future::Future<Output = Result<u64>> + Send;

	/// Reads a byte range from the blob.
	fn read_range(
		&self,
		range: Range<u64>,
	) -> impl std::future::Future<Output = Result<Bytes>> + Send;

	/// Reads the entire blob.
	fn read(&self) -> impl std::future::Future<Output = Result<Bytes>> + Send;
}

/// A read-only blob backed by an object store object.
pub(crate) struct ReadOnlyObject {
	store: Arc<dyn ObjectStore>,
	path: Path,
}

impl ReadOnlyObject {
	pub(crate) fn new(store: Arc<dyn ObjectStore>, path: Path) -> Self {
		Self {
			store,
			path,
		}
	}
}

impl ReadOnlyBlob for ReadOnlyObject {
	async fn len(&self) -> Result<u64> {
		Ok(self.store.head(&self.path).await?.size as u64)
	}

	async fn read_range(&self, range: Range<u64>) -> Result<Bytes> {
		Ok(self.store.get_range(&self.path, range).await?)
	}

	async fn read(&self) -> Result<Bytes> {
		Ok(self.store.get(&self.path).await?.bytes().await?)
	}
}
