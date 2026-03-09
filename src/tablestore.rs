use std::sync::Arc;

use bytes::Bytes;
use integer_encoding::FixedInt;
use object_store::ObjectStore;

use crate::cache::BlockCache;
use crate::error::{Error, Result};
use crate::paths::PathResolver;
use crate::sstable::block::{Block, BlockHandle};
use crate::sstable::error::SSTableError;
use crate::sstable::sst_id::SstId;
use crate::sstable::table::{
	decompress_block,
	unmask,
	Footer,
	BLOCK_CKSUM_LEN,
	BLOCK_COMPRESS_LEN,
	TABLE_FULL_FOOTER_LENGTH,
};
use crate::{Comparator, CompressionType};

/// Central abstraction for SST lifecycle management over object store.
///
/// Replaces all direct file I/O for SSTs. Handles:
/// - Atomic uploads (memtable flush)
/// - Streaming writes (compaction)
/// - Block reads with cache integration
/// - Point lookups (bloom → index → data block)
/// - SST deletion and listing
pub(crate) struct TableStore {
	object_store: Arc<dyn ObjectStore>,
	path_resolver: PathResolver,
	block_cache: Arc<BlockCache>,
}

impl TableStore {
	pub(crate) fn new(
		object_store: Arc<dyn ObjectStore>,
		path_resolver: PathResolver,
		block_cache: Arc<BlockCache>,
	) -> Self {
		Self {
			object_store,
			path_resolver,
			block_cache,
		}
	}

	/// Returns a reference to the underlying object store.
	pub(crate) fn object_store(&self) -> &Arc<dyn ObjectStore> {
		&self.object_store
	}

	/// Returns a reference to the path resolver.
	pub(crate) fn path_resolver(&self) -> &PathResolver {
		&self.path_resolver
	}

	/// Returns a reference to the block cache.
	pub(crate) fn block_cache(&self) -> &Arc<BlockCache> {
		&self.block_cache
	}

	/// Delete an SST from the object store.
	pub(crate) async fn delete_sst(&self, id: &SstId) -> Result<()> {
		let path = self.path_resolver.table_path(id);
		self.object_store.delete(&path).await?;
		Ok(())
	}

	/// Read a byte range from an SST file.
	pub(crate) async fn read_range(
		&self,
		id: &SstId,
		range: std::ops::Range<u64>,
	) -> Result<Bytes> {
		let path = self.path_resolver.table_path(id);
		Ok(self.object_store.get_range(&path, range).await?)
	}

	/// Upload an SST as a single atomic put.
	pub(crate) async fn write_sst(&self, id: &SstId, data: Bytes) -> Result<()> {
		let path = self.path_resolver.table_path(id);
		self.object_store.put(&path, data.into()).await?;
		Ok(())
	}

	/// Get the size of an SST file via HEAD request.
	pub(crate) async fn get_sst_size(&self, id: &SstId) -> Result<u64> {
		let path = self.path_resolver.table_path(id);
		let meta = self.object_store.head(&path).await?;
		Ok(meta.size as u64)
	}

	/// Read the footer from the end of an SST file.
	pub(crate) async fn read_footer(&self, id: &SstId, file_size: u64) -> Result<Footer> {
		if (file_size as usize) < TABLE_FULL_FOOTER_LENGTH {
			return Err(Error::from(SSTableError::FileTooSmall {
				file_size: file_size as usize,
				min_size: TABLE_FULL_FOOTER_LENGTH,
			}));
		}
		let offset = file_size - TABLE_FULL_FOOTER_LENGTH as u64;
		let buf = self.read_range(id, offset..file_size).await?;
		Footer::decode(&buf)
	}

	/// Read raw bytes at a block handle location.
	pub(crate) async fn read_block_bytes(
		&self,
		id: &SstId,
		location: &BlockHandle,
	) -> Result<Bytes> {
		let start = location.offset() as u64;
		let end = start + location.size() as u64;
		self.read_range(id, start..end).await
	}

	/// Write a manifest to the object store.
	pub(crate) async fn write_manifest(&self, id: u64, data: Bytes) -> Result<()> {
		let path = self.path_resolver.manifest_path(id);
		self.object_store.put(&path, data.into()).await?;
		Ok(())
	}

	/// Read a manifest from the object store.
	pub(crate) async fn read_manifest(&self, id: u64) -> Result<Bytes> {
		let path = self.path_resolver.manifest_path(id);
		let result = self.object_store.get(&path).await?;
		Ok(result.bytes().await?)
	}

	/// List all manifest IDs in the object store, sorted ascending.
	pub(crate) async fn list_manifest_ids(&self) -> Result<Vec<u64>> {
		let prefix = self.path_resolver.manifest_prefix();
		let list_result = self.object_store.list_with_delimiter(Some(&prefix)).await?;
		let mut ids = Vec::new();
		for obj in list_result.objects {
			if let Some(name) = obj.location.filename() {
				if let Some(id_str) = name.strip_suffix(".manifest") {
					if let Ok(id) = id_str.parse::<u64>() {
						ids.push(id);
					}
				}
			}
		}
		ids.sort();
		Ok(ids)
	}

	/// Delete a manifest from the object store.
	pub(crate) async fn delete_manifest(&self, id: u64) -> Result<()> {
		let path = self.path_resolver.manifest_path(id);
		self.object_store.delete(&path).await?;
		Ok(())
	}

	/// List all SST IDs in the object store.
	pub(crate) async fn list_sst_ids(&self) -> Result<Vec<SstId>> {
		let prefix = self.path_resolver.sst_path();
		let list_result = self.object_store.list_with_delimiter(Some(&prefix)).await?;
		let mut ids = Vec::new();
		for obj in list_result.objects {
			let filename = obj.location.filename().unwrap_or_default();
			if let Some(stem) = filename.strip_suffix(".sst") {
				if let Ok(id) = stem.parse::<SstId>() {
					ids.push(id);
				}
			}
		}
		Ok(ids)
	}

	/// Read and verify a table block from the object store.
	///
	/// Performs a single range read for the block data + compression byte + checksum,
	/// avoiding 3 separate round-trips.
	pub(crate) async fn read_table_block(
		&self,
		id: &SstId,
		location: &BlockHandle,
		comparator: Arc<dyn Comparator>,
	) -> Result<Block> {
		// Single range read: block_data | compress_byte | crc32
		let total_size = location.size() + BLOCK_COMPRESS_LEN + BLOCK_CKSUM_LEN;
		let start = location.offset() as u64;
		let end = start + total_size as u64;
		let buf = self.read_range(id, start..end).await?;

		let block_data = &buf[..location.size()];
		let compress_byte = buf[location.size()];
		let cksum_bytes = &buf[location.size() + BLOCK_COMPRESS_LEN..];

		// Verify checksum
		let stored_cksum = unmask(u32::decode_fixed(cksum_bytes).unwrap());
		let mut hasher = crc32fast::Hasher::new();
		hasher.update(block_data);
		hasher.update(&[compress_byte; BLOCK_COMPRESS_LEN]);
		if hasher.finalize() != stored_cksum {
			return Err(Error::from(SSTableError::ChecksumVerificationFailed {
				block_offset: location.offset() as u64,
			}));
		}

		// Decompress
		let decompressed = decompress_block(block_data, CompressionType::try_from(compress_byte)?)?;
		Ok(Block::new(decompressed, comparator))
	}
}
