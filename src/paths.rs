use object_store::path::Path;

use crate::sstable::sst_id::SstId;

/// Resolves object store paths for SSTs and manifests.
pub(crate) struct PathResolver {
	root: Path,
}

impl PathResolver {
	pub(crate) fn new(root: impl Into<Path>) -> Self {
		Self {
			root: root.into(),
		}
	}

	/// Returns the object store path for an SST.
	pub(crate) fn table_path(&self, id: &SstId) -> Path {
		self.root.child("sst").child(format!("{id}.sst"))
	}

	/// Returns the object store path for a manifest file.
	pub(crate) fn manifest_path(&self, id: u64) -> Path {
		self.root.child("manifest").child(format!("{id:020}.manifest"))
	}

	/// Returns the SST directory prefix.
	pub(crate) fn sst_path(&self) -> Path {
		self.root.child("sst")
	}

	/// Returns the manifest directory prefix.
	pub(crate) fn manifest_prefix(&self) -> Path {
		self.root.child("manifest")
	}

	/// Returns the root path.
	pub(crate) fn root(&self) -> &Path {
		&self.root
	}
}
