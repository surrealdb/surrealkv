use revision::{revisioned, Revisioned};
use std::path::Path;

use super::reader::Reader;
use crate::log::{write_field, Error as LogError, MultiSegmentReader, SegmentRef};
use crate::vfs::FileSystem;
use crate::{Error, Options, Result};

#[revisioned(revision = 1)]
#[derive(Debug, Clone)]
pub(crate) enum ManifestChangeType {
    Options(Options),
    CompactedUpToSegment(u64),
}

#[revisioned(revision = 1)]
#[derive(Debug, Clone)]
pub(crate) struct Manifest {
    pub(crate) changes: Vec<ManifestChangeType>,
}

impl Manifest {
    pub(crate) fn new() -> Self {
        Manifest {
            changes: Vec::new(),
        }
    }

    // Append a Manifest to a file containing a Vec<Manifest>
    pub(crate) fn serialize(&self) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        self.serialize_revisioned(&mut data)?;

        let mut buf = Vec::new();
        write_field(&data, &mut buf).unwrap();

        Ok(buf)
    }

    #[allow(unused)]
    pub(crate) fn extract_options(&self) -> Vec<Options> {
        self.changes
            .iter()
            .filter_map(|change_op| match change_op {
                ManifestChangeType::Options(options) => Some(options.clone()),
                _ => None,
            })
            .collect()
    }

    // Extract the last Option, irrespective of the operation type
    pub(crate) fn extract_last_option(&self) -> Option<Options> {
        self.changes
            .iter()
            .filter_map(|change_op| match change_op {
                ManifestChangeType::Options(options) => Some(options),
                _ => None,
            })
            .next_back()
            .cloned()
    }

    // Create a new manifest with an update change for an option
    pub(crate) fn with_update_option_change(opt: &Options) -> Self {
        let changes = vec![ManifestChangeType::Options(opt.clone())];
        Manifest { changes }
    }

    pub(crate) fn with_compacted_up_to_segment(segment: u64) -> Self {
        let changes = vec![ManifestChangeType::CompactedUpToSegment(segment)];
        Manifest { changes }
    }

    pub(crate) fn extract_compacted_up_to_segments(&self) -> Vec<u64> {
        self.changes
            .iter()
            .filter_map(|change_op| match change_op {
                ManifestChangeType::CompactedUpToSegment(segment) => Some(*segment),
                _ => None,
            })
            .collect()
    }

    // Load Vec<Manifest> from a dir
    #[allow(unused)]
    pub fn load_from_dir<V: FileSystem>(path: &Path, vfs: &V) -> Result<Self> {
        let mut manifests = Manifest::new();
        if !path.exists() {
            return Ok(manifests);
        }

        let sr = SegmentRef::read_segments_from_directory(path, vfs)?;
        let reader = MultiSegmentReader::new(sr)?;
        let mut reader = Reader::new_from(reader);

        loop {
            // Read the next transaction record from the log.
            let mut len_buf = [0; 4];
            let res = reader.read(&mut len_buf); // Read 4 bytes for the length
            if let Err(e) = res {
                if let Error::LogError(LogError::Eof) = e {
                    break;
                } else {
                    return Err(e);
                }
            }

            let len = u32::from_be_bytes(len_buf) as usize; // Convert bytes to length
            let mut md_bytes = vec![0u8; len];
            reader.read(&mut md_bytes)?; // Read the actual metadata

            let manifest = Manifest::deserialize_revisioned(&mut md_bytes.as_slice())?;
            manifests.changes.extend(manifest.changes);
        }

        Ok(manifests)
    }
}

#[cfg(test)]
mod tests {
    use crate::log::Aol;
    use crate::log::Options as LogOptions;
    use crate::store::Core;

    use super::*;
    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[test]
    fn test_manifest_append_and_load() {
        // Create a temporary directory
        let temp_dir = create_temp_directory();
        let opts = LogOptions::default();
        let mut a =
            Aol::open(temp_dir.path(), &opts, &crate::vfs::Dummy).expect("should create aol");

        let manifest = Manifest {
            changes: vec![ManifestChangeType::Options(Options::default())],
        };

        // Append the manifest to the file
        let buf = manifest.serialize().unwrap();
        a.append(&buf).expect("should append record");
        a.close().expect("should close aol");

        // Load the manifests from the file
        let loaded_manifest = Manifest::load_from_dir(temp_dir.path(), &crate::vfs::Dummy).unwrap();

        // Assert that the loaded manifests contain exactly one manifest
        assert_eq!(loaded_manifest.changes.len(), 1);
    }

    #[test]
    fn test_add_and_read_multiple_manifests() {
        // Step 1: Create a temporary directory
        let temp_dir = create_temp_directory();
        let log_opts = LogOptions::default();
        let mut a =
            Aol::open(temp_dir.path(), &log_opts, &crate::vfs::Dummy).expect("should create aol");

        // Step 2: Create the first Manifest instance and append it to the file
        let first_manifest = Manifest {
            changes: vec![ManifestChangeType::Options(Options::default())],
        };

        // Append the manifest to the file
        let buf = first_manifest.serialize().unwrap();
        a.append(&buf).expect("should append record");

        // Step 4: Create a new Manifest instance with changes and append it to the same file
        let mut opt = Options::new();
        opt.max_value_threshold = 1;

        let second_manifest = Manifest {
            changes: vec![ManifestChangeType::Options(opt)],
        };

        let buf = second_manifest.serialize().unwrap();

        a.append(&buf).expect("should append record");

        a.close().expect("should close aol");

        // Step 5: Load the manifests from the file
        let loaded_manifest = Manifest::load_from_dir(temp_dir.path(), &crate::vfs::Dummy).unwrap();

        // Step 6: Assert that the loaded manifests contain exactly two manifests
        assert_eq!(loaded_manifest.changes.len(), 2);
        let updated_change = loaded_manifest.changes[1].clone();

        match updated_change {
            ManifestChangeType::Options(options) => {
                assert_eq!(options.max_value_threshold, 1);
            }
            _ => {
                unreachable!("option change is not of type Update");
            }
        }
    }

    #[test]
    // Test if we can load a manifest with a different revision
    fn test_backward_compatibility() {
        // Test with manifest without any changes
        let dir_name = "tests/data/manifest_revision1";
        let manifest_path = Path::new(dir_name);

        // Load the manifest from the specified path
        let mf = Core::read_manifest(manifest_path, &crate::vfs::Dummy).unwrap();
        assert_eq!(mf.changes.len(), 1);

        // Test with manifest with changes to max_value_size
        let dir_name = "tests/data/manifest_revision1_with_updates";
        let manifest_path = Path::new(dir_name);

        // Load the manifest from the specified path
        let mf = Core::read_manifest(manifest_path, &crate::vfs::Dummy).unwrap();
        assert_eq!(mf.changes.len(), 2);
    }
}
