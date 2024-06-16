use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::PathBuf;

use crate::storage::kv::util::copy_dir_all;
use crate::storage::log::SegmentRef;
use crate::{Error, Options, Result};

use super::store::Core;

#[derive(Debug, PartialEq)]
pub(crate) enum RecoveryState {
    None,
    ClogBackedUp,
    ClogDeleted,
}

impl RecoveryState {
    pub(crate) fn load() -> Result<Self> {
        let path = PathBuf::from(".recovery_state");
        if path.exists() {
            let mut file = File::open(path)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            match contents.as_str() {
                "ClogBackedUp" => Ok(RecoveryState::ClogBackedUp),
                "ClogDeleted" => Ok(RecoveryState::ClogDeleted),
                _ => Ok(RecoveryState::None),
            }
        } else {
            Ok(RecoveryState::None)
        }
    }

    pub(crate) fn save(&self) -> Result<()> {
        let path = PathBuf::from(".recovery_state");
        let mut file = File::create(path)?;
        match self {
            RecoveryState::ClogBackedUp => {
                write!(file, "ClogBackedUp")
                    .map_err(|e| Error::CustomError(format!("recovery file write error: {}", e)))?;
                Ok(())
            }
            RecoveryState::ClogDeleted => {
                write!(file, "ClogDeleted")
                    .map_err(|e| Error::CustomError(format!("recovery file write error: {}", e)))?;
                Ok(())
            }
            RecoveryState::None => Ok(()),
        }
    }

    pub(crate) fn clear() -> Result<()> {
        let path = PathBuf::from(".recovery_state");
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }
}

fn perform_recovery(opts: &Options) -> Result<()> {
    // Encapsulate operations in a closure for easier rollback
    let result = || -> Result<()> {
        let merge_dir = opts.dir.join(".merge");
        let clog_dir = opts.dir.join("clog");
        let merge_clog_subdir = merge_dir.join("clog");

        // Original operations with modifications for transactional integrity
        // For example, deleting the .tmp.merge directory is now safe to skip as it's already backed up

        // If there is a .merge directory, try reading manifest from it
        let manifest = Core::initialize_manifest(&merge_dir)?;
        let existing_manifest = if manifest.size()? > 0 {
            Core::read_manifest(&merge_dir)?
        } else {
            return Err(Error::MergeManifestMissing);
        };

        let compacted_upto_segments = existing_manifest.extract_compacted_up_to_segments();
        if compacted_upto_segments.len() == 0 || compacted_upto_segments.len() > 1 {
            return Err(Error::MergeManifestMissing);
        }

        let compacted_upto_segment_id = compacted_upto_segments[0];
        let segs = SegmentRef::read_segments_from_directory(&clog_dir)?;
        // Step 4: Copy files from clog dir to merge clog dir and then copy it back to clog dir to avoid
        // creating a backup directory
        let merge_clog_subdir = merge_dir.join("clog");
        for seg in segs.iter() {
            if seg.id > compacted_upto_segment_id {
                // Check if the path points to a regular file
                match fs::metadata(&seg.file_path) {
                    Ok(metadata) => {
                        if metadata.is_file() {
                            // Proceed to copy the file
                            let dest_path =
                                merge_clog_subdir.join(seg.file_path.file_name().unwrap());
                            match fs::copy(&seg.file_path, &dest_path) {
                                Ok(_) => println!("File copied successfully"),
                                Err(e) => {
                                    println!("Error copying file: {:?}", e);
                                    return Err(Error::from(e));
                                }
                            }
                        } else {
                            println!("Path is not a regular file: {:?}", &seg.file_path);
                        }
                    }
                    Err(e) => {
                        println!("Error accessing file metadata: {:?}", e);
                        return Err(Error::from(e));
                    }
                }
            }
        }

        // Copy files from the `.merge/clog` directory into the `clog` directory

        // Clear any previous recovery state before setting a new one
        RecoveryState::clear()?;
        // After successful operation, update recovery state to indicate clog can be deleted
        RecoveryState::ClogDeleted.save()?;

        // Delete the `clog` directory
        if let Err(e) = fs::remove_dir_all(&clog_dir) {
            println!("Error deleting clog directory: {:?}", e);
            return Err(Error::from(e));
        }

        // Rename `merge_clog_subdir` to `clog`
        if let Err(e) = fs::rename(&merge_clog_subdir, &clog_dir) {
            println!("Error renaming merge_clog_subdir to clog: {:?}", e);
            return Err(Error::from(e));
        }

        // Clear recovery state after successful completion
        RecoveryState::clear()?;

        Ok(())
    };

    match result() {
        Ok(_) => Ok(()),
        Err(e) => {
            let backup_dir = opts.dir.join(".backup");
            let clog_dir = opts.dir.join("clog");
            rollback(&backup_dir, &clog_dir, RecoveryState::load()?)?;
            Err(e)
        }
    }
}

fn cleanup_after_recovery(opts: &Options) -> Result<()> {
    let backup_dir = opts.dir.join(".backup");
    let merge_dir = opts.dir.join(".merge");

    if backup_dir.exists() {
        fs::remove_dir_all(&backup_dir)?;
    }
    if merge_dir.exists() {
        fs::remove_dir_all(&merge_dir)?;
    }
    Ok(())
}

fn rollback(backup_dir: &PathBuf, clog_dir: &PathBuf, checkpoint: RecoveryState) -> Result<()> {
    match checkpoint {
        RecoveryState::ClogBackedUp => {
            // Restore the clog directory from backup
            let backup_clog_dir = backup_dir.join("clog");
            if backup_clog_dir.exists() && clog_dir.exists() {
                fs::remove_dir_all(clog_dir)?;
            }
            if backup_clog_dir.exists() {
                fs::rename(&backup_clog_dir, clog_dir)?;
            }
        }
        RecoveryState::ClogDeleted => {
            // Restore the clog directory from backup
            let backup_clog_dir = backup_dir.join("clog");
            if backup_clog_dir.exists() {
                fs::rename(&backup_clog_dir, clog_dir)?;
            }
        }
        _ => (),
    }

    // Clean up backup directory after rollback
    if backup_dir.exists() {
        fs::remove_dir_all(backup_dir)?;
    }

    Ok(())
}

fn needs_recovery(opts: &Options) -> Result<bool> {
    Ok(opts.dir.join(".merge").exists())
}

fn handle_clog_backed_up_state(opts: &Options) -> Result<()> {
    let backup_dir = opts.dir.join(".backup");
    let clog_dir = opts.dir.join("clog");
    rollback(&backup_dir, &clog_dir, RecoveryState::ClogBackedUp)
}

fn handle_clog_deleted_state(opts: &Options) -> Result<()> {
    let backup_dir = opts.dir.join(".backup");
    let clog_dir = opts.dir.join("clog");
    rollback(&backup_dir, &clog_dir, RecoveryState::ClogDeleted)
}

fn backup_and_prepare_for_recovery(opts: &Options) -> Result<()> {
    let backup_dir = opts.dir.join(".backup");
    let clog_dir = opts.dir.join("clog");
    let merge_dir = opts.dir.join(".merge");

    if merge_dir.exists() {
        fs::create_dir_all(&backup_dir)?;
        copy_dir_all(&clog_dir, &backup_dir.join("clog"))?;
        RecoveryState::ClogBackedUp.save()?;
    }
    Ok(())
}

/// Restores the store from a compaction process by handling .tmp.merge and .merge directories.
/// TODO: This should happen post repair
pub fn restore_from_compaction(opts: &Options) -> Result<()> {
    let tmp_merge_dir = opts.dir.join(".tmp.merge");
    // 1) Check if there is a .tmp.merge directory, delete it
    if tmp_merge_dir.exists() {
        fs::remove_dir_all(&tmp_merge_dir).map_err(Error::from)?;
        return Ok(());
    }

    if !needs_recovery(opts)? {
        return Ok(());
    }

    match RecoveryState::load()? {
        RecoveryState::ClogBackedUp => handle_clog_backed_up_state(opts)?,
        RecoveryState::ClogDeleted => handle_clog_deleted_state(opts)?,
        RecoveryState::None => backup_and_prepare_for_recovery(opts)?,
    }

    perform_recovery(opts)?;
    // Clean up merge directory after successful operation
    cleanup_after_recovery(opts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::read_to_string;

    #[test]
    fn test_recovery_state_sequentially() {
        // Setup - Ensure clean state before tests
        let path = PathBuf::from(".recovery_state");
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }

        // Test saving and loading ClogBackedUp
        RecoveryState::ClogBackedUp.save().unwrap();
        assert_eq!(RecoveryState::load().unwrap(), RecoveryState::ClogBackedUp);

        // Clear state and re-setup for next test
        RecoveryState::clear().unwrap();
        assert!(!path.exists());

        // Test saving and loading ClogDeleted
        RecoveryState::ClogDeleted.save().unwrap();
        assert_eq!(RecoveryState::load().unwrap(), RecoveryState::ClogDeleted);

        // Clear state and re-setup for next test
        RecoveryState::clear().unwrap();
        assert!(!path.exists());

        // Test loading None when no state is saved
        assert_eq!(RecoveryState::load().unwrap(), RecoveryState::None);

        // Test save contents for ClogBackedUp
        RecoveryState::ClogBackedUp.save().unwrap();
        let contents = read_to_string(&path).unwrap();
        assert_eq!(contents, "ClogBackedUp");

        // Clear state and re-setup for next test
        RecoveryState::clear().unwrap();
        assert!(!path.exists());

        // Test save contents for ClogDeleted
        RecoveryState::ClogDeleted.save().unwrap();
        let contents = read_to_string(&path).unwrap();
        assert_eq!(contents, "ClogDeleted");

        // Final clear to clean up
        RecoveryState::clear().unwrap();
        assert!(!path.exists());
    }
}
