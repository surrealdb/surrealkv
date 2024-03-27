use std::path::PathBuf;

use bytes::Bytes;
use chrono::Utc;
use crc32fast::Hasher as crc32Hasher;
use sha2::{Digest, Sha256};

/// Calculates the CRC32 hash of a byte array.
/// It creates a new CRC32 hasher, updates it with the byte array, and finalizes the hash.
/// It returns the hash as a 32-bit unsigned integer.
pub(crate) fn calculate_crc32(buf: &[u8]) -> u32 {
    let mut hasher = crc32Hasher::new();
    hasher.update(buf);
    hasher.finalize()
}

pub(crate) fn calculate_crc32_combined(buf1: &[u8], buf2: &[u8]) -> u32 {
    let mut hasher = crc32Hasher::new();
    hasher.update(buf1);
    hasher.update(buf2);
    hasher.finalize()
}

/// Gets the current time in nanoseconds since the Unix epoch.
/// It gets the current time in UTC, extracts the timestamp in nanoseconds, and asserts that the timestamp is positive.
/// It returns the timestamp as a 64-bit unsigned integer.
pub(crate) fn now() -> u64 {
    let utc_now = Utc::now();
    let timestamp = utc_now.timestamp_nanos_opt().unwrap();
    assert!(timestamp > 0);
    timestamp as u64
}

pub(crate) fn sha256(arg: Bytes) -> Bytes {
    let mut hasher = Sha256::new();
    hasher.update(arg);
    let result = hasher.finalize();
    Bytes::copy_from_slice(result.as_slice())
}

pub(crate) fn sanitize_directory(directory: &str) -> std::io::Result<PathBuf> {
    // Convert the directory string to a PathBuf
    let mut path = PathBuf::from(directory);

    // Normalize the path (resolve '..' and '.' components)
    path = path.canonicalize()?;

    // Check if the path is absolute
    if !path.is_absolute() {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Directory path must be absolute"));
    }

    // Check if the path contains any '..' components after normalization
    if path.components().any(|component| component.as_os_str() == "..") {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Directory path must not contain '..' components"));
    }

    Ok(path)
}
