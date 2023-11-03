use chrono::Utc;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use crc32fast::Hasher as crc32Hasher;

pub fn default_hash(h: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    h.hash(&mut hasher);
    hasher.finish()
}

pub(crate) fn calculate_crc32(a1: &[u8], a2: &[u8]) -> u32 {
    let mut hasher = crc32Hasher::new();
    hasher.update(a1);
    hasher.update(a2);
    hasher.finalize()
}

pub(crate) fn now() -> u64 {
    let utc_now = Utc::now();
    let timestamp = utc_now.timestamp_nanos_opt().unwrap();
    assert!(timestamp > 0);
    timestamp as u64
}
