use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use bytes::Bytes;

pub fn default_hash(h: &impl Hash) -> u64 {
    let mut hasher = DefaultHasher::new();
    h.hash(&mut hasher);
    hasher.finish()
}

#[derive(Clone)]
pub(crate) struct NoopValue {
    data: Bytes,
}

impl AsRef<Bytes> for NoopValue {
    fn as_ref(&self) -> &Bytes {
        &self.data
    }
}

impl From<Bytes> for NoopValue {
    fn from(bytes: Bytes) -> Self {
        NoopValue { data: bytes }
    }
}
