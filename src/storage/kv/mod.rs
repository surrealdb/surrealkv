pub mod entry;
pub mod error;
pub mod meta;
pub mod option;
pub mod oracle;
pub mod reader;
pub mod snapshot;
pub mod store;
pub mod transaction;
pub mod util;

use crc32fast::Hasher;

const NULL_BYTE: [u8; 1] = [0];


fn calculate_crc32(a1: &[u8], a2: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(a1);
    hasher.update(a2);
    hasher.finalize()
}

fn terminate_with_null(key: &[u8]) -> Vec<u8> {
    if !key.ends_with(&NULL_BYTE) {
        let mut terminated_key = Vec::with_capacity(key.len() + 1);
        terminated_key.extend_from_slice(key);
        terminated_key.push(0);
        terminated_key
    } else {
        key.to_vec()
    }
}

// use crate::storage::kv::error::Result;
// use crate::storage::index::snapshot::Snapshot;
// use crate::storage::index::KeyTrait;

// /// A key/value store.
// pub trait Store: Send + Sync {
//     /// Deletes a key, or does nothing if it does not exist.
//     fn delete(&mut self, key: &[u8]) -> Result<()>;

//     /// Gets a value for a key, if it exists.
//     fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

//     /// Sets a value for a key, replacing the existing value if any.
//     fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

//     /// Takes a snapshot of the store.
//     fn snapshot<P: KeyTrait, V: Clone>(&mut self) -> Result<Snapshot<P, V>>;
// }
