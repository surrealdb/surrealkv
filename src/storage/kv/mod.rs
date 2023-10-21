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

fn calculate_crc32(a1: &[u8], a2: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(a1);
    hasher.update(a2);
    hasher.finalize()
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
