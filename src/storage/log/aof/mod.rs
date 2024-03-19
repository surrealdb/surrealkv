pub mod log;
pub mod reader;

use crate::storage::kv::error::Result;

pub trait LogReader {
    fn read(&mut self) -> Result<(&[u8], u64)>;
}