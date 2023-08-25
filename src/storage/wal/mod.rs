pub mod reader;
pub mod wal;

use std::fmt;
use std::io;

#[derive(Debug, Clone)]
pub(crate) struct CorruptionError {
    kind: io::ErrorKind,
    message: String,
    segment_id: u64,
    offset: usize,
}

impl CorruptionError {
    fn new(kind: io::ErrorKind, message: &str, segment_id: u64, offset: usize) -> Self {
        CorruptionError {
            kind,
            message: message.to_string(),
            segment_id,
            offset,
        }
    }
}

impl fmt::Display for CorruptionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CorruptionError: kind={}, message={}, segment_id={}, offset={}",
            self.kind, self.message, self.segment_id, self.offset
        )
    }
}

impl std::error::Error for CorruptionError {}
