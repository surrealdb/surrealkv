//! Pure-Rust compatibility reader and migration utilities for SurrealKV V1 files.
//!
//! Enables SurrealKV V2 and SurrealDB to start up directly from existing SurrealKV V1
//! databases without manual dump/restore migration steps.

pub mod block;
pub mod database;
pub mod error;
pub mod footer;
pub mod key;
pub mod table;
pub mod varint;

#[cfg(test)]
mod tests;

pub use database::{is_v1_dir, read_all_latest};
pub use error::{Error, Result};
pub use footer::{Footer, V1_FOOTER_TOTAL_LEN, V1_MAGIC_FOOTER};
pub use key::{KeyKind, V1ParsedKey};
pub use table::{V1Record, V1TableReader};
