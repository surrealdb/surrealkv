use std::{
    fmt, io,
    sync::{Arc, PoisonError},
};

use crate::storage::index::art::TrieError;
use crate::storage::log::Error as LogError;

/// Result returning Error
pub type Result<T> = std::result::Result<T, Error>;

/// Custom error type for the storage module
#[derive(Debug)]
pub enum Error {
    Abort,
    IO(Arc<io::Error>),
    Log(LogError),
    EmptyKey,
    PoisonError(String),
    TxnClosed,
    NonExpirable,
    CorruptedMetadata,
    TxnReadOnly,
    IndexError(TrieError), // New variant to hold TrieError
    MaxKeyLengthExceeded,
    MaxValueLengthExceeded,
    KeyNotFound,
    CorruptedIndex,
    TxnReadConflict,
    StoreClosed,
    InvalidAttributeData,
    UnknownAttributeType,
    CorruptedTxRecord,
    CorruptedTxHeader,
    InvalidTxRecordID,
}

/// Error structure for encoding errors
#[derive(Debug)]
pub struct EncodeError {
    message: String,
}

/// Error structure for decoding errors
#[derive(Debug)]
pub struct DecodeError {
    message: String,
}

// Implementation of Display trait for EncodeError
impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "encode error: {}", self.message)
    }
}

// Implementation of Display trait for DecodeError
impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "decode error: {}", self.message)
    }
}

// Implementation of Display trait for Error
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Abort => write!(f, "Operation aborted"),
            Error::IO(err) => write!(f, "IO error: {}", err),
            Error::EmptyKey => write!(f, "Empty key"),
            Error::PoisonError(msg) => write!(f, "Lock Poison: {}", msg),
            Error::TxnClosed => {
                write!(f, "This transaction has been closed")
            }
            Error::NonExpirable => write!(f, "This entry cannot be expired"),
            Error::CorruptedMetadata => write!(f, "Corrupted metadata"),
            Error::TxnReadOnly => write!(f, "This transaction is read-only"),
            Error::IndexError(trie_error) => write!(f, "Index error: {}", trie_error),
            Error::MaxKeyLengthExceeded => write!(f, "Max Key length exceeded"),
            Error::MaxValueLengthExceeded => write!(f, "Max Value length exceeded"),
            Error::KeyNotFound => write!(f, "Key not found"),
            Error::CorruptedIndex => write!(f, "Corrupted index"),
            Error::TxnReadConflict => write!(f, "Transaction read conflict"),
            Error::StoreClosed => write!(f, "Store closed"),
            Error::InvalidAttributeData => write!(f, "Invalid attribute data"),
            Error::UnknownAttributeType => write!(f, "Unknown attribute type"),
            Error::Log(log_error) => write!(f, "Log error: {}", log_error),
            Error::CorruptedTxRecord => write!(f, "Corrupted transaction record"),
            Error::CorruptedTxHeader => write!(f, "Corrupted transaction header"),
            Error::InvalidTxRecordID => write!(f, "Invalid transaction record ID"),
        }
    }
}

// Implementation of Error trait for Error
impl std::error::Error for Error {}

// Implementation to convert io::Error into Error
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::IO(Arc::new(e))
    }
}

// Implementation to convert PoisonError into Error
impl<T: Sized> From<PoisonError<T>> for Error {
    fn from(e: PoisonError<T>) -> Error {
        Error::PoisonError(e.to_string())
    }
}

impl From<TrieError> for Error {
    fn from(trie_error: TrieError) -> Self {
        Error::IndexError(trie_error)
    }
}

impl From<LogError> for Error {
    fn from(log_error: LogError) -> Self {
        Error::Log(log_error)
    }
}
