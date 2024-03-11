use std::{fmt, io, sync::Arc};

use crate::storage::kv::store::Task;
use crate::storage::log::Error as LogError;
use vart::TrieError;

/// Result returning Error
pub type Result<T> = std::result::Result<T, Error>;

/// `Error` is a custom error type for the storage module.
/// It includes various variants to represent different types of errors that can occur.
#[derive(Clone, Debug)]
pub enum Error {
    Abort,                              // The operation was aborted
    IoError(Arc<io::Error>),            // An I/O error occurred
    LogError(LogError),                 // An error occurred in the log
    EmptyKey,                           // The key is empty
    TransactionClosed,                  // The transaction was closed
    NonExpirable,                       // The entry cannot be expired
    CorruptedMetadata,                  // The metadata is corrupted
    TransactionReadOnly,                // The transaction is read-only
    IndexError(TrieError),              // An error occurred in the index
    MaxKeyLengthExceeded,               // The maximum key length was exceeded
    MaxValueLengthExceeded,             // The maximum value length was exceeded
    KeyNotFound,                        // The key was not found
    CorruptedIndex,                     // The index is corrupted
    TransactionReadConflict,            // A read conflict occurred in the transaction
    StoreClosed,                        // The store was closed
    InvalidAttributeData,               // The attribute data is invalid
    UnknownAttributeType,               // The attribute type is unknown
    CorruptedTransactionRecord,         // The transaction record is corrupted
    CorruptedTransactionHeader,         // The transaction header is corrupted
    InvalidTransactionRecordId,         // The transaction record ID is invalid
    EmptyValue,                         // The value in the record is empty
    ManifestNotFound,                   // The manifest was not found
    MaxTransactionEntriesLimitExceeded, // The maximum number of entries in a transaction was exceeded
    TransactionWriteOnly,               // The transaction is write-only
    SendError(String),
    ReceiveError(String),
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
            Error::IoError(err) => write!(f, "IO error: {}", err),
            Error::EmptyKey => write!(f, "Empty key"),
            Error::TransactionClosed => {
                write!(f, "This transaction has been closed")
            }
            Error::NonExpirable => write!(f, "This entry cannot be expired"),
            Error::CorruptedMetadata => write!(f, "Corrupted metadata"),
            Error::TransactionReadOnly => write!(f, "This transaction is read-only"),
            Error::IndexError(trie_error) => write!(f, "Index error: {}", trie_error),
            Error::MaxKeyLengthExceeded => write!(f, "Max Key length exceeded"),
            Error::MaxValueLengthExceeded => write!(f, "Max Value length exceeded"),
            Error::KeyNotFound => write!(f, "Key not found"),
            Error::CorruptedIndex => write!(f, "Corrupted index"),
            Error::TransactionReadConflict => write!(f, "Transaction read conflict"),
            Error::StoreClosed => write!(f, "Store closed"),
            Error::InvalidAttributeData => write!(f, "Invalid attribute data"),
            Error::UnknownAttributeType => write!(f, "Unknown attribute type"),
            Error::LogError(log_error) => write!(f, "Log error: {}", log_error),
            Error::CorruptedTransactionRecord => write!(f, "Corrupted transaction record"),
            Error::CorruptedTransactionHeader => write!(f, "Corrupted transaction header"),
            Error::InvalidTransactionRecordId => write!(f, "Invalid transaction record ID"),
            Error::EmptyValue => write!(f, "Empty value in the record"),
            Error::ManifestNotFound => write!(f, "Manifest not found"),
            Error::MaxTransactionEntriesLimitExceeded => {
                write!(f, "Max transaction entries limit exceeded")
            }
            Error::TransactionWriteOnly => write!(f, "Transaction is write-only"),
            Error::SendError(err) => write!(f, "Send error: {}", err),
            Error::ReceiveError(err) => write!(f, "Receive error: {}", err),
        }
    }
}

// Implementation of Error trait for Error
impl std::error::Error for Error {}

// Implementation to convert io::Error into Error
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::IoError(Arc::new(e))
    }
}

impl From<TrieError> for Error {
    fn from(trie_error: TrieError) -> Self {
        Error::IndexError(trie_error)
    }
}

impl From<LogError> for Error {
    fn from(log_error: LogError) -> Self {
        Error::LogError(log_error)
    }
}

impl From<async_channel::SendError<Task>> for Error {
    fn from(error: async_channel::SendError<Task>) -> Self {
        Error::SendError(format!("Async channel send error: {}", error))
    }
}

impl From<async_channel::SendError<std::result::Result<(), Error>>> for Error {
    fn from(error: async_channel::SendError<std::result::Result<(), Error>>) -> Self {
        Error::SendError(format!("Async channel send error: {}", error))
    }
}

impl From<async_channel::RecvError> for Error {
    fn from(error: async_channel::RecvError) -> Self {
        Error::ReceiveError(format!("Async channel receive error: {}", error))
    }
}
