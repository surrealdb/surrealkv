use std::{fmt, io, sync::Arc};
use vart::TrieError;

use crate::log::Error as LogError;

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
    KeyNotFound,                        // The key was not found
    CorruptedIndex,                     // The index is corrupted
    TransactionReadConflict,            // A read conflict occurred in the transaction
    TransactionWriteConflict,           // A write conflict occurred in the transaction
    StoreClosed,                        // The store was closed
    InvalidAttributeData,               // The attribute data is invalid
    UnknownAttributeType,               // The attribute type is unknown
    CorruptedTransactionRecord(String), // The transaction record is corrupted
    CorruptedTransactionHeader(String), // The transaction header is corrupted
    InvalidTransactionRecordId,         // The transaction record ID is invalid
    EmptyValue,                         // The value in the record is empty
    ManifestNotFound,                   // The manifest was not found
    TransactionWriteOnly,               // The transaction is write-only
    SendError(String),
    ReceiveError(String),
    RevisionError(String),
    MismatchedSegmentID(u64, u64),
    CompactionAlreadyInProgress,    // Compaction is in progress
    MergeManifestMissing,           // The merge manifest is missing
    CustomError(String),            // Custom error
    InvalidOperation,               // Invalid operation
    CompactionSegmentSizeTooSmall,  // The segment size is too small for compaction
    SegmentIdExceedsLastUpdated,    // The segment ID exceeds the last updated segment
    TransactionMustBeReadOnly,      // The transaction must be read-only
    TransactionWithoutSavepoint,    // The transaction does not have a savepoint set
    MaxKVMetadataLengthExceeded,    // The maximum KV metadata length is exceeded
    ChecksumMismatch(u32, u32),     // Checksum mismatch
    SnapshotVersionIsOld(u64, u64), // The snapshot version is too old
}

// Implementation of Display trait for Error
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Abort => write!(f, "Operation aborted"),
            Error::IoError(err) => write!(f, "IO error: {err}"),
            Error::EmptyKey => write!(f, "Empty key"),
            Error::TransactionClosed => {
                write!(f, "This transaction has been closed")
            }
            Error::NonExpirable => write!(f, "This entry cannot be expired"),
            Error::CorruptedMetadata => write!(f, "Corrupted metadata"),
            Error::TransactionReadOnly => write!(f, "This transaction is read-only"),
            Error::IndexError(trie_error) => write!(f, "Index error: {trie_error}"),
            Error::KeyNotFound => write!(f, "Key not found"),
            Error::CorruptedIndex => write!(f, "Corrupted index"),
            Error::TransactionReadConflict => write!(f, "Transaction read conflict"),
            Error::TransactionWriteConflict => write!(f, "Transaction write conflict"),
            Error::StoreClosed => write!(f, "Store closed"),
            Error::InvalidAttributeData => write!(f, "Invalid attribute data"),
            Error::UnknownAttributeType => write!(f, "Unknown attribute type"),
            Error::LogError(log_error) => write!(f, "Log error: {log_error}"),
            Error::CorruptedTransactionRecord(msg) => {
                write!(f, "Corrupted transaction record: {msg}")
            }
            Error::CorruptedTransactionHeader(msg) => {
                write!(f, "Corrupted transaction header: {msg}")
            }
            Error::InvalidTransactionRecordId => write!(f, "Invalid transaction record ID"),
            Error::EmptyValue => write!(f, "Empty value in the record"),
            Error::ManifestNotFound => write!(f, "Manifest not found"),
            Error::TransactionWriteOnly => write!(f, "Transaction is write-only"),
            Error::SendError(err) => write!(f, "Send error: {err}"),
            Error::ReceiveError(err) => write!(f, "Receive error: {err}"),
            Error::MismatchedSegmentID(expected, found) => write!(
                f,
                "Mismatched segment ID: expected={expected}, found={found}"
            ),
            Error::CompactionAlreadyInProgress => write!(f, "Compaction is in progress"),
            Error::RevisionError(err) => write!(f, "Revision error: {err}"),
            Error::MergeManifestMissing => write!(f, "Merge manifest is missing"),
            Error::CustomError(err) => write!(f, "Error: {err}"),
            Error::InvalidOperation => write!(f, "Invalid operation"),
            Error::CompactionSegmentSizeTooSmall => {
                write!(
                    f,
                    "Segment size is too small for compaction, must be >= max_segment_size"
                )
            }
            Error::SegmentIdExceedsLastUpdated => {
                write!(f, "Segment ID exceeds the last updated segment")
            }
            Error::TransactionMustBeReadOnly => {
                write!(f, "Transaction must be read-only")
            }
            Error::TransactionWithoutSavepoint => {
                write!(f, "Transaction does not have a savepoint set")
            }
            Error::MaxKVMetadataLengthExceeded => {
                write!(f, "Maximum KV metadata length exceeded")
            }
            Error::ChecksumMismatch(expected, found) => {
                write!(f, "Checksum mismatch: expected={expected}, found={found}")
            }
            Error::SnapshotVersionIsOld(expected, found) => {
                write!(
                    f,
                    "Snapshot version is old: read_ts={expected}, index={found}"
                )
            }
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

impl From<revision::Error> for Error {
    fn from(err: revision::Error) -> Self {
        Error::RevisionError(err.to_string())
    }
}
