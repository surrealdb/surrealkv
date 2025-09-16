use std::sync::Arc;
use std::{fmt, io};

/// Result returning Error
pub type Result<T> = std::result::Result<T, Error>;

/// `Error` is a custom error type for the storage module.
/// It includes various variants to represent different types of errors that can
/// occur.
#[derive(Clone, Debug)]
pub enum Error {
	Abort,              // The operation was aborted
	Io(Arc<io::Error>), // An I/O error occurred
	Send(String),
	Receive(String),
	CorruptedBlock(String),
	Compression(String),
	KeyNotInOrder,
	FilterBlockEmpty,
	Decompression(String),
	InvalidFilename(String),
	CorruptedTableMetadata(String),
	InvalidTableFormat,
	TableMetadataNotFound,
	Wal(String),
	BlockNotFound,
	BatchTooLarge,
	InvalidBatchRecord,
	TransactionWriteConflict,
	TransactionClosed,
	EmptyKey,
	TransactionWriteOnly,
	TransactionReadOnly,
	TransactionWithoutSavepoint,
	KeyNotFound,
	WriteStall,
	FileDescriptorNotFound,
	TableIDCollision(u64),
	PipelineStall,
	Other(String), // Other errors
	NoSnapshot,
	CommitFail(String),
	LoadManifestFail(String),
	Corruption(String), // Data corruption detected
	VlogGCAlreadyInProgress,
	InvalidArgument(String),
	InvalidTag(String),
}

// Implementation of Display trait for Error
impl fmt::Display for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
            Self::Abort => write!(f, "Operation aborted"),
            Self::Io(err) => write!(f, "IO error: {err}"),
            Self::Send(err) => write!(f, "Send error: {err}"),
            Self::Receive(err) => write!(f, "Receive error: {err}"),
            Self::CorruptedBlock(err) => write!(f, "Corrupted block: {err}"),
            Self::Compression(err) => write!(f, "Compression error: {err}"),
            Self::KeyNotInOrder => write!(f, "Keys are not in order"),
            Self::FilterBlockEmpty => write!(f, "Filter block is empty"),
            Self::Decompression(err) => write!(f, "Decompression error: {err}"),
            Self::InvalidFilename(err) => write!(f, "Invalid filename: {err}"),
            Self::CorruptedTableMetadata(err) => write!(f, "Corrupted table metadata: {err}"),
            Self::InvalidTableFormat => write!(f, "Invalid table format"),
            Self::TableMetadataNotFound => write!(f, "Table metadata not found"),
            Self::Wal(err) => write!(f, "WAL error: {err}"),
            Self::BlockNotFound => write!(f, "Block not found"),
            Self::BatchTooLarge => write!(f, "Batch too large"),
            Self::InvalidBatchRecord => write!(f, "Invalid batch record"),
            Self::TransactionWriteConflict => write!(f, "Transaction write conflict"),
            Self::TransactionClosed => write!(f, "Transaction closed"),
            Self::EmptyKey => write!(f, "Empty key"),
            Self::TransactionWriteOnly => write!(f, "Transaction is write-only"),
            Self::TransactionReadOnly => write!(f, "Transaction is read-only"),
            Self::TransactionWithoutSavepoint => write!(f, "Transaction has no savepoint to rollback to"),
            Self::KeyNotFound => write!(f, "Key not found"),
            Self::WriteStall => write!(f, "Write stall"),
            Self::FileDescriptorNotFound => write!(f, "File descriptor not found"),
            Self::TableIDCollision(id) => write!(f, "CRITICAL ERROR: Table ID collision detected. New table ID {id} conflicts with a table ID in the merge list."),
            Self::PipelineStall => write!(f, "Pipeline stall"),
            Self::Other(err) => write!(f, "Other error: {err}"),
            Self::NoSnapshot => write!(f, "No snapshot available"),
            Self::CommitFail(err) => write!(f, "Commit failed: {err}"),
            Self::LoadManifestFail(err) => write!(f, "Failed to load manifest: {err}"),
            Self::Corruption(err) => write!(f, "Data corruption detected: {err}"),
            Self::VlogGCAlreadyInProgress => write!(f, "Vlog garbage collection already in progress"),
            Self::InvalidArgument(err) => write!(f, "Invalid argument: {err}"),
            Self::InvalidTag(err) => write!(f, "Invalid tag: {err}"),

        }
	}
}

// Implementation of Error trait for Error
impl std::error::Error for Error {}

// Implementation to convert io::Error into Error
impl From<io::Error> for Error {
	fn from(e: io::Error) -> Error {
		Error::Io(Arc::new(e))
	}
}

impl From<crate::wal::Error> for Error {
	fn from(err: crate::wal::Error) -> Self {
		Error::Wal(err.to_string())
	}
}

impl From<async_channel::SendError<std::result::Result<(), Error>>> for Error {
	fn from(error: async_channel::SendError<std::result::Result<(), Error>>) -> Self {
		Error::Send(format!("Async channel send error: {error}"))
	}
}

impl From<async_channel::RecvError> for Error {
	fn from(error: async_channel::RecvError) -> Self {
		Error::Receive(format!("Async channel receive error: {error}"))
	}
}
