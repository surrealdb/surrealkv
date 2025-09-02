use std::{fmt, io, sync::Arc};

/// Result returning Error
pub type Result<T> = std::result::Result<T, Error>;

/// `Error` is a custom error type for the storage module.
/// It includes various variants to represent different types of errors that can occur.
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
            Error::Abort => write!(f, "Operation aborted"),
            Error::Io(err) => write!(f, "IO error: {err}"),
            Error::Send(err) => write!(f, "Send error: {err}"),
            Error::Receive(err) => write!(f, "Receive error: {err}"),
            Error::CorruptedBlock(err) => write!(f, "Corrupted block: {err}"),
            Error::Compression(err) => write!(f, "Compression error: {err}"),
            Error::KeyNotInOrder => write!(f, "Keys are not in order"),
            Error::FilterBlockEmpty => write!(f, "Filter block is empty"),
            Error::Decompression(err) => write!(f, "Decompression error: {err}"),
            Error::InvalidFilename(err) => write!(f, "Invalid filename: {err}"),
            Error::CorruptedTableMetadata(err) => write!(f, "Corrupted table metadata: {err}"),
            Error::InvalidTableFormat => write!(f, "Invalid table format"),
            Error::TableMetadataNotFound => write!(f, "Table metadata not found"),
            Error::Wal(err) => write!(f, "WAL error: {err}"),
            Error::BlockNotFound => write!(f, "Block not found"),
            Error::BatchTooLarge => write!(f, "Batch too large"),
            Error::InvalidBatchRecord => write!(f, "Invalid batch record"),
            Error::TransactionWriteConflict => write!(f, "Transaction write conflict"),
            Error::TransactionClosed => write!(f, "Transaction closed"),
            Error::EmptyKey => write!(f, "Empty key"),
            Error::TransactionWriteOnly => write!(f, "Transaction is write-only"),
            Error::TransactionReadOnly => write!(f, "Transaction is read-only"),
            Error::KeyNotFound => write!(f, "Key not found"),
            Error::WriteStall => write!(f, "Write stall"),
            Error::FileDescriptorNotFound => write!(f, "File descriptor not found"),
            Error::TableIDCollision(id) => write!(f, "CRITICAL ERROR: Table ID collision detected. New table ID {id} conflicts with a table ID in the merge list."),
            Error::PipelineStall => write!(f, "Pipeline stall"),
            Error::Other(err) => write!(f, "Other error: {err}"),
            Error::NoSnapshot => write!(f, "No snapshot available"),
            Error::CommitFail(err) => write!(f, "Commit failed: {err}"),
            Error::LoadManifestFail(err) => write!(f, "Failed to load manifest: {err}"),
            Error::Corruption(err) => write!(f, "Data corruption detected: {err}"),
            Error::VlogGCAlreadyInProgress => write!(f, "Vlog garbage collection already in progress"),
            Error::InvalidArgument(err) => write!(f, "Invalid argument: {err}"),
            Error::InvalidTag(err) => write!(f, "Invalid tag: {err}"),
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
