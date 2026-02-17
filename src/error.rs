use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use std::{fmt, io};

use parking_lot::RwLock;

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
	TransactionRetry,
	TransactionClosed,
	EmptyKey,
	TransactionWriteOnly,
	TransactionReadOnly,
	TransactionWithoutSavepoint,
	KeyNotFound,
	WriteStall,
	ArenaFull, // Memtable arena is full, need rotation
	FileDescriptorNotFound,
	TableIDCollision(u64),
	TableNotFound(u64),
	PipelineStall,
	Other(String), // Other errors
	NoSnapshot,
	CommitFail(String),
	LoadManifestFail(String),
	Corruption(String), // Data corruption detected
	InvalidArgument(String),
	InvalidTag(String),
	BPlusTree(String),    // B+ tree specific errors
	InterleavedIteration, // Interleaved iteration not supported
	/// WAL corruption detected during recovery, includes location for repair
	WalCorruption {
		segment_id: usize,
		offset: usize,
		message: String,
	},
	SSTable(crate::sstable::error::SSTableError), // SSTable-specific errors
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
            Self::TransactionRetry => write!(f, "Transaction retry required: memtable history insufficient for conflict detection"),
            Self::TransactionClosed => write!(f, "Transaction closed"),
            Self::EmptyKey => write!(f, "Empty key"),
            Self::TransactionWriteOnly => write!(f, "Transaction is write-only"),
            Self::TransactionReadOnly => write!(f, "Transaction is read-only"),
            Self::TransactionWithoutSavepoint => write!(f, "Transaction has no savepoint to rollback to"),
            Self::KeyNotFound => write!(f, "Key not found"),
            Self::WriteStall => write!(f, "Write stall"),
            Self::ArenaFull => write!(f, "Memtable arena is full"),
            Self::FileDescriptorNotFound => write!(f, "File descriptor not found"),
			Self::TableIDCollision(id) => write!(f, "CRITICAL ERROR: Table ID collision detected. New table ID {id} conflicts with a table ID in the merge list."),
			Self::TableNotFound(id) => write!(f, "Table not found: {id}"),
			Self::PipelineStall => write!(f, "Pipeline stall"),
            Self::Other(err) => write!(f, "Other error: {err}"),
            Self::NoSnapshot => write!(f, "No snapshot available"),
            Self::CommitFail(err) => write!(f, "Commit failed: {err}"),
            Self::LoadManifestFail(err) => write!(f, "Failed to load manifest: {err}"),
            Self::Corruption(err) => write!(f, "Data corruption detected: {err}"),
            Self::InvalidArgument(err) => write!(f, "Invalid argument: {err}"),
            Self::InvalidTag(err) => write!(f, "Invalid tag: {err}"),
            Self::BPlusTree(err) => write!(f, "B+ tree error: {err}"),
            Self::InterleavedIteration => write!(f, "Interleaved iteration not supported: cannot mix next() and next_back() on same iterator"),
            Self::WalCorruption { segment_id, offset, message } => write!(
                f,
                "WAL corruption in segment {} at offset {}: {}",
                segment_id, offset, message
            ),
            Self::SSTable(err) => write!(f, "SSTable error: {err}"),
        }
	}
}

// Implementation of Error trait for Error
impl std::error::Error for Error {}

impl Error {
	/// Creates a WalCorruption error with the given segment ID, offset, and
	/// message
	pub fn wal_corruption(segment_id: usize, offset: usize, message: impl Into<String>) -> Self {
		Self::WalCorruption {
			segment_id,
			offset,
			message: message.into(),
		}
	}
}

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

impl From<crate::bplustree::tree::BPlusTreeError> for Error {
	fn from(err: crate::bplustree::tree::BPlusTreeError) -> Self {
		Error::BPlusTree(err.to_string())
	}
}

impl From<crate::sstable::error::SSTableError> for Error {
	fn from(err: crate::sstable::error::SSTableError) -> Self {
		Error::SSTable(err)
	}
}

impl<T> From<std::sync::PoisonError<T>> for Error {
	fn from(_err: std::sync::PoisonError<T>) -> Self {
		Error::Other("Lock poisoned - another thread panicked while holding the lock".to_string())
	}
}

/// Error severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ErrorSeverity {
	/// Error can be ignored
	#[allow(unused)]
	NoError = 0,
	/// Error is recoverable, background work continues
	#[allow(unused)]
	SoftError = 1,
	/// Error requires stopping writes, may auto-recover
	HardError = 2,
	/// Error is fatal, database must stop
	FatalError = 3,
	/// Unrecoverable error (e.g., data corruption)
	Unrecoverable = 4,
}

/// Reason for background error (for classification)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundErrorReason {
	MemtablaFlush,
	Compaction,
	ManifestWrite,
}

/// Represents a background error with its severity and context
#[derive(Debug, Clone)]
pub struct BackgroundError {
	pub error: Error,
	pub severity: ErrorSeverity,
	pub reason: BackgroundErrorReason,
	pub timestamp: Instant,
}

/// Handler for background errors that propagates errors to user operations
pub struct BackgroundErrorHandler {
	/// Current background error (None = no error)
	bg_error: RwLock<Option<BackgroundError>>,
	/// Fast atomic flag for write path checks
	is_db_stopped: AtomicBool,
	/// Stats tracking
	error_count: AtomicU64,
}

impl BackgroundErrorHandler {
	/// Creates a new background error handler
	pub fn new() -> Self {
		Self {
			bg_error: RwLock::new(None),
			is_db_stopped: AtomicBool::new(false),
			error_count: AtomicU64::new(0),
		}
	}

	/// Classify error severity based on error type and context
	fn classify_error(error: &Error, reason: BackgroundErrorReason) -> ErrorSeverity {
		match (reason, error) {
			// Corruption errors are unrecoverable
			(_, Error::Corruption(_) | Error::CorruptedBlock(_)) => ErrorSeverity::Unrecoverable,

			// Table ID collision is a critical consistency error
			(_, Error::TableIDCollision(_)) => ErrorSeverity::Unrecoverable,

			// Corrupted table metadata is unrecoverable
			(_, Error::CorruptedTableMetadata(_)) => ErrorSeverity::Unrecoverable,

			// I/O errors during memtable flush are fatal
			(BackgroundErrorReason::MemtablaFlush, Error::Io(_)) => ErrorSeverity::FatalError,

			// I/O errors during compaction are fatal
			(BackgroundErrorReason::Compaction, Error::Io(_)) => ErrorSeverity::FatalError,

			// Manifest write I/O is fatal
			(BackgroundErrorReason::ManifestWrite, Error::Io(_)) => ErrorSeverity::FatalError,

			// Default: treat as hard error for safety
			_ => ErrorSeverity::HardError,
		}
	}

	/// Set a background error. This is called by background tasks when they encounter errors.
	pub fn set_error(&self, error: Error, reason: BackgroundErrorReason) {
		let severity = Self::classify_error(&error, reason);
		let bg_error = BackgroundError {
			error: error.clone(),
			severity,
			reason,
			timestamp: Instant::now(),
		};

		// Update the error (only if severity is higher than current)
		let mut current_error = self.bg_error.write();
		if let Some(ref existing) = *current_error {
			// Only update if new error is more severe
			if severity <= existing.severity {
				log::debug!(
					"Background error not updated: new severity {:?} <= existing {:?}, error: {:?}, reason: {:?}",
					severity,
					existing.severity,
					error.to_string(),
					reason
				);
				return;
			}
		}

		*current_error = Some(bg_error.clone());
		drop(current_error);

		// Update stats
		self.error_count.fetch_add(1, Ordering::Relaxed);

		// Set stopped flag if severity is HardError or higher
		if severity >= ErrorSeverity::HardError {
			self.is_db_stopped.store(true, Ordering::Release);
			log::error!(
				"Background error (severity {:?}, reason {:?}, timestamp {:?}): {}",
				severity,
				bg_error.reason,
				bg_error.timestamp,
				error
			);
		} else {
			log::warn!(
				"Background error (severity {:?}, reason {:?}, timestamp {:?}): {}",
				severity,
				bg_error.reason,
				bg_error.timestamp,
				error
			);
		}
	}

	/// Check if the database is stopped due to a background error.
	/// Returns the error if stopped, Ok(()) otherwise.
	/// This is the fast path check used before user operations.
	pub fn check_error(&self) -> Result<()> {
		// Fast path: check atomic flag first
		if !self.is_db_stopped.load(Ordering::Acquire) {
			return Ok(());
		}

		// Slow path: get the actual error
		let bg_error = self.bg_error.read();
		if let Some(ref error) = *bg_error {
			if error.severity >= ErrorSeverity::HardError {
				return Err(error.error.clone());
			}
		}

		Ok(())
	}

	/// Get the current background error, if any
	#[cfg(test)]
	pub fn get_error(&self) -> Option<BackgroundError> {
		self.bg_error.read().clone()
	}

	/// Check if database is stopped (fast path, lock-free)
	#[cfg(test)]
	pub fn is_db_stopped(&self) -> bool {
		self.is_db_stopped.load(Ordering::Acquire)
	}

	/// Get the error count
	#[cfg(test)]
	pub fn error_count(&self) -> u64 {
		self.error_count.load(Ordering::Relaxed)
	}

	/// Clear the background error (for recovery scenarios)
	#[cfg(test)]
	pub fn clear_error(&self) {
		let mut error = self.bg_error.write();
		*error = None;
		drop(error);
		self.is_db_stopped.store(false, Ordering::Release);
		log::info!("Background error cleared");
	}
}

impl Default for BackgroundErrorHandler {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::*;

	#[test]
	fn test_error_severity_ordering() {
		assert!(ErrorSeverity::NoError < ErrorSeverity::SoftError);
		assert!(ErrorSeverity::SoftError < ErrorSeverity::HardError);
		assert!(ErrorSeverity::HardError < ErrorSeverity::FatalError);
		assert!(ErrorSeverity::FatalError < ErrorSeverity::Unrecoverable);
	}

	#[test]
	fn test_set_and_check_error() {
		let handler = BackgroundErrorHandler::new();
		assert!(!handler.is_db_stopped());

		// Set a hard error
		handler.set_error(
			Error::Io(Arc::new(std::io::Error::other("test error"))),
			BackgroundErrorReason::MemtablaFlush,
		);

		assert!(handler.is_db_stopped());
		assert!(handler.check_error().is_err());
		assert_eq!(handler.error_count(), 1);
	}

	#[test]
	fn test_error_severity_upgrade() {
		let handler = BackgroundErrorHandler::new();

		// Set a hard error first
		handler.set_error(
			Error::Io(Arc::new(std::io::Error::other("hard error"))),
			BackgroundErrorReason::ManifestWrite,
		);

		assert!(handler.is_db_stopped());

		// Set a fatal error - should upgrade
		handler.set_error(
			Error::Io(Arc::new(std::io::Error::other("fatal error"))),
			BackgroundErrorReason::MemtablaFlush,
		);

		assert!(handler.is_db_stopped());
		// Verify the error was upgraded to fatal
		let error = handler.get_error().unwrap();
		assert_eq!(error.severity, ErrorSeverity::FatalError);
	}

	#[test]
	fn test_error_downgrade_prevented() {
		let handler = BackgroundErrorHandler::new();

		// Set a hard error first
		handler.set_error(
			Error::Io(Arc::new(std::io::Error::other("hard error"))),
			BackgroundErrorReason::MemtablaFlush,
		);

		let first_error = handler.get_error().unwrap().error;

		// Try to set a soft error - should not downgrade
		handler.set_error(
			Error::Io(Arc::new(std::io::Error::other("soft error"))),
			BackgroundErrorReason::Compaction,
		);

		// Error should still be the hard error
		let current_error = handler.get_error().unwrap();
		assert_eq!(current_error.error.to_string(), first_error.to_string());
	}

	#[test]
	fn test_clear_error() {
		let handler = BackgroundErrorHandler::new();

		handler.set_error(
			Error::Io(Arc::new(std::io::Error::other("test error"))),
			BackgroundErrorReason::MemtablaFlush,
		);

		assert!(handler.is_db_stopped());
		handler.clear_error();
		assert!(!handler.is_db_stopped());
		assert!(handler.check_error().is_ok());
	}
}
