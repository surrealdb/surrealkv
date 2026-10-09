use std::cmp::Reverse;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use std::{fmt, io};

use parking_lot::RwLock;

/// Result returning Error
pub type Result<T> = std::result::Result<T, Error>;

/// `Error` is a custom error type for the storage module.
/// It includes various variants to represent different types of errors that can
/// occur.
#[derive(Clone, Debug)]
#[non_exhaustive]
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
	WriteStall {
		reason: WriteStallReason,
	},
	ArenaFull, // Memtable arena is full, need rotation
	FileDescriptorNotFound,
	TableIDCollision(u64),
	TableNotFound(u64),
	PipelineStall,
	/// A commit group failed after part of it was applied, and the database stopped. Whether the
	/// commits of that group took effect is decided by recovery when the database is opened
	/// again, so a commit of that group that fails with this error has an unknown outcome. A
	/// commit that was refused because the database had stopped did not start. Reads of data that
	/// was already visible keep working.
	DatabaseStopped(String),
	Other(String), // Other errors
	NoSnapshot,
	CommitFail(String),
	LoadManifestFail(String),
	Corruption(String), // Data corruption detected
	ManifestCorruption(String), /* Manifest inconsistency detected (e.g., log_number exceeds
	                     * WAL segments) */
	/// No manifest exists but the directory holds database files, so opening
	/// would start a new database over them and discard them.
	ManifestMissing(String),
	/// The manifest was being replaced when a step failed from the rename on, so the file on disk
	/// is either the old manifest or the new one, and the new one may not be durable.
	ManifestWriteUncertain(String),
	InvalidArgument(String),
	InvalidTag(String),
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
            Self::TransactionRetry => write!(f, "Transaction retry required: snapshot is older than the commit oracle's GC window"),
            Self::TransactionClosed => write!(f, "Transaction closed"),
            Self::EmptyKey => write!(f, "Empty key"),
            Self::TransactionWriteOnly => write!(f, "Transaction is write-only"),
            Self::TransactionReadOnly => write!(f, "Transaction is read-only"),
            Self::TransactionWithoutSavepoint => write!(f, "Transaction has no savepoint to rollback to"),
            Self::KeyNotFound => write!(f, "Key not found"),
            Self::WriteStall { reason } => write!(f, "Write stall: {:?}", reason),
            Self::ArenaFull => write!(f, "Memtable arena is full"),
            Self::FileDescriptorNotFound => write!(f, "File descriptor not found"),
			Self::TableIDCollision(id) => write!(f, "CRITICAL ERROR: Table ID collision detected. New table ID {id} conflicts with a table ID in the merge list."),
			Self::TableNotFound(id) => write!(f, "Table not found: {id}"),
			Self::PipelineStall => write!(f, "Pipeline stall"),
			Self::DatabaseStopped(err) => write!(f, "Database stopped: {err}"),
            Self::Other(err) => write!(f, "Other error: {err}"),
            Self::NoSnapshot => write!(f, "No snapshot available"),
            Self::CommitFail(err) => write!(f, "Commit failed: {err}"),
            Self::LoadManifestFail(err) => write!(f, "Failed to load manifest: {err}"),
            Self::Corruption(err) => write!(f, "Data corruption detected: {err}"),
            Self::ManifestCorruption(err) => write!(f, "Manifest corruption detected: {err}"),
            Self::ManifestMissing(err) => write!(f, "Manifest missing: {err}"),
            Self::ManifestWriteUncertain(err) => write!(f, "Manifest write outcome unknown: {err}"),
            Self::InvalidArgument(err) => write!(f, "Invalid argument: {err}"),
            Self::InvalidTag(err) => write!(f, "Invalid tag: {err}"),
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
	NoError = 0,
	/// Error is recoverable, background work continues
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
	/// A commit group failed after part of it was applied to a memtable or an L0 table.
	CommitGroup,
}

/// Reason for write stall - used for logging and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStallReason {
	/// Too many immutable memtables queued for flush
	MemtableLimit,
	/// Too many L0 files awaiting compaction
	L0FileLimit,
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
	/// The current error of each reason, at most one per reason
	bg_errors: RwLock<Vec<BackgroundError>>,
	/// Fast atomic flag for write path checks
	is_db_stopped: AtomicBool,
	/// The error of the commit group that stopped the database, kept apart from `bg_errors` so
	/// that a flush, a compaction or a checkpoint can refuse to run without taking the lock, and
	/// so that no recovery can end it.
	commit_group_stop: OnceLock<Error>,
	/// Stats tracking
	error_count: AtomicU64,
}

impl BackgroundErrorHandler {
	/// Creates a new background error handler
	pub fn new() -> Self {
		Self {
			bg_errors: RwLock::new(Vec::new()),
			is_db_stopped: AtomicBool::new(false),
			commit_group_stop: OnceLock::new(),
			error_count: AtomicU64::new(0),
		}
	}

	/// Classify error severity based on error type and context
	fn classify_error(error: &Error, reason: BackgroundErrorReason) -> ErrorSeverity {
		match (reason, error) {
			// A memtable or a table holds batches that were never published: the process
			// cannot go on
			(BackgroundErrorReason::CommitGroup, _) => ErrorSeverity::Unrecoverable,

			// Corruption errors are unrecoverable
			(_, Error::Corruption(_) | Error::CorruptedBlock(_) | Error::ManifestCorruption(_)) => {
				ErrorSeverity::Unrecoverable
			}

			// Table ID collision is a critical consistency error
			(_, Error::TableIDCollision(_)) => ErrorSeverity::Unrecoverable,

			// Corrupted table metadata is unrecoverable
			(_, Error::CorruptedTableMetadata(_)) => ErrorSeverity::Unrecoverable,

			// The manifest on disk may not be the one in memory: nothing may be retried on it
			(_, Error::ManifestWriteUncertain(_)) => ErrorSeverity::FatalError,

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

	/// The most severe error, the earliest of those equally severe.
	fn most_severe(errors: &[BackgroundError]) -> Option<&BackgroundError> {
		errors.iter().min_by_key(|e| (Reverse(e.severity), e.timestamp))
	}

	/// Sets the stopped flag from `errors`, which the caller holds the lock of.
	fn update_stopped(&self, errors: &[BackgroundError]) {
		let stopped = errors.iter().any(|e| e.severity >= ErrorSeverity::HardError);
		self.is_db_stopped.store(stopped, Ordering::Release);
	}

	/// Set a background error. This is called by background tasks when they encounter errors.
	/// Returns the severity of the error now recorded for `reason`: the new error's, or that of
	/// the more severe one it did not replace.
	pub fn set_error(&self, error: Error, reason: BackgroundErrorReason) -> ErrorSeverity {
		let severity = Self::classify_error(&error, reason);
		let bg_error = BackgroundError {
			error: error.clone(),
			severity,
			reason,
			timestamp: Instant::now(),
		};

		// An error of a reason replaces the one it has only if it is more severe, and one reason
		// never hides another's: each is ended by its own recovery
		let mut errors = self.bg_errors.write();
		match errors.iter_mut().find(|e| e.reason == reason) {
			Some(existing) if severity <= existing.severity => {
				tracing::debug!(
					"Background error not updated: new severity {:?} <= existing {:?}, error: {:?}, reason: {:?}",
					severity,
					existing.severity,
					error.to_string(),
					reason
				);
				return existing.severity;
			}
			Some(existing) => *existing = bg_error.clone(),
			None => errors.push(bg_error.clone()),
		}
		self.update_stopped(&errors);
		drop(errors);

		// Update stats
		self.error_count.fetch_add(1, Ordering::Relaxed);

		if severity >= ErrorSeverity::HardError {
			tracing::error!(
				"Background error (severity {:?}, reason {:?}, timestamp {:?}): {}",
				severity,
				bg_error.reason,
				bg_error.timestamp,
				error
			);
		} else {
			tracing::warn!(
				"Background error (severity {:?}, reason {:?}, timestamp {:?}): {}",
				severity,
				bg_error.reason,
				bg_error.timestamp,
				error
			);
		}
		severity
	}

	/// Ends the error recorded for `reason` once the work that failed has succeeded, so that
	/// writes are accepted again. Only an error that may auto-recover (below `FatalError`) is
	/// ended: a fatal one stays, and so does the error of any other reason, however it compares.
	/// Returns whether an error was ended.
	pub(crate) fn recover(&self, reason: BackgroundErrorReason) -> bool {
		let mut errors = self.bg_errors.write();
		let Some(at) = errors
			.iter()
			.position(|e| e.reason == reason && e.severity < ErrorSeverity::FatalError)
		else {
			return false;
		};
		let recovered = errors.remove(at);
		self.update_stopped(&errors);
		drop(errors);
		tracing::info!(
			"Background error recovered (severity {:?}, reason {:?}): {}",
			recovered.severity,
			recovered.reason,
			recovered.error
		);
		true
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
		let errors = self.bg_errors.read();
		if let Some(error) = Self::most_severe(&errors) {
			if error.severity >= ErrorSeverity::HardError {
				return Err(error.error.clone());
			}
		}

		Ok(())
	}

	/// Records that a commit group failed after part of it was applied: every commit fails with
	/// `error` from now on, and `commit_group_error` returns it.
	pub(crate) fn stop_commit_group(&self, error: Error) {
		let _ = self.commit_group_stop.set(error.clone());
		self.set_error(error, BackgroundErrorReason::CommitGroup);
	}

	/// The error that stopped the database because a commit group failed after part of it was
	/// applied, if one did.
	///
	/// A memtable or a table then holds batches of the group that were never published. Nothing
	/// may move them to a table, compact tables that hold them, or checkpoint them: the WAL keeps
	/// the group for recovery, and a table that held only some of it would split it.
	pub(crate) fn commit_group_error(&self) -> Option<Error> {
		self.commit_group_stop.get().cloned()
	}

	/// Get the current background error, if any
	#[cfg(test)]
	pub fn get_error(&self) -> Option<BackgroundError> {
		Self::most_severe(&self.bg_errors.read()).cloned()
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
		let mut errors = self.bg_errors.write();
		errors.clear();
		drop(errors);
		self.is_db_stopped.store(false, Ordering::Release);
		tracing::debug!("Background error cleared");
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
