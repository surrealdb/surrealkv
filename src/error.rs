use std::cmp::Reverse;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::{fmt, io};

use parking_lot::RwLock;

use crate::rt::Instant;

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
	/// A table or database is stored in a legacy layout that this code does not read in place.
	/// Opening the database with `Tree::new` migrates it; this is returned by paths that cannot
	/// (a checkpoint restore, a direct table open).
	LegacyFormat(String),
	/// Migrating a database written by an older version or another engine failed. The database
	/// was left as it was or as a restartable half-migrated state; opening it again resumes.
	MigrationFailed(String),
	TableMetadataNotFound,
	Wal(String),
	BlockNotFound,
	/// The batch of a commit encodes to more bytes than one WAL record can hold, `u32::MAX`.
	/// Nothing of it reaches the WAL.
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
	/// was already visible keep working, including after a compaction that ran while the group was
	/// applied: a compaction keeps every version above the visible sequence number, and the newest
	/// one at or below it.
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
	/// A restore from a checkpoint began to replace the files of the database and did not finish,
	/// so they are neither the database as it was nor the checkpoint.
	RestoreInterrupted(String),
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
	/// Encryption at rest failed: a key problem, an authentication failure or an unreadable
	/// encryption header. None of these is ever repaired by dropping data.
	Encryption(EncryptionError),
	/// A file system that keeps a fixed pool of files had none free for a file that was to be
	/// created. This passes: the pool is refilled in the background and the work that asked can be
	/// done again, so a flush or a compaction that fails with it waits and retries, and a build
	/// that fails with it is built again once the pool has more. A database that was open is not
	/// stopped by it.
	FilePoolExhausted,
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
            Self::LegacyFormat(err) => write!(f, "Legacy format: {err}"),
            Self::MigrationFailed(err) => write!(f, "Migration failed: {err}"),
            Self::TableMetadataNotFound => write!(f, "Table metadata not found"),
            Self::Wal(err) => write!(f, "WAL error: {err}"),
            Self::BlockNotFound => write!(f, "Block not found"),
            Self::BatchTooLarge => write!(f, "Batch too large: a batch encodes to at most {} bytes", u32::MAX),
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
            Self::RestoreInterrupted(err) => write!(f, "Restore interrupted: {err}"),
            Self::InvalidArgument(err) => write!(f, "Invalid argument: {err}"),
            Self::InvalidTag(err) => write!(f, "Invalid tag: {err}"),
            Self::InterleavedIteration => write!(f, "Interleaved iteration not supported: cannot mix next() and next_back() on same iterator"),
            Self::WalCorruption { segment_id, offset, message } => write!(
                f,
                "WAL corruption in segment {} at offset {}: {}",
                segment_id, offset, message
            ),
            Self::SSTable(err) => write!(f, "SSTable error: {err}"),
            Self::Encryption(err) => write!(f, "Encryption error: {err}"),
            Self::FilePoolExhausted => write!(f, "The file pool has no free file for a new file"),
        }
	}
}

// Implementation of Error trait for Error
impl std::error::Error for Error {}

impl Error {
	/// Whether this is an I/O error that a pool raised for holding as many files as it may: the
	/// one that does not pass by waiting for a refill (see `vfs::PoolFull`). It is an `Error::Io`
	/// and keeps that shape, so that the layers that read an I/O error as fatal go on doing so,
	/// and the layers that turn an error into text must let it through to keep it typed.
	pub(crate) fn is_pool_full(&self) -> bool {
		matches!(self, Error::Io(e) if crate::vfs::is_pool_full(e))
	}

	/// Whether this is an I/O error of a file system that ran out of room: the pool at its limit
	/// of files (`is_pool_full`), or the quota of a browser (`QuotaExceededError`, which the OPFS
	/// backend gives the kind `StorageFull`). It passes when files are released or space is freed,
	/// which a page does at any time, so it is a soft error and not a fatal one, and it keeps its
	/// type through the layers that turn an error into text.
	///
	/// The disk of a native file system is not one of these: its error carries an OS error code
	/// (`raw_os_error`), which none of the file systems of a pool or a browser make, and it is
	/// treated as it always was.
	pub(crate) fn is_storage_full(&self) -> bool {
		matches!(
			self,
			Error::Io(e) if e.kind() == std::io::ErrorKind::StorageFull && e.raw_os_error().is_none()
		)
	}

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
		// A pool that had no free file says so in the payload of its error, and the type keeps it
		// through the layers that wrap an I/O error: it is what tells a failure that passes from
		// one that does not.
		if crate::vfs::is_pool_exhausted(&e) {
			Error::FilePoolExhausted
		} else {
			Error::Io(Arc::new(e))
		}
	}
}

impl From<crate::wal::Error> for Error {
	fn from(err: crate::wal::Error) -> Self {
		match err {
			// Keeps its type: whoever handles it must tell a key from damage.
			crate::wal::Error::Encryption(err) => Error::Encryption(err),
			// And this one a pool that has no free file from a log that cannot be written.
			crate::wal::Error::PoolExhausted => Error::FilePoolExhausted,
			// A browser that is out of quota keeps its type: it passes once there is room.
			crate::wal::Error::StorageFull(message) => {
				Error::Io(Arc::new(std::io::Error::new(std::io::ErrorKind::StorageFull, message)))
			}
			err => Error::Wal(err.to_string()),
		}
	}
}

impl From<crate::sstable::error::SSTableError> for Error {
	fn from(err: crate::sstable::error::SSTableError) -> Self {
		Error::SSTable(err)
	}
}

impl From<EncryptionError> for Error {
	fn from(err: EncryptionError) -> Self {
		Error::Encryption(err)
	}
}

impl<T> From<std::sync::PoisonError<T>> for Error {
	fn from(_err: std::sync::PoisonError<T>) -> Self {
		Error::Other("Lock poisoned - another thread panicked while holding the lock".to_string())
	}
}

/// Why a key provider could not hand out key material.
///
/// Carries no free text on purpose: a provider's own message can name key ids or account
/// identifiers, and this type ends up in logs through `Debug`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum KeyProviderFailure {
	/// The provider could not be reached or is throttling. Trying again later may succeed.
	Unavailable,
	/// The provider answered and refused, or failed. Trying again will not help.
	Failed,
	/// The operating system's random number generator failed, so no fresh per-file key
	/// material could be drawn.
	RandomSource,
}

/// Why encrypted data could not be written or read.
///
/// The texts name no key id, no key material and no path, so they are safe to log. Every one of
/// these is a statement about keys, headers or authentication, never about the shape of the data
/// that follows, so none of them may be answered by repairing or deleting a file.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum EncryptionError {
	/// A file is encrypted but no key manager is configured.
	NotConfigured,
	/// A plaintext file was found in a database that is encrypted.
	PlaintextInEncryptedDb,
	/// The key manager has never held the key id the file names.
	UnknownKeyId,
	/// The key the file names has been retired, so its data can no longer be read, or a new file
	/// was about to be written under a key that is not active.
	KeyRetired,
	/// The file header is intact but does not authenticate under the configured key, so the key
	/// is not the one the file was written with.
	WrongKey,
	/// Sealed bytes failed authentication: they were corrupted, changed, or moved to another
	/// file, position or kind.
	AuthenticationFailed,
	/// The file uses a cipher suite this build does not support, or one that is reserved.
	UnsupportedSuite,
	/// The file uses an encryption format version, or a feature of the format, that this build
	/// does not support. A header whose checksum matched but that uses what this version leaves
	/// unused is reported as this and not as damage.
	UnsupportedVersion,
	/// The encryption header is truncated, fails its checksum or is otherwise not well formed.
	/// A checksum failure is media corruption, which is told apart from a wrong key on purpose.
	MalformedHeader,
	/// The key provider could not supply what was asked of it.
	KeyProvider(KeyProviderFailure),
	/// The file belongs to a different database than the one that is open.
	DbIdMismatch,
	/// The file is not the file the caller expected: its header names another file kind or id.
	/// This is what a file that was renamed or copied over another looks like.
	FileIdMismatch,
	/// A seal was asked for at a position that is not strictly after the last one sealed under
	/// the same file key. Sealing there would repeat a nonce.
	PositionReuse,
	/// The caller passed arguments that cannot be sealed or opened, such as a block kind that
	/// does not belong to the file kind. A programming error, never produced by file contents
	/// alone.
	InvalidInput(&'static str),
	/// Encryption is configured, but the database, or the checkpoint that is being restored into
	/// it, is not encrypted. Encryption is chosen when a database is created and is never added
	/// to one that exists, so this is refused and nothing is changed. A plaintext file inside a
	/// database that is encrypted is `PlaintextInEncryptedDb`.
	NotEncrypted,
	/// A checkpoint is encrypted with another cipher suite than the database it is restored into.
	/// A database keeps the suite that was chosen when it was created.
	SuiteMismatch,
}

impl EncryptionError {
	/// Whether trying the same operation again later may succeed. Only a provider that is
	/// temporarily unavailable qualifies; everything else is a fact about the files or the keys.
	pub const fn is_retryable(&self) -> bool {
		matches!(self, Self::KeyProvider(KeyProviderFailure::Unavailable))
	}

	/// The class the background tasks sort this error into, see
	/// [`BackgroundErrorHandler::classify_error`]. The match is exhaustive on purpose: an error
	/// added later has to be given a class here before the crate builds, and has none by default.
	pub(crate) const fn class(&self) -> EncryptionClass {
		match self {
			Self::KeyProvider(KeyProviderFailure::Unavailable) => EncryptionClass::Retryable,
			Self::WrongKey
			| Self::UnknownKeyId
			| Self::KeyRetired
			| Self::NotConfigured
			| Self::NotEncrypted
			| Self::PlaintextInEncryptedDb
			| Self::DbIdMismatch
			| Self::SuiteMismatch
			| Self::FileIdMismatch
			| Self::UnsupportedSuite
			| Self::UnsupportedVersion
			| Self::MalformedHeader
			| Self::KeyProvider(KeyProviderFailure::Failed) => EncryptionClass::Key,
			Self::AuthenticationFailed | Self::PositionReuse => EncryptionClass::Integrity,
			Self::KeyProvider(KeyProviderFailure::RandomSource) | Self::InvalidInput(_) => {
				EncryptionClass::Fatal
			}
		}
	}
}

/// What a failure of encryption at rest says about what to do next. See
/// [`BackgroundErrorHandler::classify_error`] for what each class does to a background task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EncryptionClass {
	/// The key provider could not be reached. Asking again later may work.
	Retryable,
	/// The configuration, the key or the files do not fit one another. Asking again changes
	/// nothing, and no file is to be repaired or removed because of it.
	Key,
	/// Sealed bytes failed to authenticate, or a seal was refused for reusing a position. Not a
	/// fact about the key, and not damage that a repair could tell from tampering.
	Integrity,
	/// The environment cannot provide what sealing needs, or the code asked for what cannot be
	/// sealed.
	Fatal,
}

impl fmt::Display for KeyProviderFailure {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Unavailable => write!(f, "the key provider is unavailable"),
			Self::Failed => write!(f, "the key provider failed"),
			Self::RandomSource => write!(f, "the system random number generator failed"),
		}
	}
}

impl fmt::Display for EncryptionError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::NotConfigured => {
				write!(f, "the data is encrypted but no key manager is configured")
			}
			Self::PlaintextInEncryptedDb => {
				write!(f, "a plaintext file was found in an encrypted database")
			}
			Self::UnknownKeyId => {
				write!(f, "the key manager does not know the key the data was encrypted with")
			}
			Self::KeyRetired => write!(f, "the key is retired and cannot be used"),
			Self::WrongKey => {
				write!(f, "wrong key: the encryption header does not authenticate under it")
			}
			Self::AuthenticationFailed => {
				write!(f, "authentication failed: the data is corrupt, was changed, or was moved")
			}
			Self::UnsupportedSuite => write!(f, "unsupported cipher suite"),
			Self::UnsupportedVersion => write!(f, "unsupported encryption format version"),
			Self::MalformedHeader => write!(f, "malformed encryption header"),
			Self::KeyProvider(failure) => write!(f, "key provider error: {failure}"),
			Self::DbIdMismatch => write!(f, "the file belongs to a different database"),
			Self::FileIdMismatch => write!(f, "the file is not the one that was expected"),
			Self::PositionReuse => {
				write!(f, "refusing to seal at a position that would repeat a nonce")
			}
			Self::InvalidInput(reason) => write!(f, "invalid encryption input: {reason}"),
			Self::NotEncrypted => write!(
				f,
				"the data is not encrypted but encryption is configured, and encryption cannot be \
				 added to a database that exists"
			),
			Self::SuiteMismatch => {
				write!(f, "the data uses another cipher suite than the database")
			}
		}
	}
}

impl std::error::Error for EncryptionError {}

/// Error severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[non_exhaustive]
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
#[non_exhaustive]
pub enum BackgroundErrorReason {
	MemtablaFlush,
	Compaction,
	ManifestWrite,
	/// A commit group failed after part of it was applied to a memtable or an L0 table.
	CommitGroup,
	/// An fsync of the value log failed: the values it was to make durable may be lost, and no
	/// later fsync can make them durable.
	ValueLogSync,
}

/// Reason for write stall - used for logging and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriteStallReason {
	/// Too many immutable memtables queued for flush
	MemtableLimit,
	/// Too many L0 files awaiting compaction
	L0FileLimit,
}

/// Represents a background error with its severity and context
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BackgroundError {
	pub error: Error,
	pub severity: ErrorSeverity,
	pub reason: BackgroundErrorReason,
	/// When the error was recorded, on a monotonic clock.
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

	/// Classify error severity based on error type and context.
	///
	/// A failure of encryption at rest (`Error::Encryption`) is sorted by what it says, and the
	/// class decides what a background task does with it:
	///
	/// | class | errors | severity | tried again | what follows |
	/// |---|---|---|---|---|
	/// | retryable | `KeyProvider(Unavailable)` | soft | yes, after the capped backoff | nothing stops; the work waits for the provider |
	/// | key | `WrongKey`, `UnknownKeyId`, `KeyRetired`, `NotConfigured`, `NotEncrypted`, `PlaintextInEncryptedDb`, `DbIdMismatch`, `SuiteMismatch`, `FileIdMismatch`, `UnsupportedSuite`, `UnsupportedVersion`, `MalformedHeader`, `KeyProvider(Failed)` | hard | no | writes stop, with this error |
	/// | integrity | `AuthenticationFailed`, `PositionReuse` | unrecoverable | no | writes stop until the database is opened again, reported as this class |
	/// | fatal | `KeyProvider(RandomSource)`, `InvalidInput` | fatal | no | writes stop for good |
	///
	/// No class repairs, moves or removes a file: that is not something a task does when it fails,
	/// and a key that does not fit says nothing about the files. The error keeps its type all the
	/// way here, and its text names no key, key id or path.
	///
	/// A soft error is recorded and does not stop writes, so a provider that is away for a while
	/// costs the database nothing but the flushes and compactions that wait for it, and the stall
	/// that follows if they wait long enough. A hard one stops writes, and the first flush that
	/// succeeds afterwards ends it, as it ends any hard error.
	pub(crate) fn classify_error(error: &Error, reason: BackgroundErrorReason) -> ErrorSeverity {
		match (reason, error) {
			// A memtable or a table holds batches that were never published: the process
			// cannot go on
			(BackgroundErrorReason::CommitGroup, _) => ErrorSeverity::Unrecoverable,

			// The value log refuses every sync after a failed one, so no write can be made
			// durable until the database is opened again. Whatever the fsync failed with (a full
			// disk included, which has lost the pages all the same), the error recorded under this
			// reason is fatal.
			(BackgroundErrorReason::ValueLogSync, _) => ErrorSeverity::FatalError,

			// A pool of files that has none free for now: a flush, a compaction or a manifest write
			// that met it did not fail, it has to wait, and nothing it wrote is wrong. It must
			// stay below the commit-group arm, which wins over every error, and above the default.
			(_, Error::FilePoolExhausted) => ErrorSeverity::SoftError,

			// A pool at its limit of files, or a quota that the browser enforces: nothing that was
			// written is wrong, and the page can release files or free space at any time. The
			// work is tried again with a back-off, and commits meanwhile fail with the typed error
			// (never a stopped database). This is above the I/O arms of a flush and a manifest
			// write, which are fatal; a full disk of a native file system has an OS error code and
			// still falls through to them.
			(_, e) if e.is_storage_full() => ErrorSeverity::SoftError,

			(_, Error::Encryption(err)) => match err.class() {
				EncryptionClass::Retryable => ErrorSeverity::SoftError,
				EncryptionClass::Key => ErrorSeverity::HardError,
				EncryptionClass::Integrity => ErrorSeverity::Unrecoverable,
				EncryptionClass::Fatal => ErrorSeverity::FatalError,
			},

			// Corruption errors are unrecoverable
			(
				_,
				Error::Corruption(_)
				| Error::CorruptedBlock(_)
				| Error::ManifestCorruption(_)
				| Error::SSTable(_),
			) => ErrorSeverity::Unrecoverable,

			// Table ID collision is a critical consistency error
			(_, Error::TableIDCollision(_)) => ErrorSeverity::Unrecoverable,

			// Corrupted table metadata is unrecoverable
			(_, Error::CorruptedTableMetadata(_)) => ErrorSeverity::Unrecoverable,

			// The manifest on disk may not be the one in memory: nothing may be retried on it
			(_, Error::ManifestWriteUncertain(_)) => ErrorSeverity::FatalError,

			// I/O errors during memtable flush are fatal
			(BackgroundErrorReason::MemtablaFlush, Error::Io(_)) => ErrorSeverity::FatalError,

			// Retryable compaction errors are soft errors: background work continues and
			// retries, writes continue until L0 stall threshold is reached
			(BackgroundErrorReason::Compaction, _) => ErrorSeverity::SoftError,

			// Manifest write I/O is fatal
			(BackgroundErrorReason::ManifestWrite, Error::Io(_)) => ErrorSeverity::FatalError,

			// Default: treat as hard error for safety
			_ => ErrorSeverity::HardError,
		}
	}

	/// Whether a background task that failed with `error` tries the work again, `severity` being
	/// what `set_error` answered for it.
	///
	/// A memtable flush is tried again after any error below `FatalError`, as it always was, with
	/// one exception: an encryption error that trying again cannot cure (a key that does not fit, a
	/// failed authentication) is not, because every attempt would create the table again, write the
	/// large values to the value log again and fail at the same place. A compaction is tried again
	/// after any error that is soft, and after an encryption error only if it is of the retryable
	/// class: every other error stops the database.
	pub(crate) fn retries(
		error: &Error,
		severity: ErrorSeverity,
		reason: BackgroundErrorReason,
	) -> bool {
		match (reason, error) {
			(BackgroundErrorReason::MemtablaFlush, Error::Encryption(err)) => {
				err.is_retryable() && severity < ErrorSeverity::FatalError
			}
			(BackgroundErrorReason::MemtablaFlush, _) => severity < ErrorSeverity::FatalError,
			(BackgroundErrorReason::Compaction, Error::Encryption(err)) => {
				err.is_retryable() && severity < ErrorSeverity::HardError
			}
			(BackgroundErrorReason::Compaction, _) => severity == ErrorSeverity::SoftError,
			_ => false,
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
		self.record_error(error, reason, severity)
	}

	/// The severity that a background failure has when a pool of files refused a creation during
	/// the unit of work that failed (`exhausted`), whatever shape the failure has now. It is the
	/// rewrap-proof second net of the typed `Error::FilePoolExhausted`: an error that went through
	/// a layer that turned it into text is classified as the unrelated failure it looks like, and
	/// would stop the database, where the pool is a moment away from having a file.
	///
	/// A failure is `SoftError` then, the work is run again, and nothing stops. Left as it is: what
	/// is `Unrecoverable` (corruption, a commit group), what leaves the manifest on disk in doubt
	/// (`ManifestWriteUncertain`, which is not a missing file whatever else happened in the unit),
	/// and the error of a pool that is at its limit (`is_pool_full`), which is soft in itself
	/// whatever happened in the unit.
	pub(crate) fn classify_after_exhaustion(
		error: &Error,
		reason: BackgroundErrorReason,
		exhausted: bool,
	) -> ErrorSeverity {
		let severity = Self::classify_error(error, reason);
		if exhausted
			&& severity < ErrorSeverity::Unrecoverable
			&& !matches!(error, Error::ManifestWriteUncertain(_))
			&& !error.is_pool_full()
		{
			if severity > ErrorSeverity::SoftError {
				tracing::debug!(
					"A pool of files refused a creation during the unit that failed with {error}: \
					 the failure is taken as one that passes, not as {severity:?}"
				);
			}
			return ErrorSeverity::SoftError;
		}
		severity
	}

	/// [`set_error`](Self::set_error) for a unit of work during which a pool may have refused a
	/// creation: see [`classify_after_exhaustion`](Self::classify_after_exhaustion).
	pub(crate) fn set_error_after_exhaustion(
		&self,
		error: Error,
		reason: BackgroundErrorReason,
		exhausted: bool,
	) -> ErrorSeverity {
		let severity = Self::classify_after_exhaustion(&error, reason, exhausted);
		self.record_error(error, reason, severity)
	}

	/// Records `error` as the error of `reason` with `severity`.
	fn record_error(
		&self,
		error: Error,
		reason: BackgroundErrorReason,
		severity: ErrorSeverity,
	) -> ErrorSeverity {
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
		self.recover_below(reason, ErrorSeverity::FatalError)
	}

	/// Like [`recover`](Self::recover), but ends only a soft error: one that stopped nothing, such
	/// as a key provider that was away. A compaction that fails for any other reason leaves its
	/// error on record for good, and a later compaction that works does not end it.
	pub(crate) fn recover_soft(&self, reason: BackgroundErrorReason) -> bool {
		self.recover_below(reason, ErrorSeverity::HardError)
	}

	/// Ends the error recorded for `reason` if its severity is below `limit`.
	fn recover_below(&self, reason: BackgroundErrorReason, limit: ErrorSeverity) -> bool {
		let mut errors = self.bg_errors.write();
		let Some(at) = errors.iter().position(|e| e.reason == reason && e.severity < limit) else {
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
	///
	/// A compaction that overlapped the group before it failed keeps those batches, and the
	/// version of each of their keys that readers can see. That is one extra version per key of
	/// the group in flight, kept until a later compaction rewrites the key after the group was
	/// published; at the bottom level a cold key can keep it indefinitely. A stopped database
	/// admits no further compaction, so nothing is added after the stop, and a reopen publishes
	/// whatever recovery finds.
	pub(crate) fn commit_group_error(&self) -> Option<Error> {
		self.commit_group_stop.get().cloned()
	}

	/// Get the current background error, if any
	pub fn get_error(&self) -> Option<BackgroundError> {
		Self::most_severe(&self.bg_errors.read()).cloned()
	}

	/// Check if database is stopped (fast path, lock-free)
	pub fn is_db_stopped(&self) -> bool {
		self.is_db_stopped.load(Ordering::Acquire)
	}

	/// Get the error count
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
	fn test_recover_ends_the_recoverable_error_of_its_reason() {
		let handler = BackgroundErrorHandler::new();

		let severity =
			handler.set_error(Error::Other("flush".into()), BackgroundErrorReason::MemtablaFlush);
		assert_eq!(severity, ErrorSeverity::HardError);
		assert!(handler.is_db_stopped());

		assert!(handler.recover(BackgroundErrorReason::MemtablaFlush));
		assert!(!handler.is_db_stopped());
		assert!(handler.check_error().is_ok());
		assert!(!handler.recover(BackgroundErrorReason::MemtablaFlush), "nothing is left to end");
	}

	#[test]
	fn test_recover_leaves_a_fatal_error() {
		let handler = BackgroundErrorHandler::new();

		let severity = handler.set_error(
			Error::ManifestWriteUncertain("renamed".into()),
			BackgroundErrorReason::MemtablaFlush,
		);
		assert_eq!(severity, ErrorSeverity::FatalError);

		assert!(!handler.recover(BackgroundErrorReason::MemtablaFlush));
		assert!(handler.is_db_stopped());
		assert!(handler.check_error().is_err());
	}

	#[test]
	fn test_recover_leaves_an_equally_severe_error_of_another_reason() {
		let handler = BackgroundErrorHandler::new();
		// Recorded first: recovering the flush must not take it for the flush's own.
		handler.set_error(Error::Other("manifest".into()), BackgroundErrorReason::ManifestWrite);
		handler.set_error(Error::Other("flush".into()), BackgroundErrorReason::MemtablaFlush);

		assert!(handler.recover(BackgroundErrorReason::MemtablaFlush));
		assert!(handler.is_db_stopped());
		assert_eq!(handler.get_error().unwrap().reason, BackgroundErrorReason::ManifestWrite);
		assert!(!handler.recover(BackgroundErrorReason::MemtablaFlush));
		assert!(handler.check_error().is_err());
	}

	#[test]
	fn test_recover_leaves_a_more_severe_error_of_another_reason() {
		let handler = BackgroundErrorHandler::new();
		handler.set_error(Error::Other("flush".into()), BackgroundErrorReason::MemtablaFlush);
		handler.set_error(
			Error::ManifestWriteUncertain("compaction".into()),
			BackgroundErrorReason::Compaction,
		);

		assert!(handler.recover(BackgroundErrorReason::MemtablaFlush));
		assert!(handler.is_db_stopped());
		let remaining = handler.get_error().unwrap();
		assert_eq!(remaining.reason, BackgroundErrorReason::Compaction);
		assert_eq!(remaining.severity, ErrorSeverity::FatalError);
	}

	#[test]
	fn test_compaction_retryable_error_is_soft_error_and_recovers() {
		let handler = BackgroundErrorHandler::new();
		let severity = handler.set_error(
			Error::Io(Arc::new(std::io::Error::other("disk transient"))),
			BackgroundErrorReason::Compaction,
		);
		assert_eq!(severity, ErrorSeverity::SoftError);
		// Soft error does NOT stop the database: writes continue until stall gate
		assert!(!handler.is_db_stopped());
		assert!(handler.check_error().is_ok());

		// Error is visible via get_error
		let err = handler.get_error().unwrap();
		assert_eq!(err.reason, BackgroundErrorReason::Compaction);
		assert_eq!(err.severity, ErrorSeverity::SoftError);

		// Recovery ends the soft error
		assert!(handler.recover(BackgroundErrorReason::Compaction));
		assert!(handler.get_error().is_none());
	}

	/// The recovery of a flush ends the flush's own error, never the stop a commit group caused:
	/// whichever came first, the database stays stopped and reports the group's error.
	#[test]
	fn test_recover_never_ends_the_stop_of_a_commit_group() {
		for flush_first in [true, false] {
			let handler = BackgroundErrorHandler::new();
			let group = || Error::DatabaseStopped("group".into());
			if flush_first {
				handler
					.set_error(Error::Other("flush".into()), BackgroundErrorReason::MemtablaFlush);
				handler.stop_commit_group(group());
			} else {
				handler.stop_commit_group(group());
				handler
					.set_error(Error::Other("flush".into()), BackgroundErrorReason::MemtablaFlush);
			}

			assert!(handler.recover(BackgroundErrorReason::MemtablaFlush), "{flush_first}");
			assert!(!handler.recover(BackgroundErrorReason::MemtablaFlush), "{flush_first}");
			assert!(!handler.recover(BackgroundErrorReason::CommitGroup), "{flush_first}");
			assert!(handler.is_db_stopped(), "{flush_first}");
			assert!(
				matches!(handler.check_error(), Err(Error::DatabaseStopped(_))),
				"{flush_first}"
			);
			assert!(handler.commit_group_error().is_some(), "{flush_first}");
			assert_eq!(handler.get_error().unwrap().reason, BackgroundErrorReason::CommitGroup);
		}
	}

	/// A flush error that is more severe than the stop's own cannot hide it either: the stop stays
	/// on record, and is what is reported once the flush error is ended.
	#[test]
	fn test_a_flush_error_does_not_hide_the_stop_of_a_commit_group() {
		let handler = BackgroundErrorHandler::new();
		handler.stop_commit_group(Error::DatabaseStopped("group".into()));
		let severity = handler.set_error(
			Error::Io(Arc::new(std::io::Error::other("flush"))),
			BackgroundErrorReason::MemtablaFlush,
		);
		assert_eq!(severity, ErrorSeverity::FatalError);

		assert!(!handler.recover(BackgroundErrorReason::MemtablaFlush), "a fatal one stays");
		assert!(matches!(handler.check_error(), Err(Error::DatabaseStopped(_))));
		assert!(handler.commit_group_error().is_some());
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

	/// A less severe error of a reason does not replace the more severe one it has, and the
	/// recovery of that reason does not end it.
	#[test]
	fn a_less_severe_error_of_a_reason_does_not_replace_a_more_severe_one() {
		let handler = BackgroundErrorHandler::new();
		let fatal = handler.set_error(
			Error::ManifestWriteUncertain("renamed".into()),
			BackgroundErrorReason::MemtablaFlush,
		);
		assert_eq!(fatal, ErrorSeverity::FatalError);

		let hard =
			handler.set_error(Error::Other("flush".into()), BackgroundErrorReason::MemtablaFlush);
		assert_eq!(hard, ErrorSeverity::FatalError, "the severity reported is the one on record");

		let current = handler.get_error().unwrap();
		assert_eq!(current.severity, ErrorSeverity::FatalError);
		assert!(matches!(current.error, Error::ManifestWriteUncertain(_)));
		assert!(!handler.recover(BackgroundErrorReason::MemtablaFlush));
		assert!(handler.is_db_stopped());
	}

	/// An error that comes after a recovery stops the database again.
	#[test]
	fn an_error_after_a_recovery_stops_the_database_again() {
		let handler = BackgroundErrorHandler::new();
		handler.set_error(Error::Other("first".into()), BackgroundErrorReason::MemtablaFlush);
		assert!(handler.recover(BackgroundErrorReason::MemtablaFlush));
		assert!(handler.check_error().is_ok());

		handler.set_error(Error::Other("second".into()), BackgroundErrorReason::MemtablaFlush);
		assert!(handler.is_db_stopped());
		let error = handler.check_error().expect_err("the second failure stops the database");
		assert!(error.to_string().contains("second"), "{error}");
	}

	/// Of the errors of several reasons, the one reported is the most severe, whichever came first.
	#[test]
	fn the_error_reported_is_the_most_severe_of_those_of_every_reason() {
		let hard = || Error::Other("hard".into());
		let fatal = || Error::ManifestWriteUncertain("fatal".into());
		for hard_first in [true, false] {
			let handler = BackgroundErrorHandler::new();
			if hard_first {
				handler.set_error(hard(), BackgroundErrorReason::MemtablaFlush);
				handler.set_error(fatal(), BackgroundErrorReason::Compaction);
			} else {
				handler.set_error(fatal(), BackgroundErrorReason::Compaction);
				handler.set_error(hard(), BackgroundErrorReason::MemtablaFlush);
			}
			let current = handler.get_error().unwrap();
			assert_eq!(current.severity, ErrorSeverity::FatalError, "hard first: {hard_first}");
			let reported = handler.check_error().expect_err("the database is stopped");
			assert!(reported.to_string().contains("fatal"), "hard first: {hard_first}: {reported}");
		}
	}

	/// A burst of failures and recoveries from several threads leaves the handler consistent: the
	/// fast flag says what the errors it holds say.
	#[test]
	fn the_error_handler_stays_consistent_under_concurrent_failures_and_recoveries() {
		for round in 0..20 {
			let handler = Arc::new(BackgroundErrorHandler::new());
			let mut threads = Vec::new();
			for t in 0..6usize {
				let handler = Arc::clone(&handler);
				threads.push(std::thread::spawn(move || {
					for n in 0..400usize {
						let reason = match (t + n) % 3 {
							0 => BackgroundErrorReason::MemtablaFlush,
							1 => BackgroundErrorReason::Compaction,
							_ => BackgroundErrorReason::ManifestWrite,
						};
						match (t * 7 + n) % 5 {
							0 => {
								handler.set_error(Error::Other("hard".into()), reason);
							}
							1 => {
								handler.set_error(
									Error::ManifestWriteUncertain("fatal".into()),
									reason,
								);
							}
							_ => {
								handler.recover(reason);
							}
						}
						let _ = handler.check_error();
					}
				}));
			}
			for thread in threads {
				thread.join().unwrap();
			}
			let stopped =
				handler.get_error().is_some_and(|e| e.severity >= ErrorSeverity::HardError);
			assert_eq!(
				handler.is_db_stopped(),
				stopped,
				"round {round}: the flag and the errors differ"
			);
			assert_eq!(handler.check_error().is_err(), stopped, "round {round}");
		}
	}

	/// Every variant, so the checks below cover a variant added later: the exhaustive match in
	/// `position` stops compiling until the new one is listed here.
	fn every_encryption_error() -> Vec<EncryptionError> {
		vec![
			EncryptionError::NotConfigured,
			EncryptionError::PlaintextInEncryptedDb,
			EncryptionError::UnknownKeyId,
			EncryptionError::KeyRetired,
			EncryptionError::WrongKey,
			EncryptionError::AuthenticationFailed,
			EncryptionError::UnsupportedSuite,
			EncryptionError::UnsupportedVersion,
			EncryptionError::MalformedHeader,
			EncryptionError::KeyProvider(KeyProviderFailure::Unavailable),
			EncryptionError::KeyProvider(KeyProviderFailure::Failed),
			EncryptionError::KeyProvider(KeyProviderFailure::RandomSource),
			EncryptionError::DbIdMismatch,
			EncryptionError::FileIdMismatch,
			EncryptionError::PositionReuse,
			EncryptionError::InvalidInput("a block kind that does not belong to the file kind"),
			EncryptionError::NotEncrypted,
			EncryptionError::SuiteMismatch,
		]
	}

	fn position(err: &EncryptionError) -> usize {
		match err {
			EncryptionError::NotConfigured => 0,
			EncryptionError::PlaintextInEncryptedDb => 1,
			EncryptionError::UnknownKeyId => 2,
			EncryptionError::KeyRetired => 3,
			EncryptionError::WrongKey => 4,
			EncryptionError::AuthenticationFailed => 5,
			EncryptionError::UnsupportedSuite => 6,
			EncryptionError::UnsupportedVersion => 7,
			EncryptionError::MalformedHeader => 8,
			EncryptionError::KeyProvider(_) => 9,
			EncryptionError::DbIdMismatch => 10,
			EncryptionError::FileIdMismatch => 11,
			EncryptionError::PositionReuse => 12,
			EncryptionError::InvalidInput(_) => 13,
			EncryptionError::NotEncrypted => 14,
			EncryptionError::SuiteMismatch => 15,
		}
	}

	#[test]
	fn every_encryption_error_variant_is_listed_once_per_shape() {
		let mut seen: Vec<usize> = every_encryption_error().iter().map(position).collect();
		seen.dedup();
		assert_eq!(seen, (0..=15).collect::<Vec<_>>());
	}

	#[test]
	fn encryption_errors_convert_into_the_crate_error_and_keep_their_identity() {
		for err in every_encryption_error() {
			let converted: Error = err.clone().into();
			match &converted {
				Error::Encryption(inner) => assert_eq!(inner, &err),
				other => panic!("{err:?} converted to {other:?}"),
			}
			// The crate error is cloned across background tasks, so the payload must survive it.
			match converted.clone() {
				Error::Encryption(inner) => assert_eq!(inner, err),
				other => panic!("{err:?} cloned to {other:?}"),
			}
			assert!(converted.to_string().ends_with(&err.to_string()), "{converted}");
			assert!(converted.to_string().starts_with("Encryption error: "), "{converted}");
		}
	}

	#[test]
	fn encryption_error_texts_name_no_key_id_path_or_secret() {
		for err in every_encryption_error() {
			let shown = err.to_string();
			assert!(!shown.is_empty());
			// A key id, a file id or a byte of key material would show up as a digit, and an
			// absolute path as a separator.
			assert!(!shown.chars().any(|c| c.is_ascii_digit()), "digit in {shown:?}");
			assert!(!shown.contains('/') && !shown.contains('\\'), "path in {shown:?}");
			// Debug must not carry more than the variant either.
			let debugged = format!("{err:?}");
			assert!(!debugged.chars().any(|c| c.is_ascii_digit()), "digit in {debugged:?}");
		}
	}

	#[test]
	fn only_an_unavailable_key_provider_is_worth_retrying() {
		for err in every_encryption_error() {
			let expected = err == EncryptionError::KeyProvider(KeyProviderFailure::Unavailable);
			assert_eq!(err.is_retryable(), expected, "{err:?}");
		}
	}

	#[test]
	fn encryption_errors_are_a_plain_std_error() {
		fn assert_error<E: std::error::Error + Clone + Send + Sync + 'static>() {}
		assert_error::<EncryptionError>();
		assert_error::<Error>();
	}

	/// The class of every variant, written out: the table in the documentation of
	/// `classify_error`, as a test.
	fn expected_class(err: &EncryptionError) -> EncryptionClass {
		use EncryptionError as E;
		use KeyProviderFailure as K;
		match err {
			E::KeyProvider(K::Unavailable) => EncryptionClass::Retryable,
			E::WrongKey
			| E::UnknownKeyId
			| E::KeyRetired
			| E::NotConfigured
			| E::NotEncrypted
			| E::PlaintextInEncryptedDb
			| E::DbIdMismatch
			| E::SuiteMismatch
			| E::FileIdMismatch
			| E::UnsupportedSuite
			| E::UnsupportedVersion
			| E::MalformedHeader
			| E::KeyProvider(K::Failed) => EncryptionClass::Key,
			E::AuthenticationFailed | E::PositionReuse => EncryptionClass::Integrity,
			E::KeyProvider(K::RandomSource) | E::InvalidInput(_) => EncryptionClass::Fatal,
		}
	}

	#[test]
	fn every_encryption_error_is_in_the_class_the_table_says() {
		for err in every_encryption_error() {
			assert_eq!(err.class(), expected_class(&err), "{err:?}");
		}
	}

	#[test]
	fn only_the_retryable_class_is_retryable() {
		for err in every_encryption_error() {
			assert_eq!(err.is_retryable(), err.class() == EncryptionClass::Retryable, "{err:?}");
		}
	}

	#[test]
	fn an_encryption_error_has_the_severity_of_its_class_whatever_the_reason() {
		let reasons = [
			BackgroundErrorReason::MemtablaFlush,
			BackgroundErrorReason::Compaction,
			BackgroundErrorReason::ManifestWrite,
		];
		for err in every_encryption_error() {
			let expected = match expected_class(&err) {
				EncryptionClass::Retryable => ErrorSeverity::SoftError,
				EncryptionClass::Key => ErrorSeverity::HardError,
				EncryptionClass::Integrity => ErrorSeverity::Unrecoverable,
				EncryptionClass::Fatal => ErrorSeverity::FatalError,
			};
			for reason in reasons {
				let severity =
					BackgroundErrorHandler::classify_error(&Error::Encryption(err.clone()), reason);
				assert_eq!(severity, expected, "{err:?} {reason:?}");
			}
			// A group that failed after part of it was applied is not about keys, whatever failed.
			assert_eq!(
				BackgroundErrorHandler::classify_error(
					&Error::Encryption(err.clone()),
					BackgroundErrorReason::CommitGroup
				),
				ErrorSeverity::Unrecoverable,
				"{err:?}"
			);
		}
	}

	#[test]
	fn a_pool_at_its_limit_or_a_quota_is_soft_and_a_full_disk_is_as_it_was() {
		let pool_full = Error::Io(Arc::new(crate::vfs::pool_full(2048)));
		let quota = Error::Io(Arc::new(std::io::Error::new(
			std::io::ErrorKind::StorageFull,
			"QuotaExceededError",
		)));
		// ENOSPC of a native file system: an OS error code, so it keeps the rule it had.
		let disk = Error::Io(Arc::new(std::io::Error::from_raw_os_error(28)));
		for reason in [
			BackgroundErrorReason::MemtablaFlush,
			BackgroundErrorReason::Compaction,
			BackgroundErrorReason::ManifestWrite,
		] {
			for error in [&pool_full, &quota] {
				let severity = BackgroundErrorHandler::classify_error(error, reason);
				assert_eq!(severity, ErrorSeverity::SoftError, "{reason:?} {error:?}");
				assert!(error.is_storage_full());
			}
		}
		assert!(pool_full.is_pool_full() && !quota.is_pool_full());
		assert!(!disk.is_storage_full());
		assert_eq!(
			BackgroundErrorHandler::classify_error(&disk, BackgroundErrorReason::MemtablaFlush),
			ErrorSeverity::FatalError
		);
		assert_eq!(
			BackgroundErrorHandler::classify_error(&disk, BackgroundErrorReason::ManifestWrite),
			ErrorSeverity::FatalError
		);
		// A commit group stays unrecoverable whatever the error.
		for error in [&pool_full, &quota] {
			assert_eq!(
				BackgroundErrorHandler::classify_error(error, BackgroundErrorReason::CommitGroup),
				ErrorSeverity::Unrecoverable
			);
		}
		// A flush that met one is tried again.
		assert!(BackgroundErrorHandler::retries(
			&pool_full,
			ErrorSeverity::SoftError,
			BackgroundErrorReason::MemtablaFlush
		));
	}

	#[test]
	fn a_flush_is_tried_again_unless_the_error_says_that_trying_again_is_no_use() {
		let reason = BackgroundErrorReason::MemtablaFlush;
		for err in every_encryption_error() {
			let error = Error::Encryption(err.clone());
			let severity = BackgroundErrorHandler::classify_error(&error, reason);
			assert_eq!(
				BackgroundErrorHandler::retries(&error, severity, reason),
				err.is_retryable(),
				"{err:?}"
			);
		}
		// What is not an encryption error keeps the rule it had: below fatal, tried again.
		let other = Error::Other("flush".into());
		assert!(BackgroundErrorHandler::retries(&other, ErrorSeverity::HardError, reason));
		assert!(!BackgroundErrorHandler::retries(&other, ErrorSeverity::FatalError, reason));
		let io = Error::Io(Arc::new(std::io::Error::other("flush")));
		assert!(!BackgroundErrorHandler::retries(&io, ErrorSeverity::FatalError, reason));
		// A key error that can be cured is not tried again once something fatal is on record.
		let soft = Error::Encryption(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable));
		assert!(BackgroundErrorHandler::retries(&soft, ErrorSeverity::SoftError, reason));
		assert!(!BackgroundErrorHandler::retries(&soft, ErrorSeverity::FatalError, reason));
	}

	#[test]
	fn a_compaction_is_tried_again_only_for_a_key_provider_that_is_away() {
		let reason = BackgroundErrorReason::Compaction;
		for err in every_encryption_error() {
			let error = Error::Encryption(err.clone());
			let severity = BackgroundErrorHandler::classify_error(&error, reason);
			assert_eq!(
				BackgroundErrorHandler::retries(&error, severity, reason),
				err.is_retryable(),
				"{err:?}"
			);
		}
		// Any other error of a compaction is tried again while it is soft, and ends it when it is
		// not.
		for error in
			[Error::Other("merge".into()), Error::Io(Arc::new(std::io::Error::other("io")))]
		{
			assert!(BackgroundErrorHandler::retries(&error, ErrorSeverity::SoftError, reason));
			for severity in [ErrorSeverity::HardError, ErrorSeverity::FatalError] {
				assert!(!BackgroundErrorHandler::retries(&error, severity, reason), "{error:?}");
			}
		}
		// A hard error on record for the compaction is not lifted by a soft one that follows.
		let soft = Error::Encryption(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable));
		assert!(!BackgroundErrorHandler::retries(&soft, ErrorSeverity::HardError, reason));
		// The reasons that are not a task of their own are never tried again by the tasks.
		assert!(!BackgroundErrorHandler::retries(
			&soft,
			ErrorSeverity::SoftError,
			BackgroundErrorReason::ManifestWrite
		));
		assert!(!BackgroundErrorHandler::retries(
			&soft,
			ErrorSeverity::SoftError,
			BackgroundErrorReason::CommitGroup
		));
	}

	/// What each class does to the handler: a provider that is away does not stop writes, a key
	/// that does not fit does, and only the first of them ends when the work succeeds.
	#[test]
	fn a_provider_that_is_away_does_not_stop_writes_and_the_other_classes_do() {
		for err in every_encryption_error() {
			let handler = BackgroundErrorHandler::new();
			let error = Error::Encryption(err.clone());
			handler.set_error(error.clone(), BackgroundErrorReason::MemtablaFlush);
			match expected_class(&err) {
				EncryptionClass::Retryable => {
					assert!(!handler.is_db_stopped(), "{err:?}");
					assert!(handler.check_error().is_ok(), "{err:?}");
					assert!(handler.recover(BackgroundErrorReason::MemtablaFlush), "{err:?}");
				}
				EncryptionClass::Key => {
					assert!(handler.is_db_stopped(), "{err:?}");
					// The commits that are refused are told why, with the type of the error.
					assert!(
						matches!(handler.check_error(), Err(Error::Encryption(ref got)) if *got == err),
						"{err:?}"
					);
					// A later flush that works ends it, as it ends any hard error.
					assert!(handler.recover(BackgroundErrorReason::MemtablaFlush), "{err:?}");
					assert!(!handler.is_db_stopped(), "{err:?}");
				}
				EncryptionClass::Integrity | EncryptionClass::Fatal => {
					assert!(handler.is_db_stopped(), "{err:?}");
					assert!(!handler.recover(BackgroundErrorReason::MemtablaFlush), "{err:?}");
					assert!(
						matches!(handler.check_error(), Err(Error::Encryption(ref got)) if *got == err),
						"{err:?}"
					);
				}
			}
		}
	}

	#[test]
	fn an_authentication_failure_is_reported_as_a_class_of_its_own() {
		let handler = BackgroundErrorHandler::new();
		let severity = handler.set_error(
			Error::Encryption(EncryptionError::AuthenticationFailed),
			BackgroundErrorReason::Compaction,
		);
		assert_eq!(severity, ErrorSeverity::Unrecoverable);
		let recorded = handler.get_error().unwrap();
		assert_eq!(recorded.reason, BackgroundErrorReason::Compaction);
		assert!(matches!(recorded.error, Error::Encryption(EncryptionError::AuthenticationFailed)));

		// Not the severity of a wrong key, which is told apart from it.
		let wrong_key = BackgroundErrorHandler::classify_error(
			&Error::Encryption(EncryptionError::WrongKey),
			BackgroundErrorReason::Compaction,
		);
		assert_eq!(wrong_key, ErrorSeverity::HardError);
		assert_ne!(wrong_key, severity);
	}

	#[test]
	fn what_is_recorded_for_an_encryption_error_names_no_key_and_no_path() {
		for err in every_encryption_error() {
			let handler = BackgroundErrorHandler::new();
			handler.set_error(Error::Encryption(err.clone()), BackgroundErrorReason::Compaction);
			let recorded = handler.get_error().unwrap().error;
			for shown in [recorded.to_string(), format!("{recorded:?}")] {
				assert!(!shown.chars().any(|c| c.is_ascii_digit()), "digit in {shown:?}");
				assert!(!shown.contains('/') && !shown.contains('\\'), "path in {shown:?}");
			}
		}
	}
}
