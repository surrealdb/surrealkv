//! The typing of a pool that has no free file: the payload an `io::Error` carries, what each
//! conversion makes of it, and what the background error handler does with the typed error.
//!
//! A pool is refilled in the background, so a flush, a compaction or a manifest write that met an
//! empty one has not failed for good, and a database that was open must not stop for it. These
//! tests pin the type and the severity, which is all there is of it until a pool exists.

use std::io;

use crate::error::{BackgroundErrorHandler, BackgroundErrorReason, Error, ErrorSeverity};
use crate::storage::log_error;
use crate::vfs::{self, into_io_error};
use crate::wal::{self, IOError};

const REASONS: [BackgroundErrorReason; 3] = [
	BackgroundErrorReason::MemtablaFlush,
	BackgroundErrorReason::Compaction,
	BackgroundErrorReason::ManifestWrite,
];

#[test]
fn an_exhausted_pool_is_soft_for_a_flush_a_compaction_and_a_manifest_write() {
	for reason in REASONS {
		assert_eq!(
			BackgroundErrorHandler::classify_error(&Error::FilePoolExhausted, reason),
			ErrorSeverity::SoftError,
			"{reason:?}"
		);
	}
}

#[test]
fn the_commit_group_arm_still_wins_over_it() {
	// A group that failed after part of it was applied cannot be taken back, whatever the error.
	assert_eq!(
		BackgroundErrorHandler::classify_error(
			&Error::FilePoolExhausted,
			BackgroundErrorReason::CommitGroup
		),
		ErrorSeverity::Unrecoverable
	);
}

#[test]
fn a_plain_io_error_is_still_fatal_for_a_flush_and_a_manifest_write() {
	// What the arm must not change: only the typed error passes.
	let io = || Error::Io(std::sync::Arc::new(io::Error::other("disk")));
	assert_eq!(
		BackgroundErrorHandler::classify_error(&io(), BackgroundErrorReason::MemtablaFlush),
		ErrorSeverity::FatalError
	);
	assert_eq!(
		BackgroundErrorHandler::classify_error(&io(), BackgroundErrorReason::ManifestWrite),
		ErrorSeverity::FatalError
	);
	assert_eq!(
		BackgroundErrorHandler::classify_error(&io(), BackgroundErrorReason::Compaction),
		ErrorSeverity::SoftError
	);
}

#[test]
fn a_flush_and_a_compaction_try_again_after_it_and_a_manifest_write_is_not_retried_here() {
	for (reason, retries) in [
		(BackgroundErrorReason::MemtablaFlush, true),
		(BackgroundErrorReason::Compaction, true),
		(BackgroundErrorReason::ManifestWrite, false),
	] {
		let severity = BackgroundErrorHandler::classify_error(&Error::FilePoolExhausted, reason);
		assert_eq!(
			BackgroundErrorHandler::retries(&Error::FilePoolExhausted, severity, reason),
			retries,
			"{reason:?}"
		);
	}
}

#[test]
fn an_exhausted_pool_does_not_stop_the_database() {
	for reason in REASONS {
		let handler = BackgroundErrorHandler::new();
		let severity = handler.set_error(Error::FilePoolExhausted, reason);
		assert_eq!(severity, ErrorSeverity::SoftError);
		assert!(!handler.is_db_stopped(), "{reason:?}: writes go on");
		assert!(handler.check_error().is_ok(), "{reason:?}");
		// It is recorded, and the flush or compaction that works afterwards ends it.
		assert!(matches!(handler.get_error().map(|e| e.error), Some(Error::FilePoolExhausted)));
	}
}

#[test]
fn an_io_error_from_a_pool_becomes_the_typed_error() {
	assert!(matches!(Error::from(vfs::pool_exhausted()), Error::FilePoolExhausted));
	// Through `?` as well, which is how it reaches the engine.
	fn through_question_mark() -> crate::Result<()> {
		Err(vfs::pool_exhausted())?
	}
	assert!(matches!(through_question_mark(), Err(Error::FilePoolExhausted)));
}

#[test]
fn every_other_io_error_converts_as_it_did() {
	let source = io::Error::new(io::ErrorKind::NotFound, "no such file");
	match Error::from(source) {
		Error::Io(e) => {
			assert_eq!(e.kind(), io::ErrorKind::NotFound);
			assert_eq!(e.to_string(), "no such file");
		}
		other => panic!("{other:?}"),
	}
	// A payload of another type, and a plain kind, stay I/O errors.
	assert!(matches!(Error::from(io::Error::other("full")), Error::Io(_)));
	assert!(matches!(Error::from(io::Error::from(io::ErrorKind::StorageFull)), Error::Io(_)));
	assert!(!vfs::is_pool_exhausted(&io::Error::other(String::from("a pool"))));
}

#[test]
fn an_io_error_from_a_pool_becomes_the_typed_wal_error() {
	assert_eq!(wal::Error::from(vfs::pool_exhausted()), wal::Error::PoolExhausted);
}

#[test]
fn every_other_io_error_converts_to_a_wal_error_as_it_did() {
	let source = io::Error::new(io::ErrorKind::PermissionDenied, "read only");
	assert_eq!(
		wal::Error::from(source),
		wal::Error::IO(IOError::new(io::ErrorKind::PermissionDenied, "read only"))
	);
}

#[test]
fn the_typed_wal_error_keeps_its_type_in_the_engine_error() {
	assert!(matches!(Error::from(wal::Error::PoolExhausted), Error::FilePoolExhausted));
	// And the others as they were: text, except an encryption error.
	assert!(matches!(Error::from(wal::Error::SegmentClosed), Error::Wal(_)));
	assert!(matches!(
		Error::from(wal::Error::Encryption(crate::EncryptionError::WrongKey)),
		Error::Encryption(crate::EncryptionError::WrongKey)
	));
	// The whole way: an exhausted pool in a log write is typed in the error of the commit.
	let from_io: Error = wal::Error::from(vfs::pool_exhausted()).into();
	assert!(matches!(from_io, Error::FilePoolExhausted));
}

#[test]
fn the_error_of_a_log_append_keeps_the_type_too() {
	assert!(matches!(log_error(wal::Error::PoolExhausted), Error::FilePoolExhausted));
	assert!(matches!(log_error(wal::Error::SegmentClosed), Error::Other(_)));
	assert!(matches!(
		log_error(wal::Error::Encryption(crate::EncryptionError::WrongKey)),
		Error::Encryption(_)
	));
}

#[test]
fn the_messages_say_what_happened() {
	assert!(Error::FilePoolExhausted.to_string().contains("no free file"));
	assert!(wal::Error::PoolExhausted.to_string().contains("no free file"));
	assert!(vfs::pool_exhausted().to_string().contains("no free file"));
}

#[test]
fn an_engine_error_goes_back_to_io_without_losing_the_pool() {
	assert!(vfs::is_pool_exhausted(&into_io_error(Error::FilePoolExhausted)));
	assert!(vfs::is_pool_exhausted(&into_io_error(Error::from(vfs::pool_exhausted()))));
	let shared = std::sync::Arc::new(io::Error::new(io::ErrorKind::NotFound, "gone"));
	let kept = Error::Io(std::sync::Arc::clone(&shared));
	let back = into_io_error(kept);
	assert_eq!(back.kind(), io::ErrorKind::NotFound);
	assert_eq!(back.to_string(), "gone");
	// A shared one that is a pool's is still a pool's.
	let pool = std::sync::Arc::new(vfs::pool_exhausted());
	let _other = std::sync::Arc::clone(&pool);
	assert!(vfs::is_pool_exhausted(&into_io_error(Error::Io(pool))));
	let other = into_io_error(Error::KeyNotFound);
	assert_eq!(other.kind(), io::ErrorKind::Other);
}
