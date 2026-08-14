use bytes::Bytes;

use crate::database::KernelDatabase as Database;
use crate::{
	CommitTimestamp,
	CommitVersion,
	ErrorCode as DatabaseErrorCode,
	ReadSelector as BranchReadSelector,
	WriteOperation,
};

#[tokio::test]
async fn public_memory_database_commit_read_flush_and_conflict() {
	let database = Database::memory(4 * 1024 * 1024).await.unwrap();
	let receipt = database
		.commit(
			CommitVersion(0),
			CommitTimestamp(10),
			vec![WriteOperation::Put {
				key: Bytes::from_static(b"public"),
				value: Bytes::from_static(b"memory"),
				expires_at: None,
			}],
		)
		.await
		.unwrap();
	assert_eq!(receipt.version, CommitVersion(1));
	let conflict = database
		.commit(
			CommitVersion(0),
			CommitTimestamp(20),
			vec![WriteOperation::Delete {
				key: Bytes::from_static(b"public"),
			}],
		)
		.await
		.unwrap_err();
	assert_eq!(conflict.code, DatabaseErrorCode::Conflict);
	assert!(database.flush().await.unwrap());
	assert_eq!(
		database.get(b"public", BranchReadSelector::Latest).await.unwrap(),
		Some(Bytes::from_static(b"memory"))
	);
}

#[tokio::test]
async fn public_local_database_reopens_through_storage_binding() {
	let directory = tempfile::tempdir().unwrap();
	let database = Database::open_local(directory.path(), 4 * 1024 * 1024).await.unwrap();
	let identity = database.database_id();
	database
		.commit(
			CommitVersion(0),
			CommitTimestamp(10),
			vec![WriteOperation::Put {
				key: Bytes::from_static(b"public"),
				value: Bytes::from_static(b"local"),
				expires_at: None,
			}],
		)
		.await
		.unwrap();
	assert!(database.flush().await.unwrap());
	drop(database);

	let reopened = Database::open_local(directory.path(), 4 * 1024 * 1024).await.unwrap();
	assert_eq!(reopened.database_id(), identity);
	assert_eq!(
		reopened.get(b"public", BranchReadSelector::Latest).await.unwrap(),
		Some(Bytes::from_static(b"local"))
	);
}
