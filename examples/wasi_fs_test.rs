use surrealkv::TreeBuilder;

#[tokio::main(flavor = "current_thread")]
async fn main() {
	let temp_dir = "./test_wasi_lsm_db";
	let _ = std::fs::remove_dir_all(temp_dir);

	println!("1. Initializing SurrealKV on WASI with standard fs...");
	{
		let tree =
			TreeBuilder::new().with_path(temp_dir.into()).build().expect("Failed to build tree");

		let mut txn = tree.begin().expect("Failed to begin txn");
		txn.set(b"key1".to_vec(), b"value1".to_vec()).expect("Failed to set key1");
		txn.set(b"key2".to_vec(), b"value2".to_vec()).expect("Failed to set key2");
		txn.commit().await.expect("Failed to commit txn 1");

		let txn = tree.begin().expect("Failed to begin read txn");
		assert_eq!(txn.get(b"key1").expect("get key1").as_deref(), Some(b"value1".as_slice()));
		assert_eq!(txn.get(b"key2").expect("get key2").as_deref(), Some(b"value2".as_slice()));
		println!("  Verified writes in same session.");

		drop(tree);
	}

	println!("2. Reopening database to verify persistence across sessions...");
	{
		let tree =
			TreeBuilder::new().with_path(temp_dir.into()).build().expect("Failed to reopen tree");

		let txn = tree.begin().expect("Failed to begin read txn after reopen");
		assert_eq!(
			txn.get(b"key1").expect("get key1").as_deref(),
			Some(b"value1".as_slice()),
			"key1 should persist"
		);
		assert_eq!(
			txn.get(b"key2").expect("get key2").as_deref(),
			Some(b"value2".as_slice()),
			"key2 should persist"
		);

		// Perform update and delete in session 2
		let mut txn = tree.begin().expect("Failed to begin write txn");
		txn.set(b"key1".to_vec(), b"value1_updated".to_vec()).expect("update key1");
		txn.delete(b"key2".to_vec()).expect("delete key2");
		txn.commit().await.expect("commit updates");

		let txn = tree.begin().expect("Failed to begin read txn");
		assert_eq!(
			txn.get(b"key1").expect("get key1").as_deref(),
			Some(b"value1_updated".as_slice())
		);
		assert_eq!(txn.get(b"key2").expect("get key2"), None);
		println!("  Verified updates and deletes across sessions.");

		drop(tree);
	}

	let _ = std::fs::remove_dir_all(temp_dir);
	println!("WASI standard fs test passed successfully!");
}
