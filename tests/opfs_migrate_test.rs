//! The move of a legacy IndexedDB database into a database of the origin private file system, in a
//! real headless Chrome and a dedicated worker. Run it, from the repository root, with
//!
//! ```text
//! export CC_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/clang
//! export AR_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/llvm-ar
//! WASM_BINDGEN_TEST_TIMEOUT=300 WASM_BINDGEN_USE_DEDICATED_WORKER=1 \
//!     wasm-pack test --headless --chrome --test opfs_migrate_test
//! ```
//!
//! Each test builds a database in the IndexedDB of the browser, in the layout of the legacy
//! `indxdb://` store (an object store `kv`, keys and values that are `Uint8Array`s, and some that
//! are `ArrayBuffer`s and strings), moves it with `migrate_from_indexeddb`, and looks at both
//! sides: the rows of the database in the origin private file system, and the IndexedDB database
//! with the API of the browser, which must be as it was (its version, its object stores and every
//! row of them). The logic of the move, and what a crash does to it, is tested over memory in
//! `src/test/indexeddb_migration_tests.rs`; what is shown here is that the real source and the real
//! files do what the stand-ins do. The global `indexedDB` of the worker is hidden for the tests of
//! a context that has none, and put back after each of them.

#![cfg(all(target_arch = "wasm32", target_os = "unknown"))]

mod opfs_common;

use opfs_common::{unique_name, watchdog};
use surrealkv::{
	delete_indexeddb,
	migrate_from_indexeddb,
	migrate_from_indexeddb_with_tuning,
	Error,
	FileSystem,
	ImportTuning,
	IndexedDbDeletion,
	IndexedDbError,
	IndexedDbMigration,
	LSMIterator,
	Mode,
	Options,
	TreeBuilder,
};
use surrealkv_compat_indxdb::wasm::{read_all_from_browser, IdbSource};
use surrealkv_compat_indxdb::ChunkLimits;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;
use web_sys::{IdbDatabase, IdbFactory, IdbOpenDbRequest, IdbRequest};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// The memtable of the tests: small, so that a copy fills it more than once.
const MEMTABLE: usize = 256 * 1024;

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

fn options() -> Options {
	Options::default()
		.with_path("db".into())
		.with_max_memtable_size(MEMTABLE)
		.with_enable_vlog(true)
		.with_vlog_value_threshold(256)
}

// ---------------------------------------------------------------------------------------------
// The example of the README, which this builds for a browser
// ---------------------------------------------------------------------------------------------

/// The code of the README. `tests/` is what compiles it, since a native build cannot and the
/// examples of the README are not built by the doctests, and a test of the library checks that the
/// README holds these lines.
#[allow(dead_code, reason = "compiled for the browser, and never called")]
mod the_example_of_the_readme {
	use surrealkv::{migrate_from_indexeddb, FileSystem, IndexedDbMigration, Options, TreeBuilder};

	async fn start() -> Result<(), Box<dyn std::error::Error>> {
		// The options that the application opens its database with, which the move makes it with.
		let options = Options::default().with_path("db".into());

		// Before the database is opened, at every start: when there is nothing to do, it only looks
		// at the files of the database.
		match migrate_from_indexeddb("my-indexeddb-name", "mydb", options.clone()).await? {
			IndexedDbMigration::Imported {
				entries,
				bytes,
				skipped,
				..
			} => {
				// The rows of the IndexedDB database are in the database now, but for the `skipped`
				// rows that the engine refuses (an empty key), which are still in IndexedDB.
				let _ = (entries, bytes, skipped);
			}
			IndexedDbMigration::Existing => {
				// The database was there already, and was left as it is.
			}
			IndexedDbMigration::NoSource => {
				// There was nothing to move: a new installation, or no IndexedDB in this context
				// (see below), which this result does not tell apart.
			}
			_ => {}
		}

		// Then the database is opened as always.
		let fs = FileSystem::opfs("mydb").await?;
		let tree =
			TreeBuilder::with_options(options.with_file_system(fs.clone())).build_async().await?;
		// ...
		tree.close().await?;
		fs.close().await?;
		Ok(())
	}
}

/// The example of the README that removes the legacy database, built the same way.
#[allow(dead_code, reason = "compiled for the browser, and never called")]
mod the_removal_example_of_the_readme {
	use std::time::Duration;

	use surrealkv::{delete_indexeddb, IndexedDbDeletion};

	async fn remove_the_legacy_database() -> Result<(), Box<dyn std::error::Error>> {
		// The most that the call waits for the browser to say that the database is deleted.
		match delete_indexeddb("my-indexeddb-name", Duration::from_secs(5)).await? {
			IndexedDbDeletion::Deleted => {
				// The database is gone.
			}
			IndexedDbDeletion::NotFound => {
				// There was no such database, and none was made.
			}
			IndexedDbDeletion::Blocked => {
				// A connection to it is still open. The browser has not cancelled the deletion,
				// and carries it out when the last connection is closed.
			}
			_ => {}
		}
		Ok(())
	}
}

// ---------------------------------------------------------------------------------------------
// IndexedDB, as the browser has it
// ---------------------------------------------------------------------------------------------

fn factory() -> IdbFactory {
	let scope: web_sys::WorkerGlobalScope = js_sys::global().unchecked_into();
	scope.indexed_db().unwrap().expect("a worker has indexedDB")
}

/// Resolves when `target` fires `success_event`, and fails the test when it fires `error` or
/// `abort`.
async fn wait_for(target: &web_sys::EventTarget, success_event: &str) {
	let promise = js_sys::Promise::new(&mut |resolve, reject| {
		let ok = Closure::once_into_js(move || {
			resolve.call0(&JsValue::NULL).unwrap();
		});
		let err = Closure::once_into_js(move || {
			reject.call1(&JsValue::NULL, &JsValue::from_str("IndexedDB error event")).unwrap();
		});
		target.add_event_listener_with_callback(success_event, ok.unchecked_ref()).unwrap();
		target.add_event_listener_with_callback("error", err.unchecked_ref()).unwrap();
		target.add_event_listener_with_callback("abort", err.unchecked_ref()).unwrap();
	});
	JsFuture::from(promise).await.expect("IndexedDB request failed");
}

/// A row of a legacy store: the key and the value as they are put in it, and the bytes that the
/// move must make of them.
struct Legacy {
	key: JsValue,
	value: JsValue,
	key_bytes: Vec<u8>,
	value_bytes: Vec<u8>,
}

impl Legacy {
	/// The way the legacy crate put a row in: a `Uint8Array` for the key, and for the value.
	fn bytes(key: &[u8], value: &[u8]) -> Self {
		Self {
			key: js_sys::Uint8Array::from(key).into(),
			value: js_sys::Uint8Array::from(value).into(),
			key_bytes: key.to_vec(),
			value_bytes: value.to_vec(),
		}
	}

	/// A value that is an `ArrayBuffer`.
	fn buffer(key: &[u8], value: &[u8]) -> Self {
		let buffer = js_sys::Uint8Array::from(value).buffer();
		Self {
			value: buffer.into(),
			..Self::bytes(key, value)
		}
	}

	/// A value that is a string, which is its UTF-8 bytes to the move.
	fn text(key: &[u8], value: &str) -> Self {
		Self {
			value: JsValue::from_str(value),
			value_bytes: value.as_bytes().to_vec(),
			..Self::bytes(key, b"")
		}
	}

	/// A key that is a string.
	fn text_key(key: &str, value: &[u8]) -> Self {
		Self {
			key: JsValue::from_str(key),
			key_bytes: key.as_bytes().to_vec(),
			..Self::bytes(b"", value)
		}
	}
}

/// What the move must make of `rows`, in the order of the keys.
fn expected(rows: &[Legacy]) -> Rows {
	let mut rows: Rows =
		rows.iter().map(|r| (r.key_bytes.clone(), r.value_bytes.clone())).collect();
	rows.sort();
	rows
}

fn weight(rows: &Rows) -> u64 {
	rows.iter().map(|(k, v)| (k.len() + v.len()) as u64).sum()
}

/// The result of a move that copied `rows` and skipped `skipped` more. It is checked by its numbers
/// and not built, since `Imported` can grow fields.
fn assert_imported(result: Result<IndexedDbMigration, Error>, rows: &Rows, skipped: u64) {
	match result {
		Ok(IndexedDbMigration::Imported {
			entries,
			bytes,
			skipped: skipped_rows,
			..
		}) => assert_eq!(
			(entries, bytes, skipped_rows),
			(rows.len() as u64, weight(rows), skipped),
			"the entries, the bytes and the skipped rows of the move"
		),
		other => panic!("the move did not copy: {other:?}"),
	}
}

/// Creates the database `name` at `version` with the object stores `stores` (keys that are not in
/// the values), and puts `rows` in `kv`.
async fn create_legacy(name: &str, version: u32, stores: &[&str], rows: &[Legacy]) {
	let open: IdbOpenDbRequest = factory().open_with_u32(name, version).unwrap();
	let stores: Vec<String> = stores.iter().map(|s| s.to_string()).collect();
	let upgrade = Closure::once_into_js(move |event: web_sys::Event| {
		let req: IdbRequest = event.target().unwrap().unchecked_into();
		let db: IdbDatabase = req.result().unwrap().unchecked_into();
		for store in &stores {
			db.create_object_store(store).unwrap();
		}
	});
	open.set_onupgradeneeded(Some(upgrade.unchecked_ref()));
	wait_for(open.as_ref(), "success").await;
	let db: IdbDatabase = open.result().unwrap().unchecked_into();
	if !rows.is_empty() {
		let tx =
			db.transaction_with_str_and_mode("kv", web_sys::IdbTransactionMode::Readwrite).unwrap();
		let store = tx.object_store("kv").unwrap();
		for row in rows {
			store.put_with_key(&row.value, &row.key).unwrap();
		}
		wait_for(tx.as_ref(), "complete").await;
	}
	db.close();
}

/// The version that `indexedDB.databases()` lists for `name`, `None` if it does not list it.
async fn listed_version(name: &str) -> Option<f64> {
	let factory = factory();
	let call: js_sys::Function =
		js_sys::Reflect::get(&factory, &"databases".into()).unwrap().dyn_into().unwrap();
	let promise: js_sys::Promise = call.call0(&factory).unwrap().dyn_into().unwrap();
	let list: js_sys::Array = JsFuture::from(promise).await.unwrap().dyn_into().unwrap();
	list.iter().find_map(|entry| {
		let listed = js_sys::Reflect::get(&entry, &"name".into()).unwrap();
		(listed.as_string().as_deref() == Some(name))
			.then(|| js_sys::Reflect::get(&entry, &"version".into()).unwrap().as_f64().unwrap())
	})
}

/// A value of a store in a form that tells its type and compares.
fn fingerprint(value: &JsValue) -> String {
	if let Some(bytes) = value.dyn_ref::<js_sys::Uint8Array>() {
		format!("u8:{:?}", bytes.to_vec())
	} else if let Some(buffer) = value.dyn_ref::<js_sys::ArrayBuffer>() {
		format!("ab:{:?}", js_sys::Uint8Array::new(buffer).to_vec())
	} else if let Some(text) = value.as_string() {
		format!("s:{text}")
	} else {
		format!("other:{value:?}")
	}
}

/// Everything about a database that the move must leave as it was: its version, its object stores
/// with their key paths, and every row of every store with the types of its key and its value.
#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
	version: u64,
	stores: Vec<(String, String)>,
	rows: Vec<(String, Vec<(String, String)>)>,
}

/// Reads `name` with the API of the browser. The database must be there: this opens it with no
/// version, which makes it otherwise.
async fn snapshot(name: &str) -> Snapshot {
	let open = factory().open(name).unwrap();
	wait_for(open.as_ref(), "success").await;
	let db: IdbDatabase = open.result().unwrap().unchecked_into();
	let version = db.version() as u64;
	let names = db.object_store_names();
	let mut stores = Vec::new();
	let mut rows = Vec::new();
	for i in 0..names.length() {
		let store_name = names.item(i).unwrap();
		let tx = db.transaction_with_str(&store_name).unwrap();
		let store = tx.object_store(&store_name).unwrap();
		stores.push((store_name.clone(), format!("{:?}", store.key_path().unwrap())));
		let keys = store.get_all_keys().unwrap();
		wait_for(keys.as_ref(), "success").await;
		let values = store.get_all().unwrap();
		wait_for(values.as_ref(), "success").await;
		let keys: js_sys::Array = keys.result().unwrap().unchecked_into();
		let values: js_sys::Array = values.result().unwrap().unchecked_into();
		let pairs = keys.iter().zip(values.iter()).map(|(k, v)| (fingerprint(&k), fingerprint(&v)));
		rows.push((store_name, pairs.collect()));
	}
	db.close();
	Snapshot {
		version,
		stores,
		rows,
	}
}

async fn delete_legacy(name: &str) {
	let req = factory().delete_database(name).unwrap();
	wait_for(req.as_ref(), "success").await;
}

// ---------------------------------------------------------------------------------------------
// The origin private file system
// ---------------------------------------------------------------------------------------------

/// Whether the directory of the database `name` is in the origin private file system. Asked
/// without making it.
async fn opfs_has(name: &str) -> bool {
	let root = opfs_common::root().await;
	let options = web_sys::FileSystemGetDirectoryOptions::new();
	options.set_create(false);
	JsFuture::from(root.get_directory_handle_with_options(name, &options)).await.is_ok()
}

/// Opens the database as the application does, and reads every row of it.
async fn database(opfs: &str) -> Rows {
	let fs = FileSystem::opfs(opfs).await.expect("mount the database");
	let tree = TreeBuilder::with_options(options().with_file_system(fs.clone()))
		.build_async()
		.await
		.expect("open the database");
	let mut rows = Vec::new();
	{
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		let mut iter = txn.iter().unwrap();
		let mut valid = iter.seek_first().unwrap();
		while valid {
			rows.push((iter.key().user_key().to_vec(), iter.value().unwrap()));
			valid = iter.next().unwrap();
		}
	}
	tree.close().await.unwrap();
	fs.close().await.unwrap();
	rows
}

/// Removes the database, and the legacy store, that a test made.
async fn clean_up(opfs: &str, legacy: &str) {
	FileSystem::destroy_opfs(opfs).await.expect("remove the database");
	delete_legacy(legacy).await;
}

fn assert_rows(found: &Rows, wanted: &Rows, what: &str) {
	assert_eq!(found.len(), wanted.len(), "{what}: the number of rows");
	for (at, (f, w)) in found.iter().zip(wanted).enumerate() {
		assert!(f.0 == w.0, "{what}: the key of row {at} differs");
		assert!(
			f.1 == w.1,
			"{what}: the value of row {at} differs ({} and {} bytes)",
			f.1.len(),
			w.1.len()
		);
	}
}

/// Rows of every kind that the legacy store holds: binary keys and values of every length from none
/// to above the threshold of the value log, `ArrayBuffer` values, string values and a string key.
fn legacy_rows(count: usize) -> Vec<Legacy> {
	let mut rows = Vec::new();
	for i in 0..count {
		let key = (i as u32).wrapping_mul(2_654_435_761).to_be_bytes();
		let len = [0usize, 1, 100, 255, 256, 257, 700, 3_000][i % 8];
		let value = opfs_common::pattern(i as u64 + 1, len);
		rows.push(match i % 11 {
			3 => Legacy::buffer(&key, &value),
			7 => Legacy::text(&key, &format!("text value {i}")),
			_ => Legacy::bytes(&key, &value),
		});
	}
	rows.push(Legacy::text_key("a key that is a string", b"its value"));
	rows.push(Legacy::bytes(b"large", &opfs_common::pattern(99, 400_000)));
	rows.push(Legacy::bytes(&[0, 255, 0, 255, 128], b""));
	rows
}

// ---------------------------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn a_legacy_store_is_imported_with_equal_contents_and_is_not_changed() {
	let legacy = unique_name("legacy", "idb");
	let opfs = unique_name("target", "db");
	let rows = legacy_rows(400);
	create_legacy(&legacy, 4, &["kv", "other"], &rows).await;
	let before = snapshot(&legacy).await;
	assert_eq!(before.version, 4);
	assert_eq!(before.rows[0].1.len(), rows.len());
	let wanted = expected(&rows);

	let result = watchdog("the move", migrate_from_indexeddb(&legacy, &opfs, options())).await;
	assert_imported(result, &wanted, 0);
	assert_rows(&database(&opfs).await, &wanted, "the database");

	// The legacy database is what it was: its version, its stores, and every row.
	assert_eq!(snapshot(&legacy).await, before);
	assert_eq!(listed_version(&legacy).await, Some(4.0));

	// The next call finds the database, and does not depend on the source: it is not looked at
	// when it is there, and not when it is gone.
	let again = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_eq!(again.unwrap(), IndexedDbMigration::Existing);
	delete_legacy(&legacy).await;
	let again = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_eq!(again.unwrap(), IndexedDbMigration::Existing);
	assert_eq!(listed_version(&legacy).await, None, "the call made the legacy database");
	assert_rows(&database(&opfs).await, &wanted, "the database after the next calls");
	clean_up(&opfs, &legacy).await;
}

#[wasm_bindgen_test]
async fn a_source_that_is_not_there_is_no_source_and_is_not_made() {
	let legacy = unique_name("missing", "idb");
	let opfs = unique_name("target", "db");

	let result = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_eq!(result.unwrap(), IndexedDbMigration::NoSource);
	assert_eq!(listed_version(&legacy).await, None, "the call made the legacy database");
	assert!(!opfs_has(&opfs).await, "the call made the database");

	// A legacy client that then creates it, at a version of its own, is the one that does.
	create_legacy(&legacy, 9, &["kv"], &[]).await;
	assert_eq!(listed_version(&legacy).await, Some(9.0));
	clean_up(&opfs, &legacy).await;
}

#[wasm_bindgen_test]
async fn a_database_without_the_store_kv_is_no_source_and_is_not_changed() {
	let legacy = unique_name("other-store", "idb");
	let opfs = unique_name("target", "db");
	create_legacy(&legacy, 3, &["something-else"], &[]).await;
	let before = snapshot(&legacy).await;

	let result = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_eq!(result.unwrap(), IndexedDbMigration::NoSource);
	assert_eq!(snapshot(&legacy).await, before);
	assert!(!opfs_has(&opfs).await, "the call made the database");
	clean_up(&opfs, &legacy).await;
}

#[wasm_bindgen_test]
async fn a_store_with_no_row_is_an_empty_database() {
	let legacy = unique_name("empty", "idb");
	let opfs = unique_name("target", "db");
	create_legacy(&legacy, 1, &["kv"], &[]).await;
	let before = snapshot(&legacy).await;

	let result = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_imported(result, &Rows::new(), 0);
	assert!(database(&opfs).await.is_empty());
	assert_eq!(snapshot(&legacy).await, before);
	let again = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_eq!(again.unwrap(), IndexedDbMigration::Existing);
	clean_up(&opfs, &legacy).await;
}

#[wasm_bindgen_test]
async fn a_large_store_is_read_in_bounded_chunks_and_every_row_is_there() {
	let legacy = unique_name("large", "idb");
	let opfs = unique_name("target", "db");
	let rows: Vec<Legacy> = (0..50_000u32)
		.map(|i| {
			Legacy::bytes(&i.to_be_bytes(), &opfs_common::pattern(i as u64, (i % 40) as usize))
		})
		.collect();
	create_legacy(&legacy, 1, &["kv"], &rows).await;
	let wanted = expected(&rows);

	let tuning = ImportTuning {
		read: ChunkLimits::new(512 * 1024, 4_000),
		..ImportTuning::default()
	};
	let (result, report) =
		migrate_from_indexeddb_with_tuning(&legacy, &opfs, options(), tuning).await;
	assert_imported(result, &wanted, 0);
	// Fifty thousand rows at most four thousand to a chunk.
	assert!(report.chunks >= 13, "{report:?}");
	assert!(report.transactions >= report.chunks, "{report:?}");
	assert_rows(&database(&opfs).await, &wanted, "fifty thousand rows");
	clean_up(&opfs, &legacy).await;
}

#[wasm_bindgen_test]
async fn a_copy_that_is_abandoned_half_way_is_made_again_and_completes() {
	let legacy = unique_name("abandoned", "idb");
	let opfs = unique_name("target", "db");
	let rows = legacy_rows(2_000);
	create_legacy(&legacy, 1, &["kv"], &rows).await;
	let wanted = expected(&rows);

	// A worker that dies after two chunks: the tree and the files are let go, with nothing
	// flushed, and the lock is released.
	let tuning = ImportTuning {
		read: ChunkLimits::new(256 * 1024, 300),
		abandon_after_chunks: Some(2),
		..ImportTuning::default()
	};
	let (result, report) =
		migrate_from_indexeddb_with_tuning(&legacy, &opfs, options(), tuning).await;
	assert!(matches!(result, Err(Error::MigrationFailed(_))), "{result:?}");
	assert_eq!(report.chunks, 2);
	{
		// What is there is the marker and part of a database, and the next call knows it.
		let fs = FileSystem::opfs(&opfs).await.expect("the lock was released");
		let names = fs.list_for_test("");
		assert!(names.iter().any(|n| n.starts_with("IMPORTING")), "{names:?}");
		assert!(!fs.list_for_test("db").is_empty(), "no file of the database is there");
		fs.close().await.unwrap();
	}

	let result =
		watchdog("the second call", migrate_from_indexeddb(&legacy, &opfs, options())).await;
	assert_imported(result, &wanted, 0);
	assert_rows(&database(&opfs).await, &wanted, "the second call");
	let again = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_eq!(again.unwrap(), IndexedDbMigration::Existing);
	clean_up(&opfs, &legacy).await;
}

#[wasm_bindgen_test]
async fn a_second_mount_while_the_files_are_open_fails_with_the_typed_lock_error() {
	let legacy = unique_name("locked", "idb");
	let opfs = unique_name("target", "db");
	let rows = legacy_rows(50);
	create_legacy(&legacy, 1, &["kv"], &rows).await;
	let wanted = expected(&rows);
	let before = snapshot(&legacy).await;

	// Another page, or this one, has the database open.
	let holder = FileSystem::opfs(&opfs).await.expect("the first mount");
	let result =
		watchdog("the locked call", migrate_from_indexeddb(&legacy, &opfs, options())).await;
	let error = result.expect_err("the files are held");
	assert!(error.is_database_locked(), "{error:?}");
	assert_eq!(snapshot(&legacy).await, before);

	// Once it lets go, the copy is made.
	holder.close().await.unwrap();
	let result = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_imported(result, &wanted, 0);
	assert_rows(&database(&opfs).await, &wanted, "after the lock was let go");
	clean_up(&opfs, &legacy).await;
}

#[wasm_bindgen_test]
async fn a_database_that_exists_is_not_imported_over() {
	let legacy = unique_name("existing", "idb");
	let opfs = unique_name("target", "db");
	let rows = legacy_rows(30);
	create_legacy(&legacy, 1, &["kv"], &rows).await;
	let before = snapshot(&legacy).await;

	// The application opened its database before it moved anything, and wrote to it.
	let mine: Rows = vec![(b"mine".to_vec(), b"written by the application".to_vec())];
	{
		let fs = FileSystem::opfs(&opfs).await.unwrap();
		let tree = TreeBuilder::with_options(options().with_file_system(fs.clone()))
			.build_async()
			.await
			.unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set(&mine[0].0[..], &mine[0].1[..]).unwrap();
		txn.commit().await.unwrap();
		tree.close().await.unwrap();
		fs.close().await.unwrap();
	}

	let result = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_eq!(result.unwrap(), IndexedDbMigration::Existing);
	assert_rows(&database(&opfs).await, &mine, "the database of the application");
	assert_eq!(snapshot(&legacy).await, before);
	clean_up(&opfs, &legacy).await;
}

#[wasm_bindgen_test]
async fn a_key_that_is_not_bytes_fails_the_copy_and_the_next_call_starts_again() {
	let legacy = unique_name("number-key", "idb");
	let opfs = unique_name("target", "db");
	let mut rows = legacy_rows(20);
	rows.push(Legacy {
		key: JsValue::from_f64(7.0),
		value: js_sys::Uint8Array::from(&b"v"[..]).into(),
		key_bytes: Vec::new(),
		value_bytes: Vec::new(),
	});
	create_legacy(&legacy, 1, &["kv"], &rows).await;
	let before = snapshot(&legacy).await;

	let result = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	let Err(Error::MigrationFailed(message)) = result else {
		panic!("a number is not a key of bytes: {result:?}");
	};
	assert!(message.contains("key"), "{message}");
	assert_eq!(snapshot(&legacy).await, before);

	// The files that the copy made are removed, and the marker is kept. The engine does not open
	// what is there: it is not a database, and what an application wrote to it would be lost.
	{
		let fs = FileSystem::opfs(&opfs).await.expect("the lock was released");
		let names = fs.list_for_test("");
		assert!(names.iter().any(|n| n.starts_with("IMPORTING")), "{names:?}");
		assert!(fs.list_for_test("db").is_empty(), "files of the failed copy are left");
		let refused =
			TreeBuilder::with_options(options().with_file_system(fs.clone())).build_async().await;
		match refused {
			Err(Error::MigrationFailed(message)) => {
				assert!(message.contains("not finished"), "{message}");
			}
			Err(error) => panic!("the open must be refused for the copy: {error:?}"),
			Ok(_) => panic!("the engine opened what is part of a copy"),
		}
		fs.close().await.unwrap();
	}

	// The rows that can be copied are the source now: the call that failed left a marker, and the
	// database is made again.
	let mut again = legacy_rows(20);
	again.truncate(20);
	delete_legacy(&legacy).await;
	create_legacy(&legacy, 1, &["kv"], &again).await;
	let wanted = expected(&again);
	let result = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_imported(result, &wanted, 0);
	assert_rows(&database(&opfs).await, &wanted, "after the source was mended");
	clean_up(&opfs, &legacy).await;
}

// ---------------------------------------------------------------------------------------------
// Rows that the engine refuses
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn rows_with_an_empty_key_are_skipped_and_counted_and_the_source_is_not_changed() {
	// IndexedDB has two keys of no bytes: the empty string and an empty binary key. They are the
	// first two rows of the store, in that order.
	let rows = || {
		let mut rows = legacy_rows(60);
		rows.push(Legacy::bytes(b"", b"the value of the key of no bytes"));
		rows.push(Legacy::text_key("", b"the value of the key that is an empty string"));
		rows
	};
	let legacy = unique_name("empty-keys", "idb");
	create_legacy(&legacy, 2, &["kv"], &rows()).await;
	let before = snapshot(&legacy).await;
	assert_eq!(before.rows[0].1.len(), rows().len());
	let wanted = expected(&legacy_rows(60));

	// The default pieces, and chunks of two rows, the first of which holds the two rows that the
	// engine refuses and nothing else.
	for (what, read) in [
		("the default chunks", ChunkLimits::default()),
		("chunks of two rows", ChunkLimits::new(usize::MAX, 2)),
	] {
		let opfs = unique_name("target", "db");
		let tuning = ImportTuning {
			read,
			..ImportTuning::default()
		};
		let (result, _) =
			migrate_from_indexeddb_with_tuning(&legacy, &opfs, options(), tuning).await;
		assert_imported(result, &wanted, 2);
		assert_rows(&database(&opfs).await, &wanted, what);
		// The rows that were skipped are still where they were, and so is the rest.
		assert_eq!(snapshot(&legacy).await, before, "{what}");
		let again = migrate_from_indexeddb(&legacy, &opfs, options()).await;
		assert_eq!(again.unwrap(), IndexedDbMigration::Existing, "{what}");
		FileSystem::destroy_opfs(&opfs).await.expect("remove the database");
	}
	delete_legacy(&legacy).await;
}

#[wasm_bindgen_test]
async fn a_source_of_rows_with_an_empty_key_only_is_an_empty_database() {
	let legacy = unique_name("only-empty-keys", "idb");
	let opfs = unique_name("target", "db");
	let rows = vec![Legacy::bytes(b"", b"a"), Legacy::text_key("", b"b")];
	create_legacy(&legacy, 1, &["kv"], &rows).await;
	let before = snapshot(&legacy).await;

	let result = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_imported(result, &Rows::new(), 2);
	assert!(database(&opfs).await.is_empty());
	assert_eq!(snapshot(&legacy).await, before);
	clean_up(&opfs, &legacy).await;
}

#[wasm_bindgen_test]
async fn two_keys_that_are_the_same_bytes_fail_the_copy_and_the_source_is_not_changed() {
	let legacy = unique_name("same-bytes", "idb");
	let opfs = unique_name("target", "db");
	let mut rows = legacy_rows(20);
	// A string key and a binary key that are the same bytes: two rows of IndexedDB, one row of the
	// database. The copy fails and does not drop one of them.
	rows.push(Legacy::text_key("same bytes", b"from the string key"));
	rows.push(Legacy::bytes(b"same bytes", b"from the binary key"));
	create_legacy(&legacy, 1, &["kv"], &rows).await;
	let before = snapshot(&legacy).await;

	let result = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	let Err(Error::MigrationFailed(message)) = result else {
		panic!("two rows of one key must fail the copy: {result:?}");
	};
	assert!(message.contains("rows were written") && message.contains("same key"), "{message}");
	assert_eq!(snapshot(&legacy).await, before);
	{
		// What is left is the marker and no file of a database.
		let fs = FileSystem::opfs(&opfs).await.expect("the lock was released");
		let names = fs.list_for_test("");
		assert!(names.iter().any(|n| n.starts_with("IMPORTING")), "{names:?}");
		assert!(fs.list_for_test("db").is_empty(), "files of the failed copy are left");
		fs.close().await.unwrap();
	}
	clean_up(&opfs, &legacy).await;
}

// ---------------------------------------------------------------------------------------------
// A store that the legacy backend did not make
// ---------------------------------------------------------------------------------------------

/// Creates the database `name` with an object store `kv` that the legacy backend did not make:
/// `parameters` is what the browser is given to make it with (a key path, or automatic keys).
async fn create_odd_store(name: &str, parameters: &[(&str, JsValue)]) {
	let open: IdbOpenDbRequest = factory().open_with_u32(name, 1).unwrap();
	let options = js_sys::Object::new();
	for (key, value) in parameters {
		js_sys::Reflect::set(&options, &(*key).into(), value).unwrap();
	}
	let upgrade = Closure::once_into_js(move |event: web_sys::Event| {
		let req: IdbRequest = event.target().unwrap().unchecked_into();
		let db: IdbDatabase = req.result().unwrap().unchecked_into();
		let create: js_sys::Function =
			js_sys::Reflect::get(&db, &"createObjectStore".into()).unwrap().unchecked_into();
		create.call2(&db, &"kv".into(), &options).unwrap();
	});
	open.set_onupgradeneeded(Some(upgrade.unchecked_ref()));
	wait_for(open.as_ref(), "success").await;
	let db: IdbDatabase = open.result().unwrap().unchecked_into();
	db.close();
}

#[wasm_bindgen_test]
async fn a_store_that_the_legacy_backend_did_not_make_fails_the_call_and_is_not_no_indexeddb() {
	// The README refuses a store `kv` with a key path or with automatic keys. That is an error of
	// reading a database that is there, and not "there is no IndexedDB here": taken for that, the
	// call would be `NoSource`, the application would start with a new database, and the next start
	// would find it and never move the data.
	for (what, parameters, named) in [
		("a key path", vec![("keyPath", JsValue::from_str("id"))], "key path"),
		("automatic keys", vec![("autoIncrement", JsValue::TRUE)], "autoIncrement"),
	] {
		let legacy = unique_name("odd-store", "idb");
		let opfs = unique_name("target", "db");
		create_odd_store(&legacy, &parameters).await;
		let before = snapshot(&legacy).await;

		// Every call, not the first only: nothing was made that the next call could take for a
		// database.
		for call in ["first", "second"] {
			let result = migrate_from_indexeddb(&legacy, &opfs, options()).await;
			let Err(Error::MigrationFailed(message)) = result else {
				panic!("{what}, the {call} call: a store that is not the legacy one must fail: {result:?}");
			};
			assert!(message.contains(named), "{what}, the {call} call: {message}");
			assert!(!opfs_has(&opfs).await, "{what}, the {call} call: the call made the database");
			assert_eq!(snapshot(&legacy).await, before, "{what}, the {call} call");
		}
		delete_legacy(&legacy).await;
	}
}

// ---------------------------------------------------------------------------------------------
// A context that has no IndexedDB
// ---------------------------------------------------------------------------------------------

/// What `indexedDB` of the global scope of the worker is made to be for as long as this is held:
/// the scope is put back as it was when this is dropped, as it is when a test fails, so that one
/// test cannot hide IndexedDB from the next. The property is looked at as a script looks at it, so
/// that what a test sees through this is what the code under test sees.
struct HiddenIndexedDb {
	/// The own property that the scope had before, `undefined` if it had none (it is then
	/// inherited, and the property that was made is removed to bring that back).
	own: JsValue,
}

impl HiddenIndexedDb {
	/// Reading `indexedDB` gives `undefined`.
	fn as_undefined() -> Self {
		let descriptor = js_sys::Object::new();
		js_sys::Reflect::set(&descriptor, &"value".into(), &JsValue::UNDEFINED).unwrap();
		let hidden = Self::put(&descriptor);
		let read = js_sys::Reflect::get(&js_sys::global(), &"indexedDB".into());
		assert!(read.is_ok_and(|value| value.is_undefined()), "indexedDB is still there");
		hidden
	}

	/// Reading `indexedDB` throws a `SecurityError`, as it does where the browser blocks storage.
	fn as_throwing() -> Self {
		let descriptor = js_sys::Object::new();
		let getter = js_sys::Function::new_no_args(
			"throw new DOMException('The operation is insecure.', 'SecurityError');",
		);
		js_sys::Reflect::set(&descriptor, &"get".into(), &getter).unwrap();
		let hidden = Self::put(&descriptor);
		let read = js_sys::Reflect::get(&js_sys::global(), &"indexedDB".into());
		assert!(read.is_err(), "reading indexedDB does not throw");
		hidden
	}

	fn put(descriptor: &js_sys::Object) -> Self {
		let (global, key) = (js_sys::global(), JsValue::from_str("indexedDB"));
		let own = js_sys::Object::get_own_property_descriptor(&global, &key);
		js_sys::Reflect::set(descriptor, &"configurable".into(), &JsValue::TRUE).unwrap();
		let defined = js_sys::Reflect::define_property(&global, &key, descriptor)
			.expect("the scope lets indexedDB be defined again");
		assert!(defined, "the scope does not let indexedDB be defined again");
		Self {
			own,
		}
	}
}

impl Drop for HiddenIndexedDb {
	fn drop(&mut self) {
		let (global, key) = (js_sys::global(), JsValue::from_str("indexedDB"));
		if self.own.is_undefined() {
			let _ = js_sys::Reflect::delete_property(&global, &key);
		} else {
			let _ = js_sys::Reflect::define_property(&global, &key, self.own.unchecked_ref());
		}
	}
}

/// A way of hiding `indexedDB`.
type Hide = fn() -> HiddenIndexedDb;

/// How `indexedDB` is hidden, for the tests below, and what it is called in their messages.
fn hidings() -> [(&'static str, Hide); 2] {
	[
		("indexedDB is undefined", HiddenIndexedDb::as_undefined),
		("reading indexedDB throws", HiddenIndexedDb::as_throwing),
	]
}

#[wasm_bindgen_test]
async fn a_worker_without_indexeddb_starts_with_a_new_database_and_makes_nothing() {
	let legacy = unique_name("legacy", "idb");
	let opfs = unique_name("target", "db");
	// A legacy database that is there, so that "no source" is not the answer for a name that is
	// not: it cannot be read in a worker that has no IndexedDB, and it must be read when it can.
	let rows = legacy_rows(40);
	create_legacy(&legacy, 2, &["kv"], &rows).await;
	let before = snapshot(&legacy).await;
	let wanted = expected(&rows);

	for (what, hide) in hidings() {
		// IndexedDB is back before anything is checked, whatever the checks find.
		let (first, second) = {
			let _hidden = hide();
			let first =
				watchdog("the call", migrate_from_indexeddb(&legacy, &opfs, options())).await;
			let second = migrate_from_indexeddb(&legacy, &opfs, options()).await;
			(first, second)
		};
		assert_eq!(first.unwrap(), IndexedDbMigration::NoSource, "{what}");
		assert_eq!(second.unwrap(), IndexedDbMigration::NoSource, "{what}: the call after");
		assert!(!opfs_has(&opfs).await, "{what}: the call made the database");
		assert_eq!(snapshot(&legacy).await, before, "{what}: the legacy database was changed");
	}

	// The browser lets the worker read it, and nothing was skipped for good: the next call moves
	// it.
	let result = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_imported(result, &wanted, 0);
	assert_rows(&database(&opfs).await, &wanted, "after IndexedDB was back");
	clean_up(&opfs, &legacy).await;
}

#[wasm_bindgen_test]
async fn a_database_that_exists_is_found_by_a_worker_without_indexeddb() {
	let legacy = unique_name("legacy", "idb");
	let opfs = unique_name("target", "db");
	let rows = legacy_rows(25);
	create_legacy(&legacy, 1, &["kv"], &rows).await;
	let wanted = expected(&rows);
	let result = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_imported(result, &wanted, 0);

	for (what, hide) in hidings() {
		let result = {
			let _hidden = hide();
			migrate_from_indexeddb(&legacy, &opfs, options()).await
		};
		assert_eq!(result.unwrap(), IndexedDbMigration::Existing, "{what}");
	}
	assert_rows(&database(&opfs).await, &wanted, "the database after the calls");
	clean_up(&opfs, &legacy).await;
}

#[wasm_bindgen_test]
async fn an_unfinished_copy_in_a_worker_without_indexeddb_fails_and_makes_no_database() {
	let legacy = unique_name("legacy", "idb");
	let opfs = unique_name("target", "db");
	let rows = legacy_rows(2_000);
	create_legacy(&legacy, 1, &["kv"], &rows).await;
	let wanted = expected(&rows);

	// A worker that dies after two chunks leaves the marker and part of a database.
	let tuning = ImportTuning {
		read: ChunkLimits::new(256 * 1024, 300),
		abandon_after_chunks: Some(2),
		..ImportTuning::default()
	};
	let (result, _) = migrate_from_indexeddb_with_tuning(&legacy, &opfs, options(), tuning).await;
	assert!(matches!(result, Err(Error::MigrationFailed(_))), "{result:?}");

	for (what, hide) in hidings() {
		let result = {
			let _hidden = hide();
			migrate_from_indexeddb(&legacy, &opfs, options()).await
		};
		let Err(Error::MigrationFailed(message)) = result else {
			panic!("{what}: a copy that cannot be made again must fail: {result:?}");
		};
		assert!(
			message.contains("cannot be made again") && message.contains("not available"),
			"{what}: {message}"
		);
		// The partial files are removed, the marker stays, and what is there is not a database.
		let fs = FileSystem::opfs(&opfs).await.expect("the lock was released");
		let names = fs.list_for_test("");
		assert!(names.iter().any(|n| n.starts_with("IMPORTING")), "{what}: {names:?}");
		assert!(fs.list_for_test("db").is_empty(), "{what}: files of the copy are left");
		let refused =
			TreeBuilder::with_options(options().with_file_system(fs.clone())).build_async().await;
		assert!(matches!(refused, Err(Error::MigrationFailed(_))), "{what}: the engine opened it");
		fs.close().await.unwrap();
	}

	// IndexedDB is back, and the copy is made.
	let result = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_imported(result, &wanted, 0);
	assert_rows(&database(&opfs).await, &wanted, "after IndexedDB was back");
	clean_up(&opfs, &legacy).await;
}

#[wasm_bindgen_test]
async fn the_readers_and_the_deletion_say_unavailable_where_there_is_no_indexeddb() {
	let name = unique_name("nothing", "idb");
	for (what, hide) in hidings() {
		let _hidden = hide();
		let source = IdbSource::new(&name);
		assert!(!surrealkv_compat_indxdb::wasm::is_indexeddb_available(), "{what}");
		assert!(matches!(source.exists().await, Err(IndexedDbError::Unavailable(_))), "{what}");
		assert!(
			matches!(
				source.read_chunk(None, &ChunkLimits::default()).await,
				Err(IndexedDbError::Unavailable(_))
			),
			"{what}"
		);
		assert!(
			matches!(read_all_from_browser(&name).await, Err(IndexedDbError::Unavailable(_))),
			"{what}"
		);
		let deletion = delete_indexeddb(&name, std::time::Duration::from_millis(100)).await;
		assert!(matches!(deletion, Err(IndexedDbError::Unavailable(_))), "{what}: {deletion:?}");
	}
	// With IndexedDB there, a name that is not there is a name that is not there.
	assert!(surrealkv_compat_indxdb::wasm::is_indexeddb_available());
	assert!(!IdbSource::new(&name).exists().await.unwrap());
	assert_eq!(listed_version(&name).await, None);
}

// ---------------------------------------------------------------------------------------------
// The IndexedDB database after the move
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn the_move_leaves_the_source_and_the_helper_removes_it_when_the_application_asks() {
	let legacy = unique_name("legacy", "idb");
	let opfs = unique_name("target", "db");
	let rows = legacy_rows(30);
	create_legacy(&legacy, 6, &["kv", "other"], &rows).await;
	let before = snapshot(&legacy).await;
	let wanted = expected(&rows);

	// The move, and the calls after it, never delete the source.
	let result = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_imported(result, &wanted, 0);
	assert_eq!(listed_version(&legacy).await, Some(6.0), "the move deleted the source");
	assert_eq!(snapshot(&legacy).await, before);
	let again = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_eq!(again.unwrap(), IndexedDbMigration::Existing);
	assert_eq!(
		snapshot(&legacy).await,
		before,
		"a call that found the database changed the source"
	);

	// The application has checked its data, and deletes the source.
	assert_rows(&database(&opfs).await, &wanted, "the database that the application checked");
	let wait = std::time::Duration::from_secs(10);
	let deleted = watchdog("the deletion", delete_indexeddb(&legacy, wait)).await;
	assert_eq!(deleted.unwrap(), IndexedDbDeletion::Deleted);
	assert_eq!(listed_version(&legacy).await, None, "the database is still listed");

	// The database in the OPFS does not need it: the next call does not look for it.
	let after = migrate_from_indexeddb(&legacy, &opfs, options()).await;
	assert_eq!(after.unwrap(), IndexedDbMigration::Existing);
	assert_rows(&database(&opfs).await, &wanted, "the database after the source was deleted");
	assert_eq!(listed_version(&legacy).await, None, "a call made the legacy database");

	// A name that is not there is not there, and is not made by asking.
	let missing = delete_indexeddb(&legacy, wait).await;
	assert_eq!(missing.unwrap(), IndexedDbDeletion::NotFound);
	assert_eq!(listed_version(&legacy).await, None, "the deletion made the legacy database");
	FileSystem::destroy_opfs(&opfs).await.expect("remove the database");
}

#[wasm_bindgen_test]
async fn a_deletion_that_a_connection_holds_up_is_blocked_and_completes_when_it_closes() {
	let legacy = unique_name("legacy", "idb");
	create_legacy(&legacy, 3, &["kv"], &legacy_rows(5)).await;

	// A client of the legacy store that is open and does not close when it is asked to.
	let open = factory().open(&legacy).unwrap();
	wait_for(open.as_ref(), "success").await;
	let holder: IdbDatabase = open.result().unwrap().unchecked_into();

	let started = js_sys::Date::now();
	let outcome = delete_indexeddb(&legacy, std::time::Duration::from_millis(300)).await;
	assert_eq!(outcome.unwrap(), IndexedDbDeletion::Blocked);
	assert!(js_sys::Date::now() - started >= 250.0, "it did not wait for the time it was given");
	assert_eq!(listed_version(&legacy).await, Some(3.0), "the database is gone while it is held");

	// The request was not cancelled: the browser deletes the database when the connection is
	// closed.
	holder.close();
	let started = js_sys::Date::now();
	while listed_version(&legacy).await.is_some() {
		assert!(
			js_sys::Date::now() - started < 10_000.0,
			"the deletion did not complete after the connection was closed"
		);
		opfs_common::sleep_ms(25.0).await;
	}
}

// ---------------------------------------------------------------------------------------------
// A call that is dropped
// ---------------------------------------------------------------------------------------------

/// Polls `future` once, and again after each turn of the event loop, and drops it, wherever it is
/// waiting, after `polls` polls if it has not finished by then. With `settle` the page waits for
/// the browser to answer after the last poll and before the drop, so that the future is dropped
/// with an answer that nobody read. Whether it finished.
async fn poll_then_drop<T>(
	future: impl std::future::Future<Output = T>,
	polls: usize,
	settle: bool,
) -> bool {
	let mut future = Box::pin(future);
	let mut context = std::task::Context::from_waker(std::task::Waker::noop());
	for poll in 0..polls {
		if future.as_mut().poll(&mut context).is_ready() {
			return true;
		}
		if poll + 1 < polls {
			opfs_common::sleep_ms(0.0).await;
		}
	}
	if settle {
		opfs_common::sleep_ms(30.0).await;
	}
	drop(future);
	false
}

/// How long the deletion of a database that nothing holds open is waited for.
const HELD_UP_AFTER_MS: f64 = 3_000.0;

/// Whether the deletion of the IndexedDB database `name` is held up by a connection that something
/// left open: it is not answered within a few seconds. (The `blocked` event does not say so: the
/// browser fires it for a connection that is on its way to closed, and the deletion goes on.)
async fn deletion_is_held_up(name: &str) -> bool {
	let request = factory().delete_database(name).unwrap();
	let done = std::rc::Rc::new(std::cell::Cell::new(false));
	let on_success = {
		let done = std::rc::Rc::clone(&done);
		Closure::<dyn FnMut()>::new(move || done.set(true))
	};
	request.set_onsuccess(Some(on_success.as_ref().unchecked_ref()));
	let started = js_sys::Date::now();
	while !done.get() && js_sys::Date::now() - started < HELD_UP_AFTER_MS {
		opfs_common::sleep_ms(10.0).await;
	}
	request.set_onsuccess(None);
	!done.get()
}

#[wasm_bindgen_test]
async fn a_call_that_is_dropped_at_any_moment_holds_nothing_and_the_next_call_completes() {
	let rows = legacy_rows(40);
	let wanted = expected(&rows);
	let tuning = || ImportTuning {
		read: ChunkLimits::new(900, 7),
		write: ChunkLimits::new(700, 5),
		commit_pause: std::time::Duration::from_millis(1),
		abandon_after_chunks: None,
	};
	let (mut held, mut changed, mut next_failed, mut finished) =
		(Vec::new(), Vec::new(), Vec::new(), 0);
	// Every poll of the first few, which is where the requests are made, with the browser's answer
	// to the last one in before the drop and not, and then wider steps up to the end of the call.
	let mut points = (1usize..=12).collect::<Vec<_>>();
	while *points.last().unwrap() < 700 {
		points.push(points.last().unwrap() * 8 / 5 + 1);
	}
	'sweep: for polls in points {
		for settle in [false, true] {
			if settle && polls > 12 {
				continue;
			}
			let legacy = unique_name("legacy", "idb");
			let opfs = unique_name("target", "db");
			create_legacy(&legacy, 3, &["kv", "other"], &rows).await;
			let before = snapshot(&legacy).await;
			opfs_common::sleep_ms(50.0).await;

			let done = {
				let (legacy, opfs) = (legacy.clone(), opfs.clone());
				poll_then_drop(
					async move {
						let _ =
							migrate_from_indexeddb_with_tuning(&legacy, &opfs, options(), tuning())
								.await;
					},
					polls,
					settle,
				)
				.await
			};
			finished += usize::from(done);
			opfs_common::sleep_ms(100.0).await;

			// IndexedDB is as it was, and no connection of the dropped call is open.
			if snapshot(&legacy).await != before {
				changed.push((polls, settle));
			}
			if deletion_is_held_up(&legacy).await {
				held.push((polls, settle));
				break 'sweep;
			}
			create_legacy(&legacy, 3, &["kv", "other"], &rows).await;

			// The files are not held either, and the next call makes the database, or finds it.
			let next =
				watchdog("the next call", migrate_from_indexeddb(&legacy, &opfs, options())).await;
			match next {
				Ok(IndexedDbMigration::Imported {
					..
				})
				| Ok(IndexedDbMigration::Existing) => {
					if database(&opfs).await != wanted {
						next_failed.push((polls, settle, "the rows differ".to_string()));
					}
				}
				Ok(other) => next_failed.push((polls, settle, format!("{other:?}"))),
				Err(error) => next_failed.push((polls, settle, error.to_string())),
			}
			clean_up(&opfs, &legacy).await;
			if done {
				// Dropped after the end: later points are the same call.
				break 'sweep;
			}
		}
	}
	assert!(held.is_empty(), "a dropped call left a connection open at (polls, settle): {held:?}");
	assert!(changed.is_empty(), "a dropped call changed IndexedDB at (polls, settle): {changed:?}");
	assert!(next_failed.is_empty(), "the call after a dropped one failed: {next_failed:?}");
	assert!(finished > 0, "the calls never finished within the polls that were tried");
}
