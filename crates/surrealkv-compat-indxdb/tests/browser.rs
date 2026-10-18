//! Browser tests of the IndexedDB reader, run in headless Chrome (`wasm-pack test --headless
//! --chrome crates/surrealkv-compat-indxdb`). Node has no IndexedDB, so they cannot run there.
//!
//! Each test creates a database of its own in the browser's IndexedDB, in the layout the legacy
//! `indxdb://` store of SurrealDB used (an object store named `kv` with out-of-line keys), reads
//! it back through the crate, and deletes it.
//!
//! What the reader must not do is tested as much as what it must: a name that is not there is not
//! created (`indexedDB.databases()` is asked afterwards, and a client that creates it at its own
//! version after the read is shown to be the first to), what is there is the same afterwards
//! (version, stores, rows), and no connection is left open (a database that is deleted after the
//! read is not blocked).

#![cfg(target_arch = "wasm32")]

use std::sync::atomic::{AtomicU64, Ordering};

use surrealkv_compat_indxdb::wasm::{is_indexeddb_available, read_all_from_browser, IdbSource};
use surrealkv_compat_indxdb::{is_indxdb_available, read_all_latest, Chunk, ChunkLimits};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;
use web_sys::{IdbDatabase, IdbFactory, IdbOpenDbRequest, IdbRequest};

wasm_bindgen_test_configure!(run_in_browser);

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A database name that no earlier test or run uses.
fn unique_name(label: &str) -> String {
	let n = COUNTER.fetch_add(1, Ordering::Relaxed);
	format!("{label}-{}-{n}", js_sys::Date::now() as u64)
}

fn factory() -> IdbFactory {
	web_sys::window().expect("window").indexed_db().unwrap().expect("indexedDB")
}

/// Resolves when `target` fires `success_event`, and fails the test when it fires `error` or
/// `abort`.
async fn wait_for(target: &web_sys::EventTarget, success_event: &str) {
	listen(target, success_event).await.expect("IndexedDB request failed");
}

/// Starts listening for `success_event` (and for an error or an abort) on `target` at once, and
/// gives the future that settles with the first of them. Unlike `wait_for` it listens before it
/// is polled, so an event that comes before the `await` is not missed.
fn listen(target: &web_sys::EventTarget, success_event: &str) -> JsFuture {
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
	JsFuture::from(promise)
}

/// Creates the database `name` with the object stores `stores` (out-of-line keys) and puts
/// `rows` into the store `kv`, if there is one. Keys and values are stored as `Uint8Array`s.
async fn create_db(name: &str, stores: &[&str], rows: &[(&[u8], &[u8])]) {
	let open: IdbOpenDbRequest = factory().open(name).unwrap();
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
		for (k, v) in rows {
			let key = js_sys::Uint8Array::from(*k);
			let val = js_sys::Uint8Array::from(*v);
			store.put_with_key(&val, &key).unwrap();
		}
		wait_for(tx.as_ref(), "complete").await;
	}
	db.close();
}

async fn delete_db(name: &str) {
	let req = factory().delete_database(name).unwrap();
	wait_for(req.as_ref(), "success").await;
}

#[wasm_bindgen_test]
fn indexeddb_is_available_in_the_browser() {
	assert!(is_indexeddb_available());
	// A name that is not an exported dump falls through to the browser's IndexedDB.
	assert!(is_indxdb_available("no-such-dump-directory"));
}

#[wasm_bindgen_test]
async fn reads_every_row_of_the_kv_store_in_key_order() {
	let name = unique_name("rows");
	create_db(&name, &["kv"], &[(b"b", b"2"), (b"a", b"1"), (b"c", b"")]).await;

	let rows = read_all_from_browser(&name).await.unwrap();
	assert_eq!(
		rows,
		vec![
			(b"a".to_vec(), b"1".to_vec()),
			(b"b".to_vec(), b"2".to_vec()),
			(b"c".to_vec(), Vec::new()),
		]
	);

	// `read_all_latest` reaches the same rows through the public entry point.
	assert_eq!(read_all_latest(&name).await.unwrap(), rows);
	delete_db(&name).await;
}

#[wasm_bindgen_test]
async fn binary_keys_and_values_come_back_unchanged() {
	let name = unique_name("binary");
	let key: Vec<u8> = (0..=255u8).collect();
	let value: Vec<u8> = (0..=255u8).rev().cycle().take(100_000).collect();
	create_db(&name, &["kv"], &[(&key, &value)]).await;

	let rows = read_all_from_browser(&name).await.unwrap();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].0, key);
	assert_eq!(rows[0].1, value);
	delete_db(&name).await;
}

#[wasm_bindgen_test]
async fn an_empty_kv_store_reads_as_no_rows() {
	let name = unique_name("empty");
	create_db(&name, &["kv"], &[]).await;
	assert!(read_all_from_browser(&name).await.unwrap().is_empty());
	delete_db(&name).await;
}

#[wasm_bindgen_test]
async fn a_database_without_a_kv_store_reads_as_no_rows() {
	let name = unique_name("other-store");
	create_db(&name, &["something-else"], &[]).await;
	assert!(read_all_from_browser(&name).await.unwrap().is_empty());
	delete_db(&name).await;
}

#[wasm_bindgen_test]
async fn many_rows_are_all_read() {
	let name = unique_name("many");
	let keys: Vec<[u8; 4]> = (0..2_000u32).map(|i| i.to_be_bytes()).collect();
	let rows: Vec<(&[u8], &[u8])> = keys.iter().map(|k| (&k[..], &k[..])).collect();
	create_db(&name, &["kv"], &rows).await;

	let read = read_all_from_browser(&name).await.unwrap();
	assert_eq!(read.len(), keys.len());
	for (row, key) in read.iter().zip(&keys) {
		assert_eq!(row.0, key);
		assert_eq!(row.1, key);
	}
	delete_db(&name).await;
}

// ---------------------------------------------------------------------------------------------
// What the browser says about the database, asked without the crate
// ---------------------------------------------------------------------------------------------

/// The version of `name` that `indexedDB.databases()` lists, `None` if it does not list it.
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

/// A value of the store in a form that tells its type and compares: what a test says two reads of
/// the same row must agree on.
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

/// Everything about a database that a reader must leave as it was: its version, its object stores
/// with their key paths, and every row of every store with the types of its key and its value.
#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
	version: u64,
	stores: Vec<(String, String)>,
	rows: Vec<(String, Vec<(String, String)>)>,
}

/// Reads `name` with the browser's own API. The database must be there: this opens it with no
/// version, which would make it otherwise.
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
		// The transaction is waited for to its end: a connection that is closed while one is
		// running is still open until it ends, and a deletion that follows would be blocked.
		let finished = listen(tx.as_ref(), "complete");
		let store = tx.object_store(&store_name).unwrap();
		let key_path = format!("{:?}", store.key_path().unwrap());
		stores.push((store_name.clone(), key_path));
		let keys = store.get_all_keys().unwrap();
		wait_for(keys.as_ref(), "success").await;
		let values = store.get_all().unwrap();
		wait_for(values.as_ref(), "success").await;
		finished.await.expect("the transaction of a snapshot did not complete");
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

/// Creates the database `name` at `version` with the stores `stores`, and puts `rows` of any key
/// and value type into the store `kv`.
async fn create_db_of(name: &str, version: u32, stores: &[&str], rows: &[(JsValue, JsValue)]) {
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
		for (key, value) in rows {
			store.put_with_key(value, key).unwrap();
		}
		wait_for(tx.as_ref(), "complete").await;
	}
	db.close();
}

fn bytes(of: &[u8]) -> JsValue {
	js_sys::Uint8Array::from(of).into()
}

/// Deletes `name` and fails the test if a connection that something left open holds it up: a
/// reader that does not close what it opened makes this `blocked`.
async fn delete_db_unblocked(name: &str) {
	let req = factory().delete_database(name).unwrap();
	let promise = js_sys::Promise::new(&mut |resolve, reject| {
		let ok = Closure::once_into_js(move || {
			resolve.call0(&JsValue::NULL).unwrap();
		});
		let blocked = Closure::once_into_js(move || {
			reject
				.call1(
					&JsValue::NULL,
					&JsValue::from_str("the deletion is blocked by an open connection"),
				)
				.unwrap();
		});
		req.set_onsuccess(Some(ok.unchecked_ref()));
		req.set_onblocked(Some(blocked.unchecked_ref()));
	});
	JsFuture::from(promise).await.expect("a connection of the reader was left open");
}

/// Every chunk of `source`, read with `limits` and continued from the key that each one gave.
async fn read_chunks(source: &IdbSource, limits: &ChunkLimits) -> Vec<Chunk<JsValue>> {
	let mut chunks: Vec<Chunk<JsValue>> = Vec::new();
	let mut after: Option<JsValue> = None;
	loop {
		let chunk = source.read_chunk(after.as_ref(), limits).await.unwrap().expect("source");
		let done = chunk.done;
		after = chunk.last.clone();
		chunks.push(chunk);
		if done {
			return chunks;
		}
		assert!(chunks.len() < 10_000, "the chunks do not end");
	}
}

fn entries_of(chunks: &[Chunk<JsValue>]) -> Vec<(Vec<u8>, Vec<u8>)> {
	chunks.iter().flat_map(|chunk| chunk.entries.iter().cloned()).collect()
}

fn sizes_of(chunks: &[Chunk<JsValue>]) -> Vec<usize> {
	chunks.iter().map(|chunk| chunk.entries.len()).collect()
}

// ---------------------------------------------------------------------------------------------
// Nothing is created
// ---------------------------------------------------------------------------------------------

/// Looks at a name that is not there in every way the crate has, and checks that the browser still
/// does not list it, and that a client that creates it afterwards at version 7 is the one that
/// does: the upgrade it runs goes from nothing, and the database is at version 7.
async fn nothing_is_created_by_looking(source: IdbSource) {
	let name = source.name().to_string();
	assert!(!source.exists().await.unwrap());
	assert!(source.read_chunk(None, &ChunkLimits::default()).await.unwrap().is_none());
	assert_eq!(listed_version(&name).await, None, "the probe made the database");
	assert!(read_all_from_browser(&name).await.unwrap().is_empty());
	assert_eq!(listed_version(&name).await, None, "the read made the database");
	assert!(read_all_latest(&name).await.unwrap().is_empty());
	assert_eq!(listed_version(&name).await, None, "the public read made the database");

	// The legacy client that creates the database at its own version.
	let open = factory().open_with_u32(&name, 7).unwrap();
	let old_version = std::rc::Rc::new(std::cell::Cell::new(f64::NAN));
	let seen = std::rc::Rc::clone(&old_version);
	let upgrade = Closure::once_into_js(move |event: web_sys::Event| {
		let event: web_sys::IdbVersionChangeEvent = event.unchecked_into();
		seen.set(event.old_version());
		let req: IdbRequest = event.target().unwrap().unchecked_into();
		let db: IdbDatabase = req.result().unwrap().unchecked_into();
		db.create_object_store("kv").unwrap();
	});
	open.set_onupgradeneeded(Some(upgrade.unchecked_ref()));
	wait_for(open.as_ref(), "success").await;
	let db: IdbDatabase = open.result().unwrap().unchecked_into();
	assert_eq!(old_version.get(), 0.0, "the client did not create the database");
	assert_eq!(db.version(), 7.0);
	db.close();
	assert_eq!(listed_version(&name).await, Some(7.0));
	delete_db_unblocked(&name).await;
}

#[wasm_bindgen_test]
async fn a_missing_database_is_not_created_by_looking_at_it() {
	nothing_is_created_by_looking(IdbSource::new(unique_name("missing"))).await;
}

#[wasm_bindgen_test]
async fn a_missing_database_is_not_created_where_the_browser_cannot_list_databases() {
	// The open settles it: the upgrade of a database that is made is aborted.
	nothing_is_created_by_looking(IdbSource::new(unique_name("missing-nolist")).without_listing())
		.await;
}

#[wasm_bindgen_test]
async fn a_database_without_a_kv_store_is_not_changed_by_looking_at_it() {
	for listing in [true, false] {
		let name = unique_name("other-store-unchanged");
		create_db_of(&name, 3, &["something-else"], &[]).await;
		let before = snapshot(&name).await;
		assert_eq!(before.version, 3);

		let source = if listing {
			IdbSource::new(&name)
		} else {
			IdbSource::new(&name).without_listing()
		};
		assert!(!source.exists().await.unwrap());
		assert!(source.read_chunk(None, &ChunkLimits::default()).await.unwrap().is_none());
		assert!(read_all_from_browser(&name).await.unwrap().is_empty());

		assert_eq!(snapshot(&name).await, before);
		assert_eq!(listed_version(&name).await, Some(3.0));
		delete_db_unblocked(&name).await;
	}
}

#[wasm_bindgen_test]
async fn reading_leaves_the_version_the_stores_and_the_rows_as_they_were() {
	for listing in [true, false] {
		let name = unique_name("unchanged");
		let rows: Vec<(JsValue, JsValue)> =
			(0..50u8).map(|i| (bytes(&[i]), bytes(&[i; 10]))).collect();
		create_db_of(&name, 5, &["kv", "other"], &rows).await;
		let before = snapshot(&name).await;
		assert_eq!(before.version, 5);
		assert_eq!(before.rows[0].1.len() + before.rows[1].1.len(), 50);

		let source = if listing {
			IdbSource::new(&name)
		} else {
			IdbSource::new(&name).without_listing()
		};
		assert!(source.exists().await.unwrap());
		let chunks = read_chunks(&source, &ChunkLimits::new(usize::MAX, 8)).await;
		assert_eq!(entries_of(&chunks).len(), 50);
		assert_eq!(read_all_from_browser(&name).await.unwrap().len(), 50);

		assert_eq!(snapshot(&name).await, before);
		delete_db_unblocked(&name).await;
	}
}

// ---------------------------------------------------------------------------------------------
// Chunks
// ---------------------------------------------------------------------------------------------

/// Rows `0..n`, each with a key of one byte and a value of `len` bytes, so that each weighs
/// `len + 1`.
fn numbered(n: u8, len: usize) -> Vec<(JsValue, JsValue)> {
	(0..n).map(|i| (bytes(&[i]), bytes(&vec![i; len]))).collect()
}

#[wasm_bindgen_test]
async fn a_chunk_is_bounded_in_bytes_and_the_next_one_continues_after_its_last_key() {
	let name = unique_name("chunk-bytes");
	create_db_of(&name, 1, &["kv"], &numbered(10, 99)).await;
	let source = IdbSource::new(&name);

	// Each row weighs one hundred bytes. Three fit in three hundred, and not four.
	let chunks = read_chunks(&source, &ChunkLimits::new(300, 1000)).await;
	assert_eq!(sizes_of(&chunks), vec![3, 3, 3, 1]);
	assert!(chunks.iter().all(|chunk| chunk.bytes <= 300));
	assert_eq!(chunks.iter().map(|c| c.done).collect::<Vec<_>>(), vec![false, false, false, true]);
	let all = entries_of(&chunks);
	assert_eq!(all.len(), 10);
	for (i, (key, value)) in all.iter().enumerate() {
		assert_eq!(key, &vec![i as u8]);
		assert_eq!(value, &vec![i as u8; 99]);
	}

	// One byte more or less than the weight of three rows.
	assert_eq!(sizes_of(&read_chunks(&source, &ChunkLimits::new(299, 1000)).await), vec![2; 5]);
	assert_eq!(
		sizes_of(&read_chunks(&source, &ChunkLimits::new(301, 1000)).await),
		vec![3, 3, 3, 1]
	);
	delete_db_unblocked(&name).await;
}

#[wasm_bindgen_test]
async fn a_chunk_is_bounded_in_entries() {
	let name = unique_name("chunk-entries");
	create_db_of(&name, 1, &["kv"], &numbered(10, 4)).await;
	let source = IdbSource::new(&name);
	let chunks = read_chunks(&source, &ChunkLimits::new(usize::MAX, 4)).await;
	assert_eq!(sizes_of(&chunks), vec![4, 4, 2]);
	assert_eq!(entries_of(&chunks).len(), 10);
	// A store that ends exactly when a chunk is full is followed by an empty chunk that is done.
	let chunks = read_chunks(&source, &ChunkLimits::new(usize::MAX, 5)).await;
	assert_eq!(sizes_of(&chunks), vec![5, 5, 0]);
	assert!(chunks.last().unwrap().done);
	assert!(chunks.last().unwrap().last.is_none());
	// One row at a time.
	assert_eq!(sizes_of(&read_chunks(&source, &ChunkLimits::new(usize::MAX, 1)).await).len(), 11);
	delete_db_unblocked(&name).await;
}

#[wasm_bindgen_test]
async fn a_row_larger_than_the_limit_is_a_chunk_of_its_own() {
	let name = unique_name("chunk-large");
	let rows = vec![
		(bytes(b"a"), bytes(&[1; 9])),
		(bytes(b"b"), bytes(&vec![2; 5000])),
		(bytes(b"c"), bytes(&[3; 9])),
		(bytes(b"d"), bytes(&[4; 9])),
	];
	create_db_of(&name, 1, &["kv"], &rows).await;
	let chunks = read_chunks(&IdbSource::new(&name), &ChunkLimits::new(1000, 1000)).await;
	assert_eq!(sizes_of(&chunks), vec![1, 1, 2]);
	assert_eq!(chunks[1].bytes, 5001);
	let all = entries_of(&chunks);
	assert_eq!(
		all.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
		vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
	);
	assert_eq!(all[1].1.len(), 5000);
	delete_db_unblocked(&name).await;
}

#[wasm_bindgen_test]
async fn a_large_store_is_read_in_several_chunks_with_every_row_once() {
	let name = unique_name("chunk-many");
	let rows: Vec<(JsValue, JsValue)> =
		(0..25_000u32).map(|i| (bytes(&i.to_be_bytes()), bytes(&[(i % 251) as u8; 20]))).collect();
	create_db_of(&name, 1, &["kv"], &rows).await;
	let chunks = read_chunks(&IdbSource::new(&name), &ChunkLimits::new(256 * 1024, 4_000)).await;
	assert!(chunks.len() > 1, "{} chunks", chunks.len());
	let all = entries_of(&chunks);
	assert_eq!(all.len(), 25_000);
	for (i, (key, value)) in all.iter().enumerate() {
		assert_eq!(key.as_slice(), &(i as u32).to_be_bytes());
		assert_eq!(value, &vec![(i as u32 % 251) as u8; 20]);
	}
	delete_db_unblocked(&name).await;
}

// ---------------------------------------------------------------------------------------------
// Keys and values of the types that the reader takes
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn values_that_are_array_buffers_or_strings_are_read_as_their_bytes() {
	let name = unique_name("types");
	let buffer = js_sys::Uint8Array::from(&[7u8, 8, 9][..]).buffer();
	let rows = vec![
		(bytes(b"a-uint8array"), bytes(&[1, 2, 3])),
		(bytes(b"b-arraybuffer"), buffer.into()),
		(bytes(b"c-string"), JsValue::from_str("h\u{e9}llo")),
	];
	create_db_of(&name, 1, &["kv"], &rows).await;
	let read = read_all_from_browser(&name).await.unwrap();
	assert_eq!(
		read,
		vec![
			(b"a-uint8array".to_vec(), vec![1, 2, 3]),
			(b"b-arraybuffer".to_vec(), vec![7, 8, 9]),
			(b"c-string".to_vec(), "h\u{e9}llo".as_bytes().to_vec()),
		]
	);
	delete_db_unblocked(&name).await;
}

#[wasm_bindgen_test]
async fn the_next_chunk_continues_after_a_key_of_any_type() {
	// String keys, and a store that has binary keys and string keys together (the browser orders
	// them by type, and the reader continues from the value that the cursor gave).
	let name = unique_name("key-types");
	let mut rows: Vec<(JsValue, JsValue)> = Vec::new();
	for i in 0..9u8 {
		rows.push((JsValue::from_str(&format!("s{i}")), bytes(&[i])));
		rows.push((bytes(&[b'b', i]), bytes(&[i])));
	}
	create_db_of(&name, 1, &["kv"], &rows).await;
	let chunks = read_chunks(&IdbSource::new(&name), &ChunkLimits::new(usize::MAX, 4)).await;
	assert_eq!(sizes_of(&chunks), vec![4, 4, 4, 4, 2]);
	let mut keys: Vec<Vec<u8>> = entries_of(&chunks).into_iter().map(|(k, _)| k).collect();
	assert_eq!(keys.len(), 18, "every row is read once");
	keys.sort();
	keys.dedup();
	assert_eq!(keys.len(), 18, "no row is read twice");
	delete_db_unblocked(&name).await;
}

#[wasm_bindgen_test]
async fn a_key_of_a_type_that_has_no_bytes_is_an_error_and_not_a_silent_skip() {
	let name = unique_name("number-key");
	create_db_of(&name, 1, &["kv"], &[(JsValue::from_f64(1.0), bytes(b"v"))]).await;
	let error = read_all_from_browser(&name).await.expect_err("a number is not bytes");
	assert!(error.to_string().contains("key"), "{error}");
	let error = IdbSource::new(&name)
		.read_chunk(None, &ChunkLimits::default())
		.await
		.expect_err("a number is not bytes");
	assert!(error.to_string().contains("key"), "{error}");
	// The connection of the read that failed is closed.
	delete_db_unblocked(&name).await;
}

#[wasm_bindgen_test]
async fn no_connection_is_left_open_by_a_read_that_succeeds_or_fails_or_finds_nothing() {
	let name = unique_name("closed");
	create_db_of(&name, 1, &["kv"], &numbered(5, 3)).await;
	let source = IdbSource::new(&name);
	let _ = read_chunks(&source, &ChunkLimits::new(usize::MAX, 2)).await;
	assert!(source.exists().await.unwrap());
	delete_db_unblocked(&name).await;

	let name = unique_name("closed-no-store");
	create_db_of(&name, 1, &["other"], &[]).await;
	assert!(!IdbSource::new(&name).exists().await.unwrap());
	assert!(read_all_from_browser(&name).await.unwrap().is_empty());
	delete_db_unblocked(&name).await;
}

#[wasm_bindgen_test]
async fn a_database_that_another_context_wants_to_delete_is_not_held_up_by_a_reader() {
	// A read that is over has closed its connection, and a deletion that arrives while the
	// database is open elsewhere is answered by the reader's `versionchange` handler: here the
	// deletion simply goes through while the chunks of a source are read one after another.
	let name = unique_name("deleted-between-chunks");
	create_db_of(&name, 1, &["kv"], &numbered(6, 3)).await;
	let source = IdbSource::new(&name);
	let first = source.read_chunk(None, &ChunkLimits::new(usize::MAX, 2)).await.unwrap().unwrap();
	assert_eq!(first.entries.len(), 2);
	delete_db_unblocked(&name).await;
	// The next chunk finds nothing, and does not make the database again.
	assert!(source
		.read_chunk(first.last.as_ref(), &ChunkLimits::new(usize::MAX, 2))
		.await
		.unwrap()
		.is_none());
	assert_eq!(listed_version(&name).await, None);
}

// ---------------------------------------------------------------------------------------------
// The shape of the store
// ---------------------------------------------------------------------------------------------

/// Creates the database `name` with a store `kv` made with a key path, or with a key generator, or
/// both: what the legacy backend never did. The rows are given as the key, if there is one to give,
/// and the value.
async fn create_kv_with(
	name: &str,
	key_path: Option<&str>,
	auto_increment: bool,
	rows: &[(Option<JsValue>, JsValue)],
) {
	let open: IdbOpenDbRequest = factory().open_with_u32(name, 1).unwrap();
	let key_path = key_path.map(str::to_string);
	let upgrade = Closure::once_into_js(move |event: web_sys::Event| {
		let req: IdbRequest = event.target().unwrap().unchecked_into();
		let db: IdbDatabase = req.result().unwrap().unchecked_into();
		let options = js_sys::Object::new();
		if let Some(path) = &key_path {
			js_sys::Reflect::set(&options, &"keyPath".into(), &path.as_str().into()).unwrap();
		}
		if auto_increment {
			js_sys::Reflect::set(&options, &"autoIncrement".into(), &true.into()).unwrap();
		}
		let create: js_sys::Function =
			js_sys::Reflect::get(&db, &"createObjectStore".into()).unwrap().unchecked_into();
		create.call2(&db, &"kv".into(), &options).unwrap();
	});
	open.set_onupgradeneeded(Some(upgrade.unchecked_ref()));
	wait_for(open.as_ref(), "success").await;
	let db: IdbDatabase = open.result().unwrap().unchecked_into();
	let tx =
		db.transaction_with_str_and_mode("kv", web_sys::IdbTransactionMode::Readwrite).unwrap();
	let store = tx.object_store("kv").unwrap();
	for (key, value) in rows {
		match key {
			Some(key) => store.put_with_key(value, key).unwrap(),
			None => store.put(value).unwrap(),
		};
	}
	wait_for(tx.as_ref(), "complete").await;
	db.close();
}

/// A store that the legacy backend did not make, and what the refusal of it says.
struct OddStore {
	label: &'static str,
	key_path: Option<&'static str>,
	auto_increment: bool,
	rows: Vec<(Option<JsValue>, JsValue)>,
	mentions: &'static str,
}

#[wasm_bindgen_test]
async fn a_store_that_is_not_of_the_shape_that_the_legacy_backend_made_is_refused() {
	let object = js_sys::Object::new();
	js_sys::Reflect::set(&object, &"id".into(), &bytes(b"id1")).unwrap();
	js_sys::Reflect::set(&object, &"v".into(), &bytes(b"x")).unwrap();
	let cases = vec![
		OddStore {
			label: "auto-increment-explicit-keys",
			key_path: None,
			auto_increment: true,
			rows: vec![(Some(bytes(b"explicit")), bytes(b"v"))],
			mentions: "autoIncrement",
		},
		OddStore {
			label: "auto-increment-made-keys",
			key_path: None,
			auto_increment: true,
			rows: vec![(None, bytes(b"one")), (None, bytes(b"two"))],
			mentions: "autoIncrement",
		},
		OddStore {
			label: "key-path-empty",
			key_path: Some(""),
			auto_increment: false,
			rows: vec![(None, bytes(b"selfkeyed"))],
			mentions: "key path",
		},
		OddStore {
			label: "key-path-inline",
			key_path: Some("id"),
			auto_increment: false,
			rows: vec![(None, object.into())],
			mentions: "key path",
		},
	];
	for case in cases {
		let OddStore {
			label,
			key_path,
			auto_increment,
			rows,
			mentions,
		} = case;
		let name = unique_name(label);
		create_kv_with(&name, key_path, auto_increment, &rows).await;
		let before = snapshot(&name).await;

		let source = IdbSource::new(&name);
		for (what, error) in [
			("exists", source.exists().await.err()),
			("read_chunk", source.read_chunk(None, &ChunkLimits::default()).await.err()),
			("read_all_from_browser", read_all_from_browser(&name).await.err()),
			("read_all_latest", read_all_latest(&name).await.err()),
		] {
			let error = error.unwrap_or_else(|| panic!("{label}: {what} accepted the store"));
			assert!(error.to_string().contains(mentions), "{label}: {what}: {error}");
		}
		assert_eq!(snapshot(&name).await, before, "{label}: the store was changed");
		// No connection of the refusals is left open.
		delete_db_unblocked(&name).await;
	}

	// A store that the legacy backend would have made is read as before.
	let name = unique_name("plain-shape");
	create_db(&name, &["kv"], &[(b"a", b"1"), (b"b", b"2")]).await;
	let source = IdbSource::new(&name);
	assert!(source.exists().await.unwrap());
	assert_eq!(read_all_from_browser(&name).await.unwrap().len(), 2);
	delete_db_unblocked(&name).await;
}

// ---------------------------------------------------------------------------------------------
// A future that is dropped
// ---------------------------------------------------------------------------------------------

/// Waits `ms` milliseconds, which is a turn of the event loop at the least: what the browser has
/// for the page is delivered.
async fn tick(ms: i32) {
	let promise = js_sys::Promise::new(&mut |resolve, _| {
		web_sys::window()
			.unwrap()
			.set_timeout_with_callback_and_timeout_and_arguments_0(&resolve, ms)
			.unwrap();
	});
	JsFuture::from(promise).await.unwrap();
}

/// Polls `future` once, and again after each turn of the event loop, and drops it, wherever it is
/// waiting, after `polls` polls if it has not finished by then. Whether it finished. The first poll
/// is what makes the request. With `settle`, the page waits for the browser to answer after the
/// last poll and before the drop, so that the future is dropped with its answer in and not read;
/// without it, the future is dropped with its request still on its way.
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
			tick(0).await;
		}
	}
	if settle {
		tick(30).await;
	}
	drop(future);
	false
}

/// How long a request that should be answered at once is waited for before it counts as held up.
const HELD_UP_AFTER_MS: i32 = 3_000;

/// Resolves to `false` if `request` is answered with `success` within `HELD_UP_AFTER_MS`, and to
/// `true` if it is not: something holds it up. The `blocked` event is not what says so, since the
/// browser fires it for a connection that is on its way to closed, and the request goes on after
/// it. The request is left to go on, and a connection that it opens is closed if
/// `close_what_opens`.
async fn held_up(request: &web_sys::IdbOpenDbRequest, close_what_opens: bool) -> bool {
	let promise = js_sys::Promise::new(&mut |resolve, _| {
		let (on_success, on_late) = (resolve.clone(), resolve);
		let success = Closure::once_into_js(move |event: web_sys::Event| {
			if close_what_opens {
				if let Some(request) = event.target().and_then(|t| t.dyn_into::<IdbRequest>().ok())
				{
					if let Ok(db) = request.result() {
						if let Some(db) = db.dyn_ref::<IdbDatabase>() {
							db.close();
						}
					}
				}
			}
			let _ = on_success.call1(&JsValue::NULL, &JsValue::FALSE);
		});
		let late = Closure::once_into_js(move || {
			let _ = on_late.call1(&JsValue::NULL, &JsValue::TRUE);
		});
		request.set_onsuccess(Some(success.unchecked_ref()));
		web_sys::window()
			.unwrap()
			.set_timeout_with_callback_and_timeout_and_arguments_0(
				late.unchecked_ref(),
				HELD_UP_AFTER_MS,
			)
			.unwrap();
	});
	JsFuture::from(promise).await.unwrap().as_bool().unwrap()
}

/// Whether a request for version `version + 1` of `name`, which has to wait for every connection
/// to it to close, is held up.
async fn upgrade_is_held_up(name: &str, version: u32) -> bool {
	let open: IdbOpenDbRequest = factory().open_with_u32(name, version + 1).unwrap();
	let upgrade = Closure::once_into_js(move |_event: web_sys::Event| {});
	open.set_onupgradeneeded(Some(upgrade.unchecked_ref()));
	held_up(&open, true).await
}

/// Whether the deletion of `name` is held up.
async fn deletion_is_held_up(name: &str) -> bool {
	let request = factory().delete_database(name).unwrap();
	// A deletion is answered with a request of its own kind, which has the same events.
	held_up(&request, false).await
}

#[wasm_bindgen_test]
async fn a_read_that_is_dropped_wherever_it_waits_leaves_no_connection_open() {
	let mut held = Vec::new();
	'sweep: for (listing, settle) in [(false, false), (false, true), (true, false)] {
		for polls in 1..=400 {
			let name = unique_name("dropped-read");
			create_db_of(&name, 2, &["kv"], &numbered(40, 8)).await;
			let source = if listing {
				IdbSource::new(&name)
			} else {
				IdbSource::new(&name).without_listing()
			};
			let finished = {
				let source = source.clone();
				poll_then_drop(
					async move {
						let _ = source.read_chunk(None, &ChunkLimits::new(usize::MAX, 10)).await;
					},
					polls,
					settle,
				)
				.await
			};
			// What the browser had for the dropped call is delivered.
			tick(30).await;
			if upgrade_is_held_up(&name, 2).await || deletion_is_held_up(&name).await {
				held.push((listing, settle, polls));
				// The request that waits for the connection never ends: no more is learned.
				break 'sweep;
			}
			if finished {
				break;
			}
			assert!(polls < 400, "a read of one chunk is not done after {polls} polls");
		}
	}
	assert!(
		held.is_empty(),
		"a dropped read left a connection open at (listing, settle, polls): {held:?}"
	);
}

#[wasm_bindgen_test]
async fn a_probe_that_is_dropped_wherever_it_waits_does_not_make_a_missing_database() {
	let mut made = Vec::new();
	for (listing, settle) in [(false, false), (false, true), (true, false)] {
		for polls in 1..=200 {
			let name = unique_name("dropped-probe");
			let source = if listing {
				IdbSource::new(&name)
			} else {
				IdbSource::new(&name).without_listing()
			};
			let finished = {
				let source = source.clone();
				poll_then_drop(
					async move {
						let _ = source.exists().await;
					},
					polls,
					settle,
				)
				.await
			};
			tick(30).await;
			if listed_version(&name).await.is_some() {
				made.push((listing, settle, polls));
				delete_db_unblocked(&name).await;
			}
			if finished {
				break;
			}
		}
	}
	assert!(
		made.is_empty(),
		"a dropped probe made the database at (listing, settle, polls): {made:?}"
	);
}

#[wasm_bindgen_test]
async fn a_dropped_open_does_not_hold_up_an_upgrade_and_the_opens_that_queue_behind_it() {
	// A connection that is left open holds up the upgrade that a legacy client asks for, and every
	// open of the name that comes after the request of that upgrade waits behind it.
	for (polls, settle) in [(1, false), (2, false), (3, false), (1, true), (2, true), (3, true)] {
		let name = unique_name("dropped-then-upgraded");
		create_db_of(&name, 1, &["kv"], &numbered(3, 4)).await;
		let source = IdbSource::new(&name).without_listing();
		{
			let source = source.clone();
			poll_then_drop(
				async move {
					let _ = source.exists().await;
				},
				polls,
				settle,
			)
			.await;
		}
		tick(30).await;

		// The request of the legacy client is left to wait for its turn, and closes what it opens.
		let upgrade: IdbOpenDbRequest = factory().open_with_u32(&name, 2).unwrap();
		let noop = Closure::once_into_js(move |_event: web_sys::Event| {});
		upgrade.set_onupgradeneeded(Some(noop.unchecked_ref()));
		let upgraded = std::rc::Rc::new(std::cell::Cell::new(false));
		let done = std::rc::Rc::clone(&upgraded);
		let closing = Closure::once_into_js(move |event: web_sys::Event| {
			let request: IdbRequest = event.target().unwrap().unchecked_into();
			request.result().unwrap().unchecked_into::<IdbDatabase>().close();
			done.set(true);
		});
		upgrade.set_onsuccess(Some(closing.unchecked_ref()));
		tick(50).await;

		// The next call of the reader: it opens the same name, and has to be answered.
		let answered = std::rc::Rc::new(std::cell::Cell::new(false));
		let flag = std::rc::Rc::clone(&answered);
		let next = source.clone();
		wasm_bindgen_futures::spawn_local(async move {
			let _ = next.exists().await;
			flag.set(true);
		});
		let mut waited = 0;
		while !answered.get() && waited < HELD_UP_AFTER_MS {
			tick(25).await;
			waited += 25;
		}
		assert!(
			answered.get(),
			"an open after a dropped open (dropped at poll {polls}, settled: {settle}) is not answered \
			 after {HELD_UP_AFTER_MS} ms; the upgrade of the legacy client has run: {}",
			upgraded.get()
		);
		assert!(
			upgraded.get() || {
				tick(100).await;
				upgraded.get()
			}
		);
		delete_db_unblocked(&name).await;
	}
}
