//! Browser tests of the IndexedDB reader, run in headless Chrome (`wasm-pack test --headless
//! --chrome crates/surrealkv-compat-indxdb`). Node has no IndexedDB, so they cannot run there.
//!
//! Each test creates a database of its own in the browser's IndexedDB, in the layout the legacy
//! `indxdb://` store of SurrealDB used (an object store named `kv` with out-of-line keys), reads
//! it back through the crate, and deletes it.

#![cfg(target_arch = "wasm32")]

use std::sync::atomic::{AtomicU64, Ordering};

use surrealkv_compat_indxdb::wasm::{is_indexeddb_available, read_all_from_browser};
use surrealkv_compat_indxdb::{is_indxdb_available, read_all_latest};
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
		let tx = db
			.transaction_with_str_and_mode("kv", web_sys::IdbTransactionMode::Readwrite)
			.unwrap();
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
