//! A legacy IndexedDB database for the browser tests of the move out of IndexedDB that run workers
//! (`tests/opfs_two_workers_test.rs`, which includes this file with `#[path]`). The coordinator
//! builds the database and looks at it before and after the workers have copied it; the workers
//! only read it, through `migrate_from_indexeddb`.
//!
//! The layout is the one of the legacy `indxdb://` store: an object store `kv`, keys and values
//! that are `Uint8Array`s. The same layout is built in `tests/opfs_migrate_test.rs`, which is not
//! shared with this file because a test target cannot include another.

#![allow(dead_code)]

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{IdbDatabase, IdbFactory, IdbOpenDbRequest, IdbRequest};

use crate::opfs_common::pattern;
use crate::workers::digest_rows;

pub type Rows = Vec<(Vec<u8>, Vec<u8>)>;

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

/// The rows of a legacy store: a few thousand of them with values of every length from none to
/// above the threshold of the value log, and three of several hundred kilobytes. In the order of
/// the keys, which is the order that IndexedDB and the engine both keep them in.
pub fn source_rows(count: usize) -> Rows {
	let mut rows: Rows = (0..count)
		.map(|i| {
			let key = (i as u32).wrapping_mul(2_654_435_761).to_be_bytes().to_vec();
			let len = [0usize, 1, 100, 255, 256, 257, 700, 3_000][i % 8];
			(key, pattern(i as u64 + 1, len))
		})
		.collect();
	for n in 0..3u64 {
		rows.push((
			format!("large-{n}").into_bytes(),
			pattern(1_000 + n, 300_000 + 40_000 * n as usize),
		));
	}
	rows.sort();
	rows
}

pub fn rows_digest(rows: &Rows) -> u64 {
	digest_rows(rows.iter().map(|(k, v)| (k.as_slice(), v.as_slice())))
}

/// Makes the database `name` (version 1) with the object store `kv` and puts `rows` in it.
pub async fn create_source(name: &str, rows: &Rows) {
	let open: IdbOpenDbRequest = factory().open_with_u32(name, 1).unwrap();
	let upgrade = Closure::once_into_js(move |event: web_sys::Event| {
		let req: IdbRequest = event.target().unwrap().unchecked_into();
		let db: IdbDatabase = req.result().unwrap().unchecked_into();
		db.create_object_store("kv").unwrap();
	});
	open.set_onupgradeneeded(Some(upgrade.unchecked_ref()));
	wait_for(open.as_ref(), "success").await;
	let db: IdbDatabase = open.result().unwrap().unchecked_into();
	// In pieces, so that no one transaction holds all of the megabytes at once.
	for chunk in rows.chunks(500) {
		let tx =
			db.transaction_with_str_and_mode("kv", web_sys::IdbTransactionMode::Readwrite).unwrap();
		let store = tx.object_store("kv").unwrap();
		for (key, value) in chunk {
			let key = js_sys::Uint8Array::from(key.as_slice());
			let value = js_sys::Uint8Array::from(value.as_slice());
			store.put_with_key(&value, &key).unwrap();
		}
		wait_for(tx.as_ref(), "complete").await;
	}
	db.close();
}

/// What the move must leave as it was.
#[derive(Debug, PartialEq, Eq)]
pub struct Fingerprint {
	pub version: u64,
	pub stores: Vec<String>,
	pub rows: u64,
	pub digest: u64,
}

fn bytes_of(value: &JsValue) -> Vec<u8> {
	if let Some(bytes) = value.dyn_ref::<js_sys::Uint8Array>() {
		bytes.to_vec()
	} else if let Some(buffer) = value.dyn_ref::<js_sys::ArrayBuffer>() {
		js_sys::Uint8Array::new(buffer).to_vec()
	} else {
		panic!(
			"a row of the legacy store that is neither a Uint8Array nor an ArrayBuffer: {value:?}"
		)
	}
}

/// Reads the whole database `name` with the API of the browser: its version, its object stores
/// and a hash of every row of `kv`. The database must be there: this opens it without a version,
/// which would make it otherwise.
pub async fn fingerprint(name: &str) -> Fingerprint {
	let open = factory().open(name).unwrap();
	wait_for(open.as_ref(), "success").await;
	let db: IdbDatabase = open.result().unwrap().unchecked_into();
	let version = db.version() as u64;
	let names = db.object_store_names();
	let stores: Vec<String> = (0..names.length()).filter_map(|i| names.item(i)).collect();
	let tx = db.transaction_with_str("kv").unwrap();
	let store = tx.object_store("kv").unwrap();
	let keys = store.get_all_keys().unwrap();
	wait_for(keys.as_ref(), "success").await;
	let values = store.get_all().unwrap();
	wait_for(values.as_ref(), "success").await;
	let keys: js_sys::Array = keys.result().unwrap().unchecked_into();
	let values: js_sys::Array = values.result().unwrap().unchecked_into();
	let rows: Rows =
		keys.iter().zip(values.iter()).map(|(k, v)| (bytes_of(&k), bytes_of(&v))).collect();
	db.close();
	Fingerprint {
		version,
		stores,
		rows: rows.len() as u64,
		digest: rows_digest(&rows),
	}
}

pub async fn delete_source(name: &str) {
	let req = factory().delete_database(name).unwrap();
	wait_for(req.as_ref(), "success").await;
}
