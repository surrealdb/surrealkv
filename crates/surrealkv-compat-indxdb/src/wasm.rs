//! Direct browser IndexedDB reader for WebAssembly environments.

#![cfg(target_arch = "wasm32")]

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{IdbDatabase, IdbFactory, IdbOpenDbRequest, IdbRequest, IdbTransactionMode};

use crate::error::{Error, Result};

/// Checks whether IndexedDB is available in the current browser global scope.
pub fn is_indexeddb_available() -> bool {
	get_idb_factory().is_ok()
}

fn get_idb_factory() -> Result<IdbFactory> {
	let global = js_sys::global();
	if let Ok(window) = global.clone().dyn_into::<web_sys::Window>() {
		window
			.indexed_db()
			.map_err(|e| Error::Js(format!("Failed to access indexedDB: {:?}", e)))?
			.ok_or_else(|| Error::Js("indexedDB is not supported on window".into()))
	} else if let Ok(worker) = global.dyn_into::<web_sys::WorkerGlobalScope>() {
		worker
			.indexed_db()
			.map_err(|e| Error::Js(format!("Failed to access indexedDB in worker: {:?}", e)))?
			.ok_or_else(|| Error::Js("indexedDB is not supported in worker".into()))
	} else {
		Err(Error::Js("No browser window or worker global found".into()))
	}
}

/// Reads all live key-value pairs from the `"kv"` object store of an IndexedDB database.
pub async fn read_all_from_browser(db_name: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	let factory = get_idb_factory()?;
	let open_req: IdbOpenDbRequest = factory
		.open(db_name)
		.map_err(|e| Error::Js(format!("Failed to open database {db_name}: {:?}", e)))?;

	let db_val = wait_for_request(open_req.as_ref()).await?;
	let db: IdbDatabase = db_val
		.dyn_into()
		.map_err(|_| Error::Js("Failed to cast request result to IdbDatabase".into()))?;

	// Check if "kv" object store exists
	let store_names = db.object_store_names();
	let mut found_kv = false;
	for i in 0..store_names.length() {
		if store_names.item(i).as_deref() == Some("kv") {
			found_kv = true;
			break;
		}
	}

	if !found_kv {
		db.close();
		return Ok(Vec::new());
	}

	let tx = db
		.transaction_with_str_and_mode("kv", IdbTransactionMode::Readonly)
		.map_err(|e| Error::Js(format!("Failed to open transaction on kv: {:?}", e)))?;

	let store = tx
		.object_store("kv")
		.map_err(|e| Error::Js(format!("Failed to get object store kv: {:?}", e)))?;

	let cursor_req = store
		.open_cursor()
		.map_err(|e| Error::Js(format!("Failed to open cursor on kv: {:?}", e)))?;

	let mut entries = Vec::new();

	loop {
		let result_val = wait_for_request(&cursor_req).await?;
		if result_val.is_null() || result_val.is_undefined() {
			break;
		}

		let cursor: web_sys::IdbCursorWithValue = result_val
			.dyn_into()
			.map_err(|_| Error::Js("Failed to cast cursor to IdbCursorWithValue".into()))?;

		let key_val =
			cursor.key().map_err(|e| Error::Js(format!("Failed to get cursor key: {:?}", e)))?;
		let val_val = cursor
			.value()
			.map_err(|e| Error::Js(format!("Failed to get cursor value: {:?}", e)))?;

		let key_bytes = js_value_to_bytes(&key_val)?;
		let val_bytes = js_value_to_bytes(&val_val)?;

		entries.push((key_bytes, val_bytes));

		// Advance cursor to trigger next onsuccess event on cursor_req
		cursor.continue_().map_err(|e| Error::Js(format!("Failed to advance cursor: {:?}", e)))?;
	}

	db.close();
	Ok(entries)
}

fn js_value_to_bytes(val: &JsValue) -> Result<Vec<u8>> {
	if let Ok(uint8_arr) = val.clone().dyn_into::<js_sys::Uint8Array>() {
		Ok(uint8_arr.to_vec())
	} else if let Ok(arr_buf) = val.clone().dyn_into::<js_sys::ArrayBuffer>() {
		let uint8_arr = js_sys::Uint8Array::new(&arr_buf);
		Ok(uint8_arr.to_vec())
	} else if let Some(s) = val.as_string() {
		Ok(s.into_bytes())
	} else {
		Err(Error::Js(format!("Unsupported IndexedDB value type: {:?}", val)))
	}
}

async fn wait_for_request(req: &IdbRequest) -> Result<JsValue> {
	let promise = js_sys::Promise::new(&mut |resolve, reject| {
		let onsuccess = Closure::once(move |event: web_sys::Event| {
			let target = event.target().unwrap();
			let req: IdbRequest = target.unchecked_into();
			resolve.call1(&JsValue::NULL, &req.result().unwrap()).unwrap();
		});
		let onerror = Closure::once(move |event: web_sys::Event| {
			let target = event.target().unwrap();
			let req: IdbRequest = target.unchecked_into();
			let err_val = req
				.error()
				.ok()
				.flatten()
				.map_or_else(|| JsValue::from_str("Unknown error"), JsValue::from);
			reject.call1(&JsValue::NULL, &err_val).unwrap();
		});

		req.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
		req.set_onerror(Some(onerror.as_ref().unchecked_ref()));

		onsuccess.forget();
		onerror.forget();
	});

	JsFuture::from(promise)
		.await
		.map_err(|e| Error::Js(format!("IndexedDB request rejected: {:?}", e)))
}
