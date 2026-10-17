//! Direct browser IndexedDB reader for WebAssembly environments.
//!
//! The reader only reads, and it never creates. A database is opened without a version, so that
//! the browser opens the one that is there at the version that it has, and a name that is not there
//! is never made: where `indexedDB.databases()` exists it says so first, and in every case the open
//! itself aborts the upgrade transaction that a missing database would run (its `oldVersion` is
//! zero), which the browser takes as "never created". A legacy client that expects to create the
//! database at its own version finds it as it would have if nothing had looked.
//!
//! A store is read in chunks (see [`crate::chunk`]), each in a read-only transaction of its own on
//! a connection of its own, which is closed when the chunk is read and on every path out of it. A
//! chunk continues after the last key of the one before, given as the JavaScript value that the
//! cursor gave, so that what type the keys are never matters.
//!
//! The store has to be of the shape that the legacy backend made (keys that are given with each
//! row, and not in the value or made by the store); one that has a key path or a key generator is
//! refused with an error. A future that is dropped while a request to open is on its way (a
//! timeout, a cancelled task) does not leave what the browser answers with open, or make a database
//! that is not there: the request is given listeners that close the connection and abort the
//! upgrade, since a connection that nobody closes holds up every later upgrade and deletion.

#![cfg(target_arch = "wasm32")]

use std::cell::Cell;
use std::rc::Rc;

use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
	IdbCursorWithValue,
	IdbDatabase,
	IdbFactory,
	IdbKeyRange,
	IdbObjectStore,
	IdbRequest,
	IdbRequestReadyState,
	IdbTransaction,
	IdbTransactionMode,
	IdbVersionChangeEvent,
};

use crate::chunk::{Chunk, ChunkFill, ChunkLimits};
use crate::error::{Error, Result};

/// The name of the object store that the legacy `indxdb://` store keeps its rows in.
pub const KV_STORE: &str = "kv";

/// Checks whether IndexedDB is available in the current browser global scope.
pub fn is_indexeddb_available() -> bool {
	get_idb_factory().is_ok()
}

fn get_idb_factory() -> Result<IdbFactory> {
	let global = js_sys::global();
	if let Ok(window) = global.clone().dyn_into::<web_sys::Window>() {
		window
			.indexed_db()
			.map_err(|e| Error::Js(format!("Failed to access indexedDB: {}", describe(&e))))?
			.ok_or_else(|| Error::Js("indexedDB is not supported on window".into()))
	} else if let Ok(worker) = global.dyn_into::<web_sys::WorkerGlobalScope>() {
		worker
			.indexed_db()
			.map_err(|e| {
				Error::Js(format!("Failed to access indexedDB in worker: {}", describe(&e)))
			})?
			.ok_or_else(|| Error::Js("indexedDB is not supported in worker".into()))
	} else {
		Err(Error::Js("No browser window or worker global found".into()))
	}
}

/// The name and the message of a `DOMException`, or what a value that is not one looks like.
fn describe(error: &JsValue) -> String {
	if let Some(exception) = error.dyn_ref::<web_sys::DomException>() {
		return format!("{}: {}", exception.name(), exception.message());
	}
	if let Some(text) = error.as_string() {
		return text;
	}
	format!("{error:?}")
}

/// A connection to a database that is closed when it goes out of scope, whichever way the code that
/// holds it ends. It closes itself too when another context asks for the database to be upgraded
/// or deleted, so that a reader never holds up a legacy client that is waiting for its turn.
struct Connection {
	db: IdbDatabase,
	_on_version_change: EventHandler,
}

impl Connection {
	fn new(db: IdbDatabase) -> Self {
		let closing = db.clone();
		let on_version_change = Closure::<dyn FnMut(web_sys::Event)>::new(move |_event| {
			closing.close();
		});
		db.set_onversionchange(Some(on_version_change.as_ref().unchecked_ref()));
		Self {
			db,
			_on_version_change: on_version_change,
		}
	}

	fn has_store(&self, name: &str) -> bool {
		let names = self.db.object_store_names();
		(0..names.length()).any(|i| names.item(i).as_deref() == Some(name))
	}
}

impl Drop for Connection {
	fn drop(&mut self) {
		self.db.set_onversionchange(None);
		self.db.close();
	}
}

/// The handlers of a request, taken off it when this is dropped, so that a request that is waited
/// for and then left, or whose wait is dropped, is never left holding a closure that is gone.
struct Handlers<'a> {
	request: &'a IdbRequest,
	_closures: Vec<EventHandler>,
}

impl Drop for Handlers<'_> {
	fn drop(&mut self) {
		self.request.set_onsuccess(None);
		self.request.set_onerror(None);
	}
}

/// Waits for the next `success` or `error` of `request`, and takes its handlers off again. A
/// request that is waited for more than once (a cursor) is given new handlers each time, with no
/// await in between that could let its event go by.
async fn wait_for_request(request: &IdbRequest) -> Result<JsValue> {
	let mut closures = Vec::new();
	let promise = js_sys::Promise::new(&mut |resolve, reject| {
		let on_success = {
			let request = request.clone();
			let reject = reject.clone();
			Closure::once(move |_event: web_sys::Event| {
				let _ = match request.result() {
					Ok(result) => resolve.call1(&JsValue::NULL, &result),
					Err(error) => reject.call1(&JsValue::NULL, &error),
				};
			})
		};
		let on_error = {
			let request = request.clone();
			Closure::once(move |event: web_sys::Event| {
				// The error is the answer of this wait: it is not for the page to report.
				event.prevent_default();
				let error = request
					.error()
					.ok()
					.flatten()
					.map_or_else(|| JsValue::from_str("Unknown error"), JsValue::from);
				let _ = reject.call1(&JsValue::NULL, &error);
			})
		};
		request.set_onsuccess(Some(on_success.as_ref().unchecked_ref()));
		request.set_onerror(Some(on_error.as_ref().unchecked_ref()));
		closures.push(on_success);
		closures.push(on_error);
	});
	let _handlers = Handlers {
		request,
		_closures: closures,
	};
	JsFuture::from(promise)
		.await
		.map_err(|e| Error::Js(format!("IndexedDB request failed: {}", describe(&e))))
}

/// Resolves when a transaction has ended, whichever way it ended, and takes its listeners off again
/// when it is dropped. A connection that is closed while a transaction on it is still ending is
/// closed in the end, but a request to delete the database that comes in that moment is told that
/// it is blocked, so a read waits for the end of its transaction before it lets go of its
/// connection.
struct TransactionEnd<'a> {
	transaction: &'a IdbTransaction,
	promise: js_sys::Promise,
	closures: Vec<(&'static str, EventHandler)>,
}

type EventHandler = Closure<dyn FnMut(web_sys::Event)>;

impl<'a> TransactionEnd<'a> {
	fn watch(transaction: &'a IdbTransaction) -> Self {
		let mut closures = Vec::new();
		let promise = js_sys::Promise::new(&mut |resolve, _reject| {
			for kind in ["complete", "abort", "error"] {
				let resolve = resolve.clone();
				let closure = Closure::<dyn FnMut(web_sys::Event)>::new(move |_event| {
					let _ = resolve.call0(&JsValue::NULL);
				});
				let _ = transaction
					.add_event_listener_with_callback(kind, closure.as_ref().unchecked_ref());
				closures.push((kind, closure));
			}
		});
		Self {
			transaction,
			promise,
			closures,
		}
	}

	async fn ended(&self) {
		let _ = JsFuture::from(self.promise.clone()).await;
	}
}

impl Drop for TransactionEnd<'_> {
	fn drop(&mut self) {
		for (kind, closure) in &self.closures {
			let _ = self
				.transaction
				.remove_event_listener_with_callback(kind, closure.as_ref().unchecked_ref());
		}
	}
}

/// An open request that has been made and whose answer nobody is waiting for any more, because the
/// future that made it was dropped (a timeout, a cancelled task): what the browser brings later
/// must not be left open, and must not be made. A connection that nobody closes holds up every
/// later upgrade and deletion of the database, and every open that queues behind them.
struct PendingOpen {
	request: web_sys::IdbOpenDbRequest,
	settled: bool,
}

/// Closes the connection that an open request answered with, if it answered with one.
fn close_answer(request: &IdbRequest) {
	if let Ok(answer) = request.result() {
		if let Some(db) = answer.dyn_ref::<IdbDatabase>() {
			db.close();
		}
	}
}

/// Aborts the upgrade that the request is running, if it is: a database that is not there is made
/// by one, and aborting it is how it is not made.
fn abort_upgrade(request: &IdbRequest) {
	if let Some(transaction) = request.transaction() {
		let _ = transaction.abort();
	}
}

impl Drop for PendingOpen {
	fn drop(&mut self) {
		if self.settled {
			return;
		}
		// The handler of the upgrade is a closure that is about to be dropped.
		self.request.set_onupgradeneeded(None);
		if self.request.ready_state() == IdbRequestReadyState::Done {
			// The answer came before this was dropped, and nobody read it: an upgrade that is
			// running (it has a transaction) is aborted, and a connection that is open is closed.
			if self.request.transaction().is_some() {
				abort_upgrade(&self.request);
			} else {
				close_answer(&self.request);
			}
			return;
		}
		// The answer is still to come. Listeners, and not handlers: the wait that was dropped
		// clears `onsuccess` as it goes, in whatever order the two are dropped. Each is
		// freed when it has run.
		let on_upgrade = Closure::once_into_js(|event: web_sys::Event| {
			let from_nothing = event
				.dyn_ref::<IdbVersionChangeEvent>()
				.is_none_or(|upgrade| upgrade.old_version() == 0.0);
			if from_nothing {
				if let Some(request) = event.target().and_then(|t| t.dyn_into::<IdbRequest>().ok())
				{
					abort_upgrade(&request);
				}
			}
		});
		let on_success = Closure::once_into_js(|event: web_sys::Event| {
			if let Some(request) = event.target().and_then(|t| t.dyn_into::<IdbRequest>().ok()) {
				close_answer(&request);
			}
		});
		let _ = self
			.request
			.add_event_listener_with_callback("upgradeneeded", on_upgrade.unchecked_ref());
		let _ =
			self.request.add_event_listener_with_callback("success", on_success.unchecked_ref());
	}
}

/// Opens the database `name` at the version that it has, and `None` if there is none. Nothing is
/// created in either case.
async fn open_existing(factory: &IdbFactory, name: &str) -> Result<Option<Connection>> {
	let request = factory
		.open(name)
		.map_err(|e| Error::Js(format!("Failed to open database {name}: {}", describe(&e))))?;

	// A database that is not there is made by an upgrade from version zero, which the browser runs
	// in a transaction that this aborts, and the browser then does not keep the database. An
	// upgrade from another version cannot happen without a version in the call, and is left alone
	// if it does.
	let aborted = Rc::new(Cell::new(false));
	let on_upgrade = {
		let aborted = Rc::clone(&aborted);
		Closure::<dyn FnMut(web_sys::Event)>::new(move |event: web_sys::Event| {
			let from_nothing = event
				.dyn_ref::<IdbVersionChangeEvent>()
				.is_none_or(|upgrade| upgrade.old_version() == 0.0);
			if !from_nothing {
				return;
			}
			aborted.set(true);
			if let Some(request) = event.target().and_then(|t| t.dyn_into::<IdbRequest>().ok()) {
				if let Some(transaction) = request.transaction() {
					let _ = transaction.abort();
				}
			}
		})
	};
	request.set_onupgradeneeded(Some(on_upgrade.as_ref().unchecked_ref()));
	let mut pending = PendingOpen {
		request: request.clone(),
		settled: false,
	};
	let outcome = wait_for_request(request.as_ref()).await;
	pending.settled = true;
	request.set_onupgradeneeded(None);
	drop(on_upgrade);

	match outcome {
		// A connection that came back although the upgrade was aborted is closed by its drop.
		Ok(db) => {
			let connection = Connection::new(db.unchecked_into());
			Ok((!aborted.get()).then_some(connection))
		}
		Err(_) if aborted.get() => Ok(None),
		Err(e) => Err(e),
	}
}

/// What `indexedDB.databases()` says about the database `name`, where the browser has the call:
/// `Some(true)` if it is listed, `Some(false)` if it is not, and `None` if there is no such call
/// or it failed, which a caller treats as "unknown" (the open settles it).
async fn is_listed(factory: &IdbFactory, name: &str) -> Option<bool> {
	let call = js_sys::Reflect::get(factory, &JsValue::from_str("databases")).ok()?;
	let call: js_sys::Function = call.dyn_into().ok()?;
	let promise: js_sys::Promise = call.call0(factory).ok()?.dyn_into().ok()?;
	let list = JsFuture::from(promise).await.ok()?;
	let list: js_sys::Array = list.dyn_into().ok()?;
	let name = JsValue::from_str(name);
	Some(list.iter().any(|entry| {
		js_sys::Reflect::get(&entry, &JsValue::from_str("name")).is_ok_and(|n| n == name)
	}))
}

/// Refuses a store `kv` that is not of the shape that the legacy backend made: one whose keys are
/// given with each row, and are not in the value (no key path) or made by the store (no key
/// generator). The rows of any other store are not the pairs that the legacy backend wrote, and
/// that they could be read all the same would be luck.
fn check_shape(store: &IdbObjectStore) -> Result<()> {
	let key_path = store
		.key_path()
		.map_err(|e| Error::Js(format!("Failed to read the key path of kv: {}", describe(&e))))?;
	if !key_path.is_null() {
		return Err(Error::Js(format!(
			"The object store kv has a key path ({}), which the legacy store does not: its keys are \
			 given with each row",
			describe(&key_path)
		)));
	}
	if store.auto_increment() {
		return Err(Error::Js(
			"The object store kv makes its own keys (autoIncrement), which the legacy store does \
			 not: its keys are given with each row"
				.into(),
		));
	}
	Ok(())
}

/// A legacy `indxdb://` store in the browser, read and never written.
#[derive(Clone, Debug)]
pub struct IdbSource {
	name: String,
	use_listing: bool,
}

impl IdbSource {
	pub fn new(name: impl Into<String>) -> Self {
		Self {
			name: name.into(),
			use_listing: true,
		}
	}

	/// The same source, without a look at `indexedDB.databases()`: as it is read in a browser that
	/// does not have the call. For the tests of that path, which Chrome never takes by itself.
	#[doc(hidden)]
	pub fn without_listing(mut self) -> Self {
		self.use_listing = false;
		self
	}

	pub fn name(&self) -> &str {
		&self.name
	}

	/// Opens the database for a read, `None` if there is none (or no object store `kv` in it).
	async fn connect(&self) -> Result<Option<Connection>> {
		let factory = get_idb_factory()?;
		if self.use_listing && is_listed(&factory, &self.name).await == Some(false) {
			return Ok(None);
		}
		let Some(connection) = open_existing(&factory, &self.name).await? else {
			return Ok(None);
		};
		Ok(connection.has_store(KV_STORE).then_some(connection))
	}

	/// Whether the database is there and has the object store `kv`. It is not created if it is
	/// not, and what is there is not changed. A store `kv` that is not of the shape that the legacy
	/// backend made (one with a key path, or one that makes its own keys) is an error.
	pub async fn exists(&self) -> Result<bool> {
		let Some(connection) = self.connect().await? else {
			return Ok(false);
		};
		let transaction = connection
			.db
			.transaction_with_str_and_mode(KV_STORE, IdbTransactionMode::Readonly)
			.map_err(|e| {
				Error::Js(format!("Failed to open a transaction on kv: {}", describe(&e)))
			})?;
		// Waited for, as a read does: the connection is closed after the end of its transaction.
		let end = TransactionEnd::watch(&transaction);
		let shape = transaction
			.object_store(KV_STORE)
			.map_err(|e| Error::Js(format!("Failed to get the object store kv: {}", describe(&e))))
			.and_then(|store| check_shape(&store));
		end.ended().await;
		shape.map(|()| true)
	}

	/// The next chunk of the rows of `kv`, after the row whose key is `after` (from the chunk
	/// before), or from the first row if there is none. `None` if the database or the store is
	/// not there.
	///
	/// The chunk is read in a read-only transaction on a connection that is closed before this
	/// returns, whichever way it returns. The key that the chunk gives to continue from is the
	/// value that the cursor gave, and not a copy that was converted to bytes.
	pub async fn read_chunk(
		&self,
		after: Option<&JsValue>,
		limits: &ChunkLimits,
	) -> Result<Option<Chunk<JsValue>>> {
		let Some(connection) = self.connect().await? else {
			return Ok(None);
		};
		let transaction = connection
			.db
			.transaction_with_str_and_mode(KV_STORE, IdbTransactionMode::Readonly)
			.map_err(|e| {
				Error::Js(format!("Failed to open a transaction on kv: {}", describe(&e)))
			})?;
		// Watched from the start, so that its end cannot go by before it is waited for.
		let end = TransactionEnd::watch(&transaction);
		let store = transaction.object_store(KV_STORE).map_err(|e| {
			Error::Js(format!("Failed to get the object store kv: {}", describe(&e)))
		})?;
		check_shape(&store)?;
		let request = match after {
			Some(key) => {
				let range = IdbKeyRange::lower_bound_with_open(key, true).map_err(|e| {
					Error::Js(format!("Failed to make a key range after a key: {}", describe(&e)))
				})?;
				store.open_cursor_with_range(&range)
			}
			None => store.open_cursor(),
		}
		.map_err(|e| Error::Js(format!("Failed to open a cursor on kv: {}", describe(&e))))?;

		let mut fill = ChunkFill::new(*limits);
		let mut entries = Vec::new();
		let mut last = None;
		let mut done = false;
		loop {
			let result = wait_for_request(&request).await?;
			if result.is_null() || result.is_undefined() {
				done = true;
				break;
			}
			let cursor: IdbCursorWithValue = result
				.dyn_into()
				.map_err(|_| Error::Js("Failed to cast the cursor to IdbCursorWithValue".into()))?;
			let key = cursor
				.key()
				.map_err(|e| Error::Js(format!("Failed to get a key: {}", describe(&e))))?;
			let value = cursor
				.value()
				.map_err(|e| Error::Js(format!("Failed to get a value: {}", describe(&e))))?;
			let key_bytes = js_value_to_bytes(&key, "key")?;
			let value_bytes = js_value_to_bytes(&value, "value")?;
			let weight = key_bytes.len().saturating_add(value_bytes.len());
			// An entry that does not fit is left for the next chunk, which begins with it.
			if !fill.admits(weight) {
				break;
			}
			fill.take(weight);
			entries.push((key_bytes, value_bytes));
			last = Some(key);
			if fill.is_full() {
				break;
			}
			cursor.continue_().map_err(|e| {
				Error::Js(format!("Failed to advance the cursor: {}", describe(&e)))
			})?;
		}
		// No request is pending now, so the transaction ends by itself. The connection is closed
		// after that, which is what lets a deletion of the database that follows go through.
		end.ended().await;
		Ok(Some(Chunk {
			entries,
			bytes: fill.bytes(),
			last,
			done,
		}))
	}
}

/// Reads all live key-value pairs from the `"kv"` object store of an IndexedDB database, in the
/// order of its keys. A database that is not there, or has no such store, reads as no pairs, and
/// is not created. The whole store is held in memory: a caller that cannot afford that reads it
/// in chunks with [`IdbSource`].
pub async fn read_all_from_browser(db_name: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	let source = IdbSource::new(db_name);
	let limits = ChunkLimits::default();
	let mut all = Vec::new();
	let mut after: Option<JsValue> = None;
	let mut first = true;
	loop {
		let Some(chunk) = source.read_chunk(after.as_ref(), &limits).await? else {
			if first {
				return Ok(Vec::new());
			}
			return Err(Error::DatabaseNotFound(format!(
				"the IndexedDB database {db_name} or its store {KV_STORE} went away while it was read"
			)));
		};
		first = false;
		all.extend(chunk.entries);
		if chunk.done {
			return Ok(all);
		}
		after = chunk.last;
	}
}

fn js_value_to_bytes(val: &JsValue, what: &str) -> Result<Vec<u8>> {
	if let Some(uint8_arr) = val.dyn_ref::<js_sys::Uint8Array>() {
		Ok(uint8_arr.to_vec())
	} else if let Some(arr_buf) = val.dyn_ref::<js_sys::ArrayBuffer>() {
		let uint8_arr = js_sys::Uint8Array::new(arr_buf);
		Ok(uint8_arr.to_vec())
	} else if let Some(s) = val.as_string() {
		Ok(s.into_bytes())
	} else {
		Err(Error::Js(format!(
			"Unsupported IndexedDB {what} type, a Uint8Array, an ArrayBuffer or a string is \
			 needed: {val:?}"
		)))
	}
}
