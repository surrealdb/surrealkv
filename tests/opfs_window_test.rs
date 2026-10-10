//! Browser tests of the OPFS backend on the main thread (the window) of a real headless Chrome
//! (`wasm-pack test --headless --chrome --test opfs_window_test`).
//!
//! `FileSystemSyncAccessHandle` exists only in dedicated workers, so on the main thread the
//! backend cannot open a sync file at all. What does work there is `get_opfs_root` and the
//! directory handle it returns, and that is what the passing tests cover. The one test that opens
//! a sync file asserts the behaviour a caller needs, a clean error, and is ignored because the
//! backend throws an uncatchable JavaScript exception instead; run it with `-- --include-ignored`
//! to see that.
//!
//! The target selects the main thread itself, so it is independent of
//! `WASM_BINDGEN_USE_DEDICATED_WORKER`.

#![cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]

mod opfs_common;

use opfs_common::*;
use surrealkv::storage::opfs::{get_opfs_root, open_opfs_sync_file};
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;
use web_sys::{FileSystemFileHandle, FileSystemGetFileOptions};

wasm_bindgen_test_configure!(run_in_browser);

/// Looks the file up through the plain directory handle (no sync access handle involved).
async fn file_handle(
	root: &web_sys::FileSystemDirectoryHandle,
	name: &str,
	create: bool,
) -> Result<FileSystemFileHandle, wasm_bindgen::JsValue> {
	let options = FileSystemGetFileOptions::new();
	options.set_create(create);
	let handle = JsFuture::from(root.get_file_handle_with_options(name, &options)).await?;
	Ok(handle.unchecked_into())
}

#[wasm_bindgen_test]
async fn these_tests_run_on_the_main_thread() {
	let global = js_sys::global();
	assert!(global.dyn_ref::<web_sys::Window>().is_some(), "this target must run on a window");
	assert!(
		global.dyn_ref::<web_sys::WorkerGlobalScope>().is_none(),
		"this target must not run in a worker"
	);
}

#[wasm_bindgen_test]
async fn get_opfs_root_works_on_the_main_thread_and_returns_a_usable_directory_handle() {
	let first = get_opfs_root().await.expect("get_opfs_root on the main thread");
	let name = unique_name("window-entry", "bin");

	// The directory can make a file entry, find it again, and remove it.
	let err =
		file_handle(&first, &name, false).await.expect_err("an entry that does not exist yet");
	assert!(text(&err).contains("NotFoundError"), "{}", text(&err));
	file_handle(&first, &name, true).await.expect("create the entry");

	// Another call returns a handle to the same directory, which sees the entry.
	for call in 0..10 {
		let again = get_opfs_root().await.expect("get_opfs_root, again");
		let same = JsFuture::from(first.is_same_entry(&again)).await.expect("isSameEntry");
		assert_eq!(same.as_bool(), Some(true), "call {call} returned a different directory");
		file_handle(&again, &name, false).await.expect("the entry is visible through every root");
	}

	JsFuture::from(first.remove_entry(&name)).await.expect("remove the entry");
	let err = file_handle(&first, &name, false).await.expect_err("the removed entry");
	assert!(text(&err).contains("NotFoundError"), "{}", text(&err));
}

#[wasm_bindgen_test]
async fn get_opfs_root_can_be_awaited_concurrently_on_the_main_thread() {
	let stem = unique_name("window-roots", "x");
	let mut tasks = Vec::new();
	for id in 0..6 {
		let name = format!("{stem}-{id}.bin");
		tasks.push(spawn(async move {
			let root = get_opfs_root().await.map_err(failed("get_opfs_root"))?;
			file_handle(&root, &name, true).await.map_err(failed("create the entry"))?;
			Ok::<_, String>(name)
		}));
	}
	let root = root().await;
	for (id, task) in tasks.into_iter().enumerate() {
		let name = finished(&format!("task {id}"), task).await;
		file_handle(&root, &name, false).await.expect("the entry a task created");
		JsFuture::from(root.remove_entry(&name)).await.expect("remove the entry");
	}
}

#[wasm_bindgen_test]
#[ignore = "open_opfs_sync_file on the main thread throws an uncatchable JavaScript exception instead of returning an error"]
async fn opening_a_sync_file_on_the_main_thread_returns_an_error_instead_of_throwing() {
	let root = root().await;
	let name = unique_name("window-sync", "bin");

	// The browser has no createSyncAccessHandle on a window. A caller must get an Err it can
	// handle. Today the call into the browser is not guarded, so a TypeError
	// ("createSyncAccessHandle is not a function") escapes and aborts the WebAssembly instance.
	let result = open_opfs_sync_file(&root, &name, true).await;
	assert!(result.is_err(), "a sync file cannot exist on the main thread");

	// Remove the empty entry that getFileHandle(create = true) made on the way, if any.
	if file_handle(&root, &name, false).await.is_ok() {
		JsFuture::from(root.remove_entry(&name)).await.expect("remove the leftover entry");
	}
}
