//! Browser tests, in a dedicated worker of a real headless Chrome (`WASM_BINDGEN_TEST_TIMEOUT=300
//! WASM_BINDGEN_USE_DEDICATED_WORKER=1 wasm-pack test --headless --chrome --test
//! opfs_options_test`), that the public entry points of the engine do not trap on
//! `wasm32-unknown-unknown`.
//!
//! The first thing `Options::default()` did there was read `SystemTime` and `Instant`, which panic
//! on that target, and a panic aborts the whole instance. The file system is still `std::fs` at
//! this point, which a browser does not have, so a tree cannot be built yet. What these tests hold
//! the engine to is that every way it can fail is an `Err` and not a trap. The tests of the
//! crate's own runtime shim, clock and error handler, which `tests/` cannot reach, are the unit
//! tests in `src/wasm_tests.rs` and `src/rt.rs`.

#![cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]

use std::time::Duration;

use surrealkv::{BackgroundErrorHandler, BackgroundErrorReason, Error, Options, Tree, TreeBuilder};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

#[wasm_bindgen_test]
fn the_default_options_and_a_builder_do_not_trap() {
	let options = Options::default();
	options.validate().expect("the default options are valid");
	let _ = TreeBuilder::new();
	let _ = TreeBuilder::with_options(Options::default().with_max_memtable_size(16 << 20));
}

#[wasm_bindgen_test]
fn a_memtable_that_a_32_bit_address_space_cannot_hold_is_refused() {
	let refused = Options::default().with_max_memtable_size(2 << 30).validate();
	assert!(matches!(refused, Err(Error::InvalidArgument(_))), "{refused:?}");
}

#[wasm_bindgen_test]
fn building_a_tree_returns_an_error_and_does_not_trap() {
	// A tree needs a file system, and the one it uses is `std::fs`, which a browser does not
	// have. The clock, the lock and the first background tasks are reached before it says so.
	let built: Result<Tree, Error> =
		TreeBuilder::new().with_path("/surrealkv-options-test".into()).build();
	match built {
		Ok(_) => panic!("a tree cannot be built over std::fs in a browser"),
		Err(error) => console_log!("building a tree failed cleanly: {error}"),
	}
	// And the instance is alive afterwards.
	let _ = Options::default();
}

#[wasm_bindgen_test]
fn a_background_error_is_recorded_without_trapping() {
	let handler = BackgroundErrorHandler::new();
	handler.set_error(Error::Other("a flush failed".into()), BackgroundErrorReason::MemtablaFlush);
	let recorded = handler.get_error().expect("the error is recorded");
	assert!(recorded.timestamp.elapsed() < Duration::from_secs(60));
}
