//! Tests of the engine that run in a dedicated worker of a real headless Chrome, where
//! `wasm32-unknown-unknown` has no threads, no `std::time` clock and no OS random source.
//!
//! They are tests of the crate's own `pub(crate)` items (the runtime shim, the clock, the error
//! handler), which an integration test under `tests/` cannot reach, so they are unit tests that
//! `wasm-pack` runs through the library target.
//!
//! Run them, from the repository root, with
//!
//! ```text
//! WASM_BINDGEN_TEST_TIMEOUT=300 WASM_BINDGEN_USE_DEDICATED_WORKER=1 \
//!     wasm-pack test --headless --chrome -- --lib
//! ```
//!
//! The test binary links the whole engine, zstd's C code included. On macOS the system `ar` writes
//! an empty archive for it, so point the C toolchain at LLVM's first:
//!
//! ```text
//! export CC_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/clang
//! export AR_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/llvm-ar
//! ```
//!
//! No `RUSTFLAGS` are needed: the tests below that draw random bytes are the proof of that, for
//! both `getrandom` versions in the graph.
//!
//! A panic or a trap in one test ends the instance, and with it the tests that did not run yet.

use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// `getrandom` 0.4 is what draws the per-file salt and the database id.
#[wasm_bindgen_test]
fn getrandom_04_draws_bytes_without_rustflags() {
	let mut first = [0u8; 32];
	let mut second = [0u8; 32];
	getrandom_04::fill(&mut first).expect("the browser has a random source");
	getrandom_04::fill(&mut second).expect("the browser has a random source");
	assert_ne!(first, [0u8; 32]);
	assert_ne!(first, second);
}

/// `rand` 0.9 seeds its generator through `getrandom` 0.3, which is the one that used to need
/// `--cfg getrandom_backend="wasm_js"` before 0.3.4.
#[wasm_bindgen_test]
fn rand_draws_bytes_without_rustflags() {
	let first: [u64; 4] = rand::random();
	let second: [u64; 4] = rand::random();
	assert_ne!(first, [0u64; 4]);
	assert_ne!(first, second);
}
