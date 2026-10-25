//! The CPU cost of commit validation: one committer checked against a window
//! of in-flight commits, none of which conflict.

use surrealkv::ValidateWindow;

fn main() {
	divan::main();
}

const WINDOWS: &[usize] = &[64, 1024, 8192];

/// Single-key writes with 40-byte keys, as single-record creates make.
#[divan::bench(args = WINDOWS)]
fn single_key(b: divan::Bencher<'_, '_>, window: usize) {
	let w = ValidateWindow::new(window, 1, 40, 0);
	b.counter(window).bench_local(|| assert_eq!(divan::black_box(&w).validate(), 0));
}

/// Single-key writes that also hold one locked read.
#[divan::bench(args = WINDOWS)]
fn single_key_locked_read(b: divan::Bencher<'_, '_>, window: usize) {
	let w = ValidateWindow::new(window, 1, 40, 1);
	b.counter(window).bench_local(|| assert_eq!(divan::black_box(&w).validate(), 0));
}

/// Writes of 4 keys (a record and its index entries).
#[divan::bench(args = WINDOWS)]
fn four_keys(b: divan::Bencher<'_, '_>, window: usize) {
	let w = ValidateWindow::new(window, 4, 40, 0);
	b.counter(window).bench_local(|| assert_eq!(divan::black_box(&w).validate(), 0));
}

/// Writes of 64 keys, where the filters fill up.
#[divan::bench(args = WINDOWS)]
fn sixty_four_keys(b: divan::Bencher<'_, '_>, window: usize) {
	let w = ValidateWindow::new(window, 64, 40, 0);
	b.counter(window).bench_local(|| assert_eq!(divan::black_box(&w).validate(), 0));
}
