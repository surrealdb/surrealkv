//! Runs `scripts/check-passthrough.sh`, the script that keeps the typed error of a file pool that
//! has no free file from being turned into text: its self-test, and the script itself on the
//! engine.
//!
//! The self-test feeds the script arms of every shape the engine has, with and without the arm for
//! the pool error, so that a change to the script that checks nothing is caught here.

#![cfg(unix)]

use std::path::Path;
use std::process::Command;

fn script() -> Command {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut command = Command::new("sh");
	command.arg(root.join("scripts/check-passthrough.sh")).current_dir(root);
	command
}

#[test]
fn the_guard_refuses_what_it_should_and_passes_what_it_should() {
	let output = script().arg("--self-test").output().expect("sh runs");
	assert!(
		output.status.success(),
		"the self-test of scripts/check-passthrough.sh failed:\n{}{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	);
}

/// Every file whose passthrough arms are enforced has as many for the pool error as for an
/// encryption error, and no file that has one is missing from the table.
#[test]
fn the_engine_passes_the_arms_of_the_pool_error_where_it_passes_those_of_an_encryption_error() {
	let output = script().output().expect("sh runs");
	assert!(
		output.status.success(),
		"scripts/check-passthrough.sh found a missing arm:\n{}{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	);
}
