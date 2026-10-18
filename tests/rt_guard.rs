//! Runs `scripts/check-rt-guard.sh`, the script that keeps the engine from naming a clock, a
//! timer, a task spawner or a thread outside the runtime shim (`src/rt.rs`): its self-test, and
//! the script itself on the engine.
//!
//! The self-test feeds the script fixtures that must be refused and fixtures that must pass, so
//! that a change to the script that makes it refuse nothing is caught here.

#![cfg(unix)]

use std::path::Path;
use std::process::Command;

fn script() -> Command {
	let root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let mut command = Command::new("sh");
	command.arg(root.join("scripts/check-rt-guard.sh")).current_dir(root);
	command
}

#[test]
fn the_guard_refuses_what_it_should_and_passes_what_it_should() {
	let output = script().arg("--self-test").output().expect("sh runs");
	assert!(
		output.status.success(),
		"the self-test of scripts/check-rt-guard.sh failed:\n{}{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	);
}

/// The engine itself passes: nothing outside `src/rt.rs`, test code and code that is compiled out
/// of a browser build calls a clock, a timer, a spawner or a thread that panics or hangs in a
/// browser.
#[test]
fn the_engine_calls_no_clock_timer_spawner_or_thread_outside_the_shim() {
	let output = script().output().expect("sh runs");
	assert!(
		output.status.success(),
		"scripts/check-rt-guard.sh found calls that must go through src/rt.rs:\n{}{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	);
}
