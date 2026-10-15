//! Runs the self-test of `scripts/check-rt-guard.sh`, the script that keeps the engine from
//! naming a clock, a timer, a task spawner or a thread outside the runtime shim (`src/rt.rs`).
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
