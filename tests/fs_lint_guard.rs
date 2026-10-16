//! Runs `scripts/check-fs-lint.sh`, which proves that the list of disallowed calls in
//! `clippy.toml` finds what it names.
//!
//! The lint is `allow` in `Cargo.toml` while the engine is moved over to its file system seam, so
//! nothing else would notice an entry that matches nothing: a path that is mistyped, or a
//! wildcard, which clippy does not reject and does not apply.

#![cfg(unix)]

use std::path::Path;
use std::process::{Command, Output};

fn root() -> &'static Path {
	Path::new(env!("CARGO_MANIFEST_DIR"))
}

fn script(config: Option<&Path>, arg: &str) -> Output {
	let mut command = Command::new("sh");
	command.arg(root().join("scripts/check-fs-lint.sh")).arg(arg).current_dir(root());
	if let Some(config) = config {
		command.env("FS_LINT_CONFIG", config);
	}
	command.output().expect("sh runs")
}

fn have_clippy() -> bool {
	Command::new("cargo")
		.args(["clippy", "--version"])
		.output()
		.map(|output| output.status.success())
		.unwrap_or(false)
}

fn text(output: &Output) -> String {
	format!(
		"{}{}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr)
	)
}

#[test]
fn every_entry_of_clippy_toml_is_found_when_the_lint_is_denied() {
	if !have_clippy() {
		eprintln!("cargo clippy is not installed: the self-test of the lint list is skipped");
		return;
	}
	let output = script(None, "--self-test");
	assert!(output.status.success(), "{}", text(&output));
}

#[test]
fn the_script_refuses_a_list_that_matches_nothing() {
	let dir = std::env::temp_dir().join(format!("skv-fs-lint-{}", std::process::id()));
	std::fs::create_dir_all(&dir).unwrap();
	let original = std::fs::read_to_string(root().join("clippy.toml")).unwrap();

	// A wildcard is not an error for clippy and matches nothing.
	let wildcard = dir.join("wildcard.toml");
	std::fs::write(&wildcard, original.replace("std::fs::read_link", "std::fs::*")).unwrap();
	let output = script(Some(&wildcard), "--static");
	assert!(!output.status.success(), "a wildcard was accepted: {}", text(&output));

	// An entry without a reason.
	let reasonless = dir.join("reasonless.toml");
	std::fs::write(
		&reasonless,
		"disallowed-methods = [\n\t{ path = \"std::fs::read\", allow-invalid = true },\n]\n",
	)
	.unwrap();
	let output = script(Some(&reasonless), "--static");
	assert!(!output.status.success(), "an entry without a reason was accepted: {}", text(&output));

	// A path that is mistyped is silent with `allow-invalid`, and is found by the fixture.
	if have_clippy() {
		let typo = dir.join("typo.toml");
		std::fs::write(&typo, original.replace("std::fs::read_link", "std::fs::read_lnk")).unwrap();
		let output = script(Some(&typo), "--self-test");
		assert!(!output.status.success(), "a mistyped path was accepted: {}", text(&output));
		assert!(text(&output).contains("std::fs::read_lnk"), "{}", text(&output));
	}
	let _ = std::fs::remove_dir_all(&dir);
}
