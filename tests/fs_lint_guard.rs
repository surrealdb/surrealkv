//! Runs `scripts/check-fs-lint.sh`, which proves that the list of disallowed calls in
//! `clippy.toml` finds what it names.
//!
//! The lint is `deny` for the library in `Cargo.toml`, with an `allow` and a reason on each place
//! that stays on `std::fs` on purpose. Nothing else would notice an entry that matches nothing (a
//! path that is mistyped, or a wildcard, which clippy does not reject and does not apply), nor a
//! level that was lowered again.

#![cfg(unix)]
#![allow(clippy::disallowed_methods, reason = "a test writes scratch lint lists on a disk")]

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

/// A source tree that has the shape the level checks read: the root manifest, a member crate and
/// the root of the library.
fn tree(name: &str, root_level: &str, member_level: &str, lib: &str) -> std::path::PathBuf {
	let dir =
		std::env::temp_dir().join(format!("skv-fs-lint-levels-{}-{name}", std::process::id()));
	let _ = std::fs::remove_dir_all(&dir);
	std::fs::create_dir_all(dir.join("src")).unwrap();
	std::fs::create_dir_all(dir.join("crates/member")).unwrap();
	std::fs::write(
		dir.join("Cargo.toml"),
		format!("[lints.clippy]\ndisallowed_methods = \"{root_level}\"\n"),
	)
	.unwrap();
	std::fs::write(
		dir.join("crates/member/Cargo.toml"),
		format!("[lints.clippy]\ndisallowed_methods = \"{member_level}\"\n"),
	)
	.unwrap();
	std::fs::write(dir.join("src/lib.rs"), lib).unwrap();
	dir
}

fn levels(dir: &Path) -> Output {
	let mut command = Command::new("sh");
	command
		.arg(root().join("scripts/check-fs-lint.sh"))
		.arg("--static")
		.current_dir(root())
		.env("FS_LINT_ROOT", dir);
	command.output().expect("sh runs")
}

#[test]
fn the_script_refuses_a_level_that_was_lowered() {
	const TESTS_ONLY: &str = "#![cfg_attr(\n\ttest,\n\tallow(clippy::disallowed_methods, reason = \"tests use a disk\")\n)]\n";

	// The shape that is meant to be accepted.
	let good = tree("good", "deny", "allow", TESTS_ONLY);
	let output = levels(&good);
	assert!(output.status.success(), "{}", text(&output));

	// The library lowered to `allow`.
	let lowered = tree("lowered", "allow", "allow", TESTS_ONLY);
	let output = levels(&lowered);
	assert!(!output.status.success(), "a library at allow was accepted: {}", text(&output));

	// A member crate raised to `deny`: the rest of the workspace is not moved over.
	let raised = tree("raised", "deny", "deny", TESTS_ONLY);
	let output = levels(&raised);
	assert!(!output.status.success(), "a member at deny was accepted: {}", text(&output));

	// A blanket allow at the root of the library for the code that is not a test.
	let blanket = tree(
		"blanket",
		"deny",
		"allow",
		"#![allow(clippy::disallowed_methods, reason = \"everything may use a disk\")]\n",
	);
	let output = levels(&blanket);
	assert!(!output.status.success(), "a blanket allow was accepted: {}", text(&output));

	// An allow that does not say why.
	let reasonless = tree("reasonless", "deny", "allow", TESTS_ONLY);
	std::fs::write(
		reasonless.join("src/lsm.rs"),
		"#[allow(clippy::disallowed_methods)]\nfn f() {}\n",
	)
	.unwrap();
	let output = levels(&reasonless);
	assert!(!output.status.success(), "an allow without a reason was accepted: {}", text(&output));

	for dir in [good, lowered, raised, blanket, reasonless] {
		let _ = std::fs::remove_dir_all(dir);
	}
}
