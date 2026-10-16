#!/bin/sh
# Proves that the list of disallowed calls in clippy.toml does what it says.
#
# `clippy.toml` names every call of `std::fs`, of `File` and of the `Path` methods that ask the
# operating system, which the engine has to make through its file system seam (src/vfs) so that
# it runs in a browser. The lint that reads it, `clippy::disallowed_methods`, is `allow` in
# Cargo.toml until the engine has been moved over, because CI runs clippy with `-D warnings`. Two
# things can go wrong in the meantime without any build noticing, and this script checks both:
#
#   1. an entry that matches nothing. clippy.toml takes exact paths only: `std::fs::*` or a
#      brace set is not an error, it is a warning that matches nothing, and a path that is
#      mistyped, or that a toolchain does not have, is silent with `allow-invalid`.
#   2. an entry that is not in the fixture, so that nobody knows whether it works.
#
# Usage:
#   scripts/check-fs-lint.sh [--static]   static checks of clippy.toml (no wildcards, a reason on
#                                         each entry, no duplicates)
#   scripts/check-fs-lint.sh --self-test  the static checks, and then: builds a scratch crate that
#                                         makes one call of every entry with the lint at `deny`,
#                                         using this clippy.toml, and fails unless clippy reports
#                                         exactly the entries of the file. Needs `cargo clippy`.
#   FS_LINT_CONFIG=file   names another clippy.toml (the self-test of the tests uses it)
#   scripts/check-fs-lint.sh --audit      counts, for each source file, the calls of the engine that
#                                         the lint finds when it is made a warning: the size of the
#                                         allow-list that the last move to the seam has to write.
#                                         Never fails because of a count.
set -eu

root=$(cd "$(dirname "$0")/.." && pwd)
config=${FS_LINT_CONFIG:-$root/clippy.toml}

entries() {
	sed -n 's/.*{ *path *= *"\([^"]*\)".*/\1/p' "$config" | sort
}

static_checks() {
	status=0
	if [ ! -s "$config" ]; then
		echo "check-fs-lint: $config is missing or empty" >&2
		return 1
	fi
	if [ "$(entries | wc -l | tr -d ' ')" -eq 0 ]; then
		echo "check-fs-lint: no entry in $config" >&2
		return 1
	fi
	if grep -n 'path *= *"[^"]*[*{}]' "$config" > /dev/null; then
		echo "check-fs-lint: a wildcard or a brace set in $config matches nothing:" >&2
		grep -n 'path *= *"[^"]*[*{}]' "$config" >&2
		status=1
	fi
	if grep -n '{ *path *=' "$config" | grep -v 'reason *= *"[^"]\{8,\}"' > /dev/null; then
		echo "check-fs-lint: an entry of $config has no reason:" >&2
		grep -n '{ *path *=' "$config" | grep -v 'reason *= *"[^"]\{8,\}"' >&2
		status=1
	fi
	dup=$(entries | uniq -d)
	if [ -n "$dup" ]; then
		echo "check-fs-lint: duplicate entries in $config: $dup" >&2
		status=1
	fi
	return $status
}

# One call of every entry, in a function whose arguments are what the methods need.
write_fixture() {
	cat > "$1/src/main.rs" <<'RS'
#![deny(clippy::disallowed_methods)]
use std::fs;
use std::path::Path;

fn calls(p: &Path, f: &fs::File, o: &mut fs::OpenOptions, e: &fs::DirEntry) {
	let _ = fs::read(p);
	let _ = fs::read_to_string(p);
	let _ = fs::write(p, b"");
	let _ = fs::copy(p, p);
	let _ = fs::rename(p, p);
	let _ = fs::remove_file(p);
	let _ = fs::remove_dir(p);
	let _ = fs::remove_dir_all(p);
	let _ = fs::create_dir(p);
	let _ = fs::create_dir_all(p);
	let _ = fs::read_dir(p);
	let _ = fs::metadata(p);
	let _ = fs::symlink_metadata(p);
	let _ = fs::canonicalize(p);
	let _ = fs::hard_link(p, p);
	let _ = fs::read_link(p);
	let _ = fs::set_permissions(p, fs::metadata(p).unwrap().permissions());
	let _ = fs::exists(p);
	let _ = fs::File::open(p);
	let _ = fs::File::create(p);
	let _ = fs::File::create_new(p);
	let _ = fs::File::options();
	let _ = f.metadata();
	let _ = f.set_len(0);
	let _ = f.sync_all();
	let _ = f.sync_data();
	let _ = f.try_clone();
	let _ = o.open(p);
	let _ = p.exists();
	let _ = p.try_exists();
	let _ = p.is_dir();
	let _ = p.is_file();
	let _ = p.metadata();
	let _ = p.read_dir();
	let _ = p.canonicalize();
	let _ = p.symlink_metadata();
	let _ = e.metadata();
	let _ = e.file_type();
}

fn main() {
	let _ = calls;
}
RS
}

self_test() {
	static_checks
	command -v cargo > /dev/null || { echo "check-fs-lint: cargo is needed for --self-test" >&2; return 1; }
	work=$(mktemp -d)
	trap 'rm -rf "$work"' EXIT
	mkdir -p "$work/src"
	cat > "$work/Cargo.toml" <<'TOML'
[package]
name = "fs-lint-fixture"
version = "0.0.0"
edition = "2021"

[workspace]
TOML
	cp "$config" "$work/clippy.toml"
	write_fixture "$work"

	# A scratch target directory: this runs inside `cargo test` as well, and must not wait for
	# the lock of the one that is running.
	out=$(cd "$work" && CARGO_TARGET_DIR="$work/target" RUSTFLAGS="" cargo clippy --message-format short 2>&1 || true)
	flagged=$(printf '%s\n' "$out" | sed -n 's/.*use of a disallowed method `\([^`]*\)`.*/\1/p' | sort -u)
	want=$(entries)

	status=0
	printf '%s\n' "$flagged" > "$work/flagged"
	printf '%s\n' "$want" > "$work/want"
	missing=$(comm -13 "$work/flagged" "$work/want")
	if [ -n "$missing" ]; then
		echo "check-fs-lint: entries that clippy did not report on the fixture (mistyped, or not in a toolchain's std, or missing from the fixture):" >&2
		printf '%s\n' "$missing" | sed 's/^/    /' >&2
		status=1
	fi
	extra=$(comm -23 "$work/flagged" "$work/want" | grep -v '^$' || true)
	if [ -n "$extra" ]; then
		echo "check-fs-lint: clippy reported calls that clippy.toml does not list: $extra" >&2
		status=1
	fi
	if [ -z "$flagged" ]; then
		echo "check-fs-lint: clippy reported nothing; its output was:" >&2
		printf '%s\n' "$out" | tail -20 >&2
		status=1
	fi

	# The lint must stay quiet when it is `allow`, which is how Cargo.toml has it: the same
	# fixture with the level lowered makes no diagnostic.
	sed -i.bak 's/#!\[deny(clippy::disallowed_methods)\]/#![allow(clippy::disallowed_methods)]/' "$work/src/main.rs"
	quiet=$(cd "$work" && CARGO_TARGET_DIR="$work/target" RUSTFLAGS="" cargo clippy --message-format short 2>&1 || true)
	if printf '%s\n' "$quiet" | grep -q 'disallowed method'; then
		echo "check-fs-lint: the lint reports at level allow" >&2
		status=1
	fi

	if [ "$status" -ne 0 ]; then
		return 1
	fi
	echo "check-fs-lint self-test: ok ($(printf '%s\n' "$want" | wc -l | tr -d ' ') entries, all reported)"
}

audit() {
	static_checks
	cd "$root"
	target=$(mktemp -d)
	trap 'rm -rf "$target"' EXIT
	CARGO_TARGET_DIR="$target" cargo clippy -p surrealkv --all-targets --message-format short \
		-- -W clippy::disallowed_methods 2>&1 \
		| sed -n 's/^\([^:]*\):[0-9]*:[0-9]*: warning: use of a disallowed method.*/\1/p' \
		| sort | uniq -c | sort -rn
}

case "${1:-}" in
	--self-test) self_test ;;
	--audit) audit ;;
	--static | "") static_checks && echo "check-fs-lint: ok" ;;
	*) echo "usage: $0 [--self-test|--audit]" >&2; exit 2 ;;
esac
